use crate::db::MainDatabase;
use interprocess::local_socket::NameType;
use interprocess::local_socket::ToFsName;
use interprocess::local_socket::ToNsName;
use interprocess::local_socket::traits::Listener;
use interprocess::local_socket::{GenericFilePath, GenericNamespaced, ListenerOptions};
use log::info;
use std::io::BufReader;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;
use std::{sync::Arc, sync::Mutex};
pub struct IpcServer {
    local_server: Mutex<Option<JoinHandle<()>>>,
    shutdown_signal: Arc<AtomicBool>,
    db: Arc<MainDatabase>,
}

macro_rules! register_db_requests {
    (
        $self:expr, $action:expr; // Accepts the context variables for the match statement
        $(
            // Syntax: EnumVariant(args) => DB_Function(args)
            $variant:ident ( $($arg:ident),* ) => $db_fn:ident
        ),* $(,)?
    ) => {
        match $action {
            $(
                // Automatically generate the match arm for each variant
                client::SupportedDBRequests::$variant( $($arg),* ) => {
                    client::data_size_to_b(&$self.db.$db_fn($(&$arg),*))
                }
            )*
            _ => client::data_size_to_b(&false),
        }
    };
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        self.shutdown_signal
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let mut local_server = self.local_server.lock().unwrap();
        if let Some(local_server) = local_server.take() {
            local_server.join().unwrap();
        }
    }
}

impl IpcServer {
    pub fn new(db: Arc<MainDatabase>) -> Arc<Self> {
        let out = Arc::new(IpcServer {
            local_server: Mutex::new(None),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            db,
        });

        out.clone().startup().unwrap();
        out
    }

    /// Starts up the api
    pub fn startup(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let name = 
            "/tmp/rusthydrus.sock".to_fs_name::<GenericFilePath>().unwrap()
        ;

        let listener = ListenerOptions::new()
            .name(name)
            .create_sync() // Synchronous backend
            .unwrap();

        let self_clone = self.clone();

        let handle = thread::spawn(move || {
            while !self_clone.shutdown_signal.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok(conn) => {
                        let mut reader = BufReader::new(conn);
                        if let Ok(recieved_data) = client::recieve(&mut reader) {
                            let response = self_clone.conn_to_function(recieved_data);
                            client::send_preserialize(&response, &mut reader);
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(std::time::Duration::from_millis(50));
                    }
                    Err(e) => {
                        log::error!("Incoming connection failed: {e}");
                    }
                }
            }
        });

        // Store the thread handle so the host application can join it later on shutdown
        *self.local_server.lock().unwrap() = Some(handle);

        Ok(())
    }

    ///
    /// Converts the functions to the u8 outputs
    ///
    fn conn_to_function(&self, action: client::SupportedDBRequests) -> Vec<u8> {
        if let client::SupportedDBRequests::LoggingNoPrint(data) = &action {
            info!("IPC LOG: {}", data);
            return client::data_size_to_b(&false);
        }

        // db calls go here
        register_db_requests! {
            self, action;

            // Enum Variant         =>  Database Synchronous Method
            SettingsGetName(name)   =>  setting_get_sync,
            SettingsSet(settings)   =>  setting_set_sync,
            SearchFiles(search, limit) => search_db_files_sync,
            GetTagIds(tag_ids) => tag_id_get_tag_sync,
            SearchTags(tag, limit) => search_db_tags_fts,
            GetNamespace(name) => search_db_namespace_sync,
            GetNamespaceTagIdsFiltered(file_id, namespace_id) => internal_file_id_get_tag_ids_where_namespace_id_sync,
            GetFileLocation(file_id) => file_get_physical_path_sync,
            //GetTagId(id)            =>  get_tag_id_sync,
            //GetFile(id)             =>  get_file_sync,
            //RelationshipAdd(u1, u2) =>  relationship_add_sync,
        }
    }
}
