use crate::db::MainDatabase;
use crate::plugins::PluginManager;
use interprocess::local_socket::ToFsName;
use interprocess::local_socket::traits::Listener;
use interprocess::local_socket::{GenericFilePath, ListenerOptions};
use log::info;
use std::io::BufReader;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;
use std::{sync::Arc, sync::Mutex};
pub struct IpcServer {
    local_server: Mutex<Option<JoinHandle<()>>>,
    should_exit: Arc<AtomicBool>,
    db: Arc<MainDatabase>,
    plugin_manager: Arc<PluginManager>,
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        self.should_exit
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let mut local_server = self.local_server.lock().unwrap();
        if let Some(local_server) = local_server.take() {
            local_server.join().unwrap();
        }
    }
}

impl IpcServer {
    pub fn new(
        db: Arc<MainDatabase>,
        should_exit: Arc<AtomicBool>,
        plugin_manager: Arc<PluginManager>,
    ) -> Arc<Self> {
        let out = Arc::new(Self {
            local_server: Mutex::new(None),
            should_exit,
            db,
            plugin_manager,
        });

        out.clone().startup().unwrap();
        out
    }

    /// Starts up the api
    pub fn startup(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let name = "/tmp/rusthydrus/rusthydrus.sock"
            .to_fs_name::<GenericFilePath>()
            .unwrap();
        let _ = std::fs::create_dir("/tmp/rusthydrus");

        let _ = std::fs::remove_file("/tmp/rusthydrus/rusthydrus.sock");

        let listener = ListenerOptions::new()
            .name(name)
            .create_sync() // Synchronous backend
            .unwrap();

        let self_clone = self.clone();

        let handle = thread::spawn(move || {
            while !self_clone.should_exit.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok(conn) => {
                        // Clone the Arc for the new connection thread
                        let self_for_conn = self_clone.clone();

                        // Spawn a dedicated thread for each client connection
                        thread::spawn(move || {
                            let mut reader = BufReader::new(conn);
                            if let Ok(recieved_data) = client::recieve(&mut reader) {
                                let response = self_for_conn.conn_to_function(recieved_data);
                                client::send_preserialize(&response, &mut reader);
                            }
                        });
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
        match action {
            client::SupportedDBRequests::LoggingNoPrint(data) => {
                info!("IPC LOG: {data}");
                client::data_size_to_b(&false)
            }
            client::SupportedDBRequests::ShouldExit => {
                client::data_size_to_b(&self.should_exit.load(Ordering::SeqCst))
            }
            client::SupportedDBRequests::ExternalPluginCall(key, callback_info) => {
                let out = self
                    .plugin_manager
                    .external_plugin_call(&key, &callback_info);

                client::data_size_to_b(&out)
            }
            action => self
                .db
                .dispatch_ipc_request(action)
                .unwrap_or_else(|| client::data_size_to_b(&false)),
        }
    }
}
