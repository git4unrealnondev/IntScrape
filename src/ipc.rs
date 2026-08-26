use crate::db::MainDatabase;
use crate::plugins::PluginManager;
use interprocess::local_socket::ToFsName;
use interprocess::local_socket::{GenericFilePath, ListenerOptions, tokio::prelude::*};
use log::info;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::io::BufReader;
use tokio::task::JoinHandle;
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

        if let Some(local_server) = self.local_server.lock().unwrap().take() {
            local_server.abort();
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

        out.clone().startup();
        out
    }

    /// Starts up the api
    pub fn startup(self: Arc<Self>) {
        let name = "/tmp/rusthydrus/rusthydrus.sock"
            .to_fs_name::<GenericFilePath>()
            .unwrap();
        let _ = std::fs::create_dir("/tmp/rusthydrus");

        let _ = std::fs::remove_file("/tmp/rusthydrus/rusthydrus.sock");

        let listener = match ListenerOptions::new().name(name).create_tokio() {
            Ok(listener) => listener,
            Err(error) => {
                log::error!("Failed to start Tokio IPC listener: {error}");
                return;
            }
        };

        let self_clone = self.clone();

        let handle = tokio::spawn(async move {
            loop {
                if self_clone.should_exit.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    result = listener.accept() => match result {
                    Ok(conn) => {
                        // Clone the Arc for the new connection thread
                        let self_for_conn = self_clone.clone();

                        // Spawn a dedicated thread for each client connection
                        tokio::spawn(async move {
                            let mut reader = BufReader::new(conn);
                            let received_data = match client::recieve(&mut reader).await {
                                Ok(data) => data,
                                Err(_) => return,
                            };
                            let response = tokio::task::block_in_place(|| {
                                self_for_conn.conn_to_function(received_data)
                            });
                            let _ = client::send_preserialize(&response, &mut reader).await;
                        });
                    }
                    Err(e) => {
                        log::error!("Incoming connection failed: {e}");
                    }
                    },
                    _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
                }
            }
        });

        // Store the thread handle so the host application can join it later on shutdown
        // The task is aborted by Drop, which also releases the listener.
        *self.local_server.lock().unwrap() = Some(handle);
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
