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

#[cfg(test)]
static IPC_TEST_SETTING_STARTED: AtomicBool = AtomicBool::new(false);

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
                        // Each connection gets its own task so clients can make
                        // independent requests concurrently.
                        let self_for_conn = self_clone.clone();

                        tokio::spawn(async move {
                            let mut reader = BufReader::new(conn);
                            let received_data = match client::recieve(&mut reader).await {
                                Ok(data) => data,
                                Err(_) => return,
                            };

                            // Database and plugin dispatch are synchronous and may wait on
                            // SQLite or plugin code. Keep that work off Tokio's IO workers so
                            // other IPC connections remain responsive while it runs.
                            let response = match run_blocking(move || {
                                self_for_conn.conn_to_function(received_data)
                            })
                            .await
                            {
                                Ok(response) => response,
                                Err(_) => return,
                            };
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
            action => {
                #[cfg(test)]
                if matches!(action, client::SupportedDBRequests::SettingsSet(_)) {
                    IPC_TEST_SETTING_STARTED.store(true, Ordering::Release);
                }
                self.db
                    .dispatch_ipc_request(action)
                    .unwrap_or_else(|| client::data_size_to_b(&false))
            }
        }
    }
}

async fn run_blocking<F>(operation: F) -> Result<Vec<u8>, tokio::task::JoinError>
where
    F: FnOnce() -> Vec<u8> + Send + 'static,
{
    tokio::task::spawn_blocking(operation).await
}

#[cfg(test)]
mod tests {
    use super::{IPC_TEST_SETTING_STARTED, IpcServer};
    use crate::db::MainDatabase;
    use crate::plugins::PluginManager;
    use rayon::ThreadPoolBuilder;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, OnceLock, mpsc};
    use std::time::Duration;
    use tempfile::tempdir;

    const SOCKET_PATH: &str = "/tmp/rusthydrus/rusthydrus.sock";
    static SOCKET_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn long_ipc_task_does_not_block_another_task() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _ = std::fs::remove_file(SOCKET_PATH);
        IPC_TEST_SETTING_STARTED.store(false, std::sync::atomic::Ordering::Release);
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let processing_pool = Arc::new(ThreadPoolBuilder::new().build().unwrap());
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = MainDatabase::new(&db_path, processing_pool, should_exit.clone());
        let plugins_path = temp_dir.path().join("plugins");
        std::fs::create_dir(&plugins_path).unwrap();
        let plugin_manager = PluginManager::new(&plugins_path, db.clone(), should_exit.clone());
        let server = IpcServer::new(db, should_exit, plugin_manager);

        for _ in 0..20 {
            if Path::new(SOCKET_PATH).exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            Path::new(SOCKET_PATH).exists(),
            "IPC socket was not created"
        );

        // Hold SQLite's writer lock so the real setting write remains blocked
        // inside the IPC worker until after the concurrent request is checked.
        let (lock_ready_sender, lock_ready_receiver) = mpsc::sync_channel(0);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let lock_path = db_path.clone();
        let lock_thread = std::thread::spawn(move || {
            let connection = r2d2_sqlite::rusqlite::Connection::open(lock_path).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            lock_ready_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            connection.execute_batch("ROLLBACK").unwrap();
        });
        lock_ready_receiver.recv().unwrap();

        let long_task = tokio::spawn(client::setting_set_async(shared_types::DbSettingsObj {
            name: "ipc_concurrency_test".to_string(),
            description: None,
            num: None,
            param: Some("blocked write".to_string()),
        }));
        for _ in 0..100 {
            if IPC_TEST_SETTING_STARTED.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(IPC_TEST_SETTING_STARTED.load(std::sync::atomic::Ordering::Acquire));

        let short_result =
            tokio::time::timeout(Duration::from_millis(200), client::should_exit_async())
                .await
                .expect("short IPC request was blocked by the long IPC request")
                .unwrap();

        assert!(!short_result);
        assert!(!long_task.is_finished());
        release_sender.send(()).unwrap();
        assert!(!long_task.await.unwrap().unwrap());
        lock_thread.join().unwrap();
        drop(server);
        let _ = std::fs::remove_file(SOCKET_PATH);
    }
}
