use crate::db::turso::TursoDatabase;
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
    db: Arc<TursoDatabase>,
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
        db: Arc<TursoDatabase>,
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

                            // Turso handlers are asynchronous. Poll the request
                            // directly so one connection never consumes a
                            // blocking-pool worker while it waits on the database.
                            let response = self_for_conn.conn_to_function(received_data).await;
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
    async fn conn_to_function(self: Arc<Self>, action: client::SupportedDBRequests) -> Vec<u8> {
        match action {
            client::SupportedDBRequests::LoggingNoPrint(data) => {
                info!("IPC LOG: {data}");
                client::data_size_to_b(&false)
            }
            client::SupportedDBRequests::ShouldExit => {
                client::data_size_to_b(&self.should_exit.load(Ordering::SeqCst))
            }
            client::SupportedDBRequests::ExternalPluginCall(key, callback_info) => {
                // Plugin callbacks are synchronous and may perform arbitrary
                // work, so keep them off Tokio's executor threads.
                let plugin_manager = self.plugin_manager.clone();
                let out = tokio::task::spawn_blocking(move || {
                    plugin_manager.external_plugin_call(&key, &callback_info)
                })
                .await
                .unwrap_or_default();

                client::data_size_to_b(&out)
            }
            action => {
                #[cfg(test)]
                if matches!(action, client::SupportedDBRequests::SettingsSet(_)) {
                    IPC_TEST_SETTING_STARTED.store(true, Ordering::Release);
                }
                self.db
                    .dispatch_ipc_request_async(action)
                    .await
                    .unwrap_or_else(|| client::data_size_to_b(&false))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IPC_TEST_SETTING_STARTED, IpcServer};
    use crate::db::turso::TursoDatabase;
    use crate::plugins::PluginManager;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;
    use tempfile::tempdir;

    const SOCKET_PATH: &str = "/tmp/rusthydrus/rusthydrus.sock";
    static SOCKET_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipc_requests_on_separate_connections_are_handled_concurrently() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _ = std::fs::remove_file(SOCKET_PATH);
        IPC_TEST_SETTING_STARTED.store(false, std::sync::atomic::Ordering::Release);
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
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

        // Fire a database write and a lifecycle read concurrently. They travel
        // over separate connections and must each complete without blocking
        // the other, matching how independent plugin/UI requests are served.
        let (write_result, exit_result) = tokio::join!(
            client::setting_set_async(shared_types::DbSettingsObj {
                name: "ipc_concurrency_test".to_string(),
                description: None,
                num: None,
                param: Some("concurrent write".to_string()),
            }),
            tokio::time::timeout(
                Duration::from_millis(200),
                client::should_exit_async(),
            )
        );

        assert!(
            write_result.unwrap(),
            "concurrent setting set over IPC failed"
        );
        let exit_result = exit_result
            .expect("short IPC request was blocked by the long IPC request")
            .unwrap();
        assert!(!exit_result);

        drop(server);
        let _ = std::fs::remove_file(SOCKET_PATH);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tag_search_over_ipc_returns_populated_results() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _ = std::fs::remove_file(SOCKET_PATH);
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
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

        // Add a tag over the IPC channel, mirroring how production populates tags.
        let added = client::tag_actions_add(vec![shared_types::FileTagAction {
            operation: shared_types::TagOperation::Add,
            tags: vec![shared_types::PluginTag {
                tag: shared_types::Tag {
                    name: "red fox".into(),
                    namespace: shared_types::GenericNamespaceObj {
                        name: "subject".into(),
                        description: None,
                    },
                },
                ..Default::default()
            }],
        }])
        .unwrap();
        assert!(added, "tag action add over IPC failed");

        // Typo + prefix queries must resolve over the same channel.
        let typo = client::search_tag_fts("red fxo".into(), Some(10)).unwrap();
        let prefix = client::search_tag_fts("red f".into(), Some(10)).unwrap();
        assert!(!typo.is_empty(), "typo search over IPC returned no results");
        assert!(
            !prefix.is_empty(),
            "prefix search over IPC returned no results"
        );

        let female_added = client::tag_actions_add(vec![shared_types::FileTagAction {
            operation: shared_types::TagOperation::Add,
            tags: vec![shared_types::PluginTag {
                tag: shared_types::Tag {
                    name: "female".into(),
                    namespace: shared_types::GenericNamespaceObj {
                        name: "subject".into(),
                        description: None,
                    },
                },
                ..Default::default()
            }],
        }])
        .unwrap();
        assert!(female_added, "female tag add over IPC failed");
        assert!(
            !client::search_tag_fts("fem".into(), Some(10))
                .unwrap()
                .is_empty(),
            "partial fem search over IPC returned no results"
        );

        drop(server);
        let _ = std::fs::remove_file(SOCKET_PATH);
    }
}
