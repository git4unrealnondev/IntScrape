use crate::db::turso::TursoDatabase;
use crate::plugins::PluginManager;
use client::channel_socket_path;
use interprocess::local_socket::ToFsName;
use interprocess::local_socket::{GenericFilePath, ListenerOptions, tokio::prelude::*};
use log::info;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::io::BufReader;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

#[cfg(test)]
static ACCEPTED_CHANNELS: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// Maximum number of requests served concurrently on any one channel.
///
/// Each channel owns its own socket, accept loop and permit pool. When a
/// channel is fully occupied (e.g. by a long-running task), further requests
/// on *that* channel queue until a permit frees up, while every other channel
/// keeps serving. The cap also bounds how many handlers can hit the database
/// at once, so a burst on one socket cannot starve the write path.
const CHANNEL_CONCURRENCY: usize = 8;

pub struct IpcServer {
    local_servers: Mutex<Vec<JoinHandle<()>>>,
    should_exit: Arc<AtomicBool>,
    db: Arc<TursoDatabase>,
    plugin_manager: Arc<PluginManager>,
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        self.should_exit
            .store(true, std::sync::atomic::Ordering::Relaxed);

        for local_server in self.local_servers.lock().unwrap().drain(..) {
            local_server.abort();
        }
    }
}

impl IpcServer {
    pub fn new(
        db: Arc<TursoDatabase>,
        should_exit: Arc<AtomicBool>,
        plugin_manager: Arc<PluginManager>,
        channels: usize,
    ) -> Arc<Self> {
        // Keep the client's view of the channel count in sync with the sockets
        // we are about to create (the caller derived `channels` from the
        // `ipc_channel_count` database setting, or the default).
        client::set_ipc_channel_count(channels);
        let out = Arc::new(Self {
            local_servers: Mutex::new(Vec::new()),
            should_exit,
            db,
            plugin_manager,
        });

        out.clone().startup(channels);
        out
    }

    /// Starts up the api.
    ///
    /// The host listens on `channels` independent sockets ("channels") — one
    /// accept loop each — so a long-running request only occupies the channel
    /// it arrived on and never blocks the others. The count comes from the
    /// `ipc_channel_count` database setting (default
    /// `client::IPC_CHANNEL_DEFAULT`; clamped by
    /// [`client::set_ipc_channel_count`]); clients route each request to the
    /// least-loaded channel.
    pub fn startup(self: Arc<Self>, channels: usize) {
        let _ = std::fs::create_dir("/tmp/rusthydrus");

        for channel in 0..channels {
            let path = channel_socket_path(channel);
            let _ = std::fs::remove_file(&path);

            let name = match path.clone().to_fs_name::<GenericFilePath>() {
                Ok(name) => name,
                Err(error) => {
                    log::error!(
                        "Failed to build IPC channel {channel} socket name {path}: {error}"
                    );
                    continue;
                }
            };

            let listener = match ListenerOptions::new().name(name).create_tokio() {
                Ok(listener) => listener,
                Err(error) => {
                    log::error!(
                        "Failed to start Tokio IPC listener on channel {channel} ({path}): {error}"
                    );
                    continue;
                }
            };

            let self_for_loop = self.clone();
            let handle = tokio::spawn(async move {
                self_for_loop.accept_loop(listener, channel).await;
            });
            self.local_servers.lock().unwrap().push(handle);
        }
    }

    /// Accepts connections on one IPC channel until shutdown. Each connection
    /// gets its own task so clients can make independent requests concurrently,
    /// even within the same channel.
    async fn accept_loop(
        self: Arc<Self>,
        listener: interprocess::local_socket::tokio::Listener,
        channel: usize,
    ) {
        // Bounds concurrency per channel (see `CHANNEL_CONCURRENCY`): requests
        // queue head-of-line on this channel only; the other channels are
        // served by their own loops and never wait on this semaphore.
        let channel_permits = Arc::new(Semaphore::new(CHANNEL_CONCURRENCY));

        loop {
            if self.should_exit.load(Ordering::Relaxed) {
                break;
            }
            // A slurp owns the database. Refuse new connections entirely
            // (instead of merely queueing them) so its BEGIN IMMEDIATE
            // transactions never wait on a UI reader; the override wraps
            // the database handle that both sides share.
            if self.db.ipc_is_paused() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }
            match listener.accept().await {
                Ok(conn) => {
                    #[cfg(test)]
                    ACCEPTED_CHANNELS.lock().unwrap().push(channel);
                    let self_for_conn = self.clone();
                    let permits = channel_permits.clone();
                    tokio::spawn(async move {
                        // Bind the permit with `let` so it lives for the whole
                        // connection task (match-arm bindings drop immediately).
                        let _permit = match permits.acquire().await {
                            Ok(permit) => permit,
                            // Semaphore closed on shutdown.
                            Err(_) => return,
                        };
                        self_for_conn.handle_connection(conn).await;
                    });
                }
                Err(e) => {
                    log::error!("IPC channel {channel} incoming connection failed: {e}");
                }
            }
        }
    }

    /// Serves a single client connection: read exactly one request, dispatch it,
    /// write the response, then close. Holding the connection open (or a slow
    /// request) only ties up its own task and channel.
    async fn handle_connection(self: Arc<Self>, conn: interprocess::local_socket::tokio::Stream) {
        self.db.ipc_task_started();
        let mut reader = BufReader::new(conn);
        let received_data = client::recieve(&mut reader).await;
        // Turso handlers are asynchronous. Poll the request
        // directly so one connection never consumes a
        // blocking-pool worker while it waits on the database.
        if let Ok(data) = received_data {
            let response = self.clone().conn_to_function(data).await;
            let _ = client::send_preserialize(&response, &mut reader).await;
        }
        self.db.ipc_task_finished();
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
                // work, so keep them off Tokio's executor threads. They ride a
                // randomly chosen channel, so a slow callback only occupies the
                // channel it landed on.
                let plugin_manager = self.plugin_manager.clone();
                let out = tokio::task::spawn_blocking(move || {
                    plugin_manager.external_plugin_call(&key, &callback_info)
                })
                .await
                .unwrap_or_default();

                client::data_size_to_b(&out)
            }
            action => self
                .db
                .dispatch_ipc_request_async(action)
                .await
                .unwrap_or_else(|| client::data_size_to_b(&false)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ACCEPTED_CHANNELS, CHANNEL_CONCURRENCY, IpcServer};
    use crate::db::turso::TursoDatabase;
    use crate::plugins::PluginManager;
    use client::{channel_socket_path, init_data_request_async_on_channel, ipc_channel_count};
    use interprocess::local_socket::{GenericFilePath, ToFsName, tokio::prelude::*};
    use shared_types::DbSettingsObj;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;
    use tempfile::tempdir;

    static SOCKET_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    /// Removes every IPC channel socket so a fresh server starts clean.
    fn clean_sockets() {
        for channel in 0..ipc_channel_count() {
            let _ = std::fs::remove_file(channel_socket_path(channel));
        }
    }

    /// Whether every IPC channel socket has been created yet.
    fn all_sockets_ready() -> bool {
        (0..ipc_channel_count()).all(|channel| Path::new(&channel_socket_path(channel)).exists())
    }

    async fn wait_for_sockets() {
        for _ in 0..50 {
            if all_sockets_ready() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn test_environment() -> (Arc<IpcServer>, tempfile::TempDir) {
        test_environment_with(4).await
    }

    /// Fresh server + DB with exactly `channels` IPC sockets.
    async fn test_environment_with(channels: usize) -> (Arc<IpcServer>, tempfile::TempDir) {
        client::set_ipc_channel_count(channels);
        clean_sockets();
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        let plugins_path = temp_dir.path().join("plugins");
        std::fs::create_dir(&plugins_path).unwrap();
        let plugin_manager = PluginManager::new(&plugins_path, db.clone(), should_exit.clone());
        let server = IpcServer::new(db, should_exit, plugin_manager, channels);
        wait_for_sockets().await;
        (server, temp_dir)
    }

    /// One request over the given explicit channel, wrapped in a timeout.
    async fn settings_request(
        channel: usize,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        init_data_request_async_on_channel::<bool, _>(
            client::SupportedDBRequests::SettingsSet(DbSettingsObj {
                name: format!("ipc_channel_{channel}"),
                description: None,
                num: None,
                param: Some("x".to_string()),
            }),
            channel,
        )
        .await
    }

    /// One settings request over the default (least-loaded) channel.
    async fn auto_settings_request() -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        init_data_request_async_on_channel::<bool, _>(
            client::SupportedDBRequests::SettingsSet(DbSettingsObj {
                name: "ipc_auto_setting".to_string(),
                description: None,
                num: None,
                param: Some("x".to_string()),
            }),
            client::IPC_CHANNEL_AUTO,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipc_requests_on_separate_connections_are_handled_concurrently() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (server, _temp_dir) = test_environment().await;
        assert!(all_sockets_ready(), "IPC sockets were not created");

        // Fire a database write and a lifecycle read concurrently. They travel
        // over separate connections (possibly different channels) and must
        // each complete without blocking the other, matching how independent
        // plugin/UI requests are served.
        let (write_result, exit_result) = tokio::join!(
            client::setting_set_async(shared_types::DbSettingsObj {
                name: "ipc_concurrency_test".to_string(),
                description: None,
                num: None,
                param: Some("concurrent write".to_string()),
            }),
            tokio::time::timeout(Duration::from_millis(200), client::should_exit_async(),)
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
        clean_sockets();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tag_search_over_ipc_returns_populated_results() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (server, _temp_dir) = test_environment().await;
        assert!(all_sockets_ready(), "IPC sockets were not created");

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
        clean_sockets();
    }

    /// A fully occupied channel must not block the others: consume every
    /// permit on channel 0 with idle connections, then confirm channel 1 keeps
    /// answering while a channel-0 request queues, and that channel 0 recovers
    /// once the idle connections are released.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_channel_does_not_block_other_channels() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (server, _temp_dir) = test_environment().await;
        assert!(all_sockets_ready(), "IPC sockets were not created");

        let held_name = channel_socket_path(0)
            .to_fs_name::<GenericFilePath>()
            .unwrap();

        // Occupy every permit on channel 0: each server task grabs a permit,
        // then waits forever for request bytes, pinning the channel.
        let mut held = Vec::with_capacity(CHANNEL_CONCURRENCY);
        for _ in 0..CHANNEL_CONCURRENCY {
            held.push(
                LocalSocketStream::connect(held_name.clone())
                    .await
                    .expect("failed to connect to IPC channel 0"),
            );
        }
        // Give every accept-loop task time to take its permit.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // A request pinned to the saturated channel queues behind the held
        // opens and must time out.
        let blocked = tokio::time::timeout(Duration::from_millis(200), settings_request(0)).await;
        assert!(
            blocked.is_err(),
            "saturated channel served a queued request"
        );

        // The other channels keep answering while channel 0 is fully occupied.
        let quick = tokio::time::timeout(Duration::from_millis(300), settings_request(1))
            .await
            .expect("another IPC channel was blocked by a saturated channel")
            .unwrap();
        assert!(quick, "request on an idle channel failed");

        // Freeing the held connections lets channel 0 serve again.
        drop(held);
        let released = tokio::time::timeout(Duration::from_millis(300), settings_request(0))
            .await
            .expect("channel 0 did not recover after its permits were released")
            .unwrap();
        assert!(released, "recovered channel 0 request failed");

        drop(server);
        clean_sockets();
    }

    /// `set_ipc_channel_count` clamps to the valid range; the default is the
    /// requested 10.
    #[test]
    fn ipc_channel_count_clamps_to_valid_range() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        for (value, expected) in [(1, 1), (2, 2), (7, 7), (64, 64), (0, 1), (999, 64)] {
            client::set_ipc_channel_count(value);
            assert_eq!(client::ipc_channel_count(), expected, "count={value}");
        }
        // Restore the value the other IPC tests rely on.
        client::set_ipc_channel_count(4);
    }

    /// The `ipc_channel_count` database setting drives how many sockets the
    /// host creates and the client count agrees, mirroring the production
    /// startup path in `main.rs`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn database_setting_controls_channel_count() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        clean_sockets();
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;

        // Store the setting, exactly as `main.rs` reads it back.
        assert!(
            db.setting_set_sync(&DbSettingsObj {
                name: client::IPC_CHANNELS_SETTING.to_string(),
                description: None,
                num: Some(3),
                param: None,
            })
            .await
        );
        let channels = db
            .setting_get_sync(client::IPC_CHANNELS_SETTING)
            .await
            .and_then(|setting| setting.num)
            .unwrap() as usize;
        assert_eq!(channels, 3, "setting was not read back");

        let plugins_path = temp_dir.path().join("plugins");
        std::fs::create_dir(&plugins_path).unwrap();
        let plugin_manager = PluginManager::new(&plugins_path, db.clone(), should_exit.clone());
        let server = IpcServer::new(db, should_exit, plugin_manager, channels);
        wait_for_sockets().await;

        assert_eq!(client::ipc_channel_count(), 3);
        for channel in 0..3 {
            assert!(
                Path::new(&channel_socket_path(channel)).exists(),
                "channel {channel} socket missing"
            );
        }
        assert!(
            !Path::new(&channel_socket_path(3)).exists(),
            "server created more channels than the setting allows"
        );

        drop(server);
        clean_sockets();
        client::set_ipc_channel_count(4);
    }

    /// Default traffic is spread across every channel by the least-loaded
    /// picker: with a few hundred sequential requests, each channel's accept
    /// loop must see work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn requests_spread_across_all_channels() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (server, _temp_dir) = test_environment().await;
        assert!(all_sockets_ready(), "IPC sockets were not created");

        ACCEPTED_CHANNELS.lock().unwrap().clear();
        for _ in 0..200 {
            assert!(
                !client::should_exit().unwrap(),
                "unexpected should_exit true"
            );
        }

        let accepts = ACCEPTED_CHANNELS.lock().unwrap().clone();
        let channels = client::ipc_channel_count();
        assert_eq!(accepts.len(), 200, "every request must open one connection");
        for channel in 0..channels {
            let hits = accepts.iter().filter(|&&c| c == channel).count();
            assert!(
                hits > 0,
                "channel {channel} never received a request (got {accepts:?})"
            );
        }

        drop(server);
        clean_sockets();
    }

    /// Concurrent requests must not pile onto one channel: a burst spread over
    /// the least-loaded picker reaches every channel, so a busy channel never
    /// absorbs all the new work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_requests_spread_across_channels() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (server, _temp_dir) = test_environment().await;
        assert!(all_sockets_ready(), "IPC sockets were not created");

        ACCEPTED_CHANNELS.lock().unwrap().clear();
        let handles = (0..20)
            .map(|_| tokio::spawn(auto_settings_request()))
            .collect::<Vec<_>>();
        for handle in handles {
            assert!(
                handle.await.unwrap().unwrap(),
                "concurrent AUTO request failed"
            );
        }

        let accepts = ACCEPTED_CHANNELS.lock().unwrap().clone();
        assert_eq!(accepts.len(), 20, "every request must open one connection");
        let channels = client::ipc_channel_count();
        for channel in 0..channels {
            let hits = accepts.iter().filter(|&&c| c == channel).count();
            assert!(
                hits > 0,
                "concurrent requests never reached channel {channel} (got {accepts:?})"
            );
        }

        drop(server);
        clean_sockets();
    }
}
