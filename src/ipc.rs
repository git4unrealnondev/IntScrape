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
    use std::time::{Duration, Instant};
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
        let (server, _db, dir) = test_environment_with_db(channels).await;
        (server, dir)
    }

    /// As `test_environment_with`, but also hands back the database so a
    /// benchmark can seed rows the plugin path would have written.
    async fn test_environment_with_db(
        channels: usize,
    ) -> (Arc<IpcServer>, Arc<TursoDatabase>, tempfile::TempDir) {
        client::set_ipc_channel_count(channels);
        clean_sockets();
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        let plugins_path = temp_dir.path().join("plugins");
        std::fs::create_dir(&plugins_path).unwrap();
        let plugin_manager = PluginManager::new(&plugins_path, db.clone(), should_exit.clone());
        let server = IpcServer::new(db.clone(), should_exit, plugin_manager, channels);
        wait_for_sockets().await;
        (server, db, temp_dir)
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

        // The FTS index only covers popular tags (count >= 5), so raise this
        // one past the threshold through the same count machinery that real
        // tag floods use (`tag_counts_apply` also mirrors the tag into the
        // `Tags_Popular` search shadow).
        let red_fox_id: i64 = {
            let mut stmt = server
                .db
                .connect()
                .unwrap()
                .prepare("SELECT id FROM Tags WHERE name = 'red fox'")
                .await
                .unwrap();
            stmt.query_row(()).await.unwrap().get(0).unwrap()
        };
        server
            .db
            .tag_counts_apply(
                &std::collections::HashMap::from([(red_fox_id as u64, 5)]),
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();

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
        let female_id: i64 = {
            let mut stmt = server
                .db
                .connect()
                .unwrap()
                .prepare("SELECT id FROM Tags WHERE name = 'female'")
                .await
                .unwrap();
            stmt.query_row(()).await.unwrap().get(0).unwrap()
        };
        server
            .db
            .tag_counts_apply(
                &std::collections::HashMap::from([(female_id as u64, 5)]),
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
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

    /// The per-request cost of the IPC layer, which is what a plugin pays once per
    /// file when it walks a scraped account one file at a time.
    ///
    /// Every request opens a fresh `LocalSocketStream` connection
    /// (`init_data_request_async_on_channel`), so the cost being measured here is
    /// connect + send + receive + close, not any database work. That is the whole
    /// point: a meganz-style scrape issues `get_tag_file` and `put_tags_to_file`
    /// per file, and if the round trip dominates, no amount of batching *inside*
    /// the database will matter.
    ///
    /// Compares three shapes at the same request count:
    /// - one request at a time (what a sequential plugin loop does)
    /// - the same requests pipelined concurrently
    /// - the same total work issued as a single bulk request
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual benchmark: IPC round-trip cost per request"]
    async fn ipc_round_trip_bench() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (server, _temp_dir) = test_environment().await;
        assert!(all_sockets_ready(), "IPC sockets were not created");
        let iters: usize = std::env::var("BENCH_IPC_ITERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);

        // Warmup, so socket creation and page-cache effects are not in the number.
        for i in 0..10 {
            auto_settings_request()
                .await
                .unwrap_or_else(|e| panic!("warmup request {i} failed: {e}"));
        }

        let started = Instant::now();
        for i in 0..iters {
            let _ = settings_request(i % 4).await.unwrap();
        }
        let sequential = started.elapsed();
        println!(
            "\nIPC: {iters} sequential requests: {:.2} ms total, {:.3} ms/request",
            sequential.as_secs_f64() * 1000.0,
            sequential.as_secs_f64() * 1000.0 / iters as f64
        );

        // Same number of requests, all in flight at once. If this is much
        // faster, the cost is per-request latency that batching would hide; if it
        // is the same, something is serializing the server side.
        let started = Instant::now();
        let handles: Vec<_> = (0..iters)
            .map(|i| tokio::spawn(async move { settings_request(i % 4).await }))
            .collect();
        for handle in handles {
            let _ = handle.await.unwrap().unwrap();
        }
        let concurrent = started.elapsed();
        println!(
            "IPC: {iters} concurrent requests: {:.2} ms total, {:.3} ms/request",
            concurrent.as_secs_f64() * 1000.0,
            concurrent.as_secs_f64() * 1000.0 / iters as f64
        );
        println!(
            "    sequential/concurrent = {:.2}x",
            sequential.as_secs_f64() / concurrent.as_secs_f64().max(f64::MIN_POSITIVE)
        );

        drop(server);
        clean_sockets();
    }

    /// Why every statement that materializes a row out of `Tags` is slow, and
    /// whether the declared foreign keys are the cause.
    ///
    /// `tag_counts_apply` costs ~2.2 s per call at 20k tags, and every
    /// relationship write funnels into it behind one lock, so this is what makes
    /// a meganz scrape spend ~2 s per file. Isolating it gave numbers that do
    /// not fit a query-shape explanation:
    ///
    /// | statement on `Tags`                        | cost    |
    /// |--------------------------------------------|---------|
    /// | `SELECT id FROM Tags WHERE id = ?` (hit)   | ~116 ms |
    /// | `SELECT id FROM Tags WHERE id = ?` (miss)  | 0.34 ms |
    /// | `SELECT 1 FROM Tags WHERE id = ?` (hit)    | ~116 ms |
    /// | `UPDATE` over 9 `Tags` rows                | ~1076 ms|
    /// | the same lookup on a control table         | 0.076 ms|
    ///
    /// A miss is cheap and a hit is not, and the projection does not matter, so
    /// the cost is in materializing the row rather than finding it — and it
    /// applies to writes, so it is per row touched rather than per statement.
    /// The control table rules out row width, the two indexes, MVCC history, and
    /// a stale snapshot: it was copied from `Tags`, so it has the same long tag
    /// names and the same indexes, filled the same way.
    ///
    /// One structural difference is left: `Tags` declares three foreign keys and
    /// the control declares none. That accounts for the whole table above — a
    /// miss touches no row so no parent key is resolved, a hit resolves the
    /// parent even for `SELECT 1`, and a control with no key is fast.
    ///
    /// So this compares a control against the same table with keys added, then
    /// rebuilds `Tags` itself without them to check the conclusion end to end
    /// instead of by inference.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual benchmark: is the Tags foreign key the slow part"]
    async fn tags_row_touch_cost_bench() {
        let _socket_lock = SOCKET_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (server, db, _temp_dir) = test_environment_with_db(4).await;
        assert!(all_sockets_ready(), "IPC sockets were not created");
        let tag_count: u64 = std::env::var("BENCH_TAGS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000);
        let reps: usize = std::env::var("BENCH_REPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);

        let ns = shared_types::GenericNamespaceObj {
            name: "probe_ns".to_string(),
            description: None,
        };
        let storage_id = 1u64;
        let map: std::collections::HashMap<
            shared_types::FileManager,
            Vec<shared_types::FileTagAction>,
        > = (0..tag_count)
            .map(|i| {
                (
                    shared_types::FileManager {
                        internal: shared_types::FileInternal {
                            id: None,
                            hash: format!("probe{i:056x}"),
                            extension: "bin".into(),
                            storage_id,
                            size_bytes: Some(1),
                        },
                        identifying_hashes: vec![],
                    },
                    vec![shared_types::FileTagAction {
                        operation: shared_types::TagOperation::Add,
                        tags: vec![shared_types::PluginTag {
                            tag: shared_types::Tag {
                                // Long names on purpose: `Tags` stores real
                                // metadata values (paths, MIME types) here, so
                                // the rows are wide and row width is the obvious
                                // first suspect for a per-row cost.
                                name: format!("a-plausibly-long-tag-name-{i:08}"),
                                namespace: ns.clone(),
                            },
                            ..Default::default()
                        }],
                    }],
                )
            })
            .collect();
        assert!(
            db.clone()
                .process_scraper(map, Vec::new(), "probe".into())
                .await,
            "failed to seed tags"
        );

        let probe_id = {
            let conn = db.connect().unwrap();
            let mut rows = conn.query("SELECT id FROM Tags LIMIT 1;", ()).await.unwrap();
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .get::<i64>(0)
                .unwrap()
        };

        // Controls, each copied from `Tags` so they hold the same rows at the
        // same width with the same indexes. The only thing that varies is
        // whether they declare the foreign key.
        let variants: [(&str, &str, &str); 3] = [
            ("control: no foreign key", "probe_plain", ""),
            (
                "control: + FOREIGN KEY",
                "probe_fk",
                "FOREIGN KEY (namespace) REFERENCES Namespace(id)",
            ),
            (
                "control: + FK ON DELETE/UPDATE CASCADE",
                "probe_fk_cascade",
                "FOREIGN KEY (namespace) REFERENCES Namespace(id) ON DELETE CASCADE ON UPDATE CASCADE",
            ),
        ];
        for (label, name, fk) in variants {
            let conn = db.connect().unwrap();
            // The FK clause goes last and is omitted entirely when empty, since
            // a dangling comma is a syntax error.
            let fk_clause = if fk.is_empty() {
                String::new()
            } else {
                format!(",\n                     {fk}")
            };
            conn.execute_batch(&format!(
                "DROP TABLE IF EXISTS {name};
                 CREATE TABLE {name} (
                     id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     namespace INTEGER NOT NULL,
                     count INTEGER NOT NULL DEFAULT 0,
                     UNIQUE(name, namespace){fk_clause}
                 );
                 CREATE INDEX idx_{name}_covering ON {name}(count DESC, name, namespace);
                 INSERT INTO {name} (id, name, namespace, count)
                     SELECT id, name, namespace, count FROM Tags;"
            ))
            .await
            .unwrap();
            println!(
                "  {label:<42} {:>9.3} ms",
                pk_read_ms(&db, name, probe_id, reps).await
            );
        }

        println!(
            "  {:<42} {:>9.3} ms",
            "Tags, as shipped",
            pk_read_ms(&db, "Tags", probe_id, reps).await
        );

        // The earlier fixture that measured 116 ms per lookup had a *second*
        // namespace in it (the noise files used their own), so `Tags` had a
        // foreign key pointing at more than one parent row, and the table held
        // ids interleaved across both. Re-measure with that shape before
        // concluding anything from the numbers above.
        {
            let ns2 = shared_types::GenericNamespaceObj {
                name: "probe_ns_two".to_string(),
                description: None,
            };
            let extra: std::collections::HashMap<
                shared_types::FileManager,
                Vec<shared_types::FileTagAction>,
            > = (0..tag_count)
                .map(|i| {
                    (
                        shared_types::FileManager {
                            internal: shared_types::FileInternal {
                                id: None,
                                hash: format!("probe2{i:056x}"),
                                extension: "bin".into(),
                                storage_id,
                                size_bytes: Some(1),
                            },
                            identifying_hashes: vec![],
                        },
                        vec![shared_types::FileTagAction {
                            operation: shared_types::TagOperation::Add,
                            tags: vec![shared_types::PluginTag {
                                tag: shared_types::Tag {
                                    name: format!("a-plausibly-long-tag-name-{i:08}"),
                                    namespace: ns2.clone(),
                                },
                                ..Default::default()
                            }],
                        }],
                    )
                })
                .collect();
            assert!(
                db.clone()
                    .process_scraper(extra, Vec::new(), "probe2".into())
                    .await,
                "failed to seed the second namespace"
            );
            println!(
                "  {:<42} {:>9.3} ms",
                "Tags, after a 2nd namespace was added",
                pk_read_ms(&db, "Tags", probe_id, reps).await
            );
        }

        // Namespace count. Every `Tags` row carries a foreign key to `Namespace`, and
        // the earlier slow fixture created a namespace *per tag batch*, so the
        // parent table had tens of thousands of rows. If resolving that key
        // costs per parent row, this is the trigger — and it would also explain
        // why a control table with the same `Tags` rows was fast when it pointed
        // at a 3-row `Namespace`.
        {
            let conn = db.connect().unwrap();
            let mut rows = conn.query("SELECT COUNT(*) FROM Namespace;", ()).await.unwrap();
            let n: i64 = rows
                .next()
                .await
                .unwrap()
                .map(|r| r.get(0).unwrap())
                .unwrap_or(0);
            println!("  Namespace rows: {n}");
        }
        // Add a namespace per tag and watch the lookup.
        {
            let batch = 2_000usize;
            for b in 0..batch {
                let ns = shared_types::GenericNamespaceObj {
                    name: format!("ns_{b:05}"),
                    description: None,
                };
                let one: std::collections::HashMap<
                    shared_types::FileManager,
                    Vec<shared_types::FileTagAction>,
                > = (0..10)
                    .map(|i| {
                        (
                            shared_types::FileManager {
                                internal: shared_types::FileInternal {
                                    id: None,
                                    hash: format!("nss{b:05}_{i:03}{:0>51}", ""),
                                    extension: "bin".into(),
                                    storage_id,
                                    size_bytes: Some(1),
                                },
                                identifying_hashes: vec![],
                            },
                            vec![shared_types::FileTagAction {
                                operation: shared_types::TagOperation::Add,
                                tags: vec![shared_types::PluginTag {
                                    tag: shared_types::Tag {
                                        name: format!("nss-tag-{b:05}-{i:03}"),
                                        namespace: ns.clone(),
                                    },
                                    ..Default::default()
                                }],
                            }],
                        )
                    })
                    .collect();
                if !db
                    .clone()
                    .process_scraper(one, Vec::new(), "ns".into())
                    .await
                {
                    panic!("seeding namespace batch {b} failed");
                }
            }
            let conn = db.connect().unwrap();
            let mut rows = conn.query("SELECT COUNT(*) FROM Namespace;", ()).await.unwrap();
            let n: i64 = rows
                .next()
                .await
                .unwrap()
                .map(|r| r.get(0).unwrap())
                .unwrap_or(0);
            println!("  Namespace rows: {n}");
            println!(
                "  {:<42} {:>9.3} ms",
                "Tags, after 2000 namespaces",
                pk_read_ms(&db, "Tags", probe_id, reps).await
            );
        }

// Write traffic. The fixture that measured 116 ms per `Tags` lookup had
        // already been through `tag_counts_apply` and a bulk tag write; this one
        // has only been seeded. If the lookup degrades after writes, the cost
        // is not the table's shape but something the writes leave behind.
        {
            let mut deltas = std::collections::HashMap::new();
            for i in 0..9u64 {
                deltas.insert(1 + i, 1u64);
            }
            for round in 0..20 {
                let _ = db.tag_counts_apply(&deltas, &Default::default()).await;
                if round == 0 {
                    println!(
                        "  {:<42} {:>9.3} ms",
                        "Tags, after 1 tag_counts_apply",
                        pk_read_ms(&db, "Tags", probe_id, reps).await
                    );
                }
            }
            println!(
                "  {:<42} {:>9.3} ms",
                "Tags, after 20 tag_counts_apply",
                pk_read_ms(&db, "Tags", probe_id, reps).await
            );
        }

        // The fix, checked rather than inferred: rebuild `Tags` without its
        // foreign keys and read it again under identical conditions.
        {
            let conn = db.connect().unwrap();
            conn.execute_batch(
                "CREATE TABLE Tags_nofk (
                     id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     namespace INTEGER NOT NULL,
                     count INTEGER NOT NULL DEFAULT 0,
                     UNIQUE(name, namespace)
                 );
                 INSERT INTO Tags_nofk (id, name, namespace, count)
                     SELECT id, name, namespace, count FROM Tags;
                 DROP TABLE Tags;
                 ALTER TABLE Tags_nofk RENAME TO Tags;
                 CREATE INDEX idx_tags_count_covering ON Tags(count DESC, name, namespace);",
            )
            .await
            .unwrap();
            println!(
                "  {:<42} {:>9.3} ms",
                "Tags, foreign keys dropped",
                pk_read_ms(&db, "Tags", probe_id, reps).await
            );
        }

        drop(server);
        clean_sockets();
    }

    /// Milliseconds per primary-key hit on `table`, prepared once and repeated.
    async fn pk_read_ms(db: &Arc<TursoDatabase>, table: &str, id: i64, reps: usize) -> f64 {
        let conn = db.connect().unwrap();
        let mut stmt = conn
            .prepare(&format!("SELECT id FROM {table} WHERE id = ?1"))
            .await
            .unwrap();
        // Warm, so the number is execution and not first-touch page faults.
        {
            let mut rows = stmt
                .query(turso::params_from_iter([turso::Value::from(id)]))
                .await
                .unwrap();
            while let Ok(Some(_)) = rows.next().await {}
        }
        let started = Instant::now();
        let mut found = 0;
        for _ in 0..reps {
            let mut rows = stmt
                .query(turso::params_from_iter([turso::Value::from(id)]))
                .await
                .unwrap();
            while let Ok(Some(_)) = rows.next().await {
                found += 1;
            }
        }
        assert!(found > 0, "{table} must contain the probe id");
        started.elapsed().as_secs_f64() * 1000.0 / reps as f64
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
