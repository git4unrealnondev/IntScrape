use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use intscrape::{
    db::turso::TursoDatabase,
    db::{
        SYSTEM_DATABASE_BACKUP_SITE, SYSTEM_DATABASE_SLURP_SITE, SYSTEM_FILE_HASH_SITE,
        SYSTEM_FILE_SIZE_SITE, SYSTEM_STORAGE_CHECK_SITE,
    },
    ipc::IpcServer,
    plugins::PluginManager,
    web::manager::DownloadsManager,
};
use fast_log::{
    consts::LogSize,
    plugin::{
        file_split::{KeepType, Rolling, RollingType},
        packer::LogPacker,
    },
};
use rayon::ThreadPoolBuilder;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Default data paths. Each can be overridden with an environment variable
/// so the daemon does not have to be run from the source tree:
/// `INTSCRAPE_DB_PATH`, `INTSCRAPE_LOG_PATH`, `INTSCRAPE_PLUGINS_PATH`.
const DEFAULT_DB_PATH: &str = "turso.db";
const DEFAULT_LOG_PATH: &str = "log.txt";
const DEFAULT_PLUGINS_PATH: &str = "compiled_plugins";
const JOB_PROCESSING_CHUNK_SIZE: usize = 256;
/// Default number of tokio worker threads (see `worker_threads()`).
const DEFAULT_WORKER_THREADS: usize = 16;

fn env_path(name: &str, default: &str) -> PathBuf {
    std::env::var(name)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(default))
}

/// Number of tokio worker threads. Override with `INTSCRAPE_WORKER_THREADS`
/// for machines with very different core counts than the original host.
fn worker_threads() -> usize {
    std::env::var("INTSCRAPE_WORKER_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_WORKER_THREADS)
}

///
/// Sets up logging in the environment. The log file is appended to (and
/// rotated by size via `file_split`), never deleted on startup, so logs
/// from previous runs survive a restart.
///
fn setup_log(log_path: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    fast_log::init(
        fast_log::Config::new()
            .level(log::LevelFilter::Info)
            .file_split(
                log_path
                    .to_str()
                    .ok_or("Failed to convert log_path to string")?,
                Rolling::new(RollingType::BySize(LogSize::GB(1))), // Split log when it hits 1 GB
                KeepType::KeepNum(10), // Keep exactly 10 chunks (10 x 1 GB = 10 GB)
                LogPacker {},
            )
            .chan_len(Some(4096)),
    )?;
    Ok(())
}

fn main() {
    // The whole daemon (all plugin jobs, the turso write layer, and every
    // reqwest/TLS connection future) shares this runtime. 4 workers was too few:
    // when the DB layer saturated them, in-flight TLS handshakes stopped being
    // polled for tens of seconds and the servers killed the stalled connections
    // ("tls handshake eof"). Give the runtime headroom so network futures are
    // always serviced promptly; the exact count is tunable via
    // INTSCRAPE_WORKER_THREADS (default 16).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    if let Err(error) = runtime.block_on(run_app()) {
        eprintln!("intscrape: {error}");
        std::process::exit(1);
    }
}

async fn run_app() -> Result<(), Box<dyn Error + Send + Sync>> {
    let db_path = env_path("INTSCRAPE_DB_PATH", DEFAULT_DB_PATH);
    let log_path = env_path("INTSCRAPE_LOG_PATH", DEFAULT_LOG_PATH);
    let plugins_path = env_path("INTSCRAPE_PLUGINS_PATH", DEFAULT_PLUGINS_PATH);

    // ctrl C handler
    let should_exit = Arc::new(AtomicBool::new(false));

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    setup_log(&log_path)?;

    let heavy_processing_pool = Arc::new(
        ThreadPoolBuilder::new()
            .thread_name(|index| format!("intscrape-processing-{index}"))
            .build()?,
    );

    let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
    log::info!("Turso database ready at {}", db_path.display());

    let plugin_manager =
        PluginManager::new(&plugins_path, db.clone(), should_exit.clone());
    db.set_plugin_manager(plugin_manager.clone());

    let download_manager = DownloadsManager::new(
        db.clone(),
        plugin_manager.clone(),
        heavy_processing_pool.clone(),
        should_exit.clone(),
    );

    // The `ipc_channel_count` database setting controls how many independent
    // IPC sockets (channels) are started; see `client::IPC_CHANNELS_SETTING`.
    let ipc_channels = db
        .setting_get_sync(client::IPC_CHANNELS_SETTING)
        .await
        .and_then(|setting| setting.num)
        .map(|count| count as usize)
        .unwrap_or(client::IPC_CHANNEL_DEFAULT);
    log::info!("Starting {ipc_channels} IPC channels");

    let ipc_server = IpcServer::new(
        db.clone(),
        should_exit.clone(),
        plugin_manager.clone(),
        ipc_channels,
    );

    // handler for ctrl c
    {
        let should_exit_clone = should_exit.clone();
        ctrlc::set_handler(move || {
            println!("received Ctrl+C!");
            log::info!("received Ctrl+C!");
            should_exit_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        })?;
    }

    // Container runtimes use SIGTERM for a normal stop. Ctrl-C handlers do not
    // receive it, so handle it explicitly to allow SQLite to close cleanly.
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    plugin_manager.callback_on_start();

    // Does the CLI input processing
    intscrape::cli::main(db.clone(), plugin_manager.clone()).await;

    // Handles adding jobs into system
    let plugin_manager_clone = plugin_manager.clone();
    let download_manager_clone = download_manager.clone();
    let db_spawn = db.clone();
    let active_system_jobs = Arc::new(AtomicUsize::new(0));
    let active_system_jobs_clone = active_system_jobs.clone();
    let should_exit_for_spawner = should_exit.clone();
    let mut spawner = tokio::task::spawn(async move {
        loop {
            if should_exit_for_spawner.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            // A slurp no longer owns the database exclusively: it commits in
            // short BEGIN IMMEDIATE batches and other jobs run alongside it.
            // Only *new* slurp jobs are deferred while one runs (a second bulk
            // import would fight over the Tags_slurp staging table and the
            // per-stage cursors); every other system job still claims work so
            // maintenance keeps up during a multi-hour import. WAL-mode
            // readers no longer pin an MVCC snapshot that stalls the import's
            // WAL truncation, so the poll itself is safe to run.
            let mut sites = plugin_manager_clone.get_storage_sites();
            sites.push(SYSTEM_DATABASE_BACKUP_SITE.to_string());
            if !db_spawn.is_slurping() {
                sites.push(SYSTEM_DATABASE_SLURP_SITE.to_string());
            }
            sites.push(SYSTEM_FILE_SIZE_SITE.to_string());
            sites.push(SYSTEM_FILE_HASH_SITE.to_string());
            sites.push(SYSTEM_STORAGE_CHECK_SITE.to_string());
            let jobs_to_run = db_spawn
                .jobs_get_torun_chunk(sites, JOB_PROCESSING_CHUNK_SIZE)
                .await;

            for job in jobs_to_run
                .iter()
                .filter(|job| intscrape::db::system_jobs::is_system_job(job))
            {
                if should_exit_for_spawner.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                // Claim the job before spawning it. This prevents the next
                // polling pass from launching a duplicate maintenance run.
                if !db_spawn.job_set_is_running(job).await {
                    log::error!(
                        "Skipping system job {} because it could not be claimed",
                        job.id
                    );
                    continue;
                }
                active_system_jobs_clone.fetch_add(1, Ordering::SeqCst);
                let db = db_spawn.clone();
                let active_system_jobs = active_system_jobs_clone.clone();
                let job = job.clone();
                tokio::spawn(async move {
                    db.run_system_job(&job).await;
                    active_system_jobs.fetch_sub(1, Ordering::SeqCst);
                });
            }

            if should_exit_for_spawner.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            let jobs_to_run: Vec<_> = jobs_to_run
                .into_iter()
                .filter(|job| !intscrape::db::system_jobs::is_system_job(job))
                .collect();

            download_manager_clone
                .add_jobs(plugin_manager_clone.match_plugin(&jobs_to_run))
                .await;

            if download_manager_clone.all_jobs_complete().await
                && jobs_to_run.is_empty()
                && active_system_jobs_clone.load(Ordering::SeqCst) == 0
                && plugin_manager_clone.are_start_threads_closed()
            {
                break;
            }

            // Checks loop every second
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    #[cfg(unix)]
    tokio::select! {
        result = &mut spawner => result?,
        _ = terminate.recv() => {
            log::info!("received SIGTERM");
            should_exit.store(true, Ordering::SeqCst);
            spawner.await?;
        }
    }

    #[cfg(not(unix))]
    spawner.await?;

    // Maintenance tasks are independent of the download manager, but still
    // need to finish before the database and process resources are dropped.
    let shutdown_deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
    while active_system_jobs.load(Ordering::SeqCst) != 0 {
        if tokio::time::Instant::now() >= shutdown_deadline {
            log::error!(
                "Shutdown timeout reached with {} system jobs still active",
                active_system_jobs.load(Ordering::SeqCst)
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Ctrl-C stops new work first; allow active downloads to finish and release
    // their in-progress URL guards before the manager is dropped.
    while !download_manager.downloads_complete().await
        || !download_manager.all_jobs_complete().await
    {
        if tokio::time::Instant::now() >= shutdown_deadline {
            log::error!("Shutdown timeout reached with downloads or workers still active");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Ensures that everything gets dropped properly
    drop(plugin_manager);
    drop(heavy_processing_pool);
    drop(ipc_server);
    drop(download_manager);

    // Call shutdown before db exit
    db.shutdown().await;
    drop(db);

    Ok(())
}
