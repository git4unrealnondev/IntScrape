use std::{
    error::Error,
    path::Path,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use crate::{
    db::{MainDatabase, SYSTEM_DATABASE_BACKUP_SITE, SYSTEM_FILE_HASH_SITE, SYSTEM_FILE_SIZE_SITE},
    ipc::IpcServer,
    plugins::PluginManager,
    web::manager::DownloadsManager,
};
use rayon::ThreadPoolBuilder;

pub mod backup_path;
pub mod cli;
pub mod db;
pub mod helper_functions;
pub mod ipc;
pub mod plugins;
pub mod web;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const DB_PATH: &str = "main.db";
const LOG_PATH: &str = "log.txt";
pub const PLUGINS_PATH: &str = "compiled_plugins";
const DB_VERSION: u64 = 6;
const JOB_PROCESSING_CHUNK_SIZE: usize = 256;

///
/// Sets up logging in the environment
///
fn setup_log() -> Result<(), Box<dyn Error + Send + Sync>> {
    let log_path = Path::new(LOG_PATH);

    // Clears log
    if std::fs::exists(log_path)? {
        std::fs::remove_file(log_path)?;
    }

    // Sets up log
    fast_log::init(
        fast_log::Config::new()
            .level(log::LevelFilter::Info)
            .file(
                log_path
                    .to_str()
                    .ok_or("Failed to convert log_path to string")?,
            )
            .chan_len(Some(4096)),
    )?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // ctrl C handler
    let should_exit = Arc::new(AtomicBool::new(false));

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    setup_log()?;

    let heavy_processing_pool = Arc::new(
        ThreadPoolBuilder::new()
            .thread_name(|index| format!("intscrape-processing-{index}"))
            .build()?,
    );

    let db = MainDatabase::new(
        Path::new(DB_PATH),
        heavy_processing_pool.clone(),
        should_exit.clone(),
    );

    let plugins_path =
        std::env::var("INTSCRAPE_PLUGINS_PATH").unwrap_or_else(|_| PLUGINS_PATH.to_owned());
    let plugin_manager =
        PluginManager::new(Path::new(&plugins_path), db.clone(), should_exit.clone());
    db.set_plugin_manager(plugin_manager.clone());

    let download_manager = DownloadsManager::new(
        db.clone(),
        plugin_manager.clone(),
        heavy_processing_pool.clone(),
        should_exit.clone(),
    );

    let ipc_server = IpcServer::new(db.clone(), should_exit.clone(), plugin_manager.clone());

    // handler for ctrl c
    {
        let should_exit_clone = should_exit.clone();
        ctrlc::set_handler(move || {
            println!("received Ctrl+C!");
            log::info!("received Ctrl+C!");
            should_exit_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        })?;
        //.expect("Error setting Ctrl-C handler");
    }

    plugin_manager.callback_on_start();

    // Does the CLI input processing
    cli::main(db.clone()).await;

    // Handles adding jobs into system
    let plugin_manager_clone = plugin_manager.clone();
    let download_manager_clone = download_manager.clone();
    let db_spawn = db.clone();
    let spawner = tokio::task::spawn(async move {
        loop {
            if should_exit.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            let mut sites = plugin_manager_clone.get_storage_sites();
            sites.push(SYSTEM_DATABASE_BACKUP_SITE.to_string());
            sites.push(SYSTEM_FILE_SIZE_SITE.to_string());
            sites.push(SYSTEM_FILE_HASH_SITE.to_string());
            let jobs_to_run = db_spawn
                .jobs_get_torun_chunk(sites, JOB_PROCESSING_CHUNK_SIZE)
                .await;

            for job in jobs_to_run
                .iter()
                .filter(|job| crate::db::system_jobs::is_system_job(job))
            {
                db_spawn.run_system_job(job).await;
            }

            let jobs_to_run: Vec<_> = jobs_to_run
                .into_iter()
                .filter(|job| !crate::db::system_jobs::is_system_job(job))
                .collect();

            download_manager_clone
                .add_jobs(plugin_manager_clone.match_plugin(&jobs_to_run))
                .await;

            if download_manager_clone.all_jobs_complete().await
                && jobs_to_run.is_empty()
                && plugin_manager_clone.are_start_threads_closed()
            {
                break;
            }

            // Checks loop every second
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    spawner.await?;

    // Ctrl-C stops new work first; allow active downloads to finish and release
    // their in-progress URL guards before the manager is dropped.
    while !download_manager.downloads_complete().await
        || !download_manager.all_jobs_complete().await
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Ensures that everything gets dropped properly
    drop(plugin_manager);
    drop(heavy_processing_pool);
    drop(ipc_server);
    drop(download_manager);

    // Call shutdown before db exit
    db.shutdown();
    drop(db);

    // Cleans up temp db files
    let _ = std::fs::remove_file("./main.db-wal");
    let _ = std::fs::remove_file("./main.db-shm");

    Ok(())
}
