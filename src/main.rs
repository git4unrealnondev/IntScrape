use std::{path::Path, sync::Arc, time::Duration};

use crate::{
    db::MainDatabase, ipc::IpcServer, plugins::PluginManager, web::manager::DownloadsManager,
};

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
const DB_VERSION: u64 = 1;

///
/// Sets up logging in the environment
///
fn setup_log() {
    let log_path = Path::new(LOG_PATH);

    // Clears log
    if std::fs::exists(log_path).unwrap() {
        std::fs::remove_file(log_path).expect("Unable to remove log.txt");
    }

    // Sets up log
    fast_log::init(
        fast_log::Config::new()
            .level(log::LevelFilter::Info)
            .file(log_path.to_str().unwrap())
            .chan_len(None),
    )
    .unwrap();
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    //console_subscriber::init();

    setup_log();

    let heavy_processing_pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());

    let db = MainDatabase::new(Path::new(DB_PATH));

    let plugin_manager = PluginManager::new(Path::new(PLUGINS_PATH), db.clone());

    let download_manager = DownloadsManager::new(
        db.clone(),
        plugin_manager.clone(),
        heavy_processing_pool.clone(),
    );

    let ipc_server = IpcServer::new(db.clone());

    plugin_manager.callback_on_start();

    // Does the CLI input processing
    cli::main(db.clone()).await;

    // Handles adding jobs into system
    let plugin_manager_clone = plugin_manager.clone();
    let download_manager_clone = download_manager.clone();
    let db_spawn = db.clone();
    let spawner = tokio::task::spawn(async move {
        loop {
            let sites = plugin_manager_clone.get_storage_sites();
            let jobs_to_run = db_spawn.jobs_get_torun(sites).await;

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

    spawner.await.unwrap();

    // Ensures that everything gets dropped properly
    drop(heavy_processing_pool);
    drop(ipc_server);
    drop(download_manager);
    drop(plugin_manager);
    drop(db);
}
