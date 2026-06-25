use std::{path::Path, sync::Arc, time::Duration};

use crate::{db::MainDatabase, plugins::PluginManager, web::manager::DownloadsManager};

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

pub mod cli;
pub mod db;
pub mod helper_functions;
pub mod plugins;
pub mod web;

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
    console_subscriber::init();
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    setup_log();

    let heavy_processing_pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());

    let db = MainDatabase::new(Path::new(DB_PATH));

    let plugin_manager = PluginManager::new(Path::new(PLUGINS_PATH), db.clone());

    let download_manager = DownloadsManager::new(
        db.clone(),
        plugin_manager.clone(),
        heavy_processing_pool.clone(),
    );

    // Does the CLI input processing
    cli::main(db.clone()).await;

    // Handles adding jobs into system
    let db_spawn = db.clone();
    let spawner = tokio::task::spawn(async move {
        loop {
            let jobs_to_run = db_spawn.jobs_get_torun().await;

            download_manager
                .add_jobs(plugin_manager.match_plugin(jobs_to_run))
                .await;

            if download_manager.all_jobs_complete().await {
                break;
            }

            // Checks loop every second
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    spawner.await.unwrap();
}
