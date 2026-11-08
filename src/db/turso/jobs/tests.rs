//! Tests for the `jobs` module.
//!
//! Extracted from `jobs.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `jobs` module itself.

use super::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

async fn new_db() -> (Arc<TursoDatabase>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("target.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    (
        TursoDatabase::new_with_exit(&db_path, should_exit).await,
        temp_dir,
    )
}

fn slurp_job() -> PluginJob {
    PluginJob {
        time: 0,
        reptime: 0,
        priority: i64::MAX as u64,
        recreation: None,
        site: "SYSTEM_DB_SLURP".to_string(),
        param: vec![ScraperParam::Normal("/tmp/source.db".to_string())],
        user_data: BTreeMap::new(),
    }
}

/// Reproduces the runtime parse error from the webui slurp adder:
/// `ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint`
/// when `jobs_add_single` inserts the SYSTEM_DB_SLURP job through the
/// normal connection/builder path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_job_upsert_parses() {
    let (db, _keep) = new_db().await;
    let id = db.jobs_add_single(slurp_job()).await;
    assert_ne!(id, 0, "jobs_add_single failed to insert the slurp job");
}

/// Same as [`Self::slurp_job_upsert_parses`], but against a copy of the
/// real development database, which is where the runtime error surfaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_job_upsert_parses_on_dev_db_copy() {
    let temp_dir = tempfile::tempdir().unwrap();
    for suffix in ["", "-wal", "-log", "-shm"] {
        let source = PathBuf::from(format!("/home/linux/rust/IntScrape/turso.db{suffix}"));
        if source.exists() {
            std::fs::copy(&source, temp_dir.path().join(format!("turso.db{suffix}"))).unwrap();
        }
    }
    let db_path = temp_dir.path().join("turso.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
    let id = db.jobs_add_single(slurp_job()).await;
    assert_ne!(id, 0, "jobs_add_single failed on the dev db copy");
}
