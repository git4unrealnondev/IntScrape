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

/// Regression: a pooled transaction that ends without being finished
/// (dropped after an error, or a commit that lost an MVCC conflict)
/// must not poison the connection for the next user. Turso's default
/// drop behaviour replays a rollback on the next query, which fails
/// with "cannot rollback - no transaction is active" when the
/// transaction already ended — and, because the flag only clears on a
/// successful rollback, every later query on that connection failed
/// identically (the jobs poller logging it every second, no job ever
/// running). Pooled transactions now drop with `Ignore`, so such a
/// connection is discarded (open transaction) or reused cleanly
/// (already-ended transaction) instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_transaction_does_not_poison_pooled_connection() {
    let (db, _keep) = new_db().await;

    // Begin a transaction on a pooled connection, end its underlying
    // transaction out from under the wrapper (what a failed COMMIT or
    // a statement auto-rollback does inside Limbo), then drop the
    // wrapper unfinished. With Turso's default drop behaviour this
    // left a dangling rollback on a now-autocommit connection, and
    // the next query's cleanup rollback failed with "cannot rollback
    // - no transaction is active" — permanently.
    {
        let mut conn = db.connect().unwrap();
        let tx = conn
            .transaction_with_behavior(TursoDatabase::write_tx_behavior())
            .await
            .unwrap();
        db.job_add_sql(&tx, &slurp_job()).await.unwrap();
        tx.execute("ROLLBACK", ()).await.unwrap();
        drop(tx); // never committed or rolled back via the wrapper
    }

    // The next checkout (possibly the same underlying connection) must
    // be able to query and write normally.
    let conn = db.connect().unwrap();
    let jobs = db
        .jobs_get_torun_chunk_sql(&conn, vec!["SYSTEM_DB_SLURP".to_string()], 10)
        .await
        .expect("query after dropped transaction must work");
    assert_eq!(jobs.len(), 0, "uncommitted job must not be visible");
    let id = db.jobs_add_single(slurp_job()).await;
    assert_ne!(id, 0, "writes after dropped transaction must work");
}

#[test]
fn rollback_state_errors_are_concurrency_conflicts() {
    // A COMMIT that lost an MVCC conflict can surface as a
    // transaction-state error instead of BusySnapshot; retry loops
    // must recognise it instead of giving up immediately.
    assert!(TursoDatabase::is_concurrency_conflict(
        &turso::Error::Error("cannot rollback - no transaction is active".to_string())
    ));
    assert!(TursoDatabase::is_concurrency_conflict(
        &turso::Error::Error("cannot commit - no transaction is active".to_string())
    ));
    // Deterministic SQL errors must not be retried as conflicts.
    assert!(!TursoDatabase::is_concurrency_conflict(
        &turso::Error::Error(
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint".to_string()
        )
    ));
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
