use std::{
    collections::HashMap,
    future::Future,
    path::Path,
    sync::Arc,
};

use shared_types::DbSettingsObj;
use smol_str::SmolStr;
use tokio::sync::RwLock;
use turso::{Builder, Database, Result, transaction::Transaction};

use crate::plugins::PluginManager;
use crate::DB_VERSION;

mod cache;
mod dead_url;
mod file;
mod jobs;
mod main;
mod namespace;
mod processing;
mod relationship;
mod schema_current;
mod search;
mod slurp;
mod sql;
pub(crate) mod system_jobs;
mod tag;

mod api;
mod ipc;

const TAG_CACHE_LIMIT: usize = 100_000;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
/// used for the internal tag db structures
pub struct TagDb {
    pub id: i64,
    pub name: SmolStr,
    pub namespace_id: i64,
}

pub struct TursoDatabase {
    db: Database,
    db_path: String,
    namespace_cache: Arc<RwLock<HashMap<String, u64>>>,
    namespace_cache_reverse: Arc<RwLock<HashMap<u64, String>>>,
    setting_cache: Arc<RwLock<HashMap<String, DbSettingsObj>>>,
    plugin_manager: Arc<parking_lot::RwLock<Option<Arc<PluginManager>>>>,
    should_exit: Arc<std::sync::atomic::AtomicBool>,
}

impl TursoDatabase {
    /// Re-runs a complete database operation from a fresh connection until it
    /// succeeds or returns a non-MVCC error. The operation must include its
    /// full transaction when it needs one; retrying a partially executed
    /// transaction would reuse a stale snapshot.
    pub(in crate::db::turso) async fn retry_mvcc<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(in crate::db::turso) fn is_concurrency_conflict(error: &turso::Error) -> bool {
        let message = error.to_string().to_ascii_lowercase();
        matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
            || message.contains("busy")
            || message.contains("conflict")
    }

    /// Opens a Turso connection without installing a busy handler. Transaction
    /// conflicts are handled by the operation that owns the transaction.
    pub(in crate::db::turso) fn connect(&self) -> Result<turso::Connection> {
        self.db.connect()
    }

    /// Creates a db at the path
    pub async fn new(db_path: &Path) -> Arc<Self> {
        Self::new_with_exit(db_path, Arc::new(std::sync::atomic::AtomicBool::new(false))).await
    }

    /// Creates a db at the path, honoring a shared exit flag for system jobs.
    pub async fn new_with_exit(
        db_path: &Path,
        should_exit: Arc<std::sync::atomic::AtomicBool>,
    ) -> Arc<Self> {
        let create_db = !db_path.exists();

        let db = Builder::new_local(&db_path.to_string_lossy())
            //.experimental_vacuum(true)
            //.experimental_without_rowid(true)
            //.experimental_materialized_views(true)
            .experimental_index_method(true)
            .build()
            .await
            .unwrap();

        // MVCC allows BEGIN CONCURRENT transactions to overlap. The pragma is
        // required for the database itself; the passive-checkpoint builder
        // option alone does not enable MVCC.
        if let Ok(conn) = db.connect()
            && let Err(error) = conn.execute("PRAGMA journal_mode = 'mvcc';", ()).await
        {
            log::error!("Failed to enable Turso MVCC mode: {error}");
        }

        let out = TursoDatabase {
            db,
            db_path: db_path.to_string_lossy().to_string(),
            namespace_cache: Arc::new(RwLock::new(HashMap::new())),
            namespace_cache_reverse: Arc::new(RwLock::new(HashMap::new())),
            setting_cache: Arc::new(RwLock::new(HashMap::new())),
            plugin_manager: Arc::new(parking_lot::RwLock::new(None)),
            should_exit,
        };

        if create_db {
            let _ = out.create_db().await;
        }


        let _ = out.check_db().await;

        Arc::new(out)
    }

    /// Installs a plugin manager handle used for regex tag registration.
    pub fn set_plugin_manager(&self, plugin_manager_add: Arc<PluginManager>) {
        *self.plugin_manager.write() = Some(plugin_manager_add);
    }

    /// Whether a shared shutdown flag has been tripped.
    pub fn should_exit(&self) -> bool {
        self.should_exit.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Checks the db on first boot, manages updates
    async fn check_db(&self) -> Result<()> {
        let mut connection = self.connect()?;
        let conn = connection.transaction().await?;


        self.table_create_tags(&conn).await;

        // Resetting is_running to false on every boot, mirroring the legacy
        // database so a crash never leaves a job stuck as "running".
        self.jobs_reset_isrunning_sql(&conn).await?;

        // Keep existing databases on the ngram tokenizer as well.
        let _ = conn
            .execute_batch(
                "DROP INDEX IF EXISTS idx_tags_fts;\nCREATE INDEX IF NOT EXISTS idx_tags_fts ON Tags USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);\nOPTIMIZE INDEX idx_tags_fts;",
            )
            .await;

        conn.pragma_update("journal_mode", "'mvcc'").await?;

        conn.commit().await?;

        self.load_cache().await?;

        Ok(())
    }

    /// Handles creation of db
    async fn create_db(&self) -> Result<()> {
        let mut connection = self.connect()?;

        let conn = connection.transaction().await?;
        self.table_create_tags(&conn).await;
        self.table_create_filestoragelocations(&conn).await;
        self.table_create_filehash(&conn).await;
        self.table_create_file(&conn).await;
        self.table_create_settings(&conn).await;
        self.table_create_dead_urls(&conn).await;
        self.table_create_parents(&conn).await;
        self.table_create_namespace(&conn).await;
        self.table_create_jobs(&conn).await;

        self.discover_initial_settings(&conn).await?;

        conn.commit().await?;

        Ok(())
    }

    /// Bakes in the same defaults the legacy database starts with, minus the
    /// removed audit-log setting. The audit-trail tables were dropped, so no
    /// `SYSTEM_audit_log_enabled` entry is created here.
    async fn discover_initial_settings(&self, conn: &Transaction<'_>) -> Result<()> {
        self.setting_set(
            conn,
            DbSettingsObj {
                name: "SYSTEM_VERSION".into(),
                description: Some("Current version that the DB is on.".into()),
                num: Some(DB_VERSION),
                param: None,
            },
        )
        .await?;
        self.setting_set(
            conn,
            DbSettingsObj {
                name: "SYSTEM_API_URL".into(),
                description: Some("Current way for external hosts to connect".into()),
                num: None,
                param: Some("127.0.0.1:3030".into()),
            },
        )
        .await?;
        self.setting_set(
            conn,
            DbSettingsObj {
                name: "SYSTEM_DEFAULT_USER_AGENT".into(),
                description: Some("The default user agent to use when connecting to a site.".into()),
                num: None,
                param: Some("IntScrape V1.0".into()),
            },
        )
        .await?;
        self.setting_set(
            conn,
            DbSettingsObj {
                name: "SYSTEM_tag_count_popular_division".into(),
                description: Some(
                    "defines the division between popular tags an non popular tags".into(),
                ),
                num: Some(5),
                param: None,
            },
        )
        .await?;
        self.setting_set(
            conn,
            DbSettingsObj {
                name: "SYSTEM_tag_count_popular_division_old".into(),
                description: Some(
                    "defines the division between popular tags an non popular tags. If different then new number then start migration inside of db".into(),
                ),
                num: Some(5),
                param: None,
            },
        )
        .await?;
        Ok(())
    }

    /// Manages the DB shutdown
    pub async fn shutdown(&self) {
        let conn = match self.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to open connection while shutting down: {error}");
                return;
            }
        };

        // FTS index rows are replayed by turso's schema reload on the next
        // open. A clean shutdown drops the index first (check_db recreates it
        // on the next boot), so a previously-crashed session can never be
        // bricked by a stale/duplicated idx_tags_fts schema row.
        if let Err(error) = conn.execute("DROP INDEX IF EXISTS idx_tags_fts;", ()).await {
            log::error!("Failed to drop FTS index while shutting down: {error}");
        }

        // `PRAGMA wal_checkpoint(TRUNCATE)` returns a result row, so it has to
        // be drained through `query` instead of `execute`.
        if let Err(error) = conn.query("PRAGMA wal_checkpoint(TRUNCATE);", ()).await {
            log::error!("Failed to checkpoint WAL file during shutdown: {error}");
        }
    }

    /// Loads the database into the cache
    async fn load_cache(&self) -> Result<()> {
        let conn = self.connect()?;

        self.namespace_load(&conn).await?;
        self.settings_load(&conn).await?;

        Ok(())
    }
}
