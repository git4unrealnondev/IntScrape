use core::{convert::Into, option::Option::Some};
use parking_lot::{Mutex, RwLock};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::{SqliteConnectionManager, rusqlite::Connection};
use std::{collections::HashMap, path::Path, time::Duration};

use crate::{
    Arc, DB_VERSION,
    db::roaring::{InternalCacheType, RelationshipStorage},
    plugins::PluginManager,
};

pub mod main;
mod old_code;
pub mod roaring;
mod tag_search;
mod update_handler;

pub const SYSTEM_DATABASE_BACKUP_SITE: &str = "__system_database_backup__";

pub enum CacheType {
    // Will be use to query the DB directly. No caching. DEFAULT OPTION
    Bare,
    // New cache method for relationships
    RelationshipRoaring(InternalCacheType),
}
#[derive(Clone)]
pub struct MainDatabase {
    pool: Pool<SqliteConnectionManager>,
    writer_conn: Arc<Mutex<PooledConnection<SqliteConnectionManager>>>,
    namespace_cache: Arc<RwLock<HashMap<String, u64>>>,
    tag_cache: Arc<RwLock<HashMap<u64, shared_types::Tag>>>,
    cache_type: Arc<RwLock<CacheType>>,
    relationship_roaring_storage: Arc<RwLock<Option<RelationshipStorage>>>,
    tag_search_cache: Arc<RwLock<tag_search::TagSearchCache>>,
    plugin_manager: Arc<RwLock<Option<Arc<PluginManager>>>>,
}

impl MainDatabase {
    #[must_use]
    pub fn new(db_path: &Path) -> Arc<Self> {
        let manager = SqliteConnectionManager::file(db_path).with_init(|c| {
            /*c.trace(Some(|statement: &str| {
                log::info!("Executing SQL: {}", statement);
            }));*/
            c.busy_timeout(Duration::from_secs(1))?;
            c.execute_batch(
                "
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 1000;
PRAGMA cache_size = -64000;
",
            )
        });

        // Enable WAL mode inside the initialization if desired
        let pool = Pool::builder()
            .max_size(10) // up to 10 simultaneous connections
            .build(manager)
            .expect("Failed to create pool");

        // Reserved writer thread to do all work on
        let writer_conn = Arc::new(Mutex::new(pool.get().unwrap()));

        let main_db: Arc<Self> = Self {
            pool,
            namespace_cache: Arc::new(RwLock::new(HashMap::new())),
            tag_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_type: Arc::new(RwLock::new(CacheType::Bare)),
            relationship_roaring_storage: Arc::new(RwLock::new(None)),
            tag_search_cache: Arc::new(RwLock::new(tag_search::TagSearchCache::default())),
            writer_conn,
            plugin_manager: Arc::new(RwLock::new(None)),
        }
        .into();

        main_db.clone().check_db().unwrap();

        main_db.load_cache();

        main_db
    }

    ///
    /// Manages the DB shutdown
    ///
    pub fn shutdown(&self) {
        let guard = self.writer_conn.lock();

        if let Err(e) = guard.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
            log::error!("Failed to checkpoint WAL file during drop: {e:?}");
        }
    }

    /// Creates or replaces an online SQLite backup at `destination`.
    pub fn backup_db_to(&self, destination: &Path) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                r2d2_sqlite::rusqlite::Error::ToSqlConversionFailure(error.into())
            })?;
        }

        let temporary = destination.with_extension("backup.tmp");
        let _ = std::fs::remove_file(&temporary);
        let temporary_string = temporary.to_string_lossy().into_owned();
        let guard = self.writer_conn.lock();
        guard.execute(
            "VACUUM INTO ?1",
            r2d2_sqlite::rusqlite::params![temporary_string],
        )?;
        drop(guard);

        std::fs::rename(&temporary, destination)
            .map_err(|error| r2d2_sqlite::rusqlite::Error::ToSqlConversionFailure(error.into()))?;
        Ok(())
    }

    ///
    /// Sets up the the namespace cache.
    /// Im assuming that theirs going to be relatively small of these. Less then 1k
    ///
    fn load_cache(&self) {
        let conn = self.pool.get().unwrap();
        for ns_id in 1..u64::MAX {
            match Self::internal_namespace_get_generic(&conn, &ns_id) {
                None => {
                    break;
                }
                Some(namespace) => {
                    self.namespace_cache.write().insert(namespace.name, ns_id);
                }
            }
        }
    }

    /// Checks to see if the DB exists
    fn check_db(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.pool.get()?;
        let conn = conn.transaction().unwrap();

        loop {
            if let Ok(Some(db_version_setting)) =
                Self::internal_setting_get(&conn, "SYSTEM_VERSION")
            {
                if let Some(db_version_local) = db_version_setting.num {
                    if db_version_local != DB_VERSION {
                        log::warn!(
                            "Local db version: {db_version_local} does not match system_version: {DB_VERSION} will attempt an upgrade."
                        );

                        match db_version_local {
                            1 => Self::internal_update_db_1_to_2(&conn)?,
                            2 => Self::internal_update_db_2_to_3(&conn)?,
                            3 => Self::internal_update_db_3_to_4(&conn)?,
                            _ => break,
                        }
                    } else {
                        break;
                    }
                }
            } else {
                self.create_initial_db(&conn)?;
                break;
            }
        }

        // Ensure additive schema changes are applied to databases already at the
        // current version as well as databases upgraded through a version step.
        Self::internal_table_create_relationship_v1(&conn);
        Self::internal_file_download_location_set_default(&conn).unwrap();

        // Resetting is_running to false
        Self::internal_jobs_reset_isrunning(&conn).unwrap();

        Self::internal_load_caching(self, &conn);

        conn.commit().unwrap();

        Ok(())
    }

    ///
    /// Creates the initial version of the DB at the file location
    ///
    fn create_initial_db(&self, conn: &Connection) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        Self::internal_table_create_namespace_v1(conn);
        Self::internal_table_create_tags_v1(conn);

        // Added in DB Version 2
        Self::internal_table_create_dead_urls_v1(conn)?;

        Self::internal_table_create_relationship_v1(conn);
        Self::internal_table_create_parents_v1(conn);

        Self::internal_table_create_settings_v1(conn);

        Self::internal_table_create_file_storage_locations_v1(conn);
        Self::internal_table_create_file_v1(conn);

        Self::internal_table_create_jobs_v1(conn);
        RelationshipStorage::internal_table_relationship_cache_create_v1(conn);
        Self::internal_db_version_set(conn, DB_VERSION)?;
        Self::internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_API_URL".into(),
                description: Some("Current way for external hosts to connect".into()),
                num: None,
                param: Some("127.0.0.1:3030".into()),
            },
        )
        .unwrap();
        Self::internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_DEFAULT_USER_AGENT".into(),
                description: Some(
                    "The default user agent to use when connecting to a site.".into(),
                ),
                num: None,
                param: Some("IntScrape V1.0".into()),
            },
        )?;
        Self::internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_audit_log_enabled".into(),
                description: Some("Whether database changes are recorded in AuditLog.".into()),
                num: Some(1),
                param: None,
            },
        )?;

        Self::internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_tag_count_popular_division".into(),
                description: Some(
                    "defines the division between popular tags an non popular tags".into(),
                ),
                num: Some(5),
                param: None,
            },
        )?;
        Self::internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_tag_count_popular_division_old".into(),
                description: Some(
                    "defines the division between popular tags an non popular tags. If different then new number then start migration inside of db".into(),
                ),
                num: Some(5),
                param: None,
            },
        )?;
        Self::internal_setup_default_cache(conn);
        Ok(())
    }
}
