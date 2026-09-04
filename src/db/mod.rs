use core::{convert::Into, option::Option::Some};
use parking_lot::{Mutex, RwLock};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::{SqliteConnectionManager, rusqlite::Connection};
use rayon::ThreadPool;
use shared_types::{DbSettingsObj, Tag};
use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::atomic::AtomicBool,
    time::Duration,
};

use crate::{
    Arc, DB_VERSION,
    db::{
        roaring::{InternalCacheType, RelationshipStorage},
        tag_search::TagSearchCache,
    },
    plugins::PluginManager,
};

pub mod main;
mod old_code;
pub mod roaring;
pub(crate) mod system_jobs;
mod tag_search;
mod update_handler;

pub const SYSTEM_DATABASE_BACKUP_SITE: &str = "SYSTEM_BACKUP";
pub const SYSTEM_DATABASE_SLURP_SITE: &str = "SYSTEM_DB_SLURP";
pub const SYSTEM_FILE_SIZE_SITE: &str = "SYSTEM_FILE_SIZE";
pub const SYSTEM_FILE_HASH_SITE: &str = "SYSTEM_FILE_HASH";
pub const SYSTEM_STORAGE_CHECK_SITE: &str = "SYSTEM_STORAGE_CHECK";
pub const SYSTEM_STORAGE_CHECK_FILENAME_MODE: &str = "filename";

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
    setting_cache: Arc<RwLock<HashMap<String, DbSettingsObj>>>,
    tag_cache: Arc<RwLock<TagCache>>,
    tag_search_cache: Arc<RwLock<TagSearchCache>>,
    cache_type: Arc<RwLock<CacheType>>,
    relationship_roaring_storage: Arc<RwLock<Option<RelationshipStorage>>>,
    plugin_manager: Arc<RwLock<Option<Arc<PluginManager>>>>,
    heavy_processing_pool: Arc<ThreadPool>,
    should_exit: Arc<AtomicBool>,
}

const TAG_CACHE_LIMIT: usize = 100_000;

pub(crate) struct TagCache {
    entries: HashMap<u64, (shared_types::Tag, u64)>,
    order: VecDeque<(u64, u64)>,
    next_generation: u64,
}

impl TagCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
        }
    }

    fn get(&mut self, tag_id: u64) -> Option<shared_types::Tag> {
        let tag = self.entries.get(&tag_id).map(|(tag, _)| tag.clone());
        if let Some(tag) = tag {
            self.insert(tag_id, tag.clone());
            Some(tag)
        } else {
            None
        }
    }

    fn insert(&mut self, tag_id: u64, tag: shared_types::Tag) {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        self.entries.insert(tag_id, (tag, generation));
        self.order.push_back((tag_id, generation));
        while self.order.len() > TAG_CACHE_LIMIT {
            if let Some((oldest_id, oldest_generation)) = self.order.pop_front()
                && self
                    .entries
                    .get(&oldest_id)
                    .is_some_and(|(_, generation)| *generation == oldest_generation)
            {
                self.entries.remove(&oldest_id);
            }
        }
    }
}

#[cfg(test)]
mod tag_cache_tests {
    use super::{TAG_CACHE_LIMIT, TagCache};
    use shared_types::{GenericNamespaceObj, Tag};

    fn tag(name: &str) -> Tag {
        Tag {
            name: name.into(),
            namespace: GenericNamespaceObj {
                name: "test".into(),
                description: None,
            },
        }
    }

    #[test]
    fn cache_is_bounded_and_keeps_recent_entries() {
        let mut cache = TagCache::new();
        cache.insert(1, tag("one"));
        assert_eq!(cache.get(1).unwrap().name, "one");

        for id in 2..=(TAG_CACHE_LIMIT as u64) {
            cache.insert(id, tag("tag"));
        }

        assert!(cache.get(1).is_some());
        cache.insert(TAG_CACHE_LIMIT as u64 + 1, tag("new"));
        assert!(cache.get(2).is_none());
    }
}

impl MainDatabase {
    #[must_use]
    pub fn new(
        db_path: &Path,
        heavy_processing_pool: Arc<ThreadPool>,
        should_exit: Arc<AtomicBool>,
    ) -> Arc<Self> {
        let manager = SqliteConnectionManager::file(db_path).with_init(|c| {
            /*c.trace(Some(|statement: &str| {
                log::info!("Executing SQL: {}", statement);
            }));*/
            // Bound genuine lockups without turning ordinary writer contention
            // into an immediate database failure.
            c.busy_timeout(Duration::from_secs(30))?;
            c.execute_batch(
                "
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
 PRAGMA busy_timeout = 30000;
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
            setting_cache: Arc::new(RwLock::new(HashMap::new())),
            tag_cache: Arc::new(RwLock::new(TagCache::new())),
            tag_search_cache: Arc::new(RwLock::new(TagSearchCache::default())),
            cache_type: Arc::new(RwLock::new(CacheType::Bare)),
            relationship_roaring_storage: Arc::new(RwLock::new(None)),
            writer_conn,
            plugin_manager: Arc::new(RwLock::new(None)),
            heavy_processing_pool,
            should_exit,
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
        let mut stmt = conn.prepare("SELECT id, name FROM Namespace").unwrap();
        let namespaces = stmt
            .query_map([], |row| {
                Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap();
        let mut cache = self.namespace_cache.write();
        // Used for tag cache
        let mut reverse_cache = HashMap::new();

        for namespace in namespaces.flatten() {
            reverse_cache.insert(namespace.0, namespace.1.clone());
            cache.insert(namespace.1, namespace.0);
        }

        // Loads initial tag cache
        let mut cache_cache = self.tag_cache.write();
        let mut stmt = conn
            .prepare("SELECT id, name, namespace FROM High_Value_Tags LIMIT ?1;")
            .unwrap();

        let loaded_caches = stmt
            .query_map([TAG_CACHE_LIMIT], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                ))
            })
            .unwrap();

        for (tag_id, name, tag_namespace_id) in loaded_caches.flatten() {
            if let Some(namespace_name) = reverse_cache.get(&tag_namespace_id) {
                cache_cache.insert(
                    tag_id,
                    Tag {
                        name,
                        namespace: shared_types::GenericNamespaceObj {
                            name: namespace_name.clone(),
                            description: None,
                        },
                    },
                );
            }
        }

        let mut stmt = conn
            .prepare("SELECT name, description, num, param FROM Settings;")
            .unwrap();

        let obj = serde_rusqlite::from_rows::<shared_types::DbSettingsObj>(stmt.raw_query());

        let mut setting_cache = self.setting_cache.write();
        for setting in obj.flatten() {
            setting_cache.insert(setting.name.clone(), setting);
        }
    }

    /// Checks to see if the DB exists
    fn check_db(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let mut pool = self.writer_conn.lock();
        let mut conn = pool.transaction().unwrap();

        loop {
            if let Ok(Some(db_version_setting)) = self.internal_setting_get(&conn, "SYSTEM_VERSION")
            {
                if let Some(db_version_local) = db_version_setting.num {
                    if db_version_local != DB_VERSION {
                        log::warn!(
                            "Local db version: {db_version_local} does not match system_version: {DB_VERSION} will attempt an upgrade."
                        );

                        match db_version_local {
                            1 => self.internal_update_db_1_to_2(&conn)?,
                            2 => self.internal_update_db_2_to_3(&conn)?,
                            3 => self.internal_update_db_3_to_4(&conn)?,
                            4 => self.internal_update_db_4_to_5(&conn)?,
                            5 => self.internal_update_db_5_to_6(&conn)?,
                            _ => break,
                        }
                        conn.commit()?;
                        pool.execute("VACUUM;", [])?;
                        conn = pool.transaction().unwrap();
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
        self.internal_table_create_relationship_v1(&conn);
        self.internal_table_create_tag_search_fts_v6(&conn).unwrap();
        self.internal_file_download_location_set_default(&conn)
            .unwrap();
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_jobs_ready_priority
             ON Jobs (site, is_running, priority DESC, time, id);",
        )?;

        // Resetting is_running to false
        MainDatabase::internal_jobs_reset_isrunning(&conn).unwrap();

        self.internal_load_caching(&conn);

        conn.commit().unwrap();

        Ok(())
    }

    ///
    /// Creates the initial version of the DB at the file location
    ///
    fn create_initial_db(&self, conn: &Connection) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        MainDatabase::internal_table_create_namespace_v1(conn);
        self.internal_table_create_tags_v1(conn);

        // Added in DB Version 2
        MainDatabase::internal_table_create_dead_urls_v1(conn)?;

        self.internal_table_create_relationship_v1(conn);
        MainDatabase::internal_table_create_parents_v1(conn);

        MainDatabase::internal_table_create_settings_v1(conn);

        MainDatabase::internal_table_create_file_storage_locations_v1(conn);
        MainDatabase::internal_table_create_file_v2(conn);
        MainDatabase::internal_table_create_file_hashes_v1(conn);

        MainDatabase::internal_table_create_jobs_v1(conn);
        RelationshipStorage::internal_table_relationship_cache_create_v1(conn);
        self.internal_db_version_set(conn, DB_VERSION)?;
        self.internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_API_URL".into(),
                description: Some("Current way for external hosts to connect".into()),
                num: None,
                param: Some("127.0.0.1:3030".into()),
            },
        )
        .unwrap();
        self.internal_setting_set(
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
        self.internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_audit_log_enabled".into(),
                description: Some("Whether database changes are recorded in AuditLog.".into()),
                num: Some(1),
                param: None,
            },
        )?;

        self.internal_setting_set(
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
        self.internal_setting_set(
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
        self.internal_setup_default_cache(conn);
        Ok(())
    }
}
