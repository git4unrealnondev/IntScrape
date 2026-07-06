use core::{convert::Into, option::Option::Some};
use parking_lot::RwLock;
use r2d2::Pool;
use r2d2_sqlite::{SqliteConnectionManager, rusqlite::Connection};
use std::{collections::HashMap, path::Path};

use crate::{
    Arc, DB_VERSION,
    db::roaring::{InternalCacheType, RelationshipStorage},
};

pub mod main;
pub mod roaring;

pub enum CacheType {
    // Will be use to query the DB directly. No caching. DEFAULT OPTION
    Bare,
    // New cache method for relationships
    RelationshipRoaring(InternalCacheType),
}

pub struct MainDatabase {
    pool: Pool<SqliteConnectionManager>,
    namespace_cache: Arc<RwLock<HashMap<String, u64>>>,
    cache_type: RwLock<CacheType>,
    relationship_roaring_storage: RwLock<Option<RelationshipStorage>>,
}

impl Drop for MainDatabase {
    fn drop(&mut self) {
        if let Ok(conn) = self.pool.get() {
            if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                log::error!("Failed to checkpoint WAL file during drop: {:?}", e);
            }
        } else {
            log::error!("Failed to get a DB connection in time. Very Weird...")
        }
    }
}

impl MainDatabase {
    pub fn new(db_path: &Path) -> Arc<Self> {
        let manager = SqliteConnectionManager::file(db_path).with_init(|c| {
            //c.trace(Some(|statement: &str| {
            //    info!("Executing SQL: {}", statement);
            //}));

            c.execute_batch(
                "
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA cache_size = -64000;
",
            )
        });

        // Enable WAL mode inside the initialization if desired
        let pool = Pool::builder()
            .max_size(10) // up to 10 simultaneous connections
            .build(manager)
            .expect("Failed to create pool");

        let main_db: Arc<MainDatabase> = MainDatabase {
            pool,
            namespace_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_type: CacheType::Bare.into(),
            relationship_roaring_storage: None.into(),
        }
        .into();

        main_db.clone().check_db().unwrap();

        main_db.load_cache();

        main_db
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

        if let Ok(Some(_)) = Self::internal_setting_get(&conn, "SYSTEM_VERSION") {
        } else {
            self.create_initial_db(&conn);
        }

        Self::internal_file_download_location_set_default(&conn).unwrap();

        // Resetting is_running to false
        Self::internal_jobs_reset_isrunning(&conn).unwrap();

        Self::internal_load_caching(self.clone(), &conn);

        conn.commit().unwrap();

        Ok(())
    }

    ///
    /// Creates the initial version of the DB at the file location
    ///
    fn create_initial_db(&self, conn: &Connection) {
        Self::internal_table_create_namespace_v1(conn);
        Self::internal_table_create_tags_v1(conn);

        Self::internal_table_create_relationship_v1(conn);
        Self::internal_trigger_create_relationship_v1(conn);
        Self::internal_table_create_parents_v1(conn);

        Self::internal_table_create_settings_v1(conn);

        Self::internal_table_create_file_storage_locations_v1(conn);
        Self::internal_table_create_file_v1(conn);

        Self::internal_table_create_jobs_v1(conn);
        RelationshipStorage::internal_table_relationship_cache_create_v1(conn);

        Self::internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".into(),
                description: Some("Current version that the DB is on.".into()),
                num: Some(DB_VERSION),
                param: None,
            },
        )
        .unwrap();
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
        )
        .unwrap();

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
        )
        .unwrap();
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
        )
        .unwrap();
        Self::internal_setup_default_cache(conn);
    }
}
