use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::SystemTime,
};

use r2d2_sqlite::rusqlite::{self, Connection, Row, params};
use shared_types::*;

use crate::{db::MainDatabase, helper_functions::get_sys_time_in_secs};

pub trait DbJobsObjExt {
    fn from_row(row: &Row) -> rusqlite::Result<Self>
    where
        Self: Sized;
}

impl DbJobsObjExt for DbJobsObj {
    /// Parses a single database row directly into your clean memory structures
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        // Deserialize the JSON string columns back into native Rust types
        let param_raw: String = row.get("param")?;
        let user_data_raw: String = row.get("user_data")?;

        let param: Vec<ScraperParam> = serde_json::from_str(&param_raw).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                6, // Column index reference
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;

        let user_data: BTreeMap<String, String> =
            serde_json::from_str(&user_data_raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        // Reconstruct the inner PluginJob config block
        let config = PluginJob {
            time: row.get::<_, i64>("time")? as u64,
            reptime: row.get::<_, i64>("reptime")? as u64,
            priority: row.get::<_, i64>("priority")? as u64,
            site: row.get("site")?,
            param,
            user_data,
        };

        // Reconstruct the master database object
        Ok(DbJobsObj {
            id: row.get::<_, i64>("id")? as u64,
            isrunning: row.get::<_, bool>("is_running")?,
            config,
        })
    }
}

impl MainDatabase {
    ///
    /// Creates the relationship table for the db
    ///
    pub(in crate::db) fn internal_table_create_relationship_v1(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS Relationship (
    fileid INTEGER NOT NULL,
    tagid  INTEGER NOT NULL,

    PRIMARY KEY (fileid, tagid),

    FOREIGN KEY (fileid)
        REFERENCES File(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    FOREIGN KEY (tagid)
        REFERENCES Tags(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_tagid_fileid ON Relationship(tagid, fileid DESC);
",
        )
        .unwrap();
    }

    ///
    /// Handles creating the triggers to manage the count in the Tags column
    ///
    pub(in crate::db) fn internal_trigger_create_relationship_v1(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "
CREATE TRIGGER IF NOT EXISTS relationship_insert_count
AFTER INSERT ON Relationship
BEGIN
    UPDATE Tags
    SET count = count + 1
    WHERE id = NEW.tagid;
END;

CREATE TRIGGER IF NOT EXISTS relationship_delete_count
AFTER DELETE ON Relationship
BEGIN
    UPDATE Tags
    SET count = count - 1
    WHERE id = OLD.tagid;
END;

",
        )
        .unwrap();
    }

    ///
    /// Creates the current default Tags table
    ///
    pub(in crate::db) fn internal_table_create_tags_v1(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    name TEXT NOT NULL, 
    namespace INTEGER NOT NULL, 
    count INTEGER NOT NULL DEFAULT 0, 

    UNIQUE(name, namespace), 

    FOREIGN KEY (namespace) REFERENCES Namespace(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tags_count ON Tags(count DESC);
CREATE INDEX IF NOT EXISTS idx_tags_namespace ON Tags(namespace);

",
        )
        .unwrap();
    }
    ///
    /// Creates the current default Namespace table
    ///
    pub(in crate::db) fn internal_table_create_namespace_v1(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Namespace (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    name TEXT NOT NULL UNIQUE, 
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_namespace ON Namespace (name);

",
        )
        .unwrap();
    }
    ///
    /// Creates the current default Settings table
    ///
    pub(in crate::db) fn internal_table_create_settings_v1(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Settings (
    name TEXT PRIMARY KEY,
    description TEXT, 
    num INTEGER, 
    param TEXT
);",
        )
        .unwrap();
    }

    ///
    /// Creates the current default Parents table
    ///
    pub(in crate::db) fn internal_table_create_parents_v1(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Parents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id INTEGER NOT NULL,
    relate_tag_id INTEGER NOT NULL,
    limit_to INTEGER,

    FOREIGN KEY (tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (relate_tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (limit_to) REFERENCES Tags(id) ON DELETE SET NULL ON UPDATE CASCADE,

    CHECK (tag_id != relate_tag_id)
);

CREATE INDEX IF NOT EXISTS idx_parents_lim ON Parents (limit_to);
CREATE INDEX IF NOT EXISTS idx_parents_rel ON Parents (relate_tag_id);
CREATE INDEX IF NOT EXISTS idx_parents ON Parents (tag_id, relate_tag_id, limit_to);

",
        )
        .unwrap();
    }

    ///
    /// Stores file locaitons to an ID
    ///
    pub(in crate::db) fn internal_table_create_file_storage_locations_v1(
        conn: &rusqlite::Connection,
    ) {
        conn.execute_batch("
CREATE TABLE IF NOT EXISTS FileStorageLocations (id INTEGER PRIMARY KEY AUTOINCREMENT, location TEXT NOT NULL);

").unwrap();
    }

    ///
    /// Creates the default File table
    ///
    pub(in crate::db) fn internal_table_create_file_v1(conn: &rusqlite::Connection) {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS File 
            (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
            hash TEXT, 
            extension TEXT, 
            storage_id INTEGER, 

            CHECK (
                (hash IS NOT NULL AND extension IS NOT NULL) OR
                (hash IS NULL AND extension IS NULL)
            ),

            FOREIGN KEY (storage_id) REFERENCES FileStorageLocations(id) ON DELETE CASCADE ON UPDATE CASCADE
            );

CREATE INDEX IF NOT EXISTS idx_file_hash ON File (hash);
").unwrap();
    }

    ///
    /// Creates the default Jobs table
    ///
    pub(in crate::db) fn internal_table_create_jobs_v1(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
    time INTEGER NOT NULL, 
    reptime INTEGER NOT NULL, 
    priority INTEGER NOT NULL,  
    is_running BOOL NOT NULL DEFAULT False,
    manager TEXT NOT NULL, 
    site TEXT NOT NULL, 
    param TEXT NOT NULL, 
    user_data TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedup 
ON Jobs (time, reptime, site, param);
",
        )
        .unwrap();
    }

    ///
    /// Used internally to get a setting
    ///
    pub(in crate::db) fn internal_setting_get(
        rusqlite_conn: &rusqlite::Connection,
        name: &str,
    ) -> Result<Option<shared_types::DbSettingsObj>, rusqlite::Error> {
        let mut stmt = rusqlite_conn
            .prepare("SELECT name, description, num, param FROM settings WHERE name = ? LIMIT 1")?;

        let mut rows = stmt.query([name])?;

        if let Some(row) = rows.next()? {
            // Unpack using serde_rusqlite
            let obj = serde_rusqlite::from_row::<shared_types::DbSettingsObj>(row)
                .map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    ///
    /// Gets jobs that should be run
    ///
    pub(in crate::db) fn internal_jobs_get_torun(
        conn: &Connection,
    ) -> Result<Vec<DbJobsObj>, rusqlite::Error> {
        let mut out = Vec::new();
        for site in Self::internal_jobs_get_all_sites(conn)? {
            for job in Self::internal_jobs_get_site(conn, &site)? {
                // Filters the jobs so we only run jobs that should be run
                if job.config.time + job.config.reptime <= get_sys_time_in_secs() && !job.isrunning
                {
                    out.push(job);
                }
            }
        }

        Ok(out)
    }

    ///
    /// Sets ALL jobs to be not running
    ///
    pub(in crate::db) fn internal_jobs_reset_isrunning(
        conn: &rusqlite::Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch("UPDATE Jobs SET is_running = false WHERE is_running IS true;")
            .unwrap();

        Ok(())
    }

    ///
    /// Sets a specific jobs to be not running
    ///
    pub(in crate::db) fn internal_jobs_set_isrunning(
        conn: &rusqlite::Connection,
        job_id: u64,
    ) -> Result<(), rusqlite::Error> {
        conn.execute(
            "UPDATE Jobs SET is_running = true WHERE id IS ?1;",
            params![job_id],
        )
        .unwrap();

        Ok(())
    }

    ///
    /// Removes a specific job
    ///
    pub(in crate::db) fn internal_job_remove(
        conn: &rusqlite::Connection,
        job_id: u64,
    ) -> Result<(), rusqlite::Error> {
        conn.execute("DELETE FROM Jobs WHERE id IS ?1;", params![job_id])
            .unwrap();

        Ok(())
    }

    ///
    /// Used internally to get all jobs from site
    ///
    pub(in crate::db) fn internal_jobs_get_site(
        rusqlite_conn: &rusqlite::Connection,
        site: &str,
    ) -> Result<Vec<shared_types::DbJobsObj>, rusqlite::Error> {
        // Select all jobs matching the given site
        let mut stmt = rusqlite_conn.prepare_cached(
            "SELECT id, time, reptime, priority, manager, site, param, user_data, is_running 
         FROM Jobs 
         WHERE site = ?1;",
        )?;

        // query_map processes each row through a closure safely
        let job_iter = stmt.query_map([site], |row| shared_types::DbJobsObj::from_row(row))?;

        // Collect the iterator results, propagating any underlying row or parsing errors
        let mut jobs = Vec::new();
        for job_result in job_iter {
            jobs.push(job_result?);
        }

        Ok(jobs)
    }

    pub(in crate::db) fn internal_jobs_add(conn: &Connection, config: &PluginJob) -> u64 {
        let mut stmt = conn
            .prepare_cached(
                "INSERT INTO Jobs (time, reptime, priority, manager, site, param, user_data) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)

ON CONFLICT(time, reptime, site, param) DO UPDATE SET
            reptime = excluded.reptime,      -- Update to the new execution time
            priority = excluded.priority,    -- Update to the new priority level
            user_data = excluded.user_data

         RETURNING id",
            )
            .unwrap();

        // Serialize on-the-fly for the TEXT columns
        let param_json = serde_json::to_string(&config.param).unwrap();
        let user_data_json = serde_json::to_string(&config.user_data).unwrap();
        let dummy_manager_json = "{}"; // Replace with your actual serialized DbJobsManager struct

        let id: u64 = stmt
            .query_row(
                params![
                    config.time,
                    config.reptime,
                    config.priority,
                    dummy_manager_json,
                    config.site,
                    param_json,
                    user_data_json
                ],
                |row| row.get(0),
            )
            .unwrap();

        id
    }

    ///
    /// Gets all sites currently in db from Jobs
    ///
    pub(in crate::db) fn internal_jobs_get_all_sites(
        conn: &Connection,
    ) -> Result<Vec<String>, rusqlite::Error> {
        // Use DISTINCT so SQLite handles deduplication natively at the engine level
        let mut stmt =
            conn.prepare_cached("SELECT DISTINCT site FROM Jobs WHERE site IS NOT NULL;")?;

        // Map each row directly to a String extraction
        let site_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;

        // Collect results, propagating any database errors upstream
        site_iter.collect()
    }

    ///
    /// Used internally to set a Setting
    ///
    pub(in crate::db) fn internal_setting_set(
        rusqlite_conn: &r2d2_sqlite::rusqlite::Connection,
        obj: &shared_types::DbSettingsObj,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        // Option A: Using raw fields manually
        let mut stmt = rusqlite_conn.prepare(
            "INSERT OR REPLACE INTO settings (name, description, num, param) 
             VALUES (?1, ?2, ?3, ?4)",
        )?;

        stmt.execute(r2d2_sqlite::rusqlite::params![
            obj.name,
            obj.description,
            obj.num,
            obj.param
        ])?;

        Ok(())
    }

    pub(in crate::db) fn internal_tag_bulk_add(
        conn: &Connection,
        tags: &[shared_types::PluginTag],
    ) -> HashMap<shared_types::Tag, u64> {
        let mut out = HashMap::new();

        // 1. Gather all namespaces across the hierarchical plugin tags
        let namespaces: HashSet<&shared_types::GenericNamespaceObj> = tags
            .iter()
            .flat_map(|t| {
                std::iter::once(&t.tag.namespace)
                    .chain(t.relates_to.as_ref().map(|r| &r.tag.namespace))
                    .chain(
                        t.relates_to
                            .as_ref()
                            .and_then(|r| r.limit_to.as_ref())
                            .map(|l| &l.namespace),
                    )
            })
            .collect();

        // 2. Bulk insert namespaces first to grab their IDs
        let namespace_ids = Self::internal_namespace_bulk_add(conn, namespaces);

        // 3. Prepare the Upsert statement. DO UPDATE ensures `RETURNING id`
        // works even if the tag already exists in the database.
        let mut stmt = conn
            .prepare_cached(
                "INSERT INTO Tags (name, namespace) VALUES (?1, ?2)
             ON CONFLICT(name, namespace) DO UPDATE SET name = excluded.name
             RETURNING id",
            )
            .unwrap();

        for tag in tags {
            // --- 1. Process base tag ---
            if let Some(&namespace_id) = namespace_ids.get(&tag.tag.namespace) {
                let tag_id: u64 = stmt
                    .query_row(params![tag.tag.name, namespace_id], |row| row.get(0))
                    .unwrap();

                // Assuming your shared_types::Tag can be cloned or constructed from tag.tag
                out.insert(tag.tag.clone(), tag_id);
            }

            // --- 2. Process relates_to tag ---
            if let Some(relate_tag) = &tag.relates_to {
                if let Some(&namespace_id) = namespace_ids.get(&relate_tag.tag.namespace) {
                    let relate_tag_id: u64 = stmt
                        .query_row(params![relate_tag.tag.name, namespace_id], |row| row.get(0))
                        .unwrap();

                    out.insert(relate_tag.tag.clone(), relate_tag_id);
                }

                // --- 3. Process limit_to tag ---
                if let Some(limit_to_tag) = &relate_tag.limit_to {
                    if let Some(&namespace_id) = namespace_ids.get(&limit_to_tag.namespace) {
                        let limit_id: u64 = stmt
                            .query_row(params![limit_to_tag.name, namespace_id], |row| row.get(0))
                            .unwrap();

                        out.insert(limit_to_tag.clone(), limit_id);
                    }
                }
            }
        }

        out
    }

    ///
    /// Bulk adds namespaces into DB returning their id
    ///
    pub(in crate::db) fn internal_namespace_bulk_add(
        conn: &Connection,
        namespaces: HashSet<&shared_types::GenericNamespaceObj>,
    ) -> HashMap<shared_types::GenericNamespaceObj, u64> {
        let mut out = HashMap::new();

        let mut stmt = conn
            .prepare_cached(
                "INSERT INTO Namespace (name, description) 
             VALUES (?1, ?2) 
             ON CONFLICT(name) DO UPDATE SET description = excluded.description
             RETURNING id",
            )
            .unwrap();

        for namespace in namespaces {
            let namespace_id: u64 = stmt
                .query_row(params![namespace.name, namespace.description], |row| {
                    row.get(0)
                })
                .unwrap();

            out.insert((*namespace).clone(), namespace_id);
        }

        out
    }

    ///
    /// Bulk adds parents into DB returning their id
    ///
    pub(in crate::db) fn internal_parents_bulk_add(
        conn: &Connection,
        parents: HashSet<&shared_types::TagParents>,
    ) -> HashMap<shared_types::TagParents, u64> {
        let mut out = HashMap::new();

        let mut stmt = conn
            .prepare_cached(
                "INSERT INTO Parents (tag_id, relate_tag_id, limit_to) 
             VALUES (?1, ?2, ?3) 
             RETURNING id",
            )
            .unwrap();

        for parent in parents {
            let parent_id: u64 = stmt
                .query_row(
                    params![parent.tag_id, parent.relate_tag_id, parent.limit_to],
                    |row| row.get(0),
                )
                .unwrap();

            out.insert((*parent).clone(), parent_id);
        }

        out
    }

    ///
    /// What everything else uses when getting a setting
    ///
    pub async fn setting_get(&self, name: String) -> Option<shared_types::DbSettingsObj> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().ok()?;
            Self::internal_setting_get(&conn, &name).ok().flatten()
        })
        .await
        .ok()
        .flatten() // Flattens the JoinError wrapper Option as well
    }

    ///
    /// What anything outside of the db uses to set a setting
    ///
    pub async fn setting_set(&self, obj: shared_types::DbSettingsObj) -> bool {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().ok()?;
            Some(Self::internal_setting_set(&conn, &obj).is_ok())
        })
        .await
        .ok()
        .flatten()
        .unwrap_or(false)
    }
    ///
    /// Sets a job to be running inside of the db
    ///
    pub async fn job_set_is_running(&self, job: &DbJobsObj) {
        let pool = self.pool.clone();

        let job_id = job.id;
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().ok().unwrap();
            Self::internal_jobs_set_isrunning(&conn, job_id).is_ok()
        })
        .await;
    }

    ///
    /// Sets a job to be running inside of the db
    ///
    pub async fn job_remove(&self, job: &DbJobsObj) {
        let pool = self.pool.clone();

        let job_id = job.id;
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().ok().unwrap();
            Self::internal_job_remove(&conn, job_id).is_ok()
        })
        .await;
    }

    ///
    /// Gets all jobs associated with a site
    ///
    pub async fn jobs_get_site(&self, site: &str) -> Vec<DbJobsObj> {
        let pool = self.pool.clone();

        let site_owned = site.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    return Vec::new();
                }
            };
            match Self::internal_jobs_get_site(&conn, &site_owned) {
                Ok(jobs) => jobs,
                Err(e) => {
                    log::error!(
                        "Database error fetching jobs for site '{}': {:?}",
                        site_owned,
                        e
                    );
                    Vec::new()
                }
            }
        })
        .await
        .unwrap_or_default()
    }

    ///
    /// Gets all jobs that can run
    ///
    pub async fn jobs_get_torun(&self) -> Vec<DbJobsObj> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    return Vec::new();
                }
            };
            match Self::internal_jobs_get_torun(&conn) {
                Ok(jobs) => jobs,
                Err(e) => {
                    log::error!("Database error fetching jobs: {:?}", e);
                    Vec::new()
                }
            }
        })
        .await
        .unwrap_or_default()
    }

    ///
    /// Adds job into db
    ///
    pub async fn jobs_add_single(&self, job: PluginJob) -> u64 {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };
            Self::internal_jobs_add(&conn, &job)
        })
        .await
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DB_VERSION;
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;
    use r2d2_sqlite::rusqlite::OpenFlags;
    use shared_types::GenericNamespaceObj;
    use shared_types::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    pub fn new_test() -> Arc<MainDatabase> {
        // Generate a unique database name for this specific test thread
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let db_uri = format!("file:test_db_{}?mode=memory&cache=shared", id);

        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI;

        // Pass the unique URI here
        let manager = SqliteConnectionManager::file(db_uri)
            .with_flags(flags)
            .with_init(|c| c.execute_batch("PRAGMA foreign_keys = ON;"));

        // Set a realistic pool size (e.g., 2 or 3) to prevent pool-starvation deadlocks
        let pool = Pool::builder()
            .max_size(3)
            .build(manager)
            .expect("Failed to create test pool");

        let main_db = Arc::new(MainDatabase { pool });
        main_db.check_db().unwrap();
        main_db
    }

    #[test]
    fn test_database_initialization_and_settings() {
        // 1. Fire up a completely self-contained in-memory pool instance
        let db = new_test();

        // Grab an isolated connection out of our pool to assert initialization
        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // 2. Validate that the tables were successfully initialized by check_db
        let table_check: i32 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Settings'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            table_check, 1,
            "The Settings table was not created during initialization"
        );

        // 3. Test that your default values were baked in successfully
        let system_version = MainDatabase::internal_setting_get(&conn, "SYSTEM_VERSION")
            .unwrap()
            .expect("SYSTEM_VERSION setting should be configured");

        assert_eq!(system_version.num, Some(DB_VERSION));

        let user_agent = MainDatabase::internal_setting_get(&conn, "SYSTEM_DEFAULT_USER_AGENT")
            .unwrap()
            .expect("Default user agent missing");

        assert_eq!(user_agent.param, Some("IntScrape V1.0".to_string()));
    }

    #[test]
    fn test_internal_tag_bulk_add_ignores_duplicates() {
        let db = new_test();
        let ns = GenericNamespaceObj {
            name: "system".to_string(),
            description: None,
        };

        let tag1 = PluginTag {
            tag: Tag {
                name: "unique_tag".to_string(),
                namespace: ns.clone(),
            },
            relates_to: None,
            ..Default::default()
        };

        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // Duplicate tag layout
        let tag2 = tag1.clone();

        // Pass duplicate elements in the batch array
        MainDatabase::internal_tag_bulk_add(&conn, &[tag1, tag2]);

        // Due to INSERT OR IGNORE, SQL should gracefully process without panicking on unique constraints
        let tag_count: i32 = conn
            .query_row(
                "SELECT count(*) FROM Tags WHERE name = 'unique_tag'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            tag_count, 1,
            "INSERT OR IGNORE failed to drop duplicate entry safely"
        );
    }

    #[test]
    fn test_internal_namespace_bulk_add_success_and_upsert() {
        let db = new_test();

        let ns1 = GenericNamespaceObj {
            name: "authors".to_string(),
            description: Some("Book creators".to_string()),
        };
        let ns2 = GenericNamespaceObj {
            name: "genres".to_string(),
            description: None,
        };

        let mut set = HashSet::new();
        set.insert(&ns1);
        set.insert(&ns2);

        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // 1. Test insertion
        let ids = MainDatabase::internal_namespace_bulk_add(&conn, set);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains_key(&ns1));
        assert!(ids.contains_key(&ns2));

        // 2. Test Upsert (ON CONFLICT update description)
        let ns1_updated = GenericNamespaceObj {
            name: "authors".to_string(),
            description: Some("Updated Description".to_string()),
        };

        let mut update_set = HashSet::new();
        update_set.insert(&ns1_updated);

        let updated_ids = MainDatabase::internal_namespace_bulk_add(&conn, update_set);
        assert_eq!(updated_ids.get(&ns1_updated), ids.get(&ns1)); // ID should remain unchanged

        // Verify description updated in DB
        let desc: String = conn
            .query_row(
                "SELECT description FROM Namespace WHERE name = 'authors'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(desc, "Updated Description");
    }

    #[test]
    fn test_internal_parents_bulk_add_with_dynamic_tags() {
        let db = new_test();
        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // 1. Construct a fully relational tag structure
        let ns = GenericNamespaceObj {
            name: "programming".to_string(),
            description: None,
        };

        let t_rust = Tag {
            name: "Rust".to_string(),
            namespace: ns.clone(),
        };
        let t_lang = Tag {
            name: "Language".to_string(),
            namespace: ns.clone(),
        };
        let t_backend = Tag {
            name: "Backend".to_string(),
            namespace: ns.clone(),
        };

        let complex_plugin_tag = PluginTag {
            tag: t_rust.clone(),
            relates_to: Some(RelationContext {
                tag: t_lang.clone(),
                limit_to: Some(t_backend.clone()),
                ..Default::default()
            }),
            ..Default::default()
        };

        // 2. Add tags dynamically through your revamped bulk add function
        // This registers all 3 tags and their namespaces simultaneously
        let tag_ids = MainDatabase::internal_tag_bulk_add(&conn, &[complex_plugin_tag]);

        // Extract the generated IDs from the map returned by the tag function
        let rust_id = *tag_ids.get(&t_rust).expect("Rust tag missing ID");
        let lang_id = *tag_ids.get(&t_lang).expect("Language tag missing ID");
        let backend_id = *tag_ids.get(&t_backend).expect("Backend tag missing ID");

        // 3. Formulate the parent relations safely using the generated IDs
        let relation1 = TagParents {
            tag_id: rust_id,
            relate_tag_id: lang_id,
            limit_to: Some(backend_id),
        };
        let relation2 = TagParents {
            tag_id: lang_id,
            relate_tag_id: backend_id,
            limit_to: None,
        };

        let mut parent_input_set = HashSet::new();
        parent_input_set.insert(&relation1);
        parent_input_set.insert(&relation2);

        // 4. Execute the parents bulk add method
        let parent_results = MainDatabase::internal_parents_bulk_add(&conn, parent_input_set);

        // 5. Verify the relationship mapping table state
        assert_eq!(
            parent_results.len(),
            2,
            "Failed to insert both relationships"
        );
        assert!(parent_results.contains_key(&relation1));
        assert!(parent_results.contains_key(&relation2));

        // Ensure rows exist inside SQLite storage engine exactly as mapped
        let total_db_parent_rows: u32 = conn
            .query_row("SELECT count(*) FROM Parents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(total_db_parent_rows, 2);
    }

    #[test]
    fn test_internal_tag_bulk_add_flatmaps_nested_namespaces() {
        let db = new_test();
        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        let ns_base = GenericNamespaceObj {
            name: "base_ns".to_string(),
            description: None,
        };
        let ns_relate = GenericNamespaceObj {
            name: "relate_ns".to_string(),
            description: None,
        };
        let ns_limit = GenericNamespaceObj {
            name: "limit_ns".to_string(),
            description: None,
        };

        let complex_tag = PluginTag {
            tag: Tag {
                name: "rust".to_string(),
                namespace: ns_base.clone(),
            },
            relates_to: Some(RelationContext {
                tag: Tag {
                    name: "programming".to_string(),
                    namespace: ns_relate.clone(),
                },
                limit_to: Some(Tag {
                    name: "limit".to_string(),
                    namespace: ns_limit.clone(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        // Execute bulk add
        MainDatabase::internal_tag_bulk_add(&conn, &[complex_tag]);

        // Assertions 1: Ensure all 3 distinct namespaces were automatically extracted and created
        let ns_count: i32 = conn
            .query_row("SELECT count(*) FROM Namespace", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns_count, 3);

        // Assertions 2: Ensure both tags ("rust" and "programming") were inserted safely
        let tag_count: i32 = conn
            .query_row("SELECT count(*) FROM Tags", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tag_count, 3);

        // Verify "rust" tag belongs to the correct mapped namespace row
        let mapped_ns_name: String = conn.query_row(
            "SELECT n.name FROM Tags t JOIN Namespace n ON t.namespace = n.id WHERE t.name = 'rust'",
            [],
            |r| r.get(0)
        ).unwrap();
        assert_eq!(mapped_ns_name, "base_ns");
    }
}
