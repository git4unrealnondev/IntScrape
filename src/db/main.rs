use r2d2_sqlite::rusqlite::OptionalExtension;
use r2d2_sqlite::rusqlite::{self, Connection, Row, params};
use rusqlite::ToSql;
use shared_types::*;
use std::path::PathBuf;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::SystemTime,
};

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
    pub(in crate::db) fn internal_table_create_relationship_v1(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS Relationship (
    file_id INTEGER NOT NULL,
    tag_id  INTEGER NOT NULL,

    PRIMARY KEY (file_id, tag_id),

    FOREIGN KEY (file_id)
        REFERENCES File(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    FOREIGN KEY (tag_id)
        REFERENCES Tags(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_tag_id_file_id ON Relationship(tag_id, file_id DESC);
",
        )
        .unwrap();
    }

    ///
    /// Handles creating the triggers to manage the count in the Tags column
    ///
    pub(in crate::db) fn internal_trigger_create_relationship_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TRIGGER IF NOT EXISTS relationship_insert_count
AFTER INSERT ON Relationship
BEGIN
    UPDATE Tags
    SET count = count + 1
    WHERE id = NEW.tag_id;
END;

CREATE TRIGGER IF NOT EXISTS relationship_delete_count
AFTER DELETE ON Relationship
BEGIN
    UPDATE Tags
    SET count = count - 1
    WHERE id = OLD.tag_id;
END;

",
        )
        .unwrap();
    }

    ///
    /// Creates the current default Tags table
    ///
    pub(in crate::db) fn internal_table_create_tags_v1(conn: &Connection) {
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
    pub(in crate::db) fn internal_table_create_namespace_v1(conn: &Connection) {
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
    pub(in crate::db) fn internal_table_create_settings_v1(conn: &Connection) {
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
    pub(in crate::db) fn internal_table_create_parents_v1(conn: &Connection) {
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

    CHECK (tag_id != relate_tag_id),

    UNIQUE(tag_id, relate_tag_id, limit_to)
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
    pub(in crate::db) fn internal_table_create_file_storage_locations_v1(conn: &Connection) {
        conn.execute_batch("
CREATE TABLE IF NOT EXISTS FileStorageLocations (id INTEGER PRIMARY KEY AUTOINCREMENT, location TEXT NOT NULL UNIQUE);

").unwrap();
    }

    ///
    /// Used internally to get a file location
    ///
    pub(in crate::db) fn internal_file_storage_location_get(
        conn: &Connection,
        name: &str,
    ) -> Result<Option<u64>, rusqlite::Error> {
        let mut stmt =
            conn.prepare("SELECT id FROM FileStorageLocations WHERE location = ? LIMIT 1")?;

        let mut rows = stmt.query([name])?;

        if let Some(row) = rows.next()? {
            // Unpack using serde_rusqlite
            let obj = serde_rusqlite::from_row::<u64>(row)
                .map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    ///
    /// Adds a file storage location
    ///
    pub(in crate::db) fn internal_file_storage_location_set(
        conn: &Connection,
        name: &str,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        let mut stmt = conn.prepare("INSERT INTO FileStorageLocations (location) VALUES (?1)")?;

        stmt.execute(params![name])?;

        Ok(())
    }

    ///
    /// Creates the default File table
    ///
    pub(in crate::db) fn internal_table_create_file_v1(conn: &Connection) {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS File 
            (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
            hash TEXT UNIQUE, 
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
    pub(in crate::db) fn internal_table_create_jobs_v1(conn: &Connection) {
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
        conn: &Connection,
        name: &str,
    ) -> Result<Option<shared_types::DbSettingsObj>, rusqlite::Error> {
        let mut stmt = conn
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
    /// Checks if we should download a file
    ///
    pub(in crate::db) fn internal_should_download_file(conn: &Connection, url: &str) -> bool {
        let source_url_nsid = Self::internal_namespace_sourceurl_get(conn);

        if let Some(tag_id) = Self::internal_tag_get_id(conn, url, source_url_nsid) {
            return !Self::internal_tag_has_files(conn, tag_id);
        }

        true
    }

    ///
    /// Adds the source url or gets it
    ///
    pub(in crate::db) fn internal_namespace_sourceurl_get(conn: &Connection) -> u64 {
        Self::internal_namespace_get_or_create(
            conn,
            &GenericNamespaceObj {
                name: "source_url".into(),
                description: Some("A source for a file".into()),
            },
        )
    }

    ///
    /// Checks if a tag has a relationship with files
    ///
    pub(in crate::db) fn internal_tag_has_files(conn: &Connection, tag_id: u64) -> bool {
        let mut stmt = conn
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM Relationships WHERE tag_id = ?1)")
            .unwrap();

        stmt.query_row(params![tag_id], |row| row.get(0))
            .unwrap_or(false) // Returns false if any unexpected error occurs
    }

    ///
    /// Checks to see if a tag exists in the db
    ///
    pub(in crate::db) fn internal_tag_get_id(
        conn: &Connection,
        name: &str,
        namespace_id: u64,
    ) -> Option<u64> {
        let mut stmt = conn
            .prepare_cached("SELECT id FROM Tags WHERE name = ?1 AND namespace = ?2")
            .unwrap();

        stmt.query_row(params![name, namespace_id], |row| row.get(0))
            .optional() // Turns QueryReturnedNoRows into Ok(None)
            .unwrap()
    }

    ///
    /// Only gets a namespace id
    ///
    pub(in crate::db) fn internal_namespace_get_id(
        conn: &Connection,
        namespace_name: &str,
    ) -> Option<u64> {
        let mut stmt = conn
            .prepare_cached("SELECT id FROM Namespaces WHERE name = ?1")
            .unwrap();

        stmt.query_row(params![namespace_name], |row| row.get(0))
            .optional() // Crucial: converts an Err(QueryReturnedNoRows) into Ok(None)
            .unwrap()
    }

    ///
    /// Gets or creates a namespace
    ///
    pub(in crate::db) fn internal_namespace_get_or_create(
        conn: &Connection,
        namespace: &GenericNamespaceObj,
    ) -> u64 {
        let mut stmt = conn
            .prepare_cached(
                "INSERT INTO Namespace (name, description) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET description = excluded.description
             RETURNING id",
            )
            .unwrap();

        stmt.query_row(params![namespace.name, namespace.description], |row| {
            row.get(0)
        })
        .unwrap()
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
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch("UPDATE Jobs SET is_running = false WHERE is_running IS true;")
            .unwrap();

        Ok(())
    }

    ///
    /// Sets a specific jobs to be not running
    ///
    pub(in crate::db) fn internal_jobs_set_isrunning(
        conn: &Connection,
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
        conn: &Connection,
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
        conn: &Connection,
        site: &str,
    ) -> Result<Vec<shared_types::DbJobsObj>, rusqlite::Error> {
        // Select all jobs matching the given site
        let mut stmt = conn.prepare_cached(
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
    /// Gets the location where files should be stored
    ///
    pub(in crate::db) fn internal_file_download_location_get(
        conn: &Connection,
    ) -> Result<Option<(PathBuf, u64)>, rusqlite::Error> {
        if let Some(setting) = Self::internal_setting_get(conn, "SYSTEM_file_location")?
            && let Some(setting_param) = setting.param
        {
            if let Ok(Some(path_id)) =
                Self::internal_file_storage_location_get(conn, &setting_param)
            {
                return Ok(Some((PathBuf::from(setting_param), path_id)));
            }
        }
        Ok(None)
    }

    ///
    /// Sets the default file download location
    ///
    pub(in crate::db) fn internal_file_download_location_set_default(
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        let default_files_location = "files";

        if Self::internal_setting_get(conn, "SYSTEM_file_location")?.is_none() {
            Self::internal_setting_set(
                conn,
                &DbSettingsObj {
                    name: "SYSTEM_file_location".into(),
                    description: Some("The default location where files are downloaded to.".into()),
                    num: None,
                    param: Some(default_files_location.into()),
                },
            )?;
        }

        if Self::internal_file_storage_location_get(conn, default_files_location)?.is_none() {
            Self::internal_file_storage_location_set(conn, default_files_location)?;
        }

        Ok(())
    }

    ///
    /// Used internally to set a Setting
    ///
    pub(in crate::db) fn internal_setting_set(
        conn: &Connection,
        obj: &shared_types::DbSettingsObj,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        // Option A: Using raw fields manually
        let mut stmt = conn.prepare(
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

    ///
    /// Used internally to add a relationship to a db
    ///
    pub(in crate::db) fn internal_relationship_add(
        conn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        // Option A: Using raw fields manually
        let mut stmt = conn.prepare(
            "INSERT OR IGNORE INTO Relationship (file_id, tag_id) 
             VALUES (?1, ?2)",
        )?;

        stmt.execute(r2d2_sqlite::rusqlite::params![file_id, tag_id])?;

        Ok(())
    }

    ///
    /// Adds tags into db
    ///
    pub(in crate::db) fn internal_tag_bulk_add(
        conn: &Connection,
        tag_actions: &[FileTagAction],
    ) -> HashMap<shared_types::Tag, u64> {
        let mut out = HashMap::new();
        let mut parents = HashSet::new();

        let is_valid_tag = |tag: &&shared_types::PluginTag| {
            matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
        };

        // 1️⃣ Gather all valid namespaces across all tag actions (unchanged)
        let namespaces: HashSet<shared_types::GenericNamespaceObj> = tag_actions
            .iter()
            .flat_map(|action| &action.tags)
            .flat_map(|t| {
                std::iter::once(t.tag.namespace.clone())
                    .chain(t.relates_to.as_ref().map(|r| r.tag.namespace.clone()))
                    .chain(
                        t.relates_to
                            .as_ref()
                            .and_then(|r| r.limit_to.as_ref())
                            .map(|l| l.namespace.clone()),
                    )
            })
            .collect();

        let namespace_ids = Self::internal_namespace_bulk_add(conn, &namespaces);

        // 2️⃣ DEDUPLICATE AND GROUP PLAIN TAGS TO BULK INSERT
        // Collect unique (name, namespace_id) tuples alongside their original struct keys
        let mut pending_tags = Vec::new();
        let mut unique_tags_set = HashSet::new();

        let valid_tags = tag_actions
            .iter()
            .flat_map(|action| &action.tags)
            .filter(is_valid_tag);

        for tag in valid_tags {
            if let Some(&ns_id) = namespace_ids.get(&tag.tag.namespace) {
                if unique_tags_set.insert((tag.tag.name.clone(), ns_id)) {
                    pending_tags.push((tag.tag.clone(), ns_id));
                }

                if let Some(relate_tag) = &tag.relates_to {
                    if let Some(&rel_ns_id) = namespace_ids.get(&relate_tag.tag.namespace) {
                        if unique_tags_set.insert((relate_tag.tag.name.clone(), rel_ns_id)) {
                            pending_tags.push((relate_tag.tag.clone(), rel_ns_id));
                        }

                        if let Some(limit_to_tag) = &relate_tag.limit_to {
                            if let Some(&lim_ns_id) = namespace_ids.get(&limit_to_tag.namespace) {
                                if unique_tags_set.insert((limit_to_tag.name.clone(), lim_ns_id)) {
                                    pending_tags.push((limit_to_tag.clone(), lim_ns_id));
                                }
                            }
                        }
                    }
                }
            }
        }

        if pending_tags.is_empty() {
            return out;
        }

        // Build an query like: INSERT INTO Tags (name, namespace) VALUES (?, ?), (?, ?)...
        let mut query = String::from("INSERT INTO Tags (name, namespace) VALUES ");
        let mut params_vector: Vec<&dyn ToSql> = Vec::with_capacity(pending_tags.len() * 2); // Adjust type to your driver's dynamic param type (e.g. rusqlite::types::ToSql / deadpool)

        for (i, (tag_obj, ns_id)) in pending_tags.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{})", i * 2 + 1, i * 2 + 2));
            params_vector.push(&tag_obj.name);
            params_vector.push(ns_id);
        }
        query.push_str(
            " ON CONFLICT(name, namespace) DO UPDATE SET name = excluded.name RETURNING id",
        );

        // Prepare and collect IDs back in exact sequence
        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params_vector))
            .unwrap();

        let mut idx = 0;
        while let Some(row) = rows.next().unwrap() {
            let tag_id: u64 = row.get(0).unwrap();
            let (tag_obj, _) = &pending_tags[idx];
            out.insert(tag_obj.clone(), tag_id);
            idx += 1;
        }

        // 4️⃣ SECOND PASS: Resolve structural parent hierarchies from memory map instantly
        let valid_tags = tag_actions
            .iter()
            .flat_map(|action| &action.tags)
            .filter(is_valid_tag);

        for tag in valid_tags {
            if let Some(&tag_id) = out.get(&tag.tag) {
                if let Some(relate_tag) = &tag.relates_to {
                    if let Some(&relate_tag_id) = out.get(&relate_tag.tag) {
                        if relate_tag.limit_to.is_none() {
                            parents.insert(shared_types::TagParents {
                                tag_id,
                                relate_tag_id,
                                limit_to: None,
                            });
                        }

                        if let Some(limit_to_tag) = &relate_tag.limit_to {
                            if let Some(&limit_id) = out.get(limit_to_tag) {
                                parents.insert(shared_types::TagParents {
                                    tag_id,
                                    relate_tag_id,
                                    limit_to: Some(limit_id),
                                });
                            }
                        }
                    }
                }
            }
        }
        if !parents.is_empty() {
            Self::internal_parents_bulk_add(conn, &parents);
        }

        out
    }

    ///
    /// Bulk adds namespaces into DB returning their id
    ///
    pub(in crate::db) fn internal_namespace_bulk_add(
        conn: &Connection,
        namespaces: &HashSet<shared_types::GenericNamespaceObj>,
    ) -> HashMap<shared_types::GenericNamespaceObj, u64> {
        let mut out = HashMap::new();

        if namespaces.is_empty() {
            return out;
        }

        let namespace_vec: Vec<&GenericNamespaceObj> = namespaces.iter().collect();

        let mut query = String::from("INSERT INTO Namespace (name, description) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(namespace_vec.len() * 2);

        // String building
        for (i, namespace) in namespace_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{})", i * 2 + 1, i * 2 + 2));
            params_vector.push(&namespace.name);
            params_vector.push(&namespace.description);
        }

        query.push_str(
            " ON CONFLICT(name) DO UPDATE SET description = excluded.description RETURNING id",
        );

        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(&*params_vector).unwrap();

        let mut idx = 0;
        while let Some(row) = rows.next().unwrap() {
            let nsid: u64 = row.get(0).unwrap();
            let namespace_obj = namespace_vec[idx];

            out.insert((*namespace_obj).clone(), nsid);
            idx += 1;
        }

        out
    }

    ///
    /// Bulk adds relationship into DB
    ///
    pub(in crate::db) fn internal_relationship_bulk_add(
        conn: &Connection,
        namespaces: &HashSet<(u64, u64)>,
    ) {
        if namespaces.is_empty() {
            return;
        }

        let namespace_vec: Vec<&(u64, u64)> = namespaces.iter().collect();

        let mut query =
            String::from("INSERT OR IGNORE INTO Relationship (file_id, tag_id) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(namespace_vec.len() * 2);

        // String building
        for (i, namespace) in namespace_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{})", i * 2 + 1, i * 2 + 2));
            params_vector.push(&namespace.0);
            params_vector.push(&namespace.1);
        }

        conn.execute(&query, &*params_vector).unwrap();
    }

    ///
    /// Bulk adds parents into DB returning their id
    ///
    pub(in crate::db) fn internal_parents_bulk_add(
        conn: &Connection,
        parents: &HashSet<shared_types::TagParents>,
    ) -> HashMap<shared_types::TagParents, u64> {
        let mut out = HashMap::new();

        if parents.is_empty() {
            return out;
        }

        let parents_vec: Vec<&shared_types::TagParents> = parents.iter().collect();

        let mut query =
            String::from("INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(parents_vec.len() * 3);

        // String building
        for (i, parent) in parents_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{}, ?{})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            params_vector.push(&parent.tag_id);
            params_vector.push(&parent.relate_tag_id);
            params_vector.push(&parent.limit_to);
        }

        query.push_str(
            " ON CONFLICT(tag_id, relate_tag_id, limit_to) 
         DO UPDATE SET tag_id = excluded.tag_id 
         RETURNING id",
        );

        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(&*params_vector).unwrap();

        let mut idx = 0;
        while let Some(row) = rows.next().unwrap() {
            let parent_id: u64 = row.get(0).unwrap();
            let parent_obj = parents_vec[idx];

            out.insert((*parent_obj).clone(), parent_id);
            idx += 1;
        }

        out
    }

    ///
    /// Bulk adds files into DB returning their id
    ///
    pub(in crate::db) fn internal_file_bulk_add(
        conn: &Connection,
        parents: &HashSet<shared_types::FileInternal>,
    ) -> HashMap<shared_types::FileInternal, u64> {
        let mut out = HashMap::new();

        if parents.is_empty() {
            return out;
        }

        let parents_vec: Vec<&shared_types::FileInternal> = parents.iter().collect();

        let mut query = String::from("INSERT INTO File (hash, extension, storage_id) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(parents_vec.len() * 3);

        // String building
        for (i, parent) in parents_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{}, ?{})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            params_vector.push(&parent.hash);
            params_vector.push(&parent.ext);
            params_vector.push(&parent.storage_id);
        }

        // FIX: Combined into a single DO UPDATE SET clause separated by a comma
        query.push_str(
            " ON CONFLICT(hash) 
         DO UPDATE SET 
            extension = excluded.extension,
            storage_id = excluded.storage_id
         RETURNING id",
        );

        let mut stmt = conn.prepare(&query).unwrap();

        // FIX: Swapped to slice_to_params to match your lifetime array structure correctly
        let mut rows = stmt.query(&*params_vector).unwrap();

        let mut idx = 0;
        while let Some(row) = rows.next().unwrap() {
            let parent_id: u64 = row.get(0).unwrap();
            let parent_obj = parents_vec[idx];

            out.insert((*parent_obj).clone(), parent_id);
            idx += 1;
        }

        out
    }

    pub(in crate::db) fn debug_print_parents(conn: &Connection) {
        // 1. Prepare the SELECT statement
        let mut stmt = conn
            .prepare("SELECT tag_id, relate_tag_id, limit_to FROM Parents")
            .unwrap();

        // 2. Query the rows and map them to a tuple or struct
        let parent_rows = stmt
            .query_map([], |row| {
                let tag_id: u64 = row.get(0)?;
                let relate_tag_id: u64 = row.get(1)?;
                let limit_to: Option<u64> = row.get(2)?;
                Ok((tag_id, relate_tag_id, limit_to))
            })
            .unwrap();

        println!("--- Parents Table Contents ---");

        // 3. Iterate and print each row
        for row in parent_rows {
            if let Ok((tag_id, relate_tag_id, limit_to)) = row {
                match limit_to {
                    Some(limit_id) => {
                        println!(
                            "Tag ID: {} -> Relate Tag ID: {} (Limited To: {})",
                            tag_id, relate_tag_id, limit_id
                        );
                    }
                    None => {
                        println!("Tag ID: {} -> Relate Tag ID: {}", tag_id, relate_tag_id);
                    }
                }
            }
        }

        println!("------------------------------");
    }

    ///
    /// Checks if we should download the file or not
    ///
    pub async fn should_download_file(&self, url: String) -> bool {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };

            Self::internal_should_download_file(&conn, &url)
        })
        .await
        .unwrap()
    }

    ///
    /// Adds relationship into db
    ///
    pub async fn add_relationship_bulk(&self, rel_list: HashSet<(u64, u64)>) {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };
            let tn = conn.transaction().unwrap();

            Self::internal_relationship_bulk_add(&tn, &rel_list);
            tn.commit().unwrap();
        })
        .await
        .unwrap()
    }

    ///
    /// Gets the location where files should be stored
    /// IE the main folder that we're using
    ///
    pub async fn file_download_location_main(&self) -> Option<(PathBuf, u64)> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().ok()?;
            Self::internal_file_download_location_get(&conn)
                .ok()
                .flatten()
        })
        .await
        .ok()
        .flatten()
    }

    ///
    /// Returns the full location of where a file should be stored
    ///
    pub async fn file_download_location_get(
        &self,
        hash: &str,
        ext: &str,
    ) -> Option<(PathBuf, u64)> {
        // If our hash is less then 6 cant return a location
        if hash.len() <= 6 {
            return None;
        }
        self.file_download_location_main().await.map(|path| {
            let mut path_buf = path.0;
            path_buf.push(&hash[0..2]);
            path_buf.push(&hash[2..4]);
            path_buf.push(&hash[4..6]);
            path_buf.push(hash);
            (path_buf.with_extension(ext), path.1)
        })
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

    ///
    /// Adds tags into db in bulk. Also adds parents
    ///
    pub async fn tags_add_bulk(&self, tags: &[FileTagAction]) -> HashMap<shared_types::Tag, u64> {
        let pool = self.pool.clone();

        let tags_owned = tags.to_vec();

        tokio::task::spawn_blocking(move || {
            let mut conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };
            let tn = conn.transaction().unwrap();
            let out_tags = Self::internal_tag_bulk_add(&tn, &tags_owned);

            tn.commit().unwrap();
            out_tags
        })
        .await
        .unwrap()
    }

    ///
    /// Adds tags into db in bulk. Also adds parents
    ///
    pub async fn file_add_bulk(&self, tags: HashSet<FileInternal>) -> HashMap<FileInternal, u64> {
        let pool = self.pool.clone();

        let tags_owned = tags.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };
            let tn = conn.transaction().unwrap();
            let out_tags = Self::internal_file_bulk_add(&tn, &tags_owned);

            tn.commit().unwrap();
            out_tags
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
        let tag1 = FileTagAction {
            tags: vec![PluginTag {
                tag: Tag {
                    name: "unique_tag".to_string(),
                    namespace: ns.clone(),
                },
                relates_to: None,
                ..Default::default()
            }],
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
        let complex_plugin_tag = FileTagAction {
            tags: vec![PluginTag {
                tag: t_rust.clone(),
                relates_to: Some(RelationContext {
                    tag: t_lang.clone(),
                    limit_to: Some(t_backend.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
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
        parent_input_set.insert(relation1.clone());
        parent_input_set.insert(relation2.clone());

        // 4. Execute the parents bulk add method
        let parent_results = MainDatabase::internal_parents_bulk_add(&conn, &parent_input_set);

        // 5. Verify the relationship mapping table state
        assert_eq!(
            parent_results.len(),
            2,
            "Failed to insert both relationships"
        );
        assert!(parent_results.contains_key(&relation1));
        assert!(parent_results.contains_key(&relation2));

        MainDatabase::debug_print_parents(&conn);

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

        let complex_tag = FileTagAction {
            tags: vec![PluginTag {
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
            }],
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
