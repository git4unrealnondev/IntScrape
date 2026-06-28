use log::info;
use r2d2_sqlite::rusqlite::OptionalExtension;
use r2d2_sqlite::rusqlite::{self, Connection, Row, params};
use rusqlite::ToSql;
use shared_types::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::db::roaring::{InternalCacheType, SearchQuery};
use crate::db::{CacheType, RelationshipStorage};
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
        let recreation_raw: String = row.get("recreation")?;
        let user_data_raw: String = row.get("user_data")?;

        let param: Vec<ScraperParam> = serde_json::from_str(&param_raw).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                6, // Column index reference
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;
        let recreation: Option<DbJobRecreation> =
            serde_json::from_str(&recreation_raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    5, // Column index reference
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
            recreation,
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

    pub(in crate::db) fn internal_load_caching(self: Arc<Self>, conn: &Connection) {
        let temp;
        loop {
            let cache = match Self::internal_setting_get(conn, "SYSTEM_cachemode") {
                Err(_) | Ok(None) => {
                    Self::internal_setup_default_cache(conn);
                    Self::internal_setting_get(conn, "SYSTEM_cachemode")
                        .unwrap()
                        .unwrap()
                        .param
                        .clone()
                }
                Ok(Some(setting)) => setting.param.clone(),
            };

            if let Some(ref cache) = cache {
                let cachemode = match cache.as_str() {
                    "Bare" => (Some(CacheType::Bare), None),
                    "RelationshipRoaringFull" => (
                        Some(CacheType::RelationshipRoaring(InternalCacheType::Full)),
                        Some(RelationshipStorage::new(
                            self.clone(),
                            InternalCacheType::Full,
                        )),
                    ),
                    "RelationshipRoaringTable" => (
                        Some(CacheType::RelationshipRoaring(InternalCacheType::Table)),
                        Some(RelationshipStorage::new(
                            self.clone(),
                            InternalCacheType::Table,
                        )),
                    ),
                    "RelationshipRoaringPopular" => {
                        if let Ok(Some(popular_count)) =
                            Self::internal_setting_get(conn, "SYSTEM_tag_count_popular_division")
                            && let Some(popular_count) = popular_count.num
                        {
                            (
                                Some(CacheType::RelationshipRoaring(InternalCacheType::Popular(
                                    popular_count,
                                ))),
                                Some(RelationshipStorage::new(
                                    self.clone(),
                                    InternalCacheType::Popular(popular_count),
                                )),
                            )
                        } else {
                            (None, None)
                        }
                    }

                    _ => {
                        Self::internal_setup_default_cache(conn);
                        (None, None)
                    }
                };
                if cachemode.0.is_some() {
                    temp = cachemode;
                    break;
                }
            } else {
                Self::internal_setup_default_cache(conn);
            }
        }
        *self.relationship_roaring_storage.write() = temp.1;
        *self.cache_type.write() = temp.0.unwrap();
    }

    /// Sets up internal cache structure
    pub(in crate::db) fn internal_setup_default_cache(conn: &Connection) {
        Self::internal_setting_set(
            conn,
            &DbSettingsObj {
                name: "SYSTEM_cachemode".to_string(),
                description: Some(
                    "The database caching options. Supports: Bare, InMemdb and InMemory"
                        .to_string(),
                ),
                num: None,
                param: Some("RelationshipRoaringFull".to_string()),
            },
        );
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
    id INTEGER PRIMARY KEY , 
    name TEXT NOT NULL, 
    namespace INTEGER NOT NULL, 
    count INTEGER NOT NULL DEFAULT 0, 

    UNIQUE(name, namespace), 

    FOREIGN KEY (namespace) REFERENCES Namespace(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_tags_count_covering ON Tags(count DESC, name, namespace);
--CREATE INDEX IF NOT EXISTS idx_tags_namespace ON Tags(namespace);

CREATE VIEW High_Value_Tags AS 
    SELECT id, name, namespace 
    FROM Tags 
    WHERE count >= 5;

CREATE VIRTUAL TABLE Tags_Popular_fts USING fts5(
    name,
    namespace UNINDEXED,
    content='High_Value_Tags',
    content_rowid='id',
    tokenize = \"unicode61 separators '_/'\"
);

-- OPTIMIZATION: Only insert if it meets the threshold
CREATE TRIGGER IF NOT EXISTS tags_ai AFTER INSERT ON Tags 
WHEN new.count >= 5
BEGIN
    INSERT INTO Tags_Popular_fts(rowid, name, namespace) 
    VALUES (new.id, new.name, new.namespace);
END;

-- OPTIMIZATION: Only attempt FTS delete if the old row actually qualified to be in there
CREATE TRIGGER IF NOT EXISTS tags_ad AFTER DELETE ON Tags 
WHEN old.count >= 5
BEGIN
    INSERT INTO Tags_Popular_fts(Tags_Popular_fts, rowid, name, namespace) 
    VALUES('delete', old.id, old.name, old.namespace);
END;

-- OPTIMIZATION: Divided into precise conditional logic to prevent unnecessary 
-- FTS virtual table thrashing during standard increments.
CREATE TRIGGER IF NOT EXISTS tags_au_upgrade AFTER UPDATE ON Tags
WHEN old.count < 5 AND new.count >= 5
BEGIN
    INSERT INTO Tags_Popular_fts(rowid, name, namespace) 
    VALUES (new.id, new.name, new.namespace);
END;

CREATE TRIGGER IF NOT EXISTS tags_au_downgrade AFTER UPDATE ON Tags
WHEN old.count >= 5 AND new.count < 5
BEGIN
    INSERT INTO Tags_Popular_fts(Tags_Popular_fts, rowid, name, namespace) 
    VALUES('delete', old.id, old.name, old.namespace);
END;

CREATE TRIGGER IF NOT EXISTS tags_au_maintain AFTER UPDATE ON Tags
WHEN old.count >= 5 AND new.count >= 5 AND (old.name != new.name OR old.namespace != new.namespace)
BEGIN
    INSERT INTO Tags_Popular_fts(Tags_Popular_fts, rowid, name, namespace) 
    VALUES('delete', old.id, old.name, old.namespace);
    
    INSERT INTO Tags_Popular_fts(rowid, name, namespace) 
    VALUES (new.id, new.name, new.namespace);
END;
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
    id INTEGER PRIMARY KEY , 
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
    id INTEGER PRIMARY KEY ,
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
CREATE TABLE IF NOT EXISTS FileStorageLocations (id INTEGER PRIMARY KEY , location TEXT NOT NULL UNIQUE);

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
            (id INTEGER PRIMARY KEY  NOT NULL, 
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
    id INTEGER PRIMARY KEY  NOT NULL, 
    time INTEGER NOT NULL, 
    reptime INTEGER NOT NULL, 
    priority INTEGER NOT NULL,  
    is_running BOOL NOT NULL DEFAULT False,
    recreation TEXT NOT NULL, 
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

    pub(in crate::db) fn internal_jobs_update(conn: &Connection, job: &DbJobsObj) {
        let recreation = serde_json::to_string(&job.config.recreation).unwrap();
        let param = serde_json::to_string(&job.config.param).unwrap();
        let user_data = serde_json::to_string(&job.config.user_data).unwrap();

        conn.execute(
            "UPDATE Jobs 
         SET time = ?1, 
             reptime = ?2, 
             priority = ?3, 
             is_running = ?4, 
             recreation = ?5, 
             site = ?6, 
             param = ?7, 
             user_data = ?8 
         WHERE id = ?9",
            params![
                job.config.time,
                job.config.reptime,
                job.config.priority,
                job.isrunning, // true/false state
                recreation,
                job.config.site,
                param,
                user_data,
                job.id
            ],
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
    /// Gets a file if its id exists in db
    ///
    pub(in crate::db) fn internal_file_id_get(
        conn: &Connection,
        file_id: &u64,
    ) -> Result<FileInternal, rusqlite::Error> {
        conn.query_row(
            "SELECT id, hash, extension, storage_id FROM File WHERE id = ?1 LIMIT 1",
            [file_id],
            |row| {
                serde_rusqlite::from_row::<FileInternal>(row)
                    .map_err(|_| rusqlite::Error::ExecuteReturnedResults)
            },
        )
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
    /// Gets a single fileid from a tag
    ///
    pub(in crate::db) fn internal_tag_get_fileid(conn: &Connection, tag: &Tag) -> Option<u64> {
        if let Some(ns_id) = Self::internal_namespace_get_id(conn, &tag.namespace.name)
            && let Some(ref tag_id) = Self::internal_tag_get_id(conn, &tag.name, ns_id)
        {
            return Self::internal_tag_id_get_file_id(conn, tag_id).ok();
        }

        None
    }

    ///
    /// Gets a single file_internal from a tag
    ///
    pub(in crate::db) fn internal_tag_get_fileinternal(
        conn: &Connection,
        tag: &Tag,
    ) -> Option<FileInternal> {
        if let Some(ns_id) = Self::internal_namespace_get_id(conn, &tag.namespace.name)
            && let Some(ref tag_id) = Self::internal_tag_get_id(conn, &tag.name, ns_id)
            && let Ok(ref file_id) = Self::internal_tag_id_get_file_id(conn, tag_id)
        {
            return Self::internal_file_id_get(conn, file_id).ok();
        }

        None
    }

    ///
    /// Gets a fileid from a tag_id
    ///
    pub(in crate::db) fn internal_tag_id_get_file_id(
        conn: &Connection,
        tag_id: &u64,
    ) -> Result<u64, rusqlite::Error> {
        conn.query_row(
            "SELECT file_id FROM Relationship WHERE tag_id = ?1 LIMIT 1;",
            params![tag_id],
            |row| row.get(0),
        )
    }

    ///
    /// Gets tag_ids for file_ids
    ///
    pub(in crate::db) fn internal_file_id_get_tag_ids(
        conn: &Connection,
        file_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn
            .prepare("SELECT tag_id FROM Relationship where file_id = ?1;")
            .unwrap();
        let mut out = HashSet::new();
        for tag_id in stmt.query_map([file_id], |row| row.get(0))?.flatten() {
            out.insert(tag_id);
        }

        Ok(out)
    }
    ///
    /// Builds a list of file -> tagid maps
    ///
    pub(in crate::db) fn internal_file_id_get_tag_ids_bulk(
        conn: &Connection,
        file_ids: &[u64],
    ) -> Result<HashMap<u64, HashSet<u64>>, rusqlite::Error> {
        let mut out: HashMap<u64, HashSet<u64>> = HashMap::new();
        if file_ids.is_empty() {
            return Ok(out);
        }

        // Build query: SELECT file_id, tag_id FROM Relationship WHERE file_id IN (?, ?, ...)
        let mut query = String::from("SELECT file_id, tag_id FROM Relationship WHERE ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(file_ids.len());

        for (i, id) in file_ids.iter().enumerate() {
            if i > 0 {
                query.push_str(" OR ");
            }
            query.push_str(&format!("file_id = ?{}", i + 1));
            params_vector.push(id);
        }

        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params_vector))?;

        while let Some(row) = rows.next()? {
            let f_id: u64 = row.get(0)?;
            let t_id: u64 = row.get(1)?;
            out.entry(f_id).or_default().insert(t_id);
        }

        Ok(out)
    }

    pub(in crate::db) fn internal_tag_id_get_tag(
        conn: &Connection,
        tags: &HashSet<u64>,
    ) -> HashMap<u64, Tag> {
        let mut out = HashMap::new();

        if tags.is_empty() {
            return out;
        }

        // 1️⃣ Convert HashSet to a Vec for predictable ordering during mapping
        let tag_ids: Vec<&u64> = tags.iter().collect();

        // 2️⃣ Build a dynamic query containing query parameters: (?1, ?2, ?3...)
        let mut query = String::from(
            "SELECT t.id, t.name, n.name, n.description \
         FROM Tags t \
         JOIN Namespace n ON t.namespace = n.id \
         WHERE t.id IN (",
        );

        let mut params_vector: Vec<&dyn ToSql> = Vec::with_capacity(tag_ids.len());

        for (i, &id) in tag_ids.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("?{}", i + 1));
            params_vector.push(id);
        }
        query.push(')');

        // 3️⃣ Prepare the statement and map rows back into your structs
        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params_vector))
            .unwrap();

        while let Some(row) = rows.next().unwrap() {
            let id: u64 = row.get(0).unwrap();
            let tag_name: String = row.get(1).unwrap();
            let namespace_name: String = row.get(2).unwrap();
            let namespace_desc: Option<String> = row.get(3).unwrap();

            let tag = Tag {
                name: tag_name,
                namespace: GenericNamespaceObj {
                    name: namespace_name,
                    description: namespace_desc,
                },
            };

            out.insert(id, tag);
        }

        out
    }

    ///
    /// Gets tags for file_ids
    ///
    pub(in crate::db) fn internal_file_ids_get_tags(
        conn: &Connection,
        file_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<Tag>> {
        let mut out: HashMap<u64, HashSet<Tag>> = HashMap::new();
        if file_ids.is_empty() {
            return out;
        }

        let file_id_vec: Vec<&u64> = file_ids.iter().collect();

        // 1️⃣ Build a bulk query selecting relationships joined with Tags and Namespaces
        let mut query = String::from(
            "SELECT r.file_id, t.id, t.name, n.name, n.description \
         FROM Relationship r \
         JOIN Tags t ON r.tag_id = t.id \
         JOIN Namespace n ON t.namespace = n.id \
         WHERE r.file_id IN (",
        );

        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(file_id_vec.len());
        for (i, &id) in file_id_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("?{}", i + 1));
            params.push(id);
        }
        query.push(')');

        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(rusqlite::params_from_iter(params)).unwrap();

        // 2️⃣ Hydrate the nested data maps
        while let Some(row) = rows.next().unwrap() {
            let file_id: u64 = row.get(0).unwrap();
            let _tag_id: u64 = row.get(1).unwrap(); // available if you ever need it
            let tag_name: String = row.get(2).unwrap();
            let namespace_name: String = row.get(3).unwrap();
            let namespace_desc: Option<String> = row.get(4).unwrap();

            let tag = Tag {
                name: tag_name,
                namespace: GenericNamespaceObj {
                    name: namespace_name,
                    description: namespace_desc,
                },
            };

            out.entry(file_id).or_default().insert(tag);
        }

        out
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
            .prepare("SELECT EXISTS(SELECT 1 FROM Relationship WHERE tag_id = ?1)")
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
            .prepare("SELECT id FROM Tags WHERE name = ?1 AND namespace = ?2")
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
            .prepare("SELECT id FROM Namespace WHERE name = ?1")
            .unwrap();

        stmt.query_row(params![namespace_name], |row| row.get(0))
            .optional() // Crucial: converts an Err(QueryReturnedNoRows) into Ok(None)
            .unwrap()
    }

    ///
    /// Gets all namespace objects
    ///
    pub(in crate::db) fn internal_namespace_get_generic(
        conn: &Connection,
        ns_id: &u64,
    ) -> Option<GenericNamespaceObj> {
        let mut stmt = conn
            .prepare("SELECT name, description FROM Namespace WHERE id = ?1;")
            .unwrap();

        stmt.query_row(params![ns_id], |row| {
            Ok(GenericNamespaceObj {
                name: row.get(0).unwrap(),
                description: row.get(1).unwrap(),
            })
        })
        .optional()
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
            .prepare(
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
        info!("JobId: {} Is being removed.", job_id);
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
        let mut stmt = conn.prepare(
            "SELECT id, time, reptime, priority, recreation, site, param, user_data, is_running 
         FROM Jobs 
         WHERE site = ?1;",
        )?;

        // query_map processes each row through a closure safely
        let job_iter = stmt.query_map([site], shared_types::DbJobsObj::from_row)?;

        // Collect the iterator results, propagating any underlying row or parsing errors
        let mut jobs = Vec::new();
        for job_result in job_iter {
            jobs.push(job_result?);
        }

        Ok(jobs)
    }

    pub(in crate::db) fn internal_jobs_add(conn: &Connection, config: &PluginJob) -> u64 {
        let mut stmt = conn
            .prepare(
                "INSERT INTO Jobs (time, reptime, priority, recreation, site, param, user_data) 
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
        let manager_json = serde_json::to_string(&config.recreation).unwrap(); // Replace with your actual serialized DbJobsManager struct

        let id: u64 = stmt
            .query_row(
                params![
                    config.time,
                    config.reptime,
                    config.priority,
                    manager_json,
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
        let mut stmt = conn.prepare("SELECT DISTINCT site FROM Jobs WHERE site IS NOT NULL;")?;

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
            && let Ok(Some(path_id)) =
                Self::internal_file_storage_location_get(conn, &setting_param)
        {
            return Ok(Some((PathBuf::from(setting_param), path_id)));
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
                if tag.tag.name.is_empty() {
                    continue;
                }
                if unique_tags_set.insert((tag.tag.name.clone(), ns_id)) {
                    pending_tags.push((tag.tag.clone(), ns_id));
                }

                if let Some(relate_tag) = &tag.relates_to {
                    if relate_tag.tag.name.is_empty() {
                        continue;
                    }
                    if let Some(&rel_ns_id) = namespace_ids.get(&relate_tag.tag.namespace) {
                        if unique_tags_set.insert((relate_tag.tag.name.clone(), rel_ns_id)) {
                            pending_tags.push((relate_tag.tag.clone(), rel_ns_id));
                        }

                        if let Some(limit_to_tag) = &relate_tag.limit_to {
                            if limit_to_tag.name.is_empty() {
                                continue;
                            }
                            if let Some(&lim_ns_id) = namespace_ids.get(&limit_to_tag.namespace)
                                && unique_tags_set.insert((limit_to_tag.name.clone(), lim_ns_id))
                            {
                                pending_tags.push((limit_to_tag.clone(), lim_ns_id));
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
            if let Some(&tag_id) = out.get(&tag.tag)
                && let Some(relate_tag) = &tag.relates_to
                && let Some(&relate_tag_id) = out.get(&relate_tag.tag)
            {
                if relate_tag.limit_to.is_none() {
                    parents.insert(shared_types::TagParents {
                        tag_id,
                        relate_tag_id,
                        limit_to: None,
                    });
                }

                if let Some(limit_to_tag) = &relate_tag.limit_to
                    && let Some(&limit_id) = out.get(limit_to_tag)
                {
                    parents.insert(shared_types::TagParents {
                        tag_id,
                        relate_tag_id,
                        limit_to: Some(limit_id),
                    });
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
    /// Deletes relationships from db
    ///
    pub(in crate::db) fn internal_relationship_bulk_delete(
        self: Arc<Self>,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) {
        if relationships.is_empty() {
            return;
        }

        let rel_vec: Vec<&(u64, u64)> = relationships.iter().collect();
        let mut query = String::from("DELETE FROM Relationship WHERE ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(rel_vec.len() * 2);

        // removes relationships between roaring
        {
            let mut guard = self.relationship_roaring_storage.write();
            if let Some(roaring) = guard.as_mut() {
                for (file_id, tag_id) in relationships.iter() {
                    roaring.remove_roaring(conn, *tag_id, *file_id);
                }
            }
        }

        for (i, rel) in rel_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(" OR ");
            }
            query.push_str(&format!(
                "(file_id = ?{} AND tag_id = ?{})",
                i * 2 + 1,
                i * 2 + 2
            ));
            params_vector.push(&rel.0);
            params_vector.push(&rel.1);
        }

        conn.execute(&query, &*params_vector).unwrap();
    }
    ///
    /// Bulk adds relationship into DB
    ///
    pub(in crate::db) fn internal_relationship_bulk_add(
        self: Arc<Self>,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) {
        if relationships.is_empty() {
            return;
        }

        // adds relationships between roaring
        {
            let mut guard = self.relationship_roaring_storage.write();
            if let Some(roaring) = guard.as_mut() {
                for (file_id, tag_id) in relationships.iter() {
                    roaring.relationship_roaring_add(conn, *tag_id, *file_id);
                }
            }
        }

        let relationships_vec: Vec<&(u64, u64)> = relationships.iter().collect();

        let mut query =
            String::from("INSERT OR IGNORE INTO Relationship (file_id, tag_id) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(relationships_vec.len() * 2);

        // String building
        for (i, relationship) in relationships_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{})", i * 2 + 1, i * 2 + 2));
            params_vector.push(&relationship.0);
            params_vector.push(&relationship.1);
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
        parents: HashSet<shared_types::FileInternal>,
    ) -> HashSet<shared_types::FileInternal> {
        let mut out = HashSet::new();

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
            params_vector.push(&parent.extension);
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
            let mut parent_obj = parents_vec[idx].clone();
            parent_obj.id = row.get(0).ok();

            out.insert(parent_obj);
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
        for (tag_id, relate_tag_id, limit_to) in parent_rows.flatten() {
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

        println!("------------------------------");
    }

    ///
    /// Handles all the processing for files and tags and relational items
    ///
    pub async fn process_scraper(
        self: Arc<Self>,
        map: HashMap<FileInternal, Vec<FileTagAction>>,
        jobs: Vec<ScraperDataReturn>,
    ) {
        // Early Exit
        if map.is_empty() && jobs.is_empty() {
            return;
        }

        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };
            let conn = conn.transaction().unwrap();

            'ScraperLoop: for scraperdatareturn in jobs {
                for skip_conditions in scraperdatareturn.skip_conditions {
                    match skip_conditions {
                        SkipIf::FileHash(_file_hash) => {}
                        SkipIf::FileTagRelationship(tag) => {
                            if let Some(ns_id) =
                                Self::internal_namespace_get_id(&conn, &tag.namespace.name)
                                && let Some(tag_id) =
                                    Self::internal_tag_get_id(&conn, &tag.name, ns_id)
                                    && Self::internal_tag_has_files(&conn, tag_id) {
                                        info!("DB Skipping adding job due to FileTagRelationship tag_id: {} having files.", tag_id);
                                        continue 'ScraperLoop;
                                    }
                        }
                        SkipIf::FileNamespaceNumber((_tag, _namespace, _id)) => {}
                        SkipIf::NoFilesDownloaded => {}
                    }
                }

                Self::internal_jobs_add(&conn, &scraperdatareturn.job);
            }

            let unique_files: HashSet<FileInternal> = map.keys().cloned().collect();
            let resolved_files = Self::internal_file_bulk_add(&conn, unique_files);

            // Build a quick, zero-allocation lookup mapping: FileInternal -> Database u64 ID
            let mut file_cache = HashMap::with_capacity(resolved_files.len());
            for file in &resolved_files {
                if let Some(db_id) = file.id {
                    file_cache.insert(file.hash.clone(), db_id);
                }
            }

            // Collect all action definitions across every file block into one flat vector
            let all_tag_actions: Vec<FileTagAction> = map.values().flatten().cloned().collect();

            let tag_cache = Self::internal_tag_bulk_add(&conn, &all_tag_actions);

            let file_ids: Vec<u64> = file_cache.values().copied().collect();
            let current_file_relationships =
                Self::internal_file_id_get_tag_ids_bulk(&conn, &file_ids).unwrap();

            let mut rels_to_add = HashSet::new();
            let mut rels_to_del = HashSet::new();

            let mut current_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
            let mut incoming_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
            let mut explicit_adds = HashSet::new();
            let mut set_deletions = HashSet::new();

            let mut tag_id_to_obj = HashMap::with_capacity(tag_cache.len());
            for (tag_obj, &tag_id) in &tag_cache {
                tag_id_to_obj.insert(tag_id, tag_obj);
            }

            for (file_internal, tag_list) in map.iter() {
                let file_id = match file_cache.get(&file_internal.hash) {
                    Some(&id) => id,
                    None => continue,
                };

                current_ns_tags.clear();
                explicit_adds.clear();
                set_deletions.clear();

                // Map current database state for this file: Namespace (&str) -> HashSet<TagId>
                if let Some(current_tag_ids) = current_file_relationships.get(&file_id) {
                    for &tag_id in current_tag_ids {
                        // Instantly resolve the full Tag object using the raw ID
                        if let Some(tag) = tag_id_to_obj.get(&tag_id) {
                            let ns_name = &tag.namespace.name;

                            if ns_name != "source_url" && !ns_name.is_empty() {
                                current_ns_tags
                                    .entry(ns_name.as_str()) // Zero allocations!
                                    .or_default()
                                    .insert(tag_id);
                            }
                        }
                    }
                }
                // Process operations
                for tag_action in tag_list {
                    match tag_action.operation {
                        TagOperation::Add => {
                            for tag in &tag_action.tags {
                                if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                    && let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                        rels_to_add.insert((file_id, tag_id));
                                        explicit_adds.insert(tag_id);
                                    }
                            }
                        }
                        TagOperation::Del => {
                            for tag in &tag_action.tags {
                                if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                    && let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                        rels_to_del.insert((file_id, tag_id));
                                    }
                            }
                        }
                        TagOperation::Set => {
                            incoming_ns_tags.clear();

                            for tag in &tag_action.tags {
                                if !matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                {
                                    continue;
                                }
                                let ns_name = &tag.tag.namespace.name;
                                if ns_name == "source_url" || ns_name.is_empty() {
                                    continue;
                                }

                                if let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                    incoming_ns_tags
                                        .entry(ns_name.as_str())
                                        .or_default()
                                        .insert(tag_id);

                                    rels_to_add.insert((file_id, tag_id));
                                }
                            }

                            // Evaluate deletions ONLY for namespaces explicitly targeted by this Set operation
                            for (ns_name, incoming_set) in &incoming_ns_tags {
                                if let Some(current_tag_ids) = current_ns_tags.get(ns_name) {
                                    for current_tag_id in current_tag_ids {
                                        if !incoming_set.contains(current_tag_id) {
                                            set_deletions.insert((file_id, *current_tag_id));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Apply targeted "Add overrides Set" rule
                for (f_id, tag_id) in &set_deletions {
                    if !explicit_adds.contains(tag_id) {
                        rels_to_del.insert((*f_id, *tag_id));
                    }
                }
            }

            // Global sanitation check for any edge deletions
            for del in &rels_to_del {
                rels_to_add.remove(del);
            }

            // 6️⃣ Step 5: Flush Relationship Mutations to DB in Batch
            if !rels_to_del.is_empty() {
                Self::internal_relationship_bulk_delete(self.clone(), &conn, &rels_to_del);
            }

            if !rels_to_add.is_empty() {
                Self::internal_relationship_bulk_add(self, &conn, &rels_to_add);
            }

            conn.commit().unwrap();
        })
        .await
        .unwrap()
    }
    ///
    /// Checks if we should download the file or not
    ///
    pub async fn jobs_update(&self, job: &DbJobsObj) {
        let pool = self.pool.clone();

        let job = job.clone();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };

            Self::internal_jobs_update(&conn, &job)
        })
        .await
        .unwrap();
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
    /// Gets a single fileid from a tag
    ///
    pub async fn tag_get_fileid(&self, tag: &Tag) -> Option<u64> {
        let pool = self.pool.clone();

        let tag = tag.clone();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };

            Self::internal_tag_get_fileid(&conn, &tag)
        })
        .await
        .unwrap()
    }
    ///
    /// Gets a file if its id exists in db
    ///
    pub async fn file_id_get(&self, file_id: u64) -> Option<FileInternal> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {:?}", e);
                    panic!();
                }
            };

            Self::internal_file_id_get(&conn, &file_id).ok()
        })
        .await
        .unwrap()
    }
    ///
    /// Adds relationship into db
    ///
    pub async fn add_relationship_bulk(self: Arc<Self>, rel_list: HashSet<(u64, u64)>) {
        if rel_list.is_empty() {
            return;
        }

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

            Self::internal_relationship_bulk_add(self, &tn, &rel_list);
            tn.commit().unwrap();
        })
        .await
        .unwrap()
    }
    ///
    /// Deletes relationship into db
    ///
    pub async fn delete_relationship_bulk(self: Arc<Self>, rel_list: HashSet<(u64, u64)>) {
        if rel_list.is_empty() {
            return;
        }

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

            Self::internal_relationship_bulk_delete(self, &tn, &rel_list);
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
    /// Returns the full location of where a file should be stored
    ///
    pub async fn file_ids_get_tags(&self, file_ids: &HashSet<u64>) -> HashMap<u64, HashSet<Tag>> {
        // If our hash is less then 6 cant return a location
        if file_ids.is_empty() {
            return HashMap::new();
        }
        let file_ids = file_ids.clone();
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().unwrap();
            Self::internal_file_ids_get_tags(&conn, &file_ids)
        })
        .await
        .ok()
        .unwrap()
    }

    pub fn search_db_files_sync(&self, search: &SearchObj, limit: &Option<u64>) -> Vec<u64> {
        use rusqlite::params_from_iter;
        use std::time::Instant;

        let start_time = Instant::now();

        // 1. Extract and Categorize Tags
        let mut and_tags = Vec::new();
        let mut or_groups: Vec<Vec<u64>> = Vec::new();
        let mut not_groups: Vec<Vec<u64>> = Vec::new();

        for holder in search.searches.clone() {
            match holder {
                SearchHolder::And(ids) => and_tags.extend(ids),
                SearchHolder::Or(ids) if !ids.is_empty() => or_groups.push(ids),
                SearchHolder::Not(ids) if !ids.is_empty() => not_groups.push(ids),
                _ => {}
            }
        }

        if and_tags.is_empty() && or_groups.is_empty() {
            return vec![];
        }

        let conn = self.pool.get().unwrap();
        // 2. PATH A: Roaring Bitmap Optimization (Memory Speed)
        let read_guard = self.relationship_roaring_storage.read();
        if let Some(ref roaring) = *read_guard {
            let mut should_quick_search = true;

            for and_tag in and_tags.iter() {
                if !roaring.relationship_cache_tagid_exists(&conn, *and_tag) {
                    should_quick_search = false;
                    break;
                }
            }
            let start_time = Instant::now();
            if should_quick_search {
                let results = SearchQuery::new(roaring)
                    .sort()
                    .limit(*limit)
                    .and_search(&and_tags)
                    .build();

                println!("Roaring Search took: {:?}", start_time.elapsed());
                return results;
            }
        }

        // 3. PATH B: Optimized SQL (Database Speed)
        // If cache is off, we use Inner Joins on the rarest tag to minimize index lookups.

        // Sort AND tags by rarity using the 'count' column in Tags table
        let mut sorted_and = and_tags;
        if sorted_and.len() > 1 {
            let placeholders = vec!["?"; sorted_and.len()].join(",");
            let count_sql = format!(
                "SELECT id FROM Tags WHERE id IN ({}) ORDER BY count ASC",
                placeholders
            );
            if let Ok(mut stmt) = conn.prepare(&count_sql) {
                let ids: Vec<u64> = stmt
                    .query_map(params_from_iter(&sorted_and), |r| r.get(0))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                if !ids.is_empty() {
                    sorted_and = ids;
                }
            }
        }

        let mut params = Vec::new();
        let driver_tag = sorted_and[0];

        // We start the query with our rarest tag
        let mut sql = "SELECT r0.fileid FROM Relationship r0".to_string();

        // Only add JOINs if there are more AND tags
        for (i, tag) in sorted_and.iter().skip(1).enumerate() {
            let alias = format!("r{}", i + 1);
            sql.push_str(&format!(
                " JOIN Relationship {0} ON r0.fileid = {0}.fileid AND {0}.tagid = ?",
                alias
            ));
            params.push(*tag);
        }

        // Start conditions with the Driver Tag
        sql.push_str(" WHERE r0.tagid = ?");
        params.push(driver_tag);

        // Add OR groups
        for (i, group) in or_groups.iter().enumerate() {
            let placeholders = vec!["?"; group.len()].join(",");
            sql.push_str(&format!(
        " AND EXISTS (SELECT 1 FROM Relationship or{} WHERE or{}.fileid = r0.fileid AND or{}.tagid IN ({}))", 
        i, i, i, placeholders
    ));
            for &tag_id in group {
                params.push(tag_id);
            }
        }

        // Add NOT groups
        for (i, group) in not_groups.iter().enumerate() {
            let placeholders = vec!["?"; group.len()].join(",");
            sql.push_str(&format!(
        " AND NOT EXISTS (SELECT 1 FROM Relationship not{} WHERE not{}.fileid = r0.fileid AND not{}.tagid IN ({}))", 
        i, i, i, placeholders
    ));
            for &tag_id in group {
                params.push(tag_id);
            }
        }

        // Finalize
        sql.push_str(" ORDER BY r0.fileid DESC");

        if let Some(l) = limit {
            sql.push_str(" LIMIT ?");
            params.push(*l);
        }
        dbg!(&sql, &params);

        let mut stmt = conn
            .prepare(&sql)
            .ok()
            .expect("Unable to prepare a db search");
        let results: Vec<u64> = stmt
            .query_map(params_from_iter(params), |row| row.get(0))
            .ok()
            .expect(" Unable to querymap")
            .filter_map(|r| r.ok())
            .collect();

        println!("SQL Search took: {:?}", start_time.elapsed());
        results
    }

    /// A sync function to get a function
    pub fn setting_get_sync(&self, name: &str) -> Option<DbSettingsObj> {
        let pool = self.pool.clone();
        let conn = pool.get().ok()?;
        Self::internal_setting_get(&conn, name).ok().flatten()
    }

    ///
    /// What everything else uses when getting a setting
    ///
    pub async fn setting_get(self: Arc<Self>, name: String) -> Option<shared_types::DbSettingsObj> {
        let name = name.clone();
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.setting_get_sync(&name))
            .await
            .ok()
            .flatten() // Flattens the JoinError wrapper Option as well
    }

    pub fn setting_set_sync(&self, obj: &DbSettingsObj) -> bool {
        let pool = self.pool.clone();
        let conn = pool.get().ok().unwrap();
        Self::internal_setting_set(&conn, obj).ok().is_some()
    }

    ///
    /// What anything outside of the db uses to set a setting
    ///
    pub async fn setting_set(self: Arc<Self>, obj: shared_types::DbSettingsObj) -> bool {
        let obj = obj.clone();
        let _self_clone = self.clone();
        tokio::task::spawn_blocking(move || self.setting_set_sync(&obj))
            .await
            .ok()
            .is_some()
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
        .await
        .unwrap();
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
        .await
        .unwrap();
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
        if tags.is_empty() {
            return HashMap::new();
        }
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
    pub async fn file_add_bulk(&self, tags: HashSet<FileInternal>) -> HashSet<FileInternal> {
        if tags.is_empty() {
            return HashSet::new();
        }
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
            let out_tags = Self::internal_file_bulk_add(&tn, tags_owned);

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
    use parking_lot::lock_api::RwLock;
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

        let main_db = Arc::new(MainDatabase {
            pool,
            namespace_cache: Arc::new(RwLock::new(HashMap::new())),
        });
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
        set.insert(ns1.clone());
        set.insert(ns2.clone());

        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // 1. Test insertion
        let ids = MainDatabase::internal_namespace_bulk_add(&conn, &set);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains_key(&ns1));
        assert!(ids.contains_key(&ns2));

        // 2. Test Upsert (ON CONFLICT update description)
        let ns1_updated = GenericNamespaceObj {
            name: "authors".to_string(),
            description: Some("Updated Description".to_string()),
        };

        let mut update_set = HashSet::new();
        update_set.insert(ns1_updated.clone());

        let updated_ids = MainDatabase::internal_namespace_bulk_add(&conn, &update_set);
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
