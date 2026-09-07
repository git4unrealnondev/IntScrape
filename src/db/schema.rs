//! Database operations for the `schema` domain.

use super::MainDatabase;
use rusqlite::Connection;
use shared_types::DbSettingsObj;
use std::collections::HashSet;

impl MainDatabase {
    ///
    /// Creates the relationship table for the db
    ///
    pub fn internal_table_create_relationship_v1(&self, conn: &Connection) {
        let namespace_ids: Vec<u64> = conn
            .prepare("SELECT id FROM Namespace ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        for namespace_id in namespace_ids {
            self.internal_relationship_partition_create(conn, namespace_id);
        }
    }

    pub fn internal_relationship_migrate_legacy(&self, conn: &Connection) {
        let legacy_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'Relationship')",
            [],
            |row| row.get(0),
        )
        .unwrap();

        if !legacy_exists {
            return;
        }

        // Rename the table and create an index on tag_id to avoid full table scans
        // across the loop, executing directly on the active connection/transaction.
        conn.execute_batch(
            "ALTER TABLE Relationship RENAME TO Relationship_legacy;
         CREATE INDEX idx_relationship_legacy_tag_id ON Relationship_legacy(tag_id);",
        )
        .unwrap();

        let namespaces: Vec<u64> = conn
            .prepare("SELECT DISTINCT id FROM Namespace;")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .flatten()
            .collect();

        for namespace_id in namespaces {
            dbg!(&namespace_id);
            self.internal_relationship_partition_create(conn, namespace_id);
            let table = self.relationship_partition_name(namespace_id);

            conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO {table} (file_id, tag_id)
                 SELECT r.file_id, r.tag_id FROM Relationship_legacy r
                 JOIN Tags t ON t.id = r.tag_id WHERE t.namespace = ?1"
                ),
                [namespace_id],
            )
            .unwrap();
        }

        conn.execute("DROP TABLE Relationship_legacy", []).unwrap();
    }

    pub fn relationship_partition_name(&self, namespace_id: u64) -> String {
        format!("Relationship_{namespace_id}")
    }

    pub fn internal_relationship_partition_create(&self, conn: &Connection, namespace_id: u64) {
        let table = self.relationship_partition_name(namespace_id);
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                    file_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    PRIMARY KEY (file_id, tag_id),
                    FOREIGN KEY (file_id) REFERENCES File(id) ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_{table}_tag_file ON {table}(tag_id, file_id DESC)"
        ))
        .unwrap();
    }

    pub fn relationship_union_source(&self, conn: &Connection, alias: &str) -> String {
        let tables: Vec<String> = conn
            .prepare("SELECT id FROM Namespace ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                let id: u64 = row.get(0)?;
                Ok(self.relationship_partition_name(id))
            })
            .unwrap()
            .flatten()
            .collect();
        let source = if tables.is_empty() {
            "SELECT NULL AS file_id, NULL AS tag_id WHERE 0".into()
        } else {
            tables
                .iter()
                .map(|table| format!("SELECT file_id, tag_id FROM {table}"))
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        };
        format!("({source}) AS {alias}")
    }

    /// Creates the compact V3 audit trail.
    pub fn internal_table_create_audit_log_v3(
        &self,
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS AuditLog (
                id INTEGER PRIMARY KEY,
                changed_at INTEGER NOT NULL,
                entity_type TEXT NOT NULL,
                action TEXT NOT NULL,
                file_id INTEGER,
                tag_id INTEGER,
                reason TEXT NOT NULL
            );",
        )?;

        let columns = conn
            .prepare("PRAGMA table_info(AuditLog)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<HashSet<_>, _>>()?;
        if !columns.contains("file_id") {
            conn.execute("ALTER TABLE AuditLog ADD COLUMN file_id INTEGER", [])?;
        }
        if !columns.contains("tag_id") {
            conn.execute("ALTER TABLE AuditLog ADD COLUMN tag_id INTEGER", [])?;
        }
        if columns.contains("entity_id")
            || columns.contains("before_json")
            || columns.contains("after_json")
        {
            if columns.contains("entity_id") {
                conn.execute_batch(
                    "UPDATE AuditLog
                     SET file_id = CASE
                         WHEN entity_type = 'file' THEN CAST(entity_id AS INTEGER)
                         ELSE file_id
                     END
                     WHERE file_id IS NULL;
                     UPDATE AuditLog
                     SET tag_id = CASE
                         WHEN entity_type = 'tag' THEN CAST(entity_id AS INTEGER)
                         ELSE tag_id
                     END
                     WHERE tag_id IS NULL;",
                )?;
            }
            conn.execute_batch(
                "CREATE TABLE AuditLog_compact (
                    id INTEGER PRIMARY KEY,
                    changed_at INTEGER NOT NULL,
                    entity_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    file_id INTEGER,
                    tag_id INTEGER,
                    reason TEXT NOT NULL
                );
                INSERT INTO AuditLog_compact
                    (id, changed_at, entity_type, action, file_id, tag_id, reason)
                SELECT id, changed_at, entity_type, action, file_id, tag_id, reason
                FROM AuditLog;
                DROP TABLE AuditLog;
                ALTER TABLE AuditLog_compact RENAME TO AuditLog;",
            )?;
        }
        Ok(())
    }

    pub fn internal_audit_log_indexes_create_v3(
        &self,
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_audit_log_entity;
             DROP INDEX IF EXISTS idx_audit_log_file_id;
             DROP INDEX IF EXISTS idx_audit_log_tag_id;
             CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON AuditLog(changed_at DESC);
             CREATE INDEX IF NOT EXISTS idx_audit_log_file_id ON AuditLog(file_id, changed_at DESC)
                 WHERE file_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_audit_log_tag_id ON AuditLog(tag_id, changed_at DESC)
                 WHERE tag_id IS NOT NULL;",
        )?;

        let partitions: Vec<u64> = conn
            .prepare("SELECT id FROM Namespace")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        for namespace_id in partitions {
            let table = self.relationship_partition_name(namespace_id);
            let insert_trigger = format!(
                "CREATE TEMP TRIGGER IF NOT EXISTS audit_relationship_insert_{namespace_id}
                 AFTER INSERT ON main.{table} BEGIN
                 INSERT INTO AuditLog (changed_at, entity_type, action, file_id, tag_id, reason)
                 VALUES (unixepoch(), 'relationship', 'create', NEW.file_id, NEW.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1), 'relationship added')); END;"
            );
            let delete_trigger = format!(
                "CREATE TEMP TRIGGER IF NOT EXISTS audit_relationship_delete_{namespace_id}
                 AFTER DELETE ON main.{table} BEGIN
                 INSERT INTO AuditLog (changed_at, entity_type, action, file_id, tag_id, reason)
                 VALUES (unixepoch(), 'relationship', 'delete', OLD.file_id, OLD.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1), 'relationship removed')); END;"
            );
            conn.execute_batch(&insert_trigger)?;
            conn.execute_batch(&delete_trigger)?;
        }
        Ok(())
    }

    pub fn internal_audit_triggers_setup(&self, conn: &Connection) -> Result<(), rusqlite::Error> {
        conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS AuditContext (
                reason TEXT NOT NULL
            );

            CREATE TEMP TRIGGER IF NOT EXISTS audit_file_insert
            AFTER INSERT ON main.File
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, file_id, reason)
                VALUES (
                    unixepoch(), 'file', 'create', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'file created')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_file_delete
            AFTER DELETE ON main.File
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, file_id, reason)
                VALUES (
                    unixepoch(), 'file', 'delete', OLD.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'file deleted')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_file_update
            AFTER UPDATE ON main.File
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, file_id, reason)
                VALUES (
                    unixepoch(), 'file', 'update', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'file updated')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_tag_insert
            AFTER INSERT ON main.Tags
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'tag', 'create', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag created')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_tag_delete
            AFTER DELETE ON main.Tags
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'tag', 'delete', OLD.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag deleted')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_tag_update
            AFTER UPDATE ON main.Tags
            WHEN OLD.name IS NOT NEW.name OR OLD.namespace IS NOT NEW.namespace
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'tag', 'update', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag updated')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_parent_insert
            AFTER INSERT ON main.Parents
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'relationship', 'create', NEW.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag parent relationship added')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_parent_delete
            AFTER DELETE ON main.Parents
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'relationship', 'delete', OLD.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag parent relationship removed')
                );
            END;",
        )
    }

    ///
    /// Handles creating the triggers to manage the count in the Tags column
    ///
    pub fn internal_trigger_create_relationship_v1(conn: &Connection) {
        let _ = conn;
    }

    ///
    /// Creates the current default Tags table
    ///
    pub fn internal_table_create_tags_v1(&self, conn: &Connection) {
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
    tokenize = 'trigram' 
);

-- OPTIMIZATION: Only insert if it meets the threshold
 CREATE TRIGGER IF NOT EXISTS tags_ai AFTER INSERT ON Tags
WHEN new.count = 5
BEGIN
    INSERT INTO Tags_Popular_fts(rowid, name, namespace) 
    VALUES (new.id, new.name, new.namespace);
END;

CREATE TRIGGER IF NOT EXISTS tags_count_au AFTER UPDATE OF count ON Tags
WHEN old.count < 5 AND new.count >= 5
BEGIN
    INSERT INTO Tags_Popular_fts(rowid, name, namespace)
    VALUES (new.id, new.name, new.namespace);
END;

CREATE TRIGGER IF NOT EXISTS tags_count_ad AFTER UPDATE OF count ON Tags
WHEN old.count >= 5 AND new.count < 5
BEGIN
    INSERT INTO Tags_Popular_fts(Tags_Popular_fts, rowid, name, namespace)
    VALUES ('delete', old.id, old.name, old.namespace);
END;

-- OPTIMIZATION: Only attempt FTS delete if the old row actually qualified to be in there
CREATE TRIGGER IF NOT EXISTS tags_ad AFTER DELETE ON Tags 
WHEN old.count >= 5
BEGIN
    INSERT INTO Tags_Popular_fts(Tags_Popular_fts, rowid, name, namespace) 
    VALUES('delete', old.id, old.name, old.namespace);
END;
",
        )
        .unwrap();
        self.internal_table_create_tag_search_fts_v6(conn).unwrap();
    }

    pub fn internal_table_create_tag_search_fts_v6(
        &self,
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        conn.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS Tags_Search_fts USING fts5(
                 name,
                 content='Tags',
                 content_rowid='id',
                 tokenize='trigram'
             );
             CREATE TRIGGER IF NOT EXISTS tags_search_ai AFTER INSERT ON Tags BEGIN
                 INSERT INTO Tags_Search_fts(rowid, name) VALUES (new.id, new.name);
             END;
             CREATE TRIGGER IF NOT EXISTS tags_search_ad AFTER DELETE ON Tags BEGIN
                 INSERT INTO Tags_Search_fts(Tags_Search_fts, rowid, name)
                 VALUES ('delete', old.id, old.name);
             END;
             CREATE TRIGGER IF NOT EXISTS tags_search_au AFTER UPDATE OF name ON Tags BEGIN
                 INSERT INTO Tags_Search_fts(Tags_Search_fts, rowid, name)
                 VALUES ('delete', old.id, old.name);
                 INSERT INTO Tags_Search_fts(rowid, name) VALUES (new.id, new.name);
             END;",
        )?;
        let indexed: u64 =
            conn.query_row("SELECT count(*) FROM Tags_Search_fts", [], |row| row.get(0))?;
        let tags: u64 = conn.query_row("SELECT count(*) FROM Tags", [], |row| row.get(0))?;
        if indexed != tags {
            conn.execute(
                "INSERT INTO Tags_Search_fts(Tags_Search_fts) VALUES ('rebuild')",
                [],
            )?;
        }
        Ok(())
    }

    ///
    /// Creates the current default Namespace table
    ///
    pub fn internal_table_create_namespace_v1(conn: &Connection) {
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
    pub fn internal_table_create_settings_v1(conn: &Connection) {
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
    pub fn internal_table_create_parents_v1(conn: &Connection) {
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

-- Stupid fix so we can have NULL limit_to to match on NULLs
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_parents_null_safe ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));

",
        )
        .unwrap();
    }

    ///
    /// Stores file locaitons to an ID
    ///
    pub fn internal_table_create_file_storage_locations_v1(conn: &Connection) {
        conn.execute_batch("
CREATE TABLE IF NOT EXISTS FileStorageLocations (id INTEGER PRIMARY KEY , location TEXT NOT NULL UNIQUE);

").unwrap();
    }

    ///
    /// Creates a dead url table
    ///
    pub fn internal_table_create_dead_urls_v1(
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS dead_urls (url TEXT PRIMARY KEY);")
    }

    ///
    /// Creates the default File table
    ///
    pub fn internal_table_create_file_v2(conn: &Connection) {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS File 
            (id INTEGER PRIMARY KEY  NOT NULL, 
            hash TEXT UNIQUE, 
            extension TEXT, 
            storage_id INTEGER, 
            size_bytes INTEGER

            CHECK (
                (hash IS NOT NULL AND extension IS NOT NULL) OR
                (hash IS NULL AND extension IS NULL)
            ),

            FOREIGN KEY (storage_id) REFERENCES FileStorageLocations(id) ON DELETE CASCADE ON UPDATE CASCADE
            );

CREATE INDEX IF NOT EXISTS idx_file_hash ON File (hash);
").unwrap();
    }

    /// Creates the filehash table
    pub fn internal_table_create_file_hashes_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS FileHashes (
    file_id INTEGER NOT NULL,
    algorithm TEXT NOT NULL,
    digest TEXT NOT NULL,

    PRIMARY KEY (file_id, algorithm),

    FOREIGN KEY (file_id)
        REFERENCES File(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_hashes_algorithm_digest
ON FileHashes (algorithm, digest);
",
        )
        .unwrap();
    }

    ///
    /// Creates the default Jobs table
    ///
    pub fn internal_table_create_jobs_v1(conn: &Connection) {
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

CREATE INDEX IF NOT EXISTS idx_jobs_ready_priority
ON Jobs (site, is_running, priority DESC, time, id);
",
        )
        .unwrap();
    }

    ///
    /// Sets the default file download location
    ///
    pub fn internal_file_download_location_set_default(
        &self,
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        let default_files_location = "files";

        if self
            .internal_setting_get(conn, "SYSTEM_file_location")?
            .is_none()
        {
            self.internal_setting_set(
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
    /// Convience function to set db version
    ///
    pub fn internal_db_version_set(
        &self,
        conn: &Connection,
        version: u64,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        self.internal_setting_set(
            conn,
            &DbSettingsObj {
                name: "SYSTEM_VERSION".into(),
                description: Some("Current version that the DB is on.".into()),
                num: Some(version),
                param: None,
            },
        )
    }
}
