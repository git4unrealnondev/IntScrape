use std::{collections::HashSet, fs};

use rusqlite::{Connection, params};

use crate::db::MainDatabase;

impl MainDatabase {
    pub fn internal_update_db_5_to_6(
        &self,
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        self.internal_table_create_tag_search_fts_v6(conn)?;
        self.internal_db_version_set(conn, 6)
    }

    ///
    /// Updates the db from Version 1 to Version 2
    ///
    pub fn internal_update_db_1_to_2(
        &self,
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        MainDatabase::internal_table_create_dead_urls_v1(conn)?;

        self.internal_db_version_set(conn, 2)?;
        Ok(())
    }

    /// Updates the db from Version 2 to Version 3.
    pub fn internal_update_db_2_to_3(
        &self,
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        self.internal_table_create_audit_log_v3(conn)?;
        self.internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_audit_log_enabled".into(),
                description: Some("Whether database changes are recorded in AuditLog.".into()),
                num: Some(1),
                param: None,
            },
        )?;

        // Existing rows predate auditing, so record their current state. The
        // relationship row keeps its indexed IDs in columns instead of
        // duplicating them in JSON.
        conn.execute(
            "INSERT INTO AuditLog
                (changed_at, entity_type, action, file_id, reason)
             SELECT unixepoch(), 'file', 'create', id,
                    'existing file imported during V3 migration'
             FROM File",
            [],
        )?;
        conn.execute(
            "INSERT INTO AuditLog
                (changed_at, entity_type, action, tag_id, reason)
             SELECT unixepoch(), 'tag', 'create', t.id,
                    'existing tag imported during V3 migration'
             FROM Tags t",
            [],
        )?;
        conn.execute(
            "INSERT INTO AuditLog
                (changed_at, entity_type, action, file_id, tag_id, reason)
             SELECT unixepoch(), 'relationship', 'create', file_id, tag_id,
                    'existing relationship imported during V3 migration'
             FROM Relationship",
            [],
        )?;
        self.internal_db_version_set(conn, 3)
    }

    /// Updates the audited global relationship table to namespace partitions.
    pub fn internal_update_db_3_to_4(
        &self,
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        conn.execute_batch(
            "DROP TRIGGER IF EXISTS audit_file_insert;
             DROP TRIGGER IF EXISTS audit_file_delete;
             DROP TRIGGER IF EXISTS audit_file_update;
             DROP TRIGGER IF EXISTS audit_tag_insert;
             DROP TRIGGER IF EXISTS audit_tag_delete;
             DROP TRIGGER IF EXISTS audit_tag_update;
             DROP TRIGGER IF EXISTS audit_parent_insert;
             DROP TRIGGER IF EXISTS audit_parent_delete;
             DROP TABLE IF EXISTS AuditLog;",
        )?;
        self.internal_relationship_migrate_legacy(conn);
        self.internal_db_version_set(conn, 4)
    }

    /// Upates from V4 to V5
    pub fn internal_update_db_4_to_5(
        &self,
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        MainDatabase::internal_table_create_file_hashes_v1(conn);

        conn.execute("ALTER TABLE File ADD COLUMN size_bytes INTEGER;", [])?;

        let mut ns_ids_to_remove = HashSet::new();

        for (ns_name, algo_name) in [
            ("FileHash-MD5", "MD5"),
            ("FileHash-SHA1", "SHA1"),
            ("FileHash-SHA256", "SHA256"),
            ("FileHash-SHA512", "SHA512"),
            ("FileHash-IPFSCID1", "IPFSCID1"),
            ("FileHash-ImageHash", "ImageHash"),
            ("FileHash-IPFSCID", "IPFSCID"),
        ] {
            if let Some(ns_id) = MainDatabase::internal_namespace_get_id(conn, ns_name) {
                let relationship_table = self.relationship_partition_name(ns_id);
                let query = format!(
                    "INSERT OR IGNORE INTO FileHashes (file_id, algorithm, digest)
                     SELECT MIN(r.file_id), ?1, t.name
                     FROM {relationship_table} r
                     JOIN Tags t ON t.id = r.tag_id
                     WHERE t.namespace = ?2
                     GROUP BY r.tag_id"
                );
                conn.execute(&query, params![algo_name, ns_id])?;
                ns_ids_to_remove.insert(ns_id);
            }
        }

        MainDatabase::internal_namespace_bulk_delete(conn, &ns_ids_to_remove)?;

        let file_storage_map = MainDatabase::internal_file_storage_get_all(conn)?;
        let mut files = conn.prepare(
            "SELECT id, hash, extension, storage_id
             FROM File
             WHERE hash IS NOT NULL AND extension IS NOT NULL",
        )?;
        let file_rows = files.query_map([], |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u64>(3)?,
            ))
        })?;
        let mut size_stmt = conn.prepare("UPDATE File SET size_bytes = ?1 WHERE id = ?2")?;

        for row in file_rows {
            let (file_id, hash, extension, storage_id) = row?;
            let file = shared_types::FileInternal {
                id: Some(file_id),
                hash,
                extension,
                storage_id,
                size_bytes: None,
            };
            let mut path = file_storage_map
                .get(&storage_id)
                .and_then(|base_path| MainDatabase::get_file_location(&file, base_path));
            if path.is_none() {
                path = file_storage_map
                    .iter()
                    .filter(|(storage, _)| **storage != storage_id)
                    .find_map(|(_, base_path)| MainDatabase::get_file_location(&file, base_path));
            }
            if let Some(path) = path
                && let Ok(metadata) = fs::metadata(path)
            {
                size_stmt.execute(params![metadata.len(), file_id])?;
            }
        }

        self.internal_db_version_set(conn, 5)
    }
}
