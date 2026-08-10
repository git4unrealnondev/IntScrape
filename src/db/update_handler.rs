use rusqlite::Connection;

use crate::db::MainDatabase;

impl MainDatabase {
    ///
    /// Updates the db from Version 1 to Version 2
    ///
    pub(in crate::db) fn internal_update_db_1_to_2(
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        Self::internal_table_create_dead_urls_v1(conn)?;

        Self::internal_db_version_set(conn, 2)?;
        Ok(())
    }

    /// Updates the db from Version 2 to Version 3.
    pub(in crate::db) fn internal_update_db_2_to_3(
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        Self::internal_table_create_audit_log_v3(conn)?;
        Self::internal_setting_set(
            conn,
            &shared_types::DbSettingsObj {
                name: "SYSTEM_audit_log_enabled".into(),
                description: Some("Whether database changes are recorded in AuditLog.".into()),
                num: Some(1),
                param: None,
            },
        )?;

        // Existing rows predate auditing, so record their current state as the
        // initial V3 value rather than leaving the audit history incomplete.
        conn.execute(
            "INSERT INTO AuditLog
                (changed_at, entity_type, entity_id, action, before_json, after_json, reason)
             SELECT unixepoch(), 'file', CAST(id AS TEXT), 'create', NULL,
                    json_object('id', id, 'hash', hash, 'extension', extension,
                                'storage_id', storage_id),
                    'existing file imported during V3 migration'
             FROM File",
            [],
        )?;
        conn.execute(
            "INSERT INTO AuditLog
                (changed_at, entity_type, entity_id, action, before_json, after_json, reason)
             SELECT unixepoch(), 'tag', CAST(t.id AS TEXT), 'create', NULL,
                    json_object('id', t.id, 'name', t.name, 'namespace', t.namespace),
                    'existing tag imported during V3 migration'
             FROM Tags t",
            [],
        )?;
        conn.execute(
            "INSERT INTO AuditLog
                (changed_at, entity_type, entity_id, action, before_json, after_json, reason)
             SELECT unixepoch(), 'relationship', r.file_id || ':' || r.tag_id, 'create', NULL,
                    json_object('file_id', r.file_id, 'tag_id', r.tag_id),
                    'existing relationship imported during V3 migration'
             FROM Relationship r",
            [],
        )?;
        Self::internal_db_version_set(conn, 3)
    }
}
