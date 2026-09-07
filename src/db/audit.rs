//! Database operations for the `audit` domain.

use super::MainDatabase;
use rusqlite::Connection;

impl MainDatabase {
    pub fn internal_audit_context_set(
        conn: &Connection,
        reason: &str,
    ) -> Result<(), rusqlite::Error> {
        let _ = (conn, reason);
        Ok(())
    }

    pub fn internal_audit_log(
        conn: &Connection,
        entity_type: &str,
        action: &str,
        file_id: Option<u64>,
        tag_id: Option<u64>,
        reason: &str,
    ) -> Result<(), rusqlite::Error> {
        let _ = (conn, entity_type, action, file_id, tag_id, reason);
        Ok(())
    }
}
