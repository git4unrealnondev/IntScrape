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
}
