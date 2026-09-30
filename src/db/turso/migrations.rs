use turso::{Connection, Result};

use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    ///
    /// Updatees the db from version 6 to 7
    ///
    pub(in crate::db) async fn update_db_6_to_7(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
ALTER TABLE Parents RENAME TO Parents_old;
DROP INDEX IF EXISTS idx_tags_fts;
DROP INDEX IF EXISTS idx_parents_rel;
DROP INDEX IF EXISTS idx_unique_parents_null_safe;
DROP TABLE IF EXISTS Tags_Popular;
",
        )
        .await?;

        self.table_create_parents_v1(conn).await?;

        conn.execute_batch("INSERT INTO Parents (tag_id, relate_tag_id, limit_to) SELECT tag_id, relate_tag_id, limit_to FROM Parents_old;
DROP TABLE Parents_old;
").await?;

        self.table_create_tags_popular_v1(conn).await?;

        conn.execute_batch(
            "INSERT INTO Tags_Popular (tag_id, name) SELECT id, name FROM Tags WHERE count >= 5;",
        )
        .await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(7),
                param: None,
            },
        )
        .await?;

        Ok(())
    }
}
