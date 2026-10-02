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

    /// Updates the db from version 7 to 8.
    ///
    /// A v7 database can be carrying a full-table FTS index on `Tags` (an older
    /// `table_create_tags_popular_v1` created one) next to the `Tags_Popular`
    /// shadow index. Search is served entirely by the shadow, so that index was
    /// pure write tax: turso's FTS index method builds a whole immutable
    /// Tantivy segment per statement flush, which made every tag insert in the
    /// scraper tags phase ~2.6x slower and logged `save metas` from inside
    /// tantivy once per chunk. The shadow rows themselves are untouched, and
    /// the shadow's canonical index is left in place — only the stray
    /// full-table index and the duplicate `idx_tags_popular_fts` name go.
    ///
    /// Rebuilding/re-marking the shadow is deliberately left to the boot-time
    /// `table_ensure_tags_popular`, which is idempotent and also covers
    /// databases that never reached this version. Both are safe in either
    /// order.
    pub(in crate::db) async fn update_db_7_to_8(&self, conn: &Connection) -> Result<()> {
        // `idx_tags_fts` is the canonical name for the *shadow* index, so it
        // must only be dropped when it is actually attached to `Tags`.
        let stray_on_tags: i64 = {
            let mut stmt = conn
                .prepare(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'index'
                           AND LOWER(name) = 'idx_tags_fts'
                           AND LOWER(tbl_name) = 'tags'
                     )",
                )
                .await?;
            stmt.query_row(()).await?.get(0)?
        };
        if stray_on_tags != 0 {
            log::info!(
                "Migration 7->8: dropping stray full-table FTS index idx_tags_fts on Tags \
                 (tag search is served by the Tags_Popular shadow)."
            );
            conn.execute_batch("DROP INDEX IF EXISTS idx_tags_fts;")
                .await?;
        }
        // Legacy duplicate FTS index over the same shadow rows. Two FTS indexes
        // on one small table double every shadow write for no read benefit.
        conn.execute_batch("DROP INDEX IF EXISTS idx_tags_popular_fts;")
            .await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(8),
                param: None,
            },
        )
        .await?;

        Ok(())
    }
}
