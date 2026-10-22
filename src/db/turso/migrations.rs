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

    /// Updates the db from version 8 to 9.
    /// An v8 database carries `idx_file_hash` and `idx_namespace`, which
    /// duplicate the unique indexes SQLite already builds for the `UNIQUE`
    /// `File.hash` and `Namespace.name` columns. The planner never selects
    /// them — `EXPLAIN QUERY PLAN` resolves both lookups to
    /// `sqlite_autoindex_File_1` / `sqlite_autoindex_Namespace_1` — so they
    /// were pure write tax: every `File` insert and every hash `UPDATE`
    /// maintained a second byte-identical b-tree for no read benefit.
    ///
    /// Set-based and restart-safe (`DROP INDEX IF EXISTS`), and a no-op on a
    /// database already created without them. The indexes are not the only
    /// path to those columns' uniqueness, so dropping them cannot relax the
    /// `UNIQUE` constraints.
    pub(in crate::db) async fn update_db_8_to_9(&self, conn: &Connection) -> Result<()> {
        log::info!(
            "Migration 8->9: dropping idx_file_hash and idx_namespace \
             (both duplicate an index the UNIQUE constraints already create)."
        );
        self.drop_redundant_indexes(conn).await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(9),
                param: None,
            },
        )
        .await?;

        Ok(())
    }

    /// Updates the db from version 9 to 10.
    ///
    /// Re-stores `Parents` around what the probes and list reads actually
    /// need. Measured in `db_bench_parents_store` (release, MVCC, fresh db per
    /// variant, 100k edges):
    ///
    /// ```text
    ///                       novel   re-add  fwd   rev    exact
    /// A v9 (expr key + rel)  14.70    3.56  56.4  47.7    44.4
    /// v10 (plain + covering) 13.17    3.33  43.4  43.7    23.1
    /// ```
    ///
    /// - `limit_to` switches from nullable to `NOT NULL DEFAULT -1`. That
    ///   replaces the `IFNULL(limit_to, -1)` *expression* dedupe index with a
    ///   plain composite key, so inserts stop recomputing the expression
    ///   (novel inserts ~10% faster, exact triple probes ~2x faster) and the
    ///   forward list becomes an index-only read. Tag ids are rowids that
    ///   start at 1, so `-1` is an unambiguous "no limit tag".
    /// - `idx_parents_rel` grows to `(relate_tag_id, tag_id, limit_to)` so
    ///   reverse reads (`child_relationships_get`,
    ///   `parent_relate_limit_exists`) stop hopping through the rowid to the
    ///   table b-tree.
    ///
    /// The copy is set-based and deduplicates on the same effective key the
    /// v9 expression index used (`IFNULL(limit_to, -1)`), so a NULL-limit row
    /// from any older generation and the sentinel encoding collapse to one
    /// row. Without-rowid storage was evaluated and is not available: turso
    /// 0.8.2 rejects `CREATE INDEX` on `WITHOUT ROWID` tables, and the reverse
    /// direction needs a secondary index.
    pub(in crate::db) async fn update_db_9_to_10(&self, conn: &Connection) -> Result<()> {
        log::info!(
            "Migration 9->10: rebuilding Parents with the sentinel limit_to and a covering reverse index."
        );
        conn.execute_batch(
            "ALTER TABLE Parents RENAME TO Parents_old;
             DROP INDEX IF EXISTS idx_parents_rel;
             DROP INDEX IF EXISTS idx_unique_parents_null_safe;
             DROP INDEX IF EXISTS idx_unique_parents;",
        )
        .await?;

        self.table_create_parents_v2(conn).await?;

        conn.execute_batch(
            "INSERT INTO Parents (tag_id, relate_tag_id, limit_to)
             SELECT tag_id, relate_tag_id, IFNULL(limit_to, -1)
             FROM Parents_old
             GROUP BY tag_id, relate_tag_id, IFNULL(limit_to, -1);

             DROP TABLE Parents_old;",
        )
        .await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(10),
                param: None,
            },
        )
        .await?;

        Ok(())
    }

    /// Updates the db from version 10 to 11.
    ///
    /// The `Tags` table had no index on `namespace` — the `UNIQUE(name,
    /// namespace)` index cannot serve a `WHERE namespace = ?` probe because
    /// namespace is its second column. Every namespace-scoped read (thread
    /// listing, board totals, tag-id fetches) full-scanned the table, which
    /// cost 30-90s on production archives with millions of tags. This adds a
    /// plain b-tree on `namespace` so those reads become index lookups.
    pub(in crate::db) async fn update_db_10_to_11(&self, conn: &Connection) -> Result<()> {
        log::info!(
            "Migration 10->11: adding idx_tags_namespace to Tags \
             (namespace lookups previously full-scanned the table)."
        );
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_tags_namespace ON Tags (namespace);",
        )
        .await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(11),
                param: None,
            },
        )
        .await?;

        Ok(())
    }
}
