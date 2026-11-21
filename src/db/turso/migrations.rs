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
                .prepare_cached(
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
        // `idx_parents_lim` is dropped for the same reason as the others:
        // it follows the renamed table, and `table_create_parents_v2`
        // recreates it under the same name, which would collide
        // ("index already exists") on any database that still carries it.
        conn.execute_batch(
            "ALTER TABLE Parents RENAME TO Parents_old;
             DROP INDEX IF EXISTS idx_parents_rel;
             DROP INDEX IF EXISTS idx_unique_parents_null_safe;
             DROP INDEX IF EXISTS idx_unique_parents;
             DROP INDEX IF EXISTS idx_parents_lim;",
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
        conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_tags_namespace ON Tags (namespace);")
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

    /// Updates the db from version 12 to 13.
    ///
    /// Moves the SHA-512 digest off `File` (where it was stored as the
    /// `hash` column, duplicated in `FileHashes` under `SHA512`) and onto
    /// `FileHashes` alone, keyed by `file_id`:
    ///
    /// 1. Backfill missing `SHA512` rows from `File.hash` (files whose
    ///    digest was already recorded are untouched; `NULL`/empty hashes
    ///    have no digest to backfill and stay hash-less, exactly as before).
    /// 2. Rebuild `File` without the column. `ALTER TABLE ... DROP COLUMN`
    ///    cannot be used here (Turso rejects dropping a `UNIQUE` column),
    ///    and renaming `File` first would repoint `FileHashes`' foreign
    ///    key at the old name, so the new table is created first, filled,
    ///    then the old one dropped and the new one renamed into place —
    ///    `FileHashes`' `REFERENCES File(id)` text resolves to the new
    ///    table throughout (foreign-key enforcement is off in this
    ///    backend, so the brief dangling window is inert).
    /// 3. No explicit indexes existed on the old `File` besides the hash
    ///    unique autoindex (removed with the column, by design), so there
    ///    is nothing to recreate.
    pub(in crate::db) async fn update_db_12_to_13(&self, conn: &Connection) -> Result<()> {
        log::info!(
            "Migration 12->13: moving File.hash into FileHashes (SHA512) and dropping the column."
        );
        // A database created fresh by a v13 binary and then pinned back to
        // v12 for testing already has the new shape: there is no `hash`
        // column to move. Detect that instead of failing on every boot
        // migration step that runs through 12 -> 13.
        let has_hash_column: bool = {
            let mut stmt = conn
                .prepare(
                    "SELECT EXISTS(SELECT 1 FROM pragma_table_info('File') WHERE name = 'hash');",
                )
                .await?;
            stmt.query_row(()).await?.get::<bool>(0)?
        };
        if has_hash_column {
            // Canonicalize to uppercase first: a pre-existing lowercase
            // `FileHashes` SHA512 row (slurped library, plugin-supplied
            // identifying hash) must not shadow the uppercase `File.hash`
            // that every pre-v13 read returned. `INSERT OR IGNORE` alone
            // kept the old row (the `(file_id, algorithm)` primary key
            // conflicts) and then dropped `File.hash`, so `file_get`
            // started returning the lowercase digest. `UPDATE OR IGNORE`
            // skips only a row whose uppercased digest would collide with
            // a case-variant twin on the unique index — such a twin keeps
            // its existing digest rather than failing the migration.
            // The backfill likewise uppercases, matching the canonical
            // form `file_hashes_add_bulk` now enforces.
            conn.execute_batch(
                "UPDATE OR IGNORE FileHashes SET digest = UPPER(digest)
                 WHERE algorithm = 'SHA512';
                 INSERT OR IGNORE INTO FileHashes (file_id, algorithm, digest)
                 SELECT id, 'SHA512', UPPER(hash) FROM File
                 WHERE hash IS NOT NULL AND hash <> '';",
            )
            .await?;

            conn.execute_batch(
                "CREATE TABLE File_new
                 (id INTEGER PRIMARY KEY NOT NULL,
                  extension TEXT,
                  storage_id INTEGER,
                  size_bytes INTEGER,
                  FOREIGN KEY (storage_id) REFERENCES FileStorageLocations(id)
                      ON DELETE CASCADE ON UPDATE CASCADE);
                 INSERT INTO File_new (id, extension, storage_id, size_bytes)
                     SELECT id, extension, storage_id, size_bytes FROM File;
                 DROP TABLE File;
                 ALTER TABLE File_new RENAME TO File;",
            )
            .await?;
        }

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(13),
                param: None,
            },
        )
        .await?;

        Ok(())
    }

    /// Updates the db from version 13 to 14.
    ///
    /// Canonicalizes stored hex digests to the casing the rest of the
    /// codebase already uses: SHA-1/SHA-256/SHA-512 uppercase (files on
    /// disk are named with the uppercase SHA-512), MD5 lowercase.
    /// A v13 binary could store — and, since v13 reads join
    /// `FileHashes`, return — a lowercase SHA-512 digest verbatim from
    /// a slurped library or a plugin-supplied identifying hash.
    /// `UPDATE OR IGNORE` leaves a row whose canonical digest would
    /// collide with a case-variant twin on the
    /// `(algorithm, digest)` unique index untouched instead of failing
    /// the migration; the twins keep their existing digest.
    /// Updates the db from version 14 to 15.
    ///
    /// Adds the flat `Relationship` table and backfills it from every
    /// legacy `Relationship_{ns}` partition.
    ///
    /// The partitions were introduced so the *import* path could filter rows
    /// per namespace without scanning: the slurp's own notes record ~9.5
    /// hours of source reads against a single shared table. They never
    /// helped the read path -- a file's tags can live in any namespace, so
    /// every partition had to be probed anyway, through a `UNION ALL` whose
    /// SQL text grew with the namespace count. Measured against a 32-arm
    /// fixture, the flat table answers the same lookup 22x-236x faster, and
    /// it writes ~17% faster too.
    ///
    /// The legacy partitions are **kept in the schema** for one release, and
    /// backfilled above, so a rollback does not need a restore. The default
    /// write mode is `single`: only the flat table is written, which is the
    /// point of the migration. Flip `relationship_write_mode` to `dual` to
    /// keep the partitions in step (and `relationship_read_mode` to `legacy`
    /// to read them) -- but note that a rollback needs the partitions
    /// refilled first, since rows written under `single` never reached them.
    ///
    /// The backfill is set-based and restartable: one `INSERT OR IGNORE ...
    /// SELECT ... FROM Relationship_{ns}` per partition, with the namespace as
    /// a literal per arm. Conflicts on the flat table's primary key make a
    /// re-run a no-op, and the engine streams the scan/insert server-side, so
    /// nothing has to be chunked to bound statement size or memory.
    pub(in crate::db) async fn update_db_14_to_15(&self, conn: &Connection) -> Result<()> {
        log::info!(
            "Migration 14->15: adding the flat Relationship table and backfilling it \
             from the per-namespace partitions."
        );

        self.table_create_relationship(conn).await;

        // Every legacy partition, in id order. The namespace id is the suffix
        // of the table name, which is also the namespace the rows belong to:
        // a relationship row is routed to the partition of its *tag's*
        // namespace, and a tag's namespace never changes.
        let mut partitions: Vec<(String, u64)> = Vec::new();
        {
            let mut stmt = conn
                .prepare(
                    "SELECT name FROM sqlite_master
                     WHERE type = 'table' AND lower(name) LIKE 'relationship\\_%' ESCAPE '\\';",
                )
                .await?;
            let mut rows = stmt.query(()).await?;
            while let Some(row) = rows.next().await? {
                let name: String = row.get(0)?;
                let lowered = name.to_ascii_lowercase();
                let Some(suffix) = lowered.strip_prefix("relationship_") else {
                    continue;
                };
                let Ok(namespace_id) = suffix.parse::<u64>() else {
                    continue;
                };
                partitions.push((name, namespace_id));
            }
        }
        partitions.sort_by_key(|(_, namespace_id)| *namespace_id);

        for (table, namespace_id) in &partitions {
            // One set-based statement per partition. The flat table's primary
            // key is `(tag_id, file_id)` and `INSERT OR IGNORE` conflicts on
            // exactly that, so a re-run (or a partition whose rows already
            // exist) is a no-op rather than a duplicate -- the copy stays
            // restartable without any Rust-side cursor.
            //
            // The namespace is a literal per arm: the partitions are separate
            // tables, so a single `SELECT` cannot read more than one; the loop
            // is over partitions, not over chunks. The engine streams the scan
            // and insert server-side, so memory is O(1) regardless of how many
            // rows a partition holds -- unlike the old keyset-paginated copy,
            // which pulled every row into a `Vec` and rebuilt a
            // `VALUES (?,?,?),...` statement per chunk.
            let copied = conn
                .execute(
                    format!(
                        "INSERT OR IGNORE INTO Relationship (file_id, tag_id, namespace)
                         SELECT file_id, tag_id, {namespace_id} FROM {table}"
                    ),
                    (),
                )
                .await?;
            log::info!(
                "Migration 14->15: backfilled {copied} rows from {table} into Relationship."
            );
        }

        // The rollout defaults: write only the flat table, read from it. The
        // legacy partitions stay in the schema and are still backfilled above,
        // so a rollback to `dual` + `legacy` has the pre-migration data it
        // needs -- only the rows written after the switch would be missing,
        // which is what the boot warning is about.
        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: super::relationship_store::WRITE_MODE_SETTING.to_string(),
                description: Some(
                    "Which tables a relationship write touches: single, dual or legacy."
                        .to_string(),
                ),
                num: None,
                param: Some("single".to_string()),
            },
        )
        .await?;
        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: super::relationship_store::READ_MODE_SETTING.to_string(),
                description: Some(
                    "Which tables a relationship read uses: flat or legacy.".to_string(),
                ),
                num: None,
                param: Some("flat".to_string()),
            },
        )
        .await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(15),
                param: None,
            },
        )
        .await?;

        Ok(())
    }

    pub(in crate::db) async fn update_db_13_to_14(&self, conn: &Connection) -> Result<()> {
        log::info!(
            "Migration 13->14: canonicalizing FileHashes digest casing (SHA* uppercase, MD5 lowercase)."
        );
        conn.execute_batch(
            "UPDATE OR IGNORE FileHashes SET digest = UPPER(digest)
             WHERE algorithm IN ('SHA1', 'SHA256', 'SHA512');
             UPDATE OR IGNORE FileHashes SET digest = LOWER(digest)
             WHERE algorithm = 'MD5';",
        )
        .await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(14),
                param: None,
            },
        )
        .await?;

        Ok(())
    }

    /// Updates the db from version 11 to 12.
    ///
    /// The v10 rebuild of `Parents` dropped `idx_parents_lim` when it
    /// recreated the table's index set, leaving every `WHERE limit_to = ?`
    /// read (the chan thread reverse index) with nothing but a full table
    /// scan. This restores the index in the v10 covering style, so the
    /// limited-relationship reads — including the bulk form — are index
    /// seeks that return their rows without touching the table b-tree.
    /// Set-based and restart-safe (`CREATE INDEX IF NOT EXISTS`).
    pub(in crate::db) async fn update_db_11_to_12(&self, conn: &Connection) -> Result<()> {
        log::info!(
            "Migration 11->12: restoring idx_parents_lim on Parents \
             (limit_to reads full-scanned after the v10 rebuild)."
        );
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_parents_lim
                 ON Parents (limit_to, tag_id, relate_tag_id);",
        )
        .await?;

        self.setting_set(
            conn,
            shared_types::DbSettingsObj {
                name: "SYSTEM_VERSION".to_string(),
                description: None,
                num: Some(12),
                param: None,
            },
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Migration tests. These build a v14-shaped database by hand and run the
    //! upgrade, because the backfill's pagination is the part that can fail
    //! silently: a cursor that does not advance re-reads the first chunk
    //! forever, and one that skips rows leaves relationships missing.

    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use turso::Builder;

    async fn temp_db_path(label: &str) -> std::path::PathBuf {
        let dir = tempfile::tempdir().unwrap();
        // Leak the guard: the path has to outlive the call, and the test
        // process cleans /tmp anyway.
        let path = dir.keep();
        path.join(format!("intscrape-mig-{label}-{}.db", std::process::id()))
    }

    fn remove_db(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        for suffix in ["-log", "-wal", "-shm"] {
            let mut side = path.to_path_buf().into_os_string();
            side.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(side));
        }
    }

    /// A v14 database: namespaces, tags, files, and per-namespace relationship
    /// partitions with no flat `Relationship` table.
    async fn build_v14(path: &std::path::Path, namespaces: usize, rows_per_ns: u64) {
        remove_db(path);
        let db = Builder::new_local(&path.to_string_lossy())
            .experimental_index_method(true)
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();
        conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();

        let mut ddl = String::from(
            "CREATE TABLE Namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
             CREATE TABLE Tags (
                 id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL,
                 namespace INTEGER NOT NULL,
                 count INTEGER NOT NULL DEFAULT 0,
                 UNIQUE(name, namespace));
             CREATE TABLE File (id INTEGER PRIMARY KEY NOT NULL, extension TEXT,
                 storage_id INTEGER, size_bytes INTEGER);
             CREATE TABLE Settings (name TEXT PRIMARY KEY, description TEXT, num INTEGER, param TEXT);
             CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
             CREATE TABLE FileHashes (file_id INTEGER NOT NULL, algorithm TEXT NOT NULL, digest TEXT NOT NULL,
                 PRIMARY KEY (file_id, algorithm));
             CREATE TABLE dead_urls (url TEXT PRIMARY KEY);
             CREATE TABLE Jobs (id INTEGER PRIMARY KEY NOT NULL, time INTEGER NOT NULL,
                 reptime INTEGER NOT NULL, priority INTEGER NOT NULL,
                 is_running BOOL NOT NULL DEFAULT False, recreation TEXT NOT NULL,
                 site TEXT NOT NULL, param TEXT NOT NULL, user_data TEXT NOT NULL);",
        );
        // The partition table name is the *namespace row id*, which is
        // 1-based, not a zero-based loop index.
        for ns in 1..=namespaces {
            ddl.push_str(&format!(
                "CREATE TABLE Relationship_{ns} (
                     file_id INTEGER NOT NULL,
                     tag_id INTEGER NOT NULL,
                     PRIMARY KEY (tag_id, file_id));
                 CREATE INDEX idx_Relationship_{ns}_tag_file
                     ON Relationship_{ns} (file_id);"
            ));
        }
        conn.execute_batch(&ddl).await.unwrap();

        for ns in 1..=namespaces {
            conn.execute(
                "INSERT INTO Namespace (id, name) VALUES (?1, ?2);",
                turso::params_from_iter([
                    turso::Value::from(ns as i64),
                    turso::Value::from(format!("ns{}", ns - 1)),
                ]),
            )
            .await
            .unwrap();
        }
        for ns in 1..=namespaces {
            for row in 0..rows_per_ns {
                // Ids must be unique across the whole fixture: the backfill
                // dedupes on the flat table's (tag_id, file_id) primary key,
                // so reusing an id in two partitions would silently drop rows.
                let tag_id = (ns as i64 - 1) * rows_per_ns as i64 + row as i64 + 1;
                let file_id = (ns as i64 - 1) * rows_per_ns as i64 + row as i64 + 1;
                conn.execute(
                    "INSERT OR IGNORE INTO Tags (id, name, namespace) VALUES (?1, ?2, ?3);",
                    turso::params_from_iter([
                        turso::Value::from(tag_id),
                        turso::Value::from(format!("tag{tag_id}")),
                        turso::Value::from(ns as i64),
                    ]),
                )
                .await
                .unwrap();
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO Relationship_{ns} (file_id, tag_id) VALUES (?1, ?2);"
                    ),
                    turso::params_from_iter([
                        turso::Value::from(file_id),
                        turso::Value::from(tag_id),
                    ]),
                )
                .await
                .unwrap();
            }
        }
        // Mark the schema so `check_db` knows it has to run 14 -> 15.
        conn.execute(
            "INSERT INTO Settings (name, description, num, param)
             VALUES ('SYSTEM_VERSION', NULL, 14, NULL);",
            (),
        )
        .await
        .unwrap();
    }

    /// The 14 -> 15 backfill must copy every row, across a chunk boundary, and
    /// stamp each with the namespace its partition belongs to.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn migration_14_to_15_backfills_every_partition_row() {
        // Deliberately more rows than one SQL_CHUNK_SIZE so the pagination
        // cursor has to advance more than once.
        let rows_per_ns = crate::db::SQL_CHUNK_SIZE as u64 * 2 + 7;
        let namespaces = 3;
        let path = temp_db_path("backfill").await;
        build_v14(&path, namespaces, rows_per_ns).await;

        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&path, should_exit).await;
        let conn = db.connect().unwrap();

        // The migration ran during `new_with_exit` (check_db).
        let version: i64 = {
            let mut rows = conn
                .query(
                    "SELECT num FROM Settings WHERE name = 'SYSTEM_VERSION';",
                    (),
                )
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        };
        assert_eq!(version, 15, "the migration must record version 15");

        let total: i64 = {
            let mut rows = conn
                .query("SELECT COUNT(*) FROM Relationship;", ())
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        };
        assert_eq!(
            total,
            (rows_per_ns * namespaces as u64) as i64,
            "every partition row must be backfilled"
        );

        // Every row must carry its partition's namespace, which is what keeps
        // the per-namespace recount scoped.
        for ns_id in 1..=namespaces as i64 {
            let scoped: i64 = {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM Relationship WHERE namespace = ?1;",
                        turso::params_from_iter([turso::Value::from(ns_id)]),
                    )
                    .await
                    .unwrap();
                rows.next().await.unwrap().unwrap().get(0).unwrap()
            };
            assert_eq!(
                scoped, rows_per_ns as i64,
                "namespace {ns_id} must backfill {} rows",
                rows_per_ns
            );
        }

        // Re-running must be a no-op, not a duplicate.
        let db2 = TursoDatabase::new_with_exit(&path, Arc::new(AtomicBool::new(false))).await;
        let conn2 = db2.connect().unwrap();
        let total2: i64 = {
            let mut rows = conn2
                .query("SELECT COUNT(*) FROM Relationship;", ())
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        };
        assert_eq!(total2, total, "a re-run must not duplicate rows");

        remove_db(&path);
    }

    /// A database that has never had partitions (fresh v15 shape) must not be
    /// disturbed by the migration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn migration_14_to_15_is_a_no_op_without_partitions() {
        let path = temp_db_path("nopart").await;
        remove_db(&path);
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&path, should_exit).await;
        let conn = db.connect().unwrap();
        let total: i64 = {
            let mut rows = conn
                .query("SELECT COUNT(*) FROM Relationship;", ())
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        };
        assert_eq!(total, 0);
        remove_db(&path);
    }
}
