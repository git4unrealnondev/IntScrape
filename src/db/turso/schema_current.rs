use turso::{Connection, Result};

use crate::db::turso::TursoDatabase;

/// One-time rebuild trigger for the popular-tag FTS shadow.
///
/// Every graceful ensure records this in `Settings(name='fts_shadow_schema'),
/// and a Database whose marker differs from this constant gets a wholesale
/// shadow rebuild on its next boot, so all segments match the on-disk FTS
/// schema this binary writes.
///
/// Limbo changed its tantivy index format between pre-releases: since
/// `turso_core 0.8.0-pre.12` the schema carries `doc_identity_hi` /
/// `doc_identity_lo` FAST fields, and the merge path requires them on every
/// segment. Indexes written by < pre.12 lack them and the first write (not a
/// search) fails with `FTS segment ... has no document identity high column`.
/// **Bump this constant whenever the turso/limbo FTS index format changes** —
/// e.g. `"pre12-docid"` when the database is served by a core that writes the
/// identity columns.
pub(in crate::db::turso) const FTS_SHADOW_SCHEMA_GENERATION: &str = "pre11-v1";
const FTS_SHADOW_MARKER_NAME: &str = "fts_shadow_schema";

impl TursoDatabase {
    /// Creates the file tables.
    pub(in crate::db::turso) async fn table_create_file(&self, conn: &Connection) {
        conn.execute_batch("
CREATE TABLE File 
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
CREATE INDEX idx_file_hash ON File (hash);

").await;
    }

    pub(in crate::db::turso) async fn table_create_filestoragelocations(&self, conn: &Connection) {
        conn.execute_batch("CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY , location TEXT NOT NULL UNIQUE);").await;
    }

    /// Creates the filehash table
    pub(in crate::db::turso) async fn table_create_filehash(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE FileHashes (
    file_id INTEGER NOT NULL,
    algorithm TEXT NOT NULL,
    digest TEXT NOT NULL,

    PRIMARY KEY (file_id, algorithm),

    FOREIGN KEY (file_id)
        REFERENCES File(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);
CREATE UNIQUE INDEX idx_file_hashes_algorithm_digest ON FileHashes (algorithm, digest);

",
        )
        .await;
    }

    pub(in crate::db::turso) async fn table_create_jobs(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE Jobs (
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
CREATE UNIQUE INDEX idx_jobs_dedup ON Jobs (time, reptime, site, param);
CREATE INDEX idx_jobs_ready_priority ON Jobs (site, is_running, priority DESC, time, id);
",
        )
        .await;
    }
    pub(in crate::db::turso) async fn table_create_namespace(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE Namespace (
    id INTEGER PRIMARY KEY , 
    name TEXT NOT NULL UNIQUE, 
    description TEXT
);
CREATE INDEX idx_namespace ON Namespace (name);
",
        )
        .await;
    }
    pub(in crate::db::turso) async fn table_create_parents(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("
CREATE TABLE Parents (
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

CREATE INDEX idx_parents_lim ON Parents (limit_to);
CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);
CREATE UNIQUE INDEX idx_unique_parents_null_safe ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));
").await
    }
    pub(in crate::db::turso) async fn table_create_tags(&self, conn: &Connection) {
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
",
        )
        .await;
    }

    /// Ensures the popular-tag FTS shadow exists and is indexed. Runs on every
    /// boot via `check_db` (not only on fresh creation, since `create_db` only
    /// fires when the database file is brand new).
    ///
    /// The Tantivy-backed FTS index only covers popular tags: search is backed
    /// by the small `Tags_Popular` table holding exactly the `count >= 5`
    /// rows, so the ngram index stays tiny and unpopular tags never show up
    /// in autocomplete. Limbo rejects a partial `USING fts` index (`WHERE
    /// count >= 5`), so the filter lives in the shadow table instead. The
    /// shadow is kept in sync by the count-change sites in relationship.rs and
    /// rebuilt wholesale at the end of every slurp.
    ///
    /// First boot / upgrade: create the shadow, mirror already-popular tags,
    /// drop the old Tags-level index (moved onto the shadow), build it and
    /// merge segments once, then record the shadow's schema generation in
    /// Settings. A shadow whose generation marker differs from this binary's
    /// (built by an older core, or a boot that died before the marker landed)
    /// gets a one-time wholesale rebuild — the tantivy index format changed
    /// between limbo pre-releases (≥ 0.8.0-pre.12 requires identity fast
    /// fields on every segment), and a search probe cannot detect a
    /// write-path schema mismatch. Steady state with a matching marker:
    /// verify-only — make sure the index exists and actually resolves, but
    /// never rebuild or OPTIMIZE a healthy index. A registered-but-torn index
    /// (process died mid-rebuild) is detected by the probe query and rebuilt,
    /// so databases already touched by a broken boot heal on their next
    /// restart without manual SQL.
    ///
    /// Both branches report their errors instead of being discarded: a failed
    /// rebuild here leaves search permanently dead (`fts_match` has no index),
    /// and `check_db` swallows failures, so this is the only place the failure
    /// can surface in the logs.
    pub(in crate::db::turso) async fn table_ensure_tags_popular(
        &self,
        conn: &Connection,
    ) -> Result<()> {
        let shadow_existed: i64 = {
            let mut stmt = conn
                .prepare(
                    // Limbo lowercases identifiers in sqlite_master, so this
                    // comparison must be case-insensitive. A case-sensitive
                    // `name = 'Tags_Popular'` returns 0 on every boot and
                    // silently re-runs the whole DROP/rebuild/OPTIMIZE cycle.
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'table' AND LOWER(name) = 'tags_popular'
                     )",
                )
                .await
                .expect("table_ensure_tags_popular shadow probe");
            stmt.query_row(())
                .await
                .expect("table_ensure_tags_popular shadow probe row")
                .get(0)
                .expect("table_ensure_tags_popular shadow probe value")
        };
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Tags_Popular (
    tag_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
",
        )
        .await?;
        if shadow_existed == 0 {
            conn.execute_batch(
                "INSERT OR IGNORE INTO Tags_Popular(tag_id, name)
                 SELECT id, name FROM Tags WHERE count >= 5;
                 DROP INDEX IF EXISTS idx_tags_fts;
                 CREATE INDEX IF NOT EXISTS idx_tags_fts ON Tags_Popular USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
                 OPTIMIZE INDEX idx_tags_fts;",
            )
            .await?;
            self.fts_shadow_write_marker(conn).await?;
        } else {
            let marker = self.fts_shadow_read_marker(conn).await?;
            if marker != Some(FTS_SHADOW_SCHEMA_GENERATION.to_string()) {
                // The shadow exists but its segments were written by an older
                // index format (or a boot died before the marker landed). The
                // search probe below cannot detect this — the read path never
                // touches the identity columns the merge path requires — so
                // rebuild wholesale so every segment matches this binary's
                // schema, then record the generation.
                log::warn!(
                    "Popular-tag FTS shadow was built by an older core (schema marker {marker:?}, want {FTS_SHADOW_SCHEMA_GENERATION}); rebuilding it once."
                );
                conn.execute_batch(
                    "DROP TABLE IF EXISTS Tags_Popular;
                     CREATE TABLE Tags_Popular (
                         tag_id INTEGER PRIMARY KEY,
                         name TEXT NOT NULL
                     );
                     INSERT INTO Tags_Popular(tag_id, name)
                     SELECT id, name FROM Tags WHERE count >= 5;
                     CREATE INDEX idx_tags_fts ON Tags_Popular USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
                     OPTIMIZE INDEX idx_tags_fts;",
                )
                .await?;
                self.fts_shadow_write_marker(conn).await?;
            } else {
                // Steady state: make sure the index exists (no-op on healthy
                // DBs), then confirm it actually resolves. Tantivy segments
                // live inside the DB file, so a crash mid-rebuild can leave
                // the index registered in sqlite_master but torn — `IF NOT
                // EXISTS` alone would never repair that, and neither would the
                // case-sensitive probe on an already-upgraded database.
                conn.execute_batch(
                    "CREATE INDEX IF NOT EXISTS idx_tags_fts ON Tags_Popular USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);",
                )
                .await?;
                // Cheap probe: one term-dictionary lookup, no document scan,
                // so steady-state boots stay fast.
                let mut healthy = false;
                if let Ok(mut rows) = conn
                    .query(
                        "SELECT fts_score(name, ?1)
                         FROM Tags_Popular
                         WHERE fts_match(name, ?1)
                         LIMIT 1;",
                        ("zq9",),
                    )
                    .await
                {
                    healthy = rows.next().await.is_ok();
                }
                if !healthy {
                    log::warn!(
                        "Popular-tag FTS index is registered but does not resolve; rebuilding it."
                    );
                    conn.execute_batch(
                        "DROP INDEX IF EXISTS idx_tags_fts;
                         CREATE INDEX idx_tags_fts ON Tags_Popular USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
                         OPTIMIZE INDEX idx_tags_fts;",
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Reads `Settings(name = 'fts_shadow_schema').param` directly (bypasses
    /// the cache — the ensure runs at boot before `load_cache`).
    async fn fts_shadow_read_marker(&self, conn: &Connection) -> Result<Option<String>> {
        let mut rows = conn
            .query(
                "SELECT param FROM Settings WHERE name = ?1 LIMIT 1;",
                (FTS_SHADOW_MARKER_NAME,),
            )
            .await?;
        if let Some(row) = rows.next().await? {
            Ok(row.get(0)?)
        } else {
            Ok(None)
        }
    }

    /// Records that this binary built the popular-tag FTS shadow with its own
    /// index format. Written only after a successful build/rebuild, so a
    /// failed rebuild stays unmarked and retries next boot.
    async fn fts_shadow_write_marker(&self, conn: &Connection) -> Result<()> {
        conn.execute(
            "INSERT OR REPLACE INTO Settings (name, description, num, param)
             VALUES (?1, NULL, NULL, ?2);",
            (FTS_SHADOW_MARKER_NAME, FTS_SHADOW_SCHEMA_GENERATION.to_string()),
        )
        .await?;
        Ok(())
    }

    pub(in crate::db::turso) async fn table_create_dead_urls(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE dead_urls (url TEXT PRIMARY KEY);
",
        )
        .await;
    }

    pub(in crate::db::turso) async fn table_create_settings(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE Settings (
    name TEXT PRIMARY KEY,
    description TEXT, 
    num INTEGER, 
    param TEXT
);

",
        )
        .await;
    }
}
