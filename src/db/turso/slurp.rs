//! Turso-native database import. Unlike the legacy SQLite implementation there
//! is no `ATTACH DATABASE`: the source is opened read-only through turso and
//! every row is streamed into the destination database via the bulk-add helpers.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

use shared_types::{
    DbJobRecreation, FileInternal, GenericNamespaceObj, PluginJob, ScraperParam, Tag, TagParents,
};
use turso::{params_from_iter, Connection, Result, Value};

use crate::db::turso::TursoDatabase;
use crate::db::SQL_CHUNK_SIZE;

/// Destination batch size for the tags stage. Imported tags arrive in source
/// id order, which is random relative to the (name, namespace) unique key, so
/// small per-chunk transactions spend most of their time re-searching that
/// index; 25k-row batches measured best on the slurp target hardware.
const SLURP_TAG_BATCH: i64 = 25_000;

/// Destination batch size for the relationship copy, per read and per target
/// namespace partition.
const SLURP_RELATIONSHIP_BATCH: usize = 25_000;

/// Keyset scans build `{column} > last` from a seed of 0, which silently drops
/// a source row whose id is 0 (some hydrus databases genuinely assign a File
/// the id 0, main.db's 6.8M-row File table has one). Seed the first read at
/// -1 so `> -1` also covers id 0, then advance with real ids.
fn keyset_bound(first_pass: bool, last: u64) -> i64 {
    if first_pass {
        -1
    } else {
        last as i64
    }
}

/// A temporary sanitized copy of a slurp source. Removed on drop, including
/// when the slurp retries or bails out part-way.
struct SlurpSourceTemp {
    path: PathBuf,
}

impl Drop for SlurpSourceTemp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Runs the sqlite3 CLI with the given arguments (extra arguments are joined
/// with spaces and executed as SQL, exactly like the interactive tool).
fn sqlite3_cli(args: &[&str]) -> std::result::Result<(), String> {
    match std::process::Command::new("sqlite3").args(args).output() {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(format!(
            "sqlite3 exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )),
        Err(error) => Err(format!("cannot spawn sqlite3: {error}")),
    }
}

/// Runs the sqlite3 CLI and returns its stdout on success.
fn sqlite3_cli_output(args: &[&str]) -> std::result::Result<String, String> {
    match std::process::Command::new("sqlite3").args(args).output() {
        Ok(output) if output.status.success() => {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        }
        Ok(output) => Err(format!(
            "sqlite3 exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )),
        Err(error) => Err(format!("cannot spawn sqlite3: {error}")),
    }
}

impl TursoDatabase {
    /// Copies the supported data from another SQLite database into turso.
    pub async fn db_slurp(&self, source: &Path) -> Result<(u64, u64, u64)> {
        if !source.is_file() {
            return Err(turso::Error::ConversionFailure(
                "source must be a file".into(),
            ));
        }

        let (sauce, _slurp_source_temp) = self.open_slurp_source(source).await?;
        let source_conn = sauce.connect()?;

        // Background pollers (system-job spawner) must stop reading the
        // destination while the import runs: an open reader snapshot keeps the
        // WAL from truncating, which slows every checkpoint as the import
        // grows. The IPC server is paused and drained so the import's
        // BEGIN IMMEDIATE transactions never wait on a UI request. The flags
        // are cleared even when the loop bails on a non-MVCC error so a failed
        // slurp never leaves the app paused behind it.
        self.set_slurping(true);
        self.ipc_pause().await;
        // Run the whole import under the classic WAL journal, then flip back to
        // MVCC when it finishes. A large autocommit (the post-copy index
        // rebuild, the Tags_slurp swap) under MVCC materializes the entire
        // delta in the in-memory commit log -- measured pinned RAM with zero
        // WAL progress for a ~15M-tag import. WAL streams b-tree builds to
        // temp files and checkpointed pages instead (~7x faster index builds,
        // ~2x faster inserts on the slurp builder). check_db re-applies
        // journal_mode=mvcc on the next boot, so a crash mid-import self-heals
        // even if the restore below is skipped.
        let wal_fast_lane = self.try_set_journal_mode("wal").await;
        if wal_fast_lane {
            log::info!("Turso slurp using WAL journal mode (MVCC commit-log fast lane).");
        } else {
            log::warn!(
                "Turso slurp could not switch off MVCC; falling back to the in-memory \
                 commit-log path (slower index builds)."
            );
        }
        let result = loop {
            match self.internal_db_slurp(&source_conn).await {
                Ok(result) => break Ok(result),
                Err(error)
                    if matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) =>
                {
                    log::warn!("Turso slurp transaction conflicted; retrying in 50ms: {error}");
                    // A failed batch is rolled back when its connection drops.
                    // Re-seed caches before restarting the import attempt.
                    if let Err(reload_error) = self.file_storage_location_cache_reload().await {
                        log::warn!(
                            "Failed to reload storage-location cache after conflict: {reload_error}"
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => break Err(error),
            }
        };
        if wal_fast_lane {
            self.checkpoint_wal().await;
            if self.try_set_journal_mode("mvcc").await {
                log::info!("Turso slurp restored MVCC journal mode.");
            } else {
                log::warn!(
                    "Turso slurp could not restore MVCC; check_db re-applies it on the next boot."
                );
            }
        }
        self.ipc_resume().await;
        self.set_slurping(false);
        result
    }

    /// Opens a turso database handle for the slurp source. When the source's
    /// schema contains virtual-table rows the limbo parser cannot load — for
    /// example an FTS5 table whose stored SQL quotes the tokenizer arguments
    /// with double quotes — the direct open fails with a parse error. In that
    /// case the source is backed up with the sqlite3 CLI, the virtual tables
    /// and triggers are dropped from the copy, and the sanitized copy is
    /// opened instead. The returned guard removes the copy when the slurp
    /// finishes.
    async fn open_slurp_source(
        &self,
        source: &Path,
    ) -> Result<(turso::Database, Option<SlurpSourceTemp>)> {
        match turso::Builder::new_local(&source.to_string_lossy())
            .read_only(true)
            .experimental_without_rowid(true)
            .build()
            .await
        {
            Ok(db) => Ok((db, None)),
            Err(open_error) => {
                log::warn!(
                    "Turso cannot open slurp source {}; retrying through a sanitized copy: {open_error}",
                    source.display()
                );
                let temp = match Self::sanitize_slurp_source_copy(source).await {
                    Ok(temp) => temp,
                    Err(sanitize_error) => {
                        log::warn!("Slurp source sanitization failed: {sanitize_error}");
                        return Err(open_error);
                    }
                };
                match turso::Builder::new_local(&temp.path.to_string_lossy())
                    .read_only(true)
                    .experimental_without_rowid(true)
                    .build()
                    .await
                {
                    Ok(db) => Ok((db, Some(temp))),
                    Err(error) => {
                        log::warn!("Sanitized slurp source copy still cannot be opened: {error}");
                        Err(open_error)
                    }
                }
            }
        }
    }

    /// Copies `source` to a temp file and drops every virtual table and
    /// trigger from the copy. The slurp only reads plain tables, so removing
    /// FTS virtual tables (and every trigger, which may reference them) from
    /// a disposable copy is safe. The copy is made with the `.backup` dot
    /// command so a WAL-mode source is checkpointed into one consistent
    /// standalone file. Returns a guard that removes the copy on drop.
    ///
    /// The sqlite3 CLI ships with the project's production image (see the
    /// Dockerfile) and most Linux distributions.
    async fn sanitize_slurp_source_copy(source: &Path) -> Result<SlurpSourceTemp> {
        let unique = format!(
            "intscrape_slurp_{}_{:x}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        );
        let copy = std::env::temp_dir().join(unique);
        let source_str = source.to_string_lossy().into_owned();
        let copy_str = copy.to_string_lossy().into_owned();

        if let Err(error) = sqlite3_cli(&[&source_str, &format!(".backup {copy_str}")]) {
            let _ = std::fs::remove_file(&copy);
            return Err(turso::Error::ConversionFailure(format!(
                "sqlite3 .backup of {} failed: {error}",
                source.display()
            )));
        }

        // Default `list` mode separates columns with `|`, one row per line.
        let query = "SELECT type, name FROM sqlite_master \
                     WHERE (type = 'table' AND rootpage = 0) OR type = 'trigger'";
        let output = match sqlite3_cli_output(&[&copy_str, query]) {
            Ok(output) => output,
            Err(error) => {
                let _ = std::fs::remove_file(&copy);
                return Err(turso::Error::ConversionFailure(format!(
                    "sqlite3 schema scan of {} failed: {error}",
                    copy.display()
                )));
            }
        };

        let mut drops = String::new();
        for line in output.lines() {
            let mut parts = line.splitn(3, '|');
            let (Some(kind), Some(name), None) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            let statement = match kind {
                "table" => format!("DROP TABLE IF EXISTS {quoted};"),
                "trigger" => format!("DROP TRIGGER IF EXISTS {quoted};"),
                _ => continue,
            };
            drops.push_str(&statement);
            drops.push('\n');
        }

        if !drops.is_empty() {
            if let Err(error) = sqlite3_cli(&[&copy_str, &drops]) {
                let _ = std::fs::remove_file(&copy);
                return Err(turso::Error::ConversionFailure(format!(
                    "sqlite3 schema sanitization of {} failed: {error}",
                    copy.display()
                )));
            }
        }

        Ok(SlurpSourceTemp { path: copy })
    }

    /// Sets the destination's journal mode and confirms the resulting mode.
    /// The pragma is database-wide, so a busy pool connection can reject the
    /// change; returning false lets the caller keep running (under mvcc) rather
    /// than abort the import.
    async fn try_set_journal_mode(&self, mode: &str) -> bool {
        let Ok(conn) = self.connect() else {
            return false;
        };
        match conn
            .pragma_update("journal_mode", &format!("'{mode}'"))
            .await
        {
            Ok(_) => {}
            Err(error) => {
                log::warn!("Turso journal_mode={mode} rejected: {error}");
                return false;
            }
        }
        let mut actual = String::new();
        if conn
            .pragma_query("journal_mode", |row| {
                actual = row.get::<String>(0).unwrap_or_default();
                Ok(())
            })
            .await
            .is_err()
        {
            return false;
        }
        actual.eq_ignore_ascii_case(mode)
    }

    /// Best-effort fold of the WAL back into the main file before restoring
    /// MVCC, so the mvcc layer does not have to ingest a growing WAL.
    async fn checkpoint_wal(&self) {
        let Ok(conn) = self.connect() else {
            return;
        };
        if let Err(error) = conn
            .pragma_query("wal_checkpoint(TRUNCATE)", |_row| Ok(()))
            .await
        {
            log::warn!("Turso WAL checkpoint before MVCC restore failed: {error}");
        }
    }

    /// Blocking variant of [`Self::db_slurp`] for use from async contexts that
    /// must keep their own future `Send` (the turso source connection shares
    /// this runtime's IO handles, so holding it while awaiting the destination
    /// helper could deadlock on the single-threaded blocking runtime).
    pub fn db_slurp_blocking(&self, source: &Path) -> Result<(u64, u64, u64)> {
        super::api::block_on(self.db_slurp(source))
    }

    /// Streams the source database's namespaces, tags, files, hashes,
    /// relationships, and parents into turso.
    async fn internal_db_slurp(&self, source: &Connection) -> Result<(u64, u64, u64)> {
        let mut conn = self.connect()?;

        // Namespaces, remembering id -> object and name -> target id.
        let slurp_started = Instant::now();
        let mut ns_by_source_id: HashMap<u64, GenericNamespaceObj> = HashMap::new();
        let mut namespace_set: HashSet<GenericNamespaceObj> = HashSet::new();
        {
            let mut stmt = source
                .prepare("SELECT id, name, description FROM Namespace")
                .await?;
            let mut rows = stmt.query(()).await?;
            while let Some(row) = rows.next().await? {
                let id: u64 = row.get(0)?;
                let ns = GenericNamespaceObj {
                    name: row.get(1)?,
                    description: row.get(2)?,
                };
                ns_by_source_id.insert(id, ns.clone());
                namespace_set.insert(ns);
            }
        }

        // Ensuring namespaces runs CREATE TABLE for their Relationship_N
        // partitions — DDL, which turso forbids inside BEGIN CONCURRENT.
        // `namespace_ensure_set` does it in a short exclusive transaction up
        // front (also seeding the in-memory namespace cache) so the immediate
        // copy below only ever executes DML, and so its snapshot sees the
        // committed namespace rows. Now largely a no-op once namespaces are
        // cached (the common warm-db case).
        let namespace_bulk = self.namespace_ensure_set(&namespace_set).await?;
        let namespace_count = namespace_bulk.len() as u64;
        log::info!(
            "Slurp stage namespaces complete: {} namespaces in {:?}",
            namespace_count,
            slurp_started.elapsed()
        );

        // Storage locations: re-use whatever rows exist, creating the rest.
        // Keep each batch short so scraper writes can commit between batches.
        let mut source_storage_ids = HashMap::new();
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        let mut locations = source
            .prepare("SELECT id, location FROM FileStorageLocations")
            .await?;
        let mut location_rows = locations.query(()).await?;
        while let Some(row) = location_rows.next().await? {
            let source_id: u64 = row.get(0)?;
            let location: String = row.get(1)?;
            let target_id = self
                .file_storage_location_get_or_create(&conn, &location)
                .await?;
            source_storage_ids.insert(source_id, target_id);
        }
        drop(location_rows);
        conn.execute("COMMIT", ()).await?;
        log::info!(
            "Slurp stage storage locations complete in {:?}",
            slurp_started.elapsed()
        );

        // Tags, chunked by id, building the source -> target tag map.
        let mut slurp_tags: HashMap<u64, u64> = HashMap::new();
        let mut tag_count = 0_u64;
        // New rows the destination actually absorbed. The relationship stage
        // below reads it (together with its own inserted-relationship count)
        // to skip recounting when nothing changed: recounting rewrites every
        // Tags.count row, which maintains the ngram index per row and is
        // pathological on destinations that already carry idx_tags_fts.
        let mut tags_added = 0_usize;
        {
            // Tags arrive in source id order, random relative to the
            // UNIQUE(name, namespace) key. Warm imports stage new rows in a
            // constraint-free Tags_slurp copy so inserts never maintain the
            // unique key per row, then swap the canonical table aside and back
            // afterwards (and rebuild the n-gram FTS index once at the very
            // end), resolving existing ids through the indexed Tags table.
            // Cold imports on an empty destination have nothing to resolve and
            // no index to re-create: they insert straight into the indexed
            // Tags table and rebuild only the dropped covering/FTS indexes at
            // the end (see below).

            // Recover from a crash between the swap steps of a previous run:
            // the canonical table is moved aside before the new one takes its
            // name, so a leftover Tags_old is the last good copy of Tags.
            {
                let mut check = conn
                    .prepare(
                        "SELECT EXISTS(
                             SELECT 1 FROM sqlite_master
                             WHERE type = 'table' AND name = ?1
                         )",
                    )
                    .await?;
                let has_tags: i64 = check.query_row(("Tags",)).await?.get(0)?;
                let has_old: i64 = check.query_row(("Tags_old",)).await?.get(0)?;
                if has_tags == 0 && has_old != 0 {
                    conn.execute("ALTER TABLE Tags_old RENAME TO Tags", ())
                        .await?;
                }
            }
            conn.execute("DROP TABLE IF EXISTS Tags_slurp", ()).await?;

            // Only a destination that already owns tags needs the per-batch
            // "exists?" lookup; an empty Tags table has nothing to resolve.
            let has_existing_tags: bool = {
                let mut stmt = conn.prepare("SELECT EXISTS(SELECT 1 FROM Tags)").await?;
                let exists: i64 = stmt.query_row(()).await?.get(0)?;
                exists != 0
            };

            // Cold import: on an empty destination there is nothing to resolve and no
            // data to preserve, so the Tags_slurp swap below is skipped
            // entirely. The canonical table (empty, so no dependent rows yet)
            // is recreated without the inline UNIQUE constraint and carries
            // the named idx_tags_name_namespace index created up front on the
            // empty table, exactly the layout the warm path produces. Every
            // 25k-row insert maintains that index incrementally in a small
            // MVCC commit that keeps the WAL flowing; there are no giant
            // post-copy CREATE INDEX autocommits. Inserting into an indexed
            // table costs ~3x per row (measured), but the WAL fast lane more
            // than makes up for the batch writes. The covering and FTS indexes
            // are rebuilt once at the end instead of being maintained per row.
            let cold_import = !has_existing_tags;
            if cold_import {
                conn.execute("DROP TABLE IF EXISTS Tags", ()).await?;
                conn.execute_batch(
                    "CREATE TABLE Tags (
                         id INTEGER PRIMARY KEY ,
                         name TEXT NOT NULL,
                         namespace INTEGER NOT NULL,
                         count INTEGER NOT NULL DEFAULT 0 ,
                         FOREIGN KEY (namespace) REFERENCES Namespace(id)
                             ON DELETE CASCADE ON UPDATE CASCADE
                     );
                     CREATE UNIQUE INDEX idx_tags_name_namespace
                         ON Tags (name, namespace);",
                )
                .await?;
                conn.execute("DROP INDEX IF EXISTS idx_tags_count_covering", ())
                    .await?;
                conn.execute("DROP INDEX IF EXISTS idx_tags_fts", ())
                    .await?;
                log::info!("Slurp tags: cold import, inserting directly into indexed Tags");
            } else {
                conn.execute_batch(
                    "CREATE TABLE Tags_slurp (
                         id INTEGER PRIMARY KEY ,
                         name TEXT NOT NULL,
                         namespace INTEGER NOT NULL,
                         count INTEGER NOT NULL DEFAULT 0 ,
                         FOREIGN KEY (namespace) REFERENCES Namespace(id)
                             ON DELETE CASCADE ON UPDATE CASCADE
                     );",
                )
                .await?;
            }

            // The row-value IN (VALUES ...) syntax triggers a full table scan
            // in the SQLite/turso planner even when a matching unique index
            // exists. Warm destinations instead populate this temporary table
            // once per batch and let the planner use an index-nested-loop JOIN
            // on the unique (name, namespace) constraint. Created up front
            // because CREATE TABLE is DDL and must stay outside the batches'
            // BEGIN IMMEDIATE transactions.
            if has_existing_tags {
                conn.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS _slurp_tags_tmp (
                         name TEXT NOT NULL, namespace INTEGER NOT NULL)",
                    (),
                )
                .await?;
            }

            // New rows are handed explicit ids in the staging table so they
            // never collide with ids already owned by the canonical Tags
            // table: the swap-back step copies every existing row across at
            // its original id, and both id spaces start at 1. The cursor
            // advances across batches; a cold import's freshly recreated
            // (empty) canonical table has max id 0, matching the old
            // auto-assignment behaviour.
            let mut next_insert_id: i64 = {
                let mut stmt = conn.prepare("SELECT COALESCE(MAX(id), 0) FROM Tags").await?;
                stmt.query_row(()).await?.get(0)?
            };
            let mut last_tag_id = 0_u64;
            let mut first_tag_pass = true;
            loop {
                let batch_started = Instant::now();
                let mut stmt = source
                    .prepare(
                        "SELECT s.id, s.name, n.name, n.description
                         FROM Tags s
                         JOIN Namespace n ON n.id = s.namespace
                         WHERE s.id > ?1
                         ORDER BY s.id
                         LIMIT ?2",
                    )
                    .await?;
                let mut rows = stmt
                    .query([keyset_bound(first_tag_pass, last_tag_id), SLURP_TAG_BATCH])
                    .await?;
                let mut batch: Vec<(u64, String, String, Option<String>)> = Vec::new();
                while let Some(row) = rows.next().await? {
                    batch.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?));
                }
                drop(stmt);
                let Some((last_id, _, _, _)) = batch.last() else {
                    break;
                };
                first_tag_pass = false;

                log::info!("Slurping {} tags into the db.", batch.len());
                let target_started = Instant::now();
                conn.execute("BEGIN IMMEDIATE", ()).await?;
                let (mapping, batch_new) = self
                    .slurp_tags_bulk_add(
                        &conn,
                        &batch,
                        has_existing_tags,
                        if cold_import { "Tags" } else { "Tags_slurp" },
                        &mut next_insert_id,
                    )
                    .await?;
                conn.execute("COMMIT", ()).await?;
                tag_count += batch.len() as u64;
                tags_added += batch_new;
                for (source_id, name, namespace, description) in &batch {
                    let key = Tag {
                        name: name.clone(),
                        namespace: GenericNamespaceObj {
                            name: namespace.clone(),
                            description: description.clone(),
                        },
                    };
                    if let Some(&target_id) = mapping.get(&key) {
                        slurp_tags.insert(*source_id, target_id as u64);
                    }
                }
                last_tag_id = *last_id;
                log::info!(
                    "Slurp tags batch complete: {} rows, target {:?}, total {:?}",
                    batch.len(),
                    target_started.elapsed(),
                    batch_started.elapsed()
                );
            }

            // A warm re-slurp that added nothing leaves Tags exactly as it
            // was: every source tag already resolved to an existing id, so
            // Tags_slurp is empty. Re-running the swap would copy all rows
            // across and rebuild both indexes for zero benefit — a
            // multi-gigabyte single-threaded sort in the MVCC layer that takes
            // tens of minutes. Skip it, drop the empty import table, and keep
            // the indexed canonical table in place.
            if cold_import {
                // idx_tags_name_namespace carried every insert up front; the
                // covering index is rebuilt once here (counts were all 0
                // through the copy), and the final stage rebuilds the dropped
                // ngram-FTS index over the whole table.
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tags_count_covering
                     ON Tags(count DESC, name, namespace)",
                    (),
                )
                .await?;
                log::info!(
                    "Slurp tags: cold import inserted {} tags into indexed Tags, rebuilt covering",
                    tag_count
                );
            } else if tags_added == 0 {
                conn.execute("DROP TABLE IF EXISTS Tags_slurp", ()).await?;
                log::info!("Slurp tags: nothing new, kept existing table");
            } else {
                // Swap the import table back under the canonical name and
                // restore uniqueness and the covering index now that the bulk
                // insert (which would re-bloat them per row) is done. Ids
                // keep their values through the rename, so relationship
                // mapping stays valid.
                //
                // The FTS index is dropped first because SQLite rewrites
                // dependent indexes on table renames; it is rebuilt once at
                // the very end. The row copy is done in chunks of
                // SLURP_TAG_BATCH under their own short transactions: a single
                // `INSERT ... SELECT` over all rows is one multi-GB MVCC
                // commit that serializes the whole delta into an in-memory
                // log (measured ~14.5 min and ~11 GB RAM for 15M rows),
                // whereas chunking (~25k per commit) runs ~2x faster and uses
                // ~100x less memory (measured ~6.8 min, ~90 MB).
                conn.execute("DROP INDEX IF EXISTS idx_tags_fts", ())
                    .await?;
                let copy_started = Instant::now();
                let max_copy_id: i64 = {
                    let mut stmt = conn
                        .prepare("SELECT COALESCE(MAX(rowid), 0) FROM Tags")
                        .await?;
                    let mut rows = stmt.query(()).await?;
                    rows.next()
                        .await?
                        .ok_or_else(|| turso::Error::ConversionFailure("no max rowid row".into()))?
                        .get::<i64>(0)?
                };
                let mut last_copy_id: i64 = 0;
                while last_copy_id < max_copy_id {
                    let next = (last_copy_id + SLURP_TAG_BATCH).min(max_copy_id);
                    conn.execute("BEGIN IMMEDIATE", ()).await?;
                    conn.execute(
                        "INSERT INTO Tags_slurp (id, name, namespace, count)
                             SELECT id, name, namespace, count FROM Tags
                             WHERE rowid > ?1 AND rowid <= ?2",
                        (last_copy_id, next),
                    )
                    .await?;
                    conn.execute("COMMIT", ()).await?;
                    last_copy_id = next;
                }
                log::info!("Slurp tags copy complete in {:?}", copy_started.elapsed());
                conn.execute_batch(
                    "ALTER TABLE Tags RENAME TO Tags_old;
                     ALTER TABLE Tags_slurp RENAME TO Tags;
                     DROP TABLE Tags_old;
                     CREATE UNIQUE INDEX idx_tags_name_namespace ON Tags (name, namespace);
                     CREATE INDEX idx_tags_count_covering ON Tags(count DESC, name, namespace);",
                )
                .await?;
            }

            if has_existing_tags {
                conn.execute("DROP TABLE IF EXISTS _slurp_tags_tmp", ())
                    .await?;
            }
        }
        log::info!(
            "Slurp stage tags complete: {} tags in {:?}",
            tag_count,
            slurp_started.elapsed()
        );

        // Files, chunked by id.
        let mut slurp_files: HashMap<u64, u64> = HashMap::new();
        let mut file_count = 0_u64;
        {
            let file_schema: String = {
                let mut stmt = source
                    .prepare(
                        "SELECT sql FROM sqlite_master
                         WHERE type = 'table' AND lower(name) = 'file'",
                    )
                    .await?;
                stmt.query_row(()).await?.get(0)?
            };
            let has_size_bytes = file_schema
                .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .any(|column| column.eq_ignore_ascii_case("size_bytes"));
            let size_column = if has_size_bytes {
                "f.size_bytes"
            } else {
                "NULL"
            };

            let mut last_file_id = 0_u64;
            let mut first_file_pass = true;
            loop {
                let batch_started = Instant::now();
                let file_query = format!(
                    "SELECT f.id, f.hash, f.extension, {size_column}, f.storage_id
                     FROM File f
                     WHERE f.id > ?1 AND f.hash IS NOT NULL
                     ORDER BY f.id
                     LIMIT ?2"
                );
                let mut stmt = source.prepare(&file_query).await?;
                let mut rows = stmt
                    .query([
                        keyset_bound(first_file_pass, last_file_id),
                        (SQL_CHUNK_SIZE * 8) as i64,
                    ])
                    .await?;
                let mut batch: Vec<(u64, String, String, Option<u64>, Option<u64>)> = Vec::new();
                while let Some(row) = rows.next().await? {
                    batch.push((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ));
                }
                drop(stmt);
                let Some((last_id, _, _, _, _)) = batch.last() else {
                    break;
                };
                first_file_pass = false;

                let mut files = HashSet::new();
                for (_, hash, extension, size_bytes, source_storage_id) in &batch {
                    let storage_id = source_storage_id
                        .and_then(|id| source_storage_ids.get(&id).copied())
                        .unwrap_or(0);
                    files.insert(FileInternal {
                        id: None,
                        hash: hash.clone(),
                        extension: extension.clone(),
                        storage_id,
                        size_bytes: *size_bytes,
                    });
                }
                file_count += files.len() as u64;
                log::info!("Slurping {} files into the db.", files.len());
                let target_started = Instant::now();
                conn.execute("BEGIN IMMEDIATE", ()).await?;
                let resolved = self
                    .file_add_bulk(&conn, &files.iter().cloned().collect::<Vec<_>>())
                    .await?;
                conn.execute("COMMIT", ()).await?;
                let target_by_hash: HashMap<&str, u64> = resolved
                    .iter()
                    .filter_map(|file| file.id.map(|id| (file.hash.as_str(), id)))
                    .collect();
                for (source_id, hash, _, _, _) in &batch {
                    if let Some(&target_id) = target_by_hash.get(hash.as_str()) {
                        slurp_files.insert(*source_id, target_id);
                    }
                }

                last_file_id = *last_id;
                log::info!(
                    "Slurp files batch complete: {} rows, target {:?}, total {:?}",
                    files.len(),
                    target_started.elapsed(),
                    batch_started.elapsed()
                );
            }
        }
        log::info!(
            "Slurp stage files complete: {} files in {:?}",
            file_count,
            slurp_started.elapsed()
        );

        // Secondary hashes, keyed through the source file id.
        {
            let has_file_hashes: bool = {
                let mut stmt = source
                    .prepare(
                        "SELECT EXISTS(
                             SELECT 1 FROM sqlite_master
                             WHERE type = 'table' AND lower(name) = 'filehashes'
                         )",
                    )
                    .await?;
                stmt.query_row(()).await?.get(0)?
            };
            if has_file_hashes {
                let mut last_file_id = 0_u64;
                let mut first_hash_pass = true;
                loop {
                    let batch_started = Instant::now();
                    let mut stmt = source
                        .prepare(
                            "SELECT h.file_id, h.algorithm, h.digest
                             FROM FileHashes h
                             WHERE h.file_id > ?1
                             ORDER BY h.file_id
                             LIMIT ?2",
                        )
                        .await?;
                    let mut rows = stmt
                        .query([
                            keyset_bound(first_hash_pass, last_file_id),
                            SQL_CHUNK_SIZE as i64,
                        ])
                        .await?;
                    let mut batch: Vec<(u64, String, String)> = Vec::new();
                    while let Some(row) = rows.next().await? {
                        batch.push((row.get(0)?, row.get(1)?, row.get(2)?));
                    }
                    drop(stmt);
                    let Some((last_id, _, _)) = batch.last() else {
                        break;
                    };
                    first_hash_pass = false;
                    let tuples: Vec<_> = batch
                        .iter()
                        .filter_map(|(file_id, algorithm, digest)| {
                            slurp_files
                                .get(file_id)
                                .map(|target_id| (*target_id, algorithm.as_str(), digest.as_str()))
                        })
                        .collect();
                    log::info!("Slurping {} hashes into the db.", tuples.len());
                    if !tuples.is_empty() {
                        let target_started = Instant::now();
                        conn.execute("BEGIN IMMEDIATE", ()).await?;
                        self.file_hashes_add_bulk(&conn, &tuples).await?;
                        conn.execute("COMMIT", ()).await?;
                        log::info!(
                            "Slurp hashes batch target complete: {} rows in {:?} (total {:?})",
                            tuples.len(),
                            target_started.elapsed(),
                            batch_started.elapsed()
                        );
                    }
                    last_file_id = *last_id;
                }
            }
        }
        log::info!(
            "Slurp stage hashes complete in {:?}",
            slurp_started.elapsed()
        );

        // Relationships. The source ships them either as per-namespace
        // `Relationship_<id>` partitions or as a single legacy `Relationship`
        // table holding every namespace. The legacy table is huge and shared,
        // so filtering it per namespace forces nearly a full table scan for
        // every sparse namespace; the production run measured ~9.5 h of source
        // reads across the stage. It is therefore streamed once in PRIMARY KEY
        // order and routed to the correct target partitions in-process. Both
        // shapes drop the per-partition file_id index while rows land, recount
        // tag counts with one aggregate pass per namespace, then rebuild the
        // index once at the end.
        {
            let source_table_exists = async |name: &str| -> Result<bool> {
                let mut stmt = source
                    .prepare(
                        "SELECT 1 FROM sqlite_master
                         WHERE type = 'table' AND lower(name) = lower(?1)",
                    )
                    .await?;
                let mut rows = stmt.query((name,)).await?;
                Ok(rows.next().await?.is_some())
            };
            let has_legacy_relationship = source_table_exists("Relationship").await?;

            // The Tags count column drives idx_tags_count_covering (ordered by
            // count DESC). Recount UPDATEs churn this index so aggressively that
            // turso's commit-log materialization pins memory with no wal
            // progress at prod scale. Drop the index around every recount and
            // restore it once after all recalcs complete.
            let mut covering_dropped = false;

            // Recount markers, persisted across runs. The recount for a
            // namespace is skipped when its stream inserted nothing, but an
            // interrupted run can leave freshly inserted relationship rows
            // without their recount. Mark namespaces whose rows actually
            // changed so the next run still recounts them even when the
            // destination is already warm (added == 0); the table is dropped
            // once an entire slurp completes with every marked namespace
            // recounted.
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _slurp_pending_recount (
                     namespace INTEGER PRIMARY KEY);",
                (),
            )
            .await?;
            let mut pending_recount: HashSet<u64> = HashSet::new();
            {
                let mut stmt = conn
                    .prepare("SELECT namespace FROM _slurp_pending_recount")
                    .await?;
                let mut rows = stmt.query(()).await?;
                while let Some(row) = rows.next().await? {
                    pending_recount.insert(row.get(0)?);
                }
            }

            // Source ids that keep their own partition table; the remaining
            // namespaces are covered by the legacy table when it exists.
            let mut partition_source_ids = HashSet::new();
            {
                let mut stmt = source
                    .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
                    .await?;
                let mut rows = stmt.query(()).await?;
                while let Some(row) = rows.next().await? {
                    let name: String = row.get(0)?;
                    // Turso canonicalizes identifiers to lowercase on disk, so
                    // match the Relationship_<id> partition prefix case-insensitively.
                    if let Some(id) = name
                        .to_ascii_lowercase()
                        .strip_prefix("relationship_")
                        .and_then(|suffix| suffix.parse::<u64>().ok())
                    {
                        partition_source_ids.insert(id);
                    }
                }
            }
            let mut legacy_namespaces: Vec<(u64, u64)> = Vec::new();
            for (source_namespace, namespace) in &ns_by_source_id {
                if partition_source_ids.contains(source_namespace) || !has_legacy_relationship {
                    continue;
                }
                let target_namespace = self
                    .namespace_get_name_cache(&namespace.name)
                    .await
                    .ok_or_else(|| {
                        turso::Error::ConversionFailure(format!(
                            "slurp namespace not cached: {}",
                            namespace.name
                        ))
                    })?;
                legacy_namespaces.push((*source_namespace, target_namespace));
            }

            // Per-partition sources: keyset-scan each `Relationship_<id>` table
            // independently and copy straight into the matching target.
            // The partition's file_id index is dropped only when the
            // destination is cold (tags_added reflects that); a warm re-slurp
            // that manages to insert nothing must not tear down and rebuild
            // an index for zero benefit (multi-minute CREATE on big
            // partitions, and it vanishes from the planner meanwhile).
            let cold_import = tags_added > 0;
            // Set once if any recount fires these: Tags.count churns
            // idx_tags_count_covering (ordered by count) so it must be
            // down while recounts run and restored once all are done.
            for (source_namespace, namespace) in &ns_by_source_id {
                if !partition_source_ids.contains(source_namespace) {
                    continue;
                }
                let target_namespace = self
                    .namespace_get_name_cache(&namespace.name)
                    .await
                    .ok_or_else(|| {
                        turso::Error::ConversionFailure(format!(
                            "slurp namespace not cached: {}",
                            namespace.name
                        ))
                    })?;

                // The partition keeps an extra index on file_id alongside its
                // PRIMARY KEY (tag_id, file_id). Rebuilding it once after every
                // row has landed is far cheaper than maintaining it on each of
                // the thousands of INSERT chunks inside the copy loop.
                if cold_import {
                    conn.execute(
                        format!(
                            "DROP INDEX IF EXISTS idx_Relationship_{target_namespace}_tag_file"
                        ),
                        (),
                    )
                    .await?;
                }

                let partition = format!("Relationship_{source_namespace}");
                let mut last_file_id = 0_u64;
                let mut last_tag_id = 0_u64;
                let mut first_rel_pass = true;
                let mut added_here = 0_usize;
                loop {
                    let batch_started = Instant::now();
                    let mut stmt = source
                        .prepare(&format!(
                            "SELECT r.file_id, r.tag_id
                             FROM {} r
                             WHERE r.file_id > ?1 OR (r.file_id = ?1 AND r.tag_id > ?2)
                             ORDER BY r.file_id, r.tag_id
                             LIMIT ?3",
                            partition
                        ))
                        .await?;
                    let mut rows = stmt
                        .query([
                            keyset_bound(first_rel_pass, last_file_id),
                            keyset_bound(first_rel_pass, last_tag_id),
                            SLURP_RELATIONSHIP_BATCH as i64,
                        ])
                        .await?;
                    let mut batch: Vec<(u64, u64)> = Vec::new();
                    while let Some(row) = rows.next().await? {
                        batch.push((row.get(0)?, row.get(1)?));
                    }
                    drop(stmt);
                    let Some((source_file_id, source_tag_id)) = batch.last() else {
                        break;
                    };
                    first_rel_pass = false;

                    let mut relationships = batch
                        .iter()
                        .filter_map(|(file_id, tag_id)| {
                            let target_file = slurp_files.get(file_id)?;
                            let target_tag = slurp_tags.get(tag_id)?;
                            Some((*target_file, *target_tag))
                        })
                        .collect::<Vec<_>>();
                    // The partition's PRIMARY KEY is (tag_id, file_id); the
                    // keyset scan returns rows in file order, so insert them
                    // sorted by the key to keep the destination pages hot.
                    relationships.sort_unstable_by_key(|&(file_id, tag_id)| (tag_id, file_id));
                    log::info!(
                        "Slurping {} relationships into the db.",
                        relationships.len()
                    );
                    let target_started = Instant::now();
                    conn.execute("BEGIN IMMEDIATE", ()).await?;
                    let added = self
                        .slurp_relationships_bulk_add(&conn, target_namespace, &relationships)
                        .await?;
                    added_here += added;
                    if added > 0 {
                        conn.execute(
                            "INSERT OR IGNORE INTO _slurp_pending_recount (namespace) VALUES (?1)",
                            (target_namespace as i64,),
                        )
                        .await?;
                    }
                    conn.execute("COMMIT", ()).await?;

                    last_file_id = *source_file_id;
                    last_tag_id = *source_tag_id;
                    log::info!(
                        "Slurp relationships batch complete: {} rows, target {:?}, total {:?}",
                        relationships.len(),
                        target_started.elapsed(),
                        batch_started.elapsed()
                    );
                }

                if cold_import || added_here > 0 || pending_recount.contains(&target_namespace) {
                    // Tags.count churns idx_tags_count_covering (ordered by
                    // count) on every recount UPDATE: each reordering mutates
                    // the count b-tree and turso's commit-log materialization
                    // pins memory with no wal progress until it is rebuilt.
                    // Drop it once around every recount and restore after all
                    // recalcs land, mirroring the cold path.
                    if !covering_dropped {
                        conn.execute("DROP INDEX IF EXISTS idx_tags_count_covering", ())
                            .await?;
                        covering_dropped = true;
                    }
                    slurp_recount_namespace(&conn, target_namespace).await?;
                    log::info!(
                        "Slurp count recalculation for namespace {} complete in {:?}",
                        target_namespace,
                        slurp_started.elapsed()
                    );
                }

                // A cold import dropped the index before the copy; warm deltas insert
                // right through it. `CREATE ... IF NOT EXISTS` is a no-op when
                // the index is already present, so it is safe to run on every
                // path: it rebuilds the index exactly when it does not exist
                // (cold import, or a crashed previous run left it dropped).
                conn.execute(
                    format!(
                        "CREATE INDEX IF NOT EXISTS idx_Relationship_{target_namespace}_tag_file
                         ON Relationship_{target_namespace} (file_id)"
                    ),
                    (),
                )
                .await?;
            }

            // Legacy sources: one ordered scan over the whole shared table,
            // routing each row to its target partition by namespace as it is
            // read. The scan returns the namespace along with the row, so the
            // single pass replaces the repeating per-namespace rescans.
            if !legacy_namespaces.is_empty() {
                let target_by_source_namespace: HashMap<u64, u64> =
                    legacy_namespaces.iter().copied().collect();
                // Only cold imports tear the file_id indexes down; see the
                // partition path for the rationale.
                if cold_import {
                    for &(_, target_namespace) in &legacy_namespaces {
                        conn.execute(
                            format!(
                                "DROP INDEX IF EXISTS idx_Relationship_{target_namespace}_tag_file"
                            ),
                            (),
                        )
                        .await?;
                    }
                }

                let mut last_file_id = 0_u64;
                let mut last_tag_id = 0_u64;
                let mut first_legacy_rel_pass = true;
                let mut added_by_namespace: HashMap<u64, usize> = HashMap::new();
                loop {
                    let batch_started = Instant::now();
                    let mut stmt = source
                        .prepare(
                            "SELECT r.file_id, r.tag_id, t.namespace
                             FROM Relationship r
                             JOIN Tags t ON t.id = r.tag_id
                             WHERE (r.file_id > ?1 OR (r.file_id = ?1 AND r.tag_id > ?2))
                             ORDER BY r.file_id, r.tag_id
                             LIMIT ?3",
                        )
                        .await?;
                    let mut rows = stmt
                        .query([
                            keyset_bound(first_legacy_rel_pass, last_file_id),
                            keyset_bound(first_legacy_rel_pass, last_tag_id),
                            SLURP_RELATIONSHIP_BATCH as i64,
                        ])
                        .await?;
                    let mut batch: Vec<(u64, u64, u64)> = Vec::new();
                    while let Some(row) = rows.next().await? {
                        batch.push((row.get(0)?, row.get(1)?, row.get(2)?));
                    }
                    drop(stmt);
                    let Some(&(next_file_id, next_tag_id, _)) = batch.last() else {
                        break;
                    };
                    first_legacy_rel_pass = false;

                    let mut namespace_groups: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
                    for &(file_id, tag_id, source_namespace) in &batch {
                        let Some(&target_namespace) =
                            target_by_source_namespace.get(&source_namespace)
                        else {
                            continue;
                        };
                        let (Some(&target_file), Some(&target_tag)) =
                            (slurp_files.get(&file_id), slurp_tags.get(&tag_id))
                        else {
                            continue;
                        };
                        namespace_groups
                            .entry(target_namespace)
                            .or_default()
                            .push((target_file, target_tag));
                    }
                    for (target_namespace, relationships) in namespace_groups.iter_mut() {
                        // Align with the partition PRIMARY KEY (tag_id, file_id)
                        // so the destination pages stay hot despite the source
                        // scan arriving in file order.
                        relationships.sort_unstable_by_key(|&(file_id, tag_id)| (tag_id, file_id));
                        log::info!(
                            "Slurping {} relationships into the db.",
                            relationships.len()
                        );
                        let target_started = Instant::now();
                        let tn = conn.transaction().await?;
                        let added = self
                            .slurp_relationships_bulk_add(
                                &tn,
                                *target_namespace,
                                relationships.as_slice(),
                            )
                            .await?;
                        *added_by_namespace.entry(*target_namespace).or_default() += added;
                        if added > 0 {
                            tn.execute(
                                "INSERT OR IGNORE INTO _slurp_pending_recount (namespace) VALUES (?1)",
                                (*target_namespace as i64,),
                            )
                            .await?;
                        }
                        tn.commit().await?;
                        log::info!(
                            "Slurp relationships batch complete: {} rows, target {:?}",
                            relationships.len(),
                            target_started.elapsed()
                        );
                    }

                    last_file_id = next_file_id;
                    last_tag_id = next_tag_id;
                    log::info!(
                        "Slurp relationships source read: {} rows, total {:?}",
                        batch.len(),
                        batch_started.elapsed()
                    );
                }

                for &(_, target_namespace) in &legacy_namespaces {
                    if cold_import
                        || added_by_namespace
                            .get(&target_namespace)
                            .copied()
                            .unwrap_or(0)
                            > 0
                        || pending_recount.contains(&target_namespace)
                    {
                        if !covering_dropped {
                            conn.execute("DROP INDEX IF EXISTS idx_tags_count_covering", ())
                                .await?;
                            covering_dropped = true;
                        }
                        slurp_recount_namespace(&conn, target_namespace).await?;
                        log::info!(
                            "Slurp count recalculation for namespace {} complete in {:?}",
                            target_namespace,
                            slurp_started.elapsed()
                        );
                    }
                    // Only cold imports rebuilt the dropped partition indexes; on warm
                    // runs the index either exists (`IF NOT EXISTS` no-ops or
                    // stays) or is missing from a crashed earlier run and is
                    // rebuilt right here.
                    conn.execute(
                        format!(
                            "CREATE INDEX IF NOT EXISTS idx_Relationship_{target_namespace}_tag_file
                             ON Relationship_{target_namespace} (file_id)"
                        ),
                        (),
                    )
                    .await?;
                }
            }

            // Every namespace that gained rows (or carries an interrupted-run
            // marker) was recounted or explicitly skipped only for a
            // fully-warm run; either way the marker table is spent now. The
            // covering index must be restored whether the source used
            // per-namespace partitions or the legacy shared Relationship table:
            // recounts drop it (count DESC churns the b-tree) and a
            // partition-only source never reaches the legacy branch to restore
            // it.
            if covering_dropped {
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tags_count_covering
                     ON Tags(count DESC, name, namespace)",
                    (),
                )
                .await?;
                log::info!("Slurp restored idx_tags_count_covering after recount");
            }
            conn.execute("DROP TABLE IF EXISTS _slurp_pending_recount", ())
                .await?;
        }

        // Parents.
        {
            let mut parents = source
                .prepare("SELECT tag_id, relate_tag_id, limit_to FROM Parents")
                .await?;
            let mut rows = parents.query(()).await?;

            let mut parent_set = HashSet::new();
            while let Some(row) = rows.next().await? {
                let (tag_id, relate_tag_id, limit_to): (u64, u64, Option<u64>) =
                    (row.get(0)?, row.get(1)?, row.get(2)?);
                if let (Some(&tag_id), Some(&relate_tag_id)) =
                    (slurp_tags.get(&tag_id), slurp_tags.get(&relate_tag_id))
                {
                    parent_set.insert(TagParents {
                        tag_id,
                        relate_tag_id,
                        limit_to: limit_to.and_then(|id| slurp_tags.get(&id).copied()),
                    });
                }
            }
            log::info!("Slurping {} parents into the db.", parent_set.len());

            // --- CRASH RECOVERY CHECK ---
            // If a previous run crashed between the rename and the drop,
            // Parents will be missing/incomplete, but Parents_old will hold the good data.
            {
                let mut check = conn
                    .prepare(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                    )
                    .await?;
                let has_parents: i64 = check.query_row(("Parents",)).await?.get(0)?;
                let has_old: i64 = check.query_row(("Parents_old",)).await?.get(0)?;

                if has_parents == 0 && has_old != 0 {
                    log::warn!(
                        "Recovery detected: restoring Parents_old to Parents after a previous crash."
                    );
                    conn.execute("ALTER TABLE Parents_old RENAME TO Parents", ())
                        .await?;
                    // The restored table may be missing the named indexes if the
                    // interrupted run had already dropped them from Parents_old.
                    conn.execute_batch(
                        "CREATE INDEX IF NOT EXISTS idx_parents_lim ON Parents (limit_to);
                         CREATE INDEX IF NOT EXISTS idx_parents_rel ON Parents (relate_tag_id);
                         CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_parents_null_safe
                             ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));",
                    )
                    .await?;
                } else {
                    // Safe cleanup of any truly dead leftovers from old successful runs
                    conn.execute("DROP TABLE IF EXISTS Parents_old", ()).await?;
                }
            }

            // 1. Drop foreign keys and shift the current table aside safely.
            // The named indexes follow the renamed table, so their names would
            // collide with the fresh Parents table that table_create_parents
            // creates below. Drop them from Parents_old first so the new table
            // gets its full index set, including the null-safe unique index
            // that makes the bulk INSERT OR IGNORE dedupe warm re-slurps.
            conn.execute_batch("PRAGMA foreign_keys = OFF;").await?;
            conn.execute_batch(
                "ALTER TABLE Parents RENAME TO Parents_old;
                 DROP INDEX IF EXISTS idx_parents_lim;
                 DROP INDEX IF EXISTS idx_parents_rel;
                 DROP INDEX IF EXISTS idx_unique_parents_null_safe;",
            )
            .await?;

            self.table_create_parents(&conn).await?;

            // 2. Copy existing data from Parents_old into the freshly
            // constrained Parents table, deduplicating with the same null-safe
            // key the unique index uses.
            log::info!("Copying and deduplicating existing parents data");
            conn.execute(
                "INSERT INTO Parents (tag_id, relate_tag_id, limit_to)
                 SELECT tag_id, relate_tag_id, limit_to
                 FROM Parents_old
                 GROUP BY tag_id, relate_tag_id, IFNULL(limit_to, -1)",
                (),
            )
            .await?;

            // 3. Insert new parents in chunks; the null-safe unique index on
            // the new Parents table makes INSERT OR IGNORE skip rows already
            // copied from Parents_old above.
            let parent_vec: Vec<_> = parent_set.into_iter().collect();
            for parent in parent_vec.chunks(SQL_CHUNK_SIZE * 8) {
                let tx = conn.transaction().await?;
                self.parents_bulk_add(&tx, parent).await?;
                tx.commit().await?;
            }

            // 4. Cleanup the old table and restore foreign keys. The fresh
            // Parents table was created fully indexed by table_create_parents,
            // so no index rebuild is needed here.
            log::info!("Dropping Parents_old");
            conn.execute_batch("DROP TABLE Parents_old; PRAGMA foreign_keys = ON;")
                .await?;
        }

        // Jobs. Source ids are intentionally not preserved: the target Jobs
        // table owns its ids, while the existing deduplication key prevents
        // duplicate imports from creating duplicate work. The copy tolerates
        // both the IntScrape legacy schema (`recreation`/`user_data`) and
        // Rust-Hydrus-style schemas (`Manager`/`UserData`/optional `priority`).
        let has_jobs: bool = {
            let mut stmt = source
                .prepare(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'table' AND lower(name) = 'jobs'
                     )",
                )
                .await?;
            stmt.query_row(()).await?.get(0)?
        };
        if has_jobs {
            self.slurp_jobs(&conn, source).await?;
        }

        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_tags_fts ON Tags USING fts
                 (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
             OPTIMIZE INDEX idx_tags_fts;",
        )
        .await?;

        Ok((namespace_count, tag_count, file_count))
    }

    /// Imports tags without the plugin-facing action path. Slurp tags are
    /// always normal tags, so regex registration and parent processing are
    /// unnecessary here; the source-to-target mapping is all the caller needs.
    ///
    /// Returns the mapping and how many rows were actually new (i.e. inserted
    /// into `insert_target`). The caller uses the count to decide whether the
    /// swap-back is needed at all: a warm re-slurp that added nothing must
    /// not re-copy the whole table and rebuild its indexes.
    ///
    /// `insert_target` is either the canonical `Tags` table (cold import:
    /// indexed, so new rows maintain the unique key inline) or the
    /// constraint-free `Tags_slurp` staging table (warm import; the caller
    /// swaps it under the canonical name afterwards). The value is an internal
    /// constant, never user input.
    ///
    /// New rows are assigned explicit ids via `next_insert_id`, which the
    /// caller seeds with the canonical table's max id. Auto-assignment would
    /// restart the fresh staging table at id 1 and collide with the existing
    /// rows the swap-back step later copies across at their original ids
    /// (UNIQUE constraint failed: tags_slurp.id).
    async fn slurp_tags_bulk_add(
        &self,
        conn: &Connection,
        batch: &[(u64, String, String, Option<String>)],
        lookup_existing: bool,
        insert_target: &str,
        next_insert_id: &mut i64,
    ) -> Result<(HashMap<Tag, i64>, usize)> {
        let mut namespace_ids = HashMap::new();
        for (_, _, namespace, _) in batch {
            if !namespace_ids.contains_key(namespace) {
                let id = self
                    .namespace_get_name_cache(namespace)
                    .await
                    .ok_or_else(|| {
                        turso::Error::ConversionFailure(format!(
                            "slurp namespace not cached: {namespace}"
                        ))
                    })?;
                namespace_ids.insert(namespace.clone(), id);
            }
        }

        // Rows the destination already owns resolve to their existing target
        // id through the Tags table (which keeps its unique index and is only
        // read here). Everything else is genuinely new and lands in
        // `insert_target` at bulk speed. Source tags are unique by
        // (name, namespace), so a tag is either existing or new, never both.
        let mut existing_ids: HashMap<(String, u64), i64> = HashMap::new();
        if lookup_existing {
            // Populate the pre-created temp table for this batch and let the
            // planner use an index-nested-loop JOIN on the unique
            // (name, namespace) constraint (created/cleaned up by the caller
            // outside any transaction).
            conn.execute("DELETE FROM _slurp_tags_tmp", ()).await?;
            for chunk in batch.chunks(SLURP_TAG_BATCH as usize / 6) {
                let mut holders = Vec::with_capacity(chunk.len());
                let mut params = Vec::with_capacity(chunk.len() * 2);
                for (_, name, namespace, _) in chunk {
                    holders.push("(?, ?)");
                    params.push(Value::from(name.as_str()));
                    params.push(Value::from(namespace_ids[namespace] as i64));
                }
                conn.execute(
                    &format!(
                        "INSERT INTO _slurp_tags_tmp (name, namespace) VALUES {}",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await?;
            }
            let mut rows = conn
                .query(
                    "SELECT Tags.id, Tags.name, Tags.namespace \
                     FROM Tags \
                     JOIN _slurp_tags_tmp \
                       ON Tags.name = _slurp_tags_tmp.name \
                       AND Tags.namespace = _slurp_tags_tmp.namespace",
                    (),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                existing_ids.insert((row.get(1)?, row.get::<i64>(2)? as u64), row.get(0)?);
            }
        }

        let new_rows: Vec<_> = batch
            .iter()
            .filter(|(_, name, namespace, _)| {
                !existing_ids.contains_key(&(name.clone(), namespace_ids[namespace]))
            })
            .collect();
        let mut holders = Vec::with_capacity(new_rows.len());
        let mut params = Vec::with_capacity(new_rows.len() * 3);
        for (_, name, namespace, _) in &new_rows {
            *next_insert_id += 1;
            holders.push("(?, ?, ?)");
            params.push(Value::from(*next_insert_id));
            params.push(Value::from(name.as_str()));
            params.push(Value::from(namespace_ids[namespace] as i64));
        }
        let mut inserted_ids: HashMap<(String, u64), i64> = HashMap::new();
        if !new_rows.is_empty() {
            let mut rows = conn
                .query(
                    format!(
                        "INSERT INTO {insert_target} (id, name, namespace) VALUES {} \
                         RETURNING id, name, namespace",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                inserted_ids.insert((row.get(1)?, row.get::<i64>(2)? as u64), row.get(0)?);
            }
        }
        let new_count = new_rows.len();

        let mut mapping = HashMap::with_capacity(batch.len());
        for (_, name, namespace, description) in batch {
            let namespace_id = namespace_ids[namespace];
            let id = existing_ids
                .get(&(name.clone(), namespace_id))
                .copied()
                .or_else(|| inserted_ids.get(&(name.clone(), namespace_id)).copied());
            if let Some(id) = id {
                mapping.insert(
                    Tag {
                        name: name.clone(),
                        namespace: GenericNamespaceObj {
                            name: namespace.clone(),
                            description: description.clone(),
                        },
                    },
                    id,
                );
            }
        }
        Ok((mapping, new_count))
    }

    /// Inserts relationships into a known namespace partition. The normal
    /// relationship helper has to resolve namespaces and maintain live tag
    /// counts for interactive writes; slurp recalculates counts once per
    /// partition after all rows have been copied.
    ///
    /// Returns how many rows were actually inserted (OR IGNORE skips rows the
    /// destination already has). The caller sums these to know whether any
    /// relationship data changed, which decides whether recounts are needed.
    async fn slurp_relationships_bulk_add(
        &self,
        conn: &Connection,
        namespace_id: u64,
        relationships: &[(u64, u64)],
    ) -> Result<usize> {
        let mut added = 0_u64;
        for chunk in relationships.chunks(SLURP_RELATIONSHIP_BATCH) {
            let mut holders = Vec::with_capacity(chunk.len());
            let mut params = Vec::with_capacity(chunk.len() * 2);
            for &(file_id, tag_id) in chunk {
                holders.push("(?, ?)");
                params.push(Value::from(file_id as i64));
                params.push(Value::from(tag_id as i64));
            }
            added += conn
                .execute(
                    format!(
                        "INSERT OR IGNORE INTO Relationship_{namespace_id} (file_id, tag_id)
                         VALUES {}",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await?;
        }
        Ok(added as usize)
    }

    /// Copies `Jobs` rows into turso, mapping whichever schema the source
    /// uses. Required columns are `time`, `reptime`, `site`, `param`; when
    /// those are missing the step is skipped rather than aborting the import.
    /// `priority` defaults to 10 when absent or NULL; `recreation` is read
    /// from `recreation` (IntScrape) or the `Manager` JSON (Rust-Hydrus);
    /// `user_data` comes from `user_data` or `UserData`. Rows whose payloads
    /// cannot be parsed are skipped with a warning so one bad job never rolls
    /// back an otherwise complete slurp.
    async fn slurp_jobs(&self, conn: &Connection, source: &Connection) -> Result<()> {
        let mut columns = HashSet::new();
        {
            let mut stmt = source.prepare("PRAGMA table_info(\"Jobs\")").await?;
            let mut rows = stmt.query(()).await?;
            while let Some(row) = rows.next().await? {
                columns.insert(row.get::<String>(1)?);
            }
        }

        const REQUIRED_JOBS_COLUMNS: [&str; 4] = ["time", "reptime", "site", "param"];
        if !REQUIRED_JOBS_COLUMNS
            .iter()
            .all(|column| columns.contains(*column))
        {
            log::warn!("Slurp: source Jobs table is missing required columns; skipping jobs.");
            return Ok(());
        }

        // Column names come from a fixed allowlist below, so interpolating
        // them into the SQL is safe.
        let priority_expr = if columns.contains("priority") {
            "priority"
        } else {
            "NULL"
        };
        let recreation_expr = if columns.contains("recreation") {
            "recreation"
        } else if columns.contains("Manager") {
            "Manager"
        } else {
            "NULL"
        };
        let user_data_expr = if columns.contains("user_data") {
            "user_data"
        } else if columns.contains("UserData") {
            "UserData"
        } else {
            "NULL"
        };

        let select = format!(
            "SELECT time, reptime, {priority_expr}, {recreation_expr}, site, param, {user_data_expr}
             FROM Jobs ORDER BY id"
        );
        let mut stmt = source.prepare(&select).await?;
        let mut rows = stmt.query(()).await?;

        let mut copied = 0_u64;
        let mut batch: Vec<PluginJob> = Vec::with_capacity(SQL_CHUNK_SIZE);
        while let Some(row) = rows.next().await? {
            let time: u64 = row.get(0)?;
            let reptime: u64 = row.get(1)?;
            let priority: Option<u64> = row.get(2)?;
            let recreation_json: Option<String> = row.get(3)?;
            let site: String = row.get(4)?;
            let param: String = row.get(5)?;
            let user_data_json: Option<String> = row.get(6)?;

            let params = match serde_json::from_str::<Vec<ScraperParam>>(&param) {
                Ok(params) => params,
                Err(error) => {
                    log::warn!(
                        "Slurp: skipping job for site '{site}' with unparseable param ({error})."
                    );
                    continue;
                }
            };
            let user_data = match user_data_json {
                Some(json) if !json.is_empty() => match serde_json::from_str(&json) {
                    Ok(data) => data,
                    Err(error) => {
                        log::warn!(
                            "Slurp: skipping job for site '{site}' with unparseable user data ({error})."
                        );
                        continue;
                    }
                },
                _ => std::collections::BTreeMap::new(),
            };

            let job = PluginJob {
                time,
                reptime,
                priority: priority.unwrap_or(10),
                recreation: recreation_json
                    .as_deref()
                    .and_then(parse_slurp_job_recreation),
                site: site.clone(),
                param: params,
                user_data,
            };
            batch.push(job);
            if batch.len() >= SQL_CHUNK_SIZE {
                conn.execute("BEGIN IMMEDIATE", ()).await?;
                self.jobs_bulk_add_sql(conn, &batch).await?;
                conn.execute("COMMIT", ()).await?;
                copied += batch.len() as u64;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            conn.execute("BEGIN IMMEDIATE", ()).await?;
            self.jobs_bulk_add_sql(conn, &batch).await?;
            conn.execute("COMMIT", ()).await?;
            copied += batch.len() as u64;
        }
        log::info!("Slurping {copied} jobs into the db.");

        Ok(())
    }
}

/// Extracts the recreation config from either the IntScrape legacy `recreation`
/// column (the `Option<DbJobRecreation>` itself) or a Rust-Hydrus `Manager`
/// column (a `DbJobsManager` object wrapping a `recreation` field).
fn parse_slurp_job_recreation(json: &str) -> Option<DbJobRecreation> {
    if let Ok(recreation) = serde_json::from_str::<Option<DbJobRecreation>>(json) {
        return recreation;
    }
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    value
        .get("recreation")
        .and_then(|recreation| {
            serde_json::from_value::<Option<DbJobRecreation>>(recreation.clone()).ok()
        })
        .flatten()
}

/// Builds a single `UPDATE Tags SET count = CASE id WHEN ? THEN ? ... END
/// WHERE id IN (...)` that *sets* each tag's count to an absolute value in one
/// round trip. Slurp recomputes counts from an aggregate over a relationship
/// partition (after zeroing the namespace), so unlike `tag_count_update_sql`
/// in relationship.rs it replaces the count instead of incrementing it.
fn tag_count_set_sql(counts: &[(u64, u64)]) -> (String, Vec<Value>) {
    let mut clauses = Vec::with_capacity(counts.len());
    let mut params = Vec::with_capacity(counts.len() * 3);
    for &(tag_id, count) in counts {
        clauses.push("WHEN ? THEN ?".to_string());
        params.push(Value::from(tag_id as i64));
        params.push(Value::from(count as i64));
    }
    let placeholders = std::iter::repeat_n("?", counts.len())
        .collect::<Vec<_>>()
        .join(", ");
    for &(tag_id, _) in counts {
        params.push(Value::from(tag_id as i64));
    }
    (
        format!(
            "UPDATE Tags SET count = CASE id {} END WHERE id IN ({placeholders});",
            clauses.join(" ")
        ),
        params,
    )
}

/// Recomputes a namespace's `Tags.count` from its freshly loaded partition.
/// Zeroing first, then one `GROUP BY` aggregate over tags (instead of a
/// correlated `COUNT(*)` per tag) prevents stale counts from drifting and runs
/// in a single concurrent transaction. The partition's PRIMARY KEY
/// (tag_id, file_id) covers the GROUP BY, so it costs one index scan rather
/// than one probe per Tags row. See `tag_count_set_sql`.
async fn slurp_recount_namespace(conn: &Connection, namespace_id: u64) -> Result<()> {
    // Build the authoritative per-tag counts with one GROUP BY over the freshly
    // landed partition, then emit every tag in the namespace (zeros included)
    // as bounded per-chunk updates. A single `UPDATE Tags SET count = 0
    // WHERE namespace = ?` would sweep the whole 15M-row Tags tree into a new
    // MVCC image even when only one namespace changes - some fragment shapes
    // never finish that rematerialization (RSS pins at ~10GB with zero wal).
    let mut counts: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    let mut rows = conn
        .query(
            format!(
                "SELECT tag_id, COUNT(*) AS count FROM Relationship_{namespace_id}
                 GROUP BY tag_id"
            ),
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        counts.insert(row.get(0)?, row.get(1)?);
    }
    let mut tag_rows = conn
        .query(
            "SELECT id FROM Tags WHERE namespace = ?1",
            (namespace_id as i64,),
        )
        .await?;
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    while let Some(row) = tag_rows.next().await? {
        let id: u64 = row.get(0)?;
        pairs.push((id, counts.get(&id).copied().unwrap_or(0)));
    }

    conn.execute("BEGIN IMMEDIATE", ()).await?;
    for chunk in pairs.chunks(SQL_CHUNK_SIZE) {
        let (count_sql, count_params) = tag_count_set_sql(chunk);
        conn.execute(count_sql, params_from_iter(count_params))
            .await?;
    }
    conn.execute("COMMIT", ()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    async fn new_target() -> Arc<TursoDatabase> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("target.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    }

    /// Point a source sqlite file at `script` and return a connection to it.
    /// The tempdir must outlive the connection and database, so all are returned.
    async fn new_source(script: &str) -> (turso::Database, turso::Connection, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        let db = turso::Builder::new_local(&source_path.to_string_lossy())
            .experimental_without_rowid(true)
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch(script).await.unwrap();
        (db, conn, temp_dir)
    }

    /// Create a standalone source database on disk and run `script` against it.
    /// Returns only the path; dropping the local handles persists the file so a
    /// later `db_slurp` can reopen it read-only.
    async fn write_source(source_path: &std::path::Path, script: &str) {
        let db = turso::Builder::new_local(&source_path.to_string_lossy())
            .experimental_without_rowid(true)
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch(script).await.unwrap();
        // Fold the WAL into the main file so a read-only reopen sees the schema
        // (matching a plain SQLite source file handed to db_slurp).
        conn.pragma_update("journal_mode", "'delete'")
            .await
            .unwrap();
        drop(conn);
        drop(db);
    }

    async fn target_jobs(db: &TursoDatabase) -> Vec<(String, u64, Option<String>, Option<String>)> {
        let conn = db.connect().unwrap();
        let mut rows = conn
            .query(
                "SELECT site, priority, recreation, user_data FROM Jobs ORDER BY id;",
                (),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            out.push((
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get::<Option<String>>(3).unwrap(),
            ));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_jobs_copies_intscrape_legacy_schema() {
        let db = new_target().await;
        let (_source_db, source, _keep) = new_source(
            "CREATE TABLE Jobs (
                 id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
                 reptime INTEGER NOT NULL, priority INTEGER NOT NULL,
                 recreation TEXT NOT NULL, site TEXT NOT NULL,
                 param TEXT NOT NULL, user_data TEXT NOT NULL);
             INSERT INTO Jobs VALUES
                 (1, 100, 60, 5, '{\"OnTagId\":[12,null]}', 'e621', '[]', '{\"k\":\"v\"}'),
                 (2, 200, 0, 10, 'null', 'gelbooru', '[{\"Normal\":\"cute\"}]', '{}');",
        )
        .await;

        let conn = db.connect().unwrap();
        db.slurp_jobs(&conn, &source).await.unwrap();

        let jobs = target_jobs(&db).await;
        assert_eq!(jobs.len(), 2, "both legacy jobs must be copied");
        let (site, priority, recreation, user_data) = &jobs[0];
        assert_eq!(site, "e621");
        assert_eq!(*priority, 5);
        assert!(
            recreation.as_deref().unwrap().contains("OnTagId"),
            "recreation from recreation column"
        );
        assert_eq!(user_data.as_deref().unwrap(), "{\"k\":\"v\"}");
        assert_eq!(jobs[1].1, 10);
        assert_eq!(
            jobs[1].2,
            Some("null".into()),
            "empty recreation stays null"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_jobs_copies_rusthydrus_schema() {
        let db = new_target().await;
        let (_source_db, source, _keep) = new_source(
            "CREATE TABLE Jobs (
                 id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
                 reptime INTEGER NOT NULL, priority INTEGER,
                 Manager TEXT NOT NULL, site TEXT NOT NULL,
                 param TEXT NOT NULL, SystemData TEXT NOT NULL,
                 UserData TEXT NOT NULL);
             INSERT INTO Jobs VALUES
                 (1, 300, 60, 7, '{\"jobtype\":\"Params\",\"recreation\":{\"OnTag\":[\"dog\",3,null]}}',
                  'r34', '[{\"Normal\":\"kino\"}]', '{\"sys\":\"x\"}', '{\"user\":\"y\"}'),
                 (2, 400, 0, NULL, '{\"jobtype\":\"Params\",\"recreation\":null}',
                  'saucenao', '[]', '{}', '{}');",
        )
        .await;

        let conn = db.connect().unwrap();
        db.slurp_jobs(&conn, &source).await.unwrap();

        let jobs = target_jobs(&db).await;
        assert_eq!(jobs.len(), 2, "both rust-hydrus jobs must be copied");
        let (site, priority, recreation, user_data) = &jobs[0];
        assert_eq!(site, "r34");
        assert_eq!(*priority, 7);
        assert!(
            recreation.as_deref().unwrap().contains("OnTag"),
            "recreation must be read out of the Manager JSON"
        );
        assert_eq!(
            user_data.as_deref().unwrap(),
            "{\"user\":\"y\"}",
            "user data from UserData"
        );
        assert_eq!(jobs[1].0, "saucenao");
        assert_eq!(jobs[1].1, 10, "NULL priority defaults to 10");
        assert_eq!(jobs[1].2, Some("null".into()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_recomputes_counts_and_rebuilds_bulk_import_indexes() {
        let db = new_target().await;

        // Warm destination: the namespace, tags and a NULL-limit parent all
        // already exist here. The source duplicates that parent, so after the
        // slurp it must be deduplicated rather than doubled while the null-safe
        // unique index is dropped.
        {
            let conn = db.connect().unwrap();
            conn.execute(
                "INSERT INTO Namespace (id, name, description) VALUES (1, 'species', 'pre');",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO Tags (id, name, namespace) VALUES (1, 'mammal', 1), (2, 'canine', 1);",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
                (),
            )
            .await
            .unwrap();
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        write_source(
            &source_path,
            "CREATE TABLE Namespace (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
             INSERT INTO Namespace (name, description) VALUES ('species', 'test');

             CREATE TABLE Tags (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                 count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
             INSERT INTO Tags (name, namespace) VALUES ('mammal', 1), ('canine', 1);

             CREATE TABLE FileStorageLocations (
                 id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
             INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

             CREATE TABLE File (
                 id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                 storage_id INTEGER, size_bytes INTEGER);
             INSERT INTO File (hash, extension, storage_id, size_bytes)
                 VALUES ('slurp-hash', 'jpg', 1, 42);

             CREATE TABLE Relationship_1 (
                 file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                 PRIMARY KEY (tag_id, file_id));
             INSERT INTO Relationship_1 (file_id, tag_id) VALUES (1, 1), (1, 2);

             CREATE TABLE Parents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                 relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
             INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
        )
        .await;

        let counts = db.db_slurp(&source_path).await.unwrap();
        assert_eq!(counts, (1, 2, 1));

        let conn = db.connect().unwrap();

        // Counts come from the aggregate upsert over the partition, not a
        // correlated per-tag probe.
        let mut rows = conn
            .query("SELECT id, count FROM Tags ORDER BY id;", ())
            .await
            .unwrap();
        let mut tag_counts = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            tag_counts.push((row.get::<u64>(0).unwrap(), row.get::<u64>(1).unwrap()));
        }
        assert!(
            tag_counts.contains(&(1, 1)),
            "mammal count must be recomputed from the partition: {tag_counts:?}"
        );
        assert!(
            tag_counts.contains(&(2, 1)),
            "canine count must be recomputed from the partition: {tag_counts:?}"
        );

        // The pre-existing NULL-limit parent and the slurped one dedupe back to
        // a single row, leaving the null-safe unique index rebuildable.
        let mut rows = conn
            .query("SELECT tag_id, relate_tag_id, limit_to FROM Parents;", ())
            .await
            .unwrap();
        let mut parents = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            parents.push((
                row.get::<u64>(0).unwrap(),
                row.get::<u64>(1).unwrap(),
                row.get::<Option<u64>>(2).unwrap(),
            ));
        }
        assert_eq!(parents, vec![(1, 2, None)]);

        // Every index dropped for the bulk load must be back in place. Turso
        // canonicalizes identifiers to lowercase on disk, so the partition
        // index is queried in its lowercase form.
        let mut rows = conn
            .query(
                "SELECT LOWER(name) FROM sqlite_schema
                 WHERE type = 'index' AND LOWER(name) IN (?1, ?2, ?3, ?4);",
                (
                    "idx_relationship_1_tag_file",
                    "idx_parents_lim",
                    "idx_parents_rel",
                    "idx_unique_parents_null_safe",
                ),
            )
            .await
            .unwrap();
        let mut present = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            present.push(row.get::<String>(0).unwrap());
        }
        assert_eq!(
            present.len(),
            4,
            "all bulk-import indexes recreated: {present:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_streams_legacy_relationship_table_once() {
        let db = new_target().await;

        // A single shared `Relationship` table holding every namespace, like
        // the production source. Rows must be read once and routed by
        // namespace; references to tags/files missing from the source must be
        // dropped rather than copied, and each target partition still gets its
        // count recompute and file_id index rebuild afterwards.
        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        write_source(
            &source_path,
            "CREATE TABLE Namespace (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
             INSERT INTO Namespace (name, description) VALUES
                 ('species', 'test'), ('artist', 'test');

             CREATE TABLE Tags (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                 count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
             INSERT INTO Tags (name, namespace) VALUES
                 ('mammal', 1), ('canine', 1), ('painter', 2);

             CREATE TABLE FileStorageLocations (
                 id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
             INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

             CREATE TABLE File (
                 id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                 storage_id INTEGER, size_bytes INTEGER);
             INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
                 ('slurp-hash', 'jpg', 1, 42), ('legacy-hash', 'png', 1, 43);

             CREATE TABLE Relationship (
                 file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                 PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
             INSERT INTO Relationship (file_id, tag_id) VALUES
                 (1, 1), (1, 2), (2, 3),
                 (2, 99),  -- tag id missing from Tags -> dropped
                 (99, 1);  -- file id missing from File -> dropped

             CREATE TABLE Parents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                 relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
             INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
        )
        .await;

        let counts = db.db_slurp(&source_path).await.unwrap();
        assert_eq!(counts, (2, 3, 2));

        let conn = db.connect().unwrap();
        let species_id = db.namespace_get_name_cache("species").await.unwrap();
        let artist_id = db.namespace_get_name_cache("artist").await.unwrap();

        // Counts recomputed per namespace from the aggregate pass.
        let mut rows = conn
            .query("SELECT name, count FROM Tags ORDER BY name;", ())
            .await
            .unwrap();
        let mut tag_counts = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            tag_counts.push((row.get::<String>(0).unwrap(), row.get::<u64>(1).unwrap()));
        }
        assert_eq!(
            tag_counts,
            vec![
                ("canine".to_string(), 1),
                ("mammal".to_string(), 1),
                ("painter".to_string(), 1),
            ]
        );

        // The scanned rows landed in the partition matching their namespace,
        // with the dangling references dropped before the bulk add.
        let mut rows = conn
            .query(
                format!(
                    "SELECT f.hash, t.name
                     FROM Relationship_{species_id} r
                     JOIN File f ON f.id = r.file_id
                     JOIN Tags t ON t.id = r.tag_id
                     ORDER BY t.name;"
                ),
                (),
            )
            .await
            .unwrap();
        let mut species_rows = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            species_rows.push((row.get::<String>(0).unwrap(), row.get::<String>(1).unwrap()));
        }
        assert_eq!(
            species_rows,
            vec![
                ("slurp-hash".to_string(), "canine".to_string()),
                ("slurp-hash".to_string(), "mammal".to_string()),
            ]
        );

        let mut rows = conn
            .query(
                format!(
                    "SELECT f.hash, t.name
                     FROM Relationship_{artist_id} r
                     JOIN File f ON f.id = r.file_id
                     JOIN Tags t ON t.id = r.tag_id;"
                ),
                (),
            )
            .await
            .unwrap();
        let mut artist_rows = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            artist_rows.push((row.get::<String>(0).unwrap(), row.get::<String>(1).unwrap()));
        }
        assert_eq!(
            artist_rows,
            vec![("legacy-hash".to_string(), "painter".to_string())]
        );

        // Every partition that received routed rows gets its index rebuilt.
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'index' AND LOWER(name) IN (?1, ?2);",
                (
                    format!("idx_relationship_{species_id}_tag_file"),
                    format!("idx_relationship_{artist_id}_tag_file"),
                ),
            )
            .await
            .unwrap();
        let rebuilt = rows.next().await.unwrap().unwrap();
        assert_eq!(rebuilt.get::<u64>(0).unwrap(), 2);

        // The parent survived its mapped copy.
        let mut rows = conn
            .query("SELECT COUNT(*) FROM Parents;", ())
            .await
            .unwrap();
        let parent_rows = rows.next().await.unwrap().unwrap();
        assert_eq!(parent_rows.get::<u64>(0).unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_tags_fresh_fast_path_then_upsert_on_rerun() {
        let db = new_target().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        write_source(
            &source_path,
            "CREATE TABLE Namespace (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
             INSERT INTO Namespace (name, description) VALUES
                 ('species', 'test'), ('artist', 'test');

             CREATE TABLE Tags (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                 count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
             INSERT INTO Tags (name, namespace) VALUES
                 ('mammal', 1), ('canine', 1), ('painter', 2);

             CREATE TABLE FileStorageLocations (
                 id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
             INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

             CREATE TABLE File (
                 id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                 storage_id INTEGER, size_bytes INTEGER);
             INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
                 ('slurp-hash', 'jpg', 1, 42), ('legacy-hash', 'png', 1, 43);

             CREATE TABLE Relationship (
                 file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                 PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
             INSERT INTO Relationship (file_id, tag_id) VALUES (1, 1), (1, 2), (2, 3);

             CREATE TABLE Parents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                 relate_tag_id INTEGER NOT NULL, limit_to INTEGER);",
        )
        .await;

        // Fresh destination: the tags stage swaps Tags for the
        // constraint-free copy (fast path) and restores the named unique
        // index afterwards.
        let counts = db.db_slurp(&source_path).await.unwrap();
        assert_eq!(counts, (2, 3, 2));

        let conn = db.connect().unwrap();
        let mut tag_rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
        assert_eq!(
            tag_rows
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<u64>(0)
                .unwrap(),
            3
        );
        let mut index_rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'index' AND name IN
                     ('idx_tags_name_namespace', 'idx_tags_count_covering');",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            index_rows
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<u64>(0)
                .unwrap(),
            2
        );

        // Re-running against the (now warm) destination must keep tag ids
        // stable: rows the destination already owns resolve to their existing
        // ids instead of being inserted again, so no duplicate tags and no
        // duplicate relationships at the (already imported) mapping.
        let counts = db.db_slurp(&source_path).await.unwrap();
        assert_eq!(counts, (2, 3, 2));
        let conn = db.connect().unwrap();
        let mut tag_rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
        assert_eq!(
            tag_rows
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<u64>(0)
                .unwrap(),
            3
        );

        // The swap left the canonical table and no temporary leftovers, and
        // the previous run's relationship rows deduplicate against this run's.
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'table' AND name IN ('Tags_slurp', 'Tags_old');",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            0
        );
        let species_id = db.namespace_get_name_cache("species").await.unwrap();
        let artist_id = db.namespace_get_name_cache("artist").await.unwrap();
        let mut species_rows = conn
            .query(
                format!("SELECT COUNT(*) FROM Relationship_{species_id};",),
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            species_rows
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<u64>(0)
                .unwrap(),
            2
        );
        let mut artist_rows = conn
            .query(
                format!("SELECT COUNT(*) FROM Relationship_{artist_id};"),
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            artist_rows
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<u64>(0)
                .unwrap(),
            1
        );
    }

    /// Cold import from a per-namespace-partition source (no legacy
    /// `Relationship` table): the tags stage must insert straight into the
    /// indexed `Tags` table, the covering index must be restored by the common
    /// post-recount path (the legacy-only branch would miss a partition-only
    /// source), and the router must reconstruct relation rows and recount.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_cold_partition_only_recounts_and_restores_covering() {
        let db = new_target().await;

        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        write_source(
            &source_path,
            "CREATE TABLE Namespace (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
             INSERT INTO Namespace (name, description) VALUES ('species', 'test');

             CREATE TABLE Tags (
                 id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                 count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
             INSERT INTO Tags (name, namespace) VALUES ('mammal', 1), ('canine', 1);

             CREATE TABLE FileStorageLocations (
                 id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
             INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

             CREATE TABLE File (
                 id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                 storage_id INTEGER, size_bytes INTEGER);
             INSERT INTO File (hash, extension, storage_id, size_bytes)
                 VALUES ('slurp-hash', 'jpg', 1, 42);

             CREATE TABLE Relationship_1 (
                 file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                 PRIMARY KEY (tag_id, file_id));
             INSERT INTO Relationship_1 (file_id, tag_id) VALUES (1, 1), (1, 2);

             CREATE TABLE Parents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                 relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
             INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
        )
        .await;

        let counts = db.db_slurp(&source_path).await.unwrap();
        assert_eq!(counts, (1, 2, 1));

        let conn = db.connect().unwrap();

        // The cold path keeps the named unique index and restores the covering
        // index after recounts even though no legacy Relationship existed.
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'index' AND name IN
                     ('idx_tags_name_namespace', 'idx_tags_count_covering');",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            2
        );

        // Counts recomputed per namespace from the partition aggregate pass.
        let mut rows = conn
            .query("SELECT name, count FROM Tags ORDER BY name;", ())
            .await
            .unwrap();
        let mut tag_counts = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            tag_counts.push((row.get::<String>(0).unwrap(), row.get::<u64>(1).unwrap()));
        }
        assert_eq!(
            tag_counts,
            vec![("canine".to_string(), 1), ("mammal".to_string(), 1)]
        );

        // The partition got its rows and its file_id index.
        let species_id = db.namespace_get_name_cache("species").await.unwrap();
        let mut rows = conn
            .query(
                format!("SELECT COUNT(*) FROM Relationship_{species_id};",),
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            2
        );
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'index' AND LOWER(name) IN (?1);",
                (format!("idx_relationship_{species_id}_tag_file"),),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            1
        );

        // The parent landed once.
        let mut rows = conn
            .query("SELECT COUNT(*) FROM Parents;", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            1
        );

        // Journal mode was restored to MVCC by the import.
        let mut mode = String::new();
        conn.pragma_query("journal_mode", |row| {
            mode = row.get::<String>(0).unwrap_or_default();
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(mode, "mvcc");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_jobs_skips_incomplete_jobs_table() {
        let db = new_target().await;
        let (_source_db, source, _keep) = new_source(
            "CREATE TABLE Jobs (
                 id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
                 site TEXT NOT NULL, param TEXT NOT NULL);",
        )
        .await;

        let conn = db.connect().unwrap();
        db.slurp_jobs(&conn, &source).await.unwrap();

        let jobs = target_jobs(&db).await;
        assert!(jobs.is_empty(), "missing columns must not abort the slurp");
    }

    /// Offline throughput smoke for the tag swap + swap-back: imports a few
    /// hundred thousand rows into `/tmp`, re-imports over the (now warm)
    /// destination, and checks the merged set stayed canonical. Ignored by
    /// default; run with `--ignored --nocapture` to see stage timings.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn slurp_swap_throughput_smoke() {
        let _ = fast_log::init(
            fast_log::Config::new()
                .level(log::LevelFilter::Info)
                .console(),
        );
        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        let db = new_target().await;
        {
            let source_db = turso::Builder::new_local(&source_path.to_string_lossy())
                .experimental_without_rowid(true)
                .build()
                .await
                .unwrap();
            let mut source = source_db.connect().unwrap();
            source
                .execute_batch(
                    "PRAGMA synchronous = OFF;
                     CREATE TABLE Namespace (
                         id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
                     INSERT INTO Namespace (name, description) VALUES ('species', 'test');
                     CREATE TABLE Tags (
                         id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                         count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
                     CREATE TABLE FileStorageLocations (
                         id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
                     INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
                     CREATE TABLE File (
                         id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                         storage_id INTEGER, size_bytes INTEGER);
                     CREATE TABLE Relationship (
                         file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                         PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
                     CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL, limit_to INTEGER);",
                )
                .await
                .unwrap();
            let tags = 20_000_u32;
            let files = 6_000_u32;
            let rels = 20_000_u32;
            {
                let tx = source.transaction().await.unwrap();
                for chunk in (1..=tags).collect::<Vec<_>>().chunks(10_000) {
                    let values = chunk
                        .iter()
                        .map(|i| format!("({i}, 'tag_{i}', 1)"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let _ = tx
                        .execute(
                            &format!("INSERT INTO Tags (id, name, namespace) VALUES {values};"),
                            (),
                        )
                        .await
                        .unwrap();
                }
                for chunk in (1..=files).collect::<Vec<_>>().chunks(10_000) {
                    let values = chunk
                        .iter()
                        .map(|i| format!("({i}, 'f{i:09x}', 'jpg', 1, 10)"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let _ = tx
                        .execute(
                            &format!(
                                "INSERT INTO File (id, hash, extension, storage_id, size_bytes)
                                 VALUES {values};"
                            ),
                            (),
                        )
                        .await
                        .unwrap();
                }
                for chunk in (1..=rels).collect::<Vec<_>>().chunks(10_000) {
                    let values = chunk
                        .iter()
                        .map(|i| format!("({}, {})", (i % files) + 1, (i % tags) + 1))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let _ = tx
                        .execute(
                            &format!("INSERT INTO Relationship (file_id, tag_id) VALUES {values};"),
                            (),
                        )
                        .await
                        .unwrap();
                }
                tx.commit().await.unwrap();
            }
            source
                .pragma_update("journal_mode", "'delete'")
                .await
                .unwrap();
            drop(source);
            drop(source_db);
        }

        eprintln!("source built: 20k tags, 6k files, 20k rels");
        let t0 = std::time::Instant::now();
        let counts = db.db_slurp(&source_path).await.unwrap();
        let first = t0.elapsed();
        assert_eq!(counts, (1, 20_000, 6_000));
        eprintln!("first import done: {first:?}");

        let t0 = std::time::Instant::now();
        let counts = db.db_slurp(&source_path).await.unwrap();
        let second = t0.elapsed();
        assert_eq!(counts, (1, 20_000, 6_000));
        eprintln!("re-import done: {second:?}");

        let conn = db.connect().unwrap();
        let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            20_000
        );
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_schema
                 WHERE type = 'table' AND name IN ('Tags_slurp', 'Tags_old');",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            0
        );

        println!("first import (20k tags / 6k files / 20k rels): {first:?}, re-import: {second:?}");
    }

    /// Write a standalone slurp source with `count` tags named `{prefix}_{i}`
    /// at source ids 1..=count plus the tables a slurp needs.
    async fn write_tag_source(source_path: &std::path::Path, prefix: &str, count: u32) {
        let values = (1..=count)
            .map(|i| format!("({i}, '{prefix}_{i}', 1)"))
            .collect::<Vec<_>>()
            .join(", ");
        let script = format!(
            "CREATE TABLE Namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT);
             INSERT INTO Namespace (id, name) VALUES (1, 'ns');
             CREATE TABLE Tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                                count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
             CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
             INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
             CREATE TABLE File (id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                                storage_id INTEGER, size_bytes INTEGER);
             CREATE TABLE Relationship (file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                                        PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
             CREATE TABLE Parents (id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                                   relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
             INSERT INTO Tags (id, name, namespace) VALUES {values};"
        );
        write_source(source_path, &script).await;
    }

    /// A warm import that adds genuinely new tags must not collide with the
    /// destination's existing ids: the staging table hands new rows ids past
    /// the canonical table's max, so the swap-back row copy
    /// (`INSERT INTO Tags_slurp ... SELECT ... FROM Tags`) cannot hit
    /// `UNIQUE constraint failed: tags_slurp.id`. Regression test for the
    /// production error at src/db/turso/system_jobs.rs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_warm_import_new_tags_do_not_collide_with_existing_ids() {
        let db = new_target().await;
        let temp_dir = tempfile::tempdir().unwrap();

        // Cold import fills the destination with a_* tags at ids 1..=3000.
        let source_a = temp_dir.path().join("source_a.db");
        write_tag_source(&source_a, "a", 3_000).await;
        assert_eq!(
            db.db_slurp(&source_a).await.unwrap(),
            (1, 3_000, 0),
            "cold import"
        );

        // Warm import of overlapping source ids 1..=200 with different names;
        // these are all genuinely new so the swap-back path runs.
        let source_b = temp_dir.path().join("source_b.db");
        write_tag_source(&source_b, "b", 200).await;
        assert_eq!(
            db.db_slurp(&source_b).await.unwrap(),
            (1, 200, 0),
            "warm import must succeed without a tags_slurp.id collision"
        );

        let conn = db.connect().unwrap();
        let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            3_200,
            "all old and new tags survive"
        );
        // Every tag id must exist exactly once: the new rows got fresh ids
        // past the old max and the old rows were copied back unchanged.
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM (
                     SELECT id, COUNT(*) AS seen FROM Tags GROUP BY id HAVING seen <> 1
                 );",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            0,
            "no duplicate or missing tag ids"
        );
        let mut rows = conn
            .query("SELECT COUNT(*) FROM Tags WHERE name LIKE 'a_%';", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            3_000
        );
        let mut rows = conn
            .query("SELECT COUNT(*) FROM Tags WHERE name LIKE 'b_%';", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            200
        );
    }

    /// A source whose schema contains a virtual table the limbo parser cannot
    /// load (an FTS5 table whose stored SQL quotes the tokenizer arg with
    /// double quotes, as some hydrus databases ship) must still slurp: the
    /// source is backed up with the sqlite3 CLI, the virtual tables and
    /// triggers are dropped from the copy, and the copy is imported.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_source_with_double_quoted_fts_falls_back_to_sanitized_copy() {
        let db = new_target().await;
        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        let script = "CREATE TABLE Namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT);
             INSERT INTO Namespace (id, name) VALUES (1, 'ns');
             CREATE TABLE Tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                                count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
             INSERT INTO Tags (id, name, namespace) VALUES (1, 'cat', 1), (2, 'dog', 1);
             CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
             INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
             CREATE TABLE File (id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                                storage_id INTEGER, size_bytes INTEGER);
             CREATE TABLE Relationship (file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                                        PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
             CREATE TABLE Parents (id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                                   relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
             CREATE VIRTUAL TABLE Tags_Popular_fts USING fts5(
                 name,
                 tokenize = \"unicode61 separators '_/'\"
             );";
        let status = std::process::Command::new("sqlite3")
            .arg(&source_path)
            .arg(script)
            .status()
            .unwrap();
        assert!(status.success(), "sqlite3 must be installed to run this test");

        // Precondition: turso cannot load that stored SQL directly.
        assert!(
            turso::Builder::new_local(&source_path.to_string_lossy())
                .read_only(true)
                .experimental_without_rowid(true)
                .build()
                .await
                .is_err(),
            "direct open must fail on the double-quoted FTS SQL"
        );

        // The full import routes through the sanitized copy.
        assert_eq!(
            db.db_slurp(&source_path).await.unwrap(),
            (1, 2, 0),
            "slurp must succeed through the sanitized copy"
        );
        let conn = db.connect().unwrap();
        let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            2
        );

        // No temp copies may be left behind.
        let leftover = std::fs::read_dir(std::env::temp_dir())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("intscrape_slurp_")
            })
            .count();
        assert_eq!(leftover, 0, "sanitized copies must be cleaned up");
    }
}
