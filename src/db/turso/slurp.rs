//! Turso-native database import. Unlike the legacy SQLite implementation there
//! is no `ATTACH DATABASE`: the source is opened read-only through turso and
//! every row is streamed into the destination database via the bulk-add helpers.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

use shared_types::{
    DbJobRecreation, FileInternal, GenericNamespaceObj, PluginJob, ScraperParam, Tag, TagParents,
};
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

/// Destination batch size for the tags stage. Imported tags arrive in source
/// id order, which is random relative to the (name, namespace) unique key, so
/// small per-chunk transactions spend most of their time re-searching that
/// index; 25k-row batches measured best on the slurp target hardware.
const SLURP_TAG_BATCH: i64 = 25_000;

/// Source read batch for the relationship keyset scan, per partition and for
/// the legacy table. Deliberately huge and independent of the destination
/// write chunk, for the same reason as the hash read above: the production
/// source full-scans the partition for every keyset position (a batch costs
/// ~40 s whether it returns 25k or 883k rows — the relationship copy alone
/// measures ~9.5 h of source reads), so larger reads mean fewer scans and
/// near-linear total-time savings. Memory is the only limit on the read side:
/// ~883k x 16 B/row keeps each batch well under 100 MB.
const SLURP_RELATIONSHIP_READ_BATCH: i64 = SQL_CHUNK_SIZE as i64 * 192; // 883,200

/// Destination transaction size for the relationship write. The read batch
/// above is orders of magnitude larger than this; a single shared transaction
/// that absorbs the whole read batch grows superlinearly on the slurp target
/// (see SLURP_HASH_WRITE_BATCH for the measured numbers), so the write is split
/// into bounded sub-transactions, each chunking its INSERT OR IGNORE at
/// SLURP_RELATIONSHIP_WRITE_BATCH inside `slurp_relationships_bulk_add`.
const SLURP_RELATIONSHIP_WRITE_BATCH: usize = 25_000;

/// Source read batch for the secondary-hash keyset scan. Deliberately huge and
/// independent of the destination write chunk: on the production source the
/// reader full-scans the FileHashes b-tree for every keyset position (a batch
/// costs ~25-30s whether it returns 4.6k or 883k rows), so larger reads mean
/// fewer scans and near-linear total-time savings. Memory is the only limit on
/// the read side: ~200 B/row in the tuple vec, so 4M rows is ~800 MB and
/// finishes the whole 45.8M-row hash stage in ~9 min (52 scans at 883k).
const SLURP_HASH_READ_BATCH: i64 = SQL_CHUNK_SIZE as i64 * 192; // 883,200

/// Destination transaction size for the hash write. The read batch above can
/// be orders of magnitude larger than this. A single shared transaction that
/// absorbs the whole read batch grows superlinearly on the slurp target
/// (measured ~57x write time for a 6x bigger batch: 37-41s at 883k rows when
/// the same rows take ~0.7s in 147k-row transactions), so the write is split
/// into bounded sub-transactions; each sub-transaction then chunks its
/// INSERT OR IGNORE statements at SQL_CHUNK_SIZE inside `file_hashes_add_bulk`.
const SLURP_HASH_WRITE_BATCH: usize = 25_000;

/// Destination table that persists where an interrupted slurp got to. Each
/// stage records the last source key whose rows fully committed, so a
/// rescheduled run (after a disk-full, a kill, or an MVCC-conflict retry)
/// skips the batches that already imported instead of re-reading the whole
/// source — the relationship copy alone measures ~9.5 h of source reads on
/// the production source. The table is dropped when a slurp completes, and a
/// `meta` row keys the cursors to the exact source file so stale progress is
/// never applied to a different database.
const SLURP_CHECKPOINT_TABLE: &str = "_slurp_checkpoint";

/// Per-batch source-id -> target-id maps for the tags and files stages. Rows
/// are written in the same transaction as the batch they describe, so a
/// checkpoint cursor never outpaces the mapping the relationship and hash
/// stages need to route their own rows. Both tables are dropped when a slurp
/// completes.
const SLURP_TAG_MAP_TABLE: &str = "_slurp_tag_map";
const SLURP_FILE_MAP_TABLE: &str = "_slurp_file_map";

/// Keyset scans build `{column} > last` from a seed of 0, which silently drops
/// a source row whose id is 0 (some hydrus databases genuinely assign a File
/// the id 0, main.db's 6.8M-row File table has one). Seed the first read at
/// -1 so `> -1` also covers id 0, then advance with real ids.
fn keyset_bound(first_pass: bool, last: u64) -> i64 {
    if first_pass { -1 } else { last as i64 }
}

/// Whether the source's `table` has an index its (file_id, tag_id) keyset
/// scan can use: a composite PRIMARY KEY starting with file_id, or an explicit
/// index whose leading columns are file_id, tag_id. The slurp opens the source
/// read-only, so a missing index can only be reported, not created here — but
/// it is the measured ~9.5 h bottleneck in the relationship copy, so the
/// operator needs to know. Pure diagnostics: any introspection failure returns
/// None and is silently skipped, never aborting the slurp.
async fn slurp_source_keyset_indexed(source: &Connection, table: &str) -> Option<bool> {
    // Composite PRIMARY KEY (file_id, tag_id) — clustered (WITHOUT ROWID) or
    // autoindexed (rowid table) — already serves the keyset. `PRAGMA table_info`
    // reports pk positions 1..n in key order.
    let mut pk_columns: Vec<(u64, String)> = Vec::new();
    {
        let mut stmt = source
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .await
            .ok()?;
        let mut rows = stmt.query(()).await.ok()?;
        loop {
            let row = match rows.next().await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(_) => return None,
            };
            let position = row.get::<u64>(5).ok()?;
            let name = row.get::<String>(1).ok()?;
            if position > 0 {
                pk_columns.push((position, name));
            }
        }
    }
    pk_columns.sort_by_key(|&(position, _)| position);
    let pk_order: Vec<String> = pk_columns.into_iter().map(|(_, name)| name).collect();
    if pk_order.len() >= 2 && pk_order[0] == "file_id" && pk_order[1] == "tag_id" {
        return Some(true);
    }

    // Explicit indexes: check each one's leading columns.
    let mut stmt = source
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type = 'index' AND lower(tbl_name) = lower(?1)",
        )
        .await
        .ok()?;
    let mut rows = stmt.query((table,)).await.ok()?;
    while let Some(row) = rows.next().await.ok().flatten() {
        let index_name: String = row.get(0).ok()?;
        if slurp_index_leading_keyset_columns(source, &index_name).await {
            return Some(true);
        }
    }
    Some(false)
}

/// Whether a single index's leading columns are (file_id, tag_id).
async fn slurp_index_leading_keyset_columns(source: &Connection, index: &str) -> bool {
    let mut stmt = match source
        .prepare(&format!("PRAGMA index_info(\"{index}\")"))
        .await
    {
        Ok(stmt) => stmt,
        Err(_) => return false,
    };
    let mut rows = match stmt.query(()).await {
        Ok(rows) => rows,
        Err(_) => return false,
    };
    let mut columns = Vec::new();
    loop {
        match rows.next().await {
            Ok(Some(row)) => match row.get::<String>(2) {
                Ok(name) => columns.push(name),
                Err(_) => return false,
            },
            Ok(None) => break,
            Err(_) => return false,
        }
    }
    columns.len() >= 2 && columns[0] == "file_id" && columns[1] == "tag_id"
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

        // The slurp no longer pauses the IPC server or the background pollers:
        // it runs alongside live UI traffic and other jobs. Its write side is
        // a sequence of short BEGIN IMMEDIATE batch transactions that commit
        // between every batch, so scraper writers (which fall back from
        // CONCURRENT to IMMEDIATE while the WAL fast lane holds) and readers
        // interleave with the import. Conflicts surface as Busy errors and are
        // retried by the loop below; every committed batch is idempotent
        // (INSERT OR IGNORE bulk-add helpers plus a per-batch cursor), so a
        // retried or resumed attempt never double-imports. The slurping flag
        // stays up for the duration so the spawner defers further slurp jobs
        // and write_tx_behavior keeps serving IMMEDIATE to scrape writers.
        // The flag is cleared even when the loop bails on a non-MVCC error so
        // a failed slurp never leaves the app paused behind it.
        self.set_slurping(true);
        // Snapshot the mode the destination serves in before the import (WAL
        // on a normal boot). The import streams into the same WAL journal; a
        // destination that is somehow still in MVCC (opened by an older core
        // before the WAL boot change) is flipped to WAL for the import and
        // restored to its pre-import mode afterwards. WAL streams b-tree
        // builds and large autocommits (the post-copy index rebuild, the
        // Tags_slurp swap) to temp files and checkpointed pages instead of
        // materializing the entire delta in the in-memory MVCC commit log
        // (~7x faster index builds, ~2x faster inserts on the slurp builder).
        // check_db re-applies the serving mode on the next boot, so a crash
        // mid-import self-heals even if the restore below is skipped.
        let serving_mode = self
            .journal_mode()
            .await
            .unwrap_or_else(|| "wal".to_owned());
        let wal_fast_lane = self.try_set_journal_mode("wal").await;
        if wal_fast_lane {
            log::info!(
                "Turso slurp using WAL journal mode (commit-log fast lane; restoring {serving_mode} after)."
            );
        } else {
            log::warn!(
                "Turso slurp could not switch to WAL; falling back to the in-memory \
                 commit-log path (slower index builds)."
            );
        }
        let result = loop {
            match self.internal_db_slurp(&source_conn, source).await {
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
            if self.try_set_journal_mode(&serving_mode).await {
                log::info!("Turso slurp restored {serving_mode} journal mode.");
            } else {
                log::warn!(
                    "Turso slurp could not restore {serving_mode}; check_db re-applies it on the next boot."
                );
            }
        }
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
            let (Some(kind), Some(name), None) = (parts.next(), parts.next(), parts.next()) else {
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

    /// Best-effort fold of the WAL back into the main file before restoring
    /// the pre-import journal mode, so the restored mode does not have to
    /// ingest a growing WAL.
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
    async fn internal_db_slurp(
        &self,
        source: &Connection,
        source_path: &Path,
    ) -> Result<(u64, u64, u64)> {
        let mut conn = self.connect()?;

        // Resume support: read any progress an interrupted earlier run left in
        // the destination. The checkpoint table records, per stage, the last
        // source key whose rows fully committed to the destination, so a
        // rescheduled run skips every batch that already landed instead of
        // re-reading the whole source. Cursors are scoped to the exact source
        // file: when the source changed, stale progress is discarded. The
        // MVCC-conflict retry loop in `db_slurp` also benefits: a retry
        // resumes past the batches the failed attempt already committed.
        conn.execute(
            format!(
                "CREATE TABLE IF NOT EXISTS {SLURP_CHECKPOINT_TABLE} (
                     stage TEXT PRIMARY KEY,
                     finished INTEGER NOT NULL DEFAULT 0,
                     last_file_id INTEGER NOT NULL DEFAULT 0,
                     last_tag_id INTEGER NOT NULL DEFAULT 0,
                     source_key TEXT NOT NULL DEFAULT '');"
            ),
            (),
        )
        .await?;
        // Source-id -> target-id maps for the tags and files stages, written in
        // the same transaction as each batch so a cursor never advances past a
        // batch whose mapping did not land. The relationship and hash stages
        // resolve through these maps, so a resumed run can skip the batches
        // behind the cursor and still route every later row to the right
        // target id.
        for table in [SLURP_TAG_MAP_TABLE, SLURP_FILE_MAP_TABLE] {
            conn.execute(
                format!(
                    "CREATE TABLE IF NOT EXISTS {table} (
                         source_id INTEGER PRIMARY KEY,
                         target_id INTEGER NOT NULL);"
                ),
                (),
            )
            .await?;
        }
        let source_key = slurp_source_key(source_path);
        {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_key FROM {SLURP_CHECKPOINT_TABLE}
                     WHERE stage = 'meta'"
                ))
                .await?;
            let stored_source: Option<String> = match stmt.query_row(()).await {
                Ok(row) => Some(row.get(0)?),
                Err(_) => None,
            };
            if stored_source.as_deref() != Some(source_key.as_str()) {
                if stored_source.is_some() {
                    log::info!(
                        "Slurp source changed since the last run; discarding resume progress."
                    );
                }
                conn.execute(format!("DELETE FROM {SLURP_CHECKPOINT_TABLE}"), ())
                    .await?;
                for table in [SLURP_TAG_MAP_TABLE, SLURP_FILE_MAP_TABLE] {
                    conn.execute(format!("DELETE FROM {table}"), ()).await?;
                }
            }
        }
        conn.execute(
            &format!(
                "INSERT INTO {SLURP_CHECKPOINT_TABLE}
                     (stage, finished, last_file_id, last_tag_id, source_key)
                 VALUES ('meta', 0, 0, 0, ?1)
                 ON CONFLICT(stage) DO UPDATE SET source_key = excluded.source_key"
            ),
            (source_key.as_str(),),
        )
        .await?;
        let checkpoint = slurp_checkpoint_load(&conn).await?;

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
        // partitions — DDL, which must run outside the write transactions of
        // the scrape loop. `namespace_ensure_set` does it in a short
        // exclusive transaction up front (also seeding the in-memory
        // namespace cache) so the immediate copy below only ever executes
        // DML, and so its snapshot sees the committed namespace rows. Now
        // largely a no-op once namespaces are cached (the common warm-db
        // case).
        let namespace_bulk = self.namespace_ensure_set(&namespace_set).await?;
        let namespace_count = namespace_bulk.len() as u64;
        log::info!(
            "Slurp stage namespaces complete: {} namespaces in {:?}",
            namespace_count,
            slurp_started.elapsed()
        );

        // Storage locations: re-use whatever rows exist, creating the rest.
        // Keep each batch short so scraper writes can commit between batches.
        //
        // NOTE: the import's BEGIN IMMEDIATE transactions are deliberately NOT
        // CONCURRENT. The bulk import is one writer walking a source db, so
        // there is nothing to overlap; and it is saturated with DDL (CREATE /
        // DROP / ALTER TABLE and INDEX, see namespace partitioning and the
        // Tags_slurp swap), which CONCURRENT rejects outright. The scraper
        // write paths use `TursoDatabase::write_tx_behavior()` instead.
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
        // Tags.count row (each rewrite churns the MVCC image).
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
            // Resume: a tags cursor from an interrupted earlier run points at
            // the last source id whose batch fully committed (the cursor is
            // written inside the batch's own transaction). Its mapping rows
            // live in _slurp_tag_map, so the covered batches are skipped whole
            // and the relationship stage still routes through the preloaded
            // map. A cursor without map rows (state left by an older build) is
            // discarded: re-walking is idempotent, skipping blind is not.
            let tags_resume = checkpoint.get("tags").copied().unwrap_or_default();
            let mut tags_map_loaded = 0_usize;
            if tags_resume.last_tag_id > 0 {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT source_id, target_id FROM {SLURP_TAG_MAP_TABLE}"
                    ))
                    .await?;
                let mut rows = stmt.query(()).await?;
                while let Some(row) = rows.next().await? {
                    slurp_tags.insert(row.get::<i64>(0)? as u64, row.get::<i64>(1)? as u64);
                    tags_map_loaded += 1;
                }
                if tags_map_loaded == 0 {
                    log::warn!(
                        "Slurp tags: cursor {} has no mapping rows; re-walking the stage from the start.",
                        tags_resume.last_tag_id
                    );
                }
            }
            let tags_resuming = tags_resume.last_tag_id > 0 && tags_map_loaded > 0;
            // The staging table holds committed batches when a warm import is
            // resumed mid-stage; dropping it here would lose those rows. A
            // fresh warm run always starts with an empty staging table.
            if !tags_resuming {
                conn.execute("DROP TABLE IF EXISTS Tags_slurp", ()).await?;
            }

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
                log::info!("Slurp tags: cold import, inserting directly into indexed Tags");
            } else {
                // IF NOT EXISTS: a resumed warm run keeps the staging table
                // the interrupted attempt left behind (it holds committed
                // batches covered by the cursor).
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS Tags_slurp (
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
                let mut stmt = conn
                    .prepare("SELECT COALESCE(MAX(id), 0) FROM Tags")
                    .await?;
                stmt.query_row(()).await?.get(0)?
            };
            if tags_resuming {
                // Committed batches of the interrupted attempt already occupy
                // ids in the staging table; new rows must continue above both
                // id spaces or the staging insert would collide. The staging
                // table can be absent here: a resumed cold import's rows went
                // straight into canonical Tags, so this attempt creates the
                // staging table fresh below and starts from canonical's max.
                let mut exists = conn
                    .prepare(
                        "SELECT EXISTS(
                             SELECT 1 FROM sqlite_schema
                             WHERE type = 'table' AND name = 'Tags_slurp'
                         )",
                    )
                    .await?;
                let has_staging: i64 = exists.query_row(()).await?.get(0)?;
                if has_staging != 0 {
                    let mut stmt = conn
                        .prepare("SELECT COALESCE(MAX(id), 0) FROM Tags_slurp")
                        .await?;
                    let staged_max: i64 = stmt.query_row(()).await?.get(0)?;
                    next_insert_id = next_insert_id.max(staged_max);
                }
            }
            let mut last_tag_id = tags_resume.last_tag_id;
            let mut first_tag_pass = !tags_resuming;
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
                // Persist the batch's source->target mapping and the stage
                // cursor inside the same transaction as the batch itself, so
                // the cursor can never advance past mapping rows (or tag rows)
                // that did not land.
                let mut pairs: Vec<(u64, i64)> = Vec::with_capacity(batch.len());
                for (source_id, name, namespace, description) in &batch {
                    let key = Tag {
                        name: name.clone(),
                        namespace: GenericNamespaceObj {
                            name: namespace.clone(),
                            description: description.clone(),
                        },
                    };
                    if let Some(&target_id) = mapping.get(&key) {
                        pairs.push((*source_id, target_id));
                    }
                }
                if !pairs.is_empty() {
                    let mut holders = Vec::with_capacity(pairs.len());
                    let mut params: Vec<Value> = Vec::with_capacity(pairs.len() * 2);
                    for (source_id, target_id) in &pairs {
                        holders.push("(?, ?)");
                        params.push(Value::from(*source_id as i64));
                        params.push(Value::from(*target_id));
                    }
                    conn.execute(
                        &format!(
                            "INSERT OR IGNORE INTO {SLURP_TAG_MAP_TABLE}
                                 (source_id, target_id) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await?;
                }
                slurp_checkpoint_write(
                    &conn,
                    "tags",
                    SlurpStageCheckpoint {
                        finished: false,
                        last_file_id: 0,
                        last_tag_id: *last_id,
                    },
                )
                .await?;
                conn.execute("COMMIT", ()).await?;
                tag_count += batch.len() as u64;
                tags_added += batch_new;
                for (source_id, target_id) in &pairs {
                    slurp_tags.insert(*source_id, *target_id as u64);
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
            //
            // The swap decision counts the staging table, not just this
            // attempt's additions: a resumed run may add nothing while the
            // interrupted attempt's committed batches still sit staged, and
            // those must go through the swap or they would be lost. Staging
            // rows are exactly the genuinely-new tags across attempts, so
            // folding them into tags_added also keeps the relationship
            // stage's recount/cold-import heuristics correct on resume.
            let staged_rows: usize = if !cold_import {
                // The staging table may not exist at all: a previous attempt
                // can have completed the swap before crashing later, leaving
                // Tags_slurp renamed into canonical Tags.
                let mut exists = conn
                    .prepare(
                        "SELECT EXISTS(
                             SELECT 1 FROM sqlite_schema
                             WHERE type = 'table' AND name = 'Tags_slurp'
                         )",
                    )
                    .await?;
                let has_staging: i64 = exists.query_row(()).await?.get(0)?;
                if has_staging != 0 {
                    let mut stmt = conn.prepare("SELECT COUNT(*) FROM Tags_slurp").await?;
                    stmt.query_row(()).await?.get::<i64>(0)? as usize
                } else {
                    0
                }
            } else {
                0
            };
            tags_added = tags_added.max(staged_rows);
            if cold_import {
                // idx_tags_name_namespace carried every insert up front; the
                // covering index is rebuilt once here (counts were all 0
                // through the copy), and the final stage rebuilds the popular
                // ngram-FTS shadow (`Tags_Popular`) over the whole table.
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
                // mapping stays valid — the popular-FTS shadow references
                // those same ids and survives the rename untouched. The row
                // copy is done in chunks of SLURP_TAG_BATCH under their own
                // short transactions: a single `INSERT ... SELECT` over all
                // rows is one multi-GB MVCC commit that serializes the whole
                // delta into an in-memory log (measured ~14.5 min and ~11 GB
                // RAM for 15M rows), whereas chunking (~25k per commit) runs
                // ~2x faster and uses ~100x less memory (measured ~6.8 min,
                // ~90 MB).
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

            // Resume: a files cursor from an interrupted earlier run points at
            // the last source id whose batch fully committed (cursor and map
            // rows are written inside the batch's own transaction). The
            // skipped batches' source->target mapping is preloaded from
            // _slurp_file_map so the hash and relationship stages still route
            // through it. A cursor without map rows (older-build state) is
            // discarded; re-walking is idempotent, skipping blind is not.
            let files_resume = checkpoint.get("files").copied().unwrap_or_default();
            let mut files_map_loaded = 0_usize;
            if files_resume.last_file_id > 0 {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT source_id, target_id FROM {SLURP_FILE_MAP_TABLE}"
                    ))
                    .await?;
                let mut rows = stmt.query(()).await?;
                while let Some(row) = rows.next().await? {
                    slurp_files.insert(row.get::<i64>(0)? as u64, row.get::<i64>(1)? as u64);
                    files_map_loaded += 1;
                }
                if files_map_loaded == 0 {
                    log::warn!(
                        "Slurp files: cursor {} has no mapping rows; re-walking the stage from the start.",
                        files_resume.last_file_id
                    );
                }
            }
            let files_resuming = files_resume.last_file_id > 0 && files_map_loaded > 0;
            let mut last_file_id = files_resume.last_file_id;
            let mut first_file_pass = !files_resuming;
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
                let target_by_hash: HashMap<&str, u64> = resolved
                    .iter()
                    .filter_map(|file| file.id.map(|id| (file.hash.as_str(), id)))
                    .collect();
                // Persist the batch's source->target mapping and the stage
                // cursor inside the same transaction as the batch itself, so
                // the cursor can never advance past mapping rows (or file
                // rows) that did not land.
                let mut pairs: Vec<(u64, i64)> = Vec::with_capacity(batch.len());
                for (source_id, hash, _, _, _) in &batch {
                    if let Some(&target_id) = target_by_hash.get(hash.as_str()) {
                        pairs.push((*source_id, target_id as i64));
                    }
                }
                if !pairs.is_empty() {
                    let mut holders = Vec::with_capacity(pairs.len());
                    let mut params: Vec<Value> = Vec::with_capacity(pairs.len() * 2);
                    for (source_id, target_id) in &pairs {
                        holders.push("(?, ?)");
                        params.push(Value::from(*source_id as i64));
                        params.push(Value::from(*target_id));
                    }
                    conn.execute(
                        &format!(
                            "INSERT OR IGNORE INTO {SLURP_FILE_MAP_TABLE}
                                 (source_id, target_id) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await?;
                }
                slurp_checkpoint_write(
                    &conn,
                    "files",
                    SlurpStageCheckpoint {
                        finished: false,
                        last_file_id: *last_id,
                        last_tag_id: 0,
                    },
                )
                .await?;
                conn.execute("COMMIT", ()).await?;
                for (source_id, target_id) in &pairs {
                    slurp_files.insert(*source_id, *target_id as u64);
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
                // Resume: every FileHashes row with file_id <= the recorded
                // cursor already committed, so the keyset scan jumps straight
                // past it. A stage that previously finished resumes with one
                // empty read. The cursor only ever advances past committed
                // batches (the sub-transactions above land before the write
                // below), so re-running at worst re-reads a partially imported
                // batch into INSERT OR IGNORE, never skips missing rows.
                let previous = checkpoint.get("hashes").copied().unwrap_or_default();
                let mut last_file_id = previous.last_file_id;
                let mut first_hash_pass = last_file_id == 0;
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
                            SLURP_HASH_READ_BATCH,
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
                        // Commit in bounded sub-transactions: one giant
                        // transaction for the whole read batch grows
                        // superlinearly on the slurp target (see
                        // SLURP_HASH_WRITE_BATCH).
                        for sub in tuples.chunks(SLURP_HASH_WRITE_BATCH) {
                            conn.execute("BEGIN IMMEDIATE", ()).await?;
                            self.file_hashes_add_bulk(&conn, sub).await?;
                            conn.execute("COMMIT", ()).await?;
                        }
                        log::info!(
                            "Slurp hashes batch target complete: {} rows in {:?} (total {:?})",
                            tuples.len(),
                            target_started.elapsed(),
                            batch_started.elapsed()
                        );
                    }
                    // Persist only after the whole read batch committed above.
                    slurp_checkpoint_write(
                        &conn,
                        "hashes",
                        SlurpStageCheckpoint {
                            finished: false,
                            last_file_id: *last_id,
                            last_tag_id: 0,
                        },
                    )
                    .await?;
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
            // A recount dropped the covering index in an interrupted earlier
            // run, then crashed before the end-of-stage restore. Detect a
            // missing index and treat it as though this run dropped it, so the
            // single shared restore below re-creates it even when this run
            // never recounts. (The tags stage creates the index on cold
            // imports, so its absence here always means a crashed recount.)
            let mut covering_dropped = {
                let mut stmt = conn
                    .prepare(
                        "SELECT EXISTS(
                             SELECT 1 FROM sqlite_master
                             WHERE type = 'index' AND name = 'idx_tags_count_covering'
                         )",
                    )
                    .await?;
                stmt.query_row(()).await?.get::<i64>(0)? == 0
            };

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
            // The tag map table outlives the attempt that wrote it: on a
            // resumed run it holds the genuinely-new tags the interrupted
            // attempt landed (a resumed cold import's rows sit in canonical
            // Tags, invisible to this attempt's tags_added), so the index
            // drop/rebuild and recount paths must fire for them too.
            let tag_map_nonempty: bool = {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT EXISTS(SELECT 1 FROM {SLURP_TAG_MAP_TABLE})"
                    ))
                    .await?;
                stmt.query_row(()).await?.get::<i64>(0)? != 0
            };
            let cold_import = tags_added > 0 || tag_map_nonempty;
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
                // The source is opened read-only, so a missing keyset index can
                // only be reported, not created here: without it every batch
                // re-scans the whole partition (~40 s each measured), which is
                // the ~9.5 h relationship-stage bottleneck. Adding
                // `CREATE INDEX ... ON <table>(file_id, tag_id)` to the source
                // (or a local copy of it) makes the scan index-driven.
                if let Some(indexed) = slurp_source_keyset_indexed(&source, &partition).await {
                    if !indexed {
                        log::warn!(
                            "Slurp: source table {partition} has no (file_id, tag_id) index; \
                             the keyset scan re-scans the whole table per batch. Add \
                             `CREATE INDEX idx_{partition}_file_tag \
                             ON {partition}(file_id, tag_id)` to the source (or a copy of it)."
                        );
                    }
                }
                // Resume: a partition whose scan already reached its natural
                // end (empty batch) with its recount and index restore
                // finished is skipped whole by a re-run; one cut short by a
                // crash resumes its keyset scan at the recorded cursor. Only
                // fully committed batches lie behind the cursor, so rows never
                // fall through, and namespaces that gained rows in the tail of
                // an interrupted run still get their recount via
                // `_slurp_pending_recount`.
                let stage_key = format!("relationship_partition_{source_namespace}");
                let resume = checkpoint.get(&stage_key).copied().unwrap_or_default();
                let already_done = resume.finished;
                if !already_done {
                    let mut last_file_id = resume.last_file_id;
                    let mut last_tag_id = resume.last_tag_id;
                    let mut first_rel_pass = last_file_id == 0 && last_tag_id == 0;
                    let mut added_here = 0_usize;
                    // Prepared once per partition. Recompiling the keyset SELECT
                    // per batch adds pure overhead to a hot path already paying
                    // ~40 s per keyset position on sources that re-scan the whole
                    // partition (see SLURP_RELATIONSHIP_*).
                    let mut source_scan = source
                        .prepare(&format!(
                            "SELECT r.file_id, r.tag_id
                             FROM {} r
                             WHERE r.file_id > ?1 OR (r.file_id = ?1 AND r.tag_id > ?2)
                             ORDER BY r.file_id, r.tag_id
                             LIMIT ?3",
                            partition
                        ))
                        .await?;
                    loop {
                        let batch_started = Instant::now();
                        let mut rows = source_scan
                            .query([
                                keyset_bound(first_rel_pass, last_file_id),
                                keyset_bound(first_rel_pass, last_tag_id),
                                SLURP_RELATIONSHIP_READ_BATCH,
                            ])
                            .await?;
                        let mut batch: Vec<(u64, u64)> = Vec::new();
                        while let Some(row) = rows.next().await? {
                            batch.push((row.get(0)?, row.get(1)?));
                        }
                        drop(rows);
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
                        // Write the large read batch in bounded sub-transactions,
                        // mirroring the hash stage: destination commits lose
                        // superlinearly above ~25k rows, while the source read is
                        // the real bottleneck (see SLURP_RELATIONSHIP_*).
                        for sub in relationships.chunks(SLURP_RELATIONSHIP_WRITE_BATCH) {
                            conn.execute("BEGIN IMMEDIATE", ()).await?;
                            let added = self
                                .slurp_relationships_bulk_add(&conn, target_namespace, sub)
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
                        }

                        // The batch committed above; only now may the cursor
                        // advance past it.
                        slurp_checkpoint_write(
                            &conn,
                            &stage_key,
                            SlurpStageCheckpoint {
                                finished: false,
                                last_file_id: *source_file_id,
                                last_tag_id: *source_tag_id,
                            },
                        )
                        .await?;
                        last_file_id = *source_file_id;
                        last_tag_id = *source_tag_id;
                        log::info!(
                            "Slurp relationships source read: {} rows, total {:?}; target write {:?}",
                            batch.len(),
                            batch_started.elapsed(),
                            target_started.elapsed()
                        );
                    }

                    if cold_import || added_here > 0 || pending_recount.contains(&target_namespace)
                    {
                        // Tags.count churns idx_tags_count_covering (ordered by
                        // count) on every recount UPDATE: each reordering
                        // mutates the count b-tree and turso's commit-log
                        // materialization pins memory with no wal progress
                        // until it is rebuilt. Drop it once around every
                        // recount and restore after all recalcs land, mirroring
                        // the cold path.
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

                    // The whole partition landed and its post-scan work is
                    // done; later runs can skip it entirely.
                    slurp_checkpoint_write(
                        &conn,
                        &stage_key,
                        SlurpStageCheckpoint {
                            finished: true,
                            last_file_id,
                            last_tag_id,
                        },
                    )
                    .await?;
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
                if let Some(indexed) = slurp_source_keyset_indexed(&source, "Relationship").await {
                    if !indexed {
                        log::warn!(
                            "Slurp: source table Relationship has no (file_id, tag_id) \
                             index; the keyset scan re-scans the whole table per batch. \
                             Add `CREATE INDEX idx_Relationship_file_tag \
                             ON Relationship(file_id, tag_id)` to the source (or a copy of it)."
                        );
                    }
                }
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

                // Resume: same cursor scheme as the per-partition path, over
                // the single shared scan. A finished pass is skipped whole; an
                // interrupted one resumes at the recorded (file_id, tag_id)
                // boundary. The per-namespace recount loop below still runs
                // for the tail of the row stream — namespaces that gained rows
                // carry a `_slurp_pending_recount` marker.
                let resume = checkpoint
                    .get("relationship_legacy")
                    .copied()
                    .unwrap_or_default();
                let already_done = resume.finished;
                if !already_done {
                    let mut last_file_id = resume.last_file_id;
                    let mut last_tag_id = resume.last_tag_id;
                    let mut first_legacy_rel_pass = last_file_id == 0 && last_tag_id == 0;
                    let mut added_by_namespace: HashMap<u64, usize> = HashMap::new();
                    // Prepared once for the whole legacy pass (see the partition
                    // path for why re-preparing per batch is pure overhead).
                    let mut source_scan = source
                        .prepare(
                            "SELECT r.file_id, r.tag_id, t.namespace
                             FROM Relationship r
                             JOIN Tags t ON t.id = r.tag_id
                             WHERE (r.file_id > ?1 OR (r.file_id = ?1 AND r.tag_id > ?2))
                             ORDER BY r.file_id, r.tag_id
                             LIMIT ?3",
                        )
                        .await?;
                    loop {
                        let batch_started = Instant::now();
                        let mut rows = source_scan
                            .query([
                                keyset_bound(first_legacy_rel_pass, last_file_id),
                                keyset_bound(first_legacy_rel_pass, last_tag_id),
                                SLURP_RELATIONSHIP_READ_BATCH,
                            ])
                            .await?;
                        let mut batch: Vec<(u64, u64, u64)> = Vec::new();
                        while let Some(row) = rows.next().await? {
                            batch.push((row.get(0)?, row.get(1)?, row.get(2)?));
                        }
                        drop(rows);
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
                        let target_started = Instant::now();
                        for (target_namespace, relationships) in namespace_groups.iter_mut() {
                            // Align with the partition PRIMARY KEY (tag_id, file_id)
                            // so the destination pages stay hot despite the source
                            // scan arriving in file order.
                            relationships
                                .sort_unstable_by_key(|&(file_id, tag_id)| (tag_id, file_id));
                            log::info!(
                                "Slurping {} relationships into the db.",
                                relationships.len()
                            );
                            // Bounded sub-transactions per chunk (see the partition
                            // path): the source read is the bottleneck, so large
                            // reads must not balloon into one huge destination
                            // commit.
                            for sub in relationships.chunks(SLURP_RELATIONSHIP_WRITE_BATCH) {
                                let tn = conn.transaction().await?;
                                let added = self
                                    .slurp_relationships_bulk_add(&tn, *target_namespace, sub)
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
                            }
                        }

                        // Every namespace transaction in this read committed
                        // above; only now may the cursor advance past it.
                        slurp_checkpoint_write(
                            &conn,
                            "relationship_legacy",
                            SlurpStageCheckpoint {
                                finished: false,
                                last_file_id: next_file_id,
                                last_tag_id: next_tag_id,
                            },
                        )
                        .await?;
                        last_file_id = next_file_id;
                        last_tag_id = next_tag_id;
                        log::info!(
                            "Slurp relationships source read: {} rows, total {:?}; target write {:?}",
                            batch.len(),
                            batch_started.elapsed(),
                            target_started.elapsed()
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
                    }

                    // The whole shared scan plus its recounts are done; later
                    // runs can skip the legacy pass entirely.
                    slurp_checkpoint_write(
                        &conn,
                        "relationship_legacy",
                        SlurpStageCheckpoint {
                            finished: true,
                            last_file_id,
                            last_tag_id,
                        },
                    )
                    .await?;
                }

                for &(_, target_namespace) in &legacy_namespaces {
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
                let (tag_id, relate_tag_id, limit_to): (u64, u64, Option<i64>) =
                    (row.get(0)?, row.get(1)?, row.get(2)?);
                // The source db can be any schema generation: pre-v10 sources
                // store SQL `NULL`, v10+ sources store the `-1` sentinel. Both
                // mean "no limit tag", and a real tag id is always > 0.
                let limit_tag_id = limit_to.filter(|stored| *stored != -1).map(|id| id as u64);
                if let (Some(&tag_id), Some(&relate_tag_id)) =
                    (slurp_tags.get(&tag_id), slurp_tags.get(&relate_tag_id))
                {
                    parent_set.insert(TagParents {
                        tag_id,
                        relate_tag_id,
                        limit_to: limit_tag_id.and_then(|id| slurp_tags.get(&id).copied()),
                    });
                }
            }
            log::info!("Slurping {} parents into the db.", parent_set.len());

            // --- CRASH RECOVERY CHECK ---
            // If a previous run crashed between the rename and the drop,
            // Parents will be missing/incomplete, but Parents_old will hold the good data.
            {
                // Name probing must fold case: turso canonicalizes schema
                // names to lowercase on disk, so a case-sensitive
                // `name = 'Parents'` matches nothing and the recovery branch
                // would never fire.
                let mut check = conn
                    .prepare(
                        "SELECT EXISTS(
                             SELECT 1 FROM sqlite_schema
                             WHERE type = 'table' AND LOWER(name) = LOWER(?1)
                         )",
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
                    // interrupted run had already dropped them from Parents_old,
                    // and may carry a stale idx_parents_lim if the run predates
                    // the lean Parents schema. Recreate both directions with the
                    // definitions the current schema uses — the IFNULL unique
                    // key is deliberately shape-agnostic here: it dedupes
                    // correctly whether the restored table still stores NULL
                    // (pre-v10) or the -1 sentinel (v10+). The rename dance
                    // below rebuilds the table fresh either way, so these only
                    // cover a run that dies again after this point.
                    conn.execute_batch(
                        "DROP INDEX IF EXISTS idx_parents_lim;
                         CREATE INDEX IF NOT EXISTS idx_parents_rel
                             ON Parents (relate_tag_id, tag_id, limit_to);
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
            // gets its full index set, including the plain composite unique
            // index that makes the bulk INSERT OR IGNORE dedupe warm re-slurps.
            conn.execute_batch("PRAGMA foreign_keys = OFF;").await?;
            conn.execute_batch(
                "ALTER TABLE Parents RENAME TO Parents_old;
                 DROP INDEX IF EXISTS idx_parents_lim;
                 DROP INDEX IF EXISTS idx_parents_rel;
                 DROP INDEX IF EXISTS idx_unique_parents_null_safe;
                 DROP INDEX IF EXISTS idx_unique_parents;",
            )
            .await?;

            self.table_create_parents(&conn).await?;

            // 2. Copy existing data from Parents_old into the freshly
            // constrained Parents table, deduplicating on the shared
            // "no limit tag" key: `IFNULL(limit_to, -1)` maps both the SQL
            // NULL of pre-v10 rows and the v10+ sentinel to one group.
            log::info!("Copying and deduplicating existing parents data");
            conn.execute(
                "INSERT INTO Parents (tag_id, relate_tag_id, limit_to)
                 SELECT tag_id, relate_tag_id, IFNULL(limit_to, -1)
                 FROM Parents_old
                 GROUP BY tag_id, relate_tag_id, IFNULL(limit_to, -1)",
                (),
            )
            .await?;

            // 3. Insert new parents in chunks; the unique index on
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

        // Rebuild the popular-tag FTS shadow once, after every stage (and
        // every recount) is done: the shadow only carries `count >= 5` tags
        // and the ngram index is built on that filtered set, so it stays
        // small. Replacing the whole table also resets any stale rows left by
        // an interrupted earlier run. The bulk `INSERT ... SELECT` lands in a
        // bare table, then one index build + OPTIMIZE merge worth of segment
        // churn happens exactly once — unlike a per-row trigger design, the
        // recount paths never maintain the index incrementally.
        conn.execute_batch(
            "DROP TABLE IF EXISTS Tags_Popular;
             CREATE TABLE Tags_Popular (
                 tag_id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL
             );
             INSERT INTO Tags_Popular(tag_id, name)
                 SELECT id, name FROM Tags WHERE count >= 5;
             CREATE INDEX idx_tags_fts ON Tags_Popular USING fts
                 (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
             OPTIMIZE INDEX idx_tags_fts;",
        )
        .await?;

        // The whole import reached its end. Drop the progress table so the
        // next scheduled slurp starts clean: a warm destination re-imports
        // cheaply because every stage dedupes with INSERT OR IGNORE and
        // skips recounts, exactly like the pre-existing warm re-slurp path.
        conn.execute(format!("DROP TABLE IF EXISTS {SLURP_CHECKPOINT_TABLE}"), ())
            .await?;
        // The per-batch mapping tables belong to the run's progress state:
        // once every stage committed there is nothing left to resume.
        for table in [SLURP_TAG_MAP_TABLE, SLURP_FILE_MAP_TABLE] {
            conn.execute(format!("DROP TABLE IF EXISTS {table}"), ())
                .await?;
        }

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
        for chunk in relationships.chunks(SLURP_RELATIONSHIP_WRITE_BATCH) {
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

/// One persisted cursor row in [`SLURP_CHECKPOINT_TABLE`]: how far a stage's
/// keyset scan had committed to the destination, and whether the stage's
/// post-scan work (tail batches, recounts, index restores) had finished before
/// the run stopped.
#[derive(Default, Clone, Copy)]
struct SlurpStageCheckpoint {
    finished: bool,
    last_file_id: u64,
    last_tag_id: u64,
}

/// Loads every slurp cursor left by a previous (interrupted) run. A fresh
/// destination, or one whose last slurp fully completed, has none.
async fn slurp_checkpoint_load(conn: &Connection) -> Result<HashMap<String, SlurpStageCheckpoint>> {
    let mut out = HashMap::new();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT stage, finished, last_file_id, last_tag_id
             FROM {SLURP_CHECKPOINT_TABLE}
             WHERE stage != 'meta'"
        ))
        .await?;
    let mut rows = stmt.query(()).await?;
    while let Some(row) = rows.next().await? {
        out.insert(
            row.get(0)?,
            SlurpStageCheckpoint {
                finished: row.get::<i64>(1)? != 0,
                last_file_id: row.get::<i64>(2)? as u64,
                last_tag_id: row.get::<i64>(3)? as u64,
            },
        );
    }
    Ok(out)
}

/// Persists a stage cursor. Must be called only after the batch's writes have
/// fully committed: the next run ignores every source row the cursor already
/// covered, so the cursor must never advance past data that did not land.
/// (A partially imported batch that was not yet checkpointed is simply re-read
/// and deduplicated by the destination's INSERT OR IGNORE bulk-add helpers.)
async fn slurp_checkpoint_write(
    conn: &Connection,
    stage: &str,
    progress: SlurpStageCheckpoint,
) -> Result<()> {
    conn.execute(
        &format!(
            "INSERT INTO {SLURP_CHECKPOINT_TABLE}
                 (stage, finished, last_file_id, last_tag_id)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(stage) DO UPDATE SET
                 finished = excluded.finished,
                 last_file_id = excluded.last_file_id,
                 last_tag_id = excluded.last_tag_id"
        ),
        params_from_iter([
            Value::from(stage),
            Value::from(progress.finished as i64),
            Value::from(progress.last_file_id as i64),
            Value::from(progress.last_tag_id as i64),
        ]),
    )
    .await?;
    Ok(())
}

/// Builds a stable fingerprint of the slurp source so cursors from a previous
/// run are never applied to a different database. The path plus the file's
/// length and mtime are enough to detect a replaced source.
fn slurp_source_key(source: &Path) -> String {
    match std::fs::metadata(source) {
        Ok(meta) => format!(
            "{}:{}:{}",
            source.to_string_lossy(),
            meta.len(),
            meta.modified()
                .ok()
                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ),
        Err(_) => source.to_string_lossy().into_owned(),
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
mod tests;
