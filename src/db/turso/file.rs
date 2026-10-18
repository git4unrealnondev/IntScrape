use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use shared_types::FileInternal;
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::{SQL_CHUNK_SIZE, turso::TursoDatabase};

/// Builds a batched `SET` clause of the form
/// `col = CASE id WHEN ?k THEN ?v ... ELSE col END, ...`.
///
/// Limbo accepts a flat `CASE` arm list at the full `SQL_CHUNK_SIZE` (verified
/// against a real database in `batched_update_matches_per_row_semantics`), so
/// this collapses one statement per row into one statement per chunk. Its
/// correlated-subquery alternative (`UPDATE ... SET x = (SELECT ...)`) does not
/// parse in limbo at all — `Parse error: no such table: file` — so the CASE
/// form is the only batched option available.
///
/// Assumes the caller bound each row as `(id, col0, col1, ...)` — the id gets
/// its own slot so it cannot collide with a single-column row's value, which is
/// what made an earlier version of this write the size into the `WHEN` arm.
fn case_set_clause(columns: &[&str], rows: usize) -> String {
    let per_row = columns.len() + 1;
    let mut out = String::new();
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(column);
        out.push_str(" = CASE id");
        for row in 0..rows {
            let base = row * per_row + 1;
            out.push_str(&format!(" WHEN ?{base} THEN ?{}", base + 1 + index));
        }
        // The `WHERE ... id IN (...)` guard already restricts the statement to
        // the rows being addressed, so `ELSE` never fires for a matched id. It
        // exists only so the expression has a value for any row that slips
        // through.
        out.push_str(&format!(" ELSE {column} END"));
    }
    out
}

/// Builds an `id IN (...)` list of `rows` placeholders, numbered from
/// `first_bind`.
///
/// The ids are bound a second time rather than reusing the `CASE` arms'
/// placeholders: turso binds parameters positionally instead of resolving `?N`
/// references, so a placeholder used twice raises
/// `Misuse("bind index N is out of bounds")`.
fn id_in_clause(first_bind: usize, rows: usize) -> String {
    (first_bind..first_bind + rows)
        .map(|bind| format!("?{bind}"))
        .collect::<Vec<_>>()
        .join(", ")
}

impl TursoDatabase {
    /// Gets a file row by id.
    pub(in crate::db::turso) async fn file_get(
        &self,
        conn: &Connection,
        file_id: &u64,
    ) -> Result<FileInternal> {
        let mut rows = conn
            .query(
                "SELECT id, hash, extension, storage_id, size_bytes FROM File WHERE id = ?1 LIMIT 1",
                (*file_id as i64,),
            )
            .await?;

        let row = rows
            .next()
            .await?
            .ok_or(turso::Error::QueryReturnedNoRows)?;

        Ok(FileInternal {
            id: Some(row.get(0)?),
            hash: row.get(1)?,
            extension: row.get(2)?,
            storage_id: row.get(3)?,
            size_bytes: row.get(4)?,
        })
    }

    /// Gets many file rows by id with batched `IN` lookups.
    ///
    /// Callers that already know the full set of ids they care about (bulk hash
    /// resolution, dedupe passes) use this instead of looping `file_get`, which
    /// costs one statement per id. Ids are deduped so a repeated id costs one
    /// `IN` slot, and missing ids are simply absent from the result.
    pub(in crate::db::turso) async fn file_get_bulk(
        &self,
        conn: &Connection,
        file_ids: &[u64],
    ) -> Result<HashMap<u64, FileInternal>> {
        let mut out = HashMap::with_capacity(file_ids.len());

        let mut ids: Vec<u64> = file_ids.to_vec();
        ids.sort_unstable();
        ids.dedup();

        for chunk in ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let mut params: Vec<Value> = Vec::with_capacity(chunk.len());
            for id in chunk {
                params.push(Value::from(*id as i64));
            }
            let mut rows = conn
                .query(
                    format!(
                        "SELECT id, hash, extension, storage_id, size_bytes
                         FROM File WHERE id IN ({placeholders});"
                    ),
                    params_from_iter(params),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                out.insert(
                    row.get::<u64>(0)?,
                    FileInternal {
                        id: Some(row.get(0)?),
                        hash: row.get(1)?,
                        extension: row.get(2)?,
                        storage_id: row.get(3)?,
                        size_bytes: row.get(4)?,
                    },
                );
            }
        }

        Ok(out)
    }

    /// Gets up to `limit` file rows with `id` strictly greater than `after_id`,
    /// in ascending id order.
    ///
    /// Keyset pagination rather than `LIMIT`/`OFFSET`: a deep walk stays an
    /// indexed range scan at any depth, and the caller can bound its own memory
    /// by holding one page at a time instead of the whole table. Returns fewer
    /// than `limit` rows only at the end of the table.
    pub(in crate::db::turso) async fn file_get_page(
        &self,
        conn: &Connection,
        after_id: u64,
        limit: usize,
    ) -> Result<Vec<FileInternal>> {
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        let mut rows = conn
            .query(
                "SELECT id, hash, extension, storage_id, size_bytes
                 FROM File WHERE id > ?1 ORDER BY id LIMIT ?2;",
                (after_id as i64, limit as i64),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            out.push(FileInternal {
                id: Some(row.get(0)?),
                hash: row.get(1)?,
                extension: row.get(2)?,
                storage_id: row.get(3)?,
                size_bytes: row.get(4)?,
            });
        }

        Ok(out)
    }

    /// Gets all recorded file storage locations: id -> base path.
    pub(in crate::db::turso) async fn file_storage_get_all(
        &self,
        conn: &Connection,
    ) -> Result<HashMap<u64, String>> {
        let mut out = HashMap::new();
        let mut rows = conn
            .query("SELECT id, location FROM FileStorageLocations;", ())
            .await?;

        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?, row.get(1)?);
        }

        Ok(out)
    }

    /// Gets all recorded hashes for a file: algorithm -> digest.
    pub(in crate::db::turso) async fn file_hashes_get(
        &self,
        conn: &Connection,
        file_id: u64,
    ) -> Result<HashMap<String, String>> {
        let mut out = HashMap::new();
        let mut rows = conn
            .query(
                "SELECT algorithm, digest FROM FileHashes WHERE file_id = ?1;",
                (file_id as i64,),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?, row.get(1)?);
        }

        Ok(out)
    }

    /// Gets every file id in the database.
    pub(in crate::db::turso) async fn file_id_get_all(
        &self,
        conn: &Connection,
    ) -> Result<HashSet<u64>> {
        let mut out = HashSet::new();
        let mut rows = conn.query("SELECT id FROM File;", ()).await?;

        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets all `file_ids` that carry a tag whose namespace has `namespace_id`.
    pub(in crate::db::turso) async fn file_id_get_namespace_id(
        &self,
        conn: &Connection,
        namespace_id: u64,
    ) -> Result<HashSet<u64>> {
        let relationship_source = self
            .relationship_union_source(
                conn,
                "Relationship",
                Some("tag_id IN (SELECT id FROM Tags WHERE namespace = ?1)"),
            )
            .await?;
        let sql = format!("SELECT DISTINCT file_id FROM {relationship_source}");

        let mut out = HashSet::new();
        let mut rows = conn.query(&sql, (namespace_id as i64,)).await?;
        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets the canonical path on disk for a file, if it can be located.
    pub(in crate::db::turso) async fn file_get_physical_path(
        &self,
        conn: &Connection,
        file_id: u64,
    ) -> Result<Option<String>> {
        let file = self.file_get(conn, &file_id).await?;
        let file_storage_map = self.file_storage_get_all(conn).await?;

        for base_path in file_storage_map.values() {
            if let Some(good_path) = file_on_disk(&file, base_path) {
                if let Ok(final_path) = good_path.canonicalize() {
                    return Ok(Some(final_path.to_string_lossy().to_string()));
                }
            }
        }

        Ok(None)
    }

    /// Gets the id of a storage location, if recorded.
    pub(in crate::db::turso) async fn file_storage_location_get(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<Option<u64>> {
        let mut rows = conn
            .query(
                "SELECT id FROM FileStorageLocations WHERE location = ?1 LIMIT 1;",
                (name,),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    /// Adds a file storage location row.
    pub(in crate::db::turso) async fn file_storage_location_set(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<()> {
        conn.execute(
            "INSERT OR IGNORE INTO FileStorageLocations (location) VALUES (?1);",
            (name,),
        )
        .await?;

        Ok(())
    }

    /// Gets the id of a storage location, creating the row when missing.
    pub(in crate::db::turso) async fn file_storage_location_get_or_create(
        &self,
        conn: &Connection,
        location_path: &str,
    ) -> Result<u64> {
        if let Some(path_id) = self.file_storage_location_get_cache(location_path).await {
            return Ok(path_id);
        }

        if let Some(path_id) = self.file_storage_location_get(conn, location_path).await? {
            self.file_storage_location_set_cache(location_path, path_id)
                .await;
            return Ok(path_id);
        }

        self.file_storage_location_set(conn, location_path).await?;

        // Another writer may have created the row between our SELECT and our
        // INSERT, in which case `last_insert_rowid` would be stale. Re-select
        // the authoritative id instead (and cache it).
        let path_id = self
            .file_storage_location_get(conn, location_path)
            .await?
            .expect("row exists after INSERT OR IGNORE");
        self.file_storage_location_set_cache(location_path, path_id)
            .await;

        Ok(path_id)
    }

    /// Gets the location where files should be stored, creating the default
    /// setting and storage row when missing.
    pub(in crate::db::turso) async fn file_download_location_get(
        &self,
        conn: &Connection,
    ) -> Result<(std::path::PathBuf, u64)> {
        let target_location =
            if let Some(setting) = self.setting_get_sql(conn, "SYSTEM_file_location").await? {
                match setting.param {
                    Some(param) if !param.is_empty() => param,
                    _ => "files".to_string(),
                }
            } else {
                self.setting_set(
                    conn,
                    shared_types::DbSettingsObj {
                        name: "SYSTEM_file_location".into(),
                        description: Some("Where to download files to".into()),
                        num: None,
                        param: Some("files".into()),
                    },
                )
                .await?;
                "files".to_string()
            };

        let path_id = self
            .file_storage_location_get_or_create(conn, &target_location)
            .await?;

        Ok((std::path::PathBuf::from(target_location), path_id))
    }

    /// Updates the hash, extension, and storage id of a batch of files.
    ///
    /// One statement per `SQL_CHUNK_SIZE` rows instead of one per file. As
    /// before, a file whose `id` is unset binds NULL and matches nothing.
    pub(in crate::db::turso) async fn file_update_batch(
        &self,
        conn: &Connection,
        files: &[FileInternal],
    ) -> Result<()> {
        const COLUMNS: [&str; 3] = ["hash", "extension", "storage_id"];

        for chunk in files.chunks(SQL_CHUNK_SIZE) {
            let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 4);
            for file in chunk {
                params.push(Value::from(file.id.map(|id| id as i64)));
                params.push(Value::from(file.hash.as_str()));
                params.push(Value::from(file.extension.as_str()));
                params.push(Value::from(file.storage_id as i64));
            }
            let set = case_set_clause(&COLUMNS, chunk.len());
            let in_list = id_in_clause(params.len() + 1, chunk.len());
            for file in chunk {
                params.push(Value::from(file.id.map(|id| id as i64)));
            }
            conn.execute(
                format!("UPDATE File SET {set} WHERE id IN ({in_list});"),
                params_from_iter(params),
            )
            .await?;
        }

        Ok(())
    }

    /// Adds a batch of `(algorithm, digest, file_id)` rows, ignoring dups.
    pub(in crate::db::turso) async fn file_hashes_add_bulk(
        &self,
        conn: &Connection,
        hashes: &[(u64, &str, &str)],
    ) -> Result<()> {
        for chunk in hashes.chunks(SQL_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let mut holders = Vec::with_capacity(chunk.len());
            let mut params = Vec::with_capacity(chunk.len() * 3);
            for (file_id, algorithm, digest) in chunk {
                holders.push("(?, ?, ?)");
                params.push(Value::from(*file_id as i64));
                params.push(Value::from(*algorithm));
                params.push(Value::from(*digest));
            }
            let sql = format!(
                "INSERT OR IGNORE INTO FileHashes (file_id, algorithm, digest) VALUES {};",
                holders.join(", ")
            );
            conn.execute(sql, params_from_iter(params)).await?;
        }

        Ok(())
    }

    /// Batched lookup of every hash against `FileHashes` and the files that
    /// own them, keyed by `(algorithm, digest)`.
    pub(in crate::db::turso) async fn hashes_files_get(
        &self,
        conn: &Connection,
        hashes: &[(String, String)],
    ) -> Result<HashMap<(String, String), FileInternal>> {
        let mut out = HashMap::new();
        if hashes.is_empty() {
            return Ok(out);
        }

        for chunk in hashes.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("(?, ?)", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT file_id, algorithm, digest FROM FileHashes WHERE (algorithm, digest) IN ({placeholders});"
            );
            let mut params = Vec::with_capacity(chunk.len() * 2);
            for (algorithm, digest) in chunk {
                params.push(Value::from(algorithm.as_str()));
                params.push(Value::from(digest.as_str()));
            }

            // Drain the chunk's matches first, then fetch every matched file in
            // one batched pass. The old loop called `file_get` per matched row,
            // so a chunk resolving 4.6k hashes issued 4.6k extra point queries
            // against `File` on top of the batched `FileHashes` lookup — and
            // `hashes_files_get_sync` is the per-job dedupe lookup, so that
            // landed on the scrape hot path.
            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            let mut matched: Vec<(String, String, u64)> = Vec::with_capacity(chunk.len());
            while let Some(row) = rows.next().await? {
                let file_id: u64 = row.get(0)?;
                let algorithm: String = row.get(1)?;
                let digest: String = row.get(2)?;
                matched.push((algorithm, digest, file_id));
            }

            if matched.is_empty() {
                continue;
            }

            let ids: Vec<u64> = matched.iter().map(|(_, _, file_id)| *file_id).collect();
            let files = self.file_get_bulk(conn, &ids).await?;
            for (algorithm, digest, file_id) in matched {
                // A match row whose file has since been deleted resolves to
                // nothing, exactly as the previous `if let Ok(file)` did.
                if let Some(file) = files.get(&file_id) {
                    out.insert((algorithm, digest), file.clone());
                }
            }
        }

        Ok(out)
    }

    /// Gets the `file_ids` whose `size_bytes` is null.
    pub(in crate::db::turso) async fn file_ids_get_size_bytes_null(
        &self,
        conn: &Connection,
    ) -> Result<HashSet<u64>> {
        let mut out = HashSet::new();
        let mut rows = conn
            .query("SELECT id FROM File WHERE size_bytes IS NULL;", ())
            .await?;

        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Sets `size_bytes` for a batch of file ids, only filling null values.
    ///
    /// One statement per `SQL_CHUNK_SIZE` rows instead of one per file.
    /// `update_missing_file_sizes` feeds this up to a full chunk per batch, so
    /// the previous per-row form meant 4600 individual autocommitted
    /// statements per batch — and it ran with no enclosing transaction, so
    /// every one of those was its own commit under MVCC.
    pub(in crate::db::turso) async fn file_sizes_update(
        &self,
        conn: &Connection,
        updates: &[(u64, u64)],
    ) -> Result<()> {
        const COLUMNS: [&str; 1] = ["size_bytes"];

        for chunk in updates.chunks(SQL_CHUNK_SIZE) {
            let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
            for (file_id, size) in chunk {
                params.push(Value::from(*file_id as i64));
                params.push(Value::from(*size as i64));
            }
            let set = case_set_clause(&COLUMNS, chunk.len());
            let in_list = id_in_clause(params.len() + 1, chunk.len());
            for (file_id, _) in chunk {
                params.push(Value::from(*file_id as i64));
            }
            conn.execute(
                format!("UPDATE File SET {set} WHERE size_bytes IS NULL AND id IN ({in_list});"),
                params_from_iter(params),
            )
            .await?;
        }

        Ok(())
    }
}

/// Mirrors `MainDatabase::get_file_location`: resolves the hash-partitioned
/// path under a storage base, repairing a missing extension on disk when the
/// file is found without one.
pub(in crate::db::turso) fn file_on_disk(
    file_internal: &FileInternal,
    base_path: &String,
) -> Option<PathBuf> {
    if file_internal.hash.len() <= 6 {
        return None;
    }
    let mut path = Path::new(base_path).to_path_buf();
    path.push(&file_internal.hash[0..2]);
    path.push(&file_internal.hash[2..4]);
    path.push(&file_internal.hash[4..6]);
    path.push(&file_internal.hash);
    let final_path = path.with_added_extension(&file_internal.extension);

    if final_path.exists() {
        return Some(final_path);
    }
    if final_path.with_extension("").exists() {
        std::fs::rename(final_path.with_extension(""), &final_path).ok()?;
        return Some(final_path);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    async fn new_test_db() -> Arc<TursoDatabase> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_storage_location_get_or_create_is_cached() {
        let db = new_test_db().await;
        let conn = db.connect().unwrap();

        let first = db
            .file_storage_location_get_or_create(&conn, "cache_me")
            .await
            .unwrap();
        let second = db
            .file_storage_location_get_or_create(&conn, "cache_me")
            .await
            .unwrap();
        assert_eq!(first, second, "repeated lookups must reuse the cached id");
        assert_eq!(
            db.file_storage_location_get_cache("cache_me").await,
            Some(first)
        );

        // Inserting a second row and reloading repopulates the cache from the
        // committed state without dropping the existing entry.
        db.file_storage_location_set(&conn, "cache_me_other")
            .await
            .unwrap();
        db.file_storage_location_cache_reload().await.unwrap();
        assert_eq!(
            db.file_storage_location_get_cache("cache_me").await,
            Some(first)
        );
        assert!(
            db.file_storage_location_get_cache("cache_me_other")
                .await
                .is_some()
        );
    }

    /// Keeps the temp dir alive for the whole test so the DB file persists.
    async fn new_test_db_owned() -> (Arc<TursoDatabase>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        (db, temp_dir)
    }

    fn test_file(hash: &str, size: Option<u64>) -> FileInternal {
        FileInternal {
            id: None,
            hash: hash.to_string(),
            extension: "jpg".to_string(),
            storage_id: 1,
            size_bytes: size,
        }
    }

    fn sorted_ids(files: &HashSet<FileInternal>) -> Vec<u64> {
        let mut ids: Vec<u64> = files.iter().flat_map(|file| file.id).collect();
        ids.sort_unstable();
        ids
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_add_bulk_inserts_new_files_and_resolves_existing() {
        let (db, _temp_dir) = new_test_db_owned().await;
        let conn = db.connect().unwrap();

        let resolved = db
            .file_add_bulk(
                &conn,
                &[test_file("aaa", Some(100)), test_file("bbb", None)],
            )
            .await
            .unwrap();
        assert_eq!(resolved.len(), 2, "both files must resolve an id");
        let first_ids = sorted_ids(&resolved);
        assert_eq!(first_ids.len(), 2);
        assert_ne!(first_ids[0], first_ids[1], "new files get distinct ids");

        // Re-adding the same hashes resolves the same ids without inserting
        // any rows (no upsert, no duplicate entries).
        let re_resolved = db
            .file_add_bulk(
                &conn,
                &[test_file("aaa", Some(999)), test_file("bbb", Some(999))],
            )
            .await
            .unwrap();
        assert_eq!(
            sorted_ids(&re_resolved),
            first_ids,
            "existing hashes must resolve to the same ids"
        );

        let count: i64 = conn
            .query("SELECT COUNT(*) FROM File;", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(count, 2, "re-adding existing hashes must not write rows");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_add_bulk_does_not_refresh_existing_metadata() {
        let (db, _temp_dir) = new_test_db_owned().await;
        let conn = db.connect().unwrap();

        let original = db
            .file_add_bulk(&conn, &[test_file("aaa", Some(100))])
            .await
            .unwrap();
        let original = original.iter().next().unwrap();
        let id = original.id.unwrap();

        // A re-scrape with different metadata must leave the stored row
        // untouched: INSERT OR IGNORE skips the conflicting hash with no write
        // (the previous ON CONFLICT ... DO UPDATE rewrote these columns).
        let re_scraped = db
            .file_add_bulk(&conn, &[test_file("aaa", Some(999))])
            .await
            .unwrap();
        let re_scraped = re_scraped.iter().next().unwrap();
        assert_eq!(re_scraped.id.unwrap(), id, "same hash -> same id");

        let stored = db.file_get(&conn, &id).await.unwrap();
        assert_eq!(
            stored.size_bytes,
            Some(100),
            "existing rows must not be rewritten on re-scrape"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_add_bulk_dedupes_duplicate_hashes_in_one_chunk() {
        let (db, _temp_dir) = new_test_db_owned().await;
        let conn = db.connect().unwrap();

        // Two entries with the same hash in one chunk produce one id, one
        // stored row — matching the single-entry-per-hash RETURNING shape the
        // old upsert handed back.
        let resolved = db
            .file_add_bulk(
                &conn,
                &[test_file("aaa", Some(100)), test_file("aaa", Some(200))],
            )
            .await
            .unwrap();
        assert_eq!(resolved.len(), 1, "one output entry per hash");

        let count: i64 = conn
            .query("SELECT COUNT(*) FROM File;", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(count, 1, "duplicate hashes must insert one row");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batched_update_matches_per_row_semantics() {
        // Pins the two things that made the batched CASE form viable: limbo
        // accepts a flat arm list at the full SQL_CHUNK_SIZE, and a chunked
        // statement updates exactly the rows the old per-row loop did. If a
        // future turso release reintroduces an expression-depth cap (the one
        // that killed the old OR-term delete in `relationship_bulk_delete`) or
        // changes NULL/bound-id handling, this fails loudly instead of silently
        // updating zero rows in the background size-backfill job.
        let (db, _temp_dir) = new_test_db_owned().await;
        let conn = db.connect().unwrap();

        let files: Vec<FileInternal> = (0..SQL_CHUNK_SIZE)
            .map(|i| test_file(&format!("h{i}"), None))
            .collect();
        let inserted = db.file_add_bulk(&conn, &files).await.unwrap();
        let mut ids: Vec<u64> = inserted.iter().filter_map(|f| f.id).collect();
        ids.sort_unstable();
        assert_eq!(ids.len(), SQL_CHUNK_SIZE);

        let updates: Vec<(u64, u64)> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, (i + 1) as u64))
            .collect();
        db.file_sizes_update(&conn, &updates).await.unwrap();

        let mut filled: Vec<(u64, Option<u64>)> = Vec::with_capacity(ids.len());
        let mut rows = conn
            .query("SELECT id, size_bytes FROM File ORDER BY id;", ())
            .await
            .unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            filled.push((row.get(0).unwrap(), row.get(1).unwrap()));
        }
        assert_eq!(filled.len(), SQL_CHUNK_SIZE, "every row must be updated");
        for ((expected_id, expected_size), (id, size)) in updates.iter().zip(&filled) {
            assert_eq!(id, expected_id, "ids must line up");
            assert_eq!(
                *size,
                Some(*expected_size),
                "a full chunk must update in one statement"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_sizes_update_leaves_existing_sizes_alone() {
        let (db, _temp_dir) = new_test_db_owned().await;
        let conn = db.connect().unwrap();

        let inserted = db
            .file_add_bulk(&conn, &[test_file("aaa", Some(100))])
            .await
            .unwrap();
        let id = inserted.iter().next().unwrap().id.unwrap();

        // The guard is `size_bytes IS NULL`, so a second pass over a file that
        // already has a size must not overwrite it.
        db.file_sizes_update(&conn, &[(id, 999)]).await.unwrap();
        assert_eq!(db.file_get(&conn, &id).await.unwrap().size_bytes, Some(100));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_update_batch_writes_every_column() {
        let (db, _temp_dir) = new_test_db_owned().await;
        let conn = db.connect().unwrap();

        let inserted = db
            .file_add_bulk(&conn, &[test_file("aaa", None), test_file("bbb", None)])
            .await
            .unwrap();
        let mut ids: Vec<u64> = inserted.iter().filter_map(|f| f.id).collect();
        ids.sort_unstable();

        let mut updates = Vec::new();
        for (index, id) in ids.iter().enumerate() {
            let mut file = test_file(&format!("renamed{index}"), None);
            file.id = Some(*id);
            file.extension = format!("png{index}");
            file.storage_id = 7;
            updates.push(file);
        }
        db.file_update_batch(&conn, &updates).await.unwrap();

        for (index, id) in ids.iter().enumerate() {
            let stored = db.file_get(&conn, id).await.unwrap();
            assert_eq!(stored.hash, format!("renamed{index}"));
            assert_eq!(stored.extension, format!("png{index}"));
            assert_eq!(stored.storage_id, 7);
        }

        // A file with no id binds NULL and must match nothing.
        let mut unset = test_file("ignored", None);
        unset.id = None;
        db.file_update_batch(&conn, &[unset]).await.unwrap();
        assert_eq!(
            conn.query("SELECT COUNT(*) FROM File WHERE hash = 'ignored';", ())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<i64>(0)
                .unwrap(),
            0,
            "an unset id must not match a row"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_add_bulk_honors_pinned_id() {
        let (db, _temp_dir) = new_test_db_owned().await;
        let conn = db.connect().unwrap();

        let mut pinned = test_file("aaa", None);
        pinned.id = Some(777);

        let resolved = db.file_add_bulk(&conn, &[pinned]).await.unwrap();
        assert_eq!(resolved.iter().next().unwrap().id, Some(777));
        let stored = db.file_get(&conn, &777).await.unwrap();
        assert_eq!(stored.hash, "aaa");
    }
}
