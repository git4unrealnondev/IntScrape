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

#[allow(dead_code)]
impl TursoDatabase {
    /// Gets a file row by id. `hash` is the file's SHA-512 digest from
    /// `FileHashes` (empty when the file has no recorded SHA-512).
    pub(in crate::db::turso) async fn file_get(
        &self,
        conn: &Connection,
        file_id: &u64,
    ) -> Result<FileInternal> {
        let mut rows = conn
            .query(
                "SELECT f.id, fh.digest, f.extension, f.storage_id, f.size_bytes
                 FROM File f
                 LEFT JOIN FileHashes fh
                   ON fh.file_id = f.id AND fh.algorithm = 'SHA512'
                 WHERE f.id = ?1 LIMIT 1",
                (*file_id as i64,),
            )
            .await?;

        let row = rows
            .next()
            .await?
            .ok_or(turso::Error::QueryReturnedNoRows)?;

        Ok(FileInternal {
            id: Some(row.get(0)?),
            hash: row.get::<Option<String>>(1)?.unwrap_or_default(),
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
                        "SELECT f.id, fh.digest, f.extension, f.storage_id, f.size_bytes
                         FROM File f
                         LEFT JOIN FileHashes fh
                           ON fh.file_id = f.id AND fh.algorithm = 'SHA512'
                         WHERE f.id IN ({placeholders});"
                    ),
                    params_from_iter(params),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                out.insert(
                    row.get::<u64>(0)?,
                    FileInternal {
                        id: Some(row.get(0)?),
                        hash: row.get::<Option<String>>(1)?.unwrap_or_default(),
                        extension: row.get(2)?,
                        storage_id: row.get(3)?,
                        size_bytes: row.get(4)?,
                    },
                );
            }
        }

        Ok(out)
    }

    /// Batched lookup of files by SHA-512 digest (from `FileHashes`),
    /// keyed by hash.
    ///
    /// Digests are resolved one point query per digest (prepared once),
    /// then the found ids go through `file_get_bulk`. A single
    /// `digest IN (...)` statement would be one indexed lookup per chunk
    /// in SQLite, but Turso's planner does not seek on
    /// `(algorithm, digest)` for an IN list — it scans every SHA-512 row
    /// (~70s per 4600-digest chunk at 1M rows, measured). Point queries
    /// on the unique index cost the same as the old `File.hash IN (...)`
    /// seeks (~3.7µs per digest, measured). See `lookup_sha512` in
    /// `file_add_bulk`, which has the same constraint.
    pub(in crate::db::turso) async fn files_get_by_hash(
        &self,
        conn: &Connection,
        hashes: &[String],
    ) -> Result<HashMap<String, FileInternal>> {
        let mut out = HashMap::new();
        if hashes.is_empty() {
            return Ok(out);
        }

        // digest -> file_id, one cached point query per digest. The query
        // uses the canonical (uppercase) form so a lowercase caller-supplied
        // digest still resolves the stored row; the result stays keyed by
        // the caller's original string.
        let mut by_digest: HashMap<String, u64> = HashMap::new();
        let mut stmt = conn
            .prepare_cached(
                "SELECT file_id FROM FileHashes WHERE algorithm = 'SHA512' AND digest = ?1",
            )
            .await?;
        for digest in hashes {
            if digest.is_empty() {
                continue;
            }
            let canonical = crate::db::canonical_digest("SHA512", digest);
            let mut rows = stmt.query((canonical.as_str(),)).await?;
            while let Some(row) = rows.next().await? {
                by_digest.insert(digest.clone(), row.get::<i64>(0)? as u64);
            }
        }

        if by_digest.is_empty() {
            return Ok(out);
        }

        let ids: Vec<u64> = by_digest.values().copied().collect();
        let files = self.file_get_bulk(conn, &ids).await?;
        for (digest, id) in by_digest {
            if let Some(file) = files.get(&id) {
                out.insert(digest, file.clone());
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
                "SELECT f.id, fh.digest, f.extension, f.storage_id, f.size_bytes
                 FROM File f
                 LEFT JOIN FileHashes fh
                   ON fh.file_id = f.id AND fh.algorithm = 'SHA512'
                 WHERE f.id > ?1 ORDER BY f.id LIMIT ?2;",
                (after_id as i64, limit as i64),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            out.push(FileInternal {
                id: Some(row.get(0)?),
                hash: row.get::<Option<String>>(1)?.unwrap_or_default(),
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

    /// Gets every recorded SHA512 digest in the database, in digest order.
    ///
    /// This is the bulk counterpart of [`Self::file_hashes_get`]: a single
    /// indexed scan instead of one query (and, over IPC, one round trip) per
    /// file. `idx_file_hashes_algorithm_digest` is on `(algorithm, digest)`,
    /// so `WHERE algorithm = 'SHA512' ORDER BY digest` is a pure index scan
    /// with no sort step, and the unique index guarantees no duplicate
    /// digests. Algorithm names are stored uppercase (`"SHA512"`), matching
    /// `HashesSupported` serialization.
    pub(in crate::db::turso) async fn file_hashes_get_all_sha512(
        &self,
        conn: &Connection,
    ) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut rows = conn
            .query(
                "SELECT digest FROM FileHashes WHERE algorithm = 'SHA512' ORDER BY digest;",
                (),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            out.push(row.get(0)?);
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
            if let Some(good_path) = file_on_disk(&file, base_path)
                && let Ok(final_path) = good_path.canonicalize()
            {
                return Ok(Some(final_path.to_string_lossy().to_string()));
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

    /// Updates the extension and storage id of a batch of files, and
    /// replaces their SHA-512 digest in `FileHashes` when `hash` is set.
    ///
    /// One statement per `SQL_CHUNK_SIZE` rows instead of one per file. As
    /// before, a file whose `id` is unset binds NULL and matches nothing.
    pub(in crate::db::turso) async fn file_update_batch(
        &self,
        conn: &Connection,
        files: &[FileInternal],
    ) -> Result<()> {
        const COLUMNS: [&str; 2] = ["extension", "storage_id"];

        for chunk in files.chunks(SQL_CHUNK_SIZE) {
            let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 3);
            for file in chunk {
                params.push(Value::from(file.id.map(|id| id as i64)));
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

            // Replace the SHA-512 digest: delete the old row and bulk-insert
            // the new one (`file_hashes_add_bulk` is INSERT OR IGNORE, which
            // would keep the old digest on the primary key).
            let with_id: Vec<&FileInternal> =
                chunk.iter().filter(|file| file.id.is_some()).collect();
            if !with_id.is_empty() {
                let placeholders = std::iter::repeat_n("?", with_id.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let params: Vec<Value> = with_id
                    .iter()
                    .map(|file| Value::from(file.id.unwrap() as i64))
                    .collect();
                conn.execute(
                    format!(
                        "DELETE FROM FileHashes WHERE algorithm = 'SHA512'
                         AND file_id IN ({placeholders});"
                    ),
                    params_from_iter(params),
                )
                .await?;

                let tuples: Vec<(u64, &str, &str)> = with_id
                    .iter()
                    .filter(|file| !file.hash.is_empty())
                    .map(|file| (file.id.unwrap(), "SHA512", file.hash.as_str()))
                    .collect();
                if !tuples.is_empty() {
                    self.file_hashes_add_bulk(conn, &tuples).await?;
                }
            }
        }

        Ok(())
    }

    /// Adds a batch of `(algorithm, digest, file_id)` rows, ignoring dups.
    ///
    /// Digests are canonicalized per algorithm (see
    /// [`crate::db::canonical_digest`]) before storage, so a lowercase
    /// SHA-512 from a slurped library or a plugin can never become the
    /// stored digest that every read path now returns.
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
                params.push(Value::from(crate::db::canonical_digest(algorithm, digest)));
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

        // One cached point query per (algorithm, digest), not a tuple-IN
        // statement: Turso's planner does not seek on `(algorithm, digest)`
        // for a tuple IN list — it scans (measured >60s per 4600-pair
        // chunk at 1M rows). Point queries use the unique index directly
        // (~3.7µs each, measured). See `lookup_sha512` in `file_add_bulk`.
        let mut stmt = conn
            .prepare_cached("SELECT file_id FROM FileHashes WHERE algorithm = ?1 AND digest = ?2")
            .await?;
        for chunk in hashes.chunks(SQL_CHUNK_SIZE) {
            // Drain the chunk's matches first, then fetch every matched file in
            // one batched pass. The old loop called `file_get` per matched row,
            // so a chunk resolving 4.6k hashes issued 4.6k extra point queries
            // against `File` on top of the batched `FileHashes` lookup — and
            // `hashes_files_get_sync` is the per-job dedupe lookup, so that
            // landed on the scrape hot path.
            let mut matched: Vec<(String, String, u64)> = Vec::with_capacity(chunk.len());
            for (algorithm, digest) in chunk {
                // Query the canonical digest form (see `file_hashes_add_bulk`);
                // the result stays keyed by the caller's original pair.
                let canonical = crate::db::canonical_digest(algorithm, digest);
                let mut rows = stmt.query((algorithm.as_str(), canonical.as_str())).await?;
                while let Some(row) = rows.next().await? {
                    matched.push((algorithm.clone(), digest.clone(), row.get::<i64>(0)? as u64));
                }
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
mod tests;
