use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use shared_types::FileInternal;
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::{SQL_CHUNK_SIZE, turso::TursoDatabase};

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
        let relationship_source = self.relationship_union_source(conn, "Relationship").await?;
        let sql = format!(
            "SELECT DISTINCT file_id FROM {relationship_source}
             WHERE tag_id IN (
                 SELECT id FROM Tags WHERE namespace = ?1
             );"
        );

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
    pub(in crate::db::turso) async fn file_update_batch(
        &self,
        conn: &Connection,
        files: &[FileInternal],
    ) -> Result<()> {
        for file in files {
            conn.execute(
                "UPDATE File
                 SET hash = ?1, extension = ?2, storage_id = ?3
                 WHERE id = ?4;",
                (
                    file.hash.as_str(),
                    file.extension.as_str(),
                    file.storage_id as i64,
                    file.id.map(|id| id as i64),
                ),
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

            let mut rows = conn.query(sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let file_id: u64 = row.get(0)?;
                let algorithm: String = row.get(1)?;
                let digest: String = row.get(2)?;
                if let Ok(file) = self.file_get(conn, &file_id).await {
                    out.insert((algorithm, digest), file);
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
    pub(in crate::db::turso) async fn file_sizes_update(
        &self,
        conn: &Connection,
        updates: &[(u64, u64)],
    ) -> Result<()> {
        for (file_id, size) in updates {
            conn.execute(
                "UPDATE File SET size_bytes = ?1 WHERE id = ?2 AND size_bytes IS NULL;",
                (*size as i64, *file_id as i64),
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
}
