//! Database operations for the `file` domain.

use super::{MainDatabase, SQL_CHUNK_SIZE};
use crate::cli::cli_structs::CheckFilesEnum;
use log::info;
use rayon::prelude::*;
use rusqlite::{Connection, Transaction, params};
use shared_types::{FileInternal, HashesSupported};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// The file-status and hash-key helpers now live on the active `turso` backend;
// the legacy layer re-exports them so its processing code and tests share one
// implementation.
pub use crate::db::{SourceUrlFileStatus, hashessupportedtoinner, hashessupportedtokey};

fn process_storage_check_chunk(
    paths: &[PathBuf],
    file_hash: &HashMap<String, FileInternal>,
    file_storage_map: &HashMap<u64, String>,
    default_file_location: &(PathBuf, u64),
) {
    // Reading and hashing are independent for each misplaced file. Keep the
    // filesystem mutations below sequential, but use all available CPU cores
    // for the expensive part of the storage check.
    let misplaced_files: Vec<_> = paths
        .par_iter()
        .filter_map(|path| {
            let hash = crate::web::manager::hash_file_sha512(path).ok()?;
            Some((path, hash))
        })
        .collect();

    misplaced_files.par_iter().for_each(|(path, hash)| {
        if let Some(file_internal) = file_hash.get(hash.as_str())
            && let Some(base_file_path) = file_storage_map.get(&file_internal.storage_id)
        {
            let mut path_buf = Path::new(base_file_path).to_path_buf();
            path_buf.push(&hash[0..2]);
            path_buf.push(&hash[2..4]);
            path_buf.push(&hash[4..6]);
            path_buf.push(&hash);

            let target_path = path_buf.with_extension(&file_internal.extension);
            if path.exists()
                && !target_path.exists()
                && std::fs::create_dir_all(target_path.parent().unwrap()).is_ok()
                && std::fs::copy(path, &target_path).is_ok()
                && std::fs::remove_file(path).is_ok()
            {
                info!(
                    "Moved file: {} to: {}",
                    path.display(),
                    target_path.display()
                );
            }
        } else {
            let mut dumpster = default_file_location.0.with_file_name("dumpster");

            info!("File {} does not exist in db.", path.display());

            dumpster.push(path);
            if std::fs::create_dir_all(dumpster.parent().unwrap()).is_ok()
                && std::fs::copy(path, &dumpster).is_ok()
                && std::fs::remove_file(path).is_ok()
            {
                info!("Moved file: {} to: {}", path.display(), dumpster.display());
            }
        }
    });
}

fn process_storage_check_filename_chunk(
    paths: &[PathBuf],
    file_hash: &HashMap<String, FileInternal>,
    file_storage_map: &HashMap<u64, String>,
    default_file_location: &(PathBuf, u64),
) {
    paths.par_iter().for_each(|path| {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return;
        };
        let Some(file_internal) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| file_hash.get(stem))
        else {
            move_to_storage_dumpster(path, default_file_location);
            return;
        };
        let Some(base_file_path) = file_storage_map.get(&file_internal.storage_id) else {
            return;
        };

        let target_path = Path::new(base_file_path)
            .join(&file_internal.hash[0..2])
            .join(&file_internal.hash[2..4])
            .join(&file_internal.hash[4..6])
            .join(file_name);
        move_to_expected_path(path, &target_path);
    });
}

fn move_to_expected_path(path: &Path, target_path: &Path) {
    if path == target_path || !path.exists() || target_path.exists() {
        return;
    }
    if std::fs::create_dir_all(target_path.parent().unwrap()).is_ok()
        && std::fs::copy(path, target_path).is_ok()
        && std::fs::remove_file(path).is_ok()
    {
        info!(
            "Moved file: {} to: {}",
            path.display(),
            target_path.display()
        );
    }
}

fn move_to_storage_dumpster(path: &Path, default_file_location: &(PathBuf, u64)) {
    let Some(file_name) = path.file_name() else {
        return;
    };
    move_to_expected_path(
        path,
        &default_file_location
            .0
            .with_file_name("dumpster")
            .join(file_name),
    );
}

impl MainDatabase {
    ///
    /// Used internally to get a file location
    ///
    pub fn internal_file_storage_location_get(
        conn: &Connection,
        name: &str,
    ) -> Result<Option<u64>, rusqlite::Error> {
        let mut stmt =
            conn.prepare("SELECT id FROM FileStorageLocations WHERE location = ? LIMIT 1")?;
        let mut rows = stmt.query([name])?;
        if let Some(row) = rows.next()? {
            let obj = serde_rusqlite::from_row::<u64>(row)
                .map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    /// Retrieves the ID of a storage location.
    /// If the location does not exist in the database, it automatically creates it.
    pub fn internal_file_storage_location_get_or_create(
        &self,
        conn: &Connection,
        location_path: &str,
    ) -> Result<u64, rusqlite::Error> {
        if let Some(path_id) = Self::internal_file_storage_location_get(conn, location_path)? {
            return Ok(path_id);
        }

        Self::internal_file_storage_location_set(conn, location_path)?;

        let path_id = conn.last_insert_rowid() as u64;

        Ok(path_id)
    }

    ///
    /// Adds a file storage location
    ///
    pub fn internal_file_storage_location_set(
        conn: &Connection,
        name: &str,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        let mut stmt = conn.prepare("INSERT INTO FileStorageLocations (location) VALUES (?1)")?;

        stmt.execute(params![name])?;

        Ok(())
    }

    ///
    /// Updates a list of files
    ///
    pub fn internal_file_update_batch(
        &self,
        tn: Transaction,
        files: &[FileInternal],
    ) -> Result<(), rusqlite::Error> {
        Self::internal_audit_context_set(&tn, "file metadata updated")?;
        {
            let mut stmt = tn.prepare(
                "UPDATE File 
             SET hash = ?1, extension = ?2, storage_id = ?3 
             WHERE id = ?4",
            )?;

            for file in files {
                stmt.execute((&file.hash, &file.extension, &file.storage_id, &file.id))?;
            }
        }

        tn.commit()
    }

    ///
    /// Gets the physical path for a file
    ///
    pub fn internal_file_get_physical_path(
        conn: &Connection,
        file_id: &u64,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let file = Self::internal_file_id_get(conn, file_id)?;

        let file_storage_map = Self::internal_file_storage_get_all(conn)?;

        for (_, base_path) in file_storage_map {
            if let Some(good_path) = Self::get_file_location(&file, &base_path) {
                let final_path = good_path.canonicalize()?;
                return Ok(Some(final_path.to_string_lossy().to_string()));
            }
        }

        // File not found in any of the physical directories
        Ok(None)
    }

    ///
    /// Gets a file if its id exists in db
    ///
    pub fn internal_file_id_get(
        conn: &Connection,
        file_id: &u64,
    ) -> Result<FileInternal, rusqlite::Error> {
        conn.query_row(
            "SELECT id, hash, extension, storage_id FROM File WHERE id = ?1 LIMIT 1",
            [file_id],
            |row| {
                serde_rusqlite::from_row::<FileInternal>(row)
                    .map_err(|_| rusqlite::Error::ExecuteReturnedResults)
            },
        )
    }

    ///
    /// Gets all `file_ids` associated with a tag with namespace id x
    ///
    pub fn internal_file_id_get_namespace_id(
        &self,
        conn: &Connection,
        namespace_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn.prepare(&format!(
            "
SELECT DISTINCT file_id FROM {} WHERE tag_id in (
    SELECT id FROM Tags WHERE namespace = ?1
); 
",
            self.relationship_union_source(conn, "Relationship")
        ))?;
        let rows = stmt.query_map(params![namespace_id], |row| row.get(0))?;

        rows.collect()
    }

    ///
    /// Gets all files in db
    ///
    pub fn internal_file_get_all(
        &self,
        conn: &Connection,
    ) -> Result<HashSet<FileInternal>, rusqlite::Error> {
        let mut stmt =
            conn.prepare("select id, hash, extension, storage_id, size_bytes FROM File")?;
        let rows = stmt.query_map([], |row| {
            Ok(FileInternal {
                id: row.get(0)?,
                hash: row.get(1)?,
                extension: row.get(2)?,
                storage_id: row.get(3)?,
                size_bytes: row.get(4)?,
            })
        })?;

        rows.collect()
    }

    ///
    /// Gets all file storage's in db
    ///
    pub fn internal_file_storage_get_all(
        conn: &Connection,
    ) -> Result<HashMap<u64, String>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT id, location FROM FileStorageLocations;")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;

        rows.collect()
    }

    ///
    /// Gets all file ids inside of the db
    ///
    pub fn internal_file_id_get_all(conn: &Connection) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT id FROM File;").unwrap();
        let out = stmt.query_map([], |row| row.get(0))?;

        out.collect()
    }

    ///
    /// Checks to see if the db contains a file hash
    /// TODO need to pull this data dynamically
    ///
    pub fn contains_hash_sync(&self, hash: &HashesSupported) -> Option<FileInternal> {
        let conn = self.pool.get().unwrap();

        let (algo, hash) = hashessupportedtoinner(hash);
        let file_id: Option<u64> = conn
            .query_row(
                "SELECT file_id FROM FileHashes WHERE algorithm = ?1 AND digest = ?2;",
                params![algo, hash],
                |row| row.get(0),
            )
            .ok();

        if let Some(file_id) = &file_id {
            Self::internal_file_id_get(&conn, file_id).ok()
        } else {
            None
        }
    }

    ///
    /// Batched lookup of every hash against `FileHashes` and the files that
    /// own them. Runs over `(algorithm, digest)` row values so millisecond
    /// jobs resolve all dedup lookups in a handful of queries instead of one
    /// round trip per hash.
    ///
    pub fn hashes_files_get_sync(
        &self,
        hashes: &[HashesSupported],
    ) -> HashMap<(String, String), FileInternal> {
        let conn = self.pool.get().unwrap();
        let mut out = HashMap::new();
        let keys = hashes
            .iter()
            .map(hashessupportedtokey)
            .collect::<Vec<(String, String)>>();

        for chunk in keys.chunks(SQL_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = (0..chunk.len())
                .map(|index| format!("(?{}, ?{})", index * 2 + 1, index * 2 + 2))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT file_id, algorithm, digest FROM FileHashes WHERE (algorithm, digest) IN ({placeholders})"
            );
            let mut stmt = conn.prepare(&query).unwrap();
            let mut query_params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() * 2);
            for (algorithm, digest) in chunk.iter() {
                query_params.push(algorithm);
                query_params.push(digest);
            }
            let rows = stmt
                .query_map(rusqlite::params_from_iter(query_params), |row| {
                    let file_id: u64 = row.get(0)?;
                    let algorithm: String = row.get(1)?;
                    let digest: String = row.get(2)?;
                    Ok(((algorithm, digest), file_id))
                })
                .unwrap();
            for matched in rows.flatten() {
                let (key, file_id) = matched;
                if let Ok(file_internal) = Self::internal_file_id_get(&conn, &file_id) {
                    out.insert(key, file_internal);
                }
            }
        }

        out
    }

    ///
    /// Gets the location where files should be stored
    ///
    pub fn internal_file_download_location_get(
        &self,
        conn: &Connection,
    ) -> Result<(PathBuf, u64), rusqlite::Error> {
        let target_location =
            if let Some(setting) = self.internal_setting_get(conn, "SYSTEM_file_location")? {
                match setting.param {
                    Some(param) => param,
                    None => "files".to_string(), // Fallback if param is null
                }
            } else {
                // No setting found at all; initialize the system defaults
                self.internal_file_download_location_set_default(conn)?;
                "files".to_string()
            };

        let path_id = self.internal_file_storage_location_get_or_create(conn, &target_location)?;

        Ok((PathBuf::from(target_location), path_id))
    }

    /// Adds a filehash to the db
    pub fn internal_file_hash_add(
        conn: &Connection,
        algo: &String,
        hash: &String,
        file_id: &u64,
    ) -> Result<usize, r2d2_sqlite::rusqlite::Error> {
        if !algo.is_empty() && !hash.is_empty() {
            conn.execute(
            "INSERT OR IGNORE INTO FileHashes (algorithm, digest, file_id) VALUES (?1, ?2, ?3);",
            params![algo, hash, file_id],
        )
        } else {
            Ok(0)
        }
    }

    ///
    /// Bulk adds files into DB returning their id
    ///
    pub fn internal_file_bulk_add(
        &self,
        conn: &Connection,
        parents: HashSet<shared_types::FileInternal>,
    ) -> HashSet<shared_types::FileInternal> {
        Self::internal_audit_context_set(conn, "file discovered from scraper or import").unwrap();
        let mut out = HashSet::new();

        if parents.is_empty() {
            return out;
        }

        let parents_vec: Vec<&shared_types::FileInternal> = parents.iter().collect();
        let mut query =
            String::from("INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(parents_vec.len() * 3);

        // String building
        for (i, parent) in parents_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!(
                "(?{}, ?{}, ?{}, ?{})",
                i * 4 + 1,
                i * 4 + 2,
                i * 4 + 3,
                i * 4 + 4
            ));
            params_vector.push(&parent.hash);
            params_vector.push(&parent.extension);
            params_vector.push(&parent.storage_id);
            params_vector.push(&parent.size_bytes);
        }

        // FIX: Combined into a single DO UPDATE SET clause separated by a comma
        query.push_str(
            " ON CONFLICT(hash) 
          DO UPDATE SET
             extension = excluded.extension,
             storage_id = excluded.storage_id,
             size_bytes = COALESCE(excluded.size_bytes, File.size_bytes)
          RETURNING id",
        );

        let mut stmt = conn.prepare(&query).unwrap();

        // FIX: Swapped to slice_to_params to match your lifetime array structure correctly
        let mut rows = stmt.query(&*params_vector).unwrap();

        let mut idx = 0;
        while let Some(row) = rows.next().unwrap() {
            let mut parent_obj = parents_vec[idx].clone();
            parent_obj.id = row.get(0).ok();

            out.insert(parent_obj);
            idx += 1;
        }

        out
    }

    ///
    /// Gets the file ids where the size_bytes is null
    ///
    fn internal_file_id_get_size_bytes_null(
        &self,
        conn: &Connection,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT id FROM File WHERE size_bytes IS Null;")?;
        Ok(stmt
            .query_map([], |f| f.get::<_, u64>(0))?
            .flatten()
            .collect())
    }

    ///
    /// Gets a file location on disk and fixes extension on FS if it doesn't exist
    ///
    pub fn get_file_location(file_internal: &FileInternal, base_path: &String) -> Option<PathBuf> {
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

    ///
    /// Fixes all files inside of the file storage location
    ///
    pub fn fix_internal_files(
        &self,
        action: &crate::cli::cli_structs::CheckFilesEnum,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Staring to fix internal files");
        let conn = self.pool.get()?;

        // Keep track of records whose size has never been recorded. These are
        // repaired from existing files below; only physically absent files are
        // sent through the redownload path.
        let missing_size_ids = self.internal_file_id_get_size_bytes_null(&conn)?;

        let file_storage_map = Self::internal_file_storage_get_all(&conn)?;

        let files = self.internal_file_get_all(&conn)?;

        let mut file_storage_missing = HashSet::new();
        let mut missing_size_updates = Vec::new();

        let mut valid_paths = HashSet::new();

        // Check the recorded storage first. A file found in another storage is
        // misplaced, not valid for the current database record.
        for file in &files {
            if let Some(file_base_path) = file_storage_map.get(&file.storage_id)
                && let Some(file_path) = Self::get_file_location(file, file_base_path)
            {
                let file_size = file
                    .id
                    .filter(|id| missing_size_ids.contains(id))
                    .and_then(|_| {
                        std::fs::metadata(&file_path)
                            .ok()
                            .map(|metadata| metadata.len())
                    });
                valid_paths.insert(file_path);
                if let Some(file_id) = file.id
                    && missing_size_ids.contains(&file_id)
                    && let Some(size) = file_size
                {
                    missing_size_updates.push((file_id, size));
                }
                continue;
            }

            file_storage_missing.insert(file);
        }

        info!("Missing {} files from db.", file_storage_missing.len());

        if !missing_size_updates.is_empty() {
            let mut writer = self.writer_lock();
            let tx = writer.transaction()?;
            for (file_id, size) in missing_size_updates {
                tx.execute(
                    "UPDATE File SET size_bytes = ?1 WHERE id = ?2 AND size_bytes IS NULL",
                    params![size, file_id],
                )?;
            }
            tx.commit()?;
        }

        if *action == CheckFilesEnum::Redownload {
            drop(conn);
            self.queue_missing_file_jobs(&file_storage_missing)?;
            return Ok(());
        }

        if !file_storage_missing.is_empty()
            || matches!(
                action,
                CheckFilesEnum::StorageCheck | CheckFilesEnum::StorageCheckFileName
            )
        {
            info!("Scanning file locations");

            let file_hash: HashMap<String, FileInternal> = files
                .into_iter()
                .map(|file| (file.hash.clone(), file))
                .collect();

            let default_file_location = self.file_download_location_main_sync().unwrap();

            if CheckFilesEnum::Print == *action {
                for (hash, _) in file_hash {
                    info!("Just printing the missing file: {hash}");
                }
            } else if matches!(
                action,
                CheckFilesEnum::StorageCheck | CheckFilesEnum::StorageCheckFileName
            ) {
                let mut files_to_scan = Vec::with_capacity(SQL_CHUNK_SIZE);
                let filename_only = *action == CheckFilesEnum::StorageCheckFileName;
                for storage_loc in file_storage_map.values() {
                    for entry in jwalk::WalkDir::new(storage_loc)
                        .into_iter()
                        .filter_map(std::result::Result::ok)
                        .filter(|entry| entry.file_type().is_file())
                        .filter(|entry| !valid_paths.contains(&entry.path()))
                    {
                        files_to_scan.push(entry.path());
                        if files_to_scan.len() == SQL_CHUNK_SIZE {
                            if filename_only {
                                process_storage_check_filename_chunk(
                                    &files_to_scan,
                                    &file_hash,
                                    &file_storage_map,
                                    &default_file_location,
                                );
                            } else {
                                process_storage_check_chunk(
                                    &files_to_scan,
                                    &file_hash,
                                    &file_storage_map,
                                    &default_file_location,
                                );
                            }
                            files_to_scan.clear();
                        }
                    }
                }
                if !files_to_scan.is_empty() {
                    if filename_only {
                        process_storage_check_filename_chunk(
                            &files_to_scan,
                            &file_hash,
                            &file_storage_map,
                            &default_file_location,
                        );
                    } else {
                        process_storage_check_chunk(
                            &files_to_scan,
                            &file_hash,
                            &file_storage_map,
                            &default_file_location,
                        );
                    }
                }

                for storage_loc in file_storage_map.values() {
                    let mut directories: Vec<_> = jwalk::WalkDir::new(storage_loc)
                        .sort(false)
                        .into_iter()
                        .filter_map(std::result::Result::ok)
                        .filter(|entry| entry.file_type().is_dir())
                        .collect();
                    // Remove children before parents so nested empty directories are cleaned up.
                    directories.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.depth()));
                    for entry in directories {
                        if entry.path() != Path::new(storage_loc)
                            && std::fs::remove_dir(entry.path()).is_ok()
                        {
                            info!("Removed empty directory: {}", entry.path().display());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Queues missing files as normal plugin jobs. The regular scheduler then
    /// applies the selected plugin's rate limiter and download handling.
    fn queue_missing_file_jobs(
        &self,
        missing_files: &HashSet<&FileInternal>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.pool.get()?;
        let candidates = missing_files
            .iter()
            .map(|file| (*file).clone())
            .collect::<HashSet<_>>();

        let mut jobs = Vec::new();
        for file in candidates {
            let Some(file_id) = file.id else {
                continue;
            };
            let tag_ids = self.internal_file_id_get_tag_ids(&conn, &file_id)?;
            let source_url = Self::internal_tag_id_get_tag(&conn, &tag_ids)
                .into_values()
                .find(|tag| tag.namespace.name == "source_url")
                .map(|tag| tag.name);
            if let Some(source_url) = source_url {
                let Some(plugin_manager) = self.plugin_manager.read().as_ref().cloned() else {
                    log::warn!("Cannot queue file {file_id}: plugin manager is not initialized");
                    continue;
                };
                let Some(site) = plugin_manager.site_for_url(&source_url) else {
                    log::warn!("Cannot queue file {file_id}: no plugin handles {source_url}");
                    continue;
                };
                jobs.push(shared_types::PluginJob {
                    time: crate::helper_functions::get_sys_time_in_secs(),
                    reptime: 0,
                    // SQLite INTEGER is signed 64-bit; keep recovery jobs at
                    // the highest representable priority without overflowing
                    // rusqlite's u64-to-integer conversion.
                    priority: shared_types::DEFAULT_PRIORITY,
                    recreation: None,
                    site: site.to_string(),
                    param: vec![shared_types::ScraperParam::Url(shared_types::Url {
                        url: source_url,
                        local_modifiers: Vec::new(),
                    })],
                    user_data: {
                        let mut user_data = std::collections::BTreeMap::new();
                        user_data.insert("INTSCRAPE_REDOWNLOAD".into(), "true".into());
                        user_data
                            .insert("INTSCRAPE_REDOWNLOAD_FILE_ID".into(), file_id.to_string());
                        user_data.insert("INTSCRAPE_REDOWNLOAD_HASH".into(), file.hash);
                        user_data.insert(
                            "INTSCRAPE_REDOWNLOAD_STORAGE_ID".into(),
                            file.storage_id.to_string(),
                        );
                        user_data
                    },
                });
            } else {
                log::warn!("Cannot redownload file {file_id}: no source URL tag");
            }
        }
        drop(conn);

        if jobs.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer_lock();
        let tx = writer.transaction()?;
        for job in &jobs {
            self.internal_jobs_add(&tx, job);
        }
        tx.commit()?;
        info!(
            "Queued {} missing files for plugin-managed redownload",
            jobs.len()
        );
        Ok(())
    }

    pub async fn update_missing_file_sizes(&self) -> Result<(), rusqlite::Error> {
        let pool = self.pool.clone();
        let should_exit = self.should_exit.clone();
        let database = self.clone();
        tokio::task::spawn_blocking(move || {
            // Resolve paths and inspect files without holding the serialized
            // writer. A storage scan can otherwise stall all database writes.

            let mut last_file_id = 0;
            loop {
               if should_exit.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                let conn = pool
                    .get()
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.into()))?;
                let storage = Self::internal_file_storage_get_all(&conn)?;

                let mut files = conn.prepare(&format!(
                    "SELECT id, hash, extension, storage_id
                 FROM File
                 WHERE size_bytes IS NULL
                   AND hash IS NOT NULL
                   AND id > ?1
                 ORDER BY id
                 LIMIT {}",
                    SQL_CHUNK_SIZE
                ))?;
                info!(
                    "System file size checker has processed files through id: {}",
                    last_file_id
                );
                let rows: Vec<_> = files
                    .query_map([last_file_id], |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, u64>(3)?,
                        ))
                    })?
                    .collect();

                if rows.is_empty() {
                    break;
                }

                let mut updates = Vec::new();
                let mut missing_paths = 0;
                let mut unreadable_files = 0;
                for row in rows {
                    let (id, hash, extension, storage_id) = row?;
                    last_file_id = id;
                    let file = FileInternal {
                        id: Some(id),
                        hash,
                        extension,
                        storage_id,
                        size_bytes: None,
                    };
                    let path = storage
                        .get(&storage_id)
                        .and_then(|base| Self::get_file_location(&file, base))
                        .or_else(|| {
                            storage
                                .iter()
                                .filter(|(id, _)| **id != storage_id)
                            .find_map(|(_, base)| Self::get_file_location(&file, base))
                        });
                    if let Some(path) = path {
                        match std::fs::metadata(path) {
                            Ok(metadata) => updates.push((id, metadata.len())),
                            Err(_) => unreadable_files += 1,
                        }
                    } else {
                        missing_paths += 1;
                    }
                }
                drop(files);
                drop(conn);

            let mut writer = database.writer_lock();
                let tx = writer.transaction()?;
                {
                    let mut update = tx.prepare("UPDATE File SET size_bytes = ?1 WHERE id = ?2")?;
                    for (id, size) in &updates {
                        update.execute(params![size, id])?;
                    }
                }
                tx.commit()?;
                if missing_paths > 0 || unreadable_files > 0 {
                    log::warn!(
                        "System file size checker skipped {} files with missing paths and {} unreadable files; they will be retried later",
                        missing_paths,
                        unreadable_files,
                    );
                }
                info!(
                    "System file size checker updated {} files",
                    updates.len()
                );
            }
            Ok(())
        })
        .await
        .unwrap()
    }

    ///
    /// Gets a file if its id exists in db
    ///
    pub async fn file_id_get(&self, file_id: u64) -> Option<FileInternal> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            Self::internal_file_id_get(&conn, &file_id).ok()
        })
        .await
        .unwrap()
    }

    ///
    /// Gets the location where files should be stored
    /// IE the main folder that we're using
    ///
    pub async fn file_download_location_main(self: Arc<Self>) -> Option<(PathBuf, u64)> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().ok()?;
            self.internal_file_download_location_get(&conn).ok()
        })
        .await
        .ok()
        .flatten()
    }

    ///
    /// Gets the location we should download to
    ///
    #[must_use]
    pub fn file_download_location_main_sync(&self) -> Option<(PathBuf, u64)> {
        let pool = self.pool.clone();
        let conn = pool.get().ok()?;
        self.internal_file_download_location_get(&conn).ok()
    }

    ///
    /// Returns the full location of where a file should be stored
    ///
    pub async fn file_download_location_get(
        self: Arc<Self>,
        hash: &str,
        ext: &str,
    ) -> Option<(PathBuf, u64)> {
        // If our hash is less then 6 cant return a location
        if hash.len() <= 6 {
            return None;
        }
        self.clone()
            .file_download_location_main()
            .await
            .map(|path| {
                let mut path_buf = path.0;
                path_buf.push(&hash[0..2]);
                path_buf.push(&hash[2..4]);
                path_buf.push(&hash[4..6]);
                path_buf.push(hash);
                (path_buf.with_extension(ext), path.1)
            })
    }

    #[must_use]
    pub fn file_download_location_get_sync(&self, hash: &str, ext: &str) -> Option<(PathBuf, u64)> {
        if hash.len() <= 6 {
            return None;
        }
        self.file_download_location_main_sync().map(|path| {
            let mut path_buf = path.0;
            path_buf.push(&hash[0..2]);
            path_buf.push(&hash[2..4]);
            path_buf.push(&hash[4..6]);
            path_buf.push(hash);
            (path_buf.with_extension(ext), path.1)
        })
    }

    ///
    /// Adds tags into db in bulk. Also adds parents
    ///
    pub async fn file_add_bulk(
        self: Arc<Self>,
        tags: HashSet<FileInternal>,
    ) -> HashSet<FileInternal> {
        if tags.is_empty() {
            return HashSet::new();
        }

        let tags_owned = tags.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer_lock_guard = self.writer_lock();
            let tn = match writer_lock_guard.transaction() {
                Ok(tn) => tn,
                Err(error) => {
                    log::error!("Failed to begin file insertion transaction: {error}");
                    return HashSet::new();
                }
            };
            let out_tags = self.internal_file_bulk_add(&tn, tags_owned);

            if let Err(error) = tn.commit() {
                log::error!("Failed to commit file insertion: {error}");
                return HashSet::new();
            }
            out_tags
        })
        .await
        .unwrap()
    }
}
