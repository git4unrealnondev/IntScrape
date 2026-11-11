use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use walkdir::WalkDir;

use bytes::Bytes;
use rayon::prelude::*;
use shared_types::{
    DEFAULT_PRIORITY, DbJobRecreation, DbJobsObj, FileInternal, HashesSupported, PluginJob,
    ScraperParam, Url,
};
use strum::IntoEnumIterator;
use turso::Connection;

use crate::backup_path::dated_backup_destination;
use crate::db::{
    SQL_CHUNK_SIZE, SYSTEM_DATABASE_BACKUP_SITE, SYSTEM_DATABASE_SLURP_SITE, SYSTEM_FILE_HASH_SITE,
    SYSTEM_FILE_SIZE_SITE, SYSTEM_STORAGE_CHECK_FILENAME_MODE,
    SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE, SYSTEM_STORAGE_CHECK_SITE,
};
use crate::web::manager::hash_bytes;

use super::TursoDatabase;

/// Namespace whose tag *name* is the URL a file was downloaded from.
const SOURCE_URL_NAMESPACE: &str = "source_url";

/// Rows per page for the storage walk.
///
/// This bounds both the file rows held in memory and the per-file tag fan-out
/// `file_ids_get_tags` returns for source-URL resolution, so it is deliberately
/// well below `SQL_CHUNK_SIZE`: a popular-missing-file page would otherwise
/// pull every tag of every file in the page just to find one `source_url` tag
/// each. The page loop is cheap (an indexed keyset range scan per page), and
/// the real cost of the walk is the filesystem probe per file either way.
const REDOWNLOAD_SCAN_PAGE: usize = 512;

/// Files per database round trip for the storage check. Each chunk costs one
/// batched filename lookup (plus one more for the files that need hashing),
/// so this is the unit the whole walk is metered in.
const STORAGE_CHECK_BATCH: usize = 1000;

/// Outcome of one storage-check (cleanup) run.
#[derive(Default, Debug)]
pub(crate) struct StorageCheckSummary {
    checked: usize,
    already_correct: usize,
    moved_to_correct_place: usize,
    hashed: usize,
    dumpstered: usize,
    failures: usize,
}

impl TursoDatabase {
    pub async fn run_system_job(&self, job: &DbJobsObj) -> bool {
        if self.should_exit() {
            return false;
        }

        // A slurp no longer pauses the poller or the IPC server; its write side
        // is short BEGIN IMMEDIATE batches that other jobs interleave with.
        // Only a second slurp (claimed in the race window before the first one
        // raised the flag) defers — a concurrent bulk import would fight over
        // the Tags_slurp staging table and the per-stage cursors.
        if self.is_slurping() && job.config.site.as_str() == SYSTEM_DATABASE_SLURP_SITE {
            log::warn!(
                "Deferring system job {} because another database slurp is running",
                job.id
            );
            return false;
        }

        let success = match job.config.site.as_str() {
            SYSTEM_DATABASE_BACKUP_SITE => self.run_backup_job(job).await,
            SYSTEM_DATABASE_SLURP_SITE => self.run_slurp_job(job).await,
            SYSTEM_FILE_SIZE_SITE => match self.update_missing_file_sizes().await {
                Ok(_) => true,
                Err(error) => {
                    log::error!("File-size system job {} failed: {error}", job.id);
                    false
                }
            },
            SYSTEM_FILE_HASH_SITE => match self.hash_missing_file_hashes().await {
                Ok(_) => true,
                Err(error) => {
                    log::error!("File-hash system job {} failed: {error}", job.id);
                    false
                }
            },
            SYSTEM_STORAGE_CHECK_SITE => self.run_storage_check_job(job).await,
            _ => return false,
        };

        if self.should_exit() {
            return false;
        }

        self.complete_system_job(job).await;
        success
    }

    async fn run_backup_job(&self, job: &DbJobsObj) -> bool {
        let Some(ScraperParam::Normal(destination)) = job.config.param.first() else {
            log::error!("Database backup job {} has no destination path", job.id);
            self.complete_system_job(job).await;
            return false;
        };
        let destination = dated_backup_destination(destination, SystemTime::now());
        match self.backup_db_to(Path::new(&destination)).await {
            Ok(()) => {
                log::info!("Database backup job {} wrote {}", job.id, destination);
                true
            }
            Err(error) => {
                log::error!(
                    "Database backup job {} failed for {}: {error}",
                    job.id,
                    destination
                );
                false
            }
        }
    }

    async fn run_slurp_job(&self, job: &DbJobsObj) -> bool {
        let Some(ScraperParam::Normal(source)) = job.config.param.first() else {
            log::error!("Database slurp job {} has no source path", job.id);
            return false;
        };

        match self.db_slurp_blocking(Path::new(source)) {
            Ok((namespaces, tags, files)) => {
                log::info!(
                    "Database slurp job {} imported {} namespaces, {} tags, and {} files",
                    job.id,
                    namespaces,
                    tags,
                    files
                );
                true
            }
            Err(error) => {
                log::error!("Database slurp job {} failed: {error}", job.id);
                false
            }
        }
    }

    /// Dispatches the `SYSTEM_STORAGE_CHECK_SITE` family by its mode param:
    ///
    /// - `redownload` — queue recovery jobs for DB files missing from disk.
    /// - `filename` — cleanup pass that trusts file names only (stored files
    ///   are named by their SHA-512): known files are moved into place,
    ///   everything else goes to the dumpster without ever hashing content.
    /// - default (no param) — cleanup pass that first tries the file name,
    ///   then hashes whatever the name could not resolve.
    async fn run_storage_check_job(&self, job: &DbJobsObj) -> bool {
        let mode = match job.config.param.first() {
            Some(ScraperParam::Normal(mode)) => mode.clone(),
            _ => String::new(),
        };

        match mode.as_str() {
            SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE => match self.redownload_missing_files().await {
                Ok(summary) => {
                    log::info!(
                        "Redownload system job {} queued {} recovery job(s): {} missing file(s), \
                             {} without a source URL, {} with no plugin for their URL",
                        job.id,
                        summary.queued_jobs,
                        summary.missing_files,
                        summary.no_source_url,
                        summary.no_plugin,
                    );
                    true
                }
                Err(error) => {
                    log::error!("Redownload system job {} failed: {error}", job.id);
                    false
                }
            },
            SYSTEM_STORAGE_CHECK_FILENAME_MODE => self.run_cleanup_job(job, true).await,
            "" => self.run_cleanup_job(job, false).await,
            other => {
                log::warn!(
                    "Storage-check system job {} has unknown mode {other:?}; ignoring",
                    job.id,
                );
                false
            }
        }
    }

    async fn run_cleanup_job(&self, job: &DbJobsObj, filename_only: bool) -> bool {
        match self.storage_check_files(filename_only).await {
            Ok(_) => true,
            Err(error) => {
                log::error!("Storage check system job {} failed: {error}", job.id);
                false
            }
        }
    }

    /// Cleanup pass over every storage root: files the database knows are
    /// moved to their correct partitioned place if they are misplaced, and
    /// files the database does not know are moved into the dumpster (the
    /// `dumpster` directory next to the default storage location, preserving
    /// each file's path relative to its storage root).
    ///
    /// Stored files are named by their SHA-512, so each file is first checked
    /// by name — one batched DB lookup per chunk and no hashing at all for
    /// the (overwhelmingly common) healthy case. Only files the name cannot
    /// resolve are hashed, and only in `filename_only` mode are they hashed
    /// never at all: they go straight to the dumpster.
    pub(crate) async fn storage_check_files(
        &self,
        filename_only: bool,
    ) -> Result<StorageCheckSummary, Box<dyn Error + Send + Sync>> {
        let conn = self.connect()?;
        let storage = self.file_storage_get_all(&conn).await?;
        if storage.is_empty() {
            log::warn!("Storage check found no storage locations recorded; nothing to check");
            return Ok(StorageCheckSummary::default());
        }
        let (default_base, _) = self.file_download_location_get(&conn).await?;
        let dumpster_root = default_base.with_file_name("dumpster");

        // Deterministic root order for reproducible walks/logs.
        let mut roots: Vec<(u64, String)> = storage.into_iter().collect();
        roots.sort_unstable();
        let storage_map: HashMap<u64, String> = roots.iter().cloned().collect();

        let mut summary = StorageCheckSummary::default();
        for (_, root) in &roots {
            if self.should_exit() {
                break;
            }
            let root_path = Path::new(root);
            // Skip the dumpster subtree when it happens to live inside the
            // root being walked, or already-dumped files would be re-dumped.
            let mut paths: Vec<PathBuf> = WalkDir::new(root_path)
                .into_iter()
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_type().is_file())
                .map(|entry| entry.path().to_path_buf())
                .filter(|path| !path.starts_with(&dumpster_root))
                .collect();
            paths.sort();

            for chunk in paths.chunks(STORAGE_CHECK_BATCH) {
                if self.should_exit() {
                    break;
                }
                summary.checked += chunk.len();
                self.storage_check_chunk(
                    &conn,
                    &storage_map,
                    root_path,
                    chunk,
                    filename_only,
                    &dumpster_root,
                    &mut summary,
                )
                .await?;
            }
        }

        log::info!(
            "Storage check finished: {} checked, {} already correct, {} moved into place, \
             {} hashed, {} to dumpster, {} failure(s).",
            summary.checked,
            summary.already_correct,
            summary.moved_to_correct_place,
            summary.hashed,
            summary.dumpstered,
            summary.failures,
        );
        Ok(summary)
    }

    #[allow(clippy::too_many_arguments)]
    async fn storage_check_chunk(
        &self,
        conn: &Connection,
        storage_map: &HashMap<u64, String>,
        root: &Path,
        paths: &[PathBuf],
        filename_only: bool,
        dumpster_root: &Path,
        summary: &mut StorageCheckSummary,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 1. Filename quick check: one batched lookup for the whole chunk.
        let stems: Vec<String> = paths
            .iter()
            .filter_map(|path| sha512_filename_stem(path))
            .collect();
        let by_hash = if stems.is_empty() {
            HashMap::new()
        } else {
            self.lookup_files_by_hash(conn, &stems).await?
        };

        let mut to_hash: Vec<&PathBuf> = Vec::new();
        for path in paths {
            match sha512_filename_stem(path).and_then(|stem| by_hash.get(&stem)) {
                Some(file) => {
                    let file = file.clone();
                    if storage_check_move_to_expected(path, storage_map, &file, filename_only) {
                        summary.moved_to_correct_place += 1;
                    } else {
                        summary.already_correct += 1;
                    }
                }
                None if filename_only => {
                    if move_to_dumpster(path, root, dumpster_root) {
                        summary.dumpstered += 1;
                    } else {
                        summary.failures += 1;
                    }
                }
                None => to_hash.push(path),
            }
        }

        if to_hash.is_empty() {
            return Ok(());
        }

        // 2. Hash fallback: hash whatever the name could not resolve, then
        // batch-lookup the computed hashes the same way.
        let mut computed: Vec<(&PathBuf, String)> = Vec::new();
        for path in to_hash {
            match crate::web::manager::hash_file_sha512(path) {
                Ok(hash) => {
                    summary.hashed += 1;
                    computed.push((path, hash));
                }
                Err(error) => {
                    log::error!("Storage check could not hash {}: {error}", path.display());
                    summary.failures += 1;
                }
            }
        }
        if computed.is_empty() {
            return Ok(());
        }
        let hashes: Vec<String> = computed.iter().map(|(_, hash)| hash.clone()).collect();
        let found = self.lookup_files_by_hash(conn, &hashes).await?;
        for (path, hash) in &computed {
            match found.get(hash) {
                Some(file) => {
                    let file = file.clone();
                    if storage_check_move_to_expected(path, storage_map, &file, filename_only) {
                        summary.moved_to_correct_place += 1;
                    } else {
                        summary.already_correct += 1;
                    }
                }
                None => {
                    if move_to_dumpster(path, root, dumpster_root) {
                        summary.dumpstered += 1;
                    } else {
                        summary.failures += 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Batched file lookup by SHA-512 digest: primary `File.hash` first,
    /// then the `SHA512` identifying hash in `FileHashes` for files whose
    /// primary hash is something else (e.g. slurp-imported rows).
    async fn lookup_files_by_hash(
        &self,
        conn: &Connection,
        hashes: &[String],
    ) -> Result<HashMap<String, FileInternal>, Box<dyn Error + Send + Sync>> {
        let mut out = self.files_get_by_hash(conn, hashes).await?;
        let keys: Vec<(String, String)> = hashes
            .iter()
            .map(|hash| ("SHA512".to_string(), hash.clone()))
            .collect();
        for ((_, digest), file) in self.hashes_files_get(conn, &keys).await? {
            out.entry(digest).or_insert(file);
        }
        Ok(out)
    }

    /// Queues a plugin scrape job for every file recorded in the database whose
    /// file is no longer present on disk.
    ///
    /// The recovery jobs are ordinary `ScraperParam::Url` jobs carrying the
    /// `INTSCRAPE_REDOWNLOAD` marker, so the normal scheduler applies the
    /// owning plugin's rate limiter and download handling, and `web::manager`
    /// skips `url_dump` and re-hashing for them. This job only has to answer
    /// "which recorded files are gone, and where did they come from".
    ///
    /// Walks `File` in keyset pages so memory is bounded by `REDOWNLOAD_SCAN_PAGE`
    /// rather than by library size, and probes the filesystem on the blocking
    /// pool so a multi-million-file walk cannot stall the async runtime.
    ///
    /// Note this does not dedupe against recovery jobs enqueued by an earlier
    /// run: `Jobs` is unique on `(time, reptime, site, param)` and `time` is
    /// the current second, so a repeated run inserts fresh rows. That matches
    /// the legacy backend's behaviour; suppressing them would need a query the
    /// "no custom SQL" constraint rules out here.
    pub(crate) async fn redownload_missing_files(
        &self,
    ) -> Result<RedownloadSummary, Box<dyn Error + Send + Sync>> {
        let conn = self.connect()?;

        // One entry per configured storage root; tiny, and read once.
        let storage = self.file_storage_get_all(&conn).await?;
        if storage.is_empty() {
            log::warn!("Redownload job found no storage locations recorded; nothing to check");
            return Ok(RedownloadSummary::default());
        }

        // Cloned once so the blocking walk owns it; the guard would otherwise be
        // held across every page's `spawn_blocking`.
        let Some(plugin_manager) = self.plugin_manager.read().clone() else {
            log::warn!("Redownload job cannot run: plugin manager is not initialized");
            return Ok(RedownloadSummary::default());
        };

        let mut summary = RedownloadSummary::default();
        let mut after_id = 0u64;

        loop {
            if self.should_exit() {
                log::info!("Redownload job stopping early on shutdown flag");
                break;
            }

            let page = self
                .file_get_page(&conn, after_id, REDOWNLOAD_SCAN_PAGE)
                .await?;
            if page.is_empty() {
                break;
            }
            after_id = page.last().and_then(|file| file.id).unwrap_or(after_id);

            // Probing storage is filesystem I/O, one stat per file per storage
            // root: offload the whole page rather than blocking a runtime
            // worker on it.
            let roots = storage.clone();
            let missing = tokio::task::spawn_blocking(move || {
                page.into_iter()
                    .filter(|file| !file_present_in_storage(file, &roots))
                    .collect::<Vec<FileInternal>>()
            })
            .await
            .map_err(|error| format!("redownload storage walk panicked: {error}"))?;

            if missing.is_empty() {
                continue;
            }
            summary.missing_files += missing.len() as u64;

            // Batched existing lookup: every tag of every missing file in this
            // page, resolved in one union query rather than per file.
            let missing_ids: HashSet<u64> = missing.iter().filter_map(|file| file.id).collect();
            let tags_by_file = self.file_ids_get_tags(&conn, &missing_ids).await?;

            let mut seen_in_page: HashSet<u64> = HashSet::new();
            let mut jobs: Vec<PluginJob> = Vec::with_capacity(missing.len());
            for file in &missing {
                let Some(file_id) = file.id else {
                    continue;
                };
                // A file can carry more than one `source_url` tag; one recovery
                // job per file is enough.
                if !seen_in_page.insert(file_id) {
                    continue;
                }

                let Some(source_url) = tags_by_file
                    .get(&file_id)
                    .and_then(|tags| {
                        tags.iter()
                            .find(|tag| tag.namespace.name == SOURCE_URL_NAMESPACE)
                    })
                    .map(|tag| tag.name.clone())
                else {
                    summary.no_source_url += 1;
                    continue;
                };

                let Some(site) = plugin_manager.site_for_url(&source_url) else {
                    summary.no_plugin += 1;
                    continue;
                };

                jobs.push(redownload_job(file, source_url, site));
            }

            if !jobs.is_empty() {
                // One chunked statement per page, autocommitted: the page is
                // already the transaction unit, so no enclosing transaction
                // (and no hand-written BEGIN/COMMIT) is needed.
                self.jobs_bulk_add_sql(&conn, &jobs).await?;
                summary.queued_jobs += jobs.len() as u64;
            }
        }

        Ok(summary)
    }

    async fn backup_db_to(
        &self,
        destination: &Path,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.connect()?;
        let _ = conn.query("PRAGMA wal_checkpoint(TRUNCATE);", ()).await;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&self.db_path, destination)?;
        Ok(())
    }

    pub async fn update_missing_file_sizes(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.connect()?;
        let storage = self.file_storage_get_all(&connection).await?;

        let mut last_file_id = 0;
        loop {
            if self.should_exit() {
                break;
            }

            let mut rows = connection
                .query(
                    &format!(
                        "SELECT f.id, fh.digest, f.extension, f.storage_id
                         FROM File f
                         JOIN FileHashes fh
                           ON fh.file_id = f.id AND fh.algorithm = 'SHA512'
                         WHERE f.size_bytes IS NULL AND f.id > ?1
                         ORDER BY f.id
                         LIMIT {}",
                        SQL_CHUNK_SIZE,
                    ),
                    (last_file_id as i64,),
                )
                .await?;

            let batch: Vec<(u64, String, String, u64)> = {
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((
                        row.get::<i64>(0)? as u64,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<i64>(3)? as u64,
                    ));
                }
                out
            };

            if batch.is_empty() {
                break;
            }

            last_file_id = batch.last().map(|(id, _, _, _)| *id).unwrap_or(0);

            let updates: Vec<(u64, u64)> = batch
                .iter()
                .filter_map(|(id, hash, extension, storage_id)| {
                    let file = shared_types::FileInternal {
                        id: Some(*id),
                        hash: hash.clone(),
                        extension: extension.clone(),
                        storage_id: *storage_id,
                        size_bytes: None,
                    };
                    let path = storage
                        .get(storage_id)
                        .and_then(|base| super::file::file_on_disk(&file, base))
                        .or_else(|| {
                            storage
                                .iter()
                                .find(|(sid, _)| **sid != *storage_id)
                                .and_then(|(_, base)| super::file::file_on_disk(&file, base))
                        })?;
                    std::fs::metadata(path).ok().map(|m| (*id, m.len()))
                })
                .collect();

            if !updates.is_empty() {
                loop {
                    match self.file_sizes_update(&connection, &updates).await {
                        Ok(()) => break,
                        Err(error) if Self::is_concurrency_conflict(&error) => {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }

            log::info!(
                "File-size system has processed files through id: {}",
                last_file_id,
            );
        }

        Ok(())
    }

    pub async fn hash_missing_file_hashes(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.connect()?;

        let algorithms: Vec<(&'static str, HashesSupported)> = HashesSupported::iter()
            .map(|hash| match hash {
                HashesSupported::Md5(_) => ("MD5", HashesSupported::Md5(String::new())),
                HashesSupported::Sha1(_) => ("SHA1", HashesSupported::Sha1(String::new())),
                HashesSupported::Sha256(_) => ("SHA256", HashesSupported::Sha256(String::new())),
                HashesSupported::Sha512(_) => ("SHA512", HashesSupported::Sha512(String::new())),
                // HashesSupported::IPFSCID(_) => ("IPFSCID", HashesSupported::IPFSCID(String::new())),
                // HashesSupported::IPFSCID1(_) => {
                //     ("IPFSCID1", HashesSupported::IPFSCID1(String::new()))
                // }
                HashesSupported::ImageHash(_) => {
                    ("ImageHash", HashesSupported::ImageHash(String::new()))
                }
            })
            .collect();

        let mut last_file_id = 0;
        loop {
            if self.should_exit() {
                break;
            }

            log::info!(
                "System file hasher has processed files through id: {}",
                last_file_id,
            );

            let mut rows = connection
                .query(
                    &format!(
                        "SELECT id FROM File f
                         WHERE NOT EXISTS (
                             SELECT 1 FROM FileHashes fh WHERE fh.file_id = f.id
                         ) AND id > ?1
                         ORDER BY id
                         LIMIT {}",
                        SQL_CHUNK_SIZE,
                    ),
                    (last_file_id as i64,),
                )
                .await?;

            let file_ids: Vec<u64> = {
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row.get::<i64>(0)? as u64);
                }
                out
            };

            if file_ids.is_empty() {
                break;
            }

            // Guarded by the `is_empty` break above.
            if let Some(last) = file_ids.last() {
                last_file_id = *last;
            }

            if self.should_exit() {
                break;
            }

            let file_paths: Vec<(u64, String)> = {
                let mut out = Vec::new();
                for &file_id in &file_ids {
                    if let Ok(Some(path)) = self.file_get_physical_path(&connection, file_id).await
                    {
                        out.push((file_id, path));
                    }
                }
                out
            };

            let pending: Vec<(u64, String, String)> = tokio::task::spawn_blocking({
                let algorithms = algorithms.clone();
                move || {
                    file_paths
                        .par_iter()
                        .flat_map(|(file_id, path)| {
                            let bytes = match std::fs::read(path) {
                                Ok(bytes) => Bytes::from(bytes),
                                Err(error) => {
                                    log::warn!("Cannot read file {file_id} while hashing: {error}");
                                    return Vec::new();
                                }
                            };
                            algorithms
                                .iter()
                                .map(|(algorithm, kind)| {
                                    (
                                        *file_id,
                                        (*algorithm).to_string(),
                                        hash_bytes(&bytes, kind).0,
                                    )
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect()
                }
            })
            .await?;

            if !pending.is_empty() {
                let tuples: Vec<(u64, &str, &str)> = pending
                    .iter()
                    .filter(|(_, _, digest)| !digest.is_empty())
                    .map(|(id, algo, digest)| (*id, algo.as_str(), digest.as_str()))
                    .collect();
                if !tuples.is_empty() {
                    loop {
                        match self.file_hashes_add_bulk(&connection, &tuples).await {
                            Ok(()) => break,
                            Err(error) if Self::is_concurrency_conflict(&error) => {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                log::info!("File-hash system updated {} files", pending.len(),);
            }
        }

        Ok(())
    }
}

/// What one redownload run did, for aggregate logging.
///
/// Counters rather than one log line per file: a storage walk can cover
/// millions of rows, and `agents.md` requires per-item logging to stay out of
/// these paths.
#[derive(Default)]
pub(crate) struct RedownloadSummary {
    missing_files: u64,
    queued_jobs: u64,
    no_source_url: u64,
    no_plugin: u64,
}

/// Whether a file's bytes are still present under any configured storage root.
///
/// Checks the file's own `storage_id` root first (the overwhelmingly common
/// case), then the rest, because a library restored from a backup or moved
/// between roots will not be where the database says it is.
fn file_present_in_storage(file: &FileInternal, roots: &HashMap<u64, String>) -> bool {
    if let Some(own_root) = roots.get(&file.storage_id)
        && super::file::file_on_disk(file, own_root).is_some()
    {
        return true;
    }
    roots
        .values()
        .any(|root| super::file::file_on_disk(file, root).is_some())
}

/// The file-name stem if it looks like a SHA-512 digest (128 hex chars,
/// normalised to the uppercase form stored files are named with). Anything
/// else cannot be a stored file's name and skips the quick check.
fn sha512_filename_stem(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    if stem.len() == 128 && stem.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(stem.to_ascii_uppercase())
    } else {
        None
    }
}

/// Where a known file belongs on disk: its storage root, hash-partitioned
/// exactly like `super::file::file_on_disk` resolves it. Returns `None` for
/// rows whose hash is too short to partition (legacy short hashes).
fn expected_storage_path(base: &Path, file: &FileInternal) -> Option<PathBuf> {
    if file.hash.len() <= 6 {
        return None;
    }
    let mut path = base.to_path_buf();
    path.push(&file.hash[0..2]);
    path.push(&file.hash[2..4]);
    path.push(&file.hash[4..6]);
    path.push(&file.hash);
    Some(path.with_added_extension(&file.extension))
}

/// Moves a file to its correct place if it is misplaced. Returns whether a
/// move happened; a file already in place (or whose target exists) reports
/// `false`. Copy-then-remove like the legacy backend, so it works across
/// filesystems; the target is never overwritten.
fn storage_check_move_to_expected(
    path: &Path,
    storage_map: &HashMap<u64, String>,
    file: &FileInternal,
    filename_only: bool,
) -> bool {
    let Some(base) = storage_map.get(&file.storage_id) else {
        log::warn!(
            "Storage check: no storage root recorded for file {}, leaving {} in place",
            file.hash,
            path.display()
        );
        return false;
    };
    let base = Path::new(base);
    let target = if filename_only {
        // The legacy filename mode kept the original file name.
        if file.hash.len() <= 6 {
            return false;
        }
        let Some(file_name) = path.file_name() else {
            return false;
        };
        base.join(&file.hash[0..2])
            .join(&file.hash[2..4])
            .join(&file.hash[4..6])
            .join(file_name)
    } else {
        let Some(target) = expected_storage_path(base, file) else {
            return false;
        };
        target
    };
    move_file_to(path, &target)
}

/// Moves a file the database does not know into the dumpster, preserving
/// its path relative to its storage root so files from different roots (or
/// with colliding names) cannot clobber each other.
fn move_to_dumpster(path: &Path, root: &Path, dumpster_root: &Path) -> bool {
    let relative = path
        .strip_prefix(root)
        .unwrap_or_else(|_| path.file_name().map(Path::new).unwrap_or(path));
    move_file_to(path, &dumpster_root.join(relative))
}

/// Copy-then-remove move that never overwrites an existing target.
/// Returns whether the file was moved.
fn move_file_to(path: &Path, target: &Path) -> bool {
    if path == target || !path.exists() || target.exists() {
        return false;
    }
    let Some(parent) = target.parent() else {
        return false;
    };
    if std::fs::create_dir_all(parent).is_ok()
        && std::fs::copy(path, target).is_ok()
        && std::fs::remove_file(path).is_ok()
    {
        log::info!("Moved file: {} to: {}", path.display(), target.display());
        true
    } else {
        log::error!(
            "Storage check failed to move {} to {}",
            path.display(),
            target.display()
        );
        false
    }
}

/// Builds the recovery job for one missing file.
///
/// Pure so the job shape can be asserted without a plugin manager: the
/// `INTSCRAPE_REDOWNLOAD*` `user_data` keys are a contract with
/// `web::manager`, not an implementation detail.
fn redownload_job(file: &FileInternal, source_url: String, site: String) -> PluginJob {
    PluginJob {
        time: crate::helper_functions::get_sys_time_in_secs(),
        reptime: 0,
        // Recovery jobs jump the queue, matching the legacy backend: a
        // recorded-but-absent file is a real loss to repair.
        priority: DEFAULT_PRIORITY,
        recreation: None,
        site,
        param: vec![ScraperParam::Url(Url {
            url: source_url,
            local_modifiers: Vec::new(),
        })],
        user_data: {
            let mut user_data = std::collections::BTreeMap::new();
            user_data.insert("INTSCRAPE_REDOWNLOAD".to_string(), "true".to_string());
            if let Some(file_id) = file.id {
                user_data.insert(
                    "INTSCRAPE_REDOWNLOAD_FILE_ID".to_string(),
                    file_id.to_string(),
                );
            }
            user_data.insert("INTSCRAPE_REDOWNLOAD_HASH".to_string(), file.hash.clone());
            user_data.insert(
                "INTSCRAPE_REDOWNLOAD_STORAGE_ID".to_string(),
                file.storage_id.to_string(),
            );
            user_data
        },
    }
}

pub fn is_system_job(job: &DbJobsObj) -> bool {
    matches!(
        job.config.site.as_str(),
        SYSTEM_DATABASE_BACKUP_SITE
            | SYSTEM_DATABASE_SLURP_SITE
            | SYSTEM_FILE_SIZE_SITE
            | SYSTEM_FILE_HASH_SITE
            | SYSTEM_STORAGE_CHECK_SITE
    ) || matches!(
        job.config.recreation,
        Some(DbJobRecreation::AlwaysTime(_, _))
    ) && job.config.site.starts_with("SYSTEM_")
}

#[cfg(test)]
mod tests;
