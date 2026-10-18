use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::Path;
use std::time::SystemTime;

use bytes::Bytes;
use rayon::prelude::*;
use shared_types::{
    DEFAULT_PRIORITY, DbJobRecreation, DbJobsObj, FileInternal, HashesSupported, PluginJob,
    ScraperParam, Url,
};
use strum::IntoEnumIterator;

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

impl TursoDatabase {
    pub async fn run_system_job(&self, job: &DbJobsObj) -> bool {
        if self.should_exit() {
            return false;
        }

        // A slurp owns the database and has already stopped the poller from
        // claiming new work (including this job). If this job was claimed just
        // before the slurp started, back off so its BEGIN IMMEDIATE writes are
        // not contended by a maintenance reader.
        if self.is_slurping() {
            log::warn!(
                "Paused system job {} while a database slurp is running",
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

    /// Dispatches the `SYSTEM_STORAGE_CHECK_SITE` family by its mode param.
    ///
    /// Only `redownload` is implemented on this backend. The other two modes
    /// (`StorageCheck` / `StorageCheckFileName`) are CLI-reachable but have no
    /// turso implementation, so they log rather than silently reporting success.
    async fn run_storage_check_job(&self, job: &DbJobsObj) -> bool {
        let mode = match job.config.param.first() {
            Some(ScraperParam::Normal(mode)) => mode.clone(),
            _ => String::new(),
        };

        if mode != SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE {
            let described = if mode.is_empty() {
                "<default>".to_string()
            } else {
                mode.clone()
            };
            log::warn!(
                "Storage-check system job {} mode {described} is not implemented on the turso \
                 backend; only '{SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE}' is. \
                 ('{SYSTEM_STORAGE_CHECK_FILENAME_MODE}' and the default StorageCheck mode are \
                 still no-ops.)",
                job.id,
            );
            return false;
        }

        match self.redownload_missing_files().await {
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
        }
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
    async fn redownload_missing_files(
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
                        "SELECT id, hash, extension, storage_id
                         FROM File
                         WHERE size_bytes IS NULL AND hash IS NOT NULL AND id > ?1
                         ORDER BY id
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

            last_file_id = *file_ids.last().unwrap();

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
struct RedownloadSummary {
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

pub(crate) fn is_system_job(job: &DbJobsObj) -> bool {
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
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use shared_types::{GenericNamespaceObj, ScraperParam, Tag};

    use crate::db::turso::TursoDatabase;
    use crate::plugins::PluginManager;

    use super::*;

    async fn new_test_db() -> (Arc<TursoDatabase>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("system_jobs.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        (db, temp_dir)
    }

    /// A database with a plugin manager that has no sites registered, so
    /// `site_for_url` always misses. Lets the walk be exercised end to end
    /// without depending on a compiled plugin being present in the test
    /// environment; the plugin-miss branch is what it lands on.
    async fn new_test_db_with_empty_plugins() -> (Arc<TursoDatabase>, tempfile::TempDir) {
        let (db, temp_dir) = new_test_db().await;
        let empty_plugins = tempfile::tempdir().unwrap();
        let should_exit = Arc::new(AtomicBool::new(false));
        let manager = PluginManager::new(empty_plugins.path(), db.clone(), should_exit);
        db.set_plugin_manager(manager);
        (db, temp_dir)
    }

    fn test_file(storage_id: u64, hash: &str) -> FileInternal {
        FileInternal {
            id: None,
            hash: hash.to_string(),
            extension: "jpg".to_string(),
            storage_id,
            size_bytes: Some(1),
        }
    }

    /// Writes a file where `file_on_disk` expects to find it.
    fn write_file_on_disk(root: &std::path::Path, file: &FileInternal) -> std::path::PathBuf {
        let mut path = root.to_path_buf();
        path.push(&file.hash[0..2]);
        path.push(&file.hash[2..4]);
        path.push(&file.hash[4..6]);
        path.push(&file.hash);
        let final_path = path.with_added_extension(&file.extension);
        std::fs::create_dir_all(final_path.parent().unwrap()).unwrap();
        std::fs::write(&final_path, b"payload").unwrap();
        final_path
    }

    fn storage_check_job(param: Option<&str>) -> DbJobsObj {
        DbJobsObj {
            id: 1,
            isrunning: false,
            config: PluginJob {
                time: 0,
                reptime: 0,
                priority: 1,
                recreation: None,
                site: SYSTEM_STORAGE_CHECK_SITE.to_string(),
                param: param
                    .map(|mode| vec![ScraperParam::Normal(mode.to_string())])
                    .unwrap_or_default(),
                user_data: std::collections::BTreeMap::new(),
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_get_page_walks_every_row_once_in_id_order() {
        let (db, _temp_dir) = new_test_db().await;
        let conn = db.connect().unwrap();
        let storage_id = db
            .file_storage_location_get_or_create(&conn, "page_storage")
            .await
            .unwrap();

        let files: Vec<FileInternal> = (0..25)
            .map(|i| test_file(storage_id, &format!("pagehash{i:02}")))
            .collect();
        let inserted = db.file_add_bulk(&conn, &files).await.unwrap();
        let mut all_ids: Vec<u64> = inserted.iter().filter_map(|f| f.id).collect();
        all_ids.sort_unstable();
        assert_eq!(all_ids.len(), 25);

        // Paging must yield every row exactly once, in ascending id order, and
        // a start key in the middle of the table must skip only what is below it.
        let mut seen: Vec<u64> = Vec::new();
        let mut after = 0u64;
        loop {
            let page = db.file_get_page(&conn, after, 7).await.unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 7, "a page must respect its limit");
            // A full row set: the pager selects every column `file_on_disk` needs.
            for file in &page {
                assert!(file.id.is_some());
                assert!(!file.hash.is_empty());
                assert_eq!(file.extension, "jpg");
            }
            after = page.last().unwrap().id.unwrap();
            seen.extend(page.into_iter().filter_map(|f| f.id));
        }
        assert_eq!(seen, all_ids, "paging must cover the table in id order");

        let mid = all_ids[12];
        let tail = db.file_get_page(&conn, mid, 100).await.unwrap();
        assert_eq!(
            tail.iter().filter_map(|f| f.id).collect::<Vec<u64>>(),
            all_ids[13..].to_vec(),
            "a mid-table start key must resume after that id"
        );
        assert!(
            db.file_get_page(&conn, 0, 0).await.unwrap().is_empty(),
            "a zero limit must return nothing"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_present_in_storage_checks_every_root() {
        let roots_dir = tempfile::tempdir().unwrap();
        let primary = roots_dir.path().join("primary");
        let secondary = roots_dir.path().join("secondary");
        std::fs::create_dir_all(&primary).unwrap();
        std::fs::create_dir_all(&secondary).unwrap();

        let mut roots = HashMap::new();
        roots.insert(1u64, primary.to_string_lossy().to_string());
        roots.insert(2u64, secondary.to_string_lossy().to_string());

        let in_primary = test_file(1, "inprimary1");
        write_file_on_disk(&primary, &in_primary);
        assert!(file_present_in_storage(&in_primary, &roots));

        // Recorded against root 2 but actually living in root 1: a library moved
        // between roots must still count as present.
        let moved = test_file(2, "movedroot01");
        write_file_on_disk(&primary, &moved);
        assert!(
            file_present_in_storage(&moved, &roots),
            "a file must be found under any configured root, not only its own"
        );

        let absent = test_file(1, "absentfil1");
        assert!(
            !file_present_in_storage(&absent, &roots),
            "an unwritten file must count as missing"
        );

        // Too short to partition, so it can never resolve to a path.
        let too_short = test_file(1, "abc");
        assert!(!file_present_in_storage(&too_short, &roots));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redownload_job_carries_the_download_manager_contract() {
        let file = FileInternal {
            id: Some(42),
            hash: "contracthash".to_string(),
            extension: "png".to_string(),
            storage_id: 7,
            size_bytes: None,
        };
        let job = redownload_job(
            &file,
            "https://example.test/a".to_string(),
            "site".to_string(),
        );

        assert_eq!(job.site, "site");
        assert_eq!(job.priority, DEFAULT_PRIORITY);
        assert_eq!(job.reptime, 0);
        assert!(job.recreation.is_none());
        assert!(
            matches!(&job.param[0], ScraperParam::Url(url) if url.url == "https://example.test/a"
                && url.local_modifiers.is_empty()),
            "the recovery job must carry the URL for the plugin to parse"
        );

        // These keys are read by name in web/manager; a rename silently turns a
        // recovery job into an ordinary scrape.
        let user_data = &job.user_data;
        assert_eq!(
            user_data.get("INTSCRAPE_REDOWNLOAD").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            user_data
                .get("INTSCRAPE_REDOWNLOAD_FILE_ID")
                .map(String::as_str),
            Some("42")
        );
        assert_eq!(
            user_data
                .get("INTSCRAPE_REDOWNLOAD_HASH")
                .map(String::as_str),
            Some("contracthash")
        );
        assert_eq!(
            user_data
                .get("INTSCRAPE_REDOWNLOAD_STORAGE_ID")
                .map(String::as_str),
            Some("7")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redownload_walk_detects_missing_files_and_resolves_source_urls() {
        let (db, _temp_dir) = new_test_db_with_empty_plugins().await;
        let storage_dir = tempfile::tempdir().unwrap();
        let conn = db.connect().unwrap();
        let storage_id = db
            .file_storage_location_get_or_create(&conn, &storage_dir.path().to_string_lossy())
            .await
            .unwrap();

        // Three recorded files: one on disk, one absent, one whose only tag is
        // not a source_url.
        let present = test_file(storage_id, "presentfil");
        let absent = test_file(storage_id, "absentfil");
        let untagged = test_file(storage_id, "untaggedfil");
        write_file_on_disk(storage_dir.path(), &present);

        let inserted = db
            .file_add_bulk(&conn, &[present.clone(), absent.clone(), untagged.clone()])
            .await
            .unwrap();
        let mut by_hash: HashMap<String, u64> = HashMap::new();
        for file in &inserted {
            by_hash.insert(file.hash.clone(), file.id.unwrap());
        }

        // source_url tag on two of them, a different tag on the third.
        let source_url_tag = Tag {
            name: "https://example.test/missing".to_string(),
            namespace: GenericNamespaceObj {
                name: SOURCE_URL_NAMESPACE.to_string(),
                description: None,
            },
        };
        let other_tag = Tag {
            name: "some-artist".to_string(),
            namespace: GenericNamespaceObj {
                name: "artist".to_string(),
                description: None,
            },
        };
        let tag_set = db
            .tag_add_bulk(&conn, &HashSet::from([source_url_tag, other_tag]))
            .await
            .unwrap();
        let source_url_tag_id = tag_set
            .iter()
            .find(|t| t.namespace_id > 0 && t.name == "https://example.test/missing")
            .map(|t| t.id as u64)
            .unwrap();
        let other_tag_id = tag_set
            .iter()
            .find(|t| t.name == "some-artist")
            .map(|t| t.id as u64)
            .unwrap();

        let rels: HashSet<(u64, u64)> = HashSet::from([
            (by_hash[&absent.hash], source_url_tag_id),
            (by_hash[&present.hash], source_url_tag_id),
            (by_hash[&untagged.hash], other_tag_id),
        ]);
        let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
        db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();
        drop(conn);

        let summary = db.redownload_missing_files().await.unwrap();

        assert_eq!(
            summary.missing_files, 2,
            "both the absent file and the untagged one are off disk; the present one is not"
        );
        assert_eq!(
            summary.no_source_url, 1,
            "the untagged missing file has no source_url tag to recover from"
        );
        assert_eq!(
            summary.no_plugin, 1,
            "the other missing file has a source_url, but no site is registered in this test"
        );
        assert_eq!(
            summary.queued_jobs, 0,
            "a plugin miss must not queue a job with an unusable site"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redownload_job_dispatch_only_implements_the_redownload_mode() {
        let (db, _temp_dir) = new_test_db_with_empty_plugins().await;

        // The CLI can enqueue these, and they have no turso implementation, so
        // they must report failure rather than silently completing as a no-op.
        assert!(
            !db.run_storage_check_job(&storage_check_job(None)).await,
            "the default StorageCheck mode is not implemented"
        );
        assert!(
            !db.run_storage_check_job(&storage_check_job(Some(SYSTEM_STORAGE_CHECK_FILENAME_MODE)))
                .await,
            "the filename mode is not implemented"
        );
        // The implemented mode must succeed (the library is empty, so it queues
        // nothing) and must not be mistaken for the unimplemented ones.
        assert!(
            db.run_storage_check_job(&storage_check_job(Some(
                SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE
            )))
            .await,
            "the redownload mode is implemented and must report success"
        );
    }
}
