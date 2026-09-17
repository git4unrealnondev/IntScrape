use std::path::Path;
use std::time::SystemTime;

use bytes::Bytes;
use rayon::prelude::*;
use shared_types::{DbJobRecreation, DbJobsObj, HashesSupported, ScraperParam};
use strum::IntoEnumIterator;

use crate::backup_path::dated_backup_destination;
use crate::db::{
    SQL_CHUNK_SIZE, SYSTEM_DATABASE_BACKUP_SITE, SYSTEM_DATABASE_SLURP_SITE, SYSTEM_FILE_HASH_SITE,
    SYSTEM_FILE_SIZE_SITE, SYSTEM_STORAGE_CHECK_SITE,
};
use crate::web::manager::hash_bytes;

use super::TursoDatabase;

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
            SYSTEM_STORAGE_CHECK_SITE => {
                log::warn!(
                    "Storage-check system job {} is not yet supported on the turso backend",
                    job.id,
                );
                false
            }
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
                HashesSupported::IPFSCID(_) => ("IPFSCID", HashesSupported::IPFSCID(String::new())),
                HashesSupported::IPFSCID1(_) => {
                    ("IPFSCID1", HashesSupported::IPFSCID1(String::new()))
                }
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
