use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use crate::backup_path::dated_backup_destination;
use crate::web::manager::hash_bytes;
use bytes::Bytes;
use rayon::prelude::*;
use shared_types::{DbJobRecreation, DbJobsObj, HashesSupported, ScraperParam};
use strum::IntoEnumIterator;

use super::{
    MainDatabase, SYSTEM_DATABASE_BACKUP_SITE, SYSTEM_FILE_HASH_SITE, SYSTEM_FILE_SIZE_SITE,
};

impl MainDatabase {
    /// Runs system jobs
    pub async fn run_system_job(&self, job: &DbJobsObj) -> bool {
        let success = match job.config.site.as_str() {
            SYSTEM_DATABASE_BACKUP_SITE => self.run_backup_job(job).await,
            SYSTEM_FILE_SIZE_SITE => match self.update_missing_file_sizes().await {
                Ok(updated) => {
                    log::info!("File-size system job {} updated {} files", job.id, updated);
                    true
                }
                Err(error) => {
                    log::error!("File-size system job {} failed: {error}", job.id);
                    false
                }
            },
            SYSTEM_FILE_HASH_SITE => match self.hash_missing_file_hashes().await {
                Ok(updated) => {
                    log::info!("File-hash system job {} updated {} files", job.id, updated);
                    true
                }
                Err(error) => {
                    log::error!("File-hash system job {} failed: {error}", job.id);
                    false
                }
            },
            _ => return false,
        };
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
        match self.backup_db_to(Path::new(&destination)) {
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

    /// Computes supported secondary hashes for files missing them in FileHashes.
    pub async fn hash_missing_file_hashes(&self) -> Result<u64, r2d2_sqlite::rusqlite::Error> {
        let pool = self.pool.clone();
        let candidates = tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(|error| {
                r2d2_sqlite::rusqlite::Error::ToSqlConversionFailure(error.into())
            })?;
            let storage = Self::internal_file_storage_get_all(&conn)?;
            let mut stmt = conn.prepare(
                "SELECT f.id, f.hash, f.extension, f.storage_id, fh.algorithm
                 FROM File f
                 LEFT JOIN FileHashes fh ON fh.file_id = f.id
                 WHERE f.hash IS NOT NULL AND f.extension IS NOT NULL",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?;

            let mut files: HashMap<u64, (String, String, PathBuf, HashSet<String>)> =
                HashMap::new();
            for row in rows {
                let (id, hash, extension, storage_id, algorithm) = row?;
                let Some(base_path) = storage.get(&storage_id) else {
                    continue;
                };
                let Some(path) = Self::get_file_location(
                    &shared_types::FileInternal {
                        id: Some(id),
                        hash: hash.clone(),
                        extension: extension.clone(),
                        storage_id,
                        size_bytes: None,
                    },
                    base_path,
                ) else {
                    continue;
                };
                let entry = files
                    .entry(id)
                    .or_insert_with(|| (hash, extension, path, HashSet::new()));
                if let Some(algorithm) = algorithm {
                    entry.3.insert(algorithm);
                }
            }

            let algorithms: Vec<(&'static str, HashesSupported)> = HashesSupported::iter()
                .map(|hash| match hash {
                    HashesSupported::Md5(_) => ("MD5", HashesSupported::Md5(String::new())),
                    HashesSupported::Sha1(_) => ("SHA1", HashesSupported::Sha1(String::new())),
                    HashesSupported::Sha256(_) => {
                        ("SHA256", HashesSupported::Sha256(String::new()))
                    }
                    HashesSupported::Sha512(_) => {
                        ("SHA512", HashesSupported::Sha512(String::new()))
                    }
                    HashesSupported::IPFSCID(_) => {
                        ("IPFSCID", HashesSupported::IPFSCID(String::new()))
                    }
                    HashesSupported::IPFSCID1(_) => {
                        ("IPFSCID1", HashesSupported::IPFSCID1(String::new()))
                    }
                    HashesSupported::ImageHash(_) => {
                        ("ImageHash", HashesSupported::ImageHash(String::new()))
                    }
                })
                .collect();
            let computed: Vec<_> = files
                .into_par_iter()
                .flat_map_iter(|(file_id, (_, _, path, existing))| {
                    let bytes = match std::fs::read(path) {
                        Ok(bytes) => Bytes::from(bytes),
                        Err(error) => {
                            log::warn!("Cannot read file {file_id} while hashing: {error}");
                            return Vec::new();
                        }
                    };

                    algorithms
                        .iter()
                        .filter(|(algorithm, _)| !existing.contains(*algorithm))
                        .map(|(algorithm, kind)| {
                            (
                                file_id,
                                (*algorithm).to_string(),
                                hash_bytes(&bytes, kind).0,
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            Ok::<_, r2d2_sqlite::rusqlite::Error>(computed)
        })
        .await
        .map_err(|error| r2d2_sqlite::rusqlite::Error::ToSqlConversionFailure(error.into()))??;

        let updated = candidates
            .iter()
            .map(|(file_id, _, _)| *file_id)
            .collect::<HashSet<_>>()
            .len() as u64;
        if candidates.is_empty() {
            return Ok(0);
        }
        let mut writer = self.writer_conn.lock();
        let tx = writer.transaction()?;
        for (file_id, algorithm, digest) in candidates {
            Self::internal_file_hash_add(&tx, &algorithm, &digest, &file_id)?;
        }
        tx.commit()?;
        Ok(updated)
    }
}

pub(crate) fn is_system_job(job: &DbJobsObj) -> bool {
    matches!(
        job.config.site.as_str(),
        SYSTEM_DATABASE_BACKUP_SITE | SYSTEM_FILE_SIZE_SITE | SYSTEM_FILE_HASH_SITE
    ) || matches!(
        job.config.recreation,
        Some(DbJobRecreation::AlwaysTime(_, _))
    ) && job.config.site.starts_with("SYSTEM_")
}
