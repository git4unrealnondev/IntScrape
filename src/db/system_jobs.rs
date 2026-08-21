use std::error::Error;
use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use crate::backup_path::dated_backup_destination;
use crate::web::manager::hash_bytes;
use bytes::Bytes;
use rayon::prelude::*;
use rusqlite::{params, params_from_iter};
use shared_types::{DbJobRecreation, DbJobsObj, HashesSupported, SQL_CHUNK_SIZE, ScraperParam};
use strum::IntoEnumIterator;

use super::{
    MainDatabase, SYSTEM_DATABASE_BACKUP_SITE, SYSTEM_DATABASE_SLURP_SITE, SYSTEM_FILE_HASH_SITE,
    SYSTEM_FILE_SIZE_SITE,
};

impl MainDatabase {
    /// Runs system jobs
    pub async fn run_system_job(&self, job: &DbJobsObj) -> bool {
        if self.should_exit.load(std::sync::atomic::Ordering::SeqCst) {
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
            _ => return false,
        };

        if self.should_exit.load(std::sync::atomic::Ordering::SeqCst) {
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

    async fn run_slurp_job(&self, job: &DbJobsObj) -> bool {
        let Some(ScraperParam::Normal(source)) = job.config.param.first() else {
            log::error!("Database slurp job {} has no source path", job.id);
            return false;
        };

        match self.db_slurp(Path::new(source)) {
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

    pub async fn hash_missing_file_hashes(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.pool.clone();
        let should_exit_clone = self.should_exit.clone();
        let writer_conn_clone = self.writer_conn.clone();

        let result = tokio::task::spawn_blocking(
            move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

                let conn = pool.get()?;

    let mut cnt = 0;
                loop {

// Safe to assume that if we're missing one hash then we need everything else
                let mut stmt = conn.prepare(
                    &format!("SELECT id 
             FROM File f 
             WHERE NOT EXISTS (
                 SELECT 1 
                 FROM FileHashes fh 
                 WHERE fh.file_id = f.id
             ) LIMIT {} OFFSET {};", SQL_CHUNK_SIZE, cnt),
                )?;

                    info!("System file hasher has hashed: {} files.", &cnt);


                    let file_ids_to_work_on: Vec<u64> = stmt
                    .query_map([], |row| Ok(row.get::<_, u64>(0)?))?
                    .flatten()
                    .collect();
                    cnt += SQL_CHUNK_SIZE;

                    if file_ids_to_work_on.is_empty() {
                        break;
                    }

                    if should_exit_clone.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }

                    let file_id_file_paths: HashMap<u64, String> = file_ids_to_work_on
                        .iter()
                        .filter_map(|f| {
                            if let Some(file_path) =
                                Self::internal_file_get_physical_path(&conn, f)
                                    .ok()
                                    .flatten()
                            {
                                Some((*f, file_path))
                            } else {
                                None
                            }
                        })
                        .collect();

                    let processed = file_id_file_paths
                        .par_iter()
                        .try_fold(Vec::new, |mut pending, (file_id, file_path)| {
                            if should_exit_clone.load(std::sync::atomic::Ordering::SeqCst) {
                                return Err(());
                            }
                            if let Ok(bytes) = std::fs::read(file_path) {
                                let bytes = Bytes::from(bytes);

                                for (algorithm_name, hashessupported) in algorithms.iter() {
                                    pending.push((
                                        *file_id,
                                        algorithm_name,
                                        hash_bytes(&bytes, hashessupported).0,
                                    ));
                                }
                            }

                            Ok(pending)
                        })
                        .try_reduce(Vec::new, |mut left, mut right| {
                            left.append(&mut right);
                            Ok(left)
                        });
                    let pending = match processed {
                        Ok(pending) => pending,
                        Err(()) => {
                            break;
                        }
                    };

                    // This is just to make sure that we're not going over some chunk size
                    for pending_list in pending.chunks(SQL_CHUNK_SIZE.try_into().unwrap()) {
                        if pending_list.is_empty() {
                            continue;
                        }

                        let mut writer = writer_conn_clone.lock();
                        let conn = writer.transaction()?;

                        let placeholders: String = pending_list
                            .iter()
                            .map(|_| "(?, ?, ?)")
                            .collect::<Vec<_>>()
                            .join(", ");

                        let sql = format!(
                            "INSERT OR IGNORE INTO FileHashes (file_id, algorithm, digest) VALUES {}",
                            placeholders
                        );

                        let mut flat_params: Vec<&dyn rusqlite::ToSql> =
                            Vec::with_capacity(pending_list.len() * 3);
                        for (file_id, algorithm, digest) in pending_list {
                            flat_params.push(file_id);
                            flat_params.push(algorithm);
                            flat_params.push(digest);
                        }

                        {
                            let mut stmt = conn.prepare(&sql)?;
                            stmt.execute(rusqlite::params_from_iter(flat_params))?;
                        }
                        conn.commit()?;
                    }
                    log::info!("File-hash system updated {} files", pending.len());
                }

                Ok(())
            },
        )
        .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(Box::new(e)),
        }
    }
    /*
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
    }*/
}

pub(crate) fn is_system_job(job: &DbJobsObj) -> bool {
    matches!(
        job.config.site.as_str(),
        SYSTEM_DATABASE_BACKUP_SITE
            | SYSTEM_DATABASE_SLURP_SITE
            | SYSTEM_FILE_SIZE_SITE
            | SYSTEM_FILE_HASH_SITE
    ) || matches!(
        job.config.recreation,
        Some(DbJobRecreation::AlwaysTime(_, _))
    ) && job.config.site.starts_with("SYSTEM_")
}
