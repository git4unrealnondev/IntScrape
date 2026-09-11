//! Database operations for the `jobs` domain.

use super::{MainDatabase, SQL_CHUNK_SIZE};
use crate::helper_functions::get_sys_time_in_secs;
use log::info;
use rusqlite::{Connection, Row, ToSql, params, params_from_iter};
use shared_types::{DbJobRecreation, DbJobsObj, PluginJob, ScraperParam};
use std::collections::BTreeMap;
use std::sync::Arc;

pub trait DbJobsObjExt {
    fn from_row(row: &Row) -> rusqlite::Result<Self>
    where
        Self: Sized;
}

impl DbJobsObjExt for DbJobsObj {
    /// Parses a single database row directly into your clean memory structures
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        // Deserialize the JSON string columns back into native Rust types
        let param_raw: String = row.get("param")?;
        let recreation_raw: String = row.get("recreation")?;
        let user_data_raw: String = row.get("user_data")?;

        let param: Vec<ScraperParam> = serde_json::from_str(&param_raw).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                6, // Column index reference
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;
        let recreation: Option<DbJobRecreation> =
            serde_json::from_str(&recreation_raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    5, // Column index reference
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        let user_data: BTreeMap<String, String> =
            serde_json::from_str(&user_data_raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        // Reconstruct the inner PluginJob config block
        let config = PluginJob {
            time: row.get::<_, i64>("time")? as u64,
            reptime: row.get::<_, i64>("reptime")? as u64,
            priority: row.get::<_, i64>("priority")? as u64,
            site: row.get("site")?,
            recreation,
            param,
            user_data,
        };

        // Reconstruct the master database object
        Ok(Self {
            id: row.get::<_, i64>("id")? as u64,
            isrunning: row.get::<_, bool>("is_running")?,
            config,
        })
    }
}

impl MainDatabase {
    pub fn internal_jobs_update(&self, conn: &Connection, job: &DbJobsObj) {
        let recreation = serde_json::to_string(&job.config.recreation).unwrap();
        let param = serde_json::to_string(&job.config.param).unwrap();
        let user_data = serde_json::to_string(&job.config.user_data).unwrap();

        let _ = conn.execute(
            "UPDATE Jobs 
         SET time = ?1, 
             reptime = ?2, 
             priority = ?3, 
             is_running = ?4, 
             recreation = ?5, 
             site = ?6, 
             param = ?7, 
             user_data = ?8 
         WHERE id = ?9",
            params![
                job.config.time,
                job.config.reptime,
                job.config.priority,
                job.isrunning, // true/false state
                recreation,
                job.config.site,
                param,
                user_data,
                job.id
            ],
        );
    }

    ///
    /// Gets jobs that should be run
    ///
    pub fn internal_jobs_get_torun(
        &self,
        conn: &Connection,
        sites: Vec<String>,
    ) -> Result<Vec<DbJobsObj>, rusqlite::Error> {
        self.internal_jobs_get_torun_chunk(conn, sites, usize::MAX)
    }

    pub fn internal_jobs_get_torun_chunk(
        &self,
        conn: &Connection,
        sites: Vec<String>,
        chunk_size: usize,
    ) -> Result<Vec<DbJobsObj>, rusqlite::Error> {
        if chunk_size == 0 || sites.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let limit = i64::try_from(chunk_size).unwrap_or(i64::MAX);
        let now = get_sys_time_in_secs();

        for sites in sites.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", sites.len())
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT id, time, reptime, priority, recreation, site, param, user_data, is_running
                 FROM Jobs
                 WHERE site IN ({placeholders})
                   AND is_running IS false
                   AND time + reptime <= ?
                 ORDER BY priority DESC, time, id
                 LIMIT ?"
            );
            let mut query_params: Vec<&dyn ToSql> =
                sites.iter().map(|site| site as &dyn ToSql).collect();
            query_params.push(&now);
            query_params.push(&limit);

            let mut stmt = conn.prepare(&query)?;
            let jobs = stmt.query_map(
                params_from_iter(query_params),
                shared_types::DbJobsObj::from_row,
            )?;
            out.extend(jobs.collect::<Result<Vec<_>, _>>()?);
        }

        out.sort_unstable_by(|left, right| {
            right
                .config
                .priority
                .cmp(&left.config.priority)
                .then_with(|| left.config.time.cmp(&right.config.time))
                .then_with(|| left.id.cmp(&right.id))
        });
        out.truncate(chunk_size);
        Ok(out)
    }

    ///
    /// Sets ALL jobs to be not running
    ///
    pub fn internal_jobs_reset_isrunning(conn: &Connection) -> Result<(), rusqlite::Error> {
        conn.execute_batch("UPDATE Jobs SET is_running = false WHERE is_running IS true;")
            .unwrap();

        Ok(())
    }

    ///
    /// Sets a specific jobs to be not running
    ///
    pub fn internal_jobs_set_isrunning(
        &self,
        conn: &Connection,
        job_id: u64,
    ) -> Result<(), rusqlite::Error> {
        conn.execute(
            "UPDATE Jobs SET is_running = true WHERE id IS ?1;",
            params![job_id],
        )?;

        Ok(())
    }

    ///
    /// Removes a specific job
    ///
    pub fn internal_job_remove(
        &self,
        conn: &Connection,
        job_id: u64,
    ) -> Result<(), rusqlite::Error> {
        info!("JobId: {job_id} Is being removed.");
        conn.execute("DELETE FROM Jobs WHERE id IS ?1;", params![job_id])
            .unwrap();

        Ok(())
    }

    ///
    /// Used internally to get all jobs from site
    ///
    pub fn internal_jobs_get_site(
        &self,
        conn: &Connection,
        site: &str,
    ) -> Result<Vec<shared_types::DbJobsObj>, rusqlite::Error> {
        // Select all jobs matching the given site
        let mut stmt = conn.prepare(
            "SELECT id, time, reptime, priority, recreation, site, param, user_data, is_running 
         FROM Jobs 
         WHERE site = ?1;",
        )?;

        // query_map processes each row through a closure safely
        let job_iter = stmt.query_map([site], shared_types::DbJobsObj::from_row)?;

        // Collect the iterator results, propagating any underlying row or parsing errors
        let mut jobs = Vec::new();
        for job_result in job_iter {
            jobs.push(job_result?);
        }

        Ok(jobs)
    }

    pub fn internal_jobs_add(&self, conn: &Connection, config: &PluginJob) -> u64 {
        let mut stmt = conn
            .prepare(
                "INSERT INTO Jobs (time, reptime, priority, recreation, site, param, user_data) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)

ON CONFLICT(time, reptime, site, param) DO UPDATE SET
            reptime = excluded.reptime,      -- Update to the new execution time
            priority = excluded.priority,    -- Update to the new priority level
            user_data = excluded.user_data

         RETURNING id",
            )
            .unwrap();

        // Serialize on-the-fly for the TEXT columns
        let param_json = serde_json::to_string(&config.param).unwrap();
        let user_data_json = serde_json::to_string(&config.user_data).unwrap();
        let manager_json = serde_json::to_string(&config.recreation).unwrap(); // Replace with your actual serialized DbJobsManager struct

        match stmt.query_row(
            params![
                config.time,
                config.reptime,
                config.priority,
                manager_json,
                config.site,
                param_json,
                user_data_json
            ],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(error) => {
                log::error!(
                    "Failed to insert or update job for site '{}': {error}",
                    config.site
                );
                0
            }
        }
    }

    ///
    /// Gets all sites currently in db from Jobs
    ///
    pub fn internal_jobs_get_all_sites(conn: &Connection) -> Result<Vec<String>, rusqlite::Error> {
        // Use DISTINCT so SQLite handles deduplication natively at the engine level
        let mut stmt = conn.prepare("SELECT DISTINCT site FROM Jobs WHERE site IS NOT NULL;")?;

        // Map each row directly to a String extraction
        let site_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;

        // Collect results, propagating any database errors upstream
        site_iter.collect()
    }

    ///
    /// Updates a job inside the db.
    ///
    pub async fn jobs_update(&self, job: &DbJobsObj) {
        let job = job.clone();
        let database = Arc::new(self.clone());
        tokio::task::spawn_blocking(move || {
            let mut pool = database.writer_lock();
            let conn = match pool.transaction() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            database.internal_jobs_update(&conn, &job);
            conn.commit().unwrap();
        })
        .await
        .unwrap();
    }

    pub async fn complete_system_job(&self, job: &DbJobsObj) {
        let mut next = job.clone();
        next.config.time = get_sys_time_in_secs();
        next.isrunning = false;

        if let Some(DbJobRecreation::AlwaysTime(interval, count)) = next.config.recreation.clone() {
            if let Some(remaining) = count {
                if remaining == 0 {
                    self.clone().job_remove(job).await;
                    return;
                }
                next.config.recreation =
                    Some(DbJobRecreation::AlwaysTime(interval, Some(remaining - 1)));
            }
            next.config.reptime = interval;
            self.clone().jobs_update(&next).await;
        } else {
            self.clone().job_remove(job).await;
        }
    }

    ///
    /// Sets a job to be running inside of the db
    ///
    pub async fn job_set_is_running(&self, job: &DbJobsObj) -> bool {
        let job_id = job.id;
        let database = self.clone();
        tokio::task::spawn_blocking(move || {
            let lock_started = std::time::Instant::now();
            let mut writer_lock_guard = database.writer_lock();

            let lock_elapsed = lock_started.elapsed();
            let transaction_started = std::time::Instant::now();
            // A deferred transaction avoids taking SQLite's writer lock until
            // the small UPDATE below actually needs it.
            let transaction = match writer_lock_guard.transaction() {
                Ok(transaction) => transaction,
                Err(error) => {
                    log::error!("Failed to begin transaction while claiming job {job_id} after waiting {lock_elapsed:?}: {error}");
                    return false;
                }
            };

            if let Err(error) = database.internal_jobs_set_isrunning(&transaction, job_id) {
                log::error!("Failed to mark job {job_id} as running: {error}");
                return false;
            }

            if let Err(error) = transaction.commit() {
                log::error!("Failed to commit running status for job {job_id} after transaction setup {transaction_started:?}: {error}");
                return false;
            }

            true
        })
        .await
        .unwrap_or_else(|error| {
            log::error!("Job {job_id} status task failed: {error}");
            false
        })
    }

    ///
    /// Sets a job to be running inside of the db
    ///
    pub async fn job_remove(&self, job: &DbJobsObj) {
        let job_id = job.id;
        let database = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer_lock_guard = database.writer_lock();
            let tn = match writer_lock_guard.transaction() {
                Ok(tn) => tn,
                Err(error) => {
                    log::error!("Failed to begin removal transaction for job {job_id}: {error}");
                    return false;
                }
            };
            if let Err(error) = database.internal_job_remove(&tn, job_id) {
                log::error!("Failed to remove job {job_id}: {error}");
                return false;
            }
            match tn.commit() {
                Ok(()) => true,
                Err(error) => {
                    log::error!("Failed to commit removal of job {job_id}: {error}");
                    false
                }
            }
        })
        .await
        .unwrap();
    }

    ///
    /// Gets all jobs associated with a site
    ///
    pub async fn jobs_get_site(self: Arc<Self>, site: &str) -> Vec<DbJobsObj> {
        let pool = self.pool.clone();
        let database = self.clone();

        let site_owned = site.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    return Vec::new();
                }
            };
            match database.internal_jobs_get_site(&conn, &site_owned) {
                Ok(jobs) => jobs,
                Err(e) => {
                    log::error!("Database error fetching jobs for site '{site_owned}': {e:?}");
                    Vec::new()
                }
            }
        })
        .await
        .unwrap_or_default()
    }

    ///
    /// Gets all jobs that can run
    ///
    pub async fn jobs_get_torun(&self, sites: Vec<String>) -> Vec<DbJobsObj> {
        self.jobs_get_torun_chunk(sites, usize::MAX).await
    }

    pub async fn jobs_get_torun_chunk(
        &self,
        sites: Vec<String>,
        chunk_size: usize,
    ) -> Vec<DbJobsObj> {
        let pool = self.pool.clone();
        let database = self.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    return Vec::new();
                }
            };
            match database.internal_jobs_get_torun_chunk(&conn, sites, chunk_size) {
                Ok(jobs) => jobs,
                Err(e) => {
                    log::error!("Database error fetching jobs: {e:?}");
                    Vec::new()
                }
            }
        })
        .await
        .unwrap_or_default()
    }
}
