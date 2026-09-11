use std::collections::BTreeMap;

use log::info;
use shared_types::{DbJobRecreation, DbJobsObj, PluginJob, ScraperParam};
use turso::{Connection, Error, Result, Row, Value, params_from_iter};

use crate::db::{SQL_CHUNK_SIZE, turso::TursoDatabase};
use crate::helper_functions::get_sys_time_in_secs;

impl TursoDatabase {
    /// Updates an existing job row.
    pub(in crate::db::turso) async fn jobs_update_sql(
        &self,
        conn: &Connection,
        job: &DbJobsObj,
    ) -> Result<()> {
        conn.execute(
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
            (
                job.config.time as i64,
                job.config.reptime as i64,
                job.config.priority as i64,
                job.isrunning,
                serde_json::to_string(&job.config.recreation).unwrap(),
                job.config.site.clone(),
                serde_json::to_string(&job.config.param).unwrap(),
                serde_json::to_string(&job.config.user_data).unwrap(),
                job.id as i64,
            ),
        )
        .await?;

        Ok(())
    }

    /// Gets jobs that should be run.
    pub(in crate::db::turso) async fn jobs_get_torun_sql(
        &self,
        conn: &Connection,
        sites: Vec<String>,
    ) -> Result<Vec<DbJobsObj>> {
        self.jobs_get_torun_chunk_sql(conn, sites, usize::MAX).await
    }

    /// Gets jobs that should be run, up to `chunk_size` of them.
    pub(in crate::db::turso) async fn jobs_get_torun_chunk_sql(
        &self,
        conn: &Connection,
        sites: Vec<String>,
        chunk_size: usize,
    ) -> Result<Vec<DbJobsObj>> {
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

            let mut query_params: Vec<Value> = sites
                .iter()
                .map(|site| Value::from(site.as_str()))
                .collect();
            query_params.push(Value::from(now as i64));
            query_params.push(Value::from(limit));

            let mut rows = conn.query(&query, params_from_iter(query_params)).await?;
            while let Some(row) = rows.next().await? {
                out.push(job_from_row(&row)?);
            }
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

    /// Sets ALL jobs to be not running.
    pub(in crate::db::turso) async fn jobs_reset_isrunning_sql(
        &self,
        conn: &Connection,
    ) -> Result<()> {
        conn.execute(
            "UPDATE Jobs SET is_running = false WHERE is_running IS true;",
            (),
        )
        .await?;

        Ok(())
    }

    /// Sets a specific job to be running.
    pub(in crate::db::turso) async fn job_set_isrunning_sql(
        &self,
        conn: &Connection,
        job_id: u64,
    ) -> Result<()> {
        conn.execute(
            "UPDATE Jobs SET is_running = true WHERE id IS ?1;",
            (job_id as i64,),
        )
        .await?;

        Ok(())
    }

    /// Removes a specific job.
    pub(in crate::db::turso) async fn job_remove_sql(
        &self,
        conn: &Connection,
        job_id: u64,
    ) -> Result<()> {
        info!("JobId: {job_id} Is being removed.");
        conn.execute("DELETE FROM Jobs WHERE id IS ?1;", (job_id as i64,))
            .await?;

        Ok(())
    }

    /// Gets all jobs associated with a site.
    pub(in crate::db::turso) async fn jobs_get_site_sql(
        &self,
        conn: &Connection,
        site: &str,
    ) -> Result<Vec<DbJobsObj>> {
        let mut rows = conn
            .query(
                "SELECT id, time, reptime, priority, recreation, site, param, user_data, is_running
                 FROM Jobs
                 WHERE site = ?1;",
                (site,),
            )
            .await?;

        let mut jobs = Vec::new();
        while let Some(row) = rows.next().await? {
            jobs.push(job_from_row(&row)?);
        }

        Ok(jobs)
    }

    /// Inserts a job, or updates the duplicate and returns its existing id.
    pub(in crate::db::turso) async fn job_add_sql(
        &self,
        conn: &Connection,
        config: &PluginJob,
    ) -> Result<u64> {
        let param_json = serde_json::to_string(&config.param).unwrap();
        let user_data_json = serde_json::to_string(&config.user_data).unwrap();
        let recreation_json = serde_json::to_string(&config.recreation).unwrap();

        let mut stmt = conn
            .prepare(
                "INSERT INTO Jobs (time, reptime, priority, recreation, site, param, user_data)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(time, reptime, site, param) DO UPDATE SET
                     reptime = excluded.reptime,
                     priority = excluded.priority,
                     user_data = excluded.user_data
                 RETURNING id",
            )
            .await?;

        let id: u64 = stmt
            .query_row((
                config.time as i64,
                config.reptime as i64,
                config.priority as i64,
                recreation_json,
                config.site.clone(),
                param_json,
                user_data_json,
            ))
            .await?
            .get(0)?;

        Ok(id)
    }

    /// Gets all sites currently in db from Jobs.
    pub(in crate::db::turso) async fn jobs_get_all_sites_sql(
        &self,
        conn: &Connection,
    ) -> Result<Vec<String>> {
        let mut rows = conn
            .query("SELECT DISTINCT site FROM Jobs WHERE site IS NOT NULL;", ())
            .await?;

        let mut sites = Vec::new();
        while let Some(row) = rows.next().await? {
            sites.push(row.get::<String>(0)?);
        }

        Ok(sites)
    }
}

impl TursoDatabase {
    /// Inserts a job (or updates its duplicate) and returns the id.
    pub async fn jobs_add_single(&self, job: PluginJob) -> u64 {
        let result = self
            .retry_mvcc(|| async {
                let conn = self.connect()?;
                self.job_add_sql(&conn, &job).await
            })
            .await;
        match result {
            Ok(id) => id,
            Err(error) => {
                log::error!("Failed to insert or update job for site '{}': {error}", job.site);
                0
            }
        }
    }

    /// Updates an existing job row.
    pub async fn jobs_update(&self, job: &DbJobsObj) -> Result<()> {
        self.retry_mvcc(|| async {
            let conn = self.connect()?;
            self.jobs_update_sql(&conn, job).await
        })
        .await
    }

    /// Marks a job as running, returning whether the claim succeeded.
    pub async fn job_set_is_running(&self, job: &DbJobsObj) -> bool {
        loop {
            let conn = match self.connect() {
                Ok(conn) => conn,
                Err(error) => {
                    log::error!(
                        "Failed to connect while claiming job {}: {error}",
                        job.id
                    );
                    return false;
                }
            };

            if let Err(error) = conn.execute("BEGIN CONCURRENT", ()).await {
                if Self::is_concurrency_conflict(&error) {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                log::error!("Failed to begin claim transaction for job {}: {error}", job.id);
                return false;
            }

            if let Err(error) = self.job_set_isrunning_sql(&conn, job.id).await {
                let _ = conn.execute("ROLLBACK", ()).await;
                if Self::is_concurrency_conflict(&error) {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                log::error!("Failed to mark job {} as running: {error}", job.id);
                return false;
            }

            match conn.execute("COMMIT", ()).await {
                Ok(_) => return true,
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    log::error!("Failed to commit job {} claim: {error}", job.id);
                    return false;
                }
            }
        }
    }

    /// Removes a job.
    pub async fn job_remove(&self, job: &DbJobsObj) -> Result<()> {
        self.retry_mvcc(|| async {
            let conn = self.connect()?;
            self.job_remove_sql(&conn, job.id).await
        })
        .await
    }

    /// Advances a recurring job to its next run, removing it when exhausted.
    pub async fn complete_system_job(&self, job: &DbJobsObj) {
        let mut next = job.clone();
        next.config.time = get_sys_time_in_secs();
        next.isrunning = false;

        if let Some(DbJobRecreation::AlwaysTime(interval, count)) = next.config.recreation.clone() {
            if let Some(remaining) = count {
                if remaining == 0 {
                    let _ = self.job_remove(job).await;
                    return;
                }
                next.config.recreation =
                    Some(DbJobRecreation::AlwaysTime(interval, Some(remaining - 1)));
            }
            next.config.reptime = interval;
            let _ = self.jobs_update(&next).await;
        } else {
            let _ = self.job_remove(job).await;
        }
    }

    /// Gets all jobs associated with a site.
    pub async fn jobs_get_site(&self, site: &str) -> Vec<DbJobsObj> {
        let conn = match self.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while fetching jobs for '{site}': {error}");
                return Vec::new();
            }
        };
        match self.jobs_get_site_sql(&conn, site).await {
            Ok(jobs) => jobs,
            Err(error) => {
                log::error!("Database error fetching jobs for site '{site}': {error}");
                Vec::new()
            }
        }
    }

    /// Gets jobs that can run now, up to `chunk_size` of them.
    pub async fn jobs_get_torun_chunk(&self, sites: Vec<String>, chunk_size: usize) -> Vec<DbJobsObj> {
        if chunk_size == 0 || sites.is_empty() {
            return Vec::new();
        }
        let conn = match self.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while fetching runnable jobs: {error}");
                return Vec::new();
            }
        };
        match self.jobs_get_torun_chunk_sql(&conn, sites, chunk_size).await {
            Ok(jobs) => jobs,
            Err(error) => {
                log::error!("Database error fetching runnable jobs: {error}");
                Vec::new()
            }
        }
    }
}

/// Parses a `Jobs` row into a clean `DbJobsObj`.
///
/// Expects columns in this order:
/// `id, time, reptime, priority, recreation, site, param, user_data, is_running`.
fn job_from_row(row: &Row) -> Result<DbJobsObj> {
    let param_raw: String = row.get(6)?;
    let recreation_raw: String = row.get(4)?;
    let user_data_raw: String = row.get(7)?;

    let param: Vec<ScraperParam> = serde_json::from_str(&param_raw).map_err(serialize_error)?;
    let recreation: Option<DbJobRecreation> =
        serde_json::from_str(&recreation_raw).map_err(serialize_error)?;
    let user_data: BTreeMap<String, String> =
        serde_json::from_str(&user_data_raw).map_err(serialize_error)?;

    Ok(DbJobsObj {
        id: row.get(0)?,
        isrunning: row.get(8)?,
        config: PluginJob {
            time: row.get(1)?,
            reptime: row.get(2)?,
            priority: row.get(3)?,
            site: row.get(5)?,
            recreation,
            param,
            user_data,
        },
    })
}

fn serialize_error(error: serde_json::Error) -> Error {
    Error::ConversionFailure(error.to_string())
}
