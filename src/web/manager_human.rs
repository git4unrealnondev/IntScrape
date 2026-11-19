use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use log::info;
use priority_semaphore::PrioritySemaphore;
use rayon::ThreadPool;
use shared_types::*;
use std::io::Write;
use tokio::sync::RwLock;

use crate::{
    db::turso::TursoDatabase,
    plugins::PluginManager,
    web::{MAX_CONCURRENT_JOBS, manager::semaphore_priority, ratelimit::RatelimitManager},
};

pub struct DownloadManager {
    db: Arc<TursoDatabase>,
    plugin_manager: Arc<PluginManager>,
    heavy_processing_pool: Arc<ThreadPool>,
    should_exit: Arc<AtomicBool>,
    plugin_storage: RwLock<HashMap<String, PluginStorage>>,
}

#[derive(Clone)]
pub struct PluginStorage {
    download_manager: Arc<DownloadManager>,
    plugin: Plugin,
    jobs: Arc<RwLock<Vec<DbJobsObj>>>,
    ratelimit: Arc<RatelimitManager>,
    // Limits the number of jobs that can run concurrently
    jobs_concurrency_limiter: Arc<PrioritySemaphore>,
    // Limits the number of concurrent web requests
    web_reach_concurrency_limiter: Arc<PrioritySemaphore>,
}

impl PluginStorage {
    /// Runs jobs in a loop
    /// Adds jobs in order of priority
    pub async fn start_work(self: Arc<Self>) {
        loop {
            // This shouldn't happen but it catches stupid
            if self.jobs.read().await.is_empty() {
                info!(
                    "DownloadManager: No jobs remaining in memory for {}. Retiring worker loop.",
                    self.plugin.name
                );
                return;
            } else if self
                .download_manager
                .should_exit
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                info!("DownloadManager: Stopping due to early should exit call");
                return;
            }

            // Runs job with highest priority first
            let job_to_run_idx = self
                .jobs
                .read()
                .await
                .iter()
                .enumerate()
                .filter(|(_, job)| !job.isrunning)
                .max_by(|(_, left), (_, right)| {
                    left.config
                        .priority
                        .cmp(&right.config.priority)
                        .then_with(|| right.config.time.cmp(&left.config.time))
                        .then_with(|| right.id.cmp(&left.id))
                })
                .map(|(idx, _)| idx);

            if let Some(job_idx) = job_to_run_idx
                && let Some(job) = self.jobs.write().await.get_mut(job_idx)
            {
                job.isrunning = true;

                if !job.isrunning {
                    info!("DownloadManager: Setting job {} to running status.", job.id);
                    job.isrunning = true;
                }

                let job_semaphore = semaphore_priority(job.config.priority);
                let concurrency_limiter =
                    self.jobs_concurrency_limiter.acquire(job_semaphore).await;

                let _permit = match concurrency_limiter {
                    Ok(permit) => permit,
                    Err(err) => {
                        log::error!("Job limiter closed; dropping job {}: {err}", job.id);
                        return;
                    }
                };

                // Adds cookies into job
                for logins in self.load_logins(&self.plugin) {
                    let same_login = |existing: &ScraperParam| match (existing, &logins) {
                        (
                            ScraperParam::Login(LoginType::Cookie(a, _)),
                            ScraperParam::Login(LoginType::Cookie(b, _)),
                        ) => a == b,
                        _ => existing == &logins,
                    };
                    job.config.param.retain(|p| !same_login(p));
                    job.config.param.push(logins);
                }

                let _ = tokio::spawn(self.clone().run_job(job.clone())).await;
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    /// Handles actually running a scraper job
    async fn run_job(self: Arc<Self>, job: DbJobsObj) -> Result<(), Box<dyn Error + Send>> {
        let local_job = job.clone();
        let local_self = self.clone();
        let plugin_data_return = match tokio::task::spawn_blocking(move || {
            local_self.download_manager.plugin_manager.url_dump(
                &ScraperDataReturn {
                    job: local_job.config.clone(),
                    ..Default::default()
                },
                &local_self.plugin,
            )
        })
        .await
        {
            Ok(Ok(data)) => data,
            Ok(Err(err)) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "DownloadManager: Encountered err: {err} while processing JobId: {}",
                        job.id
                    ),
                )));
            }
            Err(err) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Url dump had join error for JobId: {} {err}", job.id),
                )));
            }
        };

        'scraperloop: for scraper_data in plugin_data_return.iter() {
            // Stops processing asap
            if self
                .download_manager
                .should_exit
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                break 'scraperloop;
            }

            // Skips job if we have pre-existing conditions
            if self
                .download_manager
                .db
                .should_skip_processing_job(scraper_data.skip_conditions.clone())
                .await
            {
                continue 'scraperloop;
            }

            // Only send urls & url_posts to the dltext method
            for url_param in scraper_data
                .job
                .param
                .iter()
                .filter(|pred| matches!(pred, ScraperParam::Url(_) | ScraperParam::UrlPost(_)))
            {
            }

            dbg!(&scraper_data);
        }

        Ok(())
    }

    ///
    /// Loads the logins in from the DB and inserts them into the job parameters.
    ///
    fn load_logins(&self, plugin: &Plugin) -> Vec<ScraperParam> {
        let mut out = Vec::new();

        for property in &plugin.properties {
            if let PluginProperties::Login((_loginneed, logintype)) = property
                && let LoginType::Cookie(name, _) = logintype
                && let Some(api_key) = self
                    .download_manager
                    .db
                    .setting_get_sync_blocking(&format!("PLUGIN_{}_{}_COOKIE", plugin.name, name))
            {
                out.push(ScraperParam::Login(LoginType::Cookie(
                    name.clone(),
                    Some(api_key.param.clone().unwrap_or_default()),
                )));
            }
        }

        if let Some(api_key) = self
            .download_manager
            .db
            .setting_get_sync_blocking(&format!("PLUGIN_{}_API_KEY", plugin.name))
            && let Some(api_pass) = self
                .download_manager
                .db
                .setting_get_sync_blocking(&format!("PLUGIN_{}_API_PASS", plugin.name))
        {
            out.push(ScraperParam::Login(LoginType::Api(
                api_key.param.clone().unwrap_or_default(),
                api_pass.param,
            )));
        }

        out
    }
}

impl DownloadManager {
    /// Makes a new download manager
    pub fn new(
        db: Arc<TursoDatabase>,
        plugin_manager: Arc<PluginManager>,
        heavy_processing_pool: Arc<ThreadPool>,
        should_exit: Arc<AtomicBool>,
    ) -> Arc<Self> {
        DownloadManager {
            db,
            plugin_manager,
            heavy_processing_pool,
            should_exit,
            plugin_storage: RwLock::new(HashMap::new()),
        }
        .into()
    }

    /// Checks if any jobs are still waiting in the system
    pub async fn all_jobs_complete(&self) -> bool {
        for scraper in self.plugin_storage.read().await.values() {
            if !scraper.jobs.read().await.is_empty() {
                return false;
            }
        }
        true
    }

    /// Gets total count of jobs in memory
    pub async fn in_memory_job_count(&self, plugin_name: &str) -> usize {
        let mut count = 0;
        if let Some(plugin) = self.plugin_storage.read().await.get(plugin_name) {
            count += plugin.jobs.read().await.len();
        }

        count
    }

    /// Handles adding settings into the DB if they're required
    fn prompt_plugin_login(
        &self,
        plugin: &Plugin,
        include_optional: bool,
        overwrite: bool,
    ) -> usize {
        let exists = |key: &str| self.db.setting_get_sync_blocking(key).is_some();
        let mut stored = 0usize;

        for property in &plugin.properties {
            let PluginProperties::Login((login_need, login_type)) = property else {
                continue;
            };
            if *login_need == LoginNeed::Optional && !include_optional {
                continue;
            }

            match login_type {
                LoginType::Api(key, _api) => {
                    let setting_key = format!("PLUGIN_{}_API_KEY", plugin.name);
                    if overwrite || !exists(&setting_key) {
                        if !key.is_empty() {
                            println!("{key}");
                        }
                        let user_name = read_login_line("Api Key: ");
                        let user_pass = read_login_line("Password: ");
                        self.db.setting_set_sync_blocking(&DbSettingsObj {
                            name: format!("PLUGIN_{}_API_KEY", plugin.name),
                            description: Some("API Login for site.".into()),
                            num: None,
                            param: Some(user_name),
                        });
                        self.db.setting_set_sync_blocking(&DbSettingsObj {
                            name: format!("PLUGIN_{}_API_PASS", plugin.name),
                            description: Some("API Login for site.".into()),
                            num: None,
                            param: Some(user_pass),
                        });
                        stored += 2;
                    }
                }
                LoginType::ApiNamespaced(ns, key, _api) => {
                    let setting_key = format!("PLUGIN_{}_{}_API_NS", plugin.name, ns);
                    if overwrite || !exists(&setting_key) {
                        if let Some(help) = key.as_deref().or(_api.as_deref()) {
                            println!("{help}");
                        }
                        let user_name = read_login_line("Api Key: ");
                        let user_pass = read_login_line("Password: ");
                        self.db.setting_set_sync_blocking(&DbSettingsObj {
                            name: setting_key,
                            description: Some("API Login for site.".into()),
                            num: None,
                            param: Some(user_name),
                        });
                        stored += 1;
                        let _ = user_pass;
                    }
                }
                LoginType::Cookie(cookie_name, help_text) => {
                    let setting_key = format!("PLUGIN_{}_{}_COOKIE", plugin.name, cookie_name);
                    if overwrite || !exists(&setting_key) {
                        if let Some(help) = help_text {
                            println!("{help}");
                        }
                        let value = read_login_line("Cookie: ");
                        self.db.setting_set_sync_blocking(&DbSettingsObj {
                            name: setting_key,
                            description: Some("Cookie for site..".into()),
                            num: None,
                            param: Some(value),
                        });
                        stored += 1;
                    }
                }
                _ => {}
            }
        }

        stored
    }

    /// Handles any missing login details
    fn handle_login_db(&self, plugin: &Plugin) {
        let _stored = self.prompt_plugin_login(plugin, false, false);

        // Optional logins are never prompted for at startup (they are not
        // needed for most jobs). Point the user at the CLI adder instead.
        let missing_optional = plugin
            .properties
            .iter()
            .filter_map(|p| match p {
                PluginProperties::Login((LoginNeed::Optional, login_type)) => Some(login_type),
                _ => None,
            })
            .any(|login_type| {
                login_setting_keys(plugin, login_type)
                    .iter()
                    .any(|k| self.db.setting_get_sync_blocking(k).is_none())
            });
        if missing_optional {
            println!(
                "{}: an optional login is not set; run `intscrape tasks login {}` to add one.",
                plugin.name,
                plugin.name.to_ascii_lowercase()
            );
        }
    }

    /// Adds jobs into the system
    pub async fn add_jobs(
        self: &Arc<Self>,
        jobs: HashMap<shared_types::Plugin, Vec<shared_types::DbJobsObj>>,
    ) {
        let mut plugin_storage_guard = self.plugin_storage.write().await;

        for (plugin, db_jobs) in jobs {
            if let Some(plugin_storage) = plugin_storage_guard.get_mut(&plugin.name) {
                for db_jobs_new in db_jobs {
                    if !plugin_storage
                        .jobs
                        .read()
                        .await
                        .iter()
                        .any(|a| a.id == db_jobs_new.id)
                    {
                        plugin_storage.jobs.write().await.push(db_jobs_new);
                    }
                }

            // Adds plugin manager if it doesn't exist
            } else {
                // Sets up ratelimit for job
                let mut ratelimit = None;
                for properties in &plugin.properties {
                    if let PluginProperties::Ratelimit(num, duration) = properties {
                        info!(
                            "DownloadManager: Creating Ratelimiter with properties: {num} tries per: {duration:?}"
                        );

                        ratelimit = Some(RatelimitManager::new(*num, *duration));
                    }
                }
                if ratelimit.is_none() {
                    let local_ratelimit = RatelimitManager::default();

                    info!(
                        "DownloadManager: Creating Ratelimiter with properties: {} tries per: {:?}",
                        local_ratelimit.get_params().0,
                        local_ratelimit.get_params().1
                    );
                    ratelimit = Some(local_ratelimit);
                }

                // Check if we need to load login data
                let _ = self.handle_login_db(&plugin);

                let max_concurrent_jobs = plugin
                    .properties
                    .iter()
                    .find_map(|property| match property {
                        PluginProperties::JobNum(job_num) => (*job_num).try_into().ok(),
                        _ => None,
                    })
                    .unwrap_or(MAX_CONCURRENT_JOBS);

                let max_concurrent_web_reach = plugin
                    .properties
                    .iter()
                    .find_map(|property| match property {
                        PluginProperties::ThreadNum(thread_num) => (*thread_num).try_into().ok(),
                        _ => None,
                    })
                    .unwrap_or(MAX_CONCURRENT_JOBS);

                let plugin_storage = PluginStorage {
                    download_manager: self.clone(),
                    plugin: plugin.clone(),
                    jobs: Arc::new(RwLock::new(db_jobs)),
                    ratelimit: Arc::new(ratelimit.unwrap()),
                    jobs_concurrency_limiter: Arc::new(PrioritySemaphore::new(max_concurrent_jobs)),
                    web_reach_concurrency_limiter: Arc::new(PrioritySemaphore::new(
                        max_concurrent_web_reach,
                    )),
                };

                plugin_storage_guard.insert(plugin.name, plugin_storage.clone());

                tokio::task::spawn(async move {
                    Arc::new(plugin_storage).start_work().await;
                });
            }
        }
    }
}

/// Setting keys a login type is stored under.
fn login_setting_keys(plugin: &Plugin, login_type: &LoginType) -> Vec<String> {
    match login_type {
        LoginType::Cookie(name, _) => vec![format!("PLUGIN_{}_{}_COOKIE", plugin.name, name)],
        LoginType::ApiNamespaced(ns, _, _) => {
            vec![format!("PLUGIN_{}_{}_API_NS", plugin.name, ns)]
        }
        LoginType::Api(_, _) => vec![
            format!("PLUGIN_{}_API_KEY", plugin.name),
            format!("PLUGIN_{}_API_PASS", plugin.name),
        ],
        _ => vec![],
    }
}

/// Reads user input from the terminal need to make this automagic with a cli or something
fn read_login_line(prompt: &str) -> String {
    print!("{prompt}");
    let _ = std::io::stdout().flush();
    let mut buf = String::new();
    let _ = std::io::stdin().read_line(&mut buf);
    while buf.ends_with(['\n', '\r']) {
        buf.pop();
    }
    buf
}
