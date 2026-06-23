use std::{
    collections::HashMap,
    num::{NonZero, NonZeroU64},
    sync::Arc,
    time::Duration,
};

use governor::{DefaultDirectRateLimiter, Quota};
use log::info;
use reqwest::Client;
use shared_types::*;
use tokio::sync::RwLock;

use crate::{db::MainDatabase, plugins::PluginManager};

pub(in crate::web) struct Scraper {
    pub(in crate::web) job: DbJobsObj,
    pub(in crate::web) ratelimiter: Arc<DefaultDirectRateLimiter>,
    pub(in crate::web) plugin_manager: Arc<PluginManager>,
    pub(in crate::web) plugin: Plugin,
    pub(in crate::web) download_manager: Arc<DownloadsManager>,
    pub(in crate::web) text_client: Arc<Client>,
    pub(in crate::web) file_client: Arc<Client>,
}

#[derive(Clone)]
struct InternalStorage {
    plugin: Plugin,
    job_storage: Vec<DbJobsObj>,
    ratelimiter: Arc<DefaultDirectRateLimiter>,
}

pub struct DownloadsManager {
    db: Arc<MainDatabase>,
    plugin_manager: Arc<PluginManager>,
    jobs: RwLock<HashMap<String, InternalStorage>>,
}

impl Scraper {
    pub fn new(
        job: DbJobsObj,
        ratelimiter: Arc<DefaultDirectRateLimiter>,
        plugin_manager: Arc<PluginManager>,
        plugin: Plugin,
        download_manager: Arc<DownloadsManager>,
    ) -> Arc<Self> {
        let mut modifiers = Vec::new();

        for modifier in plugin.properties.iter() {
            if let PluginProperties::Modifier(target) = modifier {
                modifiers.push(target.clone());
            }
        }

        Arc::new(Scraper {
            job,
            ratelimiter,
            plugin_manager,
            plugin,
            download_manager,
            text_client: Arc::new(Self::client_create(modifiers.clone(), true)),
            file_client: Arc::new(Self::client_create(modifiers.clone(), false)),
        })
    }
    pub async fn run_scraper(self: Arc<Self>) {
        let plugin_manager = self.plugin_manager.clone();
        let job = self.job.config.clone();
        let plugin = self.plugin.clone();

        let scraper_data_return;

        let default_scraper_data = ScraperDataReturn {
            job,
            skip_conditions: vec![],
        };

        if let Ok(scrap_data) = tokio::task::spawn_blocking(move || {
            plugin_manager.url_dump(&default_scraper_data, &plugin)
        })
        .await
        {
            match scrap_data {
                Ok(good_data) => {
                    scraper_data_return = good_data;
                }
                Err(e) => {
                    log::error!(
                        "DownloadManager: While getting a dump of URL's to do {} had an error {:?}",
                        self.plugin.name,
                        e
                    );
                    self.download_manager
                        .remove_job(&self.plugin, &self.job)
                        .await;
                    return;
                }
            }
        } else {
            return;
        }

        for scrap_data in scraper_data_return {
            for param in scrap_data.job.param {
                let scraper = self.clone();
                if let Some((text, source_url)) =
                    tokio::spawn(async move { scraper.dltext(param).await })
                {
                   for data in  self.plugin_manager.parser_call(
                        text,
                        source_url,
                        &default_scraper_data,
                        &self.plugin,
                    ) {

                        match data {
                            ScraperReturn::Data(scraper_object) => {},
                            _ => {}
                        }

                    }
                }
            }
        }
    }
}

impl DownloadsManager {
    pub fn new(db: Arc<MainDatabase>, plugin_manager: Arc<PluginManager>) -> Arc<Self> {
        let dm = DownloadsManager {
            db,
            plugin_manager,
            jobs: HashMap::new().into(),
        };

        dm.into()
    }

    async fn remove_job(&self, plugin: &Plugin, job: &DbJobsObj) {
        if let Some(internal_storage) = self.jobs.write().await.get_mut(&plugin.name) {
            internal_storage.job_storage.retain(|f| f != job);
            if internal_storage.job_storage.is_empty() {
                self.jobs.write().await.remove(&plugin.name);
                self.db.job_remove(job).await;
            }
        }
    }

    ///
    /// Returns status of any Plugins or such
    ///
    pub async fn all_jobs_complete(&self) -> bool {
        self.jobs.read().await.is_empty()
    }

    ///
    /// Adds jobs into internal structure
    ///
    pub async fn add_jobs(
        self: &Arc<Self>,
        jobs: HashMap<shared_types::Plugin, Vec<shared_types::DbJobsObj>>,
    ) {
        let mut jobs_guard = self.jobs.write().await;

        for (plugin, job_storage) in jobs.iter() {
            if !jobs_guard.contains_key(&plugin.name) {
                let mut ratelimit = None;

                for properties in plugin.properties.iter() {
                    if let shared_types::PluginProperties::Ratelimit(count, time) = properties {
                        let burst_count = u32::try_from(*count).unwrap_or(u32::MAX).max(1);
                        let burst_nonzero = std::num::NonZeroU32::new(burst_count).unwrap();

                        // Note: Governor uses allow_max_burst, not allow_burst
                        ratelimit = Some(
                            Quota::with_period(*time)
                                .unwrap()
                                .allow_burst(burst_nonzero),
                        );
                    }
                }

                // Fallback default rate limiter
                if ratelimit.is_none() {
                    info!(
                        "DownloadManager was unable to pull ratelimiting information from {}",
                        &plugin.name
                    );
                    ratelimit = Some(
                        Quota::with_period(Duration::from_secs(10))
                            .unwrap()
                            .allow_burst(std::num::NonZeroU32::new(1).unwrap()),
                    );
                }

                info!(
                    "DownloadManager made ratelimiter {:?} for {}",
                    ratelimit.unwrap(),
                    &plugin.name
                );

                jobs_guard.insert(
                    plugin.name.clone(),
                    InternalStorage {
                        plugin: plugin.clone(),
                        job_storage: job_storage.to_vec(),
                        // Instantiate the Governor limiter using the built quota configuration blueprint
                        ratelimiter: Arc::new(governor::RateLimiter::direct(ratelimit.unwrap())),
                    },
                );

                let manager_clone = self.clone();
                let scraper_name = plugin.name.clone();

                tokio::task::spawn(async move {
                    manager_clone.spawn_scraper(scraper_name).await;
                });
            }
        }
    }

    ///
    /// Handles the scraper code that runs
    ///
    async fn spawn_scraper(self: &Arc<Self>, scraper_name: String) {
        loop {
            let job_to_run = {
                let mut guard = self.jobs.write().await;

                if let Some(scraper_internal) = guard.get_mut(&scraper_name) {
                    if let Some(idx) = scraper_internal
                        .job_storage
                        .iter()
                        .position(|j| !j.isrunning)
                    {
                        let job = &mut scraper_internal.job_storage[idx];
                        job.isrunning = true;

                        Some((
                            job.clone(),
                            scraper_internal.plugin.clone(),
                            scraper_internal.ratelimiter.clone(),
                        ))
                    } else {
                        if scraper_internal.job_storage.iter().all(|j| j.isrunning) {
                            info!(
                                "DownloadManager: All active jobs for {} are now dispatched.",
                                scraper_name
                            );
                            break;
                        }
                        None
                    }
                } else {
                    break;
                }
            };

            if let Some((job, plugin, ratelimiter)) = job_to_run {
                info!("DownloadManager: Setting job {} to running status.", job.id);
                self.db.job_set_is_running(&job).await;

                let scraper = Scraper::new(
                    job.clone(),
                    ratelimiter.clone(),
                    self.plugin_manager.clone(),
                    plugin,
                    self.clone(),
                );

                // Actually handles the scraping
                tokio::task::spawn(async move { scraper.run_scraper().await });
            } else {
                // Dunno why I need this but it prevents a thread from zooming to 100
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}
