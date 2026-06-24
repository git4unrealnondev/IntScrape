use file_format::FileFormat;
use hex::encode_upper;
use rayon::{ThreadPool, ThreadPoolBuilder};
use sha2::Digest;
use std::{
    collections::{HashMap, HashSet},
    fs::create_dir_all,
    num::{NonZero, NonZeroU64},
    ops::Deref,
    path::Path,
    sync::Arc,
    time::Duration,
};
use url::Url;

use bytes::{Bytes, BytesMut};
use governor::{DefaultDirectRateLimiter, Quota};
use log::info;
use reqwest::Client;
use sha2::Digest as sha2Digest;
use sha2::{Sha256, Sha512};
use shared_types::*;
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
};

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
    //Stores file urls that we're downloading
    file_urls: HashSet<String>,
}

pub struct DownloadsManager {
    db: Arc<MainDatabase>,
    plugin_manager: Arc<PluginManager>,
    jobs: RwLock<HashMap<String, InternalStorage>>,
    heavy_processing_pool: Arc<ThreadPool>,
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

        let mut tags_all = Vec::new();
        let files_all = Arc::new(Mutex::new(HashSet::new()));

        let file_tag_match = Arc::new(Mutex::new(HashSet::new()));

        'scraperloop: for scrap_data in scraper_data_return {
            for param in scrap_data.job.param.iter() {
                let scraper = self.clone();
                let param_clone = param.clone();
                if let Ok(Some((text, source_url))) =
                    tokio::spawn(async move { scraper.dltext(param_clone).await }).await
                {
                    let local_scrap_data = scrap_data.clone();
                    let scraper = self.clone();
                    let mut data_all = tokio::task::spawn_blocking(move || {
                        scraper.plugin_manager.parser_call(
                            &text,
                            &source_url,
                            &local_scrap_data,
                            &scraper.plugin,
                        )
                    })
                    .await
                    .unwrap();

                    //let mut files_all = Vec::new();

                    // Catch to prevent bad data from entering loop
                    if data_all.is_empty() {
                        data_all.push(ScraperReturn::Nothing);
                    }

                    for data in data_all {
                        match data {
                            ScraperReturn::Data(scraper_object) => {
                                let mut set = JoinSet::new();
                                for file in scraper_object.files {
                                    tags_all.extend(file.tag_list.clone());
                                    let scraper = self.clone();
                                    let files_all_clone = files_all.clone();
                                    let files_tag_match_clone = file_tag_match.clone();
                                    set.spawn(async move {
                                        if let Some(fileinternal) =
                                            scraper.file_download_logic(file.clone()).await
                                        {
                                            files_all_clone
                                                .lock()
                                                .await
                                                .insert(fileinternal.clone());
                                            files_tag_match_clone
                                                .lock()
                                                .await
                                                .insert((fileinternal, file.tag_list));
                                        }
                                    });
                                }
                                set.join_all().await;
                            }
                            ScraperReturn::Nothing => {
                                log::error!(
                                    "Worker: {} JobId: {} -- STOPPING JOB due to getting NOTHNG",
                                    self.plugin.name,
                                    self.job.id,
                                );
                                break 'scraperloop;
                            }
                            ScraperReturn::Stop(stop_reason) => {
                                log::error!(
                                    "Worker: {} JobId: {} -- STOPPING JOB {}",
                                    self.plugin.name,
                                    self.job.id,
                                    stop_reason
                                );
                                break 'scraperloop;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        // Do all our happy db stuff down here :D
        let tag_cache = self.download_manager.db.tags_add_bulk(&tags_all).await;

        let files_set = Arc::try_unwrap(files_all)
            .expect("Other tasks are still holding Arc references!")
            .into_inner();

        let file_cache = self.download_manager.db.file_add_bulk(files_set).await;

        let file_tag_match = Arc::try_unwrap(file_tag_match)
            .expect("Other tasks are still holding Arc references!")
            .into_inner();

        let mut rel_list = HashSet::new();
        for (file_internal, tag_list) in file_tag_match.iter() {
            for tag_action in tag_list {
                for tag in tag_action.tags.iter() {
                    if let Some(tag_id) = tag_cache.get(&tag.tag)
                        && let Some(file_id) = file_cache.get(file_internal)
                    {
                        rel_list.insert((*file_id, *tag_id));
                    }
                }
            }
        }
        self.download_manager
            .db
            .add_relationship_bulk(rel_list)
            .await;
    }

    ///
    /// Checks the internal storage to see if we should download a file
    ///
    async fn should_download_file(&self, url: &str) -> bool {
        if let Some(internal_storage) = self
            .download_manager
            .jobs
            .read()
            .await
            .get(&self.plugin.name)
        {
            return !internal_storage.file_urls.contains(url);
        }

        true
    }

    ///
    /// Checks if a file needs to be downloaded and parsed
    ///
    async fn file_download_logic(self: Arc<Self>, file: FileObject) -> Option<FileInternal> {
        let plugin_manager = self.download_manager.plugin_manager.clone();
        let self_clone = self.clone();
        let bytes = match file.source {
            None => {
                // Will update the UI here later

                return None;
            }
            Some(url_source) => match url_source {
                FileSource::Url(file_url) => {
                    if self
                        .download_manager
                        .db
                        .should_download_file(file_url.clone())
                        .await
                    {
                        if self.should_download_file(&file_url).await {
                            if let Some(bytes_out) = self_clone.download_file(&file_url).await {
                                bytes_out
                            } else {
                                return None;
                            }
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
                FileSource::Bytes(file_bytes) => bytes::Bytes::from(file_bytes),
            },
        };

        // After we have our bytes will do our processing here
        let bytes_clone = bytes.clone();
        tokio::task::spawn_blocking(move || {
            plugin_manager.callback_on_download(bytes_clone);
        })
        .await
        .unwrap();

        let bytes_hash = bytes.clone();
        let (hash, ext) = tokio::task::spawn_blocking(move || {
            (
                hash_bytes(&bytes_hash, &HashesSupported::Sha512("".into())).0,
                FileFormat::from_bytes(&*bytes_hash).extension().to_string(),
            )
        })
        .await
        .unwrap();

        let storage_id;
        if let Some((file_storage_path, storage_id_db)) = self
            .download_manager
            .db
            .file_download_location_get(&hash, &ext)
            .await
            && let Some(parent_dir) = file_storage_path.parent()
        {
            let mut cnt = 0;

            loop {
                let bytes_clone = bytes.clone();
                if create_dir_all(parent_dir).is_ok()
                    && tokio::fs::write(&file_storage_path, bytes_clone)
                        .await
                        .is_ok()
                {
                    storage_id = storage_id_db;
                    break;
                }

                cnt += 1;
                if cnt >= 3 {
                    log::error!(
                        "Failed to save file after 3 attempts: {:?}",
                        file_storage_path
                    );
                    return None;
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        } else {
            return None;
        }
        Some(FileInternal {
            id: None,
            hash,
            ext,
            storage_id,
        })
    }

    ///
    /// Downloads a singular file
    ///
    async fn download_file(self: Arc<Self>, file_url: &str) -> Option<Bytes> {
        let mut cnt = 0;
        let url = Url::parse(file_url);
        if url.is_err() {
            log::error!("Error while parsing url {} {:?}", file_url, url);
            return None;
        }
        let url = url.unwrap();

        loop {
            self.ratelimiter.until_ready().await;

            info!(
                "Scraper: {} JobId: {} -- Downloading url: {}",
                self.plugin.name, self.job.id, file_url
            );
            let mut response = match self.file_client.get(url.clone()).send().await {
                Ok(res) => res,
                Err(err) => {
                    log::error!(
                        "Scraper: {} JobId: {} While processing url: {} found err {:?}",
                        self.plugin.name,
                        self.job.id,
                        file_url,
                        err
                    );
                    cnt += 1;
                    continue;
                }
            };

            let response_content_length: usize = response
                .content_length()
                .unwrap_or(1024)
                .try_into()
                .unwrap_or(1024);

            let mut downloaded: usize = 0;
            let mut collected_bytes = BytesMut::with_capacity(response_content_length);

            while let Ok(Some(chunk)) = response.chunk().await {
                let bytes_recieved = chunk.len();
                downloaded += bytes_recieved;
                collected_bytes.extend_from_slice(&chunk);

                let current_progress = if downloaded > 0 {
                    (downloaded as f64 / response_content_length as f64) * 100.0
                } else {
                    (downloaded as f64) / (1024.0 * 1024.0)
                };

                //NOTE put UI updaing stuff here
            }

            if collected_bytes.len() == response_content_length {
                return Some(collected_bytes.into());
            } else {
                log::error!(
                    "Scraper: {} JobId: {} File downloading had mismatched download length. Downloaded {} Parsed {}",
                    self.plugin.name,
                    self.job.id,
                    collected_bytes.len(),
                    response_content_length
                )
            }

            cnt += 1;
            if cnt >= 3 {
                break;
            }
        }
        None
    }
}

impl DownloadsManager {
    pub fn new(
        db: Arc<MainDatabase>,
        plugin_manager: Arc<PluginManager>,
        heavy_processing_pool: Arc<ThreadPool>,
    ) -> Arc<Self> {
        let dm = DownloadsManager {
            db,
            plugin_manager,
            jobs: HashMap::new().into(),
            heavy_processing_pool,
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
                        file_urls: HashSet::new(),
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

/// Hashes the bytes and compares it to what the scraper should of recieved.
pub fn hash_bytes(bytes: &Bytes, hash: &HashesSupported) -> (String, bool) {
    match hash {
        HashesSupported::Md5(hash) => {
            let digest = md5::compute(bytes);

            // let sharedtypes::HashesSupported(hashe, _) => hash;
            if &format!("{:x}", digest) != hash {
                info!("Parser returned: {} Got: {:?}", &hash, &digest);
            }
            (format!("{:x}", digest), &format!("{:x}", digest) == hash)
        }
        HashesSupported::Sha1(hash) => {
            let mut hasher = sha1::Sha1::new();
            hasher.update(bytes);
            let hastring = encode_upper(hasher.finalize());
            let dune = &hastring == hash;
            if !dune {
                info!("Parser returned: {} Got: {}", &hash, &hastring);
            }
            (hastring, dune)
        }
        HashesSupported::Sha256(hash) => {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            let hastring = encode_upper(hasher.finalize());
            let dune = &hastring == hash;
            if !dune {
                info!("Parser returned: {} Got: {}", &hash, &hastring);
            }
            (hastring, dune)
        }
        HashesSupported::Sha512(hash) => {
            let hasher = Sha512::digest(bytes);
            let hastring = encode_upper(hasher);
            let dune = &hastring == hash;
            if !dune {
                info!("Parser returned: {} Got: {}", &hash, &hastring);
            }
            (hastring, dune)
        }
    }
}
