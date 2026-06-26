use file_format::FileFormat;
use hex::encode_upper;
use rayon::ThreadPool;
use sha2::Digest;
use std::{
    collections::{HashMap, HashSet},
    fs::create_dir_all,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use url::Url;

use bytes::{Bytes, BytesMut};
use governor::{DefaultDirectRateLimiter, Quota};
use log::info;
use reqwest::Client;
use sha2::{Sha256, Sha512};
use shared_types::*;
use tokio::{
    sync::{Mutex, RwLock, oneshot},
    task::JoinSet,
};

use crate::{db::MainDatabase, helper_functions::get_sys_time_in_secs, plugins::PluginManager};

pub(in crate::web) struct Scraper {
    pub(in crate::web) job: DbJobsObj,
    pub(in crate::web) ratelimiter: Arc<DefaultDirectRateLimiter>,
    pub(in crate::web) plugin_manager: Arc<PluginManager>,
    pub(in crate::web) plugin: Plugin,
    pub(in crate::web) download_manager: Arc<DownloadsManager>,
    pub(in crate::web) text_client: Arc<Client>,
    pub(in crate::web) file_client: Arc<Client>,
}

#[derive(Clone, Debug)]
struct InternalStorage {
    plugin: Plugin,
    job_storage: Vec<DbJobsObj>,
    completed_job_storage: Vec<DbJobsObj>,
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

    ///
    /// Handles scraping logic
    ///
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
                    scraper_data_return = Arc::new(good_data);
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

        let file_id_tag_map = Arc::new(Mutex::new(HashMap::new()));
        let job_list = Arc::new(Mutex::new(Vec::new()));
        let tag_list = Arc::new(Mutex::new(Vec::new()));
        let should_remove_job = Arc::new(AtomicBool::new(true));

        'scraperloop: for scrap_data in scraper_data_return.iter() {
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

                    // Catch to prevent bad data from entering loop
                    if data_all.is_empty() {
                        data_all.push(ScraperReturn::Nothing);
                    }

                    for data in data_all {
                        match data {
                            ScraperReturn::Data(scraper_object) => {
                                if scraper_object.files.is_empty()
                                    && scraper_object.tags.is_empty()
                                    && scraper_object.jobs.is_empty()
                                {
                                    log::info!(
                                        "Worker: {} JobId: {} -- STOPPING JOB due to files, tags & jobs being empty inside of a valid scraperreturn data object.",
                                        self.plugin.name,
                                        self.job.id,
                                    );

                                    break 'scraperloop;
                                }

                                // Adds jobs from scraper into the list to process
                                job_list.lock().await.extend(scraper_object.jobs);
                                tag_list.lock().await.extend(scraper_object.tags);

                                let mut set = JoinSet::new();
                                for mut file in scraper_object.files {
                                    let scraper = self.clone();
                                    let file_id_tag_map_clone = file_id_tag_map.clone();
                                    let should_remove_job_clone = should_remove_job.clone();
                                    let job_list_clone = job_list.clone();

                                    set.spawn(async move {
                                        let mut download_issue = false;
                                        let mut jobs = Vec::new();
                                        if let Some(fileinternal) = scraper
                                            .file_download_logic(
                                                &mut file,
                                                &mut jobs,
                                                &mut download_issue,
                                            )
                                            .await
                                        {
                                            // Adds jobs from the on_download callback
                                            job_list_clone.lock().await.extend(jobs);

                                            // Adds tag data from the file
                                            file_id_tag_map_clone
                                                .lock()
                                                .await
                                                .insert(fileinternal, file.tag_list);
                                        }

                                        if download_issue {
                                            should_remove_job_clone
                                                .store(false, std::sync::atomic::Ordering::Relaxed);
                                        }
                                    });
                                }
                                set.join_all().await;
                            }
                            ScraperReturn::Nothing => {
                                log::info!(
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
        let file_id_tag_map = Arc::try_unwrap(file_id_tag_map)
            .expect("Arc reference leak")
            .into_inner();

        let mut job_list = Arc::try_unwrap(job_list)
            .expect("Arc reference leak")
            .into_inner();

        let tags = Arc::try_unwrap(tag_list)
            .expect("Arc reference leak")
            .into_inner();

        let completed_params: std::collections::HashSet<_> = {
            let jobs_guard = self.download_manager.jobs.read().await;
            if let Some(internal_storage) = jobs_guard.get(&self.plugin.name) {
                // Collect historical parameters into a HashSet for O(1) lookups
                internal_storage
                    .completed_job_storage
                    .iter()
                    .map(|internal_job| internal_job.config.param.clone())
                    .collect()
            } else {
                std::collections::HashSet::new()
            }
        }; // <-- Lock drops here automatically!

        // 3. Deduplicate 'job_list' in-place with O(1) efficiency
        // Retain keeps elements only if the closure returns true
        job_list.retain(|job| {
            let is_dup = completed_params.contains(&job.job.param);
            if is_dup {
                info!(
                    "Scraper: Skipping duplicate job parameter discovery: {:?}",
                    job.job.param
                );
            }
            !is_dup // Keep if NOT a duplicate
        });

        self.download_manager
            .db
            .tags_add_bulk(&[FileTagAction {
                operation: TagOperation::Add,
                tags,
            }])
            .await;

        self.download_manager
            .db
            .process_scraper(file_id_tag_map, job_list)
            .await;

        if self.manage_recreation().await {
            should_remove_job.store(false, std::sync::atomic::Ordering::Relaxed);
            if let Some(internal_storage) = self
                .download_manager
                .jobs
                .write()
                .await
                .get_mut(&self.plugin.name)
            {
                internal_storage.job_storage.retain(|f| *f != self.job);
            }
        }

        if should_remove_job.load(std::sync::atomic::Ordering::Relaxed) {
            // Updates internal jobs cache
            if let Some(internal_storage) = self
                .download_manager
                .jobs
                .write()
                .await
                .get_mut(&self.plugin.name)
            {
                internal_storage
                    .completed_job_storage
                    .push(self.job.clone());
            }

            self.download_manager
                .remove_job(&self.plugin, &self.job)
                .await;
        }
    }

    async fn manage_recreation(&self) -> bool {
        if let Some(mut recreation) = self.job.config.recreation.clone() {
            match recreation {
                DbJobRecreation::AlwaysTime(timestamp, count) => {
                    let mut job = self.job.clone();
                    job.config.time = get_sys_time_in_secs();
                    job.config.reptime = timestamp;
                    job.isrunning=false;
                    if let Some(mut count) = count {
                        if count >= 1 {
                            count -= 1;
                        } else {
                            return false;
                        }
                        job.config.recreation =
                            Some(DbJobRecreation::AlwaysTime(timestamp, Some(count)));
                    }

                    self.download_manager.db.jobs_update(&job).await;
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    ///
    /// Checks the internal storage to see if we should download a file
    ///
    async fn should_download_file(&self, url: &str) -> bool {
        let mut jobs_guard = self.download_manager.jobs.write().await;

        if let Some(internal_storage) = jobs_guard.get_mut(&self.plugin.name) {
            return internal_storage.file_urls.insert(url.to_string());
        }

        // Fallback or handle if internal_storage doesn't exist
        false
    }

    ///
    /// Checks if a file needs to be downloaded and parsed
    ///
    async fn file_download_logic(
        self: Arc<Self>,
        file: &mut FileObject,
        jobs: &mut Vec<ScraperDataReturn>,
        download_issue: &mut bool,
    ) -> Option<FileInternal> {
        let plugin_manager = self.download_manager.plugin_manager.clone();
        let self_clone = self.clone();
        let bytes = match file.source {
            None => {
                // Will update the UI here later

                return None;
            }
            Some(ref url_source) => match url_source {
                FileSource::Url(file_url) => {
                    match self
                        .download_manager
                        .db
                        .tag_get_fileid(&Tag {
                            name: file_url.clone(),
                            namespace: GenericNamespaceObj {
                                name: "source_url".into(),
                                description: None,
                            },
                        })
                        .await
                    {
                        Some(file_id) => {
                            info!(
                                "Scraper: {} JobId: {} Skipping file because already in db. {}",
                                self.plugin.name, self.job.id, &file_url
                            );

                            return self.download_manager.db.file_id_get(file_id).await;
                        }
                        None => {
                            file.tag_list.push(FileTagAction {
                                operation: TagOperation::Add,
                                tags: vec![PluginTag {
                                    tag: Tag {
                                        name: file_url.to_string(),
                                        namespace: GenericNamespaceObj {
                                            name: "source_url".into(),
                                            description: Some("A source for a file".into()),
                                        },
                                    },

                                    ..Default::default()
                                }],
                            });
                            if self.should_download_file(file_url).await {
                                if let Some(bytes_out) = self_clone.download_file(file_url).await {
                                    info!(
                                        "Scraper: {} JobId: {} Successfully downloaded {}",
                                        self.plugin.name, self.job.id, &file_url
                                    );
                                    bytes_out
                                } else {
                                    *download_issue = true;
                                    return None;
                                }
                            } else {
                                info!(
                                    "Scraper: {} JobId: {} Skipping file because already in db. {}",
                                    self.plugin.name, self.job.id, &file_url
                                );

                                return None;
                            }
                        }
                    }
                }
                FileSource::Bytes(file_bytes) => bytes::Bytes::from(file_bytes.clone()),
            },
        };

        // After we have our bytes will do our processing here
        let bytes_clone = bytes.clone();
        let (tx, rx) = oneshot::channel();
        let mut tags_owned = file.tag_list.clone();
        let mut jobs_owned = jobs.clone();
        self.download_manager.heavy_processing_pool.spawn(move || {
            plugin_manager.callback_on_download(&bytes_clone, &mut tags_owned, &mut jobs_owned);
            let _ = tx.send((tags_owned, jobs_owned));
        });

        (file.tag_list, *jobs) = rx.await.unwrap();

        let bytes_hash = bytes.clone();
        let (hash, extension) = tokio::task::spawn_blocking(move || {
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
            .file_download_location_get(&hash, &extension)
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
            extension,
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

                let _current_progress = if downloaded > 0 {
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

    ///
    /// Clears a job from the system
    ///
    async fn remove_job(&self, plugin: &Plugin, job: &DbJobsObj) {
        // If all jobs are finished then remove plugin from db
        let mut remove_plugin = false;

        if let Some(internal_storage) = self.jobs.write().await.get_mut(&plugin.name) {
            internal_storage.job_storage.retain(|f| f != job);
            self.db.job_remove(job).await;
            if internal_storage.job_storage.is_empty() {
                remove_plugin = true;
            }
        }

        if remove_plugin {
            self.jobs.write().await.remove(&plugin.name);
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
            if let Some(internal_storage) = jobs_guard.get_mut(&plugin.name) {
                // Check if the worker loop had previously died/retired by verifying if storage was empty
                let needs_resurrection = internal_storage.job_storage.is_empty();

                for new_job in job_storage {
                    if !internal_storage
                        .job_storage
                        .iter()
                        .any(|j| j.id == new_job.id)
                    {
                        internal_storage.job_storage.push(new_job.clone());
                    }
                }

                // If the thread broke out and retired earlier, spawn a fresh task loop to consume the new jobs
                if needs_resurrection && !internal_storage.job_storage.is_empty() {
                    info!(
                        "DownloadManager: Resurrecting worker loop for active site: {}",
                        plugin.name
                    );
                    let manager_clone = self.clone();
                    let scraper_name = plugin.name.clone();
                    tokio::task::spawn(async move {
                        manager_clone.spawn_scraper(scraper_name).await;
                    });
                }
            } else {
                // Brand new site configuration path
                let mut ratelimit = Some(
                    Quota::with_period(Duration::from_secs(1))
                        .unwrap()
                        .allow_burst(std::num::NonZeroU32::new(1).unwrap()),
                );

                jobs_guard.insert(
                    plugin.name.clone(),
                    InternalStorage {
                        plugin: plugin.clone(),
                        job_storage: job_storage.to_vec(),
                        ratelimiter: Arc::new(governor::RateLimiter::direct(ratelimit.unwrap())),
                        completed_job_storage: vec![],
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
            let mut guard = self.jobs.write().await;

            let Some(scraper_internal) = guard.get_mut(&scraper_name) else {
                // The storage for this site was deleted entirely. Kill this worker task.
                info!(
                    "DownloadManager: Internal storage for {} removed. Killing worker loop.",
                    scraper_name
                );
                break;
            };

            // 1. If there are literally no jobs tracked in memory, kill this thread.
            if scraper_internal.job_storage.is_empty() {
                info!(
                    "DownloadManager: No jobs remaining in memory for {}. Retiring worker loop.",
                    scraper_name
                );
                break;
            }

            // 2. Look for an idle job
            let job_to_run = if let Some(idx) = scraper_internal
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
                None
            };

            // Explicitly drop the guard before doing any async work or sleeping
            drop(guard);

            if let Some((job, plugin, ratelimiter)) = job_to_run {
                info!("DownloadManager: Setting job {} to running status.", job.id);
                self.db.job_set_is_running(&job).await;

                let scraper = Scraper::new(
                    job,
                    ratelimiter,
                    self.plugin_manager.clone(),
                    plugin,
                    self.clone(),
                );

                tokio::task::spawn(async move { scraper.run_scraper().await });
            } else {
                // All jobs are currently actively running. Wait a bit for them to finish.
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
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

/*pub fn calculate_relationship_deltas(
    file_tag_match: &HashMap<FileInternal, Vec<FileTagAction>>,
    file_cache: &HashMap<FileInternal, u64>,
    tag_cache: &HashMap<Tag, u64>,
    current_file_relationships: &HashMap<u64, HashSet<Tag>>,
) -> (HashSet<(u64, u64)>, HashSet<(u64, u64)>) {
    let mut rels_to_add = HashSet::new();
    let mut rels_to_del = HashSet::new();

    for (file_internal, tag_list) in file_tag_match.iter() {
        let file_id = match file_cache.get(file_internal) {
            Some(&id) => id,
            None => continue,
        };

        // 1. Map current database state for this file: Namespace -> HashSet<TagId>
        let mut current_ns_tags: HashMap<String, HashSet<u64>> = HashMap::new();
        if let Some(current_tags) = current_file_relationships.get(&file_id) {
            for tag in current_tags {
                let ns_name = &tag.namespace.name;
                // "source_url" and empty namespaces are explicitly excluded from Set logic
                if ns_name != "source_url" && !ns_name.is_empty() {
                    if let Some(&tag_id) = tag_cache.get(tag) {
                        current_ns_tags
                            .entry(ns_name.clone())
                            .or_default()
                            .insert(tag_id);
                    }
                }
            }
        }

        // Track what tags are explicitly being added across this file's entire payload
        // to support the "Add overrides Set" business rule.
        let mut explicit_adds = HashSet::new();
        // Track deletions that originate strictly from a 'Set' operation
        let mut set_deletions = HashSet::new();

        // 2. Process operations
        for tag_action in tag_list {
            match tag_action.operation {
                TagOperation::Add => {
                    for tag in &tag_action.tags {
                        if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex) {
                            let tag_id = *tag_cache.get(&tag.tag).unwrap();
                            rels_to_add.insert((file_id, tag_id));
                            explicit_adds.insert(tag_id);
                        }
                    }
                }
                TagOperation::Del => {
                    for tag in &tag_action.tags {
                        if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex) {
                            let tag_id = *tag_cache.get(&tag.tag).unwrap();
                            rels_to_del.insert((file_id, tag_id));
                        }
                    }
                }
                TagOperation::Set => {
                    let mut incoming_ns_tags: HashMap<String, HashSet<u64>> = HashMap::new();

                    for tag in &tag_action.tags {
                        if !matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex) {
                            continue;
                        }
                        let ns_name = &tag.tag.namespace.name;
                        if ns_name == "source_url" || ns_name.is_empty() {
                            continue;
                        }

                        let tag_id = *tag_cache.get(&tag.tag).unwrap();
                        incoming_ns_tags
                            .entry(ns_name.clone())
                            .or_default()
                            .insert(tag_id);

                        rels_to_add.insert((file_id, tag_id));
                    }

                    // Evaluate deletions ONLY for namespaces explicitly targeted by this Set operation
                    for (ns_name, incoming_set) in &incoming_ns_tags {
                        if let Some(current_tag_ids) = current_ns_tags.get(ns_name) {
                            for current_tag_id in current_tag_ids {
                                if !incoming_set.contains(current_tag_id) {
                                    // Track this specifically as a Set-induced deletion
                                    set_deletions.insert((file_id, *current_tag_id));
                                }
                            }
                        }
                    }
                }
            }
        }

        // 3. Apply targeted "Add overrides Set" rule
        for (f_id, tag_id) in set_deletions {
            // Only commit the Set deletion if an explicit Add didn't countermand it
            if !explicit_adds.contains(&tag_id) {
                rels_to_del.insert((f_id, tag_id));
            }
        }
    }

    // Global sanitation check for any edge deletions
    for del in &rels_to_del {
        rels_to_add.remove(del);
    }

    (rels_to_add, rels_to_del)
}*/

pub fn calculate_relationship_deltas(
    file_tag_match: &HashMap<FileInternal, Vec<FileTagAction>>,
    file_cache: &HashMap<FileInternal, u64>,
    tag_cache: &HashMap<Tag, u64>,
    current_file_relationships: &HashMap<u64, HashSet<Tag>>,
) -> (HashSet<(u64, u64)>, HashSet<(u64, u64)>) {
    let mut rels_to_add = HashSet::new();
    let mut rels_to_del = HashSet::new();

    // Hoist working collections outside the loop to preserve allocated capacity
    let mut current_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
    let mut incoming_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
    let mut explicit_adds = HashSet::new();
    let mut set_deletions = HashSet::new();

    for (file_internal, tag_list) in file_tag_match.iter() {
        let file_id = match file_cache.get(file_internal) {
            Some(&id) => id,
            None => continue,
        };

        // Clear hoisted structures instead of re-allocating them
        current_ns_tags.clear();
        explicit_adds.clear();
        set_deletions.clear();

        // 1. Map current database state for this file: Namespace (&str) -> HashSet<TagId>
        if let Some(current_tags) = current_file_relationships.get(&file_id) {
            for tag in current_tags {
                let ns_name = &tag.namespace.name;
                if ns_name != "source_url" && !ns_name.is_empty() {
                    if let Some(&tag_id) = tag_cache.get(tag) {
                        current_ns_tags
                            .entry(ns_name.as_str()) // Replaced .clone() with &str slice reference
                            .or_default()
                            .insert(tag_id);
                    }
                }
            }
        }

        // 2. Process operations
        for tag_action in tag_list {
            match tag_action.operation {
                TagOperation::Add => {
                    for tag in &tag_action.tags {
                        if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex) {
                            let tag_id = *tag_cache.get(&tag.tag).unwrap();
                            rels_to_add.insert((file_id, tag_id));
                            explicit_adds.insert(tag_id);
                        }
                    }
                }
                TagOperation::Del => {
                    for tag in &tag_action.tags {
                        if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex) {
                            let tag_id = *tag_cache.get(&tag.tag).unwrap();
                            rels_to_del.insert((file_id, tag_id));
                        }
                    }
                }
                TagOperation::Set => {
                    incoming_ns_tags.clear(); // Clear the nested map instead of letting it drop

                    for tag in &tag_action.tags {
                        if !matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex) {
                            continue;
                        }
                        let ns_name = &tag.tag.namespace.name;
                        if ns_name == "source_url" || ns_name.is_empty() {
                            continue;
                        }

                        let tag_id = *tag_cache.get(&tag.tag).unwrap();
                        incoming_ns_tags
                            .entry(ns_name.as_str()) // Replaced .clone() with &str slice reference
                            .or_default()
                            .insert(tag_id);

                        rels_to_add.insert((file_id, tag_id));
                    }

                    // Evaluate deletions ONLY for namespaces explicitly targeted by this Set operation
                    for (ns_name, incoming_set) in &incoming_ns_tags {
                        if let Some(current_tag_ids) = current_ns_tags.get(ns_name) {
                            for current_tag_id in current_tag_ids {
                                if !incoming_set.contains(current_tag_id) {
                                    set_deletions.insert((file_id, *current_tag_id));
                                }
                            }
                        }
                    }
                }
            }
        }

        // 3. Apply targeted "Add overrides Set" rule
        for (f_id, tag_id) in &set_deletions {
            if !explicit_adds.contains(tag_id) {
                rels_to_del.insert((*f_id, *tag_id));
            }
        }
    }

    // Global sanitation check for any edge deletions
    for del in &rels_to_del {
        rels_to_add.remove(del);
    }

    (rels_to_add, rels_to_del)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple structural helpers to generate minimal testing datasets
    fn mock_tag(name: &str, ns: &str) -> Tag {
        Tag {
            name: name.to_string(),
            namespace: GenericNamespaceObj {
                name: ns.to_string(),
                description: None,
            },
        }
    }

    fn mock_file(hash: &str) -> FileInternal {
        FileInternal {
            id: None,
            hash: hash.to_string(),
            extension: "jpg".to_string(),
            storage_id: 1,
        }
    }
    #[test]
    fn test_set_operation_isolates_by_namespace_and_leaves_others_untouched() {
        let file = mock_file("hash_1");
        let tag_ns1_old = mock_tag("action", "genre");
        let tag_ns1_new = mock_tag("comedy", "genre");
        let tag_ns2_keep = mock_tag("bruce_willis", "actor"); // Should remain untouched

        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100);

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag_ns1_old.clone(), 201);
        tag_cache.insert(tag_ns1_new.clone(), 202);
        tag_cache.insert(tag_ns2_keep.clone(), 301);

        // Historical DB State: Has both a genre tag and an actor tag
        let mut current_rels = HashMap::new();
        current_rels.insert(
            100,
            HashSet::from([tag_ns1_old.clone(), tag_ns2_keep.clone()]),
        );

        // Payload action: Set only targets the "genre" namespace
        let file_tag_match = HashMap::from([(
            file,
            vec![FileTagAction {
                operation: TagOperation::Set,
                tags: vec![PluginTag {
                    tag_type: TagType::Normal,
                    tag: tag_ns1_new.clone(),
                    ..Default::default()
                }],
            }],
        )]);

        let (to_add, to_del) =
            calculate_relationship_deltas(&file_tag_match, &file_cache, &tag_cache, &current_rels);

        // Assert updates within targeted namespace occurred
        assert!(to_add.contains(&(100, 202)), "Should add new genre");
        assert!(to_del.contains(&(100, 201)), "Should remove omitted genre");

        // CRITICAL ASSERT: The actor namespace was not in the Set payload, so its tags must NOT be deleted
        assert!(
            !to_del.contains(&(100, 301)),
            "Should NOT delete tags from untouched namespaces"
        );
    }

    #[test]
    fn test_add_operation_overrides_set_deletion_in_same_namespace() {
        let file = mock_file("hash_1");
        let tag_old = mock_tag("action", "genre");
        let tag_new = mock_tag("comedy", "genre");

        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100);

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag_old.clone(), 201);
        tag_cache.insert(tag_new.clone(), 202);

        // Historical DB State: File currently has "action" (201)
        let mut current_rels = HashMap::new();
        current_rels.insert(100, HashSet::from([tag_old.clone()]));

        // Payload action: Set namespace to contain ONLY "comedy" (omitting "action"),
        // BUT followed immediately by an explicit ADD instruction for "action".
        let file_tag_match = HashMap::from([(
            file,
            vec![
                FileTagAction {
                    operation: TagOperation::Set,
                    tags: vec![PluginTag {
                        tag_type: TagType::Normal,
                        tag: tag_new.clone(),
                        ..Default::default()
                    }],
                },
                FileTagAction {
                    operation: TagOperation::Add,
                    tags: vec![PluginTag {
                        tag_type: TagType::Normal,
                        tag: tag_old.clone(),
                        ..Default::default()
                    }],
                },
            ],
        )]);

        let (to_add, to_del) =
            calculate_relationship_deltas(&file_tag_match, &file_cache, &tag_cache, &current_rels);

        // Assert new item from Set is added
        assert!(to_add.contains(&(100, 202)));

        // CRITICAL ASSERT: Add overrides Set. Even though Set omitted tag 201,
        // the explicit Add rescues it from the deletion set.
        assert!(
            !to_del.contains(&(100, 201)),
            "Explicit Add must rescue the tag from Set deletion"
        );
    }

    #[test]
    fn test_source_url_and_empty_namespaces_are_immune_to_set_overrides() {
        let file = mock_file("hash_1");
        let tag_source = mock_tag("https://example.com/image.jpg", "source_url");
        let tag_empty_ns = mock_tag("untracked_tag", "");
        let tag_normal = mock_tag("comedy", "genre");

        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100);

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag_source.clone(), 701);
        tag_cache.insert(tag_empty_ns.clone(), 702);
        tag_cache.insert(tag_normal.clone(), 202);

        // Historical DB State: File has a source url and an un-namespaced tag
        let mut current_rels = HashMap::new();
        current_rels.insert(
            100,
            HashSet::from([tag_source.clone(), tag_empty_ns.clone()]),
        );

        // Payload action: Execute a Set operation containing a completely different namespace
        let file_tag_match = HashMap::from([(
            file,
            vec![FileTagAction {
                operation: TagOperation::Set,
                tags: vec![PluginTag {
                    tag_type: TagType::Normal,
                    tag: tag_normal.clone(),
                    ..Default::default()
                }],
            }],
        )]);

        let (_, to_del) =
            calculate_relationship_deltas(&file_tag_match, &file_cache, &tag_cache, &current_rels);

        // CRITICAL ASSERT: Set should never parse or drop source_url or "" namespaces
        assert!(
            !to_del.contains(&(100, 701)),
            "source_url tags must be immune to Set clears"
        );
        assert!(
            !to_del.contains(&(100, 702)),
            "Empty namespace tags must be immune to Set clears"
        );
    }

    #[test]
    fn test_unmatched_tag_types_are_ignored_across_all_operations() {
        let file = mock_file("hash_1");
        let tag_invalid = mock_tag("regex_pattern", "genre");

        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100);

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag_invalid.clone(), 888);

        // Payload contains an unsupported TagType variant (e.g., Regex) across updates
        let file_tag_match = HashMap::from([(
            file,
            vec![
                FileTagAction {
                    operation: TagOperation::Add,
                    tags: vec![PluginTag {
                        tag_type: TagType::Special, // Invalid type variant
                        tag: tag_invalid.clone(),
                        ..Default::default()
                    }],
                },
                FileTagAction {
                    operation: TagOperation::Del,
                    tags: vec![PluginTag {
                        tag_type: TagType::Special, // Invalid type variant
                        tag: tag_invalid.clone(),
                        ..Default::default()
                    }],
                },
            ],
        )]);

        let (to_add, to_del) = calculate_relationship_deltas(
            &file_tag_match,
            &file_cache,
            &tag_cache,
            &HashMap::new(),
        );

        // CRITICAL ASSERT: Non-Normal / Non-NormalNoRegex types must never generate deltas
        assert!(
            !to_add.contains(&(100, 888)),
            "Should ignore non-normal tags on Add"
        );
        assert!(
            !to_del.contains(&(100, 888)),
            "Should ignore non-normal tags on Del"
        );
    }
    #[test]
    fn test_tag_operation_add() {
        let file = mock_file("hash_1");
        let tag = mock_tag("rust", "language");

        // Caches mimicking established DB values mapped to real IDs
        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100); // File ID 100

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag.clone(), 500); // Tag ID 500

        let file_tag_match = HashMap::from([(
            file,
            vec![FileTagAction {
                operation: TagOperation::Add,
                tags: vec![PluginTag {
                    tag: tag.clone(),
                    ..Default::default()
                }],
            }],
        )]);

        let current_rels = HashMap::new(); // Empty DB history

        let (to_add, to_del) =
            calculate_relationship_deltas(&file_tag_match, &file_cache, &tag_cache, &current_rels);

        assert!(to_add.contains(&(100, 500)));
        assert!(to_del.is_empty());
    }

    #[test]
    fn test_tag_operation_del() {
        let file = mock_file("hash_1");
        let tag = mock_tag("c++", "language");

        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100);

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag.clone(), 600);

        let file_tag_match = HashMap::from([(
            file,
            vec![FileTagAction {
                operation: TagOperation::Del,
                tags: vec![PluginTag {
                    tag: tag.clone(),
                    ..Default::default()
                }],
            }],
        )]);

        let current_rels = HashMap::new();

        let (to_add, to_del) =
            calculate_relationship_deltas(&file_tag_match, &file_cache, &tag_cache, &current_rels);

        assert!(to_add.is_empty());
        assert!(to_del.contains(&(100, 600)));
    }

    #[test]
    fn test_tag_operation_set_clears_omitted_historical_tags_in_namespace() {
        let file = mock_file("hash_1");
        let tag_old = mock_tag("action", "genre");
        let tag_new = mock_tag("comedy", "genre");

        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100);

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag_old.clone(), 201);
        tag_cache.insert(tag_new.clone(), 202);

        // Historical DB State: File 100 currently contains "action" (201)
        let mut current_rels = HashMap::new();
        current_rels.insert(100, HashSet::from([tag_old.clone()]));

        // Payload action: Update "genre" namespace to contain ONLY "comedy" (202)
        let file_tag_match = HashMap::from([(
            file,
            vec![FileTagAction {
                operation: TagOperation::Set,
                tags: vec![PluginTag {
                    tag_type: TagType::Normal,
                    tag: tag_new.clone(),
                    ..Default::default()
                }],
            }],
        )]);

        let (to_add, to_del) =
            calculate_relationship_deltas(&file_tag_match, &file_cache, &tag_cache, &current_rels);

        // Assert new relationship added
        assert!(to_add.contains(&(100, 202)));
        // Assert old relationship dropped entirely because it was missing from the new Set payload
        assert!(to_del.contains(&(100, 201)));
    }

    #[test]
    fn test_sanitization_removes_overlapping_add_and_delete_operations() {
        let file = mock_file("hash_1");
        let tag = mock_tag("overlap", "test");

        let mut file_cache = HashMap::new();
        file_cache.insert(file.clone(), 100);

        let mut tag_cache = HashMap::new();
        tag_cache.insert(tag.clone(), 999);

        // Payload contains conflicting operations to Add AND Delete the same item
        let file_tag_match = HashMap::from([(
            file,
            vec![
                FileTagAction {
                    operation: TagOperation::Add,
                    tags: vec![PluginTag {
                        tag_type: TagType::Normal,
                        tag: tag.clone(),

                        ..Default::default()
                    }],
                },
                FileTagAction {
                    operation: TagOperation::Del,
                    tags: vec![PluginTag {
                        tag_type: TagType::Normal,
                        tag: tag.clone(),

                        ..Default::default()
                    }],
                },
            ],
        )]);

        let (to_add, to_del) = calculate_relationship_deltas(
            &file_tag_match,
            &file_cache,
            &tag_cache,
            &HashMap::new(),
        );

        // Sanitization priority states: If explicitly deleting, remove from processing updates
        assert!(to_del.contains(&(100, 999)));
        assert!(
            !to_add.contains(&(100, 999)),
            "Should filter out from insertion set"
        );
    }
}
