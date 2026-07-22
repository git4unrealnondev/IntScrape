use file_format::FileFormat;
use hex::encode_upper;
use rayon::ThreadPool;
use sha2::Digest;
use std::{
    collections::{HashMap, HashSet},
    env::temp_dir,
    io::{Write, stdin, stdout},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tempfile::NamedTempFile;
use url::Url;

use bytes::Bytes;
use governor::{DefaultDirectRateLimiter, Quota};
use log::info;
use reqwest::Client;
use sha2::{Sha256, Sha512};
use shared_types::*;
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{Mutex, RwLock, Semaphore},
    task::JoinSet,
};

const MAX_CONCURRENT_DOWNLOADS: usize = 5;

use crate::{
    db::MainDatabase,
    helper_functions::{get_sys_time_in_secs, memory_manage},
    plugins::PluginManager,
};

enum TrackedFile {
    Temp(tempfile::NamedTempFile),
    Manual(std::path::PathBuf),
}

impl TrackedFile {
    // A helper method to easily extract the path reference
    fn path(&self) -> &std::path::Path {
        match self {
            TrackedFile::Temp(f) => f.path(),
            TrackedFile::Manual(p) => p,
        }
    }
}

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
    job_limiter: Arc<Semaphore>,
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

        let job = job.clone();

        let scraper = Scraper {
            job,
            ratelimiter,
            plugin_manager,
            plugin,
            download_manager,
            text_client: Arc::new(Self::client_create(modifiers.clone(), true)),
            file_client: Arc::new(Self::client_create(modifiers.clone(), false)),
        };

        scraper.into()
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

        let should_remove_job = Arc::new(AtomicBool::new(true));

        // Sets the max number of concurrent downloads
        let mut max_concurrent_downloads = MAX_CONCURRENT_DOWNLOADS;

        for property in self.plugin.properties.iter() {
            if let PluginProperties::ThreadNum(thread_num) = property {
                max_concurrent_downloads =
                    (*thread_num).try_into().unwrap_or(MAX_CONCURRENT_DOWNLOADS);
                break;
            }
        }

        let semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));

        'scraperloop: for scrap_data in scraper_data_return.iter() {
            let file_id_tag_map = Arc::new(Mutex::new(HashMap::new()));
            let job_list = Arc::new(Mutex::new(Vec::new()));
            let tag_list = Arc::new(Mutex::new(Vec::new()));

            // Should skip processing a job if a skipif exists
            if self
                .download_manager
                .db
                .clone()
                .should_skip_processing_job(scrap_data.skip_conditions.clone())
                .await
            {
                continue;
            }

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
                                    let semaphore_clone = semaphore.clone();
                                    let permit = semaphore_clone.acquire_owned().await.unwrap();
                                    set.spawn(async move {
                                        let mut download_issue = false;
                                        let mut jobs = Vec::new();

                                        // Implements concurent ratelimit protection
                                        let _permit = permit;

                                        if let Some(fileinternal) = scraper
                                            .file_download_logic(
                                                &mut file,
                                                &mut jobs,
                                                &mut download_issue,
                                                temp_dir().as_path(),
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
                                        memory_manage();
                                    });

                                    while let Some(Ok(_)) = set.try_join_next() {
                                        // Explicitly drains finished tasks to release internal tracking heap allocations
                                    }
                                }
                                //set.join_all().await;
                                while set.join_next().await.is_some() {}
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
            } // Do all our happy db stuff down here :D
            let file_id_tag_map = Arc::try_unwrap(file_id_tag_map)
                .expect("Arc reference leak")
                .into_inner();

            let job_list = Arc::try_unwrap(job_list)
                .expect("Arc reference leak")
                .into_inner();

            let tags = Arc::try_unwrap(tag_list)
                .expect("Arc reference leak")
                .into_inner();

            self.download_manager
                .db
                .tags_add_bulk(&[FileTagAction {
                    operation: TagOperation::Add,
                    tags,
                }])
                .await;

            self.download_manager
                .db
                .clone()
                .process_scraper(file_id_tag_map, job_list)
                .await;
        }
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
            if let Some(_internal_storage) = self
                .download_manager
                .jobs
                .write()
                .await
                .get_mut(&self.plugin.name)
            {
                // internal_storage
                //     .completed_job_storage
                //     .push(self.job.clone());
            }

            self.download_manager
                .remove_job(&self.plugin, &self.job)
                .await;
        }

        // Cleans up memory
        memory_manage();
    }

    async fn manage_recreation(&self) -> bool {
        if let Some(recreation) = self.job.config.recreation.clone()
            && let DbJobRecreation::AlwaysTime(timestamp, count) = recreation
        {
            let mut job = self.job.clone();
            job.config.time = get_sys_time_in_secs();
            job.config.reptime = timestamp;
            job.isrunning = false;
            if let Some(mut count) = count {
                if count >= 1 {
                    count -= 1;
                } else {
                    return false;
                }
                job.config.recreation = Some(DbJobRecreation::AlwaysTime(timestamp, Some(count)));
            }

            self.download_manager.db.jobs_update(&job).await;
            return true;
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
    /*async fn file_download_logic(
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
                        .tag_get_file_id(&Tag {
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
                                "Scraper: {} JobId: {} Skipping file because already in db. {} file_id: {}",
                                self.plugin.name, self.job.id, &file_url, &file_id
                            );

                            return self.download_manager.db.file_id_get(file_id).await;
                        }
                        None => {
                            for skip in file.skip_if.iter() {
                                match skip {
                                    SkipIf::FileTagRelationship(tag) => {
                                        if let Some(file_id) =
                                            self.download_manager.db.tag_get_file_id(&tag).await
                                        {
                                            if let Some(file_internal) =
                                                self.download_manager.db.file_id_get(file_id).await
                                            {
                                                info!(
                                                    "Scraper: {} JobId: {} Skipping file because skip_tag has file: {:?}",
                                                    self.plugin.name, self.job.id, &tag
                                                );
                                                return Some(file_internal);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }

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
                                if let Some(bytes_out) =
                                    self_clone.download_file(file_url, &file.hash).await
                                {
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
                                    "Scraper: {} JobId: {} Skipping file because internal file_urls already contains object. {}",
                                    self.plugin.name, self.job.id, &file_url
                                );

                                return None;
                            }
                        }
                    }
                }
                FileSource::Bytes(file_bytes) => bytes::Bytes::copy_from_slice(file_bytes),
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

        // 1. We have the temp_file from download_file
        let temp_file = downloaded_temp_path;

        // 2. Move the file handling completely into spawn_blocking
        let (hash, extension, storage_id_result) = tokio::task::spawn_blocking(move || {
            // Load the bytes into memory sequentially right here
            let bytes = Bytes::from(std::fs::read(&temp_file).ok().unwrap());

            let hash = hash_bytes(&bytes, &HashesSupported::Sha512("".into())).0;
            let extension = FileFormat::from_bytes(&bytes).extension().to_string();

            // Execute your .so plugin callback using the raw byte slice reference
            plugin_manager.callback_on_download(&bytes, &mut tags_owned, &mut jobs_owned);

            // Copy the file to its permanent storage location synchronously
            let mut storage_id = None;
            if let Some((file_storage_path, storage_id_db)) = file_download_location
                && let Some(parent_dir) = file_storage_path.parent()
            {
                if std::fs::create_dir_all(parent_dir).is_ok()
                    && std::fs::write(&file_storage_path, &bytes).is_ok()
                {
                    storage_id = Some(storage_id_db);
                }
            }

            // Cleanup the temporary downloaded file
            let _ = std::fs::remove_file(&temp_file);

            // CRITICAL: `bytes` falls out of scope here and is dropped IMMEDIATELY.
            // The memory is reclaimed before the async context even wakes up.
            (hash, extension, storage_id)
        })
        .await
        .unwrap();

        Some(FileInternal {
            id: None,
            hash,
            extension,
            storage_id,
        })

        /* (file.tag_list, *jobs) = rx.await.unwrap();

        let bytes_clone = bytes.clone();
        let (hash, extension) = tokio::task::spawn_blocking(move || {
            (
                hash_bytes(&bytes_clone, &HashesSupported::Sha512("".into())).0,
                FileFormat::from_bytes(&bytes_clone).extension().to_string(),
            )
        })
        .await
        .unwrap();

        let bytes_clone = bytes.clone();
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
                if create_dir_all(parent_dir).is_ok()
                    && tokio::fs::write(&file_storage_path, &bytes_clone).await.is_ok()
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
        })*/
    }*/
    /* async fn file_download_logic(
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
                        .tag_get_file_id(&Tag {
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
                                "Scraper: {} JobId: {} Skipping file because already in db. {} file_id: {}",
                                self.plugin.name, self.job.id, &file_url, &file_id
                            );

                            return self.download_manager.db.file_id_get(file_id).await;
                        }
                        None => {
                            for skip in file.skip_if.iter() {
                                match skip {
                                    SkipIf::FileTagRelationship(tag) => {
                                        if let Some(file_id) =
                                            self.download_manager.db.tag_get_file_id(&tag).await
                                        {
                                            if let Some(file_internal) =
                                                self.download_manager.db.file_id_get(file_id).await
                                            {
                                                info!(
                                                    "Scraper: {} JobId: {} Skipping file because skip_tag has file: {:?}",
                                                    self.plugin.name, self.job.id, &tag
                                                );
                                                return Some(file_internal);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }

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
                                if let Some(bytes_out) =
                                    self_clone.download_file(file_url, &file.hash).await
                                {
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
                                    "Scraper: {} JobId: {} Skipping file because internal file_urls already contains object. {}",
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
                if create_dir_all(parent_dir).is_ok()
                    && tokio::fs::write(&file_storage_path, &bytes)
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
    }*/

    async fn download_file(
        self: Arc<Self>,
        file_url: &str,
        hash: &Option<HashesSupported>,
        _temp_dir: &std::path::Path,
    ) -> Option<NamedTempFile> {
        let mut cnt = 0;
        let mut hash_cnt = 0;

        let url = match Url::parse(file_url) {
            Ok(u) => u,
            Err(err) => {
                log::error!("Error while parsing url {} {:?}", file_url, err);
                return None;
            }
        };

        loop {
            // Rate limiting
            while self.ratelimiter.check().is_err() {
                let jitter = rand::random::<u64>() % 50;
                tokio::time::sleep(Duration::from_millis(100 + jitter)).await;
            }

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
                    if cnt >= 3 {
                        break;
                    }
                    continue;
                }
            };

            let content_length_header = response.content_length();

            // Build unique temp file path
            let temp_file = tempfile::NamedTempFile::new().unwrap();
            let temp_file_path = temp_file.path().to_path_buf();

            let mut file = match File::create(&temp_file_path).await {
                Ok(f) => f,
                Err(err) => {
                    log::error!("Failed to create temporary file: {:?}", err);
                    return None;
                }
            };

            let mut downloaded_bytes_count = 0;
            let mut stream_failed = false;

            // Stream network chunks straight to disk (Constantly uses ~8KB to 64KB max per active task)
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
                        downloaded_bytes_count += chunk.len();
                        if let Err(err) = file.write_all(&chunk).await {
                            log::error!("Failed to write chunk to disk: {:?}", err);
                            stream_failed = true;
                            break;
                        }
                    }
                    Ok(None) => break, // Finished downloading successfully
                    Err(err) => {
                        log::error!(
                            "Scraper: {} JobId: {} Stream chunk error: {:?}",
                            self.plugin.name,
                            self.job.id,
                            err
                        );
                        stream_failed = true;
                        break;
                    }
                }
            }

            let _ = file.flush().await;

            if stream_failed {
                let _ = tokio::fs::remove_file(&temp_file).await;
                cnt += 1;
                if cnt >= 3 {
                    break;
                }
                continue;
            }

            let size_matches = match content_length_header {
                Some(expected) => downloaded_bytes_count == expected as usize,
                None => true,
            };

            if size_matches {
                let temp_path_clone = temp_file_path.clone();
                let hash_clone = hash.clone();

                // Compute validation hashes synchronously inside spawn_blocking
                let hash_matches = if let Some(hash_rule) = hash_clone {
                    tokio::task::spawn_blocking(move || {
                        if let Ok(bytes) = std::fs::read(&temp_path_clone) {
                            hash_bytes(&Bytes::from(bytes), &hash_rule).1
                        } else {
                            false
                        }
                    })
                    .await
                    .ok()
                    .unwrap_or(false)
                } else {
                    true
                };

                if hash_matches || hash_cnt >= 2 {
                    if hash.is_some() && hash_matches {
                        hash_cnt += 1;
                    }
                    if hash_cnt >= 2 {
                        info!("Overriding downloaded md5 with downloaded one.");
                    }
                    return Some(temp_file);
                } else {
                    log::warn!(
                        "Scraper: {} JobId: {} Hash mismatch detected. Retrying. Attempt: {}",
                        self.plugin.name,
                        self.job.id,
                        hash_cnt + 1
                    );
                    let _ = tokio::fs::remove_file(&temp_file).await;
                    hash_cnt += 1;
                }
            } else {
                log::error!(
                    "Scraper: {} JobId: {} Mismatched length. Downloaded {} Expected {:?}",
                    self.plugin.name,
                    self.job.id,
                    downloaded_bytes_count,
                    content_length_header
                );
                let _ = tokio::fs::remove_file(&temp_file).await;
            }

            cnt += 1;
            if cnt >= 3 {
                break;
            }
        }

        None
    }

    ///
    /// Downloads a singular file
    ///
    async fn file_download_logic(
        self: Arc<Self>,
        file: &mut FileObject,
        jobs: &mut Vec<ScraperDataReturn>,
        download_issue: &mut bool,
        temp_dir: &std::path::Path, // Added to provide path context to download_file
    ) -> Option<FileInternal> {
        let plugin_manager = self.download_manager.plugin_manager.clone();
        let self_clone = self.clone();

        // Download or fetch file via its disk path reference
        let temp_file = match file.source {
            None => return None,
            Some(ref url_source) => match url_source {
                FileSource::Url(file_url) => {
                    match self
                        .download_manager
                        .db
                        .tag_get_file_id(&Tag {
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
                                "Scraper: {} JobId: {} Skipping file because already in db.",
                                self.plugin.name, self.job.id
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

                            for skip in file.skip_if.iter() {
                                if let SkipIf::FileTagRelationship(tag) = skip
                                    && let Some(file_id) =
                                        self.download_manager.db.tag_get_file_id(tag).await
                                    && let Some(file_internal) =
                                        self.download_manager.db.file_id_get(file_id).await
                                {
                                    return Some(file_internal);
                                }
                            }

                          
                            if self.should_download_file(file_url).await {
                                // Calls disk streaming download helper
                                if let Some(path_out) = self_clone
                                    .download_file(file_url, &file.hash, temp_dir)
                                    .await
                                {
                                    TrackedFile::Temp(path_out)
                                } else {
                                    *download_issue = true;
                                    return None;
                                }
                            } else {
                                return None;
                            }
                        }
                    }
                }
                // If bytes are fed instantly, spill them out to a temporary file right away
                // to maintain identical architectural tracking shapes.
                FileSource::Bytes(file_bytes) => {
                    let path = temp_dir.join(format!("direct_bytes_{}.tmp", rand::random::<u32>()));
                    if std::fs::write(&path, file_bytes).is_err() {
                        return None;
                    }
                    TrackedFile::Manual(path)
                }
            },
        };

        let mut tags_owned = file.tag_list.clone();
        let mut jobs_owned = jobs.clone();

        let temp_file_path = temp_file.path().to_path_buf();

        // RUN EVERYTHING HEAVY SEQUENTIALLY ON THE THREAD POOL
        let (hash, extension, storage_id_result, final_tags, final_jobs) =
            tokio::task::spawn_blocking(move || {
                // 1. Read bytes from local disk (Hits OS Page Cache, near instantaneous)
                let bytes = Bytes::from(std::fs::read(&temp_file_path).ok().unwrap());

                // 2. Compute format and layout
                let hash = hash_bytes(&bytes, &HashesSupported::Sha512("".into())).0;
                let extension = FileFormat::from_bytes(&bytes).extension().to_string();

                let file_download_location = self
                    .download_manager
                    .db
                    .file_download_location_get_sync(&hash, &extension);

                // 3. Fire your dynamic plugin boundary (.so loading boundary takes a slice safely)
                plugin_manager.callback_on_download(&bytes, &mut tags_owned, &mut jobs_owned);

                // 4. Save file out to its designated destination location path context
                let mut storage_id = None;
                if let Some((file_storage_path, storage_id_db)) = file_download_location
                    && let Some(parent_dir) = file_storage_path.parent()
                {
                    let mut cnt = 0;
                    loop {
                        if std::fs::create_dir_all(parent_dir).is_ok()
                            && std::fs::write(&file_storage_path, &bytes).is_ok()
                        {
                            storage_id = Some(storage_id_db);
                            break;
                        }
                        cnt += 1;
                        if cnt >= 3 {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }

                // Clean up the temporary file immediately
                let _ = std::fs::remove_file(&temp_file_path);

                // CRITICAL: `bytes` falls out of scope HERE.
                // Allocated heap memory drops back down to absolute zero before task returns.
                Some((hash, extension, storage_id, tags_owned, jobs_owned))
            })
            .await
            .unwrap()
            .into_iter()
            .next()?;

        file.tag_list = final_tags;
        *jobs = final_jobs;

        let storage_id = match storage_id_result {
            Some(id) => id,
            None => return None,
        };

        Some(FileInternal {
            id: None,
            hash,
            extension,
            storage_id,
        })
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
            job_limiter: Arc::new(Semaphore::new(3)),
        };

        dm.into()
    }
    ///
    /// Loads the logins in from the DB and inserts them into the job parameters.
    ///
    fn load_logins(&self, plugin: &Plugin) -> Vec<ScraperParam> {
        let mut out = Vec::new();
        if let Some(api_key) = self
            .db
            .setting_get_sync(&format!("PLUGIN_{}_API_KEY", plugin.name))
            && let Some(api_pass) = self
                .db
                .setting_get_sync(&format!("PLUGIN_{}_API_PASS", plugin.name))
        {
            out.push(ScraperParam::Login(LoginType::Api(
                api_key.param.unwrap(),
                api_pass.param,
            )));
        }

        out
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
                // Sets up ratelimit for job
                let mut ratelimit = None;
                for properties in plugin.properties.iter() {
                    if let PluginProperties::Ratelimit(num, duration) = properties {
                        let hits = *num;
                        let total_duration = *duration;

                        info!(
                            "DownloadManager: Creating Ratelimiter with properties: {} tries per: {:?}",
                            hits, total_duration
                        );

                        // Guard against division by zero just in case
                        let hits_nonzero = std::num::NonZeroU32::new(hits)
                            .unwrap_or(std::num::NonZeroU32::new(1).unwrap());

                        // Calculate how long it takes to regenerate ONE single cell
                        let cell_replenish_interval = total_duration / hits_nonzero.get();

                        ratelimit = Some(
                            Quota::with_period(cell_replenish_interval)
                                .unwrap()
                                .allow_burst(hits_nonzero),
                        );
                    }
                }
                if ratelimit.is_none() {
                    info!(
                        "DownloadManager: Creating Ratelimiter with properties: {} tries per: {:?}",
                        1,
                        Duration::from_secs(1)
                    );
                    ratelimit = Some(
                        Quota::with_period(Duration::from_secs(1))
                            .unwrap()
                            .allow_burst(std::num::NonZeroU32::new(1).unwrap()),
                    )
                }

                // Check if we need to load login data
                self.handle_login_db(plugin);

                // Loads login parameters from db into job
                let mut job_storage = job_storage.clone();
                for job in job_storage.iter_mut() {
                    for login in self.load_logins(plugin) {
                        job.config.param.push(login);
                    }
                }

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
    /// Handles loading in login information into the db
    ///
    fn handle_login_db(self: &Arc<Self>, plugin: &Plugin) {
        for property in plugin.properties.iter() {
            if let PluginProperties::Login((login_need, login_type)) = property
                && login_need == &LoginNeed::Required
            {
                match login_type {
                    LoginType::Api(key, api) => {
                        if self
                            .db
                            .setting_get_sync(&format!("PLUGIN_{}_{}", plugin.name, "API_KEY"))
                            .is_none()
                        {
                            dbg!(&plugin.name, &key, &api);
                            let mut user_name = String::new();
                            print!("Api Key: ");
                            let _ = stdout().flush();
                            stdin()
                                .read_line(&mut user_name)
                                .expect("Did not enter a correct string");
                            if let Some('\n') = user_name.chars().next_back() {
                                user_name.pop();
                            }
                            if let Some('\r') = user_name.chars().next_back() {
                                user_name.pop();
                            }
                            let mut user_pass = String::new();
                            print!("Password: ");
                            let _ = stdout().flush();
                            stdin()
                                .read_line(&mut user_pass)
                                .expect("Did not enter a correct string");
                            if let Some('\n') = user_pass.chars().next_back() {
                                user_pass.pop();
                            }
                            if let Some('\r') = user_pass.chars().next_back() {
                                user_pass.pop();
                            }
                            self.db.setting_set_sync(&DbSettingsObj {
                                name: format!("PLUGIN_{}_API_KEY", plugin.name),
                                description: Some("API Login for site.".into()),
                                num: None,
                                param: Some(user_name),
                            });
                            self.db.setting_set_sync(&DbSettingsObj {
                                name: format!("PLUGIN_{}_API_PASS", plugin.name),
                                description: Some("API Login for site.".into()),
                                num: None,
                                param: Some(user_pass),
                            });
                        }
                    }
                    LoginType::ApiNamespaced(ns, key, api)
                        if self
                            .db
                            .setting_get_sync(&format!(
                                "PLUGIN_{}_{}_{}",
                                plugin.name, ns, "API_NS"
                            ))
                            .is_none() =>
                    {
                        dbg!(&key, &api);
                    }
                    _ => {}
                }
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

            let permit = match self.job_limiter.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    drop(guard);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };

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

                tokio::task::spawn(async move {
                    let _permit = permit;
                    scraper.run_scraper().await
                });
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
                info!("Parser returned: {} Got: {:?}", hash, digest);
            }
            (format!("{:x}", digest), &format!("{:x}", digest) == hash)
        }
        HashesSupported::Sha1(hash) => {
            let mut hasher = sha1::Sha1::new();
            hasher.update(bytes);
            let hastring = encode_upper(hasher.finalize());
            let dune = &hastring == hash;
            if !dune && !hash.is_empty() {
                info!("Parser returned: {} Got: {}", hash, hastring);
            }
            (hastring, dune)
        }
        HashesSupported::Sha256(hash) => {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            let hastring = encode_upper(hasher.finalize());
            let dune = &hastring == hash;
            if !dune && !hash.is_empty() {
                info!("Parser returned: {} Got: {}", hash, hastring);
            }
            (hastring, dune)
        }
        HashesSupported::Sha512(hash) => {
            let hasher = Sha512::digest(bytes);
            let hastring = encode_upper(hasher);
            let dune = &hastring == hash;
            if !dune && !hash.is_empty() {
                info!("Parser returned: {} Got: {}", hash, hastring);
            }
            (hastring, dune)
        }
    }
}
