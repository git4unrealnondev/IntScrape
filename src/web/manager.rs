use file_format::FileFormat;
use hex::encode_upper;
use image_hasher::{BitOrder, HasherConfig, Image};
use rayon::ThreadPool;
use sha2::Digest;
use std::{
    collections::{HashMap, HashSet},
    env::temp_dir,
    io::{Cursor, Write, stdin, stdout},
    mem::discriminant,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use strum::IntoEnumIterator;
use tempfile::NamedTempFile;
use url::Url;

use bytes::Bytes;
use governor::{DefaultDirectRateLimiter, Quota};
use log::info;
use reqwest::{Client, StatusCode};
use sha2::{Sha256, Sha512};
use shared_types::{
    DbJobRecreation, DbJobsObj, DbSettingsObj, FileInternal, FileManager, FileObject, FileSource,
    FileTagAction, GenericNamespaceObj, HashesSupported, LoginNeed, LoginType, Plugin,
    PluginProperties, PluginTag, ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf, Tag,
    TagOperation,
};
use std::error::Error;
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{Mutex, RwLock, Semaphore},
    task::JoinSet,
};

const MAX_CONCURRENT_DOWNLOADS: usize = 5;
const MAX_CONCURRENT_JOBS: usize = 1000;
const MAX_CONCURRENT_PROCESSING: usize = 20;
const MAX_DOWNLOAD_RETRIES: u8 = 3;

fn retry_delay(attempt: u8) -> Duration {
    let backoff_seconds = 1u64 << attempt.min(4);
    Duration::from_secs(backoff_seconds) + Duration::from_millis(rand::random::<u64>() % 500)
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_EARLY
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

use crate::{
    db::MainDatabase,
    helper_functions::{self, get_sys_time_in_secs, memory_manage},
    plugins::PluginManager,
    web::FileReturn,
};

enum TrackedFile {
    Temp(tempfile::NamedTempFile),
}

#[derive(Clone)]
enum DownloadHasher {
    Md5(md5::Context),
    Sha1(sha1::Sha1),
    Sha256(Sha256),
    Sha512(Sha512),
    IpfsCid(Vec<u8>),
    IpfsCid1(Vec<u8>),
    ImageHash(Vec<u8>),
}

impl DownloadHasher {
    fn new(hash: &HashesSupported) -> Self {
        match hash {
            HashesSupported::Md5(_) => Self::Md5(md5::Context::new()),
            HashesSupported::Sha1(_) => Self::Sha1(sha1::Sha1::new()),
            HashesSupported::Sha256(_) => Self::Sha256(Sha256::new()),
            HashesSupported::Sha512(_) => Self::Sha512(Sha512::new()),
            HashesSupported::IPFSCID(_) => Self::IpfsCid(Vec::new()),
            HashesSupported::IPFSCID1(_) => Self::IpfsCid1(Vec::new()),
            HashesSupported::ImageHash(_) => Self::ImageHash(Vec::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Md5(hasher) => hasher.consume(bytes),
            Self::Sha1(hasher) => hasher.update(bytes),
            Self::Sha256(hasher) => hasher.update(bytes),
            Self::Sha512(hasher) => hasher.update(bytes),
            Self::IpfsCid(store) => store.append(&mut bytes.to_vec()),
            Self::IpfsCid1(store) => store.append(&mut bytes.to_vec()),
            Self::ImageHash(store) => store.append(&mut bytes.to_vec()),
        }
    }

    fn finish(self) -> Option<String> {
        match self {
            Self::Md5(hasher) => Some(format!("{:x}", hasher.finalize())),
            Self::Sha1(hasher) => Some(encode_upper(hasher.finalize())),
            Self::Sha256(hasher) => Some(encode_upper(hasher.finalize())),
            Self::Sha512(hasher) => Some(encode_upper(hasher.finalize())),
            Self::IpfsCid(storage) => ipfs_cid::generate_cid_v0(&storage).ok(),
            Self::IpfsCid1(storage) => ipfs_cid::generate_cid_v1(&storage).ok(),
            Self::ImageHash(storage) => {
                let hasher = HasherConfig::new()
                    .hash_alg(image_hasher::HashAlg::Median)
                    .bit_order(BitOrder::MsbFirst)
                    .preproc_dct()
                    .to_hasher();
                if let Ok(img) = image::ImageReader::new(Cursor::new(storage)).with_guessed_format()
                    && let Ok(decode) = img.decode()
                {
                    return Some(hasher.hash_image(&decode).to_base64());
                }
                None
            }
        }
    }
}

impl TrackedFile {
    // A helper method to easily extract the path reference
    fn path(&self) -> &std::path::Path {
        match self {
            Self::Temp(f) => f.path(),
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
    job_limiter: Arc<Semaphore>,
    //Stores file urls that we're downloading
    file_urls: HashSet<String>,
}

pub struct DownloadsManager {
    pub db: Arc<MainDatabase>,
    plugin_manager: Arc<PluginManager>,
    jobs: RwLock<HashMap<String, InternalStorage>>,
    downloading_urls: RwLock<HashSet<String>>,
    heavy_processing_pool: Arc<ThreadPool>,
    //job_limiter: Arc<Semaphore>,
    processing_limiter: Arc<Semaphore>,
    should_exit: Arc<AtomicBool>,
    active_file_processing: Arc<AtomicUsize>,
}

struct FileProcessingGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for FileProcessingGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
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

        for modifier in &plugin.properties {
            if let PluginProperties::Modifier(target) = modifier {
                modifiers.push(target.clone());
            }
        }

        let scraper = Self {
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
    pub async fn run_scraper(self: Arc<Self>) -> Result<(), Box<dyn Error>> {
        let plugin_manager = self.plugin_manager.clone();
        let job = self.job.config.clone();
        let plugin = self.plugin.clone();
        let plugin_name = self.plugin.name.clone();
        let job_id = self.job.id.clone();

        let scraper_data_return;

        let default_scraper_data = ScraperDataReturn {
            job,
            skip_conditions: vec![],
        };

        match tokio::task::spawn_blocking(move || {
            plugin_manager.url_dump(&default_scraper_data, &plugin)
        })
        .await
        {
            Ok(scrap_data) => match scrap_data {
                Ok(good_data) => {
                    scraper_data_return = Arc::new(good_data);
                }
                Err(e) => {
                    log::error!(
                        "DownloadManager: While getting a dump of URL's to do {} had an error {:?}",
                        plugin_name,
                        e
                    );
                    self.download_manager
                        .remove_job(&self.plugin, &self.job, false)
                        .await;
                    return Err("Scrap data had an error. Check logs".into());
                }
            },
            Err(e) => {
                return Err(format!("Url Dump had error: {e}").into());
            }
        }

        let should_remove_job = Arc::new(AtomicBool::new(true));

        // Sets the max number of concurrent downloads
        let mut max_concurrent_downloads = MAX_CONCURRENT_DOWNLOADS;

        // Sets the concurrent number of items that can be downloaded at once
        for property in &self.plugin.properties {
            if let PluginProperties::ThreadNum(thread_num) = property {
                max_concurrent_downloads =
                    (*thread_num).try_into().unwrap_or(MAX_CONCURRENT_DOWNLOADS);
                break;
            }
        }

        let semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));
        let plugin_handles_text = self
            .plugin
            .properties
            .iter()
            .any(|property| matches!(property, PluginProperties::NoTextDownload));

        'scraperloop: for scrap_data in scraper_data_return.iter() {
            let file_id_tag_map = Arc::new(Mutex::new(HashMap::new()));
            let job_list = Arc::new(Mutex::new(Vec::new()));
            let tag_list = Arc::new(Mutex::new(Vec::new()));

            if self
                .clone()
                .download_manager
                .should_exit
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }

            // Should skip processing a job if a skipif exists
            if self
                .download_manager
                .db
                .clone()
                .should_skip_processing_job(scrap_data.skip_conditions.clone())
                .await
            {
                continue 'scraperloop;
            }

            for param in scrap_data.job.param.iter() {
                if self
                    .download_manager
                    .should_exit
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    log::error!(
                        "Worker: {} JobId: {} -- STOPPING JOB {} will retry next boot.",
                        plugin_name,
                        job_id,
                        "due to CTRL-C handler"
                    );
                    should_remove_job.store(false, std::sync::atomic::Ordering::Relaxed);
                    break 'scraperloop;
                }

                let text_result = if plugin_handles_text {
                    match param {
                        ScraperParam::Url(url) => Some((String::new(), url.url.clone())),
                        ScraperParam::UrlPost(url) => Some((String::new(), url.url.clone())),
                        _ => None,
                    }
                } else {
                    let scraper = self.clone();
                    let param_clone = param.clone();
                    let should_remove_job_clone = should_remove_job.clone();
                    tokio::spawn(async move {
                        scraper
                            .dltext(param_clone, should_remove_job_clone.clone())
                            .await
                    })
                    .await
                    .ok()
                    .flatten()
                };

                if let Some((text, source_url)) = text_result {
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
                        if self
                            .download_manager
                            .should_exit
                            .load(std::sync::atomic::Ordering::SeqCst)
                        {
                            log::error!(
                                "Worker: {} JobId: {} -- STOPPING JOB {} will retry next boot.",
                                plugin_name,
                                job_id,
                                "due to CTRL-C handler"
                            );
                            should_remove_job.store(false, std::sync::atomic::Ordering::Relaxed);
                            break 'scraperloop;
                        }

                        match data {
                            // File data thats actionable
                            ScraperReturn::Data(scraper_object) => {
                                if scraper_object.files.is_empty()
                                    && scraper_object.tags.is_empty()
                                    && scraper_object.jobs.is_empty()
                                {
                                    log::info!(
                                        "Worker: {} JobId: {} -- STOPPING JOB due to files, tags & jobs being empty inside of a valid scraperreturn data object.",
                                        plugin_name,
                                        job_id,
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
                                    let plugin_name = plugin_name.clone();
                                    let permit = semaphore_clone.acquire_owned().await?;
                                    scraper
                                        .download_manager
                                        .active_file_processing
                                        .fetch_add(1, Ordering::SeqCst);
                                    set.spawn(async move {
                                        let _processing_guard = FileProcessingGuard {
                                            active: scraper
                                                .download_manager
                                                .active_file_processing
                                                .clone(),
                                        };
                                        let mut download_issue = false;
                                        let mut jobs = Vec::new();

                                        // Implements concurent ratelimit protection
                                        let _permit = permit;

                                        let file_result = scraper
                                            .file_download_logic(
                                                &mut file,
                                                &mut jobs,
                                                &mut download_issue,
                                            )
                                            .await.map_err(|err| err.to_string());
                                        match file_result
                                        {
                                            Ok(Some(filereturn)) => {
                                                // Adds jobs from the on_download callback
                                                job_list_clone.lock().await.extend(jobs);

                                                match filereturn {
                                                    FileReturn::File(fileinternal) => {
                                                // Adds tag data from the file
                                                file_id_tag_map_clone
                                                    .lock()
                                                    .await
                                                    .insert(fileinternal, file.tag_list);
                                                    },
                                                    FileReturn::InDownloadQueue => {

                                                    }
                                                }

                                            }
                                            Ok(None) => {
                                                log::error!(
                                                    "Worker: {} JobId: {} -- File: {:?} had None?? This shouldn't happen unless an error or bad data got in.",
                                                    plugin_name,
                                                    job_id,
                                                    file,
                                                );
                                            }
                                            Err(err) => {
                                                log::error!(
                                                    "Worker: {} JobId: {} -- File: {:?} had err {}",
                                                    plugin_name,
                                                    job_id,
                                                    file,
                                                    err
                                                );
                                            }
                                        }

                                        if download_issue {
                                            should_remove_job_clone
                                                .store(false, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        memory_manage();
                                    });

                                    while matches!(set.try_join_next(), Some(Ok(()))) {
                                        // Explicitly drains finished tasks to release internal tracking heap allocations
                                    }
                                }
                                //set.join_all().await;
                                while set.join_next().await.is_some() {}
                            }
                            // No objects were returned. IE no files
                            ScraperReturn::Nothing => {
                                log::info!(
                                    "Worker: {} JobId: {} -- STOPPING JOB due to getting NOTHNG",
                                    plugin_name,
                                    job_id,
                                );
                                break 'scraperloop;
                            }
                            // Some sort of site issue. Will retry on next db boot
                            ScraperReturn::Stop(stop_reason) => {
                                log::error!(
                                    "Worker: {} JobId: {} -- STOPPING JOB {} will retry next boot.",
                                    plugin_name,
                                    job_id,
                                    stop_reason
                                );
                                should_remove_job
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                                break 'scraperloop;
                            }
                            ScraperReturn::Fatal(fatal_error) => {
                                log::error!(
                                    "Worker: {} JobId: {} -- Encountered fatal error: {}",
                                    plugin_name,
                                    job_id,
                                    fatal_error
                                );
                                todo!("Need to implement check logs for data");
                            }
                            ScraperReturn::RetryLater(later_duration) => {
                                let mut job = self.job.clone();
                                job.isrunning = false;
                                job.config.time = helper_functions::get_sys_time_in_secs();
                                job.config.reptime = later_duration;
                                self.download_manager.db.jobs_update(&job).await;
                                should_remove_job
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                                break 'scraperloop;
                            }
                        }
                    }
                }
            }

            // Do all our happy db stuff down here :D
            let file_id_tag_map = Arc::try_unwrap(file_id_tag_map)
                .expect("Arc reference leak")
                .into_inner();

            let job_list = Arc::try_unwrap(job_list)
                .expect("Arc reference leak")
                .into_inner();

            let tags = Arc::try_unwrap(tag_list)
                .expect("Arc reference leak")
                .into_inner();

            let audit_reason = format!("scraper: {plugin_name}");

            self.download_manager
                .db
                .tags_add_bulk(
                    &[FileTagAction {
                        operation: TagOperation::Add,
                        tags,
                    }],
                    &audit_reason,
                )
                .await;

            self.download_manager
                .db
                .clone()
                .process_scraper(file_id_tag_map, job_list, audit_reason)
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

        // Removes job if we need to
        self.download_manager
            .remove_job(
                &self.plugin,
                &self.job,
                should_remove_job.load(std::sync::atomic::Ordering::Relaxed),
            )
            .await;

        // Runs regex on new tags added in previous steps.
        // NOTE RUNS GLOBALLY so if ANY job adds tags to be checked then this will run them
        let _ = tokio::task::spawn_blocking(move || {
            self.plugin_manager.callback_tag_regex();
        })
        .await;

        /* if  {
          /*  // Updates internal jobs cache
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
            }*/


        }*/

        // Cleans up memory
        memory_manage();
        Ok(())
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
        self.download_manager
            .downloading_urls
            .write()
            .await
            .insert(url.to_string())
    }

    async fn release_download_file(&self, url: &str) {
        self.download_manager
            .downloading_urls
            .write()
            .await
            .remove(url);
    }

    async fn download_file(
        self: Arc<Self>,
        file_url: &str,
        hash: &Vec<HashesSupported>,
        temp_dir: &std::path::Path,
    ) -> Option<NamedTempFile> {
        let mut cnt = 0;

        let url = match Url::parse(file_url) {
            Ok(u) => u,
            Err(err) => {
                log::error!("Error while parsing url {file_url} {err:?}");
                return None;
            }
        };

        'main_loop: loop {
            if self
                .download_manager
                .should_exit
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }
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
                    if cnt >= MAX_DOWNLOAD_RETRIES {
                        break;
                    }
                    tokio::time::sleep(retry_delay(cnt)).await;
                    cnt += 1;
                    continue;
                }
            };
            if let Err(err) = response.error_for_status_ref() {
                let status = err.status();
                if status == Some(StatusCode::NOT_FOUND) || status == Some(StatusCode::GONE) {
                    log::error!(
                        "Scraper: {} JobId: {} While processing url: {} got a 404 error adding dead_url to db.",
                        self.plugin.name,
                        self.job.id,
                        file_url,
                    );
                    let db = self.download_manager.db.clone();
                    db.dead_url_add_async(file_url.to_string()).await;
                } else if status.is_some_and(is_retryable_status) && cnt < MAX_DOWNLOAD_RETRIES {
                    log::error!(
                        "Scraper: {} JobId: {} Got retryable error {:?}; retrying",
                        self.plugin.name,
                        self.job.id,
                        err
                    );
                    tokio::time::sleep(retry_delay(cnt)).await;
                    cnt += 1;
                    continue 'main_loop;
                } else {
                    log::error!(
                        "Scraper: {} JobId: {} Got permanent error {:?}",
                        self.plugin.name,
                        self.job.id,
                        err
                    );
                }

                break 'main_loop;
            }

            let content_length_header = response.content_length();

            // Build unique temp file path
            let temp_file = match tempfile::NamedTempFile::new_in(temp_dir) {
                Ok(file) => file,
                Err(error) => {
                    log::error!("Failed to create temporary file: {error:?}");
                    return None;
                }
            };
            let temp_file_path = temp_file.path().to_path_buf();

            let mut file = match File::create(&temp_file_path).await {
                Ok(f) => f,
                Err(err) => {
                    log::error!("Failed to create temporary file: {err:?}");
                    return None;
                }
            };

            let mut downloaded_bytes_count = 0;
            let mut stream_failed = false;
            // Stream network chunks straight to disk (Constantly uses ~8KB to 64KB max per active task)
            loop {
                if self
                    .download_manager
                    .should_exit
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let _ = tokio::fs::remove_file(&temp_file).await;
                    return None;
                }
                let chunk = tokio::select! {
                    chunk = response.chunk() => chunk,
                    _ = async {
                        while !self
                            .download_manager
                            .should_exit
                            .load(std::sync::atomic::Ordering::SeqCst)
                        {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    } => {
                        let _ = tokio::fs::remove_file(&temp_file).await;
                        return None;
                    }
                };
                match chunk {
                    Ok(Some(chunk)) => {
                        downloaded_bytes_count += chunk.len();

                        if let Err(err) = file.write_all(&chunk).await {
                            log::error!("Failed to write chunk to disk: {err:?}");
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

            if let Err(err) = file.flush().await {
                log::error!("Failed to flush temporary file: {err:?}");
                let _ = tokio::fs::remove_file(&temp_file).await;
                if cnt >= MAX_DOWNLOAD_RETRIES {
                    break;
                }
                tokio::time::sleep(retry_delay(cnt)).await;
                cnt += 1;
                continue;
            }

            if stream_failed {
                let _ = tokio::fs::remove_file(&temp_file).await;
                if cnt >= MAX_DOWNLOAD_RETRIES {
                    break;
                }
                tokio::time::sleep(retry_delay(cnt)).await;
                cnt += 1;
                continue;
            }

            let size_matches = match content_length_header {
                Some(expected) => downloaded_bytes_count as u64 == expected,
                None => true,
            };

            if size_matches {
                let hash_inputs = hash.clone();
                let hash_path = temp_file.path().to_path_buf();
                let processing_pool = self.download_manager.heavy_processing_pool.clone();
                let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
                processing_pool.spawn(move || {
                    let hash_matches = std::fs::read(&hash_path)
                        .map(|bytes| {
                            let bytes = Bytes::from(bytes);
                            hash_inputs.iter().all(|hash| hash_bytes(&bytes, hash).1)
                        })
                        .unwrap_or(false);
                    let _ = result_sender.send(hash_matches);
                });
                let hash_matches = result_receiver.await.unwrap_or(false);

                if hash_matches {
                    return Some(temp_file);
                } else {
                    log::warn!(
                        "Scraper: {} JobId: {} Hash mismatch detected. Retrying. Attempt: {}",
                        self.plugin.name,
                        self.job.id,
                        cnt + 1
                    );
                    let _ = tokio::fs::remove_file(&temp_file).await;
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

            if cnt >= MAX_DOWNLOAD_RETRIES {
                break;
            }
            if self
                .download_manager
                .should_exit
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }
            tokio::time::sleep(retry_delay(cnt)).await;
            cnt += 1;
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
    ) -> Result<Option<FileReturn>, Box<dyn Error>> {
        let plugin_manager = self.download_manager.plugin_manager.clone();
        let self_clone = self.clone();
        let should_exit = self.download_manager.should_exit.clone();

        if should_exit.load(Ordering::SeqCst) {
            return Ok(None);
        }

        if let Some(file_url) = file.source.as_ref().and_then(|source| match source {
            FileSource::Url(file_url) => Some(file_url.clone()),
            FileSource::Bytes(_) => None,
        }) {
            Self::add_source_url_tag(file, &file_url);
        }

        // Skips downloading file IF we have tag x associated with file_id
        for skip in &file.skip_if {
            if should_exit.load(Ordering::SeqCst) {
                return Ok(None);
            }
            if let SkipIf::FileTagRelationship(tag) = skip
                && let Some(file_id) = self.download_manager.db.tag_get_file_id(tag).await
                && let Some(file_internal) = self.download_manager.db.file_id_get(file_id).await
            {
                info!(
                    "Scraper: {} JobId: {} Skipping file_id {} because TAG: {:?} already in db.",
                    self.plugin.name,
                    self.job.id,
                    file_internal.id.unwrap_or(0),
                    tag
                );
                return Ok(Some(FileReturn::File(file_internal.into())));
            }
        }

        // Check the source URL before doing hash lookups.
        if let Some(file_url) = file.source.as_ref().and_then(|source| match source {
            FileSource::Url(file_url) => Some(file_url),
            FileSource::Bytes(_) => None,
        }) && let Some(file_id) = self
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
            info!(
                "Scraper: {} JobId: {} Skipping file_id {} because URL: {} already in db.",
                self.plugin.name, self.job.id, file_id, file_url
            );
            return Ok(self
                .download_manager
                .db
                .file_id_get(file_id)
                .await
                .map(|f| FileReturn::File(f.into())));
        }

        // Associates file hashes with a file object
        for hash in file.hash.iter() {
            if should_exit.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let db = self.download_manager.db.clone();
            let lookup_hash = hash.clone();
            let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
            self.download_manager.heavy_processing_pool.spawn(move || {
                let _ = result_sender.send(db.contains_hash_sync(&lookup_hash));
            });
            let file_internal = result_receiver
                .await
                .map_err(|_| std::io::Error::other("hash lookup task failed"))?;

            if let Some(file_internal) = file_internal {
                info!(
                    "Scraper: {} JobId: {} Skipping file_id {} because hash: {:?} already in db.",
                    self.plugin.name,
                    self.job.id,
                    file_internal.id.unwrap_or(0),
                    hash
                );
                return Ok(Some(FileReturn::File(file_internal.into())));
            }
        }

        // Download or fetch file via its disk path reference
        let temp_file = match file.source {
            None => return Ok(None),
            Some(ref url_source) => {
                match url_source {
                    FileSource::Url(file_url) => {
                        if self.should_download_file(file_url).await {
                            // Calls disk streaming download helper
                            let downloaded = self_clone
                                .download_file(file_url, &file.hash, temp_dir().as_path())
                                .await;
                            self.release_download_file(file_url).await;
                            if let Some(path_out) = downloaded {
                                TrackedFile::Temp(path_out)
                            } else {
                                *download_issue = true;
                                return Ok(None);
                            }
                        } else {
                            log::info!(
                                "Worker: {} JobId: {} -- Skipping file download of url: {} due to already being in download queue.",
                                self.plugin.name,
                                self.job.id,
                                file_url
                            );
                            return Ok(Some(FileReturn::InDownloadQueue));
                        }
                    }
                    // If bytes are fed instantly, spill them out to a temporary file right away
                    // to maintain identical architectural tracking shapes.
                    FileSource::Bytes(file_bytes) => {
                        if should_exit.load(Ordering::SeqCst) {
                            return Ok(None);
                        }
                        let mut temp_file = tempfile::NamedTempFile::new_in(temp_dir())?;
                        temp_file.write_all(&file_bytes)?;
                        TrackedFile::Temp(temp_file)
                    }
                }
            }
        };

        let mut tags_owned = file.tag_list.clone();
        let mut jobs_owned = jobs.clone();
        let file_hash_owned = Arc::new(std::sync::Mutex::new(file.hash.clone()));

        let file_hash_local = file_hash_owned.clone();

        let temp_file_path = temp_file.path().to_path_buf();
        let processing_file_path = temp_file_path.clone();
        let processing_permit = tokio::select! {
            permit = self
                .download_manager
                .processing_limiter
                .clone()
                .acquire_owned() => permit?,
            _ = async {
                while !should_exit.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            } => return Ok(None),
        };
        let processing_pool = self.download_manager.heavy_processing_pool.clone();
        let db = self.download_manager.db.clone();
        let should_exit_for_processing = should_exit.clone();
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();

        // Run CPU-heavy hashing and plugin callbacks on the bounded Rayon pool.
        processing_pool.spawn(move || {
            let _processing_permit = processing_permit;
            let result = (|| {
                if should_exit_for_processing.load(Ordering::SeqCst) {
                    return Err("shutdown requested".to_string());
                }

                // Read the temporary file once for hashing, format detection, and callbacks.
                let bytes = Bytes::from(
                    std::fs::read(&processing_file_path)
                        .map_err(|error| format!("failed to read temporary file: {error}"))?,
                );

                if should_exit_for_processing.load(Ordering::SeqCst) {
                    return Err("shutdown requested".to_string());
                }

                // 2. Compute format and layout
                let hash = hash_bytes(&bytes, &HashesSupported::Sha512(String::new())).0;
                let extension = FileFormat::from_bytes(&bytes).extension().to_string();
                {
                    let mut hash_guard = file_hash_local.lock().unwrap();
                    hash_guard.push(HashesSupported::Sha512(hash.clone()));
                }

                let file_download_location = db.file_download_location_get_sync(&hash, &extension);

                if should_exit_for_processing.load(Ordering::SeqCst) {
                    return Err("shutdown requested".to_string());
                }

                // Callback plugins
                plugin_manager.callback_on_download(&bytes, &mut tags_owned, &mut jobs_owned);

                // Adds hash for other types onto hash if they dont exist
                for hash_type in HashesSupported::iter() {
                    if should_exit_for_processing.load(Ordering::SeqCst) {
                        return Err("shutdown requested".to_string());
                    }

                    // Check if the file's list contains this specific variant type
                    let exists = file_hash_local
                        .clone()
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|f_h| discriminant(f_h) == discriminant(&hash_type));

                    if !exists {
                        let hash_string = hash_bytes(&bytes, &hash_type).0;

                        let new_hash = match hash_type {
                            HashesSupported::Md5(_) => HashesSupported::Md5(hash_string),
                            HashesSupported::Sha1(_) => HashesSupported::Sha1(hash_string),
                            HashesSupported::Sha256(_) => HashesSupported::Sha256(hash_string),
                            HashesSupported::Sha512(_) => HashesSupported::Sha512(hash_string),
                            HashesSupported::IPFSCID(_) => HashesSupported::IPFSCID(hash_string),
                            HashesSupported::IPFSCID1(_) => HashesSupported::IPFSCID1(hash_string),
                            HashesSupported::ImageHash(_) => {
                                HashesSupported::ImageHash(hash_string)
                            }
                        };

                        {
                            let mut hash_guard = file_hash_local.lock().unwrap();
                            hash_guard.push(new_hash);
                        }
                    }
                }

                // 4. Save file out to its designated destination location path context
                if should_exit_for_processing.load(Ordering::SeqCst) {
                    return Err("shutdown requested".to_string());
                }
                if let Some((file_storage_path, storage_id_db)) = file_download_location
                    && let Some(parent_dir) = file_storage_path.parent()
                {
                    std::fs::create_dir_all(parent_dir)
                        .map_err(|error| format!("failed to create storage directory: {error}"))?;

                    let staging_path = file_storage_path.with_extension("part");
                    if std::fs::rename(&processing_file_path, &staging_path).is_err() {
                        std::fs::copy(&processing_file_path, &staging_path)
                            .map_err(|error| format!("failed to stage file: {error}"))?;
                    }
                    std::fs::rename(&staging_path, &file_storage_path)
                        .map_err(|error| format!("failed to finalize file: {error}"))?;
                    let storage_id = storage_id_db;
                    Ok::<_, String>((
                        hash,
                        extension,
                        Some(storage_id),
                        tags_owned,
                        jobs_owned,
                        Some(bytes.len() as u64),
                    ))
                } else {
                    Err("no file storage location configured".to_string())
                }
            })();
            let _ = result_sender.send(result);
        });

        let processed = result_receiver
            .await
            .map_err(|_| "file processing task failed: processing pool stopped");
        // The temp file is no longer needed after the processing task completes.
        // This also covers storage/callback errors instead of relying only on Drop.
        let _ = tokio::fs::remove_file(&temp_file_path).await;
        let processed = match processed {
            Ok(Ok(processed)) => processed,
            Ok(Err(_error)) if should_exit.load(Ordering::SeqCst) => return Ok(None),
            Ok(Err(error)) => return Err(error.into()),
            Err(error) => return Err(error.into()),
        };

        if should_exit.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let (hash, extension, storage_id_result, final_tags, final_jobs, size_bytes) = processed;

        file.tag_list = final_tags;
        *jobs = final_jobs;

        let storage_id = match storage_id_result {
            Some(id) => id,
            None => return Ok(None),
        };

        let file_manager = FileManager {
            internal: FileInternal {
                id: None,
                hash,
                extension,
                storage_id,
                size_bytes,
            },
            identifying_hashes: file_hash_owned.lock().unwrap().clone(),
        };

        Ok(Some(FileReturn::File(file_manager)))
    }

    fn add_source_url_tag(file: &mut FileObject, file_url: &str) {
        let already_present = file.tag_list.iter().any(|action| {
            action.tags.iter().any(|plugin_tag| {
                plugin_tag.tag.name == file_url && plugin_tag.tag.namespace.name == "source_url"
            })
        });
        if already_present {
            return;
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
    }
}

impl DownloadsManager {
    pub fn new(
        db: Arc<MainDatabase>,
        plugin_manager: Arc<PluginManager>,
        heavy_processing_pool: Arc<ThreadPool>,
        should_exit: Arc<AtomicBool>,
    ) -> Arc<Self> {
        let dm = Self {
            db,
            plugin_manager,
            jobs: HashMap::new().into(),
            downloading_urls: HashSet::new().into(),
            heavy_processing_pool,
            //job_limiter: Arc::new(Semaphore::new(3)),
            processing_limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_PROCESSING)),
            should_exit,
            active_file_processing: Arc::new(AtomicUsize::new(0)),
        };

        dm.into()
    }
    ///
    /// Loads the logins in from the DB and inserts them into the job parameters.
    ///
    fn load_logins(&self, plugin: &Plugin) -> Vec<ScraperParam> {
        let mut out = Vec::new();

        for property in &plugin.properties {
            if let PluginProperties::Login((_loginneed, logintype)) = property {
                if let LoginType::Cookie(name, _) = logintype {
                    if let Some(api_key) = self
                        .db
                        .setting_get_sync(&format!("PLUGIN_{}_{}_COOKIE", plugin.name, name))
                    {
                        out.push(ScraperParam::Login(LoginType::Cookie(
                            name.clone(),
                            Some(api_key.param.unwrap()),
                        )));
                    }
                }
            }
        }

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
    pub async fn remove_job(&self, plugin: &Plugin, job: &DbJobsObj, should_remove_db: bool) {
        // If all jobs are finished then remove plugin from db
        let mut remove_plugin = false;

        if let Some(internal_storage) = self.jobs.write().await.get_mut(&plugin.name) {
            internal_storage.job_storage.retain(|f| f != job);
            if should_remove_db {
                self.db.job_remove(job).await;
            }
            if internal_storage.job_storage.is_empty() {
                remove_plugin = true;
            }
        }

        if remove_plugin {
            self.jobs.write().await.remove(&plugin.name);

            // Rebuild counts and entries once the final queued job has finished.
            self.db.refresh_tag_search_cache();
        }
    }

    ///
    /// Returns status of any Plugins or such
    ///
    pub async fn all_jobs_complete(&self) -> bool {
        self.jobs.read().await.is_empty()
    }

    pub async fn downloads_complete(&self) -> bool {
        self.downloading_urls.read().await.is_empty()
            && self.active_file_processing.load(Ordering::SeqCst) == 0
    }

    ///
    /// Adds jobs into internal structure
    ///
    pub async fn add_jobs(
        self: &Arc<Self>,
        jobs: HashMap<shared_types::Plugin, Vec<shared_types::DbJobsObj>>,
    ) {
        let mut jobs_guard = self.jobs.write().await;

        for (plugin, job_storage) in &jobs {
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
                for properties in &plugin.properties {
                    if let PluginProperties::Ratelimit(num, duration) = properties {
                        let hits = *num;
                        let total_duration = *duration;

                        info!(
                            "DownloadManager: Creating Ratelimiter with properties: {hits} tries per: {total_duration:?}"
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
                    );
                }

                // Check if we need to load login data
                let _ = self.handle_login_db(plugin);

                let max_concurrent_jobs = plugin
                    .properties
                    .iter()
                    .find_map(|property| match property {
                        PluginProperties::JobNum(job_num) => (*job_num).try_into().ok(),
                        _ => None,
                    })
                    .unwrap_or(MAX_CONCURRENT_JOBS);

                // Loads login parameters from db into job
                let mut job_storage = job_storage.clone();
                for job in &mut job_storage {
                    for login in self.load_logins(plugin) {
                        job.config.param.push(login);
                    }
                }

                jobs_guard.insert(
                    plugin.name.clone(),
                    InternalStorage {
                        plugin: plugin.clone(),
                        job_storage: job_storage.clone(),
                        ratelimiter: Arc::new(governor::RateLimiter::direct(ratelimit.unwrap())),
                        job_limiter: Arc::new(Semaphore::new(max_concurrent_jobs)),
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
    fn handle_login_db(self: &Arc<Self>, plugin: &Plugin) -> Result<(), Box<dyn Error>> {
        for property in &plugin.properties {
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
                            stdin().read_line(&mut user_name)?;
                            if user_name.ends_with('\n') {
                                user_name.pop();
                            }
                            if user_name.ends_with('\r') {
                                user_name.pop();
                            }
                            let mut user_pass = String::new();
                            print!("Password: ");
                            let _ = stdout().flush();
                            stdin().read_line(&mut user_pass)?;
                            if user_pass.ends_with('\n') {
                                user_pass.pop();
                            }
                            if user_pass.ends_with('\r') {
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
                        dbg!(&plugin.name, &key, &api);
                        let mut user_name = String::new();
                        print!("Api Key: ");
                        let _ = stdout().flush();
                        stdin().read_line(&mut user_name)?;
                        if user_name.ends_with('\n') {
                            user_name.pop();
                        }
                        if user_name.ends_with('\r') {
                            user_name.pop();
                        }
                        let mut user_pass = String::new();
                        print!("Password: ");
                        let _ = stdout().flush();
                        stdin().read_line(&mut user_pass)?;
                        if user_pass.ends_with('\n') {
                            user_pass.pop();
                        }
                        if user_pass.ends_with('\r') {
                            user_pass.pop();
                        }
                        self.db.setting_set_sync(&DbSettingsObj {
                            name: format!("PLUGIN_{}_{}_{}", plugin.name, ns, "API_NS"),
                            description: Some("API Login for site.".into()),
                            num: None,
                            param: Some(user_name),
                        });
                    }
                    LoginType::Cookie(cookie_name, help_text)
                        if self
                            .db
                            .setting_get_sync(&format!(
                                "PLUGIN_{}_{}_{}",
                                plugin.name, cookie_name, "COOKIE"
                            ))
                            .is_none() =>
                    {
                        let mut user_name = String::new();
                        if let Some(help_text) = help_text {
                            println!("{help_text}");
                        }
                        print!("Cookie: ");
                        let _ = stdout().flush();
                        stdin().read_line(&mut user_name)?;
                        if user_name.ends_with('\n') {
                            user_name.pop();
                        }
                        if user_name.ends_with('\r') {
                            user_name.pop();
                        }

                        self.db.setting_set_sync(&DbSettingsObj {
                            name: format!("PLUGIN_{}_{}_{}", plugin.name, cookie_name, "COOKIE"),
                            description: Some("Cookie for site..".into()),
                            num: None,
                            param: Some(user_name),
                        });
                    }
                    _ => {}
                }
            }
        }

        Ok(())
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
                    "DownloadManager: Internal storage for {scraper_name} removed. Killing worker loop."
                );
                break;
            };

            // 1. If there are literally no jobs tracked in memory, kill this thread.
            if scraper_internal.job_storage.is_empty() {
                info!(
                    "DownloadManager: No jobs remaining in memory for {scraper_name}. Retiring worker loop."
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

            if let Some((mut job, plugin, ratelimiter)) = job_to_run {
                let job_limiter = {
                    let guard = self.jobs.read().await;
                    let Some(internal_storage) = guard.get(&scraper_name) else {
                        break;
                    };
                    internal_storage.job_limiter.clone()
                };
                let job_permit = job_limiter.acquire_owned().await;

                info!("DownloadManager: Setting job {} to running status.", job.id);
                self.db.job_set_is_running(&job).await;

                let job_id = job.id;
                let scraper_name = scraper_name.clone();

                for logins in self.load_logins(&plugin) {
                    job.config.param.push(logins);
                }

                let scraper = Scraper::new(
                    job,
                    ratelimiter,
                    self.plugin_manager.clone(),
                    plugin,
                    self.clone(),
                );

                tokio::task::spawn(async move {
                    let _job_permit = job_permit;
                    if let Err(err) = scraper.run_scraper().await {
                        log::error!(
                            "Worker: {} JobId: {} scraper had err: {}",
                            scraper_name,
                            job_id,
                            err
                        );
                    }
                });
            } else {
                // All jobs are currently actively running. Wait a bit for them to finish.
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
}

pub fn expected_hash(hash: &HashesSupported) -> &str {
    match hash {
        HashesSupported::Md5(value)
        | HashesSupported::Sha1(value)
        | HashesSupported::Sha256(value)
        | HashesSupported::Sha512(value) => value,
        HashesSupported::IPFSCID(value) => value,
        HashesSupported::IPFSCID1(value) => value,
        HashesSupported::ImageHash(value) => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_url_tag_is_added_once() {
        let mut file = FileObject {
            source: Some(FileSource::Url("https://example.test/file.jpg".into())),
            ..Default::default()
        };

        let url = "https://example.test/file.jpg";
        Scraper::add_source_url_tag(&mut file, url);
        Scraper::add_source_url_tag(&mut file, url);

        assert_eq!(file.tag_list.len(), 1);
        assert_eq!(file.tag_list[0].tags.len(), 1);
        assert_eq!(file.tag_list[0].tags[0].tag.name, url);
        assert_eq!(file.tag_list[0].tags[0].tag.namespace.name, "source_url");
    }

    #[test]
    fn file_processing_guard_tracks_shutdown_work() {
        let active = Arc::new(AtomicUsize::new(0));
        active.fetch_add(1, Ordering::SeqCst);

        {
            let _guard = FileProcessingGuard {
                active: active.clone(),
            };
            assert_eq!(active.load(Ordering::SeqCst), 1);
        }

        assert_eq!(active.load(Ordering::SeqCst), 0);
    }
}

/// Hashes the bytes and compares it to what the scraper should of recieved.
pub fn hash_bytes(bytes: &Bytes, hash: &HashesSupported) -> (String, bool) {
    match hash {
        HashesSupported::Md5(hash) => {
            let digest = md5::compute(bytes);

            // let sharedtypes::HashesSupported(hashe, _) => hash;
            if !hash.is_empty() && &format!("{digest:x}") != hash {
                info!("Parser returned: {hash} Got: {digest:?}");
            }
            (format!("{digest:x}"), &format!("{digest:x}") == hash)
        }
        HashesSupported::Sha1(hash) => {
            let mut hasher = sha1::Sha1::new();
            hasher.update(bytes);
            let hastring = encode_upper(hasher.finalize());
            let dune = &hastring == hash;
            if !dune && !hash.is_empty() {
                info!("Parser returned: {hash} Got: {hastring}");
            }
            (hastring, dune)
        }
        HashesSupported::Sha256(hash) => {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            let hastring = encode_upper(hasher.finalize());
            let dune = &hastring == hash;
            if !dune && !hash.is_empty() {
                info!("Parser returned: {hash} Got: {hastring}");
            }
            (hastring, dune)
        }
        HashesSupported::Sha512(hash) => {
            let hasher = Sha512::digest(bytes);
            let hastring = encode_upper(hasher);
            let dune = &hastring == hash;
            if !dune && !hash.is_empty() {
                info!("Parser returned: {hash} Got: {hastring}");
            }
            (hastring, dune)
        }
        _ => {
            let mut dl_hasher = DownloadHasher::new(hash);
            dl_hasher.update(bytes);
            if let Some(hsh) = dl_hasher.finish() {
                let dune = hsh == expected_hash(hash);
                (hsh, dune)
            } else {
                ("".to_string(), false)
            }
        }
    }
}
