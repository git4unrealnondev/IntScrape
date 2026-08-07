use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
};

use bytes::Bytes;
use libloading::Library;

use log::{error, info};
use parking_lot::RwLock;
use regex::Regex;
use shared_types::{
    CallbackCustomData, CallbackCustomDataReturning, CallbackInfo, CallbackInfoInput,
    CallbackReturn, DbJobsObj, FileTagAction, GlobalCallbacks, Plugin, PluginProperties,
    ScraperDataReturn, StartupThreadType, Tag,
};

use crate::db::MainDatabase;

pub struct PluginManager {
    storage: RwLock<HashMap<String, shared_types::Plugin>>,
    storage_site: RwLock<HashMap<String, String>>,
    storage_callbacks: RwLock<HashMap<GlobalCallbacks, HashSet<String>>>,
    storage_libs: RwLock<HashMap<String, Arc<Library>>>,
    db: Arc<MainDatabase>,
    threads: RwLock<Vec<JoinHandle<()>>>,
    should_exit: Arc<AtomicBool>,
    regex_tags_cache: RwLock<HashSet<Tag>>,
    regex_cache: RwLock<HashMap<String, Arc<Regex>>>,
}

impl Drop for PluginManager {
    fn drop(&mut self) {
        let mut guard = self.threads.write();
        let threads_to_join = std::mem::take(&mut *guard);

        for thread in threads_to_join {
            // Now we own the handle and can safely call .join()
            if let Err(err) = thread.join() {
                log::error!("PluginManager thread had error {err:?}");
            }
        }
    }
}

impl PluginManager {
    pub fn new(path: &Path, db: Arc<MainDatabase>, should_exit: Arc<AtomicBool>) -> Arc<Self> {
        let plugin_manager = Self {
            storage: HashMap::new().into(),
            storage_libs: HashMap::new().into(),
            storage_site: HashMap::new().into(),
            storage_callbacks: HashMap::new().into(),
            db,
            threads: Vec::new().into(),
            should_exit,
            regex_tags_cache: HashSet::new().into(),
            regex_cache: HashMap::new().into(),
        };

        plugin_manager.load_libs(path);

        plugin_manager.debug();

        plugin_manager.into()
    }

    fn debug(&self) {
        info!("Plugin Storage: {:?}", self.storage.read());
    }

    pub fn get_storage_sites(&self) -> Vec<String> {
        let guard = self.storage_site.read();
        guard.keys().cloned().collect()
    }

    /// Adds tags to cache will let system process later.
    /// called from db needs to be relatively fast and non-blocking
    pub fn add_regex_tags(&self, tags: HashSet<Tag>) {
        let mut regex_tags_cache = self.regex_tags_cache.write();
        regex_tags_cache.extend(tags);
    }

    pub fn are_start_threads_closed(&self) -> bool {
        let guard = self.threads.read();
        for item in guard.iter() {
            if !item.is_finished() {
                return false;
            }
        }

        true
    }

    fn load_libs(&self, path: &Path) {
        for entry in fs::read_dir(path).unwrap().flatten() {
            let path = entry.path();

            let extension = path.extension().and_then(|s| s.to_str());
            match extension {
                Some("so" | "dll" | "dylib") => {
                    info!("🚚 Found plugin candidate: {:?}", path.file_name().unwrap());

                    unsafe {
                        let lib = Arc::new(Library::new(&path).unwrap());

                        if let Some(plugins) = self.get_info(&lib, path) {
                            for plugin in plugins {
                                info!("Loaded Plugin Name: {:?}", plugin.name);
                                self.storage_libs
                                    .write()
                                    .insert(plugin.name.clone(), lib.clone());

                                // Loads sites into storage
                                for property in &plugin.properties {
                                    if let PluginProperties::Sites(site_list) = property {
                                        for site in site_list {
                                            self.storage_site
                                                .write()
                                                .insert(site.clone(), plugin.name.clone());
                                        }
                                    }
                                }

                                for callback in &plugin.callbacks {
                                    if let Some(site_list) =
                                        self.storage_callbacks.write().get_mut(callback)
                                    {
                                        site_list.insert(plugin.name.clone());
                                    } else {
                                        let mut list = HashSet::new();
                                        list.insert(plugin.name.clone());
                                        self.storage_callbacks
                                            .write()
                                            .insert(callback.clone(), list);
                                    }
                                }

                                // Loads plugin mapping into storage
                                self.storage.write().insert(plugin.name.clone(), plugin);
                            }
                        }
                    }
                }
                _ => continue, // Skip directories, source files, etc.
            }
        }
    }

    ///
    /// Parses info from lib
    ///
    fn get_info(&self, lib: &Library, path: PathBuf) -> Option<Vec<shared_types::Plugin>> {
        info!("Trying to load library at path: {}", path.to_string_lossy());
        let temp: libloading::Symbol<unsafe extern "C" fn() -> Vec<shared_types::Plugin>> =
            match unsafe { lib.get(b"get_plugin_info") } {
                Err(_) => {
                    error!(
                        "Could not run get_plugin_info pull for lib. {}",
                        path.to_string_lossy()
                    );
                    return None;
                }
                Ok(lib) => lib,
            };
        unsafe { Some(temp()) }
    }

    ///
    /// Returns a list of jobs that a plugin can support
    ///
    pub fn match_plugin(&self, jobs: &[DbJobsObj]) -> HashMap<Plugin, Vec<DbJobsObj>> {
        let mut out: HashMap<Plugin, Vec<DbJobsObj>> = HashMap::new();

        for job in jobs {
            if let Some(job_name_mapping) = self.storage_site.read().get(&job.config.site)
                && let Some(plugin) = self.storage.read().get(job_name_mapping)
            {
                match out.get_mut(plugin) {
                    Some(local_list) => {
                        local_list.push(job.clone());
                    }
                    None => {
                        out.insert(plugin.clone(), vec![job.clone()]);
                    }
                }
            }
        }

        out
    }

    ///
    /// Prescraping. Returns a list of possible urls to checkout for text scraping
    ///
    pub fn url_dump(
        &self,
        scraperdata: &shared_types::ScraperDataReturn,
        scraper: &shared_types::Plugin,
    ) -> Result<Vec<shared_types::ScraperDataReturn>, libloading::Error> {
        if let Some(lib) = self.storage_libs.read().get(&scraper.name) {
            let temp: libloading::Symbol<
                unsafe extern "C" fn(
                    &shared_types::ScraperDataReturn,
                ) -> Vec<shared_types::ScraperDataReturn>,
            > = unsafe { lib.get(b"url_dump\0")? };

            return Ok(unsafe { temp(scraperdata) });
        }
        Err(libloading::Error::FreeLibraryUnknown)
    }

    ///
    /// After text scraping we send the data back to the plugin to do its work
    ///
    pub fn parser_call(
        &self,
        url_output: &str,
        source_url: &str,
        scraperdata: &shared_types::ScraperDataReturn,
        scraper: &Plugin,
    ) -> Vec<shared_types::ScraperReturn> {
        if let Some(scraper_library) = self.storage_libs.read().get(&scraper.name) {
            let temp: libloading::Symbol<
                unsafe extern "C" fn(
                    &str,
                    &str,
                    &shared_types::ScraperDataReturn,
                ) -> Vec<shared_types::ScraperReturn>,
            > = {
                unsafe {
                    match scraper_library.get(b"parser_call") {
                        Err(_err) => {
                            return vec![shared_types::ScraperReturn::Stop(
                                "Missing parser block in scraper".to_string(),
                            )];
                        }
                        Ok(out) => out,
                    }
                }
            };
            unsafe { temp(url_output, source_url, scraperdata) }
        } else {
            vec![shared_types::ScraperReturn::Nothing]
        }
    }

    ///
    /// After file downloading run callbacks for `on_download`
    ///
    pub fn callback_on_download(
        &self,
        data: &Bytes,
        tags: &mut Vec<FileTagAction>,
        jobs: &mut Vec<ScraperDataReturn>,
    ) {
        if let Some(plugin_name_list) = self
            .storage_callbacks
            .read()
            .get(&GlobalCallbacks::Download)
        {
            for plugin_name in plugin_name_list {
                if let Some(lib) = self.storage_libs.read().get(plugin_name) {
                    let temp: libloading::Symbol<unsafe extern "C" fn(&[u8]) -> CallbackReturn> = {
                        unsafe {
                            match lib.get(b"on_download") {
                                Err(err) => {
                                    error!(
                                        "Plugins: {plugin_name} could not call 'on_download' got error: {err:?}"
                                    );
                                    continue;
                                }
                                Ok(out) => out,
                            }
                        }
                    };
                    let return_data = unsafe { temp(data) };
                    tags.extend(return_data.tags);
                    jobs.extend(return_data.jobs);
                }
            }
        }
    }
    fn get_or_compile_regex(&self, pattern: &str) -> Result<Arc<Regex>, regex::Error> {
        {
            let cache = self.regex_cache.read();

            if let Some(regex) = cache.get(pattern) {
                return Ok(Arc::clone(regex));
            }
        }

        // Compile outside the lock. This prevents blocking other readers.
        let compiled = Arc::new(Regex::new(pattern)?);

        let mut cache = self.regex_cache.write();

        // Another thread may have compiled it while we were compiling.
        // Reuse the existing value in that case.
        Ok(Arc::clone(
            cache
                .entry(pattern.to_owned())
                .or_insert_with(|| Arc::clone(&compiled)),
        ))
    }
    pub fn callback_tag_regex(&self) {
        let callbacks: Vec<_> = {
            let callbacks = self.storage_callbacks.read();

            callbacks
                .iter()
                .filter_map(|(callback, plugins)| {
                    let GlobalCallbacks::Tag((search_type, namespace_allow, namespace_deny)) =
                        callback
                    else {
                        return None;
                    };

                    Some((
                        search_type.clone(),
                        namespace_allow.clone(),
                        namespace_deny.clone(),
                        plugins.clone(),
                    ))
                })
                .collect()
        };

        let regex_tags = self.regex_tags_cache.read();

        for (search_type, namespace_allow, namespace_deny, plugins) in callbacks {
            let compiled_regex = match &search_type {
                shared_types::SearchType::Regex(pattern) => {
                    match self.get_or_compile_regex(pattern) {
                        Ok(regex) => Some(regex),
                        Err(error) => {
                            log::error!("Invalid regex {pattern:?}: {error}");
                            continue;
                        }
                    }
                }
                shared_types::SearchType::String(_) => None,
            };

            for regex_tag in regex_tags.iter() {
                let namespace = &regex_tag.namespace.name;

                if namespace_deny.contains(namespace) {
                    continue;
                }

                if !namespace_allow.is_empty() && !namespace_allow.contains(namespace) {
                    continue;
                }

                let matched = match &search_type {
                    shared_types::SearchType::String(search_string) => {
                        regex_tag.name.contains(search_string)
                    }

                    shared_types::SearchType::Regex(_) => compiled_regex
                        .as_ref()
                        .is_some_and(|regex| regex.is_match(&regex_tag.name)),
                };

                if matched {
                    for plugin in &plugins {
                        dbg!(plugin, regex_tag);

                        // Invoke the plugin callback here.
                    }
                }
            }
        }
    }

    ///
    /// External callbacks IE Plugin -> Plugin
    ///
    pub fn external_plugin_call(
        &self,
        func_name: &String,
        callback_info: &CallbackInfoInput,
    ) -> HashMap<String, CallbackCustomDataReturning> {
        let mut out = HashMap::new();

        let callback_item_clean = CallbackInfo {
            func: func_name.clone(),
            vers: callback_info.vers,
            data_name: callback_info.data_name.clone(),
            data: callback_info
                .data
                .iter()
                .map(|f| match f {
                    CallbackCustomDataReturning::U8(_) => CallbackCustomData::U8,
                    CallbackCustomDataReturning::U64(_) => CallbackCustomData::U64,
                    CallbackCustomDataReturning::VU8(_) => CallbackCustomData::VU8,
                    CallbackCustomDataReturning::Vu64(_) => CallbackCustomData::Vu64,
                    CallbackCustomDataReturning::String(_) => CallbackCustomData::String,
                    CallbackCustomDataReturning::VString(_) => CallbackCustomData::VString,
                })
                .collect(),
        };

        if let Some(plugin_name_list) = self
            .storage_callbacks
            .read()
            .get(&GlobalCallbacks::Callback(callback_item_clean))
        {
            for plugin_name in plugin_name_list {
                if let Some(lib) = self.storage_libs.read().get(plugin_name) {
                    let temp: libloading::Symbol<
                        unsafe extern "C" fn(
                            &CallbackInfoInput,
                        )
                            -> HashMap<String, CallbackCustomDataReturning>,
                    > = {
                        unsafe {
                            match lib.get(func_name) {
                                Err(err) => {
                                    error!(
                                        "Plugins: {plugin_name} could not call {func_name} got error: {err:?}"
                                    );
                                    continue;
                                }
                                Ok(out) => out,
                            }
                        }
                    };

                    let return_data = unsafe { temp(callback_info) };
                    out.extend(return_data);
                }
            }
        }

        out
    }

    ///
    /// Runs callback `on_start`
    ///
    pub fn callback_on_start(&self) {
        // Spawns each plugin and waits till its finished before the next plugin gets called
        if let Some(plugin_name_list) = self
            .storage_callbacks
            .read()
            .get(&GlobalCallbacks::Start(StartupThreadType::Inline))
        {
            for plugin_name in plugin_name_list {
                if let Some(lib) = self.storage_libs.read().get(plugin_name) {
                    let temp: libloading::Symbol<unsafe extern "C" fn()> = {
                        unsafe {
                            match lib.get(b"on_start") {
                                Err(err) => {
                                    error!(
                                        "Plugins: {plugin_name} could not call 'on_download' got error: {err:?}"
                                    );
                                    continue;
                                }
                                Ok(out) => out,
                            }
                        }
                    };
                    unsafe { temp() };
                }
            }
        }
        // Spawns threads and throws them into the background functions
        if let Some(plugin_name_list) = self
            .storage_callbacks
            .read()
            .get(&GlobalCallbacks::Start(StartupThreadType::Spawn))
        {
            for plugin_name in plugin_name_list {
                if let Some(lib) = self.storage_libs.read().get(plugin_name) {
                    let lib_clone = Arc::clone(lib);

                    let name_log = plugin_name.clone();

                    self.threads.write().push(std::thread::spawn(move || {
                        unsafe {
                            match lib_clone.get::<unsafe extern "C" fn()>(b"on_start") {
                                Ok(temp) => {
                                    temp();
                                }
                                Err(err) => {
                                    error!("Plugins: background execution for '{name_log}' failed to load symbol: {err:?}");
                                }
                            }
                        }
                    }));
                }
            }
        }
        // Spawns threads and waits
        let mut threads = Vec::new();
        if let Some(plugin_name_list) = self
            .storage_callbacks
            .read()
            .get(&GlobalCallbacks::Start(StartupThreadType::SpawnInline))
        {
            for plugin_name in plugin_name_list {
                if let Some(lib) = self.storage_libs.read().get(plugin_name) {
                    let lib_clone = Arc::clone(lib);

                    let name_log = plugin_name.clone();

                    threads.push(std::thread::spawn(move || {
                        unsafe {
                            match lib_clone.get::<unsafe extern "C" fn()>(b"on_start") {
                                Ok(temp) => {
                                    temp();
                                }
                                Err(err) => {
                                    error!("Plugins: background execution for '{name_log}' failed to load symbol: {err:?}");
                                }
                            }
                        }
                    }));
                }
            }
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
