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
    regex_tags_cache: RwLock<HashMap<Tag, u64>>,
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
            regex_tags_cache: HashMap::new().into(),
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
    pub fn add_regex_tags(&self, tags: HashMap<Tag, u64>) {
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
        let mut ignored_plugins = std::env::var("INTSCRAPE_IGNORE_PLUGINS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(str::to_owned)
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        ignored_plugins.insert("file-hash".to_string());

        if !ignored_plugins.is_empty() {
            info!("Ignoring plugins: {:?}", ignored_plugins);
        }

        for entry in fs::read_dir(path).unwrap().flatten() {
            let path = entry.path();

            let extension = path.extension().and_then(|s| s.to_str());
            match extension {
                Some("so" | "dll" | "dylib") => {
                    let library_name = path
                        .file_stem()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default()
                        .strip_prefix("lib")
                        .unwrap_or_default()
                        .replace('_', "-");

                    if ignored_plugins.contains(&library_name) {
                        info!("Skipping ignored plugin library: {:?}", path.file_name());
                        continue;
                    }

                    info!("🚚 Found plugin candidate: {:?}", path.file_name().unwrap());

                    unsafe {
                        let lib = match Library::new(&path) {
                            Ok(lib) => Arc::new(lib),
                            Err(error) => {
                                error!(
                                    "Skipping plugin {} because it could not be loaded: {error}",
                                    path.display()
                                );
                                continue;
                            }
                        };

                        if let Some(plugins) = self.get_info(&lib, path) {
                            for plugin in plugins {
                                if ignored_plugins.contains(&plugin.name) {
                                    info!("Skipping ignored plugin: {:?}", plugin.name);
                                    continue;
                                }

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
    /// Runs all `on_download` callbacks concurrently and merges their results.
    pub fn callback_on_download(
        &self,
        data: &Bytes,
        tags: &mut Vec<FileTagAction>,
        jobs: &mut Vec<ScraperDataReturn>,
    ) {
        let callbacks: Vec<_> = {
            let callback_names = self.storage_callbacks.read();
            let libraries = self.storage_libs.read();
            callback_names
                .get(&GlobalCallbacks::Download)
                .into_iter()
                .flat_map(|names| names.iter())
                .filter_map(|plugin_name| {
                    libraries
                        .get(plugin_name)
                        .cloned()
                        .map(|library| (plugin_name.clone(), library))
                })
                .collect()
        };

        std::thread::scope(|scope| {
            let handles: Vec<_> = callbacks
                .into_iter()
                .map(|(plugin_name, library)| {
                    scope.spawn(move || {
                        let callback: libloading::Symbol<
                            unsafe extern "C" fn(&[u8]) -> CallbackReturn,
                        > = unsafe {
                            match library.get(b"on_download") {
                                Err(error) => {
                                    error!(
                                        "Plugins: {plugin_name} could not call 'on_download' got error: {error:?}"
                                    );
                                    return None;
                                }
                                Ok(callback) => callback,
                            }
                        };

                        Some(unsafe { callback(data) })
                    })
                })
                .collect();

            for handle in handles {
                match handle.join() {
                    Ok(Some(return_data)) => {
                        tags.extend(return_data.tags);
                        jobs.extend(return_data.jobs);
                    }
                    Ok(None) => {}
                    Err(_) => error!("Plugins: on_download callback panicked"),
                }
            }
        });
    }

    /// Compiles regex if it doesnt exist in the local cache
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

    /// Runs the regex on tags inside the internal cache
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

        // Drain the queue before processing. Callback results can add new tags,
        // which are intentionally deferred until the next pass.
        let regex_tags = std::mem::take(&mut *self.regex_tags_cache.write());

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

            for (regex_tag, source_id) in regex_tags.iter() {
                let namespace = &regex_tag.namespace.name;

                if namespace_deny.contains(namespace) {
                    continue;
                }

                if !namespace_allow.is_empty() && !namespace_allow.contains(namespace) {
                    continue;
                }

                let regex_matches: Vec<&str> = match &search_type {
                    shared_types::SearchType::String(search_string) => regex_tag
                        .name
                        .find(search_string)
                        .map(|start| vec![&regex_tag.name[start..start + search_string.len()]])
                        .unwrap_or_default(),
                    shared_types::SearchType::Regex(_) => compiled_regex
                        .as_ref()
                        .map(|regex| {
                            regex
                                .find_iter(&regex_tag.name)
                                .map(|matched| matched.as_str())
                                .collect()
                        })
                        .unwrap_or_default(),
                };

                for regex_match in regex_matches {
                    for plugin in &plugins {
                        let Some(lib) = self.storage_libs.read().get(plugin).cloned() else {
                            continue;
                        };

                        // ABI: on_regex(matched, full_tag, tag_source, tag_source_id).
                        let callback: libloading::Symbol<
                            unsafe extern "C" fn(&str, &str, &Tag, u64) -> CallbackReturn,
                        > = unsafe {
                            match lib.get(b"on_regex") {
                                Err(error) => {
                                    log::error!(
                                        "Plugins: {plugin} could not call 'on_regex' got error: {error:?}"
                                    );
                                    continue;
                                }
                                Ok(callback) => callback,
                            }
                        };

                        // The source is the full tag object; source_id is its database ID.
                        let return_data = unsafe {
                            callback(regex_match, &regex_tag.name, regex_tag, *source_id)
                        };

                        if !return_data.tags.is_empty()
                            && !self.db.tag_actions_add_sync(
                                &return_data.tags.into_iter().collect::<Vec<FileTagAction>>(),
                            )
                        {
                            log::error!("Plugins: {plugin} failed to process on_regex tags");
                        }

                        for job in return_data.jobs {
                            let _ = self.db.jobs_add_single_sync(job.job);
                        }
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
