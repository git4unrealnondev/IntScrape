use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{Write, stdin, stdout},
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
};

use bytes::Bytes;
use libloading::Library;

use log::*;
use parking_lot::RwLock;
use shared_types::*;

use crate::db::MainDatabase;

pub struct PluginManager {
    storage: RwLock<HashMap<String, shared_types::Plugin>>,
    storage_site: RwLock<HashMap<String, String>>,
    storage_callbacks: RwLock<HashMap<GlobalCallbacks, HashSet<String>>>,
    storage_libs: RwLock<HashMap<String, Arc<Library>>>,
    db: Arc<MainDatabase>,
    threads: RwLock<Vec<JoinHandle<()>>>,
}

impl Drop for PluginManager {
    fn drop(&mut self) {
        let mut guard = self.threads.write();
        let threads_to_join = std::mem::take(&mut *guard);

        for thread in threads_to_join {
            // Now we own the handle and can safely call .join()
            if let Err(err) = thread.join() {
                log::error!("PluginManager thread had error {:?}", err);
            }
        }
    }
}

impl PluginManager {
    pub fn new(path: &Path, db: Arc<MainDatabase>) -> Arc<Self> {
        let plugin_manager = PluginManager {
            storage: HashMap::new().into(),
            storage_libs: HashMap::new().into(),
            storage_site: HashMap::new().into(),
            storage_callbacks: HashMap::new().into(),
            db,
            threads: Vec::new().into(),
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
                Some("so") | Some("dll") | Some("dylib") => {
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
                                for property in plugin.properties.iter() {
                                    if let PluginProperties::Login((login_need, login_type)) =
                                        property
                                        && login_need == &LoginNeed::Required
                                    {
                                        match login_type {
                                            LoginType::Api(key, api) => {
                                                if self
                                                    .db
                                                    .setting_get_sync(&format!(
                                                        "PLUGIN_{}_{}",
                                                        plugin.name, "API_KEY"
                                                    ))
                                                    .is_none()
                                                {
                                                    dbg!(&plugin.name, &key, &api);
                                                    let mut user_name = String::new();
                                                    print!("Api Key: ");
                                                    let _ = stdout().flush();
                                                    stdin()
                                                        .read_line(&mut user_name)
                                                        .expect("Did not enter a correct string");
                                                    if let Some('\n') =
                                                        user_name.chars().next_back()
                                                    {
                                                        user_name.pop();
                                                    }
                                                    if let Some('\r') =
                                                        user_name.chars().next_back()
                                                    {
                                                        user_name.pop();
                                                    }
                                                    let mut user_pass = String::new();
                                                    print!("Password: ");
                                                    let _ = stdout().flush();
                                                    stdin()
                                                        .read_line(&mut user_pass)
                                                        .expect("Did not enter a correct string");
                                                    if let Some('\n') =
                                                        user_pass.chars().next_back()
                                                    {
                                                        user_pass.pop();
                                                    }
                                                    if let Some('\r') =
                                                        user_pass.chars().next_back()
                                                    {
                                                        user_pass.pop();
                                                    }
                                                    self.db.setting_set_sync(&DbSettingsObj {
                                                        name: format!(
                                                            "PLUGIN_{}_API_KEY",
                                                            plugin.name
                                                        ),
                                                        description: Some(
                                                            "API Login for site.".into(),
                                                        ),
                                                        num: None,
                                                        param: Some(user_name),
                                                    });
                                                    self.db.setting_set_sync(&DbSettingsObj {
                                                        name: format!(
                                                            "PLUGIN_{}_API_PASS",
                                                            plugin.name
                                                        ),
                                                        description: Some(
                                                            "API Login for site.".into(),
                                                        ),
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
                                    if let PluginProperties::Sites(site_list) = property {
                                        for site in site_list {
                                            self.storage_site
                                                .write()
                                                .insert(site.clone(), plugin.name.clone());
                                        }
                                    }
                                }

                                for callback in plugin.callbacks.iter() {
                                    if let Some(site_list) =
                                        self.storage_callbacks.write().get_mut(callback)
                                    {
                                        site_list.insert(plugin.name.to_string());
                                    } else {
                                        let mut list = HashSet::new();
                                        list.insert(plugin.name.to_string());
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
    pub fn match_plugin(&self, jobs: &Vec<DbJobsObj>) -> HashMap<Plugin, Vec<DbJobsObj>> {
        let mut out: HashMap<Plugin, Vec<DbJobsObj>> = HashMap::new();

        for job in jobs.iter() {
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
    /// After file downloading run callbacks for on_download
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
                                        "Plugins: {} could not call 'on_download' got error: {:?}",
                                        plugin_name, err
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
                                        "Plugins: {} could not call 'on_download' got error: {:?}",
                                        plugin_name, err
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
                                    error!("Plugins: background execution for '{}' failed to load symbol: {:?}", name_log, err);
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
                                    error!("Plugins: background execution for '{}' failed to load symbol: {:?}", name_log, err);
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
