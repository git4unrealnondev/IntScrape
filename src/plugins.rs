use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
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
    storage_libs: RwLock<HashMap<String, Arc<Library>>>,
    db: Arc<MainDatabase>,
}
pub type PluginInitFn = extern "C" fn();
impl PluginManager {
    pub fn new(path: &Path, db: Arc<MainDatabase>) -> Arc<Self> {
        let plugin_manager = PluginManager {
            storage: HashMap::new().into(),
            storage_libs: HashMap::new().into(),
            storage_site: HashMap::new().into(),
            db,
        };

        plugin_manager.load_libs(path);

        plugin_manager.debug();

        plugin_manager.into()
    }

    fn debug(&self) {
        info!("Plugin Storage: {:?}", self.storage.read());
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
                                info!("Loaded Plugin Name: {:?}", &plugin.name);
                                self.storage_libs
                                    .write()
                                    .insert(plugin.name.clone(), lib.clone());

                                // Loads sites into storage
                                for property in plugin.properties.iter() {
                                    if let PluginProperties::Sites(site_list) = property {
                                        for site in site_list {
                                            self.storage_site
                                                .write()
                                                .insert(site.clone(), plugin.name.clone());
                                        }
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
    pub fn match_plugin(&self, jobs: Vec<DbJobsObj>) -> HashMap<Plugin, Vec<DbJobsObj>> {
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
    pub fn callback_on_download(&self, _data: Bytes) {}
}
