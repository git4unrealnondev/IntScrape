//! Public async surface on `TursoDatabase` mirroring the legacy `MainDatabase`
//! IPC/consumer API. The `#[export_ipc]` attribute ultimately lives on the
//! `pub` methods here so the generated client and runtime call sites can be
//! re-pointed at turso without a shared-type change.

use std::collections::HashMap;
use std::path::PathBuf;

use shared_types::{
    DbSettingsObj, FileInternal, FileTagAction, GenericNamespaceObj, HashesSupported, PluginJob, Tag,
};
use turso::Result;

use crate::db::hashessupportedtokey;
use crate::db::turso::TursoDatabase;

// Thread-local runtime for blocking sync wrappers. Each thread that needs to
// call an async turso method synchronously gets its own lightweight runtime.
thread_local! {
    static BLOCK_RT: std::cell::RefCell<tokio::runtime::Runtime> = {
        std::cell::RefCell::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create blocking runtime"),
        )
    };
}

/// Runs an async future to completion on a thread-local blocking runtime.
///
/// Safe to call from any thread. When invoked from inside a tokio
/// multi-thread runtime (e.g. a CLI task on a worker), the worker is parked
/// via `block_in_place` first so a fresh runtime may drive the future
/// without tripping tokio's "Cannot start a runtime from within a runtime"
/// guard.
pub(in crate::db::turso) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        // A worker thread inside a multi-thread runtime: yield to the blocking
        // pool, then run the future on the thread-local runtime.
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| BLOCK_RT.with(|rt| rt.borrow().block_on(future)))
        }
        // On non-runtime threads (spawn_blocking, raw threads, tests) drive the
        // future directly. Single-thread runtimes cannot be blocked on, so a
        // runtime worker there must use the async API instead.
        _ => BLOCK_RT.with(|rt| rt.borrow().block_on(future)),
    }
}

impl TursoDatabase {
    ///
    /// Looks up files by their secondary hashes. Returns a map of
    /// (algorithm, digest) to the resolved file, omitting any hash that does
    /// not match a known file.
    ///
    pub async fn hashes_files_get_sync(
        &self,
        hashes: &[HashesSupported],
    ) -> HashMap<(String, String), FileInternal> {
        if hashes.is_empty() {
            return HashMap::new();
        }

        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while resolving hashes: {error}");
                return HashMap::new();
            }
        };

        let keys: Vec<(String, String)> = hashes.iter().map(hashessupportedtokey).collect();
        match self.hashes_files_get(&conn, &keys).await {
            Ok(found) => found,
            Err(error) => {
                log::error!("Failed to resolve hashes: {error}");
                HashMap::new()
            }
        }
    }

    ///
    /// Gets a file by its id.
    ///
    pub async fn file_id_get(&self, file_id: u64) -> Option<FileInternal> {
        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while fetching file {file_id}: {error}");
                return None;
            }
        };
        match self.file_get(&conn, &file_id).await {
            Ok(file) => Some(file),
            Err(error) => {
                log::debug!("File {file_id} not found: {error}");
                None
            }
        }
    }

    ///
    /// Resolves the fully partitioned on-disk location for a hash/extension,
    /// returning the path and the storage-location id. Mirrors the legacy
    /// `file_download_location_get_sync`.
    ///
    pub async fn file_download_location_get_sync(
        &self,
        hash: &str,
        ext: &str,
    ) -> Option<(PathBuf, u64)> {
        if hash.len() <= 6 {
            return None;
        }

        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while resolving download location: {error}");
                return None;
            }
        };

        let (base, path_id) = match self.file_download_location_get(&conn).await {
            Ok(location) => location,
            Err(error) => {
                log::error!("Failed to resolve download location: {error}");
                return None;
            }
        };

        let mut path_buf = base;
        path_buf.push(&hash[0..2]);
        path_buf.push(&hash[2..4]);
        path_buf.push(&hash[4..6]);
        path_buf.push(hash);
        Some((path_buf.with_extension(ext), path_id))
    }

    ///
    /// Synchronous wrapper for `file_download_location_get_sync` — used from
    /// non-tokio threads (e.g. the Rayon file processing pool).
    ///
    pub fn file_download_location_get_sync_blocking(
        &self,
        hash: &str,
        ext: &str,
    ) -> Option<(PathBuf, u64)> {
        block_on(self.file_download_location_get_sync(hash, ext))
    }

    ///
    /// SQL search is backed by the FTS index; there is no in-memory LRU cache
    /// to refresh on turso, so this is a no-op kept for compatibility.
    ///
    pub fn refresh_tag_search_cache(&self) {}

    ///
    /// Sets a setting, mirroring the legacy sync IPC.
    ///
    pub async fn setting_set_sync(&self, setting: &DbSettingsObj) -> bool {
        match self
            .retry_mvcc(|| async {
                let conn = self.connect()?;
                self.setting_set(&conn, setting.clone()).await
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                log::error!("Failed to set setting '{}': {error}", setting.name);
                false
            }
        }
    }

    ///
    /// Lists every setting currently held in the settings cache.
    ///
    pub async fn settings_get_all_sync(&self) -> Vec<DbSettingsObj> {
        let setting_guard = self.setting_cache.read().await;
        let mut settings: Vec<DbSettingsObj> =
            setting_guard.values().cloned().collect();
        settings.sort_by(|a, b| a.name.cmp(&b.name));
        settings
    }

    ///
    /// Gets a setting by name.
    ///
    pub async fn setting_get_sync(&self, name: &str) -> Option<DbSettingsObj> {
        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while getting setting '{name}': {error}");
                return None;
            }
        };
        match self.setting_get_sql(&conn, name).await {
            Ok(Some(setting)) => Some(setting),
            Ok(None) => None,
            Err(error) => {
                log::error!("Failed to get setting '{name}': {error}");
                None
            }
        }
    }

    ///
    /// Synchronous wrapper for `setting_set_sync` — used from sync plugin
    /// login helpers.
    ///
    pub fn setting_set_sync_blocking(&self, setting: &DbSettingsObj) -> bool {
        block_on(self.setting_set_sync(setting))
    }

    ///
    /// Synchronous wrapper for `setting_get_sync` — used from sync plugin
    /// login helpers.
    ///
    pub fn setting_get_sync_blocking(&self, name: &str) -> Option<DbSettingsObj> {
        block_on(self.setting_get_sync(name))
    }

    ///
    /// Adds tag actions without creating file/tag relationships.
    ///
    pub async fn tag_actions_add(&self, tag_actions: &[FileTagAction]) -> bool {
        if tag_actions.is_empty() {
            return true;
        }

        // Ensure all referenced namespaces exist (see put_tags_to_file) so
        // this transaction stays DML-only inside BEGIN CONCURRENT.
        let namespace_set: std::collections::HashSet<GenericNamespaceObj> = tag_actions
            .iter()
            .flat_map(|action| action.tags.iter())
            .flat_map(|plugin_tag| {
                let mut nss = vec![plugin_tag.tag.namespace.clone()];
                if let Some(relation) = &plugin_tag.relates_to {
                    nss.push(relation.tag.namespace.clone());
                    if let Some(limit) = &relation.limit_to {
                        nss.push(limit.namespace.clone());
                    }
                }
                nss
            })
            .collect();
        if let Err(error) = self.namespace_ensure_set(&namespace_set).await {
            log::error!("Failed to pre-ensure namespaces for tag actions: {error}");
            return false;
        }

        loop {
            let conn = match self.connect() {
                Ok(conn) => conn,
                Err(error) => {
                    log::error!("Failed to connect while adding tag actions: {error}");
                    return false;
                }
            };
            loop {
                match conn.execute("BEGIN CONCURRENT", ()).await {
                    Ok(_) => break,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(error) => {
                        log::error!("Failed to begin concurrent tag-actions transaction: {error}");
                        return false;
                    }
                }
            }
            match self.tag_action_bulk_add(&conn, tag_actions).await {
                Ok(_) => {}
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    log::error!("Failed to add tag actions: {error}");
                    return false;
                }
            }
            match conn.execute("COMMIT", ()).await {
                Ok(_) => return true,
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    log::error!("Failed to commit tag actions: {error}");
                    return false;
                }
            }
        }
    }

    ///
    /// Synchronous wrapper for `hashes_files_get_sync` — used from
    /// non-tokio threads (e.g. the heavy processing pool).
    ///
    pub fn hashes_files_get_sync_blocking(
        &self,
        hashes: &[HashesSupported],
    ) -> HashMap<(String, String), FileInternal> {
        block_on(self.hashes_files_get_sync(hashes))
    }

    ///
    /// Synchronous wrapper for `tag_actions_add` — used by plugin callbacks
    /// that run outside the tokio runtime.
    ///
    pub fn tag_actions_add_sync(&self, tag_actions: &[FileTagAction]) -> bool {
        block_on(self.tag_actions_add(tag_actions))
    }

    ///
    /// Resolves the file id that owns a given tag, if any.
    ///
    pub async fn tag_get_file_id(&self, tag: &Tag) -> Option<u64> {
        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while resolving tag file id: {error}");
                return None;
            }
        };
        match self.tag_get_file_id_sql(&conn, tag).await {
            Ok(Some(file_id)) => Some(file_id),
            Ok(None) => None,
            Err(error) => {
                log::error!("Failed to resolve tag file id: {error}");
                None
            }
        }
    }

    ///
    /// Adds a single job — sync wrapper for non-async contexts (plugins).
    ///
    pub fn jobs_add_single_sync(&self, job: PluginJob) -> u64 {
        block_on(self.jobs_add_single(job))
    }

    ///
    /// Legacy maintenance action from the SQLite backend. The turso backend has
    /// no internal-file repair phase, so this is a no-op kept for CLI parity.
    ///
    pub fn fix_internal_files(
        &self,
        _action: &crate::cli::cli_structs::CheckFilesEnum,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        log::warn!("fix_internal_files is not supported on the turso backend");
        Ok(())
    }

    ///
    /// Legacy roaring-bitmap rebuild. Search on turso is SQL/FTS backed, so this
    /// is a no-op kept for CLI parity.
    ///
    pub fn recache_roaring_db(&self) {
        log::warn!("recache_roaring_db is not supported on the turso backend");
    }
}

const _: () = {
    fn _assert_result(_: Result<()>) {}
};
