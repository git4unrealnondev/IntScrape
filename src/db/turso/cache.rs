use shared_types::DbSettingsObj;

use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Gets a setting from the internal cache
    pub(in crate::db::turso) async fn setting_get_cache(
        &self,
        setting_name: &String,
    ) -> Option<DbSettingsObj> {
        let setting_guard = self.setting_cache.read().await;
        setting_guard.get(setting_name).cloned()
    }
    pub(in crate::db::turso) async fn setting_set_cache(&self, setting: &DbSettingsObj) {
        let mut setting_guard = self.setting_cache.write().await;
        setting_guard.insert(setting.name.clone(), setting.clone());
    }

    /// Gets a namespace string by id
    pub(in crate::db::turso) async fn namespace_get_id_cache(&self, ns_id: &u64) -> Option<String> {
        let ns_guard = self.namespace_cache_reverse.read().await;
        ns_guard.get(ns_id).cloned()
    }

    /// Gets a namespace id by string
    pub(in crate::db::turso) async fn namespace_get_name_cache(
        &self,
        ns_name: &str,
    ) -> Option<u64> {
        let ns_guard = self.namespace_cache.read().await;
        ns_guard.get(ns_name).cloned()
    }

    /// Loads a namespce into the cache
    pub(in crate::db::turso) async fn namespace_set_cache(&self, ns_id: u64, ns_name: String) {
        let mut ns_cache = self.namespace_cache.write().await;
        let mut ns_reverse_cache = self.namespace_cache_reverse.write().await;

        ns_reverse_cache.insert(ns_id, ns_name.clone());
        ns_cache.insert(ns_name.clone(), ns_id);
    }
}
