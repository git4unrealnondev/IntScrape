//! Database operations for the `settings` domain.

use super::MainDatabase;
use rusqlite::Connection;
use std::sync::Arc;

impl MainDatabase {
    ///
    /// Used internally to get a setting
    ///
    pub fn internal_setting_get(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<Option<shared_types::DbSettingsObj>, rusqlite::Error> {
        {
            let setting_guard = self.setting_cache.read();
            if let Some(setting) = setting_guard.get(name) {
                return Ok(Some(setting.clone()));
            }
        }

        let mut stmt = conn
            .prepare("SELECT name, description, num, param FROM settings WHERE name = ? LIMIT 1")?;

        let mut rows = stmt.query([name])?;

        if let Some(row) = rows.next()? {
            // Unpack using serde_rusqlite
            let obj = serde_rusqlite::from_row::<shared_types::DbSettingsObj>(row)
                .map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    ///
    /// Used internally to set a Setting
    ///
    pub fn internal_setting_set(
        &self,
        conn: &Connection,
        obj: &shared_types::DbSettingsObj,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        {
            let mut setting_cache = self.setting_cache.write();
            setting_cache.insert(obj.name.clone(), obj.clone());
        }
        // Option A: Using raw fields manually
        let mut stmt = conn.prepare(
            "INSERT OR REPLACE INTO settings (name, description, num, param) 
             VALUES (?1, ?2, ?3, ?4)",
        )?;

        stmt.execute(r2d2_sqlite::rusqlite::params![
            obj.name,
            obj.description,
            obj.num,
            obj.param
        ])?;

        Ok(())
    }

    ///
    /// What everything else uses when getting a setting
    ///
    pub async fn setting_get(self: Arc<Self>, name: String) -> Option<shared_types::DbSettingsObj> {
        let name = name.clone();
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.setting_get_sync(&name))
            .await
            .ok()
            .flatten() // Flattens the JoinError wrapper Option as well
    }

    ///
    /// What anything outside of the db uses to set a setting
    ///
    pub async fn setting_set(self: Arc<Self>, obj: shared_types::DbSettingsObj) -> bool {
        let obj = obj.clone();
        let _self_clone = self.clone();
        tokio::task::spawn_blocking(move || self.setting_set_sync(&obj))
            .await
            .ok()
            .is_some()
    }
}
