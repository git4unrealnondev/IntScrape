use turso::{Connection, Result};

use crate::db::turso::TursoDatabase;
use shared_types::*;

impl TursoDatabase {
    /// Gets a namespace id, using the cache and falling back to the db.
    pub(in crate::db::turso) async fn namespace_get(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<Option<u64>> {
        if let Some(ns_id) = self.namespace_get_name_cache(name).await {
            return Ok(Some(ns_id));
        }

        let mut rows = conn
            .query("SELECT id FROM Namespace WHERE name = ?1;", (name,))
            .await?;

        if let Some(row) = rows.next().await? {
            let ns_id: u64 = row.get(0)?;
            self.namespace_set_cache(ns_id, name.to_string()).await;
            Ok(Some(ns_id))
        } else {
            Ok(None)
        }
    }

    /// Gets a setting, using the cache and falling back to the db.
    pub(in crate::db::turso) async fn setting_get_sql(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<Option<DbSettingsObj>> {
        if let Some(setting) = self.setting_get_cache(&name.to_string()).await {
            return Ok(Some(setting));
        }

        let mut rows = conn
            .query(
                "SELECT name, description, num, param FROM Settings WHERE name = ?1;",
                (name,),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            let setting = DbSettingsObj {
                name: row.get(0)?,
                description: row.get(1)?,
                num: row.get(2)?,
                param: row.get(3)?,
            };
            self.setting_set_cache(&setting).await;
            Ok(Some(setting))
        } else {
            Ok(None)
        }
    }
}

impl TursoDatabase {
    /// Sets a namespace into the db
    pub(in crate::db::turso) async fn namespace_set(
        &self,
        conn: &Connection,
        ns: GenericNamespaceObj,
    ) -> u64 {
        if let Some(ns_id) = self.namespace_get_name_cache(&ns.name).await {
            return ns_id;
        }

        conn.execute(
            "INSERT INTO Namespace (name, description) VALUES (?1, ?2);",
            (ns.name.as_str(), ns.description),
        )
        .await;

        let ns_id = conn.last_insert_rowid() as u64;

        self.namespace_set_cache(ns_id, ns.name);

        ns_id
    }

    /// Main function for managing settings
    pub(in crate::db::turso) async fn setting_set(
        &self,
        conn: &Connection,
        setting: DbSettingsObj,
    ) -> turso::Result<()> {
        if let Some(local_setting) = self.setting_get_cache(&setting.name).await
            && local_setting == setting
        {
            return Ok(());
        }
        self.setting_set_sql(conn, setting.clone()).await?;
        self.setting_set_cache(&setting).await;
        Ok(())
    }
}
