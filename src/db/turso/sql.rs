use std::collections::{HashMap, HashSet};

use shared_types::*;
use smol_str::SmolStr;
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::{
    SQL_CHUNK_SIZE,
    turso::{TagDb, TursoDatabase},
};

fn job_to_params(job: &DbJobsObj) -> Vec<Value> {
    vec![
        Value::from(job.id as i64),
        Value::from(job.config.time as i64),
        Value::from(job.config.reptime as i64),
        Value::from(job.config.priority as i64),
        Value::from(job.isrunning),
        Value::from(serde_json::to_string(&job.config.recreation).unwrap()),
        Value::from(job.config.site.clone()),
        Value::from(serde_json::to_string(&job.config.param).unwrap()),
        Value::from(serde_json::to_string(&job.config.user_data).unwrap()),
    ]
}

impl TursoDatabase {
    /// Loads the namespace into the internal cache
    pub(in crate::db::turso) async fn namespace_load(&self, conn: &Connection) -> Result<()> {
        let mut ns_guard = self.namespace_cache.write().await;
        let mut ns_reverse_guard = self.namespace_cache_reverse.write().await;
        let mut rows = conn.query("SELECT id, name FROM Namespace;", ()).await?;

        while let Some(row) = rows.next().await? {
            let id: u64 = row.get(0)?;
            let name: String = row.get(1)?;
            ns_guard.insert(name.clone(), id);
            ns_reverse_guard.insert(id, name);
        }

        Ok(())
    }

    /// Loads settings into cache
    pub(in crate::db::turso) async fn settings_load(&self, conn: &Connection) -> Result<()> {
        let mut setting_guard = self.setting_cache.write().await;
        let mut rows = conn
            .query("SELECT name, description, num, param FROM Settings;", ())
            .await?;

        while let Some(row) = rows.next().await? {
            let name: String = row.get(0)?;
            let description: Option<String> = row.get(1)?;
            let num: Option<u64> = row.get(2)?;
            let param: Option<String> = row.get(3)?;

            setting_guard.insert(
                name.clone(),
                shared_types::DbSettingsObj {
                    name,
                    description,
                    num,
                    param,
                },
            );
        }

        Ok(())
    }

    /// Updates a setting in the db
    pub(in crate::db::turso) async fn setting_set_sql(
        &self,
        conn: &Connection,
        setting: DbSettingsObj,
    ) -> Result<()> {
        conn.execute(
            "INSERT OR REPLACE INTO Settings (name, description, num, param) VALUES (?1, ?2, ?3, ?4)",
            (
                setting.name,
                setting.description,
                setting.num.map(|f| f as i64),
                setting.param,
            ),
        )
        .await?;
        Ok(())
    }

    /// Adds relationships into the db
    pub(in crate::db::turso) async fn relationship_bulk_add(
        &self,
        conn: &Connection,
        relationships: &[(u64, TagDb)],
    ) -> Result<()> {
        let mut namespace_tag_map: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
        for (file_id, tag_db) in relationships {
            namespace_tag_map
                .entry(tag_db.namespace_id)
                .or_default()
                .push((*file_id as i64, tag_db.id));
        }

        for (namespace_id, mapping) in namespace_tag_map {
            for chunk in mapping.chunks(SQL_CHUNK_SIZE) {
                let mut holders = Vec::with_capacity(chunk.len());
                let mut params = Vec::with_capacity(chunk.len() * 2);

                for (file_id, tag_id) in chunk {
                    holders.push("(?, ?)");
                    params.push(file_id.clone());
                    params.push(tag_id.clone());
                }

                let sql_string = format!(
                    "INSERT INTO Relationship_{namespace_id} (file_id, tag_id) VALUES {};",
                    holders.join(", ")
                );
                conn.execute(sql_string, params_from_iter(params)).await?;
            }
        }

        Ok(())
    }

    /// Searches tags through the Tantivy-backed FTS index.
    ///
    /// Returns the most-used matching tags, with BM25 relevance breaking ties.
    pub async fn tags_search_fts(
        &self,
        search_string: &str,
        limit: usize,
    ) -> Result<Vec<TagSearch>> {
        let mut out = Vec::new();
        if search_string.len() < 2 {
            return Ok(out);
        }
        let conn = self.db.connect()?;
        let fts_query = search_string.trim().to_owned();
        let mut rows = conn
            .query(
                "SELECT id, count, fts_score(name, ?1) AS score \
                 FROM Tags \
                 WHERE fts_match(name, ?1) \
                 ORDER BY count DESC, score ASC, id ASC \
                 LIMIT ?2;",
                (fts_query, limit as i64),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            out.push(TagSearch {
                tag_id: row.get(0)?,
                count: row.get(1)?,
            });
        }

        Ok(out)
    }

    /// Adds tags into the db
    pub(in crate::db::turso) async fn tag_action_bulk_add(
        &self,
        conn: &Connection,
        tag_actions: &[FileTagAction],
    ) -> Result<HashMap<Tag, i64>> {
        let mut tag_list = HashSet::new();

        if tag_actions.is_empty() {
            return Ok(HashMap::new());
        }

        for tag_action in tag_actions.iter() {
            for plugin_tag in tag_action.tags.iter() {
                tag_list.insert(plugin_tag.tag.clone());
                if let Some(ref relation_context) = plugin_tag.relates_to {
                    tag_list.insert(relation_context.tag.clone());
                    if let Some(ref tag) = relation_context.limit_to {
                        tag_list.insert(tag.clone());
                    }
                }
            }
        }

        let tag_db_set = self.tag_add_bulk(conn, &tag_list).await?;

        let mut ns_ids: HashMap<&str, u64> = HashMap::new();
        for tag in &tag_list {
            if let Some(ns_id) = self.namespace_get_name_cache(&tag.namespace.name).await {
                ns_ids.insert(tag.namespace.name.as_str(), ns_id);
            }
        }

        let db_by_key: HashMap<(SmolStr, i64), i64> = tag_db_set
            .iter()
            .map(|tag_db| ((tag_db.name.clone(), tag_db.namespace_id), tag_db.id))
            .collect();

        let mut tag_mapping: HashMap<Tag, i64> = HashMap::with_capacity(tag_list.len());
        for tag in &tag_list {
            if let Some(&ns_id) = ns_ids.get(tag.namespace.name.as_str())
                && let Some(&tag_id) = db_by_key.get(&(tag.name.clone().into(), ns_id as i64))
            {
                tag_mapping.insert(tag.clone(), tag_id);
            }
        }

        let mut parents = HashSet::new();
        for tag_action in tag_actions {
            for plugin_tag in &tag_action.tags {
                if let Some(relation_context) = &plugin_tag.relates_to {
                    let (Some(&child_id), Some(&parent_id)) = (
                        tag_mapping.get(&plugin_tag.tag),
                        tag_mapping.get(&relation_context.tag),
                    ) else {
                        continue;
                    };

                    let limit_to_id = relation_context
                        .limit_to
                        .as_ref()
                        .and_then(|tag| tag_mapping.get(tag))
                        .copied()
                        .map(|id| id as u64);

                    parents.insert(TagParents {
                        tag_id: child_id as u64,
                        relate_tag_id: parent_id as u64,
                        limit_to: limit_to_id,
                    });
                }
            }
        }

        if !parents.is_empty() {
            self.parents_bulk_add(conn, &parents).await?;
        }

        Ok(tag_mapping)
    }

    /// Bulk adds tag parent relationships into the db
    pub(in crate::db::turso) async fn parents_bulk_add(
        &self,
        conn: &Connection,
        parents: &HashSet<TagParents>,
    ) -> Result<()> {
        if parents.is_empty() {
            return Ok(());
        }

        let parents: Vec<&TagParents> = parents.iter().collect();

        for chunk in parents.chunks(SQL_CHUNK_SIZE) {
            conn.execute(&parents_insert_sql(chunk.len()), parents_params(chunk))
                .await?;
        }

        Ok(())
    }

    /// Gets a namespace id, creating the namespace row if it doesn't exist yet.
    pub(in crate::db::turso) async fn namespace_get_or_create_sql(
        &self,
        conn: &Connection,
        name: &str,
        description: Option<String>,
    ) -> Result<u64> {
        if let Some(ns_id) = self.namespace_get_name_cache(name).await {
            return Ok(ns_id);
        }

        let mut stmt = conn
            .prepare(
                "INSERT INTO Namespace (name, description) VALUES (?1, ?2)
                 ON CONFLICT(name) DO UPDATE SET description = excluded.description
                 RETURNING id",
            )
            .await?;

        let row = stmt.query_row((name, description)).await?;
        let ns_id: u64 = row.get(0)?;

        conn.execute_batch(format!(
            "CREATE TABLE Relationship_{ns_id} (
                    file_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    PRIMARY KEY (tag_id, file_id),
                    FOREIGN KEY (file_id) REFERENCES File(id) ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE
                );
            CREATE INDEX idx_Relationship_{ns_id}_tag_file ON Relationship_{ns_id} (file_id);"
        ))
        .await?;

        self.namespace_set_cache(ns_id, name.to_string()).await;

        Ok(ns_id)
    }

    /// Creates the per-namespace `Relationship_{id}` partition table. Kept
    /// central so every namespace creation path leaves a usable partition
    /// behind (tag adds, namespace bulk imports, slurp).
    pub(in crate::db::turso) async fn relationship_partition_create(
        &self,
        conn: &Connection,
        ns_id: u64,
    ) -> Result<()> {
        conn.execute_batch(format!(
            "CREATE TABLE IF NOT EXISTS Relationship_{ns_id} (
                    file_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    PRIMARY KEY (tag_id, file_id),
                    FOREIGN KEY (file_id) REFERENCES File(id) ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE
                );
            CREATE INDEX IF NOT EXISTS idx_Relationship_{ns_id}_tag_file ON Relationship_{ns_id} (file_id);"
        ))
        .await
    }

    /// Adds tags into the db
    /// Handles adding regex into the db
    pub(in crate::db::turso) async fn tag_add_bulk(
        &self,
        conn: &Connection,
        tag_list: &HashSet<Tag>,
    ) -> Result<HashSet<TagDb>> {
        let mut out = HashSet::new();
        let mut regex_storage = HashSet::new();

        // Early exit if empty
        if tag_list.is_empty() {
            return Ok(out);
        }

        let mut namespace_objects: HashMap<String, Option<String>> = HashMap::new();
        for tag in tag_list {
            namespace_objects
                .entry(tag.namespace.name.clone())
                .or_insert_with(|| tag.namespace.description.clone());
        }

        let mut namespace_id_mapping: HashMap<String, u64> =
            HashMap::with_capacity(namespace_objects.len());
        for (ns_name, ns_description) in namespace_objects {
            let ns_id = self
                .namespace_get_or_create_sql(conn, &ns_name, ns_description)
                .await?;
            namespace_id_mapping.insert(ns_name, ns_id);
        }

        let mut pending_tags: Vec<(&Tag, u64)> = Vec::new();
        for tag in tag_list {
            if let Some(ns_id) = namespace_id_mapping.get(&tag.namespace.name) {
                pending_tags.push((tag, *ns_id));
            }
        }

        if pending_tags.is_empty() {
            return Ok(out);
        }

        let max_id = self.tag_get_max_id(conn).await?;

        for chunk in pending_tags.chunks(SQL_CHUNK_SIZE) {
            let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
            let mut holders = Vec::with_capacity(chunk.len());
            for (tag, ns_id) in chunk {
                holders.push("(?, ?)");
                params.push(Value::from(tag.name.as_str()));
                params.push(Value::from(*ns_id as i64));
            }

            let tag_sql_str: String = format!(
                "INSERT INTO Tags (name, namespace) VALUES {} \
                 ON CONFLICT(name, namespace) DO UPDATE SET name = excluded.name \
                 RETURNING id, name, namespace;",
                holders.join(", ")
            );

            if let Ok(mut rows) = conn.query(tag_sql_str, params_from_iter(params)).await {
                while let Some(row) = rows.next().await? {
                    let id: i64 = row.get(0)?;
                    let name: String = row.get(1)?;
                    let namespace_id: i64 = row.get(2)?;

                    let name: SmolStr = name.into();

                    if id >= max_id {
                        regex_storage.insert(TagDb {
                            id,
                            name: name.clone(),
                            namespace_id,
                        });
                    }

                    out.insert(TagDb {
                        id,
                        name,
                        namespace_id,
                    });
                }
            }
        }

        // Adds new tags into the regex parser, mirroring the legacy backend.
        {
            let reverse_cache = self.namespace_cache_reverse.read().await;
            let mut tags_to_add = HashMap::new();
            for item in regex_storage {
                if let Some(namespace_name) = reverse_cache.get(&(item.namespace_id as u64)) {
                    tags_to_add.insert(
                        Tag {
                            name: item.name.to_string(),
                            namespace: GenericNamespaceObj {
                                name: namespace_name.clone(),
                                description: None,
                            },
                        },
                        item.id as u64,
                    );
                }
            }
            drop(reverse_cache);
            let plugin_manager = self.plugin_manager.read();
            if let Some(plugin_manager) = plugin_manager.as_ref() {
                plugin_manager.add_regex_tags(tags_to_add);
            }
        }

        Ok(out)
    }

    /// Adds a list of files into the db and sets their id if not already set
    pub(in crate::db::turso) async fn file_add_bulk(
        &self,
        conn: &Connection,
        file_list: &[FileInternal],
    ) -> Result<HashSet<FileInternal>> {
        let mut out = HashSet::new();

        if file_list.is_empty() {
            return Ok(out);
        }

        for file_list in file_list.chunks(SQL_CHUNK_SIZE) {
            let mut holders = Vec::with_capacity(file_list.len());
            let mut params = Vec::with_capacity(file_list.len() * 5);
            let mut file_map = HashMap::with_capacity(file_list.len());

            for file in file_list.iter() {
                holders.push("(?, ?, ?, ?, ?)");
                file_map.insert(file.hash.as_str(), file);
                params.push(Value::from(file.id.map(|f| f as i64)));
                params.push(Value::from(file.hash.as_str()));
                params.push(Value::from(file.extension.as_str()));
                params.push(Value::from(file.storage_id as i64));
                params.push(Value::from(file.size_bytes.map(|f| f as i64)));
            }
            let sql_str = format!(
                "INSERT INTO File (id, hash, extension, storage_id, size_bytes) VALUES {} \
             ON CONFLICT(hash) DO UPDATE SET \
                 extension = excluded.extension, \
                 storage_id = excluded.storage_id, \
                 size_bytes = COALESCE(excluded.size_bytes, File.size_bytes) \
             RETURNING id, hash;",
                holders.join(", ")
            );

            let mut rows = conn.query(sql_str, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let id: i64 = row.get(0)?;
                let hash: String = row.get(1)?;

                if let Some(file) = file_map.get(hash.as_str()) {
                    let mut file = (*file).clone().clone();
                    file.id = Some(id as u64);
                    out.insert(file);
                }
            }
        }

        Ok(out)
    }

    /// Gets the max id in the db currently
    async fn tag_get_max_id(&self, conn: &Connection) -> Result<i64> {
        let mut row = conn
            .query("SELECT COALESCE(MAX(id), 1) FROM Tags;", ())
            .await?;

        if let Ok(Some(row)) = row.next().await {
            let max: i64 = row.get(0)?;

            Ok(max)
        } else {
            Err(turso::Error::QueryReturnedNoRows)
        }
    }
}

/// Builds a multi-row `INSERT OR IGNORE INTO Parents` statement with one
/// `(?, ?, ?)` tuple per parent row.
fn parents_insert_sql(parent_count: usize) -> String {
    let mut placeholders = String::new();
    for row in 0..parent_count {
        if row > 0 {
            placeholders.push_str(", ");
        }
        let offset = row * 3;
        placeholders.push_str(&format!(
            "(?{}, ?{}, ?{})",
            offset + 1,
            offset + 2,
            offset + 3
        ));
    }

    format!("INSERT OR IGNORE INTO Parents (tag_id, relate_tag_id, limit_to) VALUES {placeholders}")
}

/// Binds each parent row to its three `VALUES` placeholders, mapping an absent
/// `limit_to` to SQL `NULL`.
fn parents_params(parents: &[&TagParents]) -> Vec<Value> {
    parents
        .iter()
        .flat_map(|parent| {
            [
                Value::from(parent.tag_id as i64),
                Value::from(parent.relate_tag_id as i64),
                match parent.limit_to {
                    Some(limit_id) => Value::from(limit_id as i64),
                    None => Value::Null,
                },
            ]
        })
        .collect()
}
