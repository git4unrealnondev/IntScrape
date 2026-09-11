//! IPC surface for the turso backend.
//!
//! Replicates the exact request/name surface that `MainDatabase` used to
//! provide so the generated client (`generated/client`) stays source
//! compatible. Every handler is async and dispatched through
//! `dispatch_ipc_request_async` by the IPC socket server.

use std::collections::{HashMap, HashSet};

use shared_types::{
    DbSettingsObj, FileInternal, FileTagAction, GenericNamespaceObj, PluginJob, SearchObj, Tag,
    TagParents, TagSearch,
};

use ipc_macro::export_ipc;

use crate::db::turso::TursoDatabase;

#[export_ipc(client_path = "generated/client/src/generated_api.rs")]
impl TursoDatabase {
    ///
    /// Gets namespace id if it exists
    ///
    #[ipc(name = "namespace_get", request = "GetNamespace")]
    pub async fn ipc_namespace_get(&self, name: &String) -> Option<u64> {
        let Ok(conn) = self.db.connect() else {
            return None;
        };
        self.namespace_get(&conn, name).await.ok().flatten()
    }

    ///
    /// Gets a list of tags where the tag and limits the number of returnees
    ///
    #[ipc(name = "search_tag_fts", request = "SearchTags")]
    pub async fn ipc_search_tag_fts(&self, tag: &str, limit: &Option<u64>) -> Vec<TagSearch> {
        let max_rows = limit.unwrap_or(10) as usize;
        self.tags_search_fts(tag, max_rows).await.unwrap_or_default()
    }

    /// Resolves tag names across namespaces and searches for files matching
    /// every input name, while allowing any tag with that name.
    #[ipc(name = "search_db_files_by_tags", request = "SearchFilesByTags")]
    pub async fn ipc_search_db_files_by_tags(
        &self,
        tags: &[String],
        limit: &Option<u64>,
    ) -> Vec<u64> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.search_db_files_by_tags(&conn, tags, limit)
            .await
            .unwrap_or_default()
    }

    /// Resolves tag names across namespaces while preserving boolean groups.
    #[ipc(name = "search_db_files_by_tag_groups", request = "SearchFilesByTagGroups")]
    pub async fn ipc_search_db_files_by_tag_groups(
        &self,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
        limit: &Option<u64>,
    ) -> Vec<u64> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.search_db_files_by_tag_groups(
            &conn, and_ids, and_tags, or_ids, or_tags, not_ids, not_tags, limit,
        )
        .await
        .unwrap_or_default()
    }

    ///
    /// Gets the file path of a fileid
    ///
    #[ipc(name = "get_file_path", request = "GetFileLocation")]
    pub async fn ipc_get_file_path(&self, file_id: &u64) -> Option<String> {
        let Ok(conn) = self.db.connect() else {
            return None;
        };
        self.file_get_physical_path(&conn, *file_id).await.unwrap_or(None)
    }

    #[ipc(name = "get_file_hashes", request = "GetFileHashes")]
    pub async fn ipc_get_file_hashes(&self, file_id: &u64) -> HashMap<String, String> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.file_hashes_get(&conn, *file_id).await.unwrap_or_default()
    }

    ///
    /// Gets all tag ids assocated with a namespace id
    ///
    #[ipc(name = "get_tag_ids_namespace_id", request = "GetNamespaceTagIDs")]
    pub async fn ipc_tag_id_get_namespace_id(&self, namespace_id: &u64) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.tag_id_get_namespace_id(&conn, *namespace_id)
            .await
            .unwrap_or_default()
    }

    /// Gets every tag id in the database.
    #[ipc(name = "get_tag_ids_all", request = "GetTagIDsAll")]
    pub async fn ipc_tag_id_get_all(&self) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.tag_id_get_all(&conn).await.unwrap_or_default()
    }

    ///
    /// Gets a file if a tag is associated with it
    ///
    #[ipc(name = "get_tag_file", request = "GetTagFile")]
    pub async fn ipc_get_tag_file(&self, tag: &Tag) -> Option<FileInternal> {
        let Ok(conn) = self.db.connect() else {
            return None;
        };
        self.tag_get_file(&conn, tag).await.unwrap_or(None)
    }

    ///
    /// Gets all `file_ids` with tags that have namespace id
    ///
    #[ipc(name = "get_namespace_file_ids", request = "GetNamespaceFileIDs")]
    pub async fn ipc_file_id_get_namespace_id(&self, namespace_id: &u64) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.file_id_get_namespace_id(&conn, *namespace_id)
            .await
            .unwrap_or_default()
    }

    ///
    /// Gets tag ids with a namespace_id associated with a file_id
    ///
    #[ipc(name = "get_tags_filtered", request = "GetNamespaceTagIdsFiltered")]
    pub async fn ipc_file_id_get_tag_ids_where_namespace_id(
        &self,
        file_id: &u64,
        namespace_id: &u64,
    ) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.file_id_get_tag_ids_filtered(&conn, *file_id, *namespace_id)
            .await
            .unwrap_or_default()
    }

    ///
    /// Adds a relationship between a `file_id` and `tag_id`
    ///
    #[ipc(name = "put_tags_to_file", request = "PutTagsRelationship")]
    pub async fn ipc_file_relationship_tags_add(
        &self,
        file_id: &u64,
        tag: &[FileTagAction],
    ) -> bool {
        if tag.is_empty() {
            return true;
        }

        // Tag adds resolve namespace ids from the cache; creating an uncached
        // namespace runs the partition DDL that turso forbids in concurrent
        // transactions. Ensure namespaces up front so the write transaction
        // below can stay BEGIN CONCURRENT (DML-only).
        let namespace_set: HashSet<GenericNamespaceObj> = tag
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
            log::error!("Failed to pre-ensure namespaces for file {file_id}: {error}");
            return false;
        }

        loop {
            let Ok(conn) = self.db.connect() else {
                return false;
            };
            loop {
                match conn.execute("BEGIN CONCURRENT", ()).await {
                    Ok(_) => break,
                    Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(error) => {
                        log::error!("Failed to begin concurrent tag transaction for file {file_id}: {error}");
                        return false;
                    }
                }
            }

            let tag_map = match self.tag_action_bulk_add(&conn, tag).await {
                Ok(tag_map) => tag_map,
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                Err(error) => {
                    log::error!("Failed to add tags for file {file_id}: {error}");
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return false;
                }
            };
            let relationships: HashSet<(u64, u64)> = tag_map
                .values()
                .map(|tag_id| (*file_id, *tag_id as u64))
                .collect();

            match self.relationships_bulk_add(&conn, &relationships).await {
                Ok(()) => {}
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                Err(error) => {
                    log::error!("Failed to add relationships for file {file_id}: {error}");
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return false;
                }
            }

            match conn.execute("COMMIT", ()).await {
                Ok(_) => return true,
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    log::warn!("Tag transaction for file {file_id} conflicted; retrying in 50ms: {error}");
                    let _ = conn.execute("ROLLBACK", ()).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    log::error!("Failed to commit tags for file {file_id}: {error}");
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return false;
                }
            }
        }
    }

    /// Adds tag actions without creating a file/tag relationship.
    ///
    /// This is used by tag callbacks that create structural tag relationships.
    #[ipc(name = "tag_actions_add", request = "TagActionsAdd")]
    pub async fn ipc_tag_actions_add(&self, tag_actions: &[FileTagAction]) -> bool {
        self.tag_actions_add(tag_actions).await
    }

    /// Adds tags to multiple files in one connection.
    #[ipc(name = "put_tags_to_files", request = "PutTagsRelationships")]
    pub async fn ipc_file_relationship_tags_add_bulk(
        &self,
        tags_by_file: &HashMap<u64, Vec<FileTagAction>>,
    ) -> bool {
        if tags_by_file.is_empty() {
            return true;
        }

        // Ensure all referenced namespaces exist (see put_tags_to_file) so
        // the transaction below stays DML-only inside BEGIN CONCURRENT.
        let namespace_set: HashSet<GenericNamespaceObj> = tags_by_file
            .values()
            .flatten()
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
            log::error!("Failed to pre-ensure namespaces for bulk tag add: {error}");
            return false;
        }

        loop {
            let Ok(conn) = self.db.connect() else {
                return false;
            };
            loop {
                match conn.execute("BEGIN CONCURRENT", ()).await {
                    Ok(_) => break,
                    Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(error) => {
                        log::error!("Failed to begin concurrent bulk tag transaction: {error}");
                        return false;
                    }
                }
            }

            let mut relationships: HashSet<(u64, u64)> = HashSet::new();
            let mut failed = false;
            for (file_id, tag_actions) in tags_by_file {
                if tag_actions.is_empty() {
                    continue;
                }
                match self.tag_action_bulk_add(&conn, tag_actions).await {
                    Ok(tag_map) => relationships.extend(
                        tag_map
                            .values()
                            .map(|tag_id| (*file_id, *tag_id as u64)),
                    ),
                    Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                        log::warn!("Bulk tag transaction conflicted; retrying in 50ms: {error}");
                        let _ = conn.execute("ROLLBACK", ()).await;
                        failed = true;
                        break;
                    }
                    Err(error) => {
                        log::error!("Failed to add bulk tags for file {file_id}: {error}");
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return false;
                    }
                }
            }
            if failed {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }

            if !relationships.is_empty()
                && let Err(error) = self.relationships_bulk_add(&conn, &relationships).await
            {
                let _ = conn.execute("ROLLBACK", ()).await;
                if TursoDatabase::is_concurrency_conflict(&error) {
                    log::warn!("Bulk relationship transaction conflicted; retrying in 50ms: {error}");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                log::error!("Failed to add bulk relationships: {error}");
                return false;
            }

            match conn.execute("COMMIT", ()).await {
                Ok(_) => return true,
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    log::warn!("Bulk tag transaction conflicted at commit; retrying in 50ms: {error}");
                    let _ = conn.execute("ROLLBACK", ()).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    log::error!("Failed to commit bulk tag transaction: {error}");
                    return false;
                }
            }
        }
    }

    ///
    /// Gets all file ids inside of the db.
    ///
    #[ipc(name = "get_file_ids_all", request = "GetFileListId")]
    pub async fn ipc_file_id_get_all(&self) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.file_id_get_all(&conn).await.unwrap_or_default()
    }

    ///
    /// Gets all tag ids associated with a fileid
    ///
    #[ipc(name = "relationship_get_tagid", request = "RelationshipGetFileid")]
    pub async fn ipc_relationship_get_tag_id(&self, file_id: &u64) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.relationship_get_tag_id(&conn, *file_id).await.unwrap_or_default()
    }

    /// Gets tag relationships for multiple files in one IPC request.
    #[ipc(name = "relationship_get_tagid_many", request = "RelationshipGetTagidMany")]
    pub async fn ipc_relationship_get_tag_id_many(
        &self,
        file_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<u64>> {
        let mut out = HashMap::with_capacity(file_ids.len());
        for file_id in file_ids {
            out.insert(*file_id, self.ipc_relationship_get_tag_id(file_id).await);
        }
        out
    }

    ///
    /// Gets all file ids associated with a tag_id
    ///
    #[ipc(name = "relationship_get_fileid", request = "RelationshipGetTagid")]
    pub async fn ipc_relationship_get_file_id(&self, tag_id: &u64) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.relationship_get_file_id(&conn, *tag_id).await.unwrap_or_default()
    }

    /// Gets file relationships for multiple tags in one IPC request.
    #[ipc(name = "relationship_get_fileid_many", request = "RelationshipGetFileidMany")]
    pub async fn ipc_relationship_get_file_id_many(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<u64>> {
        let mut out = HashMap::with_capacity(tag_ids.len());
        for tag_id in tag_ids {
            out.insert(*tag_id, self.ipc_relationship_get_file_id(tag_id).await);
        }
        out
    }

    /// Gets files whose tag is the related parent of the supplied structural tag.
    #[ipc(name = "relationship_get_parent_fileid", request = "RelationshipGetParentFileid")]
    pub async fn ipc_relationship_get_parent_file_id(&self, tag_id: &u64) -> HashSet<u64> {
        let Ok(conn) = self.db.connect() else {
            return HashSet::new();
        };
        self.relationship_get_parent_file_id(&conn, *tag_id)
            .await
            .unwrap_or_default()
    }

    /// Gets every parent relation declared by a child tag.
    #[ipc(name = "parent_relationships_get", request = "ParentRelationshipsGet")]
    pub async fn ipc_parent_relationships_get(&self, tag_id: &u64) -> Vec<TagParents> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.parent_relationships_get(&conn, *tag_id).await.unwrap_or_default()
    }

    /// Gets parent relations for multiple child tags in one IPC request.
    #[ipc(name = "parent_relationships_get_many", request = "ParentRelationshipsGetMany")]
    pub async fn ipc_parent_relationships_get_many(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<TagParents>> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.parent_relationships_get_many(&conn, tag_ids).await.unwrap_or_default()
    }

    /// Gets every child relation that points at a parent tag.
    #[ipc(name = "child_relationships_get", request = "ChildRelationshipsGet")]
    pub async fn ipc_child_relationships_get(&self, relate_tag_id: &u64) -> Vec<TagParents> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.child_relationships_get(&conn, *relate_tag_id).await.unwrap_or_default()
    }

    /// Gets child relations for multiple parent tags in one IPC request.
    #[ipc(name = "child_relationships_get_many", request = "ChildRelationshipsGetMany")]
    pub async fn ipc_child_relationships_get_many(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<TagParents>> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.child_relationships_get_many(&conn, tag_ids).await.unwrap_or_default()
    }

    /// Gets one exact child-parent relation, including its optional limit tag.
    #[ipc(name = "parent_relationship_get", request = "ParentRelationshipGet")]
    pub async fn ipc_parent_relationship_get(
        &self,
        tag_id: &u64,
        relate_tag_id: &u64,
    ) -> Option<TagParents> {
        let Ok(conn) = self.db.connect() else {
            return None;
        };
        self.parent_relationship_get(&conn, *tag_id, *relate_tag_id)
            .await
            .unwrap_or(None)
    }

    ///
    /// Adds tags into db in a bulk manner
    ///
    #[ipc(name = "get_tag_id_bulk", request = "GetTagIds")]
    pub async fn ipc_tag_id_get_tag(&self, tags: &HashSet<u64>) -> HashMap<u64, Tag> {
        if tags.is_empty() {
            return HashMap::new();
        }
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.tag_id_get_tag(&conn, tags).await.unwrap_or_default()
    }

    ///
    /// Marks a url as being dead in the db
    ///
    #[ipc(name = "dead_url_add", request = "AddDeadUrl")]
    pub async fn ipc_dead_url_add(&self, dead_url: &String) -> bool {
        self.dead_url_add_async(dead_url.clone()).await
    }

    ///
    /// Checks if a list of urls are dead
    ///
    #[ipc(name = "dead_url_get", request = "GetDeadUrl")]
    pub async fn ipc_dead_url_get(&self, dead_urls: &[String]) -> HashMap<String, bool> {
        self.dead_url_exist(dead_urls.to_vec()).await
    }

    ///
    /// Adds a namespace into the db
    ///
    #[ipc(name = "namespace_set", request = "SetNamespace")]
    pub async fn ipc_namespace_add(&self, namespace: &GenericNamespaceObj) -> u64 {
        let Ok(conn) = self.db.connect() else {
            return 0;
        };
        self.namespace_get_or_create_sql(&conn, &namespace.name, namespace.description.clone())
            .await
            .unwrap_or(0)
    }

    ///
    /// Human written tag searching layer
    ///
    #[ipc(name = "search_db_files", request = "SearchFiles")]
    pub async fn ipc_search_db_files_human(&self, search: &SearchObj, limit: &Option<u64>) -> Vec<u64> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.search_db_files(&conn, search, limit).await.unwrap_or_default()
    }

    /// A sync function to get a function
    #[ipc(name = "setting_get", request = "SettingsGetName")]
    pub async fn ipc_setting_get(&self, name: &str) -> Option<DbSettingsObj> {
        self.setting_get_sync(name).await
    }

    ///
    /// Returns every setting in the database.
    ///
    #[ipc(name = "settings_list", request = "SettingsList")]
    pub async fn ipc_settings_list(&self) -> Vec<DbSettingsObj> {
        self.settings_get_all_sync().await
    }

    ///
    /// Sets the setting in the db. Updates it if the setting already exists
    ///
    #[ipc(name = "setting_set", request = "SettingsSet")]
    pub async fn ipc_setting_set(&self, obj: &DbSettingsObj) -> bool {
        self.setting_set_sync(obj).await
    }

    ///
    /// Adds job into db
    ///
    #[ipc(name = "jobs_add_single", request = "JobsAddSingle")]
    pub async fn ipc_jobs_add_single(&self, job: PluginJob) -> u64 {
        self.jobs_add_single(job).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_types::{PluginTag, TagOperation, TagType};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    async fn new_test_db() -> Arc<TursoDatabase> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_tags_to_file_creates_namespace_partition_without_ddl_error() {
        let db = new_test_db().await;

        let conn = db.connect().unwrap();
        let storage_id = db
            .file_storage_location_get_or_create(&conn, "test_storage")
            .await
            .unwrap();
        drop(conn);

        let files = vec![FileInternal {
            id: None,
            hash: "ipc_hash_123".into(),
            extension: "png".into(),
            storage_id,
            size_bytes: Some(7),
        }];
        let conn = db.connect().unwrap();
        let resolved = db.file_add_bulk(&conn, &files).await.unwrap();
        drop(conn);
        let file_id = *resolved.iter().next().unwrap().id.as_ref().unwrap();

        let tag_action = FileTagAction {
            operation: TagOperation::Add,
            tags: vec![PluginTag {
                tag: Tag {
                    name: "ipc_floof".into(),
                    namespace: GenericNamespaceObj {
                        name: "ipc_fresh_ns".into(),
                        description: None,
                    },
                },
                tag_type: TagType::NormalNoRegex,
                relates_to: None,
            }],
        };

        assert!(db
            .ipc_file_relationship_tags_add(&file_id, &[tag_action])
            .await);

        let conn = db.connect().unwrap();
        let mut rows = conn
            .query(
                "SELECT id FROM Namespace WHERE name = 'ipc_fresh_ns';",
                (),
            )
            .await
            .unwrap();
        let Some(row) = rows.next().await.unwrap() else {
            panic!("namespace was never created");
        };
        let ns_id: u64 = row.get(0).unwrap();
        drop(rows);

        let mut rows = conn
            .query(
                "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = LOWER('Relationship') || '_' || ?1;",
                (ns_id as i64,),
            )
            .await
            .unwrap();
        let partition = rows.next().await.unwrap();
        assert!(partition.is_some(), "namespace partition table was never created");
        drop(rows);

        let mut rows = conn
            .query(
                &format!(
                    "SELECT COUNT(*) FROM relationship_{ns_id} WHERE file_id = ?1;",
                ),
                (file_id as i64,),
            )
            .await
            .unwrap();
        let Some(row) = rows.next().await.unwrap() else {
            panic!("expected at least one relationship row");
        };
        let relationship_count: i64 = row.get(0).unwrap();
        assert!(
            relationship_count >= 1,
            "expected the file/tag relationship to be persisted"
        );
    }
}
