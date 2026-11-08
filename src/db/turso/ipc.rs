//! IPC surface for the turso backend.
//!
//! Replicates the exact request/name surface that `MainDatabase` used to
//! provide so the generated client (`generated/client`) stays source
//! compatible. Every handler is async and dispatched through
//! `dispatch_ipc_request_async` by the IPC socket server.

use std::collections::{HashMap, HashSet};

use shared_types::helpers::collect_namespaces;
use shared_types::{
    DbJobsObj, DbSettingsObj, FileAction, FileInternal, FileTagAction, GenericNamespaceObj,
    PluginJob, SearchObj, Tag, TagParents, TagSearch,
};

use ipc_macro::export_ipc;

use crate::db::turso::TursoDatabase;

#[export_ipc(client_path = "generated/client/src/generated_api.rs")]
impl TursoDatabase {
    ///
    /// Gets namespace id if it exists
    ///
    #[ipc(name = "namespace_get", request = "GetNamespace")]
    #[allow(clippy::ptr_arg)] // #[ipc] signature feeds the generated client
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
        self.tags_search_fts(tag, max_rows)
            .await
            .unwrap_or_default()
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
    #[ipc(
        name = "search_db_files_by_tag_groups",
        request = "SearchFilesByTagGroups"
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn ipc_search_db_files_by_tag_groups(
        &self,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
        limit: &Option<u64>,
        offset: &Option<u64>,
    ) -> Vec<u64> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.search_db_files_by_tag_groups(
            &conn, and_ids, and_tags, or_ids, or_tags, not_ids, not_tags, limit, offset,
        )
        .await
        .unwrap_or_default()
    }

    /// Counts the files a boolean tag-group search matches without returning
    /// them. Paged UIs ask for this alongside a `LIMIT`ed page fetch so the
    /// leading pages never carry the whole result set across IPC.
    #[ipc(
        name = "search_db_files_by_tag_groups_count",
        request = "SearchFilesByTagGroupsCount"
    )]
    pub async fn ipc_search_db_files_by_tag_groups_count(
        &self,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
    ) -> u64 {
        let Ok(conn) = self.db.connect() else {
            return 0;
        };
        self.search_db_files_by_tag_groups_count(
            &conn, and_ids, and_tags, or_ids, or_tags, not_ids, not_tags,
        )
        .await
        .unwrap_or(0)
    }

    ///
    /// Gets the file path of a fileid
    ///
    #[ipc(name = "get_file_path", request = "GetFileLocation")]
    pub async fn ipc_get_file_path(&self, file_id: &u64) -> Option<String> {
        let Ok(conn) = self.db.connect() else {
            return None;
        };
        self.file_get_physical_path(&conn, *file_id)
            .await
            .unwrap_or(None)
    }

    #[ipc(name = "get_file_hashes", request = "GetFileHashes")]
    pub async fn ipc_get_file_hashes(&self, file_id: &u64) -> HashMap<String, String> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.file_hashes_get(&conn, *file_id)
            .await
            .unwrap_or_default()
    }

    /// Gets every recorded SHA512 digest in one query, sorted by digest.
    ///
    /// Bulk counterpart of `get_file_hashes` for callers that need the whole
    /// list (e.g. the WebUI `/api/v0/file-hashes` endpoint): one IPC round
    /// trip and one indexed scan instead of one round trip and one query per
    /// file. See `file_hashes_get_all_sha512`.
    #[ipc(name = "get_file_hashes_all_sha512", request = "GetAllFileHashesSha512")]
    pub async fn ipc_get_file_hashes_all_sha512(&self) -> Vec<String> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.file_hashes_get_all_sha512(&conn)
            .await
            .unwrap_or_default()
    }

    /// Resolves files by their secondary hashes in one bulk lookup, keyed by
    /// `(algorithm, digest)`. Omits any hash that does not match a known file.
    /// Used by bulk importers (e.g. the e6 CSV dump importer) that attach
    /// data to already-downloaded files without re-downloading them.
    /// Takes plain `(algorithm, digest)` pairs because `HashesSupported`
    /// is not `Serialize` and cannot cross the IPC boundary.
    #[ipc(name = "hashes_files_get_bulk", request = "HashesFilesGetBulk")]
    pub async fn ipc_hashes_files_get_bulk(
        &self,
        hashes: &[(String, String)],
    ) -> HashMap<(String, String), FileInternal> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.hashes_files_get(&conn, hashes).await.unwrap_or_default()
    }

    /// Bulk lookup of files by their primary `File.hash`, keyed by hash.
    /// Complements `hashes_files_get_bulk` for databases whose files store
    /// the looked-up digest as the primary hash itself (e.g. md5-primary
    /// rows from older/slurped libraries) rather than only in `FileHashes`.
    #[ipc(name = "files_get_by_hash", request = "FilesGetByHash")]
    pub async fn ipc_files_get_by_hash(
        &self,
        hashes: &[String],
    ) -> HashMap<String, FileInternal> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.files_get_by_hash(&conn, hashes).await.unwrap_or_default()
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

    /// Gets tag ids associated with a namespace id, newest-created first.
    /// `limit` bounds how many ids are returned; `None` or a limit of zero
    /// returns every id, matching the unlimited request.
    #[ipc(name = "get_tag_ids_namespace_id_limited", request = "GetNamespaceTagIDsLimited")]
    pub async fn ipc_tag_id_get_namespace_id_limited(
        &self,
        namespace_id: &u64,
        limit: &Option<u64>,
    ) -> Vec<u64> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.tag_id_get_namespace_id_limited(&conn, *namespace_id, *limit)
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
        // namespace runs the partition DDL, which must run outside the write
        // transaction below. Ensure namespaces up front so the write
        // transaction stays DML-only.
        let namespace_set = collect_namespaces(tag);
        if let Err(error) = self.namespace_ensure_set(&namespace_set).await {
            log::error!("Failed to pre-ensure namespaces for file {file_id}: {error}");
            return false;
        }

        loop {
            let mut conn = match self.db.connect() {
                Ok(conn) => conn,
                Err(error) => {
                    log::error!("Failed to connect while tagging file {file_id}: {error}");
                    return false;
                }
            };
            // CONCURRENT: this runs per UI tag request, many at once, against
            // the same popular-tag rows the scrapers write. CONCURRENT lets the
            // transactions overlap and settle write-write conflicts at commit.
            // The body is idempotent (tag_action_bulk_add + relationships_bulk_add
            // are INSERT OR IGNORE), so CONCURRENT's re-run-on-conflict is safe.
            // Count deltas are deliberately NOT applied here: they are folded in
            // after the commit through the serialized writer.
            let tx = match conn
                .transaction_with_behavior(TursoDatabase::write_tx_behavior())
                .await
            {
                Ok(tx) => tx,
                Err(error) => {
                    if TursoDatabase::is_concurrency_conflict(&error) {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    log::error!("Failed to begin tag transaction for file {file_id}: {error}");
                    return false;
                }
            };

            let tag_map = match self.tag_action_bulk_add(&tx, tag).await {
                Ok(tag_map) => tag_map,
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    // `tx` drops and rolls back.
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                Err(error) => {
                    log::error!("Failed to add tags for file {file_id}: {error}");
                    return false;
                }
            };
            let relationships: HashSet<(u64, u64)> = tag_map
                .values()
                .map(|tag_id| (*file_id, *tag_id as u64))
                .collect();

            let add_deltas = match self.relationships_bulk_add(&tx, &relationships).await {
                Ok(deltas) => deltas,
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                Err(error) => {
                    log::error!("Failed to add relationships for file {file_id}: {error}");
                    return false;
                }
            };

            match tx.commit().await {
                Ok(_) => {
                    // Relationship rows committed; fold the deferred count
                    // deltas in through the serialized writer. Failure only
                    // leaves a stale count, so it must not fail the request.
                    if let Err(error) = self.tag_counts_apply(&add_deltas, &HashMap::new()).await {
                        log::error!(
                            "Failed to apply deferred tag counts for file {file_id}: {error}"
                        );
                    }
                    return true;
                }
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    log::warn!(
                        "Tag transaction for file {file_id} conflicted; retrying in 50ms: {error}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    log::error!("Failed to commit tags for file {file_id}: {error}");
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
        // the transaction below stays DML-only.
        let namespace_set = collect_namespaces(
            &tags_by_file
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>(),
        );
        if let Err(error) = self.namespace_ensure_set(&namespace_set).await {
            log::error!("Failed to pre-ensure namespaces for bulk tag add: {error}");
            return false;
        }

        loop {
            let mut conn = match self.db.connect() {
                Ok(conn) => conn,
                Err(error) => {
                    log::error!("Failed to connect while bulk tagging: {error}");
                    return false;
                }
            };
            // CONCURRENT, same reasoning as the per-file tag transaction: the
            // body is idempotent (INSERT OR IGNORE throughout) so re-running it
            // on a commit conflict is safe, and overlapping beats queueing for
            // the single writer lock on shared popular-tag rows.
            let tx = match conn
                .transaction_with_behavior(TursoDatabase::write_tx_behavior())
                .await
            {
                Ok(tx) => tx,
                Err(error) => {
                    if TursoDatabase::is_concurrency_conflict(&error) {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    log::error!("Failed to begin bulk tag transaction: {error}");
                    return false;
                }
            };

            let mut relationships: HashSet<(u64, u64)> = HashSet::new();
            let mut conflicted = false;
            for (file_id, tag_actions) in tags_by_file {
                if tag_actions.is_empty() {
                    continue;
                }
                match self.tag_action_bulk_add(&tx, tag_actions).await {
                    Ok(tag_map) => relationships
                        .extend(tag_map.values().map(|tag_id| (*file_id, *tag_id as u64))),
                    Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                        log::warn!("Bulk tag transaction conflicted; retrying in 50ms: {error}");
                        conflicted = true;
                        break;
                    }
                    Err(error) => {
                        log::error!("Failed to add bulk tags for file {file_id}: {error}");
                        return false;
                    }
                }
            }
            if conflicted {
                // `tx` drops and rolls back.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }

            let add_deltas = if relationships.is_empty() {
                HashMap::new()
            } else {
                match self.relationships_bulk_add(&tx, &relationships).await {
                    Ok(deltas) => deltas,
                    Err(error) => {
                        if TursoDatabase::is_concurrency_conflict(&error) {
                            log::warn!(
                                "Bulk relationship transaction conflicted; retrying in 50ms: {error}"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            continue;
                        }
                        log::error!("Failed to add bulk relationships: {error}");
                        return false;
                    }
                }
            };

            match tx.commit().await {
                Ok(_) => {
                    // Fold the deferred count deltas in via the serialized
                    // writer; a failure only leaves a stale count.
                    if let Err(error) = self.tag_counts_apply(&add_deltas, &HashMap::new()).await {
                        log::error!("Failed to apply deferred tag counts: {error}");
                    }
                    return true;
                }
                Err(error) if TursoDatabase::is_concurrency_conflict(&error) => {
                    log::warn!(
                        "Bulk tag transaction conflicted at commit; retrying in 50ms: {error}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
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
        self.relationship_get_tag_id(&conn, *file_id)
            .await
            .unwrap_or_default()
    }

    /// Gets tag relationships for multiple files in one IPC request.
    #[ipc(
        name = "relationship_get_tagid_many",
        request = "RelationshipGetTagidMany"
    )]
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
        self.relationship_get_file_id(&conn, *tag_id)
            .await
            .unwrap_or_default()
    }

    /// Gets file relationships for multiple tags in one IPC request.
    #[ipc(
        name = "relationship_get_fileid_many",
        request = "RelationshipGetFileidMany"
    )]
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
    #[ipc(
        name = "relationship_get_parent_fileid",
        request = "RelationshipGetParentFileid"
    )]
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
        self.parent_relationships_get(&conn, *tag_id)
            .await
            .unwrap_or_default()
    }

    /// Gets parent relations for multiple child tags in one IPC request.
    #[ipc(
        name = "parent_relationships_get_many",
        request = "ParentRelationshipsGetMany"
    )]
    pub async fn ipc_parent_relationships_get_many(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<TagParents>> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.parent_relationships_get_many(&conn, tag_ids)
            .await
            .unwrap_or_default()
    }

    /// Gets every child relation that points at a parent tag.
    #[ipc(name = "child_relationships_get", request = "ChildRelationshipsGet")]
    pub async fn ipc_child_relationships_get(&self, relate_tag_id: &u64) -> Vec<TagParents> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.child_relationships_get(&conn, *relate_tag_id)
            .await
            .unwrap_or_default()
    }

    /// Gets every child-parent relation whose `limit_to` qualifier equals the
    /// supplied tag — the reverse index of a qualified relationship. Rows are
    /// `(tag_id -> relate_tag_id)` pairs that are only meaningful in the
    /// context of that limit tag.
    #[ipc(
        name = "limited_child_relationships_get",
        request = "LimitedChildRelationshipsGet"
    )]
    pub async fn ipc_limited_child_relationships_get(&self, limit_tag_id: &u64) -> Vec<TagParents> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.limited_child_relationships_get(&conn, *limit_tag_id)
            .await
            .unwrap_or_default()
    }

    /// Gets the child-parent relations qualified by any of the supplied
    /// limit tags in one IPC request. This is the bulk form of
    /// `limited_child_relationships_get`; results are grouped by limit tag.
    #[ipc(
        name = "limited_child_relationships_get_many",
        request = "LimitedChildRelationshipsGetMany"
    )]
    pub async fn ipc_limited_child_relationships_get_many(
        &self,
        limit_tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<TagParents>> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.limited_child_relationships_get_many(&conn, limit_tag_ids)
            .await
            .unwrap_or_default()
    }

    /// Gets child relations for multiple parent tags in one IPC request.
    #[ipc(
        name = "child_relationships_get_many",
        request = "ChildRelationshipsGetMany"
    )]
    pub async fn ipc_child_relationships_get_many(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<TagParents>> {
        let Ok(conn) = self.db.connect() else {
            return HashMap::new();
        };
        self.child_relationships_get_many(&conn, tag_ids)
            .await
            .unwrap_or_default()
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
    #[allow(clippy::ptr_arg)] // #[ipc] signature feeds the generated client
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
    /// Resolves downloaded source URLs to one representative file id each, so
    /// UIs can link URLs in text back to the actual files on disk.
    ///
    #[ipc(name = "source_url_file_ids_get", request = "SourceUrlFileIdsGet")]
    pub async fn ipc_source_url_file_ids_get(&self, urls: &[String]) -> Vec<(String, u64)> {
        let url_set: HashSet<String> = urls.iter().cloned().collect();
        self.source_url_files_get(url_set)
            .await
            .into_iter()
            .filter_map(|(url, status)| {
                status.file.and_then(|file| file.id).map(|id| (url, id))
            })
            .collect()
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
    pub async fn ipc_search_db_files_human(
        &self,
        search: &SearchObj,
        limit: &Option<u64>,
    ) -> Vec<u64> {
        let Ok(conn) = self.db.connect() else {
            return Vec::new();
        };
        self.search_db_files(&conn, search, limit, &None)
            .await
            .unwrap_or_default()
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

    ///
    /// Gets every job in the database, highest-priority first.
    ///
    #[ipc(name = "jobs_get_all", request = "JobsGetAll")]
    pub async fn ipc_jobs_get_all(&self) -> Vec<DbJobsObj> {
        self.jobs_get_all().await
    }

    ///
    /// Updates an existing job's config.
    ///
    #[ipc(name = "jobs_update", request = "JobsUpdate")]
    pub async fn ipc_jobs_update(&self, job: &DbJobsObj) -> bool {
        self.jobs_update(job).await.is_ok()
    }

    ///
    /// Removes a job from the database.
    ///
    #[ipc(name = "jobs_remove", request = "JobsRemove")]
    pub async fn ipc_jobs_remove(&self, job: &DbJobsObj) -> bool {
        self.job_remove(job).await.is_ok()
    }

    /// Imports one file from a server-side path into the library (copy /
    /// move / hardlink per `action`), recording it in the DB. Returns an
    /// empty string on success, otherwise a human-readable error message.
    ///
    #[ipc(name = "import_file", request = "ImportFile")]
    pub async fn ipc_import_file(&self, path: &String, action: &FileAction) -> String {
        self.import_file(std::path::Path::new(path), *action)
            .await
            .err()
            .unwrap_or_default()
    }

    /// Runs one CLI command (the words one would type after `intscrape` in a
    /// terminal) against this database, returning everything the command
    /// prints. Failures come back as `error: ...` lines.
    ///
    #[ipc(name = "run_cli", request = "RunCli")]
    #[allow(clippy::ptr_arg)] // #[ipc] signature feeds the generated client
    pub async fn ipc_run_cli(&self, command: &String) -> String {
        crate::cli::run_cli_command(self, command).await
    }
}


#[cfg(test)]
mod tests;
