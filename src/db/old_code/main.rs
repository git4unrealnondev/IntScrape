use super::MainDatabase;
use super::tag_search;
use crate::plugins::PluginManager;
use log::info;
use rusqlite::{OptionalExtension, params};
use shared_types::{
    DbSearchTypeEnum, DbSettingsObj, FileInternal, FileTagAction, GenericNamespaceObj, PluginJob,
    SearchHolder, SearchObj, Tag, TagSearch,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub use super::file::SourceUrlFileStatus;

impl MainDatabase {
    ///
    /// Gets namespace id if it exists
    ///
    #[must_use]
        pub fn search_db_namespace_sync(&self, name: &String) -> Option<u64> {
        // Uses cache where avilable
        {
            let cache_guard = self.namespace_cache.read();
            if let Some(ns_id) = cache_guard.get(name) {
                return Some(*ns_id);
            }
        }

        let conn = self.pool.get().unwrap();

        let mut stmt = conn
            .prepare("SELECT id FROM Namespace WHERE name = ?1")
            .ok()?;

        let result = stmt.query_row(params![name], |row| row.get::<_, u64>(0));

        result.optional().ok().flatten()
    }

    ///
    /// Gets a list of tags where the tag and limits the number of returnees
    ///
    #[must_use]
        pub fn search_db_tags_fts(&self, tag: &str, limit: &Option<u64>) -> Vec<TagSearch> {
        let max_rows = limit.unwrap_or(10).min(usize::MAX as u64) as usize;
        if max_rows == 0 {
            return Vec::new();
        }
        let cache = self.tag_search_cache.read();
        let cache_complete = cache.is_complete();
        let mut results = cache.search(tag, max_rows);
        drop(cache);

        // When the in-memory index holds the whole Tags table *and* nothing
        // has mutated since it was built, it already contains the same scored
        // candidates the FTS table would produce, so the per-keystroke DB
        // round trip is pure overhead. A dirty flag keeps tag/relationship
        // writes honest: the FTS path (kept current by triggers) fills in
        // until the next cache refresh.
        if cache_complete
            && !self
                .tag_search_dirty
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return results;
        }

        let Some(fts_query) = tag_search::fts_query(tag) else {
            return results;
        };
        let conn = self.pool.get().unwrap();
        let stmt = conn.prepare(
            "SELECT t.id, t.name, t.count
             FROM Tags_Search_fts f
             JOIN Tags t ON t.id = f.rowid
             WHERE Tags_Search_fts MATCH ?1
             LIMIT ?2",
        );
        // A stale or missing FTS index (for example a database that predates
        // the table) must degrade to "RAM results" instead of panicking the
        // IPC worker and returning nothing to the caller. The cache rebuild on
        // the next startup heals the index.
        let mut stmt = match stmt {
            Ok(stmt) => stmt,
            Err(error) => {
                log::warn!("Tag search FTS query failed ({error}); falling back to cached results");
                return results;
            }
        };
        let mut candidates = match stmt.query_map(
            rusqlite::params![fts_query, tag_search::FTS_CANDIDATE_LIMIT],
            |row| {
                let tag_id: u64 = row.get(0)?;
                let tag_name: String = row.get(1)?;
                let count: u64 = row.get(2)?;
                Ok(tag_search::tag_entry(tag_id, &tag_name, count))
            },
        ) {
            Ok(candidates) => candidates,
            Err(error) => {
                log::warn!("Tag search FTS query failed ({error}); falling back to cached results");
                return results;
            }
        };
        let existing_ids = results.iter().map(|result| result.tag_id).collect();
        for candidate in
            tag_search::search_entries(candidates.by_ref().flatten(), tag, max_rows, &existing_ids)
        {
            if !results
                .iter()
                .any(|result| result.tag_id == candidate.tag_id)
            {
                results.push(candidate);
            }
        }
        results.sort_unstable_by(tag_search::compare_results);
        results.truncate(max_rows);
        results
    }

    /// Resolves tag names across namespaces and searches for files matching
    /// every input name, while allowing any tag with that name.
    #[must_use]
        pub fn search_db_files_by_tags_sync(&self, tags: &[String], limit: &Option<u64>) -> Vec<u64> {
        self.search_db_files_by_tag_groups_sync(&[], tags, &[], &[], &[], &[], limit)
    }

    /// Resolves tag names across namespaces while preserving boolean groups.
    #[must_use]
        pub fn search_db_files_by_tag_groups_sync(
        &self,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
        limit: &Option<u64>,
    ) -> Vec<u64> {
        let resolve_tags = |tag_names: &[String]| {
            tag_names
                .iter()
                .filter_map(|tag_name| {
                    if tag_search::normalize(tag_name).is_empty() {
                        return None;
                    }

                    let matching_ids = self
                        .search_db_tags_fts(tag_name, &Some(10))
                        .into_iter()
                        .map(|result| result.tag_id)
                        .collect::<Vec<_>>();

                    (!matching_ids.is_empty()).then_some(matching_ids)
                })
                .collect::<Vec<_>>()
        };

        let mut searches = Vec::new();
        for tag_id in and_ids {
            searches.push(SearchHolder::And(vec![*tag_id]));
        }
        let resolved_and = resolve_tags(and_tags);
        if resolved_and.len() != and_tags.len() {
            return Vec::new();
        }
        for matching_ids in resolved_and {
            searches.push(SearchHolder::Or(matching_ids));
        }
        if !or_tags.is_empty() {
            let mut matching_ids = or_ids.to_vec();
            matching_ids.extend(resolve_tags(or_tags).into_iter().flatten());
            if matching_ids.is_empty() {
                return Vec::new();
            }
            searches.push(SearchHolder::Or(matching_ids));
        } else if !or_ids.is_empty() {
            searches.push(SearchHolder::Or(or_ids.to_vec()));
        }
        let mut resolved_not = not_ids.to_vec();
        for matching_ids in resolve_tags(not_tags) {
            resolved_not.extend(matching_ids);
        }
        if !resolved_not.is_empty() {
            searches.push(SearchHolder::Not(resolved_not));
        }

        self.search_db_files_human_sync(
            &SearchObj {
                search_relate: None,
                searches,
            },
            limit,
        )
    }

    ///
    /// Gets the file path of a fileid
    ///
    #[must_use]
        pub fn file_get_physical_path_sync(&self, file_id: &u64) -> Option<String> {
        let conn = self.pool.get().unwrap();
        MainDatabase::internal_file_get_physical_path(&conn, file_id).ok()?
    }

        pub fn file_hashes_get(&self, file_id: &u64) -> HashMap<String, String> {
        let conn = self.pool.get().unwrap();
        let mut statement = conn
            .prepare("SELECT algorithm, digest FROM FileHashes WHERE file_id = ?1")
            .unwrap();
        statement
            .query_map([file_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .flatten()
            .collect()
    }

    ///
    /// Gets all tag ids assocated with a namespace id
    ///
        pub fn tag_id_get_namespace_id(&self, namespace_id: &u64) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        self.internal_tag_id_get_namespace_id(&conn, namespace_id)
            .unwrap_or_default()
    }

    /// Gets every tag id in the database.
        pub fn tag_id_get_all(&self) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        let Ok(mut statement) = conn.prepare("SELECT id FROM Tags") else {
            return HashSet::new();
        };
        let Ok(rows) = statement.query_map([], |row| row.get(0)) else {
            return HashSet::new();
        };
        rows.filter_map(Result::ok).collect()
    }

    ///
    /// Gets a file if a tag is associated with it
    ///
    #[must_use]
        pub fn tag_get_file_sync(&self, tag: &Tag) -> Option<FileInternal> {
        let conn = self.pool.get().unwrap();
        self.internal_tag_get_fileinternal(&conn, tag)
    }

    ///
    /// Gets all `file_ids` with tags that have namespace id
    ///
    #[must_use]
        pub fn file_id_get_namespace_id_sync(&self, namespace_id: &u64) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        self.internal_file_id_get_namespace_id(&conn, namespace_id)
            .unwrap_or_default()
    }

    ///
    /// Gets tag ids with a namespace_id associated with a file_id
    ///
    #[must_use]
        pub fn internal_file_id_get_tag_ids_where_namespace_id_sync(
        &self,
        file_id: &u64,
        namespace_id: &u64,
    ) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();

        self.internal_file_id_get_tag_ids_where_namespace_id(&conn, file_id, namespace_id)
            .unwrap_or_default()
    }

    ///
    /// Adds a relationship between a `file_id` and `tag_id`
    ///
    #[must_use]
        pub fn file_relationship_tags_add_sync(&self, file_id: &u64, tag: &[FileTagAction]) -> bool {
        let started = std::time::Instant::now();
        let lock_started = std::time::Instant::now();
        let mut guard = self.writer_lock();
        let writer_lock_elapsed = lock_started.elapsed();
        let transaction_started = std::time::Instant::now();
        let conn = guard.transaction().unwrap();
        let transaction_begin_elapsed = transaction_started.elapsed();

        Self::internal_audit_context_set(&conn, "relationship added").unwrap();
        let tag_started = std::time::Instant::now();
        let tag_map = self.internal_tag_bulk_add(&conn, tag, self.plugin_manager.clone());
        let tag_elapsed = tag_started.elapsed();
        let relationships: HashSet<(u64, u64)> = tag_map.values().map(|f| (*file_id, *f)).collect();
        let relationship_started = std::time::Instant::now();
        self.internal_relationships_bulk_add(&conn, &relationships);
        let relationship_elapsed = relationship_started.elapsed();

        conn.commit().unwrap();

        let elapsed = started.elapsed();
        if elapsed >= std::time::Duration::from_millis(100) {
            info!(
                "Performance: relationship tag update file_id={} tags={} writer_lock={:?} transaction_begin={:?} tag_resolution={:?} relationship_write={:?} commit_total={:?}",
                file_id,
                tag.len(),
                writer_lock_elapsed,
                transaction_begin_elapsed,
                tag_elapsed,
                relationship_elapsed,
                elapsed,
            );
        }

        true
    }

    /// Adds tag actions without creating a file/tag relationship.
    ///
    /// This is used by tag callbacks that create structural tag relationships.
        pub fn tag_actions_add_sync(&self, tag_actions: &[FileTagAction]) -> bool {
        if tag_actions.is_empty() {
            return true;
        }

        let mut guard = self.writer_lock();

        let Ok(conn) = guard.transaction() else {
            return false;
        };

        Self::internal_audit_context_set(&conn, "tag callback processed").unwrap();
        self.internal_tag_bulk_add(&conn, tag_actions, self.plugin_manager.clone());
        conn.commit().is_ok()
    }

    /// Adds tags to multiple files in one SQLite transaction.
    #[must_use]
        pub fn file_relationship_tags_add_bulk_sync(
        &self,
        tags_by_file: &HashMap<u64, Vec<FileTagAction>>,
    ) -> bool {
        if tags_by_file.is_empty() {
            return true;
        }

        let mut guard = self.writer_lock();
        let conn = match guard.transaction() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to begin bulk tag transaction: {error}");
                return false;
            }
        };

        // Accumulate every (file, tag) pair across all files, then insert the
        // whole set in one batched pass. Doing a relationship insert per file
        // kept the writer lock held while each call re-resolved namespaces
        // relationship-by-relationship.
        let mut relationships: HashSet<(u64, u64)> = HashSet::new();
        for (file_id, tags) in tags_by_file {
            Self::internal_audit_context_set(&conn, "relationship added").unwrap();
            let tag_map = self.internal_tag_bulk_add(&conn, tags, self.plugin_manager.clone());
            relationships.extend(tag_map.values().map(|tag_id| (*file_id, *tag_id)));
        }
        self.internal_relationships_bulk_add(&conn, &relationships);

        match conn.commit() {
            Ok(()) => true,
            Err(error) => {
                log::error!("Failed to commit bulk tag transaction: {error}");
                false
            }
        }
    }

    ///
    /// Gets all file ids inside of the db.
    /// #Safety Returns None if an error occurs
    ///
    #[must_use]
        pub fn file_id_get_all_sync(&self) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();

        MainDatabase::internal_file_id_get_all(&conn).unwrap_or_default()
    }

    ///
    /// Gets all tag ids associated with a fileid
    ///
        pub fn relationship_get_tag_id_sync(&self, file_id: &u64) -> HashSet<u64> {
        self.refresh_roaring_memory_if_dirty();
        let roaring_guard = self.relationship_roaring_storage.read();
        if let Some(roaring) = roaring_guard.as_ref()
            && let Some(tag_ids) = roaring.relationship_search_tagid_roaring_in_memory(*file_id)
        {
            return tag_ids.into_iter().collect();
        }

        let conn = self.pool.get().unwrap();

        let mut out = HashSet::new();
        if let Ok(tag_ids) = self.internal_file_id_get_tag_ids(&conn, file_id) {
            out.extend(tag_ids);
        }
        out
    }

    /// Gets tag relationships for multiple files in one IPC request.
        pub fn relationship_get_tag_id_many_sync(
        &self,
        file_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<u64>> {
        file_ids
            .iter()
            .map(|file_id| (*file_id, self.relationship_get_tag_id_sync(file_id)))
            .collect()
    }

    ///
    /// Gets all file ids associated with a tag_id
    ///
        pub fn relationship_get_file_id_sync(&self, tag_id: &u64) -> HashSet<u64> {
        self.refresh_roaring_memory_if_dirty();
        if let Some(guard) = self.relationship_roaring_storage.read().as_ref()
            && let Some(file_ids) = guard.relationship_search_fileid_roaring_in_memory(*tag_id)
        {
            return file_ids.into_iter().collect();
        }

        let conn = self.pool.get().unwrap();

        let mut out = HashSet::new();
        if let Ok(file_ids) = self.internal_tag_id_get_file_ids(&conn, tag_id) {
            out.extend(file_ids);
        }
        out
    }

    /// Gets file relationships for multiple tags in one IPC request.
        pub fn relationship_get_file_id_many_sync(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<u64>> {
        tag_ids
            .iter()
            .map(|tag_id| (*tag_id, self.relationship_get_file_id_sync(tag_id)))
            .collect()
    }

    /// Gets files whose tag is the related parent of the supplied structural tag.
        pub fn relationship_get_parent_file_id_sync(&self, tag_id: &u64) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        let relationships = self.relationship_union_source(&conn, "relationships");
        let query = format!(
            "SELECT DISTINCT relationships.file_id
             FROM {relationships}
             WHERE relationships.tag_id = ?1
                OR relationships.tag_id IN (
                    SELECT Parents.relate_tag_id
                    FROM Parents
                    WHERE Parents.tag_id = ?1
                )"
        );
        let Ok(mut statement) = conn.prepare(&query) else {
            return HashSet::new();
        };
        statement
            .query_map([tag_id], |row| row.get(0))
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    /// Gets every parent relation declared by a child tag.
        pub fn parent_relationships_get_sync(&self, tag_id: &u64) -> Vec<shared_types::TagParents> {
        let conn = self.pool.get().unwrap();
        let Ok(mut statement) =
            conn.prepare("SELECT tag_id, relate_tag_id, limit_to FROM Parents WHERE tag_id = ?1")
        else {
            return Vec::new();
        };
        statement
            .query_map([tag_id], |row| {
                Ok(shared_types::TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: row.get(2)?,
                })
            })
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    /// Gets parent relations for multiple child tags in one IPC request.
        pub fn parent_relationships_get_many_sync(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<shared_types::TagParents>> {
        tag_ids
            .iter()
            .map(|tag_id| (*tag_id, self.parent_relationships_get_sync(tag_id)))
            .collect()
    }

    /// Gets every child relation that points at a parent tag.
        pub fn child_relationships_get_sync(
        &self,
        relate_tag_id: &u64,
    ) -> Vec<shared_types::TagParents> {
        let conn = self.pool.get().unwrap();
        let Ok(mut statement) = conn.prepare(
            "SELECT tag_id, relate_tag_id, limit_to FROM Parents WHERE relate_tag_id = ?1",
        ) else {
            return Vec::new();
        };
        statement
            .query_map([relate_tag_id], |row| {
                Ok(shared_types::TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: row.get(2)?,
                })
            })
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    /// Gets child relations for multiple parent tags in one IPC request.
        pub fn child_relationships_get_many_sync(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<shared_types::TagParents>> {
        tag_ids
            .iter()
            .map(|tag_id| (*tag_id, self.child_relationships_get_sync(tag_id)))
            .collect()
    }

    /// Gets one exact child-parent relation, including its optional limit tag.
        pub fn parent_relationship_get_sync(
        &self,
        tag_id: &u64,
        relate_tag_id: &u64,
    ) -> Option<shared_types::TagParents> {
        let conn = self.pool.get().unwrap();
        conn.query_row(
            "SELECT tag_id, relate_tag_id, limit_to
             FROM Parents
             WHERE tag_id = ?1 AND relate_tag_id = ?2
             LIMIT 1",
            rusqlite::params![tag_id, relate_tag_id],
            |row| {
                Ok(shared_types::TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: row.get(2)?,
                })
            },
        )
        .ok()
    }

    ///
    /// Adds tags into db in a bulk manner
    ///
    #[must_use]
        pub fn tag_id_get_tag_sync(&self, tags: &HashSet<u64>) -> HashMap<u64, Tag> {
        if tags.is_empty() {
            return HashMap::new();
        }

        let mut out = HashMap::with_capacity(tags.len());
        let mut missing = HashSet::new();
        {
            let mut tag_cache = self.tag_cache.write();
            for tag_id in tags {
                if let Some(tag) = tag_cache.get(*tag_id) {
                    out.insert(*tag_id, tag);
                } else {
                    missing.insert(*tag_id);
                }
            }
        }

        if missing.is_empty() {
            return out;
        }

        let conn = self.pool.get().unwrap();
        let fetched = MainDatabase::internal_tag_id_get_tag(&conn, &missing);
        {
            let mut tag_cache = self.tag_cache.write();
            for (tag_id, tag) in &fetched {
                tag_cache.insert(*tag_id, tag.clone());
            }
        }
        out.extend(fetched);
        out
    }

    ///
    /// Marks a url as being dead in the db
    ///
        pub fn dead_url_add_sync(&self, dead_url: &String) -> bool {
        let mut writer_conn = self.writer_lock();
        let conn = writer_conn.transaction().unwrap();
        let _ = self.internal_dead_url_add(&conn, dead_url);
        let _ = conn.commit();

        false
    }

    ///
    /// Checks if a lsit of urls are dead
    ///
        pub fn dead_url_get_sync(&self, dead_urls: &[String]) -> HashMap<String, bool> {
        let conn = self.pool.get().unwrap();

        if let Ok(status) = self.internal_dead_url_exist(&conn, dead_urls) {
            return status;
        }
        HashMap::new()
    }

    ///
    /// Adds a namespace into the db
    ///
    #[must_use]
        pub fn namespace_add_sync(&self, namespace: &GenericNamespaceObj) -> u64 {
        let mut guard = self.writer_lock();
        let conn = guard.transaction().unwrap();
        let out = self.internal_namespace_get_or_create(&conn, namespace);
        conn.commit().unwrap();
        out
    }

    ///
    /// Human written tag searching layer
    ///
        #[allow(unreachable_code)]
    pub fn search_db_files_human_sync(&self, search: &SearchObj, limit: &Option<u64>) -> Vec<u64> {
        let mut out = Vec::new();

        self.refresh_roaring_memory_if_dirty();

        {
            let read_guard = self.relationship_roaring_storage.read();

            if let Some(ref roaring) = *read_guard {
                // Each holder is an independent required predicate. Evaluate them
                // in order: AND/OR holders intersect into the accumulator, NOT
                // holders subtract files matching any excluded tag. Grouping is
                // preserved so multiple OR groups stay separate constraints.
                let mut result: Option<roaring::RoaringTreemap> = None;
                let mut has_positive = false;
                let mut fully_cached = true;
                'roaring_loop: for search_holder in search.searches.iter() {
                    let (ids, is_not): (&[u64], bool) = match search_holder {
                        SearchHolder::And(ids) => (ids, false),
                        SearchHolder::Or(ids) => (ids, false),
                        SearchHolder::Not(ids) => (ids, true),
                    };
                    if ids.is_empty() {
                        continue;
                    }
                    if !ids
                        .iter()
                        .all(|tag_id| roaring.tag_is_cached_in_memory(*tag_id))
                    {
                        fully_cached = false;
                        break 'roaring_loop;
                    }
                    if is_not {
                        // Excluded set is the union of every excluded tag's files.
                        if let (Some(excluded), Some(current)) = (
                            roaring.internal_search_item(ids, DbSearchTypeEnum::Or),
                            result.as_mut(),
                        ) {
                            *current -= excluded.as_ref();
                        }
                    } else {
                        has_positive = true;
                        let group_type = if matches!(search_holder, SearchHolder::And(_)) {
                            DbSearchTypeEnum::And
                        } else {
                            DbSearchTypeEnum::Or
                        };
                        let group = roaring
                            .internal_search_item(ids, group_type)
                            .map(|group| group.into_owned());
                        result = match result {
                            Some(mut current) => {
                                current &=
                                    group.as_ref().expect("ids checked non-empty before search");
                                Some(current)
                            }
                            None => group,
                        };
                    }
                }

                if fully_cached && has_positive {
                    return match result {
                        Some(bitmap) => {
                            let mut results: Vec<u64> = bitmap.iter().rev().collect();
                            if let Some(limit) = limit {
                                results.truncate(*limit as usize);
                            }
                            results
                        }
                        None => Vec::new(),
                    };
                }
            }
        }

        let conn = self.pool.get().unwrap();

        // Builds mapping for id -> namespace id
        let mut id_ns_map = HashMap::new();
        {
            let mut cache_guard = self.tag_cache.write();
            let namespace_cache = self.namespace_cache.read();

            for search_holder in search.searches.iter() {
                let ids = match search_holder {
                    SearchHolder::And(ids) => ids,
                    SearchHolder::Or(ids) => ids,
                    SearchHolder::Not(ids) => ids,
                };
                for tag_id in ids {
                    if let Some(tag) = cache_guard.get(*tag_id) {
                        // Should be safe to do because we should always have immediate up
                        // to date namespace mappings and nothing should be missing
                        id_ns_map
                            .insert(*tag_id, *namespace_cache.get(&tag.namespace.name).unwrap());
                    } else if let Ok(ns_id) = self.get_namespace_id_from_tag_id(&conn, tag_id) {
                        id_ns_map.insert(*tag_id, ns_id);
                    }
                }
            }
        }

        // Each holder maps to one clause. Within a holder every tag is a query;
        // AND gaps intersect its queries, OR gaps union them, NOT gaps are an
        // exclusion set subtracted from the accumulated result. Set operators go
        // *between* operands, so the chain never carries a dangling operator.
        let mut sql_list: Vec<String> = Vec::new();
        let mut positive_count = 0usize;
        for search_holder in search.searches.iter() {
            let (ids, is_not): (&[u64], bool) = match search_holder {
                SearchHolder::And(ids) => (ids, false),
                SearchHolder::Or(ids) => (ids, false),
                SearchHolder::Not(ids) => (ids, true),
            };
            if ids.is_empty() {
                continue;
            }

            let mut queries = Vec::with_capacity(ids.len());
            for tag_id in ids {
                if let Some(namespace_id) = id_ns_map.get(tag_id) {
                    queries.push(self.generate_file_search_sql(tag_id, namespace_id));
                }
            }
            if queries.is_empty() {
                continue;
            }

            let clause = if queries.len() == 1 {
                queries.pop().unwrap()
            } else {
                let separator = if is_not || matches!(search_holder, SearchHolder::Or(_)) {
                    " UNION "
                } else {
                    " INTERSECT "
                };
                format!("({})", queries.join(separator))
            };

            if sql_list.is_empty() {
                sql_list.push(clause);
            } else if is_not {
                sql_list.push("EXCEPT".into());
                sql_list.push(clause);
            } else {
                sql_list.push("INTERSECT".into());
                sql_list.push(clause);
            }
            if !is_not {
                positive_count += 1;
            }
        }

        if sql_list.is_empty() {
            return Vec::new();
        }
        if positive_count == 0 {
            // Not-only search: subtract exclusions from every file in the library.
            sql_list.insert(0, "SELECT id AS file_id FROM File".into());
            sql_list.insert(1, "EXCEPT".into());
        }

        // Wrap the whole compound in a FROM subquery: a bare parenthesized
        // compound is rejected by the SQLite parser as a top-level statement,
        // and the outer SELECT lets a trailing LIMIT apply to the final set.
        let mut sql_string = format!("SELECT file_id FROM ({})", sql_list.join(" "));
        if let Some(limit) = limit {
            sql_string.push_str(&format!(" LIMIT {limit}"));
        }

        let mut stmt = conn.prepare(&sql_string).unwrap();

        let file_id_list = stmt.query_map([], |f| f.get::<_, u64>(0)).unwrap();
        for file_id in file_id_list.flatten() {
            out.push(file_id)
        }

        out
    }

    /// A sync function to get a function
    #[must_use]
        pub fn setting_get_sync(&self, name: &str) -> Option<DbSettingsObj> {
        let pool = self.pool.clone();
        let conn = pool.get().ok()?;
        self.internal_setting_get(&conn, name).ok().flatten()
    }

    ///
    /// Sets the setting in the db. Updates it if the setting already exists
    ///
    #[must_use]
        pub fn setting_set_sync(&self, obj: &DbSettingsObj) -> bool {
        let mut writer_conn = self.writer_lock();
        let conn = match writer_conn.transaction() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!(
                    "Failed to begin setting transaction for {}: {error}",
                    obj.name
                );
                return false;
            }
        };
        if let Err(error) = self.internal_setting_set(&conn, obj) {
            log::error!("Failed to set setting {}: {error}", obj.name);
            return false;
        }
        if let Err(error) = conn.commit() {
            log::error!("Failed to commit setting {}: {error}", obj.name);
            return false;
        }
        false
    }

    ///
    /// Adds job into db
    ///
    #[must_use]
        pub fn jobs_add_single_sync(&self, job: PluginJob) -> u64 {
        let mut writer_conn = self.writer_lock();
        let conn = match writer_conn.transaction() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to begin job insertion transaction: {error}");
                return 0;
            }
        };
        let out = self.internal_jobs_add(&conn, &job);
        if let Err(error) = conn.commit() {
            log::error!("Failed to commit job insertion: {error}");
            return 0;
        }
        out
    }
    /*
    ///
    /// Adds job into db asynchronously.
    ///
    pub async fn jobs_add_single(&self, job: PluginJob) -> u64 {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };
            self.internal_jobs_add(&conn, &job)
        })
        .await
        .unwrap()
    }*/

    pub fn set_plugin_manager(&self, plugin_manager_add: Arc<PluginManager>) {
        let mut plugin_manager = self.plugin_manager.write();
        *plugin_manager = Some(plugin_manager_add);
    }
}
