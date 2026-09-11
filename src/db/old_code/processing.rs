//! Database operations for the `processing` domain.

use super::file::{SourceUrlFileStatus, hashessupportedtoinner};
use super::{MainDatabase, SQL_CHUNK_SIZE};
use shared_types::{
    FileInternal, FileManager, FileTagAction, ScraperDataReturn, Tag, TagOperation, TagType,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl MainDatabase {
    ///
    /// Handles all the processing for files and tags and relational items
    ///
    pub async fn process_scraper(
        self: Arc<Self>,
        map: HashMap<FileManager, Vec<FileTagAction>>,
        jobs: Vec<ScraperDataReturn>,
        audit_reason: String,
    ) {
        // Early Exit
        if map.is_empty() && jobs.is_empty() {
            return;
        }

        let database = self.clone();

        tokio::task::spawn_blocking(move || {
            // Rebuild any in-memory roaring bitmaps that a contended write had
            // to skip, before the transactions below read them.
            database.refresh_roaring_memory_if_dirty();

            // Pending scrape jobs are small and independent of the file/tag
            // work below, so they get their own short transaction.
            if !jobs.is_empty() {
                let mut writer = database.writer_lock();
                let conn = writer.transaction().unwrap();
                'ScraperLoop: for scraperdatareturn in &jobs {
                    for skip_conditions in &scraperdatareturn.skip_conditions {
                        if database.should_skip_item(&conn, skip_conditions.clone()) {
                            continue 'ScraperLoop;
                        }
                    }

                    database.internal_jobs_add(&conn, &scraperdatareturn.job);
                }
                conn.commit().unwrap();
            }

            // The scraper result is chunked into several small transactions so
            // one huge page cannot hold the single writer lock for the tens of
            // seconds the old one-shot path took. Other writers (job claims,
            // dead-URL adds, thumbnailer tag adds) interleave between chunks.
            const PROCESS_CHUNK_SIZE: usize = 100;
            let map_entries: Vec<(FileManager, Vec<FileTagAction>)> = map.into_iter().collect();
            for chunk in map_entries.chunks(PROCESS_CHUNK_SIZE) {
                let chunk_map: HashMap<FileManager, Vec<FileTagAction>> =
                    chunk.iter().cloned().collect();
                database.process_scraper_chunk(chunk_map, &audit_reason);
                // Keep the roaring memory view reasonably current while a long
                // scrape enumerates its chunks.
                database.refresh_roaring_memory_if_dirty();
            }
        })
        .await
        .unwrap();
    }

    /// Persists one chunk of a scraper result inside its own write transaction.
    ///
    /// Each chunk reaches for the writer lock independently, so between chunks
    /// the single database writer is free for other work.
    fn process_scraper_chunk(
        &self,
        map: HashMap<FileManager, Vec<FileTagAction>>,
        audit_reason: &str,
    ) {
        if map.is_empty() {
            return;
        }

        let mut writer = self.writer_lock();
        let conn = writer.transaction().unwrap();

        let unique_files: HashSet<FileInternal> = map.keys().map(|f| f.internal.clone()).collect();
        let resolved_files = self.internal_file_bulk_add(&conn, unique_files);

        let mapped_files: Vec<_> = map
            .keys()
            .filter_map(|file_manager| {
                // Find the matching resolved file
                let matching_res = resolved_files
                    .iter()
                    .find(|res| res.hash == file_manager.internal.hash)?;

                let mut temp = file_manager.clone();
                temp.internal = matching_res.clone();
                Some(temp)
            })
            .collect();

        for file in mapped_files {
            if let Some(file_id) = file.internal.id {
                for hash in &file.identifying_hashes {
                    let (algo, hash_str) = hashessupportedtoinner(hash);

                    Self::internal_file_hash_add(&conn, &algo.to_string(), hash_str, &file_id);
                }
            }
        }

        // Build a quick lookup mapping: FileInternal -> Database u64 ID
        let mut file_cache = HashMap::with_capacity(resolved_files.len());
        for file in &resolved_files {
            if let Some(db_id) = file.id {
                file_cache.insert(file.hash.clone(), db_id);
            }
        }

        // Collect all action definitions across every file block into one flat vector
        let all_tag_actions: Vec<FileTagAction> = map.values().flatten().cloned().collect();

        Self::internal_audit_context_set(&conn, audit_reason).unwrap();
        let tag_cache =
            self.internal_tag_bulk_add(&conn, &all_tag_actions, self.plugin_manager.clone());

        let file_ids: Vec<u64> = file_cache.values().copied().collect();
        let current_file_relationships = self
            .internal_file_id_get_tag_ids_bulk(&conn, &file_ids)
            .unwrap();

        let mut rels_to_add = HashSet::new();
        let mut rels_to_del = HashSet::new();

        let mut current_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
        let mut incoming_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
        let mut explicit_adds = HashSet::new();
        let mut set_deletions = HashSet::new();

        let mut tag_id_to_obj = HashMap::with_capacity(tag_cache.len());
        for (tag_obj, &tag_id) in &tag_cache {
            tag_id_to_obj.insert(tag_id, tag_obj);
        }

        for (file_internal, tag_list) in &map {
            let file_id = match file_cache.get(&file_internal.internal.hash) {
                Some(&id) => id,
                None => continue,
            };

            current_ns_tags.clear();
            explicit_adds.clear();
            set_deletions.clear();

            // Map current database state for this file: Namespace (&str) -> HashSet<tag_id>
            if let Some(current_tag_ids) = current_file_relationships.get(&file_id) {
                for &tag_id in current_tag_ids {
                    // Instantly resolve the full Tag object using the raw ID
                    if let Some(tag) = tag_id_to_obj.get(&tag_id) {
                        let ns_name = &tag.namespace.name;

                        if ns_name != "source_url" && !ns_name.is_empty() {
                            current_ns_tags
                                .entry(ns_name.as_str())
                                .or_default()
                                .insert(tag_id);
                        }
                    }
                }
            }
            // Process operations
            for tag_action in tag_list {
                match tag_action.operation {
                    TagOperation::Add => {
                        for tag in &tag_action.tags {
                            if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                && let Some(&tag_id) = tag_cache.get(&tag.tag)
                            {
                                rels_to_add.insert((file_id, tag_id));
                                explicit_adds.insert(tag_id);
                            }
                        }
                    }
                    TagOperation::Del => {
                        for tag in &tag_action.tags {
                            if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                && let Some(&tag_id) = tag_cache.get(&tag.tag)
                            {
                                rels_to_del.insert((file_id, tag_id));
                            }
                        }
                    }
                    TagOperation::Set => {
                        incoming_ns_tags.clear();

                        for tag in &tag_action.tags {
                            if !matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex) {
                                continue;
                            }
                            let ns_name = &tag.tag.namespace.name;
                            if ns_name == "source_url" || ns_name.is_empty() {
                                continue;
                            }

                            if let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                incoming_ns_tags
                                    .entry(ns_name.as_str())
                                    .or_default()
                                    .insert(tag_id);

                                rels_to_add.insert((file_id, tag_id));
                            }
                        }

                        // Evaluate deletions ONLY for namespaces explicitly targeted by this Set operation
                        for (ns_name, incoming_set) in &incoming_ns_tags {
                            if let Some(current_tag_ids) = current_ns_tags.get(ns_name) {
                                for current_tag_id in current_tag_ids {
                                    if !incoming_set.contains(current_tag_id) {
                                        set_deletions.insert((file_id, *current_tag_id));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Apply targeted "Add overrides Set" rule
            for (f_id, tag_id) in &set_deletions {
                if !explicit_adds.contains(tag_id) {
                    rels_to_del.insert((*f_id, *tag_id));
                }
            }
        }

        // Global sanitation check for any edge deletions
        for del in &rels_to_del {
            rels_to_add.remove(del);
        }

        // Flush Relationship Mutations to DB in Batch
        if !rels_to_del.is_empty() {
            Self::internal_audit_context_set(&conn, audit_reason).unwrap();
            self.internal_relationship_bulk_delete(&conn, &rels_to_del);
        }

        if !rels_to_add.is_empty() {
            Self::internal_audit_context_set(&conn, audit_reason).unwrap();
            self.internal_relationships_bulk_add(&conn, &rels_to_add);
        }

        conn.commit().unwrap();
    }

    /// Gets existing files associated with source URLs in one database query.
    pub async fn source_url_files_get(
        self: Arc<Self>,
        url_set: HashSet<String>,
    ) -> HashMap<String, SourceUrlFileStatus> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            let mut out: HashMap<String, SourceUrlFileStatus> = HashMap::new();
            let urls = url_set.into_iter().collect::<Vec<_>>();
            for urls in urls.chunks(SQL_CHUNK_SIZE) {
                if urls.is_empty() {
                    continue;
                }
                let placeholders = std::iter::repeat_n("?", urls.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!("SELECT url FROM dead_urls WHERE url IN ({placeholders})");
                let mut stmt = conn.prepare(&query).unwrap();
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(urls.iter()), |row| {
                        row.get::<_, String>(0)
                    })
                    .unwrap();
                for url in rows.flatten() {
                    out.entry(url).or_default().dead = true;
                }
            }

            let source_url_namespace_id = self.internal_namespace_get_id(&conn, "source_url");
            let Some(source_url_namespace_id) = source_url_namespace_id else {
                return out;
            };
            let relationship_table = self.relationship_partition_name(source_url_namespace_id);

            // Resolve the smallest file per source URL in one grouped join
            // instead of a correlated subquery per URL.
            let mut url_to_file_id: HashMap<String, u64> = HashMap::new();
            for urls in urls.chunks(SQL_CHUNK_SIZE) {
                if urls.is_empty() {
                    continue;
                }
                let placeholders = (1..=urls.len())
                    .map(|index| format!("?{index}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!(
                    "SELECT t.name, MIN(r.file_id)
                     FROM Tags t
                     JOIN {relationship_table} r ON r.tag_id = t.id
                     WHERE t.namespace = ?{namespace_param}
                       AND t.name IN ({placeholders})
                     GROUP BY t.id",
                    namespace_param = urls.len() + 1,
                );
                let mut stmt = conn.prepare(&query).unwrap();
                let mut query_params: Vec<&dyn rusqlite::ToSql> =
                    urls.iter().map(|url| url as &dyn rusqlite::ToSql).collect();
                query_params.push(&source_url_namespace_id);
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(query_params), |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                    })
                    .unwrap();
                for (url, file_id) in rows.flatten() {
                    url_to_file_id.insert(url, file_id);
                }
            }
            let file_id_to_url: HashMap<u64, String> = url_to_file_id
                .iter()
                .map(|(url, file_id)| (*file_id, url.clone()))
                .collect();

            // Fetch the matching file attributes in one batched pass.
            let file_ids = url_to_file_id.values().copied().collect::<Vec<_>>();
            for file_ids in file_ids.chunks(SQL_CHUNK_SIZE) {
                if file_ids.is_empty() {
                    continue;
                }
                let placeholders = (1..=file_ids.len())
                    .map(|index| format!("?{index}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!(
                    "SELECT id, hash, extension, storage_id, size_bytes
                     FROM File WHERE id IN ({placeholders})"
                );
                let mut stmt = conn.prepare(&query).unwrap();
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(file_ids.iter()), |row| {
                        Ok(FileInternal {
                            id: row.get(0)?,
                            hash: row.get(1)?,
                            extension: row.get(2)?,
                            storage_id: row.get(3)?,
                            size_bytes: row.get(4)?,
                        })
                    })
                    .unwrap();
                for file_internal in rows.flatten() {
                    if let Some(id) = file_internal.id
                        && let Some(url) = file_id_to_url.get(&id)
                    {
                        out.entry(url.clone()).or_default().file = Some(file_internal);
                    }
                }
            }
            out
        })
        .await
        .unwrap()
    }

    ///
    /// Checks if we should download the file or not
    ///
    pub async fn should_download_file(self: Arc<Self>, url: String) -> bool {
        let database = self.clone();
        let pool = self.pool.clone();
        let roaring = self.relationship_roaring_storage.clone();

        tokio::task::spawn_blocking(move || {
            // A stale in-memory roaring copy would make the URL look already
            // downloaded (or vice versa); reconcile it before deciding.
            database.refresh_roaring_memory_if_dirty();
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            let source_url_nsid = database.internal_namespace_sourceurl_get(&conn);
            let Some(tag_id) = Self::internal_tag_get_id(&conn, &url, source_url_nsid) else {
                return true;
            };
            if let Some(guard) = roaring.read().as_ref()
                && let Some(file_ids) = guard.relationship_search_fileid_roaring_in_memory(tag_id)
            {
                return file_ids.is_empty();
            }
            !self.internal_tag_has_files(&conn, tag_id)
        })
        .await
        .unwrap()
    }

    ///
    /// Gets a single `file_id` from a tag
    ///
    pub async fn tag_get_file_id(self: Arc<Self>, tag: &Tag) -> Option<u64> {
        let pool = self.pool.clone();

        let tag = tag.clone();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            self.internal_tag_get_file_id(&conn, &tag)
        })
        .await
        .unwrap()
    }
}
