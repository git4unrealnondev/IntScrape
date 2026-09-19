//! Turso-native processing for scraper results: persistence of files, tags,
//! relationships, and pending plugin jobs.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use shared_types::{
    FileInternal, FileManager, FileTagAction, GenericNamespaceObj, ScraperDataReturn, TagOperation,
};
use turso::{Result, Value, params_from_iter};

use crate::db::SourceUrlFileStatus;
use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Handles all the processing for files and tags and relational items.
    /// Returns `true` when every chunk persisted successfully. A `false`
    /// return means some database operation failed and the caller may decide
    /// to keep its job so it can be retried later.
    pub async fn process_scraper(
        self: std::sync::Arc<Self>,
        map: HashMap<FileManager, Vec<FileTagAction>>,
        jobs: Vec<ScraperDataReturn>,
        audit_reason: String,
    ) -> bool {
        if map.is_empty() && jobs.is_empty() {
            return true;
        }
        let _ = &audit_reason;

        let database = self.clone();

        // Pending scrape jobs are small and independent of the file/tag work
        // below, so they get their own connection.
        if !jobs.is_empty() {
            loop {
                let conn = match database.connect() {
                    Ok(conn) => conn,
                    Err(error) => {
                        log::error!("Failed to connect while adding pending scrape jobs: {error}");
                        return false;
                    }
                };
                if let Err(error) = conn.execute("BEGIN CONCURRENT", ()).await {
                    log::error!("Failed to begin concurrent scrape-job transaction: {error}");
                    return false;
                }

                let mut failed = false;
                'ScraperLoop: for scraperdatareturn in &jobs {
                    for skip_conditions in &scraperdatareturn.skip_conditions {
                        if database
                            .should_skip_item(&conn, skip_conditions.clone())
                            .await
                        {
                            continue 'ScraperLoop;
                        }
                    }
                    if let Err(error) = database.job_add_sql(&conn, &scraperdatareturn.job).await {
                        log::error!("Failed to add scrape job: {error}");
                        failed = true;
                        break;
                    }
                }
                if failed {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return false;
                }

                match conn.execute("COMMIT", ()).await {
                    Ok(_) => break,
                    Err(error)
                        if matches!(
                            error,
                            turso::Error::Busy(_) | turso::Error::BusySnapshot(_)
                        ) =>
                    {
                        log::warn!(
                            "Concurrent scrape-job commit conflicted; retrying in 50ms: {error}"
                        );
                        let _ = conn.execute("ROLLBACK", ()).await;
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(error) => {
                        log::error!("Failed to commit concurrent scrape-job transaction: {error}");
                        return false;
                    }
                }
            }
        }

        // Namespace rows + Relationship_{id} partitions are created with DDL,
        // which turso only permits inside an exclusive transaction. Ensure
        // every namespace this scrape references exists (and is cached) up
        // front so the chunk transactions below stay BEGIN CONCURRENT and are
        // DML-only. The ensure is a fast no-op once everything is cached.
        let namespace_set: HashSet<GenericNamespaceObj> = map
            .iter()
            .flat_map(|(_, actions)| actions.iter())
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
        if let Err(error) = database.namespace_ensure_set(&namespace_set).await {
            log::error!("Failed to pre-ensure namespaces for scrape: {error}");
            return false;
        }

        // The scraper result is chunked into several small connections so one
        // huge page cannot hold the database busy for too long.
        const PROCESS_CHUNK_SIZE: usize = 100;
        let map_entries: Vec<(FileManager, Vec<FileTagAction>)> = map.into_iter().collect();
        for chunk in map_entries.chunks(PROCESS_CHUNK_SIZE) {
            let chunk_map: HashMap<FileManager, Vec<FileTagAction>> =
                chunk.iter().cloned().collect();
            if !database
                .process_scraper_chunk_human(chunk_map)
                .await
                .unwrap_or_else(|error| {
                    log::error!("Failed to process scraper chunk: {error}");
                    false
                })
            {
                return false;
            }
        }
        true
    }

    async fn process_scraper_chunk_human(
        &self,
        map: HashMap<FileManager, Vec<FileTagAction>>,
    ) -> Result<bool> {
        let mut cnt = 0;
        loop {
            // Early Exit
            if map.is_empty() {
                return Ok(true);
            }

            // doing processing outside of transaction ideally
            let mut file_hashes = Vec::new();
            let better_mapping: HashMap<_, _> = map
                .iter()
                .map(|(filemanager, tag_actions)| {
                    (
                        filemanager.internal.hash.clone(),
                        (tag_actions, filemanager.identifying_hashes.clone()),
                    )
                })
                .collect();

            let all_tags: Vec<FileTagAction> = map.values().flatten().cloned().collect();

            let unique_files: HashSet<FileInternal> =
                map.keys().map(|f| f.internal.clone()).collect();
            let file_list: Vec<FileInternal> = unique_files.into_iter().collect();

            // Cant connect to db?
            let mut conn = self.connect()?;

            let tn = conn
                .transaction_with_behavior(turso::transaction::TransactionBehavior::Concurrent)
                .await?;

            let corrected_files = self.file_add_bulk(&tn, &file_list).await?;

            // Gets a file with id with a list of filetagaction
            let mapped_map: HashMap<_, _> = corrected_files
                .into_iter()
                .filter_map(|file| {
                    if let Some((tag_actions, identifying_hashes)) =
                        better_mapping.get(&file.hash)
                    {
                        for file_hash in identifying_hashes {
                            let (algo, algo_hash) = crate::db::hashessupportedtoinner(file_hash);
                            file_hashes.push((file.id.unwrap(), algo, algo_hash.as_str()));
                        }
                        Some((file, tag_actions))
                    } else {
                        None
                    }
                })
                .collect();

            // Adds file hashes into the db
            if !file_hashes.is_empty() {
                self.file_hashes_add_bulk(&tn, &file_hashes).await?;
            }

            let tag_id_mapping = self.tag_action_bulk_add(&tn, &all_tags).await?;
            let mut rels_to_add = HashSet::new();
            let mut rels_to_del = HashSet::new();

            for (file, tag_actions) in mapped_map {
                let mut incoming_ns: HashMap<&str, HashSet<u64>> = HashMap::new();

                for tag_action in *tag_actions {
                    match tag_action.operation {
                        TagOperation::Add => {
                            for tag in tag_action.tags.iter() {
                                if let Some(tag_id) = tag_id_mapping.get(&tag.tag) {
                                    rels_to_add.insert((file.id.unwrap(), *tag_id as u64));
                                }
                            }
                        }
                        TagOperation::Set => {
                            for tag in tag_action.tags.iter() {
                                let ns_name = &tag.tag.namespace.name;
                                if ns_name == "source_url" || ns_name.is_empty() {
                                    continue;
                                }

                                if let Some(tag_id) = tag_id_mapping.get(&tag.tag) {
                                    incoming_ns
                                        .entry(ns_name.as_str())
                                        .or_default()
                                        .insert(*tag_id as u64);
                                }
                            }
                        }
                        TagOperation::Del => {
                            for tag in tag_action.tags.iter() {
                                if let Some(tag_id) = tag_id_mapping.get(&tag.tag) {
                                    rels_to_del.insert((file.id.unwrap(), *tag_id as u64));
                                }
                            }
                        }
                    }
                }

                if !incoming_ns.is_empty() {
                    for (ns_name, tag_ids) in incoming_ns {
                        if let Some(ns_id) = self.namespace_get_id(&tn, ns_name).await? {
                            let current_tag_ids = self
                                .file_id_get_tag_ids_filtered(&tn, file.id.unwrap(), ns_id)
                                .await?;

                            for tag_id_to_remove in tag_ids.difference(&current_tag_ids) {
                                rels_to_add.insert((file.id.unwrap(), *tag_id_to_remove));
                            }

                            for tag_id_to_add in current_tag_ids.difference(&tag_ids) {
                                rels_to_del.insert((file.id.unwrap(), *tag_id_to_add));
                            }
                        }
                    }
                }
            }

            for rel in rels_to_del.iter() {
                rels_to_add.remove(rel);
            }

            if !rels_to_del.is_empty() {
                self.relationship_bulk_delete(&tn, &rels_to_del).await?;
            }

            if !rels_to_add.is_empty() {
                self.relationships_bulk_add(&tn, &rels_to_add).await?;
            }

            match tn.commit().await {
                Ok(_) => {return Ok(true);},
                Err(err) => {
                    if Self::is_concurrency_conflict(&err) {
                        log::warn!("Scraper chunk commit conflicted; retrying in 50ms: {err}");
                        cnt += 1;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    } else {
                        return Err(err);
                    }
                }
            }



            if cnt >= 25 {
                return Ok(false);
            }
        }
    }

    /// Persists one chunk of a scraper result inside its own connection.
    /// Returns `false` if any database operation in the chunk failed.
    async fn process_scraper_chunk_old(
        &self,
        map: HashMap<FileManager, Vec<FileTagAction>>,
        audit_reason: &str,
    ) -> bool {
        // Bounded retry cap for the whole chunk. Each conflict path below
        // used to re-run the entire chunk transaction with no limit; under
        // write-write contention from many concurrent jobs that burned wide
        // open on the shared tokio runtime and starved the network layer.
        // 25 attempts (~1.25s) rides out typical contention windows between
        // the 10 concurrent rule34 jobs while staying strictly bounded.
        const MAX_SCRAPER_CHUNK_ATTEMPTS: u32 = 25;
        self.process_scraper_chunk_attempt(map, audit_reason, MAX_SCRAPER_CHUNK_ATTEMPTS)
            .await
    }

    async fn process_scraper_chunk_attempt(
        &self,
        map: HashMap<FileManager, Vec<FileTagAction>>,
        audit_reason: &str,
        attempts_left: u32,
    ) -> bool {
        if map.is_empty() {
            return true;
        }

        let Ok(conn) = self.connect() else {
            log::error!("Failed to connect while processing scraper chunk");
            return false;
        };
        // The scraper chunk is DML-only here: namespaces were pre-ensured
        // (rows + partitions + in-memory cache) before the chunk loop started,
        // so tag adds resolve namespace ids from the cache and never run the
        // namespace-partition DDL that turso forbids in concurrent
        // transactions. BEGIN CONCURRENT lets unrelated writers keep going.
        loop {
            match conn.execute("BEGIN CONCURRENT", ()).await {
                Ok(_) => break,
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => {
                    log::error!("Failed to begin concurrent scraper transaction: {error}");
                    return false;
                }
            }
        }
        let _ = audit_reason;

        let unique_files: HashSet<FileInternal> = map.keys().map(|f| f.internal.clone()).collect();
        let file_list: Vec<FileInternal> = unique_files.into_iter().collect();
        let resolved_files = match self.file_add_bulk(&conn, &file_list).await {
            Ok(files) => files,
            Err(error) if Self::is_concurrency_conflict(&error) => {
                log::warn!("Scraper file transaction conflicted; retrying in 50ms: {error}");
                let _ = conn.execute("ROLLBACK", ()).await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                return if attempts_left <= 1 {
                    log::error!("Scraper file transaction kept conflicting; giving up on chunk");
                    false
                } else {
                    Box::pin(self.process_scraper_chunk_attempt(
                        map,
                        audit_reason,
                        attempts_left - 1,
                    ))
                    .await
                };
            }
            Err(error) => {
                log::error!("Failed to insert scraper files: {error}");
                let _ = conn.execute("ROLLBACK", ()).await;
                return false;
            }
        };
        let resolved_files: Vec<FileInternal> = resolved_files.into_iter().collect();

        let resolved_files_by_hash: HashMap<&str, &FileInternal> = resolved_files
            .iter()
            .map(|file| (file.hash.as_str(), file))
            .collect();
        let mapped_files: Vec<_> = map
            .keys()
            .filter_map(|file_manager| {
                let matching_res =
                    resolved_files_by_hash.get(file_manager.internal.hash.as_str())?;
                let mut temp = file_manager.clone();
                temp.internal = (*matching_res).clone();
                Some(temp)
            })
            .collect();

        let mut identifying_hashes: Vec<(u64, String, String)> = Vec::new();
        for file in mapped_files {
            if let Some(file_id) = file.internal.id {
                for hash in &file.identifying_hashes {
                    let (algo, hash_str) = crate::db::hashessupportedtoinner(hash);
                    identifying_hashes.push((file_id, algo.to_string(), hash_str.clone()));
                }
            }
        }
        if !identifying_hashes.is_empty() {
            let entries: Vec<(u64, &str, &str)> = identifying_hashes
                .iter()
                .map(|(id, algo, digest)| (*id, algo.as_str(), digest.as_str()))
                .collect();
            if let Err(error) = self.file_hashes_add_bulk(&conn, &entries).await {
                log::error!("Failed to add identifying hashes: {error}");
                let _ = conn.execute("ROLLBACK", ()).await;
                return false;
            }
        }

        // Build a quick lookup mapping: hash -> database id.
        let mut file_cache = HashMap::with_capacity(resolved_files.len());
        for file in &resolved_files {
            if let Some(db_id) = file.id {
                file_cache.insert(file.hash.clone(), db_id);
            }
        }

        // Collect all action definitions across every file block into one flat vector.
        let all_tag_actions: Vec<FileTagAction> = map.values().flatten().cloned().collect();
        let tag_cache = match self.tag_action_bulk_add(&conn, &all_tag_actions).await {
            Ok(tag_cache) => tag_cache,
            Err(error) if Self::is_concurrency_conflict(&error) => {
                log::warn!("Scraper tag transaction conflicted; retrying in 50ms: {error}");
                let _ = conn.execute("ROLLBACK", ()).await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                return if attempts_left <= 1 {
                    log::error!("Scraper tag transaction kept conflicting; giving up on chunk");
                    false
                } else {
                    Box::pin(self.process_scraper_chunk_attempt(
                        map,
                        audit_reason,
                        attempts_left - 1,
                    ))
                    .await
                };
            }
            Err(error) => {
                log::error!("Failed to add scraper tags: {error}");
                let _ = conn.execute("ROLLBACK", ()).await;
                return false;
            }
        };

        let file_ids: Vec<u64> = file_cache.values().copied().collect();
        let Ok(current_file_relationships) = self.file_id_get_tag_ids_bulk(&conn, &file_ids).await
        else {
            log::error!("Failed to read current file relationships");
            let _ = conn.execute("ROLLBACK", ()).await;
            return false;
        };

        let mut rels_to_add = HashSet::new();
        let mut rels_to_del = HashSet::new();

        let mut current_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
        let mut incoming_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
        let mut explicit_adds = HashSet::new();
        let mut set_deletions = HashSet::new();

        let mut tag_id_to_obj = HashMap::with_capacity(tag_cache.len());
        for (tag_obj, &tag_id) in &tag_cache {
            tag_id_to_obj.insert(tag_id as u64, tag_obj);
        }

        for (file_internal, tag_list) in &map {
            let file_id = match file_cache.get(&file_internal.internal.hash) {
                Some(&id) => id,
                None => continue,
            };

            current_ns_tags.clear();
            explicit_adds.clear();
            set_deletions.clear();

            // Map current database state for this file: Namespace -> set of tag ids.
            if let Some(current_tag_ids) = current_file_relationships.get(&file_id) {
                for &tag_id in current_tag_ids {
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

            for tag_action in tag_list {
                match tag_action.operation {
                    TagOperation::Add => {
                        for tag in &tag_action.tags {
                            if let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                rels_to_add.insert((file_id, tag_id as u64));
                                explicit_adds.insert(tag_id as u64);
                            }
                        }
                    }
                    TagOperation::Del => {
                        for tag in &tag_action.tags {
                            if let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                rels_to_del.insert((file_id, tag_id as u64));
                            }
                        }
                    }
                    TagOperation::Set => {
                        incoming_ns_tags.clear();

                        for tag in &tag_action.tags {
                            let ns_name = &tag.tag.namespace.name;
                            if ns_name == "source_url" || ns_name.is_empty() {
                                continue;
                            }
                            if let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                incoming_ns_tags
                                    .entry(ns_name.as_str())
                                    .or_default()
                                    .insert(tag_id as u64);
                                rels_to_add.insert((file_id, tag_id as u64));
                            }
                        }

                        // Evaluate deletions only for namespaces explicitly
                        // targeted by this Set operation.
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

            // Apply targeted "Add overrides Set" rule.
            for (f_id, tag_id) in &set_deletions {
                if !explicit_adds.contains(tag_id) {
                    rels_to_del.insert((*f_id, *tag_id));
                }
            }
        }

        // Global sanitation check for any edge deletions.
        for del in &rels_to_del {
            rels_to_add.remove(del);
        }

        if !rels_to_del.is_empty()
            && let Err(error) = self.relationship_bulk_delete(&conn, &rels_to_del).await
        {
            let _ = conn.execute("ROLLBACK", ()).await;
            if Self::is_concurrency_conflict(&error) {
                log::warn!("Scraper relationship delete conflicted; retrying in 50ms: {error}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                return if attempts_left <= 1 {
                    log::error!("Scraper relationship delete kept conflicting; giving up on chunk");
                    false
                } else {
                    Box::pin(self.process_scraper_chunk_attempt(
                        map,
                        audit_reason,
                        attempts_left - 1,
                    ))
                    .await
                };
            }
            log::error!("Failed to delete relationships in scraper chunk: {error}");
            return false;
        }

        if !rels_to_add.is_empty()
            && let Err(error) = self.relationships_bulk_add(&conn, &rels_to_add).await
        {
            let _ = conn.execute("ROLLBACK", ()).await;
            if Self::is_concurrency_conflict(&error) {
                log::warn!("Scraper relationship add conflicted; retrying in 50ms: {error}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                return if attempts_left <= 1 {
                    log::error!("Scraper relationship add kept conflicting; giving up on chunk");
                    false
                } else {
                    Box::pin(self.process_scraper_chunk_attempt(
                        map,
                        audit_reason,
                        attempts_left - 1,
                    ))
                    .await
                };
            }
            log::error!("Failed to add relationships in scraper chunk: {error}");
            return false;
        }

        match conn.execute("COMMIT", ()).await {
            Ok(_) => true,
            Err(error) if Self::is_concurrency_conflict(&error) => {
                // The caller owns the input map, so the whole chunk can be
                // safely reconstructed from the same snapshot on retry.
                log::warn!("Concurrent scraper commit conflicted; retrying in 50ms: {error}");
                let _ = conn.execute("ROLLBACK", ()).await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if attempts_left <= 1 {
                    log::error!("Scraper chunk commit kept conflicting; giving up on chunk");
                    false
                } else {
                    Box::pin(self.process_scraper_chunk_attempt(
                        map,
                        audit_reason,
                        attempts_left - 1,
                    ))
                    .await
                }
            }
            Err(error) => {
                log::error!("Failed to commit concurrent scraper transaction: {error}");
                false
            }
        }
    }

    /// Gets existing files associated with source URLs plus dead-url state.
    pub async fn source_url_files_get(
        self: std::sync::Arc<Self>,
        url_set: HashSet<String>,
    ) -> HashMap<String, SourceUrlFileStatus> {
        let mut out: HashMap<String, SourceUrlFileStatus> = HashMap::new();
        if url_set.is_empty() {
            return out;
        }

        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while resolving source url files: {error}");
                return out;
            }
        };

        let urls: Vec<String> = url_set.into_iter().collect();
        if let Ok(dead_status) = self.dead_url_get(&conn, &urls).await {
            for (url, dead) in dead_status {
                if dead {
                    out.entry(url).or_default().dead = true;
                }
            }
        }

        let Ok(Some(source_url_namespace_id)) = self.namespace_get(&conn, "source_url").await
        else {
            return out;
        };
        let relationship_table = format!("Relationship_{source_url_namespace_id}");

        // Resolve the smallest file per source URL in one grouped join.
        let mut url_to_file_id: HashMap<String, u64> = HashMap::new();
        for urls in urls.chunks(crate::db::SQL_CHUNK_SIZE) {
            if urls.is_empty() {
                continue;
            }
            let placeholders = std::iter::repeat_n("?", urls.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT t.name, MIN(r.file_id)
                 FROM Tags t
                 JOIN {relationship_table} r ON r.tag_id = t.id
                 WHERE t.namespace = ?1
                   AND t.name IN ({}) 
                 GROUP BY t.id;",
                placeholders
            );
            let mut params: Vec<Value> = urls.iter().map(|url| Value::from(url.as_str())).collect();
            params.insert(0, Value::from(source_url_namespace_id as i64));
            if let Ok(mut rows) = conn.query(&sql, params_from_iter(params)).await {
                while let Ok(Some(row)) = rows.next().await {
                    if let (Ok(url), Ok(file_id)) = (row.get::<String>(0), row.get::<u64>(1)) {
                        url_to_file_id.entry(url).or_insert(file_id);
                    }
                }
            }
        }

        let file_id_to_url: HashMap<u64, String> = url_to_file_id
            .iter()
            .map(|(url, file_id)| (*file_id, url.clone()))
            .collect();

        let file_ids: Vec<u64> = url_to_file_id.values().copied().collect();
        for file_ids in file_ids.chunks(crate::db::SQL_CHUNK_SIZE) {
            if file_ids.is_empty() {
                continue;
            }
            let placeholders = std::iter::repeat_n("?", file_ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT id, hash, extension, storage_id, size_bytes
                 FROM File WHERE id IN ({placeholders});"
            );
            let params: Vec<Value> = file_ids.iter().map(|id| Value::from(*id as i64)).collect();
            if let Ok(mut rows) = conn.query(&sql, params_from_iter(params)).await {
                while let Ok(Some(row)) = rows.next().await {
                    let file_internal = FileInternal {
                        id: row.get(0).ok(),
                        hash: row.get(1).unwrap_or_default(),
                        extension: row.get(2).unwrap_or_default(),
                        storage_id: row.get(3).unwrap_or_default(),
                        size_bytes: row.get(4).ok(),
                    };
                    if let Some(id) = file_internal.id
                        && let Some(url) = file_id_to_url.get(&id)
                    {
                        out.entry(url.clone()).or_default().file = Some(file_internal);
                    }
                }
            }
        }

        out
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SourceUrlFileStatus;
    use std::collections::HashSet;
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
    async fn process_scraper_creates_namespace_partition_without_ddl_error() {
        use shared_types::{
            FileManager, GenericNamespaceObj, PluginTag, ScraperDataReturn, Tag, TagType,
        };

        let db = new_test_db().await;

        let conn = db.connect().unwrap();
        let storage_id = db
            .file_storage_location_get_or_create(&conn, "test_storage")
            .await
            .unwrap();
        drop(conn);

        let file = FileManager {
            internal: FileInternal {
                id: None,
                hash: "abc123hash".into(),
                extension: "jpg".into(),
                storage_id,
                size_bytes: Some(42),
            },
            identifying_hashes: vec![],
        };

        let tag_action = FileTagAction {
            operation: TagOperation::Add,
            tags: vec![PluginTag {
                tag: Tag {
                    name: "floofy".into(),
                    // Fresh namespace: creating it must run CREATE TABLE for
                    // its Relationship_{id} partition, which requires an
                    // exclusive transaction.
                    namespace: GenericNamespaceObj {
                        name: "fresh_brand_new_ns".into(),
                        description: None,
                    },
                },
                tag_type: TagType::NormalNoRegex,
                relates_to: None,
            }],
        };

        let mut map: HashMap<FileManager, Vec<FileTagAction>> = HashMap::new();
        map.insert(file, vec![tag_action]);

        let persisted = db
            .clone()
            .process_scraper(map, Vec::<ScraperDataReturn>::new(), "test".into())
            .await;
        assert!(persisted, "scraper should report a successful persist");

        let conn = db.connect().unwrap();
        let mut rows = conn
            .query(
                "SELECT id FROM Namespace WHERE name = 'fresh_brand_new_ns';",
                (),
            )
            .await
            .unwrap();
        let Some(row) = rows.next().await.unwrap() else {
            panic!("namespace was never created");
        };
        let ns_id: u64 = row.get(0).unwrap();
        drop(rows);

        let partition = format!("Relationship_{ns_id}");
        let mut rows = conn
            .query(
                "SELECT name FROM sqlite_schema WHERE type = 'table' AND LOWER(name) = ?1;",
                (partition.to_ascii_lowercase().as_str(),),
            )
            .await
            .unwrap();
        let partition_created = rows.next().await.unwrap().is_some();
        drop(rows);

        assert!(
            partition_created,
            "namespace partition table was never created"
        );

        let mut rows = conn
            .query(&format!("SELECT COUNT(*) FROM {partition};"), ())
            .await
            .unwrap();
        let rel_count: u64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert!(rel_count > 0, "expected a file/tag relationship row");

        let mut rows = conn
            .query(&format!("SELECT COUNT(*) FROM {partition};"), ())
            .await
            .unwrap();
        let rel_count: u64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert!(rel_count > 0, "expected a file/tag relationship row");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_url_files_get_only_reports_dead_or_filebearing_urls() {
        let db = new_test_db().await;
        let dead_url = "https://static1.e6ai.net/data/dead/dead.jpg".to_string();
        let unknown_url = "https://static1.e6ai.net/data/4e/e9/4ee9f04f.png".to_string();
        db.dead_url_add_async(dead_url.clone()).await;

        let statuses = db
            .clone()
            .source_url_files_get(HashSet::from([dead_url.clone(), unknown_url.clone()]))
            .await;

        assert_eq!(
            statuses.get(&dead_url),
            Some(&SourceUrlFileStatus {
                file: None,
                dead: true,
            })
        );
        assert!(
            !statuses.contains_key(&unknown_url),
            "non-dead URL with no associated file must be omitted so the scraper downloads it"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopening_existing_db_with_fts_index_does_not_crash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("reopen.db");
        let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        db.shutdown().await;
        drop(db);

        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        assert!(
            db.setting_get_sync_blocking("SYSTEM_VERSION").is_some(),
            "expected settings to survive a reopen"
        );
        db.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fts_batch_twice_then_reopen_does_not_fail() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("twice.db");
        let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        let conn = db.connect().unwrap();
        let batch = "DROP INDEX IF EXISTS idx_tags_fts;\nCREATE INDEX IF NOT EXISTS idx_tags_fts ON Tags USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);\nOPTIMIZE INDEX idx_tags_fts;";
        conn.execute_batch(batch).await.unwrap();
        conn.execute_batch(batch).await.unwrap();
        drop(conn);
        db.shutdown().await;
        drop(db);

        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        assert!(db.setting_get_sync_blocking("SYSTEM_VERSION").is_some());
        db.shutdown().await;
    }
}
