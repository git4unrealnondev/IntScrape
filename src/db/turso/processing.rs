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

        // The whole scraper result is persisted as one `BEGIN CONCURRENT`
        // transaction. Chunking is gone: every bulk write below is idempotent
        // (`INSERT OR IGNORE`) and write-write conflicts are retried
        // indefinitely, so a transaction of any size converges once contention
        // clears.
        if !database
            .process_scraper_chunk_human(map)
            .await
            .unwrap_or_else(|error| {
                log::error!("Failed to process scraper: {error}");
                false
            })
        {
            return false;
        }
        true
    }

    /// Persists the whole remaining scraper result inside its own
    /// `BEGIN CONCURRENT` transaction, restarting the transaction from
    /// scratch on any concurrency conflict. Every bulk write here is
    /// idempotent (`INSERT OR IGNORE`), and an MVCC snapshot from a
    /// conflicted transaction is stale, so the only way to make progress is
    /// to roll back and re-run in a fresh transaction. Retries are unbounded.
    async fn process_scraper_chunk_human(
        &self,
        map: HashMap<FileManager, Vec<FileTagAction>>,
    ) -> Result<bool> {
        // Early Exit
        if map.is_empty() {
            return Ok(true);
        }

        // Pure-Rust prep, computed once outside the retry loop.
        let all_tags: Vec<FileTagAction> = map.values().flatten().cloned().collect();

        let unique_files: HashSet<FileInternal> = map.keys().map(|f| f.internal.clone()).collect();
        let file_list: Vec<FileInternal> = unique_files.into_iter().collect();

        'retry: loop {
            // Cant connect to db?
            let mut conn = self.connect()?;

            let tn = loop {
                match conn
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Concurrent)
                    .await
                {
                    Ok(tn) => break tn,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        log::warn!("Scraper begin conflicted; retrying in 50ms: {error}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(error) => return Err(error),
                }
            };

            // Phase 1: files.
            let corrected_files = match self.file_add_bulk(&tn, &file_list).await {
                Ok(out) => out,
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    let _ = tn.rollback().await;
                    log::warn!("Scraper file insert conflicted; retrying in 50ms: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'retry;
                }
                Err(error) => return Err(error),
            };

            let mut file_cache = HashMap::with_capacity(corrected_files.len());
            for file in &corrected_files {
                if let Some(db_id) = file.id {
                    file_cache.insert(file.hash.clone(), db_id);
                }
            }

            // Phase 1b: identifying hashes.
            let mut file_hashes: Vec<(u64, &str, &str)> = Vec::new();
            for (filemanager, _) in &map {
                let Some(file_id) = file_cache.get(&filemanager.internal.hash) else {
                    continue;
                };
                for file_hash in &filemanager.identifying_hashes {
                    let (algorithm, digest) = crate::db::hashessupportedtoinner(file_hash);
                    file_hashes.push((*file_id, algorithm, digest.as_str()));
                }
            }
            if !file_hashes.is_empty()
                && let Err(error) = self.file_hashes_add_bulk(&tn, &file_hashes).await
            {
                if Self::is_concurrency_conflict(&error) {
                    let _ = tn.rollback().await;
                    log::warn!("Scraper hash insert conflicted; retrying in 50ms: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'retry;
                }
                return Err(error);
            }

            // Phase 2: tags + parent relations. The returned id mapping is
            // what the relationship phase resolves against.
            let tag_id_mapping = match self.tag_action_bulk_add(&tn, &all_tags).await {
                Ok(mapping) => mapping,
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    let _ = tn.rollback().await;
                    log::warn!("Scraper tag insert conflicted; retrying in 50ms: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'retry;
                }
                Err(error) => return Err(error),
            };

            // Phase 3: relationships, computed against one bulk read of the
            // current file/tag state instead of per-file queries.
            let file_ids: Vec<u64> = file_cache.values().copied().collect();
            let current_file_relationships =
                match self.file_id_get_tag_ids_bulk(&tn, &file_ids).await {
                    Ok(rels) => rels,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        let _ = tn.rollback().await;
                        log::warn!("Scraper relationship read conflicted; retrying in 50ms: {error}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue 'retry;
                    }
                    Err(error) => return Err(error),
                };

            // Resolve the namespace of every current tag id: chunk tags come
            // from the mapping above, and pre-existing current tags (ones this
            // chunk does not touch) get their namespace resolved in one bulk
            // query, so a Set evaluates deletions against the file's *full*
            // current state instead of only the tags this chunk happens to
            // reference.
            let mut tag_id_to_ns_name: HashMap<u64, String> =
                HashMap::with_capacity(tag_id_mapping.len());
            for (tag_obj, &tag_id) in &tag_id_mapping {
                tag_id_to_ns_name.insert(tag_id as u64, tag_obj.namespace.name.to_string());
            }
            let mut missing: HashSet<u64> = HashSet::new();
            for current_tag_ids in current_file_relationships.values() {
                for &tag_id in current_tag_ids {
                    if !tag_id_to_ns_name.contains_key(&tag_id) {
                        missing.insert(tag_id);
                    }
                }
            }
            for missing in missing
                .into_iter()
                .collect::<Vec<_>>()
                .chunks(crate::db::SQL_CHUNK_SIZE)
            {
                let placeholders = std::iter::repeat_n("?", missing.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "SELECT t.id, n.name FROM Tags t JOIN Namespace n ON n.id = t.namespace \
                     WHERE t.id IN ({placeholders});"
                );
                let params: Vec<Value> =
                    missing.iter().map(|id| Value::from(*id as i64)).collect();
                let mut rows = match tn.query(&sql, params_from_iter(params)).await {
                    Ok(rows) => rows,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        let _ = tn.rollback().await;
                        log::warn!("Scraper tag namespace read conflicted; retrying in 50ms: {error}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue 'retry;
                    }
                    Err(error) => return Err(error),
                };
                while let Some(row) = rows.next().await? {
                    tag_id_to_ns_name.insert(row.get(0)?, row.get(1)?);
                }
            }

            let mut rels_to_add = HashSet::new();
            let mut rels_to_del = HashSet::new();
            let mut current_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
            let mut incoming_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
            let mut explicit_adds = HashSet::new();
            let mut set_deletions = HashSet::new();

            for (file_manager, tag_list) in &map {
                let file_id = match file_cache.get(&file_manager.internal.hash) {
                    Some(&id) => id,
                    None => continue,
                };

                current_ns_tags.clear();
                explicit_adds.clear();
                set_deletions.clear();

                // Current database state for this file: Namespace -> tag ids.
                if let Some(current_tag_ids) = current_file_relationships.get(&file_id) {
                    for &tag_id in current_tag_ids {
                        if let Some(ns_name) = tag_id_to_ns_name.get(&tag_id) {
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
                                if let Some(&tag_id) = tag_id_mapping.get(&tag.tag) {
                                    rels_to_add.insert((file_id, tag_id as u64));
                                    explicit_adds.insert(tag_id as u64);
                                }
                            }
                        }
                        TagOperation::Del => {
                            for tag in &tag_action.tags {
                                if let Some(&tag_id) = tag_id_mapping.get(&tag.tag) {
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
                                if let Some(&tag_id) = tag_id_mapping.get(&tag.tag) {
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
                                    for &current_tag_id in current_tag_ids {
                                        if !incoming_set.contains(&current_tag_id) {
                                            set_deletions.insert((file_id, current_tag_id));
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
                && let Err(error) = self.relationship_bulk_delete(&tn, &rels_to_del).await
            {
                if Self::is_concurrency_conflict(&error) {
                    let _ = tn.rollback().await;
                    log::warn!("Scraper relationship delete conflicted; retrying in 50ms: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'retry;
                }
                return Err(error);
            }

            if !rels_to_add.is_empty()
                && let Err(error) = self.relationships_bulk_add(&tn, &rels_to_add).await
            {
                if Self::is_concurrency_conflict(&error) {
                    let _ = tn.rollback().await;
                    log::warn!("Scraper relationship add conflicted; retrying in 50ms: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue 'retry;
                }
                return Err(error);
            }

            match tn.commit().await {
                Ok(_) => return Ok(true),
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    log::warn!("Scraper chunk commit conflicted; retrying in 50ms: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
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
