//! Turso-native processing for scraper results: persistence of files, tags,
//! relationships, and pending plugin jobs.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use shared_types::{
    FileInternal, FileManager, FileTagAction, GenericNamespaceObj, ScraperDataReturn, Tag,
    TagOperation,
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
            let mut attempts = 0u32;
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
                            "Concurrent scrape-job commit conflicted; retrying: {error}"
                        );
                        let _ = conn.execute("ROLLBACK", ()).await;
                        if scraper_backoff(&mut attempts).await {
                            log::error!(
                                "Concurrent scrape-job commit still conflicted after \
                                 {SCRAPER_MAX_RETRIES} retries; giving up"
                            );
                            return false;
                        }
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

        // The scraper result is persisted through three small, idempotent
        // (`INSERT OR IGNORE`) `BEGIN CONCURRENT` transactions — files, tags,
        // then relationships. Each phase retries its own write-write
        // conflicts with jittered backoff, so a contention spike on a shared
        // popular tag only restarts the relationship phase instead of the
        // whole chunk.
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

    /// Persists the whole remaining scraper result through three small
    /// `BEGIN CONCURRENT` transactions — files, tags, then relationships —
    /// instead of one giant MVCC transaction. Every bulk write here is
    /// idempotent (`INSERT OR IGNORE`), and an MVCC snapshot from a
    /// conflicted transaction is stale, so the only way to make progress is
    /// to roll back and re-run in a fresh transaction.
    ///
    /// Splitting matters because the relationship phase is where contention
    /// concentrates: every new relationship also bumps the shared
    /// `Tags.count` row, so two scraper chunks that both touch a popular tag
    /// collide exactly there. With one giant transaction that collision
    /// rolled back the file and tag inserts too; now only the conflicted
    /// phase retries, and each phase's smaller write set overlaps concurrent
    /// writers far less. Retries use jittered exponential backoff (see
    /// `scraper_backoff`) and are capped so a pathological contention storm
    /// cannot spin on the shared tokio runtime forever.
    async fn process_scraper_chunk_human(
        &self,
        map: HashMap<FileManager, Vec<FileTagAction>>,
    ) -> Result<bool> {
        // Early Exit
        if map.is_empty() {
            return Ok(true);
        }

        // Pure-Rust prep, computed once outside the retry loops.
        let all_tags: Vec<FileTagAction> = map.values().flatten().cloned().collect();

        let unique_files: HashSet<FileInternal> = map.keys().map(|f| f.internal.clone()).collect();
        let file_list: Vec<FileInternal> = unique_files.into_iter().collect();

        // Phase 1: files + identifying hashes, in their own transaction.
        let file_cache = self.scraper_phase_files(&map, &file_list).await?;

        // Phase 2: tags + parent relations, in their own transaction. The
        // returned id mapping is what the relationship phase resolves against.
        let tag_id_mapping = self.scraper_phase_tags(&all_tags).await?;

        // Phase 3: relationships, computed against one bulk read of the
        // current file/tag state instead of per-file queries.
        self.scraper_phase_relationships(&map, &file_cache, &tag_id_mapping)
            .await?;

        Ok(true)
    }

    /// Phase 1 of a scraper chunk: persists files and their identifying
    /// hashes in one small concurrent transaction, returning the
    /// `hash -> db id` cache the relationship phase resolves against.
    async fn scraper_phase_files(
        &self,
        map: &HashMap<FileManager, Vec<FileTagAction>>,
        file_list: &[FileInternal],
    ) -> Result<HashMap<String, u64>> {
        let mut attempts = 0u32;
        loop {
            // Cant connect to db?
            let mut conn = self.connect()?;
            let tn = loop {
                match conn
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Concurrent)
                    .await
                {
                    Ok(tn) => break tn,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        log::warn!("Scraper begin conflicted; retrying: {error}");
                        if scraper_backoff(&mut attempts).await {
                            return Err(scraper_give_up("begin"));
                        }
                    }
                    Err(error) => return Err(error),
                }
            };

            let corrected_files = match self.file_add_bulk(&tn, file_list).await {
                Ok(out) => out,
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    let _ = tn.rollback().await;
                    log::warn!("Scraper file insert conflicted; retrying: {error}");
                    if scraper_backoff(&mut attempts).await {
                        return Err(scraper_give_up("file insert"));
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };

            let mut file_cache = HashMap::with_capacity(corrected_files.len());
            for file in &corrected_files {
                if let Some(db_id) = file.id {
                    file_cache.insert(file.hash.clone(), db_id);
                }
            }

            // Identifying hashes.
            let mut file_hashes: Vec<(u64, &str, &str)> = Vec::new();
            for filemanager in map.keys() {
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
                    log::warn!("Scraper hash insert conflicted; retrying: {error}");
                    if scraper_backoff(&mut attempts).await {
                        return Err(scraper_give_up("hash insert"));
                    }
                    continue;
                }
                return Err(error);
            }

            match tn.commit().await {
                Ok(_) => return Ok(file_cache),
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    log::warn!("Scraper file phase commit conflicted; retrying: {error}");
                    if scraper_backoff(&mut attempts).await {
                        return Err(scraper_give_up("file phase commit"));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Phase 2 of a scraper chunk: persists tags (and their parent
    /// relations) in one small concurrent transaction, returning the
    /// `Tag -> id` mapping the relationship phase resolves against.
    async fn scraper_phase_tags(
        &self,
        all_tags: &[FileTagAction],
    ) -> Result<HashMap<Tag, i64>> {
        let mut attempts = 0u32;
        loop {
            // Cant connect to db?
            let mut conn = self.connect()?;
            let tn = loop {
                match conn
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Concurrent)
                    .await
                {
                    Ok(tn) => break tn,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        log::warn!("Scraper begin conflicted; retrying: {error}");
                        if scraper_backoff(&mut attempts).await {
                            return Err(scraper_give_up("begin"));
                        }
                    }
                    Err(error) => return Err(error),
                }
            };

            let tag_id_mapping = match self.tag_action_bulk_add(&tn, all_tags).await {
                Ok(mapping) => mapping,
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    let _ = tn.rollback().await;
                    log::warn!("Scraper tag insert conflicted; retrying: {error}");
                    if scraper_backoff(&mut attempts).await {
                        return Err(scraper_give_up("tag insert"));
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };

            match tn.commit().await {
                Ok(_) => return Ok(tag_id_mapping),
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    log::warn!("Scraper tag phase commit conflicted; retrying: {error}");
                    if scraper_backoff(&mut attempts).await {
                        return Err(scraper_give_up("tag phase commit"));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Phase 3 of a scraper chunk: reads the current file/tag relationship
    /// state in one bulk pass, computes the adds/deletes each `TagOperation`
    /// implies (including `Set` deletions evaluated against each file's
    /// *full* current state), and applies them. This is the phase where
    /// write-write conflicts concentrate — every new relationship also bumps
    /// the shared `Tags.count` row — so it retries on its own with jittered
    /// backoff instead of redoing the file/tag phases.
    async fn scraper_phase_relationships(
        &self,
        map: &HashMap<FileManager, Vec<FileTagAction>>,
        file_cache: &HashMap<String, u64>,
        tag_id_mapping: &HashMap<Tag, i64>,
    ) -> Result<()> {
        let mut attempts = 0u32;
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
                        log::warn!("Scraper begin conflicted; retrying: {error}");
                        if scraper_backoff(&mut attempts).await {
                            return Err(scraper_give_up("begin"));
                        }
                    }
                    Err(error) => return Err(error),
                }
            };

            let file_ids: Vec<u64> = file_cache.values().copied().collect();
            let current_file_relationships =
                match self.file_id_get_tag_ids_bulk(&tn, &file_ids).await {
                    Ok(rels) => rels,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        let _ = tn.rollback().await;
                        log::warn!("Scraper relationship read conflicted; retrying: {error}");
                        if scraper_backoff(&mut attempts).await {
                            return Err(scraper_give_up("relationship read"));
                        }
                        continue;
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
            for (tag_obj, &tag_id) in tag_id_mapping {
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
                        log::warn!(
                            "Scraper tag namespace read conflicted; retrying: {error}"
                        );
                        if scraper_backoff(&mut attempts).await {
                            return Err(scraper_give_up("tag namespace read"));
                        }
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

            for (file_manager, tag_list) in map {
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

            // Bulk add/delete return the per-tag count deltas instead of
            // bumping the shared `Tags.count` row inside this concurrent
            // transaction; the deltas are folded in after commit through the
            // serialized `tag_counts_apply` writer.
            let mut del_deltas = HashMap::new();
            if !rels_to_del.is_empty() {
                match self.relationship_bulk_delete(&tn, &rels_to_del).await {
                    Ok(deltas) => del_deltas = deltas,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        let _ = tn.rollback().await;
                        log::warn!("Scraper relationship delete conflicted; retrying: {error}");
                        if scraper_backoff(&mut attempts).await {
                            return Err(scraper_give_up("relationship delete"));
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }

            let mut add_deltas = HashMap::new();
            if !rels_to_add.is_empty() {
                match self.relationships_bulk_add(&tn, &rels_to_add).await {
                    Ok(deltas) => add_deltas = deltas,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        let _ = tn.rollback().await;
                        log::warn!("Scraper relationship add conflicted; retrying: {error}");
                        if scraper_backoff(&mut attempts).await {
                            return Err(scraper_give_up("relationship add"));
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }

            match tn.commit().await {
                Ok(_) => {
                    // Relationship rows are committed; apply the deferred count
                    // deltas through the single serialized writer so the shared
                    // `Tags.count` row is never part of two concurrent write
                    // sets. A failure here only leaves a stale count (healed by
                    // the next slurp recount), never lost rows, so it must not
                    // fail the whole chunk.
                    if let Err(error) = self.tag_counts_apply(&add_deltas, &del_deltas).await {
                        log::error!("Failed to apply deferred tag counts: {error}");
                    }
                    return Ok(());
                }
                Err(error) if Self::is_concurrency_conflict(&error) => {
                    log::warn!(
                        "Scraper relationship phase commit conflicted; retrying: {error}"
                    );
                    if scraper_backoff(&mut attempts).await {
                        return Err(scraper_give_up("relationship phase commit"));
                    }
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

/// Ceiling on how many times a scraper chunk phase retries a conflicting
/// MVCC transaction before giving up. The conflict is expected to clear after
/// a few jittered attempts; past this, it is cheaper to let the caller keep
/// the job for the next boot than to keep spinning on the shared tokio
/// runtime (the same rationale as `retry_mvcc`'s cap).
const SCRAPER_MAX_RETRIES: u32 = 32;

/// Sleeps for a jittered, exponentially growing delay between MVCC retries
/// and reports whether the retry budget is exhausted. The old fixed 50ms
/// sleep synchronized every conflicted writer: they all woke at the same
/// instant, instantly re-collided, and the herd repeated forever. Jitter
/// spreads retries across the interval so each round has only a subset of
/// the writers competing and at least one transaction makes progress.
async fn scraper_backoff(attempt: &mut u32) -> bool {
    let current = *attempt;
    *attempt += 1;
    // 25ms -> 50 -> 100 -> 200 -> 400 -> 800ms (capped).
    let base_ms = 25u64 << current.min(5);
    // Half to full base, randomized, so simultaneous retriers land apart.
    let jittered = base_ms / 2 + rand::random::<u64>() % (base_ms / 2 + 1);
    tokio::time::sleep(Duration::from_millis(jittered)).await;
    *attempt >= SCRAPER_MAX_RETRIES
}

/// Builds the error surfaced once a scraper phase exhausts its retry budget.
fn scraper_give_up(what: &str) -> turso::Error {
    turso::Error::Error(format!(
        "scraper {what} still conflicted after {SCRAPER_MAX_RETRIES} MVCC retries"
    ))
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
        // Full rebuild of the popular-tag FTS shadow: drop, refill from
        // `count >= 5`, rebuild the ngram index, optimize.
        let batch = "DROP TABLE IF EXISTS Tags_Popular;\nCREATE TABLE Tags_Popular (tag_id INTEGER PRIMARY KEY, name TEXT NOT NULL);\nINSERT INTO Tags_Popular(tag_id, name) SELECT id, name FROM Tags WHERE count >= 5;\nCREATE INDEX idx_tags_fts ON Tags_Popular USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);\nOPTIMIZE INDEX idx_tags_fts;";
        conn.execute_batch(batch).await.unwrap();
        conn.execute_batch(batch).await.unwrap();
        drop(conn);
        db.shutdown().await;
        drop(db);

        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        assert!(db.setting_get_sync_blocking("SYSTEM_VERSION").is_some());
        db.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upgrading_old_fts_schema_moves_index_onto_popular_shadow() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("upgrade.db");
        let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Build a pre-shadow database: FTS index on Tags itself, no
        // Tags_Popular, one popular tag (count 6) and one unpopular (count 1).
        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        {
            let conn = db.connect().unwrap();
            conn.execute("DROP INDEX idx_tags_fts", ()).await.unwrap();
            conn.execute("DROP TABLE Tags_Popular", ()).await.unwrap();
            conn.execute(
                "CREATE INDEX idx_tags_fts ON Tags USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO Namespace (name, description) VALUES ('subject', NULL);",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO Tags (name, namespace, count) VALUES ('wantable', 1, 6), ('shy', 1, 1);",
                (),
            )
            .await
            .unwrap();
        }
        db.shutdown().await;
        drop(db);

        // Reopen: check_db must create the shadow, copy the count >= 5 tag,
        // drop the old Tags-level index and re-point it at Tags_Popular.
        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        let conn = db.connect().unwrap();
        let shadow_names: Vec<String> = {
            let mut rows = conn
                .query("SELECT name FROM Tags_Popular ORDER BY tag_id;", ())
                .await
                .unwrap();
            let mut names = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                names.push(row.get::<String>(0).unwrap());
            }
            names
        };
        assert_eq!(
            shadow_names,
            vec!["wantable".to_string()],
            "only the count >= 5 tag is mirrored on upgrade"
        );
        let index_targets: Vec<String> = {
            let mut rows = conn
                .query(
                    "SELECT LOWER(tbl_name) FROM sqlite_schema
                     WHERE type = 'index' AND LOWER(name) = 'idx_tags_fts';",
                    (),
                )
                .await
                .unwrap();
            let mut targets = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                targets.push(row.get::<String>(0).unwrap());
            }
            targets
        };
        assert_eq!(
            index_targets,
            vec!["tags_popular".to_string()],
            "the FTS index must live on the shadow after upgrade"
        );
        let found = db.tags_search_fts("want", 10).await.unwrap();
        assert_eq!(found.len(), 1, "upgraded db must search popular tags");
        let shy = db.tags_search_fts("shy", 10).await.unwrap();
        assert!(shy.is_empty(), "unpopular tag must stay unsearchable");
        db.shutdown().await;
    }
}
