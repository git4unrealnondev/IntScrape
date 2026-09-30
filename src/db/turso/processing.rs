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
                if let Err(error) = conn.execute("BEGIN IMMEDIATE", ()).await {
                    log::error!("Failed to begin scrape-job transaction: {error}");
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
                        log::warn!("Scrape-job commit conflicted; retrying: {error}");
                        let _ = conn.execute("ROLLBACK", ()).await;
                        if scraper_backoff(&mut attempts).await {
                            log::error!(
                                "Scrape-job commit still conflicted after \
                                 {SCRAPER_MAX_RETRIES} retries; giving up"
                            );
                            return false;
                        }
                    }
                    Err(error) => {
                        log::error!("Failed to commit scrape-job transaction: {error}");
                        return false;
                    }
                }
            }
        }

        // Namespace rows + Relationship_{id} partitions are created with DDL,
        // which turso only permits inside an exclusive transaction. Ensure
        // every namespace this scrape references exists (and is cached) up
        // front so the chunk transactions below are DML-only. The ensure is a
        // fast no-op once everything is cached.
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
        // (`INSERT OR IGNORE`) plain-BEGIN transactions — files, tags, then
        // relationships. Each phase begins Deferred, so its reads run without
        // a write lock; the first write upgrades the transaction to the
        // single writer, and a conflict at that upgrade point (or at commit)
        // is retried by that phase alone. Each phase retries its own
        // write-write conflicts with jittered backoff, so a contention spike
        // on a shared popular tag only restarts the relationship phase
        // instead of the whole chunk.
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
    /// plain-BEGIN transactions — files, tags, then relationships — instead
    /// of one giant transaction. Every bulk write here is idempotent
    /// (`INSERT OR IGNORE`), and a snapshot taken before a conflicted write
    /// is stale (the conflict aborts the transaction), so the only way to
    /// make progress is to roll back and re-run in a fresh transaction.
    ///
    /// Splitting matters because the relationship phase is where contention
    /// concentrates: every new relationship also bumps the shared
    /// `Tags.count` row, so two scraper chunks that both touch a popular tag
    /// collide exactly there. With one giant transaction that collision
    /// rolled back the file and tag inserts too; now only the conflicted
    /// phase retries, and each phase's smaller write set overlaps other
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
    /// hashes in one small plain-BEGIN transaction, returning the
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
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Deferred)
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
    /// relations) in one small plain-BEGIN transaction, returning the
    /// `Tag -> id` mapping the relationship phase resolves against.
    async fn scraper_phase_tags(&self, all_tags: &[FileTagAction]) -> Result<HashMap<Tag, i64>> {
        let mut attempts = 0u32;
        loop {
            // Cant connect to db?
            let mut conn = self.connect()?;
            let tn = loop {
                match conn
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Deferred)
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
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Deferred)
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
                let params: Vec<Value> = missing.iter().map(|id| Value::from(*id as i64)).collect();
                let mut rows = match tn.query(&sql, params_from_iter(params)).await {
                    Ok(rows) => rows,
                    Err(error) if Self::is_concurrency_conflict(&error) => {
                        let _ = tn.rollback().await;
                        log::warn!("Scraper tag namespace read conflicted; retrying: {error}");
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
                    log::warn!("Scraper relationship phase commit conflicted; retrying: {error}");
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
    use std::time::Instant;

    async fn new_test_db() -> Arc<TursoDatabase> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    }

    #[test]
    #[ignore = "manual benchmark: Parents write path, lean (2-index) vs legacy (4-index) schema"]
    fn parents_add_schema_bench() {
        // The "Adding X parents into db" hot spot: the tags phase of a scraper
        // chunk cold-inserts new parent (tag -> relate -> limit_to) rows via
        // parents_bulk_add. Legacy Parents schemas maintained four indexes per
        // insert (inline UNIQUE autoindex + limit_to + relate_tag_id +
        // null-safe unique); the lean schema keeps only the two that reads
        // need (relate_tag_id + the null-safe unique that also does the
        // dedupe). This bench drives the real scraper_phase_tags path with
        // 5000 brand-new related tags on each schema and compares.
        use shared_types::{PluginTag, RelationContext, TagOperation, TagType};

        let db_stem = if std::path::Path::new("/dev/shm").exists() {
            std::path::PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        }
        .join(format!("intscrape-parents-bench-{}", std::process::id()));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            async fn side(base: &std::path::Path, label: &str, legacy_indexes: bool) {
                let db_path = base.with_file_name(format!(
                    "{}-{label}.db",
                    base.file_name().unwrap().to_string_lossy()
                ));
                let _ = std::fs::remove_file(&db_path);
                let db = TursoDatabase::new_with_exit(
                    &db_path,
                    std::sync::Arc::new(AtomicBool::new(false)),
                )
                .await;

                if legacy_indexes {
                    // Boot creates the lean Parents table; rebuild it to the
                    // legacy 4-index shape (inline UNIQUE + limit_to +
                    // relate_tag_id + null-safe unique) on the still-empty
                    // table so only the index set differs.
                    let conn = db.connect().unwrap();
                    conn.execute_batch(
                        "CREATE TABLE Parents_legacy (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            tag_id INTEGER NOT NULL,
                            relate_tag_id INTEGER NOT NULL,
                            limit_to INTEGER,
                            UNIQUE(tag_id, relate_tag_id, limit_to)
                         );
                         INSERT INTO Parents_legacy (tag_id, relate_tag_id, limit_to)
                             SELECT tag_id, relate_tag_id, limit_to FROM Parents;
                         DROP TABLE Parents;
                         ALTER TABLE Parents_legacy RENAME TO Parents;
                         CREATE INDEX idx_parents_lim ON Parents (limit_to);
                         CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);
                         CREATE UNIQUE INDEX idx_unique_parents_null_safe
                             ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));",
                    )
                    .await
                    .unwrap();
                    drop(conn);
                }

                let ns = GenericNamespaceObj {
                    name: "parentbench".into(),
                    description: None,
                };
                db.namespace_ensure_set(&HashSet::from([ns.clone()]))
                    .await
                    .unwrap();

                // 5000 new tags, each with a parent (50 shared hubs) and a
                // limit tag (25 shared), half with no limit_to at all: the
                // realistic "first scrape of new content" parent mix.
                let mut plugins = Vec::with_capacity(5000);
                for i in 0..5000u64 {
                    let limit_to = if i % 2 == 0 {
                        Some(Tag {
                            name: format!("parentlim{}", i % 25),
                            namespace: ns.clone(),
                        })
                    } else {
                        None
                    };
                    plugins.push(PluginTag {
                        tag: Tag {
                            name: format!("parentchip{i}"),
                            namespace: ns.clone(),
                        },
                        tag_type: TagType::NormalNoRegex,
                        relates_to: Some(RelationContext {
                            tag: Tag {
                                name: format!("parenthub{}", i % 50),
                                namespace: ns.clone(),
                            },
                            tag_type: TagType::NormalNoRegex,
                            limit_to,
                        }),
                    });
                }
                let all_tags = vec![FileTagAction {
                    operation: TagOperation::Add,
                    tags: plugins,
                }];

                let t = Instant::now();
                let mapping = db.scraper_phase_tags(&all_tags).await.unwrap();
                let phase_s = t.elapsed().as_secs_f64();

                let conn = db.connect().unwrap();
                let parents: i64 = {
                    let mut stmt = conn.prepare("SELECT COUNT(*) FROM Parents").await.unwrap();
                    stmt.query_row(()).await.unwrap().get(0).unwrap()
                };
                eprintln!(
                    "PARENTSBENCH {label}: tags phase {phase_s:.2}s, mapped {} tags, {parents} parents rows",
                    mapping.len()
                );
                drop(conn);
                db.shutdown().await;
                let _ = std::fs::remove_file(&db_path);
            }

            side(&db_stem, "legacy", true).await;
            side(&db_stem, "lean", false).await;
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn steady_state_boot_sees_popular_shadow() {
        // Regression: limbo lowercases identifiers in sqlite_master, so a
        // probe comparing `name = 'Tags_Popular'` returns 0 on EVERY boot and
        // silently re-runs the full DROP/rebuild/OPTIMIZE cycle. Fixing the
        // probe is what keeps steady-state boots verify-only.
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("probe.db");
        let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        db.shutdown().await;
        drop(db);
        // Second boot = steady state: the shadow created by the first boot
        // must be visible to the ensure probe.
        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        let conn = db.connect().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master
                     WHERE type = 'table' AND LOWER(name) = 'tags_popular'
                 )",
            )
            .await
            .unwrap();
        let probe: i64 = stmt.query_row(()).await.unwrap().get(0).unwrap();
        assert_eq!(probe, 1, "steady-state boot must see the shadow");
        db.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn steady_state_boot_rebuilds_missing_fts_index() {
        // The state a torn/crashed index rebuild leaves behind: the shadow
        // exists but `idx_tags_fts` is gone. The next boot (steady branch)
        // must recreate the index via IF NOT EXISTS so search works again —
        // this is what repairs databases that were already touched by a
        // broken boot.
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("indexgap.db");
        let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        {
            let conn = db.connect().unwrap();
            conn.execute(
                "INSERT INTO Namespace (name, description) VALUES ('subject', NULL);",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO Tags (name, namespace, count) VALUES ('fancyt', 1, 9);",
                (),
            )
            .await
            .unwrap();
            // Wholesale shadow build (same batch the slurp end-of-run uses),
            // then break only the index, keeping the shadow rows.
            let batch = "DROP TABLE IF EXISTS Tags_Popular;
                 CREATE TABLE Tags_Popular (
                     tag_id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL
                 );
                 INSERT INTO Tags_Popular(tag_id, name)
                     SELECT id, name FROM Tags WHERE count >= 5;
                 CREATE INDEX idx_tags_fts ON Tags_Popular USING fts
                     (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
                 OPTIMIZE INDEX idx_tags_fts;";
            conn.execute_batch(batch).await.unwrap();
            conn.execute("DROP INDEX idx_tags_fts", ()).await.unwrap();
        }
        db.shutdown().await;
        drop(db);

        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        let found = db.tags_search_fts("fancy", 10).await.unwrap();
        assert_eq!(
            found.len(),
            1,
            "reopen must rebuild the missing FTS index so search works"
        );
        db.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn steady_state_boot_rebuilds_old_generation_shadow() {
        // Legacy/generation-mismatched shadow: DB whose FTS segments were
        // written by an older core, simulated by a stale schema marker. The
        // search probe cannot detect this (the read path never needs the
        // identity columns the merge path requires), so the marker must drive
        // a one-time wholesale rebuild on the next boot.
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("gen.db");
        let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        {
            let conn = db.connect().unwrap();
            conn.execute(
                "INSERT INTO Namespace (name, description) VALUES ('subject', NULL);",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO Tags (name, namespace, count) VALUES ('genmark', 1, 12);",
                (),
            )
            .await
            .unwrap();
            // Simulate the post-build state of an older core: shadow + index
            // both fine, but the schema generation marker is stale.
            conn.execute(
                "INSERT OR REPLACE INTO Settings (name, description, num, param)
                 VALUES ('fts_shadow_schema', NULL, NULL, 'ancient');",
                (),
            )
            .await
            .unwrap();
        }
        db.shutdown().await;
        drop(db);

        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        let conn = db.connect().unwrap();
        let mut rows = conn
            .query(
                "SELECT param FROM Settings WHERE name = 'fts_shadow_schema';",
                (),
            )
            .await
            .unwrap();
        let marker: String = rows
            .next()
            .await
            .unwrap()
            .expect("marker must be written after a generation rebuild")
            .get(0)
            .unwrap();
        assert_eq!(
            marker,
            super::super::schema_current::FTS_SHADOW_SCHEMA_GENERATION,
            "stale-generation shadow must be rebuilt and re-marked with the current generation"
        );
        drop(conn);
        let found = db.tags_search_fts("genmark", 10).await.unwrap();
        assert_eq!(found.len(), 1, "rebuilt shadow must serve search");
        db.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn boot_migrates_legacy_parents_index_set() {
        // A database created before the lean Parents schema still carries the
        // redundant idx_parents_lim (and inline UNIQUE autoindex). The boot
        // migration must drop idx_parents_lim in place while leaving dedupe
        // and reads intact on the null-safe unique index.
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("legacy_parents.db");
        let should_exit = Arc::new(AtomicBool::new(false));

        let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
        let conn = db.connect().unwrap();
        // Bake the legacy 4-index Parents schema onto the fresh database.
        conn.execute_batch(
            "CREATE TABLE Parents_legacy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag_id INTEGER NOT NULL,
                relate_tag_id INTEGER NOT NULL,
                limit_to INTEGER,
                UNIQUE(tag_id, relate_tag_id, limit_to)
             );
             INSERT INTO Parents_legacy (tag_id, relate_tag_id, limit_to)
                 SELECT tag_id, relate_tag_id, limit_to FROM Parents;
             DROP TABLE Parents;
             ALTER TABLE Parents_legacy RENAME TO Parents;
             CREATE INDEX idx_parents_lim ON Parents (limit_to);
             CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);
             CREATE UNIQUE INDEX idx_unique_parents_null_safe
                 ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));",
        )
        .await
        .unwrap();
        drop(conn);
        db.shutdown().await;
        drop(db);

        // Reboot: check_db's Parents migration must run.
        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        let conn = db.connect().unwrap();

        let lim_present: i64 = {
            let mut stmt = conn
                .prepare(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'index' AND LOWER(name) = 'idx_parents_lim'
                     )",
                )
                .await
                .unwrap();
            stmt.query_row(()).await.unwrap().get(0).unwrap()
        };
        assert_eq!(lim_present, 0, "boot must drop the legacy idx_parents_lim");

        let uidx_present: i64 = {
            let mut stmt = conn
                .prepare(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'index' AND LOWER(name) = 'idx_unique_parents_null_safe'
                     )",
                )
                .await
                .unwrap();
            stmt.query_row(()).await.unwrap().get(0).unwrap()
        };
        assert_eq!(
            uidx_present, 1,
            "null-safe unique index must survive the migration"
        );

        // Dedupe still works after the migration: the OR IGNORE probes the
        // null-safe index, not the dropped inline UNIQUE.
        let conn2 = db.connect().unwrap();
        let tags = vec![
            shared_types::TagParents {
                tag_id: 1,
                relate_tag_id: 2,
                limit_to: None,
            },
            shared_types::TagParents {
                tag_id: 1,
                relate_tag_id: 2,
                limit_to: Some(3),
            },
        ];
        db.parents_bulk_add(&conn2, &tags).await.unwrap();
        let count: i64 = {
            let mut stmt = conn2.prepare("SELECT COUNT(*) FROM Parents").await.unwrap();
            stmt.query_row(()).await.unwrap().get(0).unwrap()
        };
        assert_eq!(count, 2, "two distinct parents insert");
        db.parents_bulk_add(&conn2, &tags).await.unwrap();
        let count2: i64 = {
            let mut stmt = conn2.prepare("SELECT COUNT(*) FROM Parents").await.unwrap();
            stmt.query_row(()).await.unwrap().get(0).unwrap()
        };
        assert_eq!(
            count2, 2,
            "re-asserting the same parents must dedupe through the null-safe index"
        );
        drop(conn2);
        drop(conn);
        db.shutdown().await;
    }

    #[test]
    #[ignore = "manual benchmark: FTS shadow write-path throughput on RAM-backed storage"]
    fn fts_shadow_write_path_throughput_bench() {
        // Reproduces the production hot spot: every scraper chunk that bumps a
        // popular tag runs `sync_tags_popular`, which DELETEs + INSERTs rows
        // into Tags_Popular and makes the tantivy FTS writer maintain the
        // ngram index. Prod runs the DB on tmpfs ("ram"), so this bench runs
        // on /dev/shm too — any slowness is then CPU (segment loads / identity
        // reads / merges), not disk.
        use std::time::Instant;

        let db_path = if std::path::Path::new("/dev/shm").exists() {
            std::path::PathBuf::from("/dev/shm")
                .join(format!("intscrape-fts-bench-{}.db", std::process::id()))
        } else {
            std::env::temp_dir().join(format!("intscrape-fts-bench-{}.db", std::process::id()))
        };
        let _ = std::fs::remove_file(&db_path);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
            let conn = db.connect().unwrap();
            conn.execute(
                "INSERT INTO Namespace (name, description) VALUES ('bench', NULL);",
                (),
            )
            .await
            .unwrap();

            // Seed popular tags (count >= 5) in flat row chunks.
            let popular: u64 = 20_000;
            let (seed_s, build_s) = bench_seed_db(&db, popular).await;
            let shadow_rows = bench_shadow_rows(&db).await;

            // The write path: R rounds, each touching B popular tags through
            // the real sync_tags_popular (DELETE + INSERT OR IGNORE), with
            // counts bumped beforehand exactly like relationship_add does.
            let rounds: u32 = 20;
            let per_round: u64 = 50;
            let total = Instant::now();
            let mut touched = Vec::new();
            for r in 0..rounds {
                let start = (r as u64 * per_round) % popular + 1;
                touched.clear();
                for id in start..start + per_round {
                    touched.push((id - 1) % popular + 1);
                }
                let now = Instant::now();
                db.sync_tags_popular(&conn, &touched).await.unwrap();
                eprintln!(
                    "round {r}: {per_round} shadow writes took {:.3?} ({} rows in shadow)",
                    now.elapsed(),
                    shadow_rows
                );
            }
            let writes = rounds as u64 * per_round;
            let total_s = total.elapsed().as_secs_f64();
            let per_write_ms = total_s * 1000.0 / writes as f64;

            eprintln!(
                "FTS bench (tmpfs): seed {popular} tags {seed_s:.2}s, shadow build+optimize {build_s:.2}s, {writes} shadow writes {total_s:.2}s ({per_write_ms:.3}ms/write), shadow rows {shadow_rows}"
            );
        });
        drop(db_path);
    }

    #[test]
    #[ignore = "manual benchmark: end-to-end scraper chunk persist on RAM-backed storage"]
    fn scraper_chunk_persist_throughput_bench() {
        use shared_types::{PluginTag, TagType};

        let db_path = if std::path::Path::new("/dev/shm").exists() {
            std::path::PathBuf::from("/dev/shm")
                .join(format!("intscrape-chunk-bench-{}.db", std::process::id()))
        } else {
            std::env::temp_dir().join(format!("intscrape-chunk-bench-{}.db", std::process::id()))
        };
        let _ = std::fs::remove_file(&db_path);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
            let (seed_s, build_s) = bench_seed_db(&db, 10_000).await;
            let popular_count = 10_000u64;

            let conn = db.connect().unwrap();
            let storage_id = db
                .file_storage_location_get_or_create(&conn, "bench_storage")
                .await
                .unwrap();
            drop(conn);

            let ns = GenericNamespaceObj {
                name: "bench".into(),
                description: None,
            };
            // 200 files x 24 tags (20 popular + 4 brand-new) each.
            let (map, file_list, all_tags) = bench_chunk_map(storage_id, popular_count, 200, 0, &ns);

            // Fresh chunk (all 200 files new).
            let entries = map.len();
            let t = Instant::now();
            let persisted = db
                .clone()
                .process_scraper(map.clone(), Vec::new(), "bench".into())
                .await;
            let fresh_s = t.elapsed().as_secs_f64();
            eprintln!("CHUNK fresh {entries} files persisted={persisted} in {fresh_s:.2}s");

            // Steady re-persist: same map again (idempotent bulk writes).
            let t = Instant::now();
            let persisted = db
                .clone()
                .process_scraper(map.clone(), Vec::new(), "bench".into())
                .await;
            let reap_s = t.elapsed().as_secs_f64();
            eprintln!("CHUNK re-persist {entries} files persisted={persisted} in {reap_s:.2}s");

            // Contention: two fresh, disjoint chunks (files 200..399 and
            // 400..599, so neither is persisted yet) over the same 10k
            // popular tags, in parallel. WAL allows a single writer: the
            // loser's first write busies and its phase rolls back, backs off
            // with jitter, and retries. Wall time ~= twice the single-chunk
            // cost (writes serialize); reads overlap but are a small share.
            let (map_a, _, _) = bench_chunk_map(storage_id, popular_count, 200, 200, &ns);
            let (map_b, _, _) = bench_chunk_map(storage_id, popular_count, 200, 400, &ns);
            let t = Instant::now();
            let pa = db.clone().process_scraper(map_a.clone(), Vec::new(), "a".into());
            let pb = db.clone().process_scraper(map_b.clone(), Vec::new(), "b".into());
            let (ra, rb) = tokio::join!(pa, pb);
            let both_s = t.elapsed().as_secs_f64();
            eprintln!(
                "CHUNK concurrent x2 fresh-disjoint persisted=({ra},{rb}) in {both_s:.2}s (2x fresh = {:.2}s)",
                fresh_s * 2.0
            );

            // Prod-scale: a 1000-file fresh chunk (24k relationship rows, 14k
            // count deltas). This is the regime that produced the 73-93s gaps
            // in prod logs: the old unbounded count UPDATE was ~quadratic in
            // statement size (a 24k-clause CASE tree is ~2 minutes of planner
            // cost); the folded path should be ~linear in real work.
            //
            // Replicate process_scraper's exact steps with per-step timing so
            // the end-to-end cost is attributable.
            let (big_map, _, _) = bench_chunk_map(storage_id, popular_count, 1000, 1000, &ns);
            let big_entries = big_map.len();
            let all_tags_big: Vec<FileTagAction> =
                big_map.values().flatten().cloned().collect();
            let file_list_big: Vec<FileInternal> =
                big_map.keys().map(|f| f.internal.clone()).collect();
            let mut ns_set = HashSet::new();
            ns_set.insert(ns.clone());
            let t = Instant::now();
            db.namespace_ensure_set(&ns_set).await.unwrap();
            let ns_s = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let fc = db.scraper_phase_files(&big_map, &file_list_big).await.unwrap();
            let files_s = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let mapping_big = db.scraper_phase_tags(&all_tags_big).await.unwrap();
            let tags_s = t.elapsed().as_secs_f64();
            let t = Instant::now();
            db.scraper_phase_relationships(&big_map, &fc, &mapping_big)
                .await
                .unwrap();
            let rels_s = t.elapsed().as_secs_f64();
            let big_s = ns_s + files_s + tags_s + rels_s;
            eprintln!("CHUNK big {big_entries} files replicated: ns {ns_s:.2}s + files {files_s:.2}s + tags {tags_s:.2}s + relationships {rels_s:.2}s = {big_s:.2}s");

            eprintln!(
                "CHUNK summary: seed {seed_s:.2}s build {build_s:.2}s | fresh {fresh_s:.2}s | re-persist {reap_s:.2}s | concurrent x2 {both_s:.2}s | big 1000-file {big_s:.2}s"
            );
        });
        drop(db_path);
    }

    /// Seeds `popular` count>=5 tags and builds the FTS shadow wholesale (the
    /// same batch slurp's end-of-run uses). Returns (seed_s, build_s).
    async fn bench_seed_db(db: &TursoDatabase, popular: u64) -> (f64, f64) {
        let conn = db.connect().unwrap();
        let seed_start = Instant::now();
        for chunk in (1u64..=popular).collect::<Vec<_>>().chunks(256) {
            let mut sql =
                String::from("INSERT OR REPLACE INTO Tags (name, namespace, count) VALUES ");
            let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 3);
            for (i, id) in chunk.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                let base = (i * 3) + 1;
                sql.push_str(&format!("(?{base}, ?{}, ?{})", base + 1, base + 2));
                params.push(Value::from(format!("benchtag{id}")));
                params.push(Value::from(1i64));
                params.push(Value::from(9i64));
            }
            sql.push(';');
            conn.execute(&sql, params_from_iter(params)).await.unwrap();
        }
        let seed_s = seed_start.elapsed().as_secs_f64();

        let build_start = Instant::now();
        conn.execute_batch(
            "DROP TABLE IF EXISTS Tags_Popular;
             CREATE TABLE Tags_Popular (
                 tag_id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL
             );
             INSERT INTO Tags_Popular(tag_id, name)
                 SELECT id, name FROM Tags WHERE count >= 5;
             CREATE INDEX idx_tags_fts ON Tags_Popular USING fts
                 (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
             OPTIMIZE INDEX idx_tags_fts;",
        )
        .await
        .unwrap();
        let build_s = build_start.elapsed().as_secs_f64();
        (seed_s, build_s)
    }

    async fn bench_shadow_rows(db: &TursoDatabase) -> i64 {
        let conn = db.connect().unwrap();
        let mut rows = conn
            .query("SELECT COUNT(*) FROM Tags_Popular;", ())
            .await
            .unwrap();
        rows.next()
            .await
            .unwrap()
            .expect("shadow row count")
            .get(0)
            .unwrap()
    }

    /// Builds a scraper chunk: `files` FileManagers (starting at index
    /// `start` so multiple chunks on one db stay disjoint), each with
    /// `per_file_tags` tags cycling [`popular_count`] popular tags plus
    /// brand-new ones. Returns (map, file_list, all_tags) mirroring
    /// process_scraper_chunk_human.
    fn bench_chunk_map(
        storage_id: u64,
        popular_count: u64,
        files: u64,
        start: u64,
        ns: &GenericNamespaceObj,
    ) -> (
        HashMap<FileManager, Vec<FileTagAction>>,
        Vec<FileInternal>,
        Vec<FileTagAction>,
    ) {
        use shared_types::{PluginTag, TagType};
        let mut map: HashMap<FileManager, Vec<FileTagAction>> = HashMap::new();
        for i in 0..files {
            let f = start + i;
            let file = FileManager {
                internal: FileInternal {
                    id: None,
                    hash: format!("benchfilehash{f:016x}"),
                    extension: "png".into(),
                    storage_id,
                    size_bytes: Some(1024),
                },
                identifying_hashes: vec![],
            };
            let mut action_tags = Vec::new();
            for t in 0..20u64 {
                let tag_name = format!("benchtag{}", ((f * 20 + t) % popular_count) + 1);
                action_tags.push(PluginTag {
                    tag: Tag {
                        name: tag_name,
                        namespace: ns.clone(),
                    },
                    tag_type: TagType::NormalNoRegex,
                    relates_to: None,
                });
            }
            for t in 0..4u64 {
                action_tags.push(PluginTag {
                    tag: Tag {
                        name: format!("newtag{f}_{t}"),
                        namespace: ns.clone(),
                    },
                    tag_type: TagType::NormalNoRegex,
                    relates_to: None,
                });
            }
            map.insert(
                file,
                vec![FileTagAction {
                    operation: TagOperation::Add,
                    tags: action_tags,
                }],
            );
        }
        let all_tags: Vec<FileTagAction> = map.values().flatten().cloned().collect();
        let file_list: Vec<FileInternal> = map.keys().map(|f| f.internal.clone()).collect();
        (map, file_list, all_tags)
    }

    #[test]
    #[ignore = "manual benchmark: split phase-3 cost into bulk-add vs count-apply vs shadow"]
    fn scraper_write_path_micro_dissection_bench() {
        let db_path = if std::path::Path::new("/dev/shm").exists() {
            std::path::PathBuf::from("/dev/shm")
                .join(format!("intscrape-micro-bench-{}.db", std::process::id()))
        } else {
            std::env::temp_dir().join(format!("intscrape-micro-bench-{}.db", std::process::id()))
        };
        let _ = std::fs::remove_file(&db_path);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
            let popular_count = 10_000u64;
            let _ = bench_seed_db(&db, popular_count).await;

            let conn = db.connect().unwrap();
            let storage_id = db
                .file_storage_location_get_or_create(&conn, "bench_storage")
                .await
                .unwrap();
            let ns = GenericNamespaceObj {
                name: "bench".into(),
                description: None,
            };
            let mut ns_set = HashSet::new();
            ns_set.insert(ns.clone());
            db.namespace_ensure_set(&ns_set).await.expect("pre-ensure ns");

            let (map, file_list, all_tags) = bench_chunk_map(storage_id, popular_count, 200, 0, &ns);
            let file_cache = db
                .scraper_phase_files(&map, &file_list)
                .await
                .expect("files phase");
            let mapping = db.scraper_phase_tags(&all_tags).await.expect("tags phase");

            // Rebuild the rels_to_add set exactly as scraper_phase_relationships does.
            let mut rels_to_add = HashSet::new();
            for (file, actions) in &map {
                let file_id = file_cache[&file.internal.hash];
                for action in actions {
                    for tag in &action.tags {
                        if let Some(&tag_id) = mapping.get(&tag.tag) {
                            rels_to_add.insert((file_id, tag_id as u64));
                        }
                    }
                }
            }
            let rels_count = rels_to_add.len();
            eprintln!("MICRO rels_to_add {rels_count}");

            // 1) Bulk relationship insert inside a concurrent tx + commit.
            let mut bulk_conn = db.connect().unwrap();
            let tn = bulk_conn
                .transaction_with_behavior(turso::transaction::TransactionBehavior::Deferred)
                .await
                .unwrap();
            let t = Instant::now();
            let add_deltas = db
                .relationships_bulk_add(&tn, &rels_to_add)
                .await
                .unwrap();
            let bulk_s = t.elapsed().as_secs_f64();
            tn.commit().await.unwrap();
            eprintln!(
                "MICRO relationships_bulk_add: {bulk_s:.3}s ({} deltas)",
                add_deltas.len()
            );

            // 2) Full count apply (count UPDATE + shadow sync), the serialized
            //    BEGIN IMMEDIATE path.
            let deltas_count = add_deltas.len();
            let t = Instant::now();
            db.tag_counts_apply(&add_deltas, &HashMap::new())
                .await
                .unwrap();
            let apply_s = t.elapsed().as_secs_f64();
            eprintln!("MICRO tag_counts_apply ({} deltas): {apply_s:.3}s", deltas_count);

            let shadow_conn = db.connect().unwrap();
            let touched: Vec<u64> = add_deltas.keys().copied().collect();

            // 3) Shadow sync alone, one giant batch.
            let t = Instant::now();
            db.sync_tags_popular(&shadow_conn, &touched).await.unwrap();
            let shadow_big_s = t.elapsed().as_secs_f64();
            eprintln!("MICRO sync_tags_popular big-batch ({} ids): {shadow_big_s:.3}s", touched.len());

            // 4) Same shadow sync in 100-id small batches: isolates statement
            //    compile cost from real FTS/index work.
            let t = Instant::now();
            for chunk in touched.chunks(100) {
                db.sync_tags_popular(&shadow_conn, chunk).await.unwrap();
            }
            let shadow_small_s = t.elapsed().as_secs_f64();
            eprintln!(
                "MICRO sync_tags_popular small-batch ({} id, 100/batch): {shadow_small_s:.3}s",
                touched.len()
            );

            // 5) Count-only update, one giant CASE statement vs small batches.
            //    (Mirrors tag_count_update_sql; private upstream, duplicated
            //    here so the bench isolates statement-size compile cost.)
            let count_update_sql = |part: &HashMap<u64, u64>| -> (String, Vec<Value>) {
                let mut clauses = Vec::with_capacity(part.len());
                let mut params = Vec::with_capacity(part.len() * 3);
                for (tag_id, delta) in part {
                    clauses.push("WHEN ? THEN ?".to_string());
                    params.push(Value::from(*tag_id as i64));
                    params.push(Value::from(*delta as i64));
                }
                let placeholders = std::iter::repeat_n("?", part.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                for tag_id in part.keys() {
                    params.push(Value::from(*tag_id as i64));
                }
                (
                    format!(
                        "UPDATE Tags SET count = count + CASE id {} ELSE 0 END WHERE id IN ({placeholders});",
                        clauses.join(" ")
                    ),
                    params,
                )
            };
            let big = count_update_sql(&add_deltas);
            let t = Instant::now();
            shadow_conn
                .execute(big.0, params_from_iter(big.1))
                .await
                .unwrap();
            let update_big_s = t.elapsed().as_secs_f64();
            eprintln!("MICRO count UPDATE giant CASE ({} deltas): {update_big_s:.3}s", deltas_count);

            let t = Instant::now();
            for chunk in add_deltas.iter().collect::<Vec<_>>().chunks(100) {
                let part: HashMap<u64, u64> = chunk
                    .iter()
                    .map(|(k, v)| (**k, **v))
                    .collect();
                let (sql, params) = count_update_sql(&part);
                shadow_conn
                    .execute(sql, params_from_iter(params))
                    .await
                    .unwrap();
            }
            let update_small_s = t.elapsed().as_secs_f64();
            eprintln!("MICRO count UPDATE small-batch: {update_small_s:.3}s");

            // 6) Scaling: a 10k-delta count UPDATE in one giant statement vs
            //    100-id batches. The count-UPDATE path does NOT chunk (the
            //    whole apply delta set is one statement), so this predicts
            //    prod-size chunks.
            let mut tenk: HashMap<u64, u64> = HashMap::new();
            for tag_id in 1..=10_000u64 {
                tenk.insert(tag_id, 1);
            }
            let (big, params) = count_update_sql(&tenk);
            let t = Instant::now();
            shadow_conn.execute(big, params_from_iter(params)).await.unwrap();
            let update_10k_big_s = t.elapsed().as_secs_f64();
            eprintln!("MICRO count UPDATE 10k giant CASE: {update_10k_big_s:.3}s");

            let t = Instant::now();
            for chunk in tenk.iter().collect::<Vec<_>>().chunks(100) {
                let part: HashMap<u64, u64> = chunk
                    .iter()
                    .map(|(k, v)| (**k, **v))
                    .collect();
                let (sql, params) = count_update_sql(&part);
                shadow_conn
                    .execute(sql, params_from_iter(params))
                    .await
                    .unwrap();
            }
            let update_10k_small_s = t.elapsed().as_secs_f64();
            eprintln!("MICRO count UPDATE 10k small-batch: {update_10k_small_s:.3}s");

            eprintln!(
                "MICRO summary: bulk_add {bulk_s:.3}s + apply {apply_s:.3}s | shadow big {shadow_big_s:.3}s vs small {shadow_small_s:.3}s | count-update big {update_big_s:.3}s vs small {update_small_s:.3}s | count-update 10k big {update_10k_big_s:.3}s vs small {update_10k_small_s:.3}s"
            );
        });
        drop(db_path);
    }

    #[test]
    #[ignore = "manual benchmark: per-phase cost of a fresh scraper chunk"]
    fn scraper_chunk_phase_dissection_bench() {
        let db_path = if std::path::Path::new("/dev/shm").exists() {
            std::path::PathBuf::from("/dev/shm")
                .join(format!("intscrape-phase-bench-{}.db", std::process::id()))
        } else {
            std::env::temp_dir().join(format!("intscrape-phase-bench-{}.db", std::process::id()))
        };
        let _ = std::fs::remove_file(&db_path);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let should_exit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&db_path, should_exit.clone()).await;
            let popular_count = 10_000u64;
            let (_, _) = bench_seed_db(&db, popular_count).await;

            let conn = db.connect().unwrap();
            let storage_id = db
                .file_storage_location_get_or_create(&conn, "bench_storage")
                .await
                .unwrap();
            drop(conn);

            let ns = GenericNamespaceObj {
                name: "bench".into(),
                description: None,
            };

            // process_scraper pre-ensures namespaces before chunk_human; the
            // phase calls need that DDL already done (a namespace can only be
            // partitioned once, and the relationship inserts require the
            // partition table to exist).
            let mut ns_set = HashSet::new();
            ns_set.insert(ns.clone());
            db.namespace_ensure_set(&ns_set).await.expect("pre-ensure ns");

            // Run the phases at two chunk sizes (disjoint file ranges) so the
            // per-phase scaling is visible: a 200-file fresh chunk vs a
            // 1000-file prod-scale chunk. Phase 3 is split into its two
            // sub-steps (bulk relationship insert inside the phase's write
            // transaction + the serialized count/shadow apply) so whichever
            // one stays super-linear at scale shows up directly.
            let mut real_fids: Vec<u64> = Vec::new();
            for (files, start) in [(200u64, 0u64), (1000u64, 1000u64)] {
                let (map, file_list, all_tags) =
                    bench_chunk_map(storage_id, popular_count, files, start, &ns);

                let t = Instant::now();
                let file_cache = db
                    .scraper_phase_files(&map, &file_list)
                    .await
                    .expect("files phase");
                let files_s = t.elapsed().as_secs_f64();
                eprintln!("PHASE[{files}] files: {files_s:.2}s");
                real_fids.extend(file_cache.values().copied());

                let t = Instant::now();
                let mapping = db.scraper_phase_tags(&all_tags).await.expect("tags phase");
                let tags_s = t.elapsed().as_secs_f64();
                eprintln!("PHASE[{files}] tags: {tags_s:.2}s ({} mapped)", mapping.len());

                let mut rels_to_add = HashSet::new();
                for (file, actions) in &map {
                    let file_id = file_cache[&file.internal.hash];
                    for action in actions {
                        for tag in &action.tags {
                            if let Some(&tag_id) = mapping.get(&tag.tag) {
                                rels_to_add.insert((file_id, tag_id as u64));
                            }
                        }
                    }
                }
                let rels = rels_to_add.len();

                let mut bulk_conn = db.connect().unwrap();
                let tn = bulk_conn
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Deferred)
                    .await
                    .expect("begin");
                let t = Instant::now();
                let add_deltas = db
                    .relationships_bulk_add(&tn, &rels_to_add)
                    .await
                    .expect("bulk add");
                let bulk_s = t.elapsed().as_secs_f64();
                tn.commit().await.expect("commit");
                eprintln!("PHASE[{files}] relationships bulk_add: {bulk_s:.2}s ({rels} rels)");

                // The phase-3 bulk read (current file/tag state), ONCE in a
                // single 1000-param IN statement vs in 100-id chunks. This is
                // the other place a big IN list may hit limbo's super-linear
                // planner.
                let fids: Vec<u64> = file_cache.values().copied().collect();
                let mut read_conn = db.connect().unwrap();
                let rtn = read_conn
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Deferred)
                    .await
                    .expect("begin read");
                let t = Instant::now();
                let current1 = db.file_id_get_tag_ids_bulk(&rtn, &fids).await.expect("read big");
                let read_big_s = t.elapsed().as_secs_f64();
                let mut current2 = HashMap::new();
                let t = Instant::now();
                for part in fids.chunks(100) {
                    let part_rels = db.file_id_get_tag_ids_bulk(&rtn, part).await.expect("read small");
                    for (file_id, tag_ids) in part_rels {
                        current2.insert(file_id, tag_ids);
                    }
                }
                let read_small_s = t.elapsed().as_secs_f64();
                rtn.rollback().await.expect("rollback read tx");
                eprintln!("PHASE[{files}] bulk read big IN: {read_big_s:.2}s ({} files -> {} rel sets)", fids.len(), current1.len());
                eprintln!("PHASE[{files}] bulk read 100-chunks: {read_small_s:.2}s ({} files)", current2.len());

                let t = Instant::now();
                db.tag_counts_apply(&add_deltas, &HashMap::new())
                    .await
                    .expect("count apply");
                let apply_s = t.elapsed().as_secs_f64();
                eprintln!("PHASE[{files}] tag_counts_apply: {apply_s:.2}s ({} deltas)", add_deltas.len());
                eprintln!("PHASE[{files}] summary: files {files_s:.2}s + tags {tags_s:.2}s + bulk_add {bulk_s:.2}s + read_big {read_big_s:.2}s + apply {apply_s:.2}s = {:.2}s", files_s + tags_s + bulk_s + read_big_s + apply_s);
            }
        });
        drop(db_path);
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
