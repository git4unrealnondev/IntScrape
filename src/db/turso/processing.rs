//! Turso-native processing for scraper results: persistence of files, tags,
//! relationships, and pending plugin jobs.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use shared_types::helpers::collect_namespaces;
use shared_types::{
    FileInternal, FileManager, FileTagAction, ScraperDataReturn, Tag, TagOperation,
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
                let mut conn = match database.connect() {
                    Ok(conn) => conn,
                    Err(error) => {
                        log::error!("Failed to connect while adding pending scrape jobs: {error}");
                        return false;
                    }
                };
                // CONCURRENT: these inserts run alongside the other scrapers'
                // phases and contend on shared rows. The body is DML-only and
                // idempotent (`INSERT OR IGNORE` + read-only skip checks), so
                // re-running it on a commit conflict is safe.
                let tx = match conn
                    .transaction_with_behavior(Self::write_tx_behavior())
                    .await
                {
                    Ok(tx) => tx,
                    Err(error) => {
                        log::error!("Failed to begin scrape-job transaction: {error}");
                        return false;
                    }
                };

                let mut failed = false;
                'ScraperLoop: for scraperdatareturn in &jobs {
                    for skip_conditions in &scraperdatareturn.skip_conditions {
                        if database
                            .should_skip_item(&tx, skip_conditions.clone())
                            .await
                        {
                            continue 'ScraperLoop;
                        }
                    }
                    if let Err(error) = database.job_add_sql(&tx, &scraperdatareturn.job).await {
                        log::error!("Failed to add scrape job: {error}");
                        failed = true;
                        break;
                    }
                }
                if failed {
                    return false;
                }

                match tx.commit().await {
                    Ok(_) => break,
                    Err(error)
                        if matches!(
                            error,
                            turso::Error::Busy(_) | turso::Error::BusySnapshot(_)
                        ) =>
                    {
                        log::warn!("Scrape-job commit conflicted; retrying: {error}");
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
        let namespace_set = collect_namespaces(
            &map.values()
                .flat_map(|actions| actions.iter())
                .cloned()
                .collect::<Vec<_>>(),
        );
        if let Err(error) = database.namespace_ensure_set(&namespace_set).await {
            log::error!("Failed to pre-ensure namespaces for scrape: {error}");
            return false;
        }

        // The scraper result is persisted through three small, idempotent
        // (`INSERT OR IGNORE`) CONCURRENT transactions — files, tags, then
        // relationships. CONCURRENT lets concurrent scraper chunks keep their
        // write transactions open at the same time and resolve write-write
        // conflicts at commit, instead of queueing for a single writer lock.
        // Splitting still matters because the relationship phase is where
        // contention concentrates: every new relationship also bumps the shared
        // `Tags.count` row, so two scraper chunks that both touch a popular tag
        // collide exactly there. With one giant transaction that collision
        // rolled back the file and tag inserts too; now only the conflicted
        // phase retries, and each phase's smaller write set overlaps other
        // writers far less. Retries use jittered exponential backoff (see
        // `scraper_backoff`) and are capped so a pathological contention storm
        // cannot spin on the shared tokio runtime forever.
        //
        // CONCURRENT re-runs a transaction body whose commit loses a conflict,
        // so every phase must stay idempotent: no side effects outside the
        // database and no non-idempotent statements in the body.
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
    pub(in crate::db::turso) async fn process_scraper_chunk_human(
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
    /// Resolves the db file id a chunk entry's tags must attach to.
    ///
    /// Normal entries resolve through `file_cache` by SHA512 digest. Entries
    /// with an empty digest (existing rows that never had a SHA512 hash) are
    /// resolved by their pinned `id` — the cache cannot hold them, since the
    /// empty key would collapse every hash-less file in the chunk onto one id
    /// and mislink their tags. A hash-less entry without an id cannot be
    /// resolved and its tags are dropped (same as a cache miss).
    fn resolve_file_id(internal: &FileInternal, file_cache: &HashMap<String, u64>) -> Option<u64> {
        if internal.hash.is_empty() {
            internal.id
        } else {
            file_cache.get(&internal.hash).copied()
        }
    }

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
                    .transaction_with_behavior(Self::write_tx_behavior())
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
                // A missing SHA512 (legacy rows) yields hash == "". Never put
                // it in the cache: two different hash-less files in one chunk
                // would collapse to the last writer's id and cross-link their
                // tags. The relationship phase resolves hash-less entries via
                // the pinned `FileInternal.id` instead.
                if !file.hash.is_empty()
                    && let Some(db_id) = file.id
                {
                    file_cache.insert(file.hash.clone(), db_id);
                }
            }

            // Identifying hashes.
            let mut file_hashes: Vec<(u64, &str, &str)> = Vec::new();
            for filemanager in map.keys() {
                let Some(file_id) = Self::resolve_file_id(&filemanager.internal, &file_cache)
                else {
                    continue;
                };
                for file_hash in &filemanager.identifying_hashes {
                    let (algorithm, digest) = crate::db::hashes_supported_to_inner(file_hash);
                    file_hashes.push((file_id, algorithm, digest.as_str()));
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
                    .transaction_with_behavior(Self::write_tx_behavior())
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
                    .transaction_with_behavior(Self::write_tx_behavior())
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
                let file_id = match Self::resolve_file_id(&file_manager.internal, &file_cache) {
                    Some(id) => id,
                    None => continue,
                };

                current_ns_tags.clear();
                explicit_adds.clear();
                set_deletions.clear();

                // Current database state for this file: Namespace -> tag ids.
                if let Some(current_tag_ids) = current_file_relationships.get(&file_id) {
                    for &tag_id in current_tag_ids {
                        if let Some(ns_name) = tag_id_to_ns_name.get(&tag_id)
                            && ns_name != "source_url"
                            && !ns_name.is_empty()
                        {
                            current_ns_tags
                                .entry(ns_name.as_str())
                                .or_default()
                                .insert(tag_id);
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
        &self,
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
                "SELECT f.id, fh.digest, f.extension, f.storage_id, f.size_bytes
                 FROM File f
                 LEFT JOIN FileHashes fh
                   ON fh.file_id = f.id AND fh.algorithm = 'SHA512'
                 WHERE f.id IN ({placeholders});"
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
mod tests;
