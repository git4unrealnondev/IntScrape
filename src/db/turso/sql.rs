use std::collections::{HashMap, HashSet};

use shared_types::*;
use smol_str::SmolStr;
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::{
    SQL_CHUNK_SIZE,
    turso::{TagDb, TursoDatabase},
};

/// Resolves `Tags` ids for `(name, namespace)` keys with one indexed lookup per
/// namespace instead of one point query per key.
///
/// `Tags` carries `UNIQUE(name, namespace)`, whose index leads on `name`, so
/// `name IN (...) AND namespace = ?` is a set of index seeks rather than a scan.
/// Names are sorted and deduped per namespace so the rendered statement text is
/// stable across chunks (and therefore reusable), and so the `IN` list stays
/// as short as the chunk's distinct keys rather than one slot per relationship.
///
/// Only keys that already exist appear in the result; the caller treats
/// everything else as novel and inserts it.
async fn tags_resolve_existing<'a>(
    conn: &Connection,
    keys: impl IntoIterator<Item = (&'a str, i64)>,
) -> Result<HashMap<(String, i64), i64>> {
    let mut out = HashMap::new();

    let mut names_by_namespace: HashMap<i64, Vec<&str>> = HashMap::new();
    for (name, namespace_id) in keys {
        names_by_namespace
            .entry(namespace_id)
            .or_default()
            .push(name);
    }

    for (namespace_id, mut names) in names_by_namespace {
        names.sort_unstable();
        names.dedup();
        let placeholders = std::iter::repeat_n("?", names.len())
            .collect::<Vec<_>>()
            .join(", ");
        let params: Vec<Value> = std::iter::once(Value::from(namespace_id))
            .chain(names.iter().map(|name| Value::from(*name)))
            .collect();
        let mut rows = conn
            .query(
                format!(
                    "SELECT id, name FROM Tags WHERE namespace = ?1 AND name IN ({placeholders});"
                ),
                params_from_iter(params),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            out.insert((name, namespace_id), id);
        }
    }

    Ok(out)
}

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

    /// Vacuums the db
    pub(in crate::db::turso) async fn vacuum(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("VACUUM;").await
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
    /// Only popular tags (`Tags.count >= 5`, mirrored into `Tags_Popular`)
    /// are indexed and searchable; unpopular tags never match, which keeps
    /// the ngram index small. Returns the most-used matching tags, with BM25
    /// relevance breaking ties.
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
        let fts_query = format!("{}", search_string.trim().to_owned());
        // The FTS index only covers popular tags (`Tags_Popular.count >= 5`),
        // so search joins back to Tags for the authoritative count used to
        // rank and return. Unpopular tags are intentionally not searchable.
        let mut rows = conn
            .query(
                "SELECT t.id, t.count, fts_score(p.name, ?1) AS score
FROM Tags_Popular p
JOIN Tags t ON t.id = p.tag_id
WHERE fts_match(p.name, ?1)
ORDER BY t.count DESC, score DESC, t.id ASC
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
            let parents: Vec<_> = parents.into_iter().collect();
            self.parents_bulk_add(conn, &parents).await?;
        }

        Ok(tag_mapping)
    }

    /// Bulk adds tag parent relationships into the db
    pub(in crate::db::turso) async fn parents_bulk_add(
        &self,
        conn: &Connection,
        parents: &[TagParents],
    ) -> Result<()> {
        if parents.is_empty() {
            return Ok(());
        }

        // Callers hand this a `HashSet` iteration (see `tag_action_bulk_add`
        // and the slurp import), so rows arrive in hash order. Order the batch
        // on the unique index key `(tag_id, relate_tag_id, limit_to)` so that
        // index is extended at its right edge instead of being punched all
        // over the b-tree: every insert has to probe that index for the
        // `OR IGNORE` dedupe, and a sorted feed turns those probes into
        // near-sequential seeks. This mirrors what `relationships_bulk_add`
        // already does for the relationship tables.
        // `-1` binds the absent-`limit_to` rows exactly as the sentinel is
        // stored, so NULL rows and sentinel rows group together identically.
        //
        // The sort's benefit is much smaller than an earlier comment here
        // claimed, and on turso 0.8.1 it is not reliably measurable at all:
        // `db_bench_parents` recorded a novel insert of 20k rows at 519 us/row
        // in index order versus 537 us/row reversed (a 3% win) on one run, and
        // 387 versus 363 us/row (a 6% *loss*) on another. The ratio flips sign
        // between runs on the same fixture.
        //
        // The sort is kept because it costs 0.79 us/row and cannot plausibly
        // cost more than that, while the upside on a large fresh import is real
        // even if unmeasurable here. But do not quote the old 29.6 -> 20.4
        // us/row figure as a current measurement, and do not read the existing
        // benchmark as confirming it.
        //
        // On a re-add the ordering is provably irrelevant: every row conflicts,
        // so `OR IGNORE` probes and skips without inserting (measured 96.08 vs
        // 96.19 us/row). Only the novel-insert case can benefit.
        let mut ordered: Vec<&TagParents> = parents.iter().collect();
        ordered.sort_unstable_by_key(|parent| {
            (
                parent.tag_id,
                parent.relate_tag_id,
                parent.limit_to.map(|id| id as i64).unwrap_or(-1),
            )
        });

        // One line per call, not per chunk: a large import chunks thousands of
        // times and the per-chunk line buried the rest of the log.
        log::info!("Adding {} Parents into db", ordered.len());

        for chunk in ordered.chunks(SQL_CHUNK_SIZE) {
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

        // Existing tags are resolved read-only through the UNIQUE(name,
        // namespace) index; only genuinely new tags are inserted. This
        // replaces the previous single upsert, whose `ON CONFLICT ... DO
        // UPDATE` arm rewrote every already-present tag on every re-scrape —
        // and with the tantivy FTS index attached, deleted/re-inserted each
        // document and rewrote the covering index for the whole chunk. At
        // millions of tags that rewrite dominated ingest; a re-scraped tag now
        // costs a unique-index seek and nothing is written.
        for chunk in pending_tags.chunks(SQL_CHUNK_SIZE) {
            // Existing tags are resolved read-only through the UNIQUE(name,
            // namespace) index, one indexed lookup per namespace for the whole
            // chunk. This used to be one point query per tag, so a 4.6k-tag
            // chunk issued 4.6k statements — inside the scraper's write
            // transaction (`scraper_phase_tags`), which widened the MVCC
            // conflict window for every concurrent writer for no benefit: each
            // individual probe is an index seek either way.
            let existing = tags_resolve_existing(
                conn,
                chunk
                    .iter()
                    .map(|(tag, ns_id)| (tag.name.as_str(), *ns_id as i64)),
            )
            .await?;

            let mut novel: Vec<(&Tag, u64)> = Vec::with_capacity(chunk.len());
            for (tag, ns_id) in chunk {
                if !existing.contains_key(&(tag.name.clone(), *ns_id as i64)) {
                    novel.push((tag, *ns_id));
                }
            }

            // Insert only the tags that do not exist yet. `INSERT OR IGNORE`
            // skips conflicting rows individually without touching them, so a
            // tag that slips in concurrently never triggers a write, an FTS
            // document update, or a covering-index rewrite either.
            if !novel.is_empty() {
                let mut params: Vec<Value> = Vec::with_capacity(novel.len() * 2);
                let mut holders = Vec::with_capacity(novel.len());
                for (tag, ns_id) in &novel {
                    holders.push("(?, ?)");
                    params.push(Value::from(tag.name.as_str()));
                    params.push(Value::from(*ns_id as i64));
                }

                let tag_sql_str = format!(
                    "INSERT OR IGNORE INTO Tags (name, namespace) VALUES {} \
                     RETURNING id, name, namespace;",
                    holders.join(", ")
                );

                let mut rows = conn.query(tag_sql_str, params_from_iter(params)).await?;
                let mut inserted_new: HashSet<(String, i64)> = HashSet::with_capacity(novel.len());
                while let Some(row) = rows.next().await? {
                    let id: i64 = row.get(0)?;
                    let name: String = row.get(1)?;
                    let namespace_id: i64 = row.get(2)?;
                    inserted_new.insert((name.clone(), namespace_id));

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

                // A novel tag can still lose the insert race with another
                // connection; OR IGNORE then skips it and RETURNING omits it.
                // Resolve any such straggler so no tag is ever dropped from
                // the caller's id mapping. Batched for the same reason as the
                // pre-insert resolution above.
                if inserted_new.len() < novel.len() {
                    let stragglers: Vec<(String, i64)> = novel
                        .iter()
                        .map(|(tag, ns_id)| (tag.name.clone(), *ns_id as i64))
                        .filter(|key| !inserted_new.contains(key))
                        .collect();
                    for ((name, ns_id), id) in tags_resolve_existing(
                        conn,
                        stragglers.iter().map(|(n, ns)| (n.as_str(), *ns)),
                    )
                    .await?
                    {
                        out.insert(TagDb {
                            id,
                            name: name.into(),
                            namespace_id: ns_id,
                        });
                    }
                }
            }

            for ((name, ns_id), id) in existing {
                out.insert(TagDb {
                    id,
                    name: name.into(),
                    namespace_id: ns_id,
                });
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

    /// Adds a list of files into the db and sets their id if not already set.
    ///
    /// Existing files are resolved read-only through the UNIQUE(hash) index;
    /// only genuinely new files are inserted (`INSERT OR IGNORE`), so a
    /// re-scraped file costs a unique-index seek and no write is issued. This
    /// replaces the previous `ON CONFLICT ... DO UPDATE` upsert, which rewrote
    /// every already-present row on every re-scrape (extension, storage, size)
    /// — at millions of files that rewrite dominated ingest.
    pub(in crate::db::turso) async fn file_add_bulk(
        &self,
        conn: &Connection,
        file_list: &[FileInternal],
    ) -> Result<HashSet<FileInternal>> {
        let mut out = HashSet::new();

        if file_list.is_empty() {
            return Ok(out);
        }

        for chunk in file_list.chunks(SQL_CHUNK_SIZE) {
            // Map hash -> file (last entry wins for duplicate hashes), so the
            // resolved output shape matches the previous single-entry-per-hash
            // RETURNING handling.
            let mut file_map: HashMap<&str, &FileInternal> = HashMap::with_capacity(chunk.len());
            for file in chunk {
                file_map.insert(file.hash.as_str(), file);
            }

            // Resolve the chunk's whole hash set with batched indexed lookups
            // instead of one point query per hash. `File.hash` is UNIQUE, so
            // `hash IN (...)` is a set of unique-index seeks. This used to issue
            // one statement per file inside the scraper's write transaction
            // (`scraper_phase_files`), so a 4.6k-file chunk issued 4.6k
            // statements and held the MVCC conflict window open for all of them.
            //
            // Sorted + deduped so the rendered statement text is stable across
            // chunks and the `IN` list carries only the chunk's distinct hashes.
            let mut hashes: Vec<&str> = file_map.keys().copied().collect();
            hashes.sort_unstable();
            hashes.dedup();

            // Split the chunk into already-known hashes (resolved id) and
            // genuinely new ones. Only the novel hashes are written.
            let mut existing: HashMap<&str, i64> = HashMap::with_capacity(hashes.len());
            for key_chunk in hashes.chunks(SQL_CHUNK_SIZE) {
                let placeholders = std::iter::repeat_n("?", key_chunk.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let params: Vec<Value> = key_chunk.iter().map(|hash| Value::from(*hash)).collect();
                let mut rows = conn
                    .query(
                        format!("SELECT id, hash FROM File WHERE hash IN ({placeholders});"),
                        params_from_iter(params),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    let id: i64 = row.get(0)?;
                    let hash: String = row.get(1)?;
                    // Re-key through `file_map` so the stored key borrows from
                    // the caller's chunk rather than the row-local String.
                    if let Some((key, _)) = file_map.get_key_value(hash.as_str()) {
                        existing.insert(*key, id);
                    }
                }
            }

            let novel: HashMap<&str, &FileInternal> = file_map
                .iter()
                .filter(|(hash, _)| !existing.contains_key(*hash))
                .map(|(hash, file)| (*hash, *file))
                .collect();

            if novel.is_empty() {
                for (hash, id) in existing {
                    if let Some(file) = file_map.get(hash) {
                        let mut file = (*file).clone().clone();
                        file.id = Some(id as u64);
                        out.insert(file);
                    }
                }
                continue;
            }

            let mut holders = Vec::with_capacity(novel.len());
            let mut params = Vec::with_capacity(novel.len() * 5);

            for file in novel.values() {
                holders.push("(?, ?, ?, ?, ?)");
                params.push(Value::from(file.id.map(|f| f as i64)));
                params.push(Value::from(file.hash.as_str()));
                params.push(Value::from(file.extension.as_str()));
                params.push(Value::from(file.storage_id as i64));
                params.push(Value::from(file.size_bytes.map(|f| f as i64)));
            }
            let sql_str = format!(
                "INSERT OR IGNORE INTO File (id, hash, extension, storage_id, size_bytes) \
                 VALUES {} RETURNING id, hash;",
                holders.join(", ")
            );

            let mut rows = conn.query(sql_str, params_from_iter(params)).await?;
            let mut inserted_new: HashSet<String> = HashSet::with_capacity(novel.len());
            while let Some(row) = rows.next().await? {
                let id: i64 = row.get(0)?;
                let hash: String = row.get(1)?;

                if let Some(file) = file_map.get(hash.as_str()) {
                    let mut file = (*file).clone().clone();
                    file.id = Some(id as u64);
                    out.insert(file);
                    inserted_new.insert(hash.clone());
                }
            }

            // A novel file can still lose the insert race with another
            // connection; OR IGNORE then skips it and RETURNING omits it.
            // Resolve any such straggler so no file is ever dropped from the
            // caller's id mapping.
            if inserted_new.len() < novel.len() {
                let mut missing: Vec<&str> = novel
                    .keys()
                    .copied()
                    .filter(|hash| !inserted_new.contains(*hash))
                    .collect();
                missing.sort_unstable();
                for key_chunk in missing.chunks(SQL_CHUNK_SIZE) {
                    let placeholders = std::iter::repeat_n("?", key_chunk.len())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let params: Vec<Value> =
                        key_chunk.iter().map(|hash| Value::from(*hash)).collect();
                    let mut rows = conn
                        .query(
                            format!("SELECT id, hash FROM File WHERE hash IN ({placeholders});"),
                            params_from_iter(params),
                        )
                        .await?;
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
            }

            for (hash, id) in existing {
                if let Some(file) = file_map.get(hash) {
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
pub(in crate::db::turso) fn parents_insert_sql(parent_count: usize) -> String {
    // The per-row `format!` below allocates a throwaway `String`, and the final
    // `format!` copies the whole placeholder list into a second `String`. Both
    // look like obvious waste, and were "fixed" to a preallocated buffer plus
    // `write!` -- which measured *58% slower* (1.93 ms vs 1.22 ms for a
    // 4600-row chunk over 20 rounds). `format!` with inline captured identifiers
    // compiles to tighter code than `write!` with mixed inline and positional
    // arguments. Reverted, and left as written so it is not "fixed" again. See
    // `db_bench_parents`.
    //
    // Either shape is ~0.3% of the call, so this is not worth revisiting.
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

/// Binds each parent row to its three `VALUES` placeholders.
///
/// An absent `limit_to` binds the `-1` sentinel the current `Parents` schema
/// stores instead of SQL `NULL`: tag ids are rowids that start at 1, so `-1`
/// is unambiguous, and the dedupe index can then be a plain composite
/// `(tag_id, relate_tag_id, limit_to)` key rather than an `IFNULL(limit_to,
/// -1)` expression key that every insert and probe re-evaluates.
pub(in crate::db::turso) fn parents_params(parents: &[&TagParents]) -> Vec<Value> {
    parents
        .iter()
        .flat_map(|parent| {
            [
                Value::from(parent.tag_id as i64),
                Value::from(parent.relate_tag_id as i64),
                match parent.limit_to {
                    Some(limit_id) => Value::from(limit_id as i64),
                    None => Value::from(-1),
                },
            ]
        })
        .collect()
}
