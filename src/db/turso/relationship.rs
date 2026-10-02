use std::collections::{HashMap, HashSet};

use shared_types::{PluginTag, Tag, TagParents};
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

/// Cap on the size of a single popularity fold-in statement.
///
/// `tag_counts_apply` feeds a whole chunk's deltas into `Tags.count` and the
/// FTS shadow. Limbo's planner is roughly quadratic in one statement's
/// expression size: a single `UPDATE` carrying a multi-thousand-term
/// `CASE`/`IN` tree costs seconds for 5k deltas and minutes for 50k (a big
/// scrape page), which stalls every job behind `tag_count_lock`. Statements of
/// a few hundred terms stay close to the real per-row cost (b-tree MVCC
/// writes), so the fold-in is split into pieces this size.
const POPULARITY_FOLD_CHUNK: usize = 100;

/// Minimum `Tags.count` for a tag to be mirrored into the `Tags_Popular`
/// search shadow (and therefore be autocomplete-searchable).
///
/// The shadow's whole purpose is to keep the ngram FTS index small, so the bar
/// is a literal `5` baked into the shadow-rebuild SQL in `slurp.rs`,
/// `schema_current.rs` and `migrations.rs` as well. It is named here so the
/// threshold *test* (`tags_needing_shadow_sync`) and the threshold *write*
/// (`sync_tags_popular`) are bound to the same value instead of being two
/// independently-typed `5`s that a future edit could split.
const POPULAR_TAG_THRESHOLD: i64 = 5;

/// Ceiling on the parameters in one pushed-down union statement.
///
/// The pushed form repeats the whole id list in every namespace arm, so its SQL
/// text is `arms * len * ~4` bytes — 98 KB for 1,000 ids across 32 partitions.
/// Parsing that dominates: at 1M relationships the same lookup costs 10.4 ms at
/// 50 ids per statement, 26.9 ms at 200, and 144.6 ms at 1,000. So the batch is
/// sized by this budget rather than by `SQL_CHUNK_SIZE`, which keeps each
/// statement small enough to parse while still avoiding the full scans.
///
/// (Measured ceiling: turso accepts 300,000 parameters in one statement, and
/// the codebase already relies on ~9,200. 3,200 is far inside both, and is where
/// the per-statement cost curve flattens.)
pub(in crate::db::turso) const PUSHED_UNION_PARAM_BUDGET: usize = 3_200;

/// How many ids to put in one pushed-down statement, given the arm count.
///
/// A larger library makes the pushdown pay (it replaces a scan of every
/// relationship row with one index seek per partition); a tiny one does not,
/// where a sequential scan is cheaper than a seek plus a table lookup. Sizing
/// by arm count rather than row count keeps each statement small enough to
/// parse cheaply, and deliberately does *not* try to predict which regime the
/// database is in — that would mean caching a row count and invalidating it,
/// to save a few milliseconds on installations small enough not to care.
///
/// Measured over 32 partitions, looking up 200 files, against total
/// relationship rows:
///
/// | rows      | filter outside | pushed | |
/// |-----------|----------------|--------|---|
/// | 5,000     | 9.1 ms         | 14.7 ms | scan wins by 1.6x |
/// | 20,000    | 38.4 ms        | 25.4 ms | pushed 1.5x |
/// | 80,000    | 163.7 ms       | 48.0 ms | pushed 3.4x |
/// | 300,000   | 561.3 ms       | 24.2 ms | pushed 23x |
/// | 1,000,000 | 1,201.0 ms     | 26.1 ms | pushed 46x |
///
/// The crossover sits near 10-20k rows and the loss below it is a few
/// milliseconds. Above it the pushed form is flat — bounded by the result
/// rather than the table, which is the property the whole point rests on.
/// The `Relationship_{ns}` table name for every namespace, in id order.
///
/// Returned as a list so a caller that needs both the union text and the arm
/// count (to size its batches) pays for one namespace query, not two. The batch
/// sizer used to add its own `SELECT COUNT(*) FROM Namespace` on top of the
/// `SELECT id FROM Namespace` the union builder already ran.
pub(in crate::db::turso) async fn relationship_union_tables(conn: &Connection) -> Vec<String> {
    let mut tables = Vec::new();
    if let Ok(mut rows) = conn
        .query("SELECT id FROM Namespace ORDER BY id;", ())
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            if let Ok(namespace_id) = row.get::<i64>(0) {
                tables.push(format!("Relationship_{namespace_id}"));
            }
        }
    }
    tables
}

/// How many ids to put in one pushed-down statement, given the arm count.
pub(in crate::db::turso) fn pushed_union_batch_size(arms: usize) -> usize {
    (PUSHED_UNION_PARAM_BUDGET / arms.max(1)).clamp(1, SQL_CHUNK_SIZE)
}

/// Renders the union text for a known table list. Split out from
/// `relationship_union_source` so a caller that already has the tables (and so
/// needs their count to size a batch) does not re-query them.
pub(in crate::db::turso) fn union_source_from_tables(
    tables: &[String],
    alias: &str,
    arm_predicate: Option<&str>,
) -> String {
    let source = if tables.is_empty() {
        "SELECT NULL AS file_id, NULL AS tag_id WHERE 0".to_string()
    } else {
        tables
            .iter()
            .map(|table| match arm_predicate {
                Some(predicate) => format!("SELECT file_id, tag_id FROM {table} WHERE {predicate}"),
                None => format!("SELECT file_id, tag_id FROM {table}"),
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    };
    format!("({source}) AS {alias}")
}

/// `?1, ?2, ... ?count` — numbered placeholders starting at 1.
///
/// Numbered parameters are what let a predicate be repeated across the arms of
/// a `UNION ALL` while being bound only once (see
/// `relationship_union_source`); a positional list would need the values
/// repeated per arm.
pub(in crate::db::turso) fn numbered_placeholders(count: usize) -> String {
    let mut out = String::with_capacity(count * 3);
    for index in 0..count {
        if index > 0 {
            out.push_str(", ");
        }
        out.push('?');
        out.push_str(&(index + 1).to_string());
    }
    out
}

impl TursoDatabase {
    /// Builds the inlined `SELECT file_id, tag_id FROM Relationship_x UNION ALL ...`
    /// source that spans every namespace's relationship partition.
    ///
    /// `arm_predicate` is an optional SQL fragment that is pushed *into every
    /// arm* rather than applied to the union as a whole. That distinction is
    /// the whole point of this function.
    ///
    /// Limbo does not push a predicate through `UNION ALL`: given
    /// `SELECT ... FROM (A UNION ALL B ...) WHERE file_id IN (...)` it plans
    /// `SCAN` on every arm, so the query reads *every* relationship row in the
    /// database to answer a lookup for a handful of files. Each partition does
    /// carry an index on `file_id` (`idx_Relationship_{ns}_tag_file`), but the
    /// planner never reaches it. Repeating the predicate per arm turns those
    /// 32 full scans into 32 covering-index seeks.
    ///
    /// Measured over 32 partitions and 200 files, `EXPLAIN QUERY PLAN` goes from
    /// `full_scans=32 indexed_searches=0` to `full_scans=0
    /// indexed_searches=32`, and the query from 1,201 ms to 26 ms at a million
    /// relationship rows. See `pushed_union_batch_size` for the full curve and
    /// the small-database trade-off.
    ///
    /// Because the fragment is repeated, it must use **numbered** placeholders
    /// (`?1`, `?2`, ...). A numbered parameter may appear any number of times in
    /// one statement and is bound once, so pushing a predicate into 32 arms
    /// costs no extra parameters — unlike a positional `IN (?, ?, ?)` list,
    /// which would have to be repeated 32 times over.
    ///
    /// The fragment may only reference the bare `file_id` / `tag_id` columns
    /// (arms are unaliased) and any other table it names itself.
    pub(in crate::db::turso) async fn relationship_union_source(
        &self,
        conn: &Connection,
        alias: &str,
        arm_predicate: Option<&str>,
    ) -> Result<String> {
        let tables = relationship_union_tables(conn).await;
        Ok(union_source_from_tables(&tables, alias, arm_predicate))
    }

    /// Gets the first `file_id` related to a tag, used by the tag->file lookup.
    pub(in crate::db::turso) async fn first_file_id_for_tag(
        &self,
        conn: &Connection,
        tag_id: u64,
    ) -> Result<Option<u64>> {
        let Some(namespace_id) = self.tag_namespace_id(conn, tag_id).await? else {
            return Ok(None);
        };

        let mut rows = conn
            .query(
                &format!(
                    "SELECT file_id FROM Relationship_{namespace_id} WHERE tag_id = ?1 LIMIT 1;"
                ),
                (tag_id as i64,),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    /// Gets all `tag_ids` associated with a `file_id`.
    pub(in crate::db::turso) async fn relationship_get_tag_id(
        &self,
        conn: &Connection,
        file_id: u64,
    ) -> Result<HashSet<u64>> {
        let relationship_source = self
            .relationship_union_source(conn, "relationships", Some("file_id = ?1"))
            .await?;
        let sql = format!("SELECT tag_id FROM {relationship_source};");

        let mut out = HashSet::new();
        let mut rows = conn.query(&sql, (file_id as i64,)).await?;
        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets all `file_ids` associated with a `tag_id`.
    pub(in crate::db::turso) async fn relationship_get_file_id(
        &self,
        conn: &Connection,
        tag_id: u64,
    ) -> Result<HashSet<u64>> {
        let relationship_source = self
            .relationship_union_source(conn, "relationships", Some("tag_id = ?1"))
            .await?;
        let sql = format!("SELECT file_id FROM {relationship_source};");

        let mut out = HashSet::new();
        let mut rows = conn.query(&sql, (tag_id as i64,)).await?;
        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets the `tag_ids` for a file filtered to a single namespace.
    pub(in crate::db::turso) async fn file_id_get_tag_ids_filtered(
        &self,
        conn: &Connection,
        file_id: u64,
        namespace_id: u64,
    ) -> Result<HashSet<u64>> {
        let sql = format!("SELECT tag_id FROM Relationship_{namespace_id} WHERE file_id = ?1;");

        let mut out = HashSet::new();
        let mut rows = conn.query(&sql, (file_id as i64,)).await?;
        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets files whose tag is the related parent of the supplied structural tag.
    pub(in crate::db::turso) async fn relationship_get_parent_file_id(
        &self,
        conn: &Connection,
        tag_id: u64,
    ) -> Result<HashSet<u64>> {
        // Pushed into each arm; the outer DISTINCT still collapses the
        // per-partition results into one file set.
        let relationship_source = self
            .relationship_union_source(
                conn,
                "relationships",
                Some(
                    "tag_id = ?1 OR tag_id IN (
                         SELECT Parents.relate_tag_id FROM Parents WHERE Parents.tag_id = ?1
                     )",
                ),
            )
            .await?;
        let sql = format!("SELECT DISTINCT relationships.file_id FROM {relationship_source}");

        let mut out = HashSet::new();
        let mut rows = conn.query(&sql, (tag_id as i64,)).await?;
        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets every parent relation declared by a child tag.
    pub(in crate::db::turso) async fn parent_relationships_get(
        &self,
        conn: &Connection,
        tag_id: u64,
    ) -> Result<Vec<TagParents>> {
        self.parents_by_column(conn, "tag_id", tag_id).await
    }

    /// Gets parent relations for multiple child tags.
    pub(in crate::db::turso) async fn parent_relationships_get_many(
        &self,
        conn: &Connection,
        tag_ids: &HashSet<u64>,
    ) -> Result<HashMap<u64, Vec<TagParents>>> {
        let mut out = HashMap::new();
        for tag_id in tag_ids {
            out.insert(*tag_id, self.parent_relationships_get(conn, *tag_id).await?);
        }
        Ok(out)
    }

    /// Gets every child relation that points at a parent tag.
    pub(in crate::db::turso) async fn child_relationships_get(
        &self,
        conn: &Connection,
        relate_tag_id: u64,
    ) -> Result<Vec<TagParents>> {
        self.parents_by_column(conn, "relate_tag_id", relate_tag_id)
            .await
    }

    /// Gets child relations for multiple parent tags.
    pub(in crate::db::turso) async fn child_relationships_get_many(
        &self,
        conn: &Connection,
        tag_ids: &HashSet<u64>,
    ) -> Result<HashMap<u64, Vec<TagParents>>> {
        let mut out = HashMap::new();
        for tag_id in tag_ids {
            out.insert(*tag_id, self.child_relationships_get(conn, *tag_id).await?);
        }
        Ok(out)
    }

    /// Gets one exact child-parent relation, including its optional limit tag.
    pub(in crate::db::turso) async fn parent_relationship_get(
        &self,
        conn: &Connection,
        tag_id: u64,
        relate_tag_id: u64,
    ) -> Result<Option<TagParents>> {
        let mut rows = conn
            .query(
                "SELECT tag_id, relate_tag_id, limit_to
                 FROM Parents
                 WHERE tag_id = ?1 AND relate_tag_id = ?2
                 LIMIT 1;",
                (tag_id as i64, relate_tag_id as i64),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(TagParents {
                tag_id: row.get(0)?,
                relate_tag_id: row.get(1)?,
                limit_to: row.get(2)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// Shared `Parents` query filtered by either the child (`tag_id`) or
    /// parent (`relate_tag_id`) column.
    async fn parents_by_column(
        &self,
        conn: &Connection,
        column: &str,
        value: u64,
    ) -> Result<Vec<TagParents>> {
        let sql =
            format!("SELECT tag_id, relate_tag_id, limit_to FROM Parents WHERE {column} = ?1;");

        let mut out = Vec::new();
        let mut rows = conn.query(&sql, (value as i64,)).await?;
        while let Some(row) = rows.next().await? {
            out.push(TagParents {
                tag_id: row.get(0)?,
                relate_tag_id: row.get(1)?,
                limit_to: row.get(2)?,
            });
        }

        Ok(out)
    }

    /// Returns the subset of `piece` whose `Tags_Popular` membership will not
    /// match its `count >= 5` state *after* the pending delta is applied.
    ///
    /// `sync_tags_popular` is unconditionally convergent — it re-asserts the
    /// whole batch — which is the right thing for a repair path but pure
    /// overhead on the scraper hot path: `tag_counts_apply` runs it after every
    /// relationship phase, and in steady state essentially no tag crosses the
    /// threshold, so the DELETE and the INSERT..SELECT both match zero rows
    /// while still costing two statement executions (and, because
    /// `Tags_Popular` is FTS-indexed, a Tantivy segment flush) inside the
    /// globally serialized `tag_count_lock` writer.
    ///
    /// One `LEFT JOIN` pre-read answers "is this tag in the shadow, and what is
    /// its current count", so the caller can skip the sync entirely when the
    /// answer is "already consistent" and pass only the genuinely divergent
    /// ids when it is not. That is an exact filter, not a heuristic: it
    /// compares the *post-update* threshold state against actual shadow
    /// presence, so it preserves the convergent behaviour (including repairing
    /// a shadow that drifted) and only elides work that provably changes zero
    /// rows. `decrement` must match how the delta is about to be applied —
    /// `count + delta` for adds, `MAX(count - delta, 0)` for deletes.
    ///
    /// Must be called *before* the count update, since it derives the new
    /// count from the old one.
    async fn tags_needing_shadow_sync(
        &self,
        conn: &Connection,
        piece: &[(u64, u64)],
        decrement: bool,
    ) -> Result<Vec<u64>> {
        if piece.is_empty() {
            return Ok(Vec::new());
        }
        let values: Vec<Value> = piece
            .iter()
            .map(|(id, _)| Value::from(*id as i64))
            .collect();
        let placeholders = std::iter::repeat_n("?", piece.len())
            .collect::<Vec<_>>()
            .join(",");
        // `tp.tag_id IS NOT NULL` is the shadow-presence flag; both sides are
        // primary-key lookups, so this stays a set of point reads.
        let mut rows = conn
            .query(
                format!(
                    "SELECT t.id, t.count, (tp.tag_id IS NOT NULL)
                     FROM Tags t
                     LEFT JOIN Tags_Popular tp ON tp.tag_id = t.id
                     WHERE t.id IN ({placeholders})"
                ),
                params_from_iter(values),
            )
            .await?;

        let deltas: HashMap<u64, u64> = piece.iter().copied().collect();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let tag_id: u64 = row.get(0)?;
            let old_count: i64 = row.get(1)?;
            let in_shadow: i64 = row.get(2)?;
            let delta = deltas.get(&tag_id).copied().unwrap_or(0) as i64;
            // Mirror `tag_count_update_sql` exactly, including the clamp.
            let new_count = if decrement {
                (old_count - delta).max(0)
            } else {
                old_count + delta
            };
            if (new_count >= POPULAR_TAG_THRESHOLD) != (in_shadow != 0) {
                out.push(tag_id);
            }
        }
        Ok(out)
    }

    /// Mirrors the given tags' current popularity into the search shadow:
    /// `Tags_Popular` keeps exactly the `count >= 5` tags that the FTS index
    /// covers. Must run on the same connection (and, for the batch path, the
    /// same transaction) that applied the `Tags.count` change so counts and
    /// searchability can never diverge. One DELETE for the below-threshold
    /// rows and one INSERT OR IGNORE for the qualifying ones, chunked at
    /// POPULARITY_FOLD_CHUNK so a big recount fold-in never builds one giant
    /// statement (limbo's planner cost is ~quadratic in statement size).
    pub(in crate::db::turso) async fn sync_tags_popular(
        &self,
        conn: &Connection,
        tag_ids: &[u64],
    ) -> Result<()> {
        if tag_ids.is_empty() {
            return Ok(());
        }
        for chunk in tag_ids.chunks(POPULARITY_FOLD_CHUNK) {
            // Owned value buffers only: nothing borrowed may cross the await
            // below, or the future stops proving Send inside the IPC/scraper
            // task chains (rustc reports a higher-ranked `Send` for the
            // whole handler).
            let values: Vec<i64> = chunk.iter().map(|id| *id as i64).collect();
            let mut placeholders = String::with_capacity(chunk.len() * 2);
            for (index, _) in chunk.iter().enumerate() {
                if index > 0 {
                    placeholders.push(',');
                }
                placeholders.push('?');
            }
            // No correlated subquery: limbo cannot parse an outer reference to
            // the DELETEd table (`Parse error: no such table`). The `tag_id IN`
            // guard narrows the scan to this batch; the NOT IN subquery is then
            // scoped to the same batch ids (PK point lookups) so a single
            // relationship add/delete never scans the whole popular set.
            //
            // The threshold is bound rather than inlined so this write and
            // `tags_needing_shadow_sync`'s read can never disagree about which
            // tags belong in the shadow.
            conn.execute(
                format!(
                    "DELETE FROM Tags_Popular
                     WHERE tag_id IN ({placeholders}) AND tag_id NOT IN (
                         SELECT id FROM Tags
                         WHERE id IN ({placeholders}) AND count >= ?
                     )"
                ),
                // Both placeholders lists get the same owned batch values.
                params_from_iter(
                    values
                        .iter()
                        .copied()
                        .map(Value::from)
                        .chain(values.iter().copied().map(Value::from))
                        .chain(std::iter::once(Value::from(POPULAR_TAG_THRESHOLD))),
                ),
            )
            .await?;
            conn.execute(
                format!(
                    "INSERT OR IGNORE INTO Tags_Popular(tag_id, name)
                     SELECT id, name FROM Tags
                     WHERE id IN ({placeholders}) AND count >= ?"
                ),
                params_from_iter(
                    values
                        .into_iter()
                        .map(Value::from)
                        .chain(std::iter::once(Value::from(POPULAR_TAG_THRESHOLD))),
                ),
            )
            .await?;
        }
        Ok(())
    }

    /// Adds a `file_id` -> `tag_id` relationship into its namespace partition.
    pub(in crate::db::turso) async fn relationship_add(
        &self,
        conn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) -> Result<()> {
        let Some(namespace_id) = self.tag_namespace_id(conn, tag_id).await? else {
            return Ok(());
        };
        let sql = format!(
            "INSERT OR IGNORE INTO Relationship_{namespace_id} (file_id, tag_id) VALUES (?1, ?2);"
        );
        let inserted = conn.execute(sql, (file_id as i64, tag_id as i64)).await?;
        if inserted > 0 {
            conn.execute(
                "UPDATE Tags SET count = count + 1 WHERE id = ?1;",
                (tag_id as i64,),
            )
            .await?;
            self.sync_tags_popular(conn, &[tag_id]).await?;
        }
        Ok(())
    }

    /// Bulk adds `(file_id, tag_id)` relationships.
    ///
    /// Returns the per-tag count deltas — only tags whose insert actually
    /// created a row are counted. The `Tags.count` rows are deliberately NOT
    /// updated inside this transaction: bumping a shared popular tag's count
    /// is the hottest write-write conflict in the system, so the deltas are
    /// returned and applied afterwards through `tag_counts_apply`, which
    /// serializes those writes behind a single lock.
    pub(in crate::db::turso) async fn relationships_bulk_add(
        &self,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) -> Result<HashMap<u64, u64>> {
        let mut aggregated_deltas = HashMap::new();
        if relationships.is_empty() {
            return Ok(aggregated_deltas);
        }

        // Resolve every *distinct* tag's namespace with chunked lookups instead
        // of one point query per relationship. The old code collected one id per
        // relationship, so a chunk touching 24k relationships across 200 tags
        // issued six `IN` queries that were almost entirely duplicate ids.
        let mut tag_namespaces = HashMap::new();
        let mut distinct_tag_ids: Vec<u64> =
            relationships.iter().map(|(_, tag_id)| *tag_id).collect();
        distinct_tag_ids.sort_unstable();
        distinct_tag_ids.dedup();
        for chunk in distinct_tag_ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT id, namespace FROM Tags WHERE id IN ({placeholders});");
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();
            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                tag_namespaces.insert(row.get::<u64>(0)?, row.get::<u64>(1)?);
            }
        }

        let mut by_namespace: HashMap<u64, HashSet<(u64, u64)>> = HashMap::new();
        for &(file_id, tag_id) in relationships {
            let Some(namespace_id) = tag_namespaces.get(&tag_id) else {
                continue;
            };
            by_namespace
                .entry(*namespace_id)
                .or_default()
                .insert((file_id, tag_id));
        }

        for (namespace_id, namespace_relationships) in by_namespace {
            // Feed each namespace's table rows in (tag_id, file_id) key order.
            // Tuned for the MVCC commit-log path (sequential b-tree extension
            // avoided the ~22s COW commit for a 24k-row chunk re-tagging
            // popular tags: 12.3s -> 5.0s). Under the WAL journal it is
            // neutral (A/B: 18.8s unsorted vs 19.0s sorted for the same 24k
            // rows) — kept for deterministic insert order.
            let mut rels: Vec<(u64, u64)> = namespace_relationships.into_iter().collect();
            rels.sort_unstable();
            for chunk in rels.chunks(SQL_CHUNK_SIZE) {
                let mut holders = Vec::with_capacity(chunk.len());
                let mut params = Vec::with_capacity(chunk.len() * 2);
                for (file_id, tag_id) in chunk {
                    holders.push("(?, ?)");
                    params.push(Value::from(*file_id as i64));
                    params.push(Value::from(*tag_id as i64));
                }
                let mut inserted_rows = conn
                    .query(
                        format!(
                            "INSERT OR IGNORE INTO Relationship_{namespace_id} (file_id, tag_id) \
                             VALUES {} RETURNING tag_id",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await?;
                // Only genuinely inserted rows appear in RETURNING; OR IGNORE
                // rows are skips and must not bump the count.
                while let Some(row) = inserted_rows.next().await? {
                    let tag_id: u64 = row.get(0)?;
                    *aggregated_deltas.entry(tag_id).or_default() += 1;
                }
            }
        }

        Ok(aggregated_deltas)
    }

    /// Deletes `(file_id, tag_id)` relationships.
    ///
    /// Returns the per-tag count deltas to subtract (again: only rows the
    /// DELETE actually removed). Like `relationships_bulk_add`, the
    /// `Tags.count` maintenance is deferred to `tag_counts_apply` so the
    /// shared count row stays out of the concurrent write set.
    pub(in crate::db::turso) async fn relationship_bulk_delete(
        &self,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) -> Result<HashMap<u64, u64>> {
        let mut aggregated_deltas = HashMap::new();
        if relationships.is_empty() {
            return Ok(aggregated_deltas);
        }

        // Resolve each *distinct* tag's namespace once. The old code collected
        // one id per relationship, so a chunk touching 24k relationships over
        // 200 tags issued six `IN` queries full of duplicate ids.
        let mut tag_namespaces = HashMap::new();
        let distinct_tag_ids: Vec<u64> = {
            let mut ids: Vec<u64> = relationships.iter().map(|(_, tag_id)| *tag_id).collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        for chunk in distinct_tag_ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT id, namespace FROM Tags WHERE id IN ({placeholders});");
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();
            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                tag_namespaces.insert(row.get::<u64>(0)?, row.get::<u64>(1)?);
            }
        }

        let mut by_namespace: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for &(file_id, tag_id) in relationships {
            let Some(namespace_id) = tag_namespaces.get(&tag_id) else {
                continue;
            };
            by_namespace
                .entry(*namespace_id)
                .or_default()
                .push((file_id, tag_id));
        }

        for (namespace_id, rels) in by_namespace {
            // Group by tag and delete with `tag_id = ? AND file_id IN (...)`.
            //
            // The previous form was one `(file_id = ? AND tag_id = ?)` term per
            // relationship, joined by `OR`. Two problems, both fatal:
            //
            // * Limbo caps expression depth at 100, so the statement failed
            //   outright with `Parse error: Expression tree is too large` at
            //   around a hundred rows — far below `SQL_CHUNK_SIZE`. Any
            //   `TagOperation::Del` or `Set` touching more than ~100
            //   relationships raised instead of deleting.
            // * The planner turned the term list into a `MULTI-INDEX OR`, whose
            //   cost grows with the number of terms.
            //
            // `Relationship_{ns}` is keyed `(tag_id, file_id)`, so pinning
            // `tag_id` and listing `file_id`s is a plain covering-index seek on
            // the primary key (`SEARCH ... USING COVERING INDEX
            // sqlite_autoindex_relationship_n_1 (tag_id=?)`) that does not nest
            // a term per row at all. Row-value `IN (VALUES ...)` would have been
            // the tidier spelling but is a `LIST SUBQUERY` plus a full
            // `SCAN` of the partition in limbo, so it is deliberately not used.
            //
            // A typical `Set` re-tag hits one tag across many files, so this
            // also collapses the statement count: the whole delete set becomes
            // one statement per (namespace, tag).
            let mut by_tag: HashMap<u64, Vec<u64>> = HashMap::new();
            for (file_id, tag_id) in rels {
                by_tag.entry(tag_id).or_default().push(file_id);
            }
            for (tag_id, mut file_ids) in by_tag {
                // Sorted so the IN list feeds the index in order, and deduped so
                // a repeated (file, tag) pair is not bound twice.
                file_ids.sort_unstable();
                file_ids.dedup();
                for chunk in file_ids.chunks(SQL_CHUNK_SIZE) {
                    let placeholders = std::iter::repeat_n("?", chunk.len())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let mut params = Vec::with_capacity(chunk.len() + 1);
                    params.push(Value::from(tag_id as i64));
                    params.extend(chunk.iter().map(|file_id| Value::from(*file_id as i64)));
                    let mut deleted_rows = conn
                        .query(
                            format!(
                                "DELETE FROM Relationship_{namespace_id}
                                 WHERE tag_id = ? AND file_id IN ({placeholders})
                                 RETURNING tag_id"
                            ),
                            params_from_iter(params),
                        )
                        .await?;
                    while let Some(row) = deleted_rows.next().await? {
                        let returned: u64 = row.get(0)?;
                        *aggregated_deltas.entry(returned).or_default() += 1;
                    }
                }
            }
        }

        Ok(aggregated_deltas)
    }

    /// Applies accumulated relationship count deltas to `Tags.count`.
    ///
    /// Relationship rows are written by many concurrent write transactions,
    /// but the shared `Tags.count` row they each need to bump is one hot row:
    /// two chunks touching the same popular tag guarantee a write-write
    /// conflict there. So the deltas returned by
    /// `relationships_bulk_add` / `relationship_bulk_delete` are folded in
    /// here — one short serialized transaction at a time, held behind
    /// `tag_count_lock` — after the relationship inserts have already
    /// committed. The heavy relationship inserts stay out of this writer;
    /// only the tiny count bookkeeping serializes, which is the point: the
    /// count row is never written by two transactions at once anymore.
    ///
    /// Deliberately NOT CONCURRENT (`serialized_tx_behavior`): the mutex
    /// already admits one writer at a time, so CONCURRENT would only add
    /// commit-time conflict retries here without any parallelism to trade
    /// them for. This is also the hottest `Tags.count` write in the system.
    pub(crate) async fn tag_counts_apply(
        &self,
        add_deltas: &HashMap<u64, u64>,
        del_deltas: &HashMap<u64, u64>,
    ) -> Result<()> {
        if add_deltas.is_empty() && del_deltas.is_empty() {
            return Ok(());
        }

        // One mutex guard for the whole fold-in, so no two callers ever
        // update the same count row concurrently.
        let _guard = self.tag_count_lock.lock().await;
        // No defensive copies: `retry_mvcc` takes a plain `FnMut` with no
        // `'static` bound, so the retried closure can borrow the caller's maps.
        // The clones this replaces sat *inside* `tag_count_lock`, so for a large
        // chunk they extended the serialized window that every other job's
        // fold-in queues behind, purely to be copied twice.
        self.retry_mvcc(|| async {
            let mut conn = self.connect()?;
            // `tx` rolls back on drop, so a mid-loop failure below unwinds
            // cleanly and `retry_mvcc` re-runs the whole fold-in.
            let tx = conn
                .transaction_with_behavior(Self::serialized_tx_behavior())
                .await?;
            // Fold the deltas in POPULARITY_FOLD_CHUNK pieces inside this one
            // transaction: one count UPDATE plus its matching shadow sync per
            // piece. The whole apply still commits atomically (a failed piece
            // rolls everything back and `retry_mvcc` re-runs it), and a single
            // commit keeps the FTS writer from producing one tiny segment per
            // piece. Chunking exists because limbo's planner is ~quadratic in
            // one statement's expression size: a single unbounded UPDATE with a
            // multi-thousand-term CASE/IN tree cost seconds for 5k deltas and
            // *minutes* for 50k, holding `tag_count_lock` — and every other
            // job's fold-in — hostage the whole time.
            for (deltas, decrement) in [(&add_deltas, false), (&del_deltas, true)] {
                let entries: Vec<(u64, u64)> = deltas.iter().map(|(&k, &v)| (k, v)).collect();
                for piece in entries.chunks(POPULARITY_FOLD_CHUNK) {
                    // `piece` is already a sorted-by-nothing slice of
                    // (id, delta); building a HashMap per piece just to iterate
                    // it twice cost an allocation and 100 rehashes per piece.
                    let (sql, params) = tag_count_update_sql(piece, decrement);
                    // Work out which of this piece actually needs a shadow
                    // write BEFORE the count moves (the test derives the
                    // post-update threshold from the current count). In steady
                    // state the answer is "none" — a tag only leaves the
                    // popular set when its count crosses 5, which is rare —
                    // and skipping the two shadow statements then removes
                    // ~40% of this writer's work, which matters more than the
                    // raw saving because every other job's fold-in queues
                    // behind `tag_count_lock` here. Measured on 4800 deltas
                    // whose tags were all already popular: 1480 -> 553 us per
                    // tag-sync. The filter is exact, so a genuine crossing (or
                    // a shadow that has drifted) still gets written.
                    let shadow_ids = self.tags_needing_shadow_sync(&tx, piece, decrement).await?;
                    tx.execute(sql, params_from_iter(params)).await?;
                    // Mirrors popularity into the FTS shadow in the same
                    // transaction, so a count that crossed the threshold
                    // becomes (or stops being) searchable atomically with the
                    // count itself. Idempotent, so piece-wise is equivalent
                    // to the old single unioned call.
                    if !shadow_ids.is_empty() {
                        self.sync_tags_popular(&tx, &shadow_ids).await?;
                    }
                }
            }
            tx.commit().await
        })
        .await
    }
    pub(in crate::db::turso) async fn relationship_delete(
        &self,
        conn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) -> Result<()> {
        let Some(namespace_id) = self.tag_namespace_id(conn, tag_id).await? else {
            return Ok(());
        };
        let sql =
            format!("DELETE FROM Relationship_{namespace_id} WHERE file_id = ?1 AND tag_id = ?2;");
        let deleted = conn.execute(sql, (file_id as i64, tag_id as i64)).await?;
        if deleted > 0 {
            conn.execute(
                "UPDATE Tags SET count = MAX(count - 1, 0) WHERE id = ?1;",
                (tag_id as i64,),
            )
            .await?;
            self.sync_tags_popular(conn, &[tag_id]).await?;
        }
        Ok(())
    }

    /// Gets `(file_id, tag_id)` pairs for a batch of file ids.
    pub(in crate::db::turso) async fn file_id_get_tag_ids_bulk(
        &self,
        conn: &Connection,
        file_ids: &[u64],
    ) -> Result<HashMap<u64, HashSet<u64>>> {
        let mut out: HashMap<u64, HashSet<u64>> = HashMap::new();
        if file_ids.is_empty() {
            return Ok(out);
        }

        for (sql, params) in self
            .build_file_tag_lookup_statements(conn, file_ids)
            .await?
        {
            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let f_id: u64 = row.get(0)?;
                let t_id: u64 = row.get(1)?;
                out.entry(f_id).or_default().insert(t_id);
            }
        }

        Ok(out)
    }

    /// The statements `file_id_get_tag_ids_bulk` runs, as `(sql, params)`.
    ///
    /// Split out so the hot path's SQL is observable: the pushdown lives in the
    /// *shape* of the statement, and a test that rebuilt its own SQL would not
    /// notice the function going back to the scanning form.
    pub(in crate::db::turso) async fn build_file_tag_lookup_statements(
        &self,
        conn: &Connection,
        file_ids: &[u64],
    ) -> Result<Vec<(String, Vec<Value>)>> {
        let mut out = Vec::new();
        // The `file_id IN (...)` filter is pushed into every namespace arm (see
        // `relationship_union_source`), so each partition is probed through
        // `idx_Relationship_{ns}_tag_file` instead of being scanned. Numbered
        // placeholders let the same list be repeated per arm for one binding,
        // and `pushed_union_batch_size` keeps each statement small enough to
        // parse cheaply.
        //
        // The namespace list is fetched once and reused for both the arm count
        // and every chunk's text, so a multi-chunk lookup still costs one
        // namespace query in total.
        let tables = relationship_union_tables(conn).await;
        let batch = pushed_union_batch_size(tables.len());
        for chunk in file_ids.chunks(batch) {
            let predicate = format!("file_id IN ({})", numbered_placeholders(chunk.len()));
            let source = union_source_from_tables(&tables, "r", Some(&predicate));
            let sql = format!("SELECT file_id, tag_id FROM {source}");
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();
            out.push((sql, params));
        }
        Ok(out)
    }

    /// Checks if the parent structure defined inside a single `PluginTag` exists.
    pub(in crate::db::turso) async fn parent_structure_exists(
        &self,
        conn: &Connection,
        plugin_tag: &PluginTag,
    ) -> Result<bool> {
        let Some(relation_ctx) = &plugin_tag.relates_to else {
            return Ok(false);
        };

        let Some(child_id) = self.tag_id_by_name_ns(conn, &plugin_tag.tag).await? else {
            return Ok(false);
        };
        let Some(parent_id) = self.tag_id_by_name_ns(conn, &relation_ctx.tag).await? else {
            return Ok(false);
        };
        let limit_to_id = match &relation_ctx.limit_to {
            Some(lim_tag) => self.tag_id_by_name_ns(conn, lim_tag).await?,
            None => None,
        };

        let mut rows = conn
            .query(
                "SELECT 1
                 FROM Parents
                 WHERE tag_id = ?1
                   AND relate_tag_id = ?2
                   AND (
                     (?3 IS NULL AND limit_to IS NULL) OR
                     (limit_to = ?3)
                   )
                 LIMIT 1;",
                (
                    child_id as i64,
                    parent_id as i64,
                    limit_to_id.map(|id| id as i64),
                ),
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Checks if a `relate_to`/`limit_to` pair is already declared.
    pub(in crate::db::turso) async fn parent_relate_limit_exists(
        &self,
        conn: &Connection,
        relate_to: &Tag,
        limit_to: &Tag,
    ) -> Result<bool> {
        let Some(relate_id) = self.tag_id_by_name_ns(conn, relate_to).await? else {
            return Ok(false);
        };
        let Some(limit_id) = self.tag_id_by_name_ns(conn, limit_to).await? else {
            return Ok(false);
        };

        let mut rows = conn
            .query(
                "SELECT 1
                 FROM Parents
                 WHERE relate_tag_id = ?1 AND limit_to = ?2
                 LIMIT 1;",
                (relate_id as i64, limit_id as i64),
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Resolves a tag's id by name + namespace name.
    async fn tag_id_by_name_ns(&self, conn: &Connection, tag: &Tag) -> Result<Option<u64>> {
        let mut rows = conn
            .query(
                "SELECT t.id
                 FROM Tags t
                 JOIN Namespace n ON t.namespace = n.id
                 WHERE t.name = ?1 AND n.name = ?2
                 LIMIT 1;",
                (tag.name.as_str(), tag.namespace.name.as_str()),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }
}

/// Builds a single `UPDATE Tags SET count = ... CASE id WHEN ? THEN ? ... END
/// WHERE id IN (...)`, bumping every tag's count by its aggregated delta in
/// one round trip instead of one UPDATE per tag. `decrement` clamps the count
/// at zero via `MAX(count - delta, 0)`.
fn tag_count_update_sql(deltas: &[(u64, u64)], decrement: bool) -> (String, Vec<Value>) {
    let mut clauses = Vec::with_capacity(deltas.len());
    let mut params = Vec::with_capacity(deltas.len() * 3);
    for (tag_id, delta) in deltas {
        clauses.push("WHEN ? THEN ?".to_string());
        params.push(Value::from(*tag_id as i64));
        params.push(Value::from(*delta as i64));
    }
    let placeholders = std::iter::repeat_n("?", deltas.len())
        .collect::<Vec<_>>()
        .join(", ");
    for (tag_id, _) in deltas {
        params.push(Value::from(*tag_id as i64));
    }
    let expression = if decrement {
        format!("MAX(count - CASE id {} ELSE 0 END, 0)", clauses.join(" "))
    } else {
        format!("count + CASE id {} ELSE 0 END", clauses.join(" "))
    };
    (
        format!("UPDATE Tags SET count = {expression} WHERE id IN ({placeholders});"),
        params,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_types::{FileInternal, GenericNamespaceObj, Tag};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use crate::db::turso::TagDb;

    async fn new_test_db() -> Arc<TursoDatabase> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    }

    // ---------------------------------------------------------------------
    // Relationship write-path scale bench.
    //
    // Isolates the three changes made to the bulk relationship paths:
    //
    //  * `relationship_bulk_delete` groups by tag and deletes with
    //    `tag_id = ? AND file_id IN (...)`. The previous per-relationship
    //    `(file_id = ? AND tag_id = ?) OR ...` form hit limbo's 100-deep
    //    expression limit at around a hundred rows, so any `Del`/`Set` over
    //    ~100 relationships raised `Parse error: Expression tree is too large`
    //    instead of deleting. This bench drives far past that.
    //  * Both bulk paths resolve each *distinct* tag's namespace once instead
    //    of once per relationship, which only shows up when tags repeat — so
    //    the shape here is deliberately tag-heavy (few tags, many files).
    //  * `tag_counts_apply` no longer clones both delta maps inside
    //    `tag_count_lock`, and builds its per-piece `CASE` list from a slice
    //    rather than a fresh `HashMap`.
    //
    // Env: REL_FILES (default 20000), REL_TAGS (default 200),
    // REL_TAGS_PER_FILE (default 5), REL_DELETES (default 20000). Writes go
    // through the real production functions inside a transaction, exactly as the
    // scraper's relationship phase does.
    // ---------------------------------------------------------------------

    fn rel_env(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    #[test]
    #[ignore = "manual benchmark: relationship bulk add/delete at tag-heavy scale"]
    fn relationship_write_path_scale_bench() {
        let files = rel_env("REL_FILES", 20_000);
        let tags = rel_env("REL_TAGS", 200);
        let deletes = rel_env("REL_DELETES", 20_000);

        let db_path = {
            let dir = if std::path::Path::new("/dev/shm").exists() {
                std::path::PathBuf::from("/dev/shm")
            } else {
                std::env::temp_dir()
            };
            dir.join(format!("intscrape-relbench-{}.db", std::process::id()))
        };
        for suffix in ["", "-log", "-wal", "-shm"] {
            let mut p = db_path.as_os_str().to_os_string();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
            let conn = db.connect().unwrap();

            let ns = GenericNamespaceObj {
                name: "relbench".into(),
                description: None,
            };
            db.namespace_ensure_set(&HashSet::from([ns.clone()]))
                .await
                .unwrap();

            // Few tags, many files: the shape where a `Set` re-tag or a bulk
            // delete touches the same handful of tags thousands of times.
            let mut tag_ids: Vec<u64> = Vec::new();
            for batch_start in (0..tags).step_by(200) {
                let batch: HashSet<Tag> = (batch_start..(batch_start + 200).min(tags))
                    .map(|t| Tag {
                        name: format!("relbenchtag{t}"),
                        namespace: ns.clone(),
                    })
                    .collect();
                let set = db.tag_add_bulk(&conn, &batch).await.unwrap();
                tag_ids.extend(set.iter().map(|t| t.id as u64));
            }
            tag_ids.sort_unstable();
            tag_ids.dedup();
            assert_eq!(tag_ids.len() as u64, tags);

            let storage_id = db
                .file_storage_location_get_or_create(&conn, "relbench_storage")
                .await
                .unwrap();
            let mut file_ids: Vec<u64> = Vec::new();
            for batch_start in (0..files).step_by(1000) {
                let end = (batch_start + 1000).min(files);
                let objs: Vec<FileInternal> = (batch_start..end)
                    .map(|i| FileInternal {
                        id: None,
                        hash: format!("relbenchhash{i:09}"),
                        extension: "jpg".into(),
                        storage_id,
                        size_bytes: Some(1),
                    })
                    .collect();
                let inserted = db.file_add_bulk(&conn, &objs).await.unwrap();
                file_ids.extend(inserted.iter().map(|f| f.id.unwrap()));
            }
            eprintln!("RELBENCH seeded {files} files, {tags} tags, 1 namespace");

            // Each file carries a handful of tags drawn from the whole pool, so
            // the set has far more relationships than distinct tags: the shape
            // the namespace-resolution dedupe targets.
            let per_file = rel_env("REL_TAGS_PER_FILE", 5).max(1) as usize;
            let mut relationships: HashSet<(u64, u64)> = HashSet::new();
            for (index, file_id) in file_ids.iter().enumerate() {
                for k in 0..per_file {
                    let tag = tag_ids[(index + k * 7) % tag_ids.len()];
                    relationships.insert((*file_id, tag));
                }
            }
            eprintln!(
                "RELBENCH {} relationships over {} distinct tags",
                relationships.len(),
                tag_ids.len()
            );

            // Phase 1: bulk add.
            let mut add_conn = db.connect().unwrap();
            let add_tx = add_conn
                .transaction_with_behavior(TursoDatabase::write_tx_behavior())
                .await
                .unwrap();
            let started = std::time::Instant::now();
            let add_deltas = db
                .relationships_bulk_add(&add_tx, &relationships)
                .await
                .unwrap();
            let add_s = started.elapsed();
            let commit_started = std::time::Instant::now();
            add_tx.commit().await.unwrap();
            let add_commit_s = commit_started.elapsed();
            println!(
                "RELBENCH relationships_bulk_add  {:>8.1} ms + commit {:>7.1} ms \
                 ({} rows, {} deltas, {:.2} us/row)",
                add_s.as_secs_f64() * 1000.0,
                add_commit_s.as_secs_f64() * 1000.0,
                relationships.len(),
                add_deltas.len(),
                add_s.as_secs_f64() * 1e6 / relationships.len() as f64
            );

            // Phase 2: count fold-in (the serialized writer).
            let started = std::time::Instant::now();
            db.tag_counts_apply(&add_deltas, &HashMap::new())
                .await
                .unwrap();
            println!(
                "RELBENCH tag_counts_apply        {:>8.1} ms ({} deltas)",
                started.elapsed().as_secs_f64() * 1000.0,
                add_deltas.len()
            );

            // Phase 3: bulk delete, far past the old ~100-row ceiling.
            // Delete a slice of the set, so the rows really do exist and the
            // reported deltas must account for all of them.
            let doomed: HashSet<(u64, u64)> = relationships
                .iter()
                .copied()
                .take(deletes.min(relationships.len() as u64) as usize)
                .collect();
            let mut del_conn = db.connect().unwrap();
            let del_tx = del_conn
                .transaction_with_behavior(TursoDatabase::write_tx_behavior())
                .await
                .unwrap();
            let started = std::time::Instant::now();
            let del_deltas = db.relationship_bulk_delete(&del_tx, &doomed).await.unwrap();
            let del_s = started.elapsed();
            del_tx.commit().await.unwrap();
            println!(
                "RELBENCH relationship_bulk_delete{:>8.1} ms ({} rows, {} deltas, {:.2} us/row)",
                del_s.as_secs_f64() * 1000.0,
                doomed.len(),
                del_deltas.len(),
                del_s.as_secs_f64() * 1e6 / doomed.len().max(1) as f64
            );

            let started = std::time::Instant::now();
            db.tag_counts_apply(&HashMap::new(), &del_deltas)
                .await
                .unwrap();
            println!(
                "RELBENCH tag_counts_apply (del)  {:>8.1} ms ({} deltas)",
                started.elapsed().as_secs_f64() * 1000.0,
                del_deltas.len()
            );

            // Sanity: the plan must still be a covering-index seek per tag, and
            // must not degrade into a partition scan.
            let mut del_conn = db.connect().unwrap();
            let sql = format!(
                "DELETE FROM Relationship_1 WHERE tag_id = ? AND file_id IN (?, ?) RETURNING tag_id"
            );
            let mut rows = del_conn
                .query(
                    &format!("EXPLAIN QUERY PLAN {sql}"),
                    turso::params_from_iter(vec![
                        turso::Value::from(tag_ids[0] as i64),
                        turso::Value::from(1i64),
                        turso::Value::from(2i64),
                    ]),
                )
                .await
                .unwrap();
            let mut steps = Vec::new();
            while let Some(row) = rows.next().await.unwrap() {
                steps.push(row.get::<String>(3).unwrap());
            }
            let joined = steps.join(" | ");
            println!("RELBENCH delete plan: {joined}");
            assert!(
                !joined.contains("SCAN relationship"),
                "grouped delete must not scan the partition: {joined}"
            );

            drop(del_conn);
            drop(conn);
            db.shutdown().await;
        });

        for suffix in ["", "-log", "-wal", "-shm"] {
            let mut p = db_path.as_os_str().to_os_string();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }
    }

    /// The union source must push its filter into every arm.
    ///
    /// Limbo does not push a predicate through `UNION ALL`, so leaving the
    /// `file_id IN (...)` outside made every file->tag lookup read *every*
    /// relationship row in the database, even though each partition carries an
    /// index on `file_id`. Measured on 1M relationships across 32 partitions,
    /// looking up 200 files: 3,301 ms -> 60 ms.
    ///
    /// This pins both halves: the plan must stay index-driven, and the pushed
    /// form must return exactly what the unfiltered union scan would.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_to_tag_lookup_pushes_its_filter_into_every_namespace_arm() {
        const FILES: u64 = 300;
        const NAMESPACES: [&str; 3] = ["alpha", "beta", "gamma"];

        let db = new_test_db().await;
        let conn = db.connect().unwrap();

        let mut ns_set = HashSet::new();
        for name in NAMESPACES {
            ns_set.insert(GenericNamespaceObj {
                name: name.into(),
                description: None,
            });
        }
        db.namespace_ensure_set(&ns_set).await.unwrap();

        let storage_id = db
            .file_storage_location_get_or_create(&conn, "test_storage")
            .await
            .unwrap();
        let file_objs: Vec<FileInternal> = (0..FILES)
            .map(|i| FileInternal {
                id: None,
                hash: format!("f2thash{i:05}"),
                extension: "jpg".into(),
                storage_id,
                size_bytes: Some(1),
            })
            .collect();
        let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
        let file_ids: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();

        // Spread tags across all three namespaces, several per file.
        let mut rels: HashSet<(u64, u64)> = HashSet::new();
        for (index, file_id) in file_ids.iter().enumerate() {
            let ns_name = NAMESPACES[index % NAMESPACES.len()];
            let tags: HashSet<Tag> = (0..3)
                .map(|k| Tag {
                    name: format!("{ns_name}-f2t-{k}"),
                    namespace: GenericNamespaceObj {
                        name: ns_name.into(),
                        description: None,
                    },
                })
                .collect();
            let set = db.tag_add_bulk(&conn, &tags).await.unwrap();
            for tag in set.iter() {
                rels.insert((*file_id, tag.id as u64));
            }
        }
        let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
        db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

        // A scattered subset, so the lookup is not a contiguous range.
        let wanted: Vec<u64> = file_ids
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 7 == 3)
            .map(|(_, id)| *id)
            .collect();
        assert!(
            wanted.len() > 20,
            "need a multi-namespace lookup to be meaningful"
        );

        let pushed = db.file_id_get_tag_ids_bulk(&conn, &wanted).await.unwrap();

        // Reference: the same lookup expressed as one unfiltered scan per
        // namespace, which is what the union used to degenerate into.
        let mut expected: HashMap<u64, HashSet<u64>> = HashMap::new();
        let unfiltered = self_unfiltered_source(&conn).await;
        for chunk in wanted.chunks(SQL_CHUNK_SIZE) {
            let ph = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT file_id, tag_id FROM {unfiltered} WHERE file_id IN ({ph})");
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();
            let mut rows = conn.query(&sql, params_from_iter(params)).await.unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                let f: u64 = row.get(0).unwrap();
                let t: u64 = row.get(1).unwrap();
                expected.entry(f).or_default().insert(t);
            }
        }
        assert_eq!(
            pushed, expected,
            "pushing the filter into each arm must not change the result"
        );

        // The plan the hot path actually runs. Asserted on the statement the
        // function itself produces, so reverting it to the scanning form fails
        // here rather than passing on a separately-built statement.
        for (sql, params) in db
            .build_file_tag_lookup_statements(&conn, &wanted)
            .await
            .unwrap()
        {
            let mut rows = conn
                .query(
                    &format!("EXPLAIN QUERY PLAN {sql}"),
                    params_from_iter(params),
                )
                .await
                .unwrap();
            let mut steps: Vec<String> = Vec::new();
            while let Some(row) = rows.next().await.unwrap() {
                steps.push(row.get::<String>(3).unwrap());
            }
            let joined = steps.join(" | ");
            eprintln!("PLAN: {joined}");
            assert!(
                !joined.contains("SCAN relationship"),
                "every namespace arm must be index-probed, not scanned: {joined}"
            );
        }

        // And show the shape this replaced, so the difference stays visible.
        let predicate = format!("file_id IN ({})", numbered_placeholders(wanted.len()));
        let unpushed = db
            .relationship_union_source(&conn, "r", None)
            .await
            .unwrap();
        let mut rows = conn
            .query(
                &format!(
                    "EXPLAIN QUERY PLAN SELECT file_id, tag_id FROM {unpushed} WHERE {predicate}"
                ),
                params_from_iter(
                    wanted
                        .iter()
                        .map(|id| Value::from(*id as i64))
                        .collect::<Vec<Value>>(),
                ),
            )
            .await
            .unwrap();
        let mut steps: Vec<String> = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            steps.push(row.get::<String>(3).unwrap());
        }
        let old_plan = steps.join(" | ");
        eprintln!("PLAN (pre-fix shape): {old_plan}");
        assert!(
            old_plan.contains("SCAN relationship"),
            "the filter-outside-the-union shape is expected to scan; if limbo ever \
             learns to push through UNION ALL the pushdown can be dropped: {old_plan}"
        );

        drop(conn);
        db.shutdown().await;
    }

    /// The union source with no pushed predicate: the pre-optimization shape,
    /// used here only as a correctness reference.
    async fn self_unfiltered_source(conn: &Connection) -> String {
        let mut tables = Vec::new();
        let mut rows = conn
            .query("SELECT id FROM Namespace ORDER BY id;", ())
            .await
            .unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            let id: i64 = row.get(0).unwrap();
            tables.push(format!("Relationship_{id}"));
        }
        format!(
            "({}) AS r",
            tables
                .iter()
                .map(|t| format!("SELECT file_id, tag_id FROM {t}"))
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        )
    }

    async fn tag_count(db: &TursoDatabase, conn: &Connection, tag_id: u64) -> i64 {
        let mut rows = conn
            .query("SELECT count FROM Tags WHERE id = ?1;", (tag_id as i64,))
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        row.get(0).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bulk_relationship_add_and_delete_aggregate_tag_counts() {
        let db = new_test_db().await;
        let conn = db.connect().unwrap();

        // Fresh namespace + tag (also creates the Relationship_{ns} partition).
        let tags: HashSet<Tag> = HashSet::from([Tag {
            name: "mammal".into(),
            namespace: GenericNamespaceObj {
                name: "species".into(),
                description: None,
            },
        }]);
        let tag_db_set = db.tag_add_bulk(&conn, &tags).await.unwrap();
        let tag_db: &TagDb = tag_db_set.iter().next().unwrap();
        let tag_id = tag_db.id as u64;
        assert_eq!(tag_count(&db, &conn, tag_id).await, 0);

        // Two files, both related to the same tag.
        let storage_id = db
            .file_storage_location_get_or_create(&conn, "test_storage")
            .await
            .unwrap();
        let files = db
            .file_add_bulk(
                &conn,
                &[
                    FileInternal {
                        id: None,
                        hash: "aaahash1".into(),
                        extension: "jpg".into(),
                        storage_id,
                        size_bytes: Some(1),
                    },
                    FileInternal {
                        id: None,
                        hash: "aaahash2".into(),
                        extension: "jpg".into(),
                        storage_id,
                        size_bytes: Some(2),
                    },
                ],
            )
            .await
            .unwrap();
        let file_ids: Vec<u64> = files.iter().map(|file| file.id.unwrap()).collect();
        assert_eq!(file_ids.len(), 2);

        let relationships: HashSet<(u64, u64)> =
            file_ids.iter().map(|file_id| (*file_id, tag_id)).collect();
        // The bulk add returns deltas instead of applying counts inline;
        // production folds them in after commit via `tag_counts_apply`.
        let add_deltas = db
            .relationships_bulk_add(&conn, &relationships)
            .await
            .unwrap();
        db.tag_counts_apply(&add_deltas, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            tag_count(&db, &conn, tag_id).await,
            2,
            "two relationships must increment the count twice"
        );

        let del_deltas = db
            .relationship_bulk_delete(&conn, &relationships)
            .await
            .unwrap();
        db.tag_counts_apply(&HashMap::new(), &del_deltas)
            .await
            .unwrap();
        assert_eq!(tag_count(&db, &conn, tag_id).await, 0);
    }

    /// `relationship_bulk_delete` used to build one
    /// `WHERE (file_id = ? AND tag_id = ?) OR ...` term per relationship.
    /// Limbo caps expression depth at 100, so the statement failed with
    /// `Parse error: Expression tree is too large` at around a hundred rows —
    /// far below `SQL_CHUNK_SIZE`, meaning any `TagOperation::Del` or `Set`
    /// touching more than ~100 relationships errored out instead of deleting.
    ///
    /// The fix groups by tag and deletes with `tag_id = ? AND file_id IN (...)`,
    /// which the planner serves as a covering-index seek on the
    /// `(tag_id, file_id)` primary key. This pins both halves: a delete set far
    /// past the old depth limit must succeed, and the reported deltas must
    /// still be exact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bulk_delete_handles_sets_far_past_the_expression_depth_limit() {
        const FILES: u64 = 400;
        const TAGS: u64 = 4;

        let db = new_test_db().await;
        let conn = db.connect().unwrap();

        let mut tag_ids: Vec<u64> = Vec::new();
        for t in 0..TAGS {
            let tags: HashSet<Tag> = HashSet::from([Tag {
                name: format!("bulktag{t}"),
                namespace: GenericNamespaceObj {
                    name: "subject".into(),
                    description: None,
                },
            }]);
            let set = db.tag_add_bulk(&conn, &tags).await.unwrap();
            tag_ids.push(set.iter().next().unwrap().id as u64);
        }
        let storage_id = db
            .file_storage_location_get_or_create(&conn, "test_storage")
            .await
            .unwrap();
        let file_objs: Vec<FileInternal> = (0..FILES)
            .map(|i| FileInternal {
                id: None,
                hash: format!("bulkhash{i:05}"),
                extension: "jpg".into(),
                storage_id,
                size_bytes: Some(1),
            })
            .collect();
        let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
        let file_ids: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();

        // Every file carries every tag: FILES * TAGS = 1600 relationships, well
        // past the old ~100 ceiling, and spread over only TAGS distinct tags so
        // the grouped statement collapses to TAGS statements.
        let relationships: HashSet<(u64, u64)> = file_ids
            .iter()
            .flat_map(|file_id| tag_ids.iter().map(move |tag_id| (*file_id, *tag_id)))
            .collect();
        assert_eq!(relationships.len() as u64, FILES * TAGS);

        let add_deltas = db
            .relationships_bulk_add(&conn, &relationships)
            .await
            .unwrap();
        db.tag_counts_apply(&add_deltas, &HashMap::new())
            .await
            .unwrap();
        for tag_id in &tag_ids {
            assert_eq!(
                tag_count(&db, &conn, *tag_id).await,
                FILES as i64,
                "every file carries this tag"
            );
        }

        // Delete half of them in one call: 800 relationships, 4 distinct tags.
        let doomed: HashSet<(u64, u64)> = file_ids
            .iter()
            .take(FILES as usize / 2)
            .flat_map(|file_id| tag_ids.iter().map(move |tag_id| (*file_id, *tag_id)))
            .collect();
        let del_deltas = db.relationship_bulk_delete(&conn, &doomed).await.unwrap();
        assert_eq!(
            del_deltas.values().sum::<u64>(),
            doomed.len() as u64,
            "deltas must account for every deleted relationship exactly once"
        );
        db.tag_counts_apply(&HashMap::new(), &del_deltas)
            .await
            .unwrap();
        for tag_id in &tag_ids {
            assert_eq!(
                tag_count(&db, &conn, *tag_id).await,
                (FILES / 2) as i64,
                "half the files removed"
            );
        }

        // The survivors must still be there, and the deletes must be idempotent:
        // re-deleting the same set reports nothing.
        let remaining: i64 = {
            let mut rows = conn
                .query("SELECT COUNT(*) FROM Relationship_1;", ())
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        };
        assert_eq!(
            remaining,
            (FILES / 2) as i64 * TAGS as i64,
            "only the undelivered half should remain"
        );

        let again = db.relationship_bulk_delete(&conn, &doomed).await.unwrap();
        assert!(
            again.is_empty(),
            "re-deleting an already-removed set must report no deltas"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn relationship_activity_crosses_popularity_threshold_both_ways() {
        let db = new_test_db().await;
        let conn = db.connect().unwrap();

        let tags: HashSet<Tag> = HashSet::from([Tag {
            name: "red fox".into(),
            namespace: GenericNamespaceObj {
                name: "subject".into(),
                description: None,
            },
        }]);
        let tag_db_set = db.tag_add_bulk(&conn, &tags).await.unwrap();
        let tag_db: &TagDb = tag_db_set.iter().next().unwrap();
        let tag_id = tag_db.id as u64;

        // The FTS index only covers `count >= 5`: four relationships (count 4)
        // must stay unsearchable, the fifth crossing to 5 becomes searchable,
        // and one delete (back to 4) drops it again.
        for file_id in 1_u64..=4 {
            db.relationship_add(&conn, file_id, tag_id).await.unwrap();
        }
        assert_eq!(
            db.tags_search_fts("red f", 10).await.unwrap(),
            Vec::new(),
            "count 4 must not be searchable"
        );

        db.relationship_add(&conn, 5_u64, tag_id).await.unwrap();
        let found = db.tags_search_fts("red f", 10).await.unwrap();
        assert_eq!(
            found.len(),
            1,
            "count 5 must cross into the popular FTS index"
        );

        db.relationship_delete(&conn, 5_u64, tag_id).await.unwrap();
        assert_eq!(
            db.tags_search_fts("red f", 10).await.unwrap(),
            Vec::new(),
            "count 4 must drop back out of the popular FTS index"
        );
    }

    /// `tag_counts_apply` no longer re-asserts the whole delta batch into the
    /// search shadow: it pre-reads which ids can actually change shadow
    /// membership and writes only those. This pins that the filter is an exact
    /// substitute, not a heuristic — every case where the shadow *should* move
    /// must still move it, including the two the old unconditional sync got for
    /// free (a delta that jumps clean over the threshold, and repairing a
    /// shadow that has drifted out of sync on its own).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn count_fold_in_only_writes_shadow_when_membership_changes() {
        let db = new_test_db().await;
        let conn = db.connect().unwrap();

        async fn tag_id_for(db: &TursoDatabase, conn: &Connection, name: &str) -> u64 {
            let tags: HashSet<Tag> = HashSet::from([Tag {
                name: name.into(),
                namespace: GenericNamespaceObj {
                    name: "subject".into(),
                    description: None,
                },
            }]);
            let set = db.tag_add_bulk(conn, &tags).await.unwrap();
            set.iter().next().unwrap().id as u64
        }
        async fn count_of(conn: &Connection, tag_id: u64) -> i64 {
            let mut rows = conn
                .query("SELECT count FROM Tags WHERE id = ?1;", (tag_id as i64,))
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        }

        let gradual = tag_id_for(&db, &conn, "gradual").await;
        // Climb to exactly the threshold one delta at a time.
        db.tag_counts_apply(&HashMap::from([(gradual, 4)]), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(count_of(&conn, gradual).await, 4);
        assert!(
            db.tags_search_fts("gradual", 10).await.unwrap().is_empty(),
            "count 4 must stay out of the shadow"
        );

        // +1 crosses 4 -> 5: the filter must let this one through.
        db.tag_counts_apply(&HashMap::from([(gradual, 1)]), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            db.tags_search_fts("gradual", 10).await.unwrap().len(),
            1,
            "crossing up to the threshold must become searchable"
        );

        // No crossing (+1, 5 -> 6) must not break or duplicate anything.
        db.tag_counts_apply(&HashMap::from([(gradual, 1)]), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            db.tags_search_fts("gradual", 10).await.unwrap().len(),
            1,
            "a non-crossing increment must leave the tag searchable"
        );

        // Crossing back down 6 -> 4 must drop it out again.
        db.tag_counts_apply(&HashMap::new(), &HashMap::from([(gradual, 2)]))
            .await
            .unwrap();
        assert_eq!(count_of(&conn, gradual).await, 4);
        assert!(
            db.tags_search_fts("gradual", 10).await.unwrap().is_empty(),
            "crossing down below the threshold must leave the shadow"
        );

        // A single delta that jumps clean over the threshold (0 -> 9) in one
        // step is the case a naive "did it move by one?" check would miss.
        let jumper = tag_id_for(&db, &conn, "jumper").await;
        db.tag_counts_apply(&HashMap::from([(jumper, 9)]), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(count_of(&conn, jumper).await, 9);
        assert_eq!(
            db.tags_search_fts("jumper", 10).await.unwrap().len(),
            1,
            "a delta that jumps past the threshold must become searchable"
        );

        // A decrement larger than the count clamps at 0 and must also leave
        // the shadow.
        db.tag_counts_apply(&HashMap::new(), &HashMap::from([(jumper, 100)]))
            .await
            .unwrap();
        assert_eq!(count_of(&conn, jumper).await, 0);
        assert!(
            db.tags_search_fts("jumper", 10).await.unwrap().is_empty(),
            "a clamped decrement to zero must leave the shadow"
        );

        // Drift repair: a popular tag whose shadow row vanished (interrupted
        // run, manual edit) must be restored even though the incoming delta
        // does not cross the threshold. This is the property that keeps the
        // filter equivalent to the old unconditional sync.
        let drifted = tag_id_for(&db, &conn, "drifted").await;
        db.tag_counts_apply(&HashMap::from([(drifted, 7)]), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            db.tags_search_fts("drifted", 10).await.unwrap().len(),
            1,
            "setup: drifted tag must start searchable"
        );
        conn.execute(
            "DELETE FROM Tags_Popular WHERE tag_id = ?1;",
            (drifted as i64,),
        )
        .await
        .unwrap();
        assert!(
            db.tags_search_fts("drifted", 10).await.unwrap().is_empty(),
            "setup: shadow row must actually be gone"
        );
        // Non-crossing delta (7 -> 8) that must nonetheless repair the shadow.
        db.tag_counts_apply(&HashMap::from([(drifted, 1)]), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            db.tags_search_fts("drifted", 10).await.unwrap().len(),
            1,
            "a non-crossing delta must still repair a drifted shadow row"
        );

        // And the mirror-image drift: a below-threshold tag wrongly left in
        // the shadow must be evicted.
        let stray = tag_id_for(&db, &conn, "straypop").await;
        conn.execute(
            "INSERT OR IGNORE INTO Tags_Popular(tag_id, name)
             SELECT id, name FROM Tags WHERE id = ?1;",
            (stray as i64,),
        )
        .await
        .unwrap();
        assert_eq!(
            db.tags_search_fts("straypop", 10).await.unwrap().len(),
            1,
            "setup: stray shadow row must be searchable"
        );
        // 0 -> 1, still below the threshold: the shadow row must be evicted.
        db.tag_counts_apply(&HashMap::from([(stray, 1)]), &HashMap::new())
            .await
            .unwrap();
        assert!(
            db.tags_search_fts("straypop", 10).await.unwrap().is_empty(),
            "a non-crossing delta must still evict a wrongly-present shadow row"
        );
    }
}
