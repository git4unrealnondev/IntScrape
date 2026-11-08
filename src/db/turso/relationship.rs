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

/// Decodes the stored `limit_to` value back into the API's `Option<u64>`.
///
/// The `Parents` schema stores "no limit tag" as the `-1` sentinel (tag ids
/// are rowids and start at 1, so `-1` is unambiguous) rather than SQL `NULL`,
/// which lets the dedupe index be a plain composite key. Every `Parents` read
/// funnels through this one function, so the ABI-facing `Option` never leaks
/// the storage encoding.
fn parents_limit_decode(stored: Option<i64>) -> Option<u64> {
    match stored {
        None | Some(-1) => None,
        Some(id) => Some(id as u64),
    }
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
        // No namespaces -> no relationship partitions -> no relationships.
        // (Also avoids binding ?1 against the placeholder-free WHERE 0 source.)
        if relationship_union_tables(conn).await.is_empty() {
            return Ok(HashSet::new());
        }
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
        // No namespaces -> no relationship partitions -> no relationships.
        if relationship_union_tables(conn).await.is_empty() {
            return Ok(HashSet::new());
        }
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

    /// Gets every child-parent relation whose `limit_to` qualifier is
    /// `limit_tag_id`.
    ///
    /// This is the reverse index of a qualified relationship: value-style tags
    /// (a post comment relating to its post id) share one edge shape
    /// `(child -> parent, limited to qualifier)`, so querying by the qualifier
    /// groups those values back by their context — every post relationship
    /// that belongs to one 4chan thread, for example.
    pub(in crate::db::turso) async fn limited_child_relationships_get(
        &self,
        conn: &Connection,
        limit_tag_id: u64,
    ) -> Result<Vec<TagParents>> {
        self.parents_by_column(conn, "limit_to", limit_tag_id)
            .await
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
                limit_to: parents_limit_decode(row.get(2)?),
            }))
        } else {
            Ok(None)
        }
    }

    /// Shared `Parents` query filtered by either the child (`tag_id`) or
    /// parent (`relate_tag_id`) column, decoded from the `-1` sentinel back
    /// into the API's `Option<u64>`.
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
                limit_to: parents_limit_decode(row.get(2)?),
            });
        }

        Ok(out)
    }

    /// Gets the child-parent relations whose `limit_to` qualifier is any of
    /// the supplied tags, in one pass over the `idx_parents_lim` index. This
    /// is the bulk form of `limited_child_relationships_get`: a page of 50
    /// threads is one chunked `IN (...)` read instead of one query per
    /// thread. Results are not grouped; callers bucket rows by `limit_to`.
    pub(in crate::db::turso) async fn limited_child_relationships_get_many(
        &self,
        conn: &Connection,
        limit_tag_ids: &HashSet<u64>,
    ) -> Result<HashMap<u64, Vec<TagParents>>> {
        let mut out: HashMap<u64, Vec<TagParents>> = HashMap::new();
        if limit_tag_ids.is_empty() {
            return Ok(out);
        }

        let ids: Vec<i64> = limit_tag_ids.iter().map(|&id| id as i64).collect();
        for chunk in ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT tag_id, relate_tag_id, limit_to
                 FROM Parents
                 WHERE limit_to IN ({placeholders});"
            );

            let mut rows = conn.query(&sql, params_from_iter(chunk.to_vec())).await?;
            while let Some(row) = rows.next().await? {
                let limit_tag_id: i64 = row.get(2)?;
                out.entry(limit_tag_id as u64).or_default().push(TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: parents_limit_decode(Some(limit_tag_id)),
                });
            }
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
        // With no namespaces there are no relationship partitions, so no
        // relationships can exist. Returning no statements also avoids
        // binding params against the placeholder-free `WHERE 0` source.
        if tables.is_empty() {
            return Ok(out);
        }
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
                   AND limit_to = IFNULL(?3, -1)
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
mod tests;
