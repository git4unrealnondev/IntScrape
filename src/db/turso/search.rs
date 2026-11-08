use std::collections::{HashMap, HashSet};

use shared_types::{SearchHolder, SearchObj};
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

/// How many tag ids are pulled from FTS when resolving a human name.
const FTS_NAME_LIMIT: usize = 10;

/// The row source a search iterates over.
enum Driver {
    /// One tag's posting list in its `Relationship_{ns}` partition. The scan is
    /// already restricted to files carrying `tag_id`, so the result is bounded
    /// by that tag's file count.
    Tag { namespace_id: u64, tag_id: u64 },
    /// The whole library. Required when no positive holder resolves to a single
    /// tag (a not-only search, or one made purely of multi-tag groups).
    AllFiles,
}

impl Driver {
    /// The driving row's key column: `File` exposes `id`, a relationship
    /// partition exposes `file_id`.
    fn key_column(&self) -> &'static str {
        match self {
            Driver::Tag { .. } => "file_id",
            Driver::AllFiles => "id",
        }
    }
}

/// What a single `SearchHolder` requires of the driving row.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Predicate {
    /// `And`: the row must carry every listed tag.
    All,
    /// `Or`: the row must carry at least one listed tag.
    Any,
    /// `Not`: the row must carry none of the listed tags.
    None,
}

impl TursoDatabase {
    /// Resolves tag names across namespaces and searches for files matching
    /// every input name, while allowing any tag with that name.
    pub(in crate::db::turso) async fn search_db_files_by_tags(
        &self,
        conn: &Connection,
        tags: &[String],
        limit: &Option<u64>,
    ) -> Result<Vec<u64>> {
        self.search_db_files_by_tag_groups(conn, &[], tags, &[], &[], &[], &[], limit, &None)
            .await
    }

    /// Resolves tag names across namespaces while preserving boolean groups.
    pub(in crate::db::turso) async fn search_db_files_by_tag_groups(
        &self,
        conn: &Connection,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
        limit: &Option<u64>,
        offset: &Option<u64>,
    ) -> Result<Vec<u64>> {
        let searches = self
            .tag_group_holders(and_ids, and_tags, or_ids, or_tags, not_ids, not_tags)
            .await?;
        self.search_db_files(
            conn,
            &SearchObj {
                search_relate: None,
                searches,
            },
            limit,
            offset,
        )
        .await
    }

    /// Counts the files a boolean tag-group search matches, using the same
    /// holder resolution as `search_db_files_by_tag_groups` but asking the
    /// driving scan for `COUNT(*)` instead of ids.
    ///
    /// A paged UI needs the total without pulling every matching id, so this
    /// keeps the O(result) walk inside the database (a plain index-order
    /// aggregation, no rows shipped) instead of sending the full result set
    /// across IPC for a client-side `len()`.
    pub(in crate::db::turso) async fn search_db_files_by_tag_groups_count(
        &self,
        conn: &Connection,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
    ) -> Result<u64> {
        let searches = self
            .tag_group_holders(and_ids, and_tags, or_ids, or_tags, not_ids, not_tags)
            .await?;
        self.search_db_files_count(
            conn,
            &SearchObj {
                search_relate: None,
                searches,
            },
        )
        .await
    }

    /// Builds the `SearchHolder` list behind a boolean tag-group search.
    /// Shared by the id-returning search and its count sibling so both always
    /// agree on what the groups mean.
    async fn tag_group_holders(
        &self,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
    ) -> Result<Vec<SearchHolder>> {
        let mut searches = Vec::new();
        for tag_id in and_ids {
            searches.push(SearchHolder::And(vec![*tag_id]));
        }

        let resolved_and = self.resolve_tag_names(and_tags).await?;
        if resolved_and.iter().filter(|ids| ids.is_some()).count() != and_tags.len() {
            return Ok(Vec::new());
        }
        for matching_ids in resolved_and.into_iter().flatten() {
            searches.push(SearchHolder::Or(matching_ids));
        }

        if !or_tags.is_empty() {
            let mut matching_ids = or_ids.to_vec();
            matching_ids.extend(
                self.resolve_tag_names(or_tags)
                    .await?
                    .into_iter()
                    .flatten()
                    .flatten(),
            );
            if matching_ids.is_empty() {
                return Ok(Vec::new());
            }
            searches.push(SearchHolder::Or(matching_ids));
        } else if !or_ids.is_empty() {
            searches.push(SearchHolder::Or(or_ids.to_vec()));
        }

        let mut resolved_not = not_ids.to_vec();
        for matching_ids in self
            .resolve_tag_names(not_tags)
            .await?
            .into_iter()
            .flatten()
        {
            resolved_not.extend(matching_ids);
        }
        if !resolved_not.is_empty() {
            searches.push(SearchHolder::Not(resolved_not));
        }

        Ok(searches)
    }

    /// Human-written tag searching layer.
    ///
    /// The whole predicate is emitted as **one driving scan plus correlated
    /// `EXISTS` probes**, never as a tree of set operators. Two reasons:
    ///
    /// *Speed.* Each holder becomes a filter on the driving row, so the cost is
    /// bounded by the *rarest* tag in the search rather than by the sum of all
    /// the tags' file counts. A three-tag AND over tags holding 673/820/965
    /// files went from 6.12 ms to 1.02 ms (a chain of `INTERSECT`s materialized
    /// each side into a temp B-tree and sort-merged them pairwise); a 1/3/11
    /// triple went from 0.42 ms to 0.15 ms. Every probe is a
    /// `sqlite_autoindex_relationship_n_1 (tag_id=?, file_id=?)` covering-index
    /// seek and no temp B-tree is built at all — confirmed with
    /// `EXPLAIN QUERY PLAN`.
    ///
    /// *Correctness.* Limbo cannot parse a parenthesized compound as an operand
    /// of a set operator, so the previous `A INTERSECT (B UNION C)` shape was a
    /// hard `near "INTERSECT": syntax error`. Any search mixing a multi-tag
    /// OR/NOT group with another holder therefore never ran at all — which is
    /// precisely what `search_db_files_by_tag_groups` builds for name-based
    /// searches. Correlated `EXISTS` predicates sidestep the parser entirely
    /// while expressing the same boolean algebra.
    pub(in crate::db::turso) async fn search_db_files(
        &self,
        conn: &Connection,
        search: &SearchObj,
        limit: &Option<u64>,
        offset: &Option<u64>,
    ) -> Result<Vec<u64>> {
        let sql = match self
            .build_search_sql(conn, search, limit, offset)
            .await?
        {
            Some(sql) => sql,
            None => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        let mut rows = conn.query(&sql, ()).await?;
        while let Some(row) = rows.next().await? {
            out.push(row.get(0)?);
        }
        Ok(out)
    }

    /// Upper bound a count is willing to scan before it stops. An exact total
    /// forces a walk of the whole match set; a paged UI only needs to know
    /// whether more pages exist, so counting stops at the cap and the caller
    /// displays "{cap}+". The page fetch already early-terminates the same way.
    pub(crate) const COUNT_SCAN_CAP: u64 = 20_000;

    /// Counts the files a search matches, running the same driving scan and
    /// predicates as `search_db_files` but aggregating instead of shipping
    /// rows. A paged UI asks for this once per search instead of `len()`-ing a
    /// full result set across IPC.
    ///
    /// The inner scan is bounded by `COUNT_SCAN_CAP + 1` rows: once the count
    /// reaches the cap the scan stops early and the cap is returned. A caller
    /// seeing exactly the cap knows the real total is *at least* the cap (the
    /// +1 overshoot row keeps an exact-cap total from being distinguishable,
    /// which is fine for a "N+" display).
    pub(in crate::db::turso) async fn search_db_files_count(
        &self,
        conn: &Connection,
        search: &SearchObj,
    ) -> Result<u64> {
        let sql = match self.build_search_parts(conn, search).await? {
            // The cap must bound the *row scan*, so the inner select is the
            // row-producing statement (not the count aggregate) sliced by
            // LIMIT; the outer SELECT counts what survived.
            Some((row_select, _, conditions, _)) => {
                let mut inner = row_select;
                for condition in &conditions {
                    inner.push_str(condition);
                }
                inner.push_str(&format!(" LIMIT {}", Self::COUNT_SCAN_CAP + 1));
                format!("SELECT COUNT(*) FROM ({inner})")
            }
            None => return Ok(0),
        };
        let mut rows = conn.query(&sql, ()).await?;
        match rows.next().await? {
            Some(row) => {
                let counted = row.get::<i64>(0)?.max(0) as u64;
                Ok(counted.min(Self::COUNT_SCAN_CAP))
            }
            None => Ok(0),
        }
    }

    /// Resolves the searched tags and renders the statement `search_db_files`
    /// runs. Split out from the executor so tests can inspect, and
    /// `EXPLAIN QUERY PLAN`, the exact SQL the hot path emits.
    ///
    /// Returns `None` when there is nothing to search for.
    pub(in crate::db::turso) async fn build_search_sql(
        &self,
        conn: &Connection,
        search: &SearchObj,
        limit: &Option<u64>,
        offset: &Option<u64>,
    ) -> Result<Option<String>> {
        let Some((mut sql, _, conditions, order_by)) = self.build_search_parts(conn, search).await?
        else {
            return Ok(None);
        };
        for condition in &conditions {
            sql.push_str(condition);
        }
        sql.push_str(&Self::append_order_limit_offset(order_by, limit, offset));
        Ok(Some(sql))
    }

    /// Resolves the searched tags and renders the two statements the search can
    /// run: the row-producing select (`search_db_files`) and a mirror that
    /// aggregates `COUNT(<same column>)` instead (`search_db_files_count`).
    ///
    /// Returns `(row_select, count_select, conditions)`, or `None` when there
    /// is nothing to search for. Both selects share the same driving scan and
    /// predicate list; the aggregate stays a plain scan of the driver's index
    /// (or the `File` table's primary key), so counting never needs a temp
    /// B-tree or a sorter.
    async fn build_search_parts(
        &self,
        conn: &Connection,
        search: &SearchObj,
    ) -> Result<Option<(String, String, Vec<String>, &'static str)>> {
        // Resolve every searched tag's namespace and popularity in one batched
        // query. The namespace picks the `Relationship_{ns}` partition a probe
        // has to hit; the count picks the driving scan.
        let mut id_info: HashMap<u64, (u64, i64)> = HashMap::new();
        let mut tag_ids = HashSet::new();
        for search_holder in search.searches.iter() {
            for tag_id in holder_ids(search_holder) {
                tag_ids.insert(*tag_id);
            }
        }
        let tag_ids: Vec<u64> = tag_ids.into_iter().collect();
        for chunk in tag_ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();
            let mut rows = conn
                .query(
                    format!("SELECT id, namespace, count FROM Tags WHERE id IN ({placeholders});"),
                    params_from_iter(params),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                id_info.insert(row.get::<u64>(0)?, (row.get::<u64>(1)?, row.get::<i64>(2)?));
            }
        }

        // A tag with no `Tags` row cannot be probed, because its partition is
        // unknown. Historically such a tag was skipped rather than treated as
        // unsatisfiable, so a holder whose tags are all unresolvable simply
        // stops constraining the result. `probes` resolves a holder to the
        // `(namespace, tag_id)` pairs it can actually be evaluated against.
        let probes_of = |ids: &[u64]| -> Vec<(u64, u64)> {
            ids.iter()
                .filter_map(|tag_id| {
                    id_info
                        .get(tag_id)
                        .map(|(namespace_id, _)| (*namespace_id, *tag_id))
                })
                .collect()
        };

        // Pass 1: pick the driving scan. A one-element AND/OR holder is just
        // "has this tag", so it is the one shape that can drive; the rarest such
        // tag bounds the result most tightly.
        let mut saw_positive = false;
        let mut driver: Option<Driver> = None;
        for search_holder in search.searches.iter() {
            let (ids, kind) = classify(search_holder);
            if ids.is_empty() || matches!(kind, Predicate::None) {
                continue;
            }
            saw_positive = true;
            if ids.len() != 1 {
                continue;
            }
            let probes = probes_of(ids);
            let [(namespace_id, tag_id)] = probes[..] else {
                continue;
            };
            let count = id_info.get(&tag_id).map(|(_, c)| *c).unwrap_or(0);
            let better = match &driver {
                Some(Driver::Tag {
                    tag_id: current, ..
                }) => {
                    let best = id_info.get(current).map(|(_, c)| *c).unwrap_or(0);
                    count < best
                }
                _ => true,
            };
            if better {
                driver = Some(Driver::Tag {
                    namespace_id,
                    tag_id,
                });
            }
        }
        // Without a resolvable positive holder the driver has to be the whole
        // library, which is what a not-only search means.
        let driver = match (saw_positive, driver) {
            (true, Some(driver)) => driver,
            _ => Driver::AllFiles,
        };

        // Pass 2: turn each holder into a predicate on the driving row. A
        // single-tag positive holder that *is* the driver needs no predicate —
        // the scan's own `tag_id =` restriction already guarantees it.
        let mut conditions: Vec<String> = Vec::new();
        for search_holder in search.searches.iter() {
            let (ids, kind) = classify(search_holder);
            if ids.is_empty() {
                continue;
            }
            let probes = probes_of(ids);
            if probes.is_empty() {
                continue;
            }
            if !matches!(kind, Predicate::None)
                && matches!(driver, Driver::Tag { tag_id, .. } if tag_id == probes[0].1)
                && probes.len() == 1
            {
                continue;
            }
            conditions.push(render_predicate(kind, &probes, driver.key_column()));
        }

        if conditions.is_empty() && matches!(driver, Driver::AllFiles) {
            // Nothing searchable: no positive holder resolved and there is
            // nothing to subtract.
            return Ok(None);
        }

        let (row_sql, count_sql, order_by) = match &driver {
            Driver::Tag {
                namespace_id,
                tag_id,
            } => (
                format!(
                    "SELECT d.file_id AS file_id FROM Relationship_{namespace_id} d \
                     WHERE d.tag_id = {tag_id}"
                ),
                format!(
                    "SELECT COUNT(d.file_id) FROM Relationship_{namespace_id} d \
                     WHERE d.tag_id = {tag_id}"
                ),
                "d.file_id",
            ),
            Driver::AllFiles => (
                "SELECT d.id AS file_id FROM File d WHERE 1".to_string(),
                "SELECT COUNT(d.id) FROM File d WHERE 1".to_string(),
                "d.id",
            ),
        };
        Ok(Some((row_sql, count_sql, conditions, order_by)))
    }

    /// Renders the `ORDER BY` / `LIMIT` / `OFFSET` tail. `ORDER BY` stays
    /// *inside* the single SELECT, on the driver's own key column, instead of
    /// wrapping the scan in `SELECT file_id FROM (...)`.
    ///
    /// The relationship partition's primary key is `(tag_id, file_id)`, so
    /// within one tag the index already yields `file_id` in order. Wrapping
    /// the scan hid that from the planner: the outer `ORDER BY` became
    /// `USE SORTER FOR ORDER BY`, materialising and sorting every candidate
    /// before `LIMIT` could discard it. Ordering in place drops the sorter
    /// entirely and lets `LIMIT` stop as soon as enough rows are found.
    /// Measured on 20k rows in one tag, descending, LIMIT 50: 3.16 ms ->
    /// 0.03 ms with the sort removed, and 0.11 ms with a probe that matches
    /// every row. When a search is selective enough that the whole driver has
    /// to be walked anyway, this is neutral — it never pays a sort that the
    /// index order did not already provide.
    ///
    /// `OFFSET` keeps the same property: a paged page k+1 is a skip of k * page
    /// rows over the already-ordered index, so the planner walks only
    /// `offset + limit` rows of the driving scan rather than the whole result
    /// set — and the webui no longer has to pull every matching id to reach
    /// page two.
    ///
    /// No outer wrapper is needed: limbo rejects a parenthesized compound as
    /// a compound operand, and this statement is a single `SELECT`.
    fn append_order_limit_offset(order_by: &str, limit: &Option<u64>, offset: &Option<u64>) -> String {
        let mut out = String::new();
        out.push_str(&format!(" ORDER BY {order_by} DESC"));
        if let Some(limit) = limit {
            out.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = offset.filter(|o| *o > 0) {
            out.push_str(&format!(" OFFSET {offset}"));
        }
        out
    }

    /// Resolves each human tag name to up to `FTS_NAME_LIMIT` tag ids via the
    /// popular-tag FTS index (`Tags_Popular`, `count >= 5`). A name that
    /// normalizes to empty, belongs to an unpopular tag, or matches nothing
    /// resolves to `None`.
    async fn resolve_tag_names(&self, tag_names: &[String]) -> Result<Vec<Option<Vec<u64>>>> {
        let mut out = Vec::with_capacity(tag_names.len());
        for tag_name in tag_names {
            if tag_name.trim().is_empty() {
                out.push(None);
                continue;
            }

            let matching_ids = self
                .tags_search_fts(tag_name, FTS_NAME_LIMIT)
                .await?
                .into_iter()
                .map(|result| result.tag_id)
                .collect::<Vec<_>>();

            if matching_ids.is_empty() {
                out.push(None);
            } else {
                out.push(Some(matching_ids));
            }
        }
        Ok(out)
    }
}

fn classify(search_holder: &SearchHolder) -> (&[u64], Predicate) {
    match search_holder {
        SearchHolder::And(ids) => (ids, Predicate::All),
        SearchHolder::Or(ids) => (ids, Predicate::Any),
        SearchHolder::Not(ids) => (ids, Predicate::None),
    }
}

/// Renders one holder as a boolean predicate over the driving row.
///
/// * `And([a, b])` -> `AND EXISTS(a) AND EXISTS(b)`
/// * `Or([a, b])`  -> `AND (EXISTS(a) OR EXISTS(b))`
/// * `Not([a, b])` -> `AND NOT EXISTS(a) AND NOT EXISTS(b)`
///
/// The NOT form is the De Morgan equivalent of "carries none of them", which is
/// what subtracting the holder's union means.
///
/// Tag ids are inlined rather than bound because the statement is assembled per
/// search anyway. They come from `Tags.id` (an integer primary key), never from
/// user input, so there is nothing here to bind.
fn render_predicate(kind: Predicate, probes: &[(u64, u64)], driver_key: &str) -> String {
    let probe = |namespace_id: u64, tag_id: u64, alias: &str| {
        format!(
            "EXISTS(SELECT 1 FROM Relationship_{namespace_id} {alias} \
             WHERE {alias}.tag_id = {tag_id} AND {alias}.file_id = d.{driver_key})"
        )
    };

    match kind {
        Predicate::None => probes
            .iter()
            .enumerate()
            .map(|(index, (namespace_id, tag_id))| {
                format!(
                    " AND NOT {}",
                    probe(*namespace_id, *tag_id, &format!("n{index}"))
                )
            })
            .collect(),
        Predicate::All => probes
            .iter()
            .enumerate()
            .map(|(index, (namespace_id, tag_id))| {
                format!(
                    " AND {}",
                    probe(*namespace_id, *tag_id, &format!("a{index}"))
                )
            })
            .collect(),
        Predicate::Any => {
            let parts: Vec<String> = probes
                .iter()
                .enumerate()
                .map(|(index, (namespace_id, tag_id))| {
                    probe(*namespace_id, *tag_id, &format!("o{index}"))
                })
                .collect();
            format!(" AND ({})", parts.join(" OR "))
        }
    }
}

fn holder_ids(search_holder: &SearchHolder) -> &[u64] {
    match search_holder {
        SearchHolder::And(ids) => ids,
        SearchHolder::Or(ids) => ids,
        SearchHolder::Not(ids) => ids,
    }
}


#[cfg(test)]
mod tests;
