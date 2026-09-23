use std::collections::{HashMap, HashSet};

use shared_types::{SearchHolder, SearchObj};
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

/// How many tag ids are pulled from FTS when resolving a human name.
const FTS_NAME_LIMIT: usize = 10;

impl TursoDatabase {
    /// Resolves tag names across namespaces and searches for files matching
    /// every input name, while allowing any tag with that name.
    pub(in crate::db::turso) async fn search_db_files_by_tags(
        &self,
        conn: &Connection,
        tags: &[String],
        limit: &Option<u64>,
    ) -> Result<Vec<u64>> {
        self.search_db_files_by_tag_groups(conn, &[], tags, &[], &[], &[], &[], limit)
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
    ) -> Result<Vec<u64>> {
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

        self.search_db_files(
            conn,
            &SearchObj {
                search_relate: None,
                searches,
            },
            limit,
        )
        .await
    }

    /// Human written tag searching layer.
    pub(in crate::db::turso) async fn search_db_files(
        &self,
        conn: &Connection,
        search: &SearchObj,
        limit: &Option<u64>,
    ) -> Result<Vec<u64>> {
        // Resolve all tag namespaces in batched queries instead of one query
        // per tag. Search requests commonly contain many tag IDs.
        let mut id_ns_map = HashMap::new();
        let mut tag_ids = HashSet::new();
        for search_holder in search.searches.iter() {
            let ids = holder_ids(search_holder);
            for tag_id in ids {
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
                    format!("SELECT id, namespace FROM Tags WHERE id IN ({placeholders});"),
                    params_from_iter(params),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                id_ns_map.insert(row.get::<u64>(0)?, row.get::<u64>(1)?);
            }
        }

        // Each holder maps to one clause. Within a holder every tag is a query;
        // AND groups intersect its queries, OR groups union them, NOT groups are
        // an exclusion set subtracted from the accumulated result.
        let mut sql_list: Vec<String> = Vec::new();
        let mut positive_count = 0usize;
        for search_holder in search.searches.iter() {
            let (ids, is_not): (&[u64], bool) = match search_holder {
                SearchHolder::And(ids) => (ids, false),
                SearchHolder::Or(ids) => (ids, false),
                SearchHolder::Not(ids) => (ids, true),
            };
            if ids.is_empty() {
                continue;
            }

            let mut queries = Vec::with_capacity(ids.len());
            for tag_id in ids {
                if let Some(namespace_id) = id_ns_map.get(tag_id) {
                    queries.push(format!(
                        "SELECT file_id FROM Relationship_{namespace_id} WHERE tag_id = {tag_id}"
                    ));
                }
            }
            if queries.is_empty() {
                continue;
            }

            let clause = if queries.len() == 1 {
                queries.pop().unwrap()
            } else {
                let separator = if is_not || matches!(search_holder, SearchHolder::Or(_)) {
                    " UNION "
                } else {
                    " INTERSECT "
                };
                format!("({})", queries.join(separator))
            };

            if sql_list.is_empty() {
                sql_list.push(clause);
            } else if is_not {
                sql_list.push("EXCEPT".into());
                sql_list.push(clause);
            } else {
                sql_list.push("INTERSECT".into());
                sql_list.push(clause);
            }
            if !is_not {
                positive_count += 1;
            }
        }

        if sql_list.is_empty() {
            return Ok(Vec::new());
        }
        if positive_count == 0 {
            // Not-only search: subtract exclusions from every file in the library.
            sql_list.insert(0, "SELECT id AS file_id FROM File".into());
            sql_list.insert(1, "EXCEPT".into());
        }

        let mut sql_string = format!(
            "SELECT file_id FROM ({}) ORDER BY file_id DESC",
            sql_list.join(" ")
        );
        if let Some(limit) = limit {
            sql_string.push_str(&format!(" LIMIT {limit}"));
        }

        let mut out = Vec::new();
        let mut rows = conn.query(&sql_string, ()).await?;
        while let Some(row) = rows.next().await? {
            out.push(row.get(0)?);
        }

        Ok(out)
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

fn holder_ids(search_holder: &SearchHolder) -> &[u64] {
    match search_holder {
        SearchHolder::And(ids) => ids,
        SearchHolder::Or(ids) => ids,
        SearchHolder::Not(ids) => ids,
    }
}
