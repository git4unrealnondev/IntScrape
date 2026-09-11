//! Database operations for the `search` domain.

use super::MainDatabase;
use rusqlite::{Connection, params};
use shared_types::{SearchHolder, SearchObj, Tag};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

impl MainDatabase {
    ///
    /// Updates the fts sqlite table
    ///
    pub fn internal_update_fts_table(
        self: Arc<Self>,
        conn: &Connection,
    ) -> Result<(), Box<dyn Error>> {
        conn.execute(
            "INSERT INTO Tags_Popular_fts(rowid, name, namespace) 
SELECT id, name, namespace FROM High_Value_Tags;",
            [],
        )?;
        Ok(())
    }

    ///
    /// Returns the full location of where a file should be stored
    ///
    pub async fn file_ids_get_tags(
        self: Arc<Self>,
        file_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<Tag>> {
        // If our hash is less then 6 cant return a location
        if file_ids.is_empty() {
            return HashMap::new();
        }
        let file_ids = file_ids.clone();
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().unwrap();
            self.internal_file_ids_get_tags(&conn, &file_ids)
        })
        .await
        .ok()
        .unwrap()
    }

    ///
    /// Gets the namespace id from a tag id. Shouldn't fail unless the db is inconsistent
    ///
    pub fn get_namespace_id_from_tag_id(
        &self,
        conn: &Connection,
        tag_id: &u64,
    ) -> Result<u64, rusqlite::Error> {
        conn.query_row(
            "SELECT namespace FROM Tags WHERE id = ?1 LIMIT 1;",
            params![tag_id],
            |f| f.get(0),
        )
    }

    pub fn generate_file_search_sql(&self, tag_id: &u64, namespace_id: &u64) -> String {
        format!(
            "SELECT file_id FROM Relationship_{} WHERE tag_id = {}",
            namespace_id, tag_id
        )
    }

    ///
    /// Searches the db for all file_ids that are related to the searchobj
    ///
    #[must_use]
    pub fn search_db_files_sync_old(&self, search: &SearchObj, limit: &Option<u64>) -> Vec<u64> {
        use rusqlite::params_from_iter;

        let _start_time = Instant::now();

        // 1. Extract and Categorize Tags
        let mut and_tags = Vec::new();
        let mut or_groups: Vec<Vec<u64>> = Vec::new();
        let mut not_groups: Vec<Vec<u64>> = Vec::new();

        for holder in search.searches.clone() {
            match holder {
                SearchHolder::And(ids) => and_tags.extend(ids),
                SearchHolder::Or(ids) if !ids.is_empty() => or_groups.push(ids),
                SearchHolder::Not(ids) if !ids.is_empty() => not_groups.push(ids),
                _ => {}
            }
        }

        // A NOT-only search still has a valid candidate set: all tagged files.
        // Only an entirely empty search should return no results.
        if and_tags.is_empty() && or_groups.is_empty() && not_groups.is_empty() {
            return vec![];
        }

        let mut driver_or_group = if and_tags.is_empty() {
            or_groups.first().is_some().then(|| or_groups.remove(0))
        } else {
            None
        };

        let conn = match self.pool.get() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to acquire DB connection for file search: {error}");
                return Vec::new();
            }
        };
        let mut cached_candidates = None;
        let mut cached_all_tags = false;
        let mut cached_search_type = None;
        // 2. PATH A: Roaring Bitmap Optimization (Memory Speed)
        let read_guard = self.relationship_roaring_storage.read();
        if let Some(ref roaring) = *read_guard {
            if !and_tags.is_empty() && driver_or_group.is_none() && or_groups.is_empty() {
                let (candidates, all_cached) = roaring.cached_file_ids_for_tags(
                    &conn,
                    &and_tags,
                    &shared_types::DbSearchTypeEnum::And,
                );
                cached_candidates = candidates;
                cached_all_tags = all_cached;
                cached_search_type = Some(shared_types::DbSearchTypeEnum::And);
            } else if and_tags.is_empty()
                && not_groups.is_empty()
                && driver_or_group.is_some()
                && or_groups.is_empty()
            {
                if let Some(tags) = driver_or_group.as_ref() {
                    let (candidates, all_cached) = roaring.cached_file_ids_for_tags(
                        &conn,
                        tags,
                        &shared_types::DbSearchTypeEnum::Or,
                    );
                    cached_candidates = candidates;
                    cached_all_tags = all_cached;
                    cached_search_type = Some(shared_types::DbSearchTypeEnum::Or);
                }
            }
        }

        // When every exclusion bitmap is available, apply NOT directly to the
        // positive roaring candidates. Falling back to SQL is necessary if an
        // exclusion tag is not cached, because merging uncached candidates would
        // otherwise bypass the NOT predicate.
        let not_tag_ids = not_groups.iter().flatten().copied().collect::<Vec<_>>();
        let (cached_exclusions, all_exclusions_cached) = if not_tag_ids.is_empty() {
            (None, true)
        } else if let Some(ref roaring) = *read_guard {
            roaring.cached_file_ids_for_tags(
                &conn,
                &not_tag_ids,
                &shared_types::DbSearchTypeEnum::Or,
            )
        } else {
            (None, false)
        };

        // Evaluate the complete positive expression from roaring when every
        // referenced tag is resident. This also covers grouped searches,
        // where each OR group is a required condition.
        if let Some(ref roaring) = *read_guard {
            let mut cache_groups = Vec::new();
            if !and_tags.is_empty() {
                cache_groups.push((and_tags.as_slice(), shared_types::DbSearchTypeEnum::And));
            }
            if let Some(group) = driver_or_group.as_ref() {
                cache_groups.push((group.as_slice(), shared_types::DbSearchTypeEnum::Or));
            }
            cache_groups.extend(
                or_groups
                    .iter()
                    .map(|group| (group.as_slice(), shared_types::DbSearchTypeEnum::Or)),
            );

            if !cache_groups.is_empty() {
                let mut candidates: Option<std::collections::HashSet<u64>> = None;
                let all_positive_cached = cache_groups.iter().all(|(tags, search_type)| {
                    let (group_candidates, all_cached) =
                        roaring.cached_file_ids_for_tags(&conn, tags, search_type);
                    if all_cached {
                        if let Some(group_candidates) = group_candidates {
                            let group_candidates = group_candidates
                                .into_iter()
                                .collect::<std::collections::HashSet<_>>();
                            if let Some(current) = candidates.as_mut() {
                                current.retain(|file_id| group_candidates.contains(file_id));
                            } else {
                                candidates = Some(group_candidates);
                            }
                        }
                    }
                    all_cached
                });

                if all_positive_cached && all_exclusions_cached {
                    if let Some(exclusions) = cached_exclusions {
                        let excluded = exclusions
                            .into_iter()
                            .collect::<std::collections::HashSet<_>>();
                        if let Some(current) = candidates.as_mut() {
                            current.retain(|file_id| !excluded.contains(file_id));
                        }
                    }
                    let mut results = candidates
                        .unwrap_or_default()
                        .into_iter()
                        .collect::<Vec<_>>();
                    results.sort_unstable_by(|left, right| right.cmp(left));
                    if let Some(limit) = limit {
                        results.truncate(*limit as usize);
                    }
                    return results;
                }
            }
        }

        if !not_tag_ids.is_empty() && !all_exclusions_cached {
            // Do not merge a partial OR cache into a SQL result when NOT tags
            // are present. SQL must evaluate the complete boolean expression.
            cached_candidates = None;
            cached_search_type = None;
        }

        if let (Some(candidates), true, Some(search_type)) = (
            &cached_candidates,
            cached_all_tags,
            cached_search_type.as_ref(),
        ) && not_groups.is_empty()
        {
            let mut results = candidates.clone();
            results.sort_unstable_by(|left, right| right.cmp(left));
            if let Some(limit) = limit {
                results.truncate(*limit as usize);
            }
            if matches!(
                search_type,
                shared_types::DbSearchTypeEnum::And | shared_types::DbSearchTypeEnum::Or
            ) {
                return results;
            }
        }

        if matches!(cached_search_type, Some(shared_types::DbSearchTypeEnum::Or)) {
            if let Some(tags) = driver_or_group.as_mut() {
                if let Some(ref roaring) = *read_guard {
                    tags.retain(|tag_id| roaring.tag_is_cached_in_memory(*tag_id));
                }
            }
            if driver_or_group.as_ref().is_some_and(Vec::is_empty) {
                let mut results = cached_candidates.unwrap_or_default();
                results.sort_unstable_by(|left, right| right.cmp(left));
                if let Some(limit) = limit {
                    results.truncate(*limit as usize);
                }
                return results;
            }
        }

        // 3. PATH B: Optimized SQL (Database Speed)
        // If cache is off, we use Inner Joins on the rarest tag to minimize index lookups.

        // Sort AND tags by rarity using the 'count' column in Tags table
        let mut sorted_and = and_tags;
        if sorted_and.len() > 1 {
            let placeholders = vec!["?"; sorted_and.len()].join(",");
            let count_sql =
                format!("SELECT id FROM Tags WHERE id IN ({placeholders}) ORDER BY count ASC");
            if let Ok(mut stmt) = conn.prepare(&count_sql) {
                let ids: Vec<u64> =
                    match stmt.query_map(params_from_iter(&sorted_and), |r| r.get(0)) {
                        Ok(rows) => rows.filter_map(std::result::Result::ok).collect(),
                        Err(error) => {
                            log::error!("Failed to rank AND tags for file search: {error}");
                            Vec::new()
                        }
                    };
                if !ids.is_empty() {
                    sorted_and = ids;
                }
            }
        }

        let mut params = Vec::new();
        let relationship_source = self.relationship_union_source(&conn, "r0");
        let mut sql = if let Some(driver_group) = driver_or_group {
            let placeholders = vec!["?"; driver_group.len()].join(",");
            params.extend(driver_group);
            format!(
                "SELECT DISTINCT r0.file_id FROM {relationship_source} WHERE r0.tag_id IN ({placeholders})"
            )
        } else {
            format!("SELECT DISTINCT r0.file_id FROM {relationship_source}")
        };

        // Only add JOINs if there are more AND tags
        for (i, tag) in sorted_and.iter().skip(1).enumerate() {
            let alias = format!("r{}", i + 1);
            sql.push_str(&format!(
                " JOIN {} ON r0.file_id = {alias}.file_id AND {alias}.tag_id = ?",
                self.relationship_union_source(&conn, &alias)
            ));
            params.push(*tag);
        }

        // Start the predicate list with the driver tag or a neutral condition.
        if !sorted_and.is_empty() {
            sql.push_str(if sql.contains(" WHERE ") {
                " AND r0.tag_id = ?"
            } else {
                " WHERE r0.tag_id = ?"
            });
            params.push(sorted_and[0]);
        } else if !sql.contains(" WHERE ") {
            // Start the predicate list when there is no AND driver.
            sql.push_str(" WHERE 1 = 1");
        }

        if matches!(
            cached_search_type,
            Some(shared_types::DbSearchTypeEnum::And)
        ) {
            if let Some(candidates) = &cached_candidates {
                if candidates.is_empty() {
                    return Vec::new();
                }
                let placeholders = vec!["?"; candidates.len()].join(",");
                sql.push_str(&format!(" AND r0.file_id IN ({placeholders})"));
                params.extend(candidates.iter().copied());
            }
        } else if matches!(cached_search_type, Some(shared_types::DbSearchTypeEnum::Or)) {
            if let Some(candidates) = &cached_candidates {
                if !candidates.is_empty() {
                    let placeholders = vec!["?"; candidates.len()].join(",");
                    sql.push_str(&format!(" AND r0.file_id NOT IN ({placeholders})"));
                    params.extend(candidates.iter().copied());
                }
            }
        }

        // Add OR groups
        for (i, group) in or_groups.iter().enumerate() {
            let placeholders = vec!["?"; group.len()].join(",");
            sql.push_str(&format!(
        " AND EXISTS (SELECT 1 FROM {} WHERE or{i}.file_id = r0.file_id AND or{i}.tag_id IN ({placeholders}))",
        self.relationship_union_source(&conn, &format!("or{i}"))
    ));
            for &tag_id in group {
                params.push(tag_id);
            }
        }

        // Add NOT groups
        for (i, group) in not_groups.iter().enumerate() {
            let placeholders = vec!["?"; group.len()].join(",");
            sql.push_str(&format!(
        " AND NOT EXISTS (SELECT 1 FROM {} WHERE not{i}.file_id = r0.file_id AND not{i}.tag_id IN ({placeholders}))",
        self.relationship_union_source(&conn, &format!("not{i}"))
    ));
            for &tag_id in group {
                params.push(tag_id);
            }
        }

        // Finalize
        sql.push_str(" ORDER BY r0.file_id DESC");

        let mut stmt = match conn.prepare(&sql) {
            Ok(stmt) => stmt,
            Err(error) => {
                log::error!("Unable to prepare a db search: {error}");
                return Vec::new();
            }
        };
        let mut results: Vec<u64> = match stmt.query_map(params_from_iter(params), |row| row.get(0))
        {
            Ok(rows) => rows.filter_map(std::result::Result::ok).collect(),
            Err(error) => {
                log::error!("Unable to execute a db search: {error}");
                return Vec::new();
            }
        };

        if matches!(cached_search_type, Some(shared_types::DbSearchTypeEnum::Or)) {
            results.extend(cached_candidates.unwrap_or_default());
            results.sort_unstable_by(|left, right| right.cmp(left));
            results.dedup();
        }

        if let Some(limit) = limit {
            results.truncate(*limit as usize);
        }

        results
    }
}
