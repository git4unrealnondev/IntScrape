//! Database operations for the `relationship` domain.

use super::{MainDatabase, SQL_CHUNK_SIZE};
use rusqlite::{Connection, OptionalExtension, params};
use shared_types::{PluginTag, Tag};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl MainDatabase {
    ///
    /// Gets a `file_id` from a `tag_id`
    ///
    pub fn internal_tag_id_get_file_id(
        &self,
        conn: &Connection,
        tag_id: &u64,
    ) -> Result<u64, rusqlite::Error> {
        let namespace_id: u64 = conn.query_row(
            "SELECT namespace FROM Tags WHERE id = ?1",
            [tag_id],
            |row| row.get(0),
        )?;
        let table = self.relationship_partition_name(namespace_id);
        conn.query_row(
            &format!("SELECT file_id FROM {table} WHERE tag_id = ?1 LIMIT 1;"),
            params![tag_id],
            |row| row.get(0),
        )
    }

    ///
    /// Gets `tag_ids` for `file_id`
    ///
    pub fn internal_file_id_get_tag_ids(
        &self,
        conn: &Connection,
        file_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        // Cache to get data locally
        {
            let read_guard = self.relationship_roaring_storage.read();
            if let Some(roaring) = read_guard.as_ref()
                && let Some(tag_ids) = roaring.relationship_search_tagid_roaring_in_memory(*file_id)
            {
                return Ok(tag_ids);
            }
        }

        let mut stmt = conn
            .prepare(&format!(
                "SELECT tag_id FROM {} where file_id = ?1;",
                self.relationship_union_source(conn, "r")
            ))
            .unwrap();
        let mut out = HashSet::new();
        for tag_id in stmt.query_map([file_id], |row| row.get(0))?.flatten() {
            out.insert(tag_id);
        }

        Ok(out)
    }

    ///
    /// Gets `file_ids` for `tag_id`
    ///
    pub fn internal_tag_id_get_file_ids(
        &self,
        conn: &Connection,
        tag_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT file_id FROM {} where tag_id = ?1;",
                self.relationship_union_source(conn, "r")
            ))
            .unwrap();
        let mut out = HashSet::new();
        for tag_id in stmt.query_map([tag_id], |row| row.get(0))?.flatten() {
            out.insert(tag_id);
        }

        Ok(out)
    }

    ///
    /// Gets filtered `tag_ids` for a fileid filters by nsid
    ///
    pub fn internal_file_id_get_tag_ids_where_namespace_id(
        &self,
        conn: &Connection,
        file_id: &u64,
        namespace_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let table = self.relationship_partition_name(*namespace_id);
        let mut stmt = conn.prepare(&format!("SELECT tag_id FROM {table} WHERE file_id = ?1"))?;

        let mut out = HashSet::new();

        let rows = stmt.query_map([file_id], |row| row.get(0))?;

        for tag_id in rows.flatten() {
            out.insert(tag_id);
        }

        Ok(out)
    }

    ///
    /// Builds a list of file -> `tag_id` maps
    ///
    pub fn internal_file_id_get_tag_ids_bulk(
        &self,
        conn: &Connection,
        file_ids: &[u64],
    ) -> Result<HashMap<u64, HashSet<u64>>, rusqlite::Error> {
        let mut out: HashMap<u64, HashSet<u64>> = HashMap::new();
        if file_ids.is_empty() {
            return Ok(out);
        }

        let mut uncached_file_ids = Vec::new();
        {
            let read_guard = self.relationship_roaring_storage.read();
            if let Some(roaring) = read_guard.as_ref() {
                for file_id in file_ids {
                    if let Some(tag_ids) =
                        roaring.relationship_search_tagid_roaring_in_memory(*file_id)
                    {
                        out.insert(*file_id, tag_ids);
                    } else {
                        uncached_file_ids.push(*file_id);
                    }
                }
            } else {
                uncached_file_ids.extend_from_slice(file_ids);
            }
        }

        if uncached_file_ids.is_empty() {
            return Ok(out);
        }

        // Build query: SELECT file_id, tag_id FROM Relationship WHERE file_id IN (?, ?, ...)
        let mut query = format!(
            "SELECT file_id, tag_id FROM {} WHERE ",
            self.relationship_union_source(conn, "r")
        );
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(uncached_file_ids.len());

        for (i, id) in uncached_file_ids.iter().enumerate() {
            if i > 0 {
                query.push_str(" OR ");
            }
            query.push_str(&format!("file_id = ?{}", i + 1));
            params_vector.push(id);
        }

        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params_vector))?;

        while let Some(row) = rows.next()? {
            let f_id: u64 = row.get(0)?;
            let t_id: u64 = row.get(1)?;
            out.entry(f_id).or_default().insert(t_id);
        }

        Ok(out)
    }

    ///
    /// Checks if the relationship structure defined inside a single `PluginTag` exists.
    ///
    pub fn internal_parent_structure_exists(
        &self,
        conn: &Connection,
        plugin_tag: &PluginTag,
    ) -> Result<bool, rusqlite::Error> {
        // 1️⃣ If this tag doesn't even define a relationship context, it has no parent structure
        let Some(relation_ctx) = &plugin_tag.relates_to else {
            return Ok(false);
        };

        // 2️⃣ Helper closure to look up a Tag's database ID using Name and Namespace strings
        let get_tag_db_id = |tag: &Tag| -> Result<Option<u64>, rusqlite::Error> {
            let mut stmt = conn.prepare(
                "SELECT t.id FROM Tags t \
                 JOIN Namespace n ON t.namespace = n.id \
                 WHERE t.name = ?1 AND n.name = ?2 \
                 LIMIT 1",
            )?;
            stmt.query_row([&tag.name, &tag.namespace.name], |row| row.get(0))
                .optional()
        };

        // 3️⃣ Resolve IDs for the base tag and its parent tag
        let Some(child_id) = get_tag_db_id(&plugin_tag.tag)? else {
            return Ok(false);
        };
        let Some(parent_id) = get_tag_db_id(&relation_ctx.tag)? else {
            return Ok(false);
        };

        // 4️⃣ Resolve the optional limit_to validation criteria context
        let limit_to_id = match &relation_ctx.limit_to {
            Some(lim_tag) => get_tag_db_id(lim_tag)?,
            None => None,
        };

        // 5️⃣ Verify if this specific layout pattern matches a row in the Parents table
        let mut stmt = conn.prepare(
            "SELECT EXISTS (
                SELECT 1 
                FROM Parents 
                WHERE tag_id = ?1 \
                  AND relate_tag_id = ?2 \
                  AND (
                    (?3 IS NULL AND limit_to IS NULL) OR \
                    (limit_to = ?3)
                  )
            )",
        )?;

        let structural_link_exists: bool = stmt
            .query_row(rusqlite::params![child_id, parent_id, limit_to_id], |row| {
                row.get(0)
            })?;

        Ok(structural_link_exists)
    }

    pub fn internal_parent_relate_limit_exists(
        &self,
        conn: &Connection,
        relate_to: &Tag,
        limit_to: &Tag,
    ) -> Result<bool, rusqlite::Error> {
        // 2️⃣ Helper closure to look up a Tag's database ID using Name and Namespace strings
        let get_tag_db_id = |tag: &Tag| -> Result<Option<u64>, rusqlite::Error> {
            let mut stmt = conn.prepare(
                "SELECT t.id FROM Tags t \
                 JOIN Namespace n ON t.namespace = n.id \
                 WHERE t.name = ?1 AND n.name = ?2 \
                 LIMIT 1",
            )?;
            stmt.query_row([&tag.name, &tag.namespace.name], |row| row.get(0))
                .optional()
        };

        // 3️⃣ Resolve IDs for the base tag and its parent tag
        let Some(relate_id) = get_tag_db_id(relate_to)? else {
            return Ok(false);
        };
        let Some(limit_id) = get_tag_db_id(limit_to)? else {
            return Ok(false);
        };

        // 5️⃣ Verify if this specific layout pattern matches a row in the Parents table
        let mut stmt = conn.prepare(
            "SELECT EXISTS (
                SELECT 1 
                FROM Parents 
                  WHERE relate_tag_id = ?1 
                  AND 
                    limit_to = ?2
                  
            )",
        )?;

        let structural_link_exists: bool =
            stmt.query_row(rusqlite::params![relate_id, limit_id], |row| row.get(0))?;

        Ok(structural_link_exists)
    }

    ///
    /// Used internally to add a relationship to a db
    ///
    pub fn internal_relationship_add(
        &self,
        conn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        self.tag_search_dirty.store(true, Ordering::SeqCst);
        Self::internal_audit_context_set(conn, "relationship added")?;
        let namespace_id: u64 = conn.query_row(
            "SELECT namespace FROM Tags WHERE id = ?1",
            [tag_id],
            |row| row.get(0),
        )?;
        let table = self.relationship_partition_name(namespace_id);
        conn.execute(
            &format!("INSERT OR IGNORE INTO {table} (file_id, tag_id) VALUES (?1, ?2)"),
            r2d2_sqlite::rusqlite::params![file_id, tag_id],
        )?;
        Ok(())
    }

    ///
    /// Deletes relationships from db
    ///
    pub fn internal_relationship_bulk_delete(
        &self,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) {
        if relationships.is_empty() {
            return;
        }
        self.tag_search_dirty.store(true, Ordering::SeqCst);

        let mut by_namespace: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for &(file_id, tag_id) in relationships {
            if let Ok(namespace_id) = conn.query_row(
                "SELECT namespace FROM Tags WHERE id = ?1",
                [tag_id],
                |row| row.get::<_, u64>(0),
            ) {
                by_namespace
                    .entry(namespace_id)
                    .or_default()
                    .push((file_id, tag_id));
            }
        }

        // removes relationships between roaring
        {
            // A search reader may be holding the roaring lock for the duration
            // of its intersection. Never block the writer behind it: persist the
            // SQL cache tables and mark the in-memory bitmaps for reload.
            if let Some(mut guard) = self.relationship_roaring_storage.try_write() {
                if let Some(roaring) = guard.as_mut() {
                    for (file_id, tag_id) in relationships {
                        roaring.remove_roaring(conn, *tag_id, *file_id);
                    }
                }
            } else {
                if let Some(roaring) = self.relationship_roaring_storage.read().as_ref() {
                    for (file_id, tag_id) in relationships {
                        roaring.relationship_cache_remove_sql_standalone(conn, *file_id, *tag_id);
                    }
                }
                self.roaring_memory_dirty.store(true, Ordering::SeqCst);
            }
        }

        for (namespace_id, rels) in by_namespace {
            let table = self.relationship_partition_name(namespace_id);
            let mut query = format!("DELETE FROM {table} WHERE ");
            let mut params_vector: Vec<&dyn rusqlite::types::ToSql> = Vec::new();
            for (i, rel) in rels.iter().enumerate() {
                if i > 0 {
                    query.push_str(" OR ");
                }
                query.push_str(&format!(
                    "(file_id = ?{} AND tag_id = ?{})",
                    i * 2 + 1,
                    i * 2 + 2
                ));
                params_vector.push(&rel.0);
                params_vector.push(&rel.1);
            }
            let deleted = conn.execute(&query, &*params_vector).unwrap();
            if deleted > 0 {
                for (_, tag_id) in &rels {
                    conn.execute(
                        "UPDATE Tags SET count = MAX(count - 1, 0) WHERE id = ?1",
                        [*tag_id],
                    )
                    .unwrap();
                }
            }
        }
    }

    ///
    /// Groups mixed-namespace relationships before inserting each partition.
    pub fn internal_relationships_bulk_add(
        &self,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) {
        self.tag_search_dirty.store(true, Ordering::SeqCst);

        // Resolve every tag's namespace with a few chunked row lookups instead
        // of one point query per relationship, keeping the writer lock window
        // short for large batches.
        let mut tag_namespaces = HashMap::<u64, u64>::new();
        let tag_ids = relationships
            .iter()
            .map(|(_, tag_id)| *tag_id)
            .collect::<Vec<u64>>();
        for chunk in tag_ids.chunks(SQL_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = (0..chunk.len())
                .map(|index| format!("?{}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!("SELECT id, namespace FROM Tags WHERE id IN ({placeholders})");
            let mut stmt = conn.prepare(&query).unwrap();
            let mut query_params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len());
            for tag_id in chunk.iter() {
                query_params.push(tag_id);
            }
            let rows = stmt
                .query_map(rusqlite::params_from_iter(query_params), |row| {
                    let tag_id: u64 = row.get(0)?;
                    let namespace_id: u64 = row.get(1)?;
                    Ok((tag_id, namespace_id))
                })
                .unwrap();
            for matched in rows.flatten() {
                tag_namespaces.insert(matched.0, matched.1);
            }
        }

        let mut by_namespace = HashMap::<u64, HashSet<(u64, u64)>>::new();
        for &(file_id, tag_id) in relationships {
            let Some(namespace_id) = tag_namespaces.get(&tag_id) else {
                continue;
            };
            by_namespace
                .entry(*namespace_id)
                .or_default()
                .insert((file_id, tag_id));
        }

        for (namespace_id, relationships) in by_namespace {
            self.internal_relationship_bulk_add(conn, namespace_id, &relationships);
        }
    }

    /// Bulk adds relationships to a known namespace partition.
    pub fn internal_relationship_bulk_add(
        &self,
        conn: &Connection,
        namespace_id: u64,
        relationships: &HashSet<(u64, u64)>,
    ) {
        self.tag_search_dirty.store(true, Ordering::SeqCst);
        if relationships.is_empty() {
            return;
        }

        let table = self.relationship_partition_name(namespace_id);
        let relationships = relationships.iter().copied().collect::<Vec<_>>();
        let mut inserted_relationships = 0;

        for chunk in relationships.chunks(SQL_CHUNK_SIZE) {
            let mut query = format!("INSERT OR IGNORE INTO {table} (file_id, tag_id) VALUES ");
            let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
                Vec::with_capacity(chunk.len() * 2);

            for (i, relationship) in chunk.iter().enumerate() {
                if i > 0 {
                    query.push_str(", ");
                }
                query.push_str(&format!("(?{}, ?{})", i * 2 + 1, i * 2 + 2));
                params_vector.push(&relationship.0);
                params_vector.push(&relationship.1);
            }

            match conn.execute(&query, &*params_vector) {
                Ok(inserted) => {
                    inserted_relationships += inserted;
                    if inserted > 0 {
                        for (_, tag_id) in chunk {
                            conn.execute(
                                "UPDATE Tags SET count = count + 1 WHERE id = ?1",
                                [tag_id],
                            )
                            .unwrap();
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to bulk insert relationships: {e}");
                    return;
                }
            }
        }
        // Duplicate relationship updates are common when a known file is
        // encountered again. Avoid rewriting roaring blobs in that case.
        if inserted_relationships > 0 {
            // Like the delete path, never block the writer on an active search
            // reader. Fall back to the SQL side and mark the RAM copy for reload.
            if let Some(mut guard) = self.relationship_roaring_storage.try_write() {
                if let Some(roaring) = guard.as_mut() {
                    for (file_id, tag_id) in &relationships {
                        roaring.relationship_roaring_add(conn, *file_id, *tag_id);
                    }
                }
            } else {
                if let Some(roaring) = self.relationship_roaring_storage.read().as_ref() {
                    for (file_id, tag_id) in &relationships {
                        roaring.relationship_cache_add_sql_standalone(conn, *file_id, *tag_id);
                    }
                }
                self.roaring_memory_dirty.store(true, Ordering::SeqCst);
            }
        }
    }

    ///
    /// Bulk adds parents into DB returning their id
    ///
    pub fn internal_parents_bulk_add(
        &self,
        conn: &Connection,
        parents: &HashSet<shared_types::TagParents>,
    ) -> HashMap<shared_types::TagParents, u64> {
        Self::internal_audit_context_set(conn, "tag parent relationship added").unwrap();
        let mut out = HashMap::new();

        if parents.is_empty() {
            return out;
        }

        let parents_vec: Vec<&shared_types::TagParents> = parents.iter().collect();

        let mut query =
            String::from("INSERT OR IGNORE INTO Parents (tag_id, relate_tag_id, limit_to) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(parents_vec.len() * 3);

        // String building
        for (i, parent) in parents_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{}, ?{})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            params_vector.push(&parent.tag_id);
            params_vector.push(&parent.relate_tag_id);
            params_vector.push(&parent.limit_to);
        }

        query.push_str(
            " ON CONFLICT(tag_id, relate_tag_id, limit_to) 
         DO UPDATE SET tag_id = excluded.tag_id 
         RETURNING id",
        );

        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(&*params_vector).unwrap();

        let mut idx = 0;
        while let Some(row) = rows.next().unwrap() {
            let parent_id: u64 = row.get(0).unwrap();
            let parent_obj = parents_vec[idx];

            out.insert((*parent_obj).clone(), parent_id);
            idx += 1;
        }

        out
    }

    pub fn debug_print_parents(conn: &Connection) {
        // 1. Prepare the SELECT statement
        let mut stmt = conn
            .prepare("SELECT tag_id, relate_tag_id, limit_to FROM Parents")
            .unwrap();

        // 2. Query the rows and map them to a tuple or struct
        let parent_rows = stmt
            .query_map([], |row| {
                let tag_id: u64 = row.get(0)?;
                let relate_tag_id: u64 = row.get(1)?;
                let limit_to: Option<u64> = row.get(2)?;
                Ok((tag_id, relate_tag_id, limit_to))
            })
            .unwrap();

        println!("--- Parents Table Contents ---");

        // 3. Iterate and print each row
        for (tag_id, relate_tag_id, limit_to) in parent_rows.flatten() {
            match limit_to {
                Some(limit_id) => {
                    println!(
                        "Tag ID: {tag_id} -> Relate Tag ID: {relate_tag_id} (Limited To: {limit_id})"
                    );
                }
                None => {
                    println!("Tag ID: {tag_id} -> Relate Tag ID: {relate_tag_id}");
                }
            }
        }

        println!("------------------------------");
    }

    ///
    /// Adds relationship into db
    ///
    pub async fn add_relationship_bulk(self: Arc<Self>, rel_list: HashSet<(u64, u64)>) {
        if rel_list.is_empty() {
            return;
        }

        tokio::task::spawn_blocking(move || {
            let self_clone = self.clone();
            let mut writer_conn = self_clone.writer_lock();
            let conn = writer_conn.transaction().unwrap();
            Self::internal_audit_context_set(&conn, "relationship added").unwrap();
            self.internal_relationships_bulk_add(&conn, &rel_list);
            conn.commit().unwrap();
        })
        .await
        .unwrap();
    }

    ///
    /// Deletes relationship into db
    ///
    pub async fn delete_relationship_bulk(self: Arc<Self>, rel_list: HashSet<(u64, u64)>) {
        if rel_list.is_empty() {
            return;
        }

        tokio::task::spawn_blocking(move || {
            let self_clone = self.clone();
            let mut writer_conn = self_clone.writer_lock();
            let conn = writer_conn.transaction().unwrap();
            Self::internal_audit_context_set(&conn, "relationship removed").unwrap();
            self.internal_relationship_bulk_delete(&conn, &rel_list);
            conn.commit().unwrap();
        })
        .await
        .unwrap();
    }
}
