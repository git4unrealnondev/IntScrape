//! Database operations for the `tag` domain.

use super::{MainDatabase, SQL_CHUNK_SIZE};
use crate::plugins::PluginManager;
use parking_lot::RwLock;
use rusqlite::{Connection, OptionalExtension, ToSql, params};
use shared_types::{FileInternal, FileTagAction, GenericNamespaceObj, Tag, TagType};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl MainDatabase {
    ///
    /// Gets all 'tag_ids' associated with a namespace
    ///
    pub fn internal_tag_id_get_namespace_id(
        &self,
        conn: &Connection,
        namespace_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT id FROM Tags WHERE namespace = ?1;")?;
        let rows = stmt.query_map(params![namespace_id], |row| row.get(0))?;

        rows.collect()
    }

    pub fn tag_has_files_cached(&self, conn: &Connection, tag_id: u64) -> bool {
        if let Some(guard) = self.relationship_roaring_storage.read().as_ref()
            && let Some(file_ids) = guard.relationship_search_fileid_roaring_in_memory(tag_id)
        {
            return !file_ids.is_empty();
        }

        self.internal_tag_has_files(conn, tag_id)
    }

    ///
    /// Gets a single `file_id` from a tag
    ///
    pub fn internal_tag_get_file_id(&self, conn: &Connection, tag: &Tag) -> Option<u64> {
        if let Some(ns_id) = self.internal_namespace_get_id(conn, &tag.namespace.name)
            && let Some(ref tag_id) = Self::internal_tag_get_id(conn, &tag.name, ns_id)
        {
            return self.internal_tag_id_get_file_id(conn, tag_id).ok();
        }

        None
    }

    ///
    /// Gets a single `file_internal` from a tag
    ///
    pub fn internal_tag_get_fileinternal(
        &self,
        conn: &Connection,
        tag: &Tag,
    ) -> Option<FileInternal> {
        if let Some(ns_id) = self.internal_namespace_get_id(conn, &tag.namespace.name)
            && let Some(ref tag_id) = Self::internal_tag_get_id(conn, &tag.name, ns_id)
            && let Ok(ref file_id) = self.internal_tag_id_get_file_id(conn, tag_id)
        {
            return Self::internal_file_id_get(conn, file_id).ok();
        }

        None
    }

    pub fn internal_tag_id_get_tag(conn: &Connection, tags: &HashSet<u64>) -> HashMap<u64, Tag> {
        let mut out = HashMap::new();

        if tags.is_empty() {
            return out;
        }

        // Convert HashSet to a Vec for chunking and predictable ordering
        let tag_ids: Vec<&u64> = tags.iter().collect();

        for chunk in tag_ids.chunks(SQL_CHUNK_SIZE) {
            // Build a dynamic query containing query parameters for the current chunk: (?1, ?2, ?3...)
            let mut query = String::from(
                "SELECT t.id, t.name, n.name, n.description \
             FROM Tags t \
             JOIN Namespace n ON t.namespace = n.id \
             WHERE t.id IN (",
            );

            let mut params_vector: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len());

            for (i, &id) in chunk.iter().enumerate() {
                if i > 0 {
                    query.push_str(", ");
                }
                query.push_str(&format!("?{}", i + 1));
                params_vector.push(id);
            }
            query.push(')');

            // Prepare the statement and map rows back into your structs for this chunk
            let mut stmt = conn.prepare(&query).unwrap();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params_vector))
                .unwrap();

            while let Some(row) = rows.next().unwrap() {
                let id: u64 = row.get(0).unwrap();
                let tag_name: String = row.get(1).unwrap();
                let namespace_name: String = row.get(2).unwrap();
                let namespace_desc: Option<String> = row.get(3).unwrap();

                let tag = Tag {
                    name: tag_name,
                    namespace: GenericNamespaceObj {
                        name: namespace_name,
                        description: namespace_desc,
                    },
                };

                out.insert(id, tag);
            }
        }

        out
    }

    ///
    /// Gets tags for `file_ids`
    ///
    pub fn internal_file_ids_get_tags(
        &self,
        conn: &Connection,
        file_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<Tag>> {
        let mut out: HashMap<u64, HashSet<Tag>> = HashMap::new();
        if file_ids.is_empty() {
            return out;
        }

        let file_id_vec: Vec<&u64> = file_ids.iter().collect();

        // 1️⃣ Build a bulk query selecting relationships joined with Tags and Namespaces
        let mut query = format!(
            "SELECT r.file_id, t.id, t.name, n.name, n.description \
         FROM {} \
         JOIN Tags t ON r.tag_id = t.id \
         JOIN Namespace n ON t.namespace = n.id \
         WHERE r.file_id IN (",
            self.relationship_union_source(conn, "relationships")
        );

        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(file_id_vec.len());
        for (i, &id) in file_id_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("?{}", i + 1));
            params.push(id);
        }
        query.push(')');

        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(rusqlite::params_from_iter(params)).unwrap();

        // 2️⃣ Hydrate the nested data maps
        while let Some(row) = rows.next().unwrap() {
            let file_id: u64 = row.get(0).unwrap();
            let _tag_id: u64 = row.get(1).unwrap(); // available if you ever need it
            let tag_name: String = row.get(2).unwrap();
            let namespace_name: String = row.get(3).unwrap();
            let namespace_desc: Option<String> = row.get(4).unwrap();

            let tag = Tag {
                name: tag_name,
                namespace: GenericNamespaceObj {
                    name: namespace_name,
                    description: namespace_desc,
                },
            };

            out.entry(file_id).or_default().insert(tag);
        }

        out
    }

    ///
    /// Checks if a tag has a relationship with files
    ///
    pub fn internal_tag_has_files(&self, conn: &Connection, tag_id: u64) -> bool {
        let Ok(namespace_id) = conn.query_row(
            "SELECT namespace FROM Tags WHERE id = ?1",
            [tag_id],
            |row| row.get::<_, u64>(0),
        ) else {
            return false;
        };
        let table = self.relationship_partition_name(namespace_id);
        let mut stmt = conn
            .prepare(&format!(
                "SELECT EXISTS(SELECT 1 FROM {table} WHERE tag_id = ?1)"
            ))
            .unwrap();

        stmt.query_row(params![tag_id], |row| row.get(0))
            .unwrap_or(false) // Returns false if any unexpected error occurs
    }

    ///
    /// Checks to see if a tag exists in the db
    ///
    pub fn internal_tag_get_id(conn: &Connection, name: &str, namespace_id: u64) -> Option<u64> {
        let mut stmt = conn
            .prepare("SELECT id FROM Tags WHERE name = ?1 AND namespace = ?2")
            .unwrap();

        stmt.query_row(params![name, namespace_id], |row| row.get(0))
            .optional() // Turns QueryReturnedNoRows into Ok(None)
            .unwrap()
    }

    ///
    /// Gets the max id from the tags table
    ///
    pub fn internal_tag_get_max_id(&self, conn: &Connection) -> Result<u64, rusqlite::Error> {
        conn.query_one("SELECT COALESCE(MAX(id), 1) FROM Tags;", [], |f| f.get(0))
    }

    ///
    /// Adds tags into db
    ///
    pub fn internal_tag_bulk_add(
        &self,
        conn: &Connection,
        tag_actions: &[FileTagAction],
        plugin_manager: Arc<RwLock<Option<Arc<PluginManager>>>>,
    ) -> HashMap<shared_types::Tag, u64> {
        let mut out = HashMap::new();
        let mut parents = HashSet::new();

        self.tag_search_dirty.store(true, Ordering::SeqCst);

        let is_valid_tag = |tag: &&shared_types::PluginTag| {
            matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
        };

        // 1️⃣ Gather all valid namespaces across all tag actions (unchanged)
        let namespaces: HashSet<shared_types::GenericNamespaceObj> = tag_actions
            .iter()
            .flat_map(|action| &action.tags)
            .flat_map(|t| {
                std::iter::once(t.tag.namespace.clone())
                    .chain(t.relates_to.as_ref().map(|r| r.tag.namespace.clone()))
                    .chain(
                        t.relates_to
                            .as_ref()
                            .and_then(|r| r.limit_to.as_ref())
                            .map(|l| l.namespace.clone()),
                    )
            })
            .collect();

        let namespace_ids = self.internal_namespace_bulk_add(conn, &namespaces);

        // 2️⃣ DEDUPLICATE AND GROUP PLAIN TAGS TO BULK INSERT
        // Collect unique (name, namespace_id) tuples alongside their original struct keys
        let mut pending_tags = Vec::new();
        let mut unique_tags_set = HashSet::new();

        let valid_tags = tag_actions
            .iter()
            .flat_map(|action| &action.tags)
            .filter(is_valid_tag);

        for tag in valid_tags {
            if let Some(&ns_id) = namespace_ids.get(&tag.tag.namespace) {
                if tag.tag.name.is_empty() {
                    continue;
                }
                if unique_tags_set.insert((tag.tag.name.clone(), ns_id)) {
                    pending_tags.push((tag.tag.clone(), ns_id));
                }

                if let Some(relate_tag) = &tag.relates_to {
                    if relate_tag.tag.name.is_empty() {
                        continue;
                    }
                    if let Some(&rel_ns_id) = namespace_ids.get(&relate_tag.tag.namespace) {
                        if unique_tags_set.insert((relate_tag.tag.name.clone(), rel_ns_id)) {
                            pending_tags.push((relate_tag.tag.clone(), rel_ns_id));
                        }

                        if let Some(limit_to_tag) = &relate_tag.limit_to {
                            if limit_to_tag.name.is_empty() {
                                continue;
                            }
                            if let Some(&lim_ns_id) = namespace_ids.get(&limit_to_tag.namespace)
                                && unique_tags_set.insert((limit_to_tag.name.clone(), lim_ns_id))
                            {
                                pending_tags.push((limit_to_tag.clone(), lim_ns_id));
                            }
                        }
                    }
                }
            }
        }

        if pending_tags.is_empty() {
            return out;
        }

        // Gets the largest tag_id in the db for regex filtering
        let max_tag_id = if let Ok(max_id) = self.internal_tag_get_max_id(conn) {
            max_id
        } else {
            return out;
        };

        for chunk in pending_tags.chunks(SQL_CHUNK_SIZE) {
            let mut query = String::from("INSERT INTO Tags (name, namespace) VALUES ");
            let mut params_vector: Vec<&dyn ToSql> = Vec::with_capacity(chunk.len() * 2);

            for (i, (tag_obj, ns_id)) in chunk.iter().enumerate() {
                if i > 0 {
                    query.push_str(", ");
                }
                query.push_str(&format!("(?{}, ?{})", i * 2 + 1, i * 2 + 2));
                params_vector.push(&tag_obj.name);
                params_vector.push(ns_id);
            }
            query.push_str(
                " ON CONFLICT(name, namespace) DO UPDATE SET name = excluded.name
                  RETURNING id, name, namespace",
            );

            let mut stmt = conn.prepare(&query).unwrap();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params_vector))
                .unwrap();

            let pending_by_key: HashMap<(String, u64), shared_types::Tag> = chunk
                .iter()
                .map(|(tag, namespace)| ((tag.name.clone(), *namespace), tag.clone()))
                .collect();

            while let Some(row) = rows.next().unwrap() {
                let tag_id: u64 = row.get(0).unwrap();
                let tag_name: String = row.get(1).unwrap();
                let namespace_id: u64 = row.get(2).unwrap();
                let Some(tag_obj) = pending_by_key.get(&(tag_name, namespace_id)) else {
                    continue;
                };
                out.insert(tag_obj.clone(), tag_id);
            }
        }

        // Handles the regex tags getting added into the db
        {
            let plugin_manager = plugin_manager.write();
            if let Some(plugin_manager) = &*plugin_manager {
                let mut tags_to_add = HashMap::new();
                for (tag, tag_id) in out.iter() {
                    if tag_id > &max_tag_id {
                        tags_to_add.insert(tag.clone(), *tag_id);
                    }
                }
                plugin_manager.add_regex_tags(tags_to_add);
            }
        }

        // 4️⃣ SECOND PASS: Resolve structural parent hierarchies from memory map instantly
        let valid_tags = tag_actions
            .iter()
            .flat_map(|action| &action.tags)
            .filter(is_valid_tag);

        for tag in valid_tags {
            if let Some(&tag_id) = out.get(&tag.tag)
                && let Some(relate_tag) = &tag.relates_to
                && let Some(&relate_tag_id) = out.get(&relate_tag.tag)
            {
                if relate_tag.limit_to.is_none() {
                    parents.insert(shared_types::TagParents {
                        tag_id,
                        relate_tag_id,
                        limit_to: None,
                    });
                }

                if let Some(limit_to_tag) = &relate_tag.limit_to
                    && let Some(&limit_id) = out.get(limit_to_tag)
                {
                    parents.insert(shared_types::TagParents {
                        tag_id,
                        relate_tag_id,
                        limit_to: Some(limit_id),
                    });
                }
            }
        }
        if !parents.is_empty() {
            self.internal_parents_bulk_add(conn, &parents);
        }

        let mut tag_cache = self.tag_cache.write();
        for (tag, &tag_id) in &out {
            tag_cache.insert(tag_id, tag.clone());
        }

        out
    }

    /// Deletes from db where id in
    pub fn internal_tag_bulk_delete(
        conn: &Connection,
        tag_ids: &HashSet<u64>,
    ) -> Result<usize, r2d2_sqlite::rusqlite::Error> {
        if tag_ids.is_empty() {
            return Ok(0);
        }

        // Collect IDs into a Vec
        let ids: Vec<u64> = tag_ids.iter().copied().collect();
        let mut total_deleted = 0;

        for chunk in ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let query = format!("DELETE FROM Tags WHERE id IN ({});", placeholders);

            let affected = conn.execute(&query, r2d2_sqlite::rusqlite::params_from_iter(chunk))?;
            total_deleted += affected;
        }

        Ok(total_deleted)
    }

    ///
    /// Adds tags into db in bulk. Also adds parents
    ///
    pub async fn tags_add_bulk(
        &self,
        tags: &[FileTagAction],
        audit_reason: &str,
    ) -> HashMap<shared_types::Tag, u64> {
        if tags.is_empty() {
            return HashMap::new();
        }

        let tags_owned = tags.to_vec();
        let audit_reason = audit_reason.to_string();
        let database = self.clone();

        let plugin_manager = self.plugin_manager.clone();
        tokio::task::spawn_blocking(move || {
            let out_tags;
            {
                let mut writer_lock_guard = database.writer_lock();
                let tn = match writer_lock_guard.transaction() {
                    Ok(tn) => tn,
                    Err(error) => {
                        log::error!("Failed to begin tag insertion transaction: {error}");
                        return HashMap::new();
                    }
                };
                Self::internal_audit_context_set(&tn, &audit_reason).unwrap();
                out_tags = database.internal_tag_bulk_add(&tn, &tags_owned, plugin_manager.clone());

                if let Err(error) = tn.commit() {
                    log::error!("Failed to commit tag insertion: {error}");
                    return HashMap::new();
                }
            }
            out_tags
        })
        .await
        .unwrap()
    }
}
