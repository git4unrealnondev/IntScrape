use std::collections::HashMap;
use std::collections::HashSet;

use shared_types::{FileInternal, GenericNamespaceObj, Tag};
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Gets all `tag_ids` associated with a namespace id.
    pub(in crate::db::turso) async fn tag_id_get_namespace_id(
        &self,
        conn: &Connection,
        namespace_id: u64,
    ) -> Result<HashSet<u64>> {
        let mut out = HashSet::new();
        let mut rows = conn
            .query(
                "SELECT id FROM Tags WHERE namespace = ?1;",
                (namespace_id as i64,),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets tag ids associated with a namespace id, newest-created first.
    /// `limit` bounds how many ids are returned; `None` (or zero) falls back
    /// to the unlimited read. `id DESC` rides the primary key index, so a
    /// limited read touches only the newest rows instead of scanning the
    /// whole namespace.
    pub(in crate::db::turso) async fn tag_id_get_namespace_id_limited(
        &self,
        conn: &Connection,
        namespace_id: u64,
        limit: Option<u64>,
    ) -> Result<Vec<u64>> {
        let Some(limit) = limit.filter(|&limit| limit > 0) else {
            let unlimited = self.tag_id_get_namespace_id(conn, namespace_id).await?;
            return Ok(unlimited.into_iter().collect());
        };

        let mut out = Vec::new();
        let mut rows = conn
            .query(
                "SELECT id FROM Tags WHERE namespace = ?1 ORDER BY id DESC LIMIT ?2;",
                (namespace_id as i64, limit as i64),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            out.push(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets every tag id in the database.
    pub(in crate::db::turso) async fn tag_id_get_all(
        &self,
        conn: &Connection,
    ) -> Result<HashSet<u64>> {
        let mut out = HashSet::new();
        let mut rows = conn.query("SELECT id FROM Tags", ()).await?;

        while let Some(row) = rows.next().await? {
            out.insert(row.get(0)?);
        }

        Ok(out)
    }

    /// Gets a tag id by name and namespace id.
    pub(in crate::db::turso) async fn tag_get_id(
        &self,
        conn: &Connection,
        name: &str,
        namespace_id: u64,
    ) -> Result<Option<u64>> {
        let mut rows = conn
            .query(
                "SELECT id FROM Tags WHERE name = ?1 AND namespace = ?2;",
                (name, namespace_id as i64),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    /// Gets the namespace id that a tag id belongs to.
    pub(in crate::db::turso) async fn tag_namespace_id(
        &self,
        conn: &Connection,
        tag_id: u64,
    ) -> Result<Option<u64>> {
        let mut rows = conn
            .query(
                "SELECT namespace FROM Tags WHERE id = ?1;",
                (tag_id as i64,),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    /// Gets a single `file_id` from a tag.
    pub(in crate::db::turso) async fn tag_get_file_id_sql(
        &self,
        conn: &Connection,
        tag: &Tag,
    ) -> Result<Option<u64>> {
        let Some(ns_id) = self.namespace_get(conn, &tag.namespace.name).await? else {
            return Ok(None);
        };
        let Some(tag_id) = self.tag_get_id(conn, &tag.name, ns_id).await? else {
            return Ok(None);
        };

        self.first_file_id_for_tag(conn, tag_id).await
    }

    /// Gets a single `FileInternal` from a tag.
    pub(in crate::db::turso) async fn tag_get_file(
        &self,
        conn: &Connection,
        tag: &Tag,
    ) -> Result<Option<FileInternal>> {
        let Some(file_id) = self.tag_get_file_id_sql(conn, tag).await? else {
            return Ok(None);
        };

        self.file_get(conn, &file_id).await.map(Some)
    }

    /// Batched skip-if resolver for a whole scraped page.
    ///
    /// Resolves each `(namespace name, tag name)` pair to the file it currently
    /// belongs to, keyed by the same pair after a successful resolution. This
    /// is what the per-file `skip_if` check used to do sequentially —
    /// `tag_get_file_id` then `file_id_get`, each with its own connection —
    /// one round trip per tag per file on a page; here the entire page's tag
    /// set costs three batched statements.
    ///
    /// A key resolves to an entry only when the tag has a relationship whose
    /// file row still exists. `MIN(file_id)` preserves the historical
    /// "first file by ascending file_id" choice that `LIMIT 1` made, so
    /// downstream behavior is unchanged. Keys that do not resolve (no tag, no
    /// relationship, missing file row) are simply absent: the caller treats
    /// absence as "no skip", which is the same fallback the sequential path
    /// took and is the safe direction — a failed dedup lookup downloads the
    /// file again, a wrong one would drop it.
    pub(in crate::db::turso) async fn tag_skip_files_sql(
        &self,
        conn: &Connection,
        keys: &HashSet<(String, String)>,
    ) -> Result<HashMap<(String, String), FileInternal>> {
        let mut out: HashMap<(String, String), FileInternal> = HashMap::new();
        if keys.is_empty() {
            return Ok(out);
        }

        // Step 1: resolve (namespace, tag) pairs to tag ids. Two bindings per
        // key, OR'd per pair so the Tags/Namespace join still index-probes on
        // both columns.
        let keys_vec: Vec<&(String, String)> = keys.iter().collect();
        let mut tag_ids: HashSet<u64> = HashSet::new();
        // (tag id -> its (namespace, tag) key) for the final assembly.
        let mut key_of_tag: HashMap<u64, (String, String)> = HashMap::new();

        for chunk in keys_vec.chunks(SQL_CHUNK_SIZE / 2) {
            let pairs = chunk
                .iter()
                .map(|_| "(n.name = ? AND t.name = ?)")
                .collect::<Vec<_>>()
                .join(" OR ");
            let sql = format!(
                "SELECT t.id, t.name, n.name
                 FROM Tags t
                 JOIN Namespace n ON t.namespace = n.id
                 WHERE {pairs};"
            );
            // Positional parameter blocks in turso flatten: one Vec of values
            // repeated (namespace, name) per pair.
            let params: Vec<Value> = chunk
                .iter()
                .flat_map(|(ns_name, tag_name)| {
                    [Value::from(ns_name.clone()), Value::from(tag_name.clone())]
                })
                .collect();

            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let tag_id: u64 = row.get(0)?;
                let tag_name: String = row.get(1)?;
                let ns_name: String = row.get(2)?;
                tag_ids.insert(tag_id);
                key_of_tag.insert(tag_id, (ns_name, tag_name));
            }
        }

        if tag_ids.is_empty() {
            return Ok(out);
        }

        // Step 2: first file id per tag id from the index-probed union of the
        // namespace-partitioned relationship tables, grouped into one result
        // (LIMIT 1 per tag becomes MIN(file_id) GROUP BY tag_id).
        let tables = super::relationship::relationship_union_tables(conn).await;
        let batch = super::relationship::pushed_union_batch_size(tables.len());
        let mut first_files: HashMap<u64, u64> = HashMap::new();

        let tag_ids_vec: Vec<u64> = tag_ids.iter().copied().collect();
        for chunk in tag_ids_vec.chunks(batch) {
            let predicate = format!(
                "tag_id IN ({})",
                super::relationship::numbered_placeholders(chunk.len())
            );
            let relationship_source =
                super::relationship::union_source_from_tables(&tables, "r", Some(&predicate));
            let sql = format!(
                "SELECT r.tag_id, MIN(r.file_id)
                 FROM {relationship_source}
                 GROUP BY r.tag_id;"
            );
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();

            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let tag_id: u64 = row.get(0)?;
                let file_id: u64 = row.get(1)?;
                first_files.insert(tag_id, file_id);
            }
        }

        if first_files.is_empty() {
            return Ok(out);
        }

        // Step 3: bulk-fetch the file rows. A relationship whose file row no
        // longer exists resolves to nothing, matching the old
        // `tag_get_file_id` + `file_id_get` fallback.
        let file_ids: Vec<u64> = first_files.values().copied().collect();
        let files = self.file_get_bulk(conn, &file_ids).await?;

        for (tag_id, file_id) in first_files {
            if let (Some(key), Some(file_internal)) = (key_of_tag.get(&tag_id), files.get(&file_id))
            {
                out.insert(key.clone(), file_internal.clone());
            }
        }

        Ok(out)
    }

    /// Gets the `Tag` objects for a set of tag ids, chunking the lookup.
    pub(in crate::db::turso) async fn tag_id_get_tag(
        &self,
        conn: &Connection,
        tags: &HashSet<u64>,
    ) -> Result<HashMap<u64, Tag>> {
        let mut out = HashMap::new();

        if tags.is_empty() {
            return Ok(out);
        }

        let tag_ids: Vec<u64> = tags.iter().copied().collect();

        for chunk in tag_ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT t.id, t.name, n.name, n.description
                 FROM Tags t
                 JOIN Namespace n ON t.namespace = n.id
                 WHERE t.id IN ({placeholders})"
            );
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();

            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let tag_id: u64 = row.get(0)?;
                let tag_name: String = row.get(1)?;
                let namespace_name: String = row.get(2)?;
                let namespace_desc: Option<String> = row.get(3)?;

                out.insert(
                    tag_id,
                    Tag {
                        name: tag_name,
                        namespace: GenericNamespaceObj {
                            name: namespace_name,
                            description: namespace_desc,
                        },
                    },
                );
            }
        }

        Ok(out)
    }

    /// Deletes tags from db where id in list.
    pub(in crate::db::turso) async fn tag_bulk_delete(
        &self,
        conn: &Connection,
        tag_ids: &HashSet<u64>,
    ) -> Result<usize> {
        if tag_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<u64> = tag_ids.iter().copied().collect();
        let mut total_deleted: u64 = 0;
        for chunk in ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let params = chunk
                .iter()
                .map(|id| Value::from(*id as i64))
                .collect::<Vec<Value>>();
            let deleted = conn
                .execute(
                    format!("DELETE FROM Tags WHERE id IN ({placeholders});"),
                    params,
                )
                .await?;
            total_deleted += deleted;
        }

        Ok(total_deleted as usize)
    }

    /// Checks if a tag has any relationship rows in its namespace partition.
    pub(in crate::db::turso) async fn tag_has_files(
        &self,
        conn: &Connection,
        tag_id: u64,
    ) -> Result<bool> {
        let Some(namespace_id) = self.tag_namespace_id(conn, tag_id).await? else {
            return Ok(false);
        };
        let table = format!("Relationship_{namespace_id}");
        let mut rows = conn
            .query(
                format!("SELECT 1 FROM {table} WHERE tag_id = ?1 LIMIT 1;"),
                (tag_id as i64,),
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Gets the `Tag` objects for a set of file ids, keyed by file id.
    pub(in crate::db::turso) async fn file_ids_get_tags(
        &self,
        conn: &Connection,
        file_ids: &HashSet<u64>,
    ) -> Result<HashMap<u64, HashSet<Tag>>> {
        let mut out: HashMap<u64, HashSet<Tag>> = HashMap::new();
        if file_ids.is_empty() {
            return Ok(out);
        }

        // A HashSet has no order, so collect once to get a stable, sliceable
        // sequence; the ids themselves are what matters, not their order.
        let file_id_vec: Vec<u64> = file_ids.iter().copied().collect();
        // Pushed into each namespace arm so the partitions are index-probed
        // rather than scanned; numbered placeholders keep it one binding, and
        // the namespace list is read once for both the arm count and the text.
        let tables = super::relationship::relationship_union_tables(conn).await;
        let batch = super::relationship::pushed_union_batch_size(tables.len());
        for chunk in file_id_vec.chunks(batch) {
            let predicate = format!(
                "file_id IN ({})",
                super::relationship::numbered_placeholders(chunk.len())
            );
            let relationship_source =
                super::relationship::union_source_from_tables(&tables, "r", Some(&predicate));
            let sql = format!(
                "SELECT r.file_id, t.name, n.name, n.description
                 FROM {relationship_source}
                 JOIN Tags t ON r.tag_id = t.id
                 JOIN Namespace n ON t.namespace = n.id"
            );
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();

            let mut rows = conn.query(sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let file_id: u64 = row.get(0)?;
                let tag_name: String = row.get(1)?;
                let namespace_name: String = row.get(2)?;
                let namespace_desc: Option<String> = row.get(3)?;

                out.entry(file_id).or_default().insert(Tag {
                    name: tag_name,
                    namespace: GenericNamespaceObj {
                        name: namespace_name,
                        description: namespace_desc,
                    },
                });
            }
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::turso::TursoDatabase;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    async fn new_test_db() -> Arc<TursoDatabase> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    }

    /// The batched skip-if resolver must behave exactly like the sequential
    /// `tag_get_file_id` + `file_id_get` pair it replaced:
    ///
    ///  * a tag with a relationship to an existing file resolves to that file,
    ///  * a tag that exists but owns no relationship resolves to nothing,
    ///  * a tag that does not exist at all resolves to nothing,
    ///  * a tag spread over several files picks the first file by ascending
    ///    id, the historical `LIMIT 1` choice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn skip_tag_resolution_matches_only_files_that_exist() {
        let db = new_test_db().await;
        let conn = db.connect().unwrap();

        let skipns = GenericNamespaceObj {
            name: "skipns".into(),
            description: None,
        };
        let otherns = GenericNamespaceObj {
            name: "otherns".into(),
            description: None,
        };
        db.namespace_ensure_set(&HashSet::from([skipns.clone(), otherns.clone()]))
            .await
            .unwrap();

        let storage_id = db
            .file_storage_location_get_or_create(&conn, "test_storage")
            .await
            .unwrap();
        let file_objs: Vec<FileInternal> = (0..2)
            .map(|i| FileInternal {
                id: None,
                hash: format!("skiphash{i:05}"),
                extension: "jpg".into(),
                storage_id,
                size_bytes: Some(1),
            })
            .collect();
        let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
        let (first_file, second_file) = {
            let ids: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();
            (
                *std::cmp::min(&ids[0], &ids[1]),
                *std::cmp::max(&ids[0], &ids[1]),
            )
        };

        // "skipme" exists in skipns and is attached to both files, so its
        // resolution pins the ascending-file-id rule; "lonely" exists in
        // skipns with no relationship; "ghost" lives only in otherns's
        // namespace partition; "otherns_tag" is a real relationship in a
        // different namespace and must resolve through that arm.
        let skipme = Tag {
            name: "skipme".into(),
            namespace: skipns.clone(),
        };
        let lonely = Tag {
            name: "lonely".into(),
            namespace: skipns.clone(),
        };
        let other_tag = Tag {
            name: "otherns_tag".into(),
            namespace: otherns.clone(),
        };
        let tag_ids = db
            .tag_add_bulk(
                &conn,
                &HashSet::from([skipme.clone(), lonely.clone(), other_tag.clone()]),
            )
            .await
            .unwrap();
        let skipme_id = tag_ids.iter().find(|t| t.name == "skipme").unwrap().id as u64;
        let other_id = tag_ids.iter().find(|t| t.name == "otherns_tag").unwrap().id as u64;

        let mut rels: HashSet<(u64, u64)> = HashSet::new();
        rels.insert((first_file, skipme_id));
        rels.insert((second_file, skipme_id));
        rels.insert((first_file, other_id));
        let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
        db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

        let keys = HashSet::from([
            ("skipns".to_string(), "skipme".to_string()),
            ("skipns".to_string(), "lonely".to_string()),
            ("ghost".to_string(), "never_added".to_string()),
            ("otherns".to_string(), "otherns_tag".to_string()),
        ]);
        let resolved = db.tag_skip_files_get(&keys).await;

        assert_eq!(resolved.len(), 2, "only relationship-bearing tags resolve");
        let skipme_file = resolved
            .get(&("skipns".to_string(), "skipme".to_string()))
            .expect("skipme must resolve");
        assert_eq!(
            skipme_file.id,
            Some(first_file),
            "a tag on several files must pick the first by ascending id"
        );
        let other_file = resolved
            .get(&("otherns".to_string(), "otherns_tag".to_string()))
            .expect("otherns_tag must resolve through its own namespace arm");
        assert_eq!(other_file.id, Some(first_file));
        assert!(
            !resolved.contains_key(&("skipns".to_string(), "lonely".to_string())),
            "a tag with no relationship must be absent, not an error or a skip"
        );
    }
}
