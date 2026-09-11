use std::collections::{HashMap, HashSet};

use shared_types::{PluginTag, Tag, TagParents};
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Builds the inlined `SELECT file_id, tag_id FROM Relationship_x UNION ALL ...`
    /// source that spans every namespace's relationship partition.
    pub(in crate::db::turso) async fn relationship_union_source(
        &self,
        conn: &Connection,
        alias: &str,
    ) -> Result<String> {
        let mut tables = Vec::new();
        let mut rows = conn.query("SELECT id FROM Namespace ORDER BY id;", ()).await?;
        while let Some(row) = rows.next().await? {
            let namespace_id: i64 = row.get(0)?;
            tables.push(format!("Relationship_{namespace_id}"));
        }

        let source = if tables.is_empty() {
            "SELECT NULL AS file_id, NULL AS tag_id WHERE 0".to_string()
        } else {
            tables
                .iter()
                .map(|table| format!("SELECT file_id, tag_id FROM {table}"))
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        };

        Ok(format!("({source}) AS {alias}"))
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
        let relationship_source = self.relationship_union_source(conn, "relationships").await?;
        let sql = format!("SELECT tag_id FROM {relationship_source} WHERE file_id = ?1;");

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
        let relationship_source = self.relationship_union_source(conn, "relationships").await?;
        let sql = format!("SELECT file_id FROM {relationship_source} WHERE tag_id = ?1;");

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
        let relationship_source = self.relationship_union_source(conn, "relationships").await?;
        let sql = format!(
            "SELECT DISTINCT relationships.file_id
             FROM {relationship_source}
             WHERE relationships.tag_id = ?1
                OR relationships.tag_id IN (
                    SELECT Parents.relate_tag_id
                    FROM Parents
                    WHERE Parents.tag_id = ?1
                )"
        );

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
        let sql = format!(
            "SELECT tag_id, relate_tag_id, limit_to FROM Parents WHERE {column} = ?1;"
        );

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
        let inserted = conn
            .execute(sql, (file_id as i64, tag_id as i64))
            .await?;
        if inserted > 0 {
            conn.execute(
                "UPDATE Tags SET count = count + 1 WHERE id = ?1;",
                (tag_id as i64,),
            )
            .await?;
        }
        Ok(())
    }

    /// Bulk adds `(file_id, tag_id)` relationships, incrementing tag counts.
    pub(in crate::db::turso) async fn relationships_bulk_add(
        &self,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) -> Result<()> {
        if relationships.is_empty() {
            return Ok(());
        }

        // Resolve every tag's namespace with chunked lookups instead of one
        // point query per relationship.
        let mut tag_namespaces = HashMap::new();
        let tag_ids: Vec<u64> = relationships
            .iter()
            .map(|(_, tag_id)| *tag_id)
            .collect();
        for chunk in tag_ids.chunks(SQL_CHUNK_SIZE) {
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
            let rels: Vec<(u64, u64)> = namespace_relationships.into_iter().collect();
            for chunk in rels.chunks(SQL_CHUNK_SIZE) {
                let mut holders = Vec::with_capacity(chunk.len());
                let mut params = Vec::with_capacity(chunk.len() * 2);
                for (file_id, tag_id) in chunk {
                    holders.push("(?, ?)");
                    params.push(Value::from(*file_id as i64));
                    params.push(Value::from(*tag_id as i64));
                }
                let sql = format!(
                    "INSERT OR IGNORE INTO Relationship_{namespace_id} (file_id, tag_id) VALUES {};",
                    holders.join(", ")
                );
                let inserted = conn.execute(sql, params_from_iter(params)).await?;
                if inserted > 0 {
                    let mut count_deltas: HashMap<u64, u64> = HashMap::new();
                    for (_, tag_id) in chunk {
                        *count_deltas.entry(*tag_id).or_default() += 1;
                    }
                    let (count_sql, count_params) = tag_count_update_sql(&count_deltas, false);
                    conn.execute(count_sql, params_from_iter(count_params)).await?;
                }
            }
        }

        Ok(())
    }

    /// Deletes `(file_id, tag_id)` relationships, decrementing tag counts.
    pub(in crate::db::turso) async fn relationship_bulk_delete(
        &self,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) -> Result<()> {
        if relationships.is_empty() {
            return Ok(());
        }

        let mut tag_namespaces = HashMap::new();
        let tag_ids: Vec<u64> = relationships
            .iter()
            .map(|(_, tag_id)| *tag_id)
            .collect();
        for chunk in tag_ids.chunks(SQL_CHUNK_SIZE) {
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
            for chunk in rels.chunks(SQL_CHUNK_SIZE) {
                let mut clauses = Vec::with_capacity(chunk.len());
                let mut params = Vec::with_capacity(chunk.len() * 2);
                for (file_id, tag_id) in chunk {
                    clauses.push("(file_id = ? AND tag_id = ?)".to_string());
                    params.push(Value::from(*file_id as i64));
                    params.push(Value::from(*tag_id as i64));
                }
                let sql = format!(
                    "DELETE FROM Relationship_{namespace_id} WHERE {};",
                    clauses.join(" OR ")
                );
                let deleted = conn.execute(sql, params_from_iter(params)).await?;
                if deleted > 0 {
                    let mut count_deltas: HashMap<u64, u64> = HashMap::new();
                    for (_, tag_id) in chunk {
                        *count_deltas.entry(*tag_id).or_default() += 1;
                    }
                    let (count_sql, count_params) = tag_count_update_sql(&count_deltas, true);
                    conn.execute(count_sql, params_from_iter(count_params)).await?;
                }
            }
        }

        Ok(())
    }

    /// Deletes a single `(file_id, tag_id)` relationship.
    pub(in crate::db::turso) async fn relationship_delete(
        &self,
        conn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) -> Result<()> {
        let Some(namespace_id) = self.tag_namespace_id(conn, tag_id).await? else {
            return Ok(());
        };
        let sql = format!(
            "DELETE FROM Relationship_{namespace_id} WHERE file_id = ?1 AND tag_id = ?2;"
        );
        let deleted = conn
            .execute(sql, (file_id as i64, tag_id as i64))
            .await?;
        if deleted > 0 {
            conn.execute(
                "UPDATE Tags SET count = MAX(count - 1, 0) WHERE id = ?1;",
                (tag_id as i64,),
            )
            .await?;
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

        let relationship_source = self.relationship_union_source(conn, "r").await?;
        for chunk in file_ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT file_id, tag_id FROM {relationship_source} WHERE file_id IN ({placeholders});"
            );
            let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();

            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let f_id: u64 = row.get(0)?;
                let t_id: u64 = row.get(1)?;
                out.entry(f_id).or_default().insert(t_id);
            }
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
    async fn tag_id_by_name_ns(
        &self,
        conn: &Connection,
        tag: &Tag,
    ) -> Result<Option<u64>> {
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
fn tag_count_update_sql(deltas: &HashMap<u64, u64>, decrement: bool) -> (String, Vec<Value>) {
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
    for tag_id in deltas.keys() {
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
        db.relationships_bulk_add(&conn, &relationships).await.unwrap();
        assert_eq!(
            tag_count(&db, &conn, tag_id).await,
            2,
            "two relationships must increment the count twice"
        );

        db.relationship_bulk_delete(&conn, &relationships).await.unwrap();
        assert_eq!(tag_count(&db, &conn, tag_id).await, 0);
    }
}