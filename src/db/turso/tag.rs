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

        let relationship_source = self.relationship_union_source(conn, "r").await?;
        let file_id_vec: Vec<u64> = file_ids.iter().copied().collect();
        for chunk in file_id_vec.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT r.file_id, t.name, n.name, n.description
                 FROM {relationship_source}
                 JOIN Tags t ON r.tag_id = t.id
                 JOIN Namespace n ON t.namespace = n.id
                 WHERE r.file_id IN ({placeholders});"
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
