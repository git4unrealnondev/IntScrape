use std::collections::{HashMap, HashSet};

use shared_types::GenericNamespaceObj;
use turso::{Connection, Result, Value, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Adds the `source_url` namespace if it doesn't exist, returning its id.
    pub(in crate::db::turso) async fn namespace_sourceurl_get(
        &self,
        conn: &Connection,
    ) -> Result<u64> {
        self.namespace_get_or_create_sql(conn, "source_url", Some("A source for a file".into()))
            .await
    }

    /// Only gets a namespace id by name, using the cache when available.
    pub(in crate::db::turso) async fn namespace_get_id(
        &self,
        conn: &Connection,
        namespace_name: &str,
    ) -> Result<Option<u64>> {
        self.namespace_get(conn, namespace_name).await
    }

    /// Gets a namespace object by id.
    pub(in crate::db::turso) async fn namespace_get_generic(
        &self,
        conn: &Connection,
        ns_id: u64,
    ) -> Result<Option<GenericNamespaceObj>> {
        let mut rows = conn
            .query(
                "SELECT name, description FROM Namespace WHERE id = ?1 LIMIT 1;",
                (ns_id as i64,),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(GenericNamespaceObj {
                name: row.get(0)?,
                description: row.get(1)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// Bulk adds namespaces into db, returning a map of object to id.
    pub(in crate::db::turso) async fn namespace_bulk_add(
        &self,
        conn: &Connection,
        namespaces: &HashSet<GenericNamespaceObj>,
    ) -> Result<HashMap<GenericNamespaceObj, u64>> {
        let mut out = HashMap::new();
        if namespaces.is_empty() {
            return Ok(out);
        }

        let namespace_vec: Vec<&GenericNamespaceObj> = namespaces.iter().collect();

        for chunk in namespace_vec.chunks(SQL_CHUNK_SIZE) {
            let mut holders = Vec::with_capacity(chunk.len());
            let mut params = Vec::with_capacity(chunk.len() * 2);
            let mut name_to_obj: HashMap<String, &GenericNamespaceObj> = HashMap::new();
            for namespace in chunk {
                holders.push("(?, ?)");
                params.push(Value::from(namespace.name.as_str()));
                params.push(match namespace.description.as_deref() {
                    Some(description) => Value::from(description),
                    None => Value::Null,
                });
                name_to_obj.insert(namespace.name.clone(), *namespace);
            }

            let sql = format!(
                "INSERT INTO Namespace (name, description) VALUES {}
                 ON CONFLICT(name) DO UPDATE SET description = excluded.description
                 RETURNING id, name;",
                holders.join(", ")
            );

            let mut rows = conn.query(sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                let nsid: u64 = row.get(0)?;
                let namespace_name: String = row.get(1)?;
                self.namespace_set_cache(nsid, namespace_name.clone()).await;
                if let Some(namespace_obj) = name_to_obj.get(&namespace_name) {
                    out.insert((*namespace_obj).clone(), nsid);
                }
            }
        }

        // Create Relationship partitions for any namespaces that don't have
        // one yet. Creating tables is DDL, which turso only allows inside an
        // exclusive transaction — so skip partitions that already exist and
        // let concurrent-transaction callers avoid schema writes entirely.
        // Turso canonicalizes identifiers to lowercase on disk.
        let partition_names: Vec<String> = out
            .values()
            .map(|ns_id| format!("relationship_{ns_id}"))
            .collect();
        let mut existing_partitions: HashSet<String> = HashSet::new();
        if !partition_names.is_empty() {
            let placeholders = std::iter::repeat_n("?", partition_names.len())
                .collect::<Vec<_>>()
                .join(", ");
            let mut rows = conn
                .query(
                    format!(
                        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN ({placeholders});"
                    ),
                    partition_names
                        .iter()
                        .map(|name| Value::from(name.as_str()))
                        .collect::<Vec<Value>>(),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                existing_partitions.insert(row.get::<String>(0)?.to_ascii_lowercase());
            }
        }
        for ns_id in out.values() {
            let partition = format!("relationship_{ns_id}");
            if !existing_partitions.contains(&partition) {
                self.relationship_partition_create(conn, *ns_id).await?;
            }
        }

        Ok(out)
    }

    /// Ensures every namespace in `namespaces` has a row, a cached id, and its
    /// `Relationship_{id}` partition, using a short exclusive transaction when
    /// anything is missing. Namespace creation runs CREATE TABLE (DDL), which
    /// turso forbids inside BEGIN CONCURRENT — so callers run this *before*
    /// opening their concurrent transaction, after which the tag-add paths hit
    /// the warm in-memory cache and never execute DDL. All-namespaces-cached
    /// is the fast path (no transaction at all); returns the object -> id map.
    pub(in crate::db::turso) async fn namespace_ensure_set(
        &self,
        namespaces: &HashSet<GenericNamespaceObj>,
    ) -> Result<HashMap<GenericNamespaceObj, u64>> {
        if namespaces.is_empty() {
            return Ok(HashMap::new());
        }

        {
            let ns_guard = self.namespace_cache.read().await;
            if namespaces
                .iter()
                .all(|ns| ns_guard.contains_key(ns.name.as_str()))
            {
                return Ok(namespaces
                    .iter()
                    .map(|ns| (ns.clone(), ns_guard[ns.name.as_str()]))
                    .collect());
            }
        }

        loop {
            let conn = self.connect()?;
            if let Err(error) = conn.execute("BEGIN", ()).await {
                if Self::is_concurrency_conflict(&error) {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                return Err(error);
            }
            let ensured = self.namespace_bulk_add(&conn, namespaces).await;
            let committed = conn.execute("COMMIT", ()).await;
            match (ensured, committed) {
                (Ok(map), Ok(_)) => return Ok(map),
                (Err(error), _) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if Self::is_concurrency_conflict(&error) {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    return Err(error);
                }
                (Ok(_), Err(error)) => return Err(error),
            }
        }
    }

    /// Removes namespaces (and their relationship partitions) whose id is in
    /// `ns_ids`.
    pub(in crate::db::turso) async fn namespace_bulk_delete(
        &self,
        conn: &Connection,
        ns_ids: &HashSet<u64>,
    ) -> Result<()> {
        if ns_ids.is_empty() {
            return Ok(());
        }

        let ids: Vec<u64> = ns_ids.iter().copied().collect();
        for chunk in ids.chunks(SQL_CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            conn.execute(
                format!("DELETE FROM Namespace WHERE id IN ({placeholders});"),
                chunk
                    .iter()
                    .map(|id| Value::from(*id as i64))
                    .collect::<Vec<Value>>(),
            )
            .await?;

            for ns_id in chunk {
                conn.execute(format!("DROP TABLE IF EXISTS Relationship_{ns_id};"), ())
                    .await?;
            }
        }

        let mut cache = self.namespace_cache.write().await;
        let mut reverse_cache = self.namespace_cache_reverse.write().await;
        for ns_id in ns_ids {
            if let Some(name) = reverse_cache.remove(ns_id) {
                cache.remove(&name);
            }
        }

        Ok(())
    }
}
