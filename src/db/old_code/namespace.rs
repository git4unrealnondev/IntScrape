//! Database operations for the `namespace` domain.

use super::MainDatabase;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use shared_types::GenericNamespaceObj;
use std::collections::{HashMap, HashSet};

impl MainDatabase {
    ///
    /// Adds the source url or gets it
    ///
    pub fn internal_namespace_sourceurl_get(&self, conn: &Connection) -> u64 {
        self.internal_namespace_get_or_create(
            conn,
            &GenericNamespaceObj {
                name: "source_url".into(),
                description: Some("A source for a file".into()),
            },
        )
    }

    ///
    /// Only gets a namespace id
    ///
    pub fn internal_namespace_get_id(
        &self,
        conn: &Connection,
        namespace_name: &str,
    ) -> Option<u64> {
        if let Some(&namespace_id) = self.namespace_cache.read().get(namespace_name) {
            return Some(namespace_id);
        }
        let mut stmt = conn
            .prepare("SELECT id FROM Namespace WHERE name = ?1")
            .unwrap();

        let namespace_id = stmt
            .query_row(params![namespace_name], |row| row.get(0))
            .optional() // Crucial: converts an Err(QueryReturnedNoRows) into Ok(None)
            .unwrap();
        if let Some(namespace_id) = namespace_id {
            self.namespace_cache
                .write()
                .insert(namespace_name.to_string(), namespace_id);
        }
        namespace_id
    }

    ///
    /// Gets all namespace objects
    ///
    pub fn internal_namespace_get_generic(
        conn: &Connection,
        ns_id: &u64,
    ) -> Option<GenericNamespaceObj> {
        let mut stmt = conn
            .prepare("SELECT name, description FROM Namespace WHERE id = ?1;")
            .unwrap();

        stmt.query_row(params![ns_id], |row| {
            Ok(GenericNamespaceObj {
                name: row.get(0).unwrap(),
                description: row.get(1).unwrap(),
            })
        })
        .optional()
        .unwrap()
    }

    ///
    /// Gets or creates a namespace
    ///
    pub fn internal_namespace_get_or_create(
        &self,
        conn: &Connection,
        namespace: &GenericNamespaceObj,
    ) -> u64 {
        if let Some(&namespace_id) = self.namespace_cache.read().get(&namespace.name) {
            return namespace_id;
        }

        let mut stmt = conn
            .prepare(
                "INSERT INTO Namespace (name, description) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET description = excluded.description
             RETURNING id",
            )
            .unwrap();

        let namespace_id = stmt
            .query_row(params![namespace.name, namespace.description], |row| {
                row.get(0)
            })
            .unwrap();

        self.namespace_cache
            .write()
            .insert(namespace.name.clone(), namespace_id);
        self.internal_relationship_partition_create(conn, namespace_id);
        namespace_id
    }

    ///
    /// Bulk adds namespaces into DB returning their id
    ///
    pub fn internal_namespace_bulk_add(
        &self,
        conn: &Connection,
        namespaces: &HashSet<shared_types::GenericNamespaceObj>,
    ) -> HashMap<shared_types::GenericNamespaceObj, u64> {
        let mut out = HashMap::new();

        if namespaces.is_empty() {
            return out;
        }

        let namespace_vec: Vec<&GenericNamespaceObj> = namespaces.iter().collect();

        let mut query = String::from("INSERT INTO Namespace (name, description) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(namespace_vec.len() * 2);

        // String building
        for (i, namespace) in namespace_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!("(?{}, ?{})", i * 2 + 1, i * 2 + 2));
            params_vector.push(&namespace.name);
            params_vector.push(&namespace.description);
        }

        query.push_str(
            " ON CONFLICT(name) DO UPDATE SET description = excluded.description
              RETURNING id, name",
        );

        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(&*params_vector).unwrap();

        while let Some(row) = rows.next().unwrap() {
            let nsid: u64 = row.get(0).unwrap();
            let namespace_name: String = row.get(1).unwrap();
            if let Some(namespace_obj) = namespace_vec
                .iter()
                .find(|namespace| namespace.name == namespace_name)
            {
                out.insert((**namespace_obj).clone(), nsid);
            }
        }

        let mut new_namespace_ids = Vec::new();
        {
            let mut namespace_cache = self.namespace_cache.write();
            for (namespace, &namespace_id) in &out {
                if namespace_cache
                    .insert(namespace.name.clone(), namespace_id)
                    .is_none()
                {
                    new_namespace_ids.push(namespace_id);
                }
            }
        }
        for namespace_id in new_namespace_ids {
            self.internal_relationship_partition_create(conn, namespace_id);
        }
        out
    }

    /// Removes namespaces where id in list
    pub fn internal_namespace_bulk_delete(
        conn: &Connection,
        ns_ids: &HashSet<u64>,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        if ns_ids.is_empty() {
            return Ok(());
        }

        // Generate a comma-separated list of placeholders: "?, ?, ?"
        let placeholders: Vec<String> = ns_ids.iter().map(|_| "?".to_string()).collect();
        let query = format!(
            "DELETE FROM Namespace WHERE id IN ({});",
            placeholders.join(", ")
        );

        // Execute the query, binding each element in the HashSet as a separate parameter
        conn.execute(&query, params_from_iter(ns_ids))?;

        for ns_id in ns_ids {
            let query = format!("DROP TABLE IF EXISTS Relationship_{};", ns_id);
            conn.execute(&query, [])?;
        }

        Ok(())
    }
}
