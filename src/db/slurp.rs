//! Database operations for the `slurp` domain.

use super::{MainDatabase, SQL_CHUNK_SIZE};
use rusqlite::{Connection, params};
use shared_types::{
    FileInternal, FileTagAction, GenericNamespaceObj, PluginTag, Tag, TagOperation, TagType,
};
use std::collections::HashSet;

impl MainDatabase {
    /// Copies the supported data from another SQLite database without loading
    /// the source tables into memory. Source rows are read-only through ATTACH.
    pub fn db_slurp(&self, source: &std::path::Path) -> Result<(u64, u64, u64), rusqlite::Error> {
        if !source.is_file() {
            return Err(rusqlite::Error::InvalidParameterName(
                "source must be a file".into(),
            ));
        }
        let conn = self.writer_lock();
        let source = source.to_string_lossy();
        conn.execute("ATTACH DATABASE ?1 AS slurp_source", [source.as_ref()])?;
        let result = self.internal_db_slurp_attached(&conn);
        let detach = conn.execute_batch("DETACH DATABASE slurp_source");
        match (result, detach) {
            (Ok(counts), Ok(())) => Ok(counts),
            (Err(error), _) => Err(error),
            (_, Err(error)) => Err(error),
        }
    }

    fn internal_db_slurp_attached(
        &self,
        conn: &Connection,
    ) -> Result<(u64, u64, u64), rusqlite::Error> {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS slurp_namespaces (
                 source_id INTEGER PRIMARY KEY, target_id INTEGER NOT NULL
             );
             CREATE TEMP TABLE IF NOT EXISTS slurp_tags (
                 source_id INTEGER PRIMARY KEY, target_id INTEGER NOT NULL
             );
             DELETE FROM slurp_namespaces;
             DELETE FROM slurp_tags;",
        )?;

        tx.execute(
            "INSERT OR IGNORE INTO FileStorageLocations(location)
             SELECT location FROM slurp_source.FileStorageLocations",
            [],
        )?;
        let namespaces = tx
            .prepare("SELECT name, description FROM slurp_source.Namespace")?
            .query_map([], |row| {
                Ok(GenericNamespaceObj {
                    name: row.get(0)?,
                    description: row.get(1)?,
                })
            })?
            .collect::<Result<HashSet<_>, _>>()?;
        self.internal_namespace_bulk_add(&tx, &namespaces);
        tx.execute(
            "INSERT INTO slurp_namespaces(source_id, target_id)
             SELECT s.id, n.id
             FROM slurp_source.Namespace s
             JOIN Namespace n ON n.name = s.name",
            [],
        )?;
        let mut last_tag_id = 0_u64;
        loop {
            let mut stmt = tx.prepare(
                "SELECT s.id, s.name, n.name, n.description
                 FROM slurp_source.Tags s
                 JOIN slurp_source.Namespace n ON n.id = s.namespace
                 WHERE s.id > ?1
                 ORDER BY s.id
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![last_tag_id, SQL_CHUNK_SIZE], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?;
            let batch: Vec<_> = rows.collect::<Result<_, _>>()?;
            drop(stmt);
            let Some((last_id, _, _, _)) = batch.last() else {
                break;
            };
            let actions = batch
                .iter()
                .map(|(_, name, namespace, description)| FileTagAction {
                    operation: TagOperation::Add,
                    tags: vec![PluginTag {
                        tag: Tag {
                            name: name.clone(),
                            namespace: GenericNamespaceObj {
                                name: namespace.clone(),
                                description: description.clone(),
                            },
                        },
                        tag_type: TagType::NormalNoRegex,
                        relates_to: None,
                    }],
                })
                .collect::<Vec<_>>();
            self.internal_tag_bulk_add(&tx, &actions, self.plugin_manager.clone());
            last_tag_id = *last_id;
        }
        tx.execute(
            "INSERT INTO slurp_tags(source_id, target_id)
             SELECT s.id, t.id
             FROM slurp_source.Tags s
             JOIN slurp_namespaces ns ON ns.source_id = s.namespace
             JOIN Tags t ON t.name = s.name AND t.namespace = ns.target_id",
            [],
        )?;

        let namespace_count = tx.query_row("SELECT count(*) FROM slurp_namespaces", [], |row| {
            row.get(0)
        })?;
        let tag_count = tx.query_row("SELECT count(*) FROM slurp_tags", [], |row| row.get(0))?;

        let file_schema: String = tx.query_row(
            "SELECT sql FROM slurp_source.sqlite_master
             WHERE type = 'table' AND name = 'File'",
            [],
            |row| row.get(0),
        )?;
        let has_size_bytes = file_schema
            .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
            .any(|column| column.eq_ignore_ascii_case("size_bytes"));
        let size_column = if has_size_bytes {
            "f.size_bytes"
        } else {
            "NULL"
        };
        let mut last_file_id = 0_u64;
        loop {
            let file_query = format!(
                "SELECT f.id, f.hash, f.extension, {size_column}, s.location
                 FROM slurp_source.File f
                 LEFT JOIN slurp_source.FileStorageLocations s ON s.id = f.storage_id
                 WHERE f.id > ?1 AND f.hash IS NOT NULL
                 ORDER BY f.id
                 LIMIT ?2"
            );
            let mut stmt = tx.prepare(&file_query)?;
            let rows = stmt.query_map(params![last_file_id, SQL_CHUNK_SIZE], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<u64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?;
            let batch: Vec<_> = rows.collect::<Result<_, _>>()?;
            drop(stmt);
            let Some((last_id, _, _, _, _)) = batch.last() else {
                break;
            };
            let files = batch
                .iter()
                .map(|(_, hash, extension, size_bytes, location)| {
                    let storage_id = location
                        .as_deref()
                        .map(|location| {
                            self.internal_file_storage_location_get_or_create(&tx, location)
                        })
                        .transpose()?
                        .unwrap_or_default();
                    Ok(FileInternal {
                        id: None,
                        hash: hash.clone(),
                        extension: extension.clone(),
                        storage_id,
                        size_bytes: *size_bytes,
                    })
                })
                .collect::<Result<HashSet<_>, rusqlite::Error>>()?;
            self.internal_file_bulk_add(&tx, files);
            last_file_id = *last_id;
        }
        let has_file_hashes: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM slurp_source.sqlite_master
                 WHERE type = 'table' AND name = 'FileHashes'
             )",
            [],
            |row| row.get(0),
        )?;
        if has_file_hashes {
            tx.execute(
                "INSERT OR IGNORE INTO FileHashes(file_id, algorithm, digest)
                 SELECT d.id, h.algorithm, h.digest
                 FROM slurp_source.FileHashes h
                 JOIN slurp_source.File sf ON sf.id = h.file_id
                 JOIN File d ON d.hash = sf.hash",
                [],
            )?;
        }
        let file_count = tx.query_row(
            "SELECT count(*) FROM slurp_source.File WHERE hash IS NOT NULL",
            [],
            |row| row.get(0),
        )?;

        let has_legacy_relationship: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM slurp_source.sqlite_master
                 WHERE type = 'table' AND name = 'Relationship'
             )",
            [],
            |row| row.get(0),
        )?;

        let mut namespaces = tx.prepare("SELECT source_id, target_id FROM slurp_namespaces")?;
        let namespace_rows =
            namespaces.query_map([], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)))?;
        for row in namespace_rows {
            let (source_namespace, _target_namespace) = row?;
            let source_table = format!("Relationship_{source_namespace}");
            let has_partition: bool = tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM slurp_source.sqlite_master
                     WHERE type = 'table' AND name = ?1
                 )",
                [&source_table],
                |row| row.get(0),
            )?;
            let source_table = if has_partition {
                Some(source_table)
            } else if has_legacy_relationship {
                Some("Relationship".to_string())
            } else {
                None
            };
            let Some(source_table) = source_table else {
                continue;
            };
            let mut last_file_id = 0_u64;
            let mut last_tag_id = 0_u64;
            loop {
                let query = format!(
                    "SELECT d.id, tags.target_id, r.file_id, r.tag_id
                     FROM slurp_source.{source_table} r
                     JOIN slurp_tags tags ON tags.source_id = r.tag_id
                     JOIN slurp_source.File sf ON sf.id = r.file_id
                     JOIN File d ON d.hash = sf.hash
                     JOIN slurp_source.Tags source_tag ON source_tag.id = r.tag_id
                         AND source_tag.namespace = ?4
                     WHERE r.file_id > ?1 OR (r.file_id = ?1 AND r.tag_id > ?2)
                     ORDER BY r.file_id, r.tag_id
                     LIMIT ?3"
                );
                let mut stmt = tx.prepare(&query)?;
                let rows = stmt.query_map(
                    params![last_file_id, last_tag_id, SQL_CHUNK_SIZE, source_namespace],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, u64>(2)?,
                            row.get::<_, u64>(3)?,
                        ))
                    },
                )?;
                let batch: Vec<_> = rows.collect::<Result<_, _>>()?;
                drop(stmt);
                let Some((_, _, source_file_id, source_tag_id)) = batch.last() else {
                    break;
                };
                let relationships = batch
                    .iter()
                    .map(|(file_id, tag_id, _, _)| (*file_id, *tag_id))
                    .collect();
                self.internal_relationships_bulk_add(&tx, &relationships);
                last_file_id = *source_file_id;
                last_tag_id = *source_tag_id;
            }
        }
        drop(namespaces);

        let has_parents: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM slurp_source.sqlite_master
                 WHERE type = 'table' AND name = 'Parents'
             )",
            [],
            |row| row.get(0),
        )?;
        if has_parents {
            let mut parents = tx.prepare(
                "SELECT child.target_id, parent.target_id, limit_to.target_id
                 FROM slurp_source.Parents p
                 JOIN slurp_tags child ON child.source_id = p.tag_id
                 JOIN slurp_tags parent ON parent.source_id = p.relate_tag_id
                 LEFT JOIN slurp_tags limit_to ON limit_to.source_id = p.limit_to",
            )?;
            let parent_rows = parents.query_map([], |row| {
                Ok(shared_types::TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: row.get(2)?,
                })
            })?;
            let parent_batch = parent_rows.collect::<Result<HashSet<_>, _>>()?;
            self.internal_parents_bulk_add(&tx, &parent_batch);
        }
        tx.execute_batch("DROP TABLE slurp_tags; DROP TABLE slurp_namespaces;")?;
        tx.commit()?;
        Ok((namespace_count, tag_count, file_count))
    }
}
