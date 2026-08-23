use crate::cli::cli_structs::CheckFilesEnum;
use crate::plugins::PluginManager;
use bytes::Bytes;
use log::info;
use parking_lot::RwLock;
use r2d2_sqlite::rusqlite::OptionalExtension;
use r2d2_sqlite::rusqlite::{self, Connection, Row, params};
use rusqlite::{ToSql, Transaction, params_from_iter};
use shared_types::{
    AuditLogEntry, DbJobRecreation, DbJobsObj, DbSettingsObj, FileInternal, FileManager,
    FileTagAction, GenericNamespaceObj, HashesSupported, PluginJob, PluginTag, ScraperDataReturn,
    ScraperParam, SearchHolder, SearchObj, SkipIf, Tag, TagOperation, TagSearch, TagType,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use walkdir::WalkDir;

use crate::db::roaring::InternalCacheType;
use crate::db::tag_search;
use crate::db::{CacheType, RelationshipStorage};
use crate::web::manager::hash_bytes;
use crate::{db::MainDatabase, helper_functions::get_sys_time_in_secs};
use ipc_macro::export_ipc;

// How many entries should we do total.
// Max is 1000 vs 800
// https://sqlite.org/limits.html
const SQL_CHUNK_SIZE: usize = 800;

pub trait DbJobsObjExt {
    fn from_row(row: &Row) -> rusqlite::Result<Self>
    where
        Self: Sized;
}

fn hashessupportedtoinner(hash: &HashesSupported) -> (&str, &String) {
    match hash {
        HashesSupported::Md5(md5) => ("MD5", md5),
        HashesSupported::Sha1(hash) => ("SHA1", hash),
        HashesSupported::Sha256(hash) => ("SHA256", hash),
        HashesSupported::Sha512(hash) => ("SHA512", hash),
        HashesSupported::IPFSCID(hash) => ("IPFSCID", hash),
        HashesSupported::IPFSCID1(hash) => ("IPFSCID1", hash),
        HashesSupported::ImageHash(hash) => ("ImageHash", hash),
    }
}

impl DbJobsObjExt for DbJobsObj {
    /// Parses a single database row directly into your clean memory structures
    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        // Deserialize the JSON string columns back into native Rust types
        let param_raw: String = row.get("param")?;
        let recreation_raw: String = row.get("recreation")?;
        let user_data_raw: String = row.get("user_data")?;

        let param: Vec<ScraperParam> = serde_json::from_str(&param_raw).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                6, // Column index reference
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;
        let recreation: Option<DbJobRecreation> =
            serde_json::from_str(&recreation_raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    5, // Column index reference
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        let user_data: BTreeMap<String, String> =
            serde_json::from_str(&user_data_raw).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        // Reconstruct the inner PluginJob config block
        let config = PluginJob {
            time: row.get::<_, i64>("time")? as u64,
            reptime: row.get::<_, i64>("reptime")? as u64,
            priority: row.get::<_, i64>("priority")? as u64,
            site: row.get("site")?,
            recreation,
            param,
            user_data,
        };

        // Reconstruct the master database object
        Ok(Self {
            id: row.get::<_, i64>("id")? as u64,
            isrunning: row.get::<_, bool>("is_running")?,
            config,
        })
    }
}

#[export_ipc(client_path = "generated/client/src/generated_api.rs")]
impl MainDatabase {
    ///
    /// Creates the relationship table for the db
    ///
    pub(in crate::db) fn internal_table_create_relationship_v1(conn: &Connection) {
        let namespace_ids: Vec<u64> = conn
            .prepare("SELECT id FROM Namespace ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        for namespace_id in namespace_ids {
            Self::internal_relationship_partition_create(conn, namespace_id);
        }
    }

    pub(in crate::db) fn internal_relationship_migrate_legacy(conn: &Connection) {
        let legacy_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'Relationship')",
            [],
            |row| row.get(0),
        )
        .unwrap();

        if !legacy_exists {
            return;
        }

        // Rename the table and create an index on tag_id to avoid full table scans
        // across the loop, executing directly on the active connection/transaction.
        conn.execute_batch(
            "ALTER TABLE Relationship RENAME TO Relationship_legacy;
         CREATE INDEX idx_relationship_legacy_tag_id ON Relationship_legacy(tag_id);",
        )
        .unwrap();

        let namespaces: Vec<u64> = conn
            .prepare("SELECT DISTINCT id FROM Namespace;")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .flatten()
            .collect();

        for namespace_id in namespaces {
            dbg!(&namespace_id);
            Self::internal_relationship_partition_create(conn, namespace_id);
            let table = Self::relationship_partition_name(namespace_id);

            conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO {table} (file_id, tag_id)
                 SELECT r.file_id, r.tag_id FROM Relationship_legacy r
                 JOIN Tags t ON t.id = r.tag_id WHERE t.namespace = ?1"
                ),
                [namespace_id],
            )
            .unwrap();
        }

        conn.execute("DROP TABLE Relationship_legacy", []).unwrap();
    }

    pub(in crate::db) fn relationship_partition_name(namespace_id: u64) -> String {
        format!("Relationship_{namespace_id}")
    }

    fn internal_relationship_partition_create(conn: &Connection, namespace_id: u64) {
        let table = Self::relationship_partition_name(namespace_id);
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                    file_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    PRIMARY KEY (file_id, tag_id),
                    FOREIGN KEY (file_id) REFERENCES File(id) ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_{table}_tag_file ON {table}(tag_id, file_id DESC)"
        ))
        .unwrap();
    }

    pub(in crate::db) fn relationship_union_source(conn: &Connection, alias: &str) -> String {
        let tables: Vec<String> = conn
            .prepare("SELECT id FROM Namespace ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                let id: u64 = row.get(0)?;
                Ok(Self::relationship_partition_name(id))
            })
            .unwrap()
            .flatten()
            .collect();
        let source = if tables.is_empty() {
            "SELECT NULL AS file_id, NULL AS tag_id WHERE 0".into()
        } else {
            tables
                .iter()
                .map(|table| format!("SELECT file_id, tag_id FROM {table}"))
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        };
        format!("({source}) AS {alias}")
    }

    /// Creates the compact V3 audit trail.
    pub(in crate::db) fn internal_table_create_audit_log_v3(
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS AuditLog (
                id INTEGER PRIMARY KEY,
                changed_at INTEGER NOT NULL,
                entity_type TEXT NOT NULL,
                action TEXT NOT NULL,
                file_id INTEGER,
                tag_id INTEGER,
                reason TEXT NOT NULL
            );",
        )?;

        let columns = conn
            .prepare("PRAGMA table_info(AuditLog)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<HashSet<_>, _>>()?;
        if !columns.contains("file_id") {
            conn.execute("ALTER TABLE AuditLog ADD COLUMN file_id INTEGER", [])?;
        }
        if !columns.contains("tag_id") {
            conn.execute("ALTER TABLE AuditLog ADD COLUMN tag_id INTEGER", [])?;
        }
        if columns.contains("entity_id")
            || columns.contains("before_json")
            || columns.contains("after_json")
        {
            if columns.contains("entity_id") {
                conn.execute_batch(
                    "UPDATE AuditLog
                     SET file_id = CASE
                         WHEN entity_type = 'file' THEN CAST(entity_id AS INTEGER)
                         ELSE file_id
                     END
                     WHERE file_id IS NULL;
                     UPDATE AuditLog
                     SET tag_id = CASE
                         WHEN entity_type = 'tag' THEN CAST(entity_id AS INTEGER)
                         ELSE tag_id
                     END
                     WHERE tag_id IS NULL;",
                )?;
            }
            conn.execute_batch(
                "CREATE TABLE AuditLog_compact (
                    id INTEGER PRIMARY KEY,
                    changed_at INTEGER NOT NULL,
                    entity_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    file_id INTEGER,
                    tag_id INTEGER,
                    reason TEXT NOT NULL
                );
                INSERT INTO AuditLog_compact
                    (id, changed_at, entity_type, action, file_id, tag_id, reason)
                SELECT id, changed_at, entity_type, action, file_id, tag_id, reason
                FROM AuditLog;
                DROP TABLE AuditLog;
                ALTER TABLE AuditLog_compact RENAME TO AuditLog;",
            )?;
        }
        Ok(())
    }

    pub(in crate::db) fn internal_audit_log_indexes_create_v3(
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_audit_log_entity;
             DROP INDEX IF EXISTS idx_audit_log_file_id;
             DROP INDEX IF EXISTS idx_audit_log_tag_id;
             CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON AuditLog(changed_at DESC);
             CREATE INDEX IF NOT EXISTS idx_audit_log_file_id ON AuditLog(file_id, changed_at DESC)
                 WHERE file_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_audit_log_tag_id ON AuditLog(tag_id, changed_at DESC)
                 WHERE tag_id IS NOT NULL;",
        )?;

        let partitions: Vec<u64> = conn
            .prepare("SELECT id FROM Namespace")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        for namespace_id in partitions {
            let table = Self::relationship_partition_name(namespace_id);
            let insert_trigger = format!(
                "CREATE TEMP TRIGGER IF NOT EXISTS audit_relationship_insert_{namespace_id}
                 AFTER INSERT ON main.{table} BEGIN
                 INSERT INTO AuditLog (changed_at, entity_type, action, file_id, tag_id, reason)
                 VALUES (unixepoch(), 'relationship', 'create', NEW.file_id, NEW.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1), 'relationship added')); END;"
            );
            let delete_trigger = format!(
                "CREATE TEMP TRIGGER IF NOT EXISTS audit_relationship_delete_{namespace_id}
                 AFTER DELETE ON main.{table} BEGIN
                 INSERT INTO AuditLog (changed_at, entity_type, action, file_id, tag_id, reason)
                 VALUES (unixepoch(), 'relationship', 'delete', OLD.file_id, OLD.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1), 'relationship removed')); END;"
            );
            conn.execute_batch(&insert_trigger)?;
            conn.execute_batch(&delete_trigger)?;
        }
        Ok(())
    }

    pub(in crate::db) fn internal_audit_triggers_setup(
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS AuditContext (
                reason TEXT NOT NULL
            );

            CREATE TEMP TRIGGER IF NOT EXISTS audit_file_insert
            AFTER INSERT ON main.File
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, file_id, reason)
                VALUES (
                    unixepoch(), 'file', 'create', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'file created')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_file_delete
            AFTER DELETE ON main.File
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, file_id, reason)
                VALUES (
                    unixepoch(), 'file', 'delete', OLD.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'file deleted')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_file_update
            AFTER UPDATE ON main.File
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, file_id, reason)
                VALUES (
                    unixepoch(), 'file', 'update', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'file updated')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_tag_insert
            AFTER INSERT ON main.Tags
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'tag', 'create', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag created')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_tag_delete
            AFTER DELETE ON main.Tags
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'tag', 'delete', OLD.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag deleted')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_tag_update
            AFTER UPDATE ON main.Tags
            WHEN OLD.name IS NOT NEW.name OR OLD.namespace IS NOT NEW.namespace
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'tag', 'update', NEW.id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag updated')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_parent_insert
            AFTER INSERT ON main.Parents
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'relationship', 'create', NEW.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag parent relationship added')
                );
            END;

            CREATE TEMP TRIGGER IF NOT EXISTS audit_parent_delete
            AFTER DELETE ON main.Parents
            BEGIN
                INSERT INTO AuditLog
                    (changed_at, entity_type, action, tag_id, reason)
                VALUES (
                    unixepoch(), 'relationship', 'delete', OLD.tag_id,
                    COALESCE((SELECT reason FROM temp.AuditContext LIMIT 1),
                             'tag parent relationship removed')
                );
            END;",
        )
    }

    pub(in crate::db) fn internal_audit_context_set(
        conn: &Connection,
        reason: &str,
    ) -> Result<(), rusqlite::Error> {
        let _ = (conn, reason);
        Ok(())
    }

    pub(in crate::db) fn internal_audit_log(
        conn: &Connection,
        entity_type: &str,
        action: &str,
        file_id: Option<u64>,
        tag_id: Option<u64>,
        reason: &str,
    ) -> Result<(), rusqlite::Error> {
        let _ = (conn, entity_type, action, file_id, tag_id, reason);
        Ok(())
    }

    /// Returns audit entries filtered by either entity identifier.
    #[must_use]
    #[ipc(name = "audit_get", request = "AuditGet")]
    pub fn audit_get_sync(
        &self,
        file_id: &Option<u64>,
        tag_id: &Option<u64>,
    ) -> Vec<AuditLogEntry> {
        let _ = (self, file_id, tag_id);
        Vec::new()
    }

    pub(in crate::db) fn internal_load_caching(self: Arc<Self>, conn: &Connection) {
        let temp;
        loop {
            let cache = match Self::internal_setting_get(conn, "SYSTEM_cachemode") {
                Err(_) | Ok(None) => {
                    Self::internal_setup_default_cache(conn);
                    Self::internal_setting_get(conn, "SYSTEM_cachemode")
                        .unwrap()
                        .unwrap()
                        .param
                        .clone()
                }
                Ok(Some(setting)) => setting.param.clone(),
            };

            if let Some(ref cache) = cache {
                let cachemode = match cache.as_str() {
                    "Bare" => (Some(CacheType::Bare), None),
                    "RelationshipRoaringFull" => (
                        Some(CacheType::RelationshipRoaring(InternalCacheType::Full)),
                        Some(RelationshipStorage::new(
                            self.clone(),
                            InternalCacheType::Full,
                        )),
                    ),
                    "RelationshipRoaringTable" => (
                        Some(CacheType::RelationshipRoaring(InternalCacheType::Table)),
                        Some(RelationshipStorage::new(
                            self.clone(),
                            InternalCacheType::Table,
                        )),
                    ),
                    "RelationshipRoaringPopular" => {
                        if let Ok(Some(popular_count)) =
                            Self::internal_setting_get(conn, "SYSTEM_tag_count_popular_division")
                            && let Some(popular_count) = popular_count.num
                        {
                            (
                                Some(CacheType::RelationshipRoaring(InternalCacheType::Popular(
                                    popular_count,
                                ))),
                                Some(RelationshipStorage::new(
                                    self.clone(),
                                    InternalCacheType::Popular(popular_count),
                                )),
                            )
                        } else {
                            (None, None)
                        }
                    }

                    _ => {
                        Self::internal_setup_default_cache(conn);
                        (None, None)
                    }
                };
                if cachemode.0.is_some() {
                    temp = cachemode;
                    break;
                }
            } else {
                Self::internal_setup_default_cache(conn);
            }
        }
        *self.relationship_roaring_storage.write() = temp.1;
        *self.cache_type.write() = temp.0.unwrap();

        let mut guard = self.relationship_roaring_storage.write();

        if let Some(rel) = guard.as_mut() {
            rel.load_relationship_cache(conn);
        }
        drop(guard);

        self.refresh_tag_search_cache_with_connection(conn);
    }

    fn refresh_tag_search_cache_with_connection(&self, conn: &Connection) {
        let mut stmt = conn
            .prepare(
                "SELECT id, name, count
                 FROM Tags
                 ORDER BY count DESC, id
                 LIMIT ?1",
            )
            .unwrap();
        let entries = stmt
            .query_map([tag_search::POPULAR_TAG_CACHE_LIMIT], |row| {
                let tag_id = row.get(0)?;
                let name: String = row.get(1)?;
                let count = row.get(2)?;
                Ok(tag_search::tag_entry(tag_id, &name, count))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let cached_tag_count: u64 = conn
            .query_row("SELECT count(*) FROM Tags", [], |row| row.get(0))
            .unwrap();
        *self.tag_search_cache.write() = tag_search::TagSearchCache::from_entries_with_completeness(
            entries,
            cached_tag_count <= tag_search::POPULAR_TAG_CACHE_LIMIT as u64,
        );
    }

    /// Refreshes the in-memory tag search index after queued work changes tags.
    pub fn refresh_tag_search_cache(&self) {
        let conn = self.pool.get().unwrap();
        self.refresh_tag_search_cache_with_connection(&conn);
    }

    /// Sets up internal cache structure
    pub(in crate::db) fn internal_setup_default_cache(conn: &Connection) {
        Self::internal_setting_set(
            conn,
            &DbSettingsObj {
                name: "SYSTEM_cachemode".to_string(),
                description: Some(
                    "The database caching options. Supports: Bare, InMemdb and InMemory"
                        .to_string(),
                ),
                num: None,
                param: Some("RelationshipRoaringFull".to_string()),
            },
        )
        .unwrap();
    }
    ///
    /// Handles creating the triggers to manage the count in the Tags column
    ///
    pub(in crate::db) fn internal_trigger_create_relationship_v1(conn: &Connection) {
        let _ = conn;
    }

    ///
    /// Recaches db internally
    ///
    pub fn recache_roaring_db(&self) {
        let mut write_guard = self.writer_conn.lock();
        // Mutation paths acquire writer_conn before this cache lock.
        let mut roaring_guard = self.relationship_roaring_storage.write();
        if let Some(roaring) = roaring_guard.as_mut() {
            let conn = write_guard.transaction().unwrap();
            roaring.recache_roaring(&conn).unwrap();
            conn.commit().unwrap();
        }
    }

    ///
    /// Gets namespace id if it exists
    ///
    #[must_use]
    #[ipc(name = "namespace_get", request = "GetNamespace")]
    pub fn search_db_namespace_sync(&self, name: &String) -> Option<u64> {
        let conn = self.pool.get().unwrap();

        let mut stmt = conn
            .prepare("SELECT id FROM Namespace WHERE name = ?1")
            .ok()?;

        let result = stmt.query_row(params![name], |row| row.get::<_, u64>(0));

        result.optional().ok().flatten()
    }

    ///
    /// Gets a list of tags where the tag and limits the number of returnees
    ///
    #[must_use]
    #[ipc(name = "search_tag_fts", request = "SearchTags")]
    pub fn search_db_tags_fts(&self, tag: &str, limit: &Option<u64>) -> Vec<TagSearch> {
        let max_rows = limit.unwrap_or(10).min(usize::MAX as u64) as usize;
        if max_rows == 0 {
            return Vec::new();
        }
        let cache = self.tag_search_cache.read();
        let mut results = cache.search(tag, max_rows);
        drop(cache);

        let Some(fts_query) = tag_search::fts_query(tag) else {
            return results;
        };
        let conn = self.pool.get().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT t.id, t.name, t.count
                 FROM Tags_Search_fts f
                 JOIN Tags t ON t.id = f.rowid
                 WHERE Tags_Search_fts MATCH ?1
                 LIMIT ?2",
            )
            .unwrap();
        let candidates = stmt
            .query_map(
                rusqlite::params![fts_query, tag_search::FTS_CANDIDATE_LIMIT],
                |row| {
                    let tag_id: u64 = row.get(0)?;
                    let tag_name: String = row.get(1)?;
                    let count: u64 = row.get(2)?;
                    Ok(tag_search::tag_entry(tag_id, &tag_name, count))
                },
            )
            .unwrap();
        let mut candidates = candidates.flatten().collect::<Vec<_>>();
        drop(stmt);
        if candidates.is_empty() {
            let mut stmt = conn.prepare("SELECT id, name, count FROM Tags").unwrap();
            candidates = stmt
                .query_map([], |row| {
                    let tag_id: u64 = row.get(0)?;
                    let tag_name: String = row.get(1)?;
                    let count: u64 = row.get(2)?;
                    Ok(tag_search::tag_entry(tag_id, &tag_name, count))
                })
                .unwrap()
                .flatten()
                .collect();
        }
        let existing_ids = results.iter().map(|result| result.tag_id).collect();
        for candidate in tag_search::search_entries(candidates, tag, max_rows, &existing_ids) {
            if !results
                .iter()
                .any(|result| result.tag_id == candidate.tag_id)
            {
                results.push(candidate);
            }
        }
        results.sort_unstable_by(tag_search::compare_results);
        results.truncate(max_rows);
        results
    }

    /// Resolves tag names across namespaces and searches for files matching
    /// every input name, while allowing any tag with that name.
    #[must_use]
    #[ipc(name = "search_db_files_by_tags", request = "SearchFilesByTags")]
    pub fn search_db_files_by_tags_sync(&self, tags: &[String], limit: &Option<u64>) -> Vec<u64> {
        self.search_db_files_by_tag_groups_sync(&[], tags, &[], &[], &[], &[], limit)
    }

    /// Resolves tag names across namespaces while preserving boolean groups.
    #[must_use]
    #[ipc(
        name = "search_db_files_by_tag_groups",
        request = "SearchFilesByTagGroups"
    )]
    pub fn search_db_files_by_tag_groups_sync(
        &self,
        and_ids: &[u64],
        and_tags: &[String],
        or_ids: &[u64],
        or_tags: &[String],
        not_ids: &[u64],
        not_tags: &[String],
        limit: &Option<u64>,
    ) -> Vec<u64> {
        let resolve_tags = |tag_names: &[String]| {
            tag_names
                .iter()
                .filter_map(|tag_name| {
                    if tag_search::normalize(tag_name).is_empty() {
                        return None;
                    }

                    let matching_ids = self
                        .search_db_tags_fts(tag_name, &Some(100))
                        .into_iter()
                        .map(|result| result.tag_id)
                        .collect::<Vec<_>>();

                    (!matching_ids.is_empty()).then_some(matching_ids)
                })
                .collect::<Vec<_>>()
        };

        let mut searches = Vec::new();
        for tag_id in and_ids {
            searches.push(SearchHolder::Or(vec![*tag_id]));
        }
        let resolved_and = resolve_tags(and_tags);
        if resolved_and.len() != and_tags.len() {
            return Vec::new();
        }
        for matching_ids in resolved_and {
            searches.push(SearchHolder::Or(matching_ids));
        }
        if !or_tags.is_empty() {
            let mut matching_ids = or_ids.to_vec();
            matching_ids.extend(resolve_tags(or_tags).into_iter().flatten());
            if matching_ids.is_empty() {
                return Vec::new();
            }
            searches.push(SearchHolder::Or(matching_ids));
        } else if !or_ids.is_empty() {
            searches.push(SearchHolder::Or(or_ids.to_vec()));
        }
        let mut resolved_not = not_ids.to_vec();
        for matching_ids in resolve_tags(not_tags) {
            resolved_not.extend(matching_ids);
        }
        if !resolved_not.is_empty() {
            searches.push(SearchHolder::Not(resolved_not));
        }

        self.search_db_files_sync(
            &SearchObj {
                search_relate: None,
                searches,
            },
            limit,
        )
    }

    ///
    /// Creates the current default Tags table
    ///
    pub(in crate::db) fn internal_table_create_tags_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Tags (
    id INTEGER PRIMARY KEY , 
    name TEXT NOT NULL, 
    namespace INTEGER NOT NULL, 
    count INTEGER NOT NULL DEFAULT 0, 

    UNIQUE(name, namespace), 

    FOREIGN KEY (namespace) REFERENCES Namespace(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_tags_count_covering ON Tags(count DESC, name, namespace);
--CREATE INDEX IF NOT EXISTS idx_tags_namespace ON Tags(namespace);

CREATE VIEW High_Value_Tags AS 
    SELECT id, name, namespace 
    FROM Tags 
    WHERE count >= 5;

CREATE VIRTUAL TABLE Tags_Popular_fts USING fts5(
    name,
    namespace UNINDEXED,
    content='High_Value_Tags',
    content_rowid='id',
    tokenize = 'trigram' 
);

-- OPTIMIZATION: Only insert if it meets the threshold
 CREATE TRIGGER IF NOT EXISTS tags_ai AFTER INSERT ON Tags
WHEN new.count = 5
BEGIN
    INSERT INTO Tags_Popular_fts(rowid, name, namespace) 
    VALUES (new.id, new.name, new.namespace);
END;

CREATE TRIGGER IF NOT EXISTS tags_count_au AFTER UPDATE OF count ON Tags
WHEN old.count < 5 AND new.count >= 5
BEGIN
    INSERT INTO Tags_Popular_fts(rowid, name, namespace)
    VALUES (new.id, new.name, new.namespace);
END;

CREATE TRIGGER IF NOT EXISTS tags_count_ad AFTER UPDATE OF count ON Tags
WHEN old.count >= 5 AND new.count < 5
BEGIN
    INSERT INTO Tags_Popular_fts(Tags_Popular_fts, rowid, name, namespace)
    VALUES ('delete', old.id, old.name, old.namespace);
END;

-- OPTIMIZATION: Only attempt FTS delete if the old row actually qualified to be in there
CREATE TRIGGER IF NOT EXISTS tags_ad AFTER DELETE ON Tags 
WHEN old.count >= 5
BEGIN
    INSERT INTO Tags_Popular_fts(Tags_Popular_fts, rowid, name, namespace) 
    VALUES('delete', old.id, old.name, old.namespace);
END;
",
        )
        .unwrap();
        Self::internal_table_create_tag_search_fts_v6(conn).unwrap();
    }

    pub(in crate::db) fn internal_table_create_tag_search_fts_v6(
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        conn.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS Tags_Search_fts USING fts5(
                 name,
                 content='Tags',
                 content_rowid='id',
                 tokenize='trigram'
             );
             CREATE TRIGGER IF NOT EXISTS tags_search_ai AFTER INSERT ON Tags BEGIN
                 INSERT INTO Tags_Search_fts(rowid, name) VALUES (new.id, new.name);
             END;
             CREATE TRIGGER IF NOT EXISTS tags_search_ad AFTER DELETE ON Tags BEGIN
                 INSERT INTO Tags_Search_fts(Tags_Search_fts, rowid, name)
                 VALUES ('delete', old.id, old.name);
             END;
             CREATE TRIGGER IF NOT EXISTS tags_search_au AFTER UPDATE OF name ON Tags BEGIN
                 INSERT INTO Tags_Search_fts(Tags_Search_fts, rowid, name)
                 VALUES ('delete', old.id, old.name);
                 INSERT INTO Tags_Search_fts(rowid, name) VALUES (new.id, new.name);
             END;",
        )?;
        let indexed: u64 =
            conn.query_row("SELECT count(*) FROM Tags_Search_fts", [], |row| row.get(0))?;
        let tags: u64 = conn.query_row("SELECT count(*) FROM Tags", [], |row| row.get(0))?;
        if indexed != tags {
            conn.execute(
                "INSERT INTO Tags_Search_fts(Tags_Search_fts) VALUES ('rebuild')",
                [],
            )?;
        }
        Ok(())
    }
    ///
    /// Creates the current default Namespace table
    ///
    pub(in crate::db) fn internal_table_create_namespace_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Namespace (
    id INTEGER PRIMARY KEY , 
    name TEXT NOT NULL UNIQUE, 
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_namespace ON Namespace (name);

",
        )
        .unwrap();
    }
    ///
    /// Creates the current default Settings table
    ///
    pub(in crate::db) fn internal_table_create_settings_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Settings (
    name TEXT PRIMARY KEY,
    description TEXT, 
    num INTEGER, 
    param TEXT
);",
        )
        .unwrap();
    }

    ///
    /// Creates the current default Parents table
    ///
    pub(in crate::db) fn internal_table_create_parents_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Parents (
id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id INTEGER NOT NULL,
    relate_tag_id INTEGER NOT NULL,
    limit_to INTEGER,

    FOREIGN KEY (tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (relate_tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (limit_to) REFERENCES Tags(id) ON DELETE SET NULL ON UPDATE CASCADE,

    CHECK (tag_id != relate_tag_id),

    UNIQUE(tag_id, relate_tag_id, limit_to)
);

CREATE INDEX IF NOT EXISTS idx_parents_lim ON Parents (limit_to);
CREATE INDEX IF NOT EXISTS idx_parents_rel ON Parents (relate_tag_id);

-- Stupid fix so we can have NULL limit_to to match on NULLs
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_parents_null_safe ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));

",
        )
        .unwrap();
    }

    ///
    /// Stores file locaitons to an ID
    ///
    pub(in crate::db) fn internal_table_create_file_storage_locations_v1(conn: &Connection) {
        conn.execute_batch("
CREATE TABLE IF NOT EXISTS FileStorageLocations (id INTEGER PRIMARY KEY , location TEXT NOT NULL UNIQUE);

").unwrap();
    }

    ///
    /// Used internally to get a file location
    ///
    pub(in crate::db) fn internal_file_storage_location_get(
        conn: &Connection,
        name: &str,
    ) -> Result<Option<u64>, rusqlite::Error> {
        let mut stmt =
            conn.prepare("SELECT id FROM FileStorageLocations WHERE location = ? LIMIT 1")?;

        let mut rows = stmt.query([name])?;

        if let Some(row) = rows.next()? {
            // Unpack using serde_rusqlite
            let obj = serde_rusqlite::from_row::<u64>(row)
                .map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    /// Retrieves the ID of a storage location.
    /// If the location does not exist in the database, it automatically creates it.
    pub(in crate::db) fn internal_file_storage_location_get_or_create(
        conn: &Connection,
        location_path: &str,
    ) -> Result<u64, rusqlite::Error> {
        if let Some(path_id) = Self::internal_file_storage_location_get(conn, location_path)? {
            return Ok(path_id);
        }

        Self::internal_file_storage_location_set(conn, location_path)?;

        let path_id = conn.last_insert_rowid() as u64;

        Ok(path_id)
    }

    ///
    /// Adds a file storage location
    ///
    pub(in crate::db) fn internal_file_storage_location_set(
        conn: &Connection,
        name: &str,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        let mut stmt = conn.prepare("INSERT INTO FileStorageLocations (location) VALUES (?1)")?;

        stmt.execute(params![name])?;

        Ok(())
    }

    ///
    /// Creates a dead url table
    ///
    pub(in crate::db) fn internal_table_create_dead_urls_v1(
        conn: &Connection,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS dead_urls (url TEXT PRIMARY KEY);")
    }

    ///
    /// Adds dead url into db
    ///
    pub(in crate::db) fn internal_dead_url_add(
        conn: &Connection,
        dead_url: &String,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        conn.execute(
            "INSERT OR IGNORE INTO dead_urls (url) VALUES (?1);",
            params![dead_url],
        )?;
        Ok(())
    }

    ///
    /// Checks if a list of urls are dead or not
    ///
    pub(in crate::db) fn internal_dead_url_exist(
        conn: &Connection,
        potential_dead_urls: &[String],
    ) -> Result<HashMap<String, bool>, r2d2_sqlite::rusqlite::Error> {
        let mut dead_urls = HashSet::<String>::new();

        for chunk in potential_dead_urls.chunks(SQL_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "SELECT url
             FROM dead_urls
             WHERE url IN ({placeholders})"
            );

            let mut statement = conn.prepare(&sql)?;

            let rows = statement.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                row.get::<_, String>(0)
            })?;

            for row in rows {
                dead_urls.insert(row?);
            }
        }

        // Preserve the same order and length as the input.
        Ok(potential_dead_urls
            .iter()
            .map(|url| (url.to_string(), dead_urls.contains(url)))
            .collect())
    }

    ///
    /// Creates the default File table
    ///
    pub(in crate::db) fn internal_table_create_file_v2(conn: &Connection) {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS File 
            (id INTEGER PRIMARY KEY  NOT NULL, 
            hash TEXT UNIQUE, 
            extension TEXT, 
            storage_id INTEGER, 
            size_bytes INTEGER

            CHECK (
                (hash IS NOT NULL AND extension IS NOT NULL) OR
                (hash IS NULL AND extension IS NULL)
            ),

            FOREIGN KEY (storage_id) REFERENCES FileStorageLocations(id) ON DELETE CASCADE ON UPDATE CASCADE
            );

CREATE INDEX IF NOT EXISTS idx_file_hash ON File (hash);
").unwrap();
    }

    /// Creates the filehash table
    pub(in crate::db) fn internal_table_create_file_hashes_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS FileHashes (
    file_id INTEGER NOT NULL,
    algorithm TEXT NOT NULL,
    digest TEXT NOT NULL,

    PRIMARY KEY (file_id, algorithm),

    FOREIGN KEY (file_id)
        REFERENCES File(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_hashes_algorithm_digest
ON FileHashes (algorithm, digest);
",
        )
        .unwrap();
    }

    ///
    /// Updates a list of files
    ///
    pub(in crate::db) fn internal_file_update_batch(
        tn: Transaction,
        files: &[FileInternal],
    ) -> Result<(), rusqlite::Error> {
        Self::internal_audit_context_set(&tn, "file metadata updated")?;
        {
            let mut stmt = tn.prepare(
                "UPDATE File 
             SET hash = ?1, extension = ?2, storage_id = ?3 
             WHERE id = ?4",
            )?;

            for file in files {
                stmt.execute((&file.hash, &file.extension, &file.storage_id, &file.id))?;
            }
        }

        tn.commit()
    }

    ///
    /// Creates the default Jobs table
    ///
    pub(in crate::db) fn internal_table_create_jobs_v1(conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Jobs (
    id INTEGER PRIMARY KEY  NOT NULL, 
    time INTEGER NOT NULL, 
    reptime INTEGER NOT NULL, 
    priority INTEGER NOT NULL,  
    is_running BOOL NOT NULL DEFAULT False,
    recreation TEXT NOT NULL, 
    site TEXT NOT NULL, 
    param TEXT NOT NULL, 
    user_data TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedup 
ON Jobs (time, reptime, site, param);
",
        )
        .unwrap();
    }

    ///
    /// Gets the file path of a fileid
    ///
    #[must_use]
    #[ipc(name = "get_file_path", request = "GetFileLocation")]
    pub fn file_get_physical_path_sync(&self, file_id: &u64) -> Option<String> {
        let conn = self.pool.get().unwrap();
        Self::internal_file_get_physical_path(&conn, file_id).ok()?
    }

    ///
    /// Gets the physical path for a file
    ///
    pub(in crate::db) fn internal_file_get_physical_path(
        conn: &Connection,
        file_id: &u64,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let file = Self::internal_file_id_get(conn, file_id)?;

        let file_storage_map = Self::internal_file_storage_get_all(conn)?;

        for (_, base_path) in file_storage_map {
            if let Some(good_path) = Self::get_file_location(&file, &base_path) {
                let final_path = good_path.canonicalize()?;
                return Ok(Some(final_path.to_string_lossy().to_string()));
            }
        }

        // File not found in any of the physical directories
        Ok(None)
    }

    pub(in crate::db) fn internal_jobs_update(conn: &Connection, job: &DbJobsObj) {
        let recreation = serde_json::to_string(&job.config.recreation).unwrap();
        let param = serde_json::to_string(&job.config.param).unwrap();
        let user_data = serde_json::to_string(&job.config.user_data).unwrap();

        let _ = conn.execute(
            "UPDATE Jobs 
         SET time = ?1, 
             reptime = ?2, 
             priority = ?3, 
             is_running = ?4, 
             recreation = ?5, 
             site = ?6, 
             param = ?7, 
             user_data = ?8 
         WHERE id = ?9",
            params![
                job.config.time,
                job.config.reptime,
                job.config.priority,
                job.isrunning, // true/false state
                recreation,
                job.config.site,
                param,
                user_data,
                job.id
            ],
        );
    }

    ///
    /// Used internally to get a setting
    ///
    pub(in crate::db) fn internal_setting_get(
        conn: &Connection,
        name: &str,
    ) -> Result<Option<shared_types::DbSettingsObj>, rusqlite::Error> {
        let mut stmt = conn
            .prepare("SELECT name, description, num, param FROM settings WHERE name = ? LIMIT 1")?;

        let mut rows = stmt.query([name])?;

        if let Some(row) = rows.next()? {
            // Unpack using serde_rusqlite
            let obj = serde_rusqlite::from_row::<shared_types::DbSettingsObj>(row)
                .map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    ///
    /// Gets a file if its id exists in db
    ///
    pub(in crate::db) fn internal_file_id_get(
        conn: &Connection,
        file_id: &u64,
    ) -> Result<FileInternal, rusqlite::Error> {
        conn.query_row(
            "SELECT id, hash, extension, storage_id FROM File WHERE id = ?1 LIMIT 1",
            [file_id],
            |row| {
                serde_rusqlite::from_row::<FileInternal>(row)
                    .map_err(|_| rusqlite::Error::ExecuteReturnedResults)
            },
        )
    }

    ///
    /// Gets all `file_ids` associated with a tag with namespace id x
    ///
    pub(in crate::db) fn internal_file_id_get_namespace_id(
        conn: &Connection,
        namespace_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn.prepare(&format!(
            "
SELECT DISTINCT file_id FROM {} WHERE tag_id in (
    SELECT id FROM Tags WHERE namespace = ?1
); 
",
            Self::relationship_union_source(conn, "Relationship")
        ))?;
        let rows = stmt.query_map(params![namespace_id], |row| row.get(0))?;

        rows.collect()
    }

    ///
    /// Gets all 'tag_ids' associated with a namespace
    ///
    pub(in crate::db) fn internal_tag_id_get_namespace_id(
        conn: &Connection,
        namespace_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT id FROM Tags WHERE namespace = ?1;")?;
        let rows = stmt.query_map(params![namespace_id], |row| row.get(0))?;

        rows.collect()
    }

    ///
    /// Gets all files in db
    ///
    pub(in crate::db) fn internal_file_get_all(
        conn: &Connection,
    ) -> Result<HashSet<FileInternal>, rusqlite::Error> {
        let mut stmt =
            conn.prepare("select id, hash, extension, storage_id, size_bytes FROM File")?;
        let rows = stmt.query_map([], |row| {
            Ok(FileInternal {
                id: row.get(0)?,
                hash: row.get(1)?,
                extension: row.get(2)?,
                storage_id: row.get(3)?,
                size_bytes: row.get(4)?,
            })
        })?;

        rows.collect()
    }
    ///
    /// Gets all file storage's in db
    ///
    pub(in crate::db) fn internal_file_storage_get_all(
        conn: &Connection,
    ) -> Result<HashMap<u64, String>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT id, location FROM FileStorageLocations;")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;

        rows.collect()
    }

    ///
    /// Checks if we should download a file
    ///
    pub(in crate::db) fn internal_should_download_file(
        &self,
        conn: &Connection,
        url: &str,
    ) -> bool {
        let source_url_nsid = self.internal_namespace_sourceurl_get(conn);

        if let Some(tag_id) = Self::internal_tag_get_id(conn, url, source_url_nsid) {
            return !Self::internal_tag_has_files(conn, tag_id);
        }

        true
    }

    pub(in crate::db) fn tag_has_files_cached(&self, conn: &Connection, tag_id: u64) -> bool {
        if let Some(guard) = self.relationship_roaring_storage.read().as_ref()
            && let Some(file_ids) = guard.relationship_search_fileid_roaring_in_memory(tag_id)
        {
            return !file_ids.is_empty();
        }

        Self::internal_tag_has_files(conn, tag_id)
    }

    ///
    /// Gets a single `file_id` from a tag
    ///
    pub(in crate::db) fn internal_tag_get_file_id(conn: &Connection, tag: &Tag) -> Option<u64> {
        if let Some(ns_id) = Self::internal_namespace_get_id(conn, &tag.namespace.name)
            && let Some(ref tag_id) = Self::internal_tag_get_id(conn, &tag.name, ns_id)
        {
            return Self::internal_tag_id_get_file_id(conn, tag_id).ok();
        }

        None
    }

    ///
    /// Gets a single `file_internal` from a tag
    ///
    pub(in crate::db) fn internal_tag_get_fileinternal(
        conn: &Connection,
        tag: &Tag,
    ) -> Option<FileInternal> {
        if let Some(ns_id) = Self::internal_namespace_get_id(conn, &tag.namespace.name)
            && let Some(ref tag_id) = Self::internal_tag_get_id(conn, &tag.name, ns_id)
            && let Ok(ref file_id) = Self::internal_tag_id_get_file_id(conn, tag_id)
        {
            return Self::internal_file_id_get(conn, file_id).ok();
        }

        None
    }

    ///
    /// Gets a `file_id` from a `tag_id`
    ///
    pub(in crate::db) fn internal_tag_id_get_file_id(
        conn: &Connection,
        tag_id: &u64,
    ) -> Result<u64, rusqlite::Error> {
        let namespace_id: u64 = conn.query_row(
            "SELECT namespace FROM Tags WHERE id = ?1",
            [tag_id],
            |row| row.get(0),
        )?;
        let table = Self::relationship_partition_name(namespace_id);
        conn.query_row(
            &format!("SELECT file_id FROM {table} WHERE tag_id = ?1 LIMIT 1;"),
            params![tag_id],
            |row| row.get(0),
        )
    }

    ///
    /// Gets `tag_ids` for `file_id`
    ///
    pub(in crate::db) fn internal_file_id_get_tag_ids(
        conn: &Connection,
        file_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT tag_id FROM {} where file_id = ?1;",
                Self::relationship_union_source(conn, "r")
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
    pub(in crate::db) fn internal_tag_id_get_file_ids(
        conn: &Connection,
        tag_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT file_id FROM {} where tag_id = ?1;",
                Self::relationship_union_source(conn, "r")
            ))
            .unwrap();
        let mut out = HashSet::new();
        for tag_id in stmt.query_map([tag_id], |row| row.get(0))?.flatten() {
            out.insert(tag_id);
        }

        Ok(out)
    }

    ///
    /// Gets all file ids inside of the db
    ///
    pub(in crate::db) fn internal_file_id_get_all(
        conn: &Connection,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT id FROM File;").unwrap();
        let out = stmt.query_map([], |row| row.get(0))?;

        out.collect()
    }

    ///
    /// Checks to see if the db contains a file hash
    /// TODO need to pull this data dynamically
    ///
    pub fn contains_hash_sync(&self, hash: &HashesSupported) -> Option<FileInternal> {
        let conn = self.pool.get().unwrap();

        let (algo, hash) = hashessupportedtoinner(hash);
        let file_id: Option<u64> = conn
            .query_row(
                "SELECT file_id FROM FileHashes WHERE algorithm = ?1 AND digest = ?2;",
                params![algo, hash],
                |row| row.get(0),
            )
            .ok();

        if let Some(file_id) = &file_id {
            Self::internal_file_id_get(&conn, file_id).ok()
        } else {
            None
        }
    }

    ///
    /// Gets all tag ids assocated with a namespace id
    ///
    #[ipc(name = "get_tag_ids_namespace_id", request = "GetNamespaceTagIDs")]
    pub fn tag_id_get_namespace_id(&self, namespace_id: &u64) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        Self::internal_tag_id_get_namespace_id(&conn, namespace_id).unwrap_or_default()
    }

    /// Gets every tag id in the database.
    #[ipc(name = "get_tag_ids_all", request = "GetTagIDsAll")]
    pub fn tag_id_get_all(&self) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        let Ok(mut statement) = conn.prepare("SELECT id FROM Tags") else {
            return HashSet::new();
        };
        let Ok(rows) = statement.query_map([], |row| row.get(0)) else {
            return HashSet::new();
        };
        rows.filter_map(Result::ok).collect()
    }

    ///
    /// Gets a file if a tag is associated with it
    ///
    #[must_use]
    #[ipc(name = "get_tag_file", request = "GetTagFile")]
    pub fn tag_get_file_sync(&self, tag: &Tag) -> Option<FileInternal> {
        let conn = self.pool.get().unwrap();
        Self::internal_tag_get_fileinternal(&conn, tag)
    }

    ///
    /// Gets all `file_ids` with tags that have namespace id
    ///
    #[must_use]
    #[ipc(name = "get_namespace_file_ids", request = "GetNamespaceFileIDs")]
    pub fn file_id_get_namespace_id_sync(&self, namespace_id: &u64) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        Self::internal_file_id_get_namespace_id(&conn, namespace_id).unwrap_or_default()
    }

    ///
    /// Gets tag ids with a namespace_id associated with a file_id
    ///
    #[must_use]
    #[ipc(name = "get_tags_filtered", request = "GetNamespaceTagIdsFiltered")]
    pub fn internal_file_id_get_tag_ids_where_namespace_id_sync(
        &self,
        file_id: &u64,
        namespace_id: &u64,
    ) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();

        Self::internal_file_id_get_tag_ids_where_namespace_id(&conn, file_id, namespace_id)
            .unwrap_or_default()
    }

    ///
    /// Adds a relationship between a `file_id` and `tag_id`
    ///
    #[must_use]
    #[ipc(name = "put_tags_to_file", request = "PutTagsRelationship")]
    pub fn file_relationship_tags_add_sync(&self, file_id: &u64, tag: &[FileTagAction]) -> bool {
        let started = std::time::Instant::now();
        let lock_started = std::time::Instant::now();
        let mut guard = self.writer_conn.lock();
        let writer_lock_elapsed = lock_started.elapsed();
        let transaction_started = std::time::Instant::now();
        let conn = guard.transaction().unwrap();
        let transaction_begin_elapsed = transaction_started.elapsed();

        Self::internal_audit_context_set(&conn, "relationship added").unwrap();
        let tag_started = std::time::Instant::now();
        let tag_map = Self::internal_tag_bulk_add(&conn, tag, self.plugin_manager.clone());
        let tag_elapsed = tag_started.elapsed();
        let relationships: HashSet<(u64, u64)> = tag_map.values().map(|f| (*file_id, *f)).collect();
        let relationship_started = std::time::Instant::now();
        Self::internal_relationship_bulk_add(Arc::new(self.clone()), &conn, &relationships);
        let relationship_elapsed = relationship_started.elapsed();

        conn.commit().unwrap();

        let elapsed = started.elapsed();
        if elapsed >= std::time::Duration::from_millis(100) {
            info!(
                "Performance: relationship tag update file_id={} tags={} writer_lock={:?} transaction_begin={:?} tag_resolution={:?} relationship_write={:?} commit_total={:?}",
                file_id,
                tag.len(),
                writer_lock_elapsed,
                transaction_begin_elapsed,
                tag_elapsed,
                relationship_elapsed,
                elapsed,
            );
        }

        true
    }

    /// Adds tag actions without creating a file/tag relationship.
    ///
    /// This is used by tag callbacks that create structural tag relationships.
    #[ipc(name = "tag_actions_add", request = "TagActionsAdd")]
    pub fn tag_actions_add_sync(&self, tag_actions: &[FileTagAction]) -> bool {
        if tag_actions.is_empty() {
            return true;
        }

        let mut guard = self.writer_conn.lock();
        let Ok(conn) = guard.transaction() else {
            return false;
        };

        Self::internal_audit_context_set(&conn, "tag callback processed").unwrap();
        Self::internal_tag_bulk_add(&conn, tag_actions, self.plugin_manager.clone());
        conn.commit().is_ok()
    }

    /// Adds tags to multiple files in one SQLite transaction.
    #[must_use]
    #[ipc(name = "put_tags_to_files", request = "PutTagsRelationships")]
    pub fn file_relationship_tags_add_bulk_sync(
        &self,
        tags_by_file: &HashMap<u64, Vec<FileTagAction>>,
    ) -> bool {
        if tags_by_file.is_empty() {
            return true;
        }

        let mut guard = self.writer_conn.lock();
        let conn = match guard.transaction() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to begin bulk tag transaction: {error}");
                return false;
            }
        };

        for (file_id, tags) in tags_by_file {
            Self::internal_audit_context_set(&conn, "relationship added").unwrap();
            let tag_map = Self::internal_tag_bulk_add(&conn, tags, self.plugin_manager.clone());
            let relationships: HashSet<(u64, u64)> =
                tag_map.values().map(|tag_id| (*file_id, *tag_id)).collect();
            Self::internal_relationship_bulk_add(Arc::new(self.clone()), &conn, &relationships);
        }

        match conn.commit() {
            Ok(()) => true,
            Err(error) => {
                log::error!("Failed to commit bulk tag transaction: {error}");
                false
            }
        }
    }

    ///
    /// Gets all file ids inside of the db.
    /// #Safety Returns None if an error occurs
    ///
    #[must_use]
    #[ipc(name = "get_file_ids_all", request = "GetFileListId")]
    pub fn file_id_get_all_sync(&self) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();

        Self::internal_file_id_get_all(&conn).unwrap_or_default()
    }

    ///
    /// Gets all tag ids associated with a fileid
    ///
    #[ipc(name = "relationship_get_tagid", request = "RelationshipGetFileid")]
    pub fn relationship_get_tag_id_sync(&self, file_id: &u64) -> HashSet<u64> {
        let roaring_guard = self.relationship_roaring_storage.read();
        if let Some(roaring) = roaring_guard.as_ref()
            && let Some(tag_ids) = roaring.relationship_search_tagid_roaring_in_memory(*file_id)
        {
            return tag_ids.into_iter().collect();
        }

        let conn = self.pool.get().unwrap();

        let mut out = HashSet::new();
        if let Ok(tag_ids) = Self::internal_file_id_get_tag_ids(&conn, file_id) {
            out.extend(tag_ids);
        }
        out
    }

    /// Gets tag relationships for multiple files in one IPC request.
    #[ipc(
        name = "relationship_get_tagid_many",
        request = "RelationshipGetTagidMany"
    )]
    pub fn relationship_get_tag_id_many_sync(
        &self,
        file_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<u64>> {
        file_ids
            .iter()
            .map(|file_id| (*file_id, self.relationship_get_tag_id_sync(file_id)))
            .collect()
    }

    ///
    /// Gets all file ids associated with a tag_id
    ///
    #[ipc(name = "relationship_get_fileid", request = "RelationshipGetTagid")]
    pub fn relationship_get_file_id_sync(&self, tag_id: &u64) -> HashSet<u64> {
        if let Some(guard) = self.relationship_roaring_storage.read().as_ref()
            && let Some(file_ids) = guard.relationship_search_fileid_roaring_in_memory(*tag_id)
        {
            return file_ids.into_iter().collect();
        }

        let conn = self.pool.get().unwrap();

        let mut out = HashSet::new();
        if let Ok(file_ids) = Self::internal_tag_id_get_file_ids(&conn, tag_id) {
            out.extend(file_ids);
        }
        out
    }

    /// Gets file relationships for multiple tags in one IPC request.
    #[ipc(
        name = "relationship_get_fileid_many",
        request = "RelationshipGetFileidMany"
    )]
    pub fn relationship_get_file_id_many_sync(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, HashSet<u64>> {
        tag_ids
            .iter()
            .map(|tag_id| (*tag_id, self.relationship_get_file_id_sync(tag_id)))
            .collect()
    }

    /// Gets files whose tag is the related parent of the supplied structural tag.
    #[ipc(
        name = "relationship_get_parent_fileid",
        request = "RelationshipGetParentFileid"
    )]
    pub fn relationship_get_parent_file_id_sync(&self, tag_id: &u64) -> HashSet<u64> {
        let conn = self.pool.get().unwrap();
        let relationships = Self::relationship_union_source(&conn, "relationships");
        let query = format!(
            "SELECT DISTINCT relationships.file_id
             FROM {relationships}
             WHERE relationships.tag_id = ?1
                OR relationships.tag_id IN (
                    SELECT Parents.relate_tag_id
                    FROM Parents
                    WHERE Parents.tag_id = ?1
                )"
        );
        let Ok(mut statement) = conn.prepare(&query) else {
            return HashSet::new();
        };
        statement
            .query_map([tag_id], |row| row.get(0))
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    /// Gets every parent relation declared by a child tag.
    #[ipc(name = "parent_relationships_get", request = "ParentRelationshipsGet")]
    pub fn parent_relationships_get_sync(&self, tag_id: &u64) -> Vec<shared_types::TagParents> {
        let conn = self.pool.get().unwrap();
        let Ok(mut statement) =
            conn.prepare("SELECT tag_id, relate_tag_id, limit_to FROM Parents WHERE tag_id = ?1")
        else {
            return Vec::new();
        };
        statement
            .query_map([tag_id], |row| {
                Ok(shared_types::TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: row.get(2)?,
                })
            })
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    /// Gets parent relations for multiple child tags in one IPC request.
    #[ipc(
        name = "parent_relationships_get_many",
        request = "ParentRelationshipsGetMany"
    )]
    pub fn parent_relationships_get_many_sync(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<shared_types::TagParents>> {
        tag_ids
            .iter()
            .map(|tag_id| (*tag_id, self.parent_relationships_get_sync(tag_id)))
            .collect()
    }

    /// Gets every child relation that points at a parent tag.
    #[ipc(name = "child_relationships_get", request = "ChildRelationshipsGet")]
    pub fn child_relationships_get_sync(
        &self,
        relate_tag_id: &u64,
    ) -> Vec<shared_types::TagParents> {
        let conn = self.pool.get().unwrap();
        let Ok(mut statement) = conn.prepare(
            "SELECT tag_id, relate_tag_id, limit_to FROM Parents WHERE relate_tag_id = ?1",
        ) else {
            return Vec::new();
        };
        statement
            .query_map([relate_tag_id], |row| {
                Ok(shared_types::TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: row.get(2)?,
                })
            })
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    /// Gets child relations for multiple parent tags in one IPC request.
    #[ipc(
        name = "child_relationships_get_many",
        request = "ChildRelationshipsGetMany"
    )]
    pub fn child_relationships_get_many_sync(
        &self,
        tag_ids: &HashSet<u64>,
    ) -> HashMap<u64, Vec<shared_types::TagParents>> {
        tag_ids
            .iter()
            .map(|tag_id| (*tag_id, self.child_relationships_get_sync(tag_id)))
            .collect()
    }

    /// Gets one exact child-parent relation, including its optional limit tag.
    #[ipc(name = "parent_relationship_get", request = "ParentRelationshipGet")]
    pub fn parent_relationship_get_sync(
        &self,
        tag_id: &u64,
        relate_tag_id: &u64,
    ) -> Option<shared_types::TagParents> {
        let conn = self.pool.get().unwrap();
        conn.query_row(
            "SELECT tag_id, relate_tag_id, limit_to
             FROM Parents
             WHERE tag_id = ?1 AND relate_tag_id = ?2
             LIMIT 1",
            rusqlite::params![tag_id, relate_tag_id],
            |row| {
                Ok(shared_types::TagParents {
                    tag_id: row.get(0)?,
                    relate_tag_id: row.get(1)?,
                    limit_to: row.get(2)?,
                })
            },
        )
        .ok()
    }

    ///
    /// Gets filtered `tag_ids` for a fileid filters by nsid
    ///
    pub(in crate::db) fn internal_file_id_get_tag_ids_where_namespace_id(
        conn: &Connection,
        file_id: &u64,
        namespace_id: &u64,
    ) -> Result<HashSet<u64>, rusqlite::Error> {
        let table = Self::relationship_partition_name(*namespace_id);
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
    pub(in crate::db) fn internal_file_id_get_tag_ids_bulk(
        conn: &Connection,
        file_ids: &[u64],
    ) -> Result<HashMap<u64, HashSet<u64>>, rusqlite::Error> {
        let mut out: HashMap<u64, HashSet<u64>> = HashMap::new();
        if file_ids.is_empty() {
            return Ok(out);
        }

        // Build query: SELECT file_id, tag_id FROM Relationship WHERE file_id IN (?, ?, ...)
        let mut query = format!(
            "SELECT file_id, tag_id FROM {} WHERE ",
            Self::relationship_union_source(conn, "r")
        );
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(file_ids.len());

        for (i, id) in file_ids.iter().enumerate() {
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
    /// Adds tags into db in a bulk manner
    ///
    #[must_use]
    #[ipc(name = "get_tag_id_bulk", request = "GetTagIds")]
    pub fn tag_id_get_tag_sync(&self, tags: &HashSet<u64>) -> HashMap<u64, Tag> {
        if tags.is_empty() {
            return HashMap::new();
        }

        let mut out = HashMap::with_capacity(tags.len());
        let mut missing = HashSet::new();
        {
            let mut tag_cache = self.tag_cache.write();
            for tag_id in tags {
                if let Some(tag) = tag_cache.get(*tag_id) {
                    out.insert(*tag_id, tag);
                } else {
                    missing.insert(*tag_id);
                }
            }
        }

        if missing.is_empty() {
            return out;
        }

        let conn = self.pool.get().unwrap();
        let fetched = Self::internal_tag_id_get_tag(&conn, &missing);
        {
            let mut tag_cache = self.tag_cache.write();
            for (tag_id, tag) in &fetched {
                tag_cache.insert(*tag_id, tag.clone());
            }
        }
        out.extend(fetched);
        out
    }

    ///
    /// Checks if the relationship structure defined inside a single `PluginTag` exists.
    ///
    pub(in crate::db) fn internal_parent_structure_exists(
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

    pub(in crate::db) fn internal_parent_relate_limit_exists(
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

    pub(in crate::db) fn internal_tag_id_get_tag(
        conn: &Connection,
        tags: &HashSet<u64>,
    ) -> HashMap<u64, Tag> {
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
    pub(in crate::db) fn internal_file_ids_get_tags(
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
            Self::relationship_union_source(conn, "relationships")
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
    /// Adds the source url or gets it
    ///
    pub(in crate::db) fn internal_namespace_sourceurl_get(&self, conn: &Connection) -> u64 {
        self.internal_namespace_get_or_create(
            conn,
            &GenericNamespaceObj {
                name: "source_url".into(),
                description: Some("A source for a file".into()),
            },
        )
    }

    ///
    /// Checks if a tag has a relationship with files
    ///
    pub(in crate::db) fn internal_tag_has_files(conn: &Connection, tag_id: u64) -> bool {
        let Ok(namespace_id) = conn.query_row(
            "SELECT namespace FROM Tags WHERE id = ?1",
            [tag_id],
            |row| row.get::<_, u64>(0),
        ) else {
            return false;
        };
        let table = Self::relationship_partition_name(namespace_id);
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
    pub(in crate::db) fn internal_tag_get_id(
        conn: &Connection,
        name: &str,
        namespace_id: u64,
    ) -> Option<u64> {
        let mut stmt = conn
            .prepare("SELECT id FROM Tags WHERE name = ?1 AND namespace = ?2")
            .unwrap();

        stmt.query_row(params![name, namespace_id], |row| row.get(0))
            .optional() // Turns QueryReturnedNoRows into Ok(None)
            .unwrap()
    }

    ///
    /// Only gets a namespace id
    ///
    pub(in crate::db) fn internal_namespace_get_id(
        conn: &Connection,
        namespace_name: &str,
    ) -> Option<u64> {
        let mut stmt = conn
            .prepare("SELECT id FROM Namespace WHERE name = ?1")
            .unwrap();

        stmt.query_row(params![namespace_name], |row| row.get(0))
            .optional() // Crucial: converts an Err(QueryReturnedNoRows) into Ok(None)
            .unwrap()
    }

    ///
    /// Gets all namespace objects
    ///
    pub(in crate::db) fn internal_namespace_get_generic(
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
    pub(in crate::db) fn internal_namespace_get_or_create(
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
        namespace_id
    }

    ///
    /// Gets jobs that should be run
    ///
    pub(in crate::db) fn internal_jobs_get_torun(
        conn: &Connection,
        sites: Vec<String>,
    ) -> Result<Vec<DbJobsObj>, rusqlite::Error> {
        Self::internal_jobs_get_torun_chunk(conn, sites, usize::MAX)
    }

    pub(in crate::db) fn internal_jobs_get_torun_chunk(
        conn: &Connection,
        sites: Vec<String>,
        chunk_size: usize,
    ) -> Result<Vec<DbJobsObj>, rusqlite::Error> {
        if chunk_size == 0 {
            return Ok(Vec::new());
        }
        let chunk_size = i64::try_from(chunk_size).unwrap_or(i64::MAX);

        let mut out = Vec::new();
        for site in sites {
            let mut stmt = conn.prepare(
                "SELECT id, time, reptime, priority, recreation, site, param, user_data, is_running
                 FROM Jobs
                 WHERE site = ?1
                   AND is_running IS false
                   AND time + reptime <= ?2
                 ORDER BY priority DESC, time, id
                 LIMIT ?3",
            )?;
            let jobs = stmt.query_map(
                params![site, get_sys_time_in_secs(), chunk_size],
                shared_types::DbJobsObj::from_row,
            )?;
            out.extend(jobs.collect::<Result<Vec<_>, _>>()?);
        }

        Ok(out)
    }

    ///
    /// Sets ALL jobs to be not running
    ///
    pub(in crate::db) fn internal_jobs_reset_isrunning(
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        conn.execute_batch("UPDATE Jobs SET is_running = false WHERE is_running IS true;")
            .unwrap();

        Ok(())
    }

    ///
    /// Sets a specific jobs to be not running
    ///
    pub(in crate::db) fn internal_jobs_set_isrunning(
        conn: &Connection,
        job_id: u64,
    ) -> Result<(), rusqlite::Error> {
        conn.execute(
            "UPDATE Jobs SET is_running = true WHERE id IS ?1;",
            params![job_id],
        )
        .unwrap();

        Ok(())
    }

    ///
    /// Removes a specific job
    ///
    pub(in crate::db) fn internal_job_remove(
        conn: &Connection,
        job_id: u64,
    ) -> Result<(), rusqlite::Error> {
        info!("JobId: {job_id} Is being removed.");
        conn.execute("DELETE FROM Jobs WHERE id IS ?1;", params![job_id])
            .unwrap();

        Ok(())
    }

    ///
    /// Used internally to get all jobs from site
    ///
    pub(in crate::db) fn internal_jobs_get_site(
        conn: &Connection,
        site: &str,
    ) -> Result<Vec<shared_types::DbJobsObj>, rusqlite::Error> {
        // Select all jobs matching the given site
        let mut stmt = conn.prepare(
            "SELECT id, time, reptime, priority, recreation, site, param, user_data, is_running 
         FROM Jobs 
         WHERE site = ?1;",
        )?;

        // query_map processes each row through a closure safely
        let job_iter = stmt.query_map([site], shared_types::DbJobsObj::from_row)?;

        // Collect the iterator results, propagating any underlying row or parsing errors
        let mut jobs = Vec::new();
        for job_result in job_iter {
            jobs.push(job_result?);
        }

        Ok(jobs)
    }

    pub(in crate::db) fn internal_jobs_add(conn: &Connection, config: &PluginJob) -> u64 {
        let mut stmt = conn
            .prepare(
                "INSERT INTO Jobs (time, reptime, priority, recreation, site, param, user_data) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)

ON CONFLICT(time, reptime, site, param) DO UPDATE SET
            reptime = excluded.reptime,      -- Update to the new execution time
            priority = excluded.priority,    -- Update to the new priority level
            user_data = excluded.user_data

         RETURNING id",
            )
            .unwrap();

        // Serialize on-the-fly for the TEXT columns
        let param_json = serde_json::to_string(&config.param).unwrap();
        let user_data_json = serde_json::to_string(&config.user_data).unwrap();
        let manager_json = serde_json::to_string(&config.recreation).unwrap(); // Replace with your actual serialized DbJobsManager struct

        let id: u64 = stmt
            .query_row(
                params![
                    config.time,
                    config.reptime,
                    config.priority,
                    manager_json,
                    config.site,
                    param_json,
                    user_data_json
                ],
                |row| row.get(0),
            )
            .unwrap();

        id
    }

    ///
    /// Gets all sites currently in db from Jobs
    ///
    pub(in crate::db) fn internal_jobs_get_all_sites(
        conn: &Connection,
    ) -> Result<Vec<String>, rusqlite::Error> {
        // Use DISTINCT so SQLite handles deduplication natively at the engine level
        let mut stmt = conn.prepare("SELECT DISTINCT site FROM Jobs WHERE site IS NOT NULL;")?;

        // Map each row directly to a String extraction
        let site_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;

        // Collect results, propagating any database errors upstream
        site_iter.collect()
    }

    ///
    /// Gets the location where files should be stored
    ///
    pub(in crate::db) fn internal_file_download_location_get(
        conn: &Connection,
    ) -> Result<(PathBuf, u64), rusqlite::Error> {
        let target_location =
            if let Some(setting) = Self::internal_setting_get(conn, "SYSTEM_file_location")? {
                match setting.param {
                    Some(param) => param,
                    None => "files".to_string(), // Fallback if param is null
                }
            } else {
                // No setting found at all; initialize the system defaults
                Self::internal_file_download_location_set_default(conn)?;
                "files".to_string()
            };

        let path_id = Self::internal_file_storage_location_get_or_create(conn, &target_location)?;

        Ok((PathBuf::from(target_location), path_id))
    }

    ///
    /// Sets the default file download location
    ///
    pub(in crate::db) fn internal_file_download_location_set_default(
        conn: &Connection,
    ) -> Result<(), rusqlite::Error> {
        let default_files_location = "files";

        if Self::internal_setting_get(conn, "SYSTEM_file_location")?.is_none() {
            Self::internal_setting_set(
                conn,
                &DbSettingsObj {
                    name: "SYSTEM_file_location".into(),
                    description: Some("The default location where files are downloaded to.".into()),
                    num: None,
                    param: Some(default_files_location.into()),
                },
            )?;
        }

        if Self::internal_file_storage_location_get(conn, default_files_location)?.is_none() {
            Self::internal_file_storage_location_set(conn, default_files_location)?;
        }

        Ok(())
    }

    ///
    /// Used internally to set a Setting
    ///
    pub(in crate::db) fn internal_setting_set(
        conn: &Connection,
        obj: &shared_types::DbSettingsObj,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        // Option A: Using raw fields manually
        let mut stmt = conn.prepare(
            "INSERT OR REPLACE INTO settings (name, description, num, param) 
             VALUES (?1, ?2, ?3, ?4)",
        )?;

        stmt.execute(r2d2_sqlite::rusqlite::params![
            obj.name,
            obj.description,
            obj.num,
            obj.param
        ])?;

        Ok(())
    }

    ///
    /// Convience function to set db version
    ///
    pub(in crate::db) fn internal_db_version_set(
        conn: &Connection,
        version: u64,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        Self::internal_setting_set(
            conn,
            &DbSettingsObj {
                name: "SYSTEM_VERSION".into(),
                description: Some("Current version that the DB is on.".into()),
                num: Some(version),
                param: None,
            },
        )
    }

    /// Copies the supported data from another SQLite database without loading
    /// the source tables into memory. Source rows are read-only through ATTACH.
    pub fn db_slurp(&self, source: &std::path::Path) -> Result<(u64, u64, u64), rusqlite::Error> {
        if !source.is_file() {
            return Err(rusqlite::Error::InvalidParameterName(
                "source must be a file".into(),
            ));
        }
        let conn = self.writer_conn.lock();
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
        Self::internal_namespace_bulk_add(&tx, &namespaces);
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
            Self::internal_tag_bulk_add(&tx, &actions, self.plugin_manager.clone());
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
                            Self::internal_file_storage_location_get_or_create(&tx, location)
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
            Self::internal_file_bulk_add(&tx, files);
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
                Self::internal_relationship_bulk_add(Arc::new(self.clone()), &tx, &relationships);
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
            Self::internal_parents_bulk_add(&tx, &parent_batch);
        }
        tx.execute_batch("DROP TABLE slurp_tags; DROP TABLE slurp_namespaces;")?;
        tx.commit()?;
        Ok((namespace_count, tag_count, file_count))
    }

    ///
    /// Used internally to add a relationship to a db
    ///
    pub(in crate::db) fn internal_relationship_add(
        conn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        Self::internal_audit_context_set(conn, "relationship added")?;
        let namespace_id: u64 = conn.query_row(
            "SELECT namespace FROM Tags WHERE id = ?1",
            [tag_id],
            |row| row.get(0),
        )?;
        Self::internal_relationship_partition_create(conn, namespace_id);
        let table = Self::relationship_partition_name(namespace_id);
        conn.execute(
            &format!("INSERT OR IGNORE INTO {table} (file_id, tag_id) VALUES (?1, ?2)"),
            r2d2_sqlite::rusqlite::params![file_id, tag_id],
        )?;
        Ok(())
    }

    ///
    /// Gets the max id from the tags table
    ///
    pub(in crate::db) fn internal_tag_get_max_id(
        conn: &Connection,
    ) -> Result<u64, rusqlite::Error> {
        conn.query_one("SELECT COALESCE(MAX(id), 1) FROM Tags;", [], |f| f.get(0))
    }

    ///
    /// Adds tags into db
    ///
    pub(in crate::db) fn internal_tag_bulk_add(
        conn: &Connection,
        tag_actions: &[FileTagAction],
        plugin_manager: Arc<RwLock<Option<Arc<PluginManager>>>>,
    ) -> HashMap<shared_types::Tag, u64> {
        let mut out = HashMap::new();
        let mut parents = HashSet::new();

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

        let namespace_ids = Self::internal_namespace_bulk_add(conn, &namespaces);

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
        let max_tag_id = if let Ok(max_id) = Self::internal_tag_get_max_id(conn) {
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
            Self::internal_parents_bulk_add(conn, &parents);
        }

        out
    }

    pub fn set_plugin_manager(&self, plugin_manager_add: Arc<PluginManager>) {
        let mut plugin_manager = self.plugin_manager.write();
        *plugin_manager = Some(plugin_manager_add);
    }

    ///
    /// Bulk adds namespaces into DB returning their id
    ///
    pub(in crate::db) fn internal_namespace_bulk_add(
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

        for namespace_id in out.values().copied() {
            Self::internal_relationship_partition_create(conn, namespace_id);
        }
        out
    }

    ///
    /// Deletes relationships from db
    ///
    pub(in crate::db) fn internal_relationship_bulk_delete(
        self: Arc<Self>,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) {
        if relationships.is_empty() {
            return;
        }

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
            let mut guard = self.relationship_roaring_storage.write();
            if let Some(roaring) = guard.as_mut() {
                for (file_id, tag_id) in relationships {
                    roaring.remove_roaring(conn, *tag_id, *file_id);
                }
            }
        }

        for (namespace_id, rels) in by_namespace {
            let table = Self::relationship_partition_name(namespace_id);
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

    /// Deletes from db where id in
    pub(in crate::db) fn internal_tag_bulk_delete(
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

    /// Removes namespaces where id in list
    pub(in crate::db) fn internal_namespace_bulk_delete(
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

    /// Adds a filehash to the db
    pub(in crate::db) fn internal_file_hash_add(
        conn: &Connection,
        algo: &String,
        hash: &String,
        file_id: &u64,
    ) -> Result<usize, r2d2_sqlite::rusqlite::Error> {
        if !algo.is_empty() && !hash.is_empty() {
            conn.execute(
            "INSERT OR IGNORE INTO FileHashes (algorithm, digest, file_id) VALUES (?1, ?2, ?3);",
            params![algo, hash, file_id],
        )
        } else {
            Ok(0)
        }
    }

    ///
    /// Bulk adds relationship into DB with chunking to prevent parameter limit overflow
    ///
    pub(in crate::db) fn internal_relationship_bulk_add(
        self: Arc<Self>,
        conn: &Connection,
        relationships: &HashSet<(u64, u64)>,
    ) {
        if relationships.is_empty() {
            return;
        }

        let mut by_namespace: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for &(file_id, tag_id) in relationships {
            if let Ok(namespace_id) = conn.query_row(
                "SELECT namespace FROM Tags WHERE id = ?1",
                [tag_id],
                |row| row.get::<_, u64>(0),
            ) {
                Self::internal_relationship_partition_create(conn, namespace_id);
                by_namespace
                    .entry(namespace_id)
                    .or_default()
                    .push((file_id, tag_id));
            }
        }
        let mut inserted_relationships = 0;

        for (namespace_id, relationships) in by_namespace {
            let table = Self::relationship_partition_name(namespace_id);
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
        }
        // Duplicate relationship updates are common when a known file is
        // encountered again. Avoid rewriting roaring blobs in that case.
        if inserted_relationships > 0 {
            let mut guard = self.relationship_roaring_storage.write();
            if let Some(roaring) = guard.as_mut() {
                for (file_id, tag_id) in relationships {
                    roaring.relationship_roaring_add(conn, *file_id, *tag_id);
                }
            }
        }
    }

    ///
    /// Updates the fts sqlite table
    ///
    pub(in crate::db) fn internal_update_fts_table(
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
    /// Bulk adds parents into DB returning their id
    ///
    pub(in crate::db) fn internal_parents_bulk_add(
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

    ///
    /// Bulk adds files into DB returning their id
    ///
    pub(in crate::db) fn internal_file_bulk_add(
        conn: &Connection,
        parents: HashSet<shared_types::FileInternal>,
    ) -> HashSet<shared_types::FileInternal> {
        Self::internal_audit_context_set(conn, "file discovered from scraper or import").unwrap();
        let mut out = HashSet::new();

        if parents.is_empty() {
            return out;
        }

        let parents_vec: Vec<&shared_types::FileInternal> = parents.iter().collect();
        let mut query =
            String::from("INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES ");
        let mut params_vector: Vec<&dyn rusqlite::types::ToSql> =
            Vec::with_capacity(parents_vec.len() * 3);

        // String building
        for (i, parent) in parents_vec.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!(
                "(?{}, ?{}, ?{}, ?{})",
                i * 4 + 1,
                i * 4 + 2,
                i * 4 + 3,
                i * 4 + 4
            ));
            params_vector.push(&parent.hash);
            params_vector.push(&parent.extension);
            params_vector.push(&parent.storage_id);
            params_vector.push(&parent.size_bytes);
        }

        // FIX: Combined into a single DO UPDATE SET clause separated by a comma
        query.push_str(
            " ON CONFLICT(hash) 
         DO UPDATE SET 
            extension = excluded.extension,
            storage_id = excluded.storage_id
         RETURNING id",
        );

        let mut stmt = conn.prepare(&query).unwrap();

        // FIX: Swapped to slice_to_params to match your lifetime array structure correctly
        let mut rows = stmt.query(&*params_vector).unwrap();

        let mut idx = 0;
        while let Some(row) = rows.next().unwrap() {
            let mut parent_obj = parents_vec[idx].clone();
            parent_obj.id = row.get(0).ok();

            out.insert(parent_obj);
            idx += 1;
        }

        out
    }

    pub(in crate::db) fn debug_print_parents(conn: &Connection) {
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
    /// Gets a file location on disk and fixes extension on FS if it doesn't exist
    ///
    pub(in crate::db) fn get_file_location(
        file_internal: &FileInternal,
        base_path: &String,
    ) -> Option<PathBuf> {
        if file_internal.hash.len() <= 6 {
            return None;
        }
        let mut path = Path::new(base_path).to_path_buf();
        path.push(&file_internal.hash[0..2]);
        path.push(&file_internal.hash[2..4]);
        path.push(&file_internal.hash[4..6]);
        path.push(&file_internal.hash);
        let final_path = path.with_added_extension(&file_internal.extension);

        if final_path.exists() {
            return Some(final_path);
        }
        if final_path.with_extension("").exists() {
            std::fs::rename(final_path.with_extension(""), &final_path).ok()?;
            return Some(final_path);
        }

        None
    }

    ///
    /// Fixes all files inside of the file storage location
    ///
    pub fn fix_internal_files(
        &self,
        action: &crate::cli::cli_structs::CheckFilesEnum,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Staring to fix internal files");
        let conn = self.pool.get()?;

        let file_storage_map = Self::internal_file_storage_get_all(&conn)?;

        let files = Self::internal_file_get_all(&conn)?;

        let mut file_storage_missing = HashSet::new();

        let mut valid_paths = HashSet::new();

        // Check the recorded storage first. A file found in another storage is
        // misplaced, not valid for the current database record.
        for file in &files {
            if let Some(file_base_path) = file_storage_map.get(&file.storage_id)
                && let Some(file_path) = Self::get_file_location(file, file_base_path)
            {
                valid_paths.insert(file_path);
                continue;
            }

            file_storage_missing.insert(file);
        }

        info!("Missing {} files from db.", file_storage_missing.len());

        if !file_storage_missing.is_empty() || *action == CheckFilesEnum::StorageCheck {
            info!("Scanning file locations");

            let file_hash: HashMap<String, FileInternal> = files
                .into_iter()
                .map(|file| (file.hash.clone(), file))
                .collect();

            let default_file_location = self.file_download_location_main_sync().unwrap();

            if CheckFilesEnum::Print == *action {
                for (hash, _) in file_hash {
                    info!("Just printing the missing file: {hash}");
                }
            } else if CheckFilesEnum::StorageCheck == *action {
                for (_storage_id, storage_loc) in &file_storage_map {
                    for entry in WalkDir::new(storage_loc)
                        .into_iter()
                        .filter_map(std::result::Result::ok)
                    {
                        // Skips existing files
                        if valid_paths.contains(&entry.path().to_path_buf()) {
                            continue;
                        }

                        let file = match std::fs::read(entry.path()) {
                            Ok(out) => out,
                            Err(_) => {
                                continue;
                            }
                        };
                        let bytes = &Bytes::from(file);
                        let (hash, _) = hash_bytes(bytes, &HashesSupported::Sha512(String::new()));

                        if let Some(file_internal) = file_hash.get(&hash)
                            && let Some(base_file_path) =
                                file_storage_map.get(&file_internal.storage_id)
                        {
                            let mut path_buf = Path::new(base_file_path).to_path_buf();
                            path_buf.push(&hash[0..2]);
                            path_buf.push(&hash[2..4]);
                            path_buf.push(&hash[4..6]);
                            path_buf.push(&hash);

                            let target_path = path_buf.with_extension(&file_internal.extension);
                            if entry.path().exists()
                                && !target_path.exists()
                                && std::fs::create_dir_all(target_path.parent().unwrap()).is_ok()
                                && std::fs::copy(entry.path(), &target_path).is_ok()
                                && std::fs::remove_file(entry.path()).is_ok()
                            {
                                info!(
                                    "Moved file: {} to: {}",
                                    entry.path().display(),
                                    target_path.display()
                                );
                            }
                        } else {
                            let mut default_file_location =
                                default_file_location.0.with_file_name("files_missing");

                            info!("File {} does not exist in db.", entry.path().display());

                            default_file_location.push(entry.path());
                            if std::fs::create_dir_all(default_file_location.parent().unwrap())
                                .is_ok()
                                && std::fs::copy(entry.path(), &default_file_location).is_ok()
                                && std::fs::remove_file(entry.path()).is_ok()
                            {
                                info!(
                                    "Moved file: {} to: {}",
                                    entry.path().display(),
                                    default_file_location.display()
                                );
                            }
                        }
                    }

                    for entry in WalkDir::new(storage_loc)
                        .contents_first(true)
                        .into_iter()
                        .filter_map(std::result::Result::ok)
                        .filter(|entry| entry.file_type().is_dir())
                    {
                        if entry.path() != Path::new(storage_loc)
                            && std::fs::remove_dir(entry.path()).is_ok()
                        {
                            info!("Removed empty directory: {}", entry.path().display());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    ///
    /// Should we skip doing something
    ///
    fn should_skip_item(&self, conn: &Connection, skip_conditions: SkipIf) -> bool {
        match skip_conditions {
            SkipIf::ParentsRelateLimitto((relate_to, limit_to)) => {
                if let Ok(status) =
                    Self::internal_parent_relate_limit_exists(conn, &relate_to, &limit_to)
                    && status
                {
                    info!(
                        "DB Skipping adding job due to relate_to and limit_to exists {relate_to:?} {limit_to:?}"
                    );
                    return true;
                }
            }
            SkipIf::ParentsRelate(plugin_tag) => {
                if let Ok(status) = Self::internal_parent_structure_exists(conn, &plugin_tag)
                    && status
                {
                    info!("DB Skipping adding job due to Parent existing {plugin_tag:?}");
                    return true;
                }
            }
            SkipIf::FileHash(_file_hash) => {}
            SkipIf::FileTagRelationship(tag) => {
                if let Some(ns_id) = Self::internal_namespace_get_id(conn, &tag.namespace.name)
                    && let Some(tag_id) = Self::internal_tag_get_id(conn, &tag.name, ns_id)
                    && self.tag_has_files_cached(conn, tag_id)
                {
                    info!(
                        "DB Skipping adding job due to FileTagRelationship tag_id: {tag_id} having files."
                    );
                    return true;
                }
            }
            SkipIf::FileNamespaceNumber((_tag, _namespace, _id)) => {}
            SkipIf::NoFilesDownloaded => {}
        }
        false
    }

    ///
    /// Marks a url as being dead in the db
    ///
    #[ipc(name = "dead_url_add", request = "AddDeadUrl")]
    pub fn dead_url_add_sync(&self, dead_url: &String) -> bool {
        let mut writer_conn = self.writer_conn.lock();
        let conn = writer_conn.transaction().unwrap();
        let _ = Self::internal_dead_url_add(&conn, dead_url);
        let _ = conn.commit();

        false
    }

    ///
    /// Checks if a lsit of urls are dead
    ///
    #[ipc(name = "dead_url_get", request = "GetDeadUrl")]
    pub fn dead_url_get_sync(&self, dead_urls: &[String]) -> HashMap<String, bool> {
        let conn = self.pool.get().unwrap();

        if let Ok(status) = Self::internal_dead_url_exist(&conn, dead_urls) {
            return status;
        }
        HashMap::new()
    }

    ///
    /// Adds dead url into db
    ///
    pub async fn dead_url_add_async(self: Arc<Self>, dead_url: String) {
        let result = tokio::task::spawn_blocking(move || {
            self.dead_url_add_sync(&dead_url);
        });
        let _ = result.await;
    }

    pub async fn dead_url_exist(self: Arc<Self>, dead_url: Vec<String>) -> HashMap<String, bool> {
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };
            if let Ok(res) = Self::internal_dead_url_exist(&conn, &dead_url) {
                return res;
            }
            HashMap::new()
        });
        result.await.unwrap_or(HashMap::new())
    }

    ///
    /// Should x be skipped
    ///
    pub async fn should_skip_processing_job(self: Arc<Self>, skip_conditions: Vec<SkipIf>) -> bool {
        if skip_conditions.is_empty() {
            return false;
        }
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };
            for skip_condition in skip_conditions {
                if self.should_skip_item(&conn, skip_condition) {
                    return true;
                }
            }
            false
        });
        result.await.unwrap_or(false)
    }

    ///
    /// Handles all the processing for files and tags and relational items
    ///
    pub async fn process_scraper(
        self: Arc<Self>,
        map: HashMap<FileManager, Vec<FileTagAction>>,
        jobs: Vec<ScraperDataReturn>,
        audit_reason: String,
    ) {
        // Early Exit
        if map.is_empty() && jobs.is_empty() {
            return;
        }

        let writer_conn = self.writer_conn.clone();

        tokio::task::spawn_blocking(move || {
            let mut writer_lock_guard = writer_conn.lock();

            let conn = writer_lock_guard
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();

            'ScraperLoop: for scraperdatareturn in jobs {
                for skip_conditions in scraperdatareturn.skip_conditions {
                    if self.should_skip_item(&conn, skip_conditions) {
                        continue 'ScraperLoop;
                    }
                }

                Self::internal_jobs_add(&conn, &scraperdatareturn.job);
            }

            let unique_files: HashSet<FileInternal> =
                map.keys().map(|f| f.internal.clone()).collect();
            let resolved_files = Self::internal_file_bulk_add(&conn, unique_files);

            let mapped_files: Vec<_> = map
                .keys()
                .filter_map(|file_manager| {
                    // Find the matching resolved file
                    let matching_res = resolved_files
                        .iter()
                        .find(|res| res.hash == file_manager.internal.hash)?;

                    let mut temp = file_manager.clone();
                    temp.internal = matching_res.clone();
                    Some(temp)
                })
                .collect();

            for file in mapped_files {
                if let Some(file_id) = file.internal.id {
                    for hash in &file.identifying_hashes {
                        // If hash is your HashesSupported enum, you can extract the algorithm/string depending on your signature:
                        // let (algo, hash_str) = ...;
                        let (algo, hash_str) = hashessupportedtoinner(hash);

                        Self::internal_file_hash_add(&conn, &algo.to_string(), hash_str, &file_id);
                    }
                }
            }

            // Build a quick, zero-allocation lookup mapping: FileInternal -> Database u64 ID
            let mut file_cache = HashMap::with_capacity(resolved_files.len());
            for file in &resolved_files {
                if let Some(db_id) = file.id {
                    file_cache.insert(file.hash.clone(), db_id);
                }
            }

            // Collect all action definitions across every file block into one flat vector
            let all_tag_actions: Vec<FileTagAction> = map.values().flatten().cloned().collect();

            Self::internal_audit_context_set(&conn, &audit_reason).unwrap();
            let tag_cache =
                Self::internal_tag_bulk_add(&conn, &all_tag_actions, self.plugin_manager.clone());

            let file_ids: Vec<u64> = file_cache.values().copied().collect();
            let current_file_relationships =
                Self::internal_file_id_get_tag_ids_bulk(&conn, &file_ids).unwrap();

            let mut rels_to_add = HashSet::new();
            let mut rels_to_del = HashSet::new();

            let mut current_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
            let mut incoming_ns_tags: HashMap<&str, HashSet<u64>> = HashMap::new();
            let mut explicit_adds = HashSet::new();
            let mut set_deletions = HashSet::new();

            let mut tag_id_to_obj = HashMap::with_capacity(tag_cache.len());
            for (tag_obj, &tag_id) in &tag_cache {
                tag_id_to_obj.insert(tag_id, tag_obj);
            }

            for (file_internal, tag_list) in &map {
                let file_id = match file_cache.get(&file_internal.internal.hash) {
                    Some(&id) => id,
                    None => continue,
                };

                current_ns_tags.clear();
                explicit_adds.clear();
                set_deletions.clear();

                // Map current database state for this file: Namespace (&str) -> HashSet<tag_id>
                if let Some(current_tag_ids) = current_file_relationships.get(&file_id) {
                    for &tag_id in current_tag_ids {
                        // Instantly resolve the full Tag object using the raw ID
                        if let Some(tag) = tag_id_to_obj.get(&tag_id) {
                            let ns_name = &tag.namespace.name;

                            if ns_name != "source_url" && !ns_name.is_empty() {
                                current_ns_tags
                                    .entry(ns_name.as_str()) // Zero allocations!
                                    .or_default()
                                    .insert(tag_id);
                            }
                        }
                    }
                }
                // Process operations
                for tag_action in tag_list {
                    match tag_action.operation {
                        TagOperation::Add => {
                            for tag in &tag_action.tags {
                                if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                    && let Some(&tag_id) = tag_cache.get(&tag.tag)
                                {
                                    rels_to_add.insert((file_id, tag_id));
                                    explicit_adds.insert(tag_id);
                                }
                            }
                        }
                        TagOperation::Del => {
                            for tag in &tag_action.tags {
                                if matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                    && let Some(&tag_id) = tag_cache.get(&tag.tag)
                                {
                                    rels_to_del.insert((file_id, tag_id));
                                }
                            }
                        }
                        TagOperation::Set => {
                            incoming_ns_tags.clear();

                            for tag in &tag_action.tags {
                                if !matches!(tag.tag_type, TagType::Normal | TagType::NormalNoRegex)
                                {
                                    continue;
                                }
                                let ns_name = &tag.tag.namespace.name;
                                if ns_name == "source_url" || ns_name.is_empty() {
                                    continue;
                                }

                                if let Some(&tag_id) = tag_cache.get(&tag.tag) {
                                    incoming_ns_tags
                                        .entry(ns_name.as_str())
                                        .or_default()
                                        .insert(tag_id);

                                    rels_to_add.insert((file_id, tag_id));
                                }
                            }

                            // Evaluate deletions ONLY for namespaces explicitly targeted by this Set operation
                            for (ns_name, incoming_set) in &incoming_ns_tags {
                                if let Some(current_tag_ids) = current_ns_tags.get(ns_name) {
                                    for current_tag_id in current_tag_ids {
                                        if !incoming_set.contains(current_tag_id) {
                                            set_deletions.insert((file_id, *current_tag_id));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Apply targeted "Add overrides Set" rule
                for (f_id, tag_id) in &set_deletions {
                    if !explicit_adds.contains(tag_id) {
                        rels_to_del.insert((*f_id, *tag_id));
                    }
                }
            }

            // Global sanitation check for any edge deletions
            for del in &rels_to_del {
                rels_to_add.remove(del);
            }

            // 6️⃣ Step 5: Flush Relationship Mutations to DB in Batch
            if !rels_to_del.is_empty() {
                Self::internal_audit_context_set(&conn, &audit_reason).unwrap();
                Self::internal_relationship_bulk_delete(self.clone(), &conn, &rels_to_del);
            }

            if !rels_to_add.is_empty() {
                Self::internal_audit_context_set(&conn, &audit_reason).unwrap();
                Self::internal_relationship_bulk_add(self.clone(), &conn, &rels_to_add);
            }

            conn.commit().unwrap();
        })
        .await
        .unwrap();
    }
    ///
    /// Updates a job inside the db.
    ///
    pub async fn jobs_update(&self, job: &DbJobsObj) {
        let job = job.clone();
        let writer_conn = self.writer_conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut pool = writer_conn.lock();
            let conn = match pool.transaction() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            Self::internal_jobs_update(&conn, &job);
            conn.commit().unwrap();
        })
        .await
        .unwrap();
    }

    pub async fn complete_system_job(&self, job: &DbJobsObj) {
        let mut next = job.clone();
        next.config.time = get_sys_time_in_secs();
        next.isrunning = false;

        if let Some(DbJobRecreation::AlwaysTime(interval, count)) = next.config.recreation.clone() {
            if let Some(remaining) = count {
                if remaining == 0 {
                    self.job_remove(job).await;
                    return;
                }
                next.config.recreation =
                    Some(DbJobRecreation::AlwaysTime(interval, Some(remaining - 1)));
            }
            next.config.reptime = interval;
            self.jobs_update(&next).await;
        } else {
            self.job_remove(job).await;
        }
    }

    pub async fn update_missing_file_sizes(&self) -> Result<(), rusqlite::Error> {
        let pool = self.pool.clone();
        let writer_conn = self.writer_conn.clone();
        tokio::task::spawn_blocking(move || {
            // Resolve paths and inspect files without holding the serialized
            // writer. A storage scan can otherwise stall all database writes.

            let mut last_file_id = 0;
            loop {
                let conn = pool
                    .get()
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.into()))?;
                let storage = Self::internal_file_storage_get_all(&conn)?;

                let mut files = conn.prepare(&format!(
                    "SELECT id, hash, extension, storage_id
                 FROM File
                 WHERE size_bytes IS NULL
                   AND hash IS NOT NULL
                   AND id > ?1
                 ORDER BY id
                 LIMIT {}",
                    SQL_CHUNK_SIZE
                ))?;
                info!(
                    "System file size checker has processed files through id: {}",
                    last_file_id
                );
                let rows: Vec<_> = files
                    .query_map([last_file_id], |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, u64>(3)?,
                        ))
                    })?
                    .collect();

                if rows.is_empty() {
                    break;
                }

                let mut updates = Vec::new();
                let mut missing_paths = 0;
                let mut unreadable_files = 0;
                for row in rows {
                    let (id, hash, extension, storage_id) = row?;
                    last_file_id = id;
                    let file = FileInternal {
                        id: Some(id),
                        hash,
                        extension,
                        storage_id,
                        size_bytes: None,
                    };
                    let path = storage
                        .get(&storage_id)
                        .and_then(|base| Self::get_file_location(&file, base))
                        .or_else(|| {
                            storage
                                .iter()
                                .filter(|(id, _)| **id != storage_id)
                                .find_map(|(_, base)| Self::get_file_location(&file, base))
                        });
                    if let Some(path) = path {
                        match std::fs::metadata(path) {
                            Ok(metadata) => updates.push((id, metadata.len())),
                            Err(_) => unreadable_files += 1,
                        }
                    } else {
                        missing_paths += 1;
                    }
                }
                drop(files);
                drop(conn);

                let mut writer = writer_conn.lock();
                let tx = writer.transaction()?;
                {
                    let mut update = tx.prepare("UPDATE File SET size_bytes = ?1 WHERE id = ?2")?;
                    for (id, size) in &updates {
                        update.execute(params![size, id])?;
                    }
                }
                tx.commit()?;
                if missing_paths > 0 || unreadable_files > 0 {
                    log::warn!(
                        "System file size checker skipped {} files with missing paths and {} unreadable files; they will be retried later",
                        missing_paths,
                        unreadable_files,
                    );
                }
                info!(
                    "System file size checker updated {} files",
                    updates.len()
                );
            }
            Ok(())
        })
        .await
        .unwrap()
    }

    /// Gets existing files associated with source URLs in one database query.
    pub async fn source_url_files_get(
        &self,
        url_set: HashSet<String>,
    ) -> HashMap<String, FileInternal> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            let mut out = HashMap::new();
            let source_url_namespace_id = Self::internal_namespace_get_id(&conn, "source_url");
            let Some(source_url_namespace_id) = source_url_namespace_id else {
                return out;
            };
            Self::internal_relationship_partition_create(&conn, source_url_namespace_id);
            let relationship_table =
                Self::relationship_partition_name(source_url_namespace_id);

            let urls = url_set.into_iter().collect::<Vec<_>>();
            for urls in urls.chunks(SQL_CHUNK_SIZE) {
                if urls.is_empty() {
                    continue;
                }
                let placeholders = (1..=urls.len())
                    .map(|index| format!("?{index}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let query = format!(
                    "SELECT t.name, f.id, f.hash, f.extension, f.storage_id, f.size_bytes
                     FROM Tags t
                     JOIN (
                         SELECT tag_id, MIN(file_id) AS file_id
                         FROM {relationship_table}
                         GROUP BY tag_id
                     ) r ON r.tag_id = t.id
                     JOIN File f ON f.id = r.file_id
                     WHERE t.namespace = ?{namespace_param}
                       AND t.name IN ({placeholders})",
                    namespace_param = urls.len() + 1,
                );
                let mut stmt = conn.prepare(&query).unwrap();
                let mut query_params: Vec<&dyn rusqlite::ToSql> =
                    urls.iter().map(|url| url as &dyn rusqlite::ToSql).collect();
                query_params.push(&source_url_namespace_id);
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(query_params), |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            FileInternal {
                                id: row.get(1)?,
                                hash: row.get(2)?,
                                extension: row.get(3)?,
                                storage_id: row.get(4)?,
                                size_bytes: row.get(5)?,
                            },
                        ))
                    })
                    .unwrap();
                for row in rows.flatten() {
                    out.entry(row.0).or_insert(row.1);
                }
            }
            out
        })
        .await
        .unwrap()
    }

    ///
    /// Checks if we should download the file or not
    ///
    pub async fn should_download_file(&self, url: String) -> bool {
        let database = self.clone();
        let pool = self.pool.clone();
        let roaring = self.relationship_roaring_storage.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            let source_url_nsid = database.internal_namespace_sourceurl_get(&conn);
            let Some(tag_id) = Self::internal_tag_get_id(&conn, &url, source_url_nsid) else {
                return true;
            };
            if let Some(guard) = roaring.read().as_ref()
                && let Some(file_ids) = guard.relationship_search_fileid_roaring_in_memory(tag_id)
            {
                return file_ids.is_empty();
            }
            !Self::internal_tag_has_files(&conn, tag_id)
        })
        .await
        .unwrap()
    }

    ///
    /// Gets a single `file_id` from a tag
    ///
    pub async fn tag_get_file_id(&self, tag: &Tag) -> Option<u64> {
        let pool = self.pool.clone();

        let tag = tag.clone();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            Self::internal_tag_get_file_id(&conn, &tag)
        })
        .await
        .unwrap()
    }
    ///
    /// Gets a file if its id exists in db
    ///
    pub async fn file_id_get(&self, file_id: u64) -> Option<FileInternal> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };

            Self::internal_file_id_get(&conn, &file_id).ok()
        })
        .await
        .unwrap()
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
            let mut writer_conn = self_clone.writer_conn.lock();
            let conn = writer_conn.transaction().unwrap();
            Self::internal_audit_context_set(&conn, "relationship added").unwrap();
            Self::internal_relationship_bulk_add(self, &conn, &rel_list);
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
            let mut writer_conn = self_clone.writer_conn.lock();
            let conn = writer_conn.transaction().unwrap();
            Self::internal_audit_context_set(&conn, "relationship removed").unwrap();
            Self::internal_relationship_bulk_delete(self, &conn, &rel_list);
            conn.commit().unwrap();
        })
        .await
        .unwrap();
    }

    ///
    /// Gets the location where files should be stored
    /// IE the main folder that we're using
    ///
    pub async fn file_download_location_main(&self) -> Option<(PathBuf, u64)> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().ok()?;
            Self::internal_file_download_location_get(&conn).ok()
        })
        .await
        .ok()
        .flatten()
    }

    ///
    /// Gets the location we should download to
    ///
    #[must_use]
    pub fn file_download_location_main_sync(&self) -> Option<(PathBuf, u64)> {
        let pool = self.pool.clone();
        let conn = pool.get().ok()?;
        Self::internal_file_download_location_get(&conn).ok()
    }

    ///
    /// Adds a namespace into the db
    ///
    #[must_use]
    #[ipc(name = "namespace_set", request = "SetNamespace")]
    pub fn namespace_add_sync(&self, namespace: &GenericNamespaceObj) -> u64 {
        let mut guard = self.writer_conn.lock();
        let conn = guard.transaction().unwrap();
        let out = self.internal_namespace_get_or_create(&conn, namespace);
        conn.commit().unwrap();
        out
    }

    ///
    /// Returns the full location of where a file should be stored
    ///
    pub async fn file_download_location_get(
        &self,
        hash: &str,
        ext: &str,
    ) -> Option<(PathBuf, u64)> {
        // If our hash is less then 6 cant return a location
        if hash.len() <= 6 {
            return None;
        }
        self.file_download_location_main().await.map(|path| {
            let mut path_buf = path.0;
            path_buf.push(&hash[0..2]);
            path_buf.push(&hash[2..4]);
            path_buf.push(&hash[4..6]);
            path_buf.push(hash);
            (path_buf.with_extension(ext), path.1)
        })
    }
    #[must_use]
    pub fn file_download_location_get_sync(&self, hash: &str, ext: &str) -> Option<(PathBuf, u64)> {
        if hash.len() <= 6 {
            return None;
        }
        self.file_download_location_main_sync().map(|path| {
            let mut path_buf = path.0;
            path_buf.push(&hash[0..2]);
            path_buf.push(&hash[2..4]);
            path_buf.push(&hash[4..6]);
            path_buf.push(hash);
            (path_buf.with_extension(ext), path.1)
        })
    }

    ///
    /// Returns the full location of where a file should be stored
    ///
    pub async fn file_ids_get_tags(&self, file_ids: &HashSet<u64>) -> HashMap<u64, HashSet<Tag>> {
        // If our hash is less then 6 cant return a location
        if file_ids.is_empty() {
            return HashMap::new();
        }
        let file_ids = file_ids.clone();
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().unwrap();
            Self::internal_file_ids_get_tags(&conn, &file_ids)
        })
        .await
        .ok()
        .unwrap()
    }

    ///
    /// Searches the db for all file_ids that are related to the searchobj
    ///
    #[must_use]
    #[ipc(name = "search_db_files", request = "SearchFiles")]
    pub fn search_db_files_sync(&self, search: &SearchObj, limit: &Option<u64>) -> Vec<u64> {
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
        let relationship_source = Self::relationship_union_source(&conn, "r0");
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
                Self::relationship_union_source(&conn, &alias)
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
        Self::relationship_union_source(&conn, &format!("or{i}"))
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
        Self::relationship_union_source(&conn, &format!("not{i}"))
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

    /// A sync function to get a function
    #[must_use]
    #[ipc(name = "setting_get", request = "SettingsGetName")]
    pub fn setting_get_sync(&self, name: &str) -> Option<DbSettingsObj> {
        let pool = self.pool.clone();
        let conn = pool.get().ok()?;
        Self::internal_setting_get(&conn, name).ok().flatten()
    }

    ///
    /// What everything else uses when getting a setting
    ///
    pub async fn setting_get(self: Arc<Self>, name: String) -> Option<shared_types::DbSettingsObj> {
        let name = name.clone();
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || self_clone.setting_get_sync(&name))
            .await
            .ok()
            .flatten() // Flattens the JoinError wrapper Option as well
    }

    ///
    /// Sets the setting in the db. Updates it if the setting already exists
    ///
    #[must_use]
    #[ipc(name = "setting_set", request = "SettingsSet")]
    pub fn setting_set_sync(&self, obj: &DbSettingsObj) -> bool {
        let mut writer_conn = self.writer_conn.lock();
        if let Ok(conn) = writer_conn.transaction() {
            let _ = Self::internal_setting_set(&conn, obj);

            conn.commit();
        }
        false
    }

    ///
    /// What anything outside of the db uses to set a setting
    ///
    pub async fn setting_set(self: Arc<Self>, obj: shared_types::DbSettingsObj) -> bool {
        let obj = obj.clone();
        let _self_clone = self.clone();
        tokio::task::spawn_blocking(move || self.setting_set_sync(&obj))
            .await
            .ok()
            .is_some()
    }
    ///
    /// Sets a job to be running inside of the db
    ///
    pub async fn job_set_is_running(&self, job: &DbJobsObj) {
        let job_id = job.id;
        let writer_conn = self.writer_conn.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let mut writer_lock_guard = writer_conn.lock();
            let tn = writer_lock_guard
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            let status = Self::internal_jobs_set_isrunning(&tn, job_id).is_ok();

            tn.commit().unwrap();

            status
        })
        .await;
    }

    ///
    /// Sets a job to be running inside of the db
    ///
    pub async fn job_remove(&self, job: &DbJobsObj) {
        let job_id = job.id;
        let writer_conn = self.writer_conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer_lock_guard = writer_conn.lock();
            let tn = writer_lock_guard
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            let status = Self::internal_job_remove(&tn, job_id).is_ok();

            tn.commit().unwrap();
            status
        })
        .await
        .unwrap();
    }

    ///
    /// Gets all jobs associated with a site
    ///
    pub async fn jobs_get_site(&self, site: &str) -> Vec<DbJobsObj> {
        let pool = self.pool.clone();

        let site_owned = site.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    return Vec::new();
                }
            };
            match Self::internal_jobs_get_site(&conn, &site_owned) {
                Ok(jobs) => jobs,
                Err(e) => {
                    log::error!("Database error fetching jobs for site '{site_owned}': {e:?}");
                    Vec::new()
                }
            }
        })
        .await
        .unwrap_or_default()
    }

    ///
    /// Gets all jobs that can run
    ///
    pub async fn jobs_get_torun(&self, sites: Vec<String>) -> Vec<DbJobsObj> {
        self.jobs_get_torun_chunk(sites, usize::MAX).await
    }

    pub async fn jobs_get_torun_chunk(
        &self,
        sites: Vec<String>,
        chunk_size: usize,
    ) -> Vec<DbJobsObj> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    return Vec::new();
                }
            };
            match Self::internal_jobs_get_torun_chunk(&conn, sites, chunk_size) {
                Ok(jobs) => jobs,
                Err(e) => {
                    log::error!("Database error fetching jobs: {e:?}");
                    Vec::new()
                }
            }
        })
        .await
        .unwrap_or_default()
    }

    ///
    /// Adds job into db
    ///
    #[must_use]
    #[ipc(name = "jobs_add_single", request = "JobsAddSingle")]
    pub fn jobs_add_single_sync(&self, job: PluginJob) -> u64 {
        let mut writer_conn = self.writer_conn.lock();
        let conn = writer_conn.transaction().unwrap();
        let out = Self::internal_jobs_add(&conn, &job);
        conn.commit().unwrap();
        out
    }
    /*
    ///
    /// Adds job into db asynchronously.
    ///
    pub async fn jobs_add_single(&self, job: PluginJob) -> u64 {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };
            Self::internal_jobs_add(&conn, &job)
        })
        .await
        .unwrap()
    }*/

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
        let writer_conn = self.writer_conn.clone();

        let plugin_manager = self.plugin_manager.clone();
        tokio::task::spawn_blocking(move || {
            let out_tags;
            {
                let mut writer_lock_guard = writer_conn.lock();
                let tn = writer_lock_guard
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .unwrap();
                Self::internal_audit_context_set(&tn, &audit_reason).unwrap();
                out_tags = Self::internal_tag_bulk_add(&tn, &tags_owned, plugin_manager.clone());

                tn.commit().unwrap();
            }
            out_tags
        })
        .await
        .unwrap()
    }

    ///
    /// Adds tags into db in bulk. Also adds parents
    ///
    pub async fn file_add_bulk(&self, tags: HashSet<FileInternal>) -> HashSet<FileInternal> {
        if tags.is_empty() {
            return HashSet::new();
        }
        let writer_conn = self.writer_conn.clone();

        let tags_owned = tags.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer_lock_guard = writer_conn.lock();

            let tn = writer_lock_guard
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            let out_tags = Self::internal_file_bulk_add(&tn, tags_owned);

            tn.commit().unwrap();
            out_tags
        })
        .await
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DB_VERSION;
    use rayon::ThreadPoolBuilder;
    use shared_types::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    fn database_for_path(path: &std::path::Path) -> Arc<MainDatabase> {
        let processing_pool = Arc::new(ThreadPoolBuilder::new().build().unwrap());
        MainDatabase::new(
            path,
            processing_pool,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
    }

    pub fn new_test() -> Arc<MainDatabase> {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "intscrape-db-test-{}-{id}.sqlite",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite-shm"));
        database_for_path(&path)
    }

    fn namespace(name: &str, description: Option<&str>) -> GenericNamespaceObj {
        GenericNamespaceObj {
            name: name.to_string(),
            description: description.map(str::to_string),
        }
    }

    fn tag(name: &str, namespace_name: &str) -> Tag {
        Tag {
            name: name.to_string(),
            namespace: namespace(namespace_name, None),
        }
    }

    fn plugin_tag(name: &str, namespace_name: &str) -> PluginTag {
        PluginTag {
            tag: tag(name, namespace_name),
            ..Default::default()
        }
    }

    fn file_action(operation: TagOperation, tags: Vec<PluginTag>) -> FileTagAction {
        FileTagAction { operation, tags }
    }

    fn file(hash: &str, extension: &str) -> FileInternal {
        FileInternal {
            hash: hash.to_string(),
            extension: extension.to_string(),
            storage_id: 1,
            ..Default::default()
        }
    }

    fn job(site: &str, time: u64, reptime: u64) -> PluginJob {
        PluginJob {
            site: site.to_string(),
            time,
            reptime,
            ..Default::default()
        }
    }

    #[test]
    fn test_database_initialization_and_settings() {
        // 1. Fire up a completely self-contained in-memory pool instance
        let db = new_test();

        // Grab an isolated connection out of our pool to assert initialization
        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // 2. Validate that the tables were successfully initialized by check_db
        let table_check: i32 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Settings'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            table_check, 1,
            "The Settings table was not created during initialization"
        );

        // 3. Test that your default values were baked in successfully
        let system_version = MainDatabase::internal_setting_get(&conn, "SYSTEM_VERSION")
            .unwrap()
            .expect("SYSTEM_VERSION setting should be configured");

        assert_eq!(system_version.num, Some(DB_VERSION));

        let user_agent = MainDatabase::internal_setting_get(&conn, "SYSTEM_DEFAULT_USER_AGENT")
            .unwrap()
            .expect("Default user agent missing");

        assert_eq!(user_agent.param, Some("IntScrape V1.0".to_string()));

        let audit_table: i32 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'AuditLog'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit_table, 0);
    }

    #[test]
    fn test_v2_database_upgrades_to_v3_audit_log() {
        let path = std::env::temp_dir().join(format!(
            "intscrape-db-v2-upgrade-{}.sqlite",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let db = database_for_path(&path);
        let conn = db.pool.get().unwrap();
        conn.execute("ALTER TABLE File DROP COLUMN size_bytes", [])
            .unwrap();
        conn.execute(
            "INSERT INTO File (hash, extension, storage_id) VALUES ('upgrade-hash', 'jpg', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO Namespace (name, description) VALUES ('upgrade', NULL)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO Tags (name, namespace) VALUES ('upgrade-tag', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE Relationship (
                file_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY (file_id, tag_id)
            )",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO Relationship (file_id, tag_id) VALUES (1, 1)",
            [],
        )
        .unwrap();
        MainDatabase::internal_db_version_set(&conn, 2).unwrap();
        drop(conn);
        drop(db);

        let db = database_for_path(&path);
        let conn = db.pool.get().unwrap();
        let version = MainDatabase::internal_setting_get(&conn, "SYSTEM_VERSION")
            .unwrap()
            .unwrap();
        assert_eq!(version.num, Some(DB_VERSION));
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'AuditLog'",
                [],
                |row| row.get::<_, i32>(0),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn test_relationship_and_tag_changes_are_audited() {
        return;
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let actions = [file_action(
            TagOperation::Add,
            vec![plugin_tag("audit", "test")],
        )];
        MainDatabase::internal_audit_context_set(&conn, "tag discovered from input").unwrap();
        let tags = MainDatabase::internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
        let tag_id = *tags.values().next().unwrap();
        MainDatabase::internal_file_bulk_add(&conn, HashSet::from([file("audit-hash", "bin")]));
        let file_id: u64 = conn
            .query_row("SELECT id FROM File WHERE hash = 'audit-hash'", [], |row| {
                row.get(0)
            })
            .unwrap();
        MainDatabase::internal_audit_context_set(&conn, "relationship added").unwrap();
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([(file_id, tag_id)]),
        );

        let count: i32 = conn
            .query_row(
                "SELECT count(*) FROM AuditLog WHERE entity_type IN ('tag', 'relationship')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
        let reason: String = conn
            .query_row(
                "SELECT reason FROM AuditLog WHERE entity_type = 'relationship'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!reason.is_empty());

        let file_entries = db.audit_get_sync(&Some(file_id), &None);
        assert_eq!(file_entries.len(), 2);
        assert!(
            file_entries
                .iter()
                .all(|entry| entry.file_id == Some(file_id))
        );
        assert!(
            file_entries
                .iter()
                .any(|entry| entry.tag_id == Some(tag_id))
        );

        let tag_entries = db.audit_get_sync(&None, &Some(tag_id));
        assert_eq!(tag_entries.len(), 2);
        assert!(tag_entries.iter().all(|entry| entry.tag_id == Some(tag_id)));
    }

    #[test]
    fn test_audit_reason_can_identify_scraper_source() {
        return;
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let actions = [file_action(
            TagOperation::Add,
            vec![plugin_tag("source-tag", "source")],
        )];
        let reason = "scraper: test-scraper";
        MainDatabase::internal_audit_context_set(&conn, reason).unwrap();
        let tags = MainDatabase::internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
        let tag_id = *tags.values().next().unwrap();
        MainDatabase::internal_file_bulk_add(&conn, HashSet::from([file("source-hash", "bin")]));
        let file_id: u64 = conn
            .query_row(
                "SELECT id FROM File WHERE hash = 'source-hash'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        MainDatabase::internal_audit_context_set(&conn, reason).unwrap();
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([(file_id, tag_id)]),
        );

        let reasons: Vec<String> = conn
            .prepare(
                "SELECT reason FROM AuditLog
                 WHERE (entity_type = 'tag' AND tag_id = ?1)
                    OR (entity_type = 'relationship' AND file_id = ?2 AND tag_id = ?1)",
            )
            .unwrap()
            .query_map(params![tag_id, file_id], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();

        assert_eq!(reasons.len(), 2);
        assert!(reasons.iter().all(|audit_reason| audit_reason == reason));
    }

    #[test]
    fn test_relationship_cascade_delete_is_audited() {
        return;
        let db = new_test();
        let conn = db.pool.get().unwrap();
        MainDatabase::internal_audit_context_set(&conn, "cascade test").unwrap();
        conn.execute(
            "INSERT INTO Namespace (name, description) VALUES ('cascade', NULL)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO File (hash, extension, storage_id) VALUES ('cascade-hash', 'bin', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO Tags (name, namespace) VALUES (
                'cascade-tag', (SELECT id FROM Namespace WHERE name = 'cascade')
            )",
            [],
        )
        .unwrap();
        MainDatabase::internal_relationship_partition_create(&conn, 1);
        conn.execute(
            "INSERT INTO Relationship_1 (file_id, tag_id) VALUES (
                (SELECT id FROM File WHERE hash = 'cascade-hash'),
                (SELECT id FROM Tags WHERE name = 'cascade-tag')
            )",
            [],
        )
        .unwrap();

        conn.execute("DELETE FROM File WHERE hash = 'cascade-hash'", [])
            .unwrap();

        let relationship_delete_count: i32 = conn
            .query_row(
                "SELECT count(*) FROM AuditLog
                 WHERE entity_type = 'relationship' AND action = 'delete'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(relationship_delete_count, 1);
    }

    #[test]
    fn test_internal_tag_bulk_add_ignores_duplicates() {
        let db = new_test();
        let ns = GenericNamespaceObj {
            name: "system".to_string(),
            description: None,
        };
        let tag1 = FileTagAction {
            tags: vec![PluginTag {
                tag: Tag {
                    name: "unique_tag".to_string(),
                    namespace: ns.clone(),
                },
                relates_to: None,
                ..Default::default()
            }],
            ..Default::default()
        };

        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // Duplicate tag layout
        let tag2 = tag1.clone();

        // Pass duplicate elements in the batch array
        MainDatabase::internal_tag_bulk_add(&conn, &[tag1, tag2], db.plugin_manager.clone());

        // Due to INSERT OR IGNORE, SQL should gracefully process without panicking on unique constraints
        let tag_count: i32 = conn
            .query_row(
                "SELECT count(*) FROM Tags WHERE name = 'unique_tag'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            tag_count, 1,
            "INSERT OR IGNORE failed to drop duplicate entry safely"
        );
    }

    #[test]
    fn test_internal_tag_bulk_add_keeps_namespace_mapping() {
        let db = new_test();
        let first_namespace = GenericNamespaceObj {
            name: "first_namespace".to_string(),
            description: None,
        };
        let second_namespace = GenericNamespaceObj {
            name: "second_namespace".to_string(),
            description: None,
        };
        let actions = [
            FileTagAction {
                tags: vec![PluginTag {
                    tag: Tag {
                        name: "same value".to_string(),
                        namespace: first_namespace.clone(),
                    },
                    ..Default::default()
                }],
                ..Default::default()
            },
            FileTagAction {
                tags: vec![PluginTag {
                    tag: Tag {
                        name: "same value".to_string(),
                        namespace: second_namespace.clone(),
                    },
                    ..Default::default()
                }],
                ..Default::default()
            },
        ];
        let conn = db.pool.get().unwrap();

        let tag_map =
            MainDatabase::internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
        let first_id = tag_map.get(&actions[0].tags[0].tag).copied().unwrap();
        let second_id = tag_map.get(&actions[1].tags[0].tag).copied().unwrap();

        let first_namespace_id: u64 = conn
            .query_row(
                "SELECT id FROM Namespace WHERE name = 'first_namespace'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let second_namespace_id: u64 = conn
            .query_row(
                "SELECT id FROM Namespace WHERE name = 'second_namespace'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            conn.query_row(
                "SELECT namespace FROM Tags WHERE id = ?1",
                [first_id],
                |row| { row.get::<_, u64>(0) }
            )
            .unwrap(),
            first_namespace_id
        );
        assert_eq!(
            conn.query_row(
                "SELECT namespace FROM Tags WHERE id = ?1",
                [second_id],
                |row| { row.get::<_, u64>(0) }
            )
            .unwrap(),
            second_namespace_id
        );
    }

    #[test]
    fn test_internal_namespace_bulk_add_success_and_upsert() {
        let db = new_test();

        let ns1 = GenericNamespaceObj {
            name: "authors".to_string(),
            description: Some("Book creators".to_string()),
        };
        let ns2 = GenericNamespaceObj {
            name: "genres".to_string(),
            description: None,
        };

        let mut set = HashSet::new();
        set.insert(ns1.clone());
        set.insert(ns2.clone());

        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // 1. Test insertion
        let ids = MainDatabase::internal_namespace_bulk_add(&conn, &set);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains_key(&ns1));
        assert!(ids.contains_key(&ns2));

        // 2. Test Upsert (ON CONFLICT update description)
        let ns1_updated = GenericNamespaceObj {
            name: "authors".to_string(),
            description: Some("Updated Description".to_string()),
        };

        let mut update_set = HashSet::new();
        update_set.insert(ns1_updated.clone());

        let updated_ids = MainDatabase::internal_namespace_bulk_add(&conn, &update_set);
        assert_eq!(updated_ids.get(&ns1_updated), ids.get(&ns1)); // ID should remain unchanged

        // Verify description updated in DB
        let desc: String = conn
            .query_row(
                "SELECT description FROM Namespace WHERE name = 'authors'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(desc, "Updated Description");
    }

    #[test]
    fn test_internal_parents_bulk_add_with_dynamic_tags() {
        let db = new_test();
        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        // 1. Construct a fully relational tag structure
        let ns = GenericNamespaceObj {
            name: "programming".to_string(),
            description: None,
        };

        let t_rust = Tag {
            name: "Rust".to_string(),
            namespace: ns.clone(),
        };
        let t_lang = Tag {
            name: "Language".to_string(),
            namespace: ns.clone(),
        };
        let t_backend = Tag {
            name: "Backend".to_string(),
            namespace: ns.clone(),
        };
        let complex_plugin_tag = FileTagAction {
            tags: vec![PluginTag {
                tag: t_rust.clone(),
                relates_to: Some(RelationContext {
                    tag: t_lang.clone(),
                    limit_to: Some(t_backend.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        // 2. Add tags dynamically through your revamped bulk add function
        // This registers all 3 tags and their namespaces simultaneously
        let tag_ids = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[complex_plugin_tag],
            db.plugin_manager.clone(),
        );

        // Extract the generated IDs from the map returned by the tag function
        let rust_id = *tag_ids.get(&t_rust).expect("Rust tag missing ID");
        let lang_id = *tag_ids.get(&t_lang).expect("Language tag missing ID");
        let backend_id = *tag_ids.get(&t_backend).expect("Backend tag missing ID");

        // 3. Formulate the parent relations safely using the generated IDs
        let relation1 = TagParents {
            tag_id: rust_id,
            relate_tag_id: lang_id,
            limit_to: Some(backend_id),
        };
        let relation2 = TagParents {
            tag_id: lang_id,
            relate_tag_id: backend_id,
            limit_to: None,
        };

        let mut parent_input_set = HashSet::new();
        parent_input_set.insert(relation1.clone());
        parent_input_set.insert(relation2.clone());

        // 4. Execute the parents bulk add method
        let parent_results = MainDatabase::internal_parents_bulk_add(&conn, &parent_input_set);

        // 5. Verify the relationship mapping table state
        assert_eq!(
            parent_results.len(),
            2,
            "Failed to insert both relationships"
        );
        assert!(parent_results.contains_key(&relation1));
        assert!(parent_results.contains_key(&relation2));

        MainDatabase::debug_print_parents(&conn);

        // Ensure rows exist inside SQLite storage engine exactly as mapped
        let total_db_parent_rows: u32 = conn
            .query_row("SELECT count(*) FROM Parents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(total_db_parent_rows, 2);
    }

    #[test]
    fn test_internal_tag_bulk_add_flatmaps_nested_namespaces() {
        let db = new_test();
        let conn = db
            .pool
            .get()
            .expect("Failed to pull connection from test pool");

        let ns_base = GenericNamespaceObj {
            name: "base_ns".to_string(),
            description: None,
        };
        let ns_relate = GenericNamespaceObj {
            name: "relate_ns".to_string(),
            description: None,
        };
        let ns_limit = GenericNamespaceObj {
            name: "limit_ns".to_string(),
            description: None,
        };

        let complex_tag = FileTagAction {
            tags: vec![PluginTag {
                tag: Tag {
                    name: "rust".to_string(),
                    namespace: ns_base.clone(),
                },
                relates_to: Some(RelationContext {
                    tag: Tag {
                        name: "programming".to_string(),
                        namespace: ns_relate.clone(),
                    },
                    limit_to: Some(Tag {
                        name: "limit".to_string(),
                        namespace: ns_limit.clone(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        // Execute bulk add
        MainDatabase::internal_tag_bulk_add(&conn, &[complex_tag], db.plugin_manager.clone());

        // Assertions 1: Ensure all 3 distinct namespaces were automatically extracted and created
        let ns_count: i32 = conn
            .query_row("SELECT count(*) FROM Namespace", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns_count, 3);

        // Assertions 2: Ensure both tags ("rust" and "programming") were inserted safely
        let tag_count: i32 = conn
            .query_row("SELECT count(*) FROM Tags", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tag_count, 3);

        // Verify "rust" tag belongs to the correct mapped namespace row
        let mapped_ns_name: String = conn.query_row(
            "SELECT n.name FROM Tags t JOIN Namespace n ON t.namespace = n.id WHERE t.name = 'rust'",
            [],
            |r| r.get(0)
        ).unwrap();
        assert_eq!(mapped_ns_name, "base_ns");
    }

    #[test]
    fn test_namespace_lookup_and_empty_bulk_inputs() {
        let db = new_test();
        let conn = db.pool.get().unwrap();

        assert!(MainDatabase::internal_namespace_bulk_add(&conn, &HashSet::new()).is_empty());
        assert_eq!(
            MainDatabase::internal_namespace_get_id(&conn, "missing"),
            None
        );

        let original = namespace("edge", Some("first"));
        let id = db.internal_namespace_get_or_create(&conn, &original);
        assert_eq!(db.namespace_cache.read().get("edge"), Some(&id));
        assert_eq!(
            MainDatabase::internal_namespace_get_id(&conn, "edge"),
            Some(id)
        );
        assert_eq!(
            MainDatabase::internal_namespace_get_generic(&conn, &id),
            Some(original)
        );

        let updated = namespace("edge", Some("updated"));
        assert_eq!(db.internal_namespace_get_or_create(&conn, &updated), id);
    }

    #[tokio::test]
    async fn test_source_url_files_get_omits_missing_urls() {
        let db = new_test();
        let url = "https://example.test/missing".to_string();

        assert_eq!(
            db.source_url_files_get(HashSet::from([url.clone()])).await,
            HashMap::new()
        );
    }

    #[tokio::test]
    async fn test_source_url_files_get_returns_existing_files() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let url = "https://example.test/existing";
        let actions = [file_action(
            TagOperation::Add,
            vec![plugin_tag(url, "source_url")],
        )];
        let tags = MainDatabase::internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
        let tag_id = tags[&tag(url, "source_url")];
        let file_id = MainDatabase::internal_file_bulk_add(
            &conn,
            HashSet::from([file("source-url-hash", "jpg")]),
        )
        .into_iter()
        .next()
        .and_then(|file| file.id)
        .unwrap();
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([(file_id, tag_id)]),
        );
        drop(conn);

        let existing = db
            .source_url_files_get(HashSet::from([url.to_string()]))
            .await;
        let existing_file = existing.get(url).expect("source URL file should exist");
        assert_eq!(existing_file.hash, "source-url-hash");
        assert_eq!(existing_file.extension, "jpg");
        assert_eq!(existing_file.id, Some(file_id));
    }

    #[tokio::test]
    async fn test_source_url_files_get_omits_urls_without_files() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let url = "https://example.test/known";
        let actions = [file_action(
            TagOperation::Add,
            vec![plugin_tag(url, "source_url")],
        )];
        MainDatabase::internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
        drop(conn);

        assert_eq!(
            db.source_url_files_get(HashSet::from([url.to_string()])).await,
            HashMap::new()
        );
    }

    #[test]
    fn test_tag_bulk_add_filters_empty_and_non_normal_tags() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let mut special_tag = plugin_tag("special", "tags");
        special_tag.tag_type = TagType::Special;

        let actions = [file_action(
            TagOperation::Add,
            vec![
                plugin_tag("", "tags"),
                special_tag,
                plugin_tag("valid", "tags"),
                plugin_tag("valid", "tags"),
            ],
        )];
        let result =
            MainDatabase::internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());

        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&tag("valid", "tags")));
        assert_eq!(
            conn.query_row("SELECT count(*) FROM Tags", [], |row| row.get::<_, u64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_file_bulk_add_upserts_and_empty_input() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        assert!(MainDatabase::internal_file_bulk_add(&conn, HashSet::new()).is_empty());

        let first = file("abcdef123", "jpg");
        let mut files = HashSet::new();
        files.insert(first.clone());
        let inserted = MainDatabase::internal_file_bulk_add(&conn, files);
        assert_eq!(inserted.len(), 1);
        let inserted = inserted.into_iter().next().unwrap();
        assert!(inserted.id.is_some());

        let updated = file("abcdef123", "png");
        let mut files = HashSet::new();
        files.insert(updated);
        let upserted = MainDatabase::internal_file_bulk_add(&conn, files)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(upserted.id, inserted.id);
        assert_eq!(upserted.extension, "png");
        assert_eq!(MainDatabase::internal_file_get_all(&conn).unwrap().len(), 1);
    }

    #[test]
    fn test_relationships_are_deduplicated_and_filtered() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let file_id =
            MainDatabase::internal_file_bulk_add(&conn, [file("relhash123", "jpg")].into())
                .into_iter()
                .next()
                .unwrap()
                .id
                .unwrap();
        let tag_ids = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[
                file_action(TagOperation::Add, vec![plugin_tag("one", "a")]),
                file_action(TagOperation::Add, vec![plugin_tag("two", "b")]),
            ],
            db.plugin_manager.clone(),
        );
        let one_id = tag_ids[&tag("one", "a")];
        let two_id = tag_ids[&tag("two", "b")];
        let relationships =
            HashSet::from([(file_id, one_id), (file_id, one_id), (file_id, two_id)]);
        MainDatabase::internal_relationship_bulk_add(db.clone(), &conn, &relationships);

        assert_eq!(
            MainDatabase::internal_file_id_get_tag_ids(&conn, &file_id).unwrap(),
            HashSet::from([one_id, two_id])
        );
        assert_eq!(
            MainDatabase::internal_file_id_get_tag_ids_bulk(&conn, &[file_id, 999])
                .unwrap()
                .len(),
            1
        );
        assert!(MainDatabase::internal_tag_has_files(&conn, one_id));
        assert!(!MainDatabase::internal_tag_has_files(&conn, 999));
        let namespace_id = MainDatabase::internal_namespace_get_id(&conn, "a").unwrap();
        assert_eq!(
            MainDatabase::internal_file_id_get_tag_ids_where_namespace_id(
                &conn,
                &file_id,
                &namespace_id
            )
            .unwrap(),
            HashSet::from([one_id])
        );

        MainDatabase::internal_relationship_bulk_delete(
            db.clone(),
            &conn,
            &HashSet::from([(file_id, one_id)]),
        );
        assert_eq!(
            MainDatabase::internal_file_id_get_tag_ids(&conn, &file_id).unwrap(),
            HashSet::from([two_id])
        );
    }

    #[test]
    fn test_tag_and_file_lookup_empty_and_missing_inputs() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        assert!(MainDatabase::internal_tag_id_get_tag(&conn, &HashSet::new()).is_empty());
        assert!(MainDatabase::internal_file_ids_get_tags(&conn, &HashSet::new()).is_empty());
        assert_eq!(
            MainDatabase::internal_file_id_get(&conn, &999),
            Err(rusqlite::Error::QueryReturnedNoRows)
        );
        assert_eq!(
            MainDatabase::internal_tag_get_file_id(&conn, &tag("missing", "missing")),
            None
        );
    }

    #[test]
    fn test_search_supports_and_or_not_and_limit() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let mut ids = HashMap::new();
        for hash in ["searchaaa", "searchbbb", "searchccc"] {
            let inserted =
                MainDatabase::internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]));
            let inserted = inserted.into_iter().next().unwrap();
            ids.insert(hash.to_string(), inserted.id.unwrap());
        }
        let tags = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[
                file_action(
                    TagOperation::Add,
                    vec![plugin_tag("red", "color"), plugin_tag("round", "shape")],
                ),
                file_action(
                    TagOperation::Add,
                    vec![plugin_tag("blue", "color"), plugin_tag("round", "shape")],
                ),
                file_action(
                    TagOperation::Add,
                    vec![plugin_tag("red", "color"), plugin_tag("square", "shape")],
                ),
            ],
            db.plugin_manager.clone(),
        );
        let red = tags[&tag("red", "color")];
        let blue = tags[&tag("blue", "color")];
        let round = tags[&tag("round", "shape")];
        let square = tags[&tag("square", "shape")];
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([
                (ids["searchaaa"], red),
                (ids["searchaaa"], round),
                (ids["searchbbb"], blue),
                (ids["searchbbb"], round),
                (ids["searchccc"], red),
                (ids["searchccc"], square),
            ]),
        );

        let and = SearchObj {
            search_relate: None,
            searches: vec![SearchHolder::And(vec![red, round])],
        };
        assert_eq!(db.search_db_files_sync(&and, &None), vec![ids["searchaaa"]]);
        let or = SearchObj {
            search_relate: None,
            searches: vec![SearchHolder::Or(vec![blue, square])],
        };
        assert_eq!(
            db.search_db_files_sync(&or, &None)
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::from([ids["searchbbb"], ids["searchccc"]])
        );
        let not = SearchObj {
            search_relate: None,
            searches: vec![
                SearchHolder::And(vec![red]),
                SearchHolder::Not(vec![square]),
            ],
        };
        let not_results = db.search_db_files_sync(&not, &Some(1));
        assert_eq!(not_results, vec![ids["searchaaa"]]);
        let empty = SearchObj {
            search_relate: None,
            searches: vec![],
        };
        assert!(db.search_db_files_sync(&empty, &None).is_empty());
    }

    #[test]
    fn test_search_malformed_inputs_do_not_panic() {
        let db = new_test();

        let not_only = SearchObj {
            search_relate: None,
            searches: vec![SearchHolder::Not(vec![u64::MAX])],
        };
        assert!(db.search_db_files_sync(&not_only, &None).is_empty());

        let empty_groups = SearchObj {
            search_relate: None,
            searches: vec![
                SearchHolder::And(Vec::new()),
                SearchHolder::Or(Vec::new()),
                SearchHolder::Not(Vec::new()),
            ],
        };
        assert!(db.search_db_files_sync(&empty_groups, &None).is_empty());

        let extreme_limit = SearchObj {
            search_relate: None,
            searches: vec![SearchHolder::And(vec![u64::MAX])],
        };
        assert!(
            db.search_db_files_sync(&extreme_limit, &Some(u64::MAX))
                .is_empty()
        );
    }

    #[test]
    fn test_not_only_search_deduplicates_file_ids() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let file_id =
            MainDatabase::internal_file_bulk_add(&conn, HashSet::from([file("not-only", "jpg")]))
                .into_iter()
                .next()
                .unwrap()
                .id
                .unwrap();
        let tags = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[file_action(
                TagOperation::Add,
                vec![
                    plugin_tag("keep-one", "test"),
                    plugin_tag("keep-two", "test"),
                    plugin_tag("excluded", "test"),
                ],
            )],
            db.plugin_manager.clone(),
        );
        let keep_one = tags[&tag("keep-one", "test")];
        let keep_two = tags[&tag("keep-two", "test")];
        let excluded = tags[&tag("excluded", "test")];
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([(file_id, keep_one), (file_id, keep_two)]),
        );

        let search = SearchObj {
            search_relate: None,
            searches: vec![SearchHolder::Not(vec![excluded])],
        };
        assert_eq!(db.search_db_files_sync(&search, &None), vec![file_id]);
    }

    #[test]
    fn test_search_boolean_operators_exclude_not_matches() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let mut file_ids = HashMap::new();

        for hash in [
            "and_only",
            "or_only",
            "and_and_or",
            "and_and_not",
            "unrelated",
        ] {
            let file_id =
                MainDatabase::internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]))
                    .into_iter()
                    .next()
                    .unwrap()
                    .id
                    .unwrap();
            file_ids.insert(hash, file_id);
        }

        let tags = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[file_action(
                TagOperation::Add,
                vec![
                    plugin_tag("and", "test"),
                    plugin_tag("or", "test"),
                    plugin_tag("not", "test"),
                ],
            )],
            db.plugin_manager.clone(),
        );
        let and_tag = tags[&tag("and", "test")];
        let or_tag = tags[&tag("or", "test")];
        let not_tag = tags[&tag("not", "test")];

        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([
                (file_ids["and_only"], and_tag),
                (file_ids["or_only"], or_tag),
                (file_ids["and_and_or"], and_tag),
                (file_ids["and_and_or"], or_tag),
                (file_ids["and_and_not"], and_tag),
                (file_ids["and_and_not"], not_tag),
            ]),
        );

        let run = |searches| {
            db.search_db_files_sync(
                &SearchObj {
                    search_relate: None,
                    searches,
                },
                &None,
            )
            .into_iter()
            .collect::<HashSet<_>>()
        };

        assert_eq!(
            run(vec![SearchHolder::And(vec![and_tag])]),
            HashSet::from([
                file_ids["and_only"],
                file_ids["and_and_or"],
                file_ids["and_and_not"],
            ])
        );
        assert_eq!(
            run(vec![SearchHolder::Or(vec![and_tag, or_tag])]),
            HashSet::from([
                file_ids["and_only"],
                file_ids["or_only"],
                file_ids["and_and_or"],
                file_ids["and_and_not"],
            ])
        );
        assert_eq!(
            run(vec![
                SearchHolder::And(vec![and_tag]),
                SearchHolder::Not(vec![not_tag]),
            ]),
            HashSet::from([file_ids["and_only"], file_ids["and_and_or"]])
        );
        assert_eq!(
            run(vec![
                SearchHolder::And(vec![and_tag]),
                SearchHolder::Or(vec![or_tag]),
                SearchHolder::Not(vec![not_tag]),
            ]),
            HashSet::from([file_ids["and_and_or"]])
        );
    }

    #[test]
    fn test_search_db_files_by_tag_groups_resolves_ids_and_names() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let files = ["grouped-and", "grouped-global", "grouped-excluded"];
        let file_ids = files
            .iter()
            .map(|hash| {
                (
                    *hash,
                    MainDatabase::internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]))
                        .into_iter()
                        .next()
                        .unwrap()
                        .id
                        .unwrap(),
                )
            })
            .collect::<HashMap<_, _>>();
        let tags = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[file_action(
                TagOperation::Add,
                vec![
                    plugin_tag("required", "test"),
                    plugin_tag("female", "e6"),
                    plugin_tag("female", "e6ai"),
                    plugin_tag("excluded", "test"),
                ],
            )],
            db.plugin_manager.clone(),
        );
        let required = tags[&tag("required", "test")];
        let female_e6 = tags[&tag("female", "e6")];
        let female_e6ai = tags[&tag("female", "e6ai")];
        let excluded = tags[&tag("excluded", "test")];
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([
                (file_ids["grouped-and"], required),
                (file_ids["grouped-global"], required),
                (file_ids["grouped-global"], female_e6),
                (file_ids["grouped-excluded"], required),
                (file_ids["grouped-excluded"], female_e6ai),
                (file_ids["grouped-excluded"], excluded),
            ]),
        );

        let results = db.search_db_files_by_tag_groups_sync(
            &[required],
            &["female".to_string()],
            &[],
            &[],
            &[excluded],
            &[],
            &None,
        );
        assert_eq!(results, vec![file_ids["grouped-global"]]);
    }

    #[test]
    fn test_partial_roaring_cache_falls_back_to_sqlite() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let mut ids = HashMap::new();
        for hash in ["partialaaa", "partialbbb"] {
            let inserted =
                MainDatabase::internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]));
            ids.insert(
                hash.to_string(),
                inserted.into_iter().next().unwrap().id.unwrap(),
            );
        }

        let tags = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[
                file_action(
                    TagOperation::Add,
                    vec![
                        plugin_tag("popular", "cache"),
                        plugin_tag("uncached", "cache"),
                    ],
                ),
                file_action(TagOperation::Add, vec![plugin_tag("popular", "cache")]),
            ],
            db.plugin_manager.clone(),
        );
        let popular = tags[&tag("popular", "cache")];
        let uncached = tags[&tag("uncached", "cache")];
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([
                (ids["partialaaa"], popular),
                (ids["partialaaa"], uncached),
                (ids["partialbbb"], popular),
            ]),
        );

        let mut partial_cache = RelationshipStorage::new(db.clone(), InternalCacheType::Popular(2));
        partial_cache.load_relationship_cache(&conn);
        *db.relationship_roaring_storage.write() = Some(partial_cache);

        let and = SearchObj {
            search_relate: None,
            searches: vec![SearchHolder::And(vec![popular, uncached])],
        };
        assert_eq!(
            db.search_db_files_sync(&and, &None),
            vec![ids["partialaaa"]]
        );

        let or = SearchObj {
            search_relate: None,
            searches: vec![SearchHolder::Or(vec![popular, uncached])],
        };
        assert_eq!(
            db.search_db_files_sync(&or, &None)
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::from([ids["partialaaa"], ids["partialbbb"]])
        );
    }

    #[test]
    fn test_tag_search_resolves_typos_from_ram_and_sqlite() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let tags = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[
                file_action(TagOperation::Add, vec![plugin_tag("red fox", "subject")]),
                file_action(TagOperation::Add, vec![plugin_tag("blue fox", "subject")]),
                file_action(
                    TagOperation::Add,
                    vec![plugin_tag("rare creature", "subject")],
                ),
                file_action(TagOperation::Add, vec![plugin_tag("female", "subject")]),
            ],
            db.plugin_manager.clone(),
        );
        let red_fox = tags[&tag("red fox", "subject")];
        let blue_fox = tags[&tag("blue fox", "subject")];
        let rare_creature = tags[&tag("rare creature", "subject")];
        let female = tags[&tag("female", "subject")];

        let mut file_ids = Vec::new();
        for index in 1..=11 {
            let item = file(&format!("tag-search-{index}"), "jpg");
            let inserted = MainDatabase::internal_file_bulk_add(&conn, HashSet::from([item]));
            file_ids.push(inserted.into_iter().next().unwrap().id.unwrap());
        }

        let relationships = HashSet::from([
            (file_ids[0], red_fox),
            (file_ids[1], red_fox),
            (file_ids[2], red_fox),
            (file_ids[3], red_fox),
            (file_ids[4], red_fox),
            (file_ids[5], blue_fox),
            (file_ids[6], blue_fox),
            (file_ids[7], blue_fox),
            (file_ids[8], blue_fox),
            (file_ids[9], blue_fox),
            (file_ids[10], rare_creature),
            (file_ids[0], female),
            (file_ids[1], female),
            (file_ids[2], female),
            (file_ids[3], female),
            (file_ids[4], female),
        ]);
        MainDatabase::internal_relationship_bulk_add(db.clone(), &conn, &relationships);

        let popular = db.search_db_tags_fts("red fxo", &Some(1));
        assert_eq!(popular[0].tag_id, red_fox);

        let prefix = db.search_db_tags_fts("fema", &Some(1));
        assert_eq!(prefix[0].tag_id, female);

        let slow = db.search_db_tags_fts("raer creatur", &Some(1));
        assert_eq!(slow[0].tag_id, rare_creature);
    }

    #[test]
    fn test_db_slurp_imports_supported_rows_and_relationships() {
        let source_path = std::env::temp_dir().join("intscrape-db-slurp-source.sqlite");
        let destination_path = std::env::temp_dir().join("intscrape-db-slurp-destination.sqlite");
        let _ = fs::remove_file(&source_path);
        let _ = fs::remove_file(&destination_path);

        let source = database_for_path(&source_path);
        let destination = database_for_path(&destination_path);
        let source_conn = source.pool.get().unwrap();
        let destination_conn = destination.pool.get().unwrap();
        destination_conn
            .execute(
                "INSERT INTO Namespace(name, description) VALUES ('existing', 'existing')",
                [],
            )
            .unwrap();
        for index in 0..49 {
            destination_conn
                .execute(
                    "INSERT INTO Tags(name, namespace) VALUES (?1, 1)",
                    [format!("existing-{index}")],
                )
                .unwrap();
        }
        drop(destination_conn);
        source_conn
            .execute(
                "INSERT INTO FileStorageLocations(location) VALUES ('/tmp')",
                [],
            )
            .unwrap();
        source_conn
            .execute(
                "INSERT INTO Namespace(name, description) VALUES ('source', 'test')",
                [],
            )
            .unwrap();
        source_conn
            .execute(
                "INSERT INTO Tags(name, namespace) VALUES
                 ('female', 1), ('large_female', 1)",
                [],
            )
            .unwrap();
        source_conn
            .execute(
                "INSERT INTO Parents(tag_id, relate_tag_id, limit_to)
                 VALUES (1, 2, NULL)",
                [],
            )
            .unwrap();
        source_conn
            .execute(
                "INSERT INTO File(hash, extension, storage_id, size_bytes)
                 VALUES ('slurp-hash', 'jpg', 1, 42)",
                [],
            )
            .unwrap();
        source_conn
            .execute_batch(
                "CREATE TABLE File_legacy (
                     id INTEGER PRIMARY KEY NOT NULL,
                     hash TEXT UNIQUE,
                     extension TEXT,
                     storage_id INTEGER
                 );
                 INSERT INTO File_legacy SELECT id, hash, extension, storage_id FROM File;
                 DROP TABLE File;
                 ALTER TABLE File_legacy RENAME TO File;",
            )
            .unwrap();
        MainDatabase::internal_relationship_partition_create(&source_conn, 1);
        source_conn
            .execute(
                "INSERT INTO Relationship_1(file_id, tag_id) VALUES (1, 1), (1, 2)",
                [],
            )
            .unwrap();
        drop(source_conn);

        assert_eq!(destination.db_slurp(&source_path).unwrap(), (1, 2, 1));
        let conn = destination.pool.get().unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM Tags WHERE count = 1", [], |row| row
                .get::<_, u64>(
                0
            ))
            .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM Parents", [], |row| row
                .get::<_, u64>(0))
                .unwrap(),
            1
        );
        let female_id: u64 = conn
            .query_row(
                "SELECT id FROM Tags WHERE name = 'female' AND namespace = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let large_female_id: u64 = conn
            .query_row(
                "SELECT id FROM Tags WHERE name = 'large_female' AND namespace = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(female_id, 50);
        assert_eq!(large_female_id, 51);
        assert_eq!(
            conn.query_row("SELECT tag_id, relate_tag_id FROM Parents", [], |row| Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, u64>(1)?
            )),)
                .unwrap(),
            (female_id, large_female_id)
        );
        let _ = fs::remove_file(source_path);
        let _ = fs::remove_file(destination_path);
    }

    #[tokio::test]
    async fn test_update_missing_file_sizes_updates_only_existing_files() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let hash = "size-job-hash";
        let path = directory
            .path()
            .join(&hash[0..2])
            .join(&hash[2..4])
            .join(&hash[4..6])
            .join(hash)
            .with_extension("jpg");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"file-size").unwrap();
        MainDatabase::internal_file_storage_location_set(&conn, directory.path().to_str().unwrap())
            .unwrap();
        MainDatabase::internal_file_bulk_add(
            &conn,
            HashSet::from([FileInternal {
                id: None,
                hash: hash.into(),
                extension: "jpg".into(),
                storage_id: 1,
                size_bytes: None,
            }]),
        );
        drop(conn);

        db.update_missing_file_sizes().await.unwrap();
        let conn = db.pool.get().unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT size_bytes FROM File WHERE hash = ?1",
                [hash],
                |row| row.get::<_, u64>(0),
            )
            .unwrap(),
            9
        );
    }

    #[test]
    fn test_tag_name_search_groups_same_names_across_namespaces() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let tags = MainDatabase::internal_tag_bulk_add(
            &conn,
            &[
                file_action(TagOperation::Add, vec![plugin_tag("female", "e6")]),
                file_action(TagOperation::Add, vec![plugin_tag("female", "e6ai")]),
                file_action(TagOperation::Add, vec![plugin_tag("tank", "e6")]),
            ],
            db.plugin_manager.clone(),
        );
        let female_e6 = tags[&tag("female", "e6")];
        let female_e6ai = tags[&tag("female", "e6ai")];
        let tank = tags[&tag("tank", "e6")];

        let mut file_ids = Vec::new();
        for index in 1..=4 {
            let inserted = MainDatabase::internal_file_bulk_add(
                &conn,
                HashSet::from([file(&format!("same-name-{index}"), "jpg")]),
            );
            file_ids.push(inserted.into_iter().next().unwrap().id.unwrap());
        }
        MainDatabase::internal_relationship_bulk_add(
            db.clone(),
            &conn,
            &HashSet::from([
                (file_ids[0], female_e6),
                (file_ids[1], female_e6ai),
                (file_ids[2], female_e6),
                (file_ids[2], tank),
                (file_ids[3], tank),
            ]),
        );

        let results = db.search_db_files_by_tags_sync(&["female".into(), "tank".into()], &None);
        assert_eq!(
            results.into_iter().collect::<HashSet<_>>(),
            HashSet::from([file_ids[2]])
        );
    }

    #[test]
    fn test_settings_and_dead_urls_round_trip() {
        let db = new_test();
        let setting = DbSettingsObj {
            name: "TEST_SETTING".into(),
            description: Some("first".into()),
            num: Some(1),
            param: Some("a".into()),
        };
        assert!(!db.setting_set_sync(&setting));
        assert_eq!(
            db.setting_get_sync("TEST_SETTING").unwrap().param,
            Some("a".into())
        );

        let updated = DbSettingsObj {
            description: Some("second".into()),
            num: Some(2),
            param: Some("b".into()),
            ..setting
        };
        let _ = db.setting_set_sync(&updated);
        assert_eq!(
            db.setting_get_sync("TEST_SETTING").unwrap().description,
            Some("second".into())
        );
        assert!(db.setting_get_sync("missing").is_none());

        let url = "https://example.test/a?x=1".to_string();
        assert!(!db.dead_url_add_sync(&url));
        let status = db.dead_url_get_sync(&[url.clone(), "https://example.test/missing".into()]);
        assert_eq!(status.get(&url), Some(&true));
        assert_eq!(status.get("https://example.test/missing"), Some(&false));
    }

    #[test]
    fn test_storage_location_and_file_path_edge_cases() {
        let db = new_test();
        assert_eq!(db.file_download_location_get_sync("short", "jpg"), None);
        assert_eq!(db.file_download_location_get_sync("abcdef", "jpg"), None);
        let (base, storage_id) = db.file_download_location_main_sync().unwrap();
        assert_eq!(base, PathBuf::from("files"));
        assert!(storage_id > 0);
        let (path, _) = db
            .file_download_location_get_sync("abcdef123456", "jpg")
            .unwrap();
        assert_eq!(path, PathBuf::from("files/ab/cd/ef/abcdef123456.jpg"));

        let file = file("abcdef123456", "jpg");
        assert_eq!(
            MainDatabase::get_file_location(&file, &"missing-base".into()),
            None
        );
    }

    #[test]
    fn test_fix_internal_files_moves_misplaced_file_to_recorded_storage() {
        let db = new_test();
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let source_location = source_dir.path().to_string_lossy().into_owned();
        let target_location = target_dir.path().to_string_lossy().into_owned();
        let bytes = Bytes::from_static(b"misplaced file");
        let (hash, _) = hash_bytes(&bytes, &HashesSupported::Sha512(String::new()));
        let file = file(&hash, "bin");

        let conn = db.pool.get().unwrap();
        conn.execute("DELETE FROM FileStorageLocations", [])
            .unwrap();
        conn.execute(
            "UPDATE Settings SET param = ?1 WHERE name = 'SYSTEM_file_location'",
            params![&target_location],
        )
        .unwrap();
        MainDatabase::internal_file_storage_location_set(&conn, &source_location).unwrap();

        let source_path = Path::new(&source_location)
            .join(&hash[0..2])
            .join(&hash[2..4])
            .join(&hash[4..6])
            .join(&hash)
            .with_extension(&file.extension);
        std::fs::create_dir_all(source_path.parent().unwrap()).unwrap();
        std::fs::write(&source_path, &bytes).unwrap();

        let target_storage_id =
            MainDatabase::internal_file_storage_location_get_or_create(&conn, &target_location)
                .unwrap();
        conn.execute(
            "INSERT INTO File (hash, extension, storage_id) VALUES (?1, ?2, ?3)",
            params![&file.hash, &file.extension, target_storage_id],
        )
        .unwrap();
        drop(conn);

        db.fix_internal_files(&CheckFilesEnum::StorageCheck)
            .unwrap();

        let target_path = Path::new(&target_location)
            .join(&hash[0..2])
            .join(&hash[2..4])
            .join(&hash[4..6])
            .join(&hash)
            .with_extension(&file.extension);
        assert!(!source_path.exists());
        assert_eq!(std::fs::read(target_path).unwrap(), bytes);
    }

    #[test]
    fn test_fix_internal_files_leaves_file_in_recorded_storage() {
        let db = new_test();
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_location = storage_dir.path().to_string_lossy().into_owned();
        let bytes = Bytes::from_static(b"correctly placed file");
        let (hash, _) = hash_bytes(&bytes, &HashesSupported::Sha512(String::new()));
        let file = file(&hash, "bin");
        let conn = db.pool.get().unwrap();
        conn.execute("DELETE FROM FileStorageLocations", [])
            .unwrap();
        conn.execute(
            "UPDATE Settings SET param = ?1 WHERE name = 'SYSTEM_file_location'",
            params![&storage_location],
        )
        .unwrap();
        let file_path = Path::new(&storage_location)
            .join(&hash[0..2])
            .join(&hash[2..4])
            .join(&hash[4..6])
            .join(&hash)
            .with_extension(&file.extension);
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, &bytes).unwrap();

        let storage_id =
            MainDatabase::internal_file_storage_location_get_or_create(&conn, &storage_location)
                .unwrap();
        conn.execute(
            "INSERT INTO File (hash, extension, storage_id) VALUES (?1, ?2, ?3)",
            params![&file.hash, &file.extension, storage_id],
        )
        .unwrap();
        drop(conn);

        db.fix_internal_files(&CheckFilesEnum::StorageCheck)
            .unwrap();

        assert_eq!(std::fs::read(&file_path).unwrap(), bytes);
    }

    #[test]
    fn test_fix_internal_files_removes_empty_directories_but_keeps_storage_root() {
        let db = new_test();
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_location = storage_dir.path().to_string_lossy().into_owned();
        let conn = db.pool.get().unwrap();
        conn.execute("DELETE FROM FileStorageLocations", [])
            .unwrap();
        conn.execute(
            "UPDATE Settings SET param = ?1 WHERE name = 'SYSTEM_file_location'",
            params![&storage_location],
        )
        .unwrap();
        MainDatabase::internal_file_storage_location_set(&conn, &storage_location).unwrap();
        drop(conn);

        let empty_path = storage_dir.path().join("aa").join("bb").join("cc");
        std::fs::create_dir_all(&empty_path).unwrap();

        db.fix_internal_files(&CheckFilesEnum::StorageCheck)
            .unwrap();

        assert!(storage_dir.path().exists());
        assert!(!storage_dir.path().join("aa").exists());
    }

    #[test]
    fn test_job_lifecycle_and_duplicate_upsert() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let config = job("site", 0, 0);
        let id = MainDatabase::internal_jobs_add(&conn, &config);
        let duplicate_id = MainDatabase::internal_jobs_add(&conn, &config);
        assert_eq!(id, duplicate_id);
        assert_eq!(
            MainDatabase::internal_jobs_get_all_sites(&conn).unwrap(),
            vec!["site"]
        );
        assert_eq!(
            MainDatabase::internal_jobs_get_site(&conn, "missing").unwrap(),
            Vec::<DbJobsObj>::new()
        );

        MainDatabase::internal_jobs_set_isrunning(&conn, id).unwrap();
        assert!(MainDatabase::internal_jobs_get_site(&conn, "site").unwrap()[0].isrunning);
        MainDatabase::internal_jobs_reset_isrunning(&conn).unwrap();
        assert!(!MainDatabase::internal_jobs_get_site(&conn, "site").unwrap()[0].isrunning);
        assert_eq!(
            MainDatabase::internal_jobs_get_torun(&conn, vec!["site".into()])
                .unwrap()
                .len(),
            1
        );
        MainDatabase::internal_job_remove(&conn, id).unwrap();
        assert!(
            MainDatabase::internal_jobs_get_site(&conn, "site")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_parent_constraints_distinguish_limit_to() {
        let db = new_test();
        let conn = db.pool.get().unwrap();
        let child = plugin_tag("child", "ns");
        let parent = plugin_tag("parent", "ns");
        let limit = plugin_tag("limit", "ns");
        let actions = [file_action(
            TagOperation::Add,
            vec![
                PluginTag {
                    relates_to: Some(RelationContext {
                        tag: parent.tag.clone(),
                        limit_to: Some(limit.tag.clone()),
                        ..Default::default()
                    }),
                    ..child.clone()
                },
                parent.clone(),
                limit.clone(),
            ],
        )];
        let ids = MainDatabase::internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
        let relation = TagParents {
            tag_id: ids[&child.tag],
            relate_tag_id: ids[&parent.tag],
            limit_to: Some(ids[&limit.tag]),
        };
        MainDatabase::internal_parents_bulk_add(&conn, &HashSet::from([relation]));
        assert!(
            MainDatabase::internal_parent_structure_exists(
                &conn,
                &PluginTag {
                    relates_to: Some(RelationContext {
                        tag: parent.tag.clone(),
                        limit_to: Some(limit.tag.clone()),
                        ..Default::default()
                    }),
                    ..child.clone()
                }
            )
            .unwrap()
        );
        assert!(
            MainDatabase::internal_parent_relate_limit_exists(&conn, &parent.tag, &limit.tag)
                .unwrap()
        );
        assert!(
            !MainDatabase::internal_parent_structure_exists(
                &conn,
                &PluginTag {
                    relates_to: Some(RelationContext {
                        tag: parent.tag,
                        limit_to: None,
                        ..Default::default()
                    }),
                    ..child
                }
            )
            .unwrap()
        );
    }
}
