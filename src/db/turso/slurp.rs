//! Turso-native database import. Unlike the legacy SQLite implementation there
//! is no `ATTACH DATABASE`: the source is opened read-only through rusqlite and
//! every page is streamed into the turso database via the bulk-add helpers.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use shared_types::{
    DbJobRecreation, FileInternal, FileTagAction, GenericNamespaceObj, PluginJob, PluginTag,
    ScraperParam, Tag, TagOperation, TagParents, TagType,
};
use turso::{Connection, Result};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Copies the supported data from another SQLite database into turso.
    pub async fn db_slurp(&self, source: &Path) -> Result<(u64, u64, u64)> {
        if !source.is_file() {
            return Err(turso::Error::ConversionFailure(
                "source must be a file".into(),
            ));
        }

        let read_flags = r2d2_sqlite::rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY;
        let source_conn = r2d2_sqlite::rusqlite::Connection::open_with_flags(source, read_flags)
            .map_err(db_error)?;

        loop {
            match self.internal_db_slurp(&source_conn).await {
                Ok(result) => return Ok(result),
                Err(error) if matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => {
                    log::warn!("Turso slurp transaction conflicted; retrying in 50ms: {error}");
                    // The failed attempt's inserts rolled back wholesale, so any
                    // storage-location ids it cached are stale. Re-seed from the
                    // committed rows so the retry cannot reference missing rows.
                    if let Err(reload_error) = self.file_storage_location_cache_reload().await {
                        log::warn!("Failed to reload storage-location cache after conflict: {reload_error}");
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Blocking variant of [`Self::db_slurp`] for use from async contexts that
    /// must keep their own future `Send` (the rusqlite source connection is
    /// not `Sync`, so holding it across awaits makes the async slurp future
    /// thread-unsafe). Runs the slurp on the thread-local blocking runtime.
    pub fn db_slurp_blocking(&self, source: &Path) -> Result<(u64, u64, u64)> {
        super::api::block_on(self.db_slurp(source))
    }

    /// Streams the source database's namespaces, tags, files, hashes,
    /// relationships, and parents into turso.
    async fn internal_db_slurp(
        &self,
        source: &r2d2_sqlite::rusqlite::Connection,
    ) -> Result<(u64, u64, u64)> {
        let conn = self.connect()?;

        // Namespaces, remembering id -> object and name -> target id.
        let mut ns_by_source_id: HashMap<u64, GenericNamespaceObj> = HashMap::new();
        let mut namespace_set: HashSet<GenericNamespaceObj> = HashSet::new();
        {
            let mut stmt = source
                .prepare("SELECT id, name, description FROM Namespace")
                .map_err(db_error)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        GenericNamespaceObj {
                            name: row.get(1)?,
                            description: row.get(2)?,
                        },
                    ))
                })
                .map_err(db_error)?;
            for row in rows {
                let (id, ns) = row.map_err(db_error)?;
                ns_by_source_id.insert(id, ns.clone());
                namespace_set.insert(ns);
            }
        }

        // Ensuring namespaces runs CREATE TABLE for their Relationship_N
        // partitions — DDL, which turso forbids inside BEGIN CONCURRENT.
        // `namespace_ensure_set` does it in a short exclusive transaction up
        // front (also seeding the in-memory namespace cache) so the concurrent
        // copy below only ever executes DML, and so its snapshot sees the
        // committed namespace rows. Now largely a no-op once namespaces are
        // cached (the common warm-db case).
        let namespace_bulk = self.namespace_ensure_set(&namespace_set).await?;
        let namespace_count = namespace_bulk.len() as u64;

        // A slurp can run for a long time. BEGIN CONCURRENT lets unrelated
        // Turso writers, such as the job scheduler, proceed while this import
        // builds its snapshot. Conflicts are reported at COMMIT instead of
        // blocking every writer for the duration of the import.
        conn.execute("BEGIN CONCURRENT", ()).await?;

        // Storage locations: re-use whatever rows exist, creating the rest.
        let mut locations = source
            .prepare("SELECT location FROM FileStorageLocations")
            .map_err(db_error)?;
        let mut location_rows = locations.query([]).map_err(db_error)?;
        while let Some(row) = location_rows.next().map_err(db_error)? {
            let location: String = row.get(0).map_err(db_error)?;
            self.file_storage_location_get_or_create(&conn, &location).await?;
        }
        drop(location_rows);

        // Tags, chunked by id, building the source -> target tag map.
        let mut slurp_tags: HashMap<u64, u64> = HashMap::new();
        let mut tag_count = 0_u64;
        {
            let mut last_tag_id = 0_u64;
            loop {
                let mut stmt = source
                    .prepare(
                        "SELECT s.id, s.name, n.name, n.description
                         FROM Tags s
                         JOIN Namespace n ON n.id = s.namespace
                         WHERE s.id > ?1
                         ORDER BY s.id
                         LIMIT ?2",
                    )
                    .map_err(db_error)?;
                let rows = stmt
                    .query_map([last_tag_id as i64, SQL_CHUNK_SIZE as i64], |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    })
                    .map_err(db_error)?;
                let batch: Vec<_> = rows.filter_map(|r| r.ok()).collect();
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
                log::info!("Slurping {} tags into the db.", batch.len());
                let mapping = self.tag_action_bulk_add(&conn, &actions).await?;
                tag_count += batch.len() as u64;
                for (source_id, name, namespace, description) in &batch {
                    let key = Tag {
                        name: name.clone(),
                        namespace: GenericNamespaceObj {
                            name: namespace.clone(),
                            description: description.clone(),
                        },
                    };
                    if let Some(&target_id) = mapping.get(&key) {
                        slurp_tags.insert(*source_id, target_id as u64);
                    }
                }
                last_tag_id = *last_id;
            }
        }

        // Files, chunked by id.
        let mut slurp_files: HashMap<u64, u64> = HashMap::new();
        let mut file_count = 0_u64;
        {
            let file_schema: String = source
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'File'",
                    [],
                    |row| row.get(0),
                )
                .map_err(db_error)?;
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
                     FROM File f
                     LEFT JOIN FileStorageLocations s ON s.id = f.storage_id
                     WHERE f.id > ?1 AND f.hash IS NOT NULL
                     ORDER BY f.id
                     LIMIT ?2"
                );
                let mut stmt = source.prepare(&file_query).map_err(db_error)?;
                let rows = stmt
                    .query_map([last_file_id as i64, SQL_CHUNK_SIZE as i64], |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<u64>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    })
                    .map_err(db_error)?;
                let batch: Vec<_> = rows.filter_map(|r| r.ok()).collect();
                drop(stmt);
                let Some((last_id, _, _, _, _)) = batch.last() else {
                    break;
                };

                let mut files = HashSet::new();
                for (_, hash, extension, size_bytes, location) in &batch {
                    let storage_id = match location.as_deref() {
                        Some(location) => {
                            self.file_storage_location_get_or_create(&conn, location).await?
                        }
                        None => 0,
                    };
                    files.insert(FileInternal {
                        id: None,
                        hash: hash.clone(),
                        extension: extension.clone(),
                        storage_id,
                        size_bytes: *size_bytes,
                    });
                }
                file_count += files.len() as u64;
                log::info!("Slurping {} files into the db.", files.len());
                let resolved = self
                    .file_add_bulk(&conn, &files.iter().cloned().collect::<Vec<_>>())
                    .await?;
                let target_by_hash: HashMap<&str, u64> = resolved
                    .iter()
                    .filter_map(|file| file.id.map(|id| (file.hash.as_str(), id)))
                    .collect();
                for (source_id, hash, _, _, _) in &batch {
                    if let Some(&target_id) = target_by_hash.get(hash.as_str()) {
                        slurp_files.insert(*source_id, target_id);
                    }
                }

                last_file_id = *last_id;
            }
        }

        // Secondary hashes, keyed through the source file id.
        {
            let has_file_hashes = source
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'table' AND name = 'FileHashes'
                     )",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(db_error)? == 1;
            if has_file_hashes {
                let mut last_file_id = 0_u64;
                loop {
                    let mut stmt = source
                        .prepare(
                            "SELECT h.file_id, h.algorithm, h.digest
                             FROM FileHashes h
                             WHERE h.file_id > ?1
                             ORDER BY h.file_id
                             LIMIT ?2",
                        )
                        .map_err(db_error)?;
                    let rows = stmt
                        .query_map([last_file_id as i64, SQL_CHUNK_SIZE as i64], |row| {
                            Ok((
                                row.get::<_, u64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        })
                        .map_err(db_error)?;
                    let batch: Vec<_> = rows.filter_map(|r| r.ok()).collect();
                    drop(stmt);
                    let Some((last_id, _, _)) = batch.last() else {
                        break;
                    };
                    let tuples: Vec<_> = batch
                        .iter()
                        .filter_map(|(file_id, algorithm, digest)| {
                            slurp_files
                                .get(file_id)
                                .map(|target_id| (*target_id, algorithm.as_str(), digest.as_str()))
                        })
                        .collect();
                log::info!("Slurping {} hashes into the db.", tuples.len());
                    if !tuples.is_empty() {
                        self.file_hashes_add_bulk(&conn, &tuples).await?;
                    }
                    last_file_id = *last_id;
                }
            }
        }

        // Relationships, one source namespace partition (or the legacy single
        // `Relationship` table) at a time.
        {
            let source_table_exists =
                |name: &str| -> Result<bool> {
                    let mut stmt = source
                        .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1")
                        .map_err(db_error)?;
                    let mut rows = stmt.query([name]).map_err(db_error)?;
                    Ok(rows.next().map_err(db_error)?.is_some())
                };
            let has_legacy_relationship = source_table_exists("Relationship")?;

            for source_namespace in ns_by_source_id.keys() {
                let partition = format!("Relationship_{source_namespace}");
                let source_table = if source_table_exists(&partition)? {
                    partition
                } else if has_legacy_relationship {
                    "Relationship".to_string()
                } else {
                    continue;
                };

                let mut last_file_id = 0_u64;
                let mut last_tag_id = 0_u64;
                loop {
                    let mut stmt = source
                        .prepare(&format!(
                            "SELECT r.file_id, r.tag_id
                             FROM {source_table} r
                             WHERE r.file_id > ?1 OR (r.file_id = ?1 AND r.tag_id > ?2)
                             ORDER BY r.file_id, r.tag_id
                             LIMIT ?3"
                        ))
                        .map_err(db_error)?;
                    let rows = stmt
                        .query_map([last_file_id as i64, last_tag_id as i64, SQL_CHUNK_SIZE as i64], |row| {
                            Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?))
                        })
                        .map_err(db_error)?;
                    let batch: Vec<_> = rows.filter_map(|r| r.ok()).collect();
                    drop(stmt);
                    let Some((source_file_id, source_tag_id)) = batch.last() else {
                        break;
                    };

                    let relationships = batch
                        .iter()
                        .filter_map(|(file_id, tag_id)| {
                            let target_file = slurp_files.get(file_id)?;
                            let target_tag = slurp_tags.get(tag_id)?;
                            Some((*target_file, *target_tag))
                        })
                        .collect::<HashSet<_>>();
                log::info!("Slurping {} relationships into the db.", relationships.len());
                    self.relationships_bulk_add(&conn, &relationships).await?;

                    last_file_id = *source_file_id;
                    last_tag_id = *source_tag_id;
                }
            }
        }

        // Parents.
        {
            let mut parents = source
                .prepare("SELECT tag_id, relate_tag_id, limit_to FROM Parents")
                .map_err(db_error)?;
            let rows = parents
                .query_map([], |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, Option<u64>>(2)?,
                    ))
                })
                .map_err(db_error)?;
            let mut parent_set = HashSet::new();
            for row in rows {
                let (tag_id, relate_tag_id, limit_to) = row.map_err(db_error)?;
                if let (Some(&tag_id), Some(&relate_tag_id)) =
                    (slurp_tags.get(&tag_id), slurp_tags.get(&relate_tag_id))
                {
                    parent_set.insert(TagParents {
                        tag_id,
                        relate_tag_id,
                        limit_to: limit_to.and_then(|id| slurp_tags.get(&id).copied()),
                    });
                }
            }
                log::info!("Slurping {} parents into the db.", parent_set.len());
            self.parents_bulk_add(&conn, &parent_set).await?;
        }

        // Jobs. Source ids are intentionally not preserved: the target Jobs
        // table owns its ids, while the existing deduplication key prevents
        // duplicate imports from creating duplicate work. The copy tolerates
        // both the IntScrape legacy schema (`recreation`/`user_data`) and
        // Rust-Hydrus-style schemas (`Manager`/`UserData`/optional `priority`).
        let has_jobs: bool = source
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master
                     WHERE type = 'table' AND name = 'Jobs'
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        if has_jobs {
            self.slurp_jobs(&conn, source).await?;
        }

        conn.execute("COMMIT", ()).await?;

        Ok((namespace_count, tag_count, file_count))
    }

    /// Copies `Jobs` rows into turso, mapping whichever schema the source
    /// uses. Required columns are `time`, `reptime`, `site`, `param`; when
    /// those are missing the step is skipped rather than aborting the import.
    /// `priority` defaults to 10 when absent or NULL; `recreation` is read
    /// from `recreation` (IntScrape) or the `Manager` JSON (Rust-Hydrus);
    /// `user_data` comes from `user_data` or `UserData`. Rows whose payloads
    /// cannot be parsed are skipped with a warning so one bad job never rolls
    /// back an otherwise complete slurp.
    async fn slurp_jobs(
        &self,
        conn: &Connection,
        source: &r2d2_sqlite::rusqlite::Connection,
    ) -> Result<()> {
        let mut columns = HashSet::new();
        {
            let mut stmt = source
                .prepare("PRAGMA table_info(\"Jobs\")")
                .map_err(db_error)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .map_err(db_error)?;
            for row in rows {
                columns.insert(row.map_err(db_error)?);
            }
        }

        const REQUIRED_JOBS_COLUMNS: [&str; 4] = ["time", "reptime", "site", "param"];
        if !REQUIRED_JOBS_COLUMNS
            .iter()
            .all(|column| columns.contains(*column))
        {
            log::warn!(
                "Slurp: source Jobs table is missing required columns; skipping jobs."
            );
            return Ok(());
        }

        // Column names come from a fixed allowlist below, so interpolating
        // them into the SQL is safe.
        let priority_expr = if columns.contains("priority") {
            "priority"
        } else {
            "NULL"
        };
        let recreation_expr = if columns.contains("recreation") {
            "recreation"
        } else if columns.contains("Manager") {
            "Manager"
        } else {
            "NULL"
        };
        let user_data_expr = if columns.contains("user_data") {
            "user_data"
        } else if columns.contains("UserData") {
            "UserData"
        } else {
            "NULL"
        };

        let select = format!(
            "SELECT time, reptime, {priority_expr}, {recreation_expr}, site, param, {user_data_expr}
             FROM Jobs ORDER BY id"
        );
        let mut stmt = source.prepare(&select).map_err(db_error)?;
        let rows = stmt
            .query_map(
                [],
                |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, Option<u64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .map_err(db_error)?;

        let mut copied = 0_u64;
        let mut batch: Vec<PluginJob> = Vec::with_capacity(SQL_CHUNK_SIZE);
        for row in rows {
            let (time, reptime, priority, recreation_json, site, param, user_data_json) =
                row.map_err(db_error)?;

            let params = match serde_json::from_str::<Vec<ScraperParam>>(&param) {
                Ok(params) => params,
                Err(error) => {
                    log::warn!(
                        "Slurp: skipping job for site '{site}' with unparseable param ({error})."
                    );
                    continue;
                }
            };
            let user_data = match user_data_json {
                Some(json) if !json.is_empty() => match serde_json::from_str(&json) {
                    Ok(data) => data,
                    Err(error) => {
                        log::warn!(
                            "Slurp: skipping job for site '{site}' with unparseable user data ({error})."
                        );
                        continue;
                    }
                },
                _ => std::collections::BTreeMap::new(),
            };

            let job = PluginJob {
                time,
                reptime,
                priority: priority.unwrap_or(10),
                recreation: recreation_json
                    .as_deref()
                    .and_then(parse_slurp_job_recreation),
                site: site.clone(),
                param: params,
                user_data,
            };
            batch.push(job);
            if batch.len() >= SQL_CHUNK_SIZE {
                self.jobs_bulk_add_sql(&conn, &batch).await?;
                copied += batch.len() as u64;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.jobs_bulk_add_sql(&conn, &batch).await?;
            copied += batch.len() as u64;
        }
        log::info!("Slurping {copied} jobs into the db.");

        Ok(())
    }
}

/// Extracts the recreation config from either the IntScrape legacy `recreation`
/// column (the `Option<DbJobRecreation>` itself) or a Rust-Hydrus `Manager`
/// column (a `DbJobsManager` object wrapping a `recreation` field).
fn parse_slurp_job_recreation(json: &str) -> Option<DbJobRecreation> {
    if let Ok(recreation) = serde_json::from_str::<Option<DbJobRecreation>>(json) {
        return recreation;
    }
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    value
        .get("recreation")
        .and_then(|recreation| serde_json::from_value::<Option<DbJobRecreation>>(recreation.clone()).ok())
        .flatten()
}

fn db_error(error: r2d2_sqlite::rusqlite::Error) -> turso::Error {
    turso::Error::ConversionFailure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    async fn new_target() -> Arc<TursoDatabase> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("target.db");
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    }

    /// Point a source sqlite file at `script` and return a connection to it.
    /// The tempdir must outlive the connection, so both are returned.
    fn new_source(
        script: &str,
    ) -> (r2d2_sqlite::rusqlite::Connection, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let source_path = temp_dir.path().join("source.db");
        let conn =
            r2d2_sqlite::rusqlite::Connection::open(&source_path).unwrap();
        conn.execute_batch(script).unwrap();
        (conn, temp_dir)
    }

    async fn target_jobs(
        db: &TursoDatabase,
    ) -> Vec<(String, u64, Option<String>, Option<String>)> {
        let conn = db.connect().unwrap();
        let mut rows = conn
            .query(
                "SELECT site, priority, recreation, user_data FROM Jobs ORDER BY id;",
                (),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Ok(Some(row)) = rows.next().await {
            out.push((
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get::<Option<String>>(3).unwrap(),
            ));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_jobs_copies_intscrape_legacy_schema() {
        let db = new_target().await;
        let (source, _keep) = new_source(
            "CREATE TABLE Jobs (
                 id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
                 reptime INTEGER NOT NULL, priority INTEGER NOT NULL,
                 recreation TEXT NOT NULL, site TEXT NOT NULL,
                 param TEXT NOT NULL, user_data TEXT NOT NULL);
             INSERT INTO Jobs VALUES
                 (1, 100, 60, 5, '{\"OnTagId\":[12,null]}', 'e621', '[]', '{\"k\":\"v\"}'),
                 (2, 200, 0, 10, 'null', 'gelbooru', '[{\"Normal\":\"cute\"}]', '{}');",
        );

        let conn = db.connect().unwrap();
        db.slurp_jobs(&conn, &source).await.unwrap();

        let jobs = target_jobs(&db).await;
        assert_eq!(jobs.len(), 2, "both legacy jobs must be copied");
        let (site, priority, recreation, user_data) = &jobs[0];
        assert_eq!(site, "e621");
        assert_eq!(*priority, 5);
        assert!(recreation.as_deref().unwrap().contains("OnTagId"), "recreation from recreation column");
        assert_eq!(user_data.as_deref().unwrap(), "{\"k\":\"v\"}");
        assert_eq!(jobs[1].1, 10);
        assert_eq!(jobs[1].2, Some("null".into()), "empty recreation stays null");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_jobs_copies_rusthydrus_schema() {
        let db = new_target().await;
        let (source, _keep) = new_source(
            "CREATE TABLE Jobs (
                 id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
                 reptime INTEGER NOT NULL, priority INTEGER,
                 Manager TEXT NOT NULL, site TEXT NOT NULL,
                 param TEXT NOT NULL, SystemData TEXT NOT NULL,
                 UserData TEXT NOT NULL);
             INSERT INTO Jobs VALUES
                 (1, 300, 60, 7, '{\"jobtype\":\"Params\",\"recreation\":{\"OnTag\":[\"dog\",3,null]}}',
                  'r34', '[{\"Normal\":\"kino\"}]', '{\"sys\":\"x\"}', '{\"user\":\"y\"}'),
                 (2, 400, 0, NULL, '{\"jobtype\":\"Params\",\"recreation\":null}',
                  'saucenao', '[]', '{}', '{}');",
        );

        let conn = db.connect().unwrap();
        db.slurp_jobs(&conn, &source).await.unwrap();

        let jobs = target_jobs(&db).await;
        assert_eq!(jobs.len(), 2, "both rust-hydrus jobs must be copied");
        let (site, priority, recreation, user_data) = &jobs[0];
        assert_eq!(site, "r34");
        assert_eq!(*priority, 7);
        assert!(
            recreation.as_deref().unwrap().contains("OnTag"),
            "recreation must be read out of the Manager JSON"
        );
        assert_eq!(user_data.as_deref().unwrap(), "{\"user\":\"y\"}", "user data from UserData");
        assert_eq!(jobs[1].0, "saucenao");
        assert_eq!(jobs[1].1, 10, "NULL priority defaults to 10");
        assert_eq!(jobs[1].2, Some("null".into()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slurp_jobs_skips_incomplete_jobs_table() {
        let db = new_target().await;
        let (source, _keep) = new_source(
            "CREATE TABLE Jobs (
                 id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
                 site TEXT NOT NULL, param TEXT NOT NULL);",
        );

        let conn = db.connect().unwrap();
        db.slurp_jobs(&conn, &source).await.unwrap();

        let jobs = target_jobs(&db).await;
        assert!(jobs.is_empty(), "missing columns must not abort the slurp");
    }
}
