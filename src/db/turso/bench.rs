//! Benchmarks for the database functions that sit on the hot paths.
//!
//! Manual benchmarks, following the convention of the scale benchmarks in
//! `search.rs` and `relationship.rs`: ignored tests that build a realistic
//! fixture, then print per-row latency. Run them with
//!
//! ```text
//! cargo test --lib db::turso::bench -- --ignored --nocapture
//! ```
//!
//! Every knob is an environment variable so a run can be scaled without
//! editing code. Defaults are deliberately small enough to finish in seconds;
//! the interesting numbers come from scaling up:
//!
//! ```text
//! BENCH_FILES=200000 BENCH_TAGS=200000 cargo test --lib \
//!     db::turso::bench -- --ignored --nocapture
//! ```
//!
//! `BENCH_PATH` forces an on-disk location; by default the fixture goes in
//! `/dev/shm` when available so the measurement is of the database and not of
//! the filesystem, and `BENCH_ON_DISK=1` opts out.
//!
//! What these are for: they exist to catch *regressions in shape*, not to
//! produce portable absolute numbers. Turso's timings move with machine, page
//! cache warmth and MVCC journal state, so treat a run as a baseline to diff
//! against, and read the per-row figures against `SQL_CHUNK_SIZE` to see
//! whether a bulk path is actually batching.

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use shared_types::{
        FileInternal, FileManager, FileTagAction, GenericNamespaceObj, PluginJob,
        ScraperDataReturn, ScraperParam, SearchHolder, SearchObj, Tag, TagOperation, TagParents,
        TagType, Url,
    };
    use turso::{Builder, Connection, Value, params_from_iter};

    use crate::db::SQL_CHUNK_SIZE;
    use crate::db::turso::sql::{parents_insert_sql, parents_params};
    use crate::db::turso::{PooledConnection, TagDb, TursoDatabase};

    /// Rows a benchmark seeds unless `BENCH_FILES` says otherwise.
    const DEFAULT_FILES: u64 = 50_000;

    /// Distinct tags seeded. Deliberately comparable to `DEFAULT_FILES`: tag
    /// resolution is the other hot path, and a fixture with far fewer tags than
    /// files would only measure the tag-cache-friendly case.
    const DEFAULT_TAGS: u64 = 50_000;

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    /// A RAM-backed fixture path unless the caller asks for disk, mirroring
    /// `search::scale_db_path`.
    fn bench_db_path(label: &str) -> PathBuf {
        if let Ok(forced) = std::env::var("BENCH_PATH") {
            return PathBuf::from(forced);
        }
        let on_disk = std::env::var("BENCH_ON_DISK").is_ok();
        let dir = if !on_disk && Path::new("/dev/shm").exists() {
            PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        };
        dir.join(format!("intscrape-bench-{label}-{}.db", std::process::id()))
    }

    /// Removes the database and turso's sidecars. The MVCC journal keeps a
    /// `-log` file beside the database; leaving it behind makes the next open
    /// fail with "MVCC logical log file exists ... but database header
    /// indicates WAL".
    fn bench_remove(path: &Path) {
        let _ = std::fs::remove_file(path);
        for suffix in ["-log", "-wal", "-shm"] {
            let mut side = path.to_path_buf().into_os_string();
            side.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(side));
        }
    }

    /// A seeded fixture: one storage location, `files` files, `tags` tags in a
    /// `source_url` namespace, and one `source_url` tag attached to every file
    /// so the tag-resolution path has something real to resolve.
    struct Fixture {
        db: Arc<TursoDatabase>,
        path: PathBuf,
        conn: PooledConnection,
        storage_id: u64,
        /// `(file_id, tag_id)` relationships, one source_url tag per file.
        rels: HashSet<(u64, u64)>,
        source_url_tag_id: u64,
        files: u64,
        tags: u64,
    }

    impl Fixture {
        async fn seed(label: &str, files: u64, tags: u64) -> Self {
            let path = bench_db_path(label);
            bench_remove(&path);
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&path, should_exit.clone()).await;
            let conn = db.connect().unwrap();

            let storage_id = db
                .file_storage_location_get_or_create(&conn, "bench_storage")
                .await
                .unwrap();

            // Files, in SQL_CHUNK_SIZE batches so seeding exercises the same
            // chunking the scraper does.
            let file_ids = seed_files(&db, &conn, storage_id, files).await;

            // Tags. One namespace keeps the fixture simple; the resolve path
            // groups per namespace either way.
            let tag_set: HashSet<Tag> = (0..tags)
                .map(|i| Tag {
                    name: format!("bench-tag-{i}"),
                    namespace: GenericNamespaceObj {
                        name: "source_url".to_string(),
                        description: None,
                    },
                })
                .collect();
            let inserted: HashSet<TagDb> = db.tag_add_bulk(&conn, &tag_set).await.unwrap();
            assert_eq!(
                inserted.len(),
                tags as usize,
                "every seeded tag must resolve an id"
            );
            let mut ids: Vec<u64> = inserted.iter().map(|tag| tag.id as u64).collect();
            ids.sort_unstable();
            let source_url_tag_id = ids[0];

            let rels: HashSet<(u64, u64)> = file_ids
                .iter()
                .map(|file_id| (*file_id, source_url_tag_id))
                .collect();
            let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
            db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

            Self {
                db,
                path,
                conn,
                storage_id,
                rels,
                source_url_tag_id,
                files,
                tags,
            }
        }

        fn file_ids(&self) -> Vec<u64> {
            let mut ids: Vec<u64> = self.rels.iter().map(|(file_id, _)| *file_id).collect();
            ids.sort_unstable();
            ids
        }

        fn describe(&self) -> String {
            format!(
                "{} files, {} tags, {} relationships ({})",
                self.files,
                self.tags,
                self.rels.len(),
                self.path.display()
            )
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Only clean up when we own the path, so a caller forcing
            // `BENCH_PATH` can keep the database for post-mortem inspection.
            if std::env::var("BENCH_KEEP").is_err() && std::env::var("BENCH_PATH").is_err() {
                bench_remove(&self.path);
            }
        }
    }

    async fn seed_files(
        db: &TursoDatabase,
        conn: &Connection,
        storage_id: u64,
        count: u64,
    ) -> Vec<u64> {
        let mut ids = Vec::with_capacity(count as usize);
        let mut pending: Vec<FileInternal> = Vec::with_capacity(SQL_CHUNK_SIZE);
        for i in 0..count {
            pending.push(FileInternal {
                id: None,
                hash: format!("{i:064x}"),
                extension: "jpg".to_string(),
                storage_id,
                size_bytes: Some(1024),
            });
            if pending.len() == SQL_CHUNK_SIZE {
                ids.extend(resolve_file_ids(db, conn, std::mem::take(&mut pending)).await);
            }
        }
        if !pending.is_empty() {
            ids.extend(resolve_file_ids(db, conn, pending).await);
        }
        ids
    }

    async fn resolve_file_ids(
        db: &TursoDatabase,
        conn: &Connection,
        batch: Vec<FileInternal>,
    ) -> Vec<u64> {
        let inserted = db.file_add_bulk(conn, &batch).await.unwrap();
        let mut ids: Vec<u64> = inserted.iter().filter_map(|file| file.id).collect();
        ids.sort_unstable();
        ids
    }

    /// Prints one labelled measurement, with the per-row figure that actually
    /// decides whether a bulk path is batching.
    fn report(label: &str, rows: u64, iterations: u64, elapsed: Duration) {
        let per_row_us = if rows == 0 {
            0.0
        } else {
            elapsed.as_secs_f64() * 1_000_000.0 / (rows * iterations) as f64
        };
        let per_iter_ms = elapsed.as_secs_f64() * 1_000.0 / iterations as f64;
        println!(
            "  {label:<44} {:>9.2} ms/iter  {:>9.2} us/row  ({} rows x {iterations} iters)",
            per_iter_ms, per_row_us, rows
        );
    }

    /// True for the transient "someone else holds the write lock" errors that
    /// `retry_mvcc` retries in production. Used here so the bench's writers
    /// queue the way real ones do instead of panicking on the first collision.
    fn is_busy(error: &turso::Error) -> bool {
        matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
    }

    /// Prints a statement's query plan, so a slow number can be attributed to a
    /// scan instead of guessed at.
    async fn explain(conn: &Connection, sql: &str) {
        let mut rows = match conn.query(&format!("EXPLAIN QUERY PLAN {sql}"), ()).await {
            Ok(rows) => rows,
            Err(error) => {
                println!("    (plan unavailable: {error})");
                return;
            }
        };
        while let Ok(Some(row)) = rows.next().await {
            if let Ok(detail) = row.get::<String>(3) {
                println!("    plan: {detail}");
            }
        }
    }

    /// Read-path benchmarks: point lookup versus the batched and paged forms.
    ///
    /// The point of comparison is that a caller holding *N* ids should issue
    /// one statement, not *N*. A regression back to per-row probing shows up
    /// here as `us/row` climbing by roughly the cost of a round trip.
    #[test]
    #[ignore = "manual benchmark: common read-path db functions"]
    fn db_bench_read_path() {
        let files = env_u64("BENCH_FILES", DEFAULT_FILES);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed(
            "read",
            files,
            env_u64("BENCH_TAGS", DEFAULT_TAGS),
        ));
        println!("\nBENCH read path: {}", fixture.describe());
        let db = fixture.db.clone();
        let conn = fixture.conn.clone();
        let ids = fixture.file_ids();
        let iter_count = env_u64("BENCH_ITERS", 5).max(1);

        rt.block_on(async {
            // A page of ids, one statement's worth.
            let page: Vec<u64> = ids.iter().copied().take(SQL_CHUNK_SIZE).collect();
            println!("  -- point vs batched, {} ids --", page.len());

            // `file_get` in a loop: what the batched forms replaced.
            let t = Instant::now();
            for _ in 0..iter_count {
                for id in &page {
                    let _ = db.file_get(&conn, id).await.unwrap();
                }
            }
            report(
                "file_get x N (one statement per id)",
                page.len() as u64,
                iter_count,
                t.elapsed(),
            );

            let t = Instant::now();
            for _ in 0..iter_count {
                let got = db.file_get_bulk(&conn, &page).await.unwrap();
                assert_eq!(got.len(), page.len());
            }
            report(
                "file_get_bulk (one statement)",
                page.len() as u64,
                iter_count,
                t.elapsed(),
            );

            let t = Instant::now();
            for _ in 0..iter_count {
                let tags = db
                    .file_ids_get_tags(&conn, &page.iter().copied().collect())
                    .await
                    .unwrap();
                assert_eq!(tags.len(), page.len());
            }
            report(
                "file_ids_get_tags (one union query)",
                page.len() as u64,
                iter_count,
                t.elapsed(),
            );

            // Keyset walk of the whole table.
            println!("  -- full-table traversal --");
            let t = Instant::now();
            let mut after = 0u64;
            let mut walked = 0u64;
            let mut pages = 0u64;
            loop {
                let page = db
                    .file_get_page(&conn, after, SQL_CHUNK_SIZE)
                    .await
                    .unwrap();
                if page.is_empty() {
                    break;
                }
                after = page.last().and_then(|file| file.id).unwrap_or(after);
                walked += page.len() as u64;
                pages += 1;
            }
            let walked_in_one_pass = walked;
            report("file_get_page walk (whole table)", walked, 1, t.elapsed());
            println!(
                "    {pages} pages of {SQL_CHUNK_SIZE}; {} rows",
                walked_in_one_pass
            );

            // The unbounded alternative the pager exists to avoid, for scale.
            let t = Instant::now();
            let all = db.file_id_get_all(&conn).await.unwrap();
            let elapsed = t.elapsed();
            println!(
                "  {:<44} {:>9.2} ms       (UNBOUNDED: {:?} in memory)",
                "file_id_get_all (whole table, one result)",
                elapsed.as_secs_f64() * 1_000.0,
                std::mem::size_of_val(&all) as u64
            );
            assert_eq!(all.len(), walked_in_one_pass as usize);
            drop(all);
        });
    }

    /// Write-path benchmarks, split by what the write actually costs.
    ///
    /// Three distinct costs matter and they are easy to conflate:
    ///
    /// * *novel* — inserting rows that do not exist yet (the expensive case).
    /// * *re-add* — every hash/tag already present. This must be read-only;
    ///   it is the case a re-scrape hits on every run.
    /// * *batched update* — rewriting columns of existing rows.
    #[test]
    #[ignore = "manual benchmark: common write-path db functions"]
    fn db_bench_write_path() {
        let files = env_u64("BENCH_FILES", DEFAULT_FILES);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed(
            "write",
            files,
            env_u64("BENCH_TAGS", DEFAULT_TAGS),
        ));
        println!("\nBENCH write path: {}", fixture.describe());
        let db = fixture.db.clone();
        let conn = fixture.conn.clone();
        let storage_id = fixture.storage_id;
        let iter_count = env_u64("BENCH_ITERS", 5).max(1);

        // A fresh, non-colliding hash space so the novel case really inserts.
        let novel_files = |offset: u64| -> Vec<FileInternal> {
            (0..SQL_CHUNK_SIZE as u64)
                .map(|i| FileInternal {
                    id: None,
                    hash: format!("{:064x}", offset + i),
                    extension: "jpg".to_string(),
                    storage_id,
                    size_bytes: Some(2048),
                })
                .collect()
        };

        let first = novel_files(u64::MAX / 2);
        // The same hashes again: now they exist, so this is the re-add case.
        let second = novel_files(u64::MAX / 2);

        rt.block_on(async {
            println!("  -- file_add_bulk, {} rows per call --", first.len());
            // "Novel" can only be novel once: after the first call every hash in
            // the batch exists, so further iterations would measure the re-add
            // path under the wrong label. Timed as a single call.
            let t = Instant::now();
            let inserted = db.file_add_bulk(&conn, &first).await.unwrap();
            let novel_elapsed = t.elapsed();
            assert_eq!(inserted.len(), first.len());
            report(
                "file_add_bulk NOVEL (single call)",
                first.len() as u64,
                1,
                novel_elapsed,
            );

            let t = Instant::now();
            for _ in 0..iter_count {
                let resolved = db.file_add_bulk(&conn, &second).await.unwrap();
                assert_eq!(resolved.len(), second.len());
            }
            report(
                "file_add_bulk RE-ADD (resolve only)",
                second.len() as u64,
                iter_count,
                t.elapsed(),
            );

            // A re-add that also reports different metadata must not rewrite
            // anything; if this gets slower, the resolve path started writing.
            let mut re_scrape = second.clone();
            for file in &mut re_scrape {
                file.size_bytes = Some(999_999);
            }
            let t = Instant::now();
            for _ in 0..iter_count {
                let _ = db.file_add_bulk(&conn, &re_scrape).await.unwrap();
            }
            report(
                "file_add_bulk RE-ADD (changed metadata)",
                re_scrape.len() as u64,
                iter_count,
                t.elapsed(),
            );

            // Batched updates over rows that already exist.
            let ids: Vec<u64> = db
                .file_id_get_all(&conn)
                .await
                .unwrap()
                .into_iter()
                .take(SQL_CHUNK_SIZE)
                .collect();
            let updates: Vec<(u64, u64)> = ids
                .iter()
                .enumerate()
                .map(|(index, id)| (*id, index as u64 * 7 + 1))
                .collect();

            println!("  -- batched updates, {} rows per call --", updates.len());
            // `file_sizes_update` only fills NULL sizes, so each timed round
            // has to start from NULL rows. The reset is a single batched
            // statement and stays *outside* the timer: nulling row-by-row would
            // be SQL_CHUNK_SIZE statements and would swamp the measurement,
            // which is exactly how an earlier draft of this benchmark reported
            // a nonsense figure for this function.
            let reset_sql = format!(
                "UPDATE File SET size_bytes = NULL WHERE id IN ({});",
                std::iter::repeat_n("?", ids.len())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let reset_params: Vec<turso::Value> = ids
                .iter()
                .map(|id| turso::Value::from(*id as i64))
                .collect();

            let t = Instant::now();
            conn.execute(&reset_sql, turso::params_from_iter(reset_params.clone()))
                .await
                .unwrap();
            let reset_elapsed = t.elapsed();
            println!(
                "    (reset statement, untimed: {:.2} ms for {} rows)",
                reset_elapsed.as_secs_f64() * 1_000.0,
                ids.len()
            );

            let mut sizes_total = Duration::ZERO;
            for _ in 0..iter_count {
                conn.execute(&reset_sql, turso::params_from_iter(reset_params.clone()))
                    .await
                    .unwrap();
                let t = Instant::now();
                db.file_sizes_update(&conn, &updates).await.unwrap();
                sizes_total += t.elapsed();
            }
            report(
                "file_sizes_update (batched CASE)",
                updates.len() as u64,
                iter_count,
                sizes_total,
            );

            // Hashes must not collide with the seeded fixture, or the plain
            // UPDATE (no OR IGNORE) aborts on the UNIQUE constraint.
            let rename: Vec<FileInternal> = ids
                .iter()
                .enumerate()
                .map(|(index, id)| FileInternal {
                    id: Some(*id),
                    hash: format!("renamed-{index:064x}"),
                    extension: "png".to_string(),
                    storage_id,
                    size_bytes: None,
                })
                .collect();
            let t = Instant::now();
            for _ in 0..iter_count {
                db.file_update_batch(&conn, &rename).await.unwrap();
            }
            report(
                "file_update_batch (3 columns)",
                rename.len() as u64,
                iter_count,
                t.elapsed(),
            );
        });
    }

    /// Tag-resolution benchmark, plus the query plan for the resolve statement.
    ///
    /// `tag_add_bulk` resolves existing tags before inserting, so its re-add
    /// cost *is* the tag resolve cost. That path is what the scraper hits for
    /// every tag on every run.
    #[test]
    #[ignore = "manual benchmark: tag resolution and relationship writes"]
    fn db_bench_tag_and_relationship_path() {
        let files = env_u64("BENCH_FILES", DEFAULT_FILES);
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed("tags", files, tags));
        println!("\nBENCH tag + relationship path: {}", fixture.describe());
        let db = fixture.db.clone();
        let conn = fixture.conn.clone();
        let iter_count = env_u64("BENCH_ITERS", 5).max(1);

        // A batch of tags that already exist: the re-scrape case.
        let existing: HashSet<Tag> = (0..SQL_CHUNK_SIZE as u64)
            .map(|i| Tag {
                name: format!("bench-tag-{i}"),
                namespace: GenericNamespaceObj {
                    name: "source_url".to_string(),
                    description: None,
                },
            })
            .collect();

        rt.block_on(async {
            println!("  -- tag_add_bulk, {} tags per call --", existing.len());
            let t = Instant::now();
            for _ in 0..iter_count {
                let resolved = db.tag_add_bulk(&conn, &existing).await.unwrap();
                assert_eq!(resolved.len(), existing.len());
            }
            report(
                "tag_add_bulk RE-ADD (resolve only)",
                existing.len() as u64,
                iter_count,
                t.elapsed(),
            );

            let t = Instant::now();
            for _ in 0..iter_count {
                let tags_by_file = db
                    .file_ids_get_tags(&conn, &fixture.rels.iter().map(|(f, _)| *f).take(SQL_CHUNK_SIZE).collect())
                    .await
                    .unwrap();
                assert!(!tags_by_file.is_empty());
            }
            report(
                "file_ids_get_tags (per file)",
                SQL_CHUNK_SIZE as u64,
                iter_count,
                t.elapsed(),
            );

            // The statement `tags_resolve_existing` issues, for the plan.
            println!("  -- query plans --");
            explain(
                &conn,
                &format!(
                    "SELECT id, name FROM Tags WHERE namespace = 1 AND name IN ({})",
                    std::iter::repeat_n("?", 8).collect::<Vec<_>>().join(", ")
                ),
            )
            .await;
            explain(
                &conn,
                "SELECT f.id, fh.digest FROM FileHashes fh JOIN File f ON f.id = fh.file_id WHERE fh.algorithm = 'SHA512' AND fh.digest = ?",
            )
            .await;
            explain(&conn, "SELECT f.id, fh.digest, f.extension, f.storage_id, f.size_bytes FROM File f LEFT JOIN FileHashes fh ON fh.file_id = f.id AND fh.algorithm = 'SHA512' WHERE f.id > 0 ORDER BY f.id LIMIT 100").await;

            // Relationship bulk add: idempotent re-add of an existing set, which
            // is what a re-scrape does, versus a genuinely new set.
            let ids: Vec<u64> = fixture.file_ids();
            let page: Vec<u64> = ids.iter().copied().take(SQL_CHUNK_SIZE).collect();
            let existing_rels: HashSet<(u64, u64)> = page
                .iter()
                .map(|file_id| (*file_id, fixture.source_url_tag_id))
                .collect();

            println!("  -- relationships_bulk_add, {} rows --", existing_rels.len());
            let t = Instant::now();
            for _ in 0..iter_count {
                let deltas = db.relationships_bulk_add(&conn, &existing_rels).await.unwrap();
                assert!(deltas.is_empty(), "re-adding must not bump counts");
            }
            report(
                "relationships_bulk_add RE-ADD",
                existing_rels.len() as u64,
                iter_count,
                t.elapsed(),
            );

            // A second tag on the same files, which is novel exactly once.
            let second_tag_id = fixture.source_url_tag_id + 1;
            let new_rels: HashSet<(u64, u64)> = page
                .iter()
                .map(|file_id| (*file_id, second_tag_id))
                .collect();
            let t = Instant::now();
            let deltas = db.relationships_bulk_add(&conn, &new_rels).await.unwrap();
            report(
                "relationships_bulk_add NOVEL (single call)",
                new_rels.len() as u64,
                1,
                t.elapsed(),
            );
            assert_eq!(
                deltas.len(),
                1,
                "only the newly attached tag should report a delta"
            );
        });
    }

    /// Compares the three shapes a row-update can take at the same row count.
    ///
    /// This exists because "one statement per chunk" is not automatically
    /// faster than "one statement per row": a `CASE` arm list is an expression
    /// the planner must evaluate, and at `SQL_CHUNK_SIZE` arms that can cost
    /// more than the round trips it saves. The per-row form is included as the
    /// baseline the batched form has to beat.
    #[test]
    #[ignore = "manual benchmark: per-row vs batched update shapes"]
    fn db_bench_update_shapes() {
        let files = env_u64("BENCH_FILES", DEFAULT_FILES);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed(
            "shapes",
            files,
            env_u64("BENCH_TAGS", DEFAULT_TAGS),
        ));
        println!("\nBENCH update shapes: {}", fixture.describe());
        let db = fixture.db.clone();
        let conn = fixture.conn.clone();

        let rows = env_u64("BENCH_SHAPE_ROWS", SQL_CHUNK_SIZE as u64).min(fixture.files);
        let rounds = env_u64("BENCH_ITERS", 3).max(1);

        rt.block_on(async {
            let ids: Vec<u64> = db
                .file_id_get_all(&conn)
                .await
                .unwrap()
                .into_iter()
                .take(rows as usize)
                .collect();
            let updates: Vec<(u64, u64)> = ids
                .iter()
                .enumerate()
                .map(|(index, id)| (*id, index as u64 * 7 + 1))
                .collect();
            println!("  -- {rows} rows, {rounds} rounds --");

            // Reset to NULL so every shape does the same amount of real work.
            let reset_sql = format!(
                "UPDATE File SET size_bytes = NULL WHERE id IN ({});",
                std::iter::repeat_n("?", ids.len())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let reset_params: Vec<turso::Value> = ids
                .iter()
                .map(|id| turso::Value::from(*id as i64))
                .collect();
            let reset = || async {
                conn.execute(&reset_sql, turso::params_from_iter(reset_params.clone()))
                    .await
                    .unwrap();
            };

            // Shape 1: one statement per row. The original `file_sizes_update`.
            let mut per_row = Duration::ZERO;
            for _ in 0..rounds {
                reset().await;
                let t = Instant::now();
                for (file_id, size) in &updates {
                    conn.execute(
                        "UPDATE File SET size_bytes = ?1 WHERE id = ?2 AND size_bytes IS NULL;",
                        (*size as i64, *file_id as i64),
                    )
                    .await
                    .unwrap();
                }
                per_row += t.elapsed();
            }
            report("A: per-row UPDATE (1 stmt/row)", rows, rounds, per_row);

            // Shape 2: the batched CASE form currently used by
            // `file_sizes_update`.
            let mut batched_case = Duration::ZERO;
            for _ in 0..rounds {
                reset().await;
                let t = Instant::now();
                db.file_sizes_update(&conn, &updates).await.unwrap();
                batched_case += t.elapsed();
            }
            report("B: batched CASE (1 stmt/chunk)", rows, rounds, batched_case);

            // Shape 3: plain batched `IN` with a single constant value. Not
            // usable for the real function (each row has its own size) but it
            // is the floor: what the `IN` list costs with no per-row expression.
            let mut batched_plain = Duration::ZERO;
            for _ in 0..rounds {
                reset().await;
                let t = Instant::now();
                conn.execute(
                    &format!(
                        "UPDATE File SET size_bytes = 1 WHERE id IN ({})",
                        std::iter::repeat_n("?", ids.len())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    turso::params_from_iter(reset_params.clone()),
                )
                .await
                .unwrap();
                batched_plain += t.elapsed();
            }
            report(
                "C: batched plain IN (1 stmt/chunk)",
                rows,
                rounds,
                batched_plain,
            );

            println!(
                "    B/A = {:.2}x   C/A = {:.2}x   (below 1.00 means batching won)",
                batched_case.as_secs_f64() / per_row.as_secs_f64().max(f64::MIN_POSITIVE),
                batched_plain.as_secs_f64() / per_row.as_secs_f64().max(f64::MIN_POSITIVE),
            );
        });
    }

    /// Parents insert path, split into statement building and statement execution.
    ///
    /// The two are measured separately on purpose: `parents_insert_sql` renders a
    /// placeholder list one row at a time, and at `SQL_CHUNK_SIZE` that is pure CPU
    /// competing with the insert itself. If the builder ever becomes a meaningful
    /// share of the call, it shows up here rather than as an unexplained wobble in
    /// ingest throughput.
    #[test]
    #[ignore = "manual benchmark: Parents insert path (build vs execute)"]
    fn db_bench_parents() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed("parents", 1_000, tags));
        println!("\nBENCH parents insert: {}", fixture.describe());
        let db = fixture.db.clone();
        let conn = fixture.conn.clone();
        let rounds = env_u64("BENCH_ITERS", 5).max(1);

        // A parent edge is (tag -> related tag), optionally scoped by limit_to.
        // Tag ids start at 1; the fixture seeded `tags` of them, so edges stay
        // inside that range and the foreign keys hold. `BENCH_PARENT_EDGES`
        // scales the batch past one chunk, which is where a b-tree locality
        // effect would start to show.
        let edges = env_u64("BENCH_PARENT_EDGES", SQL_CHUNK_SIZE as u64);
        let tag_count = tags;
        let parents: Vec<TagParents> = (0..edges)
            .map(|i| TagParents {
                tag_id: 1 + (i % tag_count),
                relate_tag_id: 1 + ((i / 3 + 7) % tag_count),
                // A third of the rows exercise the NULL branch of the
                // `IFNULL(limit_to, -1)` index.
                limit_to: if i.is_multiple_of(3) {
                    Some(1 + ((i + 11) % tag_count))
                } else {
                    None
                },
            })
            .collect();
        // Dedupe the way callers do, through a HashSet keyed on the whole edge.
        let unique: HashSet<TagParents> = parents.iter().cloned().collect();
        let parents: Vec<TagParents> = unique.into_iter().collect();
        println!(
            "  {} distinct edges from {} generated ({} collides)",
            parents.len(),
            edges,
            edges - parents.len() as u64
        );

        let ordered: Vec<&TagParents> = {
            let mut rows: Vec<&TagParents> = parents.iter().collect();
            rows.sort_unstable_by_key(|parent| {
                (
                    parent.tag_id,
                    parent.relate_tag_id,
                    parent.limit_to.map(|id| id as i64).unwrap_or(-1),
                )
            });
            rows
        };

        rt.block_on(async {
            // --- statement building, no database ---
            let t = Instant::now();
            for _ in 0..rounds {
                let sql = parents_insert_sql(ordered.len());
                std::hint::black_box(&sql);
            }
            report(
                "parents_insert_sql (build only)",
                ordered.len() as u64,
                rounds,
                t.elapsed(),
            );

            let t = Instant::now();
            for _ in 0..rounds {
                let params = parents_params(&ordered);
                std::hint::black_box(&params);
            }
            report(
                "parents_params (build only)",
                ordered.len() as u64,
                rounds,
                t.elapsed(),
            );

            // The sort `parents_bulk_add` does internally, isolated.
            let t = Instant::now();
            for _ in 0..rounds {
                let mut rows: Vec<&TagParents> = parents.iter().collect();
                rows.sort_unstable_by_key(|parent| {
                    (
                        parent.tag_id,
                        parent.relate_tag_id,
                        parent.limit_to.map(|id| id as i64).unwrap_or(-1),
                    )
                });
                std::hint::black_box(&rows);
            }
            report(
                "parents_bulk_add sort (only)",
                parents.len() as u64,
                rounds,
                t.elapsed(),
            );

            // --- execution ---
            // The first call inserts and later calls re-add, so a timed loop
            // over `parents_bulk_add` averages a novel insert into a "re-add"
            // figure. Time the novel call on its own, then the re-adds.
            let t = Instant::now();
            db.parents_bulk_add(&conn, &parents).await.unwrap();
            report(
                "parents_bulk_add NOVEL (single call)",
                parents.len() as u64,
                1,
                t.elapsed(),
            );

            let t = Instant::now();
            for _ in 0..rounds {
                db.parents_bulk_add(&conn, &parents).await.unwrap();
            }
            report(
                "parents_bulk_add RE-ADD (full path)",
                parents.len() as u64,
                rounds,
                t.elapsed(),
            );

            // Feed the already-sorted slice so the builder work is identical and
            // only the insert order differs. `parents_bulk_add` re-sorts, so this
            // is measured through the raw statement to isolate the order effect.
            let sql = parents_insert_sql(ordered.len());
            let t = Instant::now();
            for _ in 0..rounds {
                conn.execute(
                    sql.as_str(),
                    turso::params_from_iter(parents_params(&ordered)),
                )
                .await
                .unwrap();
            }
            report(
                "INSERT OR IGNORE sorted (direct)",
                parents.len() as u64,
                rounds,
                t.elapsed(),
            );

            let reversed: Vec<&TagParents> = ordered.iter().rev().copied().collect();
            let sql_rev = parents_insert_sql(reversed.len());
            let t = Instant::now();
            for _ in 0..rounds {
                conn.execute(
                    sql_rev.as_str(),
                    turso::params_from_iter(parents_params(&reversed)),
                )
                .await
                .unwrap();
            }
            report(
                "INSERT OR IGNORE reverse-sorted (direct)",
                parents.len() as u64,
                rounds,
                t.elapsed(),
            );

            println!("  -- query plan for the bulk insert --");
            let sql_first = format!(
                "INSERT OR IGNORE INTO Parents (tag_id, relate_tag_id, limit_to) VALUES {}",
                ["(1, 1, 1)"; 4].join(", ")
            );
            explain(&conn, &sql_first).await;
            explain(
                &conn,
                "SELECT 1 FROM Parents WHERE tag_id = 1 AND relate_tag_id = 1 \
             AND IFNULL(limit_to, -1) = 1",
            )
            .await;

            // --- novel insert order ---
            // The re-add comparison above cannot show a b-tree locality effect:
            // on a re-add every row conflicts, so `OR IGNORE` probes and skips
            // and nothing is inserted. The claim in `parents_bulk_add`'s comment
            // is about *inserting* rows, so that is what has to be measured.
            // Two disjoint edge sets of equal size: one fed in index order, one
            // fed in reverse, both genuinely novel.
            println!("  -- novel insert: index order vs reverse --");
            let half = env_u64("BENCH_ORDER_ROWS", 9_200);
            let mut set_a: Vec<TagParents> = Vec::new();
            let mut set_b: Vec<TagParents> = Vec::new();
            for i in 0..half {
                set_a.push(TagParents {
                    tag_id: 1 + (i % tags),
                    relate_tag_id: 1 + ((i / 7 + 3) % tags),
                    limit_to: None,
                });
                // Disjoint: shifted into a different band of tag ids.
                set_b.push(TagParents {
                    tag_id: 1 + (i % tags) + tags / 2,
                    relate_tag_id: 1 + ((i / 11 + 5) % tags) + tags / 2,
                    limit_to: None,
                });
            }
            let sort_key = |parent: &TagParents| {
                (
                    parent.tag_id,
                    parent.relate_tag_id,
                    parent.limit_to.map(|id| id as i64).unwrap_or(-1),
                )
            };
            let mut a: Vec<&TagParents> = set_a.iter().collect();
            a.sort_unstable_by_key(|p| sort_key(p));
            let mut b: Vec<&TagParents> = set_b.iter().collect();
            b.sort_unstable_by_key(|p| sort_key(p));
            let b_rev: Vec<&TagParents> = b.iter().rev().copied().collect();

            let sql_a = parents_insert_sql(a.len());
            let t = Instant::now();
            for chunk in a.chunks(SQL_CHUNK_SIZE) {
                conn.execute(&sql_a, turso::params_from_iter(parents_params(chunk)))
                    .await
                    .unwrap();
            }
            let sorted_novel = t.elapsed();
            report("NOVEL, index order", a.len() as u64, 1, sorted_novel);

            let sql_b = parents_insert_sql(b.len());
            let t = Instant::now();
            for chunk in b_rev.chunks(SQL_CHUNK_SIZE) {
                conn.execute(&sql_b, turso::params_from_iter(parents_params(chunk)))
                    .await
                    .unwrap();
            }
            let reverse_novel = t.elapsed();
            report("NOVEL, reverse order", b_rev.len() as u64, 1, reverse_novel);

            println!(
                "    reverse/sorted = {:.2}x  (flips sign between runs; one reading proves nothing)",
                reverse_novel.as_secs_f64() / sorted_novel.as_secs_f64().max(f64::MIN_POSITIVE)
            );
        });
    }

    /// Probes which alternative `Parents` shapes this turso build actually
    /// accepts, so a schema change is designed against reality rather than
    /// against SQLite's documented feature set.
    #[test]
    #[ignore = "manual benchmark: which Parents schema variants does turso accept?"]
    fn db_bench_parents_schema_probe() {
        let path = bench_db_path("probe");
        bench_remove(&path);
        let should_exit = Arc::new(AtomicBool::new(false));
        let rt = tokio::runtime::Runtime::new().unwrap();
        let db = rt.block_on(TursoDatabase::new_with_exit(&path, should_exit));

        rt.block_on(async {
            let conn = db.connect().unwrap();

            println!("\nBENCH Parents schema probe");

            // Is FK enforcement on? Every declared FK is only enforced when this is
            // set, and the turso backend never sets it.
            let fk: i64 = conn
                .query("PRAGMA foreign_keys;", ())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap()
                .get(0)
                .unwrap();
            println!("  PRAGMA foreign_keys = {fk}  (0 = declared FKs are NOT enforced)");

            let variants: [(&str, &str); 4] = [
                (
                    "A current: rowid + expr-unique + rel",
                    "CREATE TABLE p_a (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                 relate_tag_id INTEGER NOT NULL, limit_to INTEGER, \
                 CHECK (tag_id != relate_tag_id)); \
                 CREATE UNIQUE INDEX p_a_u ON p_a (tag_id, relate_tag_id, IFNULL(limit_to, -1)); \
                 CREATE INDEX p_a_r ON p_a (relate_tag_id);",
                ),
                (
                    "B rowid + plain-unique (no IFNULL) + rel",
                    "CREATE TABLE p_b (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                 relate_tag_id INTEGER NOT NULL, limit_to INTEGER NOT NULL DEFAULT -1, \
                 CHECK (tag_id != relate_tag_id)); \
                 CREATE UNIQUE INDEX p_b_u ON p_b (tag_id, relate_tag_id, limit_to); \
                 CREATE INDEX p_b_r ON p_b (relate_tag_id);",
                ),
                (
                    "C rowid + expr-unique only (no rel index)",
                    "CREATE TABLE p_c (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                 relate_tag_id INTEGER NOT NULL, limit_to INTEGER, \
                 CHECK (tag_id != relate_tag_id)); \
                 CREATE UNIQUE INDEX p_c_u ON p_c (tag_id, relate_tag_id, IFNULL(limit_to, -1));",
                ),
                (
                    "D WITHOUT ROWID, plain-unique + rel",
                    "CREATE TABLE p_d (tag_id INTEGER NOT NULL, relate_tag_id INTEGER NOT NULL, \
                 limit_to INTEGER NOT NULL DEFAULT -1, \
                 PRIMARY KEY (tag_id, relate_tag_id, limit_to), \
                 CHECK (tag_id != relate_tag_id)) WITHOUT ROWID; \
                 CREATE INDEX p_d_r ON p_d (relate_tag_id);",
                ),
            ];

            for (label, ddl) in variants {
                let table = label.split_whitespace().next().unwrap();
                let _ = conn
                    .execute_batch(&format!("DROP TABLE IF EXISTS {table};"))
                    .await;
                match conn.execute_batch(ddl).await {
                    Ok(()) => println!("  ACCEPTED  {label}"),
                    Err(error) => println!("  REJECTED  {label}\n              -> {error}"),
                }
            }

            // Drop B's expression entirely and confirm the NULL-sentinel semantics
            // the current index exists to provide: two rows differing only by
            // limit_to NULL vs -1.
            let _ = conn.execute_batch("DROP TABLE IF EXISTS p_b;").await;
            conn.execute_batch(
                "CREATE TABLE p_b (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER NOT NULL DEFAULT -1); \
             CREATE UNIQUE INDEX p_b_u ON p_b (tag_id, relate_tag_id, limit_to);",
            )
            .await
            .unwrap();
            let first = conn
                .execute(
                    "INSERT INTO p_b (tag_id, relate_tag_id, limit_to) VALUES (1, 2, -1);",
                    (),
                )
                .await;
            let second = conn
                .execute(
                    "INSERT INTO p_b (tag_id, relate_tag_id, limit_to) VALUES (1, 2, -1);",
                    (),
                )
                .await;
            println!(
                "\n  sentinel dedupe under a plain unique index: first={:?} second={:?}",
                first,
                second.map_err(|e| e.to_string())
            );
            println!("  (second must fail for the -1 sentinel to be safe as a unique key)");

            let _ = conn
                .execute_batch(
                    "DROP TABLE IF EXISTS p_a; DROP TABLE IF EXISTS p_b; \
                                   DROP TABLE IF EXISTS p_c; DROP TABLE IF EXISTS p_d;",
                )
                .await;
        });

        if std::env::var("BENCH_KEEP").is_err() && std::env::var("BENCH_PATH").is_err() {
            bench_remove(&path);
        }
    }

    /// Compares candidate `Parents` schemas on the same insert workload.
    ///
    /// The current shape keeps three b-trees alive per insert: the rowid table,
    /// `idx_parents_rel`, and `idx_unique_parents_null_safe`, whose key is
    /// `(tag_id, relate_tag_id, IFNULL(limit_to, -1))`. That last one is an
    /// *expression* index, so every row pays to evaluate `IFNULL` and to store a
    /// computed key. Storing `-1` instead of `NULL` would make it a plain
    /// composite index.
    ///
    /// Whether that is worth a schema change is an empirical question, so this
    /// measures it rather than assuming. Each variant runs in its own database
    /// — measuring them in sequence on one connection produced a monotonically
    /// increasing series driven by MVCC journal growth, not by the schema.
    ///
    /// No Tags rows are needed: the variant tables carry no foreign keys and the
    /// backend runs with `PRAGMA foreign_keys = 0`, so the ids need not resolve.
    #[test]
    #[ignore = "manual benchmark: Parents schema variants on novel inserts"]
    fn db_bench_parents_schema_variants() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let edges = env_u64("BENCH_PARENT_EDGES", SQL_CHUNK_SIZE as u64);
        let rounds = env_u64("BENCH_ITERS", 3).max(1);
        println!(
            "\nBENCH Parents schema variants: {edges} edges, fresh db each, \
             ids drawn from 1..{tags}"
        );

        // `limit_to` is NULL for two rows in three, matching what the scraper emits.
        let edges_for = |table_seed: u64| -> Vec<TagParents> {
            (0..edges)
                .map(|i| TagParents {
                    tag_id: 1 + (i % tags),
                    relate_tag_id: 1 + ((i / 3 + 7 + table_seed) % tags),
                    limit_to: if i.is_multiple_of(3) {
                        Some(1 + ((i + 11) % tags))
                    } else {
                        None
                    },
                })
                .collect()
        };

        struct Variant {
            label: &'static str,
            ddl: &'static str,
            /// Rewrites an edge's `limit_to` to the schema's sentinel form.
            adapt: fn(Option<u64>) -> Option<u64>,
        }

        let variants = [
            Variant {
                label: "A current (expr unique + rel)",
                ddl: "CREATE TABLE pvar_a (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                  relate_tag_id INTEGER NOT NULL, limit_to INTEGER, \
                  CHECK (tag_id != relate_tag_id)); \
                  CREATE UNIQUE INDEX pvar_a_u ON pvar_a (tag_id, relate_tag_id, IFNULL(limit_to, -1)); \
                  CREATE INDEX pvar_a_r ON pvar_a (relate_tag_id);",
                adapt: |limit_to| limit_to,
            },
            Variant {
                label: "B plain unique (-1 sentinel) + rel",
                ddl: "CREATE TABLE pvar_b (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                  relate_tag_id INTEGER NOT NULL, limit_to INTEGER NOT NULL DEFAULT -1, \
                  CHECK (tag_id != relate_tag_id)); \
                  CREATE UNIQUE INDEX pvar_b_u ON pvar_b (tag_id, relate_tag_id, limit_to); \
                  CREATE INDEX pvar_b_r ON pvar_b (relate_tag_id);",
                adapt: |limit_to| Some(limit_to.unwrap_or(u64::MAX)),
            },
            Variant {
                label: "C expr unique, no rel index",
                ddl: "CREATE TABLE pvar_c (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                  relate_tag_id INTEGER NOT NULL, limit_to INTEGER, \
                  CHECK (tag_id != relate_tag_id)); \
                  CREATE UNIQUE INDEX pvar_c_u ON pvar_c (tag_id, relate_tag_id, IFNULL(limit_to, -1));",
                adapt: |limit_to| limit_to,
            },
            Variant {
                label: "D plain unique only (1 index)",
                ddl: "CREATE TABLE pvar_d (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                  relate_tag_id INTEGER NOT NULL, limit_to INTEGER NOT NULL DEFAULT -1, \
                  CHECK (tag_id != relate_tag_id)); \
                  CREATE UNIQUE INDEX pvar_d_u ON pvar_d (tag_id, relate_tag_id, limit_to);",
                adapt: |limit_to| Some(limit_to.unwrap_or(u64::MAX)),
            },
            Variant {
                label: "E no indexes (floor)",
                ddl: "CREATE TABLE pvar_e (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                  relate_tag_id INTEGER NOT NULL, limit_to INTEGER);",
                adapt: |limit_to| limit_to,
            },
        ];

        rt.block_on(async {
            // Each variant gets its OWN database. Measuring them in sequence on
            // one connection produced a monotonically increasing series in both
            // the novel and the re-add column regardless of index count — that
            // is MVCC journal growth across the run, not a schema effect, and
            // it made "no indexes at all" look like the slowest variant. A
            // fresh database per variant is the only way to isolate the schema.
            //
            // No Tags rows are seeded: the variant tables deliberately carry no
            // foreign keys, and the backend runs with `PRAGMA foreign_keys = 0`
            // anyway, so the edge ids need not resolve.
            println!("  -- NOVEL insert, {edges} rows, fresh db per variant --");
            let mut results: Vec<(&'static str, f64)> = Vec::new();
            let chunk_params = SQL_CHUNK_SIZE * 3;

            for (index, variant) in variants.iter().enumerate() {
                let variant_path = bench_db_path(&format!("pvar{index}"));
                bench_remove(&variant_path);
                let variant_db =
                    TursoDatabase::new_with_exit(&variant_path, Arc::new(AtomicBool::new(false)))
                        .await;
                let vconn = variant_db.connect().unwrap();
                vconn.execute_batch(variant.ddl).await.unwrap();

                let edges_in = edges_for(index as u64 * 13);
                let sql = parents_insert_sql(edges_in.len());
                let params: Vec<turso::Value> = edges_in
                    .iter()
                    .flat_map(|parent| {
                        [
                            turso::Value::from(parent.tag_id as i64),
                            turso::Value::from(parent.relate_tag_id as i64),
                            match (variant.adapt)(parent.limit_to) {
                                Some(id) => turso::Value::from(id as i64),
                                None => turso::Value::Null,
                            },
                        ]
                    })
                    .collect();

                let t = Instant::now();
                for chunk in params.chunks(chunk_params) {
                    vconn
                        .execute(&sql, turso::params_from_iter(chunk.to_vec()))
                        .await
                        .unwrap();
                }
                let novel = t.elapsed();

                let t = Instant::now();
                for _ in 0..rounds {
                    for chunk in params.chunks(chunk_params) {
                        vconn
                            .execute(&sql, turso::params_from_iter(chunk.to_vec()))
                            .await
                            .unwrap();
                    }
                }
                let readd = t.elapsed();

                let novel_us = novel.as_secs_f64() * 1_000_000.0 / edges_in.len() as f64;
                let readd_us =
                    readd.as_secs_f64() * 1_000_000.0 / (edges_in.len() * rounds as usize) as f64;
                println!(
                    "  {:<34} novel {:>8.2} us/row   re-add {:>8.2} us/row",
                    variant.label, novel_us, readd_us
                );
                results.push((variant.label, novel_us));

                variant_db.shutdown().await;
                drop(variant_db);
                bench_remove(&variant_path);
            }

            // Everything is reported against A, the schema in production.
            let baseline = results
                .iter()
                .find(|(label, _)| label.starts_with('A'))
                .map(|(_, us)| *us)
                .unwrap();
            println!("\n  novel-insert cost relative to the current schema:");
            for (label, us) in &results {
                println!("    {:<34} {:.2}x", label, us / baseline);
            }
            println!(
                "\n  NOTE: C and D drop idx_parents_rel, which serves the reverse\n\
             direction (WHERE relate_tag_id = ?). They are not droppable in\n\
             production; they are here to bound how much that index costs.\n\
             E is a floor, not a candidate: it has no uniqueness at all."
            );
        });
    }

    /// Sweeps the number of rows per `VALUES` statement for the same edge batch.
    ///
    /// The schema-variant benchmark showed index count is irrelevant here, so the
    /// remaining suspect is the *statement itself*. A 4600-row `VALUES` list binds
    /// 13800 parameters, and limbo's planner cost grows with statement size — the
    /// same effect showed up on the read side, where a keyset page walk cost
    /// 13 us/row while an `IN (...)` list of the same row count cost 222 us/row.
    ///
    /// If total time falls as the statement shrinks, the lever is the chunk size,
    /// not the schema.
    #[test]
    #[ignore = "manual benchmark: Parents insert vs rows-per-statement"]
    fn db_bench_parents_chunk_sweep() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let edges = env_u64("BENCH_PARENT_EDGES", SQL_CHUNK_SIZE as u64);
        let rt = tokio::runtime::Runtime::new().unwrap();

        println!("\nBENCH Parents chunk sweep: {edges} edges total, fresh db per size");

        let make_edges = || -> Vec<TagParents> {
            (0..edges)
                .map(|i| TagParents {
                    tag_id: 1 + (i % tags),
                    relate_tag_id: 1 + ((i / 3 + 7) % tags),
                    limit_to: if i.is_multiple_of(3) {
                        Some(1 + ((i + 11) % tags))
                    } else {
                        None
                    },
                })
                .collect()
        };

        rt.block_on(async {
            println!(
                "  {:>7}  {:>11}  {:>11}  {:>11}  {:>9}",
                "rows", "novel ms", "us/row", "statements", "vs 4600"
            );
            let mut measured: Vec<(u64, f64, u64)> = Vec::new();

            for chunk_rows in [256u64, 512, 1_024, 2_048, 4_600, 9_200] {
                if chunk_rows > edges {
                    continue;
                }
                let path = bench_db_path(&format!("chunk{chunk_rows}"));
                bench_remove(&path);
                let db =
                    TursoDatabase::new_with_exit(&path, Arc::new(AtomicBool::new(false))).await;
                let conn = db.connect().unwrap();
                // No DDL here on purpose: `create_db` already built the
                // production `Parents` schema (rowid table + idx_parents_rel +
                // the IFNULL expression unique), so this measures the real
                // thing rather than a reconstruction of it.

                let batch = make_edges();
                let t = Instant::now();
                let mut statements = 0u64;
                for window in batch.chunks(chunk_rows as usize) {
                    let refs: Vec<&TagParents> = window.iter().collect();
                    conn.execute(
                        &parents_insert_sql(refs.len()),
                        turso::params_from_iter(parents_params(&refs)),
                    )
                    .await
                    .unwrap();
                    statements += 1;
                }
                measured.push((chunk_rows, t.elapsed().as_secs_f64() * 1_000.0, statements));

                db.shutdown().await;
                drop(db);
                bench_remove(&path);
            }

            // Printed only once every size has run, so the baseline exists.
            let baseline = measured
                .iter()
                .find(|(size, _, _)| *size == 4_600)
                .map(|(_, ms, _)| *ms)
                .or_else(|| measured.last().map(|(_, ms, _)| *ms))
                .unwrap_or(1.0);
            println!(
                "  {:>7}  {:>11}  {:>11}  {:>11}  {:>9}",
                "rows", "total ms", "us/row", "statements", "vs 4600"
            );
            for (size, ms, statements) in &measured {
                let per_row = ms * 1_000.0 / edges as f64;
                let ratio = if *size == 4_600 {
                    String::from("(baseline)")
                } else {
                    format!("{:.2}x", ms / baseline)
                };
                println!("  {size:>7}  {ms:>11.1}  {per_row:>11.2}  {statements:>11}  {ratio:>9}");
            }
        });
    }

    /// Sweeps flush/drain batch sizes for the scraper pipeline's two
    /// accumulator knobs (`FLUSH_BATCH_SIZE` and `ACCUMULATOR_DRAIN_SIZE` in
    /// `src/web/manager.rs`).
    ///
    /// The tunables trade one transaction per *batch* against transaction
    /// *size*: small batches pay begin/commit overhead per file (the cost this
    /// batching was added to avoid), large batches concentrate every phase's
    /// writes into fewer but heavier `CONCURRENT` transactions that contend
    /// longer with other scrapers. The optimum shows up as the size whose
    /// `us/file` bottoms out — beyond it per-batch savings stop paying for the
    /// bigger single writes.
    ///
    /// Both paths go through `process_scraper`: the flush path (files + their
    /// tags, the re-scrape case where every file and tag already exists) and
    /// the drain path (pending scrape jobs, which `job_add_sql` upserts on the
    /// `time, reptime, site, param` unique index).
    ///
    /// ```text
    /// BENCH_FLUSH_ITERS=10 BENCH_TAGS_PER_FILE=8 cargo test --lib \
    ///     db::turso::bench::tests::db_bench_flush_batch_sweep -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: flush/drain batch-size sweep"]
    fn db_bench_flush_batch_sweep() {
        let files = env_u64("BENCH_FILES", 20_000);
        let tags = env_u64("BENCH_TAGS", 20_000);
        let iters = env_u64("BENCH_FLUSH_ITERS", 5).max(1);
        let tags_per_file = env_u64("BENCH_TAGS_PER_FILE", 8).max(1);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed("flush_sweep", files, tags));
        println!("\nBENCH flush/drain batch sweep: {}", fixture.describe());

        let namespace = GenericNamespaceObj {
            name: "source_url".to_string(),
            description: None,
        };

        rt.block_on(async {
            // File + hash pairs so the FileManager keys can be rebuilt each
            // iteration (process_scraper consumes its arguments).
            let mut pairs: Vec<(u64, String)> = Vec::new();
            for (i, file_id) in fixture.file_ids().into_iter().enumerate() {
                pairs.push((file_id, format!("{i:064x}")));
            }
            let storage_id = fixture.storage_id;

            let make_manager = |i: usize| FileManager {
                internal: FileInternal {
                    id: Some(pairs[i % pairs.len()].0),
                    hash: pairs[i % pairs.len()].1.clone(),
                    extension: "jpg".to_string(),
                    storage_id,
                    size_bytes: Some(1024),
                },
                identifying_hashes: vec![],
            };

            let make_actions = |i: usize| {
                (0..tags_per_file)
                    .map(|t| FileTagAction {
                        operation: TagOperation::Add,
                        tags: vec![shared_types::PluginTag {
                            tag: Tag {
                                name: format!("bench-tag-{}", (i as u64 + t) % tags),
                                namespace: namespace.clone(),
                            },
                            tag_type: TagType::Normal,
                            relates_to: None,
                        }],
                    })
                    .collect::<Vec<_>>()
            };

            let make_job = |i: usize| ScraperDataReturn {
                job: PluginJob {
                    time: 0,
                    reptime: 0,
                    priority: 1,
                    site: "bench".to_string(),
                    recreation: None,
                    param: vec![ScraperParam::Url(Url {
                        url: format!("https://bench.local/page/{}", i),
                        local_modifiers: Vec::new(),
                    })],
                    user_data: Default::default(),
                },
                skip_conditions: Vec::new(),
            };

            println!(
                "  {:>7}  {:>10}  {:>10}  {:>10}  {:>10}",
                "batch", "flush ms", "us/file", "jobs ms", "us/job"
            );

            for batch in [1usize, 8, 16, 32, 64, 128, 256, 512, 1024] {
                // Flush path: `batch` files each carrying their tag actions.
                let mut flush = Duration::ZERO;
                for _ in 0..iters {
                    let map: HashMap<FileManager, Vec<FileTagAction>> = (0..batch)
                        .map(|i| (make_manager(i), make_actions(i)))
                        .collect();
                    let t = Instant::now();
                    let ok = fixture
                        .db
                        .clone()
                        .process_scraper(map, Vec::new(), "bench flush sweep".to_string())
                        .await;
                    assert!(ok, "process_scraper flush failed at batch {batch}");
                    flush += t.elapsed();
                }

                // Drain path: `batch` pending scrape jobs upserted in one call.
                let mut jobs_elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let jobs: Vec<ScraperDataReturn> = (0..batch).map(|i| make_job(i)).collect();
                    let t = Instant::now();
                    let ok = fixture
                        .db
                        .clone()
                        .process_scraper(HashMap::new(), jobs, "bench drain sweep".to_string())
                        .await;
                    assert!(ok, "process_scraper jobs failed at batch {batch}");
                    jobs_elapsed += t.elapsed();
                }

                let flush_ms = flush.as_secs_f64() * 1_000.0 / iters as f64;
                let jobs_ms = jobs_elapsed.as_secs_f64() * 1_000.0 / iters as f64;
                println!(
                    "  {batch:>7}  {flush_ms:>10.2}  {:>10.2}  {jobs_ms:>10.2}  {:>10.2}",
                    flush_ms * 1_000.0 / batch as f64,
                    jobs_ms * 1_000.0 / batch as f64,
                );
            }
        });
    }

    /// Whether resolving which edges already exist is cheaper than letting
    /// `INSERT OR IGNORE` discover the conflicts.
    ///
    /// This is the pattern already applied to tags and files (`tags_resolve_existing`
    /// / `file_add_bulk`): resolve the batch's existing rows first, then insert only
    /// what is genuinely novel. `parents_bulk_add` never got that treatment — it
    /// hands the whole batch to `OR IGNORE` and lets the engine reject each row on
    /// the unique index.
    ///
    /// A conflict inside `OR IGNORE` is not free: the row still walks the insert
    /// path before the index rejects it. So on a re-add-heavy workload (every
    /// re-scrape) a pre-check that finds nothing novel could skip that entirely.
    #[test]
    #[ignore = "manual benchmark: Parents OR IGNORE vs an existence pre-check"]
    fn db_bench_parents_precheck() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let edges = env_u64("BENCH_PARENT_EDGES", SQL_CHUNK_SIZE as u64);
        let rounds = env_u64("BENCH_ITERS", 5).max(1);
        let rt = tokio::runtime::Runtime::new().unwrap();

        println!("\nBENCH Parents OR IGNORE vs pre-check: {edges} edges");

        rt.block_on(async {
            let path = bench_db_path("precheck");
            bench_remove(&path);
            let db = TursoDatabase::new_with_exit(&path, Arc::new(AtomicBool::new(false))).await;
            let conn = db.connect().unwrap();

            let batch: Vec<TagParents> = (0..edges)
                .map(|i| TagParents {
                    tag_id: 1 + (i % tags),
                    relate_tag_id: 1 + ((i / 3 + 7) % tags),
                    limit_to: if i.is_multiple_of(3) {
                        Some(1 + ((i + 11) % tags))
                    } else {
                        None
                    },
                })
                .collect();
            let sql = parents_insert_sql(batch.len());
            let params = parents_params(&batch.iter().collect::<Vec<_>>());

            // First insert makes every row exist, so both shapes then do pure re-add.
            conn.execute(&sql, turso::params_from_iter(params.clone()))
                .await
                .unwrap();

            let t = Instant::now();
            for _ in 0..rounds {
                conn.execute(&sql, turso::params_from_iter(params.clone()))
                    .await
                    .unwrap();
            }
            let or_ignore = t.elapsed();
            report("re-add via INSERT OR IGNORE", edges, rounds, or_ignore);

            // Pre-check, worst case: one existence probe per row, so N statements for N
            // edges. This is the shape `tags_resolve_existing` avoids by batching,
            // and it is here to show what a naive pre-check costs.
            let t = Instant::now();
            for _ in 0..rounds {
                let mut novel: Vec<&TagParents> = Vec::new();
                for parent in &batch {
                    let hit = conn
                        .query(
                            "SELECT 1 FROM Parents WHERE tag_id = ?1 AND relate_tag_id = ?2 \
                             AND IFNULL(limit_to, -1) = ?3 LIMIT 1;",
                            (
                                parent.tag_id as i64,
                                parent.relate_tag_id as i64,
                                // `-1` is the sentinel the current Parents
                                // schema stores for "no limit tag", matching
                                // what `parents_params` binds.
                                parent.limit_to.map(|id| id as i64).unwrap_or(-1),
                            ),
                        )
                        .await
                        .unwrap()
                        .next()
                        .await
                        .unwrap()
                        .is_some();
                    if !hit {
                        novel.push(parent);
                    }
                }
                std::hint::black_box(&novel);
            }
            let precheck = t.elapsed();
            report(
                "re-add via 1 probe/row (worst case)",
                edges,
                rounds,
                precheck,
            );

            println!(
                "    pre-check / OR IGNORE = {:.2}x",
                precheck.as_secs_f64() / or_ignore.as_secs_f64().max(f64::MIN_POSITIVE)
            );
            println!("    (a pre-check only wins if it is batched into few statements;");
            println!("     one probe per row is the worst case and is shown for contrast)");

            db.shutdown().await;
            drop(db);
            bench_remove(&path);
        });
    }

    /// The batched existence pre-check, in three candidate shapes, against the
    /// `INSERT OR IGNORE` path it is meant to replace.
    ///
    /// `db_bench_parents_precheck` established that a probe per row is 10x
    /// *slower* than just letting `OR IGNORE` reject the conflicts, so a
    /// pre-check is only worth having if it collapses to a handful of
    /// statements. Three shapes get that down from N statements, and the
    /// question this benchmark answers is which one turso actually executes
    /// cheaply:
    ///
    /// 1. one range query per distinct `tag_id` in the chunk (the batch is
    ///    already sorted on the index key, so a group is one contiguous range).
    /// 2. one row-value `IN` list per chunk, which is the shape closest to the
    ///    unique index's own key.
    /// 3. one `OR` of those ranges per chunk.
    ///
    /// Both directions matter and they pull opposite ways: on a re-add the
    /// pre-check has to *find* everything (a scan is as bad as a wasted insert),
    /// and on a novel import it has to find nothing without paying for the look.
    /// So each shape is timed on both, against the same row counts.
    #[test]
    #[ignore = "manual benchmark: Parents batched existence pre-check"]
    fn db_bench_parents_precheck_batched() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let edges = env_u64("BENCH_PARENT_EDGES", SQL_CHUNK_SIZE as u64);
        let rounds = env_u64("BENCH_ITERS", 5).max(1);
        // Row-value `IN` binds three parameters per row, so its statement is
        // capped well below `SQL_CHUNK_SIZE` to stay under any bound turso puts
        // on the parameter count.
        let in_chunk = env_u64("BENCH_IN_CHUNK", 500).max(1) as usize;
        // Tags per range query in the coalesced shape. Larger means fewer
        // statements and more over-return. Swept rather than fixed, since the
        // tradeoff depends on how the batch's tags are spread.
        let span_sweep: Vec<usize> = match std::env::var("BENCH_SPAN_TAGS") {
            Ok(value) => value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .filter(|width| *width > 0)
                .collect(),
            Err(_) => vec![16, 64, 256, 1024],
        };
        assert!(!span_sweep.is_empty(), "BENCH_SPAN_TAGS must name a width");
        // The losing shapes, measured only when asked for.
        let all_shapes = std::env::var("BENCH_ALL_SHAPES").is_ok();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed("parents_precheck", 1_000, tags));
        println!("\nBENCH Parents batched pre-check: {}", fixture.describe());
        let db = fixture.db.clone();
        let conn = fixture.conn.clone();
        assert!(
            tags >= 8,
            "BENCH_TAGS must leave room for four disjoint bands"
        );

        // Disjoint tag bands, so the novel set for one shape cannot be mistaken
        // for another shape's novel set and no shape gets a free ride on
        // another shape's rows.
        let band = tags / 4;
        let batch_for = |shape: u64, i: u64| {
            let slot = i % band;
            // `relate_tag_id` is offset from `tag_id` by a non-zero amount
            // modulo `band - 1`, which keeps the parents table's
            // `CHECK (tag_id != relate_tag_id)` satisfied.
            let relate = 1 + (slot + 1 + (i / band) % (band - 1)) % band;
            let base = 1 + shape * band;
            TagParents {
                tag_id: base + slot,
                relate_tag_id: base + relate,
                limit_to: if i.is_multiple_of(3) {
                    Some(base + 1 + (i + 11) % band)
                } else {
                    None
                },
            }
        };
        let unique_for = |shape: u64| -> Vec<TagParents> {
            dedupe_sorted((0..edges).map(|i| batch_for(shape, i)).collect())
        };

        rt.block_on(async {
            let existing = unique_for(0);

            // Seed, so the re-add case has something real to collide with.
            insert_legacy(&conn, &existing).await;
            let sorted_existing: Vec<&TagParents> = existing.iter().collect();

            println!("  -- plans, before any timing --");
            explain(
                &conn,
                "SELECT relate_tag_id, limit_to FROM Parents WHERE tag_id = 1 \
                 AND relate_tag_id >= 1 AND relate_tag_id <= 4",
            )
            .await;
            explain(
                &conn,
                "SELECT tag_id, relate_tag_id, IFNULL(limit_to, -1) FROM Parents \
                 WHERE (tag_id, relate_tag_id, IFNULL(limit_to, -1)) IN (VALUES (1, 2, -1))",
            )
            .await;
            explain(
                &conn,
                "SELECT tag_id, relate_tag_id, IFNULL(limit_to, -1) FROM Parents \
                 WHERE tag_id >= 1 AND tag_id <= 64",
            )
            .await;

            println!("  -- RE-ADD (every row already present) --");
            let t = Instant::now();
            for _ in 0..rounds {
                insert_legacy(&conn, &existing).await;
            }
            report("legacy: sort + OR IGNORE", edges, rounds, t.elapsed());

            macro_rules! time_precheck {
                ($label:expr, $shape:ident) => {{
                    let t = Instant::now();
                    for _ in 0..rounds {
                        let novel_rows = $shape(&conn, &sorted_existing, in_chunk).await;
                        assert_eq!(novel_rows.len(), 0, "{} must find every row", $label);
                    }
                    report(
                        &format!("pre-check only: {}", $label),
                        edges,
                        rounds,
                        t.elapsed(),
                    );
                }};
            }
            // The two shapes that lost are opt-in, because both are slow enough at large
            // batch sizes to dominate the run: row-value `IN` degrades to a
            // table SCAN and would scan the whole batch once per statement.
            // They stay here so the losing plans can be re-confirmed on a small
            // fixture with BENCH_ALL_SHAPES=1.
            if all_shapes {
                time_precheck!("grouped range / tag_id", precheck_grouped);
                let t = Instant::now();
                let novel_rows = precheck_rowvalue(&conn, &sorted_existing, in_chunk).await;
                assert_eq!(novel_rows.len(), 0);
                report("pre-check only: row-value IN", edges, 1, t.elapsed());
            }

            // The span width trades statement count against over-return, so
            // sweep it rather than quoting a single width.
            for &width in &span_sweep {
                let t = Instant::now();
                for _ in 0..rounds {
                    let novel_rows = precheck_spans(&conn, &sorted_existing, width).await;
                    assert_eq!(novel_rows.len(), 0, "spans must find every row");
                }
                report(
                    &format!("pre-check only: coalesced spans ({width} tags)"),
                    edges,
                    rounds,
                    t.elapsed(),
                );
            }

            println!("  -- NOVEL (nothing present) --");
            {
                let fresh = unique_for(1);
                let t = Instant::now();
                insert_legacy(&conn, &fresh).await;
                report("legacy: sort + OR IGNORE", edges, 1, t.elapsed());
            }
            for (shape, label) in [
                (2_u64, "pre-check spans, then insert"),
                (3, "pre-check spans (wide), then insert"),
            ] {
                let fresh = unique_for(shape);
                let sorted: Vec<&TagParents> = fresh.iter().collect();
                let t = Instant::now();
                let picked = if shape == 2 {
                    precheck_spans(&conn, &sorted, span_sweep[0]).await
                } else {
                    precheck_spans(&conn, &sorted, *span_sweep.last().unwrap()).await
                };
                assert_eq!(
                    picked.len(),
                    fresh.len(),
                    "a novel batch must not filter anything out"
                );
                insert_slice(&conn, &picked).await;
                report(label, edges, 1, t.elapsed());
            }

            db.shutdown().await;
        });
    }

    /// The index key, as the null-safe unique index stores it.
    fn parents_key(parent: &TagParents) -> (u64, u64, i64) {
        (
            parent.tag_id,
            parent.relate_tag_id,
            parent.limit_to.map(|id| id as i64).unwrap_or(-1),
        )
    }

    fn dedupe_sorted(rows: Vec<TagParents>) -> Vec<TagParents> {
        let mut rows = rows;
        rows.sort_unstable_by_key(parents_key);
        rows.dedup_by_key(|parent| {
            (
                parent.tag_id,
                parent.relate_tag_id,
                parent.limit_to.map(|id| id as i64).unwrap_or(-1),
            )
        });
        rows
    }

    /// What `parents_bulk_add` did before the pre-check: sort, then hand the
    /// whole batch to chunked `INSERT OR IGNORE`.
    async fn insert_legacy(conn: &Connection, rows: &[TagParents]) {
        let sorted = dedupe_sorted(rows.to_vec());
        insert_slice(conn, &sorted.iter().collect::<Vec<_>>()).await;
    }

    async fn insert_slice(conn: &Connection, rows: &[&TagParents]) {
        for chunk in rows.chunks(SQL_CHUNK_SIZE) {
            conn.execute(
                &parents_insert_sql(chunk.len()),
                turso::params_from_iter(parents_params(chunk)),
            )
            .await
            .unwrap();
        }
    }

    /// One range query per distinct `tag_id`. Correct because the batch is
    /// sorted, so each tag's rows are one contiguous `[first, last]` range.
    async fn precheck_grouped<'a>(
        conn: &Connection,
        sorted: &[&'a TagParents],
        _in_chunk: usize,
    ) -> Vec<&'a TagParents> {
        let mut novel = Vec::with_capacity(sorted.len());
        let mut start = 0;
        while start < sorted.len() {
            let tag_id = sorted[start].tag_id;
            let mut end = start + 1;
            while end < sorted.len() && sorted[end].tag_id == tag_id {
                end += 1;
            }
            let group = &sorted[start..end];
            let first = group[0].relate_tag_id;
            let last = group[group.len() - 1].relate_tag_id;
            let mut rows = conn
                .query(
                    "SELECT relate_tag_id, limit_to FROM Parents \
                     WHERE tag_id = ?1 AND relate_tag_id >= ?2 AND relate_tag_id <= ?3",
                    (tag_id as i64, first as i64, last as i64),
                )
                .await
                .unwrap();
            let mut hit: HashSet<(u64, Option<u64>)> = HashSet::new();
            while let Some(row) = rows.next().await.unwrap() {
                let relate: i64 = row.get(0).unwrap();
                let limit: Option<i64> = row.get(1).unwrap();
                hit.insert((relate as u64, limit.map(|l| l as u64)));
            }
            novel.extend(
                group
                    .iter()
                    .filter(|p| !hit.contains(&(p.relate_tag_id, p.limit_to))),
            );
            start = end;
        }
        novel
    }

    /// One row-value `IN` list per statement, on the unique index's own key.
    async fn precheck_rowvalue<'a>(
        conn: &Connection,
        sorted: &[&'a TagParents],
        in_chunk: usize,
    ) -> Vec<&'a TagParents> {
        let mut hit: HashSet<(u64, u64, i64)> = HashSet::new();
        for chunk in sorted.chunks(in_chunk) {
            let placeholders = std::iter::repeat_n("(?, ?, ?)", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let mut params: Vec<turso::Value> = Vec::with_capacity(chunk.len() * 3);
            for parent in chunk {
                params.push(turso::Value::from(parent.tag_id as i64));
                params.push(turso::Value::from(parent.relate_tag_id as i64));
                // Must mirror the index's own `IFNULL(limit_to, -1)` sentinel.
                params.push(turso::Value::from(
                    parent.limit_to.map(|id| id as i64).unwrap_or(-1),
                ));
            }
            let mut rows = conn
                .query(
                    format!(
                        "SELECT tag_id, relate_tag_id, IFNULL(limit_to, -1) FROM Parents \
                         WHERE (tag_id, relate_tag_id, IFNULL(limit_to, -1)) IN (VALUES {placeholders})"
                    ),
                    turso::params_from_iter(params),
                )
                .await
                .unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                let key = (
                    row.get::<i64>(0).unwrap(),
                    row.get::<i64>(1).unwrap(),
                    row.get::<i64>(2).unwrap(),
                );
                hit.insert((key.0 as u64, key.1 as u64, key.2));
            }
        }
        sorted
            .iter()
            .filter(|p| {
                !hit.contains(&(
                    p.tag_id,
                    p.relate_tag_id,
                    p.limit_to.map(|id| id as i64).unwrap_or(-1),
                ))
            })
            .copied()
            .collect()
    }

    /// Coalesces consecutive `tag_id` groups into as few range queries as
    /// possible: `tag_id >= a AND tag_id <= b` covers the groups in between,
    /// and those are the batch's own tags, so the extra rows returned are the
    /// same rows the grouped shape would have returned anyway.
    ///
    /// `or_span_tags` trades statement count against over-return. One query per
    /// tag is `edges` statements; one query per span of `or_span_tags` is
    /// `edges / or_span_tags`, at the cost of reading `Parents` rows for tags in
    /// the gaps. Since a scraper batch is usually a contiguous-ish set of tags
    /// from one plugin, the gaps are mostly empty.
    async fn precheck_spans<'a>(
        conn: &Connection,
        sorted: &[&'a TagParents],
        or_span_tags: usize,
    ) -> Vec<&'a TagParents> {
        let mut hit: HashSet<(u64, u64, i64)> = HashSet::new();
        let mut start = 0;
        while start < sorted.len() {
            let first_tag = sorted[start].tag_id;
            // Walk the batch while the tag ids stay close enough that covering
            // the gap is cheaper than another statement.
            let mut end = start;
            let span_end = first_tag.saturating_add(or_span_tags as u64);
            while end < sorted.len() && sorted[end].tag_id <= span_end {
                end += 1;
            }
            let last_tag = sorted[end - 1].tag_id;
            let mut rows = conn
                .query(
                    "SELECT tag_id, relate_tag_id, IFNULL(limit_to, -1) FROM Parents \
                     WHERE tag_id >= ?1 AND tag_id <= ?2",
                    (first_tag as i64, last_tag as i64),
                )
                .await
                .unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                let tag: i64 = row.get(0).unwrap();
                let relate: i64 = row.get(1).unwrap();
                let limit: i64 = row.get(2).unwrap();
                hit.insert((tag as u64, relate as u64, limit));
            }
            start = end;
        }
        sorted
            .iter()
            .filter(|p| !hit.contains(&parents_key(p)))
            .copied()
            .collect()
    }

    /// The shipped `parents_bulk_add`, on the two workloads that matter, at
    /// the scale the slow-down was reported at.
    ///
    /// `db_bench_parents_precheck_batched` measures the candidate shapes in
    /// isolation; this one calls the real function, so it also covers the
    /// adaptive decision and the reuse of the sample probe. `BENCH_EDGES`
    /// scales the batch, and the fixture is fresh per workload so the novel and
    /// re-add cases are not measuring each other's leftovers.
    #[test]
    #[ignore = "manual benchmark: shipped parents_bulk_add, novel vs re-add"]
    fn db_bench_parents_precheck_end_to_end() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let edges = env_u64("BENCH_PARENT_EDGES", 200_000);
        let rounds = env_u64("BENCH_ITERS", 3).max(1);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(Fixture::seed("parents_e2e", 1_000, tags));
        println!("\nBENCH Parents end-to-end: {}", fixture.describe());
        let db = fixture.db.clone();
        let _conn = fixture.conn.clone();
        assert!(
            tags >= 8,
            "BENCH_TAGS must leave room for two disjoint bands"
        );

        let rows_for = |i: u64| {
            let tag_count = tags;
            TagParents {
                tag_id: 1 + (i % tag_count),
                // Non-zero offset from `tag_id`, keeping
                // `CHECK (tag_id != relate_tag_id)` satisfied.
                relate_tag_id: 1 + (i + 1 + i / tag_count) % tag_count,
                limit_to: if i.is_multiple_of(3) {
                    Some(1 + ((i + 11) % tag_count))
                } else {
                    None
                },
            }
        };

        rt.block_on(async {
            // Each case gets its own database, and each path its own database
            // again. That matters more than it looks: per-row insert cost grows
            // with how much is already in `Parents` (186 us/row at 4600 rows
            // against 650 us/row at 200k on this fixture), so running the second
            // measured path against a table the first one filled reads as a 39%
            // regression that is really just the table being bigger.
            // Insert the batch through the pre-pre-check path, timed.
            async fn baseline_insert(conn: &Connection, batch: &[TagParents]) -> Duration {
                let t = Instant::now();
                insert_legacy(conn, batch).await;
                t.elapsed()
            }

            for (case, re_add) in [("NOVEL import", false), ("RE-ADD", true)] {
                println!(
                    "  -- {case}: {} rows, {} --",
                    edges,
                    if re_add {
                        "all stored"
                    } else {
                        "nothing stored"
                    }
                );

                // Seed state: for a re-add, the rows have to be there first.
                let rows = dedupe_sorted((0..edges).map(rows_for).collect());
                let mut results = Vec::new();
                for (label, shipped) in [
                    ("baseline: sort + OR IGNORE", false),
                    ("parents_bulk_add", true),
                ] {
                    let path = bench_db_path(&format!(
                        "e2e_{}_{}",
                        if re_add { "readd" } else { "novel" },
                        shipped
                    ));
                    bench_remove(&path);
                    let db =
                        TursoDatabase::new_with_exit(&path, Arc::new(AtomicBool::new(false))).await;
                    let conn = db.connect().unwrap();
                    // Tags have to exist before `Parents` rows can reference them.
                    let tag_set: HashSet<Tag> = (1..=tags)
                        .map(|id| Tag {
                            name: format!("bench-tag-{id}"),
                            namespace: GenericNamespaceObj {
                                name: "source_url".to_string(),
                                description: None,
                            },
                        })
                        .collect();
                    db.tag_add_bulk(&conn, &tag_set).await.unwrap();
                    if re_add {
                        // Seed with the *baseline* path either way, so the two
                        // are re-adding identical pre-existing state.
                        insert_legacy(&conn, &rows).await;
                    }

                    // A novel import is one insert and cannot be repeated without turning into a
                    // re-add, so it is timed once. Only the re-add case loops.
                    let elapsed = if shipped {
                        let t = Instant::now();
                        if re_add {
                            for _ in 0..rounds {
                                db.parents_bulk_add(&conn, &rows).await.unwrap();
                            }
                        } else {
                            db.parents_bulk_add(&conn, &rows).await.unwrap();
                        }
                        t.elapsed()
                    } else if re_add {
                        let t = Instant::now();
                        for _ in 0..rounds {
                            insert_legacy(&conn, &rows).await;
                        }
                        t.elapsed()
                    } else {
                        baseline_insert(&conn, &rows).await
                    };
                    report(
                        label,
                        rows.len() as u64,
                        if re_add { rounds } else { 1 },
                        elapsed,
                    );

                    let count: i64 = {
                        let mut stmt = conn.prepare("SELECT COUNT(*) FROM Parents").await.unwrap();
                        stmt.query_row(()).await.unwrap().get(0).unwrap()
                    };
                    assert_eq!(
                        count,
                        rows.len() as i64,
                        "{case}/{label}: the pre-check must not change the stored row count"
                    );

                    db.shutdown().await;
                    drop(db);
                    bench_remove(&path);
                    results.push(elapsed.as_secs_f64());
                }
                println!(
                    "    shipped/baseline = {:.2}x",
                    results[1] / results[0].max(f64::MIN_POSITIVE)
                );
            }

            db.shutdown().await;
        });
    }

    /// Candidate physical layouts for `Parents`, measured as a batch at a
    /// realistic scale (`BENCH_EDGES`, default 100k edges over `BENCH_TAGS`
    /// tags) instead of one chunk at a time.
    ///
    /// The question this answers is whether moving `limit_to` out of the edge
    /// row into its own table — so the main table holds nothing but
    /// `(seq, tag_id, relate_tag_id)` and stays uniform — buys anything.
    /// `db_bench_parents_schema_variants` answered the smaller version of this
    /// at one chunk's worth of rows, and found index count irrelevant; at 100k
    /// the picture can differ, because index depth and page count finally
    /// matter and the split trades a third-column unique index for an extra
    /// b-tree plus a join that has to resolve each edge's `seq`.
    ///
    /// Every layout is measured on the same four workloads:
    ///
    /// * **novel insert** — the cold import of a fresh database.
    /// * **re-add** — `INSERT OR IGNORE` over rows that all already exist,
    ///   which is what every re-scrape pays.
    /// * **forward read** — `parent_relationships_get`'s `WHERE tag_id = ?`.
    /// * **reverse read** — `child_relationships_get`'s `WHERE relate_tag_id = ?`.
    /// * **pair read** — `parent_relationship_get`'s exact two-column probe.
    ///
    /// Statement count is reported alongside time, because the split cannot
    /// insert an edge and its `limit_to` in one statement: the `seq` is only
    /// known after the edge lands, so the limit rows need a join back. That
    /// extra round trip per chunk is the split's structural cost, and it has to
    /// be paid whether or not the b-trees are cheaper.
    ///
    /// Each layout gets its own database and its own process-wide budget, so a
    /// layout is never measured against another's leftovers.
    #[test]
    #[ignore = "manual benchmark: Parents physical layouts at batch scale"]
    fn db_bench_parents_layout() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let edges = env_u64("BENCH_EDGES", 100_000);
        let rounds = env_u64("BENCH_ITERS", 3).max(1);
        let probes = env_u64("BENCH_PROBES", 200).max(1);
        let rt = tokio::runtime::Runtime::new().unwrap();

        // The same generator every other parents benchmark uses, so the numbers
        // are comparable with `db_bench_parents_schema_variants`: distinct
        // triples over 1..tags, with `limit_to` set on one row in three.
        let batch: Vec<TagParents> = dedupe_sorted(
            (0..edges)
                .map(|i| TagParents {
                    tag_id: 1 + (i % tags),
                    relate_tag_id: 1 + (i / 3 + 7) % tags,
                    limit_to: if i.is_multiple_of(3) {
                        Some(1 + (i + 11) % tags)
                    } else {
                        None
                    },
                })
                .collect(),
        );
        assert_eq!(
            batch.len() as u64,
            edges,
            "generator must produce {} distinct edges",
            edges
        );

        // Probe keys drawn from the batch itself, so every read hits rows that
        // exist. `tag_id` repeats every `tags` edges, so the forward probes are
        // real multi-row lookups rather than single-row point hits.
        let probe_tags: Vec<u64> = batch
            .iter()
            .map(|p| p.tag_id)
            .take(probes as usize)
            .collect();
        let probe_relate: Vec<u64> = batch
            .iter()
            .map(|p| p.relate_tag_id)
            .take(probes as usize)
            .collect();
        let probe_pairs: Vec<(u64, u64)> = batch
            .iter()
            .map(|p| (p.tag_id, p.relate_tag_id))
            .take(probes as usize)
            .collect();

        println!(
            "\nBENCH Parents layouts: {} edges, {} tags, {} probes/round, {} re-add rounds",
            batch.len(),
            tags,
            probes,
            rounds
        );

        let layouts = [
            ParentLayout::Current,
            ParentLayout::Sentinel,
            ParentLayout::CurrentCover,
            ParentLayout::SentinelCover,
            ParentLayout::WithoutRowid,
            ParentLayout::SplitSeq,
            ParentLayout::FloorNoIndex,
            ParentLayout::SwapUnique,
            ParentLayout::SwapUniqueOnly,
        ];

        rt.block_on(async {
            let mut results: Vec<(ParentLayout, LayoutResult)> = Vec::new();

            for layout in layouts {
                let path = bench_db_path(&format!("layout_{}", layout.key()));
                bench_remove(&path);
                let db =
                    TursoDatabase::new_with_exit(&path, Arc::new(AtomicBool::new(false))).await;
                let conn = db.connect().unwrap();
                // Boot already built the production `Parents` table; each layout
                // replaces it so the only difference measured is the layout.
                conn.execute_batch(
                    "DROP TABLE IF EXISTS Parents;
                     DROP TABLE IF EXISTS Parents_limit;
                     DROP TABLE IF EXISTS Parents_stage;",
                )
                .await
                .unwrap();
                if let Err(error) = conn.execute_batch(layout.ddl()).await {
                    // `db_bench_parents_schema_probe` records that turso rejects
                    // `WITHOUT ROWID` without `--experimental-without-rowid`.
                    // That is a capability limit, not a measurement, so the
                    // layout is reported and skipped rather than failing the run.
                    println!(
                        "\n  == {} ==\n  SKIPPED: turso rejects this layout -> {error}",
                        layout.label()
                    );
                    db.shutdown().await;
                    drop(db);
                    bench_remove(&path);
                    continue;
                }

                println!("\n  == {} ==", layout.label());
                for sql in layout.plans() {
                    explain(&conn, sql).await;
                }

                // The split's per-statement cost, collected separately so its re-add blowup can
                // be attributed to one of the four statements rather than guessed
                // at. Empty for every single-table layout.
                let mut split_detail: Vec<Duration> = Vec::new();

                let (novel_ms, novel_stmts) = {
                    let t = Instant::now();
                    let stmts = layout_insert(&conn, layout, &batch, &mut split_detail).await;
                    (t.elapsed().as_secs_f64() * 1_000.0, stmts)
                };
                report(
                    "novel insert",
                    batch.len() as u64,
                    1,
                    Duration::from_secs_f64(novel_ms / 1_000.0),
                );
                println!(
                    "      {} statements ({:.0} rows/statement)",
                    novel_stmts,
                    batch.len() as f64 / novel_stmts.max(1) as f64
                );
                report_split_detail(&split_detail);

                let readd = {
                    let t = Instant::now();
                    for _ in 0..rounds {
                        layout_insert(&conn, layout, &batch, &mut split_detail).await;
                    }
                    t.elapsed()
                };
                report("re-add", batch.len() as u64, rounds, readd);
                report_split_detail(&split_detail);

                if matches!(
                    layout,
                    ParentLayout::FloorNoIndex | ParentLayout::SwapUniqueOnly
                ) {
                    // Without a unique key on `(tag_id, ...)` a re-add appends
                    // duplicate rows, and `SwapUniqueOnly` has no forward index,
                    // so neither measures anything the others can be compared
                    // against.
                    println!("      (re-add and reads skipped: no unique index to re-add against)");
                    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    db.shutdown().await;
                    drop(db);
                    bench_remove(&path);
                    results.push((
                        layout,
                        LayoutResult {
                            novel_ms,
                            readd_ms: f64::NAN,
                            forward_us: f64::NAN,
                            reverse_us: f64::NAN,
                            pair_us: f64::NAN,
                            bytes,
                        },
                    ));
                    continue;
                }

                // Reads are per-probe-key, so the row count is the probe count.
                let forward = {
                    let t = Instant::now();
                    let mut rows = 0u64;
                    for _ in 0..rounds {
                        for tag_id in &probe_tags {
                            rows += layout_read_tag(&conn, layout, *tag_id).await;
                        }
                    }
                    let _ = rows;
                    t.elapsed()
                };
                report("forward read (WHERE tag_id)", probes, rounds, forward);

                let reverse = {
                    let t = Instant::now();
                    for _ in 0..rounds {
                        for relate in &probe_relate {
                            layout_read_relate(&conn, layout, *relate).await;
                        }
                    }
                    t.elapsed()
                };
                report(
                    "reverse read (WHERE relate_tag_id)",
                    probes,
                    rounds,
                    reverse,
                );

                let pair = {
                    let t = Instant::now();
                    for _ in 0..rounds {
                        for (tag_id, relate) in &probe_pairs {
                            layout_read_pair(&conn, layout, *tag_id, *relate).await;
                        }
                    }
                    t.elapsed()
                };
                report("pair read (tag + relate)", probes, rounds, pair);

                // Size on disk after 100k edges: the split's whole argument is
                // that the main table stays dense, so the file size is evidence
                // either way.
                let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

                db.shutdown().await;
                drop(db);
                bench_remove(&path);

                results.push((
                    layout,
                    LayoutResult {
                        novel_ms,
                        readd_ms: readd.as_secs_f64() * 1_000.0,
                        forward_us: forward.as_secs_f64() * 1_000_000.0 / (probes * rounds) as f64,
                        reverse_us: reverse.as_secs_f64() * 1_000_000.0 / (probes * rounds) as f64,
                        pair_us: pair.as_secs_f64() * 1_000_000.0 / (probes * rounds) as f64,
                        bytes,
                    },
                ));
            }

            println!("\n  == summary, everything relative to the current schema ==");
            println!(
                "  {:<26} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}",
                "layout", "novel", "re-add", "fwd us", "rev us", "pair us", "size"
            );
            let base = results[0].1.novel_ms;
            let base_readd = results[0].1.readd_ms;
            for (layout, result) in &results {
                println!(
                    "  {:<26} {:>9.2}x {:>9} {:>10} {:>10} {:>10} {:>7}M",
                    layout.label(),
                    result.novel_ms / base,
                    if result.readd_ms.is_nan() {
                        "n/a".to_string()
                    } else {
                        format!("{:.2}x", result.readd_ms / base_readd)
                    },
                    if result.forward_us.is_nan() {
                        "n/a".to_string()
                    } else {
                        format!("{:.1}", result.forward_us)
                    },
                    if result.reverse_us.is_nan() {
                        "n/a".to_string()
                    } else {
                        format!("{:.1}", result.reverse_us)
                    },
                    if result.pair_us.is_nan() {
                        "n/a".to_string()
                    } else {
                        format!("{:.1}", result.pair_us)
                    },
                    result.bytes / (1024 * 1024),
                );
            }
            println!(
                "\n  Absolute, for the record: current schema novel {:.1} ms / re-add {:.1} ms,\n\
                 fwd {:.1} us, rev {:.1} us, pair {:.1} us per probe, {} MiB on disk.",
                base,
                base_readd,
                results[0].1.forward_us,
                results[0].1.reverse_us,
                results[0].1.pair_us,
                results[0].1.bytes / (1024 * 1024),
            );
        });
    }

    /// Prints where a split layout's time went, statement by statement. The four
    /// steps are, in order: clear the staging table, fill it, insert the edges,
    /// and attach the limits (the join). Only the split layout records these;
    /// for every other layout this prints nothing.
    fn report_split_detail(detail: &[Duration]) {
        if detail.is_empty() {
            return;
        }
        let names = [
            "1 clear staging",
            "2 fill staging",
            "3 insert edges",
            "4 attach limits (join)",
        ];
        println!("      where it went:");
        for (index, label) in names.iter().enumerate() {
            // Each chunk pushes the four steps in order, so slice by position.
            let step: Duration = detail.iter().skip(index).step_by(names.len()).sum();
            println!(
                "        {label:<26} {:>9.1} ms  ({:>5.1}% of the timed path)",
                step.as_secs_f64() * 1_000.0,
                100.0 * step.as_secs_f64()
                    / detail
                        .iter()
                        .map(|d| d.as_secs_f64())
                        .sum::<f64>()
                        .max(f64::MIN_POSITIVE)
            );
        }
    }

    struct LayoutResult {
        novel_ms: f64,
        readd_ms: f64,
        forward_us: f64,
        reverse_us: f64,
        pair_us: f64,
        bytes: u64,
    }

    /// The physical layouts under test. Each owns its own DDL and its own
    /// insert/read statements, because the split genuinely cannot be expressed
    /// through the single-table statements the others share.
    #[derive(Clone, Copy)]
    enum ParentLayout {
        /// Production today.
        Current,
        /// `limit_to` stored as `-1` instead of `NULL`, so the unique index is a
        /// plain composite instead of an `IFNULL` expression.
        Sentinel,
        /// Production, but the reverse index carries the rest of the row so
        /// `child_relationships_get` can be answered from the index alone.
        CurrentCover,
        /// Both of the above: sentinel storage plus a covering reverse index.
        SentinelCover,
        /// The whole edge is the primary key, so there is no rowid table at all.
        WithoutRowid,
        /// `(seq, tag_id, relate_tag_id)` in one table, `limit_to` in another.
        SplitSeq,
        /// Same columns, zero indexes. Not a candidate — it has no uniqueness and
        /// no read path — but it is the floor that says how much of the per-row
        /// cost is index maintenance at all. If this is close to the real
        /// layouts, no amount of index reshaping can pay for itself, which is the
        /// whole question a split-table proposal turns on.
        FloorNoIndex,
        /// Two b-trees instead of three: the unique index is keyed
        /// `(relate_tag_id, tag_id, limit_to)` so its prefix serves the reverse
        /// lookup as a covering scan, and a plain `(tag_id)` index serves the
        /// forward one. Uniqueness covers the same set of columns as before, so
        /// only the *order* of the key changes — the one degree of freedom the
        /// production schema has not spent.
        SwapUnique,
        /// The swap with no forward index at all. Not usable (forward reads
        /// would scan), but it prices the second b-tree directly: what it saves
        /// over `SwapUnique` is what the second index costs to maintain.
        SwapUniqueOnly,
    }

    impl ParentLayout {
        /// Short filename-safe tag, so each layout's database is identifiable.
        fn key(&self) -> char {
            match self {
                ParentLayout::Current => 'a',
                ParentLayout::Sentinel => 'b',
                ParentLayout::CurrentCover => 'e',
                ParentLayout::SentinelCover => 'f',
                ParentLayout::WithoutRowid => 'c',
                ParentLayout::SplitSeq => 'd',
                ParentLayout::FloorNoIndex => 'g',
                ParentLayout::SwapUnique => 'h',
                ParentLayout::SwapUniqueOnly => 'i',
            }
        }

        fn label(&self) -> String {
            match self {
                ParentLayout::Current => "A current: rowid + expr-unique + rel".to_string(),
                ParentLayout::Sentinel => "B sentinel -1 + plain unique".to_string(),
                ParentLayout::WithoutRowid => "C WITHOUT ROWID pk(edge)".to_string(),
                ParentLayout::CurrentCover => "E current + covering reverse index".to_string(),
                ParentLayout::SentinelCover => "F sentinel + covering reverse index".to_string(),
                ParentLayout::SplitSeq => "D split: seq edge + limit_to side".to_string(),
                ParentLayout::FloorNoIndex => "G floor: no indexes at all".to_string(),
                ParentLayout::SwapUnique => "H swap: unique(rel,tag,lim) + idx(tag)".to_string(),
                ParentLayout::SwapUniqueOnly => "I swap: unique(rel,tag,lim) only".to_string(),
            }
        }

        fn ddl(&self) -> &'static str {
            match self {
                ParentLayout::Current => {
                    "CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER,
                         CHECK (tag_id != relate_tag_id));
                     CREATE UNIQUE INDEX idx_unique_parents_null_safe
                         ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));
                     CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);"
                }
                ParentLayout::Sentinel => {
                    // Tag ids are >= 1, so -1 cannot collide with a real tag.
                    "CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER NOT NULL DEFAULT -1,
                         CHECK (tag_id != relate_tag_id));
                     CREATE UNIQUE INDEX idx_parents_unique
                         ON Parents (tag_id, relate_tag_id, limit_to);
                     CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);"
                }
                ParentLayout::CurrentCover => {
                    // Only the reverse index changes: it carries the whole row,
                    // so `WHERE relate_tag_id = ?` answers without a table visit.
                    "CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER,
                         CHECK (tag_id != relate_tag_id));
                     CREATE UNIQUE INDEX idx_unique_parents_null_safe
                         ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));
                     CREATE INDEX idx_parents_rel ON Parents (relate_tag_id, tag_id, limit_to);"
                }
                ParentLayout::SentinelCover => {
                    "CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER NOT NULL DEFAULT -1,
                         CHECK (tag_id != relate_tag_id));
                     CREATE UNIQUE INDEX idx_parents_unique
                         ON Parents (tag_id, relate_tag_id, limit_to);
                     CREATE INDEX idx_parents_rel ON Parents (relate_tag_id, tag_id, limit_to);"
                }
                ParentLayout::WithoutRowid => {
                    "CREATE TABLE Parents (
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER NOT NULL DEFAULT -1,
                         PRIMARY KEY (tag_id, relate_tag_id, limit_to),
                         CHECK (tag_id != relate_tag_id)) WITHOUT ROWID;
                     CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);"
                }
                ParentLayout::SwapUnique => {
                    "CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER NOT NULL DEFAULT -1,
                         CHECK (tag_id != relate_tag_id));
                     CREATE UNIQUE INDEX idx_parents_unique
                         ON Parents (relate_tag_id, tag_id, limit_to);
                     CREATE INDEX idx_parents_tag ON Parents (tag_id);"
                }
                ParentLayout::SwapUniqueOnly => {
                    "CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER NOT NULL DEFAULT -1,
                         CHECK (tag_id != relate_tag_id));
                     CREATE UNIQUE INDEX idx_parents_unique
                         ON Parents (relate_tag_id, tag_id, limit_to);"
                }
                ParentLayout::FloorNoIndex => {
                    "CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER);"
                }
                ParentLayout::SplitSeq => {
                    // NOTE: uniqueness here is on the edge alone, so this layout
                    // can hold exactly one limit per (tag, relate) pair. That is
                    // a semantic narrowing, not just a physical one — see the
                    // report. The staging table is what lets the insert avoid a
                    // per-row `seq` lookup: the batch is parked there, the edges
                    // are inserted from it, and the limit rows join back on the
                    // edge key.
                    "CREATE TABLE Parents (
                         seq INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         CHECK (tag_id != relate_tag_id));
                     CREATE UNIQUE INDEX idx_parents_edge ON Parents (tag_id, relate_tag_id);
                     CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);
                     CREATE TABLE Parents_limit (
                         seq INTEGER PRIMARY KEY,
                         limit_to INTEGER NOT NULL);
                     CREATE TABLE Parents_stage (
                         seq INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER);"
                }
            }
        }

        /// The statements whose query plans decide whether a layout's reads are
        /// index lookups or scans.
        fn plans(&self) -> Vec<&'static str> {
            vec![
                "SELECT relate_tag_id, limit_to FROM Parents WHERE tag_id = 7",
                "SELECT tag_id, limit_to FROM Parents WHERE relate_tag_id = 7",
                "SELECT limit_to FROM Parents WHERE tag_id = 7 AND relate_tag_id = 8",
            ]
        }
    }

    /// Inserts the whole batch through whichever statements the layout needs,
    /// and returns how many statements that took.
    async fn layout_insert(
        conn: &Connection,
        layout: ParentLayout,
        rows: &[TagParents],
        split_detail: &mut Vec<Duration>,
    ) -> u64 {
        let mut statements = 0u64;
        for chunk in rows.chunks(SQL_CHUNK_SIZE) {
            match layout {
                ParentLayout::SplitSeq => {
                    statements += layout_insert_split(conn, chunk, split_detail).await;
                }
                _ => {
                    conn.execute(
                        &single_table_insert_sql(chunk.len()),
                        layout_params(layout, chunk),
                    )
                    .await
                    .unwrap();
                    statements += 1;
                }
            }
        }
        statements
    }

    async fn layout_insert_split(
        conn: &Connection,
        rows: &[TagParents],
        detail: &mut Vec<Duration>,
    ) -> u64 {
        // 1. Park the chunk, keyed by a synthetic sequence number the caller
        //    controls, so the staging table never has to be read back.
        let t = Instant::now();
        conn.execute_batch("DELETE FROM Parents_stage;")
            .await
            .unwrap();
        detail.push(t.elapsed());
        let mut placeholders = String::new();
        let mut params: Vec<turso::Value> = Vec::with_capacity(rows.len() * 4);
        for (row, parent) in rows.iter().enumerate() {
            if row > 0 {
                placeholders.push_str(", ");
            }
            let offset = row * 4;
            placeholders.push_str(&format!(
                "(?{}, ?{}, ?{}, ?{})",
                offset + 1,
                offset + 2,
                offset + 3,
                offset + 4
            ));
            params.push(turso::Value::from(row as i64 + 1));
            params.push(turso::Value::from(parent.tag_id as i64));
            params.push(turso::Value::from(parent.relate_tag_id as i64));
            match parent.limit_to {
                Some(limit_id) => params.push(turso::Value::from(limit_id as i64)),
                None => params.push(turso::Value::Null),
            }
        }
        let t = Instant::now();
        conn.execute(
            &format!("INSERT INTO Parents_stage (seq, tag_id, relate_tag_id, limit_to) VALUES {placeholders}"),
            turso::params_from_iter(params),
        )
        .await
        .unwrap();
        detail.push(t.elapsed());

        // 2. The edges themselves, deduped by the edge unique index.
        let t = Instant::now();
        conn.execute(
            "INSERT OR IGNORE INTO Parents (tag_id, relate_tag_id)
             SELECT tag_id, relate_tag_id FROM Parents_stage ORDER BY tag_id, relate_tag_id;",
            (),
        )
        .await
        .unwrap();
        detail.push(t.elapsed());

        // 3. The limits, which only exist once the edge's `seq` is known. This
        //    join is the split's structural cost: without it there is no way to
        //    attach a `limit_to` to a row whose surrogate key the client never
        //    saw.
        let t = Instant::now();
        conn.execute(
            "INSERT OR REPLACE INTO Parents_limit (seq, limit_to)
             SELECT p.seq, s.limit_to
             FROM Parents_stage s
             JOIN Parents p ON p.tag_id = s.tag_id AND p.relate_tag_id = s.relate_tag_id
             WHERE s.limit_to IS NOT NULL;",
            (),
        )
        .await
        .unwrap();
        detail.push(t.elapsed());
        4
    }

    fn single_table_insert_sql(count: usize) -> String {
        let mut placeholders = String::new();
        for row in 0..count {
            if row > 0 {
                placeholders.push_str(", ");
            }
            let offset = row * 3;
            placeholders.push_str(&format!(
                "(?{}, ?{}, ?{})",
                offset + 1,
                offset + 2,
                offset + 3
            ));
        }
        format!(
            "INSERT OR IGNORE INTO Parents (tag_id, relate_tag_id, limit_to) VALUES {placeholders}"
        )
    }

    /// Binds the three columns, with `limit_to` in whichever representation the
    /// layout stores. `Current` keeps SQL `NULL`, which its `IFNULL(limit_to, -1)`
    /// unique index folds into the same key the sentinel layouts store directly;
    /// binding `-1` there instead would be a different key and would let both
    /// forms of the same edge coexist.
    fn layout_params(layout: ParentLayout, rows: &[TagParents]) -> Vec<turso::Value> {
        let sentinel = !matches!(
            layout,
            ParentLayout::Current | ParentLayout::CurrentCover | ParentLayout::FloorNoIndex
        );
        rows.iter()
            .flat_map(|parent| {
                [
                    turso::Value::from(parent.tag_id as i64),
                    turso::Value::from(parent.relate_tag_id as i64),
                    match parent.limit_to {
                        Some(limit_id) => turso::Value::from(limit_id as i64),
                        None if sentinel => turso::Value::from(-1i64),
                        None => turso::Value::Null,
                    },
                ]
            })
            .collect()
    }

    async fn layout_read_tag(conn: &Connection, layout: ParentLayout, tag_id: u64) -> u64 {
        let sql = match layout {
            ParentLayout::SplitSeq => {
                "SELECT p.relate_tag_id, l.limit_to FROM Parents p
                 LEFT JOIN Parents_limit l ON l.seq = p.seq
                 WHERE p.tag_id = ?1"
            }
            _ => "SELECT relate_tag_id, limit_to FROM Parents WHERE tag_id = ?1",
        };
        count_rows(conn, sql, (tag_id as i64,)).await
    }

    async fn layout_read_relate(conn: &Connection, layout: ParentLayout, relate: u64) -> u64 {
        let sql = match layout {
            ParentLayout::SplitSeq => {
                "SELECT p.tag_id, l.limit_to FROM Parents p
                 LEFT JOIN Parents_limit l ON l.seq = p.seq
                 WHERE p.relate_tag_id = ?1"
            }
            _ => "SELECT tag_id, limit_to FROM Parents WHERE relate_tag_id = ?1",
        };
        count_rows(conn, sql, (relate as i64,)).await
    }

    async fn layout_read_pair(
        conn: &Connection,
        layout: ParentLayout,
        tag_id: u64,
        relate: u64,
    ) -> u64 {
        let sql = match layout {
            ParentLayout::SplitSeq => {
                "SELECT l.limit_to FROM Parents p
                 LEFT JOIN Parents_limit l ON l.seq = p.seq
                 WHERE p.tag_id = ?1 AND p.relate_tag_id = ?2"
            }
            _ => "SELECT limit_to FROM Parents WHERE tag_id = ?1 AND relate_tag_id = ?2",
        };
        count_rows(conn, sql, (tag_id as i64, relate as i64)).await
    }

    async fn count_rows<P>(conn: &Connection, sql: &str, params: P) -> u64
    where
        P: turso::IntoParams,
    {
        let mut rows = conn.query(sql, params).await.unwrap();
        let mut count = 0u64;
        while rows.next().await.unwrap().is_some() {
            count += 1;
        }
        count
    }

    /// `parent_relationships_get_many` / `child_relationships_get_many` against
    /// a database that actually holds `BENCH_EDGES` edges, comparing the
    /// per-tag statement loop that ships today against the two batched forms.
    ///
    /// The loop is the interesting part: a caller asking about *N* tags pays *N*
    /// statements, so the cost of "get me the parents of these tags" scales with
    /// the caller's set size rather than with the data. That is exactly the shape
    /// `agents.md` rules out ("do not perform one SQL query per item"), and it is
    /// invisible in the insert benchmarks. Two batched forms are measured:
    ///
    /// 1. `IN (...)` chunks — one statement per chunk of tag ids.
    /// 2. Coalesced ranges — one statement per span of tag ids, which is
    ///    cheaper in statements but returns rows for tags nobody asked about.
    ///
    /// The caller asks for the *same* tags either way, so correctness is
    /// asserted by comparing each batched result against the loop's.
    #[test]
    #[ignore = "manual benchmark: Parents get_many, per-tag loop vs batched"]
    fn db_bench_parents_read_many() {
        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let edges = env_u64("BENCH_EDGES", 100_000);
        let rounds = env_u64("BENCH_ITERS", 3).max(1);
        let wanted = env_u64("BENCH_WANTED", 500).max(1).min(tags);
        let in_chunk = env_u64("BENCH_IN_CHUNK", SQL_CHUNK_SIZE as u64).max(1);
        let span = env_u64("BENCH_SPAN_TAGS", 64).max(1);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let batch: Vec<TagParents> = dedupe_sorted(
            (0..edges)
                .map(|i| TagParents {
                    tag_id: 1 + (i % tags),
                    relate_tag_id: 1 + (i / 3 + 7) % tags,
                    limit_to: if i.is_multiple_of(3) {
                        Some(1 + (i + 11) % tags)
                    } else {
                        None
                    },
                })
                .collect(),
        );
        // The tags a caller would realistically ask about: the first `wanted`
        // distinct tag ids in the batch.
        let mut asked: Vec<u64> = batch.iter().map(|parent| parent.tag_id).collect();
        asked.sort_unstable();
        asked.dedup();
        asked.truncate(wanted as usize);
        let asked_set: HashSet<u64> = asked.iter().copied().collect();

        println!(
            "\nBENCH Parents get_many: {} edges, {} tags, asking about {} tags, {} rounds",
            batch.len(),
            tags,
            asked.len(),
            rounds
        );

        rt.block_on(async {
            let path = bench_db_path("read_many");
            bench_remove(&path);
            let db = TursoDatabase::new_with_exit(&path, Arc::new(AtomicBool::new(false))).await;
            let conn = db.connect().unwrap();
            insert_slice(&conn, &batch.iter().collect::<Vec<_>>()).await;

            // Ground truth, and the loop cost.
            let t = Instant::now();
            let mut expected: HashMap<u64, Vec<TagParents>> = HashMap::new();
            for _ in 0..rounds {
                expected = db
                    .parent_relationships_get_many(&conn, &asked_set)
                    .await
                    .unwrap();
            }
            let per_tag = t.elapsed();
            report(
                "per-tag loop (shipped)",
                asked.len() as u64,
                rounds,
                per_tag,
            );
            assert_eq!(expected.len(), asked.len(), "every asked tag must resolve");

            // Batched `IN (...)`.
            let t = Instant::now();
            for _ in 0..rounds {
                let grouped = read_many_in(&conn, &asked, in_chunk as usize).await;
                assert_eq!(
                    grouped, expected,
                    "the batched IN form must return exactly what the loop returns"
                );
            }
            let batched_in = t.elapsed();
            report(
                &format!("batched IN (chunks of {in_chunk})"),
                asked.len() as u64,
                rounds,
                batched_in,
            );

            // Coalesced tag-id ranges.
            let t = Instant::now();
            for _ in 0..rounds {
                let grouped = read_many_spans(&conn, &asked, span as usize).await;
                let only_asked: HashMap<u64, Vec<TagParents>> = grouped
                    .into_iter()
                    .filter(|(tag, _)| asked_set.contains(tag))
                    .collect();
                assert_eq!(
                    only_asked, expected,
                    "the span form must return exactly what the loop returns"
                );
            }
            let batched_span = t.elapsed();
            report(
                &format!("coalesced ranges (span {span})"),
                asked.len() as u64,
                rounds,
                batched_span,
            );

            println!(
                "    IN/loop = {:.2}x   spans/loop = {:.2}x",
                batched_in.as_secs_f64() / per_tag.as_secs_f64().max(f64::MIN_POSITIVE),
                batched_span.as_secs_f64() / per_tag.as_secs_f64().max(f64::MIN_POSITIVE)
            );

            db.shutdown().await;
            drop(db);
            bench_remove(&path);
        });
    }

    /// One `IN (...)` statement per chunk of tag ids, grouped back into the same
    /// `HashMap<u64, Vec<TagParents>>` the per-tag loop returns so the two are
    /// directly comparable.
    async fn read_many_in(
        conn: &Connection,
        tag_ids: &[u64],
        chunk: usize,
    ) -> HashMap<u64, Vec<TagParents>> {
        let mut out: HashMap<u64, Vec<TagParents>> = HashMap::new();
        for window in tag_ids.chunks(chunk) {
            let placeholders = std::iter::repeat_n("?", window.len())
                .collect::<Vec<_>>()
                .join(", ");
            let params: Vec<turso::Value> = window
                .iter()
                .map(|id| turso::Value::from(*id as i64))
                .collect();
            let mut rows = conn
                .query(
                    format!(
                        "SELECT tag_id, relate_tag_id, limit_to FROM Parents \
                         WHERE tag_id IN ({placeholders})"
                    ),
                    turso::params_from_iter(params),
                )
                .await
                .unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                let parent = TagParents {
                    tag_id: row.get(0).unwrap(),
                    relate_tag_id: row.get(1).unwrap(),
                    limit_to: row.get(2).unwrap(),
                };
                out.entry(parent.tag_id).or_default().push(parent);
            }
        }
        for rows in out.values_mut() {
            rows.sort_unstable_by_key(parents_key);
        }
        out
    }

    /// One range statement per span of `span` tag ids. Cheaper in statements
    /// than `IN (...)` when the asked-for tags are dense, at the cost of
    /// returning rows for tags in the gaps.
    async fn read_many_spans(
        conn: &Connection,
        tag_ids: &[u64],
        span: usize,
    ) -> HashMap<u64, Vec<TagParents>> {
        let mut sorted = tag_ids.to_vec();
        sorted.sort_unstable();
        let mut out: HashMap<u64, Vec<TagParents>> = HashMap::new();
        let mut start = 0;
        while start < sorted.len() {
            let first = sorted[start];
            let mut end = start;
            while end < sorted.len() && sorted[end] < first + span as u64 {
                end += 1;
            }
            let last = sorted[end - 1];
            let mut rows = conn
                .query(
                    "SELECT tag_id, relate_tag_id, limit_to FROM Parents \
                     WHERE tag_id >= ?1 AND tag_id <= ?2",
                    (first as i64, last as i64),
                )
                .await
                .unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                let parent = TagParents {
                    tag_id: row.get(0).unwrap(),
                    relate_tag_id: row.get(1).unwrap(),
                    limit_to: row.get(2).unwrap(),
                };
                out.entry(parent.tag_id).or_default().push(parent);
            }
            start = end;
        }
        for rows in out.values_mut() {
            rows.sort_unstable_by_key(parents_key);
        }
        out
    }

    /// Job-queue write benchmark, since job scheduling is on the critical path
    /// for every crawl.
    #[test]
    #[ignore = "manual benchmark: job queue bulk add"]
    fn db_bench_job_queue() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let path = bench_db_path("jobs");
        bench_remove(&path);
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = rt.block_on(TursoDatabase::new_with_exit(&path, should_exit));
        println!("\nBENCH job queue: {}", path.display());

        let jobs: Vec<PluginJob> = (0..SQL_CHUNK_SIZE as u64)
            .map(|i| PluginJob {
                time: 1_700_000_000,
                reptime: 0,
                priority: i % 11,
                recreation: None,
                site: "bench-site".to_string(),
                param: vec![ScraperParam::Normal(format!("bench-term-{i}"))],
                user_data: Default::default(),
            })
            .collect();

        rt.block_on(async {
            let conn = db.connect().unwrap();

            println!("  -- jobs_bulk_add_sql, {} jobs --", jobs.len());
            let t = Instant::now();
            db.jobs_bulk_add_sql(&conn, &jobs).await.unwrap();
            report("jobs_bulk_add_sql NOVEL", jobs.len() as u64, 1, t.elapsed());

            // `ON CONFLICT ... DO UPDATE`, so a repeat is an update not an
            // insert: this is the crawl-dedup path.
            let t = Instant::now();
            db.jobs_bulk_add_sql(&conn, &jobs).await.unwrap();
            report(
                "jobs_bulk_add_sql RE-ADD",
                jobs.len() as u64,
                1,
                t.elapsed(),
            );

            let t = Instant::now();
            let all = db.jobs_get_all_sql(&conn).await.unwrap();
            let elapsed = t.elapsed();
            assert_eq!(all.len(), jobs.len());
            println!(
                "  {:<44} {:>9.2} ms       (UNBOUNDED: {} jobs in memory)",
                "jobs_get_all_sql (whole queue)",
                elapsed.as_secs_f64() * 1_000.0,
                all.len()
            );

            let t = Instant::now();
            let sites = db.jobs_get_all_sites_sql(&conn).await.unwrap();
            let sites_elapsed = t.elapsed();
            // Reported as a whole-statement cost: a "per-row" figure over a
            // single result row would be the statement cost mislabelled.
            println!(
                "  {:<44} {:>9.2} ms       (one statement, {} site(s))",
                "jobs_get_all_sites_sql",
                sites_elapsed.as_secs_f64() * 1_000.0,
                sites.len()
            );
        });

        if std::env::var("BENCH_KEEP").is_err() && std::env::var("BENCH_PATH").is_err() {
            bench_remove(&path);
        }
    }

    /// Indexable sort key for an encoded row value; `NULL` limit_to and the
    /// storage `-1` sentinel share one key so every variant sorts identically.
    fn index_key(value: &Value) -> i64 {
        match value {
            Value::Integer(integer) => *integer,
            Value::Null => -1,
            _ => i64::MIN,
        }
    }

    /// Prints `EXPLAIN QUERY PLAN` for every `Parents` statement the current
    /// storage shape serves, on a real production-schema database.
    ///
    /// Run anytime a `Parents` index changes. The expected plans:
    /// `SEARCH Parents USING COVERING INDEX idx_unique_parents` for forward
    /// lists and exact probes, and `... COVERING INDEX idx_parents_rel` for
    /// reverse reads. Any `SCAN Parents` on a hot path means an index stopped
    /// covering.
    #[test]
    #[ignore = "manual: print EXPLAIN QUERY PLAN for the Parents statements"]
    fn db_show_parents_query_plans() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = bench_db_path("parents_eqp");
            bench_remove(&path);
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&path, should_exit).await;
            let conn = db.connect().unwrap();

            let sqls: [(&str, &str); 7] = [
                (
                    "forward list (parent_relationships_get)",
                    "SELECT relate_tag_id, limit_to FROM Parents WHERE tag_id = 1",
                ),
                (
                    "reverse list (child_relationships_get)",
                    "SELECT tag_id, limit_to FROM Parents WHERE relate_tag_id = 1",
                ),
                (
                    "exact edge (parent_relationship_get)",
                    "SELECT 1 FROM Parents WHERE tag_id = 1 AND relate_tag_id = 2 LIMIT 1",
                ),
                (
                    "structure skip-check (parent_structure_exists)",
                    "SELECT 1 FROM Parents WHERE tag_id = 1 AND relate_tag_id = 2 \
                     AND limit_to = IFNULL(3, -1) LIMIT 1",
                ),
                (
                    "relate+limit skip-check (parent_relate_limit_exists)",
                    "SELECT 1 FROM Parents WHERE relate_tag_id = 1 AND limit_to = 2 LIMIT 1",
                ),
                (
                    "slurp copy (dedupe + sentinel fold)",
                    "INSERT INTO Parents (tag_id, relate_tag_id, limit_to) \
                     SELECT tag_id, relate_tag_id, IFNULL(limit_to, -1) FROM Parents_old \
                     GROUP BY tag_id, relate_tag_id, IFNULL(limit_to, -1)",
                ),
                (
                    "slurp-source whole-table read",
                    "SELECT tag_id, relate_tag_id, limit_to FROM Parents",
                ),
            ];

            println!("\nEQP Parents statements (production schema)");
            // The slurp-copy statement references Parents_old, which only
            // exists mid-slurp; shape it as the migration leaves it (pre-v10,
            // still nullable).
            conn.execute_batch(
                "CREATE TABLE Parents_old (
                     id INTEGER PRIMARY KEY,
                     tag_id INTEGER NOT NULL,
                     relate_tag_id INTEGER NOT NULL,
                     limit_to INTEGER);",
            )
            .await
            .unwrap();
            for (label, sql) in sqls {
                let explain = format!("EXPLAIN QUERY PLAN {sql};");
                let mut rows = conn.query(&explain, ()).await.unwrap();
                println!("  [{label}]");
                while let Some(row) = rows.next().await.unwrap() {
                    let detail: String = row.get(3).unwrap();
                    println!("    {}", detail.trim());
                }
            }

            db.shutdown().await;
        });
    }

    /// Measures the cost of the per-call `db.connect()` pattern: every
    /// read-side DB helper opens a fresh connection (`file_id_get`,
    /// `file_download_location_get_sync`, `dead_url_get`, ...), each of which
    /// builds a fresh schema view, versus one reused connection for N probes.
    ///
    /// Run whenever connection churn comes into suspicion: if fresh-connect
    /// dominates reused-probe cost by ~10x, the per-call pattern is a real
    /// path cost; if it is near-parity, the churn is noise and the suspicion
    /// can be dropped.
    #[test]
    #[ignore = "manual benchmark: per-call db.connect() vs reused connection"]
    fn db_bench_connect_cost() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = bench_db_path("connect_cost");
            bench_remove(&path);
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&path, should_exit).await;
            let conn_warm = db.connect().unwrap();
            // Touch the schema once so the first-query cost is not baked into
            // round one.
            conn_warm
                .query("SELECT COUNT(*) FROM sqlite_schema;", ())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap();
            drop(conn_warm);

            const PROBES: u64 = 500;
            let rounds = std::env::var("BENCH_ITERS")
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(3)
                .max(1);

            for _round in 0..rounds {
                // Pattern A: the per-call connect every read-side helper does.
                let t = Instant::now();
                for probe in 0..PROBES {
                    std::hint::black_box(probe);
                    let conn = db.connect().unwrap();
                    let mut rows = conn.query("SELECT 1;", ()).await.unwrap();
                    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap();
                }
                let fresh = t.elapsed();
                println!(
                    "BENCH connect: fresh connect+select 1, {PROBES} per round: \
                     {fresh:?} -> {} us each",
                    fresh.as_micros() / PROBES as u128
                );
                std::hint::black_box(fresh);

                // Pattern B: one reused connection, N parameterized probes.
                let conn = db.connect().unwrap();
                let t = Instant::now();
                for probe in 0..PROBES {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM sqlite_schema WHERE name = ?1;",
                            (format!("x{probe}"),),
                        )
                        .await
                        .unwrap();
                    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap();
                }
                let reuse = t.elapsed();
                println!(
                    "BENCH reuse:   N probes on 1 connection, {PROBES} per round: \
                     {reuse:?} -> {} us each",
                    reuse.as_micros() / PROBES as u128
                );
                std::hint::black_box(reuse);
            }

            db.shutdown().await;
            bench_remove(&path);
        });
    }

    /// Compares full `Parents` storage designs on both the write and the read
    /// paths, each in its own database.
    ///
    /// `db_bench_parents_schema_variants` showed the current *index set* is within
    /// noise of the no-index floor on novel inserts in release mode, so any further
    /// write win has to come from a different shape rather than from one less
    /// index. Note the engine constraint this bench documents: turso 0.8.2 rejects
    /// `CREATE INDEX` on `WITHOUT ROWID` tables, so a secondary index is only
    /// available on a rowid table and every direction an index serves has to keep
    /// the rowid table around.
    /// Candidates:
    ///
    /// - `A` — current: rowid table served by `idx_unique_parents_null_safe`
    ///   `(tag_id, relate_tag_id, IFNULL(limit_to, -1))` plus `idx_parents_rel`
    ///   `(relate_tag_id)`. Three b-trees; the expression key costs an `IFNULL`
    ///   evaluation per insert, and reverse reads resolve through the rowid.
    /// - `R` — same rowid table, reorganized to serve reads from the indexes
    ///   themselves: `limit_to` stored NOT NULL with a `-1` NULL sentinel so the
    ///   dedupe key is a plain composite index, and the reverse index made fully
    ///   covering `(relate_tag_id, tag_id, limit_to)` so reverse reads never touch
    ///   the rowid. Same three b-trees — this is a coverage and expression-cost
    ///   change, not an index-count change.
    /// - `F` — rowid table with the plain composite unique index only. Not
    ///   production material (reverse reads would be a full scan); it bounds what
    ///   the reverse index buys, the same role `E` plays in the insert-side sweep.
    ///
    /// Measured per variant: novel insert, re-add (pure conflict), forward
    /// listing for `BENCH_PARENT_PROBES` child ids one statement at a time, the
    /// same ids gathered by one `IN (...)` statement, reverse listing for parent
    /// ids, exact triple probes alternating present/absent keys, and a whole-table
    /// streaming scan (the slurp importer runs one).
    #[test]
    #[ignore = "manual benchmark: Parents storage designs (write + read paths)"]
    fn db_bench_parents_store() {
        async fn insert_rows(
            conn: &Connection,
            rows: &[Vec<Value>],
            insert_prefix: &str,
            rounds: u64,
        ) -> (Duration, u64) {
            let mut written = 0u64;
            let t = Instant::now();
            for _ in 0..rounds {
                for chunk in rows.chunks(SQL_CHUNK_SIZE) {
                    let placeholders = std::iter::repeat_n("(?,?,?)", chunk.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!("{insert_prefix} VALUES {placeholders}");
                    let flat: Vec<Value> = chunk.iter().flatten().cloned().collect();
                    conn.execute(&sql, turso::params_from_iter(flat))
                        .await
                        .unwrap();
                    written += chunk.len() as u64;
                }
            }
            (t.elapsed(), written)
        }

        async fn list_for_ids(
            conn: &Connection,
            sql: &str,
            ids: &[u64],
            rounds: u64,
        ) -> (Duration, u64) {
            let t = Instant::now();
            let mut fetched = 0u64;
            for _ in 0..rounds {
                for id in ids {
                    let mut rows = conn.query(sql, (Value::from(*id as i64),)).await.unwrap();
                    while let Some(row) = rows.next().await.unwrap() {
                        let _: i64 = row.get(0).unwrap();
                        let _: Option<i64> = row.get(1).unwrap();
                        fetched += 1;
                    }
                }
            }
            (t.elapsed(), fetched)
        }

        let tags = env_u64("BENCH_TAGS", DEFAULT_TAGS);
        let edges = env_u64("BENCH_PARENT_EDGES", 20_000);
        let probes = env_u64("BENCH_PARENT_PROBES", 200).max(1);
        let rounds = env_u64("BENCH_ITERS", 3).max(1);
        let rt = tokio::runtime::Runtime::new().unwrap();

        println!("\nBENCH Parents store: {edges} edges, {probes} probe ids, fresh db per variant");

        // Clustering on purpose: one edge in four extends the previous child, so
        // probe lists have realistic fan-out and the b-trees get locality worth
        // exploiting — the difference between "stored in child order" and random
        // order is invisible otherwise.
        let make_edges = || -> Vec<TagParents> {
            (0..edges)
                .map(|i| TagParents {
                    tag_id: 1 + (i / 4 % tags),
                    relate_tag_id: 1 + ((i / 3 + 11) % tags),
                    limit_to: if i.is_multiple_of(3) {
                        Some(1 + ((i + 17) % tags))
                    } else {
                        None
                    },
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        };

        /// Physical encoding of one edge, plus every statement the read paths need.
        struct Motor {
            label: &'static str,
            /// `INSERT OR IGNORE INTO pvar ...` including the column list.
            insert_prefix: &'static str,
            encode: fn(&TagParents) -> Vec<Value>,
            forward_sql: &'static str,
            reverse_sql: &'static str,
            exact_sql: &'static str,
            /// Aggregates enough columns that the scan cannot be optimized away.
            sum_sql: &'static str,
        }

        let motors: Vec<Motor> = vec![
            Motor {
                label: "A current (rowid + expr unique + rel)",
                insert_prefix: "INSERT OR IGNORE INTO pvar (tag_id, relate_tag_id, limit_to)",
                encode: |edge| {
                    vec![
                        Value::from(edge.tag_id as i64),
                        Value::from(edge.relate_tag_id as i64),
                        edge.limit_to
                            .map(|id| Value::from(id as i64))
                            .unwrap_or(Value::Null),
                    ]
                },
                forward_sql: "SELECT relate_tag_id, limit_to FROM pvar WHERE tag_id = ?1",
                reverse_sql: "SELECT tag_id, limit_to FROM pvar WHERE relate_tag_id = ?1",
                exact_sql: "SELECT 1 FROM pvar WHERE relate_tag_id = ?1 AND tag_id = ?2 \
                            AND ((limit_to IS NULL AND ?3 IS NULL) OR limit_to = ?3) LIMIT 1",
                sum_sql: "SELECT COUNT(tag_id), SUM(relate_tag_id), COUNT(limit_to) FROM pvar",
            },
            Motor {
                label: "R rowid, plain unique + covering rel",
                insert_prefix: "INSERT OR IGNORE INTO pvar (tag_id, relate_tag_id, limit_to)",
                encode: |edge| {
                    vec![
                        Value::from(edge.tag_id as i64),
                        Value::from(edge.relate_tag_id as i64),
                        // NULL becomes the -1 sentinel; tag ids start at 1.
                        Value::from(edge.limit_to.map(|id| id as i64).unwrap_or(-1)),
                    ]
                },
                forward_sql: "SELECT relate_tag_id, limit_to FROM pvar WHERE tag_id = ?1",
                reverse_sql: "SELECT tag_id, limit_to FROM pvar WHERE relate_tag_id = ?1",
                exact_sql: "SELECT 1 FROM pvar WHERE relate_tag_id = ?1 AND tag_id = ?2 \
                            AND limit_to = ?3 LIMIT 1",
                sum_sql: "SELECT COUNT(tag_id), SUM(relate_tag_id), COUNT(limit_to) FROM pvar",
            },
            Motor {
                label: "F plain unique only (floor)",
                insert_prefix: "INSERT OR IGNORE INTO pvar (tag_id, relate_tag_id, limit_to)",
                encode: |edge| {
                    vec![
                        Value::from(edge.tag_id as i64),
                        Value::from(edge.relate_tag_id as i64),
                        Value::from(edge.limit_to.map(|id| id as i64).unwrap_or(-1)),
                    ]
                },
                forward_sql: "SELECT relate_tag_id, limit_to FROM pvar WHERE tag_id = ?1",
                // No reverse index exists in the floor variant: this statement is
                // a full scan, printed to bound what the other variants buy.
                reverse_sql: "SELECT tag_id, limit_to FROM pvar WHERE relate_tag_id = ?1",
                exact_sql: "SELECT 1 FROM pvar WHERE relate_tag_id = ?1 AND tag_id = ?2 \
                            AND limit_to = ?3 LIMIT 1",
                sum_sql: "SELECT COUNT(tag_id), SUM(relate_tag_id), COUNT(limit_to) FROM pvar",
            },
        ];

        rt.block_on(async {
            let batch: Vec<TagParents> = make_edges();
            println!("  {} distinct edges from {} generated", batch.len(), edges);

            let mut child_ids: Vec<u64> = batch.iter().map(|edge| edge.tag_id).collect();
            child_ids.sort_unstable();
            child_ids.dedup();
            let mut parent_ids: Vec<u64> = batch.iter().map(|edge| edge.relate_tag_id).collect();
            parent_ids.sort_unstable();
            parent_ids.dedup();

            struct Results {
                novel: Duration,
                readd: Duration,
                forward: Duration,
                batched: Duration,
                reverse: Duration,
                exact: Duration,
                scan: Duration,
            }

            let mut measured: Vec<(&str, Results)> = Vec::new();

            for motor in &motors {
                let variant_path = bench_db_path("parents_store");
                bench_remove(&variant_path);
                // The flag is a no-op for rowid DDL, so every variant is opened
                // through the same builder on purpose: the only variable under
                // test is the storage shape, never the connection options.
                let db = Builder::new_local(&variant_path.to_string_lossy())
                    .experimental_without_rowid(true)
                    .build()
                    .await
                    .unwrap();
                let conn = db.connect().unwrap();
                conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();

                let ddl = match motor.label.split_whitespace().next().unwrap() {
                    "A" => {
                        "CREATE TABLE pvar (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                         relate_tag_id INTEGER NOT NULL, limit_to INTEGER, \
                         CHECK (tag_id != relate_tag_id)); \
                         CREATE UNIQUE INDEX pvar_u \
                             ON pvar (tag_id, relate_tag_id, IFNULL(limit_to, -1)); \
                         CREATE INDEX pvar_r ON pvar (relate_tag_id);"
                    }
                    "R" => {
                        "CREATE TABLE pvar (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                         relate_tag_id INTEGER NOT NULL, limit_to INTEGER NOT NULL DEFAULT -1, \
                         CHECK (tag_id != relate_tag_id)); \
                         CREATE UNIQUE INDEX pvar_u \
                             ON pvar (tag_id, relate_tag_id, limit_to); \
                         CREATE INDEX pvar_r ON pvar (relate_tag_id, tag_id, limit_to);"
                    }
                    _ => {
                        "CREATE TABLE pvar (id INTEGER PRIMARY KEY, tag_id INTEGER NOT NULL, \
                         relate_tag_id INTEGER NOT NULL, limit_to INTEGER NOT NULL DEFAULT -1, \
                         CHECK (tag_id != relate_tag_id)); \
                         CREATE UNIQUE INDEX pvar_u ON pvar (tag_id, relate_tag_id, limit_to);"
                    }
                };
                conn.execute_batch(ddl).await.unwrap();

                // Encode and sort the way `parents_bulk_add` feeds its chunks: in
                // row-key order, so the index is extended at its right edge.
                let mut rows: Vec<Vec<Value>> = batch.iter().map(motor.encode).collect();
                rows.sort_unstable_by_key(|row| {
                    (index_key(&row[0]), index_key(&row[1]), index_key(&row[2]))
                });

                let (novel, novel_written) =
                    insert_rows(&conn, &rows, motor.insert_prefix, 1).await;
                let (readd, readd_written) =
                    insert_rows(&conn, &rows, motor.insert_prefix, rounds).await;
                assert_eq!(novel_written, batch.len() as u64, "novel insert lost rows");
                assert_eq!(
                    readd_written,
                    (batch.len() as u64) * rounds,
                    "re-add lost rows"
                );

                let (forward, forward_rows) =
                    list_for_ids(&conn, motor.forward_sql, &child_ids, rounds).await;
                let (reverse, reverse_rows) =
                    list_for_ids(&conn, motor.reverse_sql, &parent_ids, rounds).await;

                // One statement gathering the same children the per-id loop
                // walked: what batching alone buys on the existing schema.
                let batched = {
                    let t = Instant::now();
                    for _ in 0..rounds {
                        for window in child_ids.chunks(2000) {
                            let placeholders = std::iter::repeat_n("?", window.len())
                                .collect::<Vec<_>>()
                                .join(",");
                            let params: Vec<Value> =
                                window.iter().map(|id| Value::from(*id as i64)).collect();
                            let sql = format!(
                                "SELECT tag_id, relate_tag_id, limit_to FROM pvar \
                                 WHERE tag_id IN ({placeholders})"
                            );
                            let mut rows = conn.query(&sql, params).await.unwrap();
                            while let Some(row) = rows.next().await.unwrap() {
                                let _: i64 = row.get(0).unwrap();
                                let _: i64 = row.get(1).unwrap();
                                let _: Option<i64> = row.get(2).unwrap();
                            }
                        }
                    }
                    t.elapsed()
                };

                // Exact triple probe, half present and half absent keys. The
                // present keys carry each variant's row limit encoding: `A`
                // keeps NULL, the sentinels store -1, so the probe parameter is
                // adapted the same way `parent_structure_exists` does.
                let mut exact_keys: Vec<(i64, i64, i64)> = Vec::with_capacity(probes as usize);
                for edge in &batch {
                    if exact_keys.len() >= probes as usize {
                        break;
                    }
                    exact_keys.push((
                        edge.relate_tag_id as i64,
                        edge.tag_id as i64,
                        match motor.label.starts_with('A') {
                            true => edge.limit_to.unwrap_or(0) as i64,
                            false => edge.limit_to.map(|id| id as i64).unwrap_or(-1),
                        },
                    ));
                }
                let exact = {
                    let t = Instant::now();
                    let mut hits = 0u64;
                    for _ in 0..rounds {
                        for (index, (parent, child, limit)) in exact_keys.iter().enumerate() {
                            let limit_value = match motor.label.starts_with('A') && index % 2 == 0 {
                                true => match *limit {
                                    0 => Value::Null,
                                    id => Value::from(id),
                                },
                                false => Value::from(*limit),
                            };
                            let mut rows = conn
                                .query(
                                    motor.exact_sql,
                                    (Value::from(*parent), Value::from(*child), limit_value),
                                )
                                .await
                                .unwrap();
                            if rows.next().await.unwrap().is_some() {
                                hits += 1;
                            }
                        }
                    }
                    println!("    {hits} probes resolved a row");
                    t.elapsed()
                };

                // Whole-table streaming scan, slurp-importer shape.
                let scan = {
                    let t = Instant::now();
                    for _ in 0..rounds {
                        let mut rows = conn.query(motor.sum_sql, ()).await.unwrap();
                        while let Some(row) = rows.next().await.unwrap() {
                            let _: i64 = row.get(0).unwrap();
                            let _: Option<i64> = row.get(1).unwrap();
                            let _: i64 = row.get(2).unwrap();
                        }
                    }
                    t.elapsed()
                };

                println!(
                    "  {}: novel {novel_written} rows, re-add {readd_written}, \
                     forward rows {forward_rows}, reverse rows {reverse_rows}",
                    motor.label
                );
                measured.push((
                    motor.label,
                    Results {
                        novel,
                        readd,
                        forward,
                        batched,
                        reverse,
                        exact,
                        scan,
                    },
                ));

                drop(db);
                bench_remove(&variant_path);
            }

            let baseline = &measured[0].1;
            println!(
                "\n  {:<38} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
                "", "novel us/r", "re-add", "fwd us/q", "rev us/q", "batch us/q", "exact us/q"
            );
            for (label, r) in &measured {
                println!(
                    "  {label:<38} {:>10.2} {:>10.2} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
                    r.novel.as_secs_f64() * 1_000_000.0 / batch.len() as f64,
                    r.readd.as_secs_f64() * 1_000_000.0 / (batch.len() * rounds as usize) as f64,
                    r.forward.as_secs_f64() * 1_000_000.0
                        / (child_ids.len() * rounds as usize) as f64,
                    r.reverse.as_secs_f64() * 1_000_000.0
                        / (parent_ids.len() * rounds as usize) as f64,
                    r.batched.as_secs_f64() * 1_000_000.0
                        / (child_ids.len() * rounds as usize) as f64,
                    r.exact.as_secs_f64() * 1_000_000.0 / (probes * rounds) as f64,
                );
            }
            println!("\n  Whole-table scan per iteration:");
            for (label, r) in &measured {
                println!(
                    "    {label:<38} {:>9.2} ms   ({:.2}x baseline)",
                    r.scan.as_secs_f64() * 1_000.0 / rounds as f64,
                    r.scan.as_nanos() as f64 / baseline.scan.as_nanos().max(1) as f64,
                );
            }
        });
    }

    /// The shape of a relationship table, measured across every path the
    /// migration would touch.
    ///
    /// Two variables, four combinations:
    ///
    /// * **Partitioned** (today): one `Relationship_{ns}` table per namespace,
    ///   so a lookup that can touch any namespace has to `UNION ALL` every
    ///   partition, with the predicate repeated per arm. **Single**: one
    ///   `Relationship` table.
    /// * **Index shape**: `idx(file_id)` (today -- the plan says `USING INDEX`,
    ///   not `USING COVERING INDEX`, so every row pays a table b-tree hop) or
    ///   `idx(file_id, tag_id)`.
    ///
    /// Every phase runs against the same fixture, in the order a scraper
    /// would exercise them: read, write, delete. The read phase batches
    /// exactly the way production does (`PUSHED_UNION_PARAM_BUDGET / arms`
    /// ids per statement for the partitioned shape, `SQL_CHUNK_SIZE` for the
    /// single table), so the statement *count* is part of the measurement
    /// rather than something the comparison papers over.
    ///
    /// ```text
    /// BENCH_ARMS=200 BENCH_FILES=200000 cargo test --lib \
    ///     db::turso::bench::tests::db_bench_relationship_shape -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: relationship partition and index shape"]
    fn db_bench_relationship_shape() {
        let arms = env_u64("BENCH_ARMS", 64).max(1) as usize;
        let files = env_u64("BENCH_FILES", 20_000);
        let rels_per_file = env_u64("BENCH_RELS_PER_FILE", 4);
        let ids_per_query = env_u64("BENCH_IDS", 64).max(1) as usize;
        let iters = env_u64("BENCH_ITERS", 5).max(1);
        let write_files = env_u64("BENCH_WRITE_FILES", 2_000);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH relationship shape: {arms} arms, {files} files, \
                 {rels_per_file} rels/file, {ids_per_query} ids/query"
            );

            // `partitioned` picks the storage shape; `covering` adds `tag_id`
            // to the file_id index so the lookup can be served from the index
            // alone.
            let variants: [(&str, bool, bool); 4] = [
                ("A partitioned, idx(file_id)", true, false),
                ("B partitioned, idx(file_id,tag_id)", true, true),
                ("C single table, idx(file_id)", false, false),
                ("D single table, idx(file_id,tag_id)", false, true),
            ];

            for (label, partitioned, covering) in variants {
                let path = bench_db_path(&format!("relshape-{}", label.chars().next().unwrap()));
                bench_remove(&path);
                let db = Builder::new_local(&path.to_string_lossy())
                    .build()
                    .await
                    .unwrap();
                let conn = db.connect().unwrap();
                conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();

                // The table a row belongs to. A partition is chosen by the
                // tag's namespace, exactly as `relationships_bulk_add` picks
                // it, so a tag's rows never straddle two tables.
                let table_for = |tag_id: u64| -> String {
                    if partitioned {
                        format!("Relationship_{}", tag_id % arms as u64)
                    } else {
                        "Relationship".to_string()
                    }
                };

                let index_cols = if covering {
                    "file_id, tag_id"
                } else {
                    "file_id"
                };
                let mut ddl = String::new();
                if partitioned {
                    for arm in 0..arms {
                        ddl.push_str(&format!(
                            "CREATE TABLE Relationship_{arm} (
                                 file_id INTEGER NOT NULL,
                                 tag_id INTEGER NOT NULL,
                                 PRIMARY KEY (tag_id, file_id));
                             CREATE INDEX idx_Relationship_{arm}_tag_file
                                 ON Relationship_{arm} ({index_cols});"
                        ));
                    }
                } else {
                    ddl.push_str(&format!(
                        "CREATE TABLE Relationship (
                             file_id INTEGER NOT NULL,
                             tag_id INTEGER NOT NULL,
                             PRIMARY KEY (tag_id, file_id));
                         CREATE INDEX idx_Relationship_file_tag
                             ON Relationship ({index_cols});"
                    ));
                }
                conn.execute_batch(&ddl).await.unwrap();

                // Seed. Rows are grouped per table and sorted by
                // (tag_id, file_id) so each b-tree is extended at its right
                // edge, the way `relationships_bulk_add` feeds it. The insert
                // phase is timed too: partitioning is supposed to buy
                // write-side b-tree locality, and that claim has to be checked
                // against the same fixture, not assumed.
                let mut per_table: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
                for file in 0..files {
                    for rel in 0..rels_per_file {
                        let tag_id = file * rels_per_file + rel + 1;
                        per_table
                            .entry(table_for(tag_id))
                            .or_default()
                            .push((file + 1, tag_id));
                    }
                }
                let total_rows = (files * rels_per_file) as usize;
                let insert_started = Instant::now();
                for (table, mut rows) in per_table {
                    rows.sort_unstable_by_key(|(file_id, tag_id)| (*tag_id, *file_id));
                    for chunk in rows.chunks(SQL_CHUNK_SIZE) {
                        let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?)").collect();
                        let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
                        for (file_id, tag_id) in chunk {
                            params.push(Value::from(*file_id as i64));
                            params.push(Value::from(*tag_id as i64));
                        }
                        conn.execute(
                            format!(
                                "INSERT INTO {table} (file_id, tag_id) VALUES {}",
                                holders.join(", ")
                            ),
                            params_from_iter(params),
                        )
                        .await
                        .unwrap();
                    }
                }
                let insert_elapsed = insert_started.elapsed();

                // How production batches the lookup: the partitioned shape is
                // limited by a *text* budget divided by the arm count, so the
                // statement count grows with the number of namespaces.
                let batch = if partitioned {
                    (3_200 / arms.max(1)).clamp(1, SQL_CHUNK_SIZE)
                } else {
                    SQL_CHUNK_SIZE
                };

                // ---- phase 1: file -> tags, N ids ----------------------
                let query_ids: Vec<u64> = (0..ids_per_query as u64)
                    .map(|i| (i * files / ids_per_query as u64) + 1)
                    .collect();
                let mut read_ms = f64::MAX;
                let mut read_rows = 0usize;
                let mut statements = 0usize;
                let mut sql_bytes = 0usize;
                for iter in 0..iters + 1 {
                    let started = Instant::now();
                    let mut rows_seen = 0usize;
                    for chunk in query_ids.chunks(batch) {
                        let placeholders: Vec<String> =
                            (1..=chunk.len()).map(|i| format!("?{i}")).collect();
                        let sql = if partitioned {
                            let arms_sql: Vec<String> = (0..arms)
                                .map(|arm| {
                                    format!(
                                        "SELECT file_id, tag_id FROM Relationship_{arm} \
                                         WHERE file_id IN ({})",
                                        placeholders.join(", ")
                                    )
                                })
                                .collect();
                            format!(
                                "SELECT file_id, tag_id FROM ({}) AS r",
                                arms_sql.join(" UNION ALL ")
                            )
                        } else {
                            format!(
                                "SELECT file_id, tag_id FROM Relationship WHERE file_id IN ({})",
                                placeholders.join(", ")
                            )
                        };
                        let params: Vec<Value> =
                            chunk.iter().map(|id| Value::from(*id as i64)).collect();
                        let mut rows = conn.query(&sql, params_from_iter(params)).await.unwrap();
                        while let Ok(Some(_)) = rows.next().await {
                            rows_seen += 1;
                        }
                        if iter == 0 {
                            statements += 1;
                            sql_bytes += sql.len();
                            // The plan, with the ids inlined so EXPLAIN does
                            // not need bindings.
                            let literals: Vec<String> =
                                chunk.iter().map(|id| id.to_string()).collect();
                            let explain_sql =
                                sql.replace(&placeholders.join(", "), &literals.join(", "));
                            explain(&conn, &explain_sql).await;
                        }
                    }
                    if iter == 0 {
                        read_rows = rows_seen;
                    } else {
                        read_ms = read_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                }

                // ---- phase 2: file -> tags, one id ---------------------
                // The single-file lookup, which is what a page load does.
                let mut single_ms = f64::MAX;
                for iter in 0..iters + 1 {
                    let started = Instant::now();
                    let sql = if partitioned {
                        let arms_sql: Vec<String> = (0..arms)
                            .map(|arm| {
                                format!(
                                    "SELECT file_id, tag_id FROM Relationship_{arm} \
                                     WHERE file_id = ?1"
                                )
                            })
                            .collect();
                        format!(
                            "SELECT file_id, tag_id FROM ({}) AS r",
                            arms_sql.join(" UNION ALL ")
                        )
                    } else {
                        "SELECT file_id, tag_id FROM Relationship WHERE file_id = ?1".to_string()
                    };
                    let mut rows = conn
                        .query(&sql, params_from_iter([Value::from(1i64)]))
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                    if iter > 0 {
                        single_ms = single_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                }

                // ---- phase 3: tag -> files, one tag -------------------
                // Served by the primary key, so the partition shape does not
                // change the plan -- but it does change how many b-trees the
                // seek has to pick between, and this is the direction the
                // search planner drives.
                let mut tag_ms = f64::MAX;
                for iter in 0..iters + 1 {
                    let started = Instant::now();
                    let sql = format!("SELECT file_id FROM {} WHERE tag_id = ?1", table_for(1));
                    let mut rows = conn
                        .query(&sql, params_from_iter([Value::from(1i64)]))
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                    if iter > 0 {
                        tag_ms = tag_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                }

                // ---- phase 4: bulk add ---------------------------------
                // Novel rows, fed the way `relationships_bulk_add` feeds
                // them: grouped per table, sorted by (tag_id, file_id),
                // chunked at SQL_CHUNK_SIZE.
                let novel: Vec<(u64, u64)> = (0..write_files)
                    .flat_map(|file| {
                        (0..rels_per_file).map(move |rel| {
                            (
                                files + file + 1,
                                (files + file) * rels_per_file + rel + 1 + total_rows as u64,
                            )
                        })
                    })
                    .collect();
                let mut novel_per_table: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
                for &(file_id, tag_id) in &novel {
                    novel_per_table
                        .entry(table_for(tag_id))
                        .or_default()
                        .push((file_id, tag_id));
                }
                let write_started = Instant::now();
                for (table, mut rows) in novel_per_table {
                    rows.sort_unstable_by_key(|(file_id, tag_id)| (*tag_id, *file_id));
                    for chunk in rows.chunks(SQL_CHUNK_SIZE) {
                        let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?)").collect();
                        let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
                        for (file_id, tag_id) in chunk {
                            params.push(Value::from(*file_id as i64));
                            params.push(Value::from(*tag_id as i64));
                        }
                        conn.execute(
                            format!(
                                "INSERT INTO {table} (file_id, tag_id) VALUES {}",
                                holders.join(", ")
                            ),
                            params_from_iter(params),
                        )
                        .await
                        .unwrap();
                    }
                }
                let write_ms = write_started.elapsed().as_secs_f64() * 1_000.0;

                // ---- phase 5: bulk delete -----------------------------
                // `DELETE ... WHERE tag_id = ? AND file_id IN (...)`, the shape
                // `relationship_bulk_delete` uses because the partition's
                // primary key is (tag_id, file_id).
                let delete_started = Instant::now();
                let mut deleted = 0u64;
                for chunk in novel.chunks(SQL_CHUNK_SIZE) {
                    let mut by_tag: HashMap<u64, Vec<u64>> = HashMap::new();
                    for &(file_id, tag_id) in chunk {
                        by_tag.entry(tag_id).or_default().push(file_id);
                    }
                    for (tag_id, mut file_ids) in by_tag {
                        file_ids.sort_unstable();
                        file_ids.dedup();
                        // Numbered placeholders throughout: the tag id takes
                        // ?1 and the file list starts at ?2, so the two
                        // parameter blocks cannot collide.
                        let placeholders: Vec<String> =
                            (2..=file_ids.len() + 1).map(|i| format!("?{i}")).collect();
                        let mut params: Vec<Value> = vec![Value::from(tag_id as i64)];
                        params.extend(file_ids.iter().map(|id| Value::from(*id as i64)));
                        deleted += conn
                            .execute(
                                format!(
                                    "DELETE FROM {} WHERE tag_id = ?1 AND file_id IN ({})",
                                    table_for(tag_id),
                                    placeholders.join(", ")
                                ),
                                params_from_iter(params),
                            )
                            .await
                            .unwrap();
                    }
                }
                let delete_ms = delete_started.elapsed().as_secs_f64() * 1_000.0;

                println!(
                    "  {label}\n    \
                     seed      {total_rows:>9} rows in {:>9.2} ms ({:>6.1} us/row)\n    \
                     read  N   {read_ms:>9.3} ms  ({read_rows} rows, {statements} stmt, \
                     {sql_bytes} B of SQL)\n    \
                     read  1   {single_ms:>9.3} ms\n    \
                     read tag  {tag_ms:>9.3} ms\n    \
                     write     {write_ms:>9.2} ms  ({} novel rows)\n    \
                     delete    {delete_ms:>9.2} ms  ({deleted} rows)",
                    insert_elapsed.as_secs_f64() * 1_000.0,
                    insert_elapsed.as_secs_f64() * 1_000_000.0 / total_rows as f64,
                    novel.len(),
                );
            }
        });
    }

    /// What a declared foreign key actually costs on this backend.
    ///
    /// The `Tags` table declares `FOREIGN KEY (namespace) REFERENCES
    /// Namespace(id)`, and `Relationship_{ns}` declares two more. The backend
    /// runs with `PRAGMA foreign_keys = 0`, so those constraints never fire
    /// and their `ON DELETE CASCADE` clauses are inert -- the code says so
    /// itself. The open question is whether *declaring* them is still
    /// expensive: an inert constraint that costs a lookup per row touched is
    /// not free, and `tag_counts_apply` touches a `Tags` row per tag.
    ///
    /// This measures the two shapes that matter rather than a bare PK probe:
    /// a full-row read (which materializes the row) and the `UPDATE ... WHERE
    /// id IN (...)` shape that `tag_counts_apply` issues. A bare `SELECT id`
    /// can be answered from the rowid alone and never materializes anything,
    /// so it cannot show a per-row cost even if one exists.
    ///
    /// The parent table's size is varied because the hypothesis behind the
    /// original 116 ms figure was that resolving the key costs *per parent
    /// row*: a `Namespace` table with tens of thousands of rows would make
    /// every `Tags` row touch expensive.
    ///
    /// ```text
    /// BENCH_FK_PARENTS=20000 BENCH_FK_ROWS=200000 cargo test --lib \
    ///     db::turso::bench::tests::db_bench_foreign_key_cost -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: what a declared foreign key costs"]
    fn db_bench_foreign_key_cost() {
        let rows = env_u64("BENCH_FK_ROWS", 100_000);
        let parents = env_u64("BENCH_FK_PARENTS", 20_000);
        let reps = env_u64("BENCH_FK_REPS", 200).max(1);
        let update_batch = env_u64("BENCH_FK_UPDATE", 100).max(1) as usize;
        let update_reps = env_u64("BENCH_FK_UPDATE_REPS", 20).max(1);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH foreign key cost: {rows} rows, {parents} parent rows, \
                 {update_batch} rows per UPDATE"
            );

            let path = bench_db_path("fkcost");
            bench_remove(&path);
            let db = Builder::new_local(&path.to_string_lossy())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();

            conn.execute_batch(&format!(
                "CREATE TABLE Namespace (
                     id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL UNIQUE,
                     description TEXT);
                 CREATE TABLE Tags_fk (
                     id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     namespace INTEGER NOT NULL,
                     count INTEGER NOT NULL DEFAULT 0,
                     UNIQUE(name, namespace),
                     FOREIGN KEY (namespace) REFERENCES Namespace(id));
                 CREATE TABLE Tags_nofk (
                     id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     namespace INTEGER NOT NULL,
                     count INTEGER NOT NULL DEFAULT 0,
                     UNIQUE(name, namespace));"
            ))
            .await
            .unwrap();

            // Parent rows, then the two `Tags` shapes with identical content.
            for chunk_start in (0..parents).step_by(SQL_CHUNK_SIZE) {
                let end = (chunk_start + SQL_CHUNK_SIZE as u64).min(parents);
                let holders: Vec<String> =
                    (chunk_start..end).map(|_| "(?, ?)".to_string()).collect();
                let mut params: Vec<Value> = Vec::new();
                for id in chunk_start..end {
                    params.push(Value::from(id as i64));
                    params.push(Value::from(format!("ns-{id}")));
                }
                conn.execute(
                    format!(
                        "INSERT INTO Namespace (id, name) VALUES {}",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await
                .unwrap();
            }
            for table in ["Tags_fk", "Tags_nofk"] {
                for chunk_start in (0..rows).step_by(SQL_CHUNK_SIZE) {
                    let end = (chunk_start + SQL_CHUNK_SIZE as u64).min(rows);
                    let holders: Vec<String> = (chunk_start..end)
                        .map(|_| "(?, ?, ?)".to_string())
                        .collect();
                    let mut params: Vec<Value> = Vec::new();
                    for id in chunk_start..end {
                        params.push(Value::from(id as i64));
                        params.push(Value::from(format!("tag-name-{id:08}")));
                        params.push(Value::from((id % parents) as i64));
                    }
                    conn.execute(
                        format!(
                            "INSERT INTO {table} (id, name, namespace) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
                }
            }

            // Probe ids spread across the table, so the reads are hits and
            // land on cold-ish pages rather than one hot row.
            let probes: Vec<i64> = (0..reps)
                .map(|i| ((i as u64 * rows / reps.max(1) as u64) % rows) as i64)
                .collect();

            for table in ["Tags_fk", "Tags_nofk"] {
                // Full-row read: this materializes the row, which is where a
                // per-row parent-key resolution would show up.
                let started = Instant::now();
                for id in &probes {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT id, name, namespace, count FROM {table} WHERE id = ?1"
                            ),
                            params_from_iter([Value::from(*id)]),
                        )
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                }
                let full_row = started.elapsed();

                // Bare-id read: answerable from the rowid alone.
                let started = Instant::now();
                for id in &probes {
                    let mut rows = conn
                        .query(
                            &format!("SELECT id FROM {table} WHERE id = ?1"),
                            params_from_iter([Value::from(*id)]),
                        )
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                }
                let bare_id = started.elapsed();

                // The `tag_counts_apply` shape: one UPDATE over a batch of
                // ids, which materializes every row it touches.
                let batch: Vec<i64> = (0..update_batch).map(|i| i as i64 + 1).collect();
                let placeholders: Vec<String> =
                    (1..=batch.len()).map(|i| format!("?{i}")).collect();
                let update_sql = format!(
                    "UPDATE {table} SET count = count + 1 WHERE id IN ({})",
                    placeholders.join(", ")
                );
                let started = Instant::now();
                for _ in 0..update_reps {
                    conn.execute(
                        &update_sql,
                        params_from_iter(batch.iter().map(|id| Value::from(*id))),
                    )
                    .await
                    .unwrap();
                }
                let update = started.elapsed();

                println!(
                    "  {table:<12} full-row read {:>9.3} ms/op   \
                     bare-id read {:>9.3} ms/op   \
                     UPDATE {} rows {:>9.3} ms/op",
                    full_row.as_secs_f64() * 1_000.0 / reps as f64,
                    bare_id.as_secs_f64() * 1_000.0 / reps as f64,
                    update_batch,
                    update.as_secs_f64() * 1_000.0 / update_reps as f64,
                );
            }
            println!(
                "  ({} tag rows, {} parent rows, {} read reps, {} update reps x {} rows)",
                rows, parents, reps, update_reps, update_batch
            );
        });
    }

    /// The `limit_to` scope model, measured against the alternative of
    /// carrying the scope on the relationship row.
    ///
    /// The forum and gallery plugins record two different things:
    ///
    /// * a *structural* edge -- "post X is a reply to post Y" -- which is a
    ///   real edge in the tag graph, and
    /// * a *scoped* value -- "the tag `Anal` belongs to gif G" -- which is
    ///   not an edge at all. It says that a tag instance is scoped to an
    ///   entity, and today it is stored as a `Parents` row with a `limit_to`
    ///   qualifier, because the tag itself is shared by every entity that
    ///   carries it.
    ///
    /// The scoped value is already implied by the file's tag set -- the file
    /// that carries `Anal` also carries the entity tag `G` -- so the
    /// `limit_to` edge is a materialized join across the file boundary. The
    /// alternative is to carry the scope *on* the relationship row:
    ///
    /// ```text
    /// today:      Relationship(file_id, tag_id)
    ///             Parents(tag_id, relate_tag_id, limit_to)   -- scope lives here
    ///
    /// proposed:  Relationship(file_id, tag_id, scope_tag_id) -- scope rides along
    ///             Parents(tag_id, relate_tag_id)             -- structural edges only
    /// ```
    ///
    /// The proposed shape stores strictly *less*: the scoped facts move from a
    /// second table onto a column of a row that already exists, and `Parents`
    /// keeps only the structural edges. The three queries below are the ones
    /// that read those facts today, and they are the ones the migration has
    /// to keep answering.
    ///
    /// ```text
    /// BENCH_SCOPE_ENTITIES=50000 BENCH_SCOPE_VALUES=2000 cargo test --lib \
    ///     db::turso::bench::tests::db_bench_relationship_scope -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: limit_to scope vs scope on the relationship"]
    fn db_bench_relationship_scope() {
        let entities = env_u64("BENCH_SCOPE_ENTITIES", 20_000);
        let values = env_u64("BENCH_SCOPE_VALUES", 2_000);
        let values_per_entity = env_u64("BENCH_SCOPE_VALUES_PER_ENTITY", 5);
        let reps = env_u64("BENCH_SCOPE_REPS", 50).max(1);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH relationship scope: {entities} entities, {values} value tags, \
                 {values_per_entity} values/entity"
            );

            // Tag ids are laid out so the two models can share them: entity
            // tags first, then value tags.
            let entity_tag = |entity: u64| entity + 1;
            let value_tag = |value: u64| entities + value + 1;

            // ---- variant A: today (scope lives in Parents) ------------
            let path_a = bench_db_path("scope-parents");
            bench_remove(&path_a);
            let db_a = Builder::new_local(&path_a.to_string_lossy())
                .build()
                .await
                .unwrap();
            let conn_a = db_a.connect().unwrap();
            conn_a
                .pragma_update("journal_mode", "'mvcc'")
                .await
                .unwrap();
            conn_a
                .execute_batch(
                    "CREATE TABLE Relationship (
                         file_id INTEGER NOT NULL,
                         tag_id INTEGER NOT NULL,
                         PRIMARY KEY (tag_id, file_id));
                     CREATE INDEX idx_Relationship_file_tag
                         ON Relationship (file_id, tag_id);
                     CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         limit_to INTEGER NOT NULL DEFAULT -1,
                         CHECK (tag_id != relate_tag_id));
                     CREATE INDEX idx_parents_rel
                         ON Parents (relate_tag_id, tag_id, limit_to);
                     CREATE INDEX idx_parents_lim
                         ON Parents (limit_to, tag_id, relate_tag_id);",
                )
                .await
                .unwrap();

            // ---- variant B: scope on the relationship row -------------
            let path_b = bench_db_path("scope-column");
            bench_remove(&path_b);
            let db_b = Builder::new_local(&path_b.to_string_lossy())
                .build()
                .await
                .unwrap();
            let conn_b = db_b.connect().unwrap();
            conn_b
                .pragma_update("journal_mode", "'mvcc'")
                .await
                .unwrap();
            conn_b
                .execute_batch(
                    "CREATE TABLE Relationship (
                         file_id INTEGER NOT NULL,
                         tag_id INTEGER NOT NULL,
                         scope_tag_id INTEGER NOT NULL DEFAULT -1,
                         PRIMARY KEY (tag_id, file_id, scope_tag_id));
                     CREATE INDEX idx_Relationship_file_tag
                         ON Relationship (file_id, tag_id);
                     CREATE INDEX idx_Relationship_scope
                         ON Relationship (scope_tag_id, tag_id, file_id);
                     CREATE TABLE Parents (
                         id INTEGER PRIMARY KEY,
                         tag_id INTEGER NOT NULL,
                         relate_tag_id INTEGER NOT NULL,
                         CHECK (tag_id != relate_tag_id));",
                )
                .await
                .unwrap();

            // The same facts in both. Each entity has one file, its own entity
            // tag, and `values_per_entity` value tags.
            let mut rel_rows_a: Vec<(u64, u64)> = Vec::new();
            let mut rel_rows_b: Vec<(u64, u64, i64)> = Vec::new();
            let mut parent_rows: Vec<(u64, u64, i64)> = Vec::new();
            for entity in 0..entities {
                let file_id = entity + 1;
                let etag = entity_tag(entity);
                // The file carries its own entity tag.
                rel_rows_a.push((file_id, etag));
                rel_rows_b.push((file_id, etag, -1));
                for v in 0..values_per_entity {
                    let value = (entity + v) % values;
                    let vtag = value_tag(value);
                    rel_rows_a.push((file_id, vtag));
                    rel_rows_b.push((file_id, vtag, etag as i64));
                    // The scope fact, stored as a Parents edge today.
                    parent_rows.push((vtag, etag, etag as i64));
                }
            }

            let seed_started = Instant::now();
            // Variant A: relationship rows without a scope, plus the scope
            // facts as `Parents` edges.
            let mut sorted = rel_rows_a.clone();
            sorted.sort_unstable_by_key(|(file_id, tag_id)| (*tag_id, *file_id));
            for chunk in sorted.chunks(SQL_CHUNK_SIZE) {
                let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?)").collect();
                let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
                for (file_id, tag_id) in chunk {
                    params.push(Value::from(*file_id as i64));
                    params.push(Value::from(*tag_id as i64));
                }
                conn_a
                    .execute(
                        format!(
                            "INSERT INTO Relationship (file_id, tag_id) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
            }
            // Variant B: the same facts, with the scope riding on the
            // relationship row, and no `Parents` edges at all.
            let mut scoped_rows = rel_rows_b.clone();
            scoped_rows.sort_unstable_by_key(|(file_id, tag_id, _)| (*tag_id, *file_id));
            for chunk in scoped_rows.chunks(SQL_CHUNK_SIZE) {
                let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?, ?)").collect();
                let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 3);
                for (file_id, tag_id, scope) in chunk {
                    params.push(Value::from(*file_id as i64));
                    params.push(Value::from(*tag_id as i64));
                    params.push(Value::from(*scope));
                }
                conn_b
                    .execute(
                        format!(
                            "INSERT INTO Relationship (file_id, tag_id, scope_tag_id) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
            }
            for chunk in parent_rows.chunks(SQL_CHUNK_SIZE) {
                let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?, ?)").collect();
                let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 3);
                for (tag_id, relate, limit) in chunk {
                    params.push(Value::from(*tag_id as i64));
                    params.push(Value::from(*relate as i64));
                    params.push(Value::from(*limit));
                }
                conn_a
                    .execute(
                        format!(
                            "INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
            }
            let seed_ms = seed_started.elapsed().as_secs_f64() * 1_000.0;

            // The three reads that matter.
            let probe_entity = entities / 2;
            let probe_value = values / 2;

            // Q1: which files carry a value tag.
            let mut q1_a = f64::MAX;
            let mut q1_b = f64::MAX;
            for _ in 0..reps {
                let started = Instant::now();
                let mut rows = conn_a
                    .query(
                        "SELECT file_id FROM Relationship WHERE tag_id = ?1;",
                        params_from_iter([Value::from(value_tag(probe_value) as i64)]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                q1_a = q1_a.min(started.elapsed().as_secs_f64() * 1_000.0);

                let started = Instant::now();
                let mut rows = conn_b
                    .query(
                        "SELECT file_id FROM Relationship WHERE tag_id = ?1;",
                        params_from_iter([Value::from(value_tag(probe_value) as i64)]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                q1_b = q1_b.min(started.elapsed().as_secs_f64() * 1_000.0);
            }

            // Q2: which value tags are scoped to an entity.
            let mut q2_a = f64::MAX;
            let mut q2_b = f64::MAX;
            for _ in 0..reps {
                let started = Instant::now();
                let mut rows = conn_a
                    .query(
                        "SELECT tag_id FROM Parents WHERE limit_to = ?1;",
                        params_from_iter([Value::from(entity_tag(probe_entity) as i64)]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                q2_a = q2_a.min(started.elapsed().as_secs_f64() * 1_000.0);

                let started = Instant::now();
                let mut rows = conn_b
                    .query(
                        "SELECT tag_id FROM Relationship WHERE scope_tag_id = ?1;",
                        params_from_iter([Value::from(entity_tag(probe_entity) as i64)]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                q2_b = q2_b.min(started.elapsed().as_secs_f64() * 1_000.0);
            }

            // Q3: which entities carry a value tag. Today this needs the
            // `Parents` edge; proposed it falls out of the relationship row.
            let mut q3_a = f64::MAX;
            let mut q3_b = f64::MAX;
            for _ in 0..reps {
                let started = Instant::now();
                let mut rows = conn_a
                    .query(
                        "SELECT limit_to FROM Parents WHERE relate_tag_id = ?1;",
                        params_from_iter([Value::from(value_tag(probe_value) as i64)]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                q3_a = q3_a.min(started.elapsed().as_secs_f64() * 1_000.0);

                let started = Instant::now();
                let mut rows = conn_b
                    .query(
                        "SELECT DISTINCT scope_tag_id FROM Relationship WHERE tag_id = ?1;",
                        params_from_iter([Value::from(value_tag(probe_value) as i64)]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                q3_b = q3_b.min(started.elapsed().as_secs_f64() * 1_000.0);
            }

            let rows_a: i64 = conn_a
                .query("SELECT COUNT(*) FROM Relationship;", ())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .map(|r| r.get(0).unwrap())
                .unwrap_or(0);
            let parents_a: i64 = conn_a
                .query("SELECT COUNT(*) FROM Parents;", ())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .map(|r| r.get(0).unwrap())
                .unwrap_or(0);
            let rows_b: i64 = conn_b
                .query("SELECT COUNT(*) FROM Relationship;", ())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .map(|r| r.get(0).unwrap())
                .unwrap_or(0);

            println!(
                "  A scope in Parents      Q1 files-with-value {:>8.3} ms   \
                 Q2 tags-scoped-to {:>8.3} ms   Q3 entities-with-value {:>8.3} ms",
                q1_a, q2_a, q3_a
            );
            println!(
                "  B scope on Relationship Q1 files-with-value {:>8.3} ms   \
                 Q2 tags-scoped-to {:>8.3} ms   Q3 entities-with-value {:>8.3} ms",
                q1_b, q2_b, q3_b
            );
            println!(
                "    A stores {rows_a} relationship rows + {parents_a} Parents rows; \
                 B stores {rows_b} relationship rows + 0 Parents rows"
            );
            println!("    seed: {seed_ms:.2} ms for both shapes");
        });
    }

    /// What the popularity bookkeeping costs, and what it would cost to take
    /// it off the write path.
    ///
    /// `Tags.count` and the `Tags_Popular` search shadow are both *derived*
    /// from the relationship rows, and today both are maintained
    /// incrementally by the writer: `tag_counts_apply` folds count deltas in
    /// behind a global lock, and `sync_tags_popular` keeps the shadow in
    /// step -- which also means a Tantivy segment is flushed per statement.
    ///
    /// Three write paths are compared, on the same fixture:
    ///
    /// * **A (today)**: count fold-in plus the shadow sync.
    /// * **B**: count fold-in only; the shadow is rebuilt wholesale later.
    /// * **C**: neither; the count is recomputed from the relationship table
    ///   when someone asks for it.
    ///
    /// The reads are measured against the same three states, so the trade-off
    /// is explicit: taking the bookkeeping off the writer moves cost onto the
    /// reader, and the question is how much of each there is.
    ///
    /// ```text
    /// BENCH_COUNT_TAGS=50000 BENCH_COUNT_RELS=5 cargo test --lib \
    ///     db::turso::bench::tests::db_bench_tag_count_path -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: incremental tag count and popular shadow"]
    fn db_bench_tag_count_path() {
        let tags = env_u64("BENCH_COUNT_TAGS", 20_000);
        let rels_per_tag = env_u64("BENCH_COUNT_RELS", 5);
        let batch = env_u64("BENCH_COUNT_BATCH", 100).max(1) as usize;
        let iters = env_u64("BENCH_COUNT_ITERS", 20).max(1);
        let threshold = 5i64;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH tag count path: {tags} tags, {rels_per_tag} rels/tag, \
                 {batch} tags per fold-in"
            );

            let path = bench_db_path("tagcount");
            bench_remove(&path);
            let db = Builder::new_local(&path.to_string_lossy())
                .experimental_index_method(true)
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();
            conn.execute_batch(
                "CREATE TABLE Tags (
                     id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL,
                     namespace INTEGER NOT NULL,
                     count INTEGER NOT NULL DEFAULT 0,
                     UNIQUE(name, namespace));
                 CREATE INDEX idx_tags_count_covering
                     ON Tags (count DESC, name, namespace);
                 CREATE TABLE Tags_Popular (
                     tag_id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL);
                 CREATE TABLE Relationship (
                     file_id INTEGER NOT NULL,
                     tag_id INTEGER NOT NULL,
                     PRIMARY KEY (tag_id, file_id));
                 CREATE INDEX idx_Relationship_file_tag
                     ON Relationship (file_id, tag_id);",
            )
            .await
            .unwrap();

            // Seed tags and their relationships, then set the counts to the
            // value the relationship rows imply.
            for chunk_start in (0..tags).step_by(SQL_CHUNK_SIZE) {
                let end = (chunk_start + SQL_CHUNK_SIZE as u64).min(tags);
                let holders: Vec<String> = (chunk_start..end)
                    .map(|_| "(?, ?, ?)".to_string())
                    .collect();
                let mut params: Vec<Value> = Vec::new();
                for id in chunk_start..end {
                    params.push(Value::from(id as i64 + 1));
                    params.push(Value::from(format!("tag-{id:08}")));
                    params.push(Value::from(1i64));
                }
                conn.execute(
                    format!(
                        "INSERT INTO Tags (id, name, namespace) VALUES {}",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await
                .unwrap();
            }
            for chunk_start in (0..tags).step_by(SQL_CHUNK_SIZE) {
                let end = (chunk_start + SQL_CHUNK_SIZE as u64).min(tags);
                let mut rows: Vec<(u64, u64)> = Vec::new();
                for id in chunk_start..end {
                    for rel in 0..rels_per_tag {
                        rows.push((id * rels_per_tag + rel + 1, id + 1));
                    }
                }
                rows.sort_unstable();
                for chunk in rows.chunks(SQL_CHUNK_SIZE) {
                    let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?)").collect();
                    let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
                    for (file_id, tag_id) in chunk {
                        params.push(Value::from(*file_id as i64));
                        params.push(Value::from(*tag_id as i64));
                    }
                    conn.execute(
                        format!(
                            "INSERT INTO Relationship (file_id, tag_id) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
                }
            }
            // Seed the counts from the relationship rows. A correlated
            // subquery is not an option here -- limbo cannot resolve an
            // outer reference in that position -- so the aggregate is read
            // out and folded in with CASE, the way `slurp_recount_namespace`
            // does it.
            {
                let mut counts: std::collections::HashMap<i64, i64> =
                    std::collections::HashMap::new();
                let mut rows = conn
                    .query(
                        "SELECT tag_id, COUNT(*) FROM Relationship GROUP BY tag_id;",
                        (),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(row)) = rows.next().await {
                    let tag_id: i64 = row.get(0).unwrap();
                    let count: i64 = row.get(1).unwrap();
                    counts.insert(tag_id, count);
                }
                let entries: Vec<(i64, i64)> = counts.into_iter().collect();
                for chunk in entries.chunks(SQL_CHUNK_SIZE) {
                    let clauses: Vec<String> =
                        chunk.iter().map(|_| "WHEN ? THEN ?".to_string()).collect();
                    let placeholders: Vec<String> =
                        (1..=chunk.len()).map(|_| "?".to_string()).collect();
                    let mut params: Vec<Value> = Vec::new();
                    for (id, count) in chunk {
                        params.push(Value::from(*id));
                        params.push(Value::from(*count));
                    }
                    for (id, _) in chunk {
                        params.push(Value::from(*id));
                    }
                    conn.execute(
                        format!(
                            "UPDATE Tags SET count = CASE id {} ELSE 0 END WHERE id IN ({})",
                            clauses.join(" "),
                            placeholders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
                }
            }

            // The FTS shadow, with the same ngram index the schema creates.
            // Built after the counts are in place so the shadow starts
            // consistent, the way it does after a slurp.
            conn.execute_batch(&format!(
                "INSERT INTO Tags_Popular (tag_id, name)
                     SELECT id, name FROM Tags WHERE count >= {threshold};
                 CREATE INDEX idx_tags_fts ON Tags_Popular USING fts (name)
                     WITH (tokenizer='ngram', min_gram=2, max_gram=3);
                 CREATE TABLE Tags_Popular_plain (
                     tag_id INTEGER PRIMARY KEY,
                     name TEXT NOT NULL);
                 INSERT INTO Tags_Popular_plain (tag_id, name)
                     SELECT id, name FROM Tags WHERE count >= {threshold};"
            ))
            .await
            .unwrap();

            // ---- the three write paths --------------------------------
            // Each one applies the same delta: +1 to `batch` tags.
            let delta_ids: Vec<i64> = (1..=batch as i64).collect();

            // A: the count fold-in plus the shadow sync, exactly as
            // `tag_counts_apply` does it.
            let count_sql = {
                let clauses: Vec<String> = delta_ids
                    .iter()
                    .map(|_| "WHEN ? THEN ?".to_string())
                    .collect();
                let placeholders: Vec<String> =
                    (1..=delta_ids.len()).map(|_| "?".to_string()).collect();
                let mut params: Vec<Value> = Vec::new();
                for id in &delta_ids {
                    params.push(Value::from(*id));
                    params.push(Value::from(1i64));
                }
                for id in &delta_ids {
                    params.push(Value::from(*id));
                }
                (
                    format!(
                        "UPDATE Tags SET count = count + CASE id {} ELSE 0 END \
                         WHERE id IN ({})",
                        clauses.join(" "),
                        placeholders.join(", ")
                    ),
                    params,
                )
            };
            let shadow_preread_sql = {
                let placeholders: Vec<String> =
                    (1..=delta_ids.len()).map(|_| "?".to_string()).collect();
                (
                    format!(
                        "SELECT t.id, t.count, (tp.tag_id IS NOT NULL)
                         FROM Tags t LEFT JOIN Tags_Popular tp ON tp.tag_id = t.id
                         WHERE t.id IN ({})",
                        placeholders.join(", ")
                    ),
                    delta_ids
                        .iter()
                        .map(|id| Value::from(*id))
                        .collect::<Vec<Value>>(),
                )
            };
            let shadow_delete_sql = {
                let placeholders: Vec<String> =
                    (1..=delta_ids.len()).map(|_| "?".to_string()).collect();
                let mut params: Vec<Value> = delta_ids
                    .iter()
                    .map(|id| Value::from(*id))
                    .collect::<Vec<Value>>();
                params.extend(delta_ids.iter().map(|id| Value::from(*id)));
                params.push(Value::from(threshold));
                (
                    format!(
                        "DELETE FROM Tags_Popular
                         WHERE tag_id IN ({0}) AND tag_id NOT IN (
                             SELECT id FROM Tags WHERE id IN ({0}) AND count >= ?)",
                        placeholders.join(", ")
                    ),
                    params,
                )
            };
            let shadow_insert_sql = {
                let placeholders: Vec<String> =
                    (1..=delta_ids.len()).map(|_| "?".to_string()).collect();
                let mut params: Vec<Value> = delta_ids
                    .iter()
                    .map(|id| Value::from(*id))
                    .collect::<Vec<Value>>();
                params.push(Value::from(threshold));
                (
                    format!(
                        "INSERT OR IGNORE INTO Tags_Popular (tag_id, name)
                         SELECT id, name FROM Tags WHERE id IN ({}) AND count >= ?",
                        placeholders.join(", ")
                    ),
                    params,
                )
            };

            let mut a_ms = f64::MAX;
            let mut a_cross_ms = f64::MAX;
            let mut b_ms = f64::MAX;
            let mut c_ms = f64::MAX;
            let mut total_crossings = 0i64;
            for iter in 0..iters {
                // A steady state: the count fold-in plus the pre-read that
                // decides whether the shadow needs touching. This is what
                // production does on every batch, and in steady state nothing
                // crosses the threshold.
                let started = Instant::now();
                conn.execute(&count_sql.0, params_from_iter(count_sql.1.clone()))
                    .await
                    .unwrap();
                let mut rows = conn
                    .query(
                        &shadow_preread_sql.0,
                        params_from_iter(shadow_preread_sql.1.clone()),
                    )
                    .await
                    .unwrap();
                let mut crossing = 0i64;
                while let Ok(Some(row)) = rows.next().await {
                    let old_count: i64 = row.get(1).unwrap();
                    let in_shadow: i64 = row.get(2).unwrap();
                    if (old_count + 1 >= threshold) != (in_shadow != 0) {
                        crossing += 1;
                    }
                }
                total_crossings += crossing;
                let a = started.elapsed().as_secs_f64() * 1_000.0;

                // A with a crossing: the two shadow statements run as well,
                // and each one flushes a Tantivy segment.
                let started = Instant::now();
                conn.execute(
                    &shadow_delete_sql.0,
                    params_from_iter(shadow_delete_sql.1.clone()),
                )
                .await
                .unwrap();
                conn.execute(
                    &shadow_insert_sql.0,
                    params_from_iter(shadow_insert_sql.1.clone()),
                )
                .await
                .unwrap();
                let a_cross = a + started.elapsed().as_secs_f64() * 1_000.0;

                // B: count fold-in only.
                let started = Instant::now();
                conn.execute(&count_sql.0, params_from_iter(count_sql.1.clone()))
                    .await
                    .unwrap();
                let b = started.elapsed().as_secs_f64() * 1_000.0;

                // C: nothing on the write path.
                let started = Instant::now();
                std::hint::black_box(&delta_ids);
                let c = started.elapsed().as_secs_f64() * 1_000.0;

                if iter > 0 {
                    a_ms = a_ms.min(a);
                    a_cross_ms = a_cross_ms.min(a_cross);
                    b_ms = b_ms.min(b);
                    c_ms = c_ms.min(c);
                }
            }

            // ---- the reads ---------------------------------------------
            // The popular-tags list, which is what the shadow exists to serve.
            let mut popular_ms = f64::MAX;
            for _ in 0..iters {
                let started = Instant::now();
                let mut rows = conn
                    .query(
                        "SELECT id, name FROM Tags WHERE count >= ?1
                         ORDER BY count DESC, name LIMIT 50;",
                        params_from_iter([Value::from(threshold)]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                popular_ms = popular_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
            }

            // The authoritative recount, which is what variant C would have to
            // run to make `count` true again.
            let recount_started = Instant::now();
            let mut counts: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
            {
                let mut rows = conn
                    .query(
                        "SELECT tag_id, COUNT(*) FROM Relationship GROUP BY tag_id;",
                        (),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(row)) = rows.next().await {
                    let tag_id: i64 = row.get(0).unwrap();
                    let count: i64 = row.get(1).unwrap();
                    counts.insert(tag_id, count);
                }
            }
            let recount_scan_ms = recount_started.elapsed().as_secs_f64() * 1_000.0;
            let recount_write_started = Instant::now();
            for chunk in counts.iter().collect::<Vec<_>>().chunks(SQL_CHUNK_SIZE) {
                let clauses: Vec<String> =
                    chunk.iter().map(|_| "WHEN ? THEN ?".to_string()).collect();
                let placeholders: Vec<String> =
                    (1..=chunk.len()).map(|_| "?".to_string()).collect();
                let mut params: Vec<Value> = Vec::new();
                for (id, count) in chunk {
                    params.push(Value::from(**id));
                    params.push(Value::from(**count));
                }
                for (id, _) in chunk {
                    params.push(Value::from(**id));
                }
                conn.execute(
                    format!(
                        "UPDATE Tags SET count = CASE id {} ELSE 0 END WHERE id IN ({})",
                        clauses.join(" "),
                        placeholders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await
                .unwrap();
            }
            let recount_write_ms = recount_write_started.elapsed().as_secs_f64() * 1_000.0;

            // The wholesale shadow rebuild, which is what variant B would
            // have to run to make the shadow true again.
            let rebuild_started = Instant::now();
            conn.execute_batch(&format!(
                "DELETE FROM Tags_Popular;
                 INSERT INTO Tags_Popular (tag_id, name)
                     SELECT id, name FROM Tags WHERE count >= {threshold};"
            ))
            .await
            .unwrap();
            let rebuild_ms = rebuild_started.elapsed().as_secs_f64() * 1_000.0;

            println!(
                "  write path, per fold-in of {batch} tags:\n    \
                 A steady state (count + pre-read)   {:>9.3} ms\n    \
                 A with a threshold crossing         {:>9.3} ms\n    \
                 B count only                       {:>9.3} ms\n    \
                 C nothing                          {:>9.3} ms\n    \
                 ({total_crossings} threshold crossings in {iters} fold-ins)",
                a_ms, a_cross_ms, b_ms, c_ms
            );
            println!(
                "  reads:\n    \
                 popular list (50)      {:>9.3} ms\n    \
                 authoritative recount   {:>9.3} ms scan + {:>9.3} ms write\n    \
                 wholesale shadow rebuild{:>9.3} ms",
                popular_ms, recount_scan_ms, recount_write_ms, rebuild_ms
            );

            // ---- is the FTS index worth its write tax? -----------------
            // The ngram index is what the shadow exists for, but it is also
            // what makes every shadow write flush a Tantivy segment. The
            // alternative is a plain scan with LIKE over the same rows --
            // affordable if the popular set is small enough, which it is by
            // construction (`count >= {threshold}`).
            let mut fts_ms = f64::MAX;
            let mut like_ms = f64::MAX;
            let mut like_rows = 0usize;
            for probe in 0..20i64 {
                let term = format!("tag-{:08}", probe * 977);
                let started = Instant::now();
                let mut rows = conn
                    .query(
                        "SELECT tag_id FROM Tags_Popular WHERE fts_match(name, ?1) LIMIT 20;",
                        params_from_iter([Value::from(term.clone())]),
                    )
                    .await
                    .unwrap();
                while let Ok(Some(_)) = rows.next().await {}
                fts_ms = fts_ms.min(started.elapsed().as_secs_f64() * 1_000.0);

                let started = Instant::now();
                let mut rows = conn
                    .query(
                        "SELECT tag_id FROM Tags_Popular_plain
                         WHERE name LIKE '%' || ?1 || '%' LIMIT 20;",
                        params_from_iter([Value::from(term.clone())]),
                    )
                    .await
                    .unwrap();
                let mut count = 0usize;
                while let Ok(Some(_)) = rows.next().await {
                    count += 1;
                }
                like_ms = like_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                like_rows = count;
            }
            let popular_rows: i64 = conn
                .query("SELECT COUNT(*) FROM Tags_Popular;", ())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .map(|r| r.get(0).unwrap())
                .unwrap_or(0);
            println!(
                "  tag search over {popular_rows} popular rows:\n    \
                 FTS (ngram)   {:>9.3} ms\n    \
                 LIKE scan     {:>9.3} ms   ({like_rows} rows matched)",
                fts_ms, like_ms
            );
        });
    }

    /// The hypothesis that the per-namespace partitioning exists to protect:
    /// that a single relationship table makes `tag_id -> file_id` lookups
    /// slow.
    ///
    /// The earlier benches measured the *other* direction (`file_id -> tag_id`,
    /// the one that has to union the partitions) and used a tag
    /// distribution where every tag belonged to exactly one file. Production
    /// is the opposite: a handful of tags carry a huge number of files, and
    /// most tags carry a few. The search planner drives on the rarest tag and
    /// then scans that tag's posting list, so the shape that matters is "one
    /// tag, many files".
    ///
    /// The fixture models that:
    ///
    /// * `hot_tags` tags each attached to `hot_files` files -- the posting
    ///   list a search scans, and the set a re-tag chunk rewrites.
    /// * `rare_tags` tags each attached to a handful of files -- the long tail.
    ///
    /// Both shapes get the same rows, fed the way production feeds them
    /// (grouped per table, sorted by `(tag_id, file_id)`), and every query is
    /// run warm. The measurements are taken after a seeding pass, and the
    /// reported number is the best of several runs, so a cold cache cannot
    /// masquerade as a shape difference.
    ///
    /// ```text
    /// BENCH_RELS=5000000 BENCH_HOT_TAGS=100 BENCH_HOT_FILES=50000 \
    ///     cargo test --lib db::turso::bench::tests::db_bench_relationship_scale \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: tag -> file lookups at scale, partitioned vs single"]
    fn db_bench_relationship_scale() {
        let hot_tags = env_u64("BENCH_HOT_TAGS", 100).max(1);
        let hot_files = env_u64("BENCH_HOT_FILES", 20_000).max(1);
        let rare_tags = env_u64("BENCH_RARE_TAGS", 200_000);
        let rare_files = env_u64("BENCH_RARE_FILES", 3).max(1);
        let arms = env_u64("BENCH_ARMS", 32).max(1) as usize;
        let iters = env_u64("BENCH_ITERS", 7).max(1);
        let rewrite = env_u64("BENCH_REWRITE", 20_000);

        let hot_row_count = hot_tags * hot_files;
        let rare_row_count = rare_tags * rare_files;
        let total_rows = hot_row_count + rare_row_count;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH relationship scale: {total_rows} rows \
                 ({hot_tags} hot tags x {hot_files} files + \
                 {rare_tags} rare tags x {rare_files} files), {arms} arms"
            );

            for (label, partitioned) in [("partitioned", true), ("single", false)] {
                let path = bench_db_path(&format!("relscale-{}", label));
                bench_remove(&path);
                let db = Builder::new_local(&path.to_string_lossy())
                    .build()
                    .await
                    .unwrap();
                let conn = db.connect().unwrap();
                conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();

                let table_for = |tag_id: u64| -> String {
                    if partitioned {
                        format!("Relationship_{}", tag_id % arms as u64)
                    } else {
                        "Relationship".to_string()
                    }
                };
                let mut ddl = String::new();
                if partitioned {
                    for arm in 0..arms {
                        ddl.push_str(&format!(
                            "CREATE TABLE Relationship_{arm} (
                                 file_id INTEGER NOT NULL,
                                 tag_id INTEGER NOT NULL,
                                 PRIMARY KEY (tag_id, file_id));
                             CREATE INDEX idx_Relationship_{arm}_tag_file
                                 ON Relationship_{arm} (file_id, tag_id);"
                        ));
                    }
                } else {
                    ddl.push_str(
                        "CREATE TABLE Relationship (
                             file_id INTEGER NOT NULL,
                             tag_id INTEGER NOT NULL,
                             PRIMARY KEY (tag_id, file_id));
                         CREATE INDEX idx_Relationship_file_tag
                             ON Relationship (file_id, tag_id);",
                    );
                }
                conn.execute_batch(&ddl).await.unwrap();

                // The rows. Hot tags get contiguous file ids so the posting
                // list is a contiguous key range in both shapes -- the case
                // that should favour the single table most.
                let mut per_table: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
                let mut push = |tag_id: u64, file_id: u64| {
                    per_table
                        .entry(table_for(tag_id))
                        .or_default()
                        .push((file_id, tag_id));
                };
                let mut file_id = 0u64;
                for hot in 0..hot_tags {
                    for _f in 0..hot_files {
                        push(hot + 1, file_id + 1);
                        file_id += 1;
                    }
                }
                for rare in 0..rare_tags {
                    let tag_id = hot_tags + rare + 1;
                    for _f in 0..rare_files {
                        push(tag_id, file_id + 1);
                        file_id += 1;
                    }
                }
                let seed_started = Instant::now();
                for (table, mut rows) in per_table {
                    rows.sort_unstable_by_key(|(file_id, tag_id)| (*tag_id, *file_id));
                    for chunk in rows.chunks(SQL_CHUNK_SIZE) {
                        let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?)").collect();
                        let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
                        for (file_id, tag_id) in chunk {
                            params.push(Value::from(*file_id as i64));
                            params.push(Value::from(*tag_id as i64));
                        }
                        conn.execute(
                            format!(
                                "INSERT INTO {table} (file_id, tag_id) VALUES {}",
                                holders.join(", ")
                            ),
                            params_from_iter(params),
                        )
                        .await
                        .unwrap();
                    }
                }
                let seed_ms = seed_started.elapsed().as_secs_f64() * 1_000.0;

                // ---- the queries --------------------------------------
                // A hot tag's posting list, which is what the search
                // planner drives on and what a re-tag chunk rewrites.
                let hot_tag = hot_tags / 2;
                let rare_tag = hot_tags + rare_tags / 2;

                let mut hot_scan_ms = f64::MAX;
                let mut hot_scan_rows = 0usize;
                for iter in 0..iters {
                    let started = Instant::now();
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT file_id FROM {} WHERE tag_id = ?1 \
                                 ORDER BY file_id LIMIT 50;",
                                table_for(hot_tag)
                            ),
                            params_from_iter([Value::from(hot_tag as i64)]),
                        )
                        .await
                        .unwrap();
                    let mut count = 0usize;
                    while let Ok(Some(_)) = rows.next().await {
                        count += 1;
                    }
                    if iter == 0 {
                        hot_scan_rows = count;
                        explain(
                            &conn,
                            &format!(
                                "SELECT file_id FROM {} WHERE tag_id = {} \
                                 ORDER BY file_id LIMIT 50;",
                                table_for(hot_tag),
                                hot_tag
                            ),
                        )
                        .await;
                    } else {
                        hot_scan_ms = hot_scan_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                }

                // The whole posting list, no LIMIT. This is the shape that
                // degrades fastest as a table grows, because it reads every
                // row the tag owns.
                let mut hot_full_ms = f64::MAX;
                let mut hot_full_rows = 0usize;
                for iter in 0..iters {
                    let started = Instant::now();
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT file_id FROM {} WHERE tag_id = ?1;",
                                table_for(hot_tag)
                            ),
                            params_from_iter([Value::from(hot_tag as i64)]),
                        )
                        .await
                        .unwrap();
                    let mut count = 0usize;
                    while let Ok(Some(_)) = rows.next().await {
                        count += 1;
                    }
                    if iter == 0 {
                        hot_full_rows = count;
                    } else {
                        hot_full_ms = hot_full_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                }

                // A rare tag's posting list: the common case, and the one the
                // search planner picks as its driver.
                let mut rare_scan_ms = f64::MAX;
                for iter in 0..iters {
                    let started = Instant::now();
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT file_id FROM {} WHERE tag_id = ?1 \
                                 ORDER BY file_id LIMIT 50;",
                                table_for(rare_tag)
                            ),
                            params_from_iter([Value::from(rare_tag as i64)]),
                        )
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                    if iter > 0 {
                        rare_scan_ms = rare_scan_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                }

                // The recount aggregate: `GROUP BY tag_id` over the whole
                // table, which `slurp_recount_namespace` runs per namespace.
                // For the partitioned shape that is one aggregate *per
                // partition*, so the total is the sum over all of them --
                // comparing one partition against the whole table would
                // flatter the partitioned shape by the arm count.
                let recount_started = Instant::now();
                let mut recount_rows = 0usize;
                // One aggregate per partition for the partitioned shape; one
                // global aggregate for the single table. The partitioned
                // shape's total is the sum over its partitions, so this
                // compares like with like.
                let recount_arms = if partitioned { arms } else { 1 };
                for arm in 0..recount_arms {
                    let sql = if partitioned {
                        format!("SELECT tag_id, COUNT(*) FROM Relationship_{arm} GROUP BY tag_id;")
                    } else {
                        "SELECT tag_id, COUNT(*) FROM Relationship GROUP BY tag_id;".to_string()
                    };
                    let mut rows = conn.query(&sql, ()).await.unwrap();
                    while let Ok(Some(_)) = rows.next().await {
                        recount_rows += 1;
                    }
                }
                let recount_ms = recount_started.elapsed().as_secs_f64() * 1_000.0;

                // ---- the write ----------------------------------------
                // Re-tag a hot tag: the chunk that the production comment
                // calls out as a 24k-row rewrite, and the one that made the
                // MVCC commit-log path expensive. The file ids are *novel*,
                // past the end of the seeded range, so this is a real insert
                // rather than a duplicate probe -- `INSERT OR IGNORE` against
                // rows that already exist measures the index lookup, not the
                // write.
                let novel_base = total_rows + 1;
                let rewrite_rows: Vec<(u64, u64)> =
                    (0..rewrite).map(|f| (novel_base + f, hot_tag)).collect();
                let mut rewrite_per_table: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
                for &(file_id, tag_id) in &rewrite_rows {
                    rewrite_per_table
                        .entry(table_for(tag_id))
                        .or_default()
                        .push((file_id, tag_id));
                }
                let write_started = Instant::now();
                for (table, mut rows) in rewrite_per_table {
                    rows.sort_unstable_by_key(|(file_id, tag_id)| (*tag_id, *file_id));
                    for chunk in rows.chunks(SQL_CHUNK_SIZE) {
                        let holders: Vec<&str> = chunk.iter().map(|_| "(?, ?)").collect();
                        let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 2);
                        for (file_id, tag_id) in chunk {
                            params.push(Value::from(*file_id as i64));
                            params.push(Value::from(*tag_id as i64));
                        }
                        conn.execute(
                            format!(
                                "INSERT OR IGNORE INTO {table} (file_id, tag_id) VALUES {}",
                                holders.join(", ")
                            ),
                            params_from_iter(params),
                        )
                        .await
                        .unwrap();
                    }
                }
                let write_ms = write_started.elapsed().as_secs_f64() * 1_000.0;

                // ---- churn: build MVCC history ------------------------
                // This is the variable the earlier benches left out. Every
                // insert and delete leaves a new version behind, and a page
                // with a long version chain costs more to read. A table that
                // has been scraped for a year has deep chains; a freshly
                // seeded one does not. The churn phase creates the history,
                // and the hot-tag scan is then measured again, so the claim
                // "one table degrades because its history is deeper" is
                // tested rather than assumed.
                let churn_rounds = env_u64("BENCH_CHURN_ROUNDS", 20).max(1);
                let churn_rows = env_u64("BENCH_CHURN_ROWS", 2_000);
                let churn_started = Instant::now();
                for round in 0..churn_rounds {
                    let base = novel_base + rewrite + round * churn_rows;
                    for &(file_id, tag_id) in &(0..churn_rows)
                        .map(|f| (base + f, hot_tag))
                        .collect::<Vec<_>>()
                    {
                        let _ = (file_id, tag_id);
                    }
                    let rows_in: Vec<(u64, u64)> =
                        (0..churn_rows).map(|f| (base + f, hot_tag)).collect();
                    let holders: Vec<&str> = rows_in.iter().map(|_| "(?, ?)").collect();
                    let mut params: Vec<Value> = Vec::with_capacity(rows_in.len() * 2);
                    for (file_id, tag_id) in &rows_in {
                        params.push(Value::from(*file_id as i64));
                        params.push(Value::from(*tag_id as i64));
                    }
                    conn.execute(
                        format!(
                            "INSERT OR IGNORE INTO {} (file_id, tag_id) VALUES {}",
                            table_for(hot_tag),
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
                }
                let churn_ms = churn_started.elapsed().as_secs_f64() * 1_000.0;

                // The hot tag scan, again, with the history behind it.
                let mut hot_scan_after_ms = f64::MAX;
                let mut hot_full_after_ms = f64::MAX;
                for iter in 0..iters {
                    let started = Instant::now();
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT file_id FROM {} WHERE tag_id = ?1 \
                                 ORDER BY file_id LIMIT 50;",
                                table_for(hot_tag)
                            ),
                            params_from_iter([Value::from(hot_tag as i64)]),
                        )
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                    if iter > 0 {
                        hot_scan_after_ms =
                            hot_scan_after_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }

                    let started = Instant::now();
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT file_id FROM {} WHERE tag_id = ?1;",
                                table_for(hot_tag)
                            ),
                            params_from_iter([Value::from(hot_tag as i64)]),
                        )
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                    if iter > 0 {
                        hot_full_after_ms =
                            hot_full_after_ms.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                }

                println!(
                    "  {label}\n    \
                     seed {total_rows} rows        {:>9.1} ms ({:.1} us/row)\n    \
                     hot tag, first 50 files     {:>9.3} ms  ({hot_scan_rows} rows)\n    \
                     hot tag, whole list         {:>9.3} ms  ({hot_full_rows} rows)\n    \
                     rare tag, first 50 files    {:>9.3} ms\n    \
                     GROUP BY tag_id             {:>9.1} ms  ({recount_rows} groups)\n    \
                     rewrite {rewrite} rows       {:>9.1} ms\n    \
                     churn {churn_rounds}x{churn_rows}     {:>9.1} ms\n    \
                     after churn, first 50       {:>9.3} ms\n    \
                     after churn, whole list     {:>9.3} ms",
                    seed_ms,
                    seed_ms * 1_000.0 / total_rows as f64,
                    hot_scan_ms,
                    hot_full_ms,
                    rare_scan_ms,
                    recount_ms,
                    write_ms,
                    churn_ms,
                    hot_scan_after_ms,
                    hot_full_after_ms,
                );
            }
        });
    }

    /// Why a tag's posting list gets slower over time, and whether the
    /// MVCC knobs can pull it back.
    ///
    /// The scale bench showed the hot tag's whole-list scan going from 36 ms
    /// to 288 ms after a churn of 40k rows, in *both* the partitioned and the
    /// single-table shape. That is not the table shape -- it is version
    /// depth. Every insert leaves a new row version behind, and a page with a
    /// long version chain costs more to read.
    ///
    /// Limbo's answer to that is an incremental GC: `mvcc_gc_threshold`
    /// (16,384 new versions by default) triggers a pass that walks at most
    /// `MAX_CHAINS_PER_GC` (4,096) row chains per pass. Under heavy write
    /// load the pass cannot keep up with the chains being created, so they
    /// grow.
    ///
    /// This measures the read latency at a series of version depths, then
    /// drops the GC threshold and re-measures, to answer two questions:
    ///
    /// * how much of the slowdown is version depth, and
    /// * whether a more aggressive GC recovers it, and what it costs on the
    ///   write path.
    ///
    /// ```text
    /// BENCH_MVCC_HOT=20000 BENCH_MVCC_CHURN=100000 cargo test --lib \
    ///     db::turso::bench::tests::db_bench_mvcc_history -- --ignored --nocapture
    /// ```
    /// Whether `tag_count_lock` has to be global, or can be sharded by tag id.
    ///
    /// The lock exists because `Tags.count` is one row per tag and two writers
    /// bumping the *same* tag's count would conflict (a write-write conflict
    /// under MVCC, which aborts and retries the whole transaction). What it
    /// actually guards is per-row, but the mutex is global, so two writers
    /// bumping *different* tags serialise anyway.
    ///
    /// This measures how much that costs. The shape is the real one: many
    /// concurrent folds-in, each carrying deltas for its own set of tags, with
    /// the sets overlapping or disjoint. If disjoint sets are much faster than
    /// overlapping ones under a global lock, the lock is over-broad and sharding
    /// it would recover the difference.
    ///
    /// The comparison is run in one process against one warm fixture, with the
    /// global and sharded paths alternating, and each reporting the best of
    /// several rounds. Two separate processes cannot answer this: the fixture
    /// and the page cache state dominate, which is exactly how the earlier
    /// count-path comparison produced two contradictory numbers.
    ///
    /// ```text
    /// BENCH_LOCK_TAGS=20000 BENCH_LOCK_BATCH=200 BENCH_LOCK_CONCURRENCY=8 \\
    ///     cargo test --release --lib db::turso::bench::tests::db_bench_count_lock \\
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: global vs sharded tag count lock"]
    fn db_bench_count_lock() {
        let tags = env_u64("BENCH_LOCK_TAGS", 20_000).max(4);
        let batch = env_u64("BENCH_LOCK_BATCH", 200).max(1) as usize;
        let concurrency = env_u64("BENCH_LOCK_CONCURRENCY", 8).max(1) as usize;
        let rounds = env_u64("BENCH_LOCK_ROUNDS", 3).max(1);
        let shards = env_u64("BENCH_LOCK_SHARDS", 32).max(1) as usize;
        let threshold = 5i64;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH tag count lock: {tags} tags, {batch} tags per fold-in, \
                 {concurrency} concurrent writers, {shards} shards"
            );

            let path = bench_db_path("taglock");
            bench_remove(&path);
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&path, should_exit).await;
            let conn = db.connect().unwrap();

            let mut ns_set = HashSet::new();
            for ns in 0..4 {
                ns_set.insert(GenericNamespaceObj {
                    name: format!("lock_ns_{ns}"),
                    description: None,
                });
            }
            db.namespace_ensure_set(&ns_set).await.unwrap();

            let tag_set: HashSet<Tag> = (0..tags)
                .map(|i| Tag {
                    name: format!("lock_tag_{i}"),
                    namespace: GenericNamespaceObj {
                        name: format!("lock_ns_{}", i % 4),
                        description: None,
                    },
                })
                .collect();
            let inserted = db.tag_add_bulk(&conn, &tag_set).await.unwrap();
            let mut tag_ids: Vec<u64> = inserted.iter().map(|t| t.id as u64).collect();
            tag_ids.sort_unstable();

            // Seed a count so the rows exist at a realistic width.
            for chunk in tag_ids.chunks(SQL_CHUNK_SIZE) {
                let (sql, params) =
                    crate::db::turso::relationship::tag_count_seed_sql_for_bench(chunk);
                conn.execute(&sql, params_from_iter(params)).await.unwrap();
            }

            // Each round: `concurrency` writers each fold in `batch` deltas.
            // `overlap` decides whether the writers share tags. Disjoint sets
            // are the case a sharded lock can parallelise; fully overlapping
            // sets are the case it cannot.
            //
            // The fold-in is driven through the real `tag_counts_apply`, so the
            // lock under test is the production one. The sharded variant is
            // simulated by running the same folds-in concurrently but
            // grouping the batch by shard first, which is the shape a
            // sharded implementation would produce.
            // One round: `concurrency` writers each fold in `batch` deltas
            // through the real `tag_counts_apply`, so the lock under test is
            // the production one. `overlap` picks whether the writers share
            // tags: disjoint sets are what a sharded lock could parallelise,
            // fully overlapping sets are what it cannot.
            async fn fold_round(
                db: &Arc<TursoDatabase>,
                tag_ids: &[u64],
                batch: usize,
                concurrency: usize,
                tags: u64,
                overlap: bool,
            ) -> Duration {
                let started = Instant::now();
                let mut handles = Vec::with_capacity(concurrency);
                for w in 0..concurrency {
                    let db = db.clone();
                    let ids: Vec<u64> = tag_ids.to_vec();
                    handles.push(tokio::spawn(async move {
                        let mut deltas: HashMap<u64, u64> = HashMap::new();
                        for b in 0..batch {
                            let idx = if overlap {
                                b % tags as usize
                            } else {
                                (w * batch + b) % tags as usize
                            };
                            *deltas.entry(ids[idx]).or_default() += 1;
                        }
                        db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
                started.elapsed()
            }

            let mut disjoint = f64::MAX;
            let mut overlap_ms = f64::MAX;
            for _ in 0..rounds {
                let e = fold_round(&db, &tag_ids, batch, concurrency, tags, false).await;
                disjoint = disjoint.min(e.as_secs_f64() * 1_000.0);
                let e = fold_round(&db, &tag_ids, batch, concurrency, tags, true).await;
                overlap_ms = overlap_ms.min(e.as_secs_f64() * 1_000.0);
            }

            let per_writer = |ms: f64| ms / concurrency as f64;
            println!(
                "  disjoint tag sets:   {disjoint:>9.2} ms total  ({:.2} ms/writer)",
                per_writer(disjoint)
            );
            println!(
                "  overlapping sets:    {overlap_ms:>9.2} ms total  ({:.2} ms/writer)",
                per_writer(overlap_ms)
            );
            println!(
                "  disjoint is {:.2}x {}",
                overlap_ms / disjoint.max(0.001),
                if disjoint < overlap_ms * 0.9 {
                    "FASTER -- the global lock serialises work that need not conflict"
                } else {
                    "about the same -- the lock is not the constraint"
                }
            );
            let _ = threshold;
        });
    }

    /// BENCH tag count sharding: does splitting one `Tags.count` row into N
    /// counter rows per tag actually buy anything?
    ///
    /// The claim under test is that the count fold-in is expensive not because
    /// of the global `tag_count_lock` but because of MVCC version chains: every
    /// writer bumps the *same* row, so that row accumulates one version per
    /// write and every reader walks the chain. `db_bench_count_lock` measured
    /// the lock and found it was not the constraint; this measures the thing it
    /// blamed instead.
    ///
    /// The design isolates the mechanism by aiming every update at one tag.
    /// Both arms then write exactly the same number of rows, so the only
    /// difference is how those writes are spread:
    ///
    ///   * `one row`   — all updates land on `(tag 0, shard 0)`, the production
    ///     shape. One row, deep chain.
    ///   * `sharded`   — the same updates spread over `shards` rows for tag 0.
    ///     Shallow chains, but reads must now sum `shards` rows.
    ///
    /// Each arm gets a freshly recreated table. Version chains persist for the
    /// life of the row, so running both arms against one fixture would let the
    /// first arm's churn set the second arm's starting depth — which is how a
    /// comparison like this ends up reporting the order the arms ran in.
    ///
    /// Reports write throughput and, before and after the churn, the read cost
    /// of the hot tag (point read) and of every tag (the `count DESC` list).
    /// Sharding only pays if the shorter chain beats the wider read.
    ///
    /// ```text
    /// BENCH_SHARD_TAGS=2000 BENCH_SHARD_UPDATES=20000 BENCH_SHARD_CONCURRENCY=8 \
    /// BENCH_SHARD_SHARDS=32 cargo test --release --lib \
    ///     db::turso::bench::tests::db_bench_tag_count_shard -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: sharded tag counters vs one row per tag"]
    fn db_bench_tag_count_shard() {
        let tags = env_u64("BENCH_SHARD_TAGS", 2_000).max(2);
        let updates = env_u64("BENCH_SHARD_UPDATES", 20_000).max(1);
        let concurrency = env_u64("BENCH_SHARD_CONCURRENCY", 8).max(1) as usize;
        let shards = env_u64("BENCH_SHARD_SHARDS", 32).max(1) as u64;
        let batch = env_u64("BENCH_SHARD_BATCH", 100).max(1) as usize;
        let rounds = env_u64("BENCH_SHARD_ROUNDS", 3).max(1);
        let all_tags = env_u64("BENCH_SHARD_SCAN_ALL", 200).max(1) as u64;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH tag count shard: {tags} tags, {updates} updates all on tag 0, \
                 {concurrency} writers, {shards} shards, batch {batch}, {rounds} rounds"
            );

            let path = bench_db_path("tagshard");
            bench_remove(&path);
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&path, should_exit).await;
            let conn = db.connect().unwrap();

            // The checkpoint threshold bounds how many row versions survive
            // before the oldest are folded away. If the superlinear write cost
            // above is really chain depth, lowering this should flatten it --
            // and that would be a one-line fix rather than a sharded schema.
            let ckpt = env_u64("BENCH_SHARD_CKPT", 0);
            if ckpt > 0 {
                conn.execute_batch(&format!(
                    "PRAGMA mvcc_checkpoint_threshold = {ckpt};"
                ))
                .await
                .unwrap();
                println!("  (mvcc_checkpoint_threshold = {ckpt}, default 4120000)");
            }

            // Read the hot tag and the popularity list. Both are measured as a
            // batch of point reads so the per-read figure is not drowned in
            // round-trip overhead.
            async fn read_hot(conn: &Connection, passes: u64) -> f64 {
                let mut best = f64::MAX;
                for _ in 0..3 {
                    let started = Instant::now();
                    for _ in 0..passes {
                        let mut rows = conn
                            .query(
                                "SELECT count FROM TagCount WHERE tag_id = 0 AND shard = 0;",
                                (),
                            )
                            .await
                            .unwrap();
                        while let Ok(Some(_)) = rows.next().await {}
                    }
                    best = best.min(started.elapsed().as_secs_f64() * 1_000.0);
                }
                best
            }

            // The sharded read: the same value now costs a scan over the tag's
            // shards, which is the price the design pays for the shorter chain.
            async fn read_hot_sharded(conn: &Connection, passes: u64) -> f64 {
                let mut best = f64::MAX;
                for _ in 0..3 {
                    let started = Instant::now();
                    for _ in 0..passes {
                        let mut rows = conn
                            .query("SELECT SUM(count) FROM TagCount WHERE tag_id = 0;", ())
                            .await
                            .unwrap();
                        while let Ok(Some(_)) = rows.next().await {}
                    }
                    best = best.min(started.elapsed().as_secs_f64() * 1_000.0);
                }
                best
            }

            async fn read_all(conn: &Connection, tags: u64) -> f64 {
                let mut best = f64::MAX;
                for _ in 0..3 {
                    let started = Instant::now();
                    let mut rows = conn
                        .query(
                            "SELECT tag_id, SUM(count) FROM TagCount \
                             WHERE tag_id < ?1 GROUP BY tag_id;",
                            (tags as i64,),
                        )
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                    best = best.min(started.elapsed().as_secs_f64() * 1_000.0);
                }
                best
            }

            // Recreate the fixture. `spread` false puts every row on shard 0
            // (the production shape); true spreads them over all shards.
            async fn reset(conn: &Connection, tags: u64, shards: u64, spread: bool) {
                conn.execute_batch("DROP TABLE IF EXISTS TagCount;")
                    .await
                    .unwrap();
                conn.execute_batch(
                    "CREATE TABLE TagCount (
                         tag_id INTEGER NOT NULL,
                         shard  INTEGER NOT NULL,
                         count  INTEGER NOT NULL DEFAULT 0,
                         PRIMARY KEY (tag_id, shard));",
                )
                .await
                .unwrap();

                let width = if spread { shards } else { 1 };
                for start in (0..tags).step_by(100) {
                    let end = (start + 100).min(tags);
                    // Each tag contributes `width` rows, so the placeholder
                    // list is per row, not per tag.
                    let rows = (end - start) * width;
                    let holders: Vec<String> =
                        std::iter::repeat_n("(?, ?, 0)".to_string(), rows as usize).collect();
                    let mut params: Vec<Value> = Vec::with_capacity(rows as usize * 2);
                    for tag_id in start..end {
                        for shard in 0..width {
                            params.push(Value::from(tag_id as i64));
                            params.push(Value::from(shard as i64));
                        }
                    }
                    conn.execute(
                        format!(
                            "INSERT INTO TagCount (tag_id, shard, count) VALUES {}",
                            holders.join(", ")
                        ),
                        params_from_iter(params),
                    )
                    .await
                    .unwrap();
                }
            }

            // One arm: `concurrency` writers each apply their share of the
            // updates in batches, one transaction per batch. A transaction per
            // batch is what `tag_counts_apply` does, so the version count per
            // row matches production rather than being an artefact of autocommit.
            async fn churn(
                db: &Arc<TursoDatabase>,
                updates: u64,
                concurrency: usize,
                batch: usize,
                shards: u64,
                spread: bool,
                ckpt: u64,
            ) -> f64 {
                let per_writer = (updates / concurrency as u64).max(1);
                let started = Instant::now();
                let mut handles = Vec::with_capacity(concurrency);
                for w in 0..concurrency {
                    let db = db.clone();
                    handles.push(tokio::spawn(async move {
                        let mut conn = db.connect().unwrap();
                        // `connect()` in production sets a 1 s busy timeout;
                        // without it a writer that loses the race for the write
                        // lock fails instantly instead of waiting its turn.
                        let _ = conn.busy_timeout(std::time::Duration::from_secs(1));
                        if ckpt > 0 {
                            let _ = conn
                                .execute_batch(&format!(
                                    "PRAGMA mvcc_checkpoint_threshold = {ckpt};"
                                ))
                                .await;
                        }
                        let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ (w as u64 + 1);
                        let mut next = move || {
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            state
                        };
                        // `serialized_tx_behavior` (BEGIN IMMEDIATE) is what
                        // production uses. It still returns `Busy` rather than
                        // waiting, so the write is wrapped in the same bounded
                        // retry `retry_mvcc` gives production: lose the race,
                        // back off, try the batch again.
                        let mut done = 0u64;
                        while done < per_writer {
                            let n = batch.min((per_writer - done) as usize) as u64;
                            let mut attempts = 0u32;
                            loop {
                                let started = conn
                                    .transaction_with_behavior(
                                        TursoDatabase::serialized_tx_behavior(),
                                    )
                                    .await;
                                let tx = match started {
                                    Ok(tx) => tx,
                                    Err(error) if is_busy(&error) => {
                                        attempts += 1;
                                        assert!(attempts < 64, "writer never got the lock: {error}");
                                        tokio::time::sleep(std::time::Duration::from_millis(5))
                                            .await;
                                        continue;
                                    }
                                    Err(error) => panic!("begin failed: {error}"),
                                };
                                let mut failed = None;
                                for _ in 0..n {
                                    let shard = if spread { next() % shards } else { 0 };
                                    if let Err(error) = tx
                                        .execute(
                                            "UPDATE TagCount SET count = count + 1 \
                                             WHERE tag_id = 0 AND shard = ?1;",
                                            (shard as i64,),
                                        )
                                        .await
                                    {
                                        failed = Some(error);
                                        break;
                                    }
                                }
                                if failed.is_none() {
                                    match tx.commit().await {
                                        Ok(()) => break,
                                        Err(error) if is_busy(&error) => {
                                            attempts += 1;
                                            assert!(
                                                attempts < 64,
                                                "writer never committed: {error}"
                                            );
                                            tokio::time::sleep(
                                                std::time::Duration::from_millis(5),
                                            )
                                            .await;
                                            continue;
                                        }
                                        Err(error) => panic!("commit failed: {error}"),
                                    }
                                }
                                let error = failed.unwrap();
                                if is_busy(&error) {
                                    attempts += 1;
                                    assert!(attempts < 64, "writer never wrote: {error}");
                                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                                    continue;
                                }
                                panic!("update failed: {error}");
                            }
                            done += n;
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
                started.elapsed().as_secs_f64() * 1_000.0
            }

            let passes = 200u64;
            let mut report: Vec<(&str, f64, f64, f64, f64, f64)> = Vec::new();

            for (label, spread) in [("one row (production)", false), ("sharded", true)] {
                // Best of `rounds` full cycles, so a single unlucky pass does
                // not decide the comparison.
                let mut best = (
                    f64::MAX, f64::MAX, f64::MAX, f64::MAX, f64::MAX,
                );
                for _ in 0..rounds {
                    reset(&conn, tags, shards, spread).await;
                    // Arm-consistent read: the sharded design pays the SUM even
                    // when cold, so comparing its cold SUM against the single
                    // row's cold point read measures the wrong thing.
                    let before_hot = if spread {
                        read_hot_sharded(&conn, passes).await
                    } else {
                        read_hot(&conn, passes).await
                    };
                    let before_all = read_all(&conn, all_tags).await;
                    let write_ms =
                        churn(&db, updates, concurrency, batch, shards, spread, ckpt).await;
                    let after_hot = if spread {
                        read_hot_sharded(&conn, passes).await
                    } else {
                        read_hot(&conn, passes).await
                    };
                    let after_all = read_all(&conn, all_tags).await;
                    // Control: within this one table, read the churned row and
                    // an untouched row back to back. Any difference is version
                    // depth and nothing else -- same index, same table, same
                    // page cache. The across-arm comparison cannot say this,
                    // because the two arms also differ in row count and index
                    // width.
                    let mut churned = f64::MAX;
                    let mut untouched = f64::MAX;
                    for _ in 0..3 {
                        let started = Instant::now();
                        for _ in 0..passes {
                            let mut rows = conn
                                .query(
                                    "SELECT count FROM TagCount WHERE tag_id = 0 AND shard = 0;",
                                    (),
                                )
                                .await
                                .unwrap();
                            while let Ok(Some(_)) = rows.next().await {}
                        }
                        churned = churned.min(started.elapsed().as_secs_f64() * 1_000.0);

                        let started = Instant::now();
                        for _ in 0..passes {
                            let mut rows = conn
                                .query(
                                    "SELECT count FROM TagCount WHERE tag_id = 1 AND shard = 0;",
                                    (),
                                )
                                .await
                                .unwrap();
                            while let Ok(Some(_)) = rows.next().await {}
                        }
                        untouched = untouched.min(started.elapsed().as_secs_f64() * 1_000.0);
                    }
                    println!(
                        "    {label:<22} churned row {churned:>8.3} ms                            untouched row {untouched:>8.3} ms                            ({:.2}x)",
                        churned / untouched.max(0.0001)
                    );
                    best = (
                        best.0.min(before_hot),
                        best.1.min(before_all),
                        best.2.min(write_ms),
                        best.3.min(after_hot),
                        best.4.min(after_all),
                    );
                }
                report.push((label, best.0, best.1, best.2, best.3, best.4));
            }

            // Plans, so a slow figure is attributed rather than guessed at.
            // Printed once: they do not vary between rounds.
            for (label, spread) in [("one row (production)", false), ("sharded", true)] {
                reset(&conn, tags, shards, spread).await;
                println!("  plans [{label}]:");
                explain(&conn, "SELECT count FROM TagCount WHERE tag_id = 0 AND shard = 0;").await;
                explain(&conn, "SELECT SUM(count) FROM TagCount WHERE tag_id = 0;").await;
                explain(
                    &conn,
                    "SELECT tag_id, SUM(count) FROM TagCount WHERE tag_id < 1 GROUP BY tag_id;",
                )
                .await;
            }

            println!(
                "  {:<22} {:>11} {:>11} {:>11} {:>11} {:>11}",
                "arm", "hot before", "list before", "write ms", "hot after", "list after"
            );
            for (label, before_hot, before_all, write_ms, after_hot, after_all) in &report {
                println!(
                    "  {label:<22} {before_hot:>8.3} ms {before_all:>8.3} ms \
                     {write_ms:>8.1} ms {after_hot:>8.3} ms {after_all:>8.3} ms"
                );
            }

            if let [(_, one_before, _, one_write, one_after, _), (_, many_before, _, many_write, many_after, _)] =
                report.as_slice()
            {
                let (one_before, one_write, one_after) = (*one_before, *one_write, *one_after);
                let (many_before, many_write, many_after) =
                    (*many_before, *many_write, *many_after);
                println!(
                    "  hot read after churn: {:.3} -> {:.3} ms ({:.2}x)",
                    one_after,
                    many_after,
                    one_after / many_after.max(0.0001)
                );
                println!(
                    "  write throughput:     {:.0} -> {:.0} ms ({:.2}x)",
                    one_write,
                    many_write,
                    one_write / many_write.max(0.0001)
                );
                println!(
                    "  read before churn:    {:.3} -> {:.3} ms (sharded overhead {:.2}x)",
                    one_before,
                    many_before,
                    many_before / one_before.max(0.0001)
                );
            }
        });
    }

    #[test]
    #[ignore = "manual benchmark: MVCC version depth and its effect on reads"]
    fn db_bench_mvcc_history() {
        let hot = env_u64("BENCH_MVCC_HOT", 20_000).max(1);
        let churn = env_u64("BENCH_MVCC_CHURN", 100_000);
        let chunk = env_u64("BENCH_MVCC_CHUNK", 5_000);
        let gc_threshold = env_u64("BENCH_MVCC_GC_THRESHOLD", 1_024).max(1) as i64;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH mvcc history: {hot} rows on the hot tag, \
                 {churn} churn rows in {chunk}-row chunks"
            );

            let path = bench_db_path("mvcc-history");
            bench_remove(&path);
            let db = Builder::new_local(&path.to_string_lossy())
                .experimental_mvcc_passive_checkpoint(true)
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();
            // The checkpoint threshold is the size of the logical log (in
            // bytes) that triggers a checkpoint. Left alone it is 4,120,000.
            // Lowering it makes the engine materialize committed rows into
            // the b-tree more often, which is the suspected cause of the
            // recovery at the end of the run.
            let ckpt_threshold = env_u64("BENCH_MVCC_CKPT_THRESHOLD", 0);
            if ckpt_threshold > 0 {
                conn.execute_batch(&format!(
                    "PRAGMA mvcc_checkpoint_threshold = {ckpt_threshold};"
                ))
                .await
                .unwrap();
                println!(
                    "    (mvcc_checkpoint_threshold set to {ckpt_threshold}, \
                     default is 4120000)"
                );
            }
            conn.execute_batch(
                "CREATE TABLE Relationship (
                     file_id INTEGER NOT NULL,
                     tag_id INTEGER NOT NULL,
                     PRIMARY KEY (tag_id, file_id));",
            )
            .await
            .unwrap();

            // Seed the hot tag's posting list.
            for start in (0..hot).step_by(SQL_CHUNK_SIZE) {
                let end = (start + SQL_CHUNK_SIZE as u64).min(hot);
                let holders: Vec<String> = (start..end).map(|_| "(?, ?)".to_string()).collect();
                let mut params: Vec<Value> = Vec::new();
                for file_id in start..end {
                    params.push(Value::from(file_id as i64 + 1));
                    params.push(Value::from(1i64));
                }
                conn.execute(
                    format!(
                        "INSERT INTO Relationship (file_id, tag_id) VALUES {}",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await
                .unwrap();
            }

            // The read under test: the hot tag's whole posting list.
            async fn measure(conn: &Connection, label: &str) -> f64 {
                let mut best = f64::MAX;
                for _ in 0..5 {
                    let started = Instant::now();
                    let mut rows = conn
                        .query("SELECT file_id FROM Relationship WHERE tag_id = 1;", ())
                        .await
                        .unwrap();
                    while let Ok(Some(_)) = rows.next().await {}
                    let elapsed = started.elapsed().as_secs_f64() * 1_000.0;
                    if elapsed < best {
                        best = elapsed;
                    }
                }
                println!("    {label:<44} {best:>9.3} ms");
                best
            }

            let baseline = measure(&conn, "baseline (no churn)").await;

            // Churn in chunks, measuring the read as the versions pile up.
            let mut file_id = hot + 1;
            let mut inserted = 0u64;
            let mut write_ms = 0.0f64;
            let mut series: Vec<(u64, f64)> = Vec::new();
            while inserted < churn {
                let n = chunk.min(churn - inserted);
                let holders: Vec<String> = (0..n).map(|_| "(?, ?)".to_string()).collect();
                let mut params: Vec<Value> = Vec::new();
                for _ in 0..n {
                    params.push(Value::from(file_id as i64));
                    params.push(Value::from(1i64));
                    file_id += 1;
                }
                let started = Instant::now();
                conn.execute(
                    format!(
                        "INSERT INTO Relationship (file_id, tag_id) VALUES {}",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await
                .unwrap();
                write_ms += started.elapsed().as_secs_f64() * 1_000.0;
                inserted += n;
                let elapsed = measure(
                    &conn,
                    &format!("after {inserted} churn rows ({write_ms:.0} ms written)"),
                )
                .await;
                series.push((inserted, elapsed));
            }

            // Now try to pull it back: drop the GC threshold and let a few
            // more writes trigger the incremental passes. The write cost is
            // timed separately here, because a more aggressive GC is not free
            // -- it has to walk chains on the commit path.
            conn.execute_batch(&format!("PRAGMA mvcc_gc_threshold = {gc_threshold};"))
                .await
                .unwrap();
            println!("    (mvcc_gc_threshold set to {gc_threshold}, default is 16384)");
            let mut gc_write_ms = 0.0f64;
            let mut gc_rows = 0u64;
            for _ in 0..8 {
                let holders: Vec<String> = (0..chunk).map(|_| "(?, ?)".to_string()).collect();
                let mut params: Vec<Value> = Vec::new();
                for _ in 0..chunk {
                    params.push(Value::from(file_id as i64));
                    params.push(Value::from(1i64));
                    file_id += 1;
                }
                let started = Instant::now();
                conn.execute(
                    format!(
                        "INSERT INTO Relationship (file_id, tag_id) VALUES {}",
                        holders.join(", ")
                    ),
                    params_from_iter(params),
                )
                .await
                .unwrap();
                gc_write_ms += started.elapsed().as_secs_f64() * 1_000.0;
                gc_rows += chunk;
            }
            let after_gc = measure(&conn, "after aggressive GC + 8 more chunks").await;

            let worst = series.last().map(|(_, v)| *v).unwrap_or(0.0);
            let rows_now = hot + churn + gc_rows;
            println!(
                "    baseline {baseline:.3} ms -> worst {worst:.3} ms \
                 -> after GC {after_gc:.3} ms"
            );
            println!(
                "    write cost: {:.2} us/row at the default threshold, \
                 {:.2} us/row at mvcc_gc_threshold = {gc_threshold}",
                write_ms * 1_000.0 / churn as f64,
                gc_write_ms * 1_000.0 / gc_rows as f64,
            );
            println!(
                "    per-row read cost: baseline {:.2} us/row (20k rows), \
                 after GC {:.2} us/row ({rows_now} rows)",
                baseline * 1_000.0 / hot as f64,
                after_gc * 1_000.0 / rows_now as f64,
            );
        });
    }

    /// The search path end to end, on both relationship stores.
    ///
    /// The relationship-scale bench measures the store primitives. This one
    /// drives the actual entry points the UI calls -- `search_db_files` and
    /// `search_db_files_count` -- because the plan those produce is not the
    /// same as the primitive's: the planner picks the rarest single-tag holder
    /// as the driving scan and turns every other holder into an `EXISTS`
    /// predicate, so the cost depends on how many holders a query has and which
    /// of them is rarest.
    ///
    /// Four query shapes, in increasing cost:
    ///
    /// * `single` -- one tag, the common "show me files tagged X".
    /// * `and2` -- two tags, both required.
    /// * `and3` -- three tags, the shape a real multi-facet search produces.
    /// * `and2_not1` -- two required and one excluded, which is the most
    ///   predicates and therefore the most probes per candidate row.
    ///
    /// Each is run as a row fetch and as a count, since the count walks the
    /// same scan but aggregates.
    ///
    /// ```text
    /// BENCH_SEARCH_FILES=40000 BENCH_SEARCH_TAGS=400 \
    ///     cargo test --release --lib db::turso::bench::tests::db_bench_search_path \
    ///     -- --ignored --nocapture
    /// ```
    /// The plan behind `search_db_files` vs `search_db_files_count`.
    ///
    /// The count path takes `row_select`, appends the predicates, adds
    /// `LIMIT COUNT_SCAN_CAP + 1` and wraps it in `SELECT COUNT(*) FROM (...)`.
    /// That drops the `ORDER BY` the row fetch carries, so the two statements
    /// are not the same scan with a different projection -- the count loses the
    /// index-ordered early termination and has to materialize the whole match
    /// set. The bench shows the cost (10x on an AND-2 query); this prints the
    /// plans so the difference is attributable rather than inferred.
    /// A/B the count statement against the old wrapped form, in one process.
    ///
    /// The whole point is that the two forms are measured against the *same*
    /// warm fixture. Running them as separate processes -- or back to back
    /// after a reseed -- makes the comparison meaningless at scale: a 3.3M-file
    /// fixture is larger than what stays comfortably cached alongside a
    /// multi-GB writer, so page-cache state and machine load dominate the
    /// difference and two identical runs can disagree by 3x.
    ///
    /// Here the fixture is seeded once, then the old and new statements are
    /// alternated for several rounds, and each form reports the *minimum*
    /// across rounds. Alternating means both forms pay the same cache
    /// conditions on average, and the minimum discards a round that was
    /// interrupted by something outside the measurement.
    ///
    /// ```text
    /// BENCH_AB_FILES=2000000 BENCH_AB_TAGS=400 BENCH_AB_ROUNDS=5 \
    ///     cargo test --release --lib db::turso::bench::tests::db_bench_count_ab \
    ///     -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual benchmark: count statement old vs new, same fixture"]
    fn db_bench_count_ab() {
        let files = env_u64("BENCH_AB_FILES", 500_000).max(1);
        let tags = env_u64("BENCH_AB_TAGS", 200).max(2);
        let tags_per_file = env_u64("BENCH_AB_TAGS_PER_FILE", 6).max(1);
        let rounds = env_u64("BENCH_AB_ROUNDS", 5).max(1);
        let arms = env_u64("BENCH_ARMS", 32).max(1) as usize;
        let cap = TursoDatabase::COUNT_SCAN_CAP + 1;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH count A/B: {files} files, {tags} tags, \
                 {tags_per_file} tags/file, {rounds} rounds"
            );

            let path = bench_db_path("count-ab");
            bench_remove(&path);
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&path, should_exit).await;
            let conn = db.connect().unwrap();
            {
                let c = db.connect().unwrap();
                for (name, value) in [
                    ("relationship_write_mode", "dual"),
                    ("relationship_read_mode", "flat"),
                ] {
                    db.setting_set(
                        &c,
                        shared_types::DbSettingsObj {
                            name: name.into(),
                            description: None,
                            num: None,
                            param: Some(value.into()),
                        },
                    )
                    .await
                    .unwrap();
                }
            }

            let mut ns_set = HashSet::new();
            for ns in 0..arms {
                ns_set.insert(GenericNamespaceObj {
                    name: format!("ab_ns_{ns}"),
                    description: None,
                });
            }
            db.namespace_ensure_set(&ns_set).await.unwrap();

            let storage_id = db
                .file_storage_location_get_or_create(&conn, "ab_store")
                .await
                .unwrap();
            let file_objs: Vec<FileInternal> = (0..files)
                .map(|i| FileInternal {
                    id: None,
                    hash: format!("ab{i:09}"),
                    extension: "jpg".into(),
                    storage_id,
                    size_bytes: Some(1),
                })
                .collect();
            let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
            let file_ids: Vec<u64> = {
                let mut v: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();
                v.sort_unstable();
                v
            };

            let mut tag_ids: Vec<u64> = Vec::with_capacity(tags as usize);
            for t in 0..tags {
                let set = db
                    .tag_add_bulk(
                        &conn,
                        &HashSet::from([Tag {
                            name: format!("ab_tag_{t}"),
                            namespace: GenericNamespaceObj {
                                name: format!("ab_ns_{}", (t as usize) % arms),
                                description: None,
                            },
                        }]),
                    )
                    .await
                    .unwrap();
                tag_ids.push(set.iter().next().unwrap().id as u64);
            }

            // Contiguous tag runs, so adjacent tags co-occur and an AND query
            // has a real intersection to compute.
            let rels: HashSet<(u64, u64)> = file_ids
                .iter()
                .enumerate()
                .flat_map(|(i, f)| {
                    let t = tag_ids.clone();
                    (0..tags_per_file as usize).map(move |k| (*f, t[(i + k) % tags as usize]))
                })
                .collect();
            let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
            db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

            let mid = (tags / 2) as usize;
            let pick = |offset: usize| tag_ids[(mid + offset) % tags as usize];

            // The two statements, for an AND-2 query.
            let search = SearchObj {
                search_relate: None,
                searches: vec![SearchHolder::And(vec![pick(0), pick(1)])],
            };
            let (row_sql, _, conditions, _) = db
                .build_search_parts(&conn, &search)
                .await
                .unwrap()
                .unwrap();

            let mut inner = row_sql.clone();
            for condition in &conditions {
                inner.push_str(condition);
            }
            // The old form: wrap the row select and LIMIT the outer count.
            let old_sql = format!("SELECT COUNT(*) FROM ({inner} LIMIT {cap})");
            // The new form: constant projection on the driver, LIMIT inside.
            let from_at = inner
                .find(" FROM ")
                .map(|at| at + " FROM ".len())
                .unwrap_or(0);
            let new_sql = format!(
                "SELECT COUNT(*) FROM (SELECT 1 FROM {} LIMIT {cap})",
                &inner[from_at..]
            );

            let run = |sql: String, conn: Connection| {
                Box::pin(async move {
                    let mut best = f64::MAX;
                    let mut last = 0i64;
                    for _ in 0..rounds {
                        let t0 = Instant::now();
                        let mut rows = conn.query(&sql, ()).await.unwrap();
                        let mut n = 0i64;
                        while let Ok(Some(row)) = rows.next().await {
                            n = row.get(0).unwrap_or(0);
                        }
                        let elapsed = t0.elapsed().as_secs_f64() * 1_000.0;
                        best = best.min(elapsed);
                        last = n;
                    }
                    (best, last)
                })
            };

            let (old_ms, old_n) = run(old_sql.clone(), conn.clone()).await;
            let (new_ms, new_n) = run(new_sql.clone(), conn.clone()).await;

            println!("  old (wrapped row select): {old_ms:>10.3} ms   -> {old_n}");
            println!("  new (capped aggregate):   {new_ms:>10.3} ms   -> {new_n}");
            if old_ms > 0.0 {
                println!("  ratio old/new: {:.2}x", old_ms / new_ms.max(0.001));
            }
            if old_n != new_n {
                println!(
                    "  WARNING: the two forms disagree ({old_n} vs {new_n}); \
                     the cap or the count is wrong"
                );
            }
            println!("  (min of {rounds} rounds each, same warm fixture)");
        });
    }

    #[test]
    #[ignore = "manual: print the search row vs count plans"]
    fn db_bench_search_count_plan() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = bench_db_path("searchplan");
            bench_remove(&path);
            let should_exit = Arc::new(AtomicBool::new(false));
            let db = TursoDatabase::new_with_exit(&path, should_exit).await;
            let conn = db.connect().unwrap();
            {
                let c = db.connect().unwrap();
                for (name, value) in [
                    ("relationship_write_mode", "dual"),
                    ("relationship_read_mode", "flat"),
                ] {
                    db.setting_set(
                        &c,
                        shared_types::DbSettingsObj {
                            name: name.into(),
                            description: None,
                            num: None,
                            param: Some(value.into()),
                        },
                    )
                    .await
                    .unwrap();
                }
            }
            db.namespace_ensure_set(&HashSet::from([GenericNamespaceObj {
                name: "plan_ns".into(),
                description: None,
            }]))
            .await
            .unwrap();
            let storage_id = db
                .file_storage_location_get_or_create(&conn, "plan_store")
                .await
                .unwrap();
            let objs: Vec<FileInternal> = (0..2_000)
                .map(|i| FileInternal {
                    id: None,
                    hash: format!("plan{i:06}"),
                    extension: "jpg".into(),
                    storage_id,
                    size_bytes: Some(1),
                })
                .collect();
            let inserted = db.file_add_bulk(&conn, &objs).await.unwrap();
            let file_ids: Vec<u64> = {
                let mut v: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();
                v.sort_unstable();
                v
            };
            let mut tag_ids = Vec::new();
            for t in 0..4u64 {
                let set = db
                    .tag_add_bulk(
                        &conn,
                        &HashSet::from([Tag {
                            name: format!("plan_tag_{t}"),
                            namespace: GenericNamespaceObj {
                                name: "plan_ns".into(),
                                description: None,
                            },
                        }]),
                    )
                    .await
                    .unwrap();
                tag_ids.push(set.iter().next().unwrap().id as u64);
            }
            let rels: HashSet<(u64, u64)> = file_ids
                .iter()
                .enumerate()
                .flat_map(|(i, f)| {
                    let tags = tag_ids.clone();
                    (0..3u64).map(move |k| (*f, tags[(i + k as usize) % 4]))
                })
                .collect();
            let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
            db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

            let search = SearchObj {
                search_relate: None,
                searches: vec![SearchHolder::And(vec![tag_ids[0], tag_ids[1]])],
            };

            // The row statement exactly as `build_search_sql` assembles it.
            let row_sql = db
                .build_search_sql(&conn, &search, &Some(50), &None)
                .await
                .unwrap()
                .unwrap();

            // The count statement exactly as `search_db_files_count` assembles
            // it now: the aggregate the builder produces, plus the predicates.
            let (_, count_select, conditions, _) = db
                .build_search_parts(&conn, &search)
                .await
                .unwrap()
                .unwrap();
            let mut count_sql = count_select.clone();
            for condition in &conditions {
                count_sql.push_str(condition);
            }
            // And the *old* wrapping form, so the two plans are side by side.
            let row_select = db
                .build_search_parts(&conn, &search)
                .await
                .unwrap()
                .unwrap()
                .0;
            let mut old_inner = row_select.clone();
            for condition in &conditions {
                old_inner.push_str(condition);
            }
            old_inner.push_str(&format!(" LIMIT {}", TursoDatabase::COUNT_SCAN_CAP + 1));
            let old_count_sql = format!("SELECT COUNT(*) FROM ({old_inner})");

            println!("\nBENCH search plan: AND-2 over {} files", file_ids.len());
            for (label, sql) in [
                ("row (search_db_files)", &row_sql),
                ("count (new: aggregate)", &count_sql),
                ("count (old: wrapped row)", &old_count_sql),
            ] {
                println!("  {label}:");
                let mut rows = conn
                    .query(&format!("EXPLAIN QUERY PLAN {sql}"), ())
                    .await
                    .unwrap();
                while let Ok(Some(row)) = rows.next().await {
                    let detail: String = row.get(3).unwrap_or_default();
                    println!("    {detail}");
                }
            }
        });
    }

    #[test]
    #[ignore = "manual benchmark: search_db_files / _count on both stores"]
    fn db_bench_search_path() {
        let files = env_u64("BENCH_SEARCH_FILES", 20_000).max(10);
        let tags = env_u64("BENCH_SEARCH_TAGS", 200).max(10);
        let iters = env_u64("BENCH_SEARCH_ITERS", 9).max(1);
        // Tags per file. Low enough that a tag's posting list is a meaningful
        // fraction of the library, high enough that multi-holder queries have
        // real intersections to compute.
        let tags_per_file = env_u64("BENCH_SEARCH_TAGS_PER_FILE", 6).max(1);
        let arms = env_u64("BENCH_ARMS", 32).max(1) as usize;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!(
                "\nBENCH search path: {files} files, {tags} tags, \
                 {tags_per_file} tags/file, {arms} partitions"
            );

            // `BENCH_SEARCH_MODE=flat|legacy|both` selects which store to
            // measure. A 20M-row fixture takes long enough to seed that
            // re-running it for the second store is its own cost, and a run
            // that dies mid-way through the second pass would otherwise lose
            // both.
            let mode = std::env::var("BENCH_SEARCH_MODE").unwrap_or_else(|_| "both".to_string());
            let variants: Vec<(&str, &str)> = match mode.as_str() {
                "flat" => vec![("flat", "flat")],
                "legacy" => vec![("legacy", "legacy")],
                _ => vec![("flat", "flat"), ("legacy", "legacy")],
            };
            for (label, read_mode) in variants {
                let path = bench_db_path(&format!("searchpath-{label}"));
                bench_remove(&path);
                let should_exit = Arc::new(AtomicBool::new(false));
                let db = TursoDatabase::new_with_exit(&path, should_exit).await;
                let conn = db.connect().unwrap();

                // Both stores get written, so the two read modes see identical
                // data and the comparison is about the read plan alone.
                {
                    let c = db.connect().unwrap();
                    for (name, value) in [
                        ("relationship_write_mode", "dual"),
                        ("relationship_read_mode", read_mode),
                    ] {
                        db.setting_set(
                            &c,
                            shared_types::DbSettingsObj {
                                name: name.into(),
                                description: None,
                                num: None,
                                param: Some(value.into()),
                            },
                        )
                        .await
                        .unwrap();
                    }
                }

                // Namespaces: one per partition, so the legacy store has to
                // probe a different table per tag.
                let mut ns_set = HashSet::new();
                for ns in 0..arms {
                    ns_set.insert(GenericNamespaceObj {
                        name: format!("sbench_ns_{ns}"),
                        description: None,
                    });
                }
                db.namespace_ensure_set(&ns_set).await.unwrap();

                let storage_id = db
                    .file_storage_location_get_or_create(&conn, "sbench_store")
                    .await
                    .unwrap();
                let file_objs: Vec<FileInternal> = (0..files)
                    .map(|i| FileInternal {
                        id: None,
                        hash: format!("sbench{i:08}"),
                        extension: "jpg".into(),
                        storage_id,
                        size_bytes: Some(1),
                    })
                    .collect();
                let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
                let file_ids: Vec<u64> = {
                    let mut v: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();
                    v.sort_unstable();
                    v
                };

                // Tags laid out round-robin across namespaces, so a tag's
                // namespace is a stable function of its index.
                let mut tag_ids: Vec<u64> = Vec::with_capacity(tags as usize);
                for t in 0..tags {
                    let ns_idx = (t as usize) % arms;
                    let tag = Tag {
                        name: format!("sbench_tag_{t}"),
                        namespace: GenericNamespaceObj {
                            name: format!("sbench_ns_{ns_idx}"),
                            description: None,
                        },
                    };
                    let set = db.tag_add_bulk(&conn, &HashSet::from([tag])).await.unwrap();
                    tag_ids.push(set.iter().next().unwrap().id as u64);
                }

                // Each file gets a *contiguous* run of tags. This matters: a
                // strided assignment (`i + k*7`) makes two adjacent tags almost
                // never co-occur, so an AND query over them matches nothing and
                // the bench measures an empty worst case instead of a real
                // intersection. Contiguous runs mean tag t and tag t+1 share
                // every file whose run spans both, which is what a real
                // multi-facet search looks like.
                let mut rels: HashSet<(u64, u64)> = HashSet::new();
                for (i, file_id) in file_ids.iter().enumerate() {
                    for k in 0..tags_per_file {
                        let tag_id = tag_ids[((i as u64 + k) % tags) as usize];
                        rels.insert((*file_id, tag_id));
                    }
                }
                let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
                db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

                // The query shapes. Pick tags from the middle of the list so
                // they are neither the first nor the last seeded.
                let mid = (tags / 2) as usize;
                let pick = |offset: usize| tag_ids[(mid + offset) % tags as usize];
                let shapes: Vec<(&str, SearchObj)> = vec![
                    (
                        "single",
                        SearchObj {
                            search_relate: None,
                            searches: vec![SearchHolder::And(vec![pick(0)])],
                        },
                    ),
                    (
                        "and2",
                        SearchObj {
                            search_relate: None,
                            searches: vec![SearchHolder::And(vec![pick(0), pick(1)])],
                        },
                    ),
                    (
                        "and3",
                        SearchObj {
                            search_relate: None,
                            searches: vec![SearchHolder::And(vec![pick(0), pick(1), pick(2)])],
                        },
                    ),
                    (
                        "and2_not1",
                        SearchObj {
                            search_relate: None,
                            searches: vec![
                                SearchHolder::And(vec![pick(0), pick(1)]),
                                SearchHolder::Not(vec![pick(3)]),
                            ],
                        },
                    ),
                ];

                println!("  {label} (read_mode={read_mode}):");
                for (shape, search) in &shapes {
                    // Warm, then best-of-N.
                    let mut rows_ms = f64::MAX;
                    let mut count_ms = f64::MAX;
                    let mut matched = 0usize;
                    for iter in 0..iters {
                        let t0 = Instant::now();
                        let rows = db
                            .search_db_files(&conn, search, &Some(50), &None)
                            .await
                            .unwrap();
                        let row_elapsed = t0.elapsed().as_secs_f64() * 1_000.0;
                        if iter == 0 {
                            matched = rows.len();
                        }

                        let t1 = Instant::now();
                        let total = db.search_db_files_count(&conn, search).await.unwrap();
                        let count_elapsed = t1.elapsed().as_secs_f64() * 1_000.0;
                        if iter == 0 {
                            assert!(
                                total >= matched as u64,
                                "{shape}: count {total} must be at least the paged rows {matched}"
                            );
                        }

                        if iter > 0 {
                            rows_ms = rows_ms.min(row_elapsed);
                            count_ms = count_ms.min(count_elapsed);
                        }
                    }
                    println!(
                        "    {shape:<10} rows(LIMIT 50) {rows_ms:>8.3} ms   \
                         count {count_ms:>8.3} ms   ({matched} rows paged)"
                    );
                }
            }
        });
    }
}
