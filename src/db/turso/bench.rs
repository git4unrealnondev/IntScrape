//! Benchmarks for the database functions that sit on the hot paths.
//!
//! Manual benchmarks, following the convention of the scale benchmarks in
//! `search.rs` and `relationship.rs`: ignored tests that build a realistic
//! fixture, then print per-row latency. Run them with
//!
//! ```text
//! cargo test --bin intscrape db::turso::bench -- --ignored --nocapture
//! ```
//!
//! Every knob is an environment variable so a run can be scaled without
//! editing code. Defaults are deliberately small enough to finish in seconds;
//! the interesting numbers come from scaling up:
//!
//! ```text
//! BENCH_FILES=200000 BENCH_TAGS=200000 cargo test --bin intscrape \
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
        FileInternal, GenericNamespaceObj, PluginJob, ScraperParam, Tag, TagParents,
    };
    use turso::Connection;

    use crate::db::SQL_CHUNK_SIZE;
    use crate::db::turso::sql::{parents_insert_sql, parents_params};
    use crate::db::turso::{TagDb, TursoDatabase};

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
        conn: Connection,
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
                &format!(
                    "SELECT id, hash FROM File WHERE hash IN ({})",
                    std::iter::repeat_n("?", 8).collect::<Vec<_>>().join(", ")
                ),
            )
            .await;
            explain(&conn, "SELECT id, hash, extension, storage_id, size_bytes FROM File WHERE id > 0 ORDER BY id LIMIT 100").await;

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
                limit_to: if i % 3 == 0 {
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
                    limit_to: if i % 3 == 0 {
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
                    limit_to: if i % 3 == 0 {
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
                    limit_to: if i % 3 == 0 {
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
                                parent.limit_to.unwrap_or(0) as i64,
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
                limit_to: if i % 3 == 0 {
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
        let conn = fixture.conn.clone();
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
                limit_to: if i % 3 == 0 {
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
                    limit_to: if i % 3 == 0 {
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
                    limit_to: if i % 3 == 0 {
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
}
