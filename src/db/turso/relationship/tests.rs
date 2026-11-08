//! Tests for the `relationship` module.
//!
//! Extracted from `relationship.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `relationship` module itself.

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

// ---------------------------------------------------------------------
// Relationship write-path scale bench.
//
// Isolates the three changes made to the bulk relationship paths:
//
//  * `relationship_bulk_delete` groups by tag and deletes with
//    `tag_id = ? AND file_id IN (...)`. The previous per-relationship
//    `(file_id = ? AND tag_id = ?) OR ...` form hit limbo's 100-deep
//    expression limit at around a hundred rows, so any `Del`/`Set` over
//    ~100 relationships raised `Parse error: Expression tree is too large`
//    instead of deleting. This bench drives far past that.
//  * Both bulk paths resolve each *distinct* tag's namespace once instead
//    of once per relationship, which only shows up when tags repeat — so
//    the shape here is deliberately tag-heavy (few tags, many files).
//  * `tag_counts_apply` no longer clones both delta maps inside
//    `tag_count_lock`, and builds its per-piece `CASE` list from a slice
//    rather than a fresh `HashMap`.
//
// Env: REL_FILES (default 20000), REL_TAGS (default 200),
// REL_TAGS_PER_FILE (default 5), REL_DELETES (default 20000). Writes go
// through the real production functions inside a transaction, exactly as the
// scraper's relationship phase does.
// ---------------------------------------------------------------------

fn rel_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore = "manual benchmark: relationship bulk add/delete at tag-heavy scale"]
fn relationship_write_path_scale_bench() {
    let files = rel_env("REL_FILES", 20_000);
    let tags = rel_env("REL_TAGS", 200);
    let deletes = rel_env("REL_DELETES", 20_000);

    let db_path = {
        let dir = if std::path::Path::new("/dev/shm").exists() {
            std::path::PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        };
        dir.join(format!("intscrape-relbench-{}.db", std::process::id()))
    };
    for suffix in ["", "-log", "-wal", "-shm"] {
        let mut p = db_path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(p));
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let should_exit = Arc::new(AtomicBool::new(false));
        let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
        let conn = db.connect().unwrap();

        let ns = GenericNamespaceObj {
            name: "relbench".into(),
            description: None,
        };
        db.namespace_ensure_set(&HashSet::from([ns.clone()]))
            .await
            .unwrap();

        // Few tags, many files: the shape where a `Set` re-tag or a bulk
        // delete touches the same handful of tags thousands of times.
        let mut tag_ids: Vec<u64> = Vec::new();
        for batch_start in (0..tags).step_by(200) {
            let batch: HashSet<Tag> = (batch_start..(batch_start + 200).min(tags))
                .map(|t| Tag {
                    name: format!("relbenchtag{t}"),
                    namespace: ns.clone(),
                })
                .collect();
            let set = db.tag_add_bulk(&conn, &batch).await.unwrap();
            tag_ids.extend(set.iter().map(|t| t.id as u64));
        }
        tag_ids.sort_unstable();
        tag_ids.dedup();
        assert_eq!(tag_ids.len() as u64, tags);

        let storage_id = db
            .file_storage_location_get_or_create(&conn, "relbench_storage")
            .await
            .unwrap();
        let mut file_ids: Vec<u64> = Vec::new();
        for batch_start in (0..files).step_by(1000) {
            let end = (batch_start + 1000).min(files);
            let objs: Vec<FileInternal> = (batch_start..end)
                .map(|i| FileInternal {
                    id: None,
                    hash: format!("relbenchhash{i:09}"),
                    extension: "jpg".into(),
                    storage_id,
                    size_bytes: Some(1),
                })
                .collect();
            let inserted = db.file_add_bulk(&conn, &objs).await.unwrap();
            file_ids.extend(inserted.iter().map(|f| f.id.unwrap()));
        }
        eprintln!("RELBENCH seeded {files} files, {tags} tags, 1 namespace");

        // Each file carries a handful of tags drawn from the whole pool, so
        // the set has far more relationships than distinct tags: the shape
        // the namespace-resolution dedupe targets.
        let per_file = rel_env("REL_TAGS_PER_FILE", 5).max(1) as usize;
        let mut relationships: HashSet<(u64, u64)> = HashSet::new();
        for (index, file_id) in file_ids.iter().enumerate() {
            for k in 0..per_file {
                let tag = tag_ids[(index + k * 7) % tag_ids.len()];
                relationships.insert((*file_id, tag));
            }
        }
        eprintln!(
            "RELBENCH {} relationships over {} distinct tags",
            relationships.len(),
            tag_ids.len()
        );

        // Phase 1: bulk add.
        let mut add_conn = db.connect().unwrap();
        let add_tx = add_conn
            .transaction_with_behavior(TursoDatabase::write_tx_behavior())
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let add_deltas = db
            .relationships_bulk_add(&add_tx, &relationships)
            .await
            .unwrap();
        let add_s = started.elapsed();
        let commit_started = std::time::Instant::now();
        add_tx.commit().await.unwrap();
        let add_commit_s = commit_started.elapsed();
        println!(
            "RELBENCH relationships_bulk_add  {:>8.1} ms + commit {:>7.1} ms \
             ({} rows, {} deltas, {:.2} us/row)",
            add_s.as_secs_f64() * 1000.0,
            add_commit_s.as_secs_f64() * 1000.0,
            relationships.len(),
            add_deltas.len(),
            add_s.as_secs_f64() * 1e6 / relationships.len() as f64
        );

        // Phase 2: count fold-in (the serialized writer).
        let started = std::time::Instant::now();
        db.tag_counts_apply(&add_deltas, &HashMap::new())
            .await
            .unwrap();
        println!(
            "RELBENCH tag_counts_apply        {:>8.1} ms ({} deltas)",
            started.elapsed().as_secs_f64() * 1000.0,
            add_deltas.len()
        );

        // Phase 3: bulk delete, far past the old ~100-row ceiling.
        // Delete a slice of the set, so the rows really do exist and the
        // reported deltas must account for all of them.
        let doomed: HashSet<(u64, u64)> = relationships
            .iter()
            .copied()
            .take(deletes.min(relationships.len() as u64) as usize)
            .collect();
        let mut del_conn = db.connect().unwrap();
        let del_tx = del_conn
            .transaction_with_behavior(TursoDatabase::write_tx_behavior())
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let del_deltas = db.relationship_bulk_delete(&del_tx, &doomed).await.unwrap();
        let del_s = started.elapsed();
        del_tx.commit().await.unwrap();
        println!(
            "RELBENCH relationship_bulk_delete{:>8.1} ms ({} rows, {} deltas, {:.2} us/row)",
            del_s.as_secs_f64() * 1000.0,
            doomed.len(),
            del_deltas.len(),
            del_s.as_secs_f64() * 1e6 / doomed.len().max(1) as f64
        );

        let started = std::time::Instant::now();
        db.tag_counts_apply(&HashMap::new(), &del_deltas)
            .await
            .unwrap();
        println!(
            "RELBENCH tag_counts_apply (del)  {:>8.1} ms ({} deltas)",
            started.elapsed().as_secs_f64() * 1000.0,
            del_deltas.len()
        );

        // Sanity: the plan must still be a covering-index seek per tag, and
        // must not degrade into a partition scan.
        let del_conn = db.connect().unwrap();
        let sql = "DELETE FROM Relationship_1 WHERE tag_id = ? AND file_id IN (?, ?) RETURNING tag_id".to_string();
        let mut rows = del_conn
            .query(
                &format!("EXPLAIN QUERY PLAN {sql}"),
                turso::params_from_iter(vec![
                    turso::Value::from(tag_ids[0] as i64),
                    turso::Value::from(1i64),
                    turso::Value::from(2i64),
                ]),
            )
            .await
            .unwrap();
        let mut steps = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            steps.push(row.get::<String>(3).unwrap());
        }
        let joined = steps.join(" | ");
        println!("RELBENCH delete plan: {joined}");
        assert!(
            !joined.contains("SCAN relationship"),
            "grouped delete must not scan the partition: {joined}"
        );

        drop(del_conn);
        drop(conn);
        db.shutdown().await;
    });

    for suffix in ["", "-log", "-wal", "-shm"] {
        let mut p = db_path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(p));
    }
}

/// The union source must push its filter into every arm.
///
/// Limbo does not push a predicate through `UNION ALL`, so leaving the
/// `file_id IN (...)` outside made every file->tag lookup read *every*
/// relationship row in the database, even though each partition carries an
/// index on `file_id`. Measured on 1M relationships across 32 partitions,
/// looking up 200 files: 3,301 ms -> 60 ms.
///
/// This pins both halves: the plan must stay index-driven, and the pushed
/// form must return exactly what the unfiltered union scan would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_to_tag_lookup_pushes_its_filter_into_every_namespace_arm() {
    const FILES: u64 = 300;
    const NAMESPACES: [&str; 3] = ["alpha", "beta", "gamma"];

    let db = new_test_db().await;
    let conn = db.connect().unwrap();

    let mut ns_set = HashSet::new();
    for name in NAMESPACES {
        ns_set.insert(GenericNamespaceObj {
            name: name.into(),
            description: None,
        });
    }
    db.namespace_ensure_set(&ns_set).await.unwrap();

    let storage_id = db
        .file_storage_location_get_or_create(&conn, "test_storage")
        .await
        .unwrap();
    let file_objs: Vec<FileInternal> = (0..FILES)
        .map(|i| FileInternal {
            id: None,
            hash: format!("f2thash{i:05}"),
            extension: "jpg".into(),
            storage_id,
            size_bytes: Some(1),
        })
        .collect();
    let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
    let file_ids: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();

    // Spread tags across all three namespaces, several per file.
    let mut rels: HashSet<(u64, u64)> = HashSet::new();
    for (index, file_id) in file_ids.iter().enumerate() {
        let ns_name = NAMESPACES[index % NAMESPACES.len()];
        let tags: HashSet<Tag> = (0..3)
            .map(|k| Tag {
                name: format!("{ns_name}-f2t-{k}"),
                namespace: GenericNamespaceObj {
                    name: ns_name.into(),
                    description: None,
                },
            })
            .collect();
        let set = db.tag_add_bulk(&conn, &tags).await.unwrap();
        for tag in set.iter() {
            rels.insert((*file_id, tag.id as u64));
        }
    }
    let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
    db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

    // A scattered subset, so the lookup is not a contiguous range.
    let wanted: Vec<u64> = file_ids
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 7 == 3)
        .map(|(_, id)| *id)
        .collect();
    assert!(
        wanted.len() > 20,
        "need a multi-namespace lookup to be meaningful"
    );

    let pushed = db.file_id_get_tag_ids_bulk(&conn, &wanted).await.unwrap();

    // Reference: the same lookup expressed as one unfiltered scan per
    // namespace, which is what the union used to degenerate into.
    let mut expected: HashMap<u64, HashSet<u64>> = HashMap::new();
    let unfiltered = self_unfiltered_source(&conn).await;
    for chunk in wanted.chunks(SQL_CHUNK_SIZE) {
        let ph = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT file_id, tag_id FROM {unfiltered} WHERE file_id IN ({ph})");
        let params: Vec<Value> = chunk.iter().map(|id| Value::from(*id as i64)).collect();
        let mut rows = conn.query(&sql, params_from_iter(params)).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            let f: u64 = row.get(0).unwrap();
            let t: u64 = row.get(1).unwrap();
            expected.entry(f).or_default().insert(t);
        }
    }
    assert_eq!(
        pushed, expected,
        "pushing the filter into each arm must not change the result"
    );

    // The plan the hot path actually runs. Asserted on the statement the
    // function itself produces, so reverting it to the scanning form fails
    // here rather than passing on a separately-built statement.
    for (sql, params) in db
        .build_file_tag_lookup_statements(&conn, &wanted)
        .await
        .unwrap()
    {
        let mut rows = conn
            .query(
                &format!("EXPLAIN QUERY PLAN {sql}"),
                params_from_iter(params),
            )
            .await
            .unwrap();
        let mut steps: Vec<String> = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            steps.push(row.get::<String>(3).unwrap());
        }
        let joined = steps.join(" | ");
        eprintln!("PLAN: {joined}");
        assert!(
            !joined.contains("SCAN relationship"),
            "every namespace arm must be index-probed, not scanned: {joined}"
        );
    }

    // And show the shape this replaced, so the difference stays visible.
    let predicate = format!("file_id IN ({})", numbered_placeholders(wanted.len()));
    let unpushed = db
        .relationship_union_source(&conn, "r", None)
        .await
        .unwrap();
    let mut rows = conn
        .query(
            &format!(
                "EXPLAIN QUERY PLAN SELECT file_id, tag_id FROM {unpushed} WHERE {predicate}"
            ),
            params_from_iter(
                wanted
                    .iter()
                    .map(|id| Value::from(*id as i64))
                    .collect::<Vec<Value>>(),
            ),
        )
        .await
        .unwrap();
    let mut steps: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        steps.push(row.get::<String>(3).unwrap());
    }
    let old_plan = steps.join(" | ");
    eprintln!("PLAN (pre-fix shape): {old_plan}");
    assert!(
        old_plan.contains("SCAN relationship"),
        "the filter-outside-the-union shape is expected to scan; if limbo ever \
         learns to push through UNION ALL the pushdown can be dropped: {old_plan}"
    );

    drop(conn);
    db.shutdown().await;
}

/// The union source with no pushed predicate: the pre-optimization shape,
/// used here only as a correctness reference.
async fn self_unfiltered_source(conn: &Connection) -> String {
    let mut tables = Vec::new();
    let mut rows = conn
        .query("SELECT id FROM Namespace ORDER BY id;", ())
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let id: i64 = row.get(0).unwrap();
        tables.push(format!("Relationship_{id}"));
    }
    format!(
        "({}) AS r",
        tables
            .iter()
            .map(|t| format!("SELECT file_id, tag_id FROM {t}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    )
}

async fn tag_count(_db: &TursoDatabase, conn: &Connection, tag_id: u64) -> i64 {
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
    // The bulk add returns deltas instead of applying counts inline;
    // production folds them in after commit via `tag_counts_apply`.
    let add_deltas = db
        .relationships_bulk_add(&conn, &relationships)
        .await
        .unwrap();
    db.tag_counts_apply(&add_deltas, &HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        tag_count(&db, &conn, tag_id).await,
        2,
        "two relationships must increment the count twice"
    );

    let del_deltas = db
        .relationship_bulk_delete(&conn, &relationships)
        .await
        .unwrap();
    db.tag_counts_apply(&HashMap::new(), &del_deltas)
        .await
        .unwrap();
    assert_eq!(tag_count(&db, &conn, tag_id).await, 0);
}

/// `relationship_bulk_delete` used to build one
/// `WHERE (file_id = ? AND tag_id = ?) OR ...` term per relationship.
/// Limbo caps expression depth at 100, so the statement failed with
/// `Parse error: Expression tree is too large` at around a hundred rows —
/// far below `SQL_CHUNK_SIZE`, meaning any `TagOperation::Del` or `Set`
/// touching more than ~100 relationships errored out instead of deleting.
///
/// The fix groups by tag and deletes with `tag_id = ? AND file_id IN (...)`,
/// which the planner serves as a covering-index seek on the
/// `(tag_id, file_id)` primary key. This pins both halves: a delete set far
/// past the old depth limit must succeed, and the reported deltas must
/// still be exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_delete_handles_sets_far_past_the_expression_depth_limit() {
    const FILES: u64 = 400;
    const TAGS: u64 = 4;

    let db = new_test_db().await;
    let conn = db.connect().unwrap();

    let mut tag_ids: Vec<u64> = Vec::new();
    for t in 0..TAGS {
        let tags: HashSet<Tag> = HashSet::from([Tag {
            name: format!("bulktag{t}"),
            namespace: GenericNamespaceObj {
                name: "subject".into(),
                description: None,
            },
        }]);
        let set = db.tag_add_bulk(&conn, &tags).await.unwrap();
        tag_ids.push(set.iter().next().unwrap().id as u64);
    }
    let storage_id = db
        .file_storage_location_get_or_create(&conn, "test_storage")
        .await
        .unwrap();
    let file_objs: Vec<FileInternal> = (0..FILES)
        .map(|i| FileInternal {
            id: None,
            hash: format!("bulkhash{i:05}"),
            extension: "jpg".into(),
            storage_id,
            size_bytes: Some(1),
        })
        .collect();
    let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
    let file_ids: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();

    // Every file carries every tag: FILES * TAGS = 1600 relationships, well
    // past the old ~100 ceiling, and spread over only TAGS distinct tags so
    // the grouped statement collapses to TAGS statements.
    let relationships: HashSet<(u64, u64)> = file_ids
        .iter()
        .flat_map(|file_id| tag_ids.iter().map(move |tag_id| (*file_id, *tag_id)))
        .collect();
    assert_eq!(relationships.len() as u64, FILES * TAGS);

    let add_deltas = db
        .relationships_bulk_add(&conn, &relationships)
        .await
        .unwrap();
    db.tag_counts_apply(&add_deltas, &HashMap::new())
        .await
        .unwrap();
    for tag_id in &tag_ids {
        assert_eq!(
            tag_count(&db, &conn, *tag_id).await,
            FILES as i64,
            "every file carries this tag"
        );
    }

    // Delete half of them in one call: 800 relationships, 4 distinct tags.
    let doomed: HashSet<(u64, u64)> = file_ids
        .iter()
        .take(FILES as usize / 2)
        .flat_map(|file_id| tag_ids.iter().map(move |tag_id| (*file_id, *tag_id)))
        .collect();
    let del_deltas = db.relationship_bulk_delete(&conn, &doomed).await.unwrap();
    assert_eq!(
        del_deltas.values().sum::<u64>(),
        doomed.len() as u64,
        "deltas must account for every deleted relationship exactly once"
    );
    db.tag_counts_apply(&HashMap::new(), &del_deltas)
        .await
        .unwrap();
    for tag_id in &tag_ids {
        assert_eq!(
            tag_count(&db, &conn, *tag_id).await,
            (FILES / 2) as i64,
            "half the files removed"
        );
    }

    // The survivors must still be there, and the deletes must be idempotent:
    // re-deleting the same set reports nothing.
    let remaining: i64 = {
        let mut rows = conn
            .query("SELECT COUNT(*) FROM Relationship_1;", ())
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    };
    assert_eq!(
        remaining,
        (FILES / 2) as i64 * TAGS as i64,
        "only the undelivered half should remain"
    );

    let again = db.relationship_bulk_delete(&conn, &doomed).await.unwrap();
    assert!(
        again.is_empty(),
        "re-deleting an already-removed set must report no deltas"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relationship_activity_crosses_popularity_threshold_both_ways() {
    let db = new_test_db().await;
    let conn = db.connect().unwrap();

    let tags: HashSet<Tag> = HashSet::from([Tag {
        name: "red fox".into(),
        namespace: GenericNamespaceObj {
            name: "subject".into(),
            description: None,
        },
    }]);
    let tag_db_set = db.tag_add_bulk(&conn, &tags).await.unwrap();
    let tag_db: &TagDb = tag_db_set.iter().next().unwrap();
    let tag_id = tag_db.id as u64;

    // The FTS index only covers `count >= 5`: four relationships (count 4)
    // must stay unsearchable, the fifth crossing to 5 becomes searchable,
    // and one delete (back to 4) drops it again.
    for file_id in 1_u64..=4 {
        db.relationship_add(&conn, file_id, tag_id).await.unwrap();
    }
    assert_eq!(
        db.tags_search_fts("red f", 10).await.unwrap(),
        Vec::new(),
        "count 4 must not be searchable"
    );

    db.relationship_add(&conn, 5_u64, tag_id).await.unwrap();
    let found = db.tags_search_fts("red f", 10).await.unwrap();
    assert_eq!(
        found.len(),
        1,
        "count 5 must cross into the popular FTS index"
    );

    db.relationship_delete(&conn, 5_u64, tag_id).await.unwrap();
    assert_eq!(
        db.tags_search_fts("red f", 10).await.unwrap(),
        Vec::new(),
        "count 4 must drop back out of the popular FTS index"
    );
}

/// `tag_counts_apply` no longer re-asserts the whole delta batch into the
/// search shadow: it pre-reads which ids can actually change shadow
/// membership and writes only those. This pins that the filter is an exact
/// substitute, not a heuristic — every case where the shadow *should* move
/// must still move it, including the two the old unconditional sync got for
/// free (a delta that jumps clean over the threshold, and repairing a
/// shadow that has drifted out of sync on its own).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_fold_in_only_writes_shadow_when_membership_changes() {
    let db = new_test_db().await;
    let conn = db.connect().unwrap();

    async fn tag_id_for(db: &TursoDatabase, conn: &Connection, name: &str) -> u64 {
        let tags: HashSet<Tag> = HashSet::from([Tag {
            name: name.into(),
            namespace: GenericNamespaceObj {
                name: "subject".into(),
                description: None,
            },
        }]);
        let set = db.tag_add_bulk(conn, &tags).await.unwrap();
        set.iter().next().unwrap().id as u64
    }
    async fn count_of(conn: &Connection, tag_id: u64) -> i64 {
        let mut rows = conn
            .query("SELECT count FROM Tags WHERE id = ?1;", (tag_id as i64,))
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    let gradual = tag_id_for(&db, &conn, "gradual").await;
    // Climb to exactly the threshold one delta at a time.
    db.tag_counts_apply(&HashMap::from([(gradual, 4)]), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(count_of(&conn, gradual).await, 4);
    assert!(
        db.tags_search_fts("gradual", 10).await.unwrap().is_empty(),
        "count 4 must stay out of the shadow"
    );

    // +1 crosses 4 -> 5: the filter must let this one through.
    db.tag_counts_apply(&HashMap::from([(gradual, 1)]), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        db.tags_search_fts("gradual", 10).await.unwrap().len(),
        1,
        "crossing up to the threshold must become searchable"
    );

    // No crossing (+1, 5 -> 6) must not break or duplicate anything.
    db.tag_counts_apply(&HashMap::from([(gradual, 1)]), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        db.tags_search_fts("gradual", 10).await.unwrap().len(),
        1,
        "a non-crossing increment must leave the tag searchable"
    );

    // Crossing back down 6 -> 4 must drop it out again.
    db.tag_counts_apply(&HashMap::new(), &HashMap::from([(gradual, 2)]))
        .await
        .unwrap();
    assert_eq!(count_of(&conn, gradual).await, 4);
    assert!(
        db.tags_search_fts("gradual", 10).await.unwrap().is_empty(),
        "crossing down below the threshold must leave the shadow"
    );

    // A single delta that jumps clean over the threshold (0 -> 9) in one
    // step is the case a naive "did it move by one?" check would miss.
    let jumper = tag_id_for(&db, &conn, "jumper").await;
    db.tag_counts_apply(&HashMap::from([(jumper, 9)]), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(count_of(&conn, jumper).await, 9);
    assert_eq!(
        db.tags_search_fts("jumper", 10).await.unwrap().len(),
        1,
        "a delta that jumps past the threshold must become searchable"
    );

    // A decrement larger than the count clamps at 0 and must also leave
    // the shadow.
    db.tag_counts_apply(&HashMap::new(), &HashMap::from([(jumper, 100)]))
        .await
        .unwrap();
    assert_eq!(count_of(&conn, jumper).await, 0);
    assert!(
        db.tags_search_fts("jumper", 10).await.unwrap().is_empty(),
        "a clamped decrement to zero must leave the shadow"
    );

    // Drift repair: a popular tag whose shadow row vanished (interrupted
    // run, manual edit) must be restored even though the incoming delta
    // does not cross the threshold. This is the property that keeps the
    // filter equivalent to the old unconditional sync.
    let drifted = tag_id_for(&db, &conn, "drifted").await;
    db.tag_counts_apply(&HashMap::from([(drifted, 7)]), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        db.tags_search_fts("drifted", 10).await.unwrap().len(),
        1,
        "setup: drifted tag must start searchable"
    );
    conn.execute(
        "DELETE FROM Tags_Popular WHERE tag_id = ?1;",
        (drifted as i64,),
    )
    .await
    .unwrap();
    assert!(
        db.tags_search_fts("drifted", 10).await.unwrap().is_empty(),
        "setup: shadow row must actually be gone"
    );
    // Non-crossing delta (7 -> 8) that must nonetheless repair the shadow.
    db.tag_counts_apply(&HashMap::from([(drifted, 1)]), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        db.tags_search_fts("drifted", 10).await.unwrap().len(),
        1,
        "a non-crossing delta must still repair a drifted shadow row"
    );

    // And the mirror-image drift: a below-threshold tag wrongly left in
    // the shadow must be evicted.
    let stray = tag_id_for(&db, &conn, "straypop").await;
    conn.execute(
        "INSERT OR IGNORE INTO Tags_Popular(tag_id, name)
         SELECT id, name FROM Tags WHERE id = ?1;",
        (stray as i64,),
    )
    .await
    .unwrap();
    assert_eq!(
        db.tags_search_fts("straypop", 10).await.unwrap().len(),
        1,
        "setup: stray shadow row must be searchable"
    );
    // 0 -> 1, still below the threshold: the shadow row must be evicted.
    db.tag_counts_apply(&HashMap::from([(stray, 1)]), &HashMap::new())
        .await
        .unwrap();
    assert!(
        db.tags_search_fts("straypop", 10).await.unwrap().is_empty(),
        "a non-crossing delta must still evict a wrongly-present shadow row"
    );
}
