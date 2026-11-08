//! Tests for the `slurp` module.
//!
//! Extracted from `slurp.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `slurp` module itself.

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
/// The tempdir must outlive the connection and database, so all are returned.
async fn new_source(script: &str) -> (turso::Database, turso::Connection, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    let db = turso::Builder::new_local(&source_path.to_string_lossy())
        .experimental_without_rowid(true)
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch(script).await.unwrap();
    (db, conn, temp_dir)
}

/// Create a standalone source database on disk and run `script` against it.
/// Returns only the path; dropping the local handles persists the file so a
/// later `db_slurp` can reopen it read-only.
async fn write_source(source_path: &std::path::Path, script: &str) {
    let db = turso::Builder::new_local(&source_path.to_string_lossy())
        .experimental_without_rowid(true)
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch(script).await.unwrap();
    // Fold the WAL into the main file so a read-only reopen sees the schema
    // (matching a plain SQLite source file handed to db_slurp).
    conn.pragma_update("journal_mode", "'delete'")
        .await
        .unwrap();
    drop(conn);
    drop(db);
}

async fn target_jobs(db: &TursoDatabase) -> Vec<(String, u64, Option<String>, Option<String>)> {
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
    let (_source_db, source, _keep) = new_source(
        "CREATE TABLE Jobs (
             id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
             reptime INTEGER NOT NULL, priority INTEGER NOT NULL,
             recreation TEXT NOT NULL, site TEXT NOT NULL,
             param TEXT NOT NULL, user_data TEXT NOT NULL);
         INSERT INTO Jobs VALUES
             (1, 100, 60, 5, '{\"OnTagId\":[12,null]}', 'e621', '[]', '{\"k\":\"v\"}'),
             (2, 200, 0, 10, 'null', 'gelbooru', '[{\"Normal\":\"cute\"}]', '{}');",
    )
    .await;

    let conn = db.connect().unwrap();
    db.slurp_jobs(&conn, &source).await.unwrap();

    let jobs = target_jobs(&db).await;
    assert_eq!(jobs.len(), 2, "both legacy jobs must be copied");
    let (site, priority, recreation, user_data) = &jobs[0];
    assert_eq!(site, "e621");
    assert_eq!(*priority, 5);
    assert!(
        recreation.as_deref().unwrap().contains("OnTagId"),
        "recreation from recreation column"
    );
    assert_eq!(user_data.as_deref().unwrap(), "{\"k\":\"v\"}");
    assert_eq!(jobs[1].1, 10);
    assert_eq!(
        jobs[1].2,
        Some("null".into()),
        "empty recreation stays null"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_jobs_copies_rusthydrus_schema() {
    let db = new_target().await;
    let (_source_db, source, _keep) = new_source(
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
    )
    .await;

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
    assert_eq!(
        user_data.as_deref().unwrap(),
        "{\"user\":\"y\"}",
        "user data from UserData"
    );
    assert_eq!(jobs[1].0, "saucenao");
    assert_eq!(jobs[1].1, 10, "NULL priority defaults to 10");
    assert_eq!(jobs[1].2, Some("null".into()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_recomputes_counts_and_rebuilds_bulk_import_indexes() {
    let db = new_target().await;

    // Warm destination: the namespace, tags and a sentinel-limit parent
    // (the "no limit tag" -1) all already exist here. The source
    // duplicates that parent with a pre-v10 NULL, so after the slurp both
    // generations must dedupe to one row.
    {
        let conn = db.connect().unwrap();
        conn.execute(
            "INSERT INTO Namespace (id, name, description) VALUES (1, 'species', 'pre');",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO Tags (id, name, namespace) VALUES (1, 'mammal', 1), (2, 'canine', 1);",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, -1);",
            (),
        )
        .await
        .unwrap();
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    write_source(
        &source_path,
        "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES ('species', 'test');

         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES ('mammal', 1), ('canine', 1);

         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes)
             VALUES ('slurp-hash', 'jpg', 1, 42);

         CREATE TABLE Relationship_1 (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (tag_id, file_id));
         INSERT INTO Relationship_1 (file_id, tag_id) VALUES (1, 1), (1, 2);

         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
         INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
    )
    .await;

    let counts = db.db_slurp(&source_path).await.unwrap();
    assert_eq!(counts, (1, 2, 1));

    let conn = db.connect().unwrap();

    // Counts come from the aggregate upsert over the partition, not a
    // correlated per-tag probe.
    let mut rows = conn
        .query("SELECT id, count FROM Tags ORDER BY id;", ())
        .await
        .unwrap();
    let mut tag_counts = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        tag_counts.push((row.get::<u64>(0).unwrap(), row.get::<u64>(1).unwrap()));
    }
    assert!(
        tag_counts.contains(&(1, 1)),
        "mammal count must be recomputed from the partition: {tag_counts:?}"
    );
    assert!(
        tag_counts.contains(&(2, 1)),
        "canine count must be recomputed from the partition: {tag_counts:?}"
    );

    // The pre-existing sentinel-limit parent and the slurped NULL-limit
    // one dedupe back to a single row stored as the -1 sentinel,
    // leaving the composite unique index rebuildable.
    let mut rows = conn
        .query("SELECT tag_id, relate_tag_id, limit_to FROM Parents;", ())
        .await
        .unwrap();
    let mut parents = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        parents.push((
            row.get::<u64>(0).unwrap(),
            row.get::<u64>(1).unwrap(),
            row.get::<Option<i64>>(2).unwrap(),
        ));
    }
    assert_eq!(parents, vec![(1, 2, Some(-1))]);

    // Every index dropped for the bulk load must be back in place. Turso
    // canonicalizes identifiers to lowercase on disk, so the partition
    // index is queried in its lowercase form. The lean Parents schema
    // carries two indexes (covering relate_tag_id + composite unique); the
    // legacy idx_parents_lim is intentionally never recreated.
    let mut rows = conn
        .query(
            "SELECT LOWER(name) FROM sqlite_schema
             WHERE type = 'index' AND LOWER(name) IN (?1, ?2, ?3);",
            (
                "idx_relationship_1_tag_file",
                "idx_parents_rel",
                "idx_unique_parents",
            ),
        )
        .await
        .unwrap();
    let mut present = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        present.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(
        present.len(),
        3,
        "all bulk-import indexes recreated: {present:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_recovers_interrupted_parents_rebuild() {
    let db = new_target().await;

    // Warm destination with one pre-existing edge, left in the crash
    // state of an interrupted slurp rebuild: Parents was renamed aside to
    // Parents_old and the fresh table never got created, so Parents is
    // missing entirely. Parents_old here keeps a pre-v10 nullable
    // limit_to to also cover the restore's shape-agnostic index work.
    {
        let conn = db.connect().unwrap();
        conn.execute_batch(
            "INSERT INTO Namespace (id, name, description) VALUES (1, 'species', 'pre');
             INSERT INTO Tags (id, name, namespace) VALUES (1, 'mammal', 1), (2, 'canine', 1);
             DROP TABLE Parents;
             CREATE TABLE Parents_old (
                 id INTEGER PRIMARY KEY,
                 tag_id INTEGER NOT NULL,
                 relate_tag_id INTEGER NOT NULL,
                 limit_to INTEGER);
             INSERT INTO Parents_old (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
        )
        .await
        .unwrap();
    }

    // The source carries the same pre-existing edge (as a pre-v10 NULL)
    // plus a novel one, so recovery must fold both into the fresh table.
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    write_source(
        &source_path,
        "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES ('species', 'test');

         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES ('mammal', 1), ('canine', 1);

         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes)
             VALUES ('slurp-hash', 'jpg', 1, 42);

         CREATE TABLE Relationship_1 (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (tag_id, file_id));
         INSERT INTO Relationship_1 (file_id, tag_id) VALUES (1, 1), (1, 2);

         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
         INSERT INTO Parents (tag_id, relate_tag_id, limit_to)
             VALUES (1, 2, NULL), (2, 1, NULL);",
    )
    .await;

    let counts = db.db_slurp(&source_path).await.unwrap();
    assert_eq!(counts, (1, 2, 1));

    let conn = db.connect().unwrap();

    // The restored edge dedupes with the slurped copy; the novel edge
    // lands. Both unscoped, so both store as the -1 sentinel.
    let mut rows = conn
        .query(
            "SELECT tag_id, relate_tag_id, limit_to FROM Parents ORDER BY tag_id, relate_tag_id;",
            (),
        )
        .await
        .unwrap();
    let mut parents = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        parents.push((
            row.get::<u64>(0).unwrap(),
            row.get::<u64>(1).unwrap(),
            row.get::<Option<i64>>(2).unwrap(),
        ));
    }
    assert_eq!(
        parents,
        vec![(1, 2, Some(-1)), (2, 1, Some(-1))],
        "recovered Parents_old rows plus slurped rows must dedupe into fresh Parents"
    );

    // The scratch table is gone and the fresh Parents schema (sentinel
    // unique + covering reverse) is what remains: the recovery-created
    // shape-agnostic IFNULL unique index must not survive the rebuild.
    for (name, want) in [
        ("parents_old", 0i64),
        ("idx_unique_parents_null_safe", 0),
        ("idx_unique_parents", 1),
        ("idx_parents_rel", 1),
    ] {
        let present: i64 = conn
            .query(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema
                 WHERE LOWER(name) = LOWER(?1));",
                (name,),
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(present, want, "{name} after interrupted-slurp recovery");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_builds_popular_only_fts_shadow() {
    let db = new_target().await;

    // Source tags: 'popular' related to five files (count 5 — must be
    // FTS-searchable) and 'rare' related to one (count 1 — must not).
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    write_source(
        &source_path,
        "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES ('species', 'test');

         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES ('popular', 1), ('rare', 1);

         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
             ('h1', 'jpg', 1, 1), ('h2', 'jpg', 1, 2), ('h3', 'jpg', 1, 3),
             ('h4', 'jpg', 1, 4), ('h5', 'jpg', 1, 5);

         CREATE TABLE Relationship_1 (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (tag_id, file_id));
         INSERT INTO Relationship_1 (file_id, tag_id) VALUES
             (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (1, 2);

         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);",
    )
    .await;

    let counts = db.db_slurp(&source_path).await.unwrap();
    assert_eq!(counts, (1, 2, 5));

    let conn = db.connect().unwrap();

    let mut rows = conn
        .query("SELECT name, count FROM Tags ORDER BY id;", ())
        .await
        .unwrap();
    let mut tag_counts = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        tag_counts.push((row.get::<String>(0).unwrap(), row.get::<u64>(1).unwrap()));
    }
    assert_eq!(
        tag_counts,
        vec![("popular".to_string(), 5), ("rare".to_string(), 1)]
    );

    // Only the popular tag is mirrored into the search shadow.
    let mut rows = conn
        .query("SELECT name FROM Tags_Popular ORDER BY tag_id;", ())
        .await
        .unwrap();
    let mut shadow = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        shadow.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(shadow, vec!["popular".to_string()]);

    // And only it is searchable (ngram typo/prefix tolerance intact).
    let popular = db.tags_search_fts("pop", 10).await.unwrap();
    assert!(!popular.is_empty(), "popular tag must be searchable");
    let rare = db.tags_search_fts("rar", 10).await.unwrap();
    assert!(rare.is_empty(), "rare tag (count 1) must not be searchable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_streams_legacy_relationship_table_once() {
    let db = new_target().await;

    // A single shared `Relationship` table holding every namespace, like
    // the production source. Rows must be read once and routed by
    // namespace; references to tags/files missing from the source must be
    // dropped rather than copied, and each target partition still gets its
    // count recompute and file_id index rebuild afterwards.
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    write_source(
        &source_path,
        "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES
             ('species', 'test'), ('artist', 'test');

         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES
             ('mammal', 1), ('canine', 1), ('painter', 2);

         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
             ('slurp-hash', 'jpg', 1, 42), ('legacy-hash', 'png', 1, 43);

         CREATE TABLE Relationship (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         INSERT INTO Relationship (file_id, tag_id) VALUES
             (1, 1), (1, 2), (2, 3),
             (2, 99),  -- tag id missing from Tags -> dropped
             (99, 1);  -- file id missing from File -> dropped

         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
         INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
    )
    .await;

    let counts = db.db_slurp(&source_path).await.unwrap();
    assert_eq!(counts, (2, 3, 2));

    let conn = db.connect().unwrap();
    let species_id = db.namespace_get_name_cache("species").await.unwrap();
    let artist_id = db.namespace_get_name_cache("artist").await.unwrap();

    // Counts recomputed per namespace from the aggregate pass.
    let mut rows = conn
        .query("SELECT name, count FROM Tags ORDER BY name;", ())
        .await
        .unwrap();
    let mut tag_counts = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        tag_counts.push((row.get::<String>(0).unwrap(), row.get::<u64>(1).unwrap()));
    }
    assert_eq!(
        tag_counts,
        vec![
            ("canine".to_string(), 1),
            ("mammal".to_string(), 1),
            ("painter".to_string(), 1),
        ]
    );

    // The scanned rows landed in the partition matching their namespace,
    // with the dangling references dropped before the bulk add.
    let mut rows = conn
        .query(
            format!(
                "SELECT f.hash, t.name
                 FROM Relationship_{species_id} r
                 JOIN File f ON f.id = r.file_id
                 JOIN Tags t ON t.id = r.tag_id
                 ORDER BY t.name;"
            ),
            (),
        )
        .await
        .unwrap();
    let mut species_rows = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        species_rows.push((row.get::<String>(0).unwrap(), row.get::<String>(1).unwrap()));
    }
    assert_eq!(
        species_rows,
        vec![
            ("slurp-hash".to_string(), "canine".to_string()),
            ("slurp-hash".to_string(), "mammal".to_string()),
        ]
    );

    let mut rows = conn
        .query(
            format!(
                "SELECT f.hash, t.name
                 FROM Relationship_{artist_id} r
                 JOIN File f ON f.id = r.file_id
                 JOIN Tags t ON t.id = r.tag_id;"
            ),
            (),
        )
        .await
        .unwrap();
    let mut artist_rows = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        artist_rows.push((row.get::<String>(0).unwrap(), row.get::<String>(1).unwrap()));
    }
    assert_eq!(
        artist_rows,
        vec![("legacy-hash".to_string(), "painter".to_string())]
    );

    // Every partition that received routed rows gets its index rebuilt.
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'index' AND LOWER(name) IN (?1, ?2);",
            (
                format!("idx_relationship_{species_id}_tag_file"),
                format!("idx_relationship_{artist_id}_tag_file"),
            ),
        )
        .await
        .unwrap();
    let rebuilt = rows.next().await.unwrap().unwrap();
    assert_eq!(rebuilt.get::<u64>(0).unwrap(), 2);

    // The parent survived its mapped copy.
    let mut rows = conn
        .query("SELECT COUNT(*) FROM Parents;", ())
        .await
        .unwrap();
    let parent_rows = rows.next().await.unwrap().unwrap();
    assert_eq!(parent_rows.get::<u64>(0).unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_tags_fresh_fast_path_then_upsert_on_rerun() {
    let db = new_target().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    write_source(
        &source_path,
        "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES
             ('species', 'test'), ('artist', 'test');

         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES
             ('mammal', 1), ('canine', 1), ('painter', 2);

         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
             ('slurp-hash', 'jpg', 1, 42), ('legacy-hash', 'png', 1, 43);

         CREATE TABLE Relationship (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         INSERT INTO Relationship (file_id, tag_id) VALUES (1, 1), (1, 2), (2, 3);

         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);",
    )
    .await;

    // Fresh destination: the tags stage swaps Tags for the
    // constraint-free copy (fast path) and restores the named unique
    // index afterwards.
    let counts = db.db_slurp(&source_path).await.unwrap();
    assert_eq!(counts, (2, 3, 2));

    let conn = db.connect().unwrap();
    let mut tag_rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
    assert_eq!(
        tag_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<u64>(0)
            .unwrap(),
        3
    );
    let mut index_rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'index' AND name IN
                 ('idx_tags_name_namespace', 'idx_tags_count_covering');",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        index_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<u64>(0)
            .unwrap(),
        2
    );

    // Re-running against the (now warm) destination must keep tag ids
    // stable: rows the destination already owns resolve to their existing
    // ids instead of being inserted again, so no duplicate tags and no
    // duplicate relationships at the (already imported) mapping.
    let counts = db.db_slurp(&source_path).await.unwrap();
    assert_eq!(counts, (2, 3, 2));
    let conn = db.connect().unwrap();
    let mut tag_rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
    assert_eq!(
        tag_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<u64>(0)
            .unwrap(),
        3
    );

    // The swap left the canonical table and no temporary leftovers, and
    // the previous run's relationship rows deduplicate against this run's.
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'table' AND name IN ('Tags_slurp', 'Tags_old');",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        0
    );
    let species_id = db.namespace_get_name_cache("species").await.unwrap();
    let artist_id = db.namespace_get_name_cache("artist").await.unwrap();
    let mut species_rows = conn
        .query(
            format!("SELECT COUNT(*) FROM Relationship_{species_id};",),
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        species_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<u64>(0)
            .unwrap(),
        2
    );
    let mut artist_rows = conn
        .query(
            format!("SELECT COUNT(*) FROM Relationship_{artist_id};"),
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        artist_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<u64>(0)
            .unwrap(),
        1
    );
}

/// Cold import from a per-namespace-partition source (no legacy
/// `Relationship` table): the tags stage must insert straight into the
/// indexed `Tags` table, the covering index must be restored by the common
/// post-recount path (the legacy-only branch would miss a partition-only
/// source), and the router must reconstruct relation rows and recount.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_cold_partition_only_recounts_and_restores_covering() {
    let db = new_target().await;

    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    write_source(
        &source_path,
        "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES ('species', 'test');

         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES ('mammal', 1), ('canine', 1);

         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');

         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes)
             VALUES ('slurp-hash', 'jpg', 1, 42);

         CREATE TABLE Relationship_1 (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (tag_id, file_id));
         INSERT INTO Relationship_1 (file_id, tag_id) VALUES (1, 1), (1, 2);

         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
         INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);",
    )
    .await;

    let counts = db.db_slurp(&source_path).await.unwrap();
    assert_eq!(counts, (1, 2, 1));

    let conn = db.connect().unwrap();

    // The cold path keeps the named unique index and restores the covering
    // index after recounts even though no legacy Relationship existed.
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'index' AND name IN
                 ('idx_tags_name_namespace', 'idx_tags_count_covering');",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        2
    );

    // Counts recomputed per namespace from the partition aggregate pass.
    let mut rows = conn
        .query("SELECT name, count FROM Tags ORDER BY name;", ())
        .await
        .unwrap();
    let mut tag_counts = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        tag_counts.push((row.get::<String>(0).unwrap(), row.get::<u64>(1).unwrap()));
    }
    assert_eq!(
        tag_counts,
        vec![("canine".to_string(), 1), ("mammal".to_string(), 1)]
    );

    // The partition got its rows and its file_id index.
    let species_id = db.namespace_get_name_cache("species").await.unwrap();
    let mut rows = conn
        .query(
            format!("SELECT COUNT(*) FROM Relationship_{species_id};",),
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        2
    );
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'index' AND LOWER(name) IN (?1);",
            (format!("idx_relationship_{species_id}_tag_file"),),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        1
    );

    // The parent landed once.
    let mut rows = conn
        .query("SELECT COUNT(*) FROM Parents;", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        1
    );

    // Journal mode was restored to the pre-import serving mode (whatever
    // the boot pragma in new_with_exit chose, not a hardcoded journal).
    let mut mode = String::new();
    conn.pragma_query("journal_mode", |row| {
        mode = row.get::<String>(0).unwrap_or_default();
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(mode, TursoDatabase::serving_journal_mode());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_jobs_skips_incomplete_jobs_table() {
    let db = new_target().await;
    let (_source_db, source, _keep) = new_source(
        "CREATE TABLE Jobs (
             id INTEGER PRIMARY KEY, time INTEGER NOT NULL,
             site TEXT NOT NULL, param TEXT NOT NULL);",
    )
    .await;

    let conn = db.connect().unwrap();
    db.slurp_jobs(&conn, &source).await.unwrap();

    let jobs = target_jobs(&db).await;
    assert!(jobs.is_empty(), "missing columns must not abort the slurp");
}

/// Offline throughput smoke for the tag swap + swap-back: imports a few
/// hundred thousand rows into `/tmp`, re-imports over the (now warm)
/// destination, and checks the merged set stayed canonical. Ignored by
/// default; run with `--ignored --nocapture` to see stage timings.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn slurp_swap_throughput_smoke() {
    let _ = fast_log::init(
        fast_log::Config::new()
            .level(log::LevelFilter::Info)
            .console(),
    );
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    let db = new_target().await;
    {
        let source_db = turso::Builder::new_local(&source_path.to_string_lossy())
            .experimental_without_rowid(true)
            .build()
            .await
            .unwrap();
        let mut source = source_db.connect().unwrap();
        source
            .execute_batch(
                "PRAGMA synchronous = OFF;
                 CREATE TABLE Namespace (
                     id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
                 INSERT INTO Namespace (name, description) VALUES ('species', 'test');
                 CREATE TABLE Tags (
                     id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                     count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
                 CREATE TABLE FileStorageLocations (
                     id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
                 INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
                 CREATE TABLE File (
                     id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                     storage_id INTEGER, size_bytes INTEGER);
                 CREATE TABLE Relationship (
                     file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                     PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
                 CREATE TABLE Parents (
                     id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                     relate_tag_id INTEGER NOT NULL, limit_to INTEGER);",
            )
            .await
            .unwrap();
        let tags = 20_000_u32;
        let files = 6_000_u32;
        let rels = 20_000_u32;
        {
            let tx = source.transaction().await.unwrap();
            for chunk in (1..=tags).collect::<Vec<_>>().chunks(10_000) {
                let values = chunk
                    .iter()
                    .map(|i| format!("({i}, 'tag_{i}', 1)"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = tx
                    .execute(
                        &format!("INSERT INTO Tags (id, name, namespace) VALUES {values};"),
                        (),
                    )
                    .await
                    .unwrap();
            }
            for chunk in (1..=files).collect::<Vec<_>>().chunks(10_000) {
                let values = chunk
                    .iter()
                    .map(|i| format!("({i}, 'f{i:09x}', 'jpg', 1, 10)"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = tx
                    .execute(
                        &format!(
                            "INSERT INTO File (id, hash, extension, storage_id, size_bytes)
                             VALUES {values};"
                        ),
                        (),
                    )
                    .await
                    .unwrap();
            }
            for chunk in (1..=rels).collect::<Vec<_>>().chunks(10_000) {
                let values = chunk
                    .iter()
                    .map(|i| format!("({}, {})", (i % files) + 1, (i % tags) + 1))
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = tx
                    .execute(
                        &format!("INSERT INTO Relationship (file_id, tag_id) VALUES {values};"),
                        (),
                    )
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
        }
        source
            .pragma_update("journal_mode", "'delete'")
            .await
            .unwrap();
        drop(source);
        drop(source_db);
    }

    eprintln!("source built: 20k tags, 6k files, 20k rels");
    let t0 = std::time::Instant::now();
    let counts = db.db_slurp(&source_path).await.unwrap();
    let first = t0.elapsed();
    assert_eq!(counts, (1, 20_000, 6_000));
    eprintln!("first import done: {first:?}");

    let t0 = std::time::Instant::now();
    let counts = db.db_slurp(&source_path).await.unwrap();
    let second = t0.elapsed();
    assert_eq!(counts, (1, 20_000, 6_000));
    eprintln!("re-import done: {second:?}");

    let conn = db.connect().unwrap();
    let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        20_000
    );
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'table' AND name IN ('Tags_slurp', 'Tags_old');",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        0
    );

    println!("first import (20k tags / 6k files / 20k rels): {first:?}, re-import: {second:?}");
}

/// Write a standalone slurp source with `count` tags named `{prefix}_{i}`
/// at source ids 1..=count plus the tables a slurp needs.
async fn write_tag_source(source_path: &std::path::Path, prefix: &str, count: u32) {
    let values = (1..=count)
        .map(|i| format!("({i}, '{prefix}_{i}', 1)"))
        .collect::<Vec<_>>()
        .join(", ");
    let script = format!(
        "CREATE TABLE Namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT);
         INSERT INTO Namespace (id, name) VALUES (1, 'ns');
         CREATE TABLE Tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                            count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
         CREATE TABLE File (id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                            storage_id INTEGER, size_bytes INTEGER);
         CREATE TABLE Relationship (file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                                    PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         CREATE TABLE Parents (id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                               relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
         INSERT INTO Tags (id, name, namespace) VALUES {values};"
    );
    write_source(source_path, &script).await;
}

/// A warm import that adds genuinely new tags must not collide with the
/// destination's existing ids: the staging table hands new rows ids past
/// the canonical table's max, so the swap-back row copy
/// (`INSERT INTO Tags_slurp ... SELECT ... FROM Tags`) cannot hit
/// `UNIQUE constraint failed: tags_slurp.id`. Regression test for the
/// production error at src/db/turso/system_jobs.rs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_warm_import_new_tags_do_not_collide_with_existing_ids() {
    let db = new_target().await;
    let temp_dir = tempfile::tempdir().unwrap();

    // Cold import fills the destination with a_* tags at ids 1..=3000.
    let source_a = temp_dir.path().join("source_a.db");
    write_tag_source(&source_a, "a", 3_000).await;
    assert_eq!(
        db.db_slurp(&source_a).await.unwrap(),
        (1, 3_000, 0),
        "cold import"
    );

    // Warm import of overlapping source ids 1..=200 with different names;
    // these are all genuinely new so the swap-back path runs.
    let source_b = temp_dir.path().join("source_b.db");
    write_tag_source(&source_b, "b", 200).await;
    assert_eq!(
        db.db_slurp(&source_b).await.unwrap(),
        (1, 200, 0),
        "warm import must succeed without a tags_slurp.id collision"
    );

    let conn = db.connect().unwrap();
    let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        3_200,
        "all old and new tags survive"
    );
    // Every tag id must exist exactly once: the new rows got fresh ids
    // past the old max and the old rows were copied back unchanged.
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM (
                 SELECT id, COUNT(*) AS seen FROM Tags GROUP BY id HAVING seen <> 1
             );",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        0,
        "no duplicate or missing tag ids"
    );
    let mut rows = conn
        .query("SELECT COUNT(*) FROM Tags WHERE name LIKE 'a_%';", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        3_000
    );
    let mut rows = conn
        .query("SELECT COUNT(*) FROM Tags WHERE name LIKE 'b_%';", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        200
    );
}

/// A source whose schema contains a virtual table the limbo parser cannot
/// load (an FTS5 table whose stored SQL quotes the tokenizer arg with
/// double quotes, as some hydrus databases ship) must still slurp: the
/// source is backed up with the sqlite3 CLI, the virtual tables and
/// triggers are dropped from the copy, and the copy is imported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_source_with_double_quoted_fts_falls_back_to_sanitized_copy() {
    let db = new_target().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    let script = "CREATE TABLE Namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT);
         INSERT INTO Namespace (id, name) VALUES (1, 'ns');
         CREATE TABLE Tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                            count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (id, name, namespace) VALUES (1, 'cat', 1), (2, 'dog', 1);
         CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
         CREATE TABLE File (id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                            storage_id INTEGER, size_bytes INTEGER);
         CREATE TABLE Relationship (file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                                    PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         CREATE TABLE Parents (id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                               relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
         CREATE VIRTUAL TABLE Tags_Popular_fts USING fts5(
             name,
             tokenize = \"unicode61 separators '_/'\"
         );";
    let status = std::process::Command::new("sqlite3")
        .arg(&source_path)
        .arg(script)
        .status()
        .unwrap();
    assert!(
        status.success(),
        "sqlite3 must be installed to run this test"
    );

    // Precondition: turso cannot load that stored SQL directly.
    assert!(
        turso::Builder::new_local(&source_path.to_string_lossy())
            .read_only(true)
            .experimental_without_rowid(true)
            .build()
            .await
            .is_err(),
        "direct open must fail on the double-quoted FTS SQL"
    );

    // The full import routes through the sanitized copy.
    assert_eq!(
        db.db_slurp(&source_path).await.unwrap(),
        (1, 2, 0),
        "slurp must succeed through the sanitized copy"
    );
    let conn = db.connect().unwrap();
    let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        2
    );

    // No temp copies may be left behind.
    let leftover = std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("intscrape_slurp_")
        })
        .count();
    assert_eq!(leftover, 0, "sanitized copies must be cleaned up");
}

/// Seeds the destination with the exact state a crashed slurp leaves
/// behind: the progress table (keyed to `source_path`'s fingerprint) and
/// the pending-recount markers of namespaces whose rows landed before the
/// crash. The next `db_slurp` must resume from this state.
async fn seed_interrupted_slurp_state(
    db: &TursoDatabase,
    source_path: &std::path::Path,
    legacy_checkpoint: SlurpStageCheckpoint,
    pending_namespaces: &[u64],
) {
    let conn = db.connect().unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {SLURP_CHECKPOINT_TABLE} (
                 stage TEXT PRIMARY KEY,
                 finished INTEGER NOT NULL DEFAULT 0,
                 last_file_id INTEGER NOT NULL DEFAULT 0,
                 last_tag_id INTEGER NOT NULL DEFAULT 0,
                 source_key TEXT NOT NULL DEFAULT '');"
        ),
        (),
    )
    .await
    .unwrap();
    conn.execute("DELETE FROM _slurp_checkpoint", ())
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO _slurp_checkpoint
             (stage, finished, last_file_id, last_tag_id, source_key)
         VALUES ('meta', 0, 0, 0, ?1)",
        (slurp_source_key(source_path).as_str(),),
    )
    .await
    .unwrap();
    slurp_checkpoint_write(&conn, "relationship_legacy", legacy_checkpoint)
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _slurp_pending_recount (namespace INTEGER PRIMARY KEY);",
        (),
    )
    .await
    .unwrap();
    conn.execute("DELETE FROM _slurp_pending_recount", ())
        .await
        .unwrap();
    for namespace in pending_namespaces {
        conn.execute(
            "INSERT OR IGNORE INTO _slurp_pending_recount (namespace) VALUES (?1)",
            (*namespace as i64,),
        )
        .await
        .unwrap();
    }
}

/// After a slurp fully completes, a restarted run whose checkpoint says the
/// staging work already finished must skip it, land on the identical final
/// state, and clean its progress tables up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_resume_skips_finished_stages_and_cleans_up() {
    let db = new_target().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    let script = "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES ('species', 'test');
         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES ('mammal', 1), ('canine', 1);
         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes)
             VALUES ('slurp-hash', 'jpg', 1, 42);
         CREATE TABLE Relationship (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         INSERT INTO Relationship (file_id, tag_id) VALUES (1, 1), (1, 2);
         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);
         INSERT INTO Parents (tag_id, relate_tag_id, limit_to) VALUES (1, 2, NULL);";
    write_source(&source_path, script).await;

    assert_eq!(db.db_slurp(&source_path).await.unwrap(), (1, 2, 1));
    // The successful run must have cleaned its progress tables.
    let conn = db.connect().unwrap();
    slurp_assert_no_progress_tables(&db).await;

    // Simulate a crash after the legacy relationship pass, but before the
    // tables were dropped at the end of the stage.
    let species_id = db.namespace_get_name_cache("species").await.unwrap();
    seed_interrupted_slurp_state(
        &db,
        &source_path,
        SlurpStageCheckpoint {
            finished: true,
            last_file_id: 0,
            last_tag_id: 0,
        },
        &[species_id],
    )
    .await;

    assert_eq!(
        db.db_slurp(&source_path).await.unwrap(),
        (1, 2, 1),
        "resumed run must import the same data"
    );

    // Identical final contents, and no leftover inter-run tables.
    slurp_assert_no_progress_tables(&db).await;
    let mut rows = conn
        .query("SELECT name, count FROM Tags ORDER BY name;", ())
        .await
        .unwrap();
    let mut tag_counts = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        tag_counts.push((row.get::<String>(0).unwrap(), row.get::<u64>(1).unwrap()));
    }
    assert_eq!(
        tag_counts,
        vec![("canine".to_string(), 1), ("mammal".to_string(), 1)]
    );
}

/// A crash that cut the legacy relationship scan short leaves its cursor
/// behind; the resume must continue from that boundary, still recount any
/// namespace whose freshly landed rows never got their recount, and keep
/// the final state identical to an uninterrupted import.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_resume_mid_legacy_scan_recounts_pending_namespaces() {
    let db = new_target().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    let script = "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES
             ('species', 'test'), ('artist', 'test');
         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES
             ('mammal', 1), ('canine', 1), ('painter', 2);
         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
             ('slurp-hash', 'jpg', 1, 42), ('legacy-hash', 'png', 1, 43);
         CREATE TABLE Relationship (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         INSERT INTO Relationship (file_id, tag_id) VALUES
             (1, 1), (1, 2), (2, 3);
         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);";
    write_source(&source_path, script).await;

    assert_eq!(db.db_slurp(&source_path).await.unwrap(), (2, 3, 2));
    let conn = db.connect().unwrap();

    // Simulate a crash part-way through the shared relationship scan: only
    // the (1, 1) row's batch had committed, and neither namespace's recount
    // had run yet, so both carry their pending markers.
    let species_id = db.namespace_get_name_cache("species").await.unwrap();
    let artist_id = db.namespace_get_name_cache("artist").await.unwrap();
    seed_interrupted_slurp_state(
        &db,
        &source_path,
        SlurpStageCheckpoint {
            finished: false,
            last_file_id: 1,
            last_tag_id: 0,
        },
        &[species_id, artist_id],
    )
    .await;

    assert_eq!(
        db.db_slurp(&source_path).await.unwrap(),
        (2, 3, 2),
        "resumed run must import the same data"
    );

    // The tail of the scan was re-read and deduplicated, and the pending
    // recounts fired: counts are authoritative again.
    let mut rows = conn
        .query("SELECT name, count FROM Tags ORDER BY name;", ())
        .await
        .unwrap();
    let mut tag_counts = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        tag_counts.push((row.get::<String>(0).unwrap(), row.get::<u64>(1).unwrap()));
    }
    assert_eq!(
        tag_counts,
        vec![
            ("canine".to_string(), 1),
            ("mammal".to_string(), 1),
            ("painter".to_string(), 1),
        ],
        "pending recounts must run on resume"
    );
    slurp_assert_no_progress_tables(&db).await;
}

/// Cursors are scoped to the exact source file: a changed source must
/// discard the stale progress instead of blindly skipping source rows a
/// different database never imported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_resume_discards_stale_progress_when_source_changes() {
    let db = new_target().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let source_a = temp_dir.path().join("source_a.db");
    let source_b = temp_dir.path().join("source_b.db");
    let script = "CREATE TABLE Namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT);
         INSERT INTO Namespace (id, name) VALUES (1, 'ns');
         CREATE TABLE Tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
                            count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (id, name, namespace) VALUES {tags};
         CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
         CREATE TABLE File (id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
                            storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (id, hash, extension, storage_id, size_bytes) VALUES (1, 'h1', 'jpg', 1, 10);
         CREATE TABLE Relationship (file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                                    PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         INSERT INTO Relationship (file_id, tag_id) VALUES (1, 1);
         CREATE TABLE Parents (id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
                               relate_tag_id INTEGER NOT NULL, limit_to INTEGER);";
    write_source(
        &source_a,
        &script.replace("{tags}", "(1, 'one', 1), (2, 'two', 1)"),
    )
    .await;
    write_source(
        &source_b,
        &script.replace("{tags}", "(1, 'three', 1), (2, 'four', 1)"),
    )
    .await;

    assert_eq!(db.db_slurp(&source_a).await.unwrap(), (1, 2, 1));

    // Simulate an interrupted run that recorded "relationship pass fully
    // done" against source_a, then the operator pointed the slurp at a
    // different source. The wait check is that source_b's relationships
    // must still land: a stale `finished` cursor applied to another
    // database would skip them entirely.
    seed_interrupted_slurp_state(
        &db,
        &source_a,
        SlurpStageCheckpoint {
            finished: true,
            last_file_id: 0,
            last_tag_id: 0,
        },
        &[],
    )
    .await;

    assert_eq!(db.db_slurp(&source_b).await.unwrap(), (1, 2, 1));
    let conn = db.connect().unwrap();
    let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        4,
        "source_b's tags must all be imported, not skipped by stale cursors"
    );
    // The relationship stage was not skipped by source_a's finished
    // marker: source_b's relationship row landed in the namespace
    // partition.
    let ns_id = db.namespace_get_name_cache("ns").await.unwrap();
    let mut rows = conn
        .query(
            format!(
                "SELECT COUNT(*) FROM Relationship_{ns_id} r
                 JOIN Tags t ON t.id = r.tag_id
                 WHERE t.name IN ('one', 'two', 'three', 'four');"
            ),
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        2,
        "both runs' relationships must land: source_a's row plus source_b's,
         not source_a's alone (which is what a stale finished cursor would leave)"
    );
}

/// The tags stage resumes from its persisted cursor: committed batches are
/// skipped whole (their source->target mapping comes from _slurp_tag_map),
/// the tail still lands, and the relationship stage routes through the
/// preloaded map — a relationship row pointing at a skipped tag must not
/// be silently dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_resume_skips_committed_tag_batches() {
    let db = new_target().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    let script = "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES ('ns', 'test');
         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES ('cat', 1), ('dog', 1);
         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
             ('h1', 'jpg', 1, 10);
         CREATE TABLE Relationship (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         INSERT INTO Relationship (file_id, tag_id) VALUES (1, 1), (1, 2);
         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);";
    write_source(&source_path, script).await;

    // Full run first: everything lands and the progress tables are cleaned.
    assert_eq!(db.db_slurp(&source_path).await.unwrap(), (1, 2, 1));
    slurp_assert_no_progress_tables(&db).await;
    {
        let conn = db.connect().unwrap();
        let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            2
        );
    }

    // Simulate a crash after source tag 1's batch committed: cursor plus
    // mapping row survive, the staging table does not (the crash happened
    // before any warm staging existed). The resume must skip the covered
    // batch, land the tail, and keep every relationship.
    seed_interrupted_slurp_state(
        &db,
        &source_path,
        SlurpStageCheckpoint {
            finished: false,
            last_file_id: 0,
            last_tag_id: 0,
        },
        &[],
    )
    .await;
    {
        let conn = db.connect().unwrap();
        conn.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {SLURP_TAG_MAP_TABLE} (
                     source_id INTEGER PRIMARY KEY,
                     target_id INTEGER NOT NULL);"
            ),
            (),
        )
        .await
        .unwrap();
        conn.execute(
            &format!("INSERT INTO {SLURP_TAG_MAP_TABLE} (source_id, target_id) VALUES (1, 1)"),
            (),
        )
        .await
        .unwrap();
        slurp_checkpoint_write(
            &conn,
            "tags",
            SlurpStageCheckpoint {
                finished: false,
                last_file_id: 0,
                last_tag_id: 1,
            },
        )
        .await
        .unwrap();
    }

    // The resumed attempt counts only what it processed: the covered
    // batch is skipped, so its tag never re-enters the count.
    assert_eq!(db.db_slurp(&source_path).await.unwrap(), (1, 1, 1));
    slurp_assert_no_progress_tables(&db).await;
    {
        let conn = db.connect().unwrap();
        let mut rows = conn.query("SELECT COUNT(*) FROM Tags;", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            2,
            "resumed run must neither duplicate nor drop tags"
        );
        let mut rows = conn
            .query("SELECT COUNT(*) FROM Relationship_1;", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
            2,
            "relationships pointing at skipped tags must route through the preloaded map"
        );
    }
}

/// The secondary-hash stage resumes from its persisted cursor: committed
/// batches are never re-read, and a re-run lands the exact same hash set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_resume_reimports_hash_tail() {
    let db = new_target().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let source_path = temp_dir.path().join("source.db");
    let script = "CREATE TABLE Namespace (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT);
         INSERT INTO Namespace (name, description) VALUES ('ns', 'test');
         CREATE TABLE Tags (
             id INTEGER PRIMARY KEY, name TEXT NOT NULL, namespace INTEGER NOT NULL,
             count INTEGER NOT NULL DEFAULT 0, UNIQUE(name, namespace));
         INSERT INTO Tags (name, namespace) VALUES ('cat', 1), ('dog', 1);
         CREATE TABLE FileStorageLocations (
             id INTEGER PRIMARY KEY, location TEXT NOT NULL UNIQUE);
         INSERT INTO FileStorageLocations (location) VALUES ('/tmp');
         CREATE TABLE File (
             id INTEGER PRIMARY KEY, hash TEXT UNIQUE, extension TEXT,
             storage_id INTEGER, size_bytes INTEGER);
         INSERT INTO File (hash, extension, storage_id, size_bytes) VALUES
             ('h1', 'jpg', 1, 10), ('h2', 'png', 1, 20);
         CREATE TABLE FileHashes (
             file_id INTEGER NOT NULL, algorithm TEXT NOT NULL, digest TEXT NOT NULL,
             PRIMARY KEY (file_id, algorithm));
         INSERT INTO FileHashes VALUES
             (1, 'MD5', 'a'), (1, 'SHA1', 'b'), (2, 'MD5', 'c'), (2, 'SHA1', 'd');
         CREATE TABLE Relationship (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;
         INSERT INTO Relationship (file_id, tag_id) VALUES (1, 1), (2, 2);
         CREATE TABLE Parents (
             id INTEGER PRIMARY KEY AUTOINCREMENT, tag_id INTEGER NOT NULL,
             relate_tag_id INTEGER NOT NULL, limit_to INTEGER);";
    write_source(&source_path, script).await;

    assert_eq!(db.db_slurp(&source_path).await.unwrap(), (1, 2, 2));
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM FileHashes;", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        4
    );

    // Simulate a crash after file 1's hashes committed: the resume must
    // jump past file 1's batch and still import file 2's tail.
    seed_interrupted_slurp_state(
        &db,
        &source_path,
        SlurpStageCheckpoint {
            finished: false,
            last_file_id: 0,
            last_tag_id: 0,
        },
        &[],
    )
    .await;
    let conn = db.connect().unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {SLURP_CHECKPOINT_TABLE}
                 (stage, finished, last_file_id, last_tag_id)
             VALUES ('hashes', 0, 1, 0)"
        ),
        (),
    )
    .await
    .unwrap();
    // A stale row-keyed state would drop file 1's hashes; the resume must
    // not. Verify the cursor row exists and feeds the resume.
    let mut rows = conn
        .query(
            &format!(
                "SELECT last_file_id FROM {SLURP_CHECKPOINT_TABLE} WHERE stage = 'hashes'"
            ),
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        1
    );

    assert_eq!(db.db_slurp(&source_path).await.unwrap(), (1, 2, 2));
    let mut rows = conn
        .query("SELECT COUNT(*) FROM FileHashes;", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        4,
        "resumed hash stage must keep every row"
    );
}

async fn slurp_assert_no_progress_tables(db: &TursoDatabase) {
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'table' AND name IN ('_slurp_checkpoint', '_slurp_pending_recount', '_slurp_tag_map', '_slurp_file_map');",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap(),
        0,
        "progress tables must be dropped once the slurp completes"
    );
}

/// The source-index diagnostic must tell a keyset-serving index apart from
/// a table that forces a full re-scan per batch: composite PKs and explicit
/// indexes leading with (file_id, tag_id) qualify; a (tag_id, file_id) PK
/// does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slurp_diagnoses_missing_keyset_index() {
    let (_, conn, _temp) = new_source(
        "CREATE TABLE Relationship_x (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (tag_id, file_id));
         INSERT INTO Relationship_x (file_id, tag_id) VALUES (1, 2);",
    )
    .await;
    assert_eq!(
        slurp_source_keyset_indexed(&conn, "Relationship_x")
            .await
            .unwrap(),
        false,
        "a (tag_id, file_id) PK does not serve the (file_id, tag_id) keyset"
    );
    conn.execute(
        "CREATE INDEX idx_x_file_tag ON Relationship_x (file_id, tag_id)",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        slurp_source_keyset_indexed(&conn, "Relationship_x")
            .await
            .unwrap(),
        true,
        "an explicit (file_id, tag_id) index must be detected"
    );

    let (_, conn, _temp) = new_source(
        "CREATE TABLE Relationship_y (
             file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
             PRIMARY KEY (file_id, tag_id)) WITHOUT ROWID;",
    )
    .await;
    assert_eq!(
        slurp_source_keyset_indexed(&conn, "Relationship_y")
            .await
            .unwrap(),
        true,
        "a clustered (file_id, tag_id) PK must be detected"
    );
}
