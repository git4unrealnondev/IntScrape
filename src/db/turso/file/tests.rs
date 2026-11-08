//! Tests for the `file` module.
//!
//! Extracted from `file.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `file` module itself.

use super::*;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

async fn new_test_db() -> Arc<TursoDatabase> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    TursoDatabase::new_with_exit(&db_path, should_exit).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_storage_location_get_or_create_is_cached() {
    let db = new_test_db().await;
    let conn = db.connect().unwrap();

    let first = db
        .file_storage_location_get_or_create(&conn, "cache_me")
        .await
        .unwrap();
    let second = db
        .file_storage_location_get_or_create(&conn, "cache_me")
        .await
        .unwrap();
    assert_eq!(first, second, "repeated lookups must reuse the cached id");
    assert_eq!(
        db.file_storage_location_get_cache("cache_me").await,
        Some(first)
    );

    // Inserting a second row and reloading repopulates the cache from the
    // committed state without dropping the existing entry.
    db.file_storage_location_set(&conn, "cache_me_other")
        .await
        .unwrap();
    db.file_storage_location_cache_reload().await.unwrap();
    assert_eq!(
        db.file_storage_location_get_cache("cache_me").await,
        Some(first)
    );
    assert!(
        db.file_storage_location_get_cache("cache_me_other")
            .await
            .is_some()
    );
}

/// Keeps the temp dir alive for the whole test so the DB file persists.
async fn new_test_db_owned() -> (Arc<TursoDatabase>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
    (db, temp_dir)
}

fn test_file(hash: &str, size: Option<u64>) -> FileInternal {
    FileInternal {
        id: None,
        hash: hash.to_string(),
        extension: "jpg".to_string(),
        storage_id: 1,
        size_bytes: size,
    }
}

fn sorted_ids(files: &HashSet<FileInternal>) -> Vec<u64> {
    let mut ids: Vec<u64> = files.iter().flat_map(|file| file.id).collect();
    ids.sort_unstable();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_add_bulk_inserts_new_files_and_resolves_existing() {
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    let resolved = db
        .file_add_bulk(
            &conn,
            &[test_file("aaa", Some(100)), test_file("bbb", None)],
        )
        .await
        .unwrap();
    assert_eq!(resolved.len(), 2, "both files must resolve an id");
    let first_ids = sorted_ids(&resolved);
    assert_eq!(first_ids.len(), 2);
    assert_ne!(first_ids[0], first_ids[1], "new files get distinct ids");

    // Re-adding the same hashes resolves the same ids without inserting
    // any rows (no upsert, no duplicate entries).
    let re_resolved = db
        .file_add_bulk(
            &conn,
            &[test_file("aaa", Some(999)), test_file("bbb", Some(999))],
        )
        .await
        .unwrap();
    assert_eq!(
        sorted_ids(&re_resolved),
        first_ids,
        "existing hashes must resolve to the same ids"
    );

    let count: i64 = conn
        .query("SELECT COUNT(*) FROM File;", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(count, 2, "re-adding existing hashes must not write rows");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_add_bulk_does_not_refresh_existing_metadata() {
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    let original = db
        .file_add_bulk(&conn, &[test_file("aaa", Some(100))])
        .await
        .unwrap();
    let original = original.iter().next().unwrap();
    let id = original.id.unwrap();

    // A re-scrape with different metadata must leave the stored row
    // untouched: INSERT OR IGNORE skips the conflicting hash with no write
    // (the previous ON CONFLICT ... DO UPDATE rewrote these columns).
    let re_scraped = db
        .file_add_bulk(&conn, &[test_file("aaa", Some(999))])
        .await
        .unwrap();
    let re_scraped = re_scraped.iter().next().unwrap();
    assert_eq!(re_scraped.id.unwrap(), id, "same hash -> same id");

    let stored = db.file_get(&conn, &id).await.unwrap();
    assert_eq!(
        stored.size_bytes,
        Some(100),
        "existing rows must not be rewritten on re-scrape"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_add_bulk_dedupes_duplicate_hashes_in_one_chunk() {
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    // Two entries with the same hash in one chunk produce one id, one
    // stored row — matching the single-entry-per-hash RETURNING shape the
    // old upsert handed back.
    let resolved = db
        .file_add_bulk(
            &conn,
            &[test_file("aaa", Some(100)), test_file("aaa", Some(200))],
        )
        .await
        .unwrap();
    assert_eq!(resolved.len(), 1, "one output entry per hash");

    let count: i64 = conn
        .query("SELECT COUNT(*) FROM File;", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(count, 1, "duplicate hashes must insert one row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_update_matches_per_row_semantics() {
    // Pins the two things that made the batched CASE form viable: limbo
    // accepts a flat arm list at the full SQL_CHUNK_SIZE, and a chunked
    // statement updates exactly the rows the old per-row loop did. If a
    // future turso release reintroduces an expression-depth cap (the one
    // that killed the old OR-term delete in `relationship_bulk_delete`) or
    // changes NULL/bound-id handling, this fails loudly instead of silently
    // updating zero rows in the background size-backfill job.
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    let files: Vec<FileInternal> = (0..SQL_CHUNK_SIZE)
        .map(|i| test_file(&format!("h{i}"), None))
        .collect();
    let inserted = db.file_add_bulk(&conn, &files).await.unwrap();
    let mut ids: Vec<u64> = inserted.iter().filter_map(|f| f.id).collect();
    ids.sort_unstable();
    assert_eq!(ids.len(), SQL_CHUNK_SIZE);

    let updates: Vec<(u64, u64)> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (*id, (i + 1) as u64))
        .collect();
    db.file_sizes_update(&conn, &updates).await.unwrap();

    let mut filled: Vec<(u64, Option<u64>)> = Vec::with_capacity(ids.len());
    let mut rows = conn
        .query("SELECT id, size_bytes FROM File ORDER BY id;", ())
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        filled.push((row.get(0).unwrap(), row.get(1).unwrap()));
    }
    assert_eq!(filled.len(), SQL_CHUNK_SIZE, "every row must be updated");
    for ((expected_id, expected_size), (id, size)) in updates.iter().zip(&filled) {
        assert_eq!(id, expected_id, "ids must line up");
        assert_eq!(
            *size,
            Some(*expected_size),
            "a full chunk must update in one statement"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_sizes_update_leaves_existing_sizes_alone() {
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    let inserted = db
        .file_add_bulk(&conn, &[test_file("aaa", Some(100))])
        .await
        .unwrap();
    let id = inserted.iter().next().unwrap().id.unwrap();

    // The guard is `size_bytes IS NULL`, so a second pass over a file that
    // already has a size must not overwrite it.
    db.file_sizes_update(&conn, &[(id, 999)]).await.unwrap();
    assert_eq!(db.file_get(&conn, &id).await.unwrap().size_bytes, Some(100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_update_batch_writes_every_column() {
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    let inserted = db
        .file_add_bulk(&conn, &[test_file("aaa", None), test_file("bbb", None)])
        .await
        .unwrap();
    let mut ids: Vec<u64> = inserted.iter().filter_map(|f| f.id).collect();
    ids.sort_unstable();

    let mut updates = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let mut file = test_file(&format!("renamed{index}"), None);
        file.id = Some(*id);
        file.extension = format!("png{index}");
        file.storage_id = 7;
        updates.push(file);
    }
    db.file_update_batch(&conn, &updates).await.unwrap();

    for (index, id) in ids.iter().enumerate() {
        let stored = db.file_get(&conn, id).await.unwrap();
        assert_eq!(stored.hash, format!("renamed{index}"));
        assert_eq!(stored.extension, format!("png{index}"));
        assert_eq!(stored.storage_id, 7);
    }

    // A file with no id binds NULL and must match nothing.
    let mut unset = test_file("ignored", None);
    unset.id = None;
    db.file_update_batch(&conn, &[unset]).await.unwrap();
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM FileHashes WHERE digest = 'ignored';", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap(),
        0,
        "an unset id must not match a row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_hashes_get_all_sha512_returns_sorted_sha512_only() {
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    // `file_add_bulk` itself records the SHA-512 digest in FileHashes
    // (schema v13+), so the bulk list is exactly the digests the files
    // were added with — in digest order.
    let sha_b = "b".repeat(128);
    let sha_a = "a".repeat(128);
    let sha_c = "c".repeat(128);
    let files = db
        .file_add_bulk(
            &conn,
            &[
                test_file(&sha_b, None),
                test_file(&sha_a, None),
                test_file(&sha_c, None),
            ],
        )
        .await
        .unwrap();
    let ids = sorted_ids(&files);
    assert_eq!(ids.len(), 3);

    // Other algorithms must not leak into the SHA-512 list, and a duplicate
    // digest insert must not duplicate the entry (unique index).
    db.file_hashes_add_bulk(
        &conn,
        &[
            (ids[0], "MD5", "0123456789abcdef0123456789abcdef"),
            (ids[1], "SHA512", &sha_a),
        ],
    )
    .await
    .unwrap();

    let all = db.file_hashes_get_all_sha512(&conn).await.unwrap();
    assert_eq!(all, vec![sha_a, sha_b, sha_c]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_add_bulk_honors_pinned_id() {
    let (db, _temp_dir) = new_test_db_owned().await;
    let conn = db.connect().unwrap();

    let mut pinned = test_file("aaa", None);
    pinned.id = Some(777);

    let resolved = db.file_add_bulk(&conn, &[pinned]).await.unwrap();
    assert_eq!(resolved.iter().next().unwrap().id, Some(777));
    let stored = db.file_get(&conn, &777).await.unwrap();
    assert_eq!(stored.hash, "aaa");
}
