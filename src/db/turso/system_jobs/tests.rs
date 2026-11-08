//! Tests for the `system_jobs` module.
//!
//! Extracted from `system_jobs.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `system_jobs` module itself.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use shared_types::{GenericNamespaceObj, ScraperParam, Tag};

use crate::db::turso::TursoDatabase;
use crate::plugins::PluginManager;

use super::*;

async fn new_test_db() -> (Arc<TursoDatabase>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("system_jobs.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    let db = TursoDatabase::new_with_exit(&db_path, should_exit).await;
    (db, temp_dir)
}

/// A database with a plugin manager that has no sites registered, so
/// `site_for_url` always misses. Lets the walk be exercised end to end
/// without depending on a compiled plugin being present in the test
/// environment; the plugin-miss branch is what it lands on.
async fn new_test_db_with_empty_plugins() -> (Arc<TursoDatabase>, tempfile::TempDir) {
    let (db, temp_dir) = new_test_db().await;
    let empty_plugins = tempfile::tempdir().unwrap();
    let should_exit = Arc::new(AtomicBool::new(false));
    let manager = PluginManager::new(empty_plugins.path(), db.clone(), should_exit);
    db.set_plugin_manager(manager);
    (db, temp_dir)
}

fn test_file(storage_id: u64, hash: &str) -> FileInternal {
    FileInternal {
        id: None,
        hash: hash.to_string(),
        extension: "jpg".to_string(),
        storage_id,
        size_bytes: Some(1),
    }
}

/// Writes a file where `file_on_disk` expects to find it.
fn write_file_on_disk(root: &std::path::Path, file: &FileInternal) -> std::path::PathBuf {
    let mut path = root.to_path_buf();
    path.push(&file.hash[0..2]);
    path.push(&file.hash[2..4]);
    path.push(&file.hash[4..6]);
    path.push(&file.hash);
    let final_path = path.with_added_extension(&file.extension);
    std::fs::create_dir_all(final_path.parent().unwrap()).unwrap();
    std::fs::write(&final_path, b"payload").unwrap();
    final_path
}

fn storage_check_job(param: Option<&str>) -> DbJobsObj {
    DbJobsObj {
        id: 1,
        isrunning: false,
        config: PluginJob {
            time: 0,
            reptime: 0,
            priority: 1,
            recreation: None,
            site: SYSTEM_STORAGE_CHECK_SITE.to_string(),
            param: param
                .map(|mode| vec![ScraperParam::Normal(mode.to_string())])
                .unwrap_or_default(),
            user_data: std::collections::BTreeMap::new(),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_get_page_walks_every_row_once_in_id_order() {
    let (db, _temp_dir) = new_test_db().await;
    let conn = db.connect().unwrap();
    let storage_id = db
        .file_storage_location_get_or_create(&conn, "page_storage")
        .await
        .unwrap();

    let files: Vec<FileInternal> = (0..25)
        .map(|i| test_file(storage_id, &format!("pagehash{i:02}")))
        .collect();
    let inserted = db.file_add_bulk(&conn, &files).await.unwrap();
    let mut all_ids: Vec<u64> = inserted.iter().filter_map(|f| f.id).collect();
    all_ids.sort_unstable();
    assert_eq!(all_ids.len(), 25);

    // Paging must yield every row exactly once, in ascending id order, and
    // a start key in the middle of the table must skip only what is below it.
    let mut seen: Vec<u64> = Vec::new();
    let mut after = 0u64;
    loop {
        let page = db.file_get_page(&conn, after, 7).await.unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 7, "a page must respect its limit");
        // A full row set: the pager selects every column `file_on_disk` needs.
        for file in &page {
            assert!(file.id.is_some());
            assert!(!file.hash.is_empty());
            assert_eq!(file.extension, "jpg");
        }
        after = page.last().unwrap().id.unwrap();
        seen.extend(page.into_iter().filter_map(|f| f.id));
    }
    assert_eq!(seen, all_ids, "paging must cover the table in id order");

    let mid = all_ids[12];
    let tail = db.file_get_page(&conn, mid, 100).await.unwrap();
    assert_eq!(
        tail.iter().filter_map(|f| f.id).collect::<Vec<u64>>(),
        all_ids[13..].to_vec(),
        "a mid-table start key must resume after that id"
    );
    assert!(
        db.file_get_page(&conn, 0, 0).await.unwrap().is_empty(),
        "a zero limit must return nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_present_in_storage_checks_every_root() {
    let roots_dir = tempfile::tempdir().unwrap();
    let primary = roots_dir.path().join("primary");
    let secondary = roots_dir.path().join("secondary");
    std::fs::create_dir_all(&primary).unwrap();
    std::fs::create_dir_all(&secondary).unwrap();

    let mut roots = HashMap::new();
    roots.insert(1u64, primary.to_string_lossy().to_string());
    roots.insert(2u64, secondary.to_string_lossy().to_string());

    let in_primary = test_file(1, "inprimary1");
    write_file_on_disk(&primary, &in_primary);
    assert!(file_present_in_storage(&in_primary, &roots));

    // Recorded against root 2 but actually living in root 1: a library moved
    // between roots must still count as present.
    let moved = test_file(2, "movedroot01");
    write_file_on_disk(&primary, &moved);
    assert!(
        file_present_in_storage(&moved, &roots),
        "a file must be found under any configured root, not only its own"
    );

    let absent = test_file(1, "absentfil1");
    assert!(
        !file_present_in_storage(&absent, &roots),
        "an unwritten file must count as missing"
    );

    // Too short to partition, so it can never resolve to a path.
    let too_short = test_file(1, "abc");
    assert!(!file_present_in_storage(&too_short, &roots));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redownload_job_carries_the_download_manager_contract() {
    let file = FileInternal {
        id: Some(42),
        hash: "contracthash".to_string(),
        extension: "png".to_string(),
        storage_id: 7,
        size_bytes: None,
    };
    let job = redownload_job(
        &file,
        "https://example.test/a".to_string(),
        "site".to_string(),
    );

    assert_eq!(job.site, "site");
    assert_eq!(job.priority, DEFAULT_PRIORITY);
    assert_eq!(job.reptime, 0);
    assert!(job.recreation.is_none());
    assert!(
        matches!(&job.param[0], ScraperParam::Url(url) if url.url == "https://example.test/a"
            && url.local_modifiers.is_empty()),
        "the recovery job must carry the URL for the plugin to parse"
    );

    // These keys are read by name in web/manager; a rename silently turns a
    // recovery job into an ordinary scrape.
    let user_data = &job.user_data;
    assert_eq!(
        user_data.get("INTSCRAPE_REDOWNLOAD").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        user_data
            .get("INTSCRAPE_REDOWNLOAD_FILE_ID")
            .map(String::as_str),
        Some("42")
    );
    assert_eq!(
        user_data
            .get("INTSCRAPE_REDOWNLOAD_HASH")
            .map(String::as_str),
        Some("contracthash")
    );
    assert_eq!(
        user_data
            .get("INTSCRAPE_REDOWNLOAD_STORAGE_ID")
            .map(String::as_str),
        Some("7")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redownload_walk_detects_missing_files_and_resolves_source_urls() {
    let (db, _temp_dir) = new_test_db_with_empty_plugins().await;
    let storage_dir = tempfile::tempdir().unwrap();
    let conn = db.connect().unwrap();
    let storage_id = db
        .file_storage_location_get_or_create(&conn, &storage_dir.path().to_string_lossy())
        .await
        .unwrap();

    // Three recorded files: one on disk, one absent, one whose only tag is
    // not a source_url.
    let present = test_file(storage_id, "presentfil");
    let absent = test_file(storage_id, "absentfil");
    let untagged = test_file(storage_id, "untaggedfil");
    write_file_on_disk(storage_dir.path(), &present);

    let inserted = db
        .file_add_bulk(&conn, &[present.clone(), absent.clone(), untagged.clone()])
        .await
        .unwrap();
    let mut by_hash: HashMap<String, u64> = HashMap::new();
    for file in &inserted {
        by_hash.insert(file.hash.clone(), file.id.unwrap());
    }

    // source_url tag on two of them, a different tag on the third.
    let source_url_tag = Tag {
        name: "https://example.test/missing".to_string(),
        namespace: GenericNamespaceObj {
            name: SOURCE_URL_NAMESPACE.to_string(),
            description: None,
        },
    };
    let other_tag = Tag {
        name: "some-artist".to_string(),
        namespace: GenericNamespaceObj {
            name: "artist".to_string(),
            description: None,
        },
    };
    let tag_set = db
        .tag_add_bulk(&conn, &HashSet::from([source_url_tag, other_tag]))
        .await
        .unwrap();
    let source_url_tag_id = tag_set
        .iter()
        .find(|t| t.namespace_id > 0 && t.name == "https://example.test/missing")
        .map(|t| t.id as u64)
        .unwrap();
    let other_tag_id = tag_set
        .iter()
        .find(|t| t.name == "some-artist")
        .map(|t| t.id as u64)
        .unwrap();

    let rels: HashSet<(u64, u64)> = HashSet::from([
        (by_hash[&absent.hash], source_url_tag_id),
        (by_hash[&present.hash], source_url_tag_id),
        (by_hash[&untagged.hash], other_tag_id),
    ]);
    let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
    db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();
    drop(conn);

    let summary = db.redownload_missing_files().await.unwrap();

    assert_eq!(
        summary.missing_files, 2,
        "both the absent file and the untagged one are off disk; the present one is not"
    );
    assert_eq!(
        summary.no_source_url, 1,
        "the untagged missing file has no source_url tag to recover from"
    );
    assert_eq!(
        summary.no_plugin, 1,
        "the other missing file has a source_url, but no site is registered in this test"
    );
    assert_eq!(
        summary.queued_jobs, 0,
        "a plugin miss must not queue a job with an unusable site"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_check_job_dispatch_implements_all_modes() {
    let (db, _temp_dir) = new_test_db_with_empty_plugins().await;

    // A fresh DB has no storage locations, so every mode succeeds
    // trivially (nothing to check / nothing missing).
    assert!(
        db.run_storage_check_job(&storage_check_job(None)).await,
        "the default StorageCheck cleanup mode is implemented"
    );
    assert!(
        db.run_storage_check_job(&storage_check_job(Some(SYSTEM_STORAGE_CHECK_FILENAME_MODE)))
            .await,
        "the filename cleanup mode is implemented"
    );
    assert!(
        db.run_storage_check_job(&storage_check_job(Some(
            SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE
        )))
        .await,
        "the redownload mode is implemented and must report success"
    );
    // An unknown mode must report failure rather than silently pass.
    assert!(
        !db.run_storage_check_job(&storage_check_job(Some("bogus"))).await,
        "an unknown mode must not report success"
    );
}

/// Registers a storage root in the DB and returns its id. Also makes it
/// the default storage location, so the dumpster lands next to it
/// (`<root>/../dumpster`) exactly like in production.
async fn register_storage(db: &TursoDatabase, root: &std::path::Path) -> u64 {
    let conn = db.connect().unwrap();
    assert!(
        db.setting_set_sync(&shared_types::DbSettingsObj {
            name: "SYSTEM_file_location".into(),
            description: None,
            num: None,
            param: Some(root.to_string_lossy().into_owned()),
        })
        .await
    );
    db.file_storage_location_get_or_create(&conn, &root.to_string_lossy())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_check_dumpsters_unknown_files() {
    let (db, temp_dir) = new_test_db().await;
    let root = temp_dir.path().join("storage");
    std::fs::create_dir_all(&root).unwrap();
    register_storage(&db, &root).await;

    // One orphan at the root, one nested (relative paths are preserved).
    std::fs::write(root.join("orphan.png"), b"orphan").unwrap();
    std::fs::create_dir_all(root.join("sub")).unwrap();
    std::fs::write(root.join("sub").join("deep.bin"), b"deep").unwrap();

    let summary = db
        .storage_check_files(false)
        .await
        .expect("storage check should succeed");

    let dumpster = temp_dir.path().join("dumpster");
    assert_eq!(summary.dumpstered, 2, "{summary:?}");
    assert_eq!(
        std::fs::read(dumpster.join("orphan.png")).unwrap(),
        b"orphan"
    );
    assert_eq!(
        std::fs::read(dumpster.join("sub").join("deep.bin")).unwrap(),
        b"deep"
    );
    assert!(!root.join("orphan.png").exists());
    assert!(!root.join("sub").join("deep.bin").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_check_moves_misplaced_known_file_into_place() {
    let (db, temp_dir) = new_test_db().await;
    let root = temp_dir.path().join("storage");
    std::fs::create_dir_all(&root).unwrap();
    let storage_id = register_storage(&db, &root).await;

    // A known file (128-hex SHA-512 name, uppercase like stored files)
    // sitting at the root instead of its partitioned place.
    let hash = "AB".repeat(64);
    let conn = db.connect().unwrap();
    db.file_add_bulk(&conn, &[test_file(storage_id, &hash)])
        .await
        .unwrap();
    let misplaced = root.join(format!("{hash}.jpg"));
    std::fs::write(&misplaced, b"payload").unwrap();

    let summary = db
        .storage_check_files(false)
        .await
        .expect("storage check should succeed");

    let expected = root
        .join("AB")
        .join("AB")
        .join("AB")
        .join(format!("{hash}.jpg"));
    assert_eq!(summary.moved_to_correct_place, 1, "{summary:?}");
    assert_eq!(std::fs::read(&expected).unwrap(), b"payload");
    assert!(!misplaced.exists());
    assert!(
        !temp_dir.path().join("dumpster").exists(),
        "a known file must never be dumped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_check_leaves_correctly_placed_file_alone() {
    let (db, temp_dir) = new_test_db().await;
    let root = temp_dir.path().join("storage");
    std::fs::create_dir_all(&root).unwrap();
    let storage_id = register_storage(&db, &root).await;

    let hash = "CD".repeat(64);
    let conn = db.connect().unwrap();
    let file = test_file(storage_id, &hash);
    db.file_add_bulk(&conn, &[file.clone()]).await.unwrap();
    let placed = write_file_on_disk(&root, &file);

    let summary = db
        .storage_check_files(false)
        .await
        .expect("storage check should succeed");

    assert_eq!(summary.checked, 1, "{summary:?}");
    assert_eq!(summary.already_correct, 1, "{summary:?}");
    assert_eq!(summary.dumpstered, 0, "{summary:?}");
    assert!(placed.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_check_filename_mode_trusts_names_only() {
    let (db, temp_dir) = new_test_db().await;
    let root = temp_dir.path().join("storage");
    std::fs::create_dir_all(&root).unwrap();
    register_storage(&db, &root).await;

    // A SHA-512-shaped name that is NOT in the DB, with content whose
    // real hash is different. Filename mode must dumpster it without
    // hashing (the name is all it trusts).
    let fake_hash = "EF".repeat(64);
    let named = root.join(format!("{fake_hash}.png"));
    std::fs::write(&named, b"content hashing to something else").unwrap();

    let summary = db
        .storage_check_files(true)
        .await
        .expect("storage check should succeed");

    assert_eq!(summary.hashed, 0, "filename mode must never hash");
    assert_eq!(summary.dumpstered, 1, "{summary:?}");
    let dumped = temp_dir.path().join("dumpster").join(format!("{fake_hash}.png"));
    assert_eq!(
        std::fs::read(dumped).unwrap(),
        b"content hashing to something else"
    );
    assert!(!named.exists());
}
