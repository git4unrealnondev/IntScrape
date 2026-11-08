//! Tests for the `tag` module.
//!
//! Extracted from `tag.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `tag` module itself.

use super::*;
use crate::db::turso::TursoDatabase;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

async fn new_test_db() -> Arc<TursoDatabase> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    TursoDatabase::new_with_exit(&db_path, should_exit).await
}

/// The batched skip-if resolver must behave exactly like the sequential
/// `tag_get_file_id` + `file_id_get` pair it replaced:
///
///  * a tag with a relationship to an existing file resolves to that file,
///  * a tag that exists but owns no relationship resolves to nothing,
///  * a tag that does not exist at all resolves to nothing,
///  * a tag spread over several files picks the first file by ascending
///    id, the historical `LIMIT 1` choice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_tag_resolution_matches_only_files_that_exist() {
    let db = new_test_db().await;
    let conn = db.connect().unwrap();

    let skipns = GenericNamespaceObj {
        name: "skipns".into(),
        description: None,
    };
    let otherns = GenericNamespaceObj {
        name: "otherns".into(),
        description: None,
    };
    db.namespace_ensure_set(&HashSet::from([skipns.clone(), otherns.clone()]))
        .await
        .unwrap();

    let storage_id = db
        .file_storage_location_get_or_create(&conn, "test_storage")
        .await
        .unwrap();
    let file_objs: Vec<FileInternal> = (0..2)
        .map(|i| FileInternal {
            id: None,
            hash: format!("skiphash{i:05}"),
            extension: "jpg".into(),
            storage_id,
            size_bytes: Some(1),
        })
        .collect();
    let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
    let (first_file, second_file) = {
        let ids: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();
        (
            *std::cmp::min(&ids[0], &ids[1]),
            *std::cmp::max(&ids[0], &ids[1]),
        )
    };

    // "skipme" exists in skipns and is attached to both files, so its
    // resolution pins the ascending-file-id rule; "lonely" exists in
    // skipns with no relationship; "ghost" lives only in otherns's
    // namespace partition; "otherns_tag" is a real relationship in a
    // different namespace and must resolve through that arm.
    let skipme = Tag {
        name: "skipme".into(),
        namespace: skipns.clone(),
    };
    let lonely = Tag {
        name: "lonely".into(),
        namespace: skipns.clone(),
    };
    let other_tag = Tag {
        name: "otherns_tag".into(),
        namespace: otherns.clone(),
    };
    let tag_ids = db
        .tag_add_bulk(
            &conn,
            &HashSet::from([skipme.clone(), lonely.clone(), other_tag.clone()]),
        )
        .await
        .unwrap();
    let skipme_id = tag_ids.iter().find(|t| t.name == "skipme").unwrap().id as u64;
    let other_id = tag_ids.iter().find(|t| t.name == "otherns_tag").unwrap().id as u64;

    let mut rels: HashSet<(u64, u64)> = HashSet::new();
    rels.insert((first_file, skipme_id));
    rels.insert((second_file, skipme_id));
    rels.insert((first_file, other_id));
    let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
    db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();

    let keys = HashSet::from([
        ("skipns".to_string(), "skipme".to_string()),
        ("skipns".to_string(), "lonely".to_string()),
        ("ghost".to_string(), "never_added".to_string()),
        ("otherns".to_string(), "otherns_tag".to_string()),
    ]);
    let resolved = db.tag_skip_files_get(&keys).await;

    assert_eq!(resolved.len(), 2, "only relationship-bearing tags resolve");
    let skipme_file = resolved
        .get(&("skipns".to_string(), "skipme".to_string()))
        .expect("skipme must resolve");
    assert_eq!(
        skipme_file.id,
        Some(first_file),
        "a tag on several files must pick the first by ascending id"
    );
    let other_file = resolved
        .get(&("otherns".to_string(), "otherns_tag".to_string()))
        .expect("otherns_tag must resolve through its own namespace arm");
    assert_eq!(other_file.id, Some(first_file));
    assert!(
        !resolved.contains_key(&("skipns".to_string(), "lonely".to_string())),
        "a tag with no relationship must be absent, not an error or a skip"
    );
}
