//! Tests for the `ipc` module.
//!
//! Extracted from `ipc.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `ipc` module itself.

use super::*;
use shared_types::{PluginTag, TagOperation, TagType};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

async fn new_test_db() -> Arc<TursoDatabase> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    TursoDatabase::new_with_exit(&db_path, should_exit).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_tags_to_file_creates_namespace_partition_without_ddl_error() {
    let db = new_test_db().await;

    let conn = db.connect().unwrap();
    let storage_id = db
        .file_storage_location_get_or_create(&conn, "test_storage")
        .await
        .unwrap();
    drop(conn);

    let files = vec![FileInternal {
        id: None,
        hash: "ipc_hash_123".into(),
        extension: "png".into(),
        storage_id,
        size_bytes: Some(7),
    }];
    let conn = db.connect().unwrap();
    let resolved = db.file_add_bulk(&conn, &files).await.unwrap();
    drop(conn);
    let file_id = *resolved.iter().next().unwrap().id.as_ref().unwrap();

    let tag_action = FileTagAction {
        operation: TagOperation::Add,
        tags: vec![PluginTag {
            tag: Tag {
                name: "ipc_floof".into(),
                namespace: GenericNamespaceObj {
                    name: "ipc_fresh_ns".into(),
                    description: None,
                },
            },
            tag_type: TagType::NormalNoRegex,
            relates_to: None,
        }],
    };

    assert!(
        db.ipc_file_relationship_tags_add(&file_id, &[tag_action])
            .await
    );

    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT id FROM Namespace WHERE name = 'ipc_fresh_ns';", ())
        .await
        .unwrap();
    let Some(row) = rows.next().await.unwrap() else {
        panic!("namespace was never created");
    };
    let ns_id: u64 = row.get(0).unwrap();
    drop(rows);

    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = LOWER('Relationship') || '_' || ?1;",
            (ns_id as i64,),
        )
        .await
        .unwrap();
    let partition = rows.next().await.unwrap();
    assert!(
        partition.is_some(),
        "namespace partition table was never created"
    );
    drop(rows);

    let mut rows = conn
        .query(
            &format!("SELECT COUNT(*) FROM relationship_{ns_id} WHERE file_id = ?1;",),
            (file_id as i64,),
        )
        .await
        .unwrap();
    let Some(row) = rows.next().await.unwrap() else {
        panic!("expected at least one relationship row");
    };
    let relationship_count: i64 = row.get(0).unwrap();
    assert!(
        relationship_count >= 1,
        "expected the file/tag relationship to be persisted"
    );
}

/// End-to-end through the IPC import entry point: copying imports the
/// file into storage and records it in the DB (findable by MD5), and
/// moving removes the source once stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_file_ipc_copies_and_moves() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = TursoDatabase::new_with_exit(
        &temp_dir.path().join("test.db"),
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let storage_dir = temp_dir.path().join("storage");
    assert!(
        db.setting_set_sync(&shared_types::DbSettingsObj {
            name: "SYSTEM_file_location".into(),
            description: None,
            num: None,
            param: Some(storage_dir.to_string_lossy().into_owned()),
        })
        .await
    );

    let contents = b"ipc import test bytes";
    let source = temp_dir.path().join("copyme.png");
    std::fs::write(&source, contents).unwrap();
    assert_eq!(
        db.ipc_import_file(
            &source.to_string_lossy().into_owned(),
            &shared_types::FileAction::Copy,
        )
        .await,
        ""
    );
    assert!(source.exists(), "copy should keep the source");

    let md5 = format!("{:x}", md5::compute(contents.as_slice()));
    let found = db
        .hashes_files_get_sync(&[shared_types::HashesSupported::Md5(md5)])
        .await;
    let file = found
        .get(&("MD5".to_string(), format!("{:x}", md5::compute(contents.as_slice()))))
        .expect("imported file should resolve by md5");
    assert!(file.id.is_some());
    assert_eq!(file.extension, "png");

    let mover = temp_dir.path().join("moveme.bin");
    std::fs::write(&mover, b"move me").unwrap();
    assert_eq!(
        db.ipc_import_file(
            &mover.to_string_lossy().into_owned(),
            &shared_types::FileAction::Move,
        )
        .await,
        ""
    );
    assert!(!mover.exists(), "move should remove the source");

    let missing = db
        .ipc_import_file(&"/definitely/missing".to_string(), &shared_types::FileAction::Copy)
        .await;
    assert!(
        missing.contains("No such file"),
        "missing file should report an error, got: {missing}"
    );
}
