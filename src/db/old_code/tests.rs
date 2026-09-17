use super::*;
use crate::DB_VERSION;
use crate::cli::cli_structs::CheckFilesEnum;
use crate::web::manager::hash_bytes;
use bytes::Bytes;
use rayon::ThreadPoolBuilder;
use rusqlite::params;
use shared_types::*;
use std::collections::HashSet;
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
    let system_version = db
        .internal_setting_get(&conn, "SYSTEM_VERSION")
        .unwrap()
        .expect("SYSTEM_VERSION setting should be configured");

    assert_eq!(system_version.num, Some(DB_VERSION));

    let user_agent = db
        .internal_setting_get(&conn, "SYSTEM_DEFAULT_USER_AGENT")
        .unwrap()
        .expect("Default user agent missing");

    assert_eq!(user_agent.param, Some("IntScrape V1.0".to_string()));
}

#[test]
fn test_v2_database_upgrade_runs_migrations() {
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
    db.internal_db_version_set(&conn, 2).unwrap();
    drop(conn);
    drop(db);

    let db = database_for_path(&path);
    let conn = db.pool.get().unwrap();
    let version = db
        .internal_setting_get(&conn, "SYSTEM_VERSION")
        .unwrap()
        .unwrap();
    assert_eq!(version.num, Some(DB_VERSION));
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
    db.internal_tag_bulk_add(&conn, &[tag1, tag2], db.plugin_manager.clone());

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

    let tag_map = db.internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
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
    let ids = db.internal_namespace_bulk_add(&conn, &set);
    assert_eq!(ids.len(), 2);
    assert!(ids.contains_key(&ns1));
    assert!(ids.contains_key(&ns2));
    assert_eq!(db.namespace_cache.read().get(&ns1.name), ids.get(&ns1));
    assert_eq!(db.namespace_cache.read().get(&ns2.name), ids.get(&ns2));

    // 2. Test Upsert (ON CONFLICT update description)
    let ns1_updated = GenericNamespaceObj {
        name: "authors".to_string(),
        description: Some("Updated Description".to_string()),
    };

    let mut update_set = HashSet::new();
    update_set.insert(ns1_updated.clone());

    let updated_ids = db.internal_namespace_bulk_add(&conn, &update_set);
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
    let tag_ids = db.internal_tag_bulk_add(&conn, &[complex_plugin_tag], db.plugin_manager.clone());

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
    let parent_results = db.internal_parents_bulk_add(&conn, &parent_input_set);

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
    db.internal_tag_bulk_add(&conn, &[complex_tag], db.plugin_manager.clone());

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

    assert!(
        db.internal_namespace_bulk_add(&conn, &HashSet::new())
            .is_empty()
    );
    assert_eq!(db.internal_namespace_get_id(&conn, "missing"), None);

    let original = namespace("edge", Some("first"));
    let id = db.internal_namespace_get_or_create(&conn, &original);
    assert_eq!(db.namespace_cache.read().get("edge"), Some(&id));
    assert_eq!(db.internal_namespace_get_id(&conn, "edge"), Some(id));
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
async fn test_source_url_files_get_marks_dead_urls() {
    let db = new_test();
    let conn = db.pool.get().unwrap();
    let url = "https://example.test/dead".to_string();
    db.internal_dead_url_add(&conn, &url).unwrap();
    drop(conn);

    let statuses = db.source_url_files_get(HashSet::from([url.clone()])).await;
    let status = statuses.get(&url).expect("dead URL should be returned");
    assert!(status.dead);
    assert!(status.file.is_none());
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
    let tags = db.internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
    let tag_id = tags[&tag(url, "source_url")];
    let file_id = db
        .internal_file_bulk_add(&conn, HashSet::from([file("source-url-hash", "jpg")]))
        .into_iter()
        .next()
        .and_then(|file| file.id)
        .unwrap();
    db.internal_relationships_bulk_add(&conn, &HashSet::from([(file_id, tag_id)]));
    drop(conn);

    let existing = db
        .source_url_files_get(HashSet::from([url.to_string()]))
        .await;
    let existing_file = existing.get(url).expect("source URL file should exist");
    let existing_file = existing_file.file.as_ref().expect("file should exist");
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
    db.internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
    drop(conn);

    assert_eq!(
        db.source_url_files_get(HashSet::from([url.to_string()]))
            .await,
        HashMap::new()
    );
}

#[test]
fn test_storage_check_is_recognized_as_system_job() {
    let job = DbJobsObj {
        id: 0,
        isrunning: false,
        config: PluginJob {
            site: crate::db::SYSTEM_STORAGE_CHECK_SITE.into(),
            ..Default::default()
        },
    };

    assert!(crate::db::system_jobs::is_system_job(&job));
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
    let result = db.internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());

    assert_eq!(result.len(), 1);
    assert!(result.contains_key(&tag("valid", "tags")));
    let tag_id = result[&tag("valid", "tags")];
    assert_eq!(db.tag_cache.write().get(tag_id), Some(tag("valid", "tags")));
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
    assert!(db.internal_file_bulk_add(&conn, HashSet::new()).is_empty());

    let first = file("abcdef123", "jpg");
    let mut files = HashSet::new();
    files.insert(first.clone());
    let inserted = db.internal_file_bulk_add(&conn, files);
    assert_eq!(inserted.len(), 1);
    let inserted = inserted.into_iter().next().unwrap();
    assert!(inserted.id.is_some());

    let updated = file("abcdef123", "png");
    let mut files = HashSet::new();
    files.insert(updated);
    let upserted = db
        .internal_file_bulk_add(&conn, files)
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(upserted.id, inserted.id);
    assert_eq!(upserted.extension, "png");
    assert_eq!(db.internal_file_get_all(&conn).unwrap().len(), 1);
}

#[test]
fn test_relationships_are_deduplicated_and_filtered() {
    let db = new_test();
    let conn = db.pool.get().unwrap();
    let file_id = db
        .internal_file_bulk_add(&conn, [file("relhash123", "jpg")].into())
        .into_iter()
        .next()
        .unwrap()
        .id
        .unwrap();
    let tag_ids = db.internal_tag_bulk_add(
        &conn,
        &[
            file_action(TagOperation::Add, vec![plugin_tag("one", "a")]),
            file_action(TagOperation::Add, vec![plugin_tag("two", "b")]),
        ],
        db.plugin_manager.clone(),
    );
    let one_id = tag_ids[&tag("one", "a")];
    let two_id = tag_ids[&tag("two", "b")];
    let relationships = HashSet::from([(file_id, one_id), (file_id, one_id), (file_id, two_id)]);
    db.internal_relationships_bulk_add(&conn, &relationships);

    assert_eq!(
        db.internal_file_id_get_tag_ids(&conn, &file_id).unwrap(),
        HashSet::from([one_id, two_id])
    );
    assert_eq!(
        db.internal_file_id_get_tag_ids_bulk(&conn, &[file_id, 999])
            .unwrap()
            .len(),
        1
    );
    assert!(db.internal_tag_has_files(&conn, one_id));
    assert!(!db.internal_tag_has_files(&conn, 999));
    let namespace_id = db.internal_namespace_get_id(&conn, "a").unwrap();
    assert_eq!(
        db.internal_file_id_get_tag_ids_where_namespace_id(&conn, &file_id, &namespace_id)
            .unwrap(),
        HashSet::from([one_id])
    );

    db.internal_relationship_bulk_delete(&conn, &HashSet::from([(file_id, one_id)]));
    assert_eq!(
        db.internal_file_id_get_tag_ids(&conn, &file_id).unwrap(),
        HashSet::from([two_id])
    );
}

#[test]
fn test_tag_and_file_lookup_empty_and_missing_inputs() {
    let db = new_test();
    let conn = db.pool.get().unwrap();
    assert!(MainDatabase::internal_tag_id_get_tag(&conn, &HashSet::new()).is_empty());
    assert!(
        db.internal_file_ids_get_tags(&conn, &HashSet::new())
            .is_empty()
    );
    assert_eq!(
        MainDatabase::internal_file_id_get(&conn, &999),
        Err(rusqlite::Error::QueryReturnedNoRows)
    );
    assert_eq!(
        db.internal_tag_get_file_id(&conn, &tag("missing", "missing")),
        None
    );
}

#[test]
fn test_search_supports_and_or_not_and_limit() {
    let db = new_test();
    let conn = db.pool.get().unwrap();
    let mut ids = HashMap::new();
    for hash in ["searchaaa", "searchbbb", "searchccc"] {
        let inserted = db.internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]));
        let inserted = inserted.into_iter().next().unwrap();
        ids.insert(hash.to_string(), inserted.id.unwrap());
    }
    let tags = db.internal_tag_bulk_add(
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
    db.internal_relationships_bulk_add(
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
    assert_eq!(
        db.search_db_files_human_sync(&and, &None),
        vec![ids["searchaaa"]]
    );
    let or = SearchObj {
        search_relate: None,
        searches: vec![SearchHolder::Or(vec![blue, square])],
    };
    assert_eq!(
        db.search_db_files_human_sync(&or, &None)
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
    let not_results = db.search_db_files_human_sync(&not, &Some(1));
    assert_eq!(not_results, vec![ids["searchaaa"]]);
    let empty = SearchObj {
        search_relate: None,
        searches: vec![],
    };
    assert!(db.search_db_files_human_sync(&empty, &None).is_empty());
}

#[test]
fn test_search_malformed_inputs_do_not_panic() {
    let db = new_test();

    let not_only = SearchObj {
        search_relate: None,
        searches: vec![SearchHolder::Not(vec![u64::MAX])],
    };
    assert!(db.search_db_files_human_sync(&not_only, &None).is_empty());

    let empty_groups = SearchObj {
        search_relate: None,
        searches: vec![
            SearchHolder::And(Vec::new()),
            SearchHolder::Or(Vec::new()),
            SearchHolder::Not(Vec::new()),
        ],
    };
    assert!(
        db.search_db_files_human_sync(&empty_groups, &None)
            .is_empty()
    );

    let extreme_limit = SearchObj {
        search_relate: None,
        searches: vec![SearchHolder::And(vec![u64::MAX])],
    };
    assert!(
        db.search_db_files_human_sync(&extreme_limit, &Some(u64::MAX))
            .is_empty()
    );
}

#[test]
fn test_not_only_search_deduplicates_file_ids() {
    let db = new_test();
    let conn = db.pool.get().unwrap();
    let file_id = db
        .internal_file_bulk_add(&conn, HashSet::from([file("not-only", "jpg")]))
        .into_iter()
        .next()
        .unwrap()
        .id
        .unwrap();
    let tags = db.internal_tag_bulk_add(
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
    db.internal_relationships_bulk_add(
        &conn,
        &HashSet::from([(file_id, keep_one), (file_id, keep_two)]),
    );

    let search = SearchObj {
        search_relate: None,
        searches: vec![SearchHolder::Not(vec![excluded])],
    };
    assert_eq!(db.search_db_files_human_sync(&search, &None), vec![file_id]);
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
        let file_id = db
            .internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]))
            .into_iter()
            .next()
            .unwrap()
            .id
            .unwrap();
        file_ids.insert(hash, file_id);
    }

    let tags = db.internal_tag_bulk_add(
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

    db.internal_relationships_bulk_add(
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
        db.search_db_files_human_sync(
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
                db.internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]))
                    .into_iter()
                    .next()
                    .unwrap()
                    .id
                    .unwrap(),
            )
        })
        .collect::<HashMap<_, _>>();
    let tags = db.internal_tag_bulk_add(
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
    db.internal_relationships_bulk_add(
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
        let inserted = db.internal_file_bulk_add(&conn, HashSet::from([file(hash, "jpg")]));
        ids.insert(
            hash.to_string(),
            inserted.into_iter().next().unwrap().id.unwrap(),
        );
    }

    let tags = db.internal_tag_bulk_add(
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
    db.internal_relationships_bulk_add(
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
        db.search_db_files_human_sync(&and, &None),
        vec![ids["partialaaa"]]
    );

    let or = SearchObj {
        search_relate: None,
        searches: vec![SearchHolder::Or(vec![popular, uncached])],
    };
    assert_eq!(
        db.search_db_files_human_sync(&or, &None)
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([ids["partialaaa"], ids["partialbbb"]])
    );
}

#[test]
fn test_tag_search_resolves_typos_from_ram_and_sqlite() {
    let db = new_test();
    let conn = db.pool.get().unwrap();
    let tags = db.internal_tag_bulk_add(
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
        let inserted = db.internal_file_bulk_add(&conn, HashSet::from([item]));
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
    db.internal_relationships_bulk_add(&conn, &relationships);

    let popular = db.search_db_tags_fts("red fxo", &Some(1));
    assert_eq!(popular[0].tag_id, red_fox);

    let prefix = db.search_db_tags_fts("fema", &Some(1));
    assert_eq!(prefix[0].tag_id, female);

    let slow = db.search_db_tags_fts("raer creatur", &Some(1));
    assert_eq!(slow[0].tag_id, rare_creature);
}

#[test]
fn test_search_tag_fts_fast_path_uses_complete_clean_cache() {
    let db = new_test();
    let conn = db.pool.get().unwrap();
    let tags = db.internal_tag_bulk_add(
        &conn,
        &[file_action(
            TagOperation::Add,
            vec![plugin_tag("red fox", "subject")],
        )],
        db.plugin_manager.clone(),
    );
    let red_fox = tags[&tag("red fox", "subject")];

    // Rebuild the tag search cache so it holds every tag and is not dirty.
    // The fast path must answer typo queries from RAM alone.
    db.refresh_tag_search_cache();
    assert!(
        !db.tag_search_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
    );

    let result = db.search_db_tags_fts("red fxo", &Some(1));
    assert_eq!(result[0].tag_id, red_fox);
    let prefix = db.search_db_tags_fts("red f", &Some(5));
    assert!(prefix.iter().any(|hit| hit.tag_id == red_fox));
}

#[test]
fn test_search_tag_fts_finds_unpopular_tag_when_cache_is_incomplete() {
    let db = new_test();
    let conn = db.pool.get().unwrap();

    // > POPULAR_TAG_CACHE_LIMIT tags so the in-memory index is not "complete"
    // and the FTS path is the one responsible for long-tail matches.
    let mut tags = Vec::new();
    for index in 0..110_000 {
        tags.push(shared_types::PluginTag {
            tag: shared_types::Tag {
                name: format!("common-filler-{index}"),
                namespace: shared_types::GenericNamespaceObj {
                    name: "bulk".to_string(),
                    description: None,
                },
            },
            ..Default::default()
        });
    }
    for index in 0..500 {
        tags.push(shared_types::PluginTag {
            tag: shared_types::Tag {
                name: format!("rare-creature-{index}"),
                namespace: shared_types::GenericNamespaceObj {
                    name: "supers".to_string(),
                    description: None,
                },
            },
            ..Default::default()
        });
    }
    db.internal_tag_bulk_add(
        &conn,
        &[shared_types::FileTagAction {
            operation: TagOperation::Add,
            tags,
        }],
        db.plugin_manager.clone(),
    );
    db.refresh_tag_search_cache();
    assert!(
        !db.tag_search_cache.read().is_complete(),
        "cache should be incomplete above the popular limit"
    );

    // Give the rare tag zero relationships so it ranks at the bottom of the
    // cached popular set and only FTS can answer it.
    let hits = db.search_db_tags_fts("rare-creature-499", &Some(10));
    assert!(
        hits.iter().any(|hit| hit.count == 0),
        "expected the unpopular tag via FTS, got {hits:?}"
    );
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
    source.internal_relationship_partition_create(&source_conn, 1);
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
    db.internal_file_bulk_add(
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
    let tags = db.internal_tag_bulk_add(
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
        let inserted = db.internal_file_bulk_add(
            &conn,
            HashSet::from([file(&format!("same-name-{index}"), "jpg")]),
        );
        file_ids.push(inserted.into_iter().next().unwrap().id.unwrap());
    }
    db.internal_relationships_bulk_add(
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

    let target_storage_id = db
        .internal_file_storage_location_get_or_create(&conn, &target_location)
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
fn test_fix_internal_files_filename_mode_moves_by_name_without_hashing() {
    let db = new_test();
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source_location = source_dir.path().to_string_lossy().into_owned();
    let target_location = target_dir.path().to_string_lossy().into_owned();
    let file = file("abcdef123456", "bin");
    let bytes = Bytes::from_static(b"content does not match the filename hash");

    let conn = db.pool.get().unwrap();
    conn.execute("DELETE FROM FileStorageLocations", [])
        .unwrap();
    conn.execute(
        "UPDATE Settings SET param = ?1 WHERE name = 'SYSTEM_file_location'",
        params![&target_location],
    )
    .unwrap();
    MainDatabase::internal_file_storage_location_set(&conn, &source_location).unwrap();
    let target_storage_id = db
        .internal_file_storage_location_get_or_create(&conn, &target_location)
        .unwrap();
    conn.execute(
        "INSERT INTO File (hash, extension, storage_id) VALUES (?1, ?2, ?3)",
        params![&file.hash, &file.extension, target_storage_id],
    )
    .unwrap();
    drop(conn);

    let source_path = Path::new(&source_location)
        .join("misplaced")
        .join(file.hash.clone() + "." + &file.extension);
    std::fs::create_dir_all(source_path.parent().unwrap()).unwrap();
    std::fs::write(&source_path, &bytes).unwrap();

    // Keep the source storage alive in the database, but do not associate the
    // file record with it. The checker must identify the file by its filename.
    db.fix_internal_files(&CheckFilesEnum::StorageCheckFileName)
        .unwrap();

    let target_path = Path::new(&target_location)
        .join("ab")
        .join("cd")
        .join("ef")
        .join(file.hash.clone() + "." + &file.extension);
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

    let storage_id = db
        .internal_file_storage_location_get_or_create(&conn, &storage_location)
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
    let id = db.internal_jobs_add(&conn, &config);
    let duplicate_id = db.internal_jobs_add(&conn, &config);
    assert_eq!(id, duplicate_id);
    assert_eq!(
        MainDatabase::internal_jobs_get_all_sites(&conn).unwrap(),
        vec!["site"]
    );
    assert_eq!(
        db.internal_jobs_get_site(&conn, "missing").unwrap(),
        Vec::<DbJobsObj>::new()
    );

    db.internal_jobs_set_isrunning(&conn, id).unwrap();
    assert!(db.internal_jobs_get_site(&conn, "site").unwrap()[0].isrunning);
    MainDatabase::internal_jobs_reset_isrunning(&conn).unwrap();
    assert!(!db.internal_jobs_get_site(&conn, "site").unwrap()[0].isrunning);
    assert_eq!(
        db.internal_jobs_get_torun(&conn, vec!["site".into()])
            .unwrap()
            .len(),
        1
    );
    db.internal_job_remove(&conn, id).unwrap();
    assert!(db.internal_jobs_get_site(&conn, "site").unwrap().is_empty());
}

#[test]
fn test_jobs_get_torun_orders_priority_across_sites() {
    let db = new_test();
    let conn = db.pool.get().unwrap();

    let mut low = job("low", 0, 0);
    low.priority = 1;
    let mut high = job("high", 0, 0);
    high.priority = 100;
    let mut middle = job("middle", 0, 0);
    middle.priority = 50;

    db.internal_jobs_add(&conn, &low);
    db.internal_jobs_add(&conn, &high);
    db.internal_jobs_add(&conn, &middle);

    let jobs = db
        .internal_jobs_get_torun_chunk(&conn, vec!["low".into(), "high".into(), "middle".into()], 2)
        .unwrap();

    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].config.priority, 100);
    assert_eq!(jobs[1].config.priority, 50);
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
    let ids = db.internal_tag_bulk_add(&conn, &actions, db.plugin_manager.clone());
    let relation = TagParents {
        tag_id: ids[&child.tag],
        relate_tag_id: ids[&parent.tag],
        limit_to: Some(ids[&limit.tag]),
    };
    db.internal_parents_bulk_add(&conn, &HashSet::from([relation]));
    assert!(
        db.internal_parent_structure_exists(
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
        db.internal_parent_relate_limit_exists(&conn, &parent.tag, &limit.tag)
            .unwrap()
    );
    assert!(
        !db.internal_parent_structure_exists(
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
