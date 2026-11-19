//! Tests for the `manager` module (extracted from `manager.rs`).

use super::*;

use shared_types::TagType;
use std::collections::HashMap;

/// Measures the three post-download suspects named during the redgifs
/// slow-crawl investigation, at the real per-file cost the run saw
/// (multi-MB media).
///
/// Ignored by default because it is a manual benchmark: run with
/// `cargo test --lib web::manager::tests::suspect_bench -- --ignored --nocapture`.
#[test]
#[ignore = "manual benchmark: per-file cost of the post-download suspects"]
fn suspect_bench() {
    use std::time::Instant;

    // A 12.7 MB file, the mean size the redgifs run downloaded.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.bin");
    std::fs::write(&path, vec![7u8; 12_700_000]).unwrap();

    // --- Suspect 2: whole-file re-read for on_download callbacks ---
    let t = Instant::now();
    let bytes = bytes::Bytes::from(std::fs::read(&path).unwrap());
    println!(
        "SUSPECT 2  std::fs::read of 12.7MB: {:>8.2?}  ({} MiB resident)",
        t.elapsed(),
        bytes.len() / 1048576
    );

    // --- Suspect 2b: is the buffering path even active? ---
    let _ = bytes;

    // --- Suspect 3: per-chunk shutdown-poll select! overhead ---
    // Mirrors manager.rs's streaming loop: a fresh 100ms sleep future is
    // constructed and polled for every network chunk.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for chunk_size in [8 * 1024usize, 64 * 1024] {
        let chunks = 12_700_000 / chunk_size;
        let t = Instant::now();
        rt.block_on(async {
            let should_exit = std::sync::atomic::AtomicBool::new(false);
            for _ in 0..chunks {
                let _fut = async {
                    while !should_exit.load(std::sync::atomic::Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                };
                // Same select! shape as the streaming loop.
                tokio::select! {
                    biased;
                    _ = std::future::ready(()) => {}
                    _ = _fut => {}
                }
            }
        });
        println!(
            "SUSPECT 3  select! with per-chunk sleep, {chunks} chunks @{chunk_size}B: {:>8.2?}",
            t.elapsed()
        );
    }
}

/// Suspect 1 needs a real database, so it is measured separately from the
/// pure in-process costs above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual benchmark: per-file blocking connection cost"]
async fn suspect_connection_bench() {
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let should_exit = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let db = crate::db::turso::TursoDatabase::new_with_exit(
        &dir.path().join("bench.db"),
        should_exit.clone(),
    )
    .await;
    let conn = db.connect().unwrap();
    let storage = dir.path().join("files");
    std::fs::create_dir_all(&storage).unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO FileStorageLocations (location) VALUES (?1);",
        (storage.to_string_lossy().as_ref(),),
    )
    .await
    .unwrap();

    const N: usize = 50;
    let hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    let t = Instant::now();
    for _ in 0..N {
        let _ = db.connect().unwrap();
    }
    println!(
        "SUSPECT 1a db.connect() alone:                {:>8.2?}/call",
        t.elapsed() / N as u32
    );

    let t = Instant::now();
    for _ in 0..N {
        let _ = db.file_download_location_get_sync_blocking(hash, "mp4");
    }
    println!(
        "SUSPECT 1b file_download_location_get_sync_blocking: {:>8.2?}/call",
        t.elapsed() / N as u32
    );

    // The async equivalent, for comparison: what the call costs without
    // the block_on + fresh-runtime handoff.
    let t = Instant::now();
    for _ in 0..N {
        let _ = db.file_download_location_get_sync(hash, "mp4").await;
    }
    println!(
        "  (same work, awaited instead of block_on):   {:>8.2?}/call",
        t.elapsed() / N as u32
    );
}

/// The download slot bounds transfers only, so it must be obtainable while
/// other work is in flight, and must be abandonable on shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_permit_bounds_transfers_and_abandons_on_shutdown() {
    let semaphore = Arc::new(PrioritySemaphore::new(1));
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // A free slot is granted immediately.
    let held = acquire_transfer_permit(&semaphore, &flag, 10).await;
    assert!(held.is_some(), "a free slot must be granted");

    // A saturated semaphore must block rather than exceed the bound.
    let waiter = tokio::time::timeout(
        Duration::from_millis(150),
        acquire_transfer_permit(&semaphore, &flag, 10),
    )
    .await;
    assert!(
        waiter.is_err(),
        "a second transfer must not start while the only slot is held"
    );

    // Releasing the permit lets the next transfer straight through. This is
    // the property that lets post-download work run outside the slot.
    drop(held);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            acquire_transfer_permit(&semaphore, &flag, 10)
        )
        .await
        .expect("slot must be reusable once released")
        .is_some()
    );

    // With the slot saturated and shutdown requested, the waiter gives up
    // instead of parking forever.
    let _busy = acquire_transfer_permit(&semaphore, &flag, 10).await;
    flag.store(true, Ordering::SeqCst);
    let abandoned = tokio::time::timeout(
        Duration::from_millis(500),
        acquire_transfer_permit(&semaphore, &flag, 10),
    )
    .await
    .expect("shutdown must not hang");
    assert!(abandoned.is_none(), "shutdown must abandon the slot wait");
}

/// The whole point of the priority semaphore: a high-priority job's
/// transfer must not sit behind a low-priority job's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_slots_are_granted_by_priority_then_fifo() {
    let semaphore = Arc::new(PrioritySemaphore::new(1));

    // Grant order is observed through a channel rather than by polling task
    // completion: a waiter that is granted immediately drops its permit and
    // lets the next one through, so `is_finished` says nothing about
    // ordering by the time it is checked.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<&'static str>();

    // Occupy the only slot.
    let held = semaphore.acquire(0).await.unwrap();

    // A low-priority waiter queues first, so it is genuinely older.
    let low_sem = semaphore.clone();
    let low_tx = tx.clone();
    let low = tokio::spawn(async move {
        let permit = low_sem.acquire(1).await.unwrap();
        let _ = low_tx.send("low");
        drop(permit);
    });
    wait_for_queued(&semaphore, 1).await;

    // A high-priority waiter queues behind it.
    let high_sem = semaphore.clone();
    let high = tokio::spawn(async move {
        let permit = high_sem.acquire(99).await.unwrap();
        let _ = tx.send("high");
        drop(permit);
    });
    wait_for_queued(&semaphore, 2).await;

    // Freeing the slot must serve the higher priority first, even though
    // the low-priority waiter has been waiting longer.
    drop(held);
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a waiter must be granted")
        .expect("channel must stay open");
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the second waiter must be granted")
        .expect("channel must stay open");
    assert_eq!(
        [first, second],
        ["high", "low"],
        "higher priority must be granted first regardless of arrival order"
    );
    low.await.unwrap();
    high.await.unwrap();

    // Equal priority must stay FIFO, so priority ordering cannot starve an
    // equal-priority job either.
    let held = semaphore.acquire(0).await.unwrap();
    let (fifo_tx, mut fifo_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
    let mut handles = Vec::new();
    for index in 0..3 {
        let sem = semaphore.clone();
        let tx = fifo_tx.clone();
        handles.push(tokio::spawn(async move {
            let permit = sem.acquire(7).await.unwrap();
            let _ = tx.send(index);
            drop(permit);
        }));
    }
    wait_for_queued(&semaphore, 3).await;
    drop(held);
    let mut order = Vec::new();
    for _ in 0..3 {
        order.push(
            tokio::time::timeout(Duration::from_secs(5), fifo_rx.recv())
                .await
                .expect("every equal-priority waiter must be granted")
                .expect("channel must stay open"),
        );
    }
    assert_eq!(
        order,
        vec![0, 1, 2],
        "equal priority must be served in arrival order"
    );
    for handle in handles {
        handle.await.unwrap();
    }
}

/// Waits until `expected` waiters are queued, so the ordering assertions
/// cannot race the spawn.
async fn wait_for_queued(semaphore: &Arc<PrioritySemaphore>, expected: usize) {
    for _ in 0..200 {
        if semaphore.queued() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!(
        "expected {expected} queued waiters, saw {}",
        semaphore.queued()
    );
}

#[test]
fn semaphore_priority_preserves_order_and_clamps_instead_of_wrapping() {
    assert_eq!(semaphore_priority(0), 0);
    assert_eq!(semaphore_priority(10), 10);
    assert_eq!(semaphore_priority(i32::MAX as u64), i32::MAX);
    // Past i32::MAX a plain cast would wrap negative and invert the
    // ordering, putting the "highest" priority job last.
    assert_eq!(semaphore_priority(i32::MAX as u64 + 1), Priority::MAX);
    assert_eq!(semaphore_priority(1 << 63), Priority::MAX);
    assert_eq!(semaphore_priority(u64::MAX), Priority::MAX);
    assert!(
        semaphore_priority(u64::MAX) > semaphore_priority(10),
        "ordering must survive saturation"
    );
}

#[test]
fn source_url_tag_is_added_once() {
    let mut file = FileObject {
        source: Some(FileSource::Url("https://example.test/file.jpg".into())),
        ..Default::default()
    };

    let url = "https://example.test/file.jpg";
    Scraper::add_source_url_tag(&mut file, url);
    Scraper::add_source_url_tag(&mut file, url);

    assert_eq!(file.tag_list.len(), 1);
    assert_eq!(file.tag_list[0].tags.len(), 1);
    assert_eq!(file.tag_list[0].tags[0].tag.name, url);
    assert_eq!(file.tag_list[0].tags[0].tag.namespace.name, "source_url");
}

#[test]
fn file_processing_guard_tracks_shutdown_work() {
    let active = Arc::new(AtomicUsize::new(0));
    active.fetch_add(1, Ordering::SeqCst);

    {
        let _guard = FileProcessingGuard {
            active: active.clone(),
        };
        assert_eq!(active.load(Ordering::SeqCst), 1);
    }

    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[test]
fn hash_match_ignores_hex_casing_and_whitespace() {
    // The e621 MD5 from the reported failure: uppercase and padded forms
    // must compare equal to the lowercase computed digest.
    let md5_upper = HashesSupported::Md5("D8153106DA2F23B23119337C34E55A66".to_string());
    assert!(hashes_match(&md5_upper, "d8153106da2f23b23119337c34e55a66"));
    let md5_padded = HashesSupported::Md5("  d8153106da2f23b23119337c34e55a66\n".to_string());
    assert!(hashes_match(
        &md5_padded,
        "d8153106da2f23b23119337c34e55a66"
    ));

    // SHA digests are canonical uppercase; lowercase input must match.
    let sha256_lower = HashesSupported::Sha256(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
    );
    assert!(hashes_match(
        &sha256_lower,
        "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
    ));

    // A `0x` prefix from some tools is tolerated.
    let sha512_prefixed = HashesSupported::Sha512(format!("0x{}", "a".repeat(128)));
    assert!(hashes_match(&sha512_prefixed, &"A".repeat(128)));

    // Genuinely different digests still mismatch.
    let md5_other = HashesSupported::Md5("0".repeat(32));
    assert!(!hashes_match(
        &md5_other,
        "d8153106da2f23b23119337c34e55a66"
    ));

    // Empty declared digest means "no expectation".
    assert!(hashes_match(
        &HashesSupported::Md5(String::new()),
        "d8153106da2f23b23119337c34e55a66"
    ));
    assert!(hashes_match(
        &HashesSupported::Md5("  ".to_string()),
        &"f".repeat(32)
    ));

    // ImageHash is base64 and case-sensitive: exact compare only.
    let image = HashesSupported::ImageHash("aBcDeFg=".to_string());
    assert!(hashes_match(&image, "aBcDeFg="));
    assert!(!hashes_match(&image, "abcdefg="));
}

/// The flush buffer must MERGE tag lists per file key, never overwrite.
///
/// The same physical file can reach the buffer twice in one page — e.g. a
/// fresh download entry followed by a dedup hit carrying a different scraped
/// tag list. `HashMap::insert` here silently dropped the first entry's tags,
/// which is how file↔tag relationships were lost on re-scrapes.
#[test]
fn flush_buffer_merges_tag_lists_instead_of_overwriting() {
    let key = FileManager {
        internal: FileInternal {
            id: None,
            hash: format!("{:0128x}", 1),
            extension: "jpg".to_string(),
            storage_id: 0,
            size_bytes: None,
        },
        identifying_hashes: vec![],
    };

    let action = |tag: &str| FileTagAction {
        operation: TagOperation::Add,
        tags: vec![PluginTag {
            tag: Tag::new(
                tag.to_string(),
                GenericNamespaceObj {
                    name: "regression".to_string(),
                    description: None,
                },
            ),
            tag_type: TagType::Normal,
            relates_to: None,
        }],
    };

    let mut batch = HashMap::new();

    // First landing (the File arm's shape).
    let mut first_tags = vec![action("first")];
    buffer_file_tags(&mut batch, key.clone(), &mut first_tags);
    assert!(
        first_tags.is_empty(),
        "tag list must be moved into the buffer"
    );

    // Second landing for the SAME file (e.g. a duplicate page entry).
    let mut second_tags = vec![action("second")];
    buffer_file_tags(&mut batch, key.clone(), &mut second_tags);

    let buffered = batch.get(&key).expect("buffered entry");
    let mut names: Vec<&str> = buffered
        .iter()
        .flat_map(|a| a.tags.iter().map(|t| t.tag.name.as_str()))
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["first", "second"],
        "both landings' tags must survive"
    );

    // A dedup-hit entry for the same file carries a pinned id, so it keys
    // differently — it must coexist, not clobber the download entry.
    let existing_key = FileManager {
        internal: FileInternal {
            id: Some(7),
            ..key.internal.clone()
        },
        identifying_hashes: vec![],
    };
    let mut existing_tags = vec![action("dedup")];
    buffer_file_tags(&mut batch, existing_key, &mut existing_tags);

    assert_eq!(batch.len(), 2, "distinct keys must coexist");
    let total_actions: usize = batch.values().map(|v| v.len()).sum();
    assert_eq!(total_actions, 3, "all three tag actions must survive");
}

/// An EMPTY tag list must not clobber or grow a buffered entry: appending
/// nothing leaves the previous landing's tags intact.
#[test]
fn flush_buffer_tolerates_empty_tag_lists() {
    let key = FileManager::from(FileInternal {
        id: None,
        hash: format!("{:0128x}", 2),
        extension: "png".to_string(),
        storage_id: 0,
        size_bytes: None,
    });

    let mut batch = HashMap::new();
    let mut empty: Vec<FileTagAction> = Vec::new();
    buffer_file_tags(&mut batch, key.clone(), &mut empty);

    assert!(
        batch.contains_key(&key),
        "the file itself must still be buffered so its row is written"
    );
    assert!(batch[&key].is_empty());
}

// ---------------------------------------------------------------------------
// file_download_logic decision-path tests.
//
// These pin the routing that decides whether a scraped tag list reaches the
// flush buffer: dedup hits must hand back the EXISTING row (AlreadyPresent)
// so its tags can be attached, while skip/dead/no-source cases must drop the
// file cleanly. None of these paths touch the network, so they run against
// synthetic PageDedup maps.
// ---------------------------------------------------------------------------

use crate::db::{SourceUrlFileStatus, hashes_supported_to_key};

struct DedupFixture {
    scraper: std::sync::Arc<Scraper>,
    _temp: tempfile::TempDir,
}

async fn dedup_fixture() -> DedupFixture {
    use crate::web::ratelimit::RatelimitManager;

    let temp = tempfile::tempdir().unwrap();
    let should_exit = Arc::new(AtomicBool::new(false));
    let db = crate::db::turso::TursoDatabase::new_with_exit(
        &temp.path().join("test.db"),
        should_exit.clone(),
    )
    .await;
    let plugin_manager =
        crate::plugins::PluginManager::new(temp.path(), db.clone(), should_exit.clone());
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap(),
    );
    let download_manager = crate::web::manager::DownloadsManager::new(
        db,
        plugin_manager.clone(),
        pool,
        should_exit.clone(),
    );

    let scraper = std::sync::Arc::new(Scraper {
        job: shared_types::DbJobsObj {
            id: 1,
            isrunning: false,
            config: shared_types::PluginJob::default(),
        },
        ratelimiter: Arc::new(RatelimitManager::new(1, std::time::Duration::from_secs(1))),
        plugin_manager,
        plugin: shared_types::Plugin {
            name: "testsite".to_string(),
            ..Default::default()
        },
        download_manager,
        text_client: Arc::new(reqwest::Client::new()),
        file_client: Arc::new(reqwest::Client::new()),
    });

    DedupFixture {
        scraper,
        _temp: temp,
    }
}

fn existing_row(file_id: u64) -> FileInternal {
    FileInternal {
        id: Some(file_id),
        hash: format!("{:0128X}", 0x2A),
        extension: "jpg".to_string(),
        storage_id: 0,
        size_bytes: Some(1024),
    }
}

fn scraped_file(url: &str) -> FileObject {
    FileObject {
        source: Some(FileSource::Url(url.to_string())),
        ..Default::default()
    }
}

fn empty_dedup() -> (
    HashMap<String, SourceUrlFileStatus>,
    HashMap<(String, String), FileInternal>,
    HashMap<(String, String), FileInternal>,
) {
    (HashMap::new(), HashMap::new(), HashMap::new())
}

async fn route(
    fx: &DedupFixture,
    file: &mut FileObject,
    source_files: &HashMap<String, SourceUrlFileStatus>,
    hash_files: &HashMap<(String, String), FileInternal>,
    skip_files: &HashMap<(String, String), FileInternal>,
) -> Result<Option<crate::web::manager::FileReturn>, Box<dyn std::error::Error>> {
    let dedup = crate::web::manager::PageDedup {
        source_files,
        hash_files,
        skip_files,
    };
    let mut jobs = Vec::new();
    let mut download_issue = false;
    fx.scraper
        .clone()
        .file_download_logic(
            file,
            &mut jobs,
            &mut download_issue,
            &dedup,
            Arc::new(crate::web::manager::PrioritySemaphore::new(1)),
        )
        .await
}

/// A URL dedup hit must return the EXISTING row via AlreadyPresent — this is
/// the routing that lets the flush buffer attach the scraped tag list to the
/// file id already in the db. Returning a fresh/new-shaped row here (or None)
/// is exactly how the tag-loss regression happened.
#[tokio::test]
async fn url_dedup_hit_routes_to_already_present_with_existing_row() {
    let fx = dedup_fixture().await;
    let url = "https://test.site/img/1.jpg";
    let existing = existing_row(42);

    let mut source_files = HashMap::new();
    source_files.insert(
        url.to_string(),
        SourceUrlFileStatus {
            file: Some(existing.clone()),
            dead: false,
        },
    );
    let (hash_files, skip_files) = (HashMap::new(), HashMap::new());

    let mut file = scraped_file(url);
    let ret = route(&fx, &mut file, &source_files, &hash_files, &skip_files)
        .await
        .expect("routing ok");

    match ret {
        Some(crate::web::manager::FileReturn::AlreadyPresent(row)) => {
            assert_eq!(row.id, Some(42), "must carry the EXISTING db row id");
            assert_eq!(row.hash, existing.hash);
        }
        other => panic!("dedup hit must route to AlreadyPresent, got {other:?}"),
    }

    // The source_url tag must already be on the tag list, so the buffered
    // flush carries it alongside the scraped tags.
    assert!(
        file.tag_list.iter().any(|action| {
            action
                .tags
                .iter()
                .any(|t| t.tag.namespace.name == "source_url")
        }),
        "source_url tag must ride along on the dedup hit's tag list"
    );
}

/// A known identifying hash (MD5 here) must route to AlreadyPresent with the
/// existing row, even though the URL is new — the same file posted under two
/// URLs must not become two rows, and its tags must still land.
#[tokio::test]
async fn hash_dedup_hit_routes_to_already_present_with_existing_row() {
    let fx = dedup_fixture().await;
    let md5 = "d8153106da2f23b23119337c34e55a66".to_string();

    let mut hash_files = HashMap::new();
    hash_files.insert(
        hashes_supported_to_key(&shared_types::HashesSupported::Md5(md5.clone())),
        existing_row(7),
    );
    let (source_files, skip_files) = (HashMap::new(), HashMap::new());

    let mut file = scraped_file("https://test.site/img/repost.jpg");
    file.hash = vec![shared_types::HashesSupported::Md5(md5)];

    let ret = route(&fx, &mut file, &source_files, &hash_files, &skip_files)
        .await
        .expect("routing ok");

    match ret {
        Some(crate::web::manager::FileReturn::AlreadyPresent(row)) => {
            assert_eq!(row.id, Some(7), "hash hit must carry the existing row id");
        }
        other => panic!("hash dedup hit must route to AlreadyPresent, got {other:?}"),
    }
}

/// A dead URL must be dropped cleanly: no download attempt, no buffered
/// entry, no panic.
#[tokio::test]
async fn dead_url_is_dropped_cleanly() {
    let fx = dedup_fixture().await;
    let url = "https://test.site/dead.jpg";

    let mut source_files = HashMap::new();
    source_files.insert(
        url.to_string(),
        SourceUrlFileStatus {
            file: Some(existing_row(1)),
            dead: true,
        },
    );
    let (hash_files, skip_files) = (HashMap::new(), HashMap::new());

    let mut file = scraped_file(url);
    let ret = route(&fx, &mut file, &source_files, &hash_files, &skip_files)
        .await
        .expect("routing ok");
    assert!(ret.is_none(), "dead URL must not produce a FileReturn");
}

/// A skip_if FileTagRelationship hit must drop the file WITHOUT attaching
/// its tags: the rule matched a DIFFERENT file holding that tag, so
/// attaching would mislink. This is the intended Ok(None) — a regression
/// back to File/AlreadyPresent here would corrupt relationships.
#[tokio::test]
async fn skip_tag_relationship_hit_drops_file_and_tags() {
    let fx = dedup_fixture().await;
    let ns = shared_types::GenericNamespaceObj {
        name: "artist".to_string(),
        description: None,
    };
    let skip_tag = shared_types::Tag::new("someone".to_string(), ns.clone());

    let mut skip_files = HashMap::new();
    skip_files.insert((ns.name.clone(), skip_tag.name.clone()), existing_row(99));
    let (source_files, hash_files) = (HashMap::new(), HashMap::new());

    let mut file = scraped_file("https://test.site/img/2.jpg");
    file.skip_if = vec![shared_types::SkipIf::FileTagRelationship(skip_tag)];

    let ret = route(&fx, &mut file, &source_files, &hash_files, &skip_files)
        .await
        .expect("routing ok");
    assert!(ret.is_none(), "skip-tag hit must drop the file");
}

/// A URL already being downloaded by a concurrent task must report
/// InDownloadQueue rather than starting a second transfer of the same file.
#[tokio::test]
async fn queued_url_reports_in_download_queue() {
    let fx = dedup_fixture().await;
    let url = "https://test.site/img/inflight.jpg";
    let (source_files, hash_files, skip_files) = empty_dedup();

    fx.scraper
        .download_manager
        .downloading_urls
        .write()
        .await
        .insert(url.to_string());

    let mut file = scraped_file(url);
    let ret = route(&fx, &mut file, &source_files, &hash_files, &skip_files)
        .await
        .expect("routing ok");
    assert!(
        matches!(ret, Some(crate::web::manager::FileReturn::InDownloadQueue)),
        "in-flight URL must report InDownloadQueue, got {ret:?}"
    );
}

/// A file with no source at all must be dropped cleanly.
#[tokio::test]
async fn file_without_source_is_dropped_cleanly() {
    let fx = dedup_fixture().await;
    let (source_files, hash_files, skip_files) = empty_dedup();

    let mut file = FileObject::default();
    let ret = route(&fx, &mut file, &source_files, &hash_files, &skip_files)
        .await
        .expect("routing ok");
    assert!(
        ret.is_none(),
        "sourceless file must not produce a FileReturn"
    );
}

// ---------------------------------------------------------------------------
// PageState / commit_file_return / prefetch_page_dedup: the pieces the
// scraper loop is now composed of, each pinned end-to-end against a real
// test database.
// ---------------------------------------------------------------------------

use crate::db::turso::TursoDatabase as ManagerTestDb;
use std::sync::atomic::AtomicBool as TestAtomicBool;

async fn page_test_db() -> (std::sync::Arc<ManagerTestDb>, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let should_exit = Arc::new(TestAtomicBool::new(false));
    let db = ManagerTestDb::new_with_exit(&temp.path().join("test.db"), should_exit).await;
    (db, temp)
}

fn page_tag(name: &str, ns: &str) -> shared_types::PluginTag {
    shared_types::PluginTag {
        tag: shared_types::Tag::new(
            name.to_string(),
            shared_types::GenericNamespaceObj {
                name: ns.to_string(),
                description: None,
            },
        ),
        tag_type: shared_types::TagType::Normal,
        relates_to: None,
    }
}

fn page_add_action(tags: Vec<shared_types::PluginTag>) -> FileTagAction {
    FileTagAction {
        operation: TagOperation::Add,
        tags,
    }
}

fn page_manager(hash: &str, id: Option<u64>) -> FileManager {
    FileManager {
        internal: FileInternal {
            id,
            hash: hash.to_string(),
            extension: "jpg".to_string(),
            storage_id: 0,
            size_bytes: Some(1024),
        },
        identifying_hashes: vec![],
    }
}

async fn page_file_id(db: &ManagerTestDb, digest: &str) -> u64 {
    let conn = db.connect().expect("connect");
    let mut rows = conn
        .query(
            "SELECT file_id FROM FileHashes WHERE algorithm = 'SHA512' AND digest = ?1;",
            (digest,),
        )
        .await
        .expect("file lookup");
    rows.next()
        .await
        .expect("row")
        .expect("file row")
        .get::<u64>(0)
        .expect("file id")
}

async fn page_file_tag_ids(db: &ManagerTestDb, file_id: u64) -> HashSet<u64> {
    db.ipc_relationship_get_tag_id(&file_id).await
}

async fn page_tag_id(db: &ManagerTestDb, name: &str) -> u64 {
    let conn = db.connect().expect("connect");
    let mut rows = conn
        .query("SELECT id FROM Tags WHERE name = ?1;", (name,))
        .await
        .expect("tag lookup");
    rows.next()
        .await
        .expect("row")
        .expect("tag row")
        .get::<u64>(0)
        .expect("tag id")
}

fn page_keep_flag() -> Arc<TestAtomicBool> {
    Arc::new(TestAtomicBool::new(true))
}

/// The page-end tail flush must commit everything still buffered — this is
/// the invariant that keeps buffered files' tags alive on every loop exit
/// path (breaks included), since the tail flush sits after the loop.
#[tokio::test]
async fn page_state_tail_flush_commits_buffered_files_and_tags() {
    let (db, _temp) = page_test_db().await;
    let state = PageState::new();
    let keep = page_keep_flag();

    let hash_a = format!("{:0128X}", 0x21);
    // Two landings for the same file merge into one buffered entry.
    let mut first = vec![page_add_action(vec![page_tag("landing-1", "regression")])];
    state
        .buffer_file(page_manager(&hash_a, None), &mut first)
        .await;
    let mut second = vec![page_add_action(vec![page_tag("landing-2", "regression")])];
    state
        .buffer_file(page_manager(&hash_a, None), &mut second)
        .await;
    // A different file lands alongside.
    let hash_b = format!("{:0128X}", 0x22);
    let mut other = vec![page_add_action(vec![page_tag("other", "regression")])];
    state
        .buffer_file(page_manager(&hash_b, None), &mut other)
        .await;

    let ok = state
        .flush_files(db.clone(), "test", 1, &keep, "tail flush failed")
        .await;
    assert!(ok, "tail flush must succeed");
    assert_eq!(
        state.buffered_file_count().await,
        0,
        "buffer must be empty after tail flush"
    );

    let file_id = page_file_id(&db, &hash_a).await;
    let mut expected = HashSet::new();
    expected.insert(page_tag_id(&db, "landing-1").await);
    expected.insert(page_tag_id(&db, "landing-2").await);
    assert_eq!(
        page_file_tag_ids(&db, file_id).await,
        expected,
        "merged landings' tags must all attach to the file"
    );
    assert_eq!(
        page_file_tag_ids(&db, page_file_id(&db, &hash_b).await).await,
        HashSet::from([page_tag_id(&db, "other").await]),
    );
}

/// Measures the one PageState cost that is still a guess: `flush_files` holds
/// the buffer lock across the whole `process_scraper` commit, so every
/// `buffer_file` on the page queues behind that commit instead of behind a map
/// operation.
///
/// This measures the mechanism directly rather than racing a real flush. An
/// earlier version spawned a flush and polled for the buffer to empty, which
/// could not work: `flush_files` does `mem::take` *before* the commit, so the
/// buffer reads empty at exactly the moment the lock is still held, and the
/// probe was guaranteed to find it free. Two arms that both measure an
/// uncontended lock agree with each other and say nothing.
///
/// So: take the lock by hand, drain the batch, then hold it across a wait that
/// stands in for the commit (`BENCH_PAGESTATE_COMMIT_MS`, default 20 ms -- a
/// real `process_scraper` on a 128-entry batch is single-digit ms, so this is
/// if anything generous). While it is held, time a `buffer_file`. Then repeat
/// with the guard dropped before the wait.
///
/// Both shapes are simulated rather than read out of `flush_files`, so this
/// stays a record of why the change was made after `flush_files` was changed to
/// release early. The number that matters is the probe's wait: if it does not
/// move between the arms, releasing the lock early buys nothing.
///
/// ```text
/// BENCH_PAGESTATE_BATCH=128 BENCH_PAGESTATE_COMMIT_MS=20 \
///     cargo test --release --lib web::manager::tests::page_state_lock_bench \
///     -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "manual benchmark: flush lock held across commit vs released"]
async fn page_state_lock_bench() {
    use std::time::Instant;

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    let batch_size = env_u64("BENCH_PAGESTATE_BATCH", 128).max(1) as usize;
    let commit_ms = env_u64("BENCH_PAGESTATE_COMMIT_MS", 20);
    let rounds = env_u64("BENCH_PAGESTATE_ROUNDS", 5).max(1);

    let (db, _temp) = page_test_db().await;
    let keep = page_keep_flag();

    // One arm, run as a task. The only difference between arms is where the
    // guard drops, which is the thing under test.
    //
    // `commit_ms == 0` runs the real `process_scraper` on the real batch, so
    // the two arms are compared on the production path. Passing an empty batch
    // there would measure a no-op commit and make both arms look free.
    async fn run_flush(
        state: Arc<PageState>,
        db: Arc<ManagerTestDb>,
        commit_ms: u64,
        hold: bool,
    ) -> f64 {
        let started = Instant::now();
        if hold {
            // Shipped shape: one guard, held across the commit.
            let mut guard = state.files.lock().await;
            let batch = std::mem::take(&mut *guard);
            if commit_ms == 0 {
                db.process_scraper(batch, Vec::new(), "bench".to_string())
                    .await;
            } else {
                drop(batch);
                tokio::time::sleep(Duration::from_millis(commit_ms)).await;
            }
            drop(guard);
        } else {
            // Candidate: take the batch, release the lock, then commit.
            let batch = {
                let mut guard = state.files.lock().await;
                std::mem::take(&mut *guard)
            };
            if commit_ms == 0 {
                db.process_scraper(batch, Vec::new(), "bench".to_string())
                    .await;
            } else {
                drop(batch);
                tokio::time::sleep(Duration::from_millis(commit_ms)).await;
            }
        }
        started.elapsed().as_secs_f64() * 1_000.0
    }

    // The probe is deliberately separate from the flush so its wait is not
    // folded into the flush's own duration.
    async fn probe(state: &Arc<PageState>) -> f64 {
        let started = Instant::now();
        let mut tags = Vec::new();
        state
            .buffer_file(page_manager(&format!("{:0128X}", 0xDEAD), None), &mut tags)
            .await;
        started.elapsed().as_secs_f64() * 1_000.0
    }

    println!(
        "\nBENCH page-state flush lock: {batch_size} entries, commit ~{commit_ms} ms, \
         {rounds} rounds"
    );

    for (label, hold) in [
        ("held across commit (old)", true),
        ("released before commit (shipped)", false),
    ] {
        let mut wait = f64::MAX;
        let mut flush_ms = f64::MAX;
        for _ in 0..rounds {
            let state = Arc::new(PageState::new());
            for i in 0..batch_size {
                let key = page_manager(&format!("{i:0128X}"), None);
                let mut tags = vec![page_add_action(vec![page_tag(
                    &format!("bench-{i}"),
                    "bench_ns",
                )])];
                state.buffer_file(key, &mut tags).await;
            }

            let flush_state = state.clone();
            let flush_db = db.clone();
            let flush =
                tokio::spawn(
                    async move { run_flush(flush_state, flush_db, commit_ms, hold).await },
                );

            // Let the flush task reach its lock acquisition. Not a correctness
            // signal -- just enough of a head start that the probe is
            // contending rather than winning the race. 2 ms is well inside the
            // commit window, so the probe still lands mid-commit.
            tokio::time::sleep(Duration::from_millis(2)).await;
            let w = probe(&state).await;
            let took = flush.await.unwrap();

            wait = wait.min(w);
            flush_ms = flush_ms.min(took);
        }
        println!("  {label:<28} flush {flush_ms:>8.3} ms   probe wait {wait:>8.3} ms");
    }

    // Sanity anchor: an uncontended buffer_file, so a probe figure can be read
    // as "N x the uncontended cost" rather than as an absolute.
    let state = PageState::new();
    let mut best = f64::MAX;
    for _ in 0..5 {
        let started = Instant::now();
        let mut tags = Vec::new();
        state
            .buffer_file(page_manager(&format!("{:0128X}", 0xFEED), None), &mut tags)
            .await;
        best = best.min(started.elapsed().as_secs_f64() * 1_000.0);
    }
    println!(
        "  {label:<28} {best:>8.3} ms   (uncontended reference)",
        label = "no contention"
    );
    let _ = keep;
}

/// Batched flushing must stay batched: below FLUSH_BATCH_SIZE nothing is
/// committed, at the threshold the buffer empties in one go.
#[tokio::test]
async fn page_state_flushes_only_at_threshold() {
    let (db, _temp) = page_test_db().await;
    let state = PageState::new();
    let keep = page_keep_flag();

    for i in 0..(FLUSH_BATCH_SIZE - 1) {
        let mut tags = Vec::new();
        state
            .buffer_file(
                page_manager(&format!("{:0128X}", 0x30 + i), None),
                &mut tags,
            )
            .await;
    }
    // Below the threshold: nothing committed, everything still buffered.
    let ok = state
        .flush_files_if_full(db.clone(), "test", 1, &keep)
        .await;
    assert!(ok);
    assert_eq!(
        state.buffered_file_count().await,
        FLUSH_BATCH_SIZE - 1,
        "below the threshold the buffer must be untouched"
    );

    // Crossing the threshold commits the whole batch.
    let mut tags = Vec::new();
    state
        .buffer_file(
            page_manager(&format!("{:0128X}", 0x30 + FLUSH_BATCH_SIZE), None),
            &mut tags,
        )
        .await;
    let ok = state
        .flush_files_if_full(db.clone(), "test", 1, &keep)
        .await;
    assert!(ok);
    assert_eq!(
        state.buffered_file_count().await,
        0,
        "at the threshold the buffer must commit and empty"
    );

    // The committed files really are in the db.
    assert!(
        page_file_id(&db, &format!("{:0128X}", 0x30)).await > 0,
        "threshold flush must have persisted the first buffered file"
    );
}

/// `commit_file_return`'s File arm end-to-end: a downloaded file plus its
/// tag list must land as a file row with the tags attached after the tail
/// flush.
#[tokio::test]
async fn commit_file_return_persists_downloaded_file_and_tags() {
    let (db, _temp) = page_test_db().await;
    let fx = dedup_fixture().await;
    let state = PageState::new();
    let keep = page_keep_flag();

    let hash = format!("{:0128X}", 0x41);
    let mut file = FileObject::default();
    file.tag_list = vec![page_add_action(vec![page_tag("downloaded", "regression")])];

    let ok = fx
        .scraper
        .commit_file_return(
            crate::web::manager::FileReturn::File(page_manager(&hash, None)),
            &mut file,
            &state,
            "test",
            1,
            &keep,
        )
        .await;
    assert!(ok);

    // Below the threshold the entry stays buffered.
    assert_eq!(state.buffered_file_count().await, 1);

    // Page-end tail flush persists it.
    let ok = state
        .flush_files(db.clone(), "test", 1, &keep, "tail failed")
        .await;
    assert!(ok);

    let file_id = page_file_id(&db, &hash).await;
    assert_eq!(
        page_file_tag_ids(&db, file_id).await,
        HashSet::from([page_tag_id(&db, "downloaded").await]),
    );
}

/// `commit_file_return`'s AlreadyPresent arm end-to-end: a dedup hit's tag
/// list must attach to the EXISTING row — the exact path that regressed
/// when the arm used to be a no-op.
#[tokio::test]
async fn commit_file_return_already_present_attaches_tags_to_existing_row() {
    let (db, _temp) = page_test_db().await;
    let fx = dedup_fixture().await;
    let state = PageState::new();
    let keep = page_keep_flag();

    // Walk 1: file lands as new.
    let hash = format!("{:0128X}", 0x42);
    let mut first = FileObject::default();
    first.tag_list = vec![page_add_action(vec![page_tag("first-walk", "regression")])];
    assert!(
        fx.scraper
            .commit_file_return(
                crate::web::manager::FileReturn::File(page_manager(&hash, None)),
                &mut first,
                &state,
                "test",
                1,
                &keep,
            )
            .await
    );
    assert!(
        state
            .flush_files(db.clone(), "test", 1, &keep, "tail failed")
            .await
    );
    let file_id = page_file_id(&db, &hash).await;

    // Walk 2: dedup hit returns the existing row with fresh tags.
    let mut second = FileObject::default();
    second.tag_list = vec![page_add_action(vec![page_tag("rewalk", "regression")])];
    assert!(
        fx.scraper
            .commit_file_return(
                crate::web::manager::FileReturn::AlreadyPresent(FileInternal {
                    id: Some(file_id),
                    hash: hash.clone(),
                    extension: "jpg".to_string(),
                    storage_id: 0,
                    size_bytes: Some(1024),
                }),
                &mut second,
                &state,
                "test",
                1,
                &keep,
            )
            .await
    );
    assert!(
        state
            .flush_files(db.clone(), "test", 1, &keep, "tail failed")
            .await
    );

    let mut expected = HashSet::new();
    expected.insert(page_tag_id(&db, "first-walk").await);
    expected.insert(page_tag_id(&db, "rewalk").await);
    assert_eq!(
        page_file_tag_ids(&db, file_id).await,
        expected,
        "dedup hit's tags must attach to the existing row"
    );
    // Exactly one file row for the digest.
    let conn = db.connect().expect("connect");
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM File WHERE id = ?1;",
            (file_id as i64,),
        )
        .await
        .expect("count");
    assert_eq!(
        rows.next()
            .await
            .expect("row")
            .expect("count")
            .get::<i64>(0)
            .expect("count"),
        1,
        "dedup hit must not create a duplicate row"
    );
}

/// The InDownloadQueue arm must buffer nothing — the transferring task owns
/// this file's persistence.
#[tokio::test]
async fn commit_file_return_in_download_queue_buffers_nothing() {
    let (_db, _temp) = page_test_db().await;
    let fx = dedup_fixture().await;
    let state = PageState::new();
    let keep = page_keep_flag();

    let mut file = FileObject::default();
    file.tag_list = vec![page_add_action(vec![page_tag("dropped", "regression")])];

    let ok = fx
        .scraper
        .commit_file_return(
            crate::web::manager::FileReturn::InDownloadQueue,
            &mut file,
            &state,
            "test",
            1,
            &keep,
        )
        .await;
    assert!(ok);
    assert_eq!(
        state.buffered_file_count().await,
        0,
        "InDownloadQueue must buffer nothing"
    );
    // The tag list is left untouched on the file (consumed by nobody).
    assert_eq!(file.tag_list.len(), 1);
}

/// The job/tag accumulators must drain to the db exactly when they cross
/// ACCUMULATOR_DRAIN_SIZE, and the drained content must actually persist.
#[tokio::test]
async fn page_state_drains_tag_accumulator_at_threshold() {
    let (db, _temp) = page_test_db().await;
    let state = PageState::new();
    let keep = page_keep_flag();

    // One below the threshold: stays in memory.
    let below: Vec<shared_types::PluginTag> = (0..ACCUMULATOR_DRAIN_SIZE - 1)
        .map(|i| page_tag(&format!("drain-{i}"), "regression"))
        .collect();
    state.push_tags(db.clone(), below, "test", 1, &keep).await;
    let (jobs, tags) = state.counts().await;
    assert_eq!(
        tags,
        ACCUMULATOR_DRAIN_SIZE - 1,
        "below threshold nothing drains"
    );
    assert_eq!(jobs, 0);

    // Crossing the threshold drains, and the drained tags persist.
    state
        .push_tags(
            db.clone(),
            vec![page_tag("drain-final", "regression")],
            "test",
            1,
            &keep,
        )
        .await;
    let (_, tags) = state.counts().await;
    assert_eq!(tags, 0, "crossing the threshold must drain the accumulator");
    assert!(
        page_tag_id(&db, "drain-0").await > 0,
        "drained tags must be persisted"
    );
    assert!(page_tag_id(&db, "drain-final").await > 0);
}

/// `prefetch_page_dedup` must resolve all three maps in one pass: source-URL
/// status, known identifying hashes, and skip-if tag owners.
#[tokio::test]
async fn prefetch_page_dedup_resolves_all_three_maps() {
    let (_db, _temp) = page_test_db().await;
    let fx = dedup_fixture().await;

    // Seed: a file whose tags include its source_url tag (as the manager
    // adds it) and an artist tag a skip_if rule can target.
    let hash = format!("{:0128X}", 0x50);
    let url = "https://test.site/seed/1.jpg";
    let mut seed = HashMap::new();
    seed.insert(
        page_manager(&hash, None),
        vec![page_add_action(vec![
            page_tag(url, "source_url"),
            page_tag("skipme", "artist"),
        ])],
    );
    let fx_db = fx.scraper.download_manager.db.clone();
    assert!(
        fx_db
            .clone()
            .process_scraper(seed, Vec::new(), "prefetch-seed".to_string())
            .await
    );
    let seeded_id = page_file_id(&fx_db, &hash).await;

    let mut probe = FileObject::default();
    probe.source = Some(FileSource::Url(url.to_string()));
    probe.hash = vec![shared_types::HashesSupported::Sha512(hash.clone())];
    probe.skip_if = vec![shared_types::SkipIf::FileTagRelationship(
        shared_types::Tag::new(
            "skipme".to_string(),
            shared_types::GenericNamespaceObj {
                name: "artist".to_string(),
                description: None,
            },
        ),
    )];

    let files = HashSet::from([probe]);
    let dedup = fx
        .scraper
        .prefetch_page_dedup(&files)
        .await
        .expect("prefetch");

    let source_status = dedup
        .source_files
        .get(url)
        .expect("source url must resolve");
    assert!(!source_status.dead);
    assert_eq!(
        source_status.file.as_ref().expect("file").id,
        Some(seeded_id),
        "source url must resolve to the seeded file"
    );

    let key =
        crate::db::hashes_supported_to_key(&shared_types::HashesSupported::Sha512(hash.clone()));
    assert_eq!(
        dedup.hash_files.get(&key).expect("hash must resolve").id,
        Some(seeded_id),
        "identifying hash must resolve to the seeded file"
    );

    let skip = dedup
        .skip_files
        .get(&("artist".to_string(), "skipme".to_string()))
        .expect("skip tag must resolve");
    assert_eq!(
        skip.id,
        Some(seeded_id),
        "skip tag must resolve to its owner"
    );
}

// ---------------------------------------------------------------------------
// Perceptual hash (ImageHash) agreement between host and plugin
// ---------------------------------------------------------------------------

mod image_hash_agreement {
    use super::*;

    /// Writes a small deterministic PNG, the kind of file both sides hash.
    fn gradient_png(w: u32, h: u32, seed: u8) -> std::path::PathBuf {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(format!("grad-{seed}.png"));
        let mut img = image::RgbaImage::new(w, h);
        for (x, y, pixel) in img.enumerate_pixels_mut() {
            let v = (x
                .wrapping_mul(3)
                .wrapping_add(y.wrapping_mul(5))
                .wrapping_add(seed as u32)) as u8;
            *pixel = image::Rgba([v, 255u8.wrapping_sub(v), (x ^ y) as u8, 255]);
        }
        let mut file = std::fs::File::create(&path).unwrap();
        image::DynamicImage::ImageRgba8(img)
            .write_to(&mut file, image::ImageFormat::Png)
            .expect("encode");
        // Leak the tempdir: the file must outlive this call.
        std::mem::forget(dir);
        path
    }

    /// The host's `image_hash_from_path` must produce a stable, well-formed hash.
    ///
    /// This is the producer side of the contract. If it changed, every hash the
    /// plugin backfills from `FileHashes` would be computed under different
    /// settings and would silently never match a locally-computed one.
    #[test]
    fn host_hash_is_stable_and_well_formed() {
        let path = gradient_png(64, 64, 1);
        let a = image_hash_from_path(&path).expect("hash a");
        let b = image_hash_from_path(&path).expect("hash b");
        // Pinned so the clip-search plugin's test can assert the same digests:
        // the two implementations are then tied to each other rather than each
        // merely agreeing with itself.
        assert_eq!(a, "9zvAhLWgD/g", "host hash for seed 1 drifted");
        assert_eq!(
            image_hash_from_path(&gradient_png(64, 64, 200)).unwrap(),
            "x2fBlyMPUwc",
            "host hash for seed 200 drifted"
        );
        assert_eq!(a, b, "hashing must be deterministic");
        // 64-bit Median hash -> 11 base64 chars (STANDARD_NO_PAD, no padding).
        assert_eq!(a.len(), 11, "unexpected hash width for {a}");
        assert!(
            a.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/'),
            "unexpected base64 alphabet in {a}"
        );
    }

    /// Different images must hash differently, and by a wide margin.
    ///
    /// This is what the duplicate threshold assumes: a 64-bit hash puts
    /// unrelated images near 32 bits apart, so a threshold of 8 bits is far
    /// outside the noise floor. If two obviously different gradients came back
    /// within 8 bits, every pair would be reported as a duplicate.
    #[test]
    fn different_images_are_far_apart() {
        use image_hasher::ImageHash;

        let a = gradient_png(64, 64, 1);
        let b = gradient_png(64, 64, 200);
        let ha = image_hash_from_path(&a).expect("hash a");
        let hb = image_hash_from_path(&b).expect("hash b");

        let left = ImageHash::<Box<[u8]>>::from_base64(&ha).expect("parse a");
        let right = ImageHash::<Box<[u8]>>::from_base64(&hb).expect("parse b");
        let bits = left.dist(&right);
        assert!(
            bits > 8,
            "different images must exceed the 8-bit duplicate threshold, got {bits}"
        );
    }

    /// A re-encode of the same picture must land inside the duplicate threshold.
    ///
    /// This is the case the feature exists for: the same image saved again,
    /// possibly at a different quality. It is also the case CLIP scores lowest,
    /// which is why the hash rescues it rather than replacing CLIP.
    #[test]
    fn a_reencode_stays_within_the_threshold() {
        use image_hasher::ImageHash;

        // Same pixels, encoded twice with different PNG compression, plus a
        // lossless resave through the decoder. A perceptual hash must be stable
        // across that.
        let path = gradient_png(64, 64, 7);
        let ha = image_hash_from_path(&path).expect("hash a");

        let img = image::open(&path).expect("decode");
        let reencoded = std::path::PathBuf::from("/tmp/opencode/reencode-test.png");
        let mut out = std::fs::File::create(&reencoded).unwrap();
        img.write_to(&mut out, image::ImageFormat::Png)
            .expect("reencode");
        let hb = image_hash_from_path(&reencoded).expect("hash b");

        let left = ImageHash::<Box<[u8]>>::from_base64(&ha).expect("parse a");
        let right = ImageHash::<Box<[u8]>>::from_base64(&hb).expect("parse b");
        assert_eq!(
            left.dist(&right),
            0,
            "a lossless re-encode must hash identically"
        );
    }
}
