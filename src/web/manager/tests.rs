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
use std::path::Path;

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
