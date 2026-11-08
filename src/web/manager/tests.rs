//! Tests for the `manager` module (extracted from `manager.rs`).

use super::*;

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
