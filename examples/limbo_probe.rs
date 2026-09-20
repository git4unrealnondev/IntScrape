//! One-off probe: time the slurp hash keyset query through the same turso
//! (limbo) read path the app uses, to see whether the (file_id, algorithm)
//! PK index is honored. Run: cargo run --example limbo_probe -- <file.db>

use std::time::Instant;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let path = std::env::args().nth(1).expect("usage: limbo_probe <file.db>");
    let db = turso::Builder::new_local(&path)
        .read_only(true)
        .experimental_without_rowid(true)
        .build()
        .await
        .expect("open");
    let conn = db.connect().expect("connect");

    // One full-table scan baseline.
    {
        let started = Instant::now();
        let mut stmt = conn.prepare("SELECT count(*) FROM FileHashes").await.unwrap();
        let count: i64 = stmt.query_row(()).await.unwrap().get(0).unwrap();
        println!(
            "count(*) = {count} in {:?}",
            started.elapsed()
        );
    }

    // Consecutive keyset batches, exactly like slurp.rs:783-831.
    let mut last_file_id = -1_i64;
    for i in 0..6_i64 {
        let mut stmt = conn
            .prepare(
                "SELECT h.file_id, h.algorithm, h.digest
                 FROM FileHashes h
                 WHERE h.file_id > ?1
                 ORDER BY h.file_id
                 LIMIT ?2",
            )
            .await
            .unwrap();
        let batch_started = Instant::now();
        let mut rows = stmt
            .query([last_file_id, 4600])
            .await
            .unwrap();
        let mut batch: Vec<(i64, String, String)> = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            batch.push((row.get(0).unwrap(), row.get(1).unwrap(), row.get(2).unwrap()));
        }
        let Some((last_id, _, _)) = batch.last() else { break };
        last_file_id = *last_id;
        println!(
            "batch {i}: rows={} last_file_id={last_file_id} elapsed={:?}",
            batch.len(),
            batch_started.elapsed()
        );
        drop(stmt);
    }

    // Deep-position batches: same query but seeded deep into the table. An
    // index seek costs the same anywhere; a full scan shrinks as `last` grows
    // (fewer qualifying rows remain) but each batch still walks from page 0.
    for i in 0..4_i64 {
        let near = 9_100_000_i64 + i * 4600;
        let mut stmt = conn
            .prepare(
                "SELECT h.file_id, h.algorithm, h.digest
                 FROM FileHashes h
                 WHERE h.file_id > ?1
                 ORDER BY h.file_id
                 LIMIT ?2",
            )
            .await
            .unwrap();
        let batch_started = Instant::now();
        let mut rows = stmt.query([near, 4600]).await.unwrap();
        let mut batch: Vec<(i64, String, String)> = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            batch.push((row.get(0).unwrap(), row.get(1).unwrap(), row.get(2).unwrap()));
        }
        let last_id = batch.last().map(|(id, _, _)| *id).unwrap_or(near);
        println!(
            "deep batch {i}: rows={} last_file_id={last_id} elapsed={:?}",
            batch.len(),
            batch_started.elapsed()
        );
        drop(stmt);
    }
}