//! Probes for turso 0.8.2's vector support, which the ImageHash near-match
//! design depends on.
//!
//! Two questions, in order:
//!
//! 1. `probe_vector_index` -- what is supported at all? Which vector types can
//!    be indexed, which distance functions exist, and does a distance-ordered
//!    query actually get served by an index rather than a scan.
//! 2. `probe_recall` -- given an index that answers, how good is the answer? A
//!    vector index is only useful if it returns the true nearest neighbours
//!    while touching a fraction of the corpus, so this measures recall and
//!    latency against ground truth computed independently in Rust.
//!
//! Both build throwaway databases in /dev/shm and touch no application table.
//!
//! ```text
//! cargo test --release --lib probe_vector_index -- --ignored --nocapture
//! cargo test --release --lib probe_recall -- --ignored --nocapture
//! ```

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    use turso::{Builder, Connection};

    /// Prints the rows a statement returns, best-effort across column types.
    async fn run(conn: &Connection, label: &str, sql: &str) -> Vec<Vec<String>> {
        let out = match conn.query(sql, ()).await {
            Ok(mut rows) => {
                let mut out: Vec<Vec<String>> = Vec::new();
                loop {
                    match rows.next().await {
                        Ok(Some(row)) => {
                            let mut values = Vec::new();
                            for i in 0..8 {
                                if let Ok(v) = row.get::<String>(i) {
                                    values.push(v);
                                } else if let Ok(v) = row.get::<i64>(i) {
                                    values.push(v.to_string());
                                } else if let Ok(v) = row.get::<f64>(i) {
                                    values.push(v.to_string());
                                } else if let Ok(v) = row.get::<Vec<u8>>(i) {
                                    values.push(format!("{} bytes", v.len()));
                                } else {
                                    break;
                                }
                            }
                            out.push(values);
                        }
                        Ok(None) => break,
                        Err(e) => {
                            println!("  FAIL {label}: {e}");
                            return Vec::new();
                        }
                    }
                }
                println!("  OK   {label}");
                for line in out.iter().take(8) {
                    println!("         {}", line.join(", "));
                }
                out
            }
            Err(e) => {
                println!("  FAIL {label}\n         {e}");
                Vec::new()
            }
        };
        out
    }

    async fn run_quiet(conn: &Connection, label: &str, sql: &str) -> bool {
        match conn.query(sql, ()).await {
            Ok(_) => {
                println!("  OK   {label}");
                true
            }
            Err(e) => {
                println!("  FAIL {label}\n         {e}");
                false
            }
        }
    }

    /// A Float1Bit literal from a 0/1 dimension list, via the engine's own
    /// constructor so the blob layout is the engine's rather than a guess.
    fn bit_literal(dims: &[i32]) -> String {
        let list = dims
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!("vector1bit(vector32('[{list}]'))")
    }

    /// A sparse-vector literal: one dimension per entry, 1.0 set / 0.0 clear.
    fn sparse_literal(bits: &[bool]) -> String {
        let body = bits
            .iter()
            .map(|b| if *b { "1.0" } else { "0.0" })
            .collect::<Vec<_>>()
            .join(",");
        format!("vector32_sparse('[{body}]')")
    }

    // -----------------------------------------------------------------------
    // 1. What is supported
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "manual probe: what turso vector support exists?"]
    async fn probe_vector_index() {
        let path = std::path::PathBuf::from("/dev/shm/intscrape-vecprobe.db");
        let _ = std::fs::remove_file(&path);
        // Matches production: `new_with_exit` enables the index-method flag.
        let db = Builder::new_local(path.to_str().unwrap())
            .experimental_index_method(true)
            .build()
            .await
            .expect("build");
        let conn = db.connect().expect("connect");
        let _should_exit = Arc::new(AtomicBool::new(false));

        println!("\nPROBE turso vector support");

        // --- Engine and constructors ---
        run(&conn, "version", "SELECT sqlite_version();").await;
        run(&conn, "vector32", "SELECT vector32('[1,2,3,4]');").await;
        run(
            &conn,
            "vector1bit hex",
            &format!("SELECT hex({});", bit_literal(&[1, 0, 1, 0])),
        )
        .await;

        // --- Is Float1Bit cosine really Hamming distance? ---
        // Two 4-dim vectors differing in one bit must be distance 1.
        run(
            &conn,
            "hamming 1100 vs 1000 (want 1)",
            &format!(
                "SELECT vector_distance_cos({}, {});",
                bit_literal(&[1, 1, 0, 0]),
                bit_literal(&[1, 0, 0, 0])
            ),
        )
        .await;
        run(
            &conn,
            "hamming 1010 vs 0101 (want 4)",
            &format!(
                "SELECT vector_distance_cos({}, {});",
                bit_literal(&[1, 0, 1, 0]),
                bit_literal(&[0, 1, 0, 1])
            ),
        )
        .await;

        // --- Can Float1Bit be indexed? ---
        run(&conn, "drop bit table", "DROP TABLE IF EXISTS B;").await;
        run(
            &conn,
            "create bit table",
            "CREATE TABLE B (id INTEGER PRIMARY KEY, h BLOB);",
        )
        .await;
        run(
            &conn,
            "index bit table",
            "CREATE INDEX b_idx ON B USING toy_vector_sparse_ivf (h);",
        )
        .await;
        run(
            &conn,
            "insert bit",
            &format!(
                "INSERT INTO B (id, h) VALUES (1, {});",
                bit_literal(&[1, 0, 1, 0])
            ),
        )
        .await;

        // --- Sparse: the type the index accepts ---
        run(&conn, "drop sparse", "DROP TABLE IF EXISTS S;").await;
        run(
            &conn,
            "create sparse",
            "CREATE TABLE S (id INTEGER PRIMARY KEY, h BLOB);",
        )
        .await;
        run(
            &conn,
            "index sparse",
            "CREATE INDEX s_idx ON S USING toy_vector_sparse_ivf (h);",
        )
        .await;
        for i in 1..=4i32 {
            let bits: Vec<bool> = (0..8).map(|j| (i + j) % 2 == 0).collect();
            run(
                &conn,
                "insert sparse",
                &format!(
                    "INSERT INTO S (id, h) VALUES ({i}, {});",
                    sparse_literal(&bits)
                ),
            )
            .await;
        }
        run(&conn, "count sparse", "SELECT count(*) FROM S;").await;

        // --- The pattern the index registers (from toy_vector_sparse_ivf.rs:326):
        //   SELECT vector_distance_jaccard(<col>, ?) as distance FROM <t>
        //   ORDER BY distance LIMIT ?
        // Alias must be `distance`; a positional parameter is expected.
        let q = "SELECT id, vector_distance_jaccard(h, ?) as distance FROM S  ORDER BY distance LIMIT 2;";
        run(
            &conn,
            "plan exact pattern",
            &format!("EXPLAIN QUERY PLAN {q}"),
        )
        .await;
        {
            let mut st = conn.prepare(q).await.expect("prepare");
            let mut rows = st
                .query(turso::params_from_iter(vec![turso::Value::from(
                    sparse_literal(&[true, false, true, false, true, false, true, false]),
                )]))
                .await
                .expect("query");
            println!("  OK   query with bound parameter");
            while let Ok(Some(row)) = rows.next().await {
                let id = row.get::<i64>(0).unwrap_or(-1);
                let d = row.get::<f64>(1).unwrap_or(f64::NAN);
                println!("         id={id} distance={d}");
            }
        }

        // --- For contrast: the same shape but with a scan-shaped alias ---
        run(&conn, "plan without LIMIT", "EXPLAIN QUERY PLAN SELECT id, vector_distance_jaccard(h, vector32_sparse('[1.0]')) as distance FROM S ORDER BY distance;").await;

        let _ = std::fs::remove_file(&path);
    }

    // -----------------------------------------------------------------------
    // 2. Recall against ground truth
    // -----------------------------------------------------------------------

    /// Ground-truth Jaccard distance in Rust. Computed independently of SQL so
    /// a planner or scoring mistake cannot make the index look correct.
    fn jaccard(a: &[bool], b: &[bool]) -> f64 {
        let mut inter = 0usize;
        let mut union = 0usize;
        for i in 0..a.len() {
            match (a[i], b[i]) {
                (true, true) => {
                    inter += 1;
                    union += 1;
                }
                (false, false) => {}
                _ => union += 1,
            }
        }
        if union == 0 {
            return 0.0;
        }
        1.0 - (inter as f64 / union as f64)
    }

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    #[tokio::test]
    #[ignore = "manual probe: toy_ivf recall vs scan_portion"]
    async fn probe_recall() {
        let rows: usize = env_usize("BENCH_RECALL_ROWS", 2_000);
        let dims: usize = env_usize("BENCH_RECALL_DIMS", 64);
        let queries: usize = env_usize("BENCH_RECALL_QUERIES", 20);
        let topk: i64 = env_usize("BENCH_RECALL_TOPK", 10) as i64;

        // Deterministic xorshift64 so runs are reproducible without rand.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        // Half the queries are corpus rows with a few bits flipped, so recall
        // has genuine near-duplicates to find rather than only random noise.
        let corpus: Vec<Vec<bool>> = (0..rows)
            .map(|_| (0..dims).map(|i| (next() >> (i % 64)) & 1 == 1).collect())
            .collect();

        let mut query_bits: Vec<Vec<bool>> = Vec::new();
        for q in 0..queries {
            if q % 2 == 0 {
                let mut v = corpus[q % corpus.len()].clone();
                for f in 0..(dims / 8).max(1) {
                    let idx = ((next() as usize) + f * 7) % dims;
                    v[idx] ^= true;
                }
                query_bits.push(v);
            } else {
                query_bits.push((0..dims).map(|i| (next() >> (i % 64)) & 1 == 1).collect());
            }
        }

        // Ground truth: sort the whole corpus by real Jaccard, keep top-k ids.
        let truth: Vec<Vec<i64>> = query_bits
            .iter()
            .map(|q| {
                let mut scored: Vec<(f64, i64)> = corpus
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (jaccard(q, v), i as i64 + 1))
                    .collect();
                scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));
                scored
                    .into_iter()
                    .take(topk as usize)
                    .map(|(_, id)| id)
                    .collect()
            })
            .collect();

        // Average set size, which is what decides whether Jaccard ordering can
        // stand in for Hamming at all.
        let density = corpus.iter().flat_map(|v| v.iter()).filter(|b| **b).count() as f64
            / (rows * dims) as f64;

        println!(
            "\nPROBE recall: {rows} rows x {dims} dims, {queries} queries, top {topk}, \
             density {density:.3}"
        );

        let path = std::path::PathBuf::from("/dev/shm/intscrape-recall.db");
        let _ = std::fs::remove_file(&path);
        let db = Builder::new_local(path.to_str().unwrap())
            .experimental_index_method(true)
            .build()
            .await
            .expect("build");
        let conn = db.connect().expect("connect");

        conn.execute_batch("CREATE TABLE R (id INTEGER PRIMARY KEY, h BLOB);")
            .await
            .expect("create");
        // A prepared INSERT whose returned row set is never drained reported
        // Ok on every insert while writing nothing -- which is exactly how the
        // first version of this probe concluded the index returned no rows.
        // `execute_batch` per row avoids that trap and is honest about failure.
        let mut inserted = 0usize;
        let mut failures = 0usize;
        for (i, v) in corpus.iter().enumerate() {
            let sql = format!(
                "INSERT INTO R (id, h) VALUES ({}, {});",
                i + 1,
                sparse_literal(v)
            );
            match conn.execute_batch(&sql).await {
                Ok(_) => {
                    inserted += 1;
                }
                Err(e) => {
                    // Report the first few and the count at failure, then stop:
                    // silently continuing is what hid this before.
                    if failures < 3 {
                        println!("  insert {i} failed after {inserted}: {e}");
                    }
                    failures += 1;
                    if failures > 3 {
                        break;
                    }
                }
            }
        }
        // Which ids are missing? A gap means the INSERT was rejected or
        // overwritten, not that the count is wrong.
        {
            let c = conn
                .query("SELECT min(id), max(id), count(*) FROM R;", ())
                .await;
            match c {
                Ok(mut r) => match r.next().await {
                    Ok(Some(row)) => {
                        let lo = row.get::<i64>(0).unwrap_or(-1);
                        let hi = row.get::<i64>(1).unwrap_or(-1);
                        let n = row.get::<i64>(2).unwrap_or(-1);
                        println!("  ids {lo}..={hi}, count {n}");
                    }
                    _ => println!("  id range unavailable"),
                },
                Err(e) => println!("  id range failed: {e}"),
            }
            match conn
                .query("SELECT count(*) FROM R WHERE id = 9000;", ())
                .await
            {
                Ok(mut r) => match r.next().await {
                    Ok(Some(row)) => {
                        println!("  id 9000 present: {}", row.get::<i64>(0).unwrap_or(-1))
                    }
                    _ => {}
                },
                Err(e) => println!("  id 9000 check failed: {e}"),
            }
        }
        println!("  seeded {inserted} of {rows} rows");
        if inserted < rows {
            println!("  (insertion stopped early; treating as a partial corpus)");
        }
        // Verify the rows actually landed. A prepared INSERT whose result
        // rows are never drained can report Ok while writing nothing.
        match conn.query("SELECT count(*) FROM R;", ()).await {
            Ok(mut r) => match r.next().await {
                Ok(Some(row)) => {
                    let n = row.get::<i64>(0).unwrap_or(-1);
                    println!("  stored rows: {n}");
                    assert!(n > 0, "inserts reported success but the table is empty");
                }
                _ => println!("  stored rows: unknown"),
            },
            Err(e) => println!("  count failed: {e}"),
        }

        let sql = "SELECT id, vector_distance_jaccard(h, ?) as distance FROM R  ORDER BY distance LIMIT ?;";

        // Sanity: does a plain scan on this table return the truth at all?
        {
            let q = truth[0].first().copied().unwrap_or(1);
            let target: Vec<bool> = corpus[(q - 1) as usize].clone();
            let mut st = conn.prepare(sql).await.expect("prepare");
            let mut rows_out = st
                .query(turso::params_from_iter(vec![
                    turso::Value::from(sparse_literal(&target)),
                    turso::Value::from(topk),
                ]))
                .await
                .expect("query");
            let mut got: Vec<i64> = Vec::new();
            while let Ok(Some(row)) = rows_out.next().await {
                if let Ok(id) = row.get::<i64>(0) {
                    got.push(id);
                }
            }
            println!("  [sanity] scan with NO index, self as query: {got:?}");
            println!("  [sanity] expected id {q} first, truth {:?}", truth[0]);
            println!(
                "  [sanity] literal text head: {}",
                &sparse_literal(&target)[..sparse_literal(&target).len().min(120)]
            );
            let lit_sql = format!(
                "SELECT id, vector_distance_jaccard(h, {}) as distance FROM R ORDER BY distance LIMIT ?;",
                sparse_literal(&target)
            );
            match conn
                .query(
                    &lit_sql,
                    turso::params_from_iter(vec![turso::Value::from(topk)]),
                )
                .await
            {
                Ok(mut r) => {
                    let mut ids = Vec::new();
                    while let Ok(Some(row)) = r.next().await {
                        if let Ok(id) = row.get::<i64>(0) {
                            ids.push(id);
                        }
                    }
                    println!("  [sanity] literal-arg query: {ids:?}");
                    // Does distance work on a stored column at all?
                    for sql in [
                        "SELECT count(*) FROM R;",
                        "SELECT vector_distance_jaccard(h, vector32_sparse('[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]')) FROM R WHERE id = 1;",
                        "SELECT vector_distance_jaccard(vector32_sparse('[1.0,0.0]'), vector32_sparse('[1.0,0.0]'));",
                        "SELECT length(h) FROM R WHERE id = 1;",
                    ] {
                        match conn.query(sql, ()).await {
                            Ok(mut r) => {
                                let mut vals = Vec::new();
                                while let Ok(Some(row)) = r.next().await {
                                    if let Ok(v) = row.get::<f64>(0) {
                                        vals.push(v.to_string());
                                    } else if let Ok(v) = row.get::<i64>(0) {
                                        vals.push(v.to_string());
                                    }
                                }
                                println!("  [probe] {sql:.70}... -> {:?}", vals);
                            }
                            Err(e) => println!("  [probe] {sql:.50}... failed: {e}"),
                        }
                    }
                }
                Err(e) => println!("  [sanity] literal-arg query failed: {e}"),
            }
        }

        for scan_portion in [1.0_f64, 0.5, 0.25, 0.1, 0.05] {
            conn.execute_batch("DROP INDEX IF EXISTS r_idx;")
                .await
                .expect("drop index");
            let ddl = format!(
                "CREATE INDEX r_idx ON R USING toy_vector_sparse_ivf (h)  WITH (scan_portion = {scan_portion});"
            );
            if let Err(e) = conn.execute_batch(&ddl).await {
                println!("  scan_portion={scan_portion}: CREATE INDEX failed: {e}");
                continue;
            }

            let mut recall_hits = 0usize;
            let mut recall_total = 0usize;
            let mut ms_total = 0.0f64;
            let mut ms_min = f64::MAX;
            let mut served = true;

            for (qi, q) in query_bits.iter().enumerate() {
                let started = Instant::now();
                // Literal, not bound: a bound vector parameter came back empty
                // for every row even though the same text as a literal returns
                // the right answer. The engine handles the literal form.
                let lit_sql = format!(
                    "SELECT id, vector_distance_jaccard(h, {}) as distance FROM R ORDER BY distance LIMIT {};",
                    sparse_literal(q),
                    topk
                );
                let mut rows_out = match conn.query(&lit_sql, ()).await {
                    Ok(r) => r,
                    Err(e) => {
                        println!("  scan_portion={scan_portion}: query failed: {e}");
                        served = false;
                        break;
                    }
                };
                let mut got: Vec<i64> = Vec::new();
                while let Ok(Some(row)) = rows_out.next().await {
                    if let Ok(id) = row.get::<i64>(0) {
                        got.push(id);
                    }
                }
                if qi == 0 {
                    println!("    [debug] returned ids: {got:?}  (truth {:?})", truth[0]);
                }
                let elapsed = started.elapsed().as_secs_f64() * 1_000.0;
                ms_total += elapsed;
                ms_min = ms_min.min(elapsed);

                // Recall = |got ∩ truth| / |truth|. Counting membership rather
                // than position, so a correct-but-reordered answer is not
                // penalised differently from a wrong one.
                let want: std::collections::HashSet<i64> = truth[qi].iter().copied().collect();
                let hits = got.iter().filter(|id| want.contains(id)).count();
                recall_hits += hits;
                recall_total += want.len();
            }

            if !served {
                continue;
            }
            let recall = recall_hits as f64 / recall_total.max(1) as f64;
            let per_query = ms_total / queries as f64;
            println!(
                "  scan_portion={scan_portion:<5} recall={recall:>6.3}  \
                 {per_query:>8.3} ms/query (best {ms_min:.3})"
            );
        }

        // Reference: a sequential scan is what the engine does without an
        // index, so this is the baseline the index has to beat.
        conn.execute_batch("DROP INDEX IF EXISTS r_idx;")
            .await
            .expect("drop");
        let mut base_ms = 0.0f64;
        for q in query_bits.iter().take(queries.min(5)) {
            let mut st = conn.prepare(sql).await.expect("prepare");
            let started = Instant::now();
            let _ = st
                .query(turso::params_from_iter(vec![
                    turso::Value::from(sparse_literal(q)),
                    turso::Value::from(topk),
                ]))
                .await
                .expect("query");
            base_ms += started.elapsed().as_secs_f64() * 1_000.0;
        }
        println!(
            "  no index (scan)    {base_ms:>8.3} ms for {} queries",
            queries.min(5)
        );

        let _ = std::fs::remove_file(&path);
    }
}
