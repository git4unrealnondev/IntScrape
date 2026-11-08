//! Tests for the `search` module.
//!
//! Extracted from `search.rs` so the production file stays focused on the
//! implementation; `use super::*` here is the `search` module itself.

use super::*;
use crate::db::turso::TursoDatabase;
use shared_types::{FileInternal, GenericNamespaceObj, Tag};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

async fn new_test_db() -> Arc<TursoDatabase> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("search.db");
    let should_exit = Arc::new(AtomicBool::new(false));
    TursoDatabase::new_with_exit(&db_path, should_exit).await
}

/// Builds a library with two namespaces, `files` files and a deterministic
/// spread of tag/file relationships, then returns
/// `(file_ids, tag_id -> its files)` so a test can compute the expected
/// answer independently of the SQL under test.
async fn seed(
    db: &TursoDatabase,
    files: u64,
    tags_per_ns: u64,
) -> (Vec<u64>, HashMap<u64, BTreeSet<u64>>) {
    let conn = db.connect().unwrap();
    let mut ns_set = HashSet::new();
    for name in ["alpha", "beta"] {
        ns_set.insert(GenericNamespaceObj {
            name: name.into(),
            description: None,
        });
    }
    db.namespace_ensure_set(&ns_set).await.unwrap();

    let storage_id = db
        .file_storage_location_get_or_create(&conn, "test_storage")
        .await
        .unwrap();

    let mut file_objs = Vec::new();
    for i in 0..files {
        file_objs.push(FileInternal {
            id: None,
            hash: format!("searchhash{i}"),
            extension: "jpg".into(),
            storage_id,
            size_bytes: Some(1),
        });
    }
    let inserted = db.file_add_bulk(&conn, &file_objs).await.unwrap();
    let file_ids: Vec<u64> = inserted.iter().map(|f| f.id.unwrap()).collect();

    // Tags spread over both namespaces.
    let mut tag_objs = HashSet::new();
    for ns in ["alpha", "beta"] {
        for t in 0..tags_per_ns {
            tag_objs.insert(Tag {
                name: format!("{ns}-tag-{t}"),
                namespace: GenericNamespaceObj {
                    name: ns.into(),
                    description: None,
                },
            });
        }
    }
    let tag_set = db.tag_add_bulk(&conn, &tag_objs).await.unwrap();
    let mut tag_ids: Vec<u64> = tag_set.iter().map(|t| t.id as u64).collect();
    tag_ids.sort_unstable();

    // Deterministic relationships: tag t covers files where
    // (file + t) % 3 != 0, so tags overlap heavily and rarities differ.
    let mut rels: HashSet<(u64, u64)> = HashSet::new();
    let mut truth: HashMap<u64, BTreeSet<u64>> = HashMap::new();
    for (t_index, &tag_id) in tag_ids.iter().enumerate() {
        for (f_index, &file_id) in file_ids.iter().enumerate() {
            if (f_index + t_index) % 3 != 0 {
                rels.insert((file_id, tag_id));
                truth.entry(tag_id).or_default().insert(file_id);
            }
        }
    }
    let deltas = db.relationships_bulk_add(&conn, &rels).await.unwrap();
    db.tag_counts_apply(&deltas, &HashMap::new()).await.unwrap();
    drop(conn);
    (file_ids, truth)
}

/// Reference implementation of the documented `SearchHolder` semantics:
/// positive holders intersect, NOT holders subtract their union, and with
/// no positive holder the driver is every file. Ordered newest-first.
///
/// A tag id with no `Tags` row is *skipped* rather than treated as
/// unsatisfiable, and a holder left with no resolvable tag stops
/// constraining the result. That quirk is inherited from the pre-rewrite
/// implementation, which dropped such a tag when it could not resolve the
/// namespace; it is pinned here so the rewrite is provably behaviour-
/// preserving. It is arguably wrong (a search for a tag that does not exist
/// should return nothing, not everything carrying the other tags) but
/// changing it is a separate decision, not an optimization.
fn expected(
    all_files: &[u64],
    truth: &HashMap<u64, BTreeSet<u64>>,
    searches: &[SearchHolder],
    limit: &Option<u64>,
) -> Vec<u64> {
    let empty = BTreeSet::new();
    let resolvable = |ids: &[u64]| -> Vec<u64> {
        ids.iter()
            .copied()
            .filter(|id| truth.contains_key(id))
            .collect()
    };
    let set_of = |ids: &[u64], union: bool| -> Option<BTreeSet<u64>> {
        let ids = resolvable(ids);
        if ids.is_empty() {
            return None;
        }
        if union {
            let mut out = BTreeSet::new();
            for id in ids {
                out.extend(truth.get(&id).unwrap_or(&empty).iter().copied());
            }
            Some(out)
        } else {
            // An AND group seeds from its first tag, then intersects the
            // rest — seeding from the empty set would make every
            // single-tag AND empty.
            let mut out = truth.get(&ids[0]).unwrap_or(&empty).clone();
            for id in &ids[1..] {
                out = out
                    .intersection(truth.get(id).unwrap_or(&empty))
                    .copied()
                    .collect();
            }
            Some(out)
        }
    };

    let mut positives: Vec<BTreeSet<u64>> = Vec::new();
    let mut exclusions: Vec<BTreeSet<u64>> = Vec::new();
    for holder in searches {
        let (ids, union) = match holder {
            SearchHolder::And(ids) => (ids, false),
            SearchHolder::Or(ids) => (ids, true),
            SearchHolder::Not(ids) => (ids, true),
        };
        let Some(set) = set_of(ids, union) else {
            continue;
        };
        match holder {
            SearchHolder::Not(_) => exclusions.push(set),
            _ => positives.push(set),
        }
    }

    let mut result: BTreeSet<u64> = if positives.is_empty() {
        all_files.iter().copied().collect()
    } else {
        let mut acc = positives[0].clone();
        for next in &positives[1..] {
            acc = acc.intersection(next).copied().collect();
        }
        acc
    };
    for exclusion in exclusions {
        result = result.difference(&exclusion).copied().collect();
    }

    let mut out: Vec<u64> = result.into_iter().collect();
    out.reverse();
    if let Some(limit) = limit {
        out.truncate(*limit as usize);
    }
    out
}

/// Differential test: the rewritten single-scan/`EXISTS` intersection and
/// the compound `INTERSECT` fallback must agree with the reference
/// semantics for every holder shape the search API can produce.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_matches_holder_semantics_across_shapes() {
    let db = new_test_db().await;
    let (file_ids, truth) = seed(&db, 40, 6).await;
    let conn = db.connect().unwrap();

    // A spread of rarities so the driver choice is actually exercised.
    let mut counts: Vec<(u64, usize)> = truth.iter().map(|(id, s)| (*id, s.len())).collect();
    counts.sort_by_key(|(_, n)| *n);
    let rare = counts[0].0;
    let mid = counts[counts.len() / 3].0;
    let common = counts[counts.len() - 1].0;
    let other = counts[1].0;

    let shapes: Vec<(Vec<SearchHolder>, Option<u64>)> = vec![
        // single tag
        (vec![SearchHolder::And(vec![common])], None),
        // two and three tags, which is the shape the rewrite targets
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::And(vec![mid]),
            ],
            None,
        ),
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::And(vec![mid]),
                SearchHolder::And(vec![rare]),
            ],
            None,
        ),
        // same three, reversed, so driver selection cannot matter
        (
            vec![
                SearchHolder::And(vec![rare]),
                SearchHolder::And(vec![mid]),
                SearchHolder::And(vec![common]),
            ],
            None,
        ),
        // duplicate tag
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::And(vec![common]),
            ],
            None,
        ),
        // OR group plus a plain tag
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::Or(vec![mid, other]),
            ],
            None,
        ),
        // OR group only: this takes the compound fallback
        (vec![SearchHolder::Or(vec![mid, other, rare])], None),
        // NOT subtracted from positives
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::And(vec![mid]),
                SearchHolder::Not(vec![rare]),
            ],
            None,
        ),
        // NOT only: driver is every file
        (vec![SearchHolder::Not(vec![common])], None),
        // NOT with several tags unions before subtracting
        (vec![SearchHolder::Not(vec![common, mid])], None),
        // positive OR + NOT together
        (
            vec![
                SearchHolder::Or(vec![common, mid]),
                SearchHolder::Not(vec![rare]),
            ],
            None,
        ),
        // limits
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::And(vec![mid]),
            ],
            Some(1),
        ),
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::And(vec![mid]),
                SearchHolder::And(vec![rare]),
            ],
            Some(5),
        ),
        (vec![SearchHolder::Not(vec![common])], Some(3)),
        // a tag with no relationships at all
        (
            vec![
                SearchHolder::And(vec![common]),
                SearchHolder::And(vec![999_999]),
            ],
            None,
        ),
    ];

    for (searches, limit) in shapes {
        let want = expected(&file_ids, &truth, &searches, &limit);
        let got = db
            .search_db_files(
                &conn,
                &SearchObj {
                    search_relate: None,
                    searches: searches.clone(),
                },
                &limit,
            &None,
                )
            .await
            .unwrap();
        assert_eq!(
            got, want,
            "search disagreed with reference semantics for {searches:?} limit={limit:?}"
        );
    }
    drop(conn);
    db.shutdown().await;
}

/// Cross-namespace intersection: the driving scan and each `EXISTS` probe
/// live in different `Relationship_{ns}` partitions, which is the case a
/// single-namespace-only implementation would get wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_tag_intersection_spans_namespaces() {
    let db = new_test_db().await;
    let (file_ids, truth) = seed(&db, 30, 4).await;
    let conn = db.connect().unwrap();

    let conn_ns = db.connect().unwrap();
    let mut rows = conn_ns
        .query(
            "SELECT id, namespace, count FROM Tags ORDER BY count DESC",
            (),
        )
        .await
        .unwrap();
    let mut per_ns: HashMap<u64, Vec<u64>> = HashMap::new();
    while let Some(row) = rows.next().await.unwrap() {
        let id: u64 = row.get(0).unwrap();
        let ns: u64 = row.get(1).unwrap();
        per_ns.entry(ns).or_default().push(id);
    }
    drop(rows);
    drop(conn_ns);

    // Pick one tag per namespace, then top up so the search has three tags
    // and the driver plus at least one probe live in different partitions.
    let mut namespaces: Vec<u64> = per_ns.keys().copied().collect();
    namespaces.sort_unstable();
    assert!(
        namespaces.len() >= 2,
        "seed must create at least two namespaces for this test to mean anything"
    );
    let mut picked: Vec<u64> = Vec::new();
    for ns in &namespaces {
        picked.push(per_ns[ns][0]);
        if picked.len() == 3 {
            break;
        }
    }
    // Only two namespaces exist, so the third tag shares one of them.
    while picked.len() < 3 {
        let ns = namespaces[picked.len() % namespaces.len()];
        let extra = per_ns[&ns]
            .iter()
            .find(|id| !picked.contains(id))
            .copied()
            .expect("namespace has spare tags");
        picked.push(extra);
    }
    let distinct_namespaces = namespaces
        .iter()
        .filter(|ns| per_ns[ns].iter().any(|id| picked.contains(id)))
        .count();
    assert!(
        distinct_namespaces >= 2,
        "test must span at least two namespaces, got {distinct_namespaces}"
    );

    let searches = vec![
        SearchHolder::And(vec![picked[0]]),
        SearchHolder::And(vec![picked[1]]),
        SearchHolder::And(vec![picked[2]]),
    ];
    let want = expected(&file_ids, &truth, &searches, &None);
    let got = db
        .search_db_files(
            &conn,
            &SearchObj {
                search_relate: None,
                searches: searches.clone(),
            },
            &None,
            &None,
        )
        .await
        .unwrap();
    assert_eq!(got, want, "cross-namespace intersection");
    drop(conn);
    db.shutdown().await;
}

// ---------------------------------------------------------------------
// Scale fixture: 1M files / 5M tags / 7 namespaces.
//
// These are manual benchmarks, not CI tests: seeding millions of rows takes
// minutes and gigabytes, which matches the existing `#[ignore]`d bench
// convention in processing.rs. Sizes are env-overridable so the fixture can
// be smoke-tested small before committing to a full run:
//
//   SCALE_FILES  default 1_000_000
//   SCALE_TAGS   default 5_000_000
//   SCALE_NS     default 7
//   SCALE_KEEP   set to keep the database for inspection
//
// The point of the fixture is the *rarity distribution*, not the row count.
// A real library is a long tail: a few tags covering a large slice of it, a
// middle band, and a huge number of tags carrying one or two files.
// `files_for` derives a tag's files as an arithmetic sequence, so the whole
// fixture is a pure function of the spec — no tag->files map is ever
// materialised, and the count written into `Tags.count` is exact by
// construction. That last part matters: `search_db_files` picks its driving
// scan from `Tags.count`, so a fixture with wrong counts would quietly
// benchmark the wrong plan.
// ---------------------------------------------------------------------

struct ScaleSpec {
    files: u64,
    tags: u64,
    namespaces: u64,
    hub_tags: u64,
    common_tags: u64,
    mid_tags: u64,
}

impl ScaleSpec {
    fn from_env() -> Self {
        let env = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let files = env("SCALE_FILES", 1_000_000);
        let tags = env("SCALE_TAGS", 5_000_000);
        let namespaces = env("SCALE_NS", 7).max(1);
        // Keep the named bands proportional so a scaled-down run has the same
        // shape as the full one.
        let hub_tags = (tags / 50_000).max(1);
        let common_tags = (tags / 1_000).max(1);
        let mid_tags = (tags / 50).max(1);
        assert!(
            hub_tags + common_tags + mid_tags < tags,
            "SCALE_TAGS={tags} is too small to hold the named tiers"
        );
        Self {
            files,
            tags,
            namespaces,
            hub_tags,
            common_tags,
            mid_tags,
        }
    }

    /// Tag ids are 1-based, matching `Tags.id`.
    fn rest_start(&self) -> u64 {
        1 + self.hub_tags + self.common_tags + self.mid_tags
    }

    /// The files carrying a tag, as `(stride, per_tag, offset)`. Within a
    /// band the stride is the band's size, so the band's tags tile the
    /// library with no file carrying two of them and every tag gets exactly
    /// `per_tag` files.
    fn files_for(&self, tag_id: u64) -> (u64, u64, u64) {
        if tag_id <= self.hub_tags {
            (self.hub_tags, self.files / self.hub_tags, tag_id - 1)
        } else if tag_id <= self.hub_tags + self.common_tags {
            let per_tag = (self.files / self.common_tags).min(200).max(1);
            (self.common_tags, per_tag, tag_id - self.hub_tags - 1)
        } else if tag_id < self.rest_start() {
            let per_tag = (self.files / self.mid_tags).min(10).max(1);
            (
                self.mid_tags,
                per_tag,
                tag_id - self.hub_tags - self.common_tags - 1,
            )
        } else {
            // The tail: one file each, round-robin over the library.
            (self.files, 1, (tag_id - self.rest_start()) % self.files)
        }
    }

    fn count_for(&self, tag_id: u64) -> u64 {
        self.files_for(tag_id).1
    }

    fn tier(&self, tag_id: u64) -> &'static str {
        if tag_id <= self.hub_tags {
            "hub"
        } else if tag_id <= self.hub_tags + self.common_tags {
            "common"
        } else if tag_id < self.rest_start() {
            "mid"
        } else {
            "tail"
        }
    }

    /// Namespaces are assigned round-robin by tag id, so every band is
    /// spread across all partitions and a tag's files always live in one
    /// `Relationship_{ns}` table.
    fn namespace_index(&self, tag_id: u64) -> usize {
        ((tag_id - 1) % self.namespaces) as usize
    }

    fn table(&self, tag_id: u64, namespace_ids: &[u64]) -> String {
        format!(
            "Relationship_{}",
            namespace_ids[self.namespace_index(tag_id)]
        )
    }
}

/// Seeds the fixture, logging per-stage timings. Returns the namespace ids
/// in creation order so the relationship writes can target the right
/// partitions.
///
/// Everything is written with bounded multi-row `INSERT`s in id order so
/// each b-tree is extended at its right edge: `Tags` by ascending name, and
/// each `Relationship_{ns}` by ascending `(tag_id, file_id)`, which is that
/// table's primary key. No per-row state accumulates, so peak memory is one
/// batch no matter how large the fixture is.
async fn scale_seed(db: &TursoDatabase, spec: &ScaleSpec) -> (Vec<u64>, f64) {
    let conn = db.connect().unwrap();

    let ns_start = Instant::now();
    let mut ns_set = HashSet::new();
    for i in 1..=spec.namespaces {
        ns_set.insert(shared_types::GenericNamespaceObj {
            name: format!("scale-ns-{i}"),
            description: None,
        });
    }
    let ns_map = db.namespace_ensure_set(&ns_set).await.unwrap();
    let mut namespace_ids: Vec<u64> = ns_set
        .iter()
        .map(|ns| *ns_map.get(ns).expect("every namespace was created"))
        .collect();
    namespace_ids.sort_unstable();
    let ns_s = ns_start.elapsed().as_secs_f64();

    let conn = db.connect().unwrap();
    let storage_id = db
        .file_storage_location_get_or_create(&conn, "scale_storage")
        .await
        .unwrap();

    let files_start = Instant::now();
    let mut done = 0u64;
    while done < spec.files {
        let batch = SQL_CHUNK_SIZE.min((spec.files - done) as usize) as u64;
        let mut sql = String::from(
            "INSERT OR IGNORE INTO File (hash, extension, storage_id, size_bytes) VALUES ",
        );
        let mut params: Vec<Value> = Vec::with_capacity((batch * 4) as usize);
        for i in 0..batch {
            if i > 0 {
                sql.push(',');
            }
            let b = (i * 4) as usize;
            sql.push_str(&format!("(?{}, ?{}, ?{}, ?{})", b + 1, b + 2, b + 3, b + 4));
            params.push(Value::from(format!("scalehash{:09}", done + i)));
            params.push(Value::from("bin"));
            params.push(Value::from(storage_id as i64));
            params.push(Value::from(1i64));
        }
        conn.execute(&sql, turso::params_from_iter(params))
            .await
            .unwrap();
        done += batch;
    }
    let files_s = files_start.elapsed().as_secs_f64();

    // Tags carry their exact final count, set at insert rather than
    // recounted afterwards. That is what keeps the fixture honest about
    // driver selection.
    let tags_start = Instant::now();
    let mut done = 0u64;
    while done < spec.tags {
        let batch = SQL_CHUNK_SIZE.min((spec.tags - done) as usize) as u64;
        let mut sql =
            String::from("INSERT OR IGNORE INTO Tags (name, namespace, count) VALUES ");
        let mut params: Vec<Value> = Vec::with_capacity((batch * 3) as usize);
        for i in 0..batch {
            if i > 0 {
                sql.push(',');
            }
            let tag_id = done + i + 1;
            let b = (i * 3) as usize;
            sql.push_str(&format!("(?{}, ?{}, ?{})", b + 1, b + 2, b + 3));
            // Zero-padded so ascending id order is also ascending name
            // order, keeping UNIQUE(name, namespace) append-only.
            params.push(Value::from(format!("scaletag{tag_id:09}")));
            params.push(Value::from(
                namespace_ids[spec.namespace_index(tag_id)] as i64,
            ));
            params.push(Value::from(spec.count_for(tag_id) as i64));
        }
        conn.execute(&sql, turso::params_from_iter(params))
            .await
            .unwrap();
        done += batch;
    }
    let tags_s = tags_start.elapsed().as_secs_f64();

    // Relationships, walked in tag-id order so each partition's rows arrive
    // in ascending (tag_id, file_id) — its primary key. One batch per
    // partition in flight at a time.
    let rel_start = Instant::now();
    let mut pending: Vec<Vec<Value>> = vec![Vec::new(); namespace_ids.len()];
    let mut relationships = 0u64;
    for tag_id in 1..=spec.tags {
        let ns = spec.namespace_index(tag_id);
        let (stride, per_tag, offset) = spec.files_for(tag_id);
        for k in 0..per_tag {
            let file_index = offset + k * stride;
            if file_index >= spec.files {
                break;
            }
            // File ids start at 1, so shift the 0-based file index.
            pending[ns].push(Value::from((file_index + 1) as i64));
            pending[ns].push(Value::from(tag_id as i64));
            relationships += 1;
        }
        if pending[ns].len() >= SQL_CHUNK_SIZE * 2 {
            scale_flush_rel(&conn, &mut pending[ns], namespace_ids[ns]).await;
        }
    }
    for ns in 0..pending.len() {
        scale_flush_rel(&conn, &mut pending[ns], namespace_ids[ns]).await;
    }
    let rel_s = rel_start.elapsed().as_secs_f64();
    eprintln!(
        "SCALE seeded {} files / {} tags / {} relationships / {} namespaces \
         in {ns_s:.1}s + {files_s:.1}s + {tags_s:.1}s + {rel_s:.1}s",
        spec.files, spec.tags, relationships, spec.namespaces
    );

    // The popular shadow plus its ngram FTS index, so name-based search is
    // exercisable too. Only `count >= 5` tags are mirrored, a small
    // fraction of a multi-million-row Tags table.
    let shadow_start = Instant::now();
    conn.execute_batch(
        "DROP TABLE IF EXISTS Tags_Popular;
         CREATE TABLE Tags_Popular (tag_id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO Tags_Popular (tag_id, name)
         SELECT id, name FROM Tags WHERE count >= 5;",
        (),
    )
    .await
    .unwrap();
    conn.execute_batch(
        "CREATE INDEX idx_tags_fts ON Tags_Popular USING fts (name)
             WITH (tokenizer='ngram', min_gram=2, max_gram=3);
         OPTIMIZE INDEX idx_tags_fts;",
    )
    .await
    .unwrap();
    let shadow_s = shadow_start.elapsed().as_secs_f64();
    eprintln!("SCALE shadow + FTS built in {shadow_s:.1}s");

    drop(conn);
    (namespace_ids, files_s + tags_s + rel_s + shadow_s)
}

async fn scale_flush_rel(conn: &Connection, rows: &mut Vec<Value>, namespace_id: u64) {
    if rows.is_empty() {
        return;
    }
    let mut sql =
        format!("INSERT OR IGNORE INTO Relationship_{namespace_id} (file_id, tag_id) VALUES ");
    for i in 0..rows.len() / 2 {
        if i > 0 {
            sql.push(',');
        }
        let b = i * 2;
        sql.push_str(&format!("(?{}, ?{})", b + 1, b + 2));
    }
    let params = std::mem::take(rows);
    conn.execute(&sql, turso::params_from_iter(params))
        .await
        .unwrap();
}

fn scale_db_path(label: &str) -> std::path::PathBuf {
    let dir = if std::path::Path::new("/dev/shm").exists() {
        std::path::PathBuf::from("/dev/shm")
    } else {
        std::env::temp_dir()
    };
    dir.join(format!(
        "intscrape-search-{label}-{}.db",
        std::process::id()
    ))
}

/// Removes a database and turso's sidecars. The MVCC journal keeps a `-log`
/// file next to the database; leaving it behind makes the next open fail
/// with "MVCC logical log file exists ... but database header indicates WAL
/// mode".
fn scale_remove(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    for suffix in ["-log", "-wal", "-shm"] {
        let mut side = path.as_os_str().to_os_string();
        side.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(side));
    }
}

/// `count` evenly-spread tag ids from a band.
fn scale_pick(spec: &ScaleSpec, tier: &str, count: usize) -> Vec<u64> {
    let (start, end) = match tier {
        "hub" => (1, spec.hub_tags),
        "common" => (spec.hub_tags + 1, spec.hub_tags + spec.common_tags),
        "mid" => (spec.hub_tags + spec.common_tags + 1, spec.rest_start() - 1),
        _ => (spec.rest_start(), spec.tags),
    };
    let span = (end - start + 1).max(1);
    (0..count)
        .map(|i| start + (i as u64 * span) / count as u64)
        .collect()
}

/// The files carrying every one of `tag_ids`, recomputed straight from the
/// fixture generator rather than queried.
///
/// This is the oracle the scale benchmark checks against. It shares no code
/// path with the SQL under test, and its cost is bounded by the sum of the
/// tags' file counts, so it stays affordable even for a hub tag.
fn scale_expected_files(spec: &ScaleSpec, tag_ids: &[u64]) -> BTreeSet<u64> {
    let mut out: Option<BTreeSet<u64>> = None;
    for tag_id in tag_ids {
        let (stride, per_tag, offset) = spec.files_for(*tag_id);
        let mut files = BTreeSet::new();
        for k in 0..per_tag {
            let file_index = offset + k * stride;
            if file_index >= spec.files {
                break;
            }
            // File ids are 1-based.
            files.insert(file_index + 1);
        }
        out = Some(match out {
            None => files,
            Some(acc) => acc.intersection(&files).copied().collect(),
        });
    }
    out.unwrap_or_default()
}

/// Times `db.search_db_files` over a batch of tag sets and prints
/// min/median/p99 plus the mean result size.
async fn scale_time_searches(
    db: &TursoDatabase,
    conn: &Connection,
    label: &str,
    groups: &[Vec<u64>],
    limit: Option<u64>,
) {
    let iters = 20;
    let mut samples: Vec<f64> = Vec::with_capacity(groups.len() * iters);
    let mut rows_total = 0usize;
    for tags in groups {
        let searches: Vec<SearchHolder> =
            tags.iter().map(|id| SearchHolder::And(vec![*id])).collect();
        let obj = || SearchObj {
            search_relate: None,
            searches: searches.clone(),
        };
        // Warm once so the first sample is not paying for a cold b-tree.
        let _ = db.search_db_files(conn, &obj(), &limit, &None).await.unwrap();
        for _ in 0..iters {
            let started = Instant::now();
            let got = db.search_db_files(conn, &obj(), &limit, &None).await.unwrap();
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
            rows_total += got.len();
        }
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pick = |q: f64| samples[((samples.len() as f64 - 1.0) * q) as usize];
    println!(
        "  {label:30} min={:>8.3}ms median={:>8.3}ms p99={:>8.3}ms mean_rows={}",
        samples[0],
        pick(0.5),
        pick(0.99),
        rows_total / (groups.len() * iters)
    );
}

/// Asserts the plan for a search stays a bounded index scan plus point
/// probes.
///
/// A `TEMP B-TREE` would mean the statement fell back to materialising and
/// sort-merging whole posting lists, which is exactly the cost the driving
/// scan exists to avoid — and it would scale with the *sum* of the tags'
/// file counts rather than the rarest one.
async fn assert_plan_index_only(
    db: &TursoDatabase,
    conn: &Connection,
    searches: Vec<SearchHolder>,
    what: &str,
) -> String {
    let sql = db
        .build_search_sql(
            conn,
            &SearchObj {
                search_relate: None,
                searches,
            },
            &None,
            &None,
        )
        .await
        .unwrap()
        .expect("search should produce a statement");
    let mut rows = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql}"), ())
        .await
        .unwrap();
    let mut steps: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        steps.push(row.get::<String>(3).unwrap());
    }
    let joined = steps.join(" | ");
    assert!(
        !joined.contains("TEMP B-TREE"),
        "{what}: plan must not build a temp B-tree (that is the INTERSECT \
         fallback this rewrite removed): {joined}"
    );
    assert!(
        !joined.contains("SCAN relationship"),
        "{what}: plan must not full-scan a relationship partition: {joined}"
    );
    assert!(
        !joined.contains("USE SORTER"),
        "{what}: ordering must come from the (tag_id, file_id) primary key \
         rather than a sorter — an outer ORDER BY silently reintroduced one, \
         costing 3.16 ms vs 0.03 ms on a 20k-row tag: {joined}"
    );
    sql
}

/// Full-scale search benchmark. Checks that a three-tag AND stays fast and
/// index-only at 1M files / 5M tags / 7 namespaces, that the driving scan
/// is the rarest tag, and that the results are actually correct — the last
/// by brute-forcing a bounded slice of the library independently of the SQL
/// under test.
#[test]
#[ignore = "manual benchmark: search at 1M files / 5M tags / 7 namespaces"]
fn search_million_file_scale_bench() {
    let spec = ScaleSpec::from_env();
    eprintln!(
        "SCALE spec: {} files, {} tags, {} namespaces \
         (hub={} common={} mid={} tail={})",
        spec.files,
        spec.tags,
        spec.namespaces,
        spec.hub_tags,
        spec.common_tags,
        spec.mid_tags,
        spec.tags - spec.rest_start() + 1
    );
    let db_path = scale_db_path("scale");
    scale_remove(&db_path);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(async {
        let should_exit = Arc::new(AtomicBool::new(false));
        TursoDatabase::new_with_exit(&db_path, should_exit).await
    });
    let (_namespace_ids, load_s) = rt.block_on(scale_seed(&db, &spec));
    eprintln!("SCALE load total {load_s:.1}s");

    rt.block_on(async {
        let conn = db.connect().unwrap();

        // Latency across rarity bands. The point of the driving scan is that
        // a triple of very common tags is bounded by the rarest of the
        // three, so the hub band should not blow up the way an INTERSECT
        // chain over the same tags did.
        println!("\nSCALE search latency by rarity band:");
        for tier in ["tail", "mid", "common", "hub"] {
            let ids = scale_pick(&spec, tier, 9);
            let groups: Vec<Vec<u64>> = (0..ids.len() - 2)
                .map(|i| vec![ids[i], ids[i + 1], ids[i + 2]])
                .collect();
            let counts: Vec<u64> = groups[0].iter().map(|id| spec.count_for(*id)).collect();
            let label = format!("3x {tier} (files {})", counts.iter().min().unwrap());
            scale_time_searches(&db, &conn, &label, &groups, None).await;
        }

        // A mixed triple, where the driver must be the tail tag even though
        // the other two are hubs.
        let hub = scale_pick(&spec, "hub", 2);
        let tail = scale_pick(&spec, "tail", 1);
        scale_time_searches(
            &db,
            &conn,
            "hub+hub+tail (driver=tail)",
            &[vec![hub[0], hub[1], tail[0]]],
            None,
        )
        .await;
        scale_time_searches(&db, &conn, "1x hub", &[vec![hub[0]]], None).await;
        scale_time_searches(&db, &conn, "1x tail", &[vec![tail[0]]], None).await;

        // `LIMIT` is the shape a UI actually issues ("newest N matching
        // files"), and it is where the in-place `ORDER BY` earns its keep:
        // with the order coming from the (tag_id, file_id) primary key the
        // scan can stop as soon as N rows are found, instead of sorting the
        // whole posting list first.
        let common = scale_pick(&spec, "common", 1);
        let mid = scale_pick(&spec, "mid", 1);
        println!("\nSCALE search latency, LIMIT 50 (early termination):");
        for (label, group) in [
            ("1x hub, LIMIT 50", vec![hub[0]]),
            (
                "3x hub, LIMIT 50",
                vec![hub[0], hub[1 % hub.len()], hub[1 % hub.len()]],
            ),
            ("3x common, LIMIT 50", vec![common[0], mid[0], tail[0]]),
            (
                "hub+hub+tail, LIMIT 50",
                vec![hub[0], hub[1 % hub.len()], tail[0]],
            ),
        ] {
            scale_time_searches(&db, &conn, label, &[group], Some(50)).await;
        }

        // The driver really is the rarest tag.
        let sql = assert_plan_index_only(
            &db,
            &conn,
            vec![
                SearchHolder::And(vec![hub[0]]),
                SearchHolder::And(vec![hub[1]]),
                SearchHolder::And(vec![tail[0]]),
            ],
            "hub+hub+tail",
        )
        .await;
        assert!(
            sql.contains(&format!("d.tag_id = {}", tail[0])),
            "the driving scan must be the rarest tag ({}), got: {sql}",
            tail[0]
        );

        // Plans for the other holder shapes stay index-only too.
        let common = scale_pick(&spec, "common", 1);
        let mid = scale_pick(&spec, "mid", 1);
        for (what, searches) in [
            ("single tag", vec![SearchHolder::And(vec![tail[0]])]),
            (
                "two tags",
                vec![
                    SearchHolder::And(vec![common[0]]),
                    SearchHolder::And(vec![mid[0]]),
                ],
            ),
            (
                "OR group",
                vec![
                    SearchHolder::And(vec![common[0]]),
                    SearchHolder::Or(vec![mid[0], tail[0]]),
                ],
            ),
            (
                "NOT group",
                vec![
                    SearchHolder::And(vec![common[0]]),
                    SearchHolder::Not(vec![mid[0], tail[0]]),
                ],
            ),
        ] {
            assert_plan_index_only(&db, &conn, searches, what).await;
        }

        // Correctness at scale, checked against an oracle that never
        // touches SQL: the fixture is a pure function of the spec, so the
        // files carrying any tag set can be recomputed from the generator
        // itself. This is independent of the statement under test in a way a
        // second query would not be. The walk is bounded by the sum of the
        // tags' counts (here 10k + 200 + 1), so it stays cheap at 1M files.
        let cases: Vec<(&str, Vec<u64>)> = vec![
            ("hub + common (rich result)", vec![hub[0], common[0]]),
            ("hub + common + tail", vec![hub[0], common[0], tail[0]]),
            (
                "hub + hub + common (empty)",
                vec![hub[0], hub[1 % hub.len()], common[0]],
            ),
            ("hub + common + mid", vec![hub[0], common[0], mid[0]]),
            ("single hub", vec![hub[0]]),
            ("single tail", vec![tail[0]]),
        ];
        for (what, probes) in cases {
            let expected = scale_expected_files(&spec, &probes);
            let got = db
                .search_db_files(
                    &conn,
                    &SearchObj {
                        search_relate: None,
                        searches: probes
                            .iter()
                            .map(|id| SearchHolder::And(vec![*id]))
                            .collect(),
                    },
                    &None,
                &None,
                    )
                .await
                .unwrap();
            let got_set: BTreeSet<u64> = got.iter().copied().collect();
            assert_eq!(
                got_set, expected,
                "{what}: search must match the fixture oracle exactly"
            );
            println!(
                "  correctness {what:30} {} files, exact match",
                expected.len()
            );
        }

        db.shutdown().await;
    });

    if std::env::var("SCALE_KEEP").is_err() {
        scale_remove(&db_path);
    } else {
        eprintln!("SCALE keeping {}", db_path.display());
    }
}

/// Smoke test for the scale fixture itself, at a size that runs in CI.
///
/// The million-row bench above is `#[ignore]`d, so nothing would otherwise
/// check that the fixture generator still produces the distribution the
/// benchmark depends on. This runs the same generator small and asserts the
/// properties the plan assertions rely on: exact `Tags.count` values, a
/// genuine long tail, every tag resolvable in exactly one partition, and
/// agreement between the search and a brute-force check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scale_fixture_is_well_formed_at_small_size() {
    let spec = ScaleSpec {
        files: 2_000,
        tags: 5_000,
        namespaces: 7,
        hub_tags: 2,
        common_tags: 20,
        mid_tags: 200,
    };
    let db = new_test_db().await;
    let (namespace_ids, _) = scale_seed(&db, &spec).await;
    assert_eq!(namespace_ids.len(), 7);
    let conn = db.connect().unwrap();

    // `Tags.count` must equal the number of relationship rows actually
    // written, because the driver scan is chosen from it.
    for tag_id in [1, 2, 3, 25, 250, 300, 2_500, 5_000] {
        let expected = spec.count_for(tag_id);
        let table = spec.table(tag_id, &namespace_ids);
        let mut rows = conn
            .query(
                &format!("SELECT COUNT(*) FROM {table} WHERE tag_id = ?1"),
                (tag_id as i64,),
            )
            .await
            .unwrap();
        let actual: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(
            actual,
            expected as i64,
            "tag {tag_id} ({}) count column must match its stored rows",
            spec.tier(tag_id)
        );
    }

    // A real long tail: the rarest tags must be dramatically smaller than
    // the hubs, otherwise the driver scan has nothing to win.
    let hub_count = spec.count_for(1);
    let tail_count = spec.count_for(spec.rest_start());
    assert!(
        hub_count >= 100 * tail_count,
        "fixture must have a long tail: hub={hub_count} tail={tail_count}"
    );

    // Every tag lives in exactly one partition, and the round-robin spread
    // really does use all of them.
    let mut used = std::collections::BTreeSet::new();
    for tag_id in 1..=spec.tags {
        used.insert(namespace_ids[spec.namespace_index(tag_id)]);
    }
    assert_eq!(used.len(), 7, "all namespaces must be populated");

    // The driving scan must be the *rarest* tag, not merely any tag. That
    // is a performance property rather than a correctness one -- results are
    // identical either way -- so it is asserted here as well as in the
    // full-scale bench, to catch a regression without the 1M-row run.
    let rare_tag = spec.rest_start();
    let fat_tag = 1u64; // a hub: far more files
    assert!(
        spec.count_for(rare_tag) < spec.count_for(fat_tag),
        "fixture sanity: the tail tag must be rarer than the hub"
    );
    let driver_sql = db
        .build_search_sql(
            &conn,
            &SearchObj {
                search_relate: None,
                searches: vec![
                    SearchHolder::And(vec![fat_tag]),
                    SearchHolder::And(vec![rare_tag]),
                    SearchHolder::And(vec![rare_tag + 1]),
                ],
            },
            &None,
            &None,
        )
        .await
        .unwrap()
        .expect("search should produce a statement");
    assert!(
        driver_sql.contains(&format!("d.tag_id = {rare_tag}")),
        "the driving scan must be the rarest tag ({rare_tag}), got: {driver_sql}"
    );

    // Plans must stay index-only, and ordering must come from the primary
    // key rather than a sorter.
    for (what, searches) in [
        (
            "two tags",
            vec![
                SearchHolder::And(vec![fat_tag]),
                SearchHolder::And(vec![rare_tag]),
            ],
        ),
        (
            "OR group",
            vec![
                SearchHolder::And(vec![fat_tag]),
                SearchHolder::Or(vec![rare_tag, rare_tag + 1]),
            ],
        ),
        (
            "NOT group",
            vec![
                SearchHolder::And(vec![fat_tag]),
                SearchHolder::Not(vec![rare_tag]),
            ],
        ),
    ] {
        let sql = db
            .build_search_sql(
                &conn,
                &SearchObj {
                    search_relate: None,
                    searches,
                },
                &None,
            &None,                )
            .await
            .unwrap()
            .expect("search should produce a statement");
        let mut rows = conn
            .query(&format!("EXPLAIN QUERY PLAN {sql}"), ())
            .await
            .unwrap();
        let mut steps: Vec<String> = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            steps.push(row.get::<String>(3).unwrap());
        }
        let joined = steps.join(" | ");
        assert!(
            !joined.contains("TEMP B-TREE"),
            "{what}: plan must not build a temp B-tree: {joined}"
        );
        assert!(
            !joined.contains("SCAN relationship"),
            "{what}: plan must not full-scan a partition: {joined}"
        );
        assert!(
            !joined.contains("USE SORTER"),
            "{what}: ordering must come from the (tag_id, file_id) primary \
             key, not a sorter: {joined}"
        );
    }

    // And the search still agrees with brute force over a window.
    let probes = [1u64, spec.rest_start(), spec.rest_start() + 1];
    let file_hi: i64 = 1_500;
    let mut brute: Vec<u64> = Vec::new();
    for file_id in 1..=file_hi {
        let mut all = true;
        for tag_id in probes {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT 1 FROM {} WHERE tag_id = ?1 AND file_id = ?2 LIMIT 1",
                        spec.table(tag_id, &namespace_ids)
                    ),
                    (tag_id as i64, file_id),
                )
                .await
                .unwrap();
            if rows.next().await.unwrap().is_none() {
                all = false;
                break;
            }
        }
        if all {
            brute.push(file_id as u64);
        }
    }
    brute.reverse();
    let got = db
        .search_db_files(
            &conn,
            &SearchObj {
                search_relate: None,
                searches: probes
                    .iter()
                    .map(|id| SearchHolder::And(vec![*id]))
                    .collect(),
            },
            &None,
        &None,
            )
        .await
        .unwrap();
    let in_window: Vec<u64> = got
        .iter()
        .copied()
        .filter(|id| *id <= file_hi as u64)
        .collect();
    assert_eq!(in_window, brute, "search must match brute force");
    drop(conn);
    db.shutdown().await;
}

/// Results must come back newest-first, and `LIMIT` must take the newest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn results_are_newest_first_and_limit_truncates() {
    let db = new_test_db().await;
    let (file_ids, truth) = seed(&db, 25, 3).await;
    let conn = db.connect().unwrap();

    let mut counts: Vec<(u64, usize)> = truth.iter().map(|(id, s)| (*id, s.len())).collect();
    counts.sort_by_key(|(_, n)| *n);
    let tag = counts[counts.len() - 1].0;

    let searches = vec![SearchHolder::And(vec![tag])];
    let all = db
        .search_db_files(
            &conn,
            &SearchObj {
                search_relate: None,
                searches: searches.clone(),
            },
            &None,
            &None,
        )
        .await
        .unwrap();
    let mut descending = all.clone();
    descending.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(all, descending, "results must be ordered by file_id DESC");
    assert!(all.len() > 2, "need a multi-row result to be meaningful");

    let limited = db
        .search_db_files(
            &conn,
            &SearchObj {
                search_relate: None,
                searches,
            },
            &Some(2),
            &None,
        )
        .await
        .unwrap();
    assert_eq!(limited, all[..2].to_vec(), "LIMIT takes the newest rows");
    drop(conn);
    db.shutdown().await;
}

/// Server-side paging must slice the same newest-first sequence the
/// unlimited search produces: `LIMIT n OFFSET m` returns rows
/// [m .. m + n] of that sequence, and overrunning the result yields an
/// empty page. This is what keeps the webui from pulling 500k ids to show
/// page two.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_pages_the_newest_first_result() {
    let db = new_test_db().await;
    let (_file_ids, truth) = seed(&db, 30, 3).await;
    let conn = db.connect().unwrap();

    let mut counts: Vec<(u64, usize)> = truth.iter().map(|(id, s)| (*id, s.len())).collect();
    counts.sort_by_key(|(_, n)| *n);
    let tag = counts[counts.len() - 1].0;

    let searches = vec![SearchHolder::And(vec![tag])];
    let all: Vec<u64> = db
        .search_db_files(&conn, &tags_obj(searches.clone()), &None, &None)
        .await
        .unwrap();
    assert!(all.len() > 10, "need a sizeable tag for paging to matter");

    for (limit, offset) in [(3u64, 0u64), (3, 3), (3, 7), (2, all.len() as u64 - 1), (5, all.len() as u64)] {
        let paged = db
            .search_db_files(&conn, &tags_obj(searches.clone()), &Some(limit), &Some(offset))
            .await
            .unwrap();
        let window: Vec<u64> = all
            .iter()
            .skip(offset as usize)
            .take(limit as usize)
            .copied()
            .collect();
        assert_eq!(
            paged, window,
            "LIMIT {limit} OFFSET {offset} must be the matching slice of the full result"
        );
    }

    // Count agreement: the aggregate sees exactly what the scan yields.
    let counted = db.search_db_files_count(&conn, &tags_obj(searches)).await.unwrap();
    assert_eq!(
        counted,
        all.len() as u64,
        "COUNT over the driving scan must equal the result size"
    );

    drop(conn);
    db.shutdown().await;
}

fn tags_obj(searches: Vec<SearchHolder>) -> SearchObj {
    SearchObj {
        search_relate: None,
        searches,
    }
}
