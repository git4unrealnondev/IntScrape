#[path = "../src/db/tag_search.rs"]
mod tag_search;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rusqlite::Connection;
use tag_search::{TagSearchCache, tag_entry};

const REAL_QUERIES: &[&str] = &[
    "female",
    "anthro",
    "breasts",
    "clothing",
    "penetration",
    "fem",
    "white hair",
];

fn real_entries() -> Vec<tag_search::TagEntry> {
    let connection = Connection::open("main.db").expect("main.db is required for this benchmark");
    let mut statement = connection
        .prepare(
            "SELECT t.id, t.name, t.count
             FROM Tags t
             JOIN Namespace n ON n.id = t.namespace
             WHERE n.name IN ('E621_General', 'E6AI_General')
             ORDER BY t.count DESC, t.id",
        )
        .unwrap();
    let entries = statement
        .query_map([], |row| {
            Ok(tag_entry(
                row.get(0)?,
                row.get::<_, String>(1)?.as_str(),
                row.get(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(!entries.is_empty(), "main.db has no General Namespace tags");
    entries
}

fn entries(count: usize, real: &[tag_search::TagEntry]) -> Vec<tag_search::TagEntry> {
    (0..count)
        .map(|id| {
            if id < real.len() {
                return real[id].clone();
            }
            let name = if id % 100 == 0 {
                format!("favorite-character-{id}")
            } else {
                format!("tag-{id}")
            };
            tag_entry(id as u64, &name, (count - id) as u64)
        })
        .collect()
}

fn benchmark_cache_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("tag_search_cache_build");
    for size in [10_000usize, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        let real = real_entries();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let cache =
                    TagSearchCache::from_entries_with_completeness(entries(size, &real), true);
                criterion::black_box(cache);
            });
        });
    }
    group.finish();
}

fn benchmark_cache_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("tag_search_cache_search");
    for size in [10_000usize, 100_000] {
        let real = real_entries();
        let cache = TagSearchCache::from_entries_with_completeness(entries(size, &real), true);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &cache, |b, cache| {
            b.iter(|| criterion::black_box(cache.search("favorite character", 10)));
        });
    }
    group.finish();
}

fn benchmark_real_world_search(c: &mut Criterion) {
    let real = real_entries();
    let cache = TagSearchCache::from_entries_with_completeness(entries(100_000, &real), true);
    let mut group = c.benchmark_group("tag_search_cache_real_world");
    for query in REAL_QUERIES {
        group.bench_with_input(BenchmarkId::from_parameter(query), query, |b, query| {
            b.iter(|| criterion::black_box(cache.search(query, 10)));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_cache_build,
    benchmark_cache_search,
    benchmark_real_world_search
);
criterion_main!(benches);
