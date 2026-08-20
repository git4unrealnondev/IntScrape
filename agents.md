# IntScrape Agent Guide

## Mission

IntScrape is a Rust scraper and download manager backed by SQLite. Changes must preserve correctness while allowing a single run to process millions of files, URLs, tags, and relationships without requiring all work or results to fit in RAM.

Treat "millions of items" as a hard operating requirement, not as a large test case. The default design target is bounded memory and bounded concurrency with throughput controlled by backpressure.

## Repository Map

- `src/main.rs`: process startup, job polling, shutdown, backups, and the Rayon processing pool.
- `src/db/mod.rs`: SQLite connection pool, writer connection, migrations, caches, and database lifecycle.
- `src/db/main.rs`: schema, queries, bulk mutations, IPC-exported database methods, and `SQL_CHUNK_SIZE`.
- `src/db/roaring.rs`: relationship cache and search implementation. `Full` loads relationship bitmaps into RAM; `Popular` and `Table` are safer for large installations.
- `src/db/tag_search.rs`: tag search cache and search indexing.
- `src/web/manager.rs`: scraper orchestration, download/process semaphores, task spawning, temporary files, hashing, and result aggregation.
- `src/web/downloading.rs`: HTTP clients, retries, rate limits, and streaming downloads.
- `src/plugins.rs`: dynamic plugin loading, plugin callbacks, startup threads, and plugin-to-host data boundaries.
- `libs/shared_types`: ABI/API data structures shared with plugins and the generated client. Collection fields here affect memory and IPC behavior.
- `libs/ipc_macro` and `generated/client`: IPC request generation and serialization. `generated/client/src/generated_api.rs` is generated; edit the source annotated with `export_ipc`, not the generated file.
- `plugins/*`: independently compiled dynamic plugins. Plugin outputs are untrusted with respect to size and must be handled as streams or bounded batches by the host.

## Current Scaling Model

- HTTP media downloads stream response chunks directly to temporary files. Keep this property; do not replace it with `bytes().await`, `Vec<u8>` accumulation, or whole-response strings for media.
- Download concurrency is currently bounded by semaphores in `src/web/manager.rs`, with defaults of 5 downloads and 2 heavy processing workers. Any new concurrency must have an explicit bound and be included in the capacity budget.
- SQLite uses WAL mode, `synchronous=NORMAL`, a connection pool, and a reserved writer connection. Writes must go through bounded transactions and avoid holding the writer lock during network, filesystem, plugin, or CPU work.
- Relationship data can use Roaring bitmaps. Prefer `Table` or `Popular` cache modes for large databases unless a measured memory budget justifies `Full`.
- IPC currently serializes one complete request and one complete response into memory and sends them over a local socket. It is not a streaming protocol. Large-result APIs must not be introduced without pagination, cursors, or a streaming protocol.

## Non-Negotiable Scaling Rules

### Bounded memory

- Never collect an unbounded iterator from SQLite, a plugin, a directory walk, or a network source merely to make processing convenient.
- Avoid `collect::<Vec<_>>()`, `HashSet`, `HashMap`, `JoinSet`, or `Vec` accumulators on paths that can contain user-scale data. Use bounded batches, a cursor, a consumer, or a spill-to-disk structure.
- A `limit` parameter is not sufficient if the query first materializes all matching rows. Apply limits and ordering in SQL and verify the query plan.
- Do not return `HashSet<u64>`, `Vec<u64>`, or maps containing millions of values through the current IPC API. Add page-oriented APIs with a stable cursor/keyset before exposing large results.
- Keep item payloads small. Avoid cloning `DbJobsObj`, URLs, tags, response text, or plugin output across task boundaries unless ownership requires it.
- `memory_manage()`/`malloc_trim()` is not a memory strategy. Fix retention and allocation behavior first; use heap profiling for evidence.

### Backpressure and task limits

- Bound every producer-consumer queue. Prefer a bounded Tokio channel or a fixed worker loop over spawning one task per item.
- Do not create a `JoinSet` or spawn tasks for an entire plugin response before consuming results. Process at most a configured batch/window at a time and await completion before admitting more work.
- Acquire permits before creating expensive work or retaining item state. A semaphore acquired after a large task set is spawned does not bound task memory.
- Separate limits for network, CPU-heavy processing, disk I/O, database writes, and per-host rate limits. Do not increase one limit without considering all downstream limits.
- Use cancellation-aware loops and ensure permits, temporary files, URL guards, and database state are released on every error and shutdown path.

### Plugins

- Assume `url_dump`, `parser_call`, callbacks, and plugin-owned collection fields can return millions of entries.
- Prefer changing plugin contracts from `Vec<T>`/`HashSet<T>` returns to iterators, callbacks, bounded pages, or a host-provided sink. If the ABI prevents this, impose a bounded adapter at the host boundary and document the maximum batch size.
- Parse and process one page or batch at a time. Do not retain all URLs from a crawl merely to deduplicate them; use a database-backed uniqueness key, bounded probabilistic filter, or external sort when exact global deduplication is required.
- Plugin code must not block Tokio worker threads with CPU-heavy parsing or synchronous I/O. Use the existing blocking/Rayon facilities with explicit capacity.
- Treat dynamic plugin data as potentially malformed or oversized. Validate lengths, URLs, strings, and nested collection sizes before expensive work.

### SQLite and data access

- Use prepared statements and transactions for bulk writes. Chunk parameter lists below SQLite's variable limit; keep `SQL_CHUNK_SIZE` centralized and measure transaction duration.
- Do not perform one SQL query per item for million-item operations. Use joins, temporary staging tables, `INSERT ... SELECT`, upserts, or bulk statements.
- Do not hold a write transaction while downloading, hashing, invoking a plugin, or doing filesystem work.
- Use keyset pagination (`WHERE id > ? ORDER BY id LIMIT ?`) instead of large `OFFSET` scans for deep traversal. Make ordering deterministic and use indexed columns.
- Add or verify indexes for every high-volume lookup and relationship direction. Use `EXPLAIN QUERY PLAN` in tests or profiling notes for new queries.
- Keep migrations set-based and restart-safe. Never load an entire table into a Rust collection during migration when SQL can transform it in place or rows can be streamed in bounded batches.
- Avoid dynamically generating a huge `UNION ALL` query as the number of namespaces grows. If namespace partitioning remains, measure SQLite statement size and consider a stable view/table or an indexed common relationship table.
- Keep cache policy explicit. Do not silently switch to `Full` for large databases, and do not duplicate the same relationship data in SQLite, Roaring, and Rust collections without a measured reason.

### Downloads and files

- Preserve streaming downloads to temporary files and incremental hashing. Never buffer media in memory.
- Enforce response-size limits where the source or plugin does not provide a trusted bound. Clean up partial files on cancellation, retry, hash mismatch, and failure.
- Deduplicate in-flight URLs with bounded or database-backed state. A process-wide `HashSet<String>` can grow without limit and must have a lifecycle/eviction policy if it is used for more than transient work.
- Keep logging out of per-item hot loops at `info` level for million-item runs. Use counters, sampled logs, structured progress, and aggregate error reporting.
- Account for file descriptors, temporary-directory capacity, disk throughput, and cleanup latency when changing download or processing concurrency.

### IPC and public APIs

- The framing code allocates the declared message size before decoding. Validate the frame length against a configured maximum before allocation and return an error instead of panicking on malformed or oversized input.
- Requests and responses must have explicit size limits, timeouts, and cancellation behavior. Never expose a whole-database operation through a synchronous one-response IPC call.
- For large data, use page size plus opaque continuation token/keyset cursor. Do not use a caller-controlled unlimited `Option<u64>` limit.
- Keep generated files unchanged. Update the exported implementation/signature, regenerate, then inspect the generated API and serialization size.
- Preserve ABI compatibility deliberately: shared types are used across dynamically loaded plugins, so changing layouts, enum variants, or collection semantics requires coordinated plugin rebuilds and migration notes.

## Implementation Workflow

1. Trace the full path from producer to durable sink before editing. Identify every collection, clone, queue, task, lock, socket frame, and transaction in the path.
2. Write down the bound for each stage: batch size, queue capacity, worker count, response bytes, temporary files, transaction rows, and retry behavior.
3. Prefer the smallest change that establishes backpressure at the earliest producer boundary. Do not compensate for an unbounded producer by raising downstream concurrency.
4. For database changes, inspect schema/indexes and run `EXPLAIN QUERY PLAN`; benchmark representative cardinalities, including millions of rows.
5. For plugin or IPC changes, test oversized, malformed, empty, duplicate, cancelled, and partial-input cases.
6. Measure resident memory, allocation rate, queue depth, throughput, SQLite busy time, WAL growth, open file descriptors, retry rate, and error counts. A successful run is not proof of scalability.

## Verification Commands

Run focused checks first, then workspace checks when feasible:

```text
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

For changes involving heap behavior, use the existing `dhat-heap` feature or an equivalent allocator profiler. For database changes, run against a disposable database populated with realistic relationship and tag cardinalities. Do not use `main.db` as a test fixture or rewrite existing user changes.

## Review Checklist

- Can input cardinality grow without bound at this boundary?
- Is memory bounded independently of total item count?
- Is concurrency bounded before tasks/items are allocated?
- Does the API stream/page rather than return a complete result?
- Are SQL limits, indexes, transaction scope, and query plans appropriate?
- Are retries, cancellation, deduplication, temp files, and permits leak-free?
- Are logs and metrics aggregated rather than one record per item?
- Are plugin/IPC input sizes validated before allocation or decoding?
- Does the change include a million-item or cardinality-scaled benchmark when behavior is performance-sensitive?
