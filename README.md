# IntScrape

A daemon that scrapes media (images/video) from supported sites into a
local Turso (SQLite-compatible) database, deduplicated by file hash, with
a plugin system for sites and a local IPC interface for clients.

## Layout

| Path | What it is |
|---|---|
| `src/lib.rs` | Library root: all modules (`db`, `web`, `plugins`, `ipc`, `cli`, …) and `DB_VERSION` (current DB schema version). |
| `src/main.rs` | Thin binary: builds the tokio runtime and runs the daemon. |
| `src/db/turso/` | Live database layer, split by domain (`file`, `tag`, `jobs`, `search`, `relationship`, `slurp`, `system_jobs`, `sql`, …). Each module's tests live in a `tests.rs` submodule next to it (e.g. `slurp/tests.rs`). A file's SHA-512 digest lives in `FileHashes` (algorithm `SHA512`, keyed by `file_id`) — schema v13 removed the duplicate `File.hash` column; `FileInternal.hash` in memory is populated by joining `FileHashes` on read. Digests are canonicalized at the database boundary (SHA-1/SHA-256/SHA-512 uppercase, MD5 lowercase — see `db::canonical_digest`; schema v14 canonicalizes existing rows), so reads never return a lowercase SHA-512. |
| `src/web/` | Download manager, HTTP client construction, rate limiting. |
| `src/plugins.rs` | Plugin host: loads `cdylib` plugins from `compiled_plugins/` via `libloading`. |
| `libs/shared_types/` | Types shared by host and plugins (the plugin ABI contract). |
| `libs/ipc_macro/` + `generated/client/` | Proc-macro that generates the IPC client used by the web UI / CLI clients. |
| `plugins/*/ ` | One crate per site/service (20 plugins). Each exports `get_plugin_info`, `url_dump`, `parser_call`, … as `#[no_mangle]` symbols. |
| `build.rs` | Builds all plugins into `compiled_plugins/` on release builds. Debug builds do **not** refresh plugins (see the warning it prints); run `cargo build --release` after editing a plugin and restart the daemon. |

The old rusqlite-backed database layer (`src/db/old_code/`, gated behind an
undefined `legacy` feature) was removed; it remains in git history.

## Build & run

```bash
cargo build --release          # builds the daemon + all plugins
./target/release/intscrape     # add jobs via the CLI / IPC client
```

Configuration is via environment variables (defaults are relative to the
current working directory, so you no longer have to run from the source
tree):

| Variable | Default | Meaning |
|---|---|---|
| `INTSCRAPE_DB_PATH` | `turso.db` | Turso database file |
| `INTSCRAPE_LOG_PATH` | `log.txt` | Log file (appended and rotated by size, never deleted on startup) |
| `INTSCRAPE_PLUGINS_PATH` | `compiled_plugins` | Directory of built plugin `.so`s |
| `INTSCRAPE_WORKER_THREADS` | `16` | Tokio worker threads for the daemon runtime |
| `INTSCRAPE_DB_POOL_SIZE` | `32` | Idle Turso connections kept in the pool (`0` disables pooling) |

### Database tuning (for when the DB is the bottleneck)

The database is Turso in MVCC journal mode (see `serving_journal_mode` in
`src/db/turso/mod.rs`). The knobs that already exist, in rough order of
impact when the system is DB-limited:

| Knob | Default | Effect |
|---|---|---|
| `ipc_channel_count` (DB setting) | `4` channels × 8 handlers = 32 concurrent DB operations | Lower it if MVCC conflict retries / `Busy` logs climb under load; raise it if the DB is idle but IPC queues back up. The default was lowered from 10 (80 concurrent) because at millions-of-files scale the DB, not IPC, is the bottleneck. |
| `INTSCRAPE_JOURNAL_MODE` | `mvcc` | `wal` restores the old single-writer WAL mode for A/B comparison. MVCC was measured much faster for bulk writes on this codebase; don't switch back without re-measuring. |
| `INTSCRAPE_WRITE_TX` | `CONCURRENT` (MVCC) | Transaction behavior for contended write paths. `IMMEDIATE` serializes writers (no MVCC retry waste, less parallelism). |
| `INTSCRAPE_SERIALIZED_TX` | `IMMEDIATE` | Transaction behavior for DDL / serialized paths. |

MVCC passive checkpointing is enabled (`experimental_mvcc_passive_checkpoint`), so the existing `wal_checkpoint(TRUNCATE)` calls (shutdown, slurp batches, backups) can reclaim the MVCC logical log while readers are active instead of only as full truncates — important at millions-of-files scale, where the log is the main SSD-write amplifier.

Every connection also gets a 1s busy timeout (`TursoDatabase::connect`),
so `IMMEDIATE` writers wait for the lock briefly instead of failing and
re-running the whole transaction. Most DB traffic is small requests, so
connections are pooled (`ConnectionPool` in `src/db/turso/mod.rs`):
checkout is free, `prepare_cached` statement caches survive across
requests, and connections returned with an open transaction are
discarded instead of pooled. Journal-mode switches drain the idle pool
first (idle connections can make the database-wide pragma fail).
Per-file/per-row IPC calls were the biggest historical cost — prefer the
bulk endpoints (`get_file_hashes_all_sha512`, `hashes_files_get_bulk`, …)
over N single-file lookups.

### IPC transport

IPC is Unix-socket IPC via `interprocess`: one connection per request,
bitcode framing (length prefix + payload), spread across the
`ipc_channel_count` independent channels (see the database tuning
table above). The Turso connection pool is what cut small-request
latency; bulk endpoints are preferred over per-file lookups (see
above).

## Plugin ABI note

Plugins are Rust `cdylib`s built with the same toolchain as the host
(by `build.rs`). Nothing but byte buffers crosses the FFI boundary:
every plugin entry point (`get_plugin_info`, `url_dump`, `parser_call`,
`on_download`, `on_regex`, external callbacks like `proxy_*`) takes or
returns bitcode-serialized bytes in an owned `shared_types::CVec<u8>`
(see `shared_types::plugin_ffi`), with the original Rust signatures kept
as `*_impl` helpers inside each plugin. Passing Rust `Vec`/`HashMap`/
struct references across the `libloading` boundary was unsound in
principle (no stable C ABI for owned types); serialized bytes are the
only thing with a well-defined layout, and bitcode is already the
host↔client IPC format, so plugins speak one wire format. Plugin
wrappers also `catch_unwind`, so a panicking plugin returns an empty
result instead of unwinding across the boundary into the `panic = "abort"`
host. The one remaining allocator assumption is standard for this
pattern: `CVec` buffers are freed by the consumer with
`Vec::from_raw_parts`, valid because both sides use the default system
allocator. (Crates like `cxx`/`uniffi`/`extism` were considered and
rejected: they target C++/other languages or WASM plugin rewrites,
neither of which fits Rust-to-Rust `cdylib` plugins.)

## Tests

```bash
cargo test --lib               # unit tests (incl. the extracted per-module tests)
cargo test --lib db::turso::bench -- --ignored --nocapture   # scale benchmarks
cargo clippy --workspace --all-targets -- -D warnings
```

No license file has been added yet — that's a deliberate decision for the
project owner to make, not an oversight.
