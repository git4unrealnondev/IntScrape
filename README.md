# IntScrape

A daemon that scrapes media (images/video) from supported sites into a
local Turso (SQLite-compatible) database, deduplicated by file hash, with
a plugin system for sites and a local IPC interface for clients.

## Layout

| Path | What it is |
|---|---|
| `src/lib.rs` | Library root: all modules (`db`, `web`, `plugins`, `ipc`, `cli`, …) and `DB_VERSION` (current DB schema version). |
| `src/main.rs` | Thin binary: builds the tokio runtime and runs the daemon. |
| `src/db/turso/` | Live database layer, split by domain (`file`, `tag`, `jobs`, `search`, `relationship`, `slurp`, `system_jobs`, `sql`, …). Each module's tests live in a `tests.rs` submodule next to it (e.g. `slurp/tests.rs`). |
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

## Plugin ABI note

Plugins are Rust `cdylib`s built with the same toolchain as the host
(by `build.rs`). `get_plugin_info` returns the `#[repr(C)]`
`shared_types::CVec<Plugin>` container rather than a bare Rust `Vec`, and
the host reconstructs it with `CVec::into_vec`. The plugin types
themselves still contain `String`/`Vec`, so the ABI is Rust-to-Rust by
construction; serializing plugin payloads to bytes would be the fully
stable ABI and is a deliberate future redesign, not an oversight.

## Tests

```bash
cargo test --lib               # unit tests (incl. the extracted per-module tests)
cargo test --lib db::turso::bench -- --ignored --nocapture   # scale benchmarks
cargo clippy --workspace --all-targets -- -D warnings
```

No license file has been added yet — that's a deliberate decision for the
project owner to make, not an oversight.
