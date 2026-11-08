//! IntScrape library: the daemon [`main`] binary is a thin wrapper around
//! these modules, so `cargo test --lib` exercises the same code the binary
//! runs (previously everything lived in the binary crate and the lib was a
//! stub containing only `backup_path`).
//!
//! [`main`]: https://docs.rs/intscrape

/// Current Turso schema version. Bump when a migration is added; opening a
/// database with a newer schema than this is rejected (see `db::turso`).
pub const DB_VERSION: u64 = 12;

pub mod backup_path;
pub mod cli;
pub mod db;
pub mod helper_functions;
pub mod ipc;
pub mod plugins;
pub mod web;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;
