use shared_types::{FileInternal, FileManager};

pub mod downloading;
pub mod manager;
pub mod manager_human;
pub mod ratelimit;

/// How many jobs we can run at the same time for a plugin
pub const MAX_CONCURRENT_JOBS: usize = 5;

/// How many concurrent downloads can we have running at once for a plugin
const MAX_CONCURRENT_DOWNLOADS: usize = 5;

#[derive(Debug)]
enum FileReturn {
    File(FileManager),
    /// The file was a dedup hit: it is already committed, so only its
    /// tag list needs persisting. The carried `FileInternal` is the
    /// existing db row (resolved by URL or hash dedup); the flush path
    /// uses it to attach the scraped tags to that file instead of
    /// creating a duplicate. Relationships survive re-walks this way.
    AlreadyPresent(FileInternal),
    InDownloadQueue,
}
