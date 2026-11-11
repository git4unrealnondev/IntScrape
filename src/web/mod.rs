use shared_types::{FileInternal, FileManager};

pub mod downloading;
pub mod manager;
pub mod ratelimit;

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
