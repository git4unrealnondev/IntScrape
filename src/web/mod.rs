use shared_types::FileManager;

pub mod downloading;
pub mod manager;
pub mod ratelimit;

enum FileReturn {
    File(FileManager),
    InDownloadQueue,
}
