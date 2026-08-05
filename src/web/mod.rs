use shared_types::FileInternal;

pub mod downloading;
pub mod manager;
pub mod scraping;

enum FileReturn {
    File(FileInternal),
    InDownloadQueue,
}
