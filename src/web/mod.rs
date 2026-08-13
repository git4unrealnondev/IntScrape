use shared_types::{FileInternal, FileManager};

pub mod downloading;
pub mod manager;
pub mod scraping;

enum FileReturn {
    File(FileManager),
    InDownloadQueue,
}
