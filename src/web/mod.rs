use shared_types::FileManager;

pub mod downloading;
pub mod manager;
pub mod ratelimit;

enum FileReturn {
    File(FileManager),
    /// The file was a dedup hit (already present in the db by tag, URL or
    /// hash). Its re-flush is intentionally skipped — the file is already
    /// committed — and the page's tag set carries any tag updates.
    AlreadyPresent,
    InDownloadQueue,
}
