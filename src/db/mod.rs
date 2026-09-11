use shared_types::{FileInternal, HashesSupported};

pub mod turso;

pub(crate) mod system_jobs;

pub const SYSTEM_DATABASE_BACKUP_SITE: &str = "SYSTEM_BACKUP";
pub const SYSTEM_DATABASE_SLURP_SITE: &str = "SYSTEM_DB_SLURP";
pub const SYSTEM_FILE_SIZE_SITE: &str = "SYSTEM_FILE_SIZE";
pub const SYSTEM_FILE_HASH_SITE: &str = "SYSTEM_FILE_HASH";
pub const SYSTEM_STORAGE_CHECK_SITE: &str = "SYSTEM_STORAGE_CHECK";
pub const SYSTEM_STORAGE_CHECK_FILENAME_MODE: &str = "filename";
pub const SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE: &str = "redownload";

/// Maximum rows per write batch for the write path. Kept separate from
/// `shared_types::SQL_CHUNK_SIZE`, which drives the plugin-facing chunking.
pub(crate) const SQL_CHUNK_SIZE: usize = 800;

#[derive(Debug, Default, PartialEq)]
pub struct SourceUrlFileStatus {
    pub file: Option<FileInternal>,
    pub dead: bool,
}

pub fn hashessupportedtoinner(hash: &HashesSupported) -> (&str, &String) {
    match hash {
        HashesSupported::Md5(md5) => ("MD5", md5),
        HashesSupported::Sha1(hash) => ("SHA1", hash),
        HashesSupported::Sha256(hash) => ("SHA256", hash),
        HashesSupported::Sha512(hash) => ("SHA512", hash),
        HashesSupported::IPFSCID(hash) => ("IPFSCID", hash),
        HashesSupported::IPFSCID1(hash) => ("IPFSCID1", hash),
        HashesSupported::ImageHash(hash) => ("ImageHash", hash),
    }
}

pub fn hashessupportedtokey(hash: &HashesSupported) -> (String, String) {
    let (algorithm, digest) = hashessupportedtoinner(hash);
    (algorithm.to_string(), digest.clone())
}

/// Legacy rusqlite-backed `MainDatabase` layer, kept for reference and its
/// unit tests. Enabled with the `legacy` cargo feature (or `--features legacy`
/// when running `cargo test`); the turso backend is the live implementation.
#[cfg(feature = "legacy")]
pub mod old_code;