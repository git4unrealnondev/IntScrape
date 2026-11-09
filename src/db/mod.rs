use shared_types::{FileInternal, HashesSupported};

pub mod turso;

pub mod system_jobs;

pub const SYSTEM_DATABASE_BACKUP_SITE: &str = "SYSTEM_BACKUP";
pub const SYSTEM_DATABASE_SLURP_SITE: &str = "SYSTEM_DB_SLURP";
pub const SYSTEM_FILE_SIZE_SITE: &str = "SYSTEM_FILE_SIZE";
pub const SYSTEM_FILE_HASH_SITE: &str = "SYSTEM_FILE_HASH";
pub const SYSTEM_STORAGE_CHECK_SITE: &str = "SYSTEM_STORAGE_CHECK";
pub const SYSTEM_STORAGE_CHECK_FILENAME_MODE: &str = "filename";
pub const SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE: &str = "redownload";

/// Maximum rows per generated SQL batch. Turso supports up to 32766 bound
/// variables per statement; the widest batch currently uses seven variables
/// per row (Jobs), so 4600 leaves a small safety margin below that ceiling.
pub(crate) const SQL_CHUNK_SIZE: usize = 4600;

#[derive(Debug, Default, PartialEq)]
pub struct SourceUrlFileStatus {
    pub file: Option<FileInternal>,
    pub dead: bool,
}

pub fn hashes_supported_to_inner(hash: &HashesSupported) -> (&str, &String) {
    match hash {
        HashesSupported::Md5(md5) => ("MD5", md5),
        HashesSupported::Sha1(hash) => ("SHA1", hash),
        HashesSupported::Sha256(hash) => ("SHA256", hash),
        HashesSupported::Sha512(hash) => ("SHA512", hash),
        // HashesSupported::IPFSCID(hash) => ("IPFSCID", hash),
        // HashesSupported::IPFSCID1(hash) => ("IPFSCID1", hash),
        HashesSupported::ImageHash(hash) => ("ImageHash", hash),
    }
}

pub fn hashes_supported_to_key(hash: &HashesSupported) -> (String, String) {
    let (algorithm, digest) = hashes_supported_to_inner(hash);
    (algorithm.to_string(), canonical_digest(algorithm, digest))
}

/// Canonical hex casing for a digest of the given algorithm.
///
/// The codebase's convention (see `web::manager::DownloadHasher::finish`
/// and `hash_bytes`) is: MD5 digests are lowercase hex, while SHA-1,
/// SHA-256 and SHA-512 digests are uppercase hex — files on disk are
/// named with the uppercase SHA-512, and the storage check uppercases
/// filename stems before lookup. Nothing in the database layer enforced
/// that convention, so any lowercase digest that reached `FileHashes`
/// (a slurped library, a plugin-supplied identifying hash, a pre-v13
/// secondary row) was stored verbatim. Before schema v13 that was
/// invisible: `File.hash` (always the manager's uppercase digest) was
/// what every read returned. Since v13 the reads join `FileHashes`,
/// so a lowercase row there is now what `file_get`,
/// `file_hashes_get_all_sha512` and friends return.
///
/// Canonicalizing at the database boundary restores the pre-v13
/// observable behaviour. Only full-length hex digests for a known
/// algorithm are transformed: short test fixtures ("aaa", "abc123"),
/// base64 image hashes, IPFS CIDs and other non-hex values pass
/// through unchanged.
pub fn canonical_digest(algorithm: &str, digest: &str) -> String {
    let expected_len = match algorithm.to_ascii_uppercase().as_str() {
        "MD5" => 32,
        "SHA1" => 40,
        "SHA256" => 64,
        "SHA512" => 128,
        _ => return digest.to_string(),
    };
    if digest.len() != expected_len || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return digest.to_string();
    }
    if algorithm.eq_ignore_ascii_case("MD5") {
        digest.to_ascii_lowercase()
    } else {
        digest.to_ascii_uppercase()
    }
}
