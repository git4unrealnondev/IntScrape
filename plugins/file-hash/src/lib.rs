use std::collections::HashSet;

use shared_types::{
    CallbackReturn, FileTagAction, GenericNamespaceObj, GlobalCallbacks, PluginTag, Tag,
};

#[derive(PartialEq, Clone, Copy, Debug, Eq, Hash)]
enum Supset {
    MD5,
    SHA1,
    SHA256,
    SHA512,
    IpfsCid,
    IPFSCID1,
    Imagehash,
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "file_hash".into(),
        callbacks: vec![
            GlobalCallbacks::Start(shared_types::StartupThreadType::SpawnInline),
            GlobalCallbacks::Download,
        ],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
fn on_download(bytes: &[u8]) -> CallbackReturn {
    let mut tags = Vec::new();
    for supset in [
        Supset::MD5,
        Supset::SHA1,
        Supset::SHA256,
        Supset::SHA512,
        Supset::IpfsCid,
        Supset::IPFSCID1,
        Supset::Imagehash,
    ] {
        if let Some(hash) = hash_file(&supset, bytes) {
            tags.push(PluginTag {
                tag: Tag {
                    namespace: get_set(&supset),
                    name: hash,
                },
                ..Default::default()
            });
        }
    }

    let mut file_tag_action_list = HashSet::new();

    file_tag_action_list.insert(FileTagAction {
        operation: shared_types::TagOperation::Set,
        tags,
    });

    CallbackReturn {
        tags: file_tag_action_list,
        ..Default::default()
    }
}

///
/// Hashes a file with the selected hash type.
/// outputs has as a string or an option string.
///
fn hash_file(hashtype: &Supset, byte: &[u8]) -> Option<String> {
    use sha1::{Digest, Sha1};
    use sha2::{Sha256, Sha512};
    match hashtype {
        Supset::MD5 => {
            let mut hasher = md5::Md5::new();
            hasher.update(byte);

            let hash = hex::encode(hasher.finalize());
            Some(hash)
        }
        Supset::SHA1 => {
            let mut hasher = Sha1::new();
            hasher.update(byte);
            let hash = hex::encode(hasher.finalize());
            Some(hash)
        }
        Supset::SHA256 => {
            let mut hasher = Sha256::new();
            hasher.update(byte);
            let hash = hex::encode(hasher.finalize());
            Some(hash)
        }
        Supset::SHA512 => {
            let mut hasher = Sha512::new();
            hasher.update(byte);
            let hash = hex::encode(hasher.finalize());
            Some(hash)
        }
        Supset::IpfsCid => {
            if let Ok(cid) = ipfs_cid::generate_cid_v0(byte) {
                return Some(cid);
            }

            None
        }
        Supset::IPFSCID1 => {
            if let Ok(cid) = ipfs_cid::generate_cid_v1(byte) {
                return Some(cid);
            }

            None
        }
        Supset::Imagehash => {
            use image_hasher::BitOrder;
            use image_hasher::HasherConfig;
            use std::io::Cursor;

            let hasher = HasherConfig::new()
                .hash_alg(image_hasher::HashAlg::Median)
                .bit_order(BitOrder::MsbFirst)
                .preproc_dct()
                .to_hasher();
            if let Ok(img) = image::ImageReader::new(Cursor::new(byte)).with_guessed_format()
                && let Ok(decode) = img.decode()
            {
                return Some(hasher.hash_image(&decode).to_base64());
            }
            None
        }
    }
}

///
/// Gets info. holder for stuff
///
fn get_set(inp: &Supset) -> GenericNamespaceObj {
    match inp {
        Supset::MD5 => GenericNamespaceObj {
            name: "FileHash-MD5".to_string(),
            description: Some("From plugin FileHash. MD5 hash of the file.".to_string()),
        },
        Supset::SHA1 => GenericNamespaceObj {
            name: "FileHash-SHA1".to_string(),
            description: Some("From plugin FileHash. SHA1 hash of the file.".to_string()),
        },
        Supset::SHA256 => GenericNamespaceObj {
            name: "FileHash-SHA256".to_string(),
            description: Some("From plugin FileHash. SHA256 hash of the file.".to_string()),
        },
        Supset::SHA512 => GenericNamespaceObj {
            name: "FileHash-SHA512".to_string(),
            description: Some("From plugin FileHash. SHA512 hash of the file.".to_string()),
        },
        Supset::IpfsCid => GenericNamespaceObj {
            name: "FileHash-IPFSCID".to_string(),
            description: Some("From plugin FileHash. IPFS Content ID of the file for usage with the IPFS network.".to_string()),
        },Supset::IPFSCID1 => GenericNamespaceObj {
            name: "FileHash-IPFSCID1".to_string(),
            description: Some("From plugin FileHash. IPFS Content ID of the file for usage with the IPFS network. Version 1 more modern".to_string()),
        },
            Supset::Imagehash => GenericNamespaceObj {
            name: "FileHash-ImageHash".to_string(),
            description: Some("From plugin FileHash. PHash of the image. Used to deduplicate similar images if the hashes aren't the same".to_string())
        }

    }
}
