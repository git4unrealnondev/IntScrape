use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use client::setting_get;
use shared_types::{
    CallbackReturn, FileTagAction, GenericNamespaceObj, GlobalCallbacks, PluginTag, Tag,
};

const LISTOFSUPSET: [Supset; 7] = [
    Supset::MD5,
    Supset::SHA1,
    Supset::SHA256,
    Supset::SHA512,
    Supset::IpfsCid,
    Supset::IPFSCID1,
    Supset::Imagehash,
];

// Number of jobs to run concurrently while on_start runs
const NUMOFJOBS: usize = 10;

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

fn handle_on_start() -> Result<(), Box<dyn Error>> {
    let should_run = match setting_get("PLUGIN_FileHash_ShouldRun".into())? {
        None => true,
        Some(setting) => setting.param.map(|param| param != "False").unwrap_or(true),
    };

    if !should_run {
        return Ok(());
    }

    let mut total_file_ids: HashMap<u64, Vec<Supset>> = client::get_file_ids_all()?
        .iter()
        .map(|&f| (f, Vec::with_capacity(LISTOFSUPSET.len())))
        .collect();

    for table in LISTOFSUPSET {
        let _ = client::log_silent(format!("Starting to process table: {:?}", table));
        let ns_id = client::namespace_set(get_set(&table))?;
        let namespace_file_ids = client::get_namespace_file_ids(ns_id)?;
        for (file_id, missing_tables) in total_file_ids.iter_mut() {
            if !namespace_file_ids.contains(file_id) {
                missing_tables.push(table);
            }
        }
    }

    let cancel_flag = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let pool = threadpool::ThreadPool::new(NUMOFJOBS);

    for (file_id, tables) in total_file_ids {
        // Stop scheduling new work immediately if exit is requested
        if client::should_exit()? {
            cancel_flag.store(true, Ordering::Relaxed);
            break;
        }

        if !tables.is_empty()
            && let Some(file_path) = client::get_file_path(file_id)?
            && let Ok(file_data) = std::fs::read(file_path)
        {
            let tx = tx.clone();
            let cancel_flag = Arc::clone(&cancel_flag);

            pool.execute(move || {
                // Check immediately when worker picks up the job
                if cancel_flag.load(Ordering::Relaxed) {
                    return;
                }

                let mut tags = Vec::with_capacity(tables.len());
                for table in tables {
                    // Cooperative check: skips remaining tables instantly on Ctrl+C
                    if cancel_flag.load(Ordering::Relaxed) {
                        return;
                    }

                    if let Some(file_hash) = hash_file(&table, &file_data) {
                        tags.push(PluginTag {
                            tag: Tag {
                                namespace: get_set(&table),
                                name: file_hash.clone(),
                            },
                            ..Default::default()
                        });

                        let _ = client::log_silent(format!(
                            "FileHash: FileId: {} hash: {} type: {}",
                            file_id,
                            file_hash,
                            get_set(&table).name
                        ));
                    }
                }

                // Send completed results back to the main thread via channel
                if !cancel_flag.load(Ordering::Relaxed) && !tags.is_empty() {
                    let _ = tx.send((file_id, tags));
                }
            });
        }
    }

    // Drop original sender so the receiver loop exits cleanly once workers finish
    drop(tx);

    // Main thread handles ALL IPC communication safely in one place
    for (file_id, tags) in rx {
        if client::should_exit()? {
            cancel_flag.store(true, Ordering::Relaxed);
            break;
        }

        let _ = client::put_tags_to_file(
            file_id,
            vec![FileTagAction {
                operation: shared_types::TagOperation::Set,
                tags,
            }],
        );
    }

    if !client::should_exit()? {
        pool.join();

        let _ = client::setting_set(shared_types::DbSettingsObj {
            name: "PLUGIN_FileHash_ShouldRun".into(),
            description: Some("Should the plugin filehash run on_start".into()),
            num: None,
            param: Some("False".into()),
        });
    }
    Ok(())
}

#[unsafe(no_mangle)]
fn on_start() {
    let _ = handle_on_start();
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
    for supset in LISTOFSUPSET {
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
