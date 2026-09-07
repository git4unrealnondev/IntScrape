use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs::create_dir_all,
    io::{BufReader, Cursor},
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use shared_types::{
    CallbackCustomData, CallbackReturn, FileTagAction, GenericNamespaceObj, GlobalCallbacks,
    PluginTag, SQL_CHUNK_SIZE, Tag,
};

use thumbnailer::{ThumbnailSize, create_thumbnails_dynamic};

static PLUGIN_NAME: &str = "File-Thumbnailer";
static SIZE_THUMBNAIL_X: u32 = 250;
static SIZE_THUMBNAIL_Y: u32 = 250;

/// Logs a thumbnail error over IPC only the first time the exact message
/// appears in this process run. Unsupported formats can be a large fraction
/// of scanned files; logging every one over the single IPC channel floods
/// the host log and adds a round trip per file.
static LOGGED_THUMBNAIL_ERRORS: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

fn log_once_thumbnail(message: String) {
    let mut logged = LOGGED_THUMBNAIL_ERRORS.lock().unwrap();
    if logged.insert(message.clone()) {
        let _ = client::log_silent(message);
    }
}

///
/// Will determine how long the video will be before attempting to take another frame.
///
#[derive(Clone)]
pub enum VideoSpacing {
    Frame(u32),      // X frames of a video before trying to take a frame
    Duration(usize), // Number of ms before attempting to take a frame
}

/// Runs the on_start code
fn handle_on_start() -> Result<(), Box<dyn Error>> {
    let should_run = match client::setting_get("PLUGIN_Thumbnail_ShouldRun".into())? {
        None => true,
        Some(setting) => setting.param.map(|param| param != "False").unwrap_or(true),
    };

    if !should_run {
        return Ok(());
    }

    // Filters by file id
    let mut total_file_ids = client::get_file_ids_all()?;

    match client::namespace_get("file_thumbnail".to_string()) {
        Ok(Some(ns_id)) => {
            if let Ok(namespace_file_ids) = client::get_namespace_file_ids(ns_id) {
                total_file_ids.retain(|f| !namespace_file_ids.contains(f));
            }
        }
        Ok(None) => {
            let _ = client::namespace_set(GenericNamespaceObj {
                name: "file_thumbnail".into(),
                description: Some("A thumbnail hash.".into()),
            });
        }
        Err(e) => {
            return Err(e);
        }
    }

    let mut completed = true;

    let mut total_file_ids_iter = total_file_ids.iter();

    loop {
        // Collect a small chunk of file IDs (e.g., 1000 items) into memory at a time
        let batch: Vec<_> = total_file_ids_iter
            .by_ref()
            .take(SQL_CHUNK_SIZE as usize)
            .collect();

        if batch.is_empty() {
            break;
        }

        let processed = batch
            .par_iter()
            .try_fold(Vec::new, |mut pending, file_id| {
                let should_stop = client::should_exit();
                if should_stop.is_err() || should_stop.is_ok_and(|stop| stop) {
                    return Err(());
                }

                // Note: file_id is &&FileId because batch contains references from .iter()
                if let Ok(Some(file_path)) = client::get_file_path(**file_id) {
                    let reader = std::fs::File::open(&file_path)
                        .map(BufReader::new)
                        .map_err(|error| format!("{error}"))
                        .and_then(generate_thumbnails);
                    match reader {
                        Ok(tags) => {
                            let tags = tags.into_iter().collect();
                            pending.push((**file_id, tags));
                        }
                        Err(error) => {
                            log_once_thumbnail(format!("{PLUGIN_NAME}: Recieved Error {error}"));
                        }
                    }
                }

                Ok(pending)
            })
            .try_reduce(Vec::new, |mut left, mut right| {
                left.append(&mut right);
                Ok(left)
            });

        let pending = match processed {
            Ok(pending) => pending,
            Err(()) => {
                completed = false;
                break;
            }
        };

        if !pending.is_empty() {
            let tags_by_file = pending.into_iter().collect();
            if client::put_tags_to_files(tags_by_file).is_err() {
                completed = false;
                break;
            }
        }
    }

    if completed {
        let _ = client::setting_set(shared_types::DbSettingsObj {
            name: "PLUGIN_Thumbnail_ShouldRun".into(),
            description: Some("Should the plugin thumbnail run on_start".into()),
            num: None,
            param: Some("False".into()),
        });
        let _ = client::log_silent(format!(
            "{PLUGIN_NAME}: Has finished on_start calculating thumbnails."
        ));
    }

    Ok(())
}

#[unsafe(no_mangle)]
fn on_start() {
    if let Ok(thumbnail_path) = process_thumb_location() {
        let _ = client::log_silent(format!(
            "{PLUGIN_NAME}: Will download thumbnails to: {}",
            thumbnail_path.to_string_lossy()
        ));
    }

    let _ = handle_on_start();
}

#[unsafe(no_mangle)]
fn file_thumbnail_generate_thumbnail_fid(
    callback: &shared_types::CallbackInfoInput,
) -> HashMap<String, shared_types::CallbackCustomDataReturning> {
    let mut out = HashMap::new();
    let index = callback.data_name.iter().position(|x| x == "file_id");
    if let Some(index) = index
        && callback.data.len() > index
        && let Some(custom_data) = callback.data.get(index)
        && let shared_types::CallbackCustomDataReturning::U64(file_id) = custom_data
    {
        let tags = client::namespace_get("file_thumbnail".into())
            .ok()
            .flatten()
            .and_then(|namespace_id| client::get_tags_filtered(*file_id, namespace_id).ok())
            .unwrap_or_default();

        let thumbnail_tags =
            client::get_tag_id_bulk(tags.iter().copied().collect()).unwrap_or_default();
        if let Ok(thumbpath) = process_thumb_location() {
            for thumbnail_id in tags {
                let Some(thumbnail_tag) = thumbnail_tags.get(&thumbnail_id) else {
                    continue;
                };
                if let Some(path) = thumbnail_path(&thumbpath, &thumbnail_tag.name)
                    && path.is_file()
                {
                    out.insert(
                        "ReturnPath".to_string(),
                        shared_types::CallbackCustomDataReturning::String(
                            path.to_string_lossy().into_owned(),
                        ),
                    );
                }
            }
        }

        if !out.is_empty() {
            return out;
        }

        let Some(Ok(file_path)) = client::get_file_path(*file_id).transpose() else {
            return out;
        };
        let tags = match std::fs::File::open(&file_path)
            .map(BufReader::new)
            .map_err(|error| format!("{error}"))
            .and_then(generate_thumbnails)
        {
            Ok(tags) => tags,
            Err(error) => {
                log_once_thumbnail(format!("{PLUGIN_NAME}: Recieved Error {error}"));
                HashSet::new()
            }
        };
        let tags: Vec<FileTagAction> = tags.iter().cloned().collect();

        if let Ok(thumbpath) = process_thumb_location() {
            for tag_action in &tags {
                for tag in &tag_action.tags {
                    if let Some(path) = thumbnail_path(&thumbpath, &tag.tag.name)
                        && path.is_file()
                    {
                        out.insert(
                            "ReturnPath".to_string(),
                            shared_types::CallbackCustomDataReturning::String(
                                path.to_string_lossy().into_owned(),
                            ),
                        );
                        break;
                    }
                }
                if out.contains_key("ReturnPath") {
                    break;
                }
            }
        }
        let _ = client::put_tags_to_file(*file_id, tags);
    }

    out
}

fn thumbnail_path(base: &Path, hash: &str) -> Option<PathBuf> {
    if hash.len() < 6 || !hash.is_ascii() {
        return None;
    }

    Some(
        base.join(&hash[0..2])
            .join(&hash[2..4])
            .join(&hash[4..6])
            .join(format!("{hash}.webp")),
    )
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "file_thumbnail".into(),
        callbacks: vec![
            GlobalCallbacks::Start(shared_types::StartupThreadType::Spawn),
            GlobalCallbacks::Download,
            GlobalCallbacks::Callback(shared_types::CallbackInfo {
                func: "file_thumbnail_generate_thumbnail_fid".to_string(),
                vers: 0,
                data_name: vec!["file_id".into()],
                data: vec![CallbackCustomData::U64],
            }),
        ],
        ..Default::default()
    }]
}

/// Gets default folder to dump thumbnails into
fn process_thumb_location() -> Result<PathBuf, Box<dyn Error>> {
    if client::setting_get("PLUGIN_thumbnail_location".into())?.is_none_or(|f| f.param.is_none())
        && let Ok(Some(local_file_setting)) = client::setting_get("SYSTEM_file_location".into())
    {
        let good_param = local_file_setting
            .param
            .ok_or("Cannot get param from sytem")?;

        // Gets the location and puts it next to the default file downloading spot
        let path = Path::new(&good_param);
        let path_canon = path.canonicalize()?;
        let mut path_final = path_canon
            .parent()
            .ok_or("Cannot strip parent dir")?
            .to_path_buf();
        // The API and WebUI containers share the plural mount point.
        path_final.push("thumbnails");

        // Makes the dir
        create_dir_all(&path_final)?;

        let _ = client::setting_set(shared_types::DbSettingsObj {
            name: "PLUGIN_thumbnail_location".into(),
            description: Some("Where thumbnails get stored".into()),
            num: None,
            param: Some(path_final.to_string_lossy().to_string()),
        });
    } else {
        if let Ok(Some(setting)) = client::setting_get("PLUGIN_thumbnail_location".into()) {
            let good_param = setting.param.ok_or("Cannot get param from sytem")?;

            return Ok(Path::new(&good_param).to_path_buf());
        }
    }

    Err("Could not find final path".into())
}

#[unsafe(no_mangle)]
fn on_download(bytes: &[u8]) -> CallbackReturn {
    match generate_thumbnails(BufReader::new(Cursor::new(bytes))) {
        Ok(tags) => CallbackReturn {
            tags,
            ..Default::default()
        },
        Err(error) => {
            log_once_thumbnail(format!("{PLUGIN_NAME}: Recieved Error {error}"));
            CallbackReturn::default()
        }
    }
}

/// Generates a thumbnail from any readable source, streaming the media
/// straight through instead of buffering the whole file in memory.
fn generate_thumbnails(
    reader: impl std::io::BufRead + std::io::Seek,
) -> Result<HashSet<FileTagAction>, String> {
    let mut tags = HashSet::new();
    let thumb = create_thumbnails_dynamic(
        reader,
        &ThumbnailSize::Custom((SIZE_THUMBNAIL_X, SIZE_THUMBNAIL_Y)),
    )
    .map_err(|error| format!("{error:?}"))?;
    let thumbpath = process_thumb_location().map_err(|error| error.to_string())?;
    let (thumb_path, thumb_hash) = make_thumbnail_path(&thumbpath, &thumb);
    let thpath = thumb_path
        .join(thumb_hash.clone())
        .with_added_extension("webp");
    let pa = thpath.to_string_lossy().to_string();

    std::fs::write(&pa, &thumb).map_err(|error| format!("{error}"))?;
    tags.insert(FileTagAction {
        operation: shared_types::TagOperation::Set,
        tags: vec![PluginTag {
            tag: Tag {
                name: thumb_hash.to_string(),
                namespace: GenericNamespaceObj {
                    name: "file_thumbnail".to_string(),
                    description: Some("A thumbnail hash.".into()),
                },
            },
            ..Default::default()
        }],
    });

    Ok(tags)
}

fn make_thumbnail_path(dbloc: &PathBuf, imgdata: &Vec<u8>) -> (PathBuf, String) {
    use sha2::Digest;
    use sha2::Sha256;
    use std::fs::canonicalize;
    use std::fs::create_dir_all;
    let mut hasher = Sha256::new();
    hasher.update(imgdata);
    let hash = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{:02X}", byte))
        .collect::<String>();

    if canonicalize(dbloc).is_err() {
        create_dir_all(dbloc).unwrap();
    }
    // Final folder location path of db
    let folderpath = canonicalize(dbloc)
        .unwrap()
        .join(&hash[0..2])
        .join(&hash[2..4])
        .join(&hash[4..6]);
    if let Ok(path) = std::fs::exists(folderpath.clone())
        && path
    {
        return (folderpath, hash);
    }

    if let Err(err) = create_dir_all(folderpath.clone()) {
        panic!("Faled to make path at: {} {}", dbloc.to_string_lossy(), err);
    }

    (folderpath, hash)
}
