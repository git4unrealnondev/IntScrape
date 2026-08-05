use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs::create_dir_all,
    path::{Path, PathBuf},
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use shared_types::{
    CallbackCustomData, CallbackReturn, FileTagAction, GenericNamespaceObj, GlobalCallbacks,
    PluginTag, Tag,
};
use webp_animation::EncodingConfig;

use thumbnailer::{Thumbnail, ThumbnailSize, create_thumbnails_dynamic, error::ThumbError};

static PLUGIN_NAME: &str = "file_thumbnail";
static SIZE_THUMBNAIL_X: u32 = 250;
static SIZE_THUMBNAIL_Y: u32 = 250;
static DEFAULT_VIDEO_SETTINGS: VideoDefaults = VideoDefaults {
    frames: 50,
    //duration: VideoSpacing::Duration(1000),
};

///
/// Default video settings
///
#[derive(Clone)]
pub struct VideoDefaults {
    frames: u32, // how many frames should be in the animated webp
                 //   duration: VideoSpacing, // how much duration should be before each frame get's captures
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
                total_file_ids.retain_mut(|f| !namespace_file_ids.contains(f));
            }
        }
        Ok(None) => {}
        Err(e) => {
            return Err(e);
        }
    }

    const BATCH_SIZE: usize = 100;
    let mut completed = true;

    for batch in total_file_ids.chunks(BATCH_SIZE) {
        let processed = batch
            .par_iter()
            .try_fold(Vec::new, |mut pending, file_id| {
                let should_stop = client::should_exit();
                if should_stop.is_err() || should_stop.is_ok_and(|stop| stop) {
                    return Err(());
                }

                if let Ok(Some(file_path)) = client::get_file_path(*file_id)
                    && let Ok(file_data) = std::fs::read(&file_path)
                {
                    let callback = on_download(&file_data);
                    let tags: Vec<_> = callback.tags.into_iter().collect();
                    pending.push((*file_id, tags));
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
    }

    Ok(())
}

#[unsafe(no_mangle)]
fn on_start() {
    if let Ok(thumbnail_path) = process_thumb_location() {
        let _ = client::log_silent(format!(
            "File-Thumbnailer: Will download thumbnails to: {}",
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
        && callback.data.len() >= index
        && let Some(custom_data) = callback.data.get(index)
        && let shared_types::CallbackCustomDataReturning::U64(file_id) = custom_data
        && let Ok(Some(file_path)) = client::get_file_path(*file_id)
        && let Ok(bytes) = std::fs::read(&file_path)
    {
        let tags = on_download(&bytes).tags;
        let tags: Vec<FileTagAction> = tags.iter().cloned().collect();

        for tag_action in tags.iter() {
            for (idx, tag) in tag_action.tags.iter().enumerate() {
                if let Ok(mut thumbpath) = process_thumb_location() {
                    let thumb_hash = tag.tag.name.as_str();
                    thumbpath.push(&thumb_hash[0..2]);
                    thumbpath.push(&thumb_hash[2..4]);
                    thumbpath.push(&thumb_hash[4..6]);
                    let thpath = thumbpath.join(thumb_hash).with_added_extension("webp");
                    let pa = thpath.to_string_lossy().to_string();

                    out.insert(
                        format!("ReturnPath_{}", idx),
                        shared_types::CallbackCustomDataReturning::String(pa),
                    );
                }
            }
        }
        let _ = client::put_tags_to_file(*file_id, tags);
    }

    out
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
        path_final.push("thumbnail");

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
    let mut tags = HashSet::new();

    if let Ok(thumb) = generate_thumbnail_u8(bytes)
        && let Ok(thumbpath) = process_thumb_location()
    {
        let (thumb_path, thumb_hash) = make_thumbnail_path(&thumbpath, &thumb);
        let thpath = thumb_path
            .join(thumb_hash.clone())
            .with_added_extension("webp");
        let pa = thpath.to_string_lossy().to_string();

        match std::fs::write(&pa, thumb) {
            Ok(_) => {
                let _ = client::log_silent(format!("File-Thumbnailer: Thumbnail put at: {}", pa));
                let _ = tags.insert(FileTagAction {
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
            }
            Err(e) => {
                let _ = client::log_silent(format!("File-Thumbnailer: Got error {:?}", e));
            }
        }
    }

    CallbackReturn {
        tags,
        ..Default::default()
    }
}

fn make_img(thumb: Thumbnail) -> Vec<u8> {
    use std::io::Cursor;
    let mut buf = Cursor::new(Vec::new());
    thumb.write_webp(&mut buf).unwrap();
    buf.into_inner()
}
use image::{AnimationDecoder, DynamicImage, ImageResult, codecs::gif::GifDecoder};
pub fn extract_gif_frames(cursor: std::io::Cursor<&[u8]>) -> ImageResult<Vec<DynamicImage>> {
    // Wrap the data in a Cursor so the decoder can read it like a file

    // Create the decoder
    let decoder = GifDecoder::new(cursor)?;

    // Decode the animation into frames
    // into_frames() returns an iterator of Frame objects
    let frames = decoder.into_frames();

    // Convert each frame into a DynamicImage and collect into a vector
    frames
        .map(|f| {
            // f? handles any decoding errors per frame
            let frame = f?;
            Ok(DynamicImage::ImageRgba8(frame.into_buffer()).resize_exact(
                SIZE_THUMBNAIL_X,
                SIZE_THUMBNAIL_Y,
                image::imageops::FilterType::Lanczos3,
            ))
        })
        .collect()
}
/*
fn make_animated_img(
    filebytes: &[u8],
    fileformat: file_format::FileFormat,
    spl: VideoDefaults,
) -> Option<Vec<u8>> {
    use image::Pixel;
    use std::io::Cursor;
    let frate = 4;

    let cursor = Cursor::new(filebytes);

    let res = thumbnailer::get_video_frame_multiple(
        cursor.clone(),
        fileformat,
        spl.frames as usize,
        frate,
        Some((SIZE_THUMBNAIL_X, SIZE_THUMBNAIL_Y)),
    );
    let webpconfig = EncodingConfig {
        encoding_type: webp_animation::EncodingType::Lossy(webp_animation::LossyEncodingConfig {
            alpha_quality: 50,
            alpha_filtering: 2,
            sns_strength: 70,
            filter_strength: 100,
            preprocessing: true,
            filter_type: 1,
            pass: 10,
            ..Default::default()
        }),
        quality: 50.0,
        method: 6,
    };
    match res {
        Ok(mut ve) => {
            if ve.is_empty()
                && fileformat.extension() == "gif"
                && let Ok(frames) = extract_gif_frames(cursor)
            {
                for frame in frames {
                    ve.push(frame);
                }
            }

            use webp_animation::Encoder;
            use webp_animation::EncoderOptions;
            let mut encoder = Encoder::new_with_options(
                (SIZE_THUMBNAIL_X, SIZE_THUMBNAIL_Y),
                EncoderOptions {
                    kmin: 3,
                    kmax: 5,
                    encoding_config: Some(webpconfig),
                    ..Default::default()
                },
            )
            .unwrap();
            let mut cnt = 0;
            for each in ve {
                let mut pixelbuf =
                    Vec::with_capacity((each.width() * each.height() * 4).try_into().unwrap());
                for each in each.into_rgba8().pixels() {
                    for test in each.channels() {
                        pixelbuf.push(*test);
                    }
                }

                encoder
                    .add_frame(&pixelbuf, (cnt * frate).try_into().unwrap())
                    .unwrap();
                cnt += 1;
            }
            let out = match encoder.finalize(((cnt + 1) * frate).try_into().unwrap()) {
                Ok(out) => out,
                Err(_err) => {
                    return None;
                }
            };
            Some(out.to_vec())
        }
        Err(_err) => None,
    }
}*/

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

fn generate_thumbnail_u8(inp: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use file_format::{FileFormat, Kind};
    use image::Pixel;
    use std::io::{Error, ErrorKind};

    return Ok(create_thumbnails_dynamic(
        std::io::BufReader::new(std::io::Cursor::new(inp)),
        &ThumbnailSize::Custom((SIZE_THUMBNAIL_X, SIZE_THUMBNAIL_Y)),
    )?);
    /*
    let thumbvec = match create_thumbnails_dynamic(
        std::io::BufReader::new(std::io::Cursor::new(inp)),
        &ThumbnailSize::Custom((SIZE_THUMBNAIL_X, SIZE_THUMBNAIL_Y)),
    ) {
        Ok(t) => t,
        Err(err) => match err {
            ThumbError::Unsupported(fformat) => {
                return Err(format!(
                    "{PLUGIN_NAME} - Cannot Parse file with format: {:?}.",
                    fformat.kind()
                )
                .into());
            }
            _ => {
                return Err(format!("{PLUGIN_NAME} - Failed to match err - 190 {:?}", err).into());
            }
        },
    };
    if thumbvec.len() > 1 {
        let webpconfig = EncodingConfig {
            encoding_type: webp_animation::EncodingType::Lossy(
                webp_animation::LossyEncodingConfig {
                    alpha_quality: 50,
                    alpha_filtering: 2,
                    sns_strength: 70,
                    filter_strength: 100,
                    preprocessing: true,
                    filter_type: 1,
                    pass: 10,
                    ..Default::default()
                },
            ),
            quality: 50.0,
            method: 6,
        };
        use webp_animation::Encoder;
        use webp_animation::EncoderOptions;
        let mut encoder = Encoder::new_with_options(
            (SIZE_THUMBNAIL_X, SIZE_THUMBNAIL_Y),
            EncoderOptions {
                kmin: 3,
                kmax: 5,
                encoding_config: Some(webpconfig),
                ..Default::default()
            },
        )
        .unwrap();
        let frate = 4;
        let mut cnt = 0;
        for dyn_image in thumbvec {
            let mut pixelbuf = Vec::with_capacity(
                (dyn_image.width() * dyn_image.height() * 4)
                    .try_into()
                    .unwrap(),
            );
            for each in dyn_image.into_rgba8().pixels() {
                for test in each.channels() {
                    pixelbuf.push(*test);
                }
            }

            encoder
                .add_frame(&pixelbuf, (cnt * frate).try_into().unwrap())
                .unwrap();
            cnt += 1;
        }

        Ok(encoder
            .finalize(((cnt + 1) * frate).try_into().unwrap())
            .map(|f| f.to_vec())?)
    } else if thumbvec.len() == 1 {
        let webp = webp::Encoder::from_image(&thumbvec[0]).unwrap();
        let out = webp.encode(70.0);

        Ok(out.to_vec())
    } else {
        Err("No thumbs returned".into())
    }*/
    /*
    let thumb = &thumbvec[0];
    match thumb.return_fileformat().kind() {
        Kind::Image => match thumb.return_fileformat() {
            FileFormat::GraphicsInterchangeFormat => {
                match make_animated_img(
                    inp,
                    thumb.return_fileformat(),
                    DEFAULT_VIDEO_SETTINGS.clone(),
                ) {
                    Some(vec) => Ok(vec),
                    None => Err(Error::new(ErrorKind::Unsupported, "GIF Defuzzing failed")),
                }
            }
            _ => Ok(make_img(thumb.clone())),
        },
        Kind::Video => {
            match make_animated_img(
                inp,
                thumb.return_fileformat(),
                DEFAULT_VIDEO_SETTINGS.clone(),
            ) {
                Some(vec) => Ok(vec),
                None => Err(Error::new(ErrorKind::Unsupported, "")),
            }
        }
        _ => {
            match make_animated_img(
                inp,
                thumb.return_fileformat(),
                DEFAULT_VIDEO_SETTINGS.clone(),
            ) {
                Some(vec) => Ok(vec),
                None => Err(Error::new(ErrorKind::Unsupported, "gif is bad")),
            }
        }
    }*/
}
