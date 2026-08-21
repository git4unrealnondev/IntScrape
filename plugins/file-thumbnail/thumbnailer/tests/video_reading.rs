extern crate core;

use std::io::Cursor;
use thumbnailer::error::ThumbError;
use thumbnailer::{ThumbnailSize, create_thumbnails_dynamic};

const VIDEO_BYTES: &[u8] = include_bytes!("assets/test.mp4");

#[test]
fn it_creates_thumbnails_for_mp4() {
    let reader = Cursor::new(VIDEO_BYTES);
    let result = create_thumbnails_dynamic(reader, &ThumbnailSize::Medium);

    match result {
        Ok(thumbnail) => assert!(!thumbnail.is_empty()),
        Err(e) => match e {
            ThumbError::Unsupported(_) => {
                assert!(true, "ffmpeg is not installed");
            }
            e => {
                panic!("failed to create thumbnails {e}");
            }
        },
    }
}
