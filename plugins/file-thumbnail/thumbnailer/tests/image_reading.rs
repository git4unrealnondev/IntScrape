use std::io::Cursor;

use image::{GenericImageView, ImageReader};
use thumbnailer::{ThumbnailSize, create_thumbnails_dynamic};

const PNG_BYTES: &[u8] = include_bytes!("assets/test.png");
const JPG_BYTES: &[u8] = include_bytes!("assets/test.jpg");
const WEBP_BYTES: &[u8] = include_bytes!("assets/test.webp");

#[test]
fn it_creates_a_thumbnail_for_png() {
    assert_thumbnail(PNG_BYTES, ThumbnailSize::Small);
}

#[test]
fn it_creates_a_thumbnail_for_jpeg() {
    assert_thumbnail(JPG_BYTES, ThumbnailSize::Medium);
}

#[test]
fn it_creates_a_thumbnail_for_webp() {
    assert_thumbnail(WEBP_BYTES, ThumbnailSize::Large);
}

fn assert_thumbnail(bytes: &[u8], size: ThumbnailSize) {
    let output = create_thumbnails_dynamic(Cursor::new(bytes), &size).unwrap();
    let image = ImageReader::new(Cursor::new(output))
        .with_guessed_format()
        .unwrap()
        .decode()
        .unwrap();

    assert_eq!(image.dimensions(), size.dimensions());
}
