use std::io::Cursor;

use image::{GenericImageView, ImageReader};
use thumbnailer::{ThumbnailSize, create_thumbnails_dynamic};

const PNG_BYTES: &[u8] = include_bytes!("assets/test.png");
const JPG_BYTES: &[u8] = include_bytes!("assets/test.jpg");
const WEBP_BYTES: &[u8] = include_bytes!("assets/test.webp");

#[test]
fn generated_thumbnail_is_valid_webp_for_each_supported_image() {
    for bytes in [PNG_BYTES, JPG_BYTES, WEBP_BYTES] {
        let output =
            create_thumbnails_dynamic(Cursor::new(bytes), &ThumbnailSize::Custom((96, 64)))
                .unwrap();
        let image = ImageReader::new(Cursor::new(output))
            .with_guessed_format()
            .unwrap()
            .decode()
            .unwrap();

        assert_eq!(image.dimensions(), (96, 64));
    }
}
