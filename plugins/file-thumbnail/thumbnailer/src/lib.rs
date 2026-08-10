//! # Thumbnailer
//!
//! This crate can be used to generate thumbnails for all kinds of files.
//!
//! Example:
//! ```
//! use thumbnailer::{create_thumbnails, Thumbnail, ThumbnailSize};
//! use std::fs::File;
//! use std::io::BufReader;
//! use std::io::Cursor;
//! use file_format::FileFormat;
//! let file = File::open("tests/assets/test.png").unwrap();
//! let reader = BufReader::new(file);
//! let mut  thumbnails = create_thumbnails(reader, FileFormat::PortableNetworkGraphics, [ThumbnailSize::Small, ThumbnailSize::Medium]).unwrap();
//!
//! let thumbnail = thumbnails.pop().unwrap();
//! let mut buf = Cursor::new(Vec::new());
//! thumbnail.write_png(&mut buf).unwrap();
//! ```
use crate::error::{ThumbError, ThumbResult};
//use crate::formats::get_base_image;
use crate::formats::image_format::read_image;
use avio::{HardwareAccel, PixelFormat, VideoDecoder};
use file_format::{FileFormat, Kind};
use image::{DynamicImage, EncodableLayout, GenericImageView, ImageFormat};
pub use size::ThumbnailSize;
use std::io::{self, BufRead, ErrorKind, Seek, Write};
use std::time::Duration;
use tempfile::NamedTempFile;
use webp_animation::EncodingConfig;

pub mod error;
mod formats;
mod size;
pub(crate) mod utils;

#[derive(Clone, Debug)]
pub struct Thumbnail {
    inner: DynamicImage,
    mime: FileFormat,
}

impl Thumbnail {
    /// Writes the bytes of the image in a png format
    pub fn write_png<W: Write + Seek>(self, writer: &mut W) -> ThumbResult<()> {
        let image = DynamicImage::ImageRgba8(self.inner.into_rgba8());
        image.write_to(writer, ImageFormat::Png)?;

        Ok(())
    }

    ///
    /// Gives internal dynamic image object
    ///
    pub fn return_dynamic_img(self) -> DynamicImage {
        self.inner
    }

    /// Writes the bytes of the image in a jpeg format
    pub fn write_jpeg<W: Write + Seek>(self, writer: &mut W, quality: u8) -> ThumbResult<()> {
        let image = DynamicImage::ImageRgb8(self.inner.into_rgb8());
        let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(writer, quality);
        encoder.encode_image(&image)?;

        Ok(())
    }
    /// Writes the bytes of the image in a webp format
    #[cfg(feature = "webp")]
    pub fn write_webp<W: Write + Seek>(self, writer: &mut W) -> ThumbResult<()> {
        use image::EncodableLayout;
        use webp;
        let image = DynamicImage::ImageRgba8(self.inner.into_rgba8());
        let webp = webp::Encoder::from_image(&image).unwrap();
        let out = webp.encode(70.0);
        writer.write_all(out.as_bytes()).unwrap();
        Ok(())
    }

    /// Returns the fileformat that it's parsed
    pub fn return_fileformat(&self) -> FileFormat {
        self.mime
    }

    /// Returns the size of the thumbnail as width,  height
    pub fn size(&self) -> (u32, u32) {
        self.inner.dimensions()
    }
}

enum Format {
    Image,
    Video,
    Other,
}

pub fn create_thumbnails_dynamic<R: BufRead + Seek>(
    mut reader: R,
    size: &ThumbnailSize,
) -> ThumbResult<Vec<u8>> {
    let frate = 4;
    let temp2 = reader.fill_buf().unwrap();
    let mime = FileFormat::from_bytes(temp2);

    let (width, height) = size.dimensions();

    let mut frames: Vec<Vec<u8>> = Vec::new();

    let mut format;

    // sets bulk formats
    match mime.kind() {
        Kind::Image => {
            format = Format::Image;
        }
        Kind::Video => {
            format = Format::Video;
        }
        _ => {
            format = Format::Other;
        }
    }

    // Overrides weird format
    match mime {
        FileFormat::GraphicsInterchangeFormat
        | FileFormat::Mpeg4Part14
        | FileFormat::Mpeg4Part14Audio
        | FileFormat::Mpeg4Part14Subtitles => {
            format = Format::Video;
        }

        _ => {}
    }

    match format {
        Format::Image => {
            if frames.is_empty() {
                let dyn_img = read_image(reader, mime)?;

                let image = DynamicImage::ImageRgba8(dyn_img.to_rgba8());
                let image =
                    image.resize_exact(width, height, image::imageops::FilterType::Lanczos3);
                let webp = webp::Encoder::from_image(&image).unwrap();

                return Ok(webp.encode(70.0).as_bytes().to_vec());
            }
        }
        Format::Video => {
            let total_frames = 50;

            let mut video_file = NamedTempFile::new()?;

            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)?;
            video_file.write_all(&buffer)?;

            frames = Vec::with_capacity(total_frames);
            let video_file_path = video_file.into_temp_path().keep().unwrap();
            let step = 4;

            let mut decoder = VideoDecoder::open(&video_file_path)
                .output_format(PixelFormat::Rgba)
                .hardware_accel(HardwareAccel::None)
                .thread_count(4)
                .build()
                .map_err(|error| {
                    ThumbError::IO(io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "Failed to load video at {}: {}",
                            video_file_path.display(),
                            error
                        ),
                    ))
                })?;

            let time_of_one_frame = 1.0 / decoder.frame_rate();

            for i in 1..=total_frames.max(1) {
                let frame_index = i * step;
                //frames_to_get.push(frame_index);
                let frame_seek = frame_index as f64 * time_of_one_frame * 1000.0;

                if let Ok(Some(frame)) = decoder.thumbnail_at_exact(
                    Duration::from_millis(frame_seek.floor() as u64),
                    width,
                    height,
                ) {
                    frames.push(frame.data());
                }
            }
            if frames.is_empty() 
                && let Ok(Some(frame)) = decoder.thumbnail_at_exact(
                    Duration::from_millis(0),
                    width,
                    height,
                ) {
                    frames.push(frame.data());
                
            }
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
                (width, height),
                EncoderOptions {
                    kmin: 3,
                    kmax: 5,
                    encoding_config: Some(webpconfig),
                    ..Default::default()
                },
            )
            .unwrap();

            if !frames.is_empty() {
                let mut last_cnt = 0;
                for (cnt, frame) in frames.iter().enumerate() {
                    encoder
                        .add_frame(frame, (cnt * frate).try_into().unwrap())
                        .unwrap();
                    last_cnt += 1;
                }
                return Ok(encoder
                    .finalize((last_cnt * frate).try_into().unwrap())
                    .unwrap()
                    .to_vec());
            }
        }

        Format::Other => {}
    }
    ThumbResult::Err(error::ThumbError::Unsupported(mime))
}
