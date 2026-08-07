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
use crate::error::ThumbResult;
//use crate::formats::get_base_image;
use crate::formats::image_format::read_image;
use avio::{HardwareAccel, PixelFormat, VideoDecoder};
use file_format::{FileFormat, Kind};
use image::{DynamicImage, EncodableLayout, GenericImageView, ImageFormat};
pub use size::ThumbnailSize;
use std::io::{BufRead, Seek, Write};
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

pub fn create_thumbnails_dynamic<R: BufRead + Seek>(
    mut reader: R,
    size: &ThumbnailSize,
) -> ThumbResult<Vec<u8>> {
    let frate = 4;
    let temp2 = reader.fill_buf().unwrap();
    let mime = FileFormat::from_bytes(temp2);

    let (width, height) = size.dimensions();
    let mut frames: Vec<Vec<u8>>;

    if mime == FileFormat::GraphicsInterchangeFormat {
        let total_frames = 50;

        let mut video_file = NamedTempFile::new()?;

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        video_file.write_all(&buffer)?;

        frames = Vec::with_capacity(total_frames);
        let video_file_path = video_file.into_temp_path().keep().unwrap();
        let step = 4;

        let mut decoder = VideoDecoder::open(video_file_path)
            .output_format(PixelFormat::Rgba)
            .hardware_accel(HardwareAccel::None)
            .thread_count(4)
            .build()
            .unwrap();

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
    } else {
        frames = Vec::new();
    }

    match mime.kind() {
        Kind::Image => {
            if frames.is_empty() {
                let dyn_img = read_image(reader, mime)?;

                let image = DynamicImage::ImageRgba8(dyn_img.to_rgba8());
                let image =
                    image.resize_exact(width, height, image::imageops::FilterType::Lanczos3);
                let webp = webp::Encoder::from_image(&image).unwrap();

                return Ok(webp.encode(70.0).as_bytes().to_vec());
            }
        }
        Kind::Video | Kind::Audio => {
            let total_frames = 50;

            let mut video_file = NamedTempFile::new()?;

            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)?;
            video_file.write_all(&buffer)?;

            frames = Vec::with_capacity(total_frames);
            let video_file_path = video_file.into_temp_path().keep().unwrap();
            let step = 4;

            let mut decoder = VideoDecoder::open(video_file_path)
                .output_format(PixelFormat::Rgba)
                .hardware_accel(HardwareAccel::None)
                .thread_count(4)
                .build()
                .unwrap();

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
        }
        _ => {
            return ThumbResult::Err(error::ThumbError::Unsupported(mime));
        }
    }

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

    if frames.is_empty() {
        ThumbResult::Err(error::ThumbError::Unsupported(mime))
    } else {
        let mut last_cnt = 0;
        for (cnt, frame) in frames.iter().enumerate() {
            encoder
                .add_frame(frame, (cnt * frate).try_into().unwrap())
                .unwrap();
            last_cnt += 1;
        }
        Ok(encoder
            .finalize((last_cnt * frate).try_into().unwrap())
            .unwrap()
            .to_vec())
    }
}

/*///
/// Creates thumbnail of requestes size despite not knowing the mime.
///
pub fn create_thumbnails_unknown_type<R: BufRead + Seek, I: IntoIterator<Item = ThumbnailSize>>(
    reader: R,
    sizes: I,
) -> ThumbResult<Vec<DynamicImage>> {
    let mut temp = BufReader::new(reader);
    let mut temp1 = temp.fill_buf().unwrap();
    let le = temp1.len();
    let temp2 = &temp1[..];
    let mime = FileFormat::from_bytes(temp2);

    dbg!(&mime);

    temp1.consume(le);
    match mime.kind() {
    Kind::Image => {},
        Kind::Video => {},

    }

   /* let image = get_base_image(temp, mime)?;
    let sizes: ThumbnailSize = sizes.into_iter().collect();
    let thumbnails = resize_images(image, &sizes, FilterType::Lanczos3)
        .into_iter()
        .map(|image| Thumbnail { inner: image, mime })
        .collect();*/

    Ok(thumbnails)
}*/

fn resize_image(image: DynamicImage, size: &ThumbnailSize) -> DynamicImage {
    let (width, height) = size.dimensions();
    image.thumbnail_exact(width, height)
}
/*
///
/// Gets multiple frames from a video if they exist.
///
pub fn get_video_frame_multiple<R: BufRead + Seek>(
    mut reader: R,
    mime: FileFormat,
    total_frames: usize,       // total number of frames to get
    step: usize,               // frames between each capture
    scale: Option<(u32, u32)>, // optional resize
) -> ThumbResult<Vec<DynamicImage>> {
    use crate::error::ThumbError;
    use crate::utils::ffmpeg_cli::{get_webp_frame, is_ffmpeg_installed};
    use image::ImageFormat;
    use image::io::Reader as ImageReader;
    use std::io::Cursor;

    lazy_static::lazy_static! {
        static ref FFMPEG_INSTALLED: bool = is_ffmpeg_installed();
    }

    if !*FFMPEG_INSTALLED {
        return Err(ThumbError::Unsupported(mime));
    }

    let mut step = step.clone();

    // Write input video to temp file
    let tempdir = tempfile::tempdir()?;
    let video_path = tempdir
        .path()
        .join("video")
        .with_extension(mime.extension());

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    std::fs::write(&video_path, &buffer)?;

    let mut frames = Vec::with_capacity(total_frames);

    // Ensure at least one frame attempt
    let iterations = total_frames.max(1);

    'main: for i in 1..=iterations {
        let frame_index = i * step;

        let webp_bytes;
        'steploop: loop {
            webp_bytes = match get_webp_frame(
                video_path
                    .to_str()
                    .expect("temporary path contains invalid UTF-8"),
                frame_index,
            ) {
                Ok(bytes) => {
                    webp_bytes = bytes;

                    break 'steploop;
                }
                Err(_) => {
                    if step == 0 {
                        break 'main;
                    }
                    step -= 1;
                    continue;
                } // stop if frame extraction fails
            };
        }
        let image =
            match ImageReader::with_format(Cursor::new(webp_bytes), ImageFormat::WebP).decode() {
                Ok(img) => img,
                Err(_) => break,
            };

        let image = if let Some(size) = scale {
            resize_image(image, &ThumbnailSize::Custom(size)).clone()
        } else {
            image
        };

        frames.push(image);
    }

    tempdir.close()?;
    Ok(frames)
}*/
