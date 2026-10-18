//! # Thumbnailer
//!
//! This crate can be used to generate thumbnails for all kinds of files.
//!
//! Example:
//! ```
//! use thumbnailer::{create_thumbnails_dynamic, ThumbnailSize};
//! use std::fs::File;
//! use std::io::BufReader;
//! let file = File::open("tests/assets/test.png").unwrap();
//! let reader = BufReader::new(file);
//! let thumbnail = create_thumbnails_dynamic(reader, &ThumbnailSize::Small).unwrap();
//! ```
use crate::error::{ThumbError, ThumbResult};
//use crate::formats::get_base_image;
use crate::formats::image_format::read_image;
use avio::{HardwareAccel, PixelFormat, VideoDecoder};
use file_format::{FileFormat, Kind};
use image::{DynamicImage, EncodableLayout, GenericImageView, ImageFormat};
pub use size::ThumbnailSize;
use std::{
    io::{self, BufRead, ErrorKind, Seek, Write},
    path::Path,
    sync::Once,
    time::Duration,
};
use tempfile::NamedTempFile;
use webp_animation::EncodingConfig;

pub mod error;
mod formats;
mod size;
pub(crate) mod utils;

static QUIET_FFMPEG_LOGGING: Once = Once::new();

fn suppress_ffmpeg_logging() {
    QUIET_FFMPEG_LOGGING.call_once(|| unsafe {
        ff_sys::av_log_set_level(ff_sys::AV_LOG_QUIET);
    });
}

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
    create_thumbnails_dynamic_in(&mut reader, size, Path::new("/tmp"))
}

fn create_thumbnails_dynamic_in<R: BufRead + Seek>(
    mut reader: R,
    size: &ThumbnailSize,
    temp_dir: &Path,
) -> ThumbResult<Vec<u8>> {
    suppress_ffmpeg_logging();

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

            let mut video_file = NamedTempFile::new_in(temp_dir)?;

            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)?;
            video_file.write_all(&buffer)?;

            frames = Vec::with_capacity(total_frames);
            let video_file_path = video_file.path().to_path_buf();
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
                && let Ok(Some(frame)) =
                    decoder.thumbnail_at_exact(Duration::from_millis(0), width, height)
            {
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
                        // libwebp's default, and previously 10 here. Do not
                        // raise this without reading this first.
                        //
                        // `pass` is NOT a quality or effort knob. It is how many
                        // rate-distortion refinement passes libwebp runs while
                        // searching for a quantiser step `q` that hits a size or
                        // PSNR *target*. It only does anything alongside
                        // `target_size` or `target_psnr`.
                        //
                        // Both are left at 0 here, so `webp_enc.c` sets
                        // `do_search_ = 0`, and `ComputeNextQ` -- the only code
                        // that advances `q` -- sits behind `if (do_search)` in
                        // both pass loops (frame_enc.c). So the loops cannot
                        // converge and never set `is_last_pass`: they just run
                        // the full macroblock decimate/tokenise/quantise walk
                        // `pass` times over identical pixels at an identical
                        // `q`. At pass=10, nine of those passes were pure waste,
                        // and pass alone dominated the cost.
                        //
                        // Measured: one 250x250 frame took 533 ms at pass=10 vs
                        // 123 ms at pass=1 (4.3x). Over the 50 frames of an
                        // animated thumbnail that is ~24 s vs ~3 s, and the
                        // encode was 94.6% of the whole video-thumbnail cost.
                        // `webp_knob_bench` also shows threading, sns_strength,
                        // filter_strength and preprocessing are each within 3%
                        // here, i.e. noise. Output size is unaffected in
                        // practice (the animation came out 0.12% *smaller*).
                        //
                        // If you ever want a hard byte target, set
                        // `target_size` and *then* a higher pass is meaningful.
                        pass: 1,
                        ..Default::default()
                    },
                ),
                quality: 50.0,
                // Note: `webp_animation`'s `apply_to` never copies `method` onto
                // the libwebp config, so this has no effect -- pass=4 (the
                // `WebPConfigInit` default) is what actually runs. Measured
                // identical output bytes at method 4 and method 6 in
                // `webp_knob_bench`. Left as-is to keep this change to the one
                // setting that does anything.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::Instant;

    /// Isolates which libwebp knob makes a single 250x250 frame expensive.
    ///
    /// The plugin's `EncodingConfig` reaches libwebp through `webp_animation`,
    /// whose `apply_to` copies `pass` but never `method` or `thread_level`. So
    /// what actually runs is lossy / quality 50 / **method 4 (the
    /// `WebPConfigInit` default)** / **pass 10** / **thread_level 0
    /// (single-threaded)** — the plugin's `method: 6` is dead config.
    ///
    /// Each knob is measured against that baseline, and `thread_level` also
    /// compares output bytes, since that is the only candidate that could be free.
    #[test]
    #[ignore = "manual benchmark: which webp knob dominates a 250x250 frame"]
    fn webp_knob_bench() {
        const W: u32 = 250;
        const H: u32 = 250;
        const RUNS: usize = 5;

        // Deterministic pseudo-noise, standing in for a decoded video frame.
        let mut pixels = vec![0u8; (W * H * 4) as usize];
        let mut state: u32 = 0x1234_5678;
        for byte in pixels.iter_mut() {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            *byte = (state >> 16) as u8;
        }

        // Mirrors LossyEncodingConfig::apply_to for the plugin's values.
        let base = |cfg: &mut webp::WebPConfig| {
            cfg.lossless = 0;
            cfg.quality = 50.0;
            cfg.segments = 1;
            cfg.sns_strength = 70;
            cfg.filter_strength = 100;
            cfg.filter_sharpness = 0;
            cfg.filter_type = 1;
            cfg.preprocessing = 1;
            cfg.pass = 10;
            cfg.alpha_compression = 1;
            cfg.alpha_filtering = 2;
            cfg.alpha_quality = 50;
        };

        let variants: Vec<(&str, Box<dyn Fn(&mut webp::WebPConfig)>)> = vec![
            ("baseline (pass=10, thread=0)", Box::new(|_| {})),
            (
                "thread_level=1",
                Box::new(|c: &mut webp::WebPConfig| c.thread_level = 1),
            ),
            (
                "thread_level=2",
                Box::new(|c: &mut webp::WebPConfig| c.thread_level = 2),
            ),
            ("pass=1", Box::new(|c: &mut webp::WebPConfig| c.pass = 1)),
            (
                "method=6",
                Box::new(|c: &mut webp::WebPConfig| c.method = 6),
            ),
            (
                "method=4 (already the default)",
                Box::new(|c: &mut webp::WebPConfig| c.method = 4),
            ),
            (
                "sns_strength=0",
                Box::new(|c: &mut webp::WebPConfig| c.sns_strength = 0),
            ),
            (
                "filter_strength=0",
                Box::new(|c: &mut webp::WebPConfig| c.filter_strength = 0),
            ),
            (
                "preprocessing=0",
                Box::new(|c: &mut webp::WebPConfig| c.preprocessing = 0),
            ),
            (
                "thread=1 + pass=1",
                Box::new(|c: &mut webp::WebPConfig| {
                    c.thread_level = 1;
                    c.pass = 1;
                }),
            ),
        ];

        let image = webp::Encoder::from_rgba(&pixels, W, H);
        println!("\nBENCH webp knobs, one {W}x{H} frame, lossy quality 50 (x{RUNS} runs)");

        let mut baseline: Option<(f64, Vec<u8>)> = None;
        for (label, tweak) in &variants {
            let mut cfg = webp::WebPConfig::new().unwrap();
            base(&mut cfg);
            tweak(&mut cfg);

            let t = Instant::now();
            let mut out = Vec::new();
            let mut failure = None;
            for _ in 0..RUNS {
                match image.encode_advanced(&cfg) {
                    Ok(memory) => out = memory.to_vec(),
                    Err(error) => {
                        failure = Some(format!("{error:?}"));
                        break;
                    }
                }
            }
            if let Some(error) = failure {
                println!("  {label:<32} rejected by libwebp: {error}");
                continue;
            }
            let ms = t.elapsed().as_secs_f64() * 1_000.0 / RUNS as f64;

            let note = match &baseline {
                None => {
                    baseline = Some((ms, out.clone()));
                    String::from("(baseline)")
                }
                Some((bms, bbytes)) => {
                    let same = if bbytes == &out {
                        "identical bytes"
                    } else {
                        "DIFFERENT bytes"
                    };
                    format!("{:>6.2}x  {same}", ms / bms)
                }
            };
            println!("  {label:<32} {ms:>8.1} ms  {note}  ({} bytes)", out.len());
        }
    }

    /// Splits the video thumbnail pipeline into extraction and encoding, to see
    /// which half actually costs.
    ///
    /// This is the diagnostic for "is the webp encoding pretty quick?": if the
    /// encode is a small share then frame extraction is the thing to optimize,
    /// and vice versa. Uses exactly the sizes and config the plugin uses
    /// (50 frames, 250x250, the same EncodingConfig).
    #[test]
    #[ignore = "manual benchmark: video thumbnail extraction vs encoding split"]
    fn video_pipeline_split_bench() {
        let bytes = include_bytes!("../tests/assets/test.mp4");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("video.mp4");
        std::fs::write(&path, bytes).unwrap();

        const W: u32 = 250;
        const H: u32 = 250;
        const TOTAL: u64 = 50;
        const STEP: u64 = 4;

        let mut decoder = VideoDecoder::open(&path)
            .output_format(PixelFormat::Rgba)
            .hardware_accel(HardwareAccel::None)
            .thread_count(4)
            .build()
            .unwrap();
        let time_of_one_frame = 1.0 / decoder.frame_rate();
        let targets: Vec<Duration> = (1..=TOTAL)
            .map(|i| {
                Duration::from_millis(
                    ((i * STEP) as f64 * time_of_one_frame * 1000.0).floor() as u64
                )
            })
            .collect();

        // --- extraction, exactly as the plugin does it ---
        let t = Instant::now();
        let mut frames: Vec<Vec<u8>> = Vec::with_capacity(TOTAL as usize);
        for target in &targets {
            if let Ok(Some(frame)) = decoder.thumbnail_at_exact(*target, W, H) {
                frames.push(frame.data());
            }
        }
        let extract = t.elapsed();

        // --- encoding, exactly the plugin's config ---
        let webpconfig = EncodingConfig {
            encoding_type: webp_animation::EncodingType::Lossy(
                webp_animation::LossyEncodingConfig {
                    alpha_quality: 50,
                    alpha_filtering: 2,
                    sns_strength: 70,
                    filter_strength: 100,
                    preprocessing: true,
                    filter_type: 1,
                    // Mirrors the production config above; was 10 before the
                    // pass fix, which is why this bench has to be kept in step.
                    pass: 1,
                    ..Default::default()
                },
            ),
            quality: 50.0,
            method: 6,
        };
        use webp_animation::{Encoder, EncoderOptions};
        let t = Instant::now();
        let mut encoder = Encoder::new_with_options(
            (W, H),
            EncoderOptions {
                kmin: 3,
                kmax: 5,
                encoding_config: Some(webpconfig),
                ..Default::default()
            },
        )
        .unwrap();
        for (cnt, frame) in frames.iter().enumerate() {
            encoder
                .add_frame(frame, (cnt * 4).try_into().unwrap())
                .unwrap();
        }
        let out = encoder
            .finalize((frames.len() * 4).try_into().unwrap())
            .unwrap();
        let encode = t.elapsed();

        let total = extract + encode;
        println!(
            "\nBENCH video thumbnail pipeline split ({} frames @ {W}x{H})",
            frames.len()
        );
        println!(
            "  extraction (50x thumbnail_at_exact): {:>8.1} ms  ({:>4.1}%)",
            extract.as_secs_f64() * 1_000.0,
            extract.as_secs_f64() / total.as_secs_f64() * 100.0
        );
        println!(
            "  encoding   (animated webp):        {:>8.1} ms  ({:>4.1}%)",
            encode.as_secs_f64() * 1_000.0,
            encode.as_secs_f64() / total.as_secs_f64() * 100.0
        );
        println!(
            "  total:                              {:>8.1} ms  -> {} bytes webp",
            total.as_secs_f64() * 1_000.0,
            out.len()
        );
    }

    /// Times the video thumbnail path, which is what every redgifs download
    /// goes through: `on_download` hands the whole file to
    /// `create_thumbnails_dynamic`, and for `Kind::Video` that seeks and decodes
    /// 50 separate frames before encoding them all as an animated webp.
    ///
    /// Manual benchmark because it needs ffmpeg and takes seconds per run.
    #[test]
    #[ignore = "manual benchmark: video thumbnail cost per file"]
    fn video_thumbnail_cost_bench() {
        let bytes = include_bytes!("../tests/assets/test.mp4");
        println!(
            "test.mp4: {} bytes, format {:?}",
            bytes.len(),
            FileFormat::from_bytes(&bytes[..])
        );

        // Warm up once so process/library init is not counted as per-file cost.
        let t = Instant::now();
        let first = create_thumbnails_dynamic(
            Cursor::new(bytes.to_vec()),
            &ThumbnailSize::Custom((32, 32)),
        );
        println!(
            "  run 1 (incl. ffmpeg init): {:>8.1} ms  -> {:?}",
            t.elapsed().as_secs_f64() * 1_000.0,
            first.as_ref().map(|v| v.len()).map_err(|e| e.to_string())
        );

        for index in 2..=4 {
            let t = Instant::now();
            let out = create_thumbnails_dynamic(
                Cursor::new(bytes.to_vec()),
                &ThumbnailSize::Custom((32, 32)),
            );
            println!(
                "  run {index} @32x32: {:>8.1} ms  -> {} byte webp",
                t.elapsed().as_secs_f64() * 1_000.0,
                out.map(|v| v.len()).unwrap_or(0)
            );
        }

        // The size the plugin actually asks for.
        let t = Instant::now();
        let full = create_thumbnails_dynamic(
            Cursor::new(bytes.to_vec()),
            &ThumbnailSize::Custom((250, 250)),
        );
        println!(
            "  @250x250 (what the plugin uses): {:>8.1} ms  -> {} byte webp",
            t.elapsed().as_secs_f64() * 1_000.0,
            full.as_ref().map(|v| v.len()).unwrap_or(0)
        );

        // The same work on an image, for contrast: the video path is the one
        // that runs 50 frame seeks.
        let png = include_bytes!("../tests/assets/test.png");
        let t = Instant::now();
        let _ =
            create_thumbnails_dynamic(Cursor::new(png.to_vec()), &ThumbnailSize::Custom((32, 32)));
        println!(
            "  image ({} bytes) @32x32, for contrast: {:>8.1} ms",
            png.len(),
            t.elapsed().as_secs_f64() * 1_000.0
        );
    }

    #[test]
    fn video_temp_file_is_removed_after_processing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = create_thumbnails_dynamic_in(
            Cursor::new(include_bytes!("../tests/assets/test.mp4")),
            &ThumbnailSize::Custom((32, 32)),
            temp_dir.path(),
        );

        assert!(result.is_ok() || matches!(result, Err(ThumbError::Unsupported(_))));
        assert_eq!(std::fs::read_dir(temp_dir.path()).unwrap().count(), 0);
    }
}
