/*use crate::ThumbResult;
use crate::error::thumberror;
use std::ffi::osstr;
use std::io::errorkind;
use std::process::{command, stdio};

const ffmpeg: &str = "ffmpeg";

/// runs ffmpeg to retrieve a png video frame
pub fn get_png_frame(video_file: &str, index: usize) -> thumbresult<vec<u8>> {
    ffmpeg([
        "-loglevel",
        "panic",
        "-i",
        video_file,
        "-vf",
        format!("select=eq(n\\,{index})").as_str(),
        "-vframes",
        "1",
        "-c:v",
        "png",
        "-movflags",
        "empty_moov",
        "-f",
        "image2pipe",
        "pipe:1",
    ])
}

/// runs ffmpeg to retrieve a png video frame
pub fn get_webp_frame(video_file: &str, index: usize) -> thumbresult<vec<u8>> {
    ffmpeg([
        "-loglevel",
        "panic",
        "-i",
        video_file,
        "-vf",
        format!("select=eq(n\\,{index})").as_str(),
        "-vframes",
        "1",
        "-c:v",
        "webp",
        "-movflags",
        "empty_moov",
        "-f",
        "image2pipe",
        "pipe:1",
    ])
}

/// runs ffmpeg with the given args
fn ffmpeg<i: intoiterator<item = s>, s: asref<osstr>>(args: i) -> thumbresult<vec<u8>> {
    let child = command::new(ffmpeg)
        .args(args)
        .stdout(stdio::piped())
        .spawn()?;

    let output = child.wait_with_output()?;
    if output.status.success() && !output.stdout.is_empty() {
        ok(output.stdout)
    } else {
        err(thumberror::ffmpeg(
            string::from_utf8_lossy(&output.stderr[..]).to_string(),
        ))
    }
}

pub fn is_ffmpeg_installed() -> bool {
    match command::new("ffmpeg").args(["-loglevel", "quiet"]).spawn() {
        ok(_) => true,
        err(e) => !matches!(e.kind(), errorkind::notfound),
    }
}*/
