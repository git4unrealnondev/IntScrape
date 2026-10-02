//! Choosing which media variant to download.
//!
//! `gif.urls` exposes several variants and they are *not* consistently present:
//! `sd` and `silent` are missing on many clips, `hd` is the only one reliably
//! there. Picking a fixed field would silently drop files, so selection walks a
//! preference order and takes the first variant that exists.

/// Preference order, best first.
///
/// * `hd` — full resolution. The default target.
/// * `sd` — mobile mp4. Only a fallback; it is genuinely smaller.
/// * `silent` — audio removed. Preferred when `silent` mode is enabled, since
///   it keeps full resolution while dropping the soundtrack.
pub const DEFAULT_PREFERENCE: &[&str] = &["hd", "sd", "silent"];

/// The poster still, used only when no video variant exists at all.
pub const STILL_VARIANT: &str = "poster";

/// Byte width of the ellipsis appended to a truncated text value. Callers that
/// re-check a truncated value need it to set the correct ceiling.
pub const ELLIPSIS_LEN: usize = '\u{2026}'.len_utf8();

/// A variant we are willing to download.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Variant {
    pub name: &'static str,
    pub url: String,
    pub has_audio: bool,
}

/// Picks the best available variant from a `urls` object.
///
/// `has_audio` comes from the gif's own `hasAudio` flag, but `silent` is by
/// definition muted regardless of that flag, so it is forced off here.
///
/// Returns `None` when the gif exposes no usable variant, which is the signal
/// to skip the entry rather than emit a file that cannot be downloaded.
pub fn select_variant(
    urls: &json::JsonValue,
    prefer_silent: bool,
    allow_still: bool,
    has_audio: bool,
) -> Option<Variant> {
    let mut order: Vec<&str> = Vec::with_capacity(DEFAULT_PREFERENCE.len() + 1);
    if prefer_silent {
        order.push("silent");
    }
    order.extend_from_slice(DEFAULT_PREFERENCE);

    for name in order {
        let Some(url) = urls[name].as_str() else {
            continue;
        };
        if url.is_empty() {
            continue;
        }
        return Some(Variant {
            name,
            url: url.to_string(),
            has_audio: has_audio && name != "silent",
        });
    }

    if allow_still
        && let Some(url) = urls[STILL_VARIANT].as_str()
        && !url.is_empty()
    {
        return Some(Variant {
            name: STILL_VARIANT,
            url: url.to_string(),
            has_audio: false,
        });
    }

    None
}

/// Extracts the extension the host should store for `url`.
///
/// Redgifs media urls are template based (`<Name>.mp4`, `<Name>-poster.jpg`), so
/// the extension is always the path's last segment. The extension is not
/// trusted for content type -- the host detects the format from the downloaded
/// bytes -- this only names the on-disk file.
pub fn extension_for(url: &str) -> Option<String> {
    let path = url.split(['?', '#']).next()?;
    let name = path.rsplit('/').next()?;
    let (_, ext) = name.rsplit_once('.')?;
    let ext = ext.trim().to_ascii_lowercase();
    if ext.is_empty() || ext.len() > 5 || !ext.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(ext)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> json::JsonValue {
        json::parse(input).unwrap()
    }

    #[test]
    fn prefers_hd() {
        let urls = parse(
            r#"{"hd": "https://media.redgifs.com/A.mp4",
                "sd": "https://media.redgifs.com/A-mobile.mp4",
                "silent": "https://media.redgifs.com/A-silent.mp4"}"#,
        );
        let variant = select_variant(&urls, false, false, true).unwrap();
        assert_eq!(variant.name, "hd");
        assert_eq!(variant.url, "https://media.redgifs.com/A.mp4");
        assert!(variant.has_audio);
    }

    #[test]
    fn falls_back_when_hd_is_missing() {
        let urls = parse(r#"{"sd": "https://media.redgifs.com/A-mobile.mp4"}"#);
        assert_eq!(
            select_variant(&urls, false, false, true).unwrap().name,
            "sd"
        );
    }

    #[test]
    fn silent_mode_prefers_silent_and_clears_audio() {
        let urls = parse(
            r#"{"hd": "https://media.redgifs.com/A.mp4",
                "silent": "https://media.redgifs.com/A-silent.mp4"}"#,
        );
        let variant = select_variant(&urls, true, false, true).unwrap();
        assert_eq!(variant.name, "silent");
        assert!(!variant.has_audio, "silent is muted by definition");
    }

    #[test]
    fn poster_is_only_used_when_allowed_and_needed() {
        let urls = parse(
            r#"{"poster": "https://media.redgifs.com/A-poster.jpg",
                "thumbnail": "https://media.redgifs.com/A-mobile.jpg"}"#,
        );
        assert!(
            select_variant(&urls, false, false, true).is_none(),
            "with stills disallowed a still-only gif must be skipped, not faked"
        );
        let still = select_variant(&urls, false, true, false).unwrap();
        assert_eq!(still.name, STILL_VARIANT);
        assert!(!still.has_audio);
    }

    #[test]
    fn empty_urls_yield_nothing() {
        assert!(select_variant(&parse("{}"), false, true, true).is_none());
        assert!(select_variant(&parse(r#"{"hd": ""}"#), false, true, true).is_none());
    }

    #[test]
    fn reads_extension_from_the_path() {
        assert_eq!(
            extension_for("https://media.redgifs.com/A.mp4").as_deref(),
            Some("mp4")
        );
        assert_eq!(
            extension_for("https://media.redgifs.com/A-poster.jpg?v=2").as_deref(),
            Some("jpg")
        );
        assert_eq!(
            extension_for("https://media.redgifs.com/A").as_deref(),
            None
        );
        assert_eq!(
            extension_for("https://media.redgifs.com/A.").as_deref(),
            None
        );
        assert_eq!(
            extension_for("https://media.redgifs.com/a.b/c").as_deref(),
            None
        );
    }
}
