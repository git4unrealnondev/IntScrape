//! Turning Redgifs json objects into files, tags and follow-up jobs.
//!
//! # Relationship model
//!
//! `Parents` rows carry the only two structural relations the site has:
//!
//! ```text
//! RedgifsGif(id) -> RedgifsUser(name)      gif uploaded by user
//! RedgifsGif(id) -> RedgifsGallery(uuid)   gif is one entry of a gallery
//! ```
//!
//! Every other tag is a value scoped to its gif via `limit_to`, which is what
//! keeps the shared tags (`RedgifsTag("Anal")` appears on thousands of gifs)
//! from collapsing into one anonymous row.
//!
//! Media files repeat those tags in their own `tag_list`, so a downloaded file
//! is searchable by its tags, uploader and gallery.

use std::collections::HashSet;

use shared_types::{
    FileObject, FileSource, FileTagAction, PluginTag, ScraperDataReturn, SkipIf, TagOperation,
};

use crate::limits::{
    MAX_GALLERY_MEMBERS, MAX_ID_LEN, MAX_TEXT_LEN, MAX_URL_LEN, MAX_VALUE_TAGS_PER_GIF,
};
use crate::media::{ELLIPSIS_LEN, extension_for, select_variant};
use crate::namespaces::{Ns, related, scoped};

/// Everything one page contributed.
#[derive(Default)]
pub struct Harvest {
    pub files: HashSet<FileObject>,
    pub tags: HashSet<PluginTag>,
    pub jobs: HashSet<ScraperDataReturn>,
}

impl Harvest {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty() && self.tags.is_empty() && self.jobs.is_empty()
    }
}

/// Runtime knobs from the database settings.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Prefer the audio-removed variant when one exists.
    pub prefer_silent: bool,
    /// Fall back to the poster still when no video variant exists.
    pub allow_still: bool,
    /// Re-request every gallery found, even when its gifs are already held.
    pub refresh_galleries: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            prefer_silent: false,
            allow_still: false,
            refresh_galleries: false,
        }
    }
}

/// The sanitised id of one gif.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GifId(String);

impl GifId {
    /// Validates an id before it reaches the database or a url.
    ///
    /// Redgifs ids are lowercase alphanumerics, occasionally with a trailing
    /// hyphen. Anything else is rejected rather than interpolated, since these
    /// ids become url path segments and tag names.
    pub fn parse(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_ID_LEN {
            return None;
        }
        if !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            return None;
        }
        Some(Self(trimmed.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A gallery uuid, validated the same way.
pub fn gallery_id(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_ID_LEN {
        return None;
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return None;
    }
    Some(trimmed.to_string())
}

/// Parses one gif object into a file plus its structural tags.
pub fn gif_into(gif: &json::JsonValue, options: &Options) -> Option<(FileObject, Vec<PluginTag>)> {
    let id = GifId::parse(gif["id"].as_str()?)?;
    let has_audio = gif["hasAudio"].as_bool().unwrap_or(false);
    let urls = &gif["urls"];

    let variant = select_variant(urls, options.prefer_silent, options.allow_still, has_audio)?;
    if variant.url.len() > MAX_URL_LEN {
        return None;
    }

    // Validated here so an unnameable url is skipped before it becomes a file;
    // the host re-derives the extension from the downloaded bytes.
    extension_for(&variant.url)?;

    let gallery = gallery_id(gif["gallery"].as_str().unwrap_or_default());

    // The gif relates to whoever uploaded it, and to its gallery when it has
    // siblings. The gallery relation is *optional*: most gifs are standalone,
    // and propagating that absence as an error silently dropped every one of
    // them.
    let mut structural: Vec<PluginTag> =
        vec![related(Ns::Gif, id.as_str(), Ns::User.tag(user_of(gif)))];
    if let Some(gallery) = &gallery {
        structural.push(related(
            Ns::Gif,
            id.as_str(),
            Ns::Gallery.tag(gallery.clone()),
        ));
    }

    // Files carry the identity tags plus the values, so a downloaded file is
    // searchable through them.
    let uploader = user_of(gif);
    let mut file_tags = vec![
        related(Ns::Gif, id.as_str(), Ns::User.tag(uploader.clone())),
        // Mirror of the relation above: the username becomes a tag the file
        // carries, so a plain `RedgifsUser:<name>` search returns their uploads.
        related(Ns::User, uploader.clone(), Ns::Gif.tag(id.as_str())),
        scoped(Ns::MediaUrl, variant.url.clone(), Ns::Gif.tag(id.as_str())),
        scoped(
            Ns::Format,
            variant.name.to_string(),
            Ns::Gif.tag(id.as_str()),
        ),
        scoped(
            Ns::HasAudio,
            variant.has_audio.to_string(),
            Ns::Gif.tag(id.as_str()),
        ),
    ];

    if let Some(embed) = urls["html"].as_str()
        && embed.len() <= MAX_URL_LEN
    {
        file_tags.push(scoped(
            Ns::EmbedUrl,
            embed.to_string(),
            Ns::Gif.tag(id.as_str()),
        ));
    }

    file_tags.extend(value_tags(gif, &id));

    // The mirror of the gallery relation above, so the gallery name itself is a
    // tag a file can carry and `RedgifsGallery:<uuid>` finds its members.
    if let Some(gallery) = &gallery {
        file_tags.push(related(
            Ns::Gallery,
            gallery.clone(),
            Ns::Gif.tag(id.as_str()),
        ));
    }

    // The file repeats every tag so a downloaded file is searchable through its
    // uploader, gallery, tags and values.
    structural.extend(file_tags.clone());

    let file = FileObject {
        source: Some(FileSource::Url(variant.url)),
        tag_list: vec![FileTagAction {
            operation: TagOperation::Add,
            tags: file_tags,
        }],
        // Redgifs serves no upstream hash, so dedup is by media url, which the
        // host resolves through `source_url_files_get`.
        ..Default::default()
    };

    Some((file, structural))
}

/// Builds the follow-up jobs a page implies.
///
/// Each entry that references a gallery gets a gallery job. A gallery listing
/// entry is not itself downloadable, so this is the only way its siblings are
/// ever collected.
pub fn gallery_jobs(gifs: &[&json::JsonValue], options: &Options) -> Vec<ScraperDataReturn> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Vec::new();

    for gif in gifs {
        let Some(gal) = gallery_id(gif["gallery"].as_str().unwrap_or_default()) else {
            continue;
        };
        if !seen.insert(gal.clone()) {
            continue;
        }

        let skip_conditions = if options.refresh_galleries {
            Vec::new()
        } else {
            // Skip when any member of this gallery is already held. Redgifs
            // reports gallery size on the listing entry as `folders`/`count`
            // inconsistently, so the first member is the reliable probe.
            let probe = gif["id"]
                .as_str()
                .and_then(GifId::parse)
                .map(|id| id.as_str().to_string());

            match probe {
                Some(probe) => vec![SkipIf::FileTagRelationship(Ns::Gif.tag(probe))],
                None => Vec::new(),
            }
        };

        out.push(ScraperDataReturn {
            job: crate::request::job_for_gallery(&gal, Vec::new()).job,
            skip_conditions,
        });
    }

    out
}

/// Truncates a free-text field to `MAX_TEXT_LEN`, dropping blanks.
fn bounded_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.len() <= MAX_TEXT_LEN {
        return Some(trimmed.to_string());
    }
    let mut end = MAX_TEXT_LEN;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = trimmed[..end].trim_end().to_string();
    out.push('\u{2026}');
    Some(out)
}

/// The uploader, or `"unknown"` when Redgifs reports no username.
///
/// The user relation is always emitted, so an anonymous upload still lands on a
/// single shared `"unknown"` user rather than creating a tag per missing name.
fn user_of(gif: &json::JsonValue) -> String {
    let name = gif["userName"].as_str().unwrap_or_default().trim();
    if name.is_empty() {
        "unknown".to_string()
    } else {
        name.to_string()
    }
}

/// Emits every value tag for one gif, bounded by `MAX_VALUE_TAGS_PER_GIF`.
///
/// The count is a real risk: Redgifs tags are not unique per gif and the
/// observed maximum was well into the dozens, so the per-gif budget stops one
/// pathological gif from dominating a page.
fn value_tags(gif: &json::JsonValue, id: &GifId) -> Vec<PluginTag> {
    let mut out: Vec<PluginTag> = Vec::with_capacity(16);
    let gif_tag = Ns::Gif.tag(id.as_str());

    // `bounded_text` truncates to MAX_TEXT_LEN and appends a U+2026 ellipsis,
    // so the post-truncation ceiling is that plus the ellipsis width. Comparing
    // against MAX_TEXT_LEN here would reject every value that had just been
    // truncated -- i.e. exactly the over-long ones the cap exists for.
    let push = |ns: Ns, value: String, out: &mut Vec<PluginTag>| {
        if value.is_empty() || value.len() > MAX_TEXT_LEN + ELLIPSIS_LEN {
            return;
        }
        out.push(scoped(ns, value, gif_tag.clone()));
    };

    if let Some(description) = gif["description"].as_str().and_then(bounded_text) {
        push(Ns::Description, description, &mut out);
    }

    for tag in gif["tags"].members() {
        if out.len() >= MAX_VALUE_TAGS_PER_GIF {
            break;
        }
        if let Some(tag) = tag.as_str()
            && let Some(tag) = bounded_text(tag)
        {
            push(Ns::Tag, tag, &mut out);
        }
    }

    for niche in gif["niches"].members() {
        if out.len() >= MAX_VALUE_TAGS_PER_GIF {
            break;
        }
        if let Some(niche) = niche.as_str()
            && let Some(niche) = bounded_text(niche)
        {
            push(Ns::Niche, niche, &mut out);
        }
    }

    // Counters. Stored as text because they change on every scrape and a
    // numeric tag would churn; the value is a snapshot, not an identity.
    if let Some(value) = gif["views"].as_u64() {
        push(Ns::Views, value.to_string(), &mut out);
    }
    if let Some(value) = gif["likes"].as_u64() {
        push(Ns::Likes, value.to_string(), &mut out);
    }
    if let Some(value) = gif["dislikes"].as_u64() {
        push(Ns::Dislikes, value.to_string(), &mut out);
    }
    if let Some(created) = gif["createDate"].as_u64() {
        push(Ns::CreatedUtc, created.to_string(), &mut out);
    }
    if let (Some(width), Some(height)) = (gif["width"].as_u64(), gif["height"].as_u64()) {
        if width > 0 && height > 0 && width < 100_000 && height < 100_000 {
            push(Ns::Dimensions, format!("{width}x{height}"), &mut out);
        }
    }
    match gif["duration"].as_u64() {
        Some(duration) if duration < 86_400 => {
            push(Ns::Duration, duration.to_string(), &mut out);
        }
        _ => {
            // Redgifs reports null duration for stills. Recorded explicitly so
            // "no duration" is distinguishable from "not scraped".
            push(Ns::Duration, "None".to_string(), &mut out);
        }
    }
    if let Some(sexuality) = gif["sexuality"].as_str()
        && let Some(sexuality) = bounded_text(sexuality)
    {
        push(Ns::Sexuality, sexuality, &mut out);
    }

    out
}

/// Builds the file for one member of a gallery, tagged with its position.
///
/// Gallery members carry the same shape as a listing entry, so the media
/// selection is identical; only the position tag is added.
pub fn gallery_member_into(
    gif: &json::JsonValue,
    position: usize,
    options: &Options,
) -> Option<(FileObject, Vec<PluginTag>)> {
    let (file, tags) = gif_into(gif, options)?;
    let id = GifId::parse(gif["id"].as_str()?)?;

    let mut tags = tags;
    tags.push(scoped(
        Ns::GalleryPosition,
        position.to_string(),
        Ns::Gif.tag(id.as_str()),
    ));

    let mut file = file;
    file.tag_list[0].tags.push(scoped(
        Ns::GalleryPosition,
        position.to_string(),
        Ns::Gif.tag(id.as_str()),
    ));

    Some((file, tags))
}

/// Caps a gallery's member list defensively.
///
/// Observed galleries are small (4 entries), but the response is untrusted, so
/// the list is bounded rather than iterated whole.
pub fn bounded_members(members: Vec<json::JsonValue>) -> Vec<json::JsonValue> {
    members.into_iter().take(MAX_GALLERY_MEMBERS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> json::JsonValue {
        json::parse(input).unwrap()
    }

    const GIF: &str = r#"{
        "id": "ShrillCourageousComet",
        "userName": "someUploader",
        "createDate": 1790831567,
        "views": 4210,
        "likes": 12,
        "gallery": null,
        "tags": ["Anal", "Creampie"],
        "niches": ["solo"],
        "description": "a clip",
        "duration": 14,
        "width": 1080,
        "height": 1920,
        "hasAudio": true,
        "sexuality": "straight",
        "urls": {
            "hd": "https://media.redgifs.com/ShrillCourageousComet.mp4",
            "poster": "https://media.redgifs.com/ShrillCourageousComet-poster.jpg",
            "html": "https://www.redgifs.com/ifr/shrillcourageouscomet"
        }
    }"#;

    #[test]
    fn gif_becomes_a_file_with_identity_and_value_tags() {
        let (file, tags) = gif_into(&parse(GIF), &Options::default()).unwrap();

        assert!(matches!(
            &file.source,
            Some(FileSource::Url(url))
                if url == "https://media.redgifs.com/ShrillCourageousComet.mp4"
        ));

        let namespaces = file.tag_list[0]
            .tags
            .iter()
            .map(|tag| tag.tag.namespace.name.as_str())
            .collect::<Vec<_>>();
        for expected in [
            "RedgifsGif",
            "RedgifsUser",
            "RedgifsTag",
            "RedgifsMediaUrl",
            "RedgifsFormat",
            "RedgifsEmbedUrl",
            "RedgifsDimensions",
            "RedgifsDuration",
            "RedgifsHasAudio",
        ] {
            assert!(namespaces.contains(&expected), "missing {expected}");
        }

        // The uploader relation must exist.
        assert!(tags.iter().any(|tag| {
            tag.tag.namespace.name == "RedgifsGif"
                && tag.relates_to.as_ref().is_some_and(|rel| {
                    rel.tag.namespace.name == "RedgifsUser" && rel.tag.name == "someUploader"
                })
        }));
    }

    #[test]
    fn ids_are_lowercased_and_validated() {
        assert_eq!(
            GifId::parse("ShrillCourageousComet").unwrap().as_str(),
            "shrillcourageouscomet"
        );
        assert_eq!(GifId::parse("  Abc-1  ").unwrap().as_str(), "abc-1");
        assert!(GifId::parse("").is_none());
        assert!(GifId::parse("has space").is_none());
        assert!(GifId::parse("sneaky/../etc").is_none());
        assert!(GifId::parse(&"a".repeat(65)).is_none());
    }

    #[test]
    fn gif_without_a_usable_variant_is_skipped() {
        let gif = parse(r#"{"id": "abc", "urls": {}}"#);
        assert!(gif_into(&gif, &Options::default()).is_none());

        // With stills allowed it becomes a poster download.
        let options = Options {
            allow_still: true,
            ..Default::default()
        };
        assert!(gif_into(
            &parse(
                r#"{"id": "abc", "urls": {"poster": "https://media.redgifs.com/abc-poster.jpg"}}"#
            ),
            &options
        )
        .is_some());
    }

    #[test]
    fn gallery_membership_is_recorded_in_both_directions() {
        let gif = parse(&GIF.replace("\"gallery\": null", "\"gallery\": \"1a0f5dccfaf-0257\""));
        let (_, tags) = gif_into(&gif, &Options::default()).unwrap();

        assert!(
            tags.iter().any(|tag| {
                tag.tag.namespace.name == "RedgifsGif"
                    && tag
                        .relates_to
                        .as_ref()
                        .is_some_and(|rel| rel.tag.namespace.name == "RedgifsGallery")
            }),
            "gif must relate to its gallery"
        );
        assert!(
            tags.iter().any(|tag| {
                tag.tag.namespace.name == "RedgifsGallery"
                    && tag
                        .relates_to
                        .as_ref()
                        .is_some_and(|rel| rel.tag.namespace.name == "RedgifsGif")
            }),
            "gallery must relate back to the gif"
        );
    }

    #[test]
    fn anonymous_uploads_share_one_user_tag() {
        let gif = parse(r#"{"id": "abc", "userName": "", "urls": {"hd": "https://m/a.mp4"}}"#);
        let (_, tags) = gif_into(&gif, &Options::default()).unwrap();
        assert!(tags.iter().any(|tag| {
            tag.tag.namespace.name == "RedgifsGif"
                && tag
                    .relates_to
                    .as_ref()
                    .is_some_and(|rel| rel.tag.name == "unknown")
        }));
    }

    #[test]
    fn gallery_entries_become_followup_jobs_and_dedupe() {
        let gifs = vec![
            parse(r#"{"id": "a1", "gallery": "gal-1"}"#),
            parse(r#"{"id": "a2", "gallery": "gal-1"}"#),
            parse(r#"{"id": "b1", "gallery": "gal-2"}"#),
            parse(r#"{"id": "c1", "gallery": null}"#),
        ];
        let refs: Vec<&json::JsonValue> = gifs.iter().collect();

        let jobs = gallery_jobs(&refs, &Options::default());
        assert_eq!(jobs.len(), 2, "one job per gallery, not per entry");

        // Every gallery job skips itself when its first member is held.
        for job in &jobs {
            assert_eq!(job.skip_conditions.len(), 1);
        }
    }

    #[test]
    fn gallery_jobs_can_be_forced_to_refresh() {
        let gifs = vec![parse(r#"{"id": "a1", "gallery": "gal-1"}"#)];
        let refs: Vec<&json::JsonValue> = gifs.iter().collect();
        let options = Options {
            refresh_galleries: true,
            ..Default::default()
        };
        assert!(gallery_jobs(&refs, &options)[0].skip_conditions.is_empty());
    }

    #[test]
    fn gallery_members_carry_their_position() {
        let (file, tags) = gallery_member_into(&parse(GIF), 3, &Options::default()).unwrap();
        assert!(tags.iter().any(|tag| {
            tag.tag.namespace.name == "RedgifsGalleryPosition" && tag.tag.name == "3"
        }));
        assert!(
            file.tag_list[0]
                .tags
                .iter()
                .any(|tag| tag.tag.namespace.name == "RedgifsGalleryPosition")
        );
    }

    #[test]
    fn gallery_member_list_is_bounded() {
        let members = (0..(MAX_GALLERY_MEMBERS + 50))
            .map(|i| parse(&format!(r#"{{"id": "a{i}"}}"#)))
            .collect();
        assert_eq!(bounded_members(members).len(), MAX_GALLERY_MEMBERS);
    }

    #[test]
    fn absurd_dimensions_are_rejected() {
        let gif = parse(&GIF.replace("\"width\": 1080", "\"width\": 999999"));
        let (_, tags) = gif_into(&gif, &Options::default()).unwrap();
        assert!(
            !tags
                .iter()
                .any(|tag| tag.tag.namespace.name == "RedgifsDimensions")
        );
    }

    #[test]
    fn missing_duration_is_recorded_explicitly() {
        let gif = parse(&GIF.replace("\"duration\": 14", "\"duration\": null"));
        let (_, tags) = gif_into(&gif, &Options::default()).unwrap();
        assert!(
            tags.iter().any(|tag| {
                tag.tag.namespace.name == "RedgifsDuration" && tag.tag.name == "None"
            })
        );
    }

    #[test]
    fn long_descriptions_are_truncated() {
        let long = "x".repeat(MAX_TEXT_LEN + 500);
        let gif = parse(&GIF.replace("a clip", &long));
        let (_, tags) = gif_into(&gif, &Options::default()).unwrap();
        let description = tags
            .iter()
            .find(|tag| tag.tag.namespace.name == "RedgifsDescription")
            .expect("description tag");
        assert!(description.tag.name.len() <= MAX_TEXT_LEN + 4);
    }

    #[test]
    fn malformed_gif_objects_are_skipped_not_panicked() {
        for input in ["{}", r#"{"id": ""}"#, r#"{"id": 12}"#, "null", "[]"] {
            let _ = gif_into(&parse(input), &Options::default());
            let _ = gallery_jobs(&[&parse(input)], &Options::default());
        }
    }
}
