//! Generic direct-link downloader.
//!
//! Successor of the legacy `File Downloader` plugin: any tag that carries a
//! file-shaped url gets a `direct download` job, the host's download pipeline
//! fetches the body directly, and the file gets tagged with the url as its
//! `source_url`. The tag-relationship skip conditions keep a link that already
//! downloaded from spawning a second copy.
//!
//! Plain page urls (no file extension in the path) and links that belong to
//! plugins handling their own fetching (MEGA, catbox) are ignored.

use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    sync::LazyLock,
    time::Duration,
};

use regex::Regex;
use shared_types::{
    CallbackReturn, DbSettingsObj, DEFAULT_PRIORITY, FileObject, FileSource, FileTagAction,
    GenericNamespaceObj, GlobalCallbacks, Plugin, PluginJob, PluginProperties, PluginTag,
    ScraperDataReturn, ScraperParam, ScraperObject, ScraperReturn, SearchType, SkipIf,
    StartupThreadType, Tag, TagOperation, Url,
};

const SITE: &str = "direct download";
const STARTUP_SETTING: &str = "PLUGIN_DirectDownload_ShouldCheckTags";

/// Any http(s) url; file-shape checks happen in code because a bregex must
/// simultaneously accept paths, queries and fragments without dragging in
/// whitespace or tag markup.
const URL_REGEX: &str = r#"(?i)https?://[^\s<>"'`]+"#;

/// Namespace the downloaded file's source tag lives in; its url tags match
/// this plugin's callback, so they are denied below to avoid endlessly
/// re-queueing already-known sources.
const NS_SOURCE_URL: &str = "source_url";

static URL_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(URL_REGEX).unwrap());

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<Plugin> {
    vec![Plugin {
        name: SITE.into(),
        properties: vec![
            PluginProperties::Sites(vec![
                SITE.into(),
                "direct".into(),
                "directdownload".into(),
                "Direct Download".into(),
            ]),
            PluginProperties::Ratelimit(2, Duration::from_secs(1)),
            PluginProperties::ThreadNum(2),
            PluginProperties::NoTextDownload,
        ],
        callbacks: vec![
            GlobalCallbacks::Start(StartupThreadType::Spawn),
            GlobalCallbacks::Tag((
                SearchType::Regex(URL_REGEX.into()),
                vec![],
                vec![NS_SOURCE_URL.into()],
            )),
        ],
    }]
}

/// Only reacts to file-shaped urls: a path is required, its final segment must
/// carry a plausible file extension, and trailing punctuation (a `"` `)` or `>`
/// closing the url inside text) gets trimmed first.
fn file_like(matched: &str) -> Option<String> {
    let mut url = matched.trim().to_string();
    if url.starts_with("//") {
        url = format!("https:{url}");
    }
    while url.ends_with(|c: char| ",.)\"'`]>;:".contains(c)) {
        url.pop();
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return None;
    }

    // Cut the query/fragment: extension checks work on the path.
    let path = url.split(['?', '#']).next()?.to_string();
    let (_, after_scheme) = path.split_once("://")?;
    let (_, path_part) = after_scheme.split_once('/')?;
    if path_part.is_empty() {
        return None;
    }

    let last = path_part.rsplit('/').next()?;
    let (base, ext) = last.rsplit_once('.')?;
    if base.is_empty() || ext.is_empty() || ext.len() > 7 {
        return None;
    }
    if !ext.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(url)
}

/// Hosts other plugins own completely; this plugin would only fight them.
fn owned_by_plugin(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    ["mega.nz", "mega.co.nz", "catbox.moe", "bsky.app"]
        .iter()
        .any(|needle| lower.contains(needle))
}

#[unsafe(no_mangle)]
fn on_regex(
    matched: &str,
    _full_tag: &str,
    tag_source: &Tag,
    _tag_source_id: u64,
) -> CallbackReturn {
    // Structural urls produced here must never re-trigger this callback.
    if tag_source.namespace.name == NS_SOURCE_URL {
        return CallbackReturn::default();
    }

    let Some(url) = file_like(matched) else {
        return CallbackReturn::default();
    };
    if owned_by_plugin(&url) {
        return CallbackReturn::default();
    }

    CallbackReturn {
        tags: HashSet::new(),
        jobs: HashSet::from([ScraperDataReturn {
            job: queued_job(&url),
            skip_conditions: vec![SkipIf::FileTagRelationship(Tag {
                name: url.clone(),
                namespace: ns_source_url(),
            })],
        }]),
    }
}

fn ns_source_url() -> GenericNamespaceObj {
    GenericNamespaceObj {
        name: NS_SOURCE_URL.into(),
        description: Some("A source for a file".into()),
    }
}

fn queued_job(url: &str) -> PluginJob {
    PluginJob {
        time: 0,
        reptime: 0,
        priority: DEFAULT_PRIORITY,
        recreation: None,
        site: SITE.to_string(),
        param: vec![ScraperParam::Url(Url {
            url: url.to_string(),
            local_modifiers: Vec::new(),
        })],
        user_data: BTreeMap::new(),
    }
}

#[unsafe(no_mangle)]
fn on_start() {
    if let Err(error) = handle_on_start() {
        let _ = client::log_silent(format!("DIRECT: startup tag scan failed: {error}"));
    }
}

/// One-shot sweep over existing tags so direct links that predate this plugin
/// (chan comments, in particular) become download jobs. Gated by a settings
/// row the same way the MEGA and catbox plugins gate theirs.
fn handle_on_start() -> Result<(), Box<dyn Error>> {
    if client::setting_get(STARTUP_SETTING.into())?.is_some() {
        return Ok(());
    }

    let tags = client::get_tag_id_bulk(client::get_tag_ids_all()?)?;
    let mut queued = HashSet::new();
    let mut failures = 0usize;

    for (tag_id, tag) in tags
        .iter()
        .filter(|(_, tag)| tag.namespace.name != NS_SOURCE_URL)
    {
        for matched in URL_RE.find_iter(&tag.name).map(|m| m.as_str()) {
            let Some(url) = file_like(matched) else {
                continue;
            };
            if owned_by_plugin(&url) || !queued.insert(url.clone()) {
                continue;
            }
            let callback_return = on_regex(&url, &tag.name, tag, *tag_id);
            for job in callback_return.jobs {
                if let Err(error) = client::jobs_add_single(job.job) {
                    failures += 1;
                    let _ =
                        client::log_silent(format!("DIRECT: could not queue {url}: {error}"));
                }
            }
        }
    }
    if failures > 0 {
        let _ = client::log_silent(format!("DIRECT: {failures} startup job(s) failed to queue"));
    }

    client::setting_set(DbSettingsObj {
        name: STARTUP_SETTING.into(),
        description: Some(
            "Whether the directdownload plugin should scan existing tags on startup".into(),
        ),
        num: None,
        param: Some("False".into()),
    })?;

    Ok(())
}

///
/// The job carries the file url and the plugin declared `NoTextDownload`, so
/// the host hands over empty text and the direct url. The download pipeline
/// fetches the body; the url gets tagged on the file itself.
///
#[unsafe(no_mangle)]
pub fn parser_call(
    _text_input: &str,
    source_url: &str,
    _scraperdata: &ScraperDataReturn,
) -> Vec<ScraperReturn> {
    if source_url.is_empty() {
        return vec![ScraperReturn::Nothing];
    }

    let source_tag = Tag {
        name: source_url.to_string(),
        namespace: ns_source_url(),
    };
    let file = FileObject {
        source: Some(FileSource::Url(source_url.to_string())),
        hash: vec![],
        tag_list: vec![FileTagAction {
            operation: TagOperation::Add,
            tags: vec![PluginTag {
                tag: source_tag.clone(),
                ..Default::default()
            }],
        }],
        // If this exact source url already hangs on a file somewhere, adding
        // another copy would be pointless.
        skip_if: vec![SkipIf::FileTagRelationship(source_tag)],
    };

    vec![ScraperReturn::Data(ScraperObject {
        files: HashSet::from([file]),
        jobs: HashSet::new(),
        tags: HashSet::new(),
    })]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_links_pass() {
        assert_eq!(
            file_like("https://files.example.com/ab12cd34.jpg"),
            Some("https://files.example.com/ab12cd34.jpg".to_string())
        );
        // Scheme-relative + trailing sentence period in comment text.
        assert_eq!(
            file_like("//files.example.com/clip.m4v."),
            Some("https://files.example.com/clip.m4v".to_string())
        );
        // Query strings keep the file shape.
        assert_eq!(
            file_like("https://host.tld/p/file.png?dl=1"),
            Some("https://host.tld/p/file.png?dl=1".to_string())
        );
    }

    #[test]
    fn pages_and_junk_rejected() {
        // No path at all.
        assert_eq!(file_like("https://example.com"), None);
        // Page-shaped path, no extension.
        assert_eq!(file_like("https://e621.net/posts/123456"), None);
        // Extension too long / missing base.
        assert_eq!(file_like("https://x.y/a/b.cdefghijk"), None);
        assert_eq!(file_like("https://x.y/a/.png"), None);
        assert_eq!(file_like("plain text"), None);
    }

    #[test]
    fn other_plugins_owners_skipped() {
        assert!(owned_by_plugin("https://files.catbox.moe/ci9dyv.mp4"));
        assert!(owned_by_plugin("https://mega.nz/file/AbC#key"));
        assert!(owned_by_plugin("https://bsky.app/profile/x"));
        assert!(!owned_by_plugin("https://files.example.com/a.jpg"));
    }

    #[test]
    fn job_carries_source_skip() {
        let job = queued_job("https://host.tld/p/file.png");
        assert_eq!(job.site, SITE);
        assert!(matches!(
            job.param.first(),
            Some(ScraperParam::Url(url)) if url.url == "https://host.tld/p/file.png"
        ));
    }
}
