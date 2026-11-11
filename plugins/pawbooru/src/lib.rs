use std::{collections::HashSet, fmt, time::Duration};

use scraper::{Html, Selector};
use shared_types::{
    DEFAULT_PRIORITY, DownloadModifiers, FileObject, FileSource, FileTagAction,
    GenericNamespaceObj, GlobalCallbacks, HashesSupported, ModifierTarget, PluginJob,
    PluginProperties, PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn,
    StartupThreadType, Tag, TargetModifier,
};

/// The single site this plugin handles.
pub enum Site {
    PawBooru,
}

/// Tag namespace identifiers used by PawBooru (Shimmie2).
/// The tag format is "namespace:name" where namespace is a prefix like
/// "artist:", "character:", "species:", "general:", "meta:", "rating:".
pub enum NsIdent {
    PostId,
    Parent,
    Children,
    Rating,
    General,
    Artist,
    Character,
    Species,
    Copyright,
    Meta,
    Invalid,
    Source,
    FileUrl,
}

impl fmt::Display for Site {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PawBooru")
    }
}

impl Site {
    /// Host for the web UI and HTML scraping.
    fn host(&self) -> &'static str {
        "pawbooru.org"
    }

    /// Build a post-view page URL from the post ID.
    fn build_post_view_url(&self, post_id: u64) -> String {
        format!("https://{}/post/view/{}", self.host(), post_id)
    }

    /// Map Shimmie2 tag namespace prefixes to our internal `NsIdent`.
    /// Shimmie2 prefixes: artist:, character:, species:, general:, meta:, rating:, parent:, children:, source:
    fn tag_ns(prefix: &str) -> Option<NsIdent> {
        Some(match prefix.to_lowercase().as_str() {
            "artist:" => NsIdent::Artist,
            "character:" => NsIdent::Character,
            "species:" => NsIdent::Species,
            "general:" => NsIdent::General,
            "meta:" => NsIdent::Meta,
            "copyright:" => NsIdent::Copyright,
            "rating:" => NsIdent::Rating,
            "parent:" => NsIdent::Parent,
            "children:" => NsIdent::Children,
            "source:" => NsIdent::Source,
            _ => return None,
        })
    }

    /// Convert our internal `NsIdent` to a `GenericNamespaceObj` for the tag.
    fn nsobj(&self, ident: &NsIdent) -> GenericNamespaceObj {
        GenericNamespaceObj {
            name: match ident {
                NsIdent::PostId => "Post ID",
                NsIdent::Parent => "Parent",
                NsIdent::Children => "Children",
                NsIdent::Rating => "Rating",
                NsIdent::General => "General",
                NsIdent::Artist => "Artist",
                NsIdent::Character => "Character",
                NsIdent::Species => "Species",
                NsIdent::Copyright => "Copyright",
                NsIdent::Meta => "Meta",
                NsIdent::Source => "Source",
                NsIdent::FileUrl => "File URL",
                NsIdent::Invalid => "Invalid",
            }
            .into(),
            description: None,
        }
    }

    /// Create a `PluginTag` from a raw tag string and the identified namespace.
    fn make_plugin_tag(&self, name: &str, ns: NsIdent) -> PluginTag {
        PluginTag {
            tag: Tag {
                name: name.to_string(),
                namespace: self.nsobj(&ns),
            },
            tag_type: shared_types::TagType::Normal,
            relates_to: None,
        }
    }
}

/// Plugin registration entry point.
#[unsafe(no_mangle)]
fn get_plugin_info() -> shared_types::CVec<u8> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(get_plugin_info_impl));
    match result {
        Ok(value) => shared_types::plugin_ffi::encode(&value),
        Err(_) => shared_types::plugin_ffi::encode(&Vec::<shared_types::Plugin>::new()),
    }
}

fn get_plugin_info_impl() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "PawBooru".into(),
        properties: vec![
            PluginProperties::Ratelimit(1, Duration::from_secs(1)),
            PluginProperties::Sites(vec!["pawbooru".into(), "pawbooru.org".into()]),
            PluginProperties::JobNum(10),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Text,
                modifier: DownloadModifiers::Timeout(Some(Duration::from_secs(30))),
            }),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header((
                    "Referer".into(),
                    "https://pawbooru.org/".into(),
                )),
            }),
        ],
        callbacks: vec![GlobalCallbacks::Start(StartupThreadType::Spawn)],
        ..Default::default()
    }]
}

/// Decodes `ScraperDataReturn` from the host and dispatches to `url_dump_impl`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn url_dump(req: *const u8, req_len: usize) -> shared_types::CVec<u8> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Some(data) = (unsafe {
            shared_types::plugin_ffi::decode::<shared_types::ScraperDataReturn>(req, req_len)
        }) else {
            return Vec::new();
        };
        url_dump_impl(&data)
    }));
    match result {
        Ok(value) => shared_types::plugin_ffi::encode(&value),
        Err(_) => shared_types::plugin_ffi::encode(&Vec::<shared_types::ScraperDataReturn>::new()),
    }
}

/// Generates scraper jobs for each page of a post-list search.
/// `url_dump` is called by the host when a new scrape job is created for this plugin's sites.
fn url_dump_impl(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();
    let site = Site::PawBooru;

    let normal_params: Vec<String> = scraperdata
        .job
        .param
        .iter()
        .filter_map(|f| {
            if let shared_types::ScraperParam::Normal(tag) = f {
                Some(tag.to_string())
            } else {
                None
            }
        })
        .collect();
    let url_params_cleaned: Vec<String> = scraperdata
        .job
        .param
        .iter()
        .filter_map(|f| {
            if let shared_types::ScraperParam::Url(url_obj) = f {
                Some(url_obj.url.to_string())
            } else {
                None
            }
        })
        .collect();

    // Adds jobs for posts back into the system
    for url in url_params_cleaned {
        if url.contains("/post/view") {
            out.push(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: DEFAULT_PRIORITY - 2,
                    param: vec![shared_types::ScraperParam::Url(shared_types::Url {
                        url,
                        ..Default::default()
                    })],
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    }

    /*
    let mut login_params = scraperdata.job.param.clone();
    login_params.retain(|f| matches!(f, ScraperParam::Login(_)));

    // Check if any input URL is a direct post-view URL.
    // If so, we should NOT paginate — just passthrough the URL(s) directly.
    let has_post_view_url = scraperdata.job.param.iter().any(|p| {
        matches!(p, ScraperParam::Url(u) if u.url.contains("/post/view/"))
    });

    if has_post_view_url {
        // Direct post-view URL(s) provided: passthrough only, no pagination.
        for param in scraperdata.job.param.clone() {
            if let ScraperParam::Url(url) = param {
                // Normalize post-view URLs by stripping query parameters
                // The post ID in the path is all that's needed.
                let normalized_url = if url.url.contains("/post/view/") {
                    let base_url = url.url.split('?').next().unwrap_or(&url.url);
                    shared_types::Url {
                        url: base_url.to_string(),
                        ..url
                    }
                } else {
                    url
                };
                let mut param = login_params.clone();
                param.push(ScraperParam::Url(normalized_url));
                out.push(ScraperDataReturn {
                    job: shared_types::PluginJob {
                        site: scraperdata.job.site.clone(),
                        priority: DEFAULT_PRIORITY - 2,
                        param,
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
        }
        return out;
    }*/

    // Extract the tag query from the initial job params.
    if !normal_params.is_empty() {
        let tags_string = normal_params.join("%20");
        let base = format!("https://{}/post/list/{}", site.host(), tags_string);

        // Pages 1..=5000; page 1 has no page suffix on Shimmie2, so the
        // suffix only appears from page 2 on.
        for page in 1..=5000u32 {
            let page_suffix = if page == 1 {
                String::new()
            } else {
                format!("/{page}")
            };
            out.push(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: DEFAULT_PRIORITY - 1, // Higher priority for search/list pages
                    param: vec![shared_types::ScraperParam::Url(shared_types::Url {
                        url: base.clone() + &page_suffix,
                        ..Default::default()
                    })],
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    }
    /*  // Only generate page 1; subsequent pages are dynamically enqueued by the parser
    // when posts are found on the current page.
    let url = site.build_post_list_url(tag_query.as_deref(), 1);
    dbg!(&url, &tag_query, 1);
    let mut param = params.clone();
    param.push(ScraperParam::Url(shared_types::Url {
        url,
        ..Default::default()
    }));
    out.push(ScraperDataReturn {
        job: shared_types::PluginJob {
            site: scraperdata.job.site.clone(),
            priority: DEFAULT_PRIORITY - 1, // Higher priority for search/list pages
            param,
            ..Default::default()
        },
        ..Default::default()
    });*/
    /*
    // URL passthrough for non-post-view, non-post-list URLs.
    for param in scraperdata.job.param.clone() {
        if let ScraperParam::Url(url) = param {
            // Skip ALL post-list URLs (they're handled by page 1 + dynamic pagination)
            // Skip post-view URLs (they're handled by the early return above)
                let mut param = login_params.clone();
                param.push(ScraperParam::Url(url));
                out.push(ScraperDataReturn {
                    job: shared_types::PluginJob {
                        site: scraperdata.job.site.clone(),
                        priority: DEFAULT_PRIORITY - 2,
                        param,
                        ..Default::default()
                    },
                    ..Default::default()
                });
        }
    }*/

    out
}

/// Decodes `ParserCallInput` from the host and dispatches to `parser_call_impl`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn parser_call(req: *const u8, req_len: usize) -> shared_types::CVec<u8> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Some(input) = (unsafe {
            shared_types::plugin_ffi::decode::<shared_types::ParserCallInput>(req, req_len)
        }) else {
            return vec![shared_types::ScraperReturn::Stop(
                "undecodable parser_call request".to_string(),
            )];
        };
        parser_call_impl(&input.text, &input.source_url, &input.data)
    }));
    match result {
        Ok(value) => shared_types::plugin_ffi::encode(&value),
        Err(_) => shared_types::plugin_ffi::encode(&vec![shared_types::ScraperReturn::Stop(
            "plugin panicked".to_string(),
        )]),
    }
}

/// Main HTML parser for post-view and post-list pages.
/// Returns `ScraperReturn::Data(ScraperObject)` containing files, tags, and child jobs.
fn parser_call_impl(
    text_input: &str,
    source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    // Handle redownload requests (INTSCRAPE_REDOWNLOAD user_data)
    if scraperdata
        .job
        .user_data
        .contains_key("INTSCRAPE_REDOWNLOAD")
    {
        return shared_types::helpers::redownload_return(source_url, &scraperdata.job.user_data)
            .unwrap_or_else(|| vec![ScraperReturn::Nothing]);
    }

    let recursion = shared_types::helpers::is_recursion(&scraperdata.job.user_data);
    let site = Site::PawBooru;

    let mut files = HashSet::new();
    let mut jobs = HashSet::new();
    let mut tags = HashSet::new();

    let html = Html::parse_document(text_input);

    // Detect page type: post/view/{id} vs post/list/...
    if source_url.contains("/post/view/") {
        parse_post_view(
            &site,
            text_input,
            source_url,
            &mut files,
            &mut tags,
            &mut jobs,
            recursion,
            scraperdata,
        );
    } else if source_url.contains("/post/list") {
        parse_post_list(
            &site,
            &html,
            source_url,
            &mut files,
            &mut tags,
            &mut jobs,
            recursion,
            scraperdata,
        );
    }

    // An empty page yields empty sets → `finalize` returns `Nothing`,
    // which the host treats as a clean stop for that page's job.
    shared_types::helpers::finalize(files, tags, jobs)
}

/// Parse a post-view page: extract the main image, tags, parent/children links.
fn parse_post_view(
    site: &Site,
    html: &str,
    source_url: &str,
    files: &mut HashSet<FileObject>,
    tags: &mut HashSet<PluginTag>,
    jobs: &mut HashSet<ScraperDataReturn>,
    recursion: bool,
    scraperdata: &ScraperDataReturn,
) {
    // Extract post ID from canonical URL or source URL
    let post_id = extract_post_id(source_url).or_else(|| extract_canonical_post_id(html));

    // Collect parsed tags for the file
    let mut file_tags = Vec::new();

    // Extract tags from data-tags attribute (space-separated with namespace prefixes)
    if let Some(tag_str) = extract_data_attr(html, "data-tags") {
        for raw in tag_str.split_whitespace() {
            if let Some((prefix, name)) = raw.split_once(':') {
                let prefix = format!("{}:", prefix);
                if let Some(ns) = Site::tag_ns(&prefix) {
                    let tag = site.make_plugin_tag(name, ns);
                    tags.insert(tag.clone());
                    file_tags.push(tag);
                } else {
                    // Unknown prefix → General
                    let tag = site.make_plugin_tag(raw, NsIdent::General);
                    tags.insert(tag.clone());
                    file_tags.push(tag);
                }
            } else {
                // No prefix → General
                let tag = site.make_plugin_tag(raw, NsIdent::General);
                tags.insert(tag.clone());
                file_tags.push(tag);
            }
        }
    }

    // Extract rating from data-rating
    if let Some(rating) = extract_data_attr(html, "data-rating") {
        let ns = match rating.chars().next() {
            Some('s') => NsIdent::Rating,
            Some('q') => NsIdent::Rating,
            Some('e') => NsIdent::Rating,
            _ => NsIdent::General,
        };
        let tag = site.make_plugin_tag(&rating, ns);
        tags.insert(tag.clone());
        file_tags.push(tag);
    }

    // Extract the full-size image URL from og:image or data-full-src
    if let Some(img_url) =
        extract_og_image(html).or_else(|| extract_data_attr(html, "data-full-src"))
    {
        let full_url = if img_url.starts_with("http") {
            img_url
        } else {
            format!("https://{}{}", site.host(), img_url)
        };

        // Extract MD5 from URL path: /_images/{md5}/{filename}
        // The MD5 is the 32-char hex string right after /_images/
        let md5 = extract_md5_from_image_url(&full_url).unwrap_or_else(|| {
            // Fallback: use post ID or hash of URL
            post_id.map(|id| id.to_string()).unwrap_or_else(|| {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                full_url.hash(&mut h);
                format!("{:x}", h.finish())
            })
        });

        let mut tag_vec = Vec::new();
        if let Some(id) = post_id {
            tag_vec.push(site.make_plugin_tag(&id.to_string(), NsIdent::PostId));
        }
        // Add source URL as a tag
        tag_vec.push(site.make_plugin_tag(source_url, NsIdent::FileUrl));
        // Add all parsed tags to the file
        tag_vec.extend(file_tags);

        files.insert(FileObject {
            source: Some(FileSource::Url(full_url)),
            hash: vec![HashesSupported::Md5(md5)],
            tag_list: vec![FileTagAction {
                tags: tag_vec,
                operation: shared_types::TagOperation::Set,
            }],
            skip_if: vec![],
        });
    }

    // Parent link: look for "Parent" link in post view
    if let Some(parent_id) = extract_parent_id(html) {
        let parent_tag = site.make_plugin_tag(&parent_id.to_string(), NsIdent::Parent);
        tags.insert(parent_tag.clone());

        // Also add a relationship tag pointing from parent to this post
        if let Some(id) = post_id {
            tags.insert(PluginTag {
                tag: Tag {
                    name: id.to_string(),
                    namespace: site.nsobj(&NsIdent::Children),
                },
                tag_type: shared_types::TagType::Normal,
                relates_to: Some(RelationContext {
                    tag: parent_tag.tag,
                    ..Default::default()
                }),
            });
        }

        // If recursion enabled, queue a job to fetch the parent post
        if recursion {
            let parent_url = site.build_post_view_url(parent_id);
            enqueue_job(
                site,
                &scraperdata.job,
                &parent_url,
                jobs,
                DEFAULT_PRIORITY - 2,
            );
        }
    }

    // Children links: similar extraction would go here if needed
    // Shimmie2 doesn't expose children list on the post view by default.
}

/// Parse a post-list page: extract post IDs and enqueue post-view jobs.
fn parse_post_list(
    site: &Site,
    html: &Html,
    _source_url: &str,
    _files: &mut HashSet<FileObject>,
    tags: &mut HashSet<PluginTag>,
    jobs: &mut HashSet<ScraperDataReturn>,
    recursion: bool,
    scraperdata: &ScraperDataReturn,
) {
    // Extract all post IDs from the image list: containers carry data-post-id.
    for id_str in extract_post_ids_from_list(html) {
        if let Ok(id) = id_str.parse::<u64>() {
            // Record the post ID as a tag (helps with deduplication)
            tags.insert(site.make_plugin_tag(&id_str, NsIdent::PostId));

            // Enqueue the post-view page for full parsing (lower priority)
            if recursion {
                let view_url = site.build_post_view_url(id);
                enqueue_job(
                    site,
                    &scraperdata.job,
                    &view_url,
                    jobs,
                    DEFAULT_PRIORITY - 2,
                );
            }
        }
    }
    // An empty page leaves every set empty, so `finalize` returns
    // `Nothing`, which the host treats as "no more posts" and stops
    // this job cleanly — the e6 convention. Pagination itself is fully
    // pre-generated by `url_dump`; the parser never enqueues pages.
}

/// Enqueue a child job for the given URL with specified priority.
fn enqueue_job(
    _site: &Site,
    parent_job: &PluginJob,
    url: &str,
    jobs: &mut HashSet<ScraperDataReturn>,
    priority: u64,
) {
    let mut params = parent_job.param.clone();
    params.retain(|f| matches!(f, ScraperParam::Login(_)));
    params.push(ScraperParam::Url(shared_types::Url {
        url: url.to_string(),
        ..Default::default()
    }));
    jobs.insert(ScraperDataReturn {
        job: PluginJob {
            site: parent_job.site.clone(),
            priority,
            param: params,
            user_data: {
                let mut ud = parent_job.user_data.clone();
                ud.insert("recursion".into(), "false".into());
                ud
            },
            ..Default::default()
        },
        ..Default::default()
    });
}

/// Extract post ID from a /post/view/{id} URL.
fn extract_post_id(url: &str) -> Option<u64> {
    url.split("/post/view/")
        .nth(1)?
        .split('/')
        .next()?
        .split('?')
        .next()?
        .parse()
        .ok()
}

/// Extract post ID from canonical link or other metadata in HTML.
fn extract_canonical_post_id(html: &str) -> Option<u64> {
    // <link rel='canonical' href='https://pawbooru.org/post/view/12345'>
    if let Some(href_start) = html.find("rel='canonical' href='") {
        let start = href_start + 21;
        let end = html[start..].find('\'')?;
        return html[start..start + end]
            .split("/post/view/")
            .nth(1)?
            .split('/')
            .next()?
            .parse()
            .ok();
    }
    // <meta property="og:url" content="https://pawbooru.org/post/view/12345">
    if let Some(content_start) = html.find("og:url' content='") {
        let start = content_start + 17;
        let end = html[start..].find('\'')?;
        return html[start..start + end]
            .split("/post/view/")
            .nth(1)?
            .split('/')
            .next()?
            .parse()
            .ok();
    }
    None
}

/// Extract a data-* attribute value from HTML.
fn extract_data_attr(html: &str, attr: &str) -> Option<String> {
    let needle = format!("{}='", attr);
    let start = html.find(&needle)?;
    let start = start + needle.len();
    let end = html[start..].find('\'')?;
    Some(html[start..start + end].to_string())
}

/// Extract og:image URL.
fn extract_og_image(html: &str) -> Option<String> {
    let needle = "og:image' content='";
    let start = html.find(needle)?;
    let start = start + needle.len();
    let end = html[start..].find('\'')?;
    Some(html[start..start + end].to_string())
}

/// Extract MD5 hash from PawBooru image URL.
/// URL format: https://pawbooru.org/_images/{md5}/{filename}
/// or: http://pawbooru.org/_images/{md5}/{filename}
fn extract_md5_from_image_url(url: &str) -> Option<String> {
    // Find /_images/ in the URL
    let prefix = "/_images/";
    let start = url.find(prefix)? + prefix.len();
    // MD5 is 32 hex chars - check bounds first
    if start + 32 > url.len() {
        return None;
    }
    let candidate = &url[start..start + 32];
    if candidate.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(candidate.to_string())
    } else {
        None
    }
}

/// Extract parent post ID from post-view HTML.
/// Looks for: <a href='/post/view/12345'>Parent</a> or similar
fn extract_parent_id(html: &str) -> Option<u64> {
    // Pattern: href='/post/view/12345'>Parent
    if let Some(pos) = html.find(">Parent<") {
        let before = &html[..pos];
        if let Some(href_start) = before.rfind("href='/post/view/") {
            let id_start = href_start + 16;
            let id_end = before[id_start..].find('\'')?;
            return before[id_start..id_start + id_end].parse().ok();
        }
    }
    // Alternative: data-parent-id
    extract_data_attr(html, "data-parent-id").and_then(|s| s.parse().ok())
}

/// Extract all post IDs from a post-list page.
fn extract_post_ids_from_list(html: &Html) -> Vec<String> {
    let mut ids = Vec::new();

    let selector = Selector::parse("div.shm-image-list").unwrap();
    for div in html.select(&selector) {
        for img_container in div.children() {
            if let Some(post_element) = img_container.value().as_element()
                && let Some(post_id) = post_element.attr("data-post-id")
            {
                ids.push(post_id.to_string());
            }
        }
    }

    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_post_id() {
        assert_eq!(
            extract_post_id("https://pawbooru.org/post/view/30290"),
            Some(30290)
        );
        assert_eq!(
            extract_post_id("https://pawbooru.org/post/view/30290?foo=bar"),
            Some(30290)
        );
        assert_eq!(extract_post_id("/post/view/123"), Some(123));
        assert_eq!(extract_post_id("https://pawbooru.org/post/list"), None);
    }

    #[test]
    fn test_tag_ns() {
        assert!(matches!(Site::tag_ns("artist:"), Some(NsIdent::Artist)));
        assert!(matches!(
            Site::tag_ns("character:"),
            Some(NsIdent::Character)
        ));
        assert!(matches!(Site::tag_ns("species:"), Some(NsIdent::Species)));
        assert!(matches!(Site::tag_ns("general:"), Some(NsIdent::General)));
        assert!(matches!(Site::tag_ns("meta:"), Some(NsIdent::Meta)));
        assert!(matches!(
            Site::tag_ns("copyright:"),
            Some(NsIdent::Copyright)
        ));
        assert!(matches!(Site::tag_ns("rating:"), Some(NsIdent::Rating)));
        assert!(matches!(Site::tag_ns("parent:"), Some(NsIdent::Parent)));
        assert!(matches!(Site::tag_ns("children:"), Some(NsIdent::Children)));
        assert!(matches!(Site::tag_ns("source:"), Some(NsIdent::Source)));
        assert!(Site::tag_ns("unknown:").is_none());
    }

    #[test]
    fn test_build_urls() {
        let site = Site::PawBooru;
        assert_eq!(
            site.build_post_view_url(12345),
            "https://pawbooru.org/post/view/12345"
        );
    }

    #[test]
    fn test_extract_data_attr() {
        let html = "data-tags='foo bar baz' data-rating='s'";
        assert_eq!(
            extract_data_attr(html, "data-tags"),
            Some("foo bar baz".into())
        );
        assert_eq!(extract_data_attr(html, "data-rating"), Some("s".into()));
        assert_eq!(extract_data_attr(html, "data-missing"), None);
    }

    #[test]
    fn test_extract_post_ids_from_list() {
        let html = Html::parse_document(
            r#"<div class="shm-image-list">
                <div data-post-id="111"></div>
                <div data-post-id="222"></div>
            </div>"#,
        );
        let ids = extract_post_ids_from_list(&html);
        assert_eq!(ids, vec!["111", "222"]);
    }

    #[test]
    fn test_extract_md5_from_image_url() {
        let url =
            "https://pawbooru.org/_images/9fb5b840b14e63d5ca530fc7f6d7a1ab/33350%20-%20test.webp";
        assert_eq!(
            extract_md5_from_image_url(url),
            Some("9fb5b840b14e63d5ca530fc7f6d7a1ab".into())
        );

        let url = "http://pawbooru.org/_images/abcdef1234567890abcdef1234567890/file.png";
        assert_eq!(
            extract_md5_from_image_url(url),
            Some("abcdef1234567890abcdef1234567890".into())
        );

        // Invalid: too short
        assert_eq!(
            extract_md5_from_image_url("https://pawbooru.org/_images/abc/file.png"),
            None
        );
        // Invalid: non-hex
        assert_eq!(
            extract_md5_from_image_url(
                "https://pawbooru.org/_images/gggggggggggggggggggggggggggggggg/file.png"
            ),
            None
        );
        // No /_images/
        assert_eq!(
            extract_md5_from_image_url(
                "https://pawbooru.org/image/abcdef1234567890abcdef1234567890/file.png"
            ),
            None
        );
    }
}
