use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use scraper::{Html, Selector};
use shared_types::{
    DEFAULT_PRIORITY, DownloadModifiers, FileObject, FileSource, FileTagAction,
    GenericNamespaceObj, LoginNeed, LoginType, ModifierTarget, PluginJob, PluginProperties,
    PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn, Tag,
    TargetModifier,
};
use url::Url;

const SITE_ROOT: &str = "www.furaffinity.net";

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "FurAffinity".into(),
        properties: vec![
            PluginProperties::ThreadNum(1),
            PluginProperties::Ratelimit(1, Duration::from_secs(2)),
            PluginProperties::Sites(vec![
                "fa".into(),
                "furaffinity".into(),
                "furaffinity.net".into(),
            ]),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Text,
                modifier: DownloadModifiers::Header((
                    "Accept".into(),
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8".into(),
                )),
            }),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header((
                    "Referer".into(),
                    format!("https://{SITE_ROOT}/"),
                )),
            }),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Timeout(Some(Duration::from_secs(60))),
            }),
            PluginProperties::Login((
                LoginNeed::Optional,
                LoginType::Cookie(
                    "a".into(),
                    Some(
                        "session cookie 'a' from furaffinity.net (required for adult content)"
                            .into(),
                    ),
                ),
            )),
            PluginProperties::Login((
                LoginNeed::Optional,
                LoginType::Cookie(
                    "b".into(),
                    Some("session cookie 'b' from furaffinity.net".into()),
                ),
            )),
        ],
        ..Default::default()
    }]
}

/// /view/<id>/ or /full/<id>/ -> <id>
fn extract_submission_id(url: &str) -> Option<String> {
    let mut segments = url.split('/');
    while let Some(seg) = segments.next() {
        if (seg == "view" || seg == "full")
            && let Some(id) = segments.next()
            && id.chars().all(|c| c.is_ascii_digit())
            && !id.is_empty()
        {
            return Some(id.to_string());
        }
    }
    None
}

fn abs_url(href: &str) -> String {
    if let Some(rest) = href.strip_prefix("//") {
        format!("https://{rest}")
    } else if href.starts_with("http") {
        href.to_string()
    } else if let Some(rest) = href.strip_prefix('/') {
        format!("https://{SITE_ROOT}/{rest}")
    } else {
        format!("https://{SITE_ROOT}/{href}")
    }
}

fn ns(name: &str, description: &str) -> GenericNamespaceObj {
    GenericNamespaceObj {
        name: name.into(),
        description: Some(description.into()),
    }
}

fn sub_id_tag(id: &str) -> Tag {
    Tag {
        name: id.to_string(),
        namespace: ns("FA_Submission_ID", "FurAffinity submission unique ID"),
    }
}

fn source_url_tag(url: &str) -> Tag {
    Tag {
        name: url.to_string(),
        namespace: ns("source_url", "Source URL for file"),
    }
}

/// "Visited" marker for a page: stored the first time the page is parsed,
/// so the same URL is never fetched again. The marker is a tag named by
/// the page URL, parented to a constant "visited" tag, checked via
/// `SkipIf::ParentsRelate`. Remove with `tasks database remove namespace
/// FA_Visited_Page` to force a re-scrape.
fn visited_page_plugin_tag(url: &str) -> PluginTag {
    let namespace = ns("FA_Visited_Page", "FurAffinity page already visited");
    PluginTag {
        tag: Tag {
            name: url.to_string(),
            namespace: namespace.clone(),
        },
        relates_to: Some(RelationContext {
            tag: Tag {
                name: "visited".into(),
                namespace,
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn visited_skip(url: &str) -> shared_types::SkipIf {
    shared_types::SkipIf::ParentsRelate(visited_page_plugin_tag(url))
}

fn tag_on(name: String, namespace: GenericNamespaceObj, relates_to: &RelationContext) -> PluginTag {
    PluginTag {
        tag: Tag { name, namespace },
        relates_to: Some(relates_to.clone()),
        ..Default::default()
    }
}

/// Listing pages (galleries/search/profiles) run before the individual
/// submissions they discover, so pagination finishes first.
const LISTING_PRIORITY: u64 = DEFAULT_PRIORITY;
const SUBMISSION_PRIORITY: u64 = DEFAULT_PRIORITY.saturating_sub(5);

fn job_for(
    site: &str,
    url: String,
    priority: u64,
    user_data: BTreeMap<String, String>,
) -> ScraperDataReturn {
    ScraperDataReturn {
        job: PluginJob {
            site: site.to_string(),
            priority,
            param: vec![ScraperParam::Url(shared_types::Url {
                url,
                ..Default::default()
            })],
            user_data,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Builds `Cookie` header modifiers from the job's login cookies (plus any
/// already attached to the incoming URL). The downloader skips raw
/// `ScraperParam::Login` params, so cookies have to travel as local
/// modifiers on the URL itself or they are never sent.
fn cookie_modifiers(scraperdata: &shared_types::ScraperDataReturn) -> Vec<TargetModifier> {
    let mut cookies: Vec<String> = Vec::new();
    for param in &scraperdata.job.param {
        match param {
            ScraperParam::Login(LoginType::Cookie(name, Some(value))) => {
                cookies.push(format!("{name}={value}"));
            }
            ScraperParam::Url(url) => {
                for m in &url.local_modifiers {
                    if let DownloadModifiers::Header((k, v)) = &m.modifier
                        && k == "Cookie"
                    {
                        cookies.push(v.clone());
                    }
                }
            }
            _ => {}
        }
    }
    if cookies.is_empty() {
        Vec::new()
    } else {
        vec![TargetModifier {
            target: ModifierTarget::Text,
            modifier: DownloadModifiers::Header(("Cookie".into(), cookies.join("; "))),
        }]
    }
}

fn with_modifiers(
    mut entry: ScraperDataReturn,
    modifiers: Vec<TargetModifier>,
) -> ScraperDataReturn {
    if let Some(ScraperParam::Url(url)) = entry.job.param.first_mut() {
        url.local_modifiers = modifiers;
    }
    entry
}

/// Normalizes a user-supplied URL to the single page the parser should fetch.
/// Pagination is followed by the parser via the site's own Next controls,
/// so we never fan out to N pages here.
fn normalize_url(raw: &str) -> Option<String> {
    let parsed = Url::parse(raw).ok()?;
    let path = parsed.path();

    if let Some(id) = extract_submission_id(raw) {
        return Some(format!("https://{SITE_ROOT}/view/{id}/"));
    }

    if path.contains("/search/") {
        // Search is `?q=<query>[&page=<n>]`; keep both, drop anything else.
        let mut out = format!(
            "https://{SITE_ROOT}/search/?q={}",
            form_encode(&query_param(&parsed, "q")?)
        );
        if let Some(page) = query_param(&parsed, "page")
            && page != "1"
        {
            out.push_str(&format!("&page={page}"));
        }
        return Some(out);
    }

    if ["/gallery/", "/favorites/", "/scraps/", "/user/"]
        .iter()
        .any(|p| path.contains(p))
    {
        // Gallery-style pages already encode the page in the path
        // (/gallery/<user>/<n>/); fetch as given (with a canonical
        // trailing slash so visited-markers match).
        return Some(if raw.ends_with('/') {
            raw.to_string()
        } else {
            format!("{raw}/")
        });
    }

    None
}

fn query_param(parsed: &Url, key: &str) -> Option<String> {
    parsed
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
        .filter(|v| !v.is_empty())
}

fn form_encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();
    let modifiers = cookie_modifiers(scraperdata);

    for param in &scraperdata.job.param {
        match param {
            ScraperParam::Url(url) => {
                if let Some(normalized) = normalize_url(&url.url) {
                    let mut entry = with_modifiers(
                        job_for(
                            &scraperdata.job.site,
                            normalized.clone(),
                            if extract_submission_id(&normalized).is_some() {
                                SUBMISSION_PRIORITY
                            } else {
                                LISTING_PRIORITY
                            },
                            BTreeMap::new(),
                        ),
                        modifiers.clone(),
                    );
                    entry.skip_conditions = if let Some(id) = extract_submission_id(&normalized) {
                        // Don't re-visit a submission whose file is stored.
                        vec![shared_types::SkipIf::FileTagRelationship(sub_id_tag(&id))]
                    } else {
                        // Don't re-visit a listing page already parsed.
                        vec![visited_skip(&normalized)]
                    };
                    out.push(entry);
                }
            }
            ScraperParam::Normal(term) if !term.trim().is_empty() => {
                let url = format!("https://{SITE_ROOT}/search/?q={}", form_encode(term.trim()));
                let mut entry = with_modifiers(
                    job_for(
                        &scraperdata.job.site,
                        url.clone(),
                        LISTING_PRIORITY,
                        BTreeMap::new(),
                    ),
                    modifiers.clone(),
                );
                entry.skip_conditions = vec![visited_skip(&url)];
                out.push(entry);
            }
            _ => {}
        }
    }

    out
}

#[unsafe(no_mangle)]
pub fn parser_call(
    text_input: &str,
    source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let document = Html::parse_document(text_input);

    if extract_submission_id(source_url).is_some() {
        // Login-gated submissions render a "System Message" instead of the
        // submission. Stop the job with a useful reason rather than saving
        // the site banner that page carries.
        if is_login_blocked(text_input) {
            return vec![ScraperReturn::Stop(
                "FurAffinity submission is available to registered users only;                  set the PLUGIN_FurAffinity_a_COOKIE / PLUGIN_FurAffinity_b_COOKIE                  settings from a logged-in session"
                    .into(),
            )];
        }
        parse_submission_page(&document, source_url, scraperdata)
    } else if ["/search/", "/gallery/", "/favorites/", "/scraps/", "/user/"]
        .iter()
        .any(|p| source_url.contains(p))
    {
        parse_listing_page(&document, source_url, scraperdata)
    } else {
        vec![ScraperReturn::Nothing]
    }
}

/// True when the page is FA's registered-users-only notice rather than a
/// submission (no main image is rendered).
/// Account name from a /user/<name>/ (or /gallery/<name>/) path.
fn extract_account_name(url: &str) -> Option<String> {
    let mut segments = url.split('/');
    while let Some(seg) = segments.next() {
        if (seg == "user" || seg == "gallery")
            && let Some(name) = segments.next()
            && !name.is_empty()
            && !name.chars().all(|c| c.is_ascii_digit())
        {
            return Some(name.to_string());
        }
    }
    None
}

fn is_login_blocked(html: &str) -> bool {
    (html.contains("available to registered users only")
        || html.contains("please log in or create an account"))
        && !html.contains("id=\"submissionImg\"")
}

fn first_text(document: &Html, selector_str: &str) -> Option<String> {
    let selector = Selector::parse(selector_str).ok()?;
    document
        .select(&selector)
        .next()
        .map(|e| e.text().collect::<String>().trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Canonicalizes HTML for storage as a tag name: attributes within each tag
/// are sorted, so the result is identical regardless of the (nondeterministic)
/// order the HTML serializer happens to emit them in. Without this, every
/// re-scrape can store a "new" description tag differing only in attribute
/// order, orphaning the previous one.
fn normalize_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(lt) = rest.find('<') {
        out.push_str(&rest[..lt]);
        rest = &rest[lt..];
        // Comments / doctype / processing instructions: copy verbatim.
        if rest.starts_with("<!--") || rest.starts_with("<!") || rest.starts_with("<?") {
            if let Some(end) = rest.find('>').map(|i| i + 1) {
                out.push_str(&rest[..end]);
                rest = &rest[end..];
                continue;
            }
            break;
        }
        let Some(gt) = rest.find('>') else { break };
        let tag = &rest[1..gt];
        let tag = tag.trim_end_matches('/');
        let self_closing = tag.len() < rest[1..gt].len();
        let mut parts = tag.splitn(2, char::is_whitespace);
        let name = parts.next().unwrap_or("");
        let attr_str = parts.next().unwrap_or("");
        // Parse attributes into (name, Option<value>) pairs.
        let mut attrs: Vec<(&str, Option<&str>)> = Vec::new();
        let mut a = attr_str.trim();
        while !a.is_empty() {
            a = a.trim_start();
            let name_end = a
                .find(|c: char| c.is_whitespace() || c == '=')
                .unwrap_or(a.len());
            let attr_name = &a[..name_end];
            a = a[name_end..].trim_start();
            if let Some(v) = a.strip_prefix('=') {
                let v = v.trim_start();
                if let Some(q) = v.chars().next().filter(|c| *c == '"' || *c == '\'') {
                    let close = v[1..].find(q).map(|i| i + 1).unwrap_or(v.len() - 1);
                    attrs.push((attr_name, Some(&v[1..close])));
                    a = &v[close + 1..];
                } else {
                    let end = v.find(char::is_whitespace).unwrap_or(v.len());
                    attrs.push((attr_name, Some(&v[..end])));
                    a = &v[end..];
                }
            } else {
                attrs.push((attr_name, None));
            }
        }
        attrs.sort_unstable_by(|a, b| a.0.cmp(b.0));
        out.push('<');
        out.push_str(name);
        for (attr_name, value) in attrs {
            out.push(' ');
            out.push_str(attr_name);
            if let Some(value) = value {
                out.push_str("=\"");
                out.push_str(value);
                out.push('"');
            }
        }
        if self_closing {
            out.push('/');
        }
        out.push('>');
        rest = &rest[gt + 1..];
    }
    out.push_str(rest);
    out
}

fn parse_submission_page(
    document: &Html,
    source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let Some(submission_id) = extract_submission_id(source_url) else {
        return vec![ScraperReturn::Nothing];
    };

    // Every piece of submission data is collected as a bare Tag first;
    // relationships (to the submission and to the file) are applied below.
    let mut data_tags: Vec<Tag> = Vec::new();
    let base_relation = RelationContext {
        tag: sub_id_tag(&submission_id),
        ..Default::default()
    };

    let push = |name: String, namespace: GenericNamespaceObj, data: &mut Vec<Tag>| {
        if !name.is_empty()
            && !data
                .iter()
                .any(|t| t.name == name && t.namespace.name == namespace.name)
        {
            data.push(Tag { name, namespace });
        }
    };

    // Title
    if let Some(title) = first_text(document, ".submission-title h2") {
        push(title, ns("FA_Title", "Submission title"), &mut data_tags);
    }

    // Author: display name + account name from the profile link.
    // e.g. <a href="/user/yggiiggy/"><span class="c-usernameBlockSimple__displayName">Speccychicken</span></a>
    if let Ok(sel) = Selector::parse(".submission-description-artist a[href^=\"/user/\"]")
        && let Some(elem) = document
            .select(&sel)
            // The first match is the avatar link (no text); the author link
            // is the one carrying the display name span.
            .find(|elem| {
                Selector::parse(".c-usernameBlockSimple__displayName")
                    .ok()
                    .and_then(|s| elem.select(&s).next())
                    .is_some()
            })
            .or_else(|| document.select(&sel).next())
    {
        let href = elem.value().attr("href").unwrap_or("");
        let username = href
            .trim_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("")
            .to_string();
        let display = Selector::parse(".c-usernameBlockSimple__displayName")
            .ok()
            .and_then(|s| elem.select(&s).next())
            .map(|e| e.text().collect::<String>().trim().to_string())
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| username.clone());

        if !display.is_empty() {
            let is_account_name = username != display;
            push(
                display,
                ns("FA_Author", "Submission author"),
                &mut data_tags,
            );
            if !username.is_empty() && is_account_name {
                push(
                    username,
                    ns("FA_Username", "Submission author's account name"),
                    &mut data_tags,
                );
            }
        }
    }

    // Posted timestamp (seconds since epoch)
    if let Ok(sel) = Selector::parse(".popup_date[data-time]")
        && let Some(ts) = document
            .select(&sel)
            .next()
            .and_then(|e| e.value().attr("data-time"))
    {
        push(
            ts.to_string(),
            ns(
                "FA_Posted_Timestamp",
                "Submission posted date (unix timestamp)",
            ),
            &mut data_tags,
        );
    }

    // Rating: sidebar badge, falling back to the twitter meta tag.
    let rating = first_text(document, ".submission-page-stats .c-contentRating--general, .submission-page-stats [class*=\"c-contentRating\"]")
        .or_else(|| {
            Selector::parse("meta[name='twitter:data2']")
                .ok()
                .and_then(|s| document.select(&s).next())
                .and_then(|e| e.value().attr("content"))
                .map(str::to_string)
        });
    if let Some(rating) = rating.filter(|r| !r.is_empty()) {
        push(rating, ns("FA_Rating", "Content rating"), &mut data_tags);
    }

    // Category / Theme / Species / Resolution / File Size come as paired
    // label+value spans inside .submission-content-stats, in order.
    let stat_names = ["FA_Category", "FA_Theme", "FA_Species"];
    let stat_descs = [
        "Submission category",
        "Submission theme",
        "Character species",
    ];
    if let Ok(sel) = Selector::parse(".submission-content-stats span span") {
        let values: Vec<String> = document
            .select(&sel)
            .map(|e| e.text().collect::<String>().trim().to_string())
            .collect();
        // First span holds the labels, second holds the values (in order).
        if values.len() >= 10 {
            for (i, (name, desc)) in stat_names.iter().zip(stat_descs.iter()).enumerate() {
                let value = &values[5 + i];
                if !value.is_empty() {
                    push(value.clone(), ns(name, desc), &mut data_tags);
                }
            }
        }
    }

    // Description (raw HTML)
    if let Ok(sel) = Selector::parse(".submission-description-text")
        && let Some(elem) = document.select(&sel).next()
    {
        let desc_html = normalize_html(elem.html().trim());
        push(
            desc_html,
            ns("FA_Description_HTML", "Submission description (raw HTML)"),
            &mut data_tags,
        );
    }

    // Keywords: the actual per-submission tags.
    // Ordinary keywords are links (.submission-tags a[href*="/search/@keywords"]
    // -> "avian", "anthro", ...), but the machine-readable category/theme/
    // species/user tags are *not* links: they render as
    // `<a data-tag-name="c_artwork_digital">` + `<span class="tag-invalid">`,
    // so the link-text selector alone silently drops them. The
    // `data-tag-name` attribute is present on every tag (valid or invalid),
    // so prefer it; fall back to link text for older markup.
    // The main image's `data-tags` attribute carries the same space-
    // separated list ("female dog c_artwork_digital t_all ...") and is
    // merged as a backstop -- `push` de-duplicates.
    // The meta[name=keywords] tag is site-wide boilerplate, not submission data.
    if let Ok(sel) = Selector::parse(".submission-tags a[data-tag-name]") {
        for elem in document.select(&sel) {
            if let Some(kw) = elem.value().attr("data-tag-name") {
                push(kw.to_string(), ns("FA_Tag", "Submission tag/keyword"), &mut data_tags);
            }
        }
    }
    if let Ok(sel) = Selector::parse(".submission-tags a[href*=\"/search/@keywords\"]") {
        for elem in document.select(&sel) {
            let kw = elem.text().collect::<String>().trim().to_string();
            push(kw, ns("FA_Tag", "Submission tag/keyword"), &mut data_tags);
        }
    }
    if let Ok(sel) = Selector::parse("#submissionImg[data-tags]") {
        for elem in document.select(&sel) {
            if let Some(tags) = elem.value().attr("data-tags") {
                for kw in tags.split_whitespace() {
                    push(kw.to_string(), ns("FA_Tag", "Submission tag/keyword"), &mut data_tags);
                }
            }
        }
    }

    // Submission-level tags: all data tied to the submission ID.
    let tags: HashSet<PluginTag> = data_tags
        .iter()
        .map(|t| tag_on(t.name.clone(), t.namespace.clone(), &base_relation))
        .collect();

    // Main file. Prefer the full-resolution source; og:image is a 600px
    // thumbnail (t.furaffinity.net) and must only be a last resort.
    let image_url = submission_image_url(document, &submission_id);
    let Some(image_url) = image_url else {
        return finalize(HashSet::new(), tags, HashSet::new());
    };

    // File-level tags: ALL submission data tied to the file (source URL),
    // so anything searchable on the file itself is attached to it.
    let file_relation = RelationContext {
        tag: source_url_tag(&image_url),
        ..Default::default()
    };
    let mut all_file_tags = vec![
        tag_on(
            image_url.clone(),
            ns("source_url", "Source URL for file"),
            &base_relation,
        ),
        PluginTag {
            tag: sub_id_tag(&submission_id),
            relates_to: Some(file_relation.clone()),
            ..Default::default()
        },
    ];
    for tag in &data_tags {
        all_file_tags.push(PluginTag {
            tag: tag.clone(),
            relates_to: Some(file_relation.clone()),
            ..Default::default()
        });
    }

    let file = FileObject {
        source: Some(FileSource::Url(image_url)),
        hash: Vec::new(),
        tag_list: vec![FileTagAction {
            operation: shared_types::TagOperation::Add,
            tags: all_file_tags,
        }],
        skip_if: vec![],
    };

    let _ = scraperdata; // site is not needed beyond url_dump
    finalize(HashSet::from([file]), tags, HashSet::new())
}

fn submission_image_url(document: &Html, submission_id: &str) -> Option<String> {
    // 1. Full-resolution source on the main image.
    if let Ok(sel) = Selector::parse("#submissionImg")
        && let Some(elem) = document.select(&sel).next()
    {
        if let Some(src) = elem.value().attr("data-fullview-src") {
            return Some(abs_url(src));
        }
        if let Some(src) = elem.value().attr("src")
            && src.contains("d.furaffinity.net")
        {
            return Some(abs_url(src));
        }
    }

    // 2. The Download button points straight at the full file.
    if let Ok(sel) = Selector::parse(".submission-controls-upper a[href*=\"d.furaffinity.net\"]")
        && let Some(href) = document
            .select(&sel)
            .next()
            .and_then(|e| e.value().attr("href"))
    {
        return Some(abs_url(href));
    }

    // 3. Last resort: og:image (thumbnail, but better than nothing).
    // Must reference this submission, or it is a site banner (e.g. the
    // login-blocked page's generic image).
    Selector::parse("meta[property='og:image']")
        .ok()
        .and_then(|s| document.select(&s).next())
        .and_then(|e| e.value().attr("content"))
        .filter(|c| c.contains(submission_id))
        .map(abs_url)
}

fn parse_listing_page(
    document: &Html,
    source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let recursion = scraperdata
        .job
        .user_data
        .get("recursion")
        .is_none_or(|f| f != "false");

    // Submission thumbnails are <figure id="sid-<id>">; fall back to links.
    let mut ids: Vec<String> = Vec::new();
    if let Ok(sel) = Selector::parse("figure[id^=\"sid-\"]") {
        for elem in document.select(&sel) {
            if let Some(id) = elem.value().attr("id").and_then(|i| i.strip_prefix("sid-"))
                && id.chars().all(|c| c.is_ascii_digit())
                && !ids.iter().any(|e| e == id)
            {
                ids.push(id.to_string());
            }
        }
    }
    if ids.is_empty()
        && let Ok(sel) = Selector::parse("a[href*=\"/view/\"]")
    {
        for elem in document.select(&sel) {
            if let Some(href) = elem.value().attr("href")
                && let Some(id) = extract_submission_id(href)
                && !ids.contains(&id)
            {
                ids.push(id);
            }
        }
    }

    let mut jobs: HashSet<ScraperDataReturn> = HashSet::new();
    for id in &ids {
        // Like e6: ask the DB directly first; if the submission's file is
        // already stored, don't even create the job. IPC failures keep the
        // job (the skip condition below is the backstop).
        let already_have = client::get_tag_file(sub_id_tag(id))
            .ok()
            .is_some_and(|f| f.is_some());
        if already_have {
            continue;
        }
        let mut user_data = BTreeMap::new();
        user_data.insert("recursion".into(), recursion.to_string());
        let mut entry = job_for(
            &scraperdata.job.site,
            format!("https://{SITE_ROOT}/view/{id}/"),
            SUBMISSION_PRIORITY,
            user_data,
        );
        entry.skip_conditions = vec![shared_types::SkipIf::FileTagRelationship(sub_id_tag(id))];
        // Keep cookies so adult/restricted submissions stay reachable.
        let entry = with_modifiers(entry, cookie_modifiers(scraperdata));
        jobs.insert(entry);
    }

    // A /user/ profile only shows recent submissions; enqueue the user's
    // full gallery (which paginates) alongside them.
    if source_url.contains("/user/")
        && let Some(account) = extract_account_name(source_url)
    {
        let gallery_url = format!("https://{SITE_ROOT}/gallery/{account}/");
        let mut entry = with_modifiers(
            job_for(
                &scraperdata.job.site,
                gallery_url.clone(),
                LISTING_PRIORITY,
                {
                    let mut user_data = BTreeMap::new();
                    user_data.insert("recursion".into(), recursion.to_string());
                    user_data
                },
            ),
            cookie_modifiers(scraperdata),
        );
        entry.skip_conditions = vec![visited_skip(&gallery_url)];
        jobs.insert(entry);
    }

    // Next page: gallery pages use a "Next" form posting to
    // /gallery/<user>/<n>/; search pages use a GET form with ?page=<n>.
    if recursion && let Some(next) = next_listing_url(document, source_url) {
        let mut user_data = scraperdata.job.user_data.clone();
        user_data.insert("recursion".into(), "true".into());
        let mut entry = with_modifiers(
            job_for(
                &scraperdata.job.site,
                next.clone(),
                LISTING_PRIORITY,
                user_data,
            ),
            cookie_modifiers(scraperdata),
        );
        entry.skip_conditions = vec![visited_skip(&next)];
        jobs.insert(entry);
    }

    // Mark this page visited so it is never fetched again.
    finalize(
        HashSet::new(),
        HashSet::from([visited_page_plugin_tag(source_url)]),
        jobs,
    )
}

fn next_listing_url(document: &Html, source_url: &str) -> Option<String> {
    // Gallery: <form action="/gallery/<user>/<n>/"><button>Next</button></form>
    if let Ok(sel) = Selector::parse("form[action*=\"/gallery/\"]") {
        for form in document.select(&sel) {
            let is_next = form
                .descendants()
                .filter_map(|n| n.value().as_text().map(|t| t.trim().to_string()))
                .any(|t| t == "Next");
            if is_next && let Some(action) = form.value().attr("action") {
                return Some(abs_url(action));
            }
        }
    }

    // Search: <form action="/search/"><input name="q">...<select name="page">...
    if source_url.contains("/search/") {
        let parsed = Url::parse(source_url).ok()?;
        let current: u32 = query_param(&parsed, "page")
            .and_then(|p| p.parse().ok())
            .unwrap_or(1);
        let q = query_param(&parsed, "q")?;

        if let Ok(sel) = Selector::parse("form[action=\"/search/\"] select[name=\"page\"] option") {
            let has_next = document
                .select(&sel)
                .filter_map(|o| o.value().attr("value"))
                .any(|v| v.parse::<u32>().is_ok_and(|n| n == current + 1));
            if has_next {
                return Some(format!(
                    "https://{SITE_ROOT}/search/?q={}&page={}",
                    form_encode(&q),
                    current + 1
                ));
            }
        }
    }

    None
}

fn finalize(
    files: HashSet<FileObject>,
    tags: HashSet<PluginTag>,
    jobs: HashSet<ScraperDataReturn>,
) -> Vec<ScraperReturn> {
    if files.is_empty() && tags.is_empty() && jobs.is_empty() {
        vec![ScraperReturn::Nothing]
    } else {
        vec![ScraperReturn::Data(shared_types::ScraperObject {
            files,
            tags,
            jobs,
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal stand-in for a real /view/ page: same selectors and shape as
    // https://www.furaffinity.net/view/66953097/
    const SUBMISSION_HTML: &str = r#"
    <html><head>
      <meta property="og:image" content="https://t.furaffinity.net/66953097@600-1793980398.jpg" />
      <meta name="twitter:data2" content="General" />
    </head><body>
      <img id="submissionImg" data-fullview-src="//d.furaffinity.net/art/yggiiggy/1793980398/1793980398.yggiiggy_11-3-26_armor.jpg"
           data-preview-src="//t.furaffinity.net/66953097@600-1793980398.jpg"
           src="//d.furaffinity.net/art/yggiiggy/1793980398/1793980398.yggiiggy_11-3-26_armor.jpg" />
      <div class="submission-description-artist">
        <a href="/user/yggiiggy/" class="c-avatarFrameCapable"><img class="avatar" /></a>
        <div class="submission-title"><h2>Modular Space suit</h2></div>
        <span class="c-usernameBlockSimple"><a href="/user/yggiiggy/"><span class="c-usernameBlockSimple__displayName">Speccychicken</span></a></span>
        <span class="popup_date" data-time="1793980398">5 hours ago</span>
      </div>
      <div class="submission-description-text user-submitted-links">Space suit with modularity</div>
      <div class="submission-page-stats"><div class="font-large inline c-contentRating--general">General</div></div>
      <div class="submission-content-stats">
        <span class="highlight"><span>Category</span><span>Theme</span><span>Species</span><span>Resolution</span><span>File Size</span></span>
        <span><span>All</span><span>All</span><span>Unspecified / Any</span><span>1200 x 960</span><span>632.9 kB</span></span>
      </div>
      <div class="submission-tags">
        <a href="/search/@keywords avian">avian</a>
        <a href="/search/@keywords anthro">anthro</a>
        <a href="/search/@keywords rooster">rooster</a>
      </div>
    </body></html>"#;

    const LISTING_HTML: &str = r#"
    <html><body>
      <figure id="sid-66953097"><a href="/view/66953097/">x</a></figure>
      <figure id="sid-66953066"><a href="/view/66953066/">x</a></figure>
      <form action="/gallery/yggiiggy/2/" method="get"><button type="submit">Next</button></form>
    </body></html>"#;

    fn data() -> ScraperDataReturn {
        ScraperDataReturn {
            job: PluginJob {
                site: "furaffinity".into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn tag_names<'a>(obj: &'a shared_types::ScraperObject, ns: &str) -> Vec<&'a str> {
        obj.tags
            .iter()
            .filter(|t| t.tag.namespace.name == ns)
            .map(|t| t.tag.name.as_str())
            .collect()
    }

    #[test]
    fn parse_submission_page() {
        let out = parser_call(
            SUBMISSION_HTML,
            "https://www.furaffinity.net/view/66953097/",
            &data(),
        );
        let ScraperReturn::Data(obj) = &out[0] else {
            panic!("expected Data, got {:?}", out[0])
        };

        // Full-resolution file, not the og:image thumbnail.
        assert_eq!(obj.files.len(), 1);
        let file = obj.files.iter().next().unwrap();
        let FileSource::Url(url) = file.source.as_ref().unwrap() else {
            panic!("expected URL source")
        };
        assert_eq!(
            url,
            "https://d.furaffinity.net/art/yggiiggy/1793980398/1793980398.yggiiggy_11-3-26_armor.jpg"
        );

        assert_eq!(tag_names(obj, "FA_Title"), ["Modular Space suit"]);
        assert_eq!(tag_names(obj, "FA_Author"), ["Speccychicken"]);
        assert_eq!(tag_names(obj, "FA_Username"), ["yggiiggy"]);
        assert_eq!(tag_names(obj, "FA_Rating"), ["General"]);
        assert_eq!(tag_names(obj, "FA_Category"), ["All"]);
        assert_eq!(tag_names(obj, "FA_Species"), ["Unspecified / Any"]);
        let mut keywords = tag_names(obj, "FA_Tag");
        keywords.sort_unstable();
        assert_eq!(keywords, ["anthro", "avian", "rooster"]);

        // File is linked to the submission and carries ALL submission data.
        let file_tag_names: Vec<&str> = file
            .tag_list
            .iter()
            .flat_map(|a| a.tags.iter())
            .map(|t| t.tag.name.as_str())
            .collect();
        for expected in [
            "66953097",
            "avian",
            "anthro",
            "rooster",
            "Modular Space suit",
            "Speccychicken",
            "yggiiggy",
            "General",
            "All",
            "Unspecified / Any",
            "1793980398",
        ] {
            assert!(
                file_tag_names.contains(&expected),
                "file should carry {expected:?}"
            );
        }
        // Resolution/File Size are not collected at all.
        let all_names: Vec<&str> = obj
            .tags
            .iter()
            .chain(file.tag_list.iter().flat_map(|a| a.tags.iter()))
            .map(|t| t.tag.namespace.name.as_str())
            .collect();
        assert!(!all_names.contains(&"FA_Resolution"));
        assert!(!all_names.contains(&"FA_File_Size"));
    }

    #[test]
    fn parse_submission_includes_prefixed_tags() {
        // Real markup from https://www.furaffinity.net/view/66885588/ :
        // category/theme/species/user appear as data-tag-name anchors with
        // tag-invalid spans (not /search/@keywords links), and the same
        // tokens are listed in #submissionImg's data-tags attribute.
        let html = r#"<html><head>
          <meta name="twitter:data2" content="General" />
        </head><body>
          <img id="submissionImg" data-tags="u_ulitochka c_artwork_digital t_all s_unspecified_any female dog booty"
               src="//d.furaffinity.net/art/ulitochka/1793466574/1793466574.ulitochka_5.png" />
          <div class="submission-description-artist">
            <div class="submission-title"><h2>Title</h2></div>
            <span class="popup_date" data-time="1793466574">x</span>
          </div>
          <div class="submission-page-stats"><div class="font-large inline c-contentRating--general">General</div></div>
          <div class="submission-content-stats">
            <span class="highlight"><span>Category</span><span>Theme</span><span>Species</span><span>Resolution</span><span>File Size</span></span>
            <span><span>Artwork (Digital)</span><span>All</span><span>Unspecified / Any</span><span>1472 x 2503</span><span>3.48 MB</span></span>
          </div>
          <div class="submission-tags"><div class="highlight">Keywords</div><div>
            <span class="tags"><span><a href="javascript:void(0);" data-tag-name="female" class="tag-block"></a><a href="/search/@keywords female">female</a></span></span>
            <span class="tags"><span><a href="javascript:void(0);" data-tag-name="dog" class="tag-block"></a><a href="/search/@keywords dog">dog</a></span></span>
            <span class="tags"><span><a href="javascript:void(0);" data-tag-name="booty" class="tag-block"></a><a href="/search/@keywords booty">booty</a></span></span>
            <span class="tags"><span><a href="javascript:void(0);" data-tag-name="u_ulitochka" class="tag-block"></a><span class="tag-invalid">u_ulitochka</span></span></span>
            <span class="tags"><span><a href="javascript:void(0);" data-tag-name="c_artwork_digital" class="tag-block"></a><span class="tag-invalid">c_artwork_digital</span></span></span>
            <span class="tags"><span><a href="javascript:void(0);" data-tag-name="t_all" class="tag-block"></a><span class="tag-invalid">t_all</span></span></span>
            <span class="tags"><span><a href="javascript:void(0);" data-tag-name="s_unspecified_any" class="tag-block"></a><span class="tag-invalid">s_unspecified_any</span></span></span>
          </div></div>
        </body></html>"#;
        let out = parser_call(html, "https://www.furaffinity.net/view/66885588/", &data());
        let ScraperReturn::Data(obj) = &out[0] else {
            panic!("expected Data, got {:?}", out[0])
        };
        let mut keywords = tag_names(obj, "FA_Tag");
        keywords.sort_unstable();
        keywords.dedup();
        assert_eq!(
            keywords,
            [
                "booty",
                "c_artwork_digital",
                "dog",
                "female",
                "s_unspecified_any",
                "t_all",
                "u_ulitochka"
            ]
        );
        // Human-readable category is still collected separately.
        assert_eq!(tag_names(obj, "FA_Category"), ["Artwork (Digital)"]);
        // Prefixed tags are also attached to the file.
        let file = obj.files.iter().next().unwrap();
        let file_tag_names: Vec<&str> = file
            .tag_list
            .iter()
            .flat_map(|a| a.tags.iter())
            .map(|t| t.tag.name.as_str())
            .collect();
        for expected in ["c_artwork_digital", "t_all", "s_unspecified_any", "u_ulitochka"] {
            assert!(
                file_tag_names.contains(&expected),
                "file should carry {expected:?}"
            );
        }
    }

    #[test]
    fn parse_listing_page() {
        let out = parser_call(
            LISTING_HTML,
            "https://www.furaffinity.net/gallery/yggiiggy/",
            &data(),
        );
        let ScraperReturn::Data(obj) = &out[0] else {
            panic!("expected Data, got {:?}", out[0])
        };
        // 2 submissions + 1 next-page job
        assert_eq!(obj.jobs.len(), 3);
        // Next-page (listing) jobs outrank submission jobs, and the page
        // itself is marked visited.
        let next = obj
            .jobs
            .iter()
            .find(|j| matches!(&j.job.param[0], ScraperParam::Url(u) if u.url.ends_with("/2/")))
            .unwrap();
        let sub = obj
            .jobs
            .iter()
            .find(|j| matches!(&j.job.param[0], ScraperParam::Url(u) if u.url.contains("/view/")))
            .unwrap();
        assert!(next.job.priority > sub.job.priority);
        assert!(
            obj.tags
                .iter()
                .any(|t| t.tag.namespace.name == "FA_Visited_Page")
        );
        let urls: Vec<String> = obj
            .jobs
            .iter()
            .filter_map(|j| match &j.job.param[0] {
                ScraperParam::Url(u) => Some(u.url.clone()),
                _ => None,
            })
            .collect();
        assert!(urls.contains(&"https://www.furaffinity.net/view/66953097/".to_string()));
        assert!(urls.contains(&"https://www.furaffinity.net/gallery/yggiiggy/2/".to_string()));
    }

    #[test]
    fn url_dump_does_not_fan_out() {
        let mk = |p: ScraperParam| ScraperDataReturn {
            job: PluginJob {
                site: "furaffinity".into(),
                param: vec![p],
                ..Default::default()
            },
            ..Default::default()
        };
        let url = |s: &str| {
            ScraperParam::Url(shared_types::Url {
                url: s.into(),
                local_modifiers: vec![],
            })
        };

        // One job per input, never 250 pages of search/gallery fan-out.
        assert_eq!(
            url_dump(&mk(url("https://www.furaffinity.net/view/66953097/"))).len(),
            1
        );
        assert_eq!(
            url_dump(&mk(url("https://www.furaffinity.net/gallery/yggiiggy/"))).len(),
            1
        );
        assert_eq!(url_dump(&mk(ScraperParam::Normal("avian".into()))).len(), 1);

        let out = url_dump(&mk(url("https://www.furaffinity.net/view/66953097")));
        assert_eq!(out[0].job.param.len(), 1);
        assert!(matches!(
            &out[0].job.param[0],
            ScraperParam::Url(u) if u.url == "https://www.furaffinity.net/view/66953097/"
        ));
        // Submission jobs run below listing pages and skip when the file
        // for the submission is already stored.
        assert_eq!(out[0].job.priority, SUBMISSION_PRIORITY);
        assert!(matches!(
            out[0].skip_conditions[0],
            shared_types::SkipIf::FileTagRelationship(_)
        ));

        let out = url_dump(&mk(url("https://www.furaffinity.net/gallery/yggiiggy/")));
        assert_eq!(out[0].job.priority, LISTING_PRIORITY);
        assert!(matches!(
            out[0].skip_conditions[0],
            shared_types::SkipIf::ParentsRelate(_)
        ));
    }

    #[test]
    fn normalize_html_is_order_stable() {
        let a = normalize_html(r#"<a href="https://x" class="y" id="z">t</a>"#);
        let b = normalize_html(r#"<a id="z" class="y" href="https://x">t</a>"#);
        let c = normalize_html(r#"<a class="y" id="z" href="https://x">t</a>"#);
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(a, r#"<a class="y" href="https://x" id="z">t</a>"#);
        // Bare attributes, self-closing tags and text pass through.
        assert_eq!(
            normalize_html(r#"<input disabled type="text">x<br/>"#),
            r#"<input disabled type="text">x<br/>"#
        );
    }

    #[test]
    fn url_dump_attaches_cookie_modifiers() {
        let entry = ScraperDataReturn {
            job: PluginJob {
                site: "furaffinity".into(),
                param: vec![
                    ScraperParam::Url(shared_types::Url {
                        url: "https://www.furaffinity.net/view/66953097/".into(),
                        local_modifiers: vec![],
                    }),
                    ScraperParam::Login(LoginType::Cookie("a".into(), Some("AVAL".into()))),
                    ScraperParam::Login(LoginType::Cookie("b".into(), Some("BVAL".into()))),
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        let out = url_dump(&entry);
        assert_eq!(out.len(), 1);
        let ScraperParam::Url(url) = &out[0].job.param[0] else {
            panic!("expected Url param")
        };
        assert_eq!(url.local_modifiers.len(), 1);
        let DownloadModifiers::Header((k, v)) = &url.local_modifiers[0].modifier else {
            panic!("expected Cookie header modifier")
        };
        assert_eq!(k, "Cookie");
        assert_eq!(v, "a=AVAL; b=BVAL");
        // Raw Login params are not forwarded (the downloader skips them).
        assert_eq!(out[0].job.param.len(), 1);
    }

    #[test]
    fn login_blocked_page_stops() {
        let html = r#"<html><head>
            <meta property="og:image" content="https://www.furaffinity.net/media/banners/fa_logo.png" />
            </head><body><div class="systemmessage">
            The owner of this page has elected to make it available to registered users only.
            To view the contents of this page please log in or create an account.
            </div></body></html>"#;
        let out = parser_call(html, "https://www.furaffinity.net/view/66953097/", &data());
        assert!(
            matches!(out[0], ScraperReturn::Stop(_)),
            "expected Stop, got {:?}",
            out[0]
        );
    }
}
