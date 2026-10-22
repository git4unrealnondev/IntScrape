//! Catbox.moe scraper: albums (`catbox.moe/c/{id}`) and direct file links.
//!
//! Two entries share one lib so site routing stays in one place:
//! - `Catbox` handles album pages. It sits on `NoTextDownload` and fetches the
//!   page itself (direct first, `proxy` plugin fallback) because catbox.moe
//!   blocks whole IP ranges and the system download pipeline has no proxy
//!   support. Album files are也为 fetched inside the parser for the same
//!   reason and handed over as bytes.
//! - `Catbox File` handles direct single-file links (`files.catbox.moe`,
//!   `i.catbox.moe`).
//!
//! Files come back in tiny batches so an album with hundreds of videos never
//! holds every transfer in memory at once.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    sync::LazyLock,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use shared_types::{
    CallbackCustomDataReturning, CallbackInfoInput, CallbackReturn, DbSettingsObj, DEFAULT_PRIORITY,
    FileObject, FileSource, FileTagAction, GenericNamespaceObj, GlobalCallbacks, Plugin, PluginJob,
    PluginProperties, PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperObject,
    ScraperReturn, SearchType, SkipIf, StartupThreadType, Tag, TagOperation, Url,
};

const SITE: &str = "catbox";
const SITE_FILE: &str = "catbox file";

const STARTUP_SETTING: &str = "PLUGIN_Catbox_ShouldCheckTags";

const ALBUM_REGEX: &str = r#"(?i)https?://(?:www\.)?catbox\.moe/c/[a-z0-9]{6,}"#;
const FILE_REGEX: &str = r#"(?i)https?://(?:files|i)\.catbox\.moe/[a-z0-9]+\.[a-z0-9]{2,5}"#;

const NS_COLLECTION: &str = "Catbox_Collection";
const NS_TITLE: &str = "Catbox_Collection_Title";
const NS_TEXT: &str = "Catbox_Collection_Text";
const NS_POSITION: &str = "Catbox_Collection_Position";
const NS_SCRAPED: &str = "Catbox_Collection_Scraping_Time";

const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0";

/// Files handed back per `ScraperReturn::Data` batch. Two keeps at most a pair
/// of whole-file buffers in flight; album sizes stay bounded no matter how
/// many attachments the uploader piled onto one page.
const FILES_PER_BATCH: usize = 2;

/// Pause between two file transfers of one album. Catbox resets bulk fetchers
/// mid-run; pacing the parser keeps large albums from dying halfway.
const FILE_PAUSE: Duration = Duration::from_millis(400);

/// Per-request timeouts: short for probe-style page fetches, generous for
/// whole file bodies.
const PAGE_TIMEOUT: Duration = Duration::from_secs(90);
const FILE_TIMEOUT: Duration = Duration::from_secs(1500);

static ALBUM_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(ALBUM_REGEX).unwrap());
static FILE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(FILE_REGEX).unwrap());

/// Album ids are exactly six base-36 chars. The regex accepts longer runs so a
/// match never stops mid-word; anything over six chars is a user page or junk
/// text, not an album, and is ignored.
fn album_url(matched: &str) -> Option<String> {
    let after = matched.split("/c/").nth(1)?;
    if after.len() == 6 && after.chars().all(|c| c.is_ascii_alphanumeric()) {
        Some(matched.to_string())
    } else {
        None
    }
}

fn ns_value(name: &str, description: Option<&str>) -> GenericNamespaceObj {
    GenericNamespaceObj {
        name: name.to_string(),
        description: description.map(str::to_string),
    }
}

fn album_tag(url: String) -> Tag {
    Tag {
        name: url,
        namespace: ns_value(NS_COLLECTION, Some("A CatBox collection album.")),
    }
}

fn source_url_tag(value: impl Into<String>) -> Tag {
    Tag {
        name: value.into(),
        namespace: ns_value("source_url", Some("A source for a file")),
    }
}

fn queued_job(site: &str, url: &str) -> PluginJob {
    PluginJob {
        time: 0,
        reptime: 0,
        priority: DEFAULT_PRIORITY,
        recreation: None,
        site: site.to_string(),
        param: vec![ScraperParam::Url(Url {
            url: url.to_string(),
            local_modifiers: Vec::new(),
        })],
        user_data: BTreeMap::new(),
    }
}

fn is_url_dead(url: &str) -> bool {
    client::dead_url_get(vec![url.to_string()])
        .ok()
        .and_then(|map| map.get(url).copied())
        .unwrap_or(false)
}

fn log_silent(message: String) {
    let _ = client::log_silent(message);
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_millis())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<Plugin> {
    vec![
        Plugin {
            name: SITE.into(),
            properties: vec![
                PluginProperties::Sites(vec![
                    SITE.into(),
                    "catbox album".into(),
                    "catbox.moe".into(),
                    "Catbox Collection".into(),
                ]),
                PluginProperties::Ratelimit(1, Duration::from_secs(2)),
                PluginProperties::ThreadNum(2),
                PluginProperties::JobNum(1),
                PluginProperties::NoTextDownload,
            ],
            callbacks: vec![
                GlobalCallbacks::Start(StartupThreadType::Spawn),
                GlobalCallbacks::Tag((
                    SearchType::Regex(ALBUM_REGEX.into()),
                    vec![],
                    vec![NS_COLLECTION.into()],
                )),
                GlobalCallbacks::Tag((
                    SearchType::Regex(FILE_REGEX.into()),
                    vec![],
                    vec!["source_url".into()],
                )),
            ],
        },
        Plugin {
            name: SITE_FILE.into(),
            properties: vec![
                PluginProperties::Sites(vec![SITE_FILE.into()]),
                PluginProperties::Ratelimit(1, Duration::from_secs(1)),
                PluginProperties::ThreadNum(3),
                PluginProperties::JobNum(2),
                PluginProperties::NoTextDownload,
            ],
            ..Default::default()
        },
    ]
}

/// Queues a scrape for one album link or a direct-file job for one file link.
///
/// Runs for freshly committed tags at runtime, and once from the startup rescan
/// so catbox links that predate this plugin (chan comments, mostly) get scrapes.
#[unsafe(no_mangle)]
fn on_regex(
    matched: &str,
    _full_tag: &str,
    tag_source: &Tag,
    _tag_source_id: u64,
) -> CallbackReturn {
    // Never react to this plugin's own metadata: the album url tag itself would
    // otherwise queue its own scrape forever.
    if tag_source.namespace.name.starts_with("Catbox_") {
        return CallbackReturn::default();
    }

    let is_album = album_url(matched).is_some();
    let is_file = FILE_RE.is_match(matched);
    if !is_album && !is_file {
        return CallbackReturn::default();
    }

    if is_url_dead(matched) {
        return CallbackReturn::default();
    }

    let site = if is_album { SITE } else { SITE_FILE };
    log_silent(format!("CATBOX: queueing {site} scrape for {matched}"));

    CallbackReturn {
        tags: HashSet::new(),
        jobs: HashSet::from([ScraperDataReturn {
            job: queued_job(site, matched),
            skip_conditions: vec![],
        }]),
    }
}

#[unsafe(no_mangle)]
fn on_start() {
    if let Err(error) = handle_on_start() {
        log_silent(format!("CATBOX: startup tag scan failed: {error}"));
    }
}

/// One-shot sweep over existing tags, gated by a settings row the same way the
/// MEGA plugin gates its scan.
fn handle_on_start() -> Result<(), Box<dyn Error>> {
    if client::setting_get(STARTUP_SETTING.into())?.is_some() {
        return Ok(());
    }

    let tags = client::get_tag_id_bulk(client::get_tag_ids_all()?)?;
    let mut queued = HashSet::new();
    let mut failures = 0usize;

    for (tag_id, tag) in tags
        .iter()
        .filter(|(_, tag)| !tag.namespace.name.starts_with("Catbox_"))
    {
        for matched in ALBUM_RE
            .find_iter(&tag.name)
            .chain(FILE_RE.find_iter(&tag.name))
            .map(|m| m.as_str())
        {
            if !queued.insert(matched.to_string()) {
                continue;
            }
            let callback_return = on_regex(matched, &tag.name, tag, *tag_id);
            for job in callback_return.jobs {
                if let Err(error) = client::jobs_add_single(job.job) {
                    failures += 1;
                    log_silent(format!("CATBOX: could not queue {matched}: {error}"));
                }
            }
        }
    }
    if failures > 0 {
        log_silent(format!("CATBOX: {failures} startup scrape(s) failed to queue"));
    }

    client::setting_set(DbSettingsObj {
        name: STARTUP_SETTING.into(),
        description: Some("Whether the catbox plugin should scan existing tags on startup".into()),
        num: None,
        param: Some("False".into()),
    })?;

    Ok(())
}

/// Expands incoming params into scrapes. Legacy `Normal` params (CLI
/// `job add catbox '...'`) carry raw text, so album and file links are found
/// and each rewritten into its own job. Direct-file jobs pass through because
/// their parser fetches the body itself.
#[unsafe(no_mangle)]
pub fn url_dump(data: &ScraperDataReturn) -> Vec<ScraperDataReturn> {
    if data.job.site == SITE_FILE {
        return vec![data.clone()];
    }

    let mut out = Vec::new();
    let mut queued = HashSet::new();

    for param in &data.job.param {
        let text = match param {
            ScraperParam::Normal(text) => text,
            _ => continue,
        };

        for matched in ALBUM_RE
            .find_iter(text)
            .chain(FILE_RE.find_iter(text))
            .map(|m| m.as_str())
        {
            if !queued.insert(matched.to_string()) || is_url_dead(matched) {
                continue;
            }
            // Only valid six-char album links route to the album scraper.
            let site = if album_url(matched).is_some() {
                SITE
            } else {
                SITE_FILE
            };
            out.push(ScraperDataReturn {
                job: queued_job(site, matched),
                skip_conditions: vec![],
            });
        }
    }

    out
}

///
/// Album parsing and single-file fetches both come through here because the
/// plugin declared `NoTextDownload`: the host hands over empty text and the
/// plugin decides what the source url means.
///
#[unsafe(no_mangle)]
pub fn parser_call(
    _text_input: &str,
    source_url: &str,
    scraperdata: &ScraperDataReturn,
) -> Vec<ScraperReturn> {
    if source_url.is_empty() {
        return vec![ScraperReturn::Nothing];
    }

    let mut fetcher = match Fetcher::new() {
        Ok(fetcher) => fetcher,
        Err(error) => {
            return vec![ScraperReturn::Stop(format!(
                "CATBOX: client build failed: {error}"
            ))];
        }
    };

    if scraperdata.job.site == SITE_FILE {
        handle_file(&mut fetcher, source_url)
    } else {
        handle_album(&mut fetcher, source_url)
    }
}

// ---------------------------------------------------------------------------
// Album pages
// ---------------------------------------------------------------------------

fn handle_album(fetcher: &mut Fetcher, album_url: &str) -> Vec<ScraperReturn> {
    match fetcher.fetch(album_url, PAGE_TIMEOUT) {
        Outcome::Body { data, .. } => {
            let page = String::from_utf8_lossy(&data);
            build_album_data(fetcher, album_url, &page)
        }
        Outcome::Dead => {
            if let Err(error) = client::dead_url_add(album_url.to_string()) {
                log_silent(format!("CATBOX: could not mark {album_url} dead: {error}"));
            }
            vec![ScraperReturn::Nothing]
        }
        Outcome::Transport(err) => {
            log_silent(format!("CATBOX: album fetch failed for {album_url}: {err}"));
            vec![ScraperReturn::RetryLater(1800)]
        }
    }
}

fn build_album_data(
    fetcher: &mut Fetcher,
    album_url: &str,
    page: &str,
) -> Vec<ScraperReturn> {
    let Some((title, text, file_urls)) = parse_album_page(page) else {
        log_silent(format!("CATBOX: no album contents parsed from {album_url}"));
        return vec![ScraperReturn::Nothing];
    };

    log_silent(format!(
        "CATBOX: parsed album {album_url}: {} file(s){}",
        file_urls.len(),
        title
            .as_ref()
            .map(|title| format!(" titled \"{title}\""))
            .unwrap_or_default()
    ));

    let album = album_tag(album_url.to_string());

    // Album metadata rides with the first batch so the structure exists as
    // early as possible in commit order.
    let mut start_tags = HashSet::new();
    if let Some(title) = &title {
        start_tags.insert(PluginTag {
            tag: Tag {
                name: title.clone(),
                namespace: ns_value(NS_TITLE, Some("The title of a catbox collection album.")),
            },
            relates_to: Some(RelationContext {
                tag: album.clone(),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    if let Some(text) = &text {
        start_tags.insert(PluginTag {
            tag: Tag {
                name: text.clone(),
                namespace: ns_value(NS_TEXT, Some("The text that is inside of a catbox collection.")),
            },
            relates_to: Some(RelationContext {
                tag: album.clone(),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    start_tags.insert(PluginTag {
        tag: Tag {
            name: now_millis().to_string(),
            namespace: ns_value(NS_SCRAPED, Some("When the album was last scraped.")),
        },
        relates_to: Some(RelationContext {
            tag: album.clone(),
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut out = Vec::new();
    let mut batch_files: HashSet<FileObject> = HashSet::new();
    let mut pending_tags = Some(start_tags);

    for (position, file_url) in file_urls.iter().enumerate() {
        if is_url_dead(file_url) {
            continue;
        }
        let bytes = match fetcher.fetch(file_url, FILE_TIMEOUT) {
            Outcome::Body { data, .. } => data,
            Outcome::Dead => {
                if let Err(error) = client::dead_url_add(file_url.clone()) {
                    log_silent(format!("CATBOX: could not mark {file_url} dead: {error}"));
                }
                continue;
            }
            Outcome::Transport(err) => {
                log_silent(format!("CATBOX: skipping unreachable {file_url}: {err}"));
                continue;
            }
        };
        thread::sleep(FILE_PAUSE);

        let position_ns = ns_value(
            NS_POSITION,
            Some("The position of the image inside of a catbox collection."),
        );
        let source_ns = ns_value("source_url", Some("A source for a file"));

        batch_files.insert(FileObject {
            source: Some(FileSource::Bytes(bytes)),
            hash: vec![],
            tag_list: vec![FileTagAction {
                operation: TagOperation::Add,
                tags: vec![
                    PluginTag {
                        tag: source_url_tag(file_url.clone()),
                        relates_to: Some(RelationContext {
                            tag: album.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    PluginTag {
                        tag: Tag {
                            name: position.to_string(),
                            namespace: position_ns,
                        },
                        relates_to: Some(RelationContext {
                            tag: Tag {
                                name: file_url.clone(),
                                namespace: source_ns,
                            },
                            limit_to: Some(album.clone()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
            }],
            skip_if: vec![SkipIf::FileTagRelationship(source_url_tag(file_url.clone()))],
        });

        if batch_files.len() >= FILES_PER_BATCH {
            out.push(batch(batch_files.drain().collect(), pending_tags.take()));
        }
    }

    if !batch_files.is_empty() || pending_tags.is_some() {
        out.push(batch(batch_files.iter().cloned().collect(), pending_tags.take()));
    }
    if out.is_empty() {
        return vec![ScraperReturn::Nothing];
    }
    out
}

fn batch(files: Vec<FileObject>, tags: Option<HashSet<PluginTag>>) -> ScraperReturn {
    ScraperReturn::Data(ScraperObject {
        files: files.into_iter().collect(),
        jobs: HashSet::new(),
        tags: tags.unwrap_or_default(),
    })
}

/// Pulls the album title, its collection text and the ordered file list.
///
/// Primary pass uses catbox's album markup exactly like the legacy plugin did
/// (`div.title`, `div.imagelist a[href]`); a raw-regex pass over links
/// backfills any album whose markup drifted.
fn parse_album_page(page: &str) -> Option<(Option<String>, Option<String>, Vec<String>)> {
    let document = Html::parse_document(page);

    // Title div: the first non-empty text line is the album title; the rest is
    // the album owner's collection text.
    let mut title = None;
    let mut text = None;
    if let Ok(title_selector) = Selector::parse("div.title") {
        if let Some(node) = document.select(&title_selector).next() {
            let mut lines: Vec<String> = node
                .text()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if !lines.is_empty() {
                title = lines.first().cloned();
                lines.remove(0);
                let combined = lines.join("\n");
                if !combined.trim().is_empty() {
                    text = Some(combined);
                }
            }
        }
    }

    let mut file_urls: Vec<String> = Vec::new();
    let mut seen = HashSet::new();

    if let (Ok(list_selector), Ok(link_selector)) = (
        Selector::parse("div.imagelist"),
        Selector::parse("a[href]"),
    ) {
        for list in document.select(&list_selector) {
            for link in list.select(&link_selector) {
                if let Some(href) = link.value().attr("href") {
                    add_file_link(href, &mut seen, &mut file_urls);
                }
            }
        }
    }
    if file_urls.is_empty() {
        for matched in FILE_RE.find_iter(page).map(|m| m.as_str()) {
            add_file_link(matched, &mut seen, &mut file_urls);
        }
    }

    if file_urls.is_empty() && title.is_none() && text.is_none() {
        return None;
    }
    Some((title, text, file_urls))
}

fn add_file_link(raw: &str, seen: &mut HashSet<String>, file_urls: &mut Vec<String>) {
    let link = normalize_link(raw);
    if !FILE_RE.is_match(&link) || !seen.insert(link.clone()) {
        return;
    }
    file_urls.push(link);
}

/// Trims trailing punctuation the way a link pasted into text typically gets
/// wrapped ("," or "." sentence-enders) and normalizes scheme-less hrefs.
fn normalize_link(link: &str) -> String {
    let mut link = link.trim().to_string();
    if link.starts_with("//") {
        link = format!("https:{link}");
    }
    while link.ends_with(|c: char| ",.)\"'`]>;".contains(c)) {
        link.pop();
    }
    link
}

// ---------------------------------------------------------------------------
// Direct files
// ---------------------------------------------------------------------------

fn handle_file(fetcher: &mut Fetcher, file_url: &str) -> Vec<ScraperReturn> {
    match fetcher.fetch(file_url, FILE_TIMEOUT) {
        // Single-file links never answer with a page; an html body here means
        // the endpoint is serving an error page (or the link isn't a file at
        // all), which is "gone" for our purposes.
        Outcome::Body {
            data,
            is_html: false,
        } => {
            let file = FileObject {
                source: Some(FileSource::Bytes(data)),
                hash: vec![],
                tag_list: vec![FileTagAction {
                    operation: TagOperation::Add,
                    tags: vec![PluginTag {
                        tag: source_url_tag(file_url.to_string()),
                        ..Default::default()
                    }],
                }],
                skip_if: vec![SkipIf::FileTagRelationship(source_url_tag(
                    file_url.to_string(),
                ))],
            };
            vec![ScraperReturn::Data(ScraperObject {
                files: HashSet::from([file]),
                jobs: HashSet::new(),
                tags: HashSet::new(),
            })]
        }
        // An html body on a file link is an error page: the link is gone.
        Outcome::Body { is_html: true, .. } | Outcome::Dead => {
            if let Err(error) = client::dead_url_add(file_url.to_string()) {
                log_silent(format!("CATBOX: could not mark {file_url} dead: {error}"));
            }
            vec![ScraperReturn::Nothing]
        }
        Outcome::Transport(err) => {
            log_silent(format!("CATBOX: file fetch failed for {file_url}: {err}"));
            vec![ScraperReturn::RetryLater(900)]
        }
    }
}
// ---------------------------------------------------------------------------
// Fetch plumbing
// ---------------------------------------------------------------------------

enum Outcome {
    /// A body came back. Text pages expect it to be html; single files treat
    /// an html body as "this link is not a file".
    Body {
        data: Vec<u8>,
        is_html: bool,
    },
    /// The link is gone (4xx) — no point revisiting it.
    Dead,
    /// The route is broken (dns, reset, timeout, net trouble).
    Transport(String),
}

/// Direct-first fetcher with the `proxy` plugin as fallback route.
///
/// catbox.moe blocks whole IP ranges — the legacy plugin ran into that on this
/// very box. Trying direct first keeps environments where catbox is reachable
/// fast; any transport failure retries once through a pooled proxy. A 404 is
/// never retried through a proxy: the answer would not change.
struct Fetcher {
    runtime: tokio::runtime::Runtime,
    direct: Client,
    proxy: Option<(ProxyRoute, Client)>,
}

struct ProxyRoute {
    /// The proxy url as the plugin gave it, used for reports.
    url: String,
    proxy: reqwest::Proxy,
}

fn client_build(proxy: Option<reqwest::Proxy>) -> Result<Client, Box<dyn Error>> {
    let mut builder = Client::builder()
        .use_rustls_tls()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(12))
        .redirect(reqwest::redirect::Policy::limited(6));
    if let Some(proxy) = proxy {
        builder = builder.proxy(proxy);
    }
    Ok(builder.build()?)
}

impl Fetcher {
    fn new() -> Result<Self, Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let direct = client_build(None)?;
        Ok(Self {
            runtime,
            direct,
            proxy: None,
        })
    }

    fn fetch(&mut self, url: &str, timeout: Duration) -> Outcome {
        match self.request(&self.direct, url, timeout) {
            Ok(outcome) => outcome,
            Err(err) => {
                // Own the route/client before requesting again so the mutable
                // proxy build step and the report step don't fight over `self`.
                let proxy = self
                    .proxy_client()
                    .map(|client| client.clone());
                let Some(proxy_client) = proxy else {
                    return Outcome::Transport(err);
                };
                log_silent(format!("CATBOX: {url} failed directly, retrying via proxy"));
                match self.request(&proxy_client, url, timeout) {
                    Ok(outcome) => {
                        self.report(true);
                        outcome
                    }
                    Err(proxy_err) => {
                        self.report(false);
                        Outcome::Transport(proxy_err)
                    }
                }
            }
        }
    }

    /// Reports how the last proxied request went, when one is on record.
    fn report(&self, success: bool) {
        if let Some((route, _)) = &self.proxy {
            report_proxy(route, success);
        }
    }

    fn proxy_client(&mut self) -> Option<&Client> {
        if self.proxy.is_none() {
            let route = working_proxy()?;
            match client_build(Some(route.proxy.clone())) {
                Ok(proxy_client) => self.proxy = Some((route, proxy_client)),
                Err(err) => log_silent(format!("CATBOX: rejecting proxy route: {err}")),
            }
        }
        self.proxy.as_ref().map(|(_, client)| client)
    }

    fn request(
        &self,
        client: &Client,
        url: &str,
        timeout: Duration,
    ) -> Result<Outcome, String> {
        let response = self.runtime.block_on(async {
            client
                .get(url)
                .timeout(timeout)
                .send()
                .await
                .map_err(|err| err.to_string())
        })?;

        if response.status().as_u16() == 404 {
            return Ok(Outcome::Dead);
        }

        let is_html = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|content_type| content_type.starts_with("text/html"));

        let data = self
            .runtime
            .block_on(async {
                response
                    .bytes()
                    .await
                    .map_err(|err| err.to_string())
            })?
            .to_vec();

        Ok(Outcome::Body { data, is_html })
    }
}

// ---------------------------------------------------------------------------
// Proxy plumbing (copied pattern from the MEGA plugin)
// ---------------------------------------------------------------------------

/// Calls a callback on the `proxy` plugin.
///
/// Returns `None` when the IPC round trip fails. The host answers unknown
/// callbacks with an empty map, so `None` maps to "no route", not an error.
fn proxy_plugin_call(
    func: &str,
    args: Vec<(&str, CallbackCustomDataReturning)>,
) -> Option<HashMap<String, CallbackCustomDataReturning>> {
    let info = CallbackInfoInput {
        vers: 0,
        data_name: args.iter().map(|(name, _)| (*name).to_string()).collect(),
        data: args.into_iter().map(|(_, value)| value).collect(),
    };
    client::external_plugin_call(func.to_string(), info).ok()
}

/// Asks the `proxy` plugin for a route that should be working right now.
///
/// `None` means "connect directly": the plugin has nothing to offer or its
/// entry is a SOCKS url this crate cannot speak.
fn working_proxy() -> Option<ProxyRoute> {
    let out = proxy_plugin_call(
        "proxy_get_proxy",
        vec![(
            "site",
            CallbackCustomDataReturning::String(SITE.to_string()),
        )],
    )?;

    let url = match out.get("proxy_url") {
        Some(CallbackCustomDataReturning::String(text)) => text.trim().to_string(),
        _ => String::new(),
    };
    if url.is_empty() {
        return None;
    }

    let kind = match out.get("proxy_type") {
        Some(CallbackCustomDataReturning::String(text)) => text.trim().to_ascii_lowercase(),
        _ => String::new(),
    };

    // `proxy_type` only matters when the stored url has no scheme, which is the
    // usual shape for a scraped proxy list.
    let candidate = if url.contains("://") {
        if is_socks(&url) {
            log_silent(format!(
                "CATBOX: proxy offered SOCKS route `{url}`; this crate has no \
                 SOCKS support, connecting directly"
            ));
            return None;
        }
        url.clone()
    } else {
        if matches!(kind.as_str(), "socks4" | "socks4a" | "socks5" | "socks5h") {
            log_silent(format!(
                "CATBOX: proxy offered SOCKS route `{url}` ({kind}); this crate \
                 has no SOCKS support, connecting directly"
            ));
            return None;
        }
        let scheme = if kind == "https" { "https" } else { "http" };
        format!("{scheme}://{url}")
    };

    let proxy = match reqwest::Proxy::all(&candidate) {
        Ok(proxy) => proxy,
        Err(error) => {
            log_silent(format!("CATBOX: rejecting proxy `{candidate}`: {error}"));
            return None;
        }
    };
    Some(ProxyRoute { url, proxy })
}

/// Tells the `proxy` plugin how the request went so the ranking learns.
fn report_proxy(route: &ProxyRoute, success: bool) {
    let _ = proxy_plugin_call(
        "proxy_report",
        vec![
            (
                "proxy_url",
                CallbackCustomDataReturning::String(route.url.clone()),
            ),
            (
                "site",
                CallbackCustomDataReturning::String(SITE.to_string()),
            ),
            (
                "success",
                CallbackCustomDataReturning::U64(u64::from(success)),
            ),
        ],
    );
}

/// Whether a proxy url uses a SOCKS scheme.
fn is_socks(candidate: &str) -> bool {
    let Some((scheme, _)) = candidate.split_once("://") else {
        return false;
    };
    matches!(scheme, "socks4" | "socks4a" | "socks5" | "socks5h")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_ALBUM: &str = r#"
<html><body>
<div class="title">My pics<span>Collection text here</span></div>
<div class="imagelist">
<a href="https://files.catbox.moe/abc123.jpg"><img src="https://files.catbox.moe/abc123.jpg"></a>
<a href="https://files.catbox.moe/def456.mp4">def456.mp4</a>
</div>
</body></html>"#;

    #[test]
    fn album_regex_shapes() {
        assert!(ALBUM_RE.is_match("https://catbox.moe/c/qzmnbs"));
        assert!(ALBUM_RE.is_match("http://www.catbox.moe/c/qzmnbs"));
        assert_eq!(ALBUM_RE.find("https://files.catbox.moe/ci9dyv.mp4"), None);
        // Run-on junk must match the regex but be rejected by id validation.
        assert!(ALBUM_RE.is_match("https://catbox.moe/c/toolongid"));
        assert_eq!(album_url("https://catbox.moe/c/toolongid"), None);
        assert_eq!(
            album_url("https://catbox.moe/c/qzmnbs").as_deref(),
            Some("https://catbox.moe/c/qzmnbs")
        );
    }

    #[test]
    fn file_regex_shapes() {
        assert!(FILE_RE.is_match("https://files.catbox.moe/ci9dyv.mp4"));
        assert!(FILE_RE.is_match("https://i.catbox.moe/ab12cd.png"));
        assert_eq!(FILE_RE.find("https://catbox.moe/c/qzmnbs"), None);
    }

    #[test]
    fn parses_album_page() {
        let (title, text, files) = parse_album_page(SAMPLE_ALBUM).unwrap();
        assert_eq!(title.as_deref(), Some("My pics"));
        assert_eq!(text.as_deref(), Some("Collection text here"));
        assert_eq!(
            files,
            vec![
                "https://files.catbox.moe/abc123.jpg".to_string(),
                "https://files.catbox.moe/def456.mp4".to_string(),
            ]
        );
    }

    #[test]
    fn schemeless_and_punctuated_hrefs_normalize() {
        let mut seen = HashSet::new();
        let mut urls = Vec::new();
        add_file_link("//files.catbox.moe/xy1234.webm,", &mut seen, &mut urls);
        assert_eq!(urls, vec!["https://files.catbox.moe/xy1234.webm".to_string()]);
    }

    #[test]
    fn junk_page_yields_none() {
        assert!(parse_album_page("<html><body>404 not found</body></html>").is_none());
    }
}
