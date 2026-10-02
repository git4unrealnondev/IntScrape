//! Job state: what a job is fetching, and how the next one is built.
//!
//! A Redgifs job resolves to exactly one of four page kinds, recorded in
//! `PluginJob::user_data` so every page is an independent, resumable job and a
//! restart picks up exactly where it stopped:
//!
//! | kind       | endpoint                                  |
//! |------------|-------------------------------------------|
//! | `listing`  | `/v2/gifs/search?search_text=..`         |
//! | `user`     | `/v2/users/{name}/search`                 |
//! | `niche`    | `/v2/niches/{name}/gifs`                  |
//! | `gif`      | `/v2/gifs/{id}`                           |
//! | `gallery`  | `/v2/gallery/{uuid}`                      |

use std::collections::BTreeMap;

use shared_types::{
    DEFAULT_PRIORITY, DownloadModifiers, ModifierTarget, PluginJob, ScraperDataReturn,
    ScraperParam, SkipIf, TargetModifier, Url,
};

use crate::auth::SITE_ROOT;

/// The site alias jobs are filed under. Must appear in
/// `PluginProperties::Sites` or `PluginManager::match_plugin` will never route
/// a job here.
pub const SITE: &str = "redgifs";

/// Descriptive user agent.
///
/// Redgifs rejects generic agents outright, the same as Reddit. This follows
/// the `<platform>:<app>:<version> (<contact>)` convention Redgifs' own docs
/// use for clients.
pub const USER_AGENT: &str = "linux:IntScrape:0.1.0 (by /u/intscrape_scraper)";

/// `user_data` keys.
pub mod key {
    /// `PageKind::as_str()`.
    pub const KIND: &str = "redgifs_kind";
    /// Gif id, for the `gif` kind.
    pub const GIF_ID: &str = "redgifs_gif_id";
    /// Uploader name, for the `user` kind.
    pub const USER: &str = "redgifs_user";
    /// Tag/niche/search term, for the `listing` and `niche` kinds.
    pub const SEARCH: &str = "redgifs_search";
    /// Gallery uuid, for the `gallery` kind.
    pub const GALLERY: &str = "redgifs_gallery";
    /// 1-based page number.
    pub const PAGE: &str = "redgifs_page";
    /// Ordering, where the endpoint accepts one.
    pub const ORDER: &str = "redgifs_order";
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageKind {
    /// `/v2/gifs/search?search_text=..` — the workhorse tag search.
    Listing,
    /// `/v2/users/{name}/search` — every gif from one uploader.
    User,
    /// `/v2/niches/{name}/gifs` — a category.
    Niche,
    /// `/v2/gifs/{id}` — one gif.
    Gif,
    /// `/v2/gallery/{uuid}` — the siblings of a gallery entry.
    Gallery,
}

impl PageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PageKind::Listing => "listing",
            PageKind::User => "user",
            PageKind::Niche => "niche",
            PageKind::Gif => "gif",
            PageKind::Gallery => "gallery",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "listing" => Some(PageKind::Listing),
            "user" => Some(PageKind::User),
            "niche" => Some(PageKind::Niche),
            "gif" => Some(PageKind::Gif),
            "gallery" => Some(PageKind::Gallery),
            _ => None,
        }
    }
}

/// Orders Redgifs accepts on the user search endpoint.
pub const ORDERS: &[&str] = &["new", "top", "trending", "views", "likes"];

/// The first page of a listing, used to seed a fresh job.
pub const FIRST_PAGE: u64 = 1;

/// Builds the `PluginJob` for one page.
///
/// The endpoint is rebuilt from `user_data` rather than carried as a url,
/// because unlike Reddit's web pages Redgifs' api is the only interface: there
/// is no HTML page to pass through to.
pub fn job_for(kind: PageKind, mut user_data: BTreeMap<String, String>) -> PluginJob {
    // The kind is recorded here rather than by each caller, so a job can never
    // be built whose declared kind disagrees with the state it carries.
    user_data.insert(key::KIND.to_string(), kind.as_str().to_string());

    PluginJob {
        site: SITE.to_string(),
        priority: DEFAULT_PRIORITY,
        param: vec![ScraperParam::Url(Url {
            // Redgifs exposes no scrapable HTML api url, so the site root is a
            // placeholder that the plugin replaces in `url_dump`. It must
            // still be a valid url for the host's bookkeeping.
            url: format!("{SITE_ROOT}/"),
            local_modifiers: api_modifiers(),
        })],
        user_data,
        ..Default::default()
    }
}

/// Job for one gallery, optionally skipping when its first member is held.
pub fn job_for_gallery(gallery: &str, skip_conditions: Vec<SkipIf>) -> ScraperDataReturn {
    let mut user_data = BTreeMap::new();
    user_data.insert(key::GALLERY.to_string(), gallery.to_string());

    ScraperDataReturn {
        job: job_for(PageKind::Gallery, user_data),
        skip_conditions,
    }
}

/// Job for the next page of an existing chain.
pub fn job_for_next_page(
    kind: PageKind,
    template: &BTreeMap<String, String>,
    page: u64,
) -> ScraperDataReturn {
    let mut user_data = template.clone();
    user_data.insert(key::KIND.to_string(), kind.as_str().to_string());
    user_data.insert(key::PAGE.to_string(), page.to_string());

    ScraperDataReturn {
        job: job_for(kind, user_data),
        skip_conditions: Vec::new(),
    }
}

/// The continuation job for the page after `next_page`, or `None` when the
/// chain is finished or over budget.
///
/// `pages` is Redgifs' reported total. A `None` means it omitted the field, in
/// which case we rely on the chain budget alone.
pub fn continuation(
    kind: PageKind,
    template: &BTreeMap<String, String>,
    next_page: u64,
    pages: Option<u64>,
) -> Option<ScraperDataReturn> {
    if next_page > crate::limits::MAX_PAGES_PER_CHAIN {
        return None;
    }
    if let Some(pages) = pages
        && next_page > pages
    {
        return None;
    }
    Some(job_for_next_page(kind, template, next_page))
}

/// Modifiers applied to the placeholder url.
///
/// Redgifs' Referer and Origin come from `api::get` on every request; these are
/// set here too so the host's shared client carries them, matching how the
/// other plugins declare site requirements.
pub fn api_modifiers() -> Vec<TargetModifier> {
    vec![
        TargetModifier {
            target: ModifierTarget::Text,
            modifier: DownloadModifiers::Useragent(USER_AGENT.to_string()),
        },
        TargetModifier {
            target: ModifierTarget::Text,
            modifier: DownloadModifiers::Header(("Accept".to_string(), ACCEPT.to_string())),
        },
    ]
}

/// `Accept` header Redgifs requires on api calls.
pub const ACCEPT: &str = "application/json, text/plain, */*";

/// Turns a user-created job into its first request.
///
/// Accepts either a bare term (`anal`), an explicit `user:NAME` / `niche:NAME`
/// / `gif:ID` prefix, or a Redgifs url, which is decomposed back into the same
/// shapes. Returns `None` for anything unrecognisable so a typo produces no
/// work instead of a wrong crawl.
pub fn request_from_params(job: &PluginJob) -> Option<PageKind> {
    // A continuation job already knows its kind. Note the `and_then`: using `?`
    // here would return None for a *fresh* job, which has no kind yet, and
    // reject every job the user just created.
    if let Some(kind) = job
        .user_data
        .get(key::KIND)
        .and_then(|value| PageKind::parse(value))
    {
        return Some(kind);
    }

    let normals: Vec<&str> = job
        .param
        .iter()
        .filter_map(|param| match param {
            ScraperParam::Normal(value) => Some(value.as_str()),
            _ => None,
        })
        .collect();

    // A recognised prefix *claims* its term: if the term after it is invalid we
    // return None rather than letting it fall through to a tag search, which
    // would turn `gif:not an id` into a search for that literal string.
    for (prefix, kind) in [
        ("user:", PageKind::User),
        ("niche:", PageKind::Niche),
        ("gif:", PageKind::Gif),
    ] {
        if let Some(term) = normals.iter().find_map(|value| value.strip_prefix(prefix)) {
            return valid_term(term, kind).then_some(kind);
        }
    }

    for param in &job.param {
        if let ScraperParam::Url(url) = param
            && let Some(kind) = kind_from_url(&url.url)
        {
            return Some(kind);
        }
    }

    if let Some(term) = normals.first()
        && valid_term(term, PageKind::Listing)
    {
        return Some(PageKind::Listing);
    }

    None
}

/// Whether a user-supplied term is safe to put in a query string.
fn valid_term(term: &str, kind: PageKind) -> bool {
    let trimmed = term.trim();
    if trimmed.is_empty() || trimmed.len() > 100 {
        return false;
    }
    match kind {
        // Gif ids are restricted; usernames and niches are not, since they go
        // into a query value rather than a path.
        PageKind::Gif => crate::parse::GifId::parse(trimmed).is_some(),
        _ => trimmed.chars().all(|c| !c.is_control()),
    }
}

/// Decomposes a Redgifs url back into a page kind.
fn kind_from_url(url: &str) -> Option<PageKind> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("//"))
        .unwrap_or(url);
    let host = rest.split('/').next()?;
    if !host.to_ascii_lowercase().contains("redgifs.com") {
        return None;
    }

    let segments = rest
        .split(['/', '?', '#'])
        .filter(|segment| !segment.is_empty())
        .skip(1)
        .collect::<Vec<_>>();

    match segments.first().copied()? {
        // `/gifs/<term>` is a search, matching gallery-dl's extractors. A
        // lowercase alphanumeric segment is indistinguishable from a gif id,
        // so search is the safe reading -- the alternative turns a tag search
        // into a single-gif fetch that 404s.
        "gifs" => Some(PageKind::Listing),
        // `/watch/<id>` and `/ifr/<id>` are unambiguous single gifs.
        "watch" | "ifr" => segments
            .get(1)
            .and_then(|id| crate::parse::GifId::parse(id).map(|_| PageKind::Gif)),
        "users" => segments
            .get(1)
            .and_then(|name| valid_term(name, PageKind::User).then_some(PageKind::User)),
        "niches" => segments
            .get(1)
            .and_then(|name| valid_term(name, PageKind::Niche).then_some(PageKind::Niche)),
        "search" | "browse" => Some(PageKind::Listing),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(normals: &[&str]) -> PluginJob {
        PluginJob {
            param: normals
                .iter()
                .map(|value| ScraperParam::Normal((*value).to_string()))
                .collect(),
            ..Default::default()
        }
    }

    fn job_with_url(url: &str) -> PluginJob {
        PluginJob {
            param: vec![ScraperParam::Url(Url {
                url: url.to_string(),
                ..Default::default()
            })],
            ..Default::default()
        }
    }

    #[test]
    fn parses_explicit_prefixes() {
        assert_eq!(
            request_from_params(&job(&["user:somebody"])),
            Some(PageKind::User)
        );
        assert_eq!(
            request_from_params(&job(&["niche:solo"])),
            Some(PageKind::Niche)
        );
        assert_eq!(
            request_from_params(&job(&["gif:Abc123"])),
            Some(PageKind::Gif)
        );
    }

    #[test]
    fn bare_term_is_a_tag_search() {
        assert_eq!(
            request_from_params(&job(&["anal"])),
            Some(PageKind::Listing)
        );
    }

    #[test]
    fn decomposes_site_urls() {
        for (url, expected) in [
            (
                "https://www.redgifs.com/watch/ShrillCourageousComet",
                PageKind::Gif,
            ),
            (
                "https://www.redgifs.com/ifr/ShrillCourageousComet",
                PageKind::Gif,
            ),
            ("https://www.redgifs.com/gifs/anal", PageKind::Listing),
            ("https://www.redgifs.com/users/somebody", PageKind::User),
            ("https://www.redgifs.com/niches/solo", PageKind::Niche),
            ("https://redgifs.com/search?query=anal", PageKind::Listing),
        ] {
            assert_eq!(
                request_from_params(&job_with_url(url)),
                Some(expected),
                "{url}"
            );
        }
    }

    #[test]
    fn rejects_foreign_and_malformed_urls() {
        for url in [
            "https://example.com/gifs/abc",
            "not a url",
            "https://www.redgifs.com/",
            "",
        ] {
            assert_eq!(request_from_params(&job_with_url(url)), None, "{url}");
        }
    }

    #[test]
    fn rejects_unusable_terms() {
        assert_eq!(request_from_params(&job(&[""])), None);
        assert_eq!(request_from_params(&job(&["   "])), None);
        assert_eq!(request_from_params(&job(&["gif:not a valid id"])), None);
        assert_eq!(request_from_params(&job(&["x".repeat(101).as_str()])), None);
        assert_eq!(request_from_params(&PluginJob::default()), None);
    }

    #[test]
    fn user_data_kind_takes_priority() {
        let mut data = BTreeMap::new();
        data.insert(
            key::KIND.to_string(),
            PageKind::Gallery.as_str().to_string(),
        );
        assert_eq!(
            request_from_params(&PluginJob {
                user_data: data,
                ..Default::default()
            }),
            Some(PageKind::Gallery)
        );
    }

    #[test]
    fn continuation_stops_at_the_chain_budget() {
        let template = BTreeMap::new();
        assert!(continuation(PageKind::Listing, &template, 1, Some(1000)).is_some());
        // Over the page budget even though redgifs reported thousands.
        assert!(continuation(PageKind::Listing, &template, 99, Some(5000)).is_none());
    }

    #[test]
    fn continuation_stops_at_the_reported_last_page() {
        let template = BTreeMap::new();
        // `pages: 3` means pages 1..=3 exist, so a next_page of 3 is still
        // valid and 4 is not.
        assert!(continuation(PageKind::Listing, &template, 4, Some(3)).is_none());
        assert!(continuation(PageKind::Listing, &template, 3, Some(3)).is_some());
        assert!(continuation(PageKind::Listing, &template, 2, Some(3)).is_some());
    }

    #[test]
    fn continuation_keeps_template_state() {
        let mut template = BTreeMap::new();
        template.insert(key::SEARCH.to_string(), "anal".to_string());

        let next = continuation(PageKind::Listing, &template, 2, Some(9)).unwrap();
        assert_eq!(
            next.job.user_data.get(key::SEARCH).map(String::as_str),
            Some("anal")
        );
        assert_eq!(
            next.job.user_data.get(key::PAGE).map(String::as_str),
            Some("2")
        );
    }

    #[test]
    fn jobs_are_filed_under_the_plugin_site() {
        let job = job_for_gallery("gal-1", Vec::new()).job;
        assert_eq!(job.site, SITE);
        assert_eq!(job.priority, DEFAULT_PRIORITY);
        assert_eq!(
            job.user_data.get(key::KIND).map(String::as_str),
            Some("gallery")
        );
    }

    #[test]
    fn useragent_is_descriptive() {
        // Redgifs rejects generic agents; assert the convention is kept.
        assert!(USER_AGENT.contains(':'));
        assert!(USER_AGENT.contains("IntScrape"));
    }
}
