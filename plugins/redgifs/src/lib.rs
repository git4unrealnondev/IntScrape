//! Redgifs scraper plugin for IntScrape.
//!
//! Downloads gifs from Redgifs and records the uploader, gallery and tag
//! relationships so a downloaded file stays connected to where it came from.
//!
//! # Usage
//!
//! ```text
//! intscrape job add --site redgifs "anal"                     # tag search
//! intscrape job add --site redgifs "niche:solo"               # a category
//! intscrape job add --site redgifs "user:someUploader"        # one uploader
//! intscrape job add --site redgifs "gif:ShrillCourageousComet"# one gif
//! intscrape job add --site redgifs "https://www.redgifs.com/watch/ID"
//! ```
//!
//! # How a scrape walks the api
//!
//! ```text
//! url_dump     "anal"  ->  GET /v2/gifs/search?search_text=anal&page=1&count=100
//! parser_call  listing  ->  files + tags for each gif
//!                             + a job per gallery referenced
//!                             + one continuation page
//! parser_call  gallery  ->  files + tags for every sibling, tagged by position
//! ```
//!
//! # What lands in the database
//!
//! `Parents` rows carry the two structural relations Redgifs has:
//!
//! ```text
//! RedgifsGif(id) -> RedgifsUser(name)     gif uploaded by user
//! RedgifsGif(id) -> RedgifsGallery(uuid)  gif is one entry of a gallery
//! ```
//!
//! Everything else (`RedgifsTag`, `RedgifsNiche`, counters, dimensions) is a
//! value tag scoped to its gif via `limit_to`, which is what keeps a tag shared
//! by thousands of gifs from collapsing into one anonymous row. Files repeat
//! those tags in their own `tag_list`, so a downloaded file is searchable
//! through them.
//!
//! # Authentication
//!
//! Every `/v2/*` endpoint needs `Authorization: Bearer <token>` from
//! `/v2/auth/temporary`, which requires no credentials. The token is cached for
//! 600s and refetched automatically after a 401. See [`auth`].

mod api;
mod auth;
mod limits;
#[cfg(test)]
mod live_tests;
mod media;
mod namespaces;
mod parse;
mod request;

use std::time::Duration;

use shared_types::{
    LoginNeed, LoginType, Plugin, PluginProperties, ScraperDataReturn, ScraperReturn,
};

use api::{ApiError, Page};
use parse::{GifId, Harvest, Options, gallery_jobs};
use request::{PageKind, key, request_from_params};

/// Plugin registration, read once by the host at startup.
#[unsafe(no_mangle)]
pub fn get_plugin_info() -> Vec<Plugin> {
    vec![Plugin {
        name: "Redgifs".to_string(),
        properties: vec![
            // The token is shared across jobs, but the per-endpoint request
            // budget is not, so this is a global ceiling for the plugin rather
            // than a per-job one. Redgifs starts returning 429 well above this.
            PluginProperties::Ratelimit(20, Duration::from_secs(1)),
            PluginProperties::Sites(vec![
                request::SITE.to_string(),
                "redgifs.com".to_string(),
                "www.redgifs.com".to_string(),
                "media.redgifs.com".to_string(),
                "i.redgifs.com".to_string(),
            ]),
            PluginProperties::JobNum(4),
            PluginProperties::ThreadNum(4),
            // Redgifs rejects generic user agents outright.
            PluginProperties::Modifier(shared_types::TargetModifier {
                target: shared_types::ModifierTarget::Text,
                modifier: shared_types::DownloadModifiers::Useragent(
                    request::USER_AGENT.to_string(),
                ),
            }),
            // Optional: a real account raises the rate limit and unlocks
            // `/v2/search/gifs`, which the temporary token cannot reach.
            PluginProperties::Login((
                LoginNeed::Optional,
                LoginType::Api(
                    "Redgifs access token. Only needed for the full-text \
                     /v2/search/gifs endpoint; tag search works without it."
                        .to_string(),
                    None,
                ),
            )),
        ],
        ..Default::default()
    }]
}

/// Resolves a job into the page it should fetch.
///
/// Unlike Reddit there is no HTML page to pass through: Redgifs' json api is
/// the only interface, so `url_dump` does not emit download jobs at all. The
/// host's per-param loop simply finds no url to fetch, and `parser_call` does
/// the requesting itself.
#[unsafe(no_mangle)]
pub fn url_dump(scraperdata: &ScraperDataReturn) -> Vec<ScraperDataReturn> {
    let job = &scraperdata.job;

    // A continuation or a spawned gallery already knows exactly what it wants.
    if let Some(kind) = job
        .user_data
        .get(key::KIND)
        .and_then(|value| PageKind::parse(value))
    {
        let mut user_data = job.user_data.clone();
        user_data.insert(key::KIND.to_string(), kind.as_str().to_string());
        return vec![ScraperDataReturn {
            job: request::job_for(kind, user_data),
            skip_conditions: Vec::new(),
        }];
    }

    // Otherwise seed the first page from the user's parameters. No fallback
    // term: a job we cannot resolve must produce no work rather than guess.
    let Some(kind) = request_from_params(job) else {
        log::error!(
            "Redgifs: job on site '{}' has no recognisable tag, user, niche, gif or url; nothing to scrape",
            job.site
        );
        return Vec::new();
    };

    let mut user_data = job.user_data.clone();
    user_data.insert(key::KIND.to_string(), kind.as_str().to_string());
    user_data.insert(key::PAGE.to_string(), request::FIRST_PAGE.to_string());

    if let Some(term) = first_term(job) {
        let (slot, value) = match kind {
            PageKind::User => (key::USER, term),
            PageKind::Niche | PageKind::Listing => (key::SEARCH, term),
            _ => (key::GIF_ID, term),
        };
        user_data.insert(slot.to_string(), value);
    }

    vec![ScraperDataReturn {
        job: request::job_for(kind, user_data),
        skip_conditions: Vec::new(),
    }]
}

/// The user's term, with any `user:`/`niche:`/`gif:` prefix removed.
fn first_term(job: &shared_types::PluginJob) -> Option<String> {
    let normals: Vec<&str> = job
        .param
        .iter()
        .filter_map(|param| match param {
            shared_types::ScraperParam::Normal(value) => Some(value.as_str()),
            _ => None,
        })
        .collect();

    for prefix in ["user:", "niche:", "gif:"] {
        if let Some(term) = normals.iter().find_map(|value| value.strip_prefix(prefix)) {
            return Some(term.trim().to_string());
        }
    }

    if let Some(param) = normals.first() {
        return Some(param.trim().to_string());
    }

    // Fall back to a url's identifying segment.
    for param in &job.param {
        if let shared_types::ScraperParam::Url(url) = param {
            if let Some(term) = term_from_url(&url.url) {
                return Some(term);
            }
        }
    }

    None
}

/// Extracts the identifying segment from a Redgifs url.
fn term_from_url(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("//"))
        .unwrap_or(url);
    if !rest
        .split('/')
        .next()?
        .to_ascii_lowercase()
        .contains("redgifs.com")
    {
        return None;
    }
    let segments = rest
        .split(['/', '?', '#'])
        .filter(|segment| !segment.is_empty())
        .skip(1)
        .collect::<Vec<_>>();

    match segments.first().copied()? {
        "gifs" | "watch" | "ifr" | "users" | "niches" => Some(segments.get(1)?.to_string()),
        "search" => {
            // /search?query=anal -- only the value is interesting.
            let query = rest.split_once("query=")?.1;
            Some(query.split('&').next().unwrap_or_default().to_string())
        }
        _ => None,
    }
}

/// Fetches the page this job describes and turns it into files, tags and jobs.
#[unsafe(no_mangle)]
pub fn parser_call(
    text_input: &str,
    _source_url: &str,
    scraperdata: &ScraperDataReturn,
) -> Vec<ScraperReturn> {
    let _ = text_input;
    let job = &scraperdata.job;
    let user_data = &job.user_data;

    let Some(kind) = user_data
        .get(key::KIND)
        .and_then(|value| PageKind::parse(value))
    else {
        // No kind means `url_dump` never ran or could not resolve this job.
        return vec![ScraperReturn::Nothing];
    };

    let options = options_from_settings();
    let harvest = match kind {
        PageKind::Gif => fetch_gif(user_data, &options),
        PageKind::Gallery => fetch_gallery(user_data, &options),
        _ => fetch_listing(kind, user_data, &options),
    };

    match harvest {
        Ok(harvest) if harvest.is_empty() => vec![ScraperReturn::Nothing],
        Ok(harvest) => vec![ScraperReturn::Data(shared_types::ScraperObject {
            files: harvest.files,
            tags: harvest.tags,
            jobs: harvest.jobs,
        })],
        Err(reason) => {
            // A 404 is a fact about the site, not a fault: the user or gif
            // simply does not exist. Anything else is worth retrying.
            match reason {
                ApiFailure::Gone => {
                    log::info!(
                        "Redgifs: {} page does not exist; dropping job",
                        kind.as_str()
                    );
                    vec![ScraperReturn::Nothing]
                }
                ApiFailure::Other(message) => {
                    log::error!("Redgifs: {} page failed: {message}", kind.as_str());
                    vec![ScraperReturn::Stop(message)]
                }
            }
        }
    }
}

/// Why a fetch failed, split so 404 can be handled without an error log.
#[derive(Debug)]
enum ApiFailure {
    Gone,
    Other(String),
}

impl From<ApiError> for ApiFailure {
    fn from(error: ApiError) -> Self {
        match error {
            ApiError::NotFound => ApiFailure::Gone,
            other => ApiFailure::Other(other.to_string()),
        }
    }
}

/// Fetches `/v2/gifs/{id}`.
fn fetch_gif(
    user_data: &std::collections::BTreeMap<String, String>,
    options: &Options,
) -> Result<Harvest, ApiFailure> {
    let raw = user_data
        .get(key::GIF_ID)
        .ok_or_else(|| ApiFailure::Other("gif job has no id".to_string()))?;
    let id =
        GifId::parse(raw).ok_or_else(|| ApiFailure::Other(format!("invalid gif id {raw:?}")))?;

    let gif = api::gif(id.as_str())?;
    let mut harvest = Harvest::default();
    if let Some((file, tags)) = parse::gif_into(&gif, options) {
        harvest.files.insert(file);
        harvest.tags.extend(tags);
        harvest.jobs.extend(gallery_jobs(&[&gif], options));
    }
    Ok(harvest)
}

/// Fetches `/v2/gallery/{uuid}`.
fn fetch_gallery(
    user_data: &std::collections::BTreeMap<String, String>,
    options: &Options,
) -> Result<Harvest, ApiFailure> {
    let gallery = user_data
        .get(key::GALLERY)
        .ok_or_else(|| ApiFailure::Other("gallery job has no id".to_string()))?;

    let members = parse::bounded_members(api::gallery(gallery)?);
    let mut harvest = Harvest::default();

    for (position, member) in members.iter().enumerate() {
        if let Some((file, tags)) = parse::gallery_member_into(member, position, options) {
            harvest.files.insert(file);
            harvest.tags.extend(tags);
        }
    }
    Ok(harvest)
}

/// Fetches one page of a listing endpoint.
fn fetch_listing(
    kind: PageKind,
    user_data: &std::collections::BTreeMap<String, String>,
    options: &Options,
) -> Result<Harvest, ApiFailure> {
    let page_number = user_data
        .get(key::PAGE)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(request::FIRST_PAGE);

    let (path, query) = listing_request(kind, user_data)?;

    let Page {
        json, pages, page, ..
    } = api::page(&path, &query, page_number)?;

    let gifs = json["gifs"].members().collect::<Vec<_>>();
    let mut harvest = Harvest::default();

    for gif in &gifs {
        if let Some((file, tags)) = parse::gif_into(gif, options) {
            harvest.files.insert(file);
            harvest.tags.extend(tags);
        }
    }

    harvest.jobs.extend(gallery_jobs(&gifs, options));

    if let Some(next) = request::continuation(kind, user_data, page + 1, pages) {
        harvest.jobs.insert(next);
    } else if let Some(pages) = pages
        && page >= pages
        && page < limits::MAX_PAGES_PER_CHAIN
    {
        log::info!(
            "Redgifs: stopping {kind:?} crawl after {page} of {pages} reported pages; \
             raise PLUGIN_Redgifs_MaxPages to go deeper"
        );
    }

    Ok(harvest)
}

/// Builds the endpoint path and query for a listing kind.
fn listing_request(
    kind: PageKind,
    user_data: &std::collections::BTreeMap<String, String>,
) -> Result<(String, Vec<(&'static str, String)>), ApiFailure> {
    let search = user_data.get(key::SEARCH).cloned();
    let order = user_data
        .get(key::ORDER)
        .filter(|value| request::ORDERS.contains(&value.as_str()))
        .cloned()
        .unwrap_or_else(|| "new".to_string());

    let mut query: Vec<(&'static str, String)> = Vec::new();

    let (path, query) = match kind {
        PageKind::User => {
            let name = user_data
                .get(key::USER)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| ApiFailure::Other("user job has no username".to_string()))?;
            query.push(("order", order));
            (format!("/v2/users/{}/search", name.to_lowercase()), query)
        }
        PageKind::Niche => {
            let name = user_data
                .get(key::SEARCH)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| ApiFailure::Other("niche job has no name".to_string()))?;
            query.push(("order", order));
            (format!("/v2/niches/{}/gifs", name.to_lowercase()), query)
        }
        PageKind::Listing => {
            // The tag search endpoint. `/v2/search/gifs` would be a better
            // match but needs a logged-in user token, which the temporary one
            // is not.
            let term = search
                .filter(|term| !term.is_empty())
                .ok_or_else(|| ApiFailure::Other("listing job has no search term".to_string()))?;
            query.push(("search_text", term));
            ("/v2/gifs/search".to_string(), query)
        }
        PageKind::Gif | PageKind::Gallery => {
            return Err(ApiFailure::Other(format!(
                "{kind:?} is not a listing endpoint"
            )));
        }
    };

    Ok((path, query))
}

/// Reads the plugin's settings.
fn options_from_settings() -> Options {
    Options {
        prefer_silent: flag("PLUGIN_Redgifs_PreferSilent", false),
        allow_still: flag("PLUGIN_Redgifs_AllowPoster", false),
        refresh_galleries: flag("PLUGIN_Redgifs_RefreshGalleries", false),
    }
}

fn flag(name: &str, default: bool) -> bool {
    match client::setting_get(name.to_string()) {
        Ok(Some(setting)) => setting
            .param
            .map(|value| !matches!(value.to_ascii_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(default),
        _ => default,
    }
}

/// Pulls the `token` field out of an auth response body.
pub(crate) fn json_token_field(body: &str) -> Option<String> {
    json::parse(body).ok()?["token"]
        .as_str()
        .map(str::to_string)
}

/// Shortens an error body for a log line without dumping a token.
pub(crate) fn summarize(body: &str) -> String {
    let mut out = body.trim().chars().take(200).collect::<String>();
    if out.len() < body.trim().len() {
        out.push('…');
    }
    out
}

/// Replaces the middle of a credential-looking string with `***`.
pub(crate) fn redact_token_hint(body: &str) -> String {
    summarize(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(params: Vec<shared_types::ScraperParam>) -> ScraperDataReturn {
        ScraperDataReturn {
            job: shared_types::PluginJob {
                site: request::SITE.to_string(),
                param: params,
                ..Default::default()
            },
            skip_conditions: Vec::new(),
        }
    }

    fn normal(value: &str) -> shared_types::ScraperParam {
        shared_types::ScraperParam::Normal(value.to_string())
    }

    fn user_data_of(data: &ScraperDataReturn) -> std::collections::BTreeMap<String, String> {
        data.job.user_data.clone()
    }

    #[test]
    fn plugin_registers_site_and_user_agent() {
        let plugins = get_plugin_info();
        let plugin = &plugins[0];
        assert_eq!(plugin.name, "Redgifs");

        let sites = plugin
            .properties
            .iter()
            .find_map(|property| match property {
                PluginProperties::Sites(sites) => Some(sites.clone()),
                _ => None,
            });
        assert!(sites.is_some_and(|sites| sites.contains(&"redgifs.com".to_string())));

        assert!(plugin.properties.iter().any(|property| matches!(
            property,
            PluginProperties::Modifier(shared_types::TargetModifier {
                modifier: shared_types::DownloadModifiers::Useragent(_),
                ..
            })
        )));
    }

    #[test]
    fn url_dump_seeds_the_first_listing_page() {
        let out = url_dump(&job(vec![normal("anal")]));
        assert_eq!(out.len(), 1);
        let data = user_data_of(&out[0]);
        assert_eq!(data.get(key::KIND).map(String::as_str), Some("listing"));
        assert_eq!(data.get(key::SEARCH).map(String::as_str), Some("anal"));
        assert_eq!(data.get(key::PAGE).map(String::as_str), Some("1"));
        assert_eq!(out[0].job.site, request::SITE);
    }

    #[test]
    fn url_dump_strips_prefixes_into_the_right_slot() {
        let data = user_data_of(&url_dump(&job(vec![normal("user:somebody")]))[0]);
        assert_eq!(data.get(key::KIND).map(String::as_str), Some("user"));
        assert_eq!(data.get(key::USER).map(String::as_str), Some("somebody"));

        let data = user_data_of(&url_dump(&job(vec![normal("niche:solo")]))[0]);
        assert_eq!(data.get(key::KIND).map(String::as_str), Some("niche"));
        assert_eq!(data.get(key::SEARCH).map(String::as_str), Some("solo"));

        let data = user_data_of(&url_dump(&job(vec![normal("gif:Abc123")]))[0]);
        assert_eq!(data.get(key::KIND).map(String::as_str), Some("gif"));
        assert_eq!(data.get(key::GIF_ID).map(String::as_str), Some("Abc123"));
    }

    #[test]
    fn url_dump_reads_a_site_url() {
        let out = url_dump(&job(vec![shared_types::ScraperParam::Url(
            shared_types::Url {
                url: "https://www.redgifs.com/watch/ShrillCourageousComet".to_string(),
                ..Default::default()
            },
        )]));
        let data = user_data_of(&out[0]);
        assert_eq!(data.get(key::KIND).map(String::as_str), Some("gif"));
        assert_eq!(
            data.get(key::GIF_ID).map(String::as_str),
            Some("ShrillCourageousComet")
        );
    }

    #[test]
    fn url_dump_passes_a_continuation_through_unchanged() {
        let mut user_data = std::collections::BTreeMap::new();
        user_data.insert(key::KIND.to_string(), "listing".to_string());
        user_data.insert(key::SEARCH.to_string(), "anal".to_string());
        user_data.insert(key::PAGE.to_string(), "4".to_string());

        let out = url_dump(&ScraperDataReturn {
            job: shared_types::PluginJob {
                site: request::SITE.to_string(),
                user_data,
                ..Default::default()
            },
            skip_conditions: Vec::new(),
        });

        assert_eq!(out.len(), 1);
        let data = user_data_of(&out[0]);
        assert_eq!(data.get(key::PAGE).map(String::as_str), Some("4"));
        assert_eq!(data.get(key::SEARCH).map(String::as_str), Some("anal"));
    }

    #[test]
    fn url_dump_produces_nothing_for_an_unusable_job() {
        assert!(url_dump(&job(Vec::new())).is_empty());
        assert!(url_dump(&job(vec![normal("gif:not a valid id")])).is_empty());
    }

    #[test]
    fn listing_request_builds_the_documented_endpoints() {
        let mut user_data = std::collections::BTreeMap::new();
        user_data.insert(key::SEARCH.to_string(), "Anal Creampie".to_string());
        let (path, query) = listing_request(PageKind::Listing, &user_data).unwrap();
        assert_eq!(path, "/v2/gifs/search");
        assert!(
            query
                .iter()
                .any(|(key, value)| *key == "search_text" && value == "Anal Creampie")
        );

        user_data.clear();
        user_data.insert(key::USER.to_string(), "Somebody".to_string());
        let (path, _) = listing_request(PageKind::User, &user_data).unwrap();
        assert_eq!(path, "/v2/users/somebody/search");

        user_data.clear();
        user_data.insert(key::SEARCH.to_string(), "Solo".to_string());
        let (path, _) = listing_request(PageKind::Niche, &user_data).unwrap();
        assert_eq!(path, "/v2/niches/solo/gifs");
    }

    #[test]
    fn listing_request_rejects_missing_terms() {
        let empty = std::collections::BTreeMap::new();
        assert!(listing_request(PageKind::Listing, &empty).is_err());
        assert!(listing_request(PageKind::User, &empty).is_err());
        assert!(listing_request(PageKind::Niche, &empty).is_err());
        // Non-listing kinds are not listing endpoints.
        assert!(listing_request(PageKind::Gif, &empty).is_err());
    }

    #[test]
    fn non_listing_kinds_are_not_listing_endpoints() {
        let mut user_data = std::collections::BTreeMap::new();
        user_data.insert(key::GIF_ID.to_string(), "abc".to_string());
        assert!(listing_request(PageKind::Gallery, &user_data).is_err());
    }

    #[test]
    fn parser_call_returns_nothing_without_a_kind() {
        let out = parser_call("", "", &job(Vec::new()));
        assert!(matches!(out[0], shared_types::ScraperReturn::Nothing));
    }

    #[test]
    fn json_token_field_extracts_the_bearer() {
        assert_eq!(
            json_token_field(r#"{"token":"abc.def.ghi"}"#).as_deref(),
            Some("abc.def.ghi")
        );
        assert!(json_token_field("not json").is_none());
        assert!(json_token_field(r#"{"addr":"x"}"#).is_none());
    }

    #[test]
    fn summaries_are_short_and_single_line() {
        let long = "x".repeat(5000);
        let summary = summarize(&long);
        assert!(summary.chars().count() <= 201);
        assert!(summary.ends_with('…'));
        assert_eq!(summarize("  hi  "), "hi");
    }

    #[test]
    fn term_from_url_handles_shapes_and_rejects_foreign() {
        assert_eq!(
            term_from_url("https://www.redgifs.com/watch/Abc123").as_deref(),
            Some("Abc123")
        );
        assert_eq!(
            term_from_url("https://redgifs.com/users/somebody").as_deref(),
            Some("somebody")
        );
        assert_eq!(
            term_from_url("https://redgifs.com/search?query=anal&x=1").as_deref(),
            Some("anal")
        );
        assert!(term_from_url("https://example.com/watch/Abc123").is_none());
        assert!(term_from_url("https://redgifs.com/").is_none());
    }
}
