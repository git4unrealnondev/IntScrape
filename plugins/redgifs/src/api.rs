//! Bounded API client.
//!
//! Two things make this deliberately conservative:
//!
//! * **Pagination is capped.** Redgifs reports `pages` up to 5000 and `total` up
//!   to 10000 for a single tag. Following that to the end from one job would
//!   enqueue an unbounded crawl, so every list request is bounded by
//!   `MAX_PAGES_PER_JOB` and the caller decides whether to enqueue a
//!   continuation.
//! * **One entry is parsed per call.** `parser_call` receives one page of text
//!   at a time; this module never accumulates pages, so its memory is a single
//!   page regardless of how deep the crawl goes.

use crate::auth::{API_ROOT, SITE_ROOT, bearer_token, invalidate};
use crate::limits::{MAX_PAGE_COUNT, REQUEST_TIMEOUT};

/// A Redgifs API failure, split so the caller can react to 401 specially.
#[derive(Debug)]
pub enum ApiError {
    /// Token rejected. The cache has already been cleared.
    Unauthorized,
    /// 404: gif, gallery, user or niche does not exist.
    NotFound,
    /// Any other non-success status.
    Status(reqwest::StatusCode, String),
    /// Transport failure or a body that could not be read.
    Transport(String),
    /// 2xx with a body that was not the json we expected.
    Malformed(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Unauthorized => write!(f, "redgifs rejected the token (401)"),
            ApiError::NotFound => write!(f, "redgifs has no such entry (404)"),
            ApiError::Status(status, body) => {
                write!(f, "redgifs returned {status}: {body}")
            }
            ApiError::Transport(message) => write!(f, "redgifs request failed: {message}"),
            ApiError::Malformed(message) => {
                write!(f, "redgifs returned an unusable body: {message}")
            }
        }
    }
}

/// A page of results plus the pagination state needed to continue.
pub struct Page {
    pub json: json::JsonValue,
    /// Total page count Redgifs reported, if it did. `None` means the field was
    /// absent, which the caller's chain budget covers.
    pub pages: Option<u64>,
    /// The page just returned.
    pub page: u64,
}

/// Performs one authenticated GET and returns the parsed body.
///
/// On 401 the token cache is cleared and the request is retried exactly once
/// with a fresh token, which covers a token Redgifs invalidated before its TTL.
pub fn get(path: &str, query: &[(&str, String)]) -> Result<json::JsonValue, ApiError> {
    get_inner(path, query, true)
}

fn get_inner(
    path: &str,
    query: &[(&str, String)],
    allow_retry: bool,
) -> Result<json::JsonValue, ApiError> {
    let token = bearer_token(false).map_err(ApiError::Transport)?;

    let mut url = format!("{API_ROOT}{path}");
    if !query.is_empty() {
        let mut separator = '?';
        for (key, value) in query {
            url.push(separator);
            url.push_str(key);
            url.push('=');
            url.push_str(&percent_encode(value));
            separator = '&';
        }
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .connect_timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|error| ApiError::Transport(error.to_string()))?;

    let response = client
        .get(&url)
        .header("Accept", "application/json, text/plain, */*")
        .header("Referer", format!("{SITE_ROOT}/"))
        .header("Origin", SITE_ROOT)
        .header("User-Agent", crate::request::USER_AGENT)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .map_err(|error| ApiError::Transport(error.to_string()))?;

    let status = response.status();
    let body = response
        .text()
        .map_err(|error| ApiError::Transport(error.to_string()))?;

    if status == reqwest::StatusCode::UNAUTHORIZED {
        invalidate();
        if allow_retry {
            return get_inner(path, query, false);
        }
        return Err(ApiError::Unauthorized);
    }

    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(ApiError::NotFound);
    }

    if !status.is_success() {
        return Err(ApiError::Status(status, crate::summarize(&body)));
    }

    json::parse(&body).map_err(|error| ApiError::Malformed(error.to_string()))
}

/// Fetches one page of a list endpoint.
///
/// `page` is 1-based and `count` is capped at the API's own maximum of 100.
pub fn page(path: &str, query: &[(&str, String)], page_number: u64) -> Result<Page, ApiError> {
    let count = MAX_PAGE_COUNT.min(100);
    let mut params: Vec<(&str, String)> = query
        .iter()
        .map(|(key, value)| (*key, value.clone()))
        .collect();
    params.push(("page", page_number.to_string()));
    params.push(("count", count.to_string()));

    let json = get(path, &params)?;

    let pages = json["pages"].as_u64();
    Ok(Page {
        json,
        pages,
        page: page_number,
    })
}

/// Fetches a single gif by id. The API lowercases ids.
pub fn gif(id: &str) -> Result<json::JsonValue, ApiError> {
    let json = get(&format!("/v2/gifs/{}", id.to_lowercase()), &[])?;
    let gif = &json["gif"];
    if gif.is_null() {
        return Err(ApiError::Malformed(
            "response had no `gif` object".to_string(),
        ));
    }
    Ok(gif.clone())
}

/// Fetches the full member list of a gallery.
///
/// Galleries are small (the largest observed was 4 entries), so this is a
/// single unpaginated call. The result is still capped defensively by the
/// caller in case Redgifs returns an unexpectedly large one.
pub fn gallery(gallery_id: &str) -> Result<Vec<json::JsonValue>, ApiError> {
    let json = get(&format!("/v2/gallery/{gallery_id}"), &[])?;
    Ok(json["gifs"].members().cloned().collect())
}

/// Minimal percent-encoding for query values.
///
/// Only the characters that would actually break a Redgifs query are escaped.
/// Tag and niche names are title-cased words and ids are `[A-Za-z0-9]`, so this
/// is defensive rather than load-bearing.
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            b' ' => out.push_str("%20"),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}
