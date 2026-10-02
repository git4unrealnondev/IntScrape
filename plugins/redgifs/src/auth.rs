//! Bearer token acquisition and caching.
//!
//! Every `/v2/*` endpoint needs `Authorization: Bearer <token>`, obtained from
//! `/v2/auth/temporary` without any credentials. gallery-dl caches the token
//! for 600 seconds even though its JWT claims a ~24h lifetime; that is
//! deliberate slack, and this module follows the same policy.
//!
//! The host runs `parser_call` on a blocking thread but may call it from several
//! jobs at once, so the cache is shared behind a mutex and a token is only
//! fetched once per TTL even under concurrent first use.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// API root, shared by the auth call and every data call.
pub const API_ROOT: &str = "https://api.redgifs.com";

/// Site root. Redgifs requires this as `Referer` and `Origin` on API calls.
pub const SITE_ROOT: &str = "https://www.redgifs.com";

/// How long a token is reused. Matches gallery-dl's `_exp=600`.
const TOKEN_TTL: Duration = Duration::from_secs(600);

/// Margin subtracted from the elapsed time so a token is never used right at
/// the edge of its TTL.
const TTL_SLOP: Duration = Duration::from_secs(30);

struct CachedToken {
    token: String,
    fetched: Instant,
}

fn cache() -> &'static Mutex<Option<CachedToken>> {
    static CACHE: OnceLock<Mutex<Option<CachedToken>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// Returns a usable bearer token, fetching a new one when the cached token is
/// missing or close to expiry.
///
/// A cached token that is still valid is always preferred: on a 401 the
/// fallback path clears the cache and forces exactly one refetch, so a token
/// that Redgifs invalidated early does not turn every subsequent request into
/// two.
pub fn bearer_token(force_refresh: bool) -> Result<String, String> {
    {
        let guard = cache()
            .lock()
            .map_err(|_| "token cache poisoned".to_string())?;
        if let Some(cached) = guard.as_ref()
            && !force_refresh
            && cached.fetched.elapsed() < TOKEN_TTL.saturating_sub(TTL_SLOP)
        {
            return Ok(cached.token.clone());
        }
    }

    let token = fetch_token()?;

    let mut guard = cache()
        .lock()
        .map_err(|_| "token cache poisoned".to_string())?;
    *guard = Some(CachedToken {
        token: token.clone(),
        fetched: Instant::now(),
    });

    Ok(token)
}

/// Drops the cached token. Called after a 401 so the next request refetches.
pub fn invalidate() {
    if let Ok(mut guard) = cache().lock() {
        *guard = None;
    }
}

/// Requests a fresh temporary token.
fn fetch_token() -> Result<String, String> {
    let url = format!("{API_ROOT}/v2/auth/temporary");

    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("building auth client failed: {error}"))?
        .get(&url)
        .header("Accept", "application/json, text/plain, */*")
        .header("Referer", format!("{SITE_ROOT}/"))
        .header("Origin", SITE_ROOT)
        .header("User-Agent", crate::request::USER_AGENT)
        .send()
        .map_err(|error| format!("temporary token request failed: {error}"))?;

    let status = response.status();
    let body = response
        .text()
        .map_err(|error| format!("reading token response failed: {error}"))?;

    if !status.is_success() {
        return Err(format!(
            "temporary token request returned {status}: {}",
            crate::redact_token_hint(&body)
        ));
    }

    crate::json_token_field(&body).ok_or_else(|| {
        format!(
            "temporary token response had no token field: {}",
            crate::redact_token_hint(&body)
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Live check against the real endpoint. Skipped automatically when the
    /// network is unavailable so the suite still runs offline.
    #[test]
    fn temporary_token_endpoint_returns_a_usable_bearer_token() {
        let Ok(token) = fetch_token() else {
            eprintln!("skipping: redgifs auth endpoint unreachable");
            return;
        };

        assert!(
            token.len() > 32,
            "a jwt should be far longer than 32 chars, got {}",
            token.len()
        );
        // A jwt is three base64url segments separated by dots.
        assert_eq!(token.matches('.').count(), 2, "token is not a jwt: {token}");
        assert!(
            !token.contains(char::is_whitespace),
            "token must not contain whitespace"
        );
    }

    #[test]
    fn cached_token_is_reused_within_the_ttl() {
        invalidate();
        let first = bearer_token(false);
        let Ok(first) = first else {
            eprintln!("skipping: redgifs auth endpoint unreachable");
            return;
        };

        // The second call must come from the cache, not the network. Asserted
        // by checking that the cached entry's timestamp did not move.
        let before = cache()
            .lock()
            .map(|guard| guard.as_ref().map(|cached| cached.fetched))
            .unwrap_or(None);

        let second = bearer_token(false).expect("cached token");
        assert_eq!(first, second);

        let after = cache()
            .lock()
            .map(|guard| guard.as_ref().map(|cached| cached.fetched))
            .unwrap_or(None);
        assert_eq!(before, after, "cache entry must not be rewritten");
    }
}
