//! Proxy registry and ranking.
//!
//! Other plugins reach this plugin through `client::external_plugin_call`, which
//! the host dispatches with `tokio::task::spawn_blocking` (see `src/ipc.rs`), so
//! every callback here is synchronous and uses `reqwest::blocking` rather than
//! nesting a runtime inside one.
//!
//! Three callbacks are exported:
//!
//! * `proxy_get_proxy`  - hand back a proxy that is currently working.
//! * `proxy_request`    - make a request through a proxy and record the result.
//! * `proxy_report`     - record a result the caller observed itself.
//!
//! Scoring is binary, as requested: a check either produced an HTTP response
//! (`rating = 1`) or it did not (`rating = 0`). Ranking among working proxies uses
//! the success rate in `rating_history` so a proxy that worked nine times out of
//! ten outranks one that got lucky once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use json::JsonValue;
use shared_types::*;

/// Newest-last cap on `rating_history`, so the setting stays small and the
/// success rate reflects recent behaviour rather than all-time behaviour.
const HISTORY_MAX: usize = 32;

/// Default per-request timeout when the caller does not supply one.
const DEFAULT_TIMEOUT_MS: u64 = 10_000;

/// Default re-probe window when the caller does not supply one.
const DEFAULT_RETRY_AFTER_SECS: u64 = 3_600;

const SETTING_LIST: &str = "PLUGIN_proxy_list";
const SETTING_SITES: &str = "PLUGIN_proxy_sites";

#[derive(Debug, Clone)]
struct Proxy {
    name: String,
    proxy_type: String,
    proxy_url: String,
    /// Binary outcome of the most recent check: 1 = a response came back.
    rating: u32,
    /// Newest last, capped at [`HISTORY_MAX`].
    rating_history: Vec<u32>,
}

/// Per-site re-probe scheduling, so a site can be re-tested on its own cadence.
#[derive(Debug, Clone)]
struct SiteState {
    site: String,
    /// Re-probe at most this often. `3600` means "come back in an hour".
    retry_after_secs: u64,
    /// Unix seconds of the last probe.
    last_probe: u64,
}

struct Registry {
    proxies: Vec<Proxy>,
    sites: Vec<SiteState>,
    /// Round-robin cursor, so proxies that have never been tried still get tried
    /// once everything else looks healthy.
    cursor: usize,
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
static HYDRATED: AtomicBool = AtomicBool::new(false);

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| {
        Mutex::new(Registry {
            proxies: Vec::new(),
            sites: Vec::new(),
            cursor: 0,
        })
    })
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// settings persistence
// ---------------------------------------------------------------------------

fn setting_get_raw(name: &str) -> Option<String> {
    match client::setting_get(name.to_string()) {
        Ok(Some(setting)) => setting.param,
        _ => None,
    }
}

fn setting_set_raw(name: &str, description: &str, value: &str) -> bool {
    client::setting_set(DbSettingsObj {
        name: name.to_string(),
        description: Some(description.to_string()),
        num: None,
        param: Some(value.to_string()),
    })
    .unwrap_or(false)
}

fn proxy_to_json(proxy: &Proxy) -> JsonValue {
    json::object! {
        name: proxy.name.clone(),
        proxy_type: proxy.proxy_type.clone(),
        proxy_url: proxy.proxy_url.clone(),
        rating: proxy.rating,
        rating_history: proxy.rating_history.clone(),
    }
}

fn json_to_proxy_list(json: json::JsonValue) -> Vec<Proxy> {
    let mut out = Vec::new();
    for member in json.members() {
        let (Some(name), Some(proxy_type), Some(proxy_url), Some(rating)) = (
            member["name"].as_str(),
            member["proxy_type"].as_str(),
            member["proxy_url"].as_str(),
            member["rating"].as_u32(),
        ) else {
            continue;
        };

        let mut history_vec = Vec::new();
        if member["rating_history"].is_array() {
            for history in member["rating_history"].members() {
                if let Some(item) = history.as_u32() {
                    history_vec.push(item);
                }
            }
        }

        out.push(Proxy {
            name: name.to_string(),
            proxy_type: proxy_type.to_string(),
            proxy_url: proxy_url.to_string(),
            rating,
            rating_history: history_vec,
        });
    }
    out
}

fn site_to_json(site: &SiteState) -> JsonValue {
    json::object! {
        site: site.site.clone(),
        retry_after_secs: site.retry_after_secs,
        last_probe: site.last_probe,
    }
}

fn json_to_sites(json: json::JsonValue) -> Vec<SiteState> {
    let mut out = Vec::new();
    for member in json.members() {
        let Some(site) = member["site"].as_str() else {
            continue;
        };
        out.push(SiteState {
            site: site.to_string(),
            retry_after_secs: member["retry_after_secs"].as_u64().unwrap_or(0),
            last_probe: member["last_probe"].as_u64().unwrap_or(0),
        });
    }
    out
}

fn hydrate(reg: &mut Registry) {
    if HYDRATED.swap(true, Ordering::SeqCst) {
        return;
    }
    reg.proxies = setting_get_raw(SETTING_LIST)
        .and_then(|raw| json::parse(&raw).ok())
        .map(json_to_proxy_list)
        .unwrap_or_default();
    reg.sites = setting_get_raw(SETTING_SITES)
        .and_then(|raw| json::parse(&raw).ok())
        .map(json_to_sites)
        .unwrap_or_default();
}

fn persist(reg: &Registry) {
    let list: Vec<JsonValue> = reg.proxies.iter().map(proxy_to_json).collect();
    setting_set_raw(
        SETTING_LIST,
        "A list of proxies that can be used by the proxy plugin",
        &json::stringify(list),
    );

    let sites: Vec<JsonValue> = reg.sites.iter().map(site_to_json).collect();
    setting_set_raw(
        SETTING_SITES,
        "Per-site proxy re-probe schedule (retry_after_secs, last_probe)",
        &json::stringify(sites),
    );
}

/// Runs `f` against the shared registry, hydrating from settings on first use.
///
/// `save` controls whether the result is written back. A poisoned lock is
/// recovered rather than propagated: the plugin is built with `panic = "abort"`
/// and is called across an `extern "C"` boundary, so a panic here would take the
/// whole host process down.
fn with_registry<R>(save: bool, f: impl FnOnce(&mut Registry) -> R) -> R {
    let mut guard = match registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    hydrate(&mut guard);
    let out = f(&mut guard);
    if save {
        persist(&guard);
    }
    out
}

// ---------------------------------------------------------------------------
// ranking
// ---------------------------------------------------------------------------

/// Fraction of recent checks that succeeded, in `0.0..=1.0`.
///
/// A proxy with no history scores `0.0` so a freshly loaded list ranks by the
/// explicit `rating` flag instead.
fn success_rate(proxy: &Proxy) -> f64 {
    if proxy.rating_history.is_empty() {
        return 0.0;
    }
    let good = proxy.rating_history.iter().filter(|v| **v == 1).count();
    good as f64 / proxy.rating_history.len() as f64
}

/// Whether this site is due for another round of probes.
///
/// An empty site has no schedule, so it is always due. Otherwise the site is
/// due once its window has elapsed since the last probe.
fn site_due_for_probe(reg: &Registry, site: &str, now: u64) -> bool {
    if site.is_empty() {
        return true;
    }
    match reg.sites.iter().find(|s| s.site == site) {
        None => true,
        Some(state) => now.saturating_sub(state.last_probe) >= state.retry_after_secs,
    }
}

/// Stamps a probe against a site and pins the window it should repeat on.
///
/// A `retry_after_secs` of `0` means "use the default" rather than "probe
/// constantly", so the stored window is never zero.
fn note_probe(reg: &mut Registry, site: &str, retry_after_secs: u64, now: u64) {
    if site.is_empty() {
        return;
    }
    let window = if retry_after_secs == 0 {
        DEFAULT_RETRY_AFTER_SECS
    } else {
        retry_after_secs
    };
    match reg.sites.iter_mut().find(|s| s.site == site) {
        Some(state) => {
            state.retry_after_secs = window;
            state.last_probe = now;
        }
        None => reg.sites.push(SiteState {
            site: site.to_string(),
            retry_after_secs: window,
            last_probe: now,
        }),
    }
}

/// Highest success rate among the given indices, ties going to the lower index.
fn best_usable(reg: &Registry, usable: &[usize]) -> Option<usize> {
    usable.iter().copied().max_by(|a, b| {
        success_rate(&reg.proxies[*a])
            .partial_cmp(&success_rate(&reg.proxies[*b]))
            .unwrap_or(std::cmp::Ordering::Equal)
    })
}

/// Entries that have never been probed, so nothing is known about them yet.
///
/// This is what lets a freshly added proxy bootstrap. Nothing can hold a
/// positive rating before something has measured it, so a list containing only
/// new entries has no usable entries and would otherwise sit idle until an
/// explicit "test every proxy" round.
fn untested(reg: &Registry) -> Vec<usize> {
    (0..reg.proxies.len())
        .filter(|i| {
            let proxy = &reg.proxies[*i];
            proxy.rating_history.is_empty() && reqwest_proxy(proxy).is_ok()
        })
        .collect()
}

/// Chooses which proxy to use for the next request.
///
/// `allow_trial` is set by callers that are about to make a request and can
/// therefore afford to spend it on a proxy that has not been measured lately.
///
/// * Inside the site's retry window the best known-working proxy is reused, so
///   the window really does hold off re-testing the rest of the list.
/// * Once the window has elapsed the list is walked round-robin instead, which
///   is what refreshes the ranking and is the whole point of the timeout.
/// * With nothing known to work, a caller that allows trials still gets an
///   entry, otherwise it gets nothing rather than a proxy already marked bad.
fn pick_proxy(reg: &mut Registry, site: &str, now: u64, allow_trial: bool) -> Option<usize> {
    if reg.proxies.is_empty() {
        return None;
    }

    let usable: Vec<usize> = (0..reg.proxies.len())
        .filter(|i| {
            let p = &reg.proxies[*i];
            reqwest_proxy(p).is_ok() && p.rating == 1
        })
        .collect();

    if !site_due_for_probe(reg, site, now)
        && let Some(best) = best_usable(reg, &usable)
    {
        return Some(best);
    }

    if allow_trial {
        let start = reg.cursor % reg.proxies.len();
        for offset in 0..reg.proxies.len() {
            let idx = (start + offset) % reg.proxies.len();
            if reqwest_proxy(&reg.proxies[idx]).is_ok() {
                reg.cursor = idx + 1;
                return Some(idx);
            }
        }
    }

    if let Some(best) = best_usable(reg, &usable) {
        return Some(best);
    }

    // Nothing is known to work yet, so offer an entry that has never been
    // measured. A proxy already marked bad is deliberately not offered here:
    // the caller would be repeating a known failure, and its report would just
    // append another zero to that proxy's history.
    untested(reg).first().copied()
}

/// Records a binary outcome and returns the new rating.
fn record(reg: &mut Registry, idx: usize, ok: bool) -> u32 {
    let score = u32::from(ok);
    let proxy = &mut reg.proxies[idx];
    proxy.rating = score;
    proxy.rating_history.push(score);
    if proxy.rating_history.len() > HISTORY_MAX {
        let excess = proxy.rating_history.len() - HISTORY_MAX;
        proxy.rating_history.drain(0..excess);
    }
    score
}

/// Builds a reqwest proxy for a registry entry.
///
/// reqwest has no `Proxy::socks5` constructor; it picks the protocol from the
/// URL scheme, so the only work here is making sure `proxy_url` carries one.
/// `proxy_type` supplies the scheme when the stored URL is a bare `host:port`,
/// which is the common shape for scraped proxy lists.
fn reqwest_proxy(proxy: &Proxy) -> Result<reqwest::Proxy, String> {
    let url = proxy.proxy_url.trim();
    if url.is_empty() {
        return Err("empty proxy_url".to_string());
    }

    let url = if url.contains("://") {
        url.to_string()
    } else {
        let scheme = match proxy.proxy_type.to_ascii_lowercase().as_str() {
            "http" | "https" | "socks4" | "socks4a" | "socks5" | "socks5h" => {
                proxy.proxy_type.to_ascii_lowercase()
            }
            "" => "http".to_string(),
            other => return Err(format!("unsupported proxy_type `{other}`")),
        };
        format!("{scheme}://{url}")
    };

    reqwest::Proxy::all(&url).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// callback input helpers
// ---------------------------------------------------------------------------

fn input_string(callback: &CallbackInfoInput, name: &str) -> String {
    callback
        .data_name
        .iter()
        .position(|key| key == name)
        .and_then(|index| callback.data.get(index))
        .and_then(|value| match value {
            CallbackCustomDataReturning::String(text) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn input_u64(callback: &CallbackInfoInput, name: &str, default: u64) -> u64 {
    callback
        .data_name
        .iter()
        .position(|key| key == name)
        .and_then(|index| callback.data.get(index))
        .and_then(|value| match value {
            CallbackCustomDataReturning::U64(number) => Some(*number),
            _ => None,
        })
        .unwrap_or(default)
}

fn out_string(map: &mut HashMap<String, CallbackCustomDataReturning>, key: &str, value: &str) {
    map.insert(
        key.to_string(),
        CallbackCustomDataReturning::String(value.to_string()),
    );
}

fn out_u64(map: &mut HashMap<String, CallbackCustomDataReturning>, key: &str, value: u64) {
    map.insert(key.to_string(), CallbackCustomDataReturning::U64(value));
}

/// Serialises the whole registry, which is what the web UI renders.
///
/// This reads the in-memory registry rather than the settings on purpose: the
/// registry is the source of truth while the process runs, and every mutation
/// writes through to the settings, so the two only diverge if something outside
/// the plugin edited a setting directly.
fn state_json(reg: &Registry) -> String {
    let proxies: Vec<JsonValue> = reg.proxies.iter().map(proxy_to_json).collect();
    let sites: Vec<JsonValue> = reg.sites.iter().map(site_to_json).collect();
    json::stringify(json::object! { proxies: proxies, sites: sites })
}

// ---------------------------------------------------------------------------
// exported callbacks
// ---------------------------------------------------------------------------

/// Returns a proxy that is currently working, without making a request.
///
/// The caller must pass `data_name: ["site"]` with a `String` value.
#[unsafe(no_mangle)]
fn proxy_get_proxy(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let site = input_string(callback, "site");
    let mut map = HashMap::new();

    let picked = with_registry(false, |reg| {
        let now = now_secs();
        pick_proxy(reg, &site, now, false)
    });

    match picked {
        Some(idx) => {
            let (url, kind, rating) = with_registry(false, |reg| {
                let p = &reg.proxies[idx];
                (p.proxy_url.clone(), p.proxy_type.clone(), p.rating as u64)
            });
            out_string(&mut map, "proxy_url", &url);
            out_string(&mut map, "proxy_type", &kind);
            out_u64(&mut map, "rating", rating);
        }
        None => {
            out_string(&mut map, "proxy_url", "");
            out_string(&mut map, "proxy_type", "");
            out_u64(&mut map, "rating", 0);
        }
    }

    map
}

/// Returns the whole registry plus the proxy that would be handed out for a site.
///
/// The caller must pass `data_name: ["site"]` with a `String` value. `state` is
/// a JSON object shaped `{ "proxies": [...], "sites": [...] }`.
#[unsafe(no_mangle)]
fn proxy_get_state(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let site = input_string(callback, "site");
    let mut map = HashMap::new();

    let (state, best) = with_registry(false, |reg| {
        let now = now_secs();
        let best = match pick_proxy(reg, &site, now, false) {
            Some(idx) => reg.proxies[idx].proxy_url.clone(),
            None => String::new(),
        };
        (state_json(reg), best)
    });

    out_string(&mut map, "state", &state);
    out_string(&mut map, "best_proxy", &best);
    map
}

/// Replaces the proxy list.
///
/// The caller must pass `data_name: ["list", "preserve_ratings"]` with
/// `String, U64`. `list` is a JSON array of
/// `{name, proxy_type, proxy_url, rating, rating_history}`.
///
/// With `preserve_ratings` set, an incoming entry whose `proxy_url` matches one
/// already in the registry keeps the stored rating and history, so renaming an
/// entry in the UI does not throw away what was measured. Entries that no longer
/// match anything are taken as given.
///
/// Entries that fail to parse are dropped rather than rejecting the whole list,
/// so one bad row cannot make the list uneditable.
#[unsafe(no_mangle)]
fn proxy_set_list(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let raw = input_string(callback, "list");
    let preserve = input_u64(callback, "preserve_ratings", 1) == 1;

    let mut map = HashMap::new();

    let mut incoming = json::parse(raw.trim())
        .map(json_to_proxy_list)
        .unwrap_or_default();
    let state = with_registry(true, |reg| {
        if preserve {
            for proxy in incoming.iter_mut() {
                if let Some(existing) = reg
                    .proxies
                    .iter()
                    .find(|p| same_endpoint(&p.proxy_url, &proxy.proxy_url))
                {
                    proxy.rating = existing.rating;
                    proxy.rating_history = existing.rating_history.clone();
                } else {
                    // A newly added proxy has not been measured, so it must not
                    // inherit a rating from the payload it was sent with.
                    proxy.rating = 0;
                    proxy.rating_history.clear();
                }
            }
        }
        reg.proxies = incoming;
        // Indices shifted, so the round-robin cursor has to be re-based.
        reg.cursor = 0;
        state_json(reg)
    });

    out_string(&mut map, "state", &state);
    map
}

/// Removes one entry by URL.
///
/// The caller must pass `data_name: ["proxy_url"]` with a `String`. `removed` is
/// `1` when an entry matched, `0` otherwise.
#[unsafe(no_mangle)]
fn proxy_remove(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let proxy_url = input_string(callback, "proxy_url");
    let mut map = HashMap::new();

    let (removed, state) = with_registry(true, |reg| {
        let before = reg.proxies.len();
        reg.proxies
            .retain(|p| !same_endpoint(&p.proxy_url, &proxy_url));
        let removed = u64::from(reg.proxies.len() != before);
        if removed == 1 {
            reg.cursor = 0;
        }
        (removed, state_json(reg))
    });

    out_u64(&mut map, "removed", removed);
    out_string(&mut map, "state", &state);
    map
}

/// Replaces the per-site probe schedule.
///
/// The caller must pass `data_name: ["sites"]` with a `String` holding a JSON
/// array of `{site, retry_after_secs, last_probe}`.
#[unsafe(no_mangle)]
fn proxy_set_sites(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let raw = input_string(callback, "sites");
    let mut map = HashMap::new();

    let sites = json::parse(raw.trim())
        .map(json_to_sites)
        .unwrap_or_default();
    let count = sites.len() as u64;
    let state = with_registry(true, |reg| {
        reg.sites = sites;
        state_json(reg)
    });

    out_u64(&mut map, "count", count);
    out_string(&mut map, "state", &state);
    map
}

/// Probes every entry in the list against a URL and records the result.
///
/// The caller must pass
/// `data_name: ["url", "site", "timeout_ms", "retry_after_secs"]` with
/// `String, String, U64, U64`, the same shape as `proxy_request`.
///
/// Unlike `proxy_request`, this walks the whole list rather than letting
/// `pick_proxy` choose, which is what makes it usable as a "test every proxy"
/// button. Each entry is probed in turn, so a slow proxy costs its own timeout
/// and the round takes roughly the sum of the per-proxy timeouts.
#[unsafe(no_mangle)]
fn proxy_test(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let url = input_string(callback, "url");
    let site = input_string(callback, "site");
    let timeout_ms = input_u64(callback, "timeout_ms", DEFAULT_TIMEOUT_MS);
    let retry_after_secs = input_u64(callback, "retry_after_secs", DEFAULT_RETRY_AFTER_SECS);

    let mut map = HashMap::new();

    if url.trim().is_empty() {
        out_u64(&mut map, "tested", 0);
        out_u64(&mut map, "passed", 0);
        out_string(&mut map, "state", "");
        return map;
    }

    // Snapshot first: the requests run without the lock held, and the list can
    // be edited through another callback while the round is in flight. Results
    // are recorded by URL rather than by index for the same reason.
    let targets: Vec<(String, String)> = with_registry(false, |reg| {
        reg.proxies
            .iter()
            .map(|p| (p.proxy_type.clone(), p.proxy_url.clone()))
            .collect()
    });

    let mut passed = 0u64;
    for (proxy_type, proxy_url) in &targets {
        let ok = attempt_request(proxy_type, proxy_url, &url, timeout_ms).is_ok();
        passed += u64::from(ok);

        let proxy_url = proxy_url.clone();
        with_registry(true, |reg| {
            if let Some(idx) = reg
                .proxies
                .iter()
                .position(|p| same_endpoint(&p.proxy_url, &proxy_url))
            {
                record(reg, idx, ok);
            }
        });
    }

    let now = now_secs();
    let tested = targets.len() as u64;
    let state = with_registry(true, |reg| {
        // A round that just measured every entry is a probe of the site.
        note_probe(reg, &site, retry_after_secs, now);
        state_json(reg)
    });

    out_u64(&mut map, "tested", tested);
    out_u64(&mut map, "passed", passed);
    out_string(&mut map, "state", &state);
    map
}

///
/// The caller must pass
/// `data_name: ["url", "site", "timeout_ms", "retry_after_secs"]` with
/// `String, String, U64, U64` values. `timeout_ms` and `retry_after_secs` are
/// optional and fall back to [`DEFAULT_TIMEOUT_MS`] and
/// [`DEFAULT_RETRY_AFTER_SECS`].
#[unsafe(no_mangle)]
fn proxy_request(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let url = input_string(callback, "url");
    let site = input_string(callback, "site");
    let timeout_ms = input_u64(callback, "timeout_ms", DEFAULT_TIMEOUT_MS);
    let retry_after_secs = input_u64(callback, "retry_after_secs", DEFAULT_RETRY_AFTER_SECS);

    let mut map = HashMap::new();

    if url.trim().is_empty() {
        out_u64(&mut map, "success", 0);
        out_u64(&mut map, "status", 0);
        out_string(&mut map, "proxy_url", "");
        out_u64(&mut map, "rating", 0);
        map.insert("body".to_string(), CallbackCustomDataReturning::VU8(vec![]));
        return map;
    }

    let now = now_secs();
    let picked = with_registry(false, |reg| pick_proxy(reg, &site, now, true));

    let Some(idx) = picked else {
        out_u64(&mut map, "success", 0);
        out_u64(&mut map, "status", 0);
        out_string(&mut map, "proxy_url", "");
        out_u64(&mut map, "rating", 0);
        map.insert("body".to_string(), CallbackCustomDataReturning::VU8(vec![]));
        return map;
    };

    let (proxy_url, proxy_type, proxy_name) = with_registry(false, |reg| {
        let p = &reg.proxies[idx];
        (p.proxy_url.clone(), p.proxy_type.clone(), p.name.clone())
    });

    // The request runs without the registry lock held: it is bounded by
    // `timeout_ms` but can still be slow, and persisting needs the lock.
    let attempt = attempt_request(&proxy_type, &proxy_url, &url, timeout_ms);

    let ok = attempt.is_ok();
    let (status, body) = match &attempt {
        Ok((status, body)) => (*status, body.clone()),
        Err(_) => (0u64, Vec::new()),
    };

    let rating = with_registry(true, |reg| {
        if idx < reg.proxies.len() {
            let score = record(reg, idx, ok);
            note_probe(reg, &site, retry_after_secs, now);
            score
        } else {
            0
        }
    });

    if let Err(reason) = &attempt {
        let _ = client::log_silent(format!(
            "proxy_request via `{proxy_name}` ({proxy_type}) failed: {reason}"
        ));
    }

    out_u64(&mut map, "success", u64::from(ok));
    out_u64(&mut map, "status", status);
    out_string(&mut map, "proxy_url", &proxy_url);
    out_string(&mut map, "proxy_type", &proxy_type);
    out_u64(&mut map, "rating", rating as u64);
    map.insert("body".to_string(), CallbackCustomDataReturning::VU8(body));

    map
}

/// Records an outcome the caller observed itself, for callers that already have
/// their own HTTP stack and only want the ranking updated.
///
/// The caller must pass
/// `data_name: ["proxy_url", "site", "success"]` with `String, String, U64`.
///
/// `proxy_url` matches an entry with or without an `http://` prefix. Returns
/// `found` (0 when the URL is not in the list, in which case nothing was scored)
/// and `rating`.
#[unsafe(no_mangle)]
fn proxy_report(callback: &CallbackInfoInput) -> HashMap<String, CallbackCustomDataReturning> {
    let proxy_url = input_string(callback, "proxy_url");
    let site = input_string(callback, "site");
    let success = input_u64(callback, "success", 0) == 1;
    // `proxy_report` does not register a `retry_after_secs` input, and adding one
    // would change the `CallbackInfo` the host matches on, so this call always
    // stamps the default window.
    let retry_after_secs = DEFAULT_RETRY_AFTER_SECS;
    let now = now_secs();

    let mut map = HashMap::new();
    let (found, rating) = with_registry(true, |reg| {
        let found = reg
            .proxies
            .iter()
            .position(|p| same_endpoint(&p.proxy_url, &proxy_url));
        let rating = match found {
            Some(idx) => record(reg, idx, success),
            // An unknown URL is not evidence about any listed proxy, so nothing
            // is scored; the caller is told via `found` so it can tell this apart
            // from a genuine failure.
            None => 0,
        };
        note_probe(reg, &site, retry_after_secs, now);
        (found.is_some(), rating)
    });

    out_u64(&mut map, "found", u64::from(found));
    out_u64(&mut map, "rating", rating as u64);
    map
}

/// Whether two proxy URLs point at the same endpoint, ignoring an `http://` or
/// `https://` prefix and any trailing slash.
///
/// Stored lists mix bare `1.2.3.4:8080` entries with fully qualified ones, and
/// a caller reporting a result should not have to know which form it got. SOCKS
/// schemes stay significant because they select a different protocol.
fn same_endpoint(a: &str, b: &str) -> bool {
    fn norm(url: &str) -> &str {
        let url = url.trim();
        let url = if url.len() >= 7 && url[..7].eq_ignore_ascii_case("http://") {
            &url[7..]
        } else if url.len() >= 8 && url[..8].eq_ignore_ascii_case("https://") {
            &url[8..]
        } else {
            url
        };
        url.trim_end_matches('/')
    }

    norm(a) == norm(b)
}

/// Performs one request through `proxy_url`, returning `(status, body)`.
///
/// Any HTTP response counts as success, including 4xx and 5xx: the question is
/// whether the proxy carried the request, not whether the target liked it.
fn attempt_request(
    proxy_type: &str,
    proxy_url: &str,
    url: &str,
    timeout_ms: u64,
) -> Result<(u64, Vec<u8>), String> {
    let proxy = reqwest_proxy(&Proxy {
        name: String::new(),
        proxy_type: proxy_type.to_string(),
        proxy_url: proxy_url.to_string(),
        rating: 0,
        rating_history: Vec::new(),
    })?;

    let client = reqwest::blocking::Client::builder()
        .proxy(proxy)
        .timeout(Duration::from_millis(timeout_ms))
        .build()
        .map_err(|e| e.to_string())?;

    let response = client.get(url).send().map_err(|e| e.to_string())?;
    let status = response.status().as_u16() as u64;
    let body = response.bytes().map(|b| b.to_vec()).unwrap_or_default();

    Ok((status, body))
}

// ---------------------------------------------------------------------------
// plugin registration and http server
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "proxy".into(),
        callbacks: vec![
            GlobalCallbacks::Start(shared_types::StartupThreadType::Spawn),
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_get_proxy".to_string(),
                vers: 0,
                data_name: vec!["site".into()],
                data: vec![CallbackCustomData::String],
            }),
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_request".to_string(),
                vers: 0,
                data_name: vec![
                    "url".into(),
                    "site".into(),
                    "timeout_ms".into(),
                    "retry_after_secs".into(),
                ],
                data: vec![
                    CallbackCustomData::String,
                    CallbackCustomData::String,
                    CallbackCustomData::U64,
                    CallbackCustomData::U64,
                ],
            }),
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_report".to_string(),
                vers: 0,
                data_name: vec!["proxy_url".into(), "site".into(), "success".into()],
                data: vec![
                    CallbackCustomData::String,
                    CallbackCustomData::String,
                    CallbackCustomData::U64,
                ],
            }),
            // Management surface, used by the web UI's proxy manager.
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_get_state".to_string(),
                vers: 0,
                data_name: vec!["site".into()],
                data: vec![CallbackCustomData::String],
            }),
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_set_list".to_string(),
                vers: 0,
                data_name: vec!["list".into(), "preserve_ratings".into()],
                data: vec![CallbackCustomData::String, CallbackCustomData::U64],
            }),
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_remove".to_string(),
                vers: 0,
                data_name: vec!["proxy_url".into()],
                data: vec![CallbackCustomData::String],
            }),
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_set_sites".to_string(),
                vers: 0,
                data_name: vec!["sites".into()],
                data: vec![CallbackCustomData::String],
            }),
            GlobalCallbacks::Callback(CallbackInfo {
                func: "proxy_test".to_string(),
                vers: 0,
                data_name: vec![
                    "url".into(),
                    "site".into(),
                    "timeout_ms".into(),
                    "retry_after_secs".into(),
                ],
                data: vec![
                    CallbackCustomData::String,
                    CallbackCustomData::String,
                    CallbackCustomData::U64,
                    CallbackCustomData::U64,
                ],
            }),
        ],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
fn on_start() {
    let proxies = with_registry(false, |reg| reg.proxies.clone());
    let sites = with_registry(false, |reg| reg.sites.clone());

    if proxies.is_empty() {
        let _ = client::log_silent(format!(
            "No proxies in {SETTING_LIST} yet, every check will fail until it is filled in"
        ));
    }

    for proxy in &proxies {
        let _ = client::log_silent(format!(
            "Loaded proxy `{}` ({}) rating {} rate {:.2} history {:?}",
            proxy.name,
            proxy.proxy_type,
            proxy.rating,
            success_rate(proxy),
            proxy.rating_history
        ));
    }
    for site in &sites {
        let _ = client::log_silent(format!(
            "Site `{}` re-probe every {}s, last probed {}s ago",
            site.site,
            site.retry_after_secs,
            now_secs().saturating_sub(site.last_probe)
        ));
    }

    // Plugins are built with `panic = "abort"` and `on_start` is called across an
    // `extern "C"` boundary, so nothing below may panic or the whole host process
    // goes down with it.
    if let Err(err) = spool_server() {
        let _ = client::log_silent(format!("Proxy server failed: {err}"));
    }
}

fn text_response(status: StatusCode, body: &str) -> vetis::VetisResult<vetis::Response> {
    Ok(vetis::Response::builder().status(status).text(body))
}

async fn health_handler(_request: vetis::Request) -> vetis::VetisResult<vetis::Response> {
    text_response(StatusCode::OK, "Health check")
}

/// Reports the current ranking as JSON.
async fn proxies_handler(_request: vetis::Request) -> vetis::VetisResult<vetis::Response> {
    let body = with_registry(false, |reg| {
        let list: Vec<JsonValue> = reg.proxies.iter().map(proxy_to_json).collect();
        let sites: Vec<JsonValue> = reg.sites.iter().map(site_to_json).collect();
        json::stringify(json::object! { proxies: list, sites: sites })
    });

    text_response(StatusCode::OK, &body)
}

/// Answers with a single potentially valid proxy as plain text, which is the
/// quickest way to check the plugin by hand.
///
/// `?site=` scopes the answer the same way the callbacks are scoped, and an
/// empty 503 means "nothing in the list has passed a check yet".
async fn best_proxy_handler(request: vetis::Request) -> vetis::VetisResult<vetis::Response> {
    let site = request
        .uri()
        .query()
        .and_then(|query| query.split('&').find_map(|pair| pair.strip_prefix("site=")))
        .unwrap_or_default()
        .to_string();

    let now = now_secs();
    let picked = with_registry(false, |reg| pick_proxy(reg, &site, now, false));
    let body = match picked {
        Some(idx) => with_registry(false, |reg| reg.proxies[idx].proxy_url.clone()),
        None => String::new(),
    };

    if body.is_empty() {
        text_response(StatusCode::SERVICE_UNAVAILABLE, "")
    } else {
        text_response(StatusCode::OK, &body)
    }
}

#[tokio::main]
async fn spool_server() -> Result<(), Box<dyn std::error::Error>> {
    let port = 8080;

    let https = ListenerConfig::builder()
        .port(port)
        .protos(vec![Version::HTTP_11])
        .interface("0.0.0.0".parse().unwrap())
        // Required on the beta line. Without it the TCP worker accepts a
        // connection, peeks to decide TLS vs plaintext, and on a plaintext
        // request returns `Ok(())` without writing a response — the client sees
        // "connection reset by peer" and curl reports HTTP 000. 0.1.6 allowed
        // plaintext implicitly, so this opt-in did not exist before.
        .allow_unsafe_connections(true)
        .build()
        .unwrap();

    // vetis keys virtual hosts by `hostname:port`, so register every name a local
    // probe can legitimately arrive under. Without this a probe to
    // `http://127.0.0.1:8080/health` is answered with 502 even though
    // `http://localhost:8080/health` works.
    let mut builder = Vetis::builder().add_listeners(build_listeners(https))?;

    for hostname in ["localhost", "127.0.0.1", "[::1]"] {
        // The beta line attaches a host to each listener whose interface and
        // port match an entry in the host's `bind_addresses`. A host with no bind
        // addresses is silently never registered, and the listener then accepts
        // connections and resets them, so this must mirror the listener config.
        let host_config = HostConfig::builder()
            .hostname(hostname)
            .bind_addresses(vec![("0.0.0.0".parse()?, port)])
            .build()?;
        let mut host = Host::new(host_config);
        // `HandlerPath` boxes the handler and is not `Clone`, so every virtual
        // host needs its own instance of each route.
        host.add_path(
            HandlerPath::builder()
                .uri("/health")
                .handler(handler_fn(health_handler))
                .build()?,
        );
        host.add_path(
            HandlerPath::builder()
                .uri("/proxies")
                .handler(handler_fn(proxies_handler))
                .build()?,
        );
        host.add_path(
            HandlerPath::builder()
                .uri("/proxy")
                .handler(handler_fn(best_proxy_handler))
                .build()?,
        );
        builder = builder.add_host(host)?;
    }

    let mut server = builder.build();

    let _ = client::log_silent_async(format!("Starting proxy server at: 0.0.0.0:{port}")).await;

    // `run` would block on `tokio::signal::ctrl_c`, which both swallows the
    // host's shutdown signal and leaves the exit poller below unreachable. Start
    // the listeners instead and drive shutdown from the host's exit flag.
    server.start().await?;

    loop {
        let exit = client::should_exit_async().await;

        if exit.is_err() || exit.is_ok_and(|f| f) {
            break;
        }

        Timer::after(Duration::from_secs(1)).await;
    }

    let _ = client::log_silent_async("Stopping proxy server".into()).await;
    server.stop().await?;

    Ok(())
}

use http::{StatusCode, Version};
use smol::Timer;
use vetis::{
    VetisServer as _,
    host::{HostConfig, handler_fn},
    listener::ListenerConfig,
};
use vetis_tokio::{
    Vetis,
    host::{Host, path::HandlerPath},
    listener::build_listeners,
};
