use anyhow::Result;
use arti_client::{TorClient, config::TorClientConfigBuilder};
use axum::extract::{Path, Query, Request, State};
use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use hyper::StatusCode;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server;
use rand::random;
use safelog::{DisplayRedacted as _, sensitive};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use shared_types::{DbSettingsObj, GlobalCallbacks, Tag};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tor_cell::relaycell::msg::Connected;
use tor_hsservice::StreamRequest;
use tor_hsservice::config::OnionServiceConfigBuilder;
use tor_proto::stream::IncomingStreamRequest;
use tower::Service;
use tower::ServiceExt;
use tower_http::services::ServeFile;

const API_VERSION: u64 = 1;
const RATE_LIMIT: Duration = Duration::from_secs(10);
const POW_DIFFICULTY: usize = 5;
const POW_MAX_AGE: Duration = Duration::from_secs(300);
const CLOCK_SKEW: Duration = Duration::from_secs(30);
const TOKEN_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_TRACKED_TOKENS: usize = 10_000;
const STORAGE_SETTING: &str = "TOR_SERVE_FILE_LOCATION";
const DEFAULT_STORAGE_LOCATION: &str = "./tor-serve";
const MAX_CONCURRENT_REQUESTS_SETTING: &str = "TOR_SERVE_MAX_CONCURRENT_REQUESTS";
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 4;

#[derive(Clone)]
struct AppState {
    rate: Arc<Mutex<HashMap<String, SystemTime>>>,
    issued_tokens: Arc<Mutex<HashMap<String, SystemTime>>>,
    http: reqwest::Client,
    admin_token: Option<String>,
    token_secret: [u8; 32],
    request_limits: Arc<Mutex<HashMap<String, (Arc<tokio::sync::Semaphore>, SystemTime)>>>,
    max_concurrent_requests: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SearchPolicy {
    allowed_extensions: Option<HashSet<String>>,
    denied_tags: HashSet<String>,
    required_tags: HashSet<String>,
    max_results: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    tags: Option<String>,
    limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BooruQuery {
    tags: Option<String>,
    page: Option<u64>,
    limit: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SearchResult {
    file_id: u64,
    extension: Option<String>,
    url: String,
    tags: HashSet<Tag>,
}

#[derive(Debug, Serialize)]
struct FileMetadata {
    extension: String,
    tags: Vec<Tag>,
    hashes: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ProofChallenge {
    challenge: String,
    difficulty: usize,
    algorithm: &'static str,
    header: &'static str,
}

fn json_error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (status, Json(serde_json::json!({ "error": message.into() })))
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
        .map(str::to_string)
}

fn token_key(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn policy_name(token: &str) -> String {
    format!("tor-serve.policy.{}", token_key(token))
}

fn storage_location() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let configured = client::setting_get(STORAGE_SETTING.to_owned())?;
    let location = configured
        .as_ref()
        .and_then(|setting| setting.param.as_deref())
        .filter(|path| !path.trim().is_empty())
        .unwrap_or(DEFAULT_STORAGE_LOCATION)
        .to_owned();
    if configured.is_none() {
        client::setting_set(DbSettingsObj {
            name: STORAGE_SETTING.to_owned(),
            description: Some("Directory used by tor-serve for local file storage.".to_owned()),
            num: None,
            param: Some(location.clone()),
        })?;
    }
    std::fs::create_dir_all(&location)?;
    Ok(PathBuf::from(location))
}

fn max_concurrent_requests() -> Result<usize, Box<dyn std::error::Error>> {
    let configured = client::setting_get(MAX_CONCURRENT_REQUESTS_SETTING.to_owned())?;
    let value = configured
        .as_ref()
        .and_then(|setting| setting.param.as_deref())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS);

    if configured.is_none() {
        client::setting_set(DbSettingsObj {
            name: MAX_CONCURRENT_REQUESTS_SETTING.to_owned(),
            description: Some(
                "Maximum number of concurrent requests allowed per client token.".to_owned(),
            ),
            num: None,
            param: Some(value.to_string()),
        })?;
    }

    Ok(value)
}

fn create_private_directory(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }

    Ok(())
}

async fn load_policy(
    token: &str,
) -> Result<SearchPolicy, Box<dyn std::error::Error + Send + Sync>> {
    client::setting_get_async(policy_name(token))
        .await?
        .and_then(|setting| {
            setting
                .param
                .and_then(|param| serde_json::from_str(&param).ok())
        })
        .map(|mut policy: SearchPolicy| {
            policy.allowed_extensions = policy.allowed_extensions.map(|extensions| {
                extensions
                    .into_iter()
                    .map(|extension| extension.trim_start_matches('.').to_ascii_lowercase())
                    .collect()
            });
            policy.denied_tags = policy
                .denied_tags
                .into_iter()
                .map(|tag| tag.to_ascii_lowercase())
                .collect();
            policy.required_tags = policy
                .required_tags
                .into_iter()
                .map(|tag| tag.to_ascii_lowercase())
                .collect();
            Ok(policy)
        })
        .unwrap_or_else(|| Ok(SearchPolicy::default()))
}

async fn permitted(
    policy: &SearchPolicy,
    file_id: u64,
    extension: &str,
    tags: &HashSet<Tag>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let allowed = permitted_tags(policy, extension, tags);
    Ok(allowed && client::get_file_path_async(file_id).await?.is_some())
}

fn permitted_tags(policy: &SearchPolicy, extension: &str, tags: &HashSet<Tag>) -> bool {
    if let Some(allowed) = &policy.allowed_extensions
        && !allowed.contains(&extension.to_ascii_lowercase())
    {
        return false;
    }
    let names = tags
        .iter()
        .map(|tag| tag.name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    // A file is denied when any of its tags is blacklisted, regardless of
    // whether the tag was also part of the search query.
    let denied = policy.denied_tags.iter().any(|tag| names.contains(tag));
    let required = policy
        .required_tags
        .iter()
        .all(|tag| names.contains(&tag.to_ascii_lowercase()));
    !denied && required
}

fn check_access(
    headers: &HeaderMap,
    state: &AppState,
) -> Result<(String, bool), (StatusCode, Json<Value>)> {
    let Some(token) = bearer_token(headers) else {
        return Err(json_error(
            StatusCode::UNAUTHORIZED,
            "missing bearer token; solve a challenge and call /api/v1/auth/token",
        ));
    };
    let now = SystemTime::now();
    let known_token = state
        .admin_token
        .as_deref()
        .is_some_and(|admin| admin == token)
        || {
            let mut tokens = state
                .issued_tokens
                .lock()
                .expect("issued-token lock poisoned");
            tokens.retain(|_, issued| now.duration_since(*issued).unwrap_or_default() < TOKEN_TTL);
            tokens.contains_key(&token)
        };
    if !known_token {
        return Err(json_error(StatusCode::UNAUTHORIZED, "unknown bearer token"));
    }
    let pow = headers
        .get("x-proof-of-work")
        .and_then(|value| value.to_str().ok())
        .is_some_and(valid_proof);
    if !pow {
        let mut rate = state.rate.lock().expect("rate limiter lock poisoned");
        rate.retain(|_, previous| now.duration_since(*previous).unwrap_or_default() < RATE_LIMIT);
        if let Some(previous) = rate.get(&token)
            && now.duration_since(*previous).unwrap_or_default() < RATE_LIMIT
        {
            let retry = RATE_LIMIT
                .saturating_sub(now.duration_since(*previous).unwrap_or_default())
                .as_secs();
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({ "error": "rate limit exceeded", "retry_after": retry })),
            ));
        }
        rate.insert(token.clone(), now);
    }
    Ok((token, pow))
}

async fn per_token_concurrency(
    State(state): axum::extract::State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let Some(token) = bearer_token(request.headers()) else {
        return next.run(request).await;
    };

    let now = SystemTime::now();
    let semaphore = {
        let mut limits = state
            .request_limits
            .lock()
            .expect("request-limit lock poisoned");
        limits.retain(|_, (semaphore, last_used)| {
            now.duration_since(*last_used).unwrap_or_default() < TOKEN_TTL
                || semaphore.available_permits() < state.max_concurrent_requests
        });
        if limits.len() >= MAX_TRACKED_TOKENS && !limits.contains_key(&token) {
            if let Some(oldest) = limits
                .iter()
                .filter(|(_, (semaphore, _))| {
                    semaphore.available_permits() == state.max_concurrent_requests
                })
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(token, _)| token.clone())
            {
                limits.remove(&oldest);
            } else {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
        limits
            .entry(token)
            .and_modify(|(_, last_used)| *last_used = now)
            .or_insert_with(|| {
                (
                    Arc::new(tokio::sync::Semaphore::new(state.max_concurrent_requests)),
                    now,
                )
            })
            .0
            .clone()
    };

    let permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let response = next.run(request).await;
    drop(permit);
    response
}

fn valid_proof(value: &str) -> bool {
    value.split_once(':').is_some_and(|(challenge, nonce)| {
        let Ok(timestamp) = u128::from_str_radix(challenge, 16) else {
            return false;
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let skew = CLOCK_SKEW.as_nanos();
        if timestamp > now.saturating_add(skew)
            || now
                > timestamp
                    .saturating_add(POW_MAX_AGE.as_nanos())
                    .saturating_add(skew)
        {
            return false;
        }
        let mut hasher = Sha256::new();
        hasher.update(challenge.as_bytes());
        hasher.update(nonce.as_bytes());
        let digest = hasher.finalize();
        let full_bytes = POW_DIFFICULTY / 2;
        let remaining_nibbles = POW_DIFFICULTY % 2;
        digest[..full_bytes].iter().all(|byte| *byte == 0)
            && (remaining_nibbles == 0 || digest[full_bytes] >> 4 == 0)
    })
}

async fn challenge() -> impl IntoResponse {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Json(ProofChallenge {
        challenge: format!("{now:x}"),
        difficulty: POW_DIFFICULTY,
        algorithm: "sha256(challenge + nonce)",
        header: "X-Proof-Of-Work: challenge:nonce",
    })
    .into_response()
}

async fn issue_token(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    let Some(proof) = headers
        .get("x-proof-of-work")
        .and_then(|value| value.to_str().ok())
    else {
        return json_error(StatusCode::BAD_REQUEST, "X-Proof-Of-Work is required").into_response();
    };
    if !valid_proof(proof) {
        return json_error(StatusCode::BAD_REQUEST, "invalid or expired proof of work")
            .into_response();
    }

    let mut material = state.token_secret.to_vec();
    material.extend_from_slice(proof.as_bytes());
    let token = hex::encode(Sha256::digest(material));
    let mut issued_tokens = state
        .issued_tokens
        .lock()
        .expect("issued-token lock poisoned");
    let now = SystemTime::now();
    issued_tokens.retain(|_, issued| now.duration_since(*issued).unwrap_or_default() < TOKEN_TTL);
    if issued_tokens.len() >= MAX_TRACKED_TOKENS {
        if let Some(oldest) = issued_tokens
            .iter()
            .min_by_key(|(_, issued)| *issued)
            .map(|(token, _)| token.clone())
        {
            issued_tokens.remove(&oldest);
        }
    }
    issued_tokens.insert(token.clone(), now);
    Json(serde_json::json!({
        "access_token": token,
        "token_type": "Bearer",
        "rate_limit": "1 request every 10 seconds without proof of work"
    }))
    .into_response()
}

async fn search(
    headers: HeaderMap,
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    let (token, _pow) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let policy = match load_policy(&token).await {
        Ok(policy) => policy,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    if query.tags.as_deref().unwrap_or_default().trim().is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "at least one search tag is required",
        )
        .into_response();
    }
    let limit = query.limit.or(policy.max_results).unwrap_or(100).min(1000);
    let tags = query
        .tags
        .unwrap_or_default()
        .split_whitespace()
        .filter(|tag| !tag.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let ids = match client::search_db_files_by_tags_async(tags, Some(limit)).await {
        Ok(ids) => ids,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    let mut results = Vec::new();
    for file_id in ids {
        let path = match client::get_file_path_async(file_id).await {
            Ok(Some(path)) => path,
            Ok(None) => continue,
            Err(error) => {
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    .into_response();
            }
        };
        let extension = PathBuf::from(&path)
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let tag_ids = match client::relationship_get_tagid_async(file_id).await {
            Ok(tag_ids) => tag_ids,
            Err(error) => {
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    .into_response();
            }
        };
        let file_tags = match client::get_tag_id_bulk_async(tag_ids).await {
            Ok(tags) => tags.into_values().collect(),
            Err(error) => {
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                    .into_response();
            }
        };
        if permitted(&policy, file_id, &extension, &file_tags)
            .await
            .unwrap_or(false)
        {
            results.push(SearchResult {
                file_id,
                extension: Some(extension),
                url: format!("/api/v1/file/{file_id}"),
                tags: file_tags,
            });
        }
    }
    Json(serde_json::json!({ "results": results, "count": results.len() })).into_response()
}

async fn booru_search(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(site): Path<String>,
    Query(query): Query<BooruQuery>,
) -> impl IntoResponse {
    let (token, _) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let policy = match load_policy(&token).await {
        Ok(policy) => policy,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    let tags = query.tags.unwrap_or_default();
    let requested_tags = tags
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect::<HashSet<_>>();
    if requested_tags.is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "at least one booru tag is required",
        )
        .into_response();
    }
    if policy
        .denied_tags
        .iter()
        .any(|tag| requested_tags.contains(tag))
        || !policy
            .required_tags
            .iter()
            .all(|tag| requested_tags.contains(tag))
    {
        return json_error(
            StatusCode::FORBIDDEN,
            "booru search violates the active tag policy",
        )
        .into_response();
    }
    let limit = query.limit.unwrap_or(100).min(320);
    let page = query.page.unwrap_or(1);
    let (url, params) = match site.to_ascii_lowercase().as_str() {
        "e621" | "e6" => (
            "https://e621.net/posts.json",
            vec![
                ("tags", tags),
                ("limit", limit.to_string()),
                ("page", page.to_string()),
            ],
        ),
        "rule34" | "r34" => (
            "https://api.rule34.xxx/index.php",
            vec![
                ("page", "dapi".into()),
                ("s", "post".into()),
                ("q", "index".into()),
                ("json", "1".into()),
                ("tags", tags),
                ("limit", limit.to_string()),
                ("pid", (page.saturating_sub(1)).to_string()),
            ],
        ),
        _ => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "unsupported booru; use e621 or rule34",
            )
            .into_response();
        }
    };
    match state.http.get(url).query(&params).send().await {
        Ok(response) => match response.json::<Value>().await {
            Ok(payload) => {
                Json(serde_json::json!({ "site": site, "results": payload })).into_response()
            }
            Err(_) => {
                json_error(StatusCode::BAD_GATEWAY, "booru returned invalid JSON").into_response()
            }
        },
        Err(_) => json_error(StatusCode::BAD_GATEWAY, "booru request failed").into_response(),
    }
}

async fn settings_get(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    let (token, _) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    match load_policy(&token).await {
        Ok(policy) => Json(policy).into_response(),
        Err(error) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    }
}

async fn settings_set(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(policy): Json<SearchPolicy>,
) -> impl IntoResponse {
    let (token, _) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let saved = match client::setting_set_async(DbSettingsObj {
        name: policy_name(&token),
        description: Some("tor-serve search policy".into()),
        num: None,
        param: Some(serde_json::to_string(&policy).unwrap_or_default()),
    })
    .await
    {
        Ok(saved) => saved,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    let persisted = match client::setting_get_async(policy_name(&token)).await {
        Ok(setting) => setting.is_some(),
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    Json(serde_json::json!({ "saved": saved || persisted, "policy": policy })).into_response()
}

#[unsafe(no_mangle)]
fn on_start() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            main().await;
        })
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "tor-serve".into(),
        callbacks: vec![GlobalCallbacks::Start(
            shared_types::StartupThreadType::Spawn,
        )],
        ..Default::default()
    }]
}

/// Gets tags associated with a file
async fn get_file_tags(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(file_id): Path<u64>,
) -> impl IntoResponse {
    let (token, _) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let tag_ids = match client::relationship_get_tagid_async(file_id).await {
        Ok(tag_ids) => tag_ids,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let tags = match client::get_tag_id_bulk_async(tag_ids).await {
        Ok(tags) => tags.into_values().collect(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let policy = match load_policy(&token).await {
        Ok(policy) => policy,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    match client::get_file_path_async(file_id).await {
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Ok(Some(path)) => {
            let extension = PathBuf::from(path)
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if permitted(&policy, file_id, &extension, &tags)
                .await
                .unwrap_or(false)
            {
                return Json(tags).into_response();
            }
        }
        Ok(None) => {}
    }
    StatusCode::NOT_FOUND.into_response()
}

/// Gets tag ids associated with a file
async fn get_file_tag_ids(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(file_id): Path<u64>,
) -> impl IntoResponse {
    let (token, _) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let tag_ids = match client::relationship_get_tagid_async(file_id).await {
        Ok(tag_ids) => tag_ids,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let policy = match load_policy(&token).await {
        Ok(policy) => policy,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    let allowed = if let Some(path) = match client::get_file_path_async(file_id).await {
        Ok(path) => path,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    } {
        let extension = PathBuf::from(path)
            .extension()
            .and_then(|value| value.to_str().map(str::to_string));
        let tags = match client::get_tag_id_bulk_async(tag_ids.clone()).await {
            Ok(tags) => tags.into_values().collect(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        extension.is_some_and(|extension| permitted_tags(&policy, &extension, &tags))
    } else {
        false
    };
    if allowed {
        Json(tag_ids).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn get_file_metadata(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(file_id): Path<u64>,
) -> impl IntoResponse {
    let (token, _) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let policy = match load_policy(&token).await {
        Ok(policy) => policy,
        Err(error) => {
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    let path = match client::get_file_path_async(file_id).await {
        Ok(Some(path)) => path,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let tag_ids = match client::relationship_get_tagid_async(file_id).await {
        Ok(ids) => ids,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let tags: Vec<Tag> = match client::get_tag_id_bulk_async(tag_ids).await {
        Ok(tags) => tags.into_values().collect(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let file_path = PathBuf::from(path);
    let extension = file_path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let tag_set = tags.iter().cloned().collect();
    if !permitted(&policy, file_id, extension, &tag_set)
        .await
        .unwrap_or(false)
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let hashes = match client::get_file_hashes_async(file_id).await {
        Ok(hashes) => hashes,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    Json(FileMetadata {
        extension: extension.to_owned(),
        tags,
        hashes,
    })
    .into_response()
}

/// Gets a file directly from disk
async fn get_file(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(file_id): Path<u64>,
    req: Request,
) -> impl IntoResponse {
    let (token, _) = match check_access(&headers, &state) {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    if let Some(filepath) = match client::get_file_path_async(file_id).await {
        Ok(filepath) => filepath,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    } {
        let path = PathBuf::from(&filepath);
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let tag_ids = match client::relationship_get_tagid_async(file_id).await {
            Ok(tag_ids) => tag_ids,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let tags = match client::get_tag_id_bulk_async(tag_ids).await {
            Ok(tags) => tags.into_values().collect(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let policy = match load_policy(&token).await {
            Ok(policy) => policy,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        if !permitted(&policy, file_id, extension, &tags)
            .await
            .unwrap_or(false)
        {
            return StatusCode::NOT_FOUND.into_response();
        }

        // Initialize the ServeFile service
        let service = ServeFile::new(path);

        // Run the service with the incoming request using oneshot
        match service.oneshot(req).await {
            Ok(response) => return response.into_response(),
            Err(_) => {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn main() {
    tracing_subscriber::fmt::init();

    let storage_location = match storage_location() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("unable to initialize tor-serve storage directory: {error}");
            return;
        }
    };
    let max_concurrent_requests = match max_concurrent_requests() {
        Ok(value) => value,
        Err(error) => {
            eprintln!("unable to load tor-serve concurrency setting: {error}");
            return;
        }
    };

    let admin_token = std::env::var("TOR_SERVE_AUTH_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let state = AppState {
        rate: Arc::new(Mutex::new(HashMap::new())),
        issued_tokens: Arc::new(Mutex::new(HashMap::new())),
        http: reqwest::Client::builder()
            .user_agent("IntScrape tor-serve/1")
            .build()
            .unwrap(),
        admin_token,
        token_secret: random(),
        request_limits: Arc::new(Mutex::new(HashMap::new())),
        max_concurrent_requests,
    };

    let router = Router::new()
        .route("/api/v1/file/{file_id}", get(get_file))
        .route("/api/v1/file/{file_id}/tag_ids", get(get_file_tag_ids))
        .route("/api/v1/file/{file_id}/tags", get(get_file_tags))
        .route("/api/v1/file/{file_id}/metadata", get(get_file_metadata))
        .route("/api/v1/challenge", get(challenge))
        .route("/api/v1/auth/token", post(issue_token))
        .route("/api/v1/search", get(search))
        .route("/api/v1/booru/{site}", get(booru_search))
        .route("/api/v1/settings", get(settings_get).post(settings_set))
        .route(
            "/",
            get(|| async { format!("Hello from version: {}", API_VERSION) }),
        )
        .with_state(state.clone())
        .layer(from_fn_with_state(state, per_token_concurrency));

    let arti_state = storage_location.join("arti-state");
    let arti_cache = storage_location.join("arti-cache");
    if let Err(error) =
        create_private_directory(&arti_state).and_then(|_| create_private_directory(&arti_cache))
    {
        eprintln!("unable to initialize Arti directories: {error}");
        return;
    }
    let config = TorClientConfigBuilder::from_directories(&arti_state, &arti_cache)
        .build()
        .expect("valid Arti storage directories");

    let client = match TorClient::create_bootstrapped(config).await {
        Ok(client) => client,
        Err(error) => {
            eprintln!("unable to bootstrap Arti: {error}");
            return;
        }
    };

    let svc_cfg = OnionServiceConfigBuilder::default()
        .nickname("intscrape-server".parse().unwrap())
        .build()
        .unwrap();

    let Some((service, request_stream)) = client.launch_onion_service(svc_cfg).unwrap() else {
        let _ = client::log_silent_async(format!("onion service was disabled in config")).await;
        return;
    };

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        loop {
            let should_exit = match client::should_exit_async().await {
                Ok(value) => value,
                Err(error) => {
                    let _ =
                        client::log_silent_async(format!("could not check exit status: {error}"))
                            .await;
                    true // Treat an error as a shutdown request, if appropriate.
                }
            };

            if should_exit {
                let _ = shutdown_tx.send(true);
                break;
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    let mut status_events = service.status_events();

    loop {
        tokio::select! {
            status = status_events.next() => {
                match status {
                    Some(status) => {
                  let _ =      client::log_silent_async(format!("onion service status: {:?}", status.state())).await;

                        if status.state().is_fully_reachable() {
                            break;
                        }
                    }

                    None => {
                     let _ =   client::log_silent_async(format!("onion service status stream ended")).await;
                        return;
                    }
                }
            }

            result = shutdown_rx.changed() => {
                match result {
                    Ok(()) if *shutdown_rx.borrow() => {
                    let _ =    client::log_silent_async(format!("shutting down before service became reachable")).await;
                        drop(service);
                        return;
                    }

                    Ok(()) => {}

                    Err(_) => {
                  let _ =      client::log_silent_async(format!("shutdown sender was dropped")).await;
                        drop(service);
                        return;
                    }
                }
            }
        }
    }

    let _ = client::log_silent_async(format!(
        "ready to serve connections on: {}",
        service.onion_address().unwrap().display_unredacted()
    ))
    .await;
    let stream_requests = tor_hsservice::handle_rend_requests(request_stream);
    tokio::pin!(stream_requests);

    loop {
        tokio::select! {
            request = stream_requests.next() => {
                let Some(stream_request) = request else {
                    break;
                };

                let router = router.clone();

                tokio::spawn(async move {
                    let request = stream_request.request().clone();

                    if let Err(err) =
                        handle_stream_request(stream_request, router).await
                    {
                      let _ =   client::log_silent_async(format!(
                            "error serving connection {:?}: {}",
                            sensitive(request),
                            err
                        )).await;
                    }
                });
            }

            result = shutdown_rx.changed() => {
                if result.is_ok() && *shutdown_rx.borrow() {
                  let _ =  client::log_silent_async(format!("shutting down onion service")).await;
                    break;
                }
            }
        }
    }

    // Dropping the service and request stream stops accepting new requests.
    drop(service);
    let _ = client::log_silent_async(format!("onion service exited cleanly")).await;
}

async fn handle_stream_request(stream_request: StreamRequest, router: Router) -> Result<()> {
    match stream_request.request() {
        IncomingStreamRequest::Begin(begin) if begin.port() == 80 => {
            let onion_service_stream = stream_request.accept(Connected::new_empty()).await?;
            let io = TokioIo::new(onion_service_stream);

            let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| {
                router.clone().call(request)
            });

            server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_service)
                .await
                .map_err(|x| anyhow::anyhow!(x))?;
        }
        _ => {
            stream_request.shutdown_circuit()?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(name: &str) -> Tag {
        Tag {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn blacklisted_file_tag_is_denied() {
        let policy = SearchPolicy {
            denied_tags: HashSet::from(["unsafe".to_owned()]),
            ..Default::default()
        };
        let tags = HashSet::from([tag("safe"), tag("unsafe")]);

        assert!(!permitted_tags(&policy, "jpg", &tags));
    }

    #[test]
    fn unrelated_file_tags_are_allowed() {
        let policy = SearchPolicy {
            denied_tags: HashSet::from(["unsafe".to_owned()]),
            ..Default::default()
        };
        let tags = HashSet::from([tag("safe")]);

        assert!(permitted_tags(&policy, "jpg", &tags));
    }
}
