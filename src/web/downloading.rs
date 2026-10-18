use std::{str::FromStr, time::Duration};

use log::info;
use reqwest::{
    Client, ClientBuilder, RequestBuilder, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use shared_types::{DownloadModifiers, ModifierTarget, ScraperParam, TargetModifier};
use url::Url;

use crate::web::manager::{Scraper, is_retryable_status};

pub(in crate::web) const MAX_URL_RETRY: u8 = 3;

/// Result of downloading one url's text.
///
/// `dltext` retries a url up to [`MAX_URL_RETRY`] times. Once those attempts
/// are spent the caller has to decide what happens to the *whole* job, which a
/// bare `Option` cannot express: "this url is a dead end" and "this site is
/// broken" need different handling from "there is nothing to download here".
pub(in crate::web) enum TextDownload {
    /// The param was not a url at all (`ScraperParam::Normal` and friends), so
    /// there is nothing to download for it and the job carries on to the next
    /// param. This is not a failure.
    Skipped,
    /// The text was retrieved, along with the url it came from.
    Success(String, String),
    /// Every attempt returned 404. On a paginated listing this is the site
    /// saying "nothing past this offset", so the job is finished rather than
    /// broken and does not need another pass on the next boot.
    NotFound,
    /// Some other error outlived `MAX_URL_RETRY` attempts, or the url could
    /// never be parsed. Treated as a transient site problem, so the job is
    /// kept and retried on the next boot.
    Failed,
}

impl Scraper {
    fn process_modifiers(
        client: ClientBuilder,
        target: Vec<TargetModifier>,
        is_text_download: bool,
    ) -> ClientBuilder {
        let mut client = client;
        for modifer in target {
            let is_text_modifier = modifer.target == ModifierTarget::Text;
            if is_text_modifier != is_text_download {
                continue;
            }
            match modifer.modifier {
                DownloadModifiers::Header((key, val)) => {
                    let key = key.clone();
                    let val = val.clone();
                    let mut headers = HeaderMap::new();
                    let header_key = HeaderName::from_str(&key).unwrap();
                    let header_val = HeaderValue::from_str(&val).unwrap();
                    headers.insert(header_key, header_val);
                    client = client.default_headers(headers);
                }
                DownloadModifiers::Useragent(useragent) => {
                    client = client.user_agent(useragent);
                }
                DownloadModifiers::Timeout(timeout) => {
                    client = client.timeout(timeout.unwrap_or(Duration::from_secs(0)));
                }
                DownloadModifiers::Cookie((url, cookie)) => {
                    let jar = reqwest::cookie::Jar::default();
                    jar.add_cookie_str(&cookie, &url.parse::<Url>().unwrap());
                    client = client.cookie_provider(jar.into());
                }
            }
        }
        client
    }

    pub(in crate::web) fn client_create(
        modifers: Vec<TargetModifier>,
        is_text_download: bool,
    ) -> Client {
        let useragent = "RustHydrus V1.0".to_string();

        loop {
            // The client that does the downloading
            let mut client = reqwest::ClientBuilder::new()
                // Keep a small number of idle keep-alives per host. Clients are
                // now shared per-plugin, so this is no longer multiplied by the
                // number of concurrent jobs; a large idle pool caused stale
                // keep-alive resets and connection bursts toward Cloudflare.
                .pool_max_idle_per_host(5)
                .user_agent(&useragent)
                .cookie_store(false)
                .gzip(true)
                .deflate(true)
                .connect_timeout(std::time::Duration::from_secs(15))
                .timeout(std::time::Duration::from_mins(2));

            client = Self::process_modifiers(client, modifers.clone(), is_text_download);

            match client.build() {
                Ok(out) => {
                    return out;
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    fn prepare_request(
        &self,
        method: reqwest::Method,
        input: &shared_types::Url,
        default_target: ModifierTarget,
    ) -> RequestBuilder {
        // 1. Determine target: Use the first modifier's target, or fallback to the default
        let target_type = input
            .local_modifiers
            .first()
            .map_or(default_target, |m| m.target.clone());

        // 2. Initialize the GET request builder from the appropriate client
        let mut request_builder = match target_type {
            ModifierTarget::Text => self.text_client.request(method, &input.url),
            ModifierTarget::Media => self.file_client.request(method, &input.url),
        };

        // 3. Iterate and apply modifiers matching the determined target
        for target_modifier in &input.local_modifiers {
            if target_modifier.target == target_type {
                request_builder = match &target_modifier.modifier {
                    DownloadModifiers::Useragent(ua) => {
                        request_builder.header(reqwest::header::USER_AGENT, ua)
                    }
                    DownloadModifiers::Timeout(Some(duration)) => {
                        request_builder.timeout(*duration)
                    }
                    DownloadModifiers::Timeout(None) => request_builder,
                    DownloadModifiers::Header((key, value)) => {
                        if let (Ok(h_name), Ok(h_val)) = (
                            HeaderName::from_bytes(key.as_bytes()),
                            HeaderValue::from_str(value),
                        ) {
                            request_builder.header(h_name, h_val)
                        } else {
                            request_builder
                        }
                    }
                    DownloadModifiers::Cookie((_, cookie)) => {
                        request_builder.header(reqwest::header::COOKIE, cookie)
                    }
                };
            }
        }

        request_builder
    }
    ///
    /// Downloads text into the client
    ///
    pub(in crate::web) async fn dltext(
        &self,
        input_url: ScraperParam,
        priority: u64,
    ) -> TextDownload {
        let url;
        let post_data;
        let mut cnt = 0;
        let method;

        match input_url {
            ScraperParam::Url(out) => {
                url = out;
                post_data = None;
                method = reqwest::Method::GET;
            }
            ScraperParam::UrlPost(url_post) => {
                url = shared_types::Url {
                    url: url_post.url,
                    local_modifiers: url_post.local_modifiers,
                };
                post_data = Some(url_post.post_data);
                method = reqwest::Method::POST;
            }
            _ => {
                return TextDownload::Skipped;
            }
        }

        let url_parsed = match Url::parse(&url.url) {
            Ok(out) => out,
            Err(_e) => {
                log::error!("ScraperDownloading: {} is not a valid URL.", url.url);
                return TextDownload::Failed;
            }
        };

        loop {
            // Waits to respect ratelimiter
            self.ratelimiter.wait(priority).await;
            info!(
                "Worker: {} JobId: {} -- Spawned web reach to: {}",
                self.plugin.name, self.job.id, url_parsed
            );

            let request = self.prepare_request(method.clone(), &url, ModifierTarget::Text);

            let futureresult = match post_data {
                None => request.send(),
                Some(ref post_data_string) => {
                    let request = request.body(post_data_string.clone());

                    request.send()
                }
            }
            .await;

            let mut dead_url = false;

            match futureresult {
                Ok(res) => {
                    if let Err(err) = res.error_for_status_ref() {
                        let status = err.status();

                        if status == Some(StatusCode::NOT_FOUND) {
                            // On a paginated listing a 404 means "there is nothing
                            // past this offset", not "try again". It still gets a
                            // couple of attempts in case the site is flapping, but
                            // once they are spent the job is over.
                            dead_url = true;
                            log::warn!(
                                "Worker: {} JobId: {} -- While processing job {:?} was unable to download text. Had a 404 error. (attempt {}/{})",
                                self.plugin.name,
                                self.job.id,
                                url_parsed,
                                cnt + 1,
                                MAX_URL_RETRY
                            );
                        } else if err.is_timeout() {
                            log::warn!(
                                "Worker: {} JobId: {} -- While processing job {:?} timed out. (attempt {}/{})",
                                self.plugin.name,
                                self.job.id,
                                url_parsed,
                                cnt + 1,
                                MAX_URL_RETRY
                            );
                        } else if let Some(status) = status
                            && is_retryable_status(status)
                        {
                            log::warn!(
                                "Worker: {} JobId: {} -- While processing job {:?} had retryable status {}. (attempt {}/{})",
                                self.plugin.name,
                                self.job.id,
                                url_parsed,
                                status,
                                cnt + 1,
                                MAX_URL_RETRY
                            );
                        } else {
                            // Not something a retry can fix (403/401/410...).
                            log::error!(
                                "Worker: {} JobId: {} -- While processing job {:?} had a permanent error {:?}",
                                self.plugin.name,
                                self.job.id,
                                url_parsed,
                                err
                            );
                            return TextDownload::Failed;
                        }
                    } else {
                        match res.text().await {
                            Ok(text) => {
                                return TextDownload::Success(
                                    text,
                                    url_parsed.as_str().to_string(),
                                );
                            }
                            Err(err) => {
                                log::error!(
                                    "Worker: {} JobId: {} -- While processing job {:?} had some error {:?}",
                                    self.plugin.name,
                                    self.job.id,
                                    url_parsed,
                                    err
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "Worker: {} JobId: {} -- While processing job {:?} was unable to download text. Had err {:?} ",
                        self.plugin.name,
                        self.job.id,
                        url_parsed,
                        err,
                    );
                }
            }

            // Attempts are spent. Hand the decision back to the caller instead of
            // silently moving on to the job's next url: a url that dies this many
            // times in a row is the end of the listing, and every url after it in
            // the job would be the same dead end.
            if cnt + 1 >= MAX_URL_RETRY {
                return if dead_url {
                    TextDownload::NotFound
                } else {
                    TextDownload::Failed
                };
            }

            cnt += 1;

            // Back off before retrying. Every failure path funnels through here,
            // so a throttling or degraded site is never hit with back-to-back
            // requests.
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
