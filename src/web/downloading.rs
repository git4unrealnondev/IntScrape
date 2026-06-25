use std::{str::FromStr, time::Duration};

use log::info;
use reqwest::{
    Client, ClientBuilder,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use shared_types::*;
use url::Url;

use crate::web::manager::Scraper;

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
                .pool_max_idle_per_host(100)
                .user_agent(&useragent)
                .cookie_store(false)
                .gzip(true)
                .deflate(true)
                .connect_timeout(std::time::Duration::from_secs(15))
                .timeout(std::time::Duration::from_secs(120));

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

    ///
    /// Downloads text into the client
    ///
    pub(in crate::web) async fn dltext(&self, input_url: ScraperParam) -> Option<(String, String)> {
        let url;
        let post_data;
        let mut cnt = 0;

        match input_url {
            ScraperParam::Url(out) => {
                url = out;
                post_data = None;
            }
            ScraperParam::UrlPost(url_post) => {
                url = url_post.url;
                post_data = Some(url_post.post_data);
            }
            _ => {
                return None;
            }
        }

        let url = match Url::parse(&url) {
            Ok(out) => out,
            Err(_e) => {
                log::error!("ScraperDownloading: {url} is not a valid URL.");
                return None;
            }
        };
        loop {
            // Waits to respect ratelimiter
            self.ratelimiter.until_ready().await;
            info!(
                "Worker: {} JobId: {} -- Spawned web reach to: {}",
                self.plugin.name,
                self.job.id,
                &url.to_string()
            );

            let futureresult = match post_data {
                None => self
                    .text_client
                    .get(url.clone())
                    .header("Accept", "application/json")
                    .send(),
                Some(ref post_data_string) => self
                    .text_client
                    .post(url.clone())
                    .body(post_data_string.clone())
                    .send(),
            }
            .await;

            match futureresult {
                Ok(res) => {
                    if let Err(err) = res.error_for_status_ref() {
                        if err.is_timeout() {
                            let time_secs = 5;
                            tokio::time::sleep(std::time::Duration::from_secs(time_secs)).await;
                            log::error!(
                                "Worker: {} JobId: {} -- While processing job {:?} was unable to download text. Had err {:?} sleeping for {} seconds.",
                                &self.plugin.name,
                                &self.job.id,
                                &url,
                                err,
                                time_secs
                            );

                            cnt += 1;
                            continue;
                        }
                    } else {
                        match res.text().await {
                            Ok(text) => {
                                return Some((text, url.as_str().to_string()));
                            }
                            Err(err) => {
                                log::error!(
                                    "Worker: {} JobId: {} -- While processing job {:?} had some error {:?}",
                                    &self.plugin.name,
                                    &self.job.id,
                                    &url,
                                    err
                                );
                                cnt += 1;
                                continue;
                            }
                        }
                    }
                }
                Err(err) => {
                    if err.is_timeout() {
                        let time_secs = 5;
                        tokio::time::sleep(std::time::Duration::from_secs(time_secs)).await;
                        log::error!(
                            "Worker: {} JobId: {} -- While processing job {:?} was unable to download text. Had err {:?} sleeping for {} seconds.",
                            &self.plugin.name,
                            &self.job.id,
                            &url,
                            err,
                            time_secs
                        );

                        cnt += 1;
                        continue;
                    } else {
                        log::error!(
                            "Worker: {} JobId: {} -- While processing job {:?} was unable to download text. Had err {:?} ",
                            &self.plugin.name,
                            &self.job.id,
                            &url,
                            err,
                        );
                    }
                }
            }

            if cnt >= 3 {
                break;
            }

            cnt += 1;
        }
        None
    }
}
