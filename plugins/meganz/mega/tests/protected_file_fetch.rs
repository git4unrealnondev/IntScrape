//!
//! Integration test for fetching nodes from password-protected MEGA URLs.
//!

use std::env;

#[tokio::test]
#[ignore = "live MEGA API test: requires MEGA_EMAIL/MEGA_PASSWORD (and MEGA_PUBLIC_URL where applicable); run with --ignored"]
async fn protected_url_fetch_test() {
    let protected_url =
        env::var("MEGA_PROTECTED_URL").expect("missing MEGA_PROTECTED_URL environment variable");
    let password = env::var("MEGA_PROTECTED_PASSWORD")
        .expect("missing MEGA_PROTECTED_PASSWORD environment variable");

    let http_client = reqwest::Client::new();
    let mega = mega::Client::builder().build(http_client).unwrap();

    let _nodes = mega
        .fetch_protected_nodes(&protected_url, &password)
        .await
        .expect("could not fetch nodes from protected URL");
}
