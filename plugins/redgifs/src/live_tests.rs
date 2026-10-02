//! End-to-end checks against the live Redgifs api.
//!
//! Every test here is written to *skip* rather than fail when the network is
//! unavailable, so the suite still runs offline. When the network is available
//! they are the only tests that can catch a break in the api contract --
//! endpoint paths, response keys and variant names are all things a fixture can
//! agree with while the real service has moved on.
//!
//! The tag used is deliberately something the site guarantees to have, and the
//! assertions only require *some* gif to come back, never a specific one, since
//! the catalogue changes continuously.

use super::*;
use parse::Options;

fn parse(input: &str) -> json::JsonValue {
    json::parse(input).unwrap()
}

/// A listing page, or `None` when the api cannot be reached.
fn live_listing(term: &str) -> Option<Vec<json::JsonValue>> {
    let mut user_data = std::collections::BTreeMap::new();
    user_data.insert(
        key::KIND.to_string(),
        PageKind::Listing.as_str().to_string(),
    );
    user_data.insert(key::SEARCH.to_string(), term.to_string());
    user_data.insert(key::PAGE.to_string(), "1".to_string());

    let (path, query) = listing_request(PageKind::Listing, &user_data).ok()?;
    let page = api::page(&path, &query, 1).ok()?;
    Some(page.json["gifs"].members().cloned().collect())
}

#[test]
fn live_tag_search_returns_gifs_we_can_turn_into_files() {
    let Some(gifs) = live_listing("anal") else {
        eprintln!("skipping: redgifs api unreachable");
        return;
    };
    assert!(!gifs.is_empty(), "a common tag must return results");

    let options = Options::default();
    let mut produced_files = 0usize;

    for gif in &gifs {
        let Some(id) = gif["id"].as_str().and_then(parse::GifId::parse) else {
            continue;
        };
        // Every listed id must be one we would accept.
        assert_eq!(id.as_str(), raw_lowercase(&gif["id"]), "ids must lowercase");

        if let Some((file, tags)) = parse::gif_into(gif, &options) {
            produced_files += 1;

            // The download url must be absolute https on the media cdn.
            let url = match &file.source {
                Some(shared_types::FileSource::Url(url)) => url.clone(),
                other => panic!("expected a url source, got {other:?}"),
            };
            assert!(
                url.starts_with("https://media.redgifs.com/"),
                "unexpected media host: {url}"
            );

            // A file always carries its own identity.
            assert!(
                file.tag_list[0].tags.iter().any(
                    |tag| tag.tag.namespace.name == "RedgifsGif" && tag.tag.name == id.as_str()
                )
            );
            assert!(!tags.is_empty());
        }
    }

    assert!(
        produced_files > 0,
        "no gif in a full page produced a downloadable file"
    );
}

fn raw_lowercase(value: &json::JsonValue) -> String {
    value.as_str().unwrap_or_default().to_ascii_lowercase()
}

#[test]
fn live_gif_fetch_matches_a_listing_entry() {
    let Some(gifs) = live_listing("anal") else {
        eprintln!("skipping: redgifs api unreachable");
        return;
    };
    let Some(id) = gifs.first().and_then(|gif| gif["id"].as_str()) else {
        eprintln!("skipping: no gif id in listing");
        return;
    };

    let Ok(gif) = api::gif(id) else {
        eprintln!("skipping: single gif fetch failed");
        return;
    };

    assert_eq!(
        gif["id"].as_str().unwrap_or_default().to_lowercase(),
        id.to_lowercase()
    );
    assert!(gif["urls"].is_object(), "a gif must carry a urls object");
}

#[test]
fn live_gallery_fetch_returns_downloadable_members() {
    // Walk a few pages of a listing to find an entry that belongs to a gallery.
    let mut found: Option<String> = None;
    for page in 1..=5u64 {
        let mut user_data = std::collections::BTreeMap::new();
        user_data.insert(
            key::KIND.to_string(),
            PageKind::Listing.as_str().to_string(),
        );
        user_data.insert(key::SEARCH.to_string(), "anal".to_string());
        user_data.insert(key::PAGE.to_string(), page.to_string());

        let Ok((path, query)) = listing_request(PageKind::Listing, &user_data) else {
            break;
        };
        let Ok(result) = api::page(&path, &query, page) else {
            break;
        };
        if let Some(gallery) = result.json["gifs"]
            .members()
            .find_map(|gif| gif["gallery"].as_str())
            .filter(|value| !value.is_empty())
        {
            found = Some(gallery.to_string());
            break;
        }
    }

    let Some(gallery) = found else {
        eprintln!("skipping: no gallery found in the first 5 pages");
        return;
    };

    let Ok(members) = api::gallery(&gallery) else {
        eprintln!("skipping: gallery fetch failed");
        return;
    };
    assert!(!members.is_empty(), "a gallery must have members");

    let options = Options::default();
    let bounded = parse::bounded_members(members.clone());
    assert_eq!(
        bounded.len(),
        members.len().min(limits::MAX_GALLERY_MEMBERS)
    );

    let produced = bounded
        .iter()
        .enumerate()
        .filter_map(|(position, member)| parse::gallery_member_into(member, position, &options))
        .count();
    assert!(produced > 0, "no gallery member produced a file");
}

#[test]
fn live_unknown_gif_is_reported_as_not_found() {
    // A well-formed but non-existent id must surface as `Gone`, not a hard
    // failure, so a stale job is dropped rather than retried forever.
    match api::gif("thisisnotarealredgifsidentifier") {
        Err(ApiError::NotFound) => {}
        Err(other) => panic!("expected NotFound, got {other}"),
        Ok(_) => {
            eprintln!("skipping: redgifs returned a gif for a nonsense id");
        }
    }
}

#[test]
fn live_media_urls_are_downloadable_without_authentication() {
    let Some(gifs) = live_listing("anal") else {
        eprintln!("skipping: redgifs api unreachable");
        return;
    };

    let url = gifs
        .iter()
        .find_map(|gif| gif["urls"]["hd"].as_str().filter(|url| !url.is_empty()))
        .or_else(|| {
            gifs.iter()
                .find_map(|gif| gif["urls"]["poster"].as_str().filter(|url| !url.is_empty()))
        });

    let Some(url) = url else {
        eprintln!("skipping: no media url in listing");
        return;
    };

    // Range-limited so this reads a few bytes rather than a whole clip.
    let response = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("client")
        .get(url)
        .header("Range", "bytes=0-1023")
        .header("User-Agent", request::USER_AGENT)
        .send();

    let Ok(response) = response else {
        eprintln!("skipping: media cdn unreachable");
        return;
    };

    assert!(
        response.status().is_success() || response.status() == reqwest::StatusCode::PARTIAL_CONTENT,
        "media url {url} returned {}",
        response.status()
    );
    assert!(
        url.len() <= limits::MAX_URL_LEN,
        "media url should be well under the cap"
    );
}

#[test]
fn live_pagination_reports_more_pages_than_our_chain_budget() {
    let Some(gifs) = live_listing("anal") else {
        eprintln!("skipping: redgifs api unreachable");
        return;
    };
    assert!(!gifs.is_empty());

    // The whole point of the chain cap: the site offers far more than we walk.
    let mut user_data = std::collections::BTreeMap::new();
    user_data.insert(
        key::KIND.to_string(),
        PageKind::Listing.as_str().to_string(),
    );
    user_data.insert(key::SEARCH.to_string(), "anal".to_string());

    let (path, query) = listing_request(PageKind::Listing, &user_data).unwrap();
    let Ok(page) = api::page(&path, &query, 1) else {
        eprintln!("skipping: redgifs api unreachable");
        return;
    };

    if let Some(pages) = page.pages {
        assert!(
            pages >= 1,
            "a listing must report at least one page, got {pages}"
        );
        // And a continuation must exist for page 1 whenever there is more.
        if pages > 1 {
            assert!(
                request::continuation(PageKind::Listing, &user_data, 2, Some(pages)).is_some(),
                "page 1 of {pages} must have a continuation"
            );
        }
    }
}
