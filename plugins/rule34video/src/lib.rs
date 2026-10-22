use std::{collections::HashSet, fmt, time::Duration};

use json::JsonValue;
use scraper::{Html, Selector};
use shared_types::{
    FileObject, FileSource, FileTagAction, GenericNamespaceObj, PluginJob, PluginProperties,
    PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf, Tag,
    TagOperation, TargetModifier, Url,
};
pub enum Site {
    Rule34Video,
}

pub enum NsIdent {
    PostId,
    Parent,
    Rating,
    General,
    Artist,
    Character,
    Metadata,
    Invalid,
    Timestamp,
    Sources,
    Species,
    PoolPosition,
    PoolTitle,
}

impl fmt::Display for Site {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Site::Rule34Video => "Rule34Video.com",
        };
        write!(f, "{s}")
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "rule34video".into(),
        properties: vec![
            PluginProperties::Modifier(TargetModifier {
                target: shared_types::ModifierTarget::Media,
                modifier: shared_types::DownloadModifiers::Timeout(Some(Duration::from_mins(300))),
            }),
            PluginProperties::Ratelimit(2, std::time::Duration::from_secs(1)),
            PluginProperties::JobNum(5),
            PluginProperties::Sites(vec!["rule34video.com".into(), "rule34video".into()]),
            PluginProperties::ThreadNum(2),
        ],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();

    let mut params = scraperdata.job.param.clone();
    params.retain(|f| matches!(f, ScraperParam::Normal(_)));

    if !params.is_empty()
        && let Some(url) = build_url(&scraperdata.job.param, 1)
    {
        let mut param = params.clone();
        param.push(ScraperParam::Url(Url {
            url,
            ..Default::default()
        }));
        out.push(ScraperDataReturn {
            job: shared_types::PluginJob {
                site: scraperdata.job.site.clone(),
                priority: shared_types::DEFAULT_PRIORITY - 2,
                param,
                ..Default::default()
            },
            ..Default::default()
        });
    }

    // Handles URL passthrough
    for param in scraperdata.job.param.clone() {
        if let ScraperParam::Url(url) = param {
            let mut param = params.clone();
            param.push(ScraperParam::Url(url));
            out.push(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: shared_types::DEFAULT_PRIORITY - 2,
                    param,
                    user_data: scraperdata.job.user_data.clone(),
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    }

    out
}

#[unsafe(no_mangle)]
fn parser_call(
    text_input: &str,
    source_url: &str,
    scraperdata: &ScraperDataReturn,
) -> Vec<ScraperReturn> {
    let recursion = scraperdata
        .job
        .user_data
        .get("recursion")
        .is_none_or(|value| value != "false");

    let document = Html::parse_document(text_input);

    let mut files = HashSet::new();
    let mut jobs = HashSet::new();
    let tags = HashSet::new();

    let video_page = is_video_page(source_url);

    if video_page {
        if let Some(file) = parse_video_page(&document, source_url) {
            files.insert(file);
        } else if !scraperdata
            .job
            .user_data
            .contains_key("INTSCRAPE_REDOWNLOAD")
        {
            // The page rendered but offered no media url — a site anomaly
            // (markup drift, embed-only page), not an exhausted listing. Stop
            // instead of Nothing so the job is kept for the next boot instead
            // of being silently dropped; the url is in the message because the
            // manager logs it without context.
            //
            // Recovery jobs carry the video page url with empty text, so a
            // media miss there is structural, not a site anomaly: retrying is
            // pointless, so they keep the old dropped-on-Nothing behaviour.
            return vec![ScraperReturn::Stop(format!(
                "video page yielded no media url: {source_url}"
            ))];
        }
    } else if recursion {
        add_video_jobs(&document, source_url, scraperdata, &mut jobs);

        // Chained pagination: a listing page that held videos spawns its next
        // page immediately, so a search walks one job per page it fetches and
        // ends on the first empty page or the past-the-end 404, instead of
        // pre-enumerating pages up front.
        if !jobs.is_empty()
            && let Some(next_page) = listing_pagination_job(scraperdata, source_url)
        {
            jobs.insert(next_page);
        }
    }

    if files.is_empty() && jobs.is_empty() && tags.is_empty() {
        return vec![ScraperReturn::Nothing];
    }

    vec![ScraperReturn::Data(shared_types::ScraperObject {
        files,
        jobs,
        tags,
    })]
}

fn build_url(params: &[shared_types::ScraperParam], page_num: u64) -> Option<String> {
    if params.is_empty() {
        return None;
    }

    let tags: Vec<&str> = params
        .iter()
        .filter_map(|f| {
            if let ScraperParam::Normal(normal) = f {
                Some(normal.as_str())
            } else {
                None
            }
        })
        .collect();

    if tags.is_empty() {
        return None;
    }

    // Set maximum documented limit of 1000 items per call for structural efficiency
    let url = format!(
        "https://rule34video.com/search/{}/?mode=async&function=get_block&block_id=custom_list_videos_videos_list_search&q={}&from_videos={page_num}",
        tags.join("-"),
        tags.join("+"),
    );

    Some(url)
}

/// Builds the listing job that continues the search this page belongs to.
///
/// rule34video paginates its listing block with a `from_videos=<n>` marker in
/// the query string, and the site answers one page past the end with 404.
/// Pagination is chained by the parser — each non-empty listing page spawns
/// its next page — so a search allocates one listing job per page it actually
/// fetches and terminates on the first page with no videos or the trailing
/// 404, instead of `url_dump` pre-enumerating page jobs up front.
///
/// The page number is rewritten in place, keeping every other query element
/// exactly as the previous page had it. A url without the marker (a listing
/// pasted straight into the job's urls) still parses as before and simply
/// does not chain. Chaining is only driven while the `recursion` user flag is
/// unset or `true`; with recursion disabled the walk is the single first page,
/// which matches what the walk produced before the chain.
fn listing_pagination_job(
    scraperdata: &ScraperDataReturn,
    source_url: &str,
) -> Option<ScraperDataReturn> {
    const PAGE_MARKER: &str = "from_videos=";

    let digits_start = source_url.find(PAGE_MARKER)? + PAGE_MARKER.len();
    let digits = source_url[digits_start..]
        .split(['&', '/'])
        .next()
        .unwrap_or("");
    let page: u64 = digits.parse().ok()?;
    let next = page.checked_add(1)?;
    let next_url = format!(
        "{}{next}{}",
        &source_url[..digits_start],
        &source_url[digits_start + digits.len()..],
    );

    // Same param shape the passthrough branch of `url_dump` produces for a
    // listing url: the search terms plus one listing page to fetch.
    let mut param = scraperdata.job.param.clone();
    param.retain(|f| matches!(f, ScraperParam::Normal(_)));
    param.push(ScraperParam::Url(Url {
        url: next_url,
        ..Default::default()
    }));

    Some(ScraperDataReturn {
        job: shared_types::PluginJob {
            site: scraperdata.job.site.clone(),
            priority: shared_types::DEFAULT_PRIORITY - 2,
            param,
            user_data: scraperdata.job.user_data.clone(),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn is_video_page(url: &str) -> bool {
    let Some(video_pos) = url.find("/video/") else {
        return false;
    };

    let rest = &url[(video_pos + "/video/".len())..];

    rest.split('/')
        .next()
        .is_some_and(|id| id.chars().all(|c| c.is_ascii_digit()))
}

fn add_video_jobs(
    document: &Html,
    source_url: &str,
    scraperdata: &ScraperDataReturn,
    jobs: &mut HashSet<ScraperDataReturn>,
) {
    let selector = Selector::parse("a.th[href]").unwrap();

    for item in document.select(&selector) {
        let Some(href) = item.value().attr("href") else {
            continue;
        };

        let Some(video_url) = absolute_rule34video_url(href, source_url) else {
            continue;
        };

        if !is_video_page(&video_url) {
            continue;
        }

        if video_url == source_url {
            continue;
        }

        let skip_conditions = skip_conditions_for(&video_url);

        jobs.insert(ScraperDataReturn {
            job: PluginJob {
                site: scraperdata.job.site.clone(),
                priority: shared_types::DEFAULT_PRIORITY - 2,
                param: vec![ScraperParam::Url(Url {
                    url: video_url,
                    local_modifiers: Vec::new(),
                })],
                user_data: scraperdata.job.user_data.clone(),
                ..Default::default()
            },
            skip_conditions,
        });
    }
}

/// Skips the job when the database already holds this video.
///
/// `parse_video_page` tags every file it yields with its id and the page it came
/// from, so a hit here means the page was already parsed and the media URL was
/// already handed to the downloader. The id is preferred because it survives
/// trailing-slash and host spelling drift that the raw page url does not.
fn skip_conditions_for(video_url: &str) -> Vec<SkipIf> {
    let tag = match extract_video_id(video_url) {
        Some(id) => Tag {
            name: id,
            namespace: namespace("Id"),
        },
        None => Tag {
            name: video_url.to_owned(),
            namespace: namespace("Source"),
        },
    };

    vec![SkipIf::FileTagRelationship(tag)]
}

fn duration_to_seconds(value: &str) -> Option<u64> {
    let value = value.strip_prefix("PT")?;

    let mut total_seconds = 0u64;
    let mut number = String::new();

    for character in value.chars() {
        if character.is_ascii_digit() {
            number.push(character);
            continue;
        }

        let amount: u64 = number.parse().ok()?;
        number.clear();

        match character {
            'H' => total_seconds += amount * 60 * 60,
            'M' => total_seconds += amount * 60,
            'S' => total_seconds += amount,
            _ => return None,
        }
    }

    if !number.is_empty() {
        return None;
    }

    Some(total_seconds)
}
fn extract_member_id(url: &str) -> Option<String> {
    let id = url.trim_end_matches('/').rsplit('/').next()?;

    id.parse::<u64>().ok().map(|_| id.to_owned())
}
fn parse_video_page(document: &Html, source_url: &str) -> Option<FileObject> {
    let media_url = find_best_video_url(document, source_url)?;

    let mut tag_actions = Vec::new();

    // Extracts the id from the video
    if let Some(video_id) = extract_video_id(source_url) {
        add_tag(&mut tag_actions, namespace("Id"), video_id.to_owned());
    }

    // Adds page to source
    add_tag(&mut tag_actions, namespace("Source"), source_url.to_owned());

    if let Some(json) = extract_video_object(document) {
        if let Some(name) = json["name"].as_str() {
            add_tag(&mut tag_actions, namespace("Title"), name.to_owned());
        }
        if let Some(desc) = json["description"].as_str() {
            add_tag(&mut tag_actions, namespace("Description"), desc.to_owned());
        }
        if let Some(date) = json["uploadDate"].as_str() {
            add_tag(&mut tag_actions, namespace("UploadDate"), date.to_owned());
        }
        if let Some(dur) = json["duration"].as_str()
            && let Some(duration) = duration_to_seconds(dur)
        {
            add_tag(
                &mut tag_actions,
                namespace("Duration"),
                duration.to_string(),
            );
        }
    }

    // General Categories
    let category_selector =
        Selector::parse(r#"a.video_meta_pill[href*="/categories/"] > span"#).unwrap();

    for element in document.select(&category_selector) {
        let category = element
            .text()
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        if category.is_empty() {
            continue;
        }

        add_tag(&mut tag_actions, namespace("Category"), category);
    }

    // Tags
    let tag_selector =
        Selector::parse(r#"span[data-item-type="tag"] > a.tag_item[href*="/tags/"]"#).unwrap();

    for element in document.select(&tag_selector) {
        let tag_name = element
            .text()
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        if tag_name.is_empty() {
            continue;
        }

        if let Some(series) = split_character_series(&tag_name) {
            add_character_tag(&mut tag_actions, tag_name, series);
        } else {
            add_tag(&mut tag_actions, namespace("General"), tag_name);
        }
    }

    // Adds pending tags
    let pending_tag_selector = Selector::parse(
        r#"span.tag_suggestion_chip[data-status="pending"][data-item-type="tag"] > span.tag_item"#,
    )
    .unwrap();

    for element in document.select(&pending_tag_selector) {
        let tag_name = element
            .text()
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        if tag_name.is_empty() {
            continue;
        }

        add_tag(&mut tag_actions, namespace("PendingTag"), tag_name);
    }

    // Uploader
    let uploader_selector = Selector::parse(r#"a.video_meta_pill[href*="/members/"]"#).unwrap();

    for element in document.select(&uploader_selector) {
        let uploader = element
            .text()
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        if uploader.is_empty() {
            continue;
        }

        add_tag(&mut tag_actions, namespace("Uploader"), uploader);

        if let Some(member_id) = element.value().attr("href").and_then(extract_member_id) {
            add_tag(&mut tag_actions, namespace("UploaderId"), member_id);
        }
    }
    // Artists
    let artist_selector = Selector::parse(r#"a.video_meta_pill[href*="/models/"]"#).unwrap();

    for element in document.select(&artist_selector) {
        let artist = element
            .text()
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        if artist.is_empty() {
            continue;
        }

        add_tag(&mut tag_actions, namespace("Artist"), artist);

        if let Some(artist_id) = element.value().attr("href").and_then(extract_member_id) {
            add_tag(&mut tag_actions, namespace("ArtistId"), artist_id);
        }
    }

    Some(FileObject {
        source: Some(FileSource::Url(media_url)),
        tag_list: tag_actions,
        ..Default::default()
    })
}

fn find_best_video_url(document: &Html, source_url: &str) -> Option<String> {
    let download_selector = Selector::parse("a.tag_item_download[href]").unwrap();

    let mut candidates = Vec::new();

    for element in document.select(&download_selector) {
        let Some(href) = element.value().attr("href") else {
            continue;
        };

        if !href.to_ascii_lowercase().contains(".mp4") {
            continue;
        }

        let Some(url) = absolute_rule34video_url(href, source_url) else {
            continue;
        };

        let text = element.text().collect::<String>();
        let quality = quality_score(&text, &url);

        candidates.push((quality, url));
    }

    /*
     * Fallback to the <video src="..."> element if download links
     * are not present.
     */
    if candidates.is_empty() {
        let video_selector = Selector::parse("video[src], video source[src]").unwrap();

        for element in document.select(&video_selector) {
            let Some(src) = element.value().attr("src") else {
                continue;
            };

            if !src.to_ascii_lowercase().contains(".mp4") {
                continue;
            }

            let url = absolute_rule34video_url(src, source_url)?;
            candidates.push((quality_score(src, &url), url));
        }
    }

    candidates
        .into_iter()
        .max_by_key(|(quality, _)| *quality)
        .map(|(_, url)| url)
}

fn quality_score(label: &str, url: &str) -> u32 {
    let combined = format!(
        "{} {}",
        label.to_ascii_lowercase(),
        url.to_ascii_lowercase()
    );

    if combined.contains("2160") || combined.contains("4k") {
        2160
    } else if combined.contains("1440") {
        1440
    } else if combined.contains("1080") {
        1080
    } else if combined.contains("720") {
        720
    } else if combined.contains("480") {
        480
    } else if combined.contains("360") {
        360
    } else {
        /*
         * An unknown quality gets a low score but remains usable.
         */
        1
    }
}

fn absolute_rule34video_url(value: &str, source_url: &str) -> Option<String> {
    let value = value.trim();

    if value.is_empty() {
        return None;
    }

    if value.starts_with("https://") || value.starts_with("http://") {
        return Some(value.to_owned());
    }

    if value.starts_with("//") {
        return Some(format!("https:{value}"));
    }

    if value.starts_with('/') {
        return Some(format!("https://rule34video.com{value}"));
    }

    if value.starts_with("video/") {
        return Some(format!("https://rule34video.com/{value}"));
    }

    /*
     * This handles relative download links if the page ever emits them.
     */
    let origin = source_url
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map(|(host, _)| host)
        .unwrap_or("rule34video.com");

    Some(format!("https://{origin}/{value}"))
}

fn extract_video_id(url: &str) -> Option<String> {
    let video_pos = url.find("/video/")?;
    let rest = &url[(video_pos + "/video/".len())..];

    let id = rest.split('/').next()?;

    if id.chars().all(|character| character.is_ascii_digit()) {
        Some(id.to_owned())
    } else {
        None
    }
}
fn extract_video_object(document: &Html) -> Option<JsonValue> {
    let selector = Selector::parse(r#"script[type="application/ld+json"]"#).ok()?;

    for script in document.select(&selector) {
        let json_text = script.text().collect::<String>();

        if let Ok(value) = json::parse(&json_text) {
            return Some(value);
        }
    }

    None
}

fn namespace(name: &str) -> GenericNamespaceObj {
    GenericNamespaceObj {
        name: format!("Rule34Video_{name}"),
        description: Some(format!("Rule34Video.com {name} metadata.")),
    }
}

fn add_tag(actions: &mut Vec<FileTagAction>, namespace: GenericNamespaceObj, value: String) {
    if value.trim().is_empty() {
        return;
    }

    actions.push(FileTagAction {
        operation: TagOperation::Add,
        tags: vec![PluginTag {
            tag: Tag {
                namespace,
                name: value,
            },
            ..Default::default()
        }],
    });
}

/// Reports the series a site tag belongs to when the tag is in the site's
/// `character (series)` shape — `naruto uzumaki (naruto)`,
/// `loona (helluva boss)`.
///
/// On <https://rule34video.com/tags/> every named character lives in the
/// "Characters" tag group and carries a parenthesised series qualifier, while
/// the other groups ("Body & features", "Positions & acts", "Moments",
/// "Camera", "Motion", "Sizes", "Style & genre", "Categories", "Other") do not
/// use qualifiers. The single exception observed across those groups is the
/// meta tag `tagme (character)`, which is a request for more character
/// tagging and not a character, so it stays in `Rule34Video_General`. Chips
/// without the shape — such as the few unqualified "Characters" group words
/// like "vtuber" — stay in `Rule34Video_General` too.
///
/// The qualifier is read from the last parenthesis group: a chip may carry an
/// extra group name, as in `hina (dress) (blue archive)`, and its series is
/// `blue archive`.
fn split_character_series(tag_name: &str) -> Option<String> {
    let paren = tag_name.rfind('(')?;
    let name = tag_name[..paren].trim();
    if name.is_empty() || name.eq_ignore_ascii_case("tagme") {
        return None;
    }

    let qualifier = tag_name[(paren + 1)..].strip_suffix(')')?.trim();
    if qualifier.is_empty() {
        return None;
    }

    Some(qualifier.to_owned())
}

/// Adds a character tag linked to the series it belongs to.
///
/// The tag keeps its full site spelling — `hinata hyuuga (naruto)` — under
/// `Rule34Video_Character`, and the series lands in `Rule34Video_Category`,
/// the namespace that already holds the page's `/categories/` pills, through a
/// parent relation: the character's category is its series. When the page also
/// lists that series as a pill, both refer to the same category tag; when it
/// does not, the relation creates the category tag, since the db side inserts
/// every tag referenced by a relation.
fn add_character_tag(actions: &mut Vec<FileTagAction>, name: String, series: String) {
    actions.push(FileTagAction {
        operation: TagOperation::Add,
        tags: vec![PluginTag {
            tag: Tag {
                namespace: namespace("Character"),
                name,
            },
            relates_to: Some(RelationContext {
                tag: Tag {
                    namespace: namespace("Category"),
                    name: series,
                },
                ..Default::default()
            }),
            ..Default::default()
        }],
    });
}
