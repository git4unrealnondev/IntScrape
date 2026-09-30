//! Scraper for [rokuhentai.com](https://rokuhentai.com).
//!
//! The site serves three page shapes and this plugin parses all of them:
//!
//! * **info** — `https://rokuhentai.com/<id>`. The manga landing page: title, page
//!   count, publish date, cover, the whole tag list, and one card per page in the
//!   gallery.
//! * **reader** — `https://rokuhentai.com/<id>/<N>`. Despite the `/<N>` in the
//!   path, this page carries *every* full-size page image for the manga, so one
//!   fetch of page 0 enumerates the entire gallery. It is deliberately not one
//!   request per image.
//! * **listing** — `https://rokuhentai.com/` and `https://rokuhentai.com/?q=<tag>`.
//!   Cursor paginated; the next page is an opaque `?p=<token>` "NEXT" link.
//!
//! Tags and page positions are recorded against the manga they belong to, so any
//! position resolves back to the site root URL it came from.
//!
//! Images are hotlink protected: they answer `403` unless the request carries a
//! `Referer` on the site, which [`get_plugin_info`] installs for media downloads.

use std::collections::HashSet;

use chrono::NaiveDate;
use scraper::{ElementRef, Html, Selector};
use shared_types::{
    DEFAULT_PRIORITY, DownloadModifiers, FileObject, FileSource, FileTagAction,
    GenericNamespaceObj, ModifierTarget, PluginProperties, PluginTag, RelationContext,
    ScraperDataReturn, ScraperParam, ScraperReturn, Tag, TagOperation, TargetModifier, Url,
};

/// Host, without a scheme. Used for the `Sites` list and the media `Referer`.
const SITE_ROOT: &str = "rokuhentai.com";

/// Root of the site, for building URLs.
const SITE_URL: &str = "https://rokuhentai.com";

/// Prefix for every namespace this plugin owns, matching the `CivitX` convention.
const NS: &str = "RokuHentai";

// ---------------------------------------------------------------------------
// parsed page shapes
// ---------------------------------------------------------------------------

/// A site tag, split into its namespace and value.
///
/// The site writes these as `namespace: value`, quoting multi-word values, so
/// `parody: "pokemon | pocket monsters"` is the `parody` namespace with the value
/// `pokemon | pocket monsters`.
#[derive(Debug)]
struct SiteTag {
    namespace: String,
    value: String,
    /// The site's own search URL for this tag, rooted at [`SITE_URL`].
    search_url: String,
}

/// A gallery page: a position, the reader page that shows it, and the full-size
/// image on it.
struct GalleryPage {
    position: u64,
    reader_url: String,
    image_url: String,
}

/// Everything the info page tells us about one manga.
struct MangaInfo {
    id: String,
    title: Option<String>,
    page_count: Option<u64>,
    cover_url: Option<String>,
    /// Unix seconds of the page's last change, to the minute, read from the date
    /// the site renders. Falls back to the publication day at midnight from the
    /// `after:` bound of the date search link when no time is rendered.
    published: Option<i64>,
    tags: Vec<SiteTag>,
    /// Positions in the gallery, ascending and deduplicated.
    positions: Vec<u64>,
}

/// One card in a listing page.
struct MangaCard {
    id: String,
    title: Option<String>,
    page_count: Option<u64>,
    /// Unix seconds, from the date the card renders. See [`MangaInfo::published`].
    published: Option<i64>,
}

/// The cards of a listing page, and where to go for the next page of results.
struct Listing {
    cards: Vec<MangaCard>,
    /// The opaque `?p=<token>` link to the next page, if there is one.
    next_url: Option<String>,
}

// ---------------------------------------------------------------------------
// plugin registration
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "RokuHentai".into(),
        properties: vec![
            PluginProperties::Ratelimit(2, std::time::Duration::from_secs(1)),
            PluginProperties::ThreadNum(4),
            PluginProperties::JobNum(1),
            PluginProperties::Sites(vec![SITE_ROOT.into(), "rokuhentai".into()]),
            // Images 403 without a `Referer` on the site. Any path under it works,
            // so one static value covers every image in every manga.
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header(("Referer".into(), format!("{SITE_URL}/"))),
            }),PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header(("Accept".into(), "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8".into())),
            }),PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header(("Accept-Language".into(), "en-US,en;q=0.9".into())),
            }),PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header(("Accept-Encoding".into(), "gzip, deflate, br, zstd".into())),
            }),
            PluginProperties::Modifier(TargetModifier { target: ModifierTarget::Media, modifier: DownloadModifiers::Useragent("Mozilla/5.0 (X11; Linux x86_64; rv:156.0) Gecko/20100101 Firefox/156.0".to_string()) })
        ],
        ..Default::default()
    }]
}

// ---------------------------------------------------------------------------
// url building
// ---------------------------------------------------------------------------

/// Builds a listing URL for a set of search terms.
///
/// The site takes one `q` parameter and separates terms with a space, which
/// `form_urlencoded` renders as `+` — the same encoding its own links use.
fn build_search_url(terms: &[&str]) -> Option<String> {
    let cleaned: Vec<&str> = terms
        .iter()
        .map(|term| term.trim())
        .filter(|term| !term.is_empty())
        .collect();

    if cleaned.is_empty() {
        return None;
    }

    let query: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", &cleaned.join(" "))
        .finish();

    Some(format!("{SITE_URL}/?{query}"))
}

/// The site's landing page for a manga.
fn manga_url(id: &str) -> String {
    format!("{SITE_URL}/{id}")
}

/// The reader page for one gallery position.
fn reader_url(id: &str, position: u64) -> String {
    format!("{SITE_URL}/{id}/{position}")
}

// ---------------------------------------------------------------------------
// selectors
// ---------------------------------------------------------------------------

fn sel(text: &str) -> Selector {
    Selector::parse(text).expect("static CSS selector must parse")
}

/// Splits a URL path into `(manga id, optional position)`.
///
/// Matches `/q1j2hz` and `/q1j2hz/12`, and rejects the site's other routes
/// (`/dmca`, `/?P=`, `/?q=`) by requiring a plausible id.
fn split_manga_path(url: &str) -> Option<(String, Option<u64>)> {
    let path = url::Url::parse(url)
        .ok()?
        .path()
        .trim_matches('/')
        .to_string();

    let mut parts = path.split('/');

    let id = parts.next()?;
    if !is_manga_id(id) {
        return None;
    }

    let position = match parts.next() {
        Some(text) => Some(text.parse::<u64>().ok()?),
        None => None,
    };

    // A third segment means this is some other route that merely looks like ours.
    if parts.next().is_some() {
        return None;
    }

    Some((id.to_string(), position))
}

/// Manga ids are short lowercase alphanumeric handles, e.g. `q1j2hz`.
fn is_manga_id(candidate: &str) -> bool {
    (5..=12).contains(&candidate.len()) && candidate.chars().all(|c| c.is_ascii_alphanumeric())
}

// ---------------------------------------------------------------------------
// parsing: shared bits
// ---------------------------------------------------------------------------

/// Splits `namespace: value` into its two halves, dropping the quotes the site
/// puts around multi-word values.
fn split_tag(raw: &str) -> Option<(&str, &str)> {
    let (namespace, value) = raw.split_once(':')?;

    let namespace = namespace.trim();
    let value = value.trim().trim_matches('"').trim();

    if namespace.is_empty() || value.is_empty() {
        return None;
    }

    Some((namespace, value))
}

/// Text content of an element with runs of whitespace collapsed.
///
/// The site's markup puts tags and titles across several nodes, so the raw text
/// needs normalising before it is used as a tag name.
fn clean_text(element: ElementRef) -> String {
    element
        .text()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
}

/// Reads a `page_count` out of a caption like `347 images . Jul 17, 2025`.
///
/// Requires the `images` word after the number rather than just leading digits, so
/// a title that happens to start with a number cannot be mistaken for a count.
fn parse_page_count(text: &str) -> Option<u64> {
    let mut words = text.split_whitespace();

    while let Some(word) = words.next() {
        let mut digits = word.chars();

        let is_count = !word.is_empty()
            && digits.all(|c| c.is_ascii_digit())
            && matches!(words.clone().next(), Some("images") | Some("image"));

        if is_count {
            return word.parse().ok();
        }
    }

    None
}

/// Reads the publish day out of the `?q=after:... before:...` search link.
///
/// The link is `after:2025-07-17 before:2025-07-18`; the upper bound is exclusive
/// so the lower bound is the publication day. Midnight, because the link carries no
/// time. This is the fallback for [`parse_timestamp`], which is to the minute.
fn parse_published(href: &str) -> Option<i64> {
    let url = url::Url::parse(href).ok()?;
    let query = url
        .query_pairs()
        .find(|(key, _)| key == "q")?
        .1
        .into_owned();

    let after = query
        .split_whitespace()
        .find_map(|term| term.strip_prefix("after:"))?;

    let date = NaiveDate::parse_from_str(after, "%Y-%m-%d").ok()?;

    Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp() - TIMESTAMP_UTC_OFFSET)
}

/// The zone the site's wall-clock timestamps are read in, as seconds east of UTC.
///
/// The site renders its timestamps as bare local time and publishes no offset
/// anywhere: there is no `<time>` element, no date `<meta>`, no JSON-LD, no
/// `sitemap.xml`, and the `Last-Modified` header is just the current clock (it is
/// sent on 404s too). The offset is therefore not recoverable from the site, and it
/// cannot be inferred from the server clock either: sampling the newest card gives
/// different answers minutes apart, because the site publishes a handful of items a
/// day rather than continuously, so "newest item is about now" is false.
///
/// UTC is used because it is the zone the `after:` day bounds are already anchored
/// to, so the precise and day-granularity readings of a manga cannot disagree about
/// which day it falls on. The site does render times *behind* GMT, so if the real
/// zone is ever established this is the one line to change.
const TIMESTAMP_UTC_OFFSET: i64 = 0;

/// Reads a timestamp out of a date the site renders, e.g. `Jul 17, 2025,  4:50 am`.
///
/// The site pads with spaces after the month and after the year comma, so this
/// splits on whitespace rather than matching a fixed layout. The shape it looks for
/// is `month day, year, hour:minute am|pm`, and it is searched for anywhere in the
/// text, because the date shares its element with the image count
/// (`10 images · Jul  8, 2025,  1:36 am`).
fn parse_timestamp(text: &str) -> Option<i64> {
    let words: Vec<&str> = text.split_whitespace().collect();

    // Scan for the month, so the date is found whatever precedes it.
    for (index, word) in words.iter().enumerate() {
        let Some(month) = month_from_name(word) else {
            continue;
        };

        let rest = words.get(index + 1..)?;

        let day = trailing_number(rest.first()?)?;
        let year = trailing_number(rest.get(1)?)?;
        let (hour, minute) = split_clock(rest.get(2)?);

        // The meridiem is a separate word (`4:50 am`), so it is read one past the
        // clock. Without one the clock is already 24 hour.
        let hour = match meridiem_after(rest, 3).as_deref() {
            Some("am") if hour == 12 => 0,
            Some("pm") if hour != 12 => hour + 12,
            _ => hour,
        };

        return NaiveDate::from_ymd_opt(year, month, day as u32)
            .and_then(|date| date.and_hms_opt(hour, minute, 0))
            .map(|stamp| stamp.and_utc().timestamp() - TIMESTAMP_UTC_OFFSET);
    }

    None
}

/// Maps a three letter month name to its number, ignoring case and a trailing comma.
fn month_from_name(word: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];

    let name = word.trim_end_matches(',').to_ascii_lowercase();

    MONTHS
        .iter()
        .position(|month| *month == name)
        .map(|index| index as u32 + 1)
}

/// Reads a bare number off a word that may carry a trailing comma, as `17,` does.
fn trailing_number(word: &str) -> Option<i32> {
    word.trim_end_matches(',').parse().ok()
}

/// Splits a `4:50` clock into hour and minute.
///
/// Tolerates a bare hour with no minutes, as `4`, and a trailing comma.
fn split_clock(word: &str) -> (u32, u32) {
    match word.trim_end_matches(',').split_once(':') {
        Some((hour, minute)) => (
            trailing_number(hour).unwrap_or(0) as u32,
            trailing_number(minute).unwrap_or(0) as u32,
        ),
        None => (trailing_number(word).unwrap_or(0) as u32, 0),
    }
}

/// Reads the meridiem that follows a clock, if the next word is one.
///
/// Split out of [`parse_timestamp`] because the clock and its meridiem are separate
/// words, and this is what joins them back together.
fn meridiem_after(words: &[&str], index: usize) -> Option<String> {
    let word = words.get(index)?.trim_end_matches(',').to_ascii_lowercase();

    matches!(word.as_str(), "am" | "pm").then_some(word)
}

// ---------------------------------------------------------------------------
// parsing: info page
// ---------------------------------------------------------------------------

/// Parses the manga landing page.
fn parse_info_page(doc: &Html, source_url: &str) -> Option<MangaInfo> {
    // The canonical link is the preferred source of the id. The URL the page was
    // fetched from is the fallback, so a page missing its canonical still parses.
    let id = doc
        .select(&sel("link[rel=canonical]"))
        .next()
        .and_then(|e| e.attr("href"))
        .and_then(split_manga_path)
        .or_else(|| split_manga_path(source_url))?
        .0;

    // The title and the page count share the `site-manga-info__title-text` class,
    // the title on an `h6` and the count on a caption `div`. So the title is the
    // first link, and the count is whichever of those elements actually reads like
    // a count, rather than whichever comes first in the document.
    let title = doc
        .select(&sel(".site-manga-info__title-text a"))
        .next()
        .map(clean_text)
        .filter(|text| !text.is_empty());

    let page_count = doc
        .select(&sel(".site-manga-info__title-text"))
        .map(clean_text)
        .find_map(|text| parse_page_count(&text));

    // The date is rendered as text inside the link that searches the day it was
    // published on (`?q=after:2025-07-17 before:2025-07-18`). The text carries the
    // time of day and so is the better source; the link is the fallback for when
    // only the day is available.
    let published = doc
        .select(&sel(".site-manga-info__title-text a[href]"))
        .find(|element| {
            element
                .attr("href")
                .is_some_and(|href| href.contains("after%3A") || href.contains("after:"))
        })
        .and_then(|element| {
            parse_timestamp(&clean_text(element))
                .or_else(|| element.attr("href").and_then(parse_published))
        });

    let cover_url = doc
        .select(&sel(".site-manga-info__cover .mdc-card__media"))
        .next()
        .and_then(|e| e.attr("style"))
        .and_then(extract_css_url);

    let tags = parse_info_tags(doc);

    // One card per gallery page, in document order. The position is read from the
    // href rather than counted, so a card that fails to parse cannot shift every
    // later page by one.
    let mut positions: Vec<u64> = doc
        .select(&sel("a.site-popunder-ad-slot[href]"))
        .filter_map(|e| e.attr("href"))
        .filter_map(split_manga_path)
        .filter(|(card_id, position)| card_id == &id && position.is_some())
        .filter_map(|(_, position)| position)
        .collect();
    positions.sort_unstable();
    positions.dedup();

    Some(MangaInfo {
        id,
        title,
        page_count,
        cover_url,
        published,
        tags,
        positions,
    })
}

/// Reads the tag chips, keeping each tag's own search URL alongside its value.
fn parse_info_tags(doc: &Html) -> Vec<SiteTag> {
    doc.select(&sel("a.mdc-chip__text[href]"))
        .filter_map(site_tag_from_anchor)
        .collect()
}

/// Reads the tag list a reader page shows alongside the gallery.
///
/// Same tags as the info page, different markup: the reader lists them as plain
/// list items rather than chips.
fn parse_reader_tags(doc: &Html) -> Vec<SiteTag> {
    doc.select(&sel("a.mdc-list-item[href]"))
        .filter_map(site_tag_from_anchor)
        .collect()
}

/// Turns a tag anchor into a [`SiteTag`], wherever on the site it appears.
fn site_tag_from_anchor(element: ElementRef) -> Option<SiteTag> {
    let search_url = element.attr("href")?.to_string();

    // The reader's list items have no `data-tag`, so fall back to the `q`
    // parameter of the link, which is the same `namespace:value` the site searches
    // by, and finally to the visible text.
    let raw = element
        .select(&sel("span[data-tag]"))
        .next()
        .and_then(|e| e.attr("data-tag"))
        .map(str::to_string)
        .or_else(|| {
            url::Url::parse(&search_url)
                .ok()?
                .query_pairs()
                .find(|(key, _)| key == "q")
                .map(|(_, value)| value.into_owned())
        })
        .unwrap_or_else(|| clean_text(element));

    let (namespace, value) = split_tag(&raw)?;

    Some(SiteTag {
        namespace: namespace.to_ascii_lowercase(),
        value: value.to_string(),
        search_url,
    })
}

/// Pulls the `url(...)` out of an inline `background-image` style.
fn extract_css_url(style: &str) -> Option<String> {
    let start = style.find("url(")? + "url(".len();
    let rest = &style[start..];
    let end = rest.find(')')?;
    Some(rest[..end].trim().trim_matches(['"', '\'']).to_string())
}

// ---------------------------------------------------------------------------
// parsing: reader page
// ---------------------------------------------------------------------------

/// Reads the reader's date, which its drawer subtitle renders next to the image
/// count: `10 images · Jul  8, 2025,  1:36 am`.
///
/// Matched on a substring because the site only ever writes the BEM modifier,
/// `site-page-drawer__subtitle--truncate`, and a class selector would miss it.
///
/// A reader URL is a complete entry point on its own, so it needs this to scope its
/// tags to a moment the same way the info page does.
fn parse_reader_published(doc: &Html) -> Option<i64> {
    doc.select(&sel("h6[class*=site-page-drawer__subtitle]"))
        .map(clean_text)
        .find_map(|text| parse_timestamp(&text))
}

/// Parses a reader page, which lists the full-size image for every gallery page.
///
/// The `data-src` of each thumbnail carries the full-size URL, so the thumbnails
/// on the info page are never needed to build a download.
fn parse_reader_page(doc: &Html) -> Option<Vec<GalleryPage>> {
    let mut pages: Vec<GalleryPage> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();

    // The one-per-page variant is the page currently being viewed. It is a `src`
    // rather than a `data-src`, and on a manga whose reader only renders one page
    // it may be the only image present.
    let selectors = [
        "img.site-reader__image[data-src][data-url]",
        "img.site-reader__one-per-page-image[src]",
    ];

    for text in selectors {
        for element in doc.select(&sel(text)) {
            let Some(page) = parse_gallery_page(&element) else {
                continue;
            };
            if seen.insert(page.position) {
                pages.push(page);
            }
        }
    }

    (!pages.is_empty()).then_some(pages)
}

/// Pulls one gallery page out of a reader `<img>`.
fn parse_gallery_page(element: &ElementRef) -> Option<GalleryPage> {
    let image_url = element
        .attr("data-src")
        .or_else(|| element.attr("src"))?
        .to_string();

    // Prefer the reader page the site advertises for this image. Fall back to the
    // `/_images/pages/<id>/<N>.jpg` shape, which carries the same position.
    let reader = element
        .attr("data-url")
        .map(str::to_string)
        .or_else(|| reader_from_image_path(&image_url))?;

    let (_, position) = split_manga_path(&reader)?;

    Some(GalleryPage {
        position: position?,
        reader_url: reader,
        image_url,
    })
}

/// Rebuilds a reader URL from a full-size image path.
///
/// `_images/pages/<id>/<N>.jpg` is the only shape the reader uses, and it is the
/// only fallback for the one-per-page image, which has no `data-url`.
fn reader_from_image_path(image_url: &str) -> Option<String> {
    let path = url::Url::parse(image_url).ok()?.path().to_string();
    let mut parts = path.split('/').filter(|s| !s.is_empty());

    if parts.next()? != "_images" || parts.next()? != "pages" {
        return None;
    }

    let id = parts.next()?;
    if !is_manga_id(id) {
        return None;
    }

    let file = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let position = file.strip_suffix(".jpg")?.parse::<u64>().ok()?;

    Some(reader_url(id, position))
}

// ---------------------------------------------------------------------------
// parsing: listing page
// ---------------------------------------------------------------------------

/// True when the document is a search/listing page rather than a manga page.
///
/// Requires a card that links to a manga, so a page holding nothing but ad slots is
/// not mistaken for a listing.
fn is_listing(doc: &Html) -> bool {
    doc.select(&sel("div.site-manga-card[id] a[href]"))
        .next()
        .is_some()
}

/// Parses a listing page: the manga cards it shows, and the link to the next page.
///
/// `raw_html` and `source_url` are needed because the next-page cursor is only
/// reachable from markup the element tree does not fully expose, and because the
/// cursor has to be paired with the search it belongs to; see [`parse_next_url`].
fn parse_listing_page(doc: &Html, raw_html: &str, source_url: &str) -> Listing {
    let cards = doc
        .select(&sel("div.site-manga-card[id]"))
        .filter_map(|element| {
            // The id alone is not enough to identify a card. Ad slots are marked up as
            // `site-manga-card` too, carry a plausible looking `site-manga-card-<id>`
            // and no link at all, so a card only counts once it links to a manga. The
            // link is also the better source of the id than the attribute.
            let (id, _) = element
                .select(&sel("a[href]"))
                .filter_map(|link| link.attr("href"))
                .find_map(split_manga_path)?;

            let title = element
                .select(&sel(".site-manga-card__title--primary"))
                .next()
                .map(clean_text)
                .filter(|text| !text.is_empty());

            // A card has several `mdc-typography--caption` elements, and when it
            // carries a secondary title that is one of them and comes first. So
            // every caption is read and the count and the date are taken out of
            // whichever ones actually hold them, rather than out of the first.
            // The count and the date share one caption, as
            // `24 images · Jan  8, 2010,  4:29 am`.
            let captions: Vec<String> = element
                .select(&sel(".site-manga-card__title .mdc-typography--caption"))
                .map(clean_text)
                .collect();

            let page_count = captions.iter().find_map(|text| parse_page_count(text));
            let published = captions.iter().find_map(|text| parse_timestamp(text));

            Some(MangaCard {
                id,
                title,
                page_count,
                published,
            })
        })
        .collect();

    Listing {
        cards,
        next_url: parse_next_url(doc, raw_html, source_url),
    }
}

/// Finds the next page of a listing.
///
/// Pagination is an opaque cursor rather than a page number, so the only way
/// forward is the cursor the page hands out.
///
/// Two places carry it, and the first is the one that survives HTML parsing. The
/// site's "NEXT" link lives inside `<noscript>`, and the parser treats
/// `<noscript>` content as raw text, so that link never becomes an element. The
/// load-more spinner next to it is a real element and holds the same cursor in
/// `data-src`, so the cursor is read from there and the next *page* URL is rebuilt
/// from it rather than reusing the spinner's `/_search` path, which answers with
/// JSON rather than HTML.
///
/// The last page of a result set carries neither, so this returns `None` there and
/// that is what ends the walk.
fn parse_next_url(doc: &Html, raw_html: &str, source_url: &str) -> Option<String> {
    let from_dom = doc
        .select(&sel("#site-search-spinner-next[data-src]"))
        .filter_map(|element| element.attr("data-src"))
        .find_map(cursor_token)
        .or_else(|| {
            doc.select(&sel("a[href]"))
                .filter_map(|element| element.attr("href"))
                .find_map(cursor_token)
        });

    // The `<noscript>` fallback, for when the spinner markup changes.
    let from_text = || {
        raw_html
            .split("p=")
            .nth(1)
            .map(|rest| {
                rest.split(|c: char| !c.is_ascii_alphanumeric())
                    .next()
                    .unwrap_or("")
            })
            .filter(|token| is_manga_id(token))
            .map(str::to_string)
    };

    let token = from_dom.or_else(from_text)?;

    Some(build_next_url(&token, source_url))
}

/// Rebuilds the next listing page from a cursor token.
///
/// The token is the whole pagination state, so the search is carried over from the
/// URL the current page was fetched with. That is what keeps a paginated walk
/// inside the result set it started in rather than sliding into the rest of the
/// site.
fn build_next_url(token: &str, source_url: &str) -> String {
    let query = url::Url::parse(source_url).ok().and_then(|url| {
        url.query_pairs()
            .find(|(key, _)| key == "q")
            .map(|(_, value)| value.into_owned())
    });

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("p", token);
    if let Some(query) = query {
        serializer.append_pair("q", &query);
    }

    format!("{SITE_URL}/?{}", serializer.finish())
}

/// Extracts the `?p=<token>` cursor out of a URL on this site.
///
/// Rejects any other host and any `p` that is not shaped like a cursor, so an
/// unrelated `?p=` elsewhere in the page cannot be mistaken for pagination.
fn cursor_token(href: &str) -> Option<String> {
    let url = url::Url::parse(href).ok()?;

    if url.host_str()? != SITE_ROOT {
        return None;
    }

    let token = url
        .query_pairs()
        .find(|(key, _)| key == "p")
        .map(|(_, value)| value.into_owned())?;

    is_manga_id(&token).then_some(token)
}

// ---------------------------------------------------------------------------
// namespaces
// ---------------------------------------------------------------------------

/// Builds one of this plugin's own namespaces.
fn ns(name: &str, description: &str) -> GenericNamespaceObj {
    GenericNamespaceObj {
        name: format!("{NS}{name}"),
        description: Some(description.to_string()),
    }
}

/// Builds a namespace for one of the site's own tag types.
///
/// The site decides its own tag vocabulary, so these are generated rather than
/// enumerated. `parody` becomes `RokuHentaiParody`.
fn site_ns(namespace: &str) -> GenericNamespaceObj {
    let mut chars = namespace.chars();
    let capitalised = match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => namespace.to_string(),
    };

    ns(
        &capitalised,
        &format!("Tag from the {namespace} field of a rokuhentai.com manga."),
    )
}

fn manga_ns() -> GenericNamespaceObj {
    ns(
        "Manga",
        "The rokuhentai.com handle of a manga, e.g. q1j2hz.",
    )
}

fn manga_url_ns() -> GenericNamespaceObj {
    ns(
        "MangaUrl",
        "Site root URL of a manga, which every tag and position resolves back to.",
    )
}

fn published_ns() -> GenericNamespaceObj {
    ns(
        "Published",
        "Unix seconds of a manga page's last change, as the site renders it. Doubles \
         as the limit_to of every tag scoped to that manga.",
    )
}

// ---------------------------------------------------------------------------
// building scraper output
// ---------------------------------------------------------------------------

/// The manga a tag belongs to, and the moment that membership was true.
///
/// Every tag this plugin records is related to a manga, and the relation is limited
/// to the manga's last change: the `limit_to` is the `RokuHentaiPublished`
/// timestamp, so a tag read back says not just "this manga has this tag" but "as of
/// the page's last change, it did". That is the site's own granularity, since the
/// date it renders is when the page last changed.
///
/// A manga whose page carried no date has no moment to limit to, so it falls back
/// to limiting to the manga itself, which is what this did before timestamps were
/// read.
struct Scope {
    /// The parent every tag relates to: the manga's handle.
    manga: Tag,
    /// The tag the relation is limited to.
    limit_to: Tag,
}

impl Scope {
    fn new(manga_id: &str, published: Option<i64>) -> Self {
        let manga = Tag {
            name: manga_id.to_string(),
            namespace: manga_ns(),
        };

        let limit_to = published
            .map(|stamp| Tag {
                name: stamp.to_string(),
                namespace: published_ns(),
            })
            .unwrap_or_else(|| manga.clone());

        Scope { manga, limit_to }
    }

    /// Ties `tag` to the manga, limited to the moment.
    ///
    /// The parent carries the manga's handle, so a tag recorded here is always
    /// resolvable: filter by the relation and you get the manga, whose
    /// `RokuHentaiMangaUrl` is the site root the tag or position came from.
    fn scoped(&self, tag: Tag) -> PluginTag {
        PluginTag {
            tag,
            relates_to: Some(RelationContext {
                tag: self.manga.clone(),
                limit_to: Some(self.limit_to.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A tag naming the manga itself, which every other tag relates to.
    fn manga_tag(&self) -> PluginTag {
        self.scoped(self.manga.clone())
    }

    /// The `RokuHentaiPublished` tag for this scope, if the page carried a date.
    ///
    /// Emitted as a scoped tag like any other, so the timestamp is reachable as a
    /// property of the manga and not only as a `limit_to` on other relations.
    fn published_tag(&self, published: Option<i64>) -> Option<PluginTag> {
        let stamp = published?;

        Some(self.scoped(Tag {
            name: stamp.to_string(),
            namespace: published_ns(),
        }))
    }
}

/// A job for `url`, inheriting the current job's site and user data.
fn job(scraperdata: &ScraperDataReturn, url: String, priority: u64) -> ScraperDataReturn {
    ScraperDataReturn {
        job: shared_types::PluginJob {
            site: scraperdata.job.site.clone(),
            priority,
            param: vec![ScraperParam::Url(Url {
                url,
                ..Default::default()
            })],
            user_data: scraperdata.job.user_data.clone(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The reader page for page 0.
///
/// One reader page lists every full-size image, so the gallery is fetched with a
/// single job rather than one per position.
fn reader_job(scraperdata: &ScraperDataReturn, id: &str) -> ScraperDataReturn {
    job(scraperdata, reader_url(id, 0), DEFAULT_PRIORITY - 1)
}

fn data(
    files: HashSet<FileObject>,
    tags: HashSet<PluginTag>,
    jobs: HashSet<ScraperDataReturn>,
) -> Vec<ScraperReturn> {
    if files.is_empty() && tags.is_empty() && jobs.is_empty() {
        return vec![ScraperReturn::Nothing];
    }

    vec![ScraperReturn::Data(shared_types::ScraperObject {
        files,
        jobs,
        tags,
    })]
}

/// Output for a manga landing page: the manga's own tags, scoped to the moment the
/// page last changed, and one job for the reader.
fn info_return(
    info: &MangaInfo,
    scraperdata: &ScraperDataReturn,
    jobs: &mut HashSet<ScraperDataReturn>,
    tags: &mut HashSet<PluginTag>,
) {
    let id = info.id.as_str();
    let scope = Scope::new(id, info.published);

    // Every tag below is scoped to the manga, which is recorded first because it is
    // the parent the relations point at.
    tags.insert(scope.manga_tag());

    // The site root URL for the manga, so a position or tag found later resolves
    // back to the page it came from.
    tags.insert(scope.scoped(Tag {
        name: manga_url(id),
        namespace: manga_url_ns(),
    }));

    if let Some(title) = &info.title {
        tags.insert(scope.scoped(Tag {
            name: title.clone(),
            namespace: ns("MangaTitle", "Title of a rokuhentai.com manga."),
        }));
    }

    if let Some(count) = info.page_count {
        tags.insert(scope.scoped(Tag {
            name: count.to_string(),
            namespace: ns("PageCount", "Number of images in a manga's gallery."),
        }));
    }

    if let Some(published) = scope.published_tag(info.published) {
        tags.insert(published);
    }

    if let Some(cover) = &info.cover_url {
        tags.insert(scope.scoped(Tag {
            name: cover.clone(),
            namespace: ns("Cover", "Cover image of a manga."),
        }));
    }

    // A tag is recorded as data, which is the whole point of a tag: it says what
    // this manga is, and it carries the site root URL to find related manga by
    // hand. It is deliberately not a job. Enqueuing one per tag turns a single
    // manga URL into 29 page fetches, and because the reader page re-emits the
    // same 29 tags the cost is paid again for every gallery that gets scraped.
    for site_tag in &info.tags {
        tags.insert(scope.scoped(Tag {
            name: site_tag.value.clone(),
            namespace: site_ns(&site_tag.namespace),
        }));

        tags.insert(scope.scoped(Tag {
            name: site_tag.search_url.clone(),
            namespace: ns(
                "TagSearchUrl",
                "Site root URL that lists manga sharing this tag.",
            ),
        }));
    }

    // The gallery itself. Positions are recorded from the reader page, which is
    // where the images are; the info page only proves how many there are.
    if !info.positions.is_empty() {
        jobs.insert(reader_job(scraperdata, id));
    }
}

/// Output for a reader page: one file per gallery position, each carrying its
/// position and the reader URL it came from, plus the tag list the page also
/// carries.
fn reader_return(
    pages: &[GalleryPage],
    reader_tags: &[SiteTag],
    published: Option<i64>,
    files: &mut HashSet<FileObject>,
    tags: &mut HashSet<PluginTag>,
) {
    let Some(id) = pages
        .first()
        .and_then(|page| split_manga_path(&page.reader_url))
        .map(|(id, _)| id)
    else {
        return;
    };

    let scope = Scope::new(&id, published);
    let manga = scope.manga.clone();

    tags.insert(scope.manga_tag());

    if let Some(published) = scope.published_tag(published) {
        tags.insert(published);
    }

    // A reader page carries the same tag list as the info page, in a different
    // markup, so a reader URL is a complete entry point on its own. As on the info
    // page the tags are data only, never jobs; see [`info_return`].
    for site_tag in reader_tags {
        tags.insert(scope.scoped(Tag {
            name: site_tag.value.clone(),
            namespace: site_ns(&site_tag.namespace),
        }));

        tags.insert(scope.scoped(Tag {
            name: site_tag.search_url.clone(),
            namespace: ns(
                "TagSearchUrl",
                "Site root URL that lists manga sharing this tag.",
            ),
        }));
    }

    for page in pages {
        let file_tags = vec![FileTagAction {
            operation: TagOperation::Set,
            tags: vec![
                PluginTag {
                    tag: manga.clone(),
                    ..Default::default()
                },
                PluginTag {
                    tag: Tag {
                        name: page.position.to_string(),
                        namespace: ns(
                            "PagePosition",
                            "Zero-based position of a page within its manga's gallery.",
                        ),
                    },
                    ..Default::default()
                },
                PluginTag {
                    tag: Tag {
                        name: page.reader_url.clone(),
                        namespace: ns(
                            "PageUrl",
                            "Reader page showing a gallery page, rooted at the site.",
                        ),
                    },
                    ..Default::default()
                },
            ],
        }];

        files.insert(FileObject {
            source: Some(FileSource::Url(page.image_url.clone())),
            tag_list: file_tags,
            ..Default::default()
        });
    }
}

/// Output for a listing page: the cards it shows, a job for each one, and a job for
/// the rest of the result set.
///
/// A listing is only ever reached through [`crate::url_dump`], that is, by a search
/// the user typed, because nothing else enqueues one. A manga page records its tags
/// as data rather than as jobs (see [`info_return`]), which is what keeps the crawl
/// from running away: the site links every manga to every tag it carries, so a
/// listing that led back out to tag searches and on to more listings never ended.
///
/// The chain is therefore bounded and has no cycle. A search walks its own pages to
/// the end, each manga yields one reader job, and the reader yields files and
/// nothing else. A manga URL on its own goes to a reader and stops. Searching a
/// common tag costs roughly what the galleries in the result set contain, which is
/// the cost of asking for the whole result set rather than its first page.
///
/// Cards are recorded as well as enqueued, because a card already carries the title
/// and page count and there is no reason to wait for the manga's own page to learn
/// them. The detail page re-emits the same tags, which is idempotent.
fn listing_return(
    listing: &Listing,
    scraperdata: &ScraperDataReturn,
    jobs: &mut HashSet<ScraperDataReturn>,
    tags: &mut HashSet<PluginTag>,
) {
    for card in &listing.cards {
        let id = card.id.as_str();
        let scope = Scope::new(id, card.published);

        tags.insert(scope.manga_tag());
        tags.insert(scope.scoped(Tag {
            name: manga_url(id),
            namespace: manga_url_ns(),
        }));

        if let Some(title) = &card.title {
            tags.insert(scope.scoped(Tag {
                name: title.clone(),
                namespace: ns("MangaTitle", "Title of a rokuhentai.com manga."),
            }));
        }

        if let Some(count) = card.page_count {
            tags.insert(scope.scoped(Tag {
                name: count.to_string(),
                namespace: ns("PageCount", "Number of images in a manga's gallery."),
            }));
        }

        if let Some(published) = scope.published_tag(card.published) {
            tags.insert(published);
        }

        jobs.insert(job(scraperdata, manga_url(id), DEFAULT_PRIORITY - 2));
    }

    // The rest of the result set. This walks a search to its end, which is bounded
    // by how many results the search has, and the cursor carries the `q` forward so
    // it cannot drift out of the search. A site-wide walk is not reachable from
    // here: nothing but `url_dump` produces a listing job, and a manga page no
    // longer enqueues tag searches.
    if let Some(next) = &listing.next_url {
        jobs.insert(job(scraperdata, next.clone(), DEFAULT_PRIORITY - 2));
    }
}

// ---------------------------------------------------------------------------
// entry points
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub fn url_dump(scraperdata: &ScraperDataReturn) -> Vec<ScraperDataReturn> {
    let mut out = Vec::new();

    let mut params = scraperdata.job.param.clone();
    params.retain(|f| matches!(f, ScraperParam::Normal(_)));

    let terms: Vec<&str> = params
        .iter()
        .filter_map(|param| match param {
            ScraperParam::Normal(normal) => Some(normal.as_str()),
            _ => None,
        })
        .collect();

    if let Some(url) = build_search_url(&terms) {
        out.push(ScraperDataReturn {
            job: shared_types::PluginJob {
                site: scraperdata.job.site.clone(),
                priority: DEFAULT_PRIORITY - 2,
                param: {
                    let mut param = params.clone();
                    param.push(ScraperParam::Url(Url {
                        url,
                        ..Default::default()
                    }));
                    param
                },
                ..Default::default()
            },
            ..Default::default()
        });
    }

    // Passthrough, so a URL handed to the plugin is scraped as given.
    for param in &scraperdata.job.param {
        if let ScraperParam::Url(url) = param {
            let mut param = params.clone();
            param.push(ScraperParam::Url(url.clone()));
            out.push(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: DEFAULT_PRIORITY - 2,
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
pub fn parser_call(
    text_input: &str,
    source_url: &str,
    scraperdata: &ScraperDataReturn,
) -> Vec<ScraperReturn> {
    let doc = Html::parse_document(text_input);

    let mut files = HashSet::new();
    let mut tags = HashSet::new();
    let mut jobs = HashSet::new();

    if let Some(pages) = parse_reader_page(&doc) {
        let reader_tags = parse_reader_tags(&doc);
        let published = parse_reader_published(&doc);
        reader_return(&pages, &reader_tags, published, &mut files, &mut tags);
    } else if is_listing(&doc) {
        let listing = parse_listing_page(&doc, text_input, source_url);
        listing_return(&listing, scraperdata, &mut jobs, &mut tags);
    } else if let Some(info) = parse_info_page(&doc, source_url) {
        info_return(&info, scraperdata, &mut jobs, &mut tags);
    } else {
        return vec![ScraperReturn::Nothing];
    }

    data(files, tags, jobs)
}
