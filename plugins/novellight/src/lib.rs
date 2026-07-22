use regex::Regex;
use std::collections::HashSet;

use scraper::{Html, Selector};
use shared_types::{
    DEFAULT_PRIORITY, FileObject, FileTagAction, GenericNamespaceObj, PluginProperties, PluginTag,
    RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf, Tag, TargetModifier,
};

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "Novelight".into(),
        properties: vec![
            PluginProperties::Ratelimit(5, std::time::Duration::from_secs(1)),
            PluginProperties::Sites(vec!["novelight".into(), "novelight.net".into()]),
        ],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();

    for param in scraperdata.job.param.iter() {
        let scraperdata = scraperdata.clone();
        match param {
            ScraperParam::Url(url) => {
                if let Ok(url) = url::Url::parse(&url.url)
                    && let Some(mut path_segments) = url.path_segments()
                {
                    let mut scraperdata = scraperdata.clone();

                    let first_item = path_segments.next();
                    // Passes catalog through for parsing
                    if first_item == Some("catalog") {
                        out.push(scraperdata);
                    }
                    // Passes book through but adds other data
                    else if first_item == Some("book")
                        && let Some(book_name) = scraperdata
                            .job
                            .user_data
                            .clone()
                            .get("book_name")
                            .map_or(path_segments.next(), |v| Some(v))
                    {
                        let fixed_book_name = book_name.replace('-', " ");
                        scraperdata
                            .job
                            .user_data
                            .insert("book_name".into(), fixed_book_name);
                        scraperdata
                            .job
                            .user_data
                            .insert("book_name_unclean".into(), book_name.to_string());

                        out.push(scraperdata);
                    }
                }
            }
            ScraperParam::Normal(_normal) => {}
            _ => {}
        }
    }
    out
}

#[derive(Debug)]
pub struct Chapter {
    pub url: String,
    pub title: String,
    pub is_paid: bool,
}

// Replicates chapter extracting + checks if a lock/paid badge exists
pub fn extract_chapters(html_content: &str) -> Vec<Chapter> {
    let mut chapters = Vec::new();
    let fragment = Html::parse_fragment(html_content);

    let anchor_selector = Selector::parse("a.chapter").unwrap();
    let title_selector = Selector::parse("div.title").unwrap();
    let cost_selector = Selector::parse(".chapter-info .cost").unwrap();

    for element in fragment.select(&anchor_selector) {
        if let Some(href) = element.value().attr("href")
            && let Some(title_elem) = element.select(&title_selector).next()
        {
            let title_text: String = title_elem
                .text()
                .collect::<Vec<_>>()
                .concat()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");

            // Replicates checking for paid icons: a.select_one(".chapter-info .cost")
            let is_paid = element.select(&cost_selector).next().is_some();

            chapters.push(Chapter {
                url: href.to_string(),
                title: title_text,
                is_paid,
            });
        }
    }
    chapters
}

// Replicates extraction of both tokens from scripts: _extract_book_tokens
fn extract_book_tokens(html: &str) -> Option<(String, String)> {
    let book_id_re = Regex::new(r#"const\s+BOOK_ID\s*=\s*"([^"]+)""#).ok()?;
    let csrf_re = Regex::new(r#"window\.CSRF_TOKEN\s*=\s*"([^"]+)""#).ok()?;

    let book_id = book_id_re.captures(html)?.get(1)?.as_str().to_string();
    let csrf = csrf_re.captures(html)?.get(1)?.as_str().to_string();

    Some((book_id, csrf))
}

#[derive(serde::Deserialize)]
struct AjaxPayload {
    html: String,
}

#[unsafe(no_mangle)]
pub fn parser_call(
    text_input: &str,
    source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let mut results = Vec::new();

    let is_pagination_request = source_url.contains("chapter-pagination");
    let is_chapter_read_request = source_url.contains("read-chapter");

    // Parses info from the search catalog
    if !is_pagination_request && !is_chapter_read_request {
        let html = Html::parse_document(text_input);
        let poster_selector = Selector::parse("div.manga-grid-list a.item").unwrap();
        let mut jobs = HashSet::new();

        for item in html.select(&poster_selector) {
            // Extracts links and inserts the chapters it scraped into the jobs queue
            if let Some(link) = item.value().attr("href") {
                let job = shared_types::PluginJob {
                    priority: DEFAULT_PRIORITY,
                    site: scraperdata.job.site.clone(),
                    param: vec![ScraperParam::Url(shared_types::Url {
                        url: format!("https://novelight.net{}", link),
                        local_modifiers: vec![],
                    })],
                    ..Default::default()
                };

                jobs.insert(ScraperDataReturn {
                    job,
                    skip_conditions: vec![],
                });
            }
        }
        if !jobs.is_empty() {
            results.push(ScraperReturn::Data(shared_types::ScraperObject {
                jobs,
                ..Default::default()
            }));
        }
    }

    if is_chapter_read_request {
        let cleaned_content = extract_chapter_body(text_input);

        let mut tags = HashSet::new();

        let chapter_num = scraperdata
            .job
            .user_data
            .get("chapter_num")
            .map_or("0", |v| v);
        let chapter_order = scraperdata
            .job
            .user_data
            .get("chapter_order")
            .map_or("0", |v| v);
        let book_id = scraperdata.job.user_data.get("book_id").map_or("0", |v| v);

        tags.insert(PluginTag {
            tag: Tag {
                name: cleaned_content,
                namespace: GenericNamespaceObj {
                    name: "NOVELLIGHT_BOOK_TEXT".into(),
                    description: Some("The text inside of a book".into()),
                },
            },
            relates_to: Some(RelationContext {
                tag: Tag {
                    name: chapter_num.to_string(),
                    namespace: GenericNamespaceObj {
                        name: "NOVELLIGHT_BOOK_CHAPTER".into(),
                        description: Some("The chapter number of a book.".into()),
                    },
                },
                limit_to: Some(Tag {
                    name: book_id.into(),
                    namespace: GenericNamespaceObj {
                        name: "NOVELLIGHT_BOOK_ID".into(),
                        description: Some(
                            "The book id that is unique to a series in Novellight.".into(),
                        ),
                    },
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        tags.insert(PluginTag {
            tag: Tag {
                name: chapter_order.to_string(),
                namespace: GenericNamespaceObj {
                    name: "NOVELLIGHT_CHAPTER_SEQUENCE".into(),
                    description: Some(
                        "The absolute index sequence location tracking order.".into(),
                    ),
                },
            },
            relates_to: Some(RelationContext {
                tag: Tag {
                    name: chapter_num.to_string(),
                    namespace: GenericNamespaceObj {
                        name: "NOVELLIGHT_BOOK_CHAPTER".into(),
                        description: Some("The chapter number of a book.".into()),
                    },
                },
                limit_to: Some(Tag {
                    name: book_id.into(),
                    namespace: GenericNamespaceObj {
                        name: "NOVELLIGHT_BOOK_ID".into(),
                        description: Some(
                            "The book id that is unique to a series in Novellight.".into(),
                        ),
                    },
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        results.push(ScraperReturn::Data(shared_types::ScraperObject {
            tags,
            ..Default::default()
        }));
        return results;
    }

    // Unpack JSON string wrapper if the input is an AJAX response payload
    let raw_html = if text_input.trim_start().starts_with('{') {
        match serde_json::from_str::<AjaxPayload>(text_input) {
            Ok(payload) => payload.html,
            Err(e) => {
                println!("Error: Failed to deserialize AJAX response body: {:?}", e);
                return results;
            }
        }
    } else {
        text_input.to_string()
    };

    let chapters = extract_chapters(&raw_html);

    let contains_chapter_one = chapters.iter().any(|c| {
        let title_clean = c.title.to_lowercase();
        title_clean.starts_with("1 ") || title_clean.contains(" 1 ") || title_clean == "1 chapter"
    });

    let current_page = scraperdata
        .job
        .user_data
        .get("page_num")
        .map_or("1", |v| v)
        .parse::<usize>()
        .unwrap_or(1);

    // Track total maximum available chapters by parsing the dropdown layout container
    let max_chapters = scraperdata
        .job
        .user_data
        .get(&format!("page_{}", current_page))
        .and_then(|v| v.parse::<usize>().ok());

    let mut scraperdata = scraperdata.clone();

    if let Some((book_id, _)) = extract_book_tokens(&raw_html) {
        scraperdata.job.user_data.insert("book_id".into(), book_id);
    }

    // Handle Follow-up Pagination Pipeline
    let should_paginate = !contains_chapter_one && (!chapters.is_empty() || !is_pagination_request);

    if should_paginate {
        let tokens = if !is_pagination_request {
            extract_book_tokens(&raw_html)
        } else {
            let b_id = scraperdata.job.user_data.get("book_id").cloned();
            let c_token = scraperdata.job.user_data.get("csrf_token").cloned();
            if let (Some(b), Some(c)) = (b_id, c_token) {
                Some((b, c))
            } else {
                None
            }
        };

        if let Some((book_id, csrf)) = tokens {
            let mut job = scraperdata.job.clone();
            let next_page = current_page + 1;

            job.user_data
                .insert("page_num".into(), next_page.to_string());
            job.user_data.insert("csrf_token".into(), csrf.clone());
            job.user_data.insert("book_id".into(), book_id.clone());

            job.priority = DEFAULT_PRIORITY - 2;

            job.param = vec![ScraperParam::Url(shared_types::Url {
                url: format!(
                    "https://novelight.net/book/ajax/chapter-pagination?csrfmiddlewaretoken={}&book_id={}&page={}",
                    csrf, book_id, next_page
                ),
                local_modifiers: vec![
                    TargetModifier {
                        target: shared_types::ModifierTarget::Text,
                        modifier: shared_types::DownloadModifiers::Header((
                            "X-Requested-With".into(),
                            "XMLHttpRequest".into(),
                        )),
                    },
                    TargetModifier {
                        target: shared_types::ModifierTarget::Text,
                        modifier: shared_types::DownloadModifiers::Header((
                            "Accept".into(),
                            "*/*".into(),
                        )),
                    },
                ],
            })];

            let mut jobs = HashSet::new();
            jobs.insert(ScraperDataReturn {
                job,
                skip_conditions: vec![],
            });
            results.push(ScraperReturn::Data(shared_types::ScraperObject {
                jobs,
                ..Default::default()
            }));
        }
    }

    // Only look for max available chapters if we are on the initial landing payload page
    if !is_pagination_request && max_chapters.is_none() {
        let base_html_doc = Html::parse_document(&raw_html);
        let option_selector = Selector::parse("select option").unwrap();

        for (index, option) in base_html_doc.select(&option_selector).enumerate() {
            if let Some(last_num) = option
                .text()
                .collect::<Vec<_>>()
                .concat()
                .split('-')
                .next_back()
            {
                scraperdata
                    .job
                    .user_data
                    .insert(format!("page_{}", index + 1), last_num.trim().to_string());
            }
        }
    }
    if let Some(max_chapters) = scraperdata
        .job
        .user_data
        .get(&format!("page_{}", current_page))
        .and_then(|v| v.parse::<usize>().ok())
    {
        // Process all individual chapter items discovered
        for (local_index, chapter) in chapters.iter().enumerate() {
            if chapter.is_paid {
                continue;
            }

            // Format into a fixed-width alpha-sortable padded sequence tracking layout identifier
            let chapter_order_string = format!("{}", max_chapters - local_index);

            let target_read_ajax = chapter
                .url
                .replace("/book/chapter/", "/book/ajax/read-chapter/");
            let mut job = scraperdata.job.clone();

            job.user_data
                .insert("chapter_num".into(), chapter.title.to_string());
            job.user_data
                .insert("chapter_order".into(), chapter_order_string.clone());
            job.user_data
                .insert("max_chapters".into(), max_chapters.to_string());

            job.param = vec![ScraperParam::Url(shared_types::Url {
                url: format!("https://novelight.net{}", target_read_ajax),
                local_modifiers: vec![
                    TargetModifier {
                        target: shared_types::ModifierTarget::Text,
                        modifier: shared_types::DownloadModifiers::Header((
                            "X-Requested-With".into(),
                            "XMLHttpRequest".into(),
                        )),
                    },
                    TargetModifier {
                        target: shared_types::ModifierTarget::Text,
                        modifier: shared_types::DownloadModifiers::Header((
                            "Accept".into(),
                            "*/*".into(),
                        )),
                    },
                ],
            })];

            let book_id = scraperdata.job.user_data.get("book_id").unwrap();

            let mut jobs = HashSet::new();
            jobs.insert(ScraperDataReturn {
                job,
                skip_conditions: vec![SkipIf::ParentsRelate(PluginTag {
                    tag: Tag {
                        name: chapter_order_string.to_string(),
                        namespace: GenericNamespaceObj {
                            name: "NOVELLIGHT_CHAPTER_SEQUENCE".into(),
                            description: Some(
                                "The absolute index sequence location tracking order.".into(),
                            ),
                        },
                    },
                    relates_to: Some(RelationContext {
                        tag: Tag {
                            name: chapter.title.to_string(),
                            namespace: GenericNamespaceObj {
                                name: "NOVELLIGHT_BOOK_CHAPTER".into(),
                                description: Some("The chapter number of a book.".into()),
                            },
                        },
                        limit_to: Some(Tag {
                            name: book_id.into(),
                            namespace: GenericNamespaceObj {
                                name: "NOVELLIGHT_BOOK_ID".into(),
                                description: Some(
                                    "The book id that is unique to a series in Novellight.".into(),
                                ),
                            },
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })],
            });
            results.push(ScraperReturn::Data(shared_types::ScraperObject {
                jobs,
                ..Default::default()
            }));
        }
    }

    let should_paginate = !contains_chapter_one && (!chapters.is_empty() || !is_pagination_request);

    if should_paginate {
        let tokens = if !is_pagination_request {
            extract_book_tokens(&raw_html)
        } else {
            let b_id = scraperdata.job.user_data.get("book_id").cloned();
            let c_token = scraperdata.job.user_data.get("csrf_token").cloned();
            if let (Some(b), Some(c)) = (b_id, c_token) {
                Some((b, c))
            } else {
                None
            }
        };

        if let Some((book_id, csrf)) = tokens {
            let mut job = scraperdata.job.clone();
            let next_page = current_page + 1;

            job.user_data
                .insert("page_num".into(), next_page.to_string());
            job.user_data.insert("csrf_token".into(), csrf.clone());
            job.user_data.insert("book_id".into(), book_id.clone());

            job.priority = DEFAULT_PRIORITY - 2;

            job.param = vec![ScraperParam::Url(shared_types::Url {
                url: format!(
                    "https://novelight.net/book/ajax/chapter-pagination?csrfmiddlewaretoken={}&book_id={}&page={}",
                    csrf, book_id, next_page
                ),
                local_modifiers: vec![
                    TargetModifier {
                        target: shared_types::ModifierTarget::Text,
                        modifier: shared_types::DownloadModifiers::Header((
                            "X-Requested-With".into(),
                            "XMLHttpRequest".into(),
                        )),
                    },
                    TargetModifier {
                        target: shared_types::ModifierTarget::Text,
                        modifier: shared_types::DownloadModifiers::Header((
                            "Accept".into(),
                            "*/*".into(),
                        )),
                    },
                ],
            })];

            let mut jobs = HashSet::new();
            jobs.insert(ScraperDataReturn {
                job,
                skip_conditions: vec![],
            });
            results.push(ScraperReturn::Data(shared_types::ScraperObject {
                jobs,
                ..Default::default()
            }));
        }
    }

    if !is_pagination_request {
        let html = Html::parse_document(&raw_html);
        let poster_selector = Selector::parse("div.second-information div.poster img").unwrap();

        if let Some(img_elem) = html.select(&poster_selector).next()
            && let Some(src_val) = img_elem.value().attr("src")
            && let Ok(base_url) = url::Url::parse(source_url)
            && let Ok(full_url) = base_url.join(src_val)
        {
            let mut files = HashSet::new();
            if let Some(book_name) = scraperdata.job.user_data.get("book_name")
                && let Some(book_name_unclean) = scraperdata.job.user_data.get("book_name_unclean")
                && let Some((book_id, _)) = extract_book_tokens(&raw_html)
            {
                files.insert(FileObject {
                    source: Some(shared_types::FileSource::Url(full_url.to_string())),
                    tag_list: vec![FileTagAction {
                        operation: shared_types::TagOperation::Set,
                        tags: vec![PluginTag {
                            tag: Tag {
                                name: format!("{}_poster", book_name_unclean),
                                namespace: GenericNamespaceObj {
                                    name: "NOVELLIGHT_book_poster".into(),
                                    description: Some("A poster for a book".into()),
                                },
                            },
                            relates_to: Some(RelationContext {
                                tag: Tag {
                                    name: book_name.to_string(),
                                    namespace: GenericNamespaceObj {
                                        name: "NOVELLIGHT_book_name".into(),
                                        description: Some("A book/novel's name".into()),
                                    },
                                },
                                limit_to: Some(Tag {
                                    name: book_id,
                                    namespace: GenericNamespaceObj {
                                        name: "NOVELLIGHT_BOOK_ID".into(),
                                        description: Some(
                                            "The book id that is unique to a series in Novellight."
                                                .into(),
                                        ),
                                    },
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                    }],
                    ..Default::default()
                });
            }

            results.push(ScraperReturn::Data(shared_types::ScraperObject {
                files,
                ..Default::default()
            }));
        }
    }

    results
}
#[derive(serde::Deserialize)]
struct ChapterReadPayload {
    content: String,
}

/// Cleans the text of ads.
/// I used gemini to make this and extract_chapter body
fn clean_watermarks(text: &str) -> String {
    let re =   Regex::new(
            r"(?i)(❖\s*N[-_\s]*[oо][-_\s]*v[-_\s]*[eе][-_\s]*l[-_\s]*[iᵢ][-_\s]*ght\s*❖|\(\s*Exclusive\s+on\s+N[-_\s]*[oо][-_\s]*v[-_\s]*[eе][-_\s]*l[-_\s]*[iᵢ][-_\s]*ght\s*\)|/\s*N[-_\s]*[oо][-_\s]*v[-_\s]*[eе][-_\s]*l[-_\s]*[iᵢ][-_\s]*ght\s*/)"
        ).unwrap()
    ;

    // Strip out the matches and trim remaining whitespace
    re.replace_all(text, "").trim().to_string()
}
pub fn extract_chapter_body(text_input: &str) -> String {
    // 1. Unpack JSON wrapper payload safely
    let raw_html = match serde_json::from_str::<ChapterReadPayload>(text_input) {
        Ok(payload) => payload.content,
        Err(e) => {
            println!(
                "Error: Failed to deserialize read-chapter JSON response: {:?}",
                e
            );
            return String::new();
        }
    };

    // 2. Parse the inner HTML fragment
    let fragment = Html::parse_fragment(&raw_html);

    // 3. Selectors for cleaning up the tree
    let div_selector = Selector::parse("div").unwrap();

    let mut clean_paragraphs = Vec::new();

    // 4. Iterate through all div elements (or choose the main text wrapper)
    for element in fragment.select(&div_selector) {
        // Skip ad blocks early
        if element
            .value()
            .attr("class")
            .is_some_and(|c| c.contains("advertisment"))
        {
            continue;
        }
        if element
            .value()
            .attr("id")
            .is_some_and(|i| i.contains("bg-ssp") || i.contains("yandex_rtb"))
        {
            continue;
        }

        // If your scraper uses a single main wrapper (like div.chapter-text),
        // ensure we only process leaf elements or collect text nodes while excluding scripts.
        // A clean approach is to iterate over child text nodes directly, filtering out parents that are scripts:

        let mut line_accumulator = String::new();

        for node in element.children() {
            if let Some(el) = node.value().as_element() {
                // If the child element is a script or ad container, skip pulling text from it
                if el.name() == "script"
                    || el.name() == "style"
                    || el.attr("class").is_some_and(|c| c.contains("advertisment"))
                {
                    continue;
                }
            }

            // Collect text if the node is a plain text node
            if let Some(text_node) = node.value().as_text() {
                line_accumulator.push_str(text_node);
            }
        }

        let trimmed = line_accumulator.trim();
        if !trimmed.is_empty() {
            // Filter and strip localized text watermarks
            let watermark_cleaned = clean_watermarks(trimmed);

            let finalized_line = watermark_cleaned.trim().to_string();
            if !finalized_line.is_empty() {
                clean_paragraphs.push(finalized_line);
            }
        }
    }

    // Join the clean lines together into standard newline separated formatting blocks
    clean_paragraphs.join("\n")
}
