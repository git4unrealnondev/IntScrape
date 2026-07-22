use json::{self};
use std::{collections::HashSet, fmt};

use shared_types::{
    DEFAULT_PRIORITY, FileObject, FileSource, FileTagAction, GenericNamespaceObj, PluginJob,
    PluginProperties, PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn,
    SkipIf, Tag, TargetModifier, Url,
};

pub enum Site {
    DivaScans,
}

pub enum NsIdent {
    SeriesSlug,
    SeriesTitle,
    SeriesDescription,
    SeriesTitlePretty,
    SeriesType,
    ChapterName,
    ChapterNum,
    ChapterText,
    PageNum,
    SourceUrl,
}

impl fmt::Display for Site {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Site::DivaScans => "DivaScans",
        };
        write!(f, "{s}")
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "DivaScans".into(),
        properties: vec![
            PluginProperties::Ratelimit(1, std::time::Duration::from_secs(2)),
            PluginProperties::Sites(vec!["divascans".into(), "divascans.org".into()]),
            PluginProperties::Modifier(TargetModifier {
                target: shared_types::ModifierTarget::Text,
                modifier: shared_types::DownloadModifiers::Header((
                    "Referer".into(),
                    "divascans.org".into(),
                )),
            }),
        ],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let site = Site::DivaScans;
    let mut out = Vec::new();

    // Handles downstream structural URL passthroughs (individual links processing)
    for param in scraperdata.job.param.clone() {
        if let ScraperParam::Url(url) = param {
            let param = vec![ScraperParam::Url(url.clone())];
            out.push(ScraperDataReturn {
                job: PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: DEFAULT_PRIORITY,
                    param,
                    ..Default::default()
                },
                ..Default::default()
            });

            // Adds chapter jobs into system
            if !url.url.contains("/chapter/") {
                for i in 1..9999 {
                    out.push(ScraperDataReturn {
                        job: PluginJob {
                            priority: DEFAULT_PRIORITY - 1,
                            site: scraperdata.job.site.clone(),
                            param: vec![ScraperParam::Url(Url {
                                url: format!("{}/chapter/{}", url.url, i),
                                ..Default::default()
                            })],
                            ..Default::default()
                        },
                        skip_conditions: vec![SkipIf::ParentsRelateLimitto((
                            Tag {
                                name: i.to_string(),
                                namespace: nsobjplg(&NsIdent::ChapterNum, &site),
                            },
                            Tag {
                                namespace: nsobjplg(&NsIdent::SeriesTitle, &site),
                                name: url.url.rsplit('/').next().unwrap().to_string(),
                            },
                        ))],
                    });
                }
            }
        }
    }

    out
}

use regex::Regex;

pub fn extract_cover_image(html: &str) -> Option<String> {
    // Matches the internal media API hash (32 hex characters followed by .webp)
    // inside the Next.js JS pushes.
    let re =
        Regex::new(r#"https%3A%2F%2Fmedia\.divascans\.org%2Fapi%2F([a-f0-9]{32})\.webp"#).ok()?;

    if let Some(cap) = re.captures(html) {
        let media_hash = cap.get(1)?.as_str();

        // Reconstruct the exact Next.js image proxy URL requested
        return Some(format!(
            "https://divascans.org/_next/image?url=https%3A%2F%2Fmedia.divascans.org%2Fapi%2F{}.webp&w=1200&q=75",
            media_hash
        ));
    }

    None
}
use scraper::{Html, Selector};
use serde_json::Value;

fn extract_clean_json(input: &str) -> Result<String, Box<dyn std::error::Error>> {
    let re = Regex::new(r#"(?s)self\.__next_f\.push\(\[\s*\d+\s*,\s*"(.*?)"\s*\]\s*\)"#)?;

    let caps = re
        .captures(input)
        .ok_or("Could not find the Next.js push pattern")?;

    let raw_string_payload = &caps[1];

    let wrapper = format!("\"{}\"", &raw_string_payload[3..]);
    let unescaped_str: String = serde_json::from_str(&wrapper)?;

    let json_value: Value = serde_json::from_str(&unescaped_str)?;

    let pretty_json = serde_json::to_string_pretty(&json_value)?;

    Ok(pretty_json)
}

fn extract_clean_json_text(input: &str) -> Result<String, Box<dyn std::error::Error>> {
    // 1. Regex to capture the content inside the quotes of the second argument
    // Matches the string between the quotes: [1,"..."]
    let re = Regex::new(r#"self\.__next_f\.push\(\[\d+,"(.*)"\]\)"#)?;

    if let Some(caps) = re.captures(input) {
        let raw_js_string = &caps[1];

        // 2. The data is a JavaScript-escaped string.
        // We use serde_json to unescape it properly (handles \u003c, \n, etc.)
        // We must add quotes around it so it looks like a valid JSON string literal.
        let json_string = format!("\"{}\"", raw_js_string);

        let text: String = serde_json::from_str(&json_string)?;

        // 3. Now 'unescaped_text' is the HTML content.
        // If you just want the text content without the HTML tags:
        //let document = Html::parse_fragment(&unescaped_text);
        //let text = document.root_element().text().collect::<Vec<_>>().join("\n");

        return Ok(text);
    }

    Err("Pattern not found".into())
}

#[unsafe(no_mangle)]
pub fn parser_call(
    text_input: &str,
    source_url: &str,
    _scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    //println!("{}", text_input);
    let site = Site::DivaScans;
    let mut files = HashSet::new();
    let jobs = HashSet::new();
    let mut tags = HashSet::new();

    if let Some(cover_url) = extract_cover_image(text_input) {
        files.insert(FileObject {
            source: Some(FileSource::Url(cover_url.to_string())),
            hash: None,
            tag_list: vec![FileTagAction {
                operation: shared_types::TagOperation::Add,
                tags: vec![PluginTag {
                    tag: Tag {
                        namespace: nsobjplg(&NsIdent::SourceUrl, &site),
                        name: cover_url.to_string(),
                    },
                    relates_to: Some(RelationContext {
                        tag: Tag {
                            namespace: nsobjplg(&NsIdent::SeriesTitle, &site),
                            name: source_url.rsplit('/').next().unwrap().to_string(),
                        },
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            }],
            skip_if: vec![],
        });
    }

    //source_url.contains("series") && source_url.contains("comic");
    let document = Html::parse_document(text_input);

    let script_selector = Selector::parse("script").unwrap();

    let mut chapters = Vec::new();
    let re = Regex::new(r"\d+:T[a-zA-Z0-9]+").unwrap();
    let mut should_download_next = false;

    // Iterate through found script nodes
    for script_element in document.select(&script_selector) {
        let script_text = script_element.inner_html();

        if script_text.contains("self.__next_f.push") && source_url.contains("/chapter/") {
            match extract_clean_json_text(&script_text) {
                Ok(cleaned_text) => {
                    if should_download_next {
                        let cleaned_text = cleaned_text.replacen("<div>", r#"<div style="overflow: visible; display: flex; flex-direction: column; gap: 1.25em;background:black; color:rgb(205, 200, 194);">"#, 1);

                        tags.insert(PluginTag {
                            tag: Tag {
                                namespace: nsobjplg(&NsIdent::ChapterText, &site),
                                name: format!(r#"<div style="overflow: visible; display: flex; flex-direction: column; gap: 1.25em;background:black; color:rgb(205, 200, 194);"> {} </div>"#, cleaned_text),
                            },
                            relates_to: Some(RelationContext {
                                tag: Tag {
                                    namespace: nsobjplg(&NsIdent::ChapterNum, &site),
                                    name: source_url.rsplit('/').next().unwrap().to_string(),
                                },
                                limit_to: Some(Tag {
                                    namespace: nsobjplg(&NsIdent::SeriesTitle, &site),
                                    name: source_url.rsplit('/').nth(2).unwrap().to_string(),
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                        should_download_next = false;
                    }
                    if re.is_match(&cleaned_text) {
                        should_download_next = true;
                    }
                }
                Err(_err) => {} //println!("REGEX failed on script {}. ERROR: {}", &script_text, &err)
            }
        }

        // Target specifically the scripts containing the Next.js push streaming data
        if script_text.contains("self.__next_f.push")
            && let Ok(cleaned_text) = extract_clean_json(&script_text)
            && let Ok(parsed) = json::parse(&cleaned_text)
        {
            // println!("{}", &script_text);
            // dbg!();
            // dbg!();
            // Main page parsing
            let series = &parsed[3][3]["series"];
            if !series.is_null()
                && let Some(slug) = series["slug"].as_str()
            {
                let title_slug = Tag {
                    name: slug.to_string(),
                    namespace: nsobjplg(&NsIdent::SeriesTitle, &site),
                };
                if let Some(title) = series["title"].as_str() {
                    tags.insert(PluginTag {
                        tag: Tag {
                            name: title.to_string(),
                            namespace: nsobjplg(&NsIdent::SeriesTitlePretty, &site),
                        },
                        relates_to: Some(RelationContext {
                            tag: title_slug.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }
                if let Some(description) = series["description"].as_str() {
                    tags.insert(PluginTag {
                        tag: Tag {
                            name: description.to_string(),
                            namespace: nsobjplg(&NsIdent::SeriesDescription, &site),
                        },
                        relates_to: Some(RelationContext {
                            tag: title_slug.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }
                /*   if let Some(cover_image) = series["coverImage"].as_str() {
                    tags.insert(PluginTag {
                        tag: Tag {
                            name: cover_image.to_string(),
                            namespace: nsobjplg(&NsIdent::SourceUrl, &site),
                        },
                        relates_to: Some(RelationContext {
                            tag: title_slug.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }*/
                for genre in series["genres"].members() {
                    if let Some(genre_parsed) = genre["slug"].as_str() {
                        tags.insert(PluginTag {
                            tag: Tag {
                                name: genre_parsed.to_string(),
                                namespace: nsobjplg(&NsIdent::SeriesSlug, &site),
                            },
                            relates_to: Some(RelationContext {
                                tag: title_slug.clone(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                    }
                }
            }

            // Chapter parsing
            for item in parsed.members() {
                // Look into index 3 of each inner array item, checking for "allChapters"
                let all_chapters = &item[3]["chapter"];

                // If it exists (is not null), loop over the chapters array
                if !all_chapters.is_null() {
                    for ch in all_chapters["pages"].members() {
                        chapters.push(ch.clone());
                    }
                }
            }
        }
    }

    for chapter in chapters {
        if let Some(image_url) = chapter["imageUrl"].as_str()
            && let Some(page_num) = chapter["pageNumber"].as_u64()
        {
            files.insert(FileObject {
                source: Some(FileSource::Url(image_url.into())),
                hash: None,
                tag_list: vec![FileTagAction {
                    operation: shared_types::TagOperation::Add,
                    tags: vec![PluginTag {
                        tag: Tag {
                            namespace: nsobjplg(&NsIdent::PageNum, &site),
                            name: page_num.to_string(),
                        },
                        relates_to: Some(RelationContext {
                            tag: Tag {
                                namespace: nsobjplg(&NsIdent::ChapterNum, &site),
                                name: source_url.rsplit('/').next().unwrap().to_string(),
                            },
                            limit_to: Some(Tag {
                                namespace: nsobjplg(&NsIdent::SeriesTitle, &site),
                                name: source_url.rsplit('/').nth(2).unwrap().to_string(),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                }],
                skip_if: vec![],
            });
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

fn nsobjplg(name: &NsIdent, site: &Site) -> GenericNamespaceObj {
    let site_str = site.to_string();

    let (suffix, description) = match name {
        NsIdent::SeriesSlug => ("Series_Slug", "Any descriptors for a series"),
        NsIdent::SeriesTitlePretty => (
            "Series_Title_Pretty",
            "The formal publication name cataloged for the book properties.",
        ),
        NsIdent::SeriesTitle => ("Series_Title", "Internal name for a series."),

        NsIdent::SeriesType => (
            "Series_Type",
            "Content categorizations indicating format varieties like Comic or Novel.",
        ),
        NsIdent::SeriesDescription => {
            ("Series_Description", "The description of a novel or comic.")
        }

        NsIdent::ChapterName => (
            "Chapter_Name",
            "Explicit URL identity slice tracking current single text chapter instances.",
        ),
        NsIdent::ChapterNum => (
            "Chapter_Number",
            "Precise float conversions ordering sequential structural timeline maps.",
        ),
        NsIdent::PageNum => ("Page_Number", "A page number as it relates to a chapter."),
        NsIdent::SourceUrl => ("source_url", "A source for a file"),
        NsIdent::ChapterText => ("Chapter_Text", "A text for a chapter"),
    };

    GenericNamespaceObj {
        name: format!("{}_{}", site_str, suffix),
        description: Some(description.to_string()),
    }
}
