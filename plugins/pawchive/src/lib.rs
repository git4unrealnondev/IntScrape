use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use shared_types::{
    DEFAULT_PRIORITY, DownloadModifiers, FileObject, FileSource, FileTagAction,
    GenericNamespaceObj, HashesSupported, ModifierTarget, PluginJob, PluginProperties, PluginTag,
    RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf, Tag, TargetModifier,
};

// root of site incase it changes
const SITE_ROOT: &str = "pawchive.pw";

// Total services suppored by site. Used by url parser
const SERVICES: [&str; 2] = ["patreon", "pivix"];

// How many max posts we can get from a creator currently 65500
const URL_GEN_MAX: u8 = u8::MAX;

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "Pawchive".into(),
        properties: vec![
            PluginProperties::ThreadNum(5),
            PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
            PluginProperties::Sites(vec!["pawchive".into(), "pawchive.pw".into()]),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Text,
                modifier: DownloadModifiers::Header(("Accept".into(), "application/json".into())),
            }),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header((
                    "Referer".into(),
                    format!("https://{SITE_ROOT}/"),
                )),
            }),
            /* PluginProperties::Modifier(TargetModifier {
            target: ModifierTarget::Media,
            modifier: DownloadModifiers::Header((
                "Accept".into(),
                    "image/avif,image/webp,image/png,image/svg+xml,image/*;q=0.8,*/*;q=0.5".into(),
                )),
            }),*/
        ],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();

    let urls: Vec<String> = scraperdata
        .job
        .param
        .iter()
        .filter_map(|f| {
            if let ScraperParam::Url(url) = f {
                Some(url.url.clone())
            } else {
                None
            }
        })
        .collect();

    for url in urls.iter() {
        for url in generate_api_request(&filter_url(url)) {
            out.push(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: shared_types::DEFAULT_PRIORITY - 2,
                    param: vec![shared_types::ScraperParam::Url(shared_types::Url {
                        url,
                        ..Default::default()
                    })],
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
    _source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let mut files = HashSet::new();
    let mut tags = HashSet::new();
    let mut jobs = HashSet::new();

    let recursion = scraperdata
        .job
        .user_data
        .get("recursion")
        .is_none_or(|f| f != "false");

    // Handles individual and groups of posts the same
    let mut posts = Vec::new();
    if let Ok(post_list) = serde_json::from_str::<Vec<PostItem>>(text_input) {
        posts.extend(post_list);
    } else {
        dbg!(serde_json::from_str::<Vec<PostItem>>(text_input));
    }
    if let Ok(post) = serde_json::from_str::<PostItem>(text_input) {
        posts.push(post);
    }

    for post in posts {
        let correct_timestamp = if let Some(edited_timestamp) = post.edited {
            edited_timestamp
        } else {
            if let Some(published_timestamp) = post.published {
                published_timestamp
            } else {
                if let Some(added_timestamp) = post.added {
                    added_timestamp
                } else {
                    continue;
                }
            }
        };
        let post_edited = Tag {
            name: format!("{}_{}", post.id, correct_timestamp),
            namespace: GenericNamespaceObj {
                name: "Pawchive_Post_Edited".into(),
                description: Some("When a post was editied in Pawchive".into()),
            },
        };
        let post_relate = RelationContext {
            limit_to: Some(post_edited.clone()),
            tag: Tag {
                name: post.id.to_string(),
                namespace: GenericNamespaceObj {
                    name: "Pawchive_Post_Id".into(),
                    description: Some("A posts unique ID".into()),
                },
            },
            ..Default::default()
        };

        // Adds title into tags
        tags.insert(PluginTag {
            tag: Tag {
                name: post.title.to_string(),
                namespace: GenericNamespaceObj {
                    name: "Pawchive_Post_Title".into(),
                    description: Some("A post's title".into()),
                },
            },
            relates_to: Some(post_relate.clone()),
            ..Default::default()
        });

        // Adds content into tags
        if !post.content.is_empty() {
            tags.insert(PluginTag {
                tag: Tag {
                    name: post.content.to_string(),
                    namespace: GenericNamespaceObj {
                        name: "Pawchive_Post_Content".into(),
                        description: Some("A post's content".into()),
                    },
                },
                relates_to: Some(post_relate.clone()),
                ..Default::default()
            });
        }

        // Relates the post id to the service & user_id
        tags.insert(PluginTag {
            tag: Tag {
                name: post.id.to_string(),
                namespace: GenericNamespaceObj {
                    name: "Pawchive_Post_Id".into(),
                    description: Some("A posts unique ID".into()),
                },
            },
            relates_to: Some(RelationContext {
                tag: Tag {
                    name: post.service,
                    namespace: GenericNamespaceObj {
                        name: "Pawchive_Post_Service".into(),
                        description: Some("The service where a post is from".into()),
                    },
                },
                limit_to: Some(Tag {
                    name: post.user,
                    namespace: GenericNamespaceObj {
                        name: "Pawchive_User_Id".into(),
                        description: Some("The Id of a user from pawchive".into()),
                    },
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        // Gets post tags
        if let Some(ref post_tags) = post.tags
            && let Ok(post_tags) = parse_custom_format(post_tags)
        {
            for tag in post_tags {
                tags.insert(PluginTag {
                    tag: Tag {
                        name: tag.to_string(),
                        namespace: GenericNamespaceObj {
                            name: "Pawchive_Post_Tag".into(),
                            description: Some("Tags associated with a post".into()),
                        },
                    },
                    relates_to: Some(post_relate.clone()),
                    ..Default::default()
                });
            }
        }

        for (idx, attachment) in post.attachments.iter().enumerate() {
            if let Some(ref attachment_path) = attachment.path {
                let file_url = format!("https://file.{SITE_ROOT}/data{}", attachment_path);
                let mut tags = vec![PluginTag {
                    tag: Tag {
                        name: file_url.clone(),
                        namespace: GenericNamespaceObj {
                            name: "source_url".into(),
                            description: Some("A source for a file".into()),
                        },
                    },
                    relates_to: Some(RelationContext {
                        tag: Tag {
                            name: idx.to_string(),
                            namespace: GenericNamespaceObj {
                                name: "Pawchive_Attachment_Position".into(),
                                description: Some("Where in the post was the file at".into()),
                            },
                        },
                        limit_to: Some(post_edited.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }];
                if let Some(attachment_name) = attachment.name.clone() {
                    tags.push(PluginTag {
                        tag: Tag {
                            name: attachment_name,
                            namespace: GenericNamespaceObj {
                                name: "source_url".into(),
                                description: Some("A source for a file".into()),
                            },
                        },
                        relates_to: Some(RelationContext {
                            tag: Tag {
                                name: idx.to_string(),
                                namespace: GenericNamespaceObj {
                                    name: "Pawchive_Attachment_Position".into(),
                                    description: Some("Where in the post was the file at".into()),
                                },
                            },
                            limit_to: Some(post_edited.clone()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }

                // Attaches a sha256 hash to the file
                if let Some(raw_file_name_with_ext) = attachment_path.rsplit('/').next()
                    && let Some(raw_file_name) = raw_file_name_with_ext.rsplit('.').next_back()
                {
                    files.insert(FileObject {
                        source: Some(FileSource::Url(file_url.clone())),
                        hash: vec![HashesSupported::Sha256(raw_file_name.to_uppercase())],
                        tag_list: vec![FileTagAction {
                            operation: shared_types::TagOperation::Add,
                            tags,
                        }],
                        skip_if: vec![],
                    });
                }
            }
        }
    }

    if files.is_empty() && jobs.is_empty() && tags.is_empty() {
        return vec![ScraperReturn::Nothing];
    }

    vec![ScraperReturn::Data(shared_types::ScraperObject {
        files,
        jobs,
        tags,
        ..Default::default()
    })]
}

/// Generates a set of urls based on url_properties
fn generate_api_request(url_properties: &[UrlProperties]) -> Vec<String> {
    let mut service = None;
    let mut user_id = None;
    let mut post_id = None;

    // Extract what we need from the properties list
    for prop in url_properties {
        match prop {
            UrlProperties::Service(s) => service = Some(s),
            UrlProperties::UserId(id) => user_id = Some(id),
            UrlProperties::PostId(id) => post_id = Some(id),
        }
    }

    match (service, user_id, post_id) {
        // Gets all posts by user
        (Some(s), Some(u), None) => (0..URL_GEN_MAX)
            .step_by(50)
            .map(|o| format!("https://{SITE_ROOT}/api/v1/{s}/user/{u}?o={o}"))
            .collect(),
        (Some(s), Some(u), Some(p)) => {
            vec![format!("https://{SITE_ROOT}/api/v1/{s}/user/{u}/post/{p}")]
        }

        _ => vec![],
    }
}

fn filter_url(url: &str) -> Vec<UrlProperties> {
    if !url.contains(SITE_ROOT) {
        return Vec::new();
    }

    let Ok(url_parsed) = url::Url::parse(url) else {
        return Vec::new();
    };

    let segments: Vec<&str> = match url_parsed.path_segments() {
        Some(s) => s.collect(),
        None => return Vec::new(),
    };

    let Some(user_idx) = segments.iter().position(|&s| s == "user") else {
        return Vec::new();
    };

    // Build the properties cleanly using vector collection or pushing conditionally
    let mut out = Vec::new();

    // 1. Service (right before "user")
    if user_idx > 0 {
        let service = segments[user_idx - 1];
        if SERVICES.contains(&service) {
            out.push(UrlProperties::Service(service.to_string()));
        }
    }

    // 2. User ID (right after "user")
    if let Some(&user_id) = segments.get(user_idx + 1) {
        out.push(UrlProperties::UserId(user_id.to_string()));
    }

    // 3. Post ID (optional, after "post")
    if let Some(post_idx) = segments[user_idx + 1..].iter().position(|&s| s == "post") {
        let absolute_post_idx = user_idx + 1 + post_idx;
        if let Some(&post_id) = segments.get(absolute_post_idx + 1) {
            out.push(UrlProperties::PostId(post_id.to_string()));
        }
    }

    out
}

fn parse_custom_format(input: &str) -> Result<Vec<String>, String> {
    // Strip outer quotes if they exist, e.g., "{...}" -> {...}
    let trimmed = input.trim();
    let inner = if trimmed.starts_with('"') && trimmed.ends_with('"') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    // Strip outer curly braces if they exist: "{A,B}" -> A,B
    let inner = inner.trim();
    let content = if inner.starts_with('{') && inner.ends_with('}') {
        &inner[1..inner.len() - 1]
    } else {
        inner
    };

    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                // Handle escaped quotes ("") or toggle quote state
                if in_quotes && chars.peek() == Some(&'"') {
                    chars.next(); // consume second quote
                    current_field.push('"');
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                // Comma splits fields only if we aren't inside quotes
                fields.push(current_field.trim().to_string());
                current_field.clear();
            }
            _ => {
                current_field.push(c);
            }
        }
    }

    // Push the final field
    fields.push(current_field.trim().to_string());

    Ok(fields)
}

fn main() {
    let raw = r#""{\"Alternative Versions\",FEET,Fluttershy,Solo's}""#;
    match parse_custom_format(raw) {
        Ok(res) => println!("{:#?}", res),
        Err(e) => eprintln!("Error: {}", e),
    }
}

#[derive(Debug)]
enum UrlProperties {
    UserId(String),
    PostId(String),
    Service(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PostItem {
    pub id: String,
    pub user: String,
    pub service: String,
    pub title: String,
    pub content: String,
    pub embed: HashMap<String, serde_json::Value>, // Handles empty object {}
    pub shared_file: bool,

    #[serde(deserialize_with = "deserialize_datetime_to_timestamp")]
    pub added: Option<i64>,

    #[serde(deserialize_with = "deserialize_datetime_to_timestamp")]
    pub published: Option<i64>,

    #[serde(deserialize_with = "deserialize_datetime_to_timestamp")]
    pub edited: Option<i64>,

    pub attachments: Vec<FileInfo>,
    pub poll: Option<serde_json::Value>,
    pub captions: Option<serde_json::Value>,
    pub tags: Option<String>,
    pub origin: String,
    pub preview_state: String,
    pub has_full: bool,
    pub preview_attempts: u32,
    pub detail_fetched: bool,
    pub import_size_cap_gb: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: Option<String>,
    pub path: Option<String>,
}

/// Helper function to parse ISO 8601 strings into Unix timestamps (i64)
/// Gemini slop coded
fn deserialize_datetime_to_timestamp<'de, D>(
    deserializer: D, // Removed `Option<>` wrapper here
) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Since serde's deserialize_with for an Option field might pass a null/absent value
    // depending on attributes, or if you want to handle optional strings:
    // If the field itself is `Option<i64>`, you can use `serde::Deserialize::deserialize(deserializer)?`
    // wrapped in an Option helper, or handle it via `Option::deserialize`.

    let s = match Option::<String>::deserialize(deserializer)? {
        Some(s) => s,
        None => return Ok(None),
    };

    // 1. Try parsing with fractional seconds (e.g., "2026-06-10T20:53:57.257638")
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(Some(naive_dt.and_utc().timestamp()));
    }

    // 2. Try parsing without fractional seconds (e.g., "2026-08-01T22:27:46")
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(Some(naive_dt.and_utc().timestamp()));
    }

    // 3. Fallback to full RFC3339/ISO 8601 with timezone if present
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Ok(Some(dt.with_timezone(&Utc).timestamp()));
    }

    Err(serde::de::Error::custom(format!(
        "Failed to parse '{}' as a valid date-time",
        s
    )))
}
