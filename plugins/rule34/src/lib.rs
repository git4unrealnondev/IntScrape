use std::{collections::HashSet, fmt};

use shared_types::{
    DEFAULT_PRIORITY, FileObject, FileSource, FileTagAction, GenericNamespaceObj, HashesSupported,
    LoginNeed, LoginType, PluginJob, PluginProperties, PluginTag, RelationContext,
    ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf, Tag,
};

pub enum Site {
    R34,
}

pub enum NsIdent {
    PostId,
    Parent,
    Rating,
    General,
    Artist,
    Character,
    Copyright,
    Metadata,
    Invalid,
    Timestamp,
    Sources,
}

impl fmt::Display for Site {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Site::R34 => "Rule34",
        };
        write!(f, "{s}")
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "Rule34".into(),
        properties: vec![
            PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
            PluginProperties::Sites(vec!["r34".into(), "rule34".into(), "rule34.xxx".into()]),
            PluginProperties::Login((
                LoginNeed::Required,
                LoginType::Api(
                    "Please put the API key for the username and the user_id for the password"
                        .into(),
                    Some("".into()),
                ),
            )),
        ],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();

    // Rule34 documentation mentions pid is the page number.
    // We stick to a standard hardlimit for safety.
    let hardlimit = 2000;

    let mut params = scraperdata.job.param.clone();
    params.retain(|f| matches!(f, ScraperParam::Login(_)));

    for i in 0..hardlimit {
        let url = build_url(&scraperdata.job.param, i, false);

        if let Some(url) = url {
            let mut param = params.clone();
            param.push(ScraperParam::Url(shared_types::Url {
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
    let recursion = scraperdata
        .job
        .user_data
        .get("recursion")
        .map_or(true, |f| f != &"false");

    let site = Site::R34;
    let mut files = HashSet::new();
    let mut jobs = HashSet::new();
    let mut tags = HashSet::new();

    if let Ok(payload) = json::parse(text_input) {
        // Handle direct array returns or nested objects gracefully
        let posts_array = if payload.is_array() {
            &payload
        } else if payload["posts"].is_array() {
            &payload["posts"]
        } else {
            &payload
        };

        if !posts_array.is_empty() {
            for post in posts_array.members() {
                let mut tag_vec = Vec::new();

                if let Some(id_val) = post["id"].as_u64() {
                    let file_tag = Tag {
                        name: id_val.to_string(),
                        namespace: nsobjplg(&NsIdent::PostId, &site),
                    };

                    tag_vec.push(PluginTag {
                        tag: file_tag.clone(),
                        ..Default::default()
                    });

                    let parent_id_opt = post["parent_id"]
                        .as_u64()
                        .map(|id| id.to_string())
                        .or_else(|| post["parent_id"].as_str().map(|s| s.to_string()))
                        // Filter out empty strings, explicit zeroes, or null-like strings universally
                        .filter(|s| !s.is_empty() && s != "0");

                    if let Some(parent) = parent_id_opt {
                        tags.insert(PluginTag {
                            tag: Tag {
                                name: parent.clone(),
                                namespace: nsobjplg(&NsIdent::Parent, &site),
                            },
                            relates_to: Some(RelationContext {
                                tag: file_tag.clone(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });

                        if recursion {
                            let mut params = scraperdata.job.param.clone();
                            params.retain(|f| matches!(f, ScraperParam::Login(_)));

                            params.push(ScraperParam::Normal(format!("&id={}", &parent)));

                            let parse_url = build_url(&params, 1, true);

                            if let Some(url) = parse_url {
                                jobs.insert(ScraperDataReturn {
                                    job: PluginJob {
                                        site: scraperdata.job.site.clone(),
                                        param: vec![ScraperParam::Url(shared_types::Url {
                                            url,
                                            ..Default::default()
                                        })],
                                        priority: DEFAULT_PRIORITY - 2,
                                        ..Default::default()
                                    },
                                    skip_conditions: vec![SkipIf::FileTagRelationship(Tag {
                                        name: parent,
                                        namespace: nsobjplg(&NsIdent::PostId, &site),
                                    })],
                                });
                            }
                        }
                    }
                }

                // Gets ratings
                if let Some(rating_str) = post["rating"].as_str() {
                    tag_vec.push(PluginTag {
                        tag: Tag {
                            name: rating_str.to_string(),
                            namespace: nsobjplg(&NsIdent::Rating, &site),
                        },
                        ..Default::default()
                    });
                }

                // Gets last change timestamp
                if let Some(timestamp) = post["change"].as_u64() {
                    tag_vec.push(PluginTag {
                        tag: Tag {
                            name: timestamp.to_string(),
                            namespace: nsobjplg(&NsIdent::Timestamp, &site),
                        },
                        ..Default::default()
                    });
                }

                // Gets sources of a file split by spaces
                if let Some(sources) = post["source"].as_str() {
                    for source in sources.split(' ') {
                        tag_vec.push(PluginTag {
                            tag: Tag {
                                name: source.to_string(),
                                namespace: nsobjplg(&NsIdent::Sources, &site),
                            },
                            ..Default::default()
                        });
                    }
                }

                if post["tag_info"].is_array() {
                    // Leverages the exact payload names from fields=tag_info
                    for tag_obj in post["tag_info"].members() {
                        if let Some(tag_name) = tag_obj["tag"].as_str() {
                            let ns_type = match tag_obj["type"].as_str() {
                                Some("artist") => NsIdent::Artist,
                                Some("character") => NsIdent::Character,
                                Some("copyright") => NsIdent::Copyright,
                                Some("metadata") => NsIdent::Metadata,
                                Some("tag") => NsIdent::General,
                                _ => NsIdent::General,
                            };

                            tag_vec.push(PluginTag {
                                tag: Tag {
                                    name: tag_name.to_string(),
                                    namespace: nsobjplg(&ns_type, &site),
                                },
                                ..Default::default()
                            });
                        }
                    }
                }

                // 5. Extract Media Sources
                let source = if post["file_url"].is_empty() {
                    None
                } else {
                    post["file_url"]
                        .as_str()
                        .map(|u| FileSource::Url(u.to_string()))
                };

                // Rule34 hash object mappings are stored lowercase
                let hash = post["hash"]
                    .as_str()
                    .map(|h| HashesSupported::Md5(h.to_string()));

                files.insert(FileObject {
                    source,
                    hash,
                    tag_list: vec![FileTagAction {
                        tags: tag_vec,
                        ..Default::default()
                    }],
                    skip_if: Vec::new(),
                });
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

fn nsobjplg(name: &NsIdent, site: &Site) -> GenericNamespaceObj {
    let site_str = site.to_string();

    enum Desc {
        Static(&'static str),
        Dynamic(String),
    }

    let (suffix, desc_type) = match name {
        NsIdent::General => (
            "General",
            Desc::Dynamic(format!("General descriptive tags for {}.", site_str)),
        ),
        NsIdent::Artist => ("Artist", Desc::Static("Creators or artistic sources.")),
        NsIdent::Character => ("Character", Desc::Static("Depicted personalities.")),
        NsIdent::Copyright => (
            "Copyright",
            Desc::Static("Origin franchises or intellectual property properties."),
        ),
        NsIdent::Metadata => (
            "Metadata",
            Desc::Static("Meta details regarding production aspects."),
        ),
        NsIdent::Parent => (
            "Parent_id",
            Desc::Static("Upstream item mapping indicators."),
        ),
        NsIdent::Invalid => ("Invalid", Desc::Static("Invalid configuration elements.")),
        NsIdent::Timestamp => ("Timestamp", Desc::Static("Last time a post was updated.")),
        NsIdent::Sources => (
            "Sources",
            Desc::Static("Alternative sources for a file. Can also be general links."),
        ),
        NsIdent::Rating => ("Rating", Desc::Static("Content classification indicators.")),
        NsIdent::PostId => (
            "Id",
            Desc::Dynamic(format!(
                "Unique structure identification indexes assigned by {}.",
                site_str
            )),
        ),
    };

    let description_string = match desc_type {
        Desc::Static(s) => s.to_string(),
        Desc::Dynamic(d) => d,
    };

    GenericNamespaceObj {
        name: format!("{}_{}", site_str, suffix),
        description: Some(description_string),
    }
}

fn build_url(
    params: &[shared_types::ScraperParam],
    pagenum: u64,
    should_skip_tag: bool,
) -> Option<String> {
    if params.is_empty() {
        return None;
    }

    // Set maximum documented limit of 1000 items per call for structural efficiency
    let limit = 100;
    let mut url = format!(
        "https://api.rule34.xxx/index.php?page=dapi&s=post&q=index&json=1&fields=tag_info&limit={}",
        limit
    );

    // Parse login details using your exact LoginType definition
    let login_info = params.iter().find_map(|p| {
        if let shared_types::ScraperParam::Login(login_type) = p {
            match login_type {
                // If using ApiNamespaced(Namespace, Some(user_id), Some(api_key))
                shared_types::LoginType::ApiNamespaced(_, Some(api_key), Some(user_id)) => {
                    Some((user_id.clone(), api_key.clone()))
                }
                // Fallback variant if your app passes it via a standard Api(user_id, Some(api_key))
                shared_types::LoginType::Api(api_key, Some(user_id)) => {
                    Some((user_id.clone(), api_key.clone()))
                }
                _ => None,
            }
        } else {
            None
        }
    });

    let tags: Vec<&str> = params
        .iter()
        .filter_map(|p| {
            if let shared_types::ScraperParam::Normal(tag) = p {
                Some(tag.as_str())
            } else {
                None
            }
        })
        .collect();

    if tags.is_empty() {
        return None;
    }

    // Append auth credentials safely if provided
    if let Some((user_id, api_key)) = login_info {
        url += &format!("&user_id={}&api_key={}", user_id, api_key);
    }

    if should_skip_tag {
        url += &format!("{}", tags.join("+"));
    } else {
        url += &format!("&tags={}", tags.join("+"));
    }
    url += &format!("&pid={}", pagenum);

    Some(url)
}
