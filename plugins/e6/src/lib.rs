use std::{collections::HashSet, fmt};

use shared_types::{
    DEFAULT_PRIORITY, FileObject, FileSource, FileTagAction, GenericNamespaceObj, HashesSupported,
    PluginJob, PluginProperties, PluginTag, RelationContext, ScraperDataReturn, ScraperParam,
    ScraperReturn, SkipIf, Tag,
};

pub enum Site {
    E6,
    E6AI,
}

pub enum NsIdent {
    PoolCreatedAt,
    PoolCreator,
    PoolCreatorId,
    PoolDescription,
    PoolName,
    PoolUpdatedAt,
    PoolId,
    PoolPosition,
    PostId,
    Sources,
    Description,
    Parent,
    Children,
    Rating,
    Meta,
    Lore,
    Artist,
    Copyright,
    Character,
    Contributor,
    Species,
    General,
    Director,
    Franchise,
    Invalid,
    PoolTimestamp,
}

impl fmt::Display for Site {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Site::E6 => "E621",
            Site::E6AI => "E6AI",
        };
        write!(f, "{s}")
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![
        shared_types::Plugin {
            name: "E621".into(),
            properties: vec![
                PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
                PluginProperties::Sites(vec!["e6".into(), "E621".into(), "e621.com".into()]),
            ],
            ..Default::default()
        },
        shared_types::Plugin {
            name: "E6AI".into(),
            properties: vec![
                PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
                PluginProperties::Sites(vec!["e6ai".into(), "E6AI".into(), "e6ai.com".into()]),
            ],
            ..Default::default()
        },
    ]
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();

    let site = if ["e6", "e621", "e621.com"].contains(&scraperdata.job.site.to_lowercase().as_str())
    {
        Site::E6
    } else {
        Site::E6AI
    };

    let hardlimit = 751;

    for i in 1..hardlimit {
        let url = build_url(&scraperdata.job.param, i, &site);

        if let Some(url) = url {
            out.push(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: shared_types::DEFAULT_PRIORITY - 2,
                    param: vec![shared_types::ScraperParam::Url(url)],
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    }

    // Handles URL passthrough
    for param in scraperdata.job.param.iter() {
        if let ScraperParam::Url(url) = param {
            out.push(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: shared_types::DEFAULT_PRIORITY - 2,
                    param: vec![shared_types::ScraperParam::Url(url.clone())],
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

    let site = if ["e6", "e621", "e621.com"].contains(&scraperdata.job.site.to_lowercase().as_str())
    {
        Site::E6
    } else {
        Site::E6AI
    };
    let mut files = HashSet::new();
    let mut jobs = HashSet::new();
    let mut tags = HashSet::new();
    if let Ok(payload) = json::parse(text_input) {
        if !payload["posts"].is_empty() {
            for post in payload["posts"].members() {
                let mut tag_vec = Vec::new();

                // Extract the post ID
                if let Some(id_val) = post["id"].as_u64() {
                    let file_tag = Tag {
                        name: id_val.to_string(),
                        namespace: nsobjplg(&NsIdent::PostId, &site),
                    };

                    tag_vec.push(PluginTag {
                        tag: file_tag.clone(),
                        ..Default::default()
                    });

                    // Gets the parent object
                    if let Some(parent) = post["relationships"]["parent_id"].as_str() {
                        tags.insert(PluginTag {
                            tag: Tag {
                                name: parent.to_string(),
                                namespace: nsobjplg(&NsIdent::Parent, &site),
                            },
                            relates_to: Some(RelationContext {
                                tag: file_tag.clone(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                    }

                    // Gets children and puts them into the db
                    for child in post["relationships"]["children"].members() {
                        if let Some(child) = child.as_u64() {
                            tags.insert(PluginTag {
                                tag: Tag {
                                    name: child.to_string(),
                                    namespace: nsobjplg(&NsIdent::Children, &site),
                                },
                                relates_to: Some(RelationContext {
                                    tag: file_tag.clone(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            });
                        }
                    }
                }

                // Extract the rating
                if let Some(rating_str) = post["rating"].as_str() {
                    tag_vec.push(PluginTag {
                        tag: Tag {
                            name: rating_str.to_string(),
                            namespace: nsobjplg(&NsIdent::Rating, &site),
                        },
                        ..Default::default()
                    });
                }

                // Extract the post description
                if let Some(desc_str) = post["description"].as_str()
                    && !desc_str.is_empty()
                {
                    tag_vec.push(PluginTag {
                        tag: Tag {
                            name: desc_str.to_string(),
                            namespace: nsobjplg(&NsIdent::Description, &site),
                        },
                        ..Default::default()
                    });
                }

                // Gets all sources from a file
                for source in post["sources"].members() {
                    if let Some(source) = source.as_str() {
                        tag_vec.push(PluginTag {
                            tag: Tag {
                                name: source.to_string(),
                                namespace: nsobjplg(&NsIdent::Sources, &site),
                            },
                            ..Default::default()
                        });
                    }
                }

                for pool in post["pools"].members() {
                    if let Some(pool_id) = pool.as_u64() {
                        if recursion {
                            let parse_url =
                                format!("https://{}.net/pools.json?search[id]={}", site, pool_id);
                            jobs.insert(ScraperDataReturn {
                                job: PluginJob {
                                    site: scraperdata.job.site.clone(),
                                    param: vec![ScraperParam::Url(parse_url)],
                                    priority: DEFAULT_PRIORITY - 2,
                                    ..Default::default()
                                },
                                ..Default::default()
                            });
                        }
                    }
                }

                // Gets all tags from a post
                for (category_name, entry) in post["tags"].entries() {
                    let ns_ident = match category_name {
                        "general" => NsIdent::General,
                        "contributor" => NsIdent::Contributor,
                        "species" => NsIdent::Species,
                        "character" => NsIdent::Character,
                        "copyright" => NsIdent::Copyright,
                        "artist" => NsIdent::Artist,
                        "director" => NsIdent::Director,
                        "franchise" => NsIdent::Franchise,
                        "lore" => NsIdent::Lore,
                        "meta" => NsIdent::Meta,
                        "invalid" => NsIdent::Invalid,
                        _ => continue,
                    };

                    let namespace_obj = nsobjplg(&ns_ident, &site);

                    for raw_tag in entry.members() {
                        if let Some(tag_name) = raw_tag.as_str() {
                            tag_vec.push(PluginTag {
                                tag: Tag {
                                    name: tag_name.to_string(),
                                    namespace: namespace_obj.clone(),
                                },
                                ..Default::default()
                            });
                        }
                    }
                }
                let source = if post["file"]["url"].is_empty() {
                    None
                } else {
                    post["file"]["url"]
                        .as_str()
                        .map(|u| FileSource::Url(u.to_string()))
                };

                // Adds file into db

                let hash = post["file"]["md5"]
                    .as_str()
                    .map(|hash| HashesSupported::Md5(hash.to_string()));

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
        // Used for pools parsing
        } else if payload["posts"].is_empty() {
            for item in payload.members() {
                // Does pool parsing
                if let Some(pool_id) = item["id"].as_u64()
                    && let Some(pool_updated) = item["updated_at"].as_str()
                {
                    let pool_updated =
                        chrono::DateTime::parse_from_str(pool_updated, "%Y-%m-%dT%H:%M:%S.%f%:z")
                            .unwrap()
                            .timestamp()
                            .to_string();

                    let pool_id_tag = Tag {
                        name: format!("{}_{}", pool_id, pool_updated),
                        namespace: nsobjplg(&NsIdent::PoolTimestamp, &site),
                    };

                    let pool_id = Tag {
                        name: pool_id.to_string(),
                        namespace: nsobjplg(&NsIdent::PoolId, &site),
                    };

                    let pool_id_relate = Some(RelationContext {
                        tag: pool_id.clone(),
                        limit_to: Some(pool_id_tag.clone()),
                        ..Default::default()
                    });

                    let pool_id = Some(RelationContext {
                        tag: pool_id,
                        ..Default::default()
                    });

                    // Adds pool name
                    if let Some(pool_name) = item["name"].as_str() {
                        if !pool_name.is_empty() {
                            tags.insert(PluginTag {
                                tag: Tag {
                                    name: pool_name.to_string(),
                                    namespace: nsobjplg(&NsIdent::PoolName, &site),
                                },
                                relates_to: pool_id_relate.clone(),
                                ..Default::default()
                            });
                        }
                    }

                    // Adds pool description
                    if let Some(pool_name) = item["description"].as_str() {
                        if !pool_name.is_empty() {
                            tags.insert(PluginTag {
                                tag: Tag {
                                    name: pool_name.to_string(),
                                    namespace: nsobjplg(&NsIdent::PoolDescription, &site),
                                },
                                relates_to: pool_id_relate.clone(),
                                ..Default::default()
                            });
                        }
                    }

                    // Adds Pool Creation time
                    if let Some(created_at) = item["created_at"].as_str() {
                        tags.insert(PluginTag {
                            tag: Tag {
                                name: chrono::DateTime::parse_from_str(
                                    created_at,
                                    "%Y-%m-%dT%H:%M:%S.%f%:z",
                                )
                                .unwrap()
                                .timestamp()
                                .to_string(),
                                namespace: nsobjplg(&NsIdent::PoolCreatedAt, &site),
                            },
                            relates_to: pool_id.clone(),
                            ..Default::default()
                        });
                    }

                    for (cnt, post_id) in item["post_ids"].members().enumerate() {
                        if let Some(post_id) = post_id.as_u64() {
                            if recursion {
                                let parse_url = format!(
                                    "https://{}.net/posts.json?tags=id:{}",
                                    site.to_string().to_lowercase(),
                                    &post_id
                                );
                                jobs.insert(ScraperDataReturn {
                                    job: PluginJob {
                                        site: scraperdata.job.site.clone(),
                                        param: vec![ScraperParam::Url(parse_url)],
                                        priority: DEFAULT_PRIORITY - 2,
                                        ..Default::default()
                                    },
                                    skip_conditions: vec![SkipIf::FileTagRelationship(Tag {
                                        name: post_id.to_string(),
                                        namespace: nsobjplg(&NsIdent::PostId, &site),
                                    })],
                                });
                            }

                            tags.insert(PluginTag {
                                tag: Tag {
                                    name: cnt.to_string(),
                                    namespace: nsobjplg(&NsIdent::PoolPosition, &site),
                                },
                                relates_to: Some(RelationContext {
                                    tag: Tag {
                                        name: post_id.to_string(),
                                        namespace: nsobjplg(&NsIdent::PostId, &site),
                                    },
                                    limit_to: Some(pool_id_tag.clone()),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            });
                        }
                    }
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

fn nsobjplg(name: &NsIdent, site: &Site) -> GenericNamespaceObj {
    let site_str = site.to_string();

    enum Desc {
        Static(&'static str),
        Dynamic(String),
    }

    let (suffix, desc_type) = match name {
        NsIdent::PoolTimestamp => (
            "Pool_Timestamp",
            Desc::Static("A combination of pool_id and the timestamp of when it was last updated"),
        ),
        NsIdent::Franchise => (
            "Franchise",
            Desc::Static("Franchise that this item came from."),
        ),
        NsIdent::Director => ("Director", Desc::Static("The director of the ai filth.")),
        NsIdent::PoolUpdatedAt => (
            "Pool_Updated_At",
            Desc::Static("Pool When the pool was last updated."),
        ),
        NsIdent::PoolCreatedAt => (
            "Created_At",
            Desc::Static("Pool When the pool was created."),
        ),
        NsIdent::PoolId => ("Pool_Id", Desc::Static("Pool identifier unique id.")),
        NsIdent::PoolCreator => ("Pool_Creator", Desc::Static("Person who made a pool.")),
        NsIdent::PoolCreatorId => (
            "Pool_Creator_ID",
            Desc::Dynamic(format!("Person's id for {} who made a pool.", site_str)),
        ),
        NsIdent::PoolName => ("Pool_Name", Desc::Static("Name of a pool.")),
        NsIdent::PoolDescription => ("Pool_Description", Desc::Static("Description for a pool.")),
        NsIdent::PoolPosition => (
            "Pool_Position",
            Desc::Static("Position of an id in a pool."),
        ),
        NsIdent::General => (
            "General",
            Desc::Dynamic(format!("General namespace for {}.", site_str)),
        ),
        NsIdent::Species => (
            "Species",
            Desc::Dynamic(format!("Species namespace for {}.", site_str)),
        ),
        NsIdent::Character => (
            "Character",
            Desc::Static("What character's are in an image."),
        ),
        NsIdent::Contributor => (
            "Contributor",
            Desc::Static(
                "For those who helped make a piece of art not directly the artist think of VA's and such.",
            ),
        ),
        NsIdent::Copyright => ("Copyright", Desc::Static("Who holds the copyright info")),
        NsIdent::Artist => ("Artist", Desc::Static("Individual who drew the filth.")),
        NsIdent::Lore => (
            "Lore",
            Desc::Static("Youre obviously here for the plot. :X"),
        ),
        NsIdent::Meta => (
            "Meta",
            Desc::Static("Additional information not relating directly to the file"),
        ),
        NsIdent::Sources => ("Sources", Desc::Static("Additional sources for a file.")),
        NsIdent::Children => (
            "Children",
            Desc::Static("Files that have a sub relationship to the current file."),
        ),
        NsIdent::Parent => (
            "Parent_id",
            Desc::Static("Files that are dom or above the current file."),
        ),
        NsIdent::Description => ("Description", Desc::Static("The description of a file.")),
        NsIdent::Invalid => ("Invalid", Desc::Static("An invalid tag")),
        NsIdent::Rating => ("Rating", Desc::Static("The rating of the file.")),
        NsIdent::PostId => (
            "Id",
            Desc::Dynamic(format!(
                "Post id used by {} to uniquly identify a Post.",
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

///
/// Builds local URLs for parsing
///
fn build_url(params: &[shared_types::ScraperParam], pagenum: u64, site: &Site) -> Option<String> {
    if params.is_empty() {
        return None;
    }

    let lowercase_site = site.to_string().to_lowercase();
    let mut url = format!("https://{}.net/posts.json", lowercase_site);

    let login_info = params.iter().find_map(|p| {
        if let shared_types::ScraperParam::Login(shared_types::LoginType::ApiNamespaced(
            _,
            Some(user),
            Some(key),
        )) = p
        {
            Some((user, key))
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

    if let Some((username, api_key)) = login_info {
        url += &format!("?login={}&api_key={}", username, api_key);
        url += &format!("&tags={}", tags.join("+"));
    } else {
        url += &format!("?tags={}", tags.join("+"));
    }

    // 4. Append the pagination tracker at the tail end
    url += &format!("&page={}", pagenum);

    Some(url)
}
