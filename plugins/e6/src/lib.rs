use std::{collections::HashSet, fmt};

use shared_types::{
    DEFAULT_PRIORITY, DownloadModifiers, FileObject, FileSource, FileTagAction, GenericNamespaceObj,
    GlobalCallbacks, HashesSupported, LoginNeed, LoginType, ModifierTarget, PluginJob, PluginProperties,
    PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf,
    StartupThreadType, TargetModifier, Tag,
};

mod csv_import;

pub enum Site {
    E6,
    E6AI,
    Blep,
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
            Site::Blep => "Blep",
        };
        write!(f, "{s}")
    }
}

impl Site {
    /// Resolves a job's `site` field to a site. Anything that is not
    /// explicitly `e621` or `blep` falls back to `E6AI`, matching the
    /// historical two-site default.
    fn from_job_site(job_site: &str) -> Site {
        match job_site.to_lowercase().as_str() {
            "e6" | "e621" | "e621.com" => Site::E6,
            "blep" | "blep.art" => Site::Blep,
            _ => Site::E6AI,
        }
    }

    /// API host for `posts.json` / `pools.json`. Note `blep.art` is
    /// `.art` where e621/e6ai are `.net`, so this cannot be derived
    /// from `Display` by lowercasing.
    fn host(&self) -> &'static str {
        match self {
            Site::E6 => "e621.net",
            Site::E6AI => "e6ai.net",
            Site::Blep => "blep.art",
        }
    }

    /// CDN host the API's `file.url` points at, used for the
    /// no-`file.url` fallback data URL. e621/e6ai serve
    /// `static1.<site>.net`; blep serves `cdn.blep.art`.
    fn cdn_host(&self) -> &'static str {
        match self {
            Site::E6 => "static1.e621.net",
            Site::E6AI => "static1.e6ai.net",
            Site::Blep => "cdn.blep.art",
        }
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> shared_types::CVec<u8> {
    // The host consumes this via `plugin_ffi::take` (see `src/plugins.rs`):
    // a bitcode encoding of `Vec<Plugin>` in an owned byte buffer.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(get_plugin_info_impl));
    match result {
        Ok(value) => shared_types::plugin_ffi::encode(&value),
        Err(_) => shared_types::plugin_ffi::encode(&Vec::<shared_types::Plugin>::new()),
    }
}

fn get_plugin_info_impl() -> Vec<shared_types::Plugin> {
    // Every entry declares the Start callback so `on_start` runs no matter
    // which site the user actually uses; `csv_import::run` guards itself so
    // a repeat invocation (one per entry) is a no-op. The CSV dump import
    // itself is per-site: each site reads its own PLUGIN_<SITE>_CsvImport_*
    // settings, so E621, E6AI and Blep dumps can be enabled independently.
    let csv_import_callbacks = vec![GlobalCallbacks::Start(StartupThreadType::Spawn)];
    vec![
        shared_types::Plugin {
            name: "E621".into(),
            properties: vec![
                PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
                PluginProperties::Sites(vec!["e6".into(), "E621".into(), "e621.com".into()]),
                PluginProperties::JobNum(5),
                PluginProperties::Login((LoginNeed::Optional, LoginType::Api("Put API key first then username. Can find more info here: https://e621.net/api_keys".into(), None) ))
            ],
            callbacks: csv_import_callbacks.clone(),
        },
        shared_types::Plugin {
            name: "E6AI".into(),
            properties: vec![
                PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
                PluginProperties::JobNum(20),
                PluginProperties::Sites(vec!["e6ai".into(), "E6AI".into(), "e6ai.com".into()]),
                PluginProperties::Modifier(TargetModifier {
                    target: ModifierTarget::Text,
                    modifier: DownloadModifiers::Timeout(Some(std::time::Duration::from_secs(30))),
                }),
                PluginProperties::Login((LoginNeed::Optional, LoginType::Api("Put API key first then username.".into(), None) ))
            ],
            callbacks: csv_import_callbacks.clone(),
        },
        shared_types::Plugin {
            name: "Blep".into(),
            properties: vec![
                PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
                PluginProperties::JobNum(5),
                PluginProperties::Sites(vec!["blep".into(), "Blep".into(), "blep.art".into()]),
                PluginProperties::Login((LoginNeed::Optional, LoginType::Api("Put API key first then username.".into(), None) ))
            ],
            callbacks: csv_import_callbacks,
        },
    ]
}

/// Runs the (setting-gated, per-site) CSV dump import. See csv_import.rs.
#[unsafe(no_mangle)]
pub fn on_start() {
    csv_import::run();
}

#[unsafe(no_mangle)]
/// # Safety
///
/// Called by the host via `libloading` with a bitcode-encoded buffer
/// that outlives the call (see `shared_types::plugin_ffi::decode`);
/// `req..req + req_len` must be valid for the call. The wrapper
/// null-checks, decodes inside `catch_unwind`, and returns an empty
/// result on failure instead of panicking.
pub unsafe extern "C" fn url_dump(req: *const u8, req_len: usize) -> shared_types::CVec<u8> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Some(data) = (unsafe {
            shared_types::plugin_ffi::decode::<shared_types::ScraperDataReturn>(req, req_len)
        }) else {
            return Vec::new();
        };
        url_dump_impl(&data)
    }));
    match result {
        Ok(value) => shared_types::plugin_ffi::encode(&value),
        Err(_) => shared_types::plugin_ffi::encode(&Vec::<shared_types::ScraperDataReturn>::new()),
    }
}

#[unsafe(no_mangle)]
pub fn url_dump_impl(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();

    let site = Site::from_job_site(&scraperdata.job.site);

    let hardlimit = 751;

    let normal_params: Vec<_> = scraperdata
        .job
        .param
        .iter()
        .filter(|f| matches!(f, ScraperParam::Normal(_)))
        .cloned()
        .collect();
    let url_params: Vec<_> = scraperdata
        .job
        .param
        .iter()
        .filter(|f| matches!(f, ScraperParam::Url(_)))
        .cloned()
        .collect();
    let login_params: Vec<_> = scraperdata
        .job
        .param
        .iter()
        .filter(|f| matches!(f, ScraperParam::Login(_)))
        .cloned()
        .collect();

    if !normal_params.is_empty() {
        for i in 1..hardlimit {
            let mut temp_params = Vec::new();
            temp_params.extend(normal_params.clone());
            temp_params.extend(login_params.clone());
            let url = build_url(&temp_params, Some(i), &site);

            if let Some(url) = url {
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
    }

    // Handles URL passthrough
    for param in url_params.iter() {
        let mut temp_params = vec![param.clone()];
        temp_params.extend(login_params.clone());
        out.push(ScraperDataReturn {
            job: shared_types::PluginJob {
                site: scraperdata.job.site.clone(),
                priority: shared_types::DEFAULT_PRIORITY - 2,
                param: temp_params,
                ..Default::default()
            },
            ..Default::default()
        });
    }

    out
}

#[unsafe(no_mangle)]
/// # Safety
///
/// Called by the host via `libloading` with a bitcode-encoded buffer
/// that outlives the call (see `shared_types::plugin_ffi::decode`);
/// `req..req + req_len` must be valid for the call. The wrapper
/// null-checks, decodes inside `catch_unwind`, and returns an empty
/// result on failure instead of panicking.
pub unsafe extern "C" fn parser_call(req: *const u8, req_len: usize) -> shared_types::CVec<u8> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Some(input) = (unsafe {
            shared_types::plugin_ffi::decode::<shared_types::ParserCallInput>(req, req_len)
        }) else {
            return vec![shared_types::ScraperReturn::Stop(
                "undecodable parser_call request".to_string(),
            )];
        };
        parser_call_impl(&input.text, &input.source_url, &input.data)
    }));
    match result {
        Ok(value) => shared_types::plugin_ffi::encode(&value),
        Err(_) => shared_types::plugin_ffi::encode(&vec![shared_types::ScraperReturn::Stop(
            "plugin panicked".to_string(),
        )]),
    }
}

#[unsafe(no_mangle)]
pub fn parser_call_impl(
    text_input: &str,
    source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    if scraperdata
        .job
        .user_data
        .contains_key("INTSCRAPE_REDOWNLOAD")
    {
        return shared_types::helpers::redownload_return(source_url, &scraperdata.job.user_data)
            .unwrap_or_else(|| vec![ScraperReturn::Nothing]);
    }

    let recursion = shared_types::helpers::is_recursion(&scraperdata.job.user_data);

    let site = Site::from_job_site(&scraperdata.job.site);

    // Adds the posts from pool parsing

    let mut files = HashSet::new();
    let mut jobs = HashSet::new();
    let mut tags = HashSet::new();

    // Parents and children parsing
    let mut posts_to_search = Vec::new();

    let login_params: Vec<_> = scraperdata
        .job
        .param
        .iter()
        .filter(|f| matches!(f, ScraperParam::Login(_)))
        .cloned()
        .collect();

    let is_logged_in = !login_params.is_empty();

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
                    if let Some(parent) = post["relationships"]["parent_id"].as_u64() {
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

                        // Adds job to get parent
                        if recursion {
                            posts_to_search.push(parent);
                        }
                    }

                    // Gets children and puts them into the db
                    for child in post["relationships"]["children"].members() {
                        if let Some(child) = child.as_u64() {
                            if recursion {
                                posts_to_search.push(child);
                            }
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
                    if let Some(pool_id) = pool.as_u64()
                        && recursion
                    {
                        let parse_url =
                            format!("https://{}/pools.json?search[id]={}", site.host(), pool_id);
                        jobs.insert(ScraperDataReturn {
                            job: PluginJob {
                                site: scraperdata.job.site.clone(),
                                param: vec![ScraperParam::Url(shared_types::Url {
                                    url: parse_url,
                                    ..Default::default()
                                })],
                                priority: DEFAULT_PRIORITY - 2,
                                ..Default::default()
                            },
                            ..Default::default()
                        });
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

                // Only attempt to bypass the source url IF we're not logged in
                let md5 = post["file"]["md5"].as_str();
                // The static URL path is case-sensitive on the CDN, so the
                // fallback URL uses the lowercase digest. The
                // `HashesSupported` emitted below intentionally stays
                // site-verbatim: the main software normalizes it on ingestion.
                let md5_lower = md5.map(|s| s.trim().to_ascii_lowercase());
                let source = post["file"]["url"]
                    .as_str()
                    .map(|u| FileSource::Url(u.to_string()))
                    .or_else(|| {
                        let hash = md5_lower.as_deref()?;
                        let extension = post["file"]["ext"].as_str()?;

                        if is_logged_in {
                            return None;
                        }

                        // MD5 must contain at least four ASCII characters.
                        if hash.len() < 4
                            || !hash.is_ascii()
                            || !hash.chars().all(|character| character.is_ascii_hexdigit())
                        {
                            return None;
                        }

                        let static_url = format!(
                            "https://{}/data/{}/{}/{}.{}",
                            site.cdn_host(),
                            &hash[0..2],
                            &hash[2..4],
                            hash,
                            extension,
                        );

                        Some(FileSource::Url(static_url))
                    });

                // Adds file into db
                if let Some(hash) = md5 {
                    files.insert(FileObject {
                        source,
                        hash: vec![HashesSupported::Md5(hash.to_string())],
                        tag_list: vec![FileTagAction {
                            tags: tag_vec,
                            ..Default::default()
                        }],
                        // Skip file if we have the md5 hash inside the db
                        skip_if: vec![],
                    });
                }
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
                    if let Some(pool_name) = item["name"].as_str()
                        && !pool_name.is_empty()
                    {
                        tags.insert(PluginTag {
                            tag: Tag {
                                name: pool_name.to_string(),
                                namespace: nsobjplg(&NsIdent::PoolName, &site),
                            },
                            relates_to: pool_id_relate.clone(),
                            ..Default::default()
                        });
                    }

                    // Adds pool description
                    if let Some(pool_name) = item["description"].as_str()
                        && !pool_name.is_empty()
                    {
                        tags.insert(PluginTag {
                            tag: Tag {
                                name: pool_name.to_string(),
                                namespace: nsobjplg(&NsIdent::PoolDescription, &site),
                            },
                            relates_to: pool_id_relate.clone(),
                            ..Default::default()
                        });
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
                                let mut local_params: Vec<_> = login_params.to_vec();
                                local_params.push(ScraperParam::Normal(format!("id:{}", post_id)));
                                if let Some(parse_url) = build_url(&local_params, None, &site) {
                                    let mut user_data = scraperdata.job.user_data.clone();
                                    user_data.insert("recursion".into(), "false".into());

                                    jobs.insert(ScraperDataReturn {
                                        job: PluginJob {
                                            site: scraperdata.job.site.clone(),
                                            param: vec![ScraperParam::Url(shared_types::Url {
                                                url: parse_url,
                                                ..Default::default()
                                            })],
                                            priority: DEFAULT_PRIORITY - 2,
                                            user_data,
                                            ..Default::default()
                                        },
                                        skip_conditions: vec![SkipIf::FileTagRelationship(Tag {
                                            name: post_id.to_string(),
                                            namespace: nsobjplg(&NsIdent::PostId, &site),
                                        })],
                                    });
                                }
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

    // Gets only posts that dont have a file attached to it
    posts_to_search.retain(|post_id| {
        client::get_tag_file(Tag {
            name: post_id.to_string(),
            namespace: nsobjplg(&NsIdent::PostId, &site),
        })
        .ok()
        .is_some_and(|f| f.is_none())
    });

    // Adds posts to search for individually
    if !posts_to_search.is_empty() {
        for chunk in posts_to_search.chunks(10) {
            let mut local_params: Vec<_> = login_params.to_vec();
            //           local_params.push(ScraperParam::Normal(format!("~id:{}", post_id)));
            chunk.iter().for_each(|post_id| {
                local_params.push(ScraperParam::Normal(format!("~id:{}", post_id)));
            });
            local_params.push(ScraperParam::Normal("+status:any".into()));

            let mut user_data = scraperdata.job.user_data.clone();
            user_data.insert("recursion".into(), "false".into());

            if recursion && let Some(url) = build_url(&local_params, None, &site) {
                jobs.insert(ScraperDataReturn {
                    job: PluginJob {
                        site: scraperdata.job.site.clone(),
                        param: vec![ScraperParam::Url(shared_types::Url {
                            url,
                            ..Default::default()
                        })],
                        priority: DEFAULT_PRIORITY - 2,
                        user_data,

                        ..Default::default()
                    },
                    skip_conditions: vec![],
                });
            }
        }
    }

    shared_types::helpers::finalize(files, tags, jobs)
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
fn build_url(
    params: &[shared_types::ScraperParam],
    pagenum: Option<u64>,
    site: &Site,
) -> Option<String> {
    if params.is_empty() {
        return None;
    }

    let mut url = format!("https://{}/posts.json", site.host());

    let login_info = params.iter().find_map(|p| {
        if let shared_types::ScraperParam::Login(shared_types::LoginType::Api(user, Some(key))) = p
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
        url += &format!("?api_key={}&login={}", username, api_key);
        url += &format!("&tags={}", tags.join("+"));
    } else {
        url += &format!("?tags={}", tags.join("+"));
    }

    if let Some(pagenum) = pagenum {
        url += &format!("&page={}", pagenum);
    }

    Some(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_types::ScraperParam;

    #[test]
    fn site_resolution_and_hosts() {
        for alias in ["e6", "e621", "e621.com", "E621"] {
            assert!(matches!(Site::from_job_site(alias), Site::E6), "{alias}");
        }
        for alias in ["e6ai", "E6AI", "e6ai.com", "unknown"] {
            assert!(
                matches!(Site::from_job_site(alias), Site::E6AI),
                "{alias}"
            );
        }
        for alias in ["blep", "Blep", "blep.art", "BLEP.ART"] {
            assert!(
                matches!(Site::from_job_site(alias), Site::Blep),
                "{alias}"
            );
        }
        assert_eq!(Site::E6.host(), "e621.net");
        assert_eq!(Site::E6AI.host(), "e6ai.net");
        assert_eq!(Site::Blep.host(), "blep.art");
        assert_eq!(Site::E6.cdn_host(), "static1.e621.net");
        assert_eq!(Site::E6AI.cdn_host(), "static1.e6ai.net");
        assert_eq!(Site::Blep.cdn_host(), "cdn.blep.art");
        assert_eq!(Site::Blep.to_string(), "Blep");
    }

    #[test]
    fn blep_build_url_and_namespaces() {
        let params = vec![ScraperParam::Normal("anthro".into())];
        assert_eq!(
            build_url(&params, Some(2), &Site::Blep).as_deref(),
            Some("https://blep.art/posts.json?tags=anthro&page=2")
        );
        assert_eq!(
            build_url(&params, None, &Site::E6).as_deref(),
            Some("https://e621.net/posts.json?tags=anthro")
        );
        assert_eq!(
            build_url(&params, None, &Site::E6AI).as_deref(),
            Some("https://e6ai.net/posts.json?tags=anthro")
        );
        assert_eq!(
            nsobjplg(&NsIdent::PostId, &Site::Blep).name,
            "Blep_Id"
        );
        assert_eq!(
            nsobjplg(&NsIdent::General, &Site::Blep).name,
            "Blep_General"
        );
    }
}
