use std::{collections::HashSet, fmt};

use scraper::{Element, Html, Selector};
use shared_types::{
    DownloadModifiers, FileObject, FileSource, FileTagAction, GenericNamespaceObj, HashesSupported,
    PluginProperties, PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn,
    SkipIf, Tag, TargetModifier, Url,
};

pub enum Site {
    AGNPH,
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
            Site::AGNPH => "Agn.Ph",
        };
        write!(f, "{s}")
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "agn.ph".into(),
        properties: vec![
            PluginProperties::Ratelimit(4, std::time::Duration::from_secs(1)),
            PluginProperties::Sites(vec!["agn.ph".into(), "agn".into()]),
            PluginProperties::Modifier(TargetModifier {
                target: shared_types::ModifierTarget::Text,
                modifier: DownloadModifiers::Cookie((
                    "https://agn.ph".into(),
                    "confirmed_age=true".into(),
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
    let mut out = Vec::new();

    let mut params = scraperdata.job.param.clone();
    params.retain(|f| matches!(f, ScraperParam::Normal(_)));

    if let Some(url) = build_url(&scraperdata.job.param, 1) {
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
pub fn parser_call(
    text_input: &str,
    source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let site = &Site::AGNPH;

    let recursion = scraperdata
        .job
        .user_data
        .get("recursion")
        .is_none_or(|f| f != "false");

    let mut files = HashSet::new();
    let mut jobs = HashSet::new();
    let mut tags = HashSet::new();

    let doc = Html::parse_document(text_input);

    // Gets all pages in a search
    let selector = Selector::parse("span.desktop-only").unwrap();
    if source_url.contains("&page=")
        && let Some(page_num) = source_url.rsplit('=').next()
        && page_num == "1"
        && let Some(page_span) = doc.select(&selector).next()
        && let Some(last_page) = page_span.child_elements().nth(4)
        && let Some(last_page_string) = last_page.text().next()
        && let Ok(last_page_number) = last_page_string.parse::<u64>()
    {
        for pagenum in 2..last_page_number {
            if let Some(url) = build_url(&scraperdata.job.param, pagenum) {
                jobs.insert(ScraperDataReturn {
                    job: shared_types::PluginJob {
                        site: scraperdata.job.site.clone(),
                        priority: shared_types::DEFAULT_PRIORITY - 1,
                        param: vec![ScraperParam::Url(Url {
                            url,
                            ..Default::default()
                        })],
                        user_data: scraperdata.job.user_data.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
        }
    }

    let mut user_data = scraperdata.job.user_data.clone();
    // Extracts the posts from a page
    let selector = Selector::parse("a.postlink").unwrap();
    let elements: Vec<_> = doc.select(&selector).collect();
    for (idx, element) in elements.iter().enumerate() {
        if let Some(raw_link) = element.attr("href")
            && let Some(post_id) = raw_link.trim_end_matches('/').rsplit('/').next()
        {
            if source_url.contains("search=pool")
                && let Some(pool_name) = source_url.rsplit("%3A").next()
            {
                user_data.insert("pool_position".into(), idx.to_string());
                user_data.insert("pool_title".into(), pool_name.to_string());
            }

            // Always run the last item in a pool to grab it
            let skip_conditions =
                if (elements.len() - 1) == idx && source_url.contains("search=pool") && recursion {
                    vec![]
                } else {
                    user_data.insert("recursion".into(), "false".into());
                    vec![SkipIf::FileTagRelationship(Tag {
                        name: post_id.to_string(),
                        namespace: nsobjplg(&NsIdent::PostId, site),
                    })]
                };

            user_data.insert("post_id".into(), post_id.to_string());
            jobs.insert(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: shared_types::DEFAULT_PRIORITY - 2,
                    param: vec![ScraperParam::Url(Url {
                        url: format!("https://agn.ph/gallery/post/show/{}", post_id),
                        ..Default::default()
                    })],
                    user_data: user_data.clone(),
                    ..Default::default()
                },
                skip_conditions,
            });
        }
    }

    // Gets next item in pool if it exists
    let selector = Selector::parse("a[id=nextinpool]").unwrap();
    let mut next_pool_page = doc.select(&selector);

    if let Some(next_pool_page) = next_pool_page.next()
        && let Some(next_page_link) = next_pool_page.attr("href")
    {
        let selector = Selector::parse("a[id=previnpool]").unwrap();
        let elements: Vec<_> = doc.select(&selector).collect();
        if elements.is_empty() {
            user_data.insert("pool_position".into(), 1.to_string());
            if let Some(pool_p) = next_pool_page.parent_element()
                && let Some(gallery_link) = pool_p.first_element_child()
                && let Some(gallery_text) = gallery_link.text().next()
            {
                user_data.insert("pool_title".into(), gallery_text.to_string());
            }
        } else {
            // Updates the pool position
            if let Some(pool_pos_text) = user_data.get_mut("pool_position")
                && let Ok(mut pool_pos) = pool_pos_text.parse::<u64>()
            {
                pool_pos += 1;

                *pool_pos_text = pool_pos.to_string();
            }
        }

        jobs.insert(ScraperDataReturn {
            job: shared_types::PluginJob {
                site: scraperdata.job.site.clone(),
                priority: shared_types::DEFAULT_PRIORITY - 3,
                param: vec![ScraperParam::Url(Url {
                    url: format!("https://agn.ph/{}", next_page_link),
                    ..Default::default()
                })],
                user_data: user_data.clone(),
                ..Default::default()
            },
            skip_conditions: vec![],
        });
        if elements.is_empty() {
            user_data.insert("pool_position".into(), 0.to_string());
        }
    }

    // Extracts data from a post
    let selector = Selector::parse("a[id=download-link]").unwrap();

    for download_link in doc.select(&selector) {
        let mut file_tags = Vec::new();

        // Gets tags from the site
        for (selector_text, namespace) in [
            ("a.mtypetag", &nsobjplg(&NsIdent::General, site)),
            ("a.dtypetag", &nsobjplg(&NsIdent::Species, site)),
            ("a.ctypetag", &nsobjplg(&NsIdent::Character, site)),
            ("a.atypetag", &nsobjplg(&NsIdent::Artist, site)),
        ] {
            let selector = Selector::parse(selector_text).unwrap();
            for element in doc.select(&selector) {
                if let Some(tag_text) = element.text().next() {
                    file_tags.push(PluginTag {
                        tag: Tag {
                            name: tag_text.to_string(),
                            namespace: namespace.clone(),
                        },
                        ..Default::default()
                    });
                }
            }
        }

        // Extracts the items from statistics panel
        let selector = Selector::parse("ul.statlist").unwrap();
        let mut stat_list = doc.select(&selector);

        if let Some(stat_list) = stat_list.next() {
            let mut stat_children = stat_list.child_elements();
            if let Some(element) = stat_children.next()
                && let Some(id_text) = element.text().next()
                && let Some(id_text_cleaned) = id_text.rsplit(':').next()
            {
                user_data.insert("post_id".into(), id_text_cleaned.into());
                file_tags.push(PluginTag {
                    tag: Tag {
                        name: id_text_cleaned.to_string(),
                        namespace: nsobjplg(&NsIdent::PostId, site),
                    },
                    ..Default::default()
                });

                if let Some(pool_position_unparsed) = user_data.get("pool_position")
                    && let Ok(pool_position) = pool_position_unparsed.parse::<u64>()
                    && let Some(pool_title) = user_data.clone().get("pool_title")
                {
                    let mut local_pool_position = pool_position.clone();
                    local_pool_position += 1;

                    user_data.insert("pool_position".into(), local_pool_position.to_string());

                    tags.insert(PluginTag {
                        tag: Tag {
                            name: pool_position.to_string(),
                            namespace: nsobjplg(&NsIdent::PoolPosition, site),
                        },
                        relates_to: Some(RelationContext {
                            tag: Tag {
                                name: id_text_cleaned.to_string(),
                                namespace: nsobjplg(&NsIdent::PostId, site),
                            },
                            limit_to: Some(Tag {
                                name: pool_title.to_string(),
                                namespace: nsobjplg(&NsIdent::PoolTitle, site),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }
            }
            if let Some(element) = stat_children.next()
                && let Some(source) = element.first_element_child()
                && let Some(source_text) = source.attr("href")
            {
                file_tags.push(PluginTag {
                    tag: Tag {
                        name: source_text.to_string(),
                        namespace: nsobjplg(&NsIdent::Sources, site),
                    },
                    ..Default::default()
                });
            }
            stat_children.next();
            if let Some(element) = stat_children.next()
                && let Some(rating) = element.first_element_child()
                && let Some(rating_text) = rating.text().next()
            {
                file_tags.push(PluginTag {
                    tag: Tag {
                        name: rating_text.to_string(),
                        namespace: nsobjplg(&NsIdent::Rating, site),
                    },
                    ..Default::default()
                });
            }
        }

        // Adds file to db
        if let Some(url) = download_link.attr("href") {
            let tag_list = vec![FileTagAction {
                operation: shared_types::TagOperation::Set,
                tags: file_tags,
            }];

            if let Some(md5_text_dirty) = url.rsplit('/').next()
                && let Some(md5_text) = md5_text_dirty.split('.').next()
            {
                files.insert(FileObject {
                    source: Some(FileSource::Url(format!("https://agn.ph{}", url))),
                    hash: vec![HashesSupported::Md5(md5_text.to_string())],
                    tag_list,
                    skip_if: vec![SkipIf::FileTagRelationship(Tag {
                        name: md5_text.to_string(),
                        namespace: GenericNamespaceObj {
                            name: "FileHash-MD5".to_string(),
                            description: Some(
                                "From plugin FileHash. MD5 hash of the file.".to_string(),
                            ),
                        },
                    })],
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
        NsIdent::Species => ("Species", Desc::Static("Species involved in post.")),
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
        NsIdent::PoolPosition => (
            "PoolPosition",
            Desc::Static("A position of a post in a pool"),
        ),
        NsIdent::PoolTitle => ("PoolTitle", Desc::Static("The title of a pool")),
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

fn build_url(params: &[shared_types::ScraperParam], pagenum: u64) -> Option<String> {
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
        "https://agn.ph/gallery/post/?search={}&page={}",
        tags.join("+"),
        pagenum
    );

    Some(url)
}
