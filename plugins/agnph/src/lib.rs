use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
};

use scraper::{Element, Html, Selector};
use shared_types::{
    DEFAULT_PRIORITY, DownloadModifiers, FileObject, FileSource, FileTagAction,
    GenericNamespaceObj, HashesSupported, LoginNeed, LoginType, PluginJob, PluginProperties,
    PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf, Tag,
    TargetModifier, Url,
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
            PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
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
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    }

    dbg!(&out);

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
        /*    for pagenum in 2..last_page_number {
            if let Some(url) = build_url(&scraperdata.job.param, pagenum) {
                jobs.insert(ScraperDataReturn {
                    job: shared_types::PluginJob {
                        site: scraperdata.job.site.clone(),
                        priority: shared_types::DEFAULT_PRIORITY - 1,
                        param: vec![ScraperParam::Url(Url {
                            url,
                            ..Default::default()
                        })],
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
        }*/
    }

    // Extracts the posts from a page
    let selector = Selector::parse("a.postlink").unwrap();
    for element in doc.select(&selector) {
        if let Some(raw_link) = element.attr("href")
            && let Some(post_id) = raw_link.trim_end_matches('/').rsplit('/').next()
        {
            let mut user_data = BTreeMap::new();
            user_data.insert("post_id".into(), post_id.to_string());
            jobs.insert(ScraperDataReturn {
                job: shared_types::PluginJob {
                    site: scraperdata.job.site.clone(),
                    priority: shared_types::DEFAULT_PRIORITY - 2,
                    param: vec![ScraperParam::Url(Url {
                        url: format!("https://agn.ph/gallery/post/show/{}", post_id),
                        ..Default::default()
                    })],
                    user_data,
                    ..Default::default()
                },
                skip_conditions: vec![SkipIf::FileTagRelationship(Tag {
                    name: post_id.to_string(),
                    namespace: nsobjplg(&NsIdent::PostId, site),
                })],
            });
        }
    }

    // Extracts data from a post

    let selector = Selector::parse("a[id=download-link]").unwrap();

    for download_link in doc.select(&selector) {
        let mut tags = Vec::new();

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
                    tags.push(PluginTag {
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
                tags.push(PluginTag {
                    tag: Tag {
                        name: id_text_cleaned.to_string(),
                        namespace: nsobjplg(&NsIdent::PostId, site),
                    },
                    ..Default::default()
                });
            }
            if let Some(element) = stat_children.next()
                && let Some(source) = element.first_element_child()
                && let Some(source_text) = source.attr("href")
            {
                tags.push(PluginTag {
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
                tags.push(PluginTag {
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
                tags,
            }];

            if let Some(md5_text_dirty) = url.rsplit('/').next()
                && let Some(md5_text) = md5_text_dirty.split('.').next()
            {
                files.insert(FileObject {
                    source: Some(FileSource::Url(format!("https://agn.ph{}", url))),
                    hash: Some(HashesSupported::Md5(md5_text.to_string())),
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
