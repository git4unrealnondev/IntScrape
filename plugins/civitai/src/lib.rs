use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashSet;

use shared_types::*;

const SITE_URL_BASE: &str = "civitai.red";

#[macro_export]
macro_rules! vec_of_strings {
    ($($x:expr),*) => (vec![$($x.to_string()),*]);
}

pub enum NsIdent {
    CivitModelId,
    CivitModelVers,
    CivitModelVersName,
    CivitModelName,
    CivitModelDescription,
    CivitModelType,
    CivitModelTags,
    CivitModelUploadTimestamp,
    CivitModelBaseModelName,
    CivitModelTrainedWords,

    CivitPostId,
    CivitPostTitle,

    CivitImageId,
    CivitImageCreatedAt,
    CivitImagePosition,
    CivitImageMD5,
    CivitImageTags,
    CivitImagePrompt,
    CivitImagePromptNegative,
    CivitImageMetadata,
    CivitImageResources,

    CivitCommentId,
    CivitCommentCreationTimestamp,
    CivitCommentContent,

    CivitUserId,
    CivitUserName,
    CivitUserLinks,
    DONOTPARSE,
}

fn nsobjplg(name: &NsIdent) -> GenericNamespaceObj {
    match name {
        NsIdent::DONOTPARSE => GenericNamespaceObj {
            name: "DONOTPARSE".to_string(),
            description: Some(
                "Should never be parsed used only for passing new urls to scrape".to_string(),
            ),
        },
        NsIdent::CivitImageId => GenericNamespaceObj {
            name: "CivitImageId".to_string(),
            description: Some(
                "Used by civitai to identify the image. Unique per upload to site.".to_string(),
            ),
        },
        NsIdent::CivitImageCreatedAt => GenericNamespaceObj {
            name: "CivitImageCreatedAt".to_string(),
            description: Some("When an image was created on the site.".to_string()),
        },
        NsIdent::CivitImageMD5 => GenericNamespaceObj {
            name: "CivitImageMd5".to_string(),
            description: Some("Hash of file uploaded stored in compressed MD5.".to_string()),
        },
        NsIdent::CivitImageTags => GenericNamespaceObj {
            name: "CivitFileTags".to_string(),
            description: Some("Tags used by civit to describe a file".to_string()),
        },
        NsIdent::CivitImagePrompt => GenericNamespaceObj {
            name: "CivitImagePrompt".to_string(),
            description: Some("Civit: Prompt used to generate an image.".to_string()),
        },
        NsIdent::CivitImagePromptNegative => GenericNamespaceObj {
            name: "CivitImagePromptNegative".to_string(),
            description: Some(
                "Civit: Negative items to help generate an image properly.".to_string(),
            ),
        },
        NsIdent::CivitImageMetadata => GenericNamespaceObj {
            name: "CivitImageMetadata".to_string(),
            description: Some("Civit: Metadata about image".to_string()),
        },
        NsIdent::CivitImageResources => GenericNamespaceObj {
            name: "CivitImageResources".to_string(),
            description: Some("Resources that were used to generate an image".to_string()),
        },
        NsIdent::CivitCommentId => GenericNamespaceObj {
            name: "CivitCommentId".to_string(),
            description: Some("The ID of a comment from civitai".to_string()),
        },
        NsIdent::CivitCommentContent => GenericNamespaceObj {
            name: "CivitCommentContent".to_string(),
            description: Some("The text from an comment".to_string()),
        },
        NsIdent::CivitCommentCreationTimestamp => GenericNamespaceObj {
            name: "CivitCommentCreationTimestamp".to_string(),
            description: Some("The timestamp of when the comment was posted".to_string()),
        },
        NsIdent::CivitUserId => GenericNamespaceObj {
            name: "CivitUserId".to_string(),
            description: Some("The unique ID of a user".to_string()),
        },
        NsIdent::CivitUserName => GenericNamespaceObj {
            name: "CivitUserName".to_string(),
            description: Some("The Username of a user from civit".to_string()),
        },
        NsIdent::CivitUserLinks => GenericNamespaceObj {
            name: "CivitUserLinks".to_string(),
            description: Some("Links to other user's page".to_string()),
        },
        NsIdent::CivitModelId => GenericNamespaceObj {
            name: "CivitModelId".to_string(),
            description: Some("Civit unique model of ai gen".to_string()),
        },
        NsIdent::CivitModelTrainedWords => GenericNamespaceObj {
            name: "CivitModelTrainedWords".to_string(),
            description: Some("unique words that change the output? of the image".to_string()),
        },
        NsIdent::CivitModelTags => GenericNamespaceObj {
            name: "CivitModelTags".to_string(),
            description: Some("Tags used by civit to describe a AI Model".to_string()),
        },

        NsIdent::CivitModelBaseModelName => GenericNamespaceObj {
            name: "CivitModelBaseModelName".to_string(),
            description: Some("The base models name. IE SDXL 1.0 points to SDXL".to_string()),
        },

        NsIdent::CivitModelDescription => GenericNamespaceObj {
            name: "CivitModelDescription".to_string(),
            description: Some("The Desicription of the model".to_string()),
        },
        NsIdent::CivitModelUploadTimestamp => GenericNamespaceObj {
            name: "CivitModelUploadTimestamp".to_string(),
            description: Some("The timestamp in which the model was uploaded to civit".to_string()),
        },

        NsIdent::CivitModelVers => GenericNamespaceObj {
            name: "CivitModelVers".to_string(),
            description: Some("The version of the model being used".to_string()),
        },
        NsIdent::CivitModelVersName => GenericNamespaceObj {
            name: "CivitModelVersName".to_string(),
            description: Some("The versions name".to_string()),
        },

        NsIdent::CivitModelName => GenericNamespaceObj {
            name: "CivitModelName".to_string(),
            description: Some("Civit Name of model".to_string()),
        },
        NsIdent::CivitModelType => GenericNamespaceObj {
            name: "CivitModelType".to_string(),
            description: Some("Civit type of model".to_string()),
        },
        NsIdent::CivitPostId => GenericNamespaceObj {
            name: "CivitPostId".to_string(),
            description: Some("The post id of a set of images".to_string()),
        },
        NsIdent::CivitPostTitle => GenericNamespaceObj {
            name: "CivitPostTitle".to_string(),
            description: Some("The post's title.".to_string()),
        },
        NsIdent::CivitImagePosition => GenericNamespaceObj {
            name: "CivitImagePosition".to_string(),
            description: Some("The image's position in the pool".to_string()),
        }, /*
                       => GenericNamespaceObj {
           name: "".to_string(),
                           description: Some("".to_string())
                       }*/
    }
}

fn md5_decode(inp: &str) -> String {
    let attachment_md5 = hex::encode(base64::prelude::BASE64_STANDARD.decode(inp).unwrap());
    attachment_md5
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<Plugin> {
    vec![Plugin {
        name: "civitai".into(),
        properties: vec![
            PluginProperties::Ratelimit(10, std::time::Duration::from_secs(1)),
            PluginProperties::Sites(vec!["civitai".into(), "{SITE_URL_BASE}".into()]),
            PluginProperties::ThreadNum(1000),
            // Needed if scraping NSFW images or searching
            PluginProperties::Login((
                LoginNeed::Required,
                LoginType::Cookie(
                    "Authorization".into(),
                    Some("Get the Authorization header from your login session. \nNeeded to extract this cookie from your browser.".into()),
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

    let mut normals = Vec::new();
    let mut urls = Vec::new();
    let mut logins = Vec::new();

    for param in scraperdata.job.param.clone() {
        match param {
            ScraperParam::Normal(val) => normals.push(val),
            ScraperParam::Url(val) => urls.push(val),
            ScraperParam::Login(logintype) => logins.push(logintype),
            // Handle other variants if any exist
            _ => {}
        }
    }

    for offset in (0..1_000_000).step_by(50) {
        if !normals.is_empty() {
            let query = normals.join(" ");

            let payload = MultiSearchPayload {
                queries: vec![SearchQuery {
                    q: query,
                    index_uid: "images_v6".into(),
                    facets: vec![
                        "aspectRatio".into(),
                        "baseModel".into(),
                        "createdAtUnix".into(),
                        "tagNames".into(),
                        "techniqueNames".into(),
                        "toolNames".into(),
                        "type".into(),
                        "user.username".into(),
                    ],
                    attributes_to_highlight: vec![],
                    highlight_pre_tag: "__ais-highlight__".into(),
                    highlight_post_tag: "__/ais-highlight__".into(),
                    limit: 51,
                    offset,
                    filter: vec![],
                }],
            };

            let mut local_modifiers = Vec::new();

            for login in logins.iter() {
                if let LoginType::Cookie(cookie, val) = login {
                    if cookie == "Authorization"
                        && let Some(auth_val) = val
                    {
                        local_modifiers.push(TargetModifier {
                            target: ModifierTarget::Text,
                            modifier: DownloadModifiers::Header((
                                cookie.to_string(),
                                auth_val.to_string(),
                            )),
                        });
                    }
                }
            }

            local_modifiers.push(TargetModifier {
                target: ModifierTarget::Text,
                modifier: DownloadModifiers::Header((
                    "Origin".into(),
                    "https://civitai.red".into(),
                )),
            });
            local_modifiers.push(TargetModifier {
                target: ModifierTarget::Text,
                modifier: DownloadModifiers::Header((
                    "Referer".into(),
                    "https://civitai.red".into(),
                )),
            });
            local_modifiers.push(TargetModifier {
                target: ModifierTarget::Text,
                modifier: DownloadModifiers::Header((
                    "content-type".into(),
                    "application/json".into(),
                )),
            });

            let mut job = scraperdata.job.clone();
            job.param = vec![ScraperParam::UrlPost(UrlPost {
                url: "https://search-new.civitai.com/multi-search".into(),
                local_modifiers,
                post_data: serde_json::to_string(&payload).unwrap(),
            })];

            out.push(ScraperDataReturn {
                job,
                ..Default::default()
            });
        }
    }

    let mut user_data = scraperdata.job.user_data.clone();

    for url_struct in urls.iter() {
        extract_url_parameters(&url_struct.url, &mut user_data);
    }

    out
}

#[unsafe(no_mangle)]
pub fn parser_call(
    text_input: &str,
    _source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let mut out = Vec::new();

    if let Ok(json) = json::parse(text_input) {
        let mut files = HashSet::new();
        let mut system_tags = HashSet::new();
        for item in json["results"].members() {
            for post in item["hits"].members() {
                let mut tags = Vec::new();
                let mut file = None;
                if let Some(id) = post["id"].as_u64() {
                    tags.push(PluginTag {
                        tag: Tag {
                            name: id.to_string(),
                            namespace: nsobjplg(&NsIdent::CivitImageId),
                        },
                        ..Default::default()
                    });

                    // Checks to see if theirs a user_id
                    if let Some(user_id) = post["user"]["id"].as_u64() {
                        system_tags.insert(PluginTag {
                            tag: Tag {
                                name: id.to_string(),
                                namespace: nsobjplg(&NsIdent::CivitImageId),
                            },
                            relates_to: Some(RelationContext {
                                tag: Tag {
                                    name: user_id.to_string(),
                                    namespace: nsobjplg(&NsIdent::CivitUserId),
                                },
                                ..Default::default()
                            }),
                            ..Default::default()
                        });

                        // Associates username to user_id
                        if let Some(user_name) = post["user"]["username"].as_str() {
                            system_tags.insert(PluginTag {
                                tag: Tag {
                                    name: user_name.to_string(),
                                    namespace: nsobjplg(&NsIdent::CivitUserName),
                                },
                                relates_to: Some(RelationContext {
                                    tag: Tag {
                                        name: user_id.to_string(),
                                        namespace: nsobjplg(&NsIdent::CivitUserId),
                                    },
                                    ..Default::default()
                                }),
                                ..Default::default()
                            });
                        }

                        // Adds username image to files
                        if let Some(user_image) = post["user"]["image"].as_str() {
                            files.insert(FileObject {
                                source: Some(FileSource::Url(user_image.to_string())),
                                tag_list: vec![FileTagAction {
                                    operation: TagOperation::Add,
                                    tags: vec![PluginTag {
                                        tag: Tag {
                                            name: user_id.to_string(),
                                            namespace: nsobjplg(&NsIdent::CivitUserId),
                                        },
                                        ..Default::default()
                                    }],
                                }],
                                ..Default::default()
                            });
                        }
                    }
                }

                // Grabs the prompt for this post
                if let Some(prompt) = post["prompt"].as_str() {
                    tags.push(PluginTag {
                        tag: Tag {
                            name: prompt.to_string(),
                            namespace: nsobjplg(&NsIdent::CivitImagePrompt),
                        },
                        ..Default::default()
                    });
                }

                // Gets tags for this post
                for tag in post["tagNames"].members() {
                    if let Some(tag_name) = tag.as_str() {
                        tags.push(PluginTag {
                            tag: Tag {
                                name: tag_name.to_string(),
                                namespace: nsobjplg(&NsIdent::CivitImageTags),
                            },
                            ..Default::default()
                        });
                    }
                }

                // Gets when the image/video was created
                if let Some(timestamp_str) = post["createdAt"].as_str()
                    && let Ok(datetime) = DateTime::parse_from_rfc3339(timestamp_str)
                {
                    let datetime_utc = datetime.with_timezone(&Utc).timestamp_millis();
                    tags.push(PluginTag {
                        tag: Tag {
                            name: datetime_utc.to_string(),
                            namespace: nsobjplg(&NsIdent::CivitImageCreatedAt),
                        },
                        ..Default::default()
                    });
                }

                // IF we have a URL and a filename then we're good to add file
                if let Some(url_key) = post["url"].as_str()
                    && let Some(name) = post["name"].as_str()
                {
                    file = Some(FileObject {
                        source: Some(FileSource::Url(generate_image_url(url_key, name))),
                        hash: vec![],
                        tag_list: vec![],
                        skip_if: vec![],
                    });
                }

                // Adds file into list and updates local tags
                if let Some(mut file) = file {
                    file.tag_list = vec![FileTagAction {
                        operation: TagOperation::Set,
                        tags,
                    }];
                    files.insert(file);
                }
            }
        }

        out.push(ScraperReturn::Data(ScraperObject {
            files,
            tags: system_tags,
            ..Default::default()
        }));
    } else {
        dbg!(&text_input);
    }

    out
}
pub fn model_by_id(modelnumber: &usize) -> String {
    let jsonmodel = ModelById::json(Id::id(*modelnumber));
    let jsondata = serde_json::to_string(&jsonmodel).unwrap();
    format!(
        "https://{SITE_URL_BASE}/api/trpc/model.getById?input={}",
        jsondata
    )
}

/// Generates a image url from a url key.
/// Looks like: 8ec7684c-7187-4ea8-8a21-4e936c969856
fn generate_image_url(url_key: &str, name: &str) -> String {
    format!("https://image.civitai.com/xG1nkqKTMzGDvpLrqFT7WA/{url_key}/original=true/{name}")
}

///
/// Extracts data from the url
///
fn extract_url_parameters(url: &str, user_data: &mut BTreeMap<String, String>) {
    let contains_image = url.contains("/images/");

    if contains_image {
        user_data.insert("scraper_type".into(), "image".into());
        if let Some(image_id) = url.rsplit("/").next() {
            user_data.insert("media_id".into(), image_id.to_string());
        }
    }
}

#[derive(Serialize)]
struct SearchQuery {
    q: String,
    #[serde(rename = "indexUid")]
    index_uid: String,
    facets: Vec<String>,
    #[serde(rename = "attributesToHighlight")]
    attributes_to_highlight: Vec<String>,
    #[serde(rename = "highlightPreTag")]
    highlight_pre_tag: String,
    #[serde(rename = "highlightPostTag")]
    highlight_post_tag: String,
    limit: u32,
    offset: u32,
    filter: Vec<String>,
}

#[derive(Serialize)]
struct MultiSearchPayload {
    queries: Vec<SearchQuery>,
}

#[derive(Serialize, Deserialize)]
pub enum Id {
    id(usize),
}

#[derive(Serialize, Deserialize)]
pub enum ModelById {
    json(Id),
}

#[derive(Serialize, Deserialize)]
pub enum Cursor {
    cursor(String),
}

#[derive(Serialize, Deserialize)]
pub enum MetaCursor {
    cursor(Vec<String>),
}

#[derive(Serialize, Deserialize)]
pub enum Values {
    values(Cursor),
}

#[derive(Serialize, Deserialize)]
pub enum MetaValues {
    values(MetaCursor),
}

#[derive(Serialize, Deserialize)]
pub struct GetInfinite {
    modelVersionId: usize,
    period: String,
    sort: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
    pending: bool,
    cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct MetaImageGetInfinite {
    json: GetInfinite,
    meta: MetaValues,
}

#[derive(Serialize, Deserialize)]
pub struct ImageGetInfinite {
    json: GetInfinite,
    meta: Values,
}
