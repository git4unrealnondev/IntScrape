use base64::Engine;
use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use shared_types::*;

enum Nsid {
    PostId,
    PostComment,
    PostTimestamp,
    ThreadId,
    AttachmentName,
    OriginalMD5,
    ThreadTitle,
}

#[unsafe(no_mangle)]
pub fn get_plugin_info() -> Vec<Plugin> {
    vec![Plugin {
        name: "4chan".into(),
        properties: vec![
            PluginProperties::Ratelimit(5, Duration::from_secs(1)),
            PluginProperties::Sites(vec![
                "4ch".to_string(),
                "4chan".to_string(),
                "4chan.net".to_string(),
            ]),
            PluginProperties::Modifier(TargetModifier {
                target: ModifierTarget::Media,
                modifier: DownloadModifiers::Header((
                    "Referrer".into(),
                    "boards.4chan.org".to_string(),
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

    extract_board_info(scraperdata, &mut out);

    out
}

#[unsafe(no_mangle)]
pub fn parser_call(
    text_input: &str,
    _source_url: &str,
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperReturn> {
    let mut out = Vec::new();

    let mut jobs = HashSet::new();
    let mut files = HashSet::new();
    let mut tags = HashSet::new();

    if let Some(board) = scraperdata.job.user_data.get("board_code")
        && let Some(search_term) = scraperdata.job.user_data.get("search_term")
        && let Ok(json_data) = json::parse(text_input)
    {
        let mut post_list = json_data["posts"].members();

        if let Some(first_post) = post_list.next()
            && let Some(thread_id) = first_post["no"].as_u64()
        {
            let rel_context = RelationContext {
                tag: Tag {
                    name: format!("{}_{}_4chan", thread_id, board),
                    namespace: nsout(&Nsid::PostId),
                },
                limit_to: Some(Tag {
                    name: format!("{}_{}_4chan", thread_id, board),
                    namespace: nsout(&Nsid::ThreadId),
                }),

                ..Default::default()
            };

            extract_post_info(first_post, &mut tags, rel_context);

            for post in post_list {
                if let Some(post_id) = post["no"].as_u64() {
                    let rel_context = RelationContext {
                        tag: Tag {
                            name: format!("{}_{}_4chan", post_id, board),
                            namespace: nsout(&Nsid::PostId),
                        },
                        limit_to: Some(Tag {
                            name: format!("{}_{}_4chan", thread_id, board),
                            namespace: nsout(&Nsid::ThreadId),
                        }),

                        ..Default::default()
                    };

                    if let Some(file_name) = post["filename"].as_str()
                        && let Some(file_ext) = post["ext"].as_str()
                        && let Some(file_hash) = post["md5"].as_str()
                        && let Some(file_url) = post["tim"].as_u64()
                    {
                        let attachment_md5 = hex::encode(
                            base64::prelude::BASE64_STANDARD.decode(file_hash).unwrap(),
                        );

                        let tag_list = vec![FileTagAction {
                            operation: TagOperation::Add,
                            tags: vec![
                                PluginTag {
                                    tag: Tag {
                                        name: format!("{}{}", file_name, file_ext),
                                        namespace: nsout(&Nsid::AttachmentName),
                                    },
                                    ..Default::default()
                                },
                                PluginTag {
                                    tag: Tag {
                                        name: attachment_md5.to_string(),
                                        namespace: nsout(&Nsid::OriginalMD5),
                                    },
                                    relates_to: Some(rel_context.clone()),
                                    ..Default::default()
                                },
                            ],
                        }];
                        files.insert(FileObject {
                            source: Some(FileSource::Url(format!(
                                "https://i.4cdn.org/{}/{}{}",
                                board, file_url, file_ext
                            ))),
                            skip_if: vec![SkipIf::FileTagRelationship(Tag {
                                name: attachment_md5.to_string(),
                                namespace: nsout(&Nsid::OriginalMD5),
                            })],
                            hash: Some(HashesSupported::Md5(attachment_md5)),
                            tag_list,

                            ..Default::default()
                        });
                    }
                    extract_post_info(post, &mut tags, rel_context);
                }
            }
        }

        for sub in json_data.members() {
            for thread in sub["threads"].members() {
                let mut should_add = false;

                if search_term == "*" {
                    should_add = true;
                }

                if let Some(sub) = thread["sub"].as_str()
                    && sub.to_lowercase().contains(&search_term.to_lowercase())
                {
                    should_add = true;
                }
                if let Some(sub) = thread["com"].as_str()
                    && sub.to_lowercase().contains(&search_term.to_lowercase())
                {
                    should_add = true;
                }

                if should_add && let Some(post_id) = thread["no"].as_u64() {
                    let mut param = Vec::new();

                    param.push(ScraperParam::Url(Url {
                        url: format!("https://a.4cdn.org/{}/thread/{}.json", board, post_id),
                        ..Default::default()
                    }));
                    jobs.insert(ScraperDataReturn {
                        job: PluginJob {
                            site: scraperdata.job.site.clone(),
                            param,
                            user_data: scraperdata.job.user_data.clone(),

                            ..Default::default()
                        },
                        ..Default::default()
                    });
                }
            }
        }
    }
    if !jobs.is_empty() {
        out.push(ScraperReturn::Data(ScraperObject {
            jobs,
            ..Default::default()
        }));
    }
    if !files.is_empty() {
        out.push(ScraperReturn::Data(ScraperObject {
            files,
            ..Default::default()
        }))
    }
    if !tags.is_empty() {
        out.push(ScraperReturn::Data(ScraperObject {
            tags,
            ..Default::default()
        }))
    }

    out
}

fn nsout(inp: &Nsid) -> GenericNamespaceObj {
    match inp {
        Nsid::PostId => GenericNamespaceObj {
            name: "Thread_Post_Id".to_string(),
            description: Some("A 4chan's post id, is unique".to_string()),
        },
        Nsid::PostTimestamp => GenericNamespaceObj {
            name: "Thread_Post_Timestamp".to_string(),
            description: Some("A 4chan's post's timestamp UNIX style".to_string()),
        },
        Nsid::ThreadId => GenericNamespaceObj {
            name: "Thread_ID_Unique".to_string(),
            description: Some("A unique thread_id for the site, board".to_string()),
        },
        Nsid::PostComment => GenericNamespaceObj {
            name: "Thread_Comment".to_string(),
            description: Some("A comment attached to a post".to_string()),
        },
        Nsid::AttachmentName => GenericNamespaceObj {
            name: "Thread_Attachment_Name".to_string(),
            description: Some("The original name of an atachment that was uploaded".to_string()),
        },Nsid::OriginalMD5 => GenericNamespaceObj {
            name: "Thread_Post_Original_MD5".to_string(),
            description: Some("The original MD5 of the image before CF Polish tampered with this. I cannot find a way to bypass or to do other naughty things to it to get the original image".to_string()),
        },
        Nsid::ThreadTitle => GenericNamespaceObj {
            name: "Thread_Title".to_string(),
            description: Some("The main thread title".to_string())
        }


    }
}

/// Gets info from a post
fn extract_post_info(
    post_json: &json::JsonValue,
    tags: &mut HashSet<PluginTag>,
    rel_context: RelationContext,
) {
    if let Some(post_comment) = post_json["com"].as_str() {
        tags.insert(PluginTag {
            tag: Tag {
                name: post_comment.to_string(),
                namespace: nsout(&Nsid::PostComment),
            },
            relates_to: Some(rel_context.clone()),
            ..Default::default()
        });
    }
    if let Some(post_timestamp) = post_json["time"].as_u64() {
        tags.insert(PluginTag {
            tag: Tag {
                name: post_timestamp.to_string(),
                namespace: nsout(&Nsid::PostTimestamp),
            },
            relates_to: Some(rel_context.clone()),
            ..Default::default()
        });
    }
    if let Some(sub) = post_json["sub"].as_str() {
        tags.insert(PluginTag {
            tag: Tag {
                name: sub.to_string(),
                namespace: nsout(&Nsid::ThreadTitle),
            },
            relates_to: Some(rel_context),
            ..Default::default()
        });
    }
}

/// Gets board info
/// Passes URL directly through
fn extract_board_info(
    scraperdata: &shared_types::ScraperDataReturn,
    scraper_return: &mut Vec<ScraperDataReturn>,
) {
    let mut normal_param = Vec::new();

    for scraper_param in scraperdata.job.param.iter() {
        match scraper_param {
            ScraperParam::Url(url) => {
                let param = vec![ScraperParam::Url(url.clone())];
                scraper_return.push(ScraperDataReturn {
                    job: PluginJob {
                        site: scraperdata.job.site.clone(),
                        user_data: scraperdata.job.user_data.clone(),
                        priority: DEFAULT_PRIORITY - 3,
                        param,
                        ..Default::default()
                    },
                    skip_conditions: vec![],
                });
            }
            ScraperParam::Normal(normal) => {
                normal_param.push(normal);
            }
            _ => {}
        }
    }

    for normal in normal_param.chunks(2) {
        if let Some(board) = normal.first()
            && let Some(search_term) = normal.get(1)
        {
            let mut user_data = BTreeMap::new();
            let mut param = Vec::new();

            param.push(ScraperParam::Url(Url {
                url: format!("https://a.4cdn.org/{}/catalog.json", board),
                ..Default::default()
            }));

            user_data.insert("board_code".into(), board.to_string());
            user_data.insert("search_term".into(), search_term.to_string());

            let job = PluginJob {
                site: scraperdata.job.site.clone(),
                user_data,
                priority: DEFAULT_PRIORITY - 2,
                param,
                ..Default::default()
            };
            scraper_return.push(ScraperDataReturn {
                job,
                skip_conditions: vec![],
            });
        }
    }
}
