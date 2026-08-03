//! Metadata-rich scraper for public and password-protected MEGA links.
//!
//! MEGA handles are recorded as the durable identity of a node.  The plugin
//! checks the handle tag before starting a transfer, so already-known nodes
//! never download their contents again.  Folder revision timestamps are used
//! as `RelationContext::limit_to` values for metadata relations.
//! Slop coded by GPT5.6-Luna with himan supervision

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use mega::{Client, Node, Nodes};
use shared_types::{
    FileObject, FileSource, FileTagAction, GenericNamespaceObj, GlobalCallbacks, PluginProperties,
    PluginTag, RelationContext, ScraperDataReturn, ScraperParam, ScraperReturn, SkipIf, Tag,
    TagOperation,
};

const SITE: &str = "meganz";
const NS_HANDLE: &str = "MEGA_handle";
const NS_NAME: &str = "MEGA_name";
const NS_SIZE: &str = "MEGA_size";
const NS_CREATED: &str = "MEGA_created_at";
const NS_MODIFIED: &str = "MEGA_modified_at";
const NS_PATH: &str = "MEGA_path";
const NS_PARENT: &str = "MEGA_parent_handle";
const NS_FOLDER_MODIFIED: &str = "MEGA_folder_last_modified";

fn namespace(name: &str) -> GenericNamespaceObj {
    GenericNamespaceObj {
        name: name.into(),
        description: Some("MEGA node metadata".into()),
    }
}

fn tag(namespace_name: &str, value: impl Into<String>) -> Tag {
    Tag {
        namespace: namespace(namespace_name),
        name: value.into(),
    }
}

/// Returns the timestamp as a millisecond unix timestamp
fn timestamp(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(|date| date.timestamp_millis().to_string())
}

/// determine which datetime is newer
fn newest(first: Option<DateTime<Utc>>, second: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.max(second)),
        (Some(first), None) => Some(first),
        (None, Some(second)) => Some(second),
        (None, None) => None,
    }
}

/// Returns the most recent timestamp on a node's containing folder chain.
fn folder_last_modified(nodes: &Nodes, node: &Node) -> Option<DateTime<Utc>> {
    let mut latest = None;
    let mut parent = node
        .parent()
        .and_then(|handle| nodes.get_node_by_handle(handle));

    while let Some(folder) = parent {
        latest = newest(latest, newest(folder.modified_at(), folder.created_at()));
        parent = folder
            .parent()
            .and_then(|handle| nodes.get_node_by_handle(handle));
    }

    latest
}

fn node_path(nodes: &Nodes, node: &Node) -> String {
    let mut names = vec![node.name().to_owned()];
    let mut parent = node.parent().map(str::to_owned);

    while let Some(handle) = parent {
        let Some(parent_node) = nodes.get_node_by_handle(&handle) else {
            break;
        };
        names.push(parent_node.name().to_owned());
        parent = parent_node.parent().map(str::to_owned);
    }

    names.reverse();
    names.join("/")
}

fn metadata_tags(nodes: &Nodes, node: &Node) -> (Vec<FileTagAction>, Tag) {
    let revision = timestamp(folder_last_modified(nodes, node)).unwrap_or_else(|| "unknown".into());
    let revision_tag = tag(NS_FOLDER_MODIFIED, revision);
    let handle_tag = tag(NS_HANDLE, node.handle());

    let mut tags = vec![
        PluginTag {
            tag: handle_tag.clone(),
            relates_to: Some(RelationContext {
                tag: revision_tag.clone(),
                // The limit is deliberately the last modified timestamp of
                // the containing folder, not the file timestamp.
                limit_to: Some(revision_tag.clone()),
                ..Default::default()
            }),
            ..Default::default()
        },
        PluginTag {
            tag: tag(NS_NAME, node.name()),
            ..Default::default()
        },
        PluginTag {
            tag: tag(NS_SIZE, node.size().to_string()),
            ..Default::default()
        },
        PluginTag {
            tag: tag(NS_PATH, node_path(nodes, node)),
            ..Default::default()
        },
        PluginTag {
            tag: revision_tag,
            ..Default::default()
        },
    ];

    if let Some(value) = timestamp(node.created_at()) {
        tags.push(PluginTag {
            tag: tag(NS_CREATED, value),
            ..Default::default()
        });
    }
    if let Some(value) = timestamp(node.modified_at()) {
        tags.push(PluginTag {
            tag: tag(NS_MODIFIED, value),
            ..Default::default()
        });
    }
    if let Some(parent) = node.parent() {
        tags.push(PluginTag {
            tag: tag(NS_PARENT, parent),
            ..Default::default()
        });
    }

    (
        vec![FileTagAction {
            operation: TagOperation::Set,
            tags,
        }],
        handle_tag,
    )
}

fn public_url(params: &[ScraperParam]) -> Option<String> {
    params.iter().find_map(|param| match param {
        ScraperParam::Url(url) if url.url.contains("mega.nz/") => Some(url.url.clone()),
        ScraperParam::Normal(value) if value.contains("mega.nz/") => Some(value.clone()),
        _ => None,
    })
}

fn password(params: &[ScraperParam]) -> Option<String> {
    params.iter().find_map(|param| match param {
        ScraperParam::Normal(value) if !value.contains("mega.nz/") => Some(value.clone()),
        _ => None,
    })
}

fn collect_file_nodes(nodes: &Nodes, node: &Node, output: &mut Vec<String>) {
    if node.kind().is_file() {
        output.push(node.handle().to_owned());
        return;
    }
    if !node.kind().is_folder() && !node.kind().is_root() {
        return;
    }

    for child in nodes
        .iter()
        .filter(|candidate| candidate.parent() == Some(node.handle()))
    {
        collect_file_nodes(nodes, child, output);
    }
}

async fn collect_files(
    client: &Client,
    nodes: &Nodes,
    handles: &[String],
    output: &mut Vec<FileObject>,
) -> Result<(), Box<dyn std::error::Error>> {
    for handle in handles {
        let Some(node) = nodes.get_node_by_handle(handle) else {
            continue;
        };
        let (tag_list, handle_tag) = metadata_tags(nodes, node);

        // Query the database before downloading. The handle is stable across
        // renames and path changes, unlike the node name or path. Log the
        // decision with enough context to identify the skipped folder/file.
        if let Some(existing_file) = client::get_tag_file(handle_tag.clone())? {
            let file_path = node_path(nodes, node);
            let folder_path = node
                .parent()
                .and_then(|parent| nodes.get_node_by_handle(parent))
                .map(|parent| node_path(nodes, parent))
                .unwrap_or_else(|| "<root>".to_owned());

            if let Some(file_id) = existing_file.id {
                client::put_tags_to_file(file_id, tag_list)?;
            } else {
                client::log_silent(format!(
                    "MEGA: handle {} exists but has no file ID; cannot update tags for path '{}' in folder '{}'",
                    node.handle(),
                    file_path,
                    folder_path,
                ))?;
            }

            client::log_silent(format!(
                "MEGA: skipping handle {} because it exists in the database; updated tags and ignored file path '{}' for folder '{}'",
                node.handle(),
                file_path,
                folder_path,
            ))?;

            continue;
        }

        let mut bytes = Vec::new();
        client.download_node(node, &mut bytes).await?;

        output.push(FileObject {
            source: Some(FileSource::Bytes(bytes)),
            tag_list,
            skip_if: vec![SkipIf::FileTagRelationship(handle_tag)],
            ..Default::default()
        });
    }
    Ok(())
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: SITE.into(),
        properties: vec![
            PluginProperties::Sites(vec![SITE.into(), "mega.nz".into(), "mega".into()]),
            PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
            PluginProperties::ThreadNum(1),
        ],
        callbacks: vec![GlobalCallbacks::Start(
            shared_types::StartupThreadType::Spawn,
        )],
        ..Default::default()
    }]
}

#[unsafe(no_mangle)]
pub fn url_dump(data: &ScraperDataReturn) -> Vec<ScraperDataReturn> {
    if public_url(&data.job.param).is_some() {
        vec![data.clone()]
    } else {
        Vec::new()
    }
}

#[unsafe(no_mangle)]
pub fn parser_call(_text: &str, _source_url: &str, data: &ScraperDataReturn) -> Vec<ScraperReturn> {
    let Some(url) = public_url(&data.job.param) else {
        return vec![ScraperReturn::Nothing];
    };

    let result = (|| -> Result<Vec<FileObject>, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let mega = Client::builder()
                .https(true)
                .max_retries(3)
                .build(reqwest::Client::new())?;
            let nodes = if let Some(password) = password(&data.job.param) {
                mega.fetch_protected_nodes(&url, &password).await?
            } else {
                mega.fetch_public_nodes(&url).await?
            };
            let mut handles = Vec::new();
            for root in nodes.roots() {
                collect_file_nodes(&nodes, root, &mut handles);
            }
            let mut files = Vec::new();
            collect_files(&mega, &nodes, &handles, &mut files).await?;
            Ok(files)
        })
    })();

    match result {
        Ok(files) if files.is_empty() => vec![ScraperReturn::Nothing],
        Ok(files) => vec![ScraperReturn::Data(shared_types::ScraperObject {
            files: files.into_iter().collect::<HashSet<_>>(),
            ..Default::default()
        })],
        Err(error) => vec![ScraperReturn::Stop(format!("MEGA scan failed: {error}"))],
    }
}
