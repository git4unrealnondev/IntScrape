//! Metadata-rich scraper for public and password-protected MEGA links.
//!
//! MEGA handles are recorded as the durable identity of a node.  The plugin
//! checks the handle tag before starting a transfer, so already-known nodes
//! never download their contents again.  Folder revision timestamps are used
//! as `RelationContext::limit_to` values for metadata relations.
//! Slop coded by GPT5.6-Luna with himan supervision

use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
};

use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use mega::{Client, ErrorCode, Node, Nodes};
use regex::Regex;
use shared_types::{
    CallbackReturn, DbSettingsObj, FileObject, FileSource, FileTagAction, GenericNamespaceObj,
    GlobalCallbacks, PluginJob, PluginProperties, PluginTag, RelationContext, ScraperDataReturn,
    ScraperParam, ScraperReturn, SearchType, SkipIf, Tag, TagOperation, Url,
};

const SITE: &str = "meganz";
const NS_HANDLE: &str = "MEGA_handle";
const NS_NAME: &str = "MEGA_name";
const NS_SIZE: &str = "MEGA_size";
const NS_CREATED: &str = "MEGA_created_at";
const NS_MODIFIED: &str = "MEGA_modified_at";
const NS_PATH: &str = "MEGA_path";
const NS_PARENT: &str = "MEGA_parent_handle";
const NS_SOURCE_URL: &str = "MEGA_source_url";
const NS_SNAPSHOT: &str = "MEGA_system_timestamp";
const STARTUP_SETTING: &str = "PLUGIN_MegaNZ_ShouldCheckTags";
const MEGA_URL_REGEX: &str = r#"(?i)\bhttps?://(?:www\.)?(?:mega\.nz|mega\.co\.nz)/(?:file|folder)/[A-Za-z0-9_-]+(?:#[A-Za-z0-9_-]+)?|https?://(?:www\.)?(?:mega\.nz|mega\.co\.nz)/#![A-Za-z0-9_-]+![A-Za-z0-9_-]+"#;
const DOWNLOAD_CONCURRENCY: usize = 5;
const MEGA_NAMESPACES: &[&str] = &[
    NS_HANDLE,
    NS_NAME,
    NS_SIZE,
    NS_CREATED,
    NS_MODIFIED,
    NS_PATH,
    NS_PARENT,
    NS_SOURCE_URL,
    NS_SNAPSHOT,
];

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

fn latest_system_timestamp(nodes: &Nodes) -> Option<DateTime<Utc>> {
    nodes.iter().fold(None, |latest, node| {
        newest(latest, newest(node.modified_at(), node.created_at()))
    })
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

fn metadata_tags(
    nodes: &Nodes,
    node: &Node,
    source_url: &str,
    system_timestamp: Option<DateTime<Utc>>,
) -> (Vec<FileTagAction>, Tag) {
    let snapshot_value = timestamp(system_timestamp).unwrap_or_else(|| "unknown".to_owned());

    let snapshot_tag = tag(NS_SNAPSHOT, snapshot_value.clone());
    let handle_tag = tag(NS_HANDLE, node.handle());

    let handle_plugin_tag = PluginTag {
        tag: handle_tag.clone(),
        relates_to: Some(RelationContext {
            tag: snapshot_tag.clone(),
            limit_to: Some(snapshot_tag.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let metadata_tag = |value: Tag| PluginTag {
        tag: value,
        relates_to: Some(RelationContext {
            tag: handle_tag.clone(),
            limit_to: Some(snapshot_tag.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut tags = vec![
        handle_plugin_tag,
        metadata_tag(tag(NS_SOURCE_URL, source_url)),
        metadata_tag(tag(NS_NAME, node.name())),
        metadata_tag(tag(NS_SIZE, node.size().to_string())),
        metadata_tag(tag(NS_PATH, node_path(nodes, node))),
        metadata_tag(tag(NS_SNAPSHOT, snapshot_value)),
    ];

    if let Some(value) = timestamp(node.created_at()) {
        tags.push(metadata_tag(tag(NS_CREATED, value)));
    }

    if let Some(value) = timestamp(node.modified_at()) {
        tags.push(metadata_tag(tag(NS_MODIFIED, value)));
    }

    if let Some(parent) = node.parent() {
        tags.push(metadata_tag(tag(NS_PARENT, parent)));
    }

    (
        vec![FileTagAction {
            operation: TagOperation::Add,
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
        //let _ = client::log_silent(format!("MEGA: Found file handle {}", node.handle()));
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
    source_url: &str,
    system_timestamp: Option<DateTime<Utc>>,
    output: &mut Vec<FileObject>,
    errors: &mut Vec<String>,
) {
    let results = stream::iter(handles.iter().map(|handle| async move {
        let Some(node) = nodes.get_node_by_handle(handle) else {
            return Err(format!(
                "MEGA handle {handle}: node was not found in the fetched node set"
            ));
        };

        let file_path = node_path(nodes, node);
        let folder_path = node
            .parent()
            .and_then(|parent| nodes.get_node_by_handle(parent))
            .map(|parent| node_path(nodes, parent))
            .unwrap_or_else(|| "<root>".to_owned());

        let (tag_list, handle_tag) = metadata_tags(nodes, node, source_url, system_timestamp);

        // Existing files are not downloaded again. Their metadata is still
        // updated using the current snapshot relation graph.
        match client::get_tag_file(handle_tag.clone()) {
            Ok(Some(existing_file)) => {
                match existing_file.id {
                    Some(file_id) => {
                        if let Err(error) = client::put_tags_to_file(file_id, tag_list) {
                            return Err(format!(
                                "MEGA handle {handle}, path '{file_path}', \
                                 folder '{folder_path}': failed to update \
                                 metadata: {error}"
                            ));
                        }

                        let _ = client::log_silent(format!(
                            "MEGA: skipping handle {handle} because it exists \
                             in the database; updated metadata for path \
                             '{file_path}' in folder '{folder_path}'"
                        ));
                    }

                    None => {
                        return Err(format!(
                            "MEGA handle {handle}, path '{file_path}', \
                             folder '{folder_path}': existing database record \
                             has no file ID"
                        ));
                    }
                }

                return Ok(None);
            }

            Ok(None) => {}

            Err(error) => {
                return Err(format!(
                    "MEGA handle {handle}, path '{file_path}', \
                     folder '{folder_path}': database lookup failed: {error}"
                ));
            }
        }

        let _ = client::log_silent(format!(
            "MEGA: attempting download of handle {handle} with file path \
             '{file_path}'"
        ));

        let mut bytes = Vec::new();

        match client.download_node(node, &mut bytes).await {
            Ok(()) => {
                let downloaded_size = bytes.len();

                let _ = client::log_silent(format!(
                    "MEGA: downloaded handle {handle} with size \
                     {downloaded_size}"
                ));

                Ok(Some(FileObject {
                    source: Some(FileSource::Bytes(bytes)),
                    tag_list,
                    skip_if: vec![SkipIf::FileTagRelationship(handle_tag)],
                    ..Default::default()
                }))
            }

            Err(error) => Err(format!(
                "MEGA handle {handle}, path '{file_path}', \
                     folder '{folder_path}': download failed: {error}"
            )),
        }
    }))
    .buffer_unordered(DOWNLOAD_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    for result in results {
        match result {
            Ok(Some(file)) => output.push(file),
            Ok(None) => {}
            Err(error) => {
                let _ = client::log_silent(format!("MEGA: {error}"));
                errors.push(error);
            }
        }
    }
}

fn add_source_job(url: &str) -> Result<(), Box<dyn Error>> {
    client::jobs_add_single(PluginJob {
        time: 0,
        reptime: 0,
        priority: 10,
        recreation: None,
        site: SITE.into(),
        param: vec![ScraperParam::Url(Url {
            url: url.into(),
            local_modifiers: Vec::new(),
        })],
        user_data: BTreeMap::new(),
    })?;

    Ok(())
}

fn handle_on_start() -> Result<(), Box<dyn Error>> {
    if client::setting_get(STARTUP_SETTING.into())?.is_some() {
        return Ok(());
    }

    let url_regex = Regex::new(MEGA_URL_REGEX)?;
    let tags = client::get_tag_id_bulk(client::get_tag_ids_all()?)?;
    for (tag_id, tag) in tags
        .iter()
        .filter(|(_, tag)| !MEGA_NAMESPACES.contains(&tag.namespace.name.as_str()))
    {
        for matched in url_regex.find_iter(&tag.name) {
            dbg!(&matched.as_str(), &tag.name, &tag, &tag_id);
            let callback_return = on_regex(matched.as_str(), &tag.name, tag, *tag_id);
            let tag_actions = callback_return.tags.into_iter().collect::<Vec<_>>();
            if !tag_actions.is_empty() && !client::tag_actions_add(tag_actions)? {
                return Err("failed to persist MEGA source tag relationship".into());
            }
            for job in callback_return.jobs {
                client::jobs_add_single(job.job)?;
            }
        }
    }

    client::setting_set(DbSettingsObj {
        name: STARTUP_SETTING.into(),
        description: Some("Whether the MEGA plugin should scan existing tags on startup".into()),
        num: None,
        param: Some("False".into()),
    })?;

    Ok(())
}

#[unsafe(no_mangle)]
fn on_start() {
    if let Err(error) = handle_on_start() {
        let _ = client::log_silent(format!("MEGA: startup tag scan failed: {error}"));
    }
}

#[unsafe(no_mangle)]
fn on_regex(
    matched: &str,
    _full_tag: &str,
    tag_source: &Tag,
    _tag_source_id: u64,
) -> CallbackReturn {
    if tag_source.namespace.name.starts_with("MEGA_") {
        return CallbackReturn::default();
    }

    // Never create a MEGA source tag from the full source tag. The callback
    // input must be the URL fragment matched by MEGA_URL_REGEX.
    if !(matched.starts_with("http://mega.nz/")
        || matched.starts_with("https://mega.nz/")
        || matched.starts_with("http://www.mega.nz/")
        || matched.starts_with("https://www.mega.nz/")
        || matched.starts_with("http://mega.co.nz/")
        || matched.starts_with("https://mega.co.nz/")
        || matched.starts_with("http://www.mega.co.nz/")
        || matched.starts_with("https://www.mega.co.nz/"))
    {
        return CallbackReturn::default();
    }

    if let Err(error) = add_source_job(matched) {
        let _ = client::log_silent(format!("MEGA: failed to queue tag source job: {error}"));
    }

    let source_url_tag = PluginTag {
        tag: tag(NS_SOURCE_URL, matched),
        relates_to: Some(RelationContext {
            tag: tag_source.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut tags = HashSet::new();
    tags.insert(FileTagAction {
        operation: TagOperation::Add,
        tags: vec![source_url_tag],
    });

    CallbackReturn {
        tags,
        ..Default::default()
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: SITE.into(),
        properties: vec![
            PluginProperties::Sites(vec![SITE.into(), "mega.nz".into(), "mega".into()]),
            PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
            PluginProperties::ThreadNum(5),
            PluginProperties::JobNum(1),
            PluginProperties::NoTextDownload,
        ],
        callbacks: vec![
            GlobalCallbacks::Start(shared_types::StartupThreadType::Spawn),
            GlobalCallbacks::Tag((
                SearchType::Regex(MEGA_URL_REGEX.into()),
                vec![],
                MEGA_NAMESPACES.iter().map(|name| (*name).into()).collect(),
            )),
        ],
    }]
}

#[unsafe(no_mangle)]
pub fn url_dump(data: &ScraperDataReturn) -> Vec<ScraperDataReturn> {
    if let Some(url) = public_url(&data.job.param) {
        if let Ok(dead) = client::dead_url_get(vec![url.clone()])
            && dead.get(&url).is_some_and(|x| *x)
        {
            return Vec::new();
        }
        vec![data.clone()]
    } else {
        Vec::new()
    }
}

#[unsafe(no_mangle)]
pub fn parser_call(_text: &str, source_url: &str, data: &ScraperDataReturn) -> Vec<ScraperReturn> {
    let Some(url) = public_url(&data.job.param) else {
        return vec![ScraperReturn::Nothing];
    };

    let result = (|| -> Result<(Vec<FileObject>, Vec<String>), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        runtime.block_on(async {
            let http = reqwest::Client::builder()
                .use_rustls_tls()
                .user_agent(
                    "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0",
                )
                .build()?;

            let mega = Client::builder().build(http)?;

            let nodes = if let Some(password) = password(&data.job.param) {
                mega.fetch_protected_nodes(&url, &password).await?
            } else {
                mega.fetch_public_nodes(&url).await?
            };

            let mut handles = Vec::new();

            for root in nodes.roots() {
                collect_file_nodes(&nodes, root, &mut handles);
            }

            let system_timestamp = latest_system_timestamp(&nodes);
            let mut files = Vec::new();
            let mut errors = Vec::new();

            collect_files(
                &mega,
                &nodes,
                &handles,
                source_url,
                system_timestamp,
                &mut files,
                &mut errors,
            )
            .await;

            Ok((files, errors))
        })
    })();

    match result {
        Err(error) => {
            // If we're blocked IE bad folder then dont try it
            if let Some(pub_url) = public_url(&data.job.param)
                && is_mega_blocked(error.as_ref())
            {
                let _ = client::dead_url_add(pub_url.to_string());
            }
            if let Some(mega::Error::InvalidPublicUrlFormat) = error.downcast_ref::<mega::Error>() {
                vec![ScraperReturn::Nothing]
            } else {
                vec![ScraperReturn::Stop(format!(
                    "MEGA: STOPPING due to error {:?}",
                    error
                ))]
            }
        }

        Ok((files, errors)) => {
            let mut output = Vec::new();

            if !files.is_empty() {
                output.push(ScraperReturn::Data(shared_types::ScraperObject {
                    files: files.into_iter().collect::<HashSet<_>>(),
                    ..Default::default()
                }));
            }

            if !errors.is_empty() {
                let backoff = 3600;
                let _ = client::log_silent(format!(
                    "MEGA: Got error(s) {:?} while processing {source_url} will retry this job in seconds: {backoff}.",
                    errors
                ));
                output.push(ScraperReturn::RetryLater(backoff));
            }

            if output.is_empty() {
                output.push(ScraperReturn::Nothing);
            }

            output
        }
    }
}

fn is_mega_blocked(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<mega::Error>()
        .is_some_and(|mega_error| {
            matches!(
                mega_error,
                mega::Error::MegaError {
                    code: ErrorCode::EBLOCKED,
                    ..
                }
            )
        })
        || error
            .downcast_ref::<mega::Error>()
            .is_some_and(|mega_error| {
                matches!(
                    mega_error,
                    mega::Error::MegaError {
                        code: ErrorCode::ENOENT,
                        ..
                    }
                )
            })
}
