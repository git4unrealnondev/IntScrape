use shared_types::{
    DownloadModifiers, ModifierTarget, PluginProperties, ScraperParam, TargetModifier,
};
use std::{task::Poll, time::Duration};
use text_trees::{FormatCharacters, StringTreeNode, TreeFormatting, TreeNode};

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "Mega".into(),
        properties: vec![
            PluginProperties::Ratelimit(10, std::time::Duration::from_secs(1)),
            PluginProperties::Sites(vec!["mega".into(), "mega.nz".into()]),
            /* PluginProperties::Modifier(TargetModifier {
                  target: ModifierTarget::Text,
                  modifier: DownloadModifiers::Header(("Accept".into(), "application/json".into())),
              }),*/
            /*  PluginProperties::Modifier(TargetModifier {
                  target: ModifierTarget::Media,
                  modifier: DownloadModifiers::Header((
                      "Referer".into(),
                      format!("https://{SITE_ROOT}/"),
                  )),
              }),*/
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

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return out,
    };

    for url in urls {
        let http_client = reqwest::Client::new();
        let mut mega = match mega::Client::builder().build(http_client) {
            Ok(client) => client,
            Err(_) => continue,
        };

        // Block on the async run function using the runtime
        let result = runtime.block_on(async { run(&mut mega, &url).await });

        // Handle your result here as needed
        let _ = result;
    }

    out
}

/// Straight yoinked from: https://github.com/Hirevo/mega-rs/blob/main/examples/public_folder_listing.rs
fn construct_tree_node(nodes: &mega::Nodes, node: &mega::Node) -> text_trees::TreeNode<String> {
    let name = if node.name().is_empty() {
        "/"
    } else {
        node.name()
    };

    // Files cannot have children. If it's a file, return a leaf node immediately.
    if !node.kind().is_folder() {
        return text_trees::TreeNode::new(name.to_string());
    }

    // Safely collect valid children for folders only
    let mut children_nodes: Vec<&mega::Node> = node
        .children()
        .iter()
        .filter_map(|hash| nodes.get_node_by_handle(hash))
        .filter(|child| child.handle() != node.handle())
        .collect();

    let (mut folders, mut files): (Vec<_>, Vec<_>) = children_nodes
        .into_iter()
        .partition(|child| child.kind().is_folder());

    folders.sort_unstable_by_key(|n| n.name());
    files.sort_unstable_by_key(|n| n.name());

    let children = folders
        .into_iter()
        .chain(files)
        .map(|child| construct_tree_node(nodes, child));

    text_trees::TreeNode::with_child_nodes(name.to_string(), children)
}

fn make_tree(nodes: &mega::Nodes, node: &mega::Node) {

    if node.kind().is_folder() {
    let children_nodes: Vec<_> = node
        .children()
        .iter()
        .filter_map(|hash| nodes.get_node_by_handle(hash))
        .filter(|child| child.handle() != node.handle())
        .collect();
    //dbg!(&children_nodes);
    }

    //if node.kind().is_file() {
    //    dbg!(node.name());
    //}




}

async fn run(mega: &mut mega::Client, public_url: &str) -> mega::Result<()> {
    let mut stdout = std::io::stdout().lock();

    let nodes = mega.fetch_public_nodes(public_url).await?;
    let formatting = TreeFormatting::dir_tree(FormatCharacters::box_chars());
    println!();

    for node in nodes.roots() {
        make_tree(&nodes, node);
    }

   /* for root in nodes.roots() {
        if root.kind().is_file() {
            dbg!(root.name());
            continue;
        }

        // let tree = construct_tree_node(&nodes, root);
        //  dbg!(&tree);
        //tree.write_with_format(&mut stdout, &formatting).unwrap();
        println!();
    }*/
    Ok(())
}
