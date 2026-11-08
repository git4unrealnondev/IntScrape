//! Bulk importer for Danbooru-style CSV dumps (`posts.csv` + `tags.csv`).
//!
//! Attaches the data from a site's CSV dump to files that were already
//! downloaded: each post is matched to an existing file by its md5 and its
//! tags/metadata are added to that file. Posts whose file was never
//! downloaded are skipped (and counted) — this importer never creates file
//! records and never downloads anything.
//!
//! Controlled from the webui Settings page through per-site settings
//! (plugin name = the site's `Display`, i.e. `E621` / `E6AI`):
//!
//! - `PLUGIN_<SITE>_CsvImport_ShouldRun` — set `param` to `True` to run.
//!   On completion it is flipped back to `False` so it does not re-run on
//!   every boot; flip it back to `True` in Settings to run again.
//! - `PLUGIN_<SITE>_CsvImport_PostsPath` — path to `posts.csv`.
//! - `PLUGIN_<SITE>_CsvImport_TagsPath` — optional path to `tags.csv`
//!   (used to categorize tags like the live plugin does). Defaults to a
//!   `tags.csv` next to `posts.csv`; if no tags file is found every tag
//!   lands in `<SITE>_General`.
//!
//! Deleted posts (`is_deleted = t`) are included on purpose: the whole
//! point is to fill in metadata for files that were downloaded before the
//! post was deleted from the site.
//!
//! Runs from `on_start` (both plugin entries declare the Start callback,
//! so this fires twice per process; a run-once guard makes the second
//! invocation a no-op).

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use shared_types::{
    DbSettingsObj, FileTagAction, PluginTag, RelationContext, Tag, TagOperation,
};

use super::{NsIdent, Site, nsobjplg};

/// Rows processed per database round trip.
const BATCH_SIZE: usize = 1000;

static STARTED: AtomicBool = AtomicBool::new(false);

/// Entry point called from `on_start`.
pub fn run() {
    // The library exposes two Plugin entries (E621 / E6AI) sharing one
    // `on_start` symbol, so the manager invokes this once per entry.
    if STARTED.swap(true, Ordering::SeqCst) {
        return;
    }
    for site in [Site::E6, Site::E6AI] {
        if let Err(error) = run_site(&site) {
            let _ = client::log_silent(format!(
                "e6 csv import {site}: failed: {error}"
            ));
        }
    }
}

fn setting_param(key: &str) -> Result<Option<String>, Box<dyn Error>> {
    Ok(client::setting_get(key.to_string())?
        .and_then(|setting| setting.param)
        .filter(|param| !param.trim().is_empty()))
}

/// Accepts the usual truthy spellings, so `true` typed in the webui settings
/// editor works just like `True`.
fn setting_enabled(key: &str) -> Result<bool, Box<dyn Error>> {
    Ok(matches!(
        setting_param(key)?.as_deref().map(str::trim),
        Some(value)
            if matches!(value.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
    ))
}

fn run_site(site: &Site) -> Result<(), Box<dyn Error>> {
    let should_run_key = format!("PLUGIN_{site}_CsvImport_ShouldRun");
    if !setting_enabled(&should_run_key)? {
        return Ok(());
    }

    let Some(posts_path) = setting_param(&format!("PLUGIN_{site}_CsvImport_PostsPath"))? else {
        let _ = client::log_silent(format!(
            "e6 csv import {site}: PLUGIN_{site}_CsvImport_ShouldRun is enabled \
             but PLUGIN_{site}_CsvImport_PostsPath is not set; nothing to do."
        ));
        return Ok(());
    };

    let tags_path = setting_param(&format!("PLUGIN_{site}_CsvImport_TagsPath"))?
        .map(PathBuf::from)
        .or_else(|| {
            Path::new(&posts_path)
                .parent()
                .map(|parent| parent.join("tags.csv"))
                .filter(|candidate| candidate.exists())
        });
    let _ = client::log_silent(format!(
        "e6 csv import {site}: starting (posts: {posts_path}, tags: {}).",
        tags_path
            .as_deref()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_else(|| "<none>".to_string())
    ));

    let (stats, completed) = import_posts(site, Path::new(&posts_path), tags_path.as_deref())?;
    let _ = client::log_silent(format!(
        "e6 csv import {site}: finished (completed: {completed}) — \
         {} rows seen, {} invalid, {} matched to a downloaded file, \
         {} skipped (no downloaded file).",
        stats.rows_seen, stats.invalid, stats.matched, stats.unmatched,
    ));

    // One-shot: disarm so the next boot does not re-import the whole dump.
    // The user re-arms by setting the ShouldRun setting back to True.
    if completed {
        client::setting_set(DbSettingsObj {
            name: should_run_key,
            description: Some(format!(
                "Should the {site} plugin run its CSV dump import on_start"
            )),
            num: None,
            param: Some("False".into()),
        })?;
    }

    Ok(())
}

#[derive(Default)]
struct ImportStats {
    rows_seen: usize,
    invalid: usize,
    matched: usize,
    unmatched: usize,
}

/// One parsed `posts.csv` row (only the columns the importer uses).
struct PostRow {
    id: u64,
    md5: String,
    rating: String,
    description: String,
    sources: Vec<String>,
    tag_string: String,
    parent_id: Option<u64>,
}

/// Column indices into a `posts.csv` record, resolved from its header row.
struct Columns {
    id: usize,
    md5: usize,
    rating: Option<usize>,
    tag_string: Option<usize>,
    source: Option<usize>,
    description: Option<usize>,
    parent_id: Option<usize>,
}

fn column_index(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers.iter().position(|header| header.trim() == name)
}

fn field<'a>(record: &'a csv::StringRecord, index: Option<usize>) -> &'a str {
    index.and_then(|i| record.get(i)).unwrap_or("")
}

fn parse_row(record: &csv::StringRecord, columns: &Columns) -> Option<PostRow> {
    let id = record.get(columns.id)?.trim().parse::<u64>().ok()?;
    let md5 = record.get(columns.md5)?.trim().to_ascii_lowercase();
    if md5.len() != 32 || !md5.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let parent_id = field(record, columns.parent_id)
        .trim()
        .parse::<u64>()
        .ok();
    let sources = field(record, columns.source)
        .split('\n')
        .map(str::trim)
        .filter(|source| !source.is_empty())
        .map(str::to_string)
        .collect();

    Some(PostRow {
        id,
        md5,
        rating: field(record, columns.rating).trim().to_string(),
        description: field(record, columns.description).to_string(),
        sources,
        tag_string: field(record, columns.tag_string).to_string(),
        parent_id,
    })
}

/// Maps a Danbooru tag category id to its namespace identity. Unknown
/// categories fall back to General, matching the live plugin's behaviour
/// of only knowing the categories it parses from the API.
fn category_ident(category: u8) -> NsIdent {
    match category {
        1 => NsIdent::Artist,
        3 => NsIdent::Copyright,
        4 => NsIdent::Character,
        5 => NsIdent::Species,
        6 => NsIdent::Invalid,
        7 => NsIdent::Meta,
        8 => NsIdent::Lore,
        _ => NsIdent::General,
    }
}

/// Loads `tags.csv` (`id,name,category,...`) into a tag-name -> category-id
/// map. A missing/unreadable file yields an empty map (every tag is then
/// categorized as General) rather than failing the whole import.
fn load_tag_categories(tags_path: Option<&Path>) -> HashMap<String, u8> {
    let Some(path) = tags_path else {
        return HashMap::new();
    };
    let mut reader = match csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
    {
        Ok(reader) => reader,
        Err(error) => {
            let _ = client::log_silent(format!(
                "e6 csv import: could not open tags file {} ({error}); \
                 all tags will use the General namespace.",
                path.display()
            ));
            return HashMap::new();
        }
    };

    let mut categories = HashMap::new();
    for record in reader.records() {
        let Ok(record) = record else { continue };
        let (Some(name), Some(category)) = (record.get(1), record.get(2)) else {
            continue;
        };
        if let Ok(category) = category.trim().parse::<u8>() {
            categories.insert(name.to_string(), category);
        }
    }
    categories
}

/// Builds the file tags for one post, mirroring exactly what the live
/// parser (`parser_call` in lib.rs) attaches to a post's file: the post id,
/// rating, description, sources and categorised tags — all as bare tags in
/// a single Add action.
fn row_tags(
    row: &PostRow,
    site: &Site,
    categories: &HashMap<String, u8>,
) -> Vec<PluginTag> {
    let mut seen = HashSet::new();
    let mut tags = Vec::new();
    let mut push = |name: String, namespace: NsIdent| {
        let tag = Tag {
            name,
            namespace: nsobjplg(&namespace, site),
        };
        // The same tag can repeat within a row (e.g. a tag that is also a
        // source); the live parser de-duplicates through its HashSet too.
        if seen.insert(tag.clone()) {
            tags.push(PluginTag {
                tag,
                ..Default::default()
            });
        }
    };

    push(row.id.to_string(), NsIdent::PostId);
    if !row.rating.is_empty() {
        push(row.rating.clone(), NsIdent::Rating);
    }
    if !row.description.trim().is_empty() {
        push(row.description.clone(), NsIdent::Description);
    }
    for source in &row.sources {
        push(source.clone(), NsIdent::Sources);
    }
    for raw_tag in row.tag_string.split_whitespace() {
        let namespace = categories
            .get(raw_tag)
            .map(|category| category_ident(*category))
            .unwrap_or(NsIdent::General);
        push(raw_tag.to_string(), namespace);
    }

    tags
}

fn import_posts(
    site: &Site,
    posts_path: &Path,
    tags_path: Option<&Path>,
) -> Result<(ImportStats, bool), Box<dyn Error>> {
    let categories = load_tag_categories(tags_path);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(posts_path)?;
    let headers = reader.headers()?.clone();
    let columns = Columns {
        id: column_index(&headers, "id").ok_or("posts.csv has no `id` column")?,
        md5: column_index(&headers, "md5").ok_or("posts.csv has no `md5` column")?,
        rating: column_index(&headers, "rating"),
        tag_string: column_index(&headers, "tag_string"),
        source: column_index(&headers, "source"),
        description: column_index(&headers, "description"),
        parent_id: column_index(&headers, "parent_id"),
    };

    let mut stats = ImportStats::default();
    let mut batch: Vec<PostRow> = Vec::with_capacity(BATCH_SIZE);
    let mut completed = true;

    for record in reader.records() {
        let Ok(record) = record else {
            stats.invalid += 1;
            continue;
        };
        stats.rows_seen += 1;
        match parse_row(&record, &columns) {
            Some(row) => batch.push(row),
            None => stats.invalid += 1,
        }

        if batch.len() >= BATCH_SIZE {
            process_batch(site, &batch, &categories, &mut stats)?;
            batch.clear();
            let _ = client::log_silent(format!(
                "e6 csv import {site}: {} rows seen, {} matched, {} skipped (no downloaded file).",
                stats.rows_seen, stats.matched, stats.unmatched,
            ));
            if client::should_exit()? {
                let _ = client::log_silent(format!(
                    "e6 csv import {site}: shutdown requested; stopping early \
                     (ShouldRun stays True so a later boot can finish)."
                ));
                completed = false;
                break;
            }
        }
    }

    if completed && !batch.is_empty() {
        process_batch(site, &batch, &categories, &mut stats)?;
    }

    Ok((stats, completed))
}

/// Parent/children relationships for one post, mirroring the live parser:
/// the `Parent_id` tag pointing at the parent post and the `Children` tag
/// pointing back at this post from the parent's side.
fn parent_tags_for_row(row: &PostRow, site: &Site) -> Vec<PluginTag> {
    let mut tags = Vec::new();
    let Some(parent_id) = row.parent_id else {
        return tags;
    };
    let post_id_tag = Tag {
        name: row.id.to_string(),
        namespace: nsobjplg(&NsIdent::PostId, site),
    };
    tags.push(PluginTag {
        tag: Tag {
            name: parent_id.to_string(),
            namespace: nsobjplg(&NsIdent::Parent, site),
        },
        relates_to: Some(RelationContext {
            tag: post_id_tag.clone(),
            ..Default::default()
        }),
        ..Default::default()
    });
    tags.push(PluginTag {
        tag: Tag {
            name: row.id.to_string(),
            namespace: nsobjplg(&NsIdent::Children, site),
        },
        relates_to: Some(RelationContext {
            tag: Tag {
                name: parent_id.to_string(),
                namespace: nsobjplg(&NsIdent::PostId, site),
            },
            ..Default::default()
        }),
        ..Default::default()
    });
    tags
}

fn process_batch(
    site: &Site,
    batch: &[PostRow],
    categories: &HashMap<String, u8>,
    stats: &mut ImportStats,
) -> Result<(), Box<dyn Error>> {
    // Bulk-resolve every post's md5 to an already-downloaded file: first via
    // the MD5 identifying hash, then (for libraries whose files store the
    // md5 as their primary hash instead) via the primary hash itself.
    let keys: Vec<(String, String)> = batch
        .iter()
        .map(|row| ("MD5".to_string(), row.md5.clone()))
        .collect();
    let mut found = client::hashes_files_get_bulk(keys)?;
    let md5s: Vec<String> = batch.iter().map(|row| row.md5.clone()).collect();
    for (hash, file) in client::files_get_by_hash(md5s)? {
        found.entry(("MD5".to_string(), hash)).or_insert(file);
    }

    let mut tags_by_file: HashMap<u64, Vec<FileTagAction>> = HashMap::new();
    let mut parent_tags: Vec<PluginTag> = Vec::new();

    for row in batch {
        let Some(file) = found.get(&("MD5".to_string(), row.md5.clone())) else {
            // Never downloaded: count it, but do not invent a file record.
            stats.unmatched += 1;
            continue;
        };
        let Some(file_id) = file.id else {
            stats.unmatched += 1;
            continue;
        };
        stats.matched += 1;

        let tags = row_tags(row, site, categories);
        if !tags.is_empty() {
            tags_by_file
                .entry(file_id)
                .or_default()
                .push(FileTagAction {
                    operation: TagOperation::Add,
                    tags,
                });
        }

        // Parent/children relationships, stored exactly like the live parser.
        parent_tags.extend(parent_tags_for_row(row, site));
    }

    if !tags_by_file.is_empty() {
        client::put_tags_to_files(tags_by_file)?;
    }
    if !parent_tags.is_empty() {
        client::tag_actions_add(vec![FileTagAction {
            operation: TagOperation::Add,
            tags: parent_tags,
        }])?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn categories() -> HashMap<String, u8> {
        HashMap::from([
            ("dragon".to_string(), 5),   // species
            ("krystal".to_string(), 4),  // character
            ("someartist".to_string(), 1), // artist
        ])
    }

    fn tag_namespaces(tags: &[PluginTag]) -> Vec<(String, String)> {
        tags.iter()
            .map(|tag| {
                (
                    tag.tag.name.clone(),
                    tag.tag.namespace.name.clone(),
                )
            })
            .collect()
    }

    #[test]
    fn row_tags_mirror_the_live_parser() {
        let row = PostRow {
            id: 42,
            md5: "0554113ffa2043d3477b86a171d9b087".into(),
            rating: "s".into(),
            description: "A description".into(),
            sources: vec![
                "https://example.com/a".into(),
                "https://example.com/b".into(),
            ],
            tag_string: "dragon krystal someartist anthro dragon".into(),
            parent_id: Some(7),
        };

        let tags = row_tags(&row, &Site::E6AI, &categories());
        let names = tag_namespaces(&tags);

        // Post id, rating, description and sources are bare tags.
        assert!(names.contains(&("42".into(), "E6AI_Id".into())));
        assert!(names.contains(&("s".into(), "E6AI_Rating".into())));
        assert!(names.contains(&("A description".into(), "E6AI_Description".into())));
        assert!(names.contains(&("https://example.com/a".into(), "E6AI_Sources".into())));
        assert!(names.contains(&("https://example.com/b".into(), "E6AI_Sources".into())));
        // Categorised via tags.csv; unknown tags fall back to General.
        assert!(names.contains(&("dragon".into(), "E6AI_Species".into())));
        assert!(names.contains(&("krystal".into(), "E6AI_Character".into())));
        assert!(names.contains(&("someartist".into(), "E6AI_Artist".into())));
        assert!(names.contains(&("anthro".into(), "E6AI_General".into())));
        // The repeated "dragon" is de-duplicated (live parser dedupes too).
        assert_eq!(names.iter().filter(|(name, _)| name == "dragon").count(), 1);
    }

    #[test]
    fn parent_tags_mirror_the_live_parser() {
        let row = PostRow {
            id: 42,
            md5: "0".repeat(32),
            rating: String::new(),
            description: String::new(),
            sources: Vec::new(),
            tag_string: String::new(),
            parent_id: Some(7),
        };
        let tags = parent_tags_for_row(&row, &Site::E6AI);
        assert_eq!(tags.len(), 2);
        // Parent_id(7) relating to this post's id tag.
        assert_eq!(tags[0].tag.name, "7");
        assert_eq!(tags[0].tag.namespace.name, "E6AI_Parent_id");
        let relates = tags[0].relates_to.as_ref().unwrap();
        assert_eq!(relates.tag.name, "42");
        assert_eq!(relates.tag.namespace.name, "E6AI_Id");
        // Children(42) relating to the parent post's id tag.
        assert_eq!(tags[1].tag.name, "42");
        assert_eq!(tags[1].tag.namespace.name, "E6AI_Children");
        let relates = tags[1].relates_to.as_ref().unwrap();
        assert_eq!(relates.tag.name, "7");
        assert_eq!(relates.tag.namespace.name, "E6AI_Id");

        // No parent -> no relationships.
        let orphan = PostRow {
            id: 1,
            md5: "0".repeat(32),
            rating: String::new(),
            description: String::new(),
            sources: Vec::new(),
            tag_string: String::new(),
            parent_id: None,
        };
        assert!(parent_tags_for_row(&orphan, &Site::E6AI).is_empty());
    }

    #[test]
    fn row_tags_omit_empty_rating_and_description() {
        let row = PostRow {
            id: 1,
            md5: "0".repeat(32),
            rating: String::new(),
            description: String::new(),
            sources: Vec::new(),
            tag_string: "anthro".into(),
            parent_id: None,
        };

        let names = tag_namespaces(&row_tags(&row, &Site::E6, &HashMap::new()));
        assert_eq!(
            names,
            vec![
                ("1".to_string(), "E621_Id".to_string()),
                ("anthro".to_string(), "E621_General".to_string()),
            ]
        );
    }

    #[test]
    fn category_ident_matches_danbooru_categories() {
        assert!(matches!(category_ident(0), NsIdent::General));
        assert!(matches!(category_ident(1), NsIdent::Artist));
        assert!(matches!(category_ident(3), NsIdent::Copyright));
        assert!(matches!(category_ident(4), NsIdent::Character));
        assert!(matches!(category_ident(5), NsIdent::Species));
        assert!(matches!(category_ident(6), NsIdent::Invalid));
        assert!(matches!(category_ident(7), NsIdent::Meta));
        assert!(matches!(category_ident(8), NsIdent::Lore));
        assert!(matches!(category_ident(2), NsIdent::General));
        assert!(matches!(category_ident(99), NsIdent::General));
    }

    #[test]
    fn parse_row_reads_a_posts_csv_record() {
        let headers = csv::StringRecord::from(vec![
            "id", "md5", "rating", "tag_string", "source", "description", "parent_id",
        ]);
        let columns = Columns {
            id: column_index(&headers, "id").unwrap(),
            md5: column_index(&headers, "md5").unwrap(),
            rating: column_index(&headers, "rating"),
            tag_string: column_index(&headers, "tag_string"),
            source: column_index(&headers, "source"),
            description: column_index(&headers, "description"),
            parent_id: column_index(&headers, "parent_id"),
        };
        let record = csv::StringRecord::from(vec![
            "3",
            "0554113FFA2043D3477B86A171D9B087",
            "s",
            "anthro dragon",
            "https://example.com/a\nhttps://example.com/b",
            "",
            "",
        ]);

        let row = parse_row(&record, &columns).expect("valid row");
        assert_eq!(row.id, 3);
        // md5s are lowercased so they match the stored FileHashes digests.
        assert_eq!(row.md5, "0554113ffa2043d3477b86a171d9b087");
        assert_eq!(row.sources.len(), 2);
        assert_eq!(row.parent_id, None);

        // A row without a usable md5 is invalid.
        let bad = csv::StringRecord::from(vec!["4", "not-an-md5", "s", "", "", "", ""]);
        assert!(parse_row(&bad, &columns).is_none());
    }
}
