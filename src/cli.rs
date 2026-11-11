extern crate clap;

use crate::cli::cli_structs::Database;
use crate::db::{SYSTEM_DATABASE_SLURP_SITE, turso::TursoDatabase};
use crate::helper_functions;
use shared_types::DbJobRecreation;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
};
use url::Url;
use walkdir::WalkDir;
// use std::str::pattern::Searcher;
use clap::CommandFactory;
use clap::Parser;
use std::sync::Arc;

pub mod cli_structs;

const DB_SLURP_JOB_ALIAS: &str = "db-slurp";

///
/// Parses strings into NORMAL inputs as scraperparam
///
fn parse_string_to_scraperparam(input: &str) -> Vec<shared_types::ScraperParam> {
    let mut out = Vec::new();

    for item in input.split(' ') {
        // Gets a url if its a proper URL
        if let Ok(url) = Url::parse(item)
            && (url.scheme() == "http" || url.scheme() == "https")
        {
            out.push(shared_types::ScraperParam::Url(shared_types::Url {
                url: url.to_string(),
                ..Default::default()
            }));
            continue;
        }
        out.push(shared_types::ScraperParam::Normal(item.to_string()));
    }

    out
}

#[must_use]
pub fn time_conv(inp: &str) -> u64 {
    if inp.to_lowercase() == *"now" {
        return 0;
    }
    let year = 31557600;
    let month = 2629800;
    let week = 604800;
    let day = 86400;
    let hour = 3600;
    let minute = 60;
    let second = 1;
    let strings = [
        "y".to_string(),
        "mo".to_string(),
        "w".to_string(),
        "d".to_string(),
        "h".to_string(),
        "m".to_string(),
        "s".to_string(),
    ];
    let nums = [year, month, week, day, hour, minute, second];
    let mut st = inp;
    let mut ttl = 0;
    for (cnt, time) in strings.iter().enumerate() {
        if st.contains(time) {
            let tmp: Vec<&str> = st.split(time).collect();
            if tmp[0].is_empty() {
                break;
            }
            ttl += nums[cnt] * tmp[0].parse::<u64>().unwrap_or(0);
            st = tmp[1];
        }
    }

    ttl
}

/// Extension used for the storage path / DB row of an imported file.
/// Files without an extension are stored as `bin`.
#[allow(dead_code)]
fn import_extension(path: &Path) -> String {
    path.extension()
        .map(|ext| ext.to_string_lossy().into_owned())
        .filter(|ext| !ext.is_empty())
        .unwrap_or_else(|| "bin".to_string())
}

/// Imports a single file: hashes it, places it at its storage location
/// (copy / move / hardlink per `file_action`), then records it in the DB
/// exactly like a downloaded file (SHA-512 primary hash plus MD5/SHA1/SHA256
/// identifying hashes, so hash-based dedupe and lookups work).
async fn import_one_file(
    db: &TursoDatabase,
    path: &Path,
    file_action: &cli_structs::FileAction,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = match file_action {
        cli_structs::FileAction::Copy => shared_types::FileAction::Copy,
        cli_structs::FileAction::Move => shared_types::FileAction::Move,
        cli_structs::FileAction::HardLink => shared_types::FileAction::HardLink,
    };
    db.import_file(path, action)
        .await
        .map_err(|error| -> Box<dyn std::error::Error> { error.into() })
}

/// Returns the main argument and parses data.
pub async fn main(db: Arc<TursoDatabase>, plugin_manager: Arc<crate::plugins::PluginManager>) {
    let args = cli_structs::MainWrapper::parse();
    if args.a.is_none() {
        return;
    }
    let _ = run(db.as_ref(), Some(plugin_manager.as_ref()), args).await;
}

// Output every CLI command prints goes through `cli_out`, which keeps the
// daemon console output intact and, while a `run_cli_command` call is in
// flight, also appends to that call's return value.
tokio::task_local! {
    static CLI_CAPTURE: std::sync::Arc<std::sync::Mutex<String>>;
}

/// Cap on captured output so a chatty command cannot balloon the IPC reply.
const CLI_OUTPUT_CAP: usize = 256 * 1024;

fn cli_out(line: String) {
    println!("{line}");
    let _ = CLI_CAPTURE.try_with(|buffer| {
        let mut buffer = buffer.lock().unwrap_or_else(|e| e.into_inner());
        if buffer.len() < CLI_OUTPUT_CAP {
            buffer.push_str(&line);
            buffer.push('\n');
        }
    });
}

/// Splits one command line into argv: whitespace separated, `''`/`""` quoted
/// spans kept together, `\"`/`\'`/`\\` escapes inside quotes.
fn split_command(command: &str) -> Result<Vec<String>, String> {
    let mut argv: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut quoted: Option<char> = None;
    let mut escaped = false;
    for character in command.chars() {
        if escaped {
            current.push(character);
            escaped = false;
        } else if character == '\\' && quoted.is_some() {
            escaped = true;
        } else if let Some(quote) = quoted {
            if character == quote {
                quoted = None;
            } else {
                current.push(character);
            }
        } else if character == '"' || character == '\'' {
            quoted = Some(character);
        } else if character.is_whitespace() {
            if !current.is_empty() {
                argv.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
        }
    }
    if quoted.is_some() {
        return Err("unterminated quote in command".into());
    }
    if !current.is_empty() {
        argv.push(current);
    }
    Ok(argv)
}

/// Runs one CLI command given as a single command line (the same words one
/// would type after `intscrape` in a terminal) and returns everything the
/// command printed. Failures are returned as `error: ...` lines.
///
/// The daemon's plugin manager comes from the database itself, so some
/// commands (`tasks login` for a site with no loaded plugin) report that
/// instead of running. Interactive commands that read from stdin (the
/// no-`--cookies` login form) are rejected: there is no terminal here.
pub async fn run_cli_command(db: &TursoDatabase, command: &str) -> String {
    match run_cli_command_inner(db, command).await {
        Ok(output) => output,
        Err(error) => format!("error: {error}"),
    }
}

async fn run_cli_command_inner(db: &TursoDatabase, command: &str) -> Result<String, String> {
    let argv = split_command(command)?;
    if argv.is_empty() {
        return Err("no command given; run with --help for usage".into());
    }
    let parsed = match cli_structs::MainWrapper::try_parse_from(
        std::iter::once("intscrape".to_string()).chain(argv),
    ) {
        Ok(parsed) => parsed,
        Err(error) => match error.kind() {
            // --help / --version render as "errors" in clap but are the
            // output the caller asked for.
            clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => {
                return Ok(error.render().to_string());
            }
            _ => return Err(error.render().to_string()),
        },
    };
    if parsed.a.is_none() {
        return Ok(cli_structs::MainWrapper::command()
            .render_help()
            .to_string());
    }

    // One CLI run at a time: outputs would interleave otherwise, and database
    // maintenance commands assume a quiet writer.
    static CLI_RUN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _cli_guard = CLI_RUN_LOCK.lock().await;

    let plugin_manager = db.plugin_manager();
    let captured = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    // The command's own return value is discarded: everything it printed
    // is in `captured` (see `cli_out`).
    CLI_CAPTURE
        .scope(captured.clone(), run(db, plugin_manager.as_deref(), parsed))
        .await?;
    let mut output = captured.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if output.len() >= CLI_OUTPUT_CAP {
        output.push_str("\n[output truncated]");
    }
    Ok(output)
}

async fn run(
    db: &TursoDatabase,
    plugin_manager: Option<&crate::plugins::PluginManager>,
    args: cli_structs::MainWrapper,
) -> Result<String, String> {
    let Some(command) = &args.a else {
        return Err("no command given; run with --help for usage".into());
    };
    match command {
        cli_structs::Test::Job(jobstruct) => {
            match jobstruct {
                cli_structs::JobStruct::Add(addstruct) => {
                    let recreation = addstruct.recursion.as_ref().and_then(|f| match f {
                        cli_structs::DbJobRecreationClap::AlwaysTime(timestamp) => Some(
                            DbJobRecreation::AlwaysTime(timestamp.timestamp, timestamp.count),
                        ),
                        _ => None,
                    });

                    let site = if addstruct.site == DB_SLURP_JOB_ALIAS {
                        SYSTEM_DATABASE_SLURP_SITE.to_string()
                    } else {
                        addstruct.site.clone()
                    };
                    let job = shared_types::PluginJob {
                        time: helper_functions::get_sys_time_in_secs(),
                        reptime: time_conv(&addstruct.time),
                        priority: 10,
                        site,
                        param: if addstruct.site == DB_SLURP_JOB_ALIAS
                            || addstruct.site == SYSTEM_DATABASE_SLURP_SITE
                        {
                            // A database path is one value, including when it contains spaces.
                            vec![shared_types::ScraperParam::Normal(addstruct.query.clone())]
                        } else {
                            parse_string_to_scraperparam(&addstruct.query)
                        },
                        user_data: BTreeMap::new(),
                        recreation,
                    };

                    db.jobs_add_single(job).await;
                }
                cli_structs::JobStruct::AddBulk(_addstruct) => {}
                cli_structs::JobStruct::Remove(_remove) => {
                    // return shared_types::AllFields::JobsRemove(shared_types::JobsRemove { site:
                    // remove.site.to_string(), query: remove.query.to_string(), time:
                    // remove.time.to_string(), })
                }
            }
        }
        cli_structs::Test::Search(searchstruct) => match searchstruct {
            cli_structs::SearchStruct::Parent(_parent) => {}
            cli_structs::SearchStruct::Fid(_id) => {}
            cli_structs::SearchStruct::Tid(_id) => {}
            cli_structs::SearchStruct::Tag(_tag) => {}
            cli_structs::SearchStruct::Hash(_hash) => {}
        },
        cli_structs::Test::Tasks(taskstruct) => match taskstruct {
            cli_structs::TasksStruct::Import(directory) => {
                // Collect every file under the given locations. `.txt`/`.json`
                // sidecars are companions to a media file (never imported
                // themselves), so they are taken out of the file set.
                let mut files = HashSet::new();
                let mut sidecars = HashMap::new();

                for location in &directory.location {
                    for file in WalkDir::new(location)
                        .into_iter()
                        .filter_map(std::result::Result::ok)
                    {
                        if file.file_type().is_file() {
                            files.insert(file.path().to_path_buf());
                        }
                    }
                }

                for file in files.clone() {
                    for ext in ["txt", "json"] {
                        let test_path =
                            Path::new(&format!("{}.{}", file.to_string_lossy(), ext)).to_path_buf();
                        if files.contains(&test_path) {
                            files.remove(&test_path);
                            sidecars.insert(file.clone(), test_path);
                        }
                    }
                }

                // Sort so imports (and their log output) are deterministic.
                let mut files: Vec<_> = files.into_iter().collect();
                files.sort();

                let mut imported = 0usize;
                let mut failed = 0usize;
                for file in &files {
                    match import_one_file(db, file, &directory.file_action).await {
                        Ok(()) => {
                            imported += 1;
                            cli_out(format!("Imported {}", file.display()));
                        }
                        Err(error) => {
                            failed += 1;
                            cli_out(format!("Failed to import {}: {error}", file.display()));
                            log::error!("Failed to import {}: {error}", file.display());
                        }
                    }
                }

                cli_out(format!(
                    "File import finished: {imported} imported, {failed} failed, {} sidecars skipped.",
                    sidecars.len()
                ));
            }
            cli_structs::TasksStruct::Scraper(action) => match action {
                cli_structs::ScraperAction::Test(inp) => {
                    cli_out(format!("{inp:?}"));
                }
            },
            cli_structs::TasksStruct::Reimport(reimp) => match reimp {
                cli_structs::Reimport::DirectoryLocation(_loc) => {}
            },
            cli_structs::TasksStruct::Database(db_action) => match db_action {
                Database::DbSlurp(slurp) => match db.db_slurp(Path::new(&slurp.source)).await {
                    Ok(counts) => cli_out(format!(
                        "Imported {} namespaces, {} tags, and {} files",
                        counts.0, counts.1, counts.2
                    )),
                    Err(error) => {
                        cli_out(format!("database slurp failed: {error}"));
                        return Err(format!("database slurp failed: {error}"));
                    }
                },
                Database::CheckFiles(action) => {
                    if let Err(error) = db.fix_internal_files(action) {
                        cli_out(format!("storage check failed: {error}"));
                        return Err(format!("storage check failed: {error}"));
                    }
                }
                Database::RecacheRoaring => {
                    db.recache_roaring_db();
                }
                Database::ScheduleBackup(backup) => {
                    db.jobs_add_single(shared_types::PluginJob {
                        time: helper_functions::get_sys_time_in_secs(),
                        reptime: time_conv(&backup.time),
                        priority: i64::MAX as u64,
                        site: crate::db::SYSTEM_DATABASE_BACKUP_SITE.to_string(),
                        param: vec![shared_types::ScraperParam::Normal(
                            backup.destination.clone(),
                        )],
                        user_data: BTreeMap::new(),
                        recreation: Some(shared_types::DbJobRecreation::AlwaysTime(
                            backup.every,
                            backup.count,
                        )),
                    })
                    .await;
                }
                Database::ScheduleHashMissing(hash_job) => {
                    db.jobs_add_single(shared_types::PluginJob {
                        time: helper_functions::get_sys_time_in_secs(),
                        reptime: time_conv(&hash_job.time),
                        priority: i64::MAX as u64,
                        site: crate::db::SYSTEM_FILE_HASH_SITE.to_string(),
                        param: Vec::new(),
                        user_data: BTreeMap::new(),
                        recreation: Some(shared_types::DbJobRecreation::AlwaysTime(
                            hash_job.every,
                            hash_job.count,
                        )),
                    })
                    .await;
                }
                _ => {}
            },
            cli_structs::TasksStruct::Login(args) => {
                let Some(plugin_manager) = plugin_manager else {
                    cli_out("plugin manager is not loaded, cannot run login here".to_string());
                    return Err("plugin manager is not loaded".into());
                };
                if args.cookies.is_none() {
                    // The prompt reads from stdin, which does not exist for a
                    // remotely-triggered command; only the --cookies file form
                    // can be used here.
                    cli_out(
                        "interactive login is not supported here; use tasks login <site> --cookies <file>"
                            .to_string(),
                    );
                    return Err("interactive login not supported here; use --cookies".into());
                }
                match plugin_manager.plugin_for_site(&args.site) {
                    Some(plugin) => {
                        if let Some(path) = &args.cookies {
                            match crate::web::manager::import_cookies_txt(db, &plugin, path) {
                                Ok(stored) => cli_out(format!(
                                    "Stored {stored} login item(s) for {}.",
                                    plugin.name
                                )),
                                Err(e) => cli_out(e.to_string()),
                            }
                        } else {
                            let stored = crate::web::manager::prompt_plugin_login(
                                db, &plugin, true, args.force,
                            );
                            cli_out(format!(
                                "Stored {stored} login item(s) for {}.",
                                plugin.name
                            ));
                        }
                    }
                    None => {
                        cli_out(format!(
                            "No plugin found for '{}'. Known sites: {}",
                            args.site,
                            plugin_manager.get_storage_sites().join(", ")
                        ));
                    }
                }
            }
            cli_structs::TasksStruct::Csv(_csvstruct) => {}
        },
    }

    // I hate this but it works and keeps everything self contained
    //data.transaction_flush();
    // AllFields::Nothing
    Ok(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn import_extension_falls_back_to_bin() {
        assert_eq!(import_extension(Path::new("photo.png")), "png");
        assert_eq!(import_extension(Path::new("archive.tar.gz")), "gz");
        assert_eq!(import_extension(Path::new("noext")), "bin");
        assert_eq!(import_extension(Path::new("trailing.")), "bin");
    }

    /// End-to-end: importing a real file copies it into storage and records
    /// it in the DB (SHA-512 primary hash + MD5/SHA1/SHA256 identifying
    /// hashes), so it can be found again by hash and is never re-imported
    /// as a duplicate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn import_one_file_records_the_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = TursoDatabase::new_with_exit(&db_path, Arc::new(AtomicBool::new(false))).await;

        // Point storage at a temp dir so the test never touches ./files.
        let storage_dir = temp_dir.path().join("storage");
        assert!(
            db.setting_set_sync(&shared_types::DbSettingsObj {
                name: "SYSTEM_file_location".into(),
                description: None,
                num: None,
                param: Some(storage_dir.to_string_lossy().into_owned()),
            })
            .await
        );

        let contents = b"not really a png, just import test bytes";
        let source = temp_dir.path().join("imported.png");
        std::fs::write(&source, contents).unwrap();

        import_one_file(&db, &source, &cli_structs::FileAction::Copy)
            .await
            .expect("import should succeed");

        // The file was copied into storage (Copy keeps the source in place).
        assert!(source.exists());
        let stored: Vec<_> = std::fs::read_dir(&storage_dir).unwrap().collect();
        assert_eq!(stored.len(), 1, "one partition dir in storage");

        // ... and recorded in the DB: findable by its MD5 identifying hash.
        let md5 = format!("{:x}", md5::compute(contents.as_slice()));
        let found = db
            .hashes_files_get_sync(&[shared_types::HashesSupported::Md5(md5)])
            .await;
        let file = found
            .get(&(
                "MD5".to_string(),
                format!("{:x}", md5::compute(contents.as_slice())),
            ))
            .expect("imported file should resolve by md5");
        assert!(file.id.is_some());
        assert_eq!(file.extension, "png");

        // Re-importing is idempotent: same file id, no error.
        import_one_file(&db, &source, &cli_structs::FileAction::Copy)
            .await
            .expect("re-import should succeed");
        let found_again = db
            .hashes_files_get_sync(&[shared_types::HashesSupported::Md5(format!(
                "{:x}",
                md5::compute(contents.as_slice())
            ))])
            .await;
        assert_eq!(
            found_again
                .get(&(
                    "MD5".to_string(),
                    format!("{:x}", md5::compute(contents.as_slice()))
                ))
                .and_then(|file| file.id),
            file.id
        );
    }

    /// Move removes the source once the file is stored; a re-import of the
    /// same content (already stored) still succeeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn import_one_file_move_removes_source() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = TursoDatabase::new_with_exit(&db_path, Arc::new(AtomicBool::new(false))).await;
        let storage_dir = temp_dir.path().join("storage");
        assert!(
            db.setting_set_sync(&shared_types::DbSettingsObj {
                name: "SYSTEM_file_location".into(),
                description: None,
                num: None,
                param: Some(storage_dir.to_string_lossy().into_owned()),
            })
            .await
        );

        let source = temp_dir.path().join("todelete.bin");
        std::fs::write(&source, b"move me").unwrap();
        import_one_file(&db, &source, &cli_structs::FileAction::Move)
            .await
            .expect("move import should succeed");
        assert!(!source.exists(), "move should remove the source");
    }

    /// The IPC CLI runner: `--help` comes back as output, bad input as
    /// `error:` lines, and a real command runs against the database.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_cli_command_help_and_errors() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = TursoDatabase::new_with_exit(
            &temp_dir.path().join("test.db"),
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let help = run_cli_command(&db, "--help").await;
        assert!(
            help.contains("Usage"),
            "help should render usage, got: {help}"
        );

        assert!(run_cli_command(&db, "").await.starts_with("error:"));
        assert!(
            run_cli_command(&db, "not-a-command")
                .await
                .starts_with("error:")
        );
        assert!(
            run_cli_command(&db, "job add 'unterminated")
                .await
                .starts_with("error:")
        );
    }

    /// A real command through the runner: adding a job lands in the DB.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_cli_command_adds_a_job() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = TursoDatabase::new_with_exit(
            &temp_dir.path().join("test.db"),
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let output = run_cli_command(&db, "job add testsite \"male\" now").await;
        assert!(!output.starts_with("error:"), "job add failed: {output}");
        let jobs = db.jobs_get_all().await;
        assert!(
            jobs.iter().any(|job| job.config.site == "testsite"),
            "expected the queued job in the db, got {jobs:?}"
        );
    }

    /// Interactive login prompts need stdin, which a remote caller does not
    /// have: the runner refuses instead of hanging.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_cli_command_rejects_interactive_login() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = TursoDatabase::new_with_exit(
            &temp_dir.path().join("test.db"),
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        let output = run_cli_command(&db, "tasks login testsite").await;
        assert!(
            output.starts_with("error:"),
            "interactive login must be refused, got: {output}"
        );
    }
}
