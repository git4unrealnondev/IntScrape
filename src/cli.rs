extern crate clap;

use crate::cli::cli_structs::Database;
use crate::helper_functions;
use crate::{
    db::{SYSTEM_DATABASE_SLURP_SITE, turso::TursoDatabase},
};
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

    // for each in 0..strings.len() { dbg!(&each); if !st.contains(&strings[each]) {
    // continue; } let tmp: Vec<&str> = st.split(&strings[each]).collect(); if
    // tmp[0].is_empty() { break; } combine += nums[each] *
    // tmp[0].parse::`<u64>`().unwrap(); st = tmp[1].to_string(); } combine
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

/// Output every CLI command prints goes through [`cli_out`], which keeps the
/// daemon console output intact and, while a [`run_cli_command`] call is in
/// flight, also appends to that call's return value.
tokio::task_local! {
    static CLI_CAPTURE: std::sync::Arc<std::sync::Mutex<String>>;
}

/// Cap on captured output so a chatty command cannot balloon the IPC reply.
const CLI_OUTPUT_CAP: usize = 256 * 1024;

fn cli_out(line: String) {
    println!("{line}");
    let _ = CLI_CAPTURE.try_with(|buffer| {
        let mut buffer = buffer.lock().unwrap();
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
                return Ok(error.render().to_string())
            }
            _ => return Err(error.render().to_string()),
        },
    };
    if parsed.a.is_none() {
        return Ok(cli_structs::MainWrapper::command().render_help().to_string());
    }

    // One CLI run at a time: outputs would interleave otherwise, and database
    // maintenance commands assume a quiet writer.
    static CLI_RUN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _cli_guard = CLI_RUN_LOCK.lock().await;

    let plugin_manager = db.plugin_manager();
    let captured = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let output = CLI_CAPTURE
        .scope(captured.clone(), run(db, plugin_manager.as_deref(), parsed))
        .await?;
    let mut output = captured.lock().unwrap().clone();
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

    match &args.a.as_ref().unwrap() {
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

                    /*  let mut system_data = BTreeMap::new();
                    for each in addstruct.system_data.chunks(2) {
                        system_data.insert(each[0].clone(), each[1].clone());
                    }
                    let (_jobtype, jobsmanager) =
                        return_jobtypemanager(addstruct.jobtype, addstruct.recursion.as_ref());
                    data.jobs_add(
                        None,
                        crate::time_func::time_secs(),
                        crate::time_func::time_conv(&addstruct.time),
                        DEFAULT_PRIORITY,
                        DEFAULT_CACHETIME,
                        DEFAULT_CACHECHECK,
                        addstruct.site.clone(),
                        parse_string_to_scraperparam(&addstruct.query),
                        system_data,
                        BTreeMap::new(),
                        jobsmanager.clone(),
                    );*/
                }
                cli_structs::JobStruct::AddBulk(_addstruct) => {
                    /*  let (_jobtype, jobsmanager) =
                        return_jobtypemanager(addstruct.jobtype, addstruct.recursion.as_ref());
                    //  for bulk in addstruct.bulkadd.iter() {
                    //     let mut vars = HashMap::new();
                    //     vars.insert("inject".to_string(), bulk.to_string());
                    //     let temp = addstruct.query.format(&vars);
                    //    if let Ok(ins) = temp {
                    data.jobs_add(
                        None,
                        crate::time_func::time_secs(),
                        crate::time_func::time_conv(&addstruct.time),
                        DEFAULT_PRIORITY,
                        DEFAULT_CACHETIME,
                        DEFAULT_CACHECHECK,
                        addstruct.site.clone(),
                        parse_string_to_scraperparam(&addstruct.query),
                        //  parse_string_to_scraperparam(&ins),
                        BTreeMap::new(),
                        BTreeMap::new(),
                        jobsmanager.clone(),
                    );
                    //  }
                    //    }*/
                }
                cli_structs::JobStruct::Remove(_remove) => {
                    // return shared_types::AllFields::JobsRemove(shared_types::JobsRemove { site:
                    // remove.site.to_string(), query: remove.query.to_string(), time:
                    // remove.time.to_string(), })
                }
            }
        }
        cli_structs::Test::Search(searchstruct) => match searchstruct {
            cli_structs::SearchStruct::Parent(_parent) => {
                /* match &data.tag_get_name(parent.tag.clone(), parent.namespace) {
                    None => {
                        dbg!("Cannot find tag.");
                    }
                    Some(tid) => {
                        dbg!("rel_get");

                        // let mut col = Vec::new(); let mut ucol = Vec::new();
                        for each in data.parents_rel_get(tid).iter() {
                            dbg!(each, data.tag_id_get(each).unwrap());
                        }
                        dbg!("tag_get");
                        for each in data.parents_tag_get(tid).iter() {
                            dbg!(each, data.tag_id_get(each).unwrap());
                        }
                    }
                }*/
            }
            cli_structs::SearchStruct::Fid(_id) => {
                /* let hstags = data.relationship_get_tagid(&id.id);
                if hstags.is_empty() {
                    println!(
                        "Cannot find any loaded relationships for fileid: {}",
                        &id.id
                    );
                } else {
                    let mut itvec: Vec<u64> = hstags.into_iter().collect();
                    itvec.sort();
                    for tid in itvec {
                        let tag = data.tag_id_get(&tid);
                        match tag {
                            None => {
                                println!("WANRING CORRUPTION DETECTED for tagid: {}", &tid);
                            }
                            Some(tagnns) => {
                                let ns = data.namespace_get_string(&tagnns.namespace).unwrap();
                                println!("ID {} Tag: {} namespace: {}", tid, tagnns.name, ns.name);
                            }
                        }
                    }
                }*/
            }
            cli_structs::SearchStruct::Tid(_id) => {
                /* let fids = data.relationship_get_fileid(&id.id);
                if !fids.is_empty() {
                    log::info!("Found Fids:");
                    for each in fids {
                        log::info!("{}", &each);
                    }
                }*/
            }
            cli_structs::SearchStruct::Tag(_tag) => {
                /*      let nsid = data.namespace_get(&tag.namespace);
                if let Some(nsid) = nsid {
                    let tid = &data.tag_get_name(tag.tag.clone(), nsid);
                    if let Some(tid) = tid {
                        let fids = data.relationship_get_fileid(tid);

                        if fids.is_empty() {
                            log::info!(
                                "Cannot find any relationships for tag id: {}",
                                &tid
                            );
                        } else {
                            log::info!("Found Fids:".to_string());
                            for each in fids {
                                log::info!("{}", &each);
                            }
                        }
                    } else {
                        log::info!("Cannot find tag :C".to_string());
                    }
                } else {
                    log::info!("Namespace isn't correct or cannot find it".to_string());
                    log::info!("Please use a namespace below:".to_string());
                }*/
            }
            cli_structs::SearchStruct::Hash(_hash) => {
                /*let file_id = data.file_get_hash(&hash.hash);
                match file_id {
                    None => {
                        println!("Cannot find hash in db: {}", &hash.hash);
                    }
                    Some(fid) => {
                        let hstags = data.relationship_get_tagid(&fid);
                        if hstags.is_empty() {
                            println!("Cannot find any loaded relationships for fileid: {}", &fid);
                        } else {
                            let mut tvec = Vec::new();
                            for tid in hstags.iter() {
                                if data.tag_id_get(tid).is_some() {
                                    tvec.push(tid)
                                }
                            }
                            tvec.sort();
                            for tid in tvec.iter() {
                                let tag = data.tag_id_get(tid);
                                match tag {
                                    None => {
                                        println!("WANRING CORRUPTION DETECTED for tagid: {}", &tid);
                                    }
                                    Some(tagnns) => {
                                        let ns =
                                            data.namespace_get_string(&tagnns.namespace).unwrap();
                                        println!(
                                            "ID {} Tag: {} namespace: {}",
                                            tid, tagnns.name, ns.name
                                        );
                                    }
                                }
                            }
                        }
                    }
                }*/
            }
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
                cli_structs::Reimport::DirectoryLocation(_loc) => {
                    /*  //let data = data.read();
                        if !Path::new(&loc.location).exists() {
                            println!("Couldn't find location: {}", &loc.location);
                        }
                        /* // Loads the scraper info for parsing.
                        let scraperlibrary = scraper.read().filter_sites_return_lib(&loc.site);
                        let libload = match scraperlibrary {
                            None => {
                                println!("Cannot find a loaded scraper. {}", &loc.site);
                                return;
                            }
                            Some(load) => load.clone(),
                        };
                        let failedtoparse: HashSet<String> = HashSet::new();
                        let file_regen = crate::globalload::scraper_file_regen(libload);
                        std::env::set_var("RAYON_NUM_THREADS", "50");
                        println!("Found location: {} Starting to process.", &loc.location);

                        // dbg!(&loc.site, &loc.location);
                        for each in jwalk::WalkDir::new(&loc.location)
                            .into_iter()
                            .filter_map(|e| e.ok())
                            .filter(|z| z.file_type().is_file())
                        {
                            // println!("{}", each.path().display()); println!("On file: {}", cnt);
                            let (fhist, b) = match download::hash_file(
                                &each.path().display().to_string(),
                                &file_regen.hash,
                            ) {
                                Ok(out) => out,
                                Err(err) => {
                                    log::info!(&format!(
                                        "Cannot hash file {} err: {:?}",
                                        &each.path().display().to_string(),
                                        err
                                    ));
                                    continue;
                                }
                            };
                            println!("File Hash: {}", &fhist);

                            // Tries to infer the type from the ext.
                            let ext = FileFormat::from_bytes(&b).extension().to_string();

                            // Error handling if we can't parse the filetyp parses the info into something the
                            // we can use for the scraper
                            let scraperinput = shared_types::ScraperFileInput {
                                hash: Some(fhist),
                                ext: Some(ext.clone()),
                            };
                            let tag = crate::globalload::scraper_file_return(libload, &scraperinput);

                            // gets sha 256 from the file.
                            let (sha2, _a) = download::hash_bytes(
                                &b,
                                &shared_types::HashesSupported::Sha512("".to_string()),
                            );
                            let filesloc = data.location_get();
                            data.storage_put(&filesloc);
                            let storage_id = data.storage_get_id(&filesloc).unwrap();

                            let ext_id = data.extension_put_string(&ext);

                            // Adds data into db
                            let file =
                                shared_types::DbFileStorage::NoIdExist(shared_types::DbFileObjNoId {
                                    hash: sha2,
                                    ext_id,
                                    storage_id,
                                });
                            let fid = data.file_add(file, true);
                            let nid =
                                data.namespace_add(tag.namespace.name, tag.namespace.description, true);
                            let tid = data.tag_add(&tag.tag, nid, true, None);
                            data.relationship_add(fid, tid, true);
                            // println!("FIle: {}", each.path().display());
                        }
                        data.transaction_flush();
                        println!("done");
                        if !failedtoparse.is_empty() {
                            println!("We've got failed items.: {}", failedtoparse.len());
                            for ke in failedtoparse.iter() {
                                println!("{}", ke);
                            }
                        }*/
                    }*/
                }
            },
            cli_structs::TasksStruct::Database(db_action) => {
                match db_action {
                    Database::DbSlurp(slurp) => {
                        match db.db_slurp(Path::new(&slurp.source)).await {
                            Ok(counts) => cli_out(format!(
                                "Imported {} namespaces, {} tags, and {} files",
                                counts.0, counts.1, counts.2
                            )),
                            Err(error) => {
                                cli_out(format!("database slurp failed: {error}"));
                                return Err(format!("database slurp failed: {error}"));
                            }
                        }
                    }
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
                }
                /* let dbstore = data.clone();
                match db {
                    // Adds extensions back onto files if they dont have them
                    cli_structs::Database::AddExtensions => {
                        log::info!("Starting to add extensions to files may take a bit.");
                        let mut ext_cache = HashMap::new();
                        for file_id in data.file_get_list_id().iter() {
                            if let Some(ref file) = data.get_file(file_id) {
                                let file_path = Path::new(file);
                                if file_path.extension().is_none() {
                                    let file_obj = data.file_get_id(file_id);
                                    let file_db = match file_obj {
                                        Some(shared_types::DbFileStorage::Exist(file)) => file,
                                        _ => continue,
                                    };

                                    let ext = match ext_cache.get(&file_db.ext_id) {
                                        None => match data.extension_get_string(&file_db.ext_id) {
                                            None => continue,
                                            Some(ext) => {
                                                ext_cache.insert(file_db.ext_id, ext.clone());
                                                ext
                                            }
                                        },
                                        Some(ext) => ext.clone(),
                                    };

                                    let new_path = file_path.with_extension(ext);
                                    log::info!(format!(
                                        "{} -> {}",
                                        file_path.to_string_lossy().to_string(),
                                        new_path.to_string_lossy().to_string()
                                    ));
                                    rename(file_path, new_path).unwrap();
                                }
                            }
                        }
                    }
                    cli_structs::Database::CheckSourceUrls(source_url_enum) => {
                        data.check_default_source_urls(source_url_enum);
                    }
                    cli_structs::Database::ConsistencyCheck => {
                        data.check_relationship_tag_relations();
                    }
                    cli_structs::Database::BackupDB => {
                        // backs up the db. check the location in setting or code if I change anything lol

                        data.backup_db();
                    }
                    cli_structs::Database::CheckFiles(action) => {
                        match action {
                            CheckFilesEnum::Redownload
                            | CheckFilesEnum::StorageCheck
                            | CheckFilesEnum::StorageCheckFileName => {
                                data.jobs_add_single_sync(shared_types::PluginJob {
                                    time: helper_functions::get_sys_time_in_secs(),
                                    reptime: 0,
                                    priority: 10,
                                    site: crate::db::SYSTEM_STORAGE_CHECK_SITE.to_string(),
                                    param: if action == CheckFilesEnum::StorageCheckFileName {
                                        vec![shared_types::ScraperParam::Normal(
                                            crate::db::SYSTEM_STORAGE_CHECK_FILENAME_MODE.to_string(),
                                        )]
                                    } else if action == CheckFilesEnum::Redownload {
                                        vec![shared_types::ScraperParam::Normal(
                                            crate::db::SYSTEM_STORAGE_CHECK_REDOWNLOAD_MODE.to_string(),
                                        )]
                                    } else {
                                        Vec::new()
                                    },
                                    user_data: BTreeMap::new(),
                                    recreation: None,
                                });
                            }
                            _ => {}
                        }
                        /*   data.check_db_paths();

                        // This will check files in the database and will see if they even exist.
                        let db_location = data.location_get();
                        let cnt: std::sync::Arc<std::sync::Mutex<u64>> =
                            std::sync::Arc::new(std::sync::Mutex::new(0));
                        if !Path::new("fileexists.txt").exists() {
                            let _ = std::fs::File::create("fileexists.txt");
                        }
                        let fiexist: std::sync::Arc<std::sync::Mutex<HashSet<u64>>> =
                            std::sync::Arc::new(std::sync::Mutex::new(
                                std::fs::read_to_string("fileexists.txt")
                                    // panic on possible file-reading errors
                                    .unwrap()
                                    // split the string into an iterator of string slices
                                    .lines()
                                    // make each slice into a string
                                    .map(|x| x.parse::<u64>().unwrap())
                                    .collect(),
                            ));
                        let f = std::sync::Arc::new(std::sync::Mutex::new(
                            std::fs::File::options()
                                .append(true)
                                .open("fileexists.txt")
                                .unwrap(),
                        ));
                        let lis = data.file_get_list_all();
                        log::info!(
                            "Checking if we have any missing or bad files.".to_string(),
                        );
                        let mut nsid: Option<u64> = None;
                        {
                            let nso = data.namespace_get(&"source_url".to_owned());
                            if let Some(ns) = nso {
                                nsid = Some(ns);
                            }
                        }

                        // Spawn default ratelimiter of 1 item per second
                        let ratelimiter_obj = Arc::new(Mutex::new(download::ratelimiter_create(
                            &0,
                            &0,
                            1,
                            std::time::Duration::from_secs(1),
                        )));
                        todo!("Will fix later");*/
                        /*lis.par_iter().for_each(|(fid, storage)| {
                            if fiexist.lock().unwrap().contains(fid) {
                                return;
                            }
                            let file = match storage {
                                shared_types::DbFileStorage::NoExistUnknown => return,
                                shared_types::DbFileStorage::NoExist(_) => return,
                                shared_types::DbFileStorage::NoIdExist(_) => return,
                                shared_types::DbFileStorage::Exist(file) => file,
                            };
                            let loc = helpers::getfinpath(&db_location, &file.hash);
                            let lispa = format!("{}/{}", loc, file.hash);
                            *cnt.lock().unwrap() += 1;
                            if *cnt.lock().unwrap() == 1000 {
                                let _ = f.lock().unwrap().flush();
                                *cnt.lock().unwrap() = 0;
                            }
                            let client = &mut download::client_create(vec![], false);
                            if !Path::new(&lispa).exists() {
                                logging::main(&format!("Cannot find hash: {}", &file.hash));
                                match action {
                                    cli_structs::CheckFilesEnum::Redownload => {}
                                    cli_structs::CheckFilesEnum::Print => {
                                        return;
                                    }
                                }
                                if let Some(nsid) = nsid {
                                    let rel = data.relationship_get_tagid( fid);
                                    for eachs in rel.iter() {
                                        let dat = data.tag_id_get( eachs).unwrap();
                                        log::info!(format!(
                                            "Got Tag: {} for fileid: {}",
                                            dat.name, fid
                                        ));
                                        if dat.namespace == nsid {
                                            let mut file = shared_types::FileObject {
                                                source: Some(shared_types::FileSource::Url(
                                                    dat.name.clone(),
                                                )),
                                                hash: shared_types::HashesSupported::Sha512(
                                                    file.hash.clone(),
                                                ),
                                                tag_list: Vec::new(),
                                                skip_if: Vec::new(),
                                            };
                                            download::dlfile_new(

                                                client,
                                                dbstore.clone(),
                                                &mut file,
                                                None,
                                                &ratelimiter_obj,
                                                &dat.name.clone(),
                                                &0,
                                                &0,
                                                None,
                                            );
                                        }
                                    }
                                }
                            } else {
                                let fil = std::fs::read(lispa).unwrap();
                                let hinfo = download::hash_bytes(
                                    &bytes::Bytes::from(fil),
                                    &shared_types::HashesSupported::Sha512(file.hash.clone()),
                                );
                                if !hinfo.1 {
                                    logging::error_log(format!(
                                        "BAD HASH: ID: {}  HASH: {}   2ND HASH: {}",
                                        &file.id, &file.hash, hinfo.0
                                    ));
                                    match action {
                                        cli_structs::CheckFilesEnum::Redownload => {}
                                        cli_structs::CheckFilesEnum::Print => {
                                            return;
                                        }
                                    }

                                    if nsid.is_some() {
                                        let rel = data.relationship_get_tagid( fid);
                                        for eachs in rel.iter() {
                                            let dat = data.tag_id_get( eachs).unwrap();
                                            log::info!(format!(
                                                "Got Tag: {} for fileid: {}",
                                                dat.name, fid
                                            ));
                                            if dat.namespace == nsid.unwrap() {
                                                let mut file = shared_types::FileObject {
                                                    source: Some(shared_types::FileSource::Url(
                                                        dat.name.clone(),
                                                    )),
                                                    hash: shared_types::HashesSupported::Sha512(
                                                        file.hash.clone(),
                                                    ),
                                                    tag_list: Vec::new(),
                                                    skip_if: Vec::new(),
                                                };
                                                download::dlfile_new(

                                                    client,
                                                    dbstore.clone(),
                                                    &mut file,
                                                    None,
                                                    &ratelimiter_obj,
                                                    &dat.name.clone(),
                                                    &0,
                                                    &0,
                                                    None,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            fiexist.lock().unwrap().insert(*fid);
                            let fout = format!("{}\n", fid).into_bytes();
                            f.lock().unwrap().write_all(&fout).unwrap();
                        });*/
                        let _ = std::fs::remove_file("fileexists.txt");
                    }
                    cli_structs::Database::CheckInMemdb => {
                        //pause();
                    }
                    cli_structs::Database::CompressDatabase => {
                        //data.condense_db_all();
                    }
                    cli_structs::Database::RemoveWhereNot(db_n_rmv) => {
                        /*let ns_id = match db_n_rmv {
                            cli_structs::NamespaceInfo::NamespaceString(ns) => {

                                match data.namespace_get(&ns.namespace_string) {
                                    None => {
                                        log::info!(format!(
                                            "Cannot find the tasks remove string in namespace {}",
                                            &ns.namespace_string
                                        ));
                                        return;
                                    }
                                    Some(id) => id,
                                }
                            }
                            cli_structs::NamespaceInfo::NamespaceId(ns) => ns.namespace_id,
                        };
                        log::info!(format!(
                            "Found Namespace: {} Removing all but id...",
                            &ns_id
                        ));

                        // data.namespace_get(inp)
                        let mut key = data.namespace_keys();
                        key.retain(|x| *x != ns_id);
                        for each in key {
                            data.delete_namespace_id(&each);
                        }
                        //data.drop_recreate_ns(&ns_id);
                        panic!();*/
                    }
                    // Removing db namespace. Will get id to remove then remove it.
                    cli_structs::Database::Remove(db_rmv) => {
                       /* let ns_id = match db_rmv {
                            cli_structs::NamespaceInfo::NamespaceString(ns) => {

                                match data.namespace_get(&ns.namespace_string) {
                                    None => {
                                        log::info!(format!(
                                            "Cannot find the tasks remove string in namespace {}",
                                            &ns.namespace_string
                                        ));
                                        return;
                                    }
                                    Some(id) => id,
                                }
                            }
                            cli_structs::NamespaceInfo::NamespaceId(ns) => ns.namespace_id,
                        };
                        log::info!(format!("Found Namespace: {} Removing...", &ns_id));
                        data.delete_namespace_id(&ns_id);*/
                    }
                    cli_structs::Database::RecacheRoaring => {
                       // data.recache_roaring();
                    }
                }*/
            }
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
                            match crate::web::manager::import_cookies_txt(&db, &plugin, path) {
                                Ok(stored) => {
                                    cli_out(format!("Stored {stored} login item(s) for {}.", plugin.name))
                                }
                                Err(e) => cli_out(format!("{e}")),
                            }
                        } else {
                            let stored = crate::web::manager::prompt_plugin_login(
                                &db, &plugin, true, args.force,
                            );
                            cli_out(format!("Stored {stored} login item(s) for {}.", plugin.name));
                        }
                    }
                    None => {
                        cli_out(
                            format!(
                                "No plugin found for '{}'. Known sites: {}",
                                args.site,
                                plugin_manager.get_storage_sites().join(", ")
                            )
                        );
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
        let stored: Vec<_> = std::fs::read_dir(&storage_dir)
            .unwrap()
            .collect();
        assert_eq!(stored.len(), 1, "one partition dir in storage");

        // ... and recorded in the DB: findable by its MD5 identifying hash.
        let md5 = format!("{:x}", md5::compute(contents.as_slice()));
        let found = db
            .hashes_files_get_sync(&[shared_types::HashesSupported::Md5(md5)])
            .await;
        let file = found
            .get(&("MD5".to_string(), format!("{:x}", md5::compute(contents.as_slice()))))
            .expect("imported file should resolve by md5");
        assert!(file.id.is_some());
        assert_eq!(file.extension, "png");

        // Re-importing is idempotent: same file id, no error.
        import_one_file(&db, &source, &cli_structs::FileAction::Copy)
            .await
            .expect("re-import should succeed");
        let found_again = db
            .hashes_files_get_sync(&[shared_types::HashesSupported::Md5(
                format!("{:x}", md5::compute(contents.as_slice())),
            )])
            .await;
        assert_eq!(
            found_again
                .get(&("MD5".to_string(), format!("{:x}", md5::compute(contents.as_slice()))))
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
        let db =
            TursoDatabase::new_with_exit(&temp_dir.path().join("test.db"), Arc::new(AtomicBool::new(false)))
                .await;

        let help = run_cli_command(&db, "--help").await;
        assert!(help.contains("Usage"), "help should render usage, got: {help}");

        assert!(run_cli_command(&db, "").await.starts_with("error:"));
        assert!(run_cli_command(&db, "not-a-command").await.starts_with("error:"));
        assert!(run_cli_command(&db, "job add 'unterminated").await.starts_with("error:"));
    }

    /// A real command through the runner: adding a job lands in the DB.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_cli_command_adds_a_job() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db =
            TursoDatabase::new_with_exit(&temp_dir.path().join("test.db"), Arc::new(AtomicBool::new(false)))
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
        let db =
            TursoDatabase::new_with_exit(&temp_dir.path().join("test.db"), Arc::new(AtomicBool::new(false)))
                .await;

        let output = run_cli_command(&db, "tasks login testsite").await;
        assert!(
            output.starts_with("error:"),
            "interactive login must be refused, got: {output}"
        );
    }






}
