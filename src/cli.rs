extern crate clap;

use crate::db::MainDatabase;
use crate::helper_functions;
use shared_types::DbJobRecreation;
use std::collections::BTreeMap;
use url::Url;
// use std::str::pattern::Searcher;
use clap::Parser;
use std::sync::Arc;

mod cli_structs;

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
            out.push(shared_types::ScraperParam::Url(url.to_string()));
            continue;
        }
        out.push(shared_types::ScraperParam::Normal(item.to_string()));
    }

    out
}

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

/// Returns the main argument and parses data.
pub async fn main(db: Arc<MainDatabase>) {
    let args = cli_structs::MainWrapper::parse();
    if args.a.is_none() {
        return;
    }

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

                    let job = shared_types::PluginJob {
                        time: helper_functions::get_sys_time_in_secs(),
                        reptime: time_conv(&addstruct.time),
                        priority: 10,
                        site: addstruct.site.clone(),
                        param: parse_string_to_scraperparam(&addstruct.query),
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
            cli_structs::TasksStruct::Import(_directory) => {
                /*  {
                    data.enclave_create_default_file_import();
                }
                for local_file in directory.location.iter() {
                    let mut files = HashMap::new();
                    let mut sidecars = HashSet::new();

                    let search_path = Path::new(&local_file);
                    if !search_path.exists() {
                        log::info!(format!("Cannot find file or path at: {}", &local_file));
                        return;
                    }

                    if search_path.is_file() {
                        files.insert(search_path.to_path_buf(), find_sidecar(search_path));
                        for sidecar in find_sidecar(search_path) {
                            sidecars.insert(sidecar);
                        }
                    }

                    if search_path.is_dir() {
                        for item in WalkDir::new(search_path).into_iter().filter_map(|a| a.ok()) {
                            if !item.path().is_file() {
                                continue;
                            }
                            log::info!(format!("Found item: {}", item.path().display()));
                            files.insert(item.path().to_path_buf(), find_sidecar(item.path()));
                            for sidecar in find_sidecar(item.path()) {
                                sidecars.insert(sidecar);
                            }
                        }
                    }
                    for sidecar in sidecars.iter() {
                        files.remove(sidecar);
                    }

                    log::info!("Starting to process files");
                    todo!("Will fix later");
                    // Removes any sidecar files from files
                    /*files.par_iter().for_each(|(file, sidecars)| {
                        let file_id =
                            parse_file( file, sidecars, data.clone(), globalload.clone());
                        match directory.file_action {
                            // Don't need to do anything as the default is to copy
                            shared_types::FileAction::Copy => {}
                            // Will remove source as we've already added it into the db
                            shared_types::FileAction::Move => {
                                std::fs::remove_file(file).unwrap();
                                for sidecar in sidecars {
                                    std::fs::remove_file(sidecar).unwrap();
                                }
                            }
                            // Will hardlink the file
                            shared_types::FileAction::HardLink => {
                                if let Some(fid) = file_id {
                                    let db = data.read();
                                    let location = db.get_file( &fid);
                                    if let Some(dbfile_location) = location {
                                        std::fs::remove_file(file).unwrap();
                                        std::fs::hard_link(dbfile_location, file).unwrap();
                                    }
                                }
                            }
                        }
                    });*/
                }*/
            }

            cli_structs::TasksStruct::Scraper(action) => match action {
                cli_structs::ScraperAction::Test(inp) => {
                    dbg!(&inp);
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
            cli_structs::TasksStruct::Database(_db) => {
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
                            CheckFilesEnum::StorageCheck => {
                                data.fix_storage_locations();
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
            cli_structs::TasksStruct::Csv(_csvstruct) => {}
        },
    }

    // I hate this but it works and keeps everything self contained
    //data.transaction_flush();
    // AllFields::Nothing
}
