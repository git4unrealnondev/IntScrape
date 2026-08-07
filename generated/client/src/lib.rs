use interprocess::local_socket::GenericFilePath;
use interprocess::local_socket::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

use shared_types::*;

mod generated_api;
pub use generated_api::*;

/// Returns whether the server is shutting down.
///
/// This is a host lifecycle request rather than a database method, so it is
/// kept alongside the generated database client API.
pub fn should_exit() -> Result<bool, Box<dyn std::error::Error>> {
    init_data_request(&SupportedDBRequests::ShouldExit)
}

pub const SOCKET_NAME: &str = "RustHydrus.sock";

#[derive(Debug, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub enum SupportedDBRequests {
    GetTagId(u64),
    GetTagFile(Tag),
    GetTagIds(HashSet<u64>),
    GetTag(String, u64),
    PutTag(String, u64, Option<u64>),
    PutTagsRelationship(u64, Vec<FileTagAction>),
    PutTagsRelationships(HashMap<u64, Vec<FileTagAction>>),
    GetTagName((String, u64)),
    RelationshipAdd(u64, u64),
    RelationshipRemove(u64, u64),
    RelationshipGetTagid(u64),
    RelationshipGetFileid(u64),
    GetFile(u64),
    GetFileExt(u64),
    GetFileHash(String),
    GetFileLocation(u64),
    GetNamespace(String),
    SetNamespace(GenericNamespaceObj),
    GetNamespaceTagIDs(u64),
    GetNamespaceFileIDs(u64),
    GetNamespaceTagIdsFiltered(u64, u64),
    GetNamespaceIds(),
    GetNamespaceString(u64),
    SettingsGetName(String),
    SettingsSet(DbSettingsObj),
    Testu64(),
    GetFileListId(),
    GetFileListAll(),
    TransactionFlush(),
    GetDBLocation(),
    Logging(String),
    LoggingNoPrint(String),
    GetFileByte(u64),
    NamespaceContainsId(u64, u64),
    FilterNamespaceById((HashSet<u64>, u64)),
    ReloadLoadedPlugins(),
    PutJob(DbJobsObj),
    GetJob(u64),
    TagDelete(u64),
    ReloadRegex,
    GetNamespaceIDsAll,
    MigrateTag((u64, u64)),
    MigrateRelationship((u64, u64, u64)),
    CondenseTags(),
    GetFileRaw(u64),
    Test,
    SearchFiles(SearchObj, Option<u64>),
    SearchTags(String, Option<u64>),
    ParentsRel(u64),
    ExternalPluginCall(String, CallbackInfoInput),
    ShouldExit,
    AddDeadUrl(String),
    GetDeadUrl(Vec<String>),
}

macro_rules! define_db_requests {
    (
        $(
            $(#[doc = $doc:expr])? // Captures optional documentation comments
            $variant:ident ( $($arg_name:ident : $arg_type:ty),* ) -> $ret_type:ty => $enum_variant:expr
        );* $(;)?
    ) => {
        // 1. Generate the functions
        $(
            $(#[doc = $doc])?
            pub fn $variant($($arg_name : $arg_type),*) -> $ret_type {
                init_data_request(&$enum_variant)
            }
        )*

        // Note: The enum itself is defined below or manually.
        // If you want the macro to also generate the enum variants automatically,
        // you can structure it to emit the enum block here.
    };
}

define_db_requests! {
    external_plugin_call(key: String, callbackinfo: CallbackInfoInput) -> Result<HashMap<String, CallbackCustomDataReturning>, Box<dyn std::error::Error>> => SupportedDBRequests::ExternalPluginCall(key, callbackinfo);

    relationship_get_fileid(tag_id: u64) -> Result<Vec<u64>, Box<dyn std::error::Error>> => SupportedDBRequests::RelationshipGetFileid(tag_id);

    /// Logs to fast_log without printing
    log_silent(log: String) -> Result<bool, Box<dyn std::error::Error>> => SupportedDBRequests::LoggingNoPrint(log);


    get_tag(name: String, namespace: u64) -> Result<Option<u64>, Box<dyn std::error::Error>> => SupportedDBRequests::GetTag(name, namespace) ;



    /// Adds a relationship to the db
    relationship_add(id1: u64, id2: u64) -> Result<bool, Box<dyn std::error::Error>> => SupportedDBRequests::RelationshipAdd(id1, id2);


    /// Gets all namespace ids in the db
    namespace_all() -> Result<Vec<GenericNamespaceObj>, Box<dyn std::error::Error>> => SupportedDBRequests::GetNamespaceIds();

    /// Gets a file object if its id exists
    get_file(file_id: u64) -> Result<Option<FileInternal>, Box<dyn std::error::Error>> => SupportedDBRequests::GetFile(file_id);

}

pub fn data_size_to_b<T: bitcode::Encode + ?Sized>(data_object: &T) -> Vec<u8> {
    // let bytd = types::x_to_bytes(tmp).to_vec();
    bitcode::encode(data_object)
}
fn init_data_request<T: bitcode::Encode + for<'de> bitcode::Decode<'de>>(
    requesttype: &SupportedDBRequests,
) -> Result<T, Box<dyn std::error::Error>> {
    let name = "/tmp/rusthydrus/rusthydrus.sock"
        .to_fs_name::<GenericFilePath>()
        .unwrap();
    let conn = LocalSocketStream::connect(name)?;
    //loop {
    // Wait indefinitely for this to get a connection. shit way of doing it will
    // likely add a wait or something this will likely block the CPU or something.

    //if let Ok(conn_out) = LocalSocketStream::connect(name.clone()) {
    //    conn = conn_out;
    //    break;
    //}
    //}
    // Wrap it into a buffered reader right away so that we could read a single line
    // out of it.
    let mut conn = BufReader::new(conn);

    // Requesting data from server.
    send(requesttype, &mut conn);

    // Recieving size Data from server
    recieve(&mut conn)
}

pub fn send<T: Sized + bitcode::Encode>(inp: &T, conn: &mut BufReader<LocalSocketStream>) {
    let byte_buf = bitcode::encode(inp);
    let size = &byte_buf.len();

    conn.get_mut().write_all(&size.to_ne_bytes()).unwrap();
    conn.get_mut().write_all(&byte_buf).unwrap();
}

/// Writes all data into buffer. Assumes data is preserialzied from data generic
/// function. Can be hella dangerous. Types going in and recieved have to match
/// EXACTLY.
pub fn send_preserialize(inp: &Vec<u8>, conn: &mut BufReader<LocalSocketStream>) {
    let mut temp = inp.len().to_ne_bytes().to_vec();
    temp.extend(inp);
    let _ = conn.get_mut().write_all(&temp);
}

/// Returns a vec of bytes that represent an object
pub fn recieve<T: for<'de> bitcode::Decode<'de>>(
    conn: &mut BufReader<LocalSocketStream>,
) -> Result<T, Box<dyn std::error::Error>> {
    let mut u64_b = [0u8; 8];
    conn.read_exact(&mut u64_b)?;
    let size_of_data = u64::from_ne_bytes(u64_b);

    let mut data_b = vec![0; size_of_data as usize];
    conn.read_exact(&mut data_b).unwrap();

    Ok(bitcode::decode(&data_b)?)
}
