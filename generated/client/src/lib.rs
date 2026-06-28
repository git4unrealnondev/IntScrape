use interprocess::local_socket::ToNsName;
use interprocess::local_socket::{GenericNamespaced, prelude::*};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

use shared_types::*;

pub const SOCKET_NAME: &str = "RustHydrus.sock";

pub fn test() -> bool {
    init_data_request(&SupportedDBRequests::Test)
}

#[derive(Debug, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub enum SupportedDBRequests {
    GetTagId(u64),
    PutTag(String, u64, Option<u64>),
    PutTagRelationship(u64, String, u64, Option<u64>),
    GetTagName((String, u64)),
    RelationshipAdd(u64, u64),
    RelationshipRemove(u64, u64),
    RelationshipGetTagid(u64),
    RelationshipGetFileid(u64),
    GetFile(u64),
    GetFileExt(u64),
    GetFileHash(String),
    GetNamespace(String),
    CreateNamespace(String, Option<String>),
    GetNamespaceTagIDs(u64),
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
    GetFileLocation(u64),
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
    /// Gets a setting by name
    setting_get(name: String) -> Option<DbSettingsObj> => SupportedDBRequests::SettingsGetName(name);

    /// Searches the DB from tags -> file ids
    search_db_files(search: SearchObj, limit: Option<u64>) -> Vec<u64> => SupportedDBRequests::SearchFiles(search, limit);

    /// Sets a setting by setting
    setting_set(setting: DbSettingsObj) -> bool => SupportedDBRequests::SettingsSet(setting);

    /// Logs to fast_log without printing
    log_silent(log: String) -> bool => SupportedDBRequests::LoggingNoPrint(log);

    /// Gets a tag
    get_tag_id(id: u64) -> Option<u64> => SupportedDBRequests::GetTagId(id);

    /// Puts a tag into the db
    put_tag(name: String, id: u64, parent: Option<u64>) -> bool => SupportedDBRequests::PutTag(name, id, parent);

        /// Adds a relationship to the db
    relationship_add(id1: u64, id2: u64) -> bool => SupportedDBRequests::RelationshipAdd(id1, id2);

    /// Gets tag_id where namespace id is x
    get_namespace_tag_ids(id: u64) -> Vec<u64> => SupportedDBRequests::GetNamespaceTagIDs(id);

}

pub fn data_size_to_b<T: bitcode::Encode + ?Sized>(data_object: &T) -> Vec<u8> {
    // let bytd = types::x_to_bytes(tmp).to_vec();
    bitcode::encode(data_object)
}
fn init_data_request<T: bitcode::Encode + for<'de> bitcode::Decode<'de>>(
    requesttype: &SupportedDBRequests,
) -> T {
    let name = SOCKET_NAME.to_ns_name::<GenericNamespaced>().unwrap();
    let conn;
    loop {
        // Wait indefinitely for this to get a connection. shit way of doing it will
        // likely add a wait or something this will likely block the CPU or something.

        if let Ok(conn_out) = LocalSocketStream::connect(name.clone()) {
            conn = conn_out;
            break;
        }
    }
    // Wrap it into a buffered reader right away so that we could read a single line
    // out of it.
    let mut conn = BufReader::new(conn);

    // Requesting data from server.
    send(requesttype, &mut conn);

    // Recieving size Data from server
    match recieve(&mut conn) {
        Ok(out) => out,
        Err(err) => {
            dbg!(err, requesttype);
            panic!();
        }
    }
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
) -> Result<T, bitcode::Error> {
    let mut u64_b = [0u8; 8];
    conn.read_exact(&mut u64_b).unwrap();
    let size_of_data = u64::from_ne_bytes(u64_b);

    let mut data_b = vec![0; size_of_data as usize];
    conn.read_exact(&mut data_b).unwrap();

    bitcode::decode(&data_b)
}
