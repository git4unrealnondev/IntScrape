use interprocess::local_socket::GenericFilePath;
use interprocess::local_socket::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

use shared_types::*;

mod generated_api;
pub use generated_api::*;

pub const SOCKET_NAME: &str = "RustHydrus.sock";

include!(concat!(env!("OUT_DIR"), "/supported_db_requests.rs"));

/// Calls a host plugin callback through the IPC server.
pub fn external_plugin_call(
    key: String,
    callbackinfo: CallbackInfoInput,
) -> Result<HashMap<String, CallbackCustomDataReturning>, Box<dyn std::error::Error>> {
    init_data_request(&SupportedDBRequests::ExternalPluginCall(key, callbackinfo))
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
