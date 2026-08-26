use interprocess::local_socket::{GenericFilePath, tokio::prelude::*};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

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
    init_data_request(SupportedDBRequests::ExternalPluginCall(key, callbackinfo))
}

/// Asynchronously calls a host plugin callback through the IPC server.
pub fn external_plugin_call_async(
    key: String,
    callbackinfo: CallbackInfoInput,
) -> impl Future<
    Output = Result<
        HashMap<String, CallbackCustomDataReturning>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
> {
    init_data_request_async(SupportedDBRequests::ExternalPluginCall(key, callbackinfo))
}

pub fn data_size_to_b<T: bitcode::Encode + ?Sized>(data_object: &T) -> Vec<u8> {
    // let bytd = types::x_to_bytes(tmp).to_vec();
    bitcode::encode(data_object)
}
trait RequestArgument {
    fn into_request(self) -> SupportedDBRequests;
}

impl RequestArgument for SupportedDBRequests {
    fn into_request(self) -> SupportedDBRequests {
        self
    }
}

impl RequestArgument for &SupportedDBRequests {
    fn into_request(self) -> SupportedDBRequests {
        self.clone()
    }
}

pub(crate) fn init_data_request<
    T: bitcode::Encode + for<'de> bitcode::Decode<'de>,
    R: RequestArgument,
>(
    requesttype: R,
) -> Result<T, Box<dyn std::error::Error>> {
    run_async(init_data_request_async(requesttype.into_request()))
        .map_err(|error| -> Box<dyn std::error::Error> { error.to_string().into() })
}

pub(crate) async fn init_data_request_async<
    T: bitcode::Encode + for<'de> bitcode::Decode<'de>,
    R: RequestArgument,
>(
    requesttype: R,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    let requesttype = requesttype.into_request();
    let name = "/tmp/rusthydrus/rusthydrus.sock"
        .to_fs_name::<GenericFilePath>()
        .unwrap();
    let conn = LocalSocketStream::connect(name)
        .await
        .map_err(|error| error.to_string())?;
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
    send(&requesttype, &mut conn)
        .await
        .map_err(|error| error.to_string())?;

    // Recieving size Data from server
    recieve(&mut conn)
        .await
        .map_err(|error| error.to_string().into())
}

fn run_async<F: Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("failed to create Tokio runtime for IPC")
            .block_on(future)
    }
}

async fn send<T: Sized + bitcode::Encode>(
    inp: &T,
    conn: &mut BufReader<LocalSocketStream>,
) -> std::io::Result<()> {
    let byte_buf = bitcode::encode(inp);
    let size = &byte_buf.len();

    conn.get_mut().write_all(&size.to_ne_bytes()).await?;
    conn.get_mut().write_all(&byte_buf).await
}

/// Writes all data into buffer. Assumes data is preserialzied from data generic
/// function. Can be hella dangerous. Types going in and recieved have to match
/// EXACTLY.
pub async fn send_preserialize(
    inp: &[u8],
    conn: &mut BufReader<LocalSocketStream>,
) -> std::io::Result<()> {
    let mut temp = inp.len().to_ne_bytes().to_vec();
    temp.extend(inp);
    conn.get_mut().write_all(&temp).await
}

/// Returns a vec of bytes that represent an object
pub async fn recieve<T: for<'de> bitcode::Decode<'de>>(
    conn: &mut BufReader<LocalSocketStream>,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    let mut u64_b = [0u8; 8];
    conn.read_exact(&mut u64_b).await?;
    let size_of_data = u64::from_ne_bytes(u64_b);

    let mut data_b = vec![0; size_of_data as usize];
    conn.read_exact(&mut data_b).await?;

    Ok(bitcode::decode(&data_b)?)
}
