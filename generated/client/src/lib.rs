use interprocess::local_socket::{GenericFilePath, tokio::prelude::*};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use shared_types::*;

mod generated_api;
pub use generated_api::*;

pub const SOCKET_NAME: &str = "RustHydrus.sock";

include!(concat!(env!("OUT_DIR"), "/supported_db_requests.rs"));

// ---------------------------------------------------------------------------
// IPC channels
//
// The host reads the `ipc_channel_count` database setting at startup and calls
// `set_ipc_channel_count`, then listens on that many independent sockets. Each
// request is routed to the least-loaded channel (fewest in-flight requests
// from this process), so a long-running task only occupies the channel it
// landed on and new work goes to a free channel instead of queueing behind it.
// ---------------------------------------------------------------------------

/// Name of the database setting (`DbSettingsObj.num`) controlling how many
/// independent IPC channels the host starts and clients pick from.
pub const IPC_CHANNELS_SETTING: &str = "ipc_channel_count";
/// Channel count used when the setting is absent / unset.
pub const IPC_CHANNEL_DEFAULT: usize = 10;
/// Safety clamp on configured channel counts.
pub const IPC_CHANNEL_MAX: usize = 64;
/// Selector value meaning "pick the least-loaded channel".
pub const IPC_CHANNEL_AUTO: usize = usize::MAX;

/// Channel count the IPC layers agree on. The host sets it from the database
/// at startup; clients read it per request.
static CHANNEL_COUNT: AtomicUsize = AtomicUsize::new(IPC_CHANNEL_DEFAULT);

/// Configures how many IPC channels to use (clamped to `1..=IPC_CHANNEL_MAX`).
/// Called by the host once it has read the `ipc_channel_count` database
/// setting.
pub fn set_ipc_channel_count(count: usize) {
    CHANNEL_COUNT.store(count.clamp(1, IPC_CHANNEL_MAX), Ordering::Relaxed);
}

/// Number of IPC channels in use.
pub fn ipc_channel_count() -> usize {
    CHANNEL_COUNT.load(Ordering::Relaxed)
}

/// Filesystem path for an IPC channel socket. Channel 0 keeps the historic
/// `rusthydrus.sock` name so older external clients still work.
pub fn channel_socket_path(channel: usize) -> String {
    let base = "/tmp/rusthydrus/rusthydrus.sock";
    if channel == 0 {
        base.to_string()
    } else {
        format!("{base}.{channel}")
    }
}

/// In-flight request count per channel, kept in sync by [`ChannelLoadGuard`]
/// so new requests can be routed to the least-loaded channel.
static CHANNEL_LOAD: [AtomicUsize; IPC_CHANNEL_MAX] =
    [const { AtomicUsize::new(0) }; IPC_CHANNEL_MAX];
/// Rotating tie-break cursor so equally-loaded channels share traffic instead
/// of stacking on the lowest index.
static RESOLVE_CURSOR: AtomicUsize = AtomicUsize::new(0);

/// Marks a channel busy for the duration of one request: increments on
/// creation and decrements on drop, so the load unwinds on every exit path.
struct ChannelLoadGuard {
    channel: usize,
}

impl ChannelLoadGuard {
    fn new(channel: usize) -> Self {
        CHANNEL_LOAD[channel].fetch_add(1, Ordering::Relaxed);
        Self { channel }
    }
}

impl Drop for ChannelLoadGuard {
    fn drop(&mut self) {
        CHANNEL_LOAD[self.channel].fetch_sub(1, Ordering::Relaxed);
    }
}

/// Picks the channel with the fewest in-flight requests. Ties resolve to the
/// first lowest-load channel found, starting the scan from a rotating cursor
/// so a burst spreads across sockets.
fn least_busy_channel(count: usize) -> usize {
    if count <= 1 {
        return 0;
    }
    let start = RESOLVE_CURSOR.fetch_add(1, Ordering::Relaxed) % count;
    let mut best = start;
    let mut best_load = CHANNEL_LOAD[start].load(Ordering::Relaxed);
    for offset in 1..count {
        let channel = (start + offset) % count;
        let load = CHANNEL_LOAD[channel].load(Ordering::Relaxed);
        if load < best_load {
            best = channel;
            best_load = load;
        }
    }
    best
}

/// Resolves a channel selector to a concrete channel index. `IPC_CHANNEL_AUTO`
/// (the default) picks the least-loaded channel; explicit indices pass through
/// unchanged so callers can pin a request to a specific channel when they
/// need to.
fn resolve_channel(channel: usize) -> usize {
    if channel == IPC_CHANNEL_AUTO {
        least_busy_channel(ipc_channel_count())
    } else {
        channel
    }
}

/// Calls a host plugin callback through the IPC server.
///
/// Plugin callbacks are arbitrary external code and may run for a long time.
/// They ride the least-loaded channel, so a slow callback only ever occupies
/// one channel while the others keep serving quick requests.
pub fn external_plugin_call(
    key: String,
    callbackinfo: CallbackInfoInput,
) -> Result<HashMap<String, CallbackCustomDataReturning>, Box<dyn std::error::Error>> {
    init_data_request_on_channel(
        SupportedDBRequests::ExternalPluginCall(key, callbackinfo),
        IPC_CHANNEL_AUTO,
    )
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
    init_data_request_async_on_channel(
        SupportedDBRequests::ExternalPluginCall(key, callbackinfo),
        IPC_CHANNEL_AUTO,
    )
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

/// Default entry point: routes over `IPC_CHANNEL_AUTO` (a uniformly random
/// channel), so no single socket accumulates traffic.
pub(crate) fn init_data_request<
    T: bitcode::Encode + for<'de> bitcode::Decode<'de>,
    R: RequestArgument,
>(
    requesttype: R,
) -> Result<T, Box<dyn std::error::Error>> {
    init_data_request_on_channel(requesttype, IPC_CHANNEL_AUTO)
}

pub(crate) async fn init_data_request_async<
    T: bitcode::Encode + for<'de> bitcode::Decode<'de>,
    R: RequestArgument,
>(
    requesttype: R,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    init_data_request_async_on_channel(requesttype, IPC_CHANNEL_AUTO).await
}

/// Sends one request over a specific IPC channel and waits for its response.
///
/// `channel` is a concrete channel index or `IPC_CHANNEL_AUTO` to pick one at
/// random. Pinning an explicit channel is useful for tests and callers that
/// need deterministic placement; normal traffic should use `IPC_CHANNEL_AUTO`.
/// A request on one channel never blocks requests on another.
pub fn init_data_request_on_channel<
    T: bitcode::Encode + for<'de> bitcode::Decode<'de>,
    R: RequestArgument,
>(
    requesttype: R,
    channel: usize,
) -> Result<T, Box<dyn std::error::Error>> {
    run_async(init_data_request_async_on_channel(
        requesttype.into_request(),
        channel,
    ))
    .map_err(|error| -> Box<dyn std::error::Error> { error.to_string().into() })
}

/// Async sibling of [`init_data_request_on_channel`].
pub async fn init_data_request_async_on_channel<
    T: bitcode::Encode + for<'de> bitcode::Decode<'de>,
    R: RequestArgument,
>(
    requesttype: R,
    channel: usize,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    let requesttype = requesttype.into_request();
    let channel = resolve_channel(channel);
    // Count this request against the channel's load for the whole round trip
    // (including any time spent queued at the server), so the next request
    // avoids a channel that is busy or backed up. Unclamped explicit channel
    // indices simply skip load tracking.
    let _load = (channel < IPC_CHANNEL_MAX).then(|| ChannelLoadGuard::new(channel));
    let name = channel_socket_path(channel)
        .to_fs_name::<GenericFilePath>()
        .unwrap();
    let conn = LocalSocketStream::connect(name)
        .await
        .map_err(|error| error.to_string())?;
    let mut conn = BufReader::new(conn);
    send(&requesttype, &mut conn)
        .await
        .map_err(|error| error.to_string())?;
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
    let mut frame = Vec::with_capacity(std::mem::size_of::<usize>() + byte_buf.len());
    frame.extend_from_slice(&byte_buf.len().to_ne_bytes());
    frame.extend_from_slice(&byte_buf);
    conn.get_mut().write_all(&frame).await
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
