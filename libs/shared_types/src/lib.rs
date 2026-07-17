use redact::{Secret, expose_secret};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

pub const DEFAULT_PRIORITY: u64 = 10;

/// FFI-safe alternative to String
#[repr(C)]
pub struct CVec<T> {
    pub ptr: *mut T,
    pub len: usize,
    pub cap: usize,
}

impl<T> CVec<T> {
    /// Convert a standard Vec into an FFI safe structure (Leaks the vec memory safely)
    pub fn from_vec(mut v: Vec<T>) -> Self {
        v.shrink_to_fit();
        let ptr = v.as_mut_ptr();
        let len = v.len();
        let cap = v.capacity();
        std::mem::forget(v); // Prevent dropping the vector elements
        Self { ptr, len, cap }
    }

    /// Reconstruct the standard Vec (Takes back ownership to read or free it)
    pub unsafe fn into_vec(self) -> Vec<T> {
        if self.ptr.is_null() {
            vec![]
        } else {
            unsafe { Vec::from_raw_parts(self.ptr, self.len, self.cap) }
        }
    }
}

#[derive(Debug, Default, Copy, Clone)]
#[repr(C)]
pub struct SensorData {
    pub value: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[repr(C)]
pub enum DbRequest {
    GetUserById { client_id: u64, user_id: u64 },
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[repr(C)]
pub enum DbResponse {
    UserData { user_id: u64, balance: f64 },
}

#[derive(Deserialize, Debug, Serialize, bitcode::Encode, bitcode::Decode, Clone)]
pub struct DbSettingsObj {
    pub name: String,
    pub description: Option<String>,
    pub num: Option<u64>,
    pub param: Option<String>,
}

pub enum DbSearchTypeEnum {
    And,
    Or,
}
#[derive(Deserialize, Debug, Serialize, bitcode::Encode, bitcode::Decode, Clone)]
pub struct SearchObj {
    pub search_relate: Option<Vec<SearchHolder>>,
    pub searches: Vec<SearchHolder>,
}
#[derive(Deserialize, Debug, Serialize, bitcode::Encode, bitcode::Decode, Clone)]
pub enum SearchHolder {
    And(Vec<u64>),
    Or(Vec<u64>),
    Not(Vec<u64>),
}
pub struct DbSearchQuery {
    pub tag_one: DbSearchObject,
    pub tag_two: DbSearchObject,
    pub search_enum: DbSearchTypeEnum,
}
pub struct DbSearchObject {
    pub tag: String,
    pub namespace: Option<String>,
    pub namespace_id: Option<u64>,
}

#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub struct UrlPost {
    pub url: String,
    // Any goofball request modifiers
    pub modifiers: Vec<DownloadModifiers>,
    // any post data to send if needed
    pub post_data: String,
}

#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    bitcode::Encode,
    bitcode::Decode,
    Default,
)]
pub struct Url {
    pub url: String,
    pub local_modifiers: Vec<TargetModifier>,
}

#[derive(
    Deserialize, Debug, Serialize, Clone, PartialEq, Eq, Hash, bitcode::Encode, bitcode::Decode,
)]
pub enum ScraperParam {
    // User defined params like search terms
    Normal(String),
    // A GET url not a POST
    Url(Url),
    // Special type of url. is designed for weird requests like POST JSON and such
    UrlPost(UrlPost),
    // Login info
    Login(LoginType),
    // Some weird database call will look into later
    Database(String),
}

///
/// Gets passed around to plugins will try and not change this too to much
///
#[derive(
    Debug,
    Clone,
    Default,
    Eq,
    Hash,
    PartialEq,
    Serialize,
    Deserialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub struct PluginJob {
    // Time to run job. 0 for immediate
    pub time: u64,
    /// Time offset to run job at
    pub reptime: u64,
    /// Determines which job we should run first. Higher values go first
    pub priority: u64,
    /// Manages the recreation of jobs
    pub recreation: Option<DbJobRecreation>,
    /// Site we're processing
    pub site: String,
    /// Any params that need to get passed into the scraper, plugin etc.
    pub param: Vec<ScraperParam>,
    /// Any initial data that should be edited by the scraper, plugin etc.
    pub user_data: BTreeMap<String, String>,
}

///
/// Used as a return from a plugin when doing text scraping
///
#[derive(Clone, Default, PartialEq, Debug)]
pub enum ScraperReturn {
    // Valid data from the system
    Data(ScraperObject),
    // STOP IMMEDIENTLY: ISSUE WITH SITE : PANICS no save
    Fatal(String),
    // Hit nothing to search. Move to next job.
    #[default]
    Nothing,
    // Stop current job, Record issue Move to next.
    Stop(String),
    // Wait X seconds before retrying.
    Timeout(u64),
    // Sends job back into queue with x waiting time
    RetryLater(Duration),
}
#[derive(Default, Clone, PartialEq, Debug)]
pub struct ScraperObject {
    pub files: HashSet<FileObject>,
    pub tags: HashSet<PluginTag>,
    pub jobs: HashSet<ScraperDataReturn>,
    //pub flags: Vec<ScraperFlags>,
}

/// Gets used for the on_download callback return
#[derive(Default)]
pub struct CallbackReturn {
    pub tags: HashSet<FileTagAction>,
    pub jobs: HashSet<ScraperDataReturn>,
}

///
/// What to do for a list of tags when they come in
///
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub enum TagOperation {
    #[default]
    Add,
    Del,
    Set,
}
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct FileTagAction {
    pub operation: TagOperation,
    pub tags: Vec<PluginTag>,
}

/// Represents one file
/// Current version of FileObject
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct FileObject {
    pub source: Option<FileSource>,
    // Hash of file
    pub hash: Option<HashesSupported>,
    pub tag_list: Vec<FileTagAction>,
    // Skips downloading the file if a tag matches this.
    pub skip_if: Vec<SkipIf>,
}

#[derive(
    Default, Clone, PartialEq, Eq, Hash, Debug, Deserialize, bitcode::Encode, bitcode::Decode,
)]
pub struct FileInternal {
    pub id: Option<u64>,
    pub hash: String,
    pub extension: String,
    pub storage_id: u64,
}

#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    Deserialize,
    Serialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub struct TagSearch {
    pub tag: Tag,
    pub tag_id: u64,
    pub count: u64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FileSource {
    Url(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum HashesSupported {
    Md5(String),
    Sha1(String),
    Sha256(String),
    Sha512(String),
}

#[derive(
    Debug, Clone, Eq, Hash, PartialEq, Deserialize, Serialize, bitcode::Encode, bitcode::Decode,
)]
pub enum DbJobRecreation {
    OnTagId(u64, Option<u64>),
    OnTag(String, u64, Option<u64>),
    AlwaysTime(u64, Option<u64>),
}

///
/// Internal db obj
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct DbJobsObj {
    pub id: u64,
    pub isrunning: bool,
    pub config: PluginJob,
}

#[repr(C)]
#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub struct ScraperDataReturn {
    pub job: PluginJob,
    pub skip_conditions: Vec<SkipIf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SkipIf {
    // If a relationship between any file and tag exists.
    FileTagRelationship(Tag),
    // The tag is unique and if their are X number or more of GenericNamespaceObj
    // associated with the file Then we'll skip it
    FileNamespaceNumber((Tag, GenericNamespaceObj, u64)),
    // Skips a file if the hash X exists
    FileHash(String),
    // Skips if no files are downloaded
    NoFilesDownloaded,
    // Skips job if plugintag exists EXACTLY as it appears here
    ParentsRelate(PluginTag),
    // Skips job if a relate_tag_id and limit_to exists
    ParentsRelateLimitto((Tag, Tag)),
}
#[repr(C)]
#[derive(Debug, Clone, Eq, Hash, PartialEq, Default)]
pub struct Plugin {
    /// Name of the site (human readable plz)
    pub name: String,
    /// Which should run at what priority. Higher priority means it should run first
    //pub priority: u64,
    /// Weather this item should handle the file download
    //pub should_handle_file_download: bool,
    /// Weather this item needs to handle text scraping
    //pub should_handle_text_scraping: bool,
    /// If we should send files back when we're scraping
    //pub should_send_files_on_scrape: bool,
    /// Any data thats needed to access restricted content
    //pub login_type: Vec<(String, LoginType, LoginNeed, Option<String>, bool)>,
    /// Any callbacks that should run on any events
    pub callbacks: Vec<GlobalCallbacks>,
    /// Storage for plugin or scraper info. Determines type.
    //pub storage_type: Option<ScraperOrPlugin>,
    pub properties: Vec<PluginProperties>,
}

#[repr(C)]
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum PluginProperties {
    /// Ratelimit number of tokens per time
    Ratelimit(u32, Duration),
    /// Sets the sites that are supported by the plugin
    Sites(Vec<String>),
    /// Sets the number of concurrent downloads for text and file
    ThreadNum(u64),
    /// Changes the text or file downloading
    Modifier(TargetModifier),
    /// Tells the system that we are going to ideally process a login type
    Login((LoginNeed, LoginType)),
}

#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub enum DownloadModifiers {
    // A useragent to use when scraping text or pulling siteinfo
    Useragent(String),
    // Timeout when making a request in seconds
    Timeout(Option<Duration>),
    //,Adds a header to a download
    Header((String, String)),
}

///
/// Determines if we should apply this to a text or media entry
///
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub struct TargetModifier {
    pub target: ModifierTarget,
    pub modifier: DownloadModifiers,
}

#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub enum ModifierTarget {
    Text,
    Media,
}

///
/// Holds the login type that we need
///
#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub enum LoginType {
    Cookie(String, Option<String>),
    Api(String, Option<String>),
    ApiNamespaced(String, Option<String>, Option<String>),
    Login(String, Option<LoginUsernameOrPassword>),
    Other(String, Option<String>),
}

///
/// Data storage for the login type needed to determine if we have to use this to access a site or
/// if its just a nice to have
///
#[derive(Debug, Clone, Eq, Hash, PartialEq, Default)]
pub enum LoginNeed {
    Required,
    #[default]
    Optional,
}

/// Struct holding username and password as secrets
#[derive(
    Debug,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub struct LoginUsernameOrPassword {
    #[serde(serialize_with = "expose_secret")]
    pub username: Secret<String>,
    #[serde(serialize_with = "expose_secret")]
    pub password: Secret<String>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum GlobalCallbacks {
    // Ran when a file is downloaded
    Download,
    // Runs when a file is imported manually
    Import,
    // Starts when the software start
    Start(StartupThreadType),
    // Used for when we need to get / register a login
    LoginNeeded,
    // Custom callback to be used for cross communication
    Callback(CallbackInfo),
    // Runs when a tag has exists.
    // First when the ns exists, 2nd when the namespace does not exist
    // Use None when searching all or Some when searching restrictivly
    Tag((Option<SearchType>, Vec<String>, Vec<String>)),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct CallbackInfo {
    // Name of plugin's function
    pub func: String,
    // Version of plugin
    pub vers: u64,
    // Name of variable
    pub data_name: Vec<String>,
    // Data for variable of data_name
    pub data: Vec<CallbackCustomData>,
}

#[derive(
    Debug, PartialEq, Eq, Hash, Clone, bitcode::Encode, bitcode::Decode, Serialize, Deserialize,
)]
pub struct CallbackInfoInput {
    // Version of the expected call. Its on the plugin to handle this properly
    pub vers: u64,
    // Name of variable
    pub data_name: Vec<String>,
    // Data for variable of data_name
    pub data: Vec<CallbackCustomDataReturning>,
}

#[derive(
    Debug, PartialEq, Eq, Hash, Clone, bitcode::Encode, bitcode::Decode, Serialize, Deserialize,
)]
pub enum CallbackCustomDataReturning {
    String(String),
    U8(Vec<u8>),
    U64(u64),
    VString(Vec<String>),
    VU8(Vec<u8>),
    Vu64(Vec<u64>),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum CallbackCustomData {
    String,
    U8,
    U64,
    VString,
    VU8,
    Vu64,
    VCallback,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub enum StartupThreadType {
    // Runs plugin and waits until it finished
    Inline,
    // Spawns a new thread, Runs concurrently to the calling function.
    Spawn,
    // DEFAULT - Waits for the on_start to finish. Runs cocurently to other on_start functions
    SpawnInline,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum SearchType {
    String(String),
    Regex(String),
}

///
/// Holds namespace info
///
#[derive(
    Hash,
    Eq,
    PartialEq,
    Clone,
    Default,
    Debug,
    Deserialize,
    bitcode::Encode,
    bitcode::Decode,
    Serialize,
)]
pub struct GenericNamespaceObj {
    pub name: String,
    pub description: Option<String>,
}

///
/// Holds a property for a namespace
///
pub struct NamespaceProperty {
    pub id: Option<u64>,
    pub name: String,
    pub property_value: String,
    pub description: Option<String>,
}

#[derive(Default, Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum TagType {
    #[default]
    Normal,
    NormalNoRegex,
    Special,
}

#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    bitcode::Encode,
    bitcode::Decode,
    Deserialize,
    Serialize,
)]
pub struct Tag {
    pub name: String,
    pub namespace: GenericNamespaceObj,
}

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct PluginTag {
    /// Use composition: A TagObject *has* a fundamental Tag definition
    pub tag: Tag,
    pub tag_type: TagType,
    pub relates_to: Option<RelationContext>,
}

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct RelationContext {
    pub tag: Tag,
    pub tag_type: TagType,
    pub limit_to: Option<Tag>,
}

#[derive(
    Clone, Debug, Hash, PartialEq, Eq, bitcode::Encode, bitcode::Decode, Deserialize, Serialize,
)]
pub struct TagParents {
    pub tag_id: u64,
    pub relate_tag_id: u64,
    /// IE Only limit this to A->B as it relates to C.
    pub limit_to: Option<u64>,
}
