use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use redact::{Secret, expose_secret};

pub const DEFAULT_PRIORITY: u64 = 10;

#[derive(Deserialize, Debug, Serialize)]
pub struct DbSettingsObj {
    pub name: String,
    pub description: Option<String>,
    pub num: Option<u64>,
    pub param: Option<String>,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct UrlPost {
    pub url: String,
    // Any goofball request modifiers
    pub modifiers: Vec<DownloadModifiers>,
    // any post data to send if needed
    pub post_data: String,
}
#[derive(Deserialize, Debug, Serialize, Clone, PartialEq)]
pub enum ScraperParam {
    // User defined params like search terms
    Normal(String),
    // A GET url not a POST
    Url(String),
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
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PluginJob {
    // Time to run job. 0 for immediate
    pub time: u64,
    /// Time offset to run job at
    pub reptime: u64,
    /// Determines which job we should run first. Higher values go first
    pub priority: u64,
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
#[derive(Clone, Default, PartialEq)]
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
#[derive(Default, Clone, PartialEq)]
pub struct ScraperObject {
    pub files: HashSet<FileObject>,
    //pub tags: HashSet<TagObject>,
    //pub jobs: HashSet<ScraperDataReturn>,
    //pub flags: Vec<ScraperFlags>,
}

///
/// What to do for a list of tags when they come in
///
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub enum TagOperation {
    #[default]
    Add,
    Del,
    Set,
}
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct FileTagAction {
    pub operation: TagOperation,
    pub tags: Vec<PluginTag>,
}

/// Represents one file
/// Current version of FileObject
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct FileObject {
    pub source: Option<FileSource>,
    // Hash of file
    pub hash: Option<HashesSupported>,
    pub tag_list: Vec<FileTagAction>,
    // Skips downloading the file if a tag matches this.
    pub skip_if: Vec<SkipIf>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum FileSource {
    Url(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum HashesSupported {
    Md5(String),
    Sha1(String),
    Sha256(String),
    Sha512(String),
}

///
/// Internal db obj
///
#[derive(Debug, Clone, PartialEq)]
pub struct DbJobsObj {
    pub id: u64,
    pub isrunning: bool,
    pub config: PluginJob,
}

#[derive(Debug, Clone, Default)]
pub struct ScraperDataReturn {
    pub job: PluginJob,
    pub skip_conditions: Vec<SkipIf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SkipIf {
    // If a relationship between any file and tag exists.
    FileTagRelationship(Tag),
    // The tag is qnique and if their are X number or more of GenericNamespaceObj
    // associated with the file Then we'll skip it
    FileNamespaceNumber((Tag, GenericNamespaceObj, u64)),
    // Skips a file if the hash X exists
    FileHash(String),
    // Skips if no files are downloaded
    NoFilesDownloaded,
}

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

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum PluginProperties {
    /// Ratelimit number of tokens per time
    Ratelimit(u64, Duration),
    /// Sets the sites that are supported by the plugin
    Sites(Vec<String>),
    /// Sets the number of concurrent access the sites
    ThreadNum(u64),
    /// Changes the text or file downloading
    Modifier(TargetModifier),
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct TargetModifier {
    pub target: ModifierTarget,
    pub modifier: DownloadModifiers,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum ModifierTarget {
    Text,
    Media,
}

///
/// Holds the login type that we need
///
#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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
    //Callback(CallbackInfo),
    // Runs when a tag has exists.
    // First when the ns exists, 2nd when the namespace does not exist
    // Use None when searching all or Some when searching restrictivly
    Tag((Option<SearchType>, Vec<String>, Vec<String>)),
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
#[derive(Hash, Eq, PartialEq, Clone, Default, Debug)]
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

#[derive(Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TagType {
    #[default]
    Normal,
    NormalNoRegex,
    Special,
}

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct Tag {
    pub name: String,
    pub namespace: GenericNamespaceObj,
}

#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct PluginTag {
    /// Use composition: A TagObject *has* a fundamental Tag definition
    pub tag: Tag,
    pub tag_type: TagType,
    pub relates_to: Option<RelationContext>,
}

#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct RelationContext {
    pub tag: Tag,
    pub tag_type: TagType,
    pub limit_to: Option<Tag>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TagParents {
    pub tag_id: u64,
    pub relate_tag_id: u64,
    /// IE Only limit this to A->B as it relates to C.
    pub limit_to: Option<u64>,
}
