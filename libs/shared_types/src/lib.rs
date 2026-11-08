use redact::{Secret, expose_secret};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

pub const DEFAULT_PRIORITY: u64 = 10;
pub const SQL_CHUNK_SIZE: u64 = 1000;

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

#[derive(PartialEq, Deserialize, Debug, Serialize, bitcode::Encode, bitcode::Decode, Clone)]
pub struct DbSettingsObj {
    pub name: String,
    pub description: Option<String>,
    pub num: Option<u64>,
    pub param: Option<String>,
}

pub enum DbSearchTypeEnum {
    And,
    Or,
    Not,
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
    pub local_modifiers: Vec<TargetModifier>,
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
    // Sends job back into queue with x waiting time in seconds
    RetryLater(u64),
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
#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    bitcode::Encode,
    bitcode::Decode,
)]
pub enum TagOperation {
    #[default]
    Add,
    Del,
    Set,
}
#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    bitcode::Decode,
    bitcode::Encode,
)]
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
    pub hash: Vec<HashesSupported>,
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
    pub size_bytes: Option<u64>,
}

#[derive(
    Default, Clone, PartialEq, Eq, Hash, Debug, Deserialize, bitcode::Encode, bitcode::Decode,
)]
pub struct FileManager {
    pub internal: FileInternal,
    pub identifying_hashes: Vec<HashesSupported>,
}
impl From<FileInternal> for FileManager {
    fn from(internal: FileInternal) -> Self {
        FileManager {
            internal,
            identifying_hashes: vec![],
        }
    }
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
    pub tag_id: u64,
    pub count: u64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FileSource {
    Url(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Deserialize, bitcode::Encode, bitcode::Decode)]
#[cfg_attr(feature = "strum", derive(strum_macros::EnumIter))]
pub enum HashesSupported {
    Md5(String),
    Sha1(String),
    Sha256(String),
    Sha512(String),
    // IPFSCID(String),
    // IPFSCID1(String),
    ImageHash(String),
}

/// What to do with a source file when importing it into the library:
/// copy it into storage (source stays), move it (source is removed), or
/// hardlink it (source and storage share one inode; same filesystem only).
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, Debug, Deserialize, Serialize, bitcode::Encode, bitcode::Decode,
)]
pub enum FileAction {
    Copy,
    Move,
    HardLink,
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
    /// Sets the number of jobs that may run concurrently for the plugin
    JobNum(u64),
    /// Sets the amount of times to check a file's hash before accepting it as correct. Common
    /// behavior for wwbsites that compress videos
    HashDownloadRetry(u8),
    /// Changes the text or file downloading
    Modifier(TargetModifier),
    /// Tells the system that we are going to ideally process a login type
    Login((LoginNeed, LoginType)),
    /// The plugin handles fetching the input itself in `parser_call`.
    NoTextDownload,
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
    // Adds a cookie into the request (url, cookie)
    // Cookie for everything must be formatted like: "foo=bar; Domain=yolo.local"
    // Cookie for individual requests must be formatted like: "foo=bar; yeet=bong"
    Cookie((String, String)),
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
    // Runs when a tag is added to the db.
    // First when the ns exists, 2nd when the namespace does not exist
    // Leave the Vecs empty to ignore the category
    Tag((SearchType, Vec<String>, Vec<String>)),
}

#[derive(
    Debug, PartialEq, Eq, Hash, Clone, bitcode::Encode, bitcode::Decode, Serialize, Deserialize,
)]
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

#[derive(
    Debug, PartialEq, Eq, Hash, Clone, bitcode::Encode, bitcode::Decode, Serialize, Deserialize,
)]
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

#[derive(
    Default,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    bitcode::Decode,
    bitcode::Encode,
)]
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
pub struct PluginTag {
    /// Use composition: A TagObject *has* a fundamental Tag definition
    pub tag: Tag,
    pub tag_type: TagType,
    pub relates_to: Option<RelationContext>,
}

#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Debug,
    Serialize,
    Deserialize,
    bitcode::Encode,
    bitcode::Decode,
)]
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

impl GenericNamespaceObj {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: Some(description.into()),
        }
    }
}

impl Tag {
    pub fn new(name: impl Into<String>, namespace: GenericNamespaceObj) -> Self {
        Self {
            name: name.into(),
            namespace,
        }
    }
}

impl PluginTag {
    pub fn new(name: impl Into<String>, namespace: GenericNamespaceObj) -> Self {
        Self {
            tag: Tag::new(name, namespace),
            ..Default::default()
        }
    }

    pub fn related(
        name: impl Into<String>,
        namespace: GenericNamespaceObj,
        relates_to: &RelationContext,
    ) -> Self {
        Self {
            tag: Tag::new(name, namespace),
            relates_to: Some(relates_to.clone()),
            ..Default::default()
        }
    }
}

impl FileTagAction {
    pub fn add(tags: Vec<PluginTag>) -> Self {
        Self {
            operation: TagOperation::Add,
            tags,
        }
    }
}

/// Shared, dependency-free helpers used by plugins and the host.
///
/// Everything here was previously copy-pasted across plugins/host call
/// sites (cookie dedup, `finalize`, URL absolutizing, id extraction,
/// namespace collection, job-param filtering). Keeping one implementation
/// stops the copies from diverging — the cookie-header growth that caused
/// FurAffinity `400`s was exactly such a divergence.
pub mod helpers {
    use super::{
        DownloadModifiers, FileObject, FileTagAction, FileSource, GenericNamespaceObj,
        HashesSupported, LoginType, ModifierTarget, PluginJob, ScraperDataReturn, ScraperObject,
        ScraperParam, ScraperReturn, TargetModifier, Url,
    };
    use std::collections::{BTreeMap, HashSet};

    /// Collapse files/tags/jobs into the standard plugin return:
    /// `Nothing` when everything is empty, else one `Data`.
    pub fn finalize(
        files: HashSet<FileObject>,
        tags: HashSet<super::PluginTag>,
        jobs: HashSet<ScraperDataReturn>,
    ) -> Vec<ScraperReturn> {
        if files.is_empty() && tags.is_empty() && jobs.is_empty() {
            vec![ScraperReturn::Nothing]
        } else {
            vec![ScraperReturn::Data(ScraperObject {
                files,
                tags,
                jobs,
            })]
        }
    }

    /// Deduplicate a `Cookie` header string by cookie name (last wins),
    /// preserving first-seen order. Heals headers bloated by repeated
    /// login injection (e.g. `a=…; b=…; a=…; b=…` → `a=…; b=…`).
    pub fn dedupe_cookie_header(header: &str) -> String {
        let mut cookies: Vec<(String, String)> = Vec::new();
        for pair in header.split(';') {
            push_cookie_pair(&mut cookies, pair);
        }
        cookies
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn push_cookie_pair(cookies: &mut Vec<(String, String)>, pair: &str) {
        let Some((name, value)) = pair.trim().split_once('=') else {
            return;
        };
        let name = name.trim().to_string();
        let value = value.trim().to_string();
        if name.is_empty() {
            return;
        }
        if let Some(existing) = cookies.iter_mut().find(|(n, _)| *n == name) {
            existing.1 = value;
        } else {
            cookies.push((name, value));
        }
    }

    /// Build a single deduplicated `Cookie` header from job params:
    /// cookies carried on URLs (as `Header` or `Cookie` modifiers) first,
    /// then `ScraperParam::Login(LoginType::Cookie)` values, which win.
    /// `None` when there are no cookies.
    pub fn cookie_header_from_params(params: &[ScraperParam]) -> Option<String> {
        let mut cookies: Vec<(String, String)> = Vec::new();
        for param in params {
            let modifiers: &[TargetModifier] = match param {
                ScraperParam::Url(url) => &url.local_modifiers,
                ScraperParam::UrlPost(post) => &post.local_modifiers,
                _ => continue,
            };
            for m in modifiers {
                match &m.modifier {
                    DownloadModifiers::Header((k, v)) if k == "Cookie" => {
                        for pair in v.split(';') {
                            push_cookie_pair(&mut cookies, pair);
                        }
                    }
                    DownloadModifiers::Cookie((_, v)) => {
                        for pair in v.split(';') {
                            push_cookie_pair(&mut cookies, pair);
                        }
                    }
                    _ => {}
                }
            }
        }
        for param in params {
            if let ScraperParam::Login(LoginType::Cookie(name, Some(value))) = param {
                push_cookie_pair(&mut cookies, &format!("{name}={value}"));
            }
        }
        if cookies.is_empty() {
            None
        } else {
            Some(
                cookies
                    .iter()
                    .map(|(name, value)| format!("{name}={value}"))
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        }
    }

    /// The standard single-`Cookie`-header modifiers for text downloads,
    /// built from the job's login cookies (empty when there are none).
    pub fn cookie_modifiers(params: &[ScraperParam]) -> Vec<TargetModifier> {
        match cookie_header_from_params(params) {
            Some(header) => vec![TargetModifier {
                target: ModifierTarget::Text,
                modifier: DownloadModifiers::Header(("Cookie".into(), header)),
            }],
            None => Vec::new(),
        }
    }

    /// Resolve a possibly-relative href against a site base
    /// (e.g. `"www.furaffinity.net"` or `"https://www.furaffinity.net"`).
    pub fn abs_url(href: &str, base: &str) -> String {
        let base = base.trim_end_matches('/');
        let base = base
            .strip_prefix("https://")
            .or_else(|| base.strip_prefix("http://"))
            .unwrap_or(base);
        if let Some(rest) = href.strip_prefix("//") {
            format!("https://{rest}")
        } else if href.starts_with("http") {
            href.to_string()
        } else if let Some(rest) = href.strip_prefix('/') {
            format!("https://{base}/{rest}")
        } else {
            format!("https://{base}/{href}")
        }
    }

    /// Segment following `keyword` in a URL path, if present.
    pub fn segment_after(url: &str, keyword: &str) -> Option<String> {
        let mut segments = url.split('/');
        while let Some(seg) = segments.next() {
            if seg == keyword {
                return segments.next().map(str::to_string);
            }
        }
        None
    }

    /// Numeric segment following `keyword` (e.g. `/view/123/` → `123`).
    pub fn numeric_segment_after(url: &str, keywords: &[&str]) -> Option<String> {
        let mut segments = url.split('/');
        while let Some(seg) = segments.next() {
            if keywords.contains(&seg)
                && let Some(id) = segments.next()
                && !id.is_empty()
                && id.chars().all(|c| c.is_ascii_digit())
            {
                return Some(id.to_string());
            }
        }
        None
    }

    /// Last non-empty path segment of a URL.
    pub fn last_segment(url: &str) -> Option<String> {
        url.trim_end_matches('/')
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    }

    /// All `ScraperParam::Normal` search terms in a job's params.
    pub fn normal_tags(params: &[ScraperParam]) -> Vec<&str> {
        params
            .iter()
            .filter_map(|p| match p {
                ScraperParam::Normal(t) => Some(t.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Whether a job recurses into discovered pages (default: yes).
    pub fn is_recursion(user_data: &BTreeMap<String, String>) -> bool {
        user_data.get("recursion").is_none_or(|f| f != "false")
    }

    pub fn set_recursion(user_data: &mut BTreeMap<String, String>, recursion: bool) {
        user_data.insert("recursion".into(), recursion.to_string());
    }

    /// The `INTSCRAPE_REDOWNLOAD` early-return shared by listing plugins:
    /// re-emit the already-known file for `source_url` keyed by hash.
    pub fn redownload_return(
        source_url: &str,
        user_data: &BTreeMap<String, String>,
    ) -> Option<Vec<ScraperReturn>> {
        let hash = user_data.get("INTSCRAPE_REDOWNLOAD_HASH")?;
        let file = FileObject {
            source: Some(FileSource::Url(source_url.to_string())),
            hash: vec![HashesSupported::Sha512(hash.clone())],
            ..Default::default()
        };
        Some(finalize(HashSet::from([file]), HashSet::new(), HashSet::new()))
    }

    /// Single-URL job for `url_dump` implementations.
    pub fn job_for(
        site: &str,
        url: String,
        priority: u64,
        user_data: BTreeMap<String, String>,
    ) -> ScraperDataReturn {
        ScraperDataReturn {
            job: PluginJob {
                site: site.to_string(),
                priority,
                param: vec![ScraperParam::Url(Url {
                    url,
                    ..Default::default()
                })],
                user_data,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Params safe to persist in the `Jobs` table. `ScraperParam::Login`
    /// entries are runtime-injected from settings at dispatch; storing
    /// them made every retry write another copy back to the DB, growing
    /// cookie headers without bound.
    pub fn persistable_params(params: &[ScraperParam]) -> Vec<ScraperParam> {
        params
            .iter()
            .filter(|p| !matches!(p, ScraperParam::Login(_)))
            .cloned()
            .collect()
    }

    /// Every namespace referenced by a set of tag actions (the tag
    /// itself, its `relates_to` target, and the relation's `limit_to`).
    /// Used to pre-ensure namespaces so DML transactions stay DML-only.
    pub fn collect_namespaces(actions: &[FileTagAction]) -> HashSet<GenericNamespaceObj> {
        actions
            .iter()
            .flat_map(|action| action.tags.iter())
            .flat_map(|plugin_tag| {
                let mut nss = vec![plugin_tag.tag.namespace.clone()];
                if let Some(relation) = &plugin_tag.relates_to {
                    nss.push(relation.tag.namespace.clone());
                    if let Some(limit) = &relation.limit_to {
                        nss.push(limit.namespace.clone());
                    }
                }
                nss
            })
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn dedupe_cookie_header_collapses_repeats() {
            assert_eq!(
                dedupe_cookie_header("a=1; b=2; a=1; b=2; a=3"),
                "a=3; b=2"
            );
            assert_eq!(dedupe_cookie_header(""), "");
        }

        #[test]
        fn cookie_header_from_params_login_wins_over_url() {
            let params = vec![
                ScraperParam::Url(Url {
                    url: "https://x/".into(),
                    local_modifiers: vec![TargetModifier {
                        target: ModifierTarget::Text,
                        modifier: DownloadModifiers::Header((
                            "Cookie".into(),
                            "a=OLD; a=OLD; b=2".into(),
                        )),
                    }],
                }),
                ScraperParam::Login(LoginType::Cookie("a".into(), Some("NEW".into()))),
            ];
            assert_eq!(
                cookie_header_from_params(&params).as_deref(),
                Some("a=NEW; b=2")
            );
            assert_eq!(cookie_header_from_params(&[]), None);
        }

        #[test]
        fn abs_url_variants() {
            assert_eq!(abs_url("//d.x/y", "www.x"), "https://d.x/y");
            assert_eq!(abs_url("https://d.x/y", "www.x"), "https://d.x/y");
            assert_eq!(abs_url("/y", "www.x"), "https://www.x/y");
            assert_eq!(abs_url("y", "https://www.x/"), "https://www.x/y");
        }

        #[test]
        fn segment_helpers() {
            assert_eq!(
                numeric_segment_after("https://x/view/123/", &["view", "full"]),
                Some("123".into())
            );
            assert_eq!(numeric_segment_after("https://x/view/abc/", &["view"]), None);
            assert_eq!(last_segment("https://x/a/b/"), Some("b".into()));
            assert_eq!(segment_after("https://x/gallery/bob/", "gallery"), Some("bob".into()));
        }

        #[test]
        fn finalize_empty_is_nothing() {
            assert_eq!(
                finalize(HashSet::new(), HashSet::new(), HashSet::new()),
                vec![ScraperReturn::Nothing]
            );
        }

        #[test]
        fn collect_namespaces_includes_relations() {
            use super::super::{PluginTag, RelationContext, Tag};
            let rel = RelationContext {
                tag: Tag::new("t", GenericNamespaceObj::new("ns2", "d")),
                ..Default::default()
            };
            let action = FileTagAction::add(vec![PluginTag {
                tag: Tag::new("t", GenericNamespaceObj::new("ns1", "d")),
                relates_to: Some(rel),
                ..Default::default()
            }]);
            let set = collect_namespaces(&[action]);
            assert_eq!(set.len(), 2);
        }
    }
}
