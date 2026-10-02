//! Every bound the plugin imposes on its own output.
//!
//! The plugin ABI returns `Vec`/`HashSet` values the host cannot bound for us
//! (`agents.md`: "impose a bounded adapter at the host boundary and document the
//! maximum batch size"). These constants are that adapter.
//!
//! Worst case per `parser_call`:
//!
//! | page      | files            | tags                        | jobs |
//! |-----------|------------------|-----------------------------|------|
//! | listing   | 100              | ~100 x 30                   | ≤100 galleries + 1 continuation |
//! | gif       | 1                | ~35                         | 0    |
//! | gallery   | `MAX_GALLERY_MEMBERS` | `MAX_GALLERY_MEMBERS` x 35 | 0 |

/// Redgifs' own maximum for a page of results.
pub const MAX_PAGE_COUNT: u64 = 100;

/// Listing pages followed before a continuation is enqueued, and the hard
/// ceiling on that continuation chain.
///
/// This matters more here than on most sites: a single tag search reported
/// `pages: 2000` and `total: 10000` in testing. Following that to the end from
/// one job would be an unbounded crawl, so the crawl is capped and the user is
/// expected to run a recursive job to go deeper.
pub const MAX_PAGES_PER_CHAIN: u64 = 10;

/// Members read out of one gallery response.
///
/// Observed galleries hold 4 entries; the response is untrusted, so this is a
/// defensive cap rather than an expectation.
pub const MAX_GALLERY_MEMBERS: usize = 64;

/// Value tags emitted per gif.
///
/// Redgifs tags are not unique per gif and the observed maximum was well into
/// the dozens. This stops one pathological gif from dominating a page.
pub const MAX_VALUE_TAGS_PER_GIF: usize = 64;

/// Longest free-text field stored as a tag name (descriptions).
pub const MAX_TEXT_LEN: usize = 10_000;

/// Longest accepted media url. Redgifs template urls are far shorter.
pub const MAX_URL_LEN: usize = 2_048;

/// Longest accepted identifier. Redgifs gif ids are `[A-Za-z0-9-]`, with a
/// trailing hyphen on some, and gallery uuids add `_`.
pub const MAX_ID_LEN: usize = 64;

/// Per-request timeout. Redgifs responses are small; galleries are the slowest
/// observed call.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
