//! Namespaces written by the Redgifs plugin.
//!
//! The site has no comments or boards, so the graph is shallower than e.g. the
//! forum plugins: a *user* owns *gifs*, and a gif may belong to a *gallery* of
//! sibling gifs. Those are the only structural relations. Everything else
//! (`tags`, `niches`, counters, dimensions) is a flat value tag scoped to the
//! gif it describes.
//!
//! ```text
//! RedgifsGif(id)  -> RedgifsUser(name)     gif uploaded by user
//! RedgifsGif(id)  -> RedgifsGallery(gal)   gif belongs to gallery
//! ```

use shared_types::{GenericNamespaceObj, PluginTag, RelationContext, Tag};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ns {
    /// Redgifs gif id, stored lowercase as the API returns it.
    Gif,
    /// Uploader username. The site's only user dimension.
    User,
    /// Gallery uuid when a gif is one entry of a multi-image gallery.
    Gallery,
    /// Position of a gif inside its gallery, zero based.
    GalleryPosition,
    /// Human description/uploader supplied title.
    Description,
    /// One tag, stored verbatim (Redgifs tags are Title Cased, e.g. "Anal").
    Tag,
    /// One niche/category name.
    Niche,
    /// Unix timestamp of upload.
    CreatedUtc,
    /// Lifetime view count at scrape time.
    Views,
    Likes,
    Dislikes,
    /// Pixel dimensions, stored as `WIDTHxHEIGHT`.
    Dimensions,
    /// Runtime in seconds, or "None" when Redgifs omits it.
    Duration,
    /// Whether the clip carries an audio track.
    HasAudio,
    /// Sexual orientation/level field Redgifs exposes on each gif.
    Sexuality,
    /// The media variant that was downloaded (`hd`, `sd`, `silent`, `poster`).
    Format,
    /// Direct media url the file was fetched from.
    MediaUrl,
    /// The `/ifr/<id>` embed page for the gif.
    EmbedUrl,
}

impl Ns {
    /// Stored namespace name. Part of the database contract: renaming one
    /// orphans every tag already written under it.
    pub fn name(self) -> &'static str {
        match self {
            Ns::Gif => "RedgifsGif",
            Ns::User => "RedgifsUser",
            Ns::Gallery => "RedgifsGallery",
            Ns::GalleryPosition => "RedgifsGalleryPosition",
            Ns::Description => "RedgifsDescription",
            Ns::Tag => "RedgifsTag",
            Ns::Niche => "RedgifsNiche",
            Ns::CreatedUtc => "RedgifsCreatedUtc",
            Ns::Views => "RedgifsViews",
            Ns::Likes => "RedgifsLikes",
            Ns::Dislikes => "RedgifsDislikes",
            Ns::Dimensions => "RedgifsDimensions",
            Ns::Duration => "RedgifsDuration",
            Ns::HasAudio => "RedgifsHasAudio",
            Ns::Sexuality => "RedgifsSexuality",
            Ns::Format => "RedgifsFormat",
            Ns::MediaUrl => "RedgifsMediaUrl",
            Ns::EmbedUrl => "RedgifsEmbedUrl",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Ns::Gif => "Redgifs gif id, lowercase as returned by the API.",
            Ns::User => "Redgifs username of the uploader.",
            Ns::Gallery => "Uuid of the gallery a gif belongs to, when it has siblings.",
            Ns::GalleryPosition => "Zero based position of a gif inside its gallery.",
            Ns::Description => "Uploader supplied title or description.",
            Ns::Tag => "A single Redgifs tag, verbatim.",
            Ns::Niche => "A Redgifs niche/category name.",
            Ns::CreatedUtc => "UNIX timestamp of when the gif was uploaded.",
            Ns::Views => "Lifetime view count at scrape time.",
            Ns::Likes => "Like count at scrape time.",
            Ns::Dislikes => "Dislike count at scrape time.",
            Ns::Dimensions => "Pixel dimensions as WIDTHxHEIGHT.",
            Ns::Duration => "Clip runtime in seconds, or None when Redgifs omits it.",
            Ns::HasAudio => "Whether the clip carries an audio track.",
            Ns::Sexuality => "Sexual orientation/level Redgifs reports for the gif.",
            Ns::Format => "Which media variant was downloaded.",
            Ns::MediaUrl => "Direct media url the file was fetched from.",
            Ns::EmbedUrl => "The /ifr/<id> embed page for the gif.",
        }
    }

    /// Builds the `Tag` half of a `PluginTag`, without a parent relation.
    pub fn tag(self, name: impl Into<String>) -> Tag {
        Tag {
            name: name.into(),
            namespace: GenericNamespaceObj {
                name: self.name().to_string(),
                description: Some(self.description().to_string()),
            },
        }
    }
}

/// Builds a structural tag: `child` belongs to `parent`, optionally scoped.
pub fn related(child_ns: Ns, child_name: impl Into<String>, parent: Tag) -> PluginTag {
    PluginTag {
        tag: child_ns.tag(child_name),
        relates_to: Some(RelationContext {
            tag: parent,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Builds a value tag scoped to the gif it describes, and records the `limit_to`
/// parent at the same time.
///
/// The `limit_to` relationship is what stops same-named tags on different gifs
/// from collapsing: `Tag("Anal")` is shared by thousands of gifs, and the
/// relation records which gif each occurrence belonged to.
pub fn scoped(child_ns: Ns, child_name: impl Into<String>, parent: Tag) -> PluginTag {
    PluginTag {
        tag: child_ns.tag(child_name),
        relates_to: Some(RelationContext {
            tag: parent.clone(),
            limit_to: Some(parent),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_names_are_unique() {
        let all = [
            Ns::Gif,
            Ns::User,
            Ns::Gallery,
            Ns::GalleryPosition,
            Ns::Description,
            Ns::Tag,
            Ns::Niche,
            Ns::CreatedUtc,
            Ns::Views,
            Ns::Likes,
            Ns::Dislikes,
            Ns::Dimensions,
            Ns::Duration,
            Ns::HasAudio,
            Ns::Sexuality,
            Ns::Format,
            Ns::MediaUrl,
            Ns::EmbedUrl,
        ];
        let mut names: Vec<&str> = all.iter().map(|ns| ns.name()).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count);
    }

    #[test]
    fn scoped_records_both_parent_and_limit_to() {
        let gif = Ns::Gif.tag("abc");
        let tag = scoped(Ns::Tag, "Anal", gif.clone());

        assert_eq!(tag.tag.namespace.name, "RedgifsTag");
        let relation = tag.relates_to.expect("relation");
        assert_eq!(relation.tag, gif);
        assert_eq!(relation.limit_to, Some(gif));
    }

    #[test]
    fn related_has_no_limit_to() {
        let tag = related(Ns::Gif, "abc", Ns::User.tag("someone"));
        let relation = tag.relates_to.expect("relation");
        assert_eq!(relation.tag.namespace.name, "RedgifsUser");
        assert!(relation.limit_to.is_none());
    }
}
