use intmap::IntMap;
use log::info;
use roaring::RoaringTreemap;
use rusqlite::{Connection, params, params_from_iter};
use rusqlite::{Error, OptionalExtension};
use std::io::Cursor;
use std::sync::Arc;

use crate::db::MainDatabase;
use shared_types::*;

/// Gets the cache type
#[derive(Clone, Debug)]
pub enum InternalCacheType {
    // Will load everything into memory
    Full,
    // Relies on sqlite table for pulls
    Table,
    // Keeps popular tags loaded in memory and other tags in sqlite
    Popular(u64),
}

pub struct SearchQuery<'a> {
    engine: &'a RelationshipStorage,
    offset: Option<u64>,
    limit: Option<u64>,
    and_search: Option<(DbSearchTypeEnum, &'a [u64])>,
    or_search: Option<(DbSearchTypeEnum, &'a [u64])>,
    sort: bool,
}

impl<'a> SearchQuery<'a> {
    pub fn new(engine: &'a RelationshipStorage) -> Self {
        Self {
            engine,
            offset: None,
            limit: None,
            and_search: None,
            or_search: None,
            sort: false,
        }
    }
    pub fn sort(mut self) -> Self {
        self.sort = true;
        self
    }

    pub fn limit(mut self, limit: Option<u64>) -> Self {
        self.limit = limit;
        self
    }
    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn and_search(mut self, tag_ids: &'a [u64]) -> Self {
        self.and_search = Some((DbSearchTypeEnum::And, tag_ids));

        self
    }
    pub fn or_search(mut self, tag_ids: &'a [u64]) -> Self {
        self.or_search = Some((DbSearchTypeEnum::Or, tag_ids));

        self
    }

    /// Finalizes the search returns applicable fileids
    pub fn build(self) -> Vec<u64> {
        if let Some((searchtype, tag_id_list)) = self.and_search
            && let Some(bitmap) = self.engine.internal_search_item(tag_id_list, searchtype)
        {
            let offset = self.offset.unwrap_or(0) as usize;
            let limit = self.limit.unwrap_or(bitmap.len()) as usize;

            // bitmap here is a Cow, meaning `.iter()` automatically borrows
            // from either the heap-allocated or reference-held treemap.
            return bitmap.iter().rev().skip(offset).take(limit).collect();
        }

        Vec::new()
    }
}

pub(in crate::db) struct RelationshipStorage {
    file_id: IntMap<u64, RoaringTreemap>,
    tag_id: IntMap<u64, RoaringTreemap>,
    internal_cache: InternalCacheType,
    db: Arc<MainDatabase>,
}
impl RelationshipStorage {
    pub fn new(db: Arc<MainDatabase>, internal_cache: InternalCacheType) -> Self {
        RelationshipStorage {
            file_id: IntMap::default(),
            tag_id: IntMap::default(),
            internal_cache,
            db,
        }
    }

    /// Creates the relationship storage for roaring bitmaps here
    pub(in crate::db) fn internal_table_relationship_cache_create_v1(conn: &Connection) {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS RelationshipRoaringFileid (
    fileid INTEGER PRIMARY KEY,
    tagid_bitmap BLOB NOT NULL
) WITHOUT ROWID;",
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS RelationshipRoaringTagid (
    tagid  INTEGER  PRIMARY KEY,
    fileid_bitmap BLOB NOT NULL
) WITHOUT ROWID;",
            [],
        )
        .unwrap();
    }
    pub(in crate::db) fn remove_roaring(&mut self, tn: &Connection, tag_id: u64, file_id: u64) {
        match self.internal_cache {
            InternalCacheType::Full | InternalCacheType::Popular(_) => {
                if let Some(tagid_bitmap) = self.file_id.get_mut(file_id) {
                    tagid_bitmap.remove(tag_id);
                }
                if let Some(fileid_bitmap) = self.tag_id.get_mut(tag_id) {
                    fileid_bitmap.remove(file_id);
                }
            }
            InternalCacheType::Table => {}
        }

        if let Some(mut tag_bitmap) = self.relationship_cache_fileid_get(tn, file_id) {
            tag_bitmap.remove(tag_id);

            self.relationship_cache_add_fileid_sql(tn, file_id, &tag_bitmap);
        }
        // Use into_owned() right away to turn the Cow into a clean, mutable RoaringTreemap
        if let Some(cow_bitmap) = self.relationship_cache_tagid_get(tn, tag_id) {
            let mut file_bitmap = cow_bitmap.into_owned();
            file_bitmap.remove(file_id);

            self.relationship_cache_add_tagid_sql(tn, tag_id, &file_bitmap);
        }
    }

    pub(in crate::db) fn recache_roaring(&mut self, tn: &Connection) -> Result<(), Error> {
        self.file_id.clear();
        self.tag_id.clear();

        info!("Starting to Recache everything inside of roaring cache");

        let mut processed: u64 = 0;
        tn.execute("DELETE FROM RelationshipRoaringTagid", [])
            .unwrap();
        tn.execute("DELETE FROM RelationshipRoaringFileid", [])
            .unwrap();
        let mut stmt = tn.prepare("SELECT CAST(file_id AS INTEGER), CAST(tag_id AS INTEGER) FROM Relationship ORDER BY file_id")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, u64>(0).unwrap(), row.get::<_, u64>(1).unwrap()))
            })
            .unwrap();

        let mut current_fileid: Option<u64> = None;
        let mut bitmap = RoaringTreemap::new();

        for row in rows {
            let (fileid, tagid) = row.unwrap();

            if Some(fileid) != current_fileid {
                if let Some(prev_fileid) = current_fileid {
                    self.relationship_cache_add_fileid_sql(tn, prev_fileid, &bitmap);
                    processed += 1;
                    if processed.is_multiple_of(10_000) {
                        println!("Processed {} fileids...", processed);
                    }
                }
                bitmap.clear();
                current_fileid = Some(fileid);
            }

            bitmap.insert(tagid);
        }

        // Flush last fileid
        if let Some(fileid) = current_fileid {
            self.relationship_cache_add_fileid_sql(tn, fileid, &bitmap);
        }

        let mut stmt = tn.prepare("SELECT CAST(file_id AS INTEGER), CAST(tag_id AS INTEGER) FROM Relationship ORDER BY tag_id")?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)))?;

        processed = 0;

        let mut current_tagid: Option<u64> = None;
        let mut bitmap = RoaringTreemap::new();

        for row in rows {
            let (fileid, tagid) = row.unwrap();

            if Some(tagid) != current_tagid {
                if let Some(prev_tagid) = current_tagid {
                    self.relationship_cache_add_tagid_sql(tn, prev_tagid, &bitmap);
                    processed += 1;
                    if processed.is_multiple_of(10_000) {
                        println!("Processed {} tagids...", processed);
                    }
                }
                bitmap.clear();
                current_tagid = Some(tagid);
            }

            bitmap.insert(fileid);
        }

        // Flush last tagid
        if let Some(tagid) = current_tagid {
            self.relationship_cache_add_tagid_sql(tn, tagid, &bitmap);
        }
        //self.file_id.shrink_to_fit();
        //self.tag_id.shrink_to_fit();

        info!("Finished recaching roaring table");
        Ok(())
    }

    /// Loads entire relationships into db
    pub(in crate::db) fn load_relationship_cache(&mut self, conn: &Connection) {
        info!(
            "Relationship Roaring is loading data in from the db table: {:?}",
            self.internal_cache
        );
        // No need to load this
        if let InternalCacheType::Table = self.internal_cache {
            return;
        }

        let params;
        let sql = match self.internal_cache {
            InternalCacheType::Popular(ref popular_count) => {
                params = vec![popular_count];
                "SELECT tagid, fileid_bitmap FROM RelationshipRoaringTagid WHERE tagid IN (SELECT id FROM Tags WHERE count >= ?)"
            }
            _ => {
                params = vec![];
                "SELECT tagid, fileid_bitmap FROM RelationshipRoaringTagid"
            }
        };

        let mut stmt = conn.prepare(sql).unwrap();
        let rows = stmt
            .query_map(params_from_iter(params), |row| {
                Ok((
                    row.get::<_, u64>(0).unwrap(),     // tagid
                    row.get::<_, Vec<u8>>(1).unwrap(), // tagid_bitmap
                ))
            })
            .unwrap();

        for (tagid, fileid_bitmap) in rows.flatten() {
            if let Ok(mut bitmap) =
                RoaringTreemap::deserialize_unchecked_from(Cursor::new(fileid_bitmap))
            {
                bitmap.optimize();
                self.tag_id.insert(tagid, bitmap);
            }
        }
        if let InternalCacheType::Popular(_) = self.internal_cache {
            return;
        }

        /*
        let mut stmt = conn
            .prepare("SELECT fileid, tagid_bitmap FROM RelationshipRoaringFileid")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, u64>(0).unwrap(),     // fileid
                    row.get::<_, Vec<u8>>(1).unwrap(), // tagid_bitmap
                ))
            })
            .unwrap();


        for (fileid, tagid_bitmap) in rows.flatten() {
            if let Ok(bitmap) =
                RoaringTreemap::deserialize_unchecked_from(Cursor::new(tagid_bitmap))
            {
                //self.file_id.insert(fileid, bitmap);
            }
        }*/
    }

    /// Checks if a relationship exists
    pub(in crate::db) fn relationship_cache_relationship_exists(
        &self,
        conn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) -> bool {
        if let Some(roaring) = self.relationship_cache_tagid_get(conn, tag_id) {
            return roaring.contains(file_id);
        }

        false
    }

    ///
    /// Checks if a tagid exists in the cache
    ///
    pub(in crate::db) fn relationship_cache_tagid_exists(
        &self,
        conn: &Connection,
        tag_id: u64,
    ) -> bool {
        match self.internal_cache {
            InternalCacheType::Popular(_) => {
                if self.tag_id.contains_key(tag_id) {
                    return true;
                }
                self.relationship_cache_tagid_get(conn, tag_id).is_some()
            }
            InternalCacheType::Full => self.tag_id.contains_key(tag_id),
            InternalCacheType::Table => self.relationship_cache_tagid_get(conn, tag_id).is_some(),
        }
    }

    ///
    /// Returns a list of all file_id's associated with a tag
    ///
    pub(in crate::db) fn relationship_search_fileid_roaring(
        &self,
        tn: &Connection,
        tag_id: u64,
    ) -> Vec<u64> {
        let mut out = Vec::new();

        if let Some(bitmap) = self.relationship_cache_tagid_get(tn, tag_id) {
            for fileid in bitmap.iter() {
                out.push(fileid);
            }
        }

        out
    }

    ///
    /// Returns the tagids associated with a fileid
    ///
    pub(in crate::db) fn relationship_search_tagid_roaring(
        &self,
        tn: &Connection,
        file_id: u64,
    ) -> Vec<u64> {
        let mut out = Vec::new();

        if let Some(tags) = self.relationship_cache_fileid_get(tn, file_id) {
            for tag in tags {
                out.push(tag);
            }
        }

        out
    }

    fn relationship_cache_tagid_get<'a>(
        &'a self,
        tn: &Connection,
        tag_id: u64,
    ) -> Option<std::borrow::Cow<'a, RoaringTreemap>> {
        match self.internal_cache {
            InternalCacheType::Full => {
                // Returns a zero-overhead borrowed reference from your RAM cache
                self.tag_id.get(tag_id).map(std::borrow::Cow::Borrowed)
            }
            InternalCacheType::Table | InternalCacheType::Popular(_) => {
                if let Some(bitmap) = self.tag_id.get(tag_id) {
                    return Some(std::borrow::Cow::Borrowed(bitmap));
                }

                // Fallback to SQLite (Must return Owned because it's ephemeral)
                if let Ok(Some(raw_bitmap)) = tn
                    .query_row(
                        "SELECT fileid_bitmap FROM RelationshipRoaringTagid WHERE tagid = ?",
                        params![tag_id],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    && let Ok(out) = RoaringTreemap::deserialize_unchecked_from(&raw_bitmap[..])
                {
                    return Some(std::borrow::Cow::Owned(out));
                }
                None
            }
        }
    }
    fn relationship_cache_fileid_get(
        &self,
        tn: &Connection,
        file_id: u64,
    ) -> Option<RoaringTreemap> {
        match self.internal_cache {
            InternalCacheType::Full => {
                return self.file_id.get(file_id).cloned();
            }
            InternalCacheType::Table | InternalCacheType::Popular(_) => {
                if let Ok(Some(raw_bitmap)) = tn
                    .query_row(
                        "SELECT tagid_bitmap FROM RelationshipRoaringFileid WHERE fileid = ?",
                        params![file_id],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    && let Ok(out) = RoaringTreemap::deserialize_unchecked_from(&raw_bitmap[..])
                {
                    return Some(out);
                }
            }
        }
        None
    }

    fn relationship_cache_add_sql(&self, tn: &Connection, file_id: u64, tag_id: u64) {
        if let Some(mut tag_bitmap) = self.relationship_cache_fileid_get(tn, file_id) {
            tag_bitmap.insert(tag_id);
            self.relationship_cache_add_fileid_sql(tn, file_id, &tag_bitmap);
        } else {
            let mut tag_bitmap = RoaringTreemap::new();
            tag_bitmap.insert(tag_id);
            self.relationship_cache_add_fileid_sql(tn, file_id, &tag_bitmap);
        }
        if let Some(cow_bitmap) = self.relationship_cache_tagid_get(tn, tag_id) {
            let mut file_bitmap = cow_bitmap.into_owned();
            file_bitmap.insert(file_id);
            self.relationship_cache_add_tagid_sql(tn, tag_id, &file_bitmap);
        } else {
            let mut file_bitmap = RoaringTreemap::new();
            file_bitmap.insert(file_id);
            self.relationship_cache_add_tagid_sql(tn, tag_id, &file_bitmap);
        }
    }

    fn relationship_cache_add_fileid_sql(
        &self,
        tn: &Connection,
        file_id: u64,
        tag_bitmap: &RoaringTreemap,
    ) {
        let mut bytes = vec![];
        tag_bitmap.serialize_into(&mut bytes).unwrap();
        tn.execute(
            "INSERT INTO RelationshipRoaringFileid (fileid, tagid_bitmap) VALUES (?, ?) ON CONFLICT(fileid) DO UPDATE SET tagid_bitmap = excluded.tagid_bitmap",
            params![file_id, bytes],
        )
        .unwrap();
    }

    fn relationship_cache_add_tagid_sql(
        &self,
        tn: &Connection,
        tag_id: u64,
        file_bitmap: &RoaringTreemap,
    ) {
        let mut bytes = vec![];
        file_bitmap.serialize_into(&mut bytes).unwrap();
        tn.execute(
            "INSERT INTO RelationshipRoaringTagid (tagid, fileid_bitmap)
     VALUES (?, ?)
     ON CONFLICT(tagid) DO UPDATE SET fileid_bitmap = excluded.fileid_bitmap",
            params![tag_id, bytes],
        )
        .unwrap();
    }

    ///
    /// Loads the relationships into the internal memory
    ///
    pub(in crate::db) fn relationship_roaring_add(
        &mut self,
        tn: &Connection,
        file_id: u64,
        tag_id: u64,
    ) {
        match self.internal_cache {
            InternalCacheType::Table => {}
            InternalCacheType::Popular(_popular_count) => {
                /*  if let Some(tagid_count) = self.db.read().get_count_for_tagid(tn, &tag_id) {
                    if popular_count <= tagid_count {
                        match self.tag_id.get_mut(tag_id) {
                            None => {
                                let mut bitmap = RoaringTreemap::new();
                                bitmap.insert(file_id);
                                self.tag_id.insert(tag_id, bitmap);
                            }
                            Some(bitmap) => {
                                bitmap.insert(file_id);
                            }
                        }
                    }
                }*/
            }
            InternalCacheType::Full => {
                match self.file_id.get_mut(file_id) {
                    None => {
                        let mut bitmap = RoaringTreemap::new();
                        bitmap.insert(tag_id);
                        //self.file_id.insert(file_id, bitmap);
                    }
                    Some(bitmap) => {
                        bitmap.insert(tag_id);
                    }
                }
                match self.tag_id.get_mut(tag_id) {
                    None => {
                        let mut bitmap = RoaringTreemap::new();
                        bitmap.insert(file_id);
                        self.tag_id.insert(tag_id, bitmap);
                    }
                    Some(bitmap) => {
                        bitmap.insert(file_id);
                    }
                }
            }
        }
        self.relationship_cache_add_sql(tn, file_id, tag_id);
    }

    fn internal_search_item<'a>(
        &'a self,
        tag_id_list: &[u64],
        searchtype: DbSearchTypeEnum,
    ) -> Option<std::borrow::Cow<'a, RoaringTreemap>> {
        use std::borrow::Cow;

        if tag_id_list.is_empty() {
            return None;
        }

        let mut bitmaps_iter = tag_id_list.iter().filter_map(|tag| self.tag_id.get(*tag));

        match searchtype {
            DbSearchTypeEnum::Or => {
                let mut first = bitmaps_iter.next()?.clone();
                for b in bitmaps_iter {
                    first |= b;
                }
                Some(Cow::Owned(first))
            }

            DbSearchTypeEnum::And => {
                let first = bitmaps_iter.next()?;
                let mut acc = Cow::Borrowed(first);

                for b in bitmaps_iter {
                    // Use bitwise intersection assignment on the inner mutated type
                    *acc.to_mut() &= b;
                    if acc.is_empty() {
                        break;
                    }
                }
                Some(acc)
            }
        }
    }
}
