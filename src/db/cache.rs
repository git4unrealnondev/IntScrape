//! Database operations for the `cache` domain.

use super::roaring::InternalCacheType;
use super::tag_search;
use super::{CacheType, MainDatabase, RelationshipStorage};
use rusqlite::Connection;
use shared_types::DbSettingsObj;

impl MainDatabase {
    pub fn internal_load_caching(&self, conn: &Connection) {
        let temp;
        loop {
            let cache = match self.internal_setting_get(conn, "SYSTEM_cachemode") {
                Err(_) | Ok(None) => {
                    self.internal_setup_default_cache(conn);
                    self.internal_setting_get(conn, "SYSTEM_cachemode")
                        .unwrap()
                        .unwrap()
                        .param
                        .clone()
                }
                Ok(Some(setting)) => setting.param.clone(),
            };

            if let Some(ref cache) = cache {
                let cachemode = match cache.as_str() {
                    "Bare" => (Some(CacheType::Bare), None),
                    "RelationshipRoaringFull" => (
                        Some(CacheType::RelationshipRoaring(InternalCacheType::Full)),
                        Some(RelationshipStorage::new(
                            self.clone().into(),
                            InternalCacheType::Full,
                        )),
                    ),
                    "RelationshipRoaringTable" => (
                        Some(CacheType::RelationshipRoaring(InternalCacheType::Table)),
                        Some(RelationshipStorage::new(
                            self.clone().into(),
                            InternalCacheType::Table,
                        )),
                    ),
                    "RelationshipRoaringPopular" => {
                        if let Ok(Some(popular_count)) =
                            self.internal_setting_get(conn, "SYSTEM_tag_count_popular_division")
                            && let Some(popular_count) = popular_count.num
                        {
                            (
                                Some(CacheType::RelationshipRoaring(InternalCacheType::Popular(
                                    popular_count,
                                ))),
                                Some(RelationshipStorage::new(
                                    self.clone().into(),
                                    InternalCacheType::Popular(popular_count),
                                )),
                            )
                        } else {
                            (None, None)
                        }
                    }

                    _ => {
                        self.internal_setup_default_cache(conn);
                        (None, None)
                    }
                };
                if cachemode.0.is_some() {
                    temp = cachemode;
                    break;
                }
            } else {
                self.internal_setup_default_cache(conn);
            }
        }
        *self.relationship_roaring_storage.write() = temp.1;
        *self.cache_type.write() = temp.0.unwrap();

        let mut guard = self.relationship_roaring_storage.write();

        if let Some(rel) = guard.as_mut() {
            rel.load_relationship_cache(conn);
        }
        drop(guard);

        self.refresh_tag_search_cache_with_connection(conn);
    }

    fn refresh_tag_search_cache_with_connection(&self, conn: &Connection) {
        let mut stmt = conn
            .prepare(
                "SELECT id, name, count
                 FROM Tags
                 ORDER BY count DESC, id
                 LIMIT ?1",
            )
            .unwrap();
        let entries = stmt
            .query_map([tag_search::POPULAR_TAG_CACHE_LIMIT], |row| {
                let tag_id = row.get(0)?;
                let name: String = row.get(1)?;
                let count = row.get(2)?;
                Ok(tag_search::tag_entry(tag_id, &name, count))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let cached_tag_count: u64 = conn
            .query_row("SELECT count(*) FROM Tags", [], |row| row.get(0))
            .unwrap();
        *self.tag_search_cache.write() = tag_search::TagSearchCache::from_entries_with_completeness(
            entries,
            cached_tag_count <= tag_search::POPULAR_TAG_CACHE_LIMIT as u64,
        );
        self.tag_search_dirty
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Refreshes the in-memory tag search index after queued work changes tags.
    pub fn refresh_tag_search_cache(&self) {
        let conn = self.pool.get().unwrap();
        self.refresh_tag_search_cache_with_connection(&conn);
    }

    /// Sets up internal cache structure
    pub fn internal_setup_default_cache(&self, conn: &Connection) {
        self.internal_setting_set(
            conn,
            &DbSettingsObj {
                name: "SYSTEM_cachemode".to_string(),
                description: Some(
                    "The database caching options. Supports: Bare, InMemdb and InMemory"
                        .to_string(),
                ),
                num: None,
                param: Some("RelationshipRoaringFull".to_string()),
            },
        )
        .unwrap();
    }

    ///
    /// Recaches db internally
    ///
    pub fn recache_roaring_db(&self) {
        let mut write_guard = self.writer_lock();
        // Mutation paths acquire writer_conn before this cache lock.
        let mut roaring_guard = self.relationship_roaring_storage.write();
        if let Some(roaring) = roaring_guard.as_mut() {
            let conn = write_guard.transaction().unwrap();
            roaring.recache_roaring(&conn).unwrap();
            if let Err(error) = conn.commit() {
                log::error!("Failed to commit relationship recache transaction: {error}");
                return;
            }
        }
        self.roaring_memory_dirty
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Reloads the in-memory roaring bitmaps from the auxiliary SQL tables when
    /// a relationship write was forced to skip them (see `roaring_memory_dirty`).
    ///
    /// Cheaper than a full recache: only the RAM copy is rebuilt; the SQL side
    /// is always kept current by the writer. Must never be called while holding
    /// the writer lock (it blocks on the roaring write lock).
    pub(crate) fn refresh_roaring_memory_if_dirty(&self) {
        if !self
            .roaring_memory_dirty
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let conn = match self.pool.get() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to acquire DB connection to refresh roaring memory: {error}");
                return;
            }
        };
        let mut roaring_guard = self.relationship_roaring_storage.write();
        if let Some(roaring) = roaring_guard.as_mut() {
            roaring.load_relationship_cache(&conn);
        }
        self.roaring_memory_dirty
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}
