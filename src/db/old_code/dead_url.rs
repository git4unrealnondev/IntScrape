//! Database operations for the `dead_url` domain.

use super::{MainDatabase, SQL_CHUNK_SIZE};
use log::info;
use rusqlite::{Connection, params};
use shared_types::SkipIf;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl MainDatabase {
    ///
    /// Adds dead url into db
    ///
    pub fn internal_dead_url_add(
        &self,
        conn: &Connection,
        dead_url: &String,
    ) -> Result<(), r2d2_sqlite::rusqlite::Error> {
        conn.execute(
            "INSERT OR IGNORE INTO dead_urls (url) VALUES (?1);",
            params![dead_url],
        )?;
        Ok(())
    }

    ///
    /// Checks if a list of urls are dead or not
    ///
    pub fn internal_dead_url_exist(
        &self,
        conn: &Connection,
        potential_dead_urls: &[String],
    ) -> Result<HashMap<String, bool>, r2d2_sqlite::rusqlite::Error> {
        let mut dead_urls = HashSet::<String>::new();

        for chunk in potential_dead_urls.chunks(SQL_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "SELECT url
             FROM dead_urls
             WHERE url IN ({placeholders})"
            );

            let mut statement = conn.prepare(&sql)?;

            let rows = statement.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                row.get::<_, String>(0)
            })?;

            for row in rows {
                dead_urls.insert(row?);
            }
        }

        // Preserve the same order and length as the input.
        Ok(potential_dead_urls
            .iter()
            .map(|url| (url.to_string(), dead_urls.contains(url)))
            .collect())
    }

    ///
    /// Checks if we should download a file
    ///
    pub fn internal_should_download_file(&self, conn: &Connection, url: &str) -> bool {
        let source_url_nsid = self.internal_namespace_sourceurl_get(conn);

        if let Some(tag_id) = Self::internal_tag_get_id(conn, url, source_url_nsid) {
            return !self.internal_tag_has_files(conn, tag_id);
        }

        true
    }

    ///
    /// Should we skip doing something
    ///
    pub fn should_skip_item(&self, conn: &Connection, skip_conditions: SkipIf) -> bool {
        match skip_conditions {
            SkipIf::ParentsRelateLimitto((relate_to, limit_to)) => {
                if let Ok(status) =
                    self.internal_parent_relate_limit_exists(conn, &relate_to, &limit_to)
                    && status
                {
                    info!(
                        "DB Skipping adding job due to relate_to and limit_to exists {relate_to:?} {limit_to:?}"
                    );
                    return true;
                }
            }
            SkipIf::ParentsRelate(plugin_tag) => {
                if let Ok(status) = self.internal_parent_structure_exists(conn, &plugin_tag)
                    && status
                {
                    info!("DB Skipping adding job due to Parent existing {plugin_tag:?}");
                    return true;
                }
            }
            SkipIf::FileHash(_file_hash) => {}
            SkipIf::FileTagRelationship(tag) => {
                if let Some(ns_id) = self.internal_namespace_get_id(conn, &tag.namespace.name)
                    && let Some(tag_id) = MainDatabase::internal_tag_get_id(conn, &tag.name, ns_id)
                    && self.tag_has_files_cached(conn, tag_id)
                {
                    info!(
                        "DB Skipping adding job due to FileTagRelationship tag_id: {tag_id} having files."
                    );
                    return true;
                }
            }
            SkipIf::FileNamespaceNumber((_tag, _namespace, _id)) => {}
            SkipIf::NoFilesDownloaded => {}
        }
        false
    }

    ///
    /// Adds dead url into db
    ///
    pub async fn dead_url_add_async(self: Arc<Self>, dead_url: String) {
        let result = tokio::task::spawn_blocking(move || {
            self.dead_url_add_sync(&dead_url);
        });
        let _ = result.await;
    }

    pub async fn dead_url_exist(self: Arc<Self>, dead_url: Vec<String>) -> HashMap<String, bool> {
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };
            if let Ok(res) = self.internal_dead_url_exist(&conn, &dead_url) {
                return res;
            }
            HashMap::new()
        });
        result.await.unwrap_or(HashMap::new())
    }

    ///
    /// Should x be skipped
    ///
    pub async fn should_skip_processing_job(self: Arc<Self>, skip_conditions: Vec<SkipIf>) -> bool {
        if skip_conditions.is_empty() {
            return false;
        }
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to acquire DB connection from pool: {e:?}");
                    panic!();
                }
            };
            for skip_condition in skip_conditions {
                if self.should_skip_item(&conn, skip_condition) {
                    return true;
                }
            }
            false
        });
        result.await.unwrap_or(false)
    }
}
