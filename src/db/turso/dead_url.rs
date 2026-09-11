use std::collections::{HashMap, HashSet};

use log::info;
use shared_types::SkipIf;
use turso::{Connection, Result, params_from_iter};

use crate::db::SQL_CHUNK_SIZE;
use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Marks a url as dead in the db.
    pub(in crate::db::turso) async fn dead_url_add(
        &self,
        conn: &Connection,
        dead_url: &str,
    ) -> Result<()> {
        conn.execute(
            "INSERT OR IGNORE INTO dead_urls (url) VALUES (?1);",
            (dead_url,),
        )
        .await?;

        Ok(())
    }

    /// Checks if a list of urls are dead, preserving the input's order and length.
    pub(in crate::db::turso) async fn dead_url_get(
        &self,
        conn: &Connection,
        potential_dead_urls: &[String],
    ) -> Result<HashMap<String, bool>> {
        let mut dead_urls = HashSet::new();

        for chunk in potential_dead_urls.chunks(SQL_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT url FROM dead_urls WHERE url IN ({placeholders});");
            let params: Vec<_> = chunk.iter().map(|url| url.as_str()).collect();

            let mut rows = conn.query(&sql, params_from_iter(params)).await?;
            while let Some(row) = rows.next().await? {
                dead_urls.insert(row.get::<String>(0)?);
            }
        }

        Ok(potential_dead_urls
            .iter()
            .map(|url| (url.clone(), dead_urls.contains(url)))
            .collect())
    }
}

impl TursoDatabase {
    /// Marks a url as being dead in the db.
    pub async fn dead_url_add_async(&self, dead_url: String) -> bool {
        let result = self
            .retry_mvcc(|| async {
                let conn = self.connect()?;
                self.dead_url_add(&conn, &dead_url).await
            })
            .await;
        if let Err(error) = &result {
            log::error!("Failed to mark dead url: {error}");
        }
        result.is_ok()
    }

    /// Checks whether a list of urls are dead.
    pub async fn dead_url_exist(&self, dead_url: Vec<String>) -> HashMap<String, bool> {
        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while checking dead urls: {error}");
                return HashMap::new();
            }
        };
        self.dead_url_get(&conn, &dead_url).await.unwrap_or_default()
    }

    /// Checks if a file with this source URL should be downloaded.
    pub async fn should_download_file(&self, url: String) -> bool {
        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while checking download eligibility: {error}");
                return true;
            }
        };

        let Ok(source_url_nsid) = self.namespace_sourceurl_get(&conn).await else {
            return true;
        };
        let Ok(Some(tag_id)) = self.tag_get_id(&conn, &url, source_url_nsid).await else {
            return true;
        };

        match self.tag_has_files(&conn, tag_id).await {
            Ok(false) | Err(_) => true,
            Ok(true) => false,
        }
    }

    /// Should x be skipped based on the given conditions.
    pub async fn should_skip_processing_job(&self, skip_conditions: Vec<SkipIf>) -> bool {
        if skip_conditions.is_empty() {
            return false;
        }
        let conn = match self.db.connect() {
            Ok(conn) => conn,
            Err(error) => {
                log::error!("Failed to connect while evaluating skip conditions: {error}");
                return false;
            }
        };
        for skip_condition in skip_conditions {
            if self.should_skip_item(&conn, skip_condition).await {
                return true;
            }
        }
        false
    }

    /// Evaluates a single skip condition against the database.
    pub(in crate::db::turso) async fn should_skip_item(
        &self,
        conn: &Connection,
        skip_conditions: SkipIf,
    ) -> bool {
        match skip_conditions {
            SkipIf::ParentsRelateLimitto((relate_to, limit_to)) => {
                if let Ok(status) =
                    self.parent_relate_limit_exists(conn, &relate_to, &limit_to).await
                    && status
                {
                    info!(
                        "DB Skipping adding job due to relate_to and limit_to exists {relate_to:?} {limit_to:?}"
                    );
                    return true;
                }
            }
            SkipIf::ParentsRelate(plugin_tag) => {
                if let Ok(status) = self.parent_structure_exists(conn, &plugin_tag).await
                    && status
                {
                    info!("DB Skipping adding job due to Parent existing {plugin_tag:?}");
                    return true;
                }
            }
            SkipIf::FileHash(_file_hash) => {}
            SkipIf::FileTagRelationship(tag) => {
                let Ok(Some(ns_id)) = self.namespace_get(conn, &tag.namespace.name).await else {
                    return false;
                };
                if let Ok(Some(tag_id)) = self.tag_get_id(conn, &tag.name, ns_id).await
                    && self.tag_has_files(conn, tag_id).await.unwrap_or(false)
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
}
