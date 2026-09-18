use turso::{Connection, Result};

use crate::db::turso::TursoDatabase;

impl TursoDatabase {
    /// Creates the file tables.
    pub(in crate::db::turso) async fn table_create_file(&self, conn: &Connection) {
        conn.execute_batch("
CREATE TABLE File 
            (id INTEGER PRIMARY KEY  NOT NULL, 
            hash TEXT UNIQUE, 
            extension TEXT, 
            storage_id INTEGER, 
            size_bytes INTEGER

            CHECK (
                (hash IS NOT NULL AND extension IS NOT NULL) OR
                (hash IS NULL AND extension IS NULL)
            ),

            FOREIGN KEY (storage_id) REFERENCES FileStorageLocations(id) ON DELETE CASCADE ON UPDATE CASCADE
            );
CREATE INDEX idx_file_hash ON File (hash);

").await;
    }

    pub(in crate::db::turso) async fn table_create_filestoragelocations(&self, conn: &Connection) {
        conn.execute_batch("CREATE TABLE FileStorageLocations (id INTEGER PRIMARY KEY , location TEXT NOT NULL UNIQUE);").await;
    }

    /// Creates the filehash table
    pub(in crate::db::turso) async fn table_create_filehash(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE FileHashes (
    file_id INTEGER NOT NULL,
    algorithm TEXT NOT NULL,
    digest TEXT NOT NULL,

    PRIMARY KEY (file_id, algorithm),

    FOREIGN KEY (file_id)
        REFERENCES File(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);
CREATE UNIQUE INDEX idx_file_hashes_algorithm_digest ON FileHashes (algorithm, digest);

",
        )
        .await;
    }

    pub(in crate::db::turso) async fn table_create_jobs(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE Jobs (
    id INTEGER PRIMARY KEY  NOT NULL, 
    time INTEGER NOT NULL, 
    reptime INTEGER NOT NULL, 
    priority INTEGER NOT NULL,  
    is_running BOOL NOT NULL DEFAULT False,
    recreation TEXT NOT NULL, 
    site TEXT NOT NULL, 
    param TEXT NOT NULL, 
    user_data TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_jobs_dedup ON Jobs (time, reptime, site, param);
CREATE INDEX idx_jobs_ready_priority ON Jobs (site, is_running, priority DESC, time, id);
",
        )
        .await;
    }
    pub(in crate::db::turso) async fn table_create_namespace(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE Namespace (
    id INTEGER PRIMARY KEY , 
    name TEXT NOT NULL UNIQUE, 
    description TEXT
);
CREATE INDEX idx_namespace ON Namespace (name);
",
        )
        .await;
    }
    pub(in crate::db::turso) async fn table_create_parents(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("
CREATE TABLE Parents (
id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id INTEGER NOT NULL,
    relate_tag_id INTEGER NOT NULL,
    limit_to INTEGER,

    FOREIGN KEY (tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (relate_tag_id) REFERENCES Tags(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (limit_to) REFERENCES Tags(id) ON DELETE SET NULL ON UPDATE CASCADE,

    CHECK (tag_id != relate_tag_id),

    UNIQUE(tag_id, relate_tag_id, limit_to)
);

CREATE INDEX idx_parents_lim ON Parents (limit_to);
CREATE INDEX idx_parents_rel ON Parents (relate_tag_id);
CREATE UNIQUE INDEX idx_unique_parents_null_safe ON Parents (tag_id, relate_tag_id, IFNULL(limit_to, -1));
").await
    }
    pub(in crate::db::turso) async fn table_create_tags(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS Tags (
    id INTEGER PRIMARY KEY , 
    name TEXT NOT NULL, 
    namespace INTEGER NOT NULL, 
    count INTEGER NOT NULL DEFAULT 0, 

    UNIQUE(name, namespace), 

    FOREIGN KEY (namespace) REFERENCES Namespace(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_tags_count_covering ON Tags(count DESC, name, namespace);
CREATE INDEX IF NOT EXISTS idx_tags_fts ON Tags USING fts (name) WITH (tokenizer='ngram', min_gram=2, max_gram=3);
OPTIMIZE INDEX idx_tags_fts;
",
        )
        .await;
    }

    pub(in crate::db::turso) async fn table_create_dead_urls(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE dead_urls (url TEXT PRIMARY KEY);
",
        )
        .await;
    }

    pub(in crate::db::turso) async fn table_create_settings(&self, conn: &Connection) {
        conn.execute_batch(
            "
CREATE TABLE Settings (
    name TEXT PRIMARY KEY,
    description TEXT, 
    num INTEGER, 
    param TEXT
);

",
        )
        .await;
    }
}
