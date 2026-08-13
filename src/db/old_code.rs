use rusqlite::Connection;

pub(in crate::db) fn internal_table_create_file_v1(conn: &Connection) {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS File 
            (id INTEGER PRIMARY KEY  NOT NULL, 
            hash TEXT UNIQUE, 
            extension TEXT, 
            storage_id INTEGER, 

            CHECK (
                (hash IS NOT NULL AND extension IS NOT NULL) OR
                (hash IS NULL AND extension IS NULL)
            ),

            FOREIGN KEY (storage_id) REFERENCES FileStorageLocations(id) ON DELETE CASCADE ON UPDATE CASCADE
            );

CREATE INDEX IF NOT EXISTS idx_file_hash ON File (hash);
").unwrap();
}
