//! Which tables hold relationships, and which of them a write touches.
//!
//! Schema v15 adds a flat `Relationship` table alongside the legacy
//! per-namespace `Relationship_{ns}` partitions.
//!
//! The partitions were introduced to give the *import* path a way to filter
//! rows per namespace -- a single shared table forced a near-full scan for
//! every sparse namespace, which measured as ~9.5 hours of source reads in
//! one production slurp. They never helped the read path: a file's tags can
//! live in any namespace, so every partition had to be probed anyway, through
//! a `UNION ALL` whose SQL text grew with the namespace count. The flat table
//! answers the same query with one index seek.
//!
//! The flat table also carries the tag's namespace on the row. That is
//! denormalized from `Tags.namespace`, and it is what lets the per-namespace
//! recount stay per-namespace: without it, a recount would have to sweep the
//! whole `Tags` tree, which the slurp's own comments record as pinning RSS
//! at ~10 GB with no wal progress.
//!
//! The legacy partitions are kept in the schema for one release so a rollback
//! does not need a restore. Two settings drive the rollout, both read through
//! the settings cache:
//!
//! * `relationship_write_mode` -- `single` (flat only), `dual` (both), or
//!   `legacy` (partitions only).
//! * `relationship_read_mode` -- `flat` or `legacy`.
//!
//! The default is `single` / `flat`: the flat table is the only write target,
//! which is the whole point of the migration. That makes `legacy` reads
//! *unsafe* unless the partitions have been kept in step, so a database whose
//! read mode is `legacy` while its write mode is `single` gets a loud warning
//! at boot -- the partitions will be missing every relationship written since
//! the switch, and the symptom is silently incomplete search results rather
//! than an error. Re-point writes at `dual` and let a slurp's recount pass
//! refill the partitions before reading them again.

use turso::Connection;

use crate::db::turso::TursoDatabase;

/// Setting that decides which tables a relationship write touches.
pub(in crate::db::turso) const WRITE_MODE_SETTING: &str = "relationship_write_mode";

/// Setting that decides which tables a relationship read uses.
pub(in crate::db::turso) const READ_MODE_SETTING: &str = "relationship_read_mode";

/// Which tables a relationship write touches.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::db::turso) enum WriteMode {
    /// Only the legacy per-namespace partitions.
    Legacy,
    /// Both the flat table and the legacy partitions.
    Dual,
    /// Only the flat table.
    Single,
}

impl WriteMode {
    /// Whether the flat table takes part in a write.
    pub(in crate::db::turso) fn writes_flat(self) -> bool {
        !matches!(self, WriteMode::Legacy)
    }

    /// Whether the legacy partitions take part in a write.
    pub(in crate::db::turso) fn writes_partitions(self) -> bool {
        !matches!(self, WriteMode::Single)
    }
}

/// Which tables a relationship read uses.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::db::turso) enum ReadMode {
    /// The flat `Relationship` table.
    Flat,
    /// The legacy per-namespace partitions, through the union.
    Legacy,
}

impl TursoDatabase {
    /// Which tables a relationship write touches.
    ///
    /// Read through the settings cache so the hot write path does not issue a
    /// query per chunk. The cache is seeded at boot from the `Settings`
    /// table, and `setting_get_sql` populates it on a miss, so a mode
    /// flipped by the API is picked up without a restart.
    ///
    /// An unset or unrecognised value resolves to `Single`: the flat table is
    /// the destination this build expects, and defaulting to `Dual` instead
    /// would quietly keep paying for a second write that the migration exists
    /// to remove.
    pub(in crate::db::turso) async fn relationship_write_mode(
        &self,
        conn: &Connection,
    ) -> WriteMode {
        match self
            .setting_get_sql(conn, WRITE_MODE_SETTING)
            .await
            .ok()
            .flatten()
            .and_then(|setting| setting.param)
            .as_deref()
        {
            Some("dual") => WriteMode::Dual,
            Some("legacy") => WriteMode::Legacy,
            _ => WriteMode::Single,
        }
    }

    /// Which tables a relationship read uses.
    pub(in crate::db::turso) async fn relationship_read_mode(
        &self,
        conn: &Connection,
    ) -> ReadMode {
        match self
            .setting_get_sql(conn, READ_MODE_SETTING)
            .await
            .ok()
            .flatten()
            .and_then(|setting| setting.param)
            .as_deref()
        {
            Some("legacy") => ReadMode::Legacy,
            _ => ReadMode::Flat,
        }
    }
}

#[cfg(test)]
mod tests {
    //! The rollout knobs have to be readable without a restart, because the
    //! rollback story is "flip a setting", not "restore a backup".

    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    async fn fresh_db() -> Arc<TursoDatabase> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.keep().join("settings.db");
        TursoDatabase::new_with_exit(&path, Arc::new(AtomicBool::new(false))).await
    }

    /// A fresh database defaults to flat-only writes and flat reads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn defaults_are_single_write_and_flat_read() {
        let db = fresh_db().await;
        let conn = db.connect().unwrap();
        assert_eq!(
            db.relationship_write_mode(&conn).await,
            WriteMode::Single,
            "a fresh v15 database must default to flat-only writes"
        );
        assert_eq!(
            db.relationship_read_mode(&conn).await,
            ReadMode::Flat,
            "a fresh v15 database must default to flat reads"
        );
    }

    /// An unset or unrecognised write mode resolves to `Single` rather than
    /// `Dual`, so a database missing the setting does not quietly keep paying
    /// for the second write the migration removes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_write_mode_falls_back_to_single() {
        let db = fresh_db().await;
        let conn = db.connect().unwrap();
        db.setting_set(
            &conn,
            shared_types::DbSettingsObj {
                name: WRITE_MODE_SETTING.into(),
                description: None,
                num: None,
                param: Some("sideways".into()),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            db.relationship_write_mode(&conn).await,
            WriteMode::Single,
            "an unrecognised mode must fall back to the flat-only default"
        );
    }

    /// Flipping the mode through the API must be visible to the next read
    /// without a restart -- that is what makes the rollback a setting flip.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mode_flips_take_effect_without_a_restart() {
        let db = fresh_db().await;
        let conn = db.connect().unwrap();

        db.setting_set(
            &conn,
            shared_types::DbSettingsObj {
                name: WRITE_MODE_SETTING.into(),
                description: None,
                num: None,
                param: Some("single".into()),
            },
        )
        .await
        .unwrap();
        assert_eq!(db.relationship_write_mode(&conn).await, WriteMode::Single);

        db.setting_set(
            &conn,
            shared_types::DbSettingsObj {
                name: READ_MODE_SETTING.into(),
                description: None,
                num: None,
                param: Some("legacy".into()),
            },
        )
        .await
        .unwrap();
        assert_eq!(db.relationship_read_mode(&conn).await, ReadMode::Legacy);
    }

    /// The mode combination that fails silently -- legacy reads with flat-only
    /// writes -- must be the one the boot warning is about. This pins the
    /// predicate itself, since a regression here would remove the only signal
    /// that search results are incomplete.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_write_with_legacy_read_is_the_warned_combination() {
        let db = fresh_db().await;
        let conn = db.connect().unwrap();

        // Default state: safe.
        assert!(
            !matches!(
                (db.relationship_write_mode(&conn).await, db.relationship_read_mode(&conn).await),
                (WriteMode::Single, ReadMode::Legacy)
            ),
            "the default combination must not be the warned one"
        );

        db.setting_set(
            &conn,
            shared_types::DbSettingsObj {
                name: READ_MODE_SETTING.into(),
                description: None,
                num: None,
                param: Some("legacy".into()),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(
                (db.relationship_write_mode(&conn).await, db.relationship_read_mode(&conn).await),
                (WriteMode::Single, ReadMode::Legacy)
            ),
            "single write with legacy read must be detected as the unsafe combination"
        );

        // Restoring dual makes it safe again.
        db.setting_set(
            &conn,
            shared_types::DbSettingsObj {
                name: WRITE_MODE_SETTING.into(),
                description: None,
                num: None,
                param: Some("dual".into()),
            },
        )
        .await
        .unwrap();
        assert!(!matches!(
            (db.relationship_write_mode(&conn).await, db.relationship_read_mode(&conn).await),
            (WriteMode::Single, ReadMode::Legacy)
        ));
    }
}
