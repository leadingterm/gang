//! Runs the shared `StateRepositoryPort` contract suite against the `SQLite` backend.

use std::future::ready;
use std::num::NonZeroUsize;
use std::path::Path;

use gang_clock::Timestamp;
use gang_spi::{
    IssuedCertRecord, Plugin, PluginContext, PortError, PrincipalRecord, RecordedScalingEvent,
    ScalingAuditEvent, ScalingEventSeq, StateRepositoryPort,
};
use gang_sqlite::{Sqlite, SqliteConfig};
use gang_types::{CertSerial, Identity};
use tempfile::TempDir;

/// A `SQLite` store together with the temporary directory that holds its database.
struct TempStore {
    // Declared before `_dir` so the store closes before its directory is removed.
    store: Sqlite,
    _dir: TempDir,
}

impl TempStore {
    /// A store over a database in a fresh temporary directory.
    fn open() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let context = PluginContext {
            config_path: Path::new("gang.toml"),
        };
        let store = Sqlite::build(SqliteConfig::new(dir.path().join("gang.db")), &context)?;
        Ok(Self { store, _dir: dir })
    }
}

impl StateRepositoryPort for TempStore {
    async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError> {
        self.store.record_issued(record).await
    }

    async fn certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Option<IssuedCertRecord>, PortError> {
        self.store.certificate(serial).await
    }

    async fn revoke(
        &self,
        serial: &CertSerial,
        at: Timestamp,
    ) -> Result<IssuedCertRecord, PortError> {
        self.store.revoke(serial, at).await
    }

    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
        self.store.revoked().await
    }

    async fn certificates_of(
        &self,
        identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        self.store.certificates_of(identity).await
    }

    async fn record_scaling_event(
        &self,
        event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError> {
        self.store.record_scaling_event(event).await
    }

    async fn scaling_events(
        &self,
        since: ScalingEventSeq,
        before: Option<ScalingEventSeq>,
        limit: NonZeroUsize,
        into: &mut Vec<RecordedScalingEvent>,
    ) -> Result<bool, PortError> {
        self.store.scaling_events(since, before, limit, into).await
    }

    async fn observe_principal(&self, identity: &Identity, at: Timestamp) -> Result<(), PortError> {
        self.store.observe_principal(identity, at).await
    }

    async fn principal(&self, identity: &Identity) -> Result<Option<PrincipalRecord>, PortError> {
        self.store.principal(identity).await
    }

    async fn revoke_principal(
        &self,
        identity: &Identity,
        at: Timestamp,
    ) -> Result<PrincipalRecord, PortError> {
        self.store.revoke_principal(identity, at).await
    }

    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        self.store.restore_principal(identity).await
    }

    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
        self.store.revoked_principals().await
    }

    async fn principals(
        &self,
        after: Option<&Identity>,
        limit: NonZeroUsize,
        only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError> {
        self.store
            .principals(after, limit, only_revoked, into)
            .await
    }
}

gang_testkit::repository_contract!(|| ready(TempStore::open()));
