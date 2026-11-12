//! An in-memory [`StateRepositoryPort`] with failure injection.
//!
//! [`FakeRepository`] keeps the certificate ledger, the principal registry and the scaling
//! audit log in memory. Each operation can be told to fail, so tests can exercise the error
//! paths of their callers. Clones share one ledger.
//!
//! The behaviour follows the `SQLite` backend, which the contract suite checks against:
//!
//! - Certificates are kept in write order. [`StateRepositoryPort::record_issued`] rejects a
//!   serial that is already stored with a backend error and keeps the first record.
//!   [`FakeRepository::insert`] is a seeding helper that replaces a record with the same serial.
//! - [`StateRepositoryPort::revoke`] only changes an active certificate. Revoking a revoked
//!   certificate keeps the first revocation time.
//! - `revoked` lists records by revocation time and then serial, and `certificates_of` lists
//!   active records by issue time and then serial.
//! - The principal registry is keyed by identity, so it lists principals in identity order.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PortError, PrincipalRecord, RecordedScalingEvent,
    ScalingAuditEvent, ScalingEventSeq, StateRepositoryPort, Subject,
};
use gang_types::{CertSerial, Identity};
use parking_lot::Mutex;

/// A write or read that [`FakeRepository`] can be told to fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Failure {
    RecordIssued,
    ScalingWrite,
    Revoke,
    ScalingRead,
    Principals,
    RevokedReads,
}

/// The data behind a [`FakeRepository`].
#[derive(Debug, Default)]
struct State {
    issued: Vec<IssuedCertRecord>,
    scaling_events: Vec<RecordedScalingEvent>,
    principals: BTreeMap<Identity, PrincipalRecord>,
    failures: BTreeSet<Failure>,
    forget_certificates: bool,
}

impl State {
    /// Whether `failure` is currently injected.
    fn fails(&self, failure: Failure) -> bool {
        self.failures.contains(&failure)
    }
}

/// An in-memory state repository that can be told to fail its operations.
#[derive(Debug, Clone, Default)]
pub struct FakeRepository {
    state: Arc<Mutex<State>>,
}

impl FakeRepository {
    /// Every certificate record written so far, in write order.
    #[must_use]
    pub fn issued(&self) -> Vec<IssuedCertRecord> {
        self.state.lock().issued.clone()
    }

    /// Stores `record` as if it had been issued, replacing a record with the same serial.
    pub fn insert(&self, record: IssuedCertRecord) {
        let mut state = self.state.lock();
        match state
            .issued
            .iter_mut()
            .find(|existing| existing.serial == record.serial)
        {
            Some(existing) => *existing = record,
            None => state.issued.push(record),
        }
    }

    /// Every scaling audit event written so far, in sequence order.
    #[must_use]
    pub fn scaling_events(&self) -> Vec<ScalingAuditEvent> {
        self.recorded_scaling_events()
            .into_iter()
            .map(|recorded| recorded.event)
            .collect()
    }

    /// Every scaling audit event written so far with its sequence number, in sequence order.
    #[must_use]
    pub fn recorded_scaling_events(&self) -> Vec<RecordedScalingEvent> {
        self.state.lock().scaling_events.clone()
    }

    /// Every principal record in the registry, ordered by identity.
    #[must_use]
    pub fn principals(&self) -> Vec<PrincipalRecord> {
        self.state.lock().principals.values().cloned().collect()
    }

    /// Makes `record_issued` fail while `fail` is `true`.
    pub fn fail_record_issued(&self, fail: bool) {
        self.set_failure(Failure::RecordIssued, fail);
    }

    /// Makes scaling audit writes fail while `fail` is `true`.
    pub fn fail_scaling_events(&self, fail: bool) {
        self.set_failure(Failure::ScalingWrite, fail);
    }

    /// Makes scaling audit reads fail while `fail` is `true`.
    pub fn fail_scaling_reads(&self, fail: bool) {
        self.set_failure(Failure::ScalingRead, fail);
    }

    /// Makes `revoke` fail while `fail` is `true`.
    pub fn fail_revoke(&self, fail: bool) {
        self.set_failure(Failure::Revoke, fail);
    }

    /// Makes `revoked` fail while `fail` is `true`.
    pub fn fail_revoked_reads(&self, fail: bool) {
        self.set_failure(Failure::RevokedReads, fail);
    }

    /// Makes every principal registry operation fail while `fail` is `true`.
    pub fn fail_principals(&self, fail: bool) {
        self.set_failure(Failure::Principals, fail);
    }

    /// Makes `certificate` report every serial as unknown while `on` is `true`. Writes still
    /// succeed.
    pub fn forget_certificates(&self, on: bool) {
        self.state.lock().forget_certificates = on;
    }

    /// Adds `failure` to the injected failures when `on` is `true`, and removes it otherwise.
    fn set_failure(&self, failure: Failure, on: bool) {
        let mut state = self.state.lock();
        if on {
            state.failures.insert(failure);
        } else {
            state.failures.remove(&failure);
        }
    }
}

impl StateRepositoryPort for FakeRepository {
    async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::RecordIssued) {
            return Err(PortError::backend("record_issued was made to fail"));
        }
        if state
            .issued
            .iter()
            .any(|existing| existing.serial == record.serial)
        {
            return Err(PortError::backend("duplicate certificate serial"));
        }
        state.issued.push(record.clone());
        Ok(())
    }

    async fn certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Option<IssuedCertRecord>, PortError> {
        let state = self.state.lock();
        if state.forget_certificates {
            return Ok(None);
        }
        Ok(state
            .issued
            .iter()
            .find(|record| &record.serial == serial)
            .cloned())
    }

    async fn revoke(
        &self,
        serial: &CertSerial,
        at: Timestamp,
    ) -> Result<IssuedCertRecord, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Revoke) {
            return Err(PortError::unavailable("certificate ledger is unavailable"));
        }
        let record = state
            .issued
            .iter_mut()
            .find(|record| &record.serial == serial)
            .ok_or_else(|| PortError::NotFound(Subject::Certificate(serial.clone())))?;
        if record.status == CertStatus::Active {
            record.status = CertStatus::Revoked { at };
        }
        Ok(record.clone())
    }

    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
        let state = self.state.lock();
        if state.fails(Failure::RevokedReads) {
            return Err(PortError::unavailable("certificate ledger is unavailable"));
        }
        let mut revoked: Vec<IssuedCertRecord> = state
            .issued
            .iter()
            .filter(|record| matches!(record.status, CertStatus::Revoked { .. }))
            .cloned()
            .collect();
        revoked.sort_by(|a, b| {
            revoked_time(a)
                .cmp(&revoked_time(b))
                .then_with(|| a.serial.cmp(&b.serial))
        });
        Ok(revoked)
    }

    async fn certificates_of(
        &self,
        identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        let mut records: Vec<IssuedCertRecord> = self
            .state
            .lock()
            .issued
            .iter()
            .filter(|record| record.identity == *identity && record.status == CertStatus::Active)
            .cloned()
            .collect();
        records.sort_by(|a, b| {
            a.issued_at
                .cmp(&b.issued_at)
                .then_with(|| a.serial.cmp(&b.serial))
        });
        Ok(records)
    }

    async fn record_scaling_event(
        &self,
        event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::ScalingWrite) {
            return Err(PortError::backend("scaling audit log is unavailable"));
        }
        let seq = ScalingEventSeq::new(state.scaling_events.len() as u64 + 1);
        state.scaling_events.push(RecordedScalingEvent {
            seq,
            event: event.clone(),
        });
        Ok(seq)
    }

    async fn scaling_events(
        &self,
        since: ScalingEventSeq,
        before: Option<ScalingEventSeq>,
        limit: NonZeroUsize,
        into: &mut Vec<RecordedScalingEvent>,
    ) -> Result<bool, PortError> {
        let state = self.state.lock();
        if state.fails(Failure::ScalingRead) {
            return Err(PortError::unavailable("scaling audit log is unavailable"));
        }
        let window: Vec<&RecordedScalingEvent> = state
            .scaling_events
            .iter()
            .filter(|recorded| recorded.seq > since)
            .filter(|recorded| before.is_none_or(|bound| recorded.seq < bound))
            .collect();
        let skipped = window.len().saturating_sub(limit.get());
        into.clear();
        into.extend(
            window
                .iter()
                .skip(skipped)
                .map(|recorded| (*recorded).clone()),
        );
        Ok(skipped > 0)
    }

    async fn observe_principal(&self, identity: &Identity, at: Timestamp) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Principals) {
            return Err(PortError::unavailable("principal registry is unavailable"));
        }
        let record = state
            .principals
            .entry(identity.clone())
            .or_insert_with(|| first_record(identity, at));
        record.last_seen = record.last_seen.max(at);
        Ok(())
    }

    async fn principal(&self, identity: &Identity) -> Result<Option<PrincipalRecord>, PortError> {
        let state = self.state.lock();
        if state.fails(Failure::Principals) {
            return Err(PortError::unavailable("principal registry is unavailable"));
        }
        Ok(state.principals.get(identity).cloned())
    }

    async fn revoke_principal(
        &self,
        identity: &Identity,
        at: Timestamp,
    ) -> Result<PrincipalRecord, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Principals) {
            return Err(PortError::unavailable("principal registry is unavailable"));
        }
        let record = state
            .principals
            .entry(identity.clone())
            .or_insert_with(|| first_record(identity, at));
        record.revoked_at.get_or_insert(at);
        Ok(record.clone())
    }

    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Principals) {
            return Err(PortError::unavailable("principal registry is unavailable"));
        }
        let record = state
            .principals
            .get_mut(identity)
            .ok_or_else(|| PortError::NotFound(Subject::Principal(identity.clone())))?;
        record.revoked_at = None;
        Ok(record.clone())
    }

    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
        let state = self.state.lock();
        if state.fails(Failure::Principals) {
            return Err(PortError::unavailable("principal registry is unavailable"));
        }
        let mut revoked: Vec<PrincipalRecord> = state
            .principals
            .values()
            .filter(|record| record.revoked_at.is_some())
            .cloned()
            .collect();
        revoked.sort_by(|a, b| {
            a.revoked_at
                .cmp(&b.revoked_at)
                .then_with(|| a.identity.cmp(&b.identity))
        });
        Ok(revoked)
    }

    async fn principals(
        &self,
        after: Option<&Identity>,
        limit: NonZeroUsize,
        only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError> {
        let state = self.state.lock();
        if state.fails(Failure::Principals) {
            return Err(PortError::unavailable("principal registry is unavailable"));
        }
        let window: Vec<&PrincipalRecord> = state
            .principals
            .iter()
            .filter(|(identity, _)| after.is_none_or(|bound| *identity > bound))
            .map(|(_, record)| record)
            .filter(|record| !only_revoked || record.revoked_at.is_some())
            .take(limit.get() + 1)
            .collect();
        let more = window.len() > limit.get();
        into.clear();
        into.extend(
            window
                .iter()
                .take(limit.get())
                .map(|record| (*record).clone()),
        );
        Ok(more)
    }
}

/// The registry record for a principal first seen at `at`.
fn first_record(identity: &Identity, at: Timestamp) -> PrincipalRecord {
    PrincipalRecord {
        identity: identity.clone(),
        first_seen: at,
        last_seen: at,
        revoked_at: None,
    }
}

/// The time `record` was revoked, or `None` while it is active.
fn revoked_time(record: &IssuedCertRecord) -> Option<Timestamp> {
    match record.status {
        CertStatus::Active => None,
        CertStatus::Revoked { at } => Some(at),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use gang_clock::Timestamp;
    use gang_spi::{
        CertStatus, IssuedCertRecord, PortError, ScalingAuditEvent, ScalingEventKind,
        ScalingEventSeq, StateRepositoryPort,
    };
    use gang_types::{CertSerial, FleetName, Identity, Principal, ProviderName, WorkerId};

    use super::FakeRepository;
    use crate::TestResult;

    /// A certificate record for serial `serial`, issued to `principal` under `provider`.
    fn record(
        serial: u8,
        provider: &str,
        principal: &str,
        issued_at: u64,
    ) -> Result<IssuedCertRecord, Box<dyn std::error::Error>> {
        Ok(IssuedCertRecord {
            serial: CertSerial::from_be_bytes(&[serial])?,
            identity: identity(provider, principal)?,
            issued_at: Timestamp::from_unix_millis(issued_at),
            expires_at: Timestamp::from_unix_millis(issued_at + 1_000),
            status: CertStatus::Active,
        })
    }

    /// The identity `principal` under `provider`.
    fn identity(provider: &str, principal: &str) -> Result<Identity, Box<dyn std::error::Error>> {
        Ok(Identity::new(
            ProviderName::try_new(provider)?,
            Principal::try_new(principal)?,
        ))
    }

    /// A worker-registered scaling event for the fleet `fleet-a`.
    fn scaling_event(worker: &str) -> Result<ScalingAuditEvent, Box<dyn std::error::Error>> {
        Ok(ScalingAuditEvent {
            at: Timestamp::from_unix_millis(1),
            fleet: FleetName::try_new("fleet-a")?,
            kind: ScalingEventKind::WorkerRegistered {
                worker: WorkerId::try_new(worker)?,
            },
            cause: None,
        })
    }

    fn limit(value: usize) -> Result<NonZeroUsize, Box<dyn std::error::Error>> {
        NonZeroUsize::new(value).ok_or_else(|| "limit must not be zero".into())
    }

    crate::repository_contract!(|| async {
        Ok::<_, Box<dyn std::error::Error>>(FakeRepository::default())
    });

    #[tokio::test]
    async fn each_failure_toggle_fails_exactly_its_operations() -> TestResult {
        let repository = FakeRepository::default();
        let issued = record(1, "github", "alice", 10)?;
        let alice = identity("github", "alice")?;

        repository.fail_record_issued(true);
        assert!(repository.record_issued(&issued).await.is_err());
        repository.fail_record_issued(false);
        repository.record_issued(&issued).await?;

        repository.fail_revoke(true);
        assert!(
            repository
                .revoke(&issued.serial, Timestamp::from_unix_millis(1))
                .await
                .is_err()
        );
        repository.fail_revoke(false);
        repository
            .revoke(&issued.serial, Timestamp::from_unix_millis(1))
            .await?;

        repository.fail_revoked_reads(true);
        assert!(matches!(
            repository.revoked().await,
            Err(PortError::Unavailable(_))
        ));
        assert!(repository.certificate(&issued.serial).await?.is_some());
        repository.fail_revoked_reads(false);
        assert_eq!(repository.revoked().await?.len(), 1);

        repository.fail_scaling_events(true);
        assert!(
            repository
                .record_scaling_event(&scaling_event("worker-1")?)
                .await
                .is_err()
        );
        repository.fail_scaling_events(false);
        repository
            .record_scaling_event(&scaling_event("worker-1")?)
            .await?;

        repository.fail_scaling_reads(true);
        let mut into = Vec::new();
        assert!(
            StateRepositoryPort::scaling_events(
                &repository,
                ScalingEventSeq::new(0),
                None,
                limit(1)?,
                &mut into,
            )
            .await
            .is_err()
        );
        repository.fail_scaling_reads(false);
        StateRepositoryPort::scaling_events(
            &repository,
            ScalingEventSeq::new(0),
            None,
            limit(1)?,
            &mut into,
        )
        .await?;

        repository.fail_principals(true);
        assert!(
            repository
                .observe_principal(&alice, Timestamp::from_unix_millis(1))
                .await
                .is_err()
        );
        assert!(repository.principal(&alice).await.is_err());
        assert!(
            repository
                .revoke_principal(&alice, Timestamp::from_unix_millis(1))
                .await
                .is_err()
        );
        assert!(repository.restore_principal(&alice).await.is_err());
        assert!(repository.revoked_principals().await.is_err());
        let mut principals_into = Vec::new();
        assert!(
            StateRepositoryPort::principals(
                &repository,
                None,
                limit(1)?,
                false,
                &mut principals_into,
            )
            .await
            .is_err()
        );
        repository.fail_principals(false);
        repository
            .observe_principal(&alice, Timestamp::from_unix_millis(1))
            .await?;
        assert!(repository.principal(&alice).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn forget_certificates_hides_lookups_but_not_writes() -> TestResult {
        let repository = FakeRepository::default();
        let issued = record(1, "github", "alice", 10)?;
        repository.forget_certificates(true);
        repository.record_issued(&issued).await?;
        assert_eq!(repository.certificate(&issued.serial).await?, None);
        assert_eq!(repository.issued(), vec![issued.clone()]);
        repository.forget_certificates(false);
        assert_eq!(repository.certificate(&issued.serial).await?, Some(issued));
        Ok(())
    }

    #[tokio::test]
    async fn clones_share_state() -> TestResult {
        let repository = FakeRepository::default();
        let clone = repository.clone();
        clone
            .record_issued(&record(1, "github", "alice", 10)?)
            .await?;
        clone.insert(record(2, "github", "bob", 20)?);
        assert_eq!(repository.issued().len(), 2);
        repository.fail_principals(true);
        assert!(
            clone
                .principal(&identity("github", "alice")?)
                .await
                .is_err()
        );
        Ok(())
    }
}
