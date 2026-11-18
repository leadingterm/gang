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
use std::sync::atomic::{AtomicUsize, Ordering};

use gang_clock::Timestamp;
use gang_spi::{
    Acquisition, CertStatus, ControllerId, ExpiringRecordPort, Insert, IssuedCertRecord,
    LeaderLeasePort, Lease, LeaseEpoch, PortError, PrincipalRecord, Record, RecordKey,
    RecordVersion, RecordedScalingEvent, Remove, Renewal, Replace, RevocationEpoch,
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
    Lease,
    Records,
}

/// The data behind a [`FakeRepository`].
#[derive(Debug, Default)]
struct State {
    issued: Vec<IssuedCertRecord>,
    scaling_events: Vec<RecordedScalingEvent>,
    principals: BTreeMap<Identity, PrincipalRecord>,
    revocation_epoch: RevocationEpoch,
    lease: Option<LeaseRow>,
    records: BTreeMap<RecordKey, Record>,
    failures: BTreeSet<Failure>,
    fail_revoke_serial: Option<CertSerial>,
    forget_certificates: bool,
    scaling_reads: Arc<AtomicUsize>,
    watch_scaling_reads: Option<Arc<AtomicUsize>>,
    reads_at_first_write: Option<usize>,
}

#[derive(Debug)]
struct LeaseRow {
    holder: Option<ControllerId>,
    epoch: LeaseEpoch,
    expires_at: Timestamp,
}

impl State {
    /// Whether `failure` is currently injected.
    fn fails(&self, failure: Failure) -> bool {
        self.failures.contains(&failure)
    }

    fn next_revocation_epoch(&self) -> Result<RevocationEpoch, PortError> {
        self.revocation_epoch
            .get()
            .checked_add(1)
            .map(RevocationEpoch::new)
            .ok_or_else(|| PortError::backend("revocation epoch overflow"))
    }
}

/// An in-memory state repository that can be told to fail its operations.
#[derive(Debug, Clone, Default)]
pub struct FakeRepository {
    state: Arc<Mutex<State>>,
    restore_gate: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
    record_issued_gate: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
}

impl FakeRepository {
    /// Pauses a successful restore after its durable mutation until `gate` is notified.
    pub fn gate_restore(&self, gate: Arc<tokio::sync::Notify>) {
        *self.restore_gate.lock() = Some(gate);
    }

    /// Pauses the next `record_issued` before durable mutation until `gate` is notified.
    pub fn gate_record_issued(&self, gate: Arc<tokio::sync::Notify>) {
        *self.record_issued_gate.lock() = Some(gate);
    }

    /// Every certificate record written so far, in write order.
    #[must_use]
    pub fn issued(&self) -> Vec<IssuedCertRecord> {
        self.state.lock().issued.clone()
    }

    /// Number of physically stored expiring records, including expired ones.
    #[must_use]
    pub fn stored_records(&self) -> usize {
        self.state.lock().records.len()
    }

    /// Records the source's page-read count when this repository first writes an audit event.
    pub fn watch_first_scaling_write(&self, source: &Self) {
        let counter = Arc::clone(&source.state.lock().scaling_reads);
        self.state.lock().watch_scaling_reads = Some(counter);
    }

    /// Source page reads observed at the first watched audit write.
    #[must_use]
    pub fn reads_at_first_scaling_write(&self) -> Option<usize> {
        self.state.lock().reads_at_first_write
    }

    /// Sets the revocation epoch for a test fixture.
    pub fn set_revocation_epoch(&self, epoch: RevocationEpoch) {
        self.state.lock().revocation_epoch = epoch;
    }

    /// Removes one audit row to construct a source with a sequence gap.
    pub fn remove_scaling_event(&self, seq: ScalingEventSeq) {
        self.state
            .lock()
            .scaling_events
            .retain(|event| event.seq != seq);
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

    /// Makes `revoke` fail for one serial while other serials remain writable.
    pub fn fail_revoke_serial(&self, serial: CertSerial) {
        self.state.lock().fail_revoke_serial = Some(serial);
    }

    /// Makes `revoked` fail while `fail` is `true`.
    pub fn fail_revoked_reads(&self, fail: bool) {
        self.set_failure(Failure::RevokedReads, fail);
    }

    /// Makes every principal registry operation fail while `fail` is `true`.
    pub fn fail_principals(&self, fail: bool) {
        self.set_failure(Failure::Principals, fail);
    }

    /// Makes every lease operation unavailable while `fail` is `true`.
    pub fn fail_lease(&self, fail: bool) {
        self.set_failure(Failure::Lease, fail);
    }

    /// Makes every expiring record operation unavailable while `fail` is `true`.
    pub fn fail_records(&self, fail: bool) {
        self.set_failure(Failure::Records, fail);
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
        let gate = self.record_issued_gate.lock().take();
        if let Some(gate) = gate {
            gate.notified().await;
        }
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
        if state.fails(Failure::Revoke) || state.fail_revoke_serial.as_ref() == Some(serial) {
            return Err(PortError::unavailable("certificate ledger is unavailable"));
        }
        let next_epoch = state.next_revocation_epoch()?;
        let record = state
            .issued
            .iter_mut()
            .find(|record| &record.serial == serial)
            .ok_or_else(|| PortError::NotFound(Subject::Certificate(serial.clone())))?;
        if record.status == CertStatus::Active {
            record.status = CertStatus::Revoked { at };
        }
        let record = record.clone();
        state.revocation_epoch = next_epoch;
        Ok(record)
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

    async fn certificates(
        &self,
        after: Option<&CertSerial>,
        limit: NonZeroUsize,
        into: &mut Vec<IssuedCertRecord>,
    ) -> Result<bool, PortError> {
        let state = self.state.lock();
        let mut records: Vec<_> = state
            .issued
            .iter()
            .filter(|record| after.is_none_or(|bound| &record.serial > bound))
            .collect();
        records.sort_by(|a, b| a.serial.cmp(&b.serial));
        let more = records.len() > limit.get();
        into.clear();
        into.extend(records.into_iter().take(limit.get()).cloned());
        Ok(more)
    }

    async fn revocation_epoch(&self) -> Result<RevocationEpoch, PortError> {
        Ok(self.state.lock().revocation_epoch)
    }

    async fn record_scaling_event(
        &self,
        event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::ScalingWrite) {
            return Err(PortError::backend("scaling audit log is unavailable"));
        }
        if state.reads_at_first_write.is_none()
            && let Some(counter) = &state.watch_scaling_reads
        {
            state.reads_at_first_write = Some(counter.load(Ordering::Relaxed));
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
        state.scaling_reads.fetch_add(1, Ordering::Relaxed);
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
        let next_epoch = state.next_revocation_epoch()?;
        let record = state
            .principals
            .entry(identity.clone())
            .or_insert_with(|| first_record(identity, at));
        record.revoked_at.get_or_insert(at);
        let record = record.clone();
        state.revocation_epoch = next_epoch;
        Ok(record)
    }

    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        let record = {
            let mut state = self.state.lock();
            if state.fails(Failure::Principals) {
                return Err(PortError::unavailable("principal registry is unavailable"));
            }
            let next_epoch = state.next_revocation_epoch()?;
            let record = state
                .principals
                .get_mut(identity)
                .ok_or_else(|| PortError::NotFound(Subject::Principal(identity.clone())))?;
            record.revoked_at = None;
            let record = record.clone();
            state.revocation_epoch = next_epoch;
            record
        };
        let gate = self.restore_gate.lock().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        Ok(record)
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

impl LeaderLeasePort for FakeRepository {
    async fn acquire(
        &self,
        holder: &ControllerId,
        stale_before: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Acquisition, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Lease) {
            return Err(PortError::unavailable("controller lease is unavailable"));
        }
        if let Some(row) = &state.lease
            && let Some(current) = &row.holder
            && row.expires_at >= stale_before
        {
            return Ok(Acquisition::Held(Lease {
                holder: current.clone(),
                epoch: row.epoch,
                expires_at: row.expires_at,
            }));
        }
        let epoch = match &state.lease {
            Some(row) => row
                .epoch
                .get()
                .checked_add(1)
                .ok_or_else(|| PortError::backend("controller lease epoch overflow"))?,
            None => 1,
        };
        let lease = Lease {
            holder: holder.clone(),
            epoch: LeaseEpoch::new(epoch),
            expires_at,
        };
        state.lease = Some(LeaseRow {
            holder: Some(holder.clone()),
            epoch: lease.epoch,
            expires_at,
        });
        Ok(Acquisition::Acquired(lease))
    }

    async fn renew(&self, lease: &Lease, expires_at: Timestamp) -> Result<Renewal, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Lease) {
            return Err(PortError::unavailable("controller lease is unavailable"));
        }
        match state.lease.as_mut() {
            Some(row) if row.holder.as_ref() == Some(&lease.holder) && row.epoch == lease.epoch => {
                row.expires_at = expires_at;
                Ok(Renewal::Renewed(Lease {
                    holder: lease.holder.clone(),
                    epoch: row.epoch,
                    expires_at,
                }))
            }
            _ => Ok(Renewal::Lost),
        }
    }

    async fn release(&self, lease: &Lease) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Lease) {
            return Err(PortError::unavailable("controller lease is unavailable"));
        }
        if let Some(row) = state.lease.as_mut()
            && row.holder.as_ref() == Some(&lease.holder)
            && row.epoch == lease.epoch
        {
            row.holder = None;
        }
        Ok(())
    }
}

impl ExpiringRecordPort for FakeRepository {
    async fn insert(
        &self,
        records: &[(RecordKey, &[u8])],
        version: RecordVersion,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Insert, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Records) {
            return Err(PortError::unavailable("expiring records are unavailable"));
        }
        let mut keys = BTreeSet::new();
        if records.iter().any(|(key, _)| !keys.insert(*key)) {
            return Ok(Insert::Exists);
        }
        if records.iter().any(|(key, _)| {
            state
                .records
                .get(key)
                .is_some_and(|record| now < record.expires_at)
        }) {
            return Ok(Insert::Exists);
        }
        for (key, body) in records {
            state.records.insert(
                *key,
                Record {
                    body: body.to_vec(),
                    version,
                    expires_at,
                },
            );
        }
        Ok(Insert::Inserted)
    }

    async fn get(&self, key: &RecordKey, now: Timestamp) -> Result<Option<Record>, PortError> {
        let state = self.state.lock();
        if state.fails(Failure::Records) {
            return Err(PortError::unavailable("expiring records are unavailable"));
        }
        Ok(state
            .records
            .get(key)
            .filter(|record| now < record.expires_at)
            .cloned())
    }

    async fn replace(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        body: &[u8],
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Replace, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Records) {
            return Err(PortError::unavailable("expiring records are unavailable"));
        }
        let Some(record) = state.records.get_mut(key) else {
            return Ok(Replace::Stale);
        };
        if now >= record.expires_at || record.version != version {
            return Ok(Replace::Stale);
        }
        let next = version
            .get()
            .checked_add(1)
            .map(RecordVersion::new)
            .ok_or_else(|| PortError::backend("record version overflow"))?;
        record.body = body.to_vec();
        record.version = next;
        record.expires_at = expires_at;
        Ok(Replace::Replaced(next))
    }

    async fn remove(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        now: Timestamp,
    ) -> Result<Remove, PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Records) {
            return Err(PortError::unavailable("expiring records are unavailable"));
        }
        if !state
            .records
            .get(key)
            .is_some_and(|record| now < record.expires_at && record.version == version)
        {
            return Ok(Remove::Stale);
        }
        state.records.remove(key);
        Ok(Remove::Removed)
    }

    async fn purge(&self, now: Timestamp) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.fails(Failure::Records) {
            return Err(PortError::unavailable("expiring records are unavailable"));
        }
        state.records.retain(|_, record| now < record.expires_at);
        Ok(())
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
        Acquisition, CertStatus, ControllerId, ExpiringRecordPort, Insert, IssuedCertRecord,
        LeaderLeasePort, PortError, RecordId, RecordKey, RecordKind, RecordVersion, Remove,
        Renewal, Replace, ScalingAuditEvent, ScalingEventKind, ScalingEventSeq,
        StateRepositoryPort,
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

    crate::database_contract!(|| async {
        Ok::<_, Box<dyn std::error::Error>>(crate::Fixture::new(FakeRepository::default()))
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
    async fn lease_and_record_failure_toggles_fail_exactly_their_operations() -> TestResult {
        let repository = FakeRepository::default();
        let now = Timestamp::from_unix_millis(1);
        let expires = Timestamp::from_unix_millis(10);
        let holder = ControllerId::new("controller");
        let Acquisition::Acquired(lease) = repository.acquire(&holder, now, expires).await? else {
            return Err("new lease was not acquired".into());
        };
        repository.fail_lease(true);
        assert!(matches!(
            repository.acquire(&holder, now, expires).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            repository.renew(&lease, expires).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            repository.release(&lease).await,
            Err(PortError::Unavailable(_))
        ));
        repository.fail_lease(false);
        assert!(matches!(
            repository.renew(&lease, expires).await?,
            Renewal::Renewed(_)
        ));
        repository.release(&lease).await?;

        let key = RecordKey {
            kind: RecordKind::LoginAttempt,
            id: RecordId::new([1; 32]),
        };
        repository.fail_records(true);
        assert!(matches!(
            ExpiringRecordPort::insert(
                &repository,
                &[(key, b"body")],
                RecordVersion::new(1),
                now,
                expires
            )
            .await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            repository.get(&key, now).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            repository
                .replace(&key, RecordVersion::new(1), b"new", now, expires)
                .await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            repository.remove(&key, RecordVersion::new(1), now).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            repository.purge(now).await,
            Err(PortError::Unavailable(_))
        ));
        repository.fail_records(false);
        assert_eq!(
            ExpiringRecordPort::insert(
                &repository,
                &[(key, b"body")],
                RecordVersion::new(1),
                now,
                expires
            )
            .await?,
            Insert::Inserted
        );
        assert!(repository.get(&key, now).await?.is_some());
        assert_eq!(
            repository
                .replace(&key, RecordVersion::new(1), b"new", now, expires)
                .await?,
            Replace::Replaced(RecordVersion::new(2))
        );
        assert_eq!(
            repository.remove(&key, RecordVersion::new(2), now).await?,
            Remove::Removed
        );
        repository.purge(now).await?;
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
