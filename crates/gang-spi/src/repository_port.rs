//! State: the certificate ledger, the principal registry and the scaling audit log.

use gang_clock::Timestamp;
use gang_types::{CertSerial, FleetName, Identity, InstanceId, ScalingCause, WorkerId};

use crate::{PortError, port};

/// Durable records. Fleet state is never stored here.
#[port]
pub trait StateRepositoryPort: Send + Sync {
    /// Records an issued certificate.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the record cannot be written.
    async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError>;

    /// The certificate with `serial`, if one was issued.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the ledger cannot be read.
    async fn certificate(&self, serial: &CertSerial)
    -> Result<Option<IssuedCertRecord>, PortError>;

    /// Marks a certificate revoked at `at` and returns its stored record.
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] if the certificate was never issued; [`PortError`] when the
    /// ledger cannot be written.
    async fn revoke(
        &self,
        serial: &CertSerial,
        at: Timestamp,
    ) -> Result<IssuedCertRecord, PortError>;

    /// Every revoked certificate, for the revocation list.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the records cannot be read.
    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError>;

    /// The certificates issued to `identity` that are not revoked, ordered by issue time and then
    /// serial. Offboarding revokes these. Expiry is not judged here: the port has no clock, so
    /// callers compare [`IssuedCertRecord::expires_at`] themselves.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the records cannot be read.
    async fn certificates_of(
        &self,
        identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError>;

    /// Appends one entry to the scaling audit log and returns its sequence number, which is
    /// greater than that of every event recorded before it.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the record cannot be written.
    async fn record_scaling_event(
        &self,
        event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError>;

    /// Overwrites `into` with the newest `limit` scaling audit events whose sequence number `seq`
    /// satisfies `since < seq < before`, in ascending sequence order, oldest first. The buffer is
    /// reused. Both bounds are exclusive: `since` of `0` is no lower bound (no event precedes
    /// `0`), and `before` of `None` is no upper bound.
    ///
    /// Returns `more`: `true` exactly when events inside the window were left out because of
    /// `limit`. An empty window leaves `into` empty and returns `false`.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the log cannot be read.
    async fn scaling_events(
        &self,
        since: ScalingEventSeq,
        before: Option<ScalingEventSeq>,
        limit: std::num::NonZeroUsize,
        into: &mut Vec<RecordedScalingEvent>,
    ) -> Result<bool, PortError>;

    /// Records that `identity` was seen at `at`. The first observation sets `first_seen`;
    /// `last_seen` becomes `at` and never moves backwards.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the registry cannot be written.
    async fn observe_principal(&self, identity: &Identity, at: Timestamp) -> Result<(), PortError>;

    /// The registry record of `identity`, if the server has seen or revoked it.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the registry cannot be read.
    async fn principal(&self, identity: &Identity) -> Result<Option<PrincipalRecord>, PortError>;

    /// Revokes `identity` at `at`, registering it first if unknown, and returns its stored record.
    /// The first revocation time is retained when the principal is already revoked.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the registry cannot be written.
    async fn revoke_principal(
        &self,
        identity: &Identity,
        at: Timestamp,
    ) -> Result<PrincipalRecord, PortError>;

    /// Clears the revocation of `identity` and returns its stored record.
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] with [`Subject::Principal`](crate::Subject::Principal) if the
    /// registry has no such principal;
    /// [`PortError`] when the registry cannot be written.
    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError>;

    /// Every revoked principal, ordered by revocation time, then provider, then principal.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the registry cannot be read.
    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError>;

    /// Overwrites `into` with at most `limit` registry records, ordered by provider, then
    /// principal, that come strictly after `after` (`None` is the start of the registry). With
    /// `only_revoked`, only revoked records are listed. The buffer is reused.
    ///
    /// Returns `more`: `true` exactly when records beyond `limit` exist after the last one
    /// returned, so a caller resumes from that record's identity. An empty listing leaves `into`
    /// empty and returns `false`.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the registry cannot be read.
    async fn principals(
        &self,
        after: Option<&gang_types::Identity>,
        limit: std::num::NonZeroUsize,
        only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError>;
}

/// A principal the server has seen: who, when first and last, and whether it is revoked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalRecord {
    /// The identity: the provider and the principal it names.
    pub identity: Identity,
    /// When it was first seen.
    pub first_seen: Timestamp,
    /// When it was last seen.
    pub last_seen: Timestamp,
    /// When it was revoked, if it is.
    pub revoked_at: Option<Timestamp>,
}

/// An issued certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedCertRecord {
    /// The certificate's serial number.
    pub serial: CertSerial,
    /// The identity the proof established, issued against the identity provider it names.
    pub identity: Identity,
    /// When it was issued.
    pub issued_at: Timestamp,
    /// When it expires.
    pub expires_at: Timestamp,
    /// Whether it has been revoked.
    pub status: CertStatus,
}

/// Whether a certificate may still be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertStatus {
    /// Valid until it expires.
    Active,
    /// Revoked at the given time.
    Revoked {
        /// When it was revoked.
        at: Timestamp,
    },
}

/// Position of a recorded scaling event. Strictly increasing and assigned by the repository;
/// `0` precedes every event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ScalingEventSeq(u64);

impl ScalingEventSeq {
    /// The sequence number `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The sequence number as an integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A scaling audit event together with its sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedScalingEvent {
    /// Where the event sits in the log.
    pub seq: ScalingEventSeq,
    /// The event itself.
    pub event: ScalingAuditEvent,
}

/// One entry of the scaling audit log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalingAuditEvent {
    /// When it happened.
    pub at: Timestamp,
    /// The fleet it concerns.
    pub fleet: FleetName,
    /// What happened.
    pub kind: ScalingEventKind,
    /// The autoscaler decision behind the event. `None` for events with no autoscaler decision
    /// behind them (worker joins and leaves) and for rows recorded before causes existed.
    pub cause: Option<ScalingCause>,
}

/// What the autoscaler did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingEventKind {
    /// The desired capacity was raised.
    ScaledOut {
        /// Capacity before.
        from: u32,
        /// Capacity requested.
        to: u32,
    },
    /// A worker was told to stop taking actions.
    DrainStarted {
        /// The drained worker.
        worker: WorkerId,
    },
    /// A draining worker was told to take actions again.
    DrainCancelled {
        /// The worker whose drain was cancelled.
        worker: WorkerId,
    },
    /// An instance was terminated.
    Terminated {
        /// The terminated instance.
        instance: InstanceId,
    },
    /// An instance's worker failed soon after its launch, before it could serve; its fleet
    /// relaunches it after a backoff.
    ExitedEarly {
        /// The instance whose worker failed.
        instance: InstanceId,
        /// The worker's own failure record: the end of its log from its last `fail` record.
        failure: String,
    },
    /// A worker connected to the scheduler.
    WorkerRegistered {
        /// The worker that connected.
        worker: WorkerId,
    },
    /// A worker that was connected is no longer.
    WorkerDeregistered {
        /// The worker that disconnected.
        worker: WorkerId,
    },
}
