//! The leader lease elects the controller that schedules and scales. The port has no clock:
//! callers pass every time it compares.

use std::fmt;

use gang_clock::Timestamp;

use crate::counter::counter;
use crate::{CandidatePriority, ControllerHostIdentity, PortError, port};

/// The lease that elects the leading controller, kept in the database beside the durable records.
#[port]
pub trait LeaderLeasePort: Send + Sync {
    /// Registers a process with its host's restart-stable priority. A persistent host retains
    /// its first database-assigned generation; distinct hosts receive increasing generations.
    /// Allocation gaps are harmless. Launched hosts use their immutable launch metadata.
    /// A process retains its first confirmed candidate across terms. An exclusive `SQLite`
    /// file lock admits only one process and uses registered generation one.
    ///
    /// # Errors
    /// Returns an error if registration fails or the persistent-host counter is exhausted.
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError>;

    /// Takes a never-held or released lease, or one expiring strictly before `stale_before`.
    /// Acquisition increments the epoch and clears any request. Concurrent attempts have at
    /// most one winner; no request steals a live lease or otherwise changes its authority.
    ///
    /// [`Takeover::Observe`] never publishes or refreshes a request. [`Takeover::Request`]
    /// may request that a distinct, lower-priority holder yield, including when its lease has
    /// wall-expired but is not yet stale. The request targets the current epoch, expires at
    /// `expires_at`, and retains the highest fresh candidate priority; the same candidate can
    /// refresh it. Freshness requires the target epoch to match and expiry to be later than `now`.
    ///
    /// A fresh request reserves a free or stale lease for its matching holder and priority.
    /// Other candidates receive [`Acquisition::Reserved`], unless a higher-priority request
    /// overrides the reservation. A live, distinct lower-priority holder yields
    /// [`Acquisition::Contending`]; other blocked acquisitions return [`Acquisition::Held`].
    ///
    /// # Errors
    ///
    /// [`PortError`] when the lease cannot be read or written.
    async fn acquire(
        &self,
        candidate: &LeaseCandidate,
        now: Timestamp,
        stale_before: Timestamp,
        expires_at: Timestamp,
        takeover: Takeover,
    ) -> Result<Acquisition, PortError>;

    /// Extends a matching holder and epoch to `expires_at`, even after expiry, before reporting
    /// either [`Renewal::Renewed`] or [`Renewal::YieldRequested`]. A fresh current-epoch request
    /// from a distinct higher-priority candidate causes the latter. Yield does not revoke
    /// authority: the holder must stop work, revoke and quiesce it, then release the lease.
    /// Only a taking or release ends the holder's lease.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the lease cannot be read or written.
    async fn renew(
        &self,
        lease: &Lease,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Renewal, PortError>;

    /// Clears a matching holder while retaining the epoch and current request reservation; a
    /// later renewal of `lease` is [`Renewal::Lost`]. A mismatched holder or epoch changes nothing.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the lease cannot be read or written.
    async fn release(&self, lease: &Lease) -> Result<(), PortError>;
}

/// Names one controller process for as long as it runs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControllerId(String);

impl ControllerId {
    /// Wraps `id`, which the caller makes unique per controller process.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The id as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ControllerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

counter!(
    /// Counts the takings of the lease: every acquisition stores the next epoch, so a holder's
    /// renewal or release never touches a lease taken after its own.
    LeaseEpoch
);

counter!(
    /// Orders distinct persistent hosts by their first database registration, not their processes.
    CandidateGeneration
);

/// A controller process and its host's restart-stable priority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseCandidate {
    /// Process identity.
    pub holder: ControllerId,
    /// The host's durable registration or launch order.
    pub priority: CandidatePriority,
}

/// Whether a blocked candidate may request a cooperative handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Takeover {
    /// Observe without publishing a request.
    Observe,
    /// Request handoff when eligible.
    Request,
}

/// A held lease, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// The controller that holds it.
    pub holder: ControllerId,
    /// The taking it belongs to.
    pub epoch: LeaseEpoch,
    /// When it expires unless renewed.
    pub expires_at: Timestamp,
}

/// The outcome of [`LeaderLeasePort::acquire`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acquisition {
    /// The caller now holds this lease.
    Acquired(Lease),
    /// Another controller holds this lease, live at `stale_before`.
    Held(Lease),
    /// A live, distinct lower-ranked holder can be asked to yield.
    Contending(Lease),
    /// A fresh request reserves a free or stale lease for another candidate.
    Reserved,
}

/// The outcome of [`LeaderLeasePort::renew`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Renewal {
    /// The lease is still held and now expires later.
    Renewed(Lease),
    /// The lease was renewed and a higher-priority candidate requests handoff.
    YieldRequested(Lease),
    /// Another taking replaced the lease, or it was released: the caller no longer holds it.
    Lost,
}
