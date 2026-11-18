//! The leader lease: which controller leads. Every controller serves clients; the one that holds
//! the lease also schedules and scales. The port has no clock: callers pass every time it
//! compares.

use std::fmt;

use gang_clock::Timestamp;

use crate::counter::counter;
use crate::{PortError, port};

/// The lease that elects the leading controller, kept in the database beside the durable records.
#[port]
pub trait LeaderLeasePort: Send + Sync {
    /// Takes the lease for `holder` until `expires_at` when it is free: never taken, released,
    /// or expiring strictly before `stale_before`. Taking it stores the next epoch.
    ///
    /// Of concurrent attempts on a free lease, exactly one is [`Acquisition::Acquired`].
    ///
    /// # Errors
    ///
    /// [`PortError`] when the lease cannot be read or written.
    async fn acquire(
        &self,
        holder: &ControllerId,
        stale_before: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Acquisition, PortError>;

    /// Extends `lease` to `expires_at` while it is stored with the same holder and epoch, even
    /// after it expired: only a taking or a release ends a holder's lease.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the lease cannot be read or written.
    async fn renew(&self, lease: &Lease, expires_at: Timestamp) -> Result<Renewal, PortError>;

    /// Frees `lease` while it is stored with the same holder and epoch: the holder is cleared and
    /// the epoch kept, so a later renewal of `lease` is [`Renewal::Lost`]. Otherwise leaves the
    /// stored lease as it is.
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
}

/// The outcome of [`LeaderLeasePort::renew`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Renewal {
    /// The lease is still held and now expires later.
    Renewed(Lease),
    /// Another taking replaced the lease, or it was released: the caller no longer holds it.
    Lost,
}
