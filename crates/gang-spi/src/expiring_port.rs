//! Expiring records: short-lived state that any controller may create and any other may read or
//! finish, such as a pending browser login or a device code. The database stores opaque bodies;
//! their encoding belongs to the caller. The port has no clock: callers pass every time it
//! compares.

use gang_clock::Timestamp;

use crate::counter::counter;
use crate::{PortError, port};

/// A store of versioned blobs that expire.
#[port]
pub trait ExpiringRecordPort: Send + Sync {
    /// Stores every record of `records` at `version` until `expires_at`,
    /// all or none: when any key is repeated in the batch or holds a record live at `now`,
    /// stores nothing and returns [`Insert::Exists`]. A record expired at `now` counts as absent
    /// and is overwritten. Callers must use a fresh version for every insertion attempt.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the records cannot be written.
    async fn insert(
        &self,
        records: &[(RecordKey, &[u8])],
        version: RecordVersion,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Insert, PortError>;

    /// The record stored at `key`, unless there is none or it expired at `now`.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the record cannot be read.
    async fn get(&self, key: &RecordKey, now: Timestamp) -> Result<Option<Record>, PortError>;

    /// Overwrites the record at `key` with `body` until `expires_at` while it is stored at
    /// `version` and live at `now`; the stored version advances by one.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the record cannot be read or written.
    async fn replace(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        body: &[u8],
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Replace, PortError>;

    /// Deletes the record at `key` while it is stored at `version` and live at `now`, so of two
    /// callers that read the same version exactly one removes it.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the record cannot be read or written.
    async fn remove(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        now: Timestamp,
    ) -> Result<Remove, PortError>;

    /// Deletes every record expired at `now`. A backend whose storage deletes expired records by
    /// itself may leave them to it: no read returns them either way.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the records cannot be deleted.
    async fn purge(&self, now: Timestamp) -> Result<(), PortError>;
}

/// What an expiring record holds; each kind has its own key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordKind {
    /// A browser sign-in waiting for its identity provider's callback.
    LoginAttempt,
    /// A device login, keyed by its device code.
    DeviceCode,
    /// The user code that points at a device login.
    UserCode,
    /// A token renewal in flight, which a repeated renewal joins.
    RenewalFlight,
}

impl RecordKind {
    /// Every kind, in declaration order.
    pub const ALL: [Self; 4] = [
        Self::LoginAttempt,
        Self::DeviceCode,
        Self::UserCode,
        Self::RenewalFlight,
    ];

    /// The kind's stable name in storage.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoginAttempt => "login-attempt",
            Self::DeviceCode => "device-code",
            Self::UserCode => "user-code",
            Self::RenewalFlight => "renewal-flight",
        }
    }
}

/// A record's id within its kind: a SHA-256 digest of the secret or token it stands for, so the
/// database never holds the secret itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordId([u8; 32]);

impl RecordId {
    /// The id with digest `digest`.
    #[must_use]
    pub const fn new(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// The digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Where a record lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordKey {
    /// What the record holds.
    pub kind: RecordKind,
    /// Which one.
    pub id: RecordId,
}

counter!(
    /// Identifies a record incarnation and advances by one on every replace.
    RecordVersion
);

impl RecordVersion {
    /// Draws a starting version from the operating system's random source.
    ///
    /// # Errors
    /// Returns an error if operating system randomness is unavailable.
    pub fn fresh() -> Result<Self, getrandom::Error> {
        let mut bytes = [0_u8; 8];
        getrandom::fill(&mut bytes)?;
        Ok(Self::new(
            (u64::from_ne_bytes(bytes) & ((1_u64 << 53) - 1)) + 1,
        ))
    }
}

/// A live record as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The caller's encoded body.
    pub body: Vec<u8>,
    /// The version a later replace or remove must name.
    pub version: RecordVersion,
    /// When the record stops being returned.
    pub expires_at: Timestamp,
}

/// The outcome of [`ExpiringRecordPort::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    /// Every record is stored at the supplied version.
    Inserted,
    /// A key already holds a live record; nothing was stored.
    Exists,
}

/// The outcome of [`ExpiringRecordPort::replace`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replace {
    /// The record now holds the new body at this version.
    Replaced(RecordVersion),
    /// The record is gone, expired, or at another version; nothing was written.
    Stale,
}

/// The outcome of [`ExpiringRecordPort::remove`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Remove {
    /// The caller removed the record.
    Removed,
    /// The record is gone, expired, or at another version; nothing was removed.
    Stale,
}
