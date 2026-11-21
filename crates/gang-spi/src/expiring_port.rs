//! Expiring records: opaque caller-encoded state such as pending authentication and learned
//! scheduler histories. Authentication records may be created and finished by any controller;
//! history batches require leader authority. The port has no clock: callers pass every time it
//! compares.

use gang_clock::Timestamp;

use crate::counter::counter;
use crate::{Lease, PortError, port};

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

    /// Lists live records of `kind` in ascending ID order, strictly after `after`.
    /// Overwrites and reuses `into`, returning at most `limit` records. Returns `true` exactly
    /// when another matching live record follows the returned page.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the records cannot be read.
    async fn records(
        &self,
        kind: RecordKind,
        after: Option<RecordId>,
        limit: std::num::NonZeroUsize,
        now: Timestamp,
        into: &mut Vec<(RecordId, Record)>,
    ) -> Result<bool, PortError>;

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

    /// Atomically applies a validated batch while the stored lease is held and live at `now`.
    /// `sequence` is strictly increasing within the lease epoch, starting above zero; shared
    /// stores reject stale or repeated sequences as [`ApplyRecords::Fenced`]. `version` is a
    /// fresh record incarnation for this request, stored on every put for subsequent CAS.
    /// Renewal preserves the sequence and a newly acquired epoch resets it. Even an empty batch
    /// checks authority and advances the sequence. Invalid batches fail before any write or
    /// sequence advancement. An exclusive store may instead serialize requests under its
    /// process-wide authority.
    ///
    /// # Errors
    /// Returns a backend error containing [`RecordBatchError`] for an invalid batch, or a
    /// port error if the batch cannot be applied.
    async fn apply(
        &self,
        lease: &Lease,
        sequence: RecordBatchSeq,
        version: RecordVersion,
        now: Timestamp,
        changes: &[RecordChange<'_>],
    ) -> Result<ApplyRecords, PortError>;

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
    /// Learned resource measurements.
    ResourceHistory,
    /// Learned action duration measurements.
    DurationHistory,
    /// A chunk of a learned history payload.
    HistoryChunk,
    /// A provider infrastructure observation retained for the dashboard history window.
    InfrastructureEvent,
    /// Learned per-source activity periods and an optional active-period checkpoint.
    SourceProfile,
    /// Protection of an old controller's draining physical worker.
    ControllerDrain,
}

impl RecordKind {
    /// Every kind, in declaration order.
    pub const ALL: [Self; 10] = [
        Self::LoginAttempt,
        Self::DeviceCode,
        Self::UserCode,
        Self::RenewalFlight,
        Self::ResourceHistory,
        Self::DurationHistory,
        Self::HistoryChunk,
        Self::InfrastructureEvent,
        Self::SourceProfile,
        Self::ControllerDrain,
    ];

    /// The kind's stable name in storage.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoginAttempt => "login-attempt",
            Self::DeviceCode => "device-code",
            Self::UserCode => "user-code",
            Self::RenewalFlight => "renewal-flight",
            Self::ResourceHistory => "resource-history",
            Self::DurationHistory => "duration-history",
            Self::HistoryChunk => "history-chunk",
            Self::InfrastructureEvent => "infrastructure-event",
            Self::SourceProfile => "source-profile",
            Self::ControllerDrain => "controller-drain",
        }
    }
}

/// A record's SHA-256 domain identifier within its kind. Authentication kinds hash secrets or
/// tokens rather than storing those secrets as identifiers; learned kinds hash canonical keys.
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

counter!(
    /// A fenced batch sequence, increasing within a single lease epoch.
    RecordBatchSeq
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

/// Maximum number of record mutations in one fenced batch.
pub const MAX_RECORD_CHANGES: usize = 99;
/// Maximum raw body size of one record in a fenced batch.
pub const MAX_RECORD_BODY_BYTES: usize = 256 * 1024;
/// Maximum sum of raw body bytes in one fenced batch.
pub const MAX_RECORD_BODY_BYTES_PER_BATCH: usize = 1_572_864;

/// One upsert or idempotent deletion in a fenced batch.
#[derive(Debug, Clone, Copy)]
pub enum RecordChange<'a> {
    /// Store an opaque body with its own expiration.
    Put {
        /// The destination record.
        key: RecordKey,
        /// Its opaque encoded payload.
        body: &'a [u8],
        /// Its original expiry.
        expires_at: Timestamp,
    },
    /// Remove the record, whether live or expired.
    Delete {
        /// The destination record.
        key: RecordKey,
    },
}

impl RecordChange<'_> {
    /// The record affected by this change.
    #[must_use]
    pub const fn key(self) -> RecordKey {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Why a fenced batch cannot be submitted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordBatchError {
    /// The number of changes exceeds the portable transaction limit.
    #[error("too many record changes")]
    TooManyChanges,
    /// A record body exceeds the portable item limit.
    #[error("record body too large")]
    BodyTooLarge,
    /// The sum of body bytes exceeds the portable transaction limit.
    #[error("record batch body budget exceeded")]
    BodyBudgetExceeded,
    /// A batch cannot modify the same key twice.
    #[error("duplicate record key")]
    DuplicateKey,
}

/// Validates portable transaction limits and key uniqueness before any write.
///
/// # Errors
/// Returns the first violated batch constraint.
pub fn validate_record_changes(changes: &[RecordChange<'_>]) -> Result<(), RecordBatchError> {
    if changes.len() > MAX_RECORD_CHANGES {
        return Err(RecordBatchError::TooManyChanges);
    }
    let mut bytes = 0_usize;
    for (index, change) in changes.iter().enumerate() {
        if let RecordChange::Put { body, .. } = change {
            if body.len() > MAX_RECORD_BODY_BYTES {
                return Err(RecordBatchError::BodyTooLarge);
            }
            bytes = bytes
                .checked_add(body.len())
                .ok_or(RecordBatchError::BodyBudgetExceeded)?;
            if bytes > MAX_RECORD_BODY_BYTES_PER_BATCH {
                return Err(RecordBatchError::BodyBudgetExceeded);
            }
        }
        if changes[..index]
            .iter()
            .any(|previous| previous.key() == change.key())
        {
            return Err(RecordBatchError::DuplicateKey);
        }
    }
    Ok(())
}

/// Outcome of a fenced record batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyRecords {
    /// All changes were applied atomically.
    Applied,
    /// Authority was not live or the batch sequence was stale; nothing was written. This does
    /// not establish that another leader holds the lease.
    Fenced,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_validation_rejects_all_portable_limits() {
        let key = RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([0; 32]),
        };
        let delete = RecordChange::Delete { key };
        assert_eq!(
            validate_record_changes(&vec![delete; MAX_RECORD_CHANGES + 1]),
            Err(RecordBatchError::TooManyChanges)
        );
        assert_eq!(
            validate_record_changes(&[delete, delete]),
            Err(RecordBatchError::DuplicateKey)
        );
        let body = vec![0; MAX_RECORD_BODY_BYTES + 1];
        let put = RecordChange::Put {
            key,
            body: &body,
            expires_at: Timestamp::UNIX_EPOCH,
        };
        assert_eq!(
            validate_record_changes(&[put]),
            Err(RecordBatchError::BodyTooLarge)
        );
        let changes: Vec<_> = (0..7)
            .map(|id| RecordChange::Put {
                key: RecordKey {
                    kind: RecordKind::ResourceHistory,
                    id: RecordId::new([id; 32]),
                },
                body: &body[..MAX_RECORD_BODY_BYTES],
                expires_at: Timestamp::UNIX_EPOCH,
            })
            .collect();
        assert_eq!(validate_record_changes(&changes[..6]), Ok(()));
        assert_eq!(
            validate_record_changes(&changes),
            Err(RecordBatchError::BodyBudgetExceeded)
        );
    }
}
