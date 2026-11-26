//! RAII pin guards protecting [`DiskCas`](crate::DiskCas) entries from eviction.

use std::sync::Arc;

use gang_types::Digest;

use crate::lru::{BlobMode, CacheKey, PinKind, ShardedLru};

/// RAII pin holding a [`DiskCas`](crate::DiskCas) entry against LRU eviction until dropped.
///
/// Used both by in-flight action attempts (`PinKind::Attempt`) and by materialized input trees in
/// `TreeCache` (`PinKind::Tree`).
#[derive(Debug)]
pub struct CasPin {
    lru: Option<Arc<ShardedLru>>,
    key: CacheKey,
    kind: PinKind,
    size: u64,
}

impl CasPin {
    /// Creates an active [`CasPin`] whose pin was already acquired on `lru`.
    #[must_use]
    pub(crate) fn new(lru: Arc<ShardedLru>, key: CacheKey, kind: PinKind, size: u64) -> Self {
        Self {
            lru: Some(lru),
            key,
            kind,
            size,
        }
    }

    /// Creates an inert [`CasPin`] (for [`Digest::EMPTY`]) that performs no unpin on drop.
    #[must_use]
    pub(crate) const fn inert(key: CacheKey, kind: PinKind, size: u64) -> Self {
        Self {
            lru: None,
            key,
            kind,
            size,
        }
    }

    /// Returns the pinned blob's [`Digest`].
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.key.digest
    }

    /// Returns the pinned blob's [`BlobMode`].
    #[must_use]
    pub const fn mode(&self) -> BlobMode {
        self.key.mode
    }

    /// Returns the composite [`CacheKey`] protected by this pin.
    #[must_use]
    pub const fn key(&self) -> CacheKey {
        self.key
    }

    /// Returns the [`PinKind`] held by this guard.
    #[must_use]
    pub const fn kind(&self) -> PinKind {
        self.kind
    }

    /// Returns the logical byte size of the pinned entry.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns `true` if this guard backs a resident [`ShardedLru`] entry.
    #[must_use]
    pub const fn is_resident(&self) -> bool {
        self.lru.is_some()
    }

    /// Explicitly releases the pin immediately.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for CasPin {
    fn drop(&mut self) {
        if let Some(lru) = self.lru.take() {
            lru.unpin(self.key, self.kind);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gang_types::Digest;

    use super::CasPin;
    use crate::lru::{BlobMode, CacheKey, InsertOutcome, PinKind, ShardedLru};

    #[test]
    fn drop_releases_pin_and_allows_eviction() {
        let lru = Arc::new(ShardedLru::new());
        let digest = Digest::from_bytes(b"pinned-payload");
        let key = CacheKey::new(digest, BlobMode::Plain);
        assert_eq!(
            lru.insert_pinned(key, digest.size(), PinKind::Attempt),
            InsertOutcome::Inserted {
                size: digest.size(),
            }
        );

        let pin = CasPin::new(Arc::clone(&lru), key, PinKind::Attempt, digest.size());
        assert!(pin.is_resident());
        assert!(lru.evict_unpinned_until(0).is_empty());

        pin.release();
        assert_eq!(lru.pinned_bytes(), 0);
        assert_eq!(lru.evict_unpinned_until(0).len(), 1);
    }
}
