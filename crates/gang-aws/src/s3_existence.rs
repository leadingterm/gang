//! Bounded positive existence cache for S3 content-addressed blobs.
//!
//! Entries expire at `min(last_modified + consider_expired_after, now + existence_ttl)` (or
//! `now + existence_ttl` when `consider_expired_after` is unset) and are invalidated immediately
//! when a read observes `NotFound` or an expired object.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_types::Digest;
use lru::LruCache;
use parking_lot::Mutex;

use crate::config::{ExpiryAge, S3Tuning};

/// Bounded LRU positive-existence cache keyed by content [`Digest`].
#[derive(Debug)]
pub(crate) struct S3ExistenceCache {
    consider_expired_after: Option<Duration>,
    existence_ttl: Duration,
    entries: Mutex<LruCache<Digest, Timestamp>>,
}

impl S3ExistenceCache {
    /// Builds a cache sized and TTL-bounded by `tuning`.
    pub(crate) fn new(tuning: &S3Tuning) -> Self {
        Self::with_bounds(
            tuning.existence_cache_entries,
            tuning.existence_ttl.get(),
            tuning.consider_expired_after,
        )
    }

    /// Builds a cache with explicit entry capacity, positive TTL, and optional object expiry age.
    pub(crate) fn with_bounds(
        capacity: NonZeroUsize,
        existence_ttl: Duration,
        consider_expired_after: Option<ExpiryAge>,
    ) -> Self {
        Self {
            consider_expired_after: consider_expired_after
                .map(|age| Duration::from_secs(u64::from(age.as_secs()))),
            existence_ttl,
            entries: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Returns whether `digest` has a fresh positive entry at `now`.
    ///
    /// Expired entries are evicted immediately on lookup.
    pub(crate) fn contains_fresh(&self, digest: &Digest, now: Timestamp) -> bool {
        let mut entries = self.entries.lock();
        let Some(&expires_at) = entries.get(digest) else {
            return false;
        };
        if now < expires_at {
            true
        } else {
            entries.pop(digest);
            false
        }
    }

    /// Records a positive `HeadObject` hit at `now` with optional S3 `last_modified` timestamp.
    ///
    /// If the computed expiry is already at or before `now`, any existing entry is removed.
    pub(crate) fn record_head(
        &self,
        digest: Digest,
        last_modified: Option<Timestamp>,
        now: Timestamp,
    ) {
        let expires_at = self.compute_expiry(last_modified, now);
        self.insert_or_evict(digest, expires_at, now);
    }

    /// Records a verified `PutObject` or completed multipart upload at `now`.
    pub(crate) fn record_write(&self, digest: Digest, now: Timestamp) {
        let expires_at = self.compute_expiry(Some(now), now);
        self.insert_or_evict(digest, expires_at, now);
    }

    /// Removes `digest` immediately (for example on `NotFound`, expiry, or corruption).
    pub(crate) fn invalidate(&self, digest: &Digest) {
        self.entries.lock().pop(digest);
    }

    fn insert_or_evict(&self, digest: Digest, expires_at: Timestamp, now: Timestamp) {
        let mut entries = self.entries.lock();
        if now < expires_at {
            entries.put(digest, expires_at);
        } else {
            entries.pop(&digest);
        }
    }

    fn compute_expiry(&self, last_modified: Option<Timestamp>, now: Timestamp) -> Timestamp {
        let ttl_deadline = now + self.existence_ttl;
        match (self.consider_expired_after, last_modified) {
            (Some(age), Some(modified)) => ttl_deadline.min(modified + age),
            _ => ttl_deadline,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn expiry_age(secs: u64) -> Result<ExpiryAge, Box<dyn std::error::Error>> {
        Ok(ExpiryAge::try_from(
            gang_types::NonZeroDuration::from_secs(secs),
        )?)
    }

    #[test]
    fn ttl_expires_entries_when_consider_expired_after_is_unset() {
        let cap = NonZeroUsize::MIN.saturating_add(15);
        let cache = S3ExistenceCache::with_bounds(cap, Duration::from_mins(5), None);
        let digest = Digest::from_bytes(b"ttl-only");
        let t0 = Timestamp::from_unix_millis(1_000_000);

        cache.record_write(digest, t0);
        assert!(cache.contains_fresh(&digest, t0));
        assert!(cache.contains_fresh(
            &digest,
            t0 + Duration::from_mins(5) - Duration::from_millis(1)
        ));
        assert!(!cache.contains_fresh(&digest, t0 + Duration::from_mins(5)));
    }

    #[test]
    fn expiry_is_clamped_to_last_modified_plus_consider_expired_after() -> TestResult {
        let cap = NonZeroUsize::MIN.saturating_add(15);
        let cache =
            S3ExistenceCache::with_bounds(cap, Duration::from_hours(1), Some(expiry_age(600)?));
        let digest = Digest::from_bytes(b"clamped");
        let last_modified = Timestamp::from_unix_millis(1_000_000);
        // Object is already 500s old at head time: remaining age is 100s < 1h TTL.
        let head_now = last_modified + Duration::from_secs(500);

        cache.record_head(digest, Some(last_modified), head_now);
        assert!(cache.contains_fresh(&digest, head_now + Duration::from_secs(99)));
        assert!(!cache.contains_fresh(&digest, head_now + Duration::from_secs(100)));
        Ok(())
    }

    #[test]
    fn expiry_is_clamped_to_now_plus_ttl_when_object_age_is_longer() -> TestResult {
        let cap = NonZeroUsize::MIN.saturating_add(15);
        let cache =
            S3ExistenceCache::with_bounds(cap, Duration::from_mins(1), Some(expiry_age(3_600)?));
        let digest = Digest::from_bytes(b"ttl-shorter");
        let last_modified = Timestamp::from_unix_millis(1_000_000);
        let head_now = last_modified + Duration::from_secs(10);

        cache.record_head(digest, Some(last_modified), head_now);
        assert!(cache.contains_fresh(&digest, head_now + Duration::from_secs(59)));
        assert!(!cache.contains_fresh(&digest, head_now + Duration::from_mins(1)));
        Ok(())
    }

    #[test]
    fn already_expired_head_is_not_cached_and_evicts_prior_entry() -> TestResult {
        let cap = NonZeroUsize::MIN.saturating_add(15);
        let cache =
            S3ExistenceCache::with_bounds(cap, Duration::from_hours(1), Some(expiry_age(60)?));
        let digest = Digest::from_bytes(b"already-expired");
        let t0 = Timestamp::from_unix_millis(1_000_000);

        cache.record_write(digest, t0);
        assert!(cache.contains_fresh(&digest, t0));

        // Recording a stale head removes any prior entry.
        cache.record_head(digest, Some(t0), t0 + Duration::from_mins(1));
        assert!(!cache.contains_fresh(&digest, t0));
        Ok(())
    }

    #[test]
    fn invalidate_removes_entry_and_capacity_evicts_lru() {
        let cap = NonZeroUsize::MIN.saturating_add(1); // capacity = 2
        let cache = S3ExistenceCache::with_bounds(cap, Duration::from_hours(1), None);
        let d1 = Digest::from_bytes(b"d1");
        let d2 = Digest::from_bytes(b"d2");
        let d3 = Digest::from_bytes(b"d3");
        let now = Timestamp::from_unix_millis(1_000_000);

        cache.record_write(d1, now);
        cache.record_write(d2, now);
        // Touch d1 so d2 becomes LRU.
        assert!(cache.contains_fresh(&d1, now));
        cache.record_write(d3, now);
        assert!(cache.contains_fresh(&d1, now));
        assert!(!cache.contains_fresh(&d2, now));
        assert!(cache.contains_fresh(&d3, now));

        cache.invalidate(&d1);
        assert!(!cache.contains_fresh(&d1, now));
    }
}
