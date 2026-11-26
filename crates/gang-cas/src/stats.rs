//! Atomic cache metrics and projections to `gang-spi` store statistics.

use std::sync::atomic::{AtomicU64, Ordering};

use gang_spi::{EngineStore, StoreKind, WorkerCacheStats};

/// Atomic counters tracking reads, evictions, used bytes, and budget for a local CAS.
#[derive(Debug, Default)]
pub struct CasStats {
    local_read_requests: AtomicU64,
    local_read_bytes: AtomicU64,
    slow_read_requests: AtomicU64,
    slow_read_bytes: AtomicU64,
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
    size_bytes: AtomicU64,
    budget_bytes: AtomicU64,
}

impl CasStats {
    /// Creates a zeroed [`CasStats`] configured with `budget_bytes`.
    #[must_use]
    pub const fn new(budget_bytes: u64) -> Self {
        Self {
            local_read_requests: AtomicU64::new(0),
            local_read_bytes: AtomicU64::new(0),
            slow_read_requests: AtomicU64::new(0),
            slow_read_bytes: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            evicted_bytes: AtomicU64::new(0),
            size_bytes: AtomicU64::new(0),
            budget_bytes: AtomicU64::new(budget_bytes),
        }
    }

    /// Records a local fast-store read (or direct filesystem materialization) of `bytes`.
    pub fn record_local_read(&self, bytes: u64) {
        self.local_read_requests.fetch_add(1, Ordering::Relaxed);
        self.local_read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a slow-store (or bypass) read of `bytes`.
    pub fn record_slow_read(&self, bytes: u64) {
        self.slow_read_requests.fetch_add(1, Ordering::Relaxed);
        self.slow_read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records `count` evicted entries totaling `bytes` (excluding replacements).
    pub fn record_evictions(&self, count: u64, bytes: u64) {
        if count > 0 {
            self.evictions.fetch_add(count, Ordering::Relaxed);
        }
        if bytes > 0 {
            self.evicted_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Adds `bytes` to the tracked resident size.
    pub fn add_size_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.size_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Subtracts `bytes` from the tracked resident size, saturating at zero.
    pub fn sub_size_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self
            .size_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    /// Replaces the tracked resident size (typically once after startup scan).
    pub fn set_size_bytes(&self, bytes: u64) {
        self.size_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Returns the total local read requests recorded.
    #[must_use]
    pub fn local_read_requests(&self) -> u64 {
        self.local_read_requests.load(Ordering::Relaxed)
    }

    /// Returns the total local read bytes recorded.
    #[must_use]
    pub fn local_read_bytes(&self) -> u64 {
        self.local_read_bytes.load(Ordering::Relaxed)
    }

    /// Returns the total slow read requests recorded.
    #[must_use]
    pub fn slow_read_requests(&self) -> u64 {
        self.slow_read_requests.load(Ordering::Relaxed)
    }

    /// Returns the total slow read bytes recorded.
    #[must_use]
    pub fn slow_read_bytes(&self) -> u64 {
        self.slow_read_bytes.load(Ordering::Relaxed)
    }

    /// Returns the total number of evictions recorded.
    #[must_use]
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Returns the total evicted bytes recorded.
    #[must_use]
    pub fn evicted_bytes(&self) -> u64 {
        self.evicted_bytes.load(Ordering::Relaxed)
    }

    /// Returns the current resident size in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Returns the configured budget in bytes.
    #[must_use]
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes.load(Ordering::Relaxed)
    }

    /// Projects these counters into a [`WorkerCacheStats`] snapshot.
    #[must_use]
    pub fn worker_cache_stats(&self) -> WorkerCacheStats {
        WorkerCacheStats {
            local_read_requests: self.local_read_requests(),
            local_read_bytes: self.local_read_bytes(),
            slow_read_requests: self.slow_read_requests(),
            slow_read_bytes: self.slow_read_bytes(),
            evictions: self.evictions(),
            evicted_bytes: self.evicted_bytes(),
            size_bytes: self.size_bytes(),
            budget_bytes: self.budget_bytes(),
        }
    }

    /// Projects these counters into an [`EngineStore`] snapshot for `store`.
    #[must_use]
    pub fn engine_store(&self, store: StoreKind) -> EngineStore {
        EngineStore {
            store,
            used: self.size_bytes(),
            budget: self.budget_bytes(),
            evicted: self.evicted_bytes(),
        }
    }
}

impl From<&CasStats> for WorkerCacheStats {
    fn from(stats: &CasStats) -> Self {
        stats.worker_cache_stats()
    }
}

#[cfg(test)]
mod tests {
    use gang_spi::{EngineStore, StoreKind, WorkerCacheStats};

    use super::CasStats;

    #[test]
    fn projects_to_worker_cache_stats_and_engine_store() {
        let stats = CasStats::new(4096);
        stats.add_size_bytes(1024);
        stats.record_local_read(256);
        stats.record_local_read(0);
        stats.record_slow_read(512);
        stats.record_evictions(2, 768);
        stats.sub_size_bytes(768);

        assert_eq!(
            stats.worker_cache_stats(),
            WorkerCacheStats {
                local_read_requests: 2,
                local_read_bytes: 256,
                slow_read_requests: 1,
                slow_read_bytes: 512,
                evictions: 2,
                evicted_bytes: 768,
                size_bytes: 256,
                budget_bytes: 4096,
            }
        );

        assert_eq!(
            stats.engine_store(StoreKind::Content),
            EngineStore {
                store: StoreKind::Content,
                used: 256,
                budget: 4096,
                evicted: 768,
            }
        );
    }

    #[test]
    fn sub_size_saturates_at_zero() {
        let stats = CasStats::new(100);
        stats.add_size_bytes(10);
        stats.sub_size_bytes(50);
        assert_eq!(stats.size_bytes(), 0);
    }
}
