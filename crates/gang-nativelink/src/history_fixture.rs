//! Test-only access to real learned-history collectors.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::HistoryWindow;
use gang_spi::{LearnedHistory, PortError};
use nativelink_config::cas_server::SchedulerConfig;
use nativelink_config::schedulers::{CpuReservationPercentile, SchedulerSpec};
use nativelink_scheduler::historical_resource_scheduler::HistoricalResourceScheduler;
use nativelink_scheduler::resource_sketch::{Budget, Policy, ResourceSketch};
use parking_lot::Mutex;
use serde_json::json;

use crate::durations::ActionDurations;
use crate::history::Histories;

/// A fixture wrapping the production resource and duration collectors.
pub struct HistoryFixture(Histories);

impl HistoryFixture {
    /// Accesses the resource sketch used by the collector.
    #[must_use]
    pub fn resource(&self) -> &ResourceSketch {
        &self.0.resource
    }

    /// Records an action duration in milliseconds.
    pub fn record(&self, target: &str, mnemonic: &str, millis: u64, now: Timestamp) {
        self.0
            .durations
            .lock()
            .record(target, mnemonic, millis, now);
    }

    /// Returns the learned duration for an action.
    #[must_use]
    pub fn expected(&self, target: &str, mnemonic: &str, now: Timestamp) -> Option<Duration> {
        self.0.durations.lock().expected(target, mnemonic, now)
    }

    /// Snapshots the collectors into persisted records.
    ///
    /// # Errors
    /// Returns an error if a collector cannot be snapshotted.
    pub fn snapshot(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        self.0.snapshot(now, into)
    }

    /// Restores the collectors from persisted records.
    ///
    /// # Errors
    /// Returns an error if the resource collector cannot be restored.
    pub fn restore(&self, now: Timestamp, records: &[LearnedHistory]) -> Result<(), PortError> {
        self.0.restore(now, records)
    }
}

/// Creates a collector with three duration keys.
///
/// # Errors
/// Returns an error if the scheduler fixture cannot be constructed.
pub fn source(days: u32) -> Result<HistoryFixture, Box<dyn std::error::Error>> {
    source_with_cap(days, 3)
}

/// Creates a collector with the given duration-key capacity.
///
/// # Errors
/// Returns an error if the scheduler fixture or capacity is invalid.
pub fn source_with_cap(
    days: u32,
    keys: usize,
) -> Result<HistoryFixture, Box<dyn std::error::Error>> {
    let spec: SchedulerConfig = serde_json::from_value(json!({
        "name": "MAIN_SCHEDULER",
        "historical_resource": {
            "learn_from_workers": true,
            "window_days": days,
            "memory_budget_bytes": 256 * 1024 * 1024,
            "headroom_percent": 25,
            "memory_property_name": "memory_kb",
            "disk_property_name": "disk_kb",
            "cpu_property_name": "cpu_millicores",
            "scheduler": { "simple": {} }
        }
    }))?;
    let SchedulerSpec::HistoricalResource(spec) = spec.spec else {
        return Err("expected historical scheduler".into());
    };
    let resource = Arc::new(HistoricalResourceScheduler::sketch_from_spec(&spec));
    let durations = Arc::new(Mutex::new(ActionDurations::new(
        NonZeroUsize::new(keys).ok_or("duration capacity must be nonzero")?,
        HistoryWindow::from_units(u64::from(days)),
    )));
    Ok(HistoryFixture(Histories::new(resource, durations)))
}

/// Creates a collector with a single resource key per tier.
#[must_use]
pub fn tiny_source() -> HistoryFixture {
    let policy = Policy {
        headroom_percent: 25,
        cpu_reservation_percentile: CpuReservationPercentile::P95,
        classes: Vec::new(),
        reserve_cpu: true,
        reserve_memory: true,
        reserve_disk: true,
    };
    let budget = Budget {
        target: 1,
        mnemonic: 1,
        command: 1,
        probation: 1,
    };
    let resource = Arc::new(ResourceSketch::with_shards(
        policy,
        budget,
        HistoryWindow::from_units(2).get(),
        1,
    ));
    let durations = Arc::new(Mutex::new(ActionDurations::new(
        NonZeroUsize::MIN.saturating_add(2),
        HistoryWindow::from_units(2),
    )));
    HistoryFixture(Histories::new(resource, durations))
}
