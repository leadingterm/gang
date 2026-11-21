//! The leader's cache usage sampler and the publisher that reads it, on an injected clock.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use gang_clock::{Clock, Timestamp};
use gang_config::{BuildsConfig, DashboardConfig};
use gang_spi::{CacheStorePort, MeasuredUsage, PortError, RenderedStoreSpec, StoreAccess, Traffic};
use gang_state::{CacheBaseline, PassStatus};
use gang_testkit::{FakeRepository, TestResult};
use gang_wire::{BucketUsage, State};
use parking_lot::Mutex;

use crate::builds::BuildsCell;
use crate::cache_usage::CacheUsage;
use crate::dashboard::Dashboard;
use crate::leadership::Leading;
use crate::test_support::{fleets, start_clock};

/// A cache whose measurements are scripted; once the script runs out it has none.
struct ScriptedCache {
    answers: Mutex<VecDeque<Result<Option<MeasuredUsage>, PortError>>>,
    fetches: AtomicUsize,
}

impl ScriptedCache {
    fn answering(
        answers: impl IntoIterator<Item = Result<Option<MeasuredUsage>, PortError>>,
    ) -> Self {
        Self {
            answers: Mutex::new(answers.into_iter().collect()),
            fetches: AtomicUsize::new(0),
        }
    }

    fn fetches(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }
}

impl CacheStorePort for ScriptedCache {
    fn content_store(&self) -> Result<RenderedStoreSpec, PortError> {
        Err(PortError::unavailable("not used"))
    }

    fn action_store(
        &self,
        _environment: &gang_types::EnvironmentName,
    ) -> Result<RenderedStoreSpec, PortError> {
        Err(PortError::unavailable("not used"))
    }

    fn worker_access(&self) -> StoreAccess {
        StoreAccess::Direct
    }

    async fn measured_usage(&self) -> Result<Option<MeasuredUsage>, PortError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        self.answers.lock().pop_front().unwrap_or(Ok(None))
    }
}

fn step() -> Duration {
    DashboardConfig::default().history_step.get()
}

fn measured(bytes: u64) -> MeasuredUsage {
    MeasuredUsage {
        bytes,
        objects: bytes / 10,
        measured: Timestamp::from_unix_millis(bytes),
    }
}

// Catches a sampler that fetches on followers, where the measurement is never published and
// every follower would spend provider requests.
#[tokio::test]
async fn followers_never_fetch() {
    let clock = start_clock();
    let cache = ScriptedCache::answering([Ok(Some(measured(1_000)))]);
    let usage = CacheUsage::default();
    let leading = Leading::default();
    let sampler = usage.sample(&cache, &*clock, &leading, step());
    tokio::pin!(sampler);
    for _ in 0..3 {
        assert!(futures::poll!(&mut sampler).is_pending());
        clock.advance(step());
    }
    assert!(futures::poll!(&mut sampler).is_pending());
    assert_eq!(cache.fetches(), 0);
    assert_eq!(usage.get(), None);
}

// Catches a sampler that waits a whole step after promotion before its first fetch, or that
// fetches more often than once per step.
#[tokio::test]
async fn promotion_fetches_at_once_then_every_step() {
    let clock = start_clock();
    let first = measured(1_000);
    let cache = ScriptedCache::answering([Ok(Some(first))]);
    let usage = CacheUsage::default();
    let leading = Leading::default();
    let sampler = usage.sample(&cache, &*clock, &leading, step());
    tokio::pin!(sampler);
    assert!(futures::poll!(&mut sampler).is_pending());
    leading.set(clock.wall());
    assert!(futures::poll!(&mut sampler).is_pending());
    assert_eq!(cache.fetches(), 1);
    assert_eq!(usage.get(), Some(first));
    let almost = step()
        .checked_sub(Duration::from_millis(1))
        .expect("the history step is longer than a millisecond");
    clock.advance(almost);
    assert!(futures::poll!(&mut sampler).is_pending());
    assert_eq!(cache.fetches(), 1);
    clock.advance(Duration::from_millis(1));
    assert!(futures::poll!(&mut sampler).is_pending());
    assert_eq!(cache.fetches(), 2);
}

// Catches a failed or empty fetch clearing the last successful measurement, and a newer
// measurement not replacing it.
#[tokio::test]
async fn failed_fetch_keeps_the_last_measurement() {
    let clock = start_clock();
    let first = measured(1_000);
    let newer = measured(2_000);
    let cache = ScriptedCache::answering([
        Ok(Some(first)),
        Err(PortError::unavailable("metrics are down")),
        Ok(None),
        Ok(Some(newer)),
    ]);
    let usage = CacheUsage::default();
    let leading = Leading::default();
    leading.set(clock.wall());
    let sampler = usage.sample(&cache, &*clock, &leading, step());
    tokio::pin!(sampler);
    assert!(futures::poll!(&mut sampler).is_pending());
    assert_eq!(usage.get(), Some(first));
    for _ in 0..2 {
        clock.advance(step());
        assert!(futures::poll!(&mut sampler).is_pending());
        assert_eq!(usage.get(), Some(first));
    }
    clock.advance(step());
    assert!(futures::poll!(&mut sampler).is_pending());
    assert_eq!(usage.get(), Some(newer));
    assert_eq!(cache.fetches(), 4);
}

// Catches a publisher that reads traffic or the measurement from anywhere but the shared sink
// and the sampler's cell, or reads them once instead of at every pass.
#[tokio::test]
async fn published_state_carries_sink_traffic_and_the_sampled_measurement() -> TestResult {
    let clock = start_clock();
    let repository = FakeRepository::default();
    let dashboard = Dashboard::start(
        DashboardConfig::default(),
        "test".to_owned(),
        Duration::from_secs(1),
        0,
        &clock,
        &repository,
    )
    .await?;
    let (builds, sink, _observations) = BuildsCell::channel(&BuildsConfig::default());
    let mut publisher = dashboard.publisher(builds, sink.clone(), CacheBaseline::default());
    let bucket = measured(5_000);
    let cache = ScriptedCache::answering([Ok(Some(bucket))]);
    let leading = Leading::default();
    leading.set(clock.wall());
    let measuring = dashboard.sample_usage(&cache, &*clock, &leading);
    tokio::pin!(measuring);
    assert!(futures::poll!(&mut measuring).is_pending());
    let fleets = fleets();
    let ok = || PassStatus::Ok {
        applied: 0,
        failed: 0,
    };
    publisher.publish(clock.wall(), ok(), &[], fleets.catalog(), &[]);
    sink.count(Traffic {
        lookups: 3,
        hits: 2,
        uploaded: 10,
        downloaded: 20,
    });
    clock.advance(Duration::from_secs(1));
    publisher.publish(clock.wall(), ok(), &[], fleets.catalog(), &[]);
    let state: State = serde_json::from_slice(&dashboard.api().hub.state_json()?)?;
    assert_eq!(
        state.cache.bucket,
        Some(BucketUsage {
            bytes: bucket.bytes,
            objects: bucket.objects,
            measure_time: bucket.measured,
        })
    );
    assert_eq!(recent_traffic(&state), (3, 2, 10, 20));
    Ok(())
}

/// The lookups, hits, uploaded and downloaded bytes summed over the recent samples of `state`.
fn recent_traffic(state: &State) -> (u32, u32, u64, u64) {
    state
        .recent
        .samples
        .iter()
        .fold((0, 0, 0, 0), |(lookups, hits, up, down), sample| {
            (
                lookups + sample.lookups,
                hits + sample.hits,
                up + sample.uploaded_bytes,
                down + sample.downloaded_bytes,
            )
        })
}

// Catches a promoted leader publishing the traffic it served long before it led as one spike in
// its first sample, and a baseline re-read when the publisher is built, which would swallow the
// traffic of the term counted before that.
#[tokio::test]
async fn a_promoted_publisher_samples_only_traffic_from_its_term() -> TestResult {
    let clock = start_clock();
    let repository = FakeRepository::default();
    let dashboard = Dashboard::start(
        DashboardConfig::default(),
        "test".to_owned(),
        Duration::from_secs(1),
        0,
        &clock,
        &repository,
    )
    .await?;
    let (builds, sink, _observations) = BuildsCell::channel(&BuildsConfig::default());
    sink.count(Traffic {
        lookups: 40,
        hits: 30,
        uploaded: 2_000,
        downloaded: 1_000,
    });
    clock.advance(Duration::from_hours(3));
    let baseline = CacheBaseline {
        traffic: sink.traffic(),
        evicted: Some(0),
    };
    sink.count(Traffic {
        lookups: 4,
        hits: 3,
        uploaded: 20,
        downloaded: 10,
    });
    let mut publisher = dashboard.publisher(builds, sink, baseline);
    let fleets = fleets();
    let ok = || PassStatus::Ok {
        applied: 0,
        failed: 0,
    };
    publisher.publish(clock.wall(), ok(), &[], fleets.catalog(), &[]);
    let state: State = serde_json::from_slice(&dashboard.api().hub.state_json()?)?;
    assert_eq!(
        recent_traffic(&state),
        (4, 3, 20, 10),
        "only the term's traffic is sampled"
    );
    Ok(())
}
