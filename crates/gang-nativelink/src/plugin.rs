//! The embedded `NativeLink` scheduler, exposed through `SchedulerPort`.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use parking_lot::Mutex;

use gang_spi::{
    ActionsView, ApiService, ControllerConnections, ControllerDrain, ControllerEndpoints,
    ControllerReady, ControllerSpec, FleetSupply, LearnedHistory, ListingLimits, PortError,
    RequestGate, SchedulerPort, SchedulerSnapshot, WorkerSpec, WorkerTls,
};
use gang_types::{FleetName, InstanceId, Url};
use nativelink_scheduler::resource_sketch::ResourceSketch;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use tokio::sync::watch;

use crate::config::NativeLinkConfig;
use crate::controller::{self, Handles, SharedServices};
use crate::snapshot;
use crate::worker;
// Hyper 1.12.0's HTTP/1 default read-buffer maximum bounds accepted request headers:
// https://docs.rs/hyper/1.12.0/src/hyper/proto/h1/io.rs.html
// Keep this in sync with the pinned Hyper version and its header policy. It also
// conservatively covers Hyper's smaller default HTTP/2 header-list limit.
const MAX_REQUEST_HEADER_BYTES: usize = 8_192 + 4_096 * 100;
const DURATION_JSON_OVERHEAD: usize = 1_024;

/// The `NativeLink` scheduler: runs the controller and worker in process and reads their state.
///
/// Each adapter constructs one original controller engine and retains it and its stores through
/// handoff instead of rebuilding a standby stack or returning to another term.
/// Learned reservations and durations remain available through the final persistence snapshot.
/// Gang's learned-history port owns persistence rather than a native store.
/// The worker's process-wide zombie reaper outlives its initiating call. The first `globals::init`
/// call decides the digest hash function and raises the open-file soft limit for the whole process.
pub struct NativeLink {
    /// Validated role settings; the endpoint is consumed during construction.
    config: NativeLinkConfig,
    /// The worker endpoint, resolved at build and reported by `endpoints()`.
    worker_endpoint: Url,
    /// The live handles of the serving controller; `None` until the controller starts serving
    /// and again after it stops.
    handles: watch::Sender<Option<Arc<Handles>>>,
    /// Learned measurements retained through the final persistence snapshot.
    histories: Mutex<Option<Arc<Histories>>>,
    /// The original controller's resource sketch.
    resources: OnceLock<Arc<ResourceSketch>>,
    /// The original controller's stores.
    shared: SharedServices,
    started: AtomicBool,
}

/// The controller is not serving: `run_controller` has not started serving, or has stopped.
#[derive(Debug, thiserror::Error)]
#[error("the controller is not serving")]
struct NotServing;

impl NativeLink {
    /// The live handles of the serving controller.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] when the controller is not serving.
    fn current(&self) -> Result<Arc<Handles>, PortError> {
        self.handles
            .borrow()
            .clone()
            .ok_or_else(|| PortError::unavailable(NotServing))
    }

    fn histories(&self) -> Result<Arc<Histories>, PortError> {
        self.histories
            .lock()
            .clone()
            .ok_or_else(|| PortError::unavailable(NotServing))
    }
}

impl NativeLink {
    /// Validates the settings and constructs the adapter without starting the controller.
    ///
    /// # Errors
    ///
    /// Returns [`PortError::Config`] if the endpoint is invalid or unresolved.
    pub fn try_new(mut config: NativeLinkConfig) -> Result<Self, PortError> {
        config.validate()?;
        let worker_endpoint = config.take_worker_endpoint()?;
        let (handles, _) = watch::channel(None);
        Ok(Self {
            config,
            worker_endpoint,
            handles,
            histories: Mutex::new(None),
            resources: OnceLock::new(),
            shared: SharedServices::default(),
            started: AtomicBool::new(false),
        })
    }
}

impl SchedulerPort for NativeLink {
    fn learned_history_byte_limit(&self) -> Result<NonZeroUsize, PortError> {
        let history = self.histories()?;
        let resource = usize::try_from(history.resource.snapshot_byte_limit())
            .unwrap_or(usize::MAX)
            .saturating_add(9);
        // RequestMetadata is a header, not part of the gRPC message-size limit.
        // JSON strings can expand to six bytes per input byte when escaped.
        let duration = MAX_REQUEST_HEADER_BYTES * 6 + DURATION_JSON_OVERHEAD;
        Ok(NonZeroUsize::new(resource.max(duration)).unwrap_or(NonZeroUsize::MIN))
    }

    async fn learned_history(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        self.histories()?.snapshot(now, into)
    }

    async fn restore_learned_history(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        self.histories()?.restore(now, records)
    }

    /// Reads the connected workers, the queued demand, and the bounded stores' usage from the
    /// serving controller.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving; [`PortError::Backend`]
    /// when the scheduler cannot list its workers or queued operations.
    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        let handles = self.current()?;
        let mut scratch = handles.scratch.lock().await;
        handles
            .worker_scheduler
            .cordons_into(&mut into.cordons)
            .await;
        let workers = handles.worker_scheduler.worker_snapshot().await;
        let completed = handles.completed.load(Ordering::Relaxed);
        snapshot::fill(
            &*handles.action_scheduler,
            &workers,
            &handles.fleets,
            &handles.durations,
            handles.clock.wall(),
            completed,
            &handles.rates,
            &mut scratch,
            into,
        )
        .await
        .map_err(PortError::backend)?;
        handles.stores.fill(&mut into.stores);
        Ok(())
    }

    /// Lists the running and queued actions of the serving controller, bounded by `limits`.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving; [`PortError::Backend`]
    /// when the scheduler cannot list its actions.
    async fn actions(
        &self,
        limits: ListingLimits,
        into: &mut ActionsView,
    ) -> Result<(), PortError> {
        let handles = self.current()?;
        let workers = handles.worker_scheduler.worker_snapshot().await;
        crate::actions::list(
            &*handles.action_scheduler,
            &workers,
            &handles.fleets,
            limits,
            into,
        )
        .await
        .map_err(PortError::backend)
    }

    /// Stops or resumes every connection of a physical instance, including future connections.
    async fn set_draining(
        &self,
        fleet: &FleetName,
        instance: &InstanceId,
        draining: bool,
    ) -> Result<(), PortError> {
        self.current()?
            .worker_scheduler
            .set_draining(fleet, instance, draining)
            .await
    }

    async fn retain_drains(
        &self,
        now: Tick,
        grace: Duration,
        live: &[(&FleetName, &FleetSupply)],
    ) -> Result<(), PortError> {
        self.current()?
            .worker_scheduler
            .retain_drains(now, grace, live)
            .await;
        Ok(())
    }

    fn endpoints(&self) -> ControllerEndpoints {
        ControllerEndpoints {
            worker: self.worker_endpoint.clone(),
        }
    }

    /// Starts the controller and serves the connections it is handed until the returned future
    /// is dropped.
    /// It sends `ready` once the controller is built and before it serves the first connection;
    /// a run that fails before that drops `ready` unsent.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the inputs cannot form a valid controller;
    /// [`PortError`] when a store cannot be started or the controller fails.
    async fn run_controller<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        spec: &ControllerSpec<'_>,
        connections: ControllerConnections<P, W, A>,
        ready: ControllerReady,
    ) -> Result<(), PortError> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(crate::errors::NativeLinkEmbedError::AlreadyServing.into());
        }
        controller::run(
            &self.config,
            spec,
            connections,
            ready,
            controller::ControllerState {
                handles: &self.handles,
                histories: &self.histories,
                resources: &self.resources,
                shared: &self.shared,
            },
        )
        .await
        .map_err(PortError::from)
    }

    async fn hand_off(&self, into: &mut ControllerDrain) -> Result<(), PortError> {
        let handles = self.current()?;
        handles.worker_scheduler.begin_hand_off().await?;
        handles.worker_scheduler.refresh_drain(into).await
    }

    /// Runs one worker until stopped, it fails, or its single action finishes.
    /// On stop, no new actions are accepted; running actions and their process groups are killed
    /// and their directories removed before returning. Dropping the future abandons actions.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the inputs cannot form a valid worker;
    /// [`PortError::Backend`] when the controller refuses the worker's certificate as revoked;
    /// the worker then never reconnects.
    /// [`PortError`] when the worker cannot be started or fails.
    async fn run_worker(
        &self,
        spec: &WorkerSpec,
        tls: WorkerTls<'_>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError> {
        worker::run(&self.config, &self.worker_endpoint, spec, tls, stop)
            .await
            .map_err(PortError::from)
    }
}

#[cfg(test)]
mod tests {
    use gang_config::WorkerConfig;
    use gang_testkit::TestResult;

    use super::*;

    fn config() -> NativeLinkConfig {
        let endpoint = "grpc://127.0.0.1:9100".parse().expect("valid URL");
        NativeLinkConfig::for_worker(Some(endpoint), &WorkerConfig::default())
    }

    #[tokio::test]
    async fn history_ports_round_trip_metadata_larger_than_grpc_message_limit() {
        use crate::durations::ActionDurations;
        use nativelink_scheduler::resource_sketch::{
            Budget, Observation, Policy, ResourceSketch, SketchKey,
        };
        use std::time::Duration;

        let server: gang_config::ServerConfig = serde_json::from_value(serde_json::json!({
            "transport": "plaintext",
            "http2": { "max_message_size": "1KiB" }
        }))
        .expect("valid server config");
        let endpoint = "grpc://127.0.0.1:9100".parse().expect("valid URL");
        let plugin = NativeLink::try_new(NativeLinkConfig::for_controller(endpoint, &server))
            .expect("valid controller config");
        let sketch = Arc::new(ResourceSketch::with_shards(
            Policy {
                headroom_percent: 0,
                classes: Vec::new(),
                reserve_cpu: true,
                reserve_memory: true,
                reserve_disk: true,
            },
            Budget {
                target: 1,
                mnemonic: 1,
                command: 1,
                probation: 1,
            },
            gang_config::HistoryWindow::from_units(1).get(),
            1,
        ));
        let durations = Arc::new(Mutex::new(ActionDurations::new(
            NonZeroUsize::new(4).expect("nonzero capacity"),
            gang_config::HistoryWindow::from_units(1),
        )));
        let now = Timestamp::from_unix_millis(1_000);
        let target = "x".repeat(100 * 1024);
        durations.lock().record(&target, "compile", 123, now);
        let key = SketchKey::for_attempt(Some("resource-target"), None, "");
        assert!(sketch.observe(
            key.clone(),
            &Observation {
                now_s: 1,
                peak_memory_kb: 123_456,
                ..Observation::default()
            }
        ));
        *plugin.histories.lock() = Some(Arc::new(Histories::new(
            Arc::clone(&sketch),
            Arc::clone(&durations),
        )));
        let limit = plugin
            .learned_history_byte_limit()
            .expect("history is available")
            .get();
        let mut records = Vec::new();
        plugin
            .learned_history(now, &mut records)
            .await
            .expect("snapshot succeeds");
        let largest = records
            .iter()
            .map(|record| record.body.len())
            .max()
            .expect("duration recorded");
        assert!(largest > 100 * 1024);
        assert!(largest <= limit);
        plugin
            .restore_learned_history(now, &[])
            .await
            .expect("clear succeeds");
        assert!(sketch.lookup(&key, 1).is_none());
        assert_eq!(durations.lock().expected(&target, "compile", now), None);
        plugin
            .restore_learned_history(now, &records)
            .await
            .expect("restore succeeds");
        assert_eq!(
            sketch
                .lookup(&key, 1)
                .expect("resource restored")
                .current
                .max_memory_kb,
            123_456
        );
        assert_eq!(
            durations.lock().expected(&target, "compile", now),
            Some(Duration::from_millis(123))
        );
    }

    #[test]
    fn endpoints_report_the_resolved_worker_endpoint() {
        let plugin = NativeLink::try_new(config()).expect("valid config builds");
        let expected: Url = "grpc://127.0.0.1:9100".parse().expect("valid URL");
        assert_eq!(plugin.endpoints().worker, expected);
    }

    #[test]
    fn build_rejects_a_config_without_a_resolved_worker_endpoint() {
        let config = NativeLinkConfig::default();
        assert!(matches!(
            NativeLink::try_new(config),
            Err(PortError::Config(_))
        ));
    }

    #[test]
    fn build_rejects_an_endpoint_that_is_not_grpc() {
        let mut config = config();
        config.worker_endpoint = Some("http://127.0.0.1:9100".parse().expect("valid URL"));
        assert!(matches!(
            NativeLink::try_new(config),
            Err(PortError::Config(_))
        ));
    }

    /// Before `run_controller` serves, every query and command reports the controller as
    /// unavailable rather than touching any scheduler state.
    #[tokio::test]
    async fn queries_are_unavailable_until_the_controller_serves() -> TestResult {
        let plugin = NativeLink::try_new(config()).expect("valid config builds");

        let mut snapshot = SchedulerSnapshot::default();
        assert!(matches!(
            plugin.snapshot(&mut snapshot).await,
            Err(PortError::Unavailable(_))
        ));

        let mut actions = ActionsView::default();
        let limits = ListingLimits {
            running: 1,
            queued: 1,
        };
        assert!(matches!(
            plugin.actions(limits, &mut actions).await,
            Err(PortError::Unavailable(_))
        ));

        let fleet = FleetName::try_new("fleet-a")?;
        let instance = InstanceId::try_new("i-0")?;
        assert!(matches!(
            plugin.set_draining(&fleet, &instance, true).await,
            Err(PortError::Unavailable(_))
        ));
        Ok(())
    }
}
