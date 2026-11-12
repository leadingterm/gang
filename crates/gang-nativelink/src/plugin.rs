//! The `NativeLink` plugin: runs the embedded scheduler and exposes it through `SchedulerPort`.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use gang_spi::{
    ActionsView, ApiService, ControllerConnections, ControllerEndpoints, ControllerReady,
    ControllerSpec, ListingLimits, Plugin, PluginContext, PortError, RequestGate, SchedulerPort,
    SchedulerSnapshot, Subject, WorkerSpec, WorkerTls,
};
use gang_types::{Url, WorkerId};
use nativelink_scheduler::admin::queued_demand;
use nativelink_util::action_messages::WorkerId as NativeWorkerId;
use tokio::sync::watch;

use crate::config::NativeLinkConfig;
use crate::controller::{self, Handles};
use crate::snapshot;
use crate::worker;

/// The `NativeLink` scheduler: runs the controller and worker in process and reads their state.
///
/// One controller and one worker at a time per process. Workers may run again after returning.
/// The controller's resource sketch snapshot task and the worker's process-wide zombie reaper
/// outlive their initiating calls. The first `globals::init` call decides the digest hash function
/// and raises the open-file soft limit for the whole process.
pub struct NativeLink {
    /// The validated plugin section; source of the endpoints.
    config: NativeLinkConfig,
    /// The worker endpoint, resolved at build and reported by `endpoints()`.
    worker_endpoint: Url,
    /// The live handles of the serving controller; `None` until the controller starts serving
    /// and again after it stops.
    handles: watch::Sender<Option<Arc<Handles>>>,
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
}

impl Plugin for NativeLink {
    type Config = NativeLinkConfig;

    /// Validates the section and builds the plugin. The controller is not started here.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the section fails validation.
    fn build(config: NativeLinkConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        config.validate()?;
        let worker_endpoint = config.resolved_worker_endpoint()?.clone();
        let (handles, _) = watch::channel(None);
        Ok(Self {
            config,
            worker_endpoint,
            handles,
        })
    }
}

impl SchedulerPort for NativeLink {
    /// Reads the connected workers and the queued demand from the serving controller.
    ///
    /// # Errors
    ///
    /// [`PortError::Unavailable`] while the controller is not serving; [`PortError::Backend`]
    /// when the scheduler cannot list its workers or queued operations.
    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        let handles = self.current()?;
        let workers = handles.worker_scheduler.worker_snapshot().await;
        let demand = queued_demand(&*handles.action_scheduler)
            .await
            .map_err(PortError::backend)?;
        let completed = handles.completed.load(Ordering::Relaxed);
        snapshot::fill(&workers, &demand, &handles.fleets, completed, into);
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

    /// Stops (`true`) or resumes (`false`) assigning new actions to a worker.
    ///
    /// An unknown worker is detected by the connected workers list, not by the error code: the
    /// scheduler reports a missing worker with a plain error. When `set_drain_worker` fails, the
    /// worker is looked up in that list; a worker missing from it is [`PortError::NotFound`].
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] with [`Subject::Worker`] when the worker is not connected;
    /// [`PortError::Unavailable`] while the controller is not serving; [`PortError::Backend`]
    /// for any other scheduler failure.
    async fn set_draining(&self, worker: &WorkerId, draining: bool) -> Result<(), PortError> {
        let handles = self.current()?;
        let id = NativeWorkerId::from(worker.as_str().to_owned());
        let Err(error) = handles
            .worker_scheduler
            .set_drain_worker(&id, draining)
            .await
        else {
            return Ok(());
        };
        let connected = handles
            .worker_scheduler
            .worker_snapshot()
            .await
            .iter()
            .any(|summary| summary.id == worker.as_str());
        if connected {
            Err(PortError::backend(error))
        } else {
            Err(PortError::NotFound(Subject::Worker(worker.clone())))
        }
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
        controller::run(&self.config, spec, connections, ready, &self.handles)
            .await
            .map_err(PortError::from)
    }

    /// Runs one worker until stopped, it fails, or its single action finishes.
    /// On stop, no new actions are accepted; running actions and their process groups are killed
    /// and their directories removed before returning. Dropping the future abandons actions.
    ///
    /// # Errors
    ///
    /// [`PortError::Config`] when the inputs cannot form a valid worker;
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
    use std::path::Path;

    use gang_testkit::TestResult;
    use serde_json::{Value, json};

    use super::*;

    /// A valid `[nativelink]` section: plain gRPC worker endpoint on loopback.
    fn section() -> Value {
        json!({
            "worker_endpoint": "grpc://127.0.0.1:9100",
            "scheduler_name": "MAIN",
        })
    }

    /// Builds the plugin from `section` with `gang.toml` as the configuration file.
    fn build(section: Value) -> Result<NativeLink, PortError> {
        let config: NativeLinkConfig =
            serde_json::from_value(section).expect("section has the config shape");
        NativeLink::build(
            config,
            &PluginContext {
                config_path: Path::new("gang.toml"),
            },
        )
    }

    #[test]
    fn endpoints_report_the_resolved_worker_endpoint() {
        let plugin = build(section()).expect("valid section builds");
        let expected: Url = "grpc://127.0.0.1:9100".parse().expect("valid URL");
        assert_eq!(plugin.endpoints().worker, expected);
    }

    #[test]
    fn build_rejects_a_config_without_a_resolved_worker_endpoint() {
        let mut value = section();
        value
            .as_object_mut()
            .expect("section is an object")
            .remove("worker_endpoint");
        assert!(matches!(build(value), Err(PortError::Config(_))));
    }

    #[test]
    fn build_rejects_an_endpoint_that_is_not_grpc() {
        let mut value = section();
        value["worker_endpoint"] = json!("http://127.0.0.1:9100");
        assert!(matches!(build(value), Err(PortError::Config(_))));
    }

    /// Before `run_controller` serves, every query and command reports the controller as
    /// unavailable rather than touching any scheduler state.
    #[tokio::test]
    async fn queries_are_unavailable_until_the_controller_serves() -> TestResult {
        let plugin = build(section()).expect("valid section builds");

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

        let worker = WorkerId::try_new("i-0-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b")?;
        assert!(matches!(
            plugin.set_draining(&worker, true).await,
            Err(PortError::Unavailable(_))
        ));
        Ok(())
    }
}
