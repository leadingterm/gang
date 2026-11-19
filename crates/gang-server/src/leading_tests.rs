//! Leadership and listener hand-off of the composed server.

use std::future::pending;
use std::net::SocketAddr;
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use gang_clock::Clock;
use gang_spi::{
    Acquisition, ControllerHostPort, ControllerId, ControllerReady, HostMilestone, LeaderLeasePort,
    Peer, PortError, SchedulerSnapshot,
};
use gang_testkit::{Side, TestResult};
use gang_types::{FleetSet, Url};
use tokio::sync::oneshot;

use crate::test_support::{api_request, plain_parts, plain_settings, queued_demand};
use crate::test_support_deployment::MemoryBinder;
use crate::{LeaderListeners, Server, ServerError};

const EXECUTION: &str = "127.0.0.1:31001";
const WORKER: &str = "127.0.0.1:31002";

#[tokio::test]
async fn replacement_serves_before_legacy_leader_hands_off() -> TestResult {
    let (parts, dials, handles) = plain_parts();
    let host = parts.provisioner.clone();
    let old_host = gang_testkit::FakeProvisioner::default();
    let mut connections = handles.scheduler.connections();
    let now = handles.clock.wall();
    let Acquisition::Acquired(old_lease) = handles
        .repository
        .acquire(
            &ControllerId::new("v031-8443"),
            now,
            now + Duration::from_mins(1),
        )
        .await?
    else {
        return Err("legacy lease was not acquired".into());
    };
    let mut settings = plain_settings();
    let renew_interval = settings.leadership.renew_interval.get();
    settings.endpoint =
        crate::PublicEndpoint::try_new(Url::parse("http://controller.example.test:443")?)?;
    let (health, health_dial) = crate::test_support_memory::memory_listener();
    let server = Server::prepare(parts, settings)
        .await?
        .with_health(Some(health));
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, mut scheduler_ready) = ControllerReady::channel();
    let mut run = Box::pin(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert!(
        matches!(scheduler_ready.try_recv(), Ok(())),
        "scheduler must start before old lease release"
    );
    let run = tokio::spawn(run);
    host.reached(HostMilestone::Serving).await?;
    assert_eq!(host.milestones(), vec![HostMilestone::Serving]);
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    let _public = dials.public.dial();
    let _worker = dials.worker.dial();
    let first = connections
        .recv()
        .await
        .ok_or("public connection missing")?;
    let second = connections
        .recv()
        .await
        .ok_or("worker connection missing")?;
    assert!(matches!(
        (first.side, second.side),
        (Side::Public, Side::Worker) | (Side::Worker, Side::Public)
    ));
    assert_follower(&controller, &health_dial).await?;
    old_host.reached(HostMilestone::HandedOff).await?;
    assert_eq!(old_host.milestones(), vec![HostMilestone::HandedOff]);
    handles.repository.release(&old_lease).await?;
    handles.clock.advance(renew_interval);
    handles.scheduler.snapshots().reached(1).await;
    assert_promoted(&controller, &health_dial).await?;
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    Ok(())
}

async fn assert_gates(controller: &gang_testkit::Controller, ready: bool) {
    for path in [
        "/build.bazel.remote.execution.v2.Execution/Execute",
        "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
    ] {
        let mut parts = api_request("POST", path, None, None).into_parts().0;
        let result = controller.admit_public(&Peer::default(), &mut parts).await;
        if ready {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(PortError::Unavailable(_))));
        }
    }
    let mut worker = api_request("POST", "/worker", None, None).into_parts().0;
    let result = controller.admit_worker(&Peer::default(), &mut worker).await;
    if ready {
        assert!(result.is_ok());
    } else {
        assert!(matches!(result, Err(PortError::Unavailable(_))));
    }
}

async fn assert_follower(
    controller: &gang_testkit::Controller,
    health_dial: &crate::test_support_memory::MemoryDialer,
) -> TestResult {
    assert_eq!(
        health_status(health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    for path in ["/v1/server", "/ui/"] {
        assert_eq!(
            controller
                .call_api(api_request("GET", path, None, None))
                .await
                .status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert_gates(controller, false).await;
    Ok(())
}

async fn assert_promoted(
    controller: &gang_testkit::Controller,
    health_dial: &crate::test_support_memory::MemoryDialer,
) -> TestResult {
    assert_eq!(health_status(health_dial).await?, http::StatusCode::OK);
    let response = controller
        .call_api(api_request("GET", "/v1/server", None, None))
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let reply = crate::test_support::read(response).await;
    let info: gang_wire::Server<serde_json::Value> = serde_json::from_slice(&reply.body)?;
    assert_eq!(info.remote_executor, info.remote_cache);
    assert_eq!(
        info.remote_cache.as_str(),
        "grpc://controller.example.test:443/"
    );
    assert_gates(controller, true).await;
    Ok(())
}

async fn health_status(
    dialer: &crate::test_support_memory::MemoryDialer,
) -> TestResult<http::StatusCode> {
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(dialer.dial())).await?;
    let driver = tokio::spawn(connection);
    let request = http::Request::builder()
        .uri(gang_types::health::health_check().leader_path())
        .body(http_body_util::Empty::<bytes::Bytes>::new())?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    drop(response);
    drop(sender);
    driver.await??;
    Ok(status)
}

#[tokio::test]
async fn shutdown_admits_existing_connections_during_drain() -> TestResult {
    let (parts, dials, handles) = plain_parts();
    let mut connections = handles.scheduler.connections();
    let drains = handles.scheduler.drains();
    let release_drain = handles.scheduler.hold_drain();
    let (health, health_dial) = crate::test_support_memory::memory_listener();
    let server = Server::prepare(parts, plain_settings())
        .await?
        .with_health(Some(health));
    let (stop, stopped) = oneshot::channel::<()>();
    let mut run = Box::pin(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    let run = tokio::spawn(run);
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    let _public = dials.public.dial();
    let _worker = dials.worker.dial();
    let first = connections.recv().await.ok_or("first connection missing")?;
    let second = connections
        .recv()
        .await
        .ok_or("second connection missing")?;
    let worker_peer = match (first.side, second.side) {
        (Side::Public, Side::Worker) => second.peer,
        (Side::Worker, Side::Public) => first.peer,
        _ => return Err("expected public and worker connections".into()),
    };
    let mut worker = api_request("POST", "/worker", None, None).into_parts().0;
    assert!(
        controller
            .admit_worker(&worker_peer, &mut worker)
            .await
            .is_ok()
    );
    assert_eq!(health_status(&health_dial).await?, http::StatusCode::OK);
    stop.send(()).map_err(|()| "server stopped early")?;
    drains.reached(1).await;
    assert_eq!(
        health_status(&health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    let mut public = api_request("POST", "/execute", None, None).into_parts().0;
    assert!(
        controller
            .admit_public(&Peer::default(), &mut public)
            .await
            .is_ok()
    );
    assert!(
        controller
            .admit_worker(&worker_peer, &mut worker)
            .await
            .is_ok()
    );
    release_drain
        .send(())
        .map_err(|()| "controller was not draining")?;
    run.await??;
    Ok(())
}

#[tokio::test]
async fn one_leads_and_the_follower_takes_over_after_release() -> TestResult {
    let (mut first, _dials, handles) = plain_parts();
    let (mut second, second_dials, other) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("second");
    let mut follower_connections = other.scheduler.connections();
    let binder = MemoryBinder::default();
    let execution: SocketAddr = EXECUTION.parse()?;
    let worker: SocketAddr = WORKER.parse()?;
    first.listeners.leader = LeaderListeners::Dedicated {
        execution,
        worker,
        binder: binder.clone(),
    };
    second.listeners.leader = LeaderListeners::Dedicated {
        execution,
        worker,
        binder: binder.clone(),
    };
    let mut settings = plain_settings();
    settings.execution_endpoint = Some(Url::parse("http://127.0.0.1:31001/")?);
    let first = Server::prepare(first, settings.clone()).await?;
    let second = Server::prepare(second, settings).await?;
    let (stop_first, stopped_first) = oneshot::channel::<()>();
    let (stop_second, stopped_second) = oneshot::channel::<()>();
    let (ready_first, first_ready) = ControllerReady::channel();
    let (ready_second, second_ready) = ControllerReady::channel();
    let mut first = pin!(first.run(
        async {
            let _ = stopped_first.await;
        },
        ready_first
    ));
    let mut second = pin!(second.run(
        async {
            let _ = stopped_second.await;
        },
        ready_second
    ));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert!(first_ready.await.is_ok());
    assert!(second_ready.await.is_ok());
    let _public_connection = second_dials.public.dial();
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert!(
        matches!(follower_connections.try_recv(), Ok(received) if received.side == Side::Public)
    );
    assert!(binder.dialer(execution).is_some());
    assert!(binder.dialer(worker).is_some());
    assert!(handles.scheduler.snapshots().get() > 0);
    assert_eq!(other.scheduler.snapshots().get(), 0);
    stop_first
        .send(())
        .map_err(|()| "first server stopped early")?;
    assert!(matches!(
        futures::poll!(first.as_mut()),
        Poll::Ready(Ok(()))
    ));
    handles.clock.advance(Duration::from_secs(3));
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert!(other.scheduler.snapshots().get() > 0);
    stop_second
        .send(())
        .map_err(|()| "second server stopped early")?;
    assert!(matches!(
        futures::poll!(second.as_mut()),
        Poll::Ready(Ok(()))
    ));
    Ok(())
}

#[tokio::test]
async fn promotion_survives_dashboard_history_read_failure() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let server = Server::prepare(parts, plain_settings()).await?;
    handles.repository.fail_scaling_reads(true);
    let (stop, stopped) = oneshot::channel::<()>();
    let mut run = pin!(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert!(handles.scheduler.snapshots().get() > 0);
    stop.send(()).map_err(|()| "server stopped early")?;
    assert!(matches!(futures::poll!(run.as_mut()), Poll::Ready(Ok(()))));
    Ok(())
}

#[tokio::test]
async fn attachment_retries_without_stopping_the_control_loop() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let fleet = parts.provisioner.clone();
    fleet.fail_attach(2);
    let attempts = fleet.attach_calls();
    let snapshots = handles.scheduler.snapshots();
    let settings = plain_settings();
    let retry_interval = settings.leadership.renew_interval.get();
    let server = Server::prepare(parts, settings).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let mut run = pin!(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(attempts.get(), 1);
    let initial_passes = snapshots.get();
    assert!(initial_passes > 0);
    handles.clock.advance(retry_interval);
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(attempts.get(), 2);
    assert!(snapshots.get() > initial_passes);
    handles.clock.advance(retry_interval);
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(attempts.get(), 3);
    handles.clock.advance(retry_interval);
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(attempts.get(), 3, "a successful attachment is not retried");
    stop.send(()).map_err(|()| "server stopped early")?;
    assert!(matches!(futures::poll!(run.as_mut()), Poll::Ready(Ok(()))));
    Ok(())
}

#[tokio::test]
async fn renewal_failures_do_not_end_the_server() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let fleet = parts.provisioner.clone();
    let snapshots = handles.scheduler.snapshots();
    let (health, health_dial) = crate::test_support_memory::memory_listener();
    let server = Server::prepare(parts, plain_settings())
        .await?
        .with_health(Some(health));
    let (stop, stopped) = oneshot::channel::<()>();
    let mut run = Box::pin(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    let run = tokio::spawn(run);
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    handles.repository.fail_lease(true);
    for _ in 0..4 {
        let next = snapshots.get() + 1;
        handles.clock.advance(Duration::from_secs(3));
        snapshots.reached(next).await;
    }
    assert_eq!(
        health_status(&health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_gates(&controller, true).await;
    let passes = snapshots.get();
    let converges = fleet.converge_requests().len();
    let desired = fleet.desired_requests().len();
    handles.scheduler.set_snapshot(SchedulerSnapshot {
        queued: vec![queued_demand(FleetSet::prefix(1)?)],
        ..SchedulerSnapshot::default()
    });
    handles.clock.advance(Duration::from_secs(1));
    snapshots.reached(passes + 1).await;
    assert_eq!(fleet.desired_requests().len(), desired);
    assert_eq!(
        fleet.converge_requests().len(),
        converges,
        "a lapsed leader converges no fleet"
    );
    handles.repository.fail_lease(false);
    let next = snapshots.get() + 1;
    handles.clock.advance(Duration::from_secs(3));
    snapshots.reached(next).await;
    assert_eq!(health_status(&health_dial).await?, http::StatusCode::OK);
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    Ok(())
}

#[tokio::test]
async fn replaced_lease_ends_the_server_after_drain() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let release_drain = handles.scheduler.hold_drain();
    let server = Server::prepare(parts, plain_settings()).await?;
    let mut run = pin!(server.run(pending(), ControllerReady::channel().0));
    assert!(futures::poll!(run.as_mut()).is_pending());
    handles.clock.advance(Duration::from_secs(20));
    let now = handles.clock.wall();
    let acquired = handles
        .repository
        .acquire(
            &ControllerId::new("replacement"),
            now,
            now + Duration::from_secs(10),
        )
        .await?;
    assert!(matches!(acquired, Acquisition::Acquired(_)));
    assert!(futures::poll!(run.as_mut()).is_pending());
    release_drain
        .send(())
        .map_err(|()| "controller was not draining")?;
    assert!(matches!(
        futures::poll!(run.as_mut()),
        Poll::Ready(Err(ServerError::LeadershipLost))
    ));
    Ok(())
}

#[tokio::test]
async fn on_public_starts_controller_before_acquiring_lease() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let now = handles.clock.wall();
    let held = handles
        .repository
        .acquire(
            &ControllerId::new("other"),
            now,
            now + Duration::from_secs(10),
        )
        .await?;
    let Acquisition::Acquired(lease) = held else {
        return Err("test lease was already held".into());
    };
    let server = Server::prepare(parts, plain_settings()).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, mut readiness) = ControllerReady::channel();
    let mut run = pin!(server.run(
        async {
            let _ = stopped.await;
        },
        ready
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert!(readiness.try_recv().is_ok());
    assert_eq!(handles.scheduler.snapshots().get(), 0);
    handles.repository.release(&lease).await?;
    handles.clock.advance(Duration::from_secs(3));
    assert!(futures::poll!(run.as_mut()).is_pending());
    stop.send(()).map_err(|()| "server stopped early")?;
    assert!(matches!(futures::poll!(run.as_mut()), Poll::Ready(Ok(()))));
    Ok(())
}
