//! Leadership and listener hand-off of the composed server.

use std::future::pending;
use std::time::Duration;

use gang_clock::Clock;
use gang_spi::{
    Acquisition, CandidateGeneration, ControllerHostPort, ControllerId, ControllerReady,
    HostMilestone, LeaderLeasePort, LeaseCandidate, Peer, SchedulerSnapshot, Takeover,
};
use gang_testkit::{Side, TestResult};
use gang_types::{FleetSet, Url};
use tokio::sync::oneshot;

use crate::test_support::{api_request, plain_parts, plain_settings, queued_demand};
use crate::{Server, ServerError};

#[tokio::test]
async fn controller_readiness_precedes_promotion_and_health() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let release_ready = handles.scheduler.hold_ready();
    let (health, health_dial) = crate::test_support_memory::memory_listener();
    let server = Server::prepare(parts, plain_settings())
        .await?
        .with_health(Some(health));
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, mut prepared) = ControllerReady::channel();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    handles
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    assert!(matches!(
        prepared.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(handles.scheduler.snapshots().get(), 0);
    assert_eq!(
        health_status(&health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    release_ready
        .send(())
        .map_err(|()| "controller stopped early")?;
    prepared.await?;
    handles.scheduler.snapshots().reached(1).await;
    assert_eq!(health_status(&health_dial).await?, http::StatusCode::OK);
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_during_initial_preparation_skips_standby() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let _release_ready = handles.scheduler.hold_ready();
    let server = Server::prepare(parts, plain_settings()).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, _prepared) = ControllerReady::channel();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    handles
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    assert_eq!(handles.scheduler.refusal_runs().get(), 0);
    Ok(())
}

#[tokio::test]
async fn same_turn_ready_and_shutdown_starts_standby_while_draining() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let release_ready = handles.scheduler.hold_ready();
    let release_drain = handles.scheduler.hold_drain();
    let server = Server::prepare(parts, plain_settings()).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, _prepared) = ControllerReady::channel();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    handles
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    release_ready
        .send(())
        .map_err(|()| "ready hold was dropped")?;
    stop.send(()).map_err(|()| "server stopped early")?;
    handles.scheduler.drains().reached(1).await;
    handles.scheduler.refusal_runs().reached(1).await;
    assert!(handles.scheduler.standby_services().await.is_some());
    assert!(!run.is_finished());
    release_drain
        .send(())
        .map_err(|()| "drain hold was dropped")?;
    run.await??;
    Ok(())
}

#[tokio::test]
async fn unpromoted_controller_mints_and_authenticates_real_bearer() -> TestResult {
    let (parts, _dials, handles) = crate::test_support::tls_parts();
    handles.verifier.accept("alice-proof", "alice");
    let now = handles.clock.wall();
    let Acquisition::Acquired(old_lease) = handles
        .repository
        .acquire(
            &LeaseCandidate {
                holder: ControllerId::new("old-controller"),
                priority: gang_spi::CandidatePriority::registered(CandidateGeneration::new(0)),
            },
            now,
            now,
            now + Duration::from_mins(1),
            Takeover::Observe,
        )
        .await?
    else {
        return Err("old lease was not acquired".into());
    };
    let server = Server::prepare(parts, crate::test_support::tls_settings()).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, prepared) = ControllerReady::channel();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    prepared.await?;
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    let mut leader = api_request("POST", "/execute", None, None).into_parts().0;
    assert!(matches!(
        controller
            .admit_public(
                gang_spi::RequestScope::Leader,
                &Peer::default(),
                &mut leader
            )
            .await,
        Ok(gang_spi::GateDecision::NotLeading)
    ));
    let token = crate::test_support::mint_token(&controller, "example-idp", "alice-proof").await;
    assert!(crate::test_support::admits(&controller, &token).await);
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    handles.repository.release(&old_lease).await?;
    Ok(())
}

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
            &LeaseCandidate {
                holder: ControllerId::new("v031-8443"),
                priority: gang_spi::CandidatePriority::registered(CandidateGeneration::new(0)),
            },
            now,
            now,
            now + Duration::from_mins(1),
            Takeover::Observe,
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
    let (ready, scheduler_ready) = ControllerReady::channel();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    scheduler_ready.await?;
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
    for (path, scope) in [
        (
            "/build.bazel.remote.execution.v2.Execution/Execute",
            gang_spi::RequestScope::Leader,
        ),
        (
            "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
            gang_spi::RequestScope::Shared,
        ),
    ] {
        let mut parts = api_request("POST", path, None, None).into_parts().0;
        let result = controller
            .admit_public(scope, &Peer::default(), &mut parts)
            .await;
        if ready || scope == gang_spi::RequestScope::Shared {
            assert!(matches!(result, Ok(gang_spi::GateDecision::Admit(_))));
        } else {
            assert!(matches!(result, Ok(gang_spi::GateDecision::NotLeading)));
        }
    }
    let mut worker = api_request("POST", "/worker", None, None).into_parts().0;
    let result = controller
        .admit_worker(
            gang_spi::RequestScope::Leader,
            &Peer::default(),
            &mut worker,
        )
        .await;
    if ready {
        assert!(matches!(result, Ok(gang_spi::GateDecision::Admit(_))));
    } else {
        assert!(matches!(result, Ok(gang_spi::GateDecision::NotLeading)));
    }
}

async fn assert_follower(
    controller: &gang_testkit::Controller,
    health_dial: &crate::test_support_memory::MemoryDialer,
) -> TestResult {
    assert_eq!(health_status(health_dial).await?, http::StatusCode::OK);
    assert_eq!(
        controller
            .call_api(api_request("GET", "/v1/server", None, None))
            .await
            .status(),
        http::StatusCode::OK
    );
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
        .uri(gang_types::health::health_check().ready_path())
        .body(http_body_util::Empty::<bytes::Bytes>::new())?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    drop(response);
    drop(sender);
    driver.await??;
    Ok(status)
}

#[tokio::test]
async fn shutdown_rejects_new_requests_on_existing_connections_during_drain() -> TestResult {
    let (parts, dials, handles) = plain_parts();
    let mut connections = handles.scheduler.connections();
    let drains = handles.scheduler.drains();
    let release_drain = handles.scheduler.hold_drain();
    let (health, health_dial) = crate::test_support_memory::memory_listener();
    let server = Server::prepare(parts, plain_settings())
        .await?
        .with_health(Some(health));
    let (stop, stopped) = oneshot::channel::<()>();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    handles.scheduler.snapshots().reached(1).await;
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
    assert!(matches!(
        controller
            .admit_worker(gang_spi::RequestScope::Leader, &worker_peer, &mut worker)
            .await,
        Ok(gang_spi::GateDecision::Admit(_))
    ));
    assert_eq!(health_status(&health_dial).await?, http::StatusCode::OK);
    stop.send(()).map_err(|()| "server stopped early")?;
    drains.reached(1).await;
    assert_eq!(
        health_status(&health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    let mut public = api_request("POST", "/execute", None, None).into_parts().0;
    assert!(matches!(
        controller
            .admit_public(
                gang_spi::RequestScope::Leader,
                &Peer::default(),
                &mut public
            )
            .await,
        Ok(gang_spi::GateDecision::NotLeading)
    ));
    assert!(matches!(
        controller
            .admit_worker(gang_spi::RequestScope::Leader, &worker_peer, &mut worker)
            .await,
        Ok(gang_spi::GateDecision::NotLeading)
    ));
    let standby = handles
        .scheduler
        .standby_services()
        .await
        .ok_or("standby missing")?;
    let shared = standby
        .call_api(api_request("GET", "/v1/server", None, None))
        .await;
    assert_eq!(shared.status(), http::StatusCode::OK);
    let mut leader = api_request("POST", "/execute", None, None).into_parts().0;
    assert!(matches!(
        standby
            .admit_public(
                gang_spi::RequestScope::Leader,
                &Peer::default(),
                &mut leader
            )
            .await,
        Ok(gang_spi::GateDecision::NotLeading)
    ));
    release_drain
        .send(())
        .map_err(|()| "controller was not draining")?;
    run.await??;
    Ok(())
}

#[tokio::test]
async fn one_leads_and_the_follower_takes_over_after_release() -> TestResult {
    let (first, _dials, handles) = plain_parts();
    let (mut second, second_dials, other) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("second");
    let mut follower_connections = other.scheduler.connections();
    let first = Server::prepare(first, plain_settings()).await?;
    let second = Server::prepare(second, plain_settings()).await?;
    let (stop_first, stopped_first) = oneshot::channel::<()>();
    let (stop_second, stopped_second) = oneshot::channel::<()>();
    let (ready_first, first_ready) = ControllerReady::channel();
    let (ready_second, second_ready) = ControllerReady::channel();
    let first = tokio::spawn(first.run(
        async {
            let _ = stopped_first.await;
        },
        ready_first,
    ));
    first_ready.await?;
    handles.scheduler.snapshots().reached(1).await;
    let second = tokio::spawn(second.run(
        async {
            let _ = stopped_second.await;
        },
        ready_second,
    ));
    second_ready.await?;
    let _public_connection = second_dials.public.dial();
    let _worker_connection = second_dials.worker.dial();
    let first_connection = follower_connections
        .recv()
        .await
        .ok_or("first connection missing")?;
    let second_connection = follower_connections
        .recv()
        .await
        .ok_or("second connection missing")?;
    assert!(matches!(
        (first_connection.side, second_connection.side),
        (Side::Public, Side::Worker) | (Side::Worker, Side::Public)
    ));
    assert!(handles.scheduler.snapshots().get() > 0);
    assert_eq!(other.scheduler.snapshots().get(), 0);
    stop_first
        .send(())
        .map_err(|()| "first server stopped early")?;
    first.await??;
    handles.clock.advance(Duration::from_secs(3));
    other.scheduler.snapshots().reached(1).await;
    stop_second
        .send(())
        .map_err(|()| "second server stopped early")?;
    second.await??;
    Ok(())
}

#[tokio::test]
async fn standby_warms_only_after_serving_milestone_completes() -> TestResult {
    let (first, _dials, handles) = plain_parts();
    let (mut second, _dials, other) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("second");
    let host = second.provisioner.clone();
    let release_serving = host.hold_serving();
    let (first_health, first_dial) = crate::test_support_memory::memory_listener();
    let (second_health, second_dial) = crate::test_support_memory::memory_listener();
    let first = Server::prepare(first, plain_settings())
        .await?
        .with_health(Some(first_health));
    let mut settings = plain_settings();
    settings.standby_routability_delay = Duration::from_secs(3);
    let second = Server::prepare(second, settings)
        .await?
        .with_health(Some(second_health));
    let (stop_first, stopped_first) = oneshot::channel::<()>();
    let (stop_second, stopped_second) = oneshot::channel::<()>();
    let (ready_first, first_ready) = ControllerReady::channel();
    let (ready_second, mut second_ready) = ControllerReady::channel();
    let first = tokio::spawn(first.run(
        async {
            let _ = stopped_first.await;
        },
        ready_first,
    ));
    first_ready.await?;
    handles.scheduler.snapshots().reached(1).await;
    let second = tokio::spawn(second.run(
        async {
            let _ = stopped_second.await;
        },
        ready_second,
    ));
    host.serving_attempts().reached(1).await;
    handles.clock.advance(Duration::from_secs(4));
    assert!(matches!(
        second_ready.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(host.milestones().is_empty());
    assert_eq!(
        health_status(&second_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(health_status(&first_dial).await?, http::StatusCode::OK);
    assert_eq!(handles.scheduler.drains().get(), 0);
    release_serving
        .send(())
        .map_err(|()| "serving milestone stopped early")?;
    second_ready.await?;
    assert_eq!(host.milestones(), vec![HostMilestone::Serving]);
    assert_eq!(health_status(&second_dial).await?, http::StatusCode::OK);
    handles.clock.advance(Duration::from_secs(2));
    assert_eq!(health_status(&first_dial).await?, http::StatusCode::OK);
    assert_eq!(handles.scheduler.drains().get(), 0);
    handles.clock.advance(Duration::from_secs(1));
    assert_eq!(health_status(&second_dial).await?, http::StatusCode::OK);
    handles.clock.advance(Duration::from_secs(3));
    assert_eq!(
        health_status(&first_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    stop_first
        .send(())
        .map_err(|()| "first server stopped early")?;
    stop_second
        .send(())
        .map_err(|()| "second server stopped early")?;
    first.await??;
    second.await??;
    assert_eq!(other.scheduler.controller_runs().get(), 1);
    Ok(())
}

#[tokio::test]
async fn newer_ready_controller_requests_handoff_without_retirement() -> TestResult {
    let (first, _dials, handles) = plain_parts();
    let release_drain = handles.scheduler.hold_drain();
    let (mut second, _dials, other) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("second");
    let (first_health, first_health_dial) = crate::test_support_memory::memory_listener();
    let (second_health, second_health_dial) = crate::test_support_memory::memory_listener();
    let first = Server::prepare(first, plain_settings())
        .await?
        .with_health(Some(first_health));
    let second = Server::prepare(second, plain_settings())
        .await?
        .with_health(Some(second_health));
    let (stop_first, stopped_first) = oneshot::channel::<()>();
    let (stop_second, stopped_second) = oneshot::channel::<()>();
    let (ready_first, first_ready) = ControllerReady::channel();
    let (ready_second, second_ready) = ControllerReady::channel();
    let first = tokio::spawn(first.run(
        async {
            let _ = stopped_first.await;
        },
        ready_first,
    ));
    first_ready.await?;
    handles.scheduler.snapshots().reached(1).await;
    let second = tokio::spawn(second.run(
        async {
            let _ = stopped_second.await;
        },
        ready_second,
    ));
    second_ready.await?;
    let follower = other
        .scheduler
        .controller()
        .await
        .ok_or("controller missing")?;
    assert_follower(&follower, &second_health_dial).await?;
    handles.clock.advance(Duration::from_secs(3));
    assert_eq!(
        health_status(&first_health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    handles.scheduler.drains().reached(1).await;
    handles.clock.advance(Duration::from_secs(3));
    other.scheduler.snapshots().reached(1).await;
    release_drain
        .send(())
        .map_err(|()| "first controller was not draining")?;
    handles.scheduler.controller_runs().reached(2).await;
    assert_eq!(
        health_status(&first_health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    handles.clock.advance(Duration::from_secs(3));
    assert_eq!(
        health_status(&first_health_dial).await?,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    let next_snapshot = handles.scheduler.snapshots().get() + 1;
    stop_second
        .send(())
        .map_err(|()| "second server stopped early")?;
    second.await??;
    handles.clock.advance(Duration::from_secs(3));
    handles.scheduler.snapshots().reached(next_snapshot).await;
    assert_eq!(
        health_status(&first_health_dial).await?,
        http::StatusCode::OK
    );
    stop_first
        .send(())
        .map_err(|()| "first server stopped early")?;
    first.await??;
    Ok(())
}

#[tokio::test]
async fn voluntary_handoff_times_out_when_controller_does_not_drain() -> TestResult {
    let (first, _dials, handles) = plain_parts();
    let (mut second, _dials, other) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("second");
    let _hold_drain = handles.scheduler.hold_drain();
    let settings = plain_settings();
    let timeout = settings.handoff_timeout;
    let first = Server::prepare(first, settings).await?;
    let second = Server::prepare(second, plain_settings()).await?;
    let (ready_first, first_ready) = ControllerReady::channel();
    let (ready_second, second_ready) = ControllerReady::channel();
    let first = tokio::spawn(first.run(pending(), ready_first));
    first_ready.await?;
    handles.scheduler.snapshots().reached(1).await;
    let (stop_second, stopped_second) = oneshot::channel::<()>();
    let second = tokio::spawn(second.run(
        async {
            let _ = stopped_second.await;
        },
        ready_second,
    ));
    second_ready.await?;
    other.scheduler.controller_runs().reached(1).await;
    handles.clock.advance(Duration::from_secs(3));
    handles.scheduler.drains().reached(1).await;
    handles.clock.advance(timeout);
    assert!(matches!(first.await?, Err(ServerError::HandoffTimedOut(_))));
    stop_second
        .send(())
        .map_err(|()| "second server stopped early")?;
    second.await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_cancels_candidate_registration() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let server = Server::prepare(parts, plain_settings()).await?;
    handles.repository.fail_lease(true);
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, mut readiness) = ControllerReady::channel();
    let mut run = Box::pin(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert!(matches!(
        readiness.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_during_voluntary_drain_does_not_start_another_term() -> TestResult {
    let (first, _dials, handles) = plain_parts();
    let (mut second, _dials, other) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("second");
    let release_drain = handles.scheduler.hold_drain();
    let first = Server::prepare(first, plain_settings()).await?;
    let second = Server::prepare(second, plain_settings()).await?;
    let (ready_first, first_ready) = ControllerReady::channel();
    let (ready_second, second_ready) = ControllerReady::channel();
    let (stop_first, stopped_first) = oneshot::channel::<()>();
    let first = tokio::spawn(first.run(
        async {
            let _ = stopped_first.await;
        },
        ready_first,
    ));
    first_ready.await?;
    handles.scheduler.snapshots().reached(1).await;
    let (stop_second, stopped_second) = oneshot::channel::<()>();
    let second = tokio::spawn(second.run(
        async {
            let _ = stopped_second.await;
        },
        ready_second,
    ));
    second_ready.await?;
    other.scheduler.controller_runs().reached(1).await;
    handles.clock.advance(Duration::from_secs(3));
    handles.scheduler.drains().reached(1).await;
    stop_first
        .send(())
        .map_err(|()| "first server stopped early")?;
    release_drain
        .send(())
        .map_err(|()| "first controller was not draining")?;
    first.await??;
    assert_eq!(handles.scheduler.controller_runs().get(), 1);
    stop_second
        .send(())
        .map_err(|()| "second server stopped early")?;
    second.await??;
    Ok(())
}

#[tokio::test]
async fn promotion_survives_dashboard_history_read_failure() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let server = Server::prepare(parts, plain_settings()).await?;
    handles.repository.fail_scaling_reads(true);
    let (stop, stopped) = oneshot::channel::<()>();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    handles.scheduler.snapshots().reached(1).await;
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
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
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    attempts.reached(1).await;
    snapshots.reached(1).await;
    let initial_passes = snapshots.get();
    handles.clock.advance(retry_interval);
    attempts.reached(2).await;
    snapshots.reached(initial_passes + 1).await;
    handles.clock.advance(retry_interval);
    attempts.reached(3).await;
    let next = snapshots.get() + 1;
    handles.clock.advance(retry_interval);
    snapshots.reached(next).await;
    assert_eq!(attempts.get(), 3, "a successful attachment is not retried");
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
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
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));
    snapshots.reached(1).await;
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
    let run = tokio::spawn(server.run(pending(), ControllerReady::channel().0));
    handles.scheduler.snapshots().reached(1).await;
    handles.clock.advance(Duration::from_secs(20));
    let now = handles.clock.wall();
    let acquired = handles
        .repository
        .acquire(
            &LeaseCandidate {
                holder: ControllerId::new("replacement"),
                priority: gang_spi::CandidatePriority::registered(CandidateGeneration::new(0)),
            },
            now,
            now,
            now + Duration::from_secs(10),
            Takeover::Observe,
        )
        .await?;
    assert!(matches!(acquired, Acquisition::Acquired(_)));
    handles.scheduler.drains().reached(1).await;
    release_drain
        .send(())
        .map_err(|()| "controller was not draining")?;
    assert!(matches!(run.await?, Err(ServerError::LeadershipLost)));
    Ok(())
}

#[tokio::test]
async fn on_public_starts_controller_before_acquiring_lease() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let now = handles.clock.wall();
    let held = handles
        .repository
        .acquire(
            &LeaseCandidate {
                holder: ControllerId::new("other"),
                priority: gang_spi::CandidatePriority::registered(CandidateGeneration::new(0)),
            },
            now,
            now,
            now + Duration::from_secs(10),
            Takeover::Observe,
        )
        .await?;
    let Acquisition::Acquired(lease) = held else {
        return Err("test lease was already held".into());
    };
    let server = Server::prepare(parts, plain_settings()).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, readiness) = ControllerReady::channel();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    readiness.await?;
    assert_eq!(handles.scheduler.snapshots().get(), 0);
    handles.repository.release(&lease).await?;
    handles.clock.advance(Duration::from_secs(3));
    handles.scheduler.snapshots().reached(1).await;
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    Ok(())
}
