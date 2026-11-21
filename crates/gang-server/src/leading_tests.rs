//! Leadership and listener hand-off of the composed server.

use std::future::pending;
use std::time::Duration;

use gang_clock::Clock;
use gang_spi::{
    Acquisition, CandidateGeneration, ControllerId, ControllerReady,
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
    assert_eq!(handles.scheduler.controller_runs().get(), 1);
    Ok(())
}


#[tokio::test]
async fn unpromoted_controller_refuses_public_and_worker_store_requests() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    let now = handles.clock.wall();
    let Acquisition::Acquired(old_lease) = handles.repository.acquire(
        &LeaseCandidate {
            holder: ControllerId::new("old-controller"),
            priority: gang_spi::CandidatePriority::registered(CandidateGeneration::new(0)),
        },
        now, now, now + Duration::from_mins(1), Takeover::Observe,
    ).await? else {
        return Err("old lease was not acquired".into());
    };
    let server = Server::prepare(parts, plain_settings()).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, prepared) = ControllerReady::channel();
    let run = tokio::spawn(server.run(async { let _ = stopped.await; }, ready));
    prepared.await?;
    let controller = handles.scheduler.controller().await.ok_or("controller missing")?;
    let mut public = api_request("POST", "/execute", None, None).into_parts().0;
    assert!(matches!(
        controller.admit_public(gang_spi::RequestScope::Controller, &Peer::default(), &mut public).await,
        Ok(gang_spi::GateDecision::NotLeading)
    ));
    let mut store = api_request("POST", "/store", None, None).into_parts().0;
    assert!(matches!(
        controller.admit_worker(gang_spi::RequestScope::WorkerStore, &Peer::default(), &mut store).await,
        Ok(gang_spi::GateDecision::NotLeading)
    ));
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    handles.repository.release(&old_lease).await?;
    Ok(())
}

#[tokio::test]
async fn prepared_milestone_precedes_takeover_and_serving() -> TestResult {
    let (first, _, old) = plain_parts();
    let (mut second, _, new) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("prepared-successor");
    let host = second.provisioner.clone();
    let release_prepared = host.hold_prepared();
    let interval = plain_settings().leadership.renew_interval.get();
    let (old_health, old_dial) = crate::test_support_memory::memory_listener();
    let (new_health, new_dial) = crate::test_support_memory::memory_listener();
    let first = Server::prepare(first, plain_settings()).await?.with_health(Some(old_health));
    let second = Server::prepare(second, plain_settings()).await?.with_health(Some(new_health));
    let (stop_old, stopped_old) = oneshot::channel::<()>();
    let (stop_new, stopped_new) = oneshot::channel::<()>();
    let (ready_old, old_ready) = ControllerReady::channel();
    let (ready_new, new_ready) = ControllerReady::channel();
    let first = tokio::spawn(first.run(async { let _ = stopped_old.await; }, ready_old));
    old_ready.await?;
    old.scheduler.snapshots().reached(1).await;
    let second = tokio::spawn(second.run(async { let _ = stopped_new.await; }, ready_new));
    host.prepared_attempts().reached(1).await;
    assert!(host.milestones().is_empty());
    assert_eq!(new.scheduler.snapshots().get(), 0);
    assert_eq!(health_status(&new_dial).await?, http::StatusCode::SERVICE_UNAVAILABLE);
    old.clock.advance(interval);
    tokio::task::yield_now().await;
    assert!(!first.is_finished(), "unprepared successor caused a handoff");
    assert_eq!(health_status(&old_dial).await?, http::StatusCode::OK);
    assert_eq!(new.scheduler.snapshots().get(), 0);
    release_prepared.send(()).map_err(|()| "prepared milestone stopped early")?;
    new_ready.await?;
    for _ in 0..20 {
        if new.scheduler.snapshots().get() > 0 { break; }
        tokio::task::yield_now().await;
        old.clock.advance(interval);
    }
    assert!(new.scheduler.snapshots().get() > 0, "prepared successor did not promote");
    assert_eq!(health_status(&new_dial).await?, http::StatusCode::OK);
    assert_eq!(host.milestones(), vec![HostMilestone::Prepared, HostMilestone::Serving]);
    first.await??;
    drop(stop_old);
    stop_new.send(()).map_err(|()| "successor stopped early")?;
    second.await??;
    Ok(())
}

#[tokio::test]
async fn replacement_refuses_requests_until_legacy_leader_releases() -> TestResult {
    let (parts, dials, handles) = plain_parts();
    let host = parts.provisioner.clone();
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
    assert_eq!(host.milestones(), vec![HostMilestone::Prepared]);
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
    handles.repository.release(&old_lease).await?;
    handles.clock.advance(renew_interval);
    handles.scheduler.snapshots().reached(1).await;
    assert_promoted(&controller, &health_dial).await?;
    assert_eq!(host.milestones(), vec![HostMilestone::Prepared, HostMilestone::Serving]);
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    Ok(())
}

async fn assert_gates(controller: &gang_testkit::Controller, ready: bool) {
    for (path, scope) in [
        (
            "/build.bazel.remote.execution.v2.Execution/Execute",
            gang_spi::RequestScope::Controller,
        ),
        (
            "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
            gang_spi::RequestScope::Controller,
        ),
    ] {
        let mut parts = api_request("POST", path, None, None).into_parts().0;
        let result = controller
            .admit_public(scope, &Peer::default(), &mut parts)
            .await;
        if ready {
            assert!(matches!(result, Ok(gang_spi::GateDecision::Admit(_))));
        } else {
            assert!(matches!(result, Ok(gang_spi::GateDecision::NotLeading)));
        }
    }
    let mut worker = api_request("POST", "/worker", None, None).into_parts().0;
    let result = controller
        .admit_worker(
            gang_spi::RequestScope::Controller,
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
    assert_eq!(health_status(health_dial).await?, http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        controller
            .call_api(api_request("GET", "/v1/server", None, None))
            .await
            .status(),
        http::StatusCode::SERVICE_UNAVAILABLE
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
async fn newer_controller_promotes_while_original_attempt_drains_without_reacquisition() -> TestResult {
    let (first, _dials, handles) = plain_parts();
    let (mut second, _dials, other) = plain_parts();
    second.repository = first.repository.clone();
    second.clock = first.clock.clone();
    second.controller = ControllerId::new("second");
    let interval = plain_settings().leadership.renew_interval.get();
    let old_host = first.provisioner.clone();
    let (first_health, first_dial) = crate::test_support_memory::memory_listener();
    let (second_health, second_dial) = crate::test_support_memory::memory_listener();
    let first = Server::prepare(first, plain_settings()).await?.with_health(Some(first_health));
    let second = Server::prepare(second, plain_settings()).await?.with_health(Some(second_health));
    let (stop_first, stopped_first) = oneshot::channel::<()>();
    let (stop_second, stopped_second) = oneshot::channel::<()>();
    let (ready_first, first_ready) = ControllerReady::channel();
    let (ready_second, second_ready) = ControllerReady::channel();
    let first = tokio::spawn(first.run(async { let _ = stopped_first.await; }, ready_first));
    first_ready.await?;
    handles.scheduler.snapshots().reached(1).await;
    let original = handles.scheduler.controller().await.ok_or("original controller missing")?;
    assert_eq!(health_status(&first_dial).await?, http::StatusCode::OK);

    let instance = gang_types::InstanceId::try_new("old-worker")?;
    let fleet = gang_types::FleetName::try_new("example-fleet")?;
    let deadline = handles.clock.wall() + Duration::from_secs(120);
    handles.scheduler.set_clock(handles.clock.clone());
    handles.scheduler.set_attempt_deadlines(vec![gang_spi::DrainingWorker {
        fleet: fleet.clone(), instance: instance.clone(), deadline,
    }]);
    handles.scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![gang_spi::WorkerState {
            id: gang_testkit::worker_id(instance.as_str())?,
            fleet,
            environment: Some("host:test".parse()?),
            running_actions: 1,
            max_inflight_actions: None,
            draining: false,
            paused: false,
            last_seen: handles.clock.wall(),
        }],
        executing: vec![gang_spi::ExecutingDemand {
            source: None,
            candidates: FleetSet::prefix(1)?,
            resources: gang_types::ActionResources::default(),
            worker: 0,
            started: handles.clock.wall(),
            expected: None,
        }],
        ..SchedulerSnapshot::default()
    });

    let second = tokio::spawn(second.run(async { let _ = stopped_second.await; }, ready_second));
    second_ready.await?;
    for _ in 0..20 {
        if other.scheduler.snapshots().get() != 0 { break; }
        tokio::task::yield_now().await;
        handles.clock.advance(interval);
    }
    assert!(other.scheduler.snapshots().get() > 0, "new controller did not promote");
    assert!(!first.is_finished(), "original exited with an outstanding stamped attempt");
    assert_eq!(handles.scheduler.controller_runs().get(), 1);
    assert_eq!(health_status(&first_dial).await?, http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(health_status(&second_dial).await?, http::StatusCode::OK);
    let response = original.call_api(api_request("GET", "/v1/server", None, None)).await;
    assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get(gang_types::handover::REQUEST_NOT_STARTED_HEADER).and_then(|v| v.to_str().ok()), Some("true"));
    let mut registration = api_request("POST", "/worker", None, None).into_parts().0;
    assert!(matches!(
        original.admit_worker(gang_spi::RequestScope::Controller, &Peer::default(), &mut registration).await,
        Ok(gang_spi::GateDecision::NotLeading)
    ));
    let mut store = api_request("POST", "/store", None, None).into_parts().0;
    assert!(matches!(
        original.admit_worker(gang_spi::RequestScope::WorkerStore, &Peer::default(), &mut store).await,
        Ok(gang_spi::GateDecision::Admit(_))
    ));
    let mut records = Vec::new();
    gang_spi::ExpiringRecordPort::records(
        &handles.repository, gang_spi::RecordKind::ControllerDrain, None,
        std::num::NonZeroUsize::new(10).ok_or("nonzero page")?,
        handles.clock.wall(), &mut records,
    ).await?;
    assert!(!records.is_empty(), "old worker protection was not installed");
    let protected: serde_json::Value = serde_json::from_slice(&records[0].1.body)?;
    assert_eq!(protected[0]["fleet"], "example-fleet");
    assert_eq!(protected[0]["instance"], "old-worker");
    assert!(!old_host.milestones().contains(&HostMilestone::Unrouted));
    stop_second.send(()).map_err(|()| "new server stopped early")?;
    second.await??;
    stop_first.send(()).map_err(|()| "original exited while still draining")?;
    handles.clock.advance(interval);
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert_eq!(handles.scheduler.controller_runs().get(), 1);
    assert_eq!(health_status(&first_dial).await?, http::StatusCode::SERVICE_UNAVAILABLE);


    handles.scheduler.set_snapshot(SchedulerSnapshot::default());
    for _ in 0..20 {
        if first.is_finished() { break; }
        tokio::task::yield_now().await;
        handles.clock.advance(interval);
    }
    assert!(first.is_finished(), "original did not exit after its attempt completed");
    first.await??;
    assert_eq!(handles.scheduler.controller_runs().get(), 1);
    assert!(old_host.milestones().contains(&HostMilestone::Unrouted));
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
