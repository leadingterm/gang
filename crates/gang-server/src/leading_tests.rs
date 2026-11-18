//! Leadership and listener hand-off of the composed server.

use std::future::pending;
use std::net::SocketAddr;
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use gang_clock::Clock;
use gang_spi::{Acquisition, ControllerId, ControllerReady, LeaderLeasePort, SchedulerSnapshot};
use gang_testkit::{Side, TestResult};
use gang_types::{FleetSet, Url};
use tokio::sync::oneshot;

use crate::test_support::{plain_parts, plain_settings, queued_demand};
use crate::test_support_deployment::MemoryBinder;
use crate::{LeaderListeners, Server, ServerError};

const EXECUTION: &str = "127.0.0.1:31001";
const WORKER: &str = "127.0.0.1:31002";

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
    let server = Server::prepare(parts, plain_settings()).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let mut run = pin!(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0
    ));
    assert!(futures::poll!(run.as_mut()).is_pending());
    handles.repository.fail_lease(true);
    for _ in 0..4 {
        handles.clock.advance(Duration::from_secs(3));
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    let passes = snapshots.get();
    let converges = fleet.converge_requests().len();
    let desired = fleet.desired_requests().len();
    handles.scheduler.set_snapshot(SchedulerSnapshot {
        queued: vec![queued_demand(FleetSet::prefix(1)?)],
        ..SchedulerSnapshot::default()
    });
    handles.clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert!(snapshots.get() > passes);
    assert_eq!(fleet.desired_requests().len(), desired);
    assert_eq!(
        fleet.converge_requests().len(),
        converges,
        "a lapsed leader converges no fleet"
    );
    handles.repository.fail_lease(false);
    stop.send(()).map_err(|()| "server stopped early")?;
    assert!(matches!(futures::poll!(run.as_mut()), Poll::Ready(Ok(()))));
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
async fn on_public_waits_for_the_lease_before_serving() -> TestResult {
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
    assert!(matches!(
        readiness.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(handles.scheduler.snapshots().get(), 0);
    handles.repository.release(&lease).await?;
    handles.clock.advance(Duration::from_secs(3));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert!(readiness.try_recv().is_ok());
    stop.send(()).map_err(|()| "server stopped early")?;
    assert!(matches!(futures::poll!(run.as_mut()), Poll::Ready(Ok(()))));
    Ok(())
}
