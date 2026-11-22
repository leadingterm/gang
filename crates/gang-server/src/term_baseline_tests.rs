//! The cache traffic and eviction baseline a controller takes when its leadership term starts,
//! driven through the whole server on an injected clock.

use std::future::pending;
use std::pin::pin;
use std::time::Duration;

use gang_clock::Clock;
use gang_config::{RoleConfig, RolePrincipal};
use gang_spi::{
    Acquisition, ControllerId, ControllerReady, EngineStore, LeaderLeasePort, LeaseCandidate,
    SchedulerSnapshot, StoreKind, Takeover, Traffic,
};
use gang_testkit::{Controller, TestResult};
use gang_types::{NonZeroDuration, PermissionSet, Principal, ProviderName, RoleName};
use gang_wire::State;
use gang_wire::routes::GET_STATE;
use http::StatusCode;
use http_body_util::BodyExt;

use crate::test_support::{api_request, plain_parts, plain_settings};
use crate::test_support_deployment::{Handles, PlainDeployment};
use crate::{Parts, Server, ServerSettings};

/// The farm-wide totals of the recent samples: lookups, hits, uploaded, downloaded, evicted.
type Totals = (u32, u32, u64, u64, u64);

/// A snapshot whose content store has evicted `total` bytes since the engine started.
fn evicted(total: u64) -> SchedulerSnapshot {
    SchedulerSnapshot {
        stores: vec![EngineStore {
            store: StoreKind::Content,
            used: 0,
            budget: 1_000_000,
            evicted: total,
        }],
        ..SchedulerSnapshot::default()
    }
}

/// Settings whose anonymous callers may read the state.
fn readable(settings: ServerSettings) -> ServerSettings {
    ServerSettings {
        roles: vec![RoleConfig {
            name: RoleName::try_new("anonymous-reader").expect("test role name is valid"),
            permissions: PermissionSet::all(),
            principals: vec![RolePrincipal {
                provider: ProviderName::try_new("none").expect("test provider name is valid"),
                principal: Principal::try_new("anonymous").expect("test principal is valid"),
            }],
        }],
        ..settings
    }
}

/// A plaintext deployment whose engine runs before it acquires the lease.
fn fixture() -> (Parts<PlainDeployment>, Handles, ServerSettings) {
    let (parts, _dials, handles) = plain_parts();
    (parts, handles, readable(plain_settings()))
}

/// Holds the lease for another controller until it expires.
async fn lease_held_elsewhere(handles: &Handles) -> TestResult {
    let now = handles.clock.wall();
    let held = handles
        .repository
        .acquire(
            &LeaseCandidate {
                holder: ControllerId::new("other"),
                priority: gang_spi::CandidatePriority::registered(
                    gang_spi::CandidateGeneration::default(),
                ),
            },
            now,
            now,
            now + Duration::from_secs(10),
            Takeover::Observe,
        )
        .await?;
    if !matches!(held, Acquisition::Acquired(_)) {
        return Err("test lease was already held".into());
    }
    Ok(())
}

/// Returns the public API status while a controller is waiting to lead.
async fn follower_status(controller: &Controller) -> StatusCode {
    controller
        .call_api(api_request("GET", GET_STATE.template, None, None))
        .await
        .status()
}

/// The totals of the recent samples the leader serves.
async fn recent_totals(controller: &Controller) -> TestResult<Totals> {
    let response = controller
        .call_api(api_request("GET", GET_STATE.template, None, None))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| -> Box<dyn std::error::Error> { error })?;
    let bytes = body.to_bytes();
    let state: State = serde_json::from_slice(&bytes)?;
    Ok(state.recent.samples.iter().fold(
        (0, 0, 0, 0, 0),
        |(lookups, hits, up, down, evicted), sample| {
            (
                lookups + sample.lookups,
                hits + sample.hits,
                up + sample.uploaded_bytes,
                down + sample.downloaded_bytes,
                evicted + sample.evicted_bytes,
            )
        },
    ))
}

// The baseline excludes traffic and evictions from before the term.
#[tokio::test]
async fn the_first_sample_holds_exactly_the_traffic_and_evictions_of_the_term() -> TestResult {
    let (parts, handles, settings) = fixture();
    lease_held_elsewhere(&handles).await?;
    handles.scheduler.set_snapshot(evicted(500));
    let server = Server::prepare(parts, settings).await?;
    let mut run = pin!(server.run(pending(), ControllerReady::channel().0));
    assert!(futures::poll!(run.as_mut()).is_pending());
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("the engine runs before the lease is taken")?;
    controller.count(Traffic {
        lookups: 40,
        hits: 30,
        uploaded: 2_000,
        downloaded: 1_000,
    });
    handles.clock.advance(Duration::from_hours(3));
    assert!(futures::poll!(run.as_mut()).is_pending());
    controller.count(Traffic {
        lookups: 4,
        hits: 3,
        uploaded: 20,
        downloaded: 10,
    });
    handles.scheduler.set_snapshot(evicted(520));
    handles.clock.advance(Duration::from_secs(1));
    for _ in 0..4 {
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    assert_eq!(recent_totals(&controller).await?, (4, 3, 20, 10, 20));
    Ok(())
}

// Catches an engine baseline faked as zero when the engine cannot be asked at the start of the
// term: the first successful snapshot must become the baseline instead of a spike of every
// eviction made before, and later evictions still count.
#[tokio::test]
async fn an_unreadable_engine_at_the_term_start_baselines_at_the_first_snapshot() -> TestResult {
    let (parts, handles, settings) = fixture();
    lease_held_elsewhere(&handles).await?;
    handles.scheduler.fail_snapshot(true);
    let poll_interval = settings.poll_interval.get();
    let snapshots = handles.scheduler.snapshots();
    let server = Server::prepare(parts, settings).await?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let (ready, readiness) = ControllerReady::channel();
    let run = tokio::spawn(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));
    readiness.await?;
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("the engine runs before the lease is taken")?;
    let next_snapshot = snapshots.get() + 1;
    handles.clock.advance(Duration::from_hours(3));
    snapshots.reached(next_snapshot).await;
    handles.scheduler.set_snapshot(evicted(500));
    handles.scheduler.fail_snapshot(false);
    let next_snapshot = snapshots.get() + 1;
    handles.clock.advance(poll_interval);
    snapshots.reached(next_snapshot).await;
    assert_eq!(
        recent_totals(&controller).await?.4,
        0,
        "no spike of earlier evictions"
    );
    handles.scheduler.set_snapshot(evicted(530));
    let next_snapshot = snapshots.get() + 1;
    handles.clock.advance(poll_interval);
    snapshots.reached(next_snapshot).await;
    assert_eq!(
        recent_totals(&controller).await?.4,
        30,
        "later evictions count"
    );
    stop.send(()).map_err(|()| "server stopped early")?;
    run.await??;
    Ok(())
}

// An engine running before leadership must exclude its earlier evictions.
#[tokio::test]
async fn a_new_term_excludes_existing_evictions() -> TestResult {
    let (parts, _dials, handles) = plain_parts();
    handles.scheduler.set_snapshot(evicted(30));
    let server = Server::prepare(parts, readable(plain_settings())).await?;
    let mut run = pin!(server.run(pending(), ControllerReady::channel().0));
    for _ in 0..4 {
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("the engine runs once the lease is taken")?;
    assert_eq!(recent_totals(&controller).await?.4, 0);
    Ok(())
}

// A successor's observational baseline is passive preparation: the original controller
// must keep renewing and serving until the successor is prepared to request takeover.
#[tokio::test]
async fn the_lease_is_renewed_while_the_term_baseline_is_taken() -> TestResult {
    let (mut parts, mut handles, mut settings) = fixture();
    let (old_parts, _dials, _old_handles) = plain_parts();
    parts.repository = old_parts.repository.clone();
    parts.clock = old_parts.clock.clone();
    handles.repository = parts.repository.clone();
    handles.clock = parts.clock.clone();
    parts.controller = ControllerId::new("baseline-successor");
    let old = Server::prepare(old_parts, plain_settings()).await?;
    let old_ready = old.readiness();
    let mut old = pin!(old.run(pending(), ControllerReady::channel().0));
    assert!(futures::poll!(old.as_mut()).is_pending());
    let renew_interval = settings.leadership.renew_interval.get();
    settings.poll_interval = NonZeroDuration::from_mins(1);
    let release_snapshot = handles.scheduler.hold_snapshot();
    let server = Server::prepare(parts, settings).await?;
    let mut run = pin!(server.run(pending(), ControllerReady::channel().0));
    assert!(futures::poll!(run.as_mut()).is_pending());
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("the engine runs before the lease is taken")?;
    for _ in 0..4 {
        handles.clock.advance(renew_interval);
        assert!(futures::poll!(old.as_mut()).is_pending());
        assert!(
            old_ready.ready(),
            "the original remains healthy during successor preparation"
        );
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    let now = handles.clock.wall();
    let stolen = handles
        .repository
        .acquire(
            &LeaseCandidate {
                holder: ControllerId::new("other"),
                priority: gang_spi::CandidatePriority::registered(
                    gang_spi::CandidateGeneration::default(),
                ),
            },
            now,
            now,
            now + Duration::from_secs(10),
            Takeover::Observe,
        )
        .await?;
    assert!(
        matches!(stolen, Acquisition::Held(_)),
        "original renewals keep its lease past the first expiry while the successor prepares"
    );
    assert_eq!(
        follower_status(&controller).await,
        StatusCode::SERVICE_UNAVAILABLE,
        "the term has not started while its baseline is taken"
    );
    release_snapshot
        .send(())
        .map_err(|()| "the baseline snapshot was not being taken")?;
    assert!(futures::poll!(run.as_mut()).is_pending());
    handles.clock.advance(renew_interval);
    if let std::task::Poll::Ready(result) = futures::poll!(old.as_mut()) {
        result?;
    }
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(
        follower_status(&controller).await,
        StatusCode::OK,
        "requests become available after the term starts"
    );
    Ok(())
}

// A timed-out snapshot must not block leadership or count old evictions.
#[tokio::test]
async fn a_timed_out_term_snapshot_does_not_block_leadership_or_count_old_evictions() -> TestResult
{
    let (parts, handles, mut settings) = fixture();
    settings.poll_interval = NonZeroDuration::from_secs(2);
    handles.scheduler.set_snapshot(evicted(500));
    let release_snapshot = handles.scheduler.hold_snapshot();
    let server = Server::prepare(parts, settings).await?;
    let mut run = pin!(server.run(pending(), ControllerReady::channel().0));
    assert!(futures::poll!(run.as_mut()).is_pending());
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("the engine runs before the lease is taken")?;

    handles.clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(
        follower_status(&controller).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    handles.clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(follower_status(&controller).await, StatusCode::OK);
    assert!(
        release_snapshot.send(()).is_err(),
        "timed-out snapshot was cancelled"
    );

    for _ in 0..4 {
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    assert_eq!(
        recent_totals(&controller).await?.4,
        0,
        "no spike of earlier evictions"
    );
    handles.scheduler.set_snapshot(evicted(530));
    handles.clock.advance(Duration::from_secs(2));
    for _ in 0..4 {
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    assert_eq!(
        recent_totals(&controller).await?.4,
        30,
        "later evictions count"
    );
    Ok(())
}
