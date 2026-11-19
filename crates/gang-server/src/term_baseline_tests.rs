//! The cache traffic and eviction baseline a controller takes when its leadership term starts,
//! driven through the whole server on an injected clock.

use std::future::pending;
use std::net::SocketAddr;
use std::pin::pin;
use std::time::Duration;

use gang_clock::{Clock, TestClock};
use gang_config::{RoleConfig, RolePrincipal};
use gang_mtls::{PROOF_HEADER, mint};
use gang_spi::{
    Acquisition, ControllerId, ControllerReady, EngineStore, LeaderLeasePort, SchedulerSnapshot,
    StoreKind, Traffic,
};
use gang_testkit::{Controller, TestResult, TestSigner};
use gang_types::{NonZeroDuration, PermissionSet, Principal, ProviderName, RoleName, Url};
use gang_wire::State;
use gang_wire::routes::{GET_STATE, INGEST_BUILDS};
use http::StatusCode;
use http_body_util::BodyExt;
use serde_json::json;
use tokio::sync::oneshot;

use crate::test_support::{api_request, plain_parts, plain_settings};
use crate::test_support_deployment::{Handles, MemoryBinder, PlainDeployment};
use crate::{LeaderListeners, Parts, Server, ServerSettings};

const EXECUTION: &str = "127.0.0.1:31001";
const WORKER: &str = "127.0.0.1:31002";
/// The origin of `plain_settings`' endpoint, which issues and checks controller proofs.
const ISSUER: &str = "http://gang.example.test";

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

/// A plaintext deployment with dedicated execution and worker listeners, so its engine runs
/// before the lease is taken, and the release of its held worker bind.
fn dedicated() -> TestResult<(
    Parts<PlainDeployment>,
    Handles,
    ServerSettings,
    oneshot::Sender<()>,
)> {
    let (mut parts, _dials, handles) = plain_parts();
    let binder = MemoryBinder::default();
    let worker: SocketAddr = WORKER.parse()?;
    let release_worker = binder.hold(worker);
    parts.listeners.leader = LeaderListeners::Dedicated {
        execution: EXECUTION.parse()?,
        worker,
        binder,
    };
    let mut settings = readable(plain_settings());
    settings.execution_endpoint = Some(Url::parse("http://127.0.0.1:31001/")?);
    Ok((parts, handles, settings, release_worker))
}

/// Holds the lease for another controller until it expires.
async fn lease_held_elsewhere(handles: &Handles) -> TestResult {
    let now = handles.clock.wall();
    let held = handles
        .repository
        .acquire(
            &ControllerId::new("other"),
            now,
            now + Duration::from_secs(10),
        )
        .await?;
    if !matches!(held, Acquisition::Acquired(_)) {
        return Err("test lease was already held".into());
    }
    Ok(())
}

/// Posts a follower's signed traffic report for a process started now, and returns the status
/// the leader answers with.
async fn follower_reports(
    controller: &Controller,
    clock: &TestClock,
    lookups: u64,
) -> TestResult<StatusCode> {
    let signer = TestSigner::authority()?;
    let now = clock.wall();
    let proof = mint(
        &signer,
        ISSUER,
        &ControllerId::new("follower"),
        now,
        NonZeroDuration::from_secs(10),
    )
    .await?;
    let body = json!({
        "origins": [],
        "calls": [],
        "lost": [],
        "unattributed": 0,
        "traffic": {
            "startTime": now.to_rfc3339(),
            "lookups": lookups,
            "hits": 0,
            "uploadedBytes": 0,
            "downloadedBytes": 0,
        },
    });
    let mut request = api_request("POST", INGEST_BUILDS.template, None, Some(body));
    request
        .headers_mut()
        .insert(PROOF_HEADER, proof.as_str().parse()?);
    let response = controller
        .call_execution_api(request)
        .await
        .ok_or("the leader serves no execution listener")?;
    Ok(response.status())
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

// Catches a baseline taken anywhere but at the start of the term: taken when the publisher is
// built, it swallows the local traffic and follower reports that arrive while the new leader
// still binds its listeners; never taken, the first sample carries hours of traffic and every
// eviction the engine made before this controller led.
#[tokio::test]
async fn the_first_sample_holds_exactly_the_traffic_and_evictions_of_the_term() -> TestResult {
    let (parts, handles, settings, release_worker) = dedicated()?;
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
    let status = follower_reports(&controller, &handles.clock, 5).await?;
    assert_eq!(status, StatusCode::OK);
    handles.scheduler.set_snapshot(evicted(520));
    release_worker
        .send(())
        .map_err(|()| "the worker listener was not being bound")?;
    for _ in 0..4 {
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    assert_eq!(recent_totals(&controller).await?, (9, 3, 20, 10, 20));
    Ok(())
}

// Catches an engine baseline faked as zero when the engine cannot be asked at the start of the
// term: the first successful snapshot must become the baseline instead of a spike of every
// eviction made before, and later evictions still count.
#[tokio::test]
async fn an_unreadable_engine_at_the_term_start_baselines_at_the_first_snapshot() -> TestResult {
    let (parts, handles, settings, release_worker) = dedicated()?;
    lease_held_elsewhere(&handles).await?;
    handles.scheduler.fail_snapshot(true);
    let server = Server::prepare(parts, settings).await?;
    let mut run = pin!(server.run(pending(), ControllerReady::channel().0));
    assert!(futures::poll!(run.as_mut()).is_pending());
    let controller = handles
        .scheduler
        .controller()
        .await
        .ok_or("the engine runs before the lease is taken")?;
    handles.clock.advance(Duration::from_hours(3));
    assert!(futures::poll!(run.as_mut()).is_pending());
    handles.scheduler.set_snapshot(evicted(500));
    handles.scheduler.fail_snapshot(false);
    release_worker
        .send(())
        .map_err(|()| "the worker listener was not being bound")?;
    for _ in 0..4 {
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    assert_eq!(
        recent_totals(&controller).await?.4,
        0,
        "no spike of earlier evictions"
    );
    handles.scheduler.set_snapshot(evicted(530));
    handles.clock.advance(Duration::from_secs(1));
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

// Catches a fresh engine, which starts with the lease already taken, losing the evictions of its
// first pass to a baseline read from it.
#[tokio::test]
async fn a_fresh_engine_counts_every_eviction_from_its_start() -> TestResult {
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
    assert_eq!(recent_totals(&controller).await?.4, 30);
    Ok(())
}

// Catches a term that starts before its baseline is taken (traffic during the wait would be in
// neither the baseline nor the first sample's growth), and a baseline wait that stops the lease
// renewals, which would let another controller take the lease while this one still thinks it
// leads.
#[tokio::test]
async fn the_lease_is_renewed_while_the_term_baseline_is_taken() -> TestResult {
    let (parts, handles, mut settings, _release_worker) = dedicated()?;
    let renew_interval = settings.leadership.renew_interval.get();
    settings.poll_interval = NonZeroDuration::from_secs(60);
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
        assert!(futures::poll!(run.as_mut()).is_pending());
    }
    let now = handles.clock.wall();
    let stolen = handles
        .repository
        .acquire(
            &ControllerId::new("other"),
            now,
            now + Duration::from_secs(10),
        )
        .await?;
    assert!(
        matches!(stolen, Acquisition::Held(_)),
        "renewals keep the lease past its first expiry"
    );
    assert_eq!(
        follower_reports(&controller, &handles.clock, 5).await?,
        StatusCode::SERVICE_UNAVAILABLE,
        "the term has not started while its baseline is taken"
    );
    release_snapshot
        .send(())
        .map_err(|()| "the baseline snapshot was not being taken")?;
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(
        follower_reports(&controller, &handles.clock, 5).await?,
        StatusCode::OK,
        "the term starts once the baseline is taken"
    );
    Ok(())
}

// Removing the timeout leaves ingest unavailable after the deadline; faking a zero baseline
// instead spikes the first sample by all 500 earlier evictions.
#[tokio::test]
async fn a_timed_out_term_snapshot_does_not_block_leadership_or_count_old_evictions() -> TestResult
{
    let (parts, handles, mut settings, release_worker) = dedicated()?;
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
        follower_reports(&controller, &handles.clock, 5).await?,
        StatusCode::SERVICE_UNAVAILABLE
    );
    handles.clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(
        follower_reports(&controller, &handles.clock, 5).await?,
        StatusCode::OK
    );
    assert!(
        release_snapshot.send(()).is_err(),
        "timed-out snapshot was cancelled"
    );

    release_worker
        .send(())
        .map_err(|()| "the worker listener was not being bound")?;
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
