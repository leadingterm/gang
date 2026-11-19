//! Forwarding and promotion behavior with a manually driven clock.

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_config::BuildsConfig;
use gang_insight::Outcome;
use gang_mtls::ControllerProof;
use gang_spi::{
    Call, CallOutcome, CertificateSignerPort, ControllerId, Observation, Origin, PortError, Rpc,
    SignatureAlgorithm, Traffic, Transferred,
};
use gang_testkit::TestSigner;
use gang_types::{BuildKey, InvocationId, NonZeroDuration};
use gang_wire::{IngestBuildsRequest, IngestTraffic};
use parking_lot::Mutex;

use crate::builds::BuildsCell;
use crate::builds_forward::{Forwarding, ProofSource, Uplink};
use crate::leadership::Leading;
use crate::test_support::identity;

/// When the forwarding process began counting traffic.
const START: Timestamp = Timestamp::from_unix_millis(500);
const PAUSE: Duration = Duration::from_secs(1);

/// An uplink answering with scripted outcomes, then accepting everything.
#[derive(Clone, Default)]
struct FakeUplink(Arc<Mutex<(VecDeque<Outcome>, Vec<IngestBuildsRequest>)>>);

impl FakeUplink {
    fn scripted(outcomes: impl IntoIterator<Item = Outcome>) -> Self {
        let uplink = Self::default();
        uplink.0.lock().0.extend(outcomes);
        uplink
    }

    fn requests(&self) -> Vec<IngestBuildsRequest> {
        self.0.lock().1.clone()
    }
}

impl Uplink for FakeUplink {
    async fn send(&self, _proof: &ControllerProof, body: Bytes) -> Outcome {
        let mut state = self.0.lock();
        state
            .1
            .push(serde_json::from_slice(&body).expect("valid request"));
        state.0.pop_front().unwrap_or(Outcome::Accepted)
    }
}

struct PendingSigner {
    inner: TestSigner,
    entered: AtomicUsize,
}

impl CertificateSignerPort for PendingSigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        self.inner.algorithm()
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        self.inner.public_key().await
    }

    async fn sign(&self, _message: &[u8]) -> Result<Vec<u8>, PortError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}

#[derive(Clone, Default)]
struct PendingUplink(Arc<Mutex<Vec<IngestBuildsRequest>>>);

impl Uplink for PendingUplink {
    async fn send(&self, _proof: &ControllerProof, body: Bytes) -> Outcome {
        self.0
            .lock()
            .push(serde_json::from_slice(&body).expect("valid request"));
        std::future::pending().await
    }
}

fn observation() -> Observation {
    Observation::Call(Call {
        origin: Origin {
            build: BuildKey {
                principal: Arc::new(identity("alice")),
                invocation: InvocationId::try_new("build").expect("valid invocation"),
            },
            correlated: None,
            tool: None,
            target: None,
            mnemonic: None,
        },
        rpc: Rpc::GetActionResult,
        outcome: CallOutcome::Ok,
        transferred: Transferred::default(),
    })
}

fn proofs() -> ProofSource {
    ProofSource::new(
        ControllerId::new("follower"),
        "https://example.com".into(),
        NonZeroDuration::from_secs(10),
    )
}

fn forwarding<U: Uplink>(uplink: U, config: &BuildsConfig) -> Forwarding<U> {
    capped(
        uplink,
        config,
        crate::test_support::tls_settings().limits.max_request_size,
    )
}

fn capped<U: Uplink>(uplink: U, config: &BuildsConfig, max_request_size: usize) -> Forwarding<U> {
    Forwarding::try_new(
        uplink,
        proofs(),
        PAUSE,
        config.max_live,
        max_request_size,
        START,
    )
    .expect("valid outbox")
}

/// The wire form of `totals` for the forwarding process.
fn wire(totals: Traffic) -> IngestTraffic {
    IngestTraffic {
        start_time: START,
        lookups: totals.lookups,
        hits: totals.hits,
        uploaded_bytes: totals.uploaded,
        downloaded_bytes: totals.downloaded,
    }
}

#[tokio::test]
async fn proof_renews_at_half_lifetime() {
    let signer = TestSigner::authority().expect("signer");
    let clock = TestClock::default();
    let mut source = proofs();
    let first = source
        .current(&signer, clock.wall())
        .await
        .expect("proof")
        .as_str()
        .to_owned();
    clock.advance(Duration::from_millis(4999));
    assert_eq!(
        source
            .current(&signer, clock.wall())
            .await
            .expect("cached")
            .as_str(),
        first
    );
    assert_eq!(signer.sign_calls(), 1);
    clock.advance(Duration::from_millis(1));
    assert_ne!(
        source
            .current(&signer, clock.wall())
            .await
            .expect("renewed")
            .as_str(),
        first
    );
    assert_eq!(signer.sign_calls(), 2);
}

// Catches a follower that waits a pause (or two) before its first batch, so a leader would only
// learn the series after the follower had already counted traffic, and a series start taken from
// the dispatch time instead of the process start.
#[tokio::test]
async fn startup_sends_zero_traffic_at_once() {
    let config = BuildsConfig::default();
    let (builds, _sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::new(Timestamp::from_unix_millis(1000));
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::default();
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    let requests = uplink.requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].calls.is_empty());
    assert_eq!(requests[0].traffic, Some(wire(Traffic::default())));
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(
        uplink.requests().len(),
        1,
        "acknowledged totals are not resent"
    );
}

// Catches traffic sampled only when calls arrive, a change that needs two pauses to leave, and
// acknowledged totals resent every pause.
#[tokio::test]
async fn traffic_without_calls_reaches_the_leader_within_one_pause() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::default();
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    let totals = Traffic {
        lookups: 2,
        hits: 1,
        uploaded: 10,
        downloaded: 20,
    };
    sink.count(totals);
    let almost = PAUSE
        .checked_sub(Duration::from_millis(1))
        .expect("the pause is longer than a millisecond");
    clock.advance(almost);
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(uplink.requests().len(), 1);
    clock.advance(Duration::from_millis(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    let requests = uplink.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].calls.is_empty());
    assert_eq!(requests[1].traffic, Some(wire(totals)));
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(uplink.requests().len(), 2);
}

// Catches traffic acknowledged by an ambiguous send (the newer totals would never leave) and a
// retry that resends the stale totals instead of the newest ones.
#[tokio::test]
async fn ambiguous_traffic_resends_the_newest_totals() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::scripted([Outcome::Accepted, Outcome::Unknown]);
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    let first = Traffic {
        lookups: 5,
        ..Traffic::default()
    };
    sink.count(first);
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(uplink.requests()[1].traffic, Some(wire(first)));
    sink.count(Traffic {
        lookups: 2,
        ..Traffic::default()
    });
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    let requests = uplink.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[2].traffic,
        Some(wire(Traffic {
            lookups: 7,
            ..Traffic::default()
        }))
    );
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(uplink.requests().len(), 3);
}

// Catches the follower handing the outbox a cap that ignores the traffic part, so a full batch
// with the largest totals would exceed the leader's request limit.
#[tokio::test]
async fn full_batches_with_maximum_traffic_stay_within_the_request_cap() {
    const CAP: usize = 2048;
    const CALLS: usize = 60;
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::default();
    let maximum = Traffic {
        lookups: u64::MAX,
        hits: u64::MAX,
        uploaded: u64::MAX,
        downloaded: u64::MAX,
    };
    sink.count(maximum);
    for _ in 0..CALLS {
        sink.record(observation());
    }
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(capped(uplink.clone(), &config, CAP)),
    );
    tokio::pin!(recorder);
    for _ in 0..CALLS {
        assert!(futures::poll!(&mut recorder).is_pending());
        clock.advance(PAUSE);
    }
    let requests = uplink.requests();
    assert!(requests.len() > 1, "the cap splits the calls");
    for request in &requests {
        let size = serde_json::to_vec(request).expect("request encodes").len();
        assert!(size <= CAP, "{size} bytes exceed the {CAP}-byte cap");
        assert_eq!(request.traffic, Some(wire(maximum)));
    }
    let forwarded: usize = requests.iter().map(|request| request.calls.len()).sum();
    assert_eq!(forwarded, CALLS);
}

#[tokio::test]
async fn failed_calls_become_losses_and_ambiguous_carried_losses_are_not_retried() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::scripted([Outcome::Unknown, Outcome::Unknown, Outcome::Accepted]);
    let obs = observation();
    sink.lose(&obs.origin().build, NonZeroU64::new(3).expect("nonzero"));
    sink.record(obs.clone());
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    sink.record(obs);
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    let requests = uplink.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].calls.len(), 1);
    assert_eq!(requests[0].lost[0].count, 3);
    assert!(requests[1].calls.is_empty());
    assert_eq!(requests[1].lost[0].count, 1);
    assert!(requests[2].lost.is_empty());
    assert_eq!(requests[2].calls.len(), 1);
    assert!(
        requests
            .iter()
            .all(|request| request.traffic == Some(wire(Traffic::default())))
    );
}

// Also catches promotion folding the forwarded traffic into the sink a second time.
#[tokio::test]
async fn promotion_folds_pending_once_and_records_later_observations_locally() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::new(Timestamp::from_unix_millis(1000));
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::default();
    let obs = observation();
    let id = obs.origin().build.id();
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    let counted = Traffic {
        lookups: 3,
        hits: 1,
        ..Traffic::default()
    };
    sink.count(counted);
    sink.record(obs.clone());
    sink.lose(&obs.origin().build, NonZeroU64::new(2).expect("nonzero"));
    assert!(futures::poll!(&mut recorder).is_pending());
    leading.set(clock.wall());
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    sink.record(obs);
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 2);
    assert_eq!(build.unrecorded, 2);
    let requests = uplink.requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].calls.is_empty());
    assert_eq!(sink.traffic(), counted);
}

#[tokio::test]
async fn promotion_interrupts_pending_proof_without_losing_unsent_calls() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = PendingSigner {
        inner: TestSigner::authority().expect("signer"),
        entered: AtomicUsize::new(0),
    };
    let leading = Leading::default();
    let uplink = FakeUplink::default();
    let obs = observation();
    let id = obs.origin().build.id();
    sink.record(obs.clone());
    sink.lose(&obs.origin().build, NonZeroU64::new(2).expect("nonzero"));
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(signer.entered.load(Ordering::SeqCst), 1);
    assert!(uplink.requests().is_empty());
    leading.set(clock.wall());
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 1);
    assert_eq!(build.unrecorded, 2);
    assert!(uplink.requests().is_empty());
    sink.record(obs);
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 2);
}

#[tokio::test]
async fn promotion_cancels_inflight_send_without_replaying_carried_losses() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = PendingUplink::default();
    let obs = observation();
    let id = obs.origin().build.id();
    sink.record(obs.clone());
    sink.lose(&obs.origin().build, NonZeroU64::new(7).expect("nonzero"));
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    clock.advance(PAUSE);
    assert!(futures::poll!(&mut recorder).is_pending());
    {
        let requests = uplink.0.lock();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].calls.len(), 1);
        assert_eq!(requests[0].lost[0].count, 7);
    }
    sink.record(obs.clone());
    leading.set(clock.wall());
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(uplink.0.lock().len(), 1);
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 1);
    assert_eq!(build.unrecorded, 1);
    sink.record(obs);
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 2);
    assert_eq!(uplink.0.lock().len(), 1);
}

#[tokio::test]
async fn promotion_interrupts_failure_pause() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::scripted([Outcome::NotApplied]);
    let obs = observation();
    let id = obs.origin().build.id();
    sink.record(obs.clone());
    sink.lose(&obs.origin().build, NonZeroU64::new(2).expect("nonzero"));
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(uplink.requests().len(), 1);
    leading.set(clock.wall());
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 0);
    assert_eq!(build.unrecorded, 3);
    assert_eq!(uplink.requests().len(), 1);
}

#[tokio::test]
async fn promotion_interrupts_partial_batch_wait() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::default();
    let obs = observation();
    let id = obs.origin().build.id();
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    sink.record(obs);
    assert!(futures::poll!(&mut recorder).is_pending());
    leading.set(clock.wall());
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 1);
    let requests = uplink.requests();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].calls.is_empty());
}
