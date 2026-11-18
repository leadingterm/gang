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
    SignatureAlgorithm,
};
use gang_testkit::TestSigner;
use gang_types::{BuildKey, InvocationId, NonZeroDuration};
use gang_wire::IngestBuildsRequest;
use parking_lot::Mutex;

use crate::builds::BuildsCell;
use crate::builds_forward::{Forwarding, ProofSource, Uplink};
use crate::leadership::Leading;
use crate::test_support::identity;

#[derive(Clone, Default)]
struct FakeUplink(Arc<Mutex<(VecDeque<Outcome>, Vec<IngestBuildsRequest>)>>);

impl Uplink for FakeUplink {
    async fn send(&self, _proof: &ControllerProof, body: Bytes) -> Outcome {
        let mut state = self.0.lock();
        state
            .1
            .push(serde_json::from_slice(&body).expect("valid request"));
        state.0.pop_front().expect("scripted outcome")
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
    Forwarding::try_new(
        uplink,
        proofs(),
        Duration::from_secs(1),
        config.max_live,
        crate::test_support::tls_settings().limits.max_request_size,
    )
    .expect("valid outbox")
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

#[tokio::test]
async fn failed_calls_become_losses_and_ambiguous_carried_losses_are_not_retried() {
    let config = BuildsConfig::default();
    let (builds, sink, observations) = BuildsCell::channel(&config);
    let clock = TestClock::default();
    let signer = TestSigner::authority().expect("signer");
    let leading = Leading::default();
    let uplink = FakeUplink::default();
    uplink
        .0
        .lock()
        .0
        .extend([Outcome::Unknown, Outcome::Unknown, Outcome::Accepted]);
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
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    sink.record(obs);
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    let state = uplink.0.lock();
    assert_eq!(state.1.len(), 3);
    assert_eq!(state.1[0].calls.len(), 1);
    assert_eq!(state.1[0].lost[0].count, 3);
    assert!(state.1[1].calls.is_empty());
    assert_eq!(state.1[1].lost[0].count, 1);
    assert!(state.1[2].lost.is_empty());
    assert_eq!(state.1[2].calls.len(), 1);
}

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
    leading.set();
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    sink.record(obs);
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 2);
    assert_eq!(build.unrecorded, 2);
    assert!(uplink.0.lock().1.is_empty());
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
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(signer.entered.load(Ordering::SeqCst), 1);
    assert!(uplink.0.lock().1.is_empty());
    leading.set();
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 1);
    assert_eq!(build.unrecorded, 2);
    assert!(uplink.0.lock().1.is_empty());
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
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    {
        let requests = uplink.0.lock();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].calls.len(), 1);
        assert_eq!(requests[0].lost[0].count, 7);
    }
    sink.record(obs.clone());
    leading.set();
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
    let uplink = FakeUplink::default();
    uplink.0.lock().0.push_back(Outcome::NotApplied);
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
    clock.advance(Duration::from_secs(1));
    assert!(futures::poll!(&mut recorder).is_pending());
    assert_eq!(uplink.0.lock().1.len(), 1);
    leading.set();
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 0);
    assert_eq!(build.unrecorded, 3);
    assert_eq!(uplink.0.lock().1.len(), 1);
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
    sink.record(obs);
    let recorder = builds.record_or_forward(
        observations,
        &clock,
        &signer,
        &leading,
        Some(forwarding(uplink.clone(), &config)),
    );
    tokio::pin!(recorder);
    assert!(futures::poll!(&mut recorder).is_pending());
    leading.set();
    assert!(futures::poll!(&mut recorder).is_pending());
    let build = builds
        .read(|builds| builds.get(id, clock.wall()))
        .expect("build");
    assert_eq!(build.usage.cache.hits, 1);
    assert!(uplink.0.lock().1.is_empty());
}
