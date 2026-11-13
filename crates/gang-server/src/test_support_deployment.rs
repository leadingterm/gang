//! The deployments the behavioural tests run: every fake, memory listeners, and the settings and
//! plugin configurations the server is composed from.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use crate::{
    Deployment, HandshakeSettings, Listeners, Parts, Provider, Providers, PublicEndpoint,
    RequestLimits, ServerSettings, Stack, TlsSettings, TransportSettings,
};
use gang_authn::{Anonymous, Mtls, MtlsConfig, NoneConfig, Token, TokenConfig};
use gang_clock::TestClock;
use gang_config::{DashboardConfig, EndpointTrust, RoleConfig, RolePrincipal};
use gang_spi::{LoginSupport, Plugin, PluginContext};
use gang_testkit::{
    FakeCache, FakeProvisioner, FakeRepository, FakeScheduler, TestPlugins, TestSigner,
};
use gang_types::{
    Isa, NonZeroDuration, Percent, PermissionSet, Principal, ProviderName, RoleName, TrustDomain,
    Url,
};
use serde_json::json;

use crate::test_support::start_clock;
use crate::test_support_fleet::fleets;
use crate::test_support_memory::{MemoryDialer, MemoryListener, memory_listener};
use crate::test_support_verifier::FakeVerifier;

/// The deployment of a TLS server: client tokens and worker certificates, both under the
/// certificate authority.
pub struct TlsDeployment;

impl Stack for TlsDeployment {
    type Signer = TestSigner;
    type Clock = TestClock;
    type Repository = FakeRepository;
    type Identity = FakeVerifier;
    type Clients = Token;
    type Workers = Mtls;
}

impl Deployment for TlsDeployment {
    type Provisioner = FakeProvisioner;
    type Cache = FakeCache;
    type Scheduler = FakeScheduler;
    type Listener = MemoryListener;
    type Plugins = TestPlugins;
}

/// The deployment of a plaintext server: every caller is anonymous.
pub struct PlainDeployment;

impl Stack for PlainDeployment {
    type Signer = TestSigner;
    type Clock = TestClock;
    type Repository = FakeRepository;
    type Identity = FakeVerifier;
    type Clients = Anonymous;
    type Workers = Anonymous;
}

impl Deployment for PlainDeployment {
    type Provisioner = FakeProvisioner;
    type Cache = FakeCache;
    type Scheduler = FakeScheduler;
    type Listener = MemoryListener;
    type Plugins = TestPlugins;
}

/// The dialers of the two listeners of a deployment.
pub struct Dials {
    /// Dials the public listener, for clients of the scheduler and the Gang API.
    pub public: MemoryDialer,
    /// Dials the worker listener.
    pub worker: MemoryDialer,
}

/// Handles to the fakes of a deployment, kept by the test after the server takes its clones.
pub struct Handles {
    /// The scheduler fake.
    pub scheduler: FakeScheduler,
    /// The state repository fake.
    pub repository: FakeRepository,
    /// The clock every fake and the server share.
    pub clock: Arc<TestClock>,
    /// The verifier of the interactive provider `example-idp`. The workers' provider
    /// `example-workers` has its own verifier, which accepts the same proofs.
    pub verifier: FakeVerifier,
    /// Switches the test signer to refuse every signature, as an unavailable backend.
    pub signer_outage: Arc<AtomicBool>,
}

/// The state every deployment shares, before its plugins are chosen.
struct Wiring {
    fleet: FakeProvisioner,
    scheduler: FakeScheduler,
    repository: FakeRepository,
    clock: Arc<TestClock>,
    providers: Providers<FakeVerifier>,
    listeners: Listeners<MemoryListener>,
}

/// Builds the shared state over fresh fakes and memory listeners.
fn wire(scheduler: FakeScheduler) -> (Wiring, Dials, Handles) {
    let (public, public_dial) = memory_listener();
    let (worker, worker_dial) = memory_listener();
    let (providers, verifier) = providers();
    let repository = FakeRepository::default();
    let clock = start_clock();
    let signer_outage = Arc::new(AtomicBool::new(false));
    let wiring = Wiring {
        fleet: FakeProvisioner::default(),
        scheduler: scheduler.clone(),
        repository: repository.clone(),
        clock: clock.clone(),
        providers,
        listeners: Listeners { public, worker },
    };
    let dials = Dials {
        public: public_dial,
        worker: worker_dial,
    };
    let handles = Handles {
        scheduler,
        repository,
        clock,
        verifier,
        signer_outage,
    };
    (wiring, dials, handles)
}

/// The two identity providers: `example-idp` logs in interactively, `example-workers` does not.
/// Each verifier accepts the same proofs. Returns the providers and the `example-idp` verifier.
pub fn providers() -> (Providers<FakeVerifier>, FakeVerifier) {
    let interactive = FakeVerifier::new(LoginSupport::Interactive);
    let workers = FakeVerifier::new(LoginSupport::Unsupported);
    for verifier in [&interactive, &workers] {
        verifier.accept("admin-proof", "admin");
        verifier.accept("alice-proof", "alice");
        verifier.accept("worker-proof", "worker-1");
    }
    let mut providers = BTreeMap::new();
    providers.insert(
        provider_name("example-idp"),
        Provider::new(interactive.clone(), json!({ "name": "example-idp" })),
    );
    providers.insert(
        provider_name("example-workers"),
        Provider::new(workers, json!({})),
    );
    (providers, interactive)
}

/// The plugin configuration context: every test configuration is in-memory, so the path is nominal.
fn plugin_context() -> PluginContext<'static> {
    PluginContext {
        config_path: Path::new("gang.toml"),
    }
}

/// The client authentication of a TLS server: `example-idp` tokens.
pub(super) fn token() -> Token {
    let config: TokenConfig = serde_json::from_value(json!({
        "token_ttl": "10m",
        "clock_skew": "30s",
        "max_cached_tokens": 16,
        "providers": ["example-idp"],
    }))
    .expect("token configuration is valid");
    Token::build(config, &plugin_context()).expect("token plugin builds")
}

/// The worker authentication of a TLS server: certificates for `example-workers` proofs.
pub(super) fn mtls() -> Mtls {
    let config: MtlsConfig = serde_json::from_value(json!({
        "provider": "example-workers",
        "certificate_ttl": "1d",
        "max_cached_certificates": 16,
    }))
    .expect("mtls configuration is valid");
    Mtls::build(config, &plugin_context()).expect("mtls plugin builds")
}

/// The authentication of a plaintext server: every caller is anonymous.
pub(super) fn anonymous() -> Anonymous {
    Anonymous::build(NoneConfig::default(), &plugin_context()).expect("none plugin builds")
}

/// Settings for a TLS server: one-second polls, x86-64 by default, trust domain `example.test`,
/// `example-idp/admin` as the only principal, bound to an `administrator` role holding every
/// permission.
#[must_use]
pub fn tls_settings() -> ServerSettings {
    ServerSettings {
        poll_interval: NonZeroDuration::from_secs(1),
        client_action_timeout: NonZeroDuration::from_mins(1),
        default_isa: Isa::X86_64,
        endpoint: endpoint("https://gang.example.test"),
        transport: TransportSettings::Tls(TlsSettings {
            trust_domain: TrustDomain::try_new("example.test").expect("test trust domain is valid"),
            certificate_backdate: Duration::from_mins(1),
            certificate_ttl: NonZeroDuration::from_hours(1),
            reissue_at_percent: Percent::from_percent(50),
            handshake: HandshakeSettings {
                handshake_timeout: NonZeroDuration::from_secs(10),
                max_pending_handshakes: NonZeroUsize::new(256).expect("256 is non-zero"),
                refusal_linger: NonZeroDuration::from_secs(2),
            },
            endpoint_trust: EndpointTrust::Ca,
        }),
        roles: vec![role_for("administrator", PermissionSet::all(), "admin")],
        limits: request_limits(),
        rate_limit: gang_config::RateLimitConfig::default(),
        principal_seen_interval: NonZeroDuration::from_mins(1),
        max_tracked_principals: NonZeroUsize::new(10_000).expect("10,000 is non-zero"),
        accept_error_backoff: NonZeroDuration::from_millis(100),
        dashboard: DashboardConfig::default(),
        login: None,
        provisioner: "test".into(),
    }
}

/// Settings for a TLS server behind a publicly trusted terminator: [`tls_settings`] with `endpoint_trust = System`.
#[must_use]
pub fn publicly_trusted_settings() -> ServerSettings {
    let mut settings = tls_settings();
    let TransportSettings::Tls(tls) = &mut settings.transport else {
        panic!("tls_settings is TLS");
    };
    tls.endpoint_trust = EndpointTrust::System;
    settings
}

/// Settings for a plaintext server: the same as [`tls_settings`] without a transport certificate.
#[must_use]
pub fn plain_settings() -> ServerSettings {
    ServerSettings {
        endpoint: endpoint("http://gang.example.test"),
        transport: TransportSettings::Plaintext,
        ..tls_settings()
    }
}

/// The parts of a [`TlsDeployment`] over fresh fakes, with two memory listeners and a
/// `grpcs` scheduler.
#[must_use]
pub fn tls_parts() -> (Parts<TlsDeployment>, Dials, Handles) {
    let (wiring, dials, handles) = wire(FakeScheduler::new().expect("the scheduler fake builds"));
    let parts = Parts {
        provisioner: wiring.fleet,
        fleets: fleets(),
        cache: FakeCache,
        signer: TestSigner::authority()
            .expect("the test signer builds")
            .with_outage(handles.signer_outage.clone()),
        scheduler: wiring.scheduler,
        repository: wiring.repository,
        providers: wiring.providers,
        clock: wiring.clock,
        listeners: wiring.listeners,
        clients: token(),
        workers: mtls(),
    };
    (parts, dials, handles)
}

/// The parts of a [`PlainDeployment`] over fresh fakes, with two memory listeners and a
/// plain `grpc` scheduler.
#[must_use]
pub fn plain_parts() -> (Parts<PlainDeployment>, Dials, Handles) {
    let (wiring, dials, handles) = wire(FakeScheduler::plain().expect("the scheduler fake builds"));
    let parts = Parts {
        provisioner: wiring.fleet,
        fleets: fleets(),
        cache: FakeCache,
        signer: TestSigner::authority()
            .expect("the test signer builds")
            .with_outage(handles.signer_outage.clone()),
        scheduler: wiring.scheduler,
        repository: wiring.repository,
        providers: wiring.providers,
        clock: wiring.clock,
        listeners: wiring.listeners,
        clients: anonymous(),
        workers: anonymous(),
    };
    (parts, dials, handles)
}

/// The request limits every test server uses: 64 KiB bodies and a 30-second request timeout.
fn request_limits() -> RequestLimits {
    RequestLimits {
        max_request_size: 64 * 1024,
        request_timeout: NonZeroDuration::from_secs(30),
    }
}

/// Parses a test endpoint URL.
pub fn endpoint(text: &str) -> PublicEndpoint {
    let url = Url::parse(text).expect("test endpoint is a valid URL");
    PublicEndpoint::try_new(url).expect("test endpoint scheme is accepted")
}

/// Parses a test provider name.
fn provider_name(text: &str) -> ProviderName {
    ProviderName::try_new(text).expect("test provider name is valid")
}

/// A role named `name` holding `permissions`, bound to `principal` authenticated by
/// `example-idp`.
#[must_use]
pub fn role_for(name: &str, permissions: PermissionSet, principal: &str) -> RoleConfig {
    RoleConfig {
        name: RoleName::try_new(name).expect("test role name is valid"),
        permissions,
        principals: vec![RolePrincipal {
            provider: provider_name("example-idp"),
            principal: Principal::try_new(principal).expect("test principal is valid"),
        }],
    }
}
