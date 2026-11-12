//! Plugins shared by the `compose!` integration tests. Each test crate declares this module and
//! composes the same plugins, so every item here is used by both crates.

use std::future::Future;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use gang_clock::Timestamp;
use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, ActionsView, ApiService,
    Authenticated, AuthenticationHost, AuthenticationPort, CacheStorePort, Capabilities,
    CertStatus, CertificateIssue, CertificateSignerPort, CertificateTarget, ClientCertificates,
    ControllerConnections, ControllerEndpoints, ControllerReady, ControllerSpec,
    CredentialLifecyclePort, Evidence, FleetInstance, FleetOptions, FleetSupply, InstanceIdentity,
    IssuedCertRecord, IssuedCertificate, IssuedToken, ListingLimits, LoginChallenge, LoginClient,
    LoginGrant, LoginPort, LoginRequest, LoginSupport, MountedScratchVolume, NodeLifecyclePort,
    Plugin, PluginContext, PortError, PreemptionNotice, PrincipalRecord, ProofVerifierPort,
    ProvisionerPort, QueuedDemand, RecordedScalingEvent, Rejection, RenderedStoreSpec, RequestGate,
    SandboxPrograms, SandboxedAction, ScalingAuditEvent, ScalingEventSeq, SchedulerPort,
    SchedulerSnapshot, Scheme, ScratchVolumePort, SignatureAlgorithm, StateRepositoryPort,
    StoreAccess, Subject, TokenIssue, WorkerSpec, WorkerTls, WorkloadCredentialPort,
};
use gang_types::{
    AuthenticationSettings, Bytes, CertSerial, Fleet, FleetName, FleetSet, Identity, InstanceId,
    InstanceLifecycle, Isa, LoginSession, Principal, Proof, ProviderName, RefreshToken, Resources,
    SchedulerSettings, Url, WorkerId, WorkerLifetime,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// Every in-test plugin is built from `number = N`; the number is what its ports report.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NumberConfig {
    /// The number that the plugin's ports report.
    pub number: u32,
}

impl SandboxPrograms for NumberConfig {
    fn try_for_each_program<E>(
        &mut self,
        _visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        Ok(())
    }
}

impl SchedulerSettings for NumberConfig {}

/// Declares a plugin built from `number = N`. Test crates reuse it through `#[macro_use] mod support;`.
macro_rules! numbered_plugin {
    ($plugin:ident) => {
        /// A plugin built from `NumberConfig`, whose ports report its `number`.
        pub struct $plugin {
            number: u32,
        }

        impl Plugin for $plugin {
            type Config = NumberConfig;

            fn build(
                config: NumberConfig,
                _context: &PluginContext<'_>,
            ) -> Result<Self, PortError> {
                Ok(Self {
                    number: config.number,
                })
            }
        }
    };
}

/// The instance id that a plugin built from `number` reports.
pub fn instance(number: u32) -> Result<InstanceId, PortError> {
    InstanceId::try_new(format!("node-{number}")).map_err(PortError::backend)
}

/// Options of the `alpha` provisioner: a capacity class and an optional machine class.
#[derive(Debug, Deserialize, FleetOptions)]
#[serde(deny_unknown_fields)]
#[fleet_options(validate = FakeFleetOptions::check)]
pub struct FakeFleetOptions {
    /// Capacity class.
    pub capacity: String,
    /// Optional machine class.
    pub machine_class: Option<String>,
}

impl FakeFleetOptions {
    /// Rejects `machine_class = "reserved"` unless `capacity` is `"on-demand"`. The
    /// `FleetOptions` derive calls this from its `validate` hook when the configuration loads,
    /// so a violation fails the parse and names the fleet.
    ///
    /// # Errors
    ///
    /// Returns a lowercase message, without a trailing period, naming the violated rule.
    pub fn check(&self) -> Result<(), String> {
        if self.machine_class.as_deref() == Some("reserved") && self.capacity != "on-demand" {
            return Err("machine class reserved needs capacity on-demand".to_owned());
        }
        Ok(())
    }
}

/// Options of the `beta` provisioner: none.
#[derive(Debug, Deserialize, FleetOptions)]
#[serde(deny_unknown_fields)]
pub struct OtherFleetOptions {}

/// The value a provisioner's ports record for their fleet options, so tests can see which
/// options reached the plugin.
pub trait SeenOptions {
    /// The value recorded for these options.
    fn seen(&self) -> String;
}

impl SeenOptions for FakeFleetOptions {
    fn seen(&self) -> String {
        self.capacity.clone()
    }
}

impl SeenOptions for OtherFleetOptions {
    fn seen(&self) -> String {
        String::new()
    }
}

macro_rules! provisioner_plugin {
    ($plugin:ident, $lifetime:expr, $options:ty) => {
        /// A provisioner plugin whose ports record the fleet options they receive.
        pub struct $plugin {
            number: u32,
            /// The fleet options each port call received, in call order.
            pub seen: Mutex<Vec<String>>,
        }

        impl Plugin for $plugin {
            type Config = NumberConfig;

            fn build(
                config: NumberConfig,
                _context: &PluginContext<'_>,
            ) -> Result<Self, PortError> {
                Ok(Self {
                    number: config.number,
                    seen: Mutex::new(Vec::new()),
                })
            }
        }

        impl $plugin {
            fn record(&self, options: &$options) {
                self.seen.lock().push(options.seen());
            }
        }

        impl ProvisionerPort for $plugin {
            type FleetOptions = $options;

            async fn supply(
                &self,
                fleet: Fleet<'_, Self::FleetOptions>,
                into: &mut FleetSupply,
            ) -> Result<(), PortError> {
                self.record(fleet.options);
                into.desired = self.number;
                into.instances.clear();
                into.exits.clear();
                into.warm = 0;
                into.instances.push(FleetInstance {
                    id: instance(self.number)?,
                    lifecycle: InstanceLifecycle::InService,
                    launched_at: Timestamp::from_unix_millis(u64::from(self.number)),
                    release_first: false,
                });
                Ok(())
            }

            async fn set_desired_capacity(
                &self,
                fleet: Fleet<'_, Self::FleetOptions>,
                desired: u32,
            ) -> Result<(), PortError> {
                self.record(fleet.options);
                if desired == self.number {
                    Ok(())
                } else {
                    Err(PortError::refused(Rejection::PolicyDenied))
                }
            }

            async fn terminate(
                &self,
                fleet: Fleet<'_, Self::FleetOptions>,
                instance: &InstanceId,
            ) -> Result<(), PortError> {
                self.record(fleet.options);
                Err(PortError::NotFound(Subject::Instance(instance.clone())))
            }

            fn worker_lifetime(&self) -> WorkerLifetime {
                $lifetime
            }
        }

        impl NodeLifecyclePort for $plugin {
            async fn identity(&self) -> Result<InstanceIdentity, PortError> {
                Ok(InstanceIdentity {
                    instance: instance(self.number)?,
                    fleet: FleetName::try_new("workers").map_err(PortError::backend)?,
                    isa: Isa::X86_64,
                })
            }

            async fn preemption_notice(&self) -> Result<Option<PreemptionNotice>, PortError> {
                let terminate_at = Timestamp::from_unix_millis(u64::from(self.number));
                Ok(Some(PreemptionNotice { terminate_at }))
            }

            async fn parked(&self) -> Result<bool, PortError> {
                Ok(false)
            }
        }

        impl ScratchVolumePort for $plugin {
            async fn mount(
                &self,
                _instance: &InstanceId,
            ) -> Result<MountedScratchVolume, PortError> {
                Ok(MountedScratchVolume {
                    path: "/scratch".into(),
                    capacity: Bytes::new(u64::from(self.number)),
                })
            }
        }

        impl WorkloadCredentialPort for $plugin {
            async fn workload_credential(&self) -> Result<Proof, PortError> {
                Ok(Proof::new(format!("workload-{}", self.number)))
            }
        }
    };
}

provisioner_plugin!(Alpha, WorkerLifetime::LongLived, FakeFleetOptions);
provisioner_plugin!(Beta, WorkerLifetime::SingleUse, OtherFleetOptions);

macro_rules! cache_plugin {
    ($plugin:ident) => {
        numbered_plugin!($plugin);

        impl CacheStorePort for $plugin {
            fn content_store(&self) -> Result<RenderedStoreSpec, PortError> {
                Ok(RenderedStoreSpec(serde_json::json!({ "content": self.number })))
            }

            fn action_store(&self) -> Result<RenderedStoreSpec, PortError> {
                Ok(RenderedStoreSpec(serde_json::json!({ "action": self.number })))
            }

            fn worker_access(&self) -> StoreAccess {
                StoreAccess::ViaController
            }
        }
    };
}

cache_plugin!(CacheDisk);
cache_plugin!(CacheCloud);

macro_rules! signer_plugin {
    ($plugin:ident) => {
        numbered_plugin!($plugin);

        impl CertificateSignerPort for $plugin {
            fn algorithm(&self) -> SignatureAlgorithm {
                SignatureAlgorithm::EcdsaP256Sha256
            }

            async fn public_key(&self) -> Result<Vec<u8>, PortError> {
                Ok(self.number.to_le_bytes().to_vec())
            }

            async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, PortError> {
                Ok([&self.number.to_le_bytes()[..], message].concat())
            }
        }
    };
}

signer_plugin!(SignerKms);
signer_plugin!(SignerFile);

/// A scheduler stub whose controller endpoints derive from its `number`.
pub struct Stub {
    number: u32,
    endpoints: ControllerEndpoints,
}

impl Plugin for Stub {
    type Config = NumberConfig;

    fn build(config: NumberConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self {
            number: config.number,
            endpoints: controller_endpoints(config.number)?,
        })
    }
}

fn controller_endpoints(number: u32) -> Result<ControllerEndpoints, PortError> {
    Ok(ControllerEndpoints {
        worker: Url::parse(&format!("grpc://worker-{number}.example:9090"))
            .map_err(PortError::backend)?,
    })
}

impl SchedulerPort for Stub {
    async fn snapshot(&self, into: &mut SchedulerSnapshot) -> Result<(), PortError> {
        into.workers.clear();
        // An empty worker list is sorted by id.
        into.queued.clear();
        let candidates = FleetSet::prefix(1).map_err(PortError::backend)?;
        into.queued.push(QueuedDemand {
            candidates,
            resources: Resources::default(),
            queued_since: Timestamp::UNIX_EPOCH,
            priority: i32::try_from(self.number).map_err(PortError::backend)?,
        });
        Ok(())
    }

    async fn actions(
        &self,
        _limits: ListingLimits,
        into: &mut ActionsView,
    ) -> Result<(), PortError> {
        into.running.clear();
        into.queued.clear();
        into.truncated_running = 0;
        into.truncated_queued = 0;
        Ok(())
    }

    async fn set_draining(&self, worker: &WorkerId, _draining: bool) -> Result<(), PortError> {
        Err(PortError::NotFound(Subject::Worker(worker.clone())))
    }

    fn endpoints(&self) -> ControllerEndpoints {
        self.endpoints.clone()
    }

    async fn run_controller<P: RequestGate, W: RequestGate, A: ApiService>(
        &self,
        _spec: &ControllerSpec<'_>,
        _connections: ControllerConnections<P, W, A>,
        ready: ControllerReady,
    ) -> Result<(), PortError> {
        ready.signal();
        Ok(())
    }

    async fn run_worker(
        &self,
        _spec: &WorkerSpec,
        _tls: WorkerTls<'_>,
        _stop: impl Future<Output = ()> + Send,
    ) -> Result<(), PortError> {
        Ok(())
    }
}

numbered_plugin!(Memory);

impl StateRepositoryPort for Memory {
    async fn record_issued(&self, _record: &IssuedCertRecord) -> Result<(), PortError> {
        Ok(())
    }

    async fn certificate(
        &self,
        _serial: &CertSerial,
    ) -> Result<Option<IssuedCertRecord>, PortError> {
        Ok(None)
    }

    async fn revoke(
        &self,
        serial: &CertSerial,
        _at: Timestamp,
    ) -> Result<IssuedCertRecord, PortError> {
        Err(PortError::NotFound(Subject::Certificate(serial.clone())))
    }

    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
        let principal =
            Principal::try_new(format!("db-{}", self.number)).map_err(PortError::backend)?;
        Ok(vec![IssuedCertRecord {
            serial: CertSerial::try_new("01").map_err(PortError::backend)?,
            identity: Identity::new(
                ProviderName::try_new("corp").map_err(PortError::backend)?,
                principal,
            ),
            issued_at: Timestamp::UNIX_EPOCH,
            expires_at: Timestamp::from_unix_millis(1),
            status: CertStatus::Revoked {
                at: Timestamp::UNIX_EPOCH,
            },
        }])
    }

    async fn certificates_of(
        &self,
        _identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        Ok(Vec::new())
    }

    async fn record_scaling_event(
        &self,
        _event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError> {
        Ok(ScalingEventSeq::new(1))
    }

    async fn scaling_events(
        &self,
        _since: ScalingEventSeq,
        _before: Option<ScalingEventSeq>,
        _limit: NonZeroUsize,
        into: &mut Vec<RecordedScalingEvent>,
    ) -> Result<bool, PortError> {
        into.clear();
        Ok(false)
    }

    async fn observe_principal(
        &self,
        _identity: &Identity,
        _at: Timestamp,
    ) -> Result<(), PortError> {
        Ok(())
    }

    async fn principal(&self, _identity: &Identity) -> Result<Option<PrincipalRecord>, PortError> {
        Ok(None)
    }

    async fn revoke_principal(
        &self,
        identity: &Identity,
        at: Timestamp,
    ) -> Result<PrincipalRecord, PortError> {
        Ok(PrincipalRecord {
            identity: identity.clone(),
            first_seen: at,
            last_seen: at,
            revoked_at: Some(at),
        })
    }

    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        Err(PortError::NotFound(Subject::Principal(identity.clone())))
    }

    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
        Ok(Vec::new())
    }

    async fn principals(
        &self,
        _after: Option<&gang_types::Identity>,
        _limit: NonZeroUsize,
        _only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError> {
        into.clear();
        Ok(false)
    }
}

macro_rules! sandbox_port {
    ($plugin:ident) => {
        impl ActionIsolationPort for $plugin {
            async fn start(
                &self,
                _spec: &ActionExecutionSpec<'_>,
            ) -> Result<SandboxedAction, PortError> {
                let number = i32::try_from(self.number).map_err(PortError::backend)?;
                Ok(SandboxedAction::new(async move {
                    Ok(ActionExecutionOutcome::Exited(number))
                }))
            }
        }
    };
}

macro_rules! sandbox_plugin {
    ($plugin:ident) => {
        numbered_plugin!($plugin);

        sandbox_port!($plugin);
    };
}

sandbox_plugin!(Direct);

/// The `[sandbox.boxed]` section: the number its actions exit with, and the program that runs it.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoxedConfig {
    /// The number the boxed plugin's actions exit with.
    pub number: u32,
    /// The program the boxed action runs; empty when the section omits it.
    #[serde(default)]
    pub program: PathBuf,
}

impl SandboxPrograms for BoxedConfig {
    fn try_for_each_program<E>(
        &mut self,
        visit: &mut impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        visit(&mut self.program)
    }
}

/// The sandbox plugin whose section names the program that runs it.
pub struct Boxed {
    number: u32,
}

impl Plugin for Boxed {
    type Config = BoxedConfig;

    fn build(config: BoxedConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self {
            number: config.number,
        })
    }
}

sandbox_port!(Boxed);

numbered_plugin!(Token);

impl ProofVerifierPort for Token {
    async fn verify(&self, proof: &Proof) -> Result<Principal, PortError> {
        if proof.expose() == "forged" {
            return Err(PortError::refused(Rejection::InvalidProof));
        }
        Principal::try_new(format!("{}:{}", proof.expose(), self.number))
            .map_err(PortError::backend)
    }
}

impl LoginPort for Token {
    async fn begin_login(&self, request: &LoginRequest<'_>) -> Result<LoginChallenge, PortError> {
        let mut authorization_url = request.redirect_uri.clone();
        let query = format!("n={}&state={}", self.number, self.number);
        authorization_url.set_query(Some(query.as_str()));
        Ok(LoginChallenge {
            authorization_url,
            state: self.number.to_string(),
            session: LoginSession::new(self.number.to_string()),
        })
    }

    async fn complete_login(
        &self,
        session: LoginSession,
        _callback_url: &Url,
    ) -> Result<LoginGrant, PortError> {
        Ok(LoginGrant {
            proof: Proof::new(session.expose()),
            refresh_token: None,
        })
    }

    async fn renew(
        &self,
        _refresh_token: &RefreshToken,
        _client: Option<&LoginClient>,
    ) -> Result<LoginGrant, PortError> {
        Err(PortError::Unsupported { operation: "renew" })
    }

    fn login(&self) -> LoginSupport {
        LoginSupport::Interactive
    }
}

/// Implements [`CredentialLifecyclePort`] for a plugin that issues and revokes nothing.
macro_rules! no_credentials {
    ($plugin:ident) => {
        impl CredentialLifecyclePort for $plugin {
            async fn issue_certificate<H: AuthenticationHost>(
                &self,
                _host: &H,
                _request: &CertificateIssue<'_>,
            ) -> Result<IssuedCertificate, PortError> {
                Err(PortError::Unsupported {
                    operation: "issue_certificate",
                })
            }

            async fn issue_token<H: AuthenticationHost>(
                &self,
                _host: &H,
                _request: &TokenIssue<'_>,
            ) -> Result<IssuedToken, PortError> {
                Err(PortError::Unsupported {
                    operation: "issue_token",
                })
            }

            async fn issue_session<H: AuthenticationHost>(
                &self,
                _host: &H,
                _request: &TokenIssue<'_>,
            ) -> Result<IssuedToken, PortError> {
                Err(PortError::Unsupported {
                    operation: "issue_session",
                })
            }

            async fn revoke_certificates<H: AuthenticationHost>(
                &self,
                _host: &H,
                _target: &CertificateTarget<'_>,
            ) -> Result<Vec<IssuedCertRecord>, PortError> {
                Err(PortError::Unsupported {
                    operation: "revoke_certificates",
                })
            }

            async fn revoked_serials<H: AuthenticationHost>(
                &self,
                _host: &H,
            ) -> Result<Vec<CertSerial>, PortError> {
                Err(PortError::Unsupported {
                    operation: "revoked_serials",
                })
            }
        }
    };
}

/// The `[authentication.clients.tls]`-style section of [`AuthTls`]: a number, and the identity
/// provider it names.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthTlsConfig {
    /// The number that the `AuthTls` plugin's ports report.
    pub number: u32,
}

/// The identity provider [`AuthTlsConfig`] names; a static so `providers` can return a reference.
static EXAMPLE_IDP: std::sync::LazyLock<ProviderName> = std::sync::LazyLock::new(|| {
    ProviderName::try_new("example-idp").expect("the example provider is a valid name")
});

impl AuthenticationSettings for AuthTlsConfig {
    fn requires_tls(&self) -> bool {
        true
    }

    fn providers(&self) -> &[ProviderName] {
        std::slice::from_ref(&EXAMPLE_IDP)
    }
}

/// An authentication plugin that needs TLS and identifies callers by mTLS certificate.
pub struct AuthTls;

impl Plugin for AuthTls {
    type Config = AuthTlsConfig;

    fn build(_config: AuthTlsConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self)
    }
}

impl AuthenticationPort for AuthTls {
    fn scheme(&self) -> Scheme {
        Scheme::Mtls
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn client_certificates(&self) -> ClientCertificates {
        ClientCertificates::Required
    }

    async fn authenticate<H: AuthenticationHost>(
        &self,
        _host: &H,
        _evidence: &Evidence<'_>,
    ) -> Result<Option<Authenticated>, PortError> {
        Ok(None)
    }
}

no_credentials!(AuthTls);

/// The `[authentication.clients.none]`-style section of [`AuthOpen`]: a number.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthOpenConfig {
    /// The number that the `AuthOpen` plugin's ports report.
    pub number: u32,
}

impl AuthenticationSettings for AuthOpenConfig {
    fn requires_tls(&self) -> bool {
        false
    }
}

/// An authentication plugin that takes no credentials and needs no TLS.
pub struct AuthOpen;

impl Plugin for AuthOpen {
    type Config = AuthOpenConfig;

    fn build(_config: AuthOpenConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self)
    }
}

impl AuthenticationPort for AuthOpen {
    fn scheme(&self) -> Scheme {
        Scheme::None
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn client_certificates(&self) -> ClientCertificates {
        ClientCertificates::Never
    }

    async fn authenticate<H: AuthenticationHost>(
        &self,
        _host: &H,
        _evidence: &Evidence<'_>,
    ) -> Result<Option<Authenticated>, PortError> {
        Ok(None)
    }
}

no_credentials!(AuthOpen);
