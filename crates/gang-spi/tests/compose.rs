//! `compose!` over in-test plugins: typed configuration and dispatch to the selected plugin.

use std::convert::Infallible;
use std::error::Error;
use std::ffi::OsString;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;
use gang_clock::Timestamp;
use gang_config::{ConfigError, GangConfig, SandboxSelection};
use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, ActionLimits,
    ActionLimitsPort, ActionScope, ActionUsage, ApiRequestBody, ApiResponseBody, ApiService,
    AuthenticationPort, BuildError, CacheStorePort, CertificateSignerPort, ControllerConnections,
    ControllerEndpoints, ControllerReady, ControllerSpec, FleetInstance, FleetSupply, Listening,
    LoginPort, LoginRequest, LoginSupport, NetworkRequest, NodeLifecyclePort, Peer, Plugin,
    PluginContext, PortError, PreemptionNotice, ProofVerifierPort, ProvisionerPort, Rejection,
    RenderedStoreSpec, RequestGate, SchedulerPort, SchedulerSnapshot, Scheme, ScratchVolumePort,
    SignatureAlgorithm, Slots, StateRepositoryPort, StoreAccess, Subject, WorkerSpec, WorkerTls,
    WorkloadCredentialPort, full_body,
};
use gang_testkit::must_ready as ready;
use gang_types::{
    AuthenticationSettings, Bytes, CertSerial, Fleet, FleetCatalog, FleetName, FleetProperties,
    InstanceLifecycle, Isa, NonZeroBytes, Principal, Proof, ProviderName, RefreshToken, Resources,
    Url, WorkerId, WorkerLifetime,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[macro_use]
mod support;

use support::{
    Alpha, AuthOpen, AuthOpenConfig, AuthTls, AuthTlsConfig, Beta, Boxed, CacheCloud, CacheDisk,
    Direct, FakeFleetOptions, Memory, NumberConfig, OtherFleetOptions, SignerFile, SignerKms, Stub,
    Token, instance,
};

/// The fleet fixture: a name plus the provisioner's narrowed options, borrowed as a [`Fleet`].
struct TestFleet {
    name: FleetName,
    options: ProvisionerFleetOptions,
}

impl TestFleet {
    fn view(&self) -> Fleet<'_, ProvisionerFleetOptions> {
        Fleet {
            name: &self.name,
            isa: Isa::X86_64,
            instance: Resources {
                vcpus: 4,
                memory: Bytes::new(8 << 30),
                disk: Bytes::new(100 << 30),
            },
            options: &self.options,
        }
    }
}

fn test_fleet(options: ProvisionerFleetOptions) -> Result<TestFleet, Box<dyn Error>> {
    Ok(TestFleet {
        name: FleetName::try_new("workers")?,
        options,
    })
}

numbered_plugin!(Fake);

/// The scope the fake limits plugin hands out: the number it was built with and the launcher it
/// lends to the action.
#[derive(Debug)]
pub struct FakeScope {
    /// The plugin's number, reported back as the action's usage.
    number: u32,
    /// The launcher `launcher` lends; it names the number so the test can observe it.
    launcher: Box<[OsString]>,
}

impl ActionScope for FakeScope {
    fn launcher(&self) -> &[OsString] {
        &self.launcher
    }
}

impl ActionLimitsPort for Fake {
    type Scope = FakeScope;

    async fn prepare(&self) -> Result<(), PortError> {
        Ok(())
    }

    async fn enter(&self, _limits: &ActionLimits) -> Result<FakeScope, PortError> {
        Ok(FakeScope {
            number: self.number,
            launcher: Box::new([
                OsString::from("fake-join"),
                OsString::from(self.number.to_string()),
            ]),
        })
    }

    async fn leave(&self, scope: FakeScope) -> Result<ActionUsage, PortError> {
        Ok(ActionUsage {
            peak_memory: Bytes::new(u64::from(scope.number)),
            cpu_time: Duration::from_secs(u64::from(scope.number)),
        })
    }

    async fn reap_abandoned(&self, _creator: u32) -> Result<(), PortError> {
        Ok(())
    }
}

gang_spi::compose! {
    provisioner: Provisioner { alpha: Alpha, beta: Beta },
    cache: Cache { disk: CacheDisk, cloud: CacheCloud },
    signer: Signer { kms: SignerKms, file: SignerFile },
    scheduler: Scheduler { stub: Stub },
    database: Database { memory: Memory },
    sandbox: Sandbox { direct: Direct, boxed: Boxed },
    identity_provider: IdentityProvider { token: Token },
    client_authentication: ClientAuthentication { token: AuthTls, none: AuthOpen },
    worker_authentication: WorkerAuthentication { mtls: AuthTls, none: AuthOpen },
    limits: Limits { fake: Fake },
}

const CONFIG: &str = r#"
poll_interval = "1s"
client_action_timeout = "1m"
default_isa = "x86-64"
listen = "127.0.0.1:9443"
worker_listen = "127.0.0.1:9090"
endpoint = "http://127.0.0.1:9443"

[server]
transport = "plaintext"

[authentication.clients.none]
number = 12

[authentication.workers.none]
number = 13

[[fleets]]
name = "workers"
isa = "x86-64"
sandbox = "boxed"
min_instances = 0
max_instances = 4
vcpus_per_instance = 4
memory_per_instance = "8GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 4
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "30s"

[provisioner.beta]
number = 2

[cache.disk]
number = 10

[signer.kms]
number = 11

[scheduler.stub]
number = 3

[database.memory]
number = 4

[sandbox.direct]
number = 5

[sandbox.boxed]
number = 6

[identity.providers.corp.token]
number = 7
"#;

// The beta section of `CONFIG`, and the alpha section that selects the other provisioner. Alpha
// requires `capacity`, which lives in the fleet's `[fleets.options]` table.
const BETA_SECTION: &str = "[provisioner.beta]\nnumber = 2";
const ALPHA_SECTION: &str =
    "[fleets.options]\ncapacity = \"spot\"\n\n[provisioner.alpha]\nnumber = 1";

fn parse(text: &str) -> Result<GangConfig<Plugins>, Box<dyn Error>> {
    Ok(text.parse()?)
}

fn sandbox_config(text: &str) -> Result<SandboxConfig, Box<dyn Error>> {
    Ok(parse(text)?.sandbox)
}

/// The programs the sandbox selection visits for `key`, in visit order.
fn visited_programs(sandbox: &mut SandboxConfig, key: SandboxKey) -> Vec<PathBuf> {
    let mut visited = Vec::new();
    sandbox
        .try_for_each_program(key, |program: &mut PathBuf| -> Result<(), Infallible> {
            visited.push(program.clone());
            Ok(())
        })
        .expect("the visitor never fails");
    visited
}

fn context() -> PluginContext<'static> {
    PluginContext {
        config_path: Path::new("gang.toml"),
    }
}

fn rejection(text: &str) -> Result<String, Box<dyn Error>> {
    match text.parse::<GangConfig<Plugins>>() {
        Ok(_) => Err("accepted".into()),
        Err(error) => Ok(gang_types::error_chain(&error).to_string()),
    }
}

fn execute(sandbox: &Sandbox) -> Result<ActionExecutionOutcome, Box<dyn Error>> {
    let argv = [OsString::from("true")];
    let spec = ActionExecutionSpec {
        command: gang_spi::ActionCommand::from_argv(&argv).ok_or("empty argv")?,
        launcher: &[],
        environment: &[],
        action_directory: Path::new("/work"),
        working_directory: Path::new("/work"),
        network: NetworkRequest::NotRequested,
    };
    Ok(ready(async { sandbox.start(&spec).await?.wait().await })?)
}

#[test]
fn every_provisioner_port_reaches_the_selected_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    assert!(matches!(config.provisioner, ProvisionerConfig::Beta(_)));
    let provisioner = Provisioner::build(config.provisioner, &context())?;
    assert!(matches!(provisioner, Provisioner::Beta(_)));
    let fleet = test_fleet(ProvisionerFleetOptions::Beta(OtherFleetOptions {}))?;

    let mut supply = FleetSupply::default();
    ready(provisioner.supply(fleet.view(), &mut supply))?;
    assert_eq!(supply.desired, 2);
    assert_eq!(
        supply.instances,
        vec![FleetInstance {
            id: instance(2)?,
            lifecycle: InstanceLifecycle::InService,
            launched_at: Timestamp::from_unix_millis(2),
            release_first: false,
        }]
    );
    ready(provisioner.supply(fleet.view(), &mut supply))?;
    assert_eq!(
        supply.instances.len(),
        1,
        "supply must overwrite its buffer"
    );

    ready(provisioner.set_desired_capacity(fleet.view(), 2))?;
    assert!(matches!(
        ready(provisioner.set_desired_capacity(fleet.view(), 9)),
        Err(PortError::Rejected {
            reason: Rejection::PolicyDenied,
            ..
        })
    ));
    let gone = instance(1)?;
    assert!(matches!(
        ready(provisioner.terminate(fleet.view(), &gone)),
        Err(PortError::NotFound(Subject::Instance(found))) if found == gone
    ));
    assert_eq!(ready(provisioner.identity())?.instance, instance(2)?);
    assert_eq!(
        ready(provisioner.preemption_notice())?,
        Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(2)
        })
    );
    assert_eq!(
        ready(provisioner.mount(&instance(2)?))?.capacity,
        Bytes::new(2)
    );
    assert_eq!(
        ready(provisioner.workload_credential())?.expose(),
        "workload-2"
    );
    Ok(())
}

#[test]
fn provisioner_reports_its_worker_lifetime_through_dispatch_for_both_plugins()
-> Result<(), Box<dyn Error>> {
    let beta = Provisioner::build(parse(CONFIG)?.provisioner, &context())?;
    assert_eq!(beta.worker_lifetime(), WorkerLifetime::SingleUse);

    let text = CONFIG.replace(BETA_SECTION, ALPHA_SECTION);
    let alpha = Provisioner::build(parse(&text)?.provisioner, &context())?;
    assert_eq!(alpha.worker_lifetime(), WorkerLifetime::LongLived);
    Ok(())
}

#[test]
fn selecting_the_other_provisioner_plugin_dispatches_to_it() -> Result<(), Box<dyn Error>> {
    let text = CONFIG.replace(BETA_SECTION, ALPHA_SECTION);
    let config = parse(&text)?;
    let provisioner = Provisioner::build(config.provisioner, &context())?;

    assert!(matches!(provisioner, Provisioner::Alpha(_)));
    let mut supply = FleetSupply::default();
    let fleet = test_fleet(ProvisionerFleetOptions::Alpha(FakeFleetOptions {
        capacity: "spot".to_owned(),
        machine_class: None,
    }))?;
    ready(provisioner.supply(fleet.view(), &mut supply))?;
    assert_eq!(supply.desired, 1);
    assert_eq!(
        ready(provisioner.workload_credential())?.expose(),
        "workload-1"
    );
    Ok(())
}

#[test]
fn cache_port_reaches_the_selected_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    let cache = Cache::build(config.cache, &context())?;

    assert!(matches!(cache, Cache::Disk(_)));
    assert_eq!(
        cache.content_store()?,
        RenderedStoreSpec(serde_json::json!({ "content": 10 }))
    );
    assert_eq!(
        cache.action_store()?,
        RenderedStoreSpec(serde_json::json!({ "action": 10 }))
    );
    assert_eq!(cache.worker_access(), StoreAccess::ViaController);
    Ok(())
}

#[test]
fn unknown_cache_plugin_is_rejected_with_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let message = rejection(&CONFIG.replace("[cache.disk]", "[cache.gcp]"))?;

    assert!(
        message.contains("gcp") && message.contains("disk") && message.contains("cloud"),
        "{message}"
    );
    Ok(())
}

#[test]
fn signer_port_reaches_the_selected_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    let signer = Signer::build(config.signer, &context())?;

    assert!(matches!(signer, Signer::Kms(_)));
    assert_eq!(signer.algorithm(), SignatureAlgorithm::EcdsaP256Sha256);
    assert_eq!(ready(signer.public_key())?, 11_u32.to_le_bytes());
    assert_eq!(
        ready(signer.sign(b"body"))?,
        [&11_u32.to_le_bytes()[..], b"body"].concat()
    );
    Ok(())
}

#[test]
fn unknown_signer_plugin_is_rejected_with_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let message = rejection(&CONFIG.replace("[signer.kms]", "[signer.gcp]"))?;

    assert!(
        message.contains("gcp") && message.contains("kms") && message.contains("file"),
        "{message}"
    );
    Ok(())
}

#[test]
fn scheduler_ports_reach_their_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    let plugin_context = context();
    let scheduler = Scheduler::build(config.scheduler, &plugin_context)?;

    assert_eq!(
        scheduler.endpoints(),
        ControllerEndpoints {
            worker: Url::parse("grpc://worker-3.example:9090")?,
        }
    );

    let mut snapshot = SchedulerSnapshot::default();
    ready(scheduler.snapshot(&mut snapshot))?;
    assert_eq!(snapshot.queued[0].priority, 3);
    ready(scheduler.snapshot(&mut snapshot))?;
    assert_eq!(
        snapshot.queued.len(),
        1,
        "snapshot must overwrite its buffer"
    );

    let worker = WorkerId::try_new("node-1abc")?;
    assert!(matches!(
        ready(scheduler.set_draining(&worker, true)),
        Err(PortError::NotFound(Subject::Worker(found))) if found == worker
    ));

    Ok(())
}

/// A request gate that admits every request.
#[derive(Clone)]
struct TestGate;

impl RequestGate for TestGate {
    async fn admit(
        &self,
        _peer: &Peer,
        _parts: &mut http::request::Parts,
    ) -> Result<(), PortError> {
        Ok(())
    }
}

/// An API that answers every request with 200 and an empty body.
#[derive(Clone)]
struct TestApi;

impl ApiService for TestApi {
    async fn call(
        &self,
        _request: http::Request<ApiRequestBody>,
    ) -> http::Response<ApiResponseBody> {
        http::Response::new(full_body(bytes::Bytes::new()))
    }
}

#[test]
fn scheduler_runs_the_selected_controller_and_worker() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    let plugin_context = context();
    let scheduler = Scheduler::build(config.scheduler, &plugin_context)?;

    let stores = RenderedStoreSpec(serde_json::Value::Null);
    let name = FleetName::try_new("workers")?;
    let properties = FleetProperties::new(
        &name,
        Isa::X86_64,
        &"boxed",
        Resources {
            vcpus: 4,
            memory: Bytes::new(1),
            disk: Bytes::new(100 << 30),
        },
        [],
    );
    let catalog = FleetCatalog::try_new(vec![properties.clone()])?;
    let controller = ControllerSpec {
        content_store: &stores,
        action_store: &stores,
        fleets: &catalog,
        client_action_timeout: Duration::from_mins(1),
    };
    let connections = ControllerConnections {
        public: Listening {
            connections: futures::stream::empty().boxed(),
            gate: TestGate,
        },
        worker: Listening {
            connections: futures::stream::empty().boxed(),
            gate: TestGate,
        },
        api: TestApi,
    };
    let (ready_signal, mut readiness) = ControllerReady::channel();
    ready(scheduler.run_controller(&controller, connections, ready_signal))?;
    assert!(
        readiness.try_recv().is_ok(),
        "the composed scheduler passes the readiness signal to its plugin"
    );

    let worker_instance = instance(3)?;
    let worker_spec = WorkerSpec {
        instance: worker_instance,
        properties,
        slots: Slots::LongLived(NonZeroU32::MIN.saturating_add(3)),
        work_directory: PathBuf::from("/work"),
        fast_cas_directory: PathBuf::from("/work/cas"),
        fast_cas_budget: Bytes::new(1024),
        content_store: stores.clone(),
        action_store: stores.clone(),
        store_access: StoreAccess::ViaController,
        entrypoint: PathBuf::from("/usr/bin/gang"),
    };
    ready(scheduler.run_worker(&worker_spec, WorkerTls::Plain, std::future::ready(())))?;
    Ok(())
}

#[test]
fn database_ports_reach_their_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    let plugin_context = context();
    let database = Database::build(config.database, &plugin_context)?;

    let serial = CertSerial::try_new("ff")?;
    assert!(matches!(
        ready(database.revoke(&serial, Timestamp::UNIX_EPOCH)),
        Err(PortError::NotFound(Subject::Certificate(found))) if found == serial
    ));
    assert_eq!(ready(database.certificate(&serial))?, None);
    let revoked = ready(database.revoked())?;
    let [record] = revoked.as_slice() else {
        return Err("expected exactly one revoked record".into());
    };
    assert_eq!(record.identity.principal, Principal::try_new("db-4")?);
    Ok(())
}

#[test]
fn identity_ports_reach_their_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    let plugin_context = context();
    let provider = config
        .identity
        .providers
        .into_values()
        .next()
        .ok_or("corp provider")?;
    let identity = IdentityProvider::build(provider, &plugin_context)?;

    assert_eq!(
        ready(identity.verify(&Proof::new("tok")))?.as_str(),
        "tok:7"
    );
    assert!(matches!(
        ready(identity.verify(&Proof::new("forged"))),
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
    let redirect_uri = Url::parse("http://127.0.0.1:1/callback")?;
    let challenge = ready(identity.begin_login(&LoginRequest {
        redirect_uri: &redirect_uri,
        client: None,
        offline: false,
    }))?;
    assert_eq!(
        challenge.authorization_url,
        Url::parse("http://127.0.0.1:1/callback?n=7&state=7")?
    );
    let callback_url = Url::parse("http://127.0.0.1:1/callback?code=c")?;
    let grant = ready(identity.complete_login(challenge.session, &callback_url))?;
    assert_eq!(grant.proof.expose(), "7");
    assert!(matches!(
        ready(identity.renew(&RefreshToken::new("r"), None)),
        Err(PortError::Unsupported { operation: "renew" })
    ));
    assert_eq!(identity.login(), LoginSupport::Interactive);
    Ok(())
}

#[test]
fn sandbox_builds_the_selected_plugin_and_dispatches_by_variant() -> Result<(), Box<dyn Error>> {
    let direct = Sandbox::build(SandboxKey::Direct, sandbox_config(CONFIG)?, &context())?;
    let boxed = Sandbox::build(SandboxKey::Boxed, sandbox_config(CONFIG)?, &context())?;

    assert!(matches!(direct, Sandbox::Direct(_)));
    assert!(matches!(boxed, Sandbox::Boxed(_)));
    assert_eq!(execute(&direct)?, ActionExecutionOutcome::Exited(5));
    assert_eq!(execute(&boxed)?, ActionExecutionOutcome::Exited(6));
    Ok(())
}

#[test]
fn a_sandbox_whose_section_is_absent_is_not_built() -> Result<(), Box<dyn Error>> {
    let text = CONFIG.replace("[sandbox.direct]\nnumber = 5\n", "");

    let result = Sandbox::build(SandboxKey::Direct, sandbox_config(&text)?, &context());
    assert!(matches!(
        result,
        Err(BuildError {
            domain: "sandbox",
            source: PortError::Config(cause),
        }) if cause.to_string() == "sandbox `direct` has no [sandbox.direct] section"
    ));
    let boxed = Sandbox::build(SandboxKey::Boxed, sandbox_config(&text)?, &context())?;
    assert_eq!(execute(&boxed)?, ActionExecutionOutcome::Exited(6));
    Ok(())
}

#[test]
fn fleet_sandbox_is_parsed_as_a_typed_key() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    let fleet = config.fleets.get(0).ok_or("fleet entry")?;

    assert_eq!(fleet.sandbox, SandboxKey::Boxed);
    assert_eq!(SandboxKey::Boxed.to_string(), "boxed");
    Ok(())
}

#[test]
fn absent_limits_section_parses_to_no_limits() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;

    assert!(config.limits.is_none());
    Ok(())
}

#[test]
fn limits_port_reaches_the_selected_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(&format!("{CONFIG}\n[limits.fake]\nnumber = 8\n"))?;
    let limits = Limits::build(config.limits.ok_or("limits section")?, &context())?;
    assert!(matches!(limits, Limits::Fake(_)));

    ready(limits.prepare())?;
    let requested = ActionLimits {
        cpu_count: NonZeroU32::new(2),
        memory: Some(NonZeroBytes::kib(1)),
    };
    let scope = ready(limits.enter(&requested))?;
    let expected = [OsString::from("fake-join"), OsString::from("8")];
    assert_eq!(scope.launcher(), expected.as_slice());
    assert_eq!(
        ready(limits.leave(scope))?,
        ActionUsage {
            peak_memory: Bytes::new(8),
            cpu_time: Duration::from_secs(8),
        }
    );
    Ok(())
}

#[test]
fn unknown_limits_plugin_is_rejected_with_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let message = rejection(&format!("{CONFIG}\n[limits.gcp]\nnumber = 1\n"))?;

    assert!(
        message.contains("gcp") && message.contains("fake"),
        "{message}"
    );
    Ok(())
}

#[test]
fn two_limits_plugin_sections_are_rejected() -> Result<(), Box<dyn Error>> {
    rejection(&format!(
        "{CONFIG}\n[limits.fake]\nnumber = 8\n\n[limits.gcp]\nnumber = 9\n"
    ))?;
    Ok(())
}

#[test]
fn unknown_provisioner_backend_is_rejected_with_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let message = rejection(&CONFIG.replace("[provisioner.beta]", "[provisioner.gcp]"))?;

    assert!(
        message.contains("gcp") && message.contains("alpha") && message.contains("beta"),
        "{message}"
    );
    Ok(())
}

#[test]
fn two_provisioner_backends_are_rejected() -> Result<(), Box<dyn Error>> {
    rejection(&format!("{CONFIG}\n[provisioner.alpha]\nnumber = 1\n"))?;
    Ok(())
}

#[test]
fn unknown_sandbox_and_unknown_plugin_fields_are_rejected() -> Result<(), Box<dyn Error>> {
    let default = rejection(&CONFIG.replace("sandbox = \"boxed\"", "sandbox = \"firejail\""))?;
    let section = rejection(&format!("{CONFIG}\n[sandbox.firejail]\nnumber = 1\n"))?;
    let field = rejection(&CONFIG.replace(
        "[scheduler.stub]\nnumber = 3",
        "[scheduler.stub]\nnumber = 3\ncolour = 1",
    ))?;

    assert!(
        default.contains("firejail") && default.contains("direct") && default.contains("boxed"),
        "{default}"
    );
    assert!(section.contains("firejail"), "{section}");
    assert!(field.contains("colour"), "{field}");
    Ok(())
}

#[test]
fn unknown_identity_plugin_is_rejected_with_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let message = rejection(&CONFIG.replace(
        "[identity.providers.corp.token]",
        "[identity.providers.corp.saml]",
    ))?;

    assert!(
        message.contains("saml") && message.contains("token"),
        "{message}"
    );
    Ok(())
}

#[test]
fn selected_configuration_serializes_and_reparses_to_the_same_plugin() -> Result<(), Box<dyn Error>>
{
    let config = parse(CONFIG)?;
    let provisioner = toml::to_string(&config.provisioner)?;
    let scheduler = toml::to_string(&config.scheduler)?;
    let sandbox = toml::to_string(&config.sandbox)?;

    assert!(matches!(
        toml::from_str::<ProvisionerConfig>(&provisioner)?,
        ProvisionerConfig::Beta(inner) if inner.number == 2
    ));
    assert!(matches!(
        toml::from_str::<SchedulerConfig>(&scheduler)?,
        SchedulerConfig::Stub(inner) if inner.number == 3
    ));
    let reparsed = toml::from_str::<SandboxConfig>(&sandbox)?;
    assert!(reparsed.is_configured(SandboxKey::Boxed));
    let boxed = Sandbox::build(SandboxKey::Boxed, reparsed, &context())?;
    assert_eq!(execute(&boxed)?, ActionExecutionOutcome::Exited(6));
    Ok(())
}

#[test]
fn sandbox_programs_are_visited_for_the_selected_section_only() -> Result<(), Box<dyn Error>> {
    let mut sandbox = sandbox_config(&CONFIG.replace(
        "[sandbox.boxed]\nnumber = 6\n",
        "[sandbox.boxed]\nnumber = 6\nprogram = \"boxed-sandbox\"\n",
    ))?;
    assert_eq!(
        visited_programs(&mut sandbox, SandboxKey::Boxed),
        [PathBuf::from("boxed-sandbox")]
    );

    let mut absent = sandbox_config(
        &CONFIG
            .replace("sandbox = \"boxed\"", "sandbox = \"direct\"")
            .replace("[sandbox.boxed]\nnumber = 6\n", ""),
    )?;
    assert!(visited_programs(&mut absent, SandboxKey::Boxed).is_empty());
    Ok(())
}

#[test]
fn sandbox_program_visit_leaves_unselected_sections_untouched() -> Result<(), Box<dyn Error>> {
    let mut sandbox = sandbox_config(&CONFIG.replace(
        "[sandbox.boxed]\nnumber = 6\n",
        "[sandbox.boxed]\nnumber = 6\nprogram = \"boxed-sandbox\"\n",
    ))?;
    let mut calls = 0;
    sandbox.try_for_each_program(
        SandboxKey::Direct,
        |program: &mut PathBuf| -> Result<(), Infallible> {
            calls += 1;
            *program = PathBuf::from("/usr/bin/rewritten");
            Ok(())
        },
    )?;
    assert_eq!(calls, 0);
    assert_eq!(
        visited_programs(&mut sandbox, SandboxKey::Boxed),
        [PathBuf::from("boxed-sandbox")]
    );
    Ok(())
}

#[test]
fn sandbox_program_visit_rewrites_paths_in_place() -> Result<(), Box<dyn Error>> {
    let mut sandbox = sandbox_config(&CONFIG.replace(
        "[sandbox.boxed]\nnumber = 6\n",
        "[sandbox.boxed]\nnumber = 6\nprogram = \"boxed-sandbox\"\n",
    ))?;
    sandbox.try_for_each_program(
        SandboxKey::Boxed,
        |program: &mut PathBuf| -> Result<(), Infallible> {
            *program = PathBuf::from("/usr/bin/boxed-sandbox");
            Ok(())
        },
    )?;
    assert_eq!(
        visited_programs(&mut sandbox, SandboxKey::Boxed),
        [PathBuf::from("/usr/bin/boxed-sandbox")]
    );
    Ok(())
}

#[test]
fn sandbox_program_visit_stops_at_the_first_error() -> Result<(), Box<dyn Error>> {
    let mut sandbox = sandbox_config(&CONFIG.replace(
        "[sandbox.boxed]\nnumber = 6\n",
        "[sandbox.boxed]\nnumber = 6\nprogram = \"boxed-sandbox\"\n",
    ))?;
    let mut calls = 0;
    let result = sandbox.try_for_each_program(SandboxKey::Boxed, |_program: &mut PathBuf| {
        calls += 1;
        Err::<(), _>("refused")
    });
    assert_eq!(result, Err("refused"));
    assert_eq!(calls, 1);
    Ok(())
}

#[test]
fn network_request_round_trips_through_text() -> Result<(), Box<dyn Error>> {
    assert_eq!(NetworkRequest::Requested.to_string(), "requested");
    assert_eq!(NetworkRequest::NotRequested.to_string(), "not-requested");
    assert_eq!(
        "requested".parse::<NetworkRequest>()?,
        NetworkRequest::Requested
    );
    assert_eq!(
        "not-requested".parse::<NetworkRequest>()?,
        NetworkRequest::NotRequested
    );
    assert!(matches!(
        "open".parse::<NetworkRequest>(),
        Err(error) if error.to_string().contains("requested")
    ));
    Ok(())
}

fn seen_values(seen: &Mutex<Vec<String>>) -> Vec<String> {
    seen.lock().clone()
}

#[test]
fn each_provisioner_port_reaches_the_selected_plugin_with_its_options() -> Result<(), Box<dyn Error>>
{
    let text = CONFIG.replace(BETA_SECTION, ALPHA_SECTION);
    let provisioner = Provisioner::build(parse(&text)?.provisioner, &context())?;
    let Provisioner::Alpha(alpha) = &provisioner else {
        return Err("expected the alpha provisioner".into());
    };
    let fleet = test_fleet(ProvisionerFleetOptions::Alpha(FakeFleetOptions {
        capacity: "spot".to_owned(),
        machine_class: Some("n2".to_owned()),
    }))?;

    let mut supply = FleetSupply::default();
    ready(provisioner.supply(fleet.view(), &mut supply))?;
    ready(provisioner.set_desired_capacity(fleet.view(), 1))?;
    assert_eq!(seen_values(&alpha.seen), ["spot", "spot"]);

    let provisioner = Provisioner::build(parse(CONFIG)?.provisioner, &context())?;
    let Provisioner::Beta(beta) = &provisioner else {
        return Err("expected the beta provisioner".into());
    };
    let fleet = test_fleet(ProvisionerFleetOptions::Beta(OtherFleetOptions {}))?;
    let mut supply = FleetSupply::default();
    ready(provisioner.supply(fleet.view(), &mut supply))?;
    assert_eq!(seen_values(&beta.seen), [""]);
    Ok(())
}

#[test]
fn foreign_fleet_options_are_rejected_without_panicking() -> Result<(), Box<dyn Error>> {
    let provisioner = Provisioner::build(parse(CONFIG)?.provisioner, &context())?;
    assert!(matches!(provisioner, Provisioner::Beta(_)));
    let fleet = test_fleet(ProvisionerFleetOptions::Alpha(FakeFleetOptions {
        capacity: "spot".to_owned(),
        machine_class: None,
    }))?;

    let mut supply = FleetSupply::default();
    assert!(matches!(
        ready(provisioner.supply(fleet.view(), &mut supply)),
        Err(PortError::Config(_))
    ));
    assert!(matches!(
        ready(provisioner.set_desired_capacity(fleet.view(), 2)),
        Err(PortError::Config(_))
    ));
    let gone = instance(1)?;
    assert!(matches!(
        ready(provisioner.terminate(fleet.view(), &gone)),
        Err(PortError::Config(_))
    ));
    Ok(())
}

#[test]
fn provisioner_options_list_every_plugin_by_key() {
    let listing = Plugins::provisioner_options();
    let keys: Vec<&str> = listing.iter().map(|(key, _)| *key).collect();
    assert_eq!(keys, ["alpha", "beta"]);

    let alpha: Vec<(&str, bool)> = listing[0]
        .1
        .iter()
        .map(|spec| (spec.key, spec.required))
        .collect();
    assert_eq!(alpha, [("capacity", true), ("machine_class", false)]);
    assert!(listing[1].1.is_empty());
}

fn fleet_options_error(text: &str) -> Result<String, Box<dyn Error>> {
    let error = parse(text).err().ok_or("expected a configuration error")?;
    match error.downcast_ref::<ConfigError>() {
        Some(ConfigError::FleetOptions { fleet, source }) => Ok(format!("{fleet}: {source}")),
        _ => Err(format!("unexpected error: {error}").into()),
    }
}

#[test]
fn unknown_fleet_option_is_rejected_with_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let text = CONFIG.replace(
        BETA_SECTION,
        "[fleets.options]\ncapacity = \"spot\"\nbogus = 1\n\n[provisioner.alpha]\nnumber = 1",
    );
    let message = fleet_options_error(&text)?;
    assert!(
        message.contains("workers")
            && message.contains("bogus")
            && message.contains("capacity")
            && message.contains("machine_class"),
        "{message}"
    );
    Ok(())
}

#[test]
fn missing_required_fleet_option_is_rejected() -> Result<(), Box<dyn Error>> {
    let text = CONFIG.replace(
        BETA_SECTION,
        "[fleets.options]\nmachine_class = \"n2\"\n\n[provisioner.alpha]\nnumber = 1",
    );
    let message = fleet_options_error(&text)?;
    assert!(message.contains("capacity"), "{message}");
    Ok(())
}

#[test]
fn reserved_machine_class_needs_on_demand_capacity() -> Result<(), Box<dyn Error>> {
    let text = CONFIG.replace(
        BETA_SECTION,
        "[fleets.options]\ncapacity = \"spot\"\nmachine_class = \"reserved\"\n\n[provisioner.alpha]\nnumber = 1",
    );
    let message = fleet_options_error(&text)?;
    assert!(
        message.contains("workers")
            && message.contains("machine class reserved needs capacity on-demand"),
        "{message}"
    );
    Ok(())
}

#[test]
fn reserved_machine_class_with_on_demand_capacity_parses() -> Result<(), Box<dyn Error>> {
    let text = CONFIG.replace(
        BETA_SECTION,
        "[fleets.options]\ncapacity = \"on-demand\"\nmachine_class = \"reserved\"\n\n[provisioner.alpha]\nnumber = 1",
    );
    parse(&text)?;
    Ok(())
}

#[test]
fn valid_fleet_options_parse_into_the_fleet_properties() -> Result<(), Box<dyn Error>> {
    let config = parse(&CONFIG.replace(BETA_SECTION, ALPHA_SECTION))?;
    let properties = config.fleets.catalog().fleets();
    assert_eq!(properties[0].get("capacity"), Some("spot"));
    Ok(())
}

fn provider_names(settings: &impl AuthenticationSettings) -> Vec<&str> {
    settings
        .providers()
        .iter()
        .map(ProviderName::as_str)
        .collect()
}

#[test]
fn client_authentication_section_parses_the_selected_plugin() -> Result<(), Box<dyn Error>> {
    let config: ClientAuthenticationConfig = toml::from_str("[token]\nnumber = 1")?;
    assert!(config.requires_tls());
    assert_eq!(provider_names(&config), ["example-idp"]);

    let open: ClientAuthenticationConfig = toml::from_str("[none]\nnumber = 2")?;
    assert!(!open.requires_tls());
    assert!(provider_names(&open).is_empty());
    Ok(())
}

#[test]
fn client_authentication_rejects_two_sections() {
    let text = "[token]\nnumber = 1\n[none]\nnumber = 2";
    assert!(toml::from_str::<ClientAuthenticationConfig>(text).is_err());
}

#[test]
fn client_authentication_rejects_an_unknown_key_and_lists_the_accepted_ones() {
    let message = toml::from_str::<ClientAuthenticationConfig>("[bogus]\nnumber = 1")
        .expect_err("an unknown key is accepted")
        .to_string();
    assert!(
        message.contains("token") && message.contains("none"),
        "{message}"
    );
}

#[test]
fn worker_authentication_rejects_an_unknown_key_and_lists_the_accepted_ones() {
    let message = toml::from_str::<WorkerAuthenticationConfig>("[bogus]\nnumber = 1")
        .expect_err("an unknown key is accepted")
        .to_string();
    assert!(
        message.contains("mtls") && message.contains("none"),
        "{message}"
    );
}

#[test]
fn client_authentication_builds_the_selected_plugin_and_reports_its_scheme()
-> Result<(), Box<dyn Error>> {
    let token: ClientAuthenticationConfig = toml::from_str("[token]\nnumber = 1")?;
    let plugin = ClientAuthentication::build(token, &context())?;
    assert!(matches!(plugin.scheme(), Scheme::Mtls));

    let none: ClientAuthenticationConfig = toml::from_str("[none]\nnumber = 2")?;
    let plugin = ClientAuthentication::build(none, &context())?;
    assert!(matches!(plugin.scheme(), Scheme::None));
    Ok(())
}

#[test]
fn composed_types_report_their_domain_keys() {
    assert_eq!(Provisioner::DOMAIN, "provisioner");
    assert_eq!(Cache::DOMAIN, "cache");
    assert_eq!(Signer::DOMAIN, "signer");
    assert_eq!(Sandbox::DOMAIN, "sandbox");
    assert_eq!(ClientAuthentication::DOMAIN, "client_authentication");
    assert_eq!(WorkerAuthentication::DOMAIN, "worker_authentication");
}

#[test]
fn config_enums_report_the_key_of_the_selected_plugin() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    assert_eq!(config.provisioner.key(), "beta");
    assert_eq!(config.cache.key(), "disk");
    assert_eq!(config.signer.key(), "kms");

    let alpha = parse(&CONFIG.replace(BETA_SECTION, ALPHA_SECTION))?;
    assert_eq!(alpha.provisioner.key(), "alpha");
    Ok(())
}

#[test]
fn authentication_enums_delegate_to_the_selected_plugin_config() -> Result<(), Box<dyn Error>> {
    let tls: AuthTlsConfig = toml::from_str("number = 1")?;
    let client: ClientAuthenticationConfig = toml::from_str("[token]\nnumber = 1")?;
    assert_eq!(client.requires_tls(), tls.requires_tls());
    assert_eq!(provider_names(&client), provider_names(&tls));

    let open: AuthOpenConfig = toml::from_str("number = 2")?;
    let worker: WorkerAuthenticationConfig = toml::from_str("[none]\nnumber = 2")?;
    assert_eq!(worker.requires_tls(), open.requires_tls());
    assert_eq!(provider_names(&worker), provider_names(&open));
    Ok(())
}

#[test]
fn sandbox_keys_round_trip_through_names_and_serde() -> Result<(), Box<dyn Error>> {
    #[derive(Deserialize, Serialize)]
    struct Holder {
        sandbox: SandboxKey,
    }

    assert_eq!(SandboxKey::ALL, [SandboxKey::Direct, SandboxKey::Boxed]);
    assert_eq!(SandboxKey::NAMES, ["direct", "boxed"]);
    for key in SandboxKey::ALL {
        assert_eq!(key.as_str().parse::<SandboxKey>()?, key);
        assert_eq!(key.to_string(), key.as_str());
    }
    let error = "tcp"
        .parse::<SandboxKey>()
        .err()
        .ok_or("expected an unknown-name error")?;
    assert_eq!(error.input(), "tcp");
    assert_eq!(error.expected(), SandboxKey::NAMES);

    let text = toml::to_string(&Holder {
        sandbox: SandboxKey::Boxed,
    })?;
    assert_eq!(text, "sandbox = \"boxed\"\n");
    assert_eq!(toml::from_str::<Holder>(&text)?.sandbox, SandboxKey::Boxed);
    assert!(toml::from_str::<Holder>("sandbox = \"tcp\"").is_err());
    Ok(())
}
