//! `gang worker`: composes one worker from the configured plugins and runs it until shutdown.

use std::process::ExitCode;
use std::sync::Arc;

use gang_client::ClientOptions;
use gang_clock::{Clock, SystemClock};
use gang_config::{GangConfig, WorkerConfig};
use gang_spi::{PluginContext, SchedulerPort};
use gang_types::{AuthenticationSettings, ProviderName, SchedulerSettings, Url};
use gang_worker::{Transport, WorkerDeployment, WorkerParts, WorkerSettings};

use crate::cli::WorkerArgs;
use crate::config_file::LoadedConfig;
use crate::enroller::WorkloadEnroller;
use crate::errors::{MainError, Result};
use crate::logging;
use crate::output::Output;
use crate::plugins::{
    Cache, Limits, Plugins, Provisioner, Scheduler, SchedulerConfig, WorkerAuthenticationConfig,
};
use crate::runtime;
use crate::worker_server::{AdvertisedCa, ServerPin, WorkerServer};

/// The adapter types a `gang worker` runs on, as `gang_worker` names them.
struct GangWorker;

impl WorkerDeployment for GangWorker {
    type Provisioner = Provisioner;
    type Cache = Cache;
    type Scheduler = Scheduler;
    type Limits = Limits;
    type Clock = SystemClock;
    type Enroller = WorkloadEnroller;
    type ServerCa = Option<AdvertisedCa>;
}

/// Runs `gang worker` with the configuration and endpoint flags in `args` until shutdown.
pub(crate) fn run(args: WorkerArgs, out: &Output) -> Result<ExitCode> {
    logging::init(out.stderr_ansi())?;
    let loaded = LoadedConfig::load(&args.config)?;
    runtime::block_on(serve(loaded, args.endpoint, args.worker_endpoint))
}

/// Builds the worker from `loaded` and runs it until shutdown, preemption, or a scheduler stop.
/// The endpoint flags override the configured `endpoint` and scheduler `worker_endpoint`.
async fn serve(
    loaded: LoadedConfig,
    endpoint_flag: Option<Url>,
    worker_endpoint_flag: Option<Url>,
) -> Result<ExitCode> {
    let shutdown = gang_os::shutdown_signal().map_err(MainError::Signals)?;
    let LoadedConfig { path, config, .. } = loaded;
    let GangConfig {
        fleets,
        worker,
        sandbox,
        execution,
        limits,
        endpoint: configured_endpoint,
        authentication,
        server,
        provisioner,
        cache,
        signer,
        scheduler,
        ..
    } = config;
    let context = PluginContext { config_path: &path };
    let endpoint = effective_endpoint(endpoint_flag, configured_endpoint);
    let access = server_access(&authentication.workers, endpoint)?;
    let pin = ServerPin::try_new(server.ca_pin, signer, &context)?;
    let scheduler = scheduler_with_worker_endpoint(scheduler, worker_endpoint_flag);
    let provisioner = Provisioner::build(provisioner, &context)?;
    let cache = Cache::build(cache, &context)?;
    let scheduler = Scheduler::build(scheduler, &context)?;
    let transport = worker_transport(
        access,
        &SchedulerPort::endpoints(&scheduler).worker,
        pin,
        enrollment_options(&worker),
        Arc::new(SystemClock::new()),
    )?;

    let settings = WorkerSettings {
        fleets,
        worker,
        sandbox,
        execution,
        limits,
        executable: std::env::current_exe().map_err(MainError::CurrentExecutable)?,
        // Unset PATH resolves sandbox programs to nothing; the worker then fails to start.
        search_path: std::env::var_os("PATH").unwrap_or_default(),
    };
    let parts = WorkerParts::<GangWorker> {
        provisioner,
        cache,
        scheduler,
        clock: SystemClock::new(),
        transport,
    };
    Box::pin(gang_worker::run::<GangWorker, Plugins>(
        parts,
        settings,
        |config| Limits::build(config, &context).map_err(|error| error.source),
        shutdown,
    ))
    .await?;
    Ok(ExitCode::SUCCESS)
}

/// The client settings of the worker's requests to the server: enrollment and the server-only CA fetch, from [worker].
#[must_use]
fn enrollment_options(worker: &WorkerConfig) -> ClientOptions {
    ClientOptions {
        http_timeout: worker.enrollment_request_timeout,
        max_response_size: worker.enrollment_max_response_size,
        ..ClientOptions::default()
    }
}

/// Where a worker reaches the server on its public endpoint, and whether it enrolls there.
enum ServerAccess {
    /// Mutual TLS: the worker enrolls through `provider` at `endpoint`.
    Enroll {
        provider: ProviderName,
        endpoint: Url,
    },
    /// No enrollment; the endpoint, when known, publishes the server CA.
    Anonymous(Option<Url>),
}

/// The worker's access to the server under `workers`.
///
/// # Errors
///
/// Returns [`MainError::WorkerEndpointMissing`] when worker authentication enrolls and no public
/// endpoint is known.
fn server_access(
    workers: &WorkerAuthenticationConfig,
    endpoint: Option<Url>,
) -> Result<ServerAccess> {
    match workers.enrollment_provider() {
        Some(provider) => Ok(ServerAccess::Enroll {
            provider: provider.clone(),
            endpoint: endpoint.ok_or(MainError::WorkerEndpointMissing)?,
        }),
        None => Ok(ServerAccess::Anonymous(endpoint)),
    }
}

/// Selects the worker transport: mutual TLS when worker authentication enrolls, server-only TLS
/// for a `grpcs` worker endpoint, plain gRPC otherwise. Performs no I/O: the server CA pin and
/// the server CA are resolved on first use, once the node serves.
///
/// # Errors
///
/// Returns [`MainError::WorkerEndpointMissing`] if a pinned server-only transport has no public
/// endpoint.
fn worker_transport(
    access: ServerAccess,
    worker_endpoint: &Url,
    pin: ServerPin,
    options: ClientOptions,
    clock: Arc<dyn Clock>,
) -> Result<Transport<WorkloadEnroller, Option<AdvertisedCa>>> {
    match access {
        ServerAccess::Enroll { provider, endpoint } => Ok(Transport::Mutual(
            WorkloadEnroller::new(WorkerServer::new(endpoint, pin, options, clock), provider),
        )),
        ServerAccess::Anonymous(_) if worker_endpoint.scheme() != "grpcs" => Ok(Transport::Plain),
        ServerAccess::Anonymous(None) if pin.is_pinned() => Err(MainError::WorkerEndpointMissing),
        ServerAccess::Anonymous(endpoint) => {
            Ok(Transport::ServerOnly(endpoint.map(|endpoint| {
                AdvertisedCa::new(WorkerServer::new(endpoint, pin, options, clock))
            })))
        }
    }
}

/// The public endpoint the worker enrolls with: the `--endpoint` flag when given, otherwise the
/// configured `endpoint`.
fn effective_endpoint(flag: Option<Url>, configured: Option<Url>) -> Option<Url> {
    flag.or(configured)
}

/// Applies `--worker-endpoint` to the scheduler, replacing any configured worker endpoint.
/// Without the flag the scheduler is returned unchanged.
fn scheduler_with_worker_endpoint(
    mut scheduler: SchedulerConfig,
    flag: Option<Url>,
) -> SchedulerConfig {
    if let Some(url) = flag {
        scheduler.set_worker_endpoint(Some(url));
    }
    scheduler
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use gang_authn::{MtlsConfig, NoneConfig};
    use gang_clock::{TestClock, Timestamp};
    use gang_config::CaPinSetting;
    use gang_nativelink::NativeLinkConfig;
    use gang_types::{CaPin, NonZeroBytes, NonZeroDuration, Url};

    use super::*;
    use crate::signer_pin::file_signer;

    fn endpoint() -> Url {
        Url::parse("https://127.0.0.1:8443").expect("test endpoint is a valid url")
    }

    fn clock() -> Arc<dyn Clock> {
        Arc::new(TestClock::new(Timestamp::UNIX_EPOCH))
    }

    #[test]
    fn enrollment_options_take_the_worker_section() {
        let worker = WorkerConfig {
            enrollment_request_timeout: NonZeroDuration::from_secs(23),
            enrollment_max_response_size: NonZeroBytes::mib(3),
            ..WorkerConfig::default()
        };
        let options = enrollment_options(&worker);
        assert_eq!(options.http_timeout, NonZeroDuration::from_secs(23));
        assert_eq!(options.max_response_size, NonZeroBytes::mib(3));
    }

    #[test]
    fn no_worker_authentication_over_grpc_is_plain() {
        let transport = worker_transport(
            server_access(
                &WorkerAuthenticationConfig::None(NoneConfig::default()),
                None,
            )
            .expect("anonymous access needs no endpoint"),
            &url("grpc://127.0.0.1:9090"),
            ServerPin::SystemRoots,
            ClientOptions::default(),
            clock(),
        )
        .expect("plain transport needs no network");
        assert!(matches!(transport, Transport::Plain));
    }

    #[test]
    fn no_worker_authentication_over_grpcs_uses_system_roots_without_public_endpoint() {
        let transport = worker_transport(
            server_access(
                &WorkerAuthenticationConfig::None(NoneConfig::default()),
                None,
            )
            .expect("anonymous access needs no endpoint"),
            &url("grpcs://127.0.0.1:9090"),
            ServerPin::SystemRoots,
            ClientOptions::default(),
            clock(),
        )
        .expect("server-only transport needs no public endpoint");
        assert!(matches!(transport, Transport::ServerOnly(None)));
    }

    #[test]
    fn a_pinned_server_only_transport_without_an_endpoint_is_rejected() {
        let result = worker_transport(
            server_access(
                &WorkerAuthenticationConfig::None(NoneConfig::default()),
                None,
            )
            .expect("anonymous access needs no endpoint"),
            &url("grpcs://127.0.0.1:9090"),
            ServerPin::Pinned(CaPin::new([0x42; 32])),
            ClientOptions::default(),
            clock(),
        );
        assert!(matches!(result, Err(MainError::WorkerEndpointMissing)));
    }

    #[test]
    fn mtls_worker_authentication_builds_mutual_transport() {
        let transport = worker_transport(
            server_access(&mtls(), Some(endpoint())).expect("endpoint is available"),
            &url("grpcs://127.0.0.1:9090"),
            ServerPin::SystemRoots,
            ClientOptions::default(),
            clock(),
        )
        .expect("the enroller is built without I/O");
        assert!(matches!(transport, Transport::Mutual(_)));
    }

    #[test]
    fn mtls_worker_authentication_without_an_endpoint_is_rejected() {
        let result = server_access(&mtls(), None);
        assert!(matches!(result, Err(MainError::WorkerEndpointMissing)));
    }

    #[test]
    fn selecting_a_signer_pinned_transport_reads_no_signer_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let context = PluginContext {
            config_path: dir.path(),
        };
        for workers in [
            mtls(),
            WorkerAuthenticationConfig::None(NoneConfig::default()),
        ] {
            let pin = ServerPin::try_new(
                Some(CaPinSetting::Signer),
                file_signer(dir.path()),
                &context,
            )
            .expect("signer builds");
            let result = worker_transport(
                server_access(&workers, Some(endpoint())).expect("endpoint is available"),
                &url("grpcs://127.0.0.1:9090"),
                pin,
                ClientOptions::default(),
                clock(),
            );
            assert!(result.is_ok());
        }
        assert!(!dir.path().join("signer").exists());
    }

    #[test]
    fn selecting_a_server_only_transport_calls_no_server() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener binds");
        listener
            .set_nonblocking(true)
            .expect("listener is nonblocking");
        let endpoint = url(&format!(
            "https://{}",
            listener.local_addr().expect("listener address")
        ));
        let result = worker_transport(
            server_access(
                &WorkerAuthenticationConfig::None(NoneConfig::default()),
                Some(endpoint),
            )
            .expect("endpoint is available"),
            &url("grpcs://127.0.0.1:9090"),
            ServerPin::Pinned(CaPin::new([0x42; 32])),
            ClientOptions::default(),
            clock(),
        );
        assert!(matches!(result, Ok(Transport::ServerOnly(Some(_)))));
        assert_eq!(
            listener.accept().expect_err("no server connection").kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    fn flag_endpoint() -> Url {
        Url::parse("https://10.0.0.5:8443").expect("test endpoint is a valid url")
    }

    fn mtls() -> WorkerAuthenticationConfig {
        WorkerAuthenticationConfig::Mtls(MtlsConfig {
            provider: ProviderName::try_new("okta").expect("test provider is a valid name"),
            certificate_ttl: NonZeroDuration::from_hours(1),
            max_cached_certificates: NonZeroUsize::new(8).expect("8 is non-zero"),
        })
    }

    #[test]
    fn endpoint_flag_takes_precedence_over_the_configured_endpoint() {
        assert_eq!(
            effective_endpoint(Some(flag_endpoint()), Some(endpoint())),
            Some(flag_endpoint())
        );
    }

    #[test]
    fn configured_endpoint_is_used_without_a_flag() {
        assert_eq!(effective_endpoint(None, Some(endpoint())), Some(endpoint()));
    }

    #[test]
    fn endpoint_flag_enrolls_under_mtls_without_a_configured_endpoint() {
        let endpoint = effective_endpoint(Some(flag_endpoint()), None);
        let transport = worker_transport(
            server_access(&mtls(), endpoint).expect("endpoint is available"),
            &url("grpcs://127.0.0.1:9090"),
            ServerPin::SystemRoots,
            ClientOptions::default(),
            clock(),
        )
        .expect("the enroller is built without I/O");
        assert!(matches!(transport, Transport::Mutual(_)));
    }

    #[test]
    fn mtls_without_a_flag_or_configured_endpoint_is_rejected() {
        let endpoint = effective_endpoint(None, None);
        let result = server_access(&mtls(), endpoint);
        assert!(matches!(result, Err(MainError::WorkerEndpointMissing)));
    }

    fn url(text: &str) -> Url {
        Url::parse(text).expect("test endpoint is a valid url")
    }

    fn native_config(worker_endpoint: Option<&str>) -> NativeLinkConfig {
        let worker_line = worker_endpoint
            .map(|endpoint| format!("worker_endpoint = \"{endpoint}\"\n"))
            .unwrap_or_default();
        let text = format!("scheduler_name = \"MAIN_SCHEDULER\"\n{worker_line}");
        toml::from_str(&text).expect("the minimal scheduler section parses")
    }

    #[test]
    fn worker_endpoint_flag_replaces_the_configured_endpoint() {
        let configured = native_config(Some("grpcs://a.example.com:8443"));
        let result = scheduler_with_worker_endpoint(
            SchedulerConfig::Nativelink(configured),
            Some(url("grpcs://b.example.com:8443")),
        );
        assert_eq!(
            result.worker_endpoint(),
            Some(&url("grpcs://b.example.com:8443"))
        );
    }

    #[test]
    fn no_worker_endpoint_flag_keeps_the_configured_endpoint() {
        let result = scheduler_with_worker_endpoint(
            SchedulerConfig::Nativelink(native_config(Some("grpcs://a.example.com:8443"))),
            None,
        );
        assert_eq!(
            result.worker_endpoint(),
            Some(&url("grpcs://a.example.com:8443"))
        );
    }

    #[test]
    fn no_worker_endpoint_flag_keeps_an_absent_endpoint_absent() {
        let absent = SchedulerConfig::Nativelink(native_config(None));
        let result = scheduler_with_worker_endpoint(absent, None);
        assert_eq!(result.worker_endpoint(), None);
    }

    #[test]
    fn worker_endpoint_flag_fills_an_absent_endpoint() {
        let absent = SchedulerConfig::Nativelink(native_config(None));
        let result =
            scheduler_with_worker_endpoint(absent, Some(url("grpcs://b.example.com:8443")));
        assert_eq!(
            result.worker_endpoint(),
            Some(&url("grpcs://b.example.com:8443"))
        );
    }
}
