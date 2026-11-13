//! `gang server`: composes the control loop, the certificate endpoint and the listeners from the
//! configured plugins, announces how clients reach it once ready, and runs until shutdown.

use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use gang_clock::SystemClock;
use gang_config::{
    AuthenticationConfig, DashboardConfig, GangConfig, IdentityConfig, PluginSet, ServerConfig,
    Transport,
};
use gang_server::{
    HandshakeSettings, Listeners, LoginSettings, Parts, Provider, Providers, PublicEndpoint,
    RequestLimits, Server, ServerSettings, TlsSettings, TransportSettings,
};
use gang_spi::{
    ControllerReady, LoginClient, PluginContext, ProvisionerPort, ServerEndpoints, Sidecar,
};
use gang_types::announcement::{Announcement, ListenerRole};
use gang_types::{Isa, NonZeroDuration, ProviderName, SchedulerSettings, Url};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::config_file::LoadedConfig;
use crate::errors::{MainError, Result};
use crate::logging;
use crate::output::Output;
use crate::plugins::{
    Cache, ClientAuthentication, Database, IdentityProvider, Plugins, Provisioner, Scheduler,
    SchedulerConfig, Signer, WorkerAuthentication,
};
use crate::runtime;

/// The configuration section of one identity provider plugin.
type ProviderConfig = <Plugins as PluginSet>::IdentityProvider;

/// The configuration section of the client authentication plugin.
type ClientAuthenticationConfig = <Plugins as PluginSet>::ClientAuthentication;

/// The configuration section of the worker authentication plugin.
type WorkerAuthenticationConfig = <Plugins as PluginSet>::WorkerAuthentication;

/// The composed server's adapter types, as `gang_server` names them.
struct GangDeployment;

impl gang_server::Stack for GangDeployment {
    type Signer = Signer;
    type Clock = SystemClock;
    type Repository = Database;
    type Identity = IdentityProvider;
    type Clients = ClientAuthentication;
    type Workers = WorkerAuthentication;
}

impl gang_server::Deployment for GangDeployment {
    type Plugins = Plugins;
    type Provisioner = Provisioner;
    type Cache = Cache;
    type Scheduler = Scheduler;
    type Listener = TcpListener;
}

/// Runs `gang server` with the configuration at `config` until shutdown.
pub(crate) fn run(config: &Path, out: &Output) -> Result<ExitCode> {
    logging::init(out.stderr_ansi())?;
    let loaded = LoadedConfig::load(config)?;
    runtime::block_on(serve(loaded, out))
}

/// Builds the server from `loaded`, announces how it is reached once ready, and runs it until
/// shutdown.
async fn serve(loaded: LoadedConfig, out: &Output) -> Result<ExitCode> {
    let shutdown = gang_os::shutdown_signal().map_err(MainError::Signals)?;
    let LoadedConfig { path, config, .. } = loaded;
    let GangConfig {
        poll_interval,
        client_action_timeout,
        default_isa,
        listen,
        worker_listen,
        endpoint,
        fleets,
        identity,
        authentication,
        server,
        provisioner,
        cache,
        signer,
        scheduler,
        database,
        dashboard,
        ..
    } = config;
    let context = PluginContext { config_path: &path };
    let login = dashboard_login(&dashboard).await?;

    let listeners = bind_listeners(listen, worker_listen).await?;
    let (public_address, worker_address) = bound_addresses(&listeners)?;
    let endpoint = match endpoint {
        Some(url) => url,
        None => server.transport.kind().public_url(public_address)?,
    };
    let derived_worker_endpoint = server.transport.kind().worker_url(worker_address)?;
    let (scheduler, worker_endpoint) = resolve_worker_endpoint(scheduler, derived_worker_endpoint);

    let provisioner_name = provisioner.key();
    let fleet = Provisioner::build(provisioner, &context)?;
    let sidecar = fleet
        .attach(&ServerEndpoints {
            public: endpoint.clone(),
            worker: worker_endpoint,
        })
        .await
        .map_err(|source| MainError::Plugin {
            domain: Provisioner::DOMAIN,
            source,
        })?;
    let sidecar_address = sidecar.as_ref().map(Sidecar::local_addr);
    let cache = Cache::build(cache, &context)?;
    let signer = Signer::build(signer, &context)?;
    let scheduler = Scheduler::build(scheduler, &context)?;
    let repository = Database::build(database, &context)?;
    let (clients, workers) = build_authentication(authentication, &context)?;
    let transport = transport_settings(&server);
    let providers = identity_providers(identity, &context)?;

    let settings = server_settings(
        SettingsSource {
            poll_interval,
            client_action_timeout,
            default_isa,
            endpoint: endpoint.clone(),
            transport,
            dashboard,
            login,
            provisioner: provisioner_name,
        },
        server,
    )?;

    let server = Box::pin(Server::<GangDeployment>::prepare(
        Parts {
            provisioner: fleet,
            fleets,
            cache,
            signer,
            scheduler,
            repository,
            providers,
            clock: Arc::new(SystemClock::new()),
            listeners,
            clients,
            workers,
        },
        settings,
    ))
    .await?;

    let startup = match server.ca_pin() {
        Some(pin) => Announcement::CaPin(pin),
        None => Announcement::Plaintext(endpoint),
    };
    let announcements = announcements(public_address, worker_address, sidecar_address, startup);
    run_announced(server, shutdown, sidecar, out, &announcements).await?;

    Ok(ExitCode::SUCCESS)
}

/// Runs `server` and the provisioner's sidecar until shutdown, printing `announcements` once the
/// controller is ready.
async fn run_announced(
    server: Server<GangDeployment>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    sidecar: Option<Sidecar>,
    out: &Output,
    announcements: &[Announcement],
) -> Result<()> {
    let (ready, readiness) = ControllerReady::channel();
    let announced = async {
        announce_when_ready(readiness, out, announcements).await?;
        std::future::pending::<Result<()>>().await
    };
    tokio::select! {
        result = Box::pin(server.run(shutdown, ready)) => result?,
        result = serve_sidecar(sidecar) => result?,
        result = announced => result?,
    }

    Ok(())
}

/// The addresses the public and worker listeners are bound to.
fn bound_addresses(listeners: &Listeners<TcpListener>) -> Result<(SocketAddr, SocketAddr)> {
    let public = listeners
        .public
        .local_addr()
        .map_err(MainError::LocalAddress)?;
    let worker = listeners
        .worker
        .local_addr()
        .map_err(MainError::LocalAddress)?;
    Ok((public, worker))
}

/// The configured values that `serve` builds the server settings from, besides the `[server]`
/// section.
struct SettingsSource {
    /// How often the control loop runs a pass.
    poll_interval: NonZeroDuration,
    /// How long a client action may take before it is timed out.
    client_action_timeout: NonZeroDuration,
    /// The interface standard the server's default isa is.
    default_isa: Isa,
    /// The endpoint clients reach the public listener at.
    endpoint: Url,
    /// How accepted connections are handled.
    transport: TransportSettings,
    /// The operator dashboard's configuration.
    dashboard: DashboardConfig,
    /// The browser sign-in of the dashboard, with its client secret already loaded.
    login: Option<LoginSettings>,
    /// The key of the configured provisioner section.
    provisioner: &'static str,
}

/// Builds the server settings from the configured values and the `[server]` section.
fn server_settings(source: SettingsSource, server: ServerConfig) -> Result<ServerSettings> {
    Ok(ServerSettings {
        poll_interval: source.poll_interval,
        client_action_timeout: source.client_action_timeout,
        default_isa: source.default_isa,
        endpoint: PublicEndpoint::try_new(source.endpoint)?,
        transport: source.transport,
        roles: server.roles,
        limits: RequestLimits {
            max_request_size: usize::try_from(server.max_request_size.get()).unwrap_or(usize::MAX),
            request_timeout: server.request_timeout,
        },
        rate_limit: server.rate_limit,
        principal_seen_interval: server.principal_seen_interval,
        max_tracked_principals: server.max_tracked_principals,
        accept_error_backoff: server.accept_error_backoff,
        dashboard: source.dashboard,
        login: source.login,
        provisioner: source.provisioner.into(),
    })
}

/// Loads the client secret of `[dashboard.login]` once and builds the dashboard's browser sign-in
/// from it. `None` when the dashboard has no login.
async fn dashboard_login(dashboard: &DashboardConfig) -> Result<Option<LoginSettings>> {
    let Some(login) = &dashboard.login else {
        return Ok(None);
    };
    let source = login.client_secret.clone();
    let secret = tokio::task::spawn_blocking(move || source.load())
        .await
        .map_err(MainError::DashboardSecretTask)?
        .map_err(MainError::DashboardSecret)?;
    Ok(Some(LoginSettings {
        provider: login.provider.clone(),
        client: LoginClient {
            id: login.client_id.clone(),
            secret,
        },
        ttl: dashboard.login_ttl,
        max_pending: dashboard.max_pending_logins,
    }))
}

/// Builds the listener and startup announcements that mean the server is ready.
fn announcements(
    public: SocketAddr,
    worker: SocketAddr,
    imds: Option<SocketAddr>,
    startup: Announcement,
) -> Vec<Announcement> {
    let mut announcements = vec![
        Announcement::Listening(ListenerRole::Public, public),
        Announcement::Listening(ListenerRole::Worker, worker),
    ];
    if let Some(address) = imds {
        announcements.push(Announcement::Listening(ListenerRole::Imds, address));
    }
    announcements.push(startup);
    announcements
}

/// Prints the announcements once the server is ready. If the controller never became ready,
/// prints nothing because `run` reports why.
async fn announce_when_ready(
    readiness: oneshot::Receiver<()>,
    out: &Output,
    announcements: &[Announcement],
) -> Result<()> {
    if readiness.await.is_ok() {
        for announcement in announcements {
            out.data(announcement.to_string())?;
        }
    }
    Ok(())
}

/// Fills an omitted scheduler worker endpoint with `derived` and returns the resolved endpoint.
fn resolve_worker_endpoint(scheduler: SchedulerConfig, derived: Url) -> (SchedulerConfig, Url) {
    let worker_endpoint = scheduler
        .worker_endpoint()
        .cloned()
        .unwrap_or_else(|| derived.clone());
    (scheduler.with_worker_endpoint(derived), worker_endpoint)
}

/// Builds the client and worker authentication plugins.
fn build_authentication(
    authentication: AuthenticationConfig<ClientAuthenticationConfig, WorkerAuthenticationConfig>,
    context: &PluginContext<'_>,
) -> Result<(ClientAuthentication, WorkerAuthentication)> {
    let clients = ClientAuthentication::build(authentication.clients, context)?;
    let workers = WorkerAuthentication::build(authentication.workers, context)?;
    Ok((clients, workers))
}

/// Serves the sidecar the provisioner attached, when there is one, and never completes
/// otherwise. The sidecar is required: when it stops, the server fails.
async fn serve_sidecar(sidecar: Option<Sidecar>) -> Result<()> {
    let Some(sidecar) = sidecar else {
        return std::future::pending().await;
    };
    Err(MainError::Sidecar(sidecar.serve().await))
}

/// Builds the transport the `[server]` section selects.
fn transport_settings(server: &ServerConfig) -> TransportSettings {
    match &server.transport {
        Transport::Plaintext => TransportSettings::Plaintext,
        Transport::Tls { trust_domain } => TransportSettings::Tls(TlsSettings {
            trust_domain: trust_domain.clone(),
            certificate_backdate: server.certificate_backdate,
            certificate_ttl: server.certificate_ttl,
            reissue_at_percent: server.reissue_at_percent,
            handshake: HandshakeSettings {
                handshake_timeout: server.handshake_timeout,
                max_pending_handshakes: server.max_pending_handshakes,
                refusal_linger: server.refusal_linger,
            },
            endpoint_trust: server.endpoint_trust,
        }),
    }
}

/// Builds every configured identity provider: its verifier from its plugin section, and the
/// section clients see when the provider has an interactive login.
fn identity_providers(
    identity: IdentityConfig<ProviderConfig>,
    context: &PluginContext<'_>,
) -> Result<Providers<IdentityProvider>> {
    identity
        .providers
        .into_iter()
        .map(|(name, plugin)| provider(name, plugin, context))
        .collect()
}

/// Builds one identity provider from its plugin section.
fn provider(
    name: ProviderName,
    plugin: ProviderConfig,
    context: &PluginContext<'_>,
) -> Result<(ProviderName, Provider<IdentityProvider>)> {
    let section = serde_json::to_value(&plugin).map_err(|source| MainError::ProviderSection {
        provider: name.clone(),
        source,
    })?;
    let verifier = IdentityProvider::build(plugin, context)?;
    Ok((name, Provider::new(verifier, section)))
}

/// Binds the public and worker listeners.
async fn bind_listeners(public: SocketAddr, worker: SocketAddr) -> Result<Listeners<TcpListener>> {
    Ok(Listeners {
        public: bind(ListenerRole::Public, public).await?,
        worker: bind(ListenerRole::Worker, worker).await?,
    })
}

/// Binds the listening socket of `role` at `address`.
async fn bind(role: ListenerRole, address: SocketAddr) -> Result<TcpListener> {
    TcpListener::bind(address)
        .await
        .map_err(|source| MainError::Bind {
            role,
            address,
            source,
        })
}

#[cfg(test)]
mod tests {
    use gang_config::ServerTransport;
    use gang_nativelink::NativeLinkConfig;

    use super::*;

    /// A controller that fails before readiness produces no startup announcements.
    #[tokio::test]
    async fn nothing_is_announced_when_the_controller_never_becomes_ready() {
        let (out, stdout, _) = Output::captured();
        let (ready, readiness) = ControllerReady::channel();
        let announcements = announcements(
            bound(),
            loopback(),
            Some(wildcard()),
            Announcement::CaPin(gang_types::CaPin::new([7; 32])),
        );
        assert_eq!(
            announcements,
            vec![
                Announcement::Listening(ListenerRole::Public, bound()),
                Announcement::Listening(ListenerRole::Worker, loopback()),
                Announcement::Listening(ListenerRole::Imds, wildcard()),
                Announcement::CaPin(gang_types::CaPin::new([7; 32])),
            ]
        );
        drop(ready);

        announce_when_ready(readiness, &out, &announcements)
            .await
            .expect("a dropped readiness signal is not an output error");
        assert!(stdout.text().is_empty());
    }

    /// A ready controller prints each announcement exactly once and in order.
    #[tokio::test]
    async fn every_announcement_is_printed_once_the_controller_is_ready() {
        let (out, stdout, _) = Output::captured();
        let (ready, readiness) = ControllerReady::channel();
        let announcements = announcements(
            bound(),
            loopback(),
            None,
            Announcement::CaPin(gang_types::CaPin::new([7; 32])),
        );
        assert_eq!(
            announcements,
            vec![
                Announcement::Listening(ListenerRole::Public, bound()),
                Announcement::Listening(ListenerRole::Worker, loopback()),
                Announcement::CaPin(gang_types::CaPin::new([7; 32])),
            ]
        );
        ready.signal();

        announce_when_ready(readiness, &out, &announcements)
            .await
            .expect("announcements are written");
        let printed: Vec<Announcement> = stdout
            .text()
            .lines()
            .map(|line| line.parse().expect("each line is an announcement"))
            .collect();
        assert_eq!(printed, announcements);
    }

    fn bound() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 41000))
    }

    fn loopback() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 9090))
    }

    fn wildcard() -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], 9090))
    }

    fn workers_endpoint() -> Url {
        Url::parse("grpcs://workers.example.com:8443").expect("test endpoint is a valid url")
    }

    /// A scheduler section with the given `worker_endpoint`, or none when it is `None`.
    fn scheduler(worker_endpoint: Option<&str>) -> SchedulerConfig {
        let endpoint = worker_endpoint
            .map(|url| format!("worker_endpoint = \"{url}\"\n"))
            .unwrap_or_default();
        let config: NativeLinkConfig =
            toml::from_str(&format!("{endpoint}scheduler_name = \"MAIN_SCHEDULER\"\n"))
                .expect("the minimal scheduler section parses");
        SchedulerConfig::Nativelink(config)
    }

    fn worker_endpoint_of(scheduler: SchedulerConfig) -> Option<Url> {
        scheduler.worker_endpoint().cloned()
    }

    #[test]
    fn loopback_listen_derives_the_worker_endpoint_from_the_bound_address() {
        let expected = ServerTransport::Plaintext
            .worker_url(bound())
            .expect("the bound address forms a url");
        let (scheduler, url) = resolve_worker_endpoint(scheduler(None), expected.clone());
        assert_eq!(url, expected);
        assert_eq!(url.as_str(), "grpc://127.0.0.1:41000/");
        assert_eq!(worker_endpoint_of(scheduler), Some(expected));
    }

    #[test]
    fn configured_endpoint_is_returned_unchanged_for_a_non_loopback_listen() {
        let derived = ServerTransport::Plaintext
            .worker_url(bound())
            .expect("the bound address forms a url");
        let (scheduler, url) =
            resolve_worker_endpoint(scheduler(Some("grpcs://workers.example.com:8443")), derived);
        assert_eq!(url, workers_endpoint());
        assert_eq!(worker_endpoint_of(scheduler), Some(workers_endpoint()));
    }

    #[tokio::test]
    async fn bound_addresses_report_the_os_chosen_ports() {
        let public = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the public listener binds to an OS port");
        let worker = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the worker listener binds to an OS port");
        let expected = (
            public
                .local_addr()
                .expect("the public listener has an address"),
            worker
                .local_addr()
                .expect("the worker listener has an address"),
        );
        let listeners = Listeners { public, worker };

        let bound = bound_addresses(&listeners).expect("the bound addresses are readable");

        assert_eq!(bound, expected);
        assert_ne!(bound.0.port(), 0);
        assert_ne!(bound.1.port(), 0);
        assert_ne!(bound.0, bound.1);
    }
}
