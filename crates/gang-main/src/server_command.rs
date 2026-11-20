//! `gang server`: composes the control loop, the certificate endpoint and the listeners from the
//! configured plugins, announces how clients reach it once ready, and runs until shutdown.

use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use gang_clock::{Clock, SystemClock};
use gang_config::{
    AuthenticationConfig, DashboardConfig, GangConfig, IdentityConfig, PluginSet, ServerConfig,
    Transport,
};
use gang_nativelink::{NativeLink, NativeLinkConfig};
use gang_server::{
    DeviceSettings, HandshakeSettings, Listeners, LoginSettings, Parts, Provider, Providers,
    PublicEndpoint, RequestLimits, Server, ServerError, ServerSettings, TlsSettings,
    TransportSettings,
};
use gang_spi::{
    ControllerHostPort, ControllerId, ControllerReady, HostMilestone, LoginClient, PluginContext,
    PortError,
};
use gang_types::announcement::{Announcement, ListenerRole};
use gang_types::{Isa, NonZeroDuration, ProviderName, Url};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::config_file::LoadedConfig;
use crate::errors::{MainError, Result};
use crate::logging;
use crate::output::Output;
use crate::plugins::{
    Cache, ClientAuthentication, Database, IdentityProvider, Plugins, Provisioner, Signer,
    WorkerAuthentication,
};
/// The shutdown deadline elapsed; service managers must not restart a retiring controller. Distinct
/// from every other exit status (2 is usage, 3 a blocked deploy), so the unit can single it out.
const SHUTDOWN_DEADLINE_EXIT_CODE: u8 = 4;

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
    type Provisioner = Arc<Provisioner>;
    type Cache = Cache;
    type Scheduler = NativeLink;
    type Listener = TcpListener;
}

/// Generates an identifier unique to this controller process.
fn controller_id() -> Result<ControllerId> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(MainError::ControllerId)?;
    let controller = ControllerId::new(format!("{:032x}", u128::from_be_bytes(bytes)));
    tracing::info!(%controller, "starting controller");
    Ok(controller)
}

/// Runs `gang server` with the configuration at `config` until shutdown.
pub(crate) fn run(config: &Path, out: &Output) -> Result<ExitCode> {
    logging::init(out.stderr_ansi())?;
    let loaded = LoadedConfig::load(config)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(MainError::Runtime)?;
    let result = runtime.block_on(serve(loaded, out));
    runtime.shutdown_timeout(std::time::Duration::ZERO);
    shutdown_exit_status(result)
}

fn shutdown_exit_status(result: Result<ExitCode>) -> Result<ExitCode> {
    match result {
        Err(MainError::ShutdownTimedOut(error)) => {
            tracing::error!(%error, "shutdown deadline elapsed");
            Ok(ExitCode::from(SHUTDOWN_DEADLINE_EXIT_CODE))
        }
        Err(MainError::Server(ServerError::HandoffTimedOut(error))) => {
            tracing::error!(%error, "handoff deadline elapsed");
            Ok(ExitCode::from(SHUTDOWN_DEADLINE_EXIT_CODE))
        }
        other => other,
    }
}

/// Builds the server from `loaded`, announces how it is reached once ready, and runs it until
/// shutdown.
async fn serve(loaded: LoadedConfig, out: &Output) -> Result<ExitCode> {
    let shutdown = gang_os::shutdown_signal().map_err(MainError::Signals)?;
    let (stop, stopped) = oneshot::channel();
    let (prepared, preparation) = oneshot::channel();
    let work = Box::pin(serve_prepared(
        loaded,
        out,
        async move {
            let _ = stopped.await;
        },
        prepared,
    ));
    stop_during_preparation(shutdown, preparation, stop, work)
        .await
        .map(|result| result.unwrap_or(ExitCode::SUCCESS))
}

async fn stop_during_preparation<T>(
    shutdown: impl Future<Output = ()>,
    mut prepared: oneshot::Receiver<()>,
    stop: oneshot::Sender<()>,
    work: impl Future<Output = Result<T>>,
) -> Result<Option<T>> {
    tokio::pin!(work);
    tokio::select! {
        biased;
        () = shutdown => {
            if prepared.try_recv().is_ok() {
                let _ = stop.send(());
                work.await.map(Some)
            } else {
                Ok(None)
            }
        }
        result = &mut work => result.map(Some),
    }
}

async fn serve_prepared(
    loaded: LoadedConfig,
    out: &Output,
    shutdown: impl Future<Output = ()> + Send + 'static,
    prepared: oneshot::Sender<()>,
) -> Result<ExitCode> {
    let controller = controller_id()?;
    let LoadedConfig { path, config, .. } = loaded;
    let GangConfig {
        actions,
        poll_interval,
        client_action_timeout,
        default_isa,
        listen,
        worker_listen,
        health_listen,
        endpoint,
        worker_endpoint,
        fleets,
        identity,
        authentication,
        server,
        provisioner,
        cache,
        signer,
        database,
        dashboard,
        builds,
        ..
    } = config;
    let context = PluginContext { config_path: &path };
    let login = dashboard_login(&dashboard).await?;

    let (health, health_address) = bind_health(health_listen).await?;

    let (listeners, public_address, worker_address) = bind_listeners(listen, worker_listen).await?;
    let endpoint = resolve_public_endpoint(endpoint, server.transport.kind(), public_address)?;
    let derived_worker_endpoint = server.transport.kind().worker_url(worker_address)?;
    let worker_endpoint = resolve_worker_endpoint(worker_endpoint, derived_worker_endpoint);
    let grace = server.http2.shutdown_grace;

    let provisioner_name = provisioner.key();
    let fleet = Arc::new(Provisioner::build(provisioner, &context)?);
    let cache = Cache::build(cache, &context)?;
    let signer = Signer::build(signer, &context)?;
    let scheduler = NativeLink::try_new(NativeLinkConfig::for_controller(worker_endpoint, &server))
        .map_err(MainError::Scheduler)?;
    let repository = Database::build(database, &context)?;
    let (clients, workers) = build_authentication(authentication, &context)?;
    let providers = identity_providers(identity, &context)?;
    let lead = server.leadership.clone();
    let backstop = shutdown_backstop(
        grace,
        actions.history_flush_interval.get(),
        &lead,
        server.request_timeout.get(),
    );
    let clock = Arc::new(SystemClock::new());

    let settings = server_settings(
        SettingsSource {
            actions: Arc::new(actions),
            poll_interval,
            handoff_timeout: backstop,
            client_action_timeout,
            default_isa,
            endpoint: endpoint.clone(),
            transport: transport_settings(&server),
            dashboard,
            builds,
            login,
            provisioner: provisioner_name,
        },
        server,
    )?;

    let server = Box::pin(Server::<GangDeployment>::prepare(
        Parts {
            provisioner: Arc::clone(&fleet),
            fleets,
            cache,
            signer,
            scheduler,
            repository,
            providers,
            clock: Arc::clone(&clock),
            listeners,
            controller,
            clients,
            workers,
        },
        settings,
    ))
    .await?
    .with_health(health);

    let notes = server_announcements(
        &server,
        endpoint,
        (public_address, Some(worker_address), health_address),
    );
    let args = (fleet, out, &notes[..], (&*clock, &lead, backstop));
    let _ = prepared.send(());
    Box::pin(run_announced(server, shutdown, args)).await?;

    Ok(ExitCode::SUCCESS)
}

/// Resolves the public endpoint from configuration or the bound address.
fn resolve_public_endpoint(
    endpoint: Option<Url>,
    transport: gang_config::ServerTransport,
    address: SocketAddr,
) -> Result<Url> {
    match endpoint {
        Some(url) => Ok(url),
        None => Ok(transport.public_url(address)?),
    }
}

async fn bind_health(
    address: Option<SocketAddr>,
) -> Result<(Option<TcpListener>, Option<SocketAddr>)> {
    let listener = match address {
        Some(address) => Some(bind(ListenerRole::Health, address).await?),
        None => None,
    };
    let bound = listener
        .as_ref()
        .map(TcpListener::local_addr)
        .transpose()
        .map_err(MainError::LocalAddress)?;
    Ok((listener, bound))
}

fn shutdown_backstop(
    grace: std::time::Duration,
    history_flush_interval: std::time::Duration,
    leadership: &gang_config::LeadershipConfig,
    request_timeout: std::time::Duration,
) -> std::time::Duration {
    // Deployment defaults: 30s history + 3s release + 10s milestone + 30s request = 73s,
    // below the lifecycle hook's 120s margin above shutdown_grace.
    grace
        .saturating_add(history_flush_interval)
        .saturating_add(leadership.renew_interval.get())
        .saturating_add(leadership.ttl.get())
        .saturating_add(request_timeout)
}

fn server_announcements(
    server: &Server<GangDeployment>,
    endpoint: Url,
    (public_address, worker, health): (SocketAddr, Option<SocketAddr>, Option<SocketAddr>),
) -> Vec<Announcement> {
    let startup = match server.ca_pin() {
        Some(pin) => Announcement::CaPin(pin),
        None => Announcement::Plaintext(endpoint),
    };
    announcements(public_address, worker, health, startup)
}

/// Binds both listeners before the controller begins serving.
async fn bind_listeners(
    listen: SocketAddr,
    worker_listen: SocketAddr,
) -> Result<(Listeners<TcpListener>, SocketAddr, SocketAddr)> {
    let public = bind(ListenerRole::Public, listen).await?;
    let public_address = public.local_addr().map_err(MainError::LocalAddress)?;
    let worker = bind(ListenerRole::Worker, worker_listen).await?;
    let worker_address = worker.local_addr().map_err(MainError::LocalAddress)?;
    Ok((Listeners { public, worker }, public_address, worker_address))
}

/// Runs `server`, printing `announcements` once the controller is ready.
async fn run_announced(
    server: Server<GangDeployment>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    (fleet, out, announcements, (clock, leadership, shutdown_grace)): (
        Arc<Provisioner>,
        &Output,
        &[Announcement],
        (
            &impl Clock,
            &gang_config::LeadershipConfig,
            std::time::Duration,
        ),
    ),
) -> Result<()> {
    let (ready, readiness) = ControllerReady::channel();
    let retiring_fleet = Arc::clone(&fleet);
    let (stop, stopped) = oneshot::channel::<()>();
    let shutdown = async move {
        shutdown_when(shutdown, retiring_fleet.retiring()).await;
        let _ = stop.send(());
    };
    let announced = async {
        Box::pin(announce_when_ready(readiness, out, announcements)).await?;
        std::future::pending::<Result<()>>().await
    };
    let work = async {
        let result = tokio::select! {
            biased;
            result = announced => return result,
            result = Box::pin(server.run(async { let _ = stopped.await; }, ready)) => result,
        };
        if matches!(&result, Ok(()) | Err(ServerError::LeadershipLost))
            && let Err(error) = report_milestone(
                fleet.as_ref(),
                HostMilestone::HandedOff,
                clock,
                leadership.renew_interval.get(),
                leadership.ttl.get(),
            )
            .await
        {
            tracing::warn!(%error, "failed to report handoff milestone");
        }
        result.map_err(Into::into)
    };
    bound_after_shutdown(clock, shutdown, work, shutdown_grace)
        .await
        .map_err(|error| {
            tracing::error!(%error, "controller shutdown deadline elapsed");
            MainError::ShutdownTimedOut(error)
        })?
}

/// Runs the shutdown sequence under one deadline measured from the stop signal.
async fn bound_after_shutdown<C: Clock, T>(
    clock: &C,
    shutdown: impl Future<Output = ()>,
    work: impl Future<Output = T>,
    grace: std::time::Duration,
) -> std::result::Result<T, gang_clock::Elapsed> {
    tokio::pin!(shutdown, work);
    tokio::select! {
        biased;
        () = &mut shutdown => clock.timeout(grace, work).await,
        result = &mut work => Ok(result),
    }
}

/// Reports a milestone within one lease lifetime, retrying transient failures.
async fn report_milestone<H: ControllerHostPort, C: Clock>(
    host: &H,
    milestone: HostMilestone,
    clock: &C,
    interval: std::time::Duration,
    bound: std::time::Duration,
) -> std::result::Result<(), PortError> {
    clock
        .timeout(bound, async {
            loop {
                match host.reached(milestone).await {
                    Ok(()) => return Ok(()),
                    Err(error @ PortError::Unavailable(_)) => {
                        tracing::warn!(%error, ?milestone, "failed to report milestone; retrying");
                    }
                    Err(error) => return Err(error),
                }
                clock.sleep(interval).await;
            }
        })
        .await
        .map_err(PortError::unavailable)?
}

/// Stops for a signal or retirement, but keeps waiting for a signal if retirement fails.
async fn shutdown_when(
    signal: impl Future<Output = ()>,
    retiring: impl Future<Output = std::result::Result<(), PortError>>,
) {
    let retirement = async {
        if let Err(error) = retiring.await {
            tracing::warn!(%error, "failed to query host retirement");
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        () = signal => {}
        () = retirement => {}
    }
}

/// The configured values that `serve` builds the server settings from, besides the `[server]`
/// section.
struct SettingsSource {
    /// Shared action scheduling and resource policy.
    actions: Arc<gang_config::ActionsConfig>,
    /// How often the control loop runs a pass.
    poll_interval: NonZeroDuration,
    /// How long a client action may take before it is timed out.
    client_action_timeout: NonZeroDuration,
    /// The shutdown backstop also bounds a voluntary handoff.
    handoff_timeout: std::time::Duration,
    /// The interface standard the server's default isa is.
    default_isa: Isa,
    /// The endpoint clients reach the public listener at.
    endpoint: Url,
    /// How accepted connections are handled.
    transport: TransportSettings,
    /// The operator dashboard's configuration.
    dashboard: DashboardConfig,
    /// The live builds configuration.
    builds: gang_config::BuildsConfig,
    /// The browser sign-in of the dashboard, with its client secret already loaded.
    login: Option<LoginSettings>,
    /// The key of the configured provisioner section.
    provisioner: &'static str,
}

/// Builds the server settings from the configured values and the `[server]` section.
fn server_settings(source: SettingsSource, server: ServerConfig) -> Result<ServerSettings> {
    Ok(ServerSettings {
        actions: source.actions,
        proxy_protocol: Arc::new(server.proxy_protocol),
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
        leadership: server.leadership,
        standby_routability_delay: server.standby_routability_delay,
        handoff_timeout: source.handoff_timeout,
        revocation_refresh_interval: server.revocation_refresh_interval,
        max_tracked_principals: server.max_tracked_principals,
        accept_error_backoff: server.accept_error_backoff,
        health_max_connections: server.health_max_connections,
        health_max_header_size: server.health_max_header_size.as_usize(),
        dashboard: source.dashboard,
        builds: source.builds,
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
        session_max_age: dashboard.session_max_age,
        renewal_reuse: dashboard.renewal_reuse,
        device: dashboard.device.enabled.then_some(DeviceSettings {
            ttl: dashboard.device.ttl,
            poll_interval: dashboard.device.poll_interval,
        }),
    }))
}

/// Builds the listener and startup announcements that mean the server is ready.
fn announcements(
    public: SocketAddr,
    worker: Option<SocketAddr>,
    health: Option<SocketAddr>,
    startup: Announcement,
) -> Vec<Announcement> {
    let mut announcements = vec![Announcement::Listening(ListenerRole::Public, public)];
    if let Some(address) = worker {
        announcements.push(Announcement::Listening(ListenerRole::Worker, address));
    }
    if let Some(address) = health {
        announcements.push(Announcement::Listening(ListenerRole::Health, address));
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

/// Fills an omitted worker endpoint with the address derived from the bound listener.
fn resolve_worker_endpoint(configured: Option<Url>, derived: Url) -> Url {
    configured.unwrap_or(derived)
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
            ca_pinned: server.ca_pin.is_some(),
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
    use gang_testkit::TestResult;

    use super::*;

    #[test]
    fn transport_settings_preserve_ca_pin() -> TestResult {
        for (config, expected) in [
            ("trust_domain = 'example.test'\n", false),
            ("trust_domain = 'example.test'\nca_pin = 'signer'\n", true),
        ] {
            let server: ServerConfig = toml::from_str(config)?;
            let TransportSettings::Tls(tls) = transport_settings(&server) else {
                panic!("test configuration uses TLS");
            };
            assert_eq!(tls.ca_pinned, expected);
        }
        Ok(())
    }

    struct RetryHost(std::sync::atomic::AtomicUsize, HostMilestone);

    impl ControllerHostPort for RetryHost {
        async fn incarnation(
            &self,
        ) -> std::result::Result<gang_spi::ControllerHostIdentity, PortError> {
            Ok(gang_spi::ControllerHostIdentity::Persistent(
                gang_types::InstanceId::try_new("retry-host").map_err(PortError::config)?,
            ))
        }

        async fn retiring(&self) -> std::result::Result<(), PortError> {
            std::future::pending().await
        }

        async fn reached(&self, milestone: HostMilestone) -> std::result::Result<(), PortError> {
            assert_eq!(milestone, self.1);
            if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Err(PortError::unavailable("temporary"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn handoff_milestone_retries_a_transient_failure() {
        let host = RetryHost(
            std::sync::atomic::AtomicUsize::new(0),
            HostMilestone::HandedOff,
        );
        let clock = gang_clock::TestClock::default();
        let report = report_milestone(
            &host,
            HostMilestone::HandedOff,
            &clock,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(3),
        );
        tokio::pin!(report);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(report.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.advance(std::time::Duration::from_secs(1));
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(report.as_mut().poll(cx))).await,
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(host.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    struct RefusingHost;

    impl ControllerHostPort for RefusingHost {
        async fn incarnation(
            &self,
        ) -> std::result::Result<gang_spi::ControllerHostIdentity, PortError> {
            Ok(gang_spi::ControllerHostIdentity::Persistent(
                gang_types::InstanceId::try_new("refusing-host").map_err(PortError::config)?,
            ))
        }

        async fn retiring(&self) -> std::result::Result<(), PortError> {
            std::future::pending().await
        }

        async fn reached(&self, _milestone: HostMilestone) -> std::result::Result<(), PortError> {
            Err(PortError::backend("permanent"))
        }
    }

    #[tokio::test]
    async fn permanent_milestone_failure_is_not_retried() {
        let clock = gang_clock::TestClock::default();
        let result = report_milestone(
            &RefusingHost,
            HostMilestone::HandedOff,
            &clock,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(3),
        )
        .await;
        assert!(matches!(result, Err(PortError::Backend(_))));
    }

    #[tokio::test]
    async fn full_drain_still_reports_handoff() {
        let clock = gang_clock::TestClock::default();
        let grace = std::time::Duration::from_secs(5);
        let reported = std::cell::Cell::new(false);
        let work = async {
            clock.sleep(std::time::Duration::from_secs(1)).await;
            clock.sleep(grace).await;
            clock.sleep(std::time::Duration::from_secs(61)).await;
            reported.set(true);
        };
        let deadline = shutdown_backstop(
            grace,
            std::time::Duration::from_secs(61),
            &gang_config::LeadershipConfig::default(),
            std::time::Duration::from_secs(30),
        );
        let run = bound_after_shutdown(&clock, std::future::ready(()), work, deadline);
        tokio::pin!(run);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.advance(std::time::Duration::from_secs(1));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.advance(std::time::Duration::from_secs(4));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.advance(std::time::Duration::from_secs(1));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.advance(std::time::Duration::from_mins(1));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.advance(std::time::Duration::from_secs(1));
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx))).await,
            std::task::Poll::Ready(Ok(()))
        ));
        assert!(reported.get());
    }

    #[tokio::test]
    async fn signal_during_preparation_exits_successfully() {
        let (prepared, preparation) = oneshot::channel();
        let (stop, _stopped) = oneshot::channel();
        let result = stop_during_preparation(
            std::future::ready(()),
            preparation,
            stop,
            std::future::pending::<Result<ExitCode>>(),
        )
        .await;
        assert!(matches!(result, Ok(None)));
        drop(prepared);
    }

    #[tokio::test]
    async fn shutdown_deadline_cuts_off_a_hung_handoff() -> TestResult {
        let clock = gang_clock::TestClock::default();
        let bound = shutdown_backstop(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(61),
            &gang_config::LeadershipConfig::default(),
            std::time::Duration::from_secs(30),
        );
        let run = bound_after_shutdown(
            &clock,
            std::future::ready(()),
            std::future::pending::<()>(),
            bound,
        );
        tokio::pin!(run);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.advance(bound);
        let std::task::Poll::Ready(Err(error)) =
            std::future::poll_fn(|cx| std::task::Poll::Ready(run.as_mut().poll(cx))).await
        else {
            panic!("hung handoff did not reach its deadline");
        };
        assert_eq!(
            shutdown_exit_status(Err(MainError::ShutdownTimedOut(error)))?,
            ExitCode::from(SHUTDOWN_DEADLINE_EXIT_CODE)
        );
        assert_eq!(
            shutdown_exit_status(Err(MainError::Server(ServerError::HandoffTimedOut(
                gang_clock::Elapsed,
            ))))?,
            ExitCode::from(SHUTDOWN_DEADLINE_EXIT_CODE)
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_completes_on_a_signal() -> TestResult {
        shutdown_when(std::future::ready(()), std::future::pending()).await;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_completes_on_retirement() -> TestResult {
        shutdown_when(std::future::pending(), std::future::ready(Ok(()))).await;
        Ok(())
    }

    #[tokio::test]
    async fn failed_retirement_waits_for_a_signal() -> TestResult {
        let (send, receive) = oneshot::channel();
        let shutdown = shutdown_when(
            async {
                let _ = receive.await;
            },
            std::future::ready(Err(PortError::unavailable("retirement failed"))),
        );
        tokio::pin!(shutdown);
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(shutdown.as_mut().poll(cx).is_pending())
        })
        .await;
        assert!(pending);
        assert!(send.send(()).is_ok());
        shutdown.await;
        Ok(())
    }

    /// A controller that fails before readiness produces no startup announcements.
    #[tokio::test]
    async fn nothing_is_announced_when_the_controller_never_becomes_ready() {
        let (out, stdout, _) = Output::captured();
        let (ready, readiness) = ControllerReady::channel();
        let announcements = announcements(
            bound(),
            Some(loopback()),
            Some(bound()),
            Announcement::CaPin(gang_types::CaPin::new([7; 32])),
        );
        assert_eq!(
            announcements,
            vec![
                Announcement::Listening(ListenerRole::Public, bound()),
                Announcement::Listening(ListenerRole::Worker, loopback()),
                Announcement::Listening(ListenerRole::Health, bound()),
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
            Some(loopback()),
            Some(bound()),
            Announcement::CaPin(gang_types::CaPin::new([7; 32])),
        );
        assert_eq!(
            announcements,
            vec![
                Announcement::Listening(ListenerRole::Public, bound()),
                Announcement::Listening(ListenerRole::Worker, loopback()),
                Announcement::Listening(ListenerRole::Health, bound()),
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

    fn workers_endpoint() -> Url {
        Url::parse("grpcs://workers.example.com:8443").expect("test endpoint is a valid url")
    }

    #[test]
    fn loopback_listen_derives_the_worker_endpoint_from_the_bound_address() {
        let expected = ServerTransport::Plaintext
            .worker_url(bound())
            .expect("the bound address forms a url");
        let url = resolve_worker_endpoint(None, expected.clone());
        assert_eq!(url, expected);
        assert_eq!(url.as_str(), "grpc://127.0.0.1:41000/");
    }

    #[test]
    fn configured_endpoint_is_returned_unchanged_for_a_non_loopback_listen() {
        let derived = ServerTransport::Plaintext
            .worker_url(bound())
            .expect("the bound address forms a url");
        let url = resolve_worker_endpoint(Some(workers_endpoint()), derived);
        assert_eq!(url, workers_endpoint());
    }

    #[test]
    fn controller_id_has_128_bits_in_lowercase_hex() -> TestResult {
        let id = controller_id()?.to_string();
        assert_eq!(id.len(), 32);
        assert!(
            id.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        Ok(())
    }

    #[test]
    fn server_settings_preserve_leadership() -> TestResult {
        let server: ServerConfig = toml::from_str(
            "transport = 'plaintext'\nstandby_routability_delay = '10s'\nrevocation_refresh_interval = '7s'\n[leadership]\nttl = '20s'\nrenew_interval = '4s'\nclock_skew = '1s'\n",
        )?;
        let leadership = server.leadership.clone();
        let refresh = server.revocation_refresh_interval;
        let actions = Arc::new(gang_config::ActionsConfig {
            kill_grace: NonZeroDuration::from_secs(7),
            ..gang_config::ActionsConfig::default()
        });
        let settings = server_settings(
            SettingsSource {
                actions: Arc::clone(&actions),
                poll_interval: NonZeroDuration::from_secs(1),
                client_action_timeout: NonZeroDuration::from_secs(1),
                handoff_timeout: std::time::Duration::from_secs(47),
                default_isa: Isa::X86_64,
                endpoint: Url::parse("http://127.0.0.1:8080")?,
                transport: transport_settings(&server),
                dashboard: DashboardConfig::default(),
                builds: gang_config::BuildsConfig::default(),
                login: None,
                provisioner: "local",
            },
            server,
        )?;
        assert!(Arc::ptr_eq(&settings.actions, &actions));
        assert_eq!(settings.leadership, leadership);
        assert_eq!(settings.revocation_refresh_interval, refresh);
        assert_eq!(
            settings.standby_routability_delay,
            std::time::Duration::from_secs(10)
        );
        assert_eq!(settings.handoff_timeout, std::time::Duration::from_secs(47));
        Ok(())
    }

    #[tokio::test]
    async fn public_and_worker_listeners_report_os_chosen_ports() -> TestResult {
        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let (_listeners, public_address, worker_address) = bind_listeners(address, address).await?;
        assert_ne!(public_address.port(), 0);
        assert_ne!(worker_address.port(), 0);
        assert_ne!(public_address, worker_address);
        Ok(())
    }
}
