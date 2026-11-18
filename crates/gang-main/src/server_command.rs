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
use gang_server::{
    DeviceSettings, HandshakeSettings, LeaderListeners, Listeners, LoginSettings, Parts, Provider,
    Providers, PublicEndpoint, RequestLimits, Server, ServerError, ServerSettings, TlsSettings,
    TransportSettings,
};
use gang_spi::{
    BoxedError, ControllerHostPort, ControllerId, ControllerReady, HostMilestone, LoginClient,
    PluginContext, PortError,
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
    type Scheduler = Scheduler;
    type Listener = TcpListener;
    type Binder = TcpBinder;
}

/// Binds leader-only TCP listeners using the same socket defaults as the public listener.
struct TcpBinder;

impl gang_server::Binder for TcpBinder {
    type Listener = TcpListener;

    async fn bind(&self, address: SocketAddr) -> std::result::Result<TcpListener, BoxedError> {
        TcpListener::bind(address).await.map_err(Into::into)
    }
}

/// Generates an identifier unique to this controller process.
fn controller_id() -> Result<ControllerId> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(MainError::ControllerId)?;
    Ok(ControllerId::new(format!(
        "{:032x}",
        u128::from_be_bytes(bytes)
    )))
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
    tracing::info!(%controller, "starting controller");
    let LoadedConfig { path, config, .. } = loaded;
    let GangConfig {
        poll_interval,
        client_action_timeout,
        default_isa,
        listen,
        worker_listen,
        health_listen,
        execution_listen,
        execution_endpoint,
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
        builds,
        ..
    } = config;
    let context = PluginContext { config_path: &path };
    let login = dashboard_login(&dashboard).await?;

    let (health, health_address) = bind_health(health_listen).await?;

    let (listeners, public_address, worker_address) =
        bind_listeners(listen, worker_listen, execution_listen).await?;
    let endpoint = resolve_public_endpoint(endpoint, server.transport.kind(), public_address)?;
    let execution_endpoint = resolve_execution_endpoint(
        execution_listen,
        execution_endpoint,
        server.transport.kind(),
    )?;
    let derived_worker_endpoint = server.transport.kind().worker_url(worker_address)?;
    let (scheduler, _) = resolve_worker_endpoint(scheduler, derived_worker_endpoint);
    let grace = scheduler
        .shutdown_grace()
        .map_or(std::time::Duration::ZERO, |(_, grace)| grace);

    let provisioner_name = provisioner.key();
    let fleet = Arc::new(Provisioner::build(provisioner, &context)?);
    let cache = Cache::build(cache, &context)?;
    let signer = Signer::build(signer, &context)?;
    let scheduler = Scheduler::build(scheduler, &context)?;
    let repository = Database::build(database, &context)?;
    let (clients, workers) = build_authentication(authentication, &context)?;
    let transport = transport_settings(&server);
    let providers = identity_providers(identity, &context)?;
    let lead = server.leadership.clone();
    let backstop = shutdown_backstop(grace, &lead, server.request_timeout.get());
    let clock = Arc::new(SystemClock::new());

    let settings = server_settings(
        SettingsSource {
            poll_interval,
            client_action_timeout,
            default_isa,
            endpoint: endpoint.clone(),
            execution_endpoint,
            transport,
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
        (public_address, worker_address, health_address),
        execution_listen,
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
    leadership: &gang_config::LeadershipConfig,
    request_timeout: std::time::Duration,
) -> std::time::Duration {
    // Deployment defaults: 3s release + 10s milestone + 30s request = 43s,
    // below the lifecycle hook's 120s margin above shutdown_grace.
    grace
        .saturating_add(leadership.renew_interval.get())
        .saturating_add(leadership.ttl.get())
        .saturating_add(request_timeout)
}

fn server_announcements(
    server: &Server<GangDeployment>,
    endpoint: Url,
    (public_address, worker, health): (SocketAddr, SocketAddr, Option<SocketAddr>),
    execution_listen: Option<SocketAddr>,
) -> Vec<Announcement> {
    let startup = match server.ca_pin() {
        Some(pin) => Announcement::CaPin(pin),
        None => Announcement::Plaintext(endpoint),
    };
    announcements(
        public_address,
        execution_listen.is_none().then_some(worker),
        health,
        startup,
    )
}

/// Binds the public listener and, in the single-controller shape, the worker listener.
async fn bind_listeners(
    listen: SocketAddr,
    worker_listen: SocketAddr,
    execution_listen: Option<SocketAddr>,
) -> Result<(Listeners<TcpBinder>, SocketAddr, SocketAddr)> {
    let public = bind(ListenerRole::Public, listen).await?;
    let public_address = public.local_addr().map_err(MainError::LocalAddress)?;
    let (leader, worker_address) = if let Some(execution) = execution_listen {
        (
            LeaderListeners::Dedicated {
                execution,
                worker: worker_listen,
                binder: TcpBinder,
            },
            worker_listen,
        )
    } else {
        let worker = bind(ListenerRole::Worker, worker_listen).await?;
        let address = worker.local_addr().map_err(MainError::LocalAddress)?;
        (LeaderListeners::OnPublic { worker }, address)
    };
    Ok((Listeners { public, leader }, public_address, worker_address))
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
        Box::pin(announce_when_ready(readiness, out, announcements, async {
            report_milestone(
                fleet.as_ref(),
                HostMilestone::Serving,
                clock,
                leadership.renew_interval.get(),
                leadership.ttl.get(),
            )
            .await
            .map_err(MainError::ServingMilestone)
        }))
        .await?;
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
    /// How often the control loop runs a pass.
    poll_interval: NonZeroDuration,
    /// How long a client action may take before it is timed out.
    client_action_timeout: NonZeroDuration,
    /// The interface standard the server's default isa is.
    default_isa: Isa,
    /// The endpoint clients reach the public listener at.
    endpoint: Url,
    /// The leader execution endpoint, when it has a dedicated listener.
    execution_endpoint: Option<Url>,
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
        poll_interval: source.poll_interval,
        client_action_timeout: source.client_action_timeout,
        default_isa: source.default_isa,
        endpoint: PublicEndpoint::try_new(source.endpoint)?,
        execution_endpoint: source.execution_endpoint,
        transport: source.transport,
        roles: server.roles,
        limits: RequestLimits {
            max_request_size: usize::try_from(server.max_request_size.get()).unwrap_or(usize::MAX),
            request_timeout: server.request_timeout,
        },
        rate_limit: server.rate_limit,
        principal_seen_interval: server.principal_seen_interval,
        leadership: server.leadership,
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
    on_ready: impl Future<Output = Result<()>>,
) -> Result<()> {
    if readiness.await.is_ok() {
        for announcement in announcements {
            out.data(announcement.to_string())?;
        }
        on_ready.await?;
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

/// Resolves a dedicated execution endpoint using the same derivation as the public endpoint.
fn resolve_execution_endpoint(
    listen: Option<SocketAddr>,
    endpoint: Option<Url>,
    transport: gang_config::ServerTransport,
) -> Result<Option<Url>> {
    listen
        .map(|address| match endpoint {
            Some(url) => Ok(url),
            None => transport.public_url(address).map_err(Into::into),
        })
        .transpose()
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
    async fn serving_milestone_retries_a_transient_failure() -> TestResult {
        let host = RetryHost(
            std::sync::atomic::AtomicUsize::new(0),
            HostMilestone::Serving,
        );
        let clock = gang_clock::TestClock::default();
        let report = report_milestone(
            &host,
            HostMilestone::Serving,
            &clock,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(3),
        );
        tokio::pin!(report);
        let first =
            std::future::poll_fn(|cx| std::task::Poll::Ready(report.as_mut().poll(cx))).await;
        assert!(first.is_pending());
        clock.advance(std::time::Duration::from_secs(1));
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(report.as_mut().poll(cx))).await,
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(host.0.load(std::sync::atomic::Ordering::SeqCst), 2);
        Ok(())
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
            reported.set(true);
        };
        let deadline = shutdown_backstop(
            grace,
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
        let reported = std::cell::Cell::new(false);

        announce_when_ready(readiness, &out, &announcements, async {
            reported.set(true);
            Ok(())
        })
        .await
        .expect("a dropped readiness signal is not an output error");
        assert!(stdout.text().is_empty());
        assert!(!reported.get());
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
        let reported = std::cell::Cell::new(false);

        announce_when_ready(readiness, &out, &announcements, async {
            reported.set(true);
            Ok(())
        })
        .await
        .expect("announcements are written");
        let printed: Vec<Announcement> = stdout
            .text()
            .lines()
            .map(|line| line.parse().expect("each line is an announcement"))
            .collect();
        assert_eq!(printed, announcements);
        assert!(reported.get());
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

    fn worker_endpoint_of(scheduler: &SchedulerConfig) -> Option<Url> {
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
        assert_eq!(worker_endpoint_of(&scheduler), Some(expected));
    }

    #[test]
    fn configured_endpoint_is_returned_unchanged_for_a_non_loopback_listen() {
        let derived = ServerTransport::Plaintext
            .worker_url(bound())
            .expect("the bound address forms a url");
        let (scheduler, url) =
            resolve_worker_endpoint(scheduler(Some("grpcs://workers.example.com:8443")), derived);
        assert_eq!(url, workers_endpoint());
        assert_eq!(worker_endpoint_of(&scheduler), Some(workers_endpoint()));
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
    fn execution_endpoint_is_derived_or_configured_only_for_a_dedicated_listener() -> TestResult {
        let configured = Url::parse("https://execution.example:443")?;
        assert_eq!(
            resolve_execution_endpoint(Some(bound()), None, ServerTransport::Plaintext)?
                .as_ref()
                .map(Url::as_str),
            Some("http://127.0.0.1:41000/")
        );
        assert_eq!(
            resolve_execution_endpoint(Some(bound()), None, ServerTransport::Tls)?
                .as_ref()
                .map(Url::as_str),
            Some("https://127.0.0.1:41000/")
        );
        assert_eq!(
            resolve_execution_endpoint(
                Some(bound()),
                Some(configured.clone()),
                ServerTransport::Tls
            )?,
            Some(configured)
        );
        assert_eq!(
            resolve_execution_endpoint(None, None, ServerTransport::Plaintext)?,
            None
        );
        Ok(())
    }

    #[test]
    fn server_settings_preserve_leadership_and_execution_endpoint() -> TestResult {
        let server: ServerConfig = toml::from_str(
            "transport = 'plaintext'\nrevocation_refresh_interval = '7s'\n[leadership]\nttl = '20s'\nrenew_interval = '4s'\nclock_skew = '1s'\n",
        )?;
        let leadership = server.leadership.clone();
        let refresh = server.revocation_refresh_interval;
        let execution_endpoint =
            resolve_execution_endpoint(Some(bound()), None, server.transport.kind())?;
        let settings = server_settings(
            SettingsSource {
                poll_interval: NonZeroDuration::from_secs(1),
                client_action_timeout: NonZeroDuration::from_secs(1),
                default_isa: Isa::X86_64,
                endpoint: Url::parse("http://127.0.0.1:8080")?,
                execution_endpoint: execution_endpoint.clone(),
                transport: transport_settings(&server),
                dashboard: DashboardConfig::default(),
                builds: gang_config::BuildsConfig::default(),
                login: None,
                provisioner: "local",
            },
            server,
        )?;
        assert_eq!(settings.execution_endpoint, execution_endpoint);
        assert_eq!(settings.leadership, leadership);
        assert_eq!(settings.revocation_refresh_interval, refresh);
        Ok(())
    }

    #[tokio::test]
    async fn public_and_worker_listeners_report_os_chosen_ports() -> TestResult {
        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let public = bind(ListenerRole::Public, address).await?;
        let worker = bind(ListenerRole::Worker, address).await?;
        let public_address = public.local_addr()?;
        let worker_address = worker.local_addr()?;
        assert_ne!(public_address.port(), 0);
        assert_ne!(worker_address.port(), 0);
        assert_ne!(public_address, worker_address);
        Ok(())
    }

    #[test]
    fn dedicated_listeners_are_not_announced_before_binding() {
        let startup = Announcement::CaPin(gang_types::CaPin::new([7; 32]));
        assert_eq!(
            announcements(bound(), None, None, startup.clone()),
            vec![
                Announcement::Listening(ListenerRole::Public, bound()),
                startup
            ]
        );
    }
}
