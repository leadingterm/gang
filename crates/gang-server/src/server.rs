//! The server: prepares the authentication core, the gates and the control API, then runs the
//! scheduler's controller and the control loop until shutdown.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use gang_clock::Clock;
use gang_config::{DashboardConfig, Fleets};
use gang_mtls::SubjectName;
use gang_spi::{
    AuthenticationPort, CacheStorePort, ClientCertificates, ControllerConnections, ControllerReady,
    ControllerSpec, CredentialLifecyclePort, Listening, SchedulerPort, Scheme, StateRepositoryPort,
};
use gang_tls::{Alpn, ClientAuth, RevocationSet};
use gang_types::{CaPin, NonZeroDuration, Url};
use tokio::sync::oneshot;

use crate::api::{Api, ApiParts};
use crate::authorizer::Authorizer;
use crate::client_address::TrustedProxies;
use crate::composite::PublicService;
use crate::control::ControlLoop;
use crate::core::Core;
use crate::dashboard::Dashboard;
use crate::deployment::{Deployment, Listeners, Parts, Stack};
use crate::endpoint::{EndpointRole, PublicEndpoint, WorkerEndpoint};
use crate::errors::ServerError;
use crate::gate::Gate;
use crate::host::Host;
use crate::info::wire_server;
use crate::listeners::{Accepting, TlsPolicy};
use crate::login::SessionRoutes;
use crate::principals::{PrincipalSightings, RevokedPrincipals};
use crate::rate_limit::RateLimiter;
use crate::revocation::Revocations;
use crate::security::{Security, TlsIdentity};
use crate::settings::{LoginSettings, ServerSettings, TransportSettings};

/// The composed server: the controller's scheduler and provisioner, the control loop, the gates that
/// admit requests on each listener, and the Gang API they hand requests to.
pub struct Server<D: Deployment> {
    core: Arc<Core<D>>,
    provisioner: D::Provisioner,
    fleets: Fleets<D::Plugins>,
    cache: D::Cache,
    scheduler: D::Scheduler,
    listeners: Listeners<D::Listener>,
    public: PublicService<D>,
    public_gate: Gate<D, D::Clients>,
    worker_gate: Gate<D, D::Workers>,
    revoked_certificates: RevocationSet,
    worker_endpoint: WorkerEndpoint,
    poll_interval: NonZeroDuration,
    client_action_timeout: NonZeroDuration,
    accept_error_backoff: NonZeroDuration,
    dashboard: Option<Dashboard>,
}

impl<D: Deployment> Server<D> {
    /// Prepares the server from its ports and settings: checks the worker URL's scheme,
    /// establishes the certificate authority when TLS is on, loads the revoked principals and
    /// certificates, and builds the core, the two gates and the Gang API. Nothing listens until
    /// [`Server::run`].
    ///
    /// # Errors
    ///
    /// Returns an error when the worker URL has the wrong scheme, a TLS listener has no usable
    /// host name, the certificate authority cannot be established, the revoked principals or
    /// certificates cannot be loaded from the ports, or the server information cannot be built.
    pub async fn prepare(parts: Parts<D>, settings: ServerSettings) -> Result<Self, ServerError> {
        let Parts {
            provisioner: fleet,
            fleets,
            cache,
            signer,
            scheduler,
            repository,
            providers,
            clock,
            listeners,
            clients,
            workers,
        } = parts;
        let ServerSettings {
            poll_interval,
            client_action_timeout,
            default_isa,
            endpoint,
            transport,
            roles,
            limits,
            rate_limit,
            principal_seen_interval,
            max_tracked_principals,
            accept_error_backoff,
            dashboard,
            login,
            provisioner,
        } = settings;
        let (worker_endpoint, security, signer) = prepare_transport::<D>(
            transport,
            scheduler.endpoints().worker,
            &endpoint,
            signer,
            &clock,
        )
        .await?;
        let info = wire_server(
            &endpoint,
            default_isa,
            security.certificate(),
            clients.scheme(),
            &providers,
        )?;
        let revoked_principals = load_revoked_principals::<D>(&repository).await?;
        let core = Core::new(
            Host {
                repository,
                signer,
                security,
                providers,
                clock,
                endpoint,
                revoked_principals,
                sightings: PrincipalSightings::new(
                    principal_seen_interval.get(),
                    max_tracked_principals,
                ),
            },
            clients,
            workers,
        );
        let revoked_certificates = revoked_certificates(&core).await?;
        let dashboard =
            start_dashboard(dashboard, provisioner, poll_interval.get(), &core.host).await?;
        let public = public_service(
            ApiParts {
                core: Arc::clone(&core),
                revocations: Revocations::new(Arc::clone(&core), revoked_certificates.clone()),
                info,
                authorizer: Authorizer::new(roles),
                limits,
                dashboard: dashboard.as_ref().map(Dashboard::api),
                limiter: Arc::new(RateLimiter::new(&rate_limit, Arc::clone(&core.host.clock))),
                login_provider: None,
            },
            login,
        );
        let proxies = Arc::new(TrustedProxies::new(rate_limit.trusted_proxies.clone()));
        Ok(Self {
            provisioner: fleet,
            fleets,
            cache,
            scheduler,
            listeners,
            public,
            public_gate: Gate::public(Arc::clone(&core), proxies),
            worker_gate: Gate::worker(Arc::clone(&core)),
            core,
            revoked_certificates,
            worker_endpoint,
            poll_interval,
            client_action_timeout,
            accept_error_backoff,
            dashboard,
        })
    }

    /// The pin of the certificate authority the server issues certificates under; `None` when the
    /// transport is plaintext.
    #[must_use]
    pub fn ca_pin(&self) -> Option<CaPin> {
        self.core.host.security.ca_pin()
    }

    /// Runs the server until `shutdown` completes or a component fails. On shutdown, each
    /// viewer is told the server is shutting down, then the connections are drained. Dropping
    /// the returned future stops every connection and task the server started.
    /// `ready` is signalled once the embedded controller is built and serves; a run that fails
    /// first drops it unsent.
    ///
    /// Under plaintext, and for every surface whose authentication plugin presents
    /// [`Scheme::None`], a warning is logged once at start.
    ///
    /// # Errors
    ///
    /// Returns an error when the stores cannot be described, a TLS listener cannot be secured,
    /// or the controller fails or returns without being asked to stop.
    /// A listener that stops accepting connections before shutdown ends the run with
    /// [`ServerError::ListenerEnded`].
    pub async fn run(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
        ready: ControllerReady,
    ) -> Result<(), ServerError> {
        let core = &*self.core;
        warn_unauthenticated(
            !core.host.security.is_tls(),
            core.host.endpoint.url(),
            self.worker_endpoint.url(),
            core.clients.scheme(),
            core.workers.scheme(),
        );
        let content_store = self.cache.content_store().map_err(ServerError::Cache)?;
        let action_store = self.cache.action_store().map_err(ServerError::Cache)?;
        let spec = ControllerSpec {
            content_store: &content_store,
            action_store: &action_store,
            fleets: self.fleets.catalog(),
            client_action_timeout: self.client_action_timeout.get(),
        };
        let (public, worker) = open_endpoints::<D>(
            self.listeners,
            core,
            &self.revoked_certificates,
            self.accept_error_backoff,
        )
        .await?;
        let (public_ended, public_end) = oneshot::channel();
        let (worker_ended, worker_end) = oneshot::channel();
        let shutdown = shutdown.shared();
        let connections = ControllerConnections {
            public: Listening {
                connections: public
                    .into_stream(public_ended)
                    .take_until(shutdown.clone())
                    .boxed(),
                gate: self.public_gate,
            },
            worker: Listening {
                connections: worker
                    .into_stream(worker_ended)
                    .take_until(shutdown.clone())
                    .boxed(),
                gate: self.worker_gate,
            },
            api: self.public,
        };
        let dashboard = self.dashboard.as_ref();
        let control = async {
            let control = ControlLoop::new(
                &self.fleets,
                self.poll_interval,
                &self.provisioner,
                &self.scheduler,
                &core.host.repository,
                &core.host.clock,
            );
            let control = match dashboard {
                Some(dashboard) => control.with_publisher(dashboard.publisher()),
                None => control,
            };
            control.run().await
        };
        let scheduler = &self.scheduler;
        let clock = &core.host.clock;
        let revoked_principals = &core.host.revoked_principals;
        let mut controller = Box::pin(scheduler.run_controller(&spec, connections, ready));
        let mut public_stopped = Box::pin(listener_stopped("public", public_end));
        let mut worker_stopped = Box::pin(listener_stopped("worker", worker_end));
        let mut background = Box::pin(async move {
            tokio::select! {
                never = control => match never {},
                never = dashboard_tasks(dashboard, scheduler, clock, revoked_principals) => never,
            }
        });
        tokio::select! {
            biased;
            () = shutdown.clone() => {
                if let Some(dashboard) = dashboard {
                    dashboard.shutdown();
                }
                drop(background);
                controller.await.map_err(ServerError::Controller)
            }
            error = &mut public_stopped => Err(error),
            error = &mut worker_stopped => Err(error),
            result = &mut controller => match result {
                Ok(()) => Err(ServerError::ControllerEnded),
                Err(error) => Err(ServerError::Controller(error)),
            },
            never = &mut background => match never {},
        }
    }
}

/// Validates the worker endpoint and establishes the listener security policy.
async fn prepare_transport<K: Stack>(
    transport: TransportSettings,
    worker_url: Url,
    endpoint: &PublicEndpoint,
    signer: K::Signer,
    clock: &Arc<K::Clock>,
) -> Result<(WorkerEndpoint, Security<K>, Arc<K::Signer>), ServerError> {
    let tls = matches!(transport, TransportSettings::Tls(_));
    let worker_endpoint = WorkerEndpoint::try_new(worker_url, tls)?;
    let signer = Arc::new(signer);
    let security = Security::<K>::establish(
        transport,
        endpoint,
        &worker_endpoint,
        Arc::clone(&signer),
        Arc::clone(clock),
    )
    .await?;
    Ok((worker_endpoint, security, signer))
}

/// Builds the public API and mounts browser routes only when the dashboard is enabled.
fn public_service<K: Stack>(
    mut parts: ApiParts<K>,
    login: Option<LoginSettings>,
) -> PublicService<K> {
    let ui = parts
        .dashboard
        .as_ref()
        .map(|_| SessionRoutes::new(Arc::clone(&parts.core), login, Arc::clone(&parts.limiter)));
    parts.login_provider = ui.as_ref().and_then(SessionRoutes::login_provider).cloned();
    PublicService {
        api: Api::new(parts),
        ui,
    }
}

/// Resolves with [`ServerError::ListenerEnded`] once the `endpoint` listener's connection stream
/// has ended; never when the stream is dropped without ending, as on shutdown.
async fn listener_stopped(endpoint: &'static str, ended: oneshot::Receiver<()>) -> ServerError {
    if ended.await.is_err() {
        std::future::pending::<()>().await;
    }
    ServerError::ListenerEnded { endpoint }
}

/// Starts the dashboard when it is enabled; a disabled dashboard builds nothing.
async fn start_dashboard<K: Stack>(
    config: DashboardConfig,
    provisioner: String,
    poll_interval: Duration,
    host: &Host<K>,
) -> Result<Option<Dashboard>, ServerError> {
    if !config.enabled {
        return Ok(None);
    }
    Dashboard::start(
        config,
        provisioner,
        poll_interval,
        host.revoked_principals.epoch(),
        &host.clock,
        &host.repository,
    )
    .await
    .map(Some)
}

/// The dashboard's background work: the actions refresher and the principals forwarder. Never
/// completes; without a dashboard it waits forever.
async fn dashboard_tasks<S: SchedulerPort, C: Clock>(
    dashboard: Option<&Dashboard>,
    scheduler: &S,
    clock: &C,
    revoked_principals: &RevokedPrincipals,
) -> Infallible {
    let Some(dashboard) = dashboard else {
        return std::future::pending().await;
    };
    tokio::select! {
        never = dashboard.refresher(scheduler, clock).run() => never,
        never = dashboard.forward_principals(revoked_principals) => never,
    }
}

/// The principals the repository has revoked, which the host refuses.
async fn load_revoked_principals<K: Stack>(
    repository: &K::Repository,
) -> Result<RevokedPrincipals, ServerError> {
    let revoked = repository
        .revoked_principals()
        .await
        .map_err(ServerError::Revocations)?;
    Ok(RevokedPrincipals::from_identities(
        revoked.into_iter().map(|principal| principal.identity),
    ))
}

/// The serials of the certificates the worker plugin reports revoked; none when it issues no
/// certificates. Only workers present certificates: a clients plugin that presents itself as
/// mTLS is refused at startup ([`ServerError::ClientScheme`]).
async fn revoked_certificates<K: Stack>(core: &Core<K>) -> Result<RevocationSet, ServerError> {
    let serials = if core.workers.capabilities().certificates {
        core.workers
            .revoked_serials(&core.host)
            .await
            .map_err(ServerError::Revocations)?
    } else {
        Vec::new()
    };
    Ok(RevocationSet::from_serials(serials))
}

/// The listeners that accept unauthenticated connections, as the startup warning names them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnauthenticatedListeners<'a> {
    /// Both endpoints, public first, when the server serves without TLS.
    plaintext: Option<(&'a Url, &'a Url)>,
    /// The surfaces whose authentication is [`Scheme::None`].
    surfaces: Vec<EndpointRole>,
}

/// Decides what the startup warning reports: whether the server serves without TLS, and which
/// surfaces have their authentication disabled.
fn unauthenticated_listeners<'a>(
    plaintext: bool,
    endpoint: &'a Url,
    worker_endpoint: &'a Url,
    public: Scheme,
    worker: Scheme,
) -> UnauthenticatedListeners<'a> {
    let surfaces = [
        (EndpointRole::Public, public),
        (EndpointRole::Worker, worker),
    ]
    .into_iter()
    .filter(|(_, scheme)| *scheme == Scheme::None)
    .map(|(role, _)| role)
    .collect();
    UnauthenticatedListeners {
        plaintext: plaintext.then_some((endpoint, worker_endpoint)),
        surfaces,
    }
}

/// Logs a warning when the server serves without TLS, and once for each surface whose
/// authentication is disabled.
fn warn_unauthenticated(
    plaintext: bool,
    endpoint: &Url,
    worker_endpoint: &Url,
    public: Scheme,
    worker: Scheme,
) {
    let listeners = unauthenticated_listeners(plaintext, endpoint, worker_endpoint, public, worker);
    if let Some((public, worker)) = listeners.plaintext {
        tracing::warn!(
            public = %public,
            worker = %worker,
            "serving without TLS: the listeners accept unauthenticated connections"
        );
    }
    for role in listeners.surfaces {
        tracing::warn!(surface = %role, "authentication is disabled on this surface");
    }
}

/// Opens the public and worker listeners, secured when the core's security is TLS. The public
/// listener asks its clients for the certificates the clients' plugin wants; the worker listener
/// asks its workers for the certificates the workers' plugin wants.
async fn open_endpoints<D: Deployment>(
    listeners: Listeners<D::Listener>,
    core: &Core<D>,
    revoked_certificates: &RevocationSet,
    accept_error_backoff: NonZeroDuration,
) -> Result<
    (
        Accepting<D::Listener, Arc<D::Signer>, D::Clock>,
        Accepting<D::Listener, Arc<D::Signer>, D::Clock>,
    ),
    ServerError,
> {
    let Security::Tls(tls) = &core.host.security else {
        return Ok((
            Accepting::plain(
                listeners.public,
                accept_error_backoff,
                Arc::clone(&core.host.clock),
            ),
            Accepting::plain(
                listeners.worker,
                accept_error_backoff,
                Arc::clone(&core.host.clock),
            ),
        ));
    };
    let public = Accepting::secured(
        listeners.public,
        Arc::clone(&tls.authority),
        policy(
            tls,
            &tls.names.public,
            core.clients.client_certificates(),
            Alpn::H2AndHttp1,
            revoked_certificates,
            accept_error_backoff,
        ),
        Arc::clone(&core.host.clock),
    )
    .await?;
    let worker = Accepting::secured(
        listeners.worker,
        Arc::clone(&tls.authority),
        policy(
            tls,
            &tls.names.worker,
            core.workers.client_certificates(),
            Alpn::H2,
            revoked_certificates,
            accept_error_backoff,
        ),
        Arc::clone(&core.host.clock),
    )
    .await?;
    Ok((public, worker))
}

/// The TLS policy of one listener: it presents a certificate named `name`, and asks its clients
/// for `certificates` and offers `alpn`.
fn policy<K: Stack>(
    tls: &TlsIdentity<K>,
    name: &SubjectName,
    certificates: ClientCertificates,
    alpn: Alpn,
    revoked_certificates: &RevocationSet,
    accept_error_backoff: NonZeroDuration,
) -> TlsPolicy {
    TlsPolicy {
        names: vec![name.clone()],
        client_auth: client_auth(certificates),
        revoked_certificates: revoked_certificates.clone(),
        certificate_ttl: tls.settings.certificate_ttl,
        reissue_at_percent: tls.settings.reissue_at_percent,
        alpn,
        handshake: tls.settings.handshake,
        accept_error_backoff,
    }
}

/// The client authentication a listener asks of its clients at the TLS layer.
fn client_auth(certificates: ClientCertificates) -> ClientAuth {
    match certificates {
        ClientCertificates::Never => ClientAuth::None,
        ClientCertificates::Optional => ClientAuth::Optional,
        ClientCertificates::Required => ClientAuth::Required,
    }
}

#[cfg(test)]
mod tests {
    use gang_spi::Scheme;
    use gang_types::Url;

    use super::{UnauthenticatedListeners, unauthenticated_listeners};
    use crate::endpoint::EndpointRole;

    fn url(text: &str) -> Url {
        Url::parse(text).expect("test url parses")
    }

    #[test]
    fn tls_with_token_clients_and_mtls_workers_warns_about_nothing() {
        let public = url("https://gang.example.com");
        let worker = url("grpcs://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(false, &public, &worker, Scheme::Token, Scheme::Mtls),
            UnauthenticatedListeners {
                plaintext: None,
                surfaces: Vec::new(),
            }
        );
    }

    #[test]
    fn plaintext_with_none_on_both_surfaces_lists_both_listeners_with_their_addresses() {
        let public = url("http://gang.example.com:8080");
        let worker = url("grpc://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(true, &public, &worker, Scheme::None, Scheme::None),
            UnauthenticatedListeners {
                plaintext: Some((&public, &worker)),
                surfaces: vec![EndpointRole::Public, EndpointRole::Worker],
            }
        );
    }

    #[test]
    fn tls_with_none_on_clients_lists_only_the_public_listener() {
        let public = url("https://gang.example.com");
        let worker = url("grpcs://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(false, &public, &worker, Scheme::None, Scheme::Mtls),
            UnauthenticatedListeners {
                plaintext: None,
                surfaces: vec![EndpointRole::Public],
            }
        );
    }

    #[test]
    fn tls_with_none_on_workers_lists_only_the_worker_listener() {
        let public = url("https://gang.example.com");
        let worker = url("grpcs://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(false, &public, &worker, Scheme::Token, Scheme::None),
            UnauthenticatedListeners {
                plaintext: None,
                surfaces: vec![EndpointRole::Worker],
            }
        );
    }

    #[test]
    fn plaintext_with_token_warns_only_about_the_transport() {
        let public = url("http://gang.example.com:8080");
        let worker = url("grpc://worker.example.com:8443");
        assert_eq!(
            unauthenticated_listeners(true, &public, &worker, Scheme::Token, Scheme::Mtls),
            UnauthenticatedListeners {
                plaintext: Some((&public, &worker)),
                surfaces: Vec::new(),
            }
        );
    }
}
