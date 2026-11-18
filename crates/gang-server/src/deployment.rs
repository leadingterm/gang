//! The adapters a server is composed from: the types the request-serving components are generic
//! over, the types only the composed server needs, and the value that carries an instance of each.

use std::net::SocketAddr;
use std::sync::Arc;

use gang_clock::Clock;
use gang_config::{Fleets, PluginSet};
use gang_spi::{
    AuthenticationPort, BoxedError, CacheStorePort, CertificateSignerPort, ControllerHostPort,
    ControllerId, CredentialLifecyclePort, ExpiringRecordPort, LeaderLeasePort, LoginPort,
    ProofVerifierPort, ProvisionerPort, SchedulerPort, StateRepositoryPort,
};
use gang_tls::Listener;

use crate::identity::Providers;

/// The adapter types the request-serving components are generic over.
pub trait Stack: 'static {
    /// The signer that holds the certificate authority's key.
    type Signer: CertificateSignerPort;
    /// The clock that times certificates, reconciliation and rotation.
    type Clock: Clock + 'static;
    /// The shared database: durable records, the leader lease and expiring records.
    type Repository: StateRepositoryPort + LeaderLeasePort + ExpiringRecordPort;
    /// The verifier for identity proofs, which also reports each provider's login support.
    type Identity: ProofVerifierPort + LoginPort;
    /// The plugin that authenticates clients of the public listener.
    type Clients: AuthenticationPort + CredentialLifecyclePort;
    /// The plugin that authenticates workers on the worker listener.
    type Workers: AuthenticationPort + CredentialLifecyclePort;
}

/// A [`Stack`] plus the adapters only the composed server needs.
pub trait Deployment: Stack {
    /// The provisioner that creates and removes worker instances and manages this controller's
    /// host.
    type Provisioner: ProvisionerPort + ControllerHostPort + 'static;
    /// The cache stores that clients and workers read and write.
    type Cache: CacheStorePort;
    /// The scheduler that dispatches actions to workers.
    type Scheduler: SchedulerPort;
    /// The listener that accepts raw connections on each endpoint.
    type Listener: Listener;
    /// Binds the listeners only the leader serves, once it leads.
    type Binder: Binder<Listener = Self::Listener>;
    /// The plugins that configure the worker fleets and the sandboxes they run.
    type Plugins: PluginSet<FleetOptions = <Self::Provisioner as ProvisionerPort>::FleetOptions>;
}

/// Binds a listener on an address.
pub trait Binder: Send + Sync + 'static {
    /// The listener it binds.
    type Listener: Listener;

    /// Binds a listener on `address`.
    ///
    /// # Errors
    ///
    /// The bind error, as when the address is in use.
    fn bind(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = Result<Self::Listener, BoxedError>> + Send;
}

/// The listeners a server serves.
pub struct Listeners<B: Binder> {
    /// The listener for clients of the cache and the Gang API, bound at start.
    pub public: B::Listener,
    /// The listeners only the leader serves.
    pub leader: LeaderListeners<B>,
}

/// The listeners only the leading controller serves.
pub enum LeaderListeners<B: Binder> {
    /// The single-controller shape: the public listener also carries the execution services, so
    /// the controller serves nothing until it leads. The worker listener is bound at start.
    OnPublic {
        /// The listener for workers of the scheduler.
        worker: B::Listener,
    },
    /// Every controller serves the public listener; the leader binds these once it leads. Their
    /// ports are fixed, so the endpoints advertised at start stay true.
    Dedicated {
        /// Where Execution and the leader routes are served.
        execution: SocketAddr,
        /// Where workers connect.
        worker: SocketAddr,
        /// Binds both.
        binder: B,
    },
}

/// Every port and listener a server is composed from.
pub struct Parts<D: Deployment> {
    /// The provisioner that creates and removes worker instances.
    pub provisioner: D::Provisioner,
    /// The worker fleets in preference order: what each fleet provisions and how it runs.
    pub fleets: Fleets<D::Plugins>,
    /// The cache stores that clients and workers read and write.
    pub cache: D::Cache,
    /// The signer that holds the certificate authority's key.
    pub signer: D::Signer,
    /// The scheduler that dispatches actions to workers.
    pub scheduler: D::Scheduler,
    /// The durable store for certificates, principals and scaling events.
    pub repository: D::Repository,
    /// The identity providers, keyed by name.
    pub providers: Providers<D::Identity>,
    /// The clock that times certificates, reconciliation and rotation, shared by all components.
    pub clock: Arc<D::Clock>,
    /// The listeners for each endpoint.
    pub listeners: Listeners<D::Binder>,
    /// This controller process's id in the leader lease.
    pub controller: ControllerId,
    /// The plugin that authenticates clients of the public listener.
    pub clients: D::Clients,
    /// The plugin that authenticates workers on the worker listener.
    pub workers: D::Workers,
}
