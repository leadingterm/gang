//! The adapters a server is composed from: the types the request-serving components are generic
//! over, the types only the composed server needs, and the value that carries an instance of each.

use std::sync::Arc;

use gang_clock::Clock;
use gang_config::{Fleets, PluginSet};
use gang_spi::{
    AuthenticationPort, CacheStorePort, CertificateSignerPort, CredentialLifecyclePort, LoginPort,
    ProofVerifierPort, ProvisionerPort, SchedulerPort, StateRepositoryPort,
};
use gang_tls::Listener;

use crate::identity::Providers;

/// The adapter types the request-serving components are generic over.
pub trait Stack: 'static {
    /// The signer that holds the certificate authority's key.
    type Signer: CertificateSignerPort;
    /// The clock that times certificates, reconciliation and rotation.
    type Clock: Clock;
    /// The durable store for certificates, principals and scaling events.
    type Repository: StateRepositoryPort;
    /// The verifier for identity proofs, which also reports each provider's login support.
    type Identity: ProofVerifierPort + LoginPort;
    /// The plugin that authenticates clients of the public listener.
    type Clients: AuthenticationPort + CredentialLifecyclePort;
    /// The plugin that authenticates workers on the worker listener.
    type Workers: AuthenticationPort + CredentialLifecyclePort;
}

/// A [`Stack`] plus the adapters only the composed server needs.
pub trait Deployment: Stack {
    /// The provisioner that creates and removes worker instances.
    type Provisioner: ProvisionerPort + 'static;
    /// The cache stores that clients and workers read and write.
    type Cache: CacheStorePort;
    /// The scheduler that dispatches actions to workers.
    type Scheduler: SchedulerPort;
    /// The listener that accepts raw connections on each endpoint.
    type Listener: Listener;
    /// The plugins that configure the worker fleets and the sandboxes they run.
    type Plugins: PluginSet<FleetOptions = <Self::Provisioner as ProvisionerPort>::FleetOptions>;
}

/// The two listeners a server serves.
pub struct Listeners<L> {
    /// The listener for clients of the scheduler and the Gang API.
    pub public: L,
    /// The listener for workers of the scheduler.
    pub worker: L,
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
    pub listeners: Listeners<D::Listener>,
    /// The plugin that authenticates clients of the public listener.
    pub clients: D::Clients,
    /// The plugin that authenticates workers on the worker listener.
    pub workers: D::Workers,
}
