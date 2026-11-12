//! The adapters `gang worker` is composed from: one trait naming the concrete port types, the
//! value that carries an instance of each, and the settings the run loop reads.

use std::ffi::OsString;
use std::path::PathBuf;

use gang_clock::Clock;
use gang_config::{ExecutionConfig, Fleets, PluginSet, WorkerConfig};
use gang_spi::{
    ActionLimitsPort, CacheStorePort, NodeLifecyclePort, ProvisionerPort, SchedulerPort,
    ScratchVolumePort, WorkloadCredentialPort,
};

use crate::credentials::Enroller;
use crate::transport::{ServerCaSource, Transport};

/// The concrete adapter types a worker runs on.
pub trait WorkerDeployment: 'static {
    /// The provisioner: the worker's identity, its scratch volume, its workload credential,
    /// its lifetime, and preemption notices.
    type Provisioner: ProvisionerPort
        + NodeLifecyclePort
        + ScratchVolumePort
        + WorkloadCredentialPort;
    /// The cache stores the worker reads and writes through.
    type Cache: CacheStorePort;
    /// The scheduler that builds and runs the worker.
    type Scheduler: SchedulerPort;
    /// The kernel limits each action runs under.
    type Limits: ActionLimitsPort;
    /// The clock that times certificate refresh and preemption.
    type Clock: Clock;
    /// The enroller that exchanges the workload credential for a certificate.
    type Enroller: Enroller;
    /// The source of the CA certificate a server-only transport trusts.
    type ServerCa: ServerCaSource;
}

/// Every port a worker is composed from.
pub struct WorkerParts<D: WorkerDeployment> {
    /// The provisioner: the worker's identity, its scratch volume, its workload credential,
    /// its lifetime, and preemption notices.
    pub provisioner: D::Provisioner,
    /// The cache stores the worker reads and writes through.
    pub cache: D::Cache,
    /// The scheduler that builds and runs the worker.
    pub scheduler: D::Scheduler,
    /// The clock that times certificate refresh and preemption.
    pub clock: D::Clock,
    /// The security mode: the server CA source for server-only TLS, the certificate enroller
    /// for mutual TLS.
    pub transport: Transport<D::Enroller, D::ServerCa>,
}

/// The settings of one `gang worker` run that are not ports.
pub struct WorkerSettings<P: PluginSet> {
    /// The worker fleets in preference order; the worker runs the fleet its identity names.
    pub fleets: Fleets<P>,
    /// The `[worker]` section.
    pub worker: WorkerConfig,
    /// The sandbox plugin sections that the sandbox file of the worker's fleet carries.
    pub sandbox: P::Sandbox,
    /// The `[execution]` section of the sandbox file.
    pub execution: ExecutionConfig,
    /// The `[limits.<plugin>]` section of the sandbox file, when one is configured.
    pub limits: Option<P::Limits>,
    /// The `gang` binary, which the worker's entrypoint links to.
    pub executable: PathBuf,
    /// The `PATH` in which sandbox programs given as bare names are resolved, on the worker.
    /// The host passes its own `PATH`. Empty entries are skipped.
    pub search_path: OsString,
}
