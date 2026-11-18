//! Plugins shared by the `compose!` integration tests. Each test crate declares this module and
//! composes the same plugins, so every item here is used by both crates.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use gang_clock::Timestamp;
use gang_spi::{
    Acquisition, ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, ActionsView,
    ApiService, Authenticated, AuthenticationHost, AuthenticationPort, CacheStorePort,
    Capabilities, CertStatus, CertificateIssue, CertificateSignerPort, CertificateTarget,
    ClientCertificates, ControllerConnections, ControllerEndpoints, ControllerHostPort,
    ControllerId, ControllerReady, ControllerSpec, CredentialLifecyclePort, Evidence,
    ExpiringRecordPort, FleetInstance, FleetOptions, FleetSupply, Held, HostMilestone, Insert,
    InstanceIdentity, IssuedCertRecord, IssuedCertificate, IssuedToken, LeaderLeasePort, Lease,
    LeaseEpoch, ListingLimits, LoginChallenge, LoginClient, LoginGrant, LoginPort, LoginRequest,
    LoginSupport, MountedScratchVolume, NodeLifecyclePort, Plugin, PluginContext, PortError,
    PreemptionNotice, PrincipalRecord, ProofVerifierPort, ProvisionerPort, QueuedDemand, Record,
    RecordKey, RecordVersion, RecordedScalingEvent, Rejection, Remove, RenderedStoreSpec, Renewal,
    Replace, RequestGate, RevocationEpoch, SandboxPrograms, SandboxedAction, ScalingAuditEvent,
    ScalingEventSeq, SchedulerPort, SchedulerSnapshot, Scheme, ScratchVolumePort,
    SignatureAlgorithm, StateRepositoryPort, StoreAccess, Subject, TokenIssue, WorkerSpec,
    WorkerTls, WorkloadCredentialPort,
};
use gang_types::{
    ActionResources, AuthenticationSettings, Bytes, CertSerial, Fleet, FleetName, FleetSet,
    Identity, InstanceId, InstanceLifecycle, Isa, LoginSession, Principal, Proof, ProviderName,
    RefreshToken, SchedulerSettings, Url, WorkerId, WorkerLifetime,
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
            desired: Mutex<u32>,
            /// The fleet options each port call received, in call order.
            pub seen: Mutex<Vec<String>>,
            /// Number of convergence calls received.
            pub converges: Mutex<usize>,
        }

        impl Plugin for $plugin {
            type Config = NumberConfig;

            fn build(
                config: NumberConfig,
                _context: &PluginContext<'_>,
            ) -> Result<Self, PortError> {
                Ok(Self {
                    number: config.number,
                    desired: Mutex::new(config.number),
                    seen: Mutex::new(Vec::new()),
                    converges: Mutex::new(0),
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
                into.desired = *self.desired.lock();
                into.instances.clear();
                into.exits.clear();
                into.warm = 0;
                into.abandoned = 0;
                into.launch_failure = None;
                into.instances.push(FleetInstance {
                    id: instance(self.number)?,
                    lifecycle: InstanceLifecycle::InService,
                    launched_at: Timestamp::from_unix_millis(u64::from(self.number)),
                    release_first: false,
                });
                Ok(())
            }

            async fn converge(
                &self,
                held: &Held,
                _fleet: Fleet<'_, Self::FleetOptions>,
            ) -> Result<(), PortError> {
                held.check()?;
                *self.converges.lock() += 1;
                Ok(())
            }

            async fn set_desired_capacity(
                &self,
                held: &Held,
                fleet: Fleet<'_, Self::FleetOptions>,
                desired: u32,
            ) -> Result<(), PortError> {
                held.check()?;
                self.record(fleet.options);
                let mut current = self.desired.lock();
                held.check()?;
                *current = if desired < *current {
                    desired.max(1)
                } else {
                    desired
                };
                Ok(())
            }

            async fn terminate(
                &self,
                held: &Held,
                fleet: Fleet<'_, Self::FleetOptions>,
                instance: &InstanceId,
            ) -> Result<(), PortError> {
                held.check()?;
                self.record(fleet.options);
                Err(PortError::NotFound(Subject::Instance(instance.clone())))
            }

            fn worker_lifetime(&self) -> WorkerLifetime {
                $lifetime
            }
        }

        impl ControllerHostPort for $plugin {
            async fn retiring(&self) -> Result<(), PortError> {
                std::future::pending().await
            }

            async fn reached(&self, _milestone: HostMilestone) -> Result<(), PortError> {
                Ok(())
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
            resources: ActionResources::default(),
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

/// The in-test database plugin.
pub struct Memory {
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    certificates: BTreeMap<CertSerial, IssuedCertRecord>,
    principals: BTreeMap<Identity, PrincipalRecord>,
    revocation_epoch: u64,
    lease: Option<MemoryLease>,
    records: BTreeMap<RecordKey, Record>,
}

struct MemoryLease {
    holder: Option<ControllerId>,
    epoch: LeaseEpoch,
    expires_at: Timestamp,
}

impl Plugin for Memory {
    type Config = NumberConfig;

    fn build(config: NumberConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        let principal =
            Principal::try_new(format!("db-{}", config.number)).map_err(PortError::backend)?;
        let record = IssuedCertRecord {
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
        };
        let mut state = MemoryState::default();
        state.certificates.insert(record.serial.clone(), record);
        Ok(Self {
            state: Mutex::new(state),
        })
    }
}

impl StateRepositoryPort for Memory {
    async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.certificates.contains_key(&record.serial) {
            return Err(PortError::backend(std::io::Error::other(
                "duplicate certificate",
            )));
        }
        state
            .certificates
            .insert(record.serial.clone(), record.clone());
        Ok(())
    }

    async fn certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Option<IssuedCertRecord>, PortError> {
        Ok(self.state.lock().certificates.get(serial).cloned())
    }

    async fn revoke(
        &self,
        serial: &CertSerial,
        at: Timestamp,
    ) -> Result<IssuedCertRecord, PortError> {
        let mut state = self.state.lock();
        let next = state.revocation_epoch.checked_add(1).ok_or_else(|| {
            PortError::backend(std::io::Error::other("revocation epoch overflow"))
        })?;
        let record = state
            .certificates
            .get_mut(serial)
            .ok_or_else(|| PortError::NotFound(Subject::Certificate(serial.clone())))?;
        if record.status == CertStatus::Active {
            record.status = CertStatus::Revoked { at };
        }
        let record = record.clone();
        state.revocation_epoch = next;
        Ok(record)
    }

    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
        let mut records: Vec<_> = self
            .state
            .lock()
            .certificates
            .values()
            .filter(|record| matches!(record.status, CertStatus::Revoked { .. }))
            .cloned()
            .collect();
        records.sort_by_key(|record| match record.status {
            CertStatus::Revoked { at } => (at, record.serial.clone()),
            CertStatus::Active => (Timestamp::UNIX_EPOCH, record.serial.clone()),
        });
        Ok(records)
    }

    async fn certificates_of(
        &self,
        identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        let mut records: Vec<_> = self
            .state
            .lock()
            .certificates
            .values()
            .filter(|record| record.identity == *identity && record.status == CertStatus::Active)
            .cloned()
            .collect();
        records.sort_by(|a, b| (a.issued_at, &a.serial).cmp(&(b.issued_at, &b.serial)));
        Ok(records)
    }

    async fn certificates(
        &self,
        after: Option<&CertSerial>,
        limit: NonZeroUsize,
        into: &mut Vec<IssuedCertRecord>,
    ) -> Result<bool, PortError> {
        let state = self.state.lock();
        let mut records = state
            .certificates
            .values()
            .filter(|record| after.is_none_or(|after| record.serial > *after));
        into.clear();
        into.extend(records.by_ref().take(limit.get()).cloned());
        Ok(records.next().is_some())
    }

    async fn revocation_epoch(&self) -> Result<RevocationEpoch, PortError> {
        Ok(RevocationEpoch::new(self.state.lock().revocation_epoch))
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

    async fn observe_principal(&self, identity: &Identity, at: Timestamp) -> Result<(), PortError> {
        let mut state = self.state.lock();
        let record = state
            .principals
            .entry(identity.clone())
            .or_insert_with(|| PrincipalRecord {
                identity: identity.clone(),
                first_seen: at,
                last_seen: at,
                revoked_at: None,
            });
        record.last_seen = record.last_seen.max(at);
        Ok(())
    }

    async fn principal(&self, identity: &Identity) -> Result<Option<PrincipalRecord>, PortError> {
        Ok(self.state.lock().principals.get(identity).cloned())
    }

    async fn revoke_principal(
        &self,
        identity: &Identity,
        at: Timestamp,
    ) -> Result<PrincipalRecord, PortError> {
        let mut state = self.state.lock();
        let next = state.revocation_epoch.checked_add(1).ok_or_else(|| {
            PortError::backend(std::io::Error::other("revocation epoch overflow"))
        })?;
        let record = state
            .principals
            .entry(identity.clone())
            .or_insert_with(|| PrincipalRecord {
                identity: identity.clone(),
                first_seen: at,
                last_seen: at,
                revoked_at: None,
            });
        record.revoked_at.get_or_insert(at);
        let record = record.clone();
        state.revocation_epoch = next;
        Ok(record)
    }

    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        let mut state = self.state.lock();
        let next = state.revocation_epoch.checked_add(1).ok_or_else(|| {
            PortError::backend(std::io::Error::other("revocation epoch overflow"))
        })?;
        let record = state
            .principals
            .get_mut(identity)
            .ok_or_else(|| PortError::NotFound(Subject::Principal(identity.clone())))?;
        record.revoked_at = None;
        let record = record.clone();
        state.revocation_epoch = next;
        Ok(record)
    }

    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
        let mut records: Vec<_> = self
            .state
            .lock()
            .principals
            .values()
            .filter(|record| record.revoked_at.is_some())
            .cloned()
            .collect();
        records.sort_by(|a, b| (a.revoked_at, &a.identity).cmp(&(b.revoked_at, &b.identity)));
        Ok(records)
    }

    async fn principals(
        &self,
        after: Option<&gang_types::Identity>,
        limit: NonZeroUsize,
        only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError> {
        let state = self.state.lock();
        let mut records = state.principals.values().filter(|record| {
            after.is_none_or(|after| record.identity > *after)
                && (!only_revoked || record.revoked_at.is_some())
        });
        into.clear();
        into.extend(records.by_ref().take(limit.get()).cloned());
        Ok(records.next().is_some())
    }
}

impl LeaderLeasePort for Memory {
    async fn acquire(
        &self,
        holder: &ControllerId,
        stale_before: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Acquisition, PortError> {
        let mut state = self.state.lock();
        if let Some(current) = &state.lease
            && let Some(current_holder) = &current.holder
            && current.expires_at >= stale_before
        {
            return Ok(Acquisition::Held(Lease {
                holder: current_holder.clone(),
                epoch: current.epoch,
                expires_at: current.expires_at,
            }));
        }
        let previous = state.lease.as_ref().map_or(0, |lease| lease.epoch.get());
        let epoch = previous
            .checked_add(1)
            .ok_or_else(|| PortError::backend(std::io::Error::other("lease epoch overflow")))?;
        let lease = Lease {
            holder: holder.clone(),
            epoch: LeaseEpoch::new(epoch),
            expires_at,
        };
        state.lease = Some(MemoryLease {
            holder: Some(holder.clone()),
            epoch: lease.epoch,
            expires_at,
        });
        Ok(Acquisition::Acquired(lease))
    }

    async fn renew(&self, lease: &Lease, expires_at: Timestamp) -> Result<Renewal, PortError> {
        let mut state = self.state.lock();
        if let Some(current) = state.lease.as_mut()
            && current.holder.as_ref() == Some(&lease.holder)
            && current.epoch == lease.epoch
        {
            current.expires_at = expires_at;
            return Ok(Renewal::Renewed(Lease {
                holder: lease.holder.clone(),
                epoch: lease.epoch,
                expires_at,
            }));
        }
        Ok(Renewal::Lost)
    }

    async fn release(&self, lease: &Lease) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if let Some(current) = state.lease.as_mut()
            && current.holder.as_ref() == Some(&lease.holder)
            && current.epoch == lease.epoch
        {
            current.holder = None;
        }
        Ok(())
    }
}

impl ExpiringRecordPort for Memory {
    async fn insert(
        &self,
        records: &[(RecordKey, &[u8])],
        version: RecordVersion,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Insert, PortError> {
        let mut state = self.state.lock();
        if records.iter().any(|(key, _)| {
            state
                .records
                .get(key)
                .is_some_and(|record| now < record.expires_at)
        }) {
            return Ok(Insert::Exists);
        }
        for (key, body) in records {
            state.records.insert(
                *key,
                Record {
                    body: body.to_vec(),
                    version,
                    expires_at,
                },
            );
        }
        Ok(Insert::Inserted)
    }

    async fn get(&self, key: &RecordKey, now: Timestamp) -> Result<Option<Record>, PortError> {
        Ok(self
            .state
            .lock()
            .records
            .get(key)
            .filter(|record| now < record.expires_at)
            .cloned())
    }

    async fn replace(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        body: &[u8],
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Replace, PortError> {
        let mut state = self.state.lock();
        let Some(record) = state
            .records
            .get_mut(key)
            .filter(|record| now < record.expires_at && record.version == version)
        else {
            return Ok(Replace::Stale);
        };
        let next = version
            .get()
            .checked_add(1)
            .ok_or_else(|| PortError::backend(std::io::Error::other("record version overflow")))?;
        let version = RecordVersion::new(next);
        *record = Record {
            body: body.to_vec(),
            version,
            expires_at,
        };
        Ok(Replace::Replaced(version))
    }

    async fn remove(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        now: Timestamp,
    ) -> Result<Remove, PortError> {
        let mut state = self.state.lock();
        if state
            .records
            .get(key)
            .is_some_and(|record| now < record.expires_at && record.version == version)
        {
            state.records.remove(key);
            return Ok(Remove::Removed);
        }
        Ok(Remove::Stale)
    }

    async fn purge(&self, now: Timestamp) -> Result<(), PortError> {
        self.state
            .lock()
            .records
            .retain(|_, record| now < record.expires_at);
        Ok(())
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
        let proof = Proof::new(session.expose());
        let principal = self.verify(&proof).await?;
        Ok(LoginGrant {
            proof,
            principal,
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
                _expires_at: gang_clock::Timestamp,
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
