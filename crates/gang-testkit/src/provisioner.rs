//! An in-memory provisioner and worker-side ports for tests.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::TestClock;
use gang_spi::{
    FleetSupply, InstanceIdentity, MountedScratchVolume, NodeLifecyclePort, PortError,
    PreemptionNotice, ProvisionerPort, ScratchVolumePort, WorkloadCredentialPort,
};
use gang_types::{Bytes, Fleet, FleetName, InstanceId, Isa, Proof, WorkerLifetime};
use parking_lot::Mutex;

use crate::{CallCount, Opaque};

#[derive(Default)]
struct Script {
    remaining: usize,
    forever: bool,
}

impl Script {
    fn fails(&mut self) -> bool {
        if self.forever {
            return true;
        }
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }
}

#[derive(Default)]
struct Failures {
    supply: bool,
    set_desired_capacity: bool,
    terminate: bool,
}

struct State {
    supplies: BTreeMap<FleetName, FleetSupply>,
    supply_calls: Vec<(FleetName, Isa)>,
    desired: Vec<(FleetName, u32)>,
    terminations: Vec<InstanceId>,
    failures: Failures,
    advance_on_supply: Option<(Arc<TestClock>, Duration)>,
    identity: Option<InstanceIdentity>,
    lifetime: WorkerLifetime,
    notice: Option<PreemptionNotice>,
    volume: PathBuf,
    capacity: Bytes,
    credentials: Script,
    parked: bool,
    parked_checks: Script,
}

/// A shared, configurable provisioner with recorded fleet calls and worker-side answers.
///
/// The default has no node identity; configure one with [`Self::set_identity`] or [`Self::new`].
#[derive(Clone)]
pub struct FakeProvisioner {
    state: Arc<Mutex<State>>,
    credential_requests: CallCount,
    parked_polls: CallCount,
}

impl Default for FakeProvisioner {
    fn default() -> Self {
        Self::with_identity(
            None,
            WorkerLifetime::LongLived,
            PathBuf::new(),
            Bytes::new(0),
        )
    }
}

impl FakeProvisioner {
    /// Creates a fake with the worker identity, lifetime and scratch-volume response given.
    #[must_use]
    pub fn new(
        identity: InstanceIdentity,
        lifetime: WorkerLifetime,
        volume: PathBuf,
        capacity: Bytes,
    ) -> Self {
        Self::with_identity(Some(identity), lifetime, volume, capacity)
    }

    fn with_identity(
        identity: Option<InstanceIdentity>,
        lifetime: WorkerLifetime,
        volume: PathBuf,
        capacity: Bytes,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                supplies: BTreeMap::new(),
                supply_calls: Vec::new(),
                desired: Vec::new(),
                terminations: Vec::new(),
                failures: Failures::default(),
                advance_on_supply: None,
                identity,
                lifetime,
                notice: None,
                volume,
                capacity,
                credentials: Script::default(),
                parked: false,
                parked_checks: Script::default(),
            })),
            credential_requests: CallCount::new(),
            parked_polls: CallCount::new(),
        }
    }

    /// Configures the supply returned for a fleet, including its instance list.
    pub fn set_supply(&self, name: &FleetName, supply: FleetSupply) {
        self.state.lock().supplies.insert(name.clone(), supply);
    }
    /// Advances `clock` by `by` whenever a supply is read, including failed reads.
    pub fn advance_on_supply(&self, clock: Arc<TestClock>, by: Duration) {
        self.state.lock().advance_on_supply = Some((clock, by));
    }
    /// Returns all supply queries, including failed ones, in order.
    #[must_use]
    pub fn supply_calls(&self) -> Vec<(FleetName, Isa)> {
        self.state.lock().supply_calls.clone()
    }
    /// Returns successful desired-capacity requests in order.
    #[must_use]
    pub fn desired_requests(&self) -> Vec<(FleetName, u32)> {
        self.state.lock().desired.clone()
    }
    /// Returns successful terminations in order.
    #[must_use]
    pub fn terminations(&self) -> Vec<InstanceId> {
        self.state.lock().terminations.clone()
    }
    /// Enables or disables supply failures.
    pub fn fail_supply(&self, fail: bool) {
        self.state.lock().failures.supply = fail;
    }
    /// Enables or disables desired-capacity failures.
    pub fn fail_set_desired_capacity(&self, fail: bool) {
        self.state.lock().failures.set_desired_capacity = fail;
    }
    /// Enables or disables termination failures.
    pub fn fail_terminate(&self, fail: bool) {
        self.state.lock().failures.terminate = fail;
    }
    /// Sets the identity reported by the node lifecycle port.
    pub fn set_identity(&self, identity: InstanceIdentity) {
        self.state.lock().identity = Some(identity);
    }
    /// Sets the preemption notice reported by the node lifecycle port.
    pub fn set_preemption_notice(&self, notice: Option<PreemptionNotice>) {
        self.state.lock().notice = notice;
    }
    /// Sets whether the node lifecycle port reports the node parked.
    pub fn set_parked(&self, parked: bool) {
        self.state.lock().parked = parked;
    }
    /// Sets the capacity reported by the scratch-volume port.
    pub fn set_capacity(&self, capacity: Bytes) {
        self.state.lock().capacity = capacity;
    }
    /// Makes the next `count` workload-credential requests fail.
    pub fn fail_workload_credential_next(&self, count: usize) {
        self.state.lock().credentials.remaining = count;
    }
    /// Makes every workload-credential request fail.
    pub fn fail_workload_credential_forever(&self) {
        self.state.lock().credentials.forever = true;
    }
    /// Makes the next `count` parked checks fail.
    pub fn fail_parked_next(&self, count: usize) {
        self.state.lock().parked_checks.remaining = count;
    }
    /// Returns a waitable counter of workload-credential requests, including failures.
    #[must_use]
    pub fn workload_credential_calls(&self) -> CallCount {
        self.credential_requests.clone()
    }
    /// Returns a waitable counter of parked checks, including failures.
    #[must_use]
    pub fn parked_calls(&self) -> CallCount {
        self.parked_polls.clone()
    }
}

impl ProvisionerPort for FakeProvisioner {
    type FleetOptions = Opaque;

    async fn supply(
        &self,
        fleet: Fleet<'_, Opaque>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        let mut state = self.state.lock();
        state.supply_calls.push((fleet.name.clone(), fleet.isa));
        if let Some((clock, by)) = &state.advance_on_supply {
            clock.advance(*by);
        }
        if state.failures.supply {
            return Err(PortError::unavailable("fleet supply is unavailable"));
        }
        if let Some(supply) = state.supplies.get_mut(fleet.name) {
            into.desired = supply.desired;
            into.warm = supply.warm;
            into.instances.clone_from(&supply.instances);
            into.exits.clear();
            into.exits.append(&mut supply.exits);
        } else {
            into.desired = 0;
            into.warm = 0;
            into.instances.clear();
            into.exits.clear();
        }
        Ok(())
    }
    async fn set_desired_capacity(
        &self,
        fleet: Fleet<'_, Opaque>,
        desired: u32,
    ) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.failures.set_desired_capacity {
            return Err(PortError::unavailable("fleet capacity is unavailable"));
        }
        state.desired.push((fleet.name.clone(), desired));
        Ok(())
    }
    async fn terminate(
        &self,
        _fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        let mut state = self.state.lock();
        if state.failures.terminate {
            return Err(PortError::unavailable(
                "instance termination is unavailable",
            ));
        }
        state.terminations.push(instance.clone());
        Ok(())
    }
    fn worker_lifetime(&self) -> WorkerLifetime {
        self.state.lock().lifetime
    }
}

impl NodeLifecyclePort for FakeProvisioner {
    async fn identity(&self) -> Result<InstanceIdentity, PortError> {
        self.state
            .lock()
            .identity
            .clone()
            .ok_or_else(|| PortError::config("node identity is not configured"))
    }
    async fn preemption_notice(&self) -> Result<Option<PreemptionNotice>, PortError> {
        Ok(self.state.lock().notice)
    }
    async fn parked(&self) -> Result<bool, PortError> {
        self.parked_polls.bump();
        let mut state = self.state.lock();
        if state.parked_checks.fails() {
            return Err(PortError::backend(io::Error::other(
                "lifecycle service unavailable",
            )));
        }
        Ok(state.parked)
    }
}

impl ScratchVolumePort for FakeProvisioner {
    async fn mount(&self, _instance: &InstanceId) -> Result<MountedScratchVolume, PortError> {
        let state = self.state.lock();
        Ok(MountedScratchVolume {
            path: state.volume.clone(),
            capacity: state.capacity,
        })
    }
}

impl WorkloadCredentialPort for FakeProvisioner {
    async fn workload_credential(&self) -> Result<Proof, PortError> {
        self.credential_requests.bump();
        if self.state.lock().credentials.fails() {
            return Err(PortError::backend(io::Error::other(
                "credential service unavailable",
            )));
        }
        Ok(Proof::new("proof-1"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::Timestamp;
    use gang_spi::FleetInstance;
    use gang_types::{InstanceLifecycle, Resources};

    #[tokio::test]
    async fn fleet_calls_record_successes_failures_and_instance_listing() -> crate::TestResult {
        let fake = FakeProvisioner::default();
        let name = FleetName::try_new("workers")?;
        let options = Opaque::default();
        let fleet = Fleet {
            name: &name,
            isa: Isa::X86_64,
            instance: Resources {
                vcpus: 1,
                memory: Bytes::new(1),
                disk: Bytes::new(1),
            },
            options: &options,
        };
        let instance = InstanceId::try_new("i-one")?;
        let supply = FleetSupply {
            desired: 1,
            warm: 0,
            instances: vec![FleetInstance {
                id: instance.clone(),
                lifecycle: InstanceLifecycle::Pending,
                launched_at: Timestamp::from_unix_millis(0),
                release_first: false,
            }],
            exits: Vec::new(),
        };
        fake.set_supply(&name, supply.clone());
        let mut actual = FleetSupply::default();
        fake.supply(fleet, &mut actual).await?;
        assert_eq!(actual, supply);
        fake.fail_supply(true);
        assert!(fake.supply(fleet, &mut actual).await.is_err());
        assert_eq!(fake.supply_calls(), vec![(name.clone(), Isa::X86_64); 2]);
        fake.set_desired_capacity(fleet, 2).await?;
        fake.fail_set_desired_capacity(true);
        assert!(fake.set_desired_capacity(fleet, 3).await.is_err());
        assert_eq!(fake.desired_requests(), vec![(name.clone(), 2)]);
        fake.terminate(fleet, &instance).await?;
        fake.fail_terminate(true);
        assert!(fake.terminate(fleet, &instance).await.is_err());
        assert_eq!(fake.terminations(), vec![instance]);
        Ok(())
    }

    #[tokio::test]
    async fn worker_ports_report_configuration_and_count_scripted_failures() -> crate::TestResult {
        let fake = FakeProvisioner::default();
        assert!(fake.identity().await.is_err());
        fake.set_identity(InstanceIdentity {
            instance: InstanceId::try_new("test-instance")?,
            fleet: FleetName::try_new("test-fleet")?,
            isa: Isa::X86_64,
        });
        let identity = fake.identity().await?;
        assert_eq!(
            fake.mount(&identity.instance).await?.capacity,
            Bytes::new(0)
        );
        fake.set_capacity(Bytes::new(42));
        assert_eq!(
            fake.mount(&identity.instance).await?.capacity,
            Bytes::new(42)
        );
        fake.set_parked(true);
        fake.fail_parked_next(1);
        assert!(fake.parked().await.is_err());
        assert!(fake.parked().await?);
        assert_eq!(fake.parked_calls().get(), 2);
        fake.fail_workload_credential_next(1);
        assert!(fake.workload_credential().await.is_err());
        fake.workload_credential().await?;
        assert_eq!(fake.workload_credential_calls().get(), 2);
        fake.fail_workload_credential_forever();
        assert!(fake.workload_credential().await.is_err());
        Ok(())
    }
}
