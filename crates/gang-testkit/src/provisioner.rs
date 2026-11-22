//! An in-memory provisioner and worker-side ports for tests.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gang_clock::TestClock;
use gang_spi::{
    ControllerHostIdentity, ControllerIdentityPort, FleetSupply, Held, InstanceIdentity,
    LaunchFailure, MountedScratchVolume, NodeLifecyclePort, PortError, PreemptionNotice,
    ProvisionerPort, ScratchVolumePort, ServerEndpoints, Sidecar, TerminationMode,
    WorkloadCredentialPort,
};
use gang_types::{
    Bytes, Fleet, FleetName, InstanceId, InstanceLifecycle, Isa, NameError, Proof, WorkerLifetime,
};
use parking_lot::Mutex;

use crate::{CallCount, Opaque};

static NEXT_HOST: AtomicU64 = AtomicU64::new(1);

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
    supply: Script,
    acknowledge_exit: Script,
    set_desired_capacity: Script,
    terminate: Script,
}

/// A callback invoked before a convergence attempt.
type ConvergeHook = Arc<dyn Fn() -> Result<(), PortError> + Send + Sync>;

/// A fleet read or convergence attempt, in the order it was made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetCall {
    /// A convergence attempt for the named fleet.
    Converge(FleetName),
    /// A supply read for the named fleet.
    Supply(FleetName),
}

struct State {
    supplies: BTreeMap<FleetName, FleetSupply>,
    launch_failures: BTreeMap<FleetName, String>,
    supply_calls: Vec<(FleetName, Isa)>,
    fleet_calls: Vec<FleetCall>,
    converges: Vec<FleetName>,
    converge_hook: Option<ConvergeHook>,
    desired: Vec<(FleetName, u32)>,
    terminations: Vec<(InstanceId, TerminationMode)>,
    failures: Failures,
    advance_on_supply: Option<(Arc<TestClock>, Duration)>,
    identity: Option<InstanceIdentity>,
    controller_identity: Result<ControllerHostIdentity, NameError>,
    lifetime: WorkerLifetime,
    notice: Option<PreemptionNotice>,
    volume: PathBuf,
    capacity: Bytes,
    account_for_scratch_usage: bool,
    credentials: Script,
    parked: bool,
    attach: Script,
    parked_checks: Script,
}

/// A shared, configurable provisioner with recorded fleet calls and worker-side answers.
///
/// Termination honours [`ProvisionerPort::terminate`], removing live members and lowering the
/// fleet's desired capacity.
/// Setting the desired capacity to its current value withdraws abandoned requests; changing it
/// leaves them untouched.
///
/// The default has no node identity; configure one with [`Self::set_identity`] or [`Self::new`].
#[derive(Clone)]
pub struct FakeProvisioner {
    state: Arc<Mutex<State>>,
    credential_requests: CallCount,
    parked_polls: CallCount,
    attach_requests: CallCount,
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
                launch_failures: BTreeMap::new(),
                supply_calls: Vec::new(),
                fleet_calls: Vec::new(),
                converges: Vec::new(),
                converge_hook: None,
                desired: Vec::new(),
                terminations: Vec::new(),
                failures: Failures::default(),
                advance_on_supply: None,
                identity,
                controller_identity: InstanceId::try_new(format!(
                    "fake-host-{}",
                    NEXT_HOST.fetch_add(1, Ordering::Relaxed)
                ))
                .map(ControllerHostIdentity::Persistent),
                lifetime,
                notice: None,
                volume,
                capacity,
                account_for_scratch_usage: false,
                credentials: Script::default(),
                parked: false,
                parked_checks: Script::default(),
                attach: Script::default(),
            })),
            credential_requests: CallCount::new(),
            parked_polls: CallCount::new(),
            attach_requests: CallCount::new(),
        }
    }

    /// Sets the stable controller incarnation shared by clones of this fake.
    #[must_use]
    pub fn with_host_identity(self, identity: ControllerHostIdentity) -> Self {
        self.state.lock().controller_identity = Ok(identity);
        self
    }

    /// Configures the supply returned for a fleet, including its instance list.
    pub fn set_supply(&self, name: &FleetName, supply: FleetSupply) {
        self.state.lock().supplies.insert(name.clone(), supply);
    }
    /// Makes launches for a fleet fail with the given provider message until cleared.
    pub fn fail_launches(&self, name: &FleetName, reason: impl Into<String>) {
        self.state
            .lock()
            .launch_failures
            .insert(name.clone(), reason.into());
    }
    /// Clears the injected launch failure for a fleet.
    pub fn clear_launch_failure(&self, name: &FleetName) {
        self.state.lock().launch_failures.remove(name);
    }
    /// Advances `clock` by `by` whenever a supply is read, including failed reads.
    pub fn advance_on_supply(&self, clock: Arc<TestClock>, by: Duration) {
        self.state.lock().advance_on_supply = Some((clock, by));
    }
    /// Sets a callback run at the start of each convergence; its error aborts the call.
    pub fn on_converge(&self, hook: impl Fn() -> Result<(), PortError> + Send + Sync + 'static) {
        self.state.lock().converge_hook = Some(Arc::new(hook));
    }
    /// Returns supply reads and convergence attempts in their shared order, including failures.
    #[must_use]
    pub fn fleet_calls(&self) -> Vec<FleetCall> {
        self.state.lock().fleet_calls.clone()
    }
    /// Returns all supply queries, including failed ones, in order.
    #[must_use]
    pub fn supply_calls(&self) -> Vec<(FleetName, Isa)> {
        self.state.lock().supply_calls.clone()
    }
    /// Returns successful convergence requests in order.
    #[must_use]
    pub fn converge_requests(&self) -> Vec<FleetName> {
        self.state.lock().converges.clone()
    }
    /// Returns successful desired-capacity requests in order.
    #[must_use]
    pub fn desired_requests(&self) -> Vec<(FleetName, u32)> {
        self.state.lock().desired.clone()
    }
    /// Returns successful terminations in order.
    #[must_use]
    pub fn terminations(&self) -> Vec<InstanceId> {
        self.state
            .lock()
            .terminations
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }
    /// Returns successful terminations and their modes in order.
    #[must_use]
    pub fn termination_calls(&self) -> Vec<(InstanceId, TerminationMode)> {
        self.state.lock().terminations.clone()
    }
    /// Enables or disables supply failures.
    pub fn fail_supply(&self, fail: bool) {
        self.state.lock().failures.supply.forever = fail;
    }
    /// Enables or disables exit-acknowledgement failures.
    pub fn fail_acknowledge_exit(&self, fail: bool) {
        self.state.lock().failures.acknowledge_exit.forever = fail;
    }
    /// Enables or disables desired-capacity failures.
    pub fn fail_set_desired_capacity(&self, fail: bool) {
        self.state.lock().failures.set_desired_capacity.forever = fail;
    }
    /// Enables or disables termination failures.
    pub fn fail_terminate(&self, fail: bool) {
        self.state.lock().failures.terminate.forever = fail;
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
    /// Treats capacity as a total volume size and subtracts allocated scratch files on mount.
    ///
    /// This controlled model does not query the host's free space. Disabled by default.
    pub fn set_capacity_accounting(&self, enabled: bool) {
        self.state.lock().account_for_scratch_usage = enabled;
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
    /// Makes the next `count` provisioner attachments fail.
    pub fn fail_attach(&self, count: usize) {
        self.state.lock().attach.remaining = count;
    }

    /// Returns a waitable count of attachment attempts, including failures.
    #[must_use]
    pub fn attach_calls(&self) -> CallCount {
        self.attach_requests.clone()
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

    async fn attach(&self, _endpoints: &ServerEndpoints) -> Result<Option<Sidecar>, PortError> {
        self.attach_requests.bump();
        if self.state.lock().attach.fails() {
            return Err(PortError::unavailable(
                "provisioner attachment is unavailable",
            ));
        }
        Ok(None)
    }

    async fn supply(
        &self,
        fleet: Fleet<'_, Opaque>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        let mut state = self.state.lock();
        state
            .fleet_calls
            .push(FleetCall::Supply(fleet.name.clone()));
        state.supply_calls.push((fleet.name.clone(), fleet.isa));
        if let Some((clock, by)) = &state.advance_on_supply {
            clock.advance(*by);
        }
        if state.failures.supply.fails() {
            return Err(PortError::unavailable("fleet supply is unavailable"));
        }
        if let Some(supply) = state.supplies.get(fleet.name) {
            into.desired = supply.desired;
            into.requested = supply.requested;
            into.warm = supply.warm;
            into.abandoned = supply.abandoned;
            into.instances.clone_from(&supply.instances);
            into.exits.clone_from(&supply.exits);
            into.events.clone_from(&supply.events);
            into.launch_failure.clone_from(&supply.launch_failure);
        } else {
            into.desired = 0;
            into.requested = None;
            into.warm = 0;
            into.abandoned = 0;
            into.instances.clear();
            into.exits.clear();
            into.events.clear();
            into.launch_failure = None;
        }
        let live = into
            .instances
            .iter()
            .filter(|instance| instance.lifecycle != InstanceLifecycle::Terminating)
            .count();
        if u64::from(into.desired) <= live as u64 {
            into.launch_failure = None;
        } else if let Some(reason) = state.launch_failures.get(fleet.name) {
            into.launch_failure = Some(LaunchFailure {
                reason: reason.clone(),
            });
        }
        Ok(())
    }
    async fn acknowledge_exit(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        let mut state = self.state.lock();
        held.check()?;
        if state.failures.acknowledge_exit.fails() {
            return Err(PortError::unavailable(
                "exit acknowledgement is unavailable",
            ));
        }
        if let Some(supply) = state.supplies.get_mut(fleet.name) {
            supply.exits.retain(|exit| exit.instance != *instance);
        }
        Ok(())
    }
    async fn converge(&self, held: &Held, fleet: Fleet<'_, Opaque>) -> Result<(), PortError> {
        let hook = {
            let mut state = self.state.lock();
            state
                .fleet_calls
                .push(FleetCall::Converge(fleet.name.clone()));
            state.converge_hook.clone()
        };
        if let Some(hook) = hook {
            hook()?;
        }
        held.check()?;
        self.state.lock().converges.push(fleet.name.clone());
        Ok(())
    }
    async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        desired: u32,
    ) -> Result<(), PortError> {
        held.check()?;
        let mut state = self.state.lock();
        if state.failures.set_desired_capacity.fails() {
            return Err(PortError::unavailable("fleet capacity is unavailable"));
        }
        state.desired.push((fleet.name.clone(), desired));
        let supply = state.supplies.entry(fleet.name.clone()).or_default();
        if desired == supply.desired {
            supply.abandoned = 0;
        }
        let live = supply
            .instances
            .iter()
            .filter(|instance| instance.lifecycle != InstanceLifecycle::Terminating)
            .count();
        let live = u32::try_from(live).unwrap_or(u32::MAX);
        supply.desired = desired.max(live);
        Ok(())
    }
    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
        mode: TerminationMode,
    ) -> Result<(), PortError> {
        held.check()?;
        let mut state = self.state.lock();
        if state.failures.terminate.fails() {
            return Err(PortError::unavailable(
                "instance termination is unavailable",
            ));
        }
        state.terminations.push((instance.clone(), mode));
        if let Some(supply) = state.supplies.get_mut(fleet.name)
            && let Some(index) = supply.instances.iter().position(|member| {
                member.id == *instance && member.lifecycle != InstanceLifecycle::Terminating
            })
        {
            supply.instances.remove(index);
            supply.desired = supply.desired.saturating_sub(1);
        }
        Ok(())
    }
    fn worker_lifetime(&self) -> WorkerLifetime {
        self.state.lock().lifetime
    }
}

impl ControllerIdentityPort for FakeProvisioner {
    async fn incarnation(&self) -> Result<ControllerHostIdentity, PortError> {
        self.state
            .lock()
            .controller_identity
            .clone()
            .map_err(PortError::backend)
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
        let capacity = if state.account_for_scratch_usage {
            state
                .capacity
                .saturating_sub(gang_os::fs::usage(&state.volume).map_err(PortError::backend)?)
        } else {
            state.capacity
        };
        Ok(MountedScratchVolume {
            path: state.volume.clone(),
            capacity,
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
    use gang_spi::{FleetInstance, InfrastructureEvent, InfrastructureEventKind};
    use gang_types::{InstanceLifecycle, Resources};

    #[tokio::test]
    async fn controller_incarnations_are_unique_and_stable_across_clones() -> crate::TestResult {
        let first = FakeProvisioner::default();
        let clone = first.clone();
        let second = FakeProvisioner::default();
        let identity = first.incarnation().await?;
        assert_eq!(clone.incarnation().await?, identity);
        assert_ne!(second.incarnation().await?, identity);
        let explicit = ControllerHostIdentity::Persistent(InstanceId::try_new("controller")?);
        let configured = second.with_host_identity(explicit.clone());
        assert_eq!(configured.clone().incarnation().await?, explicit);
        Ok(())
    }

    #[tokio::test]
    async fn converge_records_the_fleet_and_changes_nothing() -> crate::TestResult {
        let fake = FakeProvisioner::default();
        let name = FleetName::try_new("workers")?;
        let options = Opaque::default();
        let fleet = Fleet {
            name: &name,
            isa: Isa::X86_64,
            instance: Resources::default(),
            options: &options,
        };
        fake.set_supply(
            &name,
            FleetSupply {
                desired: 2,
                abandoned: 1,
                ..FleetSupply::default()
            },
        );
        let mut before = FleetSupply::default();
        fake.supply(fleet, &mut before).await?;
        crate::fence::with_held(async |held| fake.converge(held, fleet).await).await?;
        let mut after = FleetSupply::default();
        fake.supply(fleet, &mut after).await?;
        assert_eq!(
            fake.fleet_calls(),
            vec![
                FleetCall::Supply(name.clone()),
                FleetCall::Converge(name.clone()),
                FleetCall::Supply(name.clone()),
            ]
        );
        assert_eq!(before, after);
        assert_eq!(fake.converge_requests(), vec![name]);
        Ok(())
    }

    #[tokio::test]
    async fn an_expired_permission_changes_nothing() -> crate::TestResult {
        let fake = FakeProvisioner::default();
        let name = FleetName::try_new("workers")?;
        let options = Opaque::default();
        let fleet = Fleet {
            name: &name,
            isa: Isa::X86_64,
            instance: Resources::default(),
            options: &options,
        };
        let instance = InstanceId::try_new("worker")?;
        fake.set_supply(
            &name,
            FleetSupply {
                desired: 1,
                instances: vec![FleetInstance {
                    id: instance.clone(),
                    lifecycle: InstanceLifecycle::InService,
                    launched_at: Timestamp::UNIX_EPOCH,
                    release_first: false,
                }],
                ..FleetSupply::default()
            },
        );
        let mut before = FleetSupply::default();
        fake.supply(fleet, &mut before).await?;
        assert!(matches!(
            crate::fence::with_expired_held(async |held| fake
                .set_desired_capacity(held, fleet, 2)
                .await)
            .await,
            Err(gang_spi::RunError::Failed(PortError::Unavailable(_)))
        ));
        assert!(matches!(
            crate::fence::with_expired_held(async |held| fake
                .terminate(held, fleet, &instance, TerminationMode::ScaleIn)
                .await)
            .await,
            Err(gang_spi::RunError::Failed(PortError::Unavailable(_)))
        ));
        assert!(matches!(
            crate::fence::with_expired_held(async |held| fake.converge(held, fleet).await).await,
            Err(gang_spi::RunError::Failed(PortError::Unavailable(_)))
        ));
        let mut after = FleetSupply::default();
        fake.supply(fleet, &mut after).await?;
        assert_eq!(before, after);
        assert!(fake.desired_requests().is_empty());
        assert!(fake.terminations().is_empty());
        assert!(fake.converge_requests().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn lowering_capacity_keeps_live_instances_and_excludes_terminating() -> crate::TestResult
    {
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
        let instances = vec![
            FleetInstance {
                id: InstanceId::try_new("pending")?,
                lifecycle: InstanceLifecycle::Pending,
                launched_at: Timestamp::from_unix_millis(0),
                release_first: false,
            },
            FleetInstance {
                id: InstanceId::try_new("serving")?,
                lifecycle: InstanceLifecycle::InService,
                launched_at: Timestamp::from_unix_millis(0),
                release_first: false,
            },
            FleetInstance {
                id: InstanceId::try_new("terminating")?,
                lifecycle: InstanceLifecycle::Terminating,
                launched_at: Timestamp::from_unix_millis(0),
                release_first: false,
            },
        ];
        fake.set_supply(
            &name,
            FleetSupply {
                abandoned: 0,
                desired: 5,
                instances,
                ..FleetSupply::default()
            },
        );
        crate::fence::with_held(async |held| fake.set_desired_capacity(held, fleet, 0).await)
            .await?;
        let mut supply = FleetSupply::default();
        fake.supply(fleet, &mut supply).await?;
        assert_eq!(supply.desired, 2);
        assert_eq!(supply.instances.len(), 3);
        crate::fence::with_held(async |held| fake.set_desired_capacity(held, fleet, 4).await)
            .await?;
        fake.supply(fleet, &mut supply).await?;
        assert_eq!(supply.desired, 4);
        Ok(())
    }

    #[tokio::test]
    async fn abandoned_requests_follow_the_provisioner_contract() -> crate::TestResult {
        let fake = FakeProvisioner::default();
        let name = FleetName::try_new("workers")?;
        let options = Opaque::default();
        let fleet = Fleet {
            name: &name,
            isa: Isa::X86_64,
            instance: Resources::default(),
            options: &options,
        };
        fake.set_supply(
            &name,
            FleetSupply {
                desired: 2,
                abandoned: 3,
                ..FleetSupply::default()
            },
        );
        crate::provisioner_contract::abandoned_requests_contract(&fake, fleet).await
    }

    #[tokio::test]
    async fn terminate_removes_the_instance_and_lowers_desired() -> crate::TestResult {
        let fake = FakeProvisioner::default();
        let name = FleetName::try_new("workers")?;
        let options = Opaque::default();
        let fleet = Fleet {
            name: &name,
            isa: Isa::X86_64,
            instance: Resources::default(),
            options: &options,
        };
        let first = InstanceId::try_new("first")?;
        let second = InstanceId::try_new("second")?;
        fake.set_supply(
            &name,
            FleetSupply {
                abandoned: 0,
                desired: 2,
                instances: vec![
                    FleetInstance {
                        id: first.clone(),
                        lifecycle: InstanceLifecycle::InService,
                        launched_at: Timestamp::UNIX_EPOCH,
                        release_first: false,
                    },
                    FleetInstance {
                        id: second.clone(),
                        lifecycle: InstanceLifecycle::InService,
                        launched_at: Timestamp::UNIX_EPOCH,
                        release_first: false,
                    },
                ],
                ..FleetSupply::default()
            },
        );

        crate::fence::with_held(async |held| {
            fake.terminate(held, fleet, &first, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        let mut supply = FleetSupply::default();
        fake.supply(fleet, &mut supply).await?;
        assert_eq!(supply.desired, 1);
        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, second);

        let unknown = InstanceId::try_new("unknown")?;
        crate::fence::with_held(async |held| {
            fake.terminate(held, fleet, &unknown, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        crate::fence::with_held(async |held| {
            fake.terminate(held, fleet, &first, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        let mut after = FleetSupply::default();
        fake.supply(fleet, &mut after).await?;
        assert_eq!(after, supply);

        supply.instances[0].lifecycle = InstanceLifecycle::Terminating;
        fake.set_supply(&name, supply.clone());
        crate::fence::with_held(async |held| {
            fake.terminate(held, fleet, &second, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        fake.supply(fleet, &mut after).await?;
        assert_eq!(after, supply);

        supply.desired = 0;
        supply.instances[0].lifecycle = InstanceLifecycle::Pending;
        fake.set_supply(&name, supply);
        crate::fence::with_held(async |held| {
            fake.terminate(held, fleet, &second, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        fake.supply(fleet, &mut after).await?;
        assert_eq!(after.desired, 0);
        assert!(after.instances.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn launch_failure_is_reported_only_for_unmet_requests() -> crate::TestResult {
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
        fake.fail_launches(&name, "quota exceeded");
        let mut supply = FleetSupply::default();
        fake.supply(fleet, &mut supply).await?;
        assert!(supply.launch_failure.is_none());
        crate::fence::with_held(async |held| fake.set_desired_capacity(held, fleet, 2).await)
            .await?;
        fake.supply(fleet, &mut supply).await?;
        assert_eq!(supply.desired, 2);
        assert!(supply.instances.is_empty());
        assert_eq!(
            supply
                .launch_failure
                .as_ref()
                .map(|failure| failure.reason.as_str()),
            Some("quota exceeded")
        );
        let other_name = FleetName::try_new("other")?;
        let other = Fleet {
            name: &other_name,
            ..fleet
        };
        crate::fence::with_held(async |held| fake.set_desired_capacity(held, other, 1).await)
            .await?;
        fake.supply(other, &mut supply).await?;
        assert!(supply.launch_failure.is_none());
        crate::fence::with_held(async |held| fake.set_desired_capacity(held, fleet, 0).await)
            .await?;
        fake.supply(fleet, &mut supply).await?;
        assert!(supply.launch_failure.is_none());
        crate::fence::with_held(async |held| fake.set_desired_capacity(held, fleet, 1).await)
            .await?;
        fake.clear_launch_failure(&name);
        fake.supply(fleet, &mut supply).await?;
        assert!(supply.launch_failure.is_none());
        Ok(())
    }

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
            abandoned: 0,
            desired: 1,
            requested: Some(1),
            warm: 0,
            instances: vec![FleetInstance {
                id: instance.clone(),
                lifecycle: InstanceLifecycle::Pending,
                launched_at: Timestamp::from_unix_millis(0),
                release_first: false,
            }],
            exits: Vec::new(),
            events: vec![InfrastructureEvent {
                id: "event-1".to_owned(),
                at: Timestamp::from_unix_millis(1),
                kind: InfrastructureEventKind::CapacityUnavailable {
                    reason: "capacity unavailable".to_owned(),
                },
            }],
            launch_failure: None,
        };
        fake.set_supply(&name, supply.clone());
        let mut actual = FleetSupply::default();
        fake.supply(fleet, &mut actual).await?;
        assert_eq!(actual, supply);
        fake.fail_supply(true);
        assert!(fake.supply(fleet, &mut actual).await.is_err());
        assert_eq!(fake.supply_calls(), vec![(name.clone(), Isa::X86_64); 2]);
        assert_eq!(
            fake.fleet_calls(),
            vec![
                FleetCall::Supply(name.clone()),
                FleetCall::Supply(name.clone())
            ]
        );
        crate::fence::with_held(async |held| fake.set_desired_capacity(held, fleet, 2).await)
            .await?;
        fake.fail_set_desired_capacity(true);
        assert!(
            crate::fence::with_held(async |held| fake.set_desired_capacity(held, fleet, 3).await)
                .await
                .is_err()
        );
        assert_eq!(fake.desired_requests(), vec![(name.clone(), 2)]);
        crate::fence::with_held(async |held| {
            fake.terminate(held, fleet, &instance, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        fake.fail_terminate(true);
        assert!(
            crate::fence::with_held(async |held| {
                fake.terminate(held, fleet, &instance, TerminationMode::ScaleIn)
                    .await
            })
            .await
            .is_err()
        );
        assert_eq!(
            fake.termination_calls(),
            vec![(instance.clone(), TerminationMode::ScaleIn)]
        );
        assert_eq!(fake.terminations(), vec![instance]);
        fake.fail_supply(false);
        fake.supply(fleet, &mut actual).await?;
        assert_eq!(actual.events, supply.events);
        assert_eq!(actual.requested, supply.requested);
        let unknown_name = FleetName::try_new("unknown")?;
        let unknown = Fleet {
            name: &unknown_name,
            ..fleet
        };
        fake.supply(unknown, &mut actual).await?;
        assert_eq!(actual, FleetSupply::default());
        fake.supply(fleet, &mut actual).await?;
        fake.set_supply(&unknown_name, FleetSupply::default());
        fake.supply(unknown, &mut actual).await?;
        assert_eq!(actual, FleetSupply::default());
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
