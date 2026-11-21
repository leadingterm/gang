use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::actions::ActionsCache;
use crate::control::{ControlLoop, PassOutcome};
use crate::hub::{HubConfig, StateHub};
use crate::publish::StatePublisher;
use futures::poll;
use gang_clock::{Clock, Timestamp};
use gang_spi::{
    ExecutingDemand, Fence, FleetInstance, FleetSupply, Held, InstanceExit, PortError,
    ProvisionerPort, ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, SchedulerSnapshot,
    WorkerState,
};
use gang_state::BuilderConfig;
use gang_types::{
    Fleet, FleetName, FleetSet, InstanceId, InstanceLifecycle, Isa, NonZeroDuration, Resources,
    ScalingCause,
};
use gang_wire::{EventChange, Pass, PassState, SkipReason, State, name};

use crate::test_support::{clock_at, fleets, fleets_of, queued_demand};
use gang_testkit::{
    FakeProvisioner, FakeRepository, FakeScheduler, FleetCall, Opaque, TestResult, worker_on,
};

struct LapsingProvisioner {
    inner: FakeProvisioner,
    clock: Arc<gang_clock::TestClock>,
    lapse: AtomicBool,
}

impl ProvisionerPort for LapsingProvisioner {
    type FleetOptions = Opaque;

    async fn supply(
        &self,
        fleet: Fleet<'_, Opaque>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        self.inner.supply(fleet, into).await
    }

    async fn acknowledge_exit(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.inner.acknowledge_exit(held, fleet, instance).await
    }

    async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        desired: u32,
    ) -> Result<(), PortError> {
        self.inner.set_desired_capacity(held, fleet, desired).await
    }

    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.inner.terminate(held, fleet, instance).await?;
        if self.lapse.swap(false, Ordering::SeqCst) {
            self.clock.advance(Duration::from_secs(8));
        }
        Ok(())
    }

    fn worker_lifetime(&self) -> gang_types::WorkerLifetime {
        self.inner.worker_lifetime()
    }
}

struct CommittedTerminationProvisioner {
    inner: FakeProvisioner,
    fail_after_commit: AtomicBool,
    block_converge: AtomicBool,
}

impl ProvisionerPort for CommittedTerminationProvisioner {
    type FleetOptions = Opaque;

    async fn supply(
        &self,
        fleet: Fleet<'_, Opaque>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        self.inner.supply(fleet, into).await
    }

    async fn converge(&self, held: &Held, fleet: Fleet<'_, Opaque>) -> Result<(), PortError> {
        if self.block_converge.swap(false, Ordering::SeqCst) {
            return Err(PortError::unavailable(std::io::Error::from(
                std::io::ErrorKind::WouldBlock,
            )));
        }
        self.inner.converge(held, fleet).await
    }

    async fn acknowledge_exit(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.inner.acknowledge_exit(held, fleet, instance).await
    }

    async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        desired: u32,
    ) -> Result<(), PortError> {
        self.inner.set_desired_capacity(held, fleet, desired).await
    }

    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.inner.terminate(held, fleet, instance).await?;
        if self.fail_after_commit.swap(false, Ordering::SeqCst) {
            self.block_converge.store(true, Ordering::SeqCst);
            return Err(PortError::unavailable("termination deadline elapsed"));
        }
        Ok(())
    }

    fn worker_lifetime(&self) -> gang_types::WorkerLifetime {
        self.inner.worker_lifetime()
    }
}

#[tokio::test]
async fn mid_pass_lapse_forgets_unapplied_termination() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = LapsingProvisioner {
        inner: FakeProvisioner::default(),
        clock: Arc::clone(&clock),
        lapse: AtomicBool::new(true),
    };
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let first = instance_id("local-1");
    let second = instance_id("local-2");
    let mut supply = supply_with("local-1", clock.wall(), 2);
    supply.instances.push(FleetInstance {
        id: second.clone(),
        lifecycle: InstanceLifecycle::InService,
        launched_at: clock.wall(),
        release_first: false,
    });
    fleet.inner.set_supply(&fleet_name(), supply);
    scheduler.set_snapshot(SchedulerSnapshot {
        cordons: vec![owned_cordon(&first), owned_cordon(&second)],
        ..snapshot_with(vec![
            WorkerState {
                draining: true,
                ..idle_worker(&first, clock.wall())?
            },
            WorkerState {
                draining: true,
                ..idle_worker(&second, clock.wall())?
            },
        ])
    });
    let id = gang_spi::ControllerId::new("test");
    let leader = crate::leadership::Leadership::new(
        &repository,
        &clock,
        &id,
        gang_config::LeadershipConfig::default(),
    );
    let mut lease = leader
        .acquire(
            &gang_spi::LeaseCandidate {
                holder: id.clone(),
                priority: gang_spi::CandidatePriority::registered(
                    gang_spi::CandidateGeneration::default(),
                ),
            },
            &crate::health::Readiness::default(),
            Duration::ZERO,
        )
        .await;
    let hold = leader.hold(&mut lease);
    futures::pin_mut!(hold);
    assert!(poll!(hold.as_mut()).is_pending());
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        leader.fence().clone(),
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 1
        }
    );
    assert_eq!(fleet.inner.terminations().len(), 1);
    assert!(poll!(hold.as_mut()).is_pending());
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0
        }
    );
    assert_eq!(fleet.inner.terminations().len(), 2);
    Ok(())
}

const POLL_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(1);
const TICK: Duration = Duration::from_millis(1);
const BEFORE_POLL: Duration = POLL_INTERVAL.get().checked_sub(TICK).unwrap();

fn fleet_name() -> FleetName {
    FleetName::try_new("example-fleet").expect("example-fleet is a valid fleet name")
}

fn instance_id(name: &str) -> InstanceId {
    InstanceId::try_new(name).expect("test instance names are valid")
}

fn queued_snapshot() -> Result<SchedulerSnapshot, PortError> {
    Ok(SchedulerSnapshot {
        queued: vec![queued_demand(
            FleetSet::prefix(1).map_err(PortError::backend)?,
        )],
        ..SchedulerSnapshot::default()
    })
}

fn owned_cordon(instance: &InstanceId) -> gang_spi::InstanceCordon {
    gang_spi::InstanceCordon {
        fleet: 0,
        instance: Arc::new(instance.clone()),
    }
}

fn source_profiles() -> gang_reconciler::SourceProfiles {
    gang_reconciler::SourceProfiles::new(
        &gang_config::ScalingConfig::default(),
        gang_config::ActionsConfig::default().history_window,
    )
}

/// A supply with one in-service instance `name` launched at `launched_at`, and desired capacity
/// `desired`.
fn supply_with(name: &str, launched_at: Timestamp, desired: u32) -> FleetSupply {
    FleetSupply {
        requested: None,
        events: Vec::new(),
        abandoned: 0,
        desired,
        instances: vec![FleetInstance {
            id: instance_id(name),
            lifecycle: InstanceLifecycle::InService,
            launched_at,
            release_first: false,
        }],
        warm: 0,
        exits: Vec::new(),
        launch_failure: None,
    }
}

/// An idle worker on `instance`, connected at `last_seen`.
fn idle_worker(instance: &InstanceId, last_seen: Timestamp) -> TestResult<WorkerState> {
    Ok(WorkerState {
        id: worker_on(instance)?,
        fleet: fleet_name(),
        environment: Some("host:test".parse().expect("valid test environment")),
        running_actions: 0,
        max_inflight_actions: NonZeroU32::new(4),
        draining: false,
        last_seen,
        paused: false,
    })
}

#[tokio::test]
async fn lapsed_leader_does_not_record_worker_registration() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let id = gang_spi::ControllerId::new("test");
    let leader = crate::leadership::Leadership::new(
        &repository,
        &clock,
        &id,
        gang_config::LeadershipConfig::default(),
    );
    let worker = idle_worker(&instance_id("local-1"), clock.wall())?;
    scheduler.set_snapshot(snapshot_with(vec![worker]));
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        leader.fence().clone(),
    );
    control.pass().await;
    assert!(repository.scaling_events().is_empty());
    let _lease = leader
        .acquire(
            &gang_spi::LeaseCandidate {
                holder: id.clone(),
                priority: gang_spi::CandidatePriority::registered(
                    gang_spi::CandidateGeneration::default(),
                ),
            },
            &crate::health::Readiness::default(),
            Duration::ZERO,
        )
        .await;
    control.pass().await;
    assert_eq!(repository.scaling_events().len(), 1);
    Ok(())
}

#[tokio::test]
async fn lapsed_leader_converges_no_fleet() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let (fence, _keeper) = Fence::lease(
        Arc::clone(&clock) as Arc<dyn Clock>,
        gang_config::LeadershipConfig::default().call_timeout,
    );
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        fence,
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::LeadershipLapsed)
    );
    assert!(fleet.converge_requests().is_empty());
    assert!(fleet.desired_requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_lease_lapsing_during_converge_skips_the_pass_as_lapsed() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let hook_clock = Arc::clone(&clock);
    fleet.on_converge(move || {
        hook_clock.advance(Duration::from_secs(11));
        Err(PortError::unavailable(std::io::Error::other(
            "connection reset",
        )))
    });
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let (fence, keeper) = Fence::lease(
        Arc::clone(&clock) as Arc<dyn Clock>,
        NonZeroDuration::from_secs(2),
    );
    keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        fence,
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::LeadershipLapsed),
        "a lease lapsing during converge was reported as a provisioner failure"
    );
    Ok(())
}

#[tokio::test]
async fn a_converge_failure_while_the_lease_holds_skips_as_provisioner_unavailable() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    fleet.on_converge(|| {
        Err(PortError::unavailable(std::io::Error::other(
            "connection reset",
        )))
    });
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let (fence, keeper) = Fence::lease(
        Arc::clone(&clock) as Arc<dyn Clock>,
        NonZeroDuration::from_secs(2),
    );
    keeper.extend_to(clock.monotonic() + Duration::from_secs(10));
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        fence,
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::ProvisionerUnavailable),
        "a converge failure while the lease holds was not reported as provisioner unavailable"
    );
    Ok(())
}

#[tokio::test]
async fn leader_converges_every_fleet_before_reading_it() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets_of(vec![("second", 4), ("first", 4)]);
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    control.pass().await;
    assert_eq!(
        fleet.fleet_calls(),
        vec![
            FleetCall::Converge(FleetName::try_new("second")?),
            FleetCall::Supply(FleetName::try_new("second")?),
            FleetCall::Converge(FleetName::try_new("first")?),
            FleetCall::Supply(FleetName::try_new("first")?),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_supplied_exit_is_recorded_as_an_exited_early_event() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let instance = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            exits: vec![InstanceExit {
                instance: instance.clone(),
                failure: "fail  worker failed".to_owned(),
            }],
            ..FleetSupply::default()
        },
    );
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    control.pass().await;
    assert_eq!(
        repository.scaling_events(),
        vec![ScalingAuditEvent {
            at: clock.wall(),
            fleet: fleet_name(),
            kind: ScalingEventKind::ExitedEarly {
                instance,
                failure: "fail  worker failed".to_owned(),
            },
            cause: None,
        }]
    );
    control.pass().await;
    assert_eq!(repository.scaling_events().len(), 1);
    Ok(())
}

#[tokio::test]
async fn exit_survives_failed_audit() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let instance = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            requested: None,
            events: Vec::new(),
            exits: vec![InstanceExit {
                instance: instance.clone(),
                failure: "fail  worker failed".to_owned(),
            }],
            ..FleetSupply::default()
        },
    );
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    repository.fail_scaling_events(true);
    control.pass().await;
    assert!(repository.scaling_events().is_empty());
    drop(control);

    repository.fail_scaling_events(false);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    control.pass().await;
    let expected = vec![ScalingAuditEvent {
        at: clock.wall(),
        fleet: fleet_name(),
        kind: ScalingEventKind::ExitedEarly {
            instance,
            failure: "fail  worker failed".to_owned(),
        },
        cause: None,
    }];
    assert_eq!(repository.scaling_events(), expected);
    control.pass().await;
    assert_eq!(repository.scaling_events(), expected);
    Ok(())
}

#[tokio::test]
async fn exit_survives_leadership_lapse() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let instance = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            requested: None,
            events: Vec::new(),
            exits: vec![InstanceExit {
                instance: instance.clone(),
                failure: "fail  worker failed".to_owned(),
            }],
            ..FleetSupply::default()
        },
    );
    let id = gang_spi::ControllerId::new("test");
    let leader = crate::leadership::Leadership::new(
        &repository,
        &clock,
        &id,
        gang_config::LeadershipConfig::default(),
    );
    let mut lease = leader
        .acquire(
            &gang_spi::LeaseCandidate {
                holder: id.clone(),
                priority: gang_spi::CandidatePriority::registered(
                    gang_spi::CandidateGeneration::default(),
                ),
            },
            &crate::health::Readiness::default(),
            Duration::ZERO,
        )
        .await;
    let hold = leader.hold(&mut lease);
    futures::pin_mut!(hold);
    assert!(poll!(hold.as_mut()).is_pending());
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        leader.fence().clone(),
    );

    fleet.advance_on_supply(Arc::clone(&clock), Duration::from_secs(8));
    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::LeadershipLapsed)
    );
    assert!(repository.scaling_events().is_empty());
    drop(control);

    fleet.advance_on_supply(Arc::clone(&clock), Duration::ZERO);
    assert!(poll!(hold.as_mut()).is_pending());
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        leader.fence().clone(),
    );
    control.pass().await;
    let expected = vec![ScalingAuditEvent {
        at: clock.wall(),
        fleet: fleet_name(),
        kind: ScalingEventKind::ExitedEarly {
            instance,
            failure: "fail  worker failed".to_owned(),
        },
        cause: None,
    }];
    assert_eq!(repository.scaling_events(), expected);
    control.pass().await;
    assert_eq!(repository.scaling_events(), expected);
    Ok(())
}

#[tokio::test]
async fn exit_survives_failed_acknowledgement() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let instance = instance_id("local-1");
    let exit = InstanceExit {
        instance: instance.clone(),
        failure: "fail  worker failed".to_owned(),
    };
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            requested: None,
            events: Vec::new(),
            exits: vec![exit.clone()],
            ..FleetSupply::default()
        },
    );
    fleet.fail_acknowledge_exit(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    control.pass().await;
    let event = ScalingAuditEvent {
        at: clock.wall(),
        fleet: fleet_name(),
        kind: ScalingEventKind::ExitedEarly {
            instance,
            failure: exit.failure.clone(),
        },
        cause: None,
    };
    assert_eq!(repository.scaling_events(), vec![event.clone()]);
    let config = fleets.iter().next().expect("fixture has a fleet");
    let mut supply = FleetSupply::default();
    fleet.supply(config.fleet(), &mut supply).await?;
    assert_eq!(supply.exits, vec![exit]);

    drop(control);
    fleet.fail_acknowledge_exit(false);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    control.pass().await;
    assert_eq!(repository.scaling_events(), vec![event.clone(), event]);
    fleet.supply(config.fleet(), &mut supply).await?;
    assert!(supply.exits.is_empty());
    let recorded = repository.scaling_events();
    control.pass().await;
    assert_eq!(repository.scaling_events(), recorded);
    Ok(())
}

#[tokio::test]
async fn launch_failures_are_recorded_once_per_reason_until_request_is_met() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    let mut supply = supply_with("local-1", clock.wall(), 3);
    supply.instances.push(FleetInstance {
        id: instance_id("local-terminating"),
        lifecycle: InstanceLifecycle::Terminating,
        launched_at: clock.wall(),
        release_first: false,
    });
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "first".to_owned(),
    });
    for _ in 0..3 {
        fleet.set_supply(&fleet_name(), supply.clone());
        control.pass().await;
    }
    let failures = || {
        repository
            .scaling_events()
            .into_iter()
            .filter(|event| matches!(event.kind, ScalingEventKind::LaunchFailed { .. }))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        failures(),
        vec![ScalingAuditEvent {
            at: clock.wall(),
            fleet: fleet_name(),
            kind: ScalingEventKind::LaunchFailed {
                from: 1,
                to: 3,
                reason: "first".to_owned()
            },
            cause: None,
        }]
    );
    clock.advance(Duration::from_secs(1));
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "second".to_owned(),
    });
    fleet.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    assert_eq!(failures().len(), 2);
    assert_eq!(failures()[1].at, clock.wall());
    assert!(
        matches!(&failures()[1].kind, ScalingEventKind::LaunchFailed { from: 1, to: 3, reason } if reason == "second")
    );
    supply.launch_failure = None;
    fleet.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "second".to_owned(),
    });
    fleet.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    assert_eq!(failures().len(), 2);
    supply.desired = 1;
    fleet.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    supply.desired = 3;
    fleet.set_supply(&fleet_name(), supply);
    control.pass().await;
    assert_eq!(failures().len(), 3);
    assert!(
        matches!(&failures()[2].kind, ScalingEventKind::LaunchFailed { from: 1, to: 3, reason } if reason == "second")
    );
    Ok(())
}

#[tokio::test]
async fn failed_unneeded_request_is_withdrawn_and_audited() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let mut supply = supply_with("local-1", clock.wall(), 3);
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "capacity".to_owned(),
    });
    fleet.set_supply(&fleet_name(), supply);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    control.pass().await;
    assert_eq!(fleet.desired_requests(), vec![(fleet_name(), 1)]);
    assert!(repository.scaling_events().iter().any(|event| event.kind
        == (ScalingEventKind::Withdrawn { from: 3, to: 1 })
        && event.cause == Some(ScalingCause::Demand)
        && event.at == clock.wall()));
    Ok(())
}

#[tokio::test]
async fn an_abandoned_request_is_withdrawn_and_audited_once() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let instance = instance_id("local-1");
    let mut supply = supply_with("local-1", clock.wall(), 1);
    supply.abandoned = 2;
    fleet.set_supply(&fleet_name(), supply);
    scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![idle_worker(&instance, clock.wall())?],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    control.pass().await;
    control.pass().await;

    let withdrawals: Vec<_> = repository
        .scaling_events()
        .into_iter()
        .filter(|event| matches!(event.kind, ScalingEventKind::Withdrawn { .. }))
        .map(|event| event.kind)
        .collect();
    assert_eq!(
        withdrawals,
        vec![ScalingEventKind::Withdrawn { from: 3, to: 1 }],
        "abandoned requests are withdrawn and audited exactly once"
    );
    Ok(())
}

#[tokio::test]
async fn terminations_and_withdrawal_in_one_pass_never_raise_desired() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let first = instance_id("local-1");
    let second = instance_id("local-2");
    let mut supply = supply_with("local-1", clock.wall(), 3);
    supply.instances.push(FleetInstance {
        id: second.clone(),
        lifecycle: InstanceLifecycle::InService,
        launched_at: clock.wall(),
        release_first: false,
    });
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "capacity".to_owned(),
    });
    fleet.set_supply(&fleet_name(), supply);
    scheduler.set_snapshot(SchedulerSnapshot {
        cordons: vec![owned_cordon(&first), owned_cordon(&second)],
        workers: vec![
            WorkerState {
                draining: true,
                ..idle_worker(&first, clock.wall())?
            },
            WorkerState {
                draining: true,
                ..idle_worker(&second, clock.wall())?
            },
        ],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    control.pass().await;

    let name = fleet_name();
    let options = Opaque::default();
    let descriptor = Fleet {
        name: &name,
        isa: Isa::X86_64,
        instance: Resources::default(),
        options: &options,
    };
    let mut actual = FleetSupply::default();
    fleet.supply(descriptor, &mut actual).await?;
    assert_eq!(actual.desired, 0);
    assert_eq!(fleet.desired_requests(), vec![(name, 0)]);
    assert!(repository.scaling_events().iter().any(|event| event.kind
        == (ScalingEventKind::Withdrawn { from: 1, to: 0 })
        && event.cause == Some(ScalingCause::Demand)));
    Ok(())
}

#[tokio::test]
async fn queued_action_scales_out_from_an_empty_fleet() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    scheduler.set_snapshot(queued_snapshot()?);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert_eq!(fleet.desired_requests(), vec![(fleet_name(), 1)]);
    let events = repository.scaling_events();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0].kind,
        ScalingEventKind::ScaledOut {
            from: 0,
            to: 1,
            detail: Some(_)
        }
    ));
    assert_eq!(events[0].cause, Some(ScalingCause::Demand));
    Ok(())
}

#[tokio::test]
async fn failed_capacity_request_records_no_audit_event() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    scheduler.set_snapshot(queued_snapshot()?);
    fleet.fail_set_desired_capacity(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 1,
        }
    );
    assert!(repository.scaling_events().is_empty());
    Ok(())
}

#[tokio::test]
async fn failed_snapshot_skips_the_pass_without_touching_the_fleet() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    scheduler.set_snapshot(queued_snapshot()?);
    scheduler.fail_snapshot(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::SchedulerUnavailable)
    );
    assert!(fleet.desired_requests().is_empty());
    assert!(repository.scaling_events().is_empty());
    Ok(())
}

#[tokio::test]
async fn failed_supply_skips_the_pass_without_applying_steps() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    scheduler.set_snapshot(queued_snapshot()?);
    fleet.fail_supply(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::ProvisionerUnavailable)
    );
    assert!(fleet.desired_requests().is_empty());
    assert!(repository.scaling_events().is_empty());
    Ok(())
}

#[tokio::test]
async fn failed_supply_defers_worker_registration_until_a_completed_pass() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let hub = hub();
    let instance = instance_id("local-1");
    scheduler.set_snapshot(snapshot_with(vec![idle_worker(&instance, clock.wall())?]));
    fleet.fail_supply(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));

    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::ProvisionerUnavailable)
    );
    assert!(repository.scaling_events().is_empty());
    assert!(served(&hub).events.is_empty());

    fleet.fail_supply(false);
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    let events = repository.scaling_events();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].kind,
        ScalingEventKind::WorkerRegistered { worker } if *worker == worker_on(&instance)?
    ));
    let state = served(&hub);
    assert_eq!(state.events.len(), 1);
    assert!(matches!(
        &state.events[0].change,
        EventChange::WorkerRegistered { worker }
            if *worker == name::of::<name::Workers>(&worker_on(&instance)?)
    ));
    Ok(())
}

#[tokio::test]
async fn unconfigured_fleet_worker_joins_and_leaves_without_audit_events() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let hub = hub();
    let mut worker = idle_worker(&instance_id("local-1"), clock.wall())?;
    worker.fleet = FleetName::try_new("unknown").expect("fleet name is valid");
    scheduler.set_snapshot(snapshot_with(vec![worker]));
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));

    assert!(matches!(
        control.pass().await,
        PassOutcome::Completed { .. }
    ));
    assert!(repository.scaling_events().is_empty());
    assert!(served(&hub).events.is_empty());
    scheduler.set_snapshot(SchedulerSnapshot::default());
    assert!(matches!(
        control.pass().await,
        PassOutcome::Completed { .. }
    ));
    assert!(repository.scaling_events().is_empty());
    assert!(served(&hub).events.is_empty());
    Ok(())
}

#[tokio::test]
async fn drain_of_a_worker_that_left_is_neither_applied_nor_audited() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let name = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        supply_with("local-1", Timestamp::from_unix_millis(0), 1),
    );
    scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![idle_worker(&name, clock.wall())?],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    clock.advance(Duration::from_secs(15));
    scheduler.set_snapshot(SchedulerSnapshot::default());

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    let kinds: Vec<ScalingEventKind> = repository
        .scaling_events()
        .into_iter()
        .map(|event| event.kind)
        .collect();
    assert!(matches!(
        kinds.as_slice(),
        [
            ScalingEventKind::WorkerRegistered { .. },
            ScalingEventKind::WorkerDeregistered { .. },
        ]
    ));
    Ok(())
}

#[tokio::test]
async fn idle_worker_is_drained_then_terminated_once_empty() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let name = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        supply_with("local-1", Timestamp::from_unix_millis(0), 1),
    );
    scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![idle_worker(&name, clock.wall())?],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    // A new term starts no drain before its clients' demand can have rerouted.
    clock.advance(Duration::from_secs(16).max(gang_types::health::CLIENT_REROUTE_BOUND));
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    scheduler.set_snapshot(SchedulerSnapshot {
        cordons: vec![owned_cordon(&name)],
        workers: vec![WorkerState {
            draining: true,
            ..idle_worker(&name, clock.wall())?
        }],
        ..SchedulerSnapshot::default()
    });
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert!(fleet.terminations().contains(&name));

    let kinds: Vec<ScalingEventKind> = repository
        .scaling_events()
        .into_iter()
        .map(|event| event.kind)
        .collect();
    assert!(matches!(
        kinds.as_slice(),
        [
            ScalingEventKind::WorkerRegistered { .. },
            ScalingEventKind::DrainStarted { .. },
            ScalingEventKind::Terminated { .. },
        ]
    ));
    let causes: Vec<Option<ScalingCause>> = repository
        .scaling_events()
        .into_iter()
        .map(|event| event.cause)
        .collect();
    assert_eq!(
        causes,
        [
            None,
            Some(ScalingCause::IdleTimeout),
            Some(ScalingCause::DrainFinished),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_draining_worker_with_new_demand_is_resumed() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let name = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        supply_with("local-1", Timestamp::from_unix_millis(0), 1),
    );
    scheduler.set_snapshot(SchedulerSnapshot {
        cordons: vec![owned_cordon(&name)],
        workers: vec![WorkerState {
            draining: true,
            ..idle_worker(&name, clock.wall())?
        }],
        ..queued_snapshot()?
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert_eq!(
        scheduler.draining_calls(),
        vec![(fleet_name(), name.clone(), false)]
    );
    assert!(fleet.desired_requests().is_empty());

    let events = repository.scaling_events();
    let kinds: Vec<ScalingEventKind> = events.iter().map(|event| event.kind.clone()).collect();
    assert!(matches!(
        kinds.as_slice(),
        [
            ScalingEventKind::WorkerRegistered { .. },
            ScalingEventKind::DrainCancelled { .. },
        ]
    ));
    assert_eq!(events[1].cause, Some(ScalingCause::Demand));
    Ok(())
}

#[tokio::test]
async fn an_idle_worker_is_drained() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let name = instance_id("local-1");
    let idle_timeout = fleets
        .policies()
        .first()
        .expect("the test fleets configure one fleet")
        .idle_timeout;
    fleet.set_supply(
        &fleet_name(),
        supply_with("local-1", Timestamp::from_unix_millis(0), 1),
    );
    scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![idle_worker(&name, clock.wall())?],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    assert!(scheduler.draining_calls().is_empty());
    // A new term starts no drain before its clients' demand can have rerouted.
    clock.advance(idle_timeout.max(gang_types::health::CLIENT_REROUTE_BOUND));
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert_eq!(
        scheduler.draining_calls(),
        vec![(fleet_name(), name.clone(), true)]
    );
    assert!(fleet.terminations().is_empty());

    let events = repository.scaling_events();
    let kinds: Vec<ScalingEventKind> = events.iter().map(|event| event.kind.clone()).collect();
    assert!(matches!(
        kinds.as_slice(),
        [
            ScalingEventKind::WorkerRegistered { .. },
            ScalingEventKind::DrainStarted { .. },
        ]
    ));
    assert_eq!(events[1].cause, Some(ScalingCause::IdleTimeout));
    Ok(())
}

#[tokio::test]
async fn early_exit_clears_only_the_matching_fleet_termination() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets_of(vec![("first", 4), ("second", 4)]);
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let first = FleetName::try_new("first")?;
    let second = FleetName::try_new("second")?;
    let name = instance_id("shared-1");
    for fleet_name in [&first, &second] {
        fleet.set_supply(
            fleet_name,
            supply_with("shared-1", Timestamp::from_unix_millis(0), 1),
        );
    }
    let boot_grace = fleets
        .policies()
        .iter()
        .map(|policy| policy.boot_grace)
        .max()
        .expect("two fleets");
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0
        }
    );
    clock.advance(boot_grace);
    fleet.fail_terminate(true);
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 2
        }
    );
    fleet.fail_terminate(false);
    fleet.set_supply(
        &first,
        FleetSupply {
            exits: vec![InstanceExit {
                instance: name.clone(),
                failure: "worker exited".to_owned(),
            }],
            ..FleetSupply::default()
        },
    );
    fleet.set_supply(&second, FleetSupply::default());
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0
        }
    );
    assert_eq!(fleet.terminations(), vec![name]);
    let events = repository.scaling_events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].fleet, first);
    assert!(matches!(
        events[0].kind,
        ScalingEventKind::ExitedEarly { .. }
    ));
    assert_eq!(events[1].fleet, second);
    assert!(matches!(
        events[1].kind,
        ScalingEventKind::Terminated { .. }
    ));
    assert_eq!(events[1].cause, Some(ScalingCause::Orphan));
    Ok(())
}

#[tokio::test]
async fn early_exit_clears_a_failed_termination_without_a_termination_audit() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let name = instance_id("orphan-1");
    fleet.set_supply(
        &fleet_name(),
        supply_with("orphan-1", Timestamp::from_unix_millis(0), 1),
    );
    let boot_grace = fleets.policies().first().expect("one fleet").boot_grace;
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0
        }
    );
    clock.advance(boot_grace);
    fleet.fail_terminate(true);
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 1
        }
    );
    fleet.fail_terminate(false);
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            exits: vec![InstanceExit {
                instance: name,
                failure: "worker exited".to_owned(),
            }],
            ..FleetSupply::default()
        },
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0
        }
    );
    assert!(fleet.terminations().is_empty());
    let events = repository.scaling_events();
    assert!(matches!(
        events.as_slice(),
        [ScalingAuditEvent {
            kind: ScalingEventKind::ExitedEarly { .. },
            ..
        }]
    ));
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0
        }
    );
    assert!(fleet.terminations().is_empty());
    assert_eq!(repository.scaling_events().len(), 1);
    Ok(())
}

#[tokio::test]
async fn failed_termination_of_live_worker_is_cancelled_when_demand_returns() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let name = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        supply_with("local-1", Timestamp::from_unix_millis(0), 1),
    );
    scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![idle_worker(&name, clock.wall())?],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0
        }
    );
    clock.advance(Duration::from_secs(16).max(gang_types::health::CLIENT_REROUTE_BOUND));
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0
        }
    );
    scheduler.set_snapshot(SchedulerSnapshot {
        cordons: vec![owned_cordon(&name)],
        workers: vec![WorkerState {
            draining: true,
            ..idle_worker(&name, clock.wall())?
        }],
        ..SchedulerSnapshot::default()
    });
    fleet.fail_terminate(true);
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 1
        }
    );
    fleet.fail_terminate(false);
    scheduler.set_snapshot(SchedulerSnapshot {
        cordons: vec![owned_cordon(&name)],
        workers: vec![WorkerState {
            draining: true,
            ..idle_worker(&name, clock.wall())?
        }],
        ..queued_snapshot()?
    });
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0
        }
    );
    assert!(fleet.terminations().is_empty());
    assert_eq!(
        scheduler.draining_calls(),
        vec![
            (fleet_name(), name.clone(), true),
            (fleet_name(), name.clone(), false),
        ]
    );
    assert!(
        !repository
            .scaling_events()
            .iter()
            .any(|event| { matches!(event.kind, ScalingEventKind::Terminated { .. }) })
    );
    Ok(())
}

fn assert_drain_finished_terminations(repository: &FakeRepository, expected: usize) {
    let events = repository.scaling_events();
    let terminated: Vec<_> = events
        .iter()
        .filter(|event| matches!(event.kind, ScalingEventKind::Terminated { .. }))
        .collect();
    assert_eq!(terminated.len(), expected);
    for event in terminated {
        assert_eq!(event.cause, Some(ScalingCause::DrainFinished));
    }
}

#[tokio::test]
async fn committed_termination_is_retried_after_supply_disappears_or_terminates() -> TestResult {
    for terminating in [false, true] {
        let clock = clock_at(1_000_000);
        let fleets = fleets();
        let fleet = CommittedTerminationProvisioner {
            inner: FakeProvisioner::default(),
            fail_after_commit: AtomicBool::new(true),
            block_converge: AtomicBool::new(false),
        };
        let scheduler = FakeScheduler::new()?;
        let repository = FakeRepository::default();
        let name = instance_id("local-1");
        fleet.inner.set_supply(
            &fleet_name(),
            supply_with("local-1", Timestamp::from_unix_millis(0), 1),
        );
        scheduler.set_snapshot(SchedulerSnapshot {
            workers: vec![idle_worker(&name, clock.wall())?],
            ..SchedulerSnapshot::default()
        });
        let mut control = ControlLoop::new(
            &fleets,
            crate::control::ControlSettings::new(
                POLL_INTERVAL,
                &gang_config::DashboardConfig::default(),
            ),
            Arc::new(gang_config::ScalingConfig::default()),
            source_profiles(),
            &crate::control::ControlPorts {
                provisioner: &fleet,
                scheduler: &scheduler,
                repository: &repository,
                clock: &clock,
            },
            gang_testkit::fence::valid_fence(),
        );

        assert_eq!(
            control.pass().await,
            PassOutcome::Completed {
                applied: 0,
                failed: 0
            }
        );
        clock.advance(Duration::from_secs(16).max(gang_types::health::CLIENT_REROUTE_BOUND));
        assert_eq!(
            control.pass().await,
            PassOutcome::Completed {
                applied: 1,
                failed: 0
            }
        );
        scheduler.set_snapshot(SchedulerSnapshot {
            cordons: vec![owned_cordon(&name)],
            workers: vec![WorkerState {
                draining: true,
                ..idle_worker(&name, clock.wall())?
            }],
            ..SchedulerSnapshot::default()
        });
        assert_eq!(
            control.pass().await,
            PassOutcome::Completed {
                applied: 0,
                failed: 1
            }
        );
        assert_eq!(fleet.inner.terminations(), vec![name.clone()]);
        assert_drain_finished_terminations(&repository, 0);

        assert_eq!(
            control.pass().await,
            PassOutcome::Skipped(SkipReason::ProvisionerUnavailable)
        );
        assert_eq!(fleet.inner.terminations(), vec![name.clone()]);
        assert_drain_finished_terminations(&repository, 0);
        if terminating {
            let mut supply = supply_with("local-1", Timestamp::from_unix_millis(0), 0);
            supply.instances[0].lifecycle = InstanceLifecycle::Terminating;
            fleet.inner.set_supply(&fleet_name(), supply);
        }
        assert_eq!(
            control.pass().await,
            PassOutcome::Completed {
                applied: 1,
                failed: 0
            }
        );
        assert_eq!(fleet.inner.terminations(), vec![name.clone(), name.clone()]);
        assert_drain_finished_terminations(&repository, 1);

        assert_eq!(
            control.pass().await,
            PassOutcome::Completed {
                applied: 0,
                failed: 0
            }
        );
        assert_eq!(fleet.inner.terminations(), vec![name.clone(), name]);
        assert_drain_finished_terminations(&repository, 1);
    }
    Ok(())
}

#[tokio::test]
async fn failed_terminate_is_planned_again_on_the_next_pass() {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let name = instance_id("orphan-1");
    fleet.set_supply(
        &fleet_name(),
        supply_with("orphan-1", Timestamp::from_unix_millis(0), 1),
    );
    let boot_grace = fleets
        .policies()
        .first()
        .expect("the test fleets configure one fleet")
        .boot_grace;
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    clock.advance(boot_grace);

    fleet.fail_terminate(true);
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 1,
        }
    );
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 1,
        }
    );
    assert!(
        !repository
            .scaling_events()
            .iter()
            .any(|event| { matches!(event.kind, ScalingEventKind::Terminated { .. }) })
    );

    fleet.fail_terminate(false);
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert!(fleet.terminations().contains(&name));
}

#[tokio::test]
async fn audit_failure_still_counts_the_step_as_applied() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    scheduler.set_snapshot(queued_snapshot()?);
    repository.fail_scaling_events(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert_eq!(fleet.desired_requests(), vec![(fleet_name(), 1)]);
    assert!(repository.scaling_events().is_empty());
    Ok(())
}

#[tokio::test]
async fn overflow_purchase_depends_on_queued_work_not_other_fleet_execution_duration() -> TestResult
{
    // The first candidate is reachable, so there is no floor purchase on the second fleet.
    // Its 10s unknown backlog is below s*H = 4 * (30s + 15s) / 2 = 90 action-seconds;
    // a known 120s queued action exceeds that threshold, regardless of the first fleet's work.
    for (seconds, queued_duration, applied) in [
        (2, None, 0),
        (60, None, 0),
        (2, Some(Duration::from_mins(2)), 1),
        (60, Some(Duration::from_mins(2)), 1),
    ] {
        let clock = clock_at(1_000_000);
        let fleet = FakeProvisioner::default();
        let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
        let repository = FakeRepository::default();
        let fleets = fleets_of(vec![("first", 1), ("second", 4)]);
        let first = FleetName::try_new("first").expect("first is a valid fleet name");
        let second = FleetName::try_new("second").expect("second is a valid fleet name");
        fleet.set_supply(&first, supply_with("first-1", clock.wall(), 1));
        let mut queued = queued_demand(FleetSet::prefix(2).map_err(PortError::backend)?);
        queued.expected = queued_duration;
        scheduler.set_snapshot(SchedulerSnapshot {
            workers: vec![WorkerState {
                fleet: first.clone(),
                running_actions: 4,
                ..idle_worker(&instance_id("first-1"), clock.wall())?
            }],
            queued: vec![queued],
            executing: (0..4)
                .map(|_| ExecutingDemand {
                    source: None,
                    candidates: FleetSet::prefix(1).expect("one fleet is valid"),
                    resources: gang_types::ActionResources::default(),
                    worker: 0,
                    started: clock.wall(),
                    expected: Some(Duration::from_secs(seconds)),
                })
                .collect(),
            ..SchedulerSnapshot::default()
        });
        let mut control = ControlLoop::new(
            &fleets,
            crate::control::ControlSettings::new(
                POLL_INTERVAL,
                &gang_config::DashboardConfig::default(),
            ),
            Arc::new(gang_config::ScalingConfig::default()),
            source_profiles(),
            &crate::control::ControlPorts {
                provisioner: &fleet,
                scheduler: &scheduler,
                repository: &repository,
                clock: &clock,
            },
            gang_testkit::fence::valid_fence(),
        );

        assert_eq!(
            control.pass().await,
            PassOutcome::Completed { applied, failed: 0 },
            "running action: {seconds}s; queued action duration: {queued_duration:?}"
        );
        let expected_requests = if applied == 1 {
            vec![(second, 1)]
        } else {
            vec![]
        };
        assert_eq!(
            fleet.desired_requests(),
            expected_requests,
            "running action: {seconds}s; queued action duration: {queued_duration:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn each_fleet_supply_is_refreshed_in_declaration_order() {
    let clock = clock_at(1_000_000);
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let fleets = fleets_of(vec![("second", 4), ("first", 4)]);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    let first = FleetName::try_new("first").expect("first is a valid fleet name");
    let second = FleetName::try_new("second").expect("second is a valid fleet name");
    assert_eq!(
        fleet.supply_calls(),
        vec![(second, Isa::X86_64), (first, Isa::X86_64)]
    );
}

/// A snapshot whose only content is `workers`.
fn snapshot_with(workers: Vec<WorkerState>) -> SchedulerSnapshot {
    SchedulerSnapshot {
        workers,
        ..SchedulerSnapshot::default()
    }
}

/// An in-service instance `name` launched at the epoch, matching a worker on that instance.
fn in_service(name: &str) -> FleetInstance {
    FleetInstance {
        id: instance_id(name),
        lifecycle: InstanceLifecycle::InService,
        release_first: false,
        launched_at: Timestamp::from_unix_millis(0),
    }
}

#[tokio::test]
async fn registrations_follow_snapshot_worker_id_order() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let first = instance_id("local-1");
    let second = instance_id("local-2");
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            desired: 2,
            instances: vec![in_service("local-1"), in_service("local-2")],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    );
    // The scheduler port supplies workers in ID order.
    scheduler.set_snapshot(snapshot_with(vec![
        idle_worker(&first, clock.wall())?,
        idle_worker(&second, clock.wall())?,
    ]));
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );

    let recorded = repository.recorded_scaling_events();
    assert_eq!(
        recorded.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![ScalingEventSeq::new(1), ScalingEventSeq::new(2)]
    );
    assert_eq!(recorded[0].event.fleet, fleet_name());
    assert!(matches!(
        &recorded[0].event.kind,
        ScalingEventKind::WorkerRegistered { worker } if *worker == worker_on(&first)?
    ));
    assert!(matches!(
        &recorded[1].event.kind,
        ScalingEventKind::WorkerRegistered { worker } if *worker == worker_on(&second)?
    ));
    Ok(())
}

#[tokio::test]
async fn vanished_worker_is_recorded_as_deregistered_from_the_fleet_it_was_in() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets_of(vec![("first", 4), ("second", 4)]);
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let second = FleetName::try_new("second").expect("second is a valid fleet name");
    let name = instance_id("local-1");
    fleet.set_supply(
        &second,
        FleetSupply {
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            desired: 1,
            instances: vec![in_service("local-1")],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    );
    scheduler.set_snapshot(snapshot_with(vec![WorkerState {
        fleet: second.clone(),
        ..idle_worker(&name, clock.wall())?
    }]));
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    clock.advance(Duration::from_secs(15));
    scheduler.set_snapshot(snapshot_with(Vec::new()));
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );

    let deregistration = repository
        .recorded_scaling_events()
        .into_iter()
        .find(|recorded| {
            matches!(
                recorded.event.kind,
                ScalingEventKind::WorkerDeregistered { .. }
            )
        })
        .expect("the vanished worker is recorded as deregistered");
    assert_eq!(deregistration.seq, ScalingEventSeq::new(2));
    assert_eq!(deregistration.event.fleet, second);
    assert!(matches!(
        &deregistration.event.kind,
        ScalingEventKind::WorkerDeregistered { worker } if *worker == worker_on(&name)?
    ));
    Ok(())
}

#[tokio::test]
async fn unchanged_worker_set_records_nothing_on_the_next_pass() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let first = instance_id("local-1");
    let second = instance_id("local-2");
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            desired: 2,
            instances: vec![in_service("local-1"), in_service("local-2")],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    );
    scheduler.set_snapshot(snapshot_with(vec![
        idle_worker(&first, clock.wall())?,
        idle_worker(&second, clock.wall())?,
    ]));
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    assert_eq!(repository.recorded_scaling_events().len(), 2);
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );

    assert_eq!(repository.recorded_scaling_events().len(), 2);
    Ok(())
}

#[tokio::test]
async fn failed_worker_event_recording_is_not_published() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let hub = hub();
    let instance = instance_id("local-1");
    fleet.set_supply(
        &fleet_name(),
        FleetSupply {
            requested: None,
            events: Vec::new(),
            abandoned: 0,
            desired: 1,
            instances: vec![in_service("local-1")],
            warm: 0,
            exits: Vec::new(),
            launch_failure: None,
        },
    );
    scheduler.set_snapshot(snapshot_with(vec![idle_worker(&instance, clock.wall())?]));
    repository.fail_scaling_events(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 0,
            failed: 0,
        }
    );
    assert!(repository.recorded_scaling_events().is_empty());
    assert!(served(&hub).events.is_empty());
    Ok(())
}

fn initial_state() -> State {
    State {
        name: name::STATE.to_owned(),
        update_time: Timestamp::from_unix_millis(1_000_000),
        provisioner: "local".to_owned(),
        pass: Pass::ok(POLL_INTERVAL.get(), 0, 0),
        events_limit: 100,
        ..State::default()
    }
}

fn hub() -> StateHub {
    StateHub::new(
        HubConfig {
            max_viewers: NonZeroUsize::MIN,
            retry_after: Duration::from_secs(5),
            lag_frames: NonZeroUsize::MIN,
            recent_events: NonZeroUsize::MIN,
        },
        initial_state(),
    )
}

/// A publisher that writes into `hub`, with no actions sample, traffic or cache measurement.
fn publisher(hub: &StateHub) -> StatePublisher {
    publisher_with_sink(hub).0
}

fn publisher_with_sink(hub: &StateHub) -> (StatePublisher, gang_spi::ObservationSink) {
    let config = BuilderConfig {
        events_limit: 100,
        history_window: Duration::from_mins(15),
        history_step: Duration::from_secs(10),
        stale_after: Duration::from_mins(1),
        interval: POLL_INTERVAL.get(),
        provisioner: "local".to_owned(),
    };
    let builds = gang_config::BuildsConfig::default();
    let (builds, sink, _observations) = crate::builds::BuildsCell::channel(&builds);
    let publisher = StatePublisher::new(
        hub.clone(),
        config,
        ActionsCache::default(),
        builds,
        sink.clone(),
        crate::cache_usage::CacheUsage::default(),
        gang_state::CacheBaseline::default(),
    );
    (publisher, sink)
}

/// The state the hub serves now.
fn served(hub: &StateHub) -> State {
    let bytes = hub.state_json().expect("state encodes");
    serde_json::from_slice(&bytes).expect("served state parses")
}

#[tokio::test]
async fn completed_pass_publishes_an_ok_pass_with_the_fleet_rows() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let hub = hub();
    scheduler.set_snapshot(queued_snapshot()?);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );

    let state = served(&hub);
    assert_eq!(state.pass.state, PassState::Ok);
    assert_eq!((state.pass.applied, state.pass.failed), (1, 0));
    assert_eq!(state.fleets.len(), 1);
    assert_eq!(
        state.fleets[0].name,
        name::of::<name::Fleets>(&fleet_name())
    );
    Ok(())
}

#[tokio::test]
async fn skipped_pass_publishes_its_reason_with_no_good_pass_before() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let hub = hub();
    let (publisher, sink) = publisher_with_sink(&hub);
    sink.set_connection_limit(std::num::NonZeroUsize::MIN);
    let _connection = sink.connection_opened();
    sink.connection_refused();
    scheduler.set_snapshot(queued_snapshot()?);
    scheduler.fail_snapshot(true);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher);

    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::SchedulerUnavailable)
    );

    let state = served(&hub);
    assert_eq!(
        state.connections,
        gang_wire::Connections {
            active: 1,
            limit: 1,
            refused: 1
        }
    );
    assert_eq!(state.pass.state, PassState::Skipped);
    assert_eq!(
        state.pass.skip_reason,
        Some(SkipReason::SchedulerUnavailable)
    );
    assert_eq!(state.pass.last_ok_time, None);
    assert_eq!(state.pass.skipped, Some(1));
    Ok(())
}

#[tokio::test]
async fn skipped_pass_reports_the_last_good_pass_and_keeps_the_rows() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let hub = hub();
    scheduler.set_snapshot(queued_snapshot()?);
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));
    let good = clock.wall();
    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );

    clock.advance(Duration::from_secs(5));
    scheduler.fail_snapshot(true);
    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::SchedulerUnavailable)
    );

    let state = served(&hub);
    assert_eq!(state.pass.state, PassState::Skipped);
    assert_eq!(state.pass.skipped, Some(1));
    assert_eq!(state.pass.last_ok_time, Some(good));
    assert_eq!(state.fleets.len(), 1);
    Ok(())
}

#[tokio::test]
async fn worker_joins_recorded_in_a_pass_are_in_that_pass_state() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    fleet.advance_on_supply(clock.clone(), Duration::from_secs(1));
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let hub = hub();
    let instance = instance_id("local-1");
    scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![idle_worker(&instance, clock.wall())?],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));

    control.pass().await;

    let state = served(&hub);
    let expected_worker = worker_on(&instance)?;
    let event = state
        .events
        .iter()
        .find(|event| {
            matches!(
                &event.change,
                EventChange::WorkerRegistered { worker }
                    if *worker == name::of::<name::Workers>(&expected_worker)
            )
        })
        .expect("worker registration is published");
    let recorded = repository.recorded_scaling_events();
    assert_eq!(recorded.len(), 1);
    assert!(matches!(
        &recorded[0].event.kind,
        ScalingEventKind::WorkerRegistered { worker } if *worker == worker_on(&instance)?
    ));
    assert_eq!(state.update_time, clock.wall());
    assert_eq!(state.update_time, event.create_time);
    assert_eq!(event.create_time, recorded[0].event.at);
    Ok(())
}

/// The loop runs a pass at once, then one per poll interval of the injected clock, and none
/// in between.
#[tokio::test]
async fn run_passes_at_once_then_once_per_poll_interval() -> Result<(), PortError> {
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    scheduler.set_snapshot(queued_snapshot()?);
    let snapshots = scheduler.snapshots();
    let control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(
            POLL_INTERVAL,
            &gang_config::DashboardConfig::default(),
        ),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    );
    let mut run = pin!(control.run());
    tokio::select! {
        biased;
        never = &mut run => match never {},
        () = snapshots.reached(1) => {}
    }
    assert_eq!(snapshots.get(), 1, "the first pass runs at once");

    clock.advance(BEFORE_POLL);
    assert!(poll!(run.as_mut()).is_pending());
    assert_eq!(snapshots.get(), 1, "no pass before the poll interval");

    clock.advance(TICK);
    assert!(poll!(run.as_mut()).is_pending());
    assert_eq!(
        snapshots.get(),
        2,
        "the next pass runs one poll interval later"
    );
    Ok(())
}

struct EventProvisioner {
    inner: FakeProvisioner,
    acknowledgements: std::sync::atomic::AtomicUsize,
}

impl ProvisionerPort for EventProvisioner {
    type FleetOptions = Opaque;

    async fn supply(
        &self,
        fleet: Fleet<'_, Opaque>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        self.inner.supply(fleet, into).await
    }

    async fn acknowledge_event(
        &self,
        held: &Held,
        _: Fleet<'_, Opaque>,
        _: &str,
    ) -> Result<(), PortError> {
        held.check()?;
        self.acknowledgements.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        desired: u32,
    ) -> Result<(), PortError> {
        self.inner.set_desired_capacity(held, fleet, desired).await
    }

    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, Opaque>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        self.inner.terminate(held, fleet, instance).await
    }

    fn worker_lifetime(&self) -> gang_types::WorkerLifetime {
        self.inner.worker_lifetime()
    }
}

fn assert_replacement_visible(hub: &StateHub) {
    let state = served(hub);
    assert_eq!(state.status_events.len(), 1);
    assert_eq!(
        state
            .workers
            .iter()
            .filter(|worker| worker.infrastructure.is_some())
            .count(),
        2
    );
}

#[tokio::test]
async fn infrastructure_delivery_retries_and_replays_without_audit_duplicates() -> TestResult {
    use gang_spi::{InfrastructureEvent, InfrastructureEventKind};
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = EventProvisioner {
        inner: FakeProvisioner::default(),
        acknowledgements: std::sync::atomic::AtomicUsize::new(0),
    };
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let old = instance_id("local-1");
    let new = instance_id("local-2");
    let mut supply = supply_with("local-1", clock.wall(), 2);
    supply.requested = Some(1);
    supply.instances.push(in_service("local-2"));
    supply.events.push(InfrastructureEvent {
        id: "rebalance-1".to_owned(),
        at: clock.wall(),
        kind: InfrastructureEventKind::ReplacementLaunched {
            instance: new.clone(),
            replacing: Some(old.clone()),
        },
    });
    fleet.inner.set_supply(&fleet_name(), supply);
    scheduler.set_snapshot(snapshot_with(vec![
        idle_worker(&old, clock.wall())?,
        idle_worker(&new, clock.wall())?,
    ]));
    let initial_hub = hub();
    let dashboard = gang_config::DashboardConfig::default();
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(POLL_INTERVAL, &dashboard),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&initial_hub));
    repository.fail_records(true);
    control.pass().await;
    assert_eq!(fleet.acknowledgements.load(Ordering::SeqCst), 0);
    assert!(repository.scaling_events().is_empty());
    repository.fail_records(false);
    control.pass().await;
    assert_eq!(fleet.acknowledgements.load(Ordering::SeqCst), 1);
    assert_replacement_visible(&initial_hub);
    assert_eq!(repository.scaling_events().len(), 1);
    let mut redelivery = FleetSupply::default();
    fleet
        .inner
        .supply(
            fleets.iter().next().expect("test fleet exists").fleet(),
            &mut redelivery,
        )
        .await?;
    redelivery.events[0].kind = InfrastructureEventKind::InstanceLaunched {
        instance: new.clone(),
    };
    fleet.inner.set_supply(&fleet_name(), redelivery);
    control.pass().await;
    assert_replacement_visible(&initial_hub);
    assert_eq!(repository.scaling_events().len(), 1);
    drop(control);
    let mut after_ack = FleetSupply::default();
    fleet
        .inner
        .supply(
            fleets.iter().next().expect("test fleet exists").fleet(),
            &mut after_ack,
        )
        .await?;
    after_ack.events.clear();
    fleet.inner.set_supply(&fleet_name(), after_ack);
    let restarted = hub();
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(POLL_INTERVAL, &dashboard),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&restarted));
    control.pass().await;
    assert_replacement_visible(&restarted);
    Ok(())
}

#[tokio::test]
async fn capacity_status_preserves_launch_audit_through_redelivery_and_expiry() -> TestResult {
    use gang_spi::{InfrastructureEvent, InfrastructureEventKind};
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = EventProvisioner {
        inner: FakeProvisioner::default(),
        acknowledgements: std::sync::atomic::AtomicUsize::new(0),
    };
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let mut supply = supply_with("local-1", clock.wall(), 3);
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "capacity".into(),
    });
    supply.events.push(InfrastructureEvent {
        id: "capacity-1".into(),
        at: clock.wall(),
        kind: InfrastructureEventKind::CapacityUnavailable {
            reason: "capacity".into(),
        },
    });
    let dashboard = gang_config::DashboardConfig::default();
    let hub = hub();
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(POLL_INTERVAL, &dashboard),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));
    let failures = || {
        repository
            .scaling_events()
            .into_iter()
            .filter(|event| matches!(event.kind, ScalingEventKind::LaunchFailed { .. }))
            .collect::<Vec<_>>()
    };
    let first = ScalingAuditEvent {
        at: clock.wall(),
        fleet: fleet_name(),
        kind: ScalingEventKind::LaunchFailed {
            from: 1,
            to: 3,
            reason: "capacity".into(),
        },
        cause: None,
    };
    fleet.inner.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    assert_eq!(failures(), vec![first.clone()]);
    assert_eq!(served(&hub).status_events.len(), 1);
    assert_eq!(fleet.acknowledgements.load(Ordering::SeqCst), 1);

    clock.advance(Duration::from_secs(1));
    fleet.inner.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    assert_eq!(failures(), vec![first.clone()]);
    assert_eq!(served(&hub).status_events.len(), 1);
    assert_eq!(fleet.acknowledgements.load(Ordering::SeqCst), 2);

    // Expired redelivery must not resurrect operational status or erase durable audit.
    clock.advance(dashboard.history_window.get() + Duration::from_secs(1));
    supply.instances[0].launched_at = clock.wall();
    fleet.inner.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    assert_eq!(failures(), vec![first.clone()]);
    assert!(served(&hub).status_events.is_empty());
    assert_eq!(fleet.acknowledgements.load(Ordering::SeqCst), 3);

    // A new capacity observation must not mask a different concurrent launch failure.
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "different failure".into(),
    });
    supply.events[0].id = "capacity-2".into();
    supply.events[0].at = clock.wall();
    let second = ScalingAuditEvent {
        at: clock.wall(),
        fleet: fleet_name(),
        kind: ScalingEventKind::LaunchFailed {
            from: 1,
            to: 3,
            reason: "different failure".into(),
        },
        cause: None,
    };
    fleet.inner.set_supply(&fleet_name(), supply.clone());
    control.pass().await;
    assert_eq!(failures(), vec![first.clone(), second.clone()]);
    assert_eq!(served(&hub).status_events.len(), 1);
    assert_eq!(fleet.acknowledgements.load(Ordering::SeqCst), 4);

    clock.advance(Duration::from_secs(1));
    fleet.inner.set_supply(&fleet_name(), supply);
    control.pass().await;
    assert_eq!(failures(), vec![first, second]);
    assert_eq!(served(&hub).status_events.len(), 1);
    assert_eq!(fleet.acknowledgements.load(Ordering::SeqCst), 5);
    Ok(())
}

#[tokio::test]
async fn ordinary_provider_launch_does_not_suppress_worker_registration() -> TestResult {
    use gang_spi::{InfrastructureEvent, InfrastructureEventKind};
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let instance = instance_id("local-1");
    let mut supply = supply_with("local-1", clock.wall(), 1);
    supply.events.push(InfrastructureEvent {
        id: "ordinary-launch".into(),
        at: clock.wall(),
        kind: InfrastructureEventKind::InstanceLaunched {
            instance: instance.clone(),
        },
    });
    fleet.set_supply(&fleet_name(), supply);
    scheduler.set_snapshot(snapshot_with(vec![idle_worker(&instance, clock.wall())?]));
    let dashboard = gang_config::DashboardConfig::default();
    let hub = hub();
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(POLL_INTERVAL, &dashboard),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));
    control.pass().await;
    assert!(matches!(
        &repository.scaling_events()[0].kind,
        ScalingEventKind::WorkerRegistered { .. }
    ));
    assert!(served(&hub).workers[0].infrastructure.is_none());
    assert_eq!(served(&hub).status_events.len(), 1);
    Ok(())
}

#[tokio::test]
async fn failed_status_reload_clears_expired_risk_without_stopping_reconciliation() -> TestResult {
    use gang_spi::{InfrastructureEvent, InfrastructureEventKind};
    let clock = clock_at(1_000_000);
    let fleets = fleets();
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new()?;
    let repository = FakeRepository::default();
    let instance = instance_id("local-1");
    let mut supply = supply_with("local-1", clock.wall(), 1);
    supply.events.push(InfrastructureEvent {
        id: "risk".into(),
        at: clock.wall(),
        kind: InfrastructureEventKind::RebalanceRecommendation {
            instance: instance.clone(),
        },
    });
    fleet.set_supply(&fleet_name(), supply.clone());
    let dashboard = gang_config::DashboardConfig::default();
    let hub = hub();
    let mut control = ControlLoop::new(
        &fleets,
        crate::control::ControlSettings::new(POLL_INTERVAL, &dashboard),
        Arc::new(gang_config::ScalingConfig::default()),
        source_profiles(),
        &crate::control::ControlPorts {
            provisioner: &fleet,
            scheduler: &scheduler,
            repository: &repository,
            clock: &clock,
        },
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));
    control.pass().await;
    let before = served(&hub);
    assert_eq!(before.status_events.len(), 1);
    assert!(before.instances[0].infrastructure.is_some());
    let mut viewer = hub.join().expect("viewer joins");

    clock.advance(dashboard.history_window.get() + Duration::from_secs(1));
    supply.events.clear();
    supply.desired = 3;
    supply.instances[0].launched_at = clock.wall();
    supply.launch_failure = Some(gang_spi::LaunchFailure {
        reason: "capacity".into(),
    });
    fleet.set_supply(&fleet_name(), supply);
    repository.fail_records(true);
    assert!(matches!(
        control.pass().await,
        PassOutcome::Completed { .. }
    ));
    assert_eq!(fleet.desired_requests(), vec![(fleet_name(), 1)]);
    let after = served(&hub);
    assert!(after.status_events.is_empty());
    assert!(after.instances[0].infrastructure.is_none());
    let crate::hub::Next::Frame(bytes) = viewer.next().await else {
        panic!("the viewer receives the status removal delta");
    };
    let gang_wire::Frame::Delta(delta) = crate::test_support::decode_sse(&bytes) else {
        panic!("the viewer receives a delta");
    };
    assert_eq!(delta.status_events, Some(Vec::new()));
    Ok(())
}
