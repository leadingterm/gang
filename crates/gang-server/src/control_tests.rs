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
    Fence, FleetInstance, FleetSupply, Held, InstanceExit, PortError, ProvisionerPort,
    ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, SchedulerSnapshot, WorkerState,
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
    scheduler.set_snapshot(snapshot_with(vec![
        WorkerState {
            draining: true,
            ..idle_worker(&first, clock.wall())?
        },
        WorkerState {
            draining: true,
            ..idle_worker(&second, clock.wall())?
        },
    ]));
    let id = gang_spi::ControllerId::new("test");
    let leader = crate::leadership::Leadership::new(
        &repository,
        &clock,
        &id,
        gang_config::LeadershipConfig::default(),
    );
    let mut lease = leader.acquire().await;
    let hold = leader.hold(&mut lease);
    futures::pin_mut!(hold);
    assert!(poll!(hold.as_mut()).is_pending());
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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

/// A supply with one in-service instance `name` launched at `launched_at`, and desired capacity
/// `desired`.
fn supply_with(name: &str, launched_at: Timestamp, desired: u32) -> FleetSupply {
    FleetSupply {
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
        leader.fence().clone(),
    );
    control.pass().await;
    assert!(repository.scaling_events().is_empty());
    let _lease = leader.acquire().await;
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
            exits: vec![InstanceExit {
                instance: instance.clone(),
                failure: "fail  worker failed".to_owned(),
            }],
            ..FleetSupply::default()
        },
    );
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
        gang_testkit::fence::valid_fence(),
    );

    repository.fail_scaling_events(true);
    control.pass().await;
    assert!(repository.scaling_events().is_empty());
    drop(control);

    repository.fail_scaling_events(false);
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
    let mut lease = leader.acquire().await;
    let hold = leader.hold(&mut lease);
    futures::pin_mut!(hold);
    assert!(poll!(hold.as_mut()).is_pending());
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
            exits: vec![exit.clone()],
            ..FleetSupply::default()
        },
    );
    fleet.fail_acknowledge_exit(true);
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        ScalingEventKind::ScaledOut { from: 0, to: 1 }
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
    scheduler.vanish(worker_on(&name)?);
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        workers: vec![WorkerState {
            draining: true,
            ..idle_worker(&name, clock.wall())?
        }],
        ..queued_snapshot()?
    });
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert_eq!(scheduler.draining_calls(), vec![(worker_on(&name)?, false)]);
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
    assert_eq!(scheduler.draining_calls(), vec![(worker_on(&name)?, true)]);
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
async fn overflow_from_a_full_fleet_scales_the_next_candidate() -> TestResult {
    let clock = clock_at(1_000_000);
    let fleet = FakeProvisioner::default();
    let scheduler = FakeScheduler::new().expect("the scheduler fake builds");
    let repository = FakeRepository::default();
    let fleets = fleets_of(vec![("first", 1), ("second", 4)]);
    let first = FleetName::try_new("first").expect("first is a valid fleet name");
    let second = FleetName::try_new("second").expect("second is a valid fleet name");
    fleet.set_supply(&first, supply_with("first-1", clock.wall(), 1));
    scheduler.set_snapshot(SchedulerSnapshot {
        workers: vec![WorkerState {
            fleet: first.clone(),
            running_actions: 4,
            ..idle_worker(&instance_id("first-1"), clock.wall())?
        }],
        queued: vec![queued_demand(
            FleetSet::prefix(2).map_err(PortError::backend)?,
        )],
        ..SchedulerSnapshot::default()
    });
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
        gang_testkit::fence::valid_fence(),
    );

    assert_eq!(
        control.pass().await,
        PassOutcome::Completed {
            applied: 1,
            failed: 0,
        }
    );
    assert_eq!(fleet.desired_requests(), vec![(second, 1)]);
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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

/// A publisher that writes into `hub`, with no actions sample.
fn publisher(hub: &StateHub) -> StatePublisher {
    let config = BuilderConfig {
        events_limit: 100,
        history_window: Duration::from_mins(15),
        history_step: Duration::from_secs(10),
        stale_after: Duration::from_mins(1),
        interval: POLL_INTERVAL.get(),
        provisioner: "local".to_owned(),
    };
    StatePublisher::new(
        hub.clone(),
        config,
        ActionsCache::default(),
        crate::builds::BuildsCell::new(&gang_config::BuildsConfig::default()),
    )
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
    scheduler.set_snapshot(queued_snapshot()?);
    scheduler.fail_snapshot(true);
    let mut control = ControlLoop::new(
        &fleets,
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
        gang_testkit::fence::valid_fence(),
    )
    .with_publisher(publisher(&hub));

    assert_eq!(
        control.pass().await,
        PassOutcome::Skipped(SkipReason::SchedulerUnavailable)
    );

    let state = served(&hub);
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
        POLL_INTERVAL,
        &fleet,
        &scheduler,
        &repository,
        &clock,
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
