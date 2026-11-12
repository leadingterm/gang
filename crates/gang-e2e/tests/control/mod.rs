//! The control scenarios against a simulated control loop: a compliant loop passes every check,
//! and each flaw fails exactly the checks it breaks.

use std::path::Path;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_e2e::{
    BuildOutcome, Check, Control, E2eError, Host, Probe, ProbeReport, Result, Sandbox, System,
    Variant, drain_cancel, names, orphan, skipped_pass,
};
use gang_wire::{
    Actions, Cause, Event, EventChange, Frame, Instance, Lifecycle, Pass, PassState, Phase, Queue,
    QueuedAction, SkipReason, State, Worker,
};
use parking_lot::Mutex;

use super::support::{self, Simulation};
use super::{assert_compliant, failures};

/// Passes an idle worker waits before it drains.
const IDLE_PASSES: u32 = 2;

/// Passes an instance runs without a worker before it is an orphan.
const BOOT_GRACE_PASSES: u64 = 3;

/// Passes a wait runs before it times out.
const MAX_PASSES: usize = 60;

/// The simulated fleet.
const FLEET: &str = "fleets/local";

/// The wall clock of pass zero, in Unix milliseconds.
const EPOCH_MILLIS: u64 = 1_700_000_000_000;

behaviour! {
    /// How the simulated control loop behaves; each field but the compliant value breaks one check.
    struct Behaviour {
        /// Why a pass whose supply cannot be read says it was skipped.
        skip_cause: SkipCause {
            /// The provisioner was unavailable.
            Provisioner,
            /// The scheduler was unavailable, which is not what failed.
            Scheduler,
        },
        /// How skipped passes are counted.
        skip_count: SkipCount {
            /// Each skipped pass counts one more; a good pass resets the count.
            Consecutive,
            /// Every skipped pass counts one.
            Repeated,
            /// A good pass does not reset the count.
            Carried,
        },
        /// What `lastOkTime` a skipped pass carries.
        last_ok: LastOk {
            /// The time of the last good pass, absent before one.
            Tracked,
            /// The skipped pass's own time, even before any good pass.
            Invented,
            /// Never any.
            Dropped,
        },
        /// What a good pass says of the skips before it.
        ok_pass: OkPass {
            /// Nothing.
            Clears,
            /// It keeps a skip count.
            KeepsCount,
        },
        /// Whether an in-service instance without a worker past its boot grace is flagged.
        boot_flag: BootFlag {
            /// It is.
            Raised,
            /// It never is.
            Never,
        },
        /// The cause an orphan is terminated with.
        orphan_cause: OrphanCause {
            /// `ORPHAN`.
            Orphan,
            /// `DRAIN_FINISHED`.
            DrainFinished,
        },
        /// How many terminated events one termination records.
        orphan_terminations: Terminations {
            /// One.
            Once,
            /// Two.
            Twice,
        },
        /// What a drain does when an action is queued.
        demand: DrainDemand {
            /// It is cancelled.
            Cancels,
            /// It finishes, and the fleet scales out for the action.
            Ignored,
        },
        /// What a worker whose drain was cancelled does with the queued action.
        resumed: Resumed {
            /// It runs it.
            Serves,
            /// It runs it, and the action fails.
            Fails,
            /// The queued build succeeds without remote execution.
            Local,
            /// The streamed drain is missing from scaling history.
            MissingDrainHistory,
        },
        /// What a restarted server does with a drain that the restart interrupted.
        restart_cancel: RestartCancel {
            /// Nothing.
            Never,
            /// It records the drain as cancelled.
            Cancels,
        },
        /// What the process of a worker whose interrupted drain ended does.
        leak: Leak {
            /// It exits.
            Exits,
            /// It keeps running after its instance is gone.
            Lingers,
        },
    }
    shares {
        restart_terminations: Terminations = Once,
    }
}

/// How a simulated instance's worker stands with the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Link {
    /// Launched, not connected yet.
    Booting,
    /// Connected and idle for this many passes.
    Idle(u32),
    /// Connected and running an action.
    Busy,
    /// Connected and draining.
    Draining,
    /// Stopped with `SIGSTOP`; connected until the next pass notices.
    Stopped,
    /// Stopped and no longer connected, for this many passes.
    Lost(u64),
}

/// One simulated instance and its worker.
#[derive(Debug)]
struct Machine {
    id: u32,
    launched: u64,
    link: Link,
    terminating: bool,
    /// Whether a server restart interrupted its drain.
    interrupted: bool,
}

impl Machine {
    fn worker(&self) -> String {
        format!("workers/local-{}", self.id)
    }

    fn instance(&self) -> String {
        format!("instances/local-{}", self.id)
    }

    /// Whether the worker is connected to the scheduler.
    fn connected(&self) -> bool {
        !self.terminating
            && matches!(
                self.link,
                Link::Idle(_) | Link::Busy | Link::Draining | Link::Stopped
            )
    }
}

/// How the served queued build ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Served {
    /// The build succeeded remotely.
    Remote,
    /// The remote build failed.
    Failed,
    /// The build succeeded locally.
    Local,
}

/// The simulated deployment; its control loop runs one pass at a time.
struct World {
    behaviour: Behaviour,
    min_instances: u32,
    up: bool,
    held: bool,
    pass: u64,
    skipped: u32,
    last_ok: Option<Timestamp>,
    report: Pass,
    ledger: Vec<Event>,
    machines: Vec<Machine>,
    next_id: u32,
    queued: usize,
    served: usize,
    outcome: Served,
    leaked: usize,
}

impl World {
    fn new(behaviour: Behaviour, min_instances: u32) -> Self {
        Self {
            behaviour,
            min_instances,
            up: false,
            held: false,
            pass: 0,
            skipped: 0,
            last_ok: None,
            report: Pass {
                state: PassState::Skipped,
                interval: Duration::from_secs(1),
                applied: 0,
                failed: 0,
                skip_reason: Some(SkipReason::SchedulerUnavailable),
                last_ok_time: None,
                skipped: Some(0),
            },
            ledger: Vec::new(),
            machines: Vec::new(),
            next_id: 1,
            queued: 0,
            served: 0,
            outcome: Served::Remote,
            leaked: 0,
        }
    }

    fn at(pass: u64) -> Timestamp {
        Timestamp::from_unix_millis(EPOCH_MILLIS + pass * 1_000)
    }

    fn record(&mut self, cause: Option<Cause>, change: EventChange) {
        let seq = self.ledger.len() + 1;
        self.ledger.push(Event {
            name: format!("events/{seq}"),
            create_time: Self::at(self.pass),
            fleet: FLEET.to_owned(),
            cause,
            change,
        });
    }

    /// The instances that are not terminating.
    fn live(&self) -> u32 {
        u32::try_from(
            self.machines
                .iter()
                .filter(|machine| !machine.terminating)
                .count(),
        )
        .unwrap_or(u32::MAX)
    }

    fn run_pass(&mut self) {
        self.pass += 1;
        if self.held {
            self.skip();
        } else {
            self.succeed();
        }
    }

    fn skip(&mut self) {
        self.skipped = match self.behaviour.skip_count {
            SkipCount::Repeated => 1,
            SkipCount::Consecutive | SkipCount::Carried => self.skipped + 1,
        };
        let last_ok_time = match self.behaviour.last_ok {
            LastOk::Tracked => self.last_ok,
            LastOk::Invented => Some(Self::at(self.pass)),
            LastOk::Dropped => None,
        };
        let reason = match self.behaviour.skip_cause {
            SkipCause::Provisioner => SkipReason::ProvisionerUnavailable,
            SkipCause::Scheduler => SkipReason::SchedulerUnavailable,
        };
        self.report = Pass {
            state: PassState::Skipped,
            interval: Duration::from_secs(1),
            applied: 0,
            failed: 0,
            skip_reason: Some(reason),
            last_ok_time,
            skipped: Some(self.skipped),
        };
    }

    fn succeed(&mut self) {
        self.last_ok = Some(Self::at(self.pass));
        if self.behaviour.skip_count != SkipCount::Carried {
            self.skipped = 0;
        }
        let skipped = match self.behaviour.ok_pass {
            OkPass::Clears => None,
            OkPass::KeepsCount => Some(self.skipped),
        };
        self.report = Pass {
            state: PassState::Ok,
            interval: Duration::from_secs(1),
            applied: 0,
            failed: 0,
            skip_reason: None,
            last_ok_time: None,
            skipped,
        };
        self.machines.retain(|machine| !machine.terminating);
        for index in 0..self.machines.len() {
            self.step_machine(index);
        }
        self.scale_out();
    }

    fn step_machine(&mut self, index: usize) {
        let Some(machine) = self.machines.get(index) else {
            return;
        };
        let (worker, link, interrupted) = (machine.worker(), machine.link, machine.interrupted);
        let next = match link {
            Link::Booting => {
                self.record(None, EventChange::WorkerRegistered { worker });
                Link::Idle(0)
            }
            Link::Stopped => {
                self.record(None, EventChange::WorkerDeregistered { worker });
                Link::Lost(0)
            }
            Link::Lost(passes) => {
                let passes = passes + 1;
                if passes >= BOOT_GRACE_PASSES {
                    let cause = match self.behaviour.orphan_cause {
                        OrphanCause::Orphan => Cause::Orphan,
                        OrphanCause::DrainFinished => Cause::DrainFinished,
                    };
                    self.terminate(index, &cause, self.behaviour.orphan_terminations);
                }
                Link::Lost(passes)
            }
            Link::Busy => Link::Idle(0),
            Link::Idle(passes) => {
                if self.queued > 0 {
                    self.queued -= 1;
                    self.served += 1;
                    Link::Busy
                } else if passes + 1 >= IDLE_PASSES && self.live() > self.min_instances {
                    self.record(
                        Some(Cause::IdleTimeout),
                        EventChange::DrainStarted { worker },
                    );
                    Link::Draining
                } else {
                    Link::Idle(passes + 1)
                }
            }
            Link::Draining => {
                if self.queued > 0 && self.behaviour.demand == DrainDemand::Cancels {
                    self.record(None, EventChange::DrainCancelled { worker });
                    self.queued -= 1;
                    self.served += 1;
                    self.outcome = match self.behaviour.resumed {
                        Resumed::Fails => Served::Failed,
                        Resumed::Local => Served::Local,
                        Resumed::Serves | Resumed::MissingDrainHistory => Served::Remote,
                    };
                    Link::Busy
                } else {
                    let terminations = if interrupted {
                        self.behaviour.restart_terminations
                    } else {
                        Terminations::Once
                    };
                    self.terminate(index, &Cause::DrainFinished, terminations);
                    Link::Draining
                }
            }
        };
        if let Some(machine) = self.machines.get_mut(index) {
            machine.link = next;
        }
    }

    fn terminate(&mut self, index: usize, cause: &Cause, terminations: Terminations) {
        let Some(machine) = self.machines.get_mut(index) else {
            return;
        };
        machine.terminating = true;
        let instance = machine.instance();
        let interrupted = machine.interrupted;
        let times = match terminations {
            Terminations::Once => 1,
            Terminations::Twice => 2,
        };
        for _ in 0..times {
            self.record(
                Some(cause.clone()),
                EventChange::Terminated {
                    instance: instance.clone(),
                },
            );
        }
        if interrupted && self.behaviour.leak == Leak::Lingers {
            self.leaked += 1;
        }
    }

    fn scale_out(&mut self) {
        let live = self.live();
        let demand = u32::from(self.queued > 0);
        let needed = self.min_instances.max(demand);
        if live >= needed {
            return;
        }
        self.record(
            Some(Cause::Demand),
            EventChange::ScaledOut {
                from: live,
                to: needed,
            },
        );
        for _ in live..needed {
            let id = self.next_id;
            self.next_id += 1;
            self.machines.push(Machine {
                id,
                launched: self.pass,
                link: Link::Booting,
                terminating: false,
                interrupted: false,
            });
        }
    }

    fn state(&self) -> State {
        State {
            name: "state".to_owned(),
            seq: self.pass,
            update_time: Self::at(self.pass),
            provisioner: "local".to_owned(),
            pass: self.report.clone(),
            queue: Queue {
                total: u32::try_from(self.queued).unwrap_or(u32::MAX),
                unplaceable: 0,
                oldest_queue_time: None,
            },
            fleets: Vec::new(),
            workers: self
                .machines
                .iter()
                .filter(|machine| machine.connected())
                .map(|machine| Worker {
                    name: machine.worker(),
                    fleet: FLEET.to_owned(),
                    instance: machine.instance(),
                    phase: match machine.link {
                        Link::Busy => Phase::Busy,
                        Link::Draining => Phase::Draining,
                        Link::Booting | Link::Idle(_) | Link::Stopped | Link::Lost(_) => {
                            Phase::Idle
                        }
                    },
                    ..Worker::default()
                })
                .collect(),
            instances: self
                .machines
                .iter()
                .filter(|machine| !machine.connected())
                .map(|machine| Instance {
                    name: machine.instance(),
                    fleet: FLEET.to_owned(),
                    lifecycle: if machine.terminating {
                        Lifecycle::Terminating
                    } else {
                        Lifecycle::InService
                    },
                    launch_time: Self::at(machine.launched),
                    past_boot_grace: !machine.terminating
                        && self.behaviour.boot_flag == BootFlag::Raised
                        && self.pass - machine.launched >= BOOT_GRACE_PASSES,
                })
                .collect(),
            actions: Some(Actions {
                queued: vec![QueuedAction::default(); self.queued],
                ..Actions::default()
            }),
            events: self.ledger.clone(),
            events_limit: 1_000,
            completed: 0,
            principals_epoch: 0,
        }
    }
}

/// A deployment whose control loop is simulated by [`World`]; what the control scenarios do not
/// observe is delegated to [`Simulation`]. Every frame of its state stream is a snapshot.
struct ControlSim {
    inner: Simulation,
    world: Mutex<World>,
}

impl ControlSim {
    fn new(behaviour: Behaviour, min_instances: u32) -> Self {
        Self {
            inner: Simulation::new(
                Variant::baseline(Sandbox::Bwrap),
                support::Behaviour::COMPLIANT,
            ),
            world: Mutex::new(World::new(behaviour, min_instances)),
        }
    }

    /// The state of the running server.
    fn observe(&self) -> Result<State> {
        let world = self.world.lock();
        if !world.up {
            return Err(E2eError::system("the simulated server is not running"));
        }
        Ok(world.state())
    }

    /// Runs one pass and returns the state it published.
    fn step(&self) -> Result<State> {
        let mut world = self.world.lock();
        if !world.up {
            return Err(E2eError::system("the simulated server is not running"));
        }
        world.run_pass();
        Ok(world.state())
    }

    /// Queues one action and returns how many actions will have been served once it ran.
    fn enqueue(&self) -> usize {
        let mut world = self.world.lock();
        world.queued += 1;
        world.outcome = Served::Remote;
        world.served + world.queued
    }

    /// Runs passes until `target` actions were served; the build fails if they are not within
    /// the wait.
    fn serve(&self, target: usize) -> Result<BuildOutcome> {
        for _ in 0..MAX_PASSES {
            let world = self.world.lock();
            if world.served >= target {
                let code = i32::from(world.outcome == Served::Failed);
                let stderr = if world.outcome == Served::Local {
                    "INFO: 1 process: 1 local.\n"
                } else {
                    "INFO: 1 process: 1 remote.\n"
                };
                return Ok(BuildOutcome::new(Some(code), stderr.to_owned()));
            }
            drop(world);
            self.step()?;
        }
        Ok(BuildOutcome::new(
            Some(1),
            "the simulated action never ran".to_owned(),
        ))
    }
}

/// The error of a simulated wait that ran out of passes.
fn timeout(condition: &str) -> E2eError {
    E2eError::timeout(condition, Duration::ZERO)
}

impl System for ControlSim {
    fn start_server(&self) -> Result<()> {
        let mut world = self.world.lock();
        if world.up {
            return Err(E2eError::system("the simulated server is already running"));
        }
        world.up = true;
        if world.behaviour.restart_cancel == RestartCancel::Cancels {
            let interrupted: Vec<String> = world
                .machines
                .iter()
                .filter(|machine| machine.interrupted)
                .map(Machine::worker)
                .collect();
            for worker in interrupted {
                world.record(None, EventChange::DrainCancelled { worker });
            }
        }
        Ok(())
    }

    fn kill_server(&self) -> Result<()> {
        let mut world = self.world.lock();
        if !world.up {
            return Err(E2eError::system("the simulated server is not running"));
        }
        world.up = false;
        for machine in &mut world.machines {
            if machine.link == Link::Draining {
                machine.link = Link::Booting;
                machine.interrupted = true;
            }
        }
        Ok(())
    }

    fn server_pid(&self) -> Option<u32> {
        self.world.lock().up.then_some(1)
    }

    fn worker_count(&self) -> Result<usize> {
        let world = self.world.lock();
        Ok(world.machines.len() + world.leaked)
    }

    fn scaling_events(&self) -> Result<Vec<Event>> {
        let world = self.world.lock();
        let mut history = world.ledger.clone();
        if world.behaviour.resumed == Resumed::MissingDrainHistory {
            let worker = history.iter().find_map(|event| match &event.change {
                EventChange::DrainStarted { worker } => Some(worker.clone()),
                _ => None,
            });
            history.retain(|event| {
                !matches!(&event.change, EventChange::DrainStarted { worker: named } if Some(named) == worker.as_ref())
            });
        }
        Ok(history)
    }

    fn anonymous_admin_status(&self) -> Result<u16> {
        self.inner.anonymous_admin_status()
    }

    fn live_state(&self) -> Result<State> {
        self.observe()
    }

    fn await_stream(&self, done: &mut dyn FnMut(&[Frame]) -> bool) -> Result<Vec<Frame>> {
        let mut frames = vec![Frame::Snapshot(self.observe()?)];
        for _ in 0..MAX_PASSES {
            if done(&frames) {
                return Ok(frames);
            }
            frames.push(Frame::Snapshot(self.step()?));
        }
        if done(&frames) {
            return Ok(frames);
        }
        Err(timeout(
            "the simulated state stream to satisfy the awaited condition",
        ))
    }

    fn build(&self, _probes: &[Probe]) -> Result<BuildOutcome> {
        self.observe()?;
        let target = self.enqueue();
        self.serve(target)
    }

    fn clean(&self) -> Result<()> {
        self.inner.clean()
    }

    fn report(&self, probe: Probe) -> Result<ProbeReport> {
        self.inner.report(probe)
    }

    fn host(&self) -> &Host {
        self.inner.host()
    }

    fn await_history(&self, done: &mut dyn FnMut(&[Event]) -> bool) -> Result<Vec<Event>> {
        for _ in 0..MAX_PASSES {
            let ledger = self.world.lock().ledger.clone();
            if done(&ledger) {
                return Ok(ledger);
            }
            self.step()?;
        }
        Err(timeout(
            "the simulated scaling history to satisfy the awaited condition",
        ))
    }

    fn tmp_path(&self) -> &Path {
        self.inner.tmp_path()
    }

    fn stderr_tail_lines(&self) -> usize {
        self.inner.stderr_tail_lines()
    }
}

impl Control for ControlSim {
    fn hold_supply(&self) -> Result<()> {
        self.world.lock().held = true;
        Ok(())
    }

    fn release_supply(&self) -> Result<()> {
        self.world.lock().held = false;
        Ok(())
    }

    fn stop_workers(&self) -> Result<usize> {
        let mut world = self.world.lock();
        let mut stopped = 0;
        for machine in world
            .machines
            .iter_mut()
            .filter(|machine| machine.connected())
        {
            machine.link = Link::Stopped;
            stopped += 1;
        }
        Ok(stopped)
    }

    fn continue_workers(&self) -> Result<()> {
        Ok(())
    }

    fn build_during(
        &self,
        _probes: &[Probe],
        during: &mut dyn FnMut() -> Result<()>,
    ) -> Result<BuildOutcome> {
        self.observe()?;
        let target = self.enqueue();
        let observed = during();
        let outcome = self.serve(target)?;
        observed?;
        Ok(outcome)
    }
}

/// The verdicts of the orphan scenario against a loop with one standing instance that behaves
/// as `behaviour` says.
fn orphan_checks(behaviour: Behaviour) -> Result<Vec<Check>> {
    orphan(&ControlSim::new(behaviour, 1))
}

/// The verdicts of the drain-cancel scenario against a loop with no standing instance that
/// behaves as `behaviour` says.
fn drain_cancel_checks(behaviour: Behaviour) -> Result<Vec<Check>> {
    drain_cancel(&ControlSim::new(behaviour, 0))
}

/// The verdicts of the skipped-pass scenario against a loop with no standing instance that
/// behaves as `behaviour` says.
fn skipped_pass_checks(behaviour: Behaviour) -> Result<Vec<Check>> {
    skipped_pass(&ControlSim::new(behaviour, 0))
}

#[test]
fn orphan_is_compliant() {
    assert_compliant(&orphan_checks(Behaviour::COMPLIANT).expect("the orphan scenario runs"));
}

#[test]
fn drain_cancel_is_compliant() {
    assert_compliant(
        &drain_cancel_checks(Behaviour::COMPLIANT).expect("the drain-cancel scenario runs"),
    );
}

#[test]
fn skipped_pass_is_compliant() {
    assert_compliant(
        &skipped_pass_checks(Behaviour::COMPLIANT).expect("the skipped-pass scenario runs"),
    );
}

flaw! {
    unflagged_orphan_fails_the_flag_check:
        orphan_checks(boot_flag = BootFlag::Never)
        => only [names::orphan::FLAGGED_PAST_BOOT_GRACE]
}

flaw! {
    orphan_terminated_as_drained_fails_the_cause_check:
        orphan_checks(orphan_cause = OrphanCause::DrainFinished)
        => only [names::orphan::TERMINATED_AS_ORPHAN]
}

flaw! {
    orphan_terminated_twice_fails_the_once_check:
        orphan_checks(orphan_terminations = Terminations::Twice)
        => only [names::orphan::TERMINATED_ONCE]
}

flaw! {
    ignored_demand_fails_the_cancel_check:
        drain_cancel_checks(demand = DrainDemand::Ignored)
        => only [
            names::drain_cancel::DRAIN_CANCELLED,
            names::drain_cancel::RESUMED_WORKER_SERVES,
        ]
}

flaw! {
    failed_resume_fails_the_resumed_check:
        drain_cancel_checks(resumed = Resumed::Fails)
        => only [names::drain_cancel::RESUMED_WORKER_SERVES]
}

flaw! {
    drain_cancel_resumed_local:
        drain_cancel_checks(resumed = Resumed::Local)
        => only [names::drain_cancel::RESUMED_WORKER_SERVES]
}

flaw! {
    drain_cancel_missing_drain_history:
        drain_cancel_checks(resumed = Resumed::MissingDrainHistory)
        => only [
            names::drain_cancel::DRAIN_CANCELLED,
            names::drain_cancel::RESUMED_WORKER_SERVES,
        ]
}

flaw! {
    cancel_after_restart_fails_the_no_cancel_check:
        drain_cancel_checks(restart_cancel = RestartCancel::Cancels)
        => only [names::drain_cancel::NO_CANCEL_AFTER_RESTART]
}

flaw! {
    double_termination_after_restart_fails_the_once_check:
        drain_cancel_checks(restart_terminations = Terminations::Twice)
        => only [names::drain_cancel::RESTART_TERMINATES_ONCE]
}

flaw! {
    lingering_worker_fails_the_nothing_left_check:
        drain_cancel_checks(leak = Leak::Lingers)
        => only [names::drain_cancel::RESTART_LEAVES_NOTHING]
}

flaw! {
    scheduler_skip_fails_the_provisioner_check:
        skipped_pass_checks(skip_cause = SkipCause::Scheduler)
        => only [names::skipped_pass::SKIPPED_FOR_PROVISIONER]
}

flaw! {
    repeated_count_fails_the_count_check:
        skipped_pass_checks(skip_count = SkipCount::Repeated)
        => only [names::skipped_pass::SKIPS_COUNTED]
}

flaw! {
    carried_count_fails_the_reset_check:
        skipped_pass_checks(skip_count = SkipCount::Carried)
        => only [names::skipped_pass::SKIP_COUNT_RESETS]
}

flaw! {
    invented_last_ok_fails_the_absence_check:
        skipped_pass_checks(last_ok = LastOk::Invented)
        => only [names::skipped_pass::NO_LAST_OK_BEFORE_SUCCESS]
}

flaw! {
    dropped_last_ok_fails_the_kept_check:
        skipped_pass_checks(last_ok = LastOk::Dropped)
        => only [names::skipped_pass::LAST_OK_KEPT]
}

flaw! {
    kept_count_fails_the_clear_check:
        skipped_pass_checks(ok_pass = OkPass::KeepsCount)
        => only [names::skipped_pass::OK_CLEARS_SKIP]
}
