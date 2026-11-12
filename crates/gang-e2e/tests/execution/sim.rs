//! A simulated deployment for the execution scenarios: [`ExecSim`] wraps the shared
//! [`Simulation`] for everything the execution scenarios do not observe, and models the held
//! probes, the processes and scopes of one worker, and the flaws of [`Behaviour`].
//!
//! A held probe runs as a step machine driven by the journal: each read of the next journal line
//! applies the next step's effect, so what the scenario observes between two reads is the state
//! after the line it last read.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use gang_e2e::{
    BuildOutcome, COLD_START_MISFIT_MESSAGE, E2eError, Execution, ExecutionKnobs, FleetDisk,
    Generations, Host, Limits, Probe, ProbeReport, Proc, Result, Role, Sandbox, ScopeFacts,
    ServerExit, Signalled, System, Variant,
};
use gang_wire::{
    Event, EventChange, Frame, Instance, Lifecycle, Phase, State as WireState, Worker,
};
use parking_lot::{Condvar, Mutex};

use crate::support::{self, Simulation};

behaviour! {
    /// The flaws an [`ExecSim`] can have; [`Behaviour::COMPLIANT`] has none.
    pub struct Behaviour {
        /// See [`TimeoutChild`]. Whether the timed-out action's background child starts.
        timeout_child: TimeoutChild {
            /// `Started`.
            Started,
            /// `NeverStarts`.
            NeverStarts,
        },
        /// See [`Timeout`]. Whether a timed-out action fails its build.
        timeout: Timeout {
            /// `Fails`.
            Fails,
            /// `Succeeds`.
            Succeeds,
        },
        /// See [`Child`]. Whether the background child of a timed-out action is killed.
        child: Child {
            /// `Killed`.
            Killed,
            /// `Survives`.
            Survives,
        },
        /// See [`Term`]. Whether a timed-out action receives SIGTERM.
        term: Term {
            /// `Delivered`.
            Delivered,
            /// `Withheld`.
            Withheld,
        },
        /// See [`TrapStart`]. Whether the trap probe starts.
        trap_start: TrapStart {
            /// `Starts`.
            Starts,
            /// `NeverStarts`.
            NeverStarts,
        },
        /// See [`ExecWait`]. Whether `gang exec` waits for the action it relayed SIGTERM to.
        exec_wait: ExecWait {
            /// `Waits`.
            Waits,
            /// `ExitsFirst`.
            ExitsFirst,
        },
        /// See [`Grace`]. Whether an action gets `kill_grace` after SIGTERM.
        grace: Grace {
            /// `Honoured`.
            Honoured,
            /// `KilledEarly`.
            KilledEarly,
        },
        /// See [`ScopeCleanup`]. Whether a timed-out action's scope is removed.
        scope_cleanup: ScopeCleanup {
            /// `Removed`.
            Removed,
            /// `Left`.
            Left,
        },
        /// See [`Temp`]. Whether the per-action temporary directory is removed.
        temp: Temp {
            /// `Removed`.
            Removed,
            /// `Left`.
            Left,
        },
        /// See [`Ignorer`]. Whether an action that ignores SIGTERM is killed at the end of the
        /// grace.
        ignorer: Ignorer {
            /// `Killed`.
            Killed,
            /// `Survives`.
            Survives,
        },
        /// See [`SignalStatus`]. How a signalled action's exit status is reported.
        signal_status: SignalStatus {
            /// `ShellConvention`.
            ShellConvention,
            /// `RawSignal`.
            RawSignal,
        },
        /// See [`WorkerEnv`]. Whether the worker's own environment reaches a bwrap action.
        worker_env: WorkerEnv {
            /// `Hidden`.
            Hidden,
            /// `Leaked`.
            Leaked,
        },
        /// See [`Tmp`]. Whether /tmp of a bwrap action is a fresh tmpfs.
        tmp: Tmp {
            /// `Fresh`.
            Fresh,
            /// `Shared`.
            Shared,
        },
        /// See [`Binds`]. Whether the read-only binds are read-only.
        binds: Binds {
            /// `ReadOnly`.
            ReadOnly,
            /// `Writable`.
            Writable,
        },
        /// See [`Workdir`]. Whether the working directory of a bwrap action is writable.
        workdir: Workdir {
            /// `Writable`.
            Writable,
            /// `ReadOnly`.
            ReadOnly,
        },
        /// See [`ScopeReport`]. Whether the held action's scope is published.
        hold_scope: ScopeReport {
            /// `Published`.
            Published,
            /// `Missing`.
            Missing,
        },
        /// See [`BwrapScope`]. Where bwrap runs.
        bwrap_scope: BwrapScope {
            /// `Inside`.
            Inside,
            /// `Outside`.
            Outside,
        },
        /// See [`ExecScope`]. Where `gang exec` runs.
        exec_scope: ExecScope {
            /// `Leaf`.
            Leaf,
            /// `InScope`.
            InScope,
        },
        /// Whether the held bwrap action completes.
        /// Whether a held action completes once released.
        held: Held {
            /// `Completes`.
            Completes,
            /// `Fails`.
            Fails,
        },
        /// See [`OomKill`]. Whether the kernel kills an action past `memory.max`.
        oom_kill: OomKill {
            /// `Kernel`.
            Kernel,
            /// `Spared`.
            Spared,
        },
        /// See [`OomScope`]. Whether the scope of an action killed for memory is removed.
        oom_scope: OomScope {
            /// `Removed`.
            Removed,
            /// `Left`.
            Left,
            /// `Unobserved`.
            Unobserved,
        },
        /// See [`AfterOom`]. Whether the worker serves after an out-of-memory kill.
        after_oom: AfterOom {
            /// `Serves`.
            Serves,
            /// `Fails`.
            Fails,
        },
        /// See [`Stray`]. Whether the stray probe starts its stray.
        stray: Stray {
            /// `Starts`.
            Starts,
            /// `NeverStarts`.
            NeverStarts,
        },
        /// See [`Guard`]. Whether a live guard reaps its killed creator's scope.
        guard: Guard {
            /// `Reaps`.
            Reaps,
            /// `Disabled`.
            Disabled,
        },
        /// See [`ExecKill`]. Whether killing `gang exec` takes its scope and stray with it.
        exec_kill: ExecKill {
            /// `LeavesScope`.
            LeavesScope,
            /// `TakesScope`.
            TakesScope,
        },
        /// See [`Freeze`]. Whether the cgroup freezer freezes the worker's subtree.
        freeze: Freeze {
            /// `Freezes`.
            Freezes,
            /// `Ignored`.
            Ignored,
        },
        /// See [`Retire`]. When Local Mode retires a killed worker's cgroup, stray included.
        retire: Retire {
            /// On a later pass, after the replacement worker's own sweep.
            Deferred,
            /// At once, as the worker dies.
            AtDeath,
        },
        /// Whether the next action of the same worker sweeps a stale scope.
        /// Whether stale scopes are swept.
        action_sweep: Sweep {
            /// `Sweeps`.
            Sweeps,
            /// `Keeps`.
            Keeps,
        },
        /// See [`Escalation`]. How the memory escalation ends.
        escalation: Escalation {
            /// `Bounded`.
            Bounded,
            /// `Unbounded`.
            Unbounded,
            /// `Succeeds`.
            Succeeds,
        },
        /// See [`Unplaceable`]. How an action no fleet accepts is handled.
        unplaceable: Unplaceable {
            /// `Refused`.
            Refused,
            /// `Admitted`.
            Admitted,
            /// `RunsThenFails`.
            RunsThenFails,
            /// `Unnamed`.
            Unnamed,
            /// `Hangs`.
            Hangs,
        },
        /// See [`FleetDiskCheck`]. How an oversized fleet worker behaves.
        fleet_disk: FleetDiskCheck {
            /// `Refuses`.
            Refuses,
            /// `Enrolls`.
            Enrolls,
            /// `Unexplained`.
            Unexplained,
        },
        /// Whether a refusing worker's early-exit event explains the disk refusal.
        fleet_exit: FleetExit {
            /// The event names the disk refusal.
            Explained,
            /// The event is missing.
            Missing,
            /// The event names a different failure.
            WrongFailure,
        },
        /// See [`GuardServing`]. Whether the worker serves after refusing an action for disk.
        guard_serving: GuardServing {
            /// `Serves`.
            Serves,
            /// `Stops`.
            Stops,
        },
        /// See [`ColdStartExit`]. How `gang server` ends on a cold start no fleet accepts.
        cold_start: ColdStartExit {
            /// `Refused`.
            Refused,
            /// `Listens`.
            Listens,
            /// `WrongCode`.
            WrongCode,
            /// `Unnamed`.
            Unnamed,
        },
        /// See [`HeldInstance`]. Whether `gang exec` names the Local Mode instance it runs on.
        held_instance: HeldInstance {
            /// `Local`.
            Local,
            /// `Unnamed`.
            Unnamed,
        },
        /// See [`Preemption`]. Whether a spot interruption stops the worker.
        preemption: Preemption {
            /// `Stops`.
            Stops,
            /// `Ignored`.
            Ignored,
        },
        /// See [`Requeue`]. Whether the action of a preempted worker is requeued.
        requeue: Requeue {
            /// `Requeued`.
            Requeued,
            /// `Lost`.
            Lost,
        },
        /// See [`Renewal`]. How a worker renews its certificate.
        renewal: Renewal {
            /// The worker renews its certificate.
            Renews,
            /// The worker never renews its certificate.
            Never,
            /// The worker retains its previous certificate.
            KeepsPrevious,
            /// The worker restarts when renewing.
            Restarts,
            /// The worker renews only once its certificate has expired.
            AfterExpiry,
        },
    }
    shares {
        /// Whether a dead worker's stale scope and stray are gone before its replacement serves.
        worker_sweep: Sweep = Sweeps,
        /// Whether the action held across a renewal completes.
        renewal_hold: Held = Completes,
    }
}

/// The process id of the first worker; each restart takes the next.
const WORKER: u32 = 100;
/// The process id of every `gang exec`.
const EXEC: u32 = 200;
/// The process id of the guard.
const GUARD: u32 = 250;
/// The process id of the held action's bwrap.
const BWRAP: u32 = 300;
/// The process id of the timed-out action's background child.
const CHILD: u32 = 400;
/// The process id of the stray.
const STRAY_PID: u32 = 500;
/// The worker's leaf cgroup.
const WORKER_LEAF_DIR: &str = "/sim/worker";
/// The directory of the action scopes beside it.
const ACTIONS: &str = "/sim/actions";
/// The worker's delegated subtree, parent of [`WORKER_LEAF_DIR`] and [`ACTIONS`].
const SUBTREE: &str = "/sim";
/// The scope of `gang exec` [`EXEC`].
const EXEC_SCOPE: &str = "/sim/actions/200-1-1";
/// The per-action temporary directory of the noop sandbox.
const TMPDIR: &str = "/sim/scratch/gang-action-1";
/// The Local Mode instance of the worker.
const INSTANCE: &str = "local-1";

/// The simulated certificate's expiration as seconds since the epoch.
const CERTIFICATE_EXPIRY_SECS: u64 = 30;

/// The expiration of the simulated worker's first certificate.
fn certificate_expiry() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(CERTIFICATE_EXPIRY_SECS)
}

/// What one step of a held probe does: report a journal line, or end the build.
enum Step {
    Line(String),
    Done(BuildOutcome),
}

/// The journal's control flags.
#[derive(Default)]
struct JournalFlags {
    /// Whether the journal's end line is pending.
    ended: bool,
    /// Whether the go flag exists.
    go: bool,
}

/// Whether the simulated guard process is still running.
#[derive(Default, PartialEq, Eq)]
enum GuardProcess {
    /// The guard is absent or has exited.
    #[default]
    Gone,
    /// The guard is running.
    Running,
}

/// Which simulated processes live.
#[derive(Default)]
struct Live {
    /// The `gang exec` of the running action.
    exec: bool,
    /// The guard of the running action.
    guard: GuardProcess,
    /// The timed-out action's background child.
    child: bool,
    /// The stray.
    stray: bool,
}

/// What the worker of the held probes can still do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerPhase {
    /// It serves; its `NativeLink` signals the action's groups once `gang exec` is gone.
    Serving,
    /// Its subtree is frozen: it does nothing until it is thawed.
    Frozen,
    /// The harness killed it; a replacement worker serves next.
    Killed,
}

/// The mutable state of an [`ExecSim`].
struct State {
    /// The running held probe and its next step.
    machine: Option<(Probe, u8)>,
    /// The outcome of the held probe, once it ended.
    outcome: Option<BuildOutcome>,
    journal: JournalFlags,
    /// The process id of the current worker.
    worker: u32,
    live: Live,
    /// What the current worker can still do.
    worker_phase: WorkerPhase,
    /// Every existing action scope and the worker that created it.
    scopes: BTreeMap<PathBuf, u32>,
    temp_dirs: Vec<PathBuf>,
    attempts: usize,
    generations: Vec<u64>,
    preempted: bool,
    /// Whether every later build fails.
    failing: bool,
    report: Option<String>,
}

/// A simulated deployment that behaves as its [`Behaviour`] says.
pub struct ExecSim {
    inner: Simulation,
    sandbox: Sandbox,
    execution: ExecutionKnobs,
    behaviour: Behaviour,
    state: Mutex<State>,
    changed: Condvar,
}

/// A build that failed with `status` for `probe`.
fn failed(probe: Probe, status: &str) -> BuildOutcome {
    BuildOutcome::new(Some(1), format!("ERROR: //:{probe} failed: {status}\n"))
}

/// A build of one remote action that succeeded.
fn ok() -> BuildOutcome {
    BuildOutcome::new(Some(0), "INFO: 1 process: 1 remote.\n".to_owned())
}

impl ExecSim {
    /// A deployment of `variant` that behaves as `behaviour` says.
    pub fn new(variant: Variant, behaviour: Behaviour) -> Self {
        let scope_report = if behaviour.hold_scope == ScopeReport::Missing {
            support::ScopeReport::Missing
        } else {
            support::ScopeReport::Published
        };
        Self {
            inner: Simulation::new(
                variant,
                support::Behaviour {
                    scope_report,
                    ..support::Behaviour::COMPLIANT
                },
            ),
            sandbox: variant.sandbox,
            execution: variant.execution,
            behaviour,
            state: Mutex::new(State {
                machine: None,
                outcome: None,
                journal: JournalFlags::default(),
                worker: WORKER,
                live: Live::default(),
                worker_phase: WorkerPhase::Serving,
                scopes: BTreeMap::new(),
                temp_dirs: Vec::new(),
                attempts: 0,
                generations: vec![0],
                preempted: false,
                failing: false,
                report: None,
            }),
            changed: Condvar::new(),
        }
    }

    /// Whether the go flag exists.
    pub fn go(&self) -> bool {
        self.state.lock().journal.go
    }

    /// The outcome `outcome`, or a failure for `probe` once the worker fails every build.
    fn serving(state: &State, probe: Probe, outcome: BuildOutcome) -> BuildOutcome {
        if state.failing {
            failed(probe, "(Exit 1)")
        } else {
            outcome
        }
    }

    /// Removes the stale scopes of the current worker when its next action sweeps them.
    fn sweep_by_action(&self, state: &mut State) {
        if state.live.exec || self.behaviour.action_sweep == Sweep::Keeps {
            return;
        }
        let worker = state.worker;
        let before = state.scopes.len();
        state.scopes.retain(|_, owner| *owner != worker);
        if state.scopes.len() != before {
            state.live.stray = false;
        }
    }

    /// Starts `gang exec` with its scope on the current worker.
    fn start_exec(state: &mut State) {
        state.live.exec = true;
        state.live.guard = GuardProcess::Running;
        state.scopes.insert(PathBuf::from(EXEC_SCOPE), state.worker);
    }

    /// Ends `gang exec`; its scope goes unless the behaviour leaves it.
    fn finish(&self, state: &mut State, outcome: BuildOutcome) -> Step {
        state.live.exec = false;
        state.live.guard = GuardProcess::Gone;
        if self.behaviour.scope_cleanup == ScopeCleanup::Removed {
            state.scopes.remove(Path::new(EXEC_SCOPE));
        }
        Step::Done(outcome)
    }

    /// The end of the trap probe at its timeout.
    fn end_trap(&self, state: &mut State) -> Step {
        if self.behaviour.temp == Temp::Removed {
            state.temp_dirs.clear();
        } else if self.sandbox == Sandbox::Bwrap {
            state.temp_dirs.push(PathBuf::from(TMPDIR));
        }
        self.finish(state, failed(Probe::TermTrap, "Timeout"))
    }

    /// The end of the probe that ignores SIGTERM.
    fn end_ignorer(&self, state: &mut State) -> Step {
        if self.behaviour.ignorer == Ignorer::Survives {
            return Step::Done(failed(Probe::TermIgnorer, "Timeout"));
        }
        self.finish(state, failed(Probe::TermIgnorer, "Timeout"))
    }

    /// Step `step` of the held probe `probe`.
    fn step(&self, state: &mut State, probe: Probe, step: u8) -> Step {
        match probe {
            Probe::TimeoutChild | Probe::TermTrap | Probe::TermIgnorer => {
                self.step_timed(state, probe, step)
            }
            _ => self.step_held(state, probe, step),
        }
    }

    /// Step `step` of a probe of the action timeout scenarios.
    fn step_timed(&self, state: &mut State, probe: Probe, step: u8) -> Step {
        let behaviour = &self.behaviour;
        match (probe, step) {
            (Probe::TimeoutChild, 0) => {
                if behaviour.timeout_child == TimeoutChild::NeverStarts {
                    return Step::Done(failed(probe, "Timeout"));
                }
                Self::start_exec(state);
                state.live.child = true;
                Step::Line("child started".to_owned())
            }
            (Probe::TimeoutChild, _) => {
                state.live.child = behaviour.child == Child::Survives;
                let outcome = if behaviour.timeout == Timeout::Succeeds {
                    ok()
                } else {
                    failed(probe, "Timeout")
                };
                self.finish(state, outcome)
            }
            (Probe::TermTrap, 0) => {
                if behaviour.trap_start == TrapStart::NeverStarts {
                    return Step::Done(failed(probe, "Timeout"));
                }
                Self::start_exec(state);
                match self.sandbox {
                    Sandbox::Noop => {
                        state.temp_dirs.push(PathBuf::from(TMPDIR));
                        Step::Line(format!("trap started {TMPDIR}"))
                    }
                    Sandbox::Bwrap => Step::Line("trap started none".to_owned()),
                }
            }
            (Probe::TermTrap, 1) => {
                if behaviour.term == Term::Withheld {
                    return self.end_trap(state);
                }
                if behaviour.exec_wait == ExecWait::ExitsFirst {
                    state.live.exec = false;
                }
                Step::Line("trap term".to_owned())
            }
            (Probe::TermTrap, 2) if behaviour.grace == Grace::Honoured => {
                Step::Line("trap clean".to_owned())
            }
            (Probe::TermTrap, _) => self.end_trap(state),
            (Probe::TermIgnorer, 0) => {
                Self::start_exec(state);
                Step::Line("ignore started".to_owned())
            }
            (Probe::TermIgnorer, 1) if behaviour.term == Term::Delivered => {
                Step::Line("ignore term".to_owned())
            }
            (Probe::TermIgnorer, 2) if behaviour.grace == Grace::Honoured => {
                Step::Line("ignore survived".to_owned())
            }
            _ => self.end_ignorer(state),
        }
    }

    /// Step `step` of a held probe of the other execution scenarios.
    fn step_held(&self, state: &mut State, probe: Probe, step: u8) -> Step {
        let behaviour = &self.behaviour;
        match (probe, step) {
            (Probe::Stray, 0) => {
                if behaviour.stray == Stray::NeverStarts {
                    return Step::Done(failed(probe, "(Exit 1)"));
                }
                if state.journal.go {
                    return Step::Done(ok());
                }
                Self::start_exec(state);
                state.live.stray = true;
                Step::Line(format!("stray {STRAY_PID}"))
            }
            (Probe::Stray, _) => {
                if state.worker_phase == WorkerPhase::Killed && state.journal.go {
                    if behaviour.worker_sweep == Sweep::Sweeps {
                        let worker = state.worker;
                        state.scopes.retain(|_, owner| *owner == worker);
                        state.live.stray = false;
                    }
                    return Step::Done(ok());
                }
                Step::Done(failed(probe, "(Killed)"))
            }
            (Probe::HeldRelease, 0) => {
                Self::start_exec(state);
                Step::Line("held started".to_owned())
            }
            (Probe::HeldRelease, 1)
                if state.preempted && behaviour.preemption == Preemption::Stops =>
            {
                state.live.exec = false;
                state.scopes.remove(Path::new(EXEC_SCOPE));
                state.worker += 1;
                if behaviour.requeue == Requeue::Lost {
                    return Step::Done(failed(probe, "(Exit 34)"));
                }
                Self::start_exec(state);
                Step::Line("held started".to_owned())
            }
            (Probe::HeldRelease, 1) if behaviour.renewal_hold == Held::Fails => {
                self.finish(state, failed(probe, "(Exit 1)"))
            }
            (Probe::HeldHog, 0) => {
                if behaviour.oom_scope == OomScope::Unobserved {
                    return Step::Done(failed(probe, "(Exit 1)"));
                }
                Self::start_exec(state);
                Step::Line("hog started".to_owned())
            }
            (Probe::HeldHog, _) => {
                state.live.exec = false;
                if behaviour.oom_scope != OomScope::Left {
                    state.scopes.remove(Path::new(EXEC_SCOPE));
                }
                state.failing = behaviour.after_oom == AfterOom::Fails;
                Step::Done(match behaviour.oom_kill {
                    OomKill::Kernel => failed(probe, "(Exit 137)"),
                    OomKill::Spared => ok(),
                })
            }
            _ => self.finish(state, ok()),
        }
    }

    /// Runs the held probe `probe` until its step machine ends, driven by the journal reads. A
    /// worker that had stopped serving before the build fails it; one the build itself stops
    /// fails only the builds after it.
    fn held(&self, probe: Probe) -> BuildOutcome {
        let mut state = self.state.lock();
        let failing = state.failing;
        state.machine = Some((probe, 0));
        state.outcome = None;
        self.changed.notify_all();
        loop {
            if let Some(outcome) = state.outcome.take() {
                return if failing {
                    failed(probe, "(Exit 1)")
                } else {
                    outcome
                };
            }
            self.changed.wait(&mut state);
        }
    }

    /// The report of the sandbox contract probe.
    fn contract_report(&self) -> String {
        let behaviour = &self.behaviour;
        let leaked = if behaviour.worker_env == WorkerEnv::Leaked {
            "GANG_LOCAL_INSTANCE_SECRET,"
        } else {
            ""
        };
        let (entries, filesystem) = match behaviour.tmp {
            Tmp::Fresh => ("", "tmpfs"),
            Tmp::Shared => ("stale,", "ext4"),
        };
        let store = match behaviour.binds {
            Binds::ReadOnly => "erofs",
            Binds::Writable => "ok",
        };
        let writable = match behaviour.workdir {
            Workdir::Writable => "yes",
            Workdir::ReadOnly => "no",
        };
        format!(
            "env_names=PATH,E2E_OUT,GANG_NETWORK,{leaked}\ntmp_entries={entries}\n\
             tmp_fs={filesystem}\nmount:/nix/store={store}\nmount:/proc=other\n\
             cwd_writable={writable}\n"
        )
    }
}

impl System for ExecSim {
    fn start_server(&self) -> Result<()> {
        self.inner.start_server()
    }

    fn kill_server(&self) -> Result<()> {
        self.inner.kill_server()?;
        let _state = self.state.lock();
        self.changed.notify_all();
        Ok(())
    }

    fn server_pid(&self) -> Option<u32> {
        self.inner.server_pid()
    }

    fn worker_count(&self) -> Result<usize> {
        self.inner.worker_count()
    }

    fn scaling_events(&self) -> Result<Vec<Event>> {
        self.inner.scaling_events()
    }

    fn anonymous_admin_status(&self) -> Result<u16> {
        self.inner.anonymous_admin_status()
    }

    fn live_state(&self) -> Result<WireState> {
        self.inner.live_state()
    }

    fn await_stream(&self, done: &mut dyn FnMut(&[Frame]) -> bool) -> Result<Vec<Frame>> {
        if self.execution.fleet_disk != FleetDisk::Oversized {
            return self.inner.await_stream(done);
        }
        // The refusing worker's instance is reaped and replaced; a worker that enrolls instead
        // shows on the replacement's frame.
        let base = self.inner.live_state()?;
        let at = gang_clock::Timestamp::from_unix_millis(0);
        let instance = |number: u32| Instance {
            name: format!("instances/local-{number}"),
            fleet: "fleets/local-worker-x86-64".to_owned(),
            lifecycle: Lifecycle::InService,
            launch_time: at,
            past_boot_grace: false,
        };
        let first = WireState {
            instances: vec![instance(1)],
            workers: Vec::new(),
            ..base.clone()
        };
        let mut second = WireState {
            instances: vec![instance(2)],
            workers: Vec::new(),
            ..base
        };
        if self.behaviour.fleet_disk == FleetDiskCheck::Enrolls {
            second.instances = vec![instance(1)];
            second.workers = vec![Worker {
                name: "workers/local-1".to_owned(),
                fleet: "fleets/local-worker-x86-64".to_owned(),
                instance: "instances/local-1".to_owned(),
                phase: Phase::Idle,
                phase_time: at,
                running: 0,
                slots: None,
                paused: false,
                launch_time: at,
            }];
        }
        let frames = vec![Frame::Snapshot(first), Frame::Snapshot(second)];
        if done(&frames) {
            Ok(frames)
        } else {
            Err(E2eError::timeout(
                "the state stream to show a replaced instance",
                std::time::Duration::from_secs(1),
            ))
        }
    }

    fn build(&self, probes: &[Probe]) -> Result<BuildOutcome> {
        let Some(&probe) = probes.first() else {
            return self.inner.build(probes);
        };
        if self.execution.fleet_disk == FleetDisk::Oversized && probe == Probe::Pinned {
            let mut state = self.state.lock();
            while self.inner.server_pid().is_some() {
                self.changed.wait(&mut state);
            }
            return Ok(failed(probe, "server stopped"));
        }
        self.sweep_by_action(&mut self.state.lock());
        let behaviour = &self.behaviour;
        match probe {
            Probe::TimeoutChild
            | Probe::TermTrap
            | Probe::TermIgnorer
            | Probe::Stray
            | Probe::HeldRelease
            | Probe::HeldHog => Ok(self.held(probe)),
            Probe::SelfKill => Ok(failed(
                probe,
                match behaviour.signal_status {
                    SignalStatus::ShellConvention => "(Exit 137)",
                    SignalStatus::RawSignal => "(Exit 9)",
                },
            )),
            Probe::SelfTerm => Ok(failed(
                probe,
                match behaviour.signal_status {
                    SignalStatus::ShellConvention => "(Exit 143)",
                    SignalStatus::RawSignal => "(Exit 15)",
                },
            )),
            Probe::SandboxContract => {
                let report = self.contract_report();
                let mut state = self.state.lock();
                state.report = Some(report);
                Ok(Self::serving(&state, probe, ok()))
            }
            Probe::MemoryEscalation => {
                let mut state = self.state.lock();
                state.attempts = match behaviour.escalation {
                    Escalation::Bounded | Escalation::Succeeds => 2,
                    Escalation::Unbounded => 3,
                };
                Ok(if behaviour.escalation == Escalation::Succeeds {
                    ok()
                } else {
                    failed(probe, "(Killed)")
                })
            }
            Probe::DiskGuarded => {
                let mut state = self.state.lock();
                state.failing = behaviour.guard_serving == GuardServing::Stops;
                let (attempts, outcome) = match behaviour.unplaceable {
                    Unplaceable::Refused => (
                        0,
                        failed(
                            probe,
                            "no fleet accepts the platform properties of this action (disk_kb=549755813888)",
                        ),
                    ),
                    Unplaceable::Admitted => (1, ok()),
                    Unplaceable::RunsThenFails => (1, failed(probe, "(Exit 1)")),
                    Unplaceable::Unnamed => (0, failed(probe, "(Exit 1)")),
                    Unplaceable::Hangs => {
                        return Err(E2eError::timeout(
                            "bazel to finish",
                            std::time::Duration::from_mins(30),
                        ));
                    }
                };
                state.attempts = attempts;
                Ok(outcome)
            }
            Probe::Hold => {
                let outcome = self.inner.build(probes)?;
                Ok(match behaviour.held {
                    Held::Completes => outcome,
                    Held::Fails => failed(probe, "(Exit 1)"),
                })
            }
            _ => {
                let outcome = self.inner.build(probes)?;
                Ok(Self::serving(&self.state.lock(), probe, outcome))
            }
        }
    }

    fn clean(&self) -> Result<()> {
        self.inner.clean()
    }

    fn report(&self, probe: Probe) -> Result<ProbeReport> {
        if probe != Probe::SandboxContract {
            return self.inner.report(probe);
        }
        self.state
            .lock()
            .report
            .as_deref()
            .ok_or_else(|| E2eError::system("the sandbox contract probe has not been built"))?
            .parse()
    }

    fn host(&self) -> &Host {
        self.inner.host()
    }

    fn await_history(&self, done: &mut dyn FnMut(&[Event]) -> bool) -> Result<Vec<Event>> {
        if self.execution.fleet_disk != FleetDisk::Oversized {
            return self.inner.await_history(done);
        }
        let mut history = vec![Event {
            name: "events/1".to_owned(),
            create_time: gang_clock::Timestamp::from_unix_millis(0),
            fleet: "fleets/local-worker-x86-64".to_owned(),
            cause: None,
            change: EventChange::ScaledOut { from: 0, to: 1 },
        }];
        if self.behaviour.fleet_disk != FleetDiskCheck::Enrolls
            && self.behaviour.fleet_exit != FleetExit::Missing
        {
            history.push(Event {
                name: "events/2".to_owned(),
                create_time: gang_clock::Timestamp::from_unix_millis(1),
                fleet: "fleets/local-worker-x86-64".to_owned(),
                cause: None,
                change: EventChange::ExitedEarly {
                    instance: "instances/local-1".to_owned(),
                    failure: match self.behaviour.fleet_exit {
                        FleetExit::Explained | FleetExit::Missing => {
                            "declares disk_per_instance 1024TiB"
                        }
                        FleetExit::WrongFailure => "another failure",
                    }
                    .to_owned(),
                },
            });
        }
        if done(&history) {
            Ok(history)
        } else {
            Err(E2eError::timeout(
                "the scaling history to satisfy the awaited condition",
                Duration::ZERO,
            ))
        }
    }

    fn tmp_path(&self) -> &Path {
        self.inner.tmp_path()
    }

    fn stderr_tail_lines(&self) -> usize {
        self.inner.stderr_tail_lines()
    }
}

impl Limits for ExecSim {
    fn running_scope(&self) -> Result<Option<ScopeFacts>> {
        self.inner.running_scope()
    }

    fn release(&self, scope: &ScopeFacts) -> Result<()> {
        self.inner.release(scope)
    }

    fn orphan_alive(&self) -> Result<bool> {
        self.inner.orphan_alive()
    }

    fn scope_exists(&self, scope: &ScopeFacts) -> Result<bool> {
        self.inner.scope_exists(scope)
    }

    fn await_hold(&self) -> Result<Signalled> {
        self.inner.await_hold()
    }

    fn abandon_hold(&self) -> Result<()> {
        self.inner.abandon_hold()
    }

    fn attempts(&self) -> Result<usize> {
        Ok(self.state.lock().attempts)
    }
}

impl Execution for ExecSim {
    fn next_line(&self) -> Result<Option<String>> {
        let mut state = self.state.lock();
        loop {
            if let Some((probe, step)) = state.machine {
                match self.step(&mut state, probe, step) {
                    Step::Line(line) => {
                        state.machine = Some((probe, step + 1));
                        return Ok(Some(line));
                    }
                    Step::Done(outcome) => {
                        state.machine = None;
                        state.outcome = Some(outcome);
                        self.changed.notify_all();
                    }
                }
            } else if state.journal.ended {
                state.journal.ended = false;
                return Ok(None);
            } else {
                self.changed.wait(&mut state);
            }
        }
    }

    fn end_lines(&self) -> Result<()> {
        self.state.lock().journal.ended = true;
        self.changed.notify_all();
        Ok(())
    }

    fn set_go(&self, go: bool) -> Result<()> {
        self.state.lock().journal.go = go;
        Ok(())
    }

    fn processes(&self) -> Result<Vec<Proc>> {
        let held = self.inner.running_scope()?;
        let state = self.state.lock();
        let leaf = || Some(PathBuf::from(WORKER_LEAF_DIR));
        let instance = match self.behaviour.held_instance {
            HeldInstance::Local => Some(INSTANCE.to_owned()),
            HeldInstance::Unnamed => None,
        };
        let mut processes = vec![Proc {
            pid: state.worker,
            role: Role::Worker,
            cgroup: leaf(),
            instance: None,
        }];
        let has_held = held.is_some();
        if let Some(facts) = held {
            let in_scope = |inside: bool| {
                if inside {
                    Some(facts.directory.clone())
                } else {
                    leaf()
                }
            };
            processes.push(Proc {
                pid: EXEC,
                role: Role::GangExec,
                cgroup: in_scope(self.behaviour.exec_scope == ExecScope::InScope),
                instance: instance.clone(),
            });
            processes.push(Proc {
                pid: BWRAP,
                role: Role::Bwrap,
                cgroup: in_scope(self.behaviour.bwrap_scope == BwrapScope::Inside),
                instance: None,
            });
        } else if state.live.exec {
            processes.push(Proc {
                pid: EXEC,
                role: Role::GangExec,
                cgroup: leaf(),
                instance,
            });
        }
        if state.live.guard == GuardProcess::Running || has_held {
            processes.push(Proc {
                pid: GUARD,
                role: Role::GangGuard { creator: EXEC },
                cgroup: leaf(),
                instance: None,
            });
        }
        let marked = |pid: u32, marker: &str| Proc {
            pid,
            role: Role::Marked(marker.to_owned()),
            cgroup: Some(PathBuf::from(EXEC_SCOPE)),
            instance: None,
        };
        if state.live.child {
            processes.push(marked(CHILD, "e2e-timeout-child"));
        }
        if state.live.stray {
            processes.push(marked(STRAY_PID, "e2e-stray"));
        }
        Ok(processes)
    }

    fn cgroup_members(&self, cgroup: &Path) -> Result<Vec<u32>> {
        if let Some(facts) = self.inner.running_scope()?
            && facts.directory == cgroup
        {
            let mut members = Vec::new();
            if self.behaviour.bwrap_scope == BwrapScope::Inside {
                members.push(BWRAP);
            }
            if self.behaviour.exec_scope == ExecScope::InScope {
                members.push(EXEC);
            }
            return Ok(members);
        }
        let state = self.state.lock();
        Ok(if cgroup == Path::new(EXEC_SCOPE) && state.live.stray {
            vec![STRAY_PID]
        } else {
            Vec::new()
        })
    }

    fn cgroup_children(&self, cgroup: &Path) -> Result<Vec<PathBuf>> {
        Ok(if cgroup == Path::new(ACTIONS) {
            self.state.lock().scopes.keys().cloned().collect()
        } else {
            Vec::new()
        })
    }

    fn cgroup_exists(&self, cgroup: &Path) -> Result<bool> {
        Ok(self.state.lock().scopes.contains_key(cgroup))
    }

    fn kill(&self, pid: u32) -> Result<()> {
        let mut state = self.state.lock();
        if pid == EXEC {
            state.live.exec = false;
            if state.live.guard == GuardProcess::Running && self.behaviour.guard == Guard::Reaps {
                state.live.guard = GuardProcess::Gone;
                state.live.stray = false;
                state.scopes.remove(Path::new(EXEC_SCOPE));
            } else if state.live.guard == GuardProcess::Gone
                && self.behaviour.exec_kill == ExecKill::TakesScope
            {
                state.live.stray = false;
                state.scopes.remove(Path::new(EXEC_SCOPE));
            } else if state.live.guard == GuardProcess::Gone
                && state.worker_phase == WorkerPhase::Serving
            {
                // A live, unfrozen worker's NativeLink signals the action's groups once
                // `gang exec` is gone.
                state.live.stray = false;
            }
        } else if pid == state.worker {
            state.worker_phase = WorkerPhase::Killed;
            if self.behaviour.retire == Retire::AtDeath {
                // Local Mode retires the dead worker's cgroup with `cgroup.kill`.
                let dead = state.worker;
                state.scopes.retain(|_, owner| *owner != dead);
                state.live.stray = false;
            }
            state.worker += 1;
        } else if pid == GUARD {
            state.live.guard = GuardProcess::Gone;
        } else if pid == STRAY_PID {
            state.live.stray = false;
        } else if pid == CHILD {
            state.live.child = false;
        }
        Ok(())
    }

    fn await_gone(&self, pid: u32, _timeout: Duration) -> Result<bool> {
        Ok(!self.processes()?.iter().any(|process| process.pid == pid))
    }

    fn freeze(&self, cgroup: &Path, frozen: bool, _timeout: Duration) -> Result<bool> {
        if self.behaviour.freeze == Freeze::Ignored || cgroup != Path::new(SUBTREE) {
            return Ok(false);
        }
        let mut state = self.state.lock();
        state.worker_phase = match (frozen, state.worker_phase) {
            (_, WorkerPhase::Killed) => WorkerPhase::Killed,
            (true, _) => WorkerPhase::Frozen,
            (false, _) => WorkerPhase::Serving,
        };
        Ok(true)
    }

    fn worker_only_environment(&self, _worker: u32) -> Result<Vec<String>> {
        Ok(vec![
            "GANG_LOCAL_INSTANCE".to_owned(),
            "GANG_LOCAL_INSTANCE_SECRET".to_owned(),
        ])
    }

    fn readonly_bind_paths(&self) -> Result<Vec<String>> {
        Ok(vec!["/nix/store".to_owned()])
    }

    fn temp_dirs(&self) -> Result<Vec<PathBuf>> {
        Ok(self.state.lock().temp_dirs.clone())
    }

    fn path_exists(&self, path: &Path) -> Result<bool> {
        Ok(self.state.lock().temp_dirs.iter().any(|dir| dir == path))
    }

    fn request_preemption(&self, instance: &str) -> Result<()> {
        if instance != INSTANCE {
            return Err(E2eError::system(format!(
                "Local Mode instance {instance} has no record"
            )));
        }
        self.state.lock().preempted = true;
        Ok(())
    }

    fn identity_generations(&self, _instance: &str) -> Result<Vec<u64>> {
        Ok(self.state.lock().generations.clone())
    }

    fn certificate_not_after(&self, _instance: &str, _generation: u64) -> Result<SystemTime> {
        Ok(certificate_expiry())
    }

    fn generations_at(&self, _instance: &str, at: SystemTime) -> Result<Generations> {
        let mut state = self.state.lock();
        state.generations = match self.behaviour.renewal {
            Renewal::Renews | Renewal::Restarts => vec![1],
            Renewal::AfterExpiry if at >= certificate_expiry() => vec![1],
            Renewal::AfterExpiry | Renewal::Never => vec![0],
            Renewal::KeepsPrevious => vec![0, 1],
        };
        if self.behaviour.renewal == Renewal::Restarts {
            state.worker += 1;
        }
        Ok(Generations {
            held: state.generations.clone(),
            read_at: at,
        })
    }

    fn start_refused_server(&self) -> Result<ServerExit> {
        let refused = format!(
            "  fail  gang server: cold_start reserves 1 cpus, 67108864 KiB of memory and 4194304 \
             KiB of disk, {COLD_START_MISFIT_MESSAGE}\n"
        );
        Ok(match self.behaviour.cold_start {
            ColdStartExit::Refused => ServerExit {
                announced: false,
                code: Some(1),
                log: refused,
            },
            ColdStartExit::Listens => ServerExit {
                announced: true,
                code: None,
                log: "listening: public 127.0.0.1:1\n".to_owned(),
            },
            ColdStartExit::WrongCode => ServerExit {
                announced: false,
                code: Some(2),
                log: refused,
            },
            ColdStartExit::Unnamed => ServerExit {
                announced: false,
                code: Some(1),
                log: "  fail  gang server: invalid configuration\n".to_owned(),
            },
        })
    }

    fn server_log_tail(&self) -> Result<String> {
        Ok("INFO gang_server: the simulated server log\n".to_owned())
    }

    fn worker_logs(&self) -> Result<String> {
        if self.execution.fleet_disk != FleetDisk::Oversized {
            return Ok(String::new());
        }
        Ok(match self.behaviour.fleet_disk {
            FleetDiskCheck::Refuses => "fleet `local-worker-x86-64` declares disk_per_instance 1024TiB, but this worker's scratch volume leaves its actions 50GiB",
            FleetDiskCheck::Enrolls => "worker generation starting",
            FleetDiskCheck::Unexplained => "worker exited",
        }.to_owned())
    }
}
