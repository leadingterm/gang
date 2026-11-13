//! A deterministic, in-memory stand-in for a Gang deployment, so that the scenarios run and their
//! verdicts are checked without a binary, Bazel, or a sandbox.
//!
//! [`Simulation`] implements [`System`] and [`SimulatedIdentity`] implements [`Identity`]. Each
//! misbehaviour is a value of one of the enums below, so a test names the flaw it injects.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_e2e::{
    AUDIENCE, Admission, BuildOutcome, Channel, ChannelEvent, Credentials, DiskNeighbourReport,
    E2eError, EventStream, FleetLayout, HOLD_CPU_COUNT, HOLD_MEMORY_KB, Host, Identity, InputLinks,
    IsolationReport, Issuance, LARGE_FLEET, LIMIT_CPU_PERIOD_MICROS, LIMIT_HEADROOM_PERCENT,
    LIMIT_MAX_PIDS, NOOP_FLEET, Namespaces, ORGANIZATION, PermitNetwork, PoisonReport, Presenting,
    Probe, ProbeReport, Reply, Result, Run, SMALL_FLEET, Sandbox, ScopeFacts, SessionCookie,
    Settings, Signalled, System, TokenSpec, Variant, VictimReport,
};
use gang_types::{FleetName, InstanceId, WorkerId};
use gang_wire::routes::{GET_STATE, WATCH_STATE};
use gang_wire::{
    Cause, Closed, ClosedReason, Delta, Demand, Event, EventChange, Fleet, Frame, InstanceCounts,
    InstanceSize, Keyed, Pass, PassState, Phase, Policy, Queue, State, Tick, Worker, WorkerCounts,
    name,
};
use parking_lot::{Condvar, Mutex};

/// The fleet of a deployment that declares one.
const SINGLE_FLEET: &str = "local-worker-x86-64";
/// The memory of one instance of the base fleet, in KiB (`8GiB`).
const BASE_MEMORY_KB: u64 = 8 * 1024 * 1024;
/// The concurrent actions one instance of the base fleet is sized for, and its instance limit.
const BASE_ACTIONS: u32 = 4;
const BASE_INSTANCES: u32 = 4;
/// What follows the instance number in the id of a simulated worker.
const WORKER_SUFFIX: &str = "1f1d946a-7368-6397-b958-0648f8aa0688";
/// How many observations of a busy fleet report its workers before they are drained.
const DRAIN_AFTER: u32 = 3;

/// The namespaces of the harness process.
const HOST_NET: &str = "net:[1]";
const HOST_MNT: &str = "mnt:[2]";
/// The namespaces an isolated action gets.
const OWN_NET: &str = "net:[3]";
const OWN_MNT: &str = "mnt:[4]";
/// The value a probe reports for a namespace it cannot read.
const UNAVAILABLE: &str = "unavailable";
/// The sandbox's tmp path, the working directory under it, the names of that directory, its parent
/// and its grandparent, and the sibling a leak exposes.
const TMP_PATH: &str = "/tmp";
const SIM_CWD: &str = "/tmp/sim-work";
const SIM_CWD_NAME: &str = "sim-work";
const SIM_PARENT_NAME: &str = "tmp";
const SIM_GRANDPARENT_NAME: &str = "root";
const SIBLING: &str = "cas";
/// The per-action `TMPDIR` directory `NativeLink` creates beside the working directory.
const SIM_ACTION_TMP_NAME: &str = "tmp";
/// The principal of the one session the identity simulation issues.
const SIM_PRINCIPAL: &str = "local-workers:developer";
/// The bearer token of that session.
const SIM_TOKEN: &str = "sim-token";
/// The administrator of the simulated token deployment, its bearer token, the name of its session
/// cookie, and what precedes the principal in the value of a session cookie.
const SIM_ADMINISTRATOR: &str = "sim:administrator";
const SIM_ADMIN_TOKEN: &str = "sim-admin-token";
const SIM_COOKIE: &str = gang_wire::CookieNames::for_https(false).session;
const SIM_SESSION_PREFIX: &str = "sim-session-";
/// The cgroup directory and the working directory of the hold action.
const SCOPE_DIRECTORY: &str = "/sim/actions/1";
const SCOPE_WORKING_DIRECTORY: &str = "/sim/work";
/// The base of the percentages the memory headroom is taken in.
const PERCENT: u64 = 100;

behaviour! {
    /// The flaws a [`Simulation`] can have; [`Behaviour::COMPLIANT`] has none.
    pub struct Behaviour {
        /// What the sandbox shows actions.
        /// Actions may see their own namespaces, their parent directory, or neither.
        isolation: Isolation {
            /// Actions get their own net and mnt namespaces, and their parent directory shows only
            /// them.
            Honest,
            /// Actions run in the host's namespaces, and their parent directory lists the CAS
            /// sibling.
            Leaky,
            /// Actions get their own namespaces, but their parent directory lists the CAS sibling.
            LeakyParent,
            /// Actions get their own namespaces and parent directory shows only them, but their
            /// great-grandparent directory lists the CAS sibling beside the grandparent.
            LeakyGreatGrandparent,
            /// The namespace ids cannot be read, so the probes report `unavailable`.
            Unreadable,
        },
        /// Whether the sandbox provides the glibc loader.
        loader: Loader {
            /// The loader is bound into the sandbox.
            Bound,
            /// The loader is missing from the sandbox.
            Missing,
        },
        /// Whether the sandbox shows the action tools at `/bin` and `/usr/bin`.
        fhs: Fhs {
            /// The action tools are bound into the sandbox.
            Bound,
            /// The action tools are missing from the sandbox.
            Missing,
        },
        /// Whether cached inputs stay intact.
        /// This describes whether a modified cached input reaches a later action as `input_links`
        /// says.
        integrity: Integrity {
            /// The later action sees the modification exactly when `input_links = keep`.
            Intact,
            /// The later action sees the modification whatever `input_links` says.
            Poisoned,
            /// The poisoning action is refused write access to its input, and nothing is modified.
            Refused,
        },
        /// Whether rebuilds after clean hit the cache.
        /// The cache is remote.
        cache: Cache {
            /// Actions built before are served from the cache.
            Hit,
            /// Every action runs remotely again.
            Miss,
        },
        /// Whether restarts converge.
        /// This describes whether a restarted server carries the scale-out through.
        restart: Restart {
            /// The restarted server does not scale out again.
            Converges,
            /// The restarted server records a second scale-out of the fleet it already scaled.
            Repeats,
        },
        /// Whether the workers enforce per-action limits.
        limits: Limits {
            /// Every action runs in a scope the kernel limits, and its processes die with it.
            Enforced,
            /// Actions run without limits, and a process they leave behind outlives them.
            Unlimited,
        },
        /// Whether a drain is followed by the termination of its instance.
        /// Termination is recorded in the scaling log.
        termination: Termination {
            /// The drain is followed by the termination of its instance.
            Recorded,
            /// The drain is never followed by a recorded termination.
            Unrecorded,
        },
        /// How actions are placed on fleets.
        routing: Routing {
            /// An action goes to the first fleet, in preference order, that accepts its exec
            /// properties
            /// and has room for it; when none has room, to the last one that accepts it.
            Honoured,
            /// Every action goes to the first fleet, whatever its exec properties say.
            IgnoresProperties,
            /// An action goes to the first fleet that accepts it, however full that fleet is.
            NoOverflow,
            /// The fleets are preferred from the last to the first, and scale out in that order.
            ReversedPreference,
        },
        /// Whether admin routes admit an anonymous caller.
        /// The plaintext deployment binds `none:anonymous` to the administrator role.
        admin_routes: AdminRoutes {
            /// Anonymous callers get `200 OK`, as the server must answer.
            Admitted,
            /// Anonymous callers get `403 Forbidden`, which is a flaw.
            Refused,
        },
        /// Whether a build fails before it scales anything out.
        build: BuildFailure {
            /// Builds run to completion.
            None,
            /// Every build exits 34 with `UNAVAILABLE` on stderr, before any scale-out is recorded.
            BeforeScaleOut,
        },
        /// Whether the hold action's build starts the orphan it holds.
        hold: HoldBuild {
            /// The hold action runs: it publishes its scope and its orphan, then blocks until
            /// released.
            Starts,
            /// The build fails at once, without starting the orphan or signalling that it started.
            FailsEarly,
        },
        /// Whether the hold action signals that its orphan runs.
        hold_signal: HoldSignal {
            /// The orphan signals once it runs.
            Sent,
            /// The orphan runs but never signals.
            Missing,
        },
        /// Whether the disk sampler kills an action that writes past its disk reservation.
        disk: Disk {
            /// The sampler kills the hog, which fails with the worker's message.
            Enforced,
            /// Nothing is killed: the hog writes its whole file and reports.
            Ignored,
        },
        /// Whether the neighbour of a disk hog finishes its writes intact.
        neighbour: Neighbour {
            /// It writes its file, reads it back and reports `neighbour=ok`.
            Finishes,
            /// It fails before it reports.
            Fails,
        },
        /// How the memory grower fares under the soft memory layer.
        grower: Grower {
            /// The soft layer kills the first attempt, the scheduler requeues it, and it completes.
            Requeued,
            /// Nothing kills the grower: it completes on its first attempt.
            NeverKilled,
            /// The soft layer kills it again after the requeue, so it runs three times.
            KilledAgain,
            /// It fails on its requeued attempt.
            Failed,
        },
        /// Whether the worker still serves actions once the limit probes ran.
        serving: Serving {
            /// The worker serves a further action.
            Keeps,
            /// The worker refuses further actions.
            Stops,
        },
        /// Whether the scaling events of the state stream carry their causes.
        /// The cause explains why the autoscaler recorded each event.
        stream_causes: StreamCauses {
            /// Each event carries its cause.
            Recorded,
            /// No event carries a cause.
            Missing,
        },
        /// Whether the completed count follows the actions that ran.
        /// The count belongs to the live state.
        completed: Completed {
            /// An action that ran counts as completed.
            Counted,
            /// The count stays at zero.
            Frozen,
        },
        /// Whether the live state follows the fleet.
        state_view: StateView {
            /// Instances and workers appear when a build scales the fleet out and vanish when it
            /// drains.
            Live,
            /// The state never reports an instance or a worker.
            Blind,
        },
        /// Whether a fresh server already runs an instance before any build.
        warm: Warm {
            /// The fresh server runs no instance until a build scales out.
            Empty,
            /// The fresh server already runs one instance, recorded by no scaling event.
            Preloaded,
        },
        /// Whether a drain deregisters the workers it starts to drain.
        drain: Drain {
            /// Every drained worker is deregistered.
            Completes,
            /// The drain starts, but no worker is ever deregistered.
            Stalls,
        },
        /// Whether a restarted server gets the process id of the server it replaces.
        pid: PidReuse {
            /// A restarted server gets a new process id.
            Fresh,
            /// A restarted server gets the process id the killed server had.
            Reused,
        },
        /// Whether a restarted server converges to one worker.
        convergence: Convergence {
            /// A restarted server leaves the one worker it had.
            Converges,
            /// A restarted server leaves a second instance beside the first.
            Stuck,
        },
        /// Whether the hold action publishes the scope it runs in.
        scope_report: ScopeReport {
            /// The hold action publishes its scope while it runs.
            Published,
            /// The hold action never publishes its scope.
            Missing,
        },
        /// Whether the hold action spawns the orphan process it holds.
        orphan: OrphanProcess {
            /// The hold action spawns its orphan process.
            Spawned,
            /// The hold action never spawns its orphan process.
            NotSpawned,
        },
        /// Whether the identity deployment issues the admitted developer token.
        token_issue: TokenIssue {
            /// The admitted developer token is issued.
            Issues,
            /// The admitted developer token is refused.
            Refuses,
        },
        /// Whether the identity deployment admits a connection that presents a token.
        handshake: Handshake {
            /// A connection that presents a token is admitted.
            Admits,
            /// A connection that presents a token is refused.
            Refuses,
        },
        /// Whether a request carrying a valid token is served.
        requests: Requests {
            /// A request carrying a valid token is served.
            Served,
            /// A request carrying a valid token is answered denied.
            Denied,
        },
        /// Whether a request carrying no token is answered unauthenticated.
        anonymous: Anonymous {
            /// A request carrying no token is answered unauthenticated.
            Unauthenticated,
            /// A request carrying no token is served.
            Served,
        },
    }
}

/// Where the hold action's protocol stands, shared between the hold build and the test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldPhase {
    /// The hold build has not started.
    Idle,
    /// The hold build runs and waits for release; it is set again once that wait is over.
    Started,
    /// The hold action runs but never signalled that its orphan runs.
    Silent,
    /// `release` was called since the hold action started.
    Released,
    /// The server killed the hold build.
    Ended,
}

/// How the server treats the token of a revoked principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Revocation {
    /// Requests with the token are denied from the next request on, new connections included,
    /// and the server keeps running.
    Enforced,
    /// The revoked token keeps working: requests are served and builds succeed.
    Ignored,
    /// Revocation is enforced, but only by restarting the server, which changes its process id.
    RestartedOnRevoke,
    /// Connections are closed by the server before they are used, so none is open to revoke.
    EarlyClose,
    /// Revocation is enforced, but a build with the revoked token fails for a reason that is not
    /// an authentication rejection.
    BuildFailsElsewhere,
    /// Revocation is enforced, but restoring a principal has no effect.
    RestoreIgnored,
}

/// How the token deployment serves the dashboard routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dashboard {
    /// A request without a session cookie is answered `401`. A session cookie opens a
    /// `text/event-stream` that starts with a snapshot and, once the principal is revoked, ends
    /// with `closed` and the reason `revoked`.
    Compliant,
    /// A request without a session cookie is served `200`.
    OpenToAnonymous,
    /// The stream is not closed by a revocation: it stays open and keeps sending ticks.
    NeverCloses,
    /// A revocation closes the stream, but with the reason `expired`.
    ClosesAsExpired,
    /// The stream is served as `application/json`.
    NotAnEventStream,
    /// The stream starts with a tick instead of a snapshot.
    NoSnapshot,
    /// Two heartbeats come between the revocation and the `closed` event, which carries the reason `revoked`.
    TicksBeforeClosed,
    /// The stream ends where the `closed` event would come, without sending it.
    EndsBeforeClosed,
}

/// A fleet of the simulated deployment.
struct SimFleet {
    name: &'static str,
    /// The sandbox its workers run actions under.
    sandbox: Sandbox,
    /// The memory of one instance, in KiB.
    memory_kb: u64,
    /// The most instances the fleet runs.
    max_instances: u32,
    /// The concurrent actions one instance runs.
    actions_per_instance: u32,
}

impl SimFleet {
    fn new(
        name: &'static str,
        sandbox: Sandbox,
        memory_kb: u64,
        max_instances: u32,
        actions_per_instance: u32,
    ) -> Self {
        Self {
            name,
            sandbox,
            memory_kb,
            max_instances,
            actions_per_instance,
        }
    }

    /// Whether the fleet matches every property `request` sets.
    fn accepts(&self, request: &Request) -> bool {
        request.fleet.is_none_or(|name| name == self.name)
            && request.memory_kb.is_none_or(|kb| kb <= self.memory_kb)
            && request
                .sandbox
                .is_none_or(|sandbox| sandbox == self.sandbox)
    }

    /// How many actions the fleet runs at its instance limit.
    fn capacity(&self) -> u32 {
        self.max_instances * self.actions_per_instance
    }
}

/// The fleets a deployment of `variant` declares, in preference order.
fn fleets_of(variant: Variant) -> Vec<SimFleet> {
    match variant.fleets {
        FleetLayout::Single => vec![SimFleet::new(
            SINGLE_FLEET,
            variant.sandbox,
            BASE_MEMORY_KB,
            BASE_INSTANCES,
            BASE_ACTIONS,
        )],
        FleetLayout::Placement => vec![
            SimFleet::new(SMALL_FLEET, variant.sandbox, BASE_MEMORY_KB, 1, 1),
            SimFleet::new(LARGE_FLEET, variant.sandbox, 2 * BASE_MEMORY_KB, 2, 1),
            SimFleet::new(NOOP_FLEET, Sandbox::Noop, BASE_MEMORY_KB, 1, 1),
        ],
    }
}

/// What an action asks of a fleet beyond the ISA, as the sample's target of the probe declares it.
#[derive(Debug, Default)]
struct Request {
    fleet: Option<&'static str>,
    memory_kb: Option<u64>,
    sandbox: Option<Sandbox>,
}

impl Request {
    /// The properties `probe` sets.
    fn of(probe: Probe) -> Self {
        match probe {
            Probe::FleetRequested => Self {
                fleet: Some(LARGE_FLEET),
                ..Self::default()
            },
            Probe::MemoryRequested => Self {
                memory_kb: Some(2 * BASE_MEMORY_KB),
                ..Self::default()
            },
            Probe::SandboxRequested => Self {
                sandbox: Some(Sandbox::Noop),
                ..Self::default()
            },
            _ => Self::default(),
        }
    }
}

/// A deterministic Gang deployment, run in memory under one [`Variant`] and one [`Behaviour`].
pub struct Simulation {
    variant: Variant,
    behaviour: Behaviour,
    /// The fleets in preference order.
    fleets: Vec<SimFleet>,
    state: Mutex<SimState>,
    changed: Condvar,
    host: Host,
    tmp: PathBuf,
    /// Whether a worker boots only once the server has been restarted.
    slow_boot: bool,
    /// Whether the cold build records its scale-out twice.
    early_scale_out: bool,
}

/// The mutable state of a [`Simulation`].
struct SimState {
    /// The running server's process id; `None` while it is down.
    pid: Option<u32>,
    /// The process id the next started server gets.
    next_pid: u32,
    /// How many times the server was started; a restart is a new start even when it reuses the pid.
    starts: u32,
    /// The numbers of the running instances of each fleet, in the fleets' order.
    instances: Vec<Vec<u32>>,
    /// The number the next instance gets.
    next_instance: u32,
    /// Observations of the fleets since their last build.
    idle: u32,
    /// The scaling log, oldest first.
    events: Vec<Event>,
    /// The probes whose outputs the remote cache holds.
    cached: BTreeSet<&'static str>,
    /// The reports of the probes built since the last clean, by probe name.
    reports: BTreeMap<&'static str, String>,
    /// The scope of the hold action while it runs.
    scope: Option<ScopeFacts>,
    /// Whether the orphan process of the hold action is alive.
    orphan: bool,
    /// The phase of the hold action's protocol.
    hold: HoldPhase,
    /// Pending notifications, independent of the lifetime of the hold build.
    hold_notifications: VecDeque<Signalled>,
    /// The number of builds in flight; the recorded history is final once none is.
    building: u32,
    /// The number of builds begun so far, so an await knows whether one is yet to come.
    builds_started: u32,
    /// How many attempts the memory grower made in the last build of it.
    attempts: usize,
    /// The actions that ran remotely, which the live state counts as completed.
    completed: u64,
}

impl Simulation {
    /// A deployment that is not yet started, under `variant`, behaving as `behaviour` says.
    #[must_use]
    pub fn new(variant: Variant, behaviour: Behaviour) -> Self {
        let fleets = fleets_of(variant);
        let instances = fleets.iter().map(|_| Vec::new()).collect();
        Self {
            variant,
            behaviour,
            fleets,
            state: Mutex::new(SimState {
                pid: None,
                starts: 0,
                next_pid: 1,
                instances,
                next_instance: 1,
                idle: 0,
                events: Vec::new(),
                cached: BTreeSet::new(),
                reports: BTreeMap::new(),
                scope: None,
                orphan: false,
                hold: HoldPhase::Idle,
                hold_notifications: VecDeque::new(),
                building: 0,
                builds_started: 0,
                attempts: 0,
                completed: 0,
            }),
            changed: Condvar::new(),
            host: Host {
                net: Some(HOST_NET.to_owned()),
                mnt: Some(HOST_MNT.to_owned()),
            },
            tmp: PathBuf::from(TMP_PATH),
            slow_boot: false,
            early_scale_out: false,
        }
    }

    /// Makes a worker boot only once the server has been restarted, so that a build which scales
    /// out is still running when the server is killed.
    #[must_use]
    pub fn with_slow_boot(mut self) -> Self {
        self.slow_boot = true;
        self
    }

    /// Makes the cold build record its scale-out twice, as a server that scales out again before
    /// the fleet drains does.
    #[must_use]
    pub fn with_early_scale_out(mut self) -> Self {
        self.early_scale_out = true;
        self
    }

    /// Whether actions get their own net and mnt namespaces under `sandbox`.
    fn isolates(&self, sandbox: Sandbox) -> bool {
        sandbox == Sandbox::Bwrap
            && matches!(
                self.behaviour.isolation,
                Isolation::Honest | Isolation::LeakyParent | Isolation::LeakyGreatGrandparent
            )
    }

    /// Whether the action's parent directory lists the CAS sibling beside the action's directory.
    fn parent_leaks(&self) -> bool {
        if self.variant.sandbox == Sandbox::Bwrap {
            matches!(
                self.behaviour.isolation,
                Isolation::Leaky | Isolation::LeakyParent
            )
        } else {
            true
        }
    }

    /// Whether the great-grandparent directory lists the CAS sibling beside the grandparent.
    fn great_grandparent_leaks(&self) -> bool {
        if self.variant.sandbox == Sandbox::Bwrap {
            matches!(
                self.behaviour.isolation,
                Isolation::Leaky | Isolation::LeakyGreatGrandparent
            )
        } else {
            true
        }
    }

    /// `value`, or `unavailable` when the namespace ids cannot be read.
    fn readable(&self, value: &str) -> String {
        if self.behaviour.isolation == Isolation::Unreadable {
            UNAVAILABLE.to_owned()
        } else {
            value.to_owned()
        }
    }

    /// The namespaces a probe reports when it runs under `sandbox`.
    fn namespaces(&self, sandbox: Sandbox) -> Namespaces {
        let (net, mnt) = if self.isolates(sandbox) {
            (OWN_NET, OWN_MNT)
        } else {
            (HOST_NET, HOST_MNT)
        };
        Namespaces {
            net: self.readable(net),
            mnt: self.readable(mnt),
        }
    }

    /// The entries of the action's parent directory, as `ls -A ..` lists them.
    fn parent_entries(&self) -> Vec<String> {
        if self.parent_leaks() {
            vec![
                SIBLING.to_owned(),
                SIM_ACTION_TMP_NAME.to_owned(),
                SIM_CWD_NAME.to_owned(),
            ]
        } else {
            vec![SIM_ACTION_TMP_NAME.to_owned(), SIM_CWD_NAME.to_owned()]
        }
    }

    /// The entries of the action's great-grandparent directory, as `ls -A ../../..` lists them.
    fn great_grandparent_entries(&self) -> Vec<String> {
        if self.great_grandparent_leaks() {
            vec![SIBLING.to_owned(), SIM_GRANDPARENT_NAME.to_owned()]
        } else {
            vec![SIM_GRANDPARENT_NAME.to_owned()]
        }
    }

    /// Whether a later action sees a modification of a cached input.
    fn victim_sees_modification(&self) -> bool {
        match self.behaviour.integrity {
            Integrity::Intact | Integrity::Refused => {
                matches!(self.variant.input_links, InputLinks::Keep)
            }
            Integrity::Poisoned => true,
        }
    }

    /// The report `probe` writes when it runs under `sandbox`, or `None` when it writes none.
    fn report_for(&self, probe: Probe, sandbox: Sandbox) -> Option<String> {
        let namespaces = self.namespaces(sandbox);
        match probe {
            Probe::Isolated => Some(
                IsolationReport {
                    namespaces,
                    cwd: SIM_CWD.to_owned(),
                    cwd_name: SIM_CWD_NAME.to_owned(),
                    parent_name: SIM_PARENT_NAME.to_owned(),
                    parent_entries: self.parent_entries(),
                    grandparent_name: SIM_GRANDPARENT_NAME.to_owned(),
                    grandparent_entries: vec![SIM_PARENT_NAME.to_owned()],
                    great_grandparent_entries: self.great_grandparent_entries(),
                    cwd_writable: true,
                }
                .to_string(),
            ),
            Probe::NetworkRequested => {
                let requested = if self.variant.permit_network == PermitNetwork::Permitted {
                    HOST_NET
                } else {
                    OWN_NET
                };
                Some(
                    Namespaces {
                        net: self.readable(requested),
                        ..namespaces
                    }
                    .to_string(),
                )
            }
            Probe::Pinned => {
                (self.behaviour.serving == Serving::Keeps).then(|| "ran=yes\n".to_owned())
            }
            Probe::GlibcTool => Some(
                match self.behaviour.loader {
                    Loader::Bound => "glibc_tool=ran\n",
                    Loader::Missing => {
                        "glibc_tool=failed: bwrap: execvp ./glibc-tool: No such file or directory\n"
                    }
                }
                .to_owned(),
            ),
            Probe::FhsTools => Some(
                match self.behaviour.fhs {
                    Fhs::Bound => "fhs_tools=ran\n",
                    Fhs::Missing => {
                        "fhs_tools=failed: ./fhs-tools.sh: /usr/bin/env: bad interpreter: No such file or directory\n"
                    }
                }
                .to_owned(),
            ),
            Probe::Unconstrained
            | Probe::FleetRequested
            | Probe::MemoryRequested
            | Probe::SandboxRequested => Some(namespaces.to_string()),
            Probe::OverflowFirst => Some("ran=first\n".to_owned()),
            Probe::OverflowSecond => Some("ran=second\n".to_owned()),
            Probe::MemoryEscalation
            | Probe::DiskGuarded
            | Probe::TimeoutChild
            | Probe::TermTrap
            | Probe::TermIgnorer
            | Probe::SelfKill
            | Probe::SelfTerm
            | Probe::SandboxContract
            | Probe::InputRoot
            | Probe::Stray
            | Probe::HeldRelease
            | Probe::HeldHog
            | Probe::ExitThree
            | Probe::Hold
            | Probe::MemoryHog
            | Probe::MemoryGrower => None,
            Probe::DiskHog => {
                (self.behaviour.disk == Disk::Ignored).then(|| "hogged=yes\n".to_owned())
            }
            Probe::DiskNeighbour => (self.behaviour.neighbour == Neighbour::Finishes)
                .then(|| DiskNeighbourReport { intact: true }.to_string()),
            Probe::Poison => Some(
                PoisonReport {
                    modified: self.behaviour.integrity != Integrity::Refused,
                }
                .to_string(),
            ),
            Probe::Victim => Some(
                VictimReport {
                    poisoned: self.victim_sees_modification(),
                }
                .to_string(),
            ),
        }
    }

    /// Records a scaling change of `fleet` at the next instant of the log.
    fn record(state: &mut SimState, fleet: &str, change: EventChange) -> Result<()> {
        let at = Timestamp::from_unix_millis(u64::try_from(state.events.len()).unwrap_or(u64::MAX));
        let fleet = fleet_name(fleet)?;
        let seq = u64::try_from(state.events.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let cause = match &change {
            EventChange::ScaledOut { .. } => Some(Cause::Demand),
            EventChange::DrainStarted { .. } => Some(Cause::IdleTimeout),
            EventChange::Terminated { .. } => Some(Cause::DrainFinished),
            _ => None,
        };
        state.events.push(Event {
            name: name::of::<name::Events>(&seq),
            create_time: at,
            fleet,
            cause,
            change,
        });
        Ok(())
    }

    /// The index of the fleet that runs each of `actions`, or `None` when an action matches no
    /// fleet.
    fn place(&self, actions: &[Probe]) -> Option<Vec<usize>> {
        let routing = self.behaviour.routing;
        let mut load = vec![0_u32; self.fleets.len()];
        let mut placed = Vec::with_capacity(actions.len());
        for &probe in actions {
            let request = Request::of(probe);
            let mut candidates: Vec<usize> = if routing == Routing::IgnoresProperties {
                vec![0]
            } else {
                self.fleets
                    .iter()
                    .enumerate()
                    .filter(|(_, fleet)| fleet.accepts(&request))
                    .map(|(index, _)| index)
                    .collect()
            };
            if routing == Routing::ReversedPreference {
                candidates.reverse();
            }
            let has_room = |index: usize| {
                self.fleets
                    .get(index)
                    .zip(load.get(index))
                    .is_some_and(|(fleet, assigned)| *assigned < fleet.capacity())
            };
            let chosen = if routing == Routing::NoOverflow {
                candidates.first()
            } else {
                candidates
                    .iter()
                    .find(|&&index| has_room(index))
                    .or_else(|| candidates.last())
            };
            let chosen = *chosen?;
            if let Some(assigned) = load.get_mut(chosen) {
                *assigned += 1;
            }
            placed.push(chosen);
        }
        Some(placed)
    }

    /// Scales out every fleet that has actions placed on it and no instance running, recording
    /// the scale-outs in the order the fleets are preferred. Returns whether any fleet scaled out.
    fn scale_out(&self, state: &mut SimState, placed: &[usize]) -> Result<bool> {
        let mut order: Vec<usize> = (0..self.fleets.len()).collect();
        if self.behaviour.routing == Routing::ReversedPreference {
            order.reverse();
        }
        let repeats = if self.early_scale_out { 2 } else { 1 };
        let mut scaled = false;
        for index in order {
            let Some(fleet) = self.fleets.get(index) else {
                continue;
            };
            let assigned = placed.iter().filter(|&&chosen| chosen == index).count();
            let assigned = u32::try_from(assigned).unwrap_or(u32::MAX);
            if assigned == 0 || !state.instances.get(index).is_some_and(Vec::is_empty) {
                continue;
            }
            let needed = assigned
                .div_ceil(fleet.actions_per_instance)
                .min(fleet.max_instances);
            let numbers: Vec<u32> = (0..needed)
                .map(|_| {
                    let number = state.next_instance;
                    state.next_instance += 1;
                    number
                })
                .collect();
            if let Some(running) = state.instances.get_mut(index) {
                running.extend(numbers.iter().copied());
            }
            for _ in 0..repeats {
                let change = EventChange::ScaledOut {
                    from: 0,
                    to: needed,
                };
                Self::record(state, fleet.name, change)?;
            }
            for &number in &numbers {
                let change = EventChange::WorkerRegistered {
                    worker: worker_name(number)?,
                };
                Self::record(state, fleet.name, change)?;
            }
            scaled = true;
        }
        Ok(scaled)
    }

    /// The stderr lines of the probes of `probes` that fail under this behaviour; empty when none
    /// fails.
    fn failure_lines(&self, probes: &[Probe]) -> String {
        let mut lines = String::new();
        if probes.contains(&Probe::ExitThree) {
            lines.push_str("ERROR: //:exit_three failed: (Exit 3)\n");
        }
        if probes.contains(&Probe::DiskHog) && self.behaviour.disk == Disk::Enforced {
            lines.push_str(concat!(
                "ERROR: //:disk_hog failed: (Exit 1)\n",
                "ERROR: Action exceeded its disk reservation: reserved 4096 KiB (disk_kb), ",
                "limit 4915 KiB with 20% headroom, its own files came to 65536 KiB. ",
                "Raise the reservation or write less.\n",
            ));
        }
        if probes.contains(&Probe::DiskNeighbour) && self.behaviour.neighbour == Neighbour::Fails {
            lines.push_str("ERROR: //:disk_neighbour failed: (Exit 1)\n");
        }
        if probes.contains(&Probe::Pinned) && self.behaviour.serving == Serving::Stops {
            lines.push_str("ERROR: //:pinned failed: (Exit 1)\n");
        }
        lines
    }

    /// The probes a build runs: [`Probe::Poison`] runs before [`Probe::Victim`], which needs it.
    fn targets(probes: &[Probe]) -> Vec<Probe> {
        let mut targets = Vec::new();
        if probes.contains(&Probe::Victim) {
            targets.push(Probe::Poison);
        }
        for &probe in probes {
            if !targets.contains(&probe) {
                targets.push(probe);
            }
        }
        targets
    }

    /// The Bazel process summary of a build that ran `remote` actions remotely and `hits` from
    /// the cache.
    fn summary(remote: usize, hits: usize) -> String {
        let total = remote + hits;
        if total == 0 {
            return "INFO: 0 processes.\n".to_owned();
        }
        let mut parts = Vec::new();
        if remote > 0 {
            parts.push(format!("{remote} remote"));
        }
        if hits > 0 {
            parts.push(format!("{hits} remote cache hit"));
        }
        format!("INFO: {total} processes: {}.\n", parts.join(", "))
    }
}

/// The resource name of the worker of instance `number` of a simulated fleet.
fn worker_name(number: u32) -> Result<String> {
    let id = WorkerId::try_new(format!("local-{number}-{WORKER_SUFFIX}"))
        .map_err(E2eError::system_from(format!("worker id {number}")))?;
    Ok(name::of::<name::Workers>(&id))
}

/// The resource name of instance `number` of a simulated fleet.
fn instance_name(number: u32) -> Result<String> {
    let id = InstanceId::try_new(format!("local-{number}"))
        .map_err(E2eError::system_from(format!("instance id {number}")))?;
    Ok(name::of::<name::Instances>(&id))
}

/// The configured `recent_events` of the simulated server.
const EVENTS_LIMIT: u32 = 100;

/// A count as the unsigned number the wire uses.
fn sequence(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// The instant that frame number `millis` carries.
fn at(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

/// The resource name of a simulated fleet.
fn fleet_name(id: &str) -> Result<String> {
    let id = FleetName::try_new(id).map_err(E2eError::system_from(format!("fleet name {id}")))?;
    Ok(name::of::<name::Fleets>(&id))
}

/// A live state numbered `seq` that shows `fleets` and `workers`, carries `events` and counts
/// `completed` actions.
fn state_frame(
    seq: u64,
    fleets: Vec<Fleet>,
    workers: Vec<Worker>,
    events: Vec<Event>,
    completed: u64,
) -> State {
    State {
        name: name::STATE.to_owned(),
        seq,
        update_time: at(seq),
        provisioner: "local".to_owned(),
        pass: Pass {
            state: PassState::Ok,
            interval: Duration::from_millis(500),
            applied: 0,
            failed: 0,
            skip_reason: None,
            last_ok_time: None,
            skipped: None,
        },
        queue: Queue {
            total: 0,
            unplaceable: 0,
            oldest_queue_time: None,
        },
        fleets,
        workers,
        instances: Vec::new(),
        actions: None,
        events,
        events_limit: EVENTS_LIMIT,
        completed,
        principals_epoch: 0,
    }
}

impl Simulation {
    /// Drains every running instance, as the server does once the fleets have been idle: each
    /// worker starts to drain, and is deregistered unless the drain stalls. Its instance is
    /// recorded as terminated unless the behaviour leaves the termination out.
    fn drain_all(&self, state: &mut SimState) -> Result<()> {
        let drained: Vec<(&'static str, u32)> = self
            .fleets
            .iter()
            .zip(&state.instances)
            .flat_map(|(fleet, numbers)| numbers.iter().map(move |number| (fleet.name, *number)))
            .collect();
        state.instances.iter_mut().for_each(Vec::clear);
        for (fleet, number) in drained {
            let worker = worker_name(number)?;
            let instance = instance_name(number)?;
            Self::record(
                state,
                fleet,
                EventChange::DrainStarted {
                    worker: worker.clone(),
                },
            )?;
            if self.behaviour.drain == Drain::Completes {
                Self::record(state, fleet, EventChange::WorkerDeregistered { worker })?;
            }
            if self.behaviour.termination == Termination::Recorded {
                Self::record(state, fleet, EventChange::Terminated { instance })?;
            }
        }
        Ok(())
    }

    /// Adds an instance to the first fleet without recording a scaling event, as an instance a
    /// running server already has.
    fn add_running(state: &mut SimState) {
        let number = state.next_instance;
        state.next_instance += 1;
        if let Some(running) = state.instances.first_mut() {
            running.push(number);
        }
    }

    /// The fleet rows and the worker rows, ordered by id, that the live state shows for the
    /// running `instances`, with every worker in `phase`. A blind state shows none.
    fn rows(&self, instances: &[Vec<u32>], phase: &Phase) -> Result<(Vec<Fleet>, Vec<Worker>)> {
        let mut fleets = Vec::new();
        let mut workers = Vec::new();
        for (fleet, numbers) in self.fleets.iter().zip(instances) {
            let numbers: &[u32] = if self.behaviour.state_view == StateView::Blind {
                &[]
            } else {
                numbers
            };
            fleets.push(Self::fleet_row(fleet, numbers.len(), phase)?);
            for number in numbers {
                workers.push(Self::worker_row(fleet, *number, phase.clone())?);
            }
        }
        workers.sort_by(|left, right| left.name.cmp(&right.name));
        Ok((fleets, workers))
    }

    /// The row of `fleet` while `count` instances run, all of their workers in `phase`.
    fn fleet_row(fleet: &SimFleet, count: usize, phase: &Phase) -> Result<Fleet> {
        let count = u32::try_from(count).unwrap_or(u32::MAX);
        let (idle, draining) = if *phase == Phase::Draining {
            (0, count)
        } else {
            (count, 0)
        };
        Ok(Fleet {
            name: fleet_name(fleet.name)?,
            isa: gang_wire::Isa::X86_64,
            sandbox: fleet.sandbox.key().to_owned(),
            properties: BTreeMap::new(),
            instance_size: Some(InstanceSize {
                vcpus: fleet.actions_per_instance,
                memory: fleet.memory_kb * 1024,
            }),
            policy: Policy {
                min: 0,
                max: fleet.max_instances,
                target_actions: fleet.actions_per_instance,
                idle_timeout: Duration::from_secs(15),
                max_drain_wait: Duration::from_mins(1),
                boot_grace: Duration::from_secs(30),
            },
            desired: count,
            instances: InstanceCounts {
                pending: 0,
                in_service: count,
                terminating: 0,
                warm: 0,
            },
            workers: WorkerCounts {
                busy: 0,
                idle,
                draining,
                unconnected: 0,
            },
            demand: Demand {
                running: 0,
                queued: 0,
                candidates: 0,
                needed: count,
                vcpus: 0,
                memory: 0,
            },
        })
    }

    /// The row of the worker of instance `number` of `fleet`, in `phase`.
    fn worker_row(fleet: &SimFleet, number: u32, phase: Phase) -> Result<Worker> {
        Ok(Worker {
            name: worker_name(number)?,
            fleet: fleet_name(fleet.name)?,
            instance: instance_name(number)?,
            phase,
            phase_time: at(0),
            running: 0,
            slots: Some(fleet.actions_per_instance),
            paused: false,
            launch_time: at(0),
        })
    }

    /// The scaling `events` as the live state carries them: with their causes, unless the server
    /// leaves them out.
    fn streamed(&self, events: &[Event]) -> Vec<Event> {
        events
            .iter()
            .cloned()
            .map(|mut event| {
                if self.behaviour.stream_causes == StreamCauses::Missing {
                    event.cause = None;
                }
                event
            })
            .collect()
    }

    /// The live state of the deployment as `state` has it, numbered by the events recorded so far.
    fn snapshot(&self, state: &SimState) -> Result<State> {
        let (fleets, workers) = self.rows(&state.instances, &Phase::Idle)?;
        Ok(state_frame(
            sequence(state.events.len()),
            fleets,
            workers,
            self.streamed(&state.events),
            state.completed,
        ))
    }

    /// What a viewer sees while the idle fleet drains: a delta that starts the drains, then a
    /// delta that terminates the instances and removes the workers. Nothing when no instance runs.
    fn drain_deltas(&self, state: &mut SimState) -> Result<Vec<Delta>> {
        if state.instances.iter().all(Vec::is_empty) {
            return Ok(Vec::new());
        }
        let before = state.instances.clone();
        let base = sequence(state.events.len());
        let logged = state.events.len();
        self.drain_all(state)?;
        let (started, terminated): (Vec<Event>, Vec<Event>) = state
            .events
            .iter()
            .skip(logged)
            .cloned()
            .partition(|event| matches!(event.change, EventChange::DrainStarted { .. }));

        let (draining, rows) = self.rows(&before, &Phase::Draining)?;
        let removed: Vec<String> = rows.iter().map(|row| row.name.clone()).collect();
        let no_instances: Vec<Vec<u32>> = vec![Vec::new(); before.len()];
        let (emptied, _) = self.rows(&no_instances, &Phase::Idle)?;
        let delta =
            |seq: u64, fleets: Vec<Fleet>, workers: Keyed<Worker>, events: Vec<Event>| Delta {
                seq,
                base: seq - 1,
                update_time: at(seq),
                pass: None,
                queue: None,
                fleets: Some(Keyed {
                    upsert: fleets,
                    remove: Vec::new(),
                }),
                workers: Some(workers),
                instances: None,
                actions: None,
                events,
                completed: None,
                principals_epoch: None,
            };
        Ok(vec![
            delta(
                base + 1,
                draining,
                Keyed {
                    upsert: rows,
                    remove: Vec::new(),
                },
                self.streamed(&started),
            ),
            delta(
                base + 2,
                emptied,
                Keyed {
                    upsert: Vec::new(),
                    remove: removed,
                },
                self.streamed(&terminated),
            ),
        ])
    }
}

impl System for Simulation {
    fn start_server(&self) -> Result<()> {
        let mut state = self.state.lock();
        if state.pid.is_some() {
            return Err(E2eError::system("the server is already running"));
        }
        let restarted = state.next_pid > 1;
        let pid = if restarted && self.behaviour.pid == PidReuse::Reused {
            state.next_pid.saturating_sub(1)
        } else {
            let pid = state.next_pid;
            state.next_pid += 1;
            pid
        };
        state.pid = Some(pid);
        state.starts += 1;
        if !restarted && self.behaviour.warm == Warm::Preloaded {
            let number = state.next_instance;
            Self::add_running(&mut state);
            if let Some(fleet) = self.fleets.first() {
                Self::record(
                    &mut state,
                    fleet.name,
                    EventChange::WorkerRegistered {
                        worker: worker_name(number)?,
                    },
                )?;
            }
        }
        if restarted && self.behaviour.convergence == Convergence::Stuck {
            Self::add_running(&mut state);
        }
        let repeated = if restarted && self.behaviour.restart == Restart::Repeats {
            state
                .events
                .iter()
                .find(|event| matches!(event.change, EventChange::ScaledOut { .. }))
                .and_then(|event| name::parse::<name::Fleets>(&event.fleet).ok())
                .map(|fleet| fleet.to_string())
        } else {
            None
        };
        if let Some(fleet) = repeated {
            Self::record(
                &mut state,
                &fleet,
                EventChange::ScaledOut { from: 0, to: 1 },
            )?;
        }
        drop(state);
        self.changed.notify_all();
        Ok(())
    }

    fn kill_server(&self) -> Result<()> {
        let mut state = self.state.lock();
        if state.pid.take().is_none() {
            return Err(E2eError::system("the server is not running"));
        }
        // A hold that published no scope cannot be found again, and one that never signalled
        // cannot be awaited, so the server going down ends either.
        let scopeless = self.behaviour.scope_report == ScopeReport::Missing;
        if state.hold == HoldPhase::Silent || (state.hold == HoldPhase::Started && scopeless) {
            state.hold = HoldPhase::Ended;
            if self.behaviour.limits == Limits::Enforced {
                state.orphan = false;
            }
        }
        drop(state);
        self.changed.notify_all();
        Ok(())
    }

    fn server_pid(&self) -> Option<u32> {
        self.state.lock().pid
    }

    fn worker_count(&self) -> Result<usize> {
        let mut state = self.state.lock();
        let running: usize = state.instances.iter().map(Vec::len).sum();
        if running == 0 {
            return Ok(0);
        }
        if state.idle < DRAIN_AFTER {
            state.idle += 1;
            return Ok(running);
        }
        self.drain_all(&mut state)?;
        Ok(0)
    }

    fn scaling_events(&self) -> Result<Vec<Event>> {
        Ok(self.state.lock().events.clone())
    }

    fn await_history(&self, done: &mut dyn FnMut(&[Event]) -> bool) -> Result<Vec<Event>> {
        let mut state = self.state.lock();
        loop {
            if done(&state.events) {
                return Ok(state.events.clone());
            }
            if state.building > 0 || state.builds_started == 0 {
                self.changed.wait(&mut state);
                continue;
            }
            if state.instances.iter().any(|numbers| !numbers.is_empty()) {
                self.drain_all(&mut state)?;
            }
            if done(&state.events) {
                return Ok(state.events.clone());
            }
            return Err(E2eError::timeout(
                "the scaling history to satisfy the awaited condition",
                Duration::ZERO,
            ));
        }
    }

    fn anonymous_admin_status(&self) -> Result<u16> {
        Ok(match self.behaviour.admin_routes {
            AdminRoutes::Admitted => 200,
            AdminRoutes::Refused => 403,
        })
    }

    fn live_state(&self) -> Result<State> {
        self.snapshot(&self.state.lock())
    }

    fn await_stream(&self, done: &mut dyn FnMut(&[Frame]) -> bool) -> Result<Vec<Frame>> {
        let mut state = self.state.lock();
        let mut frames = vec![Frame::Snapshot(self.snapshot(&state)?)];
        if done(&frames) {
            return Ok(frames);
        }
        for delta in self.drain_deltas(&mut state)? {
            frames.push(Frame::Delta(delta));
            if done(&frames) {
                return Ok(frames);
            }
        }
        Err(E2eError::timeout(
            "the state stream to satisfy the awaited condition",
            std::time::Duration::ZERO,
        ))
    }

    fn build(&self, probes: &[Probe]) -> Result<BuildOutcome> {
        {
            let mut state = self.state.lock();
            state.building = state.building.saturating_add(1);
            state.builds_started = state.builds_started.saturating_add(1);
        }
        self.changed.notify_all();
        let outcome = self.run_build(probes);
        {
            let mut state = self.state.lock();
            state.building = state.building.saturating_sub(1);
        }
        self.changed.notify_all();
        outcome
    }

    // Models neither the action cache nor the journal, so a run changes nothing.
    fn build_run(&self, probes: &[Probe], _run: &Run) -> Result<BuildOutcome> {
        self.build(probes)
    }

    fn clean(&self) -> Result<()> {
        self.state.lock().reports.clear();
        Ok(())
    }

    fn report(&self, probe: Probe) -> Result<ProbeReport> {
        let state = self.state.lock();
        let text = state.reports.get(probe.as_str()).ok_or_else(|| {
            E2eError::system(format!("the probe `{}` has not been built", probe.as_str()))
        })?;
        text.parse()
    }

    fn host(&self) -> &Host {
        &self.host
    }

    fn tmp_path(&self) -> &std::path::Path {
        self.tmp.as_path()
    }

    fn stderr_tail_lines(&self) -> usize {
        30
    }
}

impl Simulation {
    /// The body of [`build`](Self::build), run while the build counts as in flight.
    fn run_build(&self, probes: &[Probe]) -> Result<BuildOutcome> {
        if self.behaviour.build == BuildFailure::BeforeScaleOut {
            return Ok(BuildOutcome::new(Some(34), "UNAVAILABLE".to_owned()));
        }
        if probes.contains(&Probe::Hold) {
            return Ok(match self.behaviour.hold {
                HoldBuild::Starts => self.hold()?,
                HoldBuild::FailsEarly => {
                    BuildOutcome::new(Some(1), "ERROR: //:hold failed: (Exit 1)\n".to_owned())
                }
            });
        }
        if probes.contains(&Probe::MemoryHog) {
            return Ok(self.memory_hog());
        }
        if probes.contains(&Probe::MemoryGrower) {
            return Ok(self.memory_grower());
        }
        let mut state = self.state.lock();

        let plan: Vec<(Probe, bool)> = Self::targets(probes)
            .into_iter()
            .map(|probe| {
                let hit = self.behaviour.cache == Cache::Hit
                    && probe != Probe::ExitThree
                    && state.cached.contains(probe.as_str());
                (probe, hit)
            })
            .collect();
        let hits = plan.iter().filter(|(_, hit)| *hit).count();
        let remote = plan.len() - hits;

        let actions: Vec<Probe> = plan
            .iter()
            .filter(|(_, hit)| !hit)
            .map(|(probe, _)| *probe)
            .collect();
        let Some(placed) = self.place(&actions) else {
            let stderr = "ERROR: no fleet accepts the platform properties of an action\n";
            return Ok(BuildOutcome::new(Some(1), stderr.to_owned()));
        };
        if self.scale_out(&mut state, &placed)? {
            self.changed.notify_all();
            let scaled_under = state.starts;
            while state.pid.is_none() || (self.slow_boot && state.starts == scaled_under) {
                self.changed.wait(&mut state);
            }
        }
        if remote > 0 {
            state.idle = 0;
            if self.behaviour.completed == Completed::Counted {
                state.completed += sequence(remote);
            }
        }

        let mut fleets = placed.into_iter();
        for (probe, hit) in &plan {
            let sandbox = if *hit {
                self.variant.sandbox
            } else {
                fleets
                    .next()
                    .and_then(|index| self.fleets.get(index))
                    .map_or(self.variant.sandbox, |fleet| fleet.sandbox)
            };
            if let Some(text) = self.report_for(*probe, sandbox) {
                state.reports.insert(probe.as_str(), text);
            }
            if !hit && *probe != Probe::ExitThree {
                state.cached.insert(probe.as_str());
            }
        }

        let mut stderr = self.failure_lines(probes);
        let failed = !stderr.is_empty();
        stderr.push_str(&Self::summary(remote, hits));
        let exit_code = Some(i32::from(failed));
        Ok(BuildOutcome::new(exit_code, stderr))
    }

    /// Runs the hold action: it scales out if nothing runs, publishes its scope and its orphan,
    /// then blocks until `release`. A hold that published no scope ends when the server is
    /// killed, and fails; a scoped hold survives the kill. A hold that never signals ends when
    /// the server is killed too, and `await_hold` times out on it. Under [`Limits::Enforced`] the
    /// kernel ends both the orphan and the scope with the action; otherwise both stay.
    fn hold(&self) -> Result<BuildOutcome> {
        let mut state = self.state.lock();
        if let Some(placed) = self.place(&[Probe::Hold])
            && self.scale_out(&mut state, &placed)?
        {
            self.changed.notify_all();
            let scaled_under = state.starts;
            while state.pid.is_none() || (self.slow_boot && state.starts == scaled_under) {
                self.changed.wait(&mut state);
            }
        }
        state.scope =
            (self.behaviour.scope_report == ScopeReport::Published).then(|| self.scope_facts());
        state.orphan = self.behaviour.orphan == OrphanProcess::Spawned;
        state.hold = match self.behaviour.hold_signal {
            HoldSignal::Sent => HoldPhase::Started,
            HoldSignal::Missing => HoldPhase::Silent,
        };
        if self.behaviour.hold_signal == HoldSignal::Sent {
            state.hold_notifications.push_back(Signalled::Started);
        }
        self.changed.notify_all();
        while state.hold == HoldPhase::Silent || state.hold == HoldPhase::Started {
            self.changed.wait(&mut state);
        }
        let killed = state.hold == HoldPhase::Ended;
        if !killed {
            state.hold = HoldPhase::Started;
        }
        if self.behaviour.limits == Limits::Enforced {
            state.orphan = false;
            state.scope = None;
        }
        drop(state);
        self.changed.notify_all();
        if killed {
            return Ok(BuildOutcome::new(
                Some(1),
                "ERROR: the server was killed during the hold\n".to_owned(),
            ));
        }
        Ok(BuildOutcome::new(Some(0), Self::summary(1, 0)))
    }

    /// Runs the memory hog: the kernel kills it under [`Limits::Enforced`], and it succeeds
    /// otherwise.
    fn memory_hog(&self) -> BuildOutcome {
        match self.behaviour.limits {
            Limits::Enforced => BuildOutcome::new(
                Some(137),
                format!(
                    "ERROR: //:memory_hog failed: (Exit 137)\n{}",
                    Self::summary(1, 0)
                ),
            ),
            Limits::Unlimited => BuildOutcome::new(Some(0), Self::summary(1, 0)),
        }
    }

    /// Runs the memory grower: the soft layer kills its first attempt and the scheduler requeues
    /// it, so it runs as many times as its behaviour says, and the last attempt completes.
    fn memory_grower(&self) -> BuildOutcome {
        let (attempts, completes) = match self.behaviour.grower {
            Grower::Requeued => (2, true),
            Grower::NeverKilled => (1, true),
            Grower::KilledAgain => (3, true),
            Grower::Failed => (2, false),
        };
        let mut state = self.state.lock();
        state.attempts = attempts;
        if !completes {
            return BuildOutcome::new(
                Some(1),
                "ERROR: //:memory_grower failed: (Exit 1)\n".to_owned(),
            );
        }
        state
            .reports
            .insert(Probe::MemoryGrower.as_str(), "grown=yes\n".to_owned());
        BuildOutcome::new(Some(0), Self::summary(1, 0))
    }

    /// The scope facts of the hold action under the limits the behaviour says.
    fn scope_facts(&self) -> ScopeFacts {
        let (memory_max, oom_group, cpu_max, pids_max) = match self.behaviour.limits {
            Limits::Enforced => (
                (HOLD_MEMORY_KB * 1024 * (PERCENT + u64::from(LIMIT_HEADROOM_PERCENT)) / PERCENT)
                    .to_string(),
                "1".to_owned(),
                format!(
                    "{} {}",
                    u64::from(HOLD_CPU_COUNT) * LIMIT_CPU_PERIOD_MICROS,
                    LIMIT_CPU_PERIOD_MICROS
                ),
                LIMIT_MAX_PIDS.to_string(),
            ),
            Limits::Unlimited => (
                "max".to_owned(),
                "0".to_owned(),
                format!("max {LIMIT_CPU_PERIOD_MICROS}"),
                "max".to_owned(),
            ),
        };
        ScopeFacts {
            directory: PathBuf::from(SCOPE_DIRECTORY),
            working_directory: PathBuf::from(SCOPE_WORKING_DIRECTORY),
            memory_max,
            oom_group,
            cpu_max,
            pids_max,
        }
    }
}

impl gang_e2e::Limits for Simulation {
    fn running_scope(&self) -> Result<Option<ScopeFacts>> {
        Ok(self.state.lock().scope.clone())
    }

    fn release(&self, _scope: &ScopeFacts) -> Result<()> {
        self.state.lock().hold = HoldPhase::Released;
        self.changed.notify_all();
        Ok(())
    }

    fn abandon_hold(&self) -> Result<()> {
        let mut state = self.state.lock();
        state.hold_notifications.push_back(Signalled::Abandoned);
        drop(state);
        self.changed.notify_all();
        Ok(())
    }

    fn await_hold(&self) -> Result<Signalled> {
        let mut state = self.state.lock();
        loop {
            if let Some(notification) = state.hold_notifications.pop_front() {
                return Ok(notification);
            }
            if state.hold == HoldPhase::Silent {
                // A silent hold never signals, so its deadline passes without real time.
                return Err(E2eError::timeout(
                    "the hold action to signal that its orphan runs",
                    Settings::default().hold_timeout,
                ));
            }
            self.changed.wait(&mut state);
        }
    }

    fn attempts(&self) -> Result<usize> {
        Ok(self.state.lock().attempts)
    }

    fn orphan_alive(&self) -> Result<bool> {
        Ok(self.state.lock().orphan)
    }

    fn scope_exists(&self, scope: &ScopeFacts) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .scope
            .as_ref()
            .is_some_and(|present| present.directory == scope.directory))
    }
}

/// The state of the simulated identity deployment.
struct IdentityState {
    /// The running server's process id.
    pid: Option<u32>,
    /// The principals whose tokens are revoked.
    revoked: BTreeSet<String>,
}

/// A deployment whose one token can be revoked and restored, under a [`Revocation`] behaviour.
pub struct SimulatedIdentity {
    revocation: Revocation,
    dashboard: Dashboard,
    /// The behaviour the deployment's identity flaws follow.
    behaviour: Behaviour,
    state: Arc<Mutex<IdentityState>>,
}

impl SimulatedIdentity {
    /// A running deployment whose server treats revocation as `revocation` says, and serves the
    /// dashboard as it must.
    #[must_use]
    pub fn new(revocation: Revocation) -> Self {
        Self {
            revocation,
            dashboard: Dashboard::Compliant,
            behaviour: Behaviour::COMPLIANT,
            state: Arc::new(Mutex::new(IdentityState {
                pid: Some(1),
                revoked: BTreeSet::new(),
            })),
        }
    }

    /// The same deployment, serving the dashboard as `dashboard` says.
    #[must_use]
    pub fn with_dashboard(mut self, dashboard: Dashboard) -> Self {
        self.dashboard = dashboard;
        self
    }

    /// The same deployment, behaving as `behaviour` says.
    #[must_use]
    pub fn with_behaviour(mut self, behaviour: Behaviour) -> Self {
        self.behaviour = behaviour;
        self
    }
}

/// Whether a server treating revocation as `revocation` denies the token of `principal`.
fn denies(revocation: Revocation, state: &IdentityState, principal: &str) -> bool {
    revocation != Revocation::Ignored && state.revoked.contains(principal)
}

/// The principal a simulated session cookie names, when the cookie is one the server minted.
fn session_principal(cookie: &SessionCookie) -> Option<&str> {
    cookie.value.strip_prefix(SIM_SESSION_PREFIX)
}

/// An open state stream of a [`SimulatedIdentity`]: the snapshot, then heartbeats, until the
/// principal is revoked.
pub struct SimStream {
    revocation: Revocation,
    dashboard: Dashboard,
    state: Arc<Mutex<IdentityState>>,
    principal: String,
    status: u16,
    /// Whether the first frame was sent.
    started: bool,
    /// Whether the stream ended.
    ended: bool,
    /// The heartbeats still owed before the `closed` event, under [`Dashboard::TicksBeforeClosed`].
    ticks_before_closed: u8,
}

impl SimStream {
    /// Whether the stream will still send a `closed` frame or end: the principal is revoked
    /// under a dashboard that closes on revocation.
    fn closes(&self) -> bool {
        denies(self.revocation, &self.state.lock(), &self.principal)
            && self.dashboard != Dashboard::NeverCloses
    }

    /// The reason the stream closes with once the principal is revoked.
    fn closing_reason(&self) -> ClosedReason {
        match self.dashboard {
            Dashboard::ClosesAsExpired => ClosedReason::Expired,
            _ => ClosedReason::Revoked,
        }
    }
}

impl EventStream for SimStream {
    fn status(&self) -> u16 {
        self.status
    }

    fn content_type(&self) -> Option<&str> {
        Some(if self.dashboard == Dashboard::NotAnEventStream {
            "application/json"
        } else {
            "text/event-stream"
        })
    }

    fn next_event(&mut self) -> Result<Option<Frame>> {
        if self.status != 200 || self.ended {
            return Ok(None);
        }
        if !self.started {
            self.started = true;
            return Ok(Some(if self.dashboard == Dashboard::NoSnapshot {
                heartbeat()
            } else {
                Frame::Snapshot(state_frame(0, Vec::new(), Vec::new(), Vec::new(), 0))
            }));
        }
        if !self.closes() {
            return Ok(Some(heartbeat()));
        }
        match self.dashboard {
            Dashboard::EndsBeforeClosed => {
                self.ended = true;
                Ok(None)
            }
            Dashboard::TicksBeforeClosed if self.ticks_before_closed > 0 => {
                self.ticks_before_closed -= 1;
                Ok(Some(heartbeat()))
            }
            _ => {
                self.ended = true;
                Ok(Some(Frame::Closed(Closed {
                    reason: self.closing_reason(),
                    retry_delay: None,
                })))
            }
        }
    }

    fn next_event_within(&mut self, budget: Duration) -> Result<Option<Frame>> {
        // A stream that will only send heartbeats from now on never sends anything else in time.
        let silent = self.status == 200 && !self.ended && self.started && !self.closes();
        if silent {
            return Err(E2eError::timeout(
                "the state stream to send a frame other than a heartbeat",
                budget,
            ));
        }
        self.next_event()
    }
}

/// The heartbeat frame of a stream that has nothing to say.
fn heartbeat() -> Frame {
    Frame::Tick(Tick {
        seq: 0,
        time: at(0),
    })
}

/// The one session the simulation issues.
fn credentials() -> Credentials {
    Credentials {
        principal: SIM_PRINCIPAL.to_owned(),
        home: PathBuf::from("/sim/home"),
        token: SIM_TOKEN.to_owned(),
        authority: Some(PathBuf::from("/sim/authority.pem")),
    }
}

/// An accepted connection of a [`SimulatedIdentity`].
pub struct SimChannel {
    revocation: Revocation,
    state: Arc<Mutex<IdentityState>>,
    principal: String,
    /// Whether the connection presents the token; a request without it is unauthenticated.
    presents_token: bool,
    /// Whether the server closed the connection before it was used.
    closed_early: bool,
    /// Whether a request carrying a valid token is answered denied.
    requests: Requests,
    /// Whether a request carrying no token is served.
    anonymous: Anonymous,
}

impl Channel for SimChannel {
    fn ping(&mut self) -> Result<ChannelEvent> {
        Ok(if self.closed_early {
            ChannelEvent::Closed
        } else {
            ChannelEvent::PingAcknowledged
        })
    }

    fn request(&mut self) -> Result<Reply> {
        if self.closed_early {
            return Ok(Reply::Closed);
        }
        if !self.presents_token {
            return Ok(match self.anonymous {
                Anonymous::Unauthenticated => Reply::Unauthenticated,
                Anonymous::Served => Reply::Served,
            });
        }
        let state = self.state.lock();
        let denied =
            self.requests == Requests::Denied || denies(self.revocation, &state, &self.principal);
        Ok(if denied { Reply::Denied } else { Reply::Served })
    }
}

impl Identity for SimulatedIdentity {
    type Channel = SimChannel;
    type Stream = SimStream;

    fn issue(&self, spec: &TokenSpec) -> Result<Issuance> {
        if spec.audience != AUDIENCE {
            return Ok(Issuance::Refused(format!(
                "the audience `{}` is not accepted",
                spec.audience
            )));
        }
        if spec.organization != ORGANIZATION {
            return Ok(Issuance::Refused(format!(
                "the organization `{}` is not admitted",
                spec.organization
            )));
        }
        if self.behaviour.token_issue == TokenIssue::Refuses {
            return Ok(Issuance::Refused(
                "the developer token is refused".to_owned(),
            ));
        }
        Ok(Issuance::Issued(credentials()))
    }

    fn connect(
        &self,
        credentials: &Credentials,
        presenting: Presenting,
    ) -> Result<Admission<SimChannel>> {
        if presenting == Presenting::Token && self.behaviour.handshake == Handshake::Refuses {
            return Ok(Admission::Refused);
        }
        Ok(Admission::Accepted(SimChannel {
            revocation: self.revocation,
            state: Arc::clone(&self.state),
            principal: credentials.principal.clone(),
            presents_token: presenting == Presenting::Token,
            closed_early: self.revocation == Revocation::EarlyClose,
            requests: self.behaviour.requests,
            anonymous: self.behaviour.anonymous,
        }))
    }

    fn build_as(&self, credentials: &Credentials, presenting: Presenting) -> Result<BuildOutcome> {
        let state = self.state.lock();
        if presenting == Presenting::Token
            && !denies(self.revocation, &state, &credentials.principal)
        {
            return Ok(BuildOutcome::new(
                Some(0),
                "INFO: 1 process: 1 remote.\n".to_owned(),
            ));
        }
        let stderr = match presenting {
            Presenting::Nothing => {
                "ERROR: Remote execution failed: UNAUTHENTICATED: a bearer token is required\n"
            }
            Presenting::Token if self.revocation == Revocation::BuildFailsElsewhere => {
                "ERROR: the build failed: no action ran\n"
            }
            Presenting::Token => {
                "ERROR: Remote execution failed: PERMISSION_DENIED: the token is revoked\n"
            }
        };
        Ok(BuildOutcome::new(Some(1), stderr.to_owned()))
    }

    fn revoke(&self, target: &Credentials) -> Result<()> {
        let mut state = self.state.lock();
        state.revoked.insert(target.principal.clone());
        if self.revocation == Revocation::RestartedOnRevoke {
            state.pid = state.pid.map(|pid| pid + 1);
        }
        Ok(())
    }

    fn restore(&self, target: &Credentials) -> Result<()> {
        if self.revocation != Revocation::RestoreIgnored {
            self.state.lock().revoked.remove(&target.principal);
        }
        Ok(())
    }

    fn server_pid(&self) -> Option<u32> {
        self.state.lock().pid
    }

    fn administrator(&self) -> Result<Credentials> {
        Ok(Credentials {
            principal: SIM_ADMINISTRATOR.to_owned(),
            home: PathBuf::from("/sim/admin"),
            token: SIM_ADMIN_TOKEN.to_owned(),
            authority: Some(PathBuf::from("/sim/authority.pem")),
        })
    }

    fn session_cookie(&self, who: &Credentials) -> Result<SessionCookie> {
        Ok(SessionCookie {
            name: SIM_COOKIE.to_owned(),
            value: format!("{SIM_SESSION_PREFIX}{}", who.principal),
        })
    }

    fn status(&self, path: &str, cookie: Option<&SessionCookie>) -> Result<u16> {
        if ![GET_STATE, WATCH_STATE]
            .iter()
            .any(|route| route.template == path)
        {
            return Ok(404);
        }
        let Some(cookie) = cookie else {
            return Ok(if self.dashboard == Dashboard::OpenToAnonymous {
                200
            } else {
                401
            });
        };
        Ok(match session_principal(cookie) {
            None => 401,
            Some(principal) if denies(self.revocation, &self.state.lock(), principal) => 403,
            Some(_) => 200,
        })
    }

    fn open_state_stream(&self, cookie: &SessionCookie) -> Result<SimStream> {
        let principal = session_principal(cookie);
        Ok(SimStream {
            revocation: self.revocation,
            dashboard: self.dashboard,
            state: Arc::clone(&self.state),
            principal: principal.unwrap_or_default().to_owned(),
            status: if principal.is_some() { 200 } else { 401 },
            started: false,
            ended: false,
            ticks_before_closed: if self.dashboard == Dashboard::TicksBeforeClosed {
                2
            } else {
                0
            },
        })
    }
}
