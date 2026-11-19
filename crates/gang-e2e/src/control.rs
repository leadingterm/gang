//! The control family's view of a deployment: the control-loop keys its scenarios set, the faults
//! they inject, and the readings of the state stream they judge.

use std::fs::{self, DirBuilder};
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use gang_wire::{Event, EventChange, Frame, Pass, State};
use rustix::process::{Pid, Signal, kill_process};

use crate::build_output::BuildOutcome;
use crate::errors::{E2eError, Result};
use crate::event_log::sequence;
use crate::process;
use crate::stack::Stack;
use crate::system::{Probe, System};

/// The control-loop keys of a deployment that a control scenario sets; `None` keeps the base
/// configuration's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlKnobs {
    /// The top-level `poll_interval`: the time between scheduling passes.
    pub poll_interval: Option<Duration>,
    /// `boot_grace` of every `[[fleets]]` entry.
    pub boot_grace: Option<Duration>,
    /// `idle_timeout` of every `[[fleets]]` entry.
    pub idle_timeout: Option<Duration>,
    /// `min_instances` of every `[[fleets]]` entry.
    pub min_instances: Option<u32>,
    /// `max_instances` of every `[[fleets]]` entry.
    pub max_instances: Option<u32>,
    /// Concurrent actions targeted per instance in every fleet.
    pub target_actions_per_instance: Option<u32>,
    /// Virtual CPUs advertised per instance in every fleet.
    pub vcpus_per_instance: Option<u32>,
    /// A `[[fleets.scheduled_minimum]]` window in force at every instant (all seven `days`,
    /// `00:00` to `24:00`, `time_zone = "Asia/Kathmandu"`) with this many `instances`, on every
    /// `[[fleets]]` entry.
    pub scheduled_minimum: Option<u32>,
    /// `[provisioner.local] terminate_grace`: how long a worker sent `SIGTERM` may run before it
    /// is killed.
    pub terminate_grace: Option<Duration>,
    /// `[dashboard] detail_interval`: how often the state's sample of the scheduler's actions is
    /// refreshed.
    pub detail_interval: Option<Duration>,
    /// The client action abandonment timeout.
    pub client_action_timeout: Option<Duration>,
    /// HTTP/2 per-connection request limit for the held build.
    pub max_concurrent_streams: Option<u32>,
    /// Use a nonexistent local worker launcher.
    pub broken_launcher: bool,
    /// Install a gate for worker launches.
    pub holdable_launches: bool,
}

impl ControlKnobs {
    /// Every key as the base configuration has it.
    pub const NONE: Self = Self {
        poll_interval: None,
        boot_grace: None,
        idle_timeout: None,
        min_instances: None,
        max_instances: None,
        target_actions_per_instance: None,
        vcpus_per_instance: None,
        scheduled_minimum: None,
        terminate_grace: None,
        detail_interval: None,
        client_action_timeout: None,
        max_concurrent_streams: None,
        broken_launcher: false,
        holdable_launches: false,
    };

    /// The orphan scenario: one standing instance and no more, a pass every second, a ten-second
    /// boot grace, and a terminated worker killed one second after `SIGTERM`.
    pub const ORPHAN: Self = Self {
        poll_interval: Some(Duration::from_secs(1)),
        boot_grace: Some(Duration::from_secs(10)),
        min_instances: Some(1),
        max_instances: Some(1),
        terminate_grace: Some(Duration::from_secs(1)),
        ..Self::NONE
    };

    /// The drain-cancel scenario: at most one instance and none standing, a pass every two
    /// seconds, a worker idle for five seconds drains, and the action sample is refreshed every
    /// second, so an action queued while passes are skipped shows in the state. The scenario
    /// reacts to a streamed drain within one `poll_interval`; two seconds leave that reaction,
    /// which does no I/O but one directory operation or one signal, a wide margin.
    pub const DRAIN_CANCEL: Self = Self {
        poll_interval: Some(Duration::from_secs(2)),
        idle_timeout: Some(Duration::from_secs(5)),
        min_instances: Some(0),
        max_instances: Some(1),
        detail_interval: Some(Duration::from_secs(1)),
        ..Self::NONE
    };

    /// The skipped-pass scenario: a pass every second.
    pub const SKIPPED_PASS: Self = Self {
        poll_interval: Some(Duration::from_secs(1)),
        ..Self::NONE
    };

    /// A failed local launch and a short client abandonment timeout.
    pub const LAUNCH_FAILURE: Self = Self {
        poll_interval: Some(Duration::from_secs(1)),
        idle_timeout: Some(Duration::from_secs(5)),
        min_instances: Some(0),
        max_instances: Some(1),
        client_action_timeout: Some(Duration::from_secs(15)),
        broken_launcher: true,
        ..Self::NONE
    };

    /// One worker kept by a scheduled minimum alone: `min_instances` 0, at most one instance,
    /// a pass every second.
    pub const SCHEDULED_MINIMUM: Self = Self {
        poll_interval: Some(Duration::from_secs(1)),
        min_instances: Some(0),
        max_instances: Some(1),
        scheduled_minimum: Some(1),
        ..Self::NONE
    };

    /// A short burst arriving while the first worker boots.
    pub const BURST_FROM_ZERO: Self = Self {
        poll_interval: Some(Duration::from_secs(1)),
        boot_grace: Some(Duration::from_mins(1)),
        idle_timeout: Some(Duration::from_secs(5)),
        min_instances: Some(0),
        max_instances: Some(6),
        target_actions_per_instance: Some(16),
        vcpus_per_instance: Some(16),
        // All 160 pending Execute RPCs must fit on one connection before launch release;
        // the default limit of 100 can strand the last 60.
        max_concurrent_streams: Some(160),
        holdable_launches: true,
        ..Self::NONE
    };

    /// Short running work can absorb an additional queued action.
    pub const QUEUED_BEHIND_SHORT_WORK: Self = Self {
        poll_interval: Some(Duration::from_secs(1)),
        boot_grace: Some(Duration::from_mins(1)),
        min_instances: Some(1),
        max_instances: Some(3),
        target_actions_per_instance: Some(2),
        vcpus_per_instance: Some(2),
        holdable_launches: true,
        ..Self::NONE
    };

    /// A persistent backlog grows the fleet to its bound.
    pub const BACKLOG_GROWS_FLEET: Self = Self {
        poll_interval: Some(Duration::from_secs(1)),
        boot_grace: Some(Duration::from_mins(1)),
        min_instances: Some(0),
        max_instances: Some(3),
        target_actions_per_instance: Some(1),
        vcpus_per_instance: Some(1),
        holdable_launches: true,
        ..Self::NONE
    };

    /// Every preset a control scenario runs under, in suite order: the one list the suite's
    /// presets and the configuration parse tests draw from.
    pub const PRESETS: [Self; 8] = [
        Self::ORPHAN,
        Self::DRAIN_CANCEL,
        Self::SKIPPED_PASS,
        Self::LAUNCH_FAILURE,
        Self::SCHEDULED_MINIMUM,
        Self::BURST_FROM_ZERO,
        Self::QUEUED_BEHIND_SHORT_WORK,
        Self::BACKLOG_GROWS_FLEET,
    ];
}

/// Faults a control scenario injects into a [`System`].
pub trait Control: System {
    /// Holds worker launches, including launches requested before the server starts.
    ///
    /// # Errors
    ///
    /// When the launch gate cannot be created.
    fn hold_launches(&self) -> Result<()>;

    /// Releases all held launches and allows future launches until the next hold.
    ///
    /// # Errors
    ///
    /// When the launch gate cannot be removed.
    fn release_launches(&self) -> Result<()>;

    /// Makes the provisioner's supply read fail, so that every scheduling pass is skipped, until
    /// [`Control::release_supply`]. Works before the server starts too.
    ///
    /// # Errors
    ///
    /// When the fault cannot be injected.
    fn hold_supply(&self) -> Result<()>;

    /// Undoes [`Control::hold_supply`].
    ///
    /// # Errors
    ///
    /// When the fault cannot be removed.
    fn release_supply(&self) -> Result<()>;

    /// Stops every worker process of the deployment with `SIGSTOP`: each keeps its instance
    /// alive but no longer answers the scheduler. Returns how many it stopped.
    ///
    /// # Errors
    ///
    /// When the process table cannot be read or a worker cannot be signalled.
    fn stop_workers(&self) -> Result<usize>;

    /// Resumes every worker process of the deployment with `SIGCONT`; a running worker ignores
    /// it. Undoes [`Control::stop_workers`] for a worker that outlived the scenario.
    ///
    /// # Errors
    ///
    /// When the process table cannot be read or a worker cannot be signalled.
    fn continue_workers(&self) -> Result<()>;

    /// Builds a named run using an independent Bazel output root while a primary build runs.
    ///
    /// # Errors
    ///
    /// When the additional build cannot be run.
    fn build_additional_run(
        &self,
        probes: &[Probe],
        run: &crate::execution::Run,
    ) -> Result<BuildOutcome>;

    /// Builds `probes` on another thread while `during` runs on this one, and returns how the
    /// build ended once both are done. An error of `during` is returned after the build ended.
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run, the build thread panics, or `during` fails.
    fn build_during(
        &self,
        probes: &[Probe],
        during: &mut dyn FnMut() -> Result<()>,
    ) -> Result<BuildOutcome> {
        thread::scope(|scope| {
            let build = scope.spawn(|| self.build(probes));
            let observed = during();
            let outcome = build
                .join()
                .map_err(|_| E2eError::system("the build thread panicked"))?;
            observed?;
            outcome
        })
    }
    /// Builds a named run while `during` observes it on the calling thread.
    ///
    /// # Errors
    ///
    /// When the build fails to run, its thread panics, or `during` fails.
    fn build_run_during(
        &self,
        probes: &[Probe],
        run: &crate::execution::Run,
        during: &mut dyn FnMut() -> Result<()>,
    ) -> Result<BuildOutcome> {
        thread::scope(|scope| {
            let build = scope.spawn(|| self.build_run(probes, run));
            let observed = during();
            let outcome = build
                .join()
                .map_err(|_| E2eError::system("the build thread panicked"))?;
            observed?;
            outcome
        })
    }
}

impl Control for Stack<'_> {
    fn build_additional_run(
        &self,
        probes: &[Probe],
        run: &crate::execution::Run,
    ) -> Result<BuildOutcome> {
        let (_cancel, stop) = crate::held_build::Cancel::pair();
        self.build_additional(probes, run, stop)
    }

    fn hold_launches(&self) -> Result<()> {
        self.hold_worker_launches()
    }

    fn release_launches(&self) -> Result<()> {
        self.release_worker_launches()
    }

    fn hold_supply(&self) -> Result<()> {
        let files = DesiredFiles::of(self)?;
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&files.directory)
            .map_err(E2eError::io(format!(
                "creating {}",
                files.directory.display()
            )))?;
        absent_is_fine(fs::rename(&files.desired, &files.held)).map_err(E2eError::io(format!(
            "moving {} aside",
            files.desired.display()
        )))?;
        DirBuilder::new()
            .mode(0o700)
            .create(&files.desired)
            .map_err(E2eError::io(format!(
                "creating {}",
                files.desired.display()
            )))
    }

    fn release_supply(&self) -> Result<()> {
        let files = DesiredFiles::of(self)?;
        fs::remove_dir(&files.desired).map_err(E2eError::io(format!(
            "removing {}",
            files.desired.display()
        )))?;
        absent_is_fine(fs::rename(&files.held, &files.desired)).map_err(E2eError::io(format!(
            "restoring {}",
            files.desired.display()
        )))
    }

    fn stop_workers(&self) -> Result<usize> {
        let entries = process::snapshot();
        let workers = process::workers_of(&entries, &self.placement().config_file());
        for worker in &workers {
            signal_process(worker.pid, Signal::STOP)?;
        }
        Ok(workers.len())
    }

    fn continue_workers(&self) -> Result<()> {
        let entries = process::snapshot();
        for worker in process::workers_of(&entries, &self.placement().config_file()) {
            signal_process(worker.pid, Signal::CONT)?;
        }
        Ok(())
    }
}

/// The file in the Local provisioner's state directory that persists the desired capacity of the
/// deployment's first fleet, and where [`Control::hold_supply`] keeps it. A directory in its
/// place makes the provisioner's supply read fail.
struct DesiredFiles {
    /// The provisioner's fleet directory.
    directory: PathBuf,
    /// `<fleet>.desired`, which the supply read opens.
    desired: PathBuf,
    /// Where the desired capacity waits while the supply is held.
    held: PathBuf,
}

impl DesiredFiles {
    /// The files of the first fleet that `stack`'s configuration declares.
    fn of(stack: &Stack<'_>) -> Result<Self> {
        let config_file = stack.placement().config_file();
        let text = fs::read_to_string(&config_file)
            .map_err(E2eError::io(format!("reading {}", config_file.display())))?;
        let config: toml::Table = text
            .parse()
            .map_err(E2eError::parse("the deployment's configuration"))?;
        let fleet = config
            .get("fleets")
            .and_then(toml::Value::as_array)
            .and_then(|fleets| fleets.first())
            .and_then(|fleet| fleet.get("name"))
            .and_then(toml::Value::as_str)
            .ok_or_else(|| E2eError::system("the deployment's configuration names no fleet"))?;
        let directory = stack.placement().state_dir().join("fleets");
        Ok(Self {
            desired: directory.join(format!("{fleet}.desired")),
            held: directory.join(format!("{fleet}.desired.held")),
            directory,
        })
    }
}

/// `result`, with a missing file counted as success.
fn absent_is_fine(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// Sends `signal` to the process `pid`.
fn signal_process(pid: u32, signal: Signal) -> Result<()> {
    let target = i32::try_from(pid)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| E2eError::system(format!("{pid} is not a process id")))?;
    kill_process(target, signal)
        .map_err(|errno| E2eError::io(format!("signalling worker {pid}"))(io::Error::from(errno)))
}

/// The states the frames of a state stream describe, in order: a snapshot replaces the state and
/// a delta patches the latest one; ticks and the closing frame describe none.
///
/// # Errors
///
/// When a delta comes before any snapshot or does not apply to the state before it.
pub(crate) fn states(frames: &[Frame]) -> Result<Vec<State>> {
    let mut states: Vec<State> = Vec::new();
    for frame in frames {
        match frame {
            Frame::Snapshot(state) => states.push(state.clone()),
            Frame::Delta(delta) => {
                let mut next = states.last().cloned().ok_or_else(|| {
                    E2eError::system("the state stream sent a delta before its snapshot")
                })?;
                gang_state::apply(&mut next, delta).map_err(E2eError::system_from(
                    "applying a delta of the state stream",
                ))?;
                states.push(next);
            }
            Frame::Tick(_) | Frame::Closed(_) => {}
        }
    }
    Ok(states)
}

/// Whether the latest state the frames describe satisfies `holds`. Frames that cannot be read
/// count as satisfying it, so that the wait ends and the caller reads the error from [`states`].
pub(crate) fn latest_holds(frames: &[Frame], holds: impl Fn(&State) -> bool) -> bool {
    states(frames).map_or(true, |states| states.last().is_some_and(holds))
}

/// The pass outcomes the frames carry, in order: each snapshot's, and each delta's that has one.
pub(crate) fn passes(frames: &[Frame]) -> Vec<&Pass> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::Snapshot(state) => Some(&state.pass),
            Frame::Delta(delta) => delta.pass.as_ref(),
            Frame::Tick(_) | Frame::Closed(_) => None,
        })
        .collect()
}

/// The scaling events the frames carry that were recorded after the stream opened, oldest first
/// and each once: those newer than every event of the opening snapshot.
///
/// # Errors
///
/// When the stream does not open with a snapshot, or an event name carries no sequence number.
pub(crate) fn streamed_events(frames: &[Frame]) -> Result<Vec<&Event>> {
    let Some((first, rest)) = frames.split_first() else {
        return Ok(Vec::new());
    };
    let Frame::Snapshot(opening) = first else {
        return Err(E2eError::system(
            "the state stream did not open with a snapshot",
        ));
    };
    let mut newest = opening.events.iter().try_fold(0, |newest, event| {
        sequence(event).map(|seq| newest.max(seq))
    })?;
    let mut events = Vec::new();
    for frame in rest {
        let carried: &[Event] = match frame {
            Frame::Snapshot(state) => &state.events,
            Frame::Delta(delta) => &delta.events,
            Frame::Tick(_) | Frame::Closed(_) => &[],
        };
        for event in carried {
            let seq = sequence(event)?;
            if seq > newest {
                newest = seq;
                events.push(event);
            }
        }
    }
    Ok(events)
}

/// Whether the frames carry an event recorded after the stream opened that `matches`. Frames that
/// cannot be read count as carrying one, as in [`latest_holds`].
pub(crate) fn streamed(frames: &[Frame], matches: impl Fn(&Event) -> bool) -> bool {
    streamed_events(frames).map_or(true, |events| events.into_iter().any(matches))
}

/// Whether `event` records that `instance`, a reference `instances/{instance}`, was terminated.
pub(crate) fn terminates(event: &Event, instance: &str) -> bool {
    matches!(&event.change, EventChange::Terminated { instance: terminated } if terminated == instance)
}

/// The worker whose drain `event` records starting, as a reference `workers/{worker}`.
pub(crate) fn drain_started(event: &Event) -> Option<&str> {
    match &event.change {
        EventChange::DrainStarted { worker } => Some(worker),
        _ => None,
    }
}
