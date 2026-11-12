//! Verdicts of the Gate 1 checks: pure functions from observations to named [`Check`]s.
//!
//! Each function takes what a scenario observed (a probe report, a build outcome, the scaling
//! audit log) and returns the checks it implies. Nothing here touches the system under test, so
//! every verdict can be exercised with in-memory observations.

use std::borrow::Cow;
use std::path::Path;

use gang_config::InputLinks;
use gang_types::{FleetName, InstanceId};
use gang_wire::{Cause, ClosedReason, Event, EventChange, Frame, State, name};

use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::host::Host;
use crate::mirror::follow;
use crate::names;
use crate::report::{IsolationReport, Namespaces, PoisonReport, VictimReport};
use crate::system::{
    HOLD_CPU_COUNT, HOLD_MEMORY_KB, LIMIT_CPU_PERIOD_MICROS, LIMIT_HEADROOM_PERCENT,
    LIMIT_MAX_PIDS, PermitNetwork, Sandbox, ScopeFacts,
};

/// Whether the workers' net and mnt namespaces differ from the host's.
#[must_use]
pub fn namespaces(sandbox: Sandbox, host: &Host, isolated: &Namespaces) -> Vec<Check> {
    match sandbox {
        Sandbox::Noop => vec![Check::skipped(
            names::isolation::NET_AND_MNT_NAMESPACES,
            "the noop sandbox does not isolate",
        )],
        Sandbox::Bwrap => {
            let (Some(host_net), Some(host_mnt)) = (host.net.as_deref(), host.mnt.as_deref())
            else {
                let reason = "host namespaces are not readable on this platform";
                return vec![
                    Check::skipped(names::isolation::NET_NAMESPACE, reason),
                    Check::skipped(names::isolation::MNT_NAMESPACE, reason),
                ];
            };
            vec![
                differs_from_host(
                    names::isolation::NET_NAMESPACE,
                    "net",
                    &isolated.net,
                    host_net,
                ),
                differs_from_host(
                    names::isolation::MNT_NAMESPACE,
                    "mnt",
                    &isolated.mnt,
                    host_mnt,
                ),
            ]
        }
    }
}

/// Whether the action's parent directories show no more than the sandbox allows.
///
/// Under bwrap the worker's CAS sits beside the action's grandparent directory, so the parent must
/// list only the working directory, the grandparent only the parent and the great-grandparent only
/// the grandparent. The noop control must list all three levels, the great-grandparent holding a
/// sibling of the grandparent, which proves the probe would see the CAS if it were there.
pub fn siblings_hidden(sandbox: Sandbox, isolated: &IsolationReport) -> Check {
    let cwd_name = &isolated.cwd_name;
    let parent_name = &isolated.parent_name;
    let grandparent_name = &isolated.grandparent_name;
    let parent = &isolated.parent_entries;
    let grandparent = &isolated.grandparent_entries;
    let great_grandparent = &isolated.great_grandparent_entries;
    match sandbox {
        Sandbox::Bwrap => {
            let leak = if parent.as_slice() != std::slice::from_ref(cwd_name) {
                Some(format!(
                    "the action's parent directory lists {parent:?}, expected only `{cwd_name}`"
                ))
            } else if grandparent.as_slice() != std::slice::from_ref(parent_name) {
                Some(format!(
                    "the action's grandparent directory lists {grandparent:?}, expected only `{parent_name}`"
                ))
            } else if great_grandparent.as_slice() != std::slice::from_ref(grandparent_name) {
                Some(format!(
                    "the action's great-grandparent directory lists {great_grandparent:?}, expected only `{grandparent_name}`"
                ))
            } else {
                None
            };
            Check::expect(names::isolation::SIBLINGS_HIDDEN, leak.is_none(), || {
                leak.unwrap_or_default()
            })
        }
        Sandbox::Noop => Check::expect(
            names::isolation::SIBLINGS_LISTED,
            parent.contains(cwd_name)
                && grandparent.contains(parent_name)
                && great_grandparent.contains(grandparent_name)
                && great_grandparent.len() > 1,
            || {
                format!(
                    "the probe lists {parent:?}, {grandparent:?} and then {great_grandparent:?}; the parent must list `{cwd_name}`, the grandparent `{parent_name}` and the great-grandparent `{grandparent_name}` plus a sibling"
                )
            },
        ),
    }
}

/// Whether the workers' working directory is writable, and whether it stays visible when it lies
/// under the sandbox's tmp path.
#[must_use]
pub fn working_directory(tmp_path: &Path, isolated: &IsolationReport) -> Vec<Check> {
    let cwd = isolated.cwd.as_str();
    let not_writable = || format!("working directory `{cwd}` reported cwd_writable = `no`");
    let mut checks = vec![Check::expect(
        names::isolation::WORKING_DIRECTORY_WRITABLE,
        isolated.cwd_writable,
        not_writable,
    )];
    let name = names::isolation::WORKING_DIRECTORY_VISIBLE;
    if Path::new(cwd).starts_with(tmp_path) {
        checks.push(Check::expect(name, isolated.cwd_writable, not_writable));
    } else {
        checks.push(Check::skipped(
            name,
            "the working directory is not under the sandbox tmp path",
        ));
    }
    checks
}

/// Whether network-namespace sharing follows `permit_network_property` and the request.
#[must_use]
pub fn network(
    sandbox: Sandbox,
    permit: PermitNetwork,
    host: &Host,
    isolated: &Namespaces,
    requested: &Namespaces,
) -> Vec<Check> {
    let host_net = match (sandbox, host.net.as_deref()) {
        (Sandbox::Noop, _) => {
            return vec![Check::skipped(
                names::network::SHARING,
                "the noop sandbox does not isolate",
            )];
        }
        (Sandbox::Bwrap, None) => {
            return vec![Check::skipped(
                names::network::SHARING,
                "host namespaces are not readable on this platform",
            )];
        }
        (Sandbox::Bwrap, Some(net)) => net,
    };
    let requested_net = requested.net.as_str();
    let mut checks = vec![differs_from_host(
        names::network::OWN_NAMESPACE_BY_DEFAULT,
        "net",
        &isolated.net,
        host_net,
    )];
    checks.push(match permit {
        PermitNetwork::Permitted => Check::expect(
            names::network::REQUEST_SHARED,
            requested_net == host_net,
            || format!("the requesting action ran in net namespace {requested_net}"),
        ),
        PermitNetwork::Denied => differs_from_host(
            names::network::REQUEST_NOT_PERMITTED,
            "net",
            requested_net,
            host_net,
        ),
    });
    checks
}

/// Whether `observed` is a namespace id of `kind` that differs from the host's `host`.
///
/// A value of any other shape fails outright, so an unreadable or missing probe value cannot pass
/// a "differs from the host" check.
fn differs_from_host(name: &'static str, kind: &str, observed: &str, host: &str) -> Check {
    if !namespace_id(kind, observed) {
        return Check::failed(
            name,
            format!("the probe reported `{observed}`, not a {kind} namespace id"),
        );
    }
    Check::expect(name, observed != host, || {
        format!("the action ran in the host's {kind} namespace {host}")
    })
}

/// Whether `value` has the shape `<kind>:[<inode>]` in which a probe prints a namespace id.
fn namespace_id(kind: &str, value: &str) -> bool {
    value
        .strip_prefix(kind)
        .and_then(|rest| rest.strip_prefix(":["))
        .and_then(|rest| rest.strip_suffix(']'))
        .is_some_and(|inode| !inode.is_empty() && inode.bytes().all(|byte| byte.is_ascii_digit()))
}

/// Whether an action exiting with `code` fails the build with that exit code.
pub fn exit_status(outcome: &BuildOutcome, code: i32) -> Check {
    Check::expect(
        names::build::exit_status(code),
        !outcome.succeeded() && outcome.action_exit(code),
        || {
            if outcome.succeeded() {
                "the build succeeded".to_owned()
            } else {
                format!("no action reported `(Exit {code})` in the build output")
            }
        },
    )
}

/// The suffix that quotes the last `lines` lines of a failed build's stderr, or nothing when the
/// build succeeded.
fn build_said(outcome: &BuildOutcome, lines: usize) -> String {
    if outcome.succeeded() {
        String::new()
    } else {
        format!("; the build said:\n{}", outcome.stderr_tail(lines))
    }
}

/// Whether the build succeeded with at least `actions` actions run remotely.
///
/// A failed build quotes the last `stderr_tail_lines` lines of its stderr in the detail.
pub fn remote_execution(outcome: &BuildOutcome, actions: u32, stderr_tail_lines: usize) -> Check {
    let remote = outcome.processes.remote();
    Check::expect(
        names::build::REMOTE_EXECUTION,
        outcome.succeeded() && remote >= actions,
        || {
            format!(
                "exit {:?}, {remote} remote processes, {actions} expected{}",
                outcome.exit_code,
                build_said(outcome, stderr_tail_lines)
            )
        },
    )
}

/// Whether a rebuild after clean was served entirely from the remote cache.
///
/// A failed build quotes the last `stderr_tail_lines` lines of its stderr in the detail.
pub fn cache_hit(outcome: &BuildOutcome, actions: u32, stderr_tail_lines: usize) -> Check {
    let remote = outcome.processes.remote();
    let hits = outcome.processes.remote_cache_hits();
    Check::expect(
        names::build::CACHE_HIT,
        outcome.succeeded() && remote == 0 && hits >= actions,
        || {
            format!(
                "exit {:?}, {remote} remote, {hits} remote cache hits, {actions} expected{}",
                outcome.exit_code,
                build_said(outcome, stderr_tail_lines)
            )
        },
    )
}

/// Whether a modified cached input reached a later action, as `links` predicts.
///
/// With [`InputLinks::Break`] the later action must not see the modification; with
/// [`InputLinks::Keep`] it must, which proves the modification is observable at all.
pub fn input_integrity(links: InputLinks, victim: &VictimReport) -> Check {
    match links {
        InputLinks::Break => {
            Check::expect(names::integrity::NO_ALTERATION, !victim.poisoned, || {
                "a later action saw the modified input".to_owned()
            })
        }
        InputLinks::Keep => Check::expect(names::integrity::KEEP_CONTROL, victim.poisoned, || {
            "the later action reported `poisoned = no`".to_owned()
        }),
    }
}

/// Whether the poisoning action got as far as modifying its input.
pub fn poison_attempted(poison: &PoisonReport) -> Check {
    Check::expect(names::integrity::POISON_ATTEMPTED, poison.modified, || {
        "the poisoning action reported `attempt = refused`".to_owned()
    })
}

/// Lower-case diagnostics of a build the server rejected for its identity: the gRPC
/// `UNAUTHENTICATED` and `PERMISSION_DENIED` statuses and the HTTP 401 and 403 they map to.
/// Matched after `_` is replaced with a space.
const AUTHENTICATION_MARKERS: [&str; 5] = [
    "unauthenticated",
    "permission denied",
    "permissiondenied",
    "http 401",
    "http 403",
];

/// Whether a build refused for its identity (and only then) was rejected for authentication by
/// the server. A refusal made on the client side is not a rejection.
pub fn authentication_rejection(
    name: impl Into<Cow<'static, str>>,
    outcome: &BuildOutcome,
) -> Check {
    let stderr = outcome.stderr.to_lowercase().replace('_', " ");
    let rejected = AUTHENTICATION_MARKERS
        .iter()
        .any(|marker| stderr.contains(marker));
    Check::expect(name, !outcome.succeeded() && rejected, || {
        if outcome.succeeded() {
            "the build succeeded".to_owned()
        } else {
            format!(
                "the build failed for another reason: {}",
                outcome.stderr.trim()
            )
        }
    })
}

/// Whether exactly `expected` scale-outs were recorded.
pub fn scale_outs(events: &[Event], name: impl Into<Cow<'static, str>>, expected: usize) -> Check {
    let count = events
        .iter()
        .filter(|event| matches!(event.change, EventChange::ScaledOut { .. }))
        .count();
    Check::expect(name, count == expected, || {
        format!("{count} scale-outs recorded, {expected} expected")
    })
}

/// Whether the first scale-out raised the fleet from zero to one.
pub fn first_scale_out_from_zero(events: &[Event]) -> Check {
    const NAME: &str = names::scaling::FIRST_SCALE_OUT_FROM_ZERO;
    let first = events.iter().find_map(|event| match event.change {
        EventChange::ScaledOut { from, to } => Some((from, to)),
        _ => None,
    });
    match first {
        None => Check::failed(NAME, "no scale-out was recorded"),
        Some((from, to)) => Check::expect(NAME, from == 0 && to == 1, || {
            format!("the first scale-out went from {from} to {to}")
        }),
    }
}

/// The fleets that scaled out in `events`, each once, in the order of their first scale-out. An
/// event whose fleet is not a fleet name is left out.
#[must_use]
pub fn scaled_out_fleets(events: &[Event]) -> Vec<FleetName> {
    let mut fleets: Vec<FleetName> = Vec::new();
    for event in events {
        if !matches!(event.change, EventChange::ScaledOut { .. }) {
            continue;
        }
        if let Ok(fleet) = name::parse::<name::Fleets>(&event.fleet)
            && !fleets.contains(&fleet)
        {
            fleets.push(fleet);
        }
    }
    fleets
}

/// Whether the scale-outs recorded since `before` are those of the fleets `expected`, each once
/// and in this order, and of no other fleet.
///
/// `before` is the scaling history before the step and `after` the history once it ended, so
/// `after` must start with `before`; a history that does not is a failed check.
pub fn placed_on(
    name: impl Into<Cow<'static, str>>,
    before: &[Event],
    after: &[Event],
    expected: &[&str],
) -> Check {
    let Some(recorded) = after.strip_prefix(before) else {
        return Check::failed(
            name,
            "the scaling history no longer starts with the events recorded before the step",
        );
    };
    let fleets = scaled_out_fleets(recorded);
    let observed: Vec<&str> = fleets.iter().map(FleetName::as_str).collect();
    Check::expect(name, observed == expected, || {
        format!("fleets scaled out {observed:?}, expected {expected:?}")
    })
}

/// Whether an action of the noop fleet ran in the host's own net and mnt namespaces, which the
/// sandbox `native` of the other fleets would have replaced.
pub fn noop_fleet_unisolated(native: Sandbox, host: &Host, observed: &Namespaces) -> Check {
    const NAME: &str = names::isolation::NOOP_FLEET_UNISOLATED;
    if native == Sandbox::Noop {
        return Check::skipped(
            NAME,
            "the other fleets run under the noop sandbox too, so only placement differs",
        );
    }
    let (Some(host_net), Some(host_mnt)) = (host.net.as_deref(), host.mnt.as_deref()) else {
        return Check::skipped(NAME, "host namespaces are not readable on this platform");
    };
    let Namespaces { net, mnt } = observed;
    Check::expect(NAME, net == host_net && mnt == host_mnt, || {
        format!("ran in {net} and {mnt}; the host is in {host_net} and {host_mnt}")
    })
}

/// Whether every drained worker id parses to an instance that the provisioner terminated.
pub fn worker_ids(events: &[Event]) -> Check {
    const NAME: &str = names::lifecycle::WORKER_IDS;
    let drained: Vec<&str> = events
        .iter()
        .filter_map(|event| match &event.change {
            EventChange::DrainStarted { worker } => Some(worker.as_str()),
            _ => None,
        })
        .collect();
    if drained.is_empty() {
        return Check::failed(NAME, "no worker was drained");
    }
    let terminated = terminated_instances(events);
    for worker in drained {
        let Some(instance) = worker_instance(worker) else {
            return Check::failed(
                NAME,
                format!("worker id `{worker}` does not parse to an instance"),
            );
        };
        if !terminated.contains(&instance) {
            return Check::failed(
                NAME,
                format!(
                    "worker `{worker}` is on instance `{}`, which was never terminated",
                    instance.as_str()
                ),
            );
        }
    }
    Check::passed(NAME)
}

/// Whether a worker started draining and every worker that started draining sits on an instance
/// the provisioner terminated.
///
/// False when no worker started draining: nothing was drained yet.
#[must_use]
pub fn every_drain_terminated(events: &[Event]) -> bool {
    let terminated = terminated_instances(events);
    let drained: Vec<&str> = events
        .iter()
        .filter_map(|event| match &event.change {
            EventChange::DrainStarted { worker } => Some(worker.as_str()),
            _ => None,
        })
        .collect();
    !drained.is_empty()
        && drained.iter().all(|worker| {
            worker_instance(worker).is_some_and(|instance| terminated.contains(&instance))
        })
}

/// The instances the provisioner terminated, in the order recorded. A reference that is not an
/// instance name is left out, so it terminates nothing.
fn terminated_instances(events: &[Event]) -> Vec<InstanceId> {
    events
        .iter()
        .filter_map(|event| match &event.change {
            EventChange::Terminated { instance } => name::parse::<name::Instances>(instance).ok(),
            _ => None,
        })
        .collect()
}

/// The instance that hosts the worker named by `reference`, or `None` when the reference or the
/// worker id does not parse.
fn worker_instance(reference: &str) -> Option<InstanceId> {
    name::parse::<name::Workers>(reference).ok()?.instance()
}

/// The cgroup limits of an action's scope: memory, out-of-memory group, CPU and process limits.
///
/// Each check compares one of the scope's limit files with the value the configured limits imply.
#[must_use]
pub fn scope_limits(facts: &ScopeFacts) -> Vec<Check> {
    let memory =
        (HOLD_MEMORY_KB * 1024 * u64::from(100 + LIMIT_HEADROOM_PERCENT) / 100).to_string();
    let cpu = format!(
        "{} {}",
        u64::from(HOLD_CPU_COUNT) * LIMIT_CPU_PERIOD_MICROS,
        LIMIT_CPU_PERIOD_MICROS
    );
    vec![
        expect_setting(names::limits::MEMORY_MAX, &memory, &facts.memory_max),
        expect_setting(names::limits::OOM_GROUP, "1", &facts.oom_group),
        expect_setting(names::limits::CPU_MAX, &cpu, &facts.cpu_max),
        expect_setting(
            names::limits::PIDS_MAX,
            &LIMIT_MAX_PIDS.to_string(),
            &facts.pids_max,
        ),
    ]
}

/// Whether a cgroup limit file holds the expected value.
fn expect_setting(name: &'static str, expected: &str, found: &str) -> Check {
    Check::expect(name, expected == found, || {
        format!("expected `{expected}`, found `{found}`")
    })
}

/// Whether an action exceeding the hard memory ceiling was killed, as Bazel reports it: `(Killed)`
/// or `(Exit 137)`. A failure quotes the last `stderr_tail_lines` lines of the build's stderr.
pub fn hard_ceiling(outcome: &BuildOutcome, stderr_tail_lines: usize) -> Check {
    Check::expect(
        names::limits::HARD_CEILING,
        !outcome.succeeded() && outcome.action_killed(),
        || {
            if outcome.succeeded() {
                "the build succeeded".to_owned()
            } else {
                format!(
                    "exit {:?}, no action was reported killed (`(Killed)` or `(Exit 137)`){}",
                    outcome.exit_code,
                    build_said(outcome, stderr_tail_lines)
                )
            }
        },
    )
}

/// The message the worker writes into a build's output when an action exceeds its disk
/// reservation.
const DISK_EXCEEDED: &str = "exceeded its disk reservation";

/// Whether the action `target` was killed for exceeding its disk reservation: the build failed,
/// and its output names `target` together with the worker's disk-reservation message. A failure
/// quotes the last `stderr_tail_lines` lines of the build's stderr.
pub fn disk_killed(outcome: &BuildOutcome, target: &str, stderr_tail_lines: usize) -> Check {
    let killed = !outcome.succeeded()
        && outcome.stderr.contains(target)
        && outcome.stderr.contains(DISK_EXCEEDED);
    Check::expect(names::disk::KILLED_WITH_MESSAGE, killed, || {
        if outcome.succeeded() {
            "the build succeeded".to_owned()
        } else {
            format!(
                "exit {:?}, the build did not report `{target}` exceeding its disk reservation; \
                 the build said:\n{}",
                outcome.exit_code,
                outcome.stderr_tail(stderr_tail_lines)
            )
        }
    })
}

/// Whether an action that exceeded its soft memory reservation was killed once and requeued on a
/// doubled reservation, and then completed: the build succeeded and the action ran exactly
/// `attempts` times, which is two when the first attempt was the kill.
pub fn soft_memory_requeue(outcome: &BuildOutcome, attempts: usize) -> Check {
    const STDERR_TAIL_LINES: usize = 30;
    Check::expect(
        names::memory::SOFT_REQUEUE,
        outcome.succeeded() && attempts == 2,
        || {
            if outcome.succeeded() {
                format!("the action ran {attempts} times, 2 expected")
            } else {
                format!(
                    "the build failed with exit {:?}; the build said:\n{}",
                    outcome.exit_code,
                    outcome.stderr_tail(STDERR_TAIL_LINES)
                )
            }
        },
    )
}

/// Whether no process left behind by the action survives the end of the action.
pub fn orphan_killed(alive: bool) -> Check {
    Check::expect(names::limits::ORPHAN_KILLED, !alive, || {
        "the process left behind by the action is still alive".to_owned()
    })
}

/// Whether the action's cgroup scope directory is gone once the action has ended.
pub fn scope_removed(exists: bool) -> Check {
    Check::expect(names::limits::SCOPE_REMOVED, !exists, || {
        "the action's scope directory still exists".to_owned()
    })
}

/// Whether an anonymous request to an admin route was admitted: a plaintext deployment binds
/// `none:anonymous` to the administrator role, so the route answers `200`.
pub fn anonymous_administrator_admitted(status: u16) -> Check {
    Check::expect(names::plaintext::ADMIN_ADMITTED, status == 200, || {
        format!("an anonymous request to an admin route answered {status}, not 200")
    })
}

/// The instances the state reports over every fleet: pending, in service and terminating.
#[must_use]
pub fn instances_reported(state: &State) -> u32 {
    state
        .fleets
        .iter()
        .map(|fleet| {
            let counts = fleet.instances;
            counts.pending + counts.in_service + counts.terminating
        })
        .sum()
}

/// Whether the state reports no instance and no worker: every fleet counts zero instances, and
/// neither a worker nor an instance without a worker is listed.
#[must_use]
pub fn fleet_is_empty(state: &State) -> bool {
    instances_reported(state) == 0 && state.workers.is_empty() && state.instances.is_empty()
}

/// What `state` reports, for the reason of a failed check.
fn reported(state: &State) -> String {
    format!(
        "{} instances counted by the fleets, {} listed without a worker, {} workers",
        instances_reported(state),
        state.instances.len(),
        state.workers.len()
    )
}

/// Whether the state reports an idle fleet before any build.
pub fn state_idle(state: &State) -> Check {
    Check::expect(names::plaintext::STATE_IDLE, fleet_is_empty(state), || {
        reported(state)
    })
}

/// Whether the state stream shows an instance and a worker once a build has scaled the fleet out:
/// some prefix of `frames`, followed as a client follows it, holds such a state.
///
/// A stream that cannot be followed fails with the reason it breaks for.
pub fn scaled_out_streamed(frames: &[Frame]) -> Check {
    const NAME: &str = names::plaintext::STATE_SCALED_OUT;
    let mut last: Option<State> = None;
    for end in 1..=frames.len() {
        let mirror = follow(&frames[..end]);
        if let Some(reason) = mirror.broken {
            return Check::failed(NAME, reason);
        }
        if let Some(state) = mirror.state {
            if instances_reported(&state) > 0 && !state.workers.is_empty() {
                return Check::passed(NAME);
            }
            last = Some(state);
        }
    }
    let reason = last.map_or_else(
        || "the stream carried no state".to_owned(),
        |state| {
            format!(
                "no frame of the stream showed an instance and a worker; the last state: {}",
                reported(&state)
            )
        },
    );
    Check::failed(NAME, reason)
}

/// Whether the state reports an empty fleet again once the workers drained.
pub fn state_drained(state: &State) -> Check {
    Check::expect(
        names::plaintext::STATE_DRAINED,
        fleet_is_empty(state),
        || reported(state),
    )
}

/// Whether the count of completed actions rose from `before` to `after`.
pub fn completed_increased(before: u64, after: u64) -> Check {
    Check::expect(
        names::plaintext::COMPLETED_INCREASES,
        after > before,
        || format!("the completed count went from {before} to {after}"),
    )
}

/// Whether the stream's first frame is the snapshot.
pub fn stream_starts_with_snapshot(first: Option<&Frame>) -> Check {
    Check::expect(
        names::stream::STARTS_WITH_SNAPSHOT,
        matches!(first, Some(Frame::Snapshot(_))),
        || {
            first.map_or_else(
                || "the stream ended without an event".to_owned(),
                |frame| format!("the first event was `{}`", frame.event_name()),
            )
        },
    )
}

/// Whether every delta of the stream applies to the state before it, as a client applies it.
pub fn stream_applies(frames: &[Frame]) -> Check {
    let mirror = follow(frames);
    Check::expect(names::stream::APPLIES, mirror.broken.is_none(), || {
        mirror.broken.unwrap_or_default()
    })
}

/// The scaling events a stream carried, oldest first: those of its snapshots and of its deltas,
/// each name once.
#[must_use]
pub fn streamed_events(frames: &[Frame]) -> Vec<&Event> {
    let mut events: Vec<&Event> = Vec::new();
    for frame in frames {
        let carried: &[Event] = match frame {
            Frame::Snapshot(state) => &state.events,
            Frame::Delta(delta) => &delta.events,
            Frame::Tick(_) | Frame::Closed(_) => &[],
        };
        for event in carried {
            if !events.iter().any(|seen| seen.name == event.name) {
                events.push(event);
            }
        }
    }
    events
}

/// Whether the scaling events of the stream carry their causes: `demand` for the scale-out,
/// `idle_timeout` for the start of a drain and `drain_finished` for a termination.
#[must_use]
pub fn stream_causes(frames: &[Frame]) -> Vec<Check> {
    let events = streamed_events(frames);
    vec![
        caused(
            &events,
            names::stream::CAUSE_DEMAND,
            &Cause::Demand,
            |change| matches!(change, EventChange::ScaledOut { .. }),
        ),
        caused(
            &events,
            names::stream::CAUSE_IDLE_TIMEOUT,
            &Cause::IdleTimeout,
            |change| matches!(change, EventChange::DrainStarted { .. }),
        ),
        caused(
            &events,
            names::stream::CAUSE_DRAIN_FINISHED,
            &Cause::DrainFinished,
            |change| matches!(change, EventChange::Terminated { .. }),
        ),
    ]
}

/// Whether one of `events` is a change of the kind `is_kind` recorded for `cause`.
fn caused(
    events: &[&Event],
    name: &'static str,
    cause: &Cause,
    is_kind: impl Fn(&EventChange) -> bool,
) -> Check {
    let holds = events
        .iter()
        .any(|event| is_kind(&event.change) && event.cause.as_ref() == Some(cause));
    Check::expect(name, holds, || {
        let described: Vec<String> = events.iter().map(|event| describe(event)).collect();
        format!("the stream carried [{}]", described.join(", "))
    })
}

/// An event as a failed check names it: its kind and its cause, and an early exit's instance and failure.
fn describe(event: &Event) -> String {
    let kind = match &event.change {
        EventChange::ScaledOut { .. } => "scaled_out",
        EventChange::DrainStarted { .. } => "drain_started",
        EventChange::DrainCancelled { .. } => "drain_cancelled",
        EventChange::Terminated { .. } => "terminated",
        EventChange::ExitedEarly { .. } => "exited_early",
        EventChange::WorkerRegistered { .. } => "worker_registered",
        EventChange::WorkerDeregistered { .. } => "worker_deregistered",
        EventChange::Unrecognized(_) => "unrecognized",
    };
    let cause = event.cause.as_ref().map_or("no cause", Cause::as_str);
    match &event.change {
        EventChange::ExitedEarly { instance, failure } => {
            format!("{kind} {instance}: {failure} ({cause})")
        }
        _ => format!("{kind} ({cause})"),
    }
}

/// Whether a request without a session cookie for `path` was refused as unauthenticated.
pub fn cookieless_refused(path: &str, status: u16) -> Check {
    Check::expect(
        names::dashboard::cookieless_refused(path),
        status == 401,
        || format!("a request for {path} without a session cookie answered {status}, not 401"),
    )
}

/// Whether the response a session cookie opened is an event stream.
pub fn stream_opened(status: u16, content_type: Option<&str>) -> Check {
    Check::expect(
        names::dashboard::STREAM_OPENED,
        status == 200 && content_type.is_some_and(|media| media.starts_with("text/event-stream")),
        || format!("the stream answered {status} with content type {content_type:?}"),
    )
}

/// Whether revoking the principal ended the open stream with a `closed` event whose reason is
/// `revoked`. `frames` are those read after the revocation, up to the bound the scenario waits
/// for, and `ended` says whether the stream ended within them.
pub fn closed_on_revocation(frames: &[Frame], ended: bool) -> Check {
    let closed = frames.iter().find_map(|frame| match frame {
        Frame::Closed(closed) => Some(&closed.reason),
        _ => None,
    });
    Check::expect(
        names::dashboard::CLOSED_ON_REVOCATION,
        closed == Some(&ClosedReason::Revoked),
        || match closed {
            Some(reason) => format!("the stream was closed with reason {reason}"),
            None => format!(
                "no closed event among the {} frames read after the revocation; the stream {}",
                frames.len(),
                if ended { "ended" } else { "stayed open" }
            ),
        },
    )
}

/// Whether the stream has shown all it can about a drain: a termination was recorded and the
/// state a client holds counts no instance and no worker, or the frames cannot be followed at
/// all, which no further frame repairs.
#[must_use]
pub fn drain_streamed(frames: &[Frame]) -> bool {
    let mirror = follow(frames);
    if mirror.broken.is_some() {
        return true;
    }
    let terminated = streamed_events(frames)
        .iter()
        .any(|event| matches!(event.change, EventChange::Terminated { .. }));
    terminated && mirror.state.as_ref().is_some_and(fleet_is_empty)
}
