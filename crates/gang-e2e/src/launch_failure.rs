//! A failed local launch remains visible until abandoned demand is withdrawn.

use std::fs;
use std::path::Path;
use std::time::Duration;

use gang_wire::{Event, EventChange, PassState, State};
use tokio::sync::oneshot;

use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::stack::{Stack, local_evidence};
use crate::system::{Probe, System};

const OBSERVE_TIMEOUT: Duration = Duration::from_mins(1);
const ABANDON_TIMEOUT: Duration = Duration::from_secs(20);
const WITHDRAW_TIMEOUT: Duration = Duration::from_secs(6);

// A lossy UTF-8 tail can grow when its first byte splits a character.
fn bounded(text: &str, limit: usize) -> String {
    let mut output = crate::observer::tail(text.as_bytes(), limit);
    while output.len() > limit {
        output.remove(0);
    }
    output
}

fn fleet_evidence(directory: &Path, limit: usize) -> String {
    let Ok(entries) = fs::read_dir(directory) else {
        return bounded("fleet directory unreadable", limit);
    };
    let mut paths = Vec::new();
    let mut entry_error = false;
    for entry in entries {
        match entry {
            Ok(entry) => {
                let path = entry.path();
                if path
                    .extension()
                    .is_some_and(|ext| ext == "desired" || ext == "early_exit")
                {
                    paths.push(path);
                }
            }
            Err(_) => entry_error = true,
        }
    }
    paths.sort();
    let mut lines = Vec::new();
    if entry_error {
        lines.push("fleet directory entry unreadable".to_owned());
    }
    for path in paths {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let value = match fs::read_to_string(&path) {
            Ok(text) => match text.trim().parse::<u64>() {
                Ok(value) => value.to_string(),
                Err(_) => "invalid numeric value".to_owned(),
            },
            Err(_) => "unreadable".to_owned(),
        };
        lines.push(format!("{name}: {value}"));
    }
    if lines.is_empty() {
        lines.push("no fleet records".to_owned());
    }
    bounded(&lines.join("\n"), limit)
}

struct TimeoutEvidence {
    state: String,
    fleets: String,
    launches: String,
}

fn timeout_evidence(state: Option<&State>, directory: &Path, limit: usize) -> TimeoutEvidence {
    let state = state.map_or_else(
        || "no state received".to_owned(),
        |state| {
            format!(
                "name: {:?}, seq: {}, update_time: {:?}, pass: {:?}, queue: {:?}, fleets: {:?}",
                state.name, state.seq, state.update_time, state.pass, state.queue, state.fleets
            )
        },
    );
    let launches = local_evidence(&directory.join("instances"), true, limit);
    TimeoutEvidence {
        state: bounded(&state, limit),
        fleets: fleet_evidence(&directory.join("fleets"), limit),
        launches: bounded(&launches, limit),
    }
}

fn build_evidence(joined: &std::thread::Result<Result<BuildOutcome>>, limit: usize) -> String {
    let text = match joined {
        Err(_) => "thread panicked".to_owned(),
        Ok(Ok(outcome)) => format!(
            "completed: exit_code={:?}, processes: internal={}, remote={}, remote_cache_hits={}, tls_failure={}",
            outcome.exit_code,
            outcome.processes.count("internal"),
            outcome.processes.remote(),
            outcome.processes.remote_cache_hits(),
            outcome.tls_failure()
        ),
        Ok(Err(error)) => {
            let variant = match error {
                E2eError::Io { .. } => "Io",
                E2eError::Command { .. } => "Command",
                E2eError::Parse { .. } => "Parse",
                E2eError::Template(_) => "Template",
                E2eError::Timeout { .. } => "Timeout",
                E2eError::System { context, .. } if context == "launch failure observed" => {
                    "System: observation cancellation"
                }
                E2eError::System { .. } => "System: details redacted",
                E2eError::Issuer { .. } => "Issuer",
            };
            format!("error: {variant}")
        }
    };
    bounded(&text, limit)
}

fn failed_request(state: &State) -> bool {
    state.queue.total > 0
        && state.fleets.first().is_some_and(|fleet| {
            fleet.launch_failure.is_some() && fleet.desired > fleet.instances.in_service
        })
}

/// Judges the visible failure, the pass that reported it, and its event history.
#[must_use]
pub fn failure_checks(state: &State, events: &[Event]) -> Vec<Check> {
    let fleet = state.fleets.first();
    let reason = fleet.and_then(|row| row.launch_failure.as_ref());
    let count = fleet.map_or(0, |row| {
        events
            .iter()
            .filter(|event| {
                event.fleet == row.name && matches!(event.change, EventChange::LaunchFailed { .. })
            })
            .count()
    });
    vec![
        Check::expect(
            "launch failure is visible with unmet demand",
            failed_request(state)
                && reason.is_some_and(|failure| !failure.reason.trim().is_empty()),
            || format!("state: {state:?}"),
        ),
        Check::expect("one launch failure event is recorded", count == 1, || {
            format!("recorded {count} launch failure events")
        }),
        Check::expect(
            "the pass that reports the failure ran",
            state.pass.state == PassState::Ok,
            || format!("pass: {:?}", state.pass),
        ),
    ]
}

/// Judges that an abandoned fleet request was cleared and recorded.
#[must_use]
pub fn withdrawn_checks(state: &State, events: &[Event], fleet: &str) -> Vec<Check> {
    let row = state.fleets.iter().find(|row| row.name == fleet);
    let withdrawn = events
        .iter()
        .any(|event| event.fleet == fleet && matches!(event.change, EventChange::Withdrawn { .. }));
    vec![
        Check::expect(
            "abandoned launch request is cleared",
            row.is_some_and(|row| row.desired == 0 && row.launch_failure.is_none()),
            || format!("fleet: {row:?}"),
        ),
        Check::expect("withdrawal is recorded", withdrawn, || {
            format!("no withdrawal event for {fleet}")
        }),
    ]
}

/// Runs the failure and withdrawal scenario with the real Bazel client.
pub(crate) fn launch_failure(stack: &Stack<'_>) -> Result<Vec<Check>> {
    stack.start_server()?;
    std::thread::scope(|scope| {
        let (stop, stopped) = oneshot::channel();
        let build = scope.spawn(|| stack.build_until_stopped(&[Probe::Pinned], stopped));
        let mut last_state = None;
        let failed = stack.wait_live_state(OBSERVE_TIMEOUT, |state| {
            last_state = Some(state.clone());
            failed_request(state)
        });
        let limit = stack.settings().log_tail_bytes;
        let evidence = if matches!(&failed, Err(E2eError::Timeout { .. })) {
            Some(timeout_evidence(
                last_state.as_ref(),
                &stack.placement().state_dir(),
                limit,
            ))
        } else {
            None
        };
        let history = stack.scaling_events();
        let _ = stop.send("launch failure observed".to_owned());
        let joined = build.join();
        let killed = stack.kill_bazel();
        if let Some(evidence) = evidence {
            eprintln!(
                "launch failure observation timed out\nlast state:\n{}\nfleet records:\n{}\nlaunch records:\n{}\nbuild result:\n{}",
                evidence.state,
                evidence.fleets,
                evidence.launches,
                build_evidence(&joined, limit),
            );
        }
        let joined = joined.map_err(|_| E2eError::system("the build thread panicked"));
        let _ = joined?;
        killed?;
        let failed = failed?;
        let mut checks = failure_checks(&failed, &history?);
        let fleet = failed
            .fleets
            .first()
            .ok_or_else(|| E2eError::system("the state has no fleet"))?
            .name
            .clone();
        stack.wait_live_state(ABANDON_TIMEOUT, |state| state.queue.total == 0)?;
        let cleared =
            stack.wait_live_state(WITHDRAW_TIMEOUT, |state| {
                state.fleets.iter().any(|row| {
                    row.name == fleet && row.desired == 0 && row.launch_failure.is_none()
                }) && state.events.iter().any(|event| {
                    event.fleet == fleet && matches!(event.change, EventChange::Withdrawn { .. })
                })
            })?;
        let final_history = stack.scaling_events()?;
        let failure_count = final_history
            .iter()
            .filter(|event| {
                event.fleet == fleet && matches!(event.change, EventChange::LaunchFailed { .. })
            })
            .count();
        checks.push(Check::expect(
            "launch failure is not repeated before withdrawal",
            failure_count == 1,
            || format!("recorded {failure_count} launch failure events"),
        ));
        checks.extend(withdrawn_checks(&cleared, &final_history, &fleet));
        Ok(checks)
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::time::Duration;

    use gang_wire::{Fleet, Queue, State};

    use super::{build_evidence, timeout_evidence};
    use crate::build_output::BuildOutcome;
    use crate::errors::E2eError;

    #[test]
    fn empty_evidence_describes_missing_state_and_records() {
        let root = tempfile::tempdir().expect("temporary directory");
        fs::create_dir(root.path().join("fleets")).expect("fleet directory");
        let evidence = timeout_evidence(None, root.path(), 4096);
        assert_eq!(evidence.state, "no state received");
        assert_eq!(evidence.fleets, "no fleet records");
        assert!(evidence.launches.contains("instances"));
    }

    #[test]
    fn captures_state_numeric_fleets_and_redacted_launches() {
        let root = tempfile::tempdir().expect("temporary directory");
        let fleets = root.path().join("fleets");
        let worker = root.path().join("instances/worker");
        fs::create_dir_all(&fleets).expect("fleet directory");
        fs::create_dir_all(&worker).expect("worker directory");
        fs::write(fleets.join("z.desired"), "42").expect("desired");
        fs::write(fleets.join("a.early_exit"), "123").expect("early exit");
        fs::write(fleets.join("bad.desired"), "secret-sentinel").expect("invalid marker");
        fs::write(fleets.join("x.release.json"), "secret-sentinel").expect("release");
        fs::write(
            worker.join("instance.json"),
            r#"{"pid":4242,"secret":"secret-sentinel"}"#,
        )
        .expect("instance");
        let state = State {
            queue: Queue {
                total: 3,
                ..Queue::default()
            },
            fleets: vec![Fleet {
                name: "test-fleet".to_owned(),
                desired: 42,
                ..Fleet::default()
            }],
            ..State::default()
        };
        let evidence = timeout_evidence(Some(&state), root.path(), 8192);
        assert!(evidence.state.contains("test-fleet"));
        assert!(evidence.state.contains("desired: 42"));
        assert!(evidence.fleets.contains("z.desired: 42"));
        assert!(evidence.fleets.contains("a.early_exit: 123"));
        assert!(
            evidence
                .fleets
                .contains("bad.desired: invalid numeric value")
        );
        assert!(evidence.fleets.find("a.early_exit") < evidence.fleets.find("z.desired"));
        assert!(!evidence.fleets.contains("release.json"));
        assert!(!evidence.fleets.contains("secret-sentinel"));
        assert!(evidence.launches.contains("<redacted>"));
        assert!(evidence.launches.contains("4242"));
        assert!(!evidence.launches.contains("secret-sentinel"));
    }

    #[test]
    fn large_state_name_does_not_evict_demand_snapshot() {
        let root = tempfile::tempdir().expect("temporary directory");
        let state = State {
            name: "irrelevant".repeat(1000),
            queue: Queue {
                total: 3,
                ..Queue::default()
            },
            fleets: vec![Fleet {
                name: "test-fleet".to_owned(),
                desired: 42,
                ..Fleet::default()
            }],
            ..State::default()
        };
        let evidence = timeout_evidence(Some(&state), root.path(), 4096);
        assert!(evidence.state.contains("total: 3"));
        assert!(evidence.state.contains("desired: 42"));
        assert!(evidence.state.contains("test-fleet"));
    }

    #[test]
    fn every_section_obeys_byte_budget() {
        let root = tempfile::tempdir().expect("temporary directory");
        let fleets = root.path().join("fleets");
        let worker = root.path().join("instances/worker");
        fs::create_dir_all(&fleets).expect("fleet directory");
        fs::create_dir_all(&worker).expect("worker directory");
        fs::write(fleets.join("test.desired"), "123456").expect("desired");
        fs::write(
            worker.join("instance.json"),
            r#"{"secret":"private","pid":1}"#,
        )
        .expect("instance");
        let state = State {
            name: "é".repeat(100),
            ..State::default()
        };
        let joined = Ok(Ok(BuildOutcome::new(Some(1), "failure".to_owned())));
        for limit in [0, 1, 2, 3, 16, 64] {
            let evidence = timeout_evidence(Some(&state), root.path(), limit);
            assert!(evidence.state.len() <= limit);
            assert!(evidence.fleets.len() <= limit);
            assert!(evidence.launches.len() <= limit);
            assert!(build_evidence(&joined, limit).len() <= limit);
        }
    }

    #[test]
    fn build_results_expose_only_allowlisted_information() {
        let secret = "secret-sentinel";
        let output = format!("INFO: 2 processes: 1 remote, 1 {secret}.\n{secret}");
        let joined = Ok(Ok(BuildOutcome::new(Some(1), output)));
        let text = build_evidence(&joined, 4096);
        assert!(text.contains("exit_code=Some(1)"));
        assert!(text.contains("remote=1"));
        assert!(text.contains("tls_failure=false"));
        assert!(!text.contains(secret));

        let errors = [
            E2eError::Io {
                operation: secret.to_owned(),
                source: io::Error::other(secret),
            },
            E2eError::Command {
                program: secret.to_owned(),
                outcome: secret.to_owned(),
                output: secret.to_owned(),
            },
            E2eError::parse("input")(io::Error::other(secret)),
            E2eError::Template(secret.to_owned()),
            E2eError::timeout(secret, Duration::ZERO),
            E2eError::system(secret.to_owned()),
            E2eError::issuer(secret.to_owned()),
        ];
        for (error, variant) in errors.into_iter().zip([
            "Io", "Command", "Parse", "Template", "Timeout", "System", "Issuer",
        ]) {
            let text = build_evidence(&Ok(Err(error)), 4096);
            assert!(text.contains(variant));
            assert!(!text.contains(secret));
        }
        assert!(
            build_evidence(&Ok(Err(E2eError::system("launch failure observed"))), 4096)
                .contains("observation cancellation")
        );
        assert_eq!(
            build_evidence(&Err(Box::new(secret.to_owned())), 4096),
            "thread panicked"
        );
    }
}
