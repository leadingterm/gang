//! Neighbour pressure, resource requeue, and the learned reservation on a fresh action.

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::execution::{Execution, held_build};
use crate::system::{AttemptRecord, Limits, PressureObservation, Probe};

/// The first victim attempt is killed while its neighbour remains alive.
pub const VICTIM: &str = "pressure kills only the furthest-over action";
/// The successful build retries the victim once without restarting its neighbour.
pub const REQUEUE: &str = "pressure requeue succeeds without consuming generic retries";
/// The retry reservation includes the measured peak and configured escalation.
pub const RETRY_RESERVATION: &str = "pressure retry reserves the measured escalation";
/// The next uncached action starts once with the learned peak and headroom.
pub const LEARNING: &str = "pressure peak is learned by the next action";

const VICTIM_ACTION: &str = "pressure_victim";
const NEIGHBOUR_ACTION: &str = "pressure_neighbour";
const VICTIM_KB: u64 = 64 * 1024;
const NEIGHBOUR_KB: u64 = 320 * 1024;
const CAPACITY_KB: u64 = 512 * 1024;

/// Observations used to judge both pressure and subsequent learning.
#[derive(Debug)]
pub struct Evidence<'a> {
    /// First build's outcome.
    pub build_succeeded: bool,
    /// Observed victim, neighbour, and peak, if both probes were found.
    pub observation: Option<PressureObservation>,
    /// The neighbour completed before the retry started.
    pub retry_after_neighbour: bool,
    /// Records from the first build, in start order.
    pub attempts: &'a [AttemptRecord],
    /// Next fresh build's outcome.
    pub next_succeeded: bool,
    /// Records added by the next fresh build.
    pub next_attempts: &'a [AttemptRecord],
}

fn headroom(peak: u64) -> u128 {
    (u128::from(peak) * 125).div_ceil(100)
}

/// Judges observations without assuming a successful build implies the correct victim or learning.
#[must_use]
pub fn verdicts(evidence: &Evidence<'_>) -> Vec<Check> {
    let peak_kb = evidence
        .observation
        .as_ref()
        .map_or(0, |o| o.victim_peak_kb);
    let victims: Vec<_> = evidence
        .attempts
        .iter()
        .filter(|a| a.action == VICTIM_ACTION)
        .collect();
    let neighbours: Vec<_> = evidence
        .attempts
        .iter()
        .filter(|a| a.action == NEIGHBOUR_ACTION)
        .collect();
    let next: Vec<_> = evidence
        .next_attempts
        .iter()
        .filter(|a| a.action == VICTIM_ACTION)
        .collect();
    let first = victims.first().map(|a| a.memory_kb);
    let retry = victims.get(1).map(|a| a.memory_kb);
    let neighbour = neighbours.first().map(|a| a.memory_kb);
    let required = headroom(peak_kb)
        .max((u128::from(VICTIM_KB) * 250).div_ceil(100))
        .max(u128::from(VICTIM_KB) + 1)
        .min(u128::from(CAPACITY_KB));
    let victim_ok = evidence
        .observation
        .as_ref()
        .is_some_and(|o| o.victim_killed && o.neighbour_alive)
        && peak_kb > 0;
    let requeued = evidence.build_succeeded
        && victims.len() == 2
        && neighbours.len() == 1
        && evidence.attempts.len() == 3
        && first == Some(VICTIM_KB)
        && neighbour == Some(NEIGHBOUR_KB)
        && evidence.retry_after_neighbour;
    let retry_ok = peak_kb > 0 && retry.is_some_and(|r| u128::from(r) >= required);
    let learned = evidence.next_succeeded
        && peak_kb > 0
        && next.len() == 1
        && evidence.next_attempts.len() == 1
        && next
            .first()
            .is_some_and(|r| u128::from(r.memory_kb) >= headroom(peak_kb));
    vec![
        Check::expect(VICTIM, victim_ok, || {
            format!("pressure observation: {:?}", evidence.observation)
        }),
        Check::expect(REQUEUE, requeued, || {
            format!(
                "build succeeded={}, attempts={:?}",
                evidence.build_succeeded, evidence.attempts
            )
        }),
        Check::expect(RETRY_RESERVATION, retry_ok, || {
            format!(
                "retry reservation {retry:?} KiB, required {required} KiB from peak {peak_kb} KiB"
            )
        }),
        Check::expect(LEARNING, learned, || {
            format!(
                "next build succeeded={}, attempts={:?}, learned floor={} KiB",
                evidence.next_succeeded,
                evidence.next_attempts,
                headroom(peak_kb)
            )
        }),
    ]
}

/// Runs the pressure pair and then the same target as a distinct uncached action.
///
/// # Errors
/// Returns errors from deployment operations and malformed journal records.
pub fn pressure<S: Limits, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    system.start_server()?;
    exec.set_go(false)?;
    let (build, (observation, retry_after_neighbour)) = held_build(
        system,
        exec,
        &[Probe::PressureVictim, Probe::PressureNeighbour],
        |lines| {
            let mut victim = None;
            let mut neighbour = None;
            while victim.is_none() || neighbour.is_none() {
                let Some(line) = lines.await_prefix("pressure ")? else {
                    break;
                };
                let mut words = line.split_whitespace();
                let (Some("pressure"), Some(role), Some(pid), None) =
                    (words.next(), words.next(), words.next(), words.next())
                else {
                    return Err(E2eError::system(format!(
                        "invalid pressure journal line: {line}"
                    )));
                };
                let pid = pid
                    .parse::<u32>()
                    .map_err(E2eError::system_from("parsing pressure probe pid"))?;
                match role {
                    "victim" if victim.is_none() => victim = Some(pid),
                    "neighbour" if neighbour.is_none() => neighbour = Some(pid),
                    _ => {
                        return Err(E2eError::system(format!(
                            "unexpected pressure journal line: {line}"
                        )));
                    }
                }
            }
            let observation = match (victim, neighbour) {
                (Some(victim), Some(neighbour)) => Some(system.induce_pressure(victim, neighbour)?),
                _ => None,
            };
            exec.set_go(true)?;
            let mut neighbour_done = false;
            let mut retry_after_neighbour = false;
            while let Some(line) = lines.await_prefix("pressure ")? {
                if line.starts_with("pressure neighbour_done ") {
                    neighbour_done = true;
                } else if line.starts_with("pressure victim_retry ") {
                    retry_after_neighbour = neighbour_done;
                    break;
                }
            }
            Ok((observation, retry_after_neighbour))
        },
    )?;
    let attempts = system.attempt_records()?;
    let (next_build, ()) = held_build(system, exec, &[Probe::PressureVictim], |_| Ok(()))?;
    let all_attempts = system.attempt_records()?;
    let next_attempts = all_attempts
        .strip_prefix(attempts.as_slice())
        .ok_or_else(|| E2eError::system("pressure attempt records changed between builds"))?;
    Ok(verdicts(&Evidence {
        build_succeeded: build.succeeded(),
        observation,
        retry_after_neighbour,
        attempts: &attempts,
        next_succeeded: next_build.succeeded(),
        next_attempts,
    }))
}
