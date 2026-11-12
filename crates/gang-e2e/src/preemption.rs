//! Worker lifecycle mid-action: a preempted spot worker stops at its notice without draining and its action
//! is requeued, and a worker renews its certificate before it expires while it keeps serving.

use std::time::SystemTime;

use crate::check::Check;
use crate::errors::Result;
use crate::execution::{Execution, Role, held_build, renewal_deadline};
use crate::names::{preemption as preemption_names, renewal};
use crate::system::{Probe, System};

#[derive(Debug, Default)]
struct Preempted {
    held: bool,
    worker_exited: bool,
    requeued: bool,
}

/// Runs a held action through spot preemption, checking that its worker exits and the action is requeued.
///
/// The Local Mode worker receives a terminating spot notice while the action is held; it must exit
/// without draining, the action must start again on another worker, and it must complete when released.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn preemption<S: System, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    system.start_server()?;
    exec.set_go(false)?;
    let (outcome, seen) = held_build(system, exec, &[Probe::HeldRelease], |lines| {
        if lines.await_prefix("held started")?.is_none() {
            return Ok(Preempted::default());
        }
        let before = exec.processes()?;
        let instance = before
            .iter()
            .find(|process| process.role == Role::GangExec)
            .and_then(|process| process.instance.clone());
        let workers: Vec<u32> = before
            .iter()
            .filter(|process| process.role == Role::Worker)
            .map(|process| process.pid)
            .collect();
        let Some(instance) = instance else {
            return Ok(Preempted::default());
        };
        exec.request_preemption(&instance)?;
        let requeued = lines.await_prefix("held started")?.is_some();
        let after = exec.processes()?;
        let worker_exited = !workers.is_empty()
            && workers
                .iter()
                .all(|pid| after.iter().all(|process| process.pid != *pid));
        Ok(Preempted {
            held: true,
            worker_exited,
            requeued,
        })
    })?;

    Ok(vec![
        Check::expect(preemption_names::HELD, seen.held, || {
            "the held action never started or ran on no Local Mode instance".to_owned()
        }),
        Check::expect(preemption_names::WORKER_EXITS, seen.worker_exited, || {
            "the preempted worker still ran when the action was requeued, or was never seen"
                .to_owned()
        }),
        Check::expect(preemption_names::REQUEUED, seen.requeued, || {
            "the action never started again after its worker was preempted".to_owned()
        }),
        Check::expect(
            preemption_names::BUILD_SUCCEEDS,
            outcome.succeeded(),
            || outcome.exit_summary(),
        ),
    ])
}

#[derive(Debug, Default)]
struct Renewal {
    first: Option<u64>,
    after: Vec<u64>,
    read_at: Option<SystemTime>,
    not_after: Option<SystemTime>,
    same_worker: bool,
}

/// Runs a held action while its worker renews its certificate.
///
/// The worker must replace its first observed identity generation before expiry, remove the previous
/// generation, remain the same process, complete the held action, and serve a subsequent action.
///
/// # Errors
///
/// Returns the errors of the system's operations; a system that misbehaves is a failed [`Check`].
pub fn worker_renewal<S: System, E: Execution>(system: &S, exec: &E) -> Result<Vec<Check>> {
    system.start_server()?;
    exec.set_go(false)?;
    let (outcome, renewal) = held_build(system, exec, &[Probe::HeldRelease], |lines| {
        if lines.await_prefix("held started")?.is_none() {
            return Ok(Renewal::default());
        }
        let before = exec.processes()?;
        let instance = before
            .iter()
            .find(|process| process.role == Role::GangExec)
            .and_then(|process| process.instance.clone());
        let workers: Vec<u32> = before
            .iter()
            .filter(|process| process.role == Role::Worker)
            .map(|process| process.pid)
            .collect();
        let renewal = match instance {
            None => Renewal::default(),
            Some(instance) => match exec.identity_generations(&instance)?.first().copied() {
                None => Renewal::default(),
                Some(first) => {
                    let not_after = exec.certificate_not_after(&instance, first)?;
                    let generations =
                        exec.generations_at(&instance, renewal_deadline(not_after))?;
                    let now = exec.processes()?;
                    let same_worker = !workers.is_empty()
                        && workers.iter().all(|pid| {
                            now.iter()
                                .any(|process| process.pid == *pid && process.role == Role::Worker)
                        });
                    Renewal {
                        first: Some(first),
                        after: generations.held,
                        read_at: Some(generations.read_at),
                        not_after: Some(not_after),
                        same_worker,
                    }
                }
            },
        };
        Ok(renewal)
    })?;
    let next = system.build(&[Probe::Unconstrained])?;
    let generations_reason = |renewal: &Renewal| {
        format!(
            "generations {:?} for generation {:?}, read at {:?}, not after {:?}",
            renewal.after, renewal.first, renewal.read_at, renewal.not_after
        )
    };

    Ok(vec![
        Check::expect(
            renewal::RENEWED,
            renewal.first.is_some_and(|first| {
                renewal.after.iter().any(|generation| *generation > first)
                    && renewal
                        .read_at
                        .zip(renewal.not_after)
                        .is_some_and(|(read_at, not_after)| read_at < not_after)
            }),
            || generations_reason(&renewal),
        ),
        Check::expect(
            renewal::PREVIOUS_REMOVED,
            renewal
                .first
                .is_some_and(|first| !renewal.after.contains(&first)),
            || generations_reason(&renewal),
        ),
        Check::expect(renewal::HELD_COMPLETES, outcome.succeeded(), || {
            outcome.exit_summary()
        }),
        Check::expect(
            renewal::SAME_WORKER,
            renewal.same_worker && next.succeeded(),
            || {
                if renewal.same_worker {
                    next.exit_summary()
                } else {
                    "the worker was replaced during the renewal".to_owned()
                }
            },
        ),
    ])
}
