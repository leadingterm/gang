//! The skipped-pass scenario: a scheduling pass whose provisioner supply read fails is skipped as
//! `PROVISIONER_UNAVAILABLE` and counted, and the next good pass clears the count.

use gang_wire::{Pass, PassState, SkipReason};

use crate::check::Check;
use crate::control::{Control, passes};
use crate::errors::{E2eError, Result};
use crate::names::skipped_pass as names;

/// How many passes skipped for the provisioner the scenario watches before it lets one succeed.
const WATCHED_SKIPS: usize = 2;

/// Runs the skipped-pass scenario against a deployment with no standing instance (see
/// [`ControlKnobs::SKIPPED_PASS`](crate::ControlKnobs::SKIPPED_PASS)).
///
/// The supply is held before the server starts, so no pass succeeds before the first release.
/// After the release the supply is held again from inside the wait, as soon as the first good
/// pass is streamed, so the next pass is the first skipped one after it.
///
/// # Errors
///
/// When the deployment cannot be driven or observed.
pub fn skipped_pass<S: Control>(system: &S) -> Result<Vec<Check>> {
    let mut checks = Vec::new();
    system.hold_supply()?;
    system.start_server()?;
    match system.await_stream(&mut |frames| provisioner_skips(&passes(frames)) >= WATCHED_SKIPS) {
        Ok(frames) => {
            let observed = passes(&frames);
            checks.push(Check::passed(names::SKIPPED_FOR_PROVISIONER));
            checks.push(counted(&observed));
            checks.push(no_last_ok(&observed));
        }
        Err(error @ E2eError::Timeout { .. }) => {
            checks.push(Check::failed(
                names::SKIPPED_FOR_PROVISIONER,
                gang_types::error_chain(&error).to_string(),
            ));
            checks.extend(Check::skipped_all(
                [names::SKIPS_COUNTED, names::NO_LAST_OK_BEFORE_SUCCESS],
                "no pass was skipped for the provisioner",
            ));
        }
        Err(error) => return Err(error),
    }

    system.release_supply()?;
    let mut rehold: Option<Result<()>> = None;
    let resumed = system.await_stream(&mut |frames| {
        let observed = passes(frames);
        let Some(ok) = observed.iter().position(|pass| pass.state == PassState::Ok) else {
            return false;
        };
        let held = rehold.get_or_insert_with(|| system.hold_supply());
        held.is_err()
            || observed
                .iter()
                .skip(ok)
                .any(|pass| pass.state == PassState::Skipped)
    });
    if let Some(held) = rehold {
        held?;
    }
    match resumed {
        Ok(frames) => {
            let observed = passes(&frames);
            let mut after = observed
                .iter()
                .copied()
                .skip_while(|pass| pass.state != PassState::Ok);
            let ok = after.next();
            let skipped = after.find(|pass| pass.state == PassState::Skipped);
            checks.push(cleared(ok));
            checks.extend(after_success(skipped));
        }
        Err(error @ E2eError::Timeout { .. }) => {
            checks.push(Check::failed(
                names::OK_CLEARS_SKIP,
                gang_types::error_chain(&error).to_string(),
            ));
            checks.extend(Check::skipped_all(
                [names::SKIP_COUNT_RESETS, names::LAST_OK_KEPT],
                "no good pass followed by a skipped one was observed",
            ));
        }
        Err(error) => return Err(error),
    }
    Ok(checks)
}

/// How many of `passes` were skipped because the provisioner was unavailable.
fn provisioner_skips(passes: &[&Pass]) -> usize {
    passes
        .iter()
        .filter(|pass| {
            pass.state == PassState::Skipped
                && pass.skip_reason == Some(SkipReason::ProvisionerUnavailable)
        })
        .count()
}

/// Judges that the skipped passes count up by one each.
fn counted(passes: &[&Pass]) -> Check {
    let counts: Vec<Option<u32>> = passes
        .iter()
        .filter(|pass| pass.state == PassState::Skipped)
        .map(|pass| pass.skipped)
        .collect();
    let consecutive = counts.iter().all(Option::is_some)
        && counts.windows(2).all(|pair| {
            matches!(pair, [Some(earlier), Some(later)] if earlier.checked_add(1) == Some(*later))
        });
    Check::expect(names::SKIPS_COUNTED, consecutive, || {
        format!("the skipped passes counted {counts:?}")
    })
}

/// Judges that no pass carried `lastOkTime`, none having succeeded yet.
fn no_last_ok(passes: &[&Pass]) -> Check {
    let carried: Vec<_> = passes
        .iter()
        .filter_map(|pass| pass.last_ok_time.as_ref())
        .collect();
    Check::expect(names::NO_LAST_OK_BEFORE_SUCCESS, carried.is_empty(), || {
        format!("passes before any good one carried lastOkTime {carried:?}")
    })
}

/// Judges that the first good pass carries no skip reason and no skip count.
fn cleared(ok: Option<&Pass>) -> Check {
    match ok {
        Some(pass) => Check::expect(
            names::OK_CLEARS_SKIP,
            pass.skip_reason.is_none() && pass.skipped.is_none(),
            || {
                format!(
                    "the good pass carried skipReason {:?} and skipped {:?}",
                    pass.skip_reason, pass.skipped
                )
            },
        ),
        None => Check::failed(names::OK_CLEARS_SKIP, "no pass succeeded after the release"),
    }
}

/// Judges the first skipped pass after a good one: it counts one and carries `lastOkTime`.
fn after_success(skipped: Option<&Pass>) -> [Check; 2] {
    let Some(pass) = skipped else {
        let reason = "no pass was skipped after the good one";
        return [
            Check::failed(names::SKIP_COUNT_RESETS, reason),
            Check::failed(names::LAST_OK_KEPT, reason),
        ];
    };
    [
        Check::expect(names::SKIP_COUNT_RESETS, pass.skipped == Some(1), || {
            format!("it counted {:?}", pass.skipped)
        }),
        Check::expect(names::LAST_OK_KEPT, pass.last_ok_time.is_some(), || {
            "it carried no lastOkTime".to_owned()
        }),
    ]
}
