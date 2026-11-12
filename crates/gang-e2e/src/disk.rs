//! The disk scenario: an action that writes past its disk reservation is killed by its worker and
//! fails with the worker's message, a neighbour that shares the worker is unaffected, and the
//! worker keeps serving.

use crate::check::Check;
use crate::errors::Result;
use crate::names;
use crate::report::DiskNeighbourReport;
use crate::system::{Probe, System};
use crate::verdict;

/// The reason a check of the disk sampler is skipped where the workers measure no disk.
const NEEDS_LINUX: &str = "the disk sampler needs Linux";

/// Runs the disk scenario against `system` and returns its checks.
///
/// The disk hog and its neighbour build in one invocation with `--keep_going`, so one worker runs
/// both: the deployment has one fleet that takes four actions per instance. The hog writes past
/// its disk reservation and must be killed by the worker, which fails it with the worker's
/// message and does not let it report. The neighbour stays far below its own reservation and
/// must finish with its file intact. Exactly one scale-out must have served both actions, and
/// it must have raised the fleet from zero to one, so one worker served both. The worker must
/// then serve a further action.
/// The scaling events are read once, after the build: the deployment is fresh, so they are
/// exactly the scale-outs the build caused.
///
/// `stderr_tail_lines` is how many lines of the hog's stderr its failed check quotes. `sampler`
/// says whether the workers sample disk use, which only Linux does. Without it the hog is not
/// built, since nothing would kill it: the scenario then builds the neighbour alone, and reports
/// the kill and the shared worker as skipped.
///
/// # Errors
///
/// Returns the errors of the system's operations. A system that misbehaves is a failed [`Check`].
pub fn disk<S: System>(system: &S, stderr_tail_lines: usize, sampler: bool) -> Result<Vec<Check>> {
    system.start_server()?;
    let mut checks = Vec::new();

    let probes: &[Probe] = if sampler {
        &[Probe::DiskHog, Probe::DiskNeighbour]
    } else {
        &[Probe::DiskNeighbour]
    };
    let outcome = system.build(probes)?;
    let events = system.scaling_events()?;

    if sampler {
        checks.push(verdict::disk_killed(
            &outcome,
            Probe::DiskHog.as_str(),
            stderr_tail_lines,
        ));
        checks.push(hog_wrote_no_report(system));
        checks.push(verdict::scale_outs(&events, names::disk::ONE_WORKER, 1));
        checks.push(verdict::first_scale_out_from_zero(&events));
    } else {
        checks.extend(Check::skipped_all(
            [
                names::disk::HOG_KILLED,
                names::disk::ONE_WORKER,
                names::scaling::FIRST_SCALE_OUT_FROM_ZERO,
            ],
            NEEDS_LINUX,
        ));
    }
    checks.push(neighbour_finished(system));

    let pinned = system.build(&[Probe::Pinned])?;
    checks.push(Check::expect(
        names::disk::SERVING_AFTER_KILL,
        pinned.succeeded(),
        || {
            format!(
                "the next action ended with exit code {:?}",
                pinned.exit_code
            )
        },
    ));

    Ok(checks)
}

/// Whether the disk hog left no report: a killed action never reaches the end of its script.
fn hog_wrote_no_report<S: System>(system: &S) -> Check {
    let reported = system.report(Probe::DiskHog).is_ok();
    Check::expect(names::disk::HOG_WROTE_NO_REPORT, !reported, || {
        "the disk hog wrote its report, so it was not killed".to_owned()
    })
}

/// Whether the neighbour wrote its whole file and read it back intact.
fn neighbour_finished<S: System>(system: &S) -> Check {
    let report = system
        .report(Probe::DiskNeighbour)
        .and_then(|report| DiskNeighbourReport::try_from(&report));
    match report {
        Ok(DiskNeighbourReport { intact: true }) => Check::passed(names::disk::NEIGHBOUR_FINISHED),
        Ok(DiskNeighbourReport { intact: false }) => Check::failed(
            names::disk::NEIGHBOUR_FINISHED,
            "its report says `neighbour = bad`",
        ),
        Err(error) => Check::failed(
            names::disk::NEIGHBOUR_FINISHED,
            format!(
                "its report cannot be read: {}",
                gang_types::error_chain(&error)
            ),
        ),
    }
}
