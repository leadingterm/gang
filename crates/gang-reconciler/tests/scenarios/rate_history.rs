// Quantized diagnostics and exact canonical zero moments are intentional assertions.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
use std::time::Duration;

use gang_clock::Clock;
use gang_config::SourceLimit;
use gang_reconciler::{NeedCause, PassTime, PlanStep};
use gang_testkit::TestResult;
use gang_types::{ActionResources, InstanceLifecycle, SourceId};

use super::work_aware::{Workload, purchases, secs, source, summary};
use crate::support::{Harness, X86};

fn aged() -> TestResult<Workload> {
    let mut w = Workload::new(6, 16, 50)?;
    w.learn_profile(source(1)?, secs(1800))?;
    for i in 1..=6 {
        w.serve(X86, &format!("local-{i}"))?;
    }
    Ok(w)
}

#[test]
fn inflight_gauge_survives_first_completion_row_birth() -> TestResult {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 160, Some(secs(30)))?;
    w.pass();
    assert_eq!(w.h.outlook(X86)?.load, Some(0));
    w.h.advance(secs(30));
    w.h.snapshot.queued.truncate(144);
    w.complete(
        Some(source(1)?),
        X86,
        16,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    assert_eq!(
        w.h.outlook(X86)?.load,
        Some(0),
        "160 -> 144 plus 16 completions is zero arrivals"
    );
    Ok(())
}

#[test]
fn brand_new_all_subpass_source_has_positive_load() -> TestResult {
    let mut w = aged()?;
    w.pass();
    w.h.advance(secs(1));
    w.complete(
        Some(source(1)?),
        X86,
        16,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    assert!(w.h.outlook(X86)?.load.is_some_and(|load| load > 0));
    assert_eq!(w.h.outlook(X86)?.queued, 0);
    assert!(purchases(&w.h.plan.steps).is_empty());
    Ok(())
}

#[test]
fn term_first_pass_discards_preexisting_completion_rows() -> TestResult {
    let mut w = aged()?;
    w.complete(
        Some(source(1)?),
        X86,
        64,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.h.restart();
    w.pass();
    assert_eq!(w.h.outlook(X86)?.load, Some(0));
    w.h.advance(secs(5));
    w.pass();
    assert_eq!(w.h.outlook(X86)?.load, Some(0));
    w.complete(
        Some(source(1)?),
        X86,
        8,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    assert!(w.h.outlook(X86)?.load.is_some_and(|load| load > 0));
    Ok(())
}

#[test]
fn adapter_generation_reuse_preserves_known_gauge_history() -> TestResult {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 160, Some(secs(30)))?;
    w.pass();
    for generation in [7, 19] {
        w.h.advance(secs(30));
        let remaining = w.h.snapshot.queued.len() - 16;
        w.h.snapshot.queued.truncate(remaining);
        w.h.snapshot.source_rates.clear();
        w.complete(
            Some(source(1)?),
            X86,
            16,
            Some(secs(30)),
            ActionResources::default(),
        );
        w.h.snapshot.source_rates[0].generation = generation;
        w.pass();
        assert_eq!(
            w.h.outlook(X86)?.load,
            Some(0),
            "new adapter cell cannot erase nbar"
        );
    }
    Ok(())
}

#[test]
fn live_source_eviction_cold_seeds_returning_gauge() -> TestResult {
    let mut scaling = super::work_aware::settings(50)?;
    scaling.max_sources = SourceLimit::try_new(1)?;
    let mut w = Workload {
        h: Harness::with_scaling(vec![super::work_aware::fleet("gang-x86", 6, 16)?], scaling),
    };
    w.serve(X86, "local-1")?;
    for id in [1, 2, 3, 1] {
        w.h.snapshot.queued.clear();
        w.queue(Some(source(id)?), X86, 160, None)?;
        w.h.advance(secs(1));
        assert!(purchases(&w.pass()).is_empty());
        assert_eq!(w.h.outlook(X86)?.load, Some(0));
    }
    Ok(())
}

#[test]
fn new_period_cancels_old_decayed_tail_and_keeps_first_new_completion() -> TestResult {
    let mut w = aged()?;
    w.pass();
    w.h.advance(secs(1));
    w.complete(
        Some(source(1)?),
        X86,
        64,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    assert!(w.h.outlook(X86)?.load.is_some_and(|load| load > 0));
    w.h.advance(secs(1));
    w.pass();
    w.h.advance(secs(60));
    w.pass();
    assert_eq!(
        w.h.outlook(X86)?.load,
        Some(0),
        "observed quiet closes exactly, not asymptotically"
    );
    w.h.advance(secs(1));
    w.complete(
        Some(source(1)?),
        X86,
        1,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    let expected_deci = (std::f64::consts::LN_2 / 60.0 * 30.0 * 10.0).round() as u32;
    assert_eq!(w.h.outlook(X86)?.load, Some(expected_deci));
    Ok(())
}

#[test]
fn split_after_unobserved_gap_seeds_current_gauge_without_arrival_impulse() -> TestResult {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 16, None)?;
    w.pass();
    w.h.snapshot.queued.clear();
    w.h.advance(secs(1));
    w.pass();
    w.h.advance(secs(120));
    w.h.reconciler.interrupt();
    w.h.snapshot.queued.clear();
    w.queue(Some(source(1)?), X86, 160, None)?;
    w.pass();
    assert_eq!(
        w.h.outlook(X86)?.load,
        Some(0),
        "split period seeds nbar=n_current"
    );
    Ok(())
}

#[test]
fn skipped_nonzero_gauge_keeps_arrivals_across_unobserved_gap() -> TestResult {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 16, None)?;
    w.pass();
    w.h.reconciler.interrupt();
    w.h.advance(secs(120));
    w.queue(Some(source(1)?), X86, 144, None)?;
    w.pass();
    let expected = (std::f64::consts::LN_2 / 60.0 * 144.0 * 10.0 * 10.0).round() as u32;
    assert_eq!(w.h.outlook(X86)?.load, Some(expected));
    Ok(())
}

#[test]
fn quiet_closure_requires_good_passes_and_short_lull_keeps_period() -> TestResult {
    let mut w = aged()?;
    w.pass();
    w.h.advance(secs(1));
    w.complete(
        Some(source(1)?),
        X86,
        16,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    w.h.advance(secs(1));
    w.pass();
    w.h.advance(secs(59));
    w.pass();
    assert!(w.h.outlook(X86)?.load.is_some_and(|load| load > 0));
    w.h.reconciler.interrupt();
    w.h.advance(secs(100));
    w.pass();
    assert!(
        w.h.outlook(X86)?.load.is_some_and(|load| load > 0),
        "skip cannot prove observed quiet"
    );
    w.h.advance(secs(60));
    w.pass();
    assert_eq!(w.h.outlook(X86)?.load, Some(0));
    Ok(())
}

#[test]
fn equal_ticks_accept_newest_gauge_without_division() -> TestResult {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 16, None)?;
    w.pass();
    w.queue(Some(source(1)?), X86, 16, None)?;
    w.pass();
    assert!(w.h.outlook(X86)?.load.is_some_and(|load| load <= 960));
    w.h.advance(secs(60));
    w.pass();
    // nbar(60)=16/2+32/2=24, so lambda_A=kappa*8 with 10s prior.
    let expected = (std::f64::consts::LN_2 / 60.0 * 8.0 * 10.0 * 10.0).round() as u32;
    assert_eq!(w.h.outlook(X86)?.load, Some(expected));
    Ok(())
}

#[test]
fn skipped_rate_frame_spans_gap_instead_of_reseeding() -> TestResult {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 160, Some(secs(30)))?;
    w.pass();
    w.h.reconciler.interrupt();
    w.h.advance(secs(30));
    w.h.snapshot.queued.truncate(144);
    w.complete(
        Some(source(1)?),
        X86,
        16,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.pass();
    assert_eq!(w.h.outlook(X86)?.load, Some(0));
    Ok(())
}

#[test]
fn row_read_tick_not_later_pass_tick_admits_completion_during_scan() -> TestResult {
    let mut w = aged()?;
    w.h.snapshot.source_rates_at = w.h.clock.monotonic();
    let previous_read = w.h.clock.monotonic();
    w.h.advance(secs(5));
    w.h.reconcile();
    // The callback was after row copy but before the previous listing completed.
    let during_scan = previous_read + secs(2);
    w.complete(
        Some(source(1)?),
        X86,
        16,
        Some(secs(30)),
        ActionResources::default(),
    );
    w.h.snapshot.source_rates[0].created_at = during_scan;
    w.h.snapshot.source_rates[0].at = during_scan;
    w.h.advance(secs(5));
    w.pass();
    assert!(w.h.outlook(X86)?.load.is_some_and(|load| load > 0));
    Ok(())
}

#[test]
fn closed_loop_step_down_64_to_60_and_32_keeps_refilling() -> TestResult {
    for concurrency in [60, 32] {
        let mut w = aged()?;
        w.queue(Some(source(1)?), X86, 64, Some(secs(30)))?;
        w.pass();
        for _ in 0..20 {
            w.h.advance(secs(30));
            w.complete(
                Some(source(1)?),
                X86,
                64,
                Some(secs(30)),
                ActionResources::default(),
            );
            w.pass();
        }
        w.h.snapshot.queued.truncate(concurrency);
        for _ in 0..20 {
            w.h.advance(secs(30));
            w.complete(
                Some(source(1)?),
                X86,
                u32::try_from(concurrency)?,
                Some(secs(30)),
                ActionResources::default(),
            );
            w.pass();
            assert!(
                w.h.outlook(X86)?.load.is_some_and(|load| load > 0),
                "genuine refill demoted at concurrency {concurrency}"
            );
        }
    }
    Ok(())
}

#[test]
fn untimed_raw_reapi_completions_count_but_do_not_invent_service_work() -> TestResult {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 16, Some(secs(30)))?;
    w.pass();
    w.h.advance(secs(30));
    w.complete(Some(source(1)?), X86, 16, None, ActionResources::default());
    w.pass();
    assert_eq!(
        w.h.snapshot.source_rates[0]
            .timed_completions
            .get()
            .to_bits(),
        0.0_f64.to_bits()
    );
    assert_eq!(
        w.h.snapshot.source_rates[0].work.actions.get().to_bits(),
        0.0_f64.to_bits()
    );
    assert!(
        w.h.outlook(X86)?.load.is_some_and(|load| load > 0),
        "inflight duration is a fallback, not recorded actual work"
    );
    w.h.snapshot.queued.clear();
    w.h.advance(secs(30));
    w.complete(Some(source(1)?), X86, 16, None, ActionResources::default());
    w.pass();
    assert_eq!(
        w.h.snapshot.source_rates[0].work.actions.get().to_bits(),
        0.0_f64.to_bits()
    );
    w.h.advance(secs(1));
    w.pass();
    w.h.advance(secs(60));
    w.pass();
    assert_eq!(
        w.h.outlook(X86)?.load,
        Some(0),
        "untimed activity closes after observed quiet too"
    );
    Ok(())
}

#[test]
fn actual_overflow_is_charged_on_actual_fleet_including_busy_drains() -> TestResult {
    let mut w = Workload {
        h: Harness::with_scaling(
            vec![
                super::work_aware::fleet("gang-first", 6, 16)?,
                super::work_aware::fleet("gang-second", 6, 16)?,
            ],
            super::work_aware::settings(50)?,
        ),
    };
    w.learn_profile(source(1)?, secs(1800))?;
    w.serve(0, "local-1")?;
    w.serve(1, "local-2")?;
    w.running(
        "local-2",
        8,
        Some(source(1)?),
        Duration::ZERO,
        Some(secs(30)),
    )?;
    for row in &mut w.h.snapshot.executing {
        row.candidates = gang_types::FleetSet::prefix(2)?;
    }
    w.h.set_draining("local-2", true)?;
    w.pass();
    for _ in 0..20 {
        w.h.advance(secs(30));
        w.complete(
            Some(source(1)?),
            0,
            8,
            Some(secs(30)),
            ActionResources::default(),
        );
        w.pass();
    }
    // Batch receipt makes the exponential service rate exceed its long-run mean
    // immediately after a completion wave. Only actual overflow (8) is subtracted.
    let rho = w.h.snapshot.source_rates[0].work.actions.get();
    let preferred_deci = ((rho - 8.0).max(0.0) * 10.0).round() as u32;
    assert_eq!(w.h.outlook(0)?.load, Some(preferred_deci));
    assert!(w.h.outlook(1)?.load.is_some_and(|load| load >= 80));
    assert_eq!(
        w.h.outlook(1)?.utilization,
        None,
        "display occupancy excludes drains"
    );
    Ok(())
}

fn interval_trace(interval: u64) -> TestResult<(u32, gang_spi::CompletionRates)> {
    let mut w = aged()?;
    w.queue(Some(source(1)?), X86, 32, Some(secs(20)))?;
    w.pass();
    for t in 1..=600 {
        w.h.advance(secs(1));
        // A constant-concurrency closed loop: real events, regardless of sampling cadence.
        w.complete(
            Some(source(1)?),
            X86,
            2,
            Some(secs(20)),
            ActionResources::default(),
        );
        if t % interval == 0 {
            w.pass();
        }
    }
    Ok((
        w.h.outlook(X86)?.load.ok_or("missing load")?,
        w.h.snapshot.source_rates[0],
    ))
}

#[test]
fn rate_estimator_is_interval_invariant_for_realistic_refill_trace() -> TestResult {
    let (one, one_row) = interval_trace(1)?;
    let (five, five_row) = interval_trace(5)?;
    assert_eq!(one_row.completions, five_row.completions);
    assert_eq!(one_row.timed_completions, five_row.timed_completions);
    assert_eq!(one_row.work, five_row.work);
    assert!((f64::from(one) - f64::from(five)).abs() <= f64::from(one) * 0.01);
    Ok(())
}

#[test]
fn snapshot_alias_error_stays_within_documented_step_bound() -> TestResult {
    let kappa = std::f64::consts::LN_2 / 60.0;
    let trace = |interval| -> TestResult<u32> {
        let mut w = aged()?;
        w.queue(Some(source(1)?), X86, 32, Some(secs(20)))?;
        w.pass();
        for t in 1..=10 {
            w.h.advance(secs(1));
            if t == 2 {
                w.queue(Some(source(1)?), X86, 1, Some(secs(20)))?;
            }
            w.complete(
                Some(source(1)?),
                X86,
                2,
                Some(secs(20)),
                ActionResources::default(),
            );
            if t % interval == 0 {
                w.pass();
            }
        }
        w.h.outlook(X86)?
            .load
            .ok_or_else(|| "missing offered load".into())
    };
    let one = trace(1)?;
    let five = trace(5)?;
    let count_step = 1.0;
    let duration = 20.0;
    let bound_deci_slots = kappa * 5.0 * count_step * duration * 10.0 + 1.0;
    assert!((f64::from(one) - f64::from(five)).abs() <= bound_deci_slots);
    Ok(())
}

#[test]
fn first_build_checkpoint_retains_source_admission_after_model_replacement() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.queue(Some(source(1)?), X86, 40, None)?;
    w.pass();
    w.observe_for(secs(400));
    let mut records = Vec::new();
    w.h.profiles
        .snapshot(gang_reconciler::PassTime::read(&w.h.clock), &mut records)?;
    assert_eq!(records.len(), 1, "first period has no completed bucket yet");
    w.h.profiles
        .restore(gang_reconciler::PassTime::read(&w.h.clock), &records);
    w.h.restart();
    let steps = w.pass();
    assert_eq!(purchases(&steps), vec![(X86, 0, 1, NeedCause::Demand)]);
    assert_eq!(summary(&steps)?.persistent_streams, 1);
    Ok(())
}

#[test]
fn source_period_flush_settles_unobserved_checkpoint_once() -> TestResult {
    let mut w = Workload::new(6, 16, 50)?;
    w.queue(Some(source(1)?), X86, 1, None)?;
    w.pass();
    w.observe_for(secs(400));
    let mut records = Vec::new();
    w.h.profiles
        .snapshot(gang_reconciler::PassTime::read(&w.h.clock), &mut records)?;
    w.h.profiles
        .restore(gang_reconciler::PassTime::read(&w.h.clock), &records);
    w.h.snapshot.queued.clear();
    w.h.advance(secs(60));
    let mut settled = Vec::new();
    w.h.profiles
        .snapshot(gang_reconciler::PassTime::read(&w.h.clock), &mut settled)?;
    w.h.profiles
        .restore(gang_reconciler::PassTime::read(&w.h.clock), &settled);
    let mut repeated = Vec::new();
    w.h.profiles
        .snapshot(gang_reconciler::PassTime::read(&w.h.clock), &mut repeated)?;
    assert_eq!(
        settled, repeated,
        "fenced checkpoint settlement is idempotent"
    );
    w.h.restart();
    w.queue(Some(source(1)?), X86, 40, None)?;
    let steps = w.pass();
    assert_eq!(purchases(&steps), vec![(X86, 0, 1, NeedCause::Demand)]);
    assert_eq!(summary(&steps)?.persistent_streams, 1);
    Ok(())
}

fn train_long_source(src: SourceId) -> TestResult<Workload> {
    let mut w = Workload::new(6, 16, 50)?;
    w.serve(X86, "trained-worker")?;
    w.running(
        "trained-worker",
        1,
        Some(src),
        Duration::ZERO,
        Some(secs(600)),
    )?;
    w.pass();
    for period in 1..=3 {
        w.observe_for(secs(600));
        w.complete(
            Some(src),
            X86,
            1,
            Some(secs(600)),
            ActionResources::default(),
        );
        w.running(
            "trained-worker",
            u32::from(period != 3),
            Some(src),
            Duration::ZERO,
            Some(secs(600)),
        )?;
        w.pass();
    }
    let frame = w.h.snapshot.source_rates[0];
    assert_eq!(frame.source, Some(src));
    assert!(frame.timed_completions.get() > 0.0);
    assert!(
        (frame.work.actions.get() / frame.timed_completions.get() - 600.0).abs() < 1e-9,
        "real completion frames retain the long source's 600-second mean"
    );
    w.h.advance(secs(1));
    w.pass();
    w.h.advance(Duration::from(w.h.scaling.source_idle));
    w.pass();
    let mut records = Vec::new();
    w.h.profiles
        .snapshot(PassTime::read(&w.h.clock), &mut records)?;
    assert_eq!(records.len(), 1, "the 1800-second source period is durable");
    w.h.profiles.restore(PassTime::read(&w.h.clock), &records);
    w.h.disconnect("trained-worker")?;
    w.h.set_lifecycle(
        X86,
        "trained-worker",
        InstanceLifecycle::Terminating,
        Duration::ZERO,
    )?;
    w.h.set_desired_capacity(X86, 0);
    w.h.restart();
    Ok(w)
}

#[test]
fn retained_long_source_short_static_batch_buys_only_reachability_without_ratchet() -> TestResult {
    let src = source(1)?;
    let mut w = train_long_source(src)?;
    let frame = w.h.snapshot.source_rates[0];

    // Keep the authenticated principal and long mean; only this batch's known duration changes.
    w.queue(Some(src), X86, 160, Some(secs(1)))?;
    let mut steps = w.run();
    let decision = summary(&steps)?;
    assert_eq!(
        decision.persistent_streams, 1,
        "restored history must admit the short batch as persistent, not transient"
    );
    assert_eq!(decision.readiness, secs(45));
    assert_eq!(decision.horizon, Duration::from_millis(322_500));
    assert_eq!(purchases(&steps), vec![(X86, 0, 1, NeedCause::Demand)]);
    w.h.launch(
        X86,
        "new-worker",
        InstanceLifecycle::Pending,
        Duration::ZERO,
    )?;
    for delay in [8, 1, 30, 60] {
        w.h.advance(secs(delay));
        steps.extend(w.run());
        assert_eq!(w.h.snapshot.queued.len(), 160, "the batch never refills");
        let current = w.h.snapshot.source_rates[0];
        assert_eq!(current.at, frame.at, "no new completion frame");
        assert_eq!(current.timed_completions, frame.timed_completions);
        assert_eq!(current.work, frame.work, "the long source mean is retained");
        assert_eq!(w.h.outlook(X86)?.load, Some(0), "static is not sustained");
        assert_eq!(
            w.h.outlook(X86)?.needed,
            1,
            "pending capacity cannot ratchet"
        );
    }
    steps.extend(serve_finite_cohort(&mut w, src)?);
    assert_eq!(purchases(&steps), vec![(X86, 0, 1, NeedCause::Demand)]);
    assert_eq!(w.h.supply(X86)?.desired, 1);
    Ok(())
}

fn serve_finite_cohort(w: &mut Workload, src: SourceId) -> TestResult<Vec<PlanStep>> {
    let mut steps = Vec::new();
    w.h.set_lifecycle(
        X86,
        "new-worker",
        InstanceLifecycle::InService,
        Duration::ZERO,
    )?;
    w.h.connect(X86, "new-worker")?;
    w.h.snapshot.queued.truncate(144);
    w.running("new-worker", 16, Some(src), Duration::ZERO, Some(secs(1)))?;
    steps.extend(w.run());
    assert_eq!(
        w.h.outlook(X86)?.load,
        Some(160),
        "actual load fills the worker"
    );
    assert_eq!(w.h.outlook(X86)?.needed, 1);
    for remaining in [128, 112, 96] {
        w.h.advance(secs(1));
        w.complete(
            Some(src),
            X86,
            16,
            Some(secs(1)),
            ActionResources::default(),
        );
        w.h.snapshot.queued.truncate(remaining);
        w.running("new-worker", 16, Some(src), Duration::ZERO, Some(secs(1)))?;
        steps.extend(w.run());
        assert_eq!(w.h.outlook(X86)?.load, Some(160));
        assert_eq!(
            w.h.outlook(X86)?.needed,
            1,
            "serving a finite cohort cannot justify imaginary future arrivals"
        );
    }
    Ok(steps)
}
