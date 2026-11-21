//! Historical lifecycle integrals are observed; workloads and policy runs are explicitly modeled.

// Finite modeled exposure/rate totals are converted only for printed ratios and waterline math.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Clock;
use gang_config::ScalingConfig;
use gang_reconciler::{Action, DrainReason};
use gang_spi::ExecutingDemand;
use gang_testkit::TestResult;
use gang_types::{ActionResources, Bytes, FleetSet, InstanceLifecycle, NonZeroBytes};
use serde::Deserialize;

use super::work_aware::{Workload, fleet, source};
use crate::support::{Harness, X86};

#[derive(Deserialize)]
struct Fixtures {
    provenance: String,
    modeled_common_assumptions: String,
    episodes: Vec<Episode>,
    observed_episode_c_samples: ObservedSamples,
}

#[derive(Deserialize)]
struct ObservedSamples {
    samples: Vec<[i64; 5]>,
}

#[derive(Deserialize)]
struct Episode {
    #[serde(rename = "episode")]
    label: String,
    observed: Observed,
    modeled: Modeled,
}

#[derive(Deserialize)]
struct Observed {
    window_start_utc: String,
    window_ms: u64,
    instance_ms: u64,
    intervals: Vec<[u64; 3]>,
    audit_scale_steps_relative_ms: Vec<[u64; 3]>,
}

#[derive(Deserialize)]
struct Modeled {
    kind: String,
    concurrency: u32,
    duration_ms: u64,
    boot_delay_ms: u64,
    #[serde(default)]
    ramp_up_ms: u64,
    initial_instances: u32,
    active_ms: u64,
    cpu_millicores: u64,
    memory_bytes: u64,
    assumptions: String,
}

fn fixtures() -> TestResult<Fixtures> {
    Ok(serde_json::from_str(include_str!(
        "../fixtures/rate_episodes.json"
    ))?)
}

#[test]
fn observed_anonymized_lifecycle_integrals_reproduce_five_neutral_baselines() -> TestResult {
    let fixtures = fixtures()?;
    assert!(fixtures.provenance.contains("not a causal replay"));
    for (episode, expected) in fixtures
        .episodes
        .iter()
        .zip([7_713_725, 44_321_217, 8_634_941, 6_001_672, 73_666_154])
    {
        let integral: u64 = episode
            .observed
            .intervals
            .iter()
            .map(|[_, start, end]| {
                assert!(start <= end && *end <= episode.observed.window_ms);
                end - start
            })
            .sum();
        assert_eq!(integral, expected, "episode {}", episode.label);
        assert_eq!(integral, episode.observed.instance_ms);
        assert!(
            episode
                .observed
                .audit_scale_steps_relative_ms
                .iter()
                .all(|[at, from, to]| *at < episode.observed.window_ms && from < to)
        );
    }
    let samples = &fixtures.observed_episode_c_samples.samples;
    assert_eq!(samples.len(), 107);
    assert_eq!(samples.iter().map(|sample| sample[1]).max(), Some(21));
    assert_eq!(samples.iter().map(|sample| sample[2]).max(), Some(32));
    assert!(samples.windows(2).all(|pair| pair[0][0] < pair[1][0]));
    Ok(())
}

struct Job {
    instance: String,
    start: u64,
    end: u64,
}
struct Boot {
    instance: String,
    ready: u64,
}
struct Outcome {
    instance_ms: u64,
    peak: u32,
    late_instances: u32,
    utilization_cordons: u32,
    completed: u64,
}

fn register_ready(h: &mut Harness, boots: &mut Vec<Boot>, now: u64, delay: u64) -> TestResult {
    for boot in boots.iter().filter(|boot| boot.ready <= now) {
        h.set_lifecycle(
            X86,
            &boot.instance,
            InstanceLifecycle::InService,
            Duration::from_millis(delay),
        )?;
        h.connect(X86, &boot.instance)?;
    }
    boots.retain(|boot| boot.ready > now);
    Ok(())
}

fn dispatch(
    h: &Harness,
    jobs: &mut Vec<Job>,
    mut queued: usize,
    now: u64,
    duration: u64,
    resources: ActionResources,
) -> usize {
    // Spread dispatch. Draining workers finish existing jobs and receive no new ones.
    while queued > 0 {
        let candidate = h
            .snapshot
            .workers
            .iter()
            .filter(|worker| !worker.draining)
            .filter_map(|worker| {
                let name = worker.id.instance_name()?;
                let running = jobs.iter().filter(|job| job.instance == name).count();
                let count = running as u128 + 1;
                let policy = &h.passes[X86].policy;
                (count * u128::from(resources.cpu_millicores) <= u128::from(policy.vcpus_per_instance.get()) * 1000
                    && count * u128::from(resources.memory.get()) <= u128::from(policy.memory_per_instance.get().get())
                    && count * u128::from(resources.disk.get()) <= u128::from(policy.disk_per_instance.get().get()))
                    .then_some((running, worker.id.as_str(), name))
            })
            .min_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let Some((_, _, name)) = candidate else {
            break;
        };
        jobs.push(Job {
            instance: name.to_owned(),
            start: now,
            end: now + duration,
        });
        queued -= 1;
    }
    queued
}

fn publish_workload(
    w: &mut Workload,
    jobs: &[Job],
    queued: usize,
    now: u64,
    model: &Modeled,
    resources: ActionResources,
) -> TestResult {
    let src = Some(source(1)?);
    w.h.snapshot.executing.clear();
    for worker in &mut w.h.snapshot.workers {
        worker.running_actions = 0;
    }
    for job in jobs {
        let worker =
            w.h.snapshot
                .workers
                .iter()
                .position(|worker| worker.id.instance_name() == Some(job.instance.as_str()))
                .ok_or("running job lost its worker")?;
        w.h.snapshot.workers[worker].running_actions += 1;
        w.h.snapshot.executing.push(ExecutingDemand {
            source: src,
            candidates: FleetSet::try_from_indices([X86])?,
            resources,
            worker,
            started: w.h.clock.wall() - Duration::from_millis(now - job.start),
            expected: Some(Duration::from_millis(model.duration_ms)),
        });
    }
    w.h.snapshot.queued.clear();
    w.queue(
        src,
        X86,
        queued,
        Some(Duration::from_millis(model.duration_ms)),
    )?;
    for row in &mut w.h.snapshot.queued {
        row.resources = resources;
    }
    Ok(())
}

fn provision(
    h: &mut Harness,
    boots: &mut Vec<Boot>,
    next_instance: &mut u32,
    now: u64,
    delay: u64,
) -> TestResult {
    let live = u32::try_from(h.supply(X86)?.instances.len())?;
    let desired = h.supply(X86)?.desired;
    for _ in live..desired {
        *next_instance += 1;
        let name = format!("modeled-{next_instance}");
        h.launch(X86, &name, InstanceLifecycle::Pending, Duration::ZERO)?;
        boots.push(Boot {
            instance: name,
            ready: now + delay,
        });
    }
    Ok(())
}

fn pending_work(
    model: &Modeled,
    now: u64,
    completed: u64,
    running: usize,
    step: u64,
) -> TestResult<u64> {
    let running = u64::try_from(running)?;
    let concurrency = u64::from(model.concurrency);
    if model.kind == "burst" {
        return Ok(concurrency.saturating_sub(completed + running));
    }
    if now >= model.active_ms {
        return Ok(0);
    }
    let target = (concurrency * now.saturating_add(step).min(model.ramp_up_ms))
        .checked_div(model.ramp_up_ms)
        .unwrap_or(concurrency);
    Ok(target.saturating_sub(running))
}

fn simulate(episode: &Episode) -> TestResult<Outcome> {
    const STEP_MS: u64 = 5_000;
    let model = &episode.modeled;
    let mut policy = fleet("gang-x86", 6, 16)?;
    policy.vcpus_per_instance = NonZeroU32::new(16).ok_or("zero CPU")?;
    policy.memory_per_instance = NonZeroBytes::gib(32);
    let mut w = Workload {
        h: Harness::with_scaling(vec![policy], ScalingConfig::default()),
    };
    w.h.clock = gang_clock::TestClock::new(gang_clock::Timestamp::from_rfc3339(
        &episode.observed.window_start_utc,
    )?);
    let resources = ActionResources {
        cpu_millicores: model.cpu_millicores,
        memory: Bytes::new(model.memory_bytes),
        disk: Bytes::default(),
    };
    let src = Some(source(1)?);
    let mut next_instance = 0;
    for _ in 0..model.initial_instances {
        next_instance += 1;
        w.serve(X86, &format!("modeled-{next_instance}"))?;
    }
    let mut jobs: Vec<Job> = Vec::new();
    let mut boots: Vec<Boot> = Vec::new();
    let mut outcome = Outcome {
        instance_ms: 0,
        peak: model.initial_instances,
        late_instances: 0,
        utilization_cordons: 0,
        completed: 0,
    };
    // Fixed iteration count is a deterministic hard deadline; no wall clock or sleeps.
    for step in 0..=episode.observed.window_ms / STEP_MS {
        let now = step * STEP_MS;
        let completed = jobs.iter().filter(|job| job.end <= now).count();
        if completed > 0 {
            w.complete(
                src,
                X86,
                u32::try_from(completed)?,
                Some(Duration::from_millis(model.duration_ms)),
                resources,
            );
            outcome.completed += u64::try_from(completed)?;
            jobs.retain(|job| job.end > now);
        }
        register_ready(&mut w.h, &mut boots, now, model.boot_delay_ms)?;
        let pending_work = pending_work(model, now, outcome.completed, jobs.len(), STEP_MS)?;
        let queued = dispatch(
            &w.h,
            &mut jobs,
            usize::try_from(pending_work)?,
            now,
            model.duration_ms,
            resources,
        );
        publish_workload(&mut w, &jobs, queued, now, model, resources)?;
        let decisions = w.run();
        outcome.utilization_cordons += u32::try_from(
            decisions
                .iter()
                .filter(|step| {
                    matches!(
                        step.action,
                        Action::StartDrain {
                            reason: DrainReason::Utilization,
                            ..
                        }
                    )
                })
                .count(),
        )?;
        provision(
            &mut w.h,
            &mut boots,
            &mut next_instance,
            now,
            model.boot_delay_ms,
        )?;
        let physical = u32::try_from(w.h.supply(X86)?.instances.len())?;
        outcome.peak = outcome.peak.max(physical);
        assert!(
            physical <= 6,
            "modeled {} exceeded configured maximum",
            episode.label
        );
        if now >= 1_800_000 && now < model.active_ms {
            outcome.late_instances = physical;
        }
        if now < episode.observed.window_ms {
            outcome.instance_ms += u64::from(physical) * STEP_MS;
            w.h.advance(Duration::from_millis(STEP_MS));
        }
    }
    Ok(outcome)
}

fn modeled_episode(name: &str) -> TestResult {
    let fixtures = fixtures()?;
    let episode = fixtures
        .episodes
        .iter()
        .find(|episode| episode.label == name)
        .ok_or("unknown episode")?;
    let after = simulate(episode)?;
    assert!(after.completed > 0, "real modeled actions must finish");
    match episode.modeled.kind.as_str() {
        "burst" => {
            assert!(
                (1..=2).contains(&after.peak),
                "modeled tiny burst jumped to {}",
                after.peak
            );
            assert_eq!(after.completed, u64::from(episode.modeled.concurrency));
        }
        "spread" => {
            let cpu = f64::from(episode.modeled.concurrency) * episode.modeled.cpu_millicores as f64 / 1000.0;
            let memory = f64::from(episode.modeled.concurrency) * episode.modeled.memory_bytes as f64 / (32.0 * 1024.0 * 1024.0 * 1024.0);
            let kept = (cpu.max(memory * 16.0) / (16.0 * 0.7)).ceil() as u32;
            assert!(
                after.utilization_cordons > 0,
                "spread load must exercise busy scale-in"
            );
            assert_eq!(
                after.late_instances, kept,
                "modeled spread settles to waterline capacity"
            );
        }
        "sustained" => {
            assert_eq!(episode.modeled.concurrency, 96);
            assert_eq!(episode.modeled.duration_ms, 30_000);
            assert_eq!(after.peak, 6, "queued CPU reservations reach the fleet maximum");
            assert_eq!(after.late_instances, 6, "sustained resource demand keeps capacity");
            assert!(
                after.completed > u64::from(episode.modeled.concurrency),
                "multiple closed-loop waves must finish"
            );
        }
        _ => return Err("unknown modeled workload".into()),
    }
    println!(
        "episode={} observed_instance_ms={} modeled_after_instance_ms={} ratio={:.6} assumptions={}; {}",
        episode.label,
        episode.observed.instance_ms,
        after.instance_ms,
        after.instance_ms as f64 / episode.observed.instance_ms as f64,
        episode.modeled.assumptions,
        fixtures.modeled_common_assumptions
    );
    Ok(())
}

#[test]
fn modeled_episode_a_short_burst_avoids_observed_jump() -> TestResult {
    modeled_episode("A")
}
#[test]
fn modeled_episode_b_spread_load_releases_busy_surplus() -> TestResult {
    modeled_episode("B")
}
#[test]
fn modeled_episode_c_short_burst_is_not_a_causal_sample_replay() -> TestResult {
    modeled_episode("C")
}
#[test]
fn modeled_episode_d_short_burst_avoids_observed_jump() -> TestResult {
    modeled_episode("D")
}
#[test]
fn modeled_episode_e_sustained_refill_still_requires_queued_work_payback() -> TestResult {
    modeled_episode("E")
}
