//! Integration tests for in-memory build summaries.

use std::cmp::Reverse;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::BuildsConfig;
use gang_insight::{BuildFilter, Builds};
use gang_spi::{Call, CallOutcome, Executed, ExecutionTimes, Observation, Origin, Phase, Rpc};
use gang_types::BuildKey;
use gang_wire::BuildState;

fn time(ms: u64) -> Timestamp {
    Timestamp::from_unix_millis(ms)
}
fn origin(principal: &str, invocation: &str) -> Result<Origin, Box<dyn std::error::Error>> {
    Ok(Origin {
        build: BuildKey {
            principal: Arc::new(format!("example-idp:{principal}").parse()?),
            invocation: invocation.parse()?,
        },
        correlated: None,
        tool: None,
        target: None,
        mnemonic: None,
    })
}
fn call(origin: &Origin, rpc: Rpc, outcome: CallOutcome) -> Observation {
    Observation::Call(Call {
        origin: origin.clone(),
        rpc,
        outcome,
    })
}
fn executed(origin: &Origin, start: u64, span: u64, exit_code: i32) -> Observation {
    let t = |offset| time(start + offset);
    Observation::Executed(Executed {
        origin: origin.clone(),
        digest: format!("{start}/{span}"),
        worker: None,
        exit_code,
        error: false,
        times: ExecutionTimes {
            queued: Some(Phase {
                start: t(0),
                end: t(4_000),
            }),
            input_fetch: Some(Phase {
                start: t(4_000),
                end: t(5_000),
            }),
            executing: Some(Phase {
                start: t(5_000),
                end: t(15_000),
            }),
            output_upload: Some(Phase {
                start: t(15_000),
                end: t(17_000),
            }),
            whole: Some(Phase {
                start: t(0),
                end: t(span),
            }),
        },
    })
}

#[test]
fn hit_and_miss_count_in_their_build() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let mut alice = origin("alice", "inv-1").unwrap();
    alice.target = Some("//pkg:target".parse().unwrap());
    builds.observe(
        time(0),
        &call(&alice, Rpc::GetActionResult, CallOutcome::Ok),
    );
    builds.observe(
        time(1),
        &call(&alice, Rpc::GetActionResult, CallOutcome::NotFound),
    );
    let row = builds.get(alice.build.id(), time(1)).unwrap();
    assert_eq!((row.usage.cache.hits, row.usage.cache.misses), (1, 1));
    assert_eq!(
        (
            row.missed_targets[0].target.as_str(),
            row.missed_targets[0].misses,
            row.missed_targets[0].overestimate
        ),
        ("//pkg:target", 1, 0)
    );
    let other = origin("alice", "inv-2").unwrap();
    builds.observe(
        time(2),
        &call(&other, Rpc::GetActionResult, CallOutcome::Ok),
    );
    assert_eq!(
        builds
            .list(&BuildFilter::default(), time(2), None, usize::MAX)
            .0
            .len(),
        2
    );
}

#[test]
fn same_invocation_other_principal_is_another_build() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let alice = origin("alice", "same").unwrap();
    let bob = origin("bob", "same").unwrap();
    builds.observe(
        time(0),
        &call(&alice, Rpc::GetActionResult, CallOutcome::Ok),
    );
    builds.observe(
        time(1),
        &call(&bob, Rpc::GetActionResult, CallOutcome::NotFound),
    );
    assert_eq!(
        builds
            .list(&BuildFilter::default(), time(1), None, usize::MAX)
            .0
            .len(),
        2
    );
    let cache = builds.get(alice.build.id(), time(1)).unwrap().usage.cache;
    assert_eq!((cache.hits, cache.misses), (1, 0));
}

#[test]
fn queued_and_executing_from_an_execution() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let o = origin("alice", "exec").unwrap();
    builds.observe(time(0), &call(&o, Rpc::Execute, CallOutcome::Ok));
    builds.observe(time(18_000), &executed(&o, 0, 18_000, 0));
    builds.observe(time(18_000), &executed(&o, 0, 18_000, 1));
    let row = builds.get(o.build.id(), time(18_000)).unwrap();
    let a = &row.slowest[0];
    assert_eq!(
        (
            a.queued,
            a.input_fetch,
            a.executing,
            a.output_upload,
            a.span
        ),
        (
            Duration::from_secs(4),
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_secs(18)
        )
    );
    assert_eq!(
        (row.usage.executions.completed, row.usage.executions.failed),
        (1, 1)
    );
    assert_eq!(row.usage.action_time.executing, Duration::from_secs(20));
}

#[test]
fn flood_keeps_the_record_bounded_and_slowest_exact() {
    let config = BuildsConfig::default();
    let mut builds = Builds::new(&config);
    let mut o = origin("alice", "flood").unwrap();
    builds.observe(time(0), &call(&o, Rpc::Execute, CallOutcome::Ok));
    let mut seed = 123_u64;
    let mut all = Vec::new();
    for i in 0..10_000_u64 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let span = seed % 100_000;
        o.target = Some(format!("//pkg:t{i}").parse().unwrap());
        o.mnemonic = Some(format!("mnemonic{i}").parse().unwrap());
        let observation = executed(&o, i, span, 0);
        if let Observation::Executed(action) = &observation {
            all.push((span, action.digest.clone()));
        }
        builds.observe(time(200_000), &observation);
        builds.observe(
            time(200_000),
            &call(&o, Rpc::GetActionResult, CallOutcome::NotFound),
        );
    }
    all.sort_by_key(|item| Reverse(item.0));
    let row = builds.get(o.build.id(), time(200_000)).unwrap();
    assert_eq!(
        row.slowest
            .iter()
            .map(|a| (u64::try_from(a.span.as_millis()).unwrap(), a.digest.clone()))
            .collect::<Vec<_>>(),
        all[..config.slowest_actions.get()]
    );
    assert!(row.mnemonics.len() <= config.mnemonics.get());
    assert!(row.missed_targets.len() <= config.targets.get());
    assert!(row.slowest.len() <= config.slowest_actions.get());
    assert!(row.timeline.unwrap().buckets.len() <= config.timeline_buckets.get());
}

#[test]
fn the_257th_build_drops_the_least_recently_active() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let keys: Vec<_> = (0..257)
        .map(|i| origin("alice", &format!("inv-{i}")).unwrap())
        .collect();
    for (i, o) in keys.iter().enumerate() {
        builds.observe(
            time(u64::try_from(i).unwrap()),
            &call(o, Rpc::Execute, CallOutcome::Ok),
        );
    }
    assert!(builds.get(keys[0].build.id(), time(300)).is_none());
    for o in &keys[1..] {
        assert!(builds.get(o.build.id(), time(300)).is_some());
    }
    builds.observe(time(301), &call(&keys[1], Rpc::Execute, CallOutcome::Ok));
    assert_eq!(
        builds
            .list(&BuildFilter::default(), time(301), None, usize::MAX)
            .0
            .len(),
        256
    );
}

#[test]
fn timeline_doubling_conserves_totals() {
    let config = BuildsConfig {
        timeline_buckets: NonZeroUsize::new(3).unwrap(),
        ..BuildsConfig::default()
    };
    let mut builds = Builds::new(&config);
    let o = origin("alice", "timeline").unwrap();
    builds.observe(time(0), &call(&o, Rpc::GetActionResult, CallOutcome::Ok));
    builds.observe(time(18_000), &executed(&o, 0, 18_000, 0));
    builds.observe(
        time(1_000_000),
        &call(&o, Rpc::GetActionResult, CallOutcome::NotFound),
    );
    let timeline = builds
        .get(o.build.id(), time(1_000_000))
        .unwrap()
        .timeline
        .unwrap();
    assert!(timeline.step > Duration::from_secs(1));
    assert!(timeline.buckets.len() <= 3);
    assert_eq!(timeline.buckets.iter().map(|b| b.hits).sum::<u64>(), 1);
    assert_eq!(timeline.buckets.iter().map(|b| b.misses).sum::<u64>(), 1);
    assert_eq!(
        timeline.buckets.iter().map(|b| b.queued).sum::<Duration>(),
        Duration::from_secs(4)
    );
    assert_eq!(
        timeline
            .buckets
            .iter()
            .map(|b| b.executing)
            .sum::<Duration>(),
        Duration::from_secs(10)
    );
}

#[test]
fn idle_after_without_observation() {
    let config = BuildsConfig::default();
    let mut builds = Builds::new(&config);
    let o = origin("alice", "idle").unwrap();
    builds.observe(time(1_000), &call(&o, Rpc::Execute, CallOutcome::Ok));
    let idle = config.idle_after.as_millis();
    assert_eq!(
        builds
            .get(o.build.id(), time(1_000 + idle - 1))
            .unwrap()
            .state,
        BuildState::Active
    );
    assert_eq!(
        builds.get(o.build.id(), time(1_000 + idle)).unwrap().state,
        BuildState::Idle
    );
}

#[test]
fn lost_observations_count_as_unrecorded() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let o = origin("alice", "lost").unwrap();
    builds.lose(time(10), &o.build, 3);
    builds.lose(time(20), &o.build, 4);
    let row = builds.get(o.build.id(), time(20)).unwrap();
    assert_eq!(
        (row.unrecorded, row.start_time, row.last_active_time),
        (7, time(10), time(20))
    );
}

#[test]
fn lost_observations_saturate_at_u64_max() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let o = origin("alice", "lost").unwrap();
    builds.lose(time(10), &o.build, u64::MAX);
    builds.lose(time(20), &o.build, u64::MAX);
    let row = builds.get(o.build.id(), time(20)).unwrap();
    assert_eq!(row.unrecorded, u64::MAX);
}

#[test]
fn mnemonic_cap_and_call_outcomes() {
    let config = BuildsConfig {
        mnemonics: NonZeroUsize::new(1).unwrap(),
        ..BuildsConfig::default()
    };
    let mut builds = Builds::new(&config);
    let mut o = origin("alice", "mnemonics").unwrap();
    o.mnemonic = Some("first".parse().unwrap());
    builds.observe(time(0), &call(&o, Rpc::Execute, CallOutcome::Ok));
    o.mnemonic = Some("second".parse().unwrap());
    builds.observe(time(1), &call(&o, Rpc::Execute, CallOutcome::Error));
    o.mnemonic = None;
    builds.observe(time(2), &call(&o, Rpc::UpdateActionResult, CallOutcome::Ok));
    let row = builds.get(o.build.id(), time(2)).unwrap();
    assert_eq!(row.mnemonics.len(), 1);
    assert_eq!(row.mnemonics[0].usage.executions.execute_calls, 1);
    let other = row.other_mnemonics.unwrap();
    assert_eq!(
        (other.executions.rejected, other.cache.uploaded_results),
        (1, 1)
    );
    assert_eq!(
        (
            row.usage.executions.execute_calls,
            row.usage.executions.rejected,
            row.usage.cache.uploaded_results
        ),
        (1, 1, 1)
    );
}

#[test]
fn recent_list_and_filter() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let a = origin("alice", "a").unwrap();
    let mut b = origin("alice", "b").unwrap();
    b.correlated = Some("group".parse().unwrap());
    builds.observe(time(1), &call(&a, Rpc::Execute, CallOutcome::Ok));
    builds.observe(time(2), &call(&b, Rpc::Execute, CallOutcome::Ok));
    let rows = builds
        .list(&BuildFilter::default(), time(2), None, usize::MAX)
        .0;
    assert_eq!(
        rows.iter()
            .map(|r| r.invocation_id.as_str())
            .collect::<Vec<_>>(),
        ["b", "a"]
    );
    let text = format!(
        "principal = \"{}\" AND invocationId = \"b\" AND correlatedInvocationsId = \"group\"",
        rows[0].principal
    );
    assert_eq!(
        builds
            .list(&text.parse().unwrap(), time(2), None, usize::MAX)
            .0
            .len(),
        1
    );
    assert_eq!(
        builds
            .list(
                &"invocationId=\"b\"".parse().unwrap(),
                time(2),
                None,
                usize::MAX
            )
            .0
            .len(),
        1
    );
    for text in [
        "bogus = \"x\"",
        "invocationId = x",
        "invocationId = \"x\" AND invocationId = \"y\"",
        "principal = \"alice\"",
    ] {
        assert!(text.parse::<BuildFilter>().is_err());
    }
    let mut recent = vec![];
    builds.fill_recent(time(2), time(2), &mut recent);
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].invocation_id, "b");
    assert!(recent[0].timeline.is_none());
    builds.fill_recent(time(0), time(2), &mut recent);
    assert_eq!(recent.len(), 2);
    assert!(recent[0].name < recent[1].name);
}

#[test]
fn list_pages_in_order_and_filters_before_limiting() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let newest = origin("bob", "newest").unwrap();
    let oldest = origin("alice", "oldest").unwrap();
    let mut tied = [
        (origin("alice", "tied-a").unwrap(), "tied-a"),
        (origin("alice", "tied-b").unwrap(), "tied-b"),
        (origin("alice", "tied-c").unwrap(), "tied-c"),
    ];
    builds.observe(time(30), &call(&newest, Rpc::Execute, CallOutcome::Ok));
    builds.observe(time(10), &call(&oldest, Rpc::Execute, CallOutcome::Ok));
    for (origin, _) in &tied {
        builds.observe(time(20), &call(origin, Rpc::Execute, CallOutcome::Ok));
    }
    tied.sort_by_key(|(origin, _)| origin.build.id());

    let filter = BuildFilter::default();
    let (first, more) = builds.list(&filter, time(30), None, 2);
    assert_eq!(
        first
            .iter()
            .map(|row| row.invocation_id.as_str())
            .collect::<Vec<_>>(),
        ["newest", tied[0].1]
    );
    assert!(more);
    let (second, more) = builds.list(&filter, time(30), Some((time(20), tied[0].0.build.id())), 2);
    assert_eq!(
        second
            .iter()
            .map(|row| row.invocation_id.as_str())
            .collect::<Vec<_>>(),
        [tied[1].1, tied[2].1]
    );
    assert!(more);
    let (third, more) = builds.list(&filter, time(30), Some((time(20), tied[2].0.build.id())), 2);
    assert_eq!(
        third
            .iter()
            .map(|row| row.invocation_id.as_str())
            .collect::<Vec<_>>(),
        ["oldest"]
    );
    assert!(!more);
    let (empty, more) = builds.list(&filter, time(30), Some((time(10), oldest.build.id())), 2);
    assert!(empty.is_empty());
    assert!(!more);

    let alice: BuildFilter = "principal=\"principals/example-idp%3aalice\""
        .parse()
        .unwrap();
    let (filtered, more) = builds.list(&alice, time(30), None, 2);
    assert_eq!(
        filtered
            .iter()
            .map(|row| row.invocation_id.as_str())
            .collect::<Vec<_>>(),
        [tied[0].1, tied[1].1]
    );
    assert!(more);
    let (remaining, more) =
        builds.list(&alice, time(30), Some((time(20), tied[1].0.build.id())), 2);
    assert_eq!(
        remaining
            .iter()
            .map(|row| row.invocation_id.as_str())
            .collect::<Vec<_>>(),
        [tied[2].1, "oldest"]
    );
    assert!(!more);
    let missing: BuildFilter = "invocationId=\"missing\"".parse().unwrap();
    let (empty, more) = builds.list(&missing, time(30), None, 2);
    assert!(empty.is_empty());
    assert!(!more);
    let (empty, more) = builds.list(&filter, time(30), None, 0);
    assert!(empty.is_empty());
    assert!(more);
    let (empty, more) = builds.list(&missing, time(30), None, 0);
    assert!(empty.is_empty());
    assert!(!more);
}

#[test]
fn absent_execution_phases_do_not_invent_time_or_slowest() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let o = origin("alice", "partial").unwrap();
    let observation = Observation::Executed(Executed {
        origin: o.clone(),
        digest: "partial/1".to_owned(),
        worker: None,
        exit_code: 0,
        error: false,
        times: ExecutionTimes {
            queued: None,
            input_fetch: None,
            executing: Some(Phase {
                start: time(1_000),
                end: time(2_000),
            }),
            output_upload: None,
            whole: None,
        },
    });
    builds.observe(time(0), &call(&o, Rpc::Execute, CallOutcome::Ok));
    builds.observe(time(2_000), &observation);
    let row = builds.get(o.build.id(), time(2_000)).unwrap();
    assert_eq!(row.usage.executions.completed, 1);
    assert_eq!(row.usage.action_time.queued, Duration::ZERO);
    assert_eq!(row.usage.action_time.input_fetch, Duration::ZERO);
    assert_eq!(row.usage.action_time.executing, Duration::from_secs(1));
    assert_eq!(row.usage.action_time.output_upload, Duration::ZERO);
    assert!(row.slowest.is_empty());
    let timeline = row.timeline.unwrap();
    assert_eq!(
        timeline
            .buckets
            .iter()
            .map(|b| b.executing)
            .sum::<Duration>(),
        Duration::from_secs(1)
    );
}

#[test]
fn missed_targets_replace_the_earliest_minimum() {
    let config = BuildsConfig {
        targets: NonZeroUsize::new(2).unwrap(),
        ..BuildsConfig::default()
    };
    let mut builds = Builds::new(&config);
    let mut o = origin("alice", "misses").unwrap();
    for target in ["a", "b", "c", "b", "d"] {
        o.target = Some(target.parse().unwrap());
        builds.observe(
            time(0),
            &call(&o, Rpc::GetActionResult, CallOutcome::NotFound),
        );
    }
    let row = builds.get(o.build.id(), time(0)).unwrap();
    assert_eq!(
        row.missed_targets
            .iter()
            .map(|t| (t.target.as_str(), t.misses, t.overestimate))
            .collect::<Vec<_>>(),
        [("d", 3, 2), ("c", 2, 1)]
    );
}

#[test]
fn filter_normalizes_principals_and_validates_invocations() {
    let mut builds = Builds::new(&BuildsConfig::default());
    let o = origin("alice", "build").unwrap();
    builds.observe(time(0), &call(&o, Rpc::Execute, CallOutcome::Ok));
    let filter: BuildFilter =
        "  principal=\"principals/example-idp%3aalice\" AND   invocationId=\"build\""
            .parse()
            .unwrap();
    assert_eq!(builds.list(&filter, time(0), None, usize::MAX).0.len(), 1);
    for field in ["invocationId", "correlatedInvocationsId"] {
        for value in [String::new(), "invalid\nid".to_owned(), "x".repeat(65)] {
            assert!(
                format!("{field}=\"{value}\"")
                    .parse::<BuildFilter>()
                    .is_err()
            );
        }
    }
}
