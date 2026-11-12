//! Behaviour of the pure Gate 1 verdicts over in-memory observations.

use std::path::{Path, PathBuf};

use gang_clock::Timestamp;
use gang_e2e::verdict;
use gang_e2e::{
    BuildOutcome, Check, HOLD_CPU_COUNT, HOLD_MEMORY_KB, Host, InputLinks, IsolationReport,
    LIMIT_CPU_PERIOD_MICROS, LIMIT_HEADROOM_PERCENT, LIMIT_MAX_PIDS, Namespaces, Outcome,
    PermitNetwork, PoisonReport, Sandbox, ScopeFacts, VictimReport, names,
};
use gang_types::{FleetName, InstanceId, WorkerId};
use gang_wire::{Event, EventChange, name};

const WORKER: &str = "local-1-1f1d946a-7368-6397-b958-0648f8aa0688";
const HOST_NET: &str = "net:[4026531992]";
const HOST_MNT: &str = "mnt:[4026531840]";
const OTHER_NET: &str = "net:[4026533000]";
const OTHER_MNT: &str = "mnt:[4026533001]";

/// The stderr tail length the hard-ceiling checks quote on failure: the settings default.
const TAIL: usize = 30;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn namespaces(net: &str, mnt: &str) -> Namespaces {
    Namespaces {
        net: net.to_owned(),
        mnt: mnt.to_owned(),
    }
}

/// The entries of a comma-terminated listing, as a probe prints them.
fn listing(text: &str) -> Vec<String> {
    text.split(',')
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect()
}

/// What the isolation probe reports for the action `work` under the parent `sandbox` in the
/// grandparent `work`, with the given listings of the parent, grandparent and great-grandparent
/// directories.
fn layout(parent: &str, grandparent: &str, great_grandparent: &str) -> IsolationReport {
    IsolationReport {
        namespaces: namespaces(OTHER_NET, OTHER_MNT),
        cwd: "/tmp/gang-e2e-x/work".to_owned(),
        cwd_name: "work".to_owned(),
        parent_name: "sandbox".to_owned(),
        parent_entries: listing(parent),
        grandparent_name: "work".to_owned(),
        grandparent_entries: listing(grandparent),
        great_grandparent_entries: listing(great_grandparent),
        cwd_writable: true,
    }
}

/// What the isolation probe reports for an action working in `cwd`.
fn working_in(cwd: &str, writable: bool) -> IsolationReport {
    IsolationReport {
        cwd: cwd.to_owned(),
        cwd_writable: writable,
        ..layout("work,", "sandbox,", "work,")
    }
}

fn host() -> Host {
    Host {
        net: Some(HOST_NET.to_owned()),
        mnt: Some(HOST_MNT.to_owned()),
    }
}

fn build(code: Option<i32>, stderr: &str) -> BuildOutcome {
    BuildOutcome::new(code, stderr.to_owned())
}

fn failure(check: &Check) -> &str {
    assert!(
        check.is_failure(),
        "expected a failure, got {:?}",
        check.outcome
    );
    match &check.outcome {
        Outcome::Failed(text) => text.as_str(),
        Outcome::Passed | Outcome::Skipped(_) => "",
    }
}

fn event(change: EventChange) -> Result<Event, Box<dyn std::error::Error>> {
    fleet_event("local-worker-x86-64", change)
}

fn fleet_event(fleet: &str, change: EventChange) -> Result<Event, Box<dyn std::error::Error>> {
    let fleet: FleetName = fleet.parse()?;
    Ok(Event {
        name: name::of::<name::Events>(&1_u64),
        create_time: Timestamp::from_unix_millis(0),
        fleet: name::of::<name::Fleets>(&fleet),
        cause: None,
        change,
    })
}

fn worker(id: &str) -> Result<String, Box<dyn std::error::Error>> {
    let id: WorkerId = id.parse()?;
    Ok(name::of::<name::Workers>(&id))
}

fn instance(id: &str) -> Result<String, Box<dyn std::error::Error>> {
    let id: InstanceId = id.parse()?;
    Ok(name::of::<name::Instances>(&id))
}

#[test]
fn noop_namespaces_are_skipped() {
    let checks = verdict::namespaces(Sandbox::Noop, &host(), &namespaces(HOST_NET, HOST_MNT));
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].name, names::isolation::NET_AND_MNT_NAMESPACES);
    assert!(checks[0].is_skipped());
}

#[test]
fn bwrap_namespaces_equal_to_the_host_fail() {
    let isolated = namespaces(HOST_NET, HOST_MNT);
    let checks = verdict::namespaces(Sandbox::Bwrap, &host(), &isolated);
    assert_eq!(checks.len(), 2);
    assert!(checks.iter().all(Check::is_failure));
}

#[test]
fn bwrap_namespaces_different_from_the_host_pass() {
    let isolated = namespaces(OTHER_NET, OTHER_MNT);
    let checks = verdict::namespaces(Sandbox::Bwrap, &host(), &isolated);
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].name, names::isolation::NET_NAMESPACE);
    assert_eq!(checks[1].name, names::isolation::MNT_NAMESPACE);
    assert!(checks.iter().all(Check::is_passed));
}

#[test]
fn bwrap_namespaces_skip_when_the_host_is_unreadable() {
    let unreadable = Host {
        net: None,
        mnt: Some(HOST_MNT.to_owned()),
    };
    let isolated = namespaces(OTHER_NET, OTHER_MNT);
    let checks = verdict::namespaces(Sandbox::Bwrap, &unreadable, &isolated);
    assert_eq!(checks.len(), 2);
    assert!(checks.iter().all(Check::is_skipped));
}

#[test]
fn bwrap_namespaces_unavailable_net_fails() {
    let isolated = namespaces("unavailable", OTHER_MNT);
    let checks = verdict::namespaces(Sandbox::Bwrap, &host(), &isolated);
    assert!(checks[0].is_failure());
}

#[test]
fn bwrap_namespaces_empty_mnt_fails() {
    let isolated = namespaces(OTHER_NET, "");
    let checks = verdict::namespaces(Sandbox::Bwrap, &host(), &isolated);
    assert!(checks[1].is_failure());
}

#[test]
fn siblings_hidden_passes_when_each_directory_lists_only_its_child_under_bwrap() {
    let check = verdict::siblings_hidden(Sandbox::Bwrap, &layout("work,", "sandbox,", "work,"));
    assert_eq!(check.name, names::isolation::SIBLINGS_HIDDEN);
    assert!(check.is_passed());
}

#[test]
fn siblings_hidden_fails_when_the_parent_lists_a_sibling_under_bwrap() {
    let check = verdict::siblings_hidden(Sandbox::Bwrap, &layout("cas,work,", "sandbox,", "work,"));
    assert!(check.is_failure());
    assert!(failure(&check).contains("parent directory"));
    assert!(failure(&check).contains("cas"));
}

#[test]
fn siblings_hidden_fails_when_the_grandparent_lists_a_sibling_under_bwrap() {
    let check = verdict::siblings_hidden(Sandbox::Bwrap, &layout("work,", "cas,sandbox,", "work,"));
    assert!(check.is_failure());
    assert!(failure(&check).contains("grandparent directory"));
    assert!(failure(&check).contains("cas"));
}

#[test]
fn siblings_hidden_fails_when_the_great_grandparent_lists_a_sibling_under_bwrap() {
    let check = verdict::siblings_hidden(
        Sandbox::Bwrap,
        &layout("work,", "sandbox,", "work,fast-cas,"),
    );
    assert!(check.is_failure());
    assert!(failure(&check).contains("great-grandparent directory"));
    assert!(failure(&check).contains("fast-cas"));
}

#[test]
fn siblings_hidden_passes_as_the_noop_control_with_a_sibling_at_the_great_grandparent() {
    let check = verdict::siblings_hidden(
        Sandbox::Noop,
        &layout("work,", "sandbox,", "work,fast-cas,bin,"),
    );
    assert_eq!(check.name, names::isolation::SIBLINGS_LISTED);
    assert!(check.is_passed());
}

#[test]
fn siblings_hidden_passes_as_the_noop_control_in_the_reproduced_layout() {
    let reproduced = IsolationReport {
        parent_name: "0a1b2c".to_owned(),
        ..layout("tmp,work,", "0a1b2c,", "work,fast-cas,")
    };
    let check = verdict::siblings_hidden(Sandbox::Noop, &reproduced);
    assert!(check.is_passed());
}

#[test]
fn siblings_hidden_fails_as_the_noop_control_without_a_sibling_at_the_great_grandparent() {
    let check = verdict::siblings_hidden(Sandbox::Noop, &layout("work,", "sandbox,", "work,"));
    assert!(check.is_failure());
    assert!(failure(&check).contains("great-grandparent"));
}

#[test]
fn working_directory_writable_under_tmp_passes_both_checks() {
    let isolated = working_in("/tmp/gang-e2e-x/work", true);
    let checks = verdict::working_directory(Path::new("/tmp"), &isolated);
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].name, names::isolation::WORKING_DIRECTORY_WRITABLE);
    assert_eq!(checks[1].name, names::isolation::WORKING_DIRECTORY_VISIBLE);
    assert!(checks.iter().all(Check::is_passed));
}

#[test]
fn working_directory_unwritable_under_tmp_fails_both_checks() {
    let isolated = working_in("/tmp/gang-e2e-x/work", false);
    let checks = verdict::working_directory(Path::new("/tmp"), &isolated);
    assert_eq!(checks.len(), 2);
    assert!(checks.iter().all(Check::is_failure));
}

#[test]
fn working_directory_outside_tmp_skips_the_visibility_check() {
    let isolated = working_in("/var/work", true);
    let checks = verdict::working_directory(Path::new("/tmp"), &isolated);
    assert_eq!(checks.len(), 2);
    assert!(checks[0].is_passed());
    assert!(checks[1].is_skipped());
}

fn network_checks(permit: PermitNetwork, requested_net: &str) -> Vec<Check> {
    let isolated = namespaces(OTHER_NET, OTHER_MNT);
    let requested = namespaces(requested_net, OTHER_MNT);
    verdict::network(Sandbox::Bwrap, permit, &host(), &isolated, &requested)
}

#[test]
fn network_denied_and_requested_on_own_namespace_passes() {
    let checks = network_checks(PermitNetwork::Denied, OTHER_NET);
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].name, names::network::OWN_NAMESPACE_BY_DEFAULT);
    assert_eq!(checks[1].name, names::network::REQUEST_NOT_PERMITTED);
    assert!(checks.iter().all(Check::is_passed));
}

#[test]
fn network_denied_but_requested_on_host_namespace_fails() {
    let checks = network_checks(PermitNetwork::Denied, HOST_NET);
    assert_eq!(checks.len(), 2);
    assert!(checks[0].is_passed());
    assert!(checks[1].is_failure());
}

#[test]
fn network_permitted_and_requested_on_host_namespace_passes() {
    let checks = network_checks(PermitNetwork::Permitted, HOST_NET);
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[1].name, names::network::REQUEST_SHARED);
    assert!(checks.iter().all(Check::is_passed));
}

#[test]
fn network_permitted_but_requested_on_own_namespace_fails() {
    let checks = network_checks(PermitNetwork::Permitted, OTHER_NET);
    assert_eq!(checks.len(), 2);
    assert!(checks[0].is_passed());
    assert!(checks[1].is_failure());
}

#[test]
fn network_action_sharing_the_host_namespace_fails_the_isolation_check() {
    let isolated = namespaces(HOST_NET, OTHER_MNT);
    let requested = namespaces(OTHER_NET, OTHER_MNT);
    let checks = verdict::network(
        Sandbox::Bwrap,
        PermitNetwork::Denied,
        &host(),
        &isolated,
        &requested,
    );
    assert!(checks[0].is_failure());
}

#[test]
fn network_skips_under_noop() {
    let isolated = namespaces(OTHER_NET, OTHER_MNT);
    let requested = namespaces(OTHER_NET, OTHER_MNT);
    let checks = verdict::network(
        Sandbox::Noop,
        PermitNetwork::Denied,
        &host(),
        &isolated,
        &requested,
    );
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].name, names::network::SHARING);
    assert!(checks[0].is_skipped());
}

#[test]
fn network_skips_when_the_host_net_is_unreadable() {
    let unreadable = Host {
        net: None,
        mnt: Some(HOST_MNT.to_owned()),
    };
    let isolated = namespaces(OTHER_NET, OTHER_MNT);
    let requested = namespaces(OTHER_NET, OTHER_MNT);
    let checks = verdict::network(
        Sandbox::Bwrap,
        PermitNetwork::Denied,
        &unreadable,
        &isolated,
        &requested,
    );
    assert_eq!(checks.len(), 1);
    assert!(checks[0].is_skipped());
}

#[test]
fn network_unavailable_isolated_namespace_fails_the_shape_check() {
    let isolated = namespaces("unavailable", OTHER_MNT);
    let requested = namespaces(OTHER_NET, OTHER_MNT);
    let checks = verdict::network(
        Sandbox::Bwrap,
        PermitNetwork::Denied,
        &host(),
        &isolated,
        &requested,
    );
    assert!(failure(&checks[0]).contains("not a net namespace id"));
}

#[test]
fn network_empty_requested_namespace_fails_when_denied() {
    let checks = network_checks(PermitNetwork::Denied, "");
    assert!(checks[1].is_failure());
}

#[test]
fn exit_status_with_the_expected_code_passes() {
    let outcome = build(Some(1), "ERROR: action failed (Exit 3)\n");
    let check = verdict::exit_status(&outcome, 3);
    assert_eq!(check.name, names::build::exit_status(3));
    assert!(check.is_passed());
}

#[test]
fn exit_status_does_not_match_a_longer_code() {
    let outcome = build(Some(1), "ERROR: action failed (Exit 30)\n");
    assert!(verdict::exit_status(&outcome, 3).is_failure());
}

#[test]
fn exit_status_fails_when_the_build_succeeded() {
    let outcome = build(Some(0), "INFO: Build completed successfully\n");
    let check = verdict::exit_status(&outcome, 3);
    assert_eq!(failure(&check), "the build succeeded");
}

#[test]
fn remote_execution_passes_when_every_action_ran_remotely() {
    let outcome = build(Some(0), "INFO: 2 processes: 2 remote.\n");
    let check = verdict::remote_execution(&outcome, 2, 5);
    assert_eq!(check.name, names::build::REMOTE_EXECUTION);
    assert!(check.is_passed());
}

#[test]
fn remote_execution_fails_when_too_few_ran_remotely() {
    let outcome = build(Some(0), "INFO: 2 processes: 2 remote.\n");
    assert!(verdict::remote_execution(&outcome, 3, 5).is_failure());
}

#[test]
fn remote_execution_fails_when_the_build_failed() {
    let outcome = build(Some(1), "INFO: 2 processes: 2 remote.\n");
    assert!(verdict::remote_execution(&outcome, 2, 5).is_failure());
}

#[test]
fn cache_hit_passes_when_every_action_is_a_remote_cache_hit() {
    let outcome = build(Some(0), "INFO: 3 processes: 3 remote cache hit.\n");
    let check = verdict::cache_hit(&outcome, 3, 5);
    assert_eq!(check.name, names::build::CACHE_HIT);
    assert!(check.is_passed());
}

#[test]
fn cache_hit_fails_when_an_action_ran_remotely() {
    let outcome = build(
        Some(0),
        "INFO: 3 processes: 2 remote cache hit, 1 remote.\n",
    );
    assert!(verdict::cache_hit(&outcome, 3, 5).is_failure());
}

#[test]
fn cache_hit_fails_when_an_action_missed_the_cache() {
    let outcome = build(
        Some(0),
        "INFO: 3 processes: 2 remote cache hit, 1 internal.\n",
    );
    assert!(verdict::cache_hit(&outcome, 3, 5).is_failure());
}

#[test]
fn cache_hit_fails_when_nothing_is_reported() {
    let outcome = build(Some(0), "INFO: 0 processes.\n");
    assert!(verdict::cache_hit(&outcome, 1, 5).is_failure());
}

#[test]
fn remote_execution_failure_detail_includes_the_build_output() {
    let outcome = build(
        Some(2),
        "ERROR: Cannot combine gRPC based remote execution with HTTP-based caching\n",
    );
    let check = verdict::remote_execution(&outcome, 1, 5);
    assert!(check.is_failure());
    assert!(failure(&check).contains("the build said:"));
    assert!(
        failure(&check)
            .contains("ERROR: Cannot combine gRPC based remote execution with HTTP-based caching")
    );
}

#[test]
fn cache_hit_failure_detail_includes_the_build_output() {
    let outcome = build(
        Some(2),
        "ERROR: Cannot combine gRPC based remote execution with HTTP-based caching\n",
    );
    let check = verdict::cache_hit(&outcome, 1, 5);
    assert!(check.is_failure());
    assert!(failure(&check).contains("the build said:"));
    assert!(
        failure(&check)
            .contains("ERROR: Cannot combine gRPC based remote execution with HTTP-based caching")
    );
}

#[test]
fn remote_execution_too_few_on_a_successful_build_omits_the_build_output() {
    let outcome = build(Some(0), "INFO: 2 processes: 2 remote.\n");
    let check = verdict::remote_execution(&outcome, 3, 5);
    assert!(failure(&check).contains("3 expected"));
    assert!(!failure(&check).contains("the build said"));
}

#[test]
fn remote_execution_failure_quotes_only_the_last_lines_of_the_build_output() {
    let outcome = build(
        Some(2),
        "line one\nline two\nline three\nline four\nline five\nline six\n",
    );
    let check = verdict::remote_execution(&outcome, 1, 2);
    let detail = failure(&check);
    assert!(detail.ends_with("the build said:\nline five\nline six"));
    assert!(!detail.contains("line four"));
}

#[test]
fn input_integrity_break_passes_when_the_later_action_sees_no_change() {
    let check = verdict::input_integrity(InputLinks::Break, &VictimReport { poisoned: false });
    assert_eq!(check.name, names::integrity::NO_ALTERATION);
    assert!(check.is_passed());
}

#[test]
fn input_integrity_break_fails_when_the_later_action_sees_the_change() {
    let check = verdict::input_integrity(InputLinks::Break, &VictimReport { poisoned: true });
    assert!(check.is_failure());
}

#[test]
fn input_integrity_keep_passes_when_the_control_is_poisoned() {
    let check = verdict::input_integrity(InputLinks::Keep, &VictimReport { poisoned: true });
    assert_eq!(check.name, names::integrity::KEEP_CONTROL);
    assert!(check.is_passed());
}

#[test]
fn input_integrity_keep_fails_when_the_control_is_not_poisoned() {
    let check = verdict::input_integrity(InputLinks::Keep, &VictimReport { poisoned: false });
    assert!(check.is_failure());
}

fn fleet_events() -> Result<Vec<Event>, Box<dyn std::error::Error>> {
    Ok(vec![
        event(EventChange::ScaledOut { from: 0, to: 1 })?,
        event(EventChange::ScaledOut { from: 1, to: 2 })?,
        event(EventChange::DrainStarted {
            worker: worker(WORKER)?,
        })?,
        event(EventChange::Terminated {
            instance: instance("local-1")?,
        })?,
    ])
}

#[test]
fn scale_outs_passes_on_the_expected_count() -> TestResult {
    let check = verdict::scale_outs(&fleet_events()?, "two scale-outs", 2);
    assert_eq!(check.name, "two scale-outs");
    assert!(check.is_passed());
    Ok(())
}

#[test]
fn scale_outs_fails_and_shows_the_count_otherwise() -> TestResult {
    let check = verdict::scale_outs(&fleet_events()?, "one scale-out", 1);
    assert!(failure(&check).contains("2 scale-outs recorded, 1 expected"));
    Ok(())
}

#[test]
fn first_scale_out_from_zero_passes_when_it_starts_at_zero() -> TestResult {
    let check = verdict::first_scale_out_from_zero(&fleet_events()?);
    assert!(check.is_passed());
    Ok(())
}

#[test]
fn first_scale_out_from_zero_fails_when_the_fleet_is_not_empty() -> TestResult {
    let events = [event(EventChange::ScaledOut { from: 1, to: 2 })?];
    let check = verdict::first_scale_out_from_zero(&events);
    assert!(check.is_failure());
    Ok(())
}

#[test]
fn first_scale_out_from_zero_fails_when_it_raises_the_fleet_to_two() -> TestResult {
    let events = [event(EventChange::ScaledOut { from: 0, to: 2 })?];
    let check = verdict::first_scale_out_from_zero(&events);
    assert_eq!(failure(&check), "the first scale-out went from 0 to 2");
    Ok(())
}

#[test]
fn first_scale_out_from_zero_fails_without_a_scale_out() {
    let check = verdict::first_scale_out_from_zero(&[]);
    assert_eq!(failure(&check), "no scale-out was recorded");
}

#[test]
fn worker_ids_passes_when_the_drained_worker_maps_to_a_terminated_instance() -> TestResult {
    let check = verdict::worker_ids(&fleet_events()?);
    assert_eq!(check.name, names::lifecycle::WORKER_IDS);
    assert!(check.is_passed());
    Ok(())
}

#[test]
fn worker_ids_fails_when_no_worker_was_drained() -> TestResult {
    let events = [event(EventChange::Terminated {
        instance: instance("local-1")?,
    })?];
    let check = verdict::worker_ids(&events);
    assert_eq!(failure(&check), "no worker was drained");
    Ok(())
}

#[test]
fn worker_ids_fails_for_a_worker_id_without_a_uuid() -> TestResult {
    let events = [
        event(EventChange::DrainStarted {
            worker: worker("local-1")?,
        })?,
        event(EventChange::Terminated {
            instance: instance("local-1")?,
        })?,
    ];
    let check = verdict::worker_ids(&events);
    assert!(failure(&check).contains("local-1"));
    Ok(())
}

#[test]
fn worker_ids_fails_when_the_instance_was_not_terminated() -> TestResult {
    let events = [
        event(EventChange::DrainStarted {
            worker: worker(WORKER)?,
        })?,
        event(EventChange::Terminated {
            instance: instance("local-2")?,
        })?,
    ];
    let check = verdict::worker_ids(&events);
    assert!(failure(&check).contains("local-1"));
    Ok(())
}

fn limited_scope() -> ScopeFacts {
    ScopeFacts {
        directory: PathBuf::from("/sys/fs/cgroup/action"),
        working_directory: PathBuf::from("/sim/work"),
        memory_max: (HOLD_MEMORY_KB * 1024 * u64::from(100 + LIMIT_HEADROOM_PERCENT) / 100)
            .to_string(),
        oom_group: "1".to_owned(),
        cpu_max: format!(
            "{} {}",
            u64::from(HOLD_CPU_COUNT) * LIMIT_CPU_PERIOD_MICROS,
            LIMIT_CPU_PERIOD_MICROS
        ),
        pids_max: LIMIT_MAX_PIDS.to_string(),
    }
}

fn named<'a>(checks: &'a [Check], name: &str) -> &'a Check {
    let found: Vec<&Check> = checks.iter().filter(|check| check.name == name).collect();
    assert_eq!(found.len(), 1, "`{name}` is not emitted exactly once");
    found[0]
}

#[test]
fn scope_limits_pass_for_the_configured_limits() {
    let checks = verdict::scope_limits(&limited_scope());
    assert_eq!(checks.len(), 4);
    assert!(checks.iter().all(Check::is_passed));
}

#[test]
fn scope_limits_fail_an_unlimited_memory_max() {
    let facts = ScopeFacts {
        memory_max: "max".to_owned(),
        ..limited_scope()
    };
    let checks = verdict::scope_limits(&facts);
    assert!(failure(named(&checks, names::limits::MEMORY_MAX)).contains("found `max`"));
    assert!(named(&checks, names::limits::OOM_GROUP).is_passed());
    assert!(named(&checks, names::limits::CPU_MAX).is_passed());
    assert!(named(&checks, names::limits::PIDS_MAX).is_passed());
}

#[test]
fn scope_limits_fail_a_scope_without_group_oom_kill() {
    let facts = ScopeFacts {
        oom_group: "0".to_owned(),
        ..limited_scope()
    };
    let checks = verdict::scope_limits(&facts);
    assert!(failure(named(&checks, names::limits::OOM_GROUP)).contains("found `0`"));
    assert!(named(&checks, names::limits::MEMORY_MAX).is_passed());
}

#[test]
fn scope_limits_fail_an_unlimited_cpu_max() {
    let facts = ScopeFacts {
        cpu_max: "max 100000".to_owned(),
        ..limited_scope()
    };
    let checks = verdict::scope_limits(&facts);
    assert!(failure(named(&checks, names::limits::CPU_MAX)).contains("found `max 100000`"));
    assert!(named(&checks, names::limits::PIDS_MAX).is_passed());
}

#[test]
fn scope_limits_fail_an_unlimited_pids_max() {
    let facts = ScopeFacts {
        pids_max: "max".to_owned(),
        ..limited_scope()
    };
    let checks = verdict::scope_limits(&facts);
    assert!(failure(named(&checks, names::limits::PIDS_MAX)).contains("found `max`"));
    assert!(named(&checks, names::limits::CPU_MAX).is_passed());
}

#[test]
fn hard_ceiling_passes_when_the_action_is_killed_with_137() {
    let check = verdict::hard_ceiling(&build(Some(1), "ERROR: action failed: (Exit 137)"), TAIL);
    assert!(check.is_passed());
}

#[test]
fn hard_ceiling_passes_when_bazel_reports_killed() {
    let check = verdict::hard_ceiling(
        &build(
            Some(1),
            "ERROR: /tmp/x/workspace/BUILD.bazel:99:6: Probing //:memory_hog failed: (Killed): sh failed: error executing E2eProbe command (from target //:memory_hog)",
        ),
        TAIL,
    );
    assert!(check.is_passed());
}

#[test]
fn hard_ceiling_fails_when_the_build_succeeded() {
    let check = verdict::hard_ceiling(&build(Some(0), "(Exit 137)"), TAIL);
    assert!(failure(&check).contains("the build succeeded"));
}

#[test]
fn hard_ceiling_fails_for_another_exit_code() {
    let check = verdict::hard_ceiling(&build(Some(1), "ERROR: action failed: (Exit 1)"), TAIL);
    assert!(failure(&check).contains("no action was reported killed"));
}

#[test]
fn hard_ceiling_failure_detail_includes_the_build_output() {
    let check = verdict::hard_ceiling(
        &build(
            Some(1),
            "ERROR: Build failed\nERROR: no space left on device\n",
        ),
        TAIL,
    );
    let detail = failure(&check);
    assert!(detail.contains("the build said:"));
    assert!(detail.contains("ERROR: no space left on device"));
}

#[test]
fn hard_ceiling_does_not_match_a_longer_code() {
    let check = verdict::hard_ceiling(&build(Some(1), "ERROR: action failed: (Exit 1370)"), TAIL);
    assert!(failure(&check).contains("no action was reported killed"));
}

const DISK_KILL: &str = "ERROR: /tmp/x/BUILD.bazel:20:6: Probing //:disk_hog failed: action exceeded its disk reservation: reserved 4096 KiB (disk_kb), limit 4915 KiB with 20% headroom, its own files came to 65536 KiB. Raise the reservation or write less.";

#[test]
fn disk_killed_passes_when_the_hog_fails_with_the_worker_message() {
    let check = verdict::disk_killed(&build(Some(1), DISK_KILL), "disk_hog", TAIL);
    assert!(check.is_passed());
}

#[test]
fn disk_killed_fails_when_the_build_succeeded() {
    let check = verdict::disk_killed(&build(Some(0), DISK_KILL), "disk_hog", TAIL);
    assert!(failure(&check).contains("the build succeeded"));
}

#[test]
fn disk_killed_fails_when_another_target_carries_the_message() {
    let check = verdict::disk_killed(
        &build(
            Some(1),
            "ERROR: //:disk_neighbour failed: exceeded its disk reservation",
        ),
        "disk_hog",
        TAIL,
    );
    assert!(failure(&check).contains("did not report `disk_hog`"));
}

#[test]
fn disk_killed_fails_without_the_worker_message() {
    let check = verdict::disk_killed(
        &build(Some(1), "ERROR: //:disk_hog failed: (Exit 1)"),
        "disk_hog",
        TAIL,
    );
    assert!(failure(&check).contains("did not report `disk_hog`"));
}

#[test]
fn disk_killed_failure_detail_includes_the_build_output() {
    let check = verdict::disk_killed(
        &build(
            Some(1),
            "ERROR: Build failed\nERROR: no space left on device\n",
        ),
        "disk_hog",
        TAIL,
    );
    let detail = failure(&check);
    assert!(detail.contains("the build said:"));
    assert!(detail.contains("ERROR: no space left on device"));
}

#[test]
fn soft_memory_requeue_passes_for_success_on_the_second_attempt() {
    let check = verdict::soft_memory_requeue(&build(Some(0), ""), 2);
    assert!(check.is_passed());
}

#[test]
fn soft_memory_requeue_fails_when_the_soft_layer_never_killed() {
    let check = verdict::soft_memory_requeue(&build(Some(0), ""), 1);
    assert!(failure(&check).contains("ran 1 times, 2 expected"));
}

#[test]
fn soft_memory_requeue_fails_when_requeued_more_than_once() {
    let check = verdict::soft_memory_requeue(&build(Some(0), ""), 3);
    assert!(failure(&check).contains("ran 3 times, 2 expected"));
}

#[test]
fn soft_memory_requeue_fails_when_the_build_failed() {
    let check = verdict::soft_memory_requeue(&build(Some(1), "ERROR: (Killed)"), 2);
    assert!(failure(&check).contains("the build failed with exit Some(1)"));
}

#[test]
fn orphan_killed_passes_when_the_orphan_is_gone() {
    assert!(verdict::orphan_killed(false).is_passed());
}

#[test]
fn orphan_killed_fails_when_the_orphan_is_alive() {
    let check = verdict::orphan_killed(true);
    assert!(failure(&check).contains("still alive"));
}

#[test]
fn scope_removed_passes_when_the_scope_is_gone() {
    assert!(verdict::scope_removed(false).is_passed());
}

#[test]
fn scope_removed_fails_when_the_scope_remains() {
    let check = verdict::scope_removed(true);
    assert!(failure(&check).contains("still exists"));
}

#[test]
fn every_drain_terminated_passes_when_the_drained_instance_is_terminated() -> TestResult {
    assert!(verdict::every_drain_terminated(&fleet_events()?));
    Ok(())
}

#[test]
fn every_drain_terminated_fails_when_the_drained_instance_is_not_terminated() -> TestResult {
    let events = [event(EventChange::DrainStarted {
        worker: worker(WORKER)?,
    })?];
    assert!(!verdict::every_drain_terminated(&events));
    Ok(())
}

#[test]
fn an_empty_history_is_not_drained() {
    assert!(!verdict::every_drain_terminated(&[]));
}

#[test]
fn every_drain_terminated_is_false_without_a_drain() -> TestResult {
    let events = [event(EventChange::ScaledOut { from: 0, to: 1 })?];
    assert!(!verdict::every_drain_terminated(&events));
    Ok(())
}

#[test]
fn poison_attempted_passes_when_the_action_modified_its_input() {
    let check = verdict::poison_attempted(&PoisonReport { modified: true });
    assert_eq!(check.name, names::integrity::POISON_ATTEMPTED);
    assert!(check.is_passed());
}

#[test]
fn poison_attempted_fails_when_the_action_was_refused() {
    let check = verdict::poison_attempted(&PoisonReport { modified: false });
    assert!(check.is_failure());
}

const AUTH_NAME: &str = "the build is rejected for its authentication";

#[test]
fn authentication_rejection_passes_for_an_unauthenticated_status() {
    let check = verdict::authentication_rejection(
        AUTH_NAME,
        &build(
            Some(1),
            "ERROR: Remote execution failed: UNAUTHENTICATED: no token",
        ),
    );
    assert_eq!(check.name, AUTH_NAME);
    assert!(check.is_passed());
}

#[test]
fn authentication_rejection_passes_for_a_permission_denied_status() {
    let check = verdict::authentication_rejection(
        AUTH_NAME,
        &build(
            Some(1),
            "ERROR: Remote execution failed: PERMISSION_DENIED: revoked",
        ),
    );
    assert!(check.is_passed());
}

#[test]
fn authentication_rejection_fails_for_a_credential_helper_without_a_session() {
    let check = verdict::authentication_rejection(
        AUTH_NAME,
        &build(
            Some(1),
            "ERROR: Failed to query remote execution capabilities: Failed to get credentials for 'https://127.0.0.1:34431/build.bazel.remote.execution.v2.Capabilities' from helper '/path/gang': process exited with code 1. stderr: \n  fail  gang get: not logged in\n   fix  gang login --server <URL>",
        ),
    );
    assert!(
        !check.is_passed(),
        "the client refusing to log in is not the server rejecting the build"
    );
}

#[test]
fn authentication_rejection_fails_for_a_failure_that_is_not_authentication() {
    let check =
        verdict::authentication_rejection(AUTH_NAME, &build(Some(1), "ERROR: no such target"));
    assert!(failure(&check).contains("the build failed for another reason: ERROR: no such target"));
}

#[test]
fn authentication_rejection_fails_when_the_build_succeeded() {
    let check =
        verdict::authentication_rejection(AUTH_NAME, &build(Some(0), "INFO: Build completed"));
    assert!(failure(&check).contains("the build succeeded"));
}

const SMALL: &str = "local-small";
const LARGE: &str = "local-large";
const NOOP: &str = "local-noop";

fn scaled_out(fleet: &str) -> Result<Event, Box<dyn std::error::Error>> {
    fleet_event(fleet, EventChange::ScaledOut { from: 0, to: 1 })
}

fn drained(fleet: &str) -> Result<Event, Box<dyn std::error::Error>> {
    fleet_event(
        fleet,
        EventChange::DrainStarted {
            worker: worker(WORKER)?,
        },
    )
}

fn fleet_names(events: &[Event]) -> Vec<String> {
    verdict::scaled_out_fleets(events)
        .iter()
        .map(|fleet| fleet.as_str().to_owned())
        .collect()
}

#[test]
fn scaled_out_fleets_lists_each_fleet_once_in_the_order_of_its_first_scale_out() -> TestResult {
    let events = [
        scaled_out(LARGE)?,
        scaled_out(SMALL)?,
        scaled_out(LARGE)?,
        scaled_out(NOOP)?,
    ];
    assert_eq!(fleet_names(&events), [LARGE, SMALL, NOOP]);
    Ok(())
}

#[test]
fn scaled_out_fleets_ignores_changes_that_are_not_scale_outs() -> TestResult {
    let events = [drained(SMALL)?, scaled_out(LARGE)?, drained(NOOP)?];
    assert_eq!(fleet_names(&events), [LARGE]);
    Ok(())
}

#[test]
fn scaled_out_fleets_is_empty_without_events() {
    assert!(verdict::scaled_out_fleets(&[]).is_empty());
}

const PLACED: &str = "the action is placed";

#[test]
fn placed_on_passes_for_the_expected_fleet_and_ignores_earlier_events() -> TestResult {
    let before = [scaled_out(SMALL)?, drained(SMALL)?];
    let after = [
        scaled_out(SMALL)?,
        drained(SMALL)?,
        scaled_out(LARGE)?,
        drained(LARGE)?,
    ];
    let check = verdict::placed_on(PLACED, &before, &after, &[LARGE]);
    assert_eq!(check.name, PLACED);
    assert!(check.is_passed());
    Ok(())
}

#[test]
fn placed_on_passes_for_fleets_in_the_expected_order() -> TestResult {
    let after = [scaled_out(SMALL)?, scaled_out(LARGE)?];
    assert!(verdict::placed_on(PLACED, &[], &after, &[SMALL, LARGE]).is_passed());
    Ok(())
}

#[test]
fn placed_on_counts_repeated_scale_outs_of_a_fleet_once() -> TestResult {
    let after = [scaled_out(SMALL)?, scaled_out(SMALL)?];
    assert!(verdict::placed_on(PLACED, &[], &after, &[SMALL]).is_passed());
    Ok(())
}

#[test]
fn placed_on_fails_when_another_fleet_scaled_out_too() -> TestResult {
    let after = [scaled_out(SMALL)?, scaled_out(LARGE)?];
    let check = verdict::placed_on(PLACED, &[], &after, &[LARGE]);
    let reason = failure(&check);
    assert!(
        reason.contains("[\"local-small\", \"local-large\"]"),
        "{reason}"
    );
    assert!(reason.contains("[\"local-large\"]"), "{reason}");
    Ok(())
}

#[test]
fn placed_on_fails_when_the_expected_fleet_did_not_scale_out() -> TestResult {
    let after = [scaled_out(SMALL)?];
    let check = verdict::placed_on(PLACED, &[], &after, &[SMALL, LARGE]);
    assert!(check.is_failure());
    Ok(())
}

#[test]
fn placed_on_fails_when_nothing_scaled_out() -> TestResult {
    let before = [scaled_out(SMALL)?];
    let check = verdict::placed_on(PLACED, &before, &before, &[SMALL]);
    assert!(failure(&check).contains("[]"));
    Ok(())
}

#[test]
fn placed_on_fails_when_the_fleets_scaled_out_in_the_wrong_order() -> TestResult {
    let after = [scaled_out(LARGE)?, scaled_out(SMALL)?];
    let check = verdict::placed_on(PLACED, &[], &after, &[SMALL, LARGE]);
    assert!(check.is_failure());
    Ok(())
}

#[test]
fn placed_on_passes_for_no_scale_out_when_none_is_expected() -> TestResult {
    let before = [scaled_out(SMALL)?];
    let after = [scaled_out(SMALL)?, drained(SMALL)?];
    assert!(verdict::placed_on(PLACED, &before, &after, &[]).is_passed());
    Ok(())
}

#[test]
fn placed_on_fails_when_the_history_does_not_start_with_the_earlier_events() -> TestResult {
    let before = [scaled_out(SMALL)?];
    let after = [scaled_out(LARGE)?];
    let check = verdict::placed_on(PLACED, &before, &after, &[LARGE]);
    assert!(failure(&check).contains("no longer starts with"));
    Ok(())
}

#[test]
fn noop_fleet_action_in_the_host_namespaces_passes() {
    let observed = namespaces(HOST_NET, HOST_MNT);
    let check = verdict::noop_fleet_unisolated(Sandbox::Bwrap, &host(), &observed);
    assert_eq!(check.name, names::isolation::NOOP_FLEET_UNISOLATED);
    assert!(check.is_passed());
}

#[test]
fn noop_fleet_action_in_its_own_net_namespace_fails() {
    let observed = namespaces(OTHER_NET, HOST_MNT);
    let check = verdict::noop_fleet_unisolated(Sandbox::Bwrap, &host(), &observed);
    assert!(failure(&check).contains(OTHER_NET));
}

#[test]
fn noop_fleet_action_in_its_own_mnt_namespace_fails() {
    let observed = namespaces(HOST_NET, OTHER_MNT);
    let check = verdict::noop_fleet_unisolated(Sandbox::Bwrap, &host(), &observed);
    assert!(failure(&check).contains(OTHER_MNT));
}

#[test]
fn noop_fleet_action_with_unavailable_namespaces_fails() {
    let observed = namespaces("unavailable", "unavailable");
    let check = verdict::noop_fleet_unisolated(Sandbox::Bwrap, &host(), &observed);
    assert!(check.is_failure());
}

#[test]
fn noop_fleet_isolation_is_skipped_when_the_native_sandbox_is_noop_too() {
    let observed = namespaces(HOST_NET, HOST_MNT);
    let check = verdict::noop_fleet_unisolated(Sandbox::Noop, &host(), &observed);
    assert_eq!(check.name, names::isolation::NOOP_FLEET_UNISOLATED);
    assert!(check.is_skipped());
}

#[test]
fn noop_fleet_isolation_is_skipped_when_the_host_is_unreadable() {
    let unreadable = Host {
        net: None,
        mnt: None,
    };
    let observed = namespaces(HOST_NET, HOST_MNT);
    let check = verdict::noop_fleet_unisolated(Sandbox::Bwrap, &unreadable, &observed);
    assert!(check.is_skipped());
}
