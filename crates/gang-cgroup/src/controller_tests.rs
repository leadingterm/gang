//! Behaviour of the cgroup [`Controller`] against the in-memory [`TestTree`]: preparing the
//! worker's leaf, creating actions' scopes with their limits, and tearing scopes down again.

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use gang_clock::{Clock, TestClock};
use gang_spi::{ActionLimits, ActionScope};
use gang_types::{Bytes, HeadroomPercent, NonZeroBytes, NonZeroDuration};

use crate::config::{CgroupConfig, CpuPeriod, Delegation, OomGroup};
use crate::controller::{CgroupScope, Controller};
use crate::errors::CgroupError;
use crate::fs::{Cgroupfs, PROCS};
use crate::join::enter_scope;
use crate::test_tree::{OWN_START, TestTree};

/// The type the tests drive: the controller on the in-memory tree with a test clock.
type TestController = Controller<TestTree, Arc<TestClock>>;

/// The subtree the worker process is started in.
const SUBTREE: &str = "system/gang-worker";
/// The controllers the delegated subtree offers in most tests.
const DELEGATED: [&str; 4] = ["cpu", "memory", "pids", "io"];
/// The controllers the actions are given.
const ENABLED: [&str; 3] = ["cpu", "memory", "pids"];
/// The `gang-join` link the controllers are built with.
const JOIN: &str = "/work/bin/gang-join";

/// Polls `future` to completion, advancing `clock` by `step` each time it is pending, and returns
/// its output with the number of advances. The wakers are never used: the future is polled again
/// after every advance.
fn settled<T>(clock: &TestClock, step: Duration, future: impl Future<Output = T>) -> (T, u32) {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let mut advances = 0;
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return (output, advances);
        }
        clock.advance(step);
        advances += 1;
    }
}

fn subtree() -> PathBuf {
    PathBuf::from(SUBTREE)
}

fn worker() -> PathBuf {
    Path::new(SUBTREE).join("worker")
}

fn actions() -> PathBuf {
    Path::new(SUBTREE).join("actions")
}

fn delegated() -> TestTree {
    TestTree::new(SUBTREE, &DELEGATED)
}

/// The removal poll interval of the default configuration.
fn default_poll_interval() -> Duration {
    CgroupConfig::default().removal_poll_interval.get()
}

fn controller(tree: TestTree) -> TestController {
    controller_with(tree, CgroupConfig::default())
}

fn controller_with(tree: TestTree, config: CgroupConfig) -> TestController {
    Controller::new(
        tree,
        Arc::new(TestClock::default()),
        config,
        PathBuf::from(JOIN),
    )
}

fn limits(cpu_count: Option<u32>, memory: Option<u64>) -> ActionLimits {
    ActionLimits {
        cpu_count: cpu_count.map(|count| NonZeroU32::new(count).expect("non-zero cpu count")),
        memory: memory.and_then(|bytes| NonZeroBytes::new(Bytes::new(bytes))),
    }
}

/// The trimmed content of an interface file of `cgroup`, `None` when the file does not exist.
fn trimmed(tree: &TestTree, cgroup: &Path, file: &str) -> Option<String> {
    tree.file(cgroup, file).map(|value| value.trim().to_owned())
}

/// The cgroup of `scope`, relative to the cgroup root.
fn scope_path(scope: &CgroupScope) -> PathBuf {
    scope.cgroup.clone()
}

#[tokio::test]
async fn prepare_moves_process_into_worker_leaf_and_enables_controllers() {
    let controller = controller(delegated());

    controller.prepare().await.expect("prepare");

    assert_eq!(controller.fs.member(), worker());
    assert!(controller.fs.exists(&actions()));
    assert_eq!(controller.fs.enabled(&subtree()), ENABLED);
    assert_eq!(controller.fs.enabled(&actions()), ENABLED);
}

#[tokio::test]
async fn prepare_is_repeatable_from_worker_leaf() {
    let controller = controller(delegated());
    controller.prepare().await.expect("first prepare");

    controller.prepare().await.expect("second prepare");

    assert_eq!(controller.fs.member(), worker());
    assert_eq!(controller.fs.enabled(&subtree()), ENABLED);
    assert_eq!(controller.fs.enabled(&actions()), ENABLED);
}

#[tokio::test]
async fn prepare_fails_without_moving_when_subtree_lacks_a_controller() {
    let controller = controller(TestTree::new(SUBTREE, &["cpu", "memory", "io"]));

    let error = controller.prepare().await.unwrap_err();

    assert!(
        matches!(
            error,
            CgroupError::MissingController {
                controller: "pids",
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(controller.fs.member(), subtree());
}

#[tokio::test]
async fn prepare_in_root_cgroup_fails_without_delegated_subtree() {
    let controller = controller(TestTree::new("", &DELEGATED));

    let error = controller.prepare().await.unwrap_err();

    assert!(
        matches!(error, CgroupError::NoDelegatedSubtree),
        "{error:?}"
    );
    assert_eq!(controller.fs.member(), Path::new(""));
}

#[tokio::test]
async fn prepare_fails_without_moving_when_subtree_is_not_delegated() {
    let controller = controller(delegated().undelegated());

    let error = controller.prepare().await.unwrap_err();

    assert!(
        matches!(error, CgroupError::NotDelegated { .. }),
        "{error:?}"
    );
    assert_eq!(controller.fs.member(), subtree());
    assert!(!controller.fs.exists(&worker()));
    assert!(!controller.fs.exists(&actions()));
}

#[tokio::test]
async fn prepare_trusts_an_undelegated_subtree_when_configured() {
    let config = CgroupConfig {
        delegation: Delegation::Trusted,
        ..CgroupConfig::default()
    };
    let controller = controller_with(delegated().undelegated(), config);

    controller.prepare().await.expect("prepare");

    assert_eq!(controller.fs.member(), worker());
    assert!(controller.fs.exists(&actions()));
}

#[tokio::test]
async fn prepare_fails_when_kernel_lacks_cgroup_kill() {
    let controller = controller(delegated().without_kernel_file("cgroup.kill"));

    let error = controller.prepare().await.unwrap_err();

    assert!(
        matches!(error, CgroupError::MissingKernelFeature { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn prepare_fails_when_kernel_lacks_memory_peak() {
    let controller = controller(delegated().without_kernel_file("memory.peak"));

    let error = controller.prepare().await.unwrap_err();

    assert!(
        matches!(error, CgroupError::MissingKernelFeature { .. }),
        "{error:?}"
    );
}

#[test]
fn prepare_removes_scopes_left_by_previous_worker() {
    let tree = delegated();
    let stale = actions().join("old");
    tree.mkdir_all(&stale);
    tree.populate(&stale, 2);
    let controller = controller(tree);

    let (result, pauses) = settled(
        &controller.clock,
        default_poll_interval(),
        controller.prepare(),
    );

    result.expect("prepare");
    assert!(!controller.fs.exists(&stale));
    assert!(controller.fs.killed().contains(&stale));
    assert_eq!(pauses, 2);
}

#[test]
fn prepare_fails_when_a_stale_scope_never_empties() {
    let config = CgroupConfig {
        removal_timeout: Duration::from_millis(3),
        removal_poll_interval: NonZeroDuration::from_millis(1),
        ..CgroupConfig::default()
    };
    let step = config.removal_poll_interval.get();
    let tree = delegated();
    let stale = actions().join("99992-1-0");
    tree.mkdir_all(&stale);
    tree.populate(&stale, u32::MAX);
    let controller = controller_with(tree, config);

    let (result, _pauses) = settled(&controller.clock, step, controller.prepare());

    let error = result.unwrap_err();
    assert!(
        matches!(error, CgroupError::StillPopulated { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn enter_creates_scope_with_requested_limits() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");

    let scope = controller
        .enter(&limits(Some(2), Some(1 << 30)))
        .expect("enter");

    let cgroup = scope_path(&scope);
    let tree = &controller.fs;
    assert_eq!(cgroup.parent(), Some(actions().as_path()));
    assert_eq!(
        trimmed(tree, &cgroup, "memory.max").as_deref(),
        Some("2147483648")
    );
    assert_eq!(
        trimmed(tree, &cgroup, "memory.oom.group").as_deref(),
        Some("1")
    );
    assert_eq!(
        trimmed(tree, &cgroup, "cpu.max").as_deref(),
        Some("200000 100000")
    );
    assert_eq!(trimmed(tree, &cgroup, "pids.max").as_deref(), Some("4096"));
}

#[tokio::test]
async fn enter_honours_configured_headroom_pids_and_period() {
    let config = CgroupConfig {
        memory_headroom_percent: HeadroomPercent::from_percent(50),
        max_pids: NonZeroU32::new(8).expect("non-zero pids"),
        cpu_period: CpuPeriod::try_new(Duration::from_millis(10)).expect("valid cpu period"),
        ..CgroupConfig::default()
    };
    let controller = controller_with(delegated(), config);
    controller.prepare().await.expect("prepare");

    let scope = controller
        .enter(&limits(Some(2), Some(1 << 30)))
        .expect("enter");

    let cgroup = scope_path(&scope);
    let tree = &controller.fs;
    assert_eq!(
        trimmed(tree, &cgroup, "memory.max").as_deref(),
        Some("1610612736")
    );
    assert_eq!(trimmed(tree, &cgroup, "pids.max").as_deref(), Some("8"));
    assert_eq!(
        trimmed(tree, &cgroup, "cpu.max").as_deref(),
        Some("20000 10000")
    );
}

#[tokio::test]
async fn enter_writes_the_configured_oom_group() {
    let config = CgroupConfig {
        oom_group: OomGroup::Process,
        ..CgroupConfig::default()
    };
    let controller = controller_with(delegated(), config);
    controller.prepare().await.expect("prepare");

    let scope = controller.enter(&limits(None, None)).expect("enter");

    let cgroup = scope_path(&scope);
    let tree = &controller.fs;
    assert_eq!(
        trimmed(tree, &cgroup, "memory.oom.group").as_deref(),
        Some("0")
    );
}

#[tokio::test]
async fn enter_without_requested_limits_writes_no_memory_or_cpu_maximum() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");

    let scope = controller.enter(&limits(None, None)).expect("enter");

    let cgroup = scope_path(&scope);
    let tree = &controller.fs;
    assert_eq!(trimmed(tree, &cgroup, "memory.max"), None);
    assert_eq!(trimmed(tree, &cgroup, "cpu.max"), None);
    assert_eq!(
        trimmed(tree, &cgroup, "memory.oom.group").as_deref(),
        Some("1")
    );
    assert_eq!(trimmed(tree, &cgroup, "pids.max").as_deref(), Some("4096"));
}

#[tokio::test]
async fn enter_forbids_swap_by_default() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");

    let scope = controller
        .enter(&limits(Some(2), Some(1 << 30)))
        .expect("enter");

    let cgroup = scope_path(&scope);
    assert_eq!(
        trimmed(&controller.fs, &cgroup, "memory.swap.max").as_deref(),
        Some("0")
    );
}

#[tokio::test]
async fn enter_writes_the_configured_swap_maximum() {
    let config = CgroupConfig {
        swap_max: Bytes::new(1 << 20),
        ..CgroupConfig::default()
    };
    let controller = controller_with(delegated(), config);
    controller.prepare().await.expect("prepare");

    let scope = controller
        .enter(&limits(Some(2), Some(1 << 30)))
        .expect("enter");

    let cgroup = scope_path(&scope);
    assert_eq!(
        trimmed(&controller.fs, &cgroup, "memory.swap.max").as_deref(),
        Some("1048576")
    );
}

#[tokio::test]
async fn enter_without_swap_accounting_still_applies_the_other_limits() {
    let controller = controller(delegated().without_kernel_file("memory.swap.max"));
    controller.prepare().await.expect("prepare");

    let scope = controller
        .enter(&limits(Some(2), Some(1 << 30)))
        .expect("enter");

    let cgroup = scope_path(&scope);
    let tree = &controller.fs;
    assert_eq!(trimmed(tree, &cgroup, "memory.swap.max"), None);
    assert_eq!(
        trimmed(tree, &cgroup, "memory.max").as_deref(),
        Some("2147483648")
    );
}

#[tokio::test]
async fn enter_without_a_memory_reservation_writes_no_swap_maximum() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");

    let scope = controller.enter(&limits(None, None)).expect("enter");

    let cgroup = scope_path(&scope);
    assert_eq!(trimmed(&controller.fs, &cgroup, "memory.swap.max"), None);
}

#[tokio::test]
async fn enter_before_prepare_fails_and_creates_nothing() {
    let controller = controller(delegated());

    let error = controller.enter(&limits(None, None)).unwrap_err();

    assert!(
        matches!(error, CgroupError::NotPrepared { .. }),
        "{error:?}"
    );
    assert_eq!(controller.fs.member(), subtree());
    assert!(!controller.fs.exists(&actions()));
}

#[tokio::test]
async fn enter_failing_to_apply_limits_leaves_process_and_no_scope() {
    let controller = controller(delegated().reject_writes_of("pids.max"));
    controller.prepare().await.expect("prepare");

    let error = controller
        .enter(&limits(Some(2), Some(1 << 30)))
        .unwrap_err();

    assert!(matches!(error, CgroupError::Io { .. }), "{error:?}");
    assert_eq!(controller.fs.member(), worker());
    assert!(
        controller
            .fs
            .children(&actions())
            .expect("children")
            .is_empty()
    );
}

#[tokio::test]
async fn a_scope_name_collision_is_an_error_not_a_takeover() {
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    let controller = controller(delegated()).with_serial(&SERIAL);
    controller.prepare().await.expect("prepare");
    let existing = actions().join(format!("{}-{OWN_START}-0", std::process::id()));
    controller.fs.mkdir_all(&existing);

    let error = controller.enter(&limits(None, None)).unwrap_err();

    assert!(matches!(error, CgroupError::Io { .. }), "{error:?}");
    assert_eq!(controller.fs.member(), worker());
    assert!(controller.fs.exists(&existing));
    assert!(controller.fs.killed().is_empty());
}

#[tokio::test]
async fn enter_sweeps_scopes_of_dead_creators_only() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let dead = actions().join("99999-1-1");
    let populated = actions().join("99998-1-1");
    let live = actions().join("99996-2000-1");
    let foreign = actions().join("not-a-scope");
    controller.fs.mkdir_all(&dead);
    controller.fs.mkdir_all(&populated);
    controller.fs.populate(&populated, 2);
    controller.fs.mkdir_all(&live);
    controller.fs.populate(&live, 1);
    controller.fs.set_process_start(99_996, Some(2_000));
    controller.fs.mkdir_all(&foreign);

    let before = controller.clock.monotonic();
    let scope = controller.enter(&limits(None, None));

    let _scope = scope.expect("enter");
    assert!(!controller.fs.exists(&dead));
    assert!(controller.fs.exists(&populated));
    assert!(controller.fs.killed().contains(&populated));
    assert_eq!(controller.clock.monotonic(), before);
    assert!(controller.fs.exists(&live));
    assert!(controller.fs.exists(&foreign));
}

#[tokio::test]
async fn a_dead_creators_scope_that_never_empties_does_not_block_enter() {
    let config = CgroupConfig {
        removal_timeout: Duration::from_millis(3),
        removal_poll_interval: NonZeroDuration::from_millis(1),
        ..CgroupConfig::default()
    };
    let controller = controller_with(delegated(), config);
    controller.prepare().await.expect("prepare");
    let stale = actions().join("99993-1-0");
    controller.fs.mkdir_all(&stale);
    controller.fs.populate(&stale, u32::MAX);

    let before = controller.clock.monotonic();
    let scope = controller.enter(&limits(None, None));

    let _scope = scope.expect("enter");
    assert_eq!(controller.clock.monotonic(), before);
    assert!(controller.fs.killed().contains(&stale));
    assert!(controller.fs.exists(&stale));
}

#[tokio::test]
async fn a_stale_scope_that_never_empties_does_not_delay_enter_or_leave() {
    let config = CgroupConfig {
        removal_timeout: Duration::from_millis(3),
        removal_poll_interval: NonZeroDuration::from_millis(1),
        ..CgroupConfig::default()
    };
    let step = config.removal_poll_interval.get();
    let controller = controller_with(delegated(), config);
    controller.prepare().await.expect("prepare");
    let stale = actions().join("99992-1-0");
    controller.fs.mkdir_all(&stale);
    controller.fs.populate(&stale, u32::MAX);
    let before = controller.clock.monotonic();

    let scope = controller.enter(&limits(None, None));
    let scope = scope.expect("enter");
    let (usage, leave_pauses) = settled(&controller.clock, step, controller.leave(scope));

    usage.expect("leave");
    assert_eq!(leave_pauses, 0);
    assert_eq!(controller.clock.monotonic(), before);
    assert!(controller.fs.exists(&stale));
    assert!(controller.fs.killed().contains(&stale));
}

#[tokio::test]
async fn a_stale_scope_that_empties_later_is_removed_by_the_next_sweep() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let stale = actions().join("99991-1-0");
    controller.fs.mkdir_all(&stale);
    controller.fs.populate(&stale, u32::MAX);

    let scope = controller.enter(&limits(None, None)).expect("enter");
    assert!(controller.fs.exists(&stale));
    assert!(controller.fs.killed().contains(&stale));

    controller.fs.populate(&stale, 0);
    controller.leave(scope).await.expect("leave");

    assert!(!controller.fs.exists(&stale));
}

#[tokio::test]
async fn leave_reports_usage_and_removes_the_scope() {
    let controller = controller(
        delegated()
            .with_peak_memory(123_456)
            .with_cpu_micros(2_500_000),
    );
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let cgroup = scope_path(&scope);

    let usage = controller.leave(scope).await.expect("leave");

    assert_eq!(usage.peak_memory, Bytes::new(123_456));
    assert_eq!(usage.cpu_time, Duration::from_millis(2500));
    assert_eq!(controller.fs.member(), worker());
    assert!(!controller.fs.exists(&cgroup));
    assert!(controller.fs.killed().contains(&cgroup));
}

#[tokio::test]
async fn leave_waits_for_processes_to_exit() {
    let controller = controller(delegated().with_deaths_after_kill(3));
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let cgroup = scope_path(&scope);

    let (result, pauses) = settled(
        &controller.clock,
        default_poll_interval(),
        controller.leave(scope),
    );

    result.expect("leave");
    assert!(!controller.fs.exists(&cgroup));
    assert_eq!(controller.fs.member(), worker());
    assert_eq!(pauses, 3);
}

#[tokio::test]
async fn leave_gives_up_when_processes_never_exit_but_still_returns_process() {
    let config = CgroupConfig {
        removal_timeout: Duration::from_millis(3),
        removal_poll_interval: NonZeroDuration::from_millis(1),
        ..CgroupConfig::default()
    };
    let step = config.removal_poll_interval.get();
    let controller = controller_with(delegated().with_deaths_after_kill(1_000_000), config);
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let cgroup = scope_path(&scope);

    let (result, pauses) = settled(&controller.clock, step, controller.leave(scope));

    let error = result.unwrap_err();
    assert!(
        matches!(error, CgroupError::StillPopulated { .. }),
        "{error:?}"
    );
    assert_eq!(controller.fs.member(), worker());
    assert!(controller.fs.exists(&cgroup));
    assert_eq!(pauses, 3);
}

#[tokio::test]
async fn a_populated_scope_that_never_empties_waits_the_whole_removal_timeout() {
    let config = CgroupConfig {
        removal_timeout: Duration::from_millis(30),
        removal_poll_interval: NonZeroDuration::from_millis(10),
        ..CgroupConfig::default()
    };
    let step = config.removal_poll_interval.get();
    let controller = controller_with(delegated().with_deaths_after_kill(1_000_000), config);
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let cgroup = scope_path(&scope);

    let (result, pauses) = settled(&controller.clock, step, controller.leave(scope));

    let error = result.unwrap_err();
    assert!(
        matches!(error, CgroupError::StillPopulated { .. }),
        "{error:?}"
    );
    assert_eq!(pauses, 3);
    assert_eq!(controller.fs.member(), worker());
    assert!(controller.fs.exists(&cgroup));
}

#[tokio::test]
async fn leave_removes_cgroups_the_action_created_inside_its_scope() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let cgroup = scope_path(&scope);
    let grandchild = cgroup.join("child").join("grandchild");
    controller.fs.mkdir_all(&grandchild);

    controller.leave(scope).await.expect("leave");

    assert!(!controller.fs.exists(&cgroup.join("child")));
    assert!(!controller.fs.exists(&grandchild));
    assert!(!controller.fs.exists(&cgroup));
}

#[tokio::test]
async fn leave_cleans_up_when_usage_cannot_be_read() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let cgroup = scope_path(&scope);
    controller
        .fs
        .write(&cgroup, "memory.peak", "garbage")
        .expect("overwrite memory.peak");

    let error = controller.leave(scope).await.unwrap_err();

    assert!(error.to_string().contains("memory.peak"), "{error}");
    assert_eq!(controller.fs.member(), worker());
    assert!(!controller.fs.exists(&cgroup));
    assert!(controller.fs.killed().contains(&cgroup));
}

#[tokio::test]
async fn successive_enter_leave_cycles_use_distinct_scope_names() {
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    let controller = controller(delegated()).with_serial(&SERIAL);
    controller.prepare().await.expect("prepare");

    let first = controller.enter(&limits(None, None)).expect("first enter");
    let first_cgroup = scope_path(&first);
    controller.leave(first).await.expect("first leave");

    let second = controller.enter(&limits(None, None)).expect("second enter");
    let second_cgroup = scope_path(&second);
    controller.leave(second).await.expect("second leave");

    let pid = std::process::id();
    assert_eq!(first_cgroup, actions().join(format!("{pid}-{OWN_START}-0")));
    assert_eq!(
        second_cgroup,
        actions().join(format!("{pid}-{OWN_START}-1"))
    );
}

#[tokio::test]
async fn controllers_on_separate_trees_draw_scope_names_from_one_serial() {
    let first = controller(delegated());
    let second = controller(delegated());
    first.prepare().await.expect("first prepare");
    second.prepare().await.expect("second prepare");

    let first_scope = first.enter(&limits(None, None)).expect("first enter");
    let first_cgroup = scope_path(&first_scope);
    let second_scope = second.enter(&limits(None, None)).expect("second enter");
    let second_cgroup = scope_path(&second_scope);

    assert_ne!(first_cgroup.file_name(), second_cgroup.file_name());
}

#[tokio::test]
async fn reap_waits_for_abandoned_populated_scope_and_removes_descendants() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let abandoned = actions().join("99997-1-1");
    let descendant = abandoned.join("child").join("grandchild");
    controller.fs.mkdir_all(&descendant);
    controller.fs.populate(&abandoned, 2);

    let (result, pauses) = settled(
        &controller.clock,
        default_poll_interval(),
        controller.reap_abandoned(99_997),
    );

    result.expect("reap abandoned");
    assert_eq!(pauses, 2);
    assert!(controller.fs.killed().contains(&abandoned));
    assert!(!controller.fs.exists(&abandoned));
    assert!(!controller.fs.exists(&descendant));
}

#[tokio::test]
async fn reap_leaves_live_creators_scopes_untouched() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let live = actions().join("99994-7-0");
    controller.fs.mkdir_all(&live);
    controller.fs.set_process_start(99_994, Some(7));
    controller.fs.populate(&live, 2);

    controller
        .reap_abandoned(99_994)
        .await
        .expect("reap abandoned");

    assert!(controller.fs.exists(&live));
    assert!(!controller.fs.killed().contains(&live));
}

#[tokio::test]
async fn reap_counts_a_scope_a_racing_reaper_removed_as_removed() {
    let stale = actions().join("99998-1-0");
    let controller = controller(delegated().removed_by_a_racing_reaper(&stale));
    controller.prepare().await.expect("prepare");
    controller.fs.mkdir_all(&stale);
    controller.fs.populate(&stale, 2);

    controller
        .reap_abandoned(99_998)
        .await
        .expect("a scope another reaper removed counts as removed");

    assert!(!controller.fs.exists(&stale));
}

#[tokio::test]
async fn reap_fails_after_timeout_when_abandoned_scope_never_empties() {
    let config = CgroupConfig {
        removal_timeout: Duration::from_millis(3),
        removal_poll_interval: NonZeroDuration::from_millis(1),
        ..CgroupConfig::default()
    };
    let step = config.removal_poll_interval.get();
    let controller = controller_with(delegated(), config);
    controller.prepare().await.expect("prepare");
    let abandoned = actions().join("99992-1-0");
    controller.fs.mkdir_all(&abandoned);
    controller.fs.populate(&abandoned, u32::MAX);

    let (result, pauses) = settled(&controller.clock, step, controller.reap_abandoned(99_992));

    assert!(matches!(result, Err(CgroupError::StillPopulated { .. })));
    assert_eq!(pauses, 3);
    assert!(controller.fs.killed().contains(&abandoned));
    assert!(controller.fs.exists(&abandoned));
}

#[tokio::test]
async fn reap_treats_zombie_creator_as_gone() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let abandoned = actions().join("99993-7-0");
    controller.fs.mkdir_all(&abandoned);
    controller.fs.set_process_start(99_993, Some(7));
    controller.fs.set_zombie(99_993);

    controller
        .reap_abandoned(99_993)
        .await
        .expect("reap abandoned");

    assert!(controller.fs.killed().contains(&abandoned));
    assert!(!controller.fs.exists(&abandoned));
}

#[tokio::test]
async fn reap_leaves_other_gone_creators_scopes_untouched() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let target = actions().join("99996-1-0");
    let other = actions().join("99995-1-0");
    controller.fs.mkdir_all(&target);
    controller.fs.mkdir_all(&other);

    controller
        .reap_abandoned(99_996)
        .await
        .expect("reap abandoned");

    assert!(!controller.fs.exists(&target));
    assert!(controller.fs.killed().contains(&target));
    assert!(controller.fs.exists(&other));
    assert!(!controller.fs.killed().contains(&other));
}

#[tokio::test]
async fn reap_removes_scopes_whose_creator_pid_was_reused() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let abandoned = actions().join("99991-7-0");
    controller.fs.mkdir_all(&abandoned);
    controller.fs.set_process_start(99_991, Some(8));

    controller
        .reap_abandoned(99_991)
        .await
        .expect("reap abandoned");

    assert!(controller.fs.killed().contains(&abandoned));
    assert!(!controller.fs.exists(&abandoned));
}

#[tokio::test]
async fn leave_sweeps_scopes_of_dead_creators() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let vanished = actions().join("99997-1-1");
    controller.fs.mkdir_all(&vanished);

    let (result, pauses) = settled(
        &controller.clock,
        default_poll_interval(),
        controller.leave(scope),
    );

    result.expect("leave");
    assert_eq!(pauses, 0);

    assert!(!controller.fs.exists(&vanished));
    assert!(controller.fs.killed().contains(&vanished));
}

#[tokio::test]
async fn a_scope_whose_creator_pid_was_reused_by_another_process_is_swept() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let stale = actions().join("99995-1-0");
    controller.fs.mkdir_all(&stale);
    controller.fs.set_process_start(99_995, Some(2));

    let _scope = controller.enter(&limits(None, None)).expect("enter");

    assert!(!controller.fs.exists(&stale));
    assert!(controller.fs.killed().contains(&stale));
}

#[tokio::test]
async fn a_scope_of_a_live_creator_is_never_swept() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let live = actions().join("99994-7-0");
    controller.fs.mkdir_all(&live);
    controller.fs.set_process_start(99_994, Some(7));

    let _scope = controller.enter(&limits(None, None)).expect("enter");

    assert!(controller.fs.exists(&live));
    assert!(controller.fs.killed().is_empty());
}

#[tokio::test]
async fn enter_fails_when_the_process_start_time_is_unknown() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    controller.fs.set_process_start(std::process::id(), None);

    let error = controller.enter(&limits(None, None)).unwrap_err();

    assert!(
        matches!(error, CgroupError::ProcessStart { .. }),
        "{error:?}"
    );
    assert_eq!(controller.fs.member(), worker());
    assert!(
        controller
            .fs
            .children(&actions())
            .expect("children")
            .is_empty()
    );
}

#[tokio::test]
async fn scope_names_carry_the_creator_start_time() {
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    let controller = controller(delegated()).with_serial(&SERIAL);
    controller.prepare().await.expect("prepare");
    controller
        .fs
        .set_process_start(std::process::id(), Some(4_242));

    let scope = controller.enter(&limits(None, None)).expect("enter");

    let pid = std::process::id();
    assert_eq!(scope_path(&scope), actions().join(format!("{pid}-4242-0")));
}

/// The caller stays in the worker leaf after `enter`: the scope's `cgroup.procs` is never written
/// here, because the sandbox's first process joins the scope through the launcher.
#[tokio::test]
async fn enter_never_moves_the_caller() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");

    let _scope = controller.enter(&limits(None, None)).expect("enter");

    assert_eq!(controller.fs.member(), worker());
}

/// The trampoline started through the launcher joins the scope `enter` created, wherever the
/// cgroup filesystem is mounted and whatever the trampoline's working directory.
#[tokio::test]
async fn the_launcher_joins_the_scope_enter_created() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");

    let [join, directory] = scope.launcher() else {
        panic!(
            "the launcher is the join link and the scope: {:?}",
            scope.launcher()
        );
    };
    enter_scope(Path::new(directory)).expect("the trampoline joins the scope");

    assert_eq!(join, JOIN);
    assert_eq!(
        trimmed(&controller.fs, &scope_path(&scope), PROCS),
        Some(std::process::id().to_string())
    );
}

#[tokio::test]
async fn leave_right_after_enter_removes_the_scope() {
    let controller = controller(delegated());
    controller.prepare().await.expect("prepare");
    let scope = controller.enter(&limits(None, None)).expect("enter");
    let cgroup = scope_path(&scope);

    controller.leave(scope).await.expect("leave");

    assert!(!controller.fs.exists(&cgroup));
    assert_eq!(controller.fs.member(), worker());
}
