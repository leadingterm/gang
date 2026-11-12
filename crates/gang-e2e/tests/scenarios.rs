//! Behaviour of the end-to-end scenarios against an in-memory deployment: a compliant system
//! passes every check, and each injected misbehaviour fails exactly the checks it breaks.

/// Declares a simulation's flaw enums, the struct holding one flaw per field, and its `COMPLIANT`
/// constant, whose every field is the first variant of its enum.
///
/// A field `name: Enum { Compliant, Flawed, .. }` declares `Enum`; the field's documentation is
/// also the enum's. A field in the trailing `shares { .. }` block reuses an enum an earlier field
/// declared: `name: Enum = Compliant`.
macro_rules! behaviour {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$doc:meta])*
                $field:ident: $flaw:ident {
                    $(#[$compliant_doc:meta])* $compliant:ident
                    $(, $(#[$variant_doc:meta])* $variant:ident)* $(,)?
                }
            ),+ $(,)?
        }
        $(shares {
            $($(#[$shared_doc:meta])* $shared:ident: $shared_flaw:ident = $shared_compliant:ident),+
            $(,)?
        })?
    ) => {
        $(
            $(#[$doc])*
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            $vis enum $flaw {
                $(#[$compliant_doc])*
                $compliant,
                $($(#[$variant_doc])* $variant,)*
            }
        )+

        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $vis struct $name {
            $($(#[$doc])* $vis $field: $flaw,)+
            $($($(#[$shared_doc])* $vis $shared: $shared_flaw,)+)?
        }

        impl $name {
            /// The behaviour without any flaw: every field is its enum's first variant.
            $vis const COMPLIANT: Self = Self {
                $($field: $flaw::$compliant,)+
                $($($shared: $shared_flaw::$shared_compliant,)+)?
            };
        }
    };
}

mod support;

use gang_e2e::Limits as _;
use gang_e2e::System as _;
use gang_e2e::{
    ActionLimits, Check, FleetLayout, Identity, InputLinks, LARGE_FLEET, NOOP_FLEET, Outcome,
    Probe, SMALL_FLEET, Sandbox, Settings, Signalled, SoftMemory, Variant, names,
};
use gang_e2e::{
    disk, fleets, input_integrity, lifecycle, limits, memory, network, plaintext, restart,
    revocation,
};
use gang_wire::routes::{GET_STATE, WATCH_STATE};
use support::{
    AdminRoutes, Anonymous, Behaviour, BuildFailure, Cache, Completed, Convergence, Dashboard,
    Disk, Drain, Grower, Handshake, HoldBuild, HoldSignal, Integrity, Isolation, Limits, Neighbour,
    OrphanProcess, PidReuse, Requests, Restart, Revocation, Routing, ScopeReport, Serving,
    SimulatedIdentity, Simulation, StateView, StreamCauses, Termination, TokenIssue, Warm,
};

/// The name of the check that a request for the state without a session cookie is refused.
fn cookieless_state() -> String {
    names::dashboard::cookieless_refused(GET_STATE.template)
}

/// The name of the check that a request for the state stream without a session cookie is refused.
fn cookieless_stream() -> String {
    names::dashboard::cookieless_refused(WATCH_STATE.template)
}

/// The check that an unconstrained action scales out the small fleet only.
fn unconstrained_placed() -> String {
    names::fleets::placed(names::fleets::UNCONSTRAINED, &[SMALL_FLEET])
}

/// The check that an action pinned to the large fleet scales out that fleet only.
fn pinned_placed() -> String {
    names::fleets::placed(names::fleets::PINNED, &[LARGE_FLEET])
}

/// The check that an action asking for more memory than the first fleet has scales out the large
/// fleet only.
fn memory_placed() -> String {
    names::fleets::placed(names::fleets::MEMORY, &[LARGE_FLEET])
}

/// The check that a request for the noop sandbox scales out the small fleet, which runs under noop
/// too.
fn noop_placed_native() -> String {
    names::fleets::placed(names::fleets::NOOP_SANDBOX, &[SMALL_FLEET])
}

/// The check that a request for the noop sandbox scales out the noop fleet only.
fn noop_placed() -> String {
    names::fleets::placed(names::fleets::NOOP_SANDBOX, &[NOOP_FLEET])
}

/// The check that a concurrent pair scales out the small fleet, then the large one.
fn overflow_placed() -> String {
    names::fleets::placed(names::fleets::OVERFLOW, &[SMALL_FLEET, LARGE_FLEET])
}

/// The check that an action of the noop fleet runs in the host's namespaces.
fn noop_unisolated() -> String {
    names::fleets::labelled(
        names::fleets::NOOP_SANDBOX,
        names::isolation::NOOP_FLEET_UNISOLATED,
    )
}

/// The check that an unconstrained action gets its own net namespace.
fn unconstrained_net() -> String {
    names::fleets::labelled(
        names::fleets::UNCONSTRAINED,
        names::isolation::NET_NAMESPACE,
    )
}

/// The check that an action pinned to the large fleet gets its own net namespace.
fn pinned_net() -> String {
    names::fleets::labelled(names::fleets::PINNED, names::isolation::NET_NAMESPACE)
}

/// The check that an action asking for more memory gets its own net namespace.
fn memory_net() -> String {
    names::fleets::labelled(names::fleets::MEMORY, names::isolation::NET_NAMESPACE)
}

/// The check that every worker drained after an unconstrained action is recorded as terminated.
fn unconstrained_terminated() -> String {
    names::fleets::terminated(names::fleets::UNCONSTRAINED)
}

/// The requests of the steps of the fleets scenario.
const REQUESTS: [&str; 5] = [
    names::fleets::UNCONSTRAINED,
    names::fleets::PINNED,
    names::fleets::MEMORY,
    names::fleets::NOOP_SANDBOX,
    names::fleets::OVERFLOW,
];

/// The names of the checks that did not pass.
fn failures(checks: &[Check]) -> Vec<String> {
    checks
        .iter()
        .filter(|check| check.is_failure())
        .map(|check| check.name.to_string())
        .collect()
}

/// Asserts that every check passes.
fn assert_compliant(checks: &[Check]) {
    assert_eq!(failures(checks), Vec::<&str>::new());
}

/// Asserts that the check called `name` fails.
fn assert_fails(checks: &[Check], name: impl AsRef<str>) {
    let name = name.as_ref();
    assert!(
        failures(checks).iter().any(|failed| failed == name),
        "`{name}` did not fail; failures: {:?}",
        failures(checks)
    );
}

/// Asserts that the check called `name` does not fail.
fn assert_holds(checks: &[Check], name: impl AsRef<str>) {
    let name = name.as_ref();
    assert!(
        !failures(checks).iter().any(|failed| failed == name),
        "`{name}` failed: {:?}",
        failures(checks)
    );
}

/// A variant with no limits and the given sandbox, the baseline for scenarios that do not vary
/// them.
fn baseline(sandbox: Sandbox) -> Variant {
    Variant::baseline(sandbox)
}

/// The variant the limits scenario runs under: the workers enforce cgroup limits and
/// `NativeLink`'s soft memory enforcement is off, so only the kernel can end the memory hog. The
/// suite runs it under the no-op sandbox, because the cgroup is what ends the orphan there; under
/// bwrap the PID namespace teardown would kill the orphan anyway.
fn limited() -> Variant {
    Variant {
        sandbox: Sandbox::Noop,
        limits: ActionLimits::Cgroup,
        soft_memory: SoftMemory::Off,
        ..Variant::baseline(Sandbox::Noop)
    }
}

/// The verdicts of the lifecycle scenario against a simulation that behaves as `behaviour` says.
fn lifecycle_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let variant = baseline(Sandbox::Bwrap);
    lifecycle(&Simulation::new(variant, behaviour), &variant)
}

/// The verdicts of the input integrity scenario against a simulation that behaves as `behaviour`
/// says.
fn integrity_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let variant = baseline(Sandbox::Bwrap);
    input_integrity(&Simulation::new(variant, behaviour), &variant)
}

/// The verdicts of the restart scenario against a simulation that behaves as `behaviour` says and
/// boots slowly.
fn restart_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let variant = baseline(Sandbox::Bwrap);
    restart(&Simulation::new(variant, behaviour).with_slow_boot())
}

/// The verdicts of the revocation scenario against an identity provider that revokes as
/// `enforcement` says.
fn revocation_checks(enforcement: Revocation) -> gang_e2e::Result<Vec<Check>> {
    revocation(&SimulatedIdentity::new(enforcement))
}

/// The verdicts of the revocation scenario against an identity deployment that enforces
/// revocation and otherwise behaves as `behaviour` says.
fn identity_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    revocation(&SimulatedIdentity::new(Revocation::Enforced).with_behaviour(behaviour))
}

/// The verdicts of the limits scenario against a simulation that behaves as `behaviour` says.
fn limits_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let variant = limited();
    let system = Simulation::new(variant, behaviour);
    limits(&system, system.stderr_tail_lines())
}

/// Declares a test that runs a scenario against a simulated flaw and judges the checks it emits.
///
/// `$run` takes the flaw: either a `Behaviour`, or `field = value` for a behaviour that is
/// compliant but for that field. The verdict is one of: `only [..]`, the failing checks are
/// exactly these; `fails [..]`, these checks fail, optionally followed by `holds [..]`, these
/// checks pass or are skipped.
macro_rules! flaw {
    ($test:ident: $run:ident($field:ident = $value:expr) => $($verdict:tt)+) => {
        flaw!($test: $run(Behaviour { $field: $value, ..Behaviour::COMPLIANT }) => $($verdict)+);
    };
    ($test:ident: $run:ident($flaw:expr) => only [$($failing:expr),+ $(,)?]) => {
        #[test]
        fn $test() -> gang_e2e::Result<()> {
            assert_eq!(failures(&$run($flaw)?), [$($failing),+]);
            Ok(())
        }
    };
    (
        $test:ident: $run:ident($flaw:expr)
        => fails [$($failing:expr),+ $(,)?] $(, holds [$($holding:expr),+ $(,)?])?
    ) => {
        #[test]
        fn $test() -> gang_e2e::Result<()> {
            let checks = $run($flaw)?;
            $(assert_fails(&checks, $failing);)+
            $($(assert_holds(&checks, $holding);)+)?
            Ok(())
        }
    };
}
mod identity;

mod server;

mod checked_in;
mod cli;
mod control;

mod execution;
#[test]
fn lifecycle_is_compliant_under_bwrap() {
    let variant = baseline(Sandbox::Bwrap);
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    assert_compliant(&lifecycle(&system, &variant).expect("the lifecycle runs"));
}

#[test]
fn lifecycle_is_compliant_under_noop() {
    let variant = baseline(Sandbox::Noop);
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    assert_compliant(&lifecycle(&system, &variant).expect("the lifecycle runs"));
}

#[test]
fn noop_probe_lists_the_parent_directories() {
    let variant = baseline(Sandbox::Noop);
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    let checks = lifecycle(&system, &variant).expect("the lifecycle runs");
    let listed = names::isolation::SIBLINGS_LISTED;
    assert!(
        checks.iter().any(|check| check.name == listed),
        "`{listed}` was not emitted"
    );
    assert_holds(&checks, listed);
}

#[test]
fn lifecycle_with_an_early_scale_out_is_compliant() {
    let variant = baseline(Sandbox::Bwrap);
    let system = Simulation::new(variant, Behaviour::COMPLIANT).with_early_scale_out();
    assert_compliant(&lifecycle(&system, &variant).expect("the lifecycle runs"));
}

flaw! {
    unrecorded_termination_fails_the_drain_check:
        lifecycle_checks(termination = Termination::Unrecorded)
        => fails [names::lifecycle::EVERY_DRAIN_TERMINATED]
}

flaw! {
    warm_server_fails_every_check_that_counts_from_zero:
        lifecycle_checks(warm = Warm::Preloaded)
        => only [
            names::lifecycle::STARTS_AT_ZERO,
            names::scaling::FIRST_SCALE_OUT_FROM_ZERO,
            names::lifecycle::IDLE_DRAIN,
            names::lifecycle::WORKER_IDS,
            names::lifecycle::CACHE_HIT_NEEDS_NO_WORKER
        ]
}

flaw! {
    stalled_drain_fails_the_idle_drain_check:
        lifecycle_checks(drain = Drain::Stalls)
        => only [names::lifecycle::IDLE_DRAIN]
}

#[test]
fn network_is_compliant_when_isolated() {
    let variant = baseline(Sandbox::Bwrap);
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    assert_compliant(&network(&system, &variant).expect("the network scenario runs"));
}

flaw! {
    leaky_sandbox_fails_namespace_and_sibling_checks_under_bwrap:
        lifecycle_checks(isolation = Isolation::Leaky)
        => fails [
            names::isolation::NET_NAMESPACE,
            names::isolation::MNT_NAMESPACE,
            names::isolation::SIBLINGS_HIDDEN
        ]
}

flaw! {
    unreadable_namespaces_fail_the_namespace_checks_under_bwrap:
        lifecycle_checks(isolation = Isolation::Unreadable)
        => fails [names::isolation::NET_NAMESPACE, names::isolation::MNT_NAMESPACE]
}

flaw! {
    parent_directory_leak_fails_only_the_siblings_check_under_bwrap:
        lifecycle_checks(isolation = Isolation::LeakyParent)
        => fails [names::isolation::SIBLINGS_HIDDEN],
        holds [names::isolation::NET_NAMESPACE, names::isolation::MNT_NAMESPACE]
}

flaw! {
    great_grandparent_directory_leak_fails_the_siblings_check_under_bwrap:
        lifecycle_checks(isolation = Isolation::LeakyGreatGrandparent)
        => fails [names::isolation::SIBLINGS_HIDDEN],
        holds [names::isolation::NET_NAMESPACE, names::isolation::MNT_NAMESPACE]
}

#[test]
fn integrity_is_compliant_under_break_and_keep() {
    for links in [InputLinks::Break, InputLinks::Keep] {
        let variant = Variant {
            input_links: links,
            ..baseline(Sandbox::Bwrap)
        };
        let system = Simulation::new(variant, Behaviour::COMPLIANT);
        assert_compliant(&input_integrity(&system, &variant).expect("the integrity scenario runs"));
    }
}

flaw! {
    poisoned_inputs_fail_the_integrity_check:
        integrity_checks(integrity = Integrity::Poisoned)
        => fails [names::integrity::NO_ALTERATION]
}

flaw! {
    refused_poison_attempt_fails_the_poisoning_check:
        integrity_checks(integrity = Integrity::Refused)
        => fails [names::integrity::POISON_ATTEMPTED]
}

flaw! {
    a_cache_miss_after_clean_fails_the_cache_check:
        lifecycle_checks(cache = Cache::Miss)
        => fails [names::build::CACHE_HIT]
}

#[test]
fn restart_converges_when_the_server_does_not_repeat_the_scale_out() {
    let variant = baseline(Sandbox::Bwrap);
    let system = Simulation::new(variant, Behaviour::COMPLIANT).with_slow_boot();
    assert_compliant(&restart(&system).expect("the restart scenario runs"));
}

flaw! {
    restart_that_repeats_the_scale_out_fails_the_restart_check:
        restart_checks(restart = Restart::Repeats)
        => fails [names::restart::SCALE_OUT_NOT_REPEATED]
}

flaw! {
    reused_pid_fails_the_restarted_check:
        restart_checks(pid = PidReuse::Reused)
        => only [names::restart::SERVER_RESTARTED]
}

flaw! {
    restart_that_leaves_a_second_instance_fails_the_converged_check:
        restart_checks(convergence = Convergence::Stuck)
        => only [names::restart::CONVERGED]
}

flaw! {
    hold_without_a_scope_fails_the_scope_check_of_the_restart:
        restart_checks(scope_report = ScopeReport::Missing)
        => only [names::limits::HOLD_HAS_SCOPE]
}

flaw! {
    a_hold_that_never_signals_fails_the_signal_check_of_the_restart:
        restart_checks(hold_signal = HoldSignal::Missing)
        => only [names::limits::HOLD_SIGNALLED]
}

#[test]
fn revocation_is_compliant_when_enforced() {
    let identity = SimulatedIdentity::new(Revocation::Enforced);
    assert_compliant(&revocation(&identity).expect("the revocation scenario runs"));
}

flaw! {
    revocation_ignored_fails_the_denial_refusal_and_build_checks:
        revocation_checks(Revocation::Ignored)
        => fails [
            names::revocation::OPEN_CONNECTION_DENIED,
            names::revocation::NEW_CONNECTION_DENIED,
            names::revocation::REVOKED_BUILD
        ]
}

flaw! {
    revocation_by_restart_fails_only_the_no_restart_check:
        revocation_checks(Revocation::RestartedOnRevoke)
        => fails [names::revocation::NO_RESTART],
        holds [names::revocation::OPEN_CONNECTION_DENIED]
}

flaw! {
    connection_closed_before_revocation_fails_the_open_check:
        revocation_checks(Revocation::EarlyClose)
        => fails [names::revocation::CONNECTION_OPEN]
}

flaw! {
    a_build_failing_for_another_reason_fails_the_rejection_check:
        revocation_checks(Revocation::BuildFailsElsewhere)
        => fails [names::revocation::REVOKED_BUILD]
}

flaw! {
    restore_ignored_fails_the_restore_checks:
        revocation_checks(Revocation::RestoreIgnored)
        => fails [names::revocation::RESTORED_CONNECTION, names::revocation::RESTORED_BUILD]
}

flaw! {
    refused_token_fails_the_issue_check:
        identity_checks(token_issue = TokenIssue::Refuses)
        => only [names::revocation::TOKEN_ISSUED]
}

flaw! {
    refused_handshake_fails_the_connection_check:
        identity_checks(handshake = Handshake::Refuses)
        => only [names::revocation::CONNECTION_ACCEPTED]
}

flaw! {
    denied_requests_fail_the_served_check:
        identity_checks(requests = Requests::Denied)
        => only [names::revocation::SERVED_BEFORE, names::revocation::RESTORED_CONNECTION]
}

flaw! {
    served_anonymous_request_fails_the_anonymous_check:
        identity_checks(anonymous = Anonymous::Served)
        => only [names::revocation::ANONYMOUS_REQUEST]
}

#[test]
fn plaintext_is_compliant_under_noop() {
    let variant = baseline(Sandbox::Noop);
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    assert_compliant(&plaintext(&system).expect("the plaintext scenario runs"));
}

/// The verdicts of the plaintext scenario against a simulation that behaves as `behaviour` says.
fn plaintext_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let system = Simulation::new(baseline(Sandbox::Noop), behaviour);
    plaintext(&system)
}

flaw! {
    plaintext_with_refused_admin_routes_fails_the_admin_check:
        plaintext_checks(admin_routes = AdminRoutes::Refused)
        => only [names::plaintext::ADMIN_ADMITTED]
}

#[test]
fn plaintext_judges_the_state_before_after_and_drain() -> gang_e2e::Result<()> {
    let checks = plaintext_checks(Behaviour::COMPLIANT)?;
    for name in [
        names::plaintext::STATE_IDLE,
        names::plaintext::STATE_SCALED_OUT,
        names::plaintext::COMPLETED_INCREASES,
        names::plaintext::STATE_DRAINED,
        names::plaintext::DRAIN_STREAMED,
        names::stream::STARTS_WITH_SNAPSHOT,
        names::stream::APPLIES,
        names::stream::CAUSE_DEMAND,
        names::stream::CAUSE_IDLE_TIMEOUT,
        names::stream::CAUSE_DRAIN_FINISHED,
        names::plaintext::ADMIN_ADMITTED,
    ] {
        assert_eq!(outcome_of(&checks, name), Some(&Outcome::Passed), "{name}");
    }
    Ok(())
}

flaw! {
    a_stream_without_causes_fails_the_cause_checks:
        plaintext_checks(stream_causes = StreamCauses::Missing)
        => only [
            names::stream::CAUSE_DEMAND,
            names::stream::CAUSE_IDLE_TIMEOUT,
            names::stream::CAUSE_DRAIN_FINISHED
        ]
}

flaw! {
    a_completed_count_that_does_not_rise_fails_only_the_completed_check:
        plaintext_checks(completed = Completed::Frozen)
        => only [names::plaintext::COMPLETED_INCREASES]
}

flaw! {
    a_state_that_does_not_follow_the_fleet_fails_the_scale_out_check:
        plaintext_checks(state_view = StateView::Blind)
        => only [names::plaintext::STATE_SCALED_OUT]
}

flaw! {
    a_stream_that_never_records_the_termination_fails_the_drain_check:
        plaintext_checks(termination = Termination::Unrecorded)
        => fails [names::plaintext::DRAIN_STREAMED],
        holds [names::plaintext::STATE_DRAINED, names::plaintext::ADMIN_ADMITTED]
}

/// The verdicts of the dashboard scenario against `identity`, within the suite's history timeout.
fn dashboard_of<I: Identity>(identity: &I) -> gang_e2e::Result<Vec<Check>> {
    gang_e2e::dashboard(identity, Settings::default().history_timeout)
}

/// The verdicts of the dashboard scenario against a simulation that serves it as `dashboard` says.
fn dashboard_checks(revocation: Revocation, dashboard: Dashboard) -> gang_e2e::Result<Vec<Check>> {
    let identity = SimulatedIdentity::new(revocation).with_dashboard(dashboard);
    dashboard_of(&identity)
}

#[test]
fn the_dashboard_is_compliant_when_it_serves_as_required() -> gang_e2e::Result<()> {
    assert_compliant(&dashboard_checks(
        Revocation::Enforced,
        Dashboard::Compliant,
    )?);
    Ok(())
}

#[test]
fn the_dashboard_judges_every_step() -> gang_e2e::Result<()> {
    let checks = dashboard_checks(Revocation::Enforced, Dashboard::Compliant)?;
    for name in [
        cookieless_state(),
        cookieless_stream(),
        names::dashboard::STREAM_OPENED.to_owned(),
        names::stream::STARTS_WITH_SNAPSHOT.to_owned(),
        names::dashboard::CLOSED_ON_REVOCATION.to_owned(),
    ] {
        assert_eq!(outcome_of(&checks, &name), Some(&Outcome::Passed), "{name}");
    }
    Ok(())
}

/// The verdicts of the dashboard scenario against a simulation that serves it as `dashboard` says
/// and enforces revocation.
fn dashboard_serving(dashboard: Dashboard) -> gang_e2e::Result<Vec<Check>> {
    dashboard_checks(Revocation::Enforced, dashboard)
}

/// The verdicts of the dashboard scenario against a simulation that serves it as required and
/// revokes as `revocation` says.
fn dashboard_revoking(revocation: Revocation) -> gang_e2e::Result<Vec<Check>> {
    dashboard_checks(revocation, Dashboard::Compliant)
}

flaw! {
    a_dashboard_open_to_anonymous_callers_fails_the_cookieless_checks:
        dashboard_serving(Dashboard::OpenToAnonymous)
        => only [cookieless_state(), cookieless_stream()]
}

flaw! {
    a_stream_that_stays_open_after_revocation_fails_the_closed_check:
        dashboard_serving(Dashboard::NeverCloses)
        => only [names::dashboard::CLOSED_ON_REVOCATION]
}

flaw! {
    a_stream_closed_for_another_reason_fails_the_closed_check:
        dashboard_serving(Dashboard::ClosesAsExpired)
        => only [names::dashboard::CLOSED_ON_REVOCATION]
}

flaw! {
    a_revocation_the_server_ignores_fails_the_closed_check:
        dashboard_revoking(Revocation::Ignored)
        => only [names::dashboard::CLOSED_ON_REVOCATION]
}

flaw! {
    a_response_that_is_not_an_event_stream_fails_the_opened_check:
        dashboard_serving(Dashboard::NotAnEventStream)
        => only [names::dashboard::STREAM_OPENED]
}

flaw! {
    a_stream_that_does_not_start_with_a_snapshot_fails_the_first_event_check:
        dashboard_serving(Dashboard::NoSnapshot)
        => only [names::stream::STARTS_WITH_SNAPSHOT]
}

#[test]
fn a_closed_check_passes_when_ticks_precede_the_closed_event() -> gang_e2e::Result<()> {
    let checks = dashboard_serving(Dashboard::TicksBeforeClosed)?;
    assert_compliant(&checks);
    Ok(())
}

#[test]
fn a_stream_that_ends_without_closed_fails_the_closed_check() -> gang_e2e::Result<()> {
    let checks = dashboard_serving(Dashboard::EndsBeforeClosed)?;
    match outcome_of(&checks, names::dashboard::CLOSED_ON_REVOCATION) {
        Some(Outcome::Failed(reason)) => assert!(reason.contains("ended"), "{reason}"),
        other => panic!("the closed check should fail, got {other:?}"),
    }
    assert_eq!(failures(&checks), [names::dashboard::CLOSED_ON_REVOCATION]);
    Ok(())
}

#[test]
fn limits_are_compliant_when_enforced() {
    let variant = limited();
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    let checks = limits(&system, system.stderr_tail_lines());
    assert_compliant(&checks.expect("the limits scenario runs"));
}

flaw! {
    hold_without_its_orphan_fails_the_orphan_check:
        limits_checks(orphan = OrphanProcess::NotSpawned)
        => only [names::limits::ORPHAN_RUNS]
}

flaw! {
    unlimited_actions_leave_their_orphan_alive:
        limits_checks(limits = Limits::Unlimited)
        => fails [names::limits::ORPHAN_KILLED]
}

flaw! {
    a_hog_that_the_kernel_does_not_kill_fails_the_hard_ceiling_check:
        limits_checks(limits = Limits::Unlimited)
        => fails [names::limits::HARD_CEILING]
}

flaw! {
    a_pinned_build_that_fails_fails_only_the_limits_serving_check:
        limits_checks(serving = Serving::Stops)
        => only [names::memory::SERVING_AFTER_KILL]
}

flaw! {
    a_hold_action_that_never_starts_fails_the_signal_check:
        limits_checks(hold = HoldBuild::FailsEarly)
        => only [names::limits::HOLD_SIGNALLED]
}

#[test]
fn an_early_hold_failure_reports_abandonment() {
    let checks = limits_checks(Behaviour {
        hold: HoldBuild::FailsEarly,
        ..Behaviour::COMPLIANT
    })
    .expect("the limits scenario runs");
    assert!(checks.iter().any(|check| {
        check.name == names::limits::HOLD_SIGNALLED
            && check.outcome
                == Outcome::Failed("the hold build ended before its orphan signalled".to_owned())
    }));
}

#[test]
fn abandoning_a_wait_does_not_end_the_held_build() -> gang_e2e::Result<()> {
    let variant = limited();
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    system.start_server()?;
    std::thread::scope(|scope| -> gang_e2e::Result<()> {
        let build = scope.spawn(|| system.build(&[Probe::Hold]));
        assert_eq!(system.await_hold()?, Signalled::Started);
        system.abandon_hold()?;
        assert_eq!(system.await_hold()?, Signalled::Abandoned);
        assert!(!build.is_finished());
        let facts = system.running_scope()?.expect("the held build has a scope");
        system.release(&facts)?;
        assert!(
            build
                .join()
                .expect("the build thread does not panic")?
                .succeeded()
        );
        Ok(())
    })
}

#[test]
fn held_build_releases_go_after_during_returns() -> gang_e2e::Result<()> {
    use execution::sim::{Behaviour as ExecBehaviour, ExecSim};
    use gang_e2e::{Execution as _, held_build};

    let system = ExecSim::new(baseline(Sandbox::Noop), ExecBehaviour::COMPLIANT);
    system.start_server()?;
    system.set_go(false)?;
    held_build(&system, &system, &[Probe::TimeoutChild], |_| Ok(()))?;
    assert!(system.go());
    Ok(())
}

flaw! {
    a_hold_that_never_signals_fails_the_signal_check_of_the_limits:
        limits_checks(hold_signal = HoldSignal::Missing)
        => only [names::limits::HOLD_SIGNALLED]
}

/// The verdicts of the disk scenario against a simulation that behaves as `behaviour` says.
fn disk_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let variant = baseline(Sandbox::Noop);
    let system = Simulation::new(variant, behaviour);
    disk(&system, system.stderr_tail_lines(), true)
}

#[test]
fn disk_is_compliant_when_the_hog_is_killed() -> gang_e2e::Result<()> {
    let checks = disk_checks(Behaviour::COMPLIANT)?;
    assert_compliant(&checks);
    assert_holds(&checks, names::scaling::FIRST_SCALE_OUT_FROM_ZERO);
    Ok(())
}

flaw! {
    a_hog_that_is_not_killed_fails_the_kill_check:
        disk_checks(disk = Disk::Ignored)
        => fails [names::disk::KILLED_WITH_MESSAGE]
}

flaw! {
    a_failing_neighbour_fails_its_check:
        disk_checks(neighbour = Neighbour::Fails)
        => fails [names::disk::NEIGHBOUR_FINISHED]
}

#[test]
fn two_scale_outs_fail_the_shared_worker_check() -> gang_e2e::Result<()> {
    let variant = baseline(Sandbox::Noop);
    let system = Simulation::new(variant, Behaviour::COMPLIANT).with_early_scale_out();
    let checks = disk(&system, system.stderr_tail_lines(), true)?;
    assert_fails(&checks, names::disk::ONE_WORKER);
    Ok(())
}

flaw! {
    a_pinned_build_that_fails_fails_the_serving_check:
        disk_checks(serving = Serving::Stops)
        => fails [names::disk::SERVING_AFTER_KILL]
}

#[test]
fn disk_without_the_sampler_skips_the_kill_checks() -> gang_e2e::Result<()> {
    let variant = baseline(Sandbox::Noop);
    let system = Simulation::new(variant, Behaviour::COMPLIANT);
    let checks = disk(&system, system.stderr_tail_lines(), false)?;
    assert_compliant(&checks);
    Ok(())
}

/// The variant the memory scenario runs under: the workers enforce cgroup limits and
/// `NativeLink`'s soft memory enforcement stays on.
fn soft_limited() -> Variant {
    Variant {
        sandbox: Sandbox::Noop,
        limits: ActionLimits::Cgroup,
        ..Variant::baseline(Sandbox::Noop)
    }
}

/// The verdicts of the memory scenario against a simulation that behaves as `behaviour` says.
fn memory_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let system = Simulation::new(soft_limited(), behaviour);
    memory(&system, system.stderr_tail_lines())
}

#[test]
fn memory_is_compliant_when_the_grower_is_requeued_once() -> gang_e2e::Result<()> {
    assert_compliant(&memory_checks(Behaviour::COMPLIANT)?);
    Ok(())
}

flaw! {
    a_grower_that_is_never_killed_fails_the_soft_check:
        memory_checks(grower = Grower::NeverKilled)
        => fails [names::memory::SOFT_REQUEUE]
}

flaw! {
    a_grower_killed_after_its_requeue_fails_the_soft_check:
        memory_checks(grower = Grower::KilledAgain)
        => fails [names::memory::SOFT_REQUEUE]
}

flaw! {
    a_grower_that_fails_on_its_requeue_fails_the_soft_check:
        memory_checks(grower = Grower::Failed)
        => fails [names::memory::SOFT_REQUEUE]
}

flaw! {
    a_pinned_build_that_fails_fails_only_the_memory_serving_check:
        memory_checks(serving = Serving::Stops)
        => only [names::memory::SERVING_AFTER_KILL]
}

/// The variant the fleets scenario runs under: three fleets, the first under `sandbox`.
fn placement(sandbox: Sandbox) -> Variant {
    Variant {
        fleets: FleetLayout::Placement,
        ..Variant::baseline(sandbox)
    }
}

/// The verdicts of the fleets scenario against a simulation that behaves as `behaviour` says.
fn fleets_checks(sandbox: Sandbox, behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let variant = placement(sandbox);
    let system = Simulation::new(variant, behaviour);
    fleets(&system, &variant)
}

/// The verdicts of the fleets scenario under bwrap against a simulation that behaves as
/// `behaviour` says.
fn bwrap_fleets_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    fleets_checks(Sandbox::Bwrap, behaviour)
}

/// The outcome of the check called `name`, when it was emitted.
fn outcome_of(checks: &[Check], name: impl AsRef<str>) -> Option<&Outcome> {
    checks
        .iter()
        .find(|check| check.name == name.as_ref())
        .map(|check| &check.outcome)
}

#[test]
fn fleets_is_compliant_under_bwrap() -> gang_e2e::Result<()> {
    assert_compliant(&fleets_checks(Sandbox::Bwrap, Behaviour::COMPLIANT)?);
    Ok(())
}

#[test]
fn fleets_is_compliant_under_noop() -> gang_e2e::Result<()> {
    assert_compliant(&fleets_checks(Sandbox::Noop, Behaviour::COMPLIANT)?);
    Ok(())
}

#[test]
fn fleets_judges_the_placement_of_every_step() -> gang_e2e::Result<()> {
    let checks = fleets_checks(Sandbox::Bwrap, Behaviour::COMPLIANT)?;
    for name in [
        unconstrained_placed(),
        pinned_placed(),
        memory_placed(),
        noop_placed(),
        overflow_placed(),
    ] {
        assert_eq!(outcome_of(&checks, &name), Some(&Outcome::Passed), "{name}");
    }
    Ok(())
}

#[test]
fn fleets_shows_the_noop_fleet_unisolated_under_bwrap() -> gang_e2e::Result<()> {
    let checks = fleets_checks(Sandbox::Bwrap, Behaviour::COMPLIANT)?;
    for name in [
        noop_unisolated(),
        unconstrained_net(),
        pinned_net(),
        memory_net(),
    ] {
        assert_eq!(outcome_of(&checks, &name), Some(&Outcome::Passed), "{name}");
    }
    Ok(())
}

#[test]
fn fleets_asserts_only_placement_when_the_native_sandbox_is_noop() -> gang_e2e::Result<()> {
    let checks = fleets_checks(Sandbox::Noop, Behaviour::COMPLIANT)?;
    assert!(matches!(
        outcome_of(&checks, noop_unisolated()),
        Some(Outcome::Skipped(_))
    ));
    // Every fleet runs under noop, so the first fleet fits a request for the noop sandbox.
    assert_eq!(
        outcome_of(&checks, noop_placed_native()),
        Some(&Outcome::Passed)
    );
    Ok(())
}

flaw! {
    actions_that_ignore_exec_properties_fail_every_pinned_placement:
        bwrap_fleets_checks(routing = Routing::IgnoresProperties)
        => fails [
            pinned_placed(),
            memory_placed(),
            noop_placed(),
            overflow_placed(),
            noop_unisolated()
        ],
        holds [unconstrained_placed()]
}

flaw! {
    a_first_fleet_that_never_overflows_fails_only_the_overflow_placement:
        bwrap_fleets_checks(routing = Routing::NoOverflow)
        => only [overflow_placed()]
}

flaw! {
    reversed_preference_fails_the_first_fleet_and_the_overflow_placements:
        bwrap_fleets_checks(routing = Routing::ReversedPreference)
        => fails [unconstrained_placed(), overflow_placed()],
        holds [pinned_placed(), memory_placed(), noop_placed()]
}

flaw! {
    a_leaky_sandbox_fails_the_native_fleets_namespace_checks_under_bwrap:
        bwrap_fleets_checks(isolation = Isolation::Leaky)
        => fails [unconstrained_net(), pinned_net(), memory_net()],
        holds [unconstrained_placed(), noop_unisolated()]
}

flaw! {
    unrecorded_terminations_fail_the_drain_check_of_every_step:
        bwrap_fleets_checks(termination = Termination::Unrecorded)
        => fails [unconstrained_terminated()],
        holds [unconstrained_placed()]
}

#[test]
fn a_fleets_step_whose_build_fails_before_any_scale_out_is_judged_without_waiting()
-> gang_e2e::Result<()> {
    let behaviour = Behaviour {
        build: BuildFailure::BeforeScaleOut,
        ..Behaviour::COMPLIANT
    };
    let checks = fleets_checks(Sandbox::Bwrap, behaviour)?;

    let drained = REQUESTS.map(names::fleets::drained);
    let drains: Vec<&Check> = checks
        .iter()
        .filter(|check| drained.iter().any(|name| check.name == name.as_str()))
        .collect();
    assert!(!drains.is_empty(), "no drain check was emitted");
    for check in drains {
        assert!(
            !matches!(check.outcome, Outcome::Failed(_)),
            "{}: {:?}",
            check.name,
            check.outcome
        );
    }

    let remote =
        REQUESTS.map(|request| names::fleets::labelled(request, names::build::REMOTE_EXECUTION));
    let remote: Vec<&Check> = checks
        .iter()
        .filter(|check| remote.iter().any(|name| check.name == name.as_str()))
        .collect();
    assert!(!remote.is_empty(), "no remote execution check was emitted");
    for check in remote {
        assert!(
            matches!(check.outcome, Outcome::Failed(_)),
            "{}: {:?}",
            check.name,
            check.outcome
        );
    }
    Ok(())
}
