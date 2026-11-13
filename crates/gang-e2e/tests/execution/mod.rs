//! The execution scenarios against [`sim::ExecSim`]: a compliant simulation passes every check, and each flaw fails
//! the checks it breaks.

pub(super) mod sim;

use gang_e2e::{
    ActionLimits, Check, ExecutionKnobs, Sandbox, SoftMemory, System as _, Variant, action_timeout,
    cold_start_misfit, exit_signals, memory_escalation, names, oversized_fleet, preemption,
    sandbox_contract, stale_scope, unplaceable_disk, worker_renewal,
};
use sim::*;

use super::{assert_compliant, failures};

fn scoped(sandbox: Sandbox, soft_memory: SoftMemory, execution: ExecutionKnobs) -> Variant {
    Variant {
        sandbox,
        limits: ActionLimits::Cgroup,
        soft_memory,
        execution,
        ..Variant::baseline(sandbox)
    }
}

/// The bwrap baseline under the execution knobs `execution`.
fn knob(execution: ExecutionKnobs) -> Variant {
    Variant {
        execution,
        ..Variant::baseline(Sandbox::Bwrap)
    }
}

/// Runs the action-timeout scenario using bwrap.
fn bwrap_timeout(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(
        scoped(
            Sandbox::Bwrap,
            SoftMemory::Enforced,
            ExecutionKnobs::SHORT_TIMEOUT,
        ),
        behaviour,
    );
    action_timeout(&sim, &sim, Sandbox::Bwrap)
}

/// Runs the action-timeout scenario using noop and checks exit signals.
fn noop_timeout(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(
        scoped(
            Sandbox::Noop,
            SoftMemory::Enforced,
            ExecutionKnobs::SHORT_TIMEOUT,
        ),
        behaviour,
    );
    let mut checks = action_timeout(&sim, &sim, Sandbox::Noop)?;
    checks.extend(exit_signals(&sim, sim.stderr_tail_lines())?);
    Ok(checks)
}

/// Runs the sandbox contract scenario.
fn contract(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(
        scoped(Sandbox::Bwrap, SoftMemory::Off, ExecutionKnobs::NONE),
        behaviour,
    );
    sandbox_contract(&sim, &sim, sim.stderr_tail_lines())
}

/// Runs the stale-scope scenario.
fn stale(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(
        scoped(Sandbox::Noop, SoftMemory::Off, ExecutionKnobs::NONE),
        behaviour,
    );
    stale_scope(
        &sim,
        &sim,
        sim.stderr_tail_lines(),
        gang_e2e::Settings::default().hold_timeout,
    )
}

/// Runs the memory-escalation scenario.
fn escalation(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(
        scoped(
            Sandbox::Noop,
            SoftMemory::Enforced,
            ExecutionKnobs::ONE_STEP_ESCALATION,
        ),
        behaviour,
    );
    memory_escalation(&sim, sim.stderr_tail_lines())
}

/// Runs the unplaceable disk scenario.
fn unplaceable(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(Variant::baseline(Sandbox::Bwrap), behaviour);
    unplaceable_disk(&sim, &sim, sim.stderr_tail_lines())
}

/// Runs the oversized fleet disk scenario.
fn oversized(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(knob(ExecutionKnobs::OVERSIZED_FLEET), behaviour);
    oversized_fleet(&sim, &sim)
}

/// Runs the cold-start misfit scenario.
fn cold(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(knob(ExecutionKnobs::COLD_START_MISFIT), behaviour);
    cold_start_misfit(&sim)
}

/// Runs the preemption scenario.
fn preempted(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(Variant::baseline(Sandbox::Bwrap), behaviour);
    preemption(&sim, &sim)
}

/// Runs the worker-renewal scenario.
fn renewal_run(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    let sim = ExecSim::new(knob(ExecutionKnobs::SHORT_LIVED_CERTIFICATES), behaviour);
    worker_renewal(&sim, &sim)
}

#[test]
fn bwrap_timeout_is_compliant() {
    assert_compliant(&bwrap_timeout(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn noop_timeout_is_compliant() {
    assert_compliant(&noop_timeout(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn contract_is_compliant() {
    assert_compliant(&contract(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn stale_is_compliant() {
    assert_compliant(&stale(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn stale_is_compliant_when_local_mode_retires_a_killed_worker_at_once() {
    assert_compliant(
        &stale(Behaviour {
            retire: Retire::AtDeath,
            ..Behaviour::COMPLIANT
        })
        .expect("the scenario runs"),
    );
}

#[test]
fn stale_is_compliant_when_a_killed_exec_is_retried() {
    assert_compliant(
        &stale(Behaviour {
            exec_retry: ExecRetry::Retried,
            ..Behaviour::COMPLIANT
        })
        .expect("the scenario runs"),
    );
}

#[test]
fn stale_is_compliant_when_a_stray_line_arrives_late() {
    assert_compliant(
        &stale(Behaviour {
            late_line: LateLine::Late,
            ..Behaviour::COMPLIANT
        })
        .expect("the scenario runs"),
    );
}

#[test]
fn escalation_is_compliant() {
    assert_compliant(&escalation(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn unplaceable_is_compliant() {
    assert_compliant(&unplaceable(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn oversized_is_compliant() {
    assert_compliant(&oversized(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn cold_is_compliant() {
    assert_compliant(&cold(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn preempted_is_compliant() {
    assert_compliant(&preempted(Behaviour::COMPLIANT).expect("the scenario runs"));
}

#[test]
fn renewal_run_is_compliant() {
    assert_compliant(&renewal_run(Behaviour::COMPLIANT).expect("the scenario runs"));
}
flaw! { noop_timeout_timeout_child_never_starts: noop_timeout(timeout_child = TimeoutChild::NeverStarts) => only [names::action_timeout::CHILD_STARTED, names::action_timeout::CHILD_GONE] }
flaw! { noop_timeout_timeout_succeeds: noop_timeout(timeout = Timeout::Succeeds) => only [names::action_timeout::TIMEOUT_FAILS] }
flaw! { noop_timeout_child_survives: noop_timeout(child = Child::Survives) => only [names::action_timeout::CHILD_GONE] }
flaw! { noop_timeout_term_withheld: noop_timeout(term = Term::Withheld) => only [names::action_timeout::TRAP_RAN, names::action_timeout::EXEC_WAITS, names::action_timeout::TRAP_EXITS_CLEANLY, names::action_timeout::IGNORER_SURVIVES_GRACE] }
flaw! { noop_timeout_exec_wait_exits_first: noop_timeout(exec_wait = ExecWait::ExitsFirst) => only [names::action_timeout::EXEC_WAITS] }
flaw! { noop_timeout_grace_killed_early: noop_timeout(grace = Grace::KilledEarly) => only [names::action_timeout::TRAP_EXITS_CLEANLY, names::action_timeout::IGNORER_SURVIVES_GRACE] }
flaw! { noop_timeout_scope_cleanup_left: noop_timeout(scope_cleanup = ScopeCleanup::Left) => only [names::action_timeout::SCOPE_REMOVED, names::action_timeout::IGNORER_KILLED] }
flaw! { noop_timeout_temp_left: noop_timeout(temp = Temp::Left) => only [names::action_timeout::TEMP_CLEAN] }
flaw! { bwrap_timeout_temp_left: bwrap_timeout(temp = Temp::Left) => only [names::action_timeout::TEMP_CLEAN] }
flaw! { bwrap_timeout_trap_start_never_starts: bwrap_timeout(trap_start = TrapStart::NeverStarts) => only [names::action_timeout::TRAP_RAN, names::action_timeout::EXEC_WAITS, names::action_timeout::TRAP_EXITS_CLEANLY, names::action_timeout::SCOPE_REMOVED, names::action_timeout::TEMP_CLEAN] }
flaw! { noop_timeout_ignorer_survives: noop_timeout(ignorer = Ignorer::Survives) => only [names::action_timeout::IGNORER_KILLED] }
flaw! { noop_timeout_signal_status_raw_signal: noop_timeout(signal_status = SignalStatus::RawSignal) => only [names::exit_signal::SIGKILL, names::exit_signal::SIGTERM] }
flaw! { contract_worker_env_leaked: contract(worker_env = WorkerEnv::Leaked) => only [names::sandbox_contract::WORKER_ENV_HIDDEN] }
flaw! { contract_tmp_shared: contract(tmp = Tmp::Shared) => only [names::sandbox_contract::TMP_FRESH] }
flaw! { contract_binds_writable: contract(binds = Binds::Writable) => only [names::sandbox_contract::READONLY_BINDS] }
flaw! { contract_workdir_read_only: contract(workdir = Workdir::ReadOnly) => only [names::sandbox_contract::WORKDIR_WRITABLE] }
flaw! { contract_hold_scope_missing: contract(hold_scope = ScopeReport::Missing) => only [names::sandbox_contract::HOLD_HAS_SCOPE] }
flaw! { contract_bwrap_scope_outside: contract(bwrap_scope = BwrapScope::Outside) => only [names::sandbox_contract::BWRAP_IN_SCOPE] }
flaw! { contract_exec_scope_in_scope: contract(exec_scope = ExecScope::InScope) => only [names::sandbox_contract::EXEC_OUTSIDE_SCOPE] }
flaw! { contract_held_fails: contract(held = Held::Fails) => only [names::sandbox_contract::HOLD_COMPLETES] }
flaw! { contract_oom_kill_spared: contract(oom_kill = OomKill::Spared) => only [names::oom::KILLED] }
flaw! { contract_oom_scope_left: contract(oom_scope = OomScope::Left) => only [names::oom::SCOPE_REMOVED] }
flaw! { contract_oom_scope_unobserved: contract(oom_scope = OomScope::Unobserved) => only [names::oom::KILLED, names::oom::SCOPE_REMOVED] }
flaw! { contract_after_oom_fails: contract(after_oom = AfterOom::Fails) => only [names::oom::SERVING] }
flaw! { stale_stray_never_starts: stale(stray = Stray::NeverStarts) => only [names::stale_scope::STRAY_STARTED] }
flaw! { stale_guard_disabled: stale(guard = Guard::Disabled) => only [names::stale_scope::GUARD_REAPS_SCOPE] }
flaw! { stale_exec_kill_takes_scope: stale(exec_kill = ExecKill::TakesScope) => only [names::stale_scope::SCOPE_OUTLIVES_EXEC] }
flaw! { stale_freeze_ignored: stale(freeze = Freeze::Ignored) => only [names::stale_scope::GONE_BEFORE_REPLACEMENT_SERVES] }
flaw! { stale_action_sweep_keeps: stale(action_sweep = Sweep::Keeps) => only [names::stale_scope::SWEPT_BY_NEXT_ACTION] }
flaw! { stale_worker_sweep_keeps: stale(worker_sweep = Sweep::Keeps) => only [names::stale_scope::GONE_BEFORE_REPLACEMENT_SERVES] }
flaw! { escalation_escalation_unbounded: escalation(escalation = Escalation::Unbounded) => only [names::escalation::ATTEMPTS] }
flaw! { escalation_escalation_succeeds: escalation(escalation = Escalation::Succeeds) => only [names::escalation::FAILS] }
flaw! { unplaceable_admitted: unplaceable(unplaceable = Unplaceable::Admitted) => only [names::disk_guard::REFUSED, names::disk_guard::NAMES_NO_FLEET, names::disk_guard::NEVER_RAN] }
flaw! { unplaceable_runs_then_fails: unplaceable(unplaceable = Unplaceable::RunsThenFails) => only [names::disk_guard::NAMES_NO_FLEET, names::disk_guard::NEVER_RAN] }
flaw! { unplaceable_unnamed: unplaceable(unplaceable = Unplaceable::Unnamed) => only [names::disk_guard::NAMES_NO_FLEET] }
flaw! { unplaceable_hangs: unplaceable(unplaceable = Unplaceable::Hangs) => only [names::disk_guard::REFUSED, names::disk_guard::NAMES_NO_FLEET, names::disk_guard::SERVING] }
flaw! { unplaceable_serving_stops: unplaceable(guard_serving = GuardServing::Stops) => only [names::disk_guard::SERVING] }
flaw! { oversized_enrolls: oversized(fleet_disk = FleetDiskCheck::Enrolls) => only [names::oversized_fleet::WORKER_REFUSES, names::oversized_fleet::NO_WORKER_ENROLLS] }
flaw! { oversized_unexplained: oversized(fleet_disk = FleetDiskCheck::Unexplained) => only [names::oversized_fleet::WORKER_REFUSES] }
flaw! { oversized_exit_missing: oversized(fleet_exit = FleetExit::Missing) => only [names::oversized_fleet::WORKER_REFUSES] }
flaw! { oversized_exit_wrong_failure: oversized(fleet_exit = FleetExit::WrongFailure) => only [names::oversized_fleet::WORKER_REFUSES] }
flaw! { cold_cold_start_listens: cold(cold_start = ColdStartExit::Listens) => only [names::cold_start::REFUSED, names::cold_start::EXIT_CODE, names::cold_start::MESSAGE] }
flaw! { cold_cold_start_wrong_code: cold(cold_start = ColdStartExit::WrongCode) => only [names::cold_start::EXIT_CODE] }
flaw! { cold_cold_start_unnamed: cold(cold_start = ColdStartExit::Unnamed) => only [names::cold_start::MESSAGE] }
flaw! { preempted_held_instance_unnamed: preempted(held_instance = HeldInstance::Unnamed) => only [names::preemption::HELD, names::preemption::WORKER_EXITS, names::preemption::REQUEUED] }
flaw! { preempted_preemption_ignored: preempted(preemption = Preemption::Ignored) => only [names::preemption::WORKER_EXITS, names::preemption::REQUEUED] }
flaw! { preempted_requeue_lost: preempted(requeue = Requeue::Lost) => only [names::preemption::REQUEUED, names::preemption::BUILD_SUCCEEDS] }
flaw! { renewal_run_renewal_never: renewal_run(renewal = Renewal::Never) => only [names::renewal::RENEWED, names::renewal::PREVIOUS_REMOVED] }
flaw! { renewal_run_renewal_after_expiry: renewal_run(renewal = Renewal::AfterExpiry) => only [names::renewal::RENEWED, names::renewal::PREVIOUS_REMOVED] }
flaw! { renewal_run_renewal_keeps_previous: renewal_run(renewal = Renewal::KeepsPrevious) => only [names::renewal::PREVIOUS_REMOVED] }
flaw! { renewal_run_renewal_restarts: renewal_run(renewal = Renewal::Restarts) => only [names::renewal::SAME_WORKER] }
flaw! { renewal_run_renewal_hold_fails: renewal_run(renewal_hold = Held::Fails) => only [names::renewal::HELD_COMPLETES] }
