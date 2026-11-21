//! Deterministic pressure observations exercise each independently failing verdict.

use gang_e2e::pressure::{self, Evidence};
use gang_e2e::{AttemptRecord, ResourcePolicy};

const KIB: u64 = 1024;
const PEAK: u64 = 193 * KIB;

#[derive(Clone, Copy)]
enum Behaviour {
    Healthy,
    WrongVictim,
    NeighbourDiesAfterVictim,
    GenericRetryConsumed,
    NoLearning,
    WrongRetryReservation,
    EarlyRetry,
}

struct Simulation {
    behaviour: Behaviour,
    attempts: Vec<AttemptRecord>,
    next: Vec<AttemptRecord>,
}

impl Simulation {
    fn new(behaviour: Behaviour) -> Self {
        Self {
            behaviour,
            attempts: Vec::new(),
            next: Vec::new(),
        }
    }

    fn record(records: &mut Vec<AttemptRecord>, action: &str, memory_kb: u64) {
        records.push(AttemptRecord {
            action: action.to_owned(),
            memory_kb,
        });
    }

    fn run(&mut self) -> Vec<gang_e2e::Check> {
        let victim = 64 * KIB;
        let neighbour = 320 * KIB;
        let learned = (PEAK * 125).div_ceil(100);
        let retry = learned.max((victim * 700).div_ceil(100)).max(victim + 1);
        Self::record(&mut self.attempts, "pressure_victim", victim);
        Self::record(&mut self.attempts, "pressure_neighbour", neighbour);
        let wrong_victim = matches!(self.behaviour, Behaviour::WrongVictim);
        let consumed = matches!(self.behaviour, Behaviour::GenericRetryConsumed);
        if wrong_victim {
            Self::record(&mut self.attempts, "pressure_neighbour", neighbour * 2);
        } else if !consumed {
            let reserve = if matches!(self.behaviour, Behaviour::WrongRetryReservation) {
                retry - 1
            } else {
                retry
            };
            Self::record(&mut self.attempts, "pressure_victim", reserve);
        }
        Self::record(
            &mut self.next,
            "pressure_victim",
            if matches!(self.behaviour, Behaviour::NoLearning) {
                victim
            } else {
                learned
            },
        );
        pressure::verdicts(&Evidence {
            build_succeeded: !consumed,
            observation: Some(gang_e2e::PressureObservation {
                victim_killed: !wrong_victim,
                neighbour_alive: !wrong_victim
                    && !matches!(self.behaviour, Behaviour::NeighbourDiesAfterVictim),
                victim_peak_kb: PEAK,
            }),
            retry_after_neighbour: !wrong_victim
                && !consumed
                && !matches!(self.behaviour, Behaviour::EarlyRetry),
            attempts: &self.attempts,
            next_succeeded: true,
            next_attempts: &self.next,
        })
    }
}

fn fails(checks: &[gang_e2e::Check], name: &str) -> bool {
    checks
        .iter()
        .any(|check| check.name == name && check.is_failure())
}

#[test]
fn pressure_policy_retry_waits_for_neighbour_capacity() {
    let policy = ResourcePolicy::pressure_scenario().expect("valid pressure policy");
    let capacity = policy
        .worker_memory_max
        .expect("pressure policy bounds worker memory")
        .get()
        - policy.memory.reserve.get().get();
    let victim = 64 * 1024 * 1024_u64;
    let neighbour = 320 * 1024 * 1024_u64;
    let retry_floor = (victim * u64::from(policy.memory.escalation_percent.get())).div_ceil(100);

    assert!(victim + neighbour <= capacity);
    assert!(retry_floor + neighbour > capacity);
}

#[test]
fn pool_oom_retries_wait_for_survivor_capacity() {
    let policy = ResourcePolicy::pool_oom_scenario().expect("valid pool OOM policy");
    let reserve = policy.memory.reserve.get().get();
    let actual_pool = policy
        .worker_memory_max
        .expect("pool OOM policy bounds worker memory")
        .get()
        - reserve;
    let retry_ceiling = policy
        .fleet_memory
        .expect("pool OOM policy specifies fleet memory")
        .get()
        .get()
        - reserve;
    let victim = 64 * 1024 * 1024_u64;
    let neighbour = 320 * 1024 * 1024_u64;
    let escalation = u64::from(policy.memory.escalation_percent.get());
    let neighbour_retry = (neighbour * escalation).div_ceil(100).min(retry_ceiling);
    let victim_retry = (victim * escalation).div_ceil(100).min(retry_ceiling);

    assert_eq!(actual_pool, retry_ceiling);
    assert_eq!(neighbour_retry, 416 * 1024 * 1024);
    assert!(victim + neighbour <= actual_pool);
    assert!(neighbour_retry + victim > actual_pool);
    assert!(victim_retry + neighbour > actual_pool);
}

#[test]
fn healthy_pressure_requeues_and_learns() {
    let checks = Simulation::new(Behaviour::Healthy).run();
    assert!(checks.iter().all(gang_e2e::Check::is_passed), "{checks:?}");
}

#[test]
fn selecting_largest_resident_instead_of_furthest_over_fails() {
    let checks = Simulation::new(Behaviour::WrongVictim).run();
    assert!(fails(&checks, pressure::VICTIM));
    assert!(fails(&checks, pressure::REQUEUE));
}

#[test]
fn consuming_generic_retry_budget_fails() {
    let checks = Simulation::new(Behaviour::GenericRetryConsumed).run();
    assert!(fails(&checks, pressure::REQUEUE));
}

#[test]
fn measured_retry_floor_is_enforced() {
    let checks = Simulation::new(Behaviour::WrongRetryReservation).run();
    assert!(fails(&checks, pressure::RETRY_RESERVATION));
}

#[test]
fn missing_learning_fails() {
    let checks = Simulation::new(Behaviour::NoLearning).run();
    assert!(fails(&checks, pressure::LEARNING));
}

#[test]
fn retry_before_neighbour_completes_fails() {
    let checks = Simulation::new(Behaviour::EarlyRetry).run();
    assert!(fails(&checks, pressure::REQUEUE));
}

#[test]
fn neighbour_killed_after_victim_fails() {
    let checks = Simulation::new(Behaviour::NeighbourDiesAfterVictim).run();
    assert!(fails(&checks, pressure::VICTIM));
}
