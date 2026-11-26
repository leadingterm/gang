//! Deterministic seeded discrete-event simulator for [`gang_scheduler::Scheduler`] (`WP A12`).
//!
//! Drives the pure scheduler state machine through randomized multi-fleet soak workloads and
//! targeted adversarial scenarios, asserting structural invariants after every single step:
//!
//! - Exact worker ledger conservation (`Offered`/`Running` hold CPU + memory + disk; `Executed`
//!   holds disk only) and capacity bounds.
//! - Zero leaked or orphaned operations, attempts, queue entries, or deduplication pointers.
//! - Terminal-exactly-once semantics and bounded retry/escalation budgets.
//! - Strict priority queue ordering, `QoS` `CpuWeight` mapping (`1000`/`100`/`20`), `AuditGateTier`
//!   emission, and dedup promotion preserving `seq` while never lowering priority.
//! - Exact distributed placement (highest-preference fleet, lowest reserved CPU fraction via
//!   integer cross-product, fewest gate attempts, LRU, `WorkerId`).
//! - Snapshot and action-listing projection parity across bounded and unbounded limits.
//! - Monotonic timer wakeups (`next_deadline()` never in the past after `on_timer`), quiescence /
//!   liveness convergence, cooperative `hand_off` / `Rehome`, and term-boundary `Learned`
//!   persistence.

#[cfg(test)]
mod harness {
    use std::cmp::Ordering;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Tick, Timestamp};
    use gang_config::{ActionsConfig, ScalingConfig, WholeKib};
    use gang_scheduler::{
        AdmissionRequest, AttemptKey, AttemptPhase, AttemptUsage, Census, CompletionStatus,
        DeclineReason, Effect, Effects, ExecuteError, KillCause, Learned, Now, OperationKey,
        OperationStage, OperationUpdate, QueueKey, RegisterError, RehomeReason, RetryCounts,
        Scheduler, SchedulerPolicy, SketchKey, WorkerCompleted, WorkerEvent, WorkerExecuted,
        WorkerKey, WorkerRegistration, cmp_reserved_cpu_fraction, weight,
    };
    use gang_spi::{
        ActionsView, ControllerDrain, LearnedHistory, ListingLimits, SchedulerSnapshot,
    };
    use gang_types::{
        ActionResources, AttemptId, BuildId, Bytes, Capability, CpuWeight, Digest,
        FleetActionPolicy, FleetCatalog, FleetName, FleetProperties, FleetSet, InstanceId, Isa,
        Mnemonic, NonZeroBytes, OperationName, Resources, SourceId, TargetLabel, WorkerId,
        property,
    };

    const CURRENT_VERSION: &str = "0.3.9";
    const STALE_VERSION: &str = "0.3.8";
    const INITIAL_WALL_MS: u64 = 1_700_000_000_000;

    /// Deterministic `Xoshiro256**` PRNG seeded via `SplitMix64` (zero external RNG dependencies).
    #[derive(Debug, Clone)]
    struct Rng {
        state: [u64; 4],
    }

    impl Rng {
        fn from_seed(seed: u64) -> Self {
            let mut sm = seed;
            let mut splitmix = || {
                sm = sm.wrapping_add(0x9e37_79b9_7f4a_7c15);
                let mut z = sm;
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                z ^ (z >> 31)
            };
            Self {
                state: [splitmix(), splitmix(), splitmix(), splitmix()],
            }
        }

        fn next_u64(&mut self) -> u64 {
            let result = self.state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
            let t = self.state[1] << 17;
            self.state[2] ^= self.state[0];
            self.state[3] ^= self.state[1];
            self.state[1] ^= self.state[2];
            self.state[0] ^= self.state[3];
            self.state[2] ^= t;
            self.state[3] = self.state[3].rotate_left(45);
            result
        }

        fn range(&mut self, upper: usize) -> usize {
            if upper <= 1 {
                return 0;
            }
            let bound = u64::try_from(upper).expect("usize fits u64");
            usize::try_from(self.next_u64() % bound).expect("remainder fits usize")
        }

        fn range_u64(&mut self, low: u64, high_inclusive: u64) -> u64 {
            if high_inclusive <= low {
                return low;
            }
            let span = high_inclusive - low + 1;
            low + (self.next_u64() % span)
        }

        fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
            self.next_u64() % denominator < numerator
        }

        fn uuid_bytes(&mut self) -> [u8; 16] {
            let hi = self.next_u64().to_be_bytes();
            let lo = self.next_u64().to_be_bytes();
            let mut out = [0_u8; 16];
            out[..8].copy_from_slice(&hi);
            out[8..].copy_from_slice(&lo);
            out
        }
    }

    fn multi_fleet_catalog() -> FleetCatalog {
        let make_fleet = |name: &str,
                          vcpus: u32,
                          mem_gib: u64,
                          disk_gib: u64,
                          mem_policy_cap_gib: Option<u64>,
                          network: bool| {
            let mut actions = FleetActionPolicy::default();
            actions.resources.memory = mem_policy_cap_gib.map(NonZeroBytes::gib);
            if network {
                actions.grants.insert(Capability::Network);
            }
            FleetProperties::new(
                &FleetName::try_new(name).expect("valid fleet"),
                Isa::X86_64,
                &"bwrap",
                &"host:sim".parse().expect("valid env"),
                Resources {
                    vcpus,
                    memory: Bytes::gib(mem_gib),
                    disk: Bytes::gib(disk_gib),
                },
                &actions,
                std::iter::empty(),
            )
        };

        FleetCatalog::try_new(vec![
            make_fleet("small", 4, 8, 40, None, false),
            make_fleet("medium", 8, 16, 80, Some(12), false),
            make_fleet("large-net", 16, 32, 160, None, true),
        ])
        .expect("valid catalog")
    }

    fn sim_policy(seed: u64) -> SchedulerPolicy {
        let mut actions = ActionsConfig::default();
        actions.cold_start.cpu_millicores = NonZeroU32::new(2_000).expect("nonzero");
        actions.cold_start.memory = WholeKib::from_units(2 * 1024 * 1024);
        actions.cold_start.disk = WholeKib::from_units(4 * 1024 * 1024);
        actions.max_job_retries = NonZeroU32::new(2).expect("nonzero");
        actions.max_worker_loss_retries = NonZeroU32::new(2).expect("nonzero");
        actions.memory.max_steps = NonZeroU32::new(3).expect("nonzero");
        actions.disk.max_steps = NonZeroU32::new(3).expect("nonzero");
        SchedulerPolicy {
            actions,
            scaling: ScalingConfig::default(),
            expected_gang_version: CURRENT_VERSION.to_owned(),
            dispatch_ack_timeout: Duration::from_secs(5),
            dispatch_scan_limit: 64,
            worker_timeout: Duration::from_secs(15),
            client_action_timeout: Duration::from_secs(12),
            attempt_budget: Duration::from_secs(25),
            attempt_seed: seed,
        }
    }

    #[derive(Debug, Clone)]
    struct DedupTemplate {
        action_digest: Digest,
        command_digest: String,
        platform_properties: BTreeMap<String, String>,
        target: Option<TargetLabel>,
        mnemonic: Option<Mnemonic>,
    }

    #[derive(Debug, Clone)]
    struct WorkerPreState {
        id: WorkerId,
        fleet: usize,
        available: bool,
        free: ActionResources,
        reserved_cpu: u64,
        capacity_cpu: u64,
        gate_running: u32,
        last_assigned_seq: u64,
    }

    #[derive(Debug, Default)]
    struct SimMetrics {
        executes: usize,
        dedups: usize,
        assigns: usize,
        accepts: usize,
        declines_disk: usize,
        declines_draining: usize,
        executed_ok: usize,
        executed_kills: usize,
        completed_ok: usize,
        completed_fail: usize,
        cancels: usize,
        worker_losses: usize,
        timer_sweeps: usize,
        cordons: usize,
        retain_drains: usize,
        backward_wall_jumps: usize,
    }

    impl SimMetrics {
        fn accumulate(&mut self, other: &Self) {
            self.executes += other.executes;
            self.dedups += other.dedups;
            self.assigns += other.assigns;
            self.accepts += other.accepts;
            self.declines_disk += other.declines_disk;
            self.declines_draining += other.declines_draining;
            self.executed_ok += other.executed_ok;
            self.executed_kills += other.executed_kills;
            self.completed_ok += other.completed_ok;
            self.completed_fail += other.completed_fail;
            self.cancels += other.cancels;
            self.worker_losses += other.worker_losses;
            self.timer_sweeps += other.timer_sweeps;
            self.cordons += other.cordons;
            self.retain_drains += other.retain_drains;
            self.backward_wall_jumps += other.backward_wall_jumps;
        }
    }

    struct Sim {
        seed: u64,
        step: usize,
        rng: Rng,
        catalog: FleetCatalog,
        scheduler: Scheduler,
        tick_ms: u64,
        wall_ms: u64,
        effects: Effects,
        snap: SchedulerSnapshot,
        view: ActionsView,
        pre_workers: HashMap<WorkerKey, WorkerPreState>,
        expected_queued: Vec<QueueKey>,
        next_op_seq: u64,
        next_inst_seq: u64,
        admitted_ops: Vec<OperationKey>,
        admitted_set: HashSet<OperationKey>,
        op_initial_seq: HashMap<OperationKey, u64>,
        op_max_priority: HashMap<OperationKey, i32>,
        terminal_events: HashMap<OperationKey, OperationUpdate>,
        worker_lowered_mem: HashSet<WorkerKey>,
        dedup_pool: Vec<DedupTemplate>,
        instances: Vec<(usize, Arc<InstanceId>)>,
        metrics: SimMetrics,
    }

    impl Sim {
        fn new(seed: u64, catalog: FleetCatalog, policy: SchedulerPolicy) -> Self {
            let learned = Learned::new(&policy.actions, &policy.scaling, catalog.len())
                .expect("valid learned");
            Self::with_learned(seed, catalog, policy, learned)
        }

        fn with_learned(
            seed: u64,
            catalog: FleetCatalog,
            policy: SchedulerPolicy,
            learned: Learned,
        ) -> Self {
            let scheduler = Scheduler::new(policy, &catalog, learned).expect("valid scheduler");
            let mut instances = Vec::new();
            for fleet_idx in 0..catalog.len() {
                for slot in 0..3 {
                    let id = InstanceId::try_new(format!("i-f{fleet_idx}-{slot}"))
                        .expect("valid instance id");
                    instances.push((fleet_idx, Arc::new(id)));
                }
            }
            Self {
                seed,
                step: 0,
                rng: Rng::from_seed(seed),
                catalog,
                scheduler,
                tick_ms: 1_000,
                wall_ms: INITIAL_WALL_MS,
                effects: Effects::with_capacity(64),
                snap: SchedulerSnapshot::default(),
                view: ActionsView::default(),
                pre_workers: HashMap::with_capacity(16),
                expected_queued: Vec::with_capacity(64),
                next_op_seq: 1,
                next_inst_seq: 100,
                admitted_ops: Vec::new(),
                admitted_set: HashSet::new(),
                op_initial_seq: HashMap::new(),
                op_max_priority: HashMap::new(),
                terminal_events: HashMap::new(),
                worker_lowered_mem: HashSet::new(),
                dedup_pool: Vec::new(),
                instances,
                metrics: SimMetrics::default(),
            }
        }

        fn now(&self) -> Now {
            Now {
                tick: Tick::from_elapsed(Duration::from_millis(self.tick_ms)),
                wall: Timestamp::from_unix_millis(self.wall_ms),
            }
        }

        fn advance_clock(&mut self, delta_ms: u64, allow_backward_wall: bool) {
            self.tick_ms = self.tick_ms.saturating_add(delta_ms);
            if allow_backward_wall && self.rng.chance(1, 14) {
                let jump_back_ms = self.rng.range_u64(1_000, 30_000);
                self.wall_ms = self.wall_ms.saturating_sub(jump_back_ms).max(60_000);
                self.metrics.backward_wall_jumps += 1;
            } else {
                self.wall_ms = self.wall_ms.saturating_add(delta_ms);
            }
        }

        fn fleet_name(&self, fleet_idx: usize) -> FleetName {
            FleetName::try_new(self.catalog.fleets()[fleet_idx].name()).expect("valid fleet name")
        }

        fn capture_pre_workers(&mut self) {
            self.pre_workers.clear();
            for (key, worker) in self.scheduler.workers().workers_sorted() {
                self.pre_workers.insert(
                    key,
                    WorkerPreState {
                        id: worker.id.clone(),
                        fleet: worker.fleet,
                        available: worker.is_available_for_dispatch(),
                        free: worker.ledger.free(),
                        reserved_cpu: worker.ledger.reserved().cpu_millicores,
                        capacity_cpu: worker.ledger.capacity().cpu_millicores,
                        gate_running: worker.gate_running,
                        last_assigned_seq: worker.last_assigned_seq,
                    },
                );
            }
        }

        fn make_worker_reg(
            &mut self,
            fleet_idx: usize,
            instance: Arc<InstanceId>,
            stale: bool,
            single_use: bool,
            max_inflight: Option<NonZeroU32>,
        ) -> WorkerRegistration {
            let props = &self.catalog.fleets()[fleet_idx];
            let phys_vcpus = props.capacity(property::CPU_COUNT).unwrap_or(4);
            let oversub = u64::from(self.scheduler.policy().actions.cpu_oversubscription.get());
            let cap_cpu = phys_vcpus * property::MILLICORES_PER_CORE * oversub;
            let cap_mem = Bytes::kib(
                props
                    .capacity(property::MEMORY_KB)
                    .unwrap_or(8 * 1024 * 1024),
            );
            let cap_disk = Bytes::kib(
                props
                    .capacity(property::DISK_KB)
                    .unwrap_or(40 * 1024 * 1024),
            );
            let ceil_cpu = props.limit(property::CPU_MILLICORES).unwrap_or(cap_cpu);
            let ceil_mem = Bytes::kib(
                props
                    .limit(property::MEMORY_KB)
                    .unwrap_or(cap_mem.get() / 1024),
            );
            let ceil_disk = Bytes::kib(
                props
                    .limit(property::DISK_KB)
                    .unwrap_or(cap_disk.get() / 1024),
            );

            WorkerRegistration {
                instance,
                fleet: self.fleet_name(fleet_idx),
                gang_version: if stale {
                    STALE_VERSION.to_owned()
                } else {
                    CURRENT_VERSION.to_owned()
                },
                properties: BTreeMap::new(),
                capacity: ActionResources {
                    cpu_millicores: cap_cpu,
                    memory: cap_mem,
                    disk: cap_disk,
                },
                ceilings: ActionResources {
                    cpu_millicores: ceil_cpu,
                    memory: ceil_mem,
                    disk: ceil_disk,
                },
                max_inflight,
                single_use,
                environment: Some("host:sim".parse().expect("valid env")),
                uuid_bytes: self.rng.uuid_bytes(),
            }
        }

        fn register_new_worker(
            &mut self,
            fleet_idx: usize,
            instance: Arc<InstanceId>,
            stale: bool,
            single_use: bool,
            max_inflight: Option<NonZeroU32>,
        ) -> Result<WorkerKey, RegisterError> {
            let reg = self.make_worker_reg(fleet_idx, instance, stale, single_use, max_inflight);
            self.register_worker(reg)
        }

        fn register_worker(&mut self, reg: WorkerRegistration) -> Result<WorkerKey, RegisterError> {
            self.capture_pre_workers();
            let now = self.now();
            self.effects.clear();
            let res = self.scheduler.register(now, reg, &mut self.effects);
            self.verify_effects("register", false);
            self.check_invariants("register");
            res
        }

        fn random_priority(&mut self) -> i32 {
            match self.rng.range(6) {
                0 => -25,
                1 => 0,
                2 => 99,
                3 => 100,
                4 => 199,
                _ => 200 + i32::try_from(self.rng.range(101)).expect("fits i32"),
            }
        }

        fn next_op_name(&mut self) -> OperationName {
            let seq = self.next_op_seq;
            self.next_op_seq += 1;
            OperationName::try_new(format!("operations/s{}-op{seq}", self.seed))
                .expect("valid op name")
        }

        fn build_random_request(&mut self) -> AdmissionRequest {
            let use_dedup = !self.dedup_pool.is_empty() && self.rng.chance(3, 10);
            let priority = self.random_priority();
            let source = match self.rng.range(4) {
                0 => SourceId::from_identity_str("local:alice"),
                1 => SourceId::from_identity_str("local:bob"),
                2 => SourceId::from_identity_str("ci:gate"),
                _ => None,
            };
            let initial_watchers = u32::from(self.rng.chance(8, 10));
            let name = self.next_op_name();
            let build = Some("12rsfjsg79g6h7f3".parse::<BuildId>().expect("build"));

            if use_dedup {
                let idx = self.rng.range(self.dedup_pool.len());
                let tpl = self.dedup_pool[idx].clone();
                return AdmissionRequest {
                    name,
                    action_digest: tpl.action_digest,
                    command_digest: tpl.command_digest,
                    platform_properties: tpl.platform_properties,
                    skip_cache_lookup: false,
                    do_not_cache: false,
                    source,
                    build,
                    target: tpl.target,
                    mnemonic: tpl.mnemonic,
                    priority,
                    initial_watchers,
                };
            }

            let mut props = BTreeMap::new();
            props.insert(property::ISA.to_owned(), "x86-64".to_owned());
            if self.rng.chance(1, 5) {
                props.insert(property::NETWORK.to_owned(), "requested".to_owned());
            }
            if self.rng.chance(1, 4) {
                let mem_kib = match self.rng.range(3) {
                    0 => 1024 * 1024,
                    1 => 6 * 1024 * 1024,
                    _ => 14 * 1024 * 1024,
                };
                props.insert(property::MEMORY_KB.to_owned(), mem_kib.to_string());
            }
            if self.rng.chance(1, 5) {
                props.insert(property::CPU_COUNT.to_owned(), "2".to_owned());
            }

            let target_id = self.rng.range(6);
            let target = Some(
                TargetLabel::try_new(format!("//pkg/m{target_id}:target")).expect("valid target"),
            );
            let mnemonics = ["Rustc", "CppCompile", "Link", "TestRunner"];
            let mnemonic = Some(
                Mnemonic::try_new(mnemonics[self.rng.range(mnemonics.len())])
                    .expect("valid mnemonic"),
            );

            let digest_seed = format!("seed-{}-op-{}", self.seed, self.next_op_seq);
            let action_digest = Digest::from_bytes(digest_seed.as_bytes());
            let command_digest = format!("cmd-{target_id}");
            let skip_cache_lookup = self.rng.chance(1, 8);

            if !skip_cache_lookup {
                if self.dedup_pool.len() >= 12 {
                    let victim = self.rng.range(self.dedup_pool.len());
                    self.dedup_pool.swap_remove(victim);
                }
                self.dedup_pool.push(DedupTemplate {
                    action_digest,
                    command_digest: command_digest.clone(),
                    platform_properties: props.clone(),
                    target: target.clone(),
                    mnemonic: mnemonic.clone(),
                });
            }

            AdmissionRequest {
                name,
                action_digest,
                command_digest,
                platform_properties: props,
                skip_cache_lookup,
                do_not_cache: self.rng.chance(1, 6),
                source,
                build,
                target,
                mnemonic,
                priority,
                initial_watchers,
            }
        }

        fn execute_request(&mut self, req: AdmissionRequest) -> Result<OperationKey, ExecuteError> {
            self.capture_pre_workers();
            let req_priority = req.priority;
            let req_source = req.source;
            let now = self.now();
            self.effects.clear();
            let result = self.scheduler.execute(now, req, &mut self.effects);

            if let Ok(op_key) = result {
                let is_new = self.admitted_set.insert(op_key);
                if is_new {
                    self.admitted_ops.push(op_key);
                    self.metrics.executes += 1;
                    if let Some(op) = self.scheduler.operations().get(op_key) {
                        self.op_initial_seq.insert(op_key, op.seq);
                        self.op_max_priority.insert(op_key, op.priority);
                    }
                } else {
                    self.metrics.dedups += 1;
                }

                self.verify_gate_audit(req_priority, req_source);
                if is_new {
                    self.verify_operation_driven_placement(op_key);
                }
            }

            self.verify_effects("execute", false);
            self.check_invariants("execute");
            result
        }

        fn verify_gate_audit(&self, req_priority: i32, req_source: Option<SourceId>) {
            let has_gate_audit = self.effects.iter().any(|e| {
                matches!(
                    e,
                    Effect::AuditGateTier(audit)
                        if audit.priority == req_priority && audit.source == req_source
                )
            });
            if req_priority >= 200 {
                assert!(
                    has_gate_audit,
                    "seed={} step={}: priority {req_priority} must emit AuditGateTier",
                    self.seed, self.step
                );
            } else {
                assert!(
                    !self
                        .effects
                        .iter()
                        .any(|e| matches!(e, Effect::AuditGateTier(_))),
                    "seed={} step={}: non-gate priority {req_priority} must not emit AuditGateTier",
                    self.seed,
                    self.step
                );
            }
        }

        fn verify_operation_driven_placement(&self, op_key: OperationKey) {
            let Some(op) = self.scheduler.operations().get(op_key) else {
                return;
            };
            let assigned = self.effects.iter().find_map(|e| match e {
                Effect::AssignWorker {
                    worker,
                    operation,
                    reservation,
                    ..
                } if *operation == op_key => Some((*worker, *reservation)),
                _ => None,
            });

            let expected = expected_best_worker(&self.pre_workers, op.candidates, op.reservation);
            match (assigned, expected) {
                (Some((actual, _)), Some(exp)) => {
                    assert_eq!(
                        actual, exp,
                        "seed={} step={}: distributed placement picked {actual:?}, expected {exp:?}",
                        self.seed, self.step
                    );
                }
                (Some((actual, _)), None) => {
                    panic!(
                        "seed={} step={}: assigned {actual:?} when no pre-step worker was eligible",
                        self.seed, self.step
                    );
                }
                (None, Some(exp)) => {
                    panic!(
                        "seed={} step={}: op {op_key:?} remained queued despite eligible worker {exp:?}",
                        self.seed, self.step
                    );
                }
                (None, None) => {}
            }
        }

        fn step_random(&mut self) {
            self.step += 1;
            let delta_ms = self.rng.range_u64(0, 3_000);
            self.advance_clock(delta_ms, true);

            match self.rng.range(100) {
                0..=21 => self.action_execute(),
                22..=32 => self.action_accept_or_decline(),
                33..=45 => self.action_executed(),
                46..=56 => self.action_completed(),
                57..=63 => self.action_keepalive(),
                64..=69 => self.action_register_worker(),
                70..=73 => self.action_worker_lost(),
                74..=77 => self.action_watch_unwatch_cancel(),
                78..=82 => self.action_cordon_or_retain(),
                83..=85 => self.action_going_away_or_stale_sweep(),
                86..=87 => self.action_peer_memory(),
                _ => self.action_timer_sweep(),
            }
        }

        fn action_execute(&mut self) {
            let req = self.build_random_request();
            let _ = self.execute_request(req);
        }

        fn action_register_worker(&mut self) {
            let (fleet_idx, instance) = if self.rng.chance(3, 4) {
                let idx = self.rng.range(self.instances.len());
                self.instances[idx].clone()
            } else {
                let fleet_idx = self.rng.range(self.catalog.len());
                self.next_inst_seq += 1;
                let inst = Arc::new(
                    InstanceId::try_new(format!("i-dyn-{}", self.next_inst_seq))
                        .expect("valid instance"),
                );
                (fleet_idx, inst)
            };
            let stale = self.rng.chance(1, 10);
            let single_use = self.rng.chance(1, 6);
            let max_inflight = match self.rng.range(4) {
                0 => Some(NonZeroU32::new(1).expect("1")),
                1 => Some(NonZeroU32::new(2).expect("2")),
                2 => Some(NonZeroU32::new(4).expect("4")),
                _ => None,
            };
            let _ = self.register_new_worker(fleet_idx, instance, stale, single_use, max_inflight);
        }

        fn action_accept_or_decline(&mut self) {
            let offered: Vec<(WorkerKey, AttemptId)> = self
                .scheduler
                .attempts()
                .iter()
                .filter(|(_, a)| a.phase == AttemptPhase::Offered)
                .map(|(_, a)| (a.worker, a.id.clone()))
                .collect();
            if offered.is_empty() {
                return;
            }
            let (worker, attempt_id) = offered[self.rng.range(offered.len())].clone();
            let event = match self.rng.range(10) {
                0 => {
                    self.metrics.declines_disk += 1;
                    WorkerEvent::Decline {
                        attempt_id,
                        reason: DeclineReason::DiskShortage,
                    }
                }
                1 => {
                    self.metrics.declines_draining += 1;
                    WorkerEvent::Decline {
                        attempt_id,
                        reason: DeclineReason::Draining,
                    }
                }
                _ => {
                    self.metrics.accepts += 1;
                    WorkerEvent::Accept { attempt_id }
                }
            };
            self.deliver_worker_event(worker, event, "accept_or_decline", true);
        }

        fn action_executed(&mut self) {
            let runnable: Vec<(WorkerKey, AttemptId)> = self
                .scheduler
                .attempts()
                .iter()
                .filter(|(_, a)| matches!(a.phase, AttemptPhase::Offered | AttemptPhase::Running))
                .map(|(_, a)| (a.worker, a.id.clone()))
                .collect();
            if runnable.is_empty() {
                return;
            }
            let (worker, attempt_id) = runnable[self.rng.range(runnable.len())].clone();
            let (kill, oom_kills, side_mem, peak_mem_gib, peak_disk_gib) = match self.rng.range(12)
            {
                0 => (Some(KillCause::Oom), 1, false, 6, 2),
                1 => (Some(KillCause::MemoryPressure), 0, false, 10, 2),
                2 => (None, 0, true, 14, 2),
                3 => (Some(KillCause::Oom), 1, false, 40, 2),
                4 => (Some(KillCause::DiskPressure), 0, false, 2, 16),
                5 => (Some(KillCause::SoftDisk), 0, false, 2, 10),
                _ => (None, 0, false, 1, 1),
            };
            if kill.is_some() || oom_kills > 0 || side_mem {
                self.metrics.executed_kills += 1;
            } else {
                self.metrics.executed_ok += 1;
            }
            let usage = AttemptUsage {
                peak_memory: Some(Bytes::gib(peak_mem_gib)),
                peak_disk: Some(Bytes::gib(peak_disk_gib)),
                cpu_time: Duration::from_millis(self.rng.range_u64(500, 4_000)),
                wall_time: Duration::from_millis(self.rng.range_u64(500, 3_000)),
                oom_kills,
            };
            let event = WorkerEvent::Executed(WorkerExecuted {
                attempt_id,
                usage,
                kill,
                side_channel_memory_failure: side_mem,
            });
            self.deliver_worker_event(worker, event, "executed", true);
        }

        fn action_completed(&mut self) {
            let executed: Vec<(WorkerKey, AttemptId, Timestamp)> = self
                .scheduler
                .attempts()
                .iter()
                .filter(|(_, a)| a.phase == AttemptPhase::Executed)
                .map(|(_, a)| {
                    let start = a.accepted_at.unwrap_or(a.dispatched_at).wall;
                    (a.worker, a.id.clone(), start)
                })
                .collect();
            if executed.is_empty() {
                return;
            }
            let (worker, attempt_id, start_wall) = executed[self.rng.range(executed.len())].clone();
            let status = if self.rng.chance(1, 6) {
                self.metrics.completed_fail += 1;
                CompletionStatus::Failed {
                    message: "simulated transient upload error".to_owned(),
                }
            } else {
                self.metrics.completed_ok += 1;
                CompletionStatus::Ok
            };
            let end_wall = self.now().wall;
            let event = WorkerEvent::Completed(WorkerCompleted {
                attempt_id,
                status,
                execution_timestamps: Some((start_wall, end_wall)),
            });
            self.deliver_worker_event(worker, event, "completed", true);
        }

        fn action_keepalive(&mut self) {
            let workers: Vec<(WorkerKey, usize)> = self
                .scheduler
                .workers()
                .workers_sorted()
                .map(|(k, w)| (k, w.fleet))
                .collect();
            if workers.is_empty() {
                return;
            }
            let (worker, fleet) = workers[self.rng.range(workers.len())];
            let memory_capacity = if self.rng.chance(1, 5) {
                let cap_kb = self.catalog.fleets()[fleet]
                    .capacity(property::MEMORY_KB)
                    .unwrap_or(8 * 1024 * 1024);
                let pct = self.rng.range_u64(75, 100);
                let next = Bytes::kib((cap_kb * pct) / 100);
                if let Some(w) = self.scheduler.workers().get(worker)
                    && next < w.ledger.reserved().memory
                {
                    self.worker_lowered_mem.insert(worker);
                }
                Some(next)
            } else {
                None
            };
            let local_read_requests = self.rng.range_u64(1, 50);
            self.deliver_worker_event(
                worker,
                WorkerEvent::KeepAlive {
                    cache: Some(gang_spi::WorkerCacheStats {
                        local_read_requests,
                        ..gang_spi::WorkerCacheStats::default()
                    }),
                    memory_capacity,
                },
                "keepalive",
                true,
            );
        }

        fn action_worker_lost(&mut self) {
            let workers: Vec<WorkerKey> = self
                .scheduler
                .workers()
                .workers_sorted()
                .map(|(k, _)| k)
                .collect();
            if workers.is_empty() {
                return;
            }
            let worker = workers[self.rng.range(workers.len())];
            self.worker_lowered_mem.remove(&worker);
            self.metrics.worker_losses += 1;
            self.capture_pre_workers();
            let now = self.now();
            self.effects.clear();
            self.scheduler
                .on_worker_lost(now, worker, &mut self.effects);
            self.verify_effects("on_worker_lost", false);
            self.check_invariants("on_worker_lost");
        }

        fn action_watch_unwatch_cancel(&mut self) {
            let live: Vec<(OperationKey, OperationName)> = self
                .scheduler
                .operations()
                .iter()
                .filter(|(_, op)| !op.stage.is_terminal())
                .map(|(k, op)| (k, op.name.clone()))
                .collect();
            if live.is_empty() {
                return;
            }
            let (key, name) = live[self.rng.range(live.len())].clone();
            self.capture_pre_workers();
            let now = self.now();
            self.effects.clear();
            match self.rng.range(3) {
                0 => {
                    let _ = self.scheduler.watch(now, &name);
                }
                1 => {
                    self.scheduler.unwatch(now, key, &mut self.effects);
                }
                _ => {
                    self.metrics.cancels += 1;
                    self.scheduler.cancel(now, key, &mut self.effects);
                }
            }
            self.verify_effects("watch_unwatch_cancel", false);
            self.check_invariants("watch_unwatch_cancel");
        }

        fn action_cordon_or_retain(&mut self) {
            self.capture_pre_workers();
            let now = self.now();
            self.effects.clear();
            if self.rng.chance(2, 3) {
                self.metrics.cordons += 1;
                let idx = self.rng.range(self.instances.len());
                let (fleet_idx, inst) = self.instances[idx].clone();
                let fleet_name = self.fleet_name(fleet_idx);
                let draining = self.rng.chance(1, 2);
                self.scheduler
                    .set_draining(now, &fleet_name, &inst, draining, &mut self.effects)
                    .expect("known fleet");
            } else {
                self.metrics.retain_drains += 1;
                let mut entries = Vec::new();
                for (fleet_idx, inst) in &self.instances {
                    if self.rng.chance(1, 2) {
                        entries.push((self.fleet_name(*fleet_idx), Arc::clone(inst)));
                    }
                }
                let census = Census::new(entries);
                self.scheduler
                    .retain_drains(now.tick, Duration::from_secs(6), &census);
            }
            self.verify_effects("cordon_or_retain", false);
            self.check_invariants("cordon_or_retain");
        }

        fn action_going_away_or_stale_sweep(&mut self) {
            self.capture_pre_workers();
            let now = self.now();
            self.effects.clear();
            if self.rng.chance(1, 2) {
                let workers: Vec<WorkerKey> = self
                    .scheduler
                    .workers()
                    .workers_sorted()
                    .map(|(k, _)| k)
                    .collect();
                if let Some(&worker) = workers.get(self.rng.range(workers.len().max(1))) {
                    self.scheduler.on_worker(
                        now,
                        worker,
                        WorkerEvent::GoingAway,
                        &mut self.effects,
                    );
                }
            } else {
                let _ = self.scheduler.stale_build_drains(&mut self.effects);
            }
            self.verify_effects("going_away_or_stale", false);
            self.check_invariants("going_away_or_stale");
        }

        fn action_peer_memory(&mut self) {
            let fleet = self.rng.range(self.catalog.len());
            let est_kb = self.catalog.fleets()[fleet]
                .capacity(property::MEMORY_KB)
                .unwrap_or(8 * 1024 * 1024);
            let pct = self.rng.range_u64(80, 100);
            let cap = NonZeroBytes::new(Bytes::kib((est_kb * pct) / 100)).expect("nonzero");
            self.scheduler
                .set_peer_memory_observations([gang_spi::FleetMemoryCapacity {
                    fleet,
                    capacity: cap,
                }])
                .expect("valid peer observation");
            self.check_invariants("peer_memory");
        }

        fn action_timer_sweep(&mut self) {
            self.metrics.timer_sweeps += 1;
            self.capture_pre_workers();
            let now = self.now();
            self.effects.clear();
            self.scheduler.on_timer(now, &mut self.effects);
            if let Some(next) = self.scheduler.next_deadline() {
                assert!(
                    next > now.tick,
                    "seed={} step={}: next_deadline {next:?} <= now.tick {:?}",
                    self.seed,
                    self.step,
                    now.tick
                );
            }
            self.verify_effects("on_timer", true);
            self.check_invariants("on_timer");
        }

        fn deliver_worker_event(
            &mut self,
            worker: WorkerKey,
            event: WorkerEvent,
            context: &str,
            check_now: bool,
        ) {
            self.capture_pre_workers();
            let now = self.now();
            self.effects.clear();
            self.scheduler
                .on_worker(now, worker, event, &mut self.effects);
            self.verify_effects(context, false);
            if check_now {
                self.check_invariants(context);
            }
        }

        fn verify_effects(&mut self, context: &str, allow_pre_capacity_change: bool) {
            let effects_snapshot: Vec<Effect> = self.effects.as_slice().to_vec();
            for effect in effects_snapshot {
                match effect {
                    Effect::AssignWorker {
                        worker,
                        attempt,
                        attempt_id,
                        operation,
                        cpu_weight,
                        reservation,
                        deadline,
                        deadline_wall,
                    } => {
                        self.verify_assign_effect(
                            worker,
                            attempt,
                            &attempt_id,
                            operation,
                            cpu_weight,
                            reservation,
                            deadline,
                            deadline_wall,
                            context,
                            allow_pre_capacity_change,
                        );
                    }
                    Effect::NotifyWatchers { operation, event } => {
                        if matches!(
                            event,
                            OperationUpdate::Completed
                                | OperationUpdate::Failed(_)
                                | OperationUpdate::Cancelled
                        ) {
                            let prev = self.terminal_events.insert(operation, event.clone());
                            assert!(
                                prev.is_none(),
                                "seed={} step={} ({context}): op {operation:?} duplicate terminal event: prev={prev:?}, new={event:?}",
                                self.seed,
                                self.step
                            );
                        }
                    }
                    Effect::AuditGateTier(audit) => {
                        assert!(
                            audit.priority >= 200,
                            "seed={} step={} ({context}): AuditGateTier for priority {}",
                            self.seed,
                            self.step,
                            audit.priority
                        );
                    }
                    Effect::CloseWorker { worker } => {
                        self.worker_lowered_mem.remove(&worker);
                    }
                    Effect::KillWorker { .. }
                    | Effect::SetWorkerDraining { .. }
                    | Effect::RehomeWorker { .. }
                    | Effect::RecordObservation(_) => {}
                }
            }
        }

        #[allow(clippy::too_many_arguments)]
        fn verify_assign_effect(
            &mut self,
            worker: WorkerKey,
            attempt: AttemptKey,
            attempt_id: &AttemptId,
            operation: OperationKey,
            cpu_weight: CpuWeight,
            reservation: ActionResources,
            deadline: Tick,
            deadline_wall: Timestamp,
            context: &str,
            allow_pre_capacity_change: bool,
        ) {
            let now = self.now();
            let budget = self.scheduler.policy().attempt_budget;
            self.metrics.assigns += 1;
            assert!(
                !self.scheduler.handoff().is_active(),
                "seed={} step={} ({context}): AssignWorker emitted during handoff",
                self.seed,
                self.step
            );
            assert_eq!(deadline, now.tick + budget);
            assert_eq!(deadline_wall, now.wall + budget);
            let op = self
                .scheduler
                .operations()
                .get(operation)
                .expect("assigned op exists");
            assert_eq!(cpu_weight, weight(op.priority));
            verify_exact_qos_weight(op.priority, cpu_weight, self.seed, self.step);
            assert!(self.scheduler.attempts().get(attempt).is_some());
            assert_eq!(
                self.scheduler.attempts().lookup_id(attempt_id),
                Some(attempt)
            );
            let w = self
                .scheduler
                .workers()
                .get(worker)
                .expect("assigned worker connected");
            assert!(
                !w.drain.blocks_assignment()
                    && w.is_current_build()
                    && !w.has_disk_hold()
                    && op.candidates.contains(w.fleet),
                "seed={} step={} ({context}): assigned to ineligible worker {worker:?}",
                self.seed,
                self.step
            );
            if !allow_pre_capacity_change && let Some(pre) = self.pre_workers.get(&worker) {
                assert_eq!(pre.fleet, w.fleet);
            }
            let reserved = w.ledger.reserved();
            let cap = w.ledger.capacity();
            assert!(
                reserved.cpu_millicores <= cap.cpu_millicores
                    && reserved.memory <= cap.memory
                    && reserved.disk <= cap.disk,
                "seed={} step={} ({context}): assignment {reservation:?} overflowed capacity {cap:?}",
                self.seed,
                self.step
            );
        }

        fn check_invariants(&mut self, context: &str) {
            self.check_worker_ledgers(context);
            self.check_attempt_indices(context);
            self.check_operations_and_queue(context);
            self.check_snapshot_parity(context);
            self.check_listing_parity(context);
        }

        fn check_worker_ledgers(&mut self, context: &str) {
            let workers = self.scheduler.workers();
            let attempts = self.scheduler.attempts();
            for (w_key, worker) in workers.workers_sorted() {
                let mut sum_cpu = 0_u64;
                let mut sum_mem = 0_u64;
                let mut sum_disk = 0_u64;
                let mut expected_inflight = 0_u32;
                let mut expected_gate = 0_u32;

                for &att_key in attempts.worker_attempts(w_key) {
                    let att = attempts.get(att_key).expect("indexed worker attempt");
                    assert_eq!(att.worker, w_key);
                    sum_disk = sum_disk.saturating_add(att.reservation.disk.get());
                    if matches!(att.phase, AttemptPhase::Offered | AttemptPhase::Running) {
                        sum_cpu = sum_cpu.saturating_add(att.reservation.cpu_millicores);
                        sum_mem = sum_mem.saturating_add(att.reservation.memory.get());
                        expected_inflight += 1;
                        if att.is_gate {
                            expected_gate += 1;
                        }
                    }
                }

                let reserved = worker.ledger.reserved();
                let capacity = worker.ledger.capacity();
                assert_eq!(
                    (
                        reserved.cpu_millicores,
                        reserved.memory.get(),
                        reserved.disk.get()
                    ),
                    (sum_cpu, sum_mem, sum_disk),
                    "seed={} step={} ({context}): worker {w_key:?} ledger drift",
                    self.seed,
                    self.step
                );
                assert_eq!(worker.inflight_attempts, expected_inflight);
                assert_eq!(worker.gate_running, expected_gate);
                assert!(reserved.cpu_millicores <= capacity.cpu_millicores);
                assert!(reserved.disk <= capacity.disk);
                if reserved.memory <= capacity.memory {
                    self.worker_lowered_mem.remove(&w_key);
                } else {
                    assert!(
                        self.worker_lowered_mem.contains(&w_key),
                        "seed={} step={} ({context}): worker {w_key:?} memory overflow",
                        self.seed,
                        self.step
                    );
                }
                if let Some(max) = worker.max_inflight {
                    assert!(worker.inflight_attempts <= max.get());
                }
                if worker.is_single_use() {
                    assert!(worker.inflight_attempts <= 1);
                    if worker.is_single_use_exhausted() {
                        assert_eq!(worker.inflight_attempts, 0);
                    }
                }
            }
        }

        fn check_attempt_indices(&self, context: &str) {
            let workers = self.scheduler.workers();
            let attempts = self.scheduler.attempts();
            let ops = self.scheduler.operations();
            for (att_key, att) in attempts.iter() {
                assert!(
                    workers.get(att.worker).is_some(),
                    "seed={} step={} ({context}): orphaned attempt {att_key:?}",
                    self.seed,
                    self.step
                );
                assert!(attempts.worker_attempts(att.worker).contains(&att_key));
                assert_eq!(attempts.lookup_id(&att.id), Some(att_key));
                if att.owns_operation() {
                    let op = ops.get(att.operation).expect("owned attempt op exists");
                    assert_eq!(op.stage, OperationStage::Running);
                    assert_eq!(attempts.active_for_operation(att.operation), Some(att_key));
                }
            }
        }

        fn check_operations_and_queue(&mut self, context: &str) {
            let ops = self.scheduler.operations();
            let queue = self.scheduler.queue();
            let attempts = self.scheduler.attempts();
            let workers = self.scheduler.workers();
            let actions_cfg = &self.scheduler.policy().actions;

            self.expected_queued.clear();
            let mut expected_dedup_count = 0_usize;

            for (op_key, op) in ops.iter() {
                assert_eq!(op.cpu_weight, weight(op.priority));
                if let Some(&init_seq) = self.op_initial_seq.get(&op_key) {
                    assert_eq!(op.seq, init_seq);
                }
                if let Some(&prev_prio) = self.op_max_priority.get(&op_key) {
                    assert!(op.priority >= prev_prio);
                }
                self.op_max_priority.insert(op_key, op.priority);

                assert!(op.retry_counts.generic <= actions_cfg.max_job_retries.get());
                assert!(op.retry_counts.worker_loss <= actions_cfg.max_worker_loss_retries.get());
                assert!(op.retry_counts.memory_steps <= actions_cfg.memory.max_steps.get());
                assert!(op.retry_counts.disk_steps <= actions_cfg.disk.max_steps.get());

                match op.stage {
                    OperationStage::Queued => {
                        self.expected_queued
                            .push(QueueKey::new(op.priority, op.seq, op_key));
                        assert!(attempts.active_for_operation(op_key).is_none());
                    }
                    OperationStage::Running => {
                        let att_key = attempts
                            .active_for_operation(op_key)
                            .expect("Running op has active attempt");
                        let att = attempts.get(att_key).expect("active attempt exists");
                        assert_eq!(att.operation, op_key);
                        assert!(att.owns_operation());
                        assert!(workers.get(att.worker).is_some());
                    }
                    OperationStage::Completed | OperationStage::Cancelled => {
                        assert!(attempts.active_for_operation(op_key).is_none());
                    }
                }

                if !op.stage.is_terminal() {
                    assert_eq!(ops.lookup_name(&op.name), Some(op_key));
                    if !op.skip_cache_lookup {
                        expected_dedup_count += 1;
                        assert_eq!(
                            ops.lookup_dedup(op.action_digest, &op.platform_key),
                            Some(op_key)
                        );
                    }
                }
            }

            // Pigeonhole: every live non-skip op maps to itself AND dedup_len equals their count,
            // proving zero terminal or leaked entries remain in `dedup`.
            assert_eq!(
                ops.dedup_len(),
                expected_dedup_count,
                "seed={} step={} ({context}): dedup map leaked stale/terminal entry",
                self.seed,
                self.step
            );

            self.expected_queued.sort();
            assert_eq!(queue.len(), self.expected_queued.len());
            for (actual, expected) in queue.global_iter().zip(&self.expected_queued) {
                assert_eq!(actual, *expected);
            }

            for fleet_idx in 0..self.catalog.len() {
                let expected_iter = self.expected_queued.iter().copied().filter(|entry| {
                    ops.get(entry.key)
                        .is_some_and(|op| op.candidates.contains(fleet_idx))
                });
                let actual_iter = queue.fleet_iter(fleet_idx).expect("valid fleet");
                assert!(actual_iter.eq(expected_iter));
            }
        }

        fn check_snapshot_parity(&mut self, context: &str) {
            let now = self.now();
            self.scheduler.snapshot(now, &mut self.snap);
            assert_eq!(
                self.snap.completed,
                self.scheduler.attempts().completed_count(),
                "seed={} step={} ({context}): snapshot.completed drift",
                self.seed,
                self.step
            );
            assert_eq!(self.snap.source_rates_at, now.tick);
            assert_eq!(
                self.snap.workers.len(),
                self.scheduler.workers().worker_count()
            );
            for pair in self.snap.workers.windows(2) {
                assert!(pair[0].id < pair[1].id);
            }
            for ws in &self.snap.workers {
                let w_key = self
                    .scheduler
                    .workers()
                    .lookup_id(&ws.id)
                    .expect("snapshot worker");
                let w = self.scheduler.workers().get(w_key).expect("worker");
                assert_eq!(ws.running_actions, w.inflight_attempts);
                assert_eq!(ws.capacity, Some(w.ledger.capacity()));
                assert_eq!(ws.reserved, Some(w.ledger.reserved()));
                assert_eq!(ws.draining, w.drain.is_draining());
                assert_eq!(ws.current_build, w.is_current_build());
            }
            assert_eq!(self.snap.queued.len(), self.scheduler.queue().len());
            for pair in self.snap.queued.windows(2) {
                assert!(pair[0].priority >= pair[1].priority);
            }
            let expected_executing = self.count_executing();
            assert_eq!(self.snap.executing.len(), expected_executing);
            for exec in &self.snap.executing {
                assert!(exec.worker < self.snap.workers.len());
            }
            assert_eq!(self.snap.cordons.len(), self.scheduler.cordons().len());
        }

        fn check_listing_parity(&mut self, _context: &str) {
            let expected_executing = self.count_executing();
            let total_queued = self.scheduler.queue().len();

            self.scheduler.actions(
                ListingLimits {
                    running: 2,
                    queued: 3,
                },
                &mut self.view,
            );
            assert_eq!(self.view.running.len(), expected_executing.min(2));
            assert_eq!(
                self.view.running.len()
                    + usize::try_from(self.view.truncated_running).expect("fits"),
                expected_executing
            );
            assert_eq!(self.view.queued.len(), total_queued.min(3));
            assert_eq!(
                self.view.queued.len() + usize::try_from(self.view.truncated_queued).expect("fits"),
                total_queued
            );
            for (listed, entry) in self
                .view
                .queued
                .iter()
                .zip(self.scheduler.queue().global_iter())
            {
                let op = self
                    .scheduler
                    .operations()
                    .get(entry.key)
                    .expect("queued op");
                assert_eq!(listed.info.op, op.name.as_str());
                assert_eq!(listed.info.priority, op.priority);
            }

            if self.step.is_multiple_of(10) {
                self.scheduler.actions(
                    ListingLimits {
                        running: usize::MAX,
                        queued: usize::MAX,
                    },
                    &mut self.view,
                );
                assert_eq!(self.view.truncated_running, 0);
                assert_eq!(self.view.truncated_queued, 0);
                assert_eq!(self.view.running.len(), expected_executing);
                assert_eq!(self.view.queued.len(), total_queued);
                for pair in self.view.running.windows(2) {
                    assert!(pair[0].info.op <= pair[1].info.op);
                }
                for (listed, entry) in self
                    .view
                    .queued
                    .iter()
                    .zip(self.scheduler.queue().global_iter())
                {
                    let op = self
                        .scheduler
                        .operations()
                        .get(entry.key)
                        .expect("queued op");
                    assert_eq!(listed.info.op, op.name.as_str());
                    assert_eq!(listed.info.priority, op.priority);
                }
            }
        }

        fn count_executing(&self) -> usize {
            self.scheduler
                .attempts()
                .active_operations()
                .filter(|&(op_key, att_key)| {
                    self.scheduler
                        .attempts()
                        .get(att_key)
                        .is_some_and(|a| a.phase != AttemptPhase::Executed)
                        && self
                            .scheduler
                            .operations()
                            .get(op_key)
                            .is_some_and(|op| op.stage == OperationStage::Running)
                })
                .count()
        }

        fn prepare_quiescence_workers(&mut self) -> Vec<WorkerKey> {
            let now = self.now();
            for (fleet_idx, inst) in self.instances.clone() {
                let fleet = self.fleet_name(fleet_idx);
                self.capture_pre_workers();
                self.effects.clear();
                let _ = self
                    .scheduler
                    .set_draining(now, &fleet, &inst, false, &mut self.effects);
                self.verify_effects("quiesce_uncordon", true);
            }
            let mut drain_workers = Vec::new();
            for fleet_idx in 0..self.catalog.len() {
                self.next_inst_seq += 1;
                let inst = Arc::new(
                    InstanceId::try_new(format!("i-drain-f{fleet_idx}-{}", self.next_inst_seq))
                        .expect("valid instance"),
                );
                if let Ok(wk) = self.register_new_worker(fleet_idx, inst, false, false, None) {
                    drain_workers.push(wk);
                }
            }
            drain_workers
        }

        fn run_to_quiescence(&mut self) {
            let drain_workers = self.prepare_quiescence_workers();
            let mut last_keepalive_ms = self.tick_ms;

            for iter in 0..2_000 {
                self.step += 1;
                if self.scheduler.queue().is_empty() && self.scheduler.attempts().is_empty() {
                    break;
                }
                if self.tick_ms.saturating_sub(last_keepalive_ms) >= 4_000 {
                    last_keepalive_ms = self.tick_ms;
                    for &wk in &drain_workers {
                        if self.scheduler.workers().get(wk).is_some() {
                            self.deliver_worker_event(
                                wk,
                                WorkerEvent::KeepAlive {
                                    cache: None,
                                    memory_capacity: None,
                                },
                                "quiesce_keepalive",
                                false,
                            );
                        }
                    }
                }

                let attempts: Vec<(WorkerKey, AttemptId, AttemptPhase)> = self
                    .scheduler
                    .attempts()
                    .iter()
                    .map(|(_, a)| (a.worker, a.id.clone(), a.phase))
                    .collect();

                if !attempts.is_empty() {
                    self.advance_clock(100, false);
                    for (worker, attempt_id, phase) in attempts {
                        if self.scheduler.workers().get(worker).is_none() {
                            continue;
                        }
                        let event = quiesce_event_for(phase, attempt_id, self.wall_ms);
                        self.deliver_worker_event(worker, event, "quiesce_progress", false);
                    }
                    self.check_invariants("quiesce_tick");
                } else if let Some(deadline) = self.scheduler.next_deadline() {
                    self.advance_to_tick(deadline);
                    self.action_timer_sweep();
                } else {
                    panic!(
                        "seed={} iter={iter}: stuck with {} queued ops",
                        self.seed,
                        self.scheduler.queue().len()
                    );
                }
            }

            self.assert_quiesced_and_drain_timers();
        }

        fn advance_to_tick(&mut self, deadline: Tick) {
            let target_ms =
                u64::try_from(deadline.saturating_duration_since(Tick::ORIGIN).as_millis())
                    .expect("fits u64");
            if target_ms > self.tick_ms {
                self.advance_clock(target_ms - self.tick_ms, false);
            }
        }

        fn assert_quiesced_and_drain_timers(&mut self) {
            assert!(self.scheduler.queue().is_empty());
            assert!(self.scheduler.attempts().is_empty());

            for (_, w) in self.scheduler.workers().workers_sorted() {
                assert_eq!(w.ledger.reserved(), ActionResources::default());
                assert_eq!(w.inflight_attempts, 0);
                assert_eq!(w.gate_running, 0);
            }

            for &op_key in &self.admitted_ops {
                match self.scheduler.operations().get(op_key) {
                    Some(op) => {
                        assert!(op.stage.is_terminal());
                        assert!(self.terminal_events.contains_key(&op_key));
                    }
                    None => {
                        assert!(!self.terminal_events.contains_key(&op_key));
                    }
                }
            }

            for _ in 0..500 {
                let Some(deadline) = self.scheduler.next_deadline() else {
                    break;
                };
                self.advance_to_tick(deadline);
                self.action_timer_sweep();
            }
            assert_eq!(self.scheduler.next_deadline(), None);
            assert_eq!(self.scheduler.workers().worker_count(), 0);
        }
    }

    fn quiesce_event_for(phase: AttemptPhase, attempt_id: AttemptId, wall_ms: u64) -> WorkerEvent {
        match phase {
            AttemptPhase::Offered => WorkerEvent::Accept { attempt_id },
            AttemptPhase::Running => WorkerEvent::Executed(WorkerExecuted {
                attempt_id,
                usage: AttemptUsage {
                    peak_memory: Some(Bytes::gib(1)),
                    peak_disk: Some(Bytes::gib(1)),
                    cpu_time: Duration::from_secs(1),
                    wall_time: Duration::from_secs(1),
                    oom_kills: 0,
                },
                kill: None,
                side_channel_memory_failure: false,
            }),
            AttemptPhase::Executed => WorkerEvent::Completed(WorkerCompleted {
                attempt_id,
                status: CompletionStatus::Ok,
                execution_timestamps: Some((
                    Timestamp::from_unix_millis(wall_ms.saturating_sub(1_000)),
                    Timestamp::from_unix_millis(wall_ms),
                )),
            }),
        }
    }

    fn verify_exact_qos_weight(priority: i32, actual: CpuWeight, seed: u64, step: usize) {
        let expected_raw = if priority >= 200 {
            1_000
        } else if priority >= 100 {
            100
        } else {
            20
        };
        assert_eq!(
            actual.get(),
            expected_raw,
            "seed={seed} step={step}: priority {priority} mapped to {}, expected {expected_raw}",
            actual.get()
        );
    }

    fn expected_best_worker(
        pre: &HashMap<WorkerKey, WorkerPreState>,
        candidates: FleetSet,
        reservation: ActionResources,
    ) -> Option<WorkerKey> {
        for fleet in candidates {
            let mut best: Option<(WorkerKey, &WorkerPreState)> = None;
            for (&key, state) in pre {
                if state.fleet != fleet || !state.available {
                    continue;
                }
                if reservation.cpu_millicores > state.free.cpu_millicores
                    || reservation.memory > state.free.memory
                    || reservation.disk > state.free.disk
                {
                    continue;
                }
                match best {
                    None => best = Some((key, state)),
                    Some((_, cur)) => {
                        let ord = cmp_reserved_cpu_fraction(
                            state.reserved_cpu,
                            state.capacity_cpu,
                            cur.reserved_cpu,
                            cur.capacity_cpu,
                        )
                        .then_with(|| state.gate_running.cmp(&cur.gate_running))
                        .then_with(|| state.last_assigned_seq.cmp(&cur.last_assigned_seq))
                        .then_with(|| state.id.cmp(&cur.id));
                        if ord == Ordering::Less {
                            best = Some((key, state));
                        }
                    }
                }
            }
            if let Some((winner, _)) = best {
                return Some(winner);
            }
        }
        None
    }

    fn simple_request(
        name: &str,
        digest: &[u8],
        priority: i32,
        skip_cache: bool,
        target: Option<TargetLabel>,
        mnemonic: Option<Mnemonic>,
    ) -> AdmissionRequest {
        AdmissionRequest {
            name: OperationName::try_new(name).expect("op"),
            action_digest: Digest::from_bytes(digest),
            command_digest: format!("cmd-{name}"),
            platform_properties: [(property::ISA.to_owned(), "x86-64".to_owned())]
                .into_iter()
                .collect(),
            skip_cache_lookup: skip_cache,
            do_not_cache: false,
            source: SourceId::from_identity_str("ci:gate"),
            build: None,
            target,
            mnemonic,
            priority,
            initial_watchers: 1,
        }
    }

    #[test]
    fn randomized_multi_fleet_soak_preserves_all_invariants_and_quiesces() {
        let mut totals = SimMetrics::default();

        for seed in 1_u64..=20 {
            let catalog = multi_fleet_catalog();
            let policy = sim_policy(seed);
            let mut sim = Sim::new(seed, catalog, policy);

            for fleet_idx in 0..sim.catalog.len() {
                for slot in 0..2 {
                    let inst = Arc::new(
                        InstanceId::try_new(format!("i-f{fleet_idx}-{slot}")).expect("instance"),
                    );
                    sim.register_new_worker(fleet_idx, inst, false, false, None)
                        .expect("initial register");
                }
            }

            for _ in 0..500 {
                sim.step_random();
            }
            sim.run_to_quiescence();
            totals.accumulate(&sim.metrics);
        }

        assert!(totals.executes > 800, "executes={}", totals.executes);
        assert!(totals.dedups > 80, "dedups={}", totals.dedups);
        assert!(totals.assigns > 800, "assigns={}", totals.assigns);
        assert!(totals.accepts > 200, "accepts={}", totals.accepts);
        assert!(totals.declines_disk > 10);
        assert!(totals.declines_draining > 10);
        assert!(totals.executed_ok > 150);
        assert!(totals.executed_kills > 80);
        assert!(totals.completed_ok > 100);
        assert!(totals.completed_fail > 15);
        assert!(totals.cancels > 40);
        assert!(totals.worker_losses > 80);
        assert!(totals.cordons > 80);
        assert!(totals.retain_drains > 40);
        assert!(totals.backward_wall_jumps > 150);
    }

    fn run_handoff_seed(seed: u64) {
        let catalog = multi_fleet_catalog();
        let policy = sim_policy(seed);
        let mut sim = Sim::new(seed, catalog, policy);

        for fleet_idx in 0..sim.catalog.len() {
            let shared =
                Arc::new(InstanceId::try_new(format!("i-f{fleet_idx}-0")).expect("instance"));
            let idle =
                Arc::new(InstanceId::try_new(format!("i-f{fleet_idx}-1")).expect("instance"));
            for _ in 0..2 {
                sim.register_new_worker(fleet_idx, Arc::clone(&shared), false, false, None)
                    .expect("gen register");
            }
            sim.register_new_worker(fleet_idx, idle, false, false, None)
                .expect("idle register");
        }

        for _ in 0..120 {
            sim.step_random();
        }

        let mut drain = ControllerDrain::default();
        sim.capture_pre_workers();
        let now = sim.now();
        sim.effects.clear();
        sim.scheduler
            .hand_off(now, false, &mut drain, &mut sim.effects)
            .expect("hand_off succeeds");
        sim.verify_effects("hand_off_begin", false);
        sim.check_invariants("hand_off_begin");

        assert!(!drain.complete);
        let req = sim.build_random_request();
        sim.effects.clear();
        assert_eq!(
            sim.scheduler.execute(now, req, &mut sim.effects),
            Err(ExecuteError::HandingOff)
        );
        let reg = sim.make_worker_reg(
            0,
            Arc::new(InstanceId::try_new("i-new").expect("id")),
            false,
            false,
            None,
        );
        assert_eq!(
            sim.scheduler.register(now, reg, &mut sim.effects),
            Err(RegisterError::HandingOff)
        );
        for (_, worker) in sim.scheduler.workers().workers_sorted() {
            assert!(worker.drain.is_draining() && !worker.is_available_for_dispatch());
        }
        for pair in drain.workers.windows(2) {
            assert!((&pair[0].fleet, &pair[0].instance) < (&pair[1].fleet, &pair[1].instance));
        }

        let fleet0 = sim.fleet_name(0);
        let inst0 = InstanceId::try_new("i-f0-0").expect("id");
        sim.effects.clear();
        sim.scheduler
            .set_draining(now, &fleet0, &inst0, false, &mut sim.effects)
            .expect("set_draining");
        assert!(sim.effects.is_empty());

        drain_handoff_attempts(&mut sim, seed);

        sim.effects.clear();
        let final_now = sim.now();
        sim.scheduler
            .hand_off(final_now, true, &mut drain, &mut sim.effects)
            .expect("final handoff");
        assert!(drain.workers.is_empty());
        assert!(drain.complete);
        let rehome_count = sim
            .effects
            .iter()
            .filter(|e| matches!(e, Effect::RehomeWorker { .. }))
            .count();
        assert_eq!(rehome_count, sim.scheduler.workers().worker_count());
        sim.check_invariants("handoff_complete");
    }

    fn drain_handoff_attempts(sim: &mut Sim, seed: u64) {
        for _ in 0..200 {
            let live: Vec<(WorkerKey, AttemptId, AttemptPhase)> = sim
                .scheduler
                .attempts()
                .iter()
                .map(|(_, a)| (a.worker, a.id.clone(), a.phase))
                .collect();
            if live.is_empty() {
                break;
            }
            sim.advance_clock(200, false);
            for (worker, attempt_id, phase) in live {
                if sim.scheduler.workers().get(worker).is_none() {
                    continue;
                }
                let event = quiesce_event_for(phase, attempt_id, sim.wall_ms);
                sim.capture_pre_workers();
                let step_now = sim.now();
                sim.effects.clear();
                sim.scheduler
                    .on_worker(step_now, worker, event, &mut sim.effects);
                if phase == AttemptPhase::Running {
                    assert!(
                        !sim.effects.iter().any(|e| matches!(
                            e,
                            Effect::RehomeWorker { worker: w, .. } if *w == worker
                        )),
                        "seed={seed}: worker {worker:?} rehomed early on Executed"
                    );
                }
                for e in &sim.effects {
                    if let Effect::RehomeWorker {
                        worker: w,
                        reason: RehomeReason::Idle,
                    } = e
                    {
                        assert!(sim.scheduler.attempts().worker_attempts(*w).is_empty());
                    }
                }
                sim.verify_effects("handoff_drain_step", false);
            }
            sim.check_invariants("handoff_drain_tick");
        }
    }

    #[test]
    fn adversarial_handoff_rehome_and_drain_convergence() {
        for seed in 100_u64..108 {
            run_handoff_seed(seed);
        }
    }

    fn cordon_all_named(sim: &mut Sim, names: &[&str], draining: bool) {
        for fleet_idx in 0..sim.catalog.len() {
            let fleet = sim.fleet_name(fleet_idx);
            for &inst_name in names {
                let inst = InstanceId::try_new(inst_name).expect("id");
                sim.capture_pre_workers();
                sim.effects.clear();
                let _ = sim.scheduler.set_draining(
                    sim.now(),
                    &fleet,
                    &inst,
                    draining,
                    &mut sim.effects,
                );
                sim.verify_effects("cordon_batch", false);
            }
        }
        sim.check_invariants("cordon_batch");
    }

    fn verify_even_spread_and_cordon(sim: &mut Sim) {
        let mut small_workers = Vec::new();
        for slot in 0..3 {
            let inst = Arc::new(InstanceId::try_new(format!("i-spread-{slot}")).expect("id"));
            small_workers.push(
                sim.register_new_worker(0, inst, false, false, None)
                    .expect("register"),
            );
        }
        let large_w = sim
            .register_new_worker(
                2,
                Arc::new(InstanceId::try_new("i-large-0").expect("id")),
                false,
                false,
                None,
            )
            .expect("register large");

        for i in 0..6 {
            let req = simple_request(
                &format!("operations/spread-{i}"),
                format!("spread-{i}").as_bytes(),
                100,
                false,
                None,
                None,
            );
            sim.execute_request(req).expect("execute spread");
        }
        for &wk in &small_workers {
            let w = sim.scheduler.workers().get(wk).expect("worker");
            assert_eq!(w.inflight_attempts, 2);
            assert_eq!(w.ledger.reserved().cpu_millicores, 4_000);
        }
        assert_eq!(
            sim.scheduler
                .workers()
                .get(large_w)
                .expect("large")
                .inflight_attempts,
            0
        );
        cordon_all_named(
            sim,
            &["i-spread-0", "i-spread-1", "i-spread-2", "i-large-0"],
            true,
        );
    }

    fn exec_simple(
        sim: &mut Sim,
        name: &str,
        digest: &[u8],
        priority: i32,
        skip_cache: bool,
    ) -> OperationKey {
        sim.execute_request(simple_request(
            name, digest, priority, skip_cache, None, None,
        ))
        .expect(name)
    }

    #[test]
    fn adversarial_distributed_placement_and_dedup_promotion() {
        let mut sim = Sim::new(42, multi_fleet_catalog(), sim_policy(42));
        verify_even_spread_and_cordon(&mut sim);

        let op1 = exec_simple(&mut sim, "operations/p1", b"d1", 100, false);
        let op2 = exec_simple(&mut sim, "operations/p2", b"d2", 10, false);
        let op3 = exec_simple(&mut sim, "operations/p3", b"d3", 100, false);
        let seq2 = sim.scheduler.operations().get(op2).expect("op2").seq;

        assert_eq!(
            exec_simple(&mut sim, "operations/p2-d1", b"d2", 100, false),
            op2
        );
        assert_eq!(sim.scheduler.operations().get(op2).expect("op2").seq, seq2);
        let order: Vec<OperationKey> = sim.scheduler.queue().global_iter().map(|q| q.key).collect();
        assert_eq!(order, vec![op1, op2, op3]);

        // Lower priority must not demote; Gate priority (250) promotes to front with CpuWeight::GATE.
        assert_eq!(
            exec_simple(&mut sim, "operations/p2-lo", b"d2", -50, false),
            op2
        );
        assert_eq!(
            sim.scheduler.operations().get(op2).expect("op2").priority,
            100
        );
        assert_eq!(
            exec_simple(&mut sim, "operations/p2-gt", b"d2", 250, false),
            op2
        );
        assert_eq!(
            sim.scheduler.operations().get(op2).expect("op2").cpu_weight,
            CpuWeight::GATE
        );
        assert_ne!(
            exec_simple(&mut sim, "operations/p2-nc", b"d2", 250, true),
            op2
        );

        // Uncordon small workers, promote Running op1 to Gate (260), then disconnect its worker.
        // Regression for priority-ordered requeue on worker loss: op1 (priority 260) MUST take a
        // free slot on the remaining workers ahead of lower-priority (100) ops from the same host!
        cordon_all_named(&mut sim, &["i-spread-0", "i-spread-1", "i-spread-2"], false);
        assert_eq!(
            exec_simple(&mut sim, "operations/p1-gt", b"d1", 260, false),
            op1
        );
        let op1_worker = sim
            .scheduler
            .attempts()
            .active_for_operation(op1)
            .and_then(|ak| sim.scheduler.attempts().get(ak))
            .expect("op1 active attempt")
            .worker;
        sim.capture_pre_workers();
        sim.effects.clear();
        sim.scheduler
            .on_worker_lost(sim.now(), op1_worker, &mut sim.effects);
        assert!(sim.effects.iter().any(|e| matches!(
            e,
            Effect::AssignWorker { operation, cpu_weight, .. }
                if *operation == op1 && *cpu_weight == CpuWeight::GATE
        )));
        sim.verify_effects("promoted_worker_loss", false);
        sim.check_invariants("promoted_worker_loss");
        sim.run_to_quiescence();
    }

    fn exercise_disk_shortage_decline(
        sim: &mut Sim,
        w_small: WorkerKey,
        w_large: WorkerKey,
        op: OperationKey,
    ) -> AttemptId {
        let att1 = sim
            .scheduler
            .attempts()
            .active_for_operation(op)
            .and_then(|ak| sim.scheduler.attempts().get(ak))
            .expect("att1");
        assert_eq!(att1.worker, w_small);
        let att1_id = att1.id.clone();

        sim.deliver_worker_event(
            w_small,
            WorkerEvent::Decline {
                attempt_id: att1_id,
                reason: DeclineReason::DiskShortage,
            },
            "decline_disk",
            true,
        );
        assert!(
            sim.scheduler
                .workers()
                .get(w_small)
                .expect("small")
                .has_disk_hold()
        );
        assert_eq!(
            sim.scheduler.operations().get(op).expect("op").retry_counts,
            RetryCounts::default()
        );
        let att2 = sim
            .scheduler
            .attempts()
            .active_for_operation(op)
            .and_then(|ak| sim.scheduler.attempts().get(ak))
            .expect("att2 on large");
        assert_eq!(att2.worker, w_large);
        let att2_id = att2.id.clone();

        sim.deliver_worker_event(
            w_small,
            WorkerEvent::KeepAlive {
                cache: None,
                memory_capacity: None,
            },
            "clear_disk_hold",
            true,
        );
        assert!(
            !sim.scheduler
                .workers()
                .get(w_small)
                .expect("small")
                .has_disk_hold()
        );
        att2_id
    }

    #[test]
    fn adversarial_escalation_kill_floor_and_disk_shortage_cycles() {
        let mut sim = Sim::new(77, multi_fleet_catalog(), sim_policy(77));
        let w_small = sim
            .register_new_worker(
                0,
                Arc::new(InstanceId::try_new("i-esc-small").expect("id")),
                false,
                false,
                None,
            )
            .expect("small");
        let w_large = sim
            .register_new_worker(
                2,
                Arc::new(InstanceId::try_new("i-esc-large").expect("id")),
                false,
                false,
                None,
            )
            .expect("large");

        let target = Some(TargetLabel::try_new("//heavy:link").expect("target"));
        let mnemonic = Some(Mnemonic::try_new("Link").expect("mnemonic"));
        let op = sim
            .execute_request(simple_request(
                "operations/esc-mem",
                b"esc-mem",
                100,
                false,
                target.clone(),
                mnemonic.clone(),
            ))
            .expect("execute");
        let att2_id = exercise_disk_shortage_decline(&mut sim, w_small, w_large, op);

        // OOM kill with peak 18 GiB escalates reservation and records kill floor in ResourceSketch.
        sim.deliver_worker_event(
            w_large,
            WorkerEvent::Executed(WorkerExecuted {
                attempt_id: att2_id.clone(),
                usage: AttemptUsage {
                    peak_memory: Some(Bytes::gib(18)),
                    peak_disk: Some(Bytes::gib(2)),
                    cpu_time: Duration::from_secs(2),
                    wall_time: Duration::from_secs(2),
                    oom_kills: 1,
                },
                kill: Some(KillCause::Oom),
                side_channel_memory_failure: false,
            }),
            "oom_escalate",
            true,
        );
        assert_eq!(
            sim.scheduler
                .operations()
                .get(op)
                .expect("op")
                .retry_counts
                .memory_steps,
            1
        );
        sim.deliver_worker_event(
            w_large,
            WorkerEvent::Completed(WorkerCompleted {
                attempt_id: att2_id,
                status: CompletionStatus::Ok,
                execution_timestamps: None,
            }),
            "old_att_completed",
            true,
        );

        let op2 = sim
            .execute_request(simple_request(
                "operations/esc-mem-next",
                b"esc-mem-next",
                100,
                false,
                target,
                mnemonic,
            ))
            .expect("execute op2");
        assert!(
            sim.scheduler
                .operations()
                .get(op2)
                .expect("op2")
                .reservation
                .memory
                >= Bytes::gib(18)
        );
        sim.run_to_quiescence();
    }

    #[test]
    fn adversarial_cordons_overlapping_generations_and_backward_wall_clock() {
        let mut sim = Sim::new(99, multi_fleet_catalog(), sim_policy(99));
        let inst = Arc::new(InstanceId::try_new("i-overlap").expect("id"));
        let fleet0 = sim.fleet_name(0);

        let gen1 = sim
            .register_new_worker(0, Arc::clone(&inst), false, false, None)
            .expect("gen1");
        let gen2 = sim
            .register_new_worker(0, Arc::clone(&inst), false, false, None)
            .expect("gen2");
        let gen3_stale = sim
            .register_new_worker(0, Arc::clone(&inst), true, false, None)
            .expect("gen3 stale");
        for wk in [gen1, gen2, gen3_stale] {
            let w = sim.scheduler.workers().get(wk).expect("worker");
            assert!(
                w.drain.is_cordoned() && w.drain.is_draining() && !w.is_available_for_dispatch()
            );
        }
        assert!(sim.scheduler.cordons().is_replacing(0, &inst));

        sim.effects.clear();
        sim.scheduler
            .set_draining(sim.now(), &fleet0, &inst, false, &mut sim.effects)
            .expect("set_draining");
        assert!(sim.scheduler.cordons().is_replacing(0, &inst));
        sim.check_invariants("stale_survives_uncordon");

        let empty_census = Census::new(Vec::new());
        let present_census = Census::new(vec![(fleet0, Arc::clone(&inst))]);
        let grace = Duration::from_secs(10);

        sim.scheduler
            .retain_drains(sim.now().tick, grace, &empty_census);
        assert!(sim.scheduler.cordons().contains(0, &inst));
        sim.advance_clock(5_000, false);
        sim.scheduler
            .retain_drains(sim.now().tick, grace, &present_census);
        assert_eq!(sim.scheduler.cordons().absent_since(0, &inst), None);

        // Jump wall clock backward by 2 hours while monotonic tick advances.
        sim.wall_ms = sim.wall_ms.saturating_sub(2 * 3_600 * 1_000);
        sim.advance_clock(1_000, false);
        sim.scheduler
            .retain_drains(sim.now().tick, grace, &empty_census);
        assert!(sim.scheduler.cordons().contains(0, &inst));
        sim.advance_clock(11_000, false);
        sim.scheduler
            .retain_drains(sim.now().tick, grace, &empty_census);
        assert!(!sim.scheduler.cordons().contains(0, &inst));
        sim.check_invariants("after_backward_wall_and_cordon_prune");

        let healthy = sim
            .register_new_worker(
                0,
                Arc::new(InstanceId::try_new("i-healthy").expect("id")),
                false,
                false,
                None,
            )
            .expect("healthy");
        assert!(
            sim.scheduler
                .workers()
                .get(healthy)
                .expect("h")
                .is_available_for_dispatch()
        );
        for _ in 0..80 {
            sim.wall_ms = sim.wall_ms.saturating_sub(5_000).max(120_000);
            sim.step_random();
        }
        sim.run_to_quiescence();
    }

    #[test]
    fn regression_redundant_unwatch_does_not_delay_client_less_expiry() {
        let mut sim = Sim::new(314, multi_fleet_catalog(), sim_policy(314));
        let mut req = simple_request(
            "operations/unwatch-reg",
            b"unwatch-reg",
            100,
            false,
            None,
            None,
        );
        req.initial_watchers = 0;

        let op = sim.execute_request(req).expect("admit zero-watcher op");
        let original_deadline = sim
            .scheduler
            .next_deadline()
            .expect("client-less timer armed");

        // Halfway through client_action_timeout, a redundant unwatch must NOT push the deadline back.
        sim.advance_clock(6_000, false);
        sim.effects.clear();
        sim.scheduler.unwatch(sim.now(), op, &mut sim.effects);
        assert_eq!(sim.scheduler.next_deadline(), Some(original_deadline));

        sim.advance_to_tick(original_deadline);
        sim.action_timer_sweep();
        assert!(
            sim.scheduler.operations().get(op).is_none(),
            "client-less queued op must expire at original deadline despite redundant unwatch"
        );
    }

    fn record_term1_sample(
        sim: &mut Sim,
        worker: WorkerKey,
        target: TargetLabel,
        mnemonic: Mnemonic,
    ) {
        let op1 = sim
            .execute_request(simple_request(
                "operations/term1-a",
                b"term1-a",
                100,
                false,
                Some(target),
                Some(mnemonic),
            ))
            .expect("op1");
        let att1 = sim
            .scheduler
            .attempts()
            .active_for_operation(op1)
            .and_then(|ak| sim.scheduler.attempts().get(ak))
            .expect("att1")
            .id
            .clone();
        let start_wall = sim.now().wall;
        sim.advance_clock(3_500, false);
        let end_wall = sim.now().wall;

        sim.deliver_worker_event(
            worker,
            WorkerEvent::Accept {
                attempt_id: att1.clone(),
            },
            "term1_accept",
            true,
        );
        sim.deliver_worker_event(
            worker,
            WorkerEvent::Executed(WorkerExecuted {
                attempt_id: att1.clone(),
                usage: AttemptUsage {
                    peak_memory: Some(Bytes::gib(10)),
                    peak_disk: Some(Bytes::gib(12)),
                    cpu_time: Duration::from_secs(14),
                    wall_time: Duration::from_millis(3_500),
                    oom_kills: 0,
                },
                kill: None,
                side_channel_memory_failure: false,
            }),
            "term1_executed",
            true,
        );
        sim.deliver_worker_event(
            worker,
            WorkerEvent::Completed(WorkerCompleted {
                attempt_id: att1,
                status: CompletionStatus::Ok,
                execution_timestamps: Some((start_wall, end_wall)),
            }),
            "term1_completed",
            true,
        );
    }

    #[test]
    fn term_boundary_learned_preservation_across_scheduler_terms() {
        let catalog = multi_fleet_catalog();
        let policy = sim_policy(2026);
        let mut sim1 = Sim::new(2026, catalog.clone(), policy.clone());
        let w1 = sim1
            .register_new_worker(
                2,
                Arc::new(InstanceId::try_new("i-term1").expect("id")),
                false,
                false,
                None,
            )
            .expect("term1 worker");

        let target = TargetLabel::try_new("//core:compiler").expect("target");
        let mnemonic = Mnemonic::try_new("Rustc").expect("mnemonic");
        record_term1_sample(&mut sim1, w1, target.clone(), mnemonic.clone());

        let snapshot_wall = sim1.now().wall;
        let learned1 = sim1.scheduler.into_learned();
        let byte_limit = learned1.byte_limit().get();
        let mut records: Vec<LearnedHistory> = Vec::new();
        learned1.snapshot(snapshot_wall, &mut records);
        assert!(!records.is_empty());
        assert!(records.iter().all(|r| r.body.len() <= byte_limit));

        let mut learned2 =
            Learned::new(&policy.actions, &policy.scaling, catalog.len()).expect("fresh learned2");
        learned2
            .restore(snapshot_wall + Duration::from_secs(1), &records)
            .expect("restore v1 records");

        let key = SketchKey::for_attempt(Some(target.as_str()), Some(mnemonic.as_str()), "");
        let entry = learned2
            .histories_mut()
            .resource_mut()
            .lookup(&key, (snapshot_wall.as_unix_millis() / 1000) + 1)
            .expect("restored sketch entry");
        let shaped = learned2.histories().resource().shape(&entry);
        assert!(shaped.memory_kb >= 10 * 1024 * 1024);
        assert!(shaped.disk_kb >= 12 * 1024 * 1024);

        let mut sim2 = Sim::with_learned(2027, catalog, policy, learned2);
        sim2.wall_ms = snapshot_wall.as_unix_millis() + 2_000;
        sim2.register_new_worker(
            2,
            Arc::new(InstanceId::try_new("i-term2").expect("id")),
            false,
            false,
            None,
        )
        .expect("term2 worker");

        let op2 = sim2
            .execute_request(simple_request(
                "operations/term2-a",
                b"term2-a",
                100,
                false,
                Some(target),
                Some(mnemonic),
            ))
            .expect("term2 execute");
        let rec = sim2.scheduler.operations().get(op2).expect("op2");
        assert!(rec.reservation.memory >= Bytes::gib(10));
        assert!(rec.reservation.disk >= Bytes::gib(12));

        let mut snap = SchedulerSnapshot::default();
        sim2.scheduler.snapshot(sim2.now(), &mut snap);
        assert_eq!(snap.executing.len(), 1);
        assert_eq!(
            snap.executing[0].expected,
            Some(Duration::from_millis(3_500))
        );

        for _ in 0..100 {
            sim2.step_random();
        }
        sim2.run_to_quiescence();
    }
}
