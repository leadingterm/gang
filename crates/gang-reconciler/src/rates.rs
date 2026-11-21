//! Count-conserved arrival estimates, global source periods and resource-vector charges.

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Tick;
use gang_config::{FleetPolicy, ScalingConfig};
use gang_spi::{CompletionRates, SchedulerSnapshot};
use gang_types::{SourceId, property::MILLICORES_PER_CORE};

use crate::profile::{ActivityForecast, SourceProfiles};
use crate::vectors::{CapacityVector, Horizon, ResourceVector, product, u64_to_f64, u128_to_u64};
use crate::{FleetPass, HostedSnapshot, PassTime};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct FleetRates {
    pub persistent_load: CapacityVector,
    pub arrival_load: CapacityVector,
    pub completion_load: CapacityVector,
    pub total_load: CapacityVector,
    pub transient_work: CapacityVector,
    pub persistent_queue: CapacityVector,
    pub queued_work: CapacityVector,
    pub serving_load: CapacityVector,
    pub persistent_streams: u32,
    pub streams: u32,
}

#[derive(Clone, Copy, Debug, Default)]
struct Moments {
    all: f64,
    timed: f64,
    work: ResourceVector,
}

impl Moments {
    fn row(row: &CompletionRates, at: Tick, kappa: f64) -> Self {
        let decay = decay(kappa, at, row.at);
        Self {
            all: product(row.completions.get(), decay),
            timed: product(row.timed_completions.get(), decay),
            work: ResourceVector::from_rates(row.work).scaled(decay),
        }
    }

    fn scaled(self, factor: f64) -> Self {
        Self {
            all: product(self.all, factor),
            timed: product(self.timed, factor),
            work: self.work.scaled(factor),
        }
    }

    fn add(self, rhs: Self) -> Self {
        Self {
            all: (self.all + rhs.all).min(f64::MAX),
            timed: (self.timed + rhs.timed).min(f64::MAX),
            work: self.work + rhs.work,
        }
    }

    fn sub_floor(self, rhs: Self) -> Self {
        Self {
            all: (self.all - rhs.all).max(0.0),
            timed: (self.timed - rhs.timed).max(0.0),
            work: self.work.sub_floor(rhs.work),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct CompletionDelta {
    moments: Moments,
    window_start: Option<Tick>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RefillEvidence {
    #[default]
    None,
    Growing,
    Pending,
    Confirmed,
}

#[derive(Clone, Copy, Debug, Default)]
struct Cell {
    seeded: bool,
    nbar: f64,
    n_prev: f64,
    running_prev: ResourceVector,
    rbar: ResourceVector,
    moments: Moments,
    cohort_start: Option<Tick>,
    completion_credit: f64,
    admission_high_water: f64,
    refill: RefillEvidence,
    // Per-pass scratch, reset without disturbing either estimator's history.
    n: f64,
    running: ResourceVector,
    preferred_running: ResourceVector,
    inflight_work: ResourceVector,
    fresh: Moments,
    completion_window_start: Option<Tick>,
    persistent: bool,
    refill_failed: Option<Tick>,
}

impl Cell {
    fn clear_pass(&mut self) {
        self.n = 0.0;
        self.running = ResourceVector::default();
        self.preferred_running = ResourceVector::default();
        self.inflight_work = ResourceVector::default();
        self.fresh = Moments::default();
        self.completion_window_start = None;
    }

    fn reset_history(&mut self) {
        self.seeded = false;
        self.nbar = 0.0;
        self.n_prev = 0.0;
        self.running_prev = ResourceVector::default();
        self.rbar = ResourceVector::default();
        self.moments = Moments::default();
        self.persistent = false;
        self.refill_failed = None;
        self.reset_cohort();
    }

    fn reset_cohort(&mut self) {
        self.cohort_start = None;
        self.completion_credit = 0.0;
        self.admission_high_water = 0.0;
        self.refill = RefillEvidence::None;
    }

    fn observe_cohort(&mut self, kappa: f64, now: Tick, frame_start: Option<Tick>) -> bool {
        if self.n == 0.0 {
            self.reset_cohort();
            return false;
        }
        let Some(start) = self.cohort_start.filter(|_| self.n_prev > 0.0) else {
            self.reset_cohort();
            self.cohort_start = Some(now);
            self.admission_high_water = self.n;
            return false;
        };
        if frame_start.is_some_and(|at| at >= start) {
            // Rate reads precede listings. Discard crossing windows and require
            // evidence to survive another aligned frame: receipt/listing alias
            // remains bounded by the adapter, not an atomic conservation proof.
            if self.completion_window_start.is_some_and(|at| at >= start) {
                self.completion_credit =
                    (self.completion_credit + self.fresh.all / kappa).min(f64::MAX);
            }
            let admitted = (self.n + self.completion_credit.floor()).min(f64::MAX);
            let has_completions = self.completion_credit >= 1.0;
            self.refill = match self.refill {
                RefillEvidence::Confirmed => RefillEvidence::Confirmed,
                RefillEvidence::Pending if admitted >= self.admission_high_water => {
                    RefillEvidence::Confirmed
                }
                RefillEvidence::Growing
                    if has_completions
                        && self.n > self.n_prev
                        && admitted > self.admission_high_water =>
                {
                    RefillEvidence::Confirmed
                }
                _ if has_completions && admitted > self.admission_high_water => {
                    RefillEvidence::Pending
                }
                _ if !has_completions && admitted > self.admission_high_water => {
                    RefillEvidence::Growing
                }
                _ => RefillEvidence::None,
            };
            self.admission_high_water = self.admission_high_water.max(admitted);
        }
        self.refill == RefillEvidence::Confirmed && self.n >= self.n_prev
    }

    fn estimate(
        &mut self,
        gauge_decay: f64,
        moment_decay: f64,
        read_to_now: f64,
        kappa: f64,
        unknown_seconds: f64,
    ) -> (f64, f64, ResourceVector, ResourceVector) {
        if self.seeded {
            self.nbar += (self.n_prev - self.nbar) * (1.0 - gauge_decay);
            self.rbar = self.rbar.scaled(gauge_decay) + self.running_prev.scaled(1.0 - gauge_decay);
        } else {
            self.nbar = self.n;
            self.rbar = self.running;
            self.seeded = true;
        }
        // Equal ticks still retain the newest sample for the next held interval.
        self.n_prev = self.n;
        self.running_prev = self.running;
        self.moments = self.moments.scaled(moment_decay).add(self.fresh);
        // Divide BEFORE the common final decay. Timed zero is a valid zero mean;
        // untimed completions never enter its denominator.
        let mean = if self.moments.timed > 0.0 {
            divide_work(self.moments.work, self.moments.timed)
        } else if self.n > 0.0 {
            divide_work(self.inflight_work, self.n)
        } else {
            // No live reservations to infer physical work from. The configured
            // duration prior still supplies count work, never adapter service.
            ResourceVector {
                actions: unknown_seconds,
                ..ResourceVector::default()
            }
        };
        let completed = product(self.moments.all, read_to_now);
        let arrived = (kappa * (self.n - self.nbar) + completed).clamp(0.0, f64::MAX);
        (arrived, completed, mean.scaled(arrived), mean)
    }

    fn classify(
        &mut self,
        arrived: f64,
        completed: f64,
        forecast: ActivityForecast,
        horizon: Horizon,
        scaling: &ScalingConfig,
        now: Tick,
    ) {
        let refill = if completed > 0.0 {
            arrived
                >= product(
                    completed,
                    f64::from(scaling.scale_in_waterline_percent.get()) / 100.0,
                )
        } else {
            self.n >= self.nbar
        };
        if refill {
            self.refill_failed = None;
            let persistence = forecast.age.max(forecast.learned_mean.unwrap_or_default());
            if persistence >= horizon.readiness.saturating_add(horizon.payback) {
                self.persistent = true;
            }
        } else {
            let since = *self.refill_failed.get_or_insert(now);
            if now.saturating_duration_since(since) >= scaling.load_half_life.get() {
                self.persistent = false;
            }
        }
    }
}

#[derive(Clone, Copy)]
struct SourceState {
    id: Option<SourceId>,
    occupied: bool,
    selected: bool,
    active_at: Option<Tick>,
    last_active: Option<Tick>,
    last_n: f64,
    quiet_since: Option<Tick>,
    open: bool,
    known_sum: u128,
    known_count: u64,
    forecast: ActivityForecast,
}

impl Default for SourceState {
    fn default() -> Self {
        Self {
            id: None,
            occupied: false,
            selected: false,
            active_at: None,
            last_active: None,
            last_n: 0.0,
            quiet_since: None,
            open: false,
            known_sum: 0,
            known_count: 0,
            forecast: ActivityForecast {
                age: Duration::ZERO,
                learned_mean: None,
                new_period: false,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FleetScratch {
    preferred_persistent: ResourceVector,
    arrival_load: ResourceVector,
    completion_load: ResourceVector,
    preferred_total: ResourceVector,
    preferred_persistent_actual: ResourceVector,
    overflow_persistent: ResourceVector,
    overflow_total: ResourceVector,
    actual: ResourceVector,
    serving: ResourceVector,
    transient: ResourceVector,
    persistent_queue: ResourceVector,
    queued: ResourceVector,
}

/// Bounded distinct-source state with fixed per-preferred-fleet cells. Raw adapter
/// frames live independently so slot reuse never loses the previous event baseline.
pub(crate) struct SourceRates {
    scaling: Arc<ScalingConfig>,
    profiles: SourceProfiles,
    kappa: f64,
    fleet_count: usize,
    cpu_oversubscription: NonZeroU32,
    index: HashMap<SourceId, usize>,
    sources: Vec<SourceState>,
    cells: Vec<Cell>,
    fleet_scratch: Vec<FleetScratch>,
    rates: Vec<FleetRates>,
    queued_durations: Vec<Duration>,
    executing_durations: Vec<Duration>,
    worker_fleets: Vec<Option<usize>>,
    worker_serving: Vec<bool>,
    worker_resources: Vec<ResourceVector>,
    previous_rows: Vec<CompletionRates>,
    deltas: Vec<CompletionDelta>,
    previous_read: Option<Tick>,
    previous_pass: Option<Tick>,
}

impl fmt::Debug for SourceRates {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceRates")
            .field("fleets", &self.fleet_count)
            .field("sources", &self.index.len())
            .field("source_limit", &self.scaling.max_sources.get())
            .field("completion_rows", &self.previous_rows.len())
            .finish_non_exhaustive()
    }
}

impl SourceRates {
    pub fn new(
        policies: &[FleetPolicy],
        scaling: Arc<ScalingConfig>,
        profiles: SourceProfiles,
        cpu_oversubscription: NonZeroU32,
    ) -> Self {
        let source_count = scaling.max_sources.get();
        let fleet_count = policies.len();
        let slots = source_count.saturating_add(1);
        let mut sources = vec![SourceState::default(); slots];
        sources[source_count].occupied = true; // Reserved unattributed state, never evicted.
        Self {
            kappa: std::f64::consts::LN_2 / scaling.load_half_life.get().as_secs_f64(),
            scaling,
            profiles,
            fleet_count,
            cpu_oversubscription,
            index: HashMap::with_capacity(source_count),
            sources,
            cells: vec![Cell::default(); slots.saturating_mul(fleet_count)],
            fleet_scratch: vec![FleetScratch::default(); fleet_count],
            rates: vec![FleetRates::default(); fleet_count],
            queued_durations: Vec::new(),
            executing_durations: Vec::new(),
            worker_fleets: Vec::new(),
            worker_serving: Vec::new(),
            worker_resources: Vec::new(),
            previous_rows: Vec::with_capacity(slots.saturating_mul(fleet_count)),
            deltas: Vec::with_capacity(slots.saturating_mul(fleet_count)),
            previous_read: None,
            previous_pass: None,
        }
    }

    /// A failed census proves neither quiet nor a continuous idle interval.
    /// Keep raw rows and gauge history: the next good sample spans the gap.
    pub fn interrupt(&mut self) {
        for source in &mut self.sources {
            source.quiet_since = None;
        }
    }

    pub fn fleet(&self, index: usize) -> &FleetRates {
        &self.rates[index]
    }
    pub fn queued_duration(&self, index: usize) -> Duration {
        self.queued_durations[index]
    }
    pub fn executing_duration(&self, index: usize) -> Duration {
        self.executing_durations[index]
    }

    pub fn observe(
        &mut self,
        fleets: &[FleetPass],
        hosted: &HostedSnapshot,
        attribution: &[Option<usize>],
        capacities: &[CapacityVector],
        horizons: &[Horizon],
        time: PassTime,
    ) {
        let snapshot = hosted.snapshot();
        self.reset_pass(fleets, snapshot);
        self.compute_deltas(snapshot);
        self.select_sources(snapshot);
        self.observe_actions(snapshot, fleets, time.now);
        self.observe_completion_activity(snapshot, time.now);
        self.observe_periods(time);
        self.classify_streams(snapshot.source_rates_at, horizons, time.now);
        self.charge_overflow(snapshot, fleets);
        self.charge_queue(snapshot, fleets, attribution);
        self.finish_fleet_rates(fleets, capacities);
        self.profiles.finish_pass(time);
        self.previous_rows.clear();
        self.previous_rows.extend_from_slice(&snapshot.source_rates);
        self.previous_read = Some(snapshot.source_rates_at);
        self.previous_pass = Some(time.now);
    }

    fn reset_pass(&mut self, fleets: &[FleetPass], snapshot: &SchedulerSnapshot) {
        for source in &mut self.sources {
            source.selected = false;
            source.active_at = None;
            source.known_sum = 0;
            source.known_count = 0;
        }
        for cell in &mut self.cells {
            cell.clear_pass();
        }
        self.fleet_scratch.fill(FleetScratch::default());
        self.rates.fill(FleetRates::default());
        self.worker_fleets.clear();
        self.worker_serving.clear();
        self.worker_resources.clear();
        for worker in &snapshot.workers {
            let fleet = fleets
                .iter()
                .take(self.fleet_count)
                .position(|fleet| fleet.policy.name == worker.fleet);
            let serving = !worker.draining
                && fleet.is_some_and(|fleet| {
                    worker.id.instance_name().is_some_and(|instance| {
                        snapshot
                            .cordons
                            .binary_search_by(|cordon| {
                                cordon
                                    .fleet
                                    .cmp(&fleet)
                                    .then_with(|| cordon.instance.as_str().cmp(instance))
                            })
                            .is_err()
                    })
                });
            self.worker_fleets.push(fleet);
            self.worker_serving.push(serving);
            self.worker_resources.push(ResourceVector::default());
        }
    }

    fn select_sources(&mut self, snapshot: &SchedulerSnapshot) {
        // Pin existing current-demand sources BEFORE admitting any new source. Old
        // decaying adapter tails cannot evict them or resurrect stale activity.
        for row in &snapshot.queued {
            self.pin(row.source);
        }
        for row in &snapshot.executing {
            self.pin(row.source);
        }
        for (row, delta) in snapshot.source_rates.iter().zip(&self.deltas) {
            if delta.moments.all > 0.0
                && let Some(slot) = self.lookup(row.source)
            {
                self.sources[slot].selected = true;
            }
        }
        for row in &snapshot.queued {
            self.select(row.source);
        }
        for row in &snapshot.executing {
            self.select(row.source);
        }
        for i in 0..snapshot.source_rates.len() {
            if self.deltas[i].moments.all > 0.0 {
                self.select(snapshot.source_rates[i].source);
            }
        }
    }

    fn observe_actions(&mut self, snapshot: &SchedulerSnapshot, fleets: &[FleetPass], now: Tick) {
        for row in &snapshot.queued {
            self.known_duration(row.source, row.expected);
        }
        for row in &snapshot.executing {
            self.known_duration(row.source, row.expected);
        }
        self.queued_durations.clear();
        self.executing_durations.clear();
        for row in &snapshot.queued {
            let duration = self.duration(row.source, row.expected);
            self.queued_durations.push(duration);
            self.current(
                row.source,
                row.candidates.iter().find(|&index| index < fleets.len()),
                ResourceVector::from_action(row.resources),
                duration,
                None,
                now,
            );
        }
        for row in &snapshot.executing {
            let duration = self.duration(row.source, row.expected);
            self.executing_durations.push(duration);
            let actual = self.worker_fleets.get(row.worker).copied().flatten();
            let vector = ResourceVector::from_action(row.resources);
            self.current(
                row.source,
                row.candidates.iter().find(|&index| index < fleets.len()),
                vector,
                duration,
                actual,
                now,
            );
            if let Some(actual) = actual {
                self.worker_resources[row.worker] += vector;
                self.fleet_scratch[actual].actual += vector;
                if self
                    .worker_serving
                    .get(row.worker)
                    .copied()
                    .unwrap_or(false)
                {
                    self.fleet_scratch[actual].serving += vector;
                }
            }
        }
        // Listings can lag a worker's running count. Reserve the missing
        // worker's resource budget without creating any source attribution.
        for (index, worker) in snapshot.workers.iter().enumerate() {
            let Some(fleet) = self.worker_fleets[index] else {
                continue;
            };
            let known = self.worker_resources[index];
            if f64::from(worker.running_actions) <= known.actions {
                continue;
            }
            let policy = &fleets[fleet].policy;
            let cpu = u64::from(policy.vcpus_per_instance.get())
                .saturating_mul(MILLICORES_PER_CORE)
                .saturating_mul(u64::from(self.cpu_oversubscription.get()));
            let budget = ResourceVector {
                cpu_millicores: u64_to_f64(cpu),
                memory: u64_to_f64(policy.memory_per_instance.get().get()),
                disk: u64_to_f64(policy.disk_per_instance.get().get()),
                ..ResourceVector::default()
            };
            let missing = budget.sub_floor(known);
            self.fleet_scratch[fleet].actual += missing;
            if self.worker_serving[index] {
                self.fleet_scratch[fleet].serving += missing;
            }
        }
    }

    fn observe_completion_activity(&mut self, snapshot: &SchedulerSnapshot, now: Tick) {
        for (row, fresh) in snapshot.source_rates.iter().zip(&self.deltas) {
            let Some(slot) = self.lookup(row.source) else {
                continue;
            };
            if row.preferred_fleet >= self.fleet_count {
                continue;
            }
            let cell = &mut self.cells[slot * self.fleet_count + row.preferred_fleet];
            cell.fresh = fresh.moments;
            cell.completion_window_start = fresh.window_start;
            if fresh.moments.all > 0.0 {
                let source = &mut self.sources[slot];
                let receipt = row.at.min(now);
                source.active_at = Some(source.active_at.map_or(receipt, |at| at.max(receipt)));
            }
        }
    }

    fn classify_streams(&mut self, read_at: Tick, horizons: &[Horizon], now: Tick) {
        let gauge_decay = self
            .previous_pass
            .map_or(1.0, |at| decay(self.kappa, now, at));
        let moment_decay = self
            .previous_read
            .map_or(1.0, |at| decay(self.kappa, read_at, at));
        let read_to_now = decay(self.kappa, now, read_at);
        let unknown_seconds = self.scaling.unknown_action_duration.get().as_secs_f64();
        for slot in 0..self.sources.len() {
            if !self.sources[slot].occupied || !self.sources[slot].open {
                continue;
            }
            for preferred in 0..self.fleet_count {
                let cell = &mut self.cells[slot * self.fleet_count + preferred];
                if !cell.seeded && cell.n == 0.0 && cell.fresh.all == 0.0 {
                    continue;
                }
                let forecast_ready = cell.observe_cohort(
                    self.kappa,
                    now,
                    self.previous_read.filter(|&at| read_at > at),
                );
                let (arrived, completed, rho, mean) = cell.estimate(
                    gauge_decay,
                    moment_decay,
                    read_to_now,
                    self.kappa,
                    unknown_seconds,
                );
                if let Some(horizon) = horizons.get(preferred) {
                    cell.classify(
                        arrived,
                        completed,
                        self.sources[slot].forecast,
                        *horizon,
                        &self.scaling,
                        now,
                    );
                }
                let overflow = cell.running.sub_floor(cell.preferred_running);
                let scratch = &mut self.fleet_scratch[preferred];
                scratch.preferred_total += rho
                    .component_max(cell.rbar)
                    .component_max(cell.running)
                    .sub_floor(overflow)
                    .component_max(cell.preferred_running);
                self.rates[preferred].streams = self.rates[preferred].streams.saturating_add(1);
                if cell.persistent {
                    if forecast_ready {
                        scratch.arrival_load += rho.sub_floor(overflow);
                        scratch.completion_load += mean.scaled(completed).sub_floor(overflow);
                    }
                    scratch.preferred_persistent += rho
                        .sub_floor(overflow)
                        .component_max(cell.preferred_running);
                    scratch.preferred_persistent_actual += cell.preferred_running;
                    self.rates[preferred].persistent_streams =
                        self.rates[preferred].persistent_streams.saturating_add(1);
                }
            }
        }
    }

    fn charge_overflow(&mut self, snapshot: &SchedulerSnapshot, fleets: &[FleetPass]) {
        // No source x preferred x actual matrix: route actual overflow directly
        // from execution rows AFTER classification has settled.
        for row in &snapshot.executing {
            let Some(actual) = self.worker_fleets.get(row.worker).copied().flatten() else {
                continue;
            };
            let preferred = row.candidates.iter().find(|&index| index < fleets.len());
            if preferred == Some(actual) {
                continue;
            }
            let vector = ResourceVector::from_action(row.resources);
            self.fleet_scratch[actual].overflow_total += vector;
            if self.is_persistent(row.source, preferred) {
                self.fleet_scratch[actual].overflow_persistent += vector;
            }
        }
    }

    fn charge_queue(
        &mut self,
        snapshot: &SchedulerSnapshot,
        fleets: &[FleetPass],
        attribution: &[Option<usize>],
    ) {
        for (i, row) in snapshot.queued.iter().enumerate() {
            let Some(Some(fleet)) = attribution.get(i) else {
                continue;
            };
            if *fleet >= self.fleet_count {
                continue;
            }
            let vector = ResourceVector::from_action(row.resources);
            let work = vector.scaled(self.queued_durations[i].as_secs_f64());
            let preferred = row.candidates.iter().find(|&index| index < fleets.len());
            let persistent = self.is_persistent(row.source, preferred);
            let scratch = &mut self.fleet_scratch[*fleet];
            scratch.queued += work;
            if persistent {
                scratch.persistent_queue += vector;
            } else {
                scratch.transient += work;
            }
        }
    }

    fn finish_fleet_rates(&mut self, fleets: &[FleetPass], capacities: &[CapacityVector]) {
        for (fleet, pass) in fleets.iter().enumerate().take(self.fleet_count) {
            let scratch = &mut self.fleet_scratch[fleet];
            let cap = capacities.get(fleet).copied().unwrap_or_default();
            let normalize =
                |vector: ResourceVector| vector.normalized(&pass.policy, self.cpu_oversubscription);
            let persistent = normalize(scratch.preferred_persistent)
                .component_min(cap)
                .component_max(normalize(scratch.preferred_persistent_actual))
                + normalize(scratch.overflow_persistent);
            let total = (normalize(scratch.preferred_total).component_min(cap)
                + normalize(scratch.overflow_total))
            .component_max(normalize(scratch.actual))
            .component_max(persistent);
            let rate = &mut self.rates[fleet];
            rate.persistent_load = persistent;
            rate.arrival_load = normalize(scratch.arrival_load);
            rate.completion_load = normalize(scratch.completion_load);
            rate.total_load = total;
            rate.transient_work = normalize(scratch.transient);
            rate.persistent_queue = normalize(scratch.persistent_queue);
            rate.queued_work = normalize(scratch.queued);
            rate.serving_load = normalize(scratch.serving);
        }
    }

    fn compute_deltas(&mut self, snapshot: &SchedulerSnapshot) {
        self.deltas.clear();
        let mut previous = self.previous_rows.iter().peekable();
        for row in &snapshot.source_rates {
            let key = (row.source, row.preferred_fleet);
            while previous
                .peek()
                .is_some_and(|old| (old.source, old.preferred_fleet) < key)
            {
                previous.next();
            }
            let old = previous
                .peek()
                .filter(|old| (old.source, old.preferred_fleet) == key);
            let current = Moments::row(row, snapshot.source_rates_at, self.kappa);
            // The default also excludes all pre-term events on the first frame.
            let fresh = match (self.previous_read, old) {
                (Some(_), Some(old)) if old.generation == row.generation => {
                    current.sub_floor(Moments::row(old, snapshot.source_rates_at, self.kappa))
                }
                (Some(at), _) if row.created_at >= at => current,
                _ => Moments::default(),
            };
            self.deltas.push(CompletionDelta {
                moments: fresh,
                // Counter birth/reuse may supply estimator moments, never
                // completion credit for the preceding listing cohort.
                window_start: old
                    .filter(|old| old.generation == row.generation)
                    .and(self.previous_read)
                    .filter(|&at| snapshot.source_rates_at >= at),
            });
        }
    }

    fn lookup(&self, source: Option<SourceId>) -> Option<usize> {
        match source {
            Some(id) => self.index.get(&id).copied(),
            None => self.sources.len().checked_sub(1),
        }
    }

    fn pin(&mut self, source: Option<SourceId>) {
        if let Some(slot) = self.lookup(source) {
            self.sources[slot].selected = true;
        }
    }

    fn select(&mut self, source: Option<SourceId>) {
        if let Some(slot) = self.lookup(source) {
            self.sources[slot].selected = true;
            return;
        }
        let Some(id) = source else {
            return;
        };
        let bound = self.sources.len().saturating_sub(1);
        let slot = self.sources[..bound]
            .iter()
            .position(|state| !state.occupied)
            .or_else(|| {
                self.sources[..bound]
                    .iter()
                    .enumerate()
                    .filter(|(_, state)| !state.selected)
                    .min_by_key(|(slot, state)| (state.last_active, *slot))
                    .map(|(slot, _)| slot)
            });
        let Some(slot) = slot else {
            return;
        };
        if let Some(old) = self.sources[slot].id {
            self.index.remove(&old);
        }
        self.sources[slot] = SourceState {
            id: Some(id),
            occupied: true,
            selected: true,
            ..SourceState::default()
        };
        self.cells[slot * self.fleet_count..(slot + 1) * self.fleet_count].fill(Cell::default());
        self.index.insert(id, slot);
    }

    fn known_duration(&mut self, source: Option<SourceId>, duration: Option<Duration>) {
        if source.is_none() {
            return;
        }
        if let (Some(slot), Some(duration)) = (self.lookup(source), duration) {
            let state = &mut self.sources[slot];
            state.known_sum = state.known_sum.saturating_add(duration.as_nanos());
            state.known_count = state.known_count.saturating_add(1);
        }
    }

    fn duration(&self, source: Option<SourceId>, expected: Option<Duration>) -> Duration {
        if let Some(expected) = expected {
            return expected;
        }
        if source.is_some()
            && let Some(slot) = self.lookup(source)
        {
            let state = &self.sources[slot];
            if state.known_count > 0 {
                let nanos =
                    (state.known_sum / u128::from(state.known_count)).min(Duration::MAX.as_nanos());
                return Duration::new(
                    u128_to_u64(nanos / 1_000_000_000),
                    (nanos % 1_000_000_000) as u32,
                );
            }
        }
        self.scaling.unknown_action_duration.get()
    }

    fn current(
        &mut self,
        source: Option<SourceId>,
        preferred: Option<usize>,
        vector: ResourceVector,
        duration: Duration,
        actual: Option<usize>,
        now: Tick,
    ) {
        let Some(slot) = self.lookup(source) else {
            return;
        };
        self.sources[slot].active_at = Some(now);
        let Some(preferred) = preferred.filter(|&fleet| fleet < self.fleet_count) else {
            return;
        };
        let cell = &mut self.cells[slot * self.fleet_count + preferred];
        cell.n += 1.0;
        cell.inflight_work += vector.scaled(duration.as_secs_f64());
        if let Some(actual) = actual {
            cell.running += vector;
            if actual == preferred {
                cell.preferred_running += vector;
            }
        }
    }

    fn observe_periods(&mut self, time: PassTime) {
        for slot in 0..self.sources.len() {
            if !self.sources[slot].occupied {
                continue;
            }
            let source = &mut self.sources[slot];
            let cells = &mut self.cells[slot * self.fleet_count..(slot + 1) * self.fleet_count];
            let n: f64 = cells.iter().map(|cell| cell.n).sum();
            if let Some(active_at) = source.active_at {
                let forecast = self.profiles.observe(source.id, active_at, time);
                // A profile may split conservatively over a failed scan. A last
                // observed nonzero gauge is not evidence that a quiet period ran.
                let split = source.open
                    && forecast.new_period
                    && source.last_n == 0.0
                    && source.last_active.is_some_and(|at| {
                        active_at.saturating_duration_since(at) >= self.scaling.source_idle.get()
                    });
                if split || !source.open {
                    for cell in cells.iter_mut() {
                        cell.reset_history();
                    }
                }
                source.forecast = forecast;
                source.open = true;
                source.quiet_since = None;
                source.last_active =
                    Some(source.last_active.map_or(active_at, |at| at.max(active_at)));
            } else if source.open {
                let quiet = *source.quiet_since.get_or_insert(time.now);
                if time.now.saturating_duration_since(quiet) >= self.scaling.source_idle.get() {
                    self.profiles.close(source.id, time);
                    source.open = false;
                    for cell in cells.iter_mut() {
                        cell.reset_history();
                    }
                } else {
                    source.forecast = self.profiles.forecast(source.id, time);
                }
            }
            source.last_n = n;
        }
    }

    fn is_persistent(&self, source: Option<SourceId>, preferred: Option<usize>) -> bool {
        let Some(slot) = self.lookup(source) else {
            return false;
        };
        let Some(preferred) = preferred.filter(|&fleet| fleet < self.fleet_count) else {
            return false;
        };
        self.sources[slot].open && self.cells[slot * self.fleet_count + preferred].persistent
    }
}

fn decay(kappa: f64, now: Tick, previous: Tick) -> f64 {
    (-kappa * now.saturating_duration_since(previous).as_secs_f64()).exp()
}

fn divide_work(work: ResourceVector, denominator: f64) -> ResourceVector {
    ResourceVector {
        actions: (work.actions / denominator).min(f64::MAX),
        cpu_millicores: (work.cpu_millicores / denominator).min(f64::MAX),
        memory: (work.memory / denominator).min(f64::MAX),
        disk: (work.disk / denominator).min(f64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::{Clock, TestClock, Timestamp};
    use gang_config::{HistoryWindow, ScalingInterval, SourceLimit};
    use gang_spi::{ExecutingDemand, NonNegativeFinite, QueuedDemand, ResourceRates, WorkerState};
    use gang_testkit::{fleet_policy, worker_id};
    use gang_types::{ActionResources, FleetSet, NonZeroBytes, nonzero_u32};

    fn tick(seconds: u64) -> Tick {
        Tick::ORIGIN + Duration::from_secs(seconds)
    }

    fn time(seconds: u64) -> PassTime {
        PassTime {
            now: tick(seconds),
            wall: Timestamp::default() + Duration::from_secs(seconds),
        }
    }

    fn source(number: u64) -> Option<SourceId> {
        format!("{number:016x}").parse().ok()
    }

    fn finite(value: f64) -> NonNegativeFinite {
        NonNegativeFinite::try_new(value).unwrap()
    }

    fn resource_rates(vector: ResourceVector) -> ResourceRates {
        ResourceRates {
            actions: finite(vector.actions),
            cpu_millicores: finite(vector.cpu_millicores),
            memory: finite(vector.memory),
            disk: finite(vector.disk),
        }
    }

    fn test_resources() -> ActionResources {
        ActionResources {
            cpu_millicores: 16_000,
            ..ActionResources::default()
        }
    }

    fn queued(source: Option<SourceId>, preferred: usize, expected: Option<u64>) -> QueuedDemand {
        QueuedDemand {
            source,
            candidates: FleetSet::try_from_indices([preferred]).unwrap(),
            resources: test_resources(),
            expected: expected.map(Duration::from_secs),
            queued_since: Timestamp::default(),
            priority: 0,
        }
    }

    fn executing(
        source: Option<SourceId>,
        preferred: usize,
        worker: usize,
        expected: Option<u64>,
    ) -> ExecutingDemand {
        ExecutingDemand {
            source,
            candidates: FleetSet::try_from_indices([preferred]).unwrap(),
            resources: test_resources(),
            expected: expected.map(Duration::from_secs),
            started: Timestamp::default(),
            worker,
        }
    }

    fn worker(fleet: &FleetPolicy, instance: &str, running: u32, draining: bool) -> WorkerState {
        WorkerState {
            id: worker_id(instance).unwrap(),
            fleet: fleet.name.clone(),
            environment: Some(fleet.action_environment.clone()),
            running_actions: running,
            max_inflight_actions: Some(nonzero_u32(16)),
            draining,
            paused: false,
            last_seen: Timestamp::default(),
        }
    }

    struct World {
        clock: TestClock,
        rates: SourceRates,
        fleets: Vec<FleetPass>,
        snapshot: SchedulerSnapshot,
        capacities: Vec<CapacityVector>,
        horizons: Vec<Horizon>,
    }

    impl World {
        fn new(fleet_count: usize, source_count: usize) -> Self {
            let scaling = ScalingConfig {
                max_sources: SourceLimit::try_new(source_count).unwrap(),
                ..ScalingConfig::default()
            };
            Self::configured(fleet_count, scaling)
        }

        fn configured(fleet_count: usize, scaling: ScalingConfig) -> Self {
            let policies: Vec<_> = (0..fleet_count)
                .map(|index| {
                    let mut policy = fleet_policy(&format!("fleet-{index}")).unwrap();
                    policy.vcpus_per_instance = nonzero_u32(16);
                    policy.memory_per_instance = NonZeroBytes::gib(16);
                    policy.disk_per_instance = NonZeroBytes::gib(16);
                    policy
                })
                .collect();
            let profiles = SourceProfiles::new(&scaling, HistoryWindow::from_units(7));
            let rates = SourceRates::new(&policies, Arc::new(scaling), profiles, nonzero_u32(1));
            Self {
                clock: TestClock::new(Timestamp::default()),
                rates,
                fleets: policies.into_iter().map(FleetPass::new).collect(),
                snapshot: SchedulerSnapshot::default(),
                capacities: vec![
                    CapacityVector {
                        cpu: 100.0,
                        memory: 100.0,
                        disk: 100.0
                    };
                    fleet_count
                ],
                horizons: vec![
                    Horizon {
                        readiness: Duration::from_secs(1),
                        payback: Duration::from_secs(1)
                    };
                    fleet_count
                ],
            }
        }

        fn observe(&mut self, seconds: u64) {
            self.observe_skew(seconds, seconds);
        }

        fn observe_skew(&mut self, read: u64, pass: u64) {
            self.clock.advance(
                Duration::from_secs(pass).saturating_sub(self.clock.monotonic().elapsed()),
            );
            self.snapshot.source_rates_at = tick(read);
            let mut snapshot = self.snapshot.clone();
            let mut hosted = HostedSnapshot::default();
            hosted.replace(&mut snapshot);
            let attribution: Vec<_> = self
                .snapshot
                .queued
                .iter()
                .map(|row| row.candidates.iter().next())
                .collect();
            self.rates.observe(
                &self.fleets,
                &hosted,
                &attribution,
                &self.capacities,
                &self.horizons,
                PassTime::read(&self.clock),
            );
        }

        fn queue(&mut self, count: usize, expected: Option<u64>) {
            self.snapshot.queued = vec![queued(source(1), 0, expected); count];
        }

        fn cell(&self, id: Option<SourceId>, preferred: usize) -> &Cell {
            &self.rates.cells[self.rates.lookup(id).unwrap() * self.rates.fleet_count + preferred]
        }

        // A real adapter-shaped event kernel, with receipt ticks independent of
        // reconciliation cadence. None timing contributes C_all only.
        fn complete(
            &mut self,
            id: Option<SourceId>,
            preferred: usize,
            at: u64,
            count: u32,
            duration: Option<u64>,
            resources: ActionResources,
        ) {
            let kappa = self.rates.kappa;
            let index = self
                .snapshot
                .source_rates
                .iter()
                .position(|row| row.source == id && row.preferred_fleet == preferred);
            let row = if let Some(index) = index {
                &mut self.snapshot.source_rates[index]
            } else {
                self.snapshot.source_rates.push(CompletionRates {
                    source: id,
                    preferred_fleet: preferred,
                    generation: 1,
                    created_at: tick(at),
                    at: tick(at),
                    completions: finite(0.0),
                    timed_completions: finite(0.0),
                    work: ResourceRates::default(),
                });
                self.snapshot.source_rates.last_mut().unwrap()
            };
            let old = Moments::row(row, tick(at), kappa);
            let increment = kappa * f64::from(count);
            row.at = tick(at);
            row.completions = finite(old.all + increment);
            row.timed_completions =
                finite(old.timed + if duration.is_some() { increment } else { 0.0 });
            row.work = resource_rates(
                old.work
                    + duration.map_or(ResourceVector::default(), |duration| {
                        ResourceVector::from_action(resources)
                            .scaled(increment * Duration::from_secs(duration).as_secs_f64())
                    }),
            );
            self.snapshot
                .source_rates
                .sort_unstable_by_key(|row| (row.source, row.preferred_fleet));
        }
    }

    fn near(actual: f64, expected: f64, tolerance: f64) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "{actual} != {expected} (tolerance {tolerance})"
        );
    }

    fn exact(actual: f64, expected: f64) {
        assert_eq!(actual.to_bits(), expected.to_bits());
    }

    fn sample_cohort(
        cell: &mut Cell,
        n: f64,
        completions: f64,
        frame_start: Option<u64>,
        completion_start: Option<u64>,
        now: u64,
    ) -> bool {
        cell.n = n;
        cell.fresh = Moments {
            all: completions,
            ..Moments::default()
        };
        cell.completion_window_start = completion_start.map(tick);
        let ready = cell.observe_cohort(1.0, tick(now), frame_start.map(tick));
        cell.estimate(1.0, 1.0, 1.0, 1.0, 1.0);
        ready
    }

    #[test]
    fn finite_cohort_completions_never_prove_future_admissions() {
        let mut cell = Cell::default();
        assert!(!sample_cohort(&mut cell, 10.0, 0.0, None, None, 0));
        for (now, n, completions) in [(1, 8.0, 1.9), (2, 5.0, 2.9), (3, 1.0, 3.9)] {
            assert!(!sample_cohort(
                &mut cell,
                n,
                completions,
                Some(now - 1),
                Some(now - 1),
                now,
            ));
        }
        assert_eq!(cell.refill, RefillEvidence::None);
        exact(cell.admission_high_water, 10.0);
    }

    #[test]
    fn repeated_admissions_accumulate_fractional_completion_credit() {
        let mut cell = Cell::default();
        assert!(!sample_cohort(&mut cell, 4.0, 0.0, None, None, 0));
        assert!(!sample_cohort(&mut cell, 4.0, 0.9, Some(0), Some(0), 1));
        assert!(!sample_cohort(&mut cell, 4.0, 0.9, Some(1), Some(1), 2));
        assert!(!sample_cohort(&mut cell, 4.0, 0.0, None, None, 2));
        assert!(sample_cohort(&mut cell, 4.0, 0.9, Some(2), Some(2), 3));
        assert_eq!(cell.refill, RefillEvidence::Confirmed);
        // Retained refill history must not extrapolate while the cohort drains.
        assert!(!sample_cohort(&mut cell, 3.0, 0.0, Some(3), Some(3), 4));
        assert_eq!(cell.refill, RefillEvidence::Confirmed);
    }

    #[test]
    fn zero_completion_multichunk_upload_never_forecasts_held_work() {
        let mut cell = Cell {
            persistent: true,
            ..Cell::default()
        };
        assert!(!sample_cohort(&mut cell, 20.0, 0.0, None, None, 0));
        assert!(!sample_cohort(&mut cell, 90.0, 0.0, Some(0), None, 1));
        assert_eq!(cell.refill, RefillEvidence::Growing);
        assert!(!sample_cohort(&mut cell, 160.0, 0.0, Some(1), None, 2));
        assert_eq!(cell.refill, RefillEvidence::Growing);
        for now in 3..=1024 {
            assert!(!sample_cohort(
                &mut cell,
                160.0,
                0.0,
                Some(now - 1),
                None,
                now,
            ));
            assert_ne!(cell.refill, RefillEvidence::Pending);
            assert_ne!(cell.refill, RefillEvidence::Confirmed);
        }
        exact(cell.completion_credit, 0.0);
        exact(cell.admission_high_water, 160.0);
    }

    #[test]
    fn growing_cohort_requires_completion_backed_later_growth() {
        let mut cell = Cell::default();
        assert!(!sample_cohort(&mut cell, 20.0, 0.0, None, None, 0));
        assert!(!sample_cohort(&mut cell, 90.0, 0.0, Some(0), None, 1));
        assert!(!sample_cohort(&mut cell, 160.0, 0.9, Some(1), Some(1), 2));
        assert_eq!(cell.refill, RefillEvidence::Growing);
        assert!(sample_cohort(&mut cell, 170.0, 0.2, Some(2), Some(2), 3));
        assert_eq!(cell.refill, RefillEvidence::Confirmed);
    }

    #[test]
    fn observed_empty_restarts_cohort_even_with_retained_persistence() {
        let mut cell = Cell {
            persistent: true,
            ..Cell::default()
        };
        assert!(!sample_cohort(&mut cell, 4.0, 0.0, None, None, 0));
        assert!(!sample_cohort(&mut cell, 5.0, 1.0, Some(0), Some(0), 1));
        assert!(sample_cohort(&mut cell, 5.0, 1.0, Some(1), Some(1), 2));
        assert!(!sample_cohort(&mut cell, 0.0, 0.0, Some(2), None, 3));
        assert!(cell.persistent);
        assert!(!sample_cohort(&mut cell, 100.0, 10.0, Some(3), Some(3), 4));
        assert!(!sample_cohort(&mut cell, 100.0, 0.0, Some(4), Some(4), 5));
        exact(cell.completion_credit, 0.0);
        exact(cell.admission_high_water, 100.0);
    }

    #[test]
    fn preseed_read_listing_skew_cannot_supply_completion_credit() {
        let mut cell = Cell::default();
        // Two of a finite ten finish between read 0 and the first listing at 2.
        assert!(!sample_cohort(&mut cell, 8.0, 0.0, None, None, 2));
        assert!(!sample_cohort(&mut cell, 8.0, 1.9, Some(0), Some(0), 4));
        assert!(!sample_cohort(&mut cell, 8.0, 0.0, Some(3), Some(3), 5));
        exact(cell.completion_credit, 0.0);
        assert_eq!(cell.refill, RefillEvidence::None);
    }

    #[test]
    fn counter_birth_generation_and_single_frame_alias_do_not_prove_refill() {
        let mut cell = Cell::default();
        assert!(!sample_cohort(&mut cell, 10.0, 0.0, None, None, 0));
        // Birth/reuse has no same-generation completion window.
        assert!(!sample_cohort(&mut cell, 10.0, 100.0, Some(0), None, 1));
        assert!(!sample_cohort(&mut cell, 10.0, 100.0, Some(1), None, 2));
        exact(cell.completion_credit, 0.0);
        // A late listing can overlap one frame's completions; wait for confirmation.
        assert!(!sample_cohort(&mut cell, 10.0, 1.9, Some(2), Some(2), 3));
        assert!(!sample_cohort(&mut cell, 8.0, 0.0, Some(3), Some(3), 4));
        assert_eq!(cell.refill, RefillEvidence::None);
        cell.reset_history();
        assert!(cell.cohort_start.is_none());
        exact(cell.completion_credit, 0.0);
    }

    #[test]
    fn qualified_arrival_and_completion_work_remain_unclipped() {
        let mut world = World::new(1, 2);
        world.capacities[0].cpu = 1.0;
        world.queue(16, Some(20));
        world.observe(0);
        world.complete(source(1), 0, 20, 16, Some(20), test_resources());
        world.queue(20, Some(20));
        world.observe(20);
        assert_eq!(world.rates.fleet(0).arrival_load, CapacityVector::default());
        world.complete(source(1), 0, 40, 16, Some(20), test_resources());
        world.queue(24, Some(20));
        world.observe(40);
        let weight = world.rates.kappa * (1.0 + decay(world.rates.kappa, tick(40), tick(20)));
        let rates = world.rates.fleet(0);
        near(rates.arrival_load.cpu, 20.0 * weight * 20.0, 1e-12);
        near(rates.completion_load.cpu, 16.0 * weight * 20.0, 1e-12);
        exact(rates.persistent_load.cpu, 1.0);
        assert!(rates.arrival_load.cpu > rates.persistent_load.cpu);
        world.queue(23, Some(20));
        world.observe(41);
        assert_eq!(world.rates.fleet(0).arrival_load, CapacityVector::default());
        assert_eq!(
            world.rates.fleet(0).completion_load,
            CapacityVector::default()
        );
    }

    #[test]
    fn counter_birth_and_reuse_never_credit_cohort_completions() {
        let mut world = World::new(1, 2);
        world.queue(10, Some(1));
        world.observe(0);
        world.complete(source(1), 0, 1, 2, Some(1), test_resources());
        world.queue(8, Some(1));
        world.observe(1);
        exact(world.cell(source(1), 0).completion_credit, 0.0);
        world.snapshot.source_rates[0].generation += 1;
        world.snapshot.source_rates[0].created_at = tick(2);
        world.snapshot.source_rates[0].at = tick(2);
        world.snapshot.source_rates[0].completions = finite(100.0 * world.rates.kappa);
        world.observe(2);
        exact(world.cell(source(1), 0).completion_credit, 0.0);
        world.observe(3);
        exact(world.cell(source(1), 0).completion_credit, 0.0);
        assert_eq!(world.rates.fleet(0).arrival_load, CapacityVector::default());
    }

    #[test]
    fn static_dump_first_completion_does_not_reset_known_gauge() {
        for expected in [Some(30), None] {
            let mut world = World::new(1, 4);
            world.queue(160, expected);
            world.observe(0);
            for batch in 1..=10 {
                world.queue(160 - batch * 16, expected);
                world.complete(
                    source(1),
                    0,
                    batch as u64 * 30,
                    16,
                    Some(30),
                    test_resources(),
                );
                world.observe(batch as u64 * 30);
                let cell = world.cell(source(1), 0);
                let completion = cell.moments.all;
                let arrivals = (world.rates.kappa * (cell.n - cell.nbar) + completion).max(0.0);
                near(arrivals, 0.0, 1e-12);
                assert!(!cell.persistent);
                assert_eq!(
                    world.rates.fleet(0).persistent_queue,
                    CapacityVector::default()
                );
            }
        }
    }

    #[test]
    fn brand_new_subpass_completion_has_positive_offered_load() {
        let mut world = World::new(1, 2);
        world.observe(0);
        world.complete(source(1), 0, 5, 3, Some(2), test_resources());
        world.observe(5);
        let cell = world.cell(source(1), 0);
        near(cell.moments.all, 3.0 * world.rates.kappa, 1e-15);
        exact(cell.nbar, 0.0);
        near(
            world.rates.fleet(0).total_load.cpu,
            6.0 * world.rates.kappa,
            1e-15,
        );
    }

    #[test]
    fn first_term_discards_preexisting_completion_rows() {
        let mut world = World::new(1, 2);
        world.queue(10, Some(10));
        world.complete(source(1), 0, 0, 100, Some(30), test_resources());
        world.observe(10);
        exact(world.cell(source(1), 0).moments.all, 0.0);
        assert_eq!(world.rates.fleet(0).total_load, CapacityVector::default());
        exact(world.cell(source(1), 0).nbar, 10.0);
    }

    #[test]
    fn generation_reuse_keeps_gauge_and_accepts_only_fresh_generation() {
        let mut world = World::new(1, 2);
        world.queue(160, Some(30));
        world.observe(0);
        world.queue(144, Some(30));
        world.complete(source(1), 0, 30, 16, Some(30), test_resources());
        world.snapshot.source_rates[0].generation = 9;
        world.observe(30);
        exact(world.cell(source(1), 0).nbar, 160.0);
        near(world.rates.fleet(0).total_load.cpu, 0.0, 1e-12);
        world.snapshot.source_rates[0].generation = 10;
        world.snapshot.source_rates[0].created_at = tick(1);
        world.snapshot.source_rates[0].at = tick(31);
        world.snapshot.source_rates[0].completions = finite(100.0);
        world.observe(31);
        assert!(world.cell(source(1), 0).moments.all < 1.0);
        assert!(world.cell(source(1), 0).nbar > 144.0);
    }

    #[test]
    fn row_read_tick_not_later_pass_tick_fences_new_rows() {
        let mut world = World::new(1, 2);
        world.observe_skew(0, 10);
        world.complete(source(1), 0, 5, 1, Some(4), test_resources());
        world.observe_skew(11, 20);
        let expected = 4.0 * world.rates.kappa * decay(world.rates.kappa, tick(20), tick(5));
        near(world.rates.fleet(0).total_load.cpu, expected, 1e-14);
    }

    #[test]
    fn equal_ticks_retain_newest_count_for_next_sample_hold() {
        let mut world = World::new(1, 2);
        world.queue(10, Some(2));
        world.observe(0);
        world.queue(20, Some(2));
        world.observe(0);
        exact(world.cell(source(1), 0).nbar, 10.0);
        exact(world.cell(source(1), 0).n_prev, 20.0);
        world.observe(60);
        near(world.cell(source(1), 0).nbar, 15.0, 1e-12);
    }

    #[test]
    fn quiet_close_zeros_load_exactly_and_keeps_raw_tail_baseline() {
        let mut world = World::new(1, 2);
        world
            .snapshot
            .workers
            .push(worker(&world.fleets[0].policy, "local-1", 1, false));
        world
            .snapshot
            .executing
            .push(executing(source(1), 0, 0, Some(10)));
        world.observe(0);
        world.snapshot.executing.clear();
        world.snapshot.workers[0].running_actions = 0;
        world.complete(source(1), 0, 10, 1, Some(10), test_resources());
        world.observe(10);
        world.observe(11); // first GOOD quiet observation
        world.observe(70);
        assert!(world.rates.fleet(0).total_load.cpu > 0.0);
        world.observe(71);
        assert_eq!(world.rates.fleet(0).total_load, CapacityVector::default());
        assert_eq!(
            world.rates.fleet(0).persistent_load,
            CapacityVector::default()
        );
        exact(world.cell(source(1), 0).nbar, 0.0);
        assert_eq!(world.cell(source(1), 0).rbar, ResourceVector::default());
        assert_eq!(world.rates.previous_rows.len(), 1);
        assert_eq!(
            world
                .rates
                .profiles
                .forecast(source(1), time(71))
                .learned_mean,
            Some(Duration::from_secs(10))
        );
        world.complete(source(1), 0, 80, 1, Some(2), test_resources());
        world.observe(80);
        near(
            world.cell(source(1), 0).moments.all,
            world.rates.kappa,
            1e-14,
        );
        near(
            world.cell(source(1), 0).moments.work.actions,
            2.0 * world.rates.kappa,
            1e-14,
        );
    }

    #[test]
    fn skipped_passes_delay_quiet_close_without_reseeding_nonzero_gauge() {
        let mut world = World::new(1, 2);
        world.queue(16, Some(10));
        world.observe(0);
        world.rates.interrupt();
        world.queue(8, Some(10));
        world.observe(100);
        exact(world.cell(source(1), 0).nbar, 16.0);
        world.queue(0, Some(10));
        world.observe(101);
        world.rates.interrupt();
        world.observe(160);
        world.observe(161);
        assert!(world.cell(source(1), 0).seeded);
        world.observe(220);
        assert!(!world.cell(source(1), 0).seeded);
        assert_eq!(world.rates.fleet(0).total_load, CapacityVector::default());
    }

    #[test]
    fn short_lull_keeps_period_but_long_unobserved_quiet_split_seeds_current() {
        let mut world = World::new(1, 2);
        world.queue(16, Some(10));
        world.observe(0);
        world.queue(0, Some(10));
        world.observe(5);
        world.queue(8, Some(10));
        world.observe(20);
        assert!(world.cell(source(1), 0).nbar > 8.0);
        let slot = world.rates.lookup(source(1)).unwrap();
        assert_eq!(
            world.rates.sources[slot].forecast.age,
            Duration::from_secs(20)
        );
        world.queue(0, Some(10));
        world.observe(21);
        world.rates.interrupt();
        world.queue(12, Some(10));
        world.observe(100);
        exact(world.cell(source(1), 0).nbar, 12.0);
        exact(world.cell(source(1), 0).n_prev, 12.0);
        exact(world.cell(source(1), 0).moments.all, 0.0);
        assert_eq!(world.rates.fleet(0).total_load, CapacityVector::default());
    }

    #[test]
    fn closed_loop_concurrency_steps_refind_persistence_under_waterline_tolerance() {
        for concurrency in [60, 32] {
            let mut world = World::new(1, 2);
            world.queue(64, Some(10));
            world.observe(0);
            for at in 1..=300 {
                world.queue(if at < 100 { 64 } else { concurrency }, Some(10));
                world.complete(source(1), 0, at, 8, Some(10), test_resources());
                world.observe(at);
            }
            assert!(world.cell(source(1), 0).persistent);
            exact(
                world.rates.fleet(0).persistent_queue.cpu,
                f64::from(u32::try_from(concurrency).unwrap()),
            );
            let cell = world.cell(source(1), 0);
            let arrived = (world.rates.kappa * (cell.n - cell.nbar) + cell.moments.all).max(0.0);
            assert!(arrived >= 0.7 * cell.moments.all);
        }
    }

    #[test]
    fn open_loop_excess_rises_and_no_refill_decays_after_hysteresis() {
        let mut world = World::new(1, 2);
        world.queue(10, Some(10));
        world.observe(0);
        world.queue(50, Some(10));
        world.observe(10);
        let excess = world.rates.fleet(0).total_load.cpu;
        assert!(excess > 0.0);
        assert!(world.cell(source(1), 0).persistent);
        world.queue(0, Some(10));
        world.complete(source(1), 0, 11, 1, Some(10), test_resources());
        world.observe(11);
        world.observe(70);
        assert!(world.cell(source(1), 0).persistent);
        world.observe(71);
        assert!(!world.cell(source(1), 0).persistent);
        assert!(world.rates.fleet(0).total_load.cpu < excess);
    }

    #[test]
    fn untimed_completions_do_not_dilute_timed_service_mean_and_zero_is_valid() {
        let mut world = World::new(1, 2);
        world.queue(10, Some(30));
        world.observe(0);
        world.complete(source(1), 0, 1, 1, Some(4), test_resources());
        world.complete(source(1), 0, 1, 9, None, test_resources());
        world.observe(1);
        near(
            world.rates.fleet(0).total_load.cpu,
            40.0 * world.rates.kappa,
            1e-13,
        );
        exact(world.cell(source(1), 0).moments.timed, world.rates.kappa);
        world.complete(source(1), 0, 2, 1, Some(0), test_resources());
        world.observe(2);
        let moments = world.cell(source(1), 0).moments;
        assert!(moments.work.actions / moments.timed < 4.0);
        assert!(moments.work.actions / moments.timed > 0.0);
    }

    #[test]
    fn finite_underflow_uses_inflight_duration_instead_of_zero_over_zero() {
        let mut cell = Cell {
            seeded: true,
            nbar: 10.0,
            n_prev: 10.0,
            n: 20.0,
            moments: Moments {
                all: 1.0,
                timed: 1.0,
                work: ResourceVector {
                    actions: 99.0,
                    ..ResourceVector::default()
                },
            },
            inflight_work: ResourceVector {
                actions: 40.0,
                ..ResourceVector::default()
            },
            ..Cell::default()
        };
        let (arrived, completed, rho, _) = cell.estimate(0.0, 0.0, 0.0, 0.1, 10.0);
        exact(completed, 0.0);
        exact(arrived, 1.0);
        exact(rho.actions, 2.0);
        assert!(rho.actions.is_finite());
    }

    #[test]
    fn duration_chain_pools_all_source_fleets_and_skips_none() {
        let mut world = World::new(2, 2);
        world.snapshot.queued = vec![
            queued(source(1), 0, Some(20)),
            queued(source(1), 1, None),
            queued(None, 0, Some(100)),
            queued(None, 0, None),
        ];
        world
            .snapshot
            .workers
            .push(worker(&world.fleets[1].policy, "local-1", 1, true));
        world
            .snapshot
            .executing
            .push(executing(source(1), 1, 0, Some(40)));
        world.observe(0);
        assert_eq!(world.rates.queued_duration(0), Duration::from_secs(20));
        assert_eq!(world.rates.queued_duration(1), Duration::from_secs(30));
        assert_eq!(world.rates.queued_duration(3), Duration::from_secs(10));
        assert_eq!(world.rates.executing_duration(0), Duration::from_secs(40));
        exact(world.rates.fleet(1).transient_work.cpu, 30.0);
        exact(world.rates.fleet(1).total_load.cpu, 1.0);
        exact(world.rates.fleet(1).serving_load.cpu, 0.0);
    }

    #[test]
    fn source_bound_is_distinct_sources_not_source_fleet_pairs() {
        let mut world = World::new(2, 1);
        world.snapshot.queued = vec![
            queued(source(1), 0, Some(20)),
            queued(source(1), 1, None),
            queued(source(2), 1, Some(40)),
            queued(source(2), 1, None),
        ];
        world.observe(0);
        assert_eq!(world.rates.index.len(), 1);
        assert!(world.rates.lookup(source(1)).is_some());
        assert_eq!(world.rates.queued_duration(1), Duration::from_secs(20));
        assert_eq!(world.rates.queued_duration(2), Duration::from_secs(40));
        assert_eq!(world.rates.queued_duration(3), Duration::from_secs(10));
        exact(world.rates.fleet(1).transient_work.cpu, 70.0);
        exact(world.rates.fleet(1).queued_work.cpu, 70.0);
        exact(world.cell(source(1), 0).n, 1.0);
        exact(world.cell(source(1), 1).n, 1.0);
    }

    #[test]
    fn eviction_is_cold_and_stale_adapter_tails_cannot_evict_active_queue() {
        let mut world = World::new(1, 1);
        world.queue(160, Some(30));
        world.observe(0);
        world.complete(source(1), 0, 1, 1, Some(30), test_resources());
        world.snapshot.queued = vec![queued(source(2), 0, Some(10)); 8];
        world.observe(1);
        // Fresh completion protects retained active state for this one pass.
        world.observe(2);
        assert!(world.rates.lookup(source(2)).is_some());
        assert!(world.rates.lookup(source(1)).is_none());
        world.observe(3);
        assert!(world.rates.lookup(source(2)).is_some());
        world.queue(160, Some(30));
        world.observe(4);
        exact(world.cell(source(1), 0).nbar, 160.0);
        exact(world.cell(source(1), 0).moments.all, 0.0);
        assert_eq!(world.rates.fleet(0).total_load, CapacityVector::default());
    }

    #[test]
    fn preferred_charge_subtracts_actual_overflow_including_busy_drains() {
        let mut world = World::new(2, 2);
        world.snapshot.queued = vec![queued(source(1), 0, Some(10)); 4];
        world.snapshot.workers = vec![
            worker(&world.fleets[0].policy, "local-1", 2, false),
            worker(&world.fleets[1].policy, "local-2", 3, true),
        ];
        world.snapshot.executing = vec![executing(source(1), 0, 0, Some(10)); 2];
        world
            .snapshot
            .executing
            .extend(vec![executing(source(1), 0, 1, Some(10)); 3]);
        world.observe(0);
        world.complete(source(1), 0, 10, 100, Some(10), test_resources());
        world.observe(10);
        let rho = 1000.0 * world.rates.kappa;
        near(world.rates.fleet(0).persistent_load.cpu, rho - 3.0, 1e-12);
        exact(world.rates.fleet(1).persistent_load.cpu, 3.0);
        exact(world.rates.fleet(1).total_load.cpu, 3.0);
        exact(world.rates.fleet(1).serving_load.cpu, 0.0);
        exact(world.rates.fleet(0).persistent_queue.cpu, 4.0);
        exact(world.cell(source(1), 0).n, 9.0);
        assert!(!world.cell(source(1), 1).seeded);
        world.capacities[0].cpu = 1.0;
        world.observe(11);
        exact(world.rates.fleet(0).persistent_load.cpu, 2.0);
        assert!(
            world
                .rates
                .fleet(0)
                .persistent_load
                .within(world.rates.fleet(0).total_load, 1.0)
        );
    }

    #[test]
    fn two_sources_candidate_sets_and_invalid_rows_keep_actual_floors() {
        let mut world = World::new(2, 2);
        world.snapshot.workers = vec![
            worker(&world.fleets[0].policy, "local-1", 2, false),
            worker(&world.fleets[1].policy, "local-2", 4, false),
        ];
        world.snapshot.executing = vec![
            executing(source(1), 0, 0, Some(10)),
            executing(source(2), 1, 1, Some(10)),
        ];
        let mut invalid = executing(source(3), 0, 1, Some(10));
        invalid.candidates = FleetSet::EMPTY;
        invalid.resources.cpu_millicores = 8000;
        world.snapshot.executing.push(invalid);
        world.snapshot.queued = vec![
            queued(source(1), 0, Some(10)),
            queued(source(2), 1, Some(10)),
        ];
        world.observe(0);
        exact(world.cell(source(1), 0).n, 2.0);
        exact(world.cell(source(2), 1).n, 2.0);
        exact(world.cell(source(1), 1).n, 0.0);
        exact(world.rates.fleet(0).total_load.cpu, 1.0);
        exact(world.rates.fleet(1).total_load.cpu, 1.5);
        exact(world.rates.fleet(1).total_load.memory, 1.0);
        exact(world.rates.fleet(1).serving_load.disk, 1.0);
        assert!(world.rates.lookup(source(3)).is_none());
    }

    #[test]
    fn complementary_queue_resources_remain_componentwise() {
        let mut world = World::new(1, 2);
        let mut cpu = queued(source(1), 0, Some(10));
        cpu.resources.cpu_millicores = 16_000;
        let mut memory = queued(source(2), 0, Some(10));
        memory.resources.cpu_millicores = 0;
        memory.resources.memory = world.fleets[0].policy.memory_per_instance.get();
        world.snapshot.queued = vec![cpu, memory];
        world.observe(0);
        let work = world.rates.fleet(0).transient_work;
        exact(work.cpu, 10.0);
        exact(work.memory, 10.0);
        exact(work.max_component(), 10.0);
    }
    #[test]
    fn missing_execution_rows_occupy_only_the_affected_worker() {
        let mut world = World::new(1, 2);
        world.capacities[0] = CapacityVector::ONE.scaled(4.0);
        for index in 0..4 {
            world.snapshot.workers.push(worker(
                &world.fleets[0].policy,
                &format!("local-{index}"),
                u32::from(index == 0),
                false,
            ));
        }
        world.observe(0);
        assert_eq!(world.rates.fleet(0).serving_load, CapacityVector::ONE);
        assert_eq!(world.rates.fleet(0).total_load, CapacityVector::ONE);
        assert_eq!(world.rates.index.len(), 0);
    }

    #[test]
    fn missing_rows_preserve_known_reservations_above_worker_budget() {
        let mut world = World::new(1, 2);
        world
            .snapshot
            .workers
            .push(worker(&world.fleets[0].policy, "local-1", 2, false));
        let mut row = executing(source(1), 0, 0, Some(10));
        row.resources.cpu_millicores = 32_000;
        world.snapshot.executing.push(row);
        world.observe(0);
        let load = world.rates.fleet(0).serving_load;
        exact(load.cpu, 2.0);
        exact(load.memory, 1.0);
        exact(load.disk, 1.0);
        assert_eq!(world.rates.index.len(), 1);
    }

    #[test]
    fn cpu_oversubscription_normalizes_running_load_but_not_memory() {
        let mut world = World::new(1, 2);
        world.rates.cpu_oversubscription = nonzero_u32(2);
        world
            .snapshot
            .workers
            .push(worker(&world.fleets[0].policy, "local-1", 1, false));
        let mut row = executing(source(1), 0, 0, Some(10));
        row.resources.memory = world.fleets[0].policy.memory_per_instance.get();
        world.snapshot.executing.push(row);
        world.observe(0);
        let load = world.rates.fleet(0).serving_load;
        exact(load.cpu, 0.5);
        exact(load.memory, 1.0);
        exact(
            load.utilization(CapacityVector::ONE.scaled(2.0)).unwrap(),
            0.5,
        );
    }

    fn trace(cadence: u64) -> (Moments, f64) {
        let mut world = World::configured(
            1,
            ScalingConfig {
                source_idle: ScalingInterval::from_units(3_600_000),
                ..ScalingConfig::default()
            },
        );
        // Modeled six-instance build: 96 running plus 64 queued actions.
        for index in 0..6 {
            world.snapshot.workers.push(worker(
                &world.fleets[0].policy,
                &format!("local-{index}"),
                16,
                false,
            ));
            world
                .snapshot
                .executing
                .extend(vec![executing(source(1), 0, index, Some(7)); 16]);
        }
        world.queue(64, Some(7));
        world.observe(0);
        for second in 1..=600 {
            // Modeled sustained build: variable verified completion counts and
            // durations, with small concurrency steps, identical at both cadences.
            world.complete(
                source(1),
                0,
                second,
                10 + u32::try_from(second % 7).unwrap(),
                Some(2 + second % 11),
                test_resources(),
            );
            if second % cadence == 0 {
                world.queue(64 + usize::try_from((second / 31) % 5).unwrap(), Some(7));
                world.observe(second);
            }
        }
        let cell = world.cell(source(1), 0);
        let arrived = (world.rates.kappa * (cell.n - cell.nbar) + cell.moments.all).max(0.0);
        let offered = arrived * cell.moments.work.actions / cell.moments.timed;
        (cell.moments, offered)
    }

    #[test]
    fn event_moments_are_interval_invariant_and_realistic_offered_trace_within_one_percent() {
        let (one, offered_one) = trace(1);
        let (five, offered_five) = trace(5);
        near(one.all, five.all, 1e-12);
        near(one.timed, five.timed, 1e-12);
        near(one.work.actions, five.work.actions, 1e-10);
        assert!((offered_one - offered_five).abs() / offered_one <= 0.01);
    }

    #[test]
    fn sample_hold_alias_respects_step_bound_not_exact_arrival_claim() {
        let kappa = std::f64::consts::LN_2 / 60.0;
        let mut early = Cell {
            n: 100.0,
            ..Cell::default()
        };
        early.estimate(1.0, 1.0, 1.0, kappa, 10.0);
        early.n = 200.0;
        early.estimate(1.0, 1.0, 1.0, kappa, 10.0); // step just after previous read
        early.estimate((-kappa * 5.0).exp(), 1.0, 1.0, kappa, 10.0);
        let mut late = Cell {
            n: 100.0,
            ..Cell::default()
        };
        late.estimate(1.0, 1.0, 1.0, kappa, 10.0);
        late.n = 200.0;
        late.estimate((-kappa * 5.0).exp(), 1.0, 1.0, kappa, 10.0);
        let nbar_error = (early.nbar - late.nbar).abs();
        assert!(nbar_error > 0.0);
        assert!(nbar_error <= kappa * 5.0 * 100.0);
        let arrival_error = kappa * nbar_error;
        assert!(arrival_error <= kappa * kappa * 5.0 * 100.0);
    }

    #[test]
    fn warmed_observe_and_repeat_paths_allocate_nothing() {
        let mut world = World::new(2, 1);
        world.snapshot.queued = vec![
            queued(source(1), 0, Some(10)),
            queued(source(1), 1, None),
            queued(None, 0, None),
        ];
        world
            .snapshot
            .workers
            .push(worker(&world.fleets[1].policy, "local-1", 1, true));
        world
            .snapshot
            .executing
            .push(executing(source(1), 0, 0, Some(20)));
        world.observe(0);
        world.complete(source(1), 0, 1, 1, Some(10), test_resources());
        world.snapshot.source_rates_at = tick(1);
        let mut snapshot = world.snapshot.clone();
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        let attribution = [Some(0), Some(1), Some(0)];
        world.rates.observe(
            &world.fleets,
            &hosted,
            &attribution,
            &world.capacities,
            &world.horizons,
            time(1),
        );
        let allocations = allocation_counter::measure(|| {
            for second in 2..12 {
                world.rates.observe(
                    &world.fleets,
                    &hosted,
                    &attribution,
                    &world.capacities,
                    &world.horizons,
                    time(second),
                );
            }
        });
        assert_eq!(allocations.count_total, 0);
    }

    #[test]
    fn skipped_frames_keep_event_baseline_and_new_completions() {
        let mut world = World::new(1, 2);
        world.queue(10, Some(3));
        world.observe(0);
        world.complete(source(1), 0, 1, 2, Some(3), test_resources());
        world.observe(1);
        world.rates.interrupt();
        world.complete(source(1), 0, 40, 5, Some(3), test_resources());
        world.observe_skew(50, 60);
        let expected = 2.0 * world.rates.kappa * decay(world.rates.kappa, tick(50), tick(1))
            + 5.0 * world.rates.kappa * decay(world.rates.kappa, tick(50), tick(40));
        near(world.cell(source(1), 0).moments.all, expected, 1e-14);
        near(
            world.rates.fleet(0).total_load.cpu,
            3.0 * expected * decay(world.rates.kappa, tick(60), tick(50)),
            1e-14,
        );
        exact(world.cell(source(1), 0).nbar, 10.0);
    }

    #[test]
    fn untimed_only_uses_duration_chain_but_timed_zero_does_not() {
        let mut world = World::new(1, 2);
        world.queue(10, Some(30));
        world.observe(0);
        world.complete(source(1), 0, 1, 1, None, test_resources());
        world.observe(1);
        near(
            world.rates.fleet(0).total_load.cpu,
            30.0 * world.rates.kappa,
            1e-13,
        );
        world.complete(source(1), 0, 2, 1, Some(0), test_resources());
        world.observe(2);
        exact(world.rates.fleet(0).total_load.cpu, 0.0);
        assert!(world.cell(source(1), 0).moments.all > 0.0);
        assert!(world.cell(source(1), 0).moments.timed > 0.0);
    }

    #[test]
    fn worker_wall_clock_offset_never_changes_receipt_time_estimator() {
        fn with_offset(offset: u64) -> (f64, f64) {
            let mut world = World::new(1, 2);
            world
                .snapshot
                .workers
                .push(worker(&world.fleets[0].policy, "local-1", 1, false));
            let mut action = executing(source(1), 0, 0, Some(10));
            action.started = Timestamp::default() + Duration::from_secs(offset);
            world.snapshot.executing.push(action);
            world.observe(0);
            world.complete(source(1), 0, 5, 1, Some(10), test_resources());
            world.observe(5);
            (
                world.cell(source(1), 0).moments.all,
                world.rates.fleet(0).total_load.cpu,
            )
        }
        let baseline = with_offset(0);
        let offset = with_offset(86_400);
        exact(baseline.0, offset.0);
        exact(baseline.1, offset.1);
    }

    #[test]
    fn owned_partial_cordon_is_not_serving_even_before_flags_converge() {
        let mut world = World::new(1, 2);
        world
            .snapshot
            .workers
            .push(worker(&world.fleets[0].policy, "local-1", 1, false));
        world
            .snapshot
            .executing
            .push(executing(source(1), 0, 0, Some(10)));
        world.snapshot.cordons.push(gang_spi::InstanceCordon {
            fleet: 0,
            instance: Arc::new(gang_types::InstanceId::try_new("local-1").unwrap()),
        });
        world.observe(0);
        exact(world.rates.fleet(0).total_load.cpu, 1.0);
        exact(world.rates.fleet(0).serving_load.cpu, 0.0);
    }

    #[test]
    fn zero_horizon_classification_is_finite_without_epsilon() {
        let mut world = World::new(1, 2);
        world.horizons[0] = Horizon {
            readiness: Duration::ZERO,
            payback: Duration::ZERO,
        };
        world.queue(16, None);
        world.observe(0);
        assert!(world.cell(source(1), 0).persistent);
        exact(world.rates.fleet(0).persistent_queue.cpu, 16.0);
        exact(world.rates.fleet(0).persistent_load.cpu, 0.0);
        assert!(world.rates.fleet(0).queued_work.cpu.is_finite());
    }

    #[test]
    fn all_subpass_untimed_completion_uses_only_count_duration_prior() {
        let mut world = World::new(1, 2);
        world.observe(0);
        world.complete(
            source(1),
            0,
            5,
            3,
            None,
            ActionResources {
                cpu_millicores: 16_000,
                memory: world.fleets[0].policy.memory_per_instance.get(),
                disk: world.fleets[0].policy.disk_per_instance.get(),
            },
        );
        world.observe(5);
        exact(world.cell(source(1), 0).n, 0.0);
        exact(world.cell(source(1), 0).moments.timed, 0.0);
        assert_eq!(
            world.cell(source(1), 0).moments.work,
            ResourceVector::default()
        );
        near(world.rates.fleet(0).total_load.cpu, 0.0, 1e-14);
        exact(world.rates.fleet(0).total_load.cpu, 0.0);
        exact(world.rates.fleet(0).total_load.memory, 0.0);
        exact(world.rates.fleet(0).total_load.disk, 0.0);
    }

    #[test]
    fn retention_evicts_least_recently_active_source_not_last_queried() {
        let mut world = World::new(1, 2);
        world.snapshot.queued = vec![queued(source(1), 0, Some(10))];
        world.observe(0);
        world.snapshot.queued = vec![queued(source(2), 0, Some(10))];
        world.observe(1);
        world.snapshot.queued = vec![queued(source(1), 0, Some(10))];
        world.observe(2);
        world.snapshot.queued = vec![queued(source(3), 0, Some(10))];
        world.observe(3);
        assert!(world.rates.lookup(source(1)).is_some());
        assert!(world.rates.lookup(source(2)).is_none());
        assert!(world.rates.lookup(source(3)).is_some());
        assert_eq!(world.rates.index.len(), 2);
    }

    #[test]
    fn debug_summary_contains_counts_without_source_ids_or_buffers() {
        let mut world = World::new(2, 2);
        world.queue(160, Some(30));
        world.observe(0);
        let debug = format!("{:?}", world.rates);
        assert!(debug.contains("SourceRates"));
        assert!(!debug.contains(&source(1).unwrap().to_string()));
        assert!(!debug.contains("nbar"));
        assert!(!debug.contains("moments"));
        assert!(debug.len() < 256);
    }
}
