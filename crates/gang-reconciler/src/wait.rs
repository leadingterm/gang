//! Resource-based scaling and projected reservation timelines.
//!
//! `Capacity::instances` is the census's authoritative usable set.
//!
//! Expected wait is an optional, exact reservation simulation, not scaling input.
//! Its work is capped at 128 units per actual queued action, executing action and
//! projected instance (never the configured instance maximum). Units include
//! instance probes, event visits, insertion comparisons and shifted events,
//! including insertion of running reservations. Exhaustion omits every fleet's
//! estimate for the pass; demand, rescue, N-1 and running completion stay exact.
//! Scratch storage is retained and the allowance resets on the next pass.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use gang_config::{FleetPolicy, ScalingConfig};
use gang_spi::{FleetInstance, SchedulerSnapshot};
use gang_types::{ActionResources, FleetSet, property::MILLICORES_PER_CORE};

use crate::decision::{CordonDecision, ScaleDecision, metric};
use crate::fleet::FleetPass;
use crate::hosted_snapshot::HostedSnapshot;
use crate::pass_time::PassTime;
use crate::profile::SourceProfiles;
use crate::rates::{FleetRates, SourceRates};
use crate::vectors::{CapacityVector, Horizon, ResourceVector, ScalingDimension};

#[derive(Debug)]
pub(crate) struct Capacity {
    pub live: u32,
    pub standing: u32,
    pub reliable: u32,
    /// Supply indices of connected owned drains, in canonical rescue order.
    pub resumable: Vec<usize>,
    pub instances: Vec<usize>,
}

impl Capacity {
    pub(crate) fn draining(&self) -> u32 {
        u32::try_from(self.resumable.len()).unwrap_or(u32::MAX)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InstanceView {
    pub cap: CapacityVector,
    pub empty_after: Duration,
    pub serving: bool,
    pub resume_cap: CapacityVector,
}

#[derive(Clone, Copy, Debug, Default)]
struct InstanceTimeline {
    view: InstanceView,
    ready_after: Duration,
    usable: bool,
    dispatch: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Reservation {
    cpu: u128,
    memory: u128,
    disk: u128,
}

impl Reservation {
    fn action(resources: ActionResources) -> Self {
        Self {
            cpu: u128::from(resources.cpu_millicores),
            memory: u128::from(resources.memory.get()),
            disk: u128::from(resources.disk.get()),
        }
    }

    fn capacity(policy: &FleetPolicy, factor: NonZeroU32) -> Self {
        Self {
            cpu: u128::from(
                u64::from(policy.vcpus_per_instance.get())
                    .saturating_mul(MILLICORES_PER_CORE)
                    .saturating_mul(u64::from(factor.get())),
            ),
            memory: u128::from(policy.memory_per_instance.get().get()),
            disk: u128::from(policy.disk_per_instance.get().get()),
        }
    }

    fn add(self, other: Self) -> Self {
        Self {
            cpu: self.cpu.saturating_add(other.cpu),
            memory: self.memory.saturating_add(other.memory),
            disk: self.disk.saturating_add(other.disk),
        }
    }

    fn sub(self, other: Self) -> Self {
        Self {
            cpu: self.cpu.saturating_sub(other.cpu),
            memory: self.memory.saturating_sub(other.memory),
            disk: self.disk.saturating_sub(other.disk),
        }
    }

    fn fits(self, occupied: Self, capacity: Self) -> bool {
        let total = self.add(occupied);
        total.cpu <= capacity.cpu && total.memory <= capacity.memory && total.disk <= capacity.disk
    }
}

#[derive(Clone, Copy, Debug)]
struct Release {
    after: Duration,
    occupied: Reservation,
}

#[derive(Clone, Copy, Debug)]
struct Event {
    at: Duration,
    reserve: bool,
    resources: Reservation,
}

/// A deterministic ceiling on display-only work, not a scheduling policy knob.
const PROJECTION_WORK_PER_INPUT: usize = 128;

#[derive(Debug, Default)]
struct ProjectionWork {
    remaining: usize,
    exhausted: bool,
    #[cfg(test)]
    event_visits: usize,
    #[cfg(test)]
    insertion_comparisons: usize,
    #[cfg(test)]
    shifted_events: usize,
    #[cfg(test)]
    instance_probes: usize,
}

impl ProjectionWork {
    fn new(inputs: usize) -> Self {
        Self {
            remaining: inputs.saturating_mul(PROJECTION_WORK_PER_INPUT),
            ..Self::default()
        }
    }

    fn add_instance(&mut self) {
        // Never revive a partially built timeline after exhaustion.
        self.remaining = self.remaining.saturating_add(PROJECTION_WORK_PER_INPUT);
    }

    fn charge(&mut self, units: usize) -> bool {
        if self.exhausted || units > self.remaining {
            self.exhausted = true;
            return false;
        }
        self.remaining -= units;
        true
    }
}

#[derive(Debug)]
struct DispatchInstance {
    fleet: usize,
    ready_after: Duration,
    enabled: bool,
    capacity: Reservation,
    events: Vec<Event>,
}

impl DispatchInstance {
    fn event(&mut self, event: Event, work: &mut ProjectionWork) {
        let key = (event.at, event.reserve);
        let mut low = 0;
        let mut high = self.events.len();
        while low < high {
            if !work.charge(1) {
                return;
            }
            let middle = low + (high - low) / 2;
            #[cfg(test)]
            {
                work.event_visits += 1;
                work.insertion_comparisons += 1;
            }
            if (self.events[middle].at, self.events[middle].reserve) <= key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let index = low;
        if index > 0 {
            if !work.charge(1) {
                return;
            }
            #[cfg(test)]
            {
                work.event_visits += 1;
            }
            let existing = &mut self.events[index - 1];
            if (existing.at, existing.reserve) == key {
                existing.resources = existing.resources.add(event.resources);
                return;
            }
        }
        let shifted = self.events.len() - index;
        if !work.charge(shifted.saturating_add(1)) {
            return;
        }
        #[cfg(test)]
        {
            work.shifted_events += shifted;
        }
        self.events.insert(index, event);
    }

    fn reserve_bounded(
        &mut self,
        start: Duration,
        end: Duration,
        resources: Reservation,
        work: &mut ProjectionWork,
    ) {
        if !work.charge(1) || resources == Reservation::default() || start == end {
            return;
        }
        self.event(
            Event {
                at: start,
                reserve: true,
                resources,
            },
            work,
        );
        if !work.exhausted {
            self.event(
                Event {
                    at: end,
                    reserve: false,
                    resources,
                },
                work,
            );
        }
    }

    fn event_at(&self, index: usize, work: &mut ProjectionWork) -> Option<&Event> {
        if !work.charge(1) {
            return None;
        }
        let event = self.events.get(index);
        #[cfg(test)]
        if event.is_some() {
            work.event_visits += 1;
        }
        event
    }

    fn earliest_bounded(
        &self,
        demand: Reservation,
        duration: Duration,
        work: &mut ProjectionWork,
    ) -> Option<Duration> {
        if !demand.fits(Reservation::default(), self.capacity) {
            return None;
        }
        if demand == Reservation::default() {
            return Some(self.ready_after);
        }
        let mut occupied = Reservation::default();
        let mut index = 0;
        let mut candidate = None;
        let mut now = self.ready_after;
        loop {
            let next = loop {
                let next = self.event_at(index, work);
                if work.exhausted {
                    return None;
                }
                match next {
                    Some(event) if event.at <= now => {
                        occupied = if event.reserve {
                            occupied.add(event.resources)
                        } else {
                            occupied.sub(event.resources)
                        };
                        index += 1;
                    }
                    _ => break next,
                }
            };
            if demand.fits(occupied, self.capacity) {
                let start = *candidate.get_or_insert(now);
                match next {
                    Some(event) if event.at.saturating_sub(start) < duration => {}
                    _ => return Some(start),
                }
            } else {
                candidate = None;
            }
            now = next?.at;
        }
    }

    #[cfg(test)]
    fn reserve(&mut self, start: Duration, end: Duration, resources: Reservation) {
        self.reserve_bounded(start, end, resources, &mut ProjectionWork::new(10_000));
    }

    #[cfg(test)]
    fn earliest(&self, demand: Reservation, duration: Duration) -> Option<Duration> {
        self.earliest_bounded(demand, duration, &mut ProjectionWork::new(10_000))
    }
}

#[derive(Debug)]
struct Dispatch {
    instances: Vec<DispatchInstance>,
    active: usize,
    waits: Vec<Option<Duration>>,
    work: ProjectionWork,
}

impl Dispatch {
    fn new(fleets: &[FleetPolicy]) -> Self {
        let capacity = fleets.iter().fold(0usize, |total, policy| {
            total
                .saturating_add(usize::try_from(policy.instances.max().get()).unwrap_or(usize::MAX))
        });
        Self {
            instances: Vec::with_capacity(capacity),
            active: 0,
            waits: vec![None; fleets.len()],
            work: ProjectionWork::default(),
        }
    }

    fn clear(&mut self, snapshot: &SchedulerSnapshot) {
        self.active = 0;
        self.waits.fill(None);
        self.work = ProjectionWork::new(
            snapshot
                .queued
                .len()
                .saturating_add(snapshot.executing.len()),
        );
    }

    fn add_instance(
        &mut self,
        fleet: usize,
        ready_after: Duration,
        capacity: Reservation,
    ) -> usize {
        let index = self.active;
        self.work.add_instance();
        self.active += 1;
        if let Some(instance) = self.instances.get_mut(index) {
            instance.fleet = fleet;
            instance.ready_after = ready_after;
            instance.capacity = capacity;
            instance.enabled = true;
            instance.events.clear();
        } else {
            self.instances.push(DispatchInstance {
                fleet,
                ready_after,
                enabled: true,
                capacity,
                events: Vec::new(),
            });
        }
        index
    }

    fn dispatch(
        &mut self,
        snapshot: &SchedulerSnapshot,
        attribution: &[Option<usize>],
        matching: &[FleetSet],
        rates: &SourceRates,
    ) {
        for (index, action) in snapshot.queued.iter().enumerate() {
            if !self.work.charge(1) {
                break;
            }
            let Some(attributed) = attribution.get(index).copied().flatten() else {
                continue;
            };
            let Some(candidates) = matching.get(index) else {
                continue;
            };
            let demand = Reservation::action(action.resources);
            let duration = rates.queued_duration(index);
            let mut best = None;
            for (instance, worker) in self.instances[..self.active].iter().enumerate() {
                if !self.work.charge(1) {
                    break;
                }
                #[cfg(test)]
                {
                    self.work.instance_probes += 1;
                }
                if !worker.enabled || !candidates.contains(worker.fleet) {
                    continue;
                }
                if let Some(at) = worker.earliest_bounded(demand, duration, &mut self.work)
                    && best.as_ref().is_none_or(|(earliest, _)| at < *earliest)
                {
                    best = Some((at, instance));
                }
                if self.work.exhausted {
                    break;
                }
            }
            if self.work.exhausted {
                break;
            }
            if let Some((at, instance)) = best {
                if let Some(wait) = self.waits.get_mut(attributed) {
                    *wait = Some(wait.unwrap_or(Duration::ZERO).max(at));
                }
                self.instances[instance].reserve_bounded(
                    at,
                    at.saturating_add(duration),
                    demand,
                    &mut self.work,
                );
            }
        }
        if self.work.exhausted {
            self.waits.fill(None);
        }
    }
}

/// Float-to-integer quantization deliberately floors and saturates at the public count bound.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn floor_to_u32(value: f64) -> u32 {
    value.clamp(0.0, f64::from(u32::MAX)).floor() as u32
}

fn rounded_backlog(seconds: f64) -> Duration {
    Duration::try_from_secs_f64(seconds.round()).unwrap_or(Duration::from_secs(u64::MAX))
}

fn update_metrics(fleet: &mut FleetPass, rates: &FleetRates, serving_capacity: CapacityVector) {
    fleet.outlook.load = Some(floor_to_u32(
        (rates.persistent_load.max_component() * 10.0).round(),
    ));
    fleet.outlook.backlog = Some(rounded_backlog(rates.transient_work.max_component()));
    fleet.outlook.utilization = rates
        .serving_load
        .utilization(serving_capacity)
        .map(|fraction| {
            u16::try_from(floor_to_u32((fraction * 1000.0).round().clamp(0.0, 1000.0)))
                .unwrap_or(1000)
        });
}

fn pressure(load: CapacityVector, cap: CapacityVector) -> ScalingDimension {
    let ratio = |d| {
        let demand = load.get(d);
        let available = cap.get(d);
        if available > 0.0 {
            (demand / available).min(f64::MAX)
        } else if demand > 0.0 {
            f64::MAX
        } else {
            0.0
        }
    };
    CapacityVector {
        cpu: ratio(ScalingDimension::Cpu),
        memory: ratio(ScalingDimension::Memory),
        disk: ratio(ScalingDimension::Disk),
    }
    .max_dimension()
}

#[derive(Debug)]
pub(crate) struct WorkPlanner {
    scaling: Arc<ScalingConfig>,
    factor: NonZeroU32,
    rates: SourceRates,
    attribution: Vec<Option<usize>>,
    matching: Vec<FleetSet>,
    work: Vec<u32>,
    demand: Vec<CapacityVector>,
    horizons: Vec<Horizon>,
    capacities: Vec<CapacityVector>,
    serving_capacities: Vec<CapacityVector>,
    serving: Vec<u32>,
    booting: Vec<u32>,
    summaries: Vec<ScaleDecision>,
    instances: Vec<Vec<InstanceTimeline>>,
    usable: Vec<bool>,
    worker_heads: Vec<usize>,
    execution_next: Vec<usize>,
    releases: Vec<Release>,
    dispatch: Dispatch,
}

impl WorkPlanner {
    pub(crate) fn new(
        fleets: &[FleetPolicy],
        scaling: Arc<ScalingConfig>,
        profiles: SourceProfiles,
        factor: NonZeroU32,
    ) -> Self {
        let n = fleets.len();
        Self {
            rates: SourceRates::new(fleets, Arc::clone(&scaling), profiles, factor),
            scaling,
            factor,
            attribution: Vec::new(),
            matching: Vec::new(),
            work: vec![0; n],
            demand: vec![CapacityVector::default(); n],
            horizons: vec![
                Horizon {
                    readiness: Duration::ZERO,
                    payback: Duration::ZERO
                };
                n
            ],
            capacities: vec![CapacityVector::default(); n],
            serving_capacities: vec![CapacityVector::default(); n],
            serving: vec![0; n],
            booting: vec![0; n],
            summaries: vec![ScaleDecision::default(); n],
            instances: (0..n).map(|_| Vec::new()).collect(),
            usable: Vec::new(),
            worker_heads: Vec::new(),
            execution_next: Vec::new(),
            releases: Vec::new(),
            dispatch: Dispatch::new(fleets),
        }
    }

    pub(crate) fn begin_pass(&mut self) {
        self.attribution.clear();
        self.matching.clear();
    }

    pub(crate) fn attribute(&mut self, chosen: Option<usize>, matching: FleetSet) {
        self.attribution.push(chosen);
        self.matching.push(matching);
    }

    pub(crate) fn work(&self, index: usize) -> u32 {
        self.work[index]
    }

    pub(crate) fn interrupt(&mut self) {
        self.rates.interrupt();
    }

    fn reset(&mut self, snapshot: &SchedulerSnapshot) {
        self.dispatch.clear(snapshot);
        self.capacities.fill(CapacityVector::default());
        self.serving_capacities.fill(CapacityVector::default());
        self.serving.fill(0);
        self.booting.fill(0);
        self.worker_heads.clear();
        self.worker_heads.resize(snapshot.workers.len(), usize::MAX);
        self.execution_next.clear();
        self.execution_next
            .resize(snapshot.executing.len(), usize::MAX);
        for (index, execution) in snapshot.executing.iter().enumerate() {
            if let Some(head) = self.worker_heads.get_mut(execution.worker) {
                self.execution_next[index] = *head;
                *head = index;
            }
        }
    }

    fn prepare(
        &mut self,
        fleets: &mut [FleetPass],
        hosted: &HostedSnapshot,
        time: PassTime,
        capacities: &[Capacity],
        readiness: &[Option<Duration>],
    ) {
        let snapshot = hosted.snapshot();
        self.reset(snapshot);
        for (index, fleet) in fleets.iter_mut().enumerate() {
            let boot = readiness[index].unwrap_or_else(|| {
                self.scaling
                    .effective_readiness_prior(fleet.policy.boot_grace)
            });
            self.horizons[index] = Horizon::new(
                boot,
                fleet.policy.idle_timeout,
                self.scaling.payback_percent,
            );
            fleet.outlook.readiness = readiness[index];
            fleet.outlook.expected_wait = None;
            self.usable.clear();
            self.usable.resize(fleet.supply.instances.len(), false);
            for &position in &capacities[index].instances {
                if let Some(usable) = self.usable.get_mut(position) {
                    *usable = true;
                }
            }
            self.instances[index].clear();
            for (position, instance) in fleet.supply.instances.iter().enumerate() {
                let mut connected = false;
                let mut active = false;
                for worker_index in hosted.workers_on(&fleet.policy.name, &instance.id) {
                    connected = true;
                    active |= !snapshot.workers[worker_index].draining;
                }
                let usable = self.usable[position];
                let serving = usable && active;
                let cap = if usable && (!connected || active) {
                    CapacityVector::ONE
                } else {
                    CapacityVector::default()
                };
                let resume_cap = if connected {
                    cap
                } else {
                    CapacityVector::default()
                };
                let ready_after = if serving {
                    Duration::ZERO
                } else {
                    (instance.launched_at + boot).saturating_duration_since(time.wall)
                };
                self.instances[index].push(InstanceTimeline {
                    view: InstanceView {
                        cap,
                        serving,
                        resume_cap,
                        ..InstanceView::default()
                    },
                    ready_after,
                    usable,
                    dispatch: None,
                });
                if serving {
                    self.serving[index] = self.serving[index].saturating_add(1);
                    self.serving_capacities[index] += cap;
                } else if usable && !connected {
                    self.booting[index] = self.booting[index].saturating_add(1);
                }
                self.capacities[index] += cap;
            }
            for &position in &capacities[index].resumable {
                self.instances[index][position].view.resume_cap = CapacityVector::ONE;
            }
            if fleet.supply.launch_failure.is_none() {
                let unborn = fleet.supply.desired.saturating_sub(capacities[index].live);
                self.booting[index] = self.booting[index].saturating_add(unborn);
                self.capacities[index] += CapacityVector::ONE.scaled(f64::from(unborn));
            }
        }
    }

    fn project_instance(
        &mut self,
        fleet: &FleetPass,
        hosted: &HostedSnapshot,
        instance: &FleetInstance,
        time: PassTime,
    ) {
        let snapshot = hosted.snapshot();
        self.releases.clear();
        for worker_index in hosted.workers_on(&fleet.policy.name, &instance.id) {
            let worker = &snapshot.workers[worker_index];
            let mut row = self.worker_heads[worker_index];
            let mut found = 0u32;
            let mut known = Reservation::default();
            while let Some(execution) = snapshot.executing.get(row) {
                let elapsed = time.wall.saturating_duration_since(execution.started);
                let remaining = self
                    .rates
                    .executing_duration(row)
                    .saturating_sub(elapsed)
                    .max(elapsed);
                known = known.add(Reservation::action(execution.resources));
                self.releases.push(Release {
                    after: remaining,
                    occupied: Reservation::action(execution.resources),
                });
                found = found.saturating_add(1);
                row = self.execution_next[row];
            }
            if worker.running_actions > found {
                self.releases.push(Release {
                    after: self.scaling.unknown_action_duration.get(),
                    occupied: Reservation::capacity(&fleet.policy, self.factor).sub(known),
                });
            }
        }
    }

    fn build_timeline(&mut self, fleets: &[FleetPass], hosted: &HostedSnapshot, time: PassTime) {
        for (index, fleet) in fleets.iter().enumerate() {
            let boot = self.horizons[index].readiness;
            let mut physical_booting = 0u32;
            for (position, instance) in fleet.supply.instances.iter().enumerate() {
                self.project_instance(fleet, hosted, instance, time);
                let record = &mut self.instances[index][position];
                record.view.empty_after = self
                    .releases
                    .iter()
                    .map(|release| release.after)
                    .max()
                    .unwrap_or(Duration::ZERO);
                let usable = record.usable && record.view.cap.max_component() > 0.0;
                let resumable = !usable && record.view.resume_cap.max_component() > 0.0;
                if usable || resumable {
                    let dispatch_instance = self.dispatch.add_instance(
                        index,
                        if usable {
                            record.ready_after
                        } else {
                            Duration::ZERO
                        },
                        Reservation::capacity(&fleet.policy, self.factor),
                    );
                    record.dispatch = Some(dispatch_instance);
                    if resumable {
                        self.dispatch.instances[dispatch_instance].enabled = false;
                    }
                    if usable && !record.view.serving {
                        physical_booting = physical_booting.saturating_add(1);
                    }
                    for release in &self.releases {
                        self.dispatch.instances[dispatch_instance].reserve_bounded(
                            Duration::ZERO,
                            release.after,
                            release.occupied,
                            &mut self.dispatch.work,
                        );
                        if self.dispatch.work.exhausted {
                            break;
                        }
                    }
                }
            }
            for _ in 0..self.booting[index].saturating_sub(physical_booting) {
                self.dispatch.add_instance(
                    index,
                    boot,
                    Reservation::capacity(&fleet.policy, self.factor),
                );
            }
        }
    }

    pub(crate) fn plan(
        &mut self,
        fleets: &mut [FleetPass],
        hosted: &HostedSnapshot,
        time: PassTime,
        capacities: &[Capacity],
        readiness: &[Option<Duration>],
    ) {
        self.prepare(fleets, hosted, time, capacities, readiness);
        self.rates.observe(
            fleets,
            hosted,
            &self.attribution,
            &self.capacities,
            &self.horizons,
            time,
        );
        self.build_timeline(fleets, hosted, time);
        for (index, fleet) in fleets.iter_mut().enumerate() {
            let demand = ResourceVector::from_action(fleet.outlook.demand.resources())
                .normalized(&fleet.policy, self.factor);
            self.demand[index] = demand;
            self.work[index] = fleet.outlook.demand.instances(&fleet.policy, self.factor);
            fleet.outlook.needed = fleet.outlook.minimum.max(self.work[index]);
            let base = self.serving[index].saturating_add(self.booting[index]);
            let dimension = demand.max_dimension();
            self.summaries[index] = ScaleDecision {
                dimension,
                demand: metric(demand.get(dimension)),
                capacity: metric(self.serving_capacities[index].get(dimension)),
                reliable: self.serving[index],
                booting: self.booting[index],
                needed: fleet.outlook.needed,
                extra_instances: fleet.outlook.needed.saturating_sub(base),
            };
            update_metrics(
                fleet,
                self.rates.fleet(index),
                self.serving_capacities[index],
            );
            let additional = fleet.outlook.needed.saturating_sub(base);
            let planned = if fleet.supply.launch_failure.is_some() {
                additional.min(capacities[index].draining())
            } else {
                additional
            };
            let rescues = usize::try_from(planned)
                .unwrap_or(usize::MAX)
                .min(capacities[index].resumable.len());
            for &position in capacities[index].resumable.iter().take(rescues) {
                if let Some(instance) = self.instances[index][position].dispatch {
                    self.dispatch.instances[instance].enabled = true;
                }
            }
            let resumed_count = u32::try_from(rescues).unwrap_or(u32::MAX);
            for _ in 0..planned.saturating_sub(resumed_count) {
                if fleet.supply.launch_failure.is_none() {
                    self.dispatch.add_instance(
                        index,
                        self.horizons[index].readiness,
                        Reservation::capacity(&fleet.policy, self.factor),
                    );
                }
            }
        }
        self.dispatch.dispatch(
            hosted.snapshot(),
            &self.attribution,
            &self.matching,
            &self.rates,
        );
        for (index, fleet) in fleets.iter_mut().enumerate() {
            fleet.outlook.expected_wait = self.dispatch.waits[index];
        }
    }

    pub(crate) fn instance_view(&self, fleet: usize, supply_index: usize) -> Option<InstanceView> {
        Some(self.instances.get(fleet)?.get(supply_index)?.view)
    }

    pub(crate) fn scale_decision(&self, fleet: usize) -> ScaleDecision {
        self.summaries[fleet]
    }

    fn forecast_demand(&self, fleet: usize) -> CapacityVector {
        let horizon = self.horizons[fleet];
        let rates = self.rates.fleet(fleet);
        if horizon.payback.is_zero() {
            let pending = CapacityVector::ONE.scaled(f64::from(self.booting[fleet]));
            rates
                .total_load
                .component_max(self.demand[fleet].sub_floor(pending))
        } else {
            rates.total_load
                + rates
                    .queued_work
                    .scaled(1.0 / horizon.payback.as_secs_f64())
        }
    }

    fn safety_demand(&self, fleet: usize) -> CapacityVector {
        let forecast = self.forecast_demand(fleet);
        self.demand[fleet].component_max(forecast)
    }

    pub(crate) fn needs_resume(&self, fleet: usize, extra_capacity: CapacityVector) -> bool {
        if self.horizons.get(fleet).is_none() {
            return false;
        }
        // Pending credit belongs to requested usable capacity, never projected purchases.
        // At zero horizon the forecast uses finite concurrent resource demand instead.
        !self.forecast_demand(fleet).within(
            self.serving_capacities[fleet] + extra_capacity,
            f64::from(self.scaling.scale_in_waterline_percent.get()) / 100.0,
        )
    }

    pub(crate) fn removal(&self, fleet: usize, supply_index: usize) -> Option<CordonDecision> {
        let victim = self.instance_view(fleet, supply_index)?;
        if !victim.serving {
            return None;
        }
        let horizon = self.horizons[fleet];
        let rates = self.rates.fleet(fleet);
        let cap = self.serving_capacities[fleet].sub_floor(victim.cap);
        let demand = self.safety_demand(fleet);
        let waterline = self.scaling.scale_in_waterline_percent.get();
        if self.work[fleet] > self.serving[fleet].saturating_sub(1)
            || !demand.within(cap, f64::from(waterline) / 100.0)
        {
            return None;
        }
        let dimension = pressure(demand, cap);
        Some(CordonDecision {
            readiness: horizon.readiness,
            horizon: horizon.payback,
            dimension,
            serving_instances: self.serving[fleet],
            remaining_capacity: metric(cap.get(dimension)),
            total_load: metric(rates.total_load.get(dimension)),
            queued_work: metric(rates.queued_work.get(dimension)),
            waterline_percent: waterline,
            dwell: Duration::ZERO,
            empty_after: victim.empty_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::{TestClock, Timestamp};
    use gang_config::{HistoryWindow, InstanceBounds};
    use gang_spi::{ExecutingDemand, FleetInstance, LaunchFailure, QueuedDemand, WorkerState};
    use gang_types::{
        ActionResources, InstanceId, InstanceLifecycle, NonZeroDuration, WorkerId, nonzero_u32,
    };

    fn policy(name: &str) -> FleetPolicy {
        let mut policy = gang_testkit::fleet_policy(name).expect("valid fleet name");
        policy.instances = InstanceBounds::up_to(nonzero_u32(20));
        policy.boot_grace = Duration::from_mins(10);
        policy.idle_timeout = Duration::from_mins(10);
        policy
    }

    fn queued(candidates: &[usize], expected: Option<Duration>) -> QueuedDemand {
        QueuedDemand {
            source: None,
            candidates: FleetSet::try_from_indices(candidates.iter().copied())
                .expect("valid fleets"),
            resources: ActionResources::default(),
            queued_since: Timestamp::from_unix_millis(1_000_000),
            priority: 0,
            expected,
        }
    }

    fn executing(worker: usize, expected: Option<Duration>) -> ExecutingDemand {
        ExecutingDemand {
            source: None,
            candidates: FleetSet::prefix(1).expect("one fleet"),
            resources: ActionResources::default(),
            worker,
            started: Timestamp::from_unix_millis(1_000_000),
            expected,
        }
    }

    fn worker(policy: &FleetPolicy, instance: &str, running: u32, cap: u32) -> WorkerState {
        WorkerState {
            id: gang_testkit::worker_id(instance).expect("valid worker id"),
            fleet: policy.name.clone(),
            environment: Some(policy.action_environment.clone()),
            running_actions: running,
            max_inflight_actions: Some(nonzero_u32(cap)),
            capacity: None,
            reserved: None,
            cache: None,
            draining: false,
            paused: false,
            last_seen: Timestamp::from_unix_millis(1_000_000),
        }
    }

    struct Scene {
        clock: TestClock,
        fleets: Vec<FleetPass>,
        hosted: HostedSnapshot,
        planner: WorkPlanner,
        profiles: SourceProfiles,
        capacities: Vec<Capacity>,
    }

    impl Scene {
        fn new(policies: Vec<FleetPolicy>, scaling: ScalingConfig) -> Self {
            let profiles = SourceProfiles::new(&scaling, HistoryWindow::from_units(7));
            let planner = WorkPlanner::new(
                &policies,
                Arc::new(scaling),
                profiles.clone(),
                nonzero_u32(2),
            );
            Self {
                clock: TestClock::new(Timestamp::from_unix_millis(1_000_000)),
                capacities: policies
                    .iter()
                    .map(|_| Capacity {
                        live: 0,
                        standing: 0,
                        reliable: 0,
                        resumable: Vec::new(),
                        instances: Vec::new(),
                    })
                    .collect(),
                fleets: policies.into_iter().map(FleetPass::new).collect(),
                hosted: HostedSnapshot::default(),
                profiles,
                planner,
            }
        }

        fn member(&mut self, fleet: usize, id: &str, lifecycle: InstanceLifecycle, usable: bool) {
            let position = self.fleets[fleet].supply.instances.len();
            self.fleets[fleet].supply.instances.push(FleetInstance {
                id: InstanceId::try_new(id).expect("valid instance"),
                lifecycle,
                launched_at: Timestamp::from_unix_millis(1_000_000),
                release_first: false,
            });
            self.fleets[fleet].supply.desired += 1;
            self.capacities[fleet].live += 1;
            if usable {
                self.capacities[fleet].instances.push(position);
                self.capacities[fleet].reliable += 1;
                self.capacities[fleet].standing += 1;
            } else {
                self.capacities[fleet].resumable.push(position);
            }
        }

        fn snapshot(&mut self, mut snapshot: SchedulerSnapshot, attribution: &[Option<usize>]) {
            self.planner.begin_pass();
            for fleet in &mut self.fleets {
                fleet.outlook.demand = crate::Demand::default();
                fleet.outlook.queued = 0;
            }
            for (index, action) in snapshot.queued.iter().enumerate() {
                let chosen = attribution.get(index).copied().flatten();
                self.planner.attribute(chosen, action.candidates);
                if let Some(fleet) = chosen.and_then(|i| self.fleets.get_mut(i)) {
                    fleet.outlook.demand.add_queued(action.resources);
                    fleet.outlook.queued += 1;
                }
            }
            for (worker_index, worker) in snapshot.workers.iter().enumerate() {
                if worker.draining {
                    continue;
                }
                let Some(fleet) = self
                    .fleets
                    .iter_mut()
                    .find(|f| f.policy.name == worker.fleet)
                else {
                    continue;
                };
                let mut found = 0u32;
                for execution in snapshot
                    .executing
                    .iter()
                    .filter(|row| row.worker == worker_index)
                {
                    fleet.outlook.demand.add_queued(execution.resources);
                    found += 1;
                }
                fleet
                    .outlook
                    .demand
                    .add_running(worker.running_actions.saturating_sub(found));
            }
            self.hosted.replace(&mut snapshot);
        }

        fn run(&mut self, readiness: &[Option<Duration>]) {
            self.planner.plan(
                &mut self.fleets,
                &self.hosted,
                PassTime::read(&self.clock),
                &self.capacities,
                readiness,
            );
        }
    }

    fn cpu(p: &FleetPolicy) -> u64 {
        u64::from(p.vcpus_per_instance.get()) * 1000
    }

    #[test]
    fn backlog_scales_to_max_regardless_of_payback() {
        let mut p = policy("workers");
        p.idle_timeout = Duration::from_hours(24);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        let mut workers = Vec::new();
        let mut executing_rows = Vec::new();
        for index in 0..6 {
            let id = format!("worker-{index}");
            scene.member(0, &id, InstanceLifecycle::InService, true);
            workers.push(worker(&p, &id, 2, 16));
            let mut running = executing(index, Some(Duration::from_secs(1)));
            running.resources.cpu_millicores = cpu(&p);
            executing_rows.extend([running.clone(), running]);
        }
        let mut waiting = queued(&[0], Some(Duration::from_secs(1)));
        waiting.resources.cpu_millicores = cpu(&p);
        scene.snapshot(
            SchedulerSnapshot {
                workers,
                executing: executing_rows,
                queued: vec![waiting; 50],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 50],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        let decision = scene.planner.scale_decision(0);
        assert_eq!(scene.planner.work(0), 20);
        assert_eq!(decision.needed, 20);
        assert_eq!(decision.reliable, 6);
        assert_eq!(decision.extra_instances, 14);
        assert_eq!(decision.dimension, ScalingDimension::Cpu);
        assert_eq!(decision.demand.get().to_bits(), 31.0_f64.to_bits());
        assert_eq!(decision.capacity.get().to_bits(), 6.0_f64.to_bits());
    }

    #[test]
    fn cpu_factor_changes_needed_capacity() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        let mut waiting = queued(&[0], None);
        waiting.resources.cpu_millicores = cpu(&p);
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![waiting; 4],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 4],
        );
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 2);
        scene.planner = WorkPlanner::new(
            &[p],
            Arc::clone(&scene.planner.scaling),
            scene.profiles.clone(),
            nonzero_u32(1),
        );
        for _ in 0..4 {
            scene
                .planner
                .attribute(Some(0), FleetSet::prefix(1).expect("one fleet"));
        }
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 4);
    }

    #[test]
    fn cpu_budget_saturates_at_native_ledger_limit() {
        let mut p = policy("workers");
        p.vcpus_per_instance = nonzero_u32(u32::MAX);
        let capacity = Reservation::capacity(&p, nonzero_u32(u32::MAX));
        assert_eq!(capacity.cpu, u128::from(u64::MAX));
        let mut dispatch = Dispatch::new(std::slice::from_ref(&p));
        let index = dispatch.add_instance(0, Duration::ZERO, capacity);
        let over = Reservation {
            cpu: u128::from(u64::MAX) + 1,
            ..Reservation::default()
        };
        assert_eq!(
            dispatch.instances[index].earliest(over, Duration::from_secs(1)),
            None
        );
    }

    #[test]
    fn disk_and_memory_set_resource_scale_bounds() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        let mut waiting = queued(&[0], Some(Duration::from_secs(1)));
        waiting.resources.disk = p.disk_per_instance.get();
        waiting.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![waiting; 3],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 3],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.planner.work(0), 3);
        let decision = scene.planner.scale_decision(0);
        assert_eq!(decision.dimension, ScalingDimension::Memory);
        assert_eq!(decision.demand.get().to_bits(), 3.0_f64.to_bits());
    }

    #[test]
    fn zero_resource_queue_shares_one_ready_worker() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "idle", 0, 16)],
                queued: vec![queued(&[0], Some(Duration::from_secs(100))); 160],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 160],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.planner.work(0), 1);
        assert_eq!(scene.fleets[0].outlook.expected_wait, Some(Duration::ZERO));
    }

    #[test]
    fn matching_event_boundaries_coalesce_and_preserve_order() {
        let capacity = Reservation {
            cpu: 1000,
            memory: 1000,
            ..Reservation::default()
        };
        let mut worker = DispatchInstance {
            fleet: 0,
            ready_after: Duration::ZERO,
            enabled: true,
            capacity,
            events: Vec::new(),
        };
        let first = Reservation {
            cpu: 100,
            memory: 100,
            ..Reservation::default()
        };
        let second = Reservation {
            cpu: 200,
            memory: 200,
            ..Reservation::default()
        };
        let end = Duration::from_secs(10);
        worker.reserve(Duration::ZERO, end, first);
        worker.reserve(Duration::ZERO, end, second);
        assert_eq!(worker.events.len(), 2);
        assert_eq!(worker.events[0].resources, first.add(second));
        assert_eq!(worker.events[1].resources, first.add(second));

        let fits = Reservation {
            cpu: 700,
            ..Reservation::default()
        };
        let exceeds = Reservation {
            cpu: 701,
            ..Reservation::default()
        };
        assert_eq!(worker.earliest(fits, end), Some(Duration::ZERO));
        assert_eq!(worker.earliest(exceeds, end), Some(end));

        let next_end = Duration::from_secs(20);
        worker.reserve(end, next_end, capacity);
        assert_eq!(worker.events.len(), 4);
        assert_eq!(worker.events[1].at, end);
        assert!(!worker.events[1].reserve);
        assert_eq!(worker.events[2].at, end);
        assert!(worker.events[2].reserve);
        assert_eq!(worker.earliest(fits, end), Some(Duration::ZERO));
        assert_eq!(worker.earliest(exceeds, end), Some(next_end));
    }

    #[test]
    fn resource_timeline_releases_complementary_reservations() {
        let p = policy("workers");
        let mut dispatch = Dispatch::new(std::slice::from_ref(&p));
        let capacity = Reservation::capacity(&p, nonzero_u32(2));
        let instance = dispatch.add_instance(0, Duration::ZERO, capacity);
        let cpu = Reservation {
            cpu: capacity.cpu,
            ..Reservation::default()
        };
        let memory = Reservation {
            memory: capacity.memory,
            ..Reservation::default()
        };
        let worker = &mut dispatch.instances[instance];
        worker.reserve(Duration::ZERO, Duration::from_secs(5), cpu);
        worker.reserve(Duration::ZERO, Duration::from_secs(10), memory);
        assert_eq!(
            worker.earliest(cpu, Duration::from_secs(1)),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            worker.earliest(capacity, Duration::from_secs(1)),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            worker.earliest(Reservation::default(), Duration::from_secs(1)),
            Some(Duration::ZERO)
        );
        worker.events.clear();
        worker.reserve(Duration::from_secs(5), Duration::from_secs(10), capacity);
        assert_eq!(
            worker.earliest(capacity, Duration::from_secs(4)),
            Some(Duration::ZERO)
        );
        assert_eq!(
            worker.earliest(capacity, Duration::from_secs(6)),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn serial_cpu_backlog_and_integer_boundaries() {
        let p = policy("workers");
        let mut dispatch = Dispatch::new(std::slice::from_ref(&p));
        let capacity = Reservation::capacity(&p, nonzero_u32(2));
        let instance = dispatch.add_instance(0, Duration::ZERO, capacity);
        let worker = &mut dispatch.instances[instance];
        let full_cpu = Reservation {
            cpu: capacity.cpu,
            ..Reservation::default()
        };
        for wave in 0..100 {
            let start = worker.earliest(full_cpu, Duration::from_secs(1)).unwrap();
            assert_eq!(start, Duration::from_secs(wave));
            worker.reserve(start, start + Duration::from_secs(1), full_cpu);
        }
        let too_large = Reservation {
            cpu: capacity.cpu + 1,
            ..Reservation::default()
        };
        assert_eq!(worker.earliest(too_large, Duration::from_secs(1)), None);
    }

    #[test]
    fn queued_reservations_dispatch_in_order_and_release() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        let mut a = queued(&[0], Some(Duration::from_secs(100)));
        a.resources.memory = p.memory_per_instance.get();
        let mut b = a.clone();
        b.expected = Some(Duration::from_secs(1));
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "idle", 0, 16)],
                queued: vec![a, b],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 2],
        );
        scene.fleets[0].policy.instances = InstanceBounds::up_to(nonzero_u32(1));
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(
            scene.fleets[0].outlook.expected_wait,
            Some(Duration::from_secs(100))
        );
    }

    #[test]
    fn missing_running_metadata_blocks_resources_until_unknown_duration() {
        let p = policy("workers");
        let scaling = ScalingConfig {
            unknown_action_duration: NonZeroDuration::from_secs(10),
            ..ScalingConfig::default()
        };
        let mut scene = Scene::new(vec![p.clone()], scaling);
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        let mut waiting = queued(&[0], None);
        waiting.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 1, 16)],
                queued: vec![waiting],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.run(&[Some(Duration::from_secs(90))]);
        assert_eq!(
            scene.fleets[0].outlook.expected_wait,
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            scene.planner.instance_view(0, 0).unwrap().empty_after,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn missing_metadata_only_reserves_worker_resource_deficit() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        let mut known = executing(0, Some(Duration::from_secs(100)));
        known.resources.cpu_millicores = cpu(&p);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 2, 16)],
                executing: vec![known],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(90))]);
        let worker = &scene.planner.dispatch.instances[0];
        let capacity = Reservation::capacity(&p, nonzero_u32(2));
        let half = Reservation {
            cpu: capacity.cpu / 2,
            ..Reservation::default()
        };
        assert_eq!(
            worker.earliest(half, Duration::from_secs(1)),
            Some(Duration::from_secs(10))
        );
        let occupied = worker
            .events
            .iter()
            .filter(|event| event.reserve)
            .fold(Reservation::default(), |sum, event| {
                sum.add(event.resources)
            });
        assert_eq!(occupied, capacity);
    }

    #[test]
    fn booting_and_launch_failure_control_wait_and_census() {
        let mut scene = Scene::new(vec![policy("workers")], ScalingConfig::default());
        scene.member(0, "booting", InstanceLifecycle::Pending, true);
        scene.clock.advance(Duration::from_secs(30));
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![queued(&[0], None)],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.planner.scale_decision(0).booting, 1);
        assert_eq!(
            scene.fleets[0].outlook.expected_wait,
            Some(Duration::from_secs(15))
        );
        let mut failed = Scene::new(vec![policy("workers")], ScalingConfig::default());
        failed.fleets[0].supply.desired = 6;
        failed.fleets[0].supply.launch_failure = Some(LaunchFailure {
            reason: "unavailable".into(),
        });
        failed.snapshot(
            SchedulerSnapshot {
                queued: vec![queued(&[0], None)],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        failed.run(&[None]);
        assert_eq!(failed.planner.scale_decision(0).booting, 0);
        assert_eq!(failed.fleets[0].outlook.expected_wait, None);
    }

    #[test]
    fn resumed_worker_retains_running_reservations_in_wait_projection() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "draining", InstanceLifecycle::InService, false);
        scene.fleets[0].supply.launch_failure = Some(LaunchFailure {
            reason: "unavailable".into(),
        });
        let mut drain = worker(&p, "draining", 1, 16);
        drain.draining = true;
        let mut running = executing(0, Some(Duration::from_secs(30)));
        running.resources.memory = p.memory_per_instance.get();
        let mut waiting = queued(&[0], None);
        waiting.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![drain],
                executing: vec![running],
                queued: vec![waiting],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.run(&[Some(Duration::from_secs(90))]);
        assert_eq!(
            scene.fleets[0].outlook.expected_wait,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn overlapping_connections_count_one_physical_worker_and_resume_capacity() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "mixed", InstanceLifecycle::InService, true);
        let first = worker(&p, "mixed", 0, 4);
        let mut second = worker(&p, "mixed", 1, 16);
        second.draining = true;
        second.id = WorkerId::try_new(format!(
            "{}6c5e4f3a-9b1d-4e2a-8f7c-000000000002",
            scene.fleets[0].supply.instances[0].id.worker_name(),
        ))
        .expect("renewed worker");
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![first, second],
                executing: vec![executing(1, Some(Duration::from_secs(20)))],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[None]);
        let view = scene.planner.instance_view(0, 0).unwrap();
        assert_eq!(view.cap, CapacityVector::ONE);
        assert_eq!(view.resume_cap, CapacityVector::ONE);
        assert_eq!(view.empty_after, Duration::from_secs(20));
        assert_eq!(scene.planner.scale_decision(0).reliable, 1);
    }

    #[test]
    fn excluded_draining_worker_never_supplies_capacity_but_can_resume() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "draining", InstanceLifecycle::InService, false);
        let mut drain = worker(&p, "draining", 1, 16);
        drain.draining = true;
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![drain],
                executing: vec![executing(0, Some(Duration::from_secs(20)))],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[None]);
        let view = scene.planner.instance_view(0, 0).unwrap();
        assert_eq!(view.cap, CapacityVector::default());
        assert_eq!(view.resume_cap, CapacityVector::ONE);
        assert!(!view.serving);
        assert!(scene.planner.removal(0, 0).is_none());
    }

    #[test]
    fn pending_resource_credit_does_not_trigger_waterline_rescue_or_n_minus_one() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        for name in ["first", "second", "third"] {
            scene.member(0, name, InstanceLifecycle::InService, true);
        }
        scene.member(0, "pending", InstanceLifecycle::Pending, true);
        let mut waiting = queued(&[0], Some(Duration::from_secs(10)));
        waiting.resources.cpu_millicores = cpu(&p);
        let mut snapshot = SchedulerSnapshot {
            workers: vec![
                worker(&p, "first", 0, 16),
                worker(&p, "second", 0, 16),
                worker(&p, "third", 0, 16),
            ],
            queued: vec![waiting; 8],
            ..SchedulerSnapshot::default()
        };
        scene.snapshot(snapshot.clone(), &[Some(0); 8]);
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.fleets[0].outlook.needed, 4);
        assert_eq!(scene.planner.scale_decision(0).booting, 1);
        assert!(!scene.planner.needs_resume(0, CapacityVector::default()));
        assert!(scene.planner.removal(0, 0).is_none());

        // When the credited instance connects, static resource demand still protects all
        // four instances from a cordon followed by an immediate replacement purchase.
        snapshot.workers.push(worker(&p, "pending", 0, 16));
        scene.snapshot(snapshot, &[Some(0); 8]);
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.fleets[0].outlook.needed, 4);
        assert_eq!(scene.planner.scale_decision(0).booting, 0);
        assert!(!scene.planner.needs_resume(0, CapacityVector::default()));
        assert!(scene.planner.removal(0, 0).is_none());
    }

    #[test]
    fn n_minus_one_enforces_resource_demand_and_waterline() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        let mut running = executing(0, Some(Duration::from_secs(100)));
        running.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 1, 16), worker(&p, "idle", 0, 16)],
                executing: vec![running],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert!(scene.planner.removal(0, 1).is_none());
        assert!(!scene.planner.needs_resume(0, CapacityVector::default()));
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 0, 16), worker(&p, "idle", 0, 16)],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        let removal = scene
            .planner
            .removal(0, 1)
            .expect("idle worker can be removed");
        assert_eq!(
            removal.remaining_capacity.get().to_bits(),
            1.0_f64.to_bits()
        );
        assert_eq!(removal.empty_after, Duration::ZERO);
        assert_eq!(removal.dwell, Duration::ZERO);
    }

    #[test]
    fn running_completion_projection_uses_latest_end() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.clock.advance(Duration::from_secs(20));
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 2, 16)],
                executing: vec![
                    executing(0, Some(Duration::from_secs(10))),
                    executing(0, Some(Duration::from_secs(50))),
                ],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(
            scene.planner.instance_view(0, 0).unwrap().empty_after,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn minimum_and_empty_metrics_are_preserved() {
        let mut scene = Scene::new(vec![policy("workers")], ScalingConfig::default());
        scene.fleets[0].outlook.minimum = 2;
        scene.snapshot(SchedulerSnapshot::default(), &[]);
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 0);
        assert_eq!(scene.fleets[0].outlook.needed, 2);
        assert_eq!(scene.fleets[0].outlook.expected_wait, None);
        assert_eq!(scene.fleets[0].outlook.load, Some(0));
        assert_eq!(scene.fleets[0].outlook.backlog, Some(Duration::ZERO));
    }

    #[test]
    fn draining_resource_load_requires_resume() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "serving", InstanceLifecycle::InService, true);
        scene.member(0, "draining", InstanceLifecycle::InService, false);
        let mut drain = worker(&p, "draining", 1, 16);
        drain.draining = true;
        let mut running = executing(1, Some(Duration::from_secs(30)));
        running.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "serving", 0, 16), drain],
                executing: vec![running],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert!(scene.planner.needs_resume(0, CapacityVector::default()));
        let view = scene.planner.instance_view(0, 1).unwrap();
        assert!(
            !scene
                .planner
                .needs_resume(0, view.resume_cap.sub_floor(view.cap))
        );
        assert!(scene.planner.removal(0, 0).is_none());
        assert_eq!(scene.fleets[0].outlook.utilization, Some(0));
    }

    #[test]
    fn projected_wait_uses_source_duration() {
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.fleets[0].policy.instances = InstanceBounds::up_to(nonzero_u32(1));
        let source = gang_types::SourceId::from_identity_str("test:repo").expect("valid identity");
        let mut running = executing(0, None);
        running.source = Some(source);
        running.resources.memory = p.memory_per_instance.get();
        let mut known = queued(&[0], Some(Duration::from_secs(20)));
        known.source = Some(source);
        known.resources.memory = p.memory_per_instance.get();
        let mut unknown = known.clone();
        unknown.expected = None;
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 1, 16)],
                executing: vec![running],
                queued: vec![known, unknown],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 2],
        );
        scene.run(&[Some(Duration::from_secs(90))]);
        assert_eq!(
            scene.planner.instance_view(0, 0).unwrap().empty_after,
            Duration::from_secs(20)
        );
        assert_eq!(
            scene.fleets[0].outlook.expected_wait,
            Some(Duration::from_secs(40))
        );
    }

    #[test]
    fn dispatch_chooses_earliest_compatible_fleet() {
        let a = policy("a");
        let b = policy("b");
        let mut scene = Scene::new(vec![a.clone(), b.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.member(1, "idle", InstanceLifecycle::InService, true);
        let mut running = executing(0, Some(Duration::from_secs(100)));
        running.resources.memory = a.memory_per_instance.get();
        let mut waiting = queued(&[0, 1], Some(Duration::from_secs(10)));
        waiting.resources.memory = b.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&a, "busy", 1, 16), worker(&b, "idle", 0, 16)],
                executing: vec![running],
                queued: vec![waiting],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.fleets[0].policy.instances = InstanceBounds::up_to(nonzero_u32(1));
        scene.run(&[Some(Duration::from_secs(45)), Some(Duration::from_secs(45))]);
        assert_eq!(scene.fleets[0].outlook.expected_wait, Some(Duration::ZERO));
    }

    #[test]
    fn zero_horizon_preserves_resource_scale_and_safety() {
        let mut p = policy("workers");
        p.idle_timeout = Duration::ZERO;
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        let mut waiting = queued(&[0], None);
        waiting.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "idle", 0, 16)],
                queued: vec![waiting; 3],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 3],
        );
        scene.run(&[Some(Duration::ZERO)]);
        assert_eq!(scene.planner.work(0), 3);
        assert_eq!(scene.planner.scale_decision(0).booting, 0);
        assert_eq!(scene.planner.scale_decision(0).extra_instances, 2);
        assert!(scene.planner.removal(0, 0).is_none());
        assert!(scene.planner.needs_resume(0, CapacityVector::ONE));
        let ample_capacity = CapacityVector::ONE.scaled(4.0);
        assert!(!scene.planner.needs_resume(0, ample_capacity));
        assert_eq!(scene.fleets[0].outlook.readiness, Some(Duration::ZERO));
    }

    #[test]
    fn zero_horizon_credits_only_reliable_pending_and_unborn_requests() {
        for kind in ["pending", "unborn", "failed-unborn"] {
            let mut p = policy("workers");
            p.idle_timeout = Duration::ZERO;
            let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
            scene.member(0, "idle", InstanceLifecycle::InService, true);
            if kind == "pending" {
                scene.member(0, "pending", InstanceLifecycle::Pending, true);
            } else {
                scene.fleets[0].supply.desired += 1;
                if kind == "failed-unborn" {
                    scene.fleets[0].supply.launch_failure = Some(LaunchFailure {
                        reason: "unavailable".into(),
                    });
                }
            }
            let mut waiting = queued(&[0], None);
            waiting.resources.memory =
                gang_types::Bytes::new(p.memory_per_instance.get().get() * 4 / 5);
            scene.snapshot(
                SchedulerSnapshot {
                    workers: vec![worker(&p, "idle", 0, 16)],
                    queued: vec![waiting],
                    ..SchedulerSnapshot::default()
                },
                &[Some(0)],
            );
            scene.run(&[Some(Duration::ZERO)]);
            let failed = kind == "failed-unborn";
            assert_eq!(scene.planner.work(0), 1);
            assert_eq!(scene.planner.scale_decision(0).booting, u32::from(!failed));
            assert_eq!(
                scene.planner.needs_resume(0, CapacityVector::default()),
                failed,
                "{kind}",
            );
            assert!(scene.planner.removal(0, 0).is_none());
        }
    }

    fn large_resource_queue_omits_only_wait(dimension: ScalingDimension) {
        const QUEUED: usize = 100_000;
        const MEMBERS: usize = 4;
        let p = policy("workers");
        let prefix = policy("prefix");
        let mut scene = Scene::new(vec![p.clone(), prefix.clone()], ScalingConfig::default());
        let mut workers = Vec::new();
        for name in ["first", "second", "third", "fourth"] {
            scene.member(0, name, InstanceLifecycle::InService, true);
            workers.push(worker(&p, name, 0, 16));
        }
        scene.member(1, "prefix", InstanceLifecycle::InService, true);
        workers.push(worker(&prefix, "prefix", 0, 16));
        // Failed launches leave several compatible workers and a long serial
        // backlog, rather than masking event scans with new empty instances.
        scene.fleets[0].supply.launch_failure = Some(LaunchFailure {
            reason: "unavailable".into(),
        });
        let mut waiting: Vec<_> = (0..QUEUED)
            .map(|index| {
                let mut action = queued(
                    &[0],
                    Some(Duration::from_secs(1 + u64::try_from(index % 17).unwrap())),
                );
                match dimension {
                    ScalingDimension::Memory => {
                        action.resources.memory = p.memory_per_instance.get();
                    }
                    ScalingDimension::Disk => action.resources.disk = p.disk_per_instance.get(),
                    ScalingDimension::Cpu => panic!("test requires a physical resource"),
                }
                action
            })
            .collect();
        waiting.insert(0, queued(&[1], Some(Duration::from_secs(1))));
        let attribution: Vec<_> = std::iter::once(Some(1))
            .chain(std::iter::repeat_n(Some(0), QUEUED))
            .collect();
        scene.snapshot(
            SchedulerSnapshot {
                workers: workers.clone(),
                queued: waiting,
                ..SchedulerSnapshot::default()
            },
            &attribution,
        );
        scene.run(&[Some(Duration::from_secs(45)); 2]);
        let dispatch = &scene.planner.dispatch;
        let measured = &dispatch.work;
        let bound = PROJECTION_WORK_PER_INPUT * (QUEUED + 1 + MEMBERS + 1);
        assert!(measured.exhausted);
        // These counters observe actual event reads, comparisons and Vec shifts
        // at their sites, independently of the allowance's remaining value.
        assert!(measured.event_visits > QUEUED);
        assert!(
            measured.event_visits + measured.shifted_events + measured.instance_probes <= bound
        );
        assert!(
            dispatch.instances[..MEMBERS]
                .iter()
                .all(|instance| !instance.events.is_empty())
        );
        assert!(dispatch.waits.iter().all(Option::is_none));
        assert!(
            scene
                .fleets
                .iter()
                .all(|fleet| fleet.outlook.expected_wait.is_none())
        );
        assert_eq!(scene.fleets[0].outlook.needed, 20);
        assert_eq!(scene.planner.work(0), 20);
        let decision = scene.planner.scale_decision(0);
        assert_eq!(decision.dimension, dimension);
        assert_eq!(decision.demand.get().to_bits(), 100_000.0_f64.to_bits());
        assert_eq!(decision.capacity.get().to_bits(), 4.0_f64.to_bits());
        assert_eq!(decision.extra_instances, 16);
        assert!(scene.planner.removal(0, 0).is_none());

        let mut action = queued(&[0], Some(Duration::from_secs(3)));
        action.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers,
                queued: vec![action],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.run(&[Some(Duration::from_secs(45)); 2]);
        assert!(!scene.planner.dispatch.work.exhausted);
        assert!(scene.planner.dispatch.work.event_visits < PROJECTION_WORK_PER_INPUT);
        assert_eq!(scene.fleets[0].outlook.expected_wait, Some(Duration::ZERO));
        assert_eq!(scene.fleets[0].outlook.needed, 1);
    }

    #[test]
    fn large_full_memory_queue_has_linear_display_work() {
        large_resource_queue_omits_only_wait(ScalingDimension::Memory);
    }

    #[test]
    fn large_full_disk_queue_has_linear_display_work() {
        large_resource_queue_omits_only_wait(ScalingDimension::Disk);
    }

    #[test]
    fn running_backfill_shifts_are_bounded_without_losing_completion() {
        const EXECUTING: usize = 4096;
        let p = policy("workers");
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        for name in ["busy", "later", "last"] {
            scene.member(0, name, InstanceLifecycle::InService, true);
        }
        scene.fleets[0].supply.launch_failure = Some(LaunchFailure {
            reason: "unavailable".into(),
        });
        // Execution links are traversed in reverse. Distinct descending ends
        // repeatedly backfill the release vector, requiring real Vec shifts.
        let mut running: Vec<_> = (0..EXECUTING)
            .map(|index| {
                let mut action = executing(
                    0,
                    Some(Duration::from_secs(u64::try_from(index + 1).unwrap())),
                );
                action.resources.cpu_millicores = 1 + u64::try_from(index % 3).unwrap();
                action.resources.memory =
                    gang_types::Bytes::new(1 + u64::try_from(index % 5).unwrap());
                action.resources.disk =
                    gang_types::Bytes::new(1 + u64::try_from(index % 7).unwrap());
                action
            })
            .collect();
        running.push(executing(2, Some(Duration::from_secs(99_999))));
        let waiting: Vec<_> = (0..8)
            .map(|index| {
                let mut action = queued(&[0], Some(Duration::from_secs(index + 1)));
                action.resources.memory = p.memory_per_instance.get();
                action
            })
            .collect();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![
                    worker(&p, "busy", u32::try_from(EXECUTING).unwrap(), 16),
                    worker(&p, "later", 0, 16),
                    worker(&p, "last", 1, 16),
                ],
                executing: running,
                queued: waiting,
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 8],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        let work = &scene.planner.dispatch.work;
        assert!(work.exhausted);
        assert!(work.shifted_events > EXECUTING);
        assert!(work.insertion_comparisons > 0);
        assert!(
            work.event_visits + work.shifted_events + work.instance_probes
                <= PROJECTION_WORK_PER_INPUT * (EXECUTING + 1 + 8 + 3)
        );
        assert!(scene.planner.dispatch.waits.iter().all(Option::is_none));
        assert_eq!(scene.fleets[0].outlook.expected_wait, None);
        assert_eq!(
            scene.planner.instance_view(0, 0).unwrap().empty_after,
            Duration::from_secs(u64::try_from(EXECUTING).unwrap())
        );
        assert_eq!(
            scene.planner.instance_view(0, 2).unwrap().empty_after,
            Duration::from_secs(99_999)
        );
        assert_eq!(
            scene.planner.scale_decision(0).needed,
            scene.planner.work(0)
        );
        assert!(scene.planner.removal(0, 2).is_none());
    }

    #[test]
    fn metric_quantization_is_bounded() {
        assert_eq!(floor_to_u32(15.99), 15);
        assert_eq!(floor_to_u32(f64::MAX), u32::MAX);
        assert_eq!(floor_to_u32(-1.0), 0);
        assert_eq!(rounded_backlog(6.49), Duration::from_secs(6));
        assert_eq!(rounded_backlog(6.5), Duration::from_secs(7));
        assert_eq!(rounded_backlog(f64::MAX), Duration::from_secs(u64::MAX));
    }

    #[test]
    fn warmed_planner_reuses_projection_buffers() {
        let mut p = policy("workers");
        p.vcpus_per_instance = nonzero_u32(16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        let mut running = executing(0, Some(Duration::from_secs(100)));
        running.resources.cpu_millicores = MILLICORES_PER_CORE;
        // Keep work beyond readiness so both instance buffers contain reservations.
        let mut waiting = queued(&[0], Some(Duration::from_secs(100)));
        waiting.resources.cpu_millicores = MILLICORES_PER_CORE;
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 4, 16)],
                executing: vec![running; 4],
                queued: vec![waiting; 48],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 48],
        );
        for _ in 0..3 {
            scene.run(&[Some(Duration::from_secs(45))]);
        }
        assert_eq!(scene.planner.dispatch.active, 2);
        assert!(
            scene.planner.dispatch.instances[..scene.planner.dispatch.active]
                .iter()
                .all(|instance| !instance.events.is_empty())
        );
        let allocation = allocation_counter::measure(|| {
            scene.run(&[Some(Duration::from_secs(45))]);
        });
        assert_eq!(allocation.count_total, 0);
    }
}
