//! Rate-based purchases use physical resource timelines; the slot heap is display-only.
//!
//! `Capacity::instances` is the census's authoritative usable set. Other supply members retain
//! diagnostic views but contribute no serving capacity. `InstanceView::resume_cap` is the total
//! physical capacity after clearing drain flags, not the incremental capacity of a resume.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

use gang_config::{FleetPolicy, ScalingConfig};
use gang_spi::{FleetInstance, SchedulerSnapshot};
use gang_types::FleetSet;

use crate::decision::{CordonDecision, ScaleDecision, metric};
use crate::fleet::FleetPass;
use crate::hosted_snapshot::HostedSnapshot;
use crate::pass_time::PassTime;
use crate::profile::SourceProfiles;
use crate::rates::{FleetRates, SourceRates};
use crate::vectors::{Horizon, ResourceVector, ScalingDimension, SlotVector};

#[derive(Debug)]
pub(crate) struct Capacity {
    pub live: u32,
    pub standing: u32,
    pub reliable: u32,
    pub draining: u32,
    pub instances: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InstanceView {
    pub cap: SlotVector,
    pub pre_ready: SlotVector,
    pub empty_after: Duration,
    pub serving: bool,
    pub resume_cap: SlotVector,
}

#[derive(Clone, Copy, Debug, Default)]
struct InstanceTimeline {
    view: InstanceView,
    ready_after: Duration,
    usable: bool,
}

#[derive(Clone, Copy, Debug)]
struct Release {
    after: Duration,
    occupied: SlotVector,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Slot {
    free: u64,
    uses: u64,
    index: u32,
    instance: u32,
}

#[derive(Debug)]
struct Dispatch {
    heaps: Vec<BinaryHeap<Reverse<Slot>>>,
    waits: Vec<Option<u64>>,
}

impl Dispatch {
    fn new(fleets: usize) -> Self {
        Self {
            heaps: (0..fleets).map(|_| BinaryHeap::new()).collect(),
            waits: vec![None; fleets],
        }
    }

    fn clear(&mut self) {
        for heap in &mut self.heaps {
            heap.clear();
        }
        self.waits.fill(None);
    }

    fn add_instance(&mut self, fleet: usize, ordinal: u32, slots: u32, free: u64) {
        for index in 0..slots {
            self.heaps[fleet].push(Reverse(Slot {
                free,
                uses: 0,
                index,
                instance: ordinal,
            }));
        }
    }

    fn best(&self, candidates: FleetSet) -> Option<(Slot, usize)> {
        candidates
            .iter()
            .filter_map(|fleet| self.heaps.get(fleet)?.peek().map(|slot| (slot.0, fleet)))
            .min()
    }

    fn dispatch(
        &mut self,
        snapshot: &SchedulerSnapshot,
        attribution: &[Option<usize>],
        matching: &[FleetSet],
        rates: &SourceRates,
    ) {
        for (index, _) in snapshot.queued.iter().enumerate() {
            let Some(fleet) = attribution.get(index).copied().flatten() else {
                continue;
            };
            if fleet >= self.waits.len() {
                continue;
            }
            let Some((mut slot, chosen)) = self.best(matching[index]) else {
                continue;
            };
            self.heaps[chosen].pop();
            let begin = slot.free;
            slot.free = begin.saturating_add(millis(rates.queued_duration(index)));
            slot.uses = slot.uses.saturating_add(1);
            self.waits[fleet] = Some(self.waits[fleet].unwrap_or(0).max(begin));
            self.heaps[chosen].push(Reverse(slot));
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Float-to-integer quantization deliberately floors and saturates at the public count bound.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn floor_to_u32(value: f64) -> u32 {
    value.clamp(0.0, f64::from(u32::MAX)).floor() as u32
}

fn rounded_backlog(seconds: f64) -> Duration {
    Duration::try_from_secs_f64(seconds.round()).unwrap_or(Duration::from_secs(u64::MAX))
}

fn update_metrics(fleet: &mut FleetPass, rates: &FleetRates, serving_capacity: SlotVector) {
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

fn nominal(slots: u32) -> SlotVector {
    let slots = f64::from(slots);
    SlotVector {
        actions: slots,
        cpu: slots,
        memory: slots,
        disk: slots,
    }
}

/// Keeps the longest count reservations from the already sorted resource releases.
fn project_running(remaining: &mut Vec<u64>, releases: &[Release], cap: usize) {
    remaining.clear();
    for release in releases.iter().rev() {
        let count = usize::try_from(floor_to_u32(release.occupied.actions))
            .unwrap_or(usize::MAX)
            .min(cap.saturating_sub(remaining.len()));
        remaining.resize(remaining.len().saturating_add(count), millis(release.after));
        if remaining.len() == cap {
            break;
        }
    }
    remaining.reverse();
}

/// Exact piecewise-constant integral, conditional on observed reservations and projected ends.
fn free_integral(
    cap: SlotVector,
    ready_after: Duration,
    releases: &[Release],
    horizon: Duration,
    slots: u32,
) -> SlotVector {
    if ready_after >= horizon {
        return SlotVector::default();
    }
    let mut occupied = SlotVector::default();
    for release in releases {
        occupied += release.occupied;
    }
    let mut integral = SlotVector::default();
    let mut previous = Duration::ZERO;
    for release in releases {
        let end = release.after.min(horizon);
        let start = previous.max(ready_after);
        if end > start {
            integral += cap
                .sub_floor(occupied)
                .slot_gate(slots)
                .scaled(end.saturating_sub(start).as_secs_f64());
        }
        if release.after >= horizon {
            return integral;
        }
        occupied = occupied.sub_floor(release.occupied);
        previous = release.after;
    }
    let start = previous.max(ready_after);
    if horizon > start {
        integral += cap
            .sub_floor(occupied)
            .slot_gate(slots)
            .scaled(horizon.saturating_sub(start).as_secs_f64());
    }
    integral
}

#[derive(Clone, Copy, Debug, Default)]
struct Purchase {
    transient: SlotVector,
    extra: SlotVector,
    instances: u32,
}

fn purchase(
    rates: &FleetRates,
    cap: SlotVector,
    pre_ready: SlotVector,
    booting: u32,
    horizon: Horizon,
    slots: u32,
    headroom: u32,
) -> Purchase {
    if horizon.payback.is_zero() {
        return Purchase::default();
    }
    let pre_ready_credit = pre_ready.sub_floor(
        rates
            .persistent_load
            .scaled(horizon.readiness.as_secs_f64()),
    );
    let inverse_payback = 1.0 / horizon.payback.as_secs_f64();
    let transient = rates
        .transient_work
        .sub_floor(pre_ready_credit)
        .scaled(inverse_payback)
        .sub_floor(cap.sub_floor(rates.persistent_load).slot_gate(slots));
    // Completion waves may exceed physical service without any net new input.
    let future = rates.arrival_load.component_min(
        rates.completion_load.component_min(cap)
            + rates.arrival_load.sub_floor(rates.completion_load),
    );
    let excess = future.sub_floor(cap);
    let projected =
        (rates.queued_work + future.scaled(horizon.readiness.as_secs_f64())).sub_floor(pre_ready);
    let booting_capacity = nominal(slots).scaled(f64::from(booting));
    // In-flight instances net AFTER q_P and forecast excess, preventing ratchets.
    let candidate = (transient + rates.persistent_queue + excess).sub_floor(booting_capacity);
    let payable = (projected.scaled(inverse_payback) + excess)
        .sub_floor(cap.sub_floor(future).slot_gate(slots))
        .sub_floor(booting_capacity);
    let extra = candidate.component_min(payable);
    let instances = floor_to_u32(extra.max_component() / f64::from(slots));
    Purchase {
        transient,
        extra,
        instances: instances.min(headroom),
    }
}

fn pressure(load: SlotVector, cap: SlotVector) -> ScalingDimension {
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
    SlotVector {
        actions: ratio(ScalingDimension::Actions),
        cpu: ratio(ScalingDimension::Cpu),
        memory: ratio(ScalingDimension::Memory),
        disk: ratio(ScalingDimension::Disk),
    }
    .max_dimension()
}

#[derive(Debug)]
pub(crate) struct WorkPlanner {
    scaling: Arc<ScalingConfig>,
    rates: SourceRates,
    attribution: Vec<Option<usize>>,
    matching: Vec<gang_types::FleetSet>,
    work: Vec<u32>,
    slots: Vec<u32>,
    horizons: Vec<Horizon>,
    capacities: Vec<SlotVector>,
    serving_capacities: Vec<SlotVector>,
    pre_ready: Vec<SlotVector>,
    serving_pre_ready: Vec<SlotVector>,
    serving: Vec<u32>,
    booting: Vec<u32>,
    queued: Vec<bool>,
    reachable: Vec<bool>,
    summaries: Vec<ScaleDecision>,
    instances: Vec<Vec<InstanceTimeline>>,
    usable: Vec<bool>,
    worker_heads: Vec<usize>,
    execution_next: Vec<usize>,
    releases: Vec<Release>,
    remaining: Vec<u64>,
    ordinals: Vec<u32>,
    dispatch: Dispatch,
}

impl WorkPlanner {
    pub(crate) fn new(
        fleets: &[FleetPolicy],
        scaling: Arc<ScalingConfig>,
        profiles: SourceProfiles,
    ) -> Self {
        let n = fleets.len();
        Self {
            rates: SourceRates::new(fleets, Arc::clone(&scaling), profiles),
            scaling,
            attribution: Vec::new(),
            matching: Vec::new(),
            work: vec![0; n],
            slots: vec![0; n],
            horizons: vec![
                Horizon {
                    readiness: Duration::ZERO,
                    payback: Duration::ZERO
                };
                n
            ],
            capacities: vec![SlotVector::default(); n],
            serving_capacities: vec![SlotVector::default(); n],
            pre_ready: vec![SlotVector::default(); n],
            serving_pre_ready: vec![SlotVector::default(); n],
            serving: vec![0; n],
            booting: vec![0; n],
            queued: vec![false; n],
            reachable: vec![false; n],
            summaries: vec![ScaleDecision::default(); n],
            instances: (0..n).map(|_| Vec::new()).collect(),
            usable: Vec::new(),
            worker_heads: Vec::new(),
            execution_next: Vec::new(),
            releases: Vec::new(),
            remaining: Vec::new(),
            ordinals: vec![0; n],
            dispatch: Dispatch::new(n),
        }
    }

    pub(crate) fn begin_pass(&mut self) {
        self.attribution.clear();
        self.matching.clear();
    }
    pub(crate) fn attribute(&mut self, chosen: Option<usize>, matching: gang_types::FleetSet) {
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
        self.dispatch.clear();
        self.capacities.fill(SlotVector::default());
        self.serving_capacities.fill(SlotVector::default());
        self.pre_ready.fill(SlotVector::default());
        self.serving_pre_ready.fill(SlotVector::default());
        self.serving.fill(0);
        self.booting.fill(0);
        self.queued.fill(false);
        self.reachable.fill(false);
        self.ordinals.fill(0);
        for &attribution in &self.attribution {
            if let Some(fleet) = attribution.and_then(|i| self.queued.get_mut(i)) {
                *fleet = true;
            }
        }
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
            self.slots[index] = fleet.outlook.demand.slots_per_instance(&fleet.policy);
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
            let slots = fleet.policy.target_actions_per_instance.get();
            for (position, instance) in fleet.supply.instances.iter().enumerate() {
                let mut connected = false;
                let mut serving_limit = 0u32;
                let mut resume_limit = 0u32;
                for worker_index in hosted.workers_on(&fleet.policy.name, &instance.id) {
                    let worker = &snapshot.workers[worker_index];
                    if worker.environment.as_ref() != Some(&fleet.policy.action_environment) {
                        continue;
                    }

                    connected = true;
                    let limit = worker
                        .max_inflight_actions
                        .map_or(slots, std::num::NonZeroU32::get);
                    resume_limit = resume_limit.saturating_add(limit);
                    if !worker.draining {
                        serving_limit = serving_limit.saturating_add(limit);
                    }
                }
                let usable = self.usable[position];
                let serving = usable && connected && serving_limit > 0;
                let mut cap = SlotVector::default();
                if usable {
                    cap = nominal(slots);
                    if connected {
                        cap.actions = f64::from(serving_limit.min(slots));
                        if serving_limit == 0 {
                            cap = SlotVector::default();
                        }
                    }
                }
                let resume_cap = if connected {
                    let mut resumed = nominal(slots);
                    resumed.actions = f64::from(resume_limit.min(slots));
                    resumed
                } else {
                    SlotVector::default()
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
                });
                if serving {
                    self.serving[index] = self.serving[index].saturating_add(1);
                    self.serving_capacities[index] += cap;
                } else if usable && !connected {
                    self.booting[index] = self.booting[index].saturating_add(1);
                }
                self.capacities[index] += cap;
            }
            if fleet.supply.launch_failure.is_none() {
                let unborn = fleet.supply.desired.saturating_sub(capacities[index].live);
                self.booting[index] = self.booting[index].saturating_add(unborn);
                self.capacities[index] += nominal(slots).scaled(f64::from(unborn));
            }
            self.reachable[index] = self.capacities[index].actions > 0.0;
        }
    }

    fn project_instance(
        &mut self,
        fleet: &FleetPass,
        hosted: &HostedSnapshot,
        instance: &FleetInstance,
        time: PassTime,
        display_slots: u32,
    ) -> (bool, u32) {
        let snapshot = hosted.snapshot();
        self.releases.clear();
        let mut display_limit = 0u32;
        let mut connected = false;
        for worker_index in hosted.workers_on(&fleet.policy.name, &instance.id) {
            let worker = &snapshot.workers[worker_index];
            connected = true;
            if !worker.draining
                && worker.environment.as_ref() == Some(&fleet.policy.action_environment)
            {
                display_limit = display_limit.saturating_add(
                    worker
                        .max_inflight_actions
                        .map_or(display_slots, std::num::NonZeroU32::get),
                );
            }
            let mut row = self.worker_heads[worker_index];
            let mut found = 0u32;
            while let Some(execution) = snapshot.executing.get(row) {
                let elapsed = time.wall.saturating_duration_since(execution.started);
                let remaining = self
                    .rates
                    .executing_duration(row)
                    .saturating_sub(elapsed)
                    .max(elapsed);
                self.releases.push(Release {
                    after: remaining,
                    occupied: ResourceVector::from_action(execution.resources)
                        .normalized(&fleet.policy),
                });
                found = found.saturating_add(1);
                row = self.execution_next[row];
            }
            let missing = worker.running_actions.saturating_sub(found);
            if missing > 0 {
                self.releases.push(Release {
                    after: self.scaling.unknown_action_duration.get(),
                    occupied: SlotVector {
                        actions: f64::from(missing),
                        ..SlotVector::default()
                    },
                });
            }
        }
        self.releases.sort_unstable_by_key(|release| release.after);
        (connected, display_limit)
    }

    fn build_timeline(&mut self, fleets: &[FleetPass], hosted: &HostedSnapshot, time: PassTime) {
        for (index, fleet) in fleets.iter().enumerate() {
            let nominal_slots = fleet.policy.target_actions_per_instance.get();
            let display_slots = self.slots[index];
            let boot = self.horizons[index].readiness;
            for (position, instance) in fleet.supply.instances.iter().enumerate() {
                let (connected, display_limit) =
                    self.project_instance(fleet, hosted, instance, time, display_slots);
                let record = &mut self.instances[index][position];
                record.view.empty_after = self
                    .releases
                    .last()
                    .map_or(Duration::ZERO, |release| release.after);
                if record.usable {
                    record.view.pre_ready = free_integral(
                        record.view.cap,
                        record.ready_after,
                        &self.releases,
                        boot,
                        nominal_slots,
                    );
                    self.pre_ready[index] += record.view.pre_ready;
                    if record.view.serving {
                        self.serving_pre_ready[index] += record.view.pre_ready;
                    }
                    let ordinal = self.ordinals[index];
                    self.ordinals[index] = ordinal.saturating_add(1);
                    if connected {
                        let cap = display_limit.min(display_slots) as usize;
                        project_running(&mut self.remaining, &self.releases, cap);
                        for slot in 0..cap {
                            self.dispatch.heaps[index].push(Reverse(Slot {
                                free: self.remaining.get(slot).copied().unwrap_or(0),
                                uses: u64::from(slot < self.remaining.len()),
                                index: u32::try_from(slot).unwrap_or(u32::MAX),
                                instance: ordinal,
                            }));
                        }
                    } else {
                        self.dispatch.add_instance(
                            index,
                            ordinal,
                            display_slots,
                            millis(record.ready_after),
                        );
                    }
                }
            }
            // Unborn instances arrive exactly at B, so their integral through B is zero.
            let connected_booting = u32::try_from(
                self.instances[index]
                    .iter()
                    .filter(|record| {
                        record.usable && !record.view.serving && record.view.cap.actions > 0.0
                    })
                    .count(),
            )
            .unwrap_or(u32::MAX);
            for _ in 0..self.booting[index].saturating_sub(connected_booting) {
                self.dispatch.add_instance(
                    index,
                    self.ordinals[index],
                    display_slots,
                    millis(boot),
                );
                self.ordinals[index] = self.ordinals[index].saturating_add(1);
            }
        }
    }

    fn reachability_floor(&self, fleet: usize, remaining: Option<SlotVector>) -> u32 {
        u32::from(
            self.matching
                .iter()
                .enumerate()
                .any(|(index, &candidates)| {
                    self.attribution.get(index) == Some(&Some(fleet))
                        && !candidates.iter().any(|candidate| {
                            if candidate == fleet {
                                remaining.map_or(self.reachable[fleet], |cap| cap.actions > 0.0)
                            } else {
                                self.reachable.get(candidate).copied().unwrap_or(false)
                            }
                        })
                }),
        )
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
            let bound = fleet.outlook.demand.instances(&fleet.policy);
            let serving = self.serving[index];
            let booting = if self.queued[index] {
                self.booting[index]
            } else {
                0
            };
            let base = serving.saturating_add(booting);
            let floor = self.reachability_floor(index, None);
            let rates = self.rates.fleet(index);
            let horizon = self.horizons[index];
            let slots = fleet.policy.target_actions_per_instance.get();
            let purchase = if self.queued[index] {
                purchase(
                    rates,
                    self.serving_capacities[index],
                    self.pre_ready[index],
                    self.booting[index],
                    horizon,
                    slots,
                    bound.saturating_sub(base),
                )
            } else {
                Purchase::default()
            };
            self.work[index] = if self.queued[index] && horizon.payback.is_zero() {
                bound
            } else {
                bound.min(base.saturating_add(purchase.instances.max(floor)))
            };
            fleet.outlook.needed = fleet.outlook.minimum.max(self.work[index]);
            let dimension = purchase.extra.max_dimension();
            self.summaries[index] = ScaleDecision {
                readiness: horizon.readiness,
                horizon: horizon.payback,
                slots,
                reliable: serving,
                dimension,
                capacity: metric(self.serving_capacities[index].get(dimension)),
                pre_ready: metric(f64::from(self.booting[index]) * f64::from(slots)),
                persistent_load: metric(rates.persistent_load.get(dimension)),
                transient_backlog: metric(rates.transient_work.get(dimension)),
                transient_extra: metric(purchase.transient.get(dimension)),
                persistent_queue: metric(rates.persistent_queue.get(dimension)),
                extra_instances: self.work[index].saturating_sub(base),
                reachability: floor,
                persistent_streams: rates.persistent_streams,
                streams: rates.streams,
                zero_overhead: horizon.payback.is_zero(),
            };
            update_metrics(fleet, rates, self.serving_capacities[index]);
            let additional = fleet.outlook.needed.saturating_sub(base);
            let planned = if fleet.supply.launch_failure.is_some() {
                additional.min(capacities[index].draining)
            } else {
                additional
            };
            for _ in 0..planned {
                self.dispatch.add_instance(
                    index,
                    self.ordinals[index],
                    self.slots[index],
                    millis(horizon.readiness),
                );
                self.ordinals[index] = self.ordinals[index].saturating_add(1);
            }
            self.reachable[index] |= planned > 0;
        }
        self.dispatch.dispatch(
            hosted.snapshot(),
            &self.attribution,
            &self.matching,
            &self.rates,
        );
        for (index, fleet) in fleets.iter_mut().enumerate() {
            fleet.outlook.expected_wait = self.dispatch.waits[index].map(Duration::from_millis);
        }
    }

    pub(crate) fn instance_view(&self, fleet: usize, supply_index: usize) -> Option<InstanceView> {
        Some(self.instances.get(fleet)?.get(supply_index)?.view)
    }

    pub(crate) fn scale_decision(&self, fleet: usize) -> ScaleDecision {
        self.summaries[fleet]
    }

    pub(crate) fn needs_resume(&self, fleet: usize, extra_capacity: SlotVector) -> bool {
        let Some(&horizon) = self.horizons.get(fleet) else {
            return false;
        };
        if self.queued[fleet] && horizon.payback.is_zero() {
            return true;
        }
        let rates = self.rates.fleet(fleet);
        let demand = if horizon.payback.is_zero() {
            rates.total_load
        } else {
            rates.total_load
                + rates
                    .queued_work
                    .scaled(1.0 / horizon.payback.as_secs_f64())
        };
        !demand.within(
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
        if self.queued[fleet] && horizon.payback.is_zero() {
            return None;
        }
        let rates = self.rates.fleet(fleet);
        let cap = self.serving_capacities[fleet].sub_floor(victim.cap);
        let pre_ready = self.serving_pre_ready[fleet].sub_floor(victim.pre_ready);
        let demand = if horizon.payback.is_zero() {
            rates.total_load
        } else {
            rates.total_load
                + rates
                    .queued_work
                    .scaled(1.0 / horizon.payback.as_secs_f64())
        };
        let waterline = self.scaling.scale_in_waterline_percent.get();
        if !demand.within(cap, f64::from(waterline) / 100.0) {
            return None;
        }
        if self.queued[fleet] {
            let slots = self.summaries[fleet].slots;
            let buy = purchase(rates, cap, pre_ready, 0, horizon, slots, u32::MAX);
            if buy.instances > 0 || self.reachability_floor(fleet, Some(cap)) > 0 {
                return None;
            }
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
    use gang_config::{HistoryWindow, InstanceBounds, PaybackPercent};
    use gang_spi::{ExecutingDemand, FleetInstance, LaunchFailure, QueuedDemand, WorkerState};
    use gang_types::{
        ActionResources, Bytes, InstanceId, InstanceLifecycle, NonZeroDuration, WorkerId,
        nonzero_u32,
    };

    fn assert_float_eq(actual: f64, expected: f64) {
        // These fixtures use exactly representable integral slot coordinates, without a tolerance.
        assert_eq!(
            actual.total_cmp(&expected),
            std::cmp::Ordering::Equal,
            "actual={actual}, expected={expected}"
        );
    }

    fn policy(name: &str, slots: u32) -> FleetPolicy {
        let mut policy = gang_testkit::fleet_policy(name).expect("valid fleet name");
        policy.instances = InstanceBounds::up_to(nonzero_u32(20));
        policy.target_actions_per_instance = nonzero_u32(slots);
        policy.boot_grace = Duration::from_mins(10);
        policy.idle_timeout = Duration::from_mins(10);
        policy
    }

    fn queued(candidates: &[usize], expected: Option<Duration>) -> QueuedDemand {
        QueuedDemand {
            source: None,
            candidates: FleetSet::try_from_indices(candidates.iter().copied())
                .expect("valid fleets"),
            environment: Some(
                gang_testkit::fleet_policy("workers")
                    .expect("valid policy")
                    .action_environment,
            ),
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
            let planner = WorkPlanner::new(&policies, Arc::new(scaling), profiles.clone());
            Self {
                clock: TestClock::new(Timestamp::from_unix_millis(1_000_000)),
                capacities: policies
                    .iter()
                    .map(|_| Capacity {
                        live: 0,
                        standing: 0,
                        reliable: 0,
                        draining: 0,
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
                self.capacities[fleet].draining += 1;
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

    fn persistent_scene() -> (Scene, gang_types::SourceId) {
        let mut scene = Scene::new(vec![policy("workers", 16)], ScalingConfig::default());
        let source =
            gang_types::SourceId::from_identity_str("test:payback").expect("valid identity");
        let mut demand = queued(&[0], Some(Duration::from_secs(1)));
        demand.source = Some(source);
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![demand],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        let idle = Duration::from(scene.planner.scaling.source_idle);
        let cadence = idle / 2;
        let mut remaining = Duration::from_mins(30);
        while !remaining.is_zero() {
            let elapsed = cadence.min(remaining);
            scene.clock.advance(elapsed);
            scene.run(&[Some(Duration::from_secs(45))]);
            remaining = remaining.saturating_sub(elapsed);
        }
        scene.snapshot(SchedulerSnapshot::default(), &[]);
        scene.run(&[Some(Duration::from_secs(45))]);
        scene.clock.advance(idle);
        scene.run(&[Some(Duration::from_secs(45))]);
        let mut records = Vec::new();
        scene
            .profiles
            .snapshot(PassTime::read(&scene.clock), &mut records)
            .expect("profile snapshot");
        assert!(!records.is_empty());
        scene
            .profiles
            .restore(PassTime::read(&scene.clock), &records);
        scene.planner = WorkPlanner::new(
            &[scene.fleets[0].policy.clone()],
            Arc::clone(&scene.planner.scaling),
            scene.profiles.clone(),
        );
        (scene, source)
    }

    #[test]
    fn persistent_planner_queue_buys_only_payable_work() {
        for expensive in [false, true] {
            let (mut scene, source) = persistent_scene();
            let horizon = Horizon::new(
                Duration::from_secs(45),
                scene.fleets[0].policy.idle_timeout,
                scene.planner.scaling.payback_percent,
            );
            let duration = if expensive {
                horizon.payback * 2
            } else {
                Duration::from_secs(1)
            };
            let mut demand = queued(&[0], Some(duration));
            demand.source = Some(source);
            demand.queued_since = PassTime::read(&scene.clock).wall;
            scene.snapshot(
                SchedulerSnapshot {
                    queued: vec![demand; 160],
                    ..SchedulerSnapshot::default()
                },
                &[Some(0); 160],
            );
            scene.run(&[Some(Duration::from_secs(45))]);
            let summary = scene.planner.scale_decision(0);
            assert_eq!(summary.persistent_streams, 1);
            assert_float_eq(summary.persistent_queue.get(), 160.0);
            assert_float_eq(summary.transient_backlog.get(), 0.0);
            assert_eq!(scene.planner.work(0), if expensive { 10 } else { 1 });
        }
    }

    #[test]
    fn persistent_cheap_planner_queue_does_not_ratchet_while_booting() {
        let (mut scene, source) = persistent_scene();
        let mut demand = queued(&[0], Some(Duration::from_secs(1)));
        demand.source = Some(source);
        demand.queued_since = PassTime::read(&scene.clock).wall;
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![demand; 160],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 160],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.planner.work(0), 1);
        scene.member(0, "booting", InstanceLifecycle::Pending, true);
        scene.fleets[0].supply.instances[0].launched_at = PassTime::read(&scene.clock).wall;
        for elapsed in [8, 8, 8] {
            scene.clock.advance(Duration::from_secs(elapsed));
            scene.run(&[Some(Duration::from_secs(45))]);
            assert_eq!(scene.planner.scale_decision(0).persistent_streams, 1);
            assert_eq!(scene.planner.work(0), 1);
            assert_eq!(scene.planner.scale_decision(0).extra_instances, 0);
            assert_float_eq(scene.planner.scale_decision(0).pre_ready.get(), 16.0);
        }
        assert_float_eq(
            scene.planner.instance_view(0, 0).unwrap().pre_ready.actions,
            16.0 * 24.0,
        );
        let allocation = allocation_counter::measure(|| {
            scene.run(&[Some(Duration::from_secs(45))]);
        });
        assert_eq!(allocation.count_total, 0);
        assert_eq!(scene.planner.work(0), 1);
        scene.fleets[0].supply.instances[0].lifecycle = InstanceLifecycle::InService;
        let mut snapshot = scene.hosted.snapshot().clone();
        snapshot.queued.truncate(144);
        snapshot.workers = vec![worker(&scene.fleets[0].policy, "booting", 16, 16)];
        let mut running = executing(0, Some(Duration::from_secs(1)));
        running.source = Some(source);
        running.started = PassTime::read(&scene.clock).wall;
        snapshot.executing = vec![running; 16];
        scene.snapshot(snapshot, &[Some(0); 144]);
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_float_eq(scene.planner.rates.fleet(0).persistent_load.actions, 16.0);
        assert_eq!(
            scene.planner.rates.fleet(0).arrival_load,
            SlotVector::default()
        );
        assert_eq!(scene.planner.work(0), 1);
        assert_eq!(scene.planner.scale_decision(0).extra_instances, 0);
    }

    #[test]
    fn cold_queues_keep_one_at_zero_eight_and_nine_seconds() {
        let mut scene = Scene::new(vec![policy("workers", 16)], ScalingConfig::default());
        for (count, elapsed) in [(16, 0), (48, 8), (160, 1)] {
            scene.clock.advance(Duration::from_secs(elapsed));
            scene.snapshot(
                SchedulerSnapshot {
                    queued: vec![queued(&[0], None); count],
                    ..SchedulerSnapshot::default()
                },
                &vec![Some(0); count],
            );
            scene.run(&[None]);
            assert_eq!(scene.planner.work(0), 1);
            assert_eq!(scene.fleets[0].outlook.needed, 1);
            if count == 16 {
                scene.member(0, "booting", InstanceLifecycle::Pending, true);
            }
        }
    }

    #[test]
    fn seventeen_static_actions_do_not_ratchet_after_launch() {
        let mut scene = Scene::new(vec![policy("workers", 16)], ScalingConfig::default());
        let snapshot = SchedulerSnapshot {
            queued: vec![queued(&[0], None); 17],
            ..SchedulerSnapshot::default()
        };
        scene.snapshot(snapshot.clone(), &[Some(0); 17]);
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 1);
        scene.member(0, "booting", InstanceLifecycle::Pending, true);
        scene.clock.advance(Duration::from_secs(8));
        scene.snapshot(snapshot, &[Some(0); 17]);
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 1);
        assert_eq!(scene.planner.scale_decision(0).extra_instances, 0);
    }

    #[test]
    fn learned_burst_payback_includes_idle_tail() {
        for (percent, expected) in [(100, 1), (50, 1), (25, 3)] {
            let scaling = ScalingConfig {
                payback_percent: PaybackPercent::try_new(percent).expect("valid percent"),
                ..ScalingConfig::default()
            };
            let mut scene = Scene::new(vec![policy("workers", 16)], scaling);
            scene.member(0, "booting", InstanceLifecycle::Pending, true);
            scene.snapshot(
                SchedulerSnapshot {
                    queued: vec![queued(&[0], Some(Duration::from_mins(1))); 160],
                    ..SchedulerSnapshot::default()
                },
                &[Some(0); 160],
            );
            scene.run(&[Some(Duration::from_secs(45))]);
            assert_eq!(scene.planner.work(0), expected, "payback={percent}");
        }
    }

    #[test]
    fn persistent_cheap_queue_cannot_bypass_payback() {
        let queue = SlotVector {
            actions: 160.0,
            disk: 320.0,
            ..SlotVector::default()
        };
        let buy = purchase(
            &FleetRates {
                persistent_queue: queue,
                queued_work: queue,
                ..FleetRates::default()
            },
            SlotVector::default(),
            SlotVector::default(),
            0,
            Horizon {
                readiness: Duration::from_secs(10),
                payback: Duration::from_secs(8),
            },
            16,
            20,
        );
        assert_float_eq(buy.extra.actions, 20.0);
        assert_float_eq(buy.extra.disk, 40.0);
        assert_eq!(buy.instances, 2);
        let cheap = purchase(
            &FleetRates {
                persistent_queue: queue,
                queued_work: queue.scaled(0.125),
                ..FleetRates::default()
            },
            SlotVector::default(),
            SlotVector::default(),
            0,
            Horizon {
                readiness: Duration::from_secs(10),
                payback: Duration::from_secs(8),
            },
            16,
            20,
        );
        assert_float_eq(cheap.extra.actions, 2.5);
        assert_float_eq(cheap.extra.disk, 5.0);
        assert_eq!(cheap.instances, 0);
    }

    #[test]
    fn persistent_expensive_queue_still_buys_with_headroom() {
        let buy = purchase(
            &FleetRates {
                persistent_queue: nominal(64),
                queued_work: nominal(64).scaled(300.0),
                ..FleetRates::default()
            },
            SlotVector::default(),
            SlotVector::default(),
            0,
            Horizon {
                readiness: Duration::from_secs(45),
                payback: Duration::from_mins(5),
            },
            16,
            2,
        );
        assert_eq!(buy.extra, nominal(64));
        assert_eq!(buy.instances, 2);
    }

    #[test]
    fn payback_bounds_each_dimension_before_instance_flooring() {
        let buy = purchase(
            &FleetRates {
                persistent_queue: SlotVector {
                    cpu: 32.0,
                    memory: 16.0,
                    ..SlotVector::default()
                },
                queued_work: SlotVector {
                    cpu: 8.0,
                    memory: 64.0,
                    ..SlotVector::default()
                },
                ..FleetRates::default()
            },
            SlotVector::default(),
            SlotVector::default(),
            0,
            Horizon {
                readiness: Duration::ZERO,
                payback: Duration::from_secs(1),
            },
            16,
            10,
        );
        assert_float_eq(buy.extra.cpu, 8.0);
        assert_float_eq(buy.extra.memory, 16.0);
        assert_eq!(buy.instances, 1);
    }

    #[test]
    fn persistent_payback_credits_pre_ready_spare_and_booting_capacity() {
        let buy = purchase(
            &FleetRates {
                persistent_load: nominal(8),
                arrival_load: nominal(8),
                completion_load: nominal(8),
                persistent_queue: nominal(64),
                queued_work: nominal(40).scaled(10.0),
                ..FleetRates::default()
            },
            nominal(16),
            nominal(16).scaled(10.0),
            1,
            Horizon {
                readiness: Duration::from_secs(10),
                payback: Duration::from_secs(10),
            },
            16,
            10,
        );
        // ((400 + 8 * 10 - 160) / 10 - (16 - 8)) - 16 = 8.
        assert_eq!(buy.extra, nominal(8));
        assert_eq!(buy.instances, 0);
    }

    #[test]
    fn forecast_purchase_uses_net_growth_and_credits_pending_capacity() {
        for (arrival, service, work, booting, extra, instances) in [
            (0, 0, 4, 0, 0, 0),
            (32, 32, 4, 0, 1, 0),
            (24, 16, 4, 0, 13, 0),
            (24, 16, 16, 0, 16, 1),
            (24, 16, 16, 1, 0, 0),
        ] {
            let buy = purchase(
                &FleetRates {
                    persistent_load: nominal(16),
                    arrival_load: nominal(arrival),
                    completion_load: nominal(service),
                    persistent_queue: nominal(32),
                    queued_work: nominal(work),
                    ..FleetRates::default()
                },
                nominal(16),
                nominal(16).scaled(2.0),
                booting,
                Horizon {
                    readiness: Duration::from_secs(2),
                    payback: Duration::from_secs(4),
                },
                16,
                10,
            );
            assert_eq!(
                buy.extra,
                nominal(extra),
                "arrival={arrival}, service={service}"
            );
            assert_eq!(buy.instances, instances, "work={work}, booting={booting}");
        }
    }

    #[test]
    fn nonpersistent_purchase_keeps_existing_payback_arithmetic() {
        let work = nominal(320).scaled(60.0);
        let horizon = Horizon {
            readiness: Duration::from_secs(45),
            payback: Duration::from_mins(5),
        };
        let buy = purchase(
            &FleetRates {
                transient_work: work,
                queued_work: work,
                ..FleetRates::default()
            },
            nominal(4),
            nominal(16).scaled(30.0),
            1,
            horizon,
            16,
            10,
        );
        let transient = work
            .sub_floor(nominal(16).scaled(30.0))
            .scaled(1.0 / horizon.payback.as_secs_f64())
            .sub_floor(nominal(4));
        assert_eq!(buy.transient, transient);
        assert_eq!(buy.extra, transient.sub_floor(nominal(16)));
        assert_eq!(buy.instances, 2);
    }

    #[test]
    fn persistent_queue_nets_booting_after_adding_queue() {
        let horizon = Horizon {
            readiness: Duration::from_secs(45),
            payback: Duration::from_mins(5),
        };
        let rates = FleetRates {
            persistent_load: SlotVector {
                actions: 16.0,
                ..SlotVector::default()
            },
            arrival_load: SlotVector {
                actions: 16.0,
                ..SlotVector::default()
            },
            completion_load: SlotVector {
                actions: 16.0,
                ..SlotVector::default()
            },
            persistent_queue: SlotVector {
                actions: 40.0,
                ..SlotVector::default()
            },
            queued_work: SlotVector {
                actions: 40.0 * horizon.payback.as_secs_f64(),
                ..SlotVector::default()
            },
            ..FleetRates::default()
        };
        let buy = purchase(
            &rates,
            nominal(16),
            SlotVector::default(),
            2,
            horizon,
            16,
            10,
        );
        assert_float_eq(buy.extra.actions, 8.0);
        assert_eq!(buy.instances, 0);
        let buy = purchase(
            &FleetRates {
                persistent_queue: SlotVector {
                    actions: 32.0,
                    ..SlotVector::default()
                },
                queued_work: SlotVector {
                    actions: 32.0 * horizon.payback.as_secs_f64(),
                    ..SlotVector::default()
                },
                ..rates
            },
            SlotVector::default(),
            SlotVector::default(),
            1,
            horizon,
            16,
            10,
        );
        assert_float_eq(buy.extra.actions, 16.0);
        assert_eq!(buy.instances, 1);
    }

    #[test]
    fn booting_integral_and_unborn_requests_prevent_duplicate_buys() {
        let mut scene = Scene::new(vec![policy("workers", 16)], ScalingConfig::default());
        scene.member(0, "booting", InstanceLifecycle::Pending, true);
        scene.clock.advance(Duration::from_secs(30));
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![queued(&[0], Some(Duration::from_mins(1))); 160],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 160],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_float_eq(
            scene.planner.instance_view(0, 0).unwrap().pre_ready.actions,
            480.0,
        );
        assert_eq!(scene.planner.work(0), 1);
        scene.fleets[0].supply.desired = 3;
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.planner.work(0), 3);
        assert_float_eq(scene.planner.scale_decision(0).pre_ready.get(), 48.0);
    }

    #[test]
    fn launch_failure_excludes_unborn_capacity_and_empty_queue_drops_it() {
        let mut scene = Scene::new(vec![policy("workers", 16)], ScalingConfig::default());
        scene.fleets[0].supply.desired = 6;
        scene.fleets[0].supply.launch_failure = Some(LaunchFailure {
            reason: "unavailable".into(),
        });
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![queued(&[0], None); 16],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 16],
        );
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 1);
        assert_float_eq(scene.planner.scale_decision(0).pre_ready.get(), 0.0);
        assert_eq!(scene.fleets[0].outlook.expected_wait, None);
        scene.snapshot(SchedulerSnapshot::default(), &[]);
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 0);
        assert_eq!(scene.fleets[0].outlook.expected_wait, None);
    }

    #[test]
    fn reachability_uses_all_candidates_and_earlier_planned_fleets() {
        let mut scene = Scene::new(
            vec![policy("a", 16), policy("b", 16)],
            ScalingConfig::default(),
        );
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![
                    queued(&[0, 1], None),
                    queued(&[0, 1], None),
                    queued(&[], None),
                ],
                ..SchedulerSnapshot::default()
            },
            &[Some(0), Some(1), None],
        );
        scene.run(&[None, None]);
        assert_eq!(scene.planner.work(0), 1);
        assert_eq!(scene.planner.work(1), 0);
        assert_eq!(scene.planner.scale_decision(1).reachability, 0);
        assert_eq!(
            scene.fleets[1].outlook.expected_wait,
            Some(Duration::from_secs(90))
        );
        scene.fleets[0].supply.desired = 1;
        scene.run(&[None, None]);
        assert_eq!(scene.planner.scale_decision(0).reachability, 0);
        assert_eq!(scene.planner.work(1), 0);
    }

    #[test]
    fn minimum_applies_without_queue_and_prior_is_clamped_not_boot_fallback() {
        let mut fast = policy("fast", 16);
        fast.boot_grace = Duration::from_secs(30);
        let mut scene = Scene::new(vec![fast, policy("slow", 16)], ScalingConfig::default());
        scene.fleets[0].outlook.minimum = 2;
        scene.snapshot(SchedulerSnapshot::default(), &[]);
        scene.run(&[None, None]);
        assert_eq!(scene.planner.work(0), 0);
        assert_eq!(scene.fleets[0].outlook.needed, 2);
        assert_eq!(
            scene.planner.scale_decision(0).readiness,
            Duration::from_secs(30)
        );
        assert_eq!(
            scene.planner.scale_decision(1).readiness,
            Duration::from_secs(90)
        );
        assert_eq!(scene.fleets[0].outlook.readiness, None);
    }

    #[test]
    fn no_queue_never_buys_for_a_slow_running_action() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 1, 16)],
                executing: vec![executing(0, Some(Duration::from_hours(1)))],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[None]);
        assert_eq!(scene.planner.work(0), 1);
        assert_eq!(scene.fleets[0].outlook.expected_wait, None);
        assert!(scene.planner.removal(0, 0).is_none());
        assert_eq!(
            scene.planner.instance_view(0, 0).unwrap().empty_after,
            Duration::from_hours(1)
        );
    }

    #[test]
    fn free_integral_preserves_complementary_dimensions_and_count_gate() {
        let cap = nominal(16);
        let releases = [
            Release {
                after: Duration::from_secs(5),
                occupied: SlotVector {
                    actions: 1.0,
                    cpu: 16.0,
                    ..SlotVector::default()
                },
            },
            Release {
                after: Duration::from_secs(10),
                occupied: SlotVector {
                    actions: 1.0,
                    memory: 16.0,
                    ..SlotVector::default()
                },
            },
        ];
        let free = free_integral(cap, Duration::ZERO, &releases, Duration::from_secs(10), 16);
        assert_float_eq(free.actions, 145.0);
        assert_float_eq(free.cpu, 80.0);
        assert_float_eq(free.memory, 0.0);
        assert_float_eq(free.disk, 160.0);
        let busy = [Release {
            after: Duration::from_secs(5),
            occupied: SlotVector {
                actions: 16.0,
                ..SlotVector::default()
            },
        }];
        assert_eq!(
            free_integral(cap, Duration::ZERO, &busy, Duration::from_secs(5), 16),
            SlotVector::default()
        );
        let capped = SlotVector {
            actions: 2.0,
            ..cap
        };
        let free = free_integral(capped, Duration::ZERO, &[], Duration::from_secs(5), 16);
        assert_float_eq(free.actions, 10.0);
        assert_float_eq(free.memory, 80.0);
    }

    #[test]
    fn heavy_running_reservations_and_overlapping_connections_share_one_instance() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        let first = worker(&p, "busy", 1, 4);
        let mut second = worker(&p, "busy", 1, 4);
        second.id = WorkerId::try_new(format!(
            "{}6c5e4f3a-9b1d-4e2a-8f7c-000000000002",
            scene.fleets[0].supply.instances[0].id.worker_name(),
        ))
        .expect("renewed worker");
        let mut heavy = executing(1, Some(Duration::from_secs(100)));
        heavy.resources.memory = p.memory_per_instance.get();
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![first, second],
                executing: vec![executing(0, Some(Duration::from_secs(5))), heavy],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(10))]);
        let view = scene.planner.instance_view(0, 0).unwrap();
        assert_float_eq(view.cap.actions, 8.0);
        assert_float_eq(view.cap.memory, 16.0);
        assert_float_eq(view.pre_ready.memory, 0.0);
        assert_float_eq(view.pre_ready.actions, 65.0);
        assert_eq!(view.empty_after, Duration::from_secs(100));
        assert_eq!(scene.planner.scale_decision(0).reliable, 1);
    }

    #[test]
    fn resume_view_is_total_physical_capacity_not_connection_sum_or_increment() {
        let p = policy("workers", 16);
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
        assert_float_eq(view.cap.actions, 4.0);
        assert_float_eq(view.resume_cap.actions, 16.0);
        assert_float_eq(view.resume_cap.sub_floor(view.cap).actions, 12.0);
        assert_float_eq(view.resume_cap.memory, 16.0);
        assert_eq!(view.empty_after, Duration::from_secs(20));
    }

    #[test]
    fn excluded_orphan_and_cordon_views_never_supply_capacity() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "orphan", InstanceLifecycle::InService, false);
        scene.member(0, "cordoned", InstanceLifecycle::InService, false);
        let mut drain = worker(&p, "cordoned", 1, 16);
        drain.draining = true;
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![drain],
                executing: vec![executing(0, Some(Duration::from_secs(20)))],
                queued: vec![queued(&[0], None)],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.run(&[None]);
        assert_eq!(scene.planner.scale_decision(0).reachability, 1);
        assert_eq!(
            scene.planner.instance_view(0, 0).unwrap().cap,
            SlotVector::default()
        );
        let drain = scene.planner.instance_view(0, 1).unwrap();
        assert_eq!(drain.cap, SlotVector::default());
        assert_eq!(drain.resume_cap, nominal(16));
        assert!(!drain.serving);
        assert!(scene.planner.removal(0, 1).is_none());
    }

    #[test]
    fn expected_wait_retains_parallel_dispatch_and_order_not_purchase() {
        let p = policy("workers", 2);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        for (durations, expected) in [
            (vec![100, 1], 0),
            (vec![100, 100, 1], 100),
            (vec![1, 100, 100], 1),
        ] {
            scene.snapshot(
                SchedulerSnapshot {
                    workers: vec![worker(&p, "idle", 0, 2)],
                    queued: durations
                        .iter()
                        .map(|&d| queued(&[0], Some(Duration::from_secs(d))))
                        .collect(),
                    ..SchedulerSnapshot::default()
                },
                &vec![Some(0); durations.len()],
            );
            scene.run(&[Some(Duration::from_secs(45))]);
            assert_eq!(scene.planner.work(0), 1);
            assert_eq!(scene.planner.scale_decision(0).extra_instances, 0);
            assert_eq!(
                scene.fleets[0].outlook.expected_wait,
                Some(Duration::from_secs(expected))
            );
        }
    }

    #[test]
    fn missing_running_records_use_unknown_prior_and_keep_longest_projection() {
        let p = policy("workers", 2);
        let scaling = ScalingConfig {
            unknown_action_duration: NonZeroDuration::from_secs(10),
            ..ScalingConfig::default()
        };
        let mut scene = Scene::new(vec![p.clone()], scaling);
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 3, 2)],
                executing: vec![
                    executing(0, Some(Duration::from_secs(1))),
                    executing(0, Some(Duration::from_secs(100))),
                ],
                queued: vec![queued(&[0], Some(Duration::from_secs(1)))],
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
            Duration::from_secs(100)
        );
        let releases = [
            Release {
                after: Duration::from_secs(1),
                occupied: nominal(1),
            },
            Release {
                after: Duration::from_secs(10),
                occupied: SlotVector {
                    actions: 1000.0,
                    ..SlotVector::default()
                },
            },
            Release {
                after: Duration::from_secs(100),
                occupied: nominal(1),
            },
        ];
        let mut remaining = Vec::new();
        project_running(&mut remaining, &releases, 2);
        assert_eq!(remaining, [10_000, 100_000]);
        project_running(&mut remaining, &releases, 0);
        assert!(remaining.is_empty());
    }

    #[test]
    fn running_and_queued_unknown_durations_do_not_use_readiness() {
        let p = policy("workers", 2);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 2, 2)],
                executing: vec![executing(0, None); 2],
                queued: vec![queued(&[0], None); 3],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 3],
        );
        scene.run(&[Some(Duration::from_secs(90))]);
        assert_eq!(
            scene.fleets[0].outlook.expected_wait,
            Some(Duration::from_secs(20))
        );
        assert_eq!(
            scene.planner.instance_view(0, 0).unwrap().empty_after,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn n_minus_one_checks_all_load_and_subtracts_victim_integral() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 12, 16), worker(&p, "idle", 0, 16)],
                executing: vec![executing(0, Some(Duration::from_mins(10))); 12],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert!(scene.planner.removal(0, 1).is_none());
        assert!(!scene.planner.needs_resume(0, SlotVector::default()));
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 1, 16), worker(&p, "idle", 0, 16)],
                executing: vec![executing(0, Some(Duration::from_secs(10)))],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        let removal = scene
            .planner
            .removal(0, 1)
            .expect("one load fits remaining instance");
        assert_float_eq(removal.remaining_capacity.get(), 16.0);
        assert_eq!(removal.dwell, Duration::ZERO);
        assert_eq!(removal.empty_after, Duration::ZERO);
        assert_float_eq(
            scene.planner.instance_view(0, 1).unwrap().pre_ready.actions,
            720.0,
        );
    }

    #[test]
    fn empty_soonest_is_last_end_not_fewest_running() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "slow", InstanceLifecycle::InService, true);
        scene.member(0, "fast", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "slow", 1, 16), worker(&p, "fast", 2, 16)],
                executing: vec![
                    executing(0, Some(Duration::from_secs(100))),
                    executing(1, Some(Duration::from_secs(5))),
                    executing(1, Some(Duration::from_secs(8))),
                ],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[None]);
        assert_eq!(
            scene.planner.removal(0, 0).unwrap().empty_after,
            Duration::from_secs(100)
        );
        assert_eq!(
            scene.planner.removal(0, 1).unwrap().empty_after,
            Duration::from_secs(8)
        );
    }

    #[test]
    fn zero_overhead_buys_ceiling_with_queue_and_never_divides_by_zero() {
        let mut p = policy("workers", 16);
        p.idle_timeout = Duration::ZERO;
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "idle", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "idle", 0, 16)],
                queued: vec![queued(&[0], None); 48],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 48],
        );
        scene.run(&[Some(Duration::ZERO)]);
        assert_eq!(scene.planner.work(0), 3);
        assert!(scene.planner.scale_decision(0).zero_overhead);
        assert_eq!(scene.fleets[0].outlook.readiness, Some(Duration::ZERO));
        assert!(scene.planner.removal(0, 0).is_none());
        assert!(scene.planner.needs_resume(0, nominal(16)));
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "idle", 0, 16)],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::ZERO)]);
        assert_eq!(scene.planner.work(0), 0);
        assert!(!scene.planner.needs_resume(0, SlotVector::default()));
        assert!(scene.planner.removal(0, 0).is_some());
        assert_eq!(scene.fleets[0].outlook.utilization, Some(0));
    }

    #[test]
    fn huge_horizon_stays_finite_and_empty_outlook_keeps_measured_zero() {
        let mut p = policy("workers", 16);
        p.idle_timeout = Duration::MAX;
        let mut scene = Scene::new(vec![p], ScalingConfig::default());
        scene.snapshot(
            SchedulerSnapshot {
                queued: vec![queued(&[0], Some(Duration::MAX)); 16],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 16],
        );
        scene.run(&[Some(Duration::MAX)]);
        let summary = scene.planner.scale_decision(0);
        assert_eq!(summary.horizon, Duration::MAX);
        assert!(summary.transient_backlog.get().is_finite());
        assert!(summary.transient_extra.get().is_finite());
        assert_eq!(scene.planner.work(0), 1);
        scene.snapshot(SchedulerSnapshot::default(), &[]);
        scene.run(&[Some(Duration::ZERO)]);
        assert_eq!(scene.fleets[0].outlook.load, Some(0));
        assert_eq!(scene.fleets[0].outlook.backlog, Some(Duration::ZERO));
        assert_eq!(scene.fleets[0].outlook.utilization, None);
    }

    #[test]
    fn resource_ceiling_counts_running_reservations_without_double_weighting() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        let mut heavy = executing(0, Some(Duration::from_secs(100)));
        heavy.resources.memory = p.memory_per_instance.get();
        let mut waiting = queued(&[0], Some(Duration::from_secs(100)));
        waiting.resources.memory = Bytes::new(p.memory_per_instance.get().get());
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 1, 16)],
                executing: vec![heavy],
                queued: vec![waiting],
                ..SchedulerSnapshot::default()
            },
            &[Some(0)],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert_eq!(scene.fleets[0].outlook.demand.instances(&p), 2);
        assert_float_eq(
            scene.planner.instance_view(0, 0).unwrap().pre_ready.memory,
            0.0,
        );
        assert!(scene.planner.removal(0, 0).is_none());
        assert_eq!(scene.fleets[0].outlook.utilization, Some(1000));
    }

    #[test]
    fn warmed_planner_reuses_timeline_dispatch_and_source_buffers() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 4, 16)],
                executing: vec![executing(0, Some(Duration::from_secs(100))); 4],
                queued: vec![queued(&[0], None); 48],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 48],
        );
        for _ in 0..3 {
            scene.run(&[Some(Duration::from_secs(45))]);
        }
        let allocation = allocation_counter::measure(|| {
            scene.run(&[Some(Duration::from_secs(45))]);
        });
        assert_eq!(allocation.count_total, 0);
        assert_eq!(scene.planner.work(0), 1);
    }

    #[test]
    fn diagnostic_durations_share_authenticated_source_mean_but_not_none() {
        let p = policy("workers", 2);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "busy", InstanceLifecycle::InService, true);
        let source = gang_types::SourceId::from_identity_str("test:repo").expect("valid identity");
        let mut running = executing(0, None);
        running.source = Some(source);
        let mut known = queued(&[0], Some(Duration::from_secs(20)));
        known.source = Some(source);
        let mut unknown = queued(&[0], None);
        unknown.source = Some(source);
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 2, 2)],
                executing: vec![running; 2],
                queued: vec![known, unknown.clone(), unknown],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 3],
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

        let mut anonymous = Scene::new(vec![p.clone()], ScalingConfig::default());
        anonymous.member(0, "busy", InstanceLifecycle::InService, true);
        anonymous.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "busy", 2, 2)],
                executing: vec![executing(0, None); 2],
                queued: vec![
                    queued(&[0], Some(Duration::from_secs(20))),
                    queued(&[0], None),
                    queued(&[0], None),
                ],
                ..SchedulerSnapshot::default()
            },
            &[Some(0); 3],
        );
        anonymous.run(&[Some(Duration::from_secs(90))]);
        assert_eq!(
            anonymous.planner.instance_view(0, 0).unwrap().empty_after,
            Duration::from_secs(10)
        );
        assert_eq!(
            anonymous.fleets[0].outlook.expected_wait,
            Some(Duration::from_secs(20))
        );
    }

    #[test]
    fn busy_drain_load_requires_incremental_physical_resume_capacity() {
        let p = policy("workers", 16);
        let mut scene = Scene::new(vec![p.clone()], ScalingConfig::default());
        scene.member(0, "serving", InstanceLifecycle::InService, true);
        scene.member(0, "draining", InstanceLifecycle::InService, false);
        let mut drain = worker(&p, "draining", 12, 16);
        drain.draining = true;
        scene.snapshot(
            SchedulerSnapshot {
                workers: vec![worker(&p, "serving", 0, 16), drain],
                executing: vec![executing(1, Some(Duration::from_secs(30))); 12],
                ..SchedulerSnapshot::default()
            },
            &[],
        );
        scene.run(&[Some(Duration::from_secs(45))]);
        assert!(scene.planner.needs_resume(0, SlotVector::default()));
        let view = scene.planner.instance_view(0, 1).unwrap();
        assert!(
            !scene
                .planner
                .needs_resume(0, view.resume_cap.sub_floor(view.cap))
        );
        assert!(scene.planner.removal(0, 0).is_none());
        // Drains are a safety load floor, but are not serving utilization.
        assert_eq!(scene.fleets[0].outlook.utilization, Some(0));
    }

    #[test]
    fn complementary_transient_and_persistent_queue_buy_one_not_two() {
        let buy = purchase(
            &FleetRates {
                transient_work: SlotVector {
                    cpu: 16.0,
                    ..SlotVector::default()
                },
                persistent_queue: SlotVector {
                    memory: 16.0,
                    ..SlotVector::default()
                },
                queued_work: SlotVector {
                    cpu: 16.0,
                    memory: 16.0,
                    ..SlotVector::default()
                },
                ..FleetRates::default()
            },
            SlotVector::default(),
            SlotVector::default(),
            0,
            Horizon {
                readiness: Duration::ZERO,
                payback: Duration::from_secs(1),
            },
            16,
            10,
        );
        assert_float_eq(buy.extra.cpu, 16.0);
        assert_float_eq(buy.extra.memory, 16.0);
        assert_eq!(buy.instances, 1);
    }

    #[test]
    fn elapsed_projection_is_conservative_and_empty_time_uses_all_connections() {
        let p = policy("workers", 16);
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
        let view = scene.planner.instance_view(0, 0).unwrap();
        assert_eq!(view.empty_after, Duration::from_secs(30));
        assert_float_eq(view.pre_ready.actions, 16.0 * 45.0 - 20.0 - 30.0);
    }

    #[test]
    fn integral_credits_only_capacity_after_readiness_and_handles_zero_horizon() {
        let cap = nominal(16);
        let releases = [Release {
            after: Duration::from_secs(20),
            occupied: SlotVector {
                actions: 1.0,
                memory: 16.0,
                ..SlotVector::default()
            },
        }];
        let free = free_integral(
            cap,
            Duration::from_secs(30),
            &releases,
            Duration::from_secs(45),
            16,
        );
        assert_eq!(free, cap.scaled(15.0));
        assert_eq!(
            free_integral(cap, Duration::ZERO, &releases, Duration::ZERO, 16),
            SlotVector::default()
        );
    }

    #[test]
    fn slot_ties_spread_by_slot_then_instance_and_fleet() {
        let mut dispatch = Dispatch::new(2);
        dispatch.add_instance(0, 0, 2, 0);
        dispatch.add_instance(0, 1, 2, 0);
        dispatch.add_instance(1, 0, 2, 0);
        let both = FleetSet::prefix(2).expect("two fleets");
        let first = dispatch.best(both).unwrap();
        assert_eq!((first.1, first.0.index, first.0.instance), (0, 0, 0));
        dispatch.heaps[0].pop();
        let second = dispatch.best(both).unwrap();
        assert_eq!((second.1, second.0.index, second.0.instance), (1, 0, 0));
        dispatch.heaps[1].pop();
        let third = dispatch.best(both).unwrap();
        assert_eq!((third.1, third.0.index, third.0.instance), (0, 0, 1));
    }

    #[test]
    fn count_and_display_quantization_round_and_saturate_at_their_boundaries() {
        assert_eq!(floor_to_u32(15.99), 15);
        assert_eq!(floor_to_u32(16.0), 16);
        assert_eq!(floor_to_u32(f64::MAX), u32::MAX);
        assert_eq!(floor_to_u32(-1.0), 0);
        assert_eq!(rounded_backlog(6.49), Duration::from_secs(6));
        assert_eq!(rounded_backlog(6.5), Duration::from_secs(7));
        assert_eq!(rounded_backlog(f64::MAX), Duration::from_secs(u64::MAX));
        let mut fleet = FleetPass::new(policy("workers", 16));
        update_metrics(
            &mut fleet,
            &FleetRates {
                persistent_load: SlotVector {
                    actions: 1.26,
                    ..SlotVector::default()
                },
                serving_load: SlotVector {
                    actions: 1.0,
                    ..SlotVector::default()
                },
                ..FleetRates::default()
            },
            SlotVector {
                actions: 2.0,
                ..SlotVector::default()
            },
        );
        assert_eq!(fleet.outlook.load, Some(13));
        assert_eq!(fleet.outlook.utilization, Some(500));
    }
}
