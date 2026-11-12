//! The autoscaler's control loop: each pass snapshots the scheduler and the fleets, reconciles,
//! and applies the resulting plan through the ports.

use std::convert::Infallible;

use gang_clock::{Clock, Interval, Timestamp};
use gang_config::{FleetConfig, Fleets, PluginSet};
use gang_reconciler::{Action, FleetPass, HostedSnapshot, PassTime, Plan, PlanStep, Reconciler};
use gang_spi::{
    PortError, ProvisionerPort, RecordedScalingEvent, ScalingAuditEvent, ScalingEventKind,
    SchedulerPort, SchedulerSnapshot, StateRepositoryPort, Subject, WorkerState,
};
use gang_state::PassStatus;
use gang_types::{FleetName, NonZeroDuration, WorkerId};
use gang_wire::SkipReason;

use crate::publish::StatePublisher;

/// What one control pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PassOutcome {
    /// The pass was abandoned before any step was planned or applied, for the given reason.
    Skipped(SkipReason),
    /// The plan was applied. `applied` steps succeeded and `failed` steps did not. Steps that
    /// found their worker already gone count as neither.
    Completed {
        /// Steps that were applied successfully.
        applied: u32,
        /// Steps that failed and will be planned again by a later pass.
        failed: u32,
    },
}

/// Runs reconcile passes against borrowed ports.
pub(crate) struct ControlLoop<'a, P: PluginSet, F, S, R, C> {
    /// Configured fleet names used to filter workers.
    fleets: &'a Fleets<P>,
    /// Fleet configurations in the same order as `passes` and plan fleet indices.
    configs: Vec<FleetConfig<'a, P>>,
    /// Each fleet in the reconciler's terms, in declaration order.
    passes: Vec<FleetPass>,
    /// The indexed scheduler snapshot of the latest completed pass.
    hosted: HostedSnapshot,
    /// Buffer for the next scheduler snapshot.
    incoming: SchedulerSnapshot,
    /// Workers seen in the previous completed pass.
    roster: WorkerRoster,
    /// The worker registration and deregistration events of the current pass, before recording.
    worker_changes: Vec<ScalingAuditEvent>,
    plan: Plan,
    reconciler: Reconciler,
    poll_interval: NonZeroDuration,
    provisioner: &'a F,
    scheduler: &'a S,
    repository: &'a R,
    clock: &'a C,
    /// Receives the state of each pass and of each recorded event, when set.
    publisher: Option<StatePublisher>,
}

impl<'a, P, F, S, R, C> ControlLoop<'a, P, F, S, R, C>
where
    P: PluginSet<FleetOptions = F::FleetOptions>,
    F: ProvisionerPort,
    S: SchedulerPort,
    R: StateRepositoryPort,
    C: Clock,
{
    /// Creates a control loop for the worker fleets, reading and writing through the given ports.
    ///
    /// Every fleet starts with an empty supply, which the first pass replaces.
    #[must_use]
    pub(crate) fn new(
        fleets: &'a Fleets<P>,
        poll_interval: NonZeroDuration,
        provisioner: &'a F,
        scheduler: &'a S,
        repository: &'a R,
        clock: &'a C,
    ) -> Self {
        let configs: Vec<FleetConfig<'a, P>> = fleets.iter().collect();
        let passes = configs
            .iter()
            .map(|config| FleetPass::new(config.policy.clone()))
            .collect();
        let reconciler = Reconciler::new(fleets.policies());
        Self {
            fleets,
            configs,
            passes,
            hosted: HostedSnapshot::default(),
            incoming: SchedulerSnapshot::default(),
            roster: WorkerRoster::default(),
            worker_changes: Vec::new(),
            plan: Plan::new(),
            reconciler,
            poll_interval,
            provisioner,
            scheduler,
            repository,
            clock,
            publisher: None,
        }
    }

    /// Sets the publisher that receives the state of every pass and of every recorded event.
    #[must_use]
    pub(crate) fn with_publisher(mut self, publisher: StatePublisher) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Runs one pass: read the snapshot and supplies, sample the clocks, replace the indexed
    /// snapshot, record worker registrations, deregistrations and early exits, reconcile, then apply
    /// every planned step. The pass's state is published last with the same wall time as its events.
    ///
    /// Only a failed snapshot or supply read skips a pass, before any step is applied. A skipped
    /// pass samples its clock at the skip. A failed step does not stop the others.
    pub(crate) async fn pass(&mut self) -> PassOutcome {
        let (wall, outcome) = match self.plan_pass().await {
            Ok(time) => (time.wall, self.apply_plan().await),
            Err(reason) => (
                PassTime::read(self.clock).wall,
                PassOutcome::Skipped(reason),
            ),
        };
        self.publish(wall, &outcome);
        outcome
    }

    /// Reads the snapshot and supplies, samples the clocks, replaces the indexed snapshot, records
    /// worker changes and early exits and plans the steps. Returns the pass time or why it is skipped.
    async fn plan_pass(&mut self) -> Result<PassTime, SkipReason> {
        if !self.read_snapshot().await {
            return Err(SkipReason::SchedulerUnavailable);
        }
        if !self.refresh_supplies().await {
            return Err(SkipReason::ProvisionerUnavailable);
        }
        let time = PassTime::read(self.clock);
        self.hosted.replace(&mut self.incoming);
        self.record_worker_changes(time.wall).await;
        self.record_exits(time.wall).await;
        self.reconciler
            .reconcile(&mut self.passes, &self.hosted, time, &mut self.plan);
        Ok(time)
    }

    /// Publishes the state of a finished pass when a publisher is set. A completed pass
    /// publishes its indexed snapshot and fleets with their supplies and outlooks. A skipped
    /// pass publishes only the skip.
    fn publish(&mut self, wall: Timestamp, outcome: &PassOutcome) {
        let Some(publisher) = self.publisher.as_mut() else {
            return;
        };
        match outcome {
            PassOutcome::Completed { applied, failed } => {
                publisher.publish(
                    wall,
                    PassStatus::Ok {
                        applied: *applied,
                        failed: *failed,
                        hosted: &self.hosted,
                    },
                    &self.passes,
                    self.fleets.catalog(),
                );
            }
            PassOutcome::Skipped(reason) => {
                publisher.publish(
                    wall,
                    PassStatus::Skipped(reason.clone()),
                    &self.passes,
                    self.fleets.catalog(),
                );
            }
        }
    }

    /// Runs a pass at once, then one every `poll_interval` on the injected clock, forever. A pass
    /// that overruns the interval delays the next one rather than running passes back to back.
    pub(crate) async fn run(mut self) -> Infallible {
        let mut ticks = Interval::new(self.clock, self.clock.monotonic(), self.poll_interval);
        loop {
            ticks.tick().await;
            self.pass().await;
        }
    }

    /// Refreshes the scheduler snapshot. Returns whether the pass may continue.
    async fn read_snapshot(&mut self) -> bool {
        match self.scheduler.snapshot(&mut self.incoming).await {
            Ok(()) => true,
            Err(error @ PortError::Unavailable(_)) => {
                tracing::debug!(%error, "scheduler unavailable; skipping pass");
                false
            }
            Err(error) => {
                tracing::warn!(%error, "scheduler snapshot failed; skipping pass");
                false
            }
        }
    }

    /// Records workers registered or deregistered since the previous pass, then remembers the
    /// workers of this snapshot. Every event is stamped with the wall clock of this pass.
    async fn record_worker_changes(&mut self, at: Timestamp) {
        self.roster.diff(
            &self.hosted.snapshot().workers,
            self.fleets,
            at,
            &mut self.worker_changes,
        );
        for event in self.worker_changes.drain(..) {
            Self::record_event(self.repository, self.publisher.as_ref(), event).await;
        }
    }

    /// Records the instances whose worker failed soon after its launch, as this pass's supplies
    /// report them. Every event is stamped with the wall clock of this pass.
    async fn record_exits(&mut self, at: Timestamp) {
        for (config, pass) in self.configs.iter().zip(&self.passes) {
            for exit in &pass.supply.exits {
                Self::record_event(
                    self.repository,
                    self.publisher.as_ref(),
                    ScalingAuditEvent {
                        at,
                        fleet: config.policy.name.clone(),
                        kind: ScalingEventKind::ExitedEarly {
                            instance: exit.instance.clone(),
                            failure: exit.failure.clone(),
                        },
                        cause: None,
                    },
                )
                .await;
            }
        }
    }

    /// Refreshes every fleet's supply. Returns whether the pass may continue.
    async fn refresh_supplies(&mut self) -> bool {
        for (config, pass) in self.configs.iter().zip(self.passes.iter_mut()) {
            if let Err(error) = self
                .provisioner
                .supply(config.fleet(), &mut pass.supply)
                .await
            {
                tracing::warn!(
                    fleet = %config.policy.name,
                    %error,
                    "provisioner supply failed; skipping pass"
                );
                return false;
            }
        }
        true
    }

    /// Applies the planned steps in order and reports how many succeeded.
    async fn apply_plan(&mut self) -> PassOutcome {
        let mut applied = 0;
        let mut failed = 0;
        for step in &self.plan.steps {
            match Self::apply(self.provisioner, self.scheduler, &self.configs, step).await {
                Ok(()) => {
                    applied += 1;
                    let event =
                        step.audit_event(self.plan.at, &self.passes[step.fleet].policy.name);
                    Self::record_event(self.repository, self.publisher.as_ref(), event).await;
                }
                Err(PortError::NotFound(Subject::Worker(worker))) => {
                    tracing::debug!(worker = ?worker, "worker not found; skipping drain step");
                }
                Err(error) => {
                    if let Action::Terminate { instance, .. } = &step.action {
                        self.reconciler.forget_order(step.fleet, instance);
                    }
                    tracing::warn!(?step, %error, "control step failed");
                    failed += 1;
                }
            }
        }
        PassOutcome::Completed { applied, failed }
    }

    /// Applies one step through the port that owns it.
    async fn apply(
        provisioner: &F,
        scheduler: &S,
        configs: &[FleetConfig<'_, P>],
        step: &PlanStep,
    ) -> Result<(), PortError> {
        match &step.action {
            Action::ScaleOut { to, .. } => {
                provisioner
                    .set_desired_capacity(configs[step.fleet].fleet(), *to)
                    .await
            }
            Action::StartDrain(worker) => scheduler.set_draining(worker, true).await,
            Action::CancelDrain(worker) => scheduler.set_draining(worker, false).await,
            Action::Terminate { instance, .. } => {
                provisioner
                    .terminate(configs[step.fleet].fleet(), instance)
                    .await
            }
        }
    }

    /// Records one audit event and adds it to the published history. A recording failure is
    /// logged and publishes nothing.
    async fn record_event(
        repository: &R,
        publisher: Option<&StatePublisher>,
        event: ScalingAuditEvent,
    ) {
        match repository.record_scaling_event(&event).await {
            Ok(seq) => {
                if let Some(publisher) = publisher {
                    publisher.record_event(RecordedScalingEvent { seq, event });
                }
            }
            Err(error) => tracing::error!(%error, "failed to record scaling event"),
        }
    }
}

/// Workers seen in the previous completed pass, sorted by worker id.
#[derive(Debug, Default, PartialEq, Eq)]
struct WorkerRoster {
    connected: Vec<(WorkerId, FleetName)>,
}

impl WorkerRoster {
    /// Appends registrations and then deregistrations in worker id order, tracking configured fleets.
    /// A worker changing between configured fleets keeps its id without an event.
    /// Workers are sorted by id. An unchanged pass allocates nothing once the roster has grown.
    fn diff<P: PluginSet>(
        &mut self,
        workers: &[WorkerState],
        fleets: &Fleets<P>,
        at: Timestamp,
        into: &mut Vec<ScalingAuditEvent>,
    ) {
        let connected = &mut self.connected;
        let known = connected.len();
        let mut cursor = 0;
        for worker in workers
            .iter()
            .filter(|worker| fleets.position(&worker.fleet).is_some())
        {
            while cursor < known && connected[cursor].0 < worker.id {
                cursor += 1;
            }
            if cursor < known && connected[cursor].0 == worker.id {
                continue;
            }
            into.push(ScalingAuditEvent {
                at,
                fleet: worker.fleet.clone(),
                kind: ScalingEventKind::WorkerRegistered {
                    worker: worker.id.clone(),
                },
                cause: None,
            });
            connected.push((worker.id.clone(), worker.fleet.clone()));
        }
        let registered = connected.len() > known;
        let mut current = workers
            .iter()
            .filter(|worker| fleets.position(&worker.fleet).is_some())
            .peekable();
        for (id, fleet) in connected.extract_if(..known, |(id, fleet)| {
            while current.peek().is_some_and(|worker| worker.id < *id) {
                current.next();
            }
            let Some(worker) = current.peek().filter(|worker| worker.id == *id) else {
                return true;
            };
            if worker.fleet != *fleet {
                fleet.clone_from(&worker.fleet);
            }
            false
        }) {
            into.push(ScalingAuditEvent {
                at,
                fleet,
                kind: ScalingEventKind::WorkerDeregistered { worker: id },
                cause: None,
            });
        }
        if registered {
            connected.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    fn at() -> Timestamp {
        Timestamp::from_unix_millis(5_000)
    }

    fn worker(id: &str, fleet: &str) -> WorkerState {
        WorkerState {
            id: WorkerId::try_new(id).unwrap(),
            fleet: FleetName::try_new(fleet).unwrap(),
            running_actions: 0,
            max_inflight_actions: NonZeroU32::new(1),
            draining: false,
            paused: false,
            last_seen: at(),
        }
    }

    /// Connected workers in the roster's id order.
    fn seen(entries: &[(&str, &str)]) -> WorkerRoster {
        let mut connected: Vec<(WorkerId, FleetName)> = entries
            .iter()
            .map(|(id, fleet)| {
                (
                    WorkerId::try_new(*id).unwrap(),
                    FleetName::try_new(*fleet).unwrap(),
                )
            })
            .collect();
        connected.sort_by(|a, b| a.0.cmp(&b.0));
        WorkerRoster { connected }
    }

    fn registered(id: &str, fleet: &str) -> ScalingAuditEvent {
        ScalingAuditEvent {
            at: at(),
            fleet: FleetName::try_new(fleet).unwrap(),
            kind: ScalingEventKind::WorkerRegistered {
                worker: WorkerId::try_new(id).unwrap(),
            },
            cause: None,
        }
    }

    fn deregistered(id: &str, fleet: &str) -> ScalingAuditEvent {
        ScalingAuditEvent {
            at: at(),
            fleet: FleetName::try_new(fleet).unwrap(),
            kind: ScalingEventKind::WorkerDeregistered {
                worker: WorkerId::try_new(id).unwrap(),
            },
            cause: None,
        }
    }

    /// Runs one roster pass and returns the events it recorded.
    fn events_of(connected: &mut WorkerRoster, workers: &[WorkerState]) -> Vec<ScalingAuditEvent> {
        let mut into = Vec::new();
        let fleets = crate::test_support::fleets_of(vec![("x86", 4), ("arm", 4)]);
        connected.diff(workers, &fleets, at(), &mut into);
        into
    }

    #[test]
    fn first_observation_registers_every_worker_in_id_order() {
        let mut connected = seen(&[]);
        let workers = [worker("local-1", "arm"), worker("local-2", "x86")];
        assert_eq!(
            events_of(&mut connected, &workers),
            vec![registered("local-1", "arm"), registered("local-2", "x86")]
        );
    }

    #[test]
    fn new_worker_registers_alone() {
        let mut connected = seen(&[("local-1", "x86")]);
        let workers = [worker("local-1", "x86"), worker("local-2", "x86")];
        assert_eq!(
            events_of(&mut connected, &workers),
            vec![registered("local-2", "x86")]
        );
    }

    #[test]
    fn missing_worker_deregisters_on_the_fleet_it_was_seen_on() {
        let mut connected = seen(&[("local-1", "x86"), ("local-2", "arm")]);
        let workers = [worker("local-1", "x86")];
        assert_eq!(
            events_of(&mut connected, &workers),
            vec![deregistered("local-2", "arm")]
        );
    }

    #[test]
    fn unchanged_workers_record_nothing() {
        let mut connected = seen(&[("local-1", "x86")]);
        let workers = [worker("local-1", "x86")];
        assert!(events_of(&mut connected, &workers).is_empty());
        assert_eq!(connected, seen(&[("local-1", "x86")]));
    }

    #[test]
    fn deregistered_worker_registers_again_when_it_returns() {
        let mut connected = seen(&[("local-1", "x86")]);
        assert_eq!(
            events_of(&mut connected, &[]),
            vec![deregistered("local-1", "x86")]
        );
        // The pass that recorded the deregistration remembers the empty snapshot.
        assert_eq!(
            events_of(&mut connected, &[worker("local-1", "x86")]),
            vec![registered("local-1", "x86")]
        );
    }

    #[test]
    fn worker_that_changes_fleet_keeps_its_id_without_an_event() {
        let mut connected = seen(&[("local-1", "x86")]);
        assert!(events_of(&mut connected, &[worker("local-1", "arm")]).is_empty());
        assert_eq!(connected, seen(&[("local-1", "arm")]));
    }

    #[test]
    fn missing_worker_deregisters_on_the_fleet_it_was_last_seen_on() {
        let mut connected = seen(&[]);
        assert_eq!(
            events_of(&mut connected, &[worker("local-1", "x86")]),
            vec![registered("local-1", "x86")]
        );
        assert!(events_of(&mut connected, &[worker("local-1", "arm")]).is_empty());
        assert_eq!(
            events_of(&mut connected, &[]),
            vec![deregistered("local-1", "arm")]
        );
    }

    #[test]
    fn registrations_follow_worker_id_order_and_deregistrations_follow_id_order() {
        let mut connected = seen(&[("c-old", "x86"), ("a-old", "arm")]);
        let workers = [worker("local-3", "arm"), worker("local-9", "x86")];
        assert_eq!(
            events_of(&mut connected, &workers),
            vec![
                registered("local-3", "arm"),
                registered("local-9", "x86"),
                deregistered("a-old", "arm"),
                deregistered("c-old", "x86"),
            ]
        );
    }

    #[test]
    fn registrations_follow_snapshot_worker_id_order() {
        let mut connected = seen(&[]);
        let workers = [
            worker("local-a", "x86"),
            worker("local-b", "x86"),
            worker("local-c", "x86"),
        ];
        assert_eq!(
            events_of(&mut connected, &workers),
            vec![
                registered("local-a", "x86"),
                registered("local-b", "x86"),
                registered("local-c", "x86"),
            ]
        );
        assert_eq!(
            connected,
            seen(&[("local-a", "x86"), ("local-b", "x86"), ("local-c", "x86")])
        );
    }

    #[test]
    fn registrations_among_known_workers_follow_id_order_and_connected_stays_sorted() {
        let mut connected = seen(&[("local-b", "x86")]);
        let workers = [
            worker("local-a", "x86"),
            worker("local-b", "x86"),
            worker("local-c", "arm"),
            worker("local-d", "arm"),
        ];
        assert_eq!(
            events_of(&mut connected, &workers),
            vec![
                registered("local-a", "x86"),
                registered("local-c", "arm"),
                registered("local-d", "arm"),
            ]
        );
        assert_eq!(
            connected,
            seen(&[
                ("local-a", "x86"),
                ("local-b", "x86"),
                ("local-c", "arm"),
                ("local-d", "arm"),
            ])
        );
    }

    #[test]
    fn registrations_precede_deregistrations() {
        let mut connected = seen(&[("local-x", "x86")]);
        let workers = [worker("local-k", "x86"), worker("local-m", "x86")];
        assert_eq!(
            events_of(&mut connected, &workers),
            vec![
                registered("local-k", "x86"),
                registered("local-m", "x86"),
                deregistered("local-x", "x86"),
            ]
        );
        assert_eq!(connected, seen(&[("local-k", "x86"), ("local-m", "x86")]));
    }

    #[test]
    fn unchanged_worker_set_allocates_nothing_once_tracked() {
        const PASSES: usize = 10;
        let workers = [
            worker("local-1", "arm"),
            worker("local-2", "x86"),
            worker("local-3", "x86"),
        ];
        let mut connected = WorkerRoster::default();
        let fleets = crate::test_support::fleets_of(vec![("x86", 4), ("arm", 4)]);
        let mut into = Vec::new();
        connected.diff(&workers, &fleets, at(), &mut into);
        assert_eq!(into.len(), workers.len());
        into.clear();
        let counts = allocation_counter::measure(|| {
            for _ in 0..PASSES {
                connected.diff(&workers, &fleets, at(), &mut into);
            }
        });
        assert_eq!(counts.count_total, 0, "unchanged worker set allocated");
        assert!(into.is_empty());
    }
}
