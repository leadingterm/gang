//! The autoscaler's control loop: each pass snapshots the scheduler and the fleets, reconciles,
//! and applies the resulting plan through the ports.

use std::convert::Infallible;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Clock, Interval, Timestamp};
use gang_config::{DashboardConfig, FleetConfig, Fleets, PluginSet, ScalingConfig};
use gang_reconciler::{
    Action, FleetPass, HostedSnapshot, PassTime, Plan, PlanStep, Reconciler, SourceProfiles,
};
use gang_spi::{
    ExpiringRecordPort, Fence, FleetInfrastructureEvent, FleetSupply, InfrastructureEventKind,
    Insert, LeaseLapsed, PortError, ProvisionerPort, Record, RecordId, RecordKey, RecordKind,
    RecordVersion, RecordedScalingEvent, RunError, ScalingAuditEvent, ScalingEventKind,
    SchedulerPort, SchedulerSnapshot, StateRepositoryPort, Subject, WorkerState,
};
use gang_state::PassStatus;
use gang_types::{FleetName, InstanceId, InstanceLifecycle, NonZeroDuration, WorkerId};
use gang_wire::SkipReason;

use crate::publish::StatePublisher;

/// What one control pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PassOutcome {
    /// The pass was abandoned for the given reason; earlier applied steps remain recorded.
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

/// Control-loop timing and infrastructure observation retention.
#[derive(Clone, Copy)]
pub(crate) struct ControlSettings {
    pub(crate) poll_interval: NonZeroDuration,
    infrastructure_window: Duration,
    infrastructure_limit: NonZeroUsize,
}

impl ControlSettings {
    pub(crate) fn new(poll_interval: NonZeroDuration, dashboard: &DashboardConfig) -> Self {
        Self {
            poll_interval,
            infrastructure_window: dashboard.history_window.get(),
            infrastructure_limit: dashboard.recent_events,
        }
    }
}

/// Ports borrowed by the control loop for one leadership term.
pub(crate) struct ControlPorts<'a, F, S, R, C> {
    pub(crate) provisioner: &'a F,
    pub(crate) scheduler: &'a S,
    pub(crate) repository: &'a R,
    pub(crate) clock: &'a C,
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
    /// Scratch roster, committed only after its audit events are recorded.
    roster_pending: WorkerRoster,
    /// The worker registration and deregistration events of the current pass, before recording.
    worker_changes: Vec<ScalingAuditEvent>,
    /// Last recorded launch failure reason for each fleet.
    launch_failures: Vec<Option<String>>,
    configured_fleets: bool,
    infrastructure_window: Duration,
    infrastructure_limit: NonZeroUsize,
    infrastructure: Vec<FleetInfrastructureEvent>,
    infrastructure_pending: Vec<FleetInfrastructureEvent>,
    infrastructure_page: Vec<(RecordId, Record)>,
    plan: Plan,
    /// Failed termination calls awaiting reconciliation with a fresh supply.
    pending_terminations: Vec<PlanStep>,
    reconciler: Reconciler,
    poll_interval: NonZeroDuration,
    max_boot_grace: Duration,
    provisioner: &'a F,
    scheduler: &'a S,
    repository: &'a R,
    clock: &'a C,
    fence: Fence,
    /// Receives the state of each pass and of each recorded event, when set.
    publisher: Option<StatePublisher>,
}

impl<'a, P, F, S, R, C> ControlLoop<'a, P, F, S, R, C>
where
    P: PluginSet<FleetOptions = F::FleetOptions>,
    F: ProvisionerPort,
    S: SchedulerPort,
    R: StateRepositoryPort + ExpiringRecordPort,
    C: Clock,
{
    /// Creates a control loop for the worker fleets, reading and writing through the given ports.
    ///
    /// Every fleet starts with an empty supply, which the first pass replaces.
    #[must_use]
    pub(crate) fn new(
        fleets: &'a Fleets<P>,
        settings: ControlSettings,
        scaling: Arc<ScalingConfig>,
        profiles: SourceProfiles,
        cpu_oversubscription: NonZeroU32,
        ports: &ControlPorts<'a, F, S, R, C>,
        fence: Fence,
    ) -> Self {
        let &ControlPorts {
            provisioner,
            scheduler,
            repository,
            clock,
        } = ports;
        let configs: Vec<FleetConfig<'a, P>> = fleets.iter().collect();
        let passes = configs
            .iter()
            .map(|config| FleetPass::new(config.policy.clone()))
            .collect();
        let launch_failures = vec![None; configs.len()];
        let max_boot_grace = fleets
            .policies()
            .iter()
            .map(|policy| policy.boot_grace)
            .max()
            .unwrap_or_default();
        let reconciler =
            Reconciler::new(fleets.policies(), scaling, profiles, cpu_oversubscription);
        Self {
            fleets,
            configs,
            passes,
            hosted: HostedSnapshot::default(),
            incoming: SchedulerSnapshot::default(),
            roster: WorkerRoster::default(),
            roster_pending: WorkerRoster::default(),
            worker_changes: Vec::new(),
            launch_failures,
            configured_fleets: false,
            infrastructure_window: settings.infrastructure_window,
            infrastructure_limit: settings.infrastructure_limit,
            infrastructure: Vec::new(),
            infrastructure_pending: Vec::new(),
            infrastructure_page: Vec::new(),
            plan: Plan::new(),
            pending_terminations: Vec::new(),
            reconciler,
            poll_interval: settings.poll_interval,
            max_boot_grace,
            provisioner,
            scheduler,
            repository,
            clock,
            fence,
            publisher: None,
        }
    }

    /// Sets the publisher that receives the state of every pass and of every recorded event.
    #[must_use]
    pub(crate) fn with_publisher(mut self, publisher: StatePublisher) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Runs one pass: read the snapshot, converge each fleet and read its supply, sample the clocks,
    /// replace the indexed snapshot, record worker changes and early exits, reconcile, then apply
    /// every planned step. Exit events use the current pass time. The pass's state is published
    /// last with the same wall time as its events.
    ///
    /// A failed snapshot, converge or supply read, lapsed leadership, or unavailable scheduler
    /// during a drain skips the pass. Other failed steps do not stop the others.
    pub(crate) async fn pass(&mut self) -> PassOutcome {
        let (mut wall, outcome) = match self.plan_pass().await {
            Ok(time) => (time.wall, self.apply_plan().await),
            Err(reason) => (
                PassTime::read(self.clock).wall,
                PassOutcome::Skipped(reason),
            ),
        };
        if matches!(outcome, PassOutcome::Skipped(_)) {
            wall = PassTime::read(self.clock).wall;
            self.reconciler.interrupt();
        }
        self.publish(wall, &outcome);
        outcome
    }

    /// Reads the snapshot, converges and reads each fleet, samples the clocks, records worker
    /// changes and early exits, and plans the steps. Returns the pass time or why it is skipped.
    async fn plan_pass(&mut self) -> Result<PassTime, SkipReason> {
        if !self.read_snapshot().await {
            return Err(SkipReason::SchedulerUnavailable);
        }
        self.refresh_supplies().await?;
        if !self.fence.holds() {
            return Err(SkipReason::LeadershipLapsed);
        }
        let time = PassTime::read(self.clock);
        let live: Vec<_> = self
            .passes
            .iter()
            .map(|pass| (&pass.policy.name, &pass.supply))
            .collect();
        self.scheduler
            .retain_drains(time.now, self.max_boot_grace, &live)
            .await
            .map_err(|error| {
                tracing::warn!(%error, "cannot retain physical drains; skipping pass");
                SkipReason::SchedulerUnavailable
            })?;
        self.hosted.replace(&mut self.incoming);
        if self.record_infrastructure(time.wall).await {
            self.record_worker_changes(time.wall).await;
        }
        self.record_exits(time.wall).await;
        self.record_launch_failures(time.wall).await;
        self.reconciler
            .reconcile(&mut self.passes, &self.hosted, time, &mut self.plan);
        self.pending_terminations.retain(|pending| {
            let Action::Terminate { instance, .. } = &pending.action else {
                return false;
            };
            if let Some(planned) = self
                .plan
                .steps
                .iter_mut()
                .find(|step| termination_matches(step, pending.fleet, instance))
            {
                planned.clone_from(pending);
                return true;
            }
            let live = self.passes[pending.fleet]
                .supply
                .instances
                .iter()
                .any(|member| {
                    member.id == *instance && member.lifecycle != InstanceLifecycle::Terminating
                });
            if !live {
                self.plan.steps.insert(0, pending.clone());
            }
            !live
        });
        Ok(time)
    }

    /// Publishes finished pass metadata and outlook, or the skip reason.
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
                    },
                    &self.passes,
                    self.fleets.catalog(),
                    &self.infrastructure,
                );
            }
            PassOutcome::Skipped(reason) => {
                publisher.publish(
                    wall,
                    PassStatus::Skipped(reason.clone()),
                    &self.passes,
                    self.fleets.catalog(),
                    &self.infrastructure,
                );
            }
        }
    }

    /// Runs passes and, when enabled, independent dashboard observations for this term.
    pub(crate) async fn run(mut self) -> Infallible {
        let publisher = self.publisher.clone();
        let fleets = self.fleets;
        let provisioner = self.provisioner;
        let scheduler = self.scheduler;
        let clock = self.clock;
        let poll_interval = self.poll_interval;
        if let Some(mut publisher) = publisher {
            let observations = async move {
                let configs: Vec<_> = fleets.iter().collect();
                let mut passes: Vec<_> = configs
                    .iter()
                    .map(|config| FleetPass::new(config.policy.clone()))
                    .collect();
                let mut incoming_supplies: Vec<_> =
                    configs.iter().map(|_| FleetSupply::default()).collect();
                let mut supplies = vec![false; configs.len()];
                let mut fresh_supplies = vec![false; configs.len()];
                let mut hosted = HostedSnapshot::default();
                let mut incoming = SchedulerSnapshot::default();
                let mut ticks = Interval::new(clock, clock.monotonic(), poll_interval);
                loop {
                    ticks.tick().await;
                    let deadline = clock.monotonic() + poll_interval.get();
                    let read_scheduler =
                        clock.timeout_at(deadline, scheduler.snapshot(&mut incoming));
                    let read_supplies = async {
                        for ((((config, pass), scratch), known), fresh) in configs
                            .iter()
                            .zip(&mut passes)
                            .zip(&mut incoming_supplies)
                            .zip(&mut supplies)
                            .zip(&mut fresh_supplies)
                        {
                            *fresh = matches!(
                                clock
                                    .timeout_at(
                                        deadline,
                                        provisioner.supply(config.fleet(), &mut *scratch),
                                    )
                                    .await,
                                Ok(Ok(()))
                            );
                            if *fresh {
                                *known = true;
                                std::mem::swap(&mut pass.supply, scratch);
                            }
                        }
                    };
                    let (snapshot, ()) = tokio::join!(read_scheduler, read_supplies);
                    let known = matches!(snapshot, Ok(Ok(())));
                    if known {
                        hosted.replace(&mut incoming);
                    }
                    publisher.publish(
                        clock.wall(),
                        PassStatus::Observed {
                            hosted: known.then_some(&hosted),
                            supplies: &supplies,
                            fresh_supplies: &fresh_supplies,
                        },
                        &passes,
                        fleets.catalog(),
                        &[],
                    );
                }
            };
            tokio::select! {
                biased;
                never = observations => never,
                never = self.run_passes() => never,
            }
        } else {
            self.run_passes().await
        }
    }

    /// A pass that overruns the interval delays the next one rather than running back to back.
    async fn run_passes(&mut self) -> Infallible {
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

    /// Persists provider observations before acknowledging delivery, then reloads canonical live status.
    async fn record_infrastructure(&mut self, now: Timestamp) -> bool {
        let mut complete = true;
        for (config, pass) in self.configs.iter().zip(&mut self.passes) {
            for observed in pass.supply.events.drain(..) {
                if !self.fence.holds() {
                    return false;
                }
                let record = FleetInfrastructureEvent {
                    fleet: config.policy.name.clone(),
                    event: observed,
                };
                let expires = record.event.at + self.infrastructure_window;
                if expires > now {
                    let key = infrastructure_key(&record.event.id);
                    let body = match serde_json::to_vec(&record) {
                        Ok(body) => body,
                        Err(error) => {
                            tracing::error!(%error, "cannot encode infrastructure observation");
                            complete = false;
                            continue;
                        }
                    };
                    let version = match RecordVersion::fresh() {
                        Ok(version) => version,
                        Err(error) => {
                            tracing::error!(%error, "cannot create infrastructure record version");
                            complete = false;
                            continue;
                        }
                    };
                    match self
                        .repository
                        .insert(&[(key, &body)], version, now, expires)
                        .await
                    {
                        Ok(Insert::Inserted) => {}
                        Ok(Insert::Exists) => match self.repository.get(&key, now).await {
                            Ok(Some(stored)) => match serde_json::from_slice::<
                                FleetInfrastructureEvent,
                            >(&stored.body)
                            {
                                Ok(_) => {}
                                Err(error) => {
                                    tracing::error!(%error, "invalid canonical infrastructure observation");
                                    complete = false;
                                    continue;
                                }
                            },
                            Ok(None) => {
                                complete = false;
                                continue;
                            }
                            Err(error) => {
                                tracing::error!(%error, "cannot read canonical infrastructure observation");
                                complete = false;
                                continue;
                            }
                        },
                        Err(error) => {
                            tracing::error!(%error, "cannot store infrastructure observation");
                            complete = false;
                            continue;
                        }
                    }
                }
                if let Err(error) = self
                    .fence
                    .run(async |held| {
                        self.provisioner
                            .acknowledge_event(held, config.fleet(), &record.event.id)
                            .await
                    })
                    .await
                {
                    tracing::warn!(%error, "cannot acknowledge infrastructure observation");
                    complete = false;
                }
            }
        }
        let loaded = self.load_infrastructure(now).await;
        complete && loaded
    }

    async fn load_infrastructure(&mut self, now: Timestamp) -> bool {
        self.infrastructure.clear();
        let mut complete = true;
        self.infrastructure_pending.clear();
        let mut after = None;
        loop {
            let more = match self
                .repository
                .records(
                    RecordKind::InfrastructureEvent,
                    after,
                    self.infrastructure_limit,
                    now,
                    &mut self.infrastructure_page,
                )
                .await
            {
                Ok(more) => more,
                Err(error) => {
                    tracing::error!(%error, "cannot read infrastructure observations");
                    return false;
                }
            };
            for (_, record) in &self.infrastructure_page {
                match serde_json::from_slice(&record.body) {
                    Ok(record) => self.infrastructure_pending.push(record),
                    Err(error) => {
                        tracing::warn!(%error, "invalid stored infrastructure observation");
                        complete = false;
                    }
                }
            }
            if !more {
                break;
            }
            let Some((id, _)) = self.infrastructure_page.last() else {
                tracing::error!("infrastructure record pagination returned an empty page");
                return false;
            };
            after = Some(*id);
        }
        std::mem::swap(&mut self.infrastructure, &mut self.infrastructure_pending);
        complete
    }

    /// Records workers registered or deregistered since the previous pass, then remembers the
    /// workers of this snapshot. Every event is stamped with the wall clock of this pass.
    async fn record_worker_changes(&mut self, at: Timestamp) {
        self.roster_pending.clone_from(&self.roster);
        self.roster_pending.diff(
            &self.hosted.snapshot().workers,
            self.fleets,
            at,
            &mut self.worker_changes,
        );
        self.worker_changes.retain(|event| {
            let ScalingEventKind::WorkerRegistered { worker } = &event.kind else {
                return true;
            };
            !self.infrastructure.iter().any(|record| {
                record.fleet == event.fleet
                    && matches!(&record.event.kind,
                    InfrastructureEventKind::ReplacementLaunched { instance, .. }
                    if worker.instance_name() == Some(instance.as_str()))
            })
        });
        let mut recorded = true;
        for event in self.worker_changes.drain(..) {
            recorded &=
                Self::record_event(self.repository, self.publisher.as_ref(), &self.fence, event)
                    .await
                    .is_ok();
        }
        if recorded && self.fence.holds() {
            std::mem::swap(&mut self.roster, &mut self.roster_pending);
        }
    }

    /// Records and acknowledges early exits, stopping at the first failure.
    async fn record_exits(&mut self, at: Timestamp) {
        for (fleet, pass) in self.passes.iter().enumerate() {
            for exit in &pass.supply.exits {
                self.pending_terminations
                    .retain(|step| !termination_matches(step, fleet, &exit.instance));
            }
        }
        for (config, pass) in self.configs.iter().zip(self.passes.iter_mut()) {
            for exit in pass.supply.exits.drain(..) {
                let instance = exit.instance;
                let event = ScalingAuditEvent {
                    at,
                    fleet: config.policy.name.clone(),
                    kind: ScalingEventKind::ExitedEarly {
                        instance: instance.clone(),
                        failure: exit.failure,
                    },
                    cause: None,
                };
                if Self::record_event(self.repository, self.publisher.as_ref(), &self.fence, event)
                    .await
                    .is_err()
                {
                    return;
                }
                if let Err(error) = self
                    .fence
                    .run(async |held| {
                        self.provisioner
                            .acknowledge_exit(held, config.fleet(), &instance)
                            .await
                    })
                    .await
                {
                    tracing::warn!(
                        fleet = %config.policy.name,
                        %instance,
                        %error,
                        "failed to acknowledge early exit"
                    );
                    return;
                }
            }
        }
    }

    /// Records new launch failure causes until the fleet's request is met.
    /// Expiring provider capacity observations do not replace this durable audit.
    async fn record_launch_failures(&mut self, at: Timestamp) {
        for ((config, pass), last_reason) in self
            .configs
            .iter()
            .zip(&self.passes)
            .zip(&mut self.launch_failures)
        {
            let supply = &pass.supply;
            let live = supply
                .instances
                .iter()
                .filter(|instance| instance.lifecycle != InstanceLifecycle::Terminating)
                .fold(0_u32, |count, _| count.saturating_add(1));
            if supply.desired <= live {
                *last_reason = None;
            } else if let Some(failure) = &supply.launch_failure
                && last_reason.as_deref() != Some(failure.reason.as_str())
            {
                let event = ScalingAuditEvent {
                    at,
                    fleet: config.policy.name.clone(),
                    kind: ScalingEventKind::LaunchFailed {
                        from: live,
                        to: supply.desired,
                        reason: failure.reason.clone(),
                    },
                    cause: None,
                };
                if Self::record_event(self.repository, self.publisher.as_ref(), &self.fence, event)
                    .await
                    .is_ok()
                {
                    *last_reason = Some(failure.reason.clone());
                }
            }
        }
    }

    /// Converges then reads each fleet's supply, or returns why the pass must be skipped.
    async fn refresh_supplies(&mut self) -> Result<(), SkipReason> {
        if !self.configured_fleets {
            self.provisioner.reset_configured_fleets();
            for config in &self.configs {
                if let Err(error) = self.provisioner.configure_fleet(config.fleet()) {
                    tracing::warn!(fleet = %config.policy.name, %error, "provisioner fleet configuration failed");
                    return Err(SkipReason::ProvisionerUnavailable);
                }
            }
            self.configured_fleets = true;
        }
        for (config, pass) in self.configs.iter().zip(self.passes.iter_mut()) {
            let provisioner = self.provisioner;
            let fleet = config.fleet();
            match self
                .fence
                .run(async |held| provisioner.converge(held, fleet).await)
                .await
            {
                Ok(()) => {}
                Err(error @ RunError::Lapsed(_)) => {
                    tracing::debug!(fleet = %config.policy.name, %error, "leadership lapsed; skipping pass");
                    return Err(SkipReason::LeadershipLapsed);
                }
                Err(error) => {
                    tracing::warn!(fleet = %config.policy.name, %error, "provisioner converge failed; skipping pass");
                    return Err(SkipReason::ProvisionerUnavailable);
                }
            }
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
                return Err(SkipReason::ProvisionerUnavailable);
            }
        }
        Ok(())
    }

    /// Applies the planned steps in order and reports how many succeeded.
    async fn apply_plan(&mut self) -> PassOutcome {
        let mut applied = 0;
        let mut failed = 0;
        for (index, step) in self.plan.steps.iter().enumerate() {
            match Self::apply(
                self.provisioner,
                self.scheduler,
                &self.configs,
                step,
                &self.fence,
            )
            .await
            {
                Ok(()) => {
                    if let Action::Terminate { instance, .. } = &step.action {
                        self.pending_terminations
                            .retain(|pending| !termination_matches(pending, step.fleet, instance));
                    }
                    applied += 1;
                    if let Some(event) =
                        step.audit_event(self.plan.at, &self.passes[step.fleet].policy.name)
                    {
                        let _ = Self::record_event(
                            self.repository,
                            self.publisher.as_ref(),
                            &self.fence,
                            event,
                        )
                        .await;
                    }
                }
                Err(PortError::NotFound(Subject::Worker(worker))) => {
                    tracing::debug!(worker = ?worker, "worker not found; skipping drain step");
                }
                Err(PortError::Unavailable(error))
                    if matches!(
                        &step.action,
                        Action::StartDrain { .. }
                            | Action::ReinforceDrain(_)
                            | Action::CancelDrain(_, _)
                    ) =>
                {
                    tracing::warn!(?step, %error, "scheduler unavailable; skipping remaining steps");
                    for remaining in &self.plan.steps[index + 1..] {
                        if let Action::Terminate { instance, .. } = &remaining.action {
                            self.reconciler.forget_order(remaining.fleet, instance);
                        }
                    }
                    return PassOutcome::Skipped(SkipReason::SchedulerUnavailable);
                }
                Err(error) => {
                    if let Action::Terminate { instance, .. } = &step.action {
                        if !self
                            .pending_terminations
                            .iter()
                            .any(|pending| termination_matches(pending, step.fleet, instance))
                        {
                            self.pending_terminations.push(step.clone());
                        }
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
        fence: &Fence,
    ) -> Result<(), PortError> {
        if matches!(
            &step.action,
            Action::StartDrain { .. } | Action::ReinforceDrain(_) | Action::CancelDrain(_, _)
        ) && !fence.holds()
        {
            return Err(PortError::from(LeaseLapsed));
        }
        match &step.action {
            Action::ScaleOut { to, .. } | Action::Withdraw { to, .. } => {
                let fleet = configs[step.fleet].fleet();
                fence
                    .run(async |held| provisioner.set_desired_capacity(held, fleet, *to).await)
                    .await
                    .map_err(PortError::from)
            }
            Action::StartDrain { worker, .. } | Action::ReinforceDrain(worker) => {
                Self::set_draining(scheduler, configs, step.fleet, worker, true).await
            }
            Action::CancelDrain(worker, _) => {
                Self::set_draining(scheduler, configs, step.fleet, worker, false).await
            }
            Action::Terminate { instance, .. } => {
                let fleet = configs[step.fleet].fleet();
                fence
                    .run(async |held| provisioner.terminate(held, fleet, instance).await)
                    .await
                    .map_err(PortError::from)
            }
        }
    }

    async fn set_draining(
        scheduler: &S,
        configs: &[FleetConfig<'_, P>],
        fleet: usize,
        worker: &WorkerId,
        draining: bool,
    ) -> Result<(), PortError> {
        let name = worker
            .instance_name()
            .ok_or_else(|| PortError::NotFound(Subject::Worker(worker.clone())))?;
        let instance = InstanceId::try_new(name).map_err(PortError::backend)?;
        scheduler
            .set_draining(&configs[fleet].policy.name, &instance, draining)
            .await
    }

    /// Records one audit event and adds it to the published history. A recording failure is
    /// logged, publishes nothing, and returns the event to its caller.
    async fn record_event(
        repository: &R,
        publisher: Option<&StatePublisher>,
        fence: &Fence,
        event: ScalingAuditEvent,
    ) -> Result<(), ScalingAuditEvent> {
        if !fence.holds() {
            return Err(event);
        }
        match repository.record_scaling_event(&event).await {
            Ok(seq) => {
                if let Some(publisher) = publisher {
                    publisher.record_event(RecordedScalingEvent { seq, event });
                }
                Ok(())
            }
            Err(error) => {
                tracing::error!(%error, "failed to record scaling event");
                Err(event)
            }
        }
    }
}

fn termination_matches(step: &PlanStep, fleet: usize, instance: &InstanceId) -> bool {
    step.fleet == fleet
        && matches!(&step.action, Action::Terminate { instance: id, .. } if id == instance)
}

fn infrastructure_key(id: &str) -> RecordKey {
    let digest = ring::digest::digest(&ring::digest::SHA256, id.as_bytes());
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(digest.as_ref());
    RecordKey {
        kind: RecordKind::InfrastructureEvent,
        id: RecordId::new(bytes),
    }
}

/// Workers seen in the previous completed pass, sorted by worker id.
#[derive(Debug, Default, PartialEq, Eq)]
struct WorkerRoster {
    connected: Vec<(WorkerId, FleetName)>,
}

impl Clone for WorkerRoster {
    fn clone(&self) -> Self {
        Self {
            connected: self.connected.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        if self != source {
            self.connected.clone_from(&source.connected);
        }
    }
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
    use futures::poll;
    use gang_reconciler::{NeedCause, ScaleDecision};
    use gang_spi::{FleetInstance, FleetSupply, InstanceCordon, SchedulerSnapshot, WorkerState};
    use std::num::NonZeroU32;

    type TestControl<'a> = ControlLoop<
        'a,
        gang_testkit::TestPlugins,
        gang_testkit::FakeProvisioner,
        gang_testkit::FakeScheduler,
        gang_testkit::FakeRepository,
        Arc<gang_clock::TestClock>,
    >;

    fn control<'a>(
        fleets: &'a Fleets<gang_testkit::TestPlugins>,
        provisioner: &'a gang_testkit::FakeProvisioner,
        scheduler: &'a gang_testkit::FakeScheduler,
        repository: &'a gang_testkit::FakeRepository,
        clock: &'a Arc<gang_clock::TestClock>,
    ) -> TestControl<'a> {
        let scaling = Arc::new(ScalingConfig::default());
        let profiles = SourceProfiles::new(
            &scaling,
            gang_config::ActionsConfig::default().history_window,
        );
        ControlLoop::new(
            fleets,
            ControlSettings::new(NonZeroDuration::from_secs(1), &DashboardConfig::default()),
            scaling,
            profiles,
            gang_types::nonzero_u32(2),
            &crate::control::ControlPorts {
                provisioner,
                scheduler,
                repository,
                clock,
            },
            gang_testkit::fence::valid_fence(),
        )
    }

    #[tokio::test]
    async fn drain_steps_apply_to_the_physical_fleet_and_instance() -> gang_testkit::TestResult {
        use gang_reconciler::{DrainReason, NeedCause};
        let fleets = crate::test_support::fleets_of(vec![("first", 4), ("second", 4)]);
        let provisioner = gang_testkit::FakeProvisioner::default();
        let scheduler = gang_testkit::FakeScheduler::new()?;
        let repository = gang_testkit::FakeRepository::default();
        let clock = crate::test_support::clock_at(1_000_000);
        let instance = InstanceId::try_new("local-1")?;
        let worker = gang_testkit::worker_on(&instance)?;
        let mut control = control(&fleets, &provisioner, &scheduler, &repository, &clock);
        control.plan.steps = vec![
            PlanStep {
                fleet: 1,
                action: Action::StartDrain {
                    worker: worker.clone(),
                    reason: DrainReason::IdleTimeout,
                    summary: None,
                },
            },
            PlanStep {
                fleet: 1,
                action: Action::ReinforceDrain(worker.clone()),
            },
            PlanStep {
                fleet: 1,
                action: Action::CancelDrain(worker, NeedCause::Demand),
            },
        ];
        assert_eq!(
            control.apply_plan().await,
            PassOutcome::Completed {
                applied: 3,
                failed: 0
            }
        );
        let fleet = FleetName::try_new("second")?;
        assert_eq!(
            scheduler.draining_calls(),
            vec![
                (fleet.clone(), instance.clone(), true),
                (fleet.clone(), instance.clone(), true),
                (fleet, instance, false),
            ]
        );
        let events = repository.scaling_events();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0].kind,
            ScalingEventKind::DrainStarted { .. }
        ));
        assert!(matches!(
            events[1].kind,
            ScalingEventKind::DrainCancelled { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_drain_stops_plan_and_retries_unexecuted_termination()
    -> gang_testkit::TestResult {
        let fleets = crate::test_support::fleets();
        let provisioner = gang_testkit::FakeProvisioner::default();
        let clock = crate::test_support::clock_at(1_000_000);
        let scheduler = gang_testkit::FakeScheduler::with_control_acquire_timeout(
            clock.clone(),
            NonZeroDuration::from_secs(1),
        )?;
        let repository = gang_testkit::FakeRepository::default();
        let fleet = FleetName::try_new("example-fleet")?;
        let instance = InstanceId::try_new("local-1")?;
        let worker = gang_testkit::worker_on(&instance)?;
        provisioner.set_supply(
            &fleet,
            FleetSupply {
                desired: 1,
                instances: vec![FleetInstance {
                    id: instance.clone(),
                    lifecycle: InstanceLifecycle::InService,
                    launched_at: clock.wall(),
                    release_first: false,
                }],
                ..FleetSupply::default()
            },
        );
        scheduler.set_snapshot(SchedulerSnapshot {
            cordons: vec![InstanceCordon {
                fleet: 0,
                instance: Arc::new(instance.clone()),
            }],
            workers: vec![WorkerState {
                id: worker.clone(),
                fleet,
                environment: Some("host:test".parse()?),
                running_actions: 0,
                max_inflight_actions: NonZeroU32::new(4),
                capacity: None,
                reserved: None,
                draining: true,
                paused: false,
                last_seen: clock.wall(),
            }],
            ..SchedulerSnapshot::default()
        });
        let mut control = control(&fleets, &provisioner, &scheduler, &repository, &clock);
        assert!(control.plan_pass().await.is_ok());
        assert!(
            control
                .plan
                .steps
                .iter()
                .any(|step| matches!(step.action, Action::Terminate { .. }))
        );
        control.plan.steps.insert(
            0,
            PlanStep {
                fleet: 0,
                action: Action::ReinforceDrain(worker),
            },
        );
        control.plan.steps.insert(
            0,
            PlanStep {
                fleet: 0,
                action: Action::ScaleOut {
                    from: 1,
                    to: 2,
                    need: NeedCause::Demand,
                    summary: ScaleDecision::default(),
                },
            },
        );
        let guard = scheduler.hold_control_acquisition().await?;
        {
            let apply = control.apply_plan();
            futures::pin_mut!(apply);
            assert!(poll!(apply.as_mut()).is_pending());
            clock.advance(Duration::from_secs(1));
            assert_eq!(
                apply.await,
                PassOutcome::Skipped(SkipReason::SchedulerUnavailable)
            );
        }
        assert_eq!(provisioner.desired_requests().len(), 1);
        assert!(provisioner.terminations().is_empty());
        assert!(scheduler.draining_calls().is_empty());
        assert!(
            repository
                .scaling_events()
                .iter()
                .any(|event| matches!(event.kind, ScalingEventKind::ScaledOut { .. }))
        );
        drop(guard);
        assert!(matches!(
            control.pass().await,
            PassOutcome::Completed { failed: 0, .. }
        ));
        assert_eq!(provisioner.terminations().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reinforcing_drain_passes_write_no_audit_rows() -> gang_testkit::TestResult {
        let fleets = crate::test_support::fleets();
        let provisioner = gang_testkit::FakeProvisioner::default();
        let scheduler = gang_testkit::FakeScheduler::new()?;
        let repository = gang_testkit::FakeRepository::default();
        let clock = crate::test_support::clock_at(1_000_000);
        let instance = InstanceId::try_new("local-1")?;
        let worker = gang_testkit::worker_on(&instance)?;
        let mut control = control(&fleets, &provisioner, &scheduler, &repository, &clock);
        control.plan.steps.push(PlanStep {
            fleet: 0,
            action: Action::ReinforceDrain(worker),
        });
        for _ in 0..8 {
            assert_eq!(
                control.apply_plan().await,
                PassOutcome::Completed {
                    applied: 1,
                    failed: 0
                }
            );
        }
        assert_eq!(scheduler.draining_calls().len(), 8);
        assert!(repository.scaling_events().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn applied_purchase_and_utilization_cordon_publish_detail() -> gang_testkit::TestResult {
        use gang_reconciler::{CordonDecision, DrainReason, NeedCause, ScaleDecision};
        let fleets = crate::test_support::fleets();
        let provisioner = gang_testkit::FakeProvisioner::default();
        let scheduler = gang_testkit::FakeScheduler::new()?;
        let repository = gang_testkit::FakeRepository::default();
        let clock = crate::test_support::clock_at(1_000_000);
        let instance = InstanceId::try_new("local-1")?;
        let mut control = control(&fleets, &provisioner, &scheduler, &repository, &clock);
        control.plan.steps = vec![
            PlanStep {
                fleet: 0,
                action: Action::ScaleOut {
                    from: 0,
                    to: 1,
                    need: NeedCause::Demand,
                    summary: ScaleDecision {
                        needed: 1,
                        reliable: 0,
                        ..ScaleDecision::default()
                    },
                },
            },
            PlanStep {
                fleet: 0,
                action: Action::StartDrain {
                    worker: gang_testkit::worker_on(&instance)?,
                    reason: DrainReason::Utilization,
                    summary: Some(CordonDecision {
                        serving_instances: 2,
                        waterline_percent: 70,
                        dwell: Duration::from_mins(5),
                        ..CordonDecision::default()
                    }),
                },
            },
        ];
        assert_eq!(
            control.apply_plan().await,
            PassOutcome::Completed {
                applied: 2,
                failed: 0
            }
        );
        let events = repository.scaling_events();
        assert_eq!(events.len(), 2);
        let detail = match &events[0].kind {
            ScalingEventKind::ScaledOut {
                detail: Some(detail),
                ..
            } => detail,
            kind => panic!("missing purchase detail: {kind:?}"),
        };
        assert!(detail.contains("needed=1"));
        assert!(detail.contains("reliable=0"));
        let purchase = crate::events::wire_event(RecordedScalingEvent {
            seq: gang_spi::ScalingEventSeq::new(1),
            event: events[0].clone(),
        });
        assert!(
            matches!(&purchase.change, gang_wire::EventChange::ScaledOut {
            detail: Some(value), ..
        } if value == detail)
        );
        assert_eq!(events[1].cause, Some(gang_types::ScalingCause::Utilization));
        let detail = match &events[1].kind {
            ScalingEventKind::DrainStarted {
                detail: Some(detail),
                ..
            } => detail,
            kind => panic!("missing cordon detail: {kind:?}"),
        };
        assert!(detail.contains("serving_instances=2"));
        let wire = crate::events::wire_event(RecordedScalingEvent {
            seq: gang_spi::ScalingEventSeq::new(2),
            event: events[1].clone(),
        });
        assert_eq!(
            wire.cause,
            Some(gang_wire::Cause::from(
                gang_types::ScalingCause::Utilization
            ))
        );
        assert!(
            matches!(&wire.change, gang_wire::EventChange::DrainStarted { detail: Some(value), .. } if value == detail)
        );
        Ok(())
    }

    fn retention_fleets() -> gang_testkit::TestResult<Fleets<gang_testkit::TestPlugins>> {
        let base = crate::test_support::fleets_of(vec![("first", 4), ("second", 4)]);
        Ok(Fleets::<gang_testkit::TestPlugins>::try_new(
            base.policies()
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, mut policy)| {
                    policy.boot_grace = Duration::from_secs(if index == 0 { 30 } else { 90 });
                    gang_config::FleetMember {
                        policy,
                        sandbox: gang_testkit::SandboxKey::Noop,
                        options: gang_testkit::Opaque::default(),
                    }
                })
                .collect(),
        )?)
    }

    #[tokio::test]
    async fn retention_uses_only_complete_censuses_and_includes_every_lifecycle()
    -> gang_testkit::TestResult {
        let fleets = retention_fleets()?;
        let provisioner = gang_testkit::FakeProvisioner::default();
        let scheduler = gang_testkit::FakeScheduler::new()?;
        let repository = gang_testkit::FakeRepository::default();
        let clock = crate::test_support::clock_at(1_000_000);
        let first = FleetName::try_new("first")?;
        let second = FleetName::try_new("second")?;
        let ids = [
            InstanceId::try_new("local-1")?,
            InstanceId::try_new("local-2")?,
            InstanceId::try_new("local-3")?,
        ];
        let instances = [
            InstanceLifecycle::Pending,
            InstanceLifecycle::InService,
            InstanceLifecycle::Terminating,
        ]
        .into_iter()
        .zip(&ids)
        .map(|(lifecycle, id)| gang_spi::FleetInstance {
            id: id.clone(),
            lifecycle,
            launched_at: clock.wall(),
            release_first: false,
        })
        .collect();
        provisioner.set_supply(
            &first,
            gang_spi::FleetSupply {
                instances,
                desired: 3,
                ..Default::default()
            },
        );
        provisioner.set_supply(
            &second,
            gang_spi::FleetSupply {
                instances: vec![gang_spi::FleetInstance {
                    id: ids[0].clone(),
                    lifecycle: InstanceLifecycle::Pending,
                    launched_at: clock.wall(),
                    release_first: false,
                }],
                desired: 1,
                ..Default::default()
            },
        );
        let mut control = control(&fleets, &provisioner, &scheduler, &repository, &clock);
        scheduler.fail_snapshot(true);
        assert_eq!(
            control.pass().await,
            PassOutcome::Skipped(SkipReason::SchedulerUnavailable)
        );
        scheduler.fail_snapshot(false);
        provisioner.fail_supply(true);
        assert_eq!(
            control.pass().await,
            PassOutcome::Skipped(SkipReason::ProvisionerUnavailable)
        );
        assert!(scheduler.retain_drains_calls().is_empty());
        provisioner.fail_supply(false);
        let converges = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        provisioner.on_converge(move || {
            if converges.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                Err(PortError::unavailable("second fleet census is incomplete"))
            } else {
                Ok(())
            }
        });
        assert_eq!(
            control.pass().await,
            PassOutcome::Skipped(SkipReason::ProvisionerUnavailable)
        );
        assert!(
            scheduler.retain_drains_calls().is_empty(),
            "a partial provider census must not sweep intents"
        );
        let now = clock.monotonic();
        assert!(matches!(
            control.pass().await,
            PassOutcome::Completed { .. }
        ));
        assert_eq!(
            scheduler.retain_drains_calls(),
            vec![(
                now,
                Duration::from_secs(90),
                vec![
                    (first.clone(), ids[0].clone()),
                    (first.clone(), ids[1].clone()),
                    (first, ids[2].clone()),
                    (second, ids[0].clone())
                ],
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn skipped_pass_restarts_utilization_dwell() -> gang_testkit::TestResult {
        let base = crate::test_support::fleets();
        let mut policy = base.policies()[0].clone();
        policy.idle_timeout = Duration::from_hours(1);
        let fleets =
            Fleets::<gang_testkit::TestPlugins>::try_new(vec![gang_config::FleetMember {
                policy,
                sandbox: gang_testkit::SandboxKey::Noop,
                options: gang_testkit::Opaque::default(),
            }])?;
        let provisioner = gang_testkit::FakeProvisioner::default();
        let scheduler = gang_testkit::FakeScheduler::new()?;
        let repository = gang_testkit::FakeRepository::default();
        let clock = crate::test_support::clock_at(1_000_000);
        let instance = InstanceId::try_new("local-1")?;
        let fleet = fleets.policies()[0].name.clone();
        provisioner.set_supply(
            &fleet,
            gang_spi::FleetSupply {
                instances: vec![gang_spi::FleetInstance {
                    id: instance.clone(),
                    lifecycle: InstanceLifecycle::InService,
                    launched_at: clock.wall(),
                    release_first: false,
                }],
                desired: 1,
                ..Default::default()
            },
        );
        scheduler.set_snapshot(SchedulerSnapshot {
            workers: vec![WorkerState {
                id: gang_testkit::worker_on(&instance)?,
                fleet,
                environment: Some("host:test".parse()?),
                running_actions: 0,
                max_inflight_actions: NonZeroU32::new(4),
                capacity: None,
                reserved: None,
                draining: false,
                paused: false,
                last_seen: clock.wall(),
            }],
            ..Default::default()
        });
        let mut control = control(&fleets, &provisioner, &scheduler, &repository, &clock);
        control.pass().await;
        clock.advance(gang_types::health::CLIENT_REROUTE_BOUND);
        control.pass().await;
        clock.advance(Duration::from_secs(299));
        scheduler.fail_snapshot(true);
        assert_eq!(
            control.pass().await,
            PassOutcome::Skipped(SkipReason::SchedulerUnavailable)
        );
        scheduler.fail_snapshot(false);
        clock.advance(Duration::from_secs(1));
        control.pass().await;
        assert!(
            scheduler.draining_calls().is_empty(),
            "the skipped interval cannot satisfy dwell"
        );
        clock.advance(Duration::from_secs(299));
        control.pass().await;
        assert!(scheduler.draining_calls().is_empty());
        clock.advance(Duration::from_secs(1));
        control.pass().await;
        assert_eq!(scheduler.draining_calls().len(), 1);
        assert!(
            repository
                .scaling_events()
                .iter()
                .any(|event| event.cause == Some(gang_types::ScalingCause::Utilization))
        );
        Ok(())
    }

    fn at() -> Timestamp {
        Timestamp::from_unix_millis(5_000)
    }

    fn worker(id: &str, fleet: &str) -> WorkerState {
        WorkerState {
            id: WorkerId::try_new(id).unwrap(),
            fleet: FleetName::try_new(fleet).unwrap(),
            environment: Some("host:test".parse().unwrap()),
            running_actions: 0,
            max_inflight_actions: NonZeroU32::new(1),
            capacity: None,
            reserved: None,
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

    #[test]
    fn staging_unchanged_workers_reuses_roster_storage() {
        const PASSES: usize = 10;
        let workers = [
            worker("local-1", "arm"),
            worker("local-2", "x86"),
            worker("local-3", "x86"),
        ];
        let fleets = crate::test_support::fleets_of(vec![("x86", 4), ("arm", 4)]);
        let now = at();
        let mut committed = seen(&[("local-1", "arm"), ("local-2", "x86"), ("local-3", "x86")]);
        let mut pending = WorkerRoster::default();
        let mut events = Vec::with_capacity(workers.len());
        pending.clone_from(&committed);
        pending.diff(&workers, &fleets, now, &mut events);
        std::mem::swap(&mut committed, &mut pending);
        assert!(events.is_empty());

        let counts = allocation_counter::measure(|| {
            for _ in 0..PASSES {
                pending.clone_from(&committed);
                pending.diff(&workers, &fleets, now, &mut events);
                std::mem::swap(&mut committed, &mut pending);
            }
        });
        assert_eq!(counts.count_total, 0, "unchanged roster staging allocated");
        assert!(events.is_empty());
    }
}
