//! Short-lived protection of the original engine's physical workers, never action ownership.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_reconciler::FleetPass;
use gang_spi::{
    ControllerDrain, ControllerId, ExpiringRecordPort, Insert, MAX_RECORD_BODY_BYTES, PortError,
    Record, RecordId, RecordKey, RecordKind, RecordVersion, Replace, SchedulerSnapshot,
};
use gang_types::{FleetName, InstanceId};
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Worker {
    fleet: FleetName,
    instance: InstanceId,
    deadline: Timestamp,
}

struct Chunk {
    key: RecordKey,
    version: RecordVersion,
    workers: Vec<Worker>,
    body: Vec<u8>,
}

fn key(holder: &ControllerId, index: usize) -> RecordKey {
    let mut digest = Context::new(&SHA256);
    digest.update(holder.as_str().as_bytes());
    digest.update(&index.to_be_bytes());
    let mut id = [0; 32];
    id.copy_from_slice(digest.finish().as_ref());
    RecordKey {
        kind: RecordKind::ControllerDrain,
        id: RecordId::new(id),
    }
}

fn encode(workers: &[Worker], into: &mut Vec<u8>) -> Result<(), PortError> {
    into.clear();
    serde_json::to_writer(into, workers).map_err(PortError::backend)
}

/// Only these frozen chunks may be renewed after authority release.
pub(crate) struct Outgoing {
    chunks: Vec<Chunk>,
}

impl Outgoing {
    pub(crate) fn new(holder: &ControllerId, drain: &ControllerDrain) -> Result<Self, PortError> {
        let mut chunks = Vec::new();
        let mut workers = Vec::new();
        let mut body = vec![b'['];
        let mut encoded = Vec::new();
        for entry in &drain.workers {
            let worker = Worker {
                fleet: entry.fleet.clone(),
                instance: entry.instance.clone(),
                deadline: entry.deadline,
            };
            encoded.clear();
            serde_json::to_writer(&mut encoded, &worker).map_err(PortError::backend)?;
            if encoded.len() + 2 > MAX_RECORD_BODY_BYTES {
                return Err(PortError::config(
                    "one physical worker exceeds the record body limit",
                ));
            }
            if body.len() + encoded.len() + usize::from(!workers.is_empty()) + 1
                > MAX_RECORD_BODY_BYTES
            {
                body.push(b']');
                chunks.push(Chunk {
                    key: key(holder, chunks.len()),
                    version: RecordVersion::fresh().map_err(PortError::backend)?,
                    workers: std::mem::take(&mut workers),
                    body: std::mem::replace(&mut body, vec![b'[']),
                });
            }
            if !workers.is_empty() {
                body.push(b',');
            }
            body.extend_from_slice(&encoded);
            workers.push(worker);
        }
        if !workers.is_empty() {
            body.push(b']');
            chunks.push(Chunk {
                key: key(holder, chunks.len()),
                version: RecordVersion::fresh().map_err(PortError::backend)?,
                workers,
                body,
            });
        }
        Ok(Self { chunks })
    }

    /// Idempotent before release, including an insertion whose response was lost.
    pub(crate) async fn install(
        &mut self,
        repository: &impl ExpiringRecordPort,
        now: Timestamp,
        ttl: Duration,
    ) -> Result<(), PortError> {
        for chunk in &mut self.chunks {
            let expires = chunk
                .workers
                .iter()
                .map(|worker| worker.deadline)
                .max()
                .unwrap_or(now)
                .min(now + ttl);
            if expires <= now {
                continue;
            }
            match repository
                .insert(&[(chunk.key, &chunk.body)], chunk.version, now, expires)
                .await?
            {
                Insert::Inserted => {}
                Insert::Exists => {
                    let current = repository.get(&chunk.key, now).await?.ok_or_else(|| {
                        PortError::unavailable(
                            "controller drain record expired during installation",
                        )
                    })?;
                    if current.body != chunk.body {
                        return Err(PortError::backend(
                            "controller drain record has different frozen workers",
                        ));
                    }
                    chunk.version = match repository
                        .replace(&chunk.key, current.version, &chunk.body, now, expires)
                        .await?
                    {
                        Replace::Replaced(version) => version,
                        Replace::Stale => {
                            return Err(PortError::unavailable(
                                "controller drain installation changed",
                            ));
                        }
                    };
                }
            }
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self, into: &mut ControllerDrain) {
        into.workers.clear();
        into.complete = false;
        for chunk in &self.chunks {
            into.workers
                .extend(chunk.workers.iter().map(|worker| gang_spi::DrainingWorker {
                    fleet: worker.fleet.clone(),
                    instance: worker.instance.clone(),
                    deadline: worker.deadline,
                }));
        }
    }

    pub(crate) async fn refresh(
        &mut self,
        repository: &impl ExpiringRecordPort,
        drain: &ControllerDrain,
        now: Timestamp,
        ttl: Duration,
    ) -> Result<(), PortError> {
        for chunk in &mut self.chunks {
            let previous = chunk.workers.len();
            let mut changed = false;
            let mut latest = now;
            chunk.workers.retain_mut(|worker| {
                let Ok(index) = drain.workers.binary_search_by(|current| {
                    (&current.fleet, &current.instance).cmp(&(&worker.fleet, &worker.instance))
                }) else {
                    return false;
                };
                let current = &drain.workers[index];
                let deadline = worker.deadline.min(current.deadline);
                changed |= deadline != worker.deadline;
                worker.deadline = deadline;
                if deadline <= now {
                    return false;
                }
                latest = latest.max(deadline);
                true
            });
            if chunk.workers.is_empty() {
                if repository.remove(&chunk.key, chunk.version, now).await?
                    == gang_spi::Remove::Stale
                    && let Some(current) = repository.get(&chunk.key, now).await?
                {
                    repository.remove(&chunk.key, current.version, now).await?;
                }
                continue;
            }
            // Never add workers, extend a dispatch stamp, or resurrect an expired incarnation.
            if changed || previous != chunk.workers.len() {
                encode(&chunk.workers, &mut chunk.body)?;
            }
            let expires = latest.min(now + ttl);
            match repository
                .replace(&chunk.key, chunk.version, &chunk.body, now, expires)
                .await?
            {
                Replace::Replaced(version) => chunk.version = version,
                Replace::Stale => {
                    let current = repository
                        .get(&chunk.key, now)
                        .await?
                        .ok_or_else(|| PortError::backend("controller drain protection expired"))?;
                    chunk.version = match repository
                        .replace(&chunk.key, current.version, &chunk.body, now, expires)
                        .await?
                    {
                        Replace::Replaced(version) => version,
                        Replace::Stale => {
                            return Err(PortError::unavailable(
                                "controller drain protection changed during renewal",
                            ));
                        }
                    };
                }
            }
        }
        self.chunks.retain(|chunk| !chunk.workers.is_empty());
        Ok(())
    }
}

/// The successor loads once, then refreshes only the original, nonempty record set.
#[derive(Default)]
pub(crate) struct Incoming {
    loaded: bool,
    records: Vec<(RecordId, Record, Vec<Worker>)>,
    page: Vec<(RecordId, Record)>,
    ignored: Vec<(FleetName, InstanceId)>,
    live: Vec<(usize, usize)>,
    connected: Vec<usize>,
    active: Vec<(usize, usize, usize)>,
    retained: Vec<bool>,
}

impl Incoming {
    pub(crate) async fn refresh(
        &mut self,
        repository: &impl ExpiringRecordPort,
        now: Timestamp,
        snapshot: &SchedulerSnapshot,
        fleets: &mut [FleetPass],
    ) -> Result<(), PortError> {
        if self.loaded {
            for (id, record, workers) in &mut self.records {
                if let Some(current) = repository
                    .get(
                        &RecordKey {
                            kind: RecordKind::ControllerDrain,
                            id: *id,
                        },
                        now,
                    )
                    .await?
                {
                    if record.body != current.body {
                        Vec::<Worker>::deserialize_in_place(
                            &mut serde_json::Deserializer::from_slice(&current.body),
                            workers,
                        )
                        .map_err(PortError::backend)?;
                    }
                    *record = current;
                } else {
                    record.expires_at = now;
                }
            }
        } else {
            self.records.clear();
            let mut after = None;
            loop {
                let more = repository
                    .records(
                        RecordKind::ControllerDrain,
                        after,
                        NonZeroUsize::new(gang_spi::MAX_RECORD_CHANGES)
                            .ok_or_else(|| PortError::backend("zero record page"))?,
                        now,
                        &mut self.page,
                    )
                    .await?;
                after = self.page.last().map(|entry| entry.0);
                for (id, record) in self.page.drain(..) {
                    let workers =
                        serde_json::from_slice(&record.body).map_err(PortError::backend)?;
                    self.records.push((id, record, workers));
                }
                if !more {
                    break;
                }
                if after.is_none() {
                    return Err(PortError::backend("empty controller drain page"));
                }
            }
            self.loaded = true;
        }
        self.records
            .retain(|(_, record, _)| record.expires_at > now);
        self.retain(now, snapshot, fleets);
        Ok(())
    }

    /// Index the complete provider census and current engine without copying physical names.
    fn index(&mut self, snapshot: &SchedulerSnapshot, fleets: &[FleetPass]) {
        self.live.clear();
        for (fleet, pass) in fleets.iter().enumerate() {
            self.live
                .extend((0..pass.supply.instances.len()).map(|instance| (fleet, instance)));
        }
        self.live.sort_unstable_by(|a, b| {
            (
                &fleets[a.0].policy.name,
                &fleets[a.0].supply.instances[a.1].id,
            )
                .cmp(&(
                    &fleets[b.0].policy.name,
                    &fleets[b.0].supply.instances[b.1].id,
                ))
        });
        self.connected.clear();
        self.connected.extend(0..snapshot.workers.len());
        self.connected.sort_unstable_by(|a, b| {
            let a = &snapshot.workers[*a];
            let b = &snapshot.workers[*b];
            (&a.fleet, a.id.instance_name()).cmp(&(&b.fleet, b.id.instance_name()))
        });
    }

    fn retain(&mut self, now: Timestamp, snapshot: &SchedulerSnapshot, fleets: &mut [FleetPass]) {
        if self.records.is_empty() {
            for fleet in fleets {
                fleet.handover_protected.clear();
            }
            return;
        }
        self.index(snapshot, fleets);
        self.active.clear();
        self.retained.resize(self.records.len(), false);
        self.retained.fill(false);
        for (record, (_, _, workers)) in self.records.iter().enumerate() {
            for (index, worker) in workers.iter().enumerate() {
                if worker.deadline <= now {
                    continue;
                }
                let ignored = self.ignored.binary_search_by(|entry| {
                    (&entry.0, &entry.1).cmp(&(&worker.fleet, &worker.instance))
                });
                let Err(ignored) = ignored else { continue };
                let present = self.live.binary_search_by(|entry| {
                    (
                        &fleets[entry.0].policy.name,
                        &fleets[entry.0].supply.instances[entry.1].id,
                    )
                        .cmp(&(&worker.fleet, &worker.instance))
                });
                let reconnected = self
                    .connected
                    .binary_search_by(|entry| {
                        let connected = &snapshot.workers[*entry];
                        (&connected.fleet, connected.id.instance_name())
                            .cmp(&(&worker.fleet, Some(worker.instance.as_str())))
                    })
                    .is_ok();
                if reconnected || present.is_err() {
                    self.ignored
                        .insert(ignored, (worker.fleet.clone(), worker.instance.clone()));
                    continue;
                }
                if let Ok(present) = present {
                    self.active.push((record, index, self.live[present].0));
                    self.retained[record] = true;
                }
            }
        }
        self.apply(fleets);
        let mut record = 0;
        self.records.retain(|_| {
            let retained = self.retained[record];
            record += 1;
            retained
        });
    }

    fn apply(&mut self, fleets: &mut [FleetPass]) {
        self.active.sort_unstable_by(|a, b| {
            let a = &self.records[a.0].2[a.1];
            let b = &self.records[b.0].2[b.1];
            (&a.fleet, &a.instance).cmp(&(&b.fleet, &b.instance))
        });
        for fleet in fleets.iter_mut() {
            let name = &fleet.policy.name;
            fleet.handover_protected.retain(|instance| {
                self.active
                    .binary_search_by(|entry| {
                        let worker = &self.records[entry.0].2[entry.1];
                        (&worker.fleet, &worker.instance).cmp(&(name, instance))
                    })
                    .is_ok()
            });
        }
        for &(record, index, fleet) in &self.active {
            let worker = &self.records[record].2[index];
            let protected = &mut fleets[fleet].handover_protected;
            if let Err(position) = protected.binary_search(&worker.instance) {
                protected.insert(position, worker.instance.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gang_clock::{Clock, TestClock};
    use gang_reconciler::{Action, HostedSnapshot, PassTime, Plan, Reconciler, SourceProfiles};
    use gang_spi::{DrainingWorker, FleetInstance, SchedulerSnapshot};
    use gang_testkit::{FakeRepository, TestResult, worker_on};
    use gang_types::{FleetSet, InstanceLifecycle};

    use super::*;

    fn fixture() -> TestResult<(Arc<TestClock>, Vec<FleetPass>, ControllerDrain)> {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(1_000_000)));
        let fleets = crate::test_support_fleet::fleets();
        let mut pass = FleetPass::new(fleets.policies()[0].clone());
        let instance = InstanceId::try_new("old-worker")?;
        pass.supply.desired = 1;
        pass.supply.instances.push(FleetInstance {
            id: instance.clone(),
            lifecycle: InstanceLifecycle::InService,
            launched_at: Timestamp::UNIX_EPOCH,
            release_first: true,
        });
        let drain = ControllerDrain {
            workers: vec![DrainingWorker {
                fleet: pass.policy.name.clone(),
                instance,
                deadline: clock.wall() + Duration::from_mins(6),
            }],
            complete: false,
        };
        Ok((clock, vec![pass], drain))
    }

    #[tokio::test]
    async fn old_worker_counts_live_but_never_as_free_or_booting_and_cannot_be_terminated()
    -> TestResult {
        let (clock, mut fleets, drain) = fixture()?;
        let repository = FakeRepository::default();
        let mut outgoing = Outgoing::new(&ControllerId::new("old"), &drain)?;
        outgoing
            .install(&repository, clock.wall(), Duration::from_secs(10))
            .await?;
        let mut incoming = Incoming::default();
        let mut snapshot = SchedulerSnapshot {
            queued: vec![crate::test_support_fleet::queued_demand(FleetSet::prefix(
                1,
            )?)],
            ..Default::default()
        };
        incoming
            .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
            .await?;
        let policies = vec![fleets[0].policy.clone()];
        let scaling = Arc::new(gang_config::ScalingConfig::default());
        let actions = gang_config::ActionsConfig::default();
        let profiles = SourceProfiles::new(&scaling, actions.history_window);
        let mut reconciler = Reconciler::new(&policies, Arc::clone(&scaling), profiles.clone());
        let mut hosted = HostedSnapshot::default();
        hosted.replace(&mut snapshot);
        let mut plan = Plan::new();
        reconciler.reconcile(&mut fleets, &hosted, PassTime::read(&clock), &mut plan);
        assert!(
            plan.steps
                .iter()
                .any(|step| matches!(step.action, Action::ScaleOut { to: 2, .. })),
            "the old live instance must not satisfy new-engine demand as usable or booting"
        );
        let mut limited = fleets.clone();
        limited[0].policy.instances = gang_config::InstanceBounds::up_to(std::num::NonZeroU32::MIN);
        let mut capped = Reconciler::new(&[limited[0].policy.clone()], scaling, profiles);
        let mut capped_plan = Plan::new();
        capped.reconcile(
            &mut limited,
            &hosted,
            PassTime::read(&clock),
            &mut capped_plan,
        );
        assert!(
            !capped_plan
                .steps
                .iter()
                .any(|step| matches!(step.action, Action::ScaleOut { .. })),
            "the protected instance must still consume the fleet's live/max allowance"
        );
        snapshot.queued.clear();
        hosted.replace(&mut snapshot);
        // Hold past the former action-killing grace and every orphan/idle/drain threshold.
        for _ in 0..35 {
            clock.advance(Duration::from_secs(9));
            outgoing
                .refresh(&repository, &drain, clock.wall(), Duration::from_secs(10))
                .await?;
            incoming
                .refresh(&repository, clock.wall(), hosted.snapshot(), &mut fleets)
                .await?;
            reconciler.reconcile(&mut fleets, &hosted, PassTime::read(&clock), &mut plan);
            assert!(!plan.steps.iter().any(|step| matches!(
                step.action,
                Action::Terminate { .. } | Action::StartDrain { .. }
            )));
            assert_eq!(
                fleets[0].handover_protected,
                vec![drain.workers[0].instance.clone()]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn renewal_shrinks_to_actual_stamp_and_never_resurrects_an_expired_record() -> TestResult
    {
        let (clock, _fleets, mut drain) = fixture()?;
        let repository = FakeRepository::default();
        let holder = ControllerId::new("old");
        let mut outgoing = Outgoing::new(&holder, &drain)?;
        outgoing
            .install(&repository, clock.wall(), Duration::from_secs(10))
            .await?;
        outgoing
            .install(&repository, clock.wall(), Duration::from_secs(10))
            .await?;
        drain.workers[0].deadline = clock.wall() + Duration::from_secs(3);
        outgoing
            .refresh(&repository, &drain, clock.wall(), Duration::from_secs(10))
            .await?;
        assert_eq!(
            repository
                .get(&key(&holder, 0), clock.wall())
                .await?
                .ok_or("protection missing")?
                .expires_at,
            drain.workers[0].deadline
        );
        clock.advance(Duration::from_secs(3));
        outgoing
            .refresh(&repository, &drain, clock.wall(), Duration::from_secs(10))
            .await?;
        assert!(
            repository
                .get(&key(&holder, 0), clock.wall())
                .await?
                .is_none()
        );
        drain.workers[0].deadline = clock.wall() + Duration::from_mins(1);
        outgoing
            .refresh(&repository, &drain, clock.wall(), Duration::from_secs(10))
            .await?;
        assert!(
            repository
                .get(&key(&holder, 0), clock.wall())
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_reads_fail_closed_but_empty_terms_do_not_query_forever() -> TestResult {
        let (clock, mut fleets, drain) = fixture()?;
        let repository = FakeRepository::default();
        let mut outgoing = Outgoing::new(&ControllerId::new("old"), &drain)?;
        outgoing
            .install(&repository, clock.wall(), Duration::from_secs(10))
            .await?;
        let mut incoming = Incoming::default();
        let snapshot = SchedulerSnapshot::default();
        incoming
            .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
            .await?;
        repository.fail_records(true);
        assert!(
            incoming
                .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
                .await
                .is_err()
        );
        assert_eq!(fleets[0].handover_protected.len(), 1);
        repository.fail_records(false);
        clock.advance(Duration::from_secs(10));
        incoming
            .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
            .await?;
        assert!(fleets[0].handover_protected.is_empty());
        repository.fail_records(true);
        incoming
            .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
            .await?;
        let repository = FakeRepository::default();
        let mut empty = Incoming::default();
        empty
            .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
            .await?;
        repository.fail_records(true);
        empty
            .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_or_disappearance_permanently_ends_physical_protection() -> TestResult {
        for reconnect in [false, true] {
            let (clock, mut fleets, drain) = fixture()?;
            let repository = FakeRepository::default();
            let mut outgoing = Outgoing::new(&ControllerId::new("old"), &drain)?;
            outgoing
                .install(&repository, clock.wall(), Duration::from_secs(10))
                .await?;
            let mut incoming = Incoming::default();
            let mut snapshot = SchedulerSnapshot::default();
            incoming
                .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
                .await?;
            if reconnect {
                snapshot.workers.push(gang_spi::WorkerState {
                    id: worker_on(&drain.workers[0].instance)?,
                    fleet: drain.workers[0].fleet.clone(),
                    environment: Some(fleets[0].policy.action_environment.clone()),
                    current_build: true,
                    running_actions: 0,
                    max_inflight_actions: None,
                    capacity: Some(gang_types::ActionResources {
                        cpu_millicores: 4_000,
                        memory: gang_types::Bytes::new(8 << 30),
                        disk: gang_types::Bytes::new(100 << 30),
                    }),
                    reserved: Some(gang_types::ActionResources::default()),
                    cache: None,
                    draining: false,
                    last_seen: clock.wall(),
                    paused: false,
                });
            } else {
                fleets[0].supply.instances.clear();
            }
            incoming
                .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
                .await?;
            assert!(fleets[0].handover_protected.is_empty());
            repository.fail_records(true);
            snapshot.workers.clear();
            incoming
                .refresh(&repository, clock.wall(), &snapshot, &mut fleets)
                .await?;
            assert!(fleets[0].handover_protected.is_empty());
        }
        Ok(())
    }

    #[tokio::test]
    async fn frozen_fleets_larger_than_one_record_are_chunked_without_truncation() -> TestResult {
        let repository = FakeRepository::default();
        let mut drain = ControllerDrain::default();
        let now = Timestamp::from_unix_millis(1_000);
        for index in 0..8_000 {
            drain.workers.push(DrainingWorker {
                fleet: FleetName::try_new("fleet")?,
                instance: InstanceId::try_new(format!("physical-worker-{index}"))?,
                deadline: now + Duration::from_mins(10),
            });
        }
        let mut outgoing = Outgoing::new(&ControllerId::new("large"), &drain)?;
        assert!(outgoing.chunks.len() > 1);
        assert_eq!(
            outgoing
                .chunks
                .iter()
                .map(|chunk| chunk.workers.len())
                .sum::<usize>(),
            drain.workers.len()
        );
        assert!(
            outgoing
                .chunks
                .iter()
                .all(|chunk| chunk.body.len() <= MAX_RECORD_BODY_BYTES)
        );
        outgoing
            .install(&repository, now, Duration::from_secs(10))
            .await?;
        assert_eq!(repository.stored_records(), outgoing.chunks.len());
        Ok(())
    }
}
