//! Fenced leader lease stored in `PostgreSQL`.

use gang_clock::Timestamp;
use gang_spi::{
    Acquisition, CandidateGeneration, CandidatePriority, CandidatePriorityRef,
    ControllerHostIdentity, ControllerId, LeaderLeasePort, Lease, LeaseCandidate, LeaseEpoch,
    PortError, Renewal, Takeover,
};
use tokio_postgres::Row;
use tokio_postgres::types::Type;

use crate::Postgres;
use crate::codec::{at, corrupt, get, signed, timestamp, unsigned};
use crate::errors::PostgresError;

const LEASE: &str = "leader";

fn decode_lease(row: &Row, holder: String) -> Result<Lease, PostgresError> {
    Ok(Lease {
        holder: ControllerId::new(holder),
        epoch: LeaseEpoch::new(unsigned("epoch", get(row, "epoch")?)?),
        expires_at: timestamp("expires_at", get(row, "expires_at")?)?,
    })
}

fn priority<'a>(
    row: &'a Row,
    column: &'static str,
) -> Result<Option<CandidatePriorityRef<'a>>, PostgresError> {
    get::<Option<&str>>(row, column)?
        .map(|value| CandidatePriorityRef::try_from(value).map_err(|error| corrupt(column, error)))
        .transpose()
}

impl LeaderLeasePort for Postgres {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        let generation = match host {
            ControllerHostIdentity::Launched { at, instance } => {
                self.run(async |client| {
                    client.query_typed_one("SELECT 1", &[]).await?;
                    Ok(())
                })
                .await?;
                return Ok(LeaseCandidate {
                    holder: holder.clone(),
                    priority: CandidatePriority::launched(*at, instance),
                });
            }
            ControllerHostIdentity::Persistent(host) => {
                self.mutate(async |client| {
                    if let Some(row) = client
                        .query_typed_opt(
                            "SELECT generation FROM controller_hosts WHERE host = $1",
                            &[(&host.as_str(), Type::TEXT)],
                        )
                        .await?
                    {
                        return unsigned("generation", get(&row, "generation")?);
                    }
                    let row = client
                        .query_typed_opt(
                            "SELECT value FROM counters WHERE name = 'candidate' FOR UPDATE",
                            &[],
                        )
                        .await?
                        .ok_or_else(|| corrupt("counters", "missing candidate counter"))?;
                    // The counter lock serializes first registration, including the map recheck.
                    if let Some(row) = client
                        .query_typed_opt(
                            "SELECT generation FROM controller_hosts WHERE host = $1",
                            &[(&host.as_str(), Type::TEXT)],
                        )
                        .await?
                    {
                        return unsigned("generation", get(&row, "generation")?);
                    }
                    let value = unsigned("candidate", get(&row, "value")?)?;
                    let next = value.checked_add(1).ok_or(PostgresError::Range {
                        column: "candidate",
                        value,
                    })?;
                    let stored = signed("candidate", next)?;
                    client
                        .execute_typed(
                            "UPDATE counters SET value = $1 WHERE name = 'candidate'",
                            &[(&stored, Type::INT8)],
                        )
                        .await?;
                    client
                        .execute_typed(
                            "INSERT INTO controller_hosts (host, generation) VALUES ($1, $2)",
                            &[(&host.as_str(), Type::TEXT), (&stored, Type::INT8)],
                        )
                        .await?;
                    Ok(next)
                })
                .await?
            }
        };
        Ok(LeaseCandidate {
            holder: holder.clone(),
            priority: CandidatePriority::registered(CandidateGeneration::new(generation)),
        })
    }

    async fn acquire(
        &self,
        candidate: &LeaseCandidate,
        now: Timestamp,
        stale_before: Timestamp,
        expires_at: Timestamp,
        takeover: Takeover,
    ) -> Result<Acquisition, PortError> {
        let now = at(now)?;
        let stale_before = at(stale_before)?;
        let expires_at = at(expires_at)?;
        let candidate_priority = candidate.priority.view();
        Ok(self.mutate(async |client| {
            // The insert serializes first creation with every other contender, including old writers.
            client.execute_typed(
                "INSERT INTO leases (name, holder, epoch, expires_at) VALUES ($1, NULL, 0, $2) ON CONFLICT (name) DO NOTHING",
                &[(&LEASE, Type::TEXT), (&expires_at, Type::TIMESTAMPTZ)],
            ).await?;
            let row = client.query_typed_one(
                "SELECT holder, epoch, expires_at, holder_priority, holder_priority_epoch, pending_holder, pending_priority, pending_epoch, pending_expires_at FROM leases WHERE name = $1 FOR UPDATE",
                &[(&LEASE, Type::TEXT)],
            ).await?;
            let holder: Option<String> = get(&row, "holder")?;
            let epoch: i64 = get(&row, "epoch")?;
            let epoch_unsigned = unsigned("epoch", epoch)?;
            let expiry: time::OffsetDateTime = get(&row, "expires_at")?;
            let binding: i64 = get(&row, "holder_priority_epoch")?;
            let effective_priority = if binding == epoch { priority(&row, "holder_priority")? } else { None };
            let pending_holder: Option<&str> = get(&row, "pending_holder")?;
            let pending_epoch: Option<i64> = get(&row, "pending_epoch")?;
            let pending_expiry: Option<time::OffsetDateTime> = get(&row, "pending_expires_at")?;
            let pending = match (pending_holder, pending_epoch, pending_expiry) {
                (Some(holder), Some(term), Some(expiry)) if term == epoch && expiry > now => {
                    priority(&row, "pending_priority")?.map(|rank| (holder, rank))
                }
                _ => None,
            };
            let free = holder.is_none() || expiry < stale_before;
            let same_holder = holder.as_deref() == Some(candidate.holder.as_str());
            let newer = !same_holder && Some(candidate_priority) > effective_priority;
            let may_replace = pending.as_ref().is_none_or(|(h, rank)| {
                candidate_priority > *rank || (candidate_priority == *rank && *h == candidate.holder.as_str())
            });
            if free {
                if let Some((reserved_holder, rank)) = &pending {
                    let reserved_for_candidate = *reserved_holder == candidate.holder.as_str() && *rank == candidate_priority;
                    if !(reserved_for_candidate || (takeover == Takeover::Request && candidate_priority > *rank)) {
                        return Ok(Acquisition::Reserved);
                    }
                }
                let next = epoch_unsigned.checked_add(1).ok_or(PostgresError::Range { column: "epoch", value: epoch_unsigned })?;
                let next = signed("epoch", next)?;
                let acquired = client.query_typed_one(
                    "UPDATE leases SET holder = $2, epoch = $3, expires_at = $4, record_sequence = 0, holder_priority = $5, holder_priority_epoch = $3, pending_holder = NULL, pending_priority = NULL, pending_epoch = NULL, pending_expires_at = NULL WHERE name = $1 RETURNING holder, epoch, expires_at",
                    &[(&LEASE, Type::TEXT), (&candidate.holder.as_str(), Type::TEXT), (&next, Type::INT8), (&expires_at, Type::TIMESTAMPTZ), (&candidate.priority.as_str(), Type::TEXT)],
                ).await?;
                return Ok(Acquisition::Acquired(decode_lease(&acquired, get(&acquired, "holder")?)?));
            }
            if takeover == Takeover::Request && newer && may_replace {
                client.execute_typed(
                    "UPDATE leases SET pending_holder = $2, pending_priority = $3, pending_epoch = $4, pending_expires_at = $5 WHERE name = $1",
                    &[(&LEASE, Type::TEXT), (&candidate.holder.as_str(), Type::TEXT), (&candidate.priority.as_str(), Type::TEXT), (&epoch, Type::INT8), (&expires_at, Type::TIMESTAMPTZ)],
                ).await?;
            }
            let held = decode_lease(&row, holder.ok_or_else(|| corrupt("holder", "missing held lease holder"))?)?;
            if newer && expiry > now {
                Ok(Acquisition::Contending(held))
            } else {
                Ok(Acquisition::Held(held))
            }
        }).await?)
    }

    async fn renew(
        &self,
        lease: &Lease,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Renewal, PortError> {
        let now = at(now)?;
        let expires_at = at(expires_at)?;
        let epoch = signed("epoch", lease.epoch.get())?;
        Ok(self.mutate(async |client| {
            let row = client.query_typed_opt(
                "UPDATE leases SET expires_at = $4 WHERE name = $1 AND holder = $2 AND epoch = $3 RETURNING holder, epoch, expires_at, holder_priority, holder_priority_epoch, pending_holder, pending_priority, pending_epoch, pending_expires_at",
                &[(&LEASE, Type::TEXT), (&lease.holder.as_str(), Type::TEXT), (&epoch, Type::INT8), (&expires_at, Type::TIMESTAMPTZ)],
            ).await?;
            let Some(row) = row else { return Ok(Renewal::Lost); };
            let updated = decode_lease(&row, get(&row, "holder")?)?;
            let binding: i64 = get(&row, "holder_priority_epoch")?;
            let rank = if binding == epoch { priority(&row, "holder_priority")? } else { None };
            let pending_holder: Option<&str> = get(&row, "pending_holder")?;
            let pending_epoch: Option<i64> = get(&row, "pending_epoch")?;
            let pending_expiry: Option<time::OffsetDateTime> = get(&row, "pending_expires_at")?;
            let yielding = match (pending_holder, pending_epoch, pending_expiry) {
                (Some(holder), Some(term), Some(expiry)) if term == epoch && expiry > now => {
                    let pending_priority = priority(&row, "pending_priority")?;
                    holder != lease.holder.as_str() && pending_priority.is_some() && pending_priority > rank
                }
                _ => false,
            };
            Ok(if yielding { Renewal::YieldRequested(updated) } else { Renewal::Renewed(updated) })
        }).await?)
    }

    async fn release(&self, lease: &Lease) -> Result<(), PortError> {
        let epoch = signed("epoch", lease.epoch.get())?;
        self.mutate(async |client| {
            client.execute_typed(
                "UPDATE leases SET holder = NULL WHERE name = $1 AND holder = $2 AND epoch = $3",
                &[(&LEASE, Type::TEXT), (&lease.holder.as_str(), Type::TEXT), (&epoch, Type::INT8)],
            ).await?;
            Ok(())
        })
        .await?;
        Ok(())
    }
}
