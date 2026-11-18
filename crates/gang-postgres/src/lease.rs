//! Fenced leader lease stored in `PostgreSQL`.

use gang_clock::Timestamp;
use gang_spi::{Acquisition, ControllerId, LeaderLeasePort, Lease, LeaseEpoch, PortError, Renewal};
use tokio_postgres::Row;
use tokio_postgres::types::Type;

use crate::Postgres;
use crate::codec::{at, get, signed, timestamp, unsigned};
use crate::errors::PostgresError;

const LEASE: &str = "leader";

fn decode_lease(row: &Row, holder: String) -> Result<Lease, PostgresError> {
    Ok(Lease {
        holder: ControllerId::new(holder),
        epoch: LeaseEpoch::new(unsigned("epoch", get(row, "epoch")?)?),
        expires_at: timestamp("expires_at", get(row, "expires_at")?)?,
    })
}

impl LeaderLeasePort for Postgres {
    async fn acquire(
        &self,
        holder: &ControllerId,
        stale_before: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Acquisition, PortError> {
        let stale_before = at(stale_before)?;
        let expires_at = at(expires_at)?;
        Ok(self.mutate(async |client| {
            loop {
                let row = client.query_typed_opt(
                    "INSERT INTO leases (name, holder, epoch, expires_at) VALUES ($1, $2, 1, $3) \
                     ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, epoch = leases.epoch + 1, \
                     expires_at = EXCLUDED.expires_at WHERE leases.holder IS NULL OR leases.expires_at < $4 \
                     RETURNING holder, epoch, expires_at",
                    &[(&LEASE, Type::TEXT), (&holder.as_str(), Type::TEXT), (&expires_at, Type::TIMESTAMPTZ), (&stale_before, Type::TIMESTAMPTZ)],
                ).await?;
                if let Some(row) = row {
                    return Ok(Acquisition::Acquired(decode_lease(&row, get(&row, "holder")?)?));
                }
                if let Some(row) = client.query_typed_opt(
                    "SELECT holder, epoch, expires_at FROM leases WHERE name = $1", &[(&LEASE, Type::TEXT)],
                ).await? {
                    let holder: Option<String> = get(&row, "holder")?;
                    if let Some(holder) = holder {
                        let expiry: time::OffsetDateTime = get(&row, "expires_at")?;
                        if expiry >= stale_before {
                            return Ok(Acquisition::Held(decode_lease(&row, holder)?));
                        }
                    }
                }
                // A retry requires a concurrent change after the failed insert; without further
                // changes the next insert acquires the free lease or the select observes its holder.
            }
        }).await?)
    }

    async fn renew(&self, lease: &Lease, expires_at: Timestamp) -> Result<Renewal, PortError> {
        let expires_at = at(expires_at)?;
        let epoch = signed("epoch", lease.epoch.get())?;
        Ok(self
            .mutate(async |client| {
                let row = client.query_typed_opt(
                "UPDATE leases SET expires_at = $4 WHERE name = $1 AND holder = $2 AND epoch = $3 \
                 RETURNING holder, epoch, expires_at",
                &[(&LEASE, Type::TEXT), (&lease.holder.as_str(), Type::TEXT), (&epoch, Type::INT8), (&expires_at, Type::TIMESTAMPTZ)],
            ).await?;
                match row {
                    Some(row) => Ok(Renewal::Renewed(decode_lease(
                        &row,
                        get(&row, "holder")?,
                    )?)),
                    None => Ok(Renewal::Lost),
                }
            })
            .await?)
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
