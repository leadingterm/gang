//! Versioned expiring records stored in `PostgreSQL`.

use std::collections::BTreeSet;

use gang_clock::Timestamp;
use gang_spi::{
    ExpiringRecordPort, Insert, PortError, Record, RecordKey, RecordVersion, Remove, Replace,
};
use tokio_postgres::types::Type;

use crate::Postgres;
use crate::codec::{at, get, signed, timestamp, unsigned};
use crate::plugin::Finish;

impl ExpiringRecordPort for Postgres {
    async fn insert(
        &self,
        records: &[(RecordKey, &[u8])],
        version: RecordVersion,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Insert, PortError> {
        let mut keys = BTreeSet::new();
        for (key, _) in records {
            if !keys.insert(*key) {
                return Ok(Insert::Exists);
            }
        }
        let mut records: Vec<_> = records.iter().collect();
        records.sort_unstable_by_key(|(key, _)| *key);
        let version = signed("version", version.get())?;
        let now = at(now)?;
        let expires_at = at(expires_at)?;
        Ok(self.transaction(async |tx| {
            for (key, body) in records {
                let count = tx.execute_typed(
                    "INSERT INTO expiring_records (kind, id, body, version, expires_at) VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (kind, id) DO UPDATE SET body = EXCLUDED.body, version = EXCLUDED.version, \
                     expires_at = EXCLUDED.expires_at WHERE expiring_records.expires_at <= $6",
                    &[(&key.kind.as_str(), Type::TEXT), (&key.id.as_bytes().as_slice(), Type::BYTEA), (body, Type::BYTEA), (&version, Type::INT8), (&expires_at, Type::TIMESTAMPTZ), (&now, Type::TIMESTAMPTZ)],
                ).await?;
                if count == 0 {
                    return Ok(Finish::Rollback(Insert::Exists));
                }
            }
            Ok(Finish::Commit(Insert::Inserted))
        }).await?)
    }

    async fn get(&self, key: &RecordKey, now: Timestamp) -> Result<Option<Record>, PortError> {
        let now = at(now)?;
        Ok(self.run(async |client| {
            let row = client.query_typed_opt(
                "SELECT body, version, expires_at FROM expiring_records WHERE kind = $1 AND id = $2 AND expires_at > $3",
                &[(&key.kind.as_str(), Type::TEXT), (&key.id.as_bytes().as_slice(), Type::BYTEA), (&now, Type::TIMESTAMPTZ)],
            ).await?;
            row.map(|row| {
                Ok(Record {
                    body: get(&row, "body")?,
                    version: RecordVersion::new(unsigned("version", get(&row, "version")?)?),
                    expires_at: timestamp("expires_at", get(&row, "expires_at")?)?,
                })
            }).transpose()
        }).await?)
    }

    async fn replace(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        body: &[u8],
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Replace, PortError> {
        let version = signed("version", version.get())?;
        let now = at(now)?;
        let expires_at = at(expires_at)?;
        Ok(self.mutate(async |client| {
            let row = client.query_typed_opt(
                "UPDATE expiring_records SET body = $1, version = version + 1, expires_at = $2 \
                 WHERE kind = $3 AND id = $4 AND version = $5 AND expires_at > $6 RETURNING version",
                &[(&body, Type::BYTEA), (&expires_at, Type::TIMESTAMPTZ), (&key.kind.as_str(), Type::TEXT), (&key.id.as_bytes().as_slice(), Type::BYTEA), (&version, Type::INT8), (&now, Type::TIMESTAMPTZ)],
            ).await?;
            match row {
                Some(row) => Ok(Replace::Replaced(RecordVersion::new(unsigned("version", get(&row, "version")?)?))),
                None => Ok(Replace::Stale),
            }
        }).await?)
    }

    async fn remove(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        now: Timestamp,
    ) -> Result<Remove, PortError> {
        let version = signed("version", version.get())?;
        let now = at(now)?;
        Ok(self.mutate(async |client| {
            let count = client.execute_typed(
                "DELETE FROM expiring_records WHERE kind = $1 AND id = $2 AND version = $3 AND expires_at > $4",
                &[(&key.kind.as_str(), Type::TEXT), (&key.id.as_bytes().as_slice(), Type::BYTEA), (&version, Type::INT8), (&now, Type::TIMESTAMPTZ)],
            ).await?;
            Ok(if count == 0 { Remove::Stale } else { Remove::Removed })
        }).await?)
    }

    async fn purge(&self, now: Timestamp) -> Result<(), PortError> {
        let now = at(now)?;
        self.mutate(async |client| {
            client
                .execute_typed(
                    "DELETE FROM expiring_records WHERE expires_at <= $1",
                    &[(&now, Type::TIMESTAMPTZ)],
                )
                .await?;
            Ok(())
        })
        .await?;
        Ok(())
    }
}
