use std::collections::BTreeSet;

use gang_clock::Timestamp;
use gang_spi::{
    ExpiringRecordPort, Insert, PortError, Record, RecordKey, RecordVersion, Remove, Replace,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::Sqlite;
use crate::codec::{millis, signed, timestamp, unsigned};

impl ExpiringRecordPort for Sqlite {
    async fn insert(
        &self,
        records: &[(RecordKey, &[u8])],
        version: RecordVersion,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Insert, PortError> {
        let now = millis(now)?;
        let expires_at = millis(expires_at)?;
        let records: Vec<_> = records
            .iter()
            .map(|(key, body)| (*key, body.to_vec()))
            .collect();
        Ok(self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mut keys = BTreeSet::new();
            for (key, _) in &records {
                if !keys.insert(*key) { return Ok(Insert::Exists); }
                tx.execute("DELETE FROM expiring_records WHERE kind = ?1 AND id = ?2 AND expires_at <= ?3", params![key.kind.as_str(), key.id.as_bytes().as_slice(), now])?;
                let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM expiring_records WHERE kind = ?1 AND id = ?2)", params![key.kind.as_str(), key.id.as_bytes().as_slice()], |row| row.get(0))?;
                if exists { return Ok(Insert::Exists); }
            }
            for (key, body) in records {
                tx.execute("INSERT INTO expiring_records (kind, id, body, version, expires_at) VALUES (?1, ?2, ?3, ?4, ?5)", params![key.kind.as_str(), key.id.as_bytes().as_slice(), body, signed("version", version.get())?, expires_at])?;
            }
            tx.commit()?;
            Ok(Insert::Inserted)
        }).await?)
    }

    async fn get(&self, key: &RecordKey, now: Timestamp) -> Result<Option<Record>, PortError> {
        let key = *key;
        let now = millis(now)?;
        Ok(self.run(move |db| {
            let row: Option<(Vec<u8>, i64, i64)> = db.query_row("SELECT body, version, expires_at FROM expiring_records WHERE kind = ?1 AND id = ?2 AND expires_at > ?3", params![key.kind.as_str(), key.id.as_bytes().as_slice(), now], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?;
            row.map(|(body, version, expiry)| Ok(Record { body, version: RecordVersion::new(unsigned("version", version)?), expires_at: timestamp("expires_at", expiry)? })).transpose()
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
        let key = *key;
        let version = signed("version", version.get())?;
        let body = body.to_vec();
        let now = millis(now)?;
        let expires_at = millis(expires_at)?;
        Ok(self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let next: Option<i64> = tx.query_row("UPDATE expiring_records SET body = ?1, version = version + 1, expires_at = ?2 WHERE kind = ?3 AND id = ?4 AND version = ?5 AND expires_at > ?6 RETURNING version", params![body, expires_at, key.kind.as_str(), key.id.as_bytes().as_slice(), version, now], |row| row.get(0)).optional()?;
            let result = match next { Some(value) => Replace::Replaced(RecordVersion::new(unsigned("version", value)?)), None => Replace::Stale };
            tx.commit()?;
            Ok(result)
        }).await?)
    }

    async fn remove(
        &self,
        key: &RecordKey,
        version: RecordVersion,
        now: Timestamp,
    ) -> Result<Remove, PortError> {
        let key = *key;
        let version = signed("version", version.get())?;
        let now = millis(now)?;
        Ok(self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let count = tx.execute("DELETE FROM expiring_records WHERE kind = ?1 AND id = ?2 AND version = ?3 AND expires_at > ?4", params![key.kind.as_str(), key.id.as_bytes().as_slice(), version, now])?;
            tx.commit()?;
            Ok(if count == 0 { Remove::Stale } else { Remove::Removed })
        }).await?)
    }

    async fn purge(&self, now: Timestamp) -> Result<(), PortError> {
        let now = millis(now)?;
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute("DELETE FROM expiring_records WHERE expires_at <= ?1", [now])?;
            tx.commit()?;
            Ok(())
        })
        .await?;
        Ok(())
    }
}
