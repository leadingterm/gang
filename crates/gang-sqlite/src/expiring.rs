use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    ApplyRecords, ExpiringRecordPort, Insert, Lease, PortError, Record, RecordBatchSeq,
    RecordChange, RecordId, RecordKey, RecordKind, RecordVersion, Remove, Replace,
    validate_record_changes,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::Sqlite;
use crate::codec::{millis, page, signed, timestamp, unsigned};
use crate::errors::SqliteError;

impl ExpiringRecordPort for Sqlite {
    async fn apply(
        &self,
        _lease: &Lease,
        _sequence: RecordBatchSeq,
        version: RecordVersion,
        _now: Timestamp,
        changes: &[RecordChange<'_>],
    ) -> Result<ApplyRecords, PortError> {
        validate_record_changes(changes).map_err(PortError::backend)?;
        let version = signed("version", version.get())?;
        let changes = changes
            .iter()
            .map(|change| match *change {
                RecordChange::Put {
                    key,
                    body,
                    expires_at,
                } => Ok((key, Some((body.to_vec(), millis(expires_at)?)))),
                RecordChange::Delete { key } => Ok((key, None)),
            })
            .collect::<Result<Vec<_>, SqliteError>>()?;
        // The process-wide exclusive file lock is SQLite's leadership authority.
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for (key, value) in changes {
                if let Some((body, expiry)) = value {
                    tx.execute(
                        "INSERT INTO expiring_records (kind, id, body, version, expires_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5) \
                         ON CONFLICT(kind, id) DO UPDATE SET body = excluded.body, \
                         version = excluded.version, expires_at = excluded.expires_at",
                        params![
                            key.kind.as_str(),
                            key.id.as_bytes().as_slice(),
                            body,
                            version,
                            expiry
                        ],
                    )?;
                } else {
                    tx.execute(
                        "DELETE FROM expiring_records WHERE kind = ?1 AND id = ?2",
                        params![key.kind.as_str(), key.id.as_bytes().as_slice()],
                    )?;
                }
            }
            tx.commit()?;
            Ok(ApplyRecords::Applied)
        })
        .await
        .map_err(Into::into)
    }

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

    async fn records(
        &self,
        kind: RecordKind,
        after: Option<RecordId>,
        limit: NonZeroUsize,
        now: Timestamp,
        into: &mut Vec<(RecordId, Record)>,
    ) -> Result<bool, PortError> {
        let now = millis(now)?;
        let count = i64::try_from(limit.get().saturating_add(1)).unwrap_or(i64::MAX);
        let mut records = std::mem::take(into);
        let (records, more) = self
            .run(move |db| {
                records.clear();
                let mut statement = db.prepare_cached(
                    "SELECT id, body, version, expires_at FROM expiring_records \
                 WHERE kind = ?1 AND expires_at > ?2 AND (?3 IS NULL OR id > ?3) \
                 ORDER BY id LIMIT ?4",
                )?;
                let mut rows = statement.query(params![
                    kind.as_str(),
                    now,
                    after.as_ref().map(|id| id.as_bytes().as_slice()),
                    count
                ])?;
                while let Some(row) = rows.next()? {
                    let blob =
                        row.get_ref(0)?
                            .as_blob()
                            .map_err(|error| crate::SqliteError::Corrupt {
                                column: "id",
                                detail: error.to_string(),
                            })?;
                    let id = blob.try_into().map_err(|_| crate::SqliteError::Corrupt {
                        column: "id",
                        detail: "expected a 32-byte record id".to_owned(),
                    })?;
                    records.push((
                        RecordId::new(id),
                        Record {
                            body: row.get(1)?,
                            version: RecordVersion::new(unsigned("version", row.get(2)?)?),
                            expires_at: timestamp("expires_at", row.get(3)?)?,
                        },
                    ));
                }
                let more = page(&mut records, limit);
                Ok((records, more))
            })
            .await?;
        *into = records;
        Ok(more)
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use gang_spi::{
        ControllerId, LeaseEpoch, MAX_RECORD_BODY_BYTES_PER_BATCH, MAX_RECORD_CHANGES, Plugin,
        PluginContext,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::SqliteConfig;

    fn at(ms: u64) -> Timestamp {
        Timestamp::from_unix_millis(ms)
    }

    fn key(id: u8) -> RecordKey {
        RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([id; 32]),
        }
    }

    fn lease() -> Lease {
        Lease {
            holder: ControllerId::new("test"),
            epoch: LeaseEpoch::new(1),
            expires_at: at(1),
        }
    }

    fn store(dir: &TempDir) -> Result<Sqlite, PortError> {
        Sqlite::build(
            SqliteConfig::new(dir.path().join("gang.db")),
            &PluginContext {
                config_path: Path::new("gang.toml"),
            },
        )
    }

    #[tokio::test]
    async fn pages_live_records_in_byte_order() -> Result<(), Box<dyn std::error::Error>> {
        let dir = TempDir::new()?;
        let db = store(&dir)?;
        let changes = [
            RecordChange::Put {
                key: key(255),
                body: b"last",
                expires_at: at(20),
            },
            RecordChange::Put {
                key: key(0),
                body: b"first",
                expires_at: at(20),
            },
            RecordChange::Put {
                key: key(1),
                body: b"expired",
                expires_at: at(10),
            },
            RecordChange::Put {
                key: key(128),
                body: b"middle",
                expires_at: at(20),
            },
        ];
        assert_eq!(
            db.apply(
                &lease(),
                RecordBatchSeq::new(1),
                RecordVersion::new(9),
                at(10),
                &changes
            )
            .await?,
            ApplyRecords::Applied
        );
        let limit = NonZeroUsize::new(1).expect("one is nonzero");
        let mut out = Vec::new();
        assert!(
            db.records(RecordKind::ResourceHistory, None, limit, at(10), &mut out)
                .await?
        );
        assert_eq!(out[0].0, key(0).id);
        assert_eq!(out[0].1.version, RecordVersion::new(9));
        assert!(
            db.records(
                RecordKind::ResourceHistory,
                Some(key(0).id),
                limit,
                at(10),
                &mut out
            )
            .await?
        );
        assert_eq!(out[0].0, key(128).id);
        assert!(
            !db.records(
                RecordKind::ResourceHistory,
                Some(key(128).id),
                limit,
                at(10),
                &mut out
            )
            .await?
        );
        assert_eq!(out[0].0, key(255).id);
        assert!(
            !db.records(
                RecordKind::ResourceHistory,
                Some(key(255).id),
                limit,
                at(10),
                &mut out
            )
            .await?
        );
        assert!(out.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn apply_rejects_invalid_batches_without_writes() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = TempDir::new()?;
        let db = store(&dir)?;
        let original = [RecordChange::Put {
            key: key(1),
            body: b"original",
            expires_at: at(100),
        }];
        db.apply(
            &lease(),
            RecordBatchSeq::new(1),
            RecordVersion::new(1),
            at(0),
            &original,
        )
        .await?;
        let duplicate = [
            RecordChange::Put {
                key: key(1),
                body: b"changed",
                expires_at: at(100),
            },
            RecordChange::Delete { key: key(1) },
        ];
        assert!(matches!(
            db.apply(
                &lease(),
                RecordBatchSeq::new(2),
                RecordVersion::new(2),
                at(0),
                &duplicate
            )
            .await,
            Err(PortError::Backend(_))
        ));
        let many: Vec<_> = (0..=MAX_RECORD_CHANGES)
            .map(|i| {
                Ok(RecordChange::Delete {
                    key: key(u8::try_from(i)?),
                })
            })
            .collect::<Result<_, std::num::TryFromIntError>>()?;
        assert!(matches!(
            db.apply(
                &lease(),
                RecordBatchSeq::new(2),
                RecordVersion::new(2),
                at(0),
                &many
            )
            .await,
            Err(PortError::Backend(_))
        ));
        let oversized = vec![0; MAX_RECORD_BODY_BYTES_PER_BATCH + 1];
        let changes = [RecordChange::Put {
            key: key(2),
            body: &oversized,
            expires_at: at(100),
        }];
        assert!(matches!(
            db.apply(
                &lease(),
                RecordBatchSeq::new(2),
                RecordVersion::new(2),
                at(0),
                &changes
            )
            .await,
            Err(PortError::Backend(_))
        ));
        assert_eq!(
            db.get(&key(1), at(0))
                .await?
                .expect("original remains")
                .body,
            b"original"
        );
        assert!(db.get(&key(2), at(0)).await?.is_none());
        assert_valid_replacement_after_rejections(&db).await?;
        Ok(())
    }

    async fn assert_valid_replacement_after_rejections(
        db: &Sqlite,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let changes = [
            RecordChange::Delete { key: key(1) },
            RecordChange::Put {
                key: key(2),
                body: b"new",
                expires_at: at(100),
            },
        ];
        assert_eq!(
            db.apply(
                &lease(),
                RecordBatchSeq::new(2),
                RecordVersion::new(3),
                at(0),
                &changes
            )
            .await?,
            ApplyRecords::Applied
        );
        assert!(db.get(&key(1), at(0)).await?.is_none());
        assert_eq!(
            db.get(&key(2), at(0)).await?.expect("new record").version,
            RecordVersion::new(3)
        );
        Ok(())
    }
}
