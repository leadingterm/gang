//! Versioned expiring records stored in `PostgreSQL`.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    ApplyRecords, ExpiringRecordPort, Insert, Lease, PortError, Record, RecordBatchSeq,
    RecordChange, RecordId, RecordKey, RecordKind, RecordVersion, Remove, Replace,
    validate_record_changes,
};
use tokio_postgres::types::Type;

use crate::Postgres;
use crate::codec::{at, corrupt, get, page, signed, timestamp, unsigned};
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

    async fn records(
        &self,
        kind: RecordKind,
        after: Option<RecordId>,
        limit: NonZeroUsize,
        now: Timestamp,
        into: &mut Vec<(RecordId, Record)>,
    ) -> Result<bool, PortError> {
        let now = at(now)?;
        let count = i64::try_from(limit.get().saturating_add(1)).unwrap_or(i64::MAX);
        let after = after.as_ref().map(|id| id.as_bytes().as_slice());
        let mut records = std::mem::take(into);
        let (records, more) = self
            .run(async move |client| {
                records.clear();
                let rows = client
                    .query_typed(
                        "SELECT id, body, version, expires_at FROM expiring_records \
                 WHERE kind = $1 AND expires_at > $2 AND ($3::bytea IS NULL OR id > $3) \
                 ORDER BY id LIMIT $4",
                        &[
                            (&kind.as_str(), Type::TEXT),
                            (&now, Type::TIMESTAMPTZ),
                            (&after, Type::BYTEA),
                            (&count, Type::INT8),
                        ],
                    )
                    .await?;
                for row in &rows {
                    let id: &[u8] = get(row, "id")?;
                    let id = id
                        .try_into()
                        .map_err(|_| corrupt("id", "expected a 32-byte record id"))?;
                    records.push((
                        RecordId::new(id),
                        Record {
                            body: get(row, "body")?,
                            version: RecordVersion::new(unsigned("version", get(row, "version")?)?),
                            expires_at: timestamp("expires_at", get(row, "expires_at")?)?,
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

    async fn apply(
        &self,
        lease: &Lease,
        sequence: RecordBatchSeq,
        version: RecordVersion,
        now: Timestamp,
        changes: &[RecordChange<'_>],
    ) -> Result<ApplyRecords, PortError> {
        validate_record_changes(changes).map_err(PortError::backend)?;
        let sequence = signed("record_sequence", sequence.get())?;
        let version = signed("version", version.get())?;
        let epoch = signed("epoch", lease.epoch.get())?;
        let now = at(now)?;
        Ok(self.transaction(async |tx| {
            // Lock the row even when the supplied authority is stale; all lease mutations
            // serialize against this lock until the transaction commits or rolls back.
            let row = tx.query_typed_opt(
                "SELECT holder, epoch, expires_at, record_sequence FROM leases WHERE name = $1 FOR UPDATE",
                &[(&"leader", Type::TEXT)],
            ).await?;
            let valid = match row {
                Some(row) => {
                    let holder: Option<&str> = get(&row, "holder")?;
                    let stored_epoch: i64 = get(&row, "epoch")?;
                    let expires_at: time::OffsetDateTime = get(&row, "expires_at")?;
                    let record_sequence: i64 = get(&row, "record_sequence")?;
                    holder == Some(lease.holder.as_str()) && stored_epoch == epoch
                        && expires_at > now && record_sequence < sequence
                }
                None => false,
            };
            if !valid {
                return Ok(Finish::Rollback(ApplyRecords::Fenced));
            }
            tx.execute_typed(
                "UPDATE leases SET record_sequence = $1 WHERE name = $2",
                &[(&sequence, Type::INT8), (&"leader", Type::TEXT)],
            ).await?;
            for change in changes {
                match change {
                    RecordChange::Put { key, body, expires_at } => {
                        let expires_at = at(*expires_at)?;
                        tx.execute_typed(
                            "INSERT INTO expiring_records (kind, id, body, version, expires_at) VALUES ($1, $2, $3, $4, $5) \
                             ON CONFLICT (kind, id) DO UPDATE SET body = EXCLUDED.body, version = EXCLUDED.version, expires_at = EXCLUDED.expires_at",
                            &[(&key.kind.as_str(), Type::TEXT), (&key.id.as_bytes().as_slice(), Type::BYTEA), (body, Type::BYTEA), (&version, Type::INT8), (&expires_at, Type::TIMESTAMPTZ)],
                        ).await?;
                    }
                    RecordChange::Delete { key } => {
                        tx.execute_typed(
                            "DELETE FROM expiring_records WHERE kind = $1 AND id = $2",
                            &[(&key.kind.as_str(), Type::TEXT), (&key.id.as_bytes().as_slice(), Type::BYTEA)],
                        ).await?;
                    }
                }
            }
            Ok(Finish::Commit(ApplyRecords::Applied))
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

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::num::{NonZeroU16, NonZeroUsize};
    use std::path::Path;

    use gang_clock::Timestamp;
    use gang_spi::{
        Acquisition, ApplyRecords, ControllerHostIdentity, ControllerId, ExpiringRecordPort,
        LeaderLeasePort, Lease, Plugin, PluginContext, RecordBatchSeq, RecordChange, RecordId,
        RecordKey, RecordKind, RecordVersion, Remove, Renewal, Replace, Takeover,
    };
    use gang_testkit::{SUPERUSER, shared_postgres};
    use gang_types::InstanceId;

    use crate::{Postgres, PostgresConfig, Tls};

    fn key(byte: u8) -> RecordKey {
        RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([byte; 32]),
        }
    }

    #[tokio::test]
    async fn fenced_batches_and_ordered_live_pages() -> Result<(), Box<dyn Error>> {
        let server = shared_postgres()?;
        let name = server.create_database().await?;
        let host = server.socket_dir().to_str().ok_or("invalid socket path")?;
        let mut config = PostgresConfig::new(host, name, SUPERUSER);
        config.port = NonZeroU16::new(server.port()).ok_or("invalid port")?;
        config.tls = Tls::Disable;
        let db = Postgres::build(
            config,
            &PluginContext {
                config_path: Path::new("gang.toml"),
            },
        )?;
        let now = Timestamp::from_unix_millis(10);
        let expiry = Timestamp::from_unix_millis(100);
        let candidate = db
            .register_candidate(
                &ControllerId::new("leader"),
                &ControllerHostIdentity::Persistent(InstanceId::try_new("leader-host")?),
            )
            .await?;
        let Acquisition::Acquired(lease) = db
            .acquire(&candidate, now, now, expiry, Takeover::Observe)
            .await?
        else {
            return Err("lease not acquired".into());
        };
        let lease = initial_batch_and_renewal(&db, lease, now, expiry).await?;
        ordered_pages(&db, now).await?;
        let (lease, update) = rejected_and_valid_batches(&db, lease, now).await?;
        handoff(&db, lease, now, expiry, &update).await
    }

    async fn initial_batch_and_renewal(
        db: &Postgres,
        lease: Lease,
        now: Timestamp,
        expiry: Timestamp,
    ) -> Result<Lease, Box<dyn Error>> {
        let changes = [
            RecordChange::Put {
                key: key(255),
                body: b"last",
                expires_at: expiry,
            },
            RecordChange::Put {
                key: key(0),
                body: b"first",
                expires_at: expiry,
            },
            RecordChange::Put {
                key: key(1),
                body: b"expired",
                expires_at: now,
            },
            RecordChange::Put {
                key: key(128),
                body: b"middle",
                expires_at: expiry,
            },
        ];
        let version = RecordVersion::new(41);
        assert_eq!(
            db.apply(&lease, RecordBatchSeq::new(2), version, now, &changes)
                .await?,
            ApplyRecords::Applied
        );
        let stale = [RecordChange::Delete { key: key(0) }];
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(42),
                now,
                &stale
            )
            .await?,
            ApplyRecords::Fenced
        );
        assert!(db.get(&key(0), now).await?.is_some());
        assert!(matches!(
            db.renew(&lease, now, expiry).await?,
            Renewal::Renewed(_)
        ));
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(43),
                now,
                &stale
            )
            .await?,
            ApplyRecords::Fenced
        );
        Ok(lease)
    }

    async fn ordered_pages(db: &Postgres, now: Timestamp) -> Result<(), Box<dyn Error>> {
        let version = RecordVersion::new(41);
        let mut page = Vec::new();
        let limit = NonZeroUsize::new(1).ok_or("zero limit")?;
        assert!(
            db.records(RecordKind::ResourceHistory, None, limit, now, &mut page)
                .await?
        );
        assert_eq!(page[0].0, key(0).id);
        assert_eq!(page[0].1.version, version);
        assert!(
            db.records(
                RecordKind::ResourceHistory,
                Some(key(0).id),
                limit,
                now,
                &mut page
            )
            .await?
        );
        assert_eq!(page[0].0, key(128).id);
        assert!(
            !db.records(
                RecordKind::ResourceHistory,
                Some(key(128).id),
                limit,
                now,
                &mut page
            )
            .await?
        );
        assert_eq!(page[0].0, key(255).id);
        assert!(
            !db.records(
                RecordKind::ResourceHistory,
                Some(key(255).id),
                limit,
                now,
                &mut page
            )
            .await?
        );
        assert!(page.is_empty());
        Ok(())
    }

    async fn rejected_and_valid_batches(
        db: &Postgres,
        lease: Lease,
        now: Timestamp,
    ) -> Result<(Lease, [RecordChange<'static>; 3]), Box<dyn Error>> {
        let duplicate = [
            RecordChange::Delete { key: key(0) },
            RecordChange::Delete { key: key(0) },
        ];
        assert!(
            db.apply(
                &lease,
                RecordBatchSeq::new(3),
                RecordVersion::new(44),
                now,
                &duplicate
            )
            .await
            .is_err()
        );
        assert!(db.get(&key(0), now).await?.is_some());
        let invalid = [
            RecordChange::Delete { key: key(0) },
            RecordChange::Put {
                key: key(3),
                body: b"invalid",
                expires_at: Timestamp::from_unix_millis(u64::MAX),
            },
        ];
        assert!(
            db.apply(
                &lease,
                RecordBatchSeq::new(3),
                RecordVersion::new(45),
                now,
                &invalid
            )
            .await
            .is_err()
        );
        assert!(db.get(&key(0), now).await?.is_some());
        let update = [
            RecordChange::Put {
                key: key(0),
                body: b"new",
                expires_at: Timestamp::from_unix_millis(50),
            },
            RecordChange::Delete { key: key(1) },
            RecordChange::Delete { key: key(2) },
        ];
        let new_version = RecordVersion::new(46);
        assert_eq!(
            db.apply(&lease, RecordBatchSeq::new(3), new_version, now, &update)
                .await?,
            ApplyRecords::Applied
        );
        let record = db.get(&key(0), now).await?.ok_or("missing record")?;
        assert_eq!(record.body, b"new");
        assert_eq!(record.version, new_version);
        assert_eq!(record.expires_at, Timestamp::from_unix_millis(50));
        Ok((lease, update))
    }

    async fn handoff(
        db: &Postgres,
        lease: Lease,
        now: Timestamp,
        expiry: Timestamp,
        update: &[RecordChange<'_>],
    ) -> Result<(), Box<dyn Error>> {
        let new_version = RecordVersion::new(46);
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(4),
                RecordVersion::new(47),
                expiry,
                &[]
            )
            .await?,
            ApplyRecords::Fenced
        );
        db.release(&lease).await?;
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(4),
                RecordVersion::new(48),
                now,
                update
            )
            .await?,
            ApplyRecords::Fenced
        );
        let candidate = db
            .register_candidate(
                &ControllerId::new("successor"),
                &ControllerHostIdentity::Persistent(InstanceId::try_new("successor-host")?),
            )
            .await?;
        let Acquisition::Acquired(successor) = db
            .acquire(&candidate, now, now, expiry, Takeover::Observe)
            .await?
        else {
            return Err("successor lease not acquired".into());
        };
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(4),
                RecordVersion::new(49),
                now,
                update
            )
            .await?,
            ApplyRecords::Fenced
        );
        let successor_version = RecordVersion::new(50);
        let replacement = [RecordChange::Put {
            key: key(0),
            body: b"successor",
            expires_at: expiry,
        }];
        assert_eq!(
            db.apply(
                &successor,
                RecordBatchSeq::new(1),
                successor_version,
                now,
                &replacement
            )
            .await?,
            ApplyRecords::Applied
        );
        assert_eq!(
            db.replace(&key(0), new_version, b"stale", now, expiry)
                .await?,
            Replace::Stale
        );
        assert_eq!(db.remove(&key(0), new_version, now).await?, Remove::Stale);
        assert_eq!(
            db.get(&key(0), now).await?.ok_or("missing record")?.version,
            successor_version
        );
        Ok(())
    }
}
