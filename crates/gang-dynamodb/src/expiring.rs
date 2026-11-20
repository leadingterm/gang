use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{Delete, Put, TransactWriteItem, Update};
use gang_aws_sdk::call_error;
use gang_clock::Timestamp;
use gang_spi::{
    ApplyRecords, ExpiringRecordPort, Insert, Lease, PortError, Record, RecordBatchSeq,
    RecordChange, RecordId, RecordKey, RecordKind, RecordVersion, Remove, Replace,
    validate_record_changes,
};

use crate::items::{b, bytes, key, n, number, record_pk, record_sk, s, string, timestamp};
use crate::plugin::Transaction;
use crate::repository::checked_timestamp;
use crate::{DynamoDb, DynamoError};

const MILLIS_PER_SECOND: u64 = 1_000;

fn item_key(record: &RecordKey) -> crate::items::Item {
    key(&record_pk(record.kind), &record_sk(&record.id))
}

impl ExpiringRecordPort for DynamoDb {
    async fn records(
        &self,
        kind: RecordKind,
        after: Option<RecordId>,
        limit: NonZeroUsize,
        now: Timestamp,
        into: &mut Vec<(RecordId, Record)>,
    ) -> Result<bool, PortError> {
        into.clear();
        checked_timestamp(now)?;
        self.ensure_schema().await?;
        let pk = record_pk(kind);
        let mut start = None;
        loop {
            let mut query = self
                .client()
                .await
                .query()
                .table_name(self.table())
                .key_condition_expression(if after.is_some() {
                    "#pk = :pk AND #sk > :after"
                } else {
                    "#pk = :pk"
                })
                .expression_attribute_names("#pk", "pk")
                .expression_attribute_values(":pk", s(&pk))
                .filter_expression("#expires > :now")
                .expression_attribute_names("#expires", "expires_at")
                .expression_attribute_values(":now", n(now.as_unix_millis()))
                .consistent_read(true)
                .scan_index_forward(true)
                .limit(i32::try_from(limit.get().saturating_add(1)).unwrap_or(i32::MAX))
                .set_exclusive_start_key(start);
            if let Some(after) = after {
                query = query
                    .expression_attribute_names("#sk", "sk")
                    .expression_attribute_values(":after", s(record_sk(&after)));
            }
            let response = query
                .send()
                .await
                .map_err(|error| call_error("Query", error))?;
            for mut item in response.items.unwrap_or_default() {
                if into.len() == limit.get() {
                    return Ok(true);
                }
                let mut id = [0_u8; 32];
                hex::decode_to_slice(string(&item, "sk")?, &mut id)
                    .map_err(|_| DynamoError::Malformed { attribute: "sk" })?;
                let version = RecordVersion::new(number(&item, "version")?);
                let expires_at = timestamp(&item, "expires_at")?;
                let Some(aws_sdk_dynamodb::types::AttributeValue::B(body)) = item.remove("body")
                else {
                    return Err(DynamoError::Malformed { attribute: "body" }.into());
                };
                into.push((
                    RecordId::new(id),
                    Record {
                        body: body.into_inner(),
                        version,
                        expires_at,
                    },
                ));
            }
            start = response.last_evaluated_key.filter(|key| !key.is_empty());
            if start.is_none() {
                return Ok(false);
            }
        }
    }

    async fn apply(
        &self,
        lease: &Lease,
        sequence: RecordBatchSeq,
        version: RecordVersion,
        now: Timestamp,
        changes: &[RecordChange<'_>],
    ) -> Result<ApplyRecords, PortError> {
        checked_timestamp(now)?;
        validate_record_changes(changes).map_err(PortError::backend)?;
        for change in changes {
            if let RecordChange::Put { expires_at, .. } = change {
                checked_timestamp(*expires_at)?;
            }
        }
        let fence = Update::builder()
            .table_name(self.table())
            .set_key(Some(key("lease", "leader")))
            .update_expression("SET #record_sequence = :seq")
            .condition_expression("#holder = :holder AND #epoch = :epoch AND #expires > :now AND (attribute_not_exists(#record_sequence) OR #record_sequence < :seq)")
            .expression_attribute_names("#holder", "holder")
            .expression_attribute_names("#epoch", "epoch")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_names("#record_sequence", "record_sequence")
            .expression_attribute_values(":holder", s(lease.holder.as_str()))
            .expression_attribute_values(":epoch", n(lease.epoch.get()))
            .expression_attribute_values(":now", n(now.as_unix_millis()))
            .expression_attribute_values(":seq", n(sequence.get()))
            .build().map_err(|_| DynamoError::Malformed { attribute: "lease update" })?;
        let mut writes = Vec::with_capacity(changes.len() + 1);
        writes.push(TransactWriteItem::builder().update(fence).build());
        for change in changes {
            match change {
                RecordChange::Put {
                    key,
                    body,
                    expires_at,
                } => {
                    let mut item = item_key(key);
                    item.insert("body".into(), b(body));
                    item.insert("version".into(), n(version.get()));
                    item.insert("expires_at".into(), n(expires_at.as_unix_millis()));
                    item.insert(
                        "ttl".into(),
                        n(expires_at.as_unix_millis().div_ceil(MILLIS_PER_SECOND)),
                    );
                    let put = Put::builder()
                        .table_name(self.table())
                        .set_item(Some(item))
                        .build()
                        .map_err(|_| DynamoError::Malformed { attribute: "put" })?;
                    writes.push(TransactWriteItem::builder().put(put).build());
                }
                RecordChange::Delete { key } => {
                    let delete = Delete::builder()
                        .table_name(self.table())
                        .set_key(Some(item_key(key)))
                        .build()
                        .map_err(|_| DynamoError::Malformed {
                            attribute: "delete",
                        })?;
                    writes.push(TransactWriteItem::builder().delete(delete).build());
                }
            }
        }
        Ok(match self.transact(writes).await? {
            Transaction::Committed => ApplyRecords::Applied,
            Transaction::ConditionFailed => ApplyRecords::Fenced,
        })
    }

    async fn insert(
        &self,
        records: &[(RecordKey, &[u8])],
        version: RecordVersion,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Insert, PortError> {
        checked_timestamp(now)?;
        checked_timestamp(expires_at)?;
        if records.is_empty() {
            self.ensure_schema().await?;
            return Ok(Insert::Inserted);
        }
        let mut keys = BTreeSet::new();
        if records.iter().any(|(key, _)| !keys.insert(*key)) {
            return Ok(Insert::Exists);
        }
        let mut writes = Vec::with_capacity(records.len());
        for (record, body) in records {
            let mut item = item_key(record);
            item.insert("body".into(), b(body));
            item.insert("version".into(), n(version.get()));
            item.insert("expires_at".into(), n(expires_at.as_unix_millis()));
            item.insert(
                "ttl".into(),
                n(expires_at.as_unix_millis().div_ceil(MILLIS_PER_SECOND)),
            );
            let put = Put::builder()
                .table_name(self.table())
                .set_item(Some(item))
                .condition_expression("attribute_not_exists(pk) OR #expires <= :now")
                .expression_attribute_names("#expires", "expires_at")
                .expression_attribute_values(":now", n(now.as_unix_millis()))
                .build()
                .map_err(|_| DynamoError::Malformed { attribute: "put" })?;
            writes.push(TransactWriteItem::builder().put(put).build());
        }
        Ok(match self.transact(writes).await? {
            Transaction::Committed => Insert::Inserted,
            Transaction::ConditionFailed => Insert::Exists,
        })
    }

    async fn get(&self, record: &RecordKey, now: Timestamp) -> Result<Option<Record>, PortError> {
        checked_timestamp(now)?;
        let Some(item) =
            DynamoDb::get(self, &record_pk(record.kind), &record_sk(&record.id)).await?
        else {
            return Ok(None);
        };
        let expires_at = timestamp(&item, "expires_at")?;
        if now >= expires_at {
            return Ok(None);
        }
        Ok(Some(Record {
            body: bytes(&item, "body")?.to_vec(),
            version: RecordVersion::new(number(&item, "version")?),
            expires_at,
        }))
    }

    async fn replace(
        &self,
        record: &RecordKey,
        version: RecordVersion,
        body: &[u8],
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Replace, PortError> {
        checked_timestamp(now)?;
        checked_timestamp(expires_at)?;
        self.ensure_schema().await?;
        let next = version.get().checked_add(1).ok_or(DynamoError::Malformed {
            attribute: "version",
        })?;
        let result = self
            .client()
            .await
            .update_item()
            .table_name(self.table())
            .set_key(Some(item_key(record)))
            .update_expression(
                "SET #body = :body, #version = :next, #expires = :expires, #ttl = :ttl",
            )
            .condition_expression("#version = :version AND #expires > :now")
            .expression_attribute_names("#body", "body")
            .expression_attribute_names("#version", "version")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":body", b(body))
            .expression_attribute_values(":version", n(version.get()))
            .expression_attribute_values(":next", n(next))
            .expression_attribute_values(":now", n(now.as_unix_millis()))
            .expression_attribute_values(":expires", n(expires_at.as_unix_millis()))
            .expression_attribute_values(
                ":ttl",
                n(expires_at.as_unix_millis().div_ceil(MILLIS_PER_SECOND)),
            )
            .send()
            .await;
        match result {
            Ok(_) => Ok(Replace::Replaced(RecordVersion::new(next))),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(UpdateItemError::is_conditional_check_failed_exception) =>
            {
                Ok(Replace::Stale)
            }
            Err(error) => Err(DynamoError::from(call_error("UpdateItem", error)).into()),
        }
    }

    async fn remove(
        &self,
        record: &RecordKey,
        version: RecordVersion,
        now: Timestamp,
    ) -> Result<Remove, PortError> {
        checked_timestamp(now)?;
        self.ensure_schema().await?;
        let result = self
            .client()
            .await
            .delete_item()
            .table_name(self.table())
            .set_key(Some(item_key(record)))
            .condition_expression("#version = :version AND #expires > :now")
            .expression_attribute_names("#version", "version")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_values(":version", n(version.get()))
            .expression_attribute_values(":now", n(now.as_unix_millis()))
            .send()
            .await;
        match result {
            Ok(_) => Ok(Remove::Removed),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(DeleteItemError::is_conditional_check_failed_exception) =>
            {
                Ok(Remove::Stale)
            }
            Err(error) => Err(DynamoError::from(call_error("DeleteItem", error)).into()),
        }
    }

    // DynamoDB TTL eventually deletes expired records; reads exclude them immediately.
    async fn purge(&self, now: Timestamp) -> Result<(), PortError> {
        checked_timestamp(now)?;
        self.ensure_schema().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use aws_credential_types::Credentials;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Region};
    use gang_clock::SystemClock;
    use gang_spi::{
        Acquisition, ControllerHostIdentity, ControllerId, LeaderLeasePort, LeaseCandidate,
        Takeover,
    };
    use gang_testkit::{FakeDynamoDb, TestResult};
    use gang_types::InstanceId;

    async fn setup() -> Result<(DynamoDb, FakeDynamoDb), Box<dyn std::error::Error>> {
        let fake = FakeDynamoDb::start().await?;
        let endpoint = fake.endpoint();
        let client = aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "gang-test"))
                .http_client(gang_aws_sdk::http_client())
                .endpoint_url(endpoint.as_str())
                .build(),
        );
        let config = crate::DynamoDbConfig {
            table: crate::TableName::try_new("gang-test")?,
            region: gang_aws_sdk::Region::try_new("us-east-1")?,
            endpoint: Some(endpoint),
            client: gang_aws_sdk::ClientSettings::default(),
        };
        Ok((
            DynamoDb::with_client(config, client, Arc::new(SystemClock::new())),
            fake,
        ))
    }

    fn time(ms: u64) -> Timestamp {
        Timestamp::from_unix_millis(ms)
    }

    fn record(id: u8) -> RecordKey {
        RecordKey {
            kind: RecordKind::ResourceHistory,
            id: RecordId::new([id; 32]),
        }
    }

    async fn candidate(
        db: &DynamoDb,
        name: &str,
    ) -> Result<LeaseCandidate, Box<dyn std::error::Error>> {
        Ok(db
            .register_candidate(
                &ControllerId::new(name),
                &ControllerHostIdentity::Persistent(InstanceId::try_new(name)?),
            )
            .await?)
    }

    #[tokio::test]
    async fn records_skip_expired_pages_and_report_exact_more() -> TestResult {
        let (db, _fake) = setup().await?;
        let version = RecordVersion::new(3);
        for id in 0..6 {
            let expires = if id < 4 { time(100) } else { time(101) };
            db.insert(&[(record(id), b"v")], version, time(1), expires)
                .await?;
        }
        let limit = NonZeroUsize::new(1).ok_or("nonzero limit")?;
        let mut page = Vec::new();
        assert!(
            db.records(
                RecordKind::ResourceHistory,
                None,
                limit,
                time(100),
                &mut page
            )
            .await?
        );
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0, record(4).id);
        assert!(
            !db.records(
                RecordKind::ResourceHistory,
                Some(page[0].0),
                limit,
                time(100),
                &mut page
            )
            .await?
        );
        assert_eq!(page[0].0, record(5).id);
        assert!(
            !db.records(
                RecordKind::ResourceHistory,
                Some(page[0].0),
                limit,
                time(100),
                &mut page
            )
            .await?
        );
        assert!(page.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn batches_are_fenced_atomic_and_preserve_expiry() -> TestResult {
        let (db, _fake) = setup().await?;
        let owner = candidate(&db, "first").await?;
        let Acquisition::Acquired(lease) = db
            .acquire(&owner, time(0), time(0), time(100), Takeover::Observe)
            .await?
        else {
            return Err("lease not acquired".into());
        };
        let version = RecordVersion::new(42);
        let first = record(1);
        let second = record(2);
        let changes = [
            RecordChange::Put {
                key: first,
                body: b"one",
                expires_at: time(1501),
            },
            RecordChange::Delete { key: second },
        ];
        assert_eq!(
            db.apply(&lease, RecordBatchSeq::new(1), version, time(1), &changes)
                .await?,
            ApplyRecords::Applied
        );
        assert_record_expiry(&db, &first, version).await?;
        assert_eq!(
            db.apply(&lease, RecordBatchSeq::new(2), version, time(100), &[])
                .await?,
            ApplyRecords::Fenced
        );
        let replacement = candidate(&db, "second").await?;
        let Acquisition::Acquired(next) = db
            .acquire(
                &replacement,
                time(101),
                time(101),
                time(200),
                Takeover::Observe,
            )
            .await?
        else {
            return Err("successor not acquired".into());
        };
        let stale = [
            RecordChange::Delete { key: first },
            RecordChange::Put {
                key: second,
                body: b"two",
                expires_at: time(1501),
            },
        ];
        assert_eq!(
            db.apply(&lease, RecordBatchSeq::new(2), version, time(101), &stale)
                .await?,
            ApplyRecords::Fenced
        );
        assert!(
            ExpiringRecordPort::get(&db, &first, time(101))
                .await?
                .is_some()
        );
        assert!(
            ExpiringRecordPort::get(&db, &second, time(101))
                .await?
                .is_none()
        );
        assert!(
            db.apply(
                &next,
                RecordBatchSeq::new(1),
                version,
                time(101),
                &[stale[0], stale[0]]
            )
            .await
            .is_err()
        );
        assert!(
            ExpiringRecordPort::get(&db, &first, time(101))
                .await?
                .is_some()
        );
        assert_eq!(
            db.apply(&next, RecordBatchSeq::new(1), version, time(101), &stale)
                .await?,
            ApplyRecords::Applied
        );
        assert_eq!(
            db.apply(
                &next,
                RecordBatchSeq::new(2),
                RecordVersion::new(43),
                time(101),
                &[RecordChange::Delete { key: first }]
            )
            .await?,
            ApplyRecords::Applied
        );
        Ok(())
    }

    async fn assert_record_expiry(
        db: &DynamoDb,
        record: &RecordKey,
        version: RecordVersion,
    ) -> TestResult {
        let item = ExpiringRecordPort::get(db, record, time(1))
            .await?
            .ok_or("missing record")?;
        assert_eq!(item.expires_at, time(1501));
        assert_eq!(item.version, version);
        let raw = DynamoDb::get(db, &record_pk(record.kind), &record_sk(&record.id))
            .await?
            .ok_or("missing item")?;
        assert_eq!(number(&raw, "ttl")?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn batch_sequence_survives_renewal_and_resets_on_acquisition() -> TestResult {
        let (db, _fake) = setup().await?;
        let owner = candidate(&db, "first").await?;
        let Acquisition::Acquired(lease) = db
            .acquire(&owner, time(0), time(0), time(100), Takeover::Observe)
            .await?
        else {
            return Err("lease not acquired".into());
        };
        let first = record(1);
        let second = record(2);
        let high = [RecordChange::Put {
            key: first,
            body: b"new",
            expires_at: time(1000),
        }];
        let low = [
            RecordChange::Delete { key: first },
            RecordChange::Put {
                key: second,
                body: b"old",
                expires_at: time(1000),
            },
        ];
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(41),
                time(1),
                &high
            )
            .await?,
            ApplyRecords::Applied
        );
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(42),
                time(1),
                &low
            )
            .await?,
            ApplyRecords::Fenced
        );
        assert_eq!(
            ExpiringRecordPort::get(&db, &first, time(1))
                .await?
                .ok_or("missing record")?
                .body,
            b"new"
        );
        assert!(
            ExpiringRecordPort::get(&db, &second, time(1))
                .await?
                .is_none()
        );
        assert!(matches!(
            db.renew(&lease, time(1), time(200)).await?,
            gang_spi::Renewal::Renewed(_)
        ));
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(43),
                time(1),
                &low
            )
            .await?,
            ApplyRecords::Fenced
        );
        assert!(
            db.apply(
                &lease,
                RecordBatchSeq::new(3),
                RecordVersion::new(44),
                time(1),
                &[low[0], low[0]]
            )
            .await
            .is_err()
        );
        assert_eq!(
            db.apply(
                &lease,
                RecordBatchSeq::new(3),
                RecordVersion::new(45),
                time(1),
                &[]
            )
            .await?,
            ApplyRecords::Applied
        );
        successor_epoch_resets_sequence(&db, first, second, &low).await
    }

    async fn successor_epoch_resets_sequence(
        db: &DynamoDb,
        first: RecordKey,
        second: RecordKey,
        low: &[RecordChange<'_>],
    ) -> TestResult {
        let replacement = candidate(db, "second").await?;
        let Acquisition::Acquired(next) = db
            .acquire(
                &replacement,
                time(201),
                time(201),
                time(300),
                Takeover::Observe,
            )
            .await?
        else {
            return Err("successor not acquired".into());
        };
        assert_eq!(
            db.apply(
                &next,
                RecordBatchSeq::new(1),
                RecordVersion::new(46),
                time(201),
                low
            )
            .await?,
            ApplyRecords::Applied
        );
        assert!(
            ExpiringRecordPort::get(db, &first, time(201))
                .await?
                .is_none()
        );
        assert_eq!(
            ExpiringRecordPort::get(db, &second, time(201))
                .await?
                .ok_or("missing record")?
                .body,
            b"old"
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_epoch_record_version_fences_old_cas() -> TestResult {
        let (db, _fake) = setup().await?;
        let owner = candidate(&db, "first").await?;
        let Acquisition::Acquired(first) = db
            .acquire(&owner, time(0), time(0), time(100), Takeover::Observe)
            .await?
        else {
            return Err("lease not acquired".into());
        };
        let key = record(1);
        let old = RecordVersion::new(41);
        let new = RecordVersion::new(42);
        let put = [RecordChange::Put {
            key,
            body: b"old",
            expires_at: time(1000),
        }];
        assert_eq!(
            db.apply(&first, RecordBatchSeq::new(1), old, time(1), &put)
                .await?,
            ApplyRecords::Applied
        );
        let replacement = candidate(&db, "second").await?;
        let Acquisition::Acquired(next) = db
            .acquire(
                &replacement,
                time(101),
                time(101),
                time(200),
                Takeover::Observe,
            )
            .await?
        else {
            return Err("successor not acquired".into());
        };
        let put = [RecordChange::Put {
            key,
            body: b"new",
            expires_at: time(1000),
        }];
        assert_eq!(
            db.apply(&next, RecordBatchSeq::new(1), new, time(101), &put)
                .await?,
            ApplyRecords::Applied
        );
        assert_eq!(
            db.replace(&key, old, b"stale", time(101), time(1000))
                .await?,
            Replace::Stale
        );
        assert_eq!(db.remove(&key, old, time(101)).await?, Remove::Stale);
        let stored = ExpiringRecordPort::get(&db, &key, time(101))
            .await?
            .ok_or("missing record")?;
        assert_eq!(stored.version, new);
        assert_eq!(stored.body, b"new");
        Ok(())
    }

    #[tokio::test]
    async fn oversized_batches_are_rejected_without_writes() -> TestResult {
        let (db, _fake) = setup().await?;
        let owner = candidate(&db, "leader").await?;
        let Acquisition::Acquired(lease) = db
            .acquire(&owner, time(0), time(0), time(100), Takeover::Observe)
            .await?
        else {
            return Err("lease not acquired".into());
        };
        let oversized = vec![0_u8; gang_spi::MAX_RECORD_BODY_BYTES_PER_BATCH + 1];
        let put = RecordChange::Put {
            key: record(1),
            body: &oversized,
            expires_at: time(200),
        };
        assert!(
            db.apply(
                &lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(41),
                time(1),
                &[put]
            )
            .await
            .is_err()
        );
        let deletes: Vec<_> = (0..100)
            .map(|id| RecordChange::Delete { key: record(id) })
            .collect();
        assert!(
            db.apply(
                &lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(41),
                time(1),
                &deletes
            )
            .await
            .is_err()
        );
        assert!(
            ExpiringRecordPort::get(&db, &record(1), time(1))
                .await?
                .is_none()
        );
        Ok(())
    }
}
