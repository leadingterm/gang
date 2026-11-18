use std::collections::BTreeSet;

use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{Put, TransactWriteItem};
use gang_aws_sdk::call_error;
use gang_clock::Timestamp;
use gang_spi::{
    ExpiringRecordPort, Insert, PortError, Record, RecordKey, RecordVersion, Remove, Replace,
};

use crate::items::{b, bytes, key, n, number, record_pk, record_sk, timestamp};
use crate::plugin::Transaction;
use crate::repository::checked_timestamp;
use crate::{DynamoDb, DynamoError};

const MILLIS_PER_SECOND: u64 = 1_000;

fn item_key(record: &RecordKey) -> crate::items::Item {
    key(&record_pk(record.kind), &record_sk(&record.id))
}

impl ExpiringRecordPort for DynamoDb {
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
