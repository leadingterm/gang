use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::ReturnValue;
use gang_aws_sdk::call_error;
use gang_clock::Timestamp;
use gang_spi::{Acquisition, ControllerId, LeaderLeasePort, Lease, LeaseEpoch, PortError, Renewal};

use crate::items::{Item, key, n, number, s, string, timestamp};
use crate::repository::checked_timestamp;
use crate::{DynamoDb, DynamoError};

const LEASE_PK: &str = "lease";
const LEASE_SK: &str = "leader";
const UPDATE_ITEM: &str = "UpdateItem";

fn decode(item: &Item) -> Result<Lease, DynamoError> {
    Ok(Lease {
        holder: ControllerId::new(string(item, "holder")?),
        epoch: LeaseEpoch::new(number(item, "epoch")?),
        expires_at: timestamp(item, "expires_at")?,
    })
}

impl LeaderLeasePort for DynamoDb {
    async fn acquire(
        &self,
        holder: &ControllerId,
        stale_before: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Acquisition, PortError> {
        checked_timestamp(stale_before)?;
        checked_timestamp(expires_at)?;
        self.ensure_schema().await?;
        loop {
            let result = self.client().await.update_item().table_name(self.table())
                .set_key(Some(key(LEASE_PK, LEASE_SK)))
                .update_expression("SET #holder = :holder, #expires = :expires ADD #epoch :one")
                .condition_expression("attribute_not_exists(pk) OR attribute_not_exists(#holder) OR #expires < :stale")
                .expression_attribute_names("#holder", "holder").expression_attribute_names("#expires", "expires_at")
                .expression_attribute_names("#epoch", "epoch")
                .expression_attribute_values(":holder", s(holder.as_str()))
                .expression_attribute_values(":expires", n(expires_at.as_unix_millis()))
                .expression_attribute_values(":one", n(1))
                .expression_attribute_values(":stale", n(stale_before.as_unix_millis()))
                .return_values(ReturnValue::AllNew).send().await;
            match result {
                Ok(output) => {
                    let item = output.attributes.as_ref().ok_or(DynamoError::Malformed {
                        attribute: "attributes",
                    })?;
                    return Ok(Acquisition::Acquired(decode(item)?));
                }
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(UpdateItemError::is_conditional_check_failed_exception) =>
                {
                    if let Some(item) = self.get(LEASE_PK, LEASE_SK).await?
                        && item.contains_key("holder")
                    {
                        let lease = decode(&item)?;
                        if lease.expires_at >= stale_before {
                            return Ok(Acquisition::Held(lease));
                        }
                    }
                }
                Err(error) => return Err(DynamoError::from(call_error(UPDATE_ITEM, error)).into()),
            }
        }
    }

    async fn renew(&self, lease: &Lease, expires_at: Timestamp) -> Result<Renewal, PortError> {
        checked_timestamp(expires_at)?;
        self.ensure_schema().await?;
        let result = self
            .client()
            .await
            .update_item()
            .table_name(self.table())
            .set_key(Some(key(LEASE_PK, LEASE_SK)))
            .update_expression("SET #expires = :expires")
            .condition_expression("#holder = :holder AND #epoch = :epoch")
            .expression_attribute_names("#holder", "holder")
            .expression_attribute_names("#epoch", "epoch")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_values(":holder", s(lease.holder.as_str()))
            .expression_attribute_values(":epoch", n(lease.epoch.get()))
            .expression_attribute_values(":expires", n(expires_at.as_unix_millis()))
            .send()
            .await;
        match result {
            Ok(_) => Ok(Renewal::Renewed(Lease {
                expires_at,
                ..lease.clone()
            })),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(UpdateItemError::is_conditional_check_failed_exception) =>
            {
                Ok(Renewal::Lost)
            }
            Err(error) => Err(DynamoError::from(call_error(UPDATE_ITEM, error)).into()),
        }
    }

    async fn release(&self, lease: &Lease) -> Result<(), PortError> {
        self.ensure_schema().await?;
        let result = self
            .client()
            .await
            .update_item()
            .table_name(self.table())
            .set_key(Some(key(LEASE_PK, LEASE_SK)))
            .update_expression("REMOVE #holder")
            .condition_expression("#holder = :holder AND #epoch = :epoch")
            .expression_attribute_names("#holder", "holder")
            .expression_attribute_names("#epoch", "epoch")
            .expression_attribute_values(":holder", s(lease.holder.as_str()))
            .expression_attribute_values(":epoch", n(lease.epoch.get()))
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(UpdateItemError::is_conditional_check_failed_exception) =>
            {
                Ok(())
            }
            Err(error) => Err(DynamoError::from(call_error(UPDATE_ITEM, error)).into()),
        }
    }
}
