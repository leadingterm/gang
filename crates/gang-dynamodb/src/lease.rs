use std::collections::HashMap;

use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::ReturnValue;
use gang_aws_sdk::call_error;
use gang_clock::Timestamp;
use gang_spi::{
    Acquisition, CandidateGeneration, CandidatePriority, CandidatePriorityRef,
    ControllerHostIdentity, ControllerId, LeaderLeasePort, Lease, LeaseCandidate, LeaseEpoch,
    PortError, Renewal, Takeover,
};

use crate::items::{Item, key, n, number, optional_number, s, string, timestamp};
use crate::repository::checked_timestamp;
use crate::{DynamoDb, DynamoError};

const LEASE_PK: &str = "lease";
const LEASE_SK: &str = "leader";
const COUNTER_PK: &str = "counter";
const CANDIDATE_SK: &str = "lease_candidate";
const HOST_PK: &str = "controller_host";
const UPDATE_ITEM: &str = "UpdateItem";
const SNAPSHOT: [(&str, &str, &str); 8] = [
    ("holder", "#oldholder", ":oldholder"),
    ("epoch", "#oldepoch", ":oldepoch"),
    ("expires_at", "#oldexpires", ":oldexpires"),
    ("holder_priority", "#oldownerpriority", ":oldownerpriority"),
    ("holder_priority_epoch", "#oldbinding", ":oldbinding"),
    ("successor_holder", "#oldsuccessor", ":oldsuccessor"),
    ("successor_priority", "#oldpriority", ":oldpriority"),
    ("successor_epoch", "#oldtarget", ":oldtarget"),
];

fn decode(item: &Item) -> Result<Lease, DynamoError> {
    Ok(Lease {
        holder: ControllerId::new(string(item, "holder")?),
        epoch: LeaseEpoch::new(number(item, "epoch")?),
        expires_at: timestamp(item, "expires_at")?,
    })
}

fn priority<'a>(
    item: &'a Item,
    attribute: &'static str,
) -> Result<CandidatePriorityRef<'a>, DynamoError> {
    CandidatePriorityRef::try_from(string(item, attribute)?)
        .map_err(|_| DynamoError::Malformed { attribute })
}

fn holder_priority(
    item: &Item,
    epoch: u64,
) -> Result<Option<CandidatePriorityRef<'_>>, DynamoError> {
    if optional_number(item, "holder_priority_epoch")? == Some(epoch)
        && item.contains_key("holder_priority")
    {
        Ok(Some(priority(item, "holder_priority")?))
    } else {
        Ok(None)
    }
}

fn successor(
    item: &Item,
    epoch: u64,
    now: Timestamp,
) -> Result<Option<(&str, CandidatePriorityRef<'_>)>, DynamoError> {
    if optional_number(item, "successor_epoch")? != Some(epoch)
        || optional_number(item, "successor_expires_at")?.unwrap_or(0) <= now.as_unix_millis()
    {
        return Ok(None);
    }
    Ok(Some((
        string(item, "successor_holder")?,
        priority(item, "successor_priority")?,
    )))
}

// Compare every field used to make the decision, including renewal and pending expiry.
fn snapshot_condition(item: &Item) -> (String, HashMap<String, String>, Item) {
    let mut conditions = String::with_capacity(400);
    let mut names = HashMap::new();
    let mut values = Item::new();
    for (attribute, name, value) in
        SNAPSHOT
            .into_iter()
            .chain([("successor_expires_at", "#olddeadline", ":olddeadline")])
    {
        if !conditions.is_empty() {
            conditions.push_str(" AND ");
        }
        names.insert(name.to_owned(), attribute.to_owned());
        if let Some(observed) = item.get(attribute) {
            conditions.push_str(name);
            conditions.push_str(" = ");
            conditions.push_str(value);
            values.insert(value.to_owned(), observed.clone());
        } else {
            conditions.push_str("attribute_not_exists(");
            conditions.push_str(name);
            conditions.push(')');
        }
    }
    (conditions, names, values)
}

impl DynamoDb {
    async fn update_lease_snapshot(
        &self,
        item: &Item,
        expression: &str,
        mut update_values: Item,
        free: bool,
    ) -> Result<Option<Item>, PortError> {
        let (condition, mut names, values) = snapshot_condition(item);
        names.extend([
            ("#successor".into(), "successor_holder".into()),
            ("#priority".into(), "successor_priority".into()),
            ("#target".into(), "successor_epoch".into()),
            ("#deadline".into(), "successor_expires_at".into()),
        ]);
        if free {
            names.extend([
                ("#holder".into(), "holder".into()),
                ("#epoch".into(), "epoch".into()),
                ("#expires".into(), "expires_at".into()),
                ("#ownerpriority".into(), "holder_priority".into()),
                ("#binding".into(), "holder_priority_epoch".into()),
                ("#record_sequence".into(), "record_sequence".into()),
            ]);
        }
        update_values.extend(values);
        let result = self
            .client()
            .await
            .update_item()
            .table_name(self.table())
            .set_key(Some(key(LEASE_PK, LEASE_SK)))
            .update_expression(expression)
            .condition_expression(condition)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(update_values))
            .return_values(ReturnValue::AllNew)
            .send()
            .await;
        match result {
            Ok(output) => Ok(Some(output.attributes.ok_or(DynamoError::Malformed {
                attribute: "attributes",
            })?)),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(UpdateItemError::is_conditional_check_failed_exception) =>
            {
                Ok(None)
            }
            Err(error) => Err(DynamoError::from(call_error(UPDATE_ITEM, error)).into()),
        }
    }

    async fn persistent_generation(
        &self,
        host: &gang_types::InstanceId,
    ) -> Result<CandidateGeneration, PortError> {
        if let Some(item) = self.get(HOST_PK, host.as_str()).await? {
            return Ok(CandidateGeneration::new(number(&item, "generation")?));
        }
        let result = self
            .client()
            .await
            .update_item()
            .table_name(self.table())
            .set_key(Some(key(COUNTER_PK, CANDIDATE_SK)))
            .update_expression("ADD #value :one")
            .condition_expression("attribute_not_exists(#value) OR #value < :max")
            .expression_attribute_names("#value", "value")
            .expression_attribute_values(":one", n(1))
            .expression_attribute_values(":max", n(u64::MAX))
            .return_values(ReturnValue::AllNew)
            .send()
            .await;
        let generation = match result {
            Ok(output) => {
                let item = output.attributes.as_ref().ok_or(DynamoError::Malformed {
                    attribute: "attributes",
                })?;
                CandidateGeneration::new(number(item, "value")?)
            }
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(UpdateItemError::is_conditional_check_failed_exception) =>
            {
                return Err(DynamoError::CounterExhausted {
                    counter: "candidate generation",
                }
                .into());
            }
            Err(error) => return Err(DynamoError::from(call_error(UPDATE_ITEM, error)).into()),
        };
        let mut item = key(HOST_PK, host.as_str());
        item.insert("generation".into(), n(generation.get()));
        let result = self
            .client()
            .await
            .put_item()
            .table_name(self.table())
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        match result {
            Ok(_) => Ok(generation),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(PutItemError::is_conditional_check_failed_exception) =>
            {
                let item =
                    self.get(HOST_PK, host.as_str())
                        .await?
                        .ok_or(DynamoError::Malformed {
                            attribute: "generation",
                        })?;
                Ok(CandidateGeneration::new(number(&item, "generation")?))
            }
            Err(error) => Err(DynamoError::from(call_error("PutItem", error)).into()),
        }
    }
}

impl LeaderLeasePort for DynamoDb {
    async fn register_candidate(
        &self,
        holder: &ControllerId,
        host: &ControllerHostIdentity,
    ) -> Result<LeaseCandidate, PortError> {
        let priority = match host {
            ControllerHostIdentity::Persistent(instance) => {
                CandidatePriority::registered(self.persistent_generation(instance).await?)
            }
            ControllerHostIdentity::Launched { at, instance } => {
                self.get(LEASE_PK, LEASE_SK).await?;
                CandidatePriority::launched(*at, instance)
            }
        };
        Ok(LeaseCandidate {
            holder: holder.clone(),
            priority,
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
        checked_timestamp(now)?;
        checked_timestamp(stale_before)?;
        checked_timestamp(expires_at)?;
        self.ensure_schema().await?;
        loop {
            let item = self.get(LEASE_PK, LEASE_SK).await?.unwrap_or_default();
            let epoch = optional_number(&item, "epoch")?.unwrap_or(0);
            let pending = successor(&item, epoch, now)?;
            let holder = if item.contains_key("holder") {
                Some(decode(&item)?)
            } else {
                None
            };
            let free = holder
                .as_ref()
                .is_none_or(|lease| lease.expires_at < stale_before);
            let priority = candidate.priority.view();
            let can_reserve = pending.is_none_or(|(id, rank)| {
                (id == candidate.holder.as_str() && rank == priority)
                    || (matches!(takeover, Takeover::Request) && priority > rank)
            });
            let (expression, update_values) = if free {
                if !can_reserve {
                    return Ok(Acquisition::Reserved);
                }
                let next = epoch.checked_add(1).ok_or(DynamoError::CounterExhausted {
                    counter: "lease epoch",
                })?;
                let values = Item::from([
                    (":holder".into(), s(candidate.holder.as_str())),
                    (":epoch".into(), n(next)),
                    (":expires".into(), n(expires_at.as_unix_millis())),
                    (":priority".into(), s(candidate.priority.as_str())),
                    (":zero".into(), n(0)),
                ]);
                (
                    "SET #holder = :holder, #epoch = :epoch, #expires = :expires, #ownerpriority = :priority, #binding = :epoch, #record_sequence = :zero REMOVE #successor, #priority, #target, #deadline",
                    values,
                )
            } else {
                let lease = holder.as_ref().ok_or(DynamoError::Malformed {
                    attribute: "holder",
                })?;
                let newer = candidate.holder != lease.holder
                    && Some(priority) > holder_priority(&item, epoch)?;
                if !newer || !matches!(takeover, Takeover::Request) || !can_reserve {
                    return Ok(if newer && lease.expires_at > now {
                        Acquisition::Contending(lease.clone())
                    } else {
                        Acquisition::Held(lease.clone())
                    });
                }
                let values = Item::from([
                    (":holder".into(), s(candidate.holder.as_str())),
                    (":priority".into(), s(candidate.priority.as_str())),
                    (":epoch".into(), n(epoch)),
                    (":expires".into(), n(expires_at.as_unix_millis())),
                ]);
                (
                    "SET #successor = :holder, #priority = :priority, #target = :epoch, #deadline = :expires",
                    values,
                )
            };
            if let Some(updated) = self
                .update_lease_snapshot(&item, expression, update_values, free)
                .await?
            {
                if free {
                    return Ok(Acquisition::Acquired(decode(&updated)?));
                }
                let lease = holder.ok_or(DynamoError::Malformed {
                    attribute: "holder",
                })?;
                return Ok(if lease.expires_at > now {
                    Acquisition::Contending(lease)
                } else {
                    Acquisition::Held(lease)
                });
            }
        }
    }

    async fn renew(
        &self,
        lease: &Lease,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Renewal, PortError> {
        checked_timestamp(now)?;
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
            .return_values(ReturnValue::AllNew)
            .send()
            .await;
        match result {
            Ok(output) => {
                let item = output.attributes.as_ref().ok_or(DynamoError::Malformed {
                    attribute: "attributes",
                })?;
                let updated = decode(item)?;
                let owner_priority = holder_priority(item, lease.epoch.get())?;
                let requested =
                    successor(item, lease.epoch.get(), now)?.is_some_and(|(id, rank)| {
                        id != lease.holder.as_str() && Some(rank) > owner_priority
                    });
                Ok(if requested {
                    Renewal::YieldRequested(updated)
                } else {
                    Renewal::Renewed(updated)
                })
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_captures_present_and_absent_decision_fields() -> gang_testkit::TestResult {
        let attributes = [
            "holder",
            "epoch",
            "expires_at",
            "holder_priority",
            "holder_priority_epoch",
            "successor_holder",
            "successor_priority",
            "successor_epoch",
            "successor_expires_at",
        ];
        let mut item = Item::new();
        for (index, attribute) in attributes.into_iter().enumerate() {
            item.insert(attribute.into(), n(index));
        }
        let (condition, names, values) = snapshot_condition(&item);
        assert_eq!(names.len(), attributes.len());
        assert_eq!(values.len(), attributes.len());
        for attribute in attributes {
            let name = names
                .iter()
                .find_map(|(name, value)| (value == attribute).then_some(name))
                .ok_or("decision field missing a snapshot name")?;
            let expected = item.get(attribute).ok_or("decision field missing")?;
            let value = values
                .iter()
                .find_map(|(name, value)| (value == expected).then_some(name))
                .ok_or("observed field missing a snapshot value")?;
            assert!(condition.contains(&format!("{name} = {value}")));
        }
        let (condition, names, values) = snapshot_condition(&Item::new());
        assert_eq!(names.len(), attributes.len());
        assert!(values.is_empty());
        for name in names.keys() {
            assert!(condition.contains(&format!("attribute_not_exists({name})")));
        }
        Ok(())
    }

    #[test]
    fn priority_decoding_respects_epoch_and_request_expiry() -> gang_testkit::TestResult {
        let mut item = Item::from([
            ("holder_priority".into(), s("invalid")),
            ("holder_priority_epoch".into(), n(1)),
            ("successor_priority".into(), s("invalid")),
            ("successor_epoch".into(), n(1)),
            ("successor_expires_at".into(), n(10)),
        ]);
        assert_eq!(holder_priority(&item, 2)?, None);
        assert!(holder_priority(&item, 1).is_err());
        assert!(successor(&item, 2, Timestamp::from_unix_millis(1))?.is_none());
        assert!(successor(&item, 1, Timestamp::from_unix_millis(10))?.is_none());
        item.insert("successor_holder".into(), s("candidate"));
        assert!(successor(&item, 1, Timestamp::from_unix_millis(1)).is_err());
        item.remove("holder_priority");
        assert_eq!(holder_priority(&item, 1)?, None);
        Ok(())
    }
}
