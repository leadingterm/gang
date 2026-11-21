use std::num::NonZeroUsize;

use aws_sdk_dynamodb::types::{Delete, Put, TransactWriteItem, Update};
use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PortError, PrincipalRecord, RecordedScalingEvent,
    RevocationEpoch, ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, StateRepositoryPort,
    StoredScalingKind, Subject,
};
use gang_types::{CertSerial, FleetName, Identity, Principal, ProviderName, ScalingCause};

use crate::errors::DynamoError;
use crate::items::{
    Item, identity_key, key, n, number, optional_timestamp, padded, s, serial_key, string,
    timestamp,
};
use crate::plugin::{DynamoDb, Transaction};

const CERT: &str = "cert";
const PRINCIPAL: &str = "principal";
const REVOKED_CERT: &str = "revoked-cert";
const REVOKED_PRINCIPAL: &str = "revoked-principal";
const AUDIT: &str = "audit";
const COUNTER: &str = "counter";
const REVOCATION: &str = "revocation";

fn malformed(attribute: &'static str) -> DynamoError {
    DynamoError::Malformed { attribute }
}

pub(crate) fn checked_timestamp(at: Timestamp) -> Result<(), DynamoError> {
    i64::try_from(at.as_unix_millis())
        .map(|_| ())
        .map_err(|_| malformed("timestamp"))
}

fn checked_sequence(seq: ScalingEventSeq) -> Result<(), DynamoError> {
    i64::try_from(seq.get())
        .map(|_| ())
        .map_err(|_| malformed("sequence"))
}

fn identity(item: &Item) -> Result<Identity, DynamoError> {
    Ok(Identity::new(
        ProviderName::try_new(string(item, "provider")?).map_err(|_| malformed("provider"))?,
        Principal::try_new(string(item, "principal")?).map_err(|_| malformed("principal"))?,
    ))
}

fn identity_fields(item: &mut Item, identity: &Identity) {
    item.insert("provider".into(), s(identity.provider.as_str()));
    item.insert("principal".into(), s(identity.principal.as_str()));
}

fn certificate(item: &Item) -> Result<IssuedCertRecord, DynamoError> {
    Ok(IssuedCertRecord {
        serial: CertSerial::try_new(string(item, "serial")?).map_err(|_| malformed("serial"))?,
        identity: identity(item)?,
        issued_at: timestamp(item, "issued_at")?,
        expires_at: timestamp(item, "expires_at")?,
        status: optional_timestamp(item, "revoked_at")?
            .map_or(CertStatus::Active, |at| CertStatus::Revoked { at }),
    })
}

fn certificate_item(pk: &str, sk: &str, record: &IssuedCertRecord) -> Item {
    let mut item = key(pk, sk);
    item.insert("serial".into(), s(record.serial.as_str()));
    identity_fields(&mut item, &record.identity);
    item.insert("issued_at".into(), n(record.issued_at.as_unix_millis()));
    item.insert("expires_at".into(), n(record.expires_at.as_unix_millis()));
    if let CertStatus::Revoked { at } = record.status {
        item.insert("revoked_at".into(), n(at.as_unix_millis()));
    }
    item
}

fn principal(item: &Item) -> Result<PrincipalRecord, DynamoError> {
    Ok(PrincipalRecord {
        identity: identity(item)?,
        first_seen: timestamp(item, "first_seen")?,
        last_seen: timestamp(item, "last_seen")?,
        revoked_at: optional_timestamp(item, "revoked_at")?,
    })
}

fn principal_item(pk: &str, sk: &str, record: &PrincipalRecord) -> Item {
    let mut item = key(pk, sk);
    identity_fields(&mut item, &record.identity);
    item.insert("first_seen".into(), n(record.first_seen.as_unix_millis()));
    item.insert("last_seen".into(), n(record.last_seen.as_unix_millis()));
    if let Some(at) = record.revoked_at {
        item.insert("revoked_at".into(), n(at.as_unix_millis()));
    }
    item
}

fn timed_key(at: Timestamp, suffix: &str) -> String {
    format!("{}#{suffix}", padded(at.as_unix_millis()))
}

fn identity_partition(identity: &Identity) -> String {
    format!("ident#{}", identity_key(identity))
}

fn capacity(item: &Item, name: &'static str) -> Result<Option<u32>, DynamoError> {
    item.contains_key(name)
        .then(|| u32::try_from(number(item, name)?).map_err(|_| malformed(name)))
        .transpose()
}

fn scaling(item: &Item) -> Result<RecordedScalingEvent, DynamoError> {
    let optional_text = |name| {
        item.contains_key(name)
            .then(|| string(item, name))
            .transpose()
    };
    let kind = ScalingEventKind::try_from(StoredScalingKind {
        name: string(item, "kind")?,
        from_capacity: capacity(item, "from_capacity")?,
        to_capacity: capacity(item, "to_capacity")?,
        subject: optional_text("subject")?,
        detail: optional_text("detail")?,
    })
    .map_err(|error| malformed(error.field()))?;
    Ok(RecordedScalingEvent {
        seq: ScalingEventSeq::new(string(item, "sk")?.parse().map_err(|_| malformed("sk"))?),
        event: ScalingAuditEvent {
            at: timestamp(item, "at")?,
            fleet: FleetName::try_new(string(item, "fleet")?).map_err(|_| malformed("fleet"))?,
            kind,
            cause: item
                .contains_key("cause")
                .then(|| {
                    string(item, "cause")?
                        .parse::<ScalingCause>()
                        .map_err(|_| malformed("cause"))
                })
                .transpose()?,
        },
    })
}

fn audit_item(seq: u64, event: &ScalingAuditEvent) -> Item {
    let mut item = key(AUDIT, &padded(seq));
    item.insert("at".into(), n(event.at.as_unix_millis()));
    item.insert("fleet".into(), s(event.fleet.as_str()));
    let stored = event.kind.stored();
    item.insert("kind".into(), s(stored.name));
    if let Some(value) = stored.from_capacity {
        item.insert("from_capacity".into(), n(value));
    }
    if let Some(value) = stored.to_capacity {
        item.insert("to_capacity".into(), n(value));
    }
    if let Some(value) = stored.subject {
        item.insert("subject".into(), s(value));
    }
    if let Some(value) = stored.detail {
        item.insert("detail".into(), s(value));
    }
    if let Some(value) = event.cause {
        item.insert("cause".into(), s(value.as_str()));
    }
    item
}

impl DynamoDb {
    fn put(&self, item: Item) -> Result<TransactWriteItem, DynamoError> {
        let put = Put::builder()
            .table_name(self.table())
            .set_item(Some(item))
            .build()
            .map_err(|_| malformed("put"))?;
        Ok(TransactWriteItem::builder().put(put).build())
    }

    fn delete(&self, pk: &str, sk: &str) -> Result<TransactWriteItem, DynamoError> {
        let delete = Delete::builder()
            .table_name(self.table())
            .set_key(Some(key(pk, sk)))
            .build()
            .map_err(|_| malformed("delete"))?;
        Ok(TransactWriteItem::builder().delete(delete).build())
    }

    fn counter_increment(&self) -> Result<TransactWriteItem, DynamoError> {
        let update = Update::builder()
            .table_name(self.table())
            .set_key(Some(key(COUNTER, REVOCATION)))
            .update_expression("ADD #value :one")
            .expression_attribute_names("#value", "value")
            .expression_attribute_values(":one", n(1))
            .build()
            .map_err(|_| malformed("counter"))?;
        Ok(TransactWriteItem::builder().update(update).build())
    }

    fn snapshot_put(
        &self,
        item: Item,
        previous: Option<&PrincipalRecord>,
    ) -> Result<TransactWriteItem, DynamoError> {
        let mut put = Put::builder().table_name(self.table()).set_item(Some(item));
        if let Some(record) = previous {
            put = put
                .condition_expression(if record.revoked_at.is_some() {
                    "first_seen = :first AND last_seen = :last AND revoked_at = :revoked"
                } else {
                    "first_seen = :first AND last_seen = :last AND attribute_not_exists(revoked_at)"
                })
                .expression_attribute_values(":first", n(record.first_seen.as_unix_millis()))
                .expression_attribute_values(":last", n(record.last_seen.as_unix_millis()));
            if let Some(at) = record.revoked_at {
                put = put.expression_attribute_values(":revoked", n(at.as_unix_millis()));
            }
        } else {
            put = put.condition_expression("attribute_not_exists(pk)");
        }
        Ok(TransactWriteItem::builder()
            .put(put.build().map_err(|_| malformed("principal put"))?)
            .build())
    }

    pub(crate) async fn page_items(
        &self,
        pk: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: NonZeroUsize,
        descending: bool,
    ) -> Result<(Vec<Item>, bool), DynamoError> {
        self.ensure_schema().await?;
        let mut start = None;
        let mut items = Vec::new();
        loop {
            let mut query = self
                .client()
                .await
                .query()
                .table_name(self.table())
                .key_condition_expression(match (after, before) {
                    (Some(_), Some(_)) => "#pk = :pk AND #sk BETWEEN :after AND :before",
                    (Some(_), None) => "#pk = :pk AND #sk > :after",
                    (None, Some(_)) => "#pk = :pk AND #sk < :before",
                    (None, None) => "#pk = :pk",
                })
                .expression_attribute_names("#pk", "pk")
                .expression_attribute_values(":pk", s(pk))
                .consistent_read(true)
                .scan_index_forward(!descending)
                .limit(i32::try_from(limit.get().saturating_add(1)).unwrap_or(i32::MAX))
                .set_exclusive_start_key(start);
            if let Some(after) = after {
                query = query
                    .expression_attribute_names("#sk", "sk")
                    .expression_attribute_values(":after", s(after));
            }
            if let Some(before) = before {
                if after.is_none() {
                    query = query.expression_attribute_names("#sk", "sk");
                }
                query = query.expression_attribute_values(":before", s(before));
            }
            let response = query
                .send()
                .await
                .map_err(|error| DynamoError::Call(gang_aws_sdk::call_error("Query", error)))?;
            for item in response.items.unwrap_or_default() {
                if items.len() == limit.get() {
                    return Ok((items, true));
                }
                items.push(item);
            }
            start = response.last_evaluated_key.filter(|key| !key.is_empty());
            if start.is_none() {
                return Ok((items, false));
            }
        }
    }
}

impl StateRepositoryPort for DynamoDb {
    async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError> {
        checked_timestamp(record.issued_at)?;
        checked_timestamp(record.expires_at)?;
        if let CertStatus::Revoked { at } = record.status {
            checked_timestamp(at)?;
        }
        let sk = serial_key(&record.serial);
        let put = Put::builder()
            .table_name(self.table())
            .set_item(Some(certificate_item(CERT, &sk, record)))
            .condition_expression("attribute_not_exists(pk)")
            .build()
            .map_err(|_| malformed("certificate put"))?;
        let copy = match record.status {
            CertStatus::Active => certificate_item(
                &identity_partition(&record.identity),
                &timed_key(record.issued_at, &sk),
                record,
            ),
            CertStatus::Revoked { at } => {
                certificate_item(REVOKED_CERT, &timed_key(at, &sk), record)
            }
        };
        match self
            .transact(vec![
                TransactWriteItem::builder().put(put).build(),
                self.put(copy)?,
            ])
            .await?
        {
            Transaction::Committed => Ok(()),
            Transaction::ConditionFailed => Err(malformed("duplicate certificate").into()),
        }
    }

    async fn certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Option<IssuedCertRecord>, PortError> {
        self.get(CERT, &serial_key(serial))
            .await?
            .as_ref()
            .map(certificate)
            .transpose()
            .map_err(Into::into)
    }

    async fn revoke(
        &self,
        serial: &CertSerial,
        at: Timestamp,
    ) -> Result<IssuedCertRecord, PortError> {
        checked_timestamp(at)?;
        let sk = serial_key(serial);
        loop {
            let mut record = self
                .certificate(serial)
                .await?
                .ok_or_else(|| PortError::NotFound(Subject::Certificate(serial.clone())))?;
            let existing = match record.status {
                CertStatus::Active => None,
                CertStatus::Revoked { at } => Some(at),
            };
            let effective = existing.unwrap_or(at);
            record.status = CertStatus::Revoked { at: effective };
            let mut update = Update::builder()
                .table_name(self.table())
                .set_key(Some(key(CERT, &sk)))
                .update_expression("SET revoked_at = :at")
                .expression_attribute_values(":at", n(effective.as_unix_millis()));
            update = if existing.is_some() {
                update.condition_expression("revoked_at = :at")
            } else {
                update.condition_expression(
                    "attribute_exists(pk) AND attribute_not_exists(revoked_at)",
                )
            };
            let mut writes = vec![
                TransactWriteItem::builder()
                    .update(
                        update
                            .build()
                            .map_err(|_| malformed("certificate update"))?,
                    )
                    .build(),
            ];
            if existing.is_none() {
                writes.push(self.delete(
                    &identity_partition(&record.identity),
                    &timed_key(record.issued_at, &sk),
                )?);
                writes.push(self.put(certificate_item(
                    REVOKED_CERT,
                    &timed_key(effective, &sk),
                    &record,
                ))?);
            }
            writes.push(self.counter_increment()?);
            if matches!(self.transact(writes).await?, Transaction::Committed) {
                return Ok(record);
            }
        }
    }

    async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
        self.query_partition(REVOKED_CERT)
            .await?
            .iter()
            .map(certificate)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    async fn certificates_of(
        &self,
        identity: &Identity,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        self.query_partition(&identity_partition(identity))
            .await?
            .iter()
            .map(certificate)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    async fn certificates(
        &self,
        after: Option<&CertSerial>,
        limit: NonZeroUsize,
        into: &mut Vec<IssuedCertRecord>,
    ) -> Result<bool, PortError> {
        let after = after.map(serial_key);
        let (items, more) = self
            .page_items(CERT, after.as_deref(), None, limit, false)
            .await?;
        into.clear();
        for item in &items {
            into.push(certificate(item)?);
        }
        Ok(more)
    }

    async fn revocation_epoch(&self) -> Result<RevocationEpoch, PortError> {
        Ok(RevocationEpoch::new(
            self.get(COUNTER, REVOCATION)
                .await?
                .as_ref()
                .map(|item| number(item, "value"))
                .transpose()?
                .unwrap_or(0),
        ))
    }

    async fn record_scaling_event(
        &self,
        event: &ScalingAuditEvent,
    ) -> Result<ScalingEventSeq, PortError> {
        event
            .kind
            .validate()
            .map_err(|error| malformed(error.field()))?;
        checked_timestamp(event.at)?;
        loop {
            let current = self
                .get(COUNTER, AUDIT)
                .await?
                .as_ref()
                .map(|item| number(item, "value"))
                .transpose()?;
            let next = current
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| malformed("audit counter"))?;
            let mut update = Update::builder()
                .table_name(self.table())
                .set_key(Some(key(COUNTER, AUDIT)))
                .update_expression("SET #value = :next")
                .expression_attribute_names("#value", "value")
                .expression_attribute_values(":next", n(next));
            update = if let Some(current) = current {
                update
                    .condition_expression("#value = :current")
                    .expression_attribute_values(":current", n(current))
            } else {
                update.condition_expression("attribute_not_exists(#value)")
            };
            let audit = Put::builder()
                .table_name(self.table())
                .set_item(Some(audit_item(next, event)))
                .condition_expression("attribute_not_exists(pk)")
                .build()
                .map_err(|_| malformed("audit event"))?;
            let writes = vec![
                TransactWriteItem::builder()
                    .update(update.build().map_err(|_| malformed("audit counter"))?)
                    .build(),
                TransactWriteItem::builder().put(audit).build(),
            ];
            if matches!(self.transact(writes).await?, Transaction::Committed) {
                return Ok(ScalingEventSeq::new(next));
            }
            if self.get(AUDIT, &padded(next)).await?.is_some() {
                let observed = self
                    .get(COUNTER, AUDIT)
                    .await?
                    .as_ref()
                    .map(|item| number(item, "value"))
                    .transpose()?;
                if observed == current {
                    return Err(malformed("audit sequence collision").into());
                }
            }
        }
    }

    async fn scaling_events(
        &self,
        since: ScalingEventSeq,
        before: Option<ScalingEventSeq>,
        limit: NonZeroUsize,
        into: &mut Vec<RecordedScalingEvent>,
    ) -> Result<bool, PortError> {
        checked_sequence(since)?;
        if let Some(value) = before {
            checked_sequence(value)?;
        }
        if let Some(upper) = before
            && upper.get() <= since.get() + 1
        {
            into.clear();
            return Ok(false);
        }
        let lower = padded(since.get() + u64::from(before.is_some()));
        let upper = before.map(|value| padded(value.get() - 1));
        let (items, more) = self
            .page_items(AUDIT, Some(&lower), upper.as_deref(), limit, true)
            .await?;
        into.clear();
        for item in items.iter().rev() {
            into.push(scaling(item)?);
        }
        Ok(more)
    }

    async fn observe_principal(&self, identity: &Identity, at: Timestamp) -> Result<(), PortError> {
        checked_timestamp(at)?;
        loop {
            let previous = self.principal(identity).await?;
            if previous
                .as_ref()
                .is_some_and(|record| record.last_seen >= at)
            {
                return Ok(());
            }
            let record = previous.as_ref().map_or_else(
                || PrincipalRecord {
                    identity: identity.clone(),
                    first_seen: at,
                    last_seen: at,
                    revoked_at: None,
                },
                |record| PrincipalRecord {
                    last_seen: at,
                    ..record.clone()
                },
            );
            let sk = identity_key(identity);
            let first = if let Some(old) = &previous {
                let mut update = Update::builder()
                    .table_name(self.table())
                    .set_key(Some(key(PRINCIPAL, &sk)))
                    .update_expression("SET last_seen = :at")
                    .expression_attribute_values(":at", n(at.as_unix_millis()));
                update = if let Some(revoked_at) = old.revoked_at {
                    update
                        .condition_expression("last_seen < :at AND revoked_at = :revoked")
                        .expression_attribute_values(":revoked", n(revoked_at.as_unix_millis()))
                } else {
                    update.condition_expression(
                        "last_seen < :at AND attribute_not_exists(revoked_at)",
                    )
                };
                TransactWriteItem::builder()
                    .update(update.build().map_err(|_| malformed("principal update"))?)
                    .build()
            } else {
                self.snapshot_put(principal_item(PRINCIPAL, &sk, &record), None)?
            };
            let mut writes = vec![first];
            if let Some(revoked_at) = record.revoked_at {
                writes.push(self.put(principal_item(
                    REVOKED_PRINCIPAL,
                    &timed_key(revoked_at, &sk),
                    &record,
                ))?);
            }
            if matches!(self.transact(writes).await?, Transaction::Committed) {
                return Ok(());
            }
        }
    }

    async fn principal(&self, identity: &Identity) -> Result<Option<PrincipalRecord>, PortError> {
        self.get(PRINCIPAL, &identity_key(identity))
            .await?
            .as_ref()
            .map(principal)
            .transpose()
            .map_err(Into::into)
    }

    async fn revoke_principal(
        &self,
        identity: &Identity,
        at: Timestamp,
    ) -> Result<PrincipalRecord, PortError> {
        checked_timestamp(at)?;
        loop {
            let previous = self.principal(identity).await?;
            let mut record = previous.clone().unwrap_or_else(|| PrincipalRecord {
                identity: identity.clone(),
                first_seen: at,
                last_seen: at,
                revoked_at: None,
            });
            let revoked_at = *record.revoked_at.get_or_insert(at);
            let sk = identity_key(identity);
            let writes = vec![
                self.snapshot_put(principal_item(PRINCIPAL, &sk, &record), previous.as_ref())?,
                self.put(principal_item(
                    REVOKED_PRINCIPAL,
                    &timed_key(revoked_at, &sk),
                    &record,
                ))?,
                self.counter_increment()?,
            ];
            if matches!(self.transact(writes).await?, Transaction::Committed) {
                return Ok(record);
            }
        }
    }

    async fn restore_principal(&self, identity: &Identity) -> Result<PrincipalRecord, PortError> {
        loop {
            let previous = self
                .principal(identity)
                .await?
                .ok_or_else(|| PortError::NotFound(Subject::Principal(identity.clone())))?;
            let mut record = previous.clone();
            record.revoked_at = None;
            let sk = identity_key(identity);
            let mut writes =
                vec![self.snapshot_put(principal_item(PRINCIPAL, &sk, &record), Some(&previous))?];
            if let Some(at) = previous.revoked_at {
                writes.push(self.delete(REVOKED_PRINCIPAL, &timed_key(at, &sk))?);
            }
            writes.push(self.counter_increment()?);
            if matches!(self.transact(writes).await?, Transaction::Committed) {
                return Ok(record);
            }
        }
    }

    async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
        self.query_partition(REVOKED_PRINCIPAL)
            .await?
            .iter()
            .map(principal)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    async fn principals(
        &self,
        after: Option<&Identity>,
        limit: NonZeroUsize,
        only_revoked: bool,
        into: &mut Vec<PrincipalRecord>,
    ) -> Result<bool, PortError> {
        if only_revoked {
            // The existing revoked index is ordered by revocation time, not identity.
            // Reading it avoids scanning active principals and also works for existing tables.
            let mut records = self.revoked_principals().await?;
            records.retain(|record| after.is_none_or(|after| record.identity > *after));
            records.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
            let more = records.len() > limit.get();
            into.clear();
            into.extend(records.into_iter().take(limit.get()));
            return Ok(more);
        }
        let after = after.map(identity_key);
        let (items, more) = self
            .page_items(PRINCIPAL, after.as_deref(), None, limit, false)
            .await?;
        into.clear();
        for item in &items {
            into.push(principal(item)?);
        }
        Ok(more)
    }
}
