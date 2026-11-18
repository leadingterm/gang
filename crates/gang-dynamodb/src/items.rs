use std::collections::HashMap;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use gang_clock::Timestamp;
use gang_spi::{RecordId, RecordKind};
use gang_types::{CertSerial, Identity};

use crate::DynamoError;

pub(crate) type Item = HashMap<String, AttributeValue>;

pub(crate) fn key(pk: &str, sk: &str) -> Item {
    HashMap::from([("pk".into(), s(pk)), ("sk".into(), s(sk))])
}

pub(crate) fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}
pub(crate) fn n(value: impl ToString + Copy) -> AttributeValue {
    AttributeValue::N(value.to_string())
}
pub(crate) fn b(value: &[u8]) -> AttributeValue {
    AttributeValue::B(Blob::new(value))
}

pub(crate) fn string<'a>(item: &'a Item, name: &'static str) -> Result<&'a str, DynamoError> {
    item.get(name)
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .ok_or(DynamoError::Malformed { attribute: name })
}

pub(crate) fn number(item: &Item, name: &'static str) -> Result<u64, DynamoError> {
    item.get(name)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .ok_or(DynamoError::Malformed { attribute: name })
}

pub(crate) fn optional_number(item: &Item, name: &'static str) -> Result<Option<u64>, DynamoError> {
    item.contains_key(name)
        .then(|| number(item, name))
        .transpose()
}

pub(crate) fn bytes<'a>(item: &'a Item, name: &'static str) -> Result<&'a [u8], DynamoError> {
    item.get(name)
        .and_then(|v| v.as_b().ok())
        .map(Blob::as_ref)
        .ok_or(DynamoError::Malformed { attribute: name })
}

pub(crate) fn timestamp(item: &Item, name: &'static str) -> Result<Timestamp, DynamoError> {
    Ok(Timestamp::from_unix_millis(number(item, name)?))
}

pub(crate) fn optional_timestamp(
    item: &Item,
    name: &'static str,
) -> Result<Option<Timestamp>, DynamoError> {
    optional_number(item, name).map(|v| v.map(Timestamp::from_unix_millis))
}

pub(crate) fn serial_key(serial: &CertSerial) -> String {
    serial.as_str().to_owned()
}
pub(crate) fn identity_key(identity: &Identity) -> String {
    format!(
        "{}\u{1f}{}",
        identity.provider.as_str(),
        identity.principal.as_str()
    )
}
pub(crate) fn padded(value: u64) -> String {
    format!("{value:020}")
}
pub(crate) fn record_pk(kind: RecordKind) -> String {
    format!("rec#{}", kind.as_str())
}
pub(crate) fn record_sk(id: &RecordId) -> String {
    hex::encode(id.as_bytes())
}
