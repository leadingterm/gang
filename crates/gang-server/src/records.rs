//! JSON-encoded, versioned records shared by controllers.

use gang_clock::Timestamp;
use gang_spi::{
    ExpiringRecordPort, Insert, PortError, RecordId, RecordKey, RecordKind, RecordVersion, Remove,
    Replace,
};
use ring::digest::{SHA256, digest};
use serde::{Serialize, de::DeserializeOwned};

/// The key for a secret without storing that secret in the database.
pub(crate) fn key(kind: RecordKind, secret: &[u8]) -> RecordKey {
    let hash = digest(&SHA256, secret);
    let mut bytes = [0; 32];
    bytes.copy_from_slice(hash.as_ref());
    RecordKey {
        kind,
        id: RecordId::new(bytes),
    }
}

/// Encodes a record body.
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, PortError> {
    serde_json::to_vec(value).map_err(PortError::backend)
}

/// A decoded record and the metadata required for a conditional write.
pub(crate) struct Stored<T> {
    pub(crate) value: T,
    pub(crate) version: RecordVersion,
    pub(crate) expires_at: Timestamp,
}

/// Inserts a single record.
pub(crate) async fn insert<R: ExpiringRecordPort, T: Serialize>(
    repository: &R,
    key: RecordKey,
    value: &T,
    version: RecordVersion,
    now: Timestamp,
    expires_at: Timestamp,
) -> Result<Insert, PortError> {
    let body = encode(value)?;
    repository
        .insert(&[(key, &body)], version, now, expires_at)
        .await
}

/// Atomically inserts two records with independently typed bodies.
pub(crate) async fn insert_pair<R: ExpiringRecordPort, A: Serialize, B: Serialize>(
    repository: &R,
    first: (RecordKey, &A),
    second: (RecordKey, &B),
    now: Timestamp,
    expires_at: Timestamp,
) -> Result<Insert, PortError> {
    let first_body = encode(first.1)?;
    let second_body = encode(second.1)?;
    repository
        .insert(
            &[(first.0, &first_body), (second.0, &second_body)],
            RecordVersion::fresh().map_err(PortError::backend)?,
            now,
            expires_at,
        )
        .await
}

/// Reads and decodes a live record.
pub(crate) async fn get<R: ExpiringRecordPort, T: DeserializeOwned>(
    repository: &R,
    key: &RecordKey,
    now: Timestamp,
) -> Result<Option<Stored<T>>, PortError> {
    repository
        .get(key, now)
        .await?
        .map(|record| {
            Ok(Stored {
                value: serde_json::from_slice(&record.body).map_err(PortError::backend)?,
                version: record.version,
                expires_at: record.expires_at,
            })
        })
        .transpose()
}

/// Replaces a live record only at the observed version.
pub(crate) async fn replace<R: ExpiringRecordPort, T: Serialize>(
    repository: &R,
    key: &RecordKey,
    version: RecordVersion,
    value: &T,
    now: Timestamp,
    expires_at: Timestamp,
) -> Result<Replace, PortError> {
    repository
        .replace(key, version, &encode(value)?, now, expires_at)
        .await
}

/// Removes a live record only at the observed version.
pub(crate) async fn remove<R: ExpiringRecordPort>(
    repository: &R,
    key: &RecordKey,
    version: RecordVersion,
    now: Timestamp,
) -> Result<Remove, PortError> {
    repository.remove(key, version, now).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_clock::Timestamp;
    use gang_spi::{
        ExpiringRecordPort, Insert, PortError, RecordKind, RecordVersion, Remove, Replace,
    };
    use gang_testkit::{FakeRepository, TestResult};

    use super::{get, insert, insert_pair, key, remove, replace};

    #[tokio::test]
    async fn records_share_versions_and_expire() -> TestResult {
        let first = FakeRepository::default();
        let second = first.clone();
        let now = Timestamp::from_unix_millis(1_000);
        let expiry = now + Duration::from_secs(10);
        let a = key(RecordKind::DeviceCode, b"secret");
        let b = key(RecordKind::UserCode, b"code");
        assert_eq!(
            insert_pair(&first, (a, &"state"), (b, &42u32), now, expiry).await?,
            Insert::Inserted
        );
        let stored = get::<_, String>(&second, &a, now)
            .await?
            .expect("shared record");
        assert_eq!(stored.value, "state");
        assert_eq!(stored.expires_at, expiry);
        assert!(matches!(
            replace(&second, &a, stored.version, &"approved", now, expiry).await?,
            Replace::Replaced(_)
        ));
        assert_eq!(
            remove(&first, &a, stored.version, now).await?,
            Remove::Stale
        );
        let approved = get::<_, String>(&first, &a, now)
            .await?
            .expect("updated record");
        assert_eq!(approved.value, "approved");
        assert_eq!(
            remove(&first, &a, approved.version, now).await?,
            Remove::Removed
        );
        assert!(get::<_, u32>(&second, &b, expiry).await?.is_none());
        assert_eq!(
            insert(
                &first,
                a,
                &"new",
                RecordVersion::fresh()?,
                expiry,
                expiry + Duration::from_secs(1)
            )
            .await?,
            Insert::Inserted
        );
        assert_eq!(
            remove(&second, &a, stored.version, expiry).await?,
            Remove::Stale
        );
        assert_eq!(
            get::<_, String>(&second, &a, expiry)
                .await?
                .expect("reinserted record")
                .value,
            "new"
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_body_is_a_backend_error() -> TestResult {
        let repository = FakeRepository::default();
        let now = Timestamp::from_unix_millis(1_000);
        let key = key(RecordKind::LoginAttempt, b"state");
        ExpiringRecordPort::insert(
            &repository,
            &[(key, b"not json")],
            RecordVersion::fresh()?,
            now,
            now + Duration::from_secs(1),
        )
        .await?;
        assert!(matches!(
            get::<_, String>(&repository, &key, now).await,
            Err(PortError::Backend(_))
        ));
        Ok(())
    }
}
