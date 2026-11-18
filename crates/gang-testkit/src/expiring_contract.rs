//! Contract cases for expiring records, shared by database implementations.

use gang_clock::Timestamp;
use gang_spi::{
    ExpiringRecordPort, Insert, Record, RecordId, RecordKey, RecordKind, RecordVersion, Remove,
    Replace,
};

use crate::TestResult;

fn ts(millis: u64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

fn key(kind: RecordKind, id: u8) -> RecordKey {
    RecordKey {
        kind,
        id: RecordId::new([id; 32]),
    }
}

fn record(body: &[u8], version: u64, expires_at: u64) -> Record {
    Record {
        body: body.to_vec(),
        version: RecordVersion::new(version),
        expires_at: ts(expires_at),
    }
}

/// An inserted record is readable at its first version until expiry.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if the stored record or expiry boundary differs from the contract.
pub async fn record_insert_and_expiry<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    assert_eq!(
        database
            .insert(&[(k, b"body")], RecordVersion::new(1), ts(1), ts(10))
            .await?,
        Insert::Inserted
    );
    assert_eq!(database.get(&k, ts(9)).await?, Some(record(b"body", 1, 10)));
    assert_eq!(database.get(&k, ts(10)).await?, None);
    assert_eq!(database.get(&k, ts(11)).await?, None);
    Ok(())
}

/// A batch containing a live key inserts none of its records.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if a failed batch changes either key.
pub async fn record_insert_is_atomic<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let a = key(RecordKind::LoginAttempt, 1);
    let b = key(RecordKind::LoginAttempt, 2);
    assert_eq!(
        database
            .insert(&[(a, b"old")], RecordVersion::new(1), ts(1), ts(10))
            .await?,
        Insert::Inserted
    );
    assert_eq!(
        database
            .insert(
                &[(b, b"new"), (a, b"changed")],
                RecordVersion::new(3),
                ts(2),
                ts(20)
            )
            .await?,
        Insert::Exists
    );
    assert_eq!(database.get(&a, ts(2)).await?, Some(record(b"old", 1, 10)));
    assert_eq!(database.get(&b, ts(2)).await?, None);
    let duplicate = key(RecordKind::LoginAttempt, 3);
    assert_eq!(
        database
            .insert(
                &[(duplicate, b"first"), (duplicate, b"second")],
                RecordVersion::new(3),
                ts(2),
                ts(20)
            )
            .await?,
        Insert::Exists
    );
    assert_eq!(database.get(&duplicate, ts(2)).await?, None);
    Ok(())
}

/// An expired record can be overwritten by an insert at a new version.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if the expired record prevents insertion or the new value differs.
pub async fn record_insert_over_expired<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    database
        .insert(&[(k, b"old")], RecordVersion::new(1), ts(1), ts(10))
        .await?;
    assert_eq!(
        database
            .insert(&[(k, b"new")], RecordVersion::new(3), ts(10), ts(20))
            .await?,
        Insert::Inserted
    );
    assert_eq!(database.get(&k, ts(10)).await?, Some(record(b"new", 3, 20)));
    Ok(())
}
/// A previous incarnation cannot change a reinserted record.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if the previous version can change the new record.
pub async fn a_reinserted_record_refuses_the_previous_version<D: ExpiringRecordPort>(
    database: &D,
) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    let old = RecordVersion::new(1);
    let new = RecordVersion::new(3);
    assert_eq!(
        database.insert(&[(k, b"old")], old, ts(1), ts(10)).await?,
        Insert::Inserted
    );
    assert_eq!(
        database.insert(&[(k, b"new")], new, ts(10), ts(20)).await?,
        Insert::Inserted
    );
    assert_eq!(
        database.replace(&k, old, b"wrong", ts(11), ts(20)).await?,
        Replace::Stale
    );
    assert_eq!(database.remove(&k, old, ts(11)).await?, Remove::Stale);
    assert_eq!(database.get(&k, ts(11)).await?, Some(record(b"new", 3, 20)));
    Ok(())
}

/// Concurrent inserts of one key have exactly one winner.
///
/// # Errors
/// Returns a database error or an error if the contenders do not produce one winner.
///
/// # Panics
/// Panics if the stored value is not the winner's.
pub async fn record_concurrent_insert<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    let first = [(k, b"first".as_slice())];
    let second = [(k, b"second".as_slice())];
    let (a, b) = tokio::join!(
        database.insert(&first, RecordVersion::new(1), ts(1), ts(10)),
        database.insert(&second, RecordVersion::new(3), ts(1), ts(20))
    );
    let expected = match (a?, b?) {
        (Insert::Inserted, Insert::Exists) => record(b"first", 1, 10),
        (Insert::Exists, Insert::Inserted) => record(b"second", 3, 20),
        outcomes => return Err(format!("expected one insert, got {outcomes:?}").into()),
    };
    assert_eq!(database.get(&k, ts(1)).await?, Some(expected));
    Ok(())
}

/// Replacing the current version advances it and stores the new body and expiry.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if replacement or its stored value differs from the contract.
pub async fn record_replace_current<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    database
        .insert(&[(k, b"old")], RecordVersion::new(1), ts(1), ts(10))
        .await?;
    assert_eq!(
        database
            .replace(&k, RecordVersion::new(1), b"new", ts(2), ts(20))
            .await?,
        Replace::Replaced(RecordVersion::new(2))
    );
    assert_eq!(database.get(&k, ts(2)).await?, Some(record(b"new", 2, 20)));
    Ok(())
}

/// Replacing with an old version or after expiry is stale and changes nothing.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if an invalid replacement succeeds or alters the record.
pub async fn record_replace_stale_or_expired<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    database
        .insert(&[(k, b"old")], RecordVersion::new(1), ts(1), ts(10))
        .await?;
    assert_eq!(
        database
            .replace(&k, RecordVersion::new(1), b"current", ts(2), ts(10))
            .await?,
        Replace::Replaced(RecordVersion::new(2))
    );
    assert_eq!(
        database
            .replace(&k, RecordVersion::new(1), b"wrong", ts(3), ts(20))
            .await?,
        Replace::Stale
    );
    assert_eq!(
        database.get(&k, ts(3)).await?,
        Some(record(b"current", 2, 10))
    );
    assert_eq!(
        database
            .replace(&k, RecordVersion::new(2), b"wrong", ts(10), ts(20))
            .await?,
        Replace::Stale
    );
    assert_eq!(database.get(&k, ts(10)).await?, None);
    Ok(())
}

/// Removing a live record at its current version makes it absent.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if removal does not remove the record.
pub async fn record_remove_current<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    database
        .insert(&[(k, b"body")], RecordVersion::new(1), ts(1), ts(10))
        .await?;
    assert_eq!(
        database.remove(&k, RecordVersion::new(1), ts(2)).await?,
        Remove::Removed
    );
    assert_eq!(database.get(&k, ts(2)).await?, None);
    Ok(())
}

/// Concurrent removes of the same version have exactly one winner.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if both removals win or the record remains.
pub async fn record_concurrent_remove<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    database
        .insert(&[(k, b"body")], RecordVersion::new(1), ts(1), ts(10))
        .await?;
    let (a, b) = tokio::join!(
        database.remove(&k, RecordVersion::new(1), ts(2)),
        database.remove(&k, RecordVersion::new(1), ts(2))
    );
    assert!(matches!(
        (a?, b?),
        (Remove::Removed, Remove::Stale) | (Remove::Stale, Remove::Removed)
    ));
    assert_eq!(database.get(&k, ts(2)).await?, None);
    Ok(())
}

/// Removing a stale version leaves the current record intact.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if a stale removal succeeds or changes the record.
pub async fn record_remove_stale<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let k = key(RecordKind::LoginAttempt, 1);
    database
        .insert(&[(k, b"old")], RecordVersion::new(1), ts(1), ts(10))
        .await?;
    assert_eq!(
        database
            .replace(&k, RecordVersion::new(1), b"new", ts(2), ts(20))
            .await?,
        Replace::Replaced(RecordVersion::new(2))
    );
    assert_eq!(
        database.remove(&k, RecordVersion::new(1), ts(3)).await?,
        Remove::Stale
    );
    assert_eq!(database.get(&k, ts(3)).await?, Some(record(b"new", 2, 20)));
    Ok(())
}

/// Record kinds have separate key spaces even for the same id.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if the two kinds interfere.
pub async fn record_kinds_are_separate<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let a = key(RecordKind::LoginAttempt, 1);
    let b = key(RecordKind::DeviceCode, 1);
    assert_eq!(
        database
            .insert(
                &[(a, b"a"), (b, b"b")],
                RecordVersion::new(1),
                ts(1),
                ts(10)
            )
            .await?,
        Insert::Inserted
    );
    assert_eq!(database.get(&a, ts(2)).await?, Some(record(b"a", 1, 10)));
    assert_eq!(database.get(&b, ts(2)).await?, Some(record(b"b", 1, 10)));
    assert_eq!(
        database.remove(&a, RecordVersion::new(1), ts(2)).await?,
        Remove::Removed
    );
    assert_eq!(database.get(&b, ts(2)).await?, Some(record(b"b", 1, 10)));
    Ok(())
}

/// Purging preserves live records and expired records remain unreadable.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if purge changes the visible records incorrectly.
pub async fn record_purge<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let expired = key(RecordKind::LoginAttempt, 1);
    let live = key(RecordKind::LoginAttempt, 2);
    database
        .insert(
            &[(expired, b"expired")],
            RecordVersion::new(1),
            ts(1),
            ts(10),
        )
        .await?;
    database
        .insert(&[(live, b"live")], RecordVersion::new(3), ts(1), ts(20))
        .await?;
    database.purge(ts(10)).await?;
    assert_eq!(database.get(&expired, ts(10)).await?, None);
    assert_eq!(
        database.get(&live, ts(10)).await?,
        Some(record(b"live", 3, 20))
    );
    Ok(())
}
