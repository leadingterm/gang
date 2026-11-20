//! Contract cases for expiring records, shared by database implementations.

use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    Acquisition, ApplyRecords, CandidateGeneration, CandidatePriority, ControllerId,
    ExpiringRecordPort, Insert, LeaderLeasePort, Lease, LeaseCandidate, LeaseEpoch,
    MAX_RECORD_BODY_BYTES, MAX_RECORD_BODY_BYTES_PER_BATCH, MAX_RECORD_CHANGES, Record,
    RecordBatchSeq, RecordChange, RecordId, RecordKey, RecordKind, RecordVersion, Remove, Renewal,
    Replace, Takeover,
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

/// Listing is ordered, excludes expired and other-kind records, and reports only live overflow.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if pagination, filtering, or buffer replacement violates the contract.
pub async fn record_list_pages_live_records<D: ExpiringRecordPort>(database: &D) -> TestResult {
    let kind = RecordKind::InfrastructureEvent;
    for (id, expiry) in [(1, 10), (2, 20), (3, 10), (4, 20), (5, 10)] {
        database
            .insert(
                &[(key(kind, id), &[id])],
                RecordVersion::new(1),
                ts(1),
                ts(expiry),
            )
            .await?;
    }
    database
        .insert(
            &[(key(RecordKind::LoginAttempt, 3), b"other")],
            RecordVersion::new(1),
            ts(1),
            ts(20),
        )
        .await?;
    let mut page = vec![(key(kind, 99).id, record(b"stale", 1, 99))];
    assert!(
        database
            .records(kind, None, NonZeroUsize::MIN, ts(10), &mut page)
            .await?
    );
    assert_eq!(page, vec![(key(kind, 2).id, record(&[2], 1, 20))]);
    assert!(
        !database
            .records(
                kind,
                Some(key(kind, 2).id),
                NonZeroUsize::MIN,
                ts(10),
                &mut page
            )
            .await?
    );
    assert_eq!(page, vec![(key(kind, 4).id, record(&[4], 1, 20))]);
    assert!(
        !database
            .records(
                kind,
                Some(key(kind, 4).id),
                NonZeroUsize::MIN,
                ts(10),
                &mut page
            )
            .await?
    );
    assert!(page.is_empty());
    assert!(
        !database
            .records(kind, None, NonZeroUsize::MIN, ts(20), &mut page)
            .await?
    );
    assert!(page.is_empty());
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

fn candidate(holder: &str) -> LeaseCandidate {
    LeaseCandidate {
        holder: ControllerId::new(holder),
        priority: CandidatePriority::registered(CandidateGeneration::default()),
    }
}

async fn acquired<D: LeaderLeasePort>(database: &D) -> Result<Lease, Box<dyn std::error::Error>> {
    match database
        .acquire(
            &candidate("history-contract"),
            ts(1),
            ts(1),
            ts(10_000),
            Takeover::Observe,
        )
        .await?
    {
        Acquisition::Acquired(lease) => Ok(lease),
        outcome => Err(format!("fresh fixture did not acquire a lease: {outcome:?}").into()),
    }
}

/// Lists only live records in byte order with exclusive cursors and an exact more flag.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics when paging or expiration differs from the contract.
pub async fn records_page_live_in_order<D: ExpiringRecordPort + LeaderLeasePort>(
    database: &D,
) -> TestResult {
    let lease = acquired(database).await?;
    let kind = RecordKind::ResourceHistory;
    let body = vec![7; 60_000];
    // More than one DynamoDB query page of expired records precedes the live entries.
    for start in [0_u8, 10, 20] {
        let changes: Vec<_> = (start..start + 10)
            .map(|id| RecordChange::Put {
                key: key(kind, id),
                body: &body,
                expires_at: ts(10),
            })
            .collect();
        assert_eq!(
            database
                .apply(
                    &lease,
                    RecordBatchSeq::new(u64::from(start / 10) + 1),
                    RecordVersion::new(u64::from(start / 10) + 101),
                    ts(2),
                    &changes
                )
                .await?,
            ApplyRecords::Applied
        );
    }
    let changes: Vec<_> = [32, 31, 33]
        .into_iter()
        .map(|id| RecordChange::Put {
            key: key(kind, id),
            body: b"live",
            expires_at: ts(30),
        })
        .collect();
    assert_eq!(
        database
            .apply(
                &lease,
                RecordBatchSeq::new(4),
                RecordVersion::new(104),
                ts(2),
                &changes
            )
            .await?,
        ApplyRecords::Applied
    );
    let mut page = vec![(key(kind, 99).id, record(b"stale", 1, 99))];
    let limit = NonZeroUsize::MIN.saturating_add(1);
    assert!(
        database
            .records(kind, None, limit, ts(10), &mut page)
            .await?
    );
    assert_eq!(
        page,
        vec![
            (key(kind, 31).id, record(b"live", 104, 30)),
            (key(kind, 32).id, record(b"live", 104, 30))
        ]
    );
    assert!(
        !database
            .records(kind, Some(key(kind, 32).id), limit, ts(10), &mut page)
            .await?
    );
    assert_eq!(page, vec![(key(kind, 33).id, record(b"live", 104, 30))]);
    assert!(
        !database
            .records(kind, Some(key(kind, 33).id), limit, ts(10), &mut page)
            .await?
    );
    assert!(page.is_empty());
    assert!(
        !database
            .records(kind, None, limit, ts(30), &mut page)
            .await?
    );
    assert!(page.is_empty());
    Ok(())
}

/// Applies upserts and idempotent deletes with the supplied per-record expiry.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if a mutation or its expiry is incorrect.
pub async fn apply_upserts_and_deletes<D: ExpiringRecordPort + LeaderLeasePort>(
    database: &D,
) -> TestResult {
    let lease = acquired(database).await?;
    let a = key(RecordKind::ResourceHistory, 1);
    let b = key(RecordKind::DurationHistory, 2);
    let absent = key(RecordKind::HistoryChunk, 3);
    let changes = [
        RecordChange::Put {
            key: a,
            body: b"old",
            expires_at: ts(50),
        },
        RecordChange::Put {
            key: b,
            body: b"remove",
            expires_at: ts(5),
        },
    ];
    assert_eq!(
        database
            .apply(
                &lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(101),
                ts(2),
                &changes
            )
            .await?,
        ApplyRecords::Applied
    );
    let changes = [
        RecordChange::Put {
            key: a,
            body: b"new",
            expires_at: ts(20),
        },
        RecordChange::Delete { key: b },
        RecordChange::Delete { key: absent },
    ];
    assert_eq!(
        database
            .apply(
                &lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(102),
                ts(6),
                &changes
            )
            .await?,
        ApplyRecords::Applied
    );
    assert_eq!(
        database.get(&a, ts(19)).await?,
        Some(record(b"new", 102, 20))
    );
    assert_eq!(database.get(&a, ts(20)).await?, None);
    assert_eq!(database.get(&b, ts(2)).await?, None);
    Ok(())
}

/// Rejects duplicate keys and portable transaction limits without partial writes.
///
/// # Errors
/// Returns a database error for fixture setup or reads.
///
/// # Panics
/// Panics if a limit is not enforced atomically.
pub async fn apply_enforces_batch_limits<D: ExpiringRecordPort + LeaderLeasePort>(
    database: &D,
) -> TestResult {
    let lease = acquired(database).await?;
    batch_operation_limit(database, &lease).await?;
    batch_duplicate_keys(database, &lease).await?;
    batch_body_limits(database, &lease).await
}

async fn batch_operation_limit<D: ExpiringRecordPort>(database: &D, lease: &Lease) -> TestResult {
    let kind = RecordKind::HistoryChunk;
    let limit = u8::try_from(MAX_RECORD_CHANGES)?;
    let changes: Vec<_> = (0..limit)
        .map(|id| RecordChange::Delete { key: key(kind, id) })
        .collect();
    assert_eq!(
        database
            .apply(
                lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(101),
                ts(2),
                &changes
            )
            .await?,
        ApplyRecords::Applied
    );
    let mut too_many = changes.clone();
    too_many.push(RecordChange::Put {
        key: key(kind, 100),
        body: b"x",
        expires_at: ts(100),
    });
    assert!(
        database
            .apply(
                lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(102),
                ts(2),
                &too_many
            )
            .await
            .is_err()
    );
    assert_eq!(database.get(&key(kind, 100), ts(2)).await?, None);
    Ok(())
}

async fn batch_duplicate_keys<D: ExpiringRecordPort>(database: &D, lease: &Lease) -> TestResult {
    let kind = RecordKind::HistoryChunk;
    let duplicate = [
        RecordChange::Put {
            key: key(kind, 101),
            body: b"x",
            expires_at: ts(100),
        },
        RecordChange::Delete {
            key: key(kind, 101),
        },
    ];
    assert!(
        database
            .apply(
                lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(103),
                ts(2),
                &duplicate
            )
            .await
            .is_err()
    );
    assert_eq!(database.get(&key(kind, 101), ts(2)).await?, None);
    Ok(())
}

async fn batch_body_limits<D: ExpiringRecordPort>(database: &D, lease: &Lease) -> TestResult {
    let kind = RecordKind::HistoryChunk;
    let body = vec![9; MAX_RECORD_BODY_BYTES_PER_BATCH / 6];
    let mut changes: Vec<_> = (110..116)
        .map(|id| RecordChange::Put {
            key: key(kind, id),
            body: &body,
            expires_at: ts(100),
        })
        .collect();
    assert_eq!(
        database
            .apply(
                lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(104),
                ts(2),
                &changes
            )
            .await?,
        ApplyRecords::Applied
    );
    let oversize = vec![8; MAX_RECORD_BODY_BYTES + 1];
    let too_large = [
        RecordChange::Delete {
            key: key(kind, 110),
        },
        RecordChange::Put {
            key: key(kind, 117),
            body: &oversize,
            expires_at: ts(100),
        },
    ];
    assert!(
        database
            .apply(
                lease,
                RecordBatchSeq::new(3),
                RecordVersion::new(105),
                ts(2),
                &too_large
            )
            .await
            .is_err()
    );
    assert_eq!(
        database.get(&key(kind, 110), ts(2)).await?,
        Some(record(&body, 104, 100))
    );
    assert_eq!(database.get(&key(kind, 117), ts(2)).await?, None);
    changes.push(RecordChange::Put {
        key: key(kind, 116),
        body: b"x",
        expires_at: ts(100),
    });
    assert!(
        database
            .apply(
                lease,
                RecordBatchSeq::new(3),
                RecordVersion::new(106),
                ts(2),
                &changes
            )
            .await
            .is_err()
    );
    assert_eq!(database.get(&key(kind, 116), ts(2)).await?, None);
    assert_eq!(
        database.get(&key(kind, 110), ts(2)).await?,
        Some(record(&body, 104, 100))
    );
    assert_eq!(
        database
            .apply(
                lease,
                RecordBatchSeq::new(3),
                RecordVersion::new(107),
                ts(2),
                &[]
            )
            .await?,
        ApplyRecords::Applied
    );
    Ok(())
}

async fn reject_wrong_lease_identity<D: ExpiringRecordPort>(
    database: &D,
    lease: &Lease,
    changes: &[RecordChange<'_>],
) -> TestResult {
    let wrong_holder = Lease {
        holder: ControllerId::new("other"),
        ..lease.clone()
    };
    let wrong_epoch = Lease {
        epoch: LeaseEpoch::new(lease.epoch.get() + 1),
        ..lease.clone()
    };
    for invalid in [&wrong_holder, &wrong_epoch] {
        assert_eq!(
            database
                .apply(
                    invalid,
                    RecordBatchSeq::new(2),
                    RecordVersion::new(102),
                    ts(3),
                    changes
                )
                .await?,
            ApplyRecords::Fenced
        );
        assert_eq!(
            database
                .apply(
                    invalid,
                    RecordBatchSeq::new(2),
                    RecordVersion::new(103),
                    ts(3),
                    &[]
                )
                .await?,
            ApplyRecords::Fenced
        );
    }
    Ok(())
}

/// Fences stale holders, epochs, expired and released leases, including empty batches.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if a stale writer changes any record.
pub async fn apply_rejects_invalid_leases<D: ExpiringRecordPort + LeaderLeasePort>(
    database: &D,
) -> TestResult {
    let lease = acquired(database).await?;
    let a = key(RecordKind::ResourceHistory, 1);
    let b = key(RecordKind::ResourceHistory, 2);
    let initial = [RecordChange::Put {
        key: a,
        body: b"old",
        expires_at: ts(30_000),
    }];
    database
        .apply(
            &lease,
            RecordBatchSeq::new(1),
            RecordVersion::new(101),
            ts(2),
            &initial,
        )
        .await?;
    let changes = [
        RecordChange::Delete { key: a },
        RecordChange::Put {
            key: b,
            body: b"new",
            expires_at: ts(30_000),
        },
    ];
    reject_wrong_lease_identity(database, &lease, &changes).await?;
    assert_eq!(
        database
            .apply(
                &lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(104),
                ts(10_000),
                &changes
            )
            .await?,
        ApplyRecords::Fenced
    );
    let successor = match database
        .acquire(
            &candidate("successor"),
            ts(10_001),
            ts(10_001),
            ts(20_000),
            Takeover::Observe,
        )
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("expired lease was not acquired: {outcome:?}").into()),
    };
    assert_eq!(
        database
            .apply(
                &lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(105),
                ts(10_002),
                &changes
            )
            .await?,
        ApplyRecords::Fenced
    );
    database.release(&successor).await?;
    assert_eq!(
        database
            .apply(
                &successor,
                RecordBatchSeq::new(2),
                RecordVersion::new(106),
                ts(10_003),
                &changes
            )
            .await?,
        ApplyRecords::Fenced
    );
    assert_eq!(
        database.get(&a, ts(10_003)).await?,
        Some(record(b"old", 101, 30_000))
    );
    assert_eq!(database.get(&b, ts(10_003)).await?, None);
    Ok(())
}

/// Sequences fence out-of-order batches across renewal and reset on a new epoch.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if an older batch mutates records or renewal resets its fence.
pub async fn apply_sequences_are_fenced<D: ExpiringRecordPort + LeaderLeasePort>(
    database: &D,
) -> TestResult {
    let lease = acquired(database).await?;
    let a = key(RecordKind::ResourceHistory, 1);
    let b = key(RecordKind::ResourceHistory, 2);
    let initial = [RecordChange::Put {
        key: a,
        body: b"new",
        expires_at: ts(30_000),
    }];
    assert_eq!(
        database
            .apply(
                &lease,
                RecordBatchSeq::new(2),
                RecordVersion::new(101),
                ts(2),
                &initial
            )
            .await?,
        ApplyRecords::Applied
    );
    let stale = [
        RecordChange::Delete { key: a },
        RecordChange::Put {
            key: b,
            body: b"stale",
            expires_at: ts(30_000),
        },
    ];
    assert_eq!(
        database
            .apply(
                &lease,
                RecordBatchSeq::new(1),
                RecordVersion::new(102),
                ts(3),
                &stale
            )
            .await?,
        ApplyRecords::Fenced
    );
    let Renewal::Renewed(renewed) = database.renew(&lease, ts(3), ts(20_000)).await? else {
        return Err("lease renewal failed".into());
    };
    assert_eq!(
        database
            .apply(
                &renewed,
                RecordBatchSeq::new(1),
                RecordVersion::new(103),
                ts(4),
                &stale
            )
            .await?,
        ApplyRecords::Fenced
    );
    assert_eq!(
        database.get(&a, ts(4)).await?,
        Some(record(b"new", 101, 30_000))
    );
    assert_eq!(database.get(&b, ts(4)).await?, None);
    database.release(&renewed).await?;
    let next = match database
        .acquire(
            &candidate("next"),
            ts(5),
            ts(5),
            ts(25_000),
            Takeover::Observe,
        )
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("released lease was not acquired: {outcome:?}").into()),
    };
    assert_eq!(
        database
            .apply(
                &next,
                RecordBatchSeq::new(1),
                RecordVersion::new(104),
                ts(6),
                &stale
            )
            .await?,
        ApplyRecords::Applied
    );
    assert_eq!(database.get(&a, ts(6)).await?, None);
    assert_eq!(
        database.get(&b, ts(6)).await?,
        Some(record(b"stale", 104, 30_000))
    );
    Ok(())
}

/// A reset batch sequence never reuses the previous record incarnation.
///
/// # Errors
/// Returns a database error.
///
/// # Panics
/// Panics if a stale incarnation can mutate the successor's record.
pub async fn apply_new_epoch_preserves_record_incarnations<
    D: ExpiringRecordPort + LeaderLeasePort,
>(
    database: &D,
) -> TestResult {
    let first = acquired(database).await?;
    let key = key(RecordKind::ResourceHistory, 42);
    let old = [RecordChange::Put {
        key,
        body: b"old",
        expires_at: ts(30_000),
    }];
    assert_eq!(
        database
            .apply(
                &first,
                RecordBatchSeq::new(1),
                RecordVersion::new(41),
                ts(2),
                &old
            )
            .await?,
        ApplyRecords::Applied
    );
    database.release(&first).await?;
    let next = match database
        .acquire(
            &candidate("successor"),
            ts(3),
            ts(3),
            ts(20_000),
            Takeover::Observe,
        )
        .await?
    {
        Acquisition::Acquired(lease) => lease,
        outcome => return Err(format!("released lease was not acquired: {outcome:?}").into()),
    };
    let new = [RecordChange::Put {
        key,
        body: b"new",
        expires_at: ts(30_000),
    }];
    assert_eq!(
        database
            .apply(
                &next,
                RecordBatchSeq::new(1),
                RecordVersion::new(42),
                ts(4),
                &new
            )
            .await?,
        ApplyRecords::Applied
    );
    assert_eq!(
        database
            .replace(&key, RecordVersion::new(41), b"stale", ts(5), ts(30_000))
            .await?,
        Replace::Stale
    );
    assert_eq!(
        database.remove(&key, RecordVersion::new(41), ts(5)).await?,
        Remove::Stale
    );
    assert_eq!(
        database.get(&key, ts(5)).await?,
        Some(record(b"new", 42, 30_000))
    );
    Ok(())
}
