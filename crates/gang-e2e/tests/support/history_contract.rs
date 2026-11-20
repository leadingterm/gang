//! Behavioral persistence cases shared by all database backends.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_nativelink::history_fixture::{HistoryFixture, source, source_with_cap, tiny_source};
use gang_server::LearnedHistoryStore;
use gang_spi::{
    Acquisition, CandidateGeneration, CandidatePriority, ControllerId, ExpiringRecordPort,
    LeaderLeasePort, LearnedHistory, Lease, LeaseCandidate, PortError, RecordKind, Takeover,
};
use gang_testkit::TestResult;
use nativelink_scheduler::resource_sketch::{Observation, SketchKey, Snapshot};
use serde_json::json;

const DAY: u64 = 86_400_000;
const START: u64 = 100 * DAY;

fn at(ms: u64) -> Timestamp {
    Timestamp::from_unix_millis(ms)
}

fn store() -> LearnedHistoryStore {
    LearnedHistoryStore::new(NonZeroUsize::MIN.saturating_add(4 * 1024 * 1024 - 1))
}

async fn lease<D: LeaderLeasePort>(
    db: &D,
    holder: &str,
    now: u64,
    until: u64,
) -> Result<Lease, Box<dyn std::error::Error>> {
    let candidate = LeaseCandidate {
        holder: ControllerId::new(holder),
        priority: CandidatePriority::registered(CandidateGeneration::default()),
    };
    match db
        .acquire(&candidate, at(now), at(now), at(until), Takeover::Observe)
        .await?
    {
        Acquisition::Acquired(lease) => Ok(lease),
        Acquisition::Held(_) | Acquisition::Contending(_) => Err("fixture lease held".into()),
        Acquisition::Reserved => Err("fixture lease reserved".into()),
    }
}

async fn successor<D: LeaderLeasePort>(
    db: &D,
    current: &Lease,
    now: u64,
) -> Result<Lease, Box<dyn std::error::Error>> {
    db.release(current).await?;
    lease(db, "successor", now, now + 10 * DAY).await
}

fn observe(source: &HistoryFixture, key: &SketchKey, now: u64) {
    let observation = Observation {
        peak_memory_kb: 22_387,
        cpu_millicores: Some(1500),
        peak_disk_kb: 1536,
        now_s: now / 1000,
        ..Default::default()
    };
    if matches!(key, SketchKey::Command { .. }) {
        assert!(!source.resource().observe(key.clone(), &observation));
    }
    assert!(source.resource().observe(key.clone(), &observation));
}

fn snapshot(source: &HistoryFixture, now: u64) -> Result<Vec<LearnedHistory>, PortError> {
    let mut records = Vec::new();
    source.snapshot(at(now), &mut records)?;
    Ok(records)
}

async fn restore<D: ExpiringRecordPort>(
    db: &D,
    lease: &Lease,
    store: &mut LearnedHistoryStore,
    source: &HistoryFixture,
    now: u64,
) -> TestResult {
    let mut records = Vec::new();
    store.load(db, lease, at(now), &mut records).await?;
    source.restore(at(now), &records)?;
    Ok(())
}

/// Restores actual resource and duration estimates, including a chunked large key.
///
/// # Errors
/// Returns fixture or storage errors.
/// # Panics
/// Panics if learned estimates do not survive a restart.
pub async fn history_restart<D: ExpiringRecordPort + LeaderLeasePort>(db: &D) -> TestResult {
    let lease = lease(db, "first", START, START + 10 * DAY).await?;
    let first = source(2)?;
    let key = SketchKey::for_attempt(Some(&"x".repeat(410 * 1024)), None, "");
    observe(&first, &key, START);
    first.record("target", "compile", 1200, at(START));
    let expected = first.resource().shape(
        &first
            .resource()
            .lookup(&key, START / 1000)
            .ok_or("restart resource was not learned")?,
    );
    assert!(expected.memory_kb > 0 && expected.cpu_count > 0 && expected.disk_kb > 0);
    let mut writer = store();
    writer
        .flush(
            db,
            &lease,
            at(START + 1000),
            &snapshot(&first, START + 1000)?,
        )
        .await?;
    let lease = successor(db, &lease, START + 2000).await?;
    let second = source(2)?;
    restore(db, &lease, &mut store(), &second, START + 2000).await?;
    let entry = second
        .resource()
        .lookup(&key, (START + 2000) / 1000)
        .ok_or("restart resource was not restored")?;
    assert_eq!(second.resource().shape(&entry), expected);
    assert_eq!(
        second.expected("target", "compile", at(START + 2000)),
        Some(Duration::from_millis(1200))
    );
    Ok(())
}

/// Expired histories do not return after restart.
///
/// # Errors
/// Returns fixture or storage errors.
/// # Panics
/// Panics if an expired measurement is restored.
pub async fn history_expiry<D: ExpiringRecordPort + LeaderLeasePort>(db: &D) -> TestResult {
    let lease = lease(db, "first", START, START + 10 * DAY).await?;
    let first = source(2)?;
    let key = SketchKey::for_attempt(None, None, "expiry");
    observe(&first, &key, START);
    first.record("", "expiry", 900, at(START));
    store()
        .flush(
            db,
            &lease,
            at(START + 1000),
            &snapshot(&first, START + 1000)?,
        )
        .await?;
    let lease = successor(db, &lease, START + 2 * DAY).await?;
    let second = source(2)?;
    restore(db, &lease, &mut store(), &second, START + 2 * DAY).await?;
    assert!(
        second
            .resource()
            .lookup(&key, (START + 2 * DAY) / 1000)
            .is_none()
    );
    assert_eq!(second.expected("", "expiry", at(START + 2 * DAY)), None);
    Ok(())
}

/// Duration LRU eviction removes its previously persisted head.
///
/// # Errors
/// Returns fixture or storage errors.
/// # Panics
/// Panics if an evicted record remains in storage or is restored.
pub async fn history_eviction<D: ExpiringRecordPort + LeaderLeasePort>(db: &D) -> TestResult {
    let lease = lease(db, "first", START, START + 10 * DAY).await?;
    let first = source(2)?;
    first.record("", "old", 500, at(START));
    let old = snapshot(&first, START)?
        .into_iter()
        .find(|r| r.key.kind == RecordKind::DurationHistory)
        .ok_or("eviction fixture has no duration")?;
    let mut writer = store();
    writer
        .flush(db, &lease, at(START), std::slice::from_ref(&old))
        .await?;
    for mnemonic in ["one", "two", "three"] {
        first.record("", mnemonic, 700, at(START + 1000));
    }
    writer
        .flush(
            db,
            &lease,
            at(START + 1000),
            &snapshot(&first, START + 1000)?,
        )
        .await?;
    assert!(db.get(&old.key, at(START + 1000)).await?.is_none());
    let lease = successor(db, &lease, START + 2000).await?;
    let mut reader = store();
    let second = source(2)?;
    restore(db, &lease, &mut reader, &second, START + 2000).await?;
    assert_eq!(second.expected("", "old", at(START + 2000)), None);
    assert_eq!(
        second.expected("", "three", at(START + 2000)),
        Some(Duration::from_millis(700))
    );
    // A successor with a lower cap retains the newest records, independently
    // of the database's key ordering, and removes the evicted head on flush.
    let larger = source_with_cap(2, 4)?;
    for (index, name) in ["cap-old", "cap-one", "cap-two", "cap-three"]
        .into_iter()
        .enumerate()
    {
        larger.record("", name, 1000, at(START + 3000 + index as u64));
    }
    let large_records = snapshot(&larger, START + 4000)?;
    let oldest = large_records
        .first()
        .ok_or("capacity fixture has no durations")?
        .key;
    reader
        .flush(db, &lease, at(START + 4000), &large_records)
        .await?;
    let lease = successor(db, &lease, START + 5000).await?;
    let smaller = source(2)?;
    let mut reader = store();
    restore(db, &lease, &mut reader, &smaller, START + 5000).await?;
    assert_eq!(smaller.expected("", "cap-old", at(START + 5000)), None);
    for name in ["cap-one", "cap-two", "cap-three"] {
        assert_eq!(
            smaller.expected("", name, at(START + 5000)),
            Some(Duration::from_secs(1))
        );
    }
    reader
        .flush(
            db,
            &lease,
            at(START + 5000),
            &snapshot(&smaller, START + 5000)?,
        )
        .await?;
    assert!(db.get(&oldest, at(START + 5000)).await?.is_none());
    Ok(())
}

/// A new leader with fresh production collectors sees the previous leader's measurements.
///
/// # Errors
/// Returns fixture or storage errors.
/// # Panics
/// Panics if the successor cannot use the previous leader's estimates.
pub async fn history_new_leader<D: ExpiringRecordPort + LeaderLeasePort>(db: &D) -> TestResult {
    let first_lease = lease(db, "first", START, START + 5000).await?;
    let first = source(2)?;
    let key = SketchKey::for_attempt(None, None, "leader");
    observe(&first, &key, START);
    let expected = first.resource().shape(
        &first
            .resource()
            .lookup(&key, START / 1000)
            .ok_or("leader resource was not learned")?,
    );
    assert!(expected.memory_kb > 0 && expected.cpu_count > 0 && expected.disk_kb > 0);
    first.record("", "leader", 800, at(START));
    store()
        .flush(
            db,
            &first_lease,
            at(START + 1000),
            &snapshot(&first, START + 1000)?,
        )
        .await?;
    let next_lease = lease(db, "second", START + 5001, START + 3 * DAY).await?;
    let second = source(2)?;
    restore(db, &next_lease, &mut store(), &second, START + 6000).await?;
    let entry = second
        .resource()
        .lookup(&key, (START + 6000) / 1000)
        .ok_or("successor resource was not restored")?;
    assert_eq!(second.resource().shape(&entry), expected);
    assert_eq!(
        second.expected("", "leader", at(START + 6000)),
        Some(Duration::from_millis(800))
    );
    Ok(())
}

/// Incompatible format, window and sampler generation are discarded and deleted on flush.
///
/// # Errors
/// Returns fixture or storage errors.
/// # Panics
/// Panics if incompatible data is restored or retained.
pub async fn history_incompatible<D: ExpiringRecordPort + LeaderLeasePort>(db: &D) -> TestResult {
    let lease = lease(db, "first", START, START + 10 * DAY).await?;
    let first = source(2)?;
    for name in ["format", "window", "sampler"] {
        observe(&first, &SketchKey::for_attempt(None, None, name), START);
        first.record("", name, 500, at(START));
    }
    let mut records = snapshot(&first, START)?;
    for record in &mut records {
        match record.key.kind {
            RecordKind::ResourceHistory => {
                let mut snap = Snapshot::decode(&record.body[9..], 1)?;
                let label = match &snap
                    .entries
                    .first()
                    .ok_or("resource fixture has no entry")?
                    .0
                {
                    SketchKey::Command { digest } => digest.as_ref(),
                    _ => return Err("resource fixture key is not a command".into()),
                };
                match label {
                    "format" => record.body[0] = 99,
                    "window" => record.body[1..9].copy_from_slice(&1_u64.to_be_bytes()),
                    "sampler" => {
                        snap.sampler_generation += 1;
                        record.body.truncate(9);
                        record.body.extend_from_slice(&snap.encode());
                    }
                    _ => return Err("unexpected resource fixture key".into()),
                }
            }
            RecordKind::DurationHistory => {
                let mut value: serde_json::Value = serde_json::from_slice(&record.body)?;
                match value["key"]["Mnemonic"].as_str() {
                    Some("format") => value["version"] = json!(99),
                    Some("window") => value["window_ms"] = json!(1),
                    Some("sampler") => value["sampler_generation"] = json!(999),
                    _ => return Err("unexpected duration fixture key".into()),
                }
                record.body = serde_json::to_vec(&value)?;
            }
            _ => return Err("unexpected history record kind".into()),
        }
    }
    let mut writer = store();
    writer.flush(db, &lease, at(START), &records).await?;
    let lease = successor(db, &lease, START + 1000).await?;
    let second = source(2)?;
    let mut reader = store();
    restore(db, &lease, &mut reader, &second, START + 1000).await?;
    for name in ["format", "window", "sampler"] {
        assert!(
            second
                .resource()
                .lookup(
                    &SketchKey::for_attempt(None, None, name),
                    (START + 1000) / 1000
                )
                .is_none()
        );
        assert_eq!(second.expected("", name, at(START + 1000)), None);
    }
    reader
        .flush(
            db,
            &lease,
            at(START + 1000),
            &snapshot(&second, START + 1000)?,
        )
        .await?;
    for record in records {
        assert!(db.get(&record.key, at(START + 1000)).await?.is_none());
    }
    Ok(())
}

/// Resource LRU eviction removes the persisted head before its original expiry.
///
/// # Errors
/// Returns fixture or storage errors.
/// # Panics
/// Panics if the evicted measurement survives or the replacement is lost.
pub async fn history_resource_eviction<D: ExpiringRecordPort + LeaderLeasePort>(
    db: &D,
) -> TestResult {
    let lease = lease(db, "first", START, START + 10 * DAY).await?;
    let first = tiny_source();
    let old = SketchKey::for_attempt(Some("old-target"), None, "");
    let new = SketchKey::for_attempt(Some("new-target"), None, "");
    observe(&first, &old, START);
    let records = snapshot(&first, START)?;
    assert_eq!(records.len(), 1);
    let old_head = records[0].key;
    let mut writer = store();
    writer.flush(db, &lease, at(START), &records).await?;
    observe(&first, &new, START + 1000);
    assert!(
        first
            .resource()
            .lookup(&old, (START + 1000) / 1000)
            .is_none()
    );
    let expected = first.resource().shape(
        &first
            .resource()
            .lookup(&new, (START + 1000) / 1000)
            .ok_or("replacement resource was not learned")?,
    );
    assert!(expected.memory_kb > 0 && expected.cpu_count > 0 && expected.disk_kb > 0);
    writer
        .flush(
            db,
            &lease,
            at(START + 1000),
            &snapshot(&first, START + 1000)?,
        )
        .await?;
    assert!(db.get(&old_head, at(START + 1000)).await?.is_none());
    let lease = successor(db, &lease, START + 2000).await?;
    let second = tiny_source();
    restore(db, &lease, &mut store(), &second, START + 2000).await?;
    assert!(
        second
            .resource()
            .lookup(&old, (START + 2000) / 1000)
            .is_none()
    );
    let restored = second
        .resource()
        .lookup(&new, (START + 2000) / 1000)
        .ok_or("replacement resource was not restored")?;
    assert_eq!(second.resource().shape(&restored), expected);
    Ok(())
}
