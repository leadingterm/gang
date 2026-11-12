//! Tests for the `SQLite` `StateRepositoryPort` implementation: certificate, revocation,
//! principal, and scaling-event persistence, plus schema migration and async runtime behavior.

use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PortError, PrincipalRecord, RecordedScalingEvent,
    ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, StateRepositoryPort, Subject,
};
use gang_sqlite::{JournalMode, Sqlite, SqliteConfig, SqliteError};
use gang_types::{
    CertSerial, FleetName, Identity, InstanceId, NonZeroDuration, Principal, ProviderName,
    ScalingCause, WorkerId,
};
use tempfile::TempDir;

#[cfg(test)]
mod support;

#[cfg(test)]
mod fixtures {
    use super::*;

    pub(super) fn config(dir: &TempDir) -> SqliteConfig {
        SqliteConfig::new(dir.path().join("gang.db"))
    }

    pub(super) fn store(dir: &TempDir) -> Sqlite {
        open(config(dir))
    }

    pub(super) fn database_path(dir: &TempDir) -> PathBuf {
        dir.path().join("gang.db")
    }

    pub(super) fn serial(value: &str) -> CertSerial {
        CertSerial::try_new(value).expect("test serial is valid")
    }

    pub(super) fn ts(millis: u64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    pub(super) fn provider(name: &str) -> ProviderName {
        ProviderName::try_new(name).expect("test provider is valid")
    }

    pub(super) fn principal(name: &str) -> Principal {
        Principal::try_new(name).expect("test principal is valid")
    }

    /// Opens a raw connection to the store's database file, independent of [`Sqlite`].
    pub(super) fn raw_connection(dir: &TempDir) -> rusqlite::Connection {
        rusqlite::Connection::open(database_path(dir)).expect("raw database opens")
    }

    /// Reads every row of `sql` as text, so that NULL and integers compare uniformly.
    pub(super) fn text_rows(
        connection: &rusqlite::Connection,
        sql: &str,
        columns: usize,
    ) -> Vec<Vec<String>> {
        let mut statement = connection.prepare(sql).expect("raw query prepares");
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|index| row.get::<_, String>(index))
                    .collect::<Result<Vec<String>, _>>()
            })
            .expect("raw query runs");
        rows.map(|row| row.expect("raw row reads")).collect()
    }

    pub(super) fn expected_row(values: [&str; 7]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    pub(super) fn scaling(at: u64, kind: ScalingEventKind) -> ScalingAuditEvent {
        ScalingAuditEvent {
            at: ts(at),
            fleet: FleetName::try_new("gang-x86").expect("test fleet name is valid"),
            kind,
            cause: None,
        }
    }

    pub(super) fn worker_id(name: &str) -> WorkerId {
        WorkerId::try_new(name).expect("test worker id is valid")
    }

    pub(super) fn instance_id(name: &str) -> InstanceId {
        InstanceId::try_new(name).expect("test instance id is valid")
    }

    pub(super) fn identity(provider_name: &str, principal_name: &str) -> Identity {
        Identity::new(provider(provider_name), principal(principal_name))
    }
}

use fixtures::*;
use support::{issued, open};

#[tokio::test]
async fn record_then_certificate_returns_the_same_record() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let record = issued("01", 1_000);

    store
        .record_issued(&record)
        .await
        .expect("record is stored");

    assert_eq!(
        store
            .certificate(&serial("01"))
            .await
            .expect("lookup succeeds"),
        Some(record)
    );
}

#[tokio::test]
async fn unknown_serial_has_no_certificate() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    assert_eq!(
        store
            .certificate(&serial("ff"))
            .await
            .expect("lookup succeeds"),
        None
    );
}

#[tokio::test]
async fn revoke_marks_the_certificate_revoked() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let record = issued("01", 1_000);
    store
        .record_issued(&record)
        .await
        .expect("record is stored");

    store
        .revoke(&serial("01"), ts(5_000))
        .await
        .expect("revoke succeeds");

    assert_eq!(
        store
            .certificate(&serial("01"))
            .await
            .expect("lookup succeeds"),
        Some(IssuedCertRecord {
            status: CertStatus::Revoked { at: ts(5_000) },
            ..record
        })
    );
}

#[tokio::test]
async fn revoking_an_unknown_serial_is_not_found() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    let error = store
        .revoke(&serial("ff"), ts(5_000))
        .await
        .expect_err("unknown serial cannot be revoked");

    let PortError::NotFound(subject) = error else {
        panic!("expected NotFound, got {error:?}");
    };
    assert_eq!(subject, Subject::Certificate(serial("ff")));
}

#[tokio::test]
async fn revoking_twice_keeps_the_first_revocation_time() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let record = issued("01", 1_000);
    store
        .record_issued(&record)
        .await
        .expect("record is stored");

    store
        .revoke(&serial("01"), ts(5_000))
        .await
        .expect("first revoke succeeds");
    store
        .revoke(&serial("01"), ts(9_000))
        .await
        .expect("second revoke succeeds");

    assert_eq!(
        store
            .certificate(&serial("01"))
            .await
            .expect("lookup succeeds"),
        Some(IssuedCertRecord {
            status: CertStatus::Revoked { at: ts(5_000) },
            ..record
        })
    );
}

#[tokio::test]
async fn revoked_lists_only_revoked_certificates_by_revocation_time_then_serial() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    for (serial_text, issued_at) in [("01", 1_000), ("02", 1_100), ("03", 1_200)] {
        store
            .record_issued(&issued(serial_text, issued_at))
            .await
            .expect("record is stored");
    }

    // Revocation order is the reverse of serial order, so only the sort key decides the result.
    store
        .revoke(&serial("01"), ts(2_000))
        .await
        .expect("revoke succeeds");
    store
        .revoke(&serial("03"), ts(1_000))
        .await
        .expect("revoke succeeds");

    let revoked = store.revoked().await.expect("listing succeeds");

    assert_eq!(
        revoked,
        vec![
            IssuedCertRecord {
                status: CertStatus::Revoked { at: ts(1_000) },
                ..issued("03", 1_200)
            },
            IssuedCertRecord {
                status: CertStatus::Revoked { at: ts(2_000) },
                ..issued("01", 1_000)
            },
        ]
    );
}

#[tokio::test]
async fn revoked_breaks_revocation_time_ties_by_serial() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    for (serial_text, issued_at) in [("02", 1_000), ("03", 1_100)] {
        store
            .record_issued(&issued(serial_text, issued_at))
            .await
            .expect("record is stored");
    }

    store
        .revoke(&serial("03"), ts(5_000))
        .await
        .expect("revoke succeeds");
    store
        .revoke(&serial("02"), ts(5_000))
        .await
        .expect("revoke succeeds");

    let serials: Vec<CertSerial> = store
        .revoked()
        .await
        .expect("listing succeeds")
        .into_iter()
        .map(|record| record.serial)
        .collect();

    assert_eq!(serials, vec![serial("02"), serial("03")]);
}

#[tokio::test]
async fn duplicate_record_issued_is_a_backend_error() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let record = issued("01", 1_000);
    store
        .record_issued(&record)
        .await
        .expect("first record is stored");

    let error = store
        .record_issued(&record)
        .await
        .expect_err("duplicate serial is rejected");

    assert!(
        matches!(error, PortError::Backend(_)),
        "expected Backend, got {error:?}"
    );
}

#[tokio::test]
async fn data_survives_reopening_the_database() {
    let dir = TempDir::new().expect("temp dir");
    let record = issued("01", 1_000);
    {
        let store = store(&dir);
        store
            .record_issued(&record)
            .await
            .expect("record is stored");
        store
            .revoke(&serial("01"), ts(5_000))
            .await
            .expect("revoke succeeds");
    }

    let reopened = store(&dir);

    assert_eq!(
        reopened
            .certificate(&serial("01"))
            .await
            .expect("lookup succeeds"),
        Some(IssuedCertRecord {
            status: CertStatus::Revoked { at: ts(5_000) },
            ..record
        })
    );
}

#[tokio::test]
async fn certificates_of_is_empty_when_nothing_was_issued() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    let certificates = store
        .certificates_of(&identity("alpha", "dev@example.test"))
        .await
        .expect("listing succeeds");

    assert!(
        certificates.is_empty(),
        "expected none, got {certificates:?}"
    );
}

#[tokio::test]
async fn certificates_of_lists_one_identitys_certificates_by_issue_time_then_serial() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let other_identity = IssuedCertRecord {
        identity: identity("alpha", "ops@example.test"),
        ..issued("04", 1_000)
    };
    // Insertion order differs from the expected order, so the ORDER BY decides the result.
    for record in [
        issued("02", 1_100),
        issued("03", 1_000),
        issued("01", 1_000),
        other_identity,
    ] {
        store
            .record_issued(&record)
            .await
            .expect("record is stored");
    }

    let certificates = store
        .certificates_of(&identity("alpha", "dev@example.test"))
        .await
        .expect("listing succeeds");

    assert_eq!(
        certificates,
        vec![
            issued("01", 1_000),
            issued("03", 1_000),
            issued("02", 1_100)
        ]
    );
}

#[tokio::test]
async fn revoking_removes_the_certificate_from_certificates_of() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    for record in [issued("01", 1_000), issued("02", 1_100)] {
        store
            .record_issued(&record)
            .await
            .expect("record is stored");
    }
    let who = identity("alpha", "dev@example.test");
    let before = store.certificates_of(&who).await.expect("listing succeeds");
    assert_eq!(before, vec![issued("01", 1_000), issued("02", 1_100)]);

    store
        .revoke(&serial("01"), ts(2_000))
        .await
        .expect("revoke succeeds");

    let after = store.certificates_of(&who).await.expect("listing succeeds");
    assert_eq!(after, vec![issued("02", 1_100)]);
}

#[tokio::test]
async fn certificates_of_excludes_the_same_principal_under_another_provider() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let beta = IssuedCertRecord {
        identity: identity("beta", "dev@example.test"),
        ..issued("02", 1_100)
    };
    store
        .record_issued(&issued("01", 1_000))
        .await
        .expect("record is stored");
    store.record_issued(&beta).await.expect("record is stored");

    let alpha = store
        .certificates_of(&identity("alpha", "dev@example.test"))
        .await
        .expect("listing succeeds");
    let beta_listing = store
        .certificates_of(&identity("beta", "dev@example.test"))
        .await
        .expect("listing succeeds");

    assert_eq!(alpha, vec![issued("01", 1_000)]);
    assert_eq!(beta_listing, vec![beta]);
}

#[tokio::test]
async fn certificates_of_survives_reopening_the_database() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store = store(&dir);
        for record in [issued("01", 1_000), issued("02", 1_100)] {
            store
                .record_issued(&record)
                .await
                .expect("record is stored");
        }
        store
            .revoke(&serial("02"), ts(5_000))
            .await
            .expect("revoke succeeds");
    }

    let reopened = store(&dir);

    assert_eq!(
        reopened
            .certificates_of(&identity("alpha", "dev@example.test"))
            .await
            .expect("listing succeeds"),
        vec![issued("01", 1_000)]
    );
}

/// `issued_certificates` with the migration 1 columns and indexes in a different order.
const REORDERED_ISSUED_CERTIFICATES: &str = "
BEGIN;
CREATE TABLE issued_certificates_new (
    revoked_at  INTEGER,
    principal   TEXT    NOT NULL,
    expires_at  INTEGER NOT NULL,
    serial      TEXT    NOT NULL PRIMARY KEY,
    issued_at   INTEGER NOT NULL,
    provider    TEXT    NOT NULL
) STRICT;
INSERT INTO issued_certificates_new (serial, provider, principal, issued_at, expires_at, revoked_at)
    SELECT serial, provider, principal, issued_at, expires_at, revoked_at FROM issued_certificates;
DROP TABLE issued_certificates;
ALTER TABLE issued_certificates_new RENAME TO issued_certificates;
CREATE INDEX issued_certificates_revoked ON issued_certificates (revoked_at, serial) WHERE revoked_at IS NOT NULL;
CREATE INDEX issued_certificates_identity ON issued_certificates (provider, principal, issued_at, serial) WHERE revoked_at IS NULL;
COMMIT;
";

#[tokio::test]
async fn reads_survive_a_reordered_schema() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store = store(&dir);
        store
            .record_issued(&issued("01", 1_000))
            .await
            .expect("record is stored");
        store
            .record_issued(&issued("02", 1_100))
            .await
            .expect("record is stored");
        store
            .revoke(&serial("02"), ts(5_000))
            .await
            .expect("revoke succeeds");
    }
    raw_connection(&dir)
        .execute_batch(REORDERED_ISSUED_CERTIFICATES)
        .expect("table is reordered");

    let store = store(&dir);

    assert_eq!(
        store
            .certificate(&serial("01"))
            .await
            .expect("lookup succeeds"),
        Some(issued("01", 1_000))
    );
    let revoked = IssuedCertRecord {
        status: CertStatus::Revoked { at: ts(5_000) },
        ..issued("02", 1_100)
    };
    assert_eq!(
        store
            .certificate(&serial("02"))
            .await
            .expect("lookup succeeds"),
        Some(revoked.clone())
    );
    assert_eq!(
        store.revoked().await.expect("listing succeeds"),
        vec![revoked]
    );
    assert_eq!(
        store
            .certificates_of(&identity("alpha", "dev@example.test"))
            .await
            .expect("listing succeeds"),
        vec![issued("01", 1_000)]
    );
}

fn recorded(seq: u64, event: &ScalingAuditEvent) -> RecordedScalingEvent {
    RecordedScalingEvent {
        seq: ScalingEventSeq::new(seq),
        event: event.clone(),
    }
}

fn limit(value: usize) -> NonZeroUsize {
    NonZeroUsize::MIN.saturating_add(value.saturating_sub(1))
}

#[tokio::test]
async fn scaling_events_are_appended_in_insertion_order() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let fleet = FleetName::try_new("gang-x86").expect("test fleet name is valid");
    let events = [
        ScalingAuditEvent {
            at: ts(1_000),
            fleet: fleet.clone(),
            kind: ScalingEventKind::ScaledOut { from: 1, to: 2 },
            cause: None,
        },
        ScalingAuditEvent {
            at: ts(2_000),
            fleet: fleet.clone(),
            kind: ScalingEventKind::DrainStarted {
                worker: WorkerId::try_new("worker-1").expect("test worker id is valid"),
            },
            cause: None,
        },
        ScalingAuditEvent {
            at: ts(3_000),
            fleet: fleet.clone(),
            kind: ScalingEventKind::Terminated {
                instance: InstanceId::try_new("i-0abc123").expect("test instance id is valid"),
            },
            cause: None,
        },
        ScalingAuditEvent {
            at: ts(4_000),
            fleet: fleet.clone(),
            kind: ScalingEventKind::DrainCancelled {
                worker: WorkerId::try_new("worker-1").expect("test worker id is valid"),
            },
            cause: None,
        },
    ];

    let mut seqs = Vec::new();
    for event in &events {
        seqs.push(
            store
                .record_scaling_event(event)
                .await
                .expect("event is appended"),
        );
    }
    assert_eq!(
        seqs,
        vec![
            ScalingEventSeq::new(1),
            ScalingEventSeq::new(2),
            ScalingEventSeq::new(3),
            ScalingEventSeq::new(4),
        ]
    );

    let connection = raw_connection(&dir);
    let rows = text_rows(
        &connection,
        "SELECT CAST(at AS TEXT), fleet, kind, \
                COALESCE(CAST(from_capacity AS TEXT), '-'), \
                COALESCE(CAST(to_capacity AS TEXT), '-'), \
                COALESCE(subject, '-'), COALESCE(detail, '-') \
         FROM scaling_audit ORDER BY id",
        7,
    );

    assert_eq!(
        rows,
        vec![
            expected_row(["1000", "gang-x86", "scaled_out", "1", "2", "-", "-"]),
            expected_row([
                "2000",
                "gang-x86",
                "drain_started",
                "-",
                "-",
                "worker-1",
                "-"
            ]),
            expected_row(["3000", "gang-x86", "terminated", "-", "-", "i-0abc123", "-"]),
            expected_row([
                "4000",
                "gang-x86",
                "drain_cancelled",
                "-",
                "-",
                "worker-1",
                "-"
            ]),
        ]
    );
}

#[tokio::test]
async fn early_exit_round_trips_with_failure_detail() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let failure = "fail  gang worker: the scratch volume is too small";
    let early_exit = scaling(
        1_000,
        ScalingEventKind::ExitedEarly {
            instance: instance_id("local-1"),
            failure: failure.to_owned(),
        },
    );
    let terminated = scaling(
        2_000,
        ScalingEventKind::Terminated {
            instance: instance_id("local-1"),
        },
    );
    for event in [&early_exit, &terminated] {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let connection = raw_connection(&dir);
    let rows = text_rows(
        &connection,
        "SELECT CAST(at AS TEXT), fleet, kind, \
                COALESCE(CAST(from_capacity AS TEXT), '-'), \
                COALESCE(CAST(to_capacity AS TEXT), '-'), \
                COALESCE(subject, '-'), COALESCE(detail, '-') \
         FROM scaling_audit ORDER BY id",
        7,
    );
    assert_eq!(
        rows,
        vec![
            expected_row([
                "1000",
                "gang-x86",
                "exited_early",
                "-",
                "-",
                "local-1",
                failure
            ]),
            expected_row(["2000", "gang-x86", "terminated", "-", "-", "local-1", "-"]),
        ]
    );

    let mut events = Vec::new();
    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert_eq!(
        events,
        vec![recorded(1, &early_exit), recorded(2, &terminated)]
    );
}

#[tokio::test]
async fn scaling_events_come_back_in_sequence_order_after_the_cursor() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    // Timestamps are deliberately out of order: the cursor follows the sequence, not the time.
    let first = scaling(2_000, ScalingEventKind::ScaledOut { from: 0, to: 1 });
    let second = scaling(
        1_000,
        ScalingEventKind::DrainStarted {
            worker: worker_id("worker-1"),
        },
    );
    let third = scaling(
        3_000,
        ScalingEventKind::Terminated {
            instance: instance_id("i-0abc123"),
        },
    );
    for event in [&first, &second, &third] {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut events = Vec::new();
    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert_eq!(
        events,
        vec![
            recorded(1, &first),
            recorded(2, &second),
            recorded(3, &third),
        ]
    );

    store
        .scaling_events(ScalingEventSeq::new(1), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert_eq!(events, vec![recorded(2, &second), recorded(3, &third)]);

    store
        .scaling_events(ScalingEventSeq::new(2), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert_eq!(events, vec![recorded(3, &third)]);

    store
        .scaling_events(ScalingEventSeq::new(3), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert!(events.is_empty());
}

#[tokio::test]
async fn scaling_events_on_an_empty_log_are_empty() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let mut events = vec![recorded(
        1,
        &scaling(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 }),
    )];

    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");

    assert!(events.is_empty());
}

#[tokio::test]
async fn scaling_events_with_equal_times_keep_recording_order() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let recorded_events = [
        scaling(
            5_000,
            ScalingEventKind::DrainStarted {
                worker: worker_id("worker-1"),
            },
        ),
        scaling(
            4_000,
            ScalingEventKind::Terminated {
                instance: instance_id("i-0abc123"),
            },
        ),
        scaling(5_000, ScalingEventKind::ScaledOut { from: 1, to: 2 }),
        scaling(
            5_000,
            ScalingEventKind::DrainCancelled {
                worker: worker_id("worker-1"),
            },
        ),
    ];
    for event in &recorded_events {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut events = Vec::new();
    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert_eq!(
        events,
        vec![
            recorded(1, &recorded_events[0]),
            recorded(2, &recorded_events[1]),
            recorded(3, &recorded_events[2]),
            recorded(4, &recorded_events[3]),
        ]
    );
}

#[tokio::test]
async fn every_scaling_kind_round_trips_through_the_store() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let recorded_events = [
        scaling(
            1_000,
            ScalingEventKind::ScaledOut {
                from: 2,
                to: u32::MAX,
            },
        ),
        scaling(
            2_000,
            ScalingEventKind::DrainStarted {
                worker: worker_id("worker-1"),
            },
        ),
        scaling(
            3_000,
            ScalingEventKind::DrainCancelled {
                worker: worker_id("worker-1"),
            },
        ),
        scaling(
            4_000,
            ScalingEventKind::Terminated {
                instance: instance_id("i-0abc123"),
            },
        ),
        scaling(
            5_000,
            ScalingEventKind::WorkerRegistered {
                worker: worker_id("worker-2"),
            },
        ),
        scaling(
            6_000,
            ScalingEventKind::WorkerDeregistered {
                worker: worker_id("worker-2"),
            },
        ),
    ];
    for event in &recorded_events {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut events = Vec::new();
    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");
    let expected: Vec<RecordedScalingEvent> = recorded_events
        .iter()
        .enumerate()
        .map(|(index, event)| recorded(index as u64 + 1, event))
        .collect();
    assert_eq!(events, expected);
}

#[tokio::test]
async fn every_scaling_cause_round_trips_through_the_store() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let mut recorded_events = Vec::new();
    for (index, cause) in ScalingCause::ALL.into_iter().enumerate() {
        let mut event = scaling(
            1_000 * (index as u64 + 1),
            ScalingEventKind::Terminated {
                instance: instance_id("i-0abc123"),
            },
        );
        event.cause = Some(cause);
        store
            .record_scaling_event(&event)
            .await
            .expect("event is appended");
        recorded_events.push(event);
    }

    let mut events = Vec::new();
    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");
    let expected: Vec<RecordedScalingEvent> = recorded_events
        .iter()
        .enumerate()
        .map(|(index, event)| recorded(index as u64 + 1, event))
        .collect();
    assert_eq!(events, expected);
}

#[tokio::test]
async fn scaling_events_overwrite_the_buffer_instead_of_appending() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let recorded_event = scaling(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 });
    store
        .record_scaling_event(&recorded_event)
        .await
        .expect("event is appended");

    let mut events = vec![RecordedScalingEvent {
        seq: ScalingEventSeq::new(99),
        event: scaling(
            9_000,
            ScalingEventKind::Terminated {
                instance: instance_id("stale-i"),
            },
        ),
    }];
    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert_eq!(events, vec![recorded(1, &recorded_event)]);

    store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert_eq!(events, vec![recorded(1, &recorded_event)]);

    store
        .scaling_events(ScalingEventSeq::new(1), None, limit(10), &mut events)
        .await
        .expect("events are read");
    assert!(events.is_empty());
}

#[tokio::test]
async fn scaling_events_reject_a_corrupt_row() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    store
        .record_scaling_event(&scaling(
            1_000,
            ScalingEventKind::ScaledOut { from: 0, to: 1 },
        ))
        .await
        .expect("event is appended");

    let connection = raw_connection(&dir);
    connection
        .execute(
            "INSERT INTO scaling_audit (at, fleet, kind, from_capacity, to_capacity) \
             VALUES (:at, :fleet, :kind, :from, :to)",
            rusqlite::named_params! {
                ":at": 2_000,
                ":fleet": "Sparc",
                ":kind": "scaled_out",
                ":from": 1,
                ":to": 2,
            },
        )
        .expect("raw insert succeeds");

    let mut events = Vec::new();
    let error = store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect_err("corrupt row is rejected");
    assert!(matches!(error, PortError::Backend(_)));
    assert!(error.to_string().contains("stored fleet is corrupt"));
}

#[tokio::test]
async fn scaling_events_reject_an_unknown_cause() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    store
        .record_scaling_event(&scaling(
            1_000,
            ScalingEventKind::ScaledOut { from: 0, to: 1 },
        ))
        .await
        .expect("event is appended");

    let connection = raw_connection(&dir);
    connection
        .execute(
            "INSERT INTO scaling_audit (at, fleet, kind, from_capacity, to_capacity, cause) \
             VALUES (:at, :fleet, :kind, :from, :to, :cause)",
            rusqlite::named_params! {
                ":at": 2_000,
                ":fleet": "gang-x86",
                ":kind": "scaled_out",
                ":from": 1,
                ":to": 2,
                ":cause": "idle",
            },
        )
        .expect("raw insert succeeds");

    let mut events = Vec::new();
    let error = store
        .scaling_events(ScalingEventSeq::default(), None, limit(10), &mut events)
        .await
        .expect_err("unknown cause is rejected");
    assert!(matches!(error, PortError::Backend(_)));
    assert!(error.to_string().contains("stored cause is corrupt"));
}

#[tokio::test]
async fn scaling_events_reject_a_cursor_beyond_the_stored_range() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let mut events = Vec::new();

    let error = store
        .scaling_events(ScalingEventSeq::new(u64::MAX), None, limit(10), &mut events)
        .await
        .expect_err("cursor beyond i64::MAX is rejected");

    assert!(matches!(error, PortError::Backend(_)));
}

#[tokio::test]
async fn scaling_events_before_bound_excludes_that_sequence() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let events = [
        scaling(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 }),
        scaling(2_000, ScalingEventKind::ScaledOut { from: 1, to: 2 }),
        scaling(3_000, ScalingEventKind::ScaledOut { from: 2, to: 3 }),
    ];
    for event in &events {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut page = Vec::new();
    let more = store
        .scaling_events(
            ScalingEventSeq::default(),
            Some(ScalingEventSeq::new(3)),
            limit(10),
            &mut page,
        )
        .await
        .expect("events are read");
    assert_eq!(page, vec![recorded(1, &events[0]), recorded(2, &events[1])]);
    assert!(!more);
}

#[tokio::test]
async fn scaling_events_since_and_before_bound_both_exclusively() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let events = [
        scaling(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 }),
        scaling(2_000, ScalingEventKind::ScaledOut { from: 1, to: 2 }),
        scaling(3_000, ScalingEventKind::ScaledOut { from: 2, to: 3 }),
        scaling(4_000, ScalingEventKind::ScaledOut { from: 3, to: 4 }),
        scaling(5_000, ScalingEventKind::ScaledOut { from: 4, to: 5 }),
    ];
    for event in &events {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut page = Vec::new();
    let more = store
        .scaling_events(
            ScalingEventSeq::new(1),
            Some(ScalingEventSeq::new(4)),
            limit(10),
            &mut page,
        )
        .await
        .expect("events are read");
    assert_eq!(page, vec![recorded(2, &events[1]), recorded(3, &events[2])]);
    assert!(!more);
}

#[tokio::test]
async fn scaling_events_limit_keeps_the_newest_rows_in_ascending_order() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let events = [
        scaling(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 }),
        scaling(2_000, ScalingEventKind::ScaledOut { from: 1, to: 2 }),
        scaling(3_000, ScalingEventKind::ScaledOut { from: 2, to: 3 }),
        scaling(4_000, ScalingEventKind::ScaledOut { from: 3, to: 4 }),
        scaling(5_000, ScalingEventKind::ScaledOut { from: 4, to: 5 }),
    ];
    for event in &events {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut newest = Vec::new();
    let more = store
        .scaling_events(ScalingEventSeq::default(), None, limit(2), &mut newest)
        .await
        .expect("events are read");
    assert_eq!(
        newest,
        vec![recorded(4, &events[3]), recorded(5, &events[4])]
    );
    assert!(more);

    let mut older = Vec::new();
    let more = store
        .scaling_events(
            ScalingEventSeq::default(),
            Some(ScalingEventSeq::new(4)),
            limit(2),
            &mut older,
        )
        .await
        .expect("events are read");
    assert_eq!(
        older,
        vec![recorded(2, &events[1]), recorded(3, &events[2])]
    );
    assert!(more);
}

#[tokio::test]
async fn scaling_events_more_is_false_at_the_limit_and_true_one_past_it() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let events = [
        scaling(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 }),
        scaling(2_000, ScalingEventKind::ScaledOut { from: 1, to: 2 }),
        scaling(3_000, ScalingEventKind::ScaledOut { from: 2, to: 3 }),
    ];
    for event in &events {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut exact = Vec::new();
    let more = store
        .scaling_events(ScalingEventSeq::default(), None, limit(3), &mut exact)
        .await
        .expect("events are read");
    assert_eq!(exact.len(), 3);
    assert!(!more);

    let mut one_past = Vec::new();
    let more = store
        .scaling_events(ScalingEventSeq::default(), None, limit(2), &mut one_past)
        .await
        .expect("events are read");
    assert_eq!(
        one_past,
        vec![recorded(2, &events[1]), recorded(3, &events[2])]
    );
    assert!(more);
}

#[tokio::test]
async fn scaling_events_on_an_empty_window_are_empty_and_not_more() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let events = [
        scaling(1_000, ScalingEventKind::ScaledOut { from: 0, to: 1 }),
        scaling(2_000, ScalingEventKind::ScaledOut { from: 1, to: 2 }),
        scaling(3_000, ScalingEventKind::ScaledOut { from: 2, to: 3 }),
    ];
    for event in &events {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut page = vec![recorded(99, &events[0])];
    let more = store
        .scaling_events(
            ScalingEventSeq::new(1),
            Some(ScalingEventSeq::new(2)),
            limit(10),
            &mut page,
        )
        .await
        .expect("events are read");
    assert!(page.is_empty());
    assert!(!more);

    let more = store
        .scaling_events(ScalingEventSeq::new(3), None, limit(10), &mut page)
        .await
        .expect("events are read");
    assert!(page.is_empty());
    assert!(!more);
}

#[tokio::test]
async fn scaling_events_same_millisecond_pages_keep_recording_order() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let first = scaling(
        5_000,
        ScalingEventKind::DrainStarted {
            worker: worker_id("worker-1"),
        },
    );
    let second = scaling(
        5_000,
        ScalingEventKind::DrainCancelled {
            worker: worker_id("worker-1"),
        },
    );
    for event in [&first, &second] {
        store
            .record_scaling_event(event)
            .await
            .expect("event is appended");
    }

    let mut newest = Vec::new();
    let more = store
        .scaling_events(ScalingEventSeq::default(), None, limit(1), &mut newest)
        .await
        .expect("events are read");
    assert_eq!(newest, vec![recorded(2, &second)]);
    assert!(more);

    let oldest_on_page = newest[0].seq;
    let mut older = Vec::new();
    let more = store
        .scaling_events(
            ScalingEventSeq::default(),
            Some(oldest_on_page),
            limit(1),
            &mut older,
        )
        .await
        .expect("events are read");
    assert_eq!(older, vec![recorded(1, &first)]);
    assert!(!more);
}

#[tokio::test]
async fn configured_delete_journal_mode_is_applied() {
    let dir = TempDir::new().expect("temp dir");
    let mut settings = config(&dir);
    settings.journal_mode = JournalMode::Delete;
    let store = open(settings);
    store
        .record_issued(&issued("01", 1_000))
        .await
        .expect("record is stored");

    let connection = raw_connection(&dir);
    let rows = text_rows(&connection, "PRAGMA journal_mode", 1);

    assert_eq!(rows, vec![vec!["delete".to_owned()]]);
}

#[tokio::test]
async fn fresh_database_uses_write_ahead_logging() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    // The database is opened lazily; the first call creates it.
    store
        .certificate(&serial("01"))
        .await
        .expect("lookup succeeds");

    let connection = raw_connection(&dir);
    let rows = text_rows(&connection, "PRAGMA journal_mode", 1);

    assert_eq!(rows, vec![vec!["wal".to_owned()]]);
}

#[tokio::test]
async fn observing_twice_keeps_first_seen_and_moves_last_seen_forward() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let who = identity("alpha", "dev@example.test");
    store
        .observe_principal(&who, ts(1_000))
        .await
        .expect("first sighting is recorded");
    store
        .observe_principal(&who, ts(2_000))
        .await
        .expect("second sighting is recorded");
    store
        .observe_principal(&who, ts(1_500))
        .await
        .expect("late sighting is recorded");

    let record = store
        .principal(&who)
        .await
        .expect("lookup succeeds")
        .expect("principal is registered");
    assert_eq!(record.identity, who);
    assert_eq!(record.first_seen, ts(1_000));
    assert_eq!(record.last_seen, ts(2_000));
    assert_eq!(record.revoked_at, None);
}

#[tokio::test]
async fn unknown_principal_has_no_record() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    let record = store
        .principal(&identity("alpha", "dev@example.test"))
        .await
        .expect("lookup succeeds");
    assert_eq!(record, None);
}

#[tokio::test]
async fn revoking_an_unknown_principal_registers_and_revokes_it() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let who = identity("alpha", "dev@example.test");

    let revoked = store
        .revoke_principal(&who, ts(1_000))
        .await
        .expect("revocation is recorded");
    assert_eq!(revoked.revoked_at, Some(ts(1_000)));

    let record = store
        .principal(&who)
        .await
        .expect("lookup succeeds")
        .expect("principal is registered");
    assert_eq!(revoked, record);
    assert_eq!(record.first_seen, ts(1_000));
    assert_eq!(record.last_seen, ts(1_000));
    assert_eq!(record.revoked_at, Some(ts(1_000)));
}

#[tokio::test]
async fn revoking_twice_returns_the_original_revocation_time() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let who = identity("alpha", "dev@example.test");
    store
        .revoke_principal(&who, ts(1_000))
        .await
        .expect("first revocation is recorded");

    let revoked = store
        .revoke_principal(&who, ts(2_000))
        .await
        .expect("second revocation is recorded");
    assert_eq!(revoked.revoked_at, Some(ts(1_000)));

    let record = store
        .principal(&who)
        .await
        .expect("lookup succeeds")
        .expect("principal is registered");
    assert_eq!(revoked, record);
    assert_eq!(record.revoked_at, Some(ts(1_000)));
}

#[tokio::test]
async fn observing_a_revoked_principal_keeps_it_revoked() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let who = identity("alpha", "dev@example.test");
    store
        .revoke_principal(&who, ts(1_000))
        .await
        .expect("revocation is recorded");

    store
        .observe_principal(&who, ts(2_000))
        .await
        .expect("sighting is recorded");

    let record = store
        .principal(&who)
        .await
        .expect("lookup succeeds")
        .expect("principal is registered");
    assert_eq!(record.last_seen, ts(2_000));
    assert_eq!(record.revoked_at, Some(ts(1_000)));
}

#[tokio::test]
async fn restoring_clears_the_revocation() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let who = identity("alpha", "dev@example.test");
    store
        .revoke_principal(&who, ts(1_000))
        .await
        .expect("revocation is recorded");

    store
        .restore_principal(&who)
        .await
        .expect("restore succeeds");

    let record = store
        .principal(&who)
        .await
        .expect("lookup succeeds")
        .expect("principal is registered");
    assert_eq!(record.revoked_at, None);
    let revoked = store.revoked_principals().await.expect("listing succeeds");
    assert!(revoked.is_empty());
}

#[tokio::test]
async fn restoring_an_unknown_principal_is_not_found() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let who = identity("alpha", "dev@example.test");

    let error = store
        .restore_principal(&who)
        .await
        .expect_err("unknown principal is rejected");
    assert!(matches!(
        &error,
        PortError::NotFound(Subject::Principal(found)) if found == &who
    ));
}

#[tokio::test]
async fn revoked_principals_are_ordered_by_time_provider_and_principal_and_exclude_restored() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let restored = identity("alpha", "restored@example.test");
    let beta = identity("beta", "dev@example.test");
    let alpha_late = identity("alpha", "zed@example.test");
    let gamma = identity("gamma", "ops@example.test");
    store
        .revoke_principal(&restored, ts(1_000))
        .await
        .expect("revocation is recorded");
    store
        .restore_principal(&restored)
        .await
        .expect("restore succeeds");
    store
        .revoke_principal(&beta, ts(2_000))
        .await
        .expect("revocation is recorded");
    store
        .revoke_principal(&alpha_late, ts(2_000))
        .await
        .expect("revocation is recorded");
    store
        .revoke_principal(&gamma, ts(3_000))
        .await
        .expect("revocation is recorded");
    store
        .observe_principal(&identity("delta", "seen@example.test"), ts(500))
        .await
        .expect("sighting is recorded");

    let revoked: Vec<String> = store
        .revoked_principals()
        .await
        .expect("listing succeeds")
        .into_iter()
        .map(|record| record.identity.to_string())
        .collect();
    assert_eq!(
        revoked,
        [
            "alpha:zed@example.test",
            "beta:dev@example.test",
            "gamma:ops@example.test",
        ]
    );
}

fn names(records: &[PrincipalRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| record.identity.to_string())
        .collect()
}

#[tokio::test]
async fn principals_are_ordered_by_provider_then_principal() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    for who in [
        identity("beta", "dev@example.test"),
        identity("alpha", "zed@example.test"),
        identity("alpha", "amy@example.test"),
    ] {
        store
            .observe_principal(&who, ts(1_000))
            .await
            .expect("sighting is recorded");
    }

    let mut page = Vec::new();
    let more = store
        .principals(None, limit(10), false, &mut page)
        .await
        .expect("listing succeeds");
    assert!(!more);
    assert_eq!(
        names(&page),
        [
            "alpha:amy@example.test",
            "alpha:zed@example.test",
            "beta:dev@example.test",
        ]
    );
}

#[tokio::test]
async fn principals_after_is_exclusive_and_crosses_a_provider_boundary() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    for who in [
        identity("alpha", "amy@example.test"),
        identity("alpha", "zed@example.test"),
        identity("beta", "dev@example.test"),
    ] {
        store
            .observe_principal(&who, ts(1_000))
            .await
            .expect("sighting is recorded");
    }

    let mut page = Vec::new();
    let more = store
        .principals(
            Some(&identity("alpha", "amy@example.test")),
            limit(10),
            false,
            &mut page,
        )
        .await
        .expect("listing succeeds");
    assert!(!more);
    assert_eq!(
        names(&page),
        ["alpha:zed@example.test", "beta:dev@example.test"]
    );

    let more = store
        .principals(
            Some(&identity("alpha", "zed@example.test")),
            limit(10),
            false,
            &mut page,
        )
        .await
        .expect("listing succeeds");
    assert!(!more);
    assert_eq!(names(&page), ["beta:dev@example.test"]);

    let more = store
        .principals(
            Some(&identity("beta", "dev@example.test")),
            limit(10),
            false,
            &mut page,
        )
        .await
        .expect("listing succeeds");
    assert!(!more);
    assert!(page.is_empty());
}

#[tokio::test]
async fn principals_only_revoked_filters_to_revoked_principals() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    store
        .observe_principal(&identity("alpha", "amy@example.test"), ts(1_000))
        .await
        .expect("sighting is recorded");
    store
        .revoke_principal(&identity("alpha", "zed@example.test"), ts(2_000))
        .await
        .expect("revocation is recorded");
    store
        .revoke_principal(&identity("beta", "dev@example.test"), ts(3_000))
        .await
        .expect("revocation is recorded");

    let mut revoked = Vec::new();
    let more = store
        .principals(None, limit(10), true, &mut revoked)
        .await
        .expect("listing succeeds");
    assert!(!more);
    assert_eq!(
        names(&revoked),
        ["alpha:zed@example.test", "beta:dev@example.test"]
    );

    let mut all = Vec::new();
    store
        .principals(None, limit(10), false, &mut all)
        .await
        .expect("listing succeeds");
    assert_eq!(
        names(&all),
        [
            "alpha:amy@example.test",
            "alpha:zed@example.test",
            "beta:dev@example.test",
        ]
    );
}

#[tokio::test]
async fn principals_more_is_false_at_the_limit_and_true_one_past_it() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    for who in [
        identity("alpha", "amy@example.test"),
        identity("alpha", "zed@example.test"),
        identity("beta", "dev@example.test"),
    ] {
        store
            .observe_principal(&who, ts(1_000))
            .await
            .expect("sighting is recorded");
    }

    let mut exact = Vec::new();
    let more = store
        .principals(None, limit(3), false, &mut exact)
        .await
        .expect("listing succeeds");
    assert_eq!(exact.len(), 3);
    assert!(!more);

    let mut one_past = Vec::new();
    let more = store
        .principals(None, limit(2), false, &mut one_past)
        .await
        .expect("listing succeeds");
    assert_eq!(
        names(&one_past),
        ["alpha:amy@example.test", "alpha:zed@example.test"]
    );
    assert!(more);

    let mut last = Vec::new();
    let more = store
        .principals(
            Some(&identity("alpha", "zed@example.test")),
            limit(2),
            false,
            &mut last,
        )
        .await
        .expect("listing succeeds");
    assert_eq!(names(&last), ["beta:dev@example.test"]);
    assert!(!more);
}

#[tokio::test]
async fn principals_overwrite_the_buffer_instead_of_appending() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    for who in [
        identity("alpha", "amy@example.test"),
        identity("alpha", "zed@example.test"),
        identity("beta", "dev@example.test"),
    ] {
        store
            .observe_principal(&who, ts(1_000))
            .await
            .expect("sighting is recorded");
    }

    let mut page = Vec::new();
    store
        .principals(None, limit(10), false, &mut page)
        .await
        .expect("listing succeeds");
    assert_eq!(page.len(), 3);

    let more = store
        .principals(None, limit(1), false, &mut page)
        .await
        .expect("listing succeeds");
    assert_eq!(names(&page), ["alpha:amy@example.test"]);
    assert!(more);

    let more = store
        .principals(
            Some(&identity("beta", "dev@example.test")),
            limit(1),
            true,
            &mut page,
        )
        .await
        .expect("listing succeeds");
    assert!(page.is_empty());
    assert!(!more);
}

#[tokio::test]
async fn restored_principal_drops_out_of_the_revoked_listing() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);
    let who = identity("alpha", "zed@example.test");
    store
        .revoke_principal(&who, ts(1_000))
        .await
        .expect("revocation is recorded");

    let mut revoked = Vec::new();
    store
        .principals(None, limit(10), true, &mut revoked)
        .await
        .expect("listing succeeds");
    assert_eq!(names(&revoked), ["alpha:zed@example.test"]);

    store
        .restore_principal(&who)
        .await
        .expect("restore succeeds");

    let mut after_restore = Vec::new();
    let more = store
        .principals(None, limit(10), true, &mut after_restore)
        .await
        .expect("listing succeeds");
    assert!(after_restore.is_empty());
    assert!(!more);

    let mut all = Vec::new();
    store
        .principals(None, limit(10), false, &mut all)
        .await
        .expect("listing succeeds");
    assert_eq!(names(&all), ["alpha:zed@example.test"]);
    assert_eq!(all[0].revoked_at, None);
}

#[tokio::test]
async fn database_with_a_newer_schema_is_refused() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store = store(&dir);
        store
            .certificate(&serial("01"))
            .await
            .expect("lookup migrates the database");
    }
    {
        let connection = raw_connection(&dir);
        connection
            .execute("PRAGMA user_version = 999", [])
            .expect("version is bumped");
    }

    let store = store(&dir);
    let error = store
        .certificate(&serial("01"))
        .await
        .expect_err("newer schema is rejected");

    assert!(matches!(error, PortError::Backend(_)));
    let mut schema_too_new = None;
    let mut cause = error.source();
    while let Some(current) = cause {
        if let Some(found) = current.downcast_ref::<SqliteError>() {
            schema_too_new = Some(found);
            break;
        }
        cause = current.source();
    }
    assert!(
        matches!(
            schema_too_new,
            Some(SqliteError::SchemaTooNew { found: 999, .. })
        ),
        "expected SchemaTooNew in the source chain, got {schema_too_new:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn statements_do_not_block_the_async_runtime() {
    let dir = TempDir::new().expect("temp dir");
    let mut settings = config(&dir);
    settings.busy_timeout = NonZeroDuration::from_millis(100);
    let store = open(settings);
    let who = identity("alpha", "dev@example.test");
    // The first read creates and migrates the database, so the lock below contends only with writes.
    store
        .certificate(&serial("01"))
        .await
        .expect("lookup creates the database");

    let holder = raw_connection(&dir);
    holder
        .execute_batch("BEGIN IMMEDIATE")
        .expect("write lock is taken");

    let write = store.observe_principal(&who, ts(1_000));
    // Polled after `write`. A driver that blocks the calling thread would be stuck inside
    // `write` waiting for this lock, so the COMMIT would never run and the write would fail
    // with SQLITE_BUSY once the short busy timeout lapses, instead of hanging the suite.
    let release = async {
        holder
            .execute_batch("COMMIT")
            .expect("write lock is released");
    };
    let (written, ()) = tokio::join!(write, release);

    written.expect("sighting is recorded once the lock is released");
    let record = store
        .principal(&who)
        .await
        .expect("lookup succeeds")
        .expect("principal is registered");
    assert_eq!(record.identity, who);
}
