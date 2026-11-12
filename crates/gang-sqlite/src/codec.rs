use std::fmt::Display;
use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PrincipalRecord, RecordedScalingEvent, ScalingAuditEvent,
    ScalingEventKind, ScalingEventSeq,
};
use gang_types::{
    CertSerial, FleetName, Identity, InstanceId, Principal, ProviderName, ScalingCause, WorkerId,
};
use rusqlite::Row;
use rusqlite::types::{FromSql, ValueRef};

use crate::errors::SqliteError;

/// Column list matching [`certificate_from_row`], as a string literal for `concat!`. The readers
/// look columns up by name, so the order is free.
macro_rules! certificate_columns {
    () => {
        "serial, provider, principal, issued_at, expires_at, revoked_at"
    };
}
pub(crate) use certificate_columns;

/// Column list matching [`scaling_from_row`], as a string literal for `concat!`. The readers look
/// columns up by name, so the order is free.
macro_rules! scaling_columns {
    () => {
        "id, at, fleet, kind, from_capacity, to_capacity, subject, cause, detail"
    };
}
pub(crate) use scaling_columns;

/// Column list matching [`principal_from_row`], as a string literal for `concat!`. The readers
/// look columns up by name, so the order is free.
macro_rules! principal_columns {
    () => {
        "provider, principal, first_seen, last_seen, revoked_at"
    };
}
pub(crate) use principal_columns;

/// Maps a row selected with [`certificate_columns!`] to a certificate record.
///
/// # Errors
///
/// Returns [`SqliteError::Corrupt`] when a stored value fails validation, or the driver's error
/// when a column cannot be read.
pub(crate) fn certificate_from_row(row: &Row<'_>) -> Result<IssuedCertRecord, SqliteError> {
    let serial =
        CertSerial::try_new(text(row, "serial")?).map_err(|error| corrupt("serial", error))?;
    let identity = identity_from_row(row)?;
    let issued_at = timestamp("issued_at", cell::<i64>(row, "issued_at")?)?;
    let expires_at = timestamp("expires_at", cell::<i64>(row, "expires_at")?)?;
    let status = match cell::<Option<i64>>(row, "revoked_at")? {
        Some(millis) => CertStatus::Revoked {
            at: timestamp("revoked_at", millis)?,
        },
        None => CertStatus::Active,
    };
    Ok(IssuedCertRecord {
        serial,
        identity,
        issued_at,
        expires_at,
        status,
    })
}

/// Maps a row selected with [`principal_columns!`] to a principal record.
///
/// # Errors
///
/// Returns [`SqliteError::Corrupt`] when a stored value fails validation, or the driver's error
/// when a column cannot be read.
pub(crate) fn principal_from_row(row: &Row<'_>) -> Result<PrincipalRecord, SqliteError> {
    let identity = identity_from_row(row)?;
    let first_seen = timestamp("first_seen", cell::<i64>(row, "first_seen")?)?;
    let last_seen = timestamp("last_seen", cell::<i64>(row, "last_seen")?)?;
    let revoked_at = cell::<Option<i64>>(row, "revoked_at")?
        .map(|millis| timestamp("revoked_at", millis))
        .transpose()?;
    Ok(PrincipalRecord {
        identity,
        first_seen,
        last_seen,
        revoked_at,
    })
}

/// Decodes the provider and principal identifying a stored identity.
fn identity_from_row(row: &Row<'_>) -> Result<Identity, SqliteError> {
    let provider = ProviderName::try_new(text(row, "provider")?)
        .map_err(|error| corrupt("provider", error))?;
    let principal =
        Principal::try_new(text(row, "principal")?).map_err(|error| corrupt("principal", error))?;
    Ok(Identity::new(provider, principal))
}
/// Converts a timestamp to the unix milliseconds stored in the database.
///
/// # Errors
///
/// Returns [`SqliteError::TimestampRange`] when the value exceeds `i64::MAX`.
pub(crate) fn millis(timestamp: Timestamp) -> Result<i64, SqliteError> {
    let millis = timestamp.as_unix_millis();
    i64::try_from(millis).map_err(|_| SqliteError::TimestampRange(millis))
}

/// Converts a sequence number to the integer stored in the `id` column.
///
/// # Errors
///
/// Returns [`SqliteError::SequenceRange`] when the value exceeds `i64::MAX`.
pub(crate) fn sequence_bound(seq: ScalingEventSeq) -> Result<i64, SqliteError> {
    let value = seq.get();
    i64::try_from(value).map_err(|_| SqliteError::SequenceRange(value))
}

/// Converts stored unix milliseconds back to a timestamp.
///
/// # Errors
///
/// Returns [`SqliteError::Corrupt`] for a negative value in `column`.
pub(crate) fn timestamp(column: &'static str, millis: i64) -> Result<Timestamp, SqliteError> {
    u64::try_from(millis)
        .map(Timestamp::from_unix_millis)
        .map_err(|_| corrupt(column, format!("negative timestamp {millis}")))
}

/// Keyset paging for a query that fetched `limit + 1` rows. Returns whether more rows exist and,
/// when they do, drops the extra last row so the page holds exactly `limit` rows.
#[must_use]
pub(crate) fn page<T>(rows: &mut Vec<T>, limit: NonZeroUsize) -> bool {
    let more = rows.len() > limit.get();
    if more {
        rows.pop();
    }
    more
}

gang_types::string_enum! {
    /// The stored name of each scaling event kind.
    /// This is the only place the names are spelled out.
    enum KindTag("scaling event kind") {
        ScaledOut = "scaled_out",
        DrainStarted = "drain_started",
        DrainCancelled = "drain_cancelled",
        Terminated = "terminated",
        ExitedEarly = "exited_early",
        WorkerRegistered = "worker_registered",
        WorkerDeregistered = "worker_deregistered",
    }
}

impl KindTag {
    fn of(kind: &ScalingEventKind) -> Self {
        match kind {
            ScalingEventKind::ScaledOut { .. } => Self::ScaledOut,
            ScalingEventKind::DrainStarted { .. } => Self::DrainStarted,
            ScalingEventKind::DrainCancelled { .. } => Self::DrainCancelled,
            ScalingEventKind::Terminated { .. } => Self::Terminated,
            ScalingEventKind::ExitedEarly { .. } => Self::ExitedEarly,
            ScalingEventKind::WorkerRegistered { .. } => Self::WorkerRegistered,
            ScalingEventKind::WorkerDeregistered { .. } => Self::WorkerDeregistered,
        }
    }
}

/// A scaling audit event flattened to the columns of the `scaling_audit` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScalingRow {
    pub(crate) at: i64,
    pub(crate) fleet: String,
    pub(crate) kind: &'static str,
    pub(crate) from_capacity: Option<i64>,
    pub(crate) to_capacity: Option<i64>,
    pub(crate) subject: Option<String>,
    pub(crate) cause: Option<&'static str>,
    /// The failure record for an early worker exit, when present.
    pub(crate) detail: Option<String>,
}

impl TryFrom<&ScalingAuditEvent> for ScalingRow {
    type Error = SqliteError;

    fn try_from(event: &ScalingAuditEvent) -> Result<Self, Self::Error> {
        let (from_capacity, to_capacity, subject, detail) = match &event.kind {
            ScalingEventKind::ScaledOut { from, to } => {
                (Some(i64::from(*from)), Some(i64::from(*to)), None, None)
            }
            ScalingEventKind::DrainStarted { worker }
            | ScalingEventKind::DrainCancelled { worker }
            | ScalingEventKind::WorkerRegistered { worker }
            | ScalingEventKind::WorkerDeregistered { worker } => {
                (None, None, Some(worker.as_str().to_owned()), None)
            }
            ScalingEventKind::Terminated { instance } => {
                (None, None, Some(instance.as_str().to_owned()), None)
            }
            ScalingEventKind::ExitedEarly { instance, failure } => (
                None,
                None,
                Some(instance.as_str().to_owned()),
                Some(failure.clone()),
            ),
        };
        Ok(Self {
            at: millis(event.at)?,
            fleet: event.fleet.as_str().to_owned(),
            kind: KindTag::of(&event.kind).as_str(),
            from_capacity,
            to_capacity,
            subject,
            cause: event.cause.map(ScalingCause::as_str),
            detail,
        })
    }
}

/// Maps a row selected with [`scaling_columns!`] to a scaling audit event.
///
/// # Errors
///
/// Returns [`SqliteError::Corrupt`] when a stored value fails validation, or the driver's error
/// when a column cannot be read.
pub(crate) fn scaling_from_row(row: &Row<'_>) -> Result<RecordedScalingEvent, SqliteError> {
    let seq = scaling_seq(row)?;
    let at = timestamp("at", cell::<i64>(row, "at")?)?;
    let fleet = FleetName::try_new(text(row, "fleet")?).map_err(|error| corrupt("fleet", error))?;
    let kind = scaling_kind(row)?;
    let cause = optional_cause(row)?;
    Ok(RecordedScalingEvent {
        seq,
        event: ScalingAuditEvent {
            at,
            fleet,
            kind,
            cause,
        },
    })
}

/// Reads the `id` column as the event's sequence number.
///
/// # Errors
///
/// Returns [`SqliteError::Corrupt`] when the value is negative.
pub(crate) fn scaling_seq(row: &Row<'_>) -> Result<ScalingEventSeq, SqliteError> {
    let value = cell::<i64>(row, "id")?;
    u64::try_from(value)
        .map(ScalingEventSeq::new)
        .map_err(|_| corrupt("id", format!("negative sequence {value}")))
}

/// Decodes the `kind` column together with the capacity or subject columns it determines.
fn scaling_kind(row: &Row<'_>) -> Result<ScalingEventKind, SqliteError> {
    let tag = text(row, "kind")?
        .parse::<KindTag>()
        .map_err(|error| corrupt("kind", error))?;
    Ok(match tag {
        KindTag::ScaledOut => ScalingEventKind::ScaledOut {
            from: capacity(row, "from_capacity")?,
            to: capacity(row, "to_capacity")?,
        },
        KindTag::DrainStarted => ScalingEventKind::DrainStarted {
            worker: worker(row)?,
        },
        KindTag::DrainCancelled => ScalingEventKind::DrainCancelled {
            worker: worker(row)?,
        },
        KindTag::Terminated => ScalingEventKind::Terminated {
            instance: instance(row)?,
        },
        KindTag::ExitedEarly => ScalingEventKind::ExitedEarly {
            instance: instance(row)?,
            failure: cell::<Option<String>>(row, "detail")?
                .ok_or_else(|| corrupt("detail", "expected text"))?,
        },
        KindTag::WorkerRegistered => ScalingEventKind::WorkerRegistered {
            worker: worker(row)?,
        },
        KindTag::WorkerDeregistered => ScalingEventKind::WorkerDeregistered {
            worker: worker(row)?,
        },
    })
}

/// Decodes the `cause` column. `NULL` covers events without an autoscaler decision and rows
/// recorded before causes existed.
fn optional_cause(row: &Row<'_>) -> Result<Option<ScalingCause>, SqliteError> {
    if matches!(row.get_ref("cause")?, ValueRef::Null) {
        return Ok(None);
    }
    text(row, "cause")?
        .parse::<ScalingCause>()
        .map(Some)
        .map_err(|error| corrupt("cause", error))
}

fn capacity(row: &Row<'_>, column: &'static str) -> Result<u32, SqliteError> {
    let value =
        cell::<Option<i64>>(row, column)?.ok_or_else(|| corrupt(column, "expected integer"))?;
    u32::try_from(value).map_err(|_| corrupt(column, format!("capacity {value} out of range")))
}

/// Reads the required subject, preserving the missing-subject error.
fn subject<'r>(row: &'r Row<'_>) -> Result<&'r str, SqliteError> {
    if matches!(row.get_ref("subject")?, ValueRef::Null) {
        return Err(corrupt("subject", "expected text"));
    }
    text(row, "subject")
}

fn worker(row: &Row<'_>) -> Result<WorkerId, SqliteError> {
    WorkerId::try_new(subject(row)?).map_err(|error| corrupt("subject", error))
}

fn instance(row: &Row<'_>) -> Result<InstanceId, SqliteError> {
    InstanceId::try_new(subject(row)?).map_err(|error| corrupt("subject", error))
}

fn corrupt(column: &'static str, detail: impl Display) -> SqliteError {
    SqliteError::Corrupt {
        column,
        detail: detail.to_string(),
    }
}

/// Reads a UTF-8 text column without allocating an intermediate string.
fn text<'r>(row: &'r Row<'_>, column: &'static str) -> Result<&'r str, SqliteError> {
    match row.get_ref(column)? {
        ValueRef::Text(bytes) => std::str::from_utf8(bytes).map_err(|error| corrupt(column, error)),
        _ => Err(corrupt(column, "unexpected storage type")),
    }
}

/// Reads `column` as `T`. A stored value of the wrong storage type is corrupt data, not a driver
/// failure.
fn cell<T: FromSql>(row: &Row<'_>, column: &'static str) -> Result<T, SqliteError> {
    row.get::<_, T>(column).map_err(|error| match error {
        rusqlite::Error::InvalidColumnType(..) => corrupt(column, "unexpected storage type"),
        other => SqliteError::Query(other),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_types::{FleetName, InstanceId, WorkerId};
    use rusqlite::{Connection, params};

    fn event(at: u64, kind: ScalingEventKind) -> ScalingAuditEvent {
        ScalingAuditEvent {
            at: Timestamp::from_unix_millis(at),
            fleet: FleetName::try_new("gang-x86").expect("fleet name is valid"),
            kind,
            cause: None,
        }
    }

    fn memory_db(schema: &str) -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory database opens");
        conn.execute_batch(schema).expect("schema is valid");
        conn
    }

    #[test]
    fn scaled_out_keeps_both_capacities_and_no_subject() {
        let row = ScalingRow::try_from(&event(
            1_700,
            ScalingEventKind::ScaledOut { from: 2, to: 5 },
        ))
        .expect("row maps");
        assert_eq!(
            row,
            ScalingRow {
                at: 1_700,
                fleet: "gang-x86".to_owned(),
                kind: "scaled_out",
                from_capacity: Some(2),
                to_capacity: Some(5),
                subject: None,
                cause: None,
                detail: None,
            }
        );
    }

    #[test]
    fn drain_started_keeps_worker_as_subject() {
        let worker = WorkerId::try_new("i-0abc123-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b")
            .expect("worker id is valid");
        let row = ScalingRow::try_from(&event(1, ScalingEventKind::DrainStarted { worker }))
            .expect("row maps");
        assert_eq!(row.kind, "drain_started");
        assert_eq!(row.from_capacity, None);
        assert_eq!(row.to_capacity, None);
        assert_eq!(
            row.subject.as_deref(),
            Some("i-0abc123-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b")
        );
    }

    #[test]
    fn drain_cancelled_keeps_worker_as_subject() {
        let worker = WorkerId::try_new("worker-1").expect("worker id is valid");
        let row = ScalingRow::try_from(&event(1, ScalingEventKind::DrainCancelled { worker }))
            .expect("row maps");
        assert_eq!(row.kind, "drain_cancelled");
        assert_eq!(row.from_capacity, None);
        assert_eq!(row.to_capacity, None);
        assert_eq!(row.subject.as_deref(), Some("worker-1"));
    }

    #[test]
    fn terminated_keeps_instance_as_subject() {
        let instance = InstanceId::try_new("i-0abc123").expect("instance id is valid");
        let row = ScalingRow::try_from(&event(1, ScalingEventKind::Terminated { instance }))
            .expect("row maps");
        assert_eq!(row.kind, "terminated");
        assert_eq!(row.from_capacity, None);
        assert_eq!(row.to_capacity, None);
        assert_eq!(row.subject.as_deref(), Some("i-0abc123"));
        assert_eq!(row.detail, None);
    }

    #[test]
    fn exited_early_keeps_instance_and_failure() {
        let instance = InstanceId::try_new("local-1").expect("instance id is valid");
        let failure = "fail  gang worker: the scratch volume is too small";
        let row = ScalingRow::try_from(&event(
            1,
            ScalingEventKind::ExitedEarly {
                instance,
                failure: failure.to_owned(),
            },
        ))
        .expect("row maps");
        assert_eq!(row.kind, "exited_early");
        assert_eq!(row.from_capacity, None);
        assert_eq!(row.to_capacity, None);
        assert_eq!(row.subject.as_deref(), Some("local-1"));
        assert_eq!(row.detail.as_deref(), Some(failure));
    }

    #[test]
    fn worker_registration_keeps_worker_as_subject() {
        let worker = WorkerId::try_new("worker-2").expect("worker id is valid");
        let row = ScalingRow::try_from(&event(1, ScalingEventKind::WorkerRegistered { worker }))
            .expect("row maps");
        assert_eq!(row.kind, "worker_registered");
        assert_eq!(row.from_capacity, None);
        assert_eq!(row.to_capacity, None);
        assert_eq!(row.subject.as_deref(), Some("worker-2"));
    }

    #[test]
    fn worker_deregistration_keeps_worker_as_subject() {
        let worker = WorkerId::try_new("worker-2").expect("worker id is valid");
        let row = ScalingRow::try_from(&event(1, ScalingEventKind::WorkerDeregistered { worker }))
            .expect("row maps");
        assert_eq!(row.kind, "worker_deregistered");
        assert_eq!(row.from_capacity, None);
        assert_eq!(row.to_capacity, None);
        assert_eq!(row.subject.as_deref(), Some("worker-2"));
    }

    #[test]
    fn timestamp_round_trips_through_millis() {
        let original = Timestamp::from_unix_millis(1_786_000_000_123);
        let stored = millis(original).expect("fits in i64");
        assert_eq!(
            timestamp("issued_at", stored).expect("non-negative"),
            original
        );
    }

    #[test]
    fn millis_rejects_values_above_i64_max() {
        let too_large = Timestamp::from_unix_millis(u64::MAX);
        assert!(matches!(
            millis(too_large),
            Err(SqliteError::TimestampRange(u64::MAX))
        ));
    }

    #[test]
    fn timestamp_rejects_negative_millis() {
        assert!(matches!(
            timestamp("expires_at", -1),
            Err(SqliteError::Corrupt {
                column: "expires_at",
                ..
            })
        ));
    }

    #[test]
    fn cause_is_stored_as_its_canonical_name() -> Result<(), SqliteError> {
        let mut audit = event(1, ScalingEventKind::ScaledOut { from: 1, to: 2 });
        audit.cause = Some(ScalingCause::IdleTimeout);
        let row = ScalingRow::try_from(&audit)?;
        assert_eq!(row.cause, Some("idle_timeout"));
        Ok(())
    }

    #[test]
    fn scaling_row_reads_back_through_its_columns() {
        let conn = memory_db(
            "CREATE TABLE scaling_audit (
                id INTEGER PRIMARY KEY,
                at INTEGER NOT NULL,
                fleet TEXT NOT NULL,
                kind TEXT NOT NULL,
                from_capacity INTEGER,
                to_capacity INTEGER,
                subject TEXT,
                cause TEXT,
                detail TEXT
            ) STRICT;",
        );
        let mut audit = event(1_700, ScalingEventKind::ScaledOut { from: 1, to: 2 });
        audit.cause = Some(ScalingCause::IdleTimeout);
        let row = ScalingRow::try_from(&audit).expect("row maps");
        conn.execute(
            "INSERT INTO scaling_audit (id, at, fleet, kind, from_capacity, to_capacity, subject, cause, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                7_i64,
                row.at,
                row.fleet,
                row.kind,
                row.from_capacity,
                row.to_capacity,
                row.subject,
                row.cause,
                row.detail
            ],
        )
        .expect("row inserts");
        let mut stmt = conn
            .prepare(concat!(
                "SELECT ",
                scaling_columns!(),
                " FROM scaling_audit"
            ))
            .expect("query prepares");
        let mut rows = stmt.query([]).expect("query runs");
        let stored = rows.next().expect("row reads").expect("one row");
        assert_eq!(
            scaling_from_row(stored).expect("row maps back"),
            RecordedScalingEvent {
                seq: ScalingEventSeq::new(7),
                event: audit,
            }
        );
    }

    #[test]
    fn readers_find_columns_by_name_whatever_their_order() {
        let conn = memory_db(
            "CREATE TABLE issued_certificates (
                revoked_at INTEGER,
                expires_at INTEGER NOT NULL,
                issued_at INTEGER NOT NULL,
                principal TEXT NOT NULL,
                provider TEXT NOT NULL,
                serial TEXT NOT NULL
            ) STRICT;
            CREATE TABLE principals (
                revoked_at INTEGER,
                last_seen INTEGER NOT NULL,
                first_seen INTEGER NOT NULL,
                principal TEXT NOT NULL,
                provider TEXT NOT NULL
            ) STRICT;",
        );
        conn.execute(
            "INSERT INTO issued_certificates (serial, provider, principal, issued_at, expires_at, revoked_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params!["0c0c", "google", "worker-a", 1_000_i64, 2_000_i64, None::<i64>],
        )
        .expect("certificate inserts");
        conn.execute(
            "INSERT INTO principals (provider, principal, first_seen, last_seen, revoked_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["google", "worker-a", 1_000_i64, 3_000_i64, 4_000_i64],
        )
        .expect("principal inserts");

        let mut stmt = conn
            .prepare(concat!(
                "SELECT ",
                certificate_columns!(),
                " FROM issued_certificates"
            ))
            .expect("query prepares");
        let mut rows = stmt.query([]).expect("query runs");
        let stored = rows.next().expect("row reads").expect("one row");
        assert_eq!(
            certificate_from_row(stored).expect("row maps"),
            IssuedCertRecord {
                serial: CertSerial::try_new("0c0c").expect("serial is valid"),
                identity: Identity::new(
                    ProviderName::try_new("google").expect("provider is valid"),
                    Principal::try_new("worker-a").expect("principal is valid"),
                ),
                issued_at: Timestamp::from_unix_millis(1_000),
                expires_at: Timestamp::from_unix_millis(2_000),
                status: CertStatus::Active,
            }
        );

        let mut stmt = conn
            .prepare(concat!("SELECT ", principal_columns!(), " FROM principals"))
            .expect("query prepares");
        let mut rows = stmt.query([]).expect("query runs");
        let stored = rows.next().expect("row reads").expect("one row");
        assert_eq!(
            principal_from_row(stored).expect("row maps"),
            PrincipalRecord {
                identity: Identity::new(
                    ProviderName::try_new("google").expect("provider is valid"),
                    Principal::try_new("worker-a").expect("principal is valid"),
                ),
                first_seen: Timestamp::from_unix_millis(1_000),
                last_seen: Timestamp::from_unix_millis(3_000),
                revoked_at: Some(Timestamp::from_unix_millis(4_000)),
            }
        );
    }

    #[test]
    fn page_keeps_an_exact_page_untouched() {
        let limit = NonZeroUsize::new(2).expect("limit is nonzero");
        let mut rows = vec![1, 2];
        assert!(!page(&mut rows, limit));
        assert_eq!(rows, vec![1, 2]);
    }

    #[test]
    fn page_drops_the_extra_last_row_when_more_exist() {
        let limit = NonZeroUsize::new(2).expect("limit is nonzero");
        let mut rows = vec![1, 2, 3];
        assert!(page(&mut rows, limit));
        assert_eq!(rows, vec![1, 2]);
    }

    /// Builds `SELECT <literal> AS <column>, ...` from `base`, with the literal of each column
    /// named in `overrides` replaced.
    fn select_row(base: &[(&str, &str)], overrides: &[(&str, &str)]) -> String {
        let cells: Vec<String> = base
            .iter()
            .map(|(column, literal)| {
                let value = overrides
                    .iter()
                    .find(|(name, _)| name == column)
                    .map_or(*literal, |(_, value)| *value);
                format!("{value} AS {column}")
            })
            .collect();
        format!("SELECT {}", cells.join(", "))
    }

    /// Maps the single row of `select` with `read` against a fresh in-memory database.
    fn read_row<T>(
        select: &str,
        read: impl FnOnce(&Row<'_>) -> Result<T, SqliteError>,
    ) -> Result<T, SqliteError> {
        let conn = Connection::open_in_memory().expect("in-memory database opens");
        let mut stmt = conn.prepare(select).expect("query prepares");
        let mut rows = stmt.query([]).expect("query runs");
        let row = rows.next().expect("row reads").expect("one row");
        read(row)
    }

    /// One corrupt row: the columns to overwrite and the column the mapper must report.
    struct Case {
        name: &'static str,
        overrides: &'static [(&'static str, &'static str)],
        column: &'static str,
    }

    fn assert_corrupt_column<T>(case: &str, result: Result<T, SqliteError>, column: &str) {
        match result {
            Err(SqliteError::Corrupt { column: found, .. }) => assert_eq!(found, column, "{case}"),
            Err(error) => panic!("{case}: expected corrupt {column}, got: {error}"),
            Ok(_) => panic!("{case}: row mapped despite corrupt {column}"),
        }
    }

    #[test]
    fn text_columns_reject_wrong_storage_types_and_invalid_utf8() {
        for literal in ["NULL", "42", "x'ff'", "CAST(x'ff' AS TEXT)"] {
            let select = format!("SELECT {literal} AS value");
            assert_corrupt_column(
                literal,
                read_row(&select, |row| text(row, "value").map(str::to_owned)),
                "value",
            );
        }
    }

    #[test]
    fn corrupt_certificate_rows_name_the_bad_column() {
        const VALID: &[(&str, &str)] = &[
            ("serial", "'0c0c'"),
            ("provider", "'google'"),
            ("principal", "'worker-a'"),
            ("issued_at", "1000"),
            ("expires_at", "2000"),
            ("revoked_at", "NULL"),
        ];
        let cases = [
            Case {
                name: "empty serial",
                overrides: &[("serial", "''")],
                column: "serial",
            },
            Case {
                name: "negative issued_at",
                overrides: &[("issued_at", "-1")],
                column: "issued_at",
            },
            Case {
                name: "negative expires_at",
                overrides: &[("expires_at", "-1")],
                column: "expires_at",
            },
            Case {
                name: "negative revoked_at",
                overrides: &[("revoked_at", "-1")],
                column: "revoked_at",
            },
            Case {
                name: "empty provider",
                overrides: &[("provider", "''")],
                column: "provider",
            },
            Case {
                name: "empty principal",
                overrides: &[("principal", "''")],
                column: "principal",
            },
        ];
        for case in &cases {
            let select = select_row(VALID, case.overrides);
            assert_corrupt_column(
                case.name,
                read_row(&select, certificate_from_row),
                case.column,
            );
        }

        let select = select_row(VALID, &[]);
        assert_eq!(
            read_row(&select, certificate_from_row).expect("valid row maps"),
            IssuedCertRecord {
                serial: CertSerial::try_new("0c0c").expect("serial is valid"),
                identity: Identity::new(
                    ProviderName::try_new("google").expect("provider is valid"),
                    Principal::try_new("worker-a").expect("principal is valid"),
                ),
                issued_at: Timestamp::from_unix_millis(1_000),
                expires_at: Timestamp::from_unix_millis(2_000),
                status: CertStatus::Active,
            }
        );
    }

    #[test]
    fn corrupt_principal_rows_name_the_bad_column() {
        const VALID: &[(&str, &str)] = &[
            ("provider", "'google'"),
            ("principal", "'worker-a'"),
            ("first_seen", "1000"),
            ("last_seen", "3000"),
            ("revoked_at", "4000"),
        ];
        let cases = [
            Case {
                name: "empty provider",
                overrides: &[("provider", "''")],
                column: "provider",
            },
            Case {
                name: "empty principal",
                overrides: &[("principal", "''")],
                column: "principal",
            },
            Case {
                name: "negative first_seen",
                overrides: &[("first_seen", "-1")],
                column: "first_seen",
            },
            Case {
                name: "negative last_seen",
                overrides: &[("last_seen", "-1")],
                column: "last_seen",
            },
            Case {
                name: "negative revoked_at",
                overrides: &[("revoked_at", "-1")],
                column: "revoked_at",
            },
        ];
        for case in &cases {
            let select = select_row(VALID, case.overrides);
            assert_corrupt_column(
                case.name,
                read_row(&select, principal_from_row),
                case.column,
            );
        }

        let select = select_row(VALID, &[]);
        assert_eq!(
            read_row(&select, principal_from_row).expect("valid row maps"),
            PrincipalRecord {
                identity: Identity::new(
                    ProviderName::try_new("google").expect("provider is valid"),
                    Principal::try_new("worker-a").expect("principal is valid"),
                ),
                first_seen: Timestamp::from_unix_millis(1_000),
                last_seen: Timestamp::from_unix_millis(3_000),
                revoked_at: Some(Timestamp::from_unix_millis(4_000)),
            }
        );
    }

    #[test]
    fn corrupt_scaling_rows_name_the_bad_column() {
        const SCALED_OUT: &[(&str, &str)] = &[
            ("id", "7"),
            ("at", "1700"),
            ("fleet", "'gang-x86'"),
            ("kind", "'scaled_out'"),
            ("from_capacity", "2"),
            ("to_capacity", "5"),
            ("subject", "NULL"),
            ("cause", "NULL"),
            ("detail", "NULL"),
        ];
        let cases = [
            Case {
                name: "unknown kind",
                overrides: &[("kind", "'scaled_in'")],
                column: "kind",
            },
            Case {
                name: "negative at",
                overrides: &[("at", "-1")],
                column: "at",
            },
            Case {
                name: "text where integer expected",
                overrides: &[("at", "'soon'")],
                column: "at",
            },
            Case {
                name: "text capacity",
                overrides: &[("from_capacity", "'two'")],
                column: "from_capacity",
            },
            Case {
                name: "negative sequence",
                overrides: &[("id", "-1")],
                column: "id",
            },
            Case {
                name: "empty fleet",
                overrides: &[("fleet", "''")],
                column: "fleet",
            },
            Case {
                name: "negative capacity",
                overrides: &[("from_capacity", "-1")],
                column: "from_capacity",
            },
            Case {
                name: "missing capacity",
                overrides: &[("from_capacity", "NULL")],
                column: "from_capacity",
            },
            Case {
                name: "unknown cause",
                overrides: &[("cause", "'bored'")],
                column: "cause",
            },
            Case {
                name: "drain without worker",
                overrides: &[
                    ("kind", "'drain_started'"),
                    ("from_capacity", "NULL"),
                    ("to_capacity", "NULL"),
                ],
                column: "subject",
            },
            Case {
                name: "empty worker",
                overrides: &[
                    ("kind", "'worker_registered'"),
                    ("from_capacity", "NULL"),
                    ("to_capacity", "NULL"),
                    ("subject", "''"),
                ],
                column: "subject",
            },
            Case {
                name: "empty instance",
                overrides: &[
                    ("kind", "'terminated'"),
                    ("from_capacity", "NULL"),
                    ("to_capacity", "NULL"),
                    ("subject", "''"),
                ],
                column: "subject",
            },
            Case {
                name: "early exit without detail",
                overrides: &[("kind", "'exited_early'"), ("subject", "'local-1'")],
                column: "detail",
            },
        ];
        for case in &cases {
            let select = select_row(SCALED_OUT, case.overrides);
            assert_corrupt_column(case.name, read_row(&select, scaling_from_row), case.column);
        }

        let select = select_row(SCALED_OUT, &[]);
        assert_eq!(
            read_row(&select, scaling_from_row).expect("valid row maps"),
            RecordedScalingEvent {
                seq: ScalingEventSeq::new(7),
                event: event(1_700, ScalingEventKind::ScaledOut { from: 2, to: 5 }),
            }
        );
    }
}
