use std::fmt::Display;
use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    CertStatus, IssuedCertRecord, PrincipalRecord, RecordedScalingEvent, ScalingAuditEvent,
    ScalingEventKind, ScalingEventSeq, StoredScalingKind,
};
use gang_types::{CertSerial, FleetName, Identity, Principal, ProviderName, ScalingCause};
use time::OffsetDateTime;
use tokio_postgres::Row;
use tokio_postgres::types::FromSql;

use crate::errors::PostgresError;

pub(crate) fn at(value: Timestamp) -> Result<OffsetDateTime, PostgresError> {
    let millis = value.as_unix_millis();
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000).map_err(|_| {
        PostgresError::Range {
            column: "timestamp",
            value: millis,
        }
    })
}

pub(crate) fn timestamp(
    column: &'static str,
    value: OffsetDateTime,
) -> Result<Timestamp, PostgresError> {
    let nanos = value.unix_timestamp_nanos();
    if nanos < 0 {
        return Err(corrupt(column, format!("negative timestamp {nanos}ns")));
    }
    u64::try_from(nanos / 1_000_000)
        .map(Timestamp::from_unix_millis)
        .map_err(|error| corrupt(column, error))
}

pub(crate) fn signed(column: &'static str, value: u64) -> Result<i64, PostgresError> {
    i64::try_from(value).map_err(|_| PostgresError::Range { column, value })
}

pub(crate) fn unsigned(column: &'static str, value: i64) -> Result<u64, PostgresError> {
    u64::try_from(value).map_err(|error| corrupt(column, error))
}

pub(crate) fn corrupt(column: &'static str, detail: impl Display) -> PostgresError {
    PostgresError::Corrupt {
        column,
        detail: detail.to_string(),
    }
}

/// Decode stored values as corruption, rather than as a transport failure.
pub(crate) fn get<'a, T: FromSql<'a>>(
    row: &'a Row,
    column: &'static str,
) -> Result<T, PostgresError> {
    row.try_get(column).map_err(|error| corrupt(column, error))
}

#[must_use]
pub(crate) fn page<T>(rows: &mut Vec<T>, limit: NonZeroUsize) -> bool {
    let more = rows.len() > limit.get();
    if more {
        rows.pop();
    }
    more
}

fn identity_from_row(row: &Row) -> Result<Identity, PostgresError> {
    let provider = ProviderName::try_new(get::<&str>(row, "provider")?)
        .map_err(|error| corrupt("provider", error))?;
    let principal = Principal::try_new(get::<&str>(row, "principal")?)
        .map_err(|error| corrupt("principal", error))?;
    Ok(Identity::new(provider, principal))
}

pub(crate) fn certificate_from_row(row: &Row) -> Result<IssuedCertRecord, PostgresError> {
    let serial = CertSerial::try_new(get::<&str>(row, "serial")?)
        .map_err(|error| corrupt("serial", error))?;
    let status = match get::<Option<OffsetDateTime>>(row, "revoked_at")? {
        Some(value) => CertStatus::Revoked {
            at: timestamp("revoked_at", value)?,
        },
        None => CertStatus::Active,
    };
    Ok(IssuedCertRecord {
        serial,
        identity: identity_from_row(row)?,
        issued_at: timestamp("issued_at", get(row, "issued_at")?)?,
        expires_at: timestamp("expires_at", get(row, "expires_at")?)?,
        status,
    })
}

pub(crate) fn principal_from_row(row: &Row) -> Result<PrincipalRecord, PostgresError> {
    Ok(PrincipalRecord {
        identity: identity_from_row(row)?,
        first_seen: timestamp("first_seen", get(row, "first_seen")?)?,
        last_seen: timestamp("last_seen", get(row, "last_seen")?)?,
        revoked_at: get::<Option<OffsetDateTime>>(row, "revoked_at")?
            .map(|value| timestamp("revoked_at", value))
            .transpose()?,
    })
}

fn capacity(row: &Row, column: &'static str) -> Result<Option<u32>, PostgresError> {
    get::<Option<i64>>(row, column)?
        .map(|value| {
            u32::try_from(value)
                .map_err(|_| corrupt(column, format!("capacity {value} out of range")))
        })
        .transpose()
}

pub(crate) fn scaling_from_row(row: &Row) -> Result<RecordedScalingEvent, PostgresError> {
    let name: &str = get(row, "kind")?;
    let subject: Option<&str> = get(row, "subject")?;
    let detail: Option<&[u8]> = get(row, "detail")?;
    let detail = detail
        .map(std::str::from_utf8)
        .transpose()
        .map_err(|error| corrupt("detail", error))?;
    let from_capacity = capacity(row, "from_capacity")?;
    let to_capacity = capacity(row, "to_capacity")?;
    let kind = ScalingEventKind::try_from(StoredScalingKind {
        name,
        from_capacity,
        to_capacity,
        subject,
        detail,
    })
    .map_err(|error| corrupt(error.field(), &error))?;
    let cause = get::<Option<&str>>(row, "cause")?
        .map(|value| {
            value
                .parse::<ScalingCause>()
                .map_err(|error| corrupt("cause", error))
        })
        .transpose()?;
    Ok(RecordedScalingEvent {
        seq: ScalingEventSeq::new(unsigned("sequence", get(row, "sequence")?)?),
        event: ScalingAuditEvent {
            at: timestamp("at", get(row, "at")?)?,
            fleet: FleetName::try_new(get::<&str>(row, "fleet")?)
                .map_err(|error| corrupt("fleet", error))?,
            kind,
            cause,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_round_trip() {
        for millis in [0, 1, 1_786_000_000_123] {
            let original = Timestamp::from_unix_millis(millis);
            assert_eq!(
                timestamp("at", at(original).expect("timestamp fits")).expect("nonnegative"),
                original
            );
        }
        assert!(matches!(
            at(Timestamp::from_unix_millis(u64::MAX)),
            Err(PostgresError::Range {
                column: "timestamp",
                ..
            })
        ));
    }

    #[test]
    fn negative_timestamp_is_corrupt() {
        let value = OffsetDateTime::from_unix_timestamp_nanos(-1).expect("valid time");
        assert!(matches!(
            timestamp("at", value),
            Err(PostgresError::Corrupt { column: "at", .. })
        ));
    }

    #[test]
    fn integer_bounds() {
        assert_eq!(
            signed("value", u64::try_from(i64::MAX).expect("positive")).expect("fits"),
            i64::MAX
        );
        assert!(matches!(
            signed("value", u64::MAX),
            Err(PostgresError::Range {
                column: "value",
                ..
            })
        ));
        assert_eq!(
            unsigned("value", i64::MAX).expect("nonnegative"),
            u64::try_from(i64::MAX).expect("positive")
        );
        assert!(matches!(
            unsigned("value", -1),
            Err(PostgresError::Corrupt {
                column: "value",
                ..
            })
        ));
    }
}
