//! Schema compatibility cases shared by persistent database backends.
//!
//! Each fixture is fully migrated before its stored schema version is stamped.

use std::{error::Error, future::Future, num::NonZeroUsize};

use gang_clock::Timestamp;
use gang_spi::{
    ExpiringRecordPort, PortError, RecordedScalingEvent, ScalingAuditEvent, ScalingEventKind,
    ScalingEventSeq, SchemaTooNew, SchemaVersion, StateRepositoryPort,
};
use gang_types::{FleetName, ScalingCause};

use crate::{Fixture, TestResult};

/// A newer additive schema remains usable for both reads and writes.
///
/// # Errors
///
/// Returns an error if the fixture cannot be created or a database operation fails.
///
/// # Panics
///
/// Panics if the recorded event is not returned intact.
pub async fn a_newer_compatible_schema_is_used<D, F, Fut, E>(
    supported: SchemaVersion,
    stamped: F,
) -> TestResult
where
    D: StateRepositoryPort + ExpiringRecordPort,
    F: FnOnce(SchemaVersion) -> Fut,
    Fut: Future<Output = Result<Fixture<D>, E>>,
    E: Into<Box<dyn Error>>,
{
    let current = supported
        .current
        .checked_add(1)
        .ok_or("schema version overflow")?;
    let fixture = stamped(SchemaVersion {
        current,
        oldest_compatible: supported.current,
    })
    .await
    .map_err(Into::<Box<dyn Error>>::into)?;
    let database = fixture.database();
    database.revocation_epoch().await?;
    let event = ScalingAuditEvent {
        at: Timestamp::from_unix_millis(1),
        fleet: FleetName::try_new("fleet-a")?,
        kind: ScalingEventKind::DrainStarted {
            worker: gang_types::WorkerId::try_new("worker")?,
            detail: Some("waterline held; cordon one instance".into()),
        },
        cause: Some(ScalingCause::Utilization),
    };
    let seq = database.record_scaling_event(&event).await?;
    let mut events = Vec::new();
    let limit = NonZeroUsize::new(1).ok_or("limit must not be zero")?;
    let more = database
        .scaling_events(ScalingEventSeq::default(), None, limit, &mut events)
        .await?;
    assert!(!more);
    assert_eq!(events, vec![RecordedScalingEvent { seq, event }]);
    Ok(())
}

/// A newer incompatible schema is rejected with its version in the error chain.
///
/// # Errors
///
/// Returns an error if the fixture cannot be created or the schema version overflows.
///
/// # Panics
///
/// Panics if the database accepts the schema or returns the wrong error.
pub async fn a_newer_incompatible_schema_is_refused<D, F, Fut, E>(
    supported: SchemaVersion,
    stamped: F,
) -> TestResult
where
    D: StateRepositoryPort + ExpiringRecordPort,
    F: FnOnce(SchemaVersion) -> Fut,
    Fut: Future<Output = Result<Fixture<D>, E>>,
    E: Into<Box<dyn Error>>,
{
    let current = supported
        .current
        .checked_add(1)
        .ok_or("schema version overflow")?;
    let stored = SchemaVersion {
        current,
        oldest_compatible: current,
    };
    let fixture = stamped(stored)
        .await
        .map_err(Into::<Box<dyn Error>>::into)?;
    let error = match fixture.database().revocation_epoch().await {
        Err(error @ PortError::Backend(_)) => error,
        other => {
            return Err(format!(
                "expected a backend error for an incompatible schema, got {other:?}"
            )
            .into());
        }
    };
    assert_eq!(
        schema_too_new(&error),
        Some(&SchemaTooNew {
            stored,
            supported: supported.current,
        })
    );
    Ok(())
}

fn schema_too_new<'a>(mut error: &'a (dyn Error + 'static)) -> Option<&'a SchemaTooNew> {
    loop {
        if let Some(schema) = error.downcast_ref::<SchemaTooNew>() {
            return Some(schema);
        }
        error = error.source()?;
    }
}
