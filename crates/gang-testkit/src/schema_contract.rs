//! Exact schema version cases shared by persistent database backends.

use std::{error::Error, future::Future};

use gang_spi::{ExpiringRecordPort, PortError, SchemaMismatch, StateRepositoryPort};

use crate::{Fixture, TestResult};

/// Rejects a newer stored schema with its version in the error chain.
///
/// # Errors
/// Returns an error if the fixture cannot be created or the version overflows.
///
/// # Panics
/// Panics if the mismatch error is incorrect.
pub async fn a_newer_schema_is_refused<D, F, Fut, E>(supported: u32, stamped: F) -> TestResult
where
    D: StateRepositoryPort + ExpiringRecordPort,
    F: FnOnce(u32) -> Fut,
    Fut: Future<Output = Result<Fixture<D>, E>>,
    E: Into<Box<dyn Error>>,
{
    let stored = supported.checked_add(1).ok_or("schema version overflow")?;
    assert_mismatch(supported, stored, stamped).await
}

/// Rejects an older nonzero schema with its version in the error chain.
///
/// # Errors
/// Returns an error if the fixture cannot be created.
///
/// # Panics
/// Panics if the mismatch error is incorrect.
pub async fn an_older_schema_is_refused<D, F, Fut, E>(supported: u32, stamped: F) -> TestResult
where
    D: StateRepositoryPort + ExpiringRecordPort,
    F: FnOnce(u32) -> Fut,
    Fut: Future<Output = Result<Fixture<D>, E>>,
    E: Into<Box<dyn Error>>,
{
    if supported <= 1 {
        return Ok(());
    }
    assert_mismatch(supported, supported - 1, stamped).await
}

async fn assert_mismatch<D, F, Fut, E>(supported: u32, stored: u32, stamped: F) -> TestResult
where
    D: StateRepositoryPort + ExpiringRecordPort,
    F: FnOnce(u32) -> Fut,
    Fut: Future<Output = Result<Fixture<D>, E>>,
    E: Into<Box<dyn Error>>,
{
    let fixture = stamped(stored)
        .await
        .map_err(Into::<Box<dyn Error>>::into)?;
    let error = match fixture.database().revocation_epoch().await {
        Err(error @ PortError::Backend(_)) => error,
        other => return Err(format!("expected a backend schema error, got {other:?}").into()),
    };
    let mut cause: &(dyn Error + 'static) = &error;
    loop {
        if let Some(mismatch) = cause.downcast_ref::<SchemaMismatch>() {
            assert_eq!(*mismatch, SchemaMismatch { stored, supported });
            return Ok(());
        }
        cause = cause
            .source()
            .ok_or("missing schema mismatch in error chain")?;
    }
}
