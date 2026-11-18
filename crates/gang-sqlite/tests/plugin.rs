//! Tests for the `SQLite` plugin build and open path: config validation, lazy database creation,
//! and concurrent use of one store.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use gang_spi::{PortError, StateRepositoryPort};
use gang_sqlite::SqliteConfig;
use gang_testkit::park_then_signal;
use gang_types::NonZeroDuration;
use tempfile::TempDir;
use tokio::sync::oneshot;

#[cfg(test)]
mod support;

use support::{issued, open, try_open};

#[test]
fn empty_path_is_a_config_error() {
    let error = try_open(SqliteConfig::new(PathBuf::new()))
        .err()
        .expect("empty path is rejected");

    assert!(
        matches!(error, PortError::Config(_)),
        "expected Config, got {error:?}"
    );
}

#[test]
fn building_does_not_create_the_database_file() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("gang.db");

    let built = try_open(SqliteConfig::new(path.clone()));

    assert!(built.is_ok(), "valid configuration builds");
    assert!(!path.exists(), "build does not create the database");
}

#[tokio::test]
async fn valid_configuration_builds_a_working_store() {
    let dir = TempDir::new().expect("temp dir");
    let store = open(SqliteConfig::new(dir.path().join("gang.db")));
    let record = issued("01", 1_000);

    store
        .record_issued(&record)
        .await
        .expect("record is stored");

    assert_eq!(
        store
            .certificate(&record.serial)
            .await
            .expect("lookup succeeds"),
        Some(record)
    );
}

#[tokio::test]
async fn concurrent_writes_share_one_instance() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("gang.db");
    // Holding the write lock makes both writers wait for the COMMIT below, so the overlap does
    // not depend on how quickly the store opens the file.
    let holder = rusqlite::Connection::open(&path).expect("raw connection creates the file");
    holder
        .execute_batch("BEGIN IMMEDIATE")
        .expect("raw connection takes the write lock");

    // The busy timeout is generous: it only runs out if the writers are deadlocked.
    let first = Arc::new(open(SqliteConfig {
        busy_timeout: NonZeroDuration::from_mins(5),
        ..SqliteConfig::new(path.clone())
    }));

    let (armed_one, parked_one) = oneshot::channel();
    let (armed_two, parked_two) = oneshot::channel();
    let writer_one = tokio::spawn({
        let first = Arc::clone(&first);
        async move {
            let one = issued("01", 1_000);
            park_then_signal(first.record_issued(&one), armed_one).await
        }
    });
    let writer_two = tokio::spawn({
        let second = Arc::clone(&first);
        async move {
            let two = issued("02", 1_000);
            park_then_signal(second.record_issued(&two), armed_two).await
        }
    });
    parked_one.await.expect("first writer is in flight");
    parked_two.await.expect("second writer is in flight");

    holder
        .execute_batch("COMMIT")
        .expect("raw connection releases the write lock");
    drop(holder);

    let written_one = writer_one.await.expect("first writer task completes");
    let written_two = writer_two.await.expect("second writer task completes");
    written_one.expect("first write succeeds");
    written_two.expect("second write succeeds");

    let one = issued("01", 1_000);
    let two = issued("02", 1_000);
    assert_eq!(
        first
            .certificate(&one.serial)
            .await
            .expect("lookup succeeds"),
        Some(one)
    );
    assert_eq!(
        first
            .certificate(&two.serial)
            .await
            .expect("lookup succeeds"),
        Some(two)
    );
}

#[tokio::test]
async fn a_write_outlasting_the_busy_timeout_fails_as_busy() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("gang.db");
    // Creating the file and holding its write lock keeps the lock held for the whole call, so the
    // verdict does not depend on scheduling.
    let holder = rusqlite::Connection::open(&path).expect("raw connection creates the file");
    holder
        .execute_batch("BEGIN IMMEDIATE")
        .expect("raw connection takes the write lock");

    let store = open(SqliteConfig {
        busy_timeout: NonZeroDuration::from_millis(100),
        ..SqliteConfig::new(path.clone())
    });

    let error = store
        .record_issued(&issued("01", 1_000))
        .await
        .expect_err("write fails while the lock outlasts the busy timeout");
    holder
        .execute_batch("COMMIT")
        .expect("raw connection releases the write lock");

    assert!(
        matches!(error, PortError::Unavailable(_)),
        "expected Unavailable, got {error:?}"
    );
    let mut locked = false;
    let mut cause = error.source();
    while let Some(current) = cause {
        locked |= current.to_string().contains("database is locked");
        cause = current.source();
    }
    assert!(
        locked,
        "expected a 'database is locked' cause, got {error:?}"
    );
}
