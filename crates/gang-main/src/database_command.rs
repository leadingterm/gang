//! Copies durable state between configured database plugins through the repository port.

use std::num::NonZeroUsize;
use std::path::Path;
use std::process::ExitCode;

use gang_spi::{CertStatus, PluginContext, ScalingEventSeq, StateRepositoryPort};
use serde::Deserialize;

use crate::cli::DatabaseCopyArgs;
use crate::errors::{MainError, Result};
use crate::output::Output;
use crate::plugins::{Database, DatabaseConfig};
use crate::runtime;

const PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(256).expect("page size is nonzero");

/// Only the database section is needed; unrelated configuration is ignored.
#[derive(Deserialize)]
struct DatabaseFile {
    database: DatabaseConfig,
}

fn database(path: &Path) -> Result<Database> {
    let text = std::fs::read_to_string(path).map_err(|source| MainError::ReadConfig {
        path: path.to_owned(),
        source,
    })?;
    let config: DatabaseFile =
        toml::from_str(&text).map_err(|source| MainError::DatabaseConfig {
            path: path.to_owned(),
            source,
        })?;
    Ok(Database::build(
        config.database,
        &PluginContext { config_path: path },
    )?)
}

pub(crate) fn run(args: DatabaseCopyArgs, out: &Output) -> Result<ExitCode> {
    let source = database(&args.from)?;
    let target = database(&args.to)?;
    let counts =
        runtime::block_on(async move { copy_state(&source, &target, &args.to, PAGE_SIZE).await })?;
    out.data(format!("certificates: {}", counts.certificates))?;
    out.data(format!("principals: {}", counts.principals))?;
    out.data(format!("events: {}", counts.events))?;
    Ok(ExitCode::SUCCESS)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Counts {
    certificates: usize,
    principals: usize,
    events: usize,
}

/// Copies durable records; callers must keep both databases quiescent during the copy.
async fn copy_state<S: StateRepositoryPort, T: StateRepositoryPort>(
    source: &S,
    target: &T,
    target_path: &Path,
    page: NonZeroUsize,
) -> Result<Counts> {
    let mut certificates = Vec::new();
    let mut principals = Vec::new();
    let mut events = Vec::new();
    target
        .certificates(None, page, &mut certificates)
        .await
        .map_err(MainError::Database)?;
    target
        .principals(None, page, false, &mut principals)
        .await
        .map_err(MainError::Database)?;
    target
        .scaling_events(ScalingEventSeq::default(), None, page, &mut events)
        .await
        .map_err(MainError::Database)?;
    let epoch = target
        .revocation_epoch()
        .await
        .map_err(MainError::Database)?;
    if epoch.get() != 0 || !certificates.is_empty() || !principals.is_empty() || !events.is_empty()
    {
        return Err(MainError::DatabaseNotEmpty {
            path: target_path.to_owned(),
        });
    }

    let mut counts = Counts::default();
    let mut after = None;
    loop {
        let more = source
            .certificates(after.as_ref(), page, &mut certificates)
            .await
            .map_err(MainError::Database)?;
        for record in &mut certificates {
            let status = record.status;
            record.status = CertStatus::Active;
            target
                .record_issued(record)
                .await
                .map_err(MainError::Database)?;
            if let CertStatus::Revoked { at } = status {
                target
                    .revoke(&record.serial, at)
                    .await
                    .map_err(MainError::Database)?;
            }
            counts.certificates += 1;
        }
        if !more {
            break;
        }
        after = certificates.last().map(|record| record.serial.clone());
    }

    let mut after = None;
    loop {
        let more = source
            .principals(after.as_ref(), page, false, &mut principals)
            .await
            .map_err(MainError::Database)?;
        for record in &principals {
            target
                .observe_principal(&record.identity, record.first_seen)
                .await
                .map_err(MainError::Database)?;
            target
                .observe_principal(&record.identity, record.last_seen)
                .await
                .map_err(MainError::Database)?;
            if let Some(at) = record.revoked_at {
                target
                    .revoke_principal(&record.identity, at)
                    .await
                    .map_err(MainError::Database)?;
            }
            counts.principals += 1;
        }
        if !more {
            break;
        }
        after = principals.last().map(|record| record.identity.clone());
    }

    counts.events = copy_events(source, target, target_path, page, &mut events).await?;
    Ok(counts)
}

/// Copies audit events oldest first and checks the assigned sequence numbers.
async fn copy_events<S: StateRepositoryPort, T: StateRepositoryPort>(
    source: &S,
    target: &T,
    target_path: &Path,
    page: NonZeroUsize,
    events: &mut Vec<gang_spi::RecordedScalingEvent>,
) -> Result<usize> {
    // The port returns the newest records in a window. Bound each window to at most
    // `page` sequence numbers so it yields the oldest remaining records without buffering pages.
    source
        .scaling_events(ScalingEventSeq::default(), None, NonZeroUsize::MIN, events)
        .await
        .map_err(MainError::Database)?;
    let Some(last) = events.last().map(|event| event.seq) else {
        return Ok(0);
    };
    let width = u64::try_from(page.get()).unwrap_or(u64::MAX);
    let mut since = ScalingEventSeq::default();
    let mut count = 0;
    while since < last {
        let end = since.get().saturating_add(width);
        let before = if end < last.get() {
            Some(ScalingEventSeq::new(end + 1))
        } else {
            None
        };
        source
            .scaling_events(since, before, page, events)
            .await
            .map_err(MainError::Database)?;
        if events.is_empty() {
            return Err(MainError::DatabaseSequenceGap { after: since, last });
        }
        for record in events.iter() {
            let actual = target
                .record_scaling_event(&record.event)
                .await
                .map_err(MainError::Database)?;
            if actual != record.seq {
                return Err(MainError::DatabaseSequence {
                    path: target_path.to_owned(),
                    expected: record.seq,
                    actual,
                });
            }
            since = record.seq;
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use gang_clock::Timestamp;
    use gang_spi::{IssuedCertRecord, Plugin, ScalingAuditEvent, ScalingEventKind};
    use gang_sqlite::{Sqlite, SqliteConfig};
    use gang_testkit::{FakeDynamoDb, FakeRepository, PostgresServer, TestResult, shared_postgres};
    use gang_types::{CertSerial, FleetName, Identity, Principal, ProviderName, WorkerId};

    use super::*;

    fn ts(value: u64) -> Timestamp {
        Timestamp::from_unix_millis(value)
    }

    async fn seed(source: &impl StateRepositoryPort) -> TestResult {
        let alice = Identity::new(
            ProviderName::try_new("github")?,
            Principal::try_new("alice")?,
        );
        let bob = Identity::new(ProviderName::try_new("github")?, Principal::try_new("bob")?);
        source.observe_principal(&alice, ts(10)).await?;
        source.observe_principal(&alice, ts(20)).await?;
        source.revoke_principal(&alice, ts(30)).await?;
        source.observe_principal(&bob, ts(15)).await?;
        let mut serials = Vec::new();
        for serial in [1, 2, 3] {
            let record = IssuedCertRecord {
                serial: CertSerial::from_be_bytes(&[serial])?,
                identity: alice.clone(),
                issued_at: ts(5),
                expires_at: ts(100),
                status: CertStatus::Active,
            };
            source.record_issued(&record).await?;
            serials.push(record.serial);
        }
        source.revoke(&serials[1], ts(40)).await?;
        for worker in ["worker-a", "worker-b"] {
            source
                .record_scaling_event(&ScalingAuditEvent {
                    at: ts(50),
                    fleet: FleetName::try_new("fleet-a")?,
                    kind: ScalingEventKind::WorkerRegistered {
                        worker: WorkerId::try_new(worker)?,
                    },
                    cause: None,
                })
                .await?;
        }
        source
            .record_scaling_event(&ScalingAuditEvent {
                at: ts(50),
                fleet: FleetName::try_new("fleet-a")?,
                kind: ScalingEventKind::LaunchFailed {
                    from: 2,
                    to: 7,
                    reason: "quota\0café\nretry".to_owned(),
                },
                cause: None,
            })
            .await?;
        Ok(())
    }

    async fn assert_copied<S: StateRepositoryPort, T: StateRepositoryPort>(
        source: &S,
        target: &T,
    ) -> TestResult {
        let alice = Identity::new(
            ProviderName::try_new("github")?,
            Principal::try_new("alice")?,
        );
        let bob = Identity::new(ProviderName::try_new("github")?, Principal::try_new("bob")?);
        let serials = [1, 2, 3]
            .into_iter()
            .map(|serial| CertSerial::from_be_bytes(&[serial]))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for serial in &serials {
            assert_eq!(
                source.certificate(serial).await?,
                target.certificate(serial).await?
            );
        }
        for identity in [&alice, &bob] {
            assert_eq!(
                source.principal(identity).await?,
                target.principal(identity).await?
            );
        }
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        source
            .scaling_events(ScalingEventSeq::default(), None, PAGE_SIZE, &mut expected)
            .await?;
        target
            .scaling_events(ScalingEventSeq::default(), None, PAGE_SIZE, &mut actual)
            .await?;
        assert_eq!(expected, actual);
        assert_eq!(source.revocation_epoch().await?.get(), 2);
        assert_eq!(
            source.revocation_epoch().await?,
            target.revocation_epoch().await?
        );
        Ok(())
    }

    async fn round_trip<S: StateRepositoryPort, T: StateRepositoryPort>(
        source: &S,
        target: &T,
    ) -> TestResult {
        seed(source).await?;
        assert_eq!(
            copy_state(source, target, Path::new("target.toml"), NonZeroUsize::MIN).await?,
            Counts {
                certificates: 3,
                principals: 2,
                events: 3
            }
        );
        assert_copied(source, target).await
    }

    async fn postgres_config(server: &PostgresServer, path: &Path) -> TestResult {
        std::fs::write(
            path,
            format!(
                "[database.postgres]\nhost = {:?}\nport = {}\ndatabase = {:?}\nuser = {:?}\ntls = \"disable\"\n",
                server.socket_dir().to_string_lossy(),
                server.port(),
                server.create_database().await?,
                gang_testkit::SUPERUSER,
            ),
        )?;
        Ok(())
    }

    async fn run_copy(from: &Path, to: &Path) -> Result<ExitCode> {
        let args = DatabaseCopyArgs {
            from: from.to_owned(),
            to: to.to_owned(),
        };
        tokio::task::spawn_blocking(move || run(args, &Output::captured().0))
            .await
            .expect("copy task does not panic")
    }

    async fn binary_copy(from: &Path, to: &Path) -> TestResult {
        let from = from.to_owned();
        let to = to.to_owned();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(
                std::env::var_os("GANG_BIN")
                    .ok_or_else(|| std::io::Error::other("GANG_BIN must name the gang binary"))?,
            )
            .args(["database", "copy", "--from"])
            .arg(from)
            .arg("--to")
            .arg(to)
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
        })
        .await??;
        assert!(
            output.status.success(),
            "database copy failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    #[tokio::test]
    async fn copies_sqlite_to_postgres() -> TestResult {
        let server = shared_postgres()?;
        let dir = tempfile::tempdir()?;
        let source_path = dir.path().join("sqlite.toml");
        let target_path = dir.path().join("postgres.toml");
        std::fs::write(&source_path, "[database.sqlite]\npath = \"source.db\"\n")?;
        postgres_config(&server, &target_path).await?;
        let source = database(&source_path)?;
        let target = database(&target_path)?;
        seed(&source).await?;
        let Database::Sqlite(source) = source else {
            return Err("expected SQLite source".into());
        };
        source.close().await?;
        binary_copy(&source_path, &target_path).await?;
        let source = database(&source_path)?;
        assert_copied(&source, &target).await
    }

    #[tokio::test]
    async fn copies_postgres_to_dynamodb() -> TestResult {
        let server = shared_postgres()?;
        let fake = FakeDynamoDb::start().await?;
        let dir = tempfile::tempdir()?;
        let source_path = dir.path().join("source.toml");
        let dynamodb_path = dir.path().join("dynamodb.toml");
        let restored_path = dir.path().join("restored.toml");
        postgres_config(&server, &source_path).await?;
        postgres_config(&server, &restored_path).await?;
        std::fs::write(
            &dynamodb_path,
            format!(
                "[database.dynamodb]\ntable = \"gang-test\"\nregion = \"us-east-1\"\nendpoint = {:?}\n",
                fake.endpoint().as_str(),
            ),
        )?;
        let source = database(&source_path)?;
        let restored = database(&restored_path)?;
        seed(&source).await?;
        binary_copy(&source_path, &dynamodb_path).await?;
        binary_copy(&dynamodb_path, &restored_path).await?;
        assert_copied(&source, &restored).await
    }

    #[tokio::test]
    async fn refuses_nonempty_postgres_target_without_changes() -> TestResult {
        let server = shared_postgres()?;
        let dir = tempfile::tempdir()?;
        let source_path = dir.path().join("source.toml");
        let target_path = dir.path().join("target.toml");
        std::fs::write(&source_path, "[database.sqlite]\npath = \"source.db\"\n")?;
        postgres_config(&server, &target_path).await?;
        let target = database(&target_path)?;
        let identity = Identity::new(
            ProviderName::try_new("github")?,
            Principal::try_new("alice")?,
        );
        target.observe_principal(&identity, ts(1)).await?;
        let before = target.principal(&identity).await?;
        assert!(matches!(
            run_copy(&source_path, &target_path).await,
            Err(MainError::DatabaseNotEmpty { path }) if path == target_path
        ));
        assert_eq!(target.principal(&identity).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn copies_fake_repositories() -> TestResult {
        round_trip(&FakeRepository::default(), &FakeRepository::default()).await
    }

    #[tokio::test]
    async fn copies_sqlite_databases() -> TestResult {
        let dir = tempfile::tempdir()?;
        let context = PluginContext {
            config_path: dir.path(),
        };
        let source = Sqlite::build(SqliteConfig::new(dir.path().join("source.db")), &context)?;
        let target = Sqlite::build(SqliteConfig::new(dir.path().join("target.db")), &context)?;
        round_trip(&source, &target).await
    }

    #[tokio::test]
    async fn copies_events_before_reading_every_page() -> TestResult {
        let source = FakeRepository::default();
        let target = FakeRepository::default();
        let event = ScalingAuditEvent {
            at: ts(1),
            fleet: FleetName::try_new("fleet")?,
            kind: ScalingEventKind::WorkerRegistered {
                worker: WorkerId::try_new("worker")?,
            },
            cause: None,
        };
        for _ in 0..5 {
            source.record_scaling_event(&event).await?;
        }
        target.watch_first_scaling_write(&source);
        let mut events = Vec::new();
        assert_eq!(
            copy_events(
                &source,
                &target,
                Path::new("target.toml"),
                NonZeroUsize::MIN,
                &mut events
            )
            .await?,
            5
        );
        assert!(
            target
                .reads_at_first_scaling_write()
                .is_some_and(|reads| reads <= 2)
        );
        Ok(())
    }

    #[tokio::test]
    async fn reports_empty_sequence_window_as_source_gap() -> TestResult {
        let source = FakeRepository::default();
        let target = FakeRepository::default();
        let event = ScalingAuditEvent {
            at: ts(1),
            fleet: FleetName::try_new("fleet")?,
            kind: ScalingEventKind::WorkerRegistered {
                worker: WorkerId::try_new("worker")?,
            },
            cause: None,
        };
        let first = source.record_scaling_event(&event).await?;
        source.record_scaling_event(&event).await?;
        source.remove_scaling_event(first);
        let mut events = Vec::new();
        assert!(matches!(
            copy_events(
                &source,
                &target,
                Path::new("target.toml"),
                NonZeroUsize::MIN,
                &mut events
            )
            .await,
            Err(MainError::DatabaseSequenceGap { .. })
        ));
        assert!(target.scaling_events().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn refuses_target_with_nonzero_revocation_epoch() -> TestResult {
        let source = FakeRepository::default();
        let target = FakeRepository::default();
        target.set_revocation_epoch(gang_spi::RevocationEpoch::new(1));
        assert!(matches!(
            copy_state(
                &source,
                &target,
                Path::new("target.toml"),
                NonZeroUsize::MIN
            )
            .await,
            Err(MainError::DatabaseNotEmpty { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn refuses_nonempty_target_without_changes() -> TestResult {
        let source = FakeRepository::default();
        let target = FakeRepository::default();
        let identity = Identity::new(
            ProviderName::try_new("github")?,
            Principal::try_new("alice")?,
        );
        target.observe_principal(&identity, ts(1)).await?;
        let before = target.principal(&identity).await?;
        assert!(
            matches!(copy_state(&source, &target, Path::new("target.toml"), NonZeroUsize::MIN).await, Err(MainError::DatabaseNotEmpty { path }) if path == Path::new("target.toml"))
        );
        assert_eq!(target.principal(&identity).await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn refuses_certificate_only_and_event_only_targets() -> TestResult {
        let source = FakeRepository::default();
        let certificates = FakeRepository::default();
        let events = FakeRepository::default();
        let identity = Identity::new(
            ProviderName::try_new("github")?,
            Principal::try_new("alice")?,
        );
        let record = IssuedCertRecord {
            serial: CertSerial::from_be_bytes(&[1])?,
            identity,
            issued_at: ts(1),
            expires_at: ts(2),
            status: CertStatus::Active,
        };
        certificates.record_issued(&record).await?;
        let event = ScalingAuditEvent {
            at: ts(3),
            fleet: FleetName::try_new("fleet-a")?,
            kind: ScalingEventKind::WorkerRegistered {
                worker: WorkerId::try_new("worker-a")?,
            },
            cause: None,
        };
        events.record_scaling_event(&event).await?;
        for target in [&certificates, &events] {
            assert!(matches!(
                copy_state(&source, target, Path::new("target.toml"), NonZeroUsize::MIN).await,
                Err(MainError::DatabaseNotEmpty { .. })
            ));
        }
        assert_eq!(
            certificates.certificate(&record.serial).await?,
            Some(record)
        );
        assert_eq!(events.scaling_events(), vec![event]);
        Ok(())
    }
}
