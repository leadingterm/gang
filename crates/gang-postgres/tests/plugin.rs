//! Integration tests for `PostgreSQL` plugin lifecycle and failures.

mod support;

use std::error::Error;
use std::net::TcpListener;
use std::num::{NonZeroU16, NonZeroUsize};
use std::path::Path;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_postgres::{Postgres, PostgresConfig, PostgresError, Tls};
use gang_spi::{
    Acquisition, CertStatus, ControllerId, ExpiringRecordPort, IssuedCertRecord, LeaderLeasePort,
    PortError, RecordId, RecordKey, RecordKind, RecordVersion, RecordedScalingEvent,
    ScalingAuditEvent, ScalingEventKind, ScalingEventSeq, StateRepositoryPort,
};
use gang_testkit::{POSTGRES_ARCHIVE_ENV, PostgresServer, SUPERUSER, shared_postgres};
use gang_types::{
    CertSerial, FleetName, Identity, NonZeroDuration, Principal, ProviderName, WorkerId,
};

#[tokio::test]
async fn concurrent_plugins_migrate_one_database() -> Result<(), Box<dyn Error>> {
    let server = shared_postgres()?;
    let config = support::database(&server).await?;
    let a = support::build(config.clone())?;
    let b = support::build(config)?;
    tokio::try_join!(a.revocation_epoch(), b.revocation_epoch())?;
    Ok(())
}

#[tokio::test]
async fn text_audit_details_migrate_without_loss() -> Result<(), Box<dyn Error>> {
    let server = shared_postgres()?;
    let config = support::database(&server).await?;
    let db = support::build(config.clone())?;
    let original = ScalingAuditEvent {
        at: Timestamp::from_unix_millis(10),
        fleet: FleetName::try_new("workers")?,
        kind: ScalingEventKind::LaunchFailed {
            from: 0,
            to: 1,
            reason: "quota café\nretry".to_owned(),
        },
        cause: None,
    };
    let first = db.record_scaling_event(&original).await?;
    let raw = support::raw(&server, &config.database).await?;
    raw.batch_execute(
        "ALTER TABLE scaling_audit ALTER COLUMN detail TYPE text USING convert_from(detail, 'UTF8'); \
         UPDATE schema_version SET current = 1, oldest_compatible = 1;",
    )
    .await?;
    drop(db);

    let db = support::build(config)?;
    let limit = NonZeroUsize::new(10).ok_or("zero limit")?;
    let mut events = Vec::new();
    let more = db
        .scaling_events(ScalingEventSeq::new(0), None, limit, &mut events)
        .await?;
    assert!(!more);
    assert_eq!(
        events,
        vec![RecordedScalingEvent {
            seq: first,
            event: original.clone(),
        }]
    );

    let mut second_event = original.clone();
    second_event.kind = ScalingEventKind::LaunchFailed {
        from: 0,
        to: 1,
        reason: "quota \0café\nretry".to_owned(),
    };
    let second = db.record_scaling_event(&second_event).await?;
    events.clear();
    let more = db
        .scaling_events(ScalingEventSeq::new(0), None, limit, &mut events)
        .await?;
    assert!(!more);
    assert_eq!(
        events,
        vec![
            RecordedScalingEvent {
                seq: first,
                event: original,
            },
            RecordedScalingEvent {
                seq: second,
                event: second_event,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn unreachable_server_is_unavailable() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let mut config = PostgresConfig::new("127.0.0.1", "postgres", SUPERUSER);
    config.port = NonZeroU16::new(port).ok_or("listener port is zero")?;
    config.tls = Tls::Disable;
    config.connect_timeout = NonZeroDuration::from_secs(1);
    let db = support::build(config)?;
    assert!(matches!(
        db.revocation_epoch().await,
        Err(PortError::Unavailable(_))
    ));
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn timed_out_connection_is_discarded() -> Result<(), Box<dyn Error>> {
    let server = gang_testkit::PostgresServer::from_env()?;
    let mut config = support::database(&server).await?;
    config.connect_timeout = NonZeroDuration::from_millis(500);
    config.statement_timeout = NonZeroDuration::from_millis(500);
    let db = support::build(config)?;
    db.revocation_epoch().await?;
    server.pause()?;
    let paused = db.revocation_epoch().await;
    server.resume()?;
    assert!(matches!(paused, Err(PortError::Unavailable(_))));
    db.revocation_epoch().await?;
    Ok(())
}

#[tokio::test]
async fn incompatible_schema_refuses_all_port_families() -> Result<(), Box<dyn Error>> {
    let server = shared_postgres()?;
    let config = support::database(&server).await?;
    let db = support::build(config.clone())?;
    db.revocation_epoch().await?;
    let client = support::raw(&server, &config.database).await?;
    let newer = i32::try_from(
        gang_postgres::SCHEMA_VERSION
            .current
            .checked_add(1)
            .ok_or("schema version overflow")?,
    )?;
    client
        .execute(
            "UPDATE schema_version SET current = $1, oldest_compatible = $1",
            &[&newer],
        )
        .await?;
    drop(db);
    let db = support::build(config)?;
    let now = Timestamp::from_unix_millis(1);
    let expires = Timestamp::from_unix_millis(10);
    assert!(matches!(
        db.revocation_epoch().await,
        Err(PortError::Backend(_))
    ));
    assert!(matches!(
        db.acquire(&ControllerId::new("a"), now, expires).await,
        Err(PortError::Backend(_))
    ));
    let key = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([1; 32]),
    };
    assert!(matches!(
        db.get(&key, now).await,
        Err(PortError::Backend(_))
    ));
    Ok(())
}

fn logged_server(settings: &[(&str, &str)]) -> Result<PostgresServer, Box<dyn Error>> {
    let archive = std::env::var_os(POSTGRES_ARCHIVE_ENV).ok_or("postgres archive is unset")?;
    Ok(PostgresServer::start_with(Path::new(&archive), settings)?)
}

async fn wait_for_log(
    server: &PostgresServer,
    predicate: impl Fn(&[String]) -> bool,
) -> Result<Vec<String>, Box<dyn Error>> {
    Ok(tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let lines = server.log();
            if predicate(&lines) {
                break lines;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?)
}

#[tokio::test]
async fn no_named_prepared_statements() -> Result<(), Box<dyn Error>> {
    let server = logged_server(&[("log_statement", "all")])?;
    let mut config = support::database(&server).await?;
    config.pool_size = NonZeroUsize::new(1).ok_or("zero pool size")?;
    let db = support::build(config)?;
    let now = Timestamp::from_unix_millis(1);
    let expires = Timestamp::from_unix_millis(100);
    let record = IssuedCertRecord {
        serial: CertSerial::from_be_bytes(&[1])?,
        identity: Identity::new(
            ProviderName::try_new("example")?,
            Principal::try_new("alice")?,
        ),
        issued_at: now,
        expires_at: expires,
        status: CertStatus::Active,
    };
    db.record_issued(&record).await?;
    let limit = NonZeroUsize::new(10).ok_or("zero limit")?;
    db.certificate(&record.serial).await?;
    db.certificates_of(&record.identity).await?;
    db.certificates(None, limit, &mut Vec::new()).await?;
    db.observe_principal(&record.identity, now).await?;
    db.principal(&record.identity).await?;
    db.revoke_principal(&record.identity, now).await?;
    db.revoked_principals().await?;
    db.principals(None, limit, true, &mut Vec::new()).await?;
    db.restore_principal(&record.identity).await?;
    db.revoke(&record.serial, now).await?;
    db.revoked().await?;
    db.record_scaling_event(&ScalingAuditEvent {
        at: now,
        fleet: FleetName::try_new("fleet-a")?,
        kind: ScalingEventKind::WorkerRegistered {
            worker: WorkerId::try_new("worker-a")?,
        },
        cause: None,
    })
    .await?;
    db.scaling_events(ScalingEventSeq::new(0), None, limit, &mut Vec::new())
        .await?;
    let Acquisition::Acquired(lease) = db.acquire(&ControllerId::new("a"), now, expires).await?
    else {
        return Err("lease was not acquired".into());
    };
    db.renew(&lease, expires).await?;
    db.release(&lease).await?;
    let key = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([1; 32]),
    };
    db.insert(&[(key, b"body")], RecordVersion::new(1), now, expires)
        .await?;
    db.get(&key, now).await?;
    db.replace(&key, RecordVersion::new(1), b"new", now, expires)
        .await?;
    db.remove(&key, RecordVersion::new(2), now).await?;
    db.purge(now).await?;
    db.revocation_epoch().await?;
    let lines = wait_for_log(&server, |lines| {
        lines
            .iter()
            .any(|line| line.contains("SELECT value FROM counters WHERE name = 'revocation'"))
    })
    .await?;
    assert!(
        lines.iter().any(|line| line.contains("execute <unnamed>")),
        "{lines:?}"
    );
    let timeout = lines
        .iter()
        .position(|line| line.contains("SET LOCAL statement_timeout"))
        .ok_or("migration did not set a local statement timeout")?;
    assert!(
        lines[timeout].contains("idle_in_transaction_session_timeout"),
        "migration timeout lacks idle_in_transaction_session_timeout: {}",
        lines[timeout]
    );
    let lock = lines
        .iter()
        .position(|line| line.contains("pg_advisory_xact_lock"))
        .ok_or("migration lock was not logged")?;
    assert!(
        timeout < lock,
        "migration lock was taken before its timeout: {lines:?}"
    );
    for line in &lines {
        if line.contains("execute ") || line.contains("statement:") {
            let named = line.split_whitespace().any(|word| {
                let word = word.trim_matches(|c: char| !c.is_ascii_alphanumeric());
                word.strip_prefix('s').is_some_and(|digits| {
                    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
                })
            });
            assert!(!named, "named prepared statement: {line}");
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_cancelled_call_discards_its_connection() -> Result<(), Box<dyn Error>> {
    let server = logged_server(&[("log_connections", "on")])?;
    let mut config = support::database(&server).await?;
    config.pool_size = NonZeroUsize::new(1).ok_or("zero pool size")?;
    config.statement_timeout = NonZeroDuration::from_secs(10);
    let database = config.database.clone();
    let db = support::build(config)?;
    db.revocation_epoch().await?;
    server.pause()?;
    let cancelled = tokio::time::timeout(Duration::from_millis(300), db.revocation_epoch()).await;
    server.resume()?;
    assert!(
        cancelled.is_err(),
        "paused call completed before cancellation"
    );
    db.revocation_epoch().await?;
    let matches_database = |lines: &[String]| {
        lines
            .iter()
            .filter(|line| {
                line.contains("connection authorized:")
                    && line.contains(&format!("database={database}"))
            })
            .count()
    };
    let lines = wait_for_log(&server, |lines| matches_database(lines) >= 2).await?;
    assert_eq!(matches_database(&lines), 2, "{lines:?}");
    Ok(())
}

#[tokio::test]
async fn a_transient_sql_error_discards_its_connection() -> Result<(), Box<dyn Error>> {
    let server = logged_server(&[("log_connections", "on")])?;
    let mut config = support::database(&server).await?;
    config.pool_size = NonZeroUsize::new(1).ok_or("zero pool size")?;
    let database = config.database.clone();
    let admin = support::raw(&server, &database).await?;
    let db = support::build(config)?;
    db.revocation_epoch().await?;
    let count = |lines: &[String]| {
        lines
            .iter()
            .filter(|line| {
                line.contains("connection authorized:")
                    && line.contains(&format!("database={database}"))
            })
            .count()
    };
    let before = count(&wait_for_log(&server, |lines| count(lines) >= 2).await?);
    admin
        .batch_execute("ALTER SYSTEM SET default_transaction_read_only = on")
        .await?;
    admin.batch_execute("SELECT pg_reload_conf()").await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = admin
                .query_one("SHOW default_transaction_read_only", &[])
                .await?;
            if row.get::<_, &str>(0) == "on" {
                break Ok::<(), tokio_postgres::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let failed = db
        .acquire(
            &ControllerId::new("a"),
            Timestamp::from_unix_millis(1),
            Timestamp::from_unix_millis(100),
        )
        .await;
    admin
        .batch_execute("ALTER SYSTEM RESET default_transaction_read_only")
        .await?;
    admin.batch_execute("SELECT pg_reload_conf()").await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = admin
                .query_one("SHOW default_transaction_read_only", &[])
                .await?;
            if row.get::<_, &str>(0) == "off" {
                break Ok::<(), tokio_postgres::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert!(
        matches!(failed, Err(PortError::Unavailable(_))),
        "expected transient error: {failed:?}"
    );
    db.revocation_epoch().await?;
    let lines = wait_for_log(&server, |lines| count(lines) > before).await?;
    assert!(count(&lines) > before, "transient error reused connection");
    Ok(())
}

#[tokio::test]
async fn invalid_stored_schema_is_corrupt() -> Result<(), Box<dyn Error>> {
    let server = shared_postgres()?;
    let config = support::database(&server).await?;
    let db = support::build(config.clone())?;
    db.revocation_epoch().await?;
    let client = support::raw(&server, &config.database).await?;
    for (current, oldest) in [(5_i32, 0_i32), (0, 0), (1, 2)] {
        client
            .execute(
                "UPDATE schema_version SET current = $1, oldest_compatible = $2",
                &[&current, &oldest],
            )
            .await?;
        let fresh = support::build(config.clone())?;
        assert_corrupt(fresh.revocation_epoch().await, "schema_version");
    }
    Ok(())
}

#[tokio::test]
async fn malformed_stored_values_and_missing_counter_are_backend_errors()
-> Result<(), Box<dyn Error>> {
    let server = shared_postgres()?;
    let config = support::database(&server).await?;
    let db = support::build(config.clone())?;
    db.revocation_epoch().await?;
    let client = support::raw(&server, &config.database).await?;
    client.batch_execute(
        "INSERT INTO leases (name, holder, epoch, expires_at) VALUES ('leader', 'a', -1, '2100-01-01T00:00:00Z'); \
         DELETE FROM counters WHERE name = 'revocation'"
    ).await?;
    let now = Timestamp::from_unix_millis(1);
    assert_corrupt(db.acquire(&ControllerId::new("b"), now, now).await, "epoch");
    client
        .batch_execute("ALTER TABLE leases ALTER COLUMN expires_at DROP NOT NULL; UPDATE leases SET epoch = 1, expires_at = NULL WHERE name = 'leader'")
        .await?;
    let row = client
        .query_one("SELECT expires_at FROM leases WHERE name = 'leader'", &[])
        .await?;
    assert!(
        row.try_get::<_, time::OffsetDateTime>("expires_at")
            .is_err(),
        "NULL must fail timestamp decoding"
    );
    assert_corrupt(
        db.acquire(&ControllerId::new("b"), now, now).await,
        "expires_at",
    );
    assert_corrupt(db.revocation_epoch().await, "counters");
    Ok(())
}

fn assert_corrupt<T>(result: Result<T, PortError>, column: &'static str) {
    let error = result.err();
    assert!(
        matches!(
            &error,
            Some(PortError::Backend(source))
                if matches!(
                    source.downcast_ref::<PostgresError>(),
                    Some(PostgresError::Corrupt { column: actual, .. }) if *actual == column
                )
        ),
        "expected corrupt {column}, got {error:?}"
    );
}

async fn wait_blocked_behind(
    observer: &tokio_postgres::Client,
    database: &str,
    blocker_pid: i32,
) -> Result<(), Box<dyn Error>> {
    loop {
        let blocked = observer
            .query_opt(
                "SELECT pid FROM pg_stat_activity WHERE datname = $1 AND application_name = $2 \
                 AND state = $3 AND wait_event_type = $4 AND $5 = ANY(pg_blocking_pids(pid))",
                &[&database, &"gang", &"active", &"Lock", &blocker_pid],
            )
            .await?;
        if blocked.is_some() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_cancelled_lease_call_cannot_commit_once_its_lock_is_free() -> Result<(), Box<dyn Error>>
{
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = shared_postgres()?;
        let mut config = support::database(&server).await?;
        config.pool_size = NonZeroUsize::new(1).ok_or("zero pool size")?;
        config.statement_timeout = NonZeroDuration::from_secs(30);
        let database = config.database.clone();
        let db: Postgres = support::build(config)?;
        let seeded = db
            .acquire(
                &ControllerId::new("seed"),
                Timestamp::from_unix_millis(1),
                Timestamp::from_unix_millis(1001),
            )
            .await?;
        if !matches!(seeded, Acquisition::Acquired(_)) {
            return Err("seed lease was not acquired".into());
        }
        let blocker = support::raw(&server, &database).await?;
        let observer = support::raw(&server, &database).await?;
        let blocker_pid: i32 = blocker.query_one("SELECT pg_backend_pid()", &[]).await?.get(0);
        blocker.batch_execute("BEGIN").await?;
        blocker
            .query("SELECT 1 FROM leases WHERE name = 'leader' FOR UPDATE", &[])
            .await?;
        {
            let next = ControllerId::new("next");
            let acquire = db.acquire(
                &next,
                Timestamp::from_unix_millis(2000),
                Timestamp::from_unix_millis(3000),
            );
            tokio::select! {
                result = acquire => {
                    return Err(format!("acquire finished while its row was locked: {result:?}").into());
                }
                ready = wait_blocked_behind(&observer, &database, blocker_pid) => ready?,
            }
        } // `acquire` is dropped here, with its statement waiting on the row lock.
        // The observer's lock request queues behind the dropped call's request, so it is granted only
        // after the dropped call's backend ends its transaction; the row it reads is the outcome.
        let (rows, ()) = tokio::try_join!(
            observer.query(
                "SELECT holder, epoch FROM leases WHERE name = 'leader' FOR UPDATE",
                &[],
            ),
            blocker.batch_execute("ROLLBACK"),
        )?;
        let row = rows.first().ok_or("lease row disappeared")?;
        let outcome = (row.get::<_, Option<String>>(0), row.get::<_, i64>(1));
        assert_eq!(
            outcome,
            (Some("seed".to_owned()), 1),
            "a cancelled acquire committed after its caller was gone"
        );
        Ok::<(), Box<dyn Error>>(())
    })
    .await?
}
