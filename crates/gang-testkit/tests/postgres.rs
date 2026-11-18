//! Integration tests for the isolated `PostgreSQL` test server.

use std::sync::Arc;

use gang_testkit::{PostgresServer, SUPERUSER, TestResult, shared_postgres};
use rustix::io::Errno;
use rustix::process::{Pid, test_kill_process};

#[tokio::test]
async fn creates_distinct_usable_databases() -> TestResult {
    let server = PostgresServer::from_env()?;
    let first = server.create_database().await?;
    let second = server.create_database().await?;
    assert_ne!(first, second);
    for database in [first, second] {
        let (client, connection) = tokio_postgres::Config::new()
            .host_path(server.socket_dir())
            .port(server.port())
            .user(SUPERUSER)
            .dbname(&database)
            .connect(tokio_postgres::NoTls)
            .await?;
        let task = tokio::spawn(connection);
        let row = client.query_one("SELECT 1::int4", &[]).await?;
        assert_eq!(row.get::<_, i32>(0), 1);
        drop(client);
        task.await??;
    }
    Ok(())
}

#[tokio::test]
async fn applies_settings_and_retains_ready_log() -> TestResult {
    let archive = std::env::var_os(gang_testkit::POSTGRES_ARCHIVE_ENV)
        .ok_or("PostgreSQL archive environment variable is unset")?;
    let server =
        PostgresServer::start_with(std::path::Path::new(&archive), &[("log_statement", "all")])?;
    assert!(
        server
            .log()
            .iter()
            .any(|line| line.contains("database system is ready to accept connections"))
    );
    let (client, connection) = tokio_postgres::Config::new()
        .host_path(server.socket_dir())
        .port(server.port())
        .user(SUPERUSER)
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await?;
    let task = tokio::spawn(connection);
    let row = client.query_one("SHOW log_statement", &[]).await?;
    assert_eq!(row.get::<_, &str>(0), "all");
    drop(client);
    task.await??;
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pause_blocks_connections_until_resume() -> TestResult {
    let server = PostgresServer::from_env()?;
    let database = server.create_database().await?;
    let mut config = tokio_postgres::Config::new();
    config
        .host_path(server.socket_dir())
        .port(server.port())
        .user(SUPERUSER)
        .dbname(&database);
    server.pause()?;
    let paused = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        config.connect(tokio_postgres::NoTls),
    )
    .await;
    server.resume()?;
    assert!(
        paused.is_err(),
        "connection completed while server was paused"
    );
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    let task = tokio::spawn(connection);
    client.query_one("SELECT 1::int4", &[]).await?;
    drop(client);
    task.await??;
    Ok(())
}

#[tokio::test]
async fn shares_only_while_held() -> TestResult {
    let first = shared_postgres()?;
    let second = shared_postgres()?;
    assert!(Arc::ptr_eq(&first, &second));
    let pid = first.pid();
    drop(first);
    drop(second);
    let fresh = shared_postgres()?;
    assert_ne!(pid, fresh.pid());
    Ok(())
}

#[test]
fn drop_stops_postmaster() -> TestResult {
    let server = PostgresServer::from_env()?;
    let pid = Pid::from_raw(i32::try_from(server.pid())?).ok_or("invalid postmaster pid")?;
    drop(server);
    assert_eq!(test_kill_process(pid), Err(Errno::SRCH));
    Ok(())
}
