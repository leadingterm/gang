//! Verifies `PostgreSQL` TLS hostname and certificate trust handling.

use std::error::Error;
use std::fs;
use std::num::NonZeroU16;
use std::path::Path;

use gang_postgres::{Postgres, PostgresConfig, Tls};
use gang_spi::{Plugin, PluginContext, PortError, StateRepositoryPort};
use gang_testkit::{
    ExtendedKeyUsagePurpose, POSTGRES_ARCHIVE_ENV, PostgresServer, SUPERUSER, TestAuthority,
};

#[tokio::test]
async fn verify_full_requires_the_correct_ca() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::TempDir::new()?;
    let authority = TestAuthority::new("gang test ca")?;
    let leaf = authority.issue(
        vec!["127.0.0.1".to_owned()],
        vec![ExtendedKeyUsagePurpose::ServerAuth],
        [1; 16],
        "127.0.0.1",
    )?;
    let cert = dir.path().join("server.crt");
    let key = dir.path().join("server.key");
    let ca = dir.path().join("ca.pem");
    fs::write(&cert, leaf.pem.as_str())?;
    fs::write(&key, leaf.key_pem.expose())?;
    fs::write(&ca, authority.pem.as_str())?;
    let archive = std::env::var_os(POSTGRES_ARCHIVE_ENV).ok_or("PostgreSQL archive is not set")?;
    let server = PostgresServer::start_tls(Path::new(&archive), &cert, &key)?;
    let database = server.create_database().await?;
    let mut config = PostgresConfig::new("127.0.0.1", database, SUPERUSER);
    config.port = NonZeroU16::new(server.port()).ok_or("server port is zero")?;
    config.tls = Tls::VerifyFull;
    config.ca_file = Some(ca);
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    let trusted = Postgres::build(config.clone(), &context)?;
    trusted.revocation_epoch().await?;

    let other = TestAuthority::new("other test ca")?;
    let other_ca = dir.path().join("other-ca.pem");
    fs::write(&other_ca, other.pem.as_str())?;
    config.ca_file = Some(other_ca);
    let untrusted = Postgres::build(config.clone(), &context)?;
    let error = untrusted
        .revocation_epoch()
        .await
        .expect_err("a different CA must reject the server certificate");
    assert!(
        gang_tls::is_certificate_refused(&error),
        "expected a TLS certificate failure: {error:?}"
    );
    assert!(
        matches!(error, PortError::Backend(_)),
        "expected permanent error: {error:?}"
    );

    config.ca_file = None;
    let system_roots = Postgres::build(config.clone(), &context)?;
    let error = system_roots
        .revocation_epoch()
        .await
        .expect_err("system roots must reject the test CA");
    assert!(
        gang_tls::is_certificate_refused(&error),
        "expected a TLS certificate failure: {error:?}"
    );
    assert!(
        matches!(error, PortError::Backend(_)),
        "expected permanent error: {error:?}"
    );

    let mismatch = authority.issue(
        vec!["other.example.test".to_owned()],
        vec![ExtendedKeyUsagePurpose::ServerAuth],
        [2; 16],
        "other.example.test",
    )?;
    let mismatch_cert = dir.path().join("mismatch.crt");
    let mismatch_key = dir.path().join("mismatch.key");
    fs::write(&mismatch_cert, mismatch.pem.as_str())?;
    fs::write(&mismatch_key, mismatch.key_pem.expose())?;
    let mismatch_server =
        PostgresServer::start_tls(Path::new(&archive), &mismatch_cert, &mismatch_key)?;
    config.port = NonZeroU16::new(mismatch_server.port()).ok_or("server port is zero")?;
    config.ca_file = Some(dir.path().join("ca.pem"));
    let mismatch_client = Postgres::build(config, &context)?;
    let error = mismatch_client
        .revocation_epoch()
        .await
        .expect_err("a certificate for another hostname must be rejected");
    assert!(
        gang_tls::is_certificate_refused(&error),
        "expected a TLS certificate failure: {error:?}"
    );
    assert!(
        matches!(error, PortError::Backend(_)),
        "expected permanent error: {error:?}"
    );
    Ok(())
}
