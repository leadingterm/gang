//! Tests `PostgreSQL` configuration validation.

use std::fs;
use std::path::Path;

use gang_postgres::{Postgres, PostgresConfig, Tls};
use gang_spi::{Plugin, PluginContext, PortError};
use gang_testkit::TestResult;
use tempfile::TempDir;

fn assert_config_error(config: PostgresConfig) {
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    assert!(matches!(
        Postgres::build(config, &context),
        Err(PortError::Config(_))
    ));
}

#[test]
fn password_file_must_be_readable_and_nonempty() -> TestResult {
    let dir = TempDir::new()?;
    let mut config = PostgresConfig::new("db.example.com", "gang", "gang");
    config.password_file = Some(dir.path().join("missing"));
    assert_config_error(config.clone());
    let empty = dir.path().join("empty");
    fs::write(&empty, "")?;
    config.password_file = Some(empty);
    assert_config_error(config);
    Ok(())
}

#[test]
fn ca_file_must_be_readable_and_pem() -> TestResult {
    let dir = TempDir::new()?;
    let mut config = PostgresConfig::new("db.example.com", "gang", "gang");
    config.ca_file = Some(dir.path().join("missing"));
    assert_config_error(config.clone());
    let invalid = dir.path().join("invalid.pem");
    fs::write(&invalid, "not a PEM certificate")?;
    config.ca_file = Some(invalid);
    assert_config_error(config);
    Ok(())
}

#[test]
fn transport_policy_is_checked() {
    let mut config = PostgresConfig::new("db.example.com", "gang", "gang");
    config.tls = Tls::Disable;
    assert_config_error(config);
    assert_config_error(PostgresConfig::new("/tmp", "gang", "gang"));
}

#[test]
fn relative_password_is_resolved_beside_configuration() -> TestResult {
    let dir = TempDir::new()?;
    fs::write(dir.path().join("password"), "secret\n")?;
    let path = dir.path().join("gang.toml");
    let context = PluginContext { config_path: &path };
    let mut config = PostgresConfig::new("127.0.0.1", "gang", "gang");
    config.tls = Tls::Disable;
    config.password_file = Some("password".into());
    let _postgres = Postgres::build(config, &context)?;
    Ok(())
}

#[test]
fn vendored_rds_ca_bundle_loads() -> TestResult {
    let bundle = std::env::var_os("GANG_TEST_RDS_CA_BUNDLE").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../infra/nixos/rds-ca-bundle.pem"),
        Into::into,
    );
    let mut config = PostgresConfig::new("db.example.com", "gang", "gang");
    config.ca_file = Some(bundle);
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    let _postgres = Postgres::build(config, &context)?;
    Ok(())
}
