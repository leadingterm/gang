//! Runs the shared database contract against `SQLite`, whose lease is its exclusive file lock
//! (tested in `src/lease.rs`).

use std::path::Path;

use gang_spi::{Plugin, PluginContext, StateRepositoryPort};
use gang_sqlite::{Sqlite, SqliteConfig};
use tempfile::TempDir;

gang_testkit::database_contract!(
    fresh, exclusive, schema: gang_sqlite::SCHEMA_VERSION, stamped
);

fn fresh() -> std::future::Ready<Result<gang_testkit::Fixture<Sqlite>, Box<dyn std::error::Error>>>
{
    std::future::ready(fresh_database())
}

fn fresh_database() -> Result<gang_testkit::Fixture<Sqlite>, Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    let db = Sqlite::build(SqliteConfig::new(dir.path().join("gang.db")), &context)?;
    Ok(gang_testkit::Fixture::keeping(db, dir))
}

async fn stamped(
    stored: gang_spi::SchemaVersion,
) -> Result<gang_testkit::Fixture<Sqlite>, Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let path = dir.path().join("gang.db");
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    let db = Sqlite::build(SqliteConfig::new(path.clone()), &context)?;
    db.revocation_epoch().await?;
    db.close().await?;
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute_batch(&format!("PRAGMA user_version = {}", stored.current))?;
    connection.execute("DELETE FROM schema_compatibility", [])?;
    connection.execute(
        "INSERT INTO schema_compatibility (oldest_compatible) VALUES (?1)",
        [stored.oldest_compatible],
    )?;
    drop(connection);
    let db = Sqlite::build(SqliteConfig::new(path), &context)?;
    Ok(gang_testkit::Fixture::keeping(db, dir))
}
