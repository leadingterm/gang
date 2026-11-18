//! Helpers for isolated `PostgreSQL` integration tests.

use std::error::Error;
use std::num::NonZeroU16;
use std::path::Path;

use gang_postgres::{Postgres, PostgresConfig, Tls};
use gang_spi::{Plugin, PluginContext, PortError};
use gang_testkit::{PostgresServer, SUPERUSER};

/// Creates a database and returns a socket-based configuration for it.
///
/// # Errors
/// Returns an error if the database cannot be created or the socket path is not UTF-8.
pub async fn database(server: &PostgresServer) -> Result<PostgresConfig, Box<dyn Error>> {
    let name = server.create_database().await?;
    let host = server
        .socket_dir()
        .to_str()
        .ok_or("socket path is not UTF-8")?;
    let mut config = PostgresConfig::new(host, name, SUPERUSER);
    config.port = NonZeroU16::new(server.port()).ok_or("server port is zero")?;
    config.tls = Tls::Disable;
    Ok(config)
}

/// Builds a plugin using the standard test configuration path.
///
/// # Errors
/// Returns a configuration error if the plugin cannot be built.
pub fn build(config: PostgresConfig) -> Result<Postgres, PortError> {
    Postgres::build(
        config,
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )
}

/// Connects directly to a database on the server's Unix socket.
///
/// # Errors
/// Returns an error if the connection cannot be established.
pub async fn raw(
    server: &PostgresServer,
    database: &str,
) -> Result<tokio_postgres::Client, Box<dyn Error>> {
    let (client, connection) = tokio_postgres::Config::new()
        .host_path(server.socket_dir())
        .port(server.port())
        .user(SUPERUSER)
        .dbname(database)
        .connect(tokio_postgres::NoTls)
        .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}
