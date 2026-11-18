//! `PostgreSQL`-backed shared state for one or many controllers, behind the database ports:
//! the certificate ledger, principals, scaling audit, leader lease and expiring records.
//! No session state is used; transaction-mode poolers have not been runtime-tested.
//!
//! ```
//! use std::time::Duration;
//! use gang_postgres::{PostgresConfig, Tls};
//!
//! let config = PostgresConfig::new("db.example.com", "gang", "gang");
//! assert_eq!(config.tls, Tls::VerifyFull);
//! assert_eq!(config.port.get(), 5432);
//! assert_eq!(config.pool_size.get(), 8);
//! assert_eq!(config.connect_timeout.get(), Duration::from_secs(5));
//! assert_eq!(config.statement_timeout.get(), Duration::from_secs(5));
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod codec;
mod config;
mod errors;
mod expiring;
mod lease;
mod migrations;
mod plugin;
mod repository;

pub use config::{PostgresConfig, Tls};
pub use errors::PostgresError;
pub use migrations::SCHEMA_VERSION;
pub use plugin::Postgres;
