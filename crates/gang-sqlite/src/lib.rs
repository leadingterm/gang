//! `SQLite`-backed state for Gang: the certificate ledger and the scaling audit log.
//!
//! The store opens its database lazily, on first use, so building a plugin never touches disk.
//!
//! Each store drives its database on a dedicated thread, so statements never block async workers.
//!
//! ```
//! use gang_sqlite::{SqliteConfig, Synchronous};
//! use std::time::Duration;
//! # fn assert_config<T: serde::Serialize + for<'de> serde::Deserialize<'de>>() {}
//! assert_config::<SqliteConfig>();
//! let config = SqliteConfig::new("gang.db".into());
//! assert_eq!(config.busy_timeout.get(), Duration::from_secs(5));
//! assert_eq!(config.synchronous, Synchronous::Normal);
//! ```

mod codec;
mod config;
mod errors;
mod migrations;
mod repository;
mod store;

pub use config::{JournalMode, SqliteConfig, Synchronous};
pub use errors::SqliteError;
pub use store::Sqlite;
