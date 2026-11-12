//! Store construction and certificate fixtures shared by the `gang-sqlite` test binaries.

use std::path::Path;

use gang_clock::Timestamp;
use gang_spi::{CertStatus, IssuedCertRecord, Plugin, PluginContext, PortError};
use gang_sqlite::{Sqlite, SqliteConfig};
use gang_types::{CertSerial, Identity, Principal, ProviderName};

const ONE_HOUR_MILLIS: u64 = 3_600_000;

/// Builds a store from `settings`, returning the error when the configuration is rejected.
pub fn try_open(settings: SqliteConfig) -> Result<Sqlite, PortError> {
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    Sqlite::build(settings, &context)
}

/// Builds a store from `settings`, panicking when the configuration is rejected.
pub fn open(settings: SqliteConfig) -> Sqlite {
    match try_open(settings) {
        Ok(store) => store,
        Err(error) => panic!("valid configuration builds: {error:?}"),
    }
}

/// A certificate issued to `dev@example.test` under provider `alpha`, valid for one hour.
pub fn issued(serial_text: &str, issued_at: u64) -> IssuedCertRecord {
    IssuedCertRecord {
        serial: CertSerial::try_new(serial_text).expect("test serial is valid"),
        identity: Identity::new(
            ProviderName::try_new("alpha").expect("test provider is valid"),
            Principal::try_new("dev@example.test").expect("test principal is valid"),
        ),
        issued_at: Timestamp::from_unix_millis(issued_at),
        expires_at: Timestamp::from_unix_millis(issued_at + ONE_HOUR_MILLIS),
        status: CertStatus::Active,
    }
}
