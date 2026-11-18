use std::net::IpAddr;
use std::num::{NonZeroU16, NonZeroUsize};
use std::path::PathBuf;

use gang_types::NonZeroDuration;
use rustls_pki_types::ServerName;
use serde::{Deserialize, Serialize};

use crate::PostgresError;

const DEFAULT_PORT: NonZeroU16 = NonZeroU16::new(5432).expect("5432 is nonzero");
const DEFAULT_POOL_SIZE: NonZeroUsize = NonZeroUsize::new(8).expect("8 is nonzero");
const DEFAULT_CONNECT_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(5);
const DEFAULT_STATEMENT_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(5);
const MAX_STATEMENT_TIMEOUT_MILLIS: u128 = 2_147_483_647;

/// TLS policy for the connection to `PostgreSQL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Tls {
    /// Verify the certificate chain against `ca_file`, or the system trust store if unset,
    /// and verify the host name.
    #[default]
    VerifyFull,
    /// Send everything in clear text. Allowed only for a loopback IP or a Unix socket.
    Disable,
}

/// Settings of the `PostgreSQL` state store, from `[database.postgres]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// A host name, an IP address, or the absolute path of the directory holding the server's
    /// Unix socket.
    pub host: String,
    /// Server port. Defaults to 5432.
    #[serde(default = "default_port")]
    pub port: NonZeroU16,
    /// Database name.
    pub database: String,
    /// User name.
    pub user: String,
    /// Password file, relative to the configuration file if not absolute.
    pub password_file: Option<PathBuf>,
    /// TLS policy. Defaults to [`Tls::VerifyFull`].
    #[serde(default)]
    pub tls: Tls,
    /// PEM CA bundle, relative to the configuration file if not absolute. When unset,
    /// verification uses the system trust store.
    pub ca_file: Option<PathBuf>,
    /// Maximum number of pooled connections. Defaults to eight.
    #[serde(default = "default_pool_size")]
    pub pool_size: NonZeroUsize,
    /// Maximum time to open a connection. Defaults to five seconds.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: NonZeroDuration,
    /// Maximum time for a statement to finish. Defaults to five seconds.
    #[serde(default = "default_statement_timeout")]
    pub statement_timeout: NonZeroDuration,
}

impl PostgresConfig {
    /// Creates a configuration with the specified server, database and user, and other defaults.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        user: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: DEFAULT_PORT,
            database: database.into(),
            user: user.into(),
            password_file: None,
            tls: Tls::default(),
            ca_file: None,
            pool_size: DEFAULT_POOL_SIZE,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            statement_timeout: DEFAULT_STATEMENT_TIMEOUT,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), PostgresError> {
        let detail = if self.host.is_empty() {
            Some("host must not be empty")
        } else if self.database.is_empty() {
            Some("database must not be empty")
        } else if self.user.is_empty() {
            Some("user must not be empty")
        } else if !(1..=MAX_STATEMENT_TIMEOUT_MILLIS)
            .contains(&self.statement_timeout.get().as_millis())
        {
            Some("statement_timeout must be between 1ms and 2147483647ms")
        } else if self
            .password_file
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            Some("password_file must not be empty")
        } else if self
            .ca_file
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            Some("ca_file must not be empty")
        } else if self.tls == Tls::Disable
            && !self.host.starts_with('/')
            && !self.host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
        {
            Some("tls = \"disable\" needs a loopback address or a unix socket directory")
        } else if self.tls == Tls::VerifyFull && self.host.starts_with('/') {
            Some("a unix socket cannot carry tls: set tls = \"disable\"")
        } else if self.tls == Tls::VerifyFull && ServerName::try_from(self.host.as_str()).is_err() {
            Some("host must be a valid tls server name")
        } else if self.tls == Tls::Disable && self.ca_file.is_some() {
            Some("ca_file needs tls = \"verify-full\"")
        } else {
            None
        };
        match detail {
            Some(detail) => Err(PostgresError::Config { detail }),
            None => Ok(()),
        }
    }
}

const fn default_port() -> NonZeroU16 {
    DEFAULT_PORT
}

const fn default_pool_size() -> NonZeroUsize {
    DEFAULT_POOL_SIZE
}

const fn default_connect_timeout() -> NonZeroDuration {
    DEFAULT_CONNECT_TIMEOUT
}

const fn default_statement_timeout() -> NonZeroDuration {
    DEFAULT_STATEMENT_TIMEOUT
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid(config: &PostgresConfig, expected: &'static str) {
        assert!(
            matches!(config.validate(), Err(PostgresError::Config { detail }) if detail == expected)
        );
    }

    #[test]
    fn required_fields_are_not_empty() {
        let mut config = PostgresConfig::new("db.example.com", "gang", "gang");
        assert!(config.validate().is_ok());
        config.host.clear();
        assert_invalid(&config, "host must not be empty");
        config.host = "db.example.com".into();
        config.database.clear();
        assert_invalid(&config, "database must not be empty");
        config.database = "gang".into();
        config.user.clear();
        assert_invalid(&config, "user must not be empty");
    }

    #[test]
    fn clear_text_requires_loopback_or_socket() {
        let mut config = PostgresConfig::new("127.0.0.1", "gang", "gang");
        config.tls = Tls::Disable;
        assert!(config.validate().is_ok());
        config.host = "::1".into();
        assert!(config.validate().is_ok());
        config.host = "/tmp".into();
        assert!(config.validate().is_ok());
        for host in ["db.example.com", "localhost", "192.0.2.1", "2001:db8::1"] {
            config.host = host.into();
            assert_invalid(
                &config,
                "tls = \"disable\" needs a loopback address or a unix socket directory",
            );
        }
    }

    #[test]
    fn socket_rejects_tls_and_ca_requires_tls() {
        let mut config = PostgresConfig::new("/tmp", "gang", "gang");
        assert_invalid(
            &config,
            "a unix socket cannot carry tls: set tls = \"disable\"",
        );
        config.host = "db.example.com".into();
        config.ca_file = Some("ca.pem".into());
        assert!(config.validate().is_ok());
        config.host = "127.0.0.1".into();
        config.tls = Tls::Disable;
        assert_invalid(&config, "ca_file needs tls = \"verify-full\"");
    }

    #[test]
    fn statement_timeout_fits_postgres_milliseconds() {
        let mut config = PostgresConfig::new("db.example.com", "gang", "gang");
        for millis in [1, 2_147_483_647] {
            config.statement_timeout = NonZeroDuration::from_millis(millis);
            assert!(config.validate().is_ok());
        }
        config.statement_timeout = NonZeroDuration::from_millis(2_147_483_648);
        assert_invalid(
            &config,
            "statement_timeout must be between 1ms and 2147483647ms",
        );
        assert!(NonZeroDuration::try_new(std::time::Duration::from_nanos(1)).is_err());
    }

    #[test]
    fn verify_full_requires_a_valid_server_name() {
        let mut config = PostgresConfig::new("db.example.com:5432", "gang", "gang");
        assert_invalid(&config, "host must be a valid tls server name");
        for host in ["db.example.com", "127.0.0.1", "::1"] {
            config.host = host.into();
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn file_paths_must_not_be_empty() {
        let mut config = PostgresConfig::new("db.example.com", "gang", "gang");
        config.password_file = Some(PathBuf::new());
        assert_invalid(&config, "password_file must not be empty");
        config.password_file = None;
        config.ca_file = Some(PathBuf::new());
        assert_invalid(&config, "ca_file must not be empty");
    }

    #[test]
    fn serde_defaults_round_trip_and_rejects_unknown_keys() -> gang_testkit::TestResult {
        let minimal = "host = \"db.example.com\"\ndatabase = \"gang\"\nuser = \"gang\"\n";
        let config: PostgresConfig = toml::from_str(minimal)?;
        assert_eq!(
            config,
            PostgresConfig::new("db.example.com", "gang", "gang")
        );
        assert_eq!(
            toml::from_str::<PostgresConfig>(&toml::to_string(&config)?)?,
            config
        );
        let disabled: PostgresConfig = toml::from_str(&format!("{minimal}tls = \"disable\"\n"))?;
        assert_eq!(disabled.tls, Tls::Disable);
        assert!(toml::from_str::<PostgresConfig>(&format!("{minimal}unknown = true\n")).is_err());
        Ok(())
    }
}
