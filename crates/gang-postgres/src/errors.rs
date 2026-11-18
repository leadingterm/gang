use std::path::PathBuf;

use gang_spi::SchemaTooNew;
use tokio_postgres::error::SqlState;

/// Why a `PostgreSQL` operation failed.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum PostgresError {
    /// The server could not be reached, dropped the connection, is overloaded, or cancelled the
    /// statement; trying again later may succeed.
    #[error("postgresql is unavailable")]
    #[failure(unavailable)]
    Unavailable(#[source] tokio_postgres::Error),
    /// A connection did not open, or a statement did not finish, within its configured bound.
    #[error("postgresql did not answer within {bound}")]
    #[failure(unavailable)]
    Timeout {
        /// The bound that expired: `connect_timeout` or `statement_timeout`.
        bound: &'static str,
    },
    /// The connection pool cannot hand out a connection.
    #[error("postgresql connection pool failed")]
    #[failure(unavailable)]
    Pool(#[source] deadpool_postgres::PoolError),
    /// The server rejected a statement in a way that trying again will not fix.
    #[error("postgresql statement failed")]
    Query(#[source] tokio_postgres::Error),
    /// The stored schema is newer than this binary can use.
    #[error("database schema cannot be used")]
    Schema(#[from] SchemaTooNew),
    /// A stored value is not valid for its column.
    #[error("column {column} holds an invalid value: {detail}")]
    Corrupt {
        /// The column holding the value.
        column: &'static str,
        /// What is wrong with it.
        detail: String,
    },
    /// A time or count does not fit in its column.
    #[error("{column} value {value} does not fit in the database")]
    Range {
        /// The column the value is written to.
        column: &'static str,
        /// The value that does not fit.
        value: u64,
    },
    /// The password file cannot be read.
    #[error("cannot read the postgresql password file {path}")]
    #[failure(config)]
    PasswordFile {
        /// The configured password file.
        path: PathBuf,
        /// Why reading it failed.
        source: std::io::Error,
    },
    /// The CA file cannot be read.
    #[error("cannot read the postgresql CA file {path}")]
    #[failure(config)]
    CaFile {
        /// The configured CA file.
        path: PathBuf,
        /// Why reading it failed.
        source: std::io::Error,
    },
    /// The CA file holds no usable PEM certificate.
    #[error("the postgresql CA file {path} is not a PEM certificate bundle")]
    #[failure(config)]
    CaInvalid {
        /// The configured CA file.
        path: PathBuf,
        /// Why the text is not a certificate bundle.
        source: gang_types::PemError,
    },
    /// The TLS client configuration cannot be built.
    #[error("cannot set up postgresql tls")]
    #[failure(config)]
    Tls(#[source] gang_tls::TransportError),
    /// The plugin configuration is unusable.
    #[error("invalid postgresql configuration: {detail}")]
    #[failure(config)]
    Config {
        /// The reason the configuration cannot be used.
        detail: &'static str,
    },
}

impl PostgresError {
    /// Whether the same request may succeed later, on another connection: these failures map to
    /// [`gang_spi::PortError::Unavailable`], and the connection they happened on is discarded.
    pub(crate) const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_) | Self::Timeout { .. } | Self::Pool(_)
        )
    }
}

/// The SQLSTATE classes and codes after which the same request may succeed later: connection
/// exceptions (class 08), shutdown and crash recovery (57P01-03), too many connections (53300),
/// serialization failures and deadlocks (40001, 40P01), lock waits (55P03), statement
/// cancellation, which `statement_timeout` raises (57014), a transaction ended by
/// `idle_in_transaction_session_timeout` (25P03), and a write that reached a read-only server,
/// as during a failover (25006).
fn is_transient_state(state: &SqlState) -> bool {
    state.code().starts_with("08")
        || [
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CRASH_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::TOO_MANY_CONNECTIONS,
            SqlState::T_R_SERIALIZATION_FAILURE,
            SqlState::T_R_DEADLOCK_DETECTED,
            SqlState::LOCK_NOT_AVAILABLE,
            SqlState::QUERY_CANCELED,
            SqlState::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            SqlState::READ_ONLY_SQL_TRANSACTION,
        ]
        .contains(state)
}

/// Whether a failure without a SQLSTATE happened in transit: the connection closed, or an I/O
/// error other than a refused server certificate. Decoding failures, unexpected row counts and
/// refused certificates fail the same way every time.
fn is_transient_transport(error: &tokio_postgres::Error) -> bool {
    if error.is_closed() {
        return true;
    }
    if gang_tls::is_certificate_refused(error) {
        return false;
    }
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if cause.is::<std::io::Error>() {
            return true;
        }
        source = cause.source();
    }
    false
}

impl From<tokio_postgres::Error> for PostgresError {
    fn from(error: tokio_postgres::Error) -> Self {
        let transient = error
            .code()
            .map_or_else(|| is_transient_transport(&error), is_transient_state);
        if transient {
            Self::Unavailable(error)
        } else {
            Self::Query(error)
        }
    }
}

impl From<deadpool_postgres::PoolError> for PostgresError {
    /// A connection that failed to open is classified like any other driver error.
    fn from(error: deadpool_postgres::PoolError) -> Self {
        match error {
            deadpool_postgres::PoolError::Backend(error) => Self::from(error),
            error => Self::Pool(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresError, is_transient_state};
    use tokio_postgres::error::SqlState;

    #[test]
    fn transient_sqlstates() {
        for code in [
            "08006", "57P01", "40001", "40P01", "55P03", "57014", "25P03", "25006",
        ] {
            assert!(is_transient_state(&SqlState::from_code(code)), "{code}");
        }
        for code in ["23505", "42P01", "22P02"] {
            assert!(!is_transient_state(&SqlState::from_code(code)), "{code}");
        }
    }

    #[test]
    fn error_messages_do_not_repeat_sources() {
        gang_testkit::assert_sources_not_repeated(&[
            PostgresError::Timeout {
                bound: "statement_timeout",
            },
            PostgresError::Corrupt {
                column: "value",
                detail: "invalid".into(),
            },
            PostgresError::Range {
                column: "value",
                value: u64::MAX,
            },
            PostgresError::Config { detail: "invalid" },
        ]);
    }
}
