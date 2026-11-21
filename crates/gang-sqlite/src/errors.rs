use std::path::PathBuf;

/// Why the `SQLite` state store failed.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum SqliteError {
    /// Another process holds the database lock.
    #[error("database {} is in use by another process", path.display())]
    #[failure(config)]
    Locked {
        /// The configured database file.
        path: PathBuf,
    },
    /// The database lock file could not be opened or locked.
    #[error(transparent)]
    Lock(#[from] gang_os::PathIo),
    /// The database path could not be resolved.
    #[error("cannot resolve sqlite database path {}", path.display())]
    #[failure(config)]
    ResolvePath {
        /// The configured database path.
        path: PathBuf,
        /// The filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// The database file could not be opened.
    #[error("cannot open sqlite database {}", path.display())]
    Open {
        /// The configured database file.
        path: PathBuf,
        /// The driver's cause.
        #[source]
        source: rusqlite::Error,
    },
    /// A statement failed.
    #[error("sqlite statement failed")]
    Query(rusqlite::Error),
    /// A statement could not acquire a `SQLite` lock.
    #[error("sqlite database is busy")]
    #[failure(unavailable)]
    Busy(#[source] rusqlite::Error),
    /// The database thread cannot serve statements, so the work could not run or return.
    #[error("the sqlite database thread cannot serve statements")]
    #[failure(unavailable)]
    Closed,
    /// The stored schema differs from the supported version.
    #[error("database schema cannot be used")]
    Schema(#[from] gang_spi::SchemaMismatch),
    /// A timestamp, in unix milliseconds, does not fit in a stored integer.
    #[error("timestamp of {0} ms does not fit in the database")]
    TimestampRange(u64),
    /// A scaling sequence number does not fit in a stored integer.
    #[error("scaling sequence {0} does not fit in the database")]
    SequenceRange(u64),
    /// A stored value is not valid for its column.
    #[error("stored {column} is corrupt: {detail}")]
    Corrupt {
        /// The column the value was read from.
        column: &'static str,
        /// What is wrong with the value.
        detail: String,
    },
}

impl From<rusqlite::Error> for SqliteError {
    fn from(error: rusqlite::Error) -> Self {
        if matches!(
            &error,
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked,
                    ..
                },
                _
            )
        ) {
            Self::Busy(error)
        } else {
            Self::Query(error)
        }
    }
}

impl From<tokio_rusqlite::Error<SqliteError>> for SqliteError {
    fn from(error: tokio_rusqlite::Error<SqliteError>) -> Self {
        match error {
            tokio_rusqlite::Error::Close((_, source)) => Self::from(source),
            tokio_rusqlite::Error::Error(inner) => inner,
            // Covers ConnectionClosed and any failure variant added to the non-exhaustive enum:
            // an unknown failure of the database thread means it is unusable.
            _ => Self::Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use gang_testkit::assert_sources_not_repeated;

    use super::SqliteError;

    #[test]
    fn closed_and_lock_contention_are_unavailable() {
        assert!(matches!(
            gang_spi::PortError::from(SqliteError::Closed),
            gang_spi::PortError::Unavailable(_)
        ));
        for code in [rusqlite::ffi::SQLITE_BUSY, rusqlite::ffi::SQLITE_LOCKED] {
            let driver = rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None);
            let error = SqliteError::from(driver);
            assert!(matches!(
                gang_spi::PortError::from(error),
                gang_spi::PortError::Unavailable(_)
            ));
        }
        let driver = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
            None,
        );
        assert!(matches!(
            gang_spi::PortError::from(SqliteError::from(driver)),
            gang_spi::PortError::Backend(_)
        ));
        let schema = SqliteError::Schema(gang_spi::SchemaMismatch {
            stored: 2,
            supported: 1,
        });
        assert!(matches!(
            gang_spi::PortError::from(schema),
            gang_spi::PortError::Backend(_)
        ));
    }

    #[test]
    fn no_variant_repeats_its_source() {
        let errors = [
            SqliteError::Open {
                path: "state.db".into(),
                source: rusqlite::Error::InvalidQuery,
            },
            SqliteError::Query(rusqlite::Error::InvalidQuery),
            SqliteError::Closed,
            SqliteError::Schema(gang_spi::SchemaMismatch {
                stored: 2,
                supported: 1,
            }),
            SqliteError::TimestampRange(u64::MAX),
            SqliteError::SequenceRange(u64::MAX),
            SqliteError::Corrupt {
                column: "value",
                detail: "invalid".to_owned(),
            },
        ];

        assert_sources_not_repeated(&errors);
    }
}
