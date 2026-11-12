use std::path::PathBuf;

/// Why the `SQLite` state store failed.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum SqliteError {
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
    Query(#[from] rusqlite::Error),
    /// The database thread cannot serve statements, so the work could not run or return.
    #[error("the sqlite database thread cannot serve statements")]
    Closed,
    /// The database was written by a newer version of this crate than the one running.
    #[error("schema version {found} is newer than the supported version {supported}")]
    SchemaTooNew {
        /// The version stored in the database.
        found: i64,
        /// The newest version this crate knows.
        supported: i64,
    },
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

impl From<tokio_rusqlite::Error<SqliteError>> for SqliteError {
    fn from(error: tokio_rusqlite::Error<SqliteError>) -> Self {
        match error {
            tokio_rusqlite::Error::Close((_, source)) => Self::Query(source),
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
    fn no_variant_repeats_its_source() {
        let errors = [
            SqliteError::Open {
                path: "state.db".into(),
                source: rusqlite::Error::InvalidQuery,
            },
            SqliteError::Query(rusqlite::Error::InvalidQuery),
            SqliteError::Closed,
            SqliteError::SchemaTooNew {
                found: 2,
                supported: 1,
            },
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
