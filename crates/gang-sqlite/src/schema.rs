use gang_spi::SchemaMismatch;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use crate::errors::SqliteError;

/// Current schema version.
pub const SCHEMA_VERSION: u32 = 3;

const SCHEMA: &str = "
CREATE TABLE issued_certificates (
    serial      TEXT    NOT NULL PRIMARY KEY,
    provider    TEXT    NOT NULL,
    principal   TEXT    NOT NULL,
    issued_at   INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    revoked_at  INTEGER
) STRICT;
CREATE INDEX issued_certificates_revoked ON issued_certificates (revoked_at, serial) WHERE revoked_at IS NOT NULL;
CREATE INDEX issued_certificates_identity ON issued_certificates (provider, principal, issued_at, serial) WHERE revoked_at IS NULL;
CREATE TABLE principals (
    provider    TEXT    NOT NULL,
    principal   TEXT    NOT NULL,
    first_seen  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL,
    revoked_at  INTEGER,
    PRIMARY KEY (provider, principal)
) STRICT;
CREATE INDEX principals_revoked ON principals (revoked_at, provider, principal) WHERE revoked_at IS NOT NULL;
CREATE TABLE scaling_audit (
    id        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    at        INTEGER NOT NULL,
    fleet     TEXT    NOT NULL,
    kind      TEXT    NOT NULL,
    from_capacity INTEGER,
    to_capacity   INTEGER,
    subject   TEXT,
    cause     TEXT,
    detail    TEXT
) STRICT;
CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL) STRICT;
CREATE TABLE expiring_records (kind TEXT NOT NULL, id BLOB NOT NULL, body BLOB NOT NULL, version INTEGER NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (kind, id)) STRICT;
";

/// Initializes an empty database or opens the exact supported schema.
///
/// The immediate transaction serializes initialization and rolls back on failure.
///
/// # Errors
///
/// Returns a schema mismatch, corruption, or a statement error.
pub(crate) fn initialize(connection: &mut Connection) -> Result<(), SqliteError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current =
        u32::try_from(user_version(&transaction)?).map_err(|error| SqliteError::Corrupt {
            column: "user_version",
            detail: error.to_string(),
        })?;
    if current == 0 {
        let nonempty: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%')",
            [],
            |row| row.get(0),
        )?;
        if nonempty {
            return Err(SchemaMismatch {
                stored: current,
                supported: SCHEMA_VERSION,
            }
            .into());
        }
        transaction.execute_batch(SCHEMA)?;
        transaction.execute_batch(
            "CREATE TABLE schema_compatibility (oldest_compatible INTEGER NOT NULL) STRICT;
             INSERT INTO schema_compatibility (oldest_compatible) VALUES (1);
             PRAGMA user_version = 3;",
        )?;
    } else {
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_compatibility (oldest_compatible INTEGER NOT NULL) STRICT",
        )?;
        let oldest: Option<i64> = transaction
            .query_row(
                "SELECT oldest_compatible FROM schema_compatibility LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(oldest) = oldest {
            let oldest = u32::try_from(oldest).map_err(|error| SqliteError::Corrupt {
                column: "schema_compatibility",
                detail: error.to_string(),
            })?;
            if oldest == 0 || oldest > current {
                return Err(SqliteError::Corrupt {
                    column: "schema_compatibility",
                    detail: format!(
                        "invalid oldest compatible version {oldest} for version {current}"
                    ),
                });
            }
        }
        if current != SCHEMA_VERSION {
            return Err(SchemaMismatch {
                stored: current,
                supported: SCHEMA_VERSION,
            }
            .into());
        }
    }
    transaction.commit()?;
    Ok(())
}

/// Reads `PRAGMA user_version`.
fn user_version(connection: &Connection) -> Result<i64, SqliteError> {
    Ok(connection.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_database_records_schema_compatibility() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        initialize(&mut connection).expect("initialization succeeds");
        assert_eq!(user_version(&connection).expect("version is readable"), 3);
        let oldest: u32 = connection
            .query_row(
                "SELECT oldest_compatible FROM schema_compatibility",
                [],
                |row| row.get(0),
            )
            .expect("compatibility is recorded");
        assert_eq!(oldest, 1);
    }

    /// Checks every stored schema definition on a freshly initialized database.
    #[test]
    fn fresh_database_schema_matches_expected_ddl() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        initialize(&mut connection).expect("initialization succeeds");

        let mut statement = connection
            .prepare("SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")
            .expect("schema query prepares");
        let schema = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("schema query runs")
            .collect::<Result<Vec<_>, _>>()
            .expect("schema rows read");
        let expected = [
            (
                "counters",
                "CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL) STRICT",
            ),
            (
                "expiring_records",
                "CREATE TABLE expiring_records (kind TEXT NOT NULL, id BLOB NOT NULL, body BLOB NOT NULL, version INTEGER NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (kind, id)) STRICT",
            ),
            (
                "issued_certificates",
                "CREATE TABLE issued_certificates (
    serial      TEXT    NOT NULL PRIMARY KEY,
    provider    TEXT    NOT NULL,
    principal   TEXT    NOT NULL,
    issued_at   INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    revoked_at  INTEGER
) STRICT",
            ),
            (
                "issued_certificates_identity",
                "CREATE INDEX issued_certificates_identity ON issued_certificates (provider, principal, issued_at, serial) WHERE revoked_at IS NULL",
            ),
            (
                "issued_certificates_revoked",
                "CREATE INDEX issued_certificates_revoked ON issued_certificates (revoked_at, serial) WHERE revoked_at IS NOT NULL",
            ),
            (
                "principals",
                "CREATE TABLE principals (
    provider    TEXT    NOT NULL,
    principal   TEXT    NOT NULL,
    first_seen  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL,
    revoked_at  INTEGER,
    PRIMARY KEY (provider, principal)
) STRICT",
            ),
            (
                "principals_revoked",
                "CREATE INDEX principals_revoked ON principals (revoked_at, provider, principal) WHERE revoked_at IS NOT NULL",
            ),
            (
                "scaling_audit",
                "CREATE TABLE scaling_audit (
    id        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    at        INTEGER NOT NULL,
    fleet     TEXT    NOT NULL,
    kind      TEXT    NOT NULL,
    from_capacity INTEGER,
    to_capacity   INTEGER,
    subject   TEXT,
    cause     TEXT,
    detail    TEXT
) STRICT",
            ),
            (
                "schema_compatibility",
                "CREATE TABLE schema_compatibility (oldest_compatible INTEGER NOT NULL) STRICT",
            ),
            ("sqlite_sequence", "CREATE TABLE sqlite_sequence(name,seq)"),
        ];
        assert_eq!(
            schema,
            expected.map(|(name, sql)| (name.to_owned(), sql.to_owned()))
        );
    }

    #[test]
    fn a_new_event_kind_name_is_accepted() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        initialize(&mut connection).expect("initialization succeeds");

        connection
            .execute(
                "INSERT INTO scaling_audit (at, fleet, kind) VALUES (1000, 'gang-x86', 'future_kind')",
                [],
            )
            .expect("unknown kind name is accepted by the schema");
    }

    #[test]
    fn different_schema_is_rejected() {
        for stored in [1, 2, 4] {
            let mut connection = Connection::open_in_memory().expect("database opens");
            connection
                .execute_batch(&format!("PRAGMA user_version = {stored}"))
                .expect("version recorded");
            assert!(
                matches!(initialize(&mut connection), Err(SqliteError::Schema(SchemaMismatch {
                stored: actual, supported: SCHEMA_VERSION
            })) if actual == stored)
            );
        }
    }

    #[test]
    fn invalid_schema_compatibility_is_rejected() {
        for oldest in [0, 4, -1] {
            let mut connection = Connection::open_in_memory().expect("in-memory database opens");
            initialize(&mut connection).expect("initialization succeeds");
            connection
                .execute(
                    "UPDATE schema_compatibility SET oldest_compatible = ?1",
                    [oldest],
                )
                .expect("invalid compatibility is recorded");
            assert!(matches!(
                initialize(&mut connection),
                Err(SqliteError::Corrupt {
                    column: "schema_compatibility",
                    ..
                })
            ));
        }
    }

    #[test]
    fn current_schema_without_compatibility_row_is_accepted() {
        let mut connection = Connection::open_in_memory().expect("database opens");
        initialize(&mut connection).expect("initializes");
        connection
            .execute("DELETE FROM schema_compatibility", [])
            .expect("row removed");
        initialize(&mut connection).expect("current schema accepted");
        let count: i64 = connection
            .query_row("SELECT count(*) FROM schema_compatibility", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 0);
    }

    #[test]
    fn reopening_an_up_to_date_database_changes_nothing() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        initialize(&mut connection).expect("initialization succeeds");
        connection
            .execute(
                "INSERT INTO scaling_audit (at, fleet, kind) VALUES (1000, 'gang-x86', 'terminated')",
                [],
            )
            .expect("row is inserted");

        initialize(&mut connection).expect("reopening succeeds");

        let rows: i64 = connection
            .query_row("SELECT count(*) FROM scaling_audit", [], |row| row.get(0))
            .expect("count runs");
        assert_eq!(rows, 1);
        assert_eq!(
            user_version(&connection).expect("version is readable"),
            i64::from(SCHEMA_VERSION)
        );
    }

    #[test]
    fn nonempty_unversioned_database_is_untouched() {
        let mut connection = Connection::open_in_memory().expect("database opens");
        connection
            .execute_batch("CREATE TABLE existing (x INTEGER); INSERT INTO existing VALUES (7)")
            .expect("setup");
        assert!(matches!(
            initialize(&mut connection),
            Err(SqliteError::Schema(SchemaMismatch {
                stored: 0,
                supported: SCHEMA_VERSION
            }))
        ));
        let value: i64 = connection
            .query_row("SELECT x FROM existing", [], |row| row.get(0))
            .expect("data retained");
        assert_eq!(value, 7);
        assert_eq!(user_version(&connection).expect("version"), 0);
    }

    #[test]
    fn failed_initialization_rolls_back_schema_and_version() {
        let mut connection = Connection::open_in_memory().expect("database opens");
        connection
            .execute_batch("PRAGMA max_page_count = 1")
            .expect("page limit set");
        initialize(&mut connection).expect_err("initialization exceeds page limit");
        assert_eq!(user_version(&connection).expect("version"), 0);
        let count: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .expect("schema count");
        assert_eq!(count, 0);
    }

    #[test]
    fn concurrent_initialization_is_serialized() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("schema.db");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let mut connection = Connection::open(path).expect("database opens");
                    connection
                        .busy_timeout(std::time::Duration::from_secs(10))
                        .expect("timeout set");
                    barrier.wait();
                    initialize(&mut connection).expect("initialization succeeds");
                    assert_eq!(
                        user_version(&connection).expect("version"),
                        i64::from(SCHEMA_VERSION)
                    );
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("initializer finished");
        }
    }
}
