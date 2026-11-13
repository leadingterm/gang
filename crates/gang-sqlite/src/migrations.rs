use rusqlite::{Connection, TransactionBehavior};

use crate::errors::SqliteError;

/// Ordered schema migrations. The entry at position `i` brings the schema to `user_version`
/// `i + 1`, so the versions are the positions and cannot skip a number.
///
/// Applied migrations are never edited; a schema change is a new entry at the end.
const MIGRATIONS: &[&str] = &[
    "
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
    cause     TEXT
) STRICT;
",
    "ALTER TABLE scaling_audit ADD COLUMN detail TEXT;",
];

/// Brings the schema up to the newest migration.
///
/// The whole run happens in one immediate transaction that reads `user_version` after the write
/// lock is taken. Two processes opening a fresh database therefore cannot both run the first
/// migration: the second waits, then sees the version the first one committed.
///
/// # Errors
///
/// Returns [`SqliteError::SchemaTooNew`] when the database was written by a newer version of
/// this crate, or the underlying statement error when a migration fails. A failed migration
/// rolls back the whole run.
pub(crate) fn migrate(connection: &mut Connection) -> Result<(), SqliteError> {
    migrate_with(connection, MIGRATIONS)
}

/// Applies the migrations newer than the stored version, in order, in one immediate transaction.
/// Any error drops the transaction, which rolls back every migration in the run.
fn migrate_with(connection: &mut Connection, migrations: &[&str]) -> Result<(), SqliteError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let stored = user_version(&transaction)?;
    let supported = i64::try_from(migrations.len()).unwrap_or(i64::MAX);
    if stored > supported {
        return Err(SqliteError::SchemaTooNew {
            found: stored,
            supported,
        });
    }
    for (version, sql) in (1_i64..).zip(migrations) {
        if version <= stored {
            continue;
        }
        transaction.execute_batch(sql)?;
        transaction.execute_batch(&format!("PRAGMA user_version = {version}"))?;
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

    /// Reads the rows of `sql` as pairs of text columns.
    fn text_pairs(connection: &Connection, sql: &str) -> Vec<(String, String)> {
        let mut statement = connection.prepare(sql).expect("statement prepares");
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query runs")
            .collect::<Result<Vec<_>, _>>()
            .expect("rows read")
    }

    /// Checks every stored schema definition on a freshly migrated database.
    #[test]
    fn fresh_database_schema_matches_expected_ddl() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        migrate(&mut connection).expect("migration succeeds");

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
    cause     TEXT
, detail TEXT) STRICT",
            ),
            ("sqlite_sequence", "CREATE TABLE sqlite_sequence(name,seq)"),
        ];
        assert_eq!(
            schema,
            expected.map(|(name, sql)| (name.to_owned(), sql.to_owned()))
        );
    }

    #[test]
    fn existing_scaling_events_gain_nullable_detail() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        migrate_with(&mut connection, &MIGRATIONS[..1]).expect("initial migration succeeds");
        connection
            .execute(
                "INSERT INTO scaling_audit (at, fleet, kind, subject) VALUES (1000, 'gang-x86', 'terminated', 'local-1')",
                [],
            )
            .expect("row is inserted");

        migrate(&mut connection).expect("upgrade succeeds");

        let detail: Option<String> = connection
            .query_row("SELECT detail FROM scaling_audit", [], |row| row.get(0))
            .expect("existing row is readable");
        assert_eq!(detail, None);
        assert_eq!(user_version(&connection).expect("version is readable"), 2);
    }

    #[test]
    fn a_new_event_kind_name_is_accepted_without_a_migration() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        migrate(&mut connection).expect("migration succeeds");

        connection
            .execute(
                "INSERT INTO scaling_audit (at, fleet, kind) VALUES (1000, 'gang-x86', 'future_kind')",
                [],
            )
            .expect("unknown kind name is accepted by the schema");
    }

    #[test]
    fn schema_newer_than_this_crate_is_rejected() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        let newer = i64::try_from(MIGRATIONS.len() + 1).expect("migration count fits");
        connection
            .execute_batch(&format!("PRAGMA user_version = {newer}"))
            .expect("version is recorded");

        let error = migrate(&mut connection).expect_err("newer schema is rejected");

        assert!(matches!(
            error,
            SqliteError::SchemaTooNew { found, .. } if found == newer
        ));
    }

    #[test]
    fn reopening_an_up_to_date_database_changes_nothing() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        migrate(&mut connection).expect("first migration succeeds");
        connection
            .execute(
                "INSERT INTO scaling_audit (at, fleet, kind) VALUES (1000, 'gang-x86', 'terminated')",
                [],
            )
            .expect("row is inserted");

        migrate(&mut connection).expect("second migration succeeds");

        let rows: i64 = connection
            .query_row("SELECT count(*) FROM scaling_audit", [], |row| row.get(0))
            .expect("count runs");
        assert_eq!(rows, 1);
        assert_eq!(
            user_version(&connection).expect("version is readable"),
            i64::try_from(MIGRATIONS.len()).expect("migration count fits")
        );
    }

    #[test]
    fn failed_migration_rolls_back_the_whole_run() {
        let mut connection = Connection::open_in_memory().expect("in-memory database opens");
        let first = "
CREATE TABLE issued_certificates (serial TEXT NOT NULL, principal TEXT NOT NULL) STRICT;
CREATE TABLE scaling_audit (fleet TEXT NOT NULL, kind TEXT NOT NULL) STRICT;
INSERT INTO issued_certificates (serial, principal) VALUES ('01', 'alice');
INSERT INTO scaling_audit (fleet, kind) VALUES ('gang-x86', 'terminated');
";
        let second = "CREATE TABLE existing (x INTEGER) STRICT;";
        let failing = "
CREATE TABLE scratch (x INTEGER) STRICT;
INSERT INTO scaling_audit (fleet, kind) VALUES ('gang-x86', 'scaled_out');
ALTER TABLE nonexistent ADD COLUMN y TEXT;
";
        migrate_with(&mut connection, &[first, second]).expect("initial migrations succeed");
        let migrations = [first, second, failing];

        migrate_with(&mut connection, &migrations).expect_err("failing migration is rejected");

        assert_eq!(user_version(&connection).expect("version is readable"), 2);
        let scratch: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'scratch'",
                [],
                |row| row.get(0),
            )
            .expect("count runs");
        assert_eq!(scratch, 0);
        assert_eq!(
            text_pairs(
                &connection,
                "SELECT serial, principal FROM issued_certificates"
            ),
            vec![("01".to_owned(), "alice".to_owned())]
        );
        assert_eq!(
            text_pairs(&connection, "SELECT fleet, kind FROM scaling_audit"),
            vec![("gang-x86".to_owned(), "terminated".to_owned())]
        );
    }
}
