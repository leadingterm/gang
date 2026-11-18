use gang_spi::{SchemaPlan, SchemaVersion};
use tokio_postgres::types::Type;

use crate::codec::{corrupt, get};
use crate::errors::PostgresError;

/// The schema version produced by this binary and the oldest binary it remains compatible with.
pub const SCHEMA_VERSION: SchemaVersion = SchemaVersion {
    current: 2,
    oldest_compatible: 2,
};

/// Ordered schema migrations: entry `i` brings the schema to version `i + 1`.
///
/// Applied migrations are never edited; a schema change is a new entry. Advance
/// `oldest_compatible` whenever an older binary cannot read the new representation.
/// Version 2 rejects version 1 readers.
const MIGRATIONS: &[&str] = &[
    r#"
CREATE TABLE issued_certificates (
    serial      text COLLATE "C" PRIMARY KEY,
    provider    text COLLATE "C" NOT NULL,
    principal   text COLLATE "C" NOT NULL,
    issued_at   timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL,
    revoked_at  timestamptz
);
CREATE INDEX issued_certificates_revoked ON issued_certificates (revoked_at, serial) WHERE revoked_at IS NOT NULL;
CREATE INDEX issued_certificates_identity ON issued_certificates (provider, principal, issued_at, serial) WHERE revoked_at IS NULL;
CREATE TABLE principals (
    provider    text COLLATE "C" NOT NULL,
    principal   text COLLATE "C" NOT NULL,
    first_seen  timestamptz NOT NULL,
    last_seen   timestamptz NOT NULL,
    revoked_at  timestamptz,
    PRIMARY KEY (provider, principal)
);
CREATE INDEX principals_revoked ON principals (revoked_at, provider, principal) WHERE revoked_at IS NOT NULL;
CREATE TABLE scaling_audit (
    sequence      bigint PRIMARY KEY,
    at            timestamptz NOT NULL,
    fleet         text NOT NULL,
    kind          text NOT NULL,
    from_capacity bigint,
    to_capacity   bigint,
    subject       text,
    cause         text,
    detail        text
);
CREATE TABLE counters (name text PRIMARY KEY, value bigint NOT NULL);
INSERT INTO counters (name, value) VALUES ('audit', 0), ('revocation', 0);
CREATE TABLE leases (
    name        text PRIMARY KEY,
    holder      text,
    epoch       bigint NOT NULL,
    expires_at  timestamptz NOT NULL
);
CREATE TABLE expiring_records (
    kind        text NOT NULL,
    id          bytea NOT NULL,
    body        bytea NOT NULL,
    version     bigint NOT NULL,
    expires_at  timestamptz NOT NULL,
    PRIMARY KEY (kind, id)
);
CREATE INDEX expiring_records_expiry ON expiring_records (expires_at);
"#,
    "ALTER TABLE scaling_audit ALTER COLUMN detail TYPE bytea USING convert_to(detail, 'UTF8');",
];

/// Key of the transaction-scoped advisory lock serialising schema changes across controllers.
/// The lock ends with the transaction, so a pooler may hand the connection on.
const MIGRATION_LOCK: i64 = 0x6761_6e67_7363_6d61;

/// Brings the schema up to date within the caller's timeout-bound transaction.
///
/// Typed unnamed statements complete in one round trip without session state, allowing
/// transaction-mode poolers to hand connections to other clients.
pub(crate) async fn migrate(
    transaction: &deadpool_postgres::Transaction<'_>,
) -> Result<(), PostgresError> {
    transaction
        .execute_typed(
            "SELECT pg_advisory_xact_lock($1)",
            &[(&MIGRATION_LOCK, Type::INT8)],
        )
        .await?;
    transaction
        .batch_execute("CREATE TABLE IF NOT EXISTS schema_version (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), current integer NOT NULL, oldest_compatible integer NOT NULL)")
        .await?;
    let stored = transaction
        .query_typed_opt("SELECT current, oldest_compatible FROM schema_version", &[])
        .await?
        .map(|row| -> Result<SchemaVersion, PostgresError> {
            let current: i32 = get(&row, "current")?;
            let oldest_compatible: i32 = get(&row, "oldest_compatible")?;
            let version = SchemaVersion {
                current: u32::try_from(current)
                    .map_err(|error| corrupt("schema_version", error))?,
                oldest_compatible: u32::try_from(oldest_compatible)
                    .map_err(|error| corrupt("schema_version", error))?,
            };
            if !version.is_valid() {
                return Err(corrupt(
                    "schema_version",
                    format!("invalid version {version:?}"),
                ));
            }
            Ok(version)
        })
        .transpose()?;

    match SCHEMA_VERSION.plan(stored)? {
        SchemaPlan::Use => {}
        SchemaPlan::Migrate { from } => {
            let from = usize::try_from(from).map_err(|_| PostgresError::Range {
                column: "current",
                value: u64::from(from),
            })?;
            for migration in MIGRATIONS.iter().skip(from) {
                transaction.batch_execute(migration).await?;
            }
            let current =
                i32::try_from(SCHEMA_VERSION.current).map_err(|_| PostgresError::Range {
                    column: "current",
                    value: u64::from(SCHEMA_VERSION.current),
                })?;
            let oldest_compatible =
                i32::try_from(SCHEMA_VERSION.oldest_compatible).map_err(|_| {
                    PostgresError::Range {
                        column: "oldest_compatible",
                        value: u64::from(SCHEMA_VERSION.oldest_compatible),
                    }
                })?;
            transaction
                .execute_typed(
                    "INSERT INTO schema_version (current, oldest_compatible) VALUES ($1, $2) ON CONFLICT (singleton) DO UPDATE SET current = EXCLUDED.current, oldest_compatible = EXCLUDED.oldest_compatible",
                    &[(&current, Type::INT4), (&oldest_compatible, Type::INT4)],
                )
                .await?;
        }
    }
    Ok(())
}
