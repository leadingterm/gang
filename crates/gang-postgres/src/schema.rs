use gang_spi::SchemaMismatch;
use tokio_postgres::types::Type;

use crate::codec::{corrupt, get};
use crate::errors::PostgresError;

/// Current schema version.
pub const SCHEMA_VERSION: u32 = 4;

const SCHEMA: &str = r#"
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
    detail        bytea
);
CREATE TABLE counters (name text PRIMARY KEY, value bigint NOT NULL);
INSERT INTO counters (name, value) VALUES ('audit', 0), ('revocation', 0), ('candidate', 0);
CREATE TABLE leases (
    name        text PRIMARY KEY,
    holder      text,
    epoch       bigint NOT NULL,
    safe_yield_epoch bigint,
    expires_at  timestamptz NOT NULL,
    record_sequence bigint NOT NULL DEFAULT 0,
    holder_priority text,
    holder_priority_epoch bigint NOT NULL DEFAULT 0,
    pending_holder text,
    pending_priority text,
    pending_epoch bigint,
    pending_expires_at timestamptz
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
CREATE TABLE controller_hosts (
    host       text COLLATE "C" PRIMARY KEY,
    generation bigint NOT NULL
);
"#;

/// Key of the transaction-scoped advisory lock serialising schema changes across controllers.
/// The lock ends with the transaction, so a pooler may hand the connection on.
const SCHEMA_LOCK: i64 = 0x6761_6e67_7363_6d61;

/// Initializes an empty database or checks its version within the caller's timeout-bound transaction.
///
/// Typed unnamed statements complete in one round trip without session state, allowing
/// transaction-mode poolers to hand connections to other clients.
pub(crate) async fn initialize(
    transaction: &deadpool_postgres::Transaction<'_>,
) -> Result<(), PostgresError> {
    transaction
        .execute_typed(
            "SELECT pg_advisory_xact_lock($1)",
            &[(&SCHEMA_LOCK, Type::INT8)],
        )
        .await?;
    transaction
        .batch_execute("CREATE TABLE IF NOT EXISTS schema_version (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), current integer NOT NULL, oldest_compatible integer NOT NULL)")
        .await?;
    let stored = transaction
        .query_typed_opt("SELECT current, oldest_compatible FROM schema_version", &[])
        .await?
        .map(|row| -> Result<u32, PostgresError> {
            let current = u32::try_from(get::<i32>(&row, "current")?)
                .map_err(|error| corrupt("schema_version", error))?;
            let oldest = u32::try_from(get::<i32>(&row, "oldest_compatible")?)
                .map_err(|error| corrupt("schema_version", error))?;
            if current == 0 || oldest == 0 || oldest > current {
                return Err(corrupt(
                    "schema_version",
                    format!("invalid version {current}, oldest compatible {oldest}"),
                ));
            }
            Ok(current)
        })
        .transpose()?;

    if let Some(stored) = stored {
        if stored != SCHEMA_VERSION {
            return Err(SchemaMismatch {
                stored,
                supported: SCHEMA_VERSION,
            }
            .into());
        }
        // Version 4 remains readable by older controllers; their writes leave this
        // capability marker absent or bound to a previous epoch.
        transaction
            .batch_execute("ALTER TABLE leases ADD COLUMN IF NOT EXISTS safe_yield_epoch bigint")
            .await?;
    } else {
        let row = transaction.query_typed_one(
            "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND relkind IN ('r', 'p', 'v', 'm', 'S', 'f') AND relname <> 'schema_version')",
            &[],
        ).await?;
        if row.get::<_, bool>(0) {
            return Err(SchemaMismatch {
                stored: 0,
                supported: SCHEMA_VERSION,
            }
            .into());
        }
        transaction.batch_execute(SCHEMA).await?;
        transaction
            .execute_typed(
                "INSERT INTO schema_version (current, oldest_compatible) VALUES (4, 2)",
                &[],
            )
            .await?;
    }
    Ok(())
}
