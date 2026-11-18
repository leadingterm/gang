//! Runs the shared database contract against isolated `PostgreSQL` databases.

mod support;

use std::error::Error;

use gang_postgres::Postgres;
use gang_spi::{SchemaVersion, StateRepositoryPort};
use gang_testkit::{Fixture, shared_postgres};

gang_testkit::database_contract!(fresh, schema: gang_postgres::SCHEMA_VERSION, stamped);

async fn fresh() -> Result<Fixture<Postgres>, Box<dyn Error>> {
    let server = shared_postgres()?;
    let config = support::database(&server).await?;
    let db = support::build(config)?;
    Ok(Fixture::keeping(db, server))
}

async fn stamped(stored: SchemaVersion) -> Result<Fixture<Postgres>, Box<dyn Error>> {
    let server = shared_postgres()?;
    let config = support::database(&server).await?;
    let db = support::build(config.clone())?;
    db.revocation_epoch().await?;
    let client = support::raw(&server, &config.database).await?;
    client
        .execute(
            "UPDATE schema_version SET current = $1, oldest_compatible = $2",
            &[
                &i32::try_from(stored.current)?,
                &i32::try_from(stored.oldest_compatible)?,
            ],
        )
        .await?;
    drop(db);
    let db = support::build(config)?;
    Ok(Fixture::keeping(db, server))
}
