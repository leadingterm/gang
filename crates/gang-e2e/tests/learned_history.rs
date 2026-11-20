//! Learned-history persistence across each supported database backend.

mod support {
    pub mod history_contract;
}

mod sqlite {
    use std::error::Error;
    use std::path::Path;

    use gang_spi::{Plugin, PluginContext};
    use gang_sqlite::{Sqlite, SqliteConfig};
    use gang_testkit::Fixture;
    use tempfile::TempDir;

    gang_testkit::database_contract!(@history fresh, crate::support::history_contract);

    fn fresh() -> std::future::Ready<Result<Fixture<Sqlite>, Box<dyn Error>>> {
        std::future::ready(fresh_database())
    }

    fn fresh_database() -> Result<Fixture<Sqlite>, Box<dyn Error>> {
        let dir = TempDir::new()?;
        let context = PluginContext {
            config_path: Path::new("gang.toml"),
        };
        let db = Sqlite::build(SqliteConfig::new(dir.path().join("gang.db")), &context)?;
        Ok(Fixture::keeping(db, dir))
    }
}

mod postgres {
    use std::error::Error;
    use std::num::NonZeroU16;
    use std::path::Path;

    use gang_postgres::{Postgres, PostgresConfig, Tls};
    use gang_spi::{Plugin, PluginContext};
    use gang_testkit::{Fixture, SUPERUSER, shared_postgres};

    gang_testkit::database_contract!(@history fresh, crate::support::history_contract);

    async fn fresh() -> Result<Fixture<Postgres>, Box<dyn Error>> {
        let server = shared_postgres()?;
        let name = server.create_database().await?;
        let host = server
            .socket_dir()
            .to_str()
            .ok_or("socket path is not UTF-8")?;
        let mut config = PostgresConfig::new(host, name, SUPERUSER);
        config.port = NonZeroU16::new(server.port()).ok_or("server port is zero")?;
        config.tls = Tls::Disable;
        let db = Postgres::build(
            config,
            &PluginContext {
                config_path: Path::new("gang.toml"),
            },
        )?;
        Ok(Fixture::keeping(db, server))
    }
}

mod dynamodb {
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    use aws_credential_types::Credentials;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Region};
    use gang_clock::{Clock, Sleep, TestClock, Tick, Timestamp};
    use gang_dynamodb::{DynamoDb, DynamoDbConfig, TableName};
    use gang_testkit::{FakeDynamoDb, Fixture};

    gang_testkit::database_contract!(@history fresh, crate::support::history_contract);

    struct AdvancingClock(TestClock);

    impl Clock for AdvancingClock {
        fn wall(&self) -> Timestamp {
            self.0.wall()
        }
        fn monotonic(&self) -> Tick {
            self.0.monotonic()
        }
        fn sleep_until(&self, deadline: Tick) -> Sleep {
            self.0.sleep_until(deadline)
        }
        fn sleep(&self, duration: Duration) -> Sleep {
            if duration <= Duration::from_secs(2) {
                self.0.advance(duration);
                self.0.sleep(Duration::ZERO)
            } else {
                self.0.sleep(duration)
            }
        }
    }

    async fn fresh() -> Result<Fixture<DynamoDb>, Box<dyn Error>> {
        let fake = FakeDynamoDb::start().await?;
        let endpoint = fake.endpoint();
        let client = aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "gang-test"))
                .http_client(gang_aws_sdk::http_client())
                .endpoint_url(endpoint.as_str())
                .build(),
        );
        let config = DynamoDbConfig {
            table: TableName::try_new("gang-test")?,
            region: gang_aws_sdk::Region::try_new("us-east-1")?,
            endpoint: Some(endpoint),
            client: gang_aws_sdk::ClientSettings::default(),
        };
        let database = DynamoDb::with_client(
            config,
            client,
            Arc::new(AdvancingClock(TestClock::default())),
        );
        Ok(Fixture::keeping(database, fake))
    }
}
