use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::{BehaviorVersion, Region};
use gang_clock::{Clock, Sleep, TestClock, Tick, Timestamp};
use gang_spi::{
    ControllerId, ExpiringRecordPort, LeaderLeasePort, Plugin, PluginContext, PortError, RecordId,
    RecordKey, RecordKind, RecordVersion, ScalingAuditEvent, ScalingEventKind, SchemaVersion,
    StateRepositoryPort,
};
use gang_testkit::{FakeDynamoDb, Fixture, TestResult};
use gang_types::{FleetName, Identity, Principal, ProviderName, WorkerId};

use crate::{DynamoDb, DynamoDbConfig, TableName};

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

async fn setup() -> Result<(DynamoDb, FakeDynamoDb), Box<dyn std::error::Error>> {
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
    Ok((
        DynamoDb::with_client(
            config,
            client,
            Arc::new(AdvancingClock(TestClock::default())),
        ),
        fake,
    ))
}

async fn fresh() -> Result<Fixture<DynamoDb>, Box<dyn std::error::Error>> {
    let (database, fake) = setup().await?;
    Ok(Fixture::keeping(database, fake))
}

async fn stamped(stored: SchemaVersion) -> Result<Fixture<DynamoDb>, Box<dyn std::error::Error>> {
    let (database, fake) = setup().await?;
    let mut item = crate::items::key("meta", "schema");
    item.insert("shape".into(), crate::items::n(stored.current));
    item.insert(
        "oldest_compatible".into(),
        crate::items::n(stored.oldest_compatible),
    );
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(item))
        .send()
        .await?;
    Ok(Fixture::keeping(database, fake))
}

gang_testkit::database_contract!(fresh, schema: crate::SCHEMA_VERSION, stamped);

#[tokio::test]
async fn legacy_current_schema_is_used() -> TestResult {
    let (database, _fake) = setup().await?;
    let mut item = crate::items::key("meta", "schema");
    item.insert("shape".into(), crate::items::n(1));
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(item.clone()))
        .send()
        .await?;
    database.ensure_schema().await?;
    let stored = database
        .client()
        .await
        .get_item()
        .table_name(database.table())
        .set_key(Some(crate::items::key("meta", "schema")))
        .consistent_read(true)
        .send()
        .await?;
    assert_eq!(stored.item, Some(item));
    Ok(())
}

#[tokio::test]
async fn compatible_newer_schema_is_used_without_writing() -> TestResult {
    let (database, _fake) = setup().await?;
    let mut item = crate::items::key("meta", "schema");
    item.insert("shape".into(), crate::items::n(2));
    item.insert("oldest_compatible".into(), crate::items::n(1));
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(item.clone()))
        .send()
        .await?;
    database.ensure_schema().await?;
    let stored = database
        .client()
        .await
        .get_item()
        .table_name(database.table())
        .set_key(Some(crate::items::key("meta", "schema")))
        .consistent_read(true)
        .send()
        .await?;
    assert_eq!(stored.item, Some(item));
    Ok(())
}

#[tokio::test]
async fn legacy_newer_schema_is_rejected() -> TestResult {
    let (database, _fake) = setup().await?;
    let mut item = crate::items::key("meta", "schema");
    item.insert("shape".into(), crate::items::n(2));
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(item))
        .send()
        .await?;
    assert!(matches!(
        database.ensure_schema().await,
        Err(crate::DynamoError::Schema(gang_spi::SchemaTooNew {
            stored: SchemaVersion {
                current: 2,
                oldest_compatible: 2
            },
            supported: 1,
        }))
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_stored_schema_is_rejected_without_writing() -> TestResult {
    for (shape, oldest) in [(0, None), (0, Some(0)), (2, Some(0)), (1, Some(2))] {
        let (database, _fake) = setup().await?;
        let mut item = crate::items::key("meta", "schema");
        item.insert("shape".into(), crate::items::n(shape));
        if let Some(oldest) = oldest {
            item.insert("oldest_compatible".into(), crate::items::n(oldest));
        }
        database
            .client()
            .await
            .put_item()
            .table_name(database.table())
            .set_item(Some(item.clone()))
            .send()
            .await?;
        assert!(matches!(
            database.ensure_schema().await,
            Err(crate::DynamoError::Malformed { attribute: "shape" })
        ));
        let stored = database
            .client()
            .await
            .get_item()
            .table_name(database.table())
            .set_key(Some(crate::items::key("meta", "schema")))
            .consistent_read(true)
            .send()
            .await?;
        assert_eq!(stored.item, Some(item));
    }
    Ok(())
}

#[tokio::test]
async fn revoked_principal_paging_avoids_scanning_active_principals() -> TestResult {
    let (database, fake) = setup().await?;
    let provider = ProviderName::try_new("github")?;
    for index in 0..20 {
        let identity = Identity::new(
            provider.clone(),
            Principal::try_new(format!("active-{index:02}"))?,
        );
        database
            .observe_principal(&identity, Timestamp::from_unix_millis(1))
            .await?;
    }
    let revoked = Identity::new(provider, Principal::try_new("revoked")?);
    database
        .revoke_principal(&revoked, Timestamp::from_unix_millis(2))
        .await?;
    let before = fake.queries();
    let mut records = Vec::new();
    let limit = NonZeroUsize::new(1).ok_or("nonzero limit")?;
    assert!(!database.principals(None, limit, true, &mut records).await?);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].identity, revoked);
    assert_eq!(
        fake.queries() - before,
        1,
        "active principals must not force extra queries"
    );
    Ok(())
}

#[tokio::test]
async fn transaction_conflict_is_retried() -> TestResult {
    let (database, fake) = setup().await?;
    database.ensure_schema().await?;
    fake.cancel_next_transaction(&["TransactionConflict"]);
    let key = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([7; 32]),
    };
    let result = database
        .insert(
            &[(key, b"body")],
            RecordVersion::new(1),
            Timestamp::from_unix_millis(1),
            Timestamp::from_unix_millis(10),
        )
        .await;
    assert!(
        matches!(result, Ok(gang_spi::Insert::Inserted)),
        "{result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn persistent_item_conflicts_exhaust_attempts() -> TestResult {
    let (database, fake) = setup().await?;
    database.ensure_schema().await?;
    let key = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([8; 32]),
    };
    fake.conflict_transactions_on(
        &crate::items::record_pk(key.kind),
        &crate::items::record_sk(&key.id),
        3,
    );
    let before = fake.requests();
    let result = database
        .insert(
            &[(key, b"body")],
            RecordVersion::new(1),
            Timestamp::from_unix_millis(1),
            Timestamp::from_unix_millis(10),
        )
        .await;
    assert!(matches!(result, Err(PortError::Unavailable(_))));
    assert_eq!(fake.requests() - before, 3);
    Ok(())
}
#[tokio::test]
async fn shared_contracts_survive_item_conflicts() -> TestResult {
    let (database, fake) = setup().await?;
    database.ensure_schema().await?;
    fake.conflict_transactions_on(
        &crate::items::record_pk(RecordKind::LoginAttempt),
        &crate::items::record_sk(&RecordId::new([1; 32])),
        1,
    );
    Box::pin(gang_testkit::expiring_contract::record_concurrent_insert(
        &database,
    ))
    .await?;
    fake.conflict_transactions_on("counter", "audit", 1);
    Box::pin(gang_testkit::repository_contract::concurrent_scaling_events_are_ordered(&database))
        .await?;
    Ok(())
}

#[tokio::test]
async fn transaction_cancellation_reasons_preserve_error_class() -> TestResult {
    let (database, fake) = setup().await?;
    database.ensure_schema().await?;
    let key = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([1; 32]),
    };
    let other = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([2; 32]),
    };
    for (index, code) in [
        "TransactionConflict",
        "ThrottlingError",
        "ProvisionedThroughputExceeded",
    ]
    .into_iter()
    .enumerate()
    {
        let key = RecordKey {
            kind: RecordKind::LoginAttempt,
            id: RecordId::new([u8::try_from(index)? + 3; 32]),
        };
        fake.cancel_next_transaction(&[code]);
        let result = database
            .insert(
                &[(key, b"body")],
                RecordVersion::new(1),
                Timestamp::from_unix_millis(1),
                Timestamp::from_unix_millis(10),
            )
            .await;
        assert!(
            matches!(result, Ok(gang_spi::Insert::Inserted)),
            "{code}: {result:?}"
        );
    }
    for code in ["ValidationError", "ItemCollectionSizeLimitExceeded"] {
        fake.cancel_next_transaction(&["ConditionalCheckFailed", code]);
        let result = database
            .insert(
                &[(key, b"body"), (other, b"body")],
                RecordVersion::new(1),
                Timestamp::from_unix_millis(1),
                Timestamp::from_unix_millis(10),
            )
            .await;
        assert!(
            matches!(result, Err(PortError::Backend(_))),
            "{code}: {result:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn audit_counter_zero_can_advance() -> TestResult {
    let (database, fake) = setup().await?;
    database.ensure_schema().await?;
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(crate::items::key("counter", "audit")))
        .item("value", crate::items::n(0))
        .send()
        .await?;
    fake.fail_after_requests(fake.requests() + 6);
    let event = ScalingAuditEvent {
        at: Timestamp::from_unix_millis(1),
        fleet: FleetName::try_new("fleet-a")?,
        kind: ScalingEventKind::WorkerRegistered {
            worker: WorkerId::try_new("worker")?,
        },
        cause: None,
    };
    assert_eq!(database.record_scaling_event(&event).await?.get(), 1);
    Ok(())
}

#[tokio::test]
async fn audit_sequence_collision_does_not_overwrite_event() -> TestResult {
    let (database, _fake) = setup().await?;
    let event = ScalingAuditEvent {
        at: Timestamp::from_unix_millis(1),
        fleet: FleetName::try_new("fleet-a")?,
        kind: ScalingEventKind::WorkerRegistered {
            worker: WorkerId::try_new("worker")?,
        },
        cause: None,
    };
    let seq = database.record_scaling_event(&event).await?;
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(crate::items::key("counter", "audit")))
        .item("value", crate::items::n(0))
        .send()
        .await?;
    let mut replacement = event.clone();
    replacement.at = Timestamp::from_unix_millis(2);
    assert!(database.record_scaling_event(&replacement).await.is_err());
    let mut events = Vec::new();
    database
        .scaling_events(
            gang_spi::ScalingEventSeq::default(),
            None,
            NonZeroUsize::MIN,
            &mut events,
        )
        .await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, seq);
    assert_eq!(events[0].event, event);
    Ok(())
}

#[tokio::test]
async fn lease_and_expiring_timestamps_fit_shared_range() -> TestResult {
    let (database, _fake) = setup().await?;
    let too_large = Timestamp::from_unix_millis(u64::MAX);
    let now = Timestamp::from_unix_millis(1);
    let key = RecordKey {
        kind: RecordKind::LoginAttempt,
        id: RecordId::new([9; 32]),
    };
    assert!(
        database
            .insert(&[(key, b"body")], RecordVersion::new(1), now, too_large)
            .await
            .is_err()
    );
    assert!(
        database
            .acquire(&ControllerId::new("a"), now, too_large)
            .await
            .is_err()
    );
    Ok(())
}

#[test]
fn endpoint_rejects_non_http_schemes() -> TestResult {
    let config = DynamoDbConfig {
        table: TableName::try_new("gang-test")?,
        region: gang_aws_sdk::Region::try_new("us-east-1")?,
        endpoint: Some("ftp://example.com".parse()?),
        client: gang_aws_sdk::ClientSettings::default(),
    };
    let context = PluginContext {
        config_path: Path::new("gang.toml"),
    };
    assert!(matches!(
        DynamoDb::build(config, &context),
        Err(PortError::Config(_))
    ));
    Ok(())
}

#[tokio::test]
async fn aws_attempt_timeout_is_shorter_than_operation_timeout() -> TestResult {
    let region = gang_aws_sdk::Region::try_new("us-east-1")?;
    let settings = gang_aws_sdk::ClientSettings::default();
    let sdk = gang_aws_sdk::loader(&region, &settings)
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .load()
        .await;
    let timeout = sdk.timeout_config().ok_or("missing timeout config")?;
    let attempt = timeout
        .operation_attempt_timeout()
        .ok_or("missing attempt timeout")?;
    assert!(attempt < settings.request_timeout.get());
    Ok(())
}
#[tokio::test]
async fn single_attempt_uses_the_full_request_timeout() -> TestResult {
    let region = gang_aws_sdk::Region::try_new("us-east-1")?;
    let settings = gang_aws_sdk::ClientSettings {
        max_attempts: std::num::NonZeroU32::MIN,
        ..gang_aws_sdk::ClientSettings::default()
    };
    let sdk = gang_aws_sdk::loader(&region, &settings)
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .load()
        .await;
    let timeout = sdk.timeout_config().ok_or("missing timeout config")?;
    assert_eq!(timeout.operation_attempt_timeout(), None);
    Ok(())
}

#[test]
fn table_names_validate() {
    assert!(TableName::try_new("a_-.Z9").is_ok());
    assert!(TableName::try_new("ab").is_err());
    assert!(TableName::try_new("a".repeat(255)).is_ok());
    assert!(TableName::try_new("a".repeat(256)).is_err());
    assert!(TableName::try_new("bad/name").is_err());
}
