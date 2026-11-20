use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::{BehaviorVersion, Region};
use gang_clock::{Clock, Sleep, TestClock, Tick, Timestamp};
use gang_spi::{
    Acquisition, CandidateGeneration, CandidatePriority, ControllerHostIdentity, ControllerId,
    ExpiringRecordPort, LeaderLeasePort, Plugin, PluginContext, PortError, RecordId, RecordKey,
    RecordKind, RecordVersion, Renewal, ScalingAuditEvent, ScalingEventKind, SchemaVersion,
    StateRepositoryPort, Takeover,
};
use gang_testkit::{FakeDynamoDb, Fixture, TestResult};
use gang_types::{FleetName, Identity, InstanceId, Principal, ProviderName, WorkerId};

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
    let candidate = database
        .register_candidate(
            &ControllerId::new("a"),
            &ControllerHostIdentity::Persistent(InstanceId::try_new("host-a")?),
        )
        .await?;
    assert!(
        database
            .insert(&[(key, b"body")], RecordVersion::new(1), now, too_large)
            .await
            .is_err()
    );
    assert!(
        database
            .acquire(&candidate, now, now, too_large, Takeover::Observe)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn legacy_take_invalidates_priority_binding() -> TestResult {
    let (database, _fake) = setup().await?;
    let candidate = database
        .register_candidate(
            &ControllerId::new("candidate"),
            &ControllerHostIdentity::Persistent(InstanceId::try_new("host-candidate")?),
        )
        .await?;
    let now = Timestamp::from_unix_millis(1);
    let expiry = Timestamp::from_unix_millis(100);
    let Acquisition::Acquired(old) = database
        .acquire(&candidate, now, now, expiry, Takeover::Observe)
        .await?
    else {
        return Err("candidate did not acquire lease".into());
    };
    let stored = database
        .get("lease", "leader")
        .await?
        .ok_or("acquired lease missing")?;
    assert_eq!(
        crate::items::string(&stored, "holder_priority")?,
        candidate.priority.as_str()
    );
    assert_eq!(
        crate::items::number(&stored, "holder_priority_epoch")?,
        old.epoch.get()
    );
    assert!(!stored.contains_key("holder_generation"));
    assert!(!stored.contains_key("holder_generation_epoch"));
    database
        .client()
        .await
        .update_item()
        .table_name(database.table())
        .set_key(Some(crate::items::key("lease", "leader")))
        .update_expression(
            "SET #holder = :holder, #expires = :expires, #priority = :priority ADD #epoch :one",
        )
        .expression_attribute_names("#holder", "holder")
        .expression_attribute_names("#expires", "expires_at")
        .expression_attribute_names("#epoch", "epoch")
        .expression_attribute_names("#priority", "holder_priority")
        .expression_attribute_values(":holder", crate::items::s("legacy"))
        .expression_attribute_values(":expires", crate::items::n(100))
        .expression_attribute_values(":one", crate::items::n(1))
        .expression_attribute_values(":priority", crate::items::s("unbound-invalid-priority"))
        .send()
        .await?;
    let Acquisition::Contending(legacy) = database
        .acquire(&candidate, now, now, expiry, Takeover::Request)
        .await?
    else {
        return Err("legacy holder was not recognized as lower priority".into());
    };
    assert_ne!(legacy.epoch, old.epoch);
    assert_eq!(legacy.holder, ControllerId::new("legacy"));
    let stored = database
        .get("lease", "leader")
        .await?
        .ok_or("requested lease missing")?;
    assert_eq!(
        crate::items::string(&stored, "successor_priority")?,
        candidate.priority.as_str()
    );
    assert!(!stored.contains_key("successor_generation"));
    assert!(matches!(
        database.renew(&legacy, now, Timestamp::from_unix_millis(150)).await?,
        Renewal::YieldRequested(updated) if updated.expires_at == Timestamp::from_unix_millis(150)
    ));
    Ok(())
}

#[tokio::test]
async fn handoff_resets_record_sequence_only_when_a_new_epoch_is_acquired() -> TestResult {
    let (database, _fake) = setup().await?;
    let owner = database
        .register_candidate(
            &ControllerId::new("owner"),
            &ControllerHostIdentity::Persistent(InstanceId::try_new("owner-host")?),
        )
        .await?;
    let replacement = database
        .register_candidate(
            &ControllerId::new("replacement"),
            &ControllerHostIdentity::Persistent(InstanceId::try_new("replacement-host")?),
        )
        .await?;
    let now = Timestamp::from_unix_millis(1);
    let expiry = Timestamp::from_unix_millis(100);
    let Acquisition::Acquired(lease) = database
        .acquire(&owner, now, now, expiry, Takeover::Observe)
        .await?
    else {
        return Err("owner did not acquire lease".into());
    };
    database
        .client()
        .await
        .update_item()
        .table_name(database.table())
        .set_key(Some(crate::items::key("lease", "leader")))
        .update_expression("SET #sequence = :sequence")
        .expression_attribute_names("#sequence", "record_sequence")
        .expression_attribute_values(":sequence", crate::items::n(7))
        .send()
        .await?;
    assert!(matches!(
        database
            .acquire(&replacement, now, now, expiry, Takeover::Request)
            .await?,
        Acquisition::Contending(_)
    ));
    assert!(matches!(
        database.renew(&lease, now, expiry).await?,
        Renewal::YieldRequested(_)
    ));
    let pending = database
        .get("lease", "leader")
        .await?
        .ok_or("pending lease missing")?;
    assert_eq!(crate::items::number(&pending, "record_sequence")?, 7);
    database.release(&lease).await?;
    let Acquisition::Acquired(next) = database
        .acquire(&replacement, now, now, expiry, Takeover::Observe)
        .await?
    else {
        return Err("replacement did not acquire reserved lease".into());
    };
    assert!(next.epoch > lease.epoch);
    let stored = database
        .get("lease", "leader")
        .await?
        .ok_or("replacement lease missing")?;
    assert_eq!(crate::items::number(&stored, "record_sequence")?, 0);
    assert_eq!(
        crate::items::string(&stored, "holder_priority")?,
        replacement.priority.as_str()
    );
    Ok(())
}

#[tokio::test]
async fn candidate_counter_and_lease_epoch_do_not_wrap() -> TestResult {
    let (database, _fake) = setup().await?;
    database.ensure_schema().await?;
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(crate::items::key("counter", "lease_candidate")))
        .item("value", crate::items::n(u64::MAX))
        .send()
        .await?;
    assert!(matches!(
        database
            .register_candidate(
                &ControllerId::new("overflow"),
                &ControllerHostIdentity::Persistent(InstanceId::try_new("host-overflow")?),
            )
            .await,
        Err(PortError::Backend(error))
            if matches!(
                error.downcast_ref::<crate::DynamoError>(),
                Some(crate::DynamoError::CounterExhausted { counter: "candidate generation" })
            )
    ));
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(crate::items::key("lease", "leader")))
        .item("epoch", crate::items::n(u64::MAX))
        .send()
        .await?;
    let candidate = gang_spi::LeaseCandidate {
        holder: ControllerId::new("candidate"),
        priority: CandidatePriority::registered(CandidateGeneration::new(1)),
    };
    let now = Timestamp::from_unix_millis(1);
    assert!(matches!(
        database
            .acquire(&candidate, now, now, now, Takeover::Observe)
            .await,
        Err(PortError::Backend(error))
            if matches!(
                error.downcast_ref::<crate::DynamoError>(),
                Some(crate::DynamoError::CounterExhausted { counter: "lease epoch" })
            )
    ));
    Ok(())
}

#[tokio::test]
async fn mapped_host_registration_survives_an_exhausted_counter() -> TestResult {
    let (database, _fake) = setup().await?;
    let host = InstanceId::try_new("stable-host")?;
    database.ensure_schema().await?;
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(crate::items::key("controller_host", host.as_str())))
        .item("generation", crate::items::n(7))
        .send()
        .await?;
    database
        .client()
        .await
        .put_item()
        .table_name(database.table())
        .set_item(Some(crate::items::key("counter", "lease_candidate")))
        .item("value", crate::items::n(u64::MAX))
        .send()
        .await?;
    let identity = ControllerHostIdentity::Persistent(host);
    let first = database
        .register_candidate(&ControllerId::new("first"), &identity)
        .await?;
    let retry = database
        .register_candidate(&ControllerId::new("retry"), &identity)
        .await?;
    assert_eq!(
        first.priority,
        CandidatePriority::registered(CandidateGeneration::new(7))
    );
    assert_eq!(retry.priority, first.priority);
    Ok(())
}

#[tokio::test]
async fn launched_registration_reads_database_without_allocating_a_counter() -> TestResult {
    let (database, fake) = setup().await?;
    database.ensure_schema().await?;
    let host = ControllerHostIdentity::Launched {
        at: Timestamp::from_unix_millis(42),
        instance: InstanceId::try_new("instance-a")?,
    };
    let before = fake.requests();
    let candidate = database
        .register_candidate(&ControllerId::new("first"), &host)
        .await?;
    assert_eq!(fake.requests() - before, 1);
    assert_eq!(
        candidate.priority,
        CandidatePriority::launched(
            Timestamp::from_unix_millis(42),
            &InstanceId::try_new("instance-a")?,
        )
    );
    assert!(database.get("counter", "lease_candidate").await?.is_none());
    fake.set_unavailable(true);
    assert!(
        database
            .register_candidate(&ControllerId::new("restart"), &host)
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
