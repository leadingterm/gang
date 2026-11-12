//! Public configuration contract of the AWS plugins, read through `serde_json`.

use std::path::Path;
use std::time::Duration;

use gang_aws::{
    AwsConfig, AwsFleetOptions, ClientSettings, CooldownPolicy, ExpiryAge, KmsSignerConfig,
    RetrySpotAfter, S3CacheConfig, SpotFallback, WorkloadTokenAlgorithm,
};
use gang_types::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const REGION: &str = "us-east-1";
const MOUNT: &str = "/mnt/scratch";
const BUCKET: &str = "example-cache";
const KEY_ID: &str = "alias/example-ca";

/// Overlays the keys of `extra` onto `base`, which must be a JSON object.
fn merged(mut base: Value, extra: Value) -> Value {
    if let (Some(object), Value::Object(extra)) = (base.as_object_mut(), extra) {
        object.extend(extra);
    }
    base
}

/// Parses a `[provisioner.aws]` table with the required keys plus `extra`.
fn aws(extra: Value) -> Result<AwsConfig, serde_json::Error> {
    serde_json::from_value(merged(
        json!({"region": REGION, "scratch_mount_point": MOUNT}),
        extra,
    ))
}

/// Parses a `[cache.s3]` table with the required keys plus `extra`.
fn s3(extra: Value) -> Result<S3CacheConfig, serde_json::Error> {
    serde_json::from_value(merged(json!({"region": REGION, "bucket": BUCKET}), extra))
}

/// Parses a `[signer.kms]` table with the required keys plus `extra`.
fn kms(extra: Value) -> Result<KmsSignerConfig, serde_json::Error> {
    serde_json::from_value(merged(json!({"region": REGION, "key_id": KEY_ID}), extra))
}

/// Parses a `[fleets.options]` table exactly as given.
fn fleet(value: Value) -> Result<AwsFleetOptions, serde_json::Error> {
    serde_json::from_value(value)
}

/// Parses a spot fleet whose `fallback` table is `fallback`.
fn spot_fallback(fallback: &Value) -> Result<AwsFleetOptions, serde_json::Error> {
    fleet(json!({"capacity": "spot", "fallback": fallback}))
}

/// Asserts that every value fails to parse with `parse`.
fn assert_all_rejected<T>(
    values: impl IntoIterator<Item = Value>,
    parse: impl Fn(Value) -> Result<T, serde_json::Error>,
) {
    for value in values {
        assert!(parse(value.clone()).is_err(), "accepted {value}");
    }
}

/// Asserts that every value parses with `parse`.
fn assert_all_accepted<T>(
    values: impl IntoIterator<Item = Value>,
    parse: impl Fn(Value) -> Result<T, serde_json::Error>,
) {
    for value in values {
        assert!(parse(value.clone()).is_ok(), "rejected {value}");
    }
}

/// Asserts that serializing `value`, parsing it back and serializing again yields the same JSON.
fn assert_round_trip<T: Serialize + DeserializeOwned>(value: &T) -> TestResult {
    let json = serde_json::to_value(value)?;
    let back: T = serde_json::from_value(json.clone())?;
    assert_eq!(serde_json::to_value(&back)?, json);
    Ok(())
}

/// Every `[provisioner.aws]` key defaults when absent.
#[test]
fn provisioner_defaults_apply_when_optional_keys_are_absent() -> TestResult {
    let config = aws(json!({}))?;
    assert_eq!(config.region.as_str(), REGION);
    assert_eq!(config.scratch_mount_point.as_path(), Path::new(MOUNT));
    assert_eq!(config.workload_token.audience.as_str(), "gang");
    assert_eq!(config.workload_token.ttl.get(), Duration::from_mins(15));
    assert_eq!(
        config.workload_token_algorithm,
        WorkloadTokenAlgorithm::Rs256
    );
    assert_eq!(config.client.request_timeout.get(), Duration::from_secs(10));
    assert_eq!(config.client.max_attempts.get(), 3);
    assert_eq!(
        config.client.connect_timeout.get(),
        Duration::from_millis(3100)
    );
    assert_eq!(
        config.client.retry_initial_backoff.get(),
        Duration::from_secs(1)
    );
    assert_eq!(
        config.client.retry_max_backoff.get(),
        Duration::from_secs(20)
    );
    assert_eq!(config.imds_request_timeout.get(), Duration::from_secs(1));
    assert_eq!(config.fleet_tag_key.as_str(), "gang-fleet");
    assert_eq!(config.set_capacity_cooldown, CooldownPolicy::Ignore);
    Ok(())
}

/// The client settings default when the `client` table is absent or empty.
#[test]
fn client_settings_default_when_table_is_empty() -> TestResult {
    let parsed: ClientSettings = serde_json::from_value(json!({}))?;
    assert_eq!(
        serde_json::to_value(&parsed)?,
        serde_json::to_value(ClientSettings::default())?
    );
    assert_eq!(
        serde_json::to_value(&kms(json!({}))?.client)?,
        serde_json::to_value(ClientSettings::default())?
    );
    Ok(())
}

/// Durations and token lifetimes accept the human duration text.
#[test]
fn provisioner_human_durations_parse() -> TestResult {
    let config = aws(json!({"workload_token": {"ttl": "30m"}, "imds_request_timeout": "2s"}))?;
    assert_eq!(config.workload_token.ttl.get(), Duration::from_mins(30));
    assert_eq!(config.imds_request_timeout.get(), Duration::from_secs(2));
    Ok(())
}

/// The new nested `workload_token` and `client` tables parse.
#[test]
fn provisioner_nested_tables_parse() -> TestResult {
    let config = aws(json!({
        "workload_token": {"audience": "sts.amazonaws.com", "ttl": "1h"},
        "client": {"request_timeout": "10s", "max_attempts": 5},
    }))?;
    assert_eq!(config.workload_token.audience.as_str(), "sts.amazonaws.com");
    assert_eq!(config.workload_token.ttl.get(), Duration::from_hours(1));
    assert_eq!(config.client.max_attempts.get(), 5);
    Ok(())
}

/// Region names must be non-empty and use only lowercase letters, digits and hyphens.
#[test]
fn region_rejects_malformed_names() {
    assert_all_rejected(
        [
            json!({"region": ""}),
            json!({"region": "US-EAST-1"}),
            json!({"region": "us east"}),
            json!({"region": "us_east_1"}),
        ],
        aws,
    );
    assert_all_accepted([json!({"region": "eu-north-1"})], aws);
}

/// The scratch mount point must be absolute and not the filesystem root.
#[test]
fn mount_point_rejects_relative_and_root_paths() {
    assert_all_rejected(
        [
            json!({"scratch_mount_point": "/"}),
            json!({"scratch_mount_point": "mnt/scratch"}),
            json!({"scratch_mount_point": ""}),
        ],
        aws,
    );
    assert_all_accepted([json!({"scratch_mount_point": "/mnt/scratch"})], aws);
}

/// Workload token audiences are 1 to 1000 characters.
#[test]
fn workload_token_audience_bounds() {
    assert_all_rejected(
        [
            json!({"workload_token": {"audience": ""}}),
            json!({"workload_token": {"audience": "a".repeat(1001)}}),
        ],
        aws,
    );
    assert_all_accepted(
        [
            json!({"workload_token": {"audience": "a".repeat(1000)}}),
            json!({"workload_token": {"audience": "sts.amazonaws.com"}}),
        ],
        aws,
    );
}

/// Workload token lifetimes are whole seconds from one minute to one hour.
#[test]
fn workload_token_ttl_bounds() {
    assert_all_rejected(
        [
            json!({"workload_token": {"ttl": "30s"}}),
            json!({"workload_token": {"ttl": "2h"}}),
            json!({"workload_token": {"ttl": "1500ms"}}),
            json!({"workload_token": {"ttl": "90500ms"}}),
        ],
        aws,
    );
    assert_all_accepted(
        [
            json!({"workload_token": {"ttl": "1m"}}),
            json!({"workload_token": {"ttl": "1h"}}),
        ],
        aws,
    );
}

/// The workload token algorithm is an exact, case-sensitive name.
#[test]
fn workload_token_algorithm_names_are_exact() -> TestResult {
    let config = aws(json!({"workload_token_algorithm": "es384"}))?;
    assert_eq!(
        config.workload_token_algorithm,
        WorkloadTokenAlgorithm::Es384
    );
    assert_all_rejected(
        [
            json!({"workload_token_algorithm": "ES384"}),
            json!({"workload_token_algorithm": "RS999"}),
        ],
        aws,
    );
    Ok(())
}

/// Every client timeout and the attempt count must be non-zero, through the shared `ClientSettings`.
#[test]
fn zero_client_timeouts_and_attempts_are_rejected() {
    assert_all_rejected(
        [
            json!({"request_timeout": "0s"}),
            json!({"connect_timeout": "0s"}),
            json!({"retry_initial_backoff": "0s"}),
            json!({"retry_max_backoff": "0s"}),
            json!({"max_attempts": 0}),
        ],
        serde_json::from_value::<ClientSettings>,
    );
    assert!(aws(json!({"imds_request_timeout": "0s"})).is_err());
}

/// The retry backoff ceiling must not be below the initial backoff.
#[test]
fn client_backoff_order_is_checked() {
    assert_all_rejected(
        [json!({"retry_initial_backoff": "30s", "retry_max_backoff": "20s"})],
        serde_json::from_value::<ClientSettings>,
    );
    assert_all_accepted(
        [json!({"retry_initial_backoff": "5s", "retry_max_backoff": "5s"})],
        serde_json::from_value::<ClientSettings>,
    );
    assert!(
        aws(json!({"client": {"retry_initial_backoff": "30s", "retry_max_backoff": "20s"}}))
            .is_err()
    );
}

/// Fleet tag keys follow the tag rules, reject the reserved names and the `aws:` prefix.
#[test]
fn fleet_tag_key_rules() {
    assert_all_rejected(
        [
            json!({"fleet_tag_key": ""}),
            json!({"fleet_tag_key": "a".repeat(129)}),
            json!({"fleet_tag_key": "."}),
            json!({"fleet_tag_key": ".."}),
            json!({"fleet_tag_key": "_index"}),
            json!({"fleet_tag_key": "aws:tag"}),
            json!({"fleet_tag_key": "AWS:tag"}),
            json!({"fleet_tag_key": "Aws:tag"}),
            json!({"fleet_tag_key": "has space"}),
            json!({"fleet_tag_key": "tag/slash"}),
        ],
        aws,
    );
    assert_all_accepted(
        [
            json!({"fleet_tag_key": "gang-fleet"}),
            json!({"fleet_tag_key": "a".repeat(128)}),
            json!({"fleet_tag_key": "team.a_b:c=d+e-f@g,h"}),
        ],
        aws,
    );
}

/// Bucket names are 3 to 63 characters of lowercase letters, digits, dots and hyphens.
#[test]
fn bucket_name_bounds() {
    assert_all_rejected(
        [
            json!({"bucket": "ab"}),
            json!({"bucket": "a".repeat(64)}),
            json!({"bucket": "Example-cache"}),
            json!({"bucket": "bucket_name"}),
        ],
        s3,
    );
    assert_all_accepted(
        [
            json!({"bucket": "abc"}),
            json!({"bucket": "a".repeat(63)}),
            json!({"bucket": "my.bucket-1"}),
        ],
        s3,
    );
}

/// Key prefixes must be relative, so they must not start with `/`.
#[test]
fn key_prefix_rejects_absolute_paths() -> TestResult {
    assert_all_rejected([json!({"key_prefix": "/builds"})], s3);
    assert_eq!(
        s3(json!({"key_prefix": "builds/"}))?.key_prefix.as_str(),
        "builds/"
    );
    Ok(())
}

/// Every `[cache.s3]` key defaults when absent.
#[test]
fn s3_defaults_apply_when_optional_keys_are_absent() -> TestResult {
    let config = s3(json!({}))?;
    assert_eq!(config.region.as_str(), REGION);
    assert_eq!(config.bucket.as_str(), BUCKET);
    assert_eq!(config.key_prefix.as_str(), "");
    assert_eq!(config.tuning.max_retries, 6);
    assert_eq!(config.tuning.retry_delay.get(), Duration::from_millis(300));
    assert_eq!(config.tuning.retry_jitter_percent.get(), 50);
    assert_eq!(config.tuning.multipart_max_concurrent_uploads.get(), 10);
    assert_eq!(config.tuning.retry_buffer_per_request.get(), Bytes::mib(5));
    assert!(config.tuning.consider_expired_after.is_none());
    Ok(())
}

/// Retry jitter is a percentage from 1 to 100.
#[test]
fn retry_jitter_percent_bounds() {
    assert_all_rejected(
        [
            json!({"tuning": {"retry_jitter_percent": 0}}),
            json!({"tuning": {"retry_jitter_percent": 101}}),
        ],
        s3,
    );
    assert_all_accepted(
        [
            json!({"tuning": {"retry_jitter_percent": 1}}),
            json!({"tuning": {"retry_jitter_percent": 100}}),
        ],
        s3,
    );
}

/// The multipart upload concurrency and retry delay must be non-zero.
#[test]
fn multipart_and_retry_delay_must_be_non_zero() {
    assert_all_rejected(
        [
            json!({"tuning": {"multipart_max_concurrent_uploads": 0}}),
            json!({"tuning": {"retry_delay": "0s"}}),
        ],
        s3,
    );
}

/// `consider_expired_after` is whole seconds from one second to `u32::MAX` seconds.
#[test]
fn consider_expired_after_bounds() -> TestResult {
    assert_all_rejected(
        [
            json!({"tuning": {"consider_expired_after": "0s"}}),
            json!({"tuning": {"consider_expired_after": "500ms"}}),
            json!({"tuning": {"consider_expired_after": "1500ms"}}),
            json!({"tuning": {"consider_expired_after": "4294967296s"}}),
            json!({"tuning": {"consider_expired_after": "49711d"}}),
        ],
        s3,
    );
    assert_all_accepted(
        [
            json!({"tuning": {"consider_expired_after": "1s"}}),
            json!({"tuning": {"consider_expired_after": "6d"}}),
            json!({"tuning": {"consider_expired_after": "4294967295s"}}),
        ],
        s3,
    );
    let six_days = s3(json!({"tuning": {"consider_expired_after": "6d"}}))?;
    assert_eq!(
        six_days
            .tuning
            .consider_expired_after
            .map(ExpiryAge::as_secs),
        Some(518_400)
    );
    Ok(())
}

/// Every `[signer.kms]` key defaults when absent, and the key id must be non-empty.
#[test]
fn kms_defaults_and_key_id_rules() -> TestResult {
    let config = kms(json!({}))?;
    assert_eq!(config.key_id.as_str(), KEY_ID);
    assert_eq!(config.region.as_str(), REGION);
    assert_all_rejected([json!({"key_id": ""})], kms);
    Ok(())
}

/// Unknown keys are rejected at every level of the configuration.
#[test]
fn unknown_keys_are_rejected() {
    assert_all_rejected([json!({"unexpected": true})], aws);
    assert_all_rejected([json!({"workload_token": {"unexpected": 1}})], aws);
    assert_all_rejected([json!({"client": {"unexpected": 1}})], aws);
    assert_all_rejected([json!({"unexpected": true})], s3);
    assert_all_rejected([json!({"tuning": {"unexpected": 1}})], s3);
    assert_all_rejected([json!({"unexpected": true})], kms);
    assert_all_rejected([json!({"client": {"unexpected": 1}})], kms);
    assert_all_rejected([json!({"unexpected": "x"})], fleet_from_value);
}

/// Parses a bare fleet options table; used to reach the unknown-key check.
fn fleet_from_value(extra: Value) -> Result<AwsFleetOptions, serde_json::Error> {
    fleet(merged(json!({"capacity": "spot"}), extra))
}

/// The flat keys of the previous layout are rejected in their old places.
#[test]
fn old_flat_keys_are_rejected() {
    assert_all_rejected(
        [
            json!({"request_timeout": "10s"}),
            json!({"max_attempts": 3}),
            json!({"connect_timeout": "3100ms"}),
            json!({"retry_initial_backoff": "1s"}),
            json!({"retry_max_backoff": "20s"}),
            json!({"workload_token_audience": "gang"}),
            json!({"workload_token_ttl": "15m"}),
        ],
        aws,
    );
    assert_all_rejected(
        [
            json!({"request_timeout": "10s"}),
            json!({"max_attempts": 3}),
        ],
        kms,
    );
    assert_all_rejected(
        [
            json!({"max_retries": 6}),
            json!({"retry_delay": "300ms"}),
            json!({"retry_jitter_percent": 50}),
            json!({"multipart_max_concurrent_uploads": 10}),
            json!({"retry_buffer_per_request": "5MiB"}),
            json!({"consider_expired_after": "6d"}),
        ],
        s3,
    );
}

/// The nested layout that replaces the flat keys parses.
#[test]
fn nested_layout_parses() -> TestResult {
    let config = aws(json!({
        "workload_token": {"audience": "gang", "ttl": "15m"},
        "client": {"request_timeout": "10s", "max_attempts": 3},
    }))?;
    assert_eq!(config.client.request_timeout.get(), Duration::from_secs(10));
    let signer = kms(json!({"client": {"max_attempts": 3}}))?;
    assert_eq!(signer.client.max_attempts.get(), 3);
    let cache = s3(json!({"tuning": {"max_retries": 2}}))?;
    assert_eq!(cache.tuning.max_retries, 2);
    Ok(())
}

/// The fleet `machine_class` parses as a `gang_types::MachineClass`: lowercase only, so `Gpu`
/// is rejected and `gpu` is accepted.
#[test]
fn machine_class_rule() {
    assert_all_rejected([json!({"capacity": "spot", "machine_class": "Gpu"})], fleet);
    assert_all_accepted([json!({"capacity": "spot", "machine_class": "gpu"})], fleet);
}

/// Capacity accepts only `spot` and `on-demand`, and is required.
#[test]
fn capacity_values() {
    assert_all_rejected(
        [
            json!({}),
            json!({"capacity": "ondemand"}),
            json!({"capacity": "Spot"}),
            json!({"capacity": "on_demand"}),
            json!({"machine_class": "gpu"}),
        ],
        fleet,
    );
}

/// An unknown fleet option is rejected and the message names the accepted keys.
#[test]
fn unknown_fleet_option_names_the_accepted_keys() -> TestResult {
    let message = match fleet(json!({"capacity": "spot", "zone": "a"})) {
        Ok(_) => return Err("unknown key was accepted".into()),
        Err(error) => error.to_string(),
    };
    assert!(message.contains("zone"), "{message}");
    assert!(message.contains("capacity"), "{message}");
    assert!(message.contains("machine_class"), "{message}");
    assert!(message.contains("fallback"), "{message}");
    Ok(())
}

/// A fleet without `fallback` has no spot fallback.
#[test]
fn fallback_is_absent_unless_configured() -> TestResult {
    assert!(fleet(json!({"capacity": "spot"}))?.fallback.is_none());
    Ok(())
}

/// An empty `fallback` table enables the fallback with its defaults.
#[test]
fn empty_fallback_table_applies_defaults() -> TestResult {
    let fallback = spot_fallback(&json!({}))?.fallback;
    assert_eq!(
        fallback,
        Some(SpotFallback {
            retry_spot_after: RetrySpotAfter::default(),
            drain_on_demand_first: true,
        })
    );
    Ok(())
}

/// A `retry_spot_after` outside one minute to 24 hours fails to parse, and the boundaries parse.
/// `validate_fallback` only checks that the fallback needs a spot fleet.
#[test]
fn fallback_range_and_capacity_are_checked() -> TestResult {
    assert!(spot_fallback(&json!({"retry_spot_after": "30s"})).is_err());
    assert!(spot_fallback(&json!({"retry_spot_after": "25h"})).is_err());
    let minimum = spot_fallback(&json!({"retry_spot_after": "1m"}))?.fallback;
    assert_eq!(
        minimum.map(|fallback| fallback.retry_spot_after.get()),
        Some(Duration::from_mins(1))
    );
    let maximum = spot_fallback(&json!({"retry_spot_after": "24h"}))?.fallback;
    assert_eq!(
        maximum.map(|fallback| fallback.retry_spot_after.get()),
        Some(Duration::from_hours(24))
    );
    assert!(spot_fallback(&json!({})).is_ok_and(|fleet| fleet.validate_fallback().is_ok()));
    let on_demand = fleet(json!({"capacity": "on-demand", "fallback": {}}))?;
    assert!(on_demand.validate_fallback().is_err());
    Ok(())
}

/// Parsed configuration types survive a serialize and parse round trip unchanged.
#[test]
fn configuration_round_trips() -> TestResult {
    assert_round_trip(&aws(json!({}))?)?;
    assert_round_trip(&aws(json!({
        "workload_token": {"audience": "sts.amazonaws.com", "ttl": "30m"},
        "workload_token_algorithm": "es384",
        "client": {
            "request_timeout": "20s",
            "max_attempts": 5,
            "connect_timeout": "2s",
            "retry_initial_backoff": "2s",
            "retry_max_backoff": "40s",
        },
        "imds_request_timeout": "3s",
        "fleet_tag_key": "team:prod",
    }))?)?;
    assert_round_trip(&s3(json!({}))?)?;
    assert_round_trip(&s3(json!({
        "key_prefix": "builds/",
        "tuning": {
            "max_retries": 2,
            "retry_delay": "1s",
            "retry_jitter_percent": 10,
            "multipart_max_concurrent_uploads": 4,
            "retry_buffer_per_request": "8MiB",
            "consider_expired_after": "6d",
        },
    }))?)?;
    assert_round_trip(&kms(json!({}))?)?;
    assert_round_trip(&ClientSettings::default())?;
    assert_round_trip(&fleet(json!({"capacity": "spot", "machine_class": "gpu"}))?)?;
    assert_round_trip(&fleet(
        json!({"capacity": "spot", "fallback": {"retry_spot_after": "2m"}}),
    )?)?;
    Ok(())
}
