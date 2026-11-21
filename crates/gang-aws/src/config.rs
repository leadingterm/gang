//! Settings for the AWS plugins, read from the `[provisioner.aws]`, `[cache.s3]` and
//! `[signer.kms]` tables of the gang config.
//!
//! Every struct rejects unknown keys. Each tunable has a default, applied when its key is
//! absent. The types of the fields enforce their bounds, so a config value that parses is valid.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_types::{
    Audience, BoundedDuration, FleetName, MachineClass, NANOS_PER_SECOND, NonZeroBytes,
    NonZeroDuration, Percent, Url,
};
use serde::{Deserialize, Serialize};

use crate::names::{BucketName, FleetTagKey, KeyId, KeyPrefix, MountPoint};
use gang_aws_sdk::{ClientSettings, Region};

/// Suffix that names the on-demand twin of an opted-in fleet's spot group.
pub(crate) const ON_DEMAND_SUFFIX: &str = "-on-demand";

/// Default interval between shared supply group snapshots.
pub(crate) const DEFAULT_SUPPLY_POLL_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(1);
const DEFAULT_AUTOSCALING_REQUEST_INTERVAL: NonZeroDuration = NonZeroDuration::from_millis(500);
const DEFAULT_ACTIVITY_POLL_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(60);

/// Settings for the `Aws` provisioner plugin, read from the `[provisioner.aws]` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsConfig {
    /// AWS region of the Auto Scaling groups, EC2 instances, STS and IMDS calls. Required:
    /// non-empty, only `a-z`, `0-9` and `-`.
    pub region: Region,
    /// Absolute path where the root service has mounted the scratch volume. Required. Must not
    /// be `/`. The plugin adopts the volume and never formats or mounts it.
    pub scratch_mount_point: MountPoint,
    /// Audience and lifetime requested for workload tokens. Defaults to audience `gang` and
    /// 15 minutes. The audience must be 1 to 1000 characters. The lifetime must be a whole
    /// number of seconds within `1m` to `1h`.
    #[serde(default)]
    pub workload_token: WorkloadTokenConfig,
    /// Signing algorithm requested for workload tokens. Default `rs256`; `es384` is the other
    /// choice.
    #[serde(default)]
    pub workload_token_algorithm: WorkloadTokenAlgorithm,
    /// Timeouts, attempts and retry backoff of each AWS operation, shared by the provisioner and
    /// the KMS signer. Defaults to 10 seconds per operation, 3 attempts, 3100 milliseconds to
    /// connect, and backoff from one to 20 seconds. See [`ClientSettings`] for each field.
    #[serde(default)]
    pub client: ClientSettings,
    /// Minimum interval between Auto Scaling HTTP attempts, including SDK retries. Default
    /// `500ms`.
    #[serde(default = "default_autoscaling_request_interval")]
    pub autoscaling_request_interval: NonZeroDuration,
    /// Interval between shared batched supply group snapshots. Mutations read fresh state.
    /// Default `1s`.
    #[serde(default = "default_supply_poll_interval")]
    pub supply_poll_interval: NonZeroDuration,
    /// Minimum interval between eligible queue-less history polls, including failures. Default
    /// `60s`. Settled, unchanged groups with acknowledged history need no periodic poll.
    #[serde(default = "default_activity_poll_interval")]
    pub activity_poll_interval: NonZeroDuration,
    /// Bound on each instance metadata (IMDS) request. Default `1s`. Must be greater than zero.
    #[serde(default = "default_imds_request_timeout")]
    pub imds_request_timeout: NonZeroDuration,
    /// Interval between controller retirement metadata reads. Default `5s`.
    #[serde(default = "default_retirement_poll_interval")]
    pub retirement_poll_interval: NonZeroDuration,
    /// Key of the EC2 instance tag, readable through the instance metadata service, that names
    /// the node's fleet. Defaults to `gang-fleet`. Must be 1 to 128 characters of `a-z`, `A-Z`, `0-9` and
    /// `_ . : = + - @ ,`, and must not be `.`, `..` or `_index`, which the instance metadata
    /// service reserves.
    #[serde(default)]
    pub fleet_tag_key: FleetTagKey,
    /// Whether `SetDesiredCapacity` waits for the Auto Scaling group's cooldown. Default
    /// `ignore`, which applies the capacity change at once; `honor` waits for the cooldown.
    #[serde(default)]
    pub set_capacity_cooldown: CooldownPolicy,
    /// Optional SQS queue carrying `EventBridge` infrastructure observations.
    #[serde(default)]
    pub events_queue_url: Option<Url>,
    /// Maximum number of messages received at once, from 1 to 10.
    #[serde(
        default = "default_event_batch_limit",
        deserialize_with = "deserialize_event_batch_limit"
    )]
    pub event_batch_limit: u8,
    /// How long received messages remain invisible, in whole seconds up to 12 hours.
    #[serde(
        default = "default_event_visibility_timeout",
        deserialize_with = "deserialize_event_visibility_timeout"
    )]
    pub event_visibility_timeout: NonZeroDuration,
    /// Age of scaling activities considered for infrastructure observations.
    #[serde(default = "default_activity_history_window")]
    pub activity_history_window: NonZeroDuration,
    /// Maximum scaling activities inspected per group, from 1 to 1000.
    #[serde(
        default = "default_activity_limit",
        deserialize_with = "deserialize_activity_limit"
    )]
    pub activity_limit: u32,
}

const fn default_autoscaling_request_interval() -> NonZeroDuration {
    DEFAULT_AUTOSCALING_REQUEST_INTERVAL
}
const fn default_supply_poll_interval() -> NonZeroDuration {
    DEFAULT_SUPPLY_POLL_INTERVAL
}
const fn default_activity_poll_interval() -> NonZeroDuration {
    DEFAULT_ACTIVITY_POLL_INTERVAL
}

const fn default_event_batch_limit() -> u8 {
    10
}
const fn default_activity_limit() -> u32 {
    100
}
const fn default_event_visibility_timeout() -> NonZeroDuration {
    NonZeroDuration::from_secs(30)
}
const fn default_activity_history_window() -> NonZeroDuration {
    NonZeroDuration::from_mins(15)
}

fn deserialize_event_batch_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u8, D::Error> {
    let value = u8::deserialize(deserializer)?;
    if (1..=10).contains(&value) {
        Ok(value)
    } else {
        Err(serde::de::Error::custom(
            "event_batch_limit must be between 1 and 10",
        ))
    }
}

fn deserialize_activity_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u32, D::Error> {
    let value = u32::deserialize(deserializer)?;
    if (1..=1000).contains(&value) {
        Ok(value)
    } else {
        Err(serde::de::Error::custom(
            "activity_limit must be between 1 and 1000",
        ))
    }
}

fn deserialize_event_visibility_timeout<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<NonZeroDuration, D::Error> {
    let value = NonZeroDuration::deserialize(deserializer)?;
    if value.get().subsec_nanos() == 0 && value.get().as_secs() <= 43_200 {
        Ok(value)
    } else {
        Err(serde::de::Error::custom(
            "event_visibility_timeout must be whole seconds up to 12 hours",
        ))
    }
}

/// An STS workload token lifetime: whole seconds from one minute to one hour inclusive.
///
/// `GetWebIdentityToken` requires at least 60 seconds, independently of Gang's client-token bounds.
pub type WorkloadTokenTtl = BoundedDuration<NANOS_PER_SECOND, 60, 3600>;

/// Audience and lifetime requested from STS for a worker's workload token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkloadTokenConfig {
    /// The audience the token is issued for.
    pub audience: Audience,
    /// The lifetime accepted by STS: whole seconds from one minute to one hour inclusive.
    pub ttl: WorkloadTokenTtl,
}

impl Default for WorkloadTokenConfig {
    fn default() -> Self {
        Self {
            audience: Audience::DEFAULT,
            ttl: WorkloadTokenTtl::from_units(900),
        }
    }
}

gang_types::string_enum! {
    /// Signing algorithm for workload tokens, written as `rs256` or `es384` in the config.
    #[derive(Default)]
    pub enum WorkloadTokenAlgorithm("workload token algorithm") {
        /// RSASSA-PKCS1-v1_5 with SHA-256. The default.
        #[default]
        Rs256 = "rs256",
        /// ECDSA with P-384 and SHA-384.
        Es384 = "es384",
    }
}

impl WorkloadTokenAlgorithm {
    /// The algorithm name as STS expects it, such as RS256.
    #[must_use]
    pub const fn sts_name(self) -> &'static str {
        match self {
            Self::Rs256 => "RS256",
            Self::Es384 => "ES384",
        }
    }
}

gang_types::string_enum! {
    /// Whether `SetDesiredCapacity` waits for the Auto Scaling group's cooldown. Written as `honor`
    /// or `ignore`.
    #[derive(Default)]
    pub enum CooldownPolicy("cooldown policy") {
        /// Apply the capacity change at once. The default.
        #[default]
        Ignore = "ignore",
        /// Wait for the Auto Scaling group's cooldown to end before the capacity change applies.
        Honor = "honor",
    }
}

impl CooldownPolicy {
    /// Whether the policy waits for the cooldown.
    pub(crate) const fn honors(self) -> bool {
        matches!(self, Self::Honor)
    }
}

gang_types::string_enum! {
    /// Whether a fleet buys spot or on-demand capacity. Written as `spot` or `on-demand`.
    pub enum Capacity("capacity") {
        /// Interruptible capacity, bought at the spot price.
        Spot = "spot",
        /// Capacity at the on-demand price, which AWS does not reclaim.
        OnDemand = "on-demand",
    }
}

/// The `[fleets.options]` table of the `Aws` provisioner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, gang_spi::FleetOptions)]
#[serde(deny_unknown_fields)]
#[fleet_options(validate = AwsFleetOptions::validate_fallback)]
#[fleet_options(validate_fleets = AwsFleetOptions::validate_twins)]
pub struct AwsFleetOptions {
    /// `spot` or `on-demand`. Actions request it with the `capacity` property.
    pub capacity: Capacity,
    /// Optional qualitative class, for example `gpu`. Actions request it with the
    /// `machine_class` property.
    pub machine_class: Option<MachineClass>,
    /// Presence opts a spot fleet into on-demand fallback: when Spot launches fail, new capacity
    /// is bought on-demand for `retry_spot_after`. Requires `capacity = "spot"`. Launch-only: it
    /// is not a worker property.
    #[option(launch)]
    pub fallback: Option<SpotFallback>,
}

/// Delay before on-demand fallback retries Spot, read from `retry_spot_after`. Must be a whole
/// number of milliseconds from one minute to 24 hours, inclusive. Written as a duration such as
/// `5m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NonZeroDuration", into = "NonZeroDuration")]
pub struct RetrySpotAfter(NonZeroDuration);

impl RetrySpotAfter {
    /// Shortest accepted delay, one minute.
    pub const MIN: Self = Self(NonZeroDuration::from_mins(1));
    /// Longest accepted delay, 24 hours.
    pub const MAX: Self = Self(NonZeroDuration::from_hours(24));

    /// Wraps `value` if it is a whole number of milliseconds within [`Self::MIN`] and
    /// [`Self::MAX`], inclusive.
    ///
    /// # Errors
    ///
    /// Returns [`RetrySpotAfterError::OutOfRange`] when `value` lies outside those bounds.
    pub fn try_new(value: Duration) -> Result<Self, RetrySpotAfterError> {
        match NonZeroDuration::try_new(value) {
            Ok(delay) if (Self::MIN.get()..=Self::MAX.get()).contains(&value) => Ok(Self(delay)),
            _ => Err(RetrySpotAfterError::OutOfRange {
                min: Self::MIN.get(),
                max: Self::MAX.get(),
            }),
        }
    }

    /// Returns the delay.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0.get()
    }
}

impl Default for RetrySpotAfter {
    fn default() -> Self {
        Self(NonZeroDuration::from_mins(5))
    }
}

impl TryFrom<NonZeroDuration> for RetrySpotAfter {
    type Error = RetrySpotAfterError;

    fn try_from(value: NonZeroDuration) -> Result<Self, Self::Error> {
        Self::try_new(value.get())
    }
}

impl From<RetrySpotAfter> for NonZeroDuration {
    fn from(value: RetrySpotAfter) -> Self {
        value.0
    }
}

/// Why a duration is not a valid [`RetrySpotAfter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RetrySpotAfterError {
    /// The duration lies outside the accepted range.
    #[error(
        "retry_spot_after must be a whole number of milliseconds between {} and {}",
        gang_types::duration::display(*.min),
        gang_types::duration::display(*.max)
    )]
    OutOfRange {
        /// Shortest accepted duration.
        min: Duration,
        /// Longest accepted duration.
        max: Duration,
    },
}

/// Opt-in on-demand fallback for a spot fleet, read from `[fleets.options.fallback]`. Every key
/// has a default, so an empty table enables the fallback with the defaults. Rejects unknown keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotFallback {
    /// How long after the latest failed Spot launch new capacity is bought on-demand before
    /// Spot is tried again. Defaults to five minutes. Must be within `1m` to `24h`.
    pub retry_spot_after: RetrySpotAfter,
    /// Whether on-demand instances are released before Spot instances when the fleet has
    /// surplus capacity. Defaults to `true`.
    pub drain_on_demand_first: bool,
}

impl Default for SpotFallback {
    fn default() -> Self {
        Self {
            retry_spot_after: RetrySpotAfter::default(),
            drain_on_demand_first: true,
        }
    }
}

/// Settings for the `S3Cache` plugin, read from the `[cache.s3]` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3CacheConfig {
    /// AWS region of the bucket. Required: non-empty, only `a-z`, `0-9` and `-`.
    pub region: Region,
    /// Name of the bucket that holds cache objects. Required. Must be 3 to 63 characters of
    /// `a-z`, `0-9`, `.` and `-`.
    pub bucket: BucketName,
    /// HTTP(S) S3-compatible service endpoint. Unset by default, using the AWS regional endpoint.
    #[serde(default, deserialize_with = "deserialize_s3_endpoint")]
    pub endpoint: Option<Url>,
    /// Use path-style addressing, needed by many S3-compatible services. Defaults to false.
    #[serde(default)]
    pub force_path_style: bool,
    /// Prefix prepended to every object key. Defaults to an empty string. Must not start with `/`.
    #[serde(default)]
    pub key_prefix: KeyPrefix,
    /// Retry, multipart upload and expiry tuning. Defaults to 6 retries, a 300 millisecond
    /// retry delay, 50% jitter, 10 concurrent uploads, a 5 MiB retry buffer and no expiry.
    /// See [`S3Tuning`] for each field.
    #[serde(default)]
    pub tuning: S3Tuning,
    /// How often the leader asks `CloudWatch` for the bucket's daily storage metrics. Defaults to
    /// `1h`. Must be at least `1h`. See [`UsageInterval`].
    #[serde(default)]
    pub usage_interval: UsageInterval,
    /// Timeouts, attempts and retry backoff of each `CloudWatch` request. Defaults to 10 seconds
    /// per operation, 3 attempts, 3100 milliseconds to connect, and backoff from one to 20
    /// seconds. See [`ClientSettings`] for each field.
    #[serde(default)]
    pub client: ClientSettings,
}

/// Parses the optional endpoint and rejects schemes the S3 HTTP client cannot use.
fn deserialize_s3_endpoint<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Url>, D::Error> {
    let endpoint = Option::<Url>::deserialize(deserializer)?;
    if endpoint
        .as_ref()
        .is_some_and(|url| !matches!(url.scheme(), "http" | "https"))
    {
        return Err(serde::de::Error::custom(
            "S3 endpoint must use http or https",
        ));
    }
    Ok(endpoint)
}

/// Tuning of the `S3Cache` plugin's retries, multipart uploads and expiry, read from
/// `[cache.s3.tuning]`. Every key has a default. Rejects unknown keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Tuning {
    /// Retries after a failed request. Defaults to 6.
    pub max_retries: u32,
    /// Base delay between retries. Defaults to 300 milliseconds.
    pub retry_delay: NonZeroDuration,
    /// Random jitter applied to each retry delay, as a percentage of the delay. Defaults to 50%.
    /// Must be 1 to `100`.
    pub retry_jitter_percent: Percent,
    /// Parts of one multipart upload sent in parallel. Defaults to 10. Must be at least 1.
    pub multipart_max_concurrent_uploads: NonZeroU32,
    /// Bytes buffered per request so that a failed request can be retried. Defaults to 5 MiB.
    pub retry_buffer_per_request: NonZeroBytes,
    /// Age after which a cached object is treated as expired. Unset, which disables the check,
    /// by default. When set, it must be a whole number of seconds, at most `4294967295`.
    pub consider_expired_after: Option<ExpiryAge>,
}

impl Default for S3Tuning {
    fn default() -> Self {
        Self {
            max_retries: 6,
            retry_delay: NonZeroDuration::from_millis(300),
            retry_jitter_percent: Percent::from_percent(50),
            multipart_max_concurrent_uploads: NonZeroU32::MIN.saturating_add(9),
            retry_buffer_per_request: NonZeroBytes::mib(5),
            consider_expired_after: None,
        }
    }
}

/// Age of a cached object after which it is treated as expired, in whole seconds.
/// The maximum supported age is `u32::MAX` seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NonZeroDuration", into = "NonZeroDuration")]
pub struct ExpiryAge(NonZeroU32);

impl ExpiryAge {
    /// Returns the age in whole seconds.
    #[must_use]
    pub const fn as_secs(self) -> u32 {
        self.0.get()
    }
}

impl TryFrom<NonZeroDuration> for ExpiryAge {
    type Error = ExpiryAgeError;

    fn try_from(value: NonZeroDuration) -> Result<Self, Self::Error> {
        let duration = value.get();
        if duration.subsec_nanos() != 0 {
            return Err(ExpiryAgeError::SubSecond);
        }
        u32::try_from(duration.as_secs())
            .ok()
            .and_then(NonZeroU32::new)
            .map(Self)
            .ok_or(ExpiryAgeError::TooLong)
    }
}

impl From<ExpiryAge> for NonZeroDuration {
    fn from(age: ExpiryAge) -> Self {
        Self::from_secs(u64::from(age.as_secs()))
    }
}

/// Why a duration is not a valid [`ExpiryAge`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExpiryAgeError {
    /// The duration has a part smaller than one second.
    #[error("consider_expired_after must be a whole number of seconds")]
    SubSecond,
    /// The duration is longer than `u32::MAX` seconds.
    #[error("consider_expired_after must be at most 4294967295 seconds")]
    TooLong,
}

/// How often the leader asks `CloudWatch` for the bucket's storage metrics, read from
/// `usage_interval`. `CloudWatch` reports them once a day, so the interval is at least one hour.
/// Defaults to one hour. Written as a duration such as `6h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NonZeroDuration", into = "NonZeroDuration")]
pub struct UsageInterval(NonZeroDuration);

impl UsageInterval {
    /// Shortest accepted interval, one hour.
    pub const MIN: Self = Self(NonZeroDuration::from_hours(1));

    /// Wraps `value` if it is a whole number of milliseconds of at least [`Self::MIN`].
    ///
    /// # Errors
    ///
    /// Returns [`UsageIntervalError::TooShort`] when `value` is shorter than [`Self::MIN`] or
    /// not a valid duration.
    pub fn try_new(value: Duration) -> Result<Self, UsageIntervalError> {
        match NonZeroDuration::try_new(value) {
            Ok(interval) if interval >= Self::MIN.0 => Ok(Self(interval)),
            _ => Err(UsageIntervalError::TooShort {
                min: Self::MIN.get(),
            }),
        }
    }

    /// Returns the interval.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0.get()
    }
}

impl Default for UsageInterval {
    fn default() -> Self {
        Self::MIN
    }
}

impl TryFrom<NonZeroDuration> for UsageInterval {
    type Error = UsageIntervalError;

    fn try_from(value: NonZeroDuration) -> Result<Self, Self::Error> {
        Self::try_new(value.get())
    }
}

impl From<UsageInterval> for NonZeroDuration {
    fn from(value: UsageInterval) -> Self {
        value.0
    }
}

/// Why a duration is not a valid [`UsageInterval`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UsageIntervalError {
    /// The duration is shorter than the accepted minimum.
    #[error(
        "usage_interval must be a whole number of milliseconds of at least {}",
        gang_types::duration::display(*.min)
    )]
    TooShort {
        /// Shortest accepted duration.
        min: Duration,
    },
}

/// Settings for the `KmsSigner` plugin, read from the `[signer.kms]` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KmsSignerConfig {
    /// AWS region of the KMS key. Required: non-empty, only `a-z`, `0-9` and `-`.
    pub region: Region,
    /// KMS key, as a key id, key ARN or alias. Required and non-empty.
    pub key_id: KeyId,
    /// Timeouts, attempts and retry backoff of each KMS operation, shared with the provisioner.
    /// Defaults to 10 seconds per operation, 3 attempts, 3100 milliseconds to connect, and
    /// backoff from one to 20 seconds. See [`ClientSettings`] for each field.
    #[serde(default)]
    pub client: ClientSettings,
}

/// Default bound on each instance metadata request.
fn default_imds_request_timeout() -> NonZeroDuration {
    NonZeroDuration::from_secs(1)
}

fn default_retirement_poll_interval() -> NonZeroDuration {
    NonZeroDuration::from_secs(5)
}

impl AwsFleetOptions {
    /// Checks that `fallback` is set only on a spot fleet, a rule serde cannot express across
    /// fields. The `FleetOptions` derive calls this from its `validate` hook when the
    /// configuration loads, so a violation fails the parse and names the fleet.
    ///
    /// # Errors
    ///
    /// Returns a lowercase message, without a trailing period, naming the violated rule.
    pub fn validate_fallback(&self) -> Result<(), String> {
        if self.fallback.is_some() && self.capacity != Capacity::Spot {
            return Err("fallback requires capacity = \"spot\"".to_owned());
        }
        Ok(())
    }

    /// Rejects a fleet named like the on-demand twin of another fleet that opts into spot
    /// fallback: both would drive the same Auto Scaling group. Called once with every fleet, when the
    /// configuration loads.
    ///
    /// # Errors
    ///
    /// The rule and both fleet names.
    pub fn validate_twins(fleets: &[(&FleetName, &Self)]) -> Result<(), String> {
        for (name, options) in fleets {
            if options.fallback.is_some() {
                let twin = format!("{name}{ON_DEMAND_SUFFIX}");
                if fleets.iter().any(|(other, _)| other.as_str() == twin) {
                    return Err(format!(
                        "fleet `{twin}` is the on-demand twin of fleet `{name}`, which opts into spot fallback; rename one of them"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_types::FleetName;
    use serde_json::json;

    use super::{AwsConfig, AwsFleetOptions, RetrySpotAfter};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn observation_intervals_default_and_reject_zero() -> TestResult {
        let config: AwsConfig = serde_json::from_value(json!({
            "region": "us-east-1",
            "scratch_mount_point": "/mnt/scratch"
        }))?;
        assert_eq!(
            config.autoscaling_request_interval.get(),
            Duration::from_millis(500)
        );
        assert_eq!(config.supply_poll_interval.get(), Duration::from_secs(1));
        assert_eq!(config.activity_poll_interval.get(), Duration::from_mins(1));

        for key in [
            "autoscaling_request_interval",
            "supply_poll_interval",
            "activity_poll_interval",
        ] {
            let mut value = json!({
                "region": "us-east-1",
                "scratch_mount_point": "/mnt/scratch"
            });
            value[key] = json!("0s");
            assert!(
                serde_json::from_value::<AwsConfig>(value.clone()).is_err(),
                "{key}"
            );
            value[key] = json!("2s");
            let configured: AwsConfig = serde_json::from_value(value)?;
            let interval = match key {
                "autoscaling_request_interval" => configured.autoscaling_request_interval,
                "supply_poll_interval" => configured.supply_poll_interval,
                _ => configured.activity_poll_interval,
            };
            assert_eq!(interval.get(), Duration::from_secs(2));
        }
        Ok(())
    }

    #[test]
    fn rejects_unknown_capacity_with_expected_names() {
        let result = serde_json::from_value::<AwsFleetOptions>(json!({
            "capacity": "reserved"
        }));
        assert!(matches!(result, Err(error) if error.to_string().contains("spot, on-demand")));
    }

    #[test]
    fn a_fleet_named_like_a_fallback_twin_is_rejected() -> TestResult {
        let worker = FleetName::try_new("worker")?;
        let twin = FleetName::try_new("worker-on-demand")?;
        let spot: AwsFleetOptions = serde_json::from_value(json!({
            "capacity": "spot",
            "fallback": {}
        }))?;
        let plain: AwsFleetOptions = serde_json::from_value(json!({
            "capacity": "on-demand"
        }))?;
        assert_eq!(
            AwsFleetOptions::validate_twins(&[(&worker, &spot), (&twin, &plain)]),
            Err("fleet `worker-on-demand` is the on-demand twin of fleet `worker`, which opts into spot fallback; rename one of them".to_owned())
        );
        Ok(())
    }

    #[test]
    fn a_twin_name_without_fallback_is_accepted() -> TestResult {
        let worker = FleetName::try_new("worker")?;
        let twin = FleetName::try_new("worker-on-demand")?;
        let spot: AwsFleetOptions = serde_json::from_value(json!({
            "capacity": "spot"
        }))?;
        let plain: AwsFleetOptions = serde_json::from_value(json!({
            "capacity": "on-demand"
        }))?;
        assert_eq!(
            AwsFleetOptions::validate_twins(&[(&worker, &spot), (&twin, &plain)]),
            Ok(())
        );
        Ok(())
    }

    #[test]
    fn rejects_below_one_minute() {
        assert!(RetrySpotAfter::try_new(Duration::from_secs(59)).is_err());
    }

    #[test]
    fn rejects_above_twenty_four_hours() {
        assert!(RetrySpotAfter::try_new(Duration::from_secs(24 * 3600 + 1)).is_err());
    }

    #[test]
    fn accepts_both_bounds() -> TestResult {
        let min = RetrySpotAfter::try_new(Duration::from_mins(1))?;
        let max = RetrySpotAfter::try_new(Duration::from_hours(24))?;
        assert_eq!(min.get(), Duration::from_mins(1));
        assert_eq!(max.get(), Duration::from_hours(24));
        Ok(())
    }

    #[test]
    fn parses_duration_text() -> TestResult {
        let parsed: RetrySpotAfter = serde_json::from_value(json!("2m"))?;
        assert_eq!(parsed.get(), Duration::from_mins(2));
        Ok(())
    }

    #[test]
    fn default_is_five_minutes() {
        assert_eq!(RetrySpotAfter::default().get(), Duration::from_mins(5));
    }

    #[test]
    fn rejects_a_delay_that_is_not_whole_milliseconds() {
        assert!(
            RetrySpotAfter::try_new(Duration::from_mins(1) + Duration::from_micros(1)).is_err()
        );
    }
}
