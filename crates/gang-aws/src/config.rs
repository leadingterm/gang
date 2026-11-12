//! Settings for the AWS plugins, read from the `[provisioner.aws]`, `[cache.s3]` and
//! `[signer.kms]` tables of the gang config.
//!
//! Every struct rejects unknown keys. Each tunable has a default, applied when its key is
//! absent. The types of the fields enforce their bounds, so a config value that parses is valid.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_types::{
    FleetName, MachineClass, NonZeroBytes, NonZeroDuration, Percent, WorkloadTokenSettings,
};
use serde::{Deserialize, Serialize};

use crate::client_settings::ClientSettings;
use crate::names::{BucketName, FleetTagKey, KeyId, KeyPrefix, MountPoint, Region};

/// Suffix that names the on-demand twin of an opted-in fleet's spot group.
pub(crate) const ON_DEMAND_SUFFIX: &str = "-on-demand";

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
    pub workload_token: WorkloadTokenSettings,
    /// Signing algorithm requested for workload tokens. Default `rs256`; `es384` is the other
    /// choice.
    #[serde(default)]
    pub workload_token_algorithm: WorkloadTokenAlgorithm,
    /// Timeouts, attempts and retry backoff of each AWS operation, shared by the provisioner and
    /// the KMS signer. Defaults to 10 seconds per operation, 3 attempts, 3100 milliseconds to
    /// connect, and backoff from one to 20 seconds. See [`ClientSettings`] for each field.
    #[serde(default)]
    pub client: ClientSettings,
    /// Bound on each instance metadata (IMDS) request. Default `1s`. Must be greater than zero.
    #[serde(default = "default_imds_request_timeout")]
    pub imds_request_timeout: NonZeroDuration,
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
    /// Prefix prepended to every object key. Defaults to an empty string. Must not start with `/`.
    #[serde(default)]
    pub key_prefix: KeyPrefix,
    /// Retry, multipart upload and expiry tuning. Defaults to 6 retries, a 300 millisecond
    /// retry delay, 50% jitter, 10 concurrent uploads, a 5 MiB retry buffer and no expiry.
    /// See [`S3Tuning`] for each field.
    #[serde(default)]
    pub tuning: S3Tuning,
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

/// Age of a cached object after which it is treated as expired, in whole seconds. `NativeLink`
/// reads it as a `u32`, so it is at most `u32::MAX` seconds.
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

    use super::{AwsFleetOptions, RetrySpotAfter};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

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
