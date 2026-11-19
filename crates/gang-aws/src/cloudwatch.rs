//! The bucket's size and object count, read from `CloudWatch`'s free daily S3 storage metrics.
//!
//! S3 reports `BucketSizeBytes` and `NumberOfObjects` once a day for the whole bucket, so the
//! measurement ignores the configured key prefix and may be up to a day old. Fetches are
//! serialized and attempted at most once per `usage_interval` of monotonic time, failures
//! included; between attempts the last successful measurement is returned.

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_cloudwatch::operation::get_metric_data::GetMetricDataOutput;
use aws_sdk_cloudwatch::types::{Dimension, Metric, MetricDataQuery, MetricStat, StatusCode};
use aws_smithy_types::DateTime;
use gang_aws_sdk::{ClientSettings, Region, call_error, load};
use gang_clock::{Clock, Tick, Timestamp};
use gang_spi::MeasuredUsage;
use num_traits::ToPrimitive as _;
use tokio::sync::{Mutex, OnceCell};

use crate::config::S3CacheConfig;
use crate::errors::AwsError;
use crate::names::BucketName;

const GET_METRIC_DATA: &str = "GetMetricData";

/// The `CloudWatch` namespace of the S3 storage metrics.
const NAMESPACE: &str = "AWS/S3";
/// S3 publishes one storage datapoint per day.
const PERIOD_SECONDS: i32 = 86_400;
const STATISTIC: &str = "Average";
/// Days before the start of the current UTC day that the query reaches back.
const LOOKBACK_DAYS: u64 = 3;
const DAY_MILLIS: u64 = 86_400_000;

/// Query ids; `CloudWatch` requires them to start with a lowercase letter.
const BYTES_ID: &str = "bytes";
const OBJECTS_ID: &str = "objects";

const BUCKET_SIZE_BYTES: &str = "BucketSizeBytes";
const NUMBER_OF_OBJECTS: &str = "NumberOfObjects";
const BUCKET_NAME: &str = "BucketName";
const STORAGE_TYPE: &str = "StorageType";
/// The storage type whose bytes the cache writes: S3 Standard.
const STANDARD_STORAGE: &str = "StandardStorage";
/// `NumberOfObjects` is only reported across all storage types.
const ALL_STORAGE_TYPES: &str = "AllStorageTypes";

/// The bucket's storage metrics in `CloudWatch`, fetched at most once per interval.
pub(crate) struct BucketMetrics {
    region: Region,
    bucket: BucketName,
    settings: ClientSettings,
    interval: Duration,
    clock: Arc<dyn Clock>,
    client: OnceCell<aws_sdk_cloudwatch::Client>,
    state: Mutex<Throttle>,
}

/// When the last fetch was attempted and what the last successful one measured.
#[derive(Default)]
struct Throttle {
    attempted: Option<Tick>,
    last: Option<MeasuredUsage>,
}

impl BucketMetrics {
    /// Metrics of `config`'s bucket, read through `client` once it is set, or else through a
    /// regional client loaded on first use.
    pub(crate) fn new(
        config: &S3CacheConfig,
        clock: Arc<dyn Clock>,
        client: OnceCell<aws_sdk_cloudwatch::Client>,
    ) -> Self {
        Self {
            region: config.region.clone(),
            bucket: config.bucket.clone(),
            settings: config.client.clone(),
            interval: config.usage_interval.get(),
            clock,
            client,
            state: Mutex::new(Throttle::default()),
        }
    }

    /// The latest measurement. Fetches when no attempt was made within the interval, and
    /// otherwise returns the last successful measurement.
    ///
    /// A failed fetch is returned to its caller and keeps the last measurement for later calls.
    pub(crate) async fn measured(&self) -> Result<Option<MeasuredUsage>, AwsError> {
        let mut state = self.state.lock().await;
        let now = self.clock.monotonic();
        if state
            .attempted
            .is_some_and(|at| now.saturating_duration_since(at) < self.interval)
        {
            return Ok(state.last);
        }
        state.attempted = Some(now);
        if let Some(measured) = self.fetch().await? {
            state.last = Some(measured);
        }
        Ok(state.last)
    }

    async fn client(&self) -> &aws_sdk_cloudwatch::Client {
        self.client
            .get_or_init(|| async {
                aws_sdk_cloudwatch::Client::new(&load(&self.region, &self.settings).await)
            })
            .await
    }

    /// Asks for the daily averages from the start of the UTC day `LOOKBACK_DAYS` ago until now,
    /// in one request.
    async fn fetch(&self) -> Result<Option<MeasuredUsage>, AwsError> {
        let now = self.clock.wall();
        let bucket = self.bucket.as_str();
        let output = self
            .client()
            .await
            .get_metric_data()
            .metric_data_queries(query(BYTES_ID, BUCKET_SIZE_BYTES, STANDARD_STORAGE, bucket))
            .metric_data_queries(query(
                OBJECTS_ID,
                NUMBER_OF_OBJECTS,
                ALL_STORAGE_TYPES,
                bucket,
            ))
            .start_time(date_time(window_start(now)))
            .end_time(date_time(now))
            .send()
            .await
            .map_err(|e| call_error(GET_METRIC_DATA, e))?;
        newest(&output)
    }
}

/// One daily average of `metric` for the whole bucket and `storage_type`.
fn query(id: &str, metric: &str, storage_type: &str, bucket: &str) -> MetricDataQuery {
    let metric = Metric::builder()
        .namespace(NAMESPACE)
        .metric_name(metric)
        .dimensions(Dimension::builder().name(BUCKET_NAME).value(bucket).build())
        .dimensions(
            Dimension::builder()
                .name(STORAGE_TYPE)
                .value(storage_type)
                .build(),
        )
        .build();
    MetricDataQuery::builder()
        .id(id)
        .return_data(true)
        .metric_stat(
            MetricStat::builder()
                .metric(metric)
                .period(PERIOD_SECONDS)
                .stat(STATISTIC)
                .build(),
        )
        .build()
}

/// The start of the UTC day `LOOKBACK_DAYS` before `now`'s, so that the daily periods align
/// with the midnight timestamps S3 reports.
fn window_start(now: Timestamp) -> Timestamp {
    let day = now.as_unix_millis() / DAY_MILLIS;
    Timestamp::from_unix_millis(day.saturating_sub(LOOKBACK_DAYS) * DAY_MILLIS)
}

fn date_time(at: Timestamp) -> DateTime {
    DateTime::from_millis(i64::try_from(at.as_unix_millis()).unwrap_or(i64::MAX))
}

/// The newest point at which both series have a datapoint, or `None` when they share none.
///
/// Rejects a response that is paginated or whose series are incomplete, so that a partial
/// answer never replaces a measurement.
fn newest(output: &GetMetricDataOutput) -> Result<Option<MeasuredUsage>, AwsError> {
    if output.next_token().is_some() {
        return Err(AwsError::malformed(GET_METRIC_DATA, "final page"));
    }
    let (byte_times, byte_values) = series(output, BYTES_ID, "complete bucket size result")?;
    let (object_times, object_values) = series(output, OBJECTS_ID, "complete object count result")?;

    let mut latest: Option<(DateTime, f64, f64)> = None;
    for (at, bytes) in byte_times.iter().zip(byte_values) {
        if latest.is_some_and(|(seen, _, _)| seen >= *at) {
            continue;
        }
        if let Some(objects) = object_times
            .iter()
            .zip(object_values)
            .find_map(|(time, objects)| (time == at).then_some(*objects))
        {
            latest = Some((*at, *bytes, objects));
        }
    }
    let Some((at, bytes, objects)) = latest else {
        return Ok(None);
    };

    let measured = at
        .to_millis()
        .ok()
        .and_then(|millis| u64::try_from(millis).ok())
        .ok_or_else(|| AwsError::malformed(GET_METRIC_DATA, "timestamp after the unix epoch"))?;
    Ok(Some(MeasuredUsage {
        bytes: whole(bytes)
            .ok_or_else(|| AwsError::malformed(GET_METRIC_DATA, "valid bucket size"))?,
        objects: whole(objects)
            .ok_or_else(|| AwsError::malformed(GET_METRIC_DATA, "valid object count"))?,
        measured: Timestamp::from_unix_millis(measured),
    }))
}

/// The timestamps and values of the result `id`, which must be complete and paired.
fn series<'a>(
    output: &'a GetMetricDataOutput,
    id: &str,
    what: &'static str,
) -> Result<(&'a [DateTime], &'a [f64]), AwsError> {
    let result = output
        .metric_data_results()
        .iter()
        .find(|result| result.id() == Some(id))
        .filter(|result| result.status_code() == Some(&StatusCode::Complete))
        .filter(|result| result.timestamps().len() == result.values().len())
        .ok_or_else(|| AwsError::malformed(GET_METRIC_DATA, what))?;
    Ok((result.timestamps(), result.values()))
}

/// `value` rounded to the nearest whole number, if it is finite, not negative and below 2^64.
fn whole(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    value.round().to_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_accepts_integral_values_up_to_the_largest_below_two_to_the_64() {
        assert_eq!(whole(0.0), Some(0));
        assert_eq!(whole(-0.0), Some(0));
        assert_eq!(whole(1.0), Some(1));
        assert_eq!(whole(1_234_567_890.0), Some(1_234_567_890));
        assert_eq!(whole(9_007_199_254_740_992.0), Some(1 << 53));
        assert_eq!(whole(9_223_372_036_854_775_808.0), Some(1 << 63));
        assert_eq!(
            whole(18_446_744_073_709_549_568.0),
            Some(18_446_744_073_709_549_568)
        );
    }

    #[test]
    fn whole_rounds_fractions_to_the_nearest() {
        assert_eq!(whole(0.4), Some(0));
        assert_eq!(whole(0.5), Some(1));
        assert_eq!(whole(2.6), Some(3));
        assert_eq!(whole(f64::MIN_POSITIVE), Some(0));
    }

    #[test]
    fn whole_rejects_negative_non_finite_and_out_of_range_values() {
        for value in [
            -1.0,
            -0.6,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            18_446_744_073_709_551_616.0,
            f64::MAX,
        ] {
            assert_eq!(whole(value), None, "{value}");
        }
    }

    #[test]
    fn window_starts_at_utc_midnight_three_days_back() {
        let now = Timestamp::from_unix_millis(10 * DAY_MILLIS + 3_600_000 + 1);
        assert_eq!(
            window_start(now),
            Timestamp::from_unix_millis(7 * DAY_MILLIS)
        );
        assert_eq!(
            window_start(Timestamp::from_unix_millis(DAY_MILLIS)),
            Timestamp::UNIX_EPOCH
        );
    }
}
