//! The cache stores on S3: the content-addressed store and the action cache, both opened
//! under one bucket and key prefix.
//!
//! Workers open the stores themselves with their own credentials (the instance role), so the
//! controller never proxies blob traffic. Bucket lifecycle expiry should exceed
//! `consider_expired_after`, so that an object is never deleted while the engine still treats it
//! as existing.
//!
//! The cache measures the bucket through `CloudWatch`'s daily storage metrics (the `cloudwatch`
//! module). An S3-compatible service set through `endpoint` has no `CloudWatch`, so the cache
//! reports no measurement for it and never opens a `CloudWatch` client.

use std::sync::Arc;

use gang_clock::{Clock, SystemClock};
use gang_spi::{CacheStorePort, MeasuredUsage, Plugin, PluginContext, PortError, StoreAccess};
use tokio::sync::OnceCell;

use crate::cloudwatch::BucketMetrics;
use crate::config::S3CacheConfig;
#[cfg(test)]
use crate::s3_existence::S3ExistenceCache;
use crate::s3_store::{S3ActionStore, S3Backend, S3ContentStore};

/// The cache stores on S3, opened by the workers directly.
pub struct S3Cache {
    /// The bucket's storage metrics; `None` for an S3-compatible endpoint.
    metrics: Option<BucketMetrics>,
    backend: Arc<S3Backend>,
}

impl Plugin for S3Cache {
    type Config = S3CacheConfig;

    fn build(config: S3CacheConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self::new(
            config,
            Arc::new(SystemClock::new()),
            OnceCell::new(),
            OnceCell::new(),
        ))
    }
}

impl S3Cache {
    /// A cache that reads `clock` and measures the bucket through `metrics_client` once it is
    /// set, or else through a regional client loaded on first use.
    fn new(
        config: S3CacheConfig,
        clock: Arc<dyn Clock>,
        metrics_client: OnceCell<aws_sdk_cloudwatch::Client>,
        s3_client: OnceCell<aws_sdk_s3::Client>,
    ) -> Self {
        let metrics = config
            .endpoint
            .is_none()
            .then(|| BucketMetrics::new(&config, Arc::clone(&clock), metrics_client));
        let backend = Arc::new(S3Backend::new(config, clock, s3_client));
        Self { metrics, backend }
    }

    /// Opens a [`S3ContentStore`] backed by this cache's S3 bucket and positive existence cache.
    #[must_use]
    pub fn s3_content_store(&self) -> S3ContentStore {
        S3ContentStore::new(Arc::clone(&self.backend))
    }

    /// Opens an [`S3ActionStore`] scoped to `environment` in this cache's S3 bucket.
    #[must_use]
    pub fn s3_action_store(&self, environment: &gang_types::EnvironmentName) -> S3ActionStore {
        S3ActionStore::new(Arc::clone(&self.backend), environment)
    }

    /// A cache that measures the bucket through `client` and reads `clock`.
    #[cfg(test)]
    pub(crate) fn with_metrics_client(
        config: S3CacheConfig,
        clock: Arc<dyn Clock>,
        client: aws_sdk_cloudwatch::Client,
    ) -> Self {
        Self::new(
            config,
            clock,
            OnceCell::new_with(Some(client)),
            OnceCell::new(),
        )
    }

    /// A cache backed by a pre-built S3 client and `clock` for unit tests.
    #[cfg(test)]
    pub(crate) fn with_s3_client(
        config: S3CacheConfig,
        clock: Arc<dyn Clock>,
        client: aws_sdk_s3::Client,
    ) -> Self {
        Self::new(
            config,
            clock,
            OnceCell::new(),
            OnceCell::new_with(Some(client)),
        )
    }

    /// Exposes the shared positive existence cache for unit tests.
    #[cfg(test)]
    pub(crate) fn existence_cache(&self) -> &S3ExistenceCache {
        self.backend.existence()
    }
}

impl CacheStorePort for S3Cache {
    type Content = S3ContentStore;
    type Actions = S3ActionStore;

    fn content_store(&self) -> Result<Self::Content, PortError> {
        Ok(self.s3_content_store())
    }

    fn action_store(
        &self,
        environment: &gang_types::EnvironmentName,
    ) -> Result<Self::Actions, PortError> {
        Ok(self.s3_action_store(environment))
    }

    /// The workers open the S3 stores with their own credentials.
    fn worker_access(&self) -> StoreAccess {
        StoreAccess::Direct
    }

    /// The bucket's latest daily size and object count from `CloudWatch`, or `None` for an
    /// S3-compatible endpoint or before `CloudWatch` has a datapoint.
    async fn measured_usage(&self) -> Result<Option<MeasuredUsage>, PortError> {
        match &self.metrics {
            Some(metrics) => Ok(metrics.measured().await?),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::TryFromIntError;
    use std::time::Duration;

    use aws_smithy_http_client::test_util::StaticReplayClient;
    use gang_clock::{TestClock, Timestamp};
    use serde_json::{Value, json};

    use super::*;
    use crate::cloudwatch_replay::{
        Reply, Series, client, complete, failure, metric_data, replay, requests, storage,
    };
    use crate::test_support::context;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const DAY: i64 = 86_400;
    /// Midnight UTC of 2026-11-18, in seconds since the Unix epoch.
    const TODAY: i64 = 1_794_960_000;
    /// The test clock's wall reading: 06:00 UTC on that day, in milliseconds.
    const NOW_MILLIS: u64 = 1_794_981_600_000;
    const HOUR: Duration = Duration::from_hours(1);

    fn base_config() -> Value {
        json!({ "region": "us-east-1", "bucket": "example-bucket" })
    }

    #[test]
    fn cache_port_opens_typed_direct_stores() -> TestResult {
        let cache = S3Cache::build(serde_json::from_value(base_config())?, &context())?;
        assert_eq!(cache.worker_access(), StoreAccess::Direct);
        let _content: S3ContentStore = cache.content_store()?;
        let _actions: S3ActionStore = cache.action_store(&gang_types::EnvironmentName::default())?;
        Ok(())
    }

    #[test]
    fn empty_bucket_is_rejected_at_deserialization() {
        let mut value = base_config();
        value["bucket"] = json!("");
        assert!(serde_json::from_value::<S3CacheConfig>(value).is_err());
    }

    /// A cache whose `CloudWatch` requests are answered by `responses` in order, on a test clock.
    struct Measuring {
        cache: S3Cache,
        clock: Arc<TestClock>,
        http: StaticReplayClient,
    }

    impl Measuring {
        fn new(extra: Value, responses: Vec<Reply>) -> Result<Self, serde_json::Error> {
            let mut value = base_config();
            if let (Some(config), Value::Object(extra)) = (value.as_object_mut(), extra) {
                config.extend(extra);
            }
            let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(NOW_MILLIS)));
            let http = replay(responses);
            let cache = S3Cache::with_metrics_client(
                serde_json::from_value(value)?,
                clock.clone(),
                client(&http),
            );
            Ok(Self { cache, clock, http })
        }

        /// Requests that reached the replayed `CloudWatch`.
        fn sent(&self) -> usize {
            self.http.actual_requests().count()
        }
    }

    fn usage(bytes: u64, objects: u64, seconds: i64) -> Result<MeasuredUsage, TryFromIntError> {
        Ok(MeasuredUsage {
            bytes,
            objects,
            measured: Timestamp::from_unix_millis(u64::try_from(seconds)? * 1_000),
        })
    }

    /// Catches picking the earliest or the first datapoint, or pairing the series by position
    /// instead of by timestamp.
    #[tokio::test]
    async fn unsorted_results_measure_the_newest_datapoint() -> TestResult {
        let measuring = Measuring::new(
            json!({}),
            vec![storage(
                (
                    &[TODAY - DAY, TODAY - 3 * DAY, TODAY - 2 * DAY],
                    &[3_000.0, 1_000.0, 2_000.0],
                ),
                (
                    &[TODAY - 2 * DAY, TODAY - DAY, TODAY - 3 * DAY],
                    &[20.0, 30.0, 10.0],
                ),
            )?],
        )?;

        assert_eq!(
            measuring.cache.measured_usage().await?,
            Some(usage(3_000, 30, TODAY - DAY)?)
        );
        Ok(())
    }

    /// Catches taking each series' newest datapoint independently, which would pair today's size
    /// with yesterday's count.
    #[tokio::test]
    async fn a_datapoint_missing_from_one_series_is_not_measured() -> TestResult {
        let measuring = Measuring::new(
            json!({}),
            vec![storage(
                (&[TODAY, TODAY - DAY], &[5_000.0, 4_000.0]),
                (&[TODAY - DAY, TODAY - 2 * DAY], &[40.0, 35.0]),
            )?],
        )?;

        assert_eq!(
            measuring.cache.measured_usage().await?,
            Some(usage(4_000, 40, TODAY - DAY)?)
        );
        Ok(())
    }

    /// Catches a wrong namespace, metric, storage type, statistic or period, a key-prefix filter,
    /// a window that does not start at midnight three days back or end now, or a paginated query.
    #[tokio::test]
    async fn the_query_asks_for_whole_bucket_daily_averages_over_three_days() -> TestResult {
        let measuring = Measuring::new(
            json!({"key_prefix": "gang/"}),
            vec![storage((&[TODAY], &[1.0]), (&[TODAY], &[1.0]))?],
        )?;

        measuring.cache.measured_usage().await?;

        let sent = requests(&measuring.http)?;
        let [body] = sent.as_slice() else {
            return Err(format!("expected one request, sent {}", sent.len()).into());
        };
        let query = |id: &str, metric: &str, storage_type: &str| {
            json!({
                "Id": id,
                "ReturnData": true,
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/S3",
                        "MetricName": metric,
                        "Dimensions": [
                            {"Name": "BucketName", "Value": "example-bucket"},
                            {"Name": "StorageType", "Value": storage_type},
                        ],
                    },
                    "Period": 86_400,
                    "Stat": "Average",
                },
            })
        };
        assert_eq!(
            body["MetricDataQueries"],
            json!([
                query("bytes", "BucketSizeBytes", "StandardStorage"),
                query("objects", "NumberOfObjects", "AllStorageTypes"),
            ])
        );
        assert_eq!(body["StartTime"], json!(TODAY - 3 * DAY));
        assert_eq!(body["EndTime"], json!(TODAY + 6 * 3_600));
        assert_eq!(body.get("NextToken"), None);
        assert!(!body.to_string().contains("gang/"), "{body}");
        Ok(())
    }

    /// Catches ignoring the configured interval, using the one-hour minimum instead of it, or
    /// refetching before the interval has fully elapsed.
    #[tokio::test]
    async fn a_call_within_the_interval_sends_no_request() -> TestResult {
        let measuring = Measuring::new(
            json!({"usage_interval": "2h"}),
            vec![
                storage((&[TODAY - DAY], &[100.0]), (&[TODAY - DAY], &[1.0]))?,
                storage((&[TODAY], &[200.0]), (&[TODAY], &[2.0]))?,
            ],
        )?;
        let first = Some(usage(100, 1, TODAY - DAY)?);

        assert_eq!(measuring.cache.measured_usage().await?, first);
        let just_short = (HOUR * 2)
            .checked_sub(Duration::from_millis(1))
            .ok_or("two hours are longer than a millisecond")?;
        measuring.clock.advance(just_short);
        assert_eq!(measuring.cache.measured_usage().await?, first);
        assert_eq!(measuring.sent(), 1);

        measuring.clock.advance(Duration::from_millis(1));
        assert_eq!(
            measuring.cache.measured_usage().await?,
            Some(usage(200, 2, TODAY)?)
        );
        assert_eq!(measuring.sent(), 2);
        Ok(())
    }

    /// Catches swallowing the failure, dropping the last measurement on a failure, or retrying a
    /// failed fetch before the interval elapses.
    #[tokio::test]
    async fn a_failed_fetch_is_returned_and_the_last_measurement_kept() -> TestResult {
        let measuring = Measuring::new(
            json!({}),
            vec![
                storage((&[TODAY - DAY], &[100.0]), (&[TODAY - DAY], &[1.0]))?,
                failure(500)?,
                storage((&[TODAY], &[200.0]), (&[TODAY], &[2.0]))?,
            ],
        )?;
        let first = Some(usage(100, 1, TODAY - DAY)?);
        assert_eq!(measuring.cache.measured_usage().await?, first);

        measuring.clock.advance(HOUR);
        let error = measuring
            .cache
            .measured_usage()
            .await
            .err()
            .ok_or("the failed fetch succeeded")?;
        assert!(matches!(error, PortError::Unavailable(_)), "{error}");
        assert_eq!(measuring.cache.measured_usage().await?, first);
        assert_eq!(measuring.sent(), 2);

        measuring.clock.advance(HOUR);
        assert_eq!(
            measuring.cache.measured_usage().await?,
            Some(usage(200, 2, TODAY)?)
        );
        assert_eq!(measuring.sent(), 3);
        Ok(())
    }

    /// Catches reporting zeros without a datapoint, or forgetting the last measurement when a
    /// later response has none.
    #[tokio::test]
    async fn no_shared_datapoint_measures_nothing_new() -> TestResult {
        let measuring = Measuring::new(
            json!({}),
            vec![
                storage((&[], &[]), (&[], &[]))?,
                storage((&[TODAY - DAY], &[100.0]), (&[TODAY - 2 * DAY], &[1.0]))?,
                storage((&[TODAY - DAY], &[100.0]), (&[TODAY - DAY], &[1.0]))?,
                storage((&[], &[]), (&[], &[]))?,
            ],
        )?;

        assert_eq!(measuring.cache.measured_usage().await?, None);
        measuring.clock.advance(HOUR);
        assert_eq!(measuring.cache.measured_usage().await?, None);
        measuring.clock.advance(HOUR);
        let measured = Some(usage(100, 1, TODAY - DAY)?);
        assert_eq!(measuring.cache.measured_usage().await?, measured);
        measuring.clock.advance(HOUR);
        assert_eq!(measuring.cache.measured_usage().await?, measured);
        assert_eq!(measuring.sent(), 4);
        Ok(())
    }

    /// Catches asking `CloudWatch` about a bucket on an S3-compatible service, which has none.
    #[tokio::test]
    async fn an_s3_compatible_endpoint_never_reaches_cloudwatch() -> TestResult {
        let measuring = Measuring::new(json!({"endpoint": "http://127.0.0.1:9000/"}), vec![])?;

        assert_eq!(measuring.cache.measured_usage().await?, None);
        measuring.clock.advance(HOUR);
        assert_eq!(measuring.cache.measured_usage().await?, None);
        assert_eq!(measuring.sent(), 0);
        Ok(())
    }

    /// Catches publishing a partial, paginated, unpaired or out-of-range result as a
    /// measurement, or dropping the last measurement because of one.
    #[tokio::test]
    async fn incomplete_or_invalid_results_fail_and_keep_the_last_measurement() -> TestResult {
        let today = [TODAY];
        let one = [1.0];
        let cases = [
            (
                "partial size",
                metric_data(
                    &[
                        Series {
                            status: "PartialData",
                            ..complete("bytes", &today, &one)
                        },
                        complete("objects", &today, &one),
                    ],
                    None,
                )?,
            ),
            (
                "failed count",
                metric_data(
                    &[
                        complete("bytes", &today, &one),
                        Series {
                            status: "InternalError",
                            ..complete("objects", &today, &one)
                        },
                    ],
                    None,
                )?,
            ),
            (
                "another page",
                metric_data(
                    &[
                        complete("bytes", &today, &one),
                        complete("objects", &today, &one),
                    ],
                    Some("next"),
                )?,
            ),
            (
                "missing count",
                metric_data(&[complete("bytes", &today, &one)], None)?,
            ),
            (
                "unpaired values",
                storage((&today, &[1.0, 2.0]), (&today, &one))?,
            ),
            ("negative size", storage((&today, &[-1.0]), (&today, &one))?),
            (
                "size not a number",
                storage((&today, &[f64::NAN]), (&today, &one))?,
            ),
            (
                "count beyond u64",
                storage((&today, &one), (&today, &[18_446_744_073_709_551_616.0]))?,
            ),
        ];
        for (case, bad) in cases {
            let measuring = Measuring::new(
                json!({}),
                vec![
                    storage((&[TODAY - DAY], &[100.0]), (&[TODAY - DAY], &[1.0]))?,
                    bad,
                ],
            )?;
            let first = Some(usage(100, 1, TODAY - DAY)?);
            assert_eq!(measuring.cache.measured_usage().await?, first, "{case}");

            measuring.clock.advance(HOUR);
            let error = measuring
                .cache
                .measured_usage()
                .await
                .err()
                .ok_or_else(|| format!("{case}: accepted"))?;
            assert!(
                error.to_string().contains("GetMetricData"),
                "{case}: {error}"
            );
            assert_eq!(measuring.cache.measured_usage().await?, first, "{case}");
            assert_eq!(measuring.sent(), 2, "{case}");
        }
        Ok(())
    }

    /// Catches fetching once per caller when calls overlap.
    #[tokio::test]
    async fn concurrent_callers_share_one_request() -> TestResult {
        let measuring = Measuring::new(
            json!({}),
            vec![
                storage((&[TODAY - DAY], &[100.0]), (&[TODAY - DAY], &[1.0]))?,
                storage((&[TODAY], &[200.0]), (&[TODAY], &[2.0]))?,
            ],
        )?;

        let (first, second) = tokio::join!(
            measuring.cache.measured_usage(),
            measuring.cache.measured_usage()
        );

        let measured = Some(usage(100, 1, TODAY - DAY)?);
        assert_eq!(first?, measured);
        assert_eq!(second?, measured);
        assert_eq!(measuring.sent(), 1);
        Ok(())
    }
}
