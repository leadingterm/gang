//! The cache stores on S3: the content-addressed store and the action cache, both described to the
//! build engine as cloud object stores under one bucket and key prefix.
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
use gang_spi::{
    CacheStorePort, MeasuredUsage, Plugin, PluginContext, PortError, RenderedStoreSpec, StoreAccess,
};
use serde_json::json;
use tokio::sync::OnceCell;

use crate::cloudwatch::BucketMetrics;
use crate::config::S3CacheConfig;

/// Key prefix of the content-addressed store, appended to the configured `key_prefix`.
const CONTENT_PREFIX: &str = "cas/";

/// Key prefix of the action cache, appended to the configured `key_prefix`.
const ACTION_PREFIX: &str = "ac/";

/// The `NativeLink` store kind that reads and writes S3 objects.
const STORE_KEY: &str = "experimental_cloud_object_store";

/// The `NativeLink` provider name for S3.
const PROVIDER: &str = "aws";

/// The cache stores on S3, opened by the workers directly.
pub struct S3Cache {
    config: S3CacheConfig,
    /// The bucket's storage metrics; `None` for an S3-compatible endpoint.
    metrics: Option<BucketMetrics>,
}

impl Plugin for S3Cache {
    type Config = S3CacheConfig;

    fn build(config: S3CacheConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self::new(
            config,
            Arc::new(SystemClock::new()),
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
    ) -> Self {
        let metrics = config
            .endpoint
            .is_none()
            .then(|| BucketMetrics::new(&config, clock, metrics_client));
        Self { config, metrics }
    }

    /// A cache that measures the bucket through `client` and reads `clock`.
    #[cfg(test)]
    pub(crate) fn with_metrics_client(
        config: S3CacheConfig,
        clock: Arc<dyn Clock>,
        client: aws_sdk_cloudwatch::Client,
    ) -> Self {
        Self::new(config, clock, OnceCell::new_with(Some(client)))
    }
}

impl CacheStorePort for S3Cache {
    fn content_store(&self) -> Result<RenderedStoreSpec, PortError> {
        Ok(store_spec(&self.config, CONTENT_PREFIX))
    }

    fn action_store(&self) -> Result<RenderedStoreSpec, PortError> {
        Ok(store_spec(&self.config, ACTION_PREFIX))
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

/// Describes one store under the configured key prefix, namespaced by `prefix`.
fn store_spec(config: &S3CacheConfig, prefix: &str) -> RenderedStoreSpec {
    let mut object = json!({
        "provider": PROVIDER,
        "region": config.region.as_str(),
        "bucket": config.bucket.as_str(),
        "force_path_style": config.force_path_style,
        "key_prefix": format!("{}{prefix}", config.key_prefix.as_str()),
        "retry": {
            "max_retries": config.tuning.max_retries,
            "delay": config.tuning.retry_delay.get().as_secs_f64(),
            "jitter": f64::from(config.tuning.retry_jitter_percent.get()) / 100.0,
        },
        "multipart_max_concurrent_uploads": config.tuning.multipart_max_concurrent_uploads.get(),
        "max_retry_buffer_per_request": config.tuning.retry_buffer_per_request.get(),
    });
    if let Some(endpoint) = &config.endpoint {
        object["endpoint"] = json!(endpoint.as_str());
        // NativeLink otherwise configures its S3 HTTP connector for HTTPS only.
        if endpoint.scheme() == "http" {
            object["insecure_allow_http"] = json!(true);
        }
    }
    if let Some(expiry) = &config.tuning.consider_expired_after {
        object["consider_expired_after_s"] = json!(expiry.as_secs());
    }
    RenderedStoreSpec(json!({ STORE_KEY: object }))
}

#[cfg(test)]
mod tests {
    use std::num::TryFromIntError;
    use std::time::Duration;

    use aws_smithy_http_client::test_util::StaticReplayClient;
    use gang_clock::{TestClock, Timestamp};
    use nativelink_config::stores::StoreSpec;
    use serde_json::Value;

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

    fn object(spec: &RenderedStoreSpec) -> &Value {
        &spec.0[STORE_KEY]
    }

    #[test]
    fn both_stores_deserialize_into_nativelink_store_specs() -> TestResult {
        let cache = S3Cache::build(serde_json::from_value(base_config())?, &context())?;

        serde_json::from_value::<StoreSpec>(cache.content_store()?.0)?;
        serde_json::from_value::<StoreSpec>(cache.action_store()?.0)?;
        Ok(())
    }

    #[test]
    fn endpoint_and_path_style_render_for_both_stores() -> TestResult {
        for (endpoint, path_style, allow_http) in [
            (None, false, false),
            (None, true, false),
            (Some("http://127.0.0.1:9000/"), false, true),
            (Some("http://127.0.0.1:9000/"), true, true),
            (Some("https://s3.example.com/"), false, false),
            (Some("https://s3.example.com/"), true, false),
        ] {
            let mut value = base_config();
            if let Some(endpoint) = endpoint {
                value["endpoint"] = json!(endpoint);
            }
            value["force_path_style"] = json!(path_style);
            let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;
            for spec in [cache.content_store()?, cache.action_store()?] {
                assert_eq!(
                    object(&spec).get("endpoint").and_then(Value::as_str),
                    endpoint
                );
                assert_eq!(object(&spec)["force_path_style"], json!(path_style));
                assert_eq!(
                    object(&spec)
                        .get("insecure_allow_http")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    allow_http
                );
                let parsed = serde_json::from_value::<StoreSpec>(spec.0)?;
                let StoreSpec::ExperimentalCloudObjectStore(
                    nativelink_config::stores::ExperimentalCloudObjectSpec::Aws(aws),
                ) = parsed
                else {
                    return Err("expected an AWS cloud object store".into());
                };
                assert_eq!(aws.endpoint.as_deref(), endpoint);
                assert_eq!(aws.force_path_style, path_style);
                assert_eq!(aws.common.insecure_allow_http, allow_http);
            }
        }
        Ok(())
    }

    #[test]
    fn content_and_action_keys_differ_under_the_configured_prefix() -> TestResult {
        let mut value = base_config();
        value["key_prefix"] = json!("gang/");
        let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;

        let content = cache.content_store()?;
        let action = cache.action_store()?;

        assert_eq!(object(&content)["key_prefix"].as_str(), Some("gang/cas/"));
        assert_eq!(object(&action)["key_prefix"].as_str(), Some("gang/ac/"));
        Ok(())
    }

    #[test]
    fn retry_delay_and_jitter_render_as_seconds_and_fraction() -> TestResult {
        let mut value = base_config();
        value["tuning"]["retry_delay"] = json!("300ms");
        value["tuning"]["retry_jitter_percent"] = json!(50);
        let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;

        let spec = cache.content_store()?;

        assert_eq!(object(&spec)["retry"]["delay"], json!(0.3));
        assert_eq!(object(&spec)["retry"]["jitter"], json!(0.5));
        Ok(())
    }

    #[test]
    fn expiry_is_absent_by_default_and_rendered_in_seconds_when_set() -> TestResult {
        let default = S3Cache::build(serde_json::from_value(base_config())?, &context())?;
        assert!(
            object(&default.content_store()?)
                .get("consider_expired_after_s")
                .is_none()
        );

        let mut value = base_config();
        value["tuning"]["consider_expired_after"] = json!("6d");
        let expiring = S3Cache::build(serde_json::from_value(value)?, &context())?;
        assert_eq!(
            object(&expiring.action_store()?)["consider_expired_after_s"].as_u64(),
            Some(518_400)
        );
        Ok(())
    }

    #[test]
    fn max_expiry_renders_and_deserializes_into_nativelink() -> TestResult {
        let mut value = base_config();
        value["tuning"]["consider_expired_after"] = json!("4294967295s");
        let cache = S3Cache::build(serde_json::from_value(value)?, &context())?;

        let spec = cache.content_store()?;

        assert_eq!(
            object(&spec)["consider_expired_after_s"].as_u64(),
            Some(4_294_967_295)
        );
        serde_json::from_value::<StoreSpec>(spec.0)?;
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
