//! A single request-start budget for all Auto Scaling operations and retry attempts.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::config_bag::ConfigBag;
use gang_clock::{Clock, Tick};
use tokio::sync::Mutex;

/// Builds an Auto Scaling client with a shared request-start budget.
pub(crate) fn client(
    sdk: &aws_config::SdkConfig,
    clock: Arc<dyn Clock>,
    interval: Duration,
) -> aws_sdk_autoscaling::Client {
    let http = PacedClient {
        inner: sdk.http_client().unwrap_or_else(gang_aws_sdk::http_client),
        pacing: Arc::new(Pacing {
            clock,
            interval,
            last_start: Mutex::new(None),
        }),
    };
    let config = aws_sdk_autoscaling::config::Builder::from(sdk)
        .http_client(http)
        .build();
    aws_sdk_autoscaling::Client::from_conf(config)
}

struct Pacing {
    clock: Arc<dyn Clock>,
    interval: Duration,
    last_start: Mutex<Option<Tick>>,
}

impl fmt::Debug for Pacing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pacing")
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct PacedClient {
    inner: SharedHttpClient,
    pacing: Arc<Pacing>,
}

impl HttpClient for PacedClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(PacedConnector {
            inner: self.inner.http_connector(settings, components),
            pacing: Arc::clone(&self.pacing),
        })
    }

    fn validate_base_client_config(
        &self,
        components: &RuntimeComponentsBuilder,
        config: &ConfigBag,
    ) -> Result<(), BoxError> {
        HttpClient::validate_base_client_config(&self.inner, components, config)
    }

    fn validate_final_config(
        &self,
        components: &RuntimeComponents,
        config: &ConfigBag,
    ) -> Result<(), BoxError> {
        HttpClient::validate_final_config(&self.inner, components, config)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.inner.connector_metadata()
    }
}

#[derive(Debug)]
struct PacedConnector {
    inner: SharedHttpConnector,
    pacing: Arc<Pacing>,
}

impl HttpConnector for PacedConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let inner = self.inner.clone();
        let pacing = Arc::clone(&self.pacing);
        HttpConnectorFuture::new(async move {
            let mut last_start = pacing.last_start.lock().await;
            if let Some(last) = *last_start {
                pacing.clock.sleep_until(last + pacing.interval).await;
            }
            *last_start = Some(pacing.clock.monotonic());
            // Do not reserve a future slot: dropping a waiter leaves the previous start intact.
            // Invoke the connector immediately after releasing the lock, before awaiting I/O.
            drop(last_start);
            inner.call(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use gang_clock::TestClock;
    use parking_lot::Mutex as StdMutex;

    use super::*;

    const INTERVAL: Duration = Duration::from_millis(500);

    #[derive(Debug)]
    struct RecordingConnector {
        clock: Arc<TestClock>,
        starts: Arc<StdMutex<Vec<Tick>>>,
    }

    impl HttpConnector for RecordingConnector {
        fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
            self.starts.lock().push(self.clock.monotonic());
            HttpConnectorFuture::new(async {
                let status = StatusCode::try_from(200)
                    .map_err(|error| ConnectorError::other(Box::new(error), None))?;
                Ok(HttpResponse::new(status, SdkBody::empty()))
            })
        }
    }

    type Setup = (
        Arc<TestClock>,
        Arc<StdMutex<Vec<Tick>>>,
        PacedConnector,
        PacedConnector,
    );

    fn setup() -> Setup {
        let clock = Arc::new(TestClock::default());
        let starts = Arc::new(StdMutex::new(Vec::new()));
        let inner = SharedHttpConnector::new(RecordingConnector {
            clock: Arc::clone(&clock),
            starts: Arc::clone(&starts),
        });
        let pacing = Arc::new(Pacing {
            clock: clock.clone(),
            interval: INTERVAL,
            last_start: Mutex::new(None),
        });
        let first = PacedConnector {
            inner: inner.clone(),
            pacing: Arc::clone(&pacing),
        };
        let second = PacedConnector { inner, pacing };
        (clock, starts, first, second)
    }

    fn poll(future: Pin<&mut HttpConnectorFuture>) -> Poll<Result<HttpResponse, ConnectorError>> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[derive(Debug)]
    struct RecordingClient {
        inner: SharedHttpClient,
        clock: Arc<TestClock>,
        starts: Arc<StdMutex<Vec<Tick>>>,
    }

    impl HttpClient for RecordingClient {
        fn http_connector(
            &self,
            settings: &HttpConnectorSettings,
            components: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(RecordingReplayConnector {
                inner: self.inner.http_connector(settings, components),
                clock: Arc::clone(&self.clock),
                starts: Arc::clone(&self.starts),
            })
        }
    }

    #[derive(Debug)]
    struct RecordingReplayConnector {
        inner: SharedHttpConnector,
        clock: Arc<TestClock>,
        starts: Arc<StdMutex<Vec<Tick>>>,
    }

    impl HttpConnector for RecordingReplayConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            self.starts.lock().push(self.clock.monotonic());
            self.inner.call(request)
        }
    }

    #[derive(Debug)]
    struct ControlledSleep {
        starts: Arc<StdMutex<Vec<Tick>>>,
        backoffs: Arc<StdMutex<Vec<(Duration, usize)>>>,
        release: Arc<tokio::sync::Notify>,
    }

    impl aws_sdk_autoscaling::config::AsyncSleep for ControlledSleep {
        fn sleep(&self, duration: Duration) -> aws_sdk_autoscaling::config::Sleep {
            use aws_sdk_autoscaling::config::Sleep;

            if duration > Duration::from_secs(1) {
                // The loader's operation and attempt deadlines must not expire.
                return Sleep::new(std::future::pending());
            }
            let starts = Arc::clone(&self.starts);
            let backoffs = Arc::clone(&self.backoffs);
            let release = Arc::clone(&self.release);
            Sleep::new(async move {
                backoffs.lock().push((duration, starts.lock().len()));
                release.notified().await;
            })
        }
    }

    #[tokio::test]
    async fn sdk_retries_and_client_clones_share_the_attempt_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        use aws_credential_types::Credentials;
        use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
        use aws_smithy_runtime_api::http::{Request, Response};
        use gang_aws_sdk::{ClientSettings, Region, loader};

        const NS: &str = "http://autoscaling.amazonaws.com/doc/2011-01-01/";
        let throttled = format!(
            "<ErrorResponse xmlns=\"{NS}\"><Error><Type>Sender</Type><Code>Throttling</Code><Message>Rate exceeded</Message></Error><RequestId>test</RequestId></ErrorResponse>"
        );
        let success = format!(
            "<DescribeAutoScalingGroupsResponse xmlns=\"{NS}\"><DescribeAutoScalingGroupsResult><AutoScalingGroups/></DescribeAutoScalingGroupsResult><ResponseMetadata><RequestId>test</RequestId></ResponseMetadata></DescribeAutoScalingGroupsResponse>"
        );
        let event =
            |status: u16, body: String| -> Result<ReplayEvent, Box<dyn std::error::Error>> {
                Ok(ReplayEvent::new(
                    Request::new(SdkBody::empty()),
                    Response::new(StatusCode::try_from(status)?, SdkBody::from(body)),
                ))
            };
        let replay = StaticReplayClient::new(vec![
            event(400, throttled)?,
            event(200, success.clone())?,
            event(200, success)?,
        ]);
        let clock = Arc::new(TestClock::default());
        let starts = Arc::new(StdMutex::new(Vec::new()));
        let backoffs = Arc::new(StdMutex::new(Vec::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let region = Region::try_new("us-east-1")?;
        let sdk = loader(&region, &ClientSettings::default())
            .credentials_provider(Credentials::new("AKID", "SECRET", None, None, "test"))
            .http_client(RecordingClient {
                inner: SharedHttpClient::new(replay.clone()),
                clock: Arc::clone(&clock),
                starts: Arc::clone(&starts),
            })
            .sleep_impl(ControlledSleep {
                starts: Arc::clone(&starts),
                backoffs: Arc::clone(&backoffs),
                release: Arc::clone(&release),
            })
            .load()
            .await;
        let client = super::client(&sdk, clock.clone(), INTERVAL);
        let first = client.clone();
        let second = client.clone();
        let mut first_call = Box::pin(first.describe_auto_scaling_groups().send());
        let mut second_call = Box::pin(second.describe_auto_scaling_groups().send());
        let mut cx = Context::from_waker(Waker::noop());

        // The first response is throttled and the SDK parks in its own backoff.
        assert!(first_call.as_mut().poll(&mut cx).is_pending());
        assert_eq!(starts.lock().len(), 1);
        {
            let backoffs = backoffs.lock();
            assert!(
                backoffs
                    .iter()
                    .any(|(duration, requests)| *requests == 1
                        && *duration <= Duration::from_secs(1)),
                "expected a controlled sleep after the first request; recorded sleeps: {backoffs:?}"
            );
        }
        // Start the other clone while the first operation is still in flight.
        assert!(second_call.as_mut().poll(&mut cx).is_pending());
        release.notify_waiters();
        assert!(first_call.as_mut().poll(&mut cx).is_pending());
        clock.advance(INTERVAL);
        assert!(matches!(
            second_call.as_mut().poll(&mut cx),
            Poll::Ready(Ok(_))
        ));
        assert!(first_call.as_mut().poll(&mut cx).is_pending());
        clock.advance(INTERVAL);
        assert!(matches!(
            first_call.as_mut().poll(&mut cx),
            Poll::Ready(Ok(_))
        ));

        let starts = starts.lock();
        assert_eq!(starts.len(), 3);
        assert!(starts.windows(2).all(|pair| pair[1] >= pair[0] + INTERVAL));
        assert_eq!(replay.actual_requests().count(), 3);
        Ok(())
    }

    #[test]
    fn simultaneous_calls_and_retry_attempt_share_one_budget() {
        let (clock, starts, first, second) = setup();
        let mut one = Box::pin(first.call(HttpRequest::empty()));
        let mut two = Box::pin(second.call(HttpRequest::empty()));
        let mut retry = Box::pin(first.call(HttpRequest::empty()));
        assert!(matches!(poll(one.as_mut()), Poll::Ready(Ok(_))));
        assert!(poll(two.as_mut()).is_pending());
        assert!(poll(retry.as_mut()).is_pending());
        clock.advance(
            INTERVAL
                .checked_sub(Duration::from_nanos(1))
                .expect("500ms fixture interval exceeds one nanosecond"),
        );
        assert!(poll(two.as_mut()).is_pending());
        clock.advance(Duration::from_nanos(1));
        assert!(matches!(poll(two.as_mut()), Poll::Ready(Ok(_))));
        assert!(poll(retry.as_mut()).is_pending());
        clock.advance(INTERVAL);
        assert!(matches!(poll(retry.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(
            *starts.lock(),
            vec![
                Tick::ORIGIN,
                Tick::ORIGIN + INTERVAL,
                Tick::ORIGIN + INTERVAL + INTERVAL,
            ]
        );
    }

    #[test]
    fn cancelling_a_waiter_does_not_reserve_a_slot() {
        let (clock, starts, first, second) = setup();
        let mut initial = Box::pin(first.call(HttpRequest::empty()));
        assert!(matches!(poll(initial.as_mut()), Poll::Ready(Ok(_))));
        let mut cancelled = Box::pin(first.call(HttpRequest::empty()));
        assert!(poll(cancelled.as_mut()).is_pending());
        drop(cancelled);
        let mut next = Box::pin(second.call(HttpRequest::empty()));
        assert!(poll(next.as_mut()).is_pending());
        clock.advance(INTERVAL);
        assert!(matches!(poll(next.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(*starts.lock(), vec![Tick::ORIGIN, Tick::ORIGIN + INTERVAL]);
    }

    #[test]
    fn a_long_idle_does_not_accumulate_a_burst() {
        let (clock, starts, first, second) = setup();
        let mut initial = Box::pin(first.call(HttpRequest::empty()));
        assert!(matches!(poll(initial.as_mut()), Poll::Ready(Ok(_))));
        clock.advance(Duration::from_mins(1));
        let mut after_idle = Box::pin(first.call(HttpRequest::empty()));
        let mut next = Box::pin(second.call(HttpRequest::empty()));
        assert!(matches!(poll(after_idle.as_mut()), Poll::Ready(Ok(_))));
        assert!(poll(next.as_mut()).is_pending());
        clock.advance(INTERVAL);
        assert!(matches!(poll(next.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(
            *starts.lock(),
            vec![
                Tick::ORIGIN,
                Tick::ORIGIN + Duration::from_mins(1),
                Tick::ORIGIN + Duration::from_mins(1) + INTERVAL,
            ]
        );
    }
}
