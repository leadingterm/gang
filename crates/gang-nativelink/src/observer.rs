//! Observes client calls and puts the authenticated principal in scheduler baggage.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll};

use gang_spi::{Authenticated, Call, CallOutcome, Observation, ObservationSink, Origin, Rpc};
use gang_types::{BuildKey, Identity, InvocationId, Mnemonic, TargetLabel, ToolDetails};
use http::{Method, Request, Response, StatusCode};
use nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata;
use nativelink_util::origin_event::{BAZEL_METADATA_KEY, request_metadata_from_baggage};
use opentelemetry::baggage::{BaggageExt, KeyValueMetadata};
use opentelemetry::context::{Context, FutureExt};
use tower::{Layer, Service};

/// OpenTelemetry's authenticated end-user baggage key.
pub(crate) const ENDUSER_ID: &str = "enduser.id";

const GET_ACTION_RESULT_PATH: &str = "/build.bazel.remote.execution.v2.ActionCache/GetActionResult";
const UPDATE_ACTION_RESULT_PATH: &str =
    "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult";
const EXECUTE_PATH: &str = "/build.bazel.remote.execution.v2.Execution/Execute";
const GRPC_STATUS_HEADER: &str = "grpc-status";
const GRPC_NOT_FOUND: &[u8] = b"5";
const GRPC_OK: &[u8] = b"0";

/// Farm-wide action-cache lookup counters.
#[derive(Default)]
pub(crate) struct CacheCounters {
    /// Completed action-cache lookups.
    pub(crate) lookups: AtomicU64,
    /// Lookups answered with a cache hit.
    pub(crate) hits: AtomicU64,
}

/// Builds an origin only when all supplied metadata is valid.
pub(crate) fn origin(principal: Arc<Identity>, metadata: &RequestMetadata) -> Option<Origin> {
    let invocation = InvocationId::try_new(&metadata.tool_invocation_id).ok()?;
    let correlated = if metadata.correlated_invocations_id.is_empty() {
        None
    } else {
        Some(InvocationId::try_new(&metadata.correlated_invocations_id).ok()?)
    };
    let target = if metadata.target_id.is_empty() {
        None
    } else {
        Some(TargetLabel::try_new(&metadata.target_id).ok()?)
    };
    let mnemonic = if metadata.action_mnemonic.is_empty() {
        None
    } else {
        Some(Mnemonic::try_new(&metadata.action_mnemonic).ok()?)
    };
    let tool = match &metadata.tool_details {
        Some(details) if !details.tool_name.is_empty() => {
            let text = if details.tool_version.is_empty() {
                details.tool_name.clone()
            } else {
                format!("{} {}", details.tool_name, details.tool_version)
            };
            Some(ToolDetails::try_new(text).ok()?)
        }
        _ => None,
    };
    Some(Origin {
        build: BuildKey {
            principal,
            invocation,
        },
        correlated,
        tool,
        target,
        mnemonic,
    })
}

/// Adds build observations to a client router.
#[derive(Clone)]
pub(crate) struct BuildObserverLayer {
    sink: ObservationSink,
    counters: Arc<CacheCounters>,
}

impl BuildObserverLayer {
    /// Builds a layer sharing the sink and farm-wide counters.
    pub(crate) fn new(sink: ObservationSink, counters: Arc<CacheCounters>) -> Self {
        Self { sink, counters }
    }
}

impl<S> Layer<S> for BuildObserverLayer {
    type Service = BuildObserver<S>;
    fn layer(&self, inner: S) -> Self::Service {
        BuildObserver {
            inner,
            sink: self.sink.clone(),
            counters: Arc::clone(&self.counters),
        }
    }
}

/// Observes response heads without consuming streaming response bodies.
#[derive(Clone)]
pub(crate) struct BuildObserver<S> {
    inner: S,
    sink: ObservationSink,
    counters: Arc<CacheCounters>,
}

impl<S, B, R> Service<Request<B>> for BuildObserver<S>
where
    S: Service<Request<B>, Response = Response<R>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: 'static,
    B: Send + 'static,
    R: 'static,
{
    type Response = Response<R>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let rpc = if request.method() == Method::POST {
            match request.uri().path() {
                GET_ACTION_RESULT_PATH => Some(Rpc::GetActionResult),
                UPDATE_ACTION_RESULT_PATH => Some(Rpc::UpdateActionResult),
                EXECUTE_PATH => Some(Rpc::Execute),
                _ => None,
            }
        } else {
            None
        };
        let principal = request
            .extensions()
            .get::<Authenticated>()
            .map(|auth| Arc::clone(&auth.identity));
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let sink = self.sink.clone();
        let counters = Arc::clone(&self.counters);
        Box::pin(async move {
            // OtlpLayer attaches the extracted context when it polls this future, not when it calls us.
            let context = Context::current();
            let origin = rpc.and(principal.as_ref()).and_then(|principal| {
                context
                    .baggage()
                    .get(BAZEL_METADATA_KEY)
                    .and_then(|value| request_metadata_from_baggage(value.as_str()).ok())
                    .and_then(|metadata| origin(Arc::clone(principal), &metadata))
            });
            let context = if rpc.is_some() {
                let baggage = context.baggage();
                let identity = principal
                    .map(|principal| KeyValueMetadata::new(ENDUSER_ID, principal.to_string(), ""));
                let others = baggage
                    .iter()
                    .filter(|(key, _)| key.as_str() != ENDUSER_ID)
                    .map(|(key, (value, metadata))| {
                        KeyValueMetadata::new(key.clone(), value.clone(), metadata.clone())
                    });
                context.with_baggage(identity.into_iter().chain(others))
            } else {
                context
            };
            let response = async move { inner.call(request).await }
                .with_context(context)
                .await?;
            if let Some(rpc) = rpc {
                let outcome = match (
                    response.status(),
                    response.headers().get(GRPC_STATUS_HEADER),
                ) {
                    (StatusCode::OK, None) => CallOutcome::Ok,
                    (StatusCode::OK, Some(value)) if value.as_bytes() == GRPC_OK => CallOutcome::Ok,
                    (StatusCode::OK, Some(value)) if value.as_bytes() == GRPC_NOT_FOUND => {
                        CallOutcome::NotFound
                    }
                    _ => CallOutcome::Error,
                };
                if rpc == Rpc::GetActionResult {
                    counters.lookups.fetch_add(1, Ordering::Relaxed);
                    if outcome == CallOutcome::Ok {
                        counters.hits.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if let Some(origin) = origin {
                    sink.record(Observation::Call(Call {
                        origin,
                        rpc,
                        outcome,
                    }));
                }
            }
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_rejects_invalid_optional_metadata() {
        let identity = Arc::new("local:alice".parse::<Identity>().expect("identity"));
        let mut metadata = RequestMetadata {
            tool_invocation_id: "build".to_owned(),
            ..Default::default()
        };
        let valid = origin(Arc::clone(&identity), &metadata).expect("valid origin");
        assert!(valid.correlated.is_none());
        assert!(valid.target.is_none());
        assert!(valid.mnemonic.is_none());
        assert!(valid.tool.is_none());
        metadata.target_id = "x".repeat(1025);
        assert!(origin(Arc::clone(&identity), &metadata).is_none());
        metadata.target_id.clear();
        metadata.tool_invocation_id.clear();
        assert!(origin(identity, &metadata).is_none());
    }

    #[test]
    fn tool_without_version_has_no_trailing_space() {
        let identity = Arc::new("local:alice".parse::<Identity>().expect("identity"));
        let metadata = RequestMetadata {
            tool_invocation_id: "build".to_owned(),
            tool_details: Some(
                nativelink_proto::build::bazel::remote::execution::v2::ToolDetails {
                    tool_name: "bazel".to_owned(),
                    tool_version: String::new(),
                },
            ),
            ..Default::default()
        };
        assert_eq!(
            origin(identity, &metadata)
                .expect("origin")
                .tool
                .as_ref()
                .map(gang_types::ToolDetails::as_str),
            Some("bazel")
        );
    }
}
