//! Observes client calls: counts cache traffic farm-wide, records the calls of builds, and puts
//! the authenticated principal in scheduler baggage.

#![allow(dead_code)] // Wired by controller runtime (WP D2).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll};

use axum::body::Body;
use base64::Engine as _;
use base64::prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD};
use bytes::Bytes;
use gang_reapi::build::bazel::remote::execution::v2::RequestMetadata;
use gang_spi::{
    Authenticated, Call, CallOutcome, Observation, ObservationSink, Origin, Rpc, Traffic,
    Transferred,
};
use gang_types::{BuildKey, Identity, InvocationId, Mnemonic, TargetLabel, ToolDetails};
use http::{HeaderValue, Method, Request, Response, StatusCode};
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use opentelemetry::baggage::{BaggageExt, KeyValueMetadata};
use opentelemetry::context::{Context, FutureExt};
use prost::Message as _;
use tower::{Layer, Service};

/// OpenTelemetry's authenticated end-user baggage key.
#[allow(dead_code)] // Wired by controller runtime (WP D2).
pub(crate) const ENDUSER_ID: &str = "enduser.id";

/// OpenTelemetry baggage key used to carry Bazel `RequestMetadata`.
const BAZEL_METADATA_KEY: &str = "bazel.metadata";
/// Standard gRPC metadata header carrying serialized Bazel `RequestMetadata`.
const REQUEST_METADATA_HEADER: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";

/// The cache services: every method under these paths is a cache call.
const ACTION_CACHE: &str = "/build.bazel.remote.execution.v2.ActionCache/";
const CONTENT_STORAGE: &str = "/build.bazel.remote.execution.v2.ContentAddressableStorage/";
const BYTE_STREAM: &str = "/google.bytestream.ByteStream/";
const EXECUTE_PATH: &str = "/build.bazel.remote.execution.v2.Execution/Execute";
const GRPC_STATUS_HEADER: &str = "grpc-status";
const GRPC_NOT_FOUND: &[u8] = b"5";
const GRPC_OK: &[u8] = b"0";

/// Decodes a base64-encoded `RequestMetadata` protobuf (unpadded or padded).
fn decode_request_metadata(encoded: &str) -> Option<RequestMetadata> {
    let decoded = BASE64_STANDARD_NO_PAD
        .decode(encoded.as_bytes())
        .or_else(|_| BASE64_STANDARD.decode(encoded.as_bytes()))
        .ok()?;
    RequestMetadata::decode(decoded.as_slice()).ok()
}

/// The observed call a POST to `path` makes, if any.
fn classify(path: &str) -> Option<Rpc> {
    if path == EXECUTE_PATH {
        return Some(Rpc::Execute);
    }
    if let Some(method) = path.strip_prefix(ACTION_CACHE) {
        return Some(match method {
            "GetActionResult" => Rpc::GetActionResult,
            "UpdateActionResult" => Rpc::UpdateActionResult,
            _ => Rpc::Cas,
        });
    }
    (path.starts_with(CONTENT_STORAGE) || path.starts_with(BYTE_STREAM)).then_some(Rpc::Cas)
}

/// The outcome a `grpc-status` value states.
fn grpc_outcome(status: &HeaderValue) -> CallOutcome {
    match status.as_bytes() {
        GRPC_OK => CallOutcome::Ok,
        GRPC_NOT_FOUND => CallOutcome::NotFound,
        _ => CallOutcome::Error,
    }
}

/// The outcome the response head states: HTTP 200, with `grpc-status` absent or stating it.
fn head_outcome<B>(response: &Response<B>) -> CallOutcome {
    if response.status() != StatusCode::OK {
        return CallOutcome::Error;
    }
    response
        .headers()
        .get(GRPC_STATUS_HEADER)
        .map_or(CallOutcome::Ok, grpc_outcome)
}

/// The message bytes `frame` carries: its data, or none for trailers.
fn data_len(frame: &Frame<Bytes>) -> u64 {
    frame
        .data_ref()
        .map_or(0, |data| u64::try_from(data.len()).unwrap_or(u64::MAX))
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
#[allow(dead_code)] // Wired by controller runtime (WP D2).
pub(crate) struct BuildObserverLayer {
    sink: ObservationSink,
}

#[allow(dead_code)] // Wired by controller runtime (WP D2).
impl BuildObserverLayer {
    /// Builds a layer that counts traffic in, and records calls to, `sink`.
    pub(crate) fn new(sink: ObservationSink) -> Self {
        Self { sink }
    }
}

impl<S> Layer<S> for BuildObserverLayer {
    type Service = BuildObserver<S>;
    fn layer(&self, inner: S) -> Self::Service {
        BuildObserver {
            inner,
            sink: self.sink.clone(),
        }
    }
}

/// Observes calls as their bodies stream: a cache call once its response body ends or is
/// dropped, `Execute` at its response head.
#[derive(Clone)]
pub(crate) struct BuildObserver<S> {
    inner: S,
    sink: ObservationSink,
}

impl<S> Service<Request<Body>> for BuildObserver<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let rpc = if request.method() == Method::POST {
            classify(request.uri().path())
        } else {
            None
        };
        let principal = request
            .extensions()
            .get::<Authenticated>()
            .map(|auth| Arc::clone(&auth.identity));
        let header_metadata = request
            .headers()
            .get(REQUEST_METADATA_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(decode_request_metadata);
        // A cache call counts the request bytes the service reads; `Execute` counts none.
        let uploaded =
            matches!(rpc, Some(rpc) if rpc != Rpc::Execute).then(|| Arc::new(AtomicU64::new(0)));
        let request = match &uploaded {
            Some(read) => request.map(|inner| {
                Body::new(Counting {
                    inner,
                    read: Arc::clone(read),
                })
            }),
            None => request,
        };
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let sink = self.sink.clone();
        Box::pin(async move {
            let context = Context::current();
            let request_metadata = context
                .baggage()
                .get(BAZEL_METADATA_KEY)
                .and_then(|value| decode_request_metadata(value.as_str()))
                .or(header_metadata);
            let origin = match (rpc, principal.as_ref(), request_metadata.as_ref()) {
                (Some(_), Some(principal), Some(metadata)) => {
                    origin(Arc::clone(principal), metadata)
                }
                _ => None,
            };
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
            let called = async move { inner.call(request).await }.with_context(context);
            match (rpc, uploaded) {
                (Some(rpc), Some(uploaded)) => {
                    // Created before the call so that cancellation or failure before the head
                    // still counts and records it, as an error.
                    let mut recording = Recording::new(sink, rpc, origin, uploaded);
                    let response = called.await?;
                    recording.outcome = head_outcome(&response);
                    Ok(response.map(|inner| Body::new(ObservedBody { inner, recording })))
                }
                // `Execute` streams until its action ends, so it is recorded at its head.
                (Some(rpc), None) => {
                    let response = called.await?;
                    if let Some(origin) = origin {
                        sink.record(Observation::Call(Call {
                            origin,
                            rpc,
                            outcome: head_outcome(&response),
                            transferred: Transferred::default(),
                        }));
                    }
                    Ok(response)
                }
                (None, _) => called.await,
            }
        })
    }
}

/// A request body that adds the message bytes the service reads to a shared counter.
struct Counting {
    inner: Body,
    read: Arc<AtomicU64>,
}

impl HttpBody for Counting {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &polled {
            this.read.fetch_add(data_len(frame), Ordering::Relaxed);
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// A cache call, counted and recorded exactly once: by [`Recording::finish`], or as an error when
/// dropped unfinished (cancelled or failed before its response head, or cut short mid-stream).
struct Recording {
    sink: ObservationSink,
    rpc: Rpc,
    /// The build the call belongs to, when its metadata names one; taken when recorded.
    origin: Option<Origin>,
    /// The head's outcome, until a trailer states another; an error until the head arrives.
    outcome: CallOutcome,
    /// The request message bytes the service read.
    uploaded: Arc<AtomicU64>,
    /// The response message bytes sent so far.
    downloaded: u64,
    /// Whether the call was counted and recorded.
    finished: bool,
}

impl Recording {
    fn new(
        sink: ObservationSink,
        rpc: Rpc,
        origin: Option<Origin>,
        uploaded: Arc<AtomicU64>,
    ) -> Self {
        Self {
            sink,
            rpc,
            origin,
            outcome: CallOutcome::Error,
            uploaded,
            downloaded: 0,
            finished: false,
        }
    }

    /// Counts the call farm-wide and records it for its build, unless already done. A call that
    /// did not reach its end is an error and never a hit.
    fn finish(&mut self, ended: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        let outcome = if ended {
            self.outcome
        } else {
            CallOutcome::Error
        };
        let transferred = Transferred {
            uploaded: self.uploaded.load(Ordering::Relaxed),
            downloaded: self.downloaded,
        };
        let lookup = self.rpc == Rpc::GetActionResult;
        self.sink.count(Traffic {
            lookups: u64::from(lookup),
            hits: u64::from(lookup && outcome == CallOutcome::Ok),
            uploaded: transferred.uploaded,
            downloaded: transferred.downloaded,
        });
        if let Some(origin) = self.origin.take() {
            self.sink.record(Observation::Call(Call {
                origin,
                rpc: self.rpc,
                outcome,
                transferred,
            }));
        }
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        self.finish(false);
    }
}

/// A cache call's response body. Its recording ends when the body ends, or when the body is
/// dropped.
struct ObservedBody {
    inner: Body,
    recording: Recording,
}

impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        let recording = &mut this.recording;
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                recording.downloaded = recording.downloaded.saturating_add(data_len(frame));
                // Trailers are the last frame; the server may not poll the body again.
                if let Some(trailers) = frame.trailers_ref() {
                    if let Some(status) = trailers.get(GRPC_STATUS_HEADER) {
                        recording.outcome = grpc_outcome(status);
                    }
                    recording.finish(true);
                } else if this.inner.is_end_stream() {
                    recording.finish(true);
                }
            }
            Poll::Ready(None) => recording.finish(true),
            Poll::Ready(Some(Err(_))) => recording.finish(false),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for ObservedBody {
    /// A body dropped after its last frame ended; one dropped before was cut short.
    fn drop(&mut self) {
        let ended = self.inner.is_end_stream();
        self.recording.finish(ended);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use axum::{Extension, Router, routing::post};
    use gang_reapi::build::bazel::remote::execution::v2::ToolDetails;
    use gang_types::{Principal, ProviderName};
    use tower::ServiceExt;

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
            tool_details: Some(ToolDetails {
                tool_name: "bazel".to_owned(),
                tool_version: String::new(),
            }),
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

    /// Simulated flaws: observing only the action-cache lookups and uploads; matching a service
    /// name as a bare prefix; wrapping the execution service like a cache.
    #[test]
    fn every_cache_service_method_is_observed_and_execute_alone_of_the_rest() {
        let cases = [
            ("ActionCache/GetActionResult", Some(Rpc::GetActionResult)),
            (
                "ActionCache/UpdateActionResult",
                Some(Rpc::UpdateActionResult),
            ),
            ("ContentAddressableStorage/FindMissingBlobs", Some(Rpc::Cas)),
            ("ContentAddressableStorage/BatchUpdateBlobs", Some(Rpc::Cas)),
            ("ContentAddressableStorage/BatchReadBlobs", Some(Rpc::Cas)),
            ("ContentAddressableStorage/GetTree", Some(Rpc::Cas)),
            ("Execution/Execute", Some(Rpc::Execute)),
            ("Execution/WaitExecution", None),
            ("Capabilities/GetCapabilities", None),
            ("ActionCacheMirror/GetActionResult", None),
        ];
        for (method, rpc) in cases {
            let path = format!("/build.bazel.remote.execution.v2.{method}");
            assert_eq!(classify(&path), rpc, "{path}");
        }
        for method in ["Read", "Write", "QueryWriteStatus"] {
            let path = format!("/google.bytestream.ByteStream/{method}");
            assert_eq!(classify(&path), Some(Rpc::Cas), "{path}");
        }
        for path in ["/google.longrunning.Operations/GetOperation", "/v1/status"] {
            assert_eq!(classify(path), None, "{path}");
        }
    }

    #[tokio::test]
    async fn observer_records_calls_from_request_metadata_header() {
        let (sink, mut observations) =
            ObservationSink::channel(NonZeroUsize::new(4).expect("nonzero"), NonZeroUsize::MIN);
        let auth = Authenticated {
            identity: Arc::new(Identity::new(
                ProviderName::try_new("local").expect("provider"),
                Principal::try_new("alice").expect("principal"),
            )),
            valid_until: None,
        };
        let metadata = RequestMetadata {
            tool_invocation_id: "invocation".to_owned(),
            target_id: "//pkg:lib".to_owned(),
            action_mnemonic: "Rustc".to_owned(),
            ..Default::default()
        };
        let router = Router::new()
            .route(
                "/build.bazel.remote.execution.v2.ActionCache/GetActionResult",
                post(|| async { ([("grpc-status", "0")], "") }),
            )
            .layer(BuildObserverLayer::new(sink))
            .layer(Extension(auth));
        let response = router
            .oneshot(
                Request::post("/build.bazel.remote.execution.v2.ActionCache/GetActionResult")
                    .header(
                        REQUEST_METADATA_HEADER,
                        BASE64_STANDARD_NO_PAD.encode(metadata.encode_to_vec()),
                    )
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router");
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        let delivery = observations.next().await.expect("recorded call");
        let Some(Observation::Call(call)) = delivery.into_observation() else {
            panic!("expected Call observation");
        };
        assert_eq!(call.rpc, Rpc::GetActionResult);
        assert_eq!(call.outcome, CallOutcome::Ok);
        assert_eq!(call.origin.build.invocation.as_str(), "invocation");
    }
}
