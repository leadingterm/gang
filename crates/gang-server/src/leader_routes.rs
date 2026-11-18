//! Forward leader-owned API routes without buffering their request or response bodies.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures::stream;
use gang_insight::Outcome;
use gang_mtls::{ControllerProof, PROOF_HEADER};
use gang_spi::{ApiRequestBody, ApiResponseBody, BoxedError};
use gang_tls::{HttpClient, HttpError, HttpReply, HttpRequest, RequestBody};
use gang_types::Url;
use gang_wire::ErrorReason;
use http::header::{CONNECTION, HOST, HeaderMap, HeaderName};
use http::{HeaderValue, Method, Request, Response, Uri};
use http_body_util::{BodyExt, StreamBody};

use crate::builds_forward::Uplink;
use crate::leadership::Leading;

/// Marks a request that has already crossed a follower relay.
pub(crate) const RELAYED_HEADER: &str = "x-gang-relayed";

/// The leader endpoint and the client used by a follower to reach it.
#[derive(Clone)]
pub(crate) struct LeaderRoutes {
    leading: Leading,
    upstream: Arc<HttpClient>,
    execution_endpoint: Url,
    configuration_error_logged: Arc<AtomicBool>,
}

impl LeaderRoutes {
    /// Builds forwarding using a client configured for the execution endpoint's transport.
    pub(crate) fn new(leading: Leading, upstream: HttpClient, execution_endpoint: Url) -> Self {
        Self {
            leading,
            upstream: Arc::new(upstream),
            execution_endpoint,
            configuration_error_logged: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether requests should be served by the leader instead of locally.
    pub(crate) fn should_forward(&self) -> bool {
        !self.leading.get()
    }

    /// Relays the request and response, including streaming bodies.
    pub(crate) async fn forward(
        &self,
        mut request: Request<ApiRequestBody>,
    ) -> Response<ApiResponseBody> {
        if request.headers().contains_key(RELAYED_HEADER) {
            return unavailable();
        }
        let path = request
            .uri()
            .path_and_query()
            .map_or("/", |value| value.as_str());
        let target = format!(
            "{}{}",
            self.execution_endpoint.origin().ascii_serialization(),
            path
        );
        let Ok(uri) = target.parse::<Uri>() else {
            return unavailable();
        };
        *request.uri_mut() = uri;
        *request.version_mut() = http::Version::HTTP_11;
        strip_hop_headers(request.headers_mut());
        request
            .headers_mut()
            .insert(RELAYED_HEADER, http::HeaderValue::from_static("1"));
        request.headers_mut().remove(HOST);
        let mut upstream = match self.upstream.open_request(request).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, "leader request failed");
                return unavailable();
            }
        };
        let status = upstream.status;
        let mut headers = std::mem::take(&mut upstream.headers);
        strip_hop_headers(&mut headers);
        let frames = stream::try_unfold(upstream, |mut upstream| async move {
            match upstream.next_chunk().await {
                Ok(Some(bytes)) => Ok(Some((http_body::Frame::data(bytes), upstream))),
                Ok(None) => Ok(None),
                Err(error) => Err(Box::new(error) as BoxedError),
            }
        });
        let mut response = Response::new(StreamBody::new(frames).boxed_unsync());
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response
    }
}

impl Uplink for LeaderRoutes {
    async fn send(&self, proof: &ControllerProof, body: Bytes) -> Outcome {
        let url = match self
            .execution_endpoint
            .join(gang_wire::routes::INGEST_BUILDS.template)
        {
            Ok(url) => url,
            Err(error) => {
                if !self
                    .configuration_error_logged
                    .swap(true, Ordering::Relaxed)
                {
                    tracing::error!(%error, "invalid leader ingest endpoint");
                }
                return Outcome::NotApplied;
            }
        };
        let Ok(mut value) = HeaderValue::from_str(proof.as_str()) else {
            if !self
                .configuration_error_logged
                .swap(true, Ordering::Relaxed)
            {
                tracing::error!("invalid controller proof header");
            }
            return Outcome::NotApplied;
        };
        value.set_sensitive(true);
        let headers = [(HeaderName::from_static(PROOF_HEADER), value)];
        let result = self
            .upstream
            .send(HttpRequest {
                method: Method::POST,
                url: &url,
                bearer: None,
                headers: &headers,
                body: Some(RequestBody {
                    content_type: "application/json",
                    bytes: body,
                }),
            })
            .await;
        if matches!(
            &result,
            Err(HttpError::UnsupportedScheme { .. } | HttpError::Request(_) | HttpError::Tls(_))
        ) && !self
            .configuration_error_logged
            .swap(true, Ordering::Relaxed)
        {
            tracing::error!("leader ingest transport configuration is invalid");
        }
        classify(result)
    }
}

pub(crate) fn classify(result: Result<HttpReply, HttpError>) -> Outcome {
    match result {
        Ok(reply) if reply.status.is_success() => Outcome::Accepted,
        Ok(_) => Outcome::NotApplied,
        Err(HttpError::UnsupportedScheme { .. } | HttpError::Request(_) | HttpError::Tls(_)) => {
            Outcome::NotApplied
        }
        Err(HttpError::Connect(error)) if error.is_connect() => Outcome::NotApplied,
        Err(
            HttpError::Connect(_)
            | HttpError::TimedOut
            | HttpError::Body(_)
            | HttpError::BodyTooLarge { .. },
        ) => Outcome::Unknown,
    }
}

/// Removes both the standard hop-by-hop headers and those named by Connection.
fn strip_hop_headers(headers: &mut HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "proxy-connection",
    ] {
        headers.remove(name);
    }
}

fn unavailable() -> Response<ApiResponseBody> {
    crate::api::failure(ErrorReason::DependencyUnavailable.into())
}
