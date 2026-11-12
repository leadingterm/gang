//! Plain-HTTP client for the mock IMDS, used by the worker's node lifecycle and credential ports.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue, Method, StatusCode};
use bytes::Bytes;
use gang_clock::{Clock, Timestamp};
use gang_spi::{InstanceIdentity, PreemptionNotice};
use gang_tls::{HttpClient, HttpError, HttpProtocol, HttpRequest, HttpSettings, HttpTransport};
use gang_types::{
    AccessToken, FleetName, InstanceId, Isa, NonZeroBytes, NonZeroDuration, Proof, Url,
};
use serde::Deserialize;

use crate::errors::LocalError;
use crate::imds::INSTANCE_HEADER;
use crate::worker_identity::WorkerIdentity;

/// IMDS documents and tokens are small, so 64 KiB bounds each response body.
const IMDS_MAX_BODY: NonZeroBytes = NonZeroBytes::kib(64);

/// Client for the mock IMDS over plain HTTP on the loopback address it listens on.
///
/// Every instance request carries the instance id and its secret, so the client holds no
/// per-instance state and never reuses credentials beyond the worker identity it is given.
pub(crate) struct ImdsClient {
    http: HttpClient,
    address: SocketAddr,
}

/// The identity document the mock IMDS serves; extra fields are ignored.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityDocument {
    instance_id: String,
    fleet: String,
    architecture: String,
}

/// The spot interruption notice; the action is always `terminate`, so only the time is read.
#[derive(Deserialize)]
struct PreemptionDocument {
    terminate_at_unix_ms: u64,
}

impl ImdsClient {
    /// Creates a client for the mock IMDS at `address`; no connection is opened until a call.
    /// Each request must finish within `request_timeout`.
    pub(crate) fn new(
        address: SocketAddr,
        request_timeout: NonZeroDuration,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, LocalError> {
        let http = HttpClient::try_new(&HttpSettings {
            transport: HttpTransport::Plain,
            protocol: HttpProtocol::Negotiated,
            timeout: request_timeout,
            max_body: IMDS_MAX_BODY,
            clock,
        })
        .map_err(imds_error)?;
        Ok(Self { http, address })
    }

    /// Fetches the instance's id and instruction set.
    pub(crate) async fn identity(
        &self,
        worker: &WorkerIdentity,
    ) -> Result<InstanceIdentity, LocalError> {
        let (status, body) = self
            .get(worker, "/latest/dynamic/instance-identity/document")
            .await?;
        let document: IdentityDocument =
            serde_json::from_slice(&success(status, body)?).map_err(imds_error)?;
        Ok(InstanceIdentity {
            instance: InstanceId::try_new(document.instance_id).map_err(imds_error)?,
            fleet: FleetName::try_new(document.fleet).map_err(imds_error)?,
            isa: Isa::from_str(&document.architecture).map_err(imds_error)?,
        })
    }

    /// Fetches the pending spot interruption; `None` means the mock IMDS has announced nothing.
    pub(crate) async fn preemption(
        &self,
        worker: &WorkerIdentity,
    ) -> Result<Option<PreemptionNotice>, LocalError> {
        let (status, body) = self
            .get(worker, "/latest/meta-data/spot/instance-action")
            .await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let document: PreemptionDocument =
            serde_json::from_slice(&success(status, body)?).map_err(imds_error)?;
        Ok(Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(document.terminate_at_unix_ms),
        }))
    }

    /// Fetches a workload JWT minted for the instance.
    pub(crate) async fn workload_token(
        &self,
        worker: &WorkerIdentity,
    ) -> Result<Proof, LocalError> {
        let (status, body) = self
            .get(worker, "/latest/meta-data/gang/workload-token")
            .await?;
        Ok(Proof::new(text(&success(status, body)?)?))
    }

    /// Sends an authorized GET for `path` on behalf of the worker's instance.
    async fn get(
        &self,
        worker: &WorkerIdentity,
        path: &str,
    ) -> Result<(StatusCode, Bytes), LocalError> {
        let url = Url::parse(&format!("http://{}{path}", self.address)).map_err(imds_error)?;
        let token = AccessToken::new(worker.secret.expose());
        let headers = [(
            HeaderName::from_static(INSTANCE_HEADER),
            HeaderValue::from_str(worker.instance.as_str()).map_err(imds_error)?,
        )];
        let reply = self
            .http
            .send(HttpRequest {
                method: Method::GET,
                url: &url,
                bearer: Some(&token),
                headers: &headers,
                body: None,
            })
            .await;
        match reply {
            Ok(reply) => Ok((reply.status, reply.body)),
            Err(HttpError::BodyTooLarge { status, .. }) if !status.is_success() => {
                Ok((status, Bytes::new()))
            }
            Err(error) => Err(imds_error(error)),
        }
    }
}

/// Passes a 2xx body through and turns any other status into `LocalError::ImdsStatus`.
fn success(status: StatusCode, body: Bytes) -> Result<Bytes, LocalError> {
    if status.is_success() {
        Ok(body)
    } else {
        Err(LocalError::ImdsStatus {
            status: status.as_u16(),
        })
    }
}

/// Decodes a response body as UTF-8 text.
fn text(body: &[u8]) -> Result<String, LocalError> {
    std::str::from_utf8(body)
        .map(str::to_owned)
        .map_err(imds_error)
}

/// Wraps any transport, body, URI, or decoding failure as an IMDS error.
fn imds_error(error: impl std::error::Error + Send + Sync + 'static) -> LocalError {
    LocalError::Imds(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{Router, routing::get};
    use gang_clock::SystemClock;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn rejects_an_oversized_response() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = Router::new().route(
            "/latest/meta-data/gang/workload-token",
            get(|| async { vec![b'x'; 64 * 1024 + 1] }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let client = ImdsClient::new(
            address,
            NonZeroDuration::from_secs(5),
            Arc::new(SystemClock::new()),
        )?;
        let worker = WorkerIdentity::from_values(Some("local-1"), Some("secret"))?;

        let error = client
            .workload_token(&worker)
            .await
            .err()
            .ok_or("expected body limit")?;
        assert!(matches!(
            error,
            LocalError::Imds(source)
                if matches!(source.downcast_ref::<HttpError>(), Some(HttpError::BodyTooLarge { .. }))
        ));
        server.abort();
        Ok(())
    }
}
