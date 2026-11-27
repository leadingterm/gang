//! Fresh capabilities and complete direct admission-refusal probes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use gang_clock::SystemClock;
use gang_reapi::build::bazel::remote::execution::v2::ServerCapabilities;
use gang_types::Url;
use http::{Method, Request, StatusCode, Version};
use prost::Message;
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio_rustls::TlsConnector;

use crate::balancer::proxy_v2_tcp_header;
use crate::errors::{E2eError, Result};
use crate::identity::Credentials;
use crate::tls_client::{self, ALPN_H2};

const CAPABILITIES_PATH: &str = "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities";
// Same 16 MiB buffered-response bound as the harness HTTP client.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
// Exact wire admission marker; neither arbitrary truthy values nor duplicates are evidence.
const REQUEST_NOT_STARTED_HEADER: &str = "gang-request-not-started";

/// One physical controller public listener, independent of the original NLB authority.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DirectTarget {
    pub address: SocketAddr,
    pub proxy_protocol_v2: bool,
}

/// Secret-free status and admission marker of one complete authentication response.
pub(crate) struct AuthenticationObservation {
    pub(crate) status: u16,
    pub(crate) marker: bool,
}

struct DirectExchange {
    request: Request<()>,
    payload: Bytes,
}

struct DirectReply {
    status: u16,
    headers: http::HeaderMap,
    trailers: http::HeaderMap,
}

/// Observes status and the exact admission marker after the complete response body and trailers.
/// A connection failure or unfinished response is an error, never a successful refusal.
pub(crate) fn probe_authentication(
    endpoint: &Url,
    credentials: &Credentials,
    target: DirectTarget,
    body: Bytes,
    timeout: Duration,
) -> Result<AuthenticationObservation> {
    let reply = direct_probe(
        endpoint,
        credentials,
        target,
        authentication_exchange(endpoint, body)?,
        timeout,
    )?;
    Ok(AuthenticationObservation {
        status: reply.status,
        marker: refusal_marker(&reply.headers),
    })
}

/// Requires a complete, explicitly not-started HTTP 503 on the original public listener.
pub(crate) fn probe_refusal(
    endpoint: &Url,
    credentials: &Credentials,
    target: DirectTarget,
    body: Bytes,
    timeout: Duration,
) -> Result<AuthenticationObservation> {
    let reply = direct_probe(
        endpoint,
        credentials,
        target,
        authentication_exchange(endpoint, body)?,
        timeout,
    )?;
    decode_refusal(&reply)
}

fn decode_refusal(reply: &DirectReply) -> Result<AuthenticationObservation> {
    if reply.status != 503
        || !refusal_marker(&reply.headers)
        || reply.trailers.contains_key(REQUEST_NOT_STARTED_HEADER)
    {
        return Err(E2eError::system(format!(
            "original controller did not return complete not-started503: status={}",
            reply.status,
        )));
    }
    Ok(AuthenticationObservation {
        status: reply.status,
        marker: true,
    })
}

/// Probes a public route with the original authority, bearer and actual target framing.
pub(crate) fn probe_public(
    endpoint: &Url,
    credentials: &Credentials,
    target: DirectTarget,
    path: &str,
    timeout: Duration,
) -> Result<String> {
    let reply = direct_probe(
        endpoint,
        credentials,
        target,
        public_exchange(endpoint, credentials, path)?,
        timeout,
    )?;
    let grpc_status = reply
        .trailers
        .get("grpc-status")
        .or_else(|| reply.headers.get("grpc-status"))
        .and_then(canonical_grpc_status);
    Ok(format!(
        "HTTP/2 status={} grpc-status={grpc_status:?} not-started={}",
        reply.status,
        refusal_marker(&reply.headers)
    ))
}

fn public_exchange(
    endpoint: &Url,
    credentials: &Credentials,
    path: &str,
) -> Result<DirectExchange> {
    let grpc = path == CAPABILITIES_PATH;
    let uri = endpoint
        .join(path)
        .map_err(E2eError::system_from("forming public diagnostic URI"))?
        .to_string()
        .replacen("grpcs://", "https://", 1);
    let request = Request::builder()
        .method(if grpc { Method::POST } else { Method::GET })
        .uri(uri)
        .version(Version::HTTP_2)
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer {}", credentials.token),
        )
        .header(
            http::header::CONTENT_TYPE,
            if grpc {
                "application/grpc"
            } else {
                "application/json"
            },
        )
        .header("te", "trailers")
        .body(())
        .map_err(|_| E2eError::system("building authenticated public diagnostic"))?;
    Ok(DirectExchange {
        request,
        payload: if grpc {
            Bytes::from_static(&[0; 5])
        } else {
            Bytes::new()
        },
    })
}

fn direct_h2_error(stage: &'static str) -> impl FnOnce(h2::Error) -> E2eError {
    move |error| {
        // Peer debug data and arbitrary I/O messages may contain secrets. Protocol reasons,
        // error kinds and OS error numbers identify the real cause without reflecting bytes.
        let io = error
            .get_io()
            .map(|error| (error.kind(), error.raw_os_error()));
        E2eError::system(format!(
            "{stage}: h2 reason={:?} io={io:?} remote={} reset={} go-away={}",
            error.reason(),
            error.is_remote(),
            error.is_reset(),
            error.is_go_away(),
        ))
    }
}

fn direct_probe(
    endpoint: &Url,
    credentials: &Credentials,
    target: DirectTarget,
    exchange: DirectExchange,
    timeout: Duration,
) -> Result<DirectReply> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(E2eError::io("starting direct probe runtime"))?;
    runtime.block_on(direct_over(
        endpoint,
        credentials,
        target,
        exchange,
        timeout,
        async {
            let tcp = TcpStream::connect(target.address)
                .await
                .map_err(|_| E2eError::system("connecting direct probe target"))?;
            let source = tcp
                .local_addr()
                .map_err(|_| E2eError::system("reading direct probe source"))?;
            Ok((tcp, source))
        },
    ))
}

async fn direct_over<IO>(
    endpoint: &Url,
    credentials: &Credentials,
    target: DirectTarget,
    exchange: DirectExchange,
    timeout: Duration,
    transport: impl Future<Output = Result<(IO, SocketAddr)>>,
) -> Result<DirectReply>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tls_client::within(timeout, "receiving direct probe", async {
        let server_name = direct_server_name(endpoint)?;
        let config = tls_client::client_config(
            credentials.authority.as_deref(),
            &[ALPN_H2],
            Arc::new(SystemClock::new()),
        )?;
        let (mut io, source) = transport.await?;
        if target.proxy_protocol_v2 {
            let (SocketAddr::V4(source), SocketAddr::V4(destination)) = (source, target.address)
            else {
                return Err(E2eError::system("direct probe PROXY framing requires IPv4"));
            };
            io.write_all(&proxy_v2_tcp_header(source, destination))
                .await
                .map_err(|_| E2eError::system("writing direct probe PROXY header"))?;
        }
        let tls = TlsConnector::from(Arc::new(config))
            .connect(server_name, io)
            .await
            .map_err(|_| E2eError::system("connecting direct probe TLS"))?;
        let (sender, connection) = h2::client::handshake(tls)
            .await
            .map_err(direct_h2_error("connecting direct probe HTTP/2"))?;
        // The deadline owns the connection driver.
        let mut drivers = JoinSet::new();
        drivers.spawn(connection);
        let response = receive_direct(sender, exchange);
        tokio::pin!(response);
        tokio::select! {
            biased;
            result = &mut response => result,
            driver = drivers.join_next() => match driver {
                Some(Ok(Err(error))) => Err(direct_h2_error("driving direct probe connection")(error)),
                Some(Err(error)) => Err(E2eError::system(format!(
                    "joining direct probe driver: cancelled={} panicked={}",
                    error.is_cancelled(), error.is_panic(),
                ))),
                Some(Ok(Ok(()))) => response.await,
                None => Err(E2eError::system("direct probe driver missing")),
            },
        }
    })
    .await
}

fn direct_server_name(endpoint: &Url) -> Result<ServerName<'static>> {
    if endpoint.scheme() != "grpcs" && endpoint.scheme() != "https" {
        return Err(E2eError::system("direct probe endpoint is not TLS"));
    }
    let host = endpoint
        .host_str()
        .ok_or_else(|| E2eError::system("direct probe endpoint has no host"))?;
    ServerName::try_from(host.to_owned())
        .map_err(|_| E2eError::system("invalid direct probe server name"))
}

fn direct_request(endpoint: &Url, path: &str, content_type: &str) -> Result<Request<()>> {
    let uri = endpoint
        .join(path)
        .map_err(|_| E2eError::system("joining direct probe path"))?
        .to_string()
        .replacen("grpcs://", "https://", 1);
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .version(Version::HTTP_2)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(())
        .map_err(|_| E2eError::system("building direct probe request"))
}

fn authentication_exchange(endpoint: &Url, body: Bytes) -> Result<DirectExchange> {
    Ok(DirectExchange {
        request: direct_request(
            endpoint,
            gang_wire::routes::AUTHENTICATE_SESSION.template,
            "application/json",
        )?,
        payload: body,
    })
}

async fn receive_direct(
    sender: h2::client::SendRequest<Bytes>,
    exchange: DirectExchange,
) -> Result<DirectReply> {
    let mut sender = sender
        .ready()
        .await
        .map_err(direct_h2_error("readying direct probe request"))?;
    let (response, mut outgoing) = sender
        .send_request(exchange.request, false)
        .map_err(direct_h2_error("sending direct probe request"))?;
    outgoing
        .send_data(exchange.payload, true)
        .map_err(direct_h2_error("sending direct probe body"))?;
    let response = response
        .await
        .map_err(direct_h2_error("receiving direct probe response"))?;
    let (parts, mut body) = response.into_parts();
    let mut reply = DirectReply {
        status: parts.status.as_u16(),
        headers: parts.headers,
        trailers: http::HeaderMap::new(),
    };
    let mut size = 0;
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(direct_h2_error("reading direct probe response"))?;
        size = direct_response_size(size, chunk.len())?;
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(direct_h2_error("releasing direct probe flow control"))?;
    }
    reply.trailers = body
        .trailers()
        .await
        .map_err(direct_h2_error("reading direct probe trailers"))?
        .unwrap_or_default();
    Ok(reply)
}

fn grpc_payload(data: &[u8]) -> Result<Option<(&[u8], usize)>> {
    let Some(header) = data.get(..5) else {
        return Ok(None);
    };
    if header[0] != 0 {
        return Err(E2eError::system("compressed gRPC response frame"));
    }
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if length > MAX_RESPONSE_BYTES - 5 {
        return Err(E2eError::system("gRPC response exceeds size limit"));
    }
    let end = 5 + length;
    Ok(data.get(5..end).map(|payload| (payload, end)))
}

fn direct_response_size(size: usize, chunk: usize) -> Result<usize> {
    size.checked_add(chunk)
        .filter(|size| *size <= MAX_RESPONSE_BYTES)
        .ok_or_else(|| E2eError::system("direct response exceeds size limit"))
}

fn refusal_marker(headers: &http::HeaderMap) -> bool {
    let mut markers = headers.get_all(REQUEST_NOT_STARTED_HEADER).iter();
    markers
        .next()
        .is_some_and(|marker| marker.as_bytes() == b"true")
        && markers.next().is_none()
}

/// Opens a new TLS and HTTP/2 connection and verifies one authenticated REAPI response.
pub(crate) fn probe_capabilities(
    endpoint: &Url,
    credentials: &Credentials,
    timeout: Duration,
) -> Result<()> {
    let config = tls_client::client_config(
        credentials.authority.as_deref(),
        &[ALPN_H2],
        Arc::new(SystemClock::new()),
    )?;
    let host = endpoint
        .host_str()
        .ok_or_else(|| E2eError::system("executor endpoint has no host"))?
        .to_owned();
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| E2eError::system("executor endpoint has no port"))?;
    let server_name = ServerName::try_from(host.clone())
        .map_err(E2eError::system_from("invalid executor server name"))?;
    if endpoint.scheme() != "grpcs" && endpoint.scheme() != "https" {
        return Err(E2eError::system("executor endpoint is not TLS"));
    }
    let uri = endpoint
        .join(CAPABILITIES_PATH)
        .map_err(E2eError::system_from("joining capabilities path"))?
        .to_string()
        .replacen("grpcs://", "https://", 1);
    let authorization = http::HeaderValue::from_str(&format!("Bearer {}", credentials.token))
        .map_err(|_| E2eError::system("invalid bearer token header"))?;
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(E2eError::io("starting capabilities runtime"))?;
    runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            let tcp = TcpStream::connect((host.as_str(), port))
                .await
                .map_err(E2eError::io("connecting to executor"))?;
            let tls = TlsConnector::from(Arc::new(config))
                .connect(server_name, tcp)
                .await
                .map_err(E2eError::system_from("connecting executor TLS"))?;
            let (mut sender, connection) = h2::client::handshake(tls)
                .await
                .map_err(E2eError::system_from("connecting executor HTTP/2"))?;
            let driver = tokio::spawn(connection);
            let request = Request::builder()
                .method(Method::POST)
                .uri(uri)
                .version(Version::HTTP_2)
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .header(http::header::AUTHORIZATION, authorization)
                .header("te", "trailers")
                .body(())
                .map_err(E2eError::system_from("building capabilities request"))?;
            sender = sender
                .ready()
                .await
                .map_err(E2eError::system_from("readying capabilities request"))?;
            let (response, mut body) = sender
                .send_request(request, false)
                .map_err(E2eError::system_from("sending capabilities request"))?;
            body.send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), true)
                .map_err(E2eError::system_from("sending capabilities body"))?;
            let response = response
                .await
                .map_err(E2eError::system_from("receiving capabilities response"))?;
            let (parts, mut body) = response.into_parts();
            if parts.status != StatusCode::OK {
                return Err(E2eError::system(format!(
                    "capabilities HTTP status {}",
                    parts.status
                )));
            }
            let header_status = parts.headers.get("grpc-status").cloned();
            let mut data = Vec::new();
            while let Some(chunk) = body.data().await {
                let chunk =
                    chunk.map_err(E2eError::system_from("reading capabilities response"))?;
                append_response(&mut data, &chunk)?;
                body.flow_control()
                    .release_capacity(chunk.len())
                    .map_err(E2eError::system_from("releasing capabilities flow control"))?;
            }
            let trailers = body
                .trailers()
                .await
                .map_err(E2eError::system_from("reading capabilities trailers"))?;
            let status = trailers
                .as_ref()
                .and_then(|headers| headers.get("grpc-status"))
                .or(header_status.as_ref());
            require_grpc_success(status)?;
            decode_capabilities(&data)?;
            driver.abort();
            Ok(())
        })
        .await
        .map_err(|_| E2eError::timeout("probing capabilities", timeout))?
    })
}

fn canonical_grpc_status(value: &http::HeaderValue) -> Option<u8> {
    match value.as_bytes() {
        [digit @ b'0'..=b'9'] => Some(digit - b'0'),
        [b'1', digit @ b'0'..=b'6'] => Some(10 + digit - b'0'),
        _ => None,
    }
}

fn require_grpc_success(status: Option<&http::HeaderValue>) -> Result<()> {
    match status.and_then(canonical_grpc_status) {
        Some(0) => Ok(()),
        Some(code) => Err(E2eError::system(format!(
            "capabilities gRPC status is not successful: code={code}"
        ))),
        None => Err(E2eError::system(
            "capabilities gRPC status is not successful: code=unknown",
        )),
    }
}

fn append_response(data: &mut Vec<u8>, chunk: &[u8]) -> Result<()> {
    if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(data.len()) {
        return Err(E2eError::system("capabilities response exceeds size limit"));
    }
    data.extend_from_slice(chunk);
    Ok(())
}

fn decode_capabilities(data: &[u8]) -> Result<()> {
    let (payload, end) =
        grpc_payload(data)?.ok_or_else(|| E2eError::system("truncated capabilities frame"))?;
    if end != data.len() {
        return Err(E2eError::system("invalid capabilities frame length"));
    }
    let capabilities = ServerCapabilities::decode(payload)
        .map_err(E2eError::system_from("decoding capabilities response"))?;
    if !capabilities
        .execution_capabilities
        .as_ref()
        .is_some_and(|execution| execution.exec_enabled)
    {
        return Err(E2eError::system(
            "capabilities do not advertise remote execution",
        ));
    }
    Ok(())
}

/// Tracks the longest interval between successful samples, including the trailing interval.
/// Elapsed zero is the previously verified initial successful probe.
#[derive(Debug, Default)]
pub(crate) struct Availability {
    last_success: Duration,
    longest: Duration,
    pub(crate) failures: usize,
    pub(crate) successes: usize,
}

impl Availability {
    pub(crate) fn sample(&mut self, elapsed: Duration, success: bool) {
        if success {
            self.longest = self.longest.max(elapsed.saturating_sub(self.last_success));
            self.last_success = elapsed;
            self.successes += 1;
        } else {
            self.failures += 1;
        }
    }

    pub(crate) fn finish(&self, elapsed: Duration) -> Duration {
        self.longest.max(elapsed.saturating_sub(self.last_success))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn direct_h2_diagnostics_preserve_cause_without_peer_secrets() {
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct FailingIo;

        fn failure() -> std::io::Error {
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "secret-token-proof")
        }

        impl AsyncRead for FailingIo {
            fn poll_read(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
                _buffer: &mut tokio::io::ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Err(failure()))
            }
        }

        impl AsyncWrite for FailingIo {
            fn poll_write(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
                _buffer: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                Poll::Ready(Err(failure()))
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        // Let h2 wrap a real transport failure; it does not expose From<io::Error>.
        let error = match h2::client::handshake(FailingIo).await {
            Ok((_sender, connection)) => connection.await.expect_err("transport must fail"),
            Err(error) => error,
        };
        let error = direct_h2_error("receiving direct probe response")(error);
        let rendered = format!("{error:?}");
        assert!(rendered.contains("UnexpectedEof"));
        assert!(!rendered.contains("secret-token-proof"));
        let error =
            direct_h2_error("receiving direct probe response")(h2::Error::from(h2::Reason::CANCEL));
        assert!(error.to_string().contains("CANCEL"));
    }
    use gang_testkit::{ExtendedKeyUsagePurpose, TestAuthority, TestResult};
    use rustls_pki_types::PrivatePkcs8KeyDer;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn public_diagnostics_keep_authority_bearer_and_grpc_framing() -> TestResult {
        let fixture = WireFixture::new()?;
        let endpoint = Url::parse("grpcs://nlb.example.test:8443")?;
        let grpc = public_exchange(&endpoint, &fixture.credentials, CAPABILITIES_PATH)?;
        assert_eq!(grpc.request.method(), Method::POST);
        assert_eq!(
            grpc.request
                .uri()
                .authority()
                .map(http::uri::Authority::as_str),
            Some("nlb.example.test:8443")
        );
        assert_eq!(
            grpc.request.headers()[http::header::AUTHORIZATION],
            "Bearer original.private.bearer"
        );
        assert_eq!(grpc.payload.as_ref(), &[0; 5]);
        let discovery = public_exchange(&endpoint, &fixture.credentials, "/v1/server")?;
        assert_eq!(discovery.request.method(), Method::GET);
        assert!(discovery.payload.is_empty());
        let mut invalid = fixture.credentials.clone();
        invalid.token = "secret-token\r\ninjected".into();
        let error = public_exchange(&endpoint, &invalid, CAPABILITIES_PATH)
            .err()
            .ok_or("invalid header was accepted")?;
        assert!(!format!("{error:?}").contains("secret-token"));
        Ok(())
    }

    const WIRE_DEADLINE: Duration = Duration::from_secs(5);
    const MINT_BODY: &[u8] = br#"{"provider":"test","proof":"private-proof"}"#;
    const AUTH_BODY: &[u8] = br#"{"token":"new.private.bearer","lifetime":"60s"}"#;

    struct WireFixture {
        _root: tempfile::TempDir,
        credentials: Credentials,
        acceptor: tokio_rustls::TlsAcceptor,
    }

    impl WireFixture {
        fn new() -> TestResult<Self> {
            let root = tempfile::tempdir()?;
            let authority = TestAuthority::new("direct mint authority")?;
            let leaf = authority.issue(
                vec!["nlb.example.test".to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                [1; 16],
                "nlb.example.test",
            )?;
            let authority_path = root.path().join("authority.pem");
            std::fs::write(&authority_path, authority.pem.as_str())?;
            let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der],
                PrivatePkcs8KeyDer::from(leaf.key_der).into(),
            )?;
            config.alpn_protocols = vec![ALPN_H2.to_vec()];
            Ok(Self {
                credentials: Credentials {
                    principal: "test:administrator".into(),
                    home: root.path().to_owned(),
                    token: "original.private.bearer".into(),
                    authority: Some(authority_path),
                },
                _root: root,
                acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(config)),
            })
        }
    }

    #[derive(Clone, Copy)]
    enum WireKind {
        Authentication,
        Diagnostic,
    }

    struct WireResponse {
        status: StatusCode,
        headers: http::HeaderMap,
        trailers: http::HeaderMap,
        data: Bytes,
    }

    impl WireResponse {
        fn authentication() -> Self {
            Self {
                status: StatusCode::OK,
                headers: http::HeaderMap::new(),
                trailers: http::HeaderMap::new(),
                data: Bytes::from_static(AUTH_BODY),
            }
        }
    }

    struct WireOutcome {
        reply: Result<DirectReply>,
    }

    async fn accept_public(
        acceptor: &tokio_rustls::TlsAcceptor,
        listener: &TcpListener,
        target: DirectTarget,
    ) -> TestResult<tokio_rustls::server::TlsStream<TcpStream>> {
        let (mut stream, source) = listener.accept().await?;
        if target.proxy_protocol_v2 {
            let (SocketAddr::V4(source), SocketAddr::V4(destination)) = (source, target.address)
            else {
                return Err("fixture requires IPv4".into());
            };
            let mut frame = [0; 28];
            stream.read_exact(&mut frame).await?;
            assert_eq!(frame, proxy_v2_tcp_header(source, destination));
            assert_eq!(&frame[..16], b"\r\n\r\n\0\r\nQUIT\n\x21\x11\0\x0c");
        }
        let tls = acceptor.accept(stream).await?;
        assert_eq!(tls.get_ref().1.server_name(), Some("nlb.example.test"));
        assert_eq!(tls.get_ref().1.alpn_protocol(), Some(ALPN_H2));
        Ok(tls)
    }

    async fn receive_request(request: Request<h2::RecvStream>) -> TestResult<Vec<u8>> {
        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.version(), Version::HTTP_2);
        assert_eq!(
            request.uri().to_string(),
            "https://nlb.example.test:8443/v1/session:authenticate"
        );
        assert_eq!(
            request.headers()[http::header::CONTENT_TYPE],
            "application/json"
        );
        assert!(!request.headers().contains_key(http::header::AUTHORIZATION));
        let mut body = request.into_body();
        let mut received = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk?;
            received.extend_from_slice(&chunk);
            body.flow_control().release_capacity(chunk.len())?;
        }
        assert_eq!(received, MINT_BODY);
        Ok(received)
    }

    async fn serve(
        acceptor: &tokio_rustls::TlsAcceptor,
        listener: &TcpListener,
        target: DirectTarget,
        response: WireResponse,
    ) -> TestResult<Vec<u8>> {
        let tls = accept_public(acceptor, listener, target).await?;
        let mut connection = h2::server::handshake(tls).await?;
        let (request, mut respond) = connection
            .accept()
            .await
            .ok_or("missing direct request")??;
        let mut drivers = JoinSet::new();
        drivers.spawn(async move {
            // The probe sends exactly one request.
            assert!(!matches!(connection.accept().await, Some(Ok(_))));
        });
        let received = receive_request(request).await?;
        let mut headers = http::Response::builder().status(response.status).body(())?;
        *headers.headers_mut() = response.headers;
        let mut outgoing = respond.send_response(headers, false)?;
        if !response.data.is_empty() {
            // Exercise incremental body reception.
            let split = response.data.len().min(3);
            outgoing.send_data(response.data.slice(..split), false)?;
            outgoing.send_data(response.data.slice(split..), false)?;
        }
        outgoing.send_trailers(response.trailers)?;
        drop(outgoing);
        drop(respond);
        drivers.join_next().await.ok_or("missing server driver")??;
        Ok(received)
    }

    async fn wire(
        fixture: &WireFixture,
        kind: WireKind,
        response: WireResponse,
        framed: bool,
    ) -> TestResult<WireOutcome> {
        let endpoint = Url::parse("grpcs://nlb.example.test:8443")?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let target = DirectTarget {
            address: listener.local_addr()?,
            proxy_protocol_v2: framed,
        };
        let exchange = match kind {
            WireKind::Authentication | WireKind::Diagnostic => {
                authentication_exchange(&endpoint, Bytes::from_static(MINT_BODY))?
            }
        };
        let transport = async {
            let tcp = TcpStream::connect(target.address)
                .await
                .map_err(E2eError::io("connecting fixture"))?;
            let source = tcp
                .local_addr()
                .map_err(E2eError::io("reading fixture source"))?;
            Ok((tcp, source))
        };
        let mut servers = JoinSet::new();
        let acceptor = fixture.acceptor.clone();
        servers.spawn(async move {
            serve(&acceptor, &listener, target, response)
                .await
                .map_err(|error| E2eError::system(format!("serving wire fixture: {error}")))
        });
        let (reply, served) = tokio::time::timeout(WIRE_DEADLINE, async {
            let reply = direct_over(
                &endpoint,
                &fixture.credentials,
                target,
                exchange,
                WIRE_DEADLINE,
                transport,
            )
            .await;
            let served = servers
                .join_next()
                .await
                .ok_or_else(|| E2eError::system("missing wire server"))?
                .map_err(|_| E2eError::system("joining wire server"))?;
            Ok::<_, E2eError>((reply, served))
        })
        .await??;
        served?;
        Ok(WireOutcome { reply })
    }

    fn private_error(error: &E2eError) {
        let rendered = format!("{error:?} {error} {}", gang_types::error_chain(error));
        for secret in [
            "private-proof",
            "new.private.bearer",
            "original.private.bearer",
            "secret-status",
            "secret-marker",
        ] {
            assert!(!rendered.contains(secret));
        }
    }

    #[tokio::test]
    async fn refusal_requires_complete_exact_marked503_without_reflection() -> TestResult {
        let fixture = WireFixture::new()?;
        for framed in [false, true] {
            for (status, marker, accepted) in [
                (StatusCode::SERVICE_UNAVAILABLE, Some("true"), true),
                (StatusCode::OK, Some("true"), false),
                (StatusCode::SERVICE_UNAVAILABLE, Some("false"), false),
                (StatusCode::SERVICE_UNAVAILABLE, None, false),
            ] {
                let mut response = WireResponse::authentication();
                response.status = status;
                if let Some(marker) = marker {
                    response.headers.insert(
                        REQUEST_NOT_STARTED_HEADER,
                        http::HeaderValue::from_str(marker)?,
                    );
                }
                let outcome = wire(&fixture, WireKind::Authentication, response, framed).await?;
                let observation = decode_refusal(&outcome.reply?);
                assert_eq!(observation.is_ok(), accepted);
                if let Err(error) = observation {
                    private_error(&error);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn generic_authentication_diagnostics_discard_bodies_without_asserting_refusal()
    -> TestResult {
        let fixture = WireFixture::new()?;
        for (status, marker, expected) in [
            (StatusCode::OK, None, false),
            (StatusCode::SERVICE_UNAVAILABLE, Some("true"), true),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some("secret-marker"),
                false,
            ),
        ] {
            let mut response = WireResponse::authentication();
            response.status = status;
            response.data = Bytes::from_static(b"private-proof new.private.bearer");
            if let Some(marker) = marker {
                response.headers.insert(
                    REQUEST_NOT_STARTED_HEADER,
                    http::HeaderValue::from_str(marker)?,
                );
            }
            let outcome = wire(&fixture, WireKind::Diagnostic, response, false).await?;
            let reply = outcome.reply?;
            assert_eq!(reply.status, status.as_u16());
            assert_eq!(refusal_marker(&reply.headers), expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn direct_exchange_rejects_transport_failure_and_whole_exchange_deadline() -> TestResult {
        let fixture = WireFixture::new()?;
        let endpoint = Url::parse("https://nlb.example.test:8443")?;
        let target = DirectTarget {
            address: "127.0.0.1:9443".parse()?,
            proxy_protocol_v2: false,
        };
        let exchange = authentication_exchange(&endpoint, Bytes::new())?;
        let failed = direct_over(
            &endpoint,
            &fixture.credentials,
            target,
            exchange,
            WIRE_DEADLINE,
            async { Err::<(TcpStream, SocketAddr), _>(E2eError::system("transport unavailable")) },
        )
        .await;
        assert!(matches!(failed, Err(E2eError::System { .. })));
        let exchange = authentication_exchange(&endpoint, Bytes::new())?;
        let timed_out = direct_over(
            &endpoint,
            &fixture.credentials,
            target,
            exchange,
            Duration::ZERO,
            std::future::pending::<Result<(TcpStream, SocketAddr)>>(),
        )
        .await;
        assert!(matches!(timed_out, Err(E2eError::Timeout { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_unfinished_exchange_closes_its_owned_driver() -> TestResult {
        let fixture = WireFixture::new()?;
        let endpoint = Url::parse("https://nlb.example.test:8443")?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let target = DirectTarget {
            address: listener.local_addr()?,
            proxy_protocol_v2: true,
        };
        let (started, ready) = tokio::sync::oneshot::channel();
        let acceptor = fixture.acceptor.clone();
        let mut servers = JoinSet::new();
        servers.spawn(async move {
            let serving = async {
                let tls = accept_public(&acceptor, &listener, target).await?;
                let mut connection = h2::server::handshake(tls).await?;
                let (_request, mut respond) =
                    connection.accept().await.ok_or("missing request")??;
                let response = http::Response::builder().status(StatusCode::OK).body(())?;
                let _unfinished = respond.send_response(response, false)?;
                started
                    .send(())
                    .map_err(|()| "probe cancelled before headers")?;
                assert!(!matches!(connection.accept().await, Some(Ok(_))));
                TestResult::Ok(())
            };
            serving
                .await
                .map_err(|error| E2eError::system(format!("serving unfinished fixture: {error}")))
        });
        let mut deadline = std::pin::pin!(tokio::time::sleep(WIRE_DEADLINE));
        let exchange = authentication_exchange(&endpoint, Bytes::from_static(MINT_BODY))?;
        let transport = async {
            let tcp = TcpStream::connect(target.address)
                .await
                .map_err(E2eError::io("connecting fixture"))?;
            let source = tcp
                .local_addr()
                .map_err(E2eError::io("reading fixture source"))?;
            Ok((tcp, source))
        };
        {
            let mut probe = std::pin::pin!(direct_over(
                &endpoint,
                &fixture.credentials,
                target,
                exchange,
                WIRE_DEADLINE,
                transport,
            ));
            tokio::select! {
                result = &mut probe => {
                    result?;
                    return Err("unfinished exchange completed".into());
                }
                observed = ready => observed?,
                () = &mut deadline => {
                    return Err("unfinished exchange exceeded deadline before cancellation".into());
                }
            }
        }
        tokio::select! {
            observed = servers.join_next() => {
                observed.ok_or("missing unfinished server")???;
            }
            () = &mut deadline => {
                return Err("unfinished server remained open after cancellation deadline".into());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn direct_exchange_rejects_wrong_ca_and_tls_name() -> TestResult {
        let fixture = WireFixture::new()?;
        let other = WireFixture::new()?;
        for (endpoint, credentials) in [
            ("https://nlb.example.test:8443", &other.credentials),
            ("https://wrong.example.test:8443", &fixture.credentials),
        ] {
            let endpoint = Url::parse(endpoint)?;
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let target = DirectTarget {
                address: listener.local_addr()?,
                proxy_protocol_v2: false,
            };
            let exchange = authentication_exchange(&endpoint, Bytes::new())?;
            let transport = async {
                let tcp = TcpStream::connect(target.address)
                    .await
                    .map_err(E2eError::io("connecting fixture"))?;
                let source = tcp
                    .local_addr()
                    .map_err(E2eError::io("reading fixture source"))?;
                Ok((tcp, source))
            };
            let (result, accepted) = tokio::time::timeout(WIRE_DEADLINE, async {
                tokio::join!(
                    direct_over(
                        &endpoint,
                        credentials,
                        target,
                        exchange,
                        WIRE_DEADLINE,
                        transport
                    ),
                    async {
                        let (stream, _) = listener.accept().await?;
                        Ok::<_, std::io::Error>(fixture.acceptor.accept(stream).await)
                    },
                )
            })
            .await?;
            assert!(result.is_err());
            assert!(accepted?.is_err());
        }
        Ok(())
    }

    #[test]
    fn response_bounds_and_duplicate_diagnostic_markers() -> TestResult {
        assert_eq!(
            direct_response_size(0, MAX_RESPONSE_BYTES)?,
            MAX_RESPONSE_BYTES
        );
        assert!(direct_response_size(MAX_RESPONSE_BYTES, 1).is_err());
        assert!(direct_response_size(1, usize::MAX).is_err());
        let mut headers = http::HeaderMap::new();
        headers.append(
            REQUEST_NOT_STARTED_HEADER,
            http::HeaderValue::from_static("true"),
        );
        headers.append(
            REQUEST_NOT_STARTED_HEADER,
            http::HeaderValue::from_static("true"),
        );
        assert!(!refusal_marker(&headers));
        Ok(())
    }

    #[test]
    fn grpc_failure_reports_only_canonical_numeric_status() {
        let unavailable = http::HeaderValue::from_static("14");
        let error = require_grpc_success(Some(&unavailable)).expect_err("14 is unavailable");
        assert!(error.to_string().contains("code=14"));
        assert!(require_grpc_success(Some(&http::HeaderValue::from_static("0"))).is_ok());
        for code in 0_u8..=16 {
            let value = http::HeaderValue::from_str(&code.to_string()).expect("numeric header");
            assert_eq!(canonical_grpc_status(&value), Some(code));
        }
        for raw in [
            "secret-token-proof",
            "14 secret-token-proof",
            "014",
            "17",
            "-1",
            "",
        ] {
            let header = http::HeaderValue::from_str(raw).expect("legal header bytes");
            let error = require_grpc_success(Some(&header)).expect_err("noncanonical status");
            assert!(error.to_string().contains("code=unknown"));
            assert!(!format!("{error:?}").contains("secret-token-proof"));
        }
        assert!(require_grpc_success(None).is_err());
    }

    #[test]
    fn includes_trailing_and_recovered_gaps() {
        let mut samples = Availability::default();
        samples.sample(Duration::from_secs(2), false);
        samples.sample(Duration::from_secs(4), true);
        samples.sample(Duration::from_secs(6), false);
        assert_eq!(
            samples.finish(Duration::from_secs(10)),
            Duration::from_secs(6)
        );
        assert_eq!((samples.successes, samples.failures), (1, 2));
        // Preparation, health detection or a cold-helper command cannot reset the clock or
        // turn an unsampled interval into successful fresh connections.
        samples.sample(Duration::from_secs(20), true);
        assert_eq!(
            samples.finish(Duration::from_secs(21)),
            Duration::from_secs(16)
        );
        assert_eq!((samples.successes, samples.failures), (2, 2));
    }

    #[test]
    fn rejects_invalid_frames() {
        assert!(decode_capabilities(&[]).is_err());
        assert!(decode_capabilities(&[1, 0, 0, 0, 0]).is_err());
        assert!(decode_capabilities(&[0, 0, 0, 0, 1]).is_err());
        assert!(decode_capabilities(&[0, 0, 0, 0, 0]).is_err());
        let capabilities = ServerCapabilities {
            execution_capabilities: Some(
                gang_reapi::build::bazel::remote::execution::v2::ExecutionCapabilities {
                    exec_enabled: true,
                    ..Default::default()
                },
            ),
            ..Default::default()
        }
        .encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(
            &u32::try_from(capabilities.len())
                .expect("frame length")
                .to_be_bytes(),
        );
        frame.extend_from_slice(&capabilities);
        assert!(decode_capabilities(&frame).is_ok());
        let mut full = vec![0; MAX_RESPONSE_BYTES];
        assert!(append_response(&mut full, &[0]).is_err());
    }
}
