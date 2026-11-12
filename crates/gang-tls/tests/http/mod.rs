//! HTTP client behaviour: protocols, bearer and body handling, body caps, redirects, pinned TLS
//! trust and request timeouts. Servers are loopback hyper services, one task per connection.

use std::convert::Infallible;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Waker};

use bytes::Bytes;
use gang_clock::{Clock, SystemClock, TestClock};
use gang_tls::{
    Alpn, HttpClient, HttpError, HttpProtocol, HttpReply, HttpRequest, HttpSettings, HttpTransport,
    RequestBody, ServerTrust, is_certificate_refused,
};
use gang_types::{AccessToken, NonZeroBytes, NonZeroDuration, Url};
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderName, LOCATION};
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::{http1, http2};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::support::{ServerChain, TransportFixture};

const TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(5);
const MAX_BODY: NonZeroBytes = NonZeroBytes::kib(1);

/// Which HTTP version a loopback server speaks.
#[derive(Clone, Copy)]
enum Speaks {
    Http1,
    Http2,
}

fn header_text(request: &Request<Incoming>, name: HeaderName) -> String {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

async fn respond(request: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = request.uri().path().to_owned();
    let authorization = header_text(&request, AUTHORIZATION);
    let content_type = header_text(&request, CONTENT_TYPE);
    let extra = header_text(&request, HeaderName::from_static("x-extra"));
    let body = request
        .into_body()
        .collect()
        .await
        .expect("request body reads")
        .to_bytes();
    let reply = match path.as_str() {
        "/echo" => Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from(format!(
                "{authorization}|{content_type}|{}",
                String::from_utf8_lossy(&body)
            )))),
        "/headers" => Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from(extra))),
        "/redirect" => Response::builder()
            .status(StatusCode::FOUND)
            .header(LOCATION, "/elsewhere")
            .body(Full::new(Bytes::new())),
        sized if sized.starts_with("/body/") => {
            let length: usize = sized["/body/".len()..]
                .parse()
                .expect("body length is a number");
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from(vec![b'x'; length])))
        }
        _ => Response::builder()
            .status(StatusCode::OK)
            .header("x-reply", "pong")
            .body(Full::new(Bytes::from_static(b"hello"))),
    };
    Ok(reply.expect("reply is valid"))
}

async fn serve_connection<IO>(io: IO, speaks: Speaks)
where
    IO: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let service = service_fn(respond);
    // A connection error ends only that connection; the client observes it as its own failure.
    let _ = match speaks {
        Speaks::Http1 => http1::Builder::new().serve_connection(io, service).await,
        Speaks::Http2 => {
            http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
        }
    };
}

/// Binds a plain loopback server and returns its port.
async fn serve_plain(speaks: Speaks) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener binds");
    let port = listener
        .local_addr()
        .expect("listener has an address")
        .port();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(serve_connection(TokioIo::new(tcp), speaks));
        }
    });
    port
}

/// Binds a TLS loopback server presenting `fixture`'s certificate and returns its port.
async fn serve_tls(fixture: &TransportFixture) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener binds");
    let port = listener
        .local_addr()
        .expect("listener has an address")
        .port();
    let acceptor = fixture.acceptor().clone();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(tcp).await {
                    serve_connection(TokioIo::new(tls), Speaks::Http2).await;
                }
            });
        }
    });
    port
}

fn cap(bytes: u64) -> NonZeroBytes {
    NonZeroBytes::new(gang_types::Bytes::new(bytes)).expect("test cap is non-zero")
}

fn plain_settings(protocol: HttpProtocol, max_body: NonZeroBytes) -> HttpSettings<'static> {
    HttpSettings {
        transport: HttpTransport::Plain,
        protocol,
        timeout: TIMEOUT,
        max_body,
        clock: Arc::new(SystemClock::default()),
    }
}

fn tls_settings(trust: ServerTrust<'_>, clock: Arc<dyn Clock>) -> HttpSettings<'_> {
    HttpSettings {
        transport: HttpTransport::Tls { trust },
        protocol: HttpProtocol::Http2,
        timeout: TIMEOUT,
        max_body: MAX_BODY,
        clock,
    }
}

fn url(text: &str) -> Url {
    Url::parse(text).expect("test url parses")
}

/// The URL of a TLS test server by the IPv4 address it listens on. `localhost` would also
/// resolve to `::1`, where the server does not listen and another process may, so a test could
/// reach a server that is not its own.
fn tls_url(port: u16) -> Url {
    url(&format!("https://127.0.0.1:{port}/"))
}

fn get(url: &Url) -> HttpRequest<'_> {
    HttpRequest {
        method: Method::GET,
        url,
        bearer: None,
        headers: &[],
        body: None,
    }
}

#[tokio::test]
async fn plain_http2_prior_knowledge_get_returns_status_headers_and_body() {
    let port = serve_plain(Speaks::Http2).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Http2, MAX_BODY))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/"));
    let reply: HttpReply = client
        .send(get(&target))
        .await
        .expect("prior-knowledge request succeeds");
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.headers["x-reply"], "pong");
    assert_eq!(reply.body, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn bearer_and_body_are_sent() {
    let port = serve_plain(Speaks::Http2).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Http2, MAX_BODY))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/echo"));
    let token = AccessToken::new("secret-token");
    let reply = client
        .send(HttpRequest {
            method: Method::POST,
            url: &target,
            bearer: Some(&token),
            headers: &[],
            body: Some(RequestBody {
                content_type: "application/json",
                bytes: Bytes::from_static(b"{\"a\":1}"),
            }),
        })
        .await
        .expect("request with body succeeds");
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.body,
        Bytes::from_static(b"Bearer secret-token|application/json|{\"a\":1}")
    );
}

#[tokio::test]
async fn extra_request_headers_arrive_at_the_server() {
    let port = serve_plain(Speaks::Http2).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Http2, MAX_BODY))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/headers"));
    let headers = [(
        HeaderName::from_static("x-extra"),
        HeaderValue::from_static("custom value"),
    )];
    let mut request = get(&target);
    request.headers = &headers;
    let reply = client.send(request).await.expect("request succeeds");
    assert_eq!(reply.body, Bytes::from_static(b"custom value"));
}

#[tokio::test]
async fn open_returns_the_head_and_streams_chunks_in_order() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener binds");
    let port = listener
        .local_addr()
        .expect("listener has an address")
        .port();
    let (release, gate) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.expect("client connects");
        let mut request = [0; 4096];
        assert_ne!(tcp.read(&mut request).await.expect("request reads"), 0);
        tcp.write_all(b"HTTP/1.1 200 OK\r\nx-reply: streaming\r\ntransfer-encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .await
            .expect("head and first chunk are sent");
        gate.await.expect("second chunk is released");
        tcp.write_all(b"6\r\nsecond\r\n0\r\n\r\n")
            .await
            .expect("second chunk is sent");
    });
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Negotiated, cap(1)))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/"));
    let mut stream = client
        .open(get(&target))
        .await
        .expect("response head arrives");
    assert_eq!(stream.status, StatusCode::OK);
    assert_eq!(stream.headers["x-reply"], "streaming");
    assert_eq!(
        stream.next_chunk().await.expect("first chunk reads"),
        Some(Bytes::from_static(b"first"))
    );
    release.send(()).expect("server awaits release");
    assert_eq!(
        stream.next_chunk().await.expect("second chunk reads"),
        Some(Bytes::from_static(b"second"))
    );
    assert_eq!(stream.next_chunk().await.expect("body ends"), None);
    server.await.expect("server task completes");
}

#[tokio::test]
async fn open_refuses_a_foreign_scheme_without_io() {
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Negotiated, MAX_BODY))
        .expect("plain client builds");
    let target = url("https://localhost:1/");
    let error = client
        .open(get(&target))
        .await
        .expect_err("a plain client refuses https");
    assert!(
        matches!(error, HttpError::UnsupportedScheme { ref scheme } if scheme == "https"),
        "got {error:?}"
    );
}

#[tokio::test]
async fn body_larger_than_the_cap_is_refused() {
    let port = serve_plain(Speaks::Http1).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Negotiated, cap(8)))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/body/9"));
    let error = client
        .send(get(&target))
        .await
        .expect_err("a body over the cap is refused");
    assert!(
        matches!(error, HttpError::BodyTooLarge { status, limit } if status == StatusCode::OK && limit == cap(8)),
        "got {error:?}"
    );
}

#[tokio::test]
async fn body_exactly_at_the_cap_is_accepted() {
    let port = serve_plain(Speaks::Http1).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Negotiated, cap(8)))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/body/8"));
    let reply = client
        .send(get(&target))
        .await
        .expect("a body at the cap is accepted");
    assert_eq!(reply.body, Bytes::from(vec![b'x'; 8]));
}

#[tokio::test]
async fn redirect_is_returned_not_followed() {
    let port = serve_plain(Speaks::Http2).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Http2, MAX_BODY))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/redirect"));
    let reply = client
        .send(get(&target))
        .await
        .expect("redirect reply is returned");
    assert_eq!(reply.status, StatusCode::FOUND);
    assert_eq!(reply.headers[LOCATION], "/elsewhere");
}

#[tokio::test]
async fn negotiated_protocol_reaches_an_http1_only_server() {
    let port = serve_plain(Speaks::Http1).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Negotiated, MAX_BODY))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/"));
    let reply = client
        .send(get(&target))
        .await
        .expect("http/1.1 over plain tcp succeeds");
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn http2_protocol_fails_against_an_http1_only_server() {
    let port = serve_plain(Speaks::Http1).await;
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Http2, MAX_BODY))
        .expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/"));
    let error = client
        .send(get(&target))
        .await
        .expect_err("h2 prior knowledge is refused by an http/1.1 server");
    assert!(matches!(error, HttpError::Connect(_)), "got {error:?}");
}

#[tokio::test]
async fn https_with_a_pinned_ca_succeeds() {
    let fixture = TransportFixture::with_alpn(Alpn::H2);
    let pin = fixture.ca_pin();
    let port = serve_tls(&fixture).await;
    let client = HttpClient::try_new(&tls_settings(ServerTrust::Pin(&pin), fixture.clock()))
        .expect("tls client builds");
    let target = tls_url(port);
    let reply = client
        .send(get(&target))
        .await
        .expect("pinned https request succeeds");
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn https_with_the_wrong_pin_is_a_refused_certificate() {
    let fixture = TransportFixture::with_alpn(Alpn::H2);
    let port = serve_tls(&fixture).await;
    let other = TransportFixture::new();
    let wrong_pin = other.ca_pin();
    let client = HttpClient::try_new(&tls_settings(ServerTrust::Pin(&wrong_pin), fixture.clock()))
        .expect("tls client builds");
    let target = tls_url(port);
    let error = client
        .send(get(&target))
        .await
        .expect_err("a server certificate outside the pin is refused");
    assert!(matches!(error, HttpError::Connect(_)), "got {error:?}");
    assert!(is_certificate_refused(&error), "got {error:?}");
}

/// Serves one connection on a loopback listener: answers the client hello with the server's
/// whole first flight, then closes its write side and reads until the client hangs up.
async fn serve_flight_then_close(config: Arc<rustls::ServerConfig>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener binds");
    let port = listener
        .local_addr()
        .expect("listener has an address")
        .port();
    tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.expect("the client connects");
        let mut connection =
            rustls::ServerConnection::new(config).expect("the server connection builds");
        let mut buffer = vec![0_u8; 16 * 1024];
        while !connection.wants_write() {
            let read = tcp
                .read(&mut buffer)
                .await
                .expect("the client hello arrives");
            connection
                .read_tls(&mut &buffer[..read])
                .expect("the client hello is buffered");
            connection
                .process_new_packets()
                .expect("the client hello is valid");
        }
        let mut flight = Vec::new();
        while connection.wants_write() {
            connection
                .write_tls(&mut flight)
                .expect("the flight is written to memory");
        }
        tcp.write_all(&flight).await.expect("the flight is sent");
        tcp.shutdown().await.expect("the write side closes");
        while tcp.read(&mut buffer).await.is_ok_and(|read| read > 0) {}
    });
    port
}

#[tokio::test]
async fn a_refusal_after_the_server_closed_its_side_is_still_a_refused_certificate() {
    let fixture = TransportFixture::with_alpn(Alpn::H2);
    let port = serve_flight_then_close(fixture.raw_server_config(ServerChain::LeafAndCa)).await;
    let other = TransportFixture::new();
    let wrong_pin = other.ca_pin();
    let client = HttpClient::try_new(&tls_settings(ServerTrust::Pin(&wrong_pin), fixture.clock()))
        .expect("tls client builds");
    let error = client
        .send(get(&tls_url(port)))
        .await
        .expect_err("a server certificate outside the pin is refused");
    assert!(is_certificate_refused(&error), "got {error:?}");
}

#[tokio::test]
async fn plain_client_refuses_https_urls_without_io() {
    let client = HttpClient::try_new(&plain_settings(HttpProtocol::Negotiated, MAX_BODY))
        .expect("plain client builds");
    let target = url("https://localhost:1/");
    let error = client
        .send(get(&target))
        .await
        .expect_err("a plain client refuses https");
    assert!(
        matches!(error, HttpError::UnsupportedScheme { ref scheme } if scheme == "https"),
        "got {error:?}"
    );
}

#[tokio::test]
async fn tls_client_refuses_http_urls() {
    let fixture = TransportFixture::new();
    let pin = fixture.ca_pin();
    let client = HttpClient::try_new(&tls_settings(ServerTrust::Pin(&pin), fixture.clock()))
        .expect("tls client builds");
    let target = url("http://localhost:1/");
    let error = client
        .send(get(&target))
        .await
        .expect_err("a tls client refuses http");
    assert!(
        matches!(error, HttpError::UnsupportedScheme { ref scheme } if scheme == "http"),
        "got {error:?}"
    );
}

#[tokio::test]
async fn request_timeout_fires_when_the_server_never_answers() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener binds");
    let port = listener
        .local_addr()
        .expect("listener has an address")
        .port();
    let (release, held) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let Ok((_tcp, _)) = listener.accept().await else {
            return;
        };
        // Keeps the accepted socket open and silent until the test has finished.
        let _ = held.await;
    });
    let clock = Arc::new(TestClock::default());
    let settings = HttpSettings {
        transport: HttpTransport::Plain,
        protocol: HttpProtocol::Http2,
        timeout: TIMEOUT,
        max_body: MAX_BODY,
        clock: clock.clone(),
    };
    let client = HttpClient::try_new(&settings).expect("plain client builds");
    let target = url(&format!("http://127.0.0.1:{port}/"));
    let mut request = pin!(client.send(get(&target)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(request.as_mut().poll(&mut cx).is_pending());
    clock.advance(TIMEOUT.get());
    let error = request
        .await
        .expect_err("a silent server exceeds the timeout");
    assert!(matches!(error, HttpError::TimedOut), "got {error:?}");
    let mut request = pin!(client.open(get(&target)));
    assert!(request.as_mut().poll(&mut cx).is_pending());
    clock.advance(TIMEOUT.get());
    let error = request
        .await
        .expect_err("a silent server exceeds the timeout");
    assert!(matches!(error, HttpError::TimedOut), "got {error:?}");
    drop(release);
}
