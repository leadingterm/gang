//! The HTTP/2 connection the scenarios drive: one TLS connection to a gang server's public
//! listener, carrying gRPC requests over `h2`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use gang_types::Url;
use h2::client::{self, ResponseFuture, SendRequest};
use h2::{Ping, PingPong, SendStream};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, Method, Request, Version};
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;
use tokio_rustls::TlsConnector;

use crate::errors::{E2eError, Result};
use crate::identity::{Admission, Channel, ChannelEvent, Reply};

/// The REAPI Capabilities method, which every authenticated client may call.
const CAPABILITIES_PATH: &str = "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities";

/// An empty gRPC message: no compression flag and a zero length.
const EMPTY_MESSAGE: [u8; 5] = [0; 5];

/// The `grpc-status` of a request that carries no valid token.
const GRPC_UNAUTHENTICATED: u32 = 16;

/// The `grpc-status` of a request from a principal the server refuses, such as a revoked one.
const GRPC_PERMISSION_DENIED: u32 = 7;

/// One HTTP/2 connection to a gang server's public listener. Its requests carry the token the
/// connection was opened with, if any.
pub struct H2Channel {
    /// The runtime that drives the connection.
    runtime: Runtime,
    /// The handle that sends requests on the connection.
    sender: SendRequest<Bytes>,
    /// The handle that sends pings; `None` only after a ping ended with the connection.
    ping_pong: Option<PingPong>,
    /// Becomes `true` once the connection has ended, whatever the cause.
    closed: watch::Receiver<bool>,
    /// The absolute URI of the Capabilities method.
    capabilities: String,
    /// The bearer token every request carries, if any.
    token: Option<String>,
}

impl H2Channel {
    /// Opens a connection to `endpoint` over TLS with `config`, presenting `token` on every
    /// request. The connection is established before this returns.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the endpoint has no host or no port, or the HTTP/2 handshake
    /// fails; [`E2eError::Io`] when the TCP connection cannot be opened;
    /// [`E2eError::Timeout`] when setup exceeds `handshake_timeout`. A TLS refusal is `Ok`
    /// with [`Admission::Refused`].
    pub fn connect(
        config: Arc<rustls::ClientConfig>,
        endpoint: &Url,
        token: Option<&str>,
        handshake_timeout: Duration,
    ) -> Result<Admission<Self>> {
        let host = endpoint
            .host_str()
            .ok_or_else(|| E2eError::system(format!("{endpoint} has no host")))?
            .to_owned();
        let port = endpoint
            .port_or_known_default()
            .ok_or_else(|| E2eError::system(format!("{endpoint} has no port")))?;
        let server_name = ServerName::try_from(host.clone()).map_err(E2eError::system_from(
            format!("{host} is not a server name"),
        ))?;
        let capabilities = endpoint
            .join(CAPABILITIES_PATH)
            .map_err(E2eError::system_from(format!(
                "joining the Capabilities path to {endpoint}"
            )))?
            .to_string();
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(E2eError::io("starting the connection runtime"))?;
        let deadline = tokio::time::Instant::now() + handshake_timeout;
        let condition = format!("connecting to {endpoint}");

        // A TLS refusal, whatever its cause, is the server refusing the connection.
        let tls = runtime
            .block_on(async move {
                tokio::time::timeout_at(deadline, async move {
                    let tcp = TcpStream::connect((host.as_str(), port))
                        .await
                        .map_err(E2eError::io(format!("connecting to {host}:{port}")))?;
                    Ok::<_, E2eError>(
                        TlsConnector::from(config)
                            .connect(server_name, tcp)
                            .await
                            .ok(),
                    )
                })
                .await
            })
            .map_err(|_| E2eError::timeout(condition.clone(), handshake_timeout))??;

        match tls {
            Some(tls) => Ok(Admission::Accepted(Self::establish(
                runtime,
                tls,
                capabilities,
                token,
                Some((deadline, handshake_timeout, condition)),
            )?)),
            None => Ok(Admission::Refused),
        }
    }

    /// Runs the HTTP/2 handshake over `io` on `runtime`, and drives the connection in the
    /// background. The connection's end, whatever its cause, is what `closed` reports.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the HTTP/2 handshake fails.
    fn establish<IO>(
        runtime: Runtime,
        io: IO,
        capabilities: String,
        token: Option<&str>,
        deadline: Option<(tokio::time::Instant, Duration, String)>,
    ) -> Result<Self>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, ping_pong, closed) = runtime.block_on(async move {
            let handshake = client::handshake(io);
            let (sender, mut connection) = match deadline {
                Some((deadline, waited, condition)) => tokio::time::timeout_at(deadline, handshake)
                    .await
                    .map_err(|_| E2eError::timeout(condition, waited))?,
                None => handshake.await,
            }
            .map_err(E2eError::system_from("the HTTP/2 handshake failed"))?;
            let ping_pong = connection.ping_pong();
            let (closed_tx, closed) = watch::channel(false);
            tokio::spawn(async move {
                let _ = connection.await;
                closed_tx.send_replace(true);
            });
            Ok::<_, E2eError>((sender, ping_pong, closed))
        })?;
        Ok(Self {
            runtime,
            sender,
            ping_pong,
            closed,
            capabilities,
            token: token.map(str::to_owned),
        })
    }
}

impl Channel for H2Channel {
    fn ping(&mut self) -> Result<ChannelEvent> {
        let Self {
            runtime,
            ping_pong,
            closed,
            ..
        } = self;
        let Some(mut handle) = ping_pong.take() else {
            // The handle is lost only when an earlier ping ended with the connection.
            return Ok(ChannelEvent::Closed);
        };
        let task = runtime.spawn(async move {
            let pong = handle.ping(Ping::opaque()).await;
            (handle, pong)
        });
        // A connection that ends first leaves the ping to fail on its own; its handle is dropped.
        let settled = runtime.block_on(async {
            tokio::select! {
                biased;
                settled = task => Some(settled),
                _ = closed.wait_for(|closed| *closed) => None,
            }
        });
        match settled {
            None => Ok(ChannelEvent::Closed),
            Some(settled) => {
                let (returned, pong) =
                    settled.map_err(E2eError::system_from("the ping task failed"))?;
                *ping_pong = Some(returned);
                Ok(match pong {
                    Ok(_) => ChannelEvent::PingAcknowledged,
                    Err(_) => ChannelEvent::Closed,
                })
            }
        }
    }

    fn request(&mut self) -> Result<Reply> {
        let sender = self.sender.clone();
        let capabilities = self.capabilities.clone();
        let token = self.token.clone();
        self.runtime
            .block_on(get_capabilities(sender, &capabilities, token.as_deref()))
    }
}

/// One Capabilities request on the connection: its headers are sent, its body is not yet.
struct Exchange {
    /// The server's answer, once it arrives.
    response: ResponseFuture,
    /// The request's body, still open.
    body: SendStream<Bytes>,
}

impl Exchange {
    /// Sends the request's headers on the connection behind `sender`, presenting `token` when
    /// given. `None` when the connection is gone.
    ///
    /// # Errors
    ///
    /// When the request cannot be built.
    async fn open(
        sender: SendRequest<Bytes>,
        uri: &str,
        token: Option<&str>,
    ) -> Result<Option<Self>> {
        let Ok(mut sender) = sender.ready().await else {
            return Ok(None);
        };
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, "application/grpc")
            .header("te", "trailers");
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let request = builder
            .body(())
            .map_err(E2eError::system_from("building the request"))?;
        let Ok((response, body)) = sender.send_request(request, false) else {
            return Ok(None);
        };
        Ok(Some(Self { response, body }))
    }

    /// Sends the request's empty message and classifies the server's answer by `grpc-status`.
    /// The answer decides the reply, even when the server sent it before reading the body. A
    /// connection that is gone has no answer, so it is [`Reply::Closed`].
    async fn reply(mut self) -> Reply {
        // A server may answer at the headers and then reset the unread stream with NO_ERROR
        // (RFC 9113 §8.1), which fails this send: the answer, not the body, decides the reply.
        let _ = self
            .body
            .send_data(Bytes::from_static(&EMPTY_MESSAGE), true);
        let Ok(response) = self.response.await else {
            return Reply::Closed;
        };
        let (parts, mut body) = response.into_parts();
        if let Some(status) = grpc_status(&parts.headers) {
            return classify(Some(status));
        }
        while let Some(chunk) = body.data().await {
            let Ok(chunk) = chunk else {
                return Reply::Closed;
            };
            if body.flow_control().release_capacity(chunk.len()).is_err() {
                return Reply::Closed;
            }
        }
        match body.trailers().await {
            Ok(trailers) => classify(trailers.as_ref().and_then(grpc_status)),
            Err(_) => Reply::Closed,
        }
    }
}

/// Sends one Capabilities request and classifies its answer by `grpc-status`. A connection that
/// is gone is [`Reply::Closed`], not an error.
async fn get_capabilities(
    sender: SendRequest<Bytes>,
    uri: &str,
    token: Option<&str>,
) -> Result<Reply> {
    Ok(match Exchange::open(sender, uri, token).await? {
        Some(exchange) => exchange.reply().await,
        None => Reply::Closed,
    })
}

/// The `grpc-status` carried by `headers`, when it has a valid one.
fn grpc_status(headers: &HeaderMap) -> Option<u32> {
    headers.get("grpc-status")?.to_str().ok()?.parse().ok()
}

/// The outcome of a request, given the `grpc-status` the server answered with.
fn classify(status: Option<u32>) -> Reply {
    match status {
        Some(GRPC_UNAUTHENTICATED) => Reply::Unauthenticated,
        Some(GRPC_PERMISSION_DENIED) => Reply::Denied,
        _ => Reply::Served,
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use gang_clock::SystemClock;
    use gang_types::Url;
    use http::HeaderMap;
    use http::HeaderValue;
    use tokio::io::DuplexStream;
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    use super::{Exchange, H2Channel, classify, grpc_status};
    use crate::identity::{Channel, ChannelEvent, Reply};
    use crate::{errors::E2eError, tls_client};

    /// The Capabilities URI the test connections request.
    const CAPABILITIES: &str =
        "http://gang.test/build.bazel.remote.execution.v2.Capabilities/GetCapabilities";

    #[test]
    fn silent_tls_peer_times_out() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = Url::parse(&format!("https://{}/", listener.local_addr()?))?;
        let (release, wait) = mpsc::channel::<()>();
        let server = std::thread::spawn(move || -> std::io::Result<()> {
            let (_socket, _) = listener.accept()?;
            let _ = wait.recv();
            Ok(())
        });
        let config = tls_client::client_config(None, &[b"h2"], Arc::new(SystemClock::new()))?;
        let timeout = Duration::from_millis(1);
        let result = H2Channel::connect(Arc::new(config), &endpoint, None, timeout);
        drop(release);
        server.join().map_err(|_| "server panicked")??;
        assert!(matches!(result, Err(E2eError::Timeout { waited, .. }) if waited == timeout));
        Ok(())
    }

    /// A server on one end of an in-memory connection. It answers every request at its headers
    /// with a trailers-only `grpc-status: 7`, as the server's gate refuses a revoked principal,
    /// and leaves the request's body unread, which resets the stream with NO_ERROR. It drops the
    /// connection when `close` fires or the client ends the connection.
    async fn serve(io: DuplexStream, close: oneshot::Receiver<()>) {
        let Ok(mut connection) = h2::server::handshake(io).await else {
            return;
        };
        tokio::select! {
            biased;
            _ = close => {}
            () = async {
                while let Some(Ok((request, mut respond))) = connection.accept().await {
                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc")
                        .header("grpc-status", super::GRPC_PERMISSION_DENIED.to_string())
                        .body(())
                        .expect("the response is valid");
                    let _ = respond.send_response(response, true);
                    drop(request);
                }
            } => {}
        }
    }

    /// A client connection to a [`serve`]r over an in-memory stream.
    struct Pair {
        channel: H2Channel,
        close: Option<oneshot::Sender<()>>,
        server: JoinHandle<()>,
    }

    impl Pair {
        /// Opens the connection, with the server's end running on the channel's runtime.
        fn open() -> Self {
            let runtime: Runtime = Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("the test runtime starts");
            let (client_io, server_io) = tokio::io::duplex(64 * 1024);
            let (close, close_rx) = oneshot::channel();
            let server = runtime.spawn(serve(server_io, close_rx));
            let channel =
                H2Channel::establish(runtime, client_io, CAPABILITIES.to_owned(), None, None)
                    .expect("the connection is established");
            Self {
                channel,
                close: Some(close),
                server,
            }
        }

        /// Closes the server's end of the connection, and waits until the server has dropped it.
        fn close_server(&mut self) {
            if let Some(close) = self.close.take() {
                let _ = close.send(());
            }
            self.channel
                .runtime
                .block_on(&mut self.server)
                .expect("the server task ends");
        }
    }

    #[test]
    fn grpc_status_maps_to_reply() {
        assert_eq!(classify(Some(16)), Reply::Unauthenticated);
        assert_eq!(classify(Some(7)), Reply::Denied);
        assert_eq!(classify(Some(0)), Reply::Served);
        assert_eq!(classify(None), Reply::Served);
    }

    #[test]
    fn grpc_status_is_read_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("grpc-status", HeaderValue::from_static("16"));
        assert_eq!(grpc_status(&headers), Some(16));
        assert_eq!(grpc_status(&HeaderMap::new()), None);
    }

    #[test]
    fn pings_are_acknowledged_while_the_server_is_open() {
        let mut pair = Pair::open();
        for _ in 0..2 {
            assert_eq!(
                pair.channel.ping().expect("the ping is sent"),
                ChannelEvent::PingAcknowledged
            );
        }
        pair.close_server();
    }

    #[test]
    fn a_ping_ends_closed_once_the_server_has_closed_the_connection() {
        let mut pair = Pair::open();
        pair.close_server();
        assert_eq!(
            pair.channel.ping().expect("the ping ends"),
            ChannelEvent::Closed
        );
    }

    #[test]
    fn a_request_on_a_closed_connection_has_no_reply() {
        let mut pair = Pair::open();
        pair.close_server();
        assert_eq!(
            pair.channel.request().expect("the request ends"),
            Reply::Closed
        );
    }

    /// An answer received before the request body is sent is still the reply.
    #[test]
    fn an_answer_sent_before_the_request_body_is_the_reply() {
        let mut pair = Pair::open();
        let sender = pair.channel.sender.clone();
        let reply = pair.channel.runtime.block_on(async move {
            let mut exchange = Exchange::open(sender, CAPABILITIES, None)
                .await
                .expect("the request is built")
                .expect("the connection is open");
            // The server answers at the headers and resets the unread stream: the body is sent
            // only once the client has seen that reset.
            let reset = std::future::poll_fn(|cx| exchange.body.poll_reset(cx))
                .await
                .expect("the stream is reset");
            assert_eq!(reset, h2::Reason::NO_ERROR);
            exchange.reply().await
        });
        assert_eq!(reply, Reply::Denied);
        pair.close_server();
    }
}
