//! The real [`ServerApi`]: the public listener of a [`Stack`], reached over server-TLS that
//! trusts only the deployment's CA pin.

use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use gang_clock::SystemClock;
use gang_tls::{HttpProtocol, HttpRequest, HttpTransport, RequestBody, ServerTrust};
use gang_types::{CaPin, Url};
use gang_wire::routes::WATCH_STATE;
use http::header::{AUTHORIZATION, COOKIE, HeaderName, HeaderValue};
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use x509_cert::der::Decode;

use crate::errors::{E2eError, Result};
use crate::h2_frames::H2Channel;
use crate::http::{Chunks, Http};
use crate::identity::{Admission, Credentials};
use crate::server::{Answer, Exit, Handshake, Leaf, Presented, RawStream, Request, ServerApi};
use crate::session::SessionCookie;
use crate::stack::Stack;
use crate::tls_client::{self, ALPN_H2};

/// The open `GET /v1/state:watch` response of a [`Stack`].
pub struct ServerStream {
    /// The streamed response and the runtime that drives it.
    response: Chunks,
}

impl RawStream for ServerStream {
    fn status(&self) -> u16 {
        self.response.status().as_u16()
    }

    fn next_chunk_within(&mut self, budget: Duration) -> Result<Option<Vec<u8>>> {
        self.response
            .next_within(budget, "the state stream to send a chunk")
            .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
    }
}

/// The client configuration that trusts only the deployment's CA pin, offering `h2`.
///
/// # Errors
///
/// [`E2eError::System`] when the TLS configuration is rejected.
fn pinned_config(pin: &CaPin) -> Result<rustls::ClientConfig> {
    gang_tls::client_config(
        ServerTrust::Pin(pin),
        None,
        &[ALPN_H2],
        Arc::new(SystemClock::new()),
    )
    .map_err(E2eError::system_from("configuring tls"))
}

/// Joins the target to the endpoint and supplies the request to the HTTP client.
fn with_request<T>(
    endpoint: &Url,
    request: &Request,
    send: impl FnOnce(HttpRequest<'_>) -> Result<T>,
) -> Result<T> {
    let location = endpoint
        .join(&request.target)
        .map_err(E2eError::parse("the request url"))?;
    let mut headers = Vec::with_capacity(request.headers.len() + 1);
    let credential = match &request.presented {
        Presented::Nothing => None,
        Presented::Bearer(token) => Some((AUTHORIZATION, format!("Bearer {token}"))),
        Presented::Cookie(cookie) => Some((COOKIE, cookie.header())),
    };
    if let Some((name, value)) = credential {
        let mut value = HeaderValue::from_str(&value)
            .map_err(E2eError::system_from("building the credential header"))?;
        value.set_sensitive(true);
        headers.push((name, value));
    }
    for (name, value) in &request.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(E2eError::system_from("building a request header name"))?;
        let mut value = HeaderValue::from_str(value)
            .map_err(E2eError::system_from("building a request header value"))?;
        if name == AUTHORIZATION || name == COOKIE {
            value.set_sensitive(true);
        }
        headers.push((name, value));
    }
    let body = request.body.as_ref().map(|body| RequestBody {
        content_type: body.content_type,
        bytes: body.bytes.clone().into(),
    });
    send(HttpRequest {
        method: request.method.clone(),
        url: &location,
        bearer: None,
        headers: &headers,
        body,
    })
}

/// Returns the pinned client of the running server, building it on first use after each start.
fn client(stack: &Stack<'_>) -> Result<Arc<Http>> {
    let mut slot = stack.server_http.lock();
    if let Some(client) = slot.as_ref() {
        return Ok(Arc::clone(client));
    }
    let pin = stack.ca_pin()?;
    let http = Arc::new(Http::try_new(
        HttpTransport::Tls {
            trust: ServerTrust::Pin(&pin),
        },
        HttpProtocol::Http2,
        stack.settings().command_timeout,
    )?);
    *slot = Some(Arc::clone(&http));
    Ok(http)
}

/// Sends a request and reads its entire response.
fn exchange(api: &Http, endpoint: &Url, request: &Request) -> Result<Answer> {
    let response = with_request(endpoint, request, |request| api.send(request))?;
    Ok(Answer {
        status: response.status.as_u16(),
        headers: response
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect(),
        body: response.body.to_vec(),
    })
}

impl ServerApi for Stack<'_> {
    type Stream = ServerStream;
    type Held = H2Channel;

    fn administrator_credentials(&self) -> Result<Credentials> {
        Stack::administrator(self)?.credentials(&self.ca_pin()?)
    }

    fn cookie_of(&self, who: &Credentials) -> Result<SessionCookie> {
        Stack::session_cookie(self, &who.principal)
    }

    fn send(&self, request: &Request) -> Result<Answer> {
        let api = client(self)?;
        exchange(&api, &self.endpoint()?, request)
    }

    fn watch(&self, cookie: &SessionCookie) -> Result<Self::Stream> {
        let endpoint = self.endpoint()?;
        let api = client(self)?;
        let request = Request::get(WATCH_STATE.template)
            .presenting(Presented::Cookie(cookie.clone()))
            .header("accept", "text/event-stream");
        let response = with_request(&endpoint, &request, |request| api.open(request))?;
        Ok(ServerStream { response })
    }

    fn leaf(&self) -> Result<Handshake> {
        let endpoint = self.endpoint()?;
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
        let connector = TlsConnector::from(Arc::new(pinned_config(&self.ca_pin()?)?));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(E2eError::system_from("building the leaf handshake runtime"))?;
        let timeout = self.settings().handshake_timeout;
        runtime.block_on(tls_client::within(
            timeout,
            "the listener's tls handshake",
            async move {
                let tcp = TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(E2eError::io(format!("connecting to {host}:{port}")))?;
                let tls = match connector.connect(server_name, tcp).await {
                    Ok(tls) => tls,
                    Err(error) => {
                        let refusal =
                            Handshake::Refused(gang_types::error_chain(&error).to_string());
                        return Ok::<Handshake, E2eError>(refusal);
                    }
                };
                let (_, connection) = tls.get_ref();
                let der = connection
                    .peer_certificates()
                    .and_then(|chain| chain.first())
                    .ok_or_else(|| E2eError::system("the server presented no certificate"))?;
                let certificate = x509_cert::Certificate::from_der(der.as_ref())
                    .map_err(E2eError::parse("the listener's leaf certificate"))?;
                let tbs = certificate.tbs_certificate();
                let validity = tbs.validity();
                Ok::<Handshake, E2eError>(Handshake::Verified(Leaf {
                    serial: tbs.serial_number().as_bytes().to_vec(),
                    not_before: SystemTime::UNIX_EPOCH + validity.not_before.to_unix_duration(),
                    not_after: SystemTime::UNIX_EPOCH + validity.not_after.to_unix_duration(),
                }))
            },
        ))
    }

    fn hold(&self, credentials: &Credentials) -> Result<Self::Held> {
        let config = Arc::new(pinned_config(&self.ca_pin()?)?);
        let endpoint = self.endpoint()?;
        let timeout = self.settings().handshake_timeout;
        match H2Channel::connect(config, &endpoint, Some(&credentials.token), timeout)? {
            Admission::Accepted(channel) => Ok(channel),
            Admission::Refused => Err(E2eError::system("the server refused the held connection")),
        }
    }

    fn wait_until(&self, at: SystemTime) {
        // The one wait on a deadline the deployment published itself (its leaf's validity),
        // never a poll.
        if let Ok(left) = at.duration_since(SystemTime::now()) {
            std::thread::sleep(left);
        }
    }

    fn terminate(&self, timeout: Duration) -> Result<Exit> {
        let status = self.terminate_server(timeout)?;
        match status.code() {
            Some(code) => Ok(Exit::Code(code)),
            None => match ExitStatusExt::signal(&status) {
                Some(signal) => Ok(Exit::Signal(signal)),
                None => Err(E2eError::system(format!("the server ended with {status}"))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    use gang_tls::{HttpProtocol, HttpTransport, ServerTrust};
    use gang_types::{CaPin, Url};

    use super::{exchange, with_request};
    use crate::errors::E2eError;
    use crate::http::Http;
    use crate::server::{Presented, Request};

    /// The short budget used to exercise a stalled operation.
    const SHORT_TIMEOUT: Duration = Duration::from_millis(50);

    #[test]
    fn forwards_credentials_headers_and_json_body() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Url::parse("https://example.test/")?;
        let request = Request::get("/resource")
            .presenting(Presented::Bearer("secret".to_owned()))
            .header("accept", "application/json")
            .body("application/json", b"{}".to_vec());
        with_request(&endpoint, &request, |request| {
            assert_eq!(request.url.as_str(), "https://example.test/resource");
            assert!(request.headers.iter().any(|(name, value)| {
                name == http::header::AUTHORIZATION
                    && value == "Bearer secret"
                    && value.is_sensitive()
            }));
            assert!(request.headers.iter().any(|(name, value)| {
                name == http::header::ACCEPT && value == "application/json"
            }));
            assert!(
                !request
                    .headers
                    .iter()
                    .any(|(name, _)| name == http::header::CONTENT_TYPE)
            );
            let body = request.body.expect("the request has a body");
            assert_eq!(body.content_type, "application/json");
            assert_eq!(body.bytes.as_ref(), b"{}");
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn silent_tls_peer_times_out_before_response_headers() -> Result<(), Box<dyn std::error::Error>>
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = Url::parse(&format!("https://{}/", listener.local_addr()?))?;
        let (release, wait) = mpsc::channel::<()>();
        let server = std::thread::spawn(move || -> std::io::Result<()> {
            let (_socket, _) = listener.accept()?;
            let _ = wait.recv();
            Ok(())
        });
        let pin = CaPin::new([0; 32]);
        let api = Http::try_new(
            HttpTransport::Tls {
                trust: ServerTrust::Pin(&pin),
            },
            HttpProtocol::Http2,
            SHORT_TIMEOUT,
        )?;
        let request = Request::get("/");
        let outcome = exchange(&api, &endpoint, &request);
        drop(release);
        server.join().expect("the server thread does not panic")?;
        assert!(
            matches!(outcome, Err(E2eError::Timeout { waited, .. }) if waited == SHORT_TIMEOUT)
        );
        Ok(())
    }
}
