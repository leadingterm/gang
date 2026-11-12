//! What `gang doctor` learns from the Gang server: its public information and wall clock, the
//! stored login the server was logged in with, and whether the credential helper Bazel runs
//! returns a token the server accepts. The classification of failures lives here so the checks
//! see typed faults.

use std::sync::Arc;

use gang_clock::{Clock, Timestamp};
use gang_tls::ServerTrust;
use gang_types::{AccessToken, CaPin, Url, error_chain, parse_bearer};
use gang_wire::Server;
use serde_json::Value;
use tokio::process::Command;

use crate::api::ServerApi;
use crate::errors::ClientError;
use crate::home::ClientHome;
use crate::options::ClientOptions;
use crate::session::Session;

/// What the server answered: its public information, and its wall clock when the answer
/// carried a usable `Date` header.
pub struct ServerReading {
    /// The server's public information.
    pub info: Server<Value>,
    /// The server's wall clock, read from the `Date` header of the answer.
    pub server_time: Option<Timestamp>,
}

/// Why the server could not be read, classified for the doctor's report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerFault {
    /// The server was reached, but its certificate is not trusted (or the TLS setup is
    /// rejected). The text is the failure chain.
    Untrusted(String),
    /// The server could not be reached at all. The text is the failure chain.
    Unreachable(String),
    /// The server answered with a non-success status.
    Refused {
        /// The HTTP status code.
        status: u16,
        /// The server's message.
        message: String,
    },
    /// The server answered, but not with Gang server information. The text is the failure
    /// chain.
    NotGang(String),
}

impl ServerFault {
    /// Classifies a failed request to the server.
    pub(crate) fn classify(error: ClientError) -> Self {
        match error {
            ClientError::Tls(source) => Self::Untrusted(error_chain(&source).to_string()),
            ClientError::Transport { source, .. } if gang_tls::is_tls_failure(&source) => {
                Self::Untrusted(error_chain(&source).to_string())
            }
            ClientError::Transport { source, .. } => {
                Self::Unreachable(error_chain(&source).to_string())
            }
            ClientError::Server { status, message } => Self::Refused { status, message },
            ClientError::Body(_) | ClientError::BodyTooLarge { .. } => {
                Self::NotGang(error_chain(&error).to_string())
            }
            other => Self::Unreachable(error_chain(&other).to_string()),
        }
    }
}

/// Why a credential helper did not yield a token the server accepts, classified for the
/// doctor's report. The texts never contain the helper's output or the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialFault {
    /// The helper could not be started. The text is the operating system's reason.
    NotRunnable(String),
    /// The helper could not be given the request or exited unsuccessfully. The text is the
    /// reason.
    Failed(String),
    /// The helper did not finish within the HTTP timeout.
    TimedOut {
        /// The timeout, in whole seconds.
        seconds: u64,
    },
    /// The helper printed something Bazel cannot use. The text says what is wrong.
    BadReply(String),
    /// The server answered the presented token with a non-success status.
    Refused {
        /// The HTTP status code.
        status: u16,
        /// The server's message.
        message: String,
    },
    /// The token could not be presented to the server. The text is the failure chain.
    Unreachable(String),
}

/// Reads the server and runs the credential helper. Implemented by [`ClientProbe`]; the doctor
/// depends only on this trait so it can be tested without a network or a process.
pub trait ServerProbe: Send + Sync {
    /// Reads the server's public information and wall clock.
    fn read(&self) -> impl Future<Output = Result<ServerReading, ServerFault>> + Send;

    /// Runs `<helper> get` the way Bazel does, with `{"uri": uri}` on standard input, then
    /// presents the token it returns to the server. The token is never printed.
    fn credential(
        &self,
        helper: &str,
        uri: &Url,
    ) -> impl Future<Output = Result<(), CredentialFault>> + Send;
}

/// The server probe of a real client: its HTTP API bound to the server, and what it needs to
/// present a credential helper's token to the server.
pub struct ClientProbe {
    api: ServerApi,
    options: ClientOptions,
    clock: Arc<dyn Clock>,
}

impl ClientProbe {
    /// Binds a probe to `server`, verifying its certificate against `ca_pin` when one is
    /// given and against the system roots otherwise.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`ServerApi::try_new`].
    pub fn try_new(
        server: &Url,
        ca_pin: Option<&CaPin>,
        options: &ClientOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ClientError> {
        let api = ServerApi::try_new(server, ServerTrust::from(ca_pin), options, clock.clone())?;
        Ok(Self {
            api,
            options: options.clone(),
            clock,
        })
    }

    /// Runs `<helper> get` with the request on standard input and returns its standard output.
    async fn run_helper(&self, helper: &str, uri: &Url) -> Result<Vec<u8>, CredentialFault> {
        let mut command = Command::new(helper);
        command.arg("get");
        let request = serde_json::json!({ "uri": uri.as_str() }).to_string();
        let timeout = self.options.http_timeout.get();
        let output = self
            .clock
            .timeout(
                timeout,
                gang_os::run_bounded(
                    &mut command,
                    Some(request.as_bytes()),
                    self.options.max_response_size,
                ),
            )
            .await
            .map_err(|_| CredentialFault::TimedOut {
                seconds: timeout.as_secs(),
            })?
            .map_err(|error| match error {
                gang_os::RunError::Spawn { source, .. } => {
                    CredentialFault::NotRunnable(source.to_string())
                }
                gang_os::RunError::Io { source, .. } => CredentialFault::Failed(source.to_string()),
                gang_os::RunError::StdoutTooLarge { .. } => CredentialFault::BadReply(
                    "the reply is larger than the response size limit".to_owned(),
                ),
            })?;
        if !output.status.success() {
            return Err(CredentialFault::Failed(output.status.to_string()));
        }
        Ok(output.stdout)
    }

    /// Presents `token` to the server with an authenticated server-information request. The
    /// gate answers an invalid or revoked token with a refusal even on that open endpoint.
    async fn present(&self, token: &AccessToken) -> Result<(), CredentialFault> {
        match self.api.server(Some(token)).await {
            Ok(_) => Ok(()),
            Err(ClientError::Server { status, message }) => {
                Err(CredentialFault::Refused { status, message })
            }
            Err(error) => Err(CredentialFault::Unreachable(
                error_chain(&error).to_string(),
            )),
        }
    }
}

impl ServerProbe for ClientProbe {
    async fn read(&self) -> Result<ServerReading, ServerFault> {
        self.api
            .server_and_time(None)
            .await
            .map(|(server, server_time)| ServerReading {
                info: server,
                server_time,
            })
            .map_err(ServerFault::classify)
    }

    async fn credential(&self, helper: &str, uri: &Url) -> Result<(), CredentialFault> {
        let stdout = self.run_helper(helper, uri).await?;
        let token = reply_token(&stdout)?;
        self.present(&token).await
    }
}

/// Extracts the Bearer token from a credential helper's reply: a JSON object with an object
/// member `headers`, whose `authorization` member (name compared ignoring ASCII case) holds a
/// string or an array of strings. The first `Bearer` value wins; its token is returned.
pub(crate) fn reply_token(stdout: &[u8]) -> Result<AccessToken, CredentialFault> {
    let reply: Value = serde_json::from_slice(stdout).map_err(|_| shape_error())?;
    let headers = reply
        .get("headers")
        .and_then(Value::as_object)
        .ok_or_else(shape_error)?;
    headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .flat_map(|(_, value)| authorization_values(value))
        .find_map(parse_bearer)
        .map(AccessToken::new)
        .ok_or_else(|| {
            CredentialFault::BadReply("the reply has no Bearer authorization header".to_owned())
        })
}

/// The string values of an authorization member: an array of strings, or a lone string.
fn authorization_values(value: &Value) -> Vec<&str> {
    match value {
        Value::String(text) => vec![text.as_str()],
        Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

/// The fault for a reply whose shape Bazel cannot use.
fn shape_error() -> CredentialFault {
    CredentialFault::BadReply("the reply is not a JSON object with `headers`".to_owned())
}

/// The server and CA pin of the stored login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLogin {
    /// The server URL the login authenticates against.
    pub server: Url,
    /// The pin of the Gang CA public key, or `None` when the server is trusted through the
    /// system roots or is served over plain HTTP.
    pub ca_pin: Option<CaPin>,
}

/// Reads the stored login, or `None` when the client has never logged in.
///
/// # Errors
///
/// Returns the errors of [`Session::load`].
pub fn stored_login(home: &ClientHome) -> Result<Option<StoredLogin>, ClientError> {
    Ok(Session::load(home)?.map(|session| StoredLogin {
        server: session.server,
        ca_pin: session.ca_pin,
    }))
}

#[cfg(test)]
mod tests {
    use std::io::{self, ErrorKind};
    use std::sync::Arc;

    use axum::Router;
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use gang_clock::{Clock, TestClock, Timestamp};
    use gang_tls::{HttpError, TransportError};
    use gang_types::{AccessToken, Bytes, NonZeroBytes, NonZeroDuration, ProviderName, Url};
    use http::StatusCode;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    use super::{
        ClientProbe, CredentialFault, ServerFault, ServerProbe, reply_token, stored_login,
    };
    use crate::errors::ClientError;
    use crate::options::ClientOptions;
    use crate::session::{Session, SessionKind};
    use crate::test_support::{ca_pin, home_in};

    fn rustls_text() -> String {
        rustls::Error::NoCertificatesPresented.to_string()
    }

    fn shape_error() -> CredentialFault {
        CredentialFault::BadReply("the reply is not a JSON object with `headers`".to_owned())
    }

    fn no_bearer() -> CredentialFault {
        CredentialFault::BadReply("the reply has no Bearer authorization header".to_owned())
    }

    #[test]
    fn classify_transport_tls_failure_is_untrusted() {
        let fault = ServerFault::classify(ClientError::transport(
            &Url::parse("https://gang.example.com").expect("the address is valid"),
            HttpError::Body(Box::new(io::Error::other(
                rustls::Error::NoCertificatesPresented,
            ))),
        ));

        assert_eq!(
            fault,
            ServerFault::Untrusted(format!(
                "reading the response body failed: {}",
                rustls_text()
            )),
        );
    }

    #[test]
    fn classify_transport_plain_io_is_unreachable() {
        let fault = ServerFault::classify(ClientError::transport(
            &Url::parse("https://gang.example.com").expect("the address is valid"),
            HttpError::Body(Box::new(io::Error::new(
                ErrorKind::ConnectionReset,
                "reset",
            ))),
        ));

        assert_eq!(
            fault,
            ServerFault::Unreachable("reading the response body failed: reset".to_owned()),
        );
    }

    #[test]
    fn classify_tls_is_untrusted() {
        let fault = ServerFault::classify(ClientError::Tls(TransportError::Tls(
            rustls::Error::NoCertificatesPresented,
        )));

        assert_eq!(
            fault,
            ServerFault::Untrusted(format!("tls engine error: {}", rustls_text()))
        );
    }

    #[test]
    fn classify_timed_out_transport_is_unreachable() {
        let fault = ServerFault::classify(ClientError::transport(
            &Url::parse("https://gang.example.com").expect("the address is valid"),
            HttpError::TimedOut,
        ));

        assert_eq!(
            fault,
            ServerFault::Unreachable("request timed out".to_owned())
        );
    }

    #[test]
    fn classify_server_is_refused() {
        let fault = ServerFault::classify(ClientError::Server {
            status: 503,
            message: "down".to_owned(),
        });

        assert_eq!(
            fault,
            ServerFault::Refused {
                status: 503,
                message: "down".to_owned(),
            }
        );
    }

    #[test]
    fn classify_invalid_body_is_not_gang() {
        let source = serde_json::from_str::<u8>("x").expect_err("x is not a number");

        let fault = ServerFault::classify(ClientError::Body(source));

        assert!(matches!(
            &fault,
            ServerFault::NotGang(text) if text.starts_with("the server's response is not valid: ")
        ));
    }

    #[test]
    fn classify_oversized_body_is_not_gang() {
        let error = ClientError::BodyTooLarge {
            limit: NonZeroBytes::kib(1),
        };
        let text = error.to_string();

        assert_eq!(ServerFault::classify(error), ServerFault::NotGang(text));
    }

    #[test]
    fn classify_other_error_is_unreachable() {
        let fault = ServerFault::classify(ClientError::NotLoggedIn);

        assert_eq!(fault, ServerFault::Unreachable("not logged in".to_owned()));
    }

    #[test]
    fn bearer_value_yields_its_token() {
        let token =
            reply_token(br#"{"headers":{"Authorization":["Bearer abc"]}}"#).expect("bearer reply");

        assert_eq!(token.expose(), "abc");
    }

    #[test]
    fn header_name_and_scheme_ignore_ascii_case() {
        let token =
            reply_token(br#"{"headers":{"authorization":["bEARER abc"]}}"#).expect("bearer reply");

        assert_eq!(token.expose(), "abc");
    }

    #[test]
    fn lone_string_value_is_accepted() {
        let token =
            reply_token(br#"{"headers":{"AUTHORIZATION":"Bearer abc"}}"#).expect("bearer reply");

        assert_eq!(token.expose(), "abc");
    }

    #[test]
    fn bearer_with_extra_spaces_yields_its_token() {
        let token =
            reply_token(br#"{"headers":{"authorization":["Bearer  abc"]}}"#).expect("bearer reply");

        assert_eq!(token.expose(), "abc");
    }

    #[test]
    fn first_bearer_value_wins_among_several() {
        let token = reply_token(
            br#"{"headers":{"authorization":["Basic eDp5","Bearer first","Bearer second"]}}"#,
        )
        .expect("bearer reply");

        assert_eq!(token.expose(), "first");
    }

    #[test]
    fn reply_without_authorization_header_has_no_bearer() {
        let result = reply_token(br#"{"headers":{"accept":["*/*"]}}"#);

        assert_eq!(result.unwrap_err(), no_bearer());
    }

    #[test]
    fn basic_only_has_no_bearer() {
        let result = reply_token(br#"{"headers":{"authorization":["Basic eDp5"]}}"#);

        assert_eq!(result.unwrap_err(), no_bearer());
    }

    #[test]
    fn bearer_without_token_has_no_bearer() {
        let result = reply_token(br#"{"headers":{"authorization":["Bearer "]}}"#);

        assert_eq!(result.unwrap_err(), no_bearer());
    }

    #[test]
    fn reply_that_is_not_json_is_bad() {
        let result = reply_token(b"token: x");

        assert_eq!(result.unwrap_err(), shape_error());
    }

    #[test]
    fn reply_without_headers_is_bad() {
        let result = reply_token(br#"{"token":"x"}"#);

        assert_eq!(result.unwrap_err(), shape_error());
    }

    #[test]
    fn reply_with_non_object_headers_is_bad() {
        let result = reply_token(br#"{"headers":"Bearer abc"}"#);

        assert_eq!(result.unwrap_err(), shape_error());
    }

    #[test]
    fn no_stored_login_when_never_logged_in() {
        let tmp = TempDir::new().expect("temp dir");

        let stored = stored_login(&home_in(&tmp)).expect("load succeeds");

        assert_eq!(stored, None);
    }

    #[test]
    fn stored_login_reports_server_and_pin_after_store() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let server = Url::parse("https://gang.test").expect("valid url");
        let session = Session {
            server: server.clone(),
            ca_pin: Some(ca_pin()),
            provider: ProviderName::try_new("google").expect("valid provider name"),
            kind: SessionKind::Interactive {
                login: serde_json::json!({ "issuer": "https://idp.test" }),
            },
        };
        session.store(&home).expect("store succeeds");

        let stored = stored_login(&home).expect("load succeeds");

        assert_eq!(
            stored.map(|login| (login.server, login.ca_pin)),
            Some((server, Some(ca_pin())))
        );
    }

    /// The fake credential helper Bazel builds and exposes through `FAKE_CREDENTIAL_HELPER`.
    fn fake_helper() -> String {
        let raw = std::env::var_os("FAKE_CREDENTIAL_HELPER")
            .expect("FAKE_CREDENTIAL_HELPER must be set by the Bazel test_env");
        let path = std::env::current_dir()
            .expect("a working directory")
            .join(raw);
        path.to_str().expect("runfiles paths are UTF-8").to_owned()
    }

    /// The request URI that makes the fake helper run `mode` with `arg`.
    fn fake_request(mode: &str, arg: &str) -> Url {
        Url::parse(&format!("https://gang.test/{mode}/{arg}/")).expect("valid url")
    }

    /// The request URI that makes the fake helper answer `reply`, reading its request first
    /// when `mode` is `reply` and not when it is `unread`.
    fn fake_reply(mode: &str, reply: &[u8]) -> Url {
        fake_request(mode, &URL_SAFE_NO_PAD.encode(reply))
    }

    fn probe_at(server: &Url, options: &ClientOptions) -> ClientProbe {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(Timestamp::UNIX_EPOCH));
        ClientProbe::try_new(server, None, options, clock).expect("the probe binds")
    }

    /// A probe for helper tests, which never present a token to the server.
    fn helper_probe(options: &ClientOptions) -> ClientProbe {
        probe_at(
            &Url::parse("http://127.0.0.1:1/").expect("loopback url parses"),
            options,
        )
    }

    /// A plain HTTP server on loopback answering every request with `status` and `body`.
    async fn answering(status: StatusCode, body: &'static str) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding the fake server");
        let address = listener
            .local_addr()
            .expect("fake server has a local address");
        let app = Router::new().fallback(move || async move { (status, body) });
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Url::parse(&format!("http://{address}/")).expect("loopback url parses")
    }

    /// The address of a loopback port with nothing listening on it.
    async fn closed_port() -> Url {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding a port");
        let address = listener.local_addr().expect("a local address");
        drop(listener);
        Url::parse(&format!("http://{address}/")).expect("loopback url parses")
    }

    #[tokio::test]
    async fn helper_that_is_missing_is_not_runnable() {
        let dir = TempDir::new().expect("temp dir");
        let missing = dir.path().join("missing");
        let probe = helper_probe(&ClientOptions::default());

        let result = probe
            .run_helper(
                missing.to_str().expect("temp paths are UTF-8"),
                &fake_request("echo", "-"),
            )
            .await;

        assert!(matches!(result, Err(CredentialFault::NotRunnable(_))));
    }

    #[tokio::test]
    async fn helper_that_exits_unsuccessfully_is_failed() {
        let probe = helper_probe(&ClientOptions::default());

        let result = probe
            .run_helper(&fake_helper(), &fake_request("exit", "3"))
            .await;

        assert_eq!(
            result,
            Err(CredentialFault::Failed("exit status: 3".to_owned()))
        );
    }

    #[tokio::test]
    async fn reply_one_byte_over_the_limit_is_bad() {
        let options = ClientOptions {
            max_response_size: NonZeroBytes::new(Bytes::new(8)).expect("8 is non-zero"),
            ..ClientOptions::default()
        };
        let probe = helper_probe(&options);

        let result = probe
            .run_helper(&fake_helper(), &fake_reply("reply", b"123456789"))
            .await;

        assert_eq!(
            result,
            Err(CredentialFault::BadReply(
                "the reply is larger than the response size limit".to_owned()
            ))
        );
    }

    #[tokio::test]
    async fn reply_of_exactly_the_limit_is_accepted() {
        let options = ClientOptions {
            max_response_size: NonZeroBytes::new(Bytes::new(9)).expect("9 is non-zero"),
            ..ClientOptions::default()
        };
        let probe = helper_probe(&options);

        let result = probe
            .run_helper(&fake_helper(), &fake_reply("reply", b"123456789"))
            .await;

        assert_eq!(result, Ok(b"123456789".to_vec()));
    }

    #[tokio::test]
    async fn helper_that_answers_without_reading_its_request_is_judged_by_its_reply() {
        // A request larger than any pipe buffer cannot be written before the helper exits
        // without reading it, so the write always meets a broken pipe.
        let mut uri = fake_reply("unread", br#"{"headers":{}}"#);
        uri.set_path(&format!("{}{}", uri.path(), "a".repeat(1 << 20)));
        let probe = helper_probe(&ClientOptions::default());

        let result = probe.run_helper(&fake_helper(), &uri).await;

        assert_eq!(result, Ok(br#"{"headers":{}}"#.to_vec()));
    }

    #[tokio::test]
    async fn helper_gets_get_and_the_request_on_stdin() {
        let uri = fake_request("echo", "-");
        let probe = helper_probe(&ClientOptions::default());

        let result = probe.run_helper(&fake_helper(), &uri).await;

        assert_eq!(
            result,
            Ok(format!("get\n{{\"uri\":\"{uri}\"}}").into_bytes())
        );
    }

    #[tokio::test]
    async fn helper_that_never_finishes_times_out() {
        let options = ClientOptions {
            http_timeout: NonZeroDuration::from_secs(1),
            ..ClientOptions::default()
        };
        let clock = Arc::new(TestClock::new(Timestamp::UNIX_EPOCH));
        let server = Url::parse("http://127.0.0.1:1/").expect("loopback url parses");
        let probe =
            ClientProbe::try_new(&server, None, &options, clock.clone()).expect("the probe binds");
        let helper = fake_helper();
        let uri = fake_request("hang", "-");
        let future = probe.run_helper(&helper, &uri);
        tokio::pin!(future);
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(future.as_mut().poll(cx).is_pending())
        })
        .await;
        assert!(pending);
        clock.advance(options.http_timeout.get());
        assert_eq!(future.await, Err(CredentialFault::TimedOut { seconds: 1 }));
    }

    #[tokio::test]
    async fn present_refused_by_server_is_refused() {
        let server = answering(StatusCode::UNAUTHORIZED, "denied").await;
        let probe = probe_at(&server, &ClientOptions::default());

        let result = probe.present(&AccessToken::new("stale")).await;

        assert_eq!(
            result,
            Err(CredentialFault::Refused {
                status: 401,
                message: "denied".to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn present_to_a_closed_port_is_unreachable() {
        let server = closed_port().await;
        let probe = probe_at(&server, &ClientOptions::default());

        let result = probe.present(&AccessToken::new("token")).await;

        assert!(matches!(result, Err(CredentialFault::Unreachable(_))));
    }

    #[tokio::test]
    async fn read_refused_by_server_is_refused() {
        let server = answering(StatusCode::UNAUTHORIZED, "denied").await;
        let probe = probe_at(&server, &ClientOptions::default());

        let result = probe.read().await;

        assert!(matches!(
            result,
            Err(ServerFault::Refused { status: 401, ref message }) if message == "denied"
        ));
    }

    #[tokio::test]
    async fn credential_token_the_server_refuses_is_refused() {
        let helper = fake_helper();
        let server = answering(StatusCode::UNAUTHORIZED, "denied").await;
        let probe = probe_at(&server, &ClientOptions::default());

        let result = probe
            .credential(
                &helper,
                &fake_reply(
                    "reply",
                    br#"{"headers":{"authorization":["Bearer stale"]}}"#,
                ),
            )
            .await;

        assert_eq!(
            result,
            Err(CredentialFault::Refused {
                status: 401,
                message: "denied".to_owned(),
            })
        );
    }
}
