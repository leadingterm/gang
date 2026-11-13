//! The HTTP client for a Gang server: its server information, token minting, certificate
//! issuance, revocation, and principal restoration.

use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use gang_clock::{Clock, Timestamp};
use gang_tls::{
    HttpClient, HttpError, HttpProtocol, HttpReply, HttpRequest, HttpScheme, HttpSettings,
    HttpTransport, RequestBody, ServerTrust,
};
use gang_types::{AccessToken, CertSerial, DeviceSecret, Identity, Url};
use gang_wire::routes::{self, Method, Route};
use gang_wire::{
    AuthenticateRequest, AuthenticateResponse, Certificate, CreateCertificateRequest,
    DeviceAuthorization, ErrorReason, ErrorResponse, Principal, RedeemDeviceAuthorizationRequest,
    RevokePrincipalResponse, Server,
};
use http::header::{DATE, RETRY_AFTER};
use http::{HeaderMap, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::errors::ClientError;
use crate::options::ClientOptions;

/// An HTTP client bound to one Gang server.
///
/// Only `http` and `https` base URLs are accepted. Over `https`, TLS verifies the server
/// according to the [`ServerTrust`] given to [`ServerApi::try_new`]. Over `http` there is no TLS
/// and no certificate store is read. The client holds no bearer token: each request takes the
/// bearer token to present, if any. Redirects are not followed, each request is bounded by the
/// HTTP timeout, and every response body is capped at the configured size.
pub struct ServerApi {
    http: HttpClient,
    base: Url,
    /// The longest server error text quoted in a message, in characters.
    max_error_chars: NonZeroUsize,
}

impl ServerApi {
    /// Builds a client for the server at `base`. Performs no I/O.
    ///
    /// `trust` decides how an `https` server's certificate is verified. Plain `http` ignores
    /// it. An `https` server is offered `h2` then `http/1.1` in ALPN; a plain `http` server is
    /// reached over HTTP/2 with prior knowledge.
    ///
    /// `clock` judges whether an `https` server's certificate is within its validity period.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::UnsupportedServerUrl`] when `base` is neither `http` nor
    /// `https`, before any TLS configuration is built; [`ClientError::Tls`] when the TLS
    /// configuration is rejected; and [`ClientError::Transport`] when the HTTP client cannot
    /// be built.
    pub fn try_new(
        base: &Url,
        trust: ServerTrust<'_>,
        options: &ClientOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ClientError> {
        let scheme = HttpScheme::of(base)
            .ok_or_else(|| ClientError::UnsupportedServerUrl { url: base.clone() })?;
        let (transport, protocol) = match scheme {
            HttpScheme::Https => (HttpTransport::Tls { trust }, HttpProtocol::Negotiated),
            HttpScheme::Http => (HttpTransport::Plain, HttpProtocol::Http2),
        };
        let http = HttpClient::try_new(&HttpSettings {
            transport,
            protocol,
            timeout: options.http_timeout,
            max_body: options.max_response_size,
            clock,
        })
        .map_err(|error| client_error(base, error))?;
        Ok(Self {
            http,
            base: base.clone(),
            max_error_chars: options.max_error_message_chars,
        })
    }

    /// Fetches the server's public information from its unauthenticated endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RateLimited`] for a 429 answer, [`ClientError::Server`] for another
    /// non-2xx answer, [`ClientError::BodyTooLarge`]
    /// for an oversized body, [`ClientError::Body`] when the body is not a server
    /// document, and [`ClientError::Transport`] when the server cannot be reached.
    pub async fn server(
        &self,
        bearer: Option<&AccessToken>,
    ) -> Result<Server<serde_json::Value>, ClientError> {
        self.execute(
            bearer,
            &routes::GET_SERVER,
            routes::GET_SERVER.template,
            None,
        )
        .await
    }

    /// Fetches the server document together with the server's wall clock, read from the `Date`
    /// header of the answer (`None` when it is absent or not an HTTP date).
    ///
    /// # Errors
    ///
    /// As [`ServerApi::server`].
    pub async fn server_and_time(
        &self,
        bearer: Option<&AccessToken>,
    ) -> Result<(Server<serde_json::Value>, Option<Timestamp>), ClientError> {
        self.execute_timed(
            bearer,
            &routes::GET_SERVER,
            routes::GET_SERVER.template,
            None,
        )
        .await
    }

    /// Exchanges an identity-provider proof for a short-lived Gang bearer token.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RateLimited`] for a 429 answer, [`ClientError::Server`] for another
    /// non-2xx answer (for example when the proof is
    /// rejected; the server's reason is in its message), [`ClientError::BodyTooLarge`] for an
    /// oversized body, [`ClientError::Body`] when the body is not an authentication response,
    /// and [`ClientError::Transport`] when the server cannot be reached.
    pub async fn authenticate(
        &self,
        request: &AuthenticateRequest,
    ) -> Result<AuthenticateResponse, ClientError> {
        let result = self
            .execute(
                None,
                &routes::AUTHENTICATE_SESSION,
                routes::AUTHENTICATE_SESSION.template,
                Some(encode(request)?),
            )
            .await;
        match result {
            Err(ClientError::LoginRejected)
                if matches!(&request.credential, gang_wire::Credential::Proof(_)) =>
            {
                Err(ClientError::Server {
                    status: 401,
                    message: "proof rejected".to_owned(),
                })
            }
            other => other,
        }
    }

    /// Starts a device authorization.
    ///
    /// # Errors
    ///
    /// Returns a transport, server, or response decoding error.
    pub async fn create_device_authorization(&self) -> Result<DeviceAuthorization, ClientError> {
        self.execute(
            None,
            &routes::CREATE_DEVICE_AUTHORIZATION,
            routes::CREATE_DEVICE_AUTHORIZATION.template,
            None,
        )
        .await
    }

    /// Redeems a device authorization with its secret.
    ///
    /// # Errors
    ///
    /// Returns a pending, expired, rate-limit, transport, server, or decoding error.
    pub async fn redeem_device_authorization(
        &self,
        code: &str,
        secret: &DeviceSecret,
    ) -> Result<AuthenticateResponse, ClientError> {
        let path = routes::REDEEM_DEVICE_AUTHORIZATION.path(&code.to_owned());
        match self
            .execute(
                None,
                &routes::REDEEM_DEVICE_AUTHORIZATION,
                &path,
                Some(encode(&RedeemDeviceAuthorizationRequest {
                    secret: secret.clone(),
                })?),
            )
            .await
        {
            Err(ClientError::Server { status: 404, .. }) => Err(ClientError::DeviceLoginExpired),
            result => result,
        }
    }

    /// Submits a certificate signing request and returns the issued certificate.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RateLimited`] for a 429 answer, [`ClientError::Server`] for another
    /// non-2xx answer (the server's refusal reason
    /// is in its message), [`ClientError::BodyTooLarge`] for an oversized body,
    /// [`ClientError::Body`] when the body is not a certificate, and
    /// [`ClientError::Transport`] when the server cannot be reached.
    pub async fn create_certificate(
        &self,
        bearer: Option<&AccessToken>,
        request: &CreateCertificateRequest,
    ) -> Result<Certificate, ClientError> {
        self.execute(
            bearer,
            &routes::CREATE_CERTIFICATE,
            routes::CREATE_CERTIFICATE.template,
            Some(encode(request)?),
        )
        .await
    }

    /// Revokes one certificate. The server decides whether the bearer token's holder may
    /// revoke.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RateLimited`] for a 429 answer, [`ClientError::Server`] for another
    /// non-2xx answer (the server's refusal reason
    /// is in its message), [`ClientError::BodyTooLarge`] for an oversized body,
    /// [`ClientError::Body`] when the body is not a certificate, and
    /// [`ClientError::Transport`] when the server cannot be reached.
    pub async fn revoke_certificate(
        &self,
        bearer: Option<&AccessToken>,
        serial: &CertSerial,
    ) -> Result<Certificate, ClientError> {
        let path = routes::REVOKE_CERTIFICATE.path(serial);
        self.execute(bearer, &routes::REVOKE_CERTIFICATE, &path, None)
            .await
    }

    /// Revokes a principal and the certificates it holds. The server decides whether the
    /// bearer token's holder may revoke.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RateLimited`] for a 429 answer, [`ClientError::Server`] for another
    /// non-2xx answer (the server's refusal reason
    /// is in its message), [`ClientError::BodyTooLarge`] for an oversized body,
    /// [`ClientError::Body`] when the body is not a revocation response, and
    /// [`ClientError::Transport`] when the server cannot be reached.
    pub async fn revoke_principal(
        &self,
        bearer: Option<&AccessToken>,
        identity: &Identity,
    ) -> Result<RevokePrincipalResponse, ClientError> {
        let path = routes::REVOKE_PRINCIPAL.path(identity);
        self.execute(bearer, &routes::REVOKE_PRINCIPAL, &path, None)
            .await
    }

    /// Restores a revoked principal. The server decides whether the bearer token's holder may
    /// restore.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RateLimited`] for a 429 answer, [`ClientError::Server`] for another
    /// non-2xx answer (the server's refusal reason
    /// is in its message), [`ClientError::BodyTooLarge`] for an oversized body,
    /// [`ClientError::Body`] when the body is not a principal, and
    /// [`ClientError::Transport`] when the server cannot be reached.
    pub async fn restore_principal(
        &self,
        bearer: Option<&AccessToken>,
        identity: &Identity,
    ) -> Result<Principal, ClientError> {
        let path = routes::RESTORE_PRINCIPAL.path(identity);
        self.execute(bearer, &routes::RESTORE_PRINCIPAL, &path, None)
            .await
    }

    /// Sends the request for `route` to `path`, with `json` as its body, `bearer` as its
    /// authorization, and decodes a success answer as `T`.
    async fn execute<T: DeserializeOwned>(
        &self,
        bearer: Option<&AccessToken>,
        route: &Route,
        path: &str,
        json: Option<Bytes>,
    ) -> Result<T, ClientError> {
        self.execute_timed(bearer, route, path, json)
            .await
            .map(|(value, _)| value)
    }

    /// As [`ServerApi::execute`], with the server's clock reading from the answer's `Date`
    /// header.
    async fn execute_timed<T: DeserializeOwned>(
        &self,
        bearer: Option<&AccessToken>,
        route: &Route,
        path: &str,
        json: Option<Bytes>,
    ) -> Result<(T, Option<Timestamp>), ClientError> {
        let reply = self.send(bearer, route, path, json).await?;
        let server_time = server_time(&reply.headers);
        if reply.status == StatusCode::TOO_MANY_REQUESTS {
            return Err(ClientError::RateLimited {
                retry_after: retry_after_header(&reply.headers)
                    .or_else(|| envelope_retry_delay(&reply.body)),
            });
        }
        if !reply.status.is_success() {
            if let Ok(envelope) = serde_json::from_slice::<ErrorResponse>(&reply.body) {
                match (reply.status, envelope.error.reason()) {
                    (StatusCode::BAD_REQUEST, Some(ErrorReason::AuthorizationPending)) => {
                        return Err(ClientError::AuthorizationPending);
                    }
                    (StatusCode::NOT_FOUND, Some(ErrorReason::DeviceAuthorizationNotFound)) => {
                        return Err(ClientError::DeviceLoginExpired);
                    }
                    (StatusCode::UNAUTHORIZED, Some(ErrorReason::ProofRejected))
                        if route.template == routes::AUTHENTICATE_SESSION.template =>
                    {
                        return Err(ClientError::LoginRejected);
                    }
                    _ => {}
                }
            }
            return Err(ClientError::Server {
                status: reply.status.as_u16(),
                message: server_message(reply.status, &reply.body, self.max_error_chars),
            });
        }
        let value = serde_json::from_slice(&reply.body).map_err(ClientError::Body)?;
        Ok((value, server_time))
    }

    /// Sends one request to the server, presenting `bearer`, and reads the whole answer.
    async fn send(
        &self,
        bearer: Option<&AccessToken>,
        route: &Route,
        path: &str,
        json: Option<Bytes>,
    ) -> Result<HttpReply, ClientError> {
        let url = self.base.join(path)?;
        let method = match route.method {
            Method::Get => http::Method::GET,
            Method::Post => http::Method::POST,
        };
        let body = json.map(|bytes| RequestBody {
            content_type: "application/json",
            bytes,
        });
        self.http
            .send(HttpRequest {
                method,
                url: &url,
                bearer,
                headers: &[],
                body,
            })
            .await
            .map_err(|error| client_error(&self.base, error))
    }
}

/// The client error for a failed HTTP exchange with the server at `server`.
fn client_error(server: &Url, error: HttpError) -> ClientError {
    match error {
        HttpError::Tls(error) => ClientError::Tls(error),
        HttpError::BodyTooLarge { limit, .. } => ClientError::BodyTooLarge { limit },
        other => ClientError::transport(server, other),
    }
}

/// The server's clock reading in the `Date` header of an answer; `None` when the header is
/// absent or not an HTTP date.
fn server_time(headers: &HeaderMap) -> Option<Timestamp> {
    headers
        .get(DATE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_http_date)
}

/// Parses an HTTP `Date` header into a timestamp. `None` for text that is not an HTTP date or
/// that lies before the Unix epoch.
pub(crate) fn parse_http_date(text: &str) -> Option<Timestamp> {
    let time = httpdate::parse_http_date(text).ok()?;
    let millis = time.duration_since(UNIX_EPOCH).ok()?.as_millis();
    u64::try_from(millis).ok().map(Timestamp::from_unix_millis)
}

/// The wait a `Retry-After` header asks for, in whole seconds; `None` when the header is absent
/// or is not a number.
fn retry_after_header(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// Encodes a request body as JSON.
fn encode(request: &impl Serialize) -> Result<Bytes, ClientError> {
    serde_json::to_vec(request)
        .map(Bytes::from)
        .map_err(ClientError::Request)
}

/// The wait in the `RetryInfo` detail of the error envelope in `body`, if there is one.
fn envelope_retry_delay(body: &[u8]) -> Option<Duration> {
    serde_json::from_slice::<ErrorResponse>(body)
        .ok()?
        .error
        .retry_delay()
}

/// The human-readable reason in a non-2xx body: the `message` of an [`ErrorResponse`] when the
/// body is one, else the body text cut to `max_chars` characters, else the status reason.
fn server_message(status: StatusCode, body: &[u8], max_chars: NonZeroUsize) -> String {
    if let Ok(envelope) = serde_json::from_slice::<ErrorResponse>(body) {
        return envelope.error.message;
    }
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    if text.is_empty() {
        return status
            .canonical_reason()
            .map_or_else(|| status.to_string(), str::to_owned);
    }
    truncate_chars(text, max_chars).into_owned()
}

/// Borrows text within the character limit, or returns its prefix followed by an ellipsis.
#[must_use]
pub(crate) fn truncate_chars(text: &str, max_chars: NonZeroUsize) -> Cow<'_, str> {
    match text.char_indices().nth(max_chars.get()) {
        Some((cut, _)) => Cow::Owned(format!("{}...", &text[..cut])),
        None => Cow::Borrowed(text),
    }
}

#[cfg(test)]
mod tests {
    use gang_clock::{TestClock, Timestamp};
    use gang_tls::ServerTrust;
    use gang_types::{CaPin, Url};
    use gang_wire::{ErrorReason, ErrorResponse, Status};
    use http::header::RETRY_AFTER;
    use http::{HeaderMap, HeaderValue, StatusCode};

    use std::borrow::Cow;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use super::{ServerApi, parse_http_date, retry_after_header, server_message, truncate_chars};
    use crate::errors::ClientError;
    use crate::options::ClientOptions;

    fn max_chars() -> NonZeroUsize {
        ClientOptions::default().max_error_message_chars
    }

    #[test]
    fn truncate_chars_borrows_shorter_text() {
        let limit = NonZeroUsize::new(3).expect("3 is non-zero");
        assert!(matches!(truncate_chars("ab", limit), Cow::Borrowed("ab")));
    }

    #[test]
    fn truncate_chars_borrows_exact_length_text() {
        let limit = NonZeroUsize::new(3).expect("3 is non-zero");
        assert!(matches!(truncate_chars("abc", limit), Cow::Borrowed("abc")));
    }

    #[test]
    fn truncate_chars_owns_truncated_text() {
        let limit = NonZeroUsize::new(3).expect("3 is non-zero");
        assert!(matches!(truncate_chars("abcd", limit), Cow::Owned(text) if text == "abc..."));
    }

    #[test]
    fn truncate_chars_cuts_at_multibyte_boundary() {
        let limit = NonZeroUsize::new(2).expect("2 is non-zero");
        assert!(matches!(truncate_chars("aé界", limit), Cow::Owned(text) if text == "aé..."));
    }

    #[test]
    fn error_envelope_yields_its_message() {
        let body = serde_json::to_vec(&ErrorResponse::from(Status::new(
            ErrorReason::ProofRejected,
            "proof rejected",
            [],
        )))
        .expect("error serializes");
        assert_eq!(
            server_message(StatusCode::UNAUTHORIZED, &body, max_chars()),
            "proof rejected"
        );
    }

    #[test]
    fn plain_text_body_is_trimmed_and_used() {
        assert_eq!(
            server_message(StatusCode::BAD_GATEWAY, b"  upstream down\n", max_chars()),
            "upstream down"
        );
    }

    #[test]
    fn empty_body_uses_the_status_reason() {
        assert_eq!(
            server_message(StatusCode::NOT_FOUND, b"", max_chars()),
            "Not Found"
        );
    }

    #[test]
    fn long_text_body_is_cut_at_the_configured_limit() {
        let limit = NonZeroUsize::new(5).expect("5 is non-zero");

        let message = server_message(StatusCode::BAD_GATEWAY, b"abcdefgh", limit);

        assert_eq!(message, "abcde...");
    }

    #[test]
    fn parses_the_rfc_example_date() {
        let parsed = parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT");

        assert_eq!(parsed, Some(Timestamp::from_unix_millis(784_111_777_000)));
    }

    #[test]
    fn the_epoch_is_a_date() {
        let parsed = parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT");

        assert_eq!(parsed, Some(Timestamp::UNIX_EPOCH));
    }

    #[test]
    fn garbage_is_not_a_date() {
        assert_eq!(parse_http_date("next tuesday, probably"), None);
        assert_eq!(parse_http_date(""), None);
    }

    fn headers_with_retry_after(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static(value));
        headers
    }

    #[test]
    fn retry_after_seconds_are_the_wait() {
        let wait = retry_after_header(&headers_with_retry_after("7"));

        assert_eq!(wait, Some(std::time::Duration::from_secs(7)));
    }

    #[test]
    fn retry_after_seconds_are_trimmed() {
        let wait = retry_after_header(&headers_with_retry_after("  12 "));

        assert_eq!(wait, Some(std::time::Duration::from_secs(12)));
    }

    #[test]
    fn retry_after_http_date_is_not_a_wait() {
        let wait = retry_after_header(&headers_with_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"));

        assert_eq!(wait, None);
    }

    #[test]
    fn absent_retry_after_is_no_wait() {
        assert_eq!(retry_after_header(&HeaderMap::new()), None);
    }

    fn pin() -> CaPin {
        CaPin::of_spki(b"test")
    }

    fn clock() -> Arc<TestClock> {
        Arc::new(TestClock::new(Timestamp::UNIX_EPOCH))
    }

    #[test]
    fn non_http_scheme_is_unsupported() {
        let base = Url::parse("ftp://gang.example/").expect("the url parses");
        let pin = pin();
        let error = ServerApi::try_new(
            &base,
            ServerTrust::Pin(&pin),
            &ClientOptions::default(),
            clock(),
        )
        .err()
        .expect("an ftp base is refused");
        match error {
            ClientError::UnsupportedServerUrl { url } => assert_eq!(url, base),
            other => panic!("expected UnsupportedServerUrl, got {other}"),
        }
    }

    #[test]
    fn plain_http_base_is_accepted() {
        let base = Url::parse("http://gang.example/").expect("the url parses");
        let pin = pin();
        let result = ServerApi::try_new(
            &base,
            ServerTrust::Pin(&pin),
            &ClientOptions::default(),
            clock(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn https_base_with_pin_is_accepted() {
        let base = Url::parse("https://gang.example/").expect("the url parses");
        let pin = pin();
        let result = ServerApi::try_new(
            &base,
            ServerTrust::Pin(&pin),
            &ClientOptions::default(),
            clock(),
        );
        assert!(result.is_ok());
    }
}
