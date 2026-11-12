//! The HTTP client used for discovery, keys and token exchange.

use std::sync::Arc;

use gang_clock::Clock;
use gang_tls::{
    HttpClient, HttpError, HttpProtocol, HttpReply, HttpRequest, HttpScheme, HttpSettings,
    HttpTransport, RequestBody, ServerTrust,
};
use gang_types::Url;
use http::{Method, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use url::form_urlencoded;

use crate::config::OidcConfig;
use crate::errors::OidcError;

/// An HTTP client that only talks to issuers, built on `gang-tls`: TLS 1.3 against the bundled
/// web roots for `https` issuers, no redirects, a total timeout and a body size cap.
pub(crate) struct Http {
    client: HttpClient,
}

impl Http {
    /// Builds the client. Performs no I/O.
    ///
    /// An `https` issuer is reached over TLS with the bundled web roots; a loopback `http`
    /// issuer is reached in plaintext. The configured timeout bounds one whole request,
    /// including reading the body, and `max_response_size` caps every response body.
    pub(crate) fn new(config: &OidcConfig, clock: Arc<dyn Clock>) -> Result<Self, OidcError> {
        let transport = if HttpScheme::of(config.issuer_url.as_url()) == Some(HttpScheme::Https) {
            HttpTransport::Tls {
                trust: ServerTrust::Bundled,
            }
        } else {
            HttpTransport::Plain
        };
        let client = HttpClient::try_new(&HttpSettings {
            transport,
            protocol: HttpProtocol::Negotiated,
            timeout: config.http.timeout,
            max_body: config.http.max_response_size,
            clock,
        })?;
        Ok(Self { client })
    }

    /// Sends a GET and decodes the JSON body.
    pub(crate) async fn get_json<T: DeserializeOwned>(&self, url: &Url) -> Result<T, OidcError> {
        self.send(Method::GET, url, None).await
    }

    /// Sends a form-encoded POST and decodes the JSON body.
    pub(crate) async fn post_form_json<T: DeserializeOwned>(
        &self,
        url: &Url,
        form: &[(&str, &str)],
    ) -> Result<T, OidcError> {
        let encoded = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form)
            .finish();
        let body = RequestBody {
            content_type: "application/x-www-form-urlencoded",
            bytes: encoded.into(),
        };
        self.send(Method::POST, url, Some(body)).await
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        url: &Url,
        body: Option<RequestBody>,
    ) -> Result<T, OidcError> {
        let request = HttpRequest {
            method,
            url,
            bearer: None,
            headers: &[],
            body,
        };
        let reply = match self.client.send(request).await {
            Ok(reply) => reply,
            // The status decides the error even when its body is too large to read.
            Err(HttpError::BodyTooLarge { status, .. }) if !status.is_success() => {
                return Err(failure(status, &[]));
            }
            Err(error) => return Err(error.into()),
        };
        decode(&reply)
    }
}

/// The `error` member of an OAuth error response, when present as a string.
#[derive(Deserialize)]
struct ErrorBody {
    error: Option<String>,
}

fn decode<T: DeserializeOwned>(reply: &HttpReply) -> Result<T, OidcError> {
    if !reply.status.is_success() {
        return Err(failure(reply.status, &reply.body));
    }
    serde_json::from_slice(&reply.body).map_err(OidcError::Body)
}

fn failure(status: StatusCode, body: &[u8]) -> OidcError {
    if let Some(error) = serde_json::from_slice::<ErrorBody>(body)
        .ok()
        .and_then(|parsed| parsed.error)
    {
        return OidcError::OAuth { error };
    }
    if status.is_server_error() {
        OidcError::ServerError {
            status: status.as_u16(),
        }
    } else {
        OidcError::UnexpectedStatus {
            status: status.as_u16(),
        }
    }
}
