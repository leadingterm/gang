//! The browser session cookie of the dashboard, minted for a principal the way the server's token
//! plugin mints one: an ES256 JWT that names the principal in `sub` and the use `session` in its
//! `use` claim, signed by the deployment's signing key.
//!
//! The in-process issuer cannot vouch for a session: the server accepts a session only when it is
//! signed with its own key, which the harness provisioned and still holds.

use std::fs;
use std::path::Path;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::Timestamp;
use gang_types::{CaPin, Url};
use gang_wire::CookieNames;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::{DecodePrivateKey, EncodePublicKey};
use serde_json::json;

use crate::errors::{E2eError, Result};

/// The audience of every token the server issues.
const SESSION_AUDIENCE: &str = "gang";

/// A cookie a browser would send: its name and its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCookie {
    /// The cookie name, which depends on the scheme of the endpoint.
    pub name: String,
    /// The session token.
    pub value: String,
}

impl SessionCookie {
    /// The value of the `Cookie` header that presents this cookie.
    #[must_use]
    pub fn header(&self) -> String {
        format!("{}={}", self.name, self.value)
    }
}

/// The name of the session cookie of the server at `endpoint`.
#[must_use]
pub fn cookie_name(endpoint: &Url) -> &'static str {
    CookieNames::for_https(endpoint.scheme() == "https").session
}

/// The intended use of a signed token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenUsage {
    /// A bearer credential of a client.
    Client,
    /// A browser session.
    Session,
}

impl TokenUsage {
    const fn claim(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Session => "session",
        }
    }
}

/// Mints the session cookie of `principal` (`provider:principal`) for the server at `endpoint`,
/// valid from `now` for `lifetime` and signed with the PKCS#8 DER key in `key_file`.
///
/// # Errors
///
/// [`E2eError::Io`] when the key file cannot be read, and [`E2eError::System`] when it is not a
/// P-256 key or the key's public half cannot be encoded.
pub fn session_cookie(
    key_file: &Path,
    endpoint: &Url,
    principal: &str,
    now: Timestamp,
    lifetime: Duration,
) -> Result<SessionCookie> {
    let token = signed_token(
        key_file,
        endpoint,
        principal,
        TokenUsage::Session,
        now,
        lifetime,
    )?;
    Ok(SessionCookie {
        name: cookie_name(endpoint).to_owned(),
        value: token,
    })
}

/// Mints a token for `principal` for the server at `endpoint`, signed with the key in `key_file`.
///
/// # Errors
///
/// [`E2eError::Io`] when the key file cannot be read, and [`E2eError::System`] when it is not a
/// P-256 key or the key's public half cannot be encoded.
pub fn signed_token(
    key_file: &Path,
    endpoint: &Url,
    principal: &str,
    usage: TokenUsage,
    issued_at: Timestamp,
    lifetime: Duration,
) -> Result<String> {
    let der =
        fs::read(key_file).map_err(E2eError::io(format!("reading {}", key_file.display())))?;
    let key = SigningKey::from_pkcs8_der(&der).map_err(E2eError::system_from(format!(
        "{} is not a PKCS#8 P-256 key",
        key_file.display()
    )))?;
    let spki = key
        .verifying_key()
        .to_public_key_der()
        .map_err(E2eError::system_from(
            "encoding the signing key's public half",
        ))?;
    let pin = CaPin::of_spki(spki.as_bytes());
    let kid = pin.digest_hex().to_string();

    let issued_at = issued_at.as_unix_millis() / 1000;
    let header = json!({ "alg": "ES256", "typ": "JWT", "kid": kid });
    let claims = json!({
        "iss": endpoint.origin().ascii_serialization(),
        "aud": SESSION_AUDIENCE,
        "sub": principal,
        "iat": issued_at,
        "nbf": issued_at,
        "exp": issued_at + lifetime.as_secs(),
        "use": usage.claim(),
    });
    let signing_input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let signature: Signature = key.sign(signing_input.as_bytes());
    Ok(format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use super::{TokenUsage, signed_token};
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use gang_clock::Timestamp;
    use gang_types::Url;
    use p256::ecdsa::SigningKey;
    use p256::pkcs8::EncodePrivateKey;
    use serde_json::Value;
    use std::time::Duration;

    #[test]
    fn client_token_has_client_use_and_expiration() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let key = SigningKey::from_slice(&[1; 32])?;
        let der = key.to_pkcs8_der()?;
        let key_file = temporary.path().join("key.der");
        std::fs::write(&key_file, der.as_bytes())?;
        let issued = Timestamp::from_unix_millis(1_700_000_000_000);
        let lifetime = Duration::from_secs(300);
        let token = signed_token(
            &key_file,
            &Url::parse("https://example.test")?,
            "provider:client",
            TokenUsage::Client,
            issued,
            lifetime,
        )?;
        let payload = token.split('.').nth(1).ok_or("missing payload")?;
        let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
        let iat = issued.as_unix_millis() / 1000;
        assert_eq!(claims["use"], "client");
        assert_eq!(claims["exp"], iat + lifetime.as_secs());
        Ok(())
    }
}
