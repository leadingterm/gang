//! Browser session tokens and cookie-bearing requests for the gate tests.

use std::sync::Arc;
use std::time::Duration;

use gang_authn::{TokenKey, TokenUse, issuer_of, mint};
use gang_clock::{Clock, TestClock};
use gang_spi::CertificateSignerPort;
use gang_testkit::TestSigner;
use gang_types::{AccessToken, Identity, Url};
use http::Request;
use http::header::{AUTHORIZATION, COOKIE};
use http::request::Parts;

/// The origin the TLS test settings serve. Its origin is the issuer of every test token.
const TLS_ENDPOINT: &str = "https://gang.example.test";

/// The lifetime of a session token minted by [`session_token`]: 10 minutes.
pub const SESSION_TTL: Duration = Duration::from_mins(10);

/// A browser session token for `identity`, minted at the clock's instant under the test signer
/// for the TLS endpoint's issuer.
///
/// # Panics
///
/// Panics when the test signer cannot mint a token.
pub async fn session_token(clock: &Arc<TestClock>, identity: &Identity) -> AccessToken {
    let signer = TestSigner::authority().expect("the test signer builds");
    let der = signer.public_key().await.expect("test public key encodes");
    let key = TokenKey::from_spki(&der).expect("test public key loads");
    let endpoint = Url::parse(TLS_ENDPOINT).expect("test endpoint is a valid URL");
    let issued = mint(
        &signer,
        &key,
        &issuer_of(&endpoint),
        identity,
        clock.wall(),
        SESSION_TTL,
        TokenUse::Session,
    )
    .await
    .expect("session token mints");
    issued.token
}

/// The parts of a `method` request for `path`, carrying `cookie` as a `name=value` pair in its
/// `Cookie` header and `bearer` as an `Authorization: Bearer` header, for driving the public
/// gate directly.
///
/// # Panics
///
/// Panics when `method` or `path` is not a valid request line.
#[must_use]
pub fn cookie_parts(
    method: &str,
    path: &str,
    cookie: Option<(&str, &AccessToken)>,
    bearer: Option<&AccessToken>,
) -> Parts {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some((name, token)) = cookie {
        builder = builder.header(COOKIE, format!("{name}={}", token.expose()));
    }
    if let Some(token) = bearer {
        builder = builder.header(AUTHORIZATION, format!("Bearer {}", token.expose()));
    }
    builder
        .body(())
        .expect("test request is well formed")
        .into_parts()
        .0
}
