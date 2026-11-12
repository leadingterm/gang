//! The session cookie the harness mints for the dashboard scenarios: its name per endpoint scheme
//! and the token it carries, which the server accepts only when it is a `session` token signed
//! with its own key.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::Timestamp;
use gang_e2e::{E2eError, SessionCookie, cookie_name, session_cookie};
use gang_types::Url;
use p256::SecretKey;
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// What a test returns: any error fails it.
type TestResult<T = ()> = Result<T, Box<dyn Error>>;

/// The instant the cookies are minted at, in seconds.
const NOW: u64 = 1_700_000_000;

/// How long the cookies are valid, in seconds.
const LIFETIME: u64 = 600;

/// The principal the cookies are minted for.
const PRINCIPAL: &str = "e2e:administrator";

fn key() -> TestResult<SigningKey> {
    let secret = SecretKey::from_bytes(&[0x11_u8; 32].into())?;
    Ok(SigningKey::from(secret))
}

/// Writes `key` as PKCS#8 DER into `directory`, the way the harness provisions the signer key.
fn write_key(directory: &Path, key: &SigningKey) -> TestResult<PathBuf> {
    let path = directory.join("ca.key");
    let der = key.to_pkcs8_der()?;
    std::fs::write(&path, der.as_bytes())?;
    Ok(path)
}

fn endpoint(text: &str) -> TestResult<Url> {
    Ok(Url::parse(text)?)
}

fn mint(key_file: &Path, endpoint: &Url) -> Result<SessionCookie, E2eError> {
    session_cookie(
        key_file,
        endpoint,
        PRINCIPAL,
        Timestamp::from_unix_millis(NOW * 1000),
        Duration::from_secs(LIFETIME),
    )
}

/// The decoded JSON of a segment of a compact token.
fn segment(token: &str, index: usize) -> TestResult<Value> {
    let text = token
        .split('.')
        .nth(index)
        .ok_or("the token lacks the segment")?;
    let bytes = URL_SAFE_NO_PAD.decode(text)?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 0x0f])
        .filter_map(|nibble| char::from_digit(u32::from(nibble), 16))
        .collect()
}

#[test]
fn an_https_endpoint_gets_the_host_prefixed_cookie() -> TestResult {
    assert_eq!(
        cookie_name(&endpoint("https://127.0.0.1:43211/")?),
        "__Host-gang_session"
    );
    Ok(())
}

#[test]
fn a_plain_http_endpoint_gets_the_plain_cookie() -> TestResult {
    assert_eq!(
        cookie_name(&endpoint("http://127.0.0.1:43211/")?),
        "gang_session"
    );
    Ok(())
}

#[test]
fn the_header_presents_the_name_and_the_value() {
    let cookie = SessionCookie {
        name: "gang_session".to_owned(),
        value: "a.b.c".to_owned(),
    };
    assert_eq!(cookie.header(), "gang_session=a.b.c");
}

#[test]
fn the_cookie_carries_a_session_token_of_the_principal() -> TestResult {
    let directory = tempfile::tempdir()?;
    let key_file = write_key(directory.path(), &key()?)?;
    let cookie = mint(&key_file, &endpoint("https://127.0.0.1:43211/")?)?;

    assert_eq!(cookie.name, "__Host-gang_session");
    let claims = segment(&cookie.value, 1)?;
    assert_eq!(claims["iss"], "https://127.0.0.1:43211");
    assert_eq!(claims["aud"], "gang");
    assert_eq!(claims["sub"], PRINCIPAL);
    assert_eq!(claims["use"], "session");
    assert_eq!(claims["iat"], NOW);
    assert_eq!(claims["nbf"], NOW);
    assert_eq!(claims["exp"], NOW + LIFETIME);
    Ok(())
}

#[test]
fn the_issuer_is_the_origin_of_the_endpoint() -> TestResult {
    let directory = tempfile::tempdir()?;
    let key_file = write_key(directory.path(), &key()?)?;
    for (endpoint_text, origin) in [
        ("http://127.0.0.1:8080/ui/", "http://127.0.0.1:8080"),
        ("https://gang.example.test/", "https://gang.example.test"),
        (
            "https://gang.example.test:443/ui",
            "https://gang.example.test",
        ),
    ] {
        let cookie = mint(&key_file, &endpoint(endpoint_text)?)?;
        assert_eq!(segment(&cookie.value, 1)?["iss"], origin, "{endpoint_text}");
    }
    Ok(())
}

#[test]
fn the_header_names_the_key_the_way_the_server_does() -> TestResult {
    let directory = tempfile::tempdir()?;
    let key = key()?;
    let key_file = write_key(directory.path(), &key)?;
    let cookie = mint(&key_file, &endpoint("http://127.0.0.1:43211/")?)?;

    let spki = key.verifying_key().to_public_key_der()?;
    let header = segment(&cookie.value, 0)?;
    assert_eq!(header["alg"], "ES256");
    assert_eq!(header["typ"], "JWT");
    let digest: [u8; 32] = Sha256::digest(spki.as_bytes()).into();
    assert_eq!(header["kid"], hex(&digest));
    Ok(())
}

#[test]
fn the_signature_verifies_against_the_signing_key() -> TestResult {
    let directory = tempfile::tempdir()?;
    let key = key()?;
    let key_file = write_key(directory.path(), &key)?;
    let cookie = mint(&key_file, &endpoint("http://127.0.0.1:43211/")?)?;

    let (signing_input, signature) = cookie
        .value
        .rsplit_once('.')
        .ok_or("the token has no signature")?;
    let raw = URL_SAFE_NO_PAD.decode(signature)?;
    assert_eq!(raw.len(), 64, "ES256 signatures are the fixed r || s form");
    let signature = Signature::from_slice(&raw)?;
    VerifyingKey::from(&key).verify(signing_input.as_bytes(), &signature)?;
    Ok(())
}

#[test]
fn a_missing_key_file_is_an_io_error() -> TestResult {
    let directory = tempfile::tempdir()?;
    let error = mint(
        &directory.path().join("absent.key"),
        &endpoint("http://127.0.0.1:43211/")?,
    )
    .expect_err("there is no key");
    assert!(matches!(error, E2eError::Io { .. }), "{error:?}");
    Ok(())
}

#[test]
fn a_file_that_is_not_a_key_is_a_system_error() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("ca.key");
    std::fs::write(&path, b"not a key")?;
    let error =
        mint(&path, &endpoint("http://127.0.0.1:43211/")?).expect_err("the file is not a key");
    assert!(matches!(error, E2eError::System { .. }), "{error:?}");
    Ok(())
}
