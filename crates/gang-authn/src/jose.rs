//! JOSE compact serialization: segment encoding, the wire shapes of the header and claims, and
//! the ES256 signature form.

use std::borrow::Cow;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_spi::PortError;
use p256::ecdsa::Signature;
use serde::{Deserialize, Deserializer, Serialize};

use crate::errors::TokenError;
use crate::token::TokenUse;

/// The only signature algorithm Gang issues or accepts.
pub(crate) const ALGORITHM: &str = "ES256";

/// The `typ` a token may carry; any other type is refused.
pub(crate) const JWT_TYPE: &str = "JWT";

/// The largest token accepted, in bytes. Bounds the work a caller can make verification do.
pub(crate) const MAX_TOKEN_BYTES: usize = 8 * 1024;

/// A base64url segment decodes to at most three bytes per four token bytes.
pub(crate) const MAX_SEGMENT_DECODED: usize = MAX_TOKEN_BYTES * 3 / 4;

/// The header of a token being minted. Field order is the wire order.
#[derive(Serialize)]
pub(crate) struct HeaderOut<'a> {
    pub alg: &'static str,
    pub typ: &'static str,
    pub kid: &'a str,
}

/// The header of a token being verified.
#[derive(Deserialize)]
pub(crate) struct HeaderIn<'a> {
    #[serde(borrow)]
    pub alg: Cow<'a, str>,
    #[serde(default, borrow, deserialize_with = "deserialize_optional_borrowed")]
    pub typ: Option<Cow<'a, str>>,
    #[serde(borrow)]
    pub kid: Cow<'a, str>,
}

/// Borrows unescaped optional strings instead of allocating through `Option<Cow<str>>`.
fn deserialize_optional_borrowed<'de, D>(deserializer: D) -> Result<Option<Cow<'de, str>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Borrowed<'a>(#[serde(borrow)] Cow<'a, str>);

    Ok(Option::<Borrowed<'de>>::deserialize(deserializer)?.map(|value| value.0))
}

/// The claims of a token being minted. Field order is the wire order.
#[derive(Serialize)]
pub(crate) struct ClaimsOut<'a> {
    pub iss: &'a str,
    pub aud: &'a str,
    pub sub: &'a str,
    pub iat: u64,
    pub nbf: u64,
    pub exp: u64,
    #[serde(rename = "use")]
    pub usage: TokenUse,
}

/// The claims of a token being verified.
#[derive(Deserialize)]
pub(crate) struct ClaimsIn<'a> {
    #[serde(borrow)]
    pub iss: Cow<'a, str>,
    #[serde(borrow)]
    pub aud: Cow<'a, str>,
    #[serde(borrow)]
    pub sub: Cow<'a, str>,
    pub nbf: u64,
    pub exp: u64,
    #[serde(rename = "use")]
    pub usage: TokenUse,
}

/// Splits a compact token into its header, claims, and signature segments.
///
/// # Errors
///
/// [`TokenError::Malformed`] when the token is longer than [`MAX_TOKEN_BYTES`] or does not have
/// exactly three segments.
pub(crate) fn segments(token: &str) -> Result<(&str, &str, &str), TokenError> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(TokenError::Malformed);
    }
    let mut parts = token.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(header), Some(claims), Some(signature), None) => Ok((header, claims, signature)),
        _ => Err(TokenError::Malformed),
    }
}

/// Decodes a base64url segment without padding.
///
/// # Errors
///
/// [`TokenError::Malformed`] when the segment is not valid base64url.
pub(crate) fn decode_segment_into<'b>(
    segment: &str,
    buffer: &'b mut [u8; MAX_SEGMENT_DECODED],
) -> Result<&'b [u8], TokenError> {
    let len = URL_SAFE_NO_PAD
        .decode_slice(segment, buffer)
        .map_err(|_| TokenError::Malformed)?;
    Ok(&buffer[..len])
}

/// Decodes the signature segment of a token. A segment with a character outside the base64url
/// alphabet is not base64url at all; one in the alphabet that does not decode, such as a
/// truncated signature, is a bad signature.
///
/// # Errors
///
/// [`TokenError::Malformed`] when the segment has a character outside the base64url alphabet;
/// [`TokenError::Signature`] when it is in the alphabet but does not decode.
pub(crate) fn decode_signature_segment(segment: &str) -> Result<[u8; 64], TokenError> {
    if !segment
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(TokenError::Malformed);
    }
    let mut signature = [0; 64];
    let len = URL_SAFE_NO_PAD
        .decode_slice(segment, &mut signature)
        .map_err(|_| TokenError::Signature)?;
    if len != signature.len() {
        return Err(TokenError::Signature);
    }
    Ok(signature)
}

/// Decodes a base64url segment and parses its JSON.
///
/// # Errors
///
/// [`TokenError::Malformed`] when the segment is not base64url JSON of type `T`.
pub(crate) fn parse_segment<'a, T: Deserialize<'a>>(
    segment: &str,
    buffer: &'a mut [u8; MAX_SEGMENT_DECODED],
) -> Result<T, TokenError> {
    let bytes = decode_segment_into(segment, buffer)?;
    serde_json::from_slice(bytes).map_err(|_| TokenError::Malformed)
}

/// Serializes `value` as JSON and encodes it as a base64url segment without padding.
///
/// # Errors
///
/// [`serde_json::Error`] when `value` cannot be serialized.
pub(crate) fn encode_segment<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(value)?))
}

/// Converts an ASN.1 DER ECDSA signature into the fixed-width `r || s` form JOSE uses, encoded
/// as a base64url segment.
///
/// # Errors
///
/// [`PortError::Backend`] when `der` is not a P-256 ECDSA signature.
pub(crate) fn encode_signature(der: &[u8]) -> Result<String, PortError> {
    let signature =
        Signature::from_der(der).map_err(|error| PortError::backend(error.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_borrows_unescaped_strings_and_accepts_escapes() {
        let mut buffer = [0; MAX_SEGMENT_DECODED];
        let segment = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256","typ":"JWT","kid":"key"}"#);
        let header: HeaderIn<'_> = parse_segment(&segment, &mut buffer).expect("header parses");
        assert!(matches!(header.alg, Cow::Borrowed("ES256")));
        assert!(matches!(header.typ, Some(Cow::Borrowed("JWT"))));
        assert!(matches!(header.kid, Cow::Borrowed("key")));

        let segment = URL_SAFE_NO_PAD
            .encode(r#"{"alg":"ES\u0032\u0035\u0036","typ":"J\u0057T","kid":"key"}"#);
        let header: HeaderIn<'_> = parse_segment(&segment, &mut buffer).expect("header parses");
        assert_eq!(header.alg, "ES256");
        assert_eq!(header.typ.as_deref(), Some("JWT"));
        assert_eq!(header.kid, "key");
    }
}
