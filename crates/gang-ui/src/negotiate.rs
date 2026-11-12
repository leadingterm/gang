//! Content-coding negotiation over `Accept-Encoding` for embedded assets.

use http::HeaderValue;

use crate::Asset;

const BROTLI_CODING: &str = "br";
const GZIP_CODING: &str = "gzip";
const X_GZIP_CODING: &str = "x-gzip";
const WILDCARD_CODING: &str = "*";
const QUALITY_PARAM: &str = "q";
const MAX_QUALITY_DIGITS: usize = 3;

const BR_HEADER: HeaderValue = HeaderValue::from_static(BROTLI_CODING);
const GZIP_HEADER: HeaderValue = HeaderValue::from_static(GZIP_CODING);

/// Content coding selected for a response body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Encoding {
    /// Brotli-compressed variant (`br`).
    Brotli,
    /// Gzip-compressed variant (`gzip`).
    Gzip,
    /// The identity bytes, uncompressed.
    Identity,
}

impl Encoding {
    /// Picks the best coding the client accepts and the asset carries.
    ///
    /// Brotli is preferred over gzip; a missing or non-visible-ASCII header
    /// yields identity.
    #[must_use]
    pub(crate) fn choose(accept: Option<&HeaderValue>, asset: &Asset) -> Self {
        let Some(accept) = accept.and_then(|value| value.to_str().ok()) else {
            return Self::Identity;
        };
        let accepted = Accepted::parse(accept);
        if asset.br.is_some() && accepted.br.or(accepted.wildcard).unwrap_or(false) {
            return Self::Brotli;
        }
        if asset.gz.is_some() && accepted.gzip.or(accepted.wildcard).unwrap_or(false) {
            return Self::Gzip;
        }
        Self::Identity
    }

    /// The `Content-Encoding` header value, absent for identity.
    #[must_use]
    pub(crate) fn content_encoding(self) -> Option<HeaderValue> {
        match self {
            Self::Brotli => Some(BR_HEADER),
            Self::Gzip => Some(GZIP_HEADER),
            Self::Identity => None,
        }
    }

    /// The bytes to send for this coding, falling back to identity when the
    /// variant is absent.
    #[must_use]
    pub(crate) fn body(self, asset: &Asset) -> &'static [u8] {
        match self {
            Self::Brotli => asset.br.unwrap_or(asset.identity),
            Self::Gzip => asset.gz.unwrap_or(asset.identity),
            Self::Identity => asset.identity,
        }
    }
}

/// What an `Accept-Encoding` value says of each coding the bundle carries: `None` when unlisted,
/// `Some(acceptable)` otherwise; a coding listed more than once is acceptable only if every item
/// accepts it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Accepted {
    br: Option<bool>,
    gzip: Option<bool>,
    wildcard: Option<bool>,
}

impl Accepted {
    /// Scans the header, ignoring items with malformed quality values.
    fn parse(accept: &str) -> Self {
        let mut result = Self::default();
        for item in accept.split(',') {
            let mut parts = item.split(';');
            let name = parts.next().map_or("", str::trim);
            let Some(accepted) = quality(parts) else {
                continue;
            };
            let slot = if name.eq_ignore_ascii_case(BROTLI_CODING) {
                &mut result.br
            } else if name.eq_ignore_ascii_case(GZIP_CODING)
                || name.eq_ignore_ascii_case(X_GZIP_CODING)
            {
                &mut result.gzip
            } else if name == WILDCARD_CODING {
                &mut result.wildcard
            } else {
                continue;
            };
            *slot = Some(slot.map_or(accepted, |previous| previous && accepted));
        }
        result
    }
}

/// Whether the item's `q` parameter accepts the coding; `None` when malformed.
fn quality<'a>(params: impl Iterator<Item = &'a str>) -> Option<bool> {
    for param in params {
        let Some((key, value)) = param.split_once('=') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case(QUALITY_PARAM) {
            return parse_quality(value.trim());
        }
    }
    Some(true)
}

/// Parses a `qvalue` per RFC 9110: `0[.ddd]` or `1[.000]`.
fn parse_quality(value: &str) -> Option<bool> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > MAX_QUALITY_DIGITS || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match whole {
        "0" => Some(fraction.bytes().any(|b| b != b'0')),
        "1" if fraction.bytes().all(|b| b == b'0') => Some(true),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDENTITY: &[u8] = b"identity-bytes";
    const BR: &[u8] = b"br-bytes";
    const GZ: &[u8] = b"gz-bytes";

    fn asset(br: Option<&'static [u8]>, gz: Option<&'static [u8]>) -> Asset {
        Asset {
            path: "assets/app.js",
            content_type: "text/javascript; charset=utf-8",
            etag: "\"etag\"",
            identity: IDENTITY,
            br,
            gz,
            immutable: true,
        }
    }

    fn both() -> Asset {
        asset(Some(BR), Some(GZ))
    }

    fn gz_only() -> Asset {
        asset(None, Some(GZ))
    }

    fn br_only() -> Asset {
        asset(Some(BR), None)
    }

    fn none() -> Asset {
        asset(None, None)
    }

    /// Asserts the choice, the `Content-Encoding` header, and the body for an
    /// `Accept-Encoding` value (or an absent header when `accept` is `None`).
    fn expect(accept: Option<&str>, asset: &Asset, expected: Encoding, body: &[u8]) {
        let value = accept.map(|v| HeaderValue::from_str(v).expect("valid header value"));
        let chosen = Encoding::choose(value.as_ref(), asset);
        assert_eq!(chosen, expected, "choice for accept {accept:?}");

        let header = match expected {
            Encoding::Brotli => Some(HeaderValue::from_static("br")),
            Encoding::Gzip => Some(HeaderValue::from_static("gzip")),
            Encoding::Identity => None,
        };
        assert_eq!(
            chosen.content_encoding(),
            header,
            "content-encoding for accept {accept:?}"
        );
        assert_eq!(chosen.body(asset), body, "body for accept {accept:?}");
    }

    #[test]
    fn explicit_br_refusal_overrides_wildcard() {
        expect(Some("br;q=0, *"), &both(), Encoding::Gzip, GZ);
    }

    #[test]
    fn identity_only_is_identity() {
        expect(Some("identity"), &both(), Encoding::Identity, IDENTITY);
    }

    #[test]
    fn x_gzip_counts_as_gzip() {
        expect(Some("x-gzip"), &both(), Encoding::Gzip, GZ);
    }

    #[test]
    fn unparsable_quality_is_ignored() {
        expect(Some("br;q=abc"), &both(), Encoding::Identity, IDENTITY);
        expect(Some("br;q=abc, gzip"), &both(), Encoding::Gzip, GZ);
    }

    #[test]
    fn out_of_range_quality_is_ignored() {
        expect(Some("gzip;q=1.5"), &both(), Encoding::Identity, IDENTITY);
    }

    #[test]
    fn fractional_quality_is_accepted() {
        expect(Some("gzip;q=0.5"), &both(), Encoding::Gzip, GZ);
        expect(Some("gzip;q=0.000"), &both(), Encoding::Identity, IDENTITY);
    }

    #[test]
    fn non_utf8_header_is_identity() {
        let value = HeaderValue::from_bytes(b"gzip\xff").expect("obs-text is a valid value");
        let chosen = Encoding::choose(Some(&value), &both());
        assert_eq!(
            chosen,
            Encoding::Identity,
            "non-utf-8 header must be identity"
        );
        assert_eq!(chosen.content_encoding(), None, "identity has no coding");
        assert_eq!(chosen.body(&both()), IDENTITY, "identity body");
    }

    #[test]
    fn gzip_only_asset_ignores_brotli() {
        expect(Some("br"), &gz_only(), Encoding::Identity, IDENTITY);
        expect(Some("gzip, br"), &gz_only(), Encoding::Gzip, GZ);
    }

    #[test]
    fn brotli_only_asset_ignores_gzip() {
        expect(Some("gzip"), &br_only(), Encoding::Identity, IDENTITY);
        expect(Some("br"), &br_only(), Encoding::Brotli, BR);
    }

    #[test]
    fn asset_without_variants_is_identity() {
        expect(Some("gzip, br"), &none(), Encoding::Identity, IDENTITY);
        expect(None, &none(), Encoding::Identity, IDENTITY);
    }
}
