//! Content negotiation: encoding selection, Vary, `ETag` and Content-Length.

use crate::support::*;
use gang_ui::Asset;
use http::StatusCode;

const UI: &str = "/ui/";
const ACCEPT_ENCODING: &str = "accept-encoding";
const CONTENT_ENCODING: &str = "content-encoding";
const CONTENT_LENGTH: &str = "content-length";
const ETAG: &str = "etag";
const VARY: &str = "vary";

fn url_of(asset: &Asset) -> String {
    format!("{UI}{}", asset.path)
}

fn fetch_with_accept(path: &str, accept: Option<&str>) -> http::Response<bytes::Bytes> {
    let headers: Vec<(&str, &str)> = accept
        .map(|value| (ACCEPT_ENCODING, value))
        .into_iter()
        .collect();
    get(path, &headers)
}

#[test]
fn accept_encoding_matrix_selects_expected_body() {
    let asset = script();
    let br = asset.br.expect("script has a brotli variant");
    let gz = asset.gz.expect("script has a gzip variant");
    let identity = asset.identity;
    let cases: [(Option<&str>, Option<&str>, &[u8]); 16] = [
        (None, None, identity),
        (Some("gzip"), Some("gzip"), gz),
        (Some("br"), Some("br"), br),
        (Some("gzip, br"), Some("br"), br),
        (Some("br;q=0, gzip"), Some("gzip"), gz),
        (Some("gzip;q=0"), None, identity),
        (Some("*"), Some("br"), br),
        (Some("*;q=0"), None, identity),
        (Some("deflate"), None, identity),
        (Some("GZIP"), Some("gzip"), gz),
        (Some("br;q=0.5, gzip;q=1"), Some("br"), br),
        (Some("gzip;q=0, gzip;q=1"), None, identity),
        (Some("gzip;q=1, gzip;q=0"), None, identity),
        (Some("*;q=1, *;q=0"), None, identity),
        (Some("*;q=0, *;q=1"), None, identity),
        (Some("br;q=0.001"), Some("br"), br),
    ];
    for (accept, encoding, body) in cases {
        let response = fetch_with_accept(&url_of(asset), accept);
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "status for accept-encoding {accept:?}"
        );
        assert_eq!(
            header(&response, CONTENT_ENCODING),
            encoding,
            "content-encoding for accept-encoding {accept:?}"
        );
        assert_eq!(
            &response.body()[..],
            body,
            "body for accept-encoding {accept:?}"
        );
    }
}

#[test]
fn vary_names_accept_encoding_on_encoded_assets() {
    let script_response = get(&url_of(script()), &[]);
    assert_eq!(
        header(&script_response, VARY),
        Some("Accept-Encoding"),
        "vary on encoded script"
    );

    let index_response = get(&url_of(index()), &[]);
    assert_eq!(
        header(&index_response, VARY),
        Some("Accept-Encoding"),
        "vary on encoded index"
    );

    let font_response = get(&url_of(font()), &[]);
    assert_eq!(
        header(&font_response, VARY),
        None,
        "fonts have no variants and must not carry vary"
    );
}

#[test]
fn fonts_are_never_encoded() {
    let asset = font();
    let response = get(&url_of(asset), &[(ACCEPT_ENCODING, "br, gzip")]);
    assert_eq!(response.status(), StatusCode::OK, "status for font");
    assert_eq!(
        header(&response, CONTENT_ENCODING),
        None,
        "font must not be content-encoded"
    );
    assert_eq!(
        &response.body()[..],
        asset.identity,
        "font body must be the identity bytes"
    );
}

#[test]
fn etag_is_the_same_for_every_encoding() {
    let asset = script();
    for accept in [None, Some("gzip"), Some("br")] {
        let response = fetch_with_accept(&url_of(asset), accept);
        assert_eq!(
            header(&response, ETAG),
            Some(asset.etag),
            "etag for accept-encoding {accept:?}"
        );
    }
}

#[test]
fn encoded_content_length_is_the_encoded_size() {
    let asset = script();
    let br = asset.br.expect("script has a brotli variant");
    let gz = asset.gz.expect("script has a gzip variant");
    let path = url_of(asset);
    for (accept, body) in [(None, asset.identity), (Some("gzip"), gz), (Some("br"), br)] {
        let response = fetch_with_accept(&path, accept);
        let expected = body.len().to_string();
        assert_eq!(
            header(&response, CONTENT_LENGTH),
            Some(expected.as_str()),
            "content-length for accept-encoding {accept:?}"
        );
        assert_eq!(
            response.body().len(),
            body.len(),
            "body length for accept-encoding {accept:?}"
        );
    }

    let head = send("HEAD", &path, &[(ACCEPT_ENCODING, "br")]);
    let expected = br.len().to_string();
    assert_eq!(
        header(&head, CONTENT_LENGTH),
        Some(expected.as_str()),
        "HEAD content-length must be the selected encoded size"
    );
    assert!(head.body().is_empty(), "HEAD body must be empty");
}
