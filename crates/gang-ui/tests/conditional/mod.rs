//! Conditional requests: If-None-Match handling and the 304 response shape.

use crate::support::*;
use http::StatusCode;

const UI: &str = "/ui/";
const ACCEPT_ENCODING: &str = "accept-encoding";
const CONTENT_ENCODING: &str = "content-encoding";
const CONTENT_LENGTH: &str = "content-length";
const CONTENT_TYPE: &str = "content-type";
const ETAG: &str = "etag";
const IF_NONE_MATCH: &str = "if-none-match";
const VARY: &str = "vary";
const METHODS: [&str; 2] = ["GET", "HEAD"];
const OTHER_ETAG: &str = "\"0000000000000000000000000000000000000000000000000000000000000000\"";

#[test]
fn matching_etag_gives_304() {
    let asset = script();
    let path = format!("{UI}{}", asset.path);
    let forms = [
        asset.etag.to_owned(),
        format!("W/{}", asset.etag),
        format!("{OTHER_ETAG}, {}", asset.etag),
        "*".to_owned(),
    ];
    for method in METHODS {
        for form in &forms {
            let response = send(method, &path, &[(IF_NONE_MATCH, form.as_str())]);
            assert_eq!(
                response.status(),
                StatusCode::NOT_MODIFIED,
                "{method} with if-none-match {form}"
            );
            assert!(
                response.body().is_empty(),
                "{method} 304 body must be empty for if-none-match {form}"
            );
            assert_eq!(
                header(&response, ETAG),
                Some(asset.etag),
                "{method} 304 etag for if-none-match {form}"
            );
            assert_secure(&response);
        }
    }
}

#[test]
fn non_matching_etag_gives_200_with_body() {
    let asset = script();
    let path = format!("{UI}{}", asset.path);
    let length = asset.identity.len().to_string();
    for method in METHODS {
        let response = send(method, &path, &[(IF_NONE_MATCH, OTHER_ETAG)]);
        assert_eq!(response.status(), StatusCode::OK, "{method} status");
        assert_eq!(
            header(&response, ETAG),
            Some(asset.etag),
            "{method} etag on 200"
        );
        assert_eq!(
            header(&response, CONTENT_LENGTH),
            Some(length.as_str()),
            "{method} content-length on 200"
        );
        if method == "GET" {
            assert_eq!(
                &response.body()[..],
                asset.identity,
                "GET body must be the identity bytes"
            );
        } else {
            assert!(response.body().is_empty(), "HEAD body must be empty");
        }
        assert_secure(&response);
    }
}

#[test]
fn etag_applies_to_the_fallback() {
    let index_asset = index();
    let response = send("GET", "/ui/actions", &[(IF_NONE_MATCH, index_asset.etag)]);
    assert_eq!(
        response.status(),
        StatusCode::NOT_MODIFIED,
        "fallback with matching index etag"
    );
    assert_eq!(
        header(&response, ETAG),
        Some(index_asset.etag),
        "fallback 304 etag"
    );
    assert!(
        response.body().is_empty(),
        "fallback 304 body must be empty"
    );
    assert_secure(&response);

    let plain = get("/ui/actions", &[]);
    assert_eq!(
        plain.status(),
        StatusCode::OK,
        "fallback without conditional header"
    );
    assert_eq!(
        &plain.body()[..],
        index_asset.identity,
        "fallback body must be the index identity bytes"
    );
}

#[test]
fn not_modified_keeps_vary_for_encoded_assets_and_carries_no_encoding() {
    let asset = script();
    let path = format!("{UI}{}", asset.path);
    let response = send(
        "GET",
        &path,
        &[(ACCEPT_ENCODING, "br"), (IF_NONE_MATCH, asset.etag)],
    );
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED, "status");
    assert_eq!(
        header(&response, VARY),
        Some("Accept-Encoding"),
        "304 must keep vary for encoded assets"
    );
    assert_eq!(
        header(&response, CONTENT_ENCODING),
        None,
        "304 must not carry content-encoding"
    );
    assert_eq!(
        header(&response, CONTENT_LENGTH),
        None,
        "304 must not carry content-length"
    );
    assert_eq!(
        header(&response, CONTENT_TYPE),
        None,
        "304 must not carry content-type"
    );
    assert_eq!(header(&response, ETAG), Some(asset.etag), "304 etag");
    assert!(response.body().is_empty(), "304 body must be empty");
    assert_secure(&response);
}
