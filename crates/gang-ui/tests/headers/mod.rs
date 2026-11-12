//! Header contract tests: security, caching, content type, length and HEAD parity.

use crate::support::{assert_secure, font, get, header, index, script, send};
use gang_ui::Asset;
use http::StatusCode;

const ROOT_PATH: &str = "/ui/";
const MISSING_PATH: &str = "/ui/missing.js";
const FALLBACK_PATH: &str = "/ui/actions";
const UNSAFE_METHODS: [&str; 4] = ["POST", "PUT", "DELETE", "PATCH"];
const ALLOW_VALUE: &str = "GET, HEAD";
const IMMUTABLE_VALUE: &str = "public, max-age=31536000, immutable";
const NO_STORE_VALUE: &str = "no-store";
const ACCEPT_BR: (&str, &str) = ("accept-encoding", "br, gzip");
const REQUEST_HEADERS: [&[(&str, &str)]; 2] = [&[], &[ACCEPT_BR]];

fn asset_url(asset: &Asset) -> String {
    format!("/ui/{}", asset.path)
}

fn fixtures() -> [&'static Asset; 3] {
    [index(), script(), font()]
}

#[test]
fn ok_response_has_every_security_header() {
    let response = get(ROOT_PATH, &[]);
    assert_eq!(response.status(), StatusCode::OK, "GET /ui/ status");
    assert_secure(&response);
}

#[test]
fn not_modified_response_has_every_security_header() {
    let first = get(ROOT_PATH, &[]);
    let etag = header(&first, "etag").expect("index response carries an etag");
    let response = get(ROOT_PATH, &[("if-none-match", etag)]);
    assert_eq!(
        response.status(),
        StatusCode::NOT_MODIFIED,
        "conditional GET status"
    );
    assert_secure(&response);
    assert!(response.body().is_empty(), "304 body must be empty");
    for name in ["content-length", "content-type", "content-encoding"] {
        assert_eq!(header(&response, name), None, "304 must not carry {name}");
    }
    assert_eq!(header(&response, "etag"), Some(etag), "304 etag");
    assert_eq!(
        header(&response, "cache-control"),
        Some(NO_STORE_VALUE),
        "304 cache-control"
    );
}

#[test]
fn not_found_response_has_every_security_header() {
    let response = get(MISSING_PATH, &[]);
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "missing asset status"
    );
    assert_secure(&response);
    assert!(response.body().is_empty(), "404 body must be empty");
    assert_eq!(
        header(&response, "cache-control"),
        Some(NO_STORE_VALUE),
        "404 cache-control"
    );
}

#[test]
fn method_not_allowed_response_has_every_security_header() {
    for method in UNSAFE_METHODS {
        for path in [ROOT_PATH, MISSING_PATH] {
            let response = send(method, path, &[]);
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {path} status; method check must win over the path"
            );
            assert_eq!(
                header(&response, "allow"),
                Some(ALLOW_VALUE),
                "{method} {path} allow"
            );
            assert_secure(&response);
            assert!(
                response.body().is_empty(),
                "{method} {path} 405 body must be empty"
            );
        }
    }
}

#[test]
fn csp_denies_by_default_and_forbids_framing() {
    let response = get(ROOT_PATH, &[]);
    let csp = header(&response, "content-security-policy").expect("csp header");
    let directives: Vec<&str> = csp.split(';').map(str::trim).collect();
    for required in [
        "default-src 'none'",
        "base-uri 'none'",
        "frame-ancestors 'none'",
    ] {
        assert!(
            directives.contains(&required),
            "csp lacks {required}: {csp}"
        );
    }
    for forbidden in ["'unsafe-inline'", "'unsafe-eval'", "*"] {
        assert!(
            !csp.contains(forbidden),
            "csp must not contain {forbidden}: {csp}"
        );
    }
}

#[test]
fn hashed_assets_are_immutable_and_the_index_is_not_stored() {
    let script_asset = script();
    let hashed = get(&asset_url(script_asset), &[]);
    assert_eq!(hashed.status(), StatusCode::OK, "hashed script status");
    assert_eq!(
        header(&hashed, "cache-control"),
        Some(IMMUTABLE_VALUE),
        "hashed script cache-control"
    );

    let index_response = get(&asset_url(index()), &[]);
    assert_eq!(index_response.status(), StatusCode::OK, "index status");
    assert_eq!(
        header(&index_response, "cache-control"),
        Some(NO_STORE_VALUE),
        "index cache-control"
    );

    let fallback = get(FALLBACK_PATH, &[]);
    assert_eq!(fallback.status(), StatusCode::OK, "SPA fallback status");
    assert_eq!(
        header(&fallback, "cache-control"),
        Some(NO_STORE_VALUE),
        "SPA fallback cache-control"
    );
}

#[test]
fn content_types_match_the_asset() {
    let cases: [(&Asset, &str); 3] = [
        (index(), "text/html; charset=utf-8"),
        (script(), "text/javascript; charset=utf-8"),
        (font(), "font/woff2"),
    ];
    for (asset, expected) in cases {
        assert_eq!(
            asset.content_type, expected,
            "asset content type of {}",
            asset.path
        );
        let response = get(&asset_url(asset), &[]);
        assert_eq!(
            header(&response, "content-type"),
            Some(expected),
            "response content-type of {}",
            asset.path
        );
    }
}

#[test]
fn content_length_matches_the_body() {
    for asset in fixtures() {
        let url = asset_url(asset);
        let response = get(&url, &[]);
        assert_eq!(
            response.body().as_ref(),
            asset.identity,
            "identity body of {}",
            asset.path
        );
        let expected = response.body().len().to_string();
        assert_eq!(
            header(&response, "content-length"),
            Some(expected.as_str()),
            "GET content-length of {}",
            asset.path
        );

        let head = send("HEAD", &url, &[]);
        assert_eq!(
            header(&head, "content-length"),
            Some(expected.as_str()),
            "HEAD content-length of {}",
            asset.path
        );
        assert!(head.body().is_empty(), "HEAD body of {}", asset.path);
    }
}

#[test]
fn head_has_the_headers_of_get_and_no_body() {
    for asset in fixtures() {
        let url = asset_url(asset);
        for headers in REQUEST_HEADERS {
            let response = get(&url, headers);
            let head = send("HEAD", &url, headers);
            assert_eq!(
                head.status(),
                response.status(),
                "HEAD status of {}",
                asset.path
            );
            assert_eq!(
                head.headers(),
                response.headers(),
                "HEAD headers of {} must equal GET headers",
                asset.path
            );
            assert!(head.body().is_empty(), "HEAD body of {}", asset.path);
        }
    }
}
