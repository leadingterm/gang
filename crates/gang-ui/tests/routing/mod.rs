//! Request routing behaviour of `respond` for the `/ui` mount point.

use crate::support::*;
use http::StatusCode;

/// Requests that must resolve to the SPA index with a 200 and the index bytes.
const INDEX_ALIAS_PATHS: &[&str] = &["/ui", "/ui/"];

/// Extensionless paths that fall back to the SPA index.
const FALLBACK_PATHS: &[&str] = &[
    "/ui/actions",
    "/ui/workers/some-id",
    "/ui/actions/",
    "/ui/session-less",
];

/// Paths with an extension that name no embedded asset.
const MISSING_PATHS: &[&str] = &[
    "/ui/missing.js",
    "/ui/assets/missing.css",
    "/ui/favicon.ico",
    "/ui/a/b.js",
];

/// Path forms that must never reach the table, with or without an extension.
const TRAVERSAL_PATHS: &[&str] = &[
    "/ui/../etc/passwd",
    "/ui/assets/../index.html",
    "/ui/%2e%2e/x",
    "/ui/%2E%2E/x",
    "/ui/a%5cb",
    "/ui/a%5Cb",
    "/ui/a\\b",
    "/ui/..",
    "/ui/actions/..",
    "/ui/..%2f",
];

/// Paths outside the `/ui` mount.
const OUTSIDE_PATHS: &[&str] = &["/", "/uix", "/v1/state", "/ui-x/y", ""];

#[test]
fn ui_and_ui_slash_serve_the_index() {
    for path in INDEX_ALIAS_PATHS {
        let response = get(path, &[]);
        assert_eq!(response.status(), StatusCode::OK, "status for {path:?}");
        assert_eq!(
            &response.body()[..],
            index().identity,
            "body for {path:?} must be the index bytes"
        );
    }
}

#[test]
fn extensionless_paths_fall_back_to_the_index() {
    for path in FALLBACK_PATHS {
        let response = get(path, &[]);
        assert_eq!(response.status(), StatusCode::OK, "status for {path:?}");
        assert_eq!(
            &response.body()[..],
            index().identity,
            "body for {path:?} must be the index bytes"
        );
        assert_eq!(
            header(&response, "cache-control"),
            Some("no-store"),
            "fallback for {path:?} must not be cached"
        );
    }
}

#[test]
fn a_hashed_asset_is_served_by_path() {
    let asset = script();
    let path = format!("/ui/{}", asset.path);
    let response = get(&path, &[]);
    assert_eq!(response.status(), StatusCode::OK, "status for {path:?}");
    assert_eq!(
        &response.body()[..],
        asset.identity,
        "identity body for {path:?}"
    );
}

#[test]
fn index_html_is_served_by_name() {
    let response = get("/ui/index.html", &[]);
    assert_eq!(response.status(), StatusCode::OK, "status for index.html");
    assert_eq!(
        &response.body()[..],
        index().identity,
        "body for /ui/index.html"
    );
    assert_eq!(
        header(&response, "content-type"),
        Some("text/html; charset=utf-8"),
        "content type for index.html"
    );
}

#[test]
fn missing_files_with_an_extension_are_not_found() {
    for path in MISSING_PATHS {
        let response = get(path, &[]);
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "status for {path:?}"
        );
        assert!(
            response.body().is_empty(),
            "404 body for {path:?} must be empty"
        );
    }
}

#[test]
fn traversal_is_not_found() {
    for path in TRAVERSAL_PATHS {
        let response = get(path, &[]);
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "status for {path:?}"
        );
        assert!(
            response.body().is_empty(),
            "404 body for {path:?} must be empty"
        );
    }
}

#[test]
fn paths_outside_ui_are_not_found() {
    for path in OUTSIDE_PATHS {
        let response = get(path, &[]);
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "status for {path:?}"
        );
    }
}

#[test]
fn a_double_slash_prefix_is_not_the_index_alias() {
    let response = get("/ui//assets/x.js", &[]);
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "/ui//assets/x.js must not resolve"
    );
}
