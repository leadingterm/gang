//! Request dispatch: maps a method, request path and request headers to a response.

use bytes::Bytes;
use http::header::{
    ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ETAG,
    IF_NONE_MATCH, VARY,
};
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode, header};

use crate::asset::Asset;
use crate::conditional::not_modified;
use crate::headers::{ALLOW as ALLOW_METHODS, IMMUTABLE, NO_STORE, secure};
use crate::negotiate::Encoding;
use crate::route::{Route, resolve};

/// Upper bound on response headers: six security headers plus validators, cache, vary, type, length and encoding.
const RESPONSE_HEADER_CAPACITY: usize = 12;

/// Value of `Vary` on responses whose asset has encoded variants.
const VARY_ACCEPT_ENCODING: HeaderValue = HeaderValue::from_static("Accept-Encoding");

/// Serves the dashboard under `/ui`.
///
/// Contract:
/// - Only `GET` and `HEAD` are served; any other method yields `405` with `Allow: GET, HEAD`,
///   checked before the path is looked at.
/// - `path` is the full request path (no query string). Paths outside `/ui` and paths that
///   escape the bundle yield `404`. Extensionless paths under `/ui` fall back to `index.html`.
/// - A found asset yields `200` with its content type, `ETag` and length. Brotli or gzip
///   variants are chosen from `Accept-Encoding` when the asset has them. A matching
///   `If-None-Match` yields `304`.
/// - `HEAD` returns the same headers as `GET` with an empty body.
/// - Every response carries the security headers.
#[must_use]
pub fn respond(method: &Method, path: &str, headers: &HeaderMap) -> Response<Bytes> {
    if *method != Method::GET && *method != Method::HEAD {
        return method_not_allowed();
    }
    match resolve(path) {
        Route::Asset(asset) => serve(asset, method, headers),
        Route::NotFound => not_found(),
    }
}

/// Builds an empty-bodied response with the given status and headers, plus the security headers.
fn reply(status: StatusCode, mut headers: HeaderMap) -> Response<Bytes> {
    secure(&mut headers);
    let mut response = Response::new(Bytes::new());
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

/// Builds the `404` response.
fn not_found() -> Response<Bytes> {
    let mut headers = HeaderMap::with_capacity(RESPONSE_HEADER_CAPACITY);
    headers.insert(CACHE_CONTROL, NO_STORE);
    reply(StatusCode::NOT_FOUND, headers)
}

/// Builds the `405` response.
fn method_not_allowed() -> Response<Bytes> {
    let mut headers = HeaderMap::with_capacity(RESPONSE_HEADER_CAPACITY);
    headers.insert(header::ALLOW, ALLOW_METHODS);
    headers.insert(CACHE_CONTROL, NO_STORE);
    reply(StatusCode::METHOD_NOT_ALLOWED, headers)
}

/// Builds the `200` or `304` response for a found asset.
fn serve(asset: &'static Asset, method: &Method, request: &HeaderMap) -> Response<Bytes> {
    let encoding = Encoding::choose(request.get(ACCEPT_ENCODING), asset);
    let mut headers = HeaderMap::with_capacity(RESPONSE_HEADER_CAPACITY);
    headers.insert(ETAG, HeaderValue::from_static(asset.etag));
    headers.insert(
        CACHE_CONTROL,
        if asset.immutable { IMMUTABLE } else { NO_STORE },
    );
    if asset.has_variants() {
        headers.insert(VARY, VARY_ACCEPT_ENCODING);
    }
    if not_modified(request.get(IF_NONE_MATCH), asset.etag) {
        return reply(StatusCode::NOT_MODIFIED, headers);
    }

    let body = encoding.body(asset);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(asset.content_type));
    headers.insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
    if let Some(value) = encoding.content_encoding() {
        headers.insert(CONTENT_ENCODING, value);
    }

    let mut response = reply(StatusCode::OK, headers);
    if *method == Method::GET {
        *response.body_mut() = Bytes::from_static(body);
    }
    response
}
