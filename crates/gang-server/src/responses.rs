//! Response parts several services build the same way.

use bytes::Bytes;
use gang_spi::{ApiResponseBody, full_body};
use gang_wire::INTERNAL_ERROR_BODY;
use http::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderMap, HeaderValue};
use http::{Response, StatusCode};
use serde::Serialize;

/// Marks the response as one that no cache may store.
pub(crate) fn no_store(headers: &mut HeaderMap) {
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
}

/// An empty response with `status` that no cache may store.
pub(crate) fn empty(status: StatusCode) -> Response<ApiResponseBody> {
    let mut response = Response::new(full_body(Vec::new()));
    *response.status_mut() = status;
    no_store(response.headers_mut());
    response
}

/// Renders `body` as JSON with `status`. A body that cannot be serialized becomes a constant
/// internal error.
pub(crate) fn json<T: Serialize>(status: StatusCode, body: &T) -> Response<ApiResponseBody> {
    match serde_json::to_vec(body) {
        Ok(bytes) => json_response(status, bytes),
        Err(error) => {
            tracing::error!(%error, "cannot serialize api response");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                Bytes::from_static(INTERNAL_ERROR_BODY.as_bytes()),
            )
        }
    }
}

/// Renders `body` as a JSON response with `status` that the caller must not store.
pub(crate) fn json_response(
    status: StatusCode,
    body: impl Into<Bytes>,
) -> Response<ApiResponseBody> {
    let mut response = Response::new(full_body(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    no_store(response.headers_mut());
    response
}
