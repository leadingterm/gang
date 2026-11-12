//! Response parts several services build the same way.

use gang_spi::{ApiResponseBody, full_body};
use http::header::{CACHE_CONTROL, HeaderMap, HeaderValue};
use http::{Response, StatusCode};

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
