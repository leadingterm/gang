//! In-memory, path-style S3 endpoint for multi-controller cache scenarios.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use gang_types::Url;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;

use crate::errors::E2eError;

const LISTEN_ADDRESS: &str = "127.0.0.1:0";
const LAST_MODIFIED: &str = "Thu, 01 Jan 1970 00:00:00 GMT";
const XML: &str = "application/xml";
const REQUEST_LOG_LIMIT: usize = 100;
const DECODED_LENGTH: &str = "x-amz-decoded-content-length";
const AWS_CHUNKED: &str = "aws-chunked";

#[derive(Default)]
struct Store {
    objects: BTreeMap<String, Bytes>,
    uploads: BTreeMap<String, (String, BTreeMap<u32, Bytes>)>,
    requests: VecDeque<String>,
    next_upload: u64,
}

/// A loopback S3-compatible object store. Dropping it stops its server.
pub struct FakeS3 {
    endpoint: Url,
    store: Arc<Mutex<Store>>,
    server: JoinHandle<()>,
}

impl FakeS3 {
    /// Starts a path-style S3 endpoint on an ephemeral loopback port.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener cannot be bound or its address cannot be read.
    pub async fn start() -> Result<Self, E2eError> {
        let listener = tokio::net::TcpListener::bind(LISTEN_ADDRESS)
            .await
            .map_err(E2eError::io("binding fake S3"))?;
        let address = listener
            .local_addr()
            .map_err(E2eError::io("reading fake S3 address"))?;
        let endpoint = Url::parse(&format!("http://{address}"))
            .map_err(E2eError::system_from("forming fake S3 endpoint"))?;
        let store = Arc::new(Mutex::new(Store::default()));
        let router = Router::new().fallback(serve).with_state(Arc::clone(&store));
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(Self {
            endpoint,
            store,
            server,
        })
    }

    /// Returns the HTTP endpoint for path-style S3 clients.
    #[must_use]
    pub fn endpoint(&self) -> Url {
        self.endpoint.clone()
    }

    /// Returns the number of completed objects across all buckets.
    #[must_use]
    pub fn objects(&self) -> usize {
        self.store.lock().objects.len()
    }

    /// Returns the retained request log, oldest first. Byte counts are encoded request sizes.
    #[must_use]
    pub fn requests(&self) -> Vec<String> {
        self.store.lock().requests.iter().cloned().collect()
    }

    /// Returns stored path-style keys and their decoded object sizes, in key order.
    #[must_use]
    pub fn keys(&self) -> Vec<(String, usize)> {
        self.store
            .lock()
            .objects
            .iter()
            .map(|(key, data)| (key.clone(), data.len()))
            .collect()
    }
}

impl Drop for FakeS3 {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve(State(store): State<Arc<Mutex<Store>>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, usize::MAX).await;
    let size = body.as_ref().map_or(0, Bytes::len);
    let mut store = store.lock();
    let result = match body {
        Ok(body) => {
            let decoded = if parts.method == Method::PUT {
                decode_body(&parts.headers, body)
            } else {
                Some(body)
            };
            decoded.map_or_else(
                || response(StatusCode::BAD_REQUEST, Bytes::new(), None),
                |body| handle(&mut store, &parts, body),
            )
        }
        Err(_) => response(StatusCode::BAD_REQUEST, Bytes::new(), None),
    };
    if store.requests.len() == REQUEST_LOG_LIMIT {
        store.requests.pop_front();
    }
    store.requests.push_back(format!(
        "{} {} -> {} {} bytes",
        parts.method,
        parts.uri,
        result.status().as_u16(),
        size
    ));
    result
}

fn handle(store: &mut Store, parts: &http::request::Parts, body: Bytes) -> Response {
    let key = parts.uri.path().to_owned();
    let query = parts.uri.query().unwrap_or("");
    let parameter = |name: &str| {
        query
            .split('&')
            .filter_map(|part| part.split_once('='))
            .find_map(|(key, value)| (key == name).then_some(value))
    };
    let upload = parameter("uploadId");
    match (&parts.method, upload, parameter("partNumber")) {
        (&Method::POST, None, _) if query.split('&').any(|p| p == "uploads" || p == "uploads=") => {
            let Some(next_upload) = store.next_upload.checked_add(1) else {
                return response(StatusCode::INTERNAL_SERVER_ERROR, Bytes::new(), None);
            };
            store.next_upload = next_upload;
            let id = next_upload.to_string();
            store.uploads.insert(id.clone(), (key, BTreeMap::new()));
            xml(
                StatusCode::OK,
                format!(
                    "<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
                ),
            )
        }
        (&Method::PUT, Some(id), Some(number)) => {
            let Some((upload_key, uploaded)) = store.uploads.get_mut(id) else {
                return missing("NoSuchUpload");
            };
            let Ok(number) = number.parse::<u32>() else {
                return response(StatusCode::BAD_REQUEST, Bytes::new(), None);
            };
            if *upload_key != key || number == 0 {
                return missing("NoSuchUpload");
            }
            let tag = etag(&body);
            uploaded.insert(number, body);
            response(StatusCode::OK, Bytes::new(), Some(&tag))
        }
        (&Method::POST, Some(id), _) => {
            if store.uploads.get(id).is_none_or(|(path, _)| *path != key) {
                return missing("NoSuchUpload");
            }
            let Some((_, parts)) = store.uploads.remove(id) else {
                return missing("NoSuchUpload");
            };
            let mut data = Vec::new();
            for part in parts.into_values() {
                data.extend_from_slice(&part);
            }
            let tag = etag(&data);
            store.objects.insert(key, Bytes::from(data));
            xml(
                StatusCode::OK,
                format!(
                    "<CompleteMultipartUploadResult><ETag>{tag}</ETag></CompleteMultipartUploadResult>"
                ),
            )
        }
        (&Method::DELETE, Some(id), _) => {
            if store.uploads.get(id).is_none_or(|(path, _)| *path != key) {
                return missing("NoSuchUpload");
            }
            store.uploads.remove(id);
            response(StatusCode::NO_CONTENT, Bytes::new(), None)
        }
        (&Method::PUT, None, _) => {
            let tag = etag(&body);
            store.objects.insert(key, body);
            response(StatusCode::OK, Bytes::new(), Some(&tag))
        }
        (&Method::DELETE, None, _) => {
            store.objects.remove(&key);
            response(StatusCode::NO_CONTENT, Bytes::new(), None)
        }
        (&Method::GET | &Method::HEAD, None, _) => {
            get_object(store, &key, &parts.method, &parts.headers)
        }
        _ => response(StatusCode::METHOD_NOT_ALLOWED, Bytes::new(), None),
    }
}

fn decode_body(headers: &http::HeaderMap, body: Bytes) -> Option<Bytes> {
    let chunked = headers
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .any(|value| {
            value.to_str().ok().is_some_and(|value| {
                value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case(AWS_CHUNKED))
            })
        });
    if !chunked {
        return Some(body);
    }
    let expected = headers
        .get(DECODED_LENGTH)?
        .to_str()
        .ok()?
        .parse::<usize>()
        .ok()?;
    let mut input = body.as_ref();
    let mut decoded = Vec::new();
    loop {
        let line = take_line(&mut input)?;
        let size = std::str::from_utf8(line.split(|byte| *byte == b';').next()?).ok()?;
        let size = usize::from_str_radix(size, 16).ok()?;
        if size == 0 {
            loop {
                let trailer = take_line(&mut input)?;
                if trailer.is_empty() {
                    return (input.is_empty() && decoded.len() == expected)
                        .then(|| Bytes::from(decoded));
                }
                if !trailer.contains(&b':') {
                    return None;
                }
            }
        }
        let (chunk, rest) = input.split_at_checked(size)?;
        decoded.extend_from_slice(chunk);
        input = rest.strip_prefix(b"\r\n")?;
        if decoded.len() > expected {
            return None;
        }
    }
}

fn take_line<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let end = input.windows(2).position(|pair| pair == b"\r\n")?;
    let (line, rest) = input.split_at(end);
    *input = rest.get(2..)?;
    Some(line)
}

fn get_object(store: &Store, key: &str, method: &Method, headers: &http::HeaderMap) -> Response {
    let Some(data) = store.objects.get(key) else {
        return missing("NoSuchKey");
    };
    let tag = etag(data);
    let mut status = StatusCode::OK;
    let mut selected = data.clone();
    let mut content_range = None;
    if let Some(range) = headers.get(header::RANGE) {
        let parsed = range
            .to_str()
            .ok()
            .and_then(|s| s.strip_prefix("bytes="))
            .and_then(|s| s.split_once('-'))
            .and_then(|(a, b)| {
                if data.is_empty() {
                    return None;
                }
                if a.is_empty() {
                    let suffix = b.parse::<usize>().ok()?;
                    return (suffix > 0)
                        .then_some((data.len().saturating_sub(suffix), data.len() - 1));
                }
                let start = a.parse::<usize>().ok()?;
                let end = if b.is_empty() {
                    data.len() - 1
                } else {
                    b.parse::<usize>().ok()?.min(data.len() - 1)
                };
                (start < data.len() && start <= end).then_some((start, end))
            });
        let Some((start, end)) = parsed else {
            return response(StatusCode::RANGE_NOT_SATISFIABLE, Bytes::new(), None);
        };
        selected = data.slice(start..=end);
        content_range = Some(format!("bytes {start}-{end}/{}", data.len()));
        status = StatusCode::PARTIAL_CONTENT;
    }
    let length = selected.len();
    if *method == Method::HEAD {
        selected = Bytes::new();
    }
    let mut result = response(status, selected, Some(&tag));
    if let Ok(value) = length.to_string().parse() {
        result.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    if let Some(range) = content_range.and_then(|s| s.parse().ok()) {
        result.headers_mut().insert(header::CONTENT_RANGE, range);
    }
    result
}

fn etag(data: &[u8]) -> String {
    format!("\"{}\"", hex::encode(Sha256::digest(data)))
}

fn response(status: StatusCode, body: Bytes, tag: Option<&str>) -> Response {
    let mut result = Response::new(Body::from(body));
    *result.status_mut() = status;
    result.headers_mut().insert(
        header::LAST_MODIFIED,
        http::HeaderValue::from_static(LAST_MODIFIED),
    );
    if let Some(value) = tag.and_then(|tag| tag.parse().ok()) {
        result.headers_mut().insert(header::ETAG, value);
    }
    result
}

fn xml(status: StatusCode, body: String) -> Response {
    let mut result = response(status, Bytes::from(body), None);
    result
        .headers_mut()
        .insert(header::CONTENT_TYPE, http::HeaderValue::from_static(XML));
    result
}

fn missing(code: &str) -> Response {
    xml(
        StatusCode::NOT_FOUND,
        format!("<Error><Code>{code}</Code><Message>not found</Message></Error>"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_clock::SystemClock;
    use gang_testkit::TestResult;
    use gang_tls::{
        HttpClient, HttpProtocol, HttpReply, HttpRequest, HttpSettings, HttpTransport, RequestBody,
    };
    use gang_types::{NonZeroBytes, NonZeroDuration};
    use http::{HeaderName, HeaderValue};

    fn client() -> Result<HttpClient, gang_tls::HttpError> {
        HttpClient::try_new(&HttpSettings {
            transport: HttpTransport::Plain,
            protocol: HttpProtocol::Negotiated,
            timeout: NonZeroDuration::from_secs(5),
            max_body: NonZeroBytes::mib(1),
            clock: Arc::new(SystemClock::new()),
        })
    }

    async fn request(
        client: &HttpClient,
        fake: &FakeS3,
        method: Method,
        path: &str,
        data: &'static [u8],
        headers: &[(HeaderName, HeaderValue)],
    ) -> TestResult<HttpReply> {
        let url = fake.endpoint().join(path)?;
        Ok(client
            .send(HttpRequest {
                method,
                url: &url,
                bearer: None,
                headers,
                body: Some(RequestBody {
                    content_type: "application/octet-stream",
                    bytes: Bytes::from_static(data),
                }),
            })
            .await?)
    }

    #[tokio::test]
    async fn objects_ranges_head_and_delete() -> TestResult {
        let fake = FakeS3::start().await?;
        let client = client()?;
        let missing = request(&client, &fake, Method::GET, "/bucket/key", b"", &[]).await?;
        assert_eq!(missing.status, StatusCode::NOT_FOUND);
        assert!(std::str::from_utf8(&missing.body)?.contains("<Code>NoSuchKey</Code>"));
        let missing_head = request(&client, &fake, Method::HEAD, "/bucket/key", b"", &[]).await?;
        assert_eq!(missing_head.status, StatusCode::NOT_FOUND);
        assert!(missing_head.body.is_empty());
        let put = request(&client, &fake, Method::PUT, "/bucket/key", b"abcdef", &[]).await?;
        assert_eq!(put.status, StatusCode::OK);
        assert!(put.headers.contains_key(header::ETAG));
        assert_eq!(fake.objects(), 1);
        let get = request(&client, &fake, Method::GET, "/bucket/key", b"", &[]).await?;
        assert_eq!(get.body, "abcdef");
        let head = request(&client, &fake, Method::HEAD, "/bucket/key", b"", &[]).await?;
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.headers[header::CONTENT_LENGTH], "6");
        assert!(head.body.is_empty());
        let range = [(header::RANGE, HeaderValue::from_static("bytes=1-3"))];
        let partial = request(&client, &fake, Method::GET, "/bucket/key", b"", &range).await?;
        assert_eq!(partial.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(partial.headers[header::CONTENT_RANGE], "bytes 1-3/6");
        assert_eq!(partial.body, "bcd");
        let suffix = [(header::RANGE, HeaderValue::from_static("bytes=-2"))];
        assert_eq!(
            request(&client, &fake, Method::GET, "/bucket/key", b"", &suffix)
                .await?
                .body,
            "ef"
        );
        assert_eq!(
            request(&client, &fake, Method::DELETE, "/bucket/key", b"", &[])
                .await?
                .status,
            StatusCode::NO_CONTENT
        );
        assert_eq!(fake.objects(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn multipart_assemble_and_abort() -> TestResult {
        let fake = FakeS3::start().await?;
        let client = client()?;
        let create = request(
            &client,
            &fake,
            Method::POST,
            "/bucket/key?uploads",
            b"",
            &[],
        )
        .await?;
        assert_eq!(create.status, StatusCode::OK);
        assert!(std::str::from_utf8(&create.body)?.contains("<UploadId>1</UploadId>"));
        for (path, data) in [
            ("/bucket/key?partNumber=2&uploadId=1", &b"bar"[..]),
            ("/bucket/key?partNumber=1&uploadId=1", &b"foo"[..]),
        ] {
            let part = request(&client, &fake, Method::PUT, path, data, &[]).await?;
            assert_eq!(part.status, StatusCode::OK);
            assert!(part.headers.contains_key(header::ETAG));
        }
        assert_eq!(
            request(
                &client,
                &fake,
                Method::POST,
                "/bucket/key?uploadId=1",
                b"<CompleteMultipartUpload/>",
                &[]
            )
            .await?
            .status,
            StatusCode::OK
        );
        assert_eq!(fake.objects(), 1);
        assert_eq!(
            request(&client, &fake, Method::GET, "/bucket/key", b"", &[])
                .await?
                .body,
            "foobar"
        );
        assert_eq!(
            request(
                &client,
                &fake,
                Method::POST,
                "/bucket/other?uploads",
                b"",
                &[]
            )
            .await?
            .status,
            StatusCode::OK
        );
        assert_eq!(
            request(
                &client,
                &fake,
                Method::DELETE,
                "/bucket/other?uploadId=2",
                b"",
                &[]
            )
            .await?
            .status,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            request(
                &client,
                &fake,
                Method::POST,
                "/bucket/other?uploadId=2",
                b"",
                &[]
            )
            .await?
            .status,
            StatusCode::NOT_FOUND
        );
        Ok(())
    }

    #[tokio::test]
    async fn chunked_put_returns_decoded_ranges_and_metadata() -> TestResult {
        let fake = FakeS3::start().await?;
        let client = client()?;
        let headers = [
            (
                header::CONTENT_ENCODING,
                HeaderValue::from_static("aws-chunked"),
            ),
            (
                HeaderName::from_static(DECODED_LENGTH),
                HeaderValue::from_static("6"),
            ),
        ];
        let put = request(
            &client, &fake, Method::PUT, "/bucket/chunked",
            b"3;chunk-signature=abc\r\nabc\r\n3;chunk-signature=def\r\ndef\r\n0;chunk-signature=xyz\r\n\r\n",
            &headers,
        ).await?;
        assert_eq!(put.status, StatusCode::OK);
        let get = request(&client, &fake, Method::GET, "/bucket/chunked", b"", &[]).await?;
        assert_eq!(get.body, "abcdef");
        assert_eq!(get.headers[header::CONTENT_LENGTH], "6");
        assert_eq!(get.headers[header::ETAG], put.headers[header::ETAG]);
        let head = request(&client, &fake, Method::HEAD, "/bucket/chunked", b"", &[]).await?;
        assert_eq!(head.headers[header::CONTENT_LENGTH], "6");
        assert_eq!(head.headers[header::ETAG], put.headers[header::ETAG]);
        let range = [(header::RANGE, HeaderValue::from_static("bytes=1-3"))];
        let partial = request(&client, &fake, Method::GET, "/bucket/chunked", b"", &range).await?;
        assert_eq!(partial.body, "bcd");
        assert_eq!(fake.keys(), vec![("/bucket/chunked".to_owned(), 6)]);
        assert!(
            fake.requests()
                .iter()
                .any(|line| line.starts_with("PUT /bucket/chunked -> 200 "))
        );
        Ok(())
    }

    #[tokio::test]
    async fn chunked_multipart_trailer_and_bad_length() -> TestResult {
        let fake = FakeS3::start().await?;
        let client = client()?;
        request(
            &client,
            &fake,
            Method::POST,
            "/bucket/key?uploads",
            b"",
            &[],
        )
        .await?;
        let headers = [
            (
                header::CONTENT_ENCODING,
                HeaderValue::from_static("aws-chunked"),
            ),
            (
                HeaderName::from_static(DECODED_LENGTH),
                HeaderValue::from_static("6"),
            ),
        ];
        let part = request(
            &client,
            &fake,
            Method::PUT,
            "/bucket/key?partNumber=1&uploadId=1",
            b"3\r\nfoo\r\n3\r\nbar\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n",
            &headers,
        )
        .await?;
        assert_eq!(part.status, StatusCode::OK);
        assert!(part.headers.contains_key(header::ETAG));
        let bad = [
            (
                header::CONTENT_ENCODING,
                HeaderValue::from_static("aws-chunked"),
            ),
            (
                HeaderName::from_static(DECODED_LENGTH),
                HeaderValue::from_static("7"),
            ),
        ];
        assert_eq!(
            request(
                &client,
                &fake,
                Method::PUT,
                "/bucket/bad",
                b"3\r\nfoo\r\n0\r\n\r\n",
                &bad
            )
            .await?
            .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            request(
                &client,
                &fake,
                Method::POST,
                "/bucket/key?uploadId=1",
                b"<CompleteMultipartUpload/>",
                &[]
            )
            .await?
            .status,
            StatusCode::OK
        );
        assert_eq!(
            request(&client, &fake, Method::GET, "/bucket/key", b"", &[])
                .await?
                .body,
            "foobar"
        );
        assert_eq!(fake.keys(), vec![("/bucket/key".to_owned(), 6)]);
        Ok(())
    }

    #[tokio::test]
    async fn request_log_retains_only_latest_entries() -> TestResult {
        let fake = FakeS3::start().await?;
        let client = client()?;
        for _ in 0..=REQUEST_LOG_LIMIT {
            request(&client, &fake, Method::GET, "/bucket/missing", b"", &[]).await?;
        }
        assert_eq!(fake.requests().len(), REQUEST_LOG_LIMIT);
        assert!(
            fake.requests()
                .iter()
                .all(|line| line == "GET /bucket/missing -> 404 0 bytes")
        );
        Ok(())
    }
}
