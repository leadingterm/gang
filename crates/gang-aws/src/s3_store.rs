//! S3-backed [`ContentStore`] and [`ActionStore`] implementations via `aws-sdk-s3` and
//! `gang-aws-sdk`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::DateTime;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream, FuturesUnordered};
use futures::{StreamExt as _, TryStreamExt as _};
use gang_aws_sdk::call_error;
use gang_clock::{Clock, Timestamp};
use gang_spi::{
    ActionStore, BlobStream, ByteRange, ContentStore, EngineStore, PortError, Rejection,
};
use gang_types::{Digest, EnvironmentName};
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use tokio::sync::OnceCell;
use tokio::task::JoinSet;

use crate::config::{ExpiryAge, S3CacheConfig};
use crate::errors::AwsError;
use crate::s3_existence::S3ExistenceCache;

/// Key prefix of the content-addressed store, appended to the configured `key_prefix`.
const CONTENT_PREFIX: &str = "cas/";

/// Key prefix of the action cache, appended to the configured `key_prefix`.
const ACTION_PREFIX: &str = "ac/";

/// Maximum number of parts in one S3 multipart upload.
const MAX_MULTIPART_PARTS: u64 = 10_000;

/// Maximum size of one S3 multipart part (5 GiB).
const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

const HTTP_NOT_FOUND: u16 = 404;

/// Shared state for [`S3ContentStore`] and [`S3ActionStore`].
pub(crate) struct S3Backend {
    config: S3CacheConfig,
    clock: Arc<dyn Clock>,
    client: OnceCell<aws_sdk_s3::Client>,
    existence: Arc<S3ExistenceCache>,
    multipart_aborts: Mutex<JoinSet<()>>,
}

impl std::fmt::Debug for S3Backend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Backend")
            .field("region", &self.config.region)
            .field("bucket", &self.config.bucket)
            .field("key_prefix", &self.config.key_prefix)
            .finish_non_exhaustive()
    }
}

impl S3Backend {
    /// Builds a lazy S3 backend that opens its `aws-sdk-s3` client on first use.
    pub(crate) fn new(
        config: S3CacheConfig,
        clock: Arc<dyn Clock>,
        client: OnceCell<aws_sdk_s3::Client>,
    ) -> Self {
        let existence = Arc::new(S3ExistenceCache::new(&config.tuning));
        Self {
            config,
            clock,
            client,
            existence,
            multipart_aborts: Mutex::new(JoinSet::new()),
        }
    }

    async fn client(&self) -> &aws_sdk_s3::Client {
        self.client
            .get_or_init(|| async {
                let sdk = gang_aws_sdk::load(&self.config.region, &self.config.client).await;
                let mut builder = aws_sdk_s3::config::Builder::from(&sdk)
                    .force_path_style(self.config.force_path_style);
                if let Some(endpoint) = &self.config.endpoint {
                    builder = builder.endpoint_url(endpoint.as_str());
                }
                aws_sdk_s3::Client::from_conf(builder.build())
            })
            .await
    }

    fn bucket(&self) -> &str {
        self.config.bucket.as_str()
    }

    fn expiry_age(&self) -> Option<ExpiryAge> {
        self.config.tuning.consider_expired_after
    }

    #[cfg(test)]
    pub(crate) fn existence(&self) -> &S3ExistenceCache {
        &self.existence
    }
}

impl Drop for S3Backend {
    fn drop(&mut self) {
        // Dropping the final store must not cancel aborts already issued by upload guards.
        // Call drain_multipart_aborts before shutting down the runtime to wait for completion.
        self.multipart_aborts.get_mut().detach_all();
    }
}

struct MultipartAbortGuard {
    backend: Arc<S3Backend>,
    client: aws_sdk_s3::Client,
    runtime: tokio::runtime::Handle,
    key: String,
    upload_id: String,
    armed: bool,
}

impl MultipartAbortGuard {
    async fn abort(&mut self) {
        match abort_multipart(
            &self.client,
            self.backend.bucket(),
            &self.key,
            &self.upload_id,
        )
        .await
        {
            Ok(()) => self.armed = false,
            Err(error) => tracing::warn!(%error, "failed to abort S3 multipart upload"),
        }
    }
}

impl Drop for MultipartAbortGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let client = self.client.clone();
        let bucket = self.backend.bucket().to_owned();
        let key = std::mem::take(&mut self.key);
        let upload_id = std::mem::take(&mut self.upload_id);
        let mut tasks = self.backend.multipart_aborts.lock();
        while tasks.try_join_next().is_some() {}
        tasks.spawn_on(
            async move {
                if let Err(error) = abort_multipart(&client, &bucket, &key, &upload_id).await {
                    tracing::warn!(%error, "failed to abort cancelled S3 multipart upload");
                }
            },
            &self.runtime,
        );
    }
}

async fn abort_multipart(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<(), PortError> {
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .map_err(|error| {
            PortError::from(AwsError::from(call_error("AbortMultipartUpload", error)))
        })?;
    Ok(())
}

/// Computes the CAS S3 key prefix (`<key_prefix>cas/`), matching `v0.3.9`.
#[must_use]
pub(crate) fn cas_key_prefix(config: &S3CacheConfig) -> String {
    format!("{}{CONTENT_PREFIX}", config.key_prefix.as_str())
}

/// Computes the AC S3 key prefix (`<key_prefix>ac/[<name>/<version>/]`), matching `v0.3.9`.
#[must_use]
pub(crate) fn action_key_prefix(config: &S3CacheConfig, environment: &EnvironmentName) -> String {
    match environment.cache_namespace() {
        Some(namespace) => format!("{}{ACTION_PREFIX}{namespace}/", config.key_prefix.as_str()),
        None => format!("{}{ACTION_PREFIX}", config.key_prefix.as_str()),
    }
}

/// Formats an object key under `prefix` using the v0.3.9-compatible `{hex}-{size}` digest layout.
#[must_use]
pub(crate) fn object_key(prefix: &str, digest: Digest) -> String {
    format!("{prefix}{}-{}", digest.hex(), digest.size())
}

/// S3-backed [`ContentStore`] implementation.
#[derive(Debug, Clone)]
pub struct S3ContentStore {
    backend: Arc<S3Backend>,
    prefix: String,
}

impl S3ContentStore {
    pub(crate) fn new(backend: Arc<S3Backend>) -> Self {
        let prefix = cas_key_prefix(&backend.config);
        Self { backend, prefix }
    }

    /// Waits for cancellation-triggered multipart aborts to finish.
    ///
    /// Stop uploads before calling this during shutdown, and keep the Tokio runtime alive until
    /// it returns. Abort failures are logged by their cleanup tasks.
    pub async fn drain_multipart_aborts(&self) {
        while std::future::poll_fn(|cx| self.backend.multipart_aborts.lock().poll_join_next(cx))
            .await
            .is_some()
        {}
    }

    fn key(&self, digest: Digest) -> String {
        object_key(&self.prefix, digest)
    }

    async fn head_blob(&self, digest: Digest, now: Timestamp) -> Result<Option<u64>, PortError> {
        let key = self.key(digest);
        let output = self
            .backend
            .client()
            .await
            .head_object()
            .bucket(self.backend.bucket())
            .key(&key)
            .send()
            .await;

        let head = match output {
            Ok(head) => head,
            Err(sdk_err) if is_head_not_found(&sdk_err) => {
                self.backend.existence.invalidate(&digest);
                return Ok(None);
            }
            Err(sdk_err) => {
                return Err(PortError::from(AwsError::from(call_error(
                    "HeadObject",
                    sdk_err,
                ))));
            }
        };

        let last_modified = parse_last_modified(head.last_modified());
        if is_expired(last_modified, self.backend.expiry_age(), now) {
            self.backend.existence.invalidate(&digest);
            return Ok(None);
        }

        let Some(content_length) = head
            .content_length()
            .and_then(|length| u64::try_from(length).ok())
        else {
            self.backend.existence.invalidate(&digest);
            return Err(PortError::from(AwsError::malformed(
                "HeadObject",
                "content_length",
            )));
        };

        if content_length != digest.size() {
            self.backend.existence.invalidate(&digest);
            return Ok(None);
        }

        self.backend
            .existence
            .record_head(digest, last_modified, now);
        Ok(Some(content_length))
    }

    async fn upload_single(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        let capacity = usize::try_from(digest.size()).map_err(|_| {
            PortError::from(AwsError::Config {
                field: "single_put_max",
                reason: "blob size exceeds addressable memory".to_owned(),
            })
        })?;
        let mut buffer = BytesMut::with_capacity(capacity);
        let mut hasher = Sha256::new();
        let mut actual_size: u64 = 0;
        let mut chunks = body.chunks;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            actual_size = actual_size.saturating_add(chunk_len);
            if actual_size > digest.size() {
                return Err(PortError::refused(Rejection::DigestMismatch));
            }
            hasher.update(&chunk);
            buffer.extend_from_slice(&chunk);
        }
        let actual_hash: [u8; 32] = hasher.finalize().into();
        if actual_size != digest.size() || actual_hash != *digest.hash() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }

        let content_length = i64::try_from(digest.size()).unwrap_or(i64::MAX);
        self.backend
            .client()
            .await
            .put_object()
            .bucket(self.backend.bucket())
            .key(self.key(digest))
            .content_length(content_length)
            .body(ByteStream::from(buffer.freeze()))
            .send()
            .await
            .map_err(|err| PortError::from(AwsError::from(call_error("PutObject", err))))?;

        self.backend
            .existence
            .record_write(digest, self.backend.clock.wall());
        Ok(())
    }

    async fn upload_multipart(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        let min_part = self.backend.config.tuning.single_put_max.as_u64();
        let part_size_u64 = digest
            .size()
            .div_ceil(MAX_MULTIPART_PARTS)
            .max(min_part)
            .min(MAX_MULTIPART_PART_BYTES);
        let part_size = usize::try_from(part_size_u64).unwrap_or(usize::MAX);
        let key = self.key(digest);
        let client = self.backend.client().await;
        let bucket = self.backend.bucket();

        let created = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .send()
            .await
            .map_err(|err| {
                PortError::from(AwsError::from(call_error("CreateMultipartUpload", err)))
            })?;
        let upload_id = created.upload_id().map(str::to_owned).ok_or_else(|| {
            PortError::from(AwsError::malformed("CreateMultipartUpload", "upload_id"))
        })?;

        let mut abort_guard = MultipartAbortGuard {
            backend: Arc::clone(&self.backend),
            client: client.clone(),
            runtime: tokio::runtime::Handle::current(),
            key,
            upload_id,
            armed: true,
        };
        let result = self
            .upload_multipart_parts(
                digest,
                body,
                &abort_guard.key,
                &abort_guard.upload_id,
                part_size,
            )
            .await;
        if let Err(error) = result {
            abort_guard.abort().await;
            return Err(error);
        }
        abort_guard.armed = false;

        self.backend
            .existence
            .record_write(digest, self.backend.clock.wall());
        Ok(())
    }

    async fn upload_multipart_parts(
        &self,
        digest: Digest,
        body: BlobStream,
        key: &str,
        upload_id: &str,
        part_size: usize,
    ) -> Result<(), PortError> {
        let client = self.backend.client().await;
        let bucket = self.backend.bucket();
        let max_concurrent = usize::try_from(
            self.backend
                .config
                .tuning
                .multipart_max_concurrent_uploads
                .get(),
        )
        .unwrap_or(usize::MAX);

        let mut chunks = body.chunks;
        let mut buffer = BytesMut::with_capacity(part_size);
        let mut hasher = Sha256::new();
        let mut actual_size: u64 = 0;
        let mut next_part_number: i32 = 1;
        let mut in_flight = FuturesUnordered::new();
        let mut completed_parts = Vec::new();

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            actual_size = actual_size.saturating_add(chunk_len);
            if actual_size > digest.size() {
                return Err(PortError::refused(Rejection::DigestMismatch));
            }
            hasher.update(&chunk);
            buffer.extend_from_slice(&chunk);

            while buffer.len() >= part_size {
                let part_bytes = buffer.split_to(part_size).freeze();
                let part_number = next_part_number;
                next_part_number = next_part_number.checked_add(1).ok_or_else(|| {
                    PortError::backend(std::io::Error::other("multipart part count overflow"))
                })?;

                while in_flight.len() >= max_concurrent {
                    if let Some(part_result) = in_flight.next().await {
                        completed_parts.push(part_result?);
                    }
                }
                in_flight.push(upload_one_part(
                    client,
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    part_bytes,
                ));
            }
        }

        let actual_hash: [u8; 32] = hasher.finalize().into();
        if actual_size != digest.size() || actual_hash != *digest.hash() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }

        if !buffer.is_empty() {
            let part_bytes = buffer.freeze();
            let part_number = next_part_number;
            while in_flight.len() >= max_concurrent {
                if let Some(part_result) = in_flight.next().await {
                    completed_parts.push(part_result?);
                }
            }
            in_flight.push(upload_one_part(
                client,
                bucket,
                key,
                upload_id,
                part_number,
                part_bytes,
            ));
        }

        while let Some(part_result) = in_flight.next().await {
            completed_parts.push(part_result?);
        }

        completed_parts.sort_unstable_by_key(CompletedPart::part_number);

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|err| {
                PortError::from(AwsError::from(call_error("CompleteMultipartUpload", err)))
            })?;

        Ok(())
    }
}

async fn upload_one_part(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    bytes: Bytes,
) -> Result<CompletedPart, PortError> {
    let content_length = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
    let output = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .content_length(content_length)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(|err| PortError::from(AwsError::from(call_error("UploadPart", err))))?;

    Ok(CompletedPart::builder()
        .part_number(part_number)
        .set_e_tag(output.e_tag().map(str::to_owned))
        .build())
}

impl ContentStore for S3ContentStore {
    async fn find_missing(
        &self,
        digests: &[Digest],
        missing: &mut Vec<Digest>,
    ) -> Result<(), PortError> {
        missing.clear();
        if digests.is_empty() {
            return Ok(());
        }

        let now = self.backend.clock.wall();
        let mut present = HashSet::new();
        let mut to_check = Vec::new();
        let mut seen_checks = HashSet::new();

        for &digest in digests {
            if digest.is_empty() || self.backend.existence.contains_fresh(&digest, now) {
                present.insert(digest);
            } else if seen_checks.insert(digest) {
                to_check.push(digest);
            }
        }

        if !to_check.is_empty() {
            let concurrency = self.backend.config.tuning.head_concurrency.get();
            let results: Vec<(Digest, bool)> = stream::iter(to_check)
                .map(|digest| async move {
                    let exists = self.head_blob(digest, now).await?.is_some();
                    Ok::<_, PortError>((digest, exists))
                })
                .buffer_unordered(concurrency)
                .try_collect()
                .await?;

            for (digest, exists) in results {
                if exists {
                    present.insert(digest);
                }
            }
        }

        for &digest in digests {
            if !digest.is_empty() && !present.contains(&digest) {
                missing.push(digest);
            }
        }
        Ok(())
    }

    async fn read(
        &self,
        digest: Digest,
        range: ByteRange,
    ) -> Result<Option<BlobStream>, PortError> {
        if digest.is_empty() {
            if range.offset > 0 {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            return Ok(Some(BlobStream::empty()));
        }

        let now = self.backend.clock.wall();
        if range.offset >= digest.size() {
            let Some(total) = self.head_blob(digest, now).await? else {
                return Ok(None);
            };
            if range.offset > total {
                return Err(PortError::refused(Rejection::OutOfRange));
            }
            return Ok(Some(BlobStream::empty()));
        }

        let is_full_read =
            range.offset == 0 && range.limit.is_none_or(|limit| limit.get() >= digest.size());
        let key = self.key(digest);

        let mut request = self
            .backend
            .client()
            .await
            .get_object()
            .bucket(self.backend.bucket())
            .key(&key);
        if !is_full_read {
            let range_header = match range.limit {
                Some(limit) => {
                    let end_inclusive = range.offset.saturating_add(limit.get()).saturating_sub(1);
                    format!("bytes={}-{end_inclusive}", range.offset)
                }
                None => format!("bytes={}-", range.offset),
            };
            request = request.range(range_header);
        }

        let output = match request.send().await {
            Ok(output) => output,
            Err(sdk_err) if is_get_not_found(&sdk_err) => {
                self.backend.existence.invalidate(&digest);
                return Ok(None);
            }
            Err(sdk_err) => {
                return Err(PortError::from(AwsError::from(call_error(
                    "GetObject",
                    sdk_err,
                ))));
            }
        };

        let last_modified = parse_last_modified(output.last_modified());
        if is_expired(last_modified, self.backend.expiry_age(), now) {
            self.backend.existence.invalidate(&digest);
            return Ok(None);
        }

        let content_length = output
            .content_length()
            .and_then(|length| u64::try_from(length).ok());

        if is_full_read {
            return self
                .full_read_stream(digest, key, output.body, content_length, last_modified, now)
                .map(Some);
        }

        let stream_size = content_length.unwrap_or_else(|| {
            let available = digest.size().saturating_sub(range.offset);
            range
                .limit
                .map_or(available, |limit| available.min(limit.get()))
        });
        let chunks = raw_read_stream(output.body);
        Ok(Some(BlobStream {
            size: stream_size,
            chunks,
        }))
    }

    async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
        if body.size != digest.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        if digest.is_empty() {
            let mut chunks = body.chunks;
            while let Some(chunk) = chunks.next().await {
                if !chunk?.is_empty() {
                    return Err(PortError::refused(Rejection::DigestMismatch));
                }
            }
            return Ok(());
        }

        if digest.size() <= self.backend.config.tuning.single_put_max.as_u64() {
            self.upload_single(digest, body).await
        } else {
            self.upload_multipart(digest, body).await
        }
    }

    fn usage(&self) -> Option<EngineStore> {
        None
    }

    async fn shutdown(&self) {
        self.drain_multipart_aborts().await;
    }
}

impl S3ContentStore {
    fn full_read_stream(
        &self,
        digest: Digest,
        key: String,
        body: ByteStream,
        content_length: Option<u64>,
        last_modified: Option<Timestamp>,
        now: Timestamp,
    ) -> Result<BlobStream, PortError> {
        if let Some(len) = content_length
            && len != digest.size()
        {
            tracing::error!(
                key = %key,
                expected = %digest,
                actual_bytes = len,
                "S3 blob content length mismatch on full read"
            );
            self.backend.existence.invalidate(&digest);
            return Err(PortError::rejected(
                Rejection::DigestMismatch,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "S3 object {key} length {len} does not match expected {}",
                        digest.size()
                    ),
                ),
            ));
        }
        let chunks = verified_read_stream(
            body,
            digest,
            key,
            Arc::clone(&self.backend.existence),
            last_modified,
            now,
        );
        Ok(BlobStream {
            size: digest.size(),
            chunks,
        })
    }
}

/// State machine for streaming whole-blob SHA-256 verification.
struct VerifiedReadState {
    body: ByteStream,
    hasher: Sha256,
    bytes_read: u64,
    expected: Digest,
    key: String,
    existence: Arc<S3ExistenceCache>,
    last_modified: Option<Timestamp>,
    now: Timestamp,
    done: bool,
}

fn verified_read_stream(
    body: ByteStream,
    expected: Digest,
    key: String,
    existence: Arc<S3ExistenceCache>,
    last_modified: Option<Timestamp>,
    now: Timestamp,
) -> BoxStream<'static, Result<Bytes, PortError>> {
    let state = VerifiedReadState {
        body,
        hasher: Sha256::new(),
        bytes_read: 0,
        expected,
        key,
        existence,
        last_modified,
        now,
        done: false,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        loop {
            match state.body.next().await {
                Some(Ok(chunk)) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    state.hasher.update(&chunk);
                    let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
                    state.bytes_read = state.bytes_read.saturating_add(len);
                    return Some((Ok(chunk), state));
                }
                Some(Err(err)) => {
                    state.done = true;
                    return Some((Err(PortError::unavailable(err)), state));
                }
                None => {
                    state.done = true;
                    let actual_hash: [u8; 32] = state.hasher.finalize_reset().into();
                    if state.bytes_read != state.expected.size()
                        || actual_hash != *state.expected.hash()
                    {
                        tracing::error!(
                            key = %state.key,
                            expected = %state.expected,
                            actual_bytes = state.bytes_read,
                            "S3 blob digest mismatch on full read"
                        );
                        state.existence.invalidate(&state.expected);
                        return Some((
                            Err(PortError::rejected(
                                Rejection::DigestMismatch,
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("S3 object {} failed SHA-256 verification", state.key),
                                ),
                            )),
                            state,
                        ));
                    }
                    state
                        .existence
                        .record_head(state.expected, state.last_modified, state.now);
                    return None;
                }
            }
        }
    }))
}

fn raw_read_stream(body: ByteStream) -> BoxStream<'static, Result<Bytes, PortError>> {
    Box::pin(stream::unfold(
        (body, false),
        |(mut body, done)| async move {
            if done {
                return None;
            }
            loop {
                match body.next().await {
                    Some(Ok(chunk)) => {
                        if chunk.is_empty() {
                            continue;
                        }
                        return Some((Ok(chunk), (body, false)));
                    }
                    Some(Err(err)) => {
                        return Some((Err(PortError::unavailable(err)), (body, true)));
                    }
                    None => return None,
                }
            }
        },
    ))
}

/// S3-backed [`ActionStore`] implementation scoped to one [`EnvironmentName`].
#[derive(Debug, Clone)]
pub struct S3ActionStore {
    backend: Arc<S3Backend>,
    prefix: String,
}

impl S3ActionStore {
    pub(crate) fn new(backend: Arc<S3Backend>, environment: &EnvironmentName) -> Self {
        let prefix = action_key_prefix(&backend.config, environment);
        Self { backend, prefix }
    }

    fn key(&self, action: Digest) -> String {
        object_key(&self.prefix, action)
    }
}

impl ActionStore for S3ActionStore {
    async fn get(&self, action: Digest) -> Result<Option<Bytes>, PortError> {
        let output = self
            .backend
            .client()
            .await
            .get_object()
            .bucket(self.backend.bucket())
            .key(self.key(action))
            .send()
            .await;

        let object = match output {
            Ok(object) => object,
            Err(sdk_err) if is_get_not_found(&sdk_err) => return Ok(None),
            Err(sdk_err) => {
                return Err(PortError::from(AwsError::from(call_error(
                    "GetObject",
                    sdk_err,
                ))));
            }
        };

        let last_modified = parse_last_modified(object.last_modified());
        if is_expired(
            last_modified,
            self.backend.expiry_age(),
            self.backend.clock.wall(),
        ) {
            return Ok(None);
        }

        let collected = object
            .body
            .collect()
            .await
            .map_err(PortError::unavailable)?;
        Ok(Some(collected.into_bytes()))
    }

    async fn put(&self, action: Digest, result: Bytes) -> Result<(), PortError> {
        let content_length = i64::try_from(result.len()).unwrap_or(i64::MAX);
        self.backend
            .client()
            .await
            .put_object()
            .bucket(self.backend.bucket())
            .key(self.key(action))
            .content_length(content_length)
            .body(ByteStream::from(result))
            .send()
            .await
            .map_err(|err| PortError::from(AwsError::from(call_error("PutObject", err))))?;
        Ok(())
    }

    fn usage(&self) -> Option<EngineStore> {
        None
    }
}

fn is_head_not_found(err: &SdkError<HeadObjectError, HttpResponse>) -> bool {
    match err {
        SdkError::ServiceError(service) => {
            matches!(service.err(), HeadObjectError::NotFound(_))
                || service.raw().status().as_u16() == HTTP_NOT_FOUND
        }
        _ => false,
    }
}

fn is_get_not_found(err: &SdkError<GetObjectError, HttpResponse>) -> bool {
    match err {
        SdkError::ServiceError(service) => {
            matches!(service.err(), GetObjectError::NoSuchKey(_))
                || service.raw().status().as_u16() == HTTP_NOT_FOUND
        }
        _ => false,
    }
}

fn parse_last_modified(dt: Option<&DateTime>) -> Option<Timestamp> {
    let millis = dt.and_then(|value| value.to_millis().ok())?;
    let clamped = u64::try_from(millis).unwrap_or(0);
    Some(Timestamp::from_unix_millis(clamped))
}

fn is_expired(
    last_modified: Option<Timestamp>,
    consider_expired_after: Option<ExpiryAge>,
    now: Timestamp,
) -> bool {
    let (Some(modified), Some(age)) = (last_modified, consider_expired_after) else {
        return false;
    };
    modified + Duration::from_secs(u64::from(age.as_secs())) <= now
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::fmt::Write as _;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use aws_credential_types::Credentials;
    use axum::Router;
    use axum::body::Bytes as AxumBytes;
    use axum::extract::{DefaultBodyLimit, Path, State};
    use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{delete, get, head, post, put};
    use gang_aws_sdk::{ClientSettings, Region, loader};
    use gang_clock::{Clock, TestClock, Timestamp};
    use gang_spi::{CacheStorePort as _, Plugin as _};
    use gang_testkit::{action_store_contract, content_store_contract};
    use gang_types::NonZeroDuration;
    use parking_lot::Mutex;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::cache::S3Cache;
    use crate::test_support::context;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[derive(Clone)]
    struct StoredObject {
        data: Vec<u8>,
        last_modified: Timestamp,
    }

    #[derive(Default)]
    struct MultipartUpload {
        key: String,
        parts: BTreeMap<i32, Vec<u8>>,
    }

    struct FakeS3State {
        bucket: String,
        clock: Arc<TestClock>,
        objects: HashMap<String, StoredObject>,
        uploads: HashMap<String, MultipartUpload>,
        next_upload_id: u64,
        head_requests: usize,
        put_requests: usize,
        get_requests: usize,
        create_multipart_requests: usize,
        upload_part_requests: usize,
        complete_multipart_requests: usize,
        abort_multipart_requests: usize,
        abort_received: Arc<tokio::sync::Notify>,
        in_flight_heads: Arc<AtomicUsize>,
        max_concurrent_heads: Arc<AtomicUsize>,
        head_delay: Duration,
    }

    impl FakeS3State {
        fn new(bucket: String, clock: Arc<TestClock>) -> Self {
            Self {
                bucket,
                clock,
                objects: HashMap::new(),
                uploads: HashMap::new(),
                next_upload_id: 1,
                head_requests: 0,
                put_requests: 0,
                get_requests: 0,
                create_multipart_requests: 0,
                upload_part_requests: 0,
                complete_multipart_requests: 0,
                abort_multipart_requests: 0,
                abort_received: Arc::new(tokio::sync::Notify::new()),
                in_flight_heads: Arc::new(AtomicUsize::new(0)),
                max_concurrent_heads: Arc::new(AtomicUsize::new(0)),
                head_delay: Duration::ZERO,
            }
        }
    }

    struct FakeS3 {
        endpoint: String,
        state: Arc<Mutex<FakeS3State>>,
        server: JoinHandle<()>,
    }

    impl Drop for FakeS3 {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    impl FakeS3 {
        async fn start(bucket: &str, clock: Arc<TestClock>) -> Result<Self, std::io::Error> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let state = Arc::new(Mutex::new(FakeS3State::new(bucket.to_owned(), clock)));
            let router = Router::new()
                .route("/{bucket}/{*key}", head(handle_head))
                .route("/{bucket}/{*key}", get(handle_get))
                .route("/{bucket}/{*key}", put(handle_put))
                .route("/{bucket}/{*key}", post(handle_post))
                .route("/{bucket}/{*key}", delete(handle_delete))
                .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
                .with_state(Arc::clone(&state));
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, router).await;
            });
            Ok(Self {
                endpoint: format!("http://{address}"),
                state,
                server,
            })
        }

        async fn s3_client(&self) -> aws_sdk_s3::Client {
            let region = Region::try_new("us-east-1").expect("valid test region");
            let settings = ClientSettings {
                request_timeout: NonZeroDuration::from_hours(1),
                connect_timeout: NonZeroDuration::from_hours(1),
                max_attempts: NonZeroU32::MIN,
                retry_initial_backoff: NonZeroDuration::from_millis(1),
                retry_max_backoff: NonZeroDuration::from_millis(1),
            };
            let sdk = loader(&region, &settings)
                .endpoint_url(&self.endpoint)
                .credentials_provider(Credentials::new(
                    "AKIDEXAMPLE",
                    "secretexample",
                    None,
                    None,
                    "fake-s3",
                ))
                .load()
                .await;
            let conf = aws_sdk_s3::config::Builder::from(&sdk)
                .endpoint_url(&self.endpoint)
                .force_path_style(true)
                .build();
            aws_sdk_s3::Client::from_conf(conf)
        }
    }

    fn format_http_date(timestamp: Timestamp) -> String {
        let secs = i64::try_from(timestamp.as_unix_millis() / 1_000).unwrap_or(0);
        let days = secs.div_euclid(86_400);
        let sod = secs.rem_euclid(86_400);
        let hour = sod / 3_600;
        let minute = (sod % 3_600) / 60;
        let second = sod % 60;
        let weekday = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"]
            [usize::try_from((days + 7_000_000) % 7).unwrap_or(0)];

        // Howard Hinnant's civil-from-days conversion.
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = u64::try_from(z - era * 146_097).unwrap_or(0);
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let y = i64::try_from(yoe).unwrap_or(0) + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = y + i64::from(month <= 2);
        let month_name = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ][usize::try_from(month - 1).unwrap_or(0)];

        format!("{weekday}, {day:02} {month_name} {year:04} {hour:02}:{minute:02}:{second:02} GMT")
    }

    fn not_found_response(code: &str) -> Response {
        let body = format!("<Error><Code>{code}</Code><Message>not found</Message></Error>");
        (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "application/xml")],
            body,
        )
            .into_response()
    }

    async fn handle_head(
        Path((bucket, key)): Path<(String, String)>,
        State(state): State<Arc<Mutex<FakeS3State>>>,
    ) -> Response {
        let (delay, in_flight, max_concurrent, expected_bucket) = {
            let mut guard = state.lock();
            guard.head_requests += 1;
            (
                guard.head_delay,
                Arc::clone(&guard.in_flight_heads),
                Arc::clone(&guard.max_concurrent_heads),
                guard.bucket.clone(),
            )
        };
        let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        max_concurrent.fetch_max(active, Ordering::SeqCst);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        in_flight.fetch_sub(1, Ordering::SeqCst);

        if bucket != expected_bucket {
            return StatusCode::NOT_FOUND.into_response();
        }
        let object = state.lock().objects.get(&key).cloned();
        match object {
            Some(obj) => {
                let mut headers = HeaderMap::new();
                if let Ok(val) = HeaderValue::from_str(&obj.data.len().to_string()) {
                    headers.insert(header::CONTENT_LENGTH, val);
                }
                if let Ok(val) = HeaderValue::from_str(&format_http_date(obj.last_modified)) {
                    headers.insert(header::LAST_MODIFIED, val);
                }
                headers.insert(header::ETAG, HeaderValue::from_static("\"etag\""));
                (StatusCode::OK, headers).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn handle_get(
        Path((bucket, key)): Path<(String, String)>,
        headers: HeaderMap,
        State(state): State<Arc<Mutex<FakeS3State>>>,
    ) -> Response {
        let object = {
            let mut guard = state.lock();
            guard.get_requests += 1;
            if bucket != guard.bucket {
                return not_found_response("NoSuchBucket");
            }
            guard.objects.get(&key).cloned()
        };
        let Some(obj) = object else {
            return not_found_response("NoSuchKey");
        };

        let mut resp_headers = HeaderMap::new();
        if let Ok(val) = HeaderValue::from_str(&format_http_date(obj.last_modified)) {
            resp_headers.insert(header::LAST_MODIFIED, val);
        }
        resp_headers.insert(header::ETAG, HeaderValue::from_static("\"etag\""));

        if let Some(range_val) = headers.get(header::RANGE).and_then(|v| v.to_str().ok())
            && let Some(spec) = range_val.strip_prefix("bytes=")
            && let Some((start_s, end_s)) = spec.split_once('-')
            && let Ok(start) = start_s.parse::<usize>()
        {
            if start >= obj.data.len() {
                return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
            }
            let end_inclusive = if end_s.is_empty() {
                obj.data.len() - 1
            } else if let Ok(end) = end_s.parse::<usize>() {
                end.min(obj.data.len() - 1)
            } else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            if end_inclusive < start {
                return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
            }
            let slice = obj.data[start..=end_inclusive].to_vec();
            if let Ok(val) = HeaderValue::from_str(&slice.len().to_string()) {
                resp_headers.insert(header::CONTENT_LENGTH, val);
            }
            if let Ok(val) =
                HeaderValue::from_str(&format!("bytes {start}-{end_inclusive}/{}", obj.data.len()))
            {
                resp_headers.insert(header::CONTENT_RANGE, val);
            }
            return (StatusCode::PARTIAL_CONTENT, resp_headers, slice).into_response();
        }

        if let Ok(val) = HeaderValue::from_str(&obj.data.len().to_string()) {
            resp_headers.insert(header::CONTENT_LENGTH, val);
        }
        (StatusCode::OK, resp_headers, obj.data).into_response()
    }

    fn parse_query(uri: &Uri) -> HashMap<String, String> {
        uri.query()
            .unwrap_or("")
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| match pair.split_once('=') {
                Some((key, val)) => (key.to_owned(), val.to_owned()),
                None => (pair.to_owned(), String::new()),
            })
            .collect()
    }

    async fn handle_put(
        Path((bucket, key)): Path<(String, String)>,
        uri: Uri,
        State(state): State<Arc<Mutex<FakeS3State>>>,
        body: AxumBytes,
    ) -> Response {
        let query = parse_query(&uri);
        let mut guard = state.lock();
        if bucket != guard.bucket {
            return not_found_response("NoSuchBucket");
        }
        if let (Some(upload_id), Some(part_number_str)) =
            (query.get("uploadId"), query.get("partNumber"))
        {
            guard.upload_part_requests += 1;
            let Ok(part_number) = part_number_str.parse::<i32>() else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            let Some(upload) = guard.uploads.get_mut(upload_id) else {
                return not_found_response("NoSuchUpload");
            };
            upload.parts.insert(part_number, body.to_vec());
            let mut headers = HeaderMap::new();
            if let Ok(val) = HeaderValue::from_str(&format!("\"part-{part_number}\"")) {
                headers.insert(header::ETAG, val);
            }
            return (StatusCode::OK, headers).into_response();
        }

        guard.put_requests += 1;
        let now = guard.clock.wall();
        guard.objects.insert(
            key,
            StoredObject {
                data: body.to_vec(),
                last_modified: now,
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(header::ETAG, HeaderValue::from_static("\"etag\""));
        (StatusCode::OK, headers).into_response()
    }

    async fn handle_post(
        Path((bucket, key)): Path<(String, String)>,
        uri: Uri,
        State(state): State<Arc<Mutex<FakeS3State>>>,
        body: AxumBytes,
    ) -> Response {
        let query = parse_query(&uri);
        let mut guard = state.lock();
        if bucket != guard.bucket {
            return not_found_response("NoSuchBucket");
        }
        if query.contains_key("uploads") {
            guard.create_multipart_requests += 1;
            let id = format!("upload-{}", guard.next_upload_id);
            guard.next_upload_id += 1;
            guard.uploads.insert(
                id.clone(),
                MultipartUpload {
                    key: key.clone(),
                    parts: BTreeMap::new(),
                },
            );
            let xml = format!(
                "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
            );
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/xml")],
                xml,
            )
                .into_response();
        }
        if let Some(upload_id) = query.get("uploadId") {
            guard.complete_multipart_requests += 1;
            let Some(upload) = guard.uploads.remove(upload_id) else {
                return not_found_response("NoSuchUpload");
            };
            let part_numbers = parse_completed_part_numbers(&body);
            let mut assembled = Vec::new();
            for part_num in part_numbers {
                if let Some(bytes) = upload.parts.get(&part_num) {
                    assembled.extend_from_slice(bytes);
                }
            }
            let now = guard.clock.wall();
            guard.objects.insert(
                upload.key,
                StoredObject {
                    data: assembled,
                    last_modified: now,
                },
            );
            let xml = format!(
                "<CompleteMultipartUploadResult><Location>http://127.0.0.1/{bucket}/{key}</Location><Bucket>{bucket}</Bucket><Key>{key}</Key><ETag>\"done\"</ETag></CompleteMultipartUploadResult>"
            );
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/xml")],
                xml,
            )
                .into_response();
        }
        StatusCode::BAD_REQUEST.into_response()
    }

    async fn handle_delete(
        Path((bucket, _key)): Path<(String, String)>,
        uri: Uri,
        State(state): State<Arc<Mutex<FakeS3State>>>,
    ) -> Response {
        let query = parse_query(&uri);
        let mut guard = state.lock();
        if bucket != guard.bucket {
            return not_found_response("NoSuchBucket");
        }
        if let Some(upload_id) = query.get("uploadId") {
            guard.abort_multipart_requests += 1;
            guard.uploads.remove(upload_id);
            guard.abort_received.notify_one();
            return StatusCode::NO_CONTENT.into_response();
        }
        StatusCode::BAD_REQUEST.into_response()
    }

    fn parse_completed_part_numbers(xml: &[u8]) -> Vec<i32> {
        let text = String::from_utf8_lossy(xml);
        let mut numbers = Vec::new();
        let mut rest = text.as_ref();
        while let Some(start) = rest.find("<PartNumber>") {
            let after_start = &rest[start + "<PartNumber>".len()..];
            let Some(end) = after_start.find("</PartNumber>") else {
                break;
            };
            if let Ok(n) = after_start[..end].trim().parse::<i32>() {
                numbers.push(n);
            }
            rest = &after_start[end + "</PartNumber>".len()..];
        }
        numbers
    }

    async fn make_cache(
        fake: &FakeS3,
        clock: Arc<TestClock>,
        extra: Value,
    ) -> Result<S3Cache, Box<dyn std::error::Error>> {
        let mut config = json!({
            "region": "us-east-1",
            "bucket": "example-bucket",
            "endpoint": fake.endpoint,
            "force_path_style": true,
        });
        if let (Some(base), Value::Object(extra)) = (config.as_object_mut(), extra) {
            base.extend(extra);
        }
        let parsed: S3CacheConfig = serde_json::from_value(config)?;
        Ok(S3Cache::with_s3_client(
            parsed,
            clock,
            fake.s3_client().await,
        ))
    }

    #[test]
    fn key_layout_matches_v039_nativelink_keys() -> TestResult {
        let default_env = EnvironmentName::default();
        let bumped_env = EnvironmentName::try_from("debian-13/2".to_owned())?;
        let digest = Digest::from_bytes(b"key-parity-payload");
        let nativelink_digest_key = {
            let mut s = String::with_capacity(64 + 1 + 20);
            for byte in digest.hash() {
                let _ = write!(s, "{byte:02x}");
            }
            let _ = write!(s, "-{}", digest.size());
            s
        };

        for prefix in ["", "gang/", "org/team/"] {
            let config_json = json!({
                "region": "us-east-1",
                "bucket": "example-bucket",
                "key_prefix": prefix,
            });
            let cache = S3Cache::build(serde_json::from_value(config_json)?, &context())?;

            let cas_store = cache.content_store()?;
            assert_eq!(
                cas_store.key(digest),
                format!("{prefix}cas/{nativelink_digest_key}")
            );

            for (env, expected_ac_prefix) in [
                (&default_env, format!("{prefix}ac/")),
                (&bumped_env, format!("{prefix}ac/debian-13/2/")),
            ] {
                let ac_store = cache.action_store(env)?;
                assert_eq!(
                    ac_store.key(digest),
                    format!("{expected_ac_prefix}{nativelink_digest_key}")
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn content_and_action_stores_satisfy_contracts() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_794_960_000_000,
        )));
        let fake = FakeS3::start("example-bucket", Arc::clone(&clock)).await?;
        let cache = make_cache(&fake, clock, json!({})).await?;

        let content = cache.s3_content_store();
        assert!(content.usage().is_none());
        content_store_contract(&content).await?;

        let default_ac = cache.s3_action_store(&EnvironmentName::default());
        assert!(default_ac.usage().is_none());
        action_store_contract(&default_ac).await?;

        let bumped_env = EnvironmentName::try_from("debian-13/2".to_owned())?;
        let bumped_ac = cache.s3_action_store(&bumped_env);
        let probe = Digest::from_bytes(b"env-isolation-probe");
        default_ac
            .put(probe, Bytes::from_static(b"default-val"))
            .await?;
        assert_eq!(bumped_ac.get(probe).await?, None);
        bumped_ac
            .put(probe, Bytes::from_static(b"bumped-val"))
            .await?;
        assert_eq!(
            default_ac.get(probe).await?,
            Some(Bytes::from_static(b"default-val"))
        );
        assert_eq!(
            bumped_ac.get(probe).await?,
            Some(Bytes::from_static(b"bumped-val"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn full_read_detects_corrupted_s3_object_and_invalidates_existence() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_794_960_000_000,
        )));
        let fake = FakeS3::start("example-bucket", Arc::clone(&clock)).await?;
        let cache = make_cache(&fake, Arc::clone(&clock), json!({})).await?;
        let content = cache.s3_content_store();

        let payload = b"original-valid-payload";
        let digest = Digest::from_bytes(payload);
        content
            .write(digest, BlobStream::from_bytes(Bytes::from_static(payload)))
            .await?;

        // Corrupt bytes in S3 with identical byte length.
        let key = content.key(digest);
        {
            let mut guard = fake.state.lock();
            let obj = guard.objects.get_mut(&key).ok_or("stored object missing")?;
            obj.data[0] ^= 0xff;
        }

        // Full read streams and fails with Rejection::DigestMismatch.
        let stream = content
            .read(digest, ByteRange::FULL)
            .await?
            .ok_or("corrupted object is still returned before stream verification")?;
        let err = stream
            .collect()
            .await
            .err()
            .ok_or("corrupted stream unexpectedly succeeded")?;
        assert!(matches!(
            err,
            PortError::Rejected {
                reason: Rejection::DigestMismatch,
                ..
            }
        ));

        // Corrupt length is also caught immediately on full read.
        {
            let mut guard = fake.state.lock();
            let obj = guard.objects.get_mut(&key).ok_or("stored object missing")?;
            obj.data.pop();
        }
        let err = content
            .read(digest, ByteRange::FULL)
            .await
            .err()
            .ok_or("truncated object unexpectedly succeeded")?;
        assert!(matches!(
            err,
            PortError::Rejected {
                reason: Rejection::DigestMismatch,
                ..
            }
        ));

        // Existence cache was invalidated so find_missing hits S3 again and sees length mismatch -> missing.
        let mut missing = Vec::new();
        content.find_missing(&[digest], &mut missing).await?;
        assert_eq!(missing, vec![digest]);
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_multipart_upload_aborts_uploaded_parts() -> TestResult {
        cancel_multipart_upload(false).await
    }

    #[tokio::test]
    async fn cancelling_multipart_upload_then_dropping_store_still_aborts() -> TestResult {
        cancel_multipart_upload(true).await
    }

    async fn cancel_multipart_upload(drop_stores: bool) -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_794_960_000_000,
        )));
        let fake = FakeS3::start("example-bucket", Arc::clone(&clock)).await?;
        let cache = make_cache(
            &fake,
            clock,
            json!({"tuning": {
                "single_put_max": "5MiB", "multipart_max_concurrent_uploads": 1
            }}),
        )
        .await?;
        let content = cache.s3_content_store();
        let payload = Bytes::from(vec![0x5a; 10 * 1024 * 1024 + 1]);
        let digest = Digest::from_bytes(&payload);
        let (blocked_tx, blocked_rx) = tokio::sync::oneshot::channel();
        let body = BlobStream {
            size: digest.size(),
            chunks: Box::pin(
                stream::once(std::future::ready(Ok(payload))).chain(stream::once(async move {
                    let _ = blocked_tx.send(());
                    std::future::pending::<Result<Bytes, PortError>>().await
                })),
            ),
        };
        let mut upload = Box::pin(content.write(digest, body));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = &mut upload => panic!("upload unexpectedly finished: {result:?}"),
                result = blocked_rx => result.expect("upload reached blocked body"),
            }
        })
        .await?;
        {
            let state = fake.state.lock();
            assert_eq!(state.create_multipart_requests, 1);
            assert_eq!(
                state.upload_part_requests, 1,
                "first part must exist before cancellation"
            );
            assert_eq!(
                state
                    .uploads
                    .values()
                    .next()
                    .expect("active upload")
                    .parts
                    .len(),
                1
            );
        }
        drop(upload);
        if drop_stores {
            let abort_received = Arc::clone(&fake.state.lock().abort_received);
            drop(content);
            drop(cache);
            tokio::time::timeout(Duration::from_secs(10), abort_received.notified()).await?;
        } else {
            tokio::time::timeout(Duration::from_secs(10), content.drain_multipart_aborts()).await?;
        }
        let state = fake.state.lock();
        assert_eq!(state.abort_multipart_requests, 1);
        assert!(
            state.uploads.is_empty(),
            "cancelled upload must not leave orphaned parts"
        );
        assert_eq!(state.complete_multipart_requests, 0);
        assert!(state.objects.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn multipart_upload_streams_and_aborts_on_mismatch() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_794_960_000_000,
        )));
        let fake = FakeS3::start("example-bucket", Arc::clone(&clock)).await?;
        let cache = make_cache(
            &fake,
            Arc::clone(&clock),
            json!({
                "tuning": {
                    "single_put_max": "5MiB",
                    "multipart_max_concurrent_uploads": 2
                }
            }),
        )
        .await?;
        let content = cache.s3_content_store();

        // 5 MiB + 100 bytes forces multipart upload (2 parts: 5 MiB + 100 B).
        let five_mib = 5 * 1024 * 1024;
        let total_len = five_mib + 100;
        let payload: Vec<u8> = (0..total_len)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect();
        let digest = Digest::from_bytes(&payload);

        let chunk1 = Bytes::copy_from_slice(&payload[..3 * 1024 * 1024]);
        let chunk2 = Bytes::copy_from_slice(&payload[3 * 1024 * 1024..]);
        let stream = BlobStream {
            size: digest.size(),
            chunks: Box::pin(stream::iter([Ok(chunk1), Ok(chunk2)])),
        };
        content.write(digest, stream).await?;

        {
            let guard = fake.state.lock();
            assert_eq!(guard.put_requests, 0);
            assert_eq!(guard.create_multipart_requests, 1);
            assert_eq!(guard.upload_part_requests, 2);
            assert_eq!(guard.complete_multipart_requests, 1);
            assert_eq!(guard.abort_multipart_requests, 0);
            assert!(guard.uploads.is_empty());
        }

        let read_back = content
            .read(digest, ByteRange::FULL)
            .await?
            .ok_or("multipart object readable")?
            .collect()
            .await?;
        assert_eq!(read_back.as_ref(), payload.as_slice());

        // Now send mismatched bytes for a multipart-sized digest and assert abort + invisibility.
        let mut corrupted = payload.clone();
        corrupted[total_len - 1] ^= 0xff;
        let bad_target = Digest::from_bytes(&vec![0xaa_u8; total_len]);
        let bad_stream = BlobStream::from_bytes(Bytes::from(corrupted));
        let err = content
            .write(bad_target, bad_stream)
            .await
            .err()
            .ok_or("mismatched multipart upload succeeded")?;
        assert!(matches!(
            err,
            PortError::Rejected {
                reason: Rejection::DigestMismatch,
                ..
            }
        ));
        {
            let guard = fake.state.lock();
            assert_eq!(guard.abort_multipart_requests, 1);
            assert_eq!(guard.complete_multipart_requests, 1);
            assert!(guard.uploads.is_empty());
        }
        assert!(content.read(bad_target, ByteRange::FULL).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn consider_expired_after_applies_to_cas_and_ac() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_794_960_000_000,
        )));
        let fake = FakeS3::start("example-bucket", Arc::clone(&clock)).await?;
        let cache = make_cache(
            &fake,
            Arc::clone(&clock),
            json!({
                "tuning": {
                    "consider_expired_after": "60s",
                    "existence_ttl": "1h"
                }
            }),
        )
        .await?;
        let content = cache.s3_content_store();
        let actions = cache.s3_action_store(&EnvironmentName::default());

        let blob = Bytes::from_static(b"expiring-cas-blob");
        let blob_digest = Digest::from_bytes(&blob);
        content
            .write(blob_digest, BlobStream::from_bytes(blob.clone()))
            .await?;

        let action_digest = Digest::from_bytes(b"expiring-action");
        actions
            .put(action_digest, Bytes::from_static(b"cached-action-result"))
            .await?;

        // Before 60s: fresh in both existence cache and S3.
        clock.advance(Duration::from_secs(59));
        let mut missing = Vec::new();
        content.find_missing(&[blob_digest], &mut missing).await?;
        assert!(missing.is_empty());
        assert_eq!(fake.state.lock().head_requests, 0);
        assert!(content.read(blob_digest, ByteRange::FULL).await?.is_some());
        assert!(actions.get(action_digest).await?.is_some());

        // At 60s: expired for find_missing, read, and ActionStore::get.
        clock.advance(Duration::from_secs(1));
        content.find_missing(&[blob_digest], &mut missing).await?;
        assert_eq!(missing, vec![blob_digest]);
        assert_eq!(fake.state.lock().head_requests, 1);
        assert!(content.read(blob_digest, ByteRange::FULL).await?.is_none());
        assert!(actions.get(action_digest).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn find_missing_bounds_head_concurrency_and_uses_existence_cache() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_794_960_000_000,
        )));
        let fake = FakeS3::start("example-bucket", Arc::clone(&clock)).await?;
        fake.state.lock().head_delay = Duration::from_millis(20);
        let cache = make_cache(
            &fake,
            Arc::clone(&clock),
            json!({
                "tuning": {
                    "head_concurrency": 3,
                    "existence_ttl": "5m"
                }
            }),
        )
        .await?;
        let content = cache.s3_content_store();

        let digests: Vec<Digest> = (0_u32..10)
            .map(|i| Digest::from_bytes(format!("blob-{i}").as_bytes()))
            .collect();
        for &digest in &digests[..5] {
            let bytes = Bytes::from(format!(
                "blob-{}",
                digests
                    .iter()
                    .position(|d| *d == digest)
                    .unwrap_or_default()
            ));
            content.write(digest, BlobStream::from_bytes(bytes)).await?;
        }
        // Invalidate existence cache for the written blobs so find_missing must HeadObject all 10.
        for digest in &digests[..5] {
            cache.existence_cache().invalidate(digest);
        }

        let mut missing = Vec::new();
        content.find_missing(&digests, &mut missing).await?;
        assert_eq!(missing, digests[5..]);

        {
            let guard = fake.state.lock();
            assert_eq!(guard.head_requests, 10);
            assert!(
                guard.max_concurrent_heads.load(Ordering::SeqCst) <= 3,
                "max concurrent heads exceeded 3: {}",
                guard.max_concurrent_heads.load(Ordering::SeqCst)
            );
        }

        // Second find_missing hits existence cache for the 5 present blobs (only 5 missing HEADs).
        content.find_missing(&digests, &mut missing).await?;
        assert_eq!(missing, digests[5..]);
        assert_eq!(fake.state.lock().head_requests, 15);

        // Removing an existing blob from S3 and reading it invalidates the existence cache.
        let key0 = content.key(digests[0]);
        fake.state.lock().objects.remove(&key0);
        assert!(content.read(digests[0], ByteRange::FULL).await?.is_none());
        content.find_missing(&[digests[0]], &mut missing).await?;
        assert_eq!(missing, vec![digests[0]]);
        Ok(())
    }

    #[tokio::test]
    async fn read_range_with_clipping_and_at_end() -> TestResult {
        let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_794_960_000_000,
        )));
        let fake = FakeS3::start("example-bucket", Arc::clone(&clock)).await?;
        let cache = make_cache(&fake, Arc::clone(&clock), json!({})).await?;
        let content = cache.s3_content_store();

        let payload = b"abcdefghij";
        let digest = Digest::from_bytes(payload);
        content
            .write(digest, BlobStream::from_bytes(Bytes::from_static(payload)))
            .await?;

        let stream = content
            .read(
                digest,
                ByteRange {
                    offset: 0,
                    limit: NonZeroU64::new(100),
                },
            )
            .await?
            .ok_or("range covering full object exists")?;
        assert_eq!(stream.size, 10);
        assert_eq!(stream.collect().await?.as_ref(), payload);
        Ok(())
    }
}
