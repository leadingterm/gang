//! `google.bytestream.ByteStream` gRPC service over [`gang_spi::ContentStore`].
//!
//! Supports streaming `Read` (identity and `compressed-blobs/zstd`, honoring `read_offset` and
//! `read_limit` in `read_chunk` frames), resumable streaming `Write` (with sequential
//! `write_offset`, `finish_write`, and bounded `size + 1` ZSTD decompression), and
//! `QueryWriteStatus`, all bounded by [`InflightBlobBudget`] (`max_inflight_blob_bytes`).

use std::collections::{HashMap, VecDeque};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_cas::{
    Compression, DEFAULT_ZSTD_LEVEL, ResourceName, compress_zstd, compress_zstd_stream,
    decompress_zstd_bounded, decompress_zstd_stream, verify_identity_stream,
};
use gang_reapi::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use gang_reapi::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use gang_spi::{BlobStream, ByteRange, ContentStore, PortError, Rejection};
use gang_types::{Digest, NonZeroBytes};
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status, Streaming};

use crate::reapi_cas::{BlobPermit, InflightBlobBudget, map_port_error};

const DEFAULT_READ_CHUNK: NonZeroBytes = NonZeroBytes::kib(64);
const DEFAULT_MAX_PENDING_UPLOADS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1023);
const UPLOAD_CHANNEL_CAPACITY: usize = 8;

/// Configuration for [`ByteStreamService`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteStreamConfig {
    /// Target chunk size emitted per `ByteStream.Read` response frame (`[server] read_chunk`).
    pub read_chunk: NonZeroBytes,
    /// Maximum number of concurrent or paused incomplete upload sessions tracked for resumption.
    pub max_pending_uploads: NonZeroUsize,
}

impl Default for ByteStreamConfig {
    fn default() -> Self {
        Self {
            read_chunk: DEFAULT_READ_CHUNK,
            max_pending_uploads: DEFAULT_MAX_PENDING_UPLOADS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UploadKey {
    instance_name: String,
    upload_id: String,
    compression: Compression,
    digest: Digest,
}

impl UploadKey {
    fn from_resource(resource: &ResourceName) -> Result<Self, Status> {
        match resource {
            ResourceName::Upload {
                instance_name,
                upload_id,
                compression,
                digest,
            } => Ok(Self {
                instance_name: instance_name.clone(),
                upload_id: upload_id.clone(),
                compression: *compression,
                digest: *digest,
            }),
            ResourceName::Blob { .. } => Err(Status::invalid_argument(
                "ByteStream.Write requires an uploads/... resource name",
            )),
        }
    }
}

enum UploadItem {
    Chunk(Bytes, BlobPermit),
    Finish,
}

struct UploadEntry {
    active_writer: AsyncMutex<()>,
    decoded_chunk_limit: u64,
    committed_size: AtomicI64,
    chunk_tx: Mutex<Option<mpsc::Sender<UploadItem>>>,
    result_rx: AsyncMutex<Option<oneshot::Receiver<Result<(), PortError>>>>,
    terminal_error: Mutex<Option<Status>>,
    error_notify: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl UploadEntry {
    fn committed_size(&self) -> i64 {
        self.committed_size.load(Ordering::Acquire)
    }

    fn record_error(&self, status: &Status) {
        let mut guard = self.terminal_error.lock();
        if guard.is_none() {
            *guard = Some(status.clone());
        }
        self.error_notify.notify_waiters();
    }

    fn take_terminal_error(&self) -> Option<Status> {
        self.terminal_error.lock().clone()
    }
}

#[derive(Default)]
struct UploadRegistryState {
    entries: HashMap<UploadKey, Arc<UploadEntry>>,
    order: VecDeque<UploadKey>,
}

struct UploadRegistry {
    max_pending: usize,
    state: Mutex<UploadRegistryState>,
}

impl UploadRegistry {
    fn new(max_pending: NonZeroUsize) -> Self {
        Self {
            max_pending: max_pending.get(),
            state: Mutex::new(UploadRegistryState::default()),
        }
    }

    fn get(&self, key: &UploadKey) -> Option<Arc<UploadEntry>> {
        self.state.lock().entries.get(key).cloned()
    }

    fn remove(&self, key: &UploadKey) {
        let mut state = self.state.lock();
        state.entries.remove(key);
        state.order.retain(|existing| existing != key);
    }

    fn get_or_insert_with(
        &self,
        key: &UploadKey,
        create: impl FnOnce() -> Arc<UploadEntry>,
    ) -> Arc<UploadEntry> {
        let evicted: Option<Arc<UploadEntry>>;
        let entry = {
            let mut state = self.state.lock();
            if let Some(existing) = state.entries.get(key) {
                return Arc::clone(existing);
            }
            evicted = if state.entries.len() >= self.max_pending {
                state
                    .order
                    .pop_front()
                    .and_then(|old_key| state.entries.remove(&old_key))
            } else {
                None
            };
            let created = create();
            state.entries.insert(key.clone(), Arc::clone(&created));
            state.order.push_back(key.clone());
            created
        };
        if let Some(old) = evicted {
            // Dropping `chunk_tx` without `UploadItem::Finish` causes the writer task to abort.
            drop(old.chunk_tx.lock().take());
        }
        entry
    }
}

/// `google.bytestream.ByteStream` gRPC server backed by a [`ContentStore`].
#[derive(Clone)]
pub struct ByteStreamService<C> {
    store: Arc<C>,
    budget: InflightBlobBudget,
    config: ByteStreamConfig,
    uploads: Arc<UploadRegistry>,
}

impl<C: ContentStore> std::fmt::Debug for ByteStreamService<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ByteStreamService")
            .field("store", &self.store)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<C: ContentStore> ByteStreamService<C> {
    /// Creates a [`ByteStreamService`] backed by `store`, `budget`, and `config`.
    #[must_use]
    pub fn new(store: Arc<C>, budget: InflightBlobBudget, config: ByteStreamConfig) -> Self {
        Self {
            store,
            budget,
            config,
            uploads: Arc::new(UploadRegistry::new(config.max_pending_uploads)),
        }
    }

    /// Creates a [`ByteStreamService`] with default [`ByteStreamConfig`].
    #[must_use]
    pub fn with_defaults(store: Arc<C>, budget: InflightBlobBudget) -> Self {
        Self::new(store, budget, ByteStreamConfig::default())
    }

    /// Wraps this service in a tonic [`ByteStreamServer`].
    #[must_use]
    pub fn into_server(self) -> ByteStreamServer<Self> {
        ByteStreamServer::new(self)
    }

    fn effective_read_chunk(&self) -> usize {
        let configured = self.config.read_chunk.get().as_usize().max(1);
        let cap = usize::try_from(self.budget.capacity())
            .unwrap_or(usize::MAX)
            .max(1);
        configured.min(cap)
    }

    async fn read_identity(
        &self,
        digest: Digest,
        offset: u64,
        limit: Option<NonZeroU64>,
    ) -> Result<BoxStream<'static, Result<ReadResponse, Status>>, Status> {
        if digest.is_empty() {
            if offset > 0 {
                return Err(Status::out_of_range(
                    "read_offset exceeds empty blob size 0",
                ));
            }
            return Ok(Box::pin(stream::empty()));
        }

        let range = ByteRange { offset, limit };
        let Some(blob_stream) = self
            .store
            .read(digest, range)
            .await
            .map_err(|err| map_port_error(&err))?
        else {
            return Err(Status::not_found(format!("blob {digest} not found")));
        };

        if blob_stream.size == 0 {
            return Ok(Box::pin(stream::empty()));
        }

        Ok(rechunk_and_budget_stream(
            blob_stream.chunks,
            self.effective_read_chunk(),
            self.budget.clone(),
        ))
    }

    async fn read_zstd(
        &self,
        digest: Digest,
        offset: u64,
        limit: Option<NonZeroU64>,
    ) -> Result<BoxStream<'static, Result<ReadResponse, Status>>, Status> {
        if digest.is_empty() {
            let compressed =
                compress_zstd(&[], DEFAULT_ZSTD_LEVEL).map_err(|err| map_port_error(&err))?;
            let total = u64::try_from(compressed.len()).unwrap_or(u64::MAX);
            if offset > total {
                return Err(Status::out_of_range(format!(
                    "read_offset {offset} exceeds compressed empty blob size {total}"
                )));
            }
            if offset == total {
                return Ok(Box::pin(stream::empty()));
            }
            let start = usize::try_from(offset).unwrap_or(usize::MAX);
            let available = total - offset;
            let len = limit.map_or(available, |lim| available.min(lim.get()));
            let len_usize = usize::try_from(len).unwrap_or(usize::MAX);
            let slice = compressed.slice(start..start.saturating_add(len_usize));
            return Ok(rechunk_and_budget_stream(
                Box::pin(stream::once(std::future::ready(Ok(slice)))),
                self.effective_read_chunk(),
                self.budget.clone(),
            ));
        }

        let Some(blob_stream) = self
            .store
            .read(digest, ByteRange::FULL)
            .await
            .map_err(|err| map_port_error(&err))?
        else {
            return Err(Status::not_found(format!("blob {digest} not found")));
        };

        let compressed_stream = compress_zstd_stream(blob_stream.chunks, DEFAULT_ZSTD_LEVEL);
        let ranged = slice_compressed_stream(compressed_stream, offset, limit);
        Ok(rechunk_and_budget_stream(
            ranged,
            self.effective_read_chunk(),
            self.budget.clone(),
        ))
    }

    fn spawn_upload_entry(&self, digest: Digest, compression: Compression) -> Arc<UploadEntry> {
        let (tx, rx) = mpsc::channel::<UploadItem>(UPLOAD_CHANNEL_CAPACITY);
        let (result_tx, result_rx) = oneshot::channel();
        let store = Arc::clone(&self.store);

        let entry = Arc::new(UploadEntry {
            active_writer: AsyncMutex::new(()),
            decoded_chunk_limit: match compression {
                Compression::Identity => 0,
                Compression::Zstd => digest
                    .size()
                    .saturating_add(1)
                    .min(u64::try_from(gang_cas::STREAM_CHUNK_BYTES).unwrap_or(u64::MAX)),
            },
            committed_size: AtomicI64::new(0),
            chunk_tx: Mutex::new(Some(tx)),
            result_rx: AsyncMutex::new(Some(result_rx)),
            terminal_error: Mutex::new(None),
            error_notify: Notify::new(),
            task: Mutex::new(None),
        });

        let entry_weak = Arc::downgrade(&entry);
        let handle = tokio::spawn(async move {
            let raw_stream = upload_item_stream(rx);
            let body: BlobStream = match compression {
                Compression::Identity => verify_identity_stream(digest, raw_stream),
                Compression::Zstd => decompress_zstd_stream(digest, raw_stream),
            };
            let result = store.write(digest, body).await;
            if let Err(ref err) = result
                && let Some(entry_ref) = entry_weak.upgrade()
            {
                entry_ref.record_error(&map_port_error(err));
            }
            let _ = result_tx.send(result);
        });
        *entry.task.lock() = Some(handle);
        entry
    }
}

fn upload_item_stream(
    rx: mpsc::Receiver<UploadItem>,
) -> BoxStream<'static, Result<Bytes, PortError>> {
    struct State {
        rx: mpsc::Receiver<UploadItem>,
        active_permit: Option<BlobPermit>,
        done: bool,
    }

    let state = State {
        rx,
        active_permit: None,
        done: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        state.active_permit = None;
        match state.rx.recv().await {
            Some(UploadItem::Chunk(bytes, permit)) => {
                state.active_permit = Some(permit);
                Some((Ok(bytes), state))
            }
            Some(UploadItem::Finish) => None,
            None => {
                state.done = true;
                Some((
                    Err(PortError::unavailable("upload aborted before finish_write")),
                    state,
                ))
            }
        }
    }))
}

fn slice_compressed_stream(
    input: BoxStream<'static, Result<Bytes, PortError>>,
    offset: u64,
    limit: Option<NonZeroU64>,
) -> BoxStream<'static, Result<Bytes, PortError>> {
    struct State {
        input: BoxStream<'static, Result<Bytes, PortError>>,
        offset: u64,
        seen: u64,
        remaining: Option<u64>,
        done: bool,
    }

    if offset == 0 && limit.is_none() {
        return input;
    }

    let state = State {
        input,
        offset,
        seen: 0,
        remaining: limit.map(NonZeroU64::get),
        done: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        while let Some(next) = state.input.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            let chunk_start = state.seen;
            let chunk_end = state.seen.saturating_add(len);
            state.seen = chunk_end;

            if chunk_end <= state.offset {
                continue;
            }
            let skip = usize::try_from(state.offset.saturating_sub(chunk_start)).unwrap_or(0);
            let mut slice = chunk.slice(skip..);
            if let Some(rem) = state.remaining {
                if rem == 0 {
                    return None;
                }
                let slice_len = u64::try_from(slice.len()).unwrap_or(u64::MAX);
                if slice_len >= rem {
                    let take = usize::try_from(rem).unwrap_or(slice.len());
                    slice = slice.slice(..take);
                    state.remaining = Some(0);
                    state.done = true;
                } else {
                    state.remaining = Some(rem - slice_len);
                }
            }
            if !slice.is_empty() {
                return Some((Ok(slice), state));
            }
        }

        state.done = true;
        if state.seen < state.offset {
            return Some((Err(PortError::refused(Rejection::OutOfRange)), state));
        }
        None
    }))
}

fn rechunk_and_budget_stream(
    input: BoxStream<'static, Result<Bytes, PortError>>,
    chunk_size: usize,
    budget: InflightBlobBudget,
) -> BoxStream<'static, Result<ReadResponse, Status>> {
    struct State {
        input: BoxStream<'static, Result<Bytes, PortError>>,
        pending: Bytes,
        chunk_size: usize,
        budget: InflightBlobBudget,
        active_permit: Option<BlobPermit>,
        done: bool,
    }

    let state = State {
        input,
        pending: Bytes::new(),
        chunk_size: chunk_size.max(1),
        budget,
        active_permit: None,
        done: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        // Release the previous yielded frame's permit only when the downstream consumer polls for
        // the next frame (or drops the stream).
        state.active_permit = None;

        while state.pending.is_empty() {
            match state.input.next().await {
                Some(Ok(chunk)) => {
                    if !chunk.is_empty() {
                        state.pending = chunk;
                    }
                }
                Some(Err(err)) => {
                    state.done = true;
                    return Some((Err(map_port_error(&err)), state));
                }
                None => return None,
            }
        }

        let take = state.pending.len().min(state.chunk_size);
        let frame_bytes = state.pending.split_to(take);
        let frame_len = u64::try_from(frame_bytes.len()).unwrap_or(u64::MAX);

        match state.budget.acquire(frame_len).await {
            Ok(permit) => {
                state.active_permit = Some(permit);
                Some((Ok(ReadResponse { data: frame_bytes }), state))
            }
            Err(status) => {
                state.done = true;
                Some((Err(status), state))
            }
        }
    }))
}

async fn handle_empty_digest_write(
    first: WriteRequest,
    mut stream: Streaming<WriteRequest>,
    expected_name: &str,
    compression: Compression,
) -> Result<Response<WriteResponse>, Status> {
    let mut expected_offset = 0_i64;
    let mut zstd_buf = BytesMut::new();
    let mut current = Some(first);

    loop {
        let msg = match current.take() {
            Some(msg) => msg,
            None => match stream.message().await? {
                Some(msg) => msg,
                None => {
                    return Err(Status::invalid_argument(
                        "ByteStream.Write stream ended before finish_write",
                    ));
                }
            },
        };

        if !msg.resource_name.is_empty() && msg.resource_name != expected_name {
            return Err(Status::invalid_argument(
                "ByteStream.Write resource_name mismatch",
            ));
        }
        if msg.write_offset < 0 || msg.write_offset != expected_offset {
            return Err(Status::invalid_argument(format!(
                "ByteStream.Write offset mismatch: expected {expected_offset}, got {}",
                msg.write_offset
            )));
        }

        let len_i64 = i64::try_from(msg.data.len())
            .map_err(|_| Status::invalid_argument("WriteRequest chunk size overflow"))?;
        expected_offset = expected_offset
            .checked_add(len_i64)
            .ok_or_else(|| Status::invalid_argument("write_offset overflow"))?;

        if !msg.data.is_empty() {
            match compression {
                Compression::Identity => {
                    return Err(Status::invalid_argument(
                        "non-empty payload for empty blob digest",
                    ));
                }
                Compression::Zstd => {
                    // Reject huge compressed frames claiming an empty blob before buffering.
                    if zstd_buf.len().saturating_add(msg.data.len()) > 64 * 1024 {
                        return Err(Status::invalid_argument(
                            "compressed empty blob exceeds frame limit",
                        ));
                    }
                    zstd_buf.extend_from_slice(&msg.data);
                }
            }
        }

        if msg.finish_write {
            if stream.message().await?.is_some() {
                return Err(Status::invalid_argument(
                    "received WriteRequest after finish_write",
                ));
            }
            if compression == Compression::Zstd {
                decompress_zstd_bounded(Digest::EMPTY, &zstd_buf)
                    .map_err(|err| map_port_error(&err))?;
            }
            return Ok(Response::new(WriteResponse {
                committed_size: expected_offset,
            }));
        }
    }
}

async fn send_chunk_bounded(
    entry: &UploadEntry,
    budget: &InflightBlobBudget,
    tx: &mpsc::Sender<UploadItem>,
    data: Bytes,
) -> Result<(), Status> {
    let max_slice = usize::try_from(budget.capacity())
        .unwrap_or(usize::MAX)
        .max(1);
    let mut offset = 0_usize;
    while offset < data.len() {
        let end = offset.saturating_add(max_slice).min(data.len());
        let slice = data.slice(offset..end);
        let slice_len = u64::try_from(slice.len()).unwrap_or(u64::MAX);
        // The decoder retains this input permit until it has drained all output
        // from the slice. Reserve for either representation once; never acquire
        // an output permit while retaining an input permit from the same budget.
        let reservation = slice_len.max(entry.decoded_chunk_limit);

        let permit = {
            let notified = entry.error_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(err) = entry.take_terminal_error() {
                return Err(err);
            }
            tokio::select! {
                res = budget.acquire(reservation) => res?,
                () = notified => {
                    return Err(entry.take_terminal_error().unwrap_or_else(|| {
                        Status::internal("upload task terminated unexpectedly")
                    }));
                }
            }
        };

        let notified = entry.error_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if let Some(err) = entry.take_terminal_error() {
            return Err(err);
        }
        tokio::select! {
            res = tx.send(UploadItem::Chunk(slice, permit)) => {
                if res.is_err() {
                    return Err(entry.take_terminal_error().unwrap_or_else(|| {
                        Status::internal("upload writer closed")
                    }));
                }
            }
            () = notified => {
                return Err(entry.take_terminal_error().unwrap_or_else(|| {
                    Status::internal("upload writer failed")
                }));
            }
        }
        offset = end;
    }
    Ok(())
}

async fn drive_upload_stream(
    entry: &UploadEntry,
    budget: &InflightBlobBudget,
    tx: &mpsc::Sender<UploadItem>,
    first: WriteRequest,
    stream: &mut Streaming<WriteRequest>,
    expected_name: &str,
) -> Result<(i64, bool), Status> {
    let mut expected_offset = entry.committed_size();
    let mut current = Some(first);

    loop {
        let msg = match current.take() {
            Some(msg) => msg,
            None => match stream.message().await {
                Ok(Some(msg)) => msg,
                Ok(None) => return Ok((expected_offset, false)),
                Err(status) => {
                    return Err(entry.take_terminal_error().unwrap_or(status));
                }
            },
        };

        if !msg.resource_name.is_empty() && msg.resource_name != expected_name {
            return Err(Status::invalid_argument(
                "ByteStream.Write resource_name changed mid-stream",
            ));
        }
        if msg.write_offset < 0 || msg.write_offset != expected_offset {
            return Err(Status::invalid_argument(format!(
                "ByteStream.Write offset mismatch: expected {expected_offset}, got {}",
                msg.write_offset
            )));
        }

        let len_i64 = i64::try_from(msg.data.len())
            .map_err(|_| Status::invalid_argument("WriteRequest chunk size overflow"))?;
        let next_offset = expected_offset
            .checked_add(len_i64)
            .ok_or_else(|| Status::invalid_argument("write_offset overflow"))?;

        if !msg.data.is_empty() {
            send_chunk_bounded(entry, budget, tx, msg.data).await?;
            expected_offset = next_offset;
            entry
                .committed_size
                .store(expected_offset, Ordering::Release);
        }

        if msg.finish_write {
            if stream.message().await?.is_some() {
                drop(entry.chunk_tx.lock().take());
                return Err(Status::invalid_argument(
                    "received WriteRequest after finish_write",
                ));
            }
            return Ok((expected_offset, true));
        }
    }
}

async fn finalize_upload(
    uploads: &UploadRegistry,
    key: &UploadKey,
    entry: &UploadEntry,
    committed_size: i64,
) -> Result<Response<WriteResponse>, Status> {
    let sender = entry.chunk_tx.lock().take();
    if let Some(sender) = sender {
        let _ = sender.send(UploadItem::Finish).await;
        drop(sender);
    }

    let result_rx = entry.result_rx.lock().await.take();
    if let Some(rx) = result_rx {
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                uploads.remove(key);
                return Err(map_port_error(&err));
            }
            Err(_) => {
                uploads.remove(key);
                return Err(Status::internal("upload writer task dropped unexpectedly"));
            }
        }
    }
    let handle = entry.task.lock().take();
    if let Some(handle) = handle {
        let _ = handle.await;
    }
    uploads.remove(key);

    Ok(Response::new(WriteResponse { committed_size }))
}

#[tonic::async_trait]
impl<C: ContentStore> ByteStream for ByteStreamService<C> {
    type ReadStream = BoxStream<'static, Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();
        let parsed = ResourceName::parse(&req.resource_name)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let ResourceName::Blob {
            compression,
            digest,
            ..
        } = parsed
        else {
            return Err(Status::invalid_argument(
                "ByteStream.Read requires a blobs/ or compressed-blobs/ resource name",
            ));
        };

        if req.read_offset < 0 {
            return Err(Status::out_of_range("read_offset cannot be negative"));
        }
        if req.read_limit < 0 {
            return Err(Status::invalid_argument("read_limit cannot be negative"));
        }
        let offset = u64::try_from(req.read_offset)
            .map_err(|_| Status::out_of_range("read_offset out of range"))?;
        let limit = if req.read_limit == 0 {
            None
        } else {
            NonZeroU64::new(u64::try_from(req.read_limit).unwrap_or(0))
        };

        let stream = match compression {
            Compression::Identity => self.read_identity(digest, offset, limit).await?,
            Compression::Zstd => self.read_zstd(digest, offset, limit).await?,
        };
        Ok(Response::new(stream))
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let Some(first) = stream.message().await? else {
            return Err(Status::invalid_argument(
                "empty ByteStream.Write request stream",
            ));
        };

        let expected_name = first.resource_name.clone();
        let parsed = ResourceName::parse(&expected_name)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let key = UploadKey::from_resource(&parsed)?;

        if key.digest.is_empty() {
            return handle_empty_digest_write(first, stream, &expected_name, key.compression).await;
        }

        let entry = self.uploads.get_or_insert_with(&key, || {
            self.spawn_upload_entry(key.digest, key.compression)
        });
        let Ok(_writer_guard) = entry.active_writer.try_lock() else {
            return Err(Status::aborted(
                "concurrent Write in progress for the same upload resource",
            ));
        };
        if let Some(err) = entry.take_terminal_error() {
            self.uploads.remove(&key);
            return Err(err);
        }

        let Some(tx) = entry.chunk_tx.lock().clone() else {
            return Err(Status::failed_precondition(
                "upload has already been finalized",
            ));
        };

        let (expected_offset, finished) = match drive_upload_stream(
            &entry,
            &self.budget,
            &tx,
            first,
            &mut stream,
            &expected_name,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                if entry.take_terminal_error().is_some() || entry.chunk_tx.lock().is_none() {
                    self.uploads.remove(&key);
                }
                return Err(err);
            }
        };

        if !finished {
            if let Some(err) = entry.take_terminal_error() {
                self.uploads.remove(&key);
                return Err(err);
            }
            return Ok(Response::new(WriteResponse {
                committed_size: expected_offset,
            }));
        }

        drop(tx);
        finalize_upload(&self.uploads, &key, &entry, expected_offset).await
    }

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        let req = request.into_inner();
        let parsed = ResourceName::parse(&req.resource_name)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let key = UploadKey::from_resource(&parsed)?;

        if let Some(entry) = self.uploads.get(&key) {
            if let Some(err) = entry.take_terminal_error() {
                self.uploads.remove(&key);
                return Err(err);
            }
            return Ok(Response::new(QueryWriteStatusResponse {
                committed_size: entry.committed_size(),
                complete: false,
            }));
        }

        if key.digest.is_empty() {
            return Ok(Response::new(QueryWriteStatusResponse {
                committed_size: 0,
                complete: true,
            }));
        }

        let mut missing = Vec::new();
        self.store
            .find_missing(&[key.digest], &mut missing)
            .await
            .map_err(|err| map_port_error(&err))?;

        if missing.is_empty() {
            let committed_size = i64::try_from(key.digest.size()).unwrap_or(i64::MAX);
            Ok(Response::new(QueryWriteStatusResponse {
                committed_size,
                complete: true,
            }))
        } else {
            Err(Status::not_found(format!(
                "upload `{}` not found",
                req.resource_name
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt as _;
    use gang_cas::{
        Compression, DEFAULT_ZSTD_LEVEL, ResourceName, compress_zstd, decompress_zstd_bounded,
    };
    use gang_reapi::google::bytestream::byte_stream_client::ByteStreamClient;
    use gang_reapi::google::bytestream::{QueryWriteStatusRequest, ReadRequest, WriteRequest};
    use gang_spi::{BlobStream, ByteRange, ContentStore, EngineStore, PortError};
    use gang_testkit::MemoryContentStore;
    use gang_types::{Digest, NonZeroBytes};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tonic::Code;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::{Channel, Endpoint};

    use super::{ByteStreamConfig, ByteStreamService};
    use crate::reapi_cas::InflightBlobBudget;

    async fn spawn_service(
        store: Arc<MemoryContentStore>,
        budget_bytes: u64,
        read_chunk_bytes: u64,
    ) -> (Channel, InflightBlobBudget, JoinHandle<()>) {
        let budget = InflightBlobBudget::new(
            NonZeroBytes::new(gang_types::Bytes::new(budget_bytes)).expect("nonzero budget"),
        );
        let config = ByteStreamConfig {
            read_chunk: NonZeroBytes::new(gang_types::Bytes::new(read_chunk_bytes))
                .expect("nonzero chunk"),
            max_pending_uploads: NonZeroUsize::new(16).expect("16"),
        };
        let service = ByteStreamService::new(store, budget.clone(), config);

        let (channel, handle) = serve_service(service).await;
        (channel, budget, handle)
    }

    async fn serve_service<C: ContentStore>(
        service: ByteStreamService<C>,
    ) -> (Channel, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = TcpIncoming::from(listener);

        let handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(service.into_server())
                .serve_with_incoming(incoming)
                .await;
        });

        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");

        (channel, handle)
    }

    #[derive(Debug)]
    struct BudgetedStore {
        inner: MemoryContentStore,
        budget: InflightBlobBudget,
        peak_reserved: Arc<AtomicU64>,
        decoded_chunks: Arc<AtomicU64>,
    }

    impl ContentStore for BudgetedStore {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            self.inner.find_missing(digests, missing).await
        }

        async fn read(
            &self,
            digest: Digest,
            range: ByteRange,
        ) -> Result<Option<BlobStream>, PortError> {
            self.inner.read(digest, range).await
        }

        async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
            let budget = self.budget.clone();
            let peak = Arc::clone(&self.peak_reserved);
            let chunks = Arc::clone(&self.decoded_chunks);
            let guarded = body.chunks.map(move |chunk| {
                if let Ok(ref bytes) = chunk {
                    let reserved = budget.capacity() - budget.available();
                    assert!(
                        reserved >= u64::try_from(bytes.len()).expect("length"),
                        "decoded bytes must retain their reservation"
                    );
                    assert!(
                        bytes.len() <= gang_cas::STREAM_CHUNK_BYTES,
                        "decoder output must stay bounded"
                    );
                    peak.fetch_max(reserved, Ordering::Relaxed);
                    chunks.fetch_add(1, Ordering::Relaxed);
                }
                chunk
            });
            self.inner
                .write(
                    digest,
                    BlobStream {
                        size: body.size,
                        chunks: Box::pin(guarded),
                    },
                )
                .await
        }

        fn usage(&self) -> Option<EngineStore> {
            None
        }
    }

    #[tokio::test]
    async fn concurrent_compressed_writes_account_for_expansion() {
        let budget = InflightBlobBudget::new(NonZeroBytes::kib(64));
        let store = Arc::new(BudgetedStore {
            inner: MemoryContentStore::new(),
            budget: budget.clone(),
            peak_reserved: Arc::new(AtomicU64::new(0)),
            decoded_chunks: Arc::new(AtomicU64::new(0)),
        });
        let service = ByteStreamService::with_defaults(Arc::clone(&store), budget.clone());
        let (channel, handle) = serve_service(service).await;
        let upload = |id: &'static str, value: u8| {
            let channel = channel.clone();
            async move {
                let payload = Bytes::from(vec![value; 512 * 1024]);
                let digest = Digest::from_bytes(&payload);
                let compressed = compress_zstd(&payload, DEFAULT_ZSTD_LEVEL).expect("compress");
                let mut client = ByteStreamClient::new(channel);
                let result = client
                    .write(futures::stream::iter([WriteRequest {
                        resource_name: ResourceName::upload("", id, Compression::Zstd, digest)
                            .expect("name")
                            .to_string(),
                        write_offset: 0,
                        finish_write: true,
                        data: compressed.clone(),
                    }]))
                    .await
                    .expect("compressed write")
                    .into_inner();
                assert_eq!(
                    result.committed_size,
                    i64::try_from(compressed.len()).expect("wire size")
                );
                (digest, payload)
            }
        };
        let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(upload("first", 41), upload("second", 42))
        })
        .await
        .expect("concurrent uploads must not deadlock");
        assert_eq!(store.inner.get_bytes(first.0), Some(first.1));
        assert_eq!(store.inner.get_bytes(second.0), Some(second.1));
        assert_eq!(
            store.peak_reserved.load(Ordering::Relaxed),
            budget.capacity()
        );
        assert!(store.decoded_chunks.load(Ordering::Relaxed) >= 16);
        assert_eq!(budget.available(), budget.capacity());
        handle.abort();
    }

    #[tokio::test]
    async fn empty_compressed_upload_rejects_oversized_frame() {
        let store = Arc::new(MemoryContentStore::new());
        let (channel, budget, handle) = spawn_service(store, 1024, 64).await;
        let mut client = ByteStreamClient::new(channel);
        let name = ResourceName::upload("", "empty-large", Compression::Zstd, Digest::EMPTY)
            .expect("name")
            .to_string();
        let error = client
            .write(futures::stream::iter(vec![WriteRequest {
                resource_name: name,
                write_offset: 0,
                finish_write: true,
                data: Bytes::from(vec![0; 64 * 1024 + 1]),
            }]))
            .await
            .expect_err("oversized compressed empty blob");
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(budget.available(), budget.capacity());
        handle.abort();
    }

    #[tokio::test]
    async fn read_raw_zstd_ranges_and_empty_digest() {
        let store = Arc::new(MemoryContentStore::new());
        let payload = Bytes::from(b"0123456789abcdefghijklmnopqrstuvwxyz".repeat(10));
        let digest = store.insert(payload.clone());
        let (channel, budget, handle) = spawn_service(Arc::clone(&store), 4096, 32).await;
        let mut client = ByteStreamClient::new(channel);

        // Full raw read arrives in <= 32-byte frames.
        let mut stream = client
            .read(ReadRequest {
                resource_name: ResourceName::blob("", Compression::Identity, digest)
                    .expect("name")
                    .to_string(),
                read_offset: 0,
                read_limit: 0,
            })
            .await
            .expect("read")
            .into_inner();
        let mut collected = Vec::new();
        let mut frames = 0_usize;
        while let Some(frame) = stream.message().await.expect("frame") {
            assert!(frame.data.len() <= 32);
            collected.extend_from_slice(&frame.data);
            frames += 1;
        }
        assert!(frames > 1);
        assert_eq!(Bytes::from(collected), payload);

        // Offset + limit slice.
        let mut sliced = client
            .read(ReadRequest {
                resource_name: ResourceName::blob("inst", Compression::Identity, digest)
                    .expect("name")
                    .to_string(),
                read_offset: 10,
                read_limit: 16,
            })
            .await
            .expect("sliced read")
            .into_inner();
        let mut slice_buf = Vec::new();
        while let Some(frame) = sliced.message().await.expect("frame") {
            slice_buf.extend_from_slice(&frame.data);
        }
        assert_eq!(Bytes::from(slice_buf), payload.slice(10..26));

        // offset == size -> empty stream.
        let mut at_end = client
            .read(ReadRequest {
                resource_name: ResourceName::blob("", Compression::Identity, digest)
                    .expect("name")
                    .to_string(),
                read_offset: i64::try_from(payload.len()).expect("len"),
                read_limit: 0,
            })
            .await
            .expect("at end")
            .into_inner();
        assert!(at_end.message().await.expect("eof").is_none());

        // offset > size -> OUT_OF_RANGE.
        let oob = client
            .read(ReadRequest {
                resource_name: ResourceName::blob("", Compression::Identity, digest)
                    .expect("name")
                    .to_string(),
                read_offset: i64::try_from(payload.len() + 1).expect("len"),
                read_limit: 0,
            })
            .await
            .expect_err("oob");
        assert_eq!(oob.code(), Code::OutOfRange);

        // ZSTD compressed read round-trips cleanly.
        let mut zstd_stream = client
            .read(ReadRequest {
                resource_name: ResourceName::blob("", Compression::Zstd, digest)
                    .expect("name")
                    .to_string(),
                read_offset: 0,
                read_limit: 0,
            })
            .await
            .expect("zstd read")
            .into_inner();
        let mut compressed = Vec::new();
        while let Some(frame) = zstd_stream.message().await.expect("frame") {
            compressed.extend_from_slice(&frame.data);
        }
        let decoded = decompress_zstd_bounded(digest, &compressed).expect("decompress read stream");
        assert_eq!(decoded, payload);

        // Digest::EMPTY short-circuits without hitting store.
        store.reset_calls();
        let mut empty_stream = client
            .read(ReadRequest {
                resource_name: ResourceName::blob("", Compression::Identity, Digest::EMPTY)
                    .expect("name")
                    .to_string(),
                read_offset: 0,
                read_limit: 0,
            })
            .await
            .expect("empty read")
            .into_inner();
        assert!(empty_stream.message().await.expect("eof").is_none());
        assert_eq!(store.read_calls(), 0);
        assert_eq!(budget.available(), budget.capacity());

        handle.abort();
    }

    #[tokio::test]
    async fn write_raw_zstd_resumable_and_rejects_bomb_and_bad_offsets() {
        let store = Arc::new(MemoryContentStore::new());
        let (channel, _budget, handle) = spawn_service(Arc::clone(&store), 4096, 64).await;
        let mut client = ByteStreamClient::new(channel);

        // 1. Unknown upload -> QueryWriteStatus returns NOT_FOUND.
        let payload = Bytes::from_static(b"hello-resumable-bytestream-payload");
        let digest = Digest::from_bytes(&payload);
        let upload_name = ResourceName::upload("team", "u-1", Compression::Identity, digest)
            .expect("name")
            .to_string();
        let missing_status = client
            .query_write_status(QueryWriteStatusRequest {
                resource_name: upload_name.clone(),
            })
            .await
            .expect_err("unknown upload");
        assert_eq!(missing_status.code(), Code::NotFound);

        // 2. Partial write (finish_write = false) -> QueryWriteStatus -> Resume -> Complete.
        let part1 = payload.slice(..12);
        let part2 = payload.slice(12..);
        let partial_resp = client
            .write(futures::stream::iter(vec![WriteRequest {
                resource_name: upload_name.clone(),
                write_offset: 0,
                finish_write: false,
                data: part1.clone(),
            }]))
            .await
            .expect("partial write")
            .into_inner();
        assert_eq!(partial_resp.committed_size, 12);
        assert!(!store.contains(digest), "not visible before finish_write");

        let status = client
            .query_write_status(QueryWriteStatusRequest {
                resource_name: upload_name.clone(),
            })
            .await
            .expect("query status")
            .into_inner();
        assert_eq!(status.committed_size, 12);
        assert!(!status.complete);

        // Bad resume offset -> INVALID_ARGUMENT.
        let bad_resume = client
            .write(futures::stream::iter(vec![WriteRequest {
                resource_name: upload_name.clone(),
                write_offset: 5,
                finish_write: true,
                data: part2.clone(),
            }]))
            .await
            .expect_err("wrong resume offset");
        assert_eq!(bad_resume.code(), Code::InvalidArgument);

        // Valid resume finishes the blob.
        let final_resp = client
            .write(futures::stream::iter(vec![WriteRequest {
                resource_name: upload_name.clone(),
                write_offset: 12,
                finish_write: true,
                data: part2,
            }]))
            .await
            .expect("resumed write")
            .into_inner();
        assert_eq!(
            final_resp.committed_size,
            i64::try_from(payload.len()).expect("len")
        );
        assert_eq!(store.get_bytes(digest), Some(payload.clone()));

        // QueryWriteStatus on finalized blob already in CAS returns complete = true.
        let done_status = client
            .query_write_status(QueryWriteStatusRequest {
                resource_name: ResourceName::upload(
                    "team",
                    "another-id",
                    Compression::Identity,
                    digest,
                )
                .expect("name")
                .to_string(),
            })
            .await
            .expect("complete status")
            .into_inner();
        assert!(done_status.complete);
        assert_eq!(
            done_status.committed_size,
            i64::try_from(payload.len()).expect("len")
        );

        handle.abort();
    }

    #[tokio::test]
    async fn write_zstd_rejects_bomb_and_short_circuits_empty() {
        let store = Arc::new(MemoryContentStore::new());
        let (channel, budget, handle) = spawn_service(Arc::clone(&store), 4096, 64).await;
        let mut client = ByteStreamClient::new(channel);

        // ZSTD upload works, and ZSTD size+1 bomb is rejected with INVALID_ARGUMENT.
        let zstd_data = Bytes::from(b"compressed-upload-data-".repeat(20));
        let zstd_digest = Digest::from_bytes(&zstd_data);
        let zstd_wire = compress_zstd(&zstd_data, DEFAULT_ZSTD_LEVEL).expect("compress");
        let zstd_name = ResourceName::upload("", "u-zstd", Compression::Zstd, zstd_digest)
            .expect("name")
            .to_string();
        let zstd_len = i64::try_from(zstd_wire.len()).expect("len");
        let zstd_resp = client
            .write(futures::stream::iter(vec![WriteRequest {
                resource_name: zstd_name,
                write_offset: 0,
                finish_write: true,
                data: zstd_wire,
            }]))
            .await
            .expect("zstd write")
            .into_inner();
        assert_eq!(zstd_resp.committed_size, zstd_len);
        assert_eq!(store.get_bytes(zstd_digest), Some(zstd_data));

        let bomb_raw = vec![0_u8; 256 * 1024];
        let bomb_wire = compress_zstd(&bomb_raw, DEFAULT_ZSTD_LEVEL).expect("compress bomb");
        let claimed = Digest::new([0x33_u8; 32], 64);
        let bomb_name = ResourceName::upload("", "u-bomb", Compression::Zstd, claimed)
            .expect("name")
            .to_string();
        let bomb_err = client
            .write(futures::stream::iter(vec![WriteRequest {
                resource_name: bomb_name,
                write_offset: 0,
                finish_write: true,
                data: bomb_wire,
            }]))
            .await
            .expect_err("zstd bomb rejected");
        assert_eq!(bomb_err.code(), Code::InvalidArgument);
        assert!(!store.contains(claimed));
        assert_eq!(budget.available(), budget.capacity());

        // 4. Digest::EMPTY write short-circuits.
        store.reset_calls();
        let empty_name = ResourceName::upload("", "u-empty", Compression::Identity, Digest::EMPTY)
            .expect("name")
            .to_string();
        let empty_resp = client
            .write(futures::stream::iter(vec![WriteRequest {
                resource_name: empty_name,
                write_offset: 0,
                finish_write: true,
                data: Bytes::new(),
            }]))
            .await
            .expect("empty write")
            .into_inner();
        assert_eq!(empty_resp.committed_size, 0);
        assert_eq!(store.write_calls(), 0);

        handle.abort();
    }

    #[tokio::test]
    async fn bytestream_read_respects_inflight_budget_backpressure() {
        let store = Arc::new(MemoryContentStore::new());
        let payload = Bytes::from(vec![0x55_u8; 64]);
        let digest = store.insert(payload.clone());
        let (channel, budget, handle) = spawn_service(Arc::clone(&store), 32, 32).await;
        let mut client = ByteStreamClient::new(channel);

        // Hold all 32 bytes of the budget externally -> Read cannot yield its first 32-byte chunk
        // until the permit is released.
        let held = budget.acquire(32).await.expect("acquire 32");
        let mut stream = client
            .read(ReadRequest {
                resource_name: ResourceName::blob("", Compression::Identity, digest)
                    .expect("name")
                    .to_string(),
                read_offset: 0,
                read_limit: 0,
            })
            .await
            .expect("response header")
            .into_inner();

        let first_frame_fut = tokio::spawn(async move {
            let mut out = Vec::new();
            while let Some(frame) = stream.message().await.expect("frame") {
                out.extend_from_slice(&frame.data);
            }
            Bytes::from(out)
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !first_frame_fut.is_finished(),
            "streaming read should wait for in-flight budget"
        );

        drop(held);
        let received = first_frame_fut.await.expect("join");
        assert_eq!(received, payload);
        assert_eq!(budget.available(), 32);

        // Verify direct service ReadStream also drops permits on cancellation.
        let svc = ByteStreamService::new(
            store,
            budget.clone(),
            ByteStreamConfig {
                read_chunk: NonZeroBytes::new(gang_types::Bytes::new(16)).expect("16"),
                max_pending_uploads: NonZeroUsize::new(8).expect("8"),
            },
        );
        let mut direct = <ByteStreamService<MemoryContentStore> as gang_reapi::google::bytestream::byte_stream_server::ByteStream>::read(
            &svc,
            tonic::Request::new(ReadRequest {
                resource_name: ResourceName::blob("", Compression::Identity, digest)
                    .expect("name")
                    .to_string(),
                read_offset: 0,
                read_limit: 0,
            }),
        )
        .await
        .expect("direct read")
        .into_inner();
        let chunk = direct.next().await.expect("first chunk").expect("ok");
        assert_eq!(chunk.data.len(), 16);
        assert_eq!(budget.available(), 16);
        drop(direct);
        assert_eq!(budget.available(), 32);

        handle.abort();
    }

    #[tokio::test]
    async fn bytestream_write_respects_inflight_budget_backpressure() {
        let store = Arc::new(MemoryContentStore::new());
        let payload = Bytes::from(vec![0x77_u8; 48]);
        let digest = Digest::from_bytes(&payload);
        let (channel, budget, handle) = spawn_service(Arc::clone(&store), 32, 32).await;
        let mut client = ByteStreamClient::new(channel);

        let held = budget.acquire(32).await.expect("acquire full budget");
        let upload_name = ResourceName::upload("", "u-bp", Compression::Identity, digest)
            .expect("name")
            .to_string();
        let write_task = tokio::spawn(async move {
            client
                .write(futures::stream::iter(vec![WriteRequest {
                    resource_name: upload_name,
                    write_offset: 0,
                    finish_write: true,
                    data: payload,
                }]))
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !write_task.is_finished(),
            "streaming write should wait for in-flight budget"
        );

        drop(held);
        let resp = write_task
            .await
            .expect("join")
            .expect("write succeeds")
            .into_inner();
        assert_eq!(resp.committed_size, 48);
        assert!(store.contains(digest));
        assert_eq!(budget.available(), 32);

        handle.abort();
    }
}
