//! Fenced persistence of scheduler history and source activity profiles.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{
    ApplyRecords, ExpiringRecordPort, LearnedHistory, Lease, MAX_RECORD_BODY_BYTES,
    MAX_RECORD_BODY_BYTES_PER_BATCH, MAX_RECORD_CHANGES, PortError, RecordBatchSeq, RecordChange,
    RecordId, RecordKey, RecordKind, RecordVersion,
};
use ring::digest::{Context, SHA256, digest};

const BODY_LIMIT: usize = MAX_RECORD_BODY_BYTES;
const BATCH_LIMIT: usize = MAX_RECORD_CHANGES;
const BATCH_BYTES: usize = MAX_RECORD_BODY_BYTES_PER_BATCH;
const HEADER: usize = 56;
const MAGIC: &[u8; 4] = b"GH01";

type DesiredHistory<'a> = BTreeMap<RecordKey, Option<(&'a LearnedHistory, [u8; 32])>>;

#[derive(Debug, Clone)]
struct Head {
    generation: u64,
    count: u32,
    hash: [u8; 32],
    expires_at: Timestamp,
}

impl Head {
    fn encode(&self, body: &[u8]) -> Result<Vec<u8>, HistoryError> {
        let mut header = Vec::with_capacity(if self.count != 0 {
            HEADER
        } else {
            HEADER + body.len()
        });
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&self.generation.to_be_bytes());
        header.extend_from_slice(
            &u64::try_from(body.len())
                .map_err(|_| HistoryError::Malformed)?
                .to_be_bytes(),
        );
        header.extend_from_slice(&self.hash);
        header.extend_from_slice(&self.count.to_be_bytes());
        if self.count == 0 {
            header.extend_from_slice(body);
        }
        Ok(header)
    }
}

/// Failure to read or persist learned history.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// The repository could not complete an operation.
    #[error("history storage: {0}")]
    Storage(#[from] PortError),
    /// The write was fenced; this does not establish that another leader holds the lease.
    #[error("learned history writes are fenced")]
    Fenced,
    /// A source record is invalid or exceeds the configured encoding bound.
    #[error("invalid or oversized learned history")]
    Malformed,
}

/// Tracks only committed history fingerprints and chunk metadata between snapshots.
#[derive(Debug)]
pub struct LearnedHistoryStore {
    max_record_bytes: NonZeroUsize,
    heads: BTreeMap<RecordKey, Head>,
    cleanup: BTreeSet<RecordKey>,
    sequence: u64,
    uncertain: bool,
    prepared: BTreeMap<RecordKey, ([u8; 32], Timestamp)>,
    preserved: BTreeSet<RecordKey>,
    canonical_pending: bool,
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    let mut result = [0; 32];
    result.copy_from_slice(digest(&SHA256, bytes).as_ref());
    result
}

fn chunk_key(key: RecordKey, generation: u64, hash: &[u8; 32], index: u32) -> RecordKey {
    let mut context = Context::new(&SHA256);
    context.update(b"gang-learned-history-chunk-v1\0");
    context.update(key.kind.as_str().as_bytes());
    context.update(key.id.as_bytes());
    context.update(&generation.to_be_bytes());
    context.update(hash);
    context.update(&index.to_be_bytes());
    let mut id = [0; 32];
    id.copy_from_slice(context.finish().as_ref());
    RecordKey {
        kind: RecordKind::HistoryChunk,
        id: RecordId::new(id),
    }
}

fn chunks(key: RecordKey, head: &Head) -> impl Iterator<Item = RecordKey> + '_ {
    (0..head.count).map(move |index| chunk_key(key, head.generation, &head.hash, index))
}

impl LearnedHistoryStore {
    /// Creates a store with a bound on each encoded source record.
    ///
    /// Use exactly one store for each newly acquired lease epoch, including all load retries.
    #[must_use]
    pub fn new(max_record_bytes: NonZeroUsize) -> Self {
        Self {
            max_record_bytes,
            heads: BTreeMap::new(),
            cleanup: BTreeSet::new(),
            sequence: 0,
            uncertain: false,
            prepared: BTreeMap::new(),
            preserved: BTreeSet::new(),
            canonical_pending: false,
        }
    }

    async fn apply<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
        changes: &[RecordChange<'_>],
    ) -> Result<(), HistoryError> {
        let version = RecordVersion::fresh().map_err(PortError::backend)?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(HistoryError::Malformed)?;
        self.uncertain = true;
        match repository
            .apply(
                lease,
                RecordBatchSeq::new(self.sequence),
                version,
                now,
                changes,
            )
            .await?
        {
            ApplyRecords::Applied => {
                self.uncertain = false;
                Ok(())
            }
            ApplyRecords::Fenced => Err(HistoryError::Fenced),
        }
    }

    async fn clean<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
    ) -> Result<(), HistoryError> {
        while !self.cleanup.is_empty() {
            let keys: Vec<_> = self.cleanup.iter().copied().take(BATCH_LIMIT).collect();
            let changes: Vec<_> = keys
                .iter()
                .map(|&key| RecordChange::Delete { key })
                .collect();
            self.apply(repository, lease, now, &changes).await?;
            for key in keys {
                self.cleanup.remove(&key);
            }
        }
        Ok(())
    }

    /// Loads live history and removes malformed heads and orphan chunks under the lease.
    ///
    /// # Errors
    /// Returns a storage error or lease loss if loading or cleanup cannot complete.
    pub async fn load<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), HistoryError> {
        self.reconcile(repository, lease, now, Some(into)).await
    }

    /// Reads complete valid histories without claiming authority or deleting another holder's
    /// staged chunks. Heavy import finishes before a prepared contender requests handover.
    pub(crate) async fn prepare<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), HistoryError> {
        self.scan_heads(repository, now, Some(into)).await?;
        self.cleanup.clear();
        self.uncertain = false;
        Ok(())
    }

    /// Stores fingerprints of the native post-import representation, not a second body copy.
    pub(crate) fn prepared(&mut self, records: &[LearnedHistory]) {
        self.prepared.clear();
        self.prepared.extend(
            records
                .iter()
                .map(|record| (record.key, (hash(&record.body), record.expires_at))),
        );
        self.canonical_pending = true;
    }

    /// A bounded authority check only; canonical reconciliation is persistence work, not
    /// a destructive reload of the running engine during promotion.
    pub(crate) async fn activate<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
    ) -> Result<(), HistoryError> {
        self.apply(repository, lease, now, &[]).await?;
        self.canonical_pending = true;
        Ok(())
    }

    async fn scan_heads<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        now: Timestamp,
        mut into: Option<&mut Vec<LearnedHistory>>,
    ) -> Result<(), HistoryError> {
        self.heads.clear();
        self.cleanup.clear();
        if let Some(records) = into.as_mut() {
            records.clear();
        }
        self.uncertain = true;
        let mut page = Vec::new();
        let limit = NonZeroUsize::MIN.saturating_add(BATCH_LIMIT - 1);
        for kind in [
            RecordKind::ResourceHistory,
            RecordKind::DurationHistory,
            RecordKind::SourceProfile,
        ] {
            let mut after = None;
            loop {
                let more = repository
                    .records(kind, after, limit, now, &mut page)
                    .await?;
                after = page.last().map(|(id, _)| *id);
                for (id, record) in page.drain(..) {
                    let key = RecordKey { kind, id };
                    if let Some((head, body)) =
                        self.decode_latest(repository, key, record, now).await?
                    {
                        if let Some(records) = into.as_mut() {
                            records.push(LearnedHistory {
                                key,
                                body: body.into(),
                                expires_at: head.expires_at,
                            });
                        }
                        self.heads.insert(key, head);
                    } else {
                        self.cleanup.insert(key);
                    }
                }
                if !more {
                    break;
                }
                if after.is_none() {
                    return Err(HistoryError::Malformed);
                }
            }
        }
        Ok(())
    }

    async fn reconcile<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
        into: Option<&mut Vec<LearnedHistory>>,
    ) -> Result<(), HistoryError> {
        self.apply(repository, lease, now, &[]).await?;
        self.scan_heads(repository, now, into).await?;
        let referenced: BTreeSet<_> = self
            .heads
            .iter()
            .flat_map(|(&key, head)| chunks(key, head))
            .collect();
        self.clean(repository, lease, now).await?;
        self.uncertain = true;
        let mut page = Vec::new();
        let limit = NonZeroUsize::MIN.saturating_add(BATCH_LIMIT - 1);
        let mut after = None;
        loop {
            let more = repository
                .records(RecordKind::HistoryChunk, after, limit, now, &mut page)
                .await?;
            for (id, _) in &page {
                let key = RecordKey {
                    kind: RecordKind::HistoryChunk,
                    id: *id,
                };
                if !referenced.contains(&key) {
                    self.cleanup.insert(key);
                }
            }
            after = page.last().map(|(id, _)| *id);
            self.clean(repository, lease, now).await?;
            self.uncertain = true;
            if !more {
                break;
            }
            if after.is_none() {
                return Err(HistoryError::Malformed);
            }
        }
        self.uncertain = false;
        Ok(())
    }

    async fn decode_latest<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        key: RecordKey,
        mut record: gang_spi::Record,
        now: Timestamp,
    ) -> Result<Option<(Head, Vec<u8>)>, HistoryError> {
        loop {
            let version = record.version;
            if let Some(decoded) = self.decode(repository, key, record, now).await? {
                return Ok(Some(decoded));
            }
            // A passive reader may race the old writer replacing a head and cleaning its
            // previous chunks. Retry the new incarnation, never mislabel it as malformed.
            match repository.get(&key, now).await? {
                Some(current) if current.version != version => record = current,
                _ => return Ok(None),
            }
        }
    }

    async fn decode<R: ExpiringRecordPort>(
        &self,
        repository: &R,
        key: RecordKey,
        mut record: gang_spi::Record,
        now: Timestamp,
    ) -> Result<Option<(Head, Vec<u8>)>, HistoryError> {
        let bytes = &record.body;
        if bytes.len() < HEADER || bytes.len() > BODY_LIMIT || &bytes[..4] != MAGIC {
            return Ok(None);
        }
        let generation = u64::from_be_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| HistoryError::Malformed)?,
        );
        let len_u64 = u64::from_be_bytes(
            bytes[12..20]
                .try_into()
                .map_err(|_| HistoryError::Malformed)?,
        );
        let Ok(len) = usize::try_from(len_u64) else {
            return Ok(None);
        };
        if generation == 0 || len > self.max_record_bytes.get() {
            return Ok(None);
        }
        let mut expected = [0; 32];
        expected.copy_from_slice(&bytes[20..52]);
        let count = u32::from_be_bytes(
            bytes[52..56]
                .try_into()
                .map_err(|_| HistoryError::Malformed)?,
        );
        let expected_count = len / BODY_LIMIT + usize::from(len % BODY_LIMIT != 0);
        let body = if count == 0 {
            if len != bytes.len() - HEADER {
                return Ok(None);
            }
            drop(record.body.drain(..HEADER));
            record.body
        } else {
            if bytes.len() != HEADER
                || usize::try_from(count).ok() != Some(expected_count)
                || len <= BODY_LIMIT - HEADER
            {
                return Ok(None);
            }
            let mut body = Vec::with_capacity(len);
            for index in 0..count {
                let Some(chunk) = repository
                    .get(&chunk_key(key, generation, &expected, index), now)
                    .await?
                else {
                    return Ok(None);
                };
                let expected_len = (len - body.len()).min(BODY_LIMIT);
                if chunk.body.len() != expected_len || chunk.expires_at != record.expires_at {
                    return Ok(None);
                }
                body.extend_from_slice(&chunk.body);
            }
            body
        };
        if hash(&body) != expected {
            return Ok(None);
        }
        Ok(Some((
            Head {
                generation,
                count,
                hash: expected,
                expires_at: record.expires_at,
            },
            body,
        )))
    }

    async fn commit_heads<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
        pending: &mut Vec<(RecordKey, Vec<u8>, Head, Option<Head>)>,
    ) -> Result<(), HistoryError> {
        if pending.is_empty() {
            return Ok(());
        }
        let changes: Vec<_> = pending
            .iter()
            .map(|(key, body, head, _)| RecordChange::Put {
                key: *key,
                body,
                expires_at: head.expires_at,
            })
            .collect();
        self.apply(repository, lease, now, &changes).await?;
        for (key, _, head, old) in pending.drain(..) {
            for chunk in chunks(key, &head) {
                self.cleanup.remove(&chunk);
            }
            self.heads.insert(key, head);
            if let Some(old) = old {
                self.cleanup.extend(chunks(key, &old));
            }
        }
        self.clean(repository, lease, now).await
    }

    async fn stage_chunks<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
        record: &LearnedHistory,
        head: &Head,
    ) -> Result<(), HistoryError> {
        let parts: Vec<_> = (0..head.count)
            .zip(record.body.chunks(BODY_LIMIT))
            .collect();
        // Six full chunks fit within both the raw-byte and operation limits.
        let per_batch = BATCH_LIMIT.min(BATCH_BYTES / BODY_LIMIT);
        for batch in parts.chunks(per_batch) {
            let changes: Vec<_> = batch
                .iter()
                .map(|(index, body)| RecordChange::Put {
                    key: chunk_key(record.key, head.generation, &head.hash, *index),
                    body,
                    expires_at: head.expires_at,
                })
                .collect();
            self.apply(repository, lease, now, &changes).await?;
            self.cleanup.extend(
                batch
                    .iter()
                    .map(|(index, _)| chunk_key(record.key, head.generation, &head.hash, *index)),
            );
        }
        Ok(())
    }

    async fn reconcile_before_write<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
    ) -> Result<(), HistoryError> {
        if self.uncertain || self.canonical_pending {
            // Decode one record at a time to validate canonical heads; retain only metadata,
            // not another full learned-history dump and never mutate the running engine.
            self.reconcile(repository, lease, now, None).await?;
            if self.canonical_pending {
                self.preserved.extend(self.heads.keys().copied());
                self.preserved.extend(self.prepared.keys().copied());
                self.canonical_pending = false;
            }
        }
        Ok(())
    }

    fn desired<'a>(
        &self,
        now: Timestamp,
        records: &'a [LearnedHistory],
    ) -> Result<DesiredHistory<'a>, HistoryError> {
        let mut desired = BTreeMap::new();
        for record in records {
            if !matches!(
                record.key.kind,
                RecordKind::ResourceHistory
                    | RecordKind::DurationHistory
                    | RecordKind::SourceProfile
            ) || record.body.len() > self.max_record_bytes.get()
            {
                return Err(HistoryError::Malformed);
            }
            let entry = (record.expires_at > now).then(|| (record, hash(&record.body)));
            if desired.insert(record.key, entry).is_some() {
                return Err(HistoryError::Malformed);
            }
        }
        Ok(desired)
    }

    /// Persists a source snapshot, acknowledging each committed batch independently.
    ///
    /// # Errors
    /// Returns storage or lease errors, or `Malformed` for an invalid or oversized source record.
    pub async fn flush<R: ExpiringRecordPort>(
        &mut self,
        repository: &R,
        lease: &Lease,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), HistoryError> {
        self.reconcile_before_write(repository, lease, now).await?;
        let desired = self.desired(now, records)?;
        self.clean(repository, lease, now).await?;
        let removed: Vec<_> = self
            .heads
            .keys()
            .copied()
            .filter(|key| {
                !self.preserved.contains(key) && desired.get(key).is_none_or(Option::is_none)
            })
            .collect();
        for batch in removed.chunks(BATCH_LIMIT) {
            let changes: Vec<_> = batch
                .iter()
                .map(|&key| RecordChange::Delete { key })
                .collect();
            self.apply(repository, lease, now, &changes).await?;
            for key in batch {
                if let Some(old) = self.heads.remove(key) {
                    self.cleanup.extend(chunks(*key, &old));
                }
            }
        }
        self.clean(repository, lease, now).await?;
        let mut pending = Vec::new();
        let mut pending_bytes = 0;
        for (record, fingerprint) in desired.values().flatten() {
            let fingerprint = *fingerprint;
            if self
                .prepared
                .get(&record.key)
                .is_none_or(|&(prepared, expires)| {
                    prepared != fingerprint || expires != record.expires_at
                })
            {
                self.preserved.remove(&record.key);
            }
            if self.preserved.contains(&record.key) {
                continue;
            }
            let old = self.heads.get(&record.key).cloned();
            if old.as_ref().is_some_and(|head| {
                head.hash == fingerprint && head.expires_at == record.expires_at
            }) {
                continue;
            }
            let generation = match &old {
                Some(head) => head
                    .generation
                    .checked_add(1)
                    .ok_or(HistoryError::Malformed)?,
                None => 1,
            };
            let chunked = record.body.len() > BODY_LIMIT - HEADER;
            let count = if chunked {
                u32::try_from(
                    record.body.len() / BODY_LIMIT
                        + usize::from(record.body.len() % BODY_LIMIT != 0),
                )
                .map_err(|_| HistoryError::Malformed)?
            } else {
                0
            };
            let head = Head {
                generation,
                count,
                hash: fingerprint,
                expires_at: record.expires_at,
            };
            let header = head.encode(&record.body)?;
            if pending.len() == BATCH_LIMIT || pending_bytes + header.len() > BATCH_BYTES {
                self.commit_heads(repository, lease, now, &mut pending)
                    .await?;
                pending_bytes = 0;
            }
            if chunked {
                self.stage_chunks(repository, lease, now, record, &head)
                    .await?;
            }
            pending_bytes += header.len();
            pending.push((record.key, header, head, old));
        }
        self.commit_heads(repository, lease, now, &mut pending)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_spi::{Acquisition, ControllerId, Insert, LeaderLeasePort, Record, Remove, Replace};
    use gang_testkit::FakeRepository;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Fail one selected apply call while retaining the real in-memory repository semantics.
    struct Failing {
        inner: FakeRepository,
        calls: AtomicUsize,
        fail_at: usize,
        after_commit: bool,
        hang: bool,
    }

    impl ExpiringRecordPort for Failing {
        async fn insert(
            &self,
            records: &[(RecordKey, &[u8])],
            version: RecordVersion,
            now: Timestamp,
            expires_at: Timestamp,
        ) -> Result<Insert, PortError> {
            ExpiringRecordPort::insert(&self.inner, records, version, now, expires_at).await
        }
        async fn get(&self, key: &RecordKey, now: Timestamp) -> Result<Option<Record>, PortError> {
            self.inner.get(key, now).await
        }
        async fn replace(
            &self,
            key: &RecordKey,
            version: RecordVersion,
            body: &[u8],
            now: Timestamp,
            expires_at: Timestamp,
        ) -> Result<Replace, PortError> {
            self.inner
                .replace(key, version, body, now, expires_at)
                .await
        }
        async fn remove(
            &self,
            key: &RecordKey,
            version: RecordVersion,
            now: Timestamp,
        ) -> Result<Remove, PortError> {
            self.inner.remove(key, version, now).await
        }
        async fn records(
            &self,
            kind: RecordKind,
            after: Option<RecordId>,
            limit: NonZeroUsize,
            now: Timestamp,
            into: &mut Vec<(RecordId, Record)>,
        ) -> Result<bool, PortError> {
            self.inner.records(kind, after, limit, now, into).await
        }
        async fn apply(
            &self,
            lease: &Lease,
            sequence: RecordBatchSeq,
            version: RecordVersion,
            now: Timestamp,
            changes: &[RecordChange<'_>],
        ) -> Result<ApplyRecords, PortError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == self.fail_at {
                if self.after_commit {
                    self.inner
                        .apply(lease, sequence, version, now, changes)
                        .await?;
                    if self.hang {
                        std::future::pending::<()>().await;
                    }
                }
                return Err(PortError::unavailable("injected batch failure"));
            }
            self.inner
                .apply(lease, sequence, version, now, changes)
                .await
        }
        async fn purge(&self, now: Timestamp) -> Result<(), PortError> {
            self.inner.purge(now).await
        }
    }

    fn time(ms: u64) -> Timestamp {
        Timestamp::from_unix_millis(ms)
    }
    fn store() -> LearnedHistoryStore {
        LearnedHistoryStore::new(NonZeroUsize::new(2_000_000).unwrap())
    }
    fn entry(id: u8, size: usize, expiry: Timestamp) -> LearnedHistory {
        LearnedHistory {
            key: RecordKey {
                kind: RecordKind::ResourceHistory,
                id: RecordId::new([id; 32]),
            },
            body: vec![id; size].into(),
            expires_at: expiry,
        }
    }
    async fn lease(repository: &FakeRepository) -> Lease {
        match repository
            .acquire(
                &gang_spi::LeaseCandidate {
                    holder: ControllerId::new("history-test"),
                    priority: gang_spi::CandidatePriority::registered(
                        gang_spi::CandidateGeneration::default(),
                    ),
                },
                time(1),
                time(1),
                time(100_000),
                gang_spi::Takeover::Observe,
            )
            .await
            .unwrap()
        {
            Acquisition::Acquired(lease) => lease,
            Acquisition::Held(_) | Acquisition::Contending(_) | Acquisition::Reserved => {
                panic!("fresh repository must grant lease")
            }
        }
    }

    #[tokio::test]
    async fn large_history_replacement_and_inline_collects_chunks() {
        let repository = FakeRepository::default();
        let lease = lease(&repository).await;
        let mut first = store();
        let mut record = entry(1, 410 * 1024, time(50_000));
        first
            .flush(&repository, &lease, time(10), &[record.clone()])
            .await
            .unwrap();
        assert_eq!(repository.stored_records(), 3);
        let mut loaded = Vec::new();
        repository.release(&lease).await.unwrap();
        let lease = self::lease(&repository).await;
        let mut successor = store();
        successor
            .load(&repository, &lease, time(20), &mut loaded)
            .await
            .unwrap();
        assert_eq!(loaded, vec![record.clone()]);
        std::sync::Arc::make_mut(&mut record.body)[0] = 42;
        successor
            .flush(&repository, &lease, time(30), &[record.clone()])
            .await
            .unwrap();
        assert_eq!(repository.stored_records(), 3);
        record.body = record.body[..10].into();
        successor
            .flush(&repository, &lease, time(40), &[record.clone()])
            .await
            .unwrap();
        assert_eq!(repository.stored_records(), 1);
        successor
            .load(&repository, &lease, time(50), &mut loaded)
            .await
            .unwrap();
        assert_eq!(loaded, vec![record]);
    }

    #[tokio::test]
    async fn inline_heads_share_a_batch() {
        let repository = FakeRepository::default();
        let lease = lease(&repository).await;
        let counting = Failing {
            inner: repository,
            calls: AtomicUsize::new(0),
            fail_at: usize::MAX,
            after_commit: false,
            hang: false,
        };
        let records = [entry(1, 10, time(50_000)), entry(2, 10, time(50_000))];
        store()
            .flush(&counting, &lease, time(10), &records)
            .await
            .unwrap();
        assert_eq!(counting.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn partial_failure_acknowledges_only_committed_heads() {
        let repository = FakeRepository::default();
        let lease = lease(&repository).await;
        let failing = Failing {
            inner: repository.clone(),
            calls: AtomicUsize::new(0),
            fail_at: 1,
            after_commit: false,
            hang: false,
        };
        let records: Vec<_> = (0..100).map(|id| entry(id, 10, time(50_000))).collect();
        let mut store = store();
        assert!(matches!(
            store.flush(&failing, &lease, time(10), &records).await,
            Err(HistoryError::Storage(_))
        ));
        assert_eq!(store.heads.len(), BATCH_LIMIT);
        store
            .flush(&failing, &lease, time(20), &records)
            .await
            .unwrap();
        assert_eq!(failing.calls.load(Ordering::SeqCst), 4);
        let mut loaded = Vec::new();
        store
            .load(&repository, &lease, time(30), &mut loaded)
            .await
            .unwrap();
        assert_eq!(loaded, records);
    }

    #[tokio::test]
    async fn lost_commit_response_reloads_before_changed_snapshot() {
        let repository = FakeRepository::default();
        let lease = lease(&repository).await;
        let failing = Failing {
            inner: repository.clone(),
            calls: AtomicUsize::new(0),
            fail_at: 1,
            after_commit: true,
            hang: false,
        };
        let mut store = store();
        let mut record = entry(5, 410 * 1024, time(50_000));
        assert!(matches!(
            store
                .flush(&failing, &lease, time(10), &[record.clone()])
                .await,
            Err(HistoryError::Storage(_))
        ));
        std::sync::Arc::make_mut(&mut record.body)[0] = 99;
        store
            .flush(&failing, &lease, time(20), &[record.clone()])
            .await
            .unwrap();
        let mut loaded = Vec::new();
        store
            .load(&repository, &lease, time(30), &mut loaded)
            .await
            .unwrap();
        assert_eq!(loaded, vec![record]);
        assert_eq!(repository.stored_records(), 3);
    }

    #[tokio::test]
    async fn cancelled_delete_reloads_before_identical_reinsert() {
        let repository = FakeRepository::default();
        let lease = lease(&repository).await;
        let failing = Failing {
            inner: repository.clone(),
            calls: AtomicUsize::new(0),
            fail_at: 1,
            after_commit: true,
            hang: true,
        };
        let mut store = store();
        let record = entry(6, 10, time(50_000));
        store
            .flush(&failing, &lease, time(10), std::slice::from_ref(&record))
            .await
            .unwrap();
        {
            let mut future = Box::pin(store.flush(&failing, &lease, time(20), &[]));
            assert!(matches!(
                futures::poll!(&mut future),
                std::task::Poll::Pending
            ));
        }
        store
            .flush(&failing, &lease, time(30), std::slice::from_ref(&record))
            .await
            .unwrap();
        let mut loaded = Vec::new();
        store
            .load(&repository, &lease, time(40), &mut loaded)
            .await
            .unwrap();
        assert_eq!(loaded, vec![record]);
    }

    #[tokio::test]
    async fn expiry_is_original() {
        let repository = FakeRepository::default();
        let lease = lease(&repository).await;
        let mut store = store();
        let record = entry(3, 410 * 1024, time(100));
        store
            .flush(&repository, &lease, time(10), &[record])
            .await
            .unwrap();
        let mut loaded = Vec::new();
        store
            .load(&repository, &lease, time(100), &mut loaded)
            .await
            .unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn missing_chunk_discards_head_and_collects_remaining_chunks() {
        let repository = FakeRepository::default();
        let lease = lease(&repository).await;
        let mut store = store();
        let record = entry(4, 410 * 1024, time(50_000));
        store
            .flush(&repository, &lease, time(10), std::slice::from_ref(&record))
            .await
            .unwrap();
        store
            .apply(
                &repository,
                &lease,
                time(20),
                &[RecordChange::Delete {
                    key: chunk_key(record.key, 1, &hash(&record.body), 0),
                }],
            )
            .await
            .unwrap();
        let mut loaded = Vec::new();
        store
            .load(&repository, &lease, time(30), &mut loaded)
            .await
            .unwrap();
        assert!(loaded.is_empty());
        assert_eq!(repository.stored_records(), 0);
    }

    #[tokio::test]
    async fn prepared_snapshot_cannot_overwrite_or_resurrect_late_canonical_heads() {
        let repository = FakeRepository::default();
        let old_lease = lease(&repository).await;
        let mut old = store();
        let mut old_records = vec![
            entry(1, 410 * 1024, time(50_000)),
            entry(2, 32, time(50_000)),
        ];
        old.flush(&repository, &old_lease, time(10), &old_records)
            .await
            .unwrap();
        let mut candidate = store();
        let mut prepared = Vec::new();
        candidate
            .prepare(&repository, time(20), &mut prepared)
            .await
            .unwrap();
        // Import may migrate representation without adding any actual observations.
        std::sync::Arc::make_mut(&mut prepared[0].body)[1] = 77;
        candidate.prepared(&prepared);
        std::sync::Arc::make_mut(&mut old_records[0].body)[0] = 42;
        old_records.pop();
        old_records.push(entry(3, 410 * 1024, time(50_000)));
        old.flush(&repository, &old_lease, time(30), &old_records)
            .await
            .unwrap();
        repository.release(&old_lease).await.unwrap();
        let lease = self::lease(&repository).await;
        candidate
            .activate(&repository, &lease, time(40))
            .await
            .unwrap();
        candidate
            .flush(&repository, &lease, time(50), &prepared)
            .await
            .unwrap();
        let mut observer = store();
        let mut canonical = Vec::new();
        observer
            .prepare(&repository, time(60), &mut canonical)
            .await
            .unwrap();
        assert_eq!(
            canonical, old_records,
            "late updates and new keys win; deleted keys stay deleted"
        );
        assert_eq!(
            prepared[0].body[1], 77,
            "reconciliation does not mutate the running snapshot"
        );
        candidate
            .flush(&repository, &lease, time(65), &prepared)
            .await
            .unwrap();
        observer
            .prepare(&repository, time(66), &mut canonical)
            .await
            .unwrap();
        assert_eq!(
            canonical, old_records,
            "unchanged protection survives later periodic flushes"
        );
        // Actual new candidate learning may replace its key, but not delete unknown old keys.
        prepared.truncate(1);
        std::sync::Arc::make_mut(&mut prepared[0].body)[0] = 91;
        prepared[0].expires_at = time(60_000);
        candidate
            .flush(&repository, &lease, time(70), &prepared)
            .await
            .unwrap();
        observer
            .prepare(&repository, time(80), &mut canonical)
            .await
            .unwrap();
        assert_eq!(canonical.len(), 2);
        assert_eq!(&canonical[0], &prepared[0]);
        assert_eq!(&canonical[1], &old_records[1]);
        candidate
            .flush(&repository, &lease, time(90), &[])
            .await
            .unwrap();
        observer
            .prepare(&repository, time(100), &mut canonical)
            .await
            .unwrap();
        assert_eq!(canonical.as_slice(), std::slice::from_ref(&old_records[1]));
    }
}
