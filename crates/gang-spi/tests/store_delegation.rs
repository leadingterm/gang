//! Macro delegation tests for [`ContentStore`] and [`ActionStore`] using the two-argument
//! `__gang_delegate_*!` arm inside a module with no local `bytes` or `Digest` imports.

/// Leaf store implementations for the two variants; keeps `bytes` and `Digest` out of `delegated`.
mod fixtures {
    use std::collections::HashMap;

    use bytes::Bytes;
    use gang_spi::{
        ActionStore, BlobStream, ByteRange, ContentStore, EngineStore, PortError, StoreKind,
    };
    use gang_types::Digest;
    use parking_lot::Mutex;

    /// Content store fixture tagged with a distinct [`EngineStore`] usage value.
    #[derive(Debug)]
    pub struct TaggedContent {
        used: Option<u64>,
        blobs: Mutex<HashMap<Digest, Bytes>>,
    }

    impl TaggedContent {
        pub fn new(used: Option<u64>) -> Self {
            Self {
                used,
                blobs: Mutex::new(HashMap::new()),
            }
        }
    }

    impl ContentStore for TaggedContent {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            missing.clear();
            let blobs = self.blobs.lock();
            for &digest in digests {
                if !digest.is_empty() && !blobs.contains_key(&digest) {
                    missing.push(digest);
                }
            }
            Ok(())
        }

        async fn read(
            &self,
            digest: Digest,
            _range: ByteRange,
        ) -> Result<Option<BlobStream>, PortError> {
            let entry = self.blobs.lock().get(&digest).cloned();
            Ok(entry.map(BlobStream::from_bytes))
        }

        async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
            let bytes = body.collect().await?;
            self.blobs.lock().insert(digest, bytes);
            Ok(())
        }

        fn usage(&self) -> Option<EngineStore> {
            self.used.map(|used| EngineStore {
                store: StoreKind::Content,
                used,
                budget: 1024,
                evicted: 0,
            })
        }
    }

    /// Action store fixture tagged with a distinct [`EngineStore`] usage value.
    #[derive(Debug)]
    pub struct TaggedAction {
        used: Option<u64>,
        entries: Mutex<HashMap<Digest, Bytes>>,
    }

    impl TaggedAction {
        pub fn new(used: Option<u64>) -> Self {
            Self {
                used,
                entries: Mutex::new(HashMap::new()),
            }
        }
    }

    impl ActionStore for TaggedAction {
        async fn get(&self, action: Digest) -> Result<Option<Bytes>, PortError> {
            Ok(self.entries.lock().get(&action).cloned())
        }

        async fn put(&self, action: Digest, result: Bytes) -> Result<(), PortError> {
            self.entries.lock().insert(action, result);
            Ok(())
        }

        fn usage(&self) -> Option<EngineStore> {
            self.used.map(|used| EngineStore {
                store: StoreKind::Action,
                used,
                budget: 2048,
                evicted: 0,
            })
        }
    }
}

/// Isolated module with NO `bytes` or `Digest` imports, proving `__gang_delegate_ContentStore!`
/// and `__gang_delegate_ActionStore!` resolve all signature types through `gang_spi::__private`.
mod delegated {
    use super::fixtures::{TaggedAction, TaggedContent};

    /// Two-variant dispatch enum for [`ContentStore`].
    #[derive(Debug)]
    pub enum DispatchContent {
        Alpha(TaggedContent),
        Beta(TaggedContent),
    }

    gang_spi::__gang_delegate_ContentStore!(DispatchContent { Alpha, Beta });

    /// Two-variant dispatch enum for [`ActionStore`].
    #[derive(Debug)]
    pub enum DispatchAction {
        Alpha(TaggedAction),
        Beta(TaggedAction),
    }

    gang_spi::__gang_delegate_ActionStore!(DispatchAction { Alpha, Beta });
}

use std::sync::Arc;

use bytes::Bytes;
use gang_spi::{ActionStore, BlobStream, ByteRange, ContentStore, StoreKind};
use gang_testkit::TestResult;
use gang_types::Digest;

use delegated::{DispatchAction, DispatchContent};
use fixtures::{TaggedAction, TaggedContent};

#[tokio::test]
async fn content_store_delegation_dispatches_to_selected_variant() -> TestResult {
    let alpha = DispatchContent::Alpha(TaggedContent::new(Some(11)));
    let beta = DispatchContent::Beta(TaggedContent::new(None));

    let payload_a = Bytes::from_static(b"alpha-blob");
    let digest_a = Digest::from_bytes(&payload_a);
    let payload_b = Bytes::from_static(b"beta-blob");
    let digest_b = Digest::from_bytes(&payload_b);

    alpha
        .write(digest_a, BlobStream::from_bytes(payload_a.clone()))
        .await?;
    beta.write(digest_b, BlobStream::from_bytes(payload_b.clone()))
        .await?;

    let mut missing = Vec::new();
    alpha
        .find_missing(&[digest_a, digest_b], &mut missing)
        .await?;
    assert_eq!(missing, vec![digest_b]);

    beta.find_missing(&[digest_a, digest_b], &mut missing)
        .await?;
    assert_eq!(missing, vec![digest_a]);

    let read_a = alpha
        .read(digest_a, ByteRange::FULL)
        .await?
        .ok_or("alpha must have digest_a")?;
    assert_eq!(read_a.collect().await?, payload_a);
    assert!(alpha.read(digest_b, ByteRange::FULL).await?.is_none());

    let read_b = beta
        .read(digest_b, ByteRange::FULL)
        .await?
        .ok_or("beta must have digest_b")?;
    assert_eq!(read_b.collect().await?, payload_b);
    assert!(beta.read(digest_a, ByteRange::FULL).await?.is_none());

    let alpha_usage = alpha.usage().ok_or("alpha reports usage")?;
    assert_eq!(alpha_usage.store, StoreKind::Content);
    assert_eq!(alpha_usage.used, 11);
    assert_eq!(beta.usage(), None);

    let shared = Arc::new(alpha);
    let read_shared = shared
        .read(digest_a, ByteRange::FULL)
        .await?
        .ok_or("Arc<DispatchContent> delegates read")?;
    assert_eq!(read_shared.collect().await?, payload_a);
    Ok(())
}

#[tokio::test]
async fn action_store_delegation_dispatches_to_selected_variant() -> TestResult {
    let alpha = DispatchAction::Alpha(TaggedAction::new(Some(22)));
    let beta = DispatchAction::Beta(TaggedAction::new(None));

    let action_a = Digest::from_bytes(b"action-a");
    let result_a = Bytes::from_static(b"result-a");
    let action_b = Digest::from_bytes(b"action-b");
    let result_b = Bytes::from_static(b"result-b");

    alpha.put(action_a, result_a.clone()).await?;
    beta.put(action_b, result_b.clone()).await?;

    assert_eq!(alpha.get(action_a).await?, Some(result_a.clone()));
    assert_eq!(alpha.get(action_b).await?, None);

    assert_eq!(beta.get(action_b).await?, Some(result_b));
    assert_eq!(beta.get(action_a).await?, None);

    let alpha_usage = alpha.usage().ok_or("alpha reports usage")?;
    assert_eq!(alpha_usage.store, StoreKind::Action);
    assert_eq!(alpha_usage.used, 22);
    assert_eq!(beta.usage(), None);

    let shared = Arc::new(alpha);
    assert_eq!(shared.get(action_a).await?, Some(result_a));
    Ok(())
}
