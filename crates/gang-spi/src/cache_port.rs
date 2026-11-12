//! Cache: where the content-addressable and action stores live.

use crate::{PortError, StoreAccess, port};

/// The cache stores, as `NativeLink` store specifications.
#[port]
pub trait CacheStorePort: Send + Sync {
    /// The store that holds content-addressable blobs.
    ///
    /// # Errors
    ///
    /// The cache's own error when the store cannot be described.
    fn content_store(&self) -> Result<RenderedStoreSpec, PortError>;

    /// The store that holds action results.
    ///
    /// # Errors
    ///
    /// The cache's own error when the store cannot be described.
    fn action_store(&self) -> Result<RenderedStoreSpec, PortError>;

    /// How workers reach the two stores: opened directly, or through the controller when only the controller can open them.
    fn worker_access(&self) -> StoreAccess;
}

/// One `NativeLink` `StoreSpec` value, for example `{ "filesystem": { .. } }` or
/// `{ "experimental_cloud_object_store": { .. } }`, that the scheduler plugin deserializes.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedStoreSpec(pub serde_json::Value);
