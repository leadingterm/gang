//! Cache: where the content-addressable and action stores live.

use gang_clock::Timestamp;

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

    /// The action-result store for the server-owned environment version.
    ///
    /// # Errors
    ///
    /// The cache's own error when the store cannot be described.
    fn action_store(
        &self,
        environment: &gang_types::EnvironmentName,
    ) -> Result<RenderedStoreSpec, PortError>;

    /// How workers reach the two stores: opened directly, or through the controller when only the controller can open them.
    fn worker_access(&self) -> StoreAccess;

    /// The provider's latest measurement for stores not bounded by the engine.
    ///
    /// Returns `None` when the backend has no measurement.
    ///
    /// # Errors
    ///
    /// The cache's own error when the measurement cannot be fetched.
    async fn measured_usage(&self) -> Result<Option<MeasuredUsage>, PortError>;
}

/// A provider's measurement of the storage it holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MeasuredUsage {
    /// Bytes held by the provider.
    pub bytes: u64,
    /// Objects held by the provider.
    pub objects: u64,
    /// When the provider measured the usage.
    pub measured: Timestamp,
}

/// One `NativeLink` `StoreSpec` value, for example `{ "filesystem": { .. } }` or
/// `{ "experimental_cloud_object_store": { .. } }`, that the scheduler plugin deserializes.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedStoreSpec(pub serde_json::Value);
