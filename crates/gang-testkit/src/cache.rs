//! A test double for the cache port, with in-memory stores.

use gang_spi::{CacheStorePort, PortError, RenderedStoreSpec, StoreAccess};

/// A cache whose two stores are in-memory and whose workers open them directly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FakeCache;

/// The in-memory store description that both of the fake's stores report.
fn memory_store() -> RenderedStoreSpec {
    RenderedStoreSpec(serde_json::json!({"memory": {}}))
}

impl CacheStorePort for FakeCache {
    fn content_store(&self) -> Result<RenderedStoreSpec, PortError> {
        Ok(memory_store())
    }

    fn action_store(&self) -> Result<RenderedStoreSpec, PortError> {
        Ok(memory_store())
    }

    fn worker_access(&self) -> StoreAccess {
        StoreAccess::Direct
    }
}
