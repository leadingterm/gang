//! A test double for the cache port, with in-memory stores.

use gang_spi::{CacheStorePort, MeasuredUsage, PortError, StoreAccess};

use crate::{MemoryActionStore, MemoryContentStore};

/// A cache whose two stores are in-memory and whose workers open them directly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FakeCache;

impl CacheStorePort for FakeCache {
    type Content = MemoryContentStore;
    type Actions = MemoryActionStore;

    fn content_store(&self) -> Result<Self::Content, PortError> {
        Ok(MemoryContentStore::new())
    }

    fn action_store(
        &self,
        _environment: &gang_types::EnvironmentName,
    ) -> Result<Self::Actions, PortError> {
        Ok(MemoryActionStore::new())
    }

    fn worker_access(&self) -> StoreAccess {
        StoreAccess::Direct
    }

    async fn measured_usage(&self) -> Result<Option<MeasuredUsage>, PortError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_has_no_provider_measurement() {
        assert_eq!(FakeCache.measured_usage().await.expect("measurement"), None);
    }
}
