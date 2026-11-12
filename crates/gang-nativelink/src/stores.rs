//! Builds the named stores of `NativeLink`'s configuration and looks them up by name.

use std::sync::Arc;

use nativelink_config::cas_server::StoreConfig;
use nativelink_error::make_input_err;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::Store;

use crate::errors::NativeLinkEmbedError;

/// Creates every store in `stores` under its name, then runs the post-init hooks of all stores.
pub(crate) async fn build_stores(
    stores: Vec<StoreConfig>,
) -> Result<Arc<StoreManager>, NativeLinkEmbedError> {
    let store_manager = Arc::new(StoreManager::new());
    for StoreConfig { name, spec } in stores {
        let store = store_factory(&spec, &store_manager, None)
            .await
            .map_err(NativeLinkEmbedError::native("store creation failed"))?;
        store_manager
            .add_store(&name, store)
            .map_err(NativeLinkEmbedError::native("store registration failed"))?;
    }
    store_manager
        .run_post_init()
        .await
        .map_err(NativeLinkEmbedError::native("store post-init failed"))?;
    Ok(store_manager)
}

/// Returns the store registered under `name`, or an error naming the store when none is.
pub(crate) fn lookup(manager: &StoreManager, name: &str) -> Result<Store, NativeLinkEmbedError> {
    manager.get_store(name).ok_or_else(|| {
        NativeLinkEmbedError::native("store lookup")(make_input_err!(
            "store {name} is not configured"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use serde_json::json;

    use super::*;

    #[test]
    fn lookup_of_a_missing_store_names_it() {
        let manager = StoreManager::new();
        let Err(error) = lookup(&manager, "missing") else {
            panic!("an empty manager has no store named `missing`");
        };
        let source = error.source().map(ToString::to_string).unwrap_or_default();
        assert!(source.contains("missing"), "source was {source:?}");
    }

    #[tokio::test]
    async fn build_stores_registers_every_store() {
        let config: StoreConfig =
            serde_json::from_value(json!({"name": "A", "memory": {}})).expect("valid store config");
        let manager = build_stores(vec![config])
            .await
            .expect("memory store builds");
        assert!(manager.get_store("A").is_some());
    }
}
