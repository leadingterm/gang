//! Exact-path lookup over the generated `assets` table.

use crate::Asset;
use crate::assets::ASSETS;

/// Finds the embedded asset whose relative path equals `path` exactly.
///
/// The table is sorted by path by the generator, so this is a binary search.
#[must_use]
pub fn lookup(path: &str) -> Option<&'static Asset> {
    ASSETS
        .binary_search_by_key(&path, |asset| asset.path)
        .ok()
        .and_then(|index| ASSETS.get(index))
}
