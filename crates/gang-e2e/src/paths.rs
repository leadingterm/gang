//! Anchoring of user-supplied paths: a relative path names a place under the directory the user ran
//! the command from.

use std::path::{Path, PathBuf};

/// `path` as a path that does not depend on the current directory: an absolute `path` is returned
/// unchanged, and a relative one is joined to `base`.
#[must_use]
pub fn anchored(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
