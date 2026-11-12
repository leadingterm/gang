//! Maps request paths onto embedded assets or the single-page-app fallback.

use std::path::Path;

use crate::Asset;
use crate::table::lookup;

/// Mount point of the dashboard, without a trailing slash.
const MOUNT: &str = "/ui";
/// Prefix every asset request path starts with.
const PREFIX: &str = "/ui/";
/// Table path served for the mount root and for extensionless routes.
const INDEX_PATH: &str = "index.html";
/// Parent-directory segment, rejected anywhere in the relative path.
const PARENT_SEGMENT: &str = "..";
/// Backslash, rejected anywhere in the relative path.
const BACKSLASH: char = '\\';
/// Percent-encoded dot, rejected case-insensitively.
const ENCODED_DOT: &[u8] = b"%2e";
/// Percent-encoded backslash, rejected case-insensitively.
const ENCODED_BACKSLASH: &[u8] = b"%5c";
/// Width of the percent-encoded tokens, used for the byte window scan.
const ENCODED_WIDTH: usize = 3;

/// Outcome of resolving a request path.
pub(crate) enum Route {
    /// Serve this embedded asset.
    Asset(&'static Asset),
    /// No asset matches the request.
    NotFound,
}

/// Resolves a full request path (`/ui`, `/ui/`, `/ui/assets/x.js`, ...) to a route.
pub(crate) fn resolve(path: &str) -> Route {
    let Some(rel) = relative(path) else {
        return Route::NotFound;
    };
    if is_traversal(rel) {
        return Route::NotFound;
    }
    let found = if has_extension(rel) {
        lookup(rel)
    } else {
        lookup(INDEX_PATH)
    };
    found.map_or(Route::NotFound, Route::Asset)
}

/// Returns the path below the mount point, or `None` when the path is outside it.
fn relative(path: &str) -> Option<&str> {
    if path == MOUNT {
        return Some("");
    }
    path.strip_prefix(PREFIX)
}

/// Reports whether the relative path could escape the dist root.
fn is_traversal(rel: &str) -> bool {
    rel.contains(PARENT_SEGMENT)
        || rel.contains(BACKSLASH)
        || rel.as_bytes().windows(ENCODED_WIDTH).any(|window| {
            window.eq_ignore_ascii_case(ENCODED_DOT)
                || window.eq_ignore_ascii_case(ENCODED_BACKSLASH)
        })
}

/// Reports whether the last path segment carries a file extension.
fn has_extension(rel: &str) -> bool {
    Path::new(rel).extension().is_some()
}
