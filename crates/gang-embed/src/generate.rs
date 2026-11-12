//! Scans a dist tree and writes the generated asset table.

use std::io;
use std::path::{Component, Path};

use crate::bundle::Bundle;
use crate::errors::EmbedError;

/// Scans `dist` and writes the generated Rust asset table to `out`.
///
/// The `include_bytes!` paths in the output are relative to the directory that
/// contains `out`, so `out` must live in the same output tree as `dist`.
///
/// The output is deterministic: the same dist tree always yields byte-identical
/// source, because entries are sorted by path before rendering.
///
/// # Errors
///
/// Returns the scan error if the dist tree cannot be read, or
/// [`EmbedError::Read`] / [`EmbedError::Write`] if either path cannot be made
/// absolute, or [`EmbedError::Write`] if `out` cannot be written.
pub fn generate(dist: &Path, out: &Path) -> Result<(), EmbedError> {
    let bundle = Bundle::scan(dist)?;
    let abs_dist = std::path::absolute(dist).map_err(|source| EmbedError::Read {
        path: dist.to_path_buf(),
        source,
    })?;
    let abs_out = std::path::absolute(out).map_err(|source| EmbedError::Write {
        path: out.to_path_buf(),
        source,
    })?;
    let out_dir = abs_out.parent().ok_or_else(|| EmbedError::Write {
        path: out.to_path_buf(),
        source: io::Error::new(
            io::ErrorKind::InvalidInput,
            "output path has no parent directory",
        ),
    })?;
    for path in [abs_dist.as_path(), out_dir] {
        if path.to_str().is_none() {
            return Err(EmbedError::NonUtf8Path {
                path: path.to_path_buf(),
            });
        }
    }
    let root = relative_root(out_dir, &abs_dist);
    std::fs::write(&abs_out, bundle.render(&root)).map_err(|source| EmbedError::Write {
        path: out.to_path_buf(),
        source,
    })
}

/// Returns the `/`-separated lexical path from `from_dir` to `to`.
///
/// Both paths must be absolute and UTF-8; the caller validates this. The common
/// leading components are stripped, one `..` is emitted per remaining component
/// of `from_dir`, and the remaining components of `to` follow. An empty result
/// is returned as `"."`.
fn relative_root(from_dir: &Path, to: &Path) -> String {
    let from: Vec<Component<'_>> = from_dir.components().collect();
    let to: Vec<Component<'_>> = to.components().collect();
    let common = from.iter().zip(&to).take_while(|(a, b)| a == b).count();

    let mut parts: Vec<String> = Vec::new();
    for _ in common..from.len() {
        parts.push("..".to_owned());
    }
    for component in &to[common..] {
        parts.push(component.as_os_str().to_string_lossy().into_owned());
    }

    if parts.is_empty() {
        ".".to_owned()
    } else {
        parts.join("/")
    }
}
