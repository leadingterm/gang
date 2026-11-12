//! Scans a dist tree into a deterministic list of embeddable assets.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::content_type::content_type_of;
use crate::errors::EmbedError;

/// Path prefix of content-hashed files, which are served as immutable.
const IMMUTABLE_PREFIX: &str = "assets/";
/// Suffix of a Brotli-compressed sibling variant.
const BR_SUFFIX: &str = ".br";
/// Suffix of a gzip-compressed sibling variant.
const GZ_SUFFIX: &str = ".gz";

/// One identity file of the dist tree with its availability of compressed variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Path relative to the dist root, `/` separated, e.g. `assets/main-0123.js`.
    pub path: String,
    /// MIME type derived from the extension of `path`.
    pub content_type: &'static str,
    /// Strong validator: the quoted lowercase hex SHA-256 of the identity bytes.
    pub etag: String,
    /// True iff `path` lives under `assets/`.
    pub immutable: bool,
    /// True iff a `<path>.br` file exists beside this entry.
    pub br: bool,
    /// True iff a `<path>.gz` file exists beside this entry.
    pub gz: bool,
}

/// The complete set of entries of a dist tree, sorted by path in byte order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    entries: Vec<Entry>,
}

impl Bundle {
    /// Walks `dist` recursively and builds the bundle of its identity files.
    ///
    /// Symbolic links are followed. Compressed variants (`.br`, `.gz`) are
    /// attached to their identity file and never become entries.
    ///
    /// # Errors
    ///
    /// Returns [`EmbedError::Read`] when a directory or file cannot be read,
    /// [`EmbedError::NonUtf8Path`] when a file name is not valid UTF-8, and
    /// [`EmbedError::OrphanVariant`] when a compressed variant has no identity file.
    pub fn scan(dist: &Path) -> Result<Self, EmbedError> {
        let mut files = BTreeSet::new();
        collect_files(dist, "", &mut files)?;
        let identities = identity_paths(&files)?;
        let entries = identities
            .into_iter()
            .map(|path| entry_for(dist, &files, path))
            .collect::<Result<Vec<Entry>, EmbedError>>()?;
        Ok(Self { entries })
    }

    /// Returns the entries in path byte order.
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

/// Recursively records the relative path of every regular file under `dir`.
fn collect_files(dir: &Path, prefix: &str, files: &mut BTreeSet<String>) -> Result<(), EmbedError> {
    let listing = fs::read_dir(dir).map_err(|source| read_error(dir, source))?;
    for item in listing {
        let item = item.map_err(|source| read_error(dir, source))?;
        let full = item.path();
        let relative = join_relative(prefix, item.file_name(), &full)?;
        let metadata = fs::metadata(&full).map_err(|source| read_error(&full, source))?;
        if metadata.is_dir() {
            collect_files(&full, &relative, files)?;
        } else if metadata.is_file() {
            files.insert(relative);
        }
    }
    Ok(())
}

/// Appends one UTF-8 file name to a relative directory prefix.
fn join_relative(prefix: &str, name: OsString, full: &Path) -> Result<String, EmbedError> {
    let name = name.into_string().map_err(|_| EmbedError::NonUtf8Path {
        path: full.to_path_buf(),
    })?;
    Ok(if prefix.is_empty() {
        name
    } else {
        format!("{prefix}/{name}")
    })
}

/// Separates identity paths from compressed variants, rejecting orphan variants.
fn identity_paths(files: &BTreeSet<String>) -> Result<Vec<&String>, EmbedError> {
    let mut identities = Vec::with_capacity(files.len());
    for path in files {
        match variant_base(path) {
            Some(base) if !files.contains(base) => {
                return Err(EmbedError::OrphanVariant { path: path.clone() });
            }
            Some(_) => {}
            None => identities.push(path),
        }
    }
    Ok(identities)
}

/// Returns the identity path a compressed variant belongs to, if `path` is a variant.
fn variant_base(path: &str) -> Option<&str> {
    path.strip_suffix(BR_SUFFIX)
        .or_else(|| path.strip_suffix(GZ_SUFFIX))
}

/// Builds the entry for one identity path, reading and hashing its bytes.
fn entry_for(dist: &Path, files: &BTreeSet<String>, path: &str) -> Result<Entry, EmbedError> {
    let identity_path = dist.join(path);
    let bytes = fs::read(&identity_path).map_err(|source| read_error(&identity_path, source))?;
    Ok(Entry {
        path: path.to_owned(),
        content_type: content_type_of(path),
        etag: etag_of(&bytes),
        immutable: path.starts_with(IMMUTABLE_PREFIX),
        br: has_variant(files, path, BR_SUFFIX),
        gz: has_variant(files, path, GZ_SUFFIX),
    })
}

/// Reports whether the sibling variant `path + suffix` is present.
fn has_variant(files: &BTreeSet<String>, path: &str, suffix: &str) -> bool {
    files.contains(&format!("{path}{suffix}"))
}

/// Computes the strong `ETag` of identity bytes: the quoted lowercase hex SHA-256.
fn etag_of(bytes: &[u8]) -> String {
    format!("\"{}\"", hex::encode(Sha256::digest(bytes)))
}

/// Wraps an I/O failure on `path` as a read error.
fn read_error(path: &Path, source: io::Error) -> EmbedError {
    EmbedError::Read {
        path: path.to_path_buf(),
        source,
    }
}
