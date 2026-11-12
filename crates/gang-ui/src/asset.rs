//! The embedded asset record produced by the build-time generator.

/// One file of the embedded dashboard bundle.
#[derive(Debug)]
pub struct Asset {
    /// Relative path within the bundle, `/`-separated, without a leading slash (e.g. `assets/main-0123.js`).
    pub path: &'static str,
    /// The `Content-Type` value served for this file.
    pub content_type: &'static str,
    /// Strong entity tag, including the surrounding double quotes, derived from the identity bytes.
    pub etag: &'static str,
    /// The uncompressed file contents.
    pub identity: &'static [u8],
    /// Brotli-compressed contents, when the build produced a `.br` sibling.
    pub br: Option<&'static [u8]>,
    /// Gzip-compressed contents, when the build produced a `.gz` sibling.
    pub gz: Option<&'static [u8]>,
    /// Whether the path is content-hashed and may be cached indefinitely.
    pub immutable: bool,
}

impl Asset {
    /// Reports whether a precompressed variant (brotli or gzip) is embedded.
    pub(crate) fn has_variants(&self) -> bool {
        self.br.is_some() || self.gz.is_some()
    }
}
