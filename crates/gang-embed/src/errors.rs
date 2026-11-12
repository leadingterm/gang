//! Typed errors for the embed tool.

use std::io;
use std::path::PathBuf;

/// Errors produced while scanning a dist tree or writing generated source.
#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    /// The command line did not contain exactly a dist directory and an output path.
    #[error("usage: gang-embed <dist-dir> <out.rs>")]
    Usage,
    /// A file or directory could not be read.
    #[error("cannot read {}", path.display())]
    Read {
        /// The path that could not be read.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: io::Error,
    },
    /// The generated output could not be written.
    #[error("cannot write {}", path.display())]
    Write {
        /// The path that could not be written.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: io::Error,
    },
    /// A path within the dist tree is not valid UTF-8.
    #[error("path is not valid utf-8: {}", path.display())]
    NonUtf8Path {
        /// The offending path.
        path: PathBuf,
    },
    /// A `.br` or `.gz` file exists without its identity file.
    #[error("compressed variant has no identity file: {path}")]
    OrphanVariant {
        /// The relative path of the compressed variant.
        path: String,
    },
}
