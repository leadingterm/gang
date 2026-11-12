//! Where one end-to-end run keeps its state: reserved loopback ports and a private scratch tree.
//!
//! A [`Placement`] owns a temporary directory that holds every file and directory the run
//! writes. The directory is removed when the placement is dropped. Each directory is created
//! with mode `0700` so that concurrent runs on one machine do not see each other.

use std::path::{Path, PathBuf};

use crate::errors::{E2eError, Result};
use crate::ports;

/// The private scratch tree of one end-to-end run.
///
/// The tree is removed when the placement is dropped.
#[derive(Debug)]
pub struct Placement {
    root: tempfile::TempDir,
    public_port: u16,
    worker_port: u16,
    imds_port: u16,
}

impl Placement {
    /// Creates a placement under `scratch_root`, or under the system temporary directory when
    /// `scratch_root` is `None`.
    ///
    /// Every directory the run uses is created here with mode `0700`. The workspace is not
    /// created: the sample is staged into it later.
    ///
    /// # Errors
    ///
    /// Returns [`E2eError::Io`] when the root cannot be created, or [`E2eError::System`] when
    /// a directory cannot be created or a loopback port cannot be reserved.
    pub fn create(scratch_root: Option<&Path>) -> Result<Self> {
        let base = scratch_root.map_or_else(std::env::temp_dir, Path::to_path_buf);
        let root = tempfile::Builder::new()
            .prefix("gang-e2e-")
            .tempdir_in(base)
            .map_err(E2eError::io("creating the placement root"))?;
        let placement = Self {
            root,
            public_port: ports::reserve()?,
            worker_port: ports::reserve()?,
            imds_port: ports::reserve()?,
        };
        for directory in [
            placement.state_dir(),
            placement.cache_dir(),
            placement.keys_dir(),
            placement.noop_scratch(),
            placement.bazel_root(),
            placement.home(),
        ] {
            gang_os::fs::ensure_private_dir(&directory)
                .map_err(E2eError::system_from("creating a placement directory"))?;
        }
        Ok(placement)
    }

    /// The reserved loopback port of the public server listener.
    #[must_use]
    pub const fn public_port(&self) -> u16 {
        self.public_port
    }

    /// The reserved loopback port of the worker server listener.
    #[must_use]
    pub const fn worker_port(&self) -> u16 {
        self.worker_port
    }

    /// The loopback port the mock instance metadata service of this run is reserved on.
    #[must_use]
    pub const fn imds_port(&self) -> u16 {
        self.imds_port
    }

    /// The root directory of the placement.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    /// The directory holding the server's state.
    #[must_use]
    pub fn state_dir(&self) -> PathBuf {
        self.root().join("state")
    }

    /// The directory holding the server's cache.
    #[must_use]
    pub fn cache_dir(&self) -> PathBuf {
        self.root().join("cache")
    }

    /// The directory holding the signing keys.
    fn keys_dir(&self) -> PathBuf {
        self.root().join("keys")
    }

    /// The CA private key file, inside the keys directory.
    #[must_use]
    pub fn ca_key(&self) -> PathBuf {
        self.keys_dir().join("ca.key")
    }

    /// The `SQLite` database file. It is not created here.
    #[must_use]
    pub fn database(&self) -> PathBuf {
        self.root().join("gang.db")
    }

    /// The scratch directory the noop sandbox runs actions in.
    #[must_use]
    pub fn noop_scratch(&self) -> PathBuf {
        self.root().join("scratch")
    }

    /// The `gang.toml` file the run writes.
    #[must_use]
    pub fn config_file(&self) -> PathBuf {
        self.root().join("gang.toml")
    }

    /// The log file the server's output is appended to.
    #[must_use]
    pub fn server_log(&self) -> PathBuf {
        self.root().join("server.log")
    }

    /// The Bazel workspace the sample is staged into. It is not created here.
    #[must_use]
    pub fn workspace(&self) -> PathBuf {
        self.root().join("workspace")
    }

    /// The output user root Bazel uses for this run.
    #[must_use]
    pub fn bazel_root(&self) -> PathBuf {
        self.root().join("bazel")
    }

    /// The home directory every `gang` and Bazel command runs with.
    #[must_use]
    pub fn home(&self) -> PathBuf {
        self.root().join("home")
    }

    /// The final path component of the root, used as the sample's salt seed.
    #[must_use]
    pub fn name(&self) -> String {
        self.root()
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    }
}
