use std::fs::{self, DirBuilder};
use std::path::{Path, PathBuf};

use gang_config::SANDBOX_FILE_NAME;
use gang_os::PathIo;
use gang_os::fs::{
    PRIVATE_FILE, ensure_private_dir, remove_dir_optional, replace_symlink, write_atomic,
};
use gang_spi::{EXEC_LINK_NAME, JOIN_LINK_NAME};
use gang_types::CertificatePem;

use crate::credentials::Credentials;
use crate::errors::WorkerError;

const PUBLIC_FILE_MODE: u32 = 0o644;

/// On-disk layout of a worker under its mounted scratch volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    root: PathBuf,
}

/// Paths of the identity files written for one generation of credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityFiles {
    /// The directory holding this generation's files; removing it removes all of them.
    pub directory: PathBuf,
    /// The worker's certificate, presented on its mutual TLS connections.
    pub certificate: PathBuf,
    /// The private key matching `certificate`.
    pub private_key: PathBuf,
    /// The certificate authority that signed `certificate`.
    pub server_ca: PathBuf,
}

impl Layout {
    /// Creates the layout rooted at the mounted scratch volume `scratch`.
    #[must_use]
    pub fn new(scratch: &Path) -> Self {
        Self {
            root: scratch.to_path_buf(),
        }
    }

    /// Returns the directory where action work is staged.
    #[must_use]
    pub fn work_directory(&self) -> PathBuf {
        self.root.join("work")
    }

    /// Returns the directory of the fast content-addressed store.
    #[must_use]
    pub fn fast_cas_directory(&self) -> PathBuf {
        self.root.join("fast-cas")
    }

    /// Returns the path of the rendered sandbox configuration, beside the entrypoint link.
    #[must_use]
    pub fn sandbox_file(&self) -> PathBuf {
        self.entrypoint().with_file_name(SANDBOX_FILE_NAME)
    }

    /// Returns the path of the `gang-exec` entrypoint link.
    #[must_use]
    pub fn entrypoint(&self) -> PathBuf {
        self.bin_directory().join(EXEC_LINK_NAME)
    }

    /// Returns the path of the `gang-join` link, through which a command starts inside an
    /// action's limits scope.
    #[must_use]
    pub fn join_link(&self) -> PathBuf {
        self.bin_directory().join(JOIN_LINK_NAME)
    }

    fn bin_directory(&self) -> PathBuf {
        self.root.join("bin")
    }

    fn identity_directory(&self) -> PathBuf {
        self.root.join("identity")
    }

    /// Creates the directories, writes the sandbox configuration, and links the entrypoint and
    /// the join link.
    ///
    /// The identity directory is emptied of the generations an earlier process left behind, then
    /// created readable only by its owner. An existing sandbox file, entrypoint link, or join
    /// link is replaced.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Io`] when a directory, the sandbox file, or the link cannot be
    /// created or written.
    pub fn prepare(&self, sandbox_file: &str, executable: &Path) -> Result<(), WorkerError> {
        create_directory(&self.work_directory())?;
        create_directory(&self.fast_cas_directory())?;
        create_directory(&self.bin_directory())?;
        remove_dir_optional(&self.identity_directory())?;
        ensure_private_dir(&self.identity_directory())?;
        write_atomic(
            &self.sandbox_file(),
            sandbox_file.as_bytes(),
            PUBLIC_FILE_MODE,
        )?;
        replace_symlink(executable, &self.entrypoint())?;
        replace_symlink(executable, &self.join_link())?;
        Ok(())
    }

    /// Writes the server CA used by server-only TLS.
    ///
    /// # Errors
    ///
    /// Returns an error if the CA file cannot be written.
    pub fn write_server_ca(&self, ca: &CertificatePem) -> Result<PathBuf, PathIo> {
        let path = self.identity_directory().join("server-ca.pem");
        write_atomic(&path, ca.as_str().as_bytes(), PUBLIC_FILE_MODE)?;
        Ok(path)
    }

    /// Writes the certificate, private key, and server CA of `credentials` into the directory of
    /// `generation`. The private key is written with mode `0600`.
    ///
    /// The files of a generation are written once and never rewritten. A directory left behind by
    /// an earlier failed attempt at the same generation is removed first; if this write fails, the
    /// partial directory is removed.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Io`] when the generation's directory or a file cannot be written.
    pub fn write_credentials(
        &self,
        generation: u64,
        credentials: &Credentials,
    ) -> Result<IdentityFiles, WorkerError> {
        let directory = self.generation_directory(generation);
        remove_dir_optional(&directory)?;
        ensure_private_dir(&directory)?;
        let files = IdentityFiles {
            directory: directory.clone(),
            certificate: directory.join("certificate.pem"),
            private_key: directory.join("private-key.pem"),
            server_ca: directory.join("ca.pem"),
        };
        let written = write_atomic(
            &files.certificate,
            credentials.certificate.as_str().as_bytes(),
            PUBLIC_FILE_MODE,
        )
        .and_then(|()| {
            write_atomic(
                &files.private_key,
                credentials.private_key.expose().as_bytes(),
                PRIVATE_FILE,
            )
        })
        .and_then(|()| {
            write_atomic(
                &files.server_ca,
                credentials.ca_certificate.as_str().as_bytes(),
                PUBLIC_FILE_MODE,
            )
        });
        match written {
            Ok(()) => Ok(files),
            Err(error) => {
                // Best-effort cleanup; the failure that caused the error is the one to report.
                let _ = fs::remove_dir_all(&directory);
                Err(error.into())
            }
        }
    }

    /// Removes the directory of one generation's credentials, with every file in it.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Io`] when the directory exists and cannot be removed.
    pub fn remove_credentials(&self, files: &IdentityFiles) -> Result<(), WorkerError> {
        remove_dir_optional(&files.directory).map_err(WorkerError::from)
    }

    fn generation_directory(&self, generation: u64) -> PathBuf {
        self.identity_directory().join(generation.to_string())
    }
}

/// Creates `path` and any missing parents with the default directory mode.
fn create_directory(path: &Path) -> Result<(), PathIo> {
    DirBuilder::new()
        .recursive(true)
        .create(path)
        .map_err(PathIo::at("create directory", path))
}
