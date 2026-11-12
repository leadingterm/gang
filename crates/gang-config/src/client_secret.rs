//! Where the dashboard login's client secret comes from.

use std::io;
use std::path::PathBuf;

use gang_types::{AbsolutePathBuf, ClientSecret};

/// The client secret of `[dashboard.login]`: written inline, or kept in a file that the server reads once at start.
#[derive(Debug, Clone)]
pub enum ClientSecretSource {
    /// The secret itself, written in `gang.toml`. For development; its `Debug` output is redacted.
    Inline(ClientSecret),
    /// An absolute path to a file that holds the secret.
    File(AbsolutePathBuf),
}

/// Why the secret file could not be used.
#[derive(Debug, thiserror::Error, gang_types::Fix)]
pub enum SecretFileError {
    /// The file could not be read as UTF-8 text.
    #[error("cannot read the client secret file {}", path.display())]
    #[fix("check dashboard.login.client_secret_file")]
    Read {
        /// The file that could not be read.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: io::Error,
    },
    /// The file holds nothing but whitespace.
    #[error("the client secret file {} is empty", path.display())]
    #[fix("put the client secret in the file named by dashboard.login.client_secret_file")]
    Empty {
        /// The file that holds no secret.
        path: PathBuf,
    },
}

impl ClientSecretSource {
    /// Returns the secret: the inline one, or the content of the file with surrounding whitespace trimmed.
    ///
    /// # Errors
    ///
    /// [`SecretFileError::Read`] when the file cannot be read as UTF-8, [`SecretFileError::Empty`]
    /// when it holds only whitespace.
    pub fn load(&self) -> Result<ClientSecret, SecretFileError> {
        match self {
            Self::Inline(secret) => Ok(secret.clone()),
            Self::File(path) => {
                let text =
                    std::fs::read_to_string(path).map_err(|source| SecretFileError::Read {
                        path: path.as_path().to_path_buf(),
                        source,
                    })?;
                let trimmed = text.trim();
                if trimmed.is_empty() {
                    return Err(SecretFileError::Empty {
                        path: path.as_path().to_path_buf(),
                    });
                }
                Ok(ClientSecret::new(trimmed))
            }
        }
    }
}
