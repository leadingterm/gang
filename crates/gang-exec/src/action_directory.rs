//! The action directory: the directory under `NativeLink`'s work directory that holds one action.

use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::errors::ExecError;

/// The directory under `NativeLink`'s work directory that holds one action, and the working
/// directory of its command inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionDirectory {
    /// The canonical `<root>/<id>`.
    pub path: PathBuf,
    /// The canonical working directory of the command, inside `path`.
    pub working_directory: PathBuf,
}

impl ActionDirectory {
    /// Resolves the action directory of an action whose command runs in `working_directory`.
    ///
    /// `NativeLink` gives each action a directory directly under its work directory. The action's
    /// files live in `<root>/<id>/work/`, and its command runs in the working directory
    /// `<root>/<id>/work/<command working directory>`. Both paths are canonicalized first, so a
    /// symbolic link cannot move the working directory out of the work directory. The result is the
    /// canonical `<root>/<id>`. Both results are canonical, so a sandbox can bind `path` at its own
    /// path and still enter the working directory.
    ///
    /// # Errors
    ///
    /// - [`ExecError::WorkDirectory`] when `work_directory` cannot be canonicalized.
    /// - [`ExecError::WorkingDirectory`] when `working_directory` cannot be canonicalized.
    /// - [`ExecError::OutsideWorkDirectory`] when `working_directory` is not strictly inside
    ///   `work_directory`.
    pub fn try_new(work_directory: &Path, working_directory: &Path) -> Result<Self, ExecError> {
        let root = fs::canonicalize(work_directory).map_err(|source| ExecError::WorkDirectory {
            path: work_directory.to_path_buf(),
            source,
        })?;
        let working = fs::canonicalize(working_directory).map_err(ExecError::WorkingDirectory)?;
        let id = working
            .strip_prefix(&root)
            .ok()
            .and_then(|rest| match rest.components().next() {
                Some(Component::Normal(id)) => Some(id),
                _ => None,
            });
        match id {
            Some(id) => {
                let mut path = root;
                path.push(id);
                Ok(Self {
                    path,
                    working_directory: working,
                })
            }
            None => Err(ExecError::OutsideWorkDirectory {
                working_directory: working,
                work_directory: root,
            }),
        }
    }
}
