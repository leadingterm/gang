//! The action directory: the directory under `NativeLink`'s work directory that holds one action.

use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::errors::ExecError;

/// Resolves the action directory of an action whose command runs in `working_directory`.
///
/// `NativeLink` gives each action a directory directly under its work directory. The action's
/// files live in `<root>/<id>/work/`, and its command runs in the working directory
/// `<root>/<id>/work/<command working directory>`. Both paths are canonicalized first, so a
/// symbolic link cannot move the working directory out of the work directory. The result is the
/// canonical `<root>/<id>`.
///
/// # Errors
///
/// - [`ExecError::WorkDirectory`] when `work_directory` cannot be canonicalized.
/// - [`ExecError::WorkingDirectory`] when `working_directory` cannot be canonicalized.
/// - [`ExecError::OutsideWorkDirectory`] when `working_directory` is not strictly inside
///   `work_directory`.
pub fn action_directory(
    work_directory: &Path,
    working_directory: &Path,
) -> Result<PathBuf, ExecError> {
    let root = fs::canonicalize(work_directory).map_err(|source| ExecError::WorkDirectory {
        path: work_directory.to_path_buf(),
        source,
    })?;
    let working = fs::canonicalize(working_directory).map_err(ExecError::WorkingDirectory)?;
    let id = working
        .strip_prefix(&root)
        .ok()
        .and_then(|rest| match rest.components().next() {
            Some(Component::Normal(id)) => Some(id.to_os_string()),
            _ => None,
        });
    match id {
        Some(id) => Ok(root.join(id)),
        None => Err(ExecError::OutsideWorkDirectory {
            working_directory: working,
            work_directory: root,
        }),
    }
}
