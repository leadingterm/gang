//! Resolution of sandbox programs to the executable files the sandbox file names.

use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::errors::WorkerError;

/// Resolves `program` to the canonical path of an executable regular file.
///
/// A program with more than one path component, or an absolute one, is the only candidate. A
/// bare name is looked up in each directory of `search_path`, in order; empty entries are
/// skipped, so an empty entry never stands for the current directory. The first candidate that
/// resolves to an executable regular file wins, and symbolic links are resolved.
///
/// # Errors
///
/// [`WorkerError::SandboxProgram`] when no candidate is an executable regular file.
pub(crate) fn resolve_program(program: &Path, search_path: &OsStr) -> Result<PathBuf, WorkerError> {
    let candidates: Vec<PathBuf> = if program.is_absolute() || program.components().count() > 1 {
        vec![program.to_path_buf()]
    } else {
        std::env::split_paths(search_path)
            .filter(|directory| !directory.as_os_str().is_empty())
            .map(|directory| directory.join(program))
            .collect()
    };
    candidates
        .iter()
        .find_map(|candidate| executable(candidate))
        .ok_or_else(|| WorkerError::SandboxProgram {
            program: program.to_path_buf(),
        })
}

/// Returns the canonical path of `candidate` when it is an executable regular file.
fn executable(candidate: &Path) -> Option<PathBuf> {
    let canonical = fs::canonicalize(candidate).ok()?;
    let metadata = fs::metadata(&canonical).ok()?;
    (metadata.is_file() && (metadata.permissions().mode() & 0o111) != 0).then_some(canonical)
}
