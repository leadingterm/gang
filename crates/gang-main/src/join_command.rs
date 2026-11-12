//! The `gang-join` trampoline: joins an action's limits scope, then executes the command in place.
//!
//! The sandbox starts each action command through the `gang-join` link that `gang worker` creates
//! to this binary, with the scope directory before the command. The process writes its own pid
//! into the scope and replaces itself with the command, so the command keeps the pid, environment,
//! working directory, stdio, process group, and open files it was started with. Nothing is printed
//! here: when joining fails, the error is returned and the command never starts.

use std::ffi::OsString;

use crate::errors::MainError;

/// Joins the scope named by the first of `arguments` and executes the rest in place.
///
/// Returns only when joining or executing fails, with the failure as the error.
pub(crate) fn run(arguments: &[OsString]) -> MainError {
    gang_cgroup::join(arguments).into()
}
