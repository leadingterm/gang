//! The `gang-join` trampoline: moves the calling process into an action's scope, then replaces it
//! with the program, so the program starts under the scope's limits.

use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::errors::CgroupError;
use crate::fs::PROCS;

/// Runs as `gang-join <scope> <program> [<argument>...]`, where `arguments` holds everything after
/// `gang-join`. Writes this process's id into the scope's `cgroup.procs`, then replaces the process
/// with `program` in place.
///
/// The scope's limits hold from the program's first instruction. The environment, working
/// directory, standard streams, open descriptors and process group pass through the exec
/// unchanged. The function returns only on failure, and every failure is reported before anything
/// is executed, so the command never runs outside its limits.
#[must_use]
pub fn join(arguments: &[OsString]) -> CgroupError {
    let [scope, program, rest @ ..] = arguments else {
        return CgroupError::JoinUsage;
    };
    let scope = Path::new(scope);
    if let Err(source) = enter_scope(scope) {
        return CgroupError::Join {
            scope: scope.to_path_buf(),
            source,
        };
    }
    let program = PathBuf::from(program);
    let source = Command::new(&program).args(rest).exec();
    CgroupError::Exec { program, source }
}

/// Moves this process into `scope` by writing its id to the scope's `cgroup.procs`. The file is
/// never created: a directory that is not a scope is an error.
pub(crate) fn enter_scope(scope: &Path) -> io::Result<()> {
    let mut procs = OpenOptions::new().write(true).open(scope.join(PROCS))?;
    procs.write_all(std::process::id().to_string().as_bytes())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::join;
    use crate::errors::CgroupError;

    #[test]
    fn a_directory_that_is_not_a_scope_is_refused_before_anything_runs() {
        let scope = tempfile::tempdir().expect("create the scope directory");
        let arguments = [
            scope.path().as_os_str().to_owned(),
            OsString::from("gang-never-runs"),
        ];

        let error = join(&arguments);

        assert!(matches!(error, CgroupError::Join { .. }), "{error:?}");
    }

    #[test]
    fn fewer_than_two_arguments_are_a_usage_error() {
        assert!(matches!(join(&[]), CgroupError::JoinUsage));
        assert!(matches!(
            join(&[OsString::from("scope")]),
            CgroupError::JoinUsage
        ));
    }
}
