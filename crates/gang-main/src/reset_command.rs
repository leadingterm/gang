//! The `gang-reset` trampoline clears the inherited signal mask and executes the action in place.
//!
//! Signal dispositions are already default: nothing sets `SIG_IGN`, and `exec` resets handlers.

use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::process::{Command, ExitCode};

use crate::errors::{MainError, Result};

/// Clears the mask and replaces this process with the action; returns only on failure.
pub(crate) fn run(arguments: &[OsString]) -> Result<ExitCode> {
    let (program, args) = arguments.split_first().ok_or(MainError::ResetUsage)?;
    gang_os::clear_mask().map_err(MainError::ResetMask)?;
    Err(MainError::ResetExec(
        Command::new(program).args(args).exec(),
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::ffi::OsString;

    use gang_testkit::ChildTest;

    const RESET_HELPER: ChildTest = ChildTest::new("reset_command::tests::reset_helper");
    const MASK_HELPER: ChildTest = ChildTest::new("reset_command::tests::mask_helper");

    /// A missing action is reported as a typed usage failure.
    #[test]
    fn requires_a_program() {
        assert!(matches!(
            super::run(&[]),
            Err(crate::errors::MainError::ResetUsage)
        ));
    }

    /// Re-executes the test binary through the trampoline with SIGTERM blocked.
    #[test]
    fn reset_unblocks_the_action() -> Result<(), Box<dyn std::error::Error>> {
        let blocked = gang_os::block_terminations()?;
        let spawned = RESET_HELPER
            .command()?
            .env("GANG_RESET_HELPER", "launch")
            .output();
        drop(blocked);
        let output = spawned?;
        MASK_HELPER.assert_completed(&output);
        Ok(())
    }

    /// The first invocation execs this helper again; the second inspects its inherited mask.
    #[test]
    #[ignore = "child test: run by reset_unblocks_the_action"]
    fn reset_helper() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("GANG_RESET_HELPER").as_deref() == Some(std::ffi::OsStr::new("launch"))
        {
            let executable = std::env::current_exe()?;
            // The target phase is selected in the environment before exec.
            let args: Vec<OsString> = std::iter::once(executable.into_os_string())
                .chain(MASK_HELPER.arguments().into_iter().map(OsString::from))
                .collect();
            return super::run(&args).map(|_| ()).map_err(Into::into);
        }
        Err("unexpected helper invocation".into())
    }

    /// Reports the mask after the trampoline execs this test binary.
    #[test]
    #[ignore = "child test: run by reset_helper"]
    fn mask_helper() -> Result<(), Box<dyn std::error::Error>> {
        let status = std::fs::read_to_string("/proc/thread-self/status")?;
        let mask = status
            .lines()
            .find_map(|line| line.strip_prefix("SigBlk:"))
            .ok_or("missing SigBlk")?
            .trim();
        assert_eq!(u64::from_str_radix(mask, 16)? & (1 << (15 - 1)), 0);
        MASK_HELPER.complete();
        Ok(())
    }
}
