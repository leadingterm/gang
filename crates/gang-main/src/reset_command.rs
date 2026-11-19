//! The `gang-reset` trampoline clears the inherited signal mask and executes the action in place.
//!
//! Signal dispositions are already default: nothing sets `SIG_IGN`, and `exec` resets handlers.

use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::process::{Command, ExitCode};

use crate::errors::{MainError, Result};

/// Clears the mask and replaces this process with the action; returns only on failure.
pub(crate) fn run(arguments: &[OsString]) -> Result<ExitCode> {
    let (gate, action) = if arguments.first().is_some_and(|arg| arg == "--") {
        (None, &arguments[1..])
    } else if arguments
        .first()
        .is_some_and(|arg| arg == "--execution-gate-fd")
        && arguments.get(2).is_some_and(|arg| arg == "--")
    {
        let fd = arguments
            .get(1)
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|fd| *fd >= 0)
            .ok_or(MainError::ResetUsage)?;
        (Some(fd), &arguments[3..])
    } else {
        return Err(MainError::ResetUsage);
    };
    let (program, args) = action.split_first().ok_or(MainError::ResetUsage)?;
    let mut command = Command::new(program);
    command.args(args);
    if let Some(fd) = gate {
        let environment = gated_environment(fd).map_err(MainError::ResetGate)?;
        command.env_clear().envs(environment);
    }
    gang_os::clear_mask().map_err(MainError::ResetMask)?;
    Err(MainError::ResetExec(command.exec()))
}

#[cfg(target_os = "linux")]
fn gated_environment(fd: i32) -> std::io::Result<Vec<(OsString, OsString)>> {
    use rustix::fs::{SealFlags, fcntl_get_seals};
    let gate = gang_os::ExecutionGateChild::from_inherited_fd(fd)?;
    let mut environment = std::fs::File::from(gate.wait()?);
    let required = SealFlags::SEAL | SealFlags::SHRINK | SealFlags::GROW | SealFlags::WRITE;
    if !fcntl_get_seals(&environment)?.contains(required) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "environment is not sealed",
        ));
    }
    gang_os::read_execution_environment(&mut environment)
}

#[cfg(not(target_os = "linux"))]
fn gated_environment(_fd: i32) -> std::io::Result<Vec<(OsString, OsString)>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "execution gates require Linux",
    ))
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use std::ffi::OsString;

    use gang_testkit::ChildTest;

    const RESET_HELPER: ChildTest = ChildTest::new("reset_command::tests::reset_helper");
    const MASK_HELPER: ChildTest = ChildTest::new("reset_command::tests::mask_helper");
    const GATE_HELPER: ChildTest = ChildTest::new("reset_command::tests::gate_helper");
    const GATE_PAYLOAD: ChildTest = ChildTest::new("reset_command::tests::gate_payload");

    /// A missing action is reported as a typed usage failure.
    #[test]
    fn requires_a_program() {
        assert!(matches!(
            super::run(&[]),
            Err(crate::errors::MainError::ResetUsage)
        ));
    }

    #[test]
    fn rejects_old_and_malformed_gate_grammar() {
        for args in [
            vec!["program"],
            vec!["--"],
            vec!["--execution-gate-fd", "invalid", "--", "program"],
            vec!["--execution-gate-fd", "1", "program"],
        ] {
            let args = args.into_iter().map(OsString::from).collect::<Vec<_>>();
            assert!(matches!(
                super::run(&args),
                Err(crate::errors::MainError::ResetUsage)
            ));
        }
    }

    #[test]
    fn closed_gate_never_executes_payload() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::fd::AsRawFd;
        use std::process::Stdio;
        let (parent, child) = gang_os::execution_gate_pair()?;
        rustix::io::fcntl_setfd(&child, rustix::io::FdFlags::empty())?;
        let process = GATE_HELPER
            .command()?
            .env_clear()
            .env("GATE_TEST_FD", child.as_raw_fd().to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        drop(child);
        drop(parent.try_ready()?);
        drop(parent);
        let output = process.wait_with_output()?;
        GATE_HELPER.assert_completed(&output);
        assert!(!GATE_PAYLOAD.completed_in(&String::from_utf8_lossy(&output.stdout)));
        Ok(())
    }

    #[test]
    fn bad_ack_never_executes_payload() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::fd::AsRawFd;
        use std::process::Stdio;
        let (parent, child) = gang_os::execution_gate_pair()?;
        rustix::io::fcntl_setfd(&child, rustix::io::FdFlags::empty())?;
        let process = GATE_HELPER
            .command()?
            .env_clear()
            .env("GATE_TEST_FD", child.as_raw_fd().to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        drop(child);
        drop(parent.try_ready()?);
        rustix::net::send(&parent, b"X", rustix::net::SendFlags::NOSIGNAL)?;
        let output = process.wait_with_output()?;
        GATE_HELPER.assert_completed(&output);
        assert!(!GATE_PAYLOAD.completed_in(&String::from_utf8_lossy(&output.stdout)));
        Ok(())
    }

    #[test]
    fn sealed_release_installs_exact_environment() -> Result<(), Box<dyn std::error::Error>> {
        use rustix::fs::{MemfdFlags, SealFlags};
        use std::os::fd::{AsFd, AsRawFd};
        use std::os::unix::ffi::OsStringExt;
        use std::process::Stdio;
        let (parent, child) = gang_os::execution_gate_pair()?;
        rustix::io::fcntl_setfd(&child, rustix::io::FdFlags::empty())?;
        let process = GATE_HELPER
            .command()?
            .env_clear()
            .env("GATE_TEST_FD", child.as_raw_fd().to_string())
            .env("GATE_TEST_RELEASE", "yes")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        drop(child);
        drop(parent.try_ready()?);
        let mut file = std::fs::File::from(rustix::fs::memfd_create(
            "gate-env",
            MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING,
        )?);
        let environment = vec![
            (OsString::from("ACTION_MARKER"), OsString::from("released")),
            (OsString::from("PATH"), OsString::from("/action-only")),
            (OsString::from("LD_PRELOAD"), OsString::new()),
            (
                OsString::from("NONUNICODE"),
                OsString::from_vec(vec![0xff, b'x']),
            ),
        ];
        gang_os::write_execution_environment(&mut file, &environment)?;
        rustix::fs::fcntl_add_seals(
            &file,
            SealFlags::SEAL | SealFlags::SHRINK | SealFlags::GROW | SealFlags::WRITE,
        )?;
        parent.release(file.as_fd())?;
        let output = process.wait_with_output()?;
        GATE_PAYLOAD.assert_completed(&output);
        Ok(())
    }

    #[test]
    #[ignore = "child test: run by gate release tests"]
    fn gate_helper() -> Result<(), Box<dyn std::error::Error>> {
        for key in ["ACTION_MARKER", "LD_PRELOAD", "PATH", "NONUNICODE"] {
            assert!(
                std::env::var_os(key).is_none(),
                "{key} leaked before the gate"
            );
        }
        let release = std::env::var_os("GATE_TEST_RELEASE").is_some();
        let fd = std::env::var("GATE_TEST_FD")?;
        let mut args = vec![
            OsString::from("--execution-gate-fd"),
            fd.into(),
            OsString::from("--"),
            std::env::current_exe()?.into_os_string(),
        ];
        args.extend(GATE_PAYLOAD.arguments().into_iter().map(OsString::from));
        let result = super::run(&args);
        assert!(!release, "positive release did not execute the payload");
        assert!(matches!(
            result,
            Err(crate::errors::MainError::ResetGate(_))
        ));
        GATE_HELPER.complete();
        Ok(())
    }

    #[test]
    #[ignore = "child test: run by sealed_release_installs_exact_environment"]
    fn gate_payload() {
        use std::os::unix::ffi::OsStringExt;
        let actual = std::env::vars_os().collect::<std::collections::BTreeMap<_, _>>();
        let expected = [
            (OsString::from("ACTION_MARKER"), OsString::from("released")),
            (OsString::from("PATH"), OsString::from("/action-only")),
            (OsString::from("LD_PRELOAD"), OsString::new()),
            (
                OsString::from("NONUNICODE"),
                OsString::from_vec(vec![0xff, b'x']),
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(actual, expected);
        GATE_PAYLOAD.complete();
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
            let args: Vec<OsString> = std::iter::once(OsString::from("--"))
                .chain(std::iter::once(executable.into_os_string()))
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
