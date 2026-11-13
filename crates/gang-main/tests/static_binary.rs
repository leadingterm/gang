//! Checks that the shipped `gang` binary is static, that `LD_PRELOAD` cannot
//! make `gang-exec` run foreign code, and that the CLI behaves as documented
//! when run as a black box.
#![cfg(target_os = "linux")]

mod support;

use std::error::Error;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::time::Duration;

use object::read::elf::{Dyn, ElfFile64, ProgramHeader};
use object::{Endianness, elf};
use support::{required_path, wait_within};

type TestResult = Result<(), Box<dyn Error>>;

/// Deadline for every child process this file spawns.
const CHILD_LIMIT: Duration = Duration::from_mins(1);

/// Runs `command` to completion within `limit`, capturing stdout and stderr.
fn output_within(command: &mut Command, limit: Duration) -> Result<Output, Box<dyn Error>> {
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    wait_within(child, limit, true)
}

/// Runs `command` to completion within `limit` and returns its exit status.
fn status_within(command: &mut Command, limit: Duration) -> Result<ExitStatus, Box<dyn Error>> {
    let child = command.spawn()?;
    Ok(wait_within(child, limit, true)?.status)
}

/// A `gang` command with an empty environment and `HOME` and
/// `XDG_CONFIG_HOME` both set to `home`.
fn gang_command(home: &Path) -> Result<Command, Box<dyn Error>> {
    let mut command = Command::new(required_path("GANG_BIN")?);
    command
        .env_clear()
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home)
        .stdin(Stdio::null());
    Ok(command)
}

/// Reads the checked-in config whose file name is `name`.
fn checked_in_config(name: &str) -> Result<PathBuf, Box<dyn Error>> {
    let listed = std::env::var("GANG_CHECKED_IN_CONFIGS")
        .map_err(|_| "GANG_CHECKED_IN_CONFIGS must be set by the Bazel test_env")?;
    for entry in listed.split_whitespace() {
        let path = std::env::current_dir()?.join(entry);
        if path.file_name() == Some(OsStr::new(name)) {
            return Ok(path);
        }
    }
    Err(format!("{name} is not listed in GANG_CHECKED_IN_CONFIGS").into())
}

/// Makes `gang server` read a copy of `gang.local.toml` rewritten by `rewrite`
/// and asserts that it is rejected before serving anything.
fn assert_server_rejects(rewrite: impl FnOnce(&str) -> String) -> TestResult {
    let dir = tempfile::tempdir()?;
    let original = std::fs::read_to_string(checked_in_config("gang.local.toml")?)?;
    let config = dir.path().join("gang.toml");
    std::fs::write(&config, rewrite(&original))?;

    let out = output_within(
        gang_command(dir.path())?
            .arg("server")
            .arg("--config")
            .arg(&config),
        CHILD_LIMIT,
    )?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        !stdout.contains("ca-pin:") && !stdout.contains("plaintext:"),
        "a rejected configuration must not serve; stdout: {stdout}"
    );
    assert!(
        stderr.trim_start().starts_with("fail"),
        "stderr must start with a fail record; stderr: {stderr}"
    );
    Ok(())
}

/// Turns the loopback listener of `gang.local.toml` into a public one and
/// drops its endpoint.
fn public_listen_without_endpoint(text: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("endpoint = ") {
            continue;
        }
        if trimmed.starts_with("listen = ") {
            out.push_str("listen = \"0.0.0.0:9443\"\n");
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

#[test]
fn gang_binary_has_no_elf_interpreter() -> TestResult {
    let bytes = std::fs::read(required_path("GANG_BIN")?)?;
    let file = ElfFile64::<Endianness>::parse(&bytes[..])?;
    let endian = file.endian();
    for ph in file.elf_program_headers() {
        assert_ne!(
            ph.p_type(endian),
            elf::PT_INTERP,
            "gang must be static: it has a PT_INTERP"
        );
    }
    Ok(())
}

#[test]
fn gang_binary_needs_no_shared_library() -> TestResult {
    let bytes = std::fs::read(required_path("GANG_BIN")?)?;
    let file = ElfFile64::<Endianness>::parse(&bytes[..])?;
    let endian = file.endian();
    for ph in file.elf_program_headers() {
        if let Some(dynamics) = ph.dynamic(endian, &bytes[..])? {
            for entry in dynamics {
                assert_ne!(
                    entry.tag(endian),
                    elf::DT_NEEDED,
                    "gang must be static: it has a DT_NEEDED entry"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn preload_library_runs_in_a_dynamic_process() -> TestResult {
    let dir = tempfile::tempdir()?;
    let marker = dir.path().join("control");
    status_within(
        Command::new(required_path("LOADED_PROBE")?)
            .env_clear()
            .env("LD_PRELOAD", required_path("PRELOAD_LIB")?)
            .env("PRELOAD_MARKER", &marker),
        CHILD_LIMIT,
    )?;
    assert!(
        marker.try_exists()?,
        "PRELOAD_LIB constructor did not run in a dynamically linked process"
    );
    Ok(())
}

#[test]
fn gang_exec_ignores_loader_environment() -> TestResult {
    let dir = tempfile::tempdir()?;
    let exec = dir.path().join("gang-exec");
    std::os::unix::fs::symlink(required_path("GANG_BIN")?, &exec)?;
    let marker = dir.path().join("exec-marker");
    // Expected to fail (no sandbox file is provided); only the spawn and the
    // absence of the marker matter here.
    status_within(
        Command::new(&exec)
            .arg("true")
            .env_clear()
            .env("LD_PRELOAD", required_path("PRELOAD_LIB")?)
            .env("PRELOAD_MARKER", &marker),
        CHILD_LIMIT,
    )?;
    assert!(
        !marker.try_exists()?,
        "gang-exec executed code injected via LD_PRELOAD"
    );
    Ok(())
}

/// The release binary reports Cargo.toml's workspace version: Bazel takes it
/// from there, so the two cannot drift.
#[test]
fn short_version_is_the_cargo_workspace_version() -> TestResult {
    let manifest: toml::Table =
        std::fs::read_to_string(required_path("WORKSPACE_MANIFEST")?)?.parse()?;
    let version = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("package"))
        .and_then(|package| package.get("version"))
        .and_then(toml::Value::as_str)
        .ok_or("cargo.toml has no [workspace.package] version")?;
    let home = tempfile::tempdir()?;
    let output = output_within(gang_command(home.path())?.arg("-V"), CHILD_LIMIT)?;
    assert!(output.status.success(), "gang -V failed: {output:?}");
    assert_eq!(
        String::from_utf8(output.stdout)?.trim(),
        format!("gang {version}")
    );
    Ok(())
}

#[test]
fn usage_errors_exit_with_code_two() -> TestResult {
    let dir = tempfile::tempdir()?;
    for args in [&["bogus"][..], &["revoke"][..]] {
        let out = output_within(gang_command(dir.path())?.args(args), CHILD_LIMIT)?;
        assert_eq!(out.status.code(), Some(2), "gang {args:?}");
    }
    Ok(())
}

#[test]
fn doctor_without_a_server_exits_one_naming_the_command() -> TestResult {
    let dir = tempfile::tempdir()?;
    let out = output_within(gang_command(dir.path())?.arg("doctor"), CHILD_LIMIT)?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("fail") && stderr.contains("gang doctor:"),
        "stderr must name the command; stderr: {stderr}"
    );
    Ok(())
}

#[test]
fn quiet_keeps_failures() -> TestResult {
    let dir = tempfile::tempdir()?;
    let out = output_within(
        gang_command(dir.path())?.arg("-q").arg("doctor"),
        CHILD_LIMIT,
    )?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(out.stdout.is_empty(), "quiet run wrote to stdout");
    assert!(
        stderr.contains("fail"),
        "quiet run must keep failures; stderr: {stderr}"
    );
    Ok(())
}

#[test]
fn colour_follows_the_environment_on_stderr() -> TestResult {
    let dir = tempfile::tempdir()?;

    let plain = output_within(
        gang_command(dir.path())?.env("NO_COLOR", "1").arg("doctor"),
        CHILD_LIMIT,
    )?;
    let stderr = String::from_utf8_lossy(&plain.stderr);
    assert!(
        !stderr.contains('\u{1b}'),
        "NO_COLOR must suppress escape sequences; stderr: {stderr}"
    );

    let forced = output_within(
        gang_command(dir.path())?
            .env("CLICOLOR_FORCE", "1")
            .arg("doctor"),
        CHILD_LIMIT,
    )?;
    let stderr = String::from_utf8_lossy(&forced.stderr);
    assert!(
        stderr.contains("\u{1b}["),
        "CLICOLOR_FORCE must enable escape sequences; stderr: {stderr}"
    );
    Ok(())
}

#[test]
fn a_configuration_with_an_unknown_sandbox_is_rejected() -> TestResult {
    assert_server_rejects(|text| format!("{text}\n[sandbox.bogus]\n"))
}

#[test]
fn a_public_listen_without_an_endpoint_is_rejected() -> TestResult {
    assert_server_rejects(public_listen_without_endpoint)
}
