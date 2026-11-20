//! Re-executed probes of the action's native identity and trust environment.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::{File, TryLockError};
use std::path::Path;
use std::process::{Command, ExitCode};
use std::sync::Arc;

/// Runs a native roots, `PostgreSQL`, or nonblocking lock-witness action probe.
#[must_use]
pub fn action_environment_probe(arguments: &[OsString]) -> ExitCode {
    let result = match arguments {
        [mode] if mode == "roots" => gang_tls::client_config(
            gang_tls::ServerTrust::System,
            None,
            &[],
            Arc::new(gang_clock::SystemClock::new()),
        )
        .map(|_| ())
        .map_err(|error| error.to_string()),
        [mode, archive] if mode == "postgres" => {
            gang_testkit::PostgresServer::start(Path::new(archive))
                .map(drop)
                .map_err(|error| error.to_string())
        }
        [mode, output, marker] if mode == "environment" => environment_witness(output, marker),
        [mode, lock, shell, script] if mode == "lock" => {
            let file = File::open(lock).map_err(|error| error.to_string());
            file.and_then(|file| {
                let witness = match file.try_lock() {
                    Ok(()) => "exclusive",
                    Err(TryLockError::WouldBlock) => "contended",
                    Err(error) => return Err(error.to_string()),
                };
                let status = Command::new(shell)
                    .arg("-c")
                    .arg(script)
                    // Inherit the open description so the witness lasts while the child runs.
                    .stdin(file.try_clone().map_err(|error| error.to_string())?)
                    .env("E2E_LOCK_WITNESS", witness)
                    .status()
                    .map_err(|error| error.to_string())?;
                if status.success() {
                    Ok(())
                } else {
                    Err(format!("lock witness command failed: {status}"))
                }
            })
        }
        _ => Err("expected roots, postgres <archive>, or lock <file> <shell> <script>".to_owned()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("action environment probe: {error}");
            ExitCode::FAILURE
        }
    }
}

fn environment_witness(output: &OsStr, marker: &OsStr) -> Result<(), String> {
    let environment: BTreeMap<_, _> = std::env::vars_os().collect();
    let expected = [
        "GANG_CPU_COUNT",
        "GANG_E2E_EXACT_ENV",
        "GANG_MEMORY_KB",
        "GANG_NETWORK",
        "PWD",
        "TMPDIR",
    ];
    if environment
        .keys()
        .map(OsString::as_os_str)
        .ne(expected.into_iter().map(OsStr::new))
    {
        return Err(format!(
            "unexpected environment keys: {:?}",
            environment.keys()
        ));
    }
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let tmp = cwd
        .parent()
        .ok_or_else(|| "action working directory has no parent".to_owned())?
        .join("tmp");
    if environment
        .get(OsStr::new("GANG_E2E_EXACT_ENV"))
        .map(OsString::as_os_str)
        != Some(marker)
        || environment
            .get(OsStr::new("GANG_NETWORK"))
            .is_none_or(|v| !v.is_empty())
        || ["GANG_CPU_COUNT", "GANG_MEMORY_KB"].iter().any(|key| {
            environment.get(OsStr::new(key)).is_none_or(|value| {
                value.to_str().is_none_or(|text| {
                    !text.is_empty() && !text.bytes().all(|byte| byte.is_ascii_digit())
                })
            })
        })
        || environment.get(OsStr::new("PWD")).map(OsString::as_os_str) != Some(cwd.as_os_str())
        || environment
            .get(OsStr::new("TMPDIR"))
            .map(OsString::as_os_str)
            != Some(tmp.as_os_str())
        || !tmp.is_dir()
    {
        return Err("unexpected action environment values".to_owned());
    }
    std::fs::write(output, b"ok\n").map_err(|error| error.to_string())
}
