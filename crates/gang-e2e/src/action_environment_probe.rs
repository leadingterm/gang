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
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let tmp = cwd
        .parent()
        .ok_or_else(|| "action working directory has no parent".to_owned())?
        .join("tmp");
    validate_environment(&environment, &cwd, &tmp, marker)?;
    if !tmp.is_dir() {
        return Err("action temporary directory does not exist".to_owned());
    }
    std::fs::write(output, b"ok\n").map_err(|error| error.to_string())
}

fn validate_environment(
    environment: &BTreeMap<OsString, OsString>,
    cwd: &Path,
    tmp: &Path,
    marker: &OsStr,
) -> Result<(), String> {
    let expected = ["GANG_E2E_EXACT_ENV", "GANG_MEMORY_KB", "PWD", "TMPDIR"];
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
    let memory = environment
        .get(OsStr::new("GANG_MEMORY_KB"))
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<u64>().ok());
    if environment
        .get(OsStr::new("GANG_E2E_EXACT_ENV"))
        .map(OsString::as_os_str)
        != Some(marker)
        || memory.is_none_or(|value| value == 0)
        || environment.get(OsStr::new("PWD")).map(OsString::as_os_str) != Some(cwd.as_os_str())
        || environment
            .get(OsStr::new("TMPDIR"))
            .map(OsString::as_os_str)
            != Some(tmp.as_os_str())
    {
        return Err("unexpected action environment values".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment() -> BTreeMap<OsString, OsString> {
        [
            ("GANG_E2E_EXACT_ENV", "declared-marker"),
            ("GANG_MEMORY_KB", "1024"),
            ("PWD", "/action/work"),
            ("TMPDIR", "/action/tmp"),
        ]
        .into_iter()
        .map(|(key, value)| (OsString::from(key), OsString::from(value)))
        .collect()
    }

    fn validate(environment: &BTreeMap<OsString, OsString>) -> Result<(), String> {
        validate_environment(
            environment,
            Path::new("/action/work"),
            Path::new("/action/tmp"),
            OsStr::new("declared-marker"),
        )
    }

    #[test]
    fn accepts_exact_action_environment() {
        assert!(validate(&environment()).is_ok());
    }

    #[test]
    fn rejects_unrequested_reserved_and_ambient_keys() {
        for key in [
            "GANG_CPU_COUNT",
            "GANG_NETWORK",
            "HOME",
            "PATH",
            "LD_LIBRARY_PATH",
        ] {
            let mut values = environment();
            values.insert(OsString::from(key), OsString::new());
            assert!(validate(&values).is_err(), "accepted extra key {key}");
        }
        let mut values = environment();
        values.insert(OsString::from("GANG_CPU_COUNT"), OsString::new());
        values.insert(OsString::from("GANG_NETWORK"), OsString::new());
        assert!(validate(&values).is_err());
    }

    #[test]
    fn rejects_missing_or_invalid_memory() {
        for value in ["", "0", "-1", "not-a-number", "18446744073709551616"] {
            let mut values = environment();
            values.insert(OsString::from("GANG_MEMORY_KB"), OsString::from(value));
            assert!(validate(&values).is_err(), "accepted memory {value:?}");
        }
        let mut values = environment();
        values.remove(OsStr::new("GANG_MEMORY_KB"));
        assert!(validate(&values).is_err());
    }

    #[test]
    fn rejects_wrong_marker_and_runtime_paths() {
        for (key, value) in [
            ("GANG_E2E_EXACT_ENV", "wrong-marker"),
            ("PWD", "/action/other"),
            ("TMPDIR", "/action/other"),
        ] {
            let mut values = environment();
            values.insert(OsString::from(key), OsString::from(value));
            assert!(validate(&values).is_err(), "accepted wrong {key}");
        }
    }
}
