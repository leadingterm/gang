//! Re-executed probes of the action's native identity and trust environment.

use std::ffi::OsString;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

/// Runs the requested native roots or `PostgreSQL` action probe.
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
        _ => Err("expected roots or postgres <archive>".to_owned()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("action environment probe: {error}");
            ExitCode::FAILURE
        }
    }
}
