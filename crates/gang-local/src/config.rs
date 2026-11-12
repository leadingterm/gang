use gang_types::{AbsolutePathBuf, NonZeroDuration};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use thiserror::Error;

/// Settings for the `Local` provisioner plugin, read from the `[provisioner.local]` table of the gang config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// Absolute path to the root of provisioner state: instance records, scratch dirs, logs, and the workload signing key.
    pub state_dir: AbsolutePathBuf,
    /// Loopback listen address with a non-zero port. It is also the OIDC issuer `http://{mock_imds_listen}`.
    pub mock_imds_listen: LoopbackAddr,
    /// Audience and lifetime of the workload JWTs minted by this provisioner.
    #[serde(default)]
    pub workload_token: gang_types::WorkloadTokenSettings,
    /// Bound on each mock IMDS request made by a worker. Default `5s`.
    #[serde(default = "default_imds_request_timeout")]
    pub imds_request_timeout: NonZeroDuration,
    /// How long the fleet waits before it launches replacements after a worker died within this
    /// long of its launch; stops a worker that crashes at start from being relaunched on every
    /// pass. Default `30s`.
    #[serde(default = "default_relaunch_backoff")]
    pub relaunch_backoff: NonZeroDuration,
    /// Argv prefix prepended to the worker command line, so that each worker can run under a wrapper,
    /// for example `["systemd-run", "--user", "--scope", "-p", "Delegate=yes", "--"]`, which gives each
    /// worker its own delegated cgroup subtree. The launcher must exec the command that follows it in
    /// place (same process, pid, process group and standard input) rather than fork it. Default empty:
    /// the worker is run directly.
    #[serde(default)]
    pub worker_launcher: WorkerLauncher,
    /// How long a worker that was sent SIGTERM may keep running before the fleet kills its process
    /// group with SIGKILL. Default `10s`.
    #[serde(default = "default_terminate_grace")]
    pub terminate_grace: NonZeroDuration,
}

/// A socket address on a loopback interface with a non-zero port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SocketAddr", into = "SocketAddr")]
pub struct LoopbackAddr(SocketAddr);

impl LoopbackAddr {
    /// The wrapped socket address.
    #[must_use]
    pub const fn get(self) -> SocketAddr {
        self.0
    }
}

impl TryFrom<SocketAddr> for LoopbackAddr {
    type Error = LocalConfigError;

    fn try_from(addr: SocketAddr) -> Result<Self, Self::Error> {
        if !addr.ip().is_loopback() {
            return Err(LocalConfigError::NonLoopbackImds);
        }
        if addr.port() == 0 {
            return Err(LocalConfigError::EphemeralImdsPort);
        }
        Ok(Self(addr))
    }
}

impl From<LoopbackAddr> for SocketAddr {
    fn from(addr: LoopbackAddr) -> Self {
        addr.0
    }
}

/// The argv prefix that runs each worker, validated so that it names a program and has no empty arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct WorkerLauncher(Vec<String>);

impl WorkerLauncher {
    /// The launcher argv; empty when the worker is run directly.
    #[must_use]
    pub fn argv(&self) -> &[String] {
        &self.0
    }
}

impl TryFrom<Vec<String>> for WorkerLauncher {
    type Error = LocalConfigError;

    fn try_from(argv: Vec<String>) -> Result<Self, Self::Error> {
        if argv.first().is_some_and(String::is_empty) {
            return Err(LocalConfigError::LauncherWithoutProgram);
        }
        if argv.iter().any(String::is_empty) {
            return Err(LocalConfigError::LauncherEmptyArgument);
        }
        Ok(Self(argv))
    }
}

impl From<WorkerLauncher> for Vec<String> {
    fn from(launcher: WorkerLauncher) -> Self {
        launcher.0
    }
}

/// Reasons a `[provisioner.local]` value is rejected while it is parsed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LocalConfigError {
    /// The mock IMDS address is not a loopback address.
    #[error("mock_imds_listen must be a loopback address")]
    NonLoopbackImds,
    /// The mock IMDS address uses an ephemeral port.
    #[error("mock_imds_listen must name a fixed port, because it is the workload token issuer")]
    EphemeralImdsPort,
    /// The first element of the worker launcher is empty.
    #[error("worker_launcher must name a program in its first element")]
    LauncherWithoutProgram,
    /// A later element of the worker launcher is empty.
    #[error("worker_launcher must not contain an empty argument")]
    LauncherEmptyArgument,
}

/// The local provisioner has no fleet options: any key in `[fleets.options]` is rejected.
#[derive(Debug, Deserialize, gang_spi::FleetOptions)]
#[serde(deny_unknown_fields)]
pub struct LocalFleetOptions {}

fn default_imds_request_timeout() -> NonZeroDuration {
    NonZeroDuration::from_secs(5)
}

fn default_relaunch_backoff() -> NonZeroDuration {
    NonZeroDuration::from_secs(30)
}

fn default_terminate_grace() -> NonZeroDuration {
    NonZeroDuration::from_secs(10)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A config with only the required keys, so each test sets just the key it exercises.
    fn minimal() -> serde_json::Value {
        json!({
            "state_dir": "/var/lib/gang",
            "mock_imds_listen": "127.0.0.1:9000",
        })
    }

    /// Parses the minimal config with one extra key set.
    fn with(key: &str, value: serde_json::Value) -> serde_json::Result<LocalConfig> {
        let mut object = minimal();
        if let Some(map) = object.as_object_mut() {
            map.insert(key.to_owned(), value);
        }
        serde_json::from_value(object)
    }

    #[test]
    fn rejects_relative_state_dir() {
        assert!(with("state_dir", json!("relative/state")).is_err());
    }

    #[test]
    fn parses_human_durations_for_the_tunables() -> Result<(), Box<dyn std::error::Error>> {
        let config = with("relaunch_backoff", json!("1m"))?;
        assert_eq!(config.relaunch_backoff, NonZeroDuration::from_mins(1));

        let config = with("imds_request_timeout", json!("90s"))?;
        assert_eq!(config.imds_request_timeout, NonZeroDuration::from_secs(90));

        let config = with("terminate_grace", json!("30s"))?;
        assert_eq!(config.terminate_grace, NonZeroDuration::from_secs(30));
        Ok(())
    }

    #[test]
    fn rejects_zero_durations() {
        for key in [
            "imds_request_timeout",
            "relaunch_backoff",
            "terminate_grace",
        ] {
            assert!(with(key, json!("0s")).is_err(), "{key} accepted zero");
        }
    }

    #[test]
    fn accepts_loopback_addresses_and_rejects_others() -> Result<(), Box<dyn std::error::Error>> {
        let v4 = with("mock_imds_listen", json!("127.0.0.2:9000"))?;
        assert_eq!(
            v4.mock_imds_listen.get(),
            "127.0.0.2:9000".parse::<SocketAddr>()?
        );

        let v6: LocalConfig = serde_json::from_value(json!({
            "state_dir": "/var/lib/gang",
            "mock_imds_listen": "[::1]:9000",
        }))?;
        assert_eq!(
            v6.mock_imds_listen.get(),
            "[::1]:9000".parse::<SocketAddr>()?
        );

        assert!(with("mock_imds_listen", json!("10.0.0.1:9000")).is_err());
        assert!(with("mock_imds_listen", json!("[2001:db8::1]:9000")).is_err());
        Ok(())
    }

    #[test]
    fn rejects_ephemeral_loopback_ports() -> Result<(), Box<dyn std::error::Error>> {
        for addr in ["127.0.0.1:0", "[::1]:0"] {
            let error =
                with("mock_imds_listen", json!(addr)).expect_err("ephemeral port must be rejected");
            assert_eq!(
                error.to_string(),
                LocalConfigError::EphemeralImdsPort.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn workload_token_rejects_a_ttl_below_a_minute() {
        assert!(with("workload_token", json!({"ttl": "30s"})).is_err());
    }

    #[test]
    fn workload_token_rejects_an_empty_audience() {
        assert!(with("workload_token", json!({"audience": ""})).is_err());
    }

    #[test]
    fn workload_token_parses_a_custom_audience_and_ttl() -> Result<(), Box<dyn std::error::Error>> {
        let config = with("workload_token", json!({"audience": "other", "ttl": "45m"}))?;
        assert_eq!(
            config.workload_token,
            serde_json::from_value::<gang_types::WorkloadTokenSettings>(
                json!({"audience": "other", "ttl": "45m"})
            )?
        );
        Ok(())
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(with("unexpected", json!(true)).is_err());
    }

    #[test]
    fn fleet_options_accept_no_keys() {
        assert!(serde_json::from_value::<LocalFleetOptions>(json!({})).is_ok());
        assert!(serde_json::from_value::<LocalFleetOptions>(json!({"anything": 1})).is_err());
    }

    #[test]
    fn worker_launcher_parses_as_argv() -> Result<(), Box<dyn std::error::Error>> {
        let config = with(
            "worker_launcher",
            json!([
                "systemd-run",
                "--user",
                "--scope",
                "-p",
                "Delegate=yes",
                "--"
            ]),
        )?;
        assert_eq!(
            config.worker_launcher.argv(),
            [
                "systemd-run",
                "--user",
                "--scope",
                "-p",
                "Delegate=yes",
                "--"
            ]
        );
        Ok(())
    }

    #[test]
    fn worker_launcher_rejects_an_empty_program() {
        assert!(with("worker_launcher", json!([""])).is_err());
        assert!(with("worker_launcher", json!(["", "--"])).is_err());
    }

    #[test]
    fn worker_launcher_rejects_an_empty_argument() {
        assert!(with("worker_launcher", json!(["systemd-run", ""])).is_err());
    }

    #[test]
    fn round_trips_through_serde() -> Result<(), Box<dyn std::error::Error>> {
        let config = with("worker_launcher", json!(["systemd-run", "--"]))?;
        let value = serde_json::to_value(&config)?;
        let parsed: LocalConfig = serde_json::from_value(value)?;
        assert_eq!(parsed, config);
        assert_eq!(parsed.relaunch_backoff, NonZeroDuration::from_secs(30));
        Ok(())
    }
}
