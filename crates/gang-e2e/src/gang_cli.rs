//! Command builders for the `gang` binary under test.
//!
//! The command-line shapes here are the contract with the `gang` binary.

use std::path::{Path, PathBuf};
use std::process::Command;

use gang_types::{CaPin, Url};

/// The `gang` binary under test, with the configuration and home directory its commands use.
#[derive(Debug, Clone)]
pub struct GangCli {
    binary: PathBuf,
    config: PathBuf,
    home: PathBuf,
}

impl GangCli {
    /// Describes the `gang` binary at `binary`, which uses the configuration at `config` and keeps
    /// each identity's credential store under `home`.
    #[must_use]
    pub fn new(binary: PathBuf, config: PathBuf, home: PathBuf) -> Self {
        Self {
            binary,
            config,
            home,
        }
    }

    /// The `server` command, which serves until it is stopped.
    #[must_use]
    pub fn server(&self) -> Command {
        let mut command = self.command();
        command.arg("server").arg("--config").arg(&self.config);
        command
    }

    /// The `login` command for a workload, which reads its token from the file at `token`.
    ///
    /// `ca_pin` is the pin of the certificate authority the server's certificate chains to, and
    /// the login is made with the identity provider [`crate::template::PROVIDER`].
    #[must_use]
    pub fn login_workload(&self, server: &Url, ca_pin: &CaPin, token: &Path) -> Command {
        let mut command = self.command();
        command
            .arg("login")
            .arg("--server")
            .arg(server.as_str())
            .arg("--ca-pin")
            .arg(ca_pin.to_string())
            .arg("--workload")
            .arg("--provider")
            .arg(crate::template::PROVIDER)
            .arg("--token")
            .arg(token);
        command
    }

    /// The `bazelrc` command, which prints the Bazel rc text of the deployment at `server` on
    /// standard output. When the server has a certificate authority, the command also writes
    /// `gang-ca.pem` into `workspace`, so it runs there.
    #[must_use]
    pub fn bazelrc(&self, server: &Url, ca_pin: Option<&CaPin>, workspace: &Path) -> Command {
        let mut command = self.command();
        command
            .current_dir(workspace)
            .arg("bazelrc")
            .arg("--server")
            .arg(server.as_str())
            .arg("--helper")
            .arg(&self.binary);
        if let Some(pin) = ca_pin {
            command.arg("--ca-pin").arg(pin.to_string());
        }
        command
    }

    /// The `revoke` command for the principal `provider:principal`, which revokes it: its
    /// sessions are refused on their next request and new logins are blocked.
    #[must_use]
    pub fn revoke_principal(&self, principal: &str) -> Command {
        let mut command = self.command();
        command.arg("revoke").arg("--principal").arg(principal);
        command
    }

    /// The `restore` command for the principal `provider:principal`, which lets it log in and
    /// be served again.
    #[must_use]
    pub fn restore_principal(&self, principal: &str) -> Command {
        let mut command = self.command();
        command.arg("restore").arg("--principal").arg(principal);
        command
    }

    /// The `get` command, which Bazel runs as its credential helper: it reads a request on
    /// standard input and prints the headers to send, with the bearer token among them.
    #[must_use]
    pub fn get(&self) -> Command {
        let mut command = self.command();
        command.arg("get");
        command
    }

    /// The client home directory of this identity.
    #[must_use]
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// A command for the binary with the home directory of this identity.
    fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        client_home(&mut command, &self.home);
        command
    }
}

/// Isolates a client's home and prevents CI's shared temporary credential directory from leaking in.
pub(crate) fn client_home(command: &mut Command, home: &Path) {
    command
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home)
        .env_remove("RUNNER_TEMP");
}
