//! The administrator of a deployment: the identity that revokes and restores principals and reads
//! the scaling history.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use gang_types::{CaPin, Url};
use parking_lot::Mutex;

use crate::errors::{E2eError, Result};
use crate::event_log::EventFollower;
use crate::gang_cli::GangCli;
use crate::identity::{Credentials, Issuance};
use crate::issuer::{TestIssuer, TokenSpec};
use crate::process;
use crate::secured::log_in;

/// The subject of the administrator's token. The deployment template makes provider `e2e`
/// principal `administrator` the server's administrator.
pub const ADMINISTRATOR: &str = "administrator";

/// The administrator of a deployment. It logs in with a token of its own on first use, then it
/// revokes and restores principals through the `gang` client and reads the scaling history from
/// the server API with its bearer token.
pub struct Administrator<'a> {
    /// The `gang` client with the administrator's home directory.
    cli: GangCli,
    /// The in-process issuer that vouches for the administrator's token.
    issuer: &'a TestIssuer,
    /// The HTTPS URL of the server's public API.
    api: Url,
    /// The workspace in which the administrator's login writes the Gang CA certificate.
    workspace: PathBuf,
    /// Maximum time for each administrative command.
    command_timeout: Duration,
    /// The administrator's credentials, once logged in.
    credentials: Mutex<Option<Credentials>>,
}

impl<'a> Administrator<'a> {
    /// An administrator that acts through `cli`, logs in in `workspace`, and reads the API at
    /// `api`, with each command bounded by `command_timeout`.
    pub(crate) fn new(
        cli: GangCli,
        issuer: &'a TestIssuer,
        api: Url,
        workspace: PathBuf,
        command_timeout: Duration,
    ) -> Self {
        Self {
            cli,
            issuer,
            api,
            workspace,
            command_timeout,
            credentials: Mutex::new(None),
        }
    }

    /// Logs the administrator in, if it is not logged in yet, so that its credentials are stored
    /// before any connection is opened on its behalf.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the administrator cannot log in.
    pub(crate) fn enroll(&self, pin: &CaPin) -> Result<()> {
        self.credentials(pin)?;
        Ok(())
    }

    /// Revokes the principal `principal` using the stored login, logging in first.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the administrator cannot log in, and [`E2eError::Command`] when
    /// `gang revoke` does not succeed.
    pub(crate) fn revoke(&self, pin: &CaPin, principal: &str) -> Result<()> {
        self.credentials(pin)?;
        Self::administer(
            self.cli.revoke_principal(principal),
            "gang revoke",
            self.command_timeout,
        )
    }

    /// Restores the principal `principal` using the stored login, logging in first.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the administrator cannot log in, and [`E2eError::Command`] when
    /// `gang restore` does not succeed.
    pub(crate) fn restore(&self, pin: &CaPin, principal: &str) -> Result<()> {
        self.credentials(pin)?;
        Self::administer(
            self.cli.restore_principal(principal),
            "gang restore",
            self.command_timeout,
        )
    }

    /// A follower of the server's scaling events, reached with the administrator's bearer token.
    /// Logs the administrator in first when it is not logged in yet. `handshake_timeout`
    /// bounds connection setup and response headers.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the administrator cannot log in or the events cannot be read;
    /// [`E2eError::Timeout`] when connection setup or response headers take too long.
    pub(crate) fn event_follower(
        &self,
        pin: &CaPin,
        handshake_timeout: Duration,
    ) -> Result<EventFollower> {
        let credentials = self.credentials(pin)?;
        EventFollower::open(&self.api, &credentials, handshake_timeout)
    }

    /// Runs one administrative `command`, which must succeed.
    ///
    /// # Errors
    ///
    /// [`E2eError::Command`] named `program` when the command does not succeed, or
    /// [`E2eError::Timeout`] when it exceeds `timeout`.
    fn administer(command: Command, program: &str, timeout: Duration) -> Result<()> {
        let captured = process::run(command, None, timeout)?;
        if captured.succeeded() {
            Ok(())
        } else {
            Err(E2eError::Command {
                program: program.to_owned(),
                outcome: captured.code.map_or_else(
                    || "terminated by a signal".to_owned(),
                    |code| format!("exited with status {code}"),
                ),
                output: captured.combined(),
            })
        }
    }

    /// The administrator's credentials. The first call mints a token for [`ADMINISTRATOR`] and
    /// logs in with it in the administrator's home; later calls return the cached credentials.
    ///
    /// # Errors
    ///
    /// [`E2eError::System`] when the server refuses the administrator or the login fails.
    pub(crate) fn credentials(&self, pin: &CaPin) -> Result<Credentials> {
        let mut cached = self.credentials.lock();
        if let Some(credentials) = cached.as_ref() {
            return Ok(credentials.clone());
        }
        match log_in(
            &self.cli,
            self.issuer,
            &TokenSpec::admitted(ADMINISTRATOR),
            &self.api,
            pin,
            &self.workspace,
            self.command_timeout,
        )? {
            Issuance::Issued(credentials) => {
                *cached = Some(credentials.clone());
                Ok(credentials)
            }
            Issuance::Refused(output) => Err(E2eError::system(format!(
                "the server refused the administrator: {output}"
            ))),
        }
    }
}
