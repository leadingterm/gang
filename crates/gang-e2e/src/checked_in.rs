//! The checked-in-configs scenario: every `configs/*.toml` the repository ships is moved into a
//! placement of its own and run with `gang server`. The local ones start and announce themselves;
//! the AWS one cannot reach AWS here, so it must pass configuration loading and stop only at its
//! first AWS call.

use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::Command;

use gang_config::Document;
use gang_main::plugins::{CacheConfig, Plugins, ProvisionerConfig, SignerConfig};
use gang_types::announcement::Announcement;

use crate::Ran;
use crate::check::Check;
use crate::cli::{failure, outcome};
use crate::cli_terminal::{Finished, run_until};
use crate::errors::{E2eError, Result};
use crate::names;
use crate::placement::Placement;
use crate::stack::is_announcement;
use crate::suite::Invocation;
use crate::template::{self, Transport};

/// One checked-in configuration: its file name in `configs/` and whether it needs AWS to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShippedConfig {
    /// The file name, such as `gang.local.toml`.
    pub name: String,
    /// Whether its provisioner, cache or signer is an AWS plugin.
    pub needs_aws: bool,
}

/// How `gang server` ran on a checked-in configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Start {
    /// It announced itself: its standard output lines through its startup line. It was stopped.
    Announced(Vec<String>),
    /// It exited before announcing.
    Exited(Ran),
}

/// The checked-in configurations as the checked-in-configs scenario starts and observes them.
pub trait Shipped {
    /// The checked-in configurations, sorted by name.
    ///
    /// # Errors
    ///
    /// When the directory or a file cannot be read or parsed.
    fn configs(&self) -> Result<Vec<ShippedConfig>>;

    /// Starts `gang server` on `config` moved into a fresh placement, and stops it once it
    /// announced itself or exited.
    ///
    /// # Errors
    ///
    /// When the configuration cannot be placed or the server cannot be started, or
    /// [`E2eError::Timeout`] once the harness deadline passed.
    fn start(&self, config: &ShippedConfig) -> Result<Start>;
}

/// Every `configs/*.toml` runs: one checked-in configuration runs locally, each local one starts
/// `gang server` and announces itself, and each AWS one passes configuration loading and stops
/// only for want of AWS.
///
/// # Errors
///
/// When the configurations cannot be listed, read or parsed, or a server cannot be started.
pub fn checked_in_configs<S: Shipped>(shipped: &S) -> Result<Vec<Check>> {
    let configs = shipped.configs()?;
    let found: Vec<&str> = configs.iter().map(|config| config.name.as_str()).collect();
    let mut checks = vec![Check::expect(
        names::checked_in_configs::FOUND,
        configs.iter().any(|config| !config.needs_aws),
        || format!("found {found:?}"),
    )];
    for config in &configs {
        let name = config.name.as_str();
        match (config.needs_aws, shipped.start(config)?) {
            (false, Start::Announced(lines)) => checks.push(Check::expect(
                names::checked_in_configs::starts(name),
                lines.last().is_some_and(|line| is_startup(line)),
                || format!("it printed {lines:?}"),
            )),
            (false, Start::Exited(ran)) => checks.push(Check::failed(
                names::checked_in_configs::starts(name),
                outcome(&ran),
            )),
            (true, Start::Exited(ran)) => checks.push(Check::expect(
                names::checked_in_configs::loads(name),
                ran.code == Some(1)
                    && failure(&ran.stderr, "gang server").is_some_and(|subject| {
                        !subject.starts_with("invalid configuration")
                            && !subject.starts_with("cannot read the configuration")
                    }),
                || outcome(&ran),
            )),
            (true, Start::Announced(lines)) => checks.push(Check::failed(
                names::checked_in_configs::loads(name),
                format!("it announced itself without AWS: {lines:?}"),
            )),
        }
    }
    Ok(checks)
}

/// Whether `line` is the startup line of a server: its CA pin, or its plaintext endpoint.
fn is_startup(line: &str) -> bool {
    matches!(
        line.trim().parse::<Announcement>(),
        Ok(Announcement::CaPin(_) | Announcement::Plaintext(_)),
    )
}

/// `document` moved into `placement`: both listeners on ephemeral loopback ports with the endpoint
/// and the scheduler's worker endpoint derived from them, the local state, cache, signer key,
/// database and noop scratch under the placement, the mock IMDS on the placement's reserved port
/// together with every identity provider that trusted the old one, and a dashboard client secret
/// file written into the placement. AWS sections are kept.
///
/// # Errors
///
/// An invalid placement path, an IMDS address that is not loopback, or a secret file that cannot
/// be written.
pub(crate) fn retarget(
    mut document: Document<Plugins>,
    placement: &Placement,
) -> Result<Document<Plugins>> {
    template::place(&mut document, placement, Transport::Tls)?;
    let ephemeral = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    document.listen = ephemeral;
    document.worker_listen = ephemeral;
    Ok(document)
}

/// The checked-in configurations of the invocation, each started with the `gang` binary under
/// test.
pub(crate) struct ShippedConfigs<'a> {
    invocation: &'a Invocation,
}

impl<'a> ShippedConfigs<'a> {
    /// The checked-in configurations of `invocation`.
    pub(crate) const fn new(invocation: &'a Invocation) -> Self {
        Self { invocation }
    }
}

/// Parses the checked-in configuration at `path`.
fn parse_shipped(path: &Path) -> Result<Document<Plugins>> {
    let text =
        fs::read_to_string(path).map_err(E2eError::io(format!("reading {}", path.display())))?;
    Document::<Plugins>::parse(&text).map_err(E2eError::parse("a checked-in configuration"))
}

impl Shipped for ShippedConfigs<'_> {
    fn configs(&self) -> Result<Vec<ShippedConfig>> {
        let directory = &self.invocation.configs;
        let entries = fs::read_dir(directory)
            .map_err(E2eError::io(format!("reading {}", directory.display())))?;
        let mut file_names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(E2eError::io(format!("listing {}", directory.display())))?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if Path::new(&file_name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("toml"))
            {
                file_names.push(file_name);
            }
        }
        file_names.sort();
        file_names
            .into_iter()
            .map(|name| -> Result<ShippedConfig> {
                let document = parse_shipped(&directory.join(&name))?;
                let needs_aws = matches!(document.provisioner, ProvisionerConfig::Aws(_))
                    || matches!(document.cache, CacheConfig::S3(_))
                    || matches!(document.signer, SignerConfig::Kms(_));
                Ok(ShippedConfig { name, needs_aws })
            })
            .collect()
    }

    fn start(&self, config: &ShippedConfig) -> Result<Start> {
        let placement = Placement::create(self.invocation.settings.scratch_root.as_deref())?;
        let document = parse_shipped(&self.invocation.configs.join(&config.name))?;
        let document = retarget(document, &placement)?;
        let text = document.to_toml().map_err(E2eError::system_from(
            "rendering a checked-in configuration",
        ))?;
        fs::write(placement.config_file(), text).map_err(E2eError::io(format!(
            "writing {}",
            placement.config_file().display()
        )))?;
        let mut command = Command::new(&self.invocation.gang);
        command
            .arg("server")
            .arg("--config")
            .arg(placement.config_file())
            .env("HOME", placement.home())
            .env("XDG_CONFIG_HOME", placement.home());
        without_aws(&mut command);
        match run_until(
            &mut command,
            self.invocation.settings.history_timeout,
            is_announcement,
        )? {
            Finished::Printed(lines) => Ok(Start::Announced(lines)),
            Finished::Exited(ran) => Ok(Start::Exited(ran)),
        }
    }
}

/// The AWS environment variables a command must not inherit from the host.
const AWS_CREDENTIAL_VARIABLES: [&str; 11] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
];

/// Keeps AWS credentials of the host away from `command` and turns the EC2 metadata lookup off,
/// so an AWS plugin fails at once.
fn without_aws(command: &mut Command) {
    command.env("AWS_EC2_METADATA_DISABLED", "true");
    for name in AWS_CREDENTIAL_VARIABLES {
        command.env_remove(name);
    }
}
