//! Behaviour of `gang.toml` parsing against a composition of in-test plugins.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use gang_config::{
    CaPinSetting, ClientSecretSource, ConfigError, Document, FleetConfig, FleetMember,
    FleetOptionsError, FleetOptionsTable, FleetPolicy, Fleets, GangConfig, InputLinks,
    InstanceBounds, PluginSet, SandboxFile, SandboxFileError, SandboxSelection, SecretFileError,
    ServerTransport, Transport, WorkerConfig,
};
use gang_types::{
    AbsolutePathBuf, AuthenticationSettings, Bytes, CaPin, ClientSecret, FleetCountError,
    FleetName, Isa, MAX_FLEETS, NonZeroBytes, NonZeroDuration, Percent, Permission, PermissionSet,
    ProviderName, SchedulerSettings, TrustDomain, Url,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AwsSection {
    region: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalSection {
    state_dir: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProvisionerConfig {
    Aws(AwsSection),
    Local(LocalSection),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AwsOptions {
    capacity: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalOptions {}

#[derive(Debug)]
enum FleetOptions {
    Aws(AwsOptions),
    Local(LocalOptions),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilesystemSection {
    path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CacheConfig {
    Filesystem(FilesystemSection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSignerSection {
    key_path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SignerConfig {
    File(FileSignerSection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CgroupSection {
    max_pids: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RlimitSection {
    nofile: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LimitsConfig {
    Cgroup(CgroupSection),
    Rlimit(RlimitSection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeLinkSection {
    scheduler_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SchedulerConfig {
    Nativelink(NativeLinkSection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqliteSection {
    path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DatabaseConfig {
    Sqlite(SqliteSection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoopSection {
    #[serde(with = "gang_types::duration")]
    kill_grace: Duration,
    #[serde(default)]
    program: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BwrapSection {
    permit_network_property: bool,
    #[serde(default)]
    program: PathBuf,
}

/// The section key of each composed sandbox plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SandboxKey {
    #[serde(rename = "noop")]
    Noop,
    #[serde(rename = "bwrap")]
    Bwrap,
}

impl fmt::Display for SandboxKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noop => formatter.write_str("noop"),
            Self::Bwrap => formatter.write_str("bwrap"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxConfig {
    noop: Option<NoopSection>,
    bwrap: Option<BwrapSection>,
}

impl SandboxSelection for SandboxConfig {
    type Key = SandboxKey;

    fn is_configured(&self, key: SandboxKey) -> bool {
        match key {
            SandboxKey::Noop => self.noop.is_some(),
            SandboxKey::Bwrap => self.bwrap.is_some(),
        }
    }

    fn try_for_each_program<E>(
        &mut self,
        key: SandboxKey,
        mut visit: impl FnMut(&mut PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        match key {
            SandboxKey::Noop => match &mut self.noop {
                Some(noop) => visit(&mut noop.program),
                None => Ok(()),
            },
            SandboxKey::Bwrap => match &mut self.bwrap {
                Some(bwrap) => visit(&mut bwrap.program),
                None => Ok(()),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcSection {
    issuer_url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IdentityProviderConfig {
    Oidc(OidcSection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenSection {
    token_ttl: String,
    clock_skew: String,
    max_cached_tokens: u32,
    providers: Vec<ProviderName>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MtlsSection {
    provider: ProviderName,
    certificate_ttl: String,
    max_cached_certificates: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoneSection {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClientAuthenticationConfig {
    Token(TokenSection),
    None(NoneSection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkerAuthenticationConfig {
    Mtls(MtlsSection),
    None(NoneSection),
}

#[derive(Debug)]
struct Plugins;

impl PluginSet for Plugins {
    type Provisioner = ProvisionerConfig;
    type Cache = CacheConfig;
    type Signer = SignerConfig;
    type Scheduler = SchedulerConfig;
    type Database = DatabaseConfig;
    type SandboxKey = SandboxKey;
    type Sandbox = SandboxConfig;
    type IdentityProvider = IdentityProviderConfig;
    type Limits = LimitsConfig;
    type FleetOptions = FleetOptions;
    type ClientAuthentication = ClientAuthenticationConfig;
    type WorkerAuthentication = WorkerAuthenticationConfig;

    fn fleet_options(
        provisioner: &ProvisionerConfig,
        table: FleetOptionsTable,
    ) -> Result<FleetOptions, FleetOptionsError> {
        match provisioner {
            ProvisionerConfig::Aws(_) => table.try_into().map(FleetOptions::Aws),
            ProvisionerConfig::Local(_) => table.try_into().map(FleetOptions::Local),
        }
    }

    /// Rejects the test-only conflicting fleet names in declaration order.
    fn validate_fleets(fleets: &[(&FleetName, &FleetOptions)]) -> Result<(), String> {
        if fleets
            .iter()
            .any(|(name, _)| name.as_str() == "conflict-first")
            && fleets
                .iter()
                .any(|(name, _)| name.as_str() == "conflict-second")
        {
            return Err(format!(
                "conflicting fleets: {}",
                fleets
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        Ok(())
    }

    fn option_properties(options: &FleetOptions) -> Vec<(&'static str, String)> {
        match options {
            FleetOptions::Aws(aws) => vec![("capacity", aws.capacity.clone())],
            FleetOptions::Local(_) => Vec::new(),
        }
    }
}

impl SchedulerSettings for SchedulerConfig {}

impl AuthenticationSettings for ClientAuthenticationConfig {
    fn requires_tls(&self) -> bool {
        false
    }

    fn providers(&self) -> &[ProviderName] {
        match self {
            Self::Token(token) => &token.providers,
            Self::None(_) => &[],
        }
    }
}

impl AuthenticationSettings for WorkerAuthenticationConfig {
    fn requires_tls(&self) -> bool {
        matches!(self, Self::Mtls(_))
    }

    fn providers(&self) -> &[ProviderName] {
        match self {
            Self::Mtls(mtls) => std::slice::from_ref(&mtls.provider),
            Self::None(_) => &[],
        }
    }
}

const TOP: &str = r#"
poll_interval = "500ms"
client_action_timeout = "1m"
default_isa = "x86-64"
listen = "127.0.0.1:9443"
worker_listen = "127.0.0.1:9445"
endpoint = "https://gang.example.com:9443"
"#;

const FLEET: &str = r#"
[[fleets]]
name = "local-worker-x86-64"
isa = "x86-64"
sandbox = "bwrap"
min_instances = 0
max_instances = 4
vcpus_per_instance = 4
memory_per_instance = "8GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 4
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "30s"
"#;

const TWO_FLEETS: &str = r#"
[[fleets]]
name = "amd64-spot"
isa = "x86-64"
sandbox = "bwrap"
min_instances = 0
max_instances = 4
vcpus_per_instance = 8
memory_per_instance = "29GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 2
idle_timeout = "5m"
max_drain_wait = "1h"
boot_grace = "10m"

[fleets.options]
capacity = "spot"

[[fleets]]
name = "arm64-on-demand"
isa = "aarch64"
sandbox = "noop"
min_instances = 1
max_instances = 2
vcpus_per_instance = 4
memory_per_instance = "8GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 1
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "30s"

[fleets.options]
capacity = "on-demand"
"#;

const PROVISIONER: &str = r#"
[provisioner.local]
state_dir = "/tmp/gang-local"
"#;

const AWS_PROVISIONER: &str = r#"
[provisioner.aws]
region = "us-east-1"
"#;

const CACHE: &str = r#"
[cache.filesystem]
path = "/tmp/gang-local/cache"
"#;

const SIGNER: &str = r#"
[signer.file]
key_path = "/tmp/gang-local/ca.pem"
"#;

const SCHEDULER: &str = r#"
[scheduler.nativelink]
scheduler_name = "MAIN_SCHEDULER"
"#;

const DATABASE: &str = r#"
[database.sqlite]
path = "/tmp/gang-local/gang.db"
"#;

const SANDBOX: &str = r#"
[sandbox.noop]
kill_grace = "2s"

[sandbox.bwrap]
permit_network_property = false
"#;

const IDENTITY: &str = r#"
[identity.providers.google.oidc]
issuer_url = "https://accounts.google.com"
"#;

const SERVER: &str = r#"
[server]
trust_domain = "gang.internal"
"#;

const PLAINTEXT_SERVER: &str = r#"
[server]
transport = "plaintext"
"#;

const TRANSPORT_SERVER: &str = r#"
[server]
trust_domain = "gang.internal"
handshake_timeout = "3s"
max_pending_handshakes = 8
accept_error_backoff = "250ms"
"#;

const ZERO_HANDSHAKE_SERVER: &str = r#"
[server]
trust_domain = "gang.internal"
handshake_timeout = "0s"
"#;

const ZERO_PENDING_SERVER: &str = r#"
[server]
trust_domain = "gang.internal"
max_pending_handshakes = 0
"#;

const ZERO_BACKOFF_SERVER: &str = r#"
[server]
trust_domain = "gang.internal"
accept_error_backoff = "0s"
"#;

const AUTHENTICATION: &str = r#"
[authentication.clients.token]
token_ttl = "10m"
clock_skew = "30s"
max_cached_tokens = 4096
providers = ["google"]

[authentication.workers.mtls]
provider = "google"
certificate_ttl = "1d"
max_cached_certificates = 4096
"#;

const PLAINTEXT_AUTHENTICATION: &str = r"
[authentication.clients.none]

[authentication.workers.none]
";

const SANDBOX_FILE: &str = r#"
work_directory = "/var/lib/gang/work"
selected = "bwrap"

[sandbox.noop]
kill_grace = "2s"

[sandbox.bwrap]
permit_network_property = false

[execution]
input_links = "break"

[limits.cgroup]
max_pids = 512
"#;

/// A complete valid file with the given section text replacing the named default.
struct Sections {
    top: &'static str,
    fleet: &'static str,
    provisioner: &'static str,
    cache: &'static str,
    signer: &'static str,
    scheduler: &'static str,
    database: &'static str,
    sandbox: &'static str,
    identity: &'static str,
    authentication: &'static str,
    execution: &'static str,
    limits: &'static str,
    server: &'static str,
    worker: &'static str,
    dashboard: &'static str,
}

impl Sections {
    const VALID: Self = Self {
        top: TOP,
        fleet: FLEET,
        provisioner: PROVISIONER,
        cache: CACHE,
        signer: SIGNER,
        scheduler: SCHEDULER,
        database: DATABASE,
        sandbox: SANDBOX,
        identity: IDENTITY,
        authentication: AUTHENTICATION,
        execution: "",
        limits: "",
        server: SERVER,
        worker: "",
        dashboard: "",
    };

    fn text(&self) -> String {
        [
            self.top,
            self.fleet,
            self.provisioner,
            self.cache,
            self.signer,
            self.scheduler,
            self.database,
            self.sandbox,
            self.identity,
            self.authentication,
            self.execution,
            self.limits,
            self.server,
            self.worker,
            self.dashboard,
        ]
        .concat()
    }

    fn parse(&self) -> Result<GangConfig<Plugins>, ConfigError> {
        self.text().parse()
    }
}

/// The message of `error` followed by the message of each of its sources, joined with `: `, the
/// way a user sees the failure.
fn chain(error: &dyn Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

fn rejection(sections: &Sections) -> Result<String, Box<dyn Error>> {
    match sections.parse() {
        Ok(config) => Err(format!("accepted: {config:?}").into()),
        Err(error) => Ok(chain(&error)),
    }
}

/// The message of a rejection that must come from the TOML layer, failing the test when the
/// error is any other kind.
fn toml_rejection(sections: &Sections) -> Result<String, Box<dyn Error>> {
    match sections.parse() {
        Ok(config) => Err(format!("accepted: {config:?}").into()),
        Err(error @ ConfigError::Gang(_)) => Ok(chain(&error)),
        Err(error) => Err(format!("not a TOML rejection: {error}").into()),
    }
}

/// A `[[fleets]]` entry on the local provisioner with the given name and no options.
fn fleet_block(name: &str) -> String {
    format!(
        "[[fleets]]\nname = \"{name}\"\nisa = \"x86-64\"\nsandbox = \"bwrap\"\nmin_instances = 0\nmax_instances = 1\nvcpus_per_instance = 1\nmemory_per_instance = \"1GiB\"\ndisk_per_instance = \"100GiB\"\ntarget_actions_per_instance = 1\nidle_timeout = \"15s\"\nmax_drain_wait = \"1m\"\nboot_grace = \"30s\"\n"
    )
}

/// The local fleet followed by a `[fleets.options]` table holding the given lines.
fn fleet_with_options(options: &str) -> &'static str {
    format!("{FLEET}[fleets.options]\n{options}").leak()
}

#[test]
fn parses_a_complete_file_into_typed_values() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert_eq!(config.poll_interval, NonZeroDuration::from_millis(500));
    assert_eq!(config.client_action_timeout, NonZeroDuration::from_mins(1));
    assert_eq!(config.default_isa, Isa::X86_64);
    assert_eq!(config.listen.port(), 9443);
    assert_eq!(config.worker_listen.port(), 9445);
    assert_eq!(
        config.endpoint.as_ref().and_then(Url::host_str),
        Some("gang.example.com")
    );
    let fleet = config.fleets.get(0).ok_or("local fleet")?;
    assert_eq!(config.fleets.len(), 1);
    assert_eq!(fleet.policy.name.as_str(), "local-worker-x86-64");
    assert_eq!(fleet.policy.isa, Isa::X86_64);
    assert_eq!(fleet.sandbox, SandboxKey::Bwrap);
    assert_eq!(fleet.policy.memory_per_instance, NonZeroBytes::gib(8));
    assert_eq!(fleet.policy.disk_per_instance, NonZeroBytes::gib(100));
    assert_eq!(fleet.policy.idle_timeout, Duration::from_secs(15));
    assert!(
        matches!(&config.provisioner, ProvisionerConfig::Local(local) if local.state_dir == "/tmp/gang-local")
    );
    assert!(
        matches!(&config.cache, CacheConfig::Filesystem(cache) if cache.path == "/tmp/gang-local/cache")
    );
    assert!(
        matches!(&config.signer, SignerConfig::File(signer) if signer.key_path == "/tmp/gang-local/ca.pem")
    );
    assert!(
        matches!(&config.scheduler, SchedulerConfig::Nativelink(section) if section.scheduler_name == "MAIN_SCHEDULER")
    );
    assert!(
        matches!(&config.database, DatabaseConfig::Sqlite(section) if section.path.ends_with("gang.db"))
    );
    let noop = config.sandbox.noop.ok_or("noop sandbox")?;
    assert_eq!(noop.kill_grace, Duration::from_secs(2));
    assert!(
        config
            .sandbox
            .bwrap
            .is_some_and(|bwrap| !bwrap.permit_network_property)
    );
    assert!(matches!(
        &config.authentication.workers,
        WorkerAuthenticationConfig::Mtls(mtls) if mtls.provider.as_str() == "google"
    ));
    Ok(())
}

#[test]
fn a_document_written_back_parses_to_the_same_configuration() -> Result<(), Box<dyn Error>> {
    let text = Sections::VALID.text();
    let rendered = Document::<Plugins>::parse(&text)?.to_toml()?;
    let original = text.parse::<GangConfig<Plugins>>()?;
    let reparsed = rendered.parse::<GangConfig<Plugins>>()?;

    assert_eq!(reparsed.listen, original.listen);
    assert_eq!(reparsed.poll_interval, original.poll_interval);
    assert_eq!(
        reparsed.client_action_timeout,
        original.client_action_timeout
    );
    assert_eq!(reparsed.server.transport, original.server.transport);
    assert_eq!(reparsed.fleets.len(), original.fleets.len());
    assert_eq!(Document::<Plugins>::parse(&rendered)?.to_toml()?, rendered);
    Ok(())
}

#[test]
fn pinning_a_document_sets_only_the_ca_pin() -> Result<(), Box<dyn Error>> {
    let text = Sections::VALID.text();
    let pin = CaPin::new([0xab; 32]);
    let pinned = Document::<Plugins>::parse(&text)?.pinned(pin).to_toml()?;
    let config = pinned.parse::<GangConfig<Plugins>>()?;
    let original = text.parse::<GangConfig<Plugins>>()?;

    assert_eq!(config.server.ca_pin, Some(CaPinSetting::Pinned(pin)));
    assert_eq!(config.server.transport, original.server.transport);
    assert_eq!(config.listen, original.listen);
    Ok(())
}

#[test]
fn parses_identity_providers_with_their_plugin_section() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert!(matches!(
        config.identity.providers.get("google"),
        Some(IdentityProviderConfig::Oidc(oidc)) if oidc.issuer_url.contains("google")
    ));
    assert_eq!(
        config.identity.providers.keys().collect::<Vec<_>>(),
        [&ProviderName::try_new("google")?]
    );
    Ok(())
}

#[test]
fn parses_the_authentication_sections_into_typed_values() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert!(matches!(
        &config.authentication.clients,
        ClientAuthenticationConfig::Token(token)
            if token.token_ttl == "10m"
                && token.clock_skew == "30s"
                && token.max_cached_tokens == 4096
                && token.providers.len() == 1
    ));
    assert!(matches!(
        &config.authentication.workers,
        WorkerAuthenticationConfig::Mtls(mtls)
            if mtls.provider.as_str() == "google"
                && mtls.certificate_ttl == "1d"
                && mtls.max_cached_certificates == 4096
    ));
    Ok(())
}

#[test]
fn certificate_backdate_defaults_to_one_minute_when_omitted() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert_eq!(config.server.certificate_backdate, Duration::from_mins(1));
    Ok(())
}

#[test]
fn parses_a_custom_certificate_backdate() -> Result<(), Box<dyn Error>> {
    let backdated = SERVER.replace(
        "trust_domain = \"gang.internal\"\n",
        "trust_domain = \"gang.internal\"\ncertificate_backdate = \"2m\"\n",
    );

    let config = Sections {
        server: backdated.leak(),
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.certificate_backdate, Duration::from_mins(2));
    Ok(())
}

#[test]
fn parses_a_ca_pin_from_sha256_text() -> Result<(), Box<dyn Error>> {
    let pinned = SERVER.replace(
        "trust_domain = \"gang.internal\"\n",
        &format!(
            "trust_domain = \"gang.internal\"\nca_pin = \"sha256:{}\"\n",
            "ab".repeat(32)
        ),
    );

    let config = Sections {
        server: pinned.leak(),
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(
        config.server.ca_pin,
        Some(CaPinSetting::Pinned(CaPin::new([0xab; 32])))
    );
    Ok(())
}

#[test]
fn parses_a_signer_ca_pin() -> Result<(), Box<dyn Error>> {
    let signer = SERVER.replace(
        "trust_domain = \"gang.internal\"\n",
        "trust_domain = \"gang.internal\"\nca_pin = \"signer\"\n",
    );

    let config = Sections {
        server: signer.leak(),
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.ca_pin, Some(CaPinSetting::Signer));
    Ok(())
}

#[test]
fn rejects_a_ca_pin_that_is_not_64_hex_characters() -> Result<(), Box<dyn Error>> {
    let pinned = SERVER.replace(
        "trust_domain = \"gang.internal\"\n",
        "trust_domain = \"gang.internal\"\nca_pin = \"sha256:abcdef0123\"\n",
    );

    let error = Sections {
        server: pinned.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)), "{error}");
    Ok(())
}

#[test]
fn ca_pin_is_absent_by_default() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert!(config.server.ca_pin.is_none());
    Ok(())
}

#[test]
fn parses_custom_server_transport_settings() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: TRANSPORT_SERVER,
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(
        config.server.handshake_timeout,
        NonZeroDuration::from_secs(3)
    );
    assert_eq!(config.server.max_pending_handshakes.get(), 8);
    assert_eq!(
        config.server.accept_error_backoff,
        NonZeroDuration::from_millis(250)
    );
    Ok(())
}

#[test]
fn rejects_a_zero_handshake_timeout() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        server: ZERO_HANDSHAKE_SERVER,
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    Ok(())
}

#[test]
fn rejects_a_zero_accept_error_backoff() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        server: ZERO_BACKOFF_SERVER,
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    Ok(())
}

#[test]
fn rejects_a_zero_max_pending_handshakes() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        server: ZERO_PENDING_SERVER,
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    Ok(())
}

#[test]
fn the_section_key_selects_the_backend() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        provisioner: "[provisioner.aws]\nregion = \"eu-north-1\"\n",
        fleet: fleet_with_options("capacity = \"spot\"\n"),
        ..Sections::VALID
    }
    .parse()?;

    assert!(
        matches!(&config.provisioner, ProvisionerConfig::Aws(aws) if aws.region == "eu-north-1")
    );
    Ok(())
}

#[test]
fn rejects_an_unknown_backend_and_lists_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        provisioner: "[provisioner.gcp]\nregion = \"x\"\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("gcp"), "{message}");
    assert!(
        message.contains("aws") && message.contains("local"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_two_backends_in_an_exactly_one_domain() -> Result<(), Box<dyn Error>> {
    let both =
        "[provisioner.local]\nstate_dir = \"/a\"\n[provisioner.aws]\nregion = \"eu-north-1\"\n";

    rejection(&Sections {
        provisioner: both,
        ..Sections::VALID
    })?;
    Ok(())
}

#[test]
fn rejects_a_missing_backend() -> Result<(), Box<dyn Error>> {
    rejection(&Sections {
        database: "[database]\n",
        ..Sections::VALID
    })?;
    Ok(())
}

#[test]
fn rejects_an_unknown_top_level_field() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        top: TOP.replace("poll_interval", "poll_intervall").leak(),
        ..Sections::VALID
    })?;

    assert!(message.contains("poll_intervall"), "{message}");
    Ok(())
}

#[test]
fn rejects_private_listen_as_an_unknown_field() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        top: format!("{TOP}private_listen = \"127.0.0.1:9090\"\n").leak(),
        ..Sections::VALID
    })?;

    assert!(message.contains("private_listen"), "{message}");
    Ok(())
}

#[test]
fn rejects_an_unknown_field_in_a_plugin_section() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        provisioner: "[provisioner.local]\nstate_dir = \"/a\"\nbogus = 1\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("bogus"), "{message}");
    Ok(())
}

#[test]
fn rejects_an_unknown_identity_plugin_and_lists_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let unknown = "[identity.providers.corp.saml]\nx = 1\n";

    let message = rejection(&Sections {
        identity: unknown,
        ..Sections::VALID
    })?;

    assert!(
        message.contains("saml") && message.contains("oidc"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_worker_authentication_provider_that_names_no_provider() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        authentication: AUTHENTICATION
            .replace("provider = \"google\"", "provider = \"corp\"")
            .leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::AuthenticationProviderNotConfigured(name) if name.as_str() == "corp"),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_a_client_authentication_provider_that_names_no_provider() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        authentication: AUTHENTICATION
            .replace("providers = [\"google\"]", "providers = [\"corp\"]")
            .leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::AuthenticationProviderNotConfigured(name) if name.as_str() == "corp"),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_an_endpoint_that_is_not_https_under_tls() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        top: TOP
            .replace(
                "endpoint = \"https://gang.example.com:9443\"",
                "endpoint = \"http://localhost:9443\"",
            )
            .leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(
            &error,
            ConfigError::EndpointNotHttps(url) if url.as_str() == "http://localhost:9443/"
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn parses_an_https_endpoint_on_localhost() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        top: TOP
            .replace(
                "endpoint = \"https://gang.example.com:9443\"",
                "endpoint = \"https://localhost:9443\"",
            )
            .leak(),
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(
        config.endpoint.as_ref().map(Url::as_str),
        Some("https://localhost:9443/")
    );
    Ok(())
}

#[test]
fn plaintext_accepts_an_http_or_https_endpoint() -> Result<(), Box<dyn Error>> {
    for endpoint in [
        "http://gang.example.com:9443",
        "https://gang.example.com:9443",
    ] {
        let config = Sections {
            top: TOP
                .replace(
                    "endpoint = \"https://gang.example.com:9443\"",
                    &format!("endpoint = \"{endpoint}\""),
                )
                .leak(),
            server: PLAINTEXT_SERVER,
            authentication: PLAINTEXT_AUTHENTICATION,
            ..Sections::VALID
        }
        .parse()?;

        let expected = format!("{endpoint}/");
        assert_eq!(
            config.endpoint.as_ref().map(Url::as_str),
            Some(expected.as_str())
        );
    }
    Ok(())
}

#[test]
fn plaintext_rejects_an_endpoint_that_is_neither_http_nor_https() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        top: TOP
            .replace(
                "endpoint = \"https://gang.example.com:9443\"",
                "endpoint = \"ftp://gang.example.com:9443\"",
            )
            .leak(),
        server: PLAINTEXT_SERVER,
        authentication: PLAINTEXT_AUTHENTICATION,
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::EndpointNotHttp(url) if url.scheme() == "ftp"),
        "{error}"
    );
    Ok(())
}

#[test]
fn omitted_endpoint_on_a_loopback_listener_parses_as_none() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        top: TOP
            .replace("endpoint = \"https://gang.example.com:9443\"\n", "")
            .leak(),
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.endpoint, None);
    Ok(())
}

#[test]
fn omitted_endpoint_on_a_public_listener_is_rejected() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        top: TOP
            .replace("endpoint = \"https://gang.example.com:9443\"\n", "")
            .replace("listen = \"127.0.0.1:9443\"", "listen = \"0.0.0.0:9443\"")
            .leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::EndpointRequired { listen } if listen.port() == 9443),
        "{error}"
    );
    Ok(())
}

#[test]
fn plaintext_with_none_surfaces_parses() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: PLAINTEXT_SERVER,
        authentication: PLAINTEXT_AUTHENTICATION,
        ..Sections::VALID
    }
    .parse()?;

    assert!(matches!(config.server.transport, Transport::Plaintext));
    assert!(matches!(
        config.authentication.clients,
        ClientAuthenticationConfig::None(_)
    ));
    assert!(matches!(
        config.authentication.workers,
        WorkerAuthenticationConfig::None(_)
    ));
    Ok(())
}

#[test]
fn plaintext_with_none_needs_no_identity_section() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        identity: "",
        server: PLAINTEXT_SERVER,
        authentication: PLAINTEXT_AUTHENTICATION,
        ..Sections::VALID
    }
    .parse()?;

    assert!(config.identity.providers.is_empty());
    Ok(())
}

#[test]
fn token_without_any_identity_provider_is_rejected_naming_the_provider()
-> Result<(), Box<dyn Error>> {
    let error = Sections {
        identity: "",
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::AuthenticationProviderNotConfigured(name) if name.as_str() == "google"),
        "{error}"
    );
    Ok(())
}

#[test]
fn plaintext_rejects_a_worker_plugin_that_requires_tls() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        server: PLAINTEXT_SERVER,
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::AuthenticationRequiresTls),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_an_unknown_fleet_sandbox_and_lists_the_accepted_keys() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        fleet: FLEET
            .replace("sandbox = \"bwrap\"", "sandbox = \"gvisor\"")
            .leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(&error, ConfigError::Gang(_)), "{error}");
    let message = chain(&error);
    assert!(message.contains("gvisor"), "{message}");
    assert!(
        message.contains("noop") && message.contains("bwrap"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_fleet_sandbox_without_a_section() -> Result<(), Box<dyn Error>> {
    let only_noop = "[sandbox.noop]\nkill_grace = \"2s\"\n";

    let error = Sections {
        sandbox: only_noop,
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(
            &error,
            ConfigError::FleetSandboxNotConfigured { fleet, key }
                if fleet.as_str() == "local-worker-x86-64" && key == "bwrap"
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_a_default_isa_without_a_fleet() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        top: TOP.replace("\"x86-64\"", "\"aarch64\"").leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(
        error,
        ConfigError::DefaultIsaWithoutFleet(Isa::Aarch64)
    ));
    Ok(())
}

#[test]
fn a_fleet_with_zero_max_instances_is_rejected() -> Result<(), Box<dyn Error>> {
    let zero_max = FLEET.replace("max_instances = 4", "max_instances = 0");
    assert_ne!(zero_max, FLEET, "fixture must contain max_instances = 4");

    let error = Sections {
        fleet: zero_max.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted a fleet with zero max_instances")?;

    assert!(matches!(error, ConfigError::Gang(_)), "{error}");
    Ok(())
}

#[test]
fn rejects_a_fleet_whose_minimum_exceeds_its_maximum() -> Result<(), Box<dyn Error>> {
    let inverted = FLEET.replace("min_instances = 0", "min_instances = 5");

    let error = Sections {
        fleet: inverted.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(
            &error,
            ConfigError::FleetBounds { fleet, min_instances: 5, max_instances: 4 }
                if fleet.as_str() == "local-worker-x86-64"
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_a_fleet_name_with_an_uppercase_letter() -> Result<(), Box<dyn Error>> {
    let uppercase = FLEET.replace("local-worker-x86-64", "Local-worker-x86-64");

    let error = Sections {
        fleet: uppercase.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    Ok(())
}

#[test]
fn rejects_a_zero_vcpus_per_instance() -> Result<(), Box<dyn Error>> {
    let zero = FLEET.replace("vcpus_per_instance = 4", "vcpus_per_instance = 0");

    let error = Sections {
        fleet: zero.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    Ok(())
}

#[test]
fn rejects_a_zero_target_actions_per_instance() -> Result<(), Box<dyn Error>> {
    let zero = FLEET.replace(
        "target_actions_per_instance = 4",
        "target_actions_per_instance = 0",
    );

    let error = Sections {
        fleet: zero.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    Ok(())
}

#[test]
fn rejects_a_fleet_without_a_boot_grace() -> Result<(), Box<dyn Error>> {
    let without = FLEET.replace("boot_grace = \"30s\"\n", "");
    assert_ne!(without, FLEET, "fixture must contain boot_grace");

    let error = Sections {
        fleet: without.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    assert!(chain(&error).contains("boot_grace"), "{error}");
    Ok(())
}

#[test]
fn parses_a_boot_grace_in_minutes() -> Result<(), Box<dyn Error>> {
    let minutes = FLEET.replace("boot_grace = \"30s\"", "boot_grace = \"10m\"");

    let config = Sections {
        fleet: minutes.leak(),
        ..Sections::VALID
    }
    .parse()?;

    let fleet = config.fleets.get(0).ok_or("local fleet")?;
    assert_eq!(fleet.policy.boot_grace, Duration::from_mins(10));
    Ok(())
}

#[test]
fn fleet_config_builds_the_fleet() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    let fleet = config.fleets.get(0).ok_or("local fleet")?.fleet();

    assert_eq!(fleet.name.as_str(), "local-worker-x86-64");
    assert_eq!(fleet.isa, Isa::X86_64);
    assert_eq!(fleet.instance.vcpus, 4);
    assert_eq!(fleet.instance.memory, Bytes::new(8 * 1024 * 1024 * 1024));
    assert!(matches!(fleet.options, FleetOptions::Local(_)));
    Ok(())
}

#[test]
fn a_fleet_config_view_is_copyable() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    let a = config.fleets.get(0).ok_or("local fleet")?;
    let b = a;
    assert_eq!(a.index, b.index);
    Ok(())
}

#[test]
fn parses_fleets_in_preference_order() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        provisioner: AWS_PROVISIONER,
        fleet: TWO_FLEETS,
        ..Sections::VALID
    }
    .parse()?;

    let amd64: FleetName = "amd64-spot".parse()?;
    let arm64: FleetName = "arm64-on-demand".parse()?;
    assert_eq!(config.fleets.len(), 2);
    assert_eq!(config.fleets.position(&amd64), Some(0));
    assert_eq!(config.fleets.position(&arm64), Some(1));
    assert_eq!(config.fleets.policies()[1].isa, Isa::Aarch64);

    let first = config.fleets.by_name(&amd64).ok_or("amd64 fleet")?;
    assert_eq!(first.index, 0);
    assert_eq!(first.sandbox, SandboxKey::Bwrap);
    assert!(matches!(first.options, FleetOptions::Aws(aws) if aws.capacity == "spot"));

    let second = config.fleets.get(1).ok_or("arm64 fleet")?;
    assert_eq!(second.policy.name, arm64);
    assert_eq!(second.sandbox, SandboxKey::Noop);
    assert!(config.fleets.get(2).is_none());

    let catalog = config.fleets.catalog();
    assert_eq!(catalog.fleets().len(), 2);
    assert_eq!(catalog.fleets()[0].get("fleet"), Some("amd64-spot"));
    assert_eq!(catalog.fleets()[0].get("ISA"), Some("x86-64"));
    assert_eq!(catalog.fleets()[0].get("sandbox"), Some("bwrap"));
    assert_eq!(catalog.fleets()[0].get("cpu_count"), Some("8"));
    assert_eq!(catalog.fleets()[0].get("memory_kb"), Some("30408704"));
    assert_eq!(catalog.fleets()[0].get("capacity"), Some("spot"));
    assert_eq!(catalog.fleets()[1].get("fleet"), Some("arm64-on-demand"));
    assert_eq!(catalog.fleets()[1].get("sandbox"), Some("noop"));
    assert_eq!(catalog.fleets()[1].get("capacity"), Some("on-demand"));
    Ok(())
}

#[test]
fn rejects_an_unknown_fleet_option_and_names_the_fleet() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        provisioner: AWS_PROVISIONER,
        fleet: fleet_with_options("capcity = \"spot\"\n"),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(
            &error,
            ConfigError::FleetOptions { fleet, .. }
                if fleet.as_str() == "local-worker-x86-64"
        ),
        "{error}"
    );
    assert!(chain(&error).contains("capacity"), "{error}");
    Ok(())
}

#[test]
fn rejects_a_fleet_option_that_is_missing() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        provisioner: AWS_PROVISIONER,
        fleet: fleet_with_options(""),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(
            &error,
            ConfigError::FleetOptions { fleet, .. }
                if fleet.as_str() == "local-worker-x86-64"
        ),
        "{error}"
    );
    assert!(chain(&error).contains("capacity"), "{error}");
    Ok(())
}

#[test]
fn rejects_a_provisioners_fleet_set_in_declaration_order() -> Result<(), Box<dyn Error>> {
    let fleets = [
        fleet_block("conflict-second"),
        fleet_block("conflict-first"),
    ]
    .concat();
    let error = Sections {
        fleet: fleets.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;
    assert!(
        matches!(&error, ConfigError::FleetSet(text) if text == "conflicting fleets: conflict-second, conflict-first"),
        "{error}"
    );
    Ok(())
}

#[test]
fn accepts_a_provisioners_valid_fleet_set() -> Result<(), Box<dyn Error>> {
    let fleets = [fleet_block("conflict-first"), fleet_block("other")].concat();
    Sections {
        fleet: fleets.leak(),
        ..Sections::VALID
    }
    .parse()?;
    Ok(())
}

#[test]
fn rejects_two_fleets_with_the_same_name() -> Result<(), Box<dyn Error>> {
    let duplicate = [fleet_block("same"), fleet_block("same")].concat();

    let error = Sections {
        fleet: duplicate.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::DuplicateFleet(name) if name.as_str() == "same"),
        "{error}"
    );
    Ok(())
}

#[test]
fn exactly_the_maximum_number_of_fleets_is_accepted() -> Result<(), Box<dyn Error>> {
    let many: String = (0..MAX_FLEETS)
        .map(|index| FLEET.replace("local-worker-x86-64", &format!("fleet-{index:02}")))
        .collect();

    let config = Sections {
        fleet: many.leak(),
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.fleets.len(), MAX_FLEETS);
    Ok(())
}

#[test]
fn rejects_more_fleets_than_the_catalog_holds() -> Result<(), Box<dyn Error>> {
    let many: String = (0..65)
        .map(|index| fleet_block(&format!("fleet-{index}")))
        .collect();

    let error = Sections {
        fleet: many.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(error, ConfigError::TooManyFleets { count: 65 }),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_an_unknown_field_in_a_fleet_entry() -> Result<(), Box<dyn Error>> {
    let bogus = FLEET.replace(
        "boot_grace = \"30s\"\n",
        "boot_grace = \"30s\"\nbogus = 1\n",
    );

    let error = Sections {
        fleet: bogus.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(&error, ConfigError::Gang(_)), "{error}");
    assert!(chain(&error).contains("bogus"), "{error}");
    Ok(())
}

#[test]
fn fleets_empty_array_is_a_missing_fleet() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        fleet: "fleets = []\n",
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(error, ConfigError::DefaultIsaWithoutFleet(Isa::X86_64)),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_a_fleet_without_a_sandbox() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        fleet: FLEET.replace("sandbox = \"bwrap\"\n", "").leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(&error, ConfigError::Gang(_)), "{error}");
    assert!(chain(&error).contains("sandbox"), "{error}");
    Ok(())
}

#[test]
fn rejects_a_fleet_without_a_disk_per_instance() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        fleet: FLEET.replace("disk_per_instance = \"100GiB\"\n", "").leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(&error, ConfigError::Gang(_)), "{error}");
    assert!(chain(&error).contains("disk_per_instance"), "{error}");
    Ok(())
}

#[test]
fn rejects_a_fleet_options_value_that_is_not_a_table() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        fleet: FLEET
            .replace(
                "boot_grace = \"30s\"\n",
                "boot_grace = \"30s\"\noptions = \"x\"\n",
            )
            .leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(&error, ConfigError::Gang(_)), "{error}");
    Ok(())
}

#[test]
fn unknown_fleet_key_lists_sandbox_and_options_as_accepted() -> Result<(), Box<dyn Error>> {
    let bogus = FLEET.replace(
        "boot_grace = \"30s\"\n",
        "boot_grace = \"30s\"\nbogus = 1\n",
    );

    let error = Sections {
        fleet: bogus.leak(),
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(matches!(&error, ConfigError::Gang(_)), "{error}");
    let message = chain(&error);
    assert!(message.contains("`sandbox`"), "{message}");
    assert!(message.contains("`options`"), "{message}");
    Ok(())
}

#[test]
fn execution_defaults_to_keep() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert!(matches!(config.execution.input_links, InputLinks::Keep));
    Ok(())
}

#[test]
fn execution_accepts_break_input_links() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        execution: "[execution]\ninput_links = \"break\"\n",
        ..Sections::VALID
    }
    .parse()?;

    assert!(matches!(config.execution.input_links, InputLinks::Break));
    Ok(())
}

#[test]
fn rejects_an_unknown_input_links_value() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        execution: "[execution]\ninput_links = \"link\"\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("unknown variant"), "{message}");
    Ok(())
}

#[test]
fn limits_are_absent_by_default() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert!(config.limits.is_none());
    Ok(())
}

#[test]
fn parses_a_cgroup_limits_section() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        limits: "[limits.cgroup]\nmax_pids = 512\n",
        ..Sections::VALID
    }
    .parse()?;

    assert!(matches!(
        &config.limits,
        Some(LimitsConfig::Cgroup(cgroup)) if cgroup.max_pids == Some(512)
    ));
    Ok(())
}

#[test]
fn rejects_two_limits_sections() -> Result<(), Box<dyn Error>> {
    rejection(&Sections {
        limits: "[limits.cgroup]\nmax_pids = 512\n\n[limits.rlimit]\nnofile = 1024\n",
        ..Sections::VALID
    })?;
    Ok(())
}

#[test]
fn server_and_worker_defaults_apply_when_sections_are_absent() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert_eq!(config.server.transport.kind(), ServerTransport::Tls);
    assert_eq!(
        config.server.transport,
        Transport::Tls {
            trust_domain: TrustDomain::try_new("gang.internal")?,
        }
    );
    assert!(config.server.ca_pin.is_none());
    assert_eq!(
        config.server.certificate_ttl,
        NonZeroDuration::from_hours(7 * 24)
    );
    assert_eq!(config.server.certificate_backdate, Duration::from_mins(1));
    assert_eq!(
        config.server.handshake_timeout,
        NonZeroDuration::from_secs(10)
    );
    assert_eq!(config.server.max_pending_handshakes.get(), 256);
    assert_eq!(
        config.server.accept_error_backoff,
        NonZeroDuration::from_millis(100)
    );
    assert_eq!(
        config.server.principal_seen_interval,
        NonZeroDuration::from_mins(1)
    );
    assert_eq!(config.server.max_tracked_principals.get(), 10_000);
    assert_eq!(config.server.roles, Vec::new());
    assert_eq!(config.server.max_request_size.get(), 64 * 1024);
    assert_eq!(
        config.server.request_timeout,
        NonZeroDuration::from_secs(30)
    );
    assert_eq!(config.worker, WorkerConfig::default());
    assert_eq!(config.worker.fast_cas_percent, Percent::from_percent(50));
    assert_eq!(
        config.worker.certificate_refresh_percent,
        Percent::from_percent(75)
    );
    assert_eq!(config.worker.preemption_margin, Duration::from_secs(10));
    assert_eq!(
        config.worker.lifecycle_poll_interval,
        NonZeroDuration::from_secs(5)
    );
    assert_eq!(
        config.worker.enrollment_retry_interval,
        NonZeroDuration::from_secs(10)
    );
    assert_eq!(
        config.worker.enrollment_deadline,
        NonZeroDuration::from_mins(5)
    );
    Ok(())
}

#[test]
fn server_section_overrides_each_field() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\ncertificate_ttl = \"1d\"\ncertificate_backdate = \"2m\"\nmax_request_size = \"1MiB\"\nrequest_timeout = \"45s\"\nprincipal_seen_interval = \"2m\"\nmax_tracked_principals = 123\n\n[[server.roles]]\nname = \"operators\"\npermissions = [\"state.read\"]\nprincipals = [{ provider = \"google\", principal = \"dev@example.com\" }]\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.max_request_size.get(), 1024 * 1024);
    assert_eq!(
        config.server.request_timeout,
        NonZeroDuration::from_secs(45)
    );
    assert_eq!(
        config.server.certificate_ttl,
        NonZeroDuration::from_hours(24)
    );
    assert_eq!(config.server.certificate_backdate, Duration::from_mins(2));
    assert_eq!(
        config.server.principal_seen_interval,
        NonZeroDuration::from_mins(2)
    );
    assert_eq!(config.server.max_tracked_principals.get(), 123);
    assert_eq!(config.server.roles.len(), 1);
    assert_eq!(config.server.roles[0].name.as_str(), "operators");
    assert_eq!(
        config.server.roles[0].principals[0].principal.as_str(),
        "dev@example.com"
    );
    Ok(())
}

#[test]
fn server_certificate_ttl_defaults_when_omitted() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nmax_request_size = \"1MiB\"\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(
        config.server.certificate_ttl,
        NonZeroDuration::from_hours(7 * 24)
    );
    Ok(())
}

#[test]
fn server_reissue_at_percent_defaults_to_fifty() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;

    assert_eq!(config.server.reissue_at_percent, Percent::from_percent(50));
    Ok(())
}

#[test]
fn server_reissue_at_percent_accepts_one() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nreissue_at_percent = 1\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.reissue_at_percent, Percent::from_percent(1));
    Ok(())
}

#[test]
fn server_reissue_at_percent_accepts_one_hundred() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nreissue_at_percent = 100\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.reissue_at_percent, Percent::from_percent(100));
    Ok(())
}

#[test]
fn server_reissue_at_percent_rejects_zero() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nreissue_at_percent = 0\n",
        ..Sections::VALID
    })?;

    assert!(
        message.contains("reissue_at_percent") && message.contains("1 to 100"),
        "{message}"
    );
    Ok(())
}

#[test]
fn server_reissue_at_percent_rejects_one_hundred_and_one() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nreissue_at_percent = 101\n",
        ..Sections::VALID
    })?;

    assert!(
        message.contains("reissue_at_percent") && message.contains("1 to 100"),
        "{message}"
    );
    Ok(())
}

/// Rejects the given `[server]` text, failing the test if the configuration is accepted.
fn assert_server_rejected(server: &'static str) -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server,
        ..Sections::VALID
    })?;

    assert!(!message.is_empty());
    Ok(())
}

#[test]
fn rate_limit_defaults_apply_when_the_section_is_omitted() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;
    let rate_limit = &config.server.rate_limit;

    assert_eq!(rate_limit.client.burst.get(), 30);
    assert_eq!(
        rate_limit.client.refill_interval,
        NonZeroDuration::from_secs(2)
    );
    assert_eq!(rate_limit.global.burst.get(), 500);
    assert_eq!(
        rate_limit.global.refill_interval,
        NonZeroDuration::from_millis(50)
    );
    assert_eq!(rate_limit.max_tracked_clients.get(), 10_000);
    assert!(rate_limit.trusted_proxies.is_empty());
    assert_eq!(rate_limit.ipv6_prefix_length.get(), 64);
    Ok(())
}

#[test]
fn every_rate_limit_key_can_be_overridden() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nmax_tracked_clients = 42\nipv6_prefix_length = 56\ntrusted_proxies = [\"10.0.0.0/8\", \"2001:db8::/32\"]\n\n[server.rate_limit.client]\nburst = 7\nrefill_interval = \"3s\"\n\n[server.rate_limit.global]\nburst = 900\nrefill_interval = \"20ms\"\n",
        ..Sections::VALID
    }
    .parse()?;
    let rate_limit = &config.server.rate_limit;

    assert_eq!(rate_limit.client.burst.get(), 7);
    assert_eq!(
        rate_limit.client.refill_interval,
        NonZeroDuration::from_secs(3)
    );
    assert_eq!(rate_limit.global.burst.get(), 900);
    assert_eq!(
        rate_limit.global.refill_interval,
        NonZeroDuration::from_millis(20)
    );
    assert_eq!(rate_limit.max_tracked_clients.get(), 42);
    assert_eq!(
        rate_limit.trusted_proxies,
        vec![
            "10.0.0.0/8".parse::<ipnet::IpNet>()?,
            "2001:db8::/32".parse::<ipnet::IpNet>()?,
        ]
    );
    assert_eq!(rate_limit.ipv6_prefix_length.get(), 56);
    Ok(())
}

#[test]
fn rejects_a_client_bucket_missing_its_refill_interval() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit.client]\nburst = 5\n",
    )
}

#[test]
fn rejects_a_global_bucket_missing_its_burst() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit.global]\nrefill_interval = \"50ms\"\n",
    )
}

#[test]
fn rejects_a_zero_client_burst() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit.client]\nburst = 0\nrefill_interval = \"2s\"\n",
    )
}

#[test]
fn rejects_a_zero_global_refill_interval() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit.global]\nburst = 500\nrefill_interval = \"0s\"\n",
    )
}

#[test]
fn rejects_zero_tracked_clients() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nmax_tracked_clients = 0\n",
    )
}

#[test]
fn rejects_a_trusted_proxy_without_a_prefix() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\ntrusted_proxies = [\"10.0.0.1\"]\n",
    )
}

#[test]
fn rejects_a_trusted_proxy_that_is_not_an_address() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\ntrusted_proxies = [\"nope\"]\n",
    )
}

#[test]
fn ipv6_prefix_length_defaults_when_the_key_is_omitted() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nmax_tracked_clients = 42\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.rate_limit.ipv6_prefix_length.get(), 64);
    Ok(())
}

#[test]
fn rejects_a_zero_ipv6_prefix_length() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nipv6_prefix_length = 0\n",
    )
}

#[test]
fn rejects_an_ipv6_prefix_length_above_128() -> Result<(), Box<dyn Error>> {
    assert_server_rejected(
        "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nipv6_prefix_length = 129\n",
    )
}

#[test]
fn accepts_the_longest_ipv6_prefix_length() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nipv6_prefix_length = 128\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.rate_limit.ipv6_prefix_length.get(), 128);
    Ok(())
}

#[test]
fn accepts_the_shortest_ipv6_prefix_length() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nipv6_prefix_length = 1\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.rate_limit.ipv6_prefix_length.get(), 1);
    Ok(())
}

#[test]
fn rejects_an_unknown_rate_limit_key_and_names_it() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\n\n[server.rate_limit]\nrequests_per_second = 5\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("requests_per_second"), "{message}");
    Ok(())
}

#[test]
fn plaintext_server_needs_no_trust_domain() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: PLAINTEXT_SERVER,
        authentication: PLAINTEXT_AUTHENTICATION,
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.transport, Transport::Plaintext);
    Ok(())
}

#[test]
fn tls_requires_a_trust_domain() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        server: "[server]\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("trust_domain is required"), "{message}");
    Ok(())
}

#[test]
fn an_omitted_server_section_is_rejected() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        server: "",
        ..Sections::VALID
    })?;

    assert!(message.contains("server"), "{message}");
    Ok(())
}

#[test]
fn plaintext_ignores_a_trust_domain() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: "[server]\ntransport = \"plaintext\"\ntrust_domain = \"x.example.com\"\n",
        authentication: PLAINTEXT_AUTHENTICATION,
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.transport, Transport::Plaintext);
    Ok(())
}

#[test]
fn rejects_a_zero_principal_seen_interval() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nprincipal_seen_interval = \"0s\"\n",
        ..Sections::VALID
    })?;

    assert!(
        message.contains("principal_seen_interval") && message.contains("zero"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_zero_max_tracked_principals() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nmax_tracked_principals = 0\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("max_tracked_principals"), "{message}");
    Ok(())
}

#[test]
fn rejects_shutdown_grace_in_the_server_section() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: "[server]\ntrust_domain = \"gang.internal\"\nshutdown_grace = \"5s\"\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("shutdown_grace"), "{message}");
    Ok(())
}

#[test]
fn accepts_a_valid_role() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "operators"
permissions = ["state.read", "events.read"]
principals = [{ provider = "google", principal = "dev@example.com" }]
"#,
        ..Sections::VALID
    }
    .parse()?;

    let role = &config.server.roles[0];
    assert_eq!(config.server.roles.len(), 1);
    assert_eq!(role.name.as_str(), "operators");
    assert_eq!(
        role.permissions,
        PermissionSet::EMPTY
            .with(Permission::StateRead)
            .with(Permission::EventsRead)
    );
    assert_eq!(role.principals.len(), 1);
    assert_eq!(role.principals[0].provider.as_str(), "google");
    assert_eq!(role.principals[0].principal.as_str(), "dev@example.com");
    Ok(())
}

#[test]
fn role_wildcard_grants_every_permission() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "administrator"
permissions = ["*"]
principals = [{ provider = "google", principal = "dev@example.com" }]
"#,
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.server.roles[0].permissions, PermissionSet::all());
    for permission in Permission::ALL {
        assert!(config.server.roles[0].permissions.contains(permission));
    }
    Ok(())
}

#[test]
fn rejects_a_duplicate_role_name() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "operators"
permissions = ["state.read"]
principals = [{ provider = "google", principal = "dev@example.com" }]

[[server.roles]]
name = "operators"
permissions = ["events.read"]
principals = [{ provider = "google", principal = "ops@example.com" }]
"#,
        ..Sections::VALID
    })?;

    assert!(
        message.contains("role `operators` is declared twice"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_an_unknown_permission_and_lists_the_accepted_names() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "operators"
permissions = ["state.write"]
principals = [{ provider = "google", principal = "dev@example.com" }]
"#,
        ..Sections::VALID
    })?;

    for accepted in [
        "state.read",
        "principals.read",
        "events.read",
        "principals.revoke",
    ] {
        assert!(message.contains(accepted), "{message}");
    }
    Ok(())
}

#[test]
fn rejects_a_role_without_permissions() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "operators"
permissions = []
principals = [{ provider = "google", principal = "dev@example.com" }]
"#,
        ..Sections::VALID
    })?;

    assert!(
        message.contains("role `operators` grants no permissions"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_role_without_principals() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "operators"
permissions = ["state.read"]
principals = []
"#,
        ..Sections::VALID
    })?;

    assert!(
        message.contains("role `operators` has no principals"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_role_naming_an_unknown_provider() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "operators"
permissions = ["state.read"]
principals = [{ provider = "corp", principal = "dev@example.com" }]
"#,
        ..Sections::VALID
    })?;

    assert!(
        message.contains(
            "role `operators` names provider `corp` that has no [identity.providers.corp] section"
        ),
        "{message}"
    );
    Ok(())
}

#[test]
fn role_may_name_the_reserved_none_provider() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.roles]]
name = "anonymous"
permissions = ["state.read"]
principals = [{ provider = "none", principal = "anonymous" }]
"#,
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(
        config.server.roles[0].principals[0].provider.as_str(),
        "none"
    );
    Ok(())
}

#[test]
fn rejects_the_retired_administrators_section() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        server: r#"[server]
trust_domain = "gang.internal"

[[server.administrators]]
provider = "google"
principal = "dev@example.com"
"#,
        ..Sections::VALID
    })?;

    assert!(message.contains("administrators"), "{message}");
    Ok(())
}

#[test]
fn rejects_the_reserved_none_as_an_identity_provider_name() -> Result<(), Box<dyn Error>> {
    let error = Sections {
        identity: "[identity.providers.none.oidc]\nissuer_url = \"https://accounts.google.com\"\n",
        authentication: PLAINTEXT_AUTHENTICATION,
        ..Sections::VALID
    }
    .parse()
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::ReservedProviderName(name) if name.as_str() == "none"),
        "{error}"
    );
    Ok(())
}

#[test]
fn rejects_a_missing_authentication_surface() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        authentication: "[authentication.clients.none]\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("workers"), "{message}");
    Ok(())
}

#[test]
fn rejects_two_plugin_sections_on_one_surface() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        authentication: "[authentication.clients.none]\n[authentication.clients.token]\ntoken_ttl = \"10m\"\nclock_skew = \"30s\"\nmax_cached_tokens = 4096\nproviders = []\n\n[authentication.workers.none]\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("clients"), "{message}");
    Ok(())
}

#[test]
fn rejects_an_unknown_field_in_a_client_plugin_section() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        authentication: "[authentication.clients.none]\nbogus = 1\n\n[authentication.workers.none]\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("bogus"), "{message}");
    Ok(())
}

#[test]
fn worker_section_overrides_each_field() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        worker: "[worker]\nfast_cas_percent = 20\ncertificate_refresh_percent = 60\npreemption_margin = \"30s\"\nlifecycle_poll_interval = \"1s\"\nenrollment_retry_interval = \"2s\"\nenrollment_deadline = \"2m\"\n",
        ..Sections::VALID
    }
    .parse()?;

    assert_eq!(config.worker.fast_cas_percent, Percent::from_percent(20));
    assert_eq!(
        config.worker.certificate_refresh_percent,
        Percent::from_percent(60)
    );
    assert_eq!(config.worker.preemption_margin, Duration::from_secs(30));
    assert_eq!(
        config.worker.lifecycle_poll_interval,
        NonZeroDuration::from_secs(1)
    );
    assert_eq!(
        config.worker.enrollment_retry_interval,
        NonZeroDuration::from_secs(2)
    );
    assert_eq!(
        config.worker.enrollment_deadline,
        NonZeroDuration::from_mins(2)
    );
    Ok(())
}

#[test]
fn rejects_a_worker_percent_outside_one_to_one_hundred() -> Result<(), Box<dyn Error>> {
    for field in ["fast_cas_percent", "certificate_refresh_percent"] {
        for value in [0, 101] {
            let worker = format!("[worker]\n{field} = {value}\n");
            let message = toml_rejection(&Sections {
                worker: worker.leak(),
                ..Sections::VALID
            })?;

            assert!(
                message.contains(field) && message.contains("1 to 100"),
                "{message}"
            );
        }
    }
    Ok(())
}

#[test]
fn rejects_a_zero_lifecycle_poll_interval() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        worker: "[worker]\nlifecycle_poll_interval = \"0s\"\n",
        ..Sections::VALID
    })?;

    assert!(
        message.contains("lifecycle_poll_interval") && message.contains("zero"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_zero_poll_interval() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        top: TOP
            .replace("poll_interval = \"500ms\"", "poll_interval = \"0s\"")
            .leak(),
        ..Sections::VALID
    })?;

    assert!(
        message.contains("poll_interval") && message.contains("zero"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_zero_client_action_timeout() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        top: TOP
            .replace(
                "client_action_timeout = \"1m\"",
                "client_action_timeout = \"0s\"",
            )
            .leak(),
        ..Sections::VALID
    })?;

    assert!(
        message.contains("client_action_timeout") && message.contains("zero"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_zero_enrollment_retry_interval() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        worker: "[worker]\nenrollment_retry_interval = \"0s\"\n",
        ..Sections::VALID
    })?;

    assert!(
        message.contains("enrollment_retry_interval") && message.contains("zero"),
        "{message}"
    );
    Ok(())
}

#[test]
fn rejects_a_zero_enrollment_deadline() -> Result<(), Box<dyn Error>> {
    let message = toml_rejection(&Sections {
        worker: "[worker]\nenrollment_deadline = \"0s\"\n",
        ..Sections::VALID
    })?;

    assert!(
        message.contains("enrollment_deadline") && message.contains("zero"),
        "{message}"
    );
    Ok(())
}

#[test]
fn sandbox_file_round_trips_through_toml() -> Result<(), Box<dyn Error>> {
    let file: SandboxFile<Plugins> = SANDBOX_FILE.parse()?;

    let rendered = file.to_toml()?;
    assert!(rendered.contains("selected = \"bwrap\""), "{rendered}");
    let again: SandboxFile<Plugins> = rendered.parse()?;

    assert_eq!(again.selected, file.selected);
    assert_eq!(again.selected, SandboxKey::Bwrap);
    assert!(again.sandbox.is_configured(SandboxKey::Noop));
    assert!(again.sandbox.is_configured(SandboxKey::Bwrap));
    let noop = again.sandbox.noop.as_ref().ok_or("noop sandbox")?;
    assert_eq!(noop.kill_grace, Duration::from_secs(2));
    assert!(
        again
            .sandbox
            .bwrap
            .as_ref()
            .is_some_and(|bwrap| !bwrap.permit_network_property)
    );
    assert!(matches!(again.execution.input_links, InputLinks::Break));
    assert!(matches!(
        &again.limits,
        Some(LimitsConfig::Cgroup(cgroup)) if cgroup.max_pids == Some(512)
    ));
    Ok(())
}

#[test]
fn sandbox_file_without_execution_or_limits_defaults() -> Result<(), Box<dyn Error>> {
    let file: SandboxFile<Plugins> =
        "work_directory = \"/var/lib/gang/work\"\nselected = \"bwrap\"\n\n[sandbox.bwrap]\npermit_network_property = false\n"
            .parse()?;

    assert!(matches!(file.execution.input_links, InputLinks::Keep));
    assert!(file.limits.is_none());
    Ok(())
}

#[test]
fn sandbox_file_rejects_a_selected_sandbox_without_a_section() -> Result<(), Box<dyn Error>> {
    let error = "work_directory = \"/var/lib/gang/work\"\nselected = \"bwrap\"\n\n[sandbox.noop]\nkill_grace = \"2s\"\n"
        .parse::<SandboxFile<Plugins>>()
        .err()
        .ok_or("accepted")?;

    assert!(matches!(
        error,
        SandboxFileError::SelectedNotConfigured(key) if key == "bwrap"
    ));
    Ok(())
}

#[test]
fn sandbox_file_rejects_an_unknown_top_level_field() -> Result<(), Box<dyn Error>> {
    let error = "work_directory = \"/var/lib/gang/work\"\nselected = \"bwrap\"\nbogus = 1\n\n[sandbox.bwrap]\npermit_network_property = false\n"
        .parse::<SandboxFile<Plugins>>()
        .err()
        .ok_or("accepted")?;

    assert!(chain(&error).contains("bogus"), "{error}");
    Ok(())
}

#[test]
fn sandbox_file_work_directory_round_trips_through_toml() -> Result<(), Box<dyn Error>> {
    let file: SandboxFile<Plugins> = SANDBOX_FILE.parse()?;
    assert_eq!(
        file.work_directory.as_path(),
        Path::new("/var/lib/gang/work")
    );

    let again: SandboxFile<Plugins> = file.to_toml()?.parse()?;
    assert_eq!(again.work_directory, file.work_directory);
    Ok(())
}

#[test]
fn sandbox_file_without_work_directory_is_rejected() -> Result<(), Box<dyn Error>> {
    let error = "selected = \"bwrap\"\n\n[sandbox.bwrap]\npermit_network_property = false\n"
        .parse::<SandboxFile<Plugins>>()
        .err()
        .ok_or("accepted")?;

    assert!(matches!(error, SandboxFileError::Parse(_)));
    assert!(chain(&error).contains("work_directory"), "{error}");
    Ok(())
}

#[test]
fn sandbox_file_rejects_a_relative_work_directory() -> Result<(), Box<dyn Error>> {
    let error = "work_directory = \"relative/work\"\nselected = \"bwrap\"\n\n[sandbox.bwrap]\npermit_network_property = false\n"
        .parse::<SandboxFile<Plugins>>()
        .err()
        .ok_or("accepted")?;

    assert!(matches!(error, SandboxFileError::Parse(_)));
    assert!(chain(&error).contains("not absolute"), "{error}");
    Ok(())
}

#[test]
fn sandbox_file_beside_names_the_sibling_of_the_entrypoint() {
    assert_eq!(
        SandboxFile::<Plugins>::beside(Path::new("/opt/worker/gang-exec")),
        PathBuf::from("/opt/worker/sandbox.toml")
    );
}

#[test]
fn malformed_gang_toml_is_a_gang_error() -> Result<(), Box<dyn Error>> {
    let error = "poll_interval = "
        .parse::<GangConfig<Plugins>>()
        .err()
        .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)));
    assert!(
        error.to_string().starts_with("invalid gang.toml"),
        "{error}"
    );
    Ok(())
}

#[test]
fn malformed_sandbox_file_is_a_sandbox_file_error() -> Result<(), Box<dyn Error>> {
    let error = "work_directory = \"/var/lib/gang/work\"\nselected = [\n"
        .parse::<SandboxFile<Plugins>>()
        .err()
        .ok_or("accepted")?;

    assert!(matches!(error, SandboxFileError::Parse(_)));
    assert!(
        error.to_string().starts_with("invalid sandbox file"),
        "{error}"
    );
    Ok(())
}

/// A fleet member built directly, with `capacity` as its only option.
fn hand_member(
    name: &str,
    isa: Isa,
    sandbox: SandboxKey,
    capacity: &str,
) -> Result<FleetMember<Plugins>, Box<dyn Error>> {
    let policy = FleetPolicy {
        name: name.parse()?,
        isa,
        instances: InstanceBounds::up_to(NonZeroU32::MIN.saturating_add(3)),
        vcpus_per_instance: NonZeroU32::MIN,
        memory_per_instance: NonZeroBytes::gib(1),
        disk_per_instance: NonZeroBytes::gib(100),
        target_actions_per_instance: NonZeroU32::MIN,
        idle_timeout: Duration::from_secs(15),
        max_drain_wait: Duration::from_mins(1),
        boot_grace: Duration::from_secs(30),
    };
    Ok(FleetMember {
        policy,
        sandbox,
        options: FleetOptions::Aws(AwsOptions {
            capacity: capacity.to_owned(),
        }),
    })
}

/// Rebuilds a parsed fleet as a member, cloning its policy and options.
fn member_from_parsed(fleet: FleetConfig<'_, Plugins>) -> FleetMember<Plugins> {
    let options = match fleet.options {
        FleetOptions::Aws(aws) => FleetOptions::Aws(AwsOptions {
            capacity: aws.capacity.clone(),
        }),
        FleetOptions::Local(_) => FleetOptions::Local(LocalOptions {}),
    };
    FleetMember {
        policy: fleet.policy.clone(),
        sandbox: fleet.sandbox,
        options,
    }
}

#[test]
fn try_new_builds_the_columns_and_catalog_in_order() -> Result<(), Box<dyn Error>> {
    let members = vec![
        hand_member("amd64-spot", Isa::X86_64, SandboxKey::Bwrap, "spot")?,
        hand_member(
            "arm64-on-demand",
            Isa::Aarch64,
            SandboxKey::Noop,
            "on-demand",
        )?,
    ];
    let fleets = Fleets::try_new(members)?;

    let amd64: FleetName = "amd64-spot".parse()?;
    let arm64: FleetName = "arm64-on-demand".parse()?;
    assert_eq!(fleets.len(), 2);
    assert_eq!(fleets.position(&amd64), Some(0));
    assert_eq!(fleets.position(&arm64), Some(1));
    assert_eq!(fleets.policies()[1].isa, Isa::Aarch64);

    let first = fleets.get(0).ok_or("amd64 fleet")?;
    assert_eq!(first.sandbox, SandboxKey::Bwrap);
    assert!(matches!(first.options, FleetOptions::Aws(aws) if aws.capacity == "spot"));

    let second = fleets.get(1).ok_or("arm64 fleet")?;
    assert_eq!(second.sandbox, SandboxKey::Noop);
    assert!(matches!(second.options, FleetOptions::Aws(aws) if aws.capacity == "on-demand"));
    assert!(fleets.get(2).is_none());

    let catalog = fleets.catalog();
    assert_eq!(catalog.fleets().len(), 2);
    assert_eq!(catalog.fleets()[0].get("fleet"), Some("amd64-spot"));
    assert_eq!(catalog.fleets()[0].get("sandbox"), Some("bwrap"));
    assert_eq!(catalog.fleets()[0].get("capacity"), Some("spot"));
    assert_eq!(catalog.fleets()[1].get("fleet"), Some("arm64-on-demand"));
    assert_eq!(catalog.fleets()[1].get("sandbox"), Some("noop"));
    assert_eq!(catalog.fleets()[1].get("capacity"), Some("on-demand"));
    Ok(())
}

#[test]
fn an_empty_fleet_set_reports_empty() -> Result<(), Box<dyn Error>> {
    let empty = Fleets::<Plugins>::try_new(Vec::new())?;
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);

    let config = Sections::VALID.parse()?;
    assert!(!config.fleets.is_empty());
    Ok(())
}

#[test]
fn sandbox_programs_visit_only_the_selected_section() -> Result<(), Box<dyn Error>> {
    let mut sandbox: SandboxConfig = toml::from_str(
        "[noop]\nkill_grace = \"2s\"\nprogram = \"noop-a\"\n\n[bwrap]\npermit_network_property = false\nprogram = \"bwrap-b\"\n",
    )?;
    let mut visited = Vec::new();
    let result: Result<(), &str> = sandbox.try_for_each_program(SandboxKey::Bwrap, |program| {
        visited.push(program.clone());
        *program = PathBuf::from("bwrap-rewritten");
        Ok(())
    });
    result?;
    assert_eq!(visited, [PathBuf::from("bwrap-b")]);
    assert_eq!(
        sandbox.noop.as_ref().map(|noop| noop.program.clone()),
        Some(PathBuf::from("noop-a"))
    );
    assert_eq!(
        sandbox.bwrap.as_ref().map(|bwrap| bwrap.program.clone()),
        Some(PathBuf::from("bwrap-rewritten"))
    );
    Ok(())
}

#[test]
fn try_new_matches_the_fleets_parsed_from_toml() -> Result<(), Box<dyn Error>> {
    let parsed = Sections {
        provisioner: AWS_PROVISIONER,
        fleet: TWO_FLEETS,
        ..Sections::VALID
    }
    .parse()?;
    let members: Vec<_> = parsed.fleets.iter().map(member_from_parsed).collect();
    let fleets = Fleets::try_new(members)?;

    assert_eq!(fleets.policies(), parsed.fleets.policies());
    assert_eq!(fleets.catalog(), parsed.fleets.catalog());
    let sandboxes =
        |fleets: &Fleets<Plugins>| fleets.iter().map(|fleet| fleet.sandbox).collect::<Vec<_>>();
    assert_eq!(sandboxes(&fleets), sandboxes(&parsed.fleets));
    Ok(())
}

#[test]
fn sandbox_programs_of_an_absent_section_visit_nothing() -> Result<(), Box<dyn Error>> {
    let mut sandbox: SandboxConfig =
        toml::from_str("[noop]\nkill_grace = \"2s\"\nprogram = \"noop-a\"\n")?;
    let mut calls = 0;
    let result: Result<(), &str> = sandbox.try_for_each_program(SandboxKey::Bwrap, |_| {
        calls += 1;
        Ok(())
    });
    result?;
    assert_eq!(calls, 0);
    Ok(())
}

#[test]
fn try_new_rejects_more_fleets_than_the_catalog_holds() -> Result<(), Box<dyn Error>> {
    let members = (0..65)
        .map(|index| {
            hand_member(
                &format!("fleet-{index}"),
                Isa::X86_64,
                SandboxKey::Bwrap,
                "spot",
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let error = Fleets::try_new(members).err().ok_or("accepted")?;
    assert_eq!(error, FleetCountError { count: 65 });
    Ok(())
}

#[test]
fn sandbox_programs_stop_at_the_first_error() -> Result<(), Box<dyn Error>> {
    let mut sandbox: SandboxConfig = toml::from_str(
        "[noop]\nkill_grace = \"2s\"\nprogram = \"noop-a\"\n\n[bwrap]\npermit_network_property = false\nprogram = \"bwrap-b\"\n",
    )?;
    let mut calls = 0;
    let stopped = sandbox.try_for_each_program(SandboxKey::Noop, |_| {
        calls += 1;
        Err("stop")
    });
    assert_eq!(stopped, Err("stop"));
    assert_eq!(calls, 1);
    Ok(())
}

/// The configuration with the given `[dashboard]` text and every other section valid.
fn with_dashboard(dashboard: &'static str) -> Result<GangConfig<Plugins>, ConfigError> {
    Sections {
        dashboard,
        ..Sections::VALID
    }
    .parse()
}

#[test]
fn dashboard_defaults_apply_when_the_section_is_omitted() -> Result<(), Box<dyn Error>> {
    let config = Sections::VALID.parse()?;
    let dashboard = &config.dashboard;

    assert!(dashboard.enabled);
    assert_eq!(dashboard.max_viewers.get(), 64);
    assert_eq!(dashboard.retry_after, Duration::from_secs(5));
    assert_eq!(dashboard.heartbeat, NonZeroDuration::from_secs(10));
    assert_eq!(dashboard.lag_frames.get(), 32);
    assert_eq!(dashboard.recent_events.get(), 200);
    assert_eq!(dashboard.max_listed_running, 2000);
    assert_eq!(dashboard.max_listed_queued, 200);
    assert_eq!(dashboard.detail_interval, NonZeroDuration::from_secs(10));
    assert_eq!(dashboard.actions_stale_after, None);
    assert_eq!(dashboard.actions_stale_after(), Duration::from_secs(30));
    assert_eq!(dashboard.login_ttl, NonZeroDuration::from_mins(10));
    assert_eq!(dashboard.max_pending_logins.get(), 1024);
    assert!(dashboard.login.is_none());
    Ok(())
}

#[test]
fn every_dashboard_key_can_be_overridden() -> Result<(), Box<dyn Error>> {
    let config = with_dashboard(
        r#"
[dashboard]
enabled = false
max_viewers = 8
retry_after = "0s"
heartbeat = "2s"
lag_frames = 4
recent_events = 50
max_listed_running = 100
max_listed_queued = 10
detail_interval = "3s"
actions_stale_after = "45s"
login_ttl = "5m"
max_pending_logins = 16
"#,
    )?;
    let dashboard = &config.dashboard;

    assert!(!dashboard.enabled);
    assert_eq!(dashboard.max_viewers.get(), 8);
    assert_eq!(dashboard.retry_after, Duration::ZERO);
    assert_eq!(dashboard.heartbeat, NonZeroDuration::from_secs(2));
    assert_eq!(dashboard.lag_frames.get(), 4);
    assert_eq!(dashboard.recent_events.get(), 50);
    assert_eq!(dashboard.max_listed_running, 100);
    assert_eq!(dashboard.max_listed_queued, 10);
    assert_eq!(dashboard.detail_interval, NonZeroDuration::from_secs(3));
    assert_eq!(
        dashboard.actions_stale_after,
        Some(NonZeroDuration::from_secs(45))
    );
    assert_eq!(dashboard.actions_stale_after(), Duration::from_secs(45));
    assert_eq!(dashboard.login_ttl, NonZeroDuration::from_mins(5));
    assert_eq!(dashboard.max_pending_logins.get(), 16);
    Ok(())
}

#[test]
fn stale_threshold_defaults_to_three_refresh_intervals() -> Result<(), Box<dyn Error>> {
    let config = with_dashboard("[dashboard]\ndetail_interval = \"4s\"\n")?;

    assert_eq!(
        config.dashboard.actions_stale_after(),
        Duration::from_secs(12)
    );
    Ok(())
}

#[test]
fn stale_threshold_equal_to_or_below_the_refresh_interval_is_rejected() -> Result<(), Box<dyn Error>>
{
    for text in [
        "[dashboard]\ndetail_interval = \"10s\"\nactions_stale_after = \"10s\"\n",
        "[dashboard]\ndetail_interval = \"10s\"\nactions_stale_after = \"5s\"\n",
    ] {
        let error = with_dashboard(text).err().ok_or("accepted")?;

        assert!(
            matches!(error, ConfigError::StaleThresholdNotAboveRefresh),
            "{error}"
        );
    }
    Ok(())
}

#[test]
fn zero_heartbeat_detail_interval_and_login_ttl_are_rejected() -> Result<(), Box<dyn Error>> {
    for key in ["heartbeat", "detail_interval", "login_ttl"] {
        let text: &'static str = format!("[dashboard]\n{key} = \"0s\"\n").leak();
        let message = toml_rejection(&Sections {
            dashboard: text,
            ..Sections::VALID
        })?;

        assert!(
            message.contains(key) && message.contains("zero"),
            "{key}: {message}"
        );
    }
    Ok(())
}

#[test]
fn zero_retry_after_is_accepted() -> Result<(), Box<dyn Error>> {
    let config = with_dashboard("[dashboard]\nretry_after = \"0s\"\n")?;

    assert_eq!(config.dashboard.retry_after, Duration::ZERO);
    Ok(())
}

#[test]
fn dashboard_login_is_accepted_and_keeps_its_secret_out_of_debug() -> Result<(), Box<dyn Error>> {
    let config = with_dashboard(
        r#"
[dashboard.login]
provider = "google"
client_id = "dashboard-client"
client_secret = "s3cr3t-dashboard-value"
"#,
    )?;
    let login = config.dashboard.login.as_ref().ok_or("login missing")?;

    assert_eq!(login.provider.as_str(), "google");
    assert_eq!(login.client_id, "dashboard-client");
    assert!(matches!(
        &login.client_secret,
        ClientSecretSource::Inline(secret) if secret.matches("s3cr3t-dashboard-value")
    ));
    let debug = format!("{:?}", config.dashboard);
    assert!(!debug.contains("s3cr3t-dashboard-value"), "{debug}");
    Ok(())
}

#[test]
fn dashboard_login_provider_must_be_configured() -> Result<(), Box<dyn Error>> {
    let error = with_dashboard(
        "[dashboard.login]\nprovider = \"corp\"\nclient_id = \"c\"\nclient_secret = \"s\"\n",
    )
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::DashboardLoginProviderNotConfigured(name) if name.as_str() == "corp"),
        "{error}"
    );
    Ok(())
}

#[test]
fn dashboard_login_provider_cannot_be_the_reserved_none() -> Result<(), Box<dyn Error>> {
    let error = with_dashboard(
        "[dashboard.login]\nprovider = \"none\"\nclient_id = \"c\"\nclient_secret = \"s\"\n",
    )
    .err()
    .ok_or("accepted")?;

    assert!(
        matches!(&error, ConfigError::DashboardLoginProviderNotConfigured(name) if name.as_str() == "none"),
        "{error}"
    );
    Ok(())
}

#[test]
fn dashboard_login_client_id_must_be_non_empty() -> Result<(), Box<dyn Error>> {
    let error = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \"\"\nclient_secret = \"s\"\n",
    )
    .err()
    .ok_or("accepted")?;

    let message = chain(&error);
    assert!(message.contains("client_id must not be blank"), "{message}");
    Ok(())
}

#[test]
fn a_blank_dashboard_login_client_id_is_rejected() -> Result<(), Box<dyn Error>> {
    let error = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \" \"\nclient_secret = \"s\"\n",
    )
    .err()
    .ok_or("accepted")?;

    let message = chain(&error);
    assert!(message.contains("client_id must not be blank"), "{message}");
    Ok(())
}

#[test]
fn dashboard_login_inline_secret_must_not_be_blank() -> Result<(), Box<dyn Error>> {
    let empty = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \"c\"\nclient_secret = \"\"\n",
    )
    .err()
    .ok_or("accepted")?;
    let spaces = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \"c\"\nclient_secret = \"   \"\n",
    )
    .err()
    .ok_or("accepted")?;

    for error in [empty, spaces] {
        assert!(matches!(error, ConfigError::Gang(_)), "{error}");
        let message = chain(&error);
        assert!(message.contains("client_secret is empty"), "{message}");
    }
    Ok(())
}

#[test]
fn login_reads_a_secret_file_path() -> Result<(), Box<dyn Error>> {
    // The file does not exist on the test host, so a parse that opened it would fail.
    let config = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \"c\"\nclient_secret_file = \"/run/secrets/dashboard\"\n",
    )?;
    let login = config.dashboard.login.as_ref().ok_or("login missing")?;

    assert!(matches!(
        &login.client_secret,
        ClientSecretSource::File(path) if path.as_path() == Path::new("/run/secrets/dashboard")
    ));
    Ok(())
}

#[test]
fn login_with_both_secret_keys_is_rejected() -> Result<(), Box<dyn Error>> {
    let error = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \"c\"\nclient_secret = \"s\"\nclient_secret_file = \"/run/secrets/dashboard\"\n",
    )
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)), "{error}");
    let message = chain(&error);
    assert!(message.contains("client_secret_file"), "{message}");
    Ok(())
}

#[test]
fn login_with_neither_secret_key_is_rejected() -> Result<(), Box<dyn Error>> {
    let error = with_dashboard("[dashboard.login]\nprovider = \"google\"\nclient_id = \"c\"\n")
        .err()
        .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)), "{error}");
    let message = chain(&error);
    assert!(message.contains("client_secret_file"), "{message}");
    Ok(())
}

#[test]
fn login_with_a_relative_secret_file_is_rejected() -> Result<(), Box<dyn Error>> {
    let error = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \"c\"\nclient_secret_file = \"secrets/dashboard\"\n",
    )
    .err()
    .ok_or("accepted")?;

    assert!(matches!(error, ConfigError::Gang(_)), "{error}");
    let message = chain(&error);
    assert!(message.contains("not absolute"), "{message}");
    Ok(())
}

#[test]
fn loading_a_secret_file_trims_surrounding_whitespace() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("secret");
    std::fs::write(&path, "  value\n")?;

    let secret = ClientSecretSource::File(AbsolutePathBuf::try_new(path)?).load()?;

    assert_eq!(secret.expose(), "value");
    Ok(())
}

#[test]
fn loading_an_empty_secret_file_names_the_path() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("secret");
    std::fs::write(&path, " \n\t\n")?;

    let error = ClientSecretSource::File(AbsolutePathBuf::try_new(path.clone())?)
        .load()
        .err()
        .ok_or("loaded")?;

    assert!(
        matches!(&error, SecretFileError::Empty { path: named } if *named == path),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains(&path.display().to_string()), "{message}");
    Ok(())
}

#[test]
fn loading_a_missing_secret_file_is_a_read_error() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("missing");

    let error = ClientSecretSource::File(AbsolutePathBuf::try_new(path)?)
        .load()
        .err()
        .ok_or("loaded")?;

    assert!(matches!(error, SecretFileError::Read { .. }), "{error}");
    Ok(())
}

#[test]
fn an_inline_secret_loads_as_itself() -> Result<(), Box<dyn Error>> {
    let secret = ClientSecretSource::Inline(ClientSecret::new("inline-value")).load()?;

    assert_eq!(secret.expose(), "inline-value");
    Ok(())
}

#[test]
fn the_secret_never_appears_in_debug_output() -> Result<(), Box<dyn Error>> {
    let config = with_dashboard(
        "[dashboard.login]\nprovider = \"google\"\nclient_id = \"c\"\nclient_secret = \"s3cr3t-dashboard-value\"\n",
    )?;
    let login = config.dashboard.login.as_ref().ok_or("login missing")?;

    let debug = format!("{login:?}");
    assert!(!debug.contains("s3cr3t-dashboard-value"), "{debug}");
    Ok(())
}

#[test]
fn unknown_dashboard_key_is_rejected_and_named() -> Result<(), Box<dyn Error>> {
    let message = rejection(&Sections {
        dashboard: "[dashboard]\nbogus = 1\n",
        ..Sections::VALID
    })?;

    assert!(message.contains("bogus"), "{message}");
    Ok(())
}

/// Scheduler fixture exposing a configurable worker endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerEndpointSection {
    scheduler_name: String,
    worker_endpoint: Option<Url>,
}

/// Scheduler selections for worker endpoint validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkerEndpointScheduler {
    Nativelink(WorkerEndpointSection),
}

impl SchedulerSettings for WorkerEndpointScheduler {
    fn worker_endpoint(&self) -> Option<&Url> {
        match self {
            Self::Nativelink(section) => section.worker_endpoint.as_ref(),
        }
    }

    fn worker_endpoint_key(&self) -> Option<&'static str> {
        Some("worker_endpoint")
    }
}

/// Plugin composition using the worker endpoint scheduler fixture.
#[derive(Debug)]
struct WorkerEndpointPlugins;

impl PluginSet for WorkerEndpointPlugins {
    type Provisioner = ProvisionerConfig;
    type Cache = CacheConfig;
    type Signer = SignerConfig;
    type Scheduler = WorkerEndpointScheduler;
    type Database = DatabaseConfig;
    type SandboxKey = SandboxKey;
    type Sandbox = SandboxConfig;
    type IdentityProvider = IdentityProviderConfig;
    type Limits = LimitsConfig;
    type FleetOptions = FleetOptions;
    type ClientAuthentication = ClientAuthenticationConfig;
    type WorkerAuthentication = WorkerAuthenticationConfig;

    fn fleet_options(
        provisioner: &ProvisionerConfig,
        table: FleetOptionsTable,
    ) -> Result<FleetOptions, FleetOptionsError> {
        Plugins::fleet_options(provisioner, table)
    }

    fn option_properties(options: &FleetOptions) -> Vec<(&'static str, String)> {
        Plugins::option_properties(options)
    }
}

#[test]
fn an_omitted_worker_endpoint_on_a_non_loopback_worker_listen_is_rejected()
-> Result<(), Box<dyn Error>> {
    let sections = Sections {
        top: TOP
            .replace(
                "worker_listen = \"127.0.0.1:9445\"",
                "worker_listen = \"0.0.0.0:9445\"",
            )
            .leak(),
        ..Sections::VALID
    };
    let error = sections
        .text()
        .parse::<GangConfig<WorkerEndpointPlugins>>()
        .err()
        .ok_or("accepted a missing worker endpoint")?;
    assert!(
        matches!(
            &error,
            ConfigError::WorkerEndpointRequired { key: "worker_endpoint", listen }
                if listen.to_string() == "0.0.0.0:9445"
        ),
        "{error}"
    );
    let message = chain(&error);
    assert!(message.contains("worker_endpoint"), "{message}");
    assert!(message.contains("0.0.0.0:9445"), "{message}");
    Ok(())
}

#[test]
fn a_plaintext_worker_endpoint_under_tls_is_rejected() -> Result<(), Box<dyn Error>> {
    let sections = Sections {
        scheduler: "[scheduler.nativelink]\nscheduler_name = \"MAIN_SCHEDULER\"\nworker_endpoint = \"grpc://scheduler.example.com:9445\"\n",
        ..Sections::VALID
    };
    let error = sections
        .text()
        .parse::<GangConfig<WorkerEndpointPlugins>>()
        .err()
        .ok_or("accepted a plaintext worker endpoint under TLS")?;
    assert!(
        matches!(
            &error,
            ConfigError::WorkerEndpointNotGrpcs(endpoint)
                if endpoint.as_str() == "grpc://scheduler.example.com:9445"
                    || endpoint.as_str() == "grpc://scheduler.example.com:9445/"
        ),
        "{error}"
    );
    let message = chain(&error);
    assert!(
        message.contains("grpc://scheduler.example.com:9445"),
        "{message}"
    );
    assert!(message.contains("grpcs"), "{message}");
    Ok(())
}

#[test]
fn worker_endpoint_scheme_respects_the_server_transport() -> Result<(), Box<dyn Error>> {
    for (server, authentication, scheme) in [
        (SERVER, AUTHENTICATION, "grpcs"),
        (PLAINTEXT_SERVER, PLAINTEXT_AUTHENTICATION, "grpc"),
        (PLAINTEXT_SERVER, PLAINTEXT_AUTHENTICATION, "grpcs"),
    ] {
        let scheduler = format!(
            "[scheduler.nativelink]\nscheduler_name = \"MAIN_SCHEDULER\"\n\
             worker_endpoint = \"{scheme}://scheduler.example.com:9445\"\n"
        );
        Sections {
            server,
            authentication,
            scheduler: scheduler.leak(),
            ..Sections::VALID
        }
        .text()
        .parse::<GangConfig<WorkerEndpointPlugins>>()?;
    }
    Ok(())
}

#[test]
fn plaintext_rejects_a_non_grpc_worker_endpoint() -> Result<(), Box<dyn Error>> {
    let sections = Sections {
        server: PLAINTEXT_SERVER,
        authentication: PLAINTEXT_AUTHENTICATION,
        scheduler: "[scheduler.nativelink]\nscheduler_name = \"MAIN_SCHEDULER\"\nworker_endpoint = \"https://scheduler.example.com:9445\"\n",
        ..Sections::VALID
    };
    let error = sections
        .text()
        .parse::<GangConfig<WorkerEndpointPlugins>>()
        .err()
        .ok_or("accepted a non-grpc worker endpoint")?;
    assert!(
        matches!(&error, ConfigError::WorkerEndpointNotGrpc(_)),
        "{error}"
    );
    Ok(())
}

#[test]
fn an_omitted_worker_endpoint_on_loopback_is_accepted() -> Result<(), Box<dyn Error>> {
    Sections::VALID
        .text()
        .parse::<GangConfig<WorkerEndpointPlugins>>()?;
    Ok(())
}

#[test]
fn a_scheduler_without_a_worker_endpoint_key_accepts_a_public_worker_listener()
-> Result<(), Box<dyn Error>> {
    Sections {
        top: TOP
            .replace(
                "worker_listen = \"127.0.0.1:9445\"",
                "worker_listen = \"0.0.0.0:9445\"",
            )
            .leak(),
        ..Sections::VALID
    }
    .parse()?;
    Ok(())
}

#[test]
fn worker_enrollment_client_limits_parse_from_the_worker_section() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        worker: "[worker]\nenrollment_request_timeout = \"23s\"\nenrollment_max_response_size = \"3MiB\"\n",
        ..Sections::VALID
    }
    .parse()?;
    assert_eq!(
        config.worker.enrollment_request_timeout,
        NonZeroDuration::from_secs(23)
    );
    assert_eq!(
        config.worker.enrollment_max_response_size,
        NonZeroBytes::mib(3)
    );
    Ok(())
}

#[test]
fn worker_enrollment_client_limits_default_when_omitted() -> Result<(), Box<dyn Error>> {
    let config = Sections {
        worker: "[worker]\n",
        ..Sections::VALID
    }
    .parse()?;
    assert_eq!(
        config.worker.enrollment_request_timeout,
        NonZeroDuration::from_secs(10)
    );
    assert_eq!(
        config.worker.enrollment_max_response_size,
        NonZeroBytes::mib(1)
    );
    Ok(())
}

#[test]
fn a_hostless_worker_endpoint_is_rejected() -> Result<(), Box<dyn Error>> {
    let sections = Sections {
        scheduler: "[scheduler.nativelink]\nscheduler_name = \"MAIN_SCHEDULER\"\nworker_endpoint = \"grpcs:worker\"\n",
        ..Sections::VALID
    };
    let error = sections
        .text()
        .parse::<GangConfig<WorkerEndpointPlugins>>()
        .err()
        .ok_or("accepted a hostless worker endpoint")?;
    assert!(
        matches!(&error, ConfigError::WorkerEndpointWithoutHost(endpoint) if endpoint.as_str() == "grpcs:worker"),
        "{error}"
    );
    assert_eq!(
        error.to_string(),
        "worker endpoint `grpcs:worker` has no host"
    );
    Ok(())
}
