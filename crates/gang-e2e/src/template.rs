//! The `gang.toml` template of a run: the base configuration, adapted to one placement, one
//! variant, and one transport. Every key a scenario does not name is carried over unchanged.

use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::Duration;

use gang_config::{
    ClientSecretSource, DashboardLogin, Document, RawFleet, RoleConfig, RolePrincipal,
};
use gang_local::LoopbackAddr;
use gang_main::plugins::{
    CacheConfig, ClientAuthenticationConfig, DatabaseConfig, IdentityProviderConfig, LimitsConfig,
    Plugins, ProvisionerConfig, SandboxKey, SignerConfig, WorkerAuthenticationConfig,
};
use gang_types::{
    AbsolutePathBuf, Bytes, CaPin, Capability, ClientSecret, DeviceCodeTtl, Isa, NonZeroBytes,
    NonZeroDuration, Percent, Permission, PermissionSet, ProviderName, SessionMaxAge, TokenTtl,
    Url,
};

use crate::access::{IdentityKnobs, Sessions, SignIn, StandingWorker};
use crate::browser::{SESSION_MAX_AGE, SESSION_TOKEN_TTL};
use crate::control::ControlKnobs;
use crate::errors::{E2eError, Result};
use crate::execution::{
    ActionTimeout, CERTIFICATE_REFRESH_PERCENT, ColdStart, ESCALATION_MAX_STEPS,
    ESCALATION_PERCENT, ExecutionKnobs, FleetDisk, MISFIT_COLD_START_MEMORY, MemoryEscalation,
    OVERSIZED_FLEET_DISK, REFUSAL_RELAUNCH_BACKOFF, SHORT_ACTION_TIMEOUT_SECS,
    WORKER_CERTIFICATE_TTL_SECS, WorkerCertificates,
};
use crate::issuer::{
    AUDIENCE, CONSENT_PROMPT, DASHBOARD_CLIENT_ID, DASHBOARD_CLIENT_SECRET, OFFLINE_ACCESS,
    ORGANIZATION, ORGANIZATION_CLAIM,
};
use crate::placement::Placement;
use crate::server::{
    CERTIFICATE_BACKDATE, CERTIFICATE_TTL, CertificateLifetime, MAX_REQUEST_SIZE, RATE_LIMIT_BURST,
    RATE_LIMIT_REFILL, REISSUE_AT_PERCENT, RequestLimits, ServerKnobs, TRUSTED_PROXIES,
    VIEWER_RETRY_AFTER, ViewerLimit,
};
use crate::settings::ActionTools;
use crate::system::{
    ActionLimits, FleetLayout, LARGE_FLEET, LIMIT_CPU_PERIOD_MICROS, LIMIT_MAX_PIDS, NOOP_FLEET,
    PermitNetwork, SMALL_FLEET, Sandbox, SoftMemory, Variant,
};

/// The `max_instances` of [`SMALL_FLEET`] and [`NOOP_FLEET`].
const SMALL_MAX_INSTANCES: NonZeroU32 = NonZeroU32::MIN;

/// The `max_instances` of [`LARGE_FLEET`] in the placement layout.
const LARGE_MAX_INSTANCES: u32 = 2;

/// The factor by which [`LARGE_FLEET`]'s instances exceed the base fleet's.
const LARGE_FACTOR: u32 = 2;

/// Four unreserved cold actions fit one 6 GiB worker instead of scaling out.
const COLD_START_MEMORY: &str = "1536MiB";

/// Two unreserved cold lifecycle actions must fit one worker's [`FLEET_DISK`] too.
const COLD_START_DISK: &str = "2GiB";

/// The `disk_per_instance` of every Local Mode fleet the harness starts. A worker refuses a fleet
/// disk that its scratch volume's free space, less the fast content store's budget, cannot hold,
/// so the gate must not inherit the checked-in 20GiB that may exceed the available disk space. It
/// holds at least two of the harness's cold-start disks ([`COLD_START_DISK`]), so cold lifecycle
/// actions share one worker as they share its memory, and it needs about 16GiB free under the
/// default fast content store share of 50%.
const FLEET_DISK: &str = "8GiB";

/// The identity provider of the in-process issuer: the key of its `[identity.providers.<key>]`
/// section, and the provider that administers the server.
pub(crate) const PROVIDER: &str = "e2e";

/// How long the identity scenario's device authorization remains valid.
pub(crate) const DEVICE_TTL: Duration = Duration::from_mins(1);

/// `[server.rate_limit.client] burst` of the identity deployment. Every check comes from the same
/// loopback address, so the default bucket would let the scenario's own volume, not the
/// behaviour under test, decide its later checks.
const IDENTITY_CLIENT_BURST: NonZeroU32 = NonZeroU32::new(120).expect("120 is non-zero");

/// The principal of the administrator's token, under [`PROVIDER`].
const ADMINISTRATOR_PRINCIPAL: &str = "administrator";

/// Which transport the endpoints of the configuration use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Plain HTTP/2: endpoints are `grpc://`, and the server runs without TLS.
    Plain,
    /// HTTP/2 over TLS: endpoints are `grpcs://`.
    Tls,
}

impl Transport {
    /// The URL scheme of the public endpoint of this transport. Bazel maps it to the scheme of
    /// the endpoints: `http` to `grpc`, `https` to `grpcs`.
    pub(crate) const fn endpoint_scheme(self) -> &'static str {
        match self {
            Self::Plain => "http",
            Self::Tls => "https",
        }
    }
}

/// Rewrites placement-dependent settings shared by scenario and checked-in configurations.
///
/// AWS-backed plugins are retained. Local identity providers that trusted the old mock IMDS
/// address are pointed to the new one, and file-backed dashboard secrets are materialized under
/// the placement.
/// Local Mode fleets get the harness's [`FLEET_DISK`].
pub(crate) fn place(
    document: &mut Document<Plugins>,
    placement: &Placement,
    transport: Transport,
) -> Result<()> {
    document.listen = SocketAddr::from((Ipv4Addr::LOCALHOST, placement.public_port()));
    document.worker_listen = SocketAddr::from((Ipv4Addr::LOCALHOST, placement.worker_port()));
    document.endpoint = None;
    document.worker_endpoint = None;
    document.actions.cold_start.memory = COLD_START_MEMORY.parse().map_err(template_error)?;
    document.actions.cold_start.disk = COLD_START_DISK.parse().map_err(template_error)?;
    if let ProvisionerConfig::Local(local) = &mut document.provisioner {
        let disk = FLEET_DISK.parse::<NonZeroBytes>().map_err(template_error)?;
        document.actions.memory.reserve = NonZeroBytes::mib(32);
        let memory = NonZeroBytes::gib(6);
        for fleet in &mut document.fleets {
            fleet.disk_per_instance = disk;
            fleet.memory_per_instance = memory;
        }
        let old = format!("http://{}", local.mock_imds_listen.get());
        local.state_dir = absolute(placement.state_dir(), "the placement's state directory")?;
        local.mock_imds_listen = LoopbackAddr::try_from(SocketAddr::from((
            Ipv4Addr::LOCALHOST,
            placement.imds_port(),
        )))
        .map_err(E2eError::system_from("the placement's imds address"))?;
        let issuer = Url::parse(&format!("http://127.0.0.1:{}", placement.imds_port()))
            .map_err(E2eError::parse("the placement's imds url"))?;
        for provider in document.identity.providers.values_mut() {
            let IdentityProviderConfig::Oidc(oidc) = provider;
            if oidc.issuer_url.as_url().as_str().trim_end_matches('/') == old {
                oidc.issuer_url = issuer
                    .clone()
                    .try_into()
                    .map_err(E2eError::system_from("the placement's imds issuer"))?;
            }
        }
    }
    if let CacheConfig::Filesystem(cache) = &mut document.cache {
        cache.path = absolute(placement.cache_dir(), "the placement's cache directory")?;
    }
    if let SignerConfig::File(signer) = &mut document.signer {
        signer.key_file = absolute(placement.ca_key(), "the placement's CA key")?;
    }
    match &mut document.database {
        DatabaseConfig::Sqlite(sqlite) => sqlite.path = placement.database(),
        DatabaseConfig::Postgres(_) | DatabaseConfig::Dynamodb(_) => {}
    }
    if let Some(noop) = &mut document.sandbox.noop {
        noop.scratch_dir = Some(absolute(
            placement.noop_scratch(),
            "the placement's noop scratch directory",
        )?);
    }
    if let Some(login) = &mut document.dashboard.login
        && let ClientSecretSource::File(path) = &mut login.client_secret
    {
        let secret = placement.root().join("dashboard-client-secret");
        fs::write(&secret, "example-client-secret\n")
            .map_err(E2eError::io(format!("writing {}", secret.display())))?;
        *path = absolute(secret, "the placement's dashboard client secret")?;
    }
    if transport == Transport::Plain {
        document.identity.providers.clear();
        document.server.transport = gang_config::Transport::Plaintext;
        document.server.ca_pin = None;
        document.server.roles = vec![RoleConfig::anonymous_administrator()];
        document.authentication.clients =
            ClientAuthenticationConfig::None(gang_authn::NoneConfig {});
        document.authentication.workers =
            WorkerAuthenticationConfig::None(gang_authn::NoneConfig {});
    }
    Ok(())
}

/// Converts a placement path to the absolute path required by plugin settings.
fn absolute(path: PathBuf, what: &'static str) -> Result<AbsolutePathBuf> {
    AbsolutePathBuf::try_new(path).map_err(E2eError::system_from(what))
}

/// The typed base configuration of a run.
#[derive(Debug)]
pub struct ConfigTemplate {
    document: Document<Plugins>,
}

fn render_resource_policy(
    document: &mut Document<Plugins>,
    policy: &crate::execution::ResourcePolicy,
) {
    document.actions.memory = policy.memory;
    document.actions.reservation_headroom_percent = policy.reservation_headroom_percent;
    document.actions.max_job_retries = policy.max_job_retries;
    if let Some(memory) = policy.fleet_memory {
        if document.actions.cold_start.memory.get().get() > memory.as_u64() {
            document.actions.cold_start.memory =
                gang_config::WholeKib::from_units(policy.memory.min_reservation.as_units());
        }
        for fleet in &mut document.fleets {
            fleet.memory_per_instance = memory;
        }
    }
    if let Some(vcpus) = policy.fleet_vcpus {
        for fleet in &mut document.fleets {
            fleet.vcpus_per_instance = vcpus;
            fleet.target_actions_per_instance = fleet
                .target_actions_per_instance
                .max(NonZeroU32::MIN.saturating_add(1));
        }
    }
    if let Some(cold_start) = policy.cold_start {
        document.actions.cold_start = cold_start;
    }
}

/// Prepares the first fleet and sandbox from the same action environment.
pub(crate) fn prepare_sandbox(
    document: &mut Document<Plugins>,
    sandbox: Sandbox,
    tools: &ActionTools,
) -> Result<()> {
    let fleet = document
        .fleets
        .first_mut()
        .ok_or_else(|| template_error("missing the first [[fleets]] entry"))?;
    fleet.sandbox = match sandbox {
        Sandbox::Bwrap => SandboxKey::Bwrap,
        Sandbox::Noop => SandboxKey::Noop,
    };
    if let Some(noop) = document.sandbox.noop.as_mut() {
        noop.environment = "host:e2e".parse().map_err(template_error)?;
    }
    match sandbox {
        Sandbox::Bwrap => {
            let bwrap = document
                .sandbox
                .bwrap
                .as_mut()
                .ok_or_else(|| template_error("missing [sandbox.bwrap]"))?;
            bwrap.program = tools.bwrap_program()?.to_path_buf();
            let root = tools
                .action_rootfs
                .as_ref()
                .ok_or_else(|| template_error("GANG_E2E_ACTION_ROOTFS is required for bwrap"))?;
            let root = fs::canonicalize(root)
                .map_err(E2eError::io("canonicalizing the action rootfs envelope"))?;
            fleet.action_environment = fs::read_to_string(root.join("identity"))
                .map_err(E2eError::io("reading action rootfs identity"))?
                .trim()
                .parse()
                .map_err(template_error)?;
            bwrap.root = absolute(root, "the action rootfs envelope")?;
        }
        Sandbox::Noop => {
            fleet.action_environment = "host:e2e".parse().map_err(template_error)?;
            if document.sandbox.noop.is_none() {
                return Err(template_error("missing [sandbox.noop]"));
            }
        }
    }
    Ok(())
}

impl ConfigTemplate {
    /// Parses the base configuration text.
    ///
    /// # Errors
    /// Returns a template error when the configuration is malformed or has invalid fields.
    pub fn parse(base: &str) -> Result<Self> {
        Ok(Self {
            document: Document::<Plugins>::parse(base)
                .map_err(|error| template_error(gang_types::error_chain(&error)))?,
        })
    }

    /// Rewrites placement-dependent settings for the requested transport.
    ///
    /// # Errors
    /// Returns an error if a placement path or plugin setting cannot be constructed.
    pub fn place(mut self, placement: &Placement, transport: Transport) -> Result<Self> {
        place(&mut self.document, placement, transport)?;
        Ok(self)
    }

    /// Wraps the Local launcher with the harness's FIFO gate, retaining its entire argv.
    pub(crate) fn with_launch_gate(
        mut self,
        gate: Option<&Path>,
        timeout: Duration,
    ) -> Result<Self> {
        let Some(gate) = gate else {
            return Ok(self);
        };
        let ProvisionerConfig::Local(local) = &mut self.document.provisioner else {
            return Err(template_error("missing [provisioner.local]"));
        };
        let executable =
            std::env::current_exe().map_err(E2eError::io("locating the harness executable"))?;
        let executable = executable
            .to_str()
            .ok_or_else(|| template_error("harness executable path is not UTF-8"))?;
        let gate = gate
            .to_str()
            .ok_or_else(|| template_error("launch gate path is not UTF-8"))?;
        let mut launcher = vec![
            executable.to_owned(),
            "--gang-e2e-launch-gate".to_owned(),
            gate.to_owned(),
            timeout.as_millis().to_string(),
        ];
        launcher.extend(local.worker_launcher.argv().iter().cloned());
        local.worker_launcher = launcher.try_into().map_err(template_error)?;
        Ok(self)
    }

    fn project_egress(
        document: &mut Document<Plugins>,
        variant: &Variant,
        _tools: &ActionTools,
    ) -> Result<()> {
        let fleet = document
            .fleets
            .first_mut()
            .ok_or_else(|| template_error("missing the first [[fleets]] entry"))?;
        fleet.sandbox = match variant.sandbox {
            Sandbox::Bwrap => SandboxKey::Bwrap,
            Sandbox::Noop => SandboxKey::Noop,
        };
        fleet.actions.grants.remove(&Capability::Network);
        if variant.permit_network == PermitNetwork::Permitted {
            if variant.sandbox != Sandbox::Bwrap {
                return Err(template_error("network grants require the bwrap sandbox"));
            }
            fleet.actions.grants.insert(Capability::Network);
        }
        Ok(())
    }

    fn project_limits(
        document: &mut Document<Plugins>,
        variant: &Variant,
        worker_launcher: &[String],
    ) -> Result<()> {
        match variant.limits {
            ActionLimits::Absent => {
                document.limits = None;
                if let ProvisionerConfig::Local(local) = &mut document.provisioner {
                    local.worker_launcher = gang_local::WorkerLauncher::default();
                }
            }
            ActionLimits::Cgroup => {
                let config = gang_cgroup::CgroupConfig {
                    delegation: gang_cgroup::Delegation::Trusted,
                    max_pids: NonZeroU32::new(LIMIT_MAX_PIDS)
                        .ok_or_else(|| template_error("max pids must be nonzero"))?,
                    cpu_period: gang_cgroup::CpuPeriod::try_new(Duration::from_micros(
                        LIMIT_CPU_PERIOD_MICROS,
                    ))
                    .map_err(template_error)?,
                    ..Default::default()
                };
                document.limits = Some(LimitsConfig::Cgroup(config));
                let ProvisionerConfig::Local(local) = &mut document.provisioner else {
                    return Err(template_error("missing [provisioner.local]"));
                };
                let mut launcher = worker_launcher.to_vec();
                if let Some(policy) = &variant.execution.resource_policy
                    && let Some(bound) = policy.worker_memory_max
                {
                    let separator =
                        launcher
                            .iter()
                            .position(|word| word == "--")
                            .ok_or_else(|| {
                                template_error("worker launcher must contain -- for MemoryMax")
                            })?;
                    launcher.insert(separator, format!("--property=MemoryMax={}", bound.get()));
                }
                local.worker_launcher = launcher.try_into().map_err(template_error)?;
            }
        }
        Ok(())
    }

    /// Applies the scenario's sandbox, fleet, execution, control and server knobs.
    ///
    /// # Errors
    /// Returns an error when the configuration cannot support the selected variant.
    pub fn vary(
        mut self,
        variant: &Variant,
        worker_launcher: &[String],
        tools: &ActionTools,
    ) -> Result<Self> {
        let document = &mut self.document;
        Self::project_egress(document, variant, tools)?;
        prepare_sandbox(document, variant.sandbox, tools)?;
        if variant.fleets == FleetLayout::Placement {
            if document.sandbox.noop.is_none() {
                return Err(template_error("missing [sandbox.noop]"));
            }
            placement_fleets(&mut document.fleets)?;
        }
        document.execution.input_links = variant.input_links;
        Self::project_limits(document, variant, worker_launcher)?;
        if variant.soft_memory == SoftMemory::Enforced {
            document.actions.memory.enforcement = gang_config::MemoryEnforcement::Soft;
        }
        if variant.soft_disk {
            document.actions.disk.enforcement = gang_config::DiskEnforcement::Soft;
            // Keep the disk-hog probe's 4 MiB reservation below its 64 MiB output.
            document.actions.disk.min_reservation =
                gang_config::DiskMinReservation::from_units(1024);
        }
        render_control(document, &variant.control)?;
        render_server(document, variant.server)?;
        render_execution(document, variant.execution)?;
        if let Some(policy) = &variant.execution.resource_policy {
            render_resource_policy(document, policy);
        }
        Ok(self)
    }

    /// Adds the test issuer and grants its administrator all permissions.
    ///
    /// # Errors
    /// Returns an error if the issuer, names, or client authentication are invalid.
    pub fn with_identity(mut self, issuer: &Url, knobs: IdentityKnobs) -> Result<Self> {
        let name: ProviderName = PROVIDER.parse().map_err(template_error)?;
        let existing = self.document.identity.providers.remove(&name);
        let mut oidc = match existing {
            Some(IdentityProviderConfig::Oidc(oidc)) => oidc,
            None => gang_oidc::OidcConfig {
                issuer_url: issuer.clone().try_into().map_err(template_error)?,
                audience: AUDIENCE.parse().map_err(template_error)?,
                additional_audiences: Vec::new(),
                principal_claim: "sub".to_owned(),
                required_claims: std::collections::BTreeMap::default(),
                login: None,
                cache: gang_oidc::OidcCache::default(),
                http: gang_oidc::OidcHttp::default(),
                tokens: gang_oidc::OidcTokens::default(),
            },
        };
        oidc.issuer_url = issuer.clone().try_into().map_err(template_error)?;
        oidc.audience = AUDIENCE.parse().map_err(template_error)?;
        "sub".clone_into(&mut oidc.principal_claim);
        oidc.required_claims = [(ORGANIZATION_CLAIM.to_owned(), ORGANIZATION.into())].into();
        self.document
            .identity
            .providers
            .insert(name.clone(), IdentityProviderConfig::Oidc(oidc));
        self.document.server.roles = vec![RoleConfig {
            name: "administrator".parse().map_err(template_error)?,
            permissions: PermissionSet::all(),
            principals: vec![RolePrincipal {
                provider: name.clone(),
                principal: ADMINISTRATOR_PRINCIPAL.parse().map_err(template_error)?,
            }],
        }];
        let ClientAuthenticationConfig::Token(token) = &mut self.document.authentication.clients
        else {
            return Err(template_error("missing [authentication.clients.token]"));
        };
        token.set_providers([name]).map_err(template_error)?;
        render_identity(&mut self.document, knobs)?;
        Ok(self)
    }

    /// Pins the server's certificate authority.
    ///
    /// # Errors
    /// Returns a template error if the pin cannot be applied.
    pub fn with_ca_pin(mut self, pin: &CaPin) -> Result<Self> {
        self.document = self.document.pinned(*pin);
        Ok(self)
    }

    /// Returns the file-backed signer's key path.
    ///
    /// # Errors
    /// Returns a template error when the signer is not file-backed.
    pub fn signer_key_file(&self) -> Result<PathBuf> {
        let SignerConfig::File(signer) = &self.document.signer else {
            return Err(template_error("missing [signer.file] key_file"));
        };
        Ok(signer.key_file.as_path().to_path_buf())
    }

    /// Serializes the typed configuration as TOML.
    ///
    /// # Errors
    /// Returns a template error if serialization fails.
    pub fn render(&self) -> Result<String> {
        self.document.to_toml().map_err(template_error)
    }

    /// Returns the configured default ISA.
    ///
    /// # Errors
    /// Returns a template error if the ISA cannot be read.
    pub fn default_isa(&self) -> Result<Isa> {
        Ok(self.document.default_isa)
    }

    /// Returns the large fleet's memory in KiB.
    ///
    /// # Errors
    /// Returns a template error if the first fleet is absent or doubling its size overflows.
    pub fn large_memory_kb(&self) -> Result<u64> {
        let fleet = self
            .document
            .fleets
            .first()
            .ok_or_else(|| template_error("missing the first [[fleets]] entry"))?;
        Ok(enlarged(fleet)?.1.as_u64() / 1024)
    }
}

/// Maps a template failure to its public error.
pub(crate) fn template_error(error: impl std::fmt::Display) -> E2eError {
    E2eError::Template(error.to_string())
}

/// Doubles a fleet's CPU and memory, checking both bounds.
fn enlarged(base: &RawFleet<SandboxKey>) -> Result<(NonZeroU32, NonZeroBytes)> {
    let overflow = || template_error("the large fleet's size overflows");
    let vcpus = base
        .vcpus_per_instance
        .get()
        .checked_mul(LARGE_FACTOR)
        .and_then(NonZeroU32::new)
        .ok_or_else(overflow)?;
    let memory = base
        .memory_per_instance
        .as_u64()
        .checked_mul(u64::from(LARGE_FACTOR))
        .and_then(|bytes| NonZeroBytes::new(Bytes::new(bytes)))
        .ok_or_else(overflow)?;
    Ok((vcpus, memory))
}

/// Copies one raw fleet with a new placement name and capacity.
fn placement_fleet(
    base: &RawFleet<SandboxKey>,
    name: &str,
    max: NonZeroU32,
) -> Result<RawFleet<SandboxKey>> {
    Ok(RawFleet {
        name: name.parse().map_err(template_error)?,
        isa: base.isa,
        action_environment: base.action_environment.clone(),
        min_instances: 0,
        max_instances: max,
        vcpus_per_instance: base.vcpus_per_instance,
        memory_per_instance: base.memory_per_instance,
        disk_per_instance: base.disk_per_instance,
        target_actions_per_instance: NonZeroU32::MIN,
        idle_timeout: base.idle_timeout,
        max_drain_wait: base.max_drain_wait,
        boot_grace: base.boot_grace,
        sandbox: base.sandbox,
        actions: base.actions.clone(),
        options: base.options.clone(),
        scheduled_minimum: Vec::new(),
    })
}

/// Replaces the single base fleet with the three placement fleets.
fn placement_fleets(fleets: &mut Vec<RawFleet<SandboxKey>>) -> Result<()> {
    if fleets.len() != 1 {
        return Err(template_error(format!(
            "the placement layout needs exactly one [[fleets]] entry in the base, found {}",
            fleets.len()
        )));
    }
    let base = fleets
        .first()
        .ok_or_else(|| template_error("missing the first [[fleets]] entry"))?;
    let (vcpus, memory) = enlarged(base)?;
    let small = placement_fleet(base, SMALL_FLEET, SMALL_MAX_INSTANCES)?;
    let max = NonZeroU32::new(LARGE_MAX_INSTANCES)
        .ok_or_else(|| template_error("large fleet maximum must be nonzero"))?;
    let mut large = placement_fleet(base, LARGE_FLEET, max)?;
    large.vcpus_per_instance = vcpus;
    large.memory_per_instance = memory;
    let mut noop = placement_fleet(base, NOOP_FLEET, SMALL_MAX_INSTANCES)?;
    noop.sandbox = SandboxKey::Noop;
    noop.action_environment = "host:e2e".parse().map_err(template_error)?;
    noop.actions.grants.remove(&Capability::Network);
    *fleets = vec![small, large, noop];
    Ok(())
}

/// Applies execution scenario tuning.
fn render_execution(document: &mut Document<Plugins>, knobs: ExecutionKnobs) -> Result<()> {
    if knobs.action_timeout == ActionTimeout::Short {
        document.actions.timeout = Duration::from_secs(u64::from(SHORT_ACTION_TIMEOUT_SECS))
            .try_into()
            .map_err(template_error)?;
    }
    if knobs.memory_escalation == MemoryEscalation::OneStep {
        document.actions.memory.max_steps = NonZeroU32::new(ESCALATION_MAX_STEPS)
            .ok_or_else(|| template_error("escalation steps must be nonzero"))?;
        document.actions.memory.escalation_percent =
            ESCALATION_PERCENT.try_into().map_err(template_error)?;
    }
    if knobs.cold_start == ColdStart::Misfit {
        document.actions.cold_start.memory =
            MISFIT_COLD_START_MEMORY.parse().map_err(template_error)?;
    }
    if knobs.fleet_disk == FleetDisk::Oversized {
        document
            .fleets
            .first_mut()
            .ok_or_else(|| template_error("missing the first [[fleets]] entry"))?
            .disk_per_instance = OVERSIZED_FLEET_DISK.parse().map_err(template_error)?;
        let ProvisionerConfig::Local(local) = &mut document.provisioner else {
            return Err(template_error("missing [provisioner.local]"));
        };
        local.relaunch_backoff = REFUSAL_RELAUNCH_BACKOFF.parse().map_err(template_error)?;
    }
    if knobs.worker_certificates == WorkerCertificates::ShortLived {
        let WorkerAuthenticationConfig::Mtls(mtls) = &mut document.authentication.workers else {
            return Err(template_error("missing [authentication.workers.mtls]"));
        };
        mtls.certificate_ttl = NonZeroDuration::from_secs(WORKER_CERTIFICATE_TTL_SECS);
        document.worker.certificate_refresh_percent =
            Percent::from_percent(CERTIFICATE_REFRESH_PERCENT);
    }
    Ok(())
}

/// Adds interactive login settings to the test provider.
fn configure_login(oidc: &mut gang_oidc::OidcConfig, subject: &str) -> Result<()> {
    oidc.additional_audiences = vec![DASHBOARD_CLIENT_ID.parse().map_err(template_error)?];
    oidc.login = Some(gang_oidc::LoginConfig {
        client_secret: None,
        scopes: vec!["openid".to_owned()],
        authorization_params: [("login_hint".to_owned(), subject.to_owned())].into(),
        offline: gang_oidc::OfflineLogin {
            scopes: Vec::new(),
            authorization_params: [OFFLINE_ACCESS, CONSENT_PROMPT]
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .into(),
        },
    });
    Ok(())
}

/// Applies identity scenario tuning after the test issuer has been configured.
fn render_identity(document: &mut Document<Plugins>, knobs: IdentityKnobs) -> Result<()> {
    if knobs.standing_worker == StandingWorker::One
        && knobs.sessions == Sessions::Default
        && knobs.sign_in != SignIn::Absent
    {
        document.dashboard.device.ttl =
            DeviceCodeTtl::try_new(DEVICE_TTL).map_err(template_error)?;
        document.server.rate_limit.client.burst = IDENTITY_CLIENT_BURST;
    }
    if let Some(subject) = knobs.sign_in.subject(knobs.sessions) {
        let provider: ProviderName = PROVIDER.parse().map_err(template_error)?;
        let Some(IdentityProviderConfig::Oidc(oidc)) =
            document.identity.providers.get_mut(&provider)
        else {
            return Err(template_error("missing test identity provider"));
        };
        configure_login(oidc, subject)?;
        document.dashboard.login = Some(DashboardLogin {
            provider,
            client_id: DASHBOARD_CLIENT_ID.to_owned(),
            client_secret: ClientSecretSource::Inline(ClientSecret::new(DASHBOARD_CLIENT_SECRET)),
        });
    }
    if knobs.sessions == Sessions::Short {
        let ClientAuthenticationConfig::Token(token) = &mut document.authentication.clients else {
            return Err(template_error("missing [authentication.clients.token]"));
        };
        token.token_ttl = TokenTtl::try_new(SESSION_TOKEN_TTL).map_err(template_error)?;
        token.clock_skew = Duration::ZERO;
        document.dashboard.session_max_age =
            SessionMaxAge::try_new(SESSION_MAX_AGE).map_err(template_error)?;
        document.server.roles.push(RoleConfig {
            name: "viewer".parse().map_err(template_error)?,
            permissions: [Permission::StateRead].into_iter().collect(),
            principals: vec![RolePrincipal {
                provider: PROVIDER.parse().map_err(template_error)?,
                principal: knobs
                    .sign_in
                    .subject(knobs.sessions)
                    .ok_or_else(|| template_error("short sessions require interactive sign-in"))?
                    .parse()
                    .map_err(template_error)?,
            }],
        });
    }
    if knobs.standing_worker == StandingWorker::One {
        document
            .fleets
            .first_mut()
            .ok_or_else(|| template_error("missing the first [[fleets]] entry"))?
            .min_instances = 1;
    }
    Ok(())
}

/// Applies control-loop tuning to the configuration.
fn render_control(document: &mut Document<Plugins>, knobs: &ControlKnobs) -> Result<()> {
    if let Some(interval) = knobs.poll_interval {
        document.poll_interval = NonZeroDuration::try_new(interval).map_err(template_error)?;
    }
    if let Some(timeout) = knobs.client_action_timeout {
        document.client_action_timeout =
            NonZeroDuration::try_new(timeout).map_err(template_error)?;
    }
    if let Some(max) = knobs.max_concurrent_streams {
        document.server.http2.max_concurrent_streams = NonZeroU32::new(max)
            .ok_or_else(|| template_error("max concurrent streams must be nonzero"))?;
    }
    if let Some(percent) = knobs.payback_percent {
        document.scaling.payback_percent =
            gang_config::PaybackPercent::try_new(percent).map_err(template_error)?;
    }
    if let Some(half_life) = knobs.load_half_life {
        document.scaling.load_half_life =
            gang_config::ScalingInterval::try_new(half_life).map_err(template_error)?;
    }
    if let Some(idle) = knobs.source_idle {
        document.scaling.source_idle =
            gang_config::ScalingInterval::try_new(idle).map_err(template_error)?;
    }
    if let Some(duration) = knobs.unknown_action_duration {
        document.scaling.unknown_action_duration =
            NonZeroDuration::try_new(duration).map_err(template_error)?;
    }
    if let Some(prior) = knobs.readiness_prior {
        document.scaling.readiness_prior =
            NonZeroDuration::try_new(prior).map_err(template_error)?;
    }
    if let Some(dwell) = knobs.scale_in_dwell {
        document.scaling.scale_in_dwell =
            NonZeroDuration::try_new(dwell).map_err(template_error)?;
    }
    if let Some(max) = knobs.max_cordons {
        document.scaling.max_cordons = max;
    }
    if knobs.broken_launcher {
        let ProvisionerConfig::Local(local) = &mut document.provisioner else {
            return Err(template_error("missing [provisioner.local]"));
        };
        local.worker_launcher = vec!["gang-e2e-nonexistent-worker-launcher".to_owned()]
            .try_into()
            .map_err(template_error)?;
    }
    for fleet in &mut document.fleets {
        if let Some(grace) = knobs.boot_grace {
            fleet.boot_grace = grace;
        }
        if let Some(timeout) = knobs.idle_timeout {
            fleet.idle_timeout = timeout;
        }
        if let Some(wait) = knobs.max_drain_wait {
            fleet.max_drain_wait = wait;
        }
        if let Some(min) = knobs.min_instances {
            fleet.min_instances = min;
        }
        if let Some(max) = knobs.max_instances {
            fleet.max_instances = NonZeroU32::new(max)
                .ok_or_else(|| template_error("max instances must be nonzero"))?;
        }
        if let Some(target) = knobs.target_actions_per_instance {
            fleet.target_actions_per_instance = NonZeroU32::new(target)
                .ok_or_else(|| template_error("target actions per instance must be nonzero"))?;
        }
        if let Some(vcpus) = knobs.vcpus_per_instance {
            fleet.vcpus_per_instance = NonZeroU32::new(vcpus)
                .ok_or_else(|| template_error("vcpus per instance must be nonzero"))?;
        }
        if let Some(instances) = knobs.scheduled_minimum {
            let instances = NonZeroU32::new(instances)
                .ok_or_else(|| template_error("scheduled minimum instances must be nonzero"))?;
            let days = gang_config::Weekdays::try_from(gang_config::Weekday::ALL.to_vec())
                .map_err(template_error)?;
            fleet.scheduled_minimum = vec![
                gang_config::ScheduledMinimum::try_new(
                    instances,
                    days,
                    "00:00".parse().map_err(template_error)?,
                    "24:00".parse().map_err(template_error)?,
                    "Asia/Kathmandu".parse().map_err(template_error)?,
                )
                .map_err(template_error)?,
            ];
        }
    }
    if let Some(grace) = knobs.terminate_grace {
        let ProvisionerConfig::Local(local) = &mut document.provisioner else {
            return Err(template_error("missing [provisioner.local]"));
        };
        local.terminate_grace = NonZeroDuration::try_new(grace).map_err(template_error)?;
    }
    if let Some(interval) = knobs.detail_interval {
        document.dashboard.detail_interval =
            NonZeroDuration::try_new(interval).map_err(template_error)?;
    }
    Ok(())
}

/// Applies request, viewer and certificate tuning.
fn render_server(document: &mut Document<Plugins>, knobs: ServerKnobs) -> Result<()> {
    if knobs.request_limits == RequestLimits::Tight {
        document.server.max_request_size =
            Bytes::new(u64::try_from(MAX_REQUEST_SIZE).map_err(template_error)?);
        document.server.rate_limit.trusted_proxies = TRUSTED_PROXIES
            .iter()
            .map(|proxy| proxy.parse().map_err(template_error))
            .collect::<Result<_>>()?;
        document.server.rate_limit.client.burst = NonZeroU32::new(RATE_LIMIT_BURST)
            .ok_or_else(|| template_error("rate limit burst must be nonzero"))?;
        document.server.rate_limit.client.refill_interval =
            NonZeroDuration::try_new(RATE_LIMIT_REFILL).map_err(template_error)?;
    }
    if knobs.viewers == ViewerLimit::One {
        document.dashboard.max_viewers = NonZeroUsize::MIN;
        document.dashboard.retry_after = VIEWER_RETRY_AFTER;
    }
    if knobs.certificate == CertificateLifetime::Short {
        document.server.certificate_ttl =
            NonZeroDuration::try_new(CERTIFICATE_TTL).map_err(template_error)?;
        let percent = u8::try_from(REISSUE_AT_PERCENT).map_err(template_error)?;
        document.server.reissue_at_percent = Percent::new(percent)
            .ok_or_else(|| template_error("reissue percentage must be between 1 and 100"))?;
        document.server.certificate_backdate = CERTIFICATE_BACKDATE;
    }
    Ok(())
}
