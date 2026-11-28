//! The configuration shared by every plugin set.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::str::FromStr;

use gang_types::{
    AuthenticationSettings, CaPin, Isa, NonZeroDuration, ProviderName, Url, property,
};
use serde::{Deserialize, Serialize};

use crate::actions::ActionsConfig;
use crate::authentication_config::AuthenticationConfig;
use crate::builds_config::BuildsConfig;
use crate::ca_pin_setting::CaPinSetting;
use crate::dashboard_config::DashboardConfig;
use crate::errors::ConfigError;
use crate::execution_config::ExecutionConfig;
use crate::fleets::{Fleets, RawFleet};
use crate::identity_config::IdentityConfig;
use crate::plugin_set::PluginSet;
use crate::role_config::RoleConfig;
use crate::scaling::ScalingConfig;
use crate::server_config::{EndpointTrust, ServerConfig, Transport};
use crate::worker_config::WorkerConfig;

/// The most samples retained in the dashboard history window.
const MAX_HISTORY_STEPS: u128 = 1440;

/// `gang.toml`. The plugin sections are typed by the composition's [`PluginSet`]; `F` is the
/// fleet column: the typed `[[fleets]]` entries until parsing resolves them into [`Fleets`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(serialize = "F: Serialize", deserialize = "F: Deserialize<'de>")
)]
pub struct GangConfig<P: PluginSet, F = Fleets<P>> {
    /// How often the autoscaler reconciles.
    pub poll_interval: NonZeroDuration,
    /// How long a client action may wait before it is abandoned.
    pub client_action_timeout: NonZeroDuration,
    /// The instruction set `gang bazelrc --server` writes into the static Bazel flags as the
    /// default exec property `ISA=<isa>`; some fleet must have it.
    pub default_isa: Isa,
    /// Where `gang server` accepts its public connections: the Gang API (`/v1/*`) and the REAPI
    /// services Bazel and `gang login` use, served on one HTTP/2 listener.
    pub listen: SocketAddr,
    /// The URL clients and workers dial to reach `listen`. Under `transport = "tls"` it is an
    /// https URL; under `transport = "plaintext"` it is an http or https URL. Omitted, it is
    /// derived from the bound address and only allowed when `listen` is loopback. `gang-mtls`
    /// takes the server certificate's host name from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<Url>,
    /// Where `gang server` accepts worker connections. A non-loopback listener requires
    /// `worker_endpoint`.
    pub worker_listen: SocketAddr,
    /// URL workers dial to reach `worker_listen`. Omitted only for a loopback listener.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_endpoint: Option<Url>,
    /// Plaintext HTTP health listener for load balancers. `GET /ready` answers 200 only while
    /// this controller is serving, holds the lease, and has public and worker listeners ready.
    /// Unset: no health listener.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_listen: Option<SocketAddr>,
    /// The worker fleets in preference order.
    pub fleets: F,
    /// Scheduling and enforcement policy for actions.
    #[serde(default)]
    pub actions: ActionsConfig,
    /// Rate-based scaling policy. The section may be omitted.
    #[serde(default)]
    pub scaling: ScalingConfig,
    /// The identity providers that prove identity before a certificate or token is issued. The
    /// section may be omitted, which means no providers: the `none` plugins need none, and a
    /// plugin that names a provider is rejected by validation.
    #[serde(default)]
    pub identity: IdentityConfig<P::IdentityProvider>,
    /// The authentication plugins: one for the clients surface and one for the workers surface.
    pub authentication: AuthenticationConfig<P::ClientAuthentication, P::WorkerAuthentication>,
    /// How `gang exec` prepares an action's inputs.
    #[serde(default)]
    pub execution: ExecutionConfig,
    /// Settings of `gang server`: transport, Gang's CA, listener tunables, roles and request
    /// limits. The section is required.
    pub server: ServerConfig,
    /// Settings of `gang worker`: CAS budget, certificate refresh and preemption stop. The
    /// section may be omitted.
    #[serde(default)]
    pub worker: WorkerConfig,
    /// Settings of the operator dashboard that `gang server` serves: viewer limits, refresh
    /// intervals and the optional login. The section may be omitted.
    #[serde(default)]
    pub dashboard: DashboardConfig,
    /// Limits for in-memory build insight. The section may be omitted.
    #[serde(default)]
    pub builds: BuildsConfig,

    /// The provisioner plugin: the worker fleets' instances.
    pub provisioner: P::Provisioner,
    /// The cache plugin: the cache store.
    pub cache: P::Cache,
    /// The signer plugin: the certificate signing key.
    pub signer: P::Signer,
    /// The database plugin.
    pub database: P::Database,
    /// The sandbox plugins.
    pub sandbox: P::Sandbox,
    /// The limits plugin, when one is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<P::Limits>,
}

/// A `gang.toml` as written: the fleets are the raw `[[fleets]]` entries and the references between
/// sections are not yet checked. The form to edit and write back with [`GangConfig::to_toml`];
/// parsing through [`FromStr`] validates and resolves the fleets.
pub type Document<P> = GangConfig<P, Vec<RawFleet<<P as PluginSet>::SandboxKey>>>;

impl<P: PluginSet> GangConfig<P, Vec<RawFleet<P::SandboxKey>>> {
    /// Parses the text of a `gang.toml` without validating it.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Gang`] for malformed TOML, unknown or missing fields and unknown, missing
    /// or repeated plugin sections.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        toml::from_str(text).map_err(ConfigError::Gang)
    }

    /// Resolves the fleets and checks the references between sections that serde cannot
    /// express.
    ///
    /// # Errors
    ///
    /// Returns the fleet and cross-section errors of [`ConfigError`].
    pub fn resolve(self) -> Result<GangConfig<P>, ConfigError> {
        let GangConfig {
            poll_interval,
            client_action_timeout,
            default_isa,
            listen,
            endpoint,
            worker_listen,
            worker_endpoint,
            health_listen,
            fleets,
            actions,
            scaling,
            identity,
            authentication,
            execution,
            server,
            worker,
            dashboard,
            builds,
            provisioner,
            cache,
            signer,
            database,
            sandbox,
            limits,
        } = self;
        Self::check_listener_collisions(listen, worker_listen, health_listen)?;
        Self::check_leadership(&server)?;
        Self::check_cpu(&actions.cpu)?;
        let fleets = Fleets::resolve(fleets, &provisioner, &sandbox, actions.memory.reserve.get())?;
        Self::check_fleet_settings(&fleets, default_isa)?;
        if let Some(policy) = &execution.egress {
            policy.validate().map_err(ConfigError::InvalidEgress)?;
        }
        if execution.egress.is_none()
            && fleets.policies().iter().any(|fleet| {
                fleet
                    .actions
                    .grants
                    .contains(&gang_types::Capability::Network)
            })
        {
            return Err(ConfigError::MissingEgress);
        }
        Self::check_actions(&fleets, &actions)?;
        if scaling.scale_in_dwell.get() < scaling.load_half_life.get() {
            return Err(ConfigError::ScaleInDwellTooShort {
                scale_in_dwell: scaling.scale_in_dwell.get(),
                load_half_life: scaling.load_half_life.get(),
            });
        }
        if worker.connect_timeout.get() / 2 >= server.worker_timeout.get() {
            return Err(ConfigError::WorkerConnectTimeoutTooLong {
                connect_timeout: worker.connect_timeout.get(),
                worker_timeout: server.worker_timeout.get(),
            });
        }
        if let Some(name) = identity.providers.keys().find(|name| name.is_anonymous()) {
            return Err(ConfigError::ReservedProviderName(name.clone()));
        }
        if endpoint.is_none() && !listen.ip().is_loopback() {
            return Err(ConfigError::EndpointRequired { listen });
        }
        Self::check_transport(&server, endpoint.as_ref(), &authentication)?;
        Self::check_worker_endpoint(&server, worker_listen, worker_endpoint.as_ref())?;
        Self::check_authentication_providers(&identity.providers, &authentication.clients)?;
        Self::check_authentication_providers(&identity.providers, &authentication.workers)?;
        Self::check_roles(&server.roles, &identity.providers)?;
        Self::check_dashboard(&dashboard, &identity.providers)?;
        Ok(GangConfig {
            poll_interval,
            client_action_timeout,
            default_isa,
            listen,
            endpoint,
            worker_listen,
            worker_endpoint,
            health_listen,
            fleets,
            actions,
            scaling,
            identity,
            authentication,
            execution,
            server,
            worker,
            dashboard,
            builds,
            provisioner,
            cache,
            signer,
            database,
            sandbox,
            limits,
        })
    }

    fn check_cpu(cpu: &crate::CpuTuning) -> Result<(), ConfigError> {
        if cpu.headroom_percent >= 100 {
            return Err(ConfigError::InvalidCpuWatermarks(
                "headroom must be below 100",
            ));
        }
        if cpu.clear_utilization_percent > cpu.saturate_utilization_percent {
            return Err(ConfigError::InvalidCpuWatermarks(
                "clear utilization exceeds saturation",
            ));
        }
        if cpu.clear_psi_percent > cpu.saturate_psi_percent {
            return Err(ConfigError::InvalidCpuWatermarks(
                "clear PSI exceeds saturation",
            ));
        }
        Ok(())
    }

    fn check_actions(fleets: &Fleets<P>, actions: &ActionsConfig) -> Result<(), ConfigError> {
        for fleet in fleets.policies() {
            let effective = fleet.actions.resources.effective(fleet.instance());
            for (key, value, capacity) in [
                (
                    "actions.memory.min_reservation",
                    actions.memory.min_reservation.as_units() * 1024,
                    effective.memory.get(),
                ),
                (
                    "actions.disk.min_reservation",
                    actions.disk.min_reservation.as_units() * 1024,
                    effective.disk.get(),
                ),
            ] {
                if value > capacity {
                    return Err(ConfigError::ActionResourceDoesNotFit {
                        key,
                        value,
                        capacity,
                        fleet: fleet.name.clone(),
                        relation: "at most",
                    });
                }
            }
        }
        let cold = &actions.cold_start;
        let fits = fleets.policies().iter().any(|fleet| {
            let effective = fleet.actions.resources.effective(fleet.instance());
            u64::from(effective.vcpus) * property::MILLICORES_PER_CORE
                >= u64::from(cold.cpu_millicores.get())
                && effective.memory.get() >= cold.memory.as_units() * 1024
                && effective.disk.get() >= cold.disk.as_units() * 1024
        });
        if cold.enabled && !fits {
            return Err(ConfigError::ColdStartFitsNoFleet {
                cpu_millicores: cold.cpu_millicores.get(),
                memory_kb: cold.memory.as_units(),
                disk_kb: cold.disk.as_units(),
            });
        }
        Ok(())
    }

    /// Checks the default ISA against resolved fleets.
    fn check_fleet_settings(fleets: &Fleets<P>, default_isa: Isa) -> Result<(), ConfigError> {
        if !fleets
            .policies()
            .iter()
            .any(|policy| policy.isa == default_isa)
        {
            return Err(ConfigError::DefaultIsaWithoutFleet(default_isa));
        }
        Ok(())
    }

    /// Rejects non-ephemeral listeners whose bind addresses overlap.
    fn check_listener_collisions(
        listen: SocketAddr,
        worker_listen: SocketAddr,
        health_listen: Option<SocketAddr>,
    ) -> Result<(), ConfigError> {
        let listeners = [
            ("listen", Some(listen)),
            ("worker_listen", Some(worker_listen)),
            ("health_listen", health_listen),
        ];
        for (index, (first, address)) in listeners.iter().enumerate() {
            if let Some(address) = address
                && address.port() != 0
            {
                for (second, other) in &listeners[index + 1..] {
                    if let Some(other) = other
                        && address.port() == other.port()
                        && (address.ip() == other.ip()
                            || ((address.ip().is_unspecified() || other.ip().is_unspecified())
                                && address.is_ipv4() == other.is_ipv4()))
                    {
                        return Err(ConfigError::ListenerCollision {
                            first,
                            second,
                            listen: *address,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Checks that renewal, mutation margin and clock skew fit strictly within the lease, and
    /// that a takeover completes before clients drop a frozen leader.
    fn check_leadership(server: &ServerConfig) -> Result<(), ConfigError> {
        let leadership = &server.leadership;
        if leadership
            .ttl
            .get()
            .checked_sub(leadership.clock_skew)
            .and_then(|remaining| remaining.checked_sub(leadership.call_timeout.get()))
            .is_none_or(|remaining| leadership.renew_interval.get() >= remaining)
        {
            return Err(ConfigError::LeadershipRenewalTooLate {
                ttl: leadership.ttl.get(),
                renew_interval: leadership.renew_interval.get(),
                call_timeout: leadership.call_timeout.get(),
                clock_skew: leadership.clock_skew,
            });
        }
        let window = gang_types::handover::handover_window(
            leadership.renew_interval.get(),
            leadership.call_timeout.get(),
        );
        let call_timeout = leadership.call_timeout.get();
        let last_attempt = gang_types::handover::minimum_cli_last_attempt();
        if window.saturating_add(call_timeout) >= last_attempt {
            return Err(ConfigError::HandoverOutlastsCli {
                window,
                call_timeout,
                last_attempt,
            });
        }
        Self::check_leadership_keepalive(server)
    }

    /// Checks that a replacement leader becomes healthy before clients reconnect.
    fn check_leadership_keepalive(server: &ServerConfig) -> Result<(), ConfigError> {
        let leadership = &server.leadership;
        let takeover = leadership
            .ttl
            .get()
            .saturating_add(leadership.clock_skew)
            .saturating_add(leadership.renew_interval.get());
        let healthy_window = gang_types::health::health_check().healthy_window();
        let keepalive_timeout = gang_types::health::CLIENT_KEEPALIVE_TIMEOUT;
        if takeover.saturating_add(healthy_window) >= keepalive_timeout {
            return Err(ConfigError::LeadershipOutlastsKeepalive {
                takeover,
                healthy_window,
                keepalive_timeout,
            });
        }
        Ok(())
    }

    /// Checks the advertised worker endpoint against its listener and transport.
    fn check_worker_endpoint(
        server: &ServerConfig,
        listen: SocketAddr,
        endpoint: Option<&Url>,
    ) -> Result<(), ConfigError> {
        let Some(endpoint) = endpoint else {
            if !listen.ip().is_loopback() {
                return Err(ConfigError::WorkerEndpointRequired {
                    key: "worker_endpoint",
                    listen,
                });
            }
            return Ok(());
        };
        if endpoint.host_str().is_none() {
            return Err(ConfigError::WorkerEndpointWithoutHost(endpoint.clone()));
        }
        let scheme = endpoint.scheme();
        match &server.transport {
            Transport::Tls { .. } if scheme != server.transport.kind().worker_scheme() => {
                Err(ConfigError::WorkerEndpointNotGrpcs(endpoint.clone()))
            }
            Transport::Plaintext if scheme != "grpc" && scheme != "grpcs" => {
                Err(ConfigError::WorkerEndpointNotGrpc(endpoint.clone()))
            }
            _ => Ok(()),
        }
    }

    /// Checks that every identity provider a surface's plugin names is configured.
    fn check_authentication_providers(
        providers: &BTreeMap<ProviderName, P::IdentityProvider>,
        selection: &impl AuthenticationSettings,
    ) -> Result<(), ConfigError> {
        match selection
            .providers()
            .iter()
            .find(|name| !providers.contains_key(*name))
        {
            Some(name) => Err(ConfigError::AuthenticationProviderNotConfigured(
                name.clone(),
            )),
            None => Ok(()),
        }
    }

    /// Checks each role: names are unique, it grants a permission and lists a principal, and
    /// each principal's provider is configured or the reserved `none`.
    fn check_roles(
        roles: &[RoleConfig],
        providers: &BTreeMap<ProviderName, P::IdentityProvider>,
    ) -> Result<(), ConfigError> {
        let mut names = BTreeSet::new();
        for role in roles {
            if !names.insert(role.name.as_str()) {
                return Err(ConfigError::DuplicateRole(role.name.clone()));
            }
            if role.permissions.is_empty() {
                return Err(ConfigError::RoleWithoutPermissions(role.name.clone()));
            }
            if role.principals.is_empty() {
                return Err(ConfigError::RoleWithoutPrincipals(role.name.clone()));
            }
            for principal in &role.principals {
                if !principal.provider.is_anonymous()
                    && !providers.contains_key(&principal.provider)
                {
                    return Err(ConfigError::RoleProviderNotConfigured {
                        role: role.name.clone(),
                        provider: principal.provider.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Checks dashboard history, stale threshold, and login provider settings.
    fn check_dashboard(
        dashboard: &DashboardConfig,
        providers: &BTreeMap<ProviderName, P::IdentityProvider>,
    ) -> Result<(), ConfigError> {
        let step = dashboard.history_step.get();
        let window = dashboard.history_window.get();
        let step_nanos = step.as_nanos();
        let window_nanos = window.as_nanos();
        if step.subsec_nanos() != 0
            || window_nanos < step_nanos
            || !window_nanos.is_multiple_of(step_nanos)
            || window_nanos / step_nanos > MAX_HISTORY_STEPS
        {
            return Err(ConfigError::HistoryWindowNotWholeSteps);
        }
        if dashboard
            .actions_stale_after
            .is_some_and(|stale_after| stale_after <= dashboard.detail_interval)
        {
            return Err(ConfigError::StaleThresholdNotAboveRefresh);
        }
        let Some(login) = &dashboard.login else {
            return Ok(());
        };
        if login.provider.is_anonymous() || !providers.contains_key(&login.provider) {
            return Err(ConfigError::DashboardLoginProviderNotConfigured(
                login.provider.clone(),
            ));
        }
        Ok(())
    }
}

impl<P: PluginSet, F> GangConfig<P, F> {
    /// This configuration with the server's CA pinned to `pin`.
    #[must_use]
    pub fn pinned(mut self, pin: CaPin) -> Self {
        self.server.ca_pin = Some(CaPinSetting::Pinned(pin));
        self
    }

    /// This configuration for an unauthenticated plaintext server: the transport is plaintext
    /// without a CA pin, one administrator role goes to the anonymous caller, and the endpoint
    /// and authentication are the given ones.
    #[must_use]
    pub fn insecure(
        mut self,
        endpoint: Option<Url>,
        authentication: AuthenticationConfig<P::ClientAuthentication, P::WorkerAuthentication>,
    ) -> Self {
        self.server.transport = Transport::Plaintext;
        self.server.ca_pin = None;
        self.server.roles = vec![RoleConfig::anonymous_administrator()];
        self.authentication = authentication;
        self.endpoint = endpoint;
        self
    }

    /// Checks transport and endpoint schemes, and rejects pinned non-enrolling workers when the
    /// system-trusted endpoint advertises no CA. An omitted endpoint is derived from the bound
    /// address by the server and needs no scheme check here.
    fn check_transport(
        server: &ServerConfig,
        endpoint: Option<&Url>,
        authentication: &AuthenticationConfig<P::ClientAuthentication, P::WorkerAuthentication>,
    ) -> Result<(), ConfigError> {
        match &server.transport {
            Transport::Tls { .. } => {
                if let Some(endpoint) = endpoint
                    && endpoint.scheme() != server.transport.kind().public_scheme()
                {
                    return Err(ConfigError::EndpointNotHttps(endpoint.clone()));
                }
                if server.endpoint_trust == EndpointTrust::System
                    && server.ca_pin.is_some()
                    && authentication.workers.enrollment_provider().is_none()
                {
                    return Err(ConfigError::PinnedWorkersNeedAdvertisedCa);
                }
            }
            Transport::Plaintext => {
                if authentication.clients.requires_tls() || authentication.workers.requires_tls() {
                    return Err(ConfigError::AuthenticationRequiresTls);
                }
                if let Some(endpoint) = endpoint {
                    let scheme = endpoint.scheme();
                    // A plaintext server sits behind a proxy that may serve https.
                    if scheme != "https" && scheme != server.transport.kind().public_scheme() {
                        return Err(ConfigError::EndpointNotHttp(endpoint.clone()));
                    }
                }
            }
        }
        Ok(())
    }
}

impl<P: PluginSet> FromStr for GangConfig<P> {
    type Err = ConfigError;

    /// Parses and validates the text of a `gang.toml`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Gang`] for malformed TOML, unknown or missing fields (a `[[fleets]]` entry
    /// included) and unknown, missing or repeated plugin sections; the message lists the
    /// accepted keys.
    /// Then the fleet and cross-section errors of [`ConfigError`].
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Document::<P>::parse(text)?.resolve()
    }
}

impl<P: PluginSet, F: Serialize> GangConfig<P, F> {
    /// Renders the configuration as TOML.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Render`] when a section cannot be serialized as TOML.
    pub fn to_toml(&self) -> Result<String, ConfigError> {
        Ok(toml::to_string(self)?)
    }
}
