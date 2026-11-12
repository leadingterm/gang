//! The configuration shared by every plugin set.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::str::FromStr;

use gang_types::{
    AuthenticationSettings, CaPin, Isa, NonZeroDuration, ProviderName, SchedulerSettings, Url,
};
use serde::{Deserialize, Serialize};

use crate::authentication_config::AuthenticationConfig;
use crate::ca_pin_setting::CaPinSetting;
use crate::dashboard_config::DashboardConfig;
use crate::errors::ConfigError;
use crate::execution_config::ExecutionConfig;
use crate::fleets::{Fleets, RawFleet};
use crate::identity_config::IdentityConfig;
use crate::plugin_set::PluginSet;
use crate::role_config::RoleConfig;
use crate::server_config::{ServerConfig, Transport};
use crate::worker_config::WorkerConfig;

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
    /// Where `gang server` accepts worker connections. A non-loopback listener requires a
    /// configured scheduler worker endpoint when the scheduler supports one.
    pub worker_listen: SocketAddr,
    /// The worker fleets in preference order.
    pub fleets: F,
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

    /// The provisioner plugin: the worker fleets' instances.
    pub provisioner: P::Provisioner,
    /// The cache plugin: the cache store.
    pub cache: P::Cache,
    /// The signer plugin: the certificate signing key.
    pub signer: P::Signer,
    /// The scheduler plugin.
    pub scheduler: P::Scheduler,
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
    /// [`ConfigError::Gang`] for malformed TOML, unknown or missing fields and unknown, missing or repeated plugin sections.
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
            fleets,
            identity,
            authentication,
            execution,
            server,
            worker,
            dashboard,
            provisioner,
            cache,
            signer,
            scheduler,
            database,
            sandbox,
            limits,
        } = self;
        let fleets = Fleets::resolve(fleets, &provisioner, &sandbox)?;
        if !fleets
            .policies()
            .iter()
            .any(|policy| policy.isa == default_isa)
        {
            return Err(ConfigError::DefaultIsaWithoutFleet(default_isa));
        }
        if let Some(name) = identity.providers.keys().find(|name| name.is_anonymous()) {
            return Err(ConfigError::ReservedProviderName(name.clone()));
        }
        if endpoint.is_none() && !listen.ip().is_loopback() {
            return Err(ConfigError::EndpointRequired { listen });
        }
        Self::check_transport(&server, endpoint.as_ref(), &authentication)?;
        Self::check_worker_endpoint(&server, worker_listen, &scheduler)?;
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
            fleets,
            identity,
            authentication,
            execution,
            server,
            worker,
            dashboard,
            provisioner,
            cache,
            signer,
            scheduler,
            database,
            sandbox,
            limits,
        })
    }

    /// Checks the scheduler's advertised worker endpoint against its listener and transport.
    fn check_worker_endpoint(
        server: &ServerConfig,
        listen: SocketAddr,
        scheduler: &P::Scheduler,
    ) -> Result<(), ConfigError> {
        let Some(endpoint) = scheduler.worker_endpoint() else {
            if let Some(key) = scheduler.worker_endpoint_key()
                && !listen.ip().is_loopback()
            {
                return Err(ConfigError::WorkerEndpointRequired { key, listen });
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

    /// Checks the `[dashboard]` section: an explicit stale threshold must exceed the refresh
    /// interval, and a login's provider must be configured.
    fn check_dashboard(
        dashboard: &DashboardConfig,
        providers: &BTreeMap<ProviderName, P::IdentityProvider>,
    ) -> Result<(), ConfigError> {
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

    /// Checks the transport: `mtls` needs TLS, a TLS server needs an https endpoint, and a
    /// plaintext server needs an http or https one. An omitted endpoint
    /// is derived from the bound address by the server and needs no check here.
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
    /// accepted keys. Then the fleet and cross-section errors of [`ConfigError`].
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
