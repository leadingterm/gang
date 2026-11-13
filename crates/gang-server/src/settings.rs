//! The settings the server is composed from, built by the binary field by field.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_config::{DashboardConfig, EndpointTrust, RoleConfig};
use gang_spi::LoginClient;
use gang_types::{Isa, NonZeroDuration, Percent, ProviderName, TrustDomain};

use crate::endpoint::PublicEndpoint;

/// How accepted connections are handled before they reach the application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportSettings {
    /// Connections are served as received, without a TLS handshake.
    Plaintext,
    /// Connections complete a TLS handshake under the server's certificate authority.
    Tls(TlsSettings),
}

impl TransportSettings {
    /// Returns what clients trust at the endpoint. A plaintext server sits behind a terminator
    /// that clients verify with their system roots.
    #[must_use]
    pub(crate) fn endpoint_trust(&self) -> EndpointTrust {
        match self {
            Self::Tls(tls) => tls.endpoint_trust,
            Self::Plaintext => EndpointTrust::System,
        }
    }
}

/// How TLS connections are issued certificates and handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSettings {
    /// The trust domain that certificate subject names are issued under.
    pub trust_domain: TrustDomain,
    /// How far before issuance a certificate's validity starts, to absorb clock skew.
    pub certificate_backdate: Duration,
    /// Lifetime of each certificate the server issues: its listener certificates and the client
    /// certificates it issues to workers and clients.
    pub certificate_ttl: NonZeroDuration,
    /// How far through its lifetime a server certificate is reissued. The default is 50:
    /// reissued at half its lifetime.
    pub reissue_at_percent: Percent,
    /// How TLS handshakes are bounded.
    pub handshake: HandshakeSettings,
    /// What clients see at the endpoint: under `System` the server information advertises no CA.
    pub endpoint_trust: EndpointTrust,
}

/// Limits on the TLS handshakes of one listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandshakeSettings {
    /// How long a TLS handshake may take before the connection is dropped.
    pub handshake_timeout: NonZeroDuration,
    /// How many TLS handshakes may be in progress at once on one listener.
    pub max_pending_handshakes: NonZeroUsize,
    /// How long a refused TLS connection stays open for its client to read the alert.
    pub refusal_linger: NonZeroDuration,
}

/// Limits on each Gang API request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLimits {
    /// The largest request body the API reads, in bytes.
    pub max_request_size: usize,
    /// How long the API may take to answer one request.
    pub request_timeout: NonZeroDuration,
}

/// Settings of the interactive login the browser sign-in uses.
#[derive(Debug, Clone)]
pub struct LoginSettings {
    /// The identity provider the browser signs in with.
    pub provider: ProviderName,
    /// The client the provider authorizes the sign-in for.
    pub client: LoginClient,
    /// How long a sign-in stays open between starting it and completing it.
    pub ttl: NonZeroDuration,
    /// How many sign-ins may be open at once.
    pub max_pending: NonZeroUsize,
}

/// Everything the server needs to run, apart from the ports it is given.
#[derive(Debug, Clone)]
pub struct ServerSettings {
    /// How often the control loop runs a reconcile pass.
    pub poll_interval: NonZeroDuration,
    /// How long a client action may take before the scheduler gives up on it.
    pub client_action_timeout: NonZeroDuration,
    /// The instruction set clients use for actions that name none; the server sends it in
    /// `Server`.
    pub default_isa: Isa,
    /// The public endpoint the server is reached at; its host is named in the server
    /// certificates.
    pub endpoint: PublicEndpoint,
    /// How accepted connections are handled.
    pub transport: TransportSettings,
    /// The roles the server grants permissions through; each binds identities to permissions.
    pub roles: Vec<RoleConfig>,
    /// Limits on each Gang API request.
    pub limits: RequestLimits,
    /// Limits on the routes that are open to anyone.
    pub rate_limit: gang_config::RateLimitConfig,
    /// The shortest interval between two writes of one principal's last-seen time.
    pub principal_seen_interval: NonZeroDuration,
    /// Maximum principal sightings tracked before the oldest can be written early.
    pub max_tracked_principals: NonZeroUsize,
    /// How long a listener waits after a failed accept before accepting again.
    pub accept_error_backoff: NonZeroDuration,
    /// The operator dashboard: viewer limits, refresh intervals and the actions cache.
    pub dashboard: DashboardConfig,
    /// The browser sign-in the dashboard offers; `None` when it offers none.
    pub login: Option<LoginSettings>,
    /// The key of the configured provisioner plugin, such as `aws` or `local`, that the state
    /// reports.
    pub provisioner: String,
}
