//! The error returned while preparing or running the server.

use std::fmt;

use gang_mtls::MtlsError;
use gang_spi::PortError;
use gang_tls::TransportError;
use gang_types::Url;

use crate::endpoint::EndpointRole;

/// Formats the schemes accepted by an endpoint without allocating.
struct ExpectedSchemes<'a>(&'a [&'static str]);

impl fmt::Display for ExpectedSchemes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let [scheme] = self.0 {
            return write!(f, "the {scheme} scheme");
        }
        f.write_str("one of the schemes ")?;
        for (index, scheme) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            f.write_str(scheme)?;
        }
        Ok(())
    }
}

/// Why the server could not be prepared or stopped while running.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The certificate authority could not be established from the signer.
    #[error("cannot establish the certificate authority")]
    Authority(#[source] MtlsError),

    /// The listener certificate could not be issued by the certificate authority.
    #[error("cannot issue a server certificate")]
    ServerCertificate(#[source] MtlsError),

    /// A TLS listener could not be built or failed while running.
    #[error("the tls transport failed")]
    Transport(#[from] TransportError),

    /// The revoked principals or serials could not be loaded at startup.
    #[error("cannot load the revoked principals or serials")]
    Revocations(#[source] PortError),

    /// The scaling events that seed the dashboard could not be read at startup.
    #[error("cannot read the scaling events of the dashboard")]
    DashboardEvents(#[source] PortError),

    /// The cache stores could not be described to the scheduler.
    #[error("cannot describe the cache stores")]
    Cache(#[source] PortError),

    /// An endpoint does not use an accepted scheme.
    #[error("{role} endpoint `{url}` must use {schemes}", schemes = ExpectedSchemes(.expected))]
    EndpointScheme {
        /// The endpoint's role.
        role: EndpointRole,
        /// The offending URL.
        url: Url,
        /// The schemes the endpoint accepts.
        expected: &'static [&'static str],
    },

    /// A listener endpoint URL has no host name that a certificate can name.
    #[error("{role} endpoint `{url}` has no usable host name")]
    EndpointHost {
        /// The endpoint's role.
        role: EndpointRole,
        /// The offending URL.
        url: Url,
    },

    /// The clients authentication plugin presents itself as mTLS, which only workers use.
    #[error("the clients authentication plugin must not present itself as mtls")]
    ClientScheme,

    /// The scheduler's controller failed.
    #[error("the controller stopped")]
    Controller(#[source] PortError),

    /// The scheduler's controller returned without being asked to stop.
    #[error("the controller stopped without being asked to")]
    ControllerEnded,

    /// A listener stopped accepting connections while the server was running, so its endpoint
    /// can no longer receive clients.
    #[error("the {endpoint} listener stopped accepting connections")]
    ListenerEnded {
        /// The endpoint's role, `public` or `worker`.
        endpoint: &'static str,
    },
}
