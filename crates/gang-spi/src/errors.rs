//! The error every port and plugin build returns.

use gang_types::{CertSerial, Identity, InstanceId, ProviderName, WorkerId};

/// A boxed cause from a backend.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Why a port call or a plugin build failed. The variants tell the caller what to do:
/// retry on the next pass, reject the client, or stop.
#[derive(Debug, thiserror::Error)]
pub enum PortError {
    /// The plugin's configuration cannot be used.
    #[error("invalid plugin configuration: {0}")]
    Config(#[source] BoxedError),
    /// The named thing does not exist in the backend.
    #[error("{0} not found")]
    NotFound(Subject),
    /// The backend refused the request: a proof failed verification or a policy denied it.
    #[error("rejected: {reason}")]
    Rejected {
        /// Why the request was refused.
        reason: Rejection,
        /// The backend's explanation, logged by the server and never sent to a client.
        #[source]
        source: Option<BoxedError>,
    },
    /// The plugin does not provide the operation.
    #[error("{operation} is not supported by this plugin")]
    Unsupported {
        /// The operation that was requested.
        operation: &'static str,
    },
    /// The backend could not be reached or is overloaded; trying again later may succeed.
    #[error("backend unavailable: {0}")]
    Unavailable(#[source] BoxedError),
    /// The backend failed in a way that trying again will not fix.
    #[error("backend failure: {0}")]
    Backend(#[source] BoxedError),
}

impl PortError {
    /// A refusal with the backend's explanation.
    #[must_use]
    pub fn rejected(reason: Rejection, cause: impl Into<BoxedError>) -> Self {
        Self::Rejected {
            reason,
            source: Some(cause.into()),
        }
    }

    /// A refusal without an underlying explanation.
    #[must_use]
    pub fn refused(reason: Rejection) -> Self {
        Self::Rejected {
            reason,
            source: None,
        }
    }

    /// A transient backend failure.
    #[must_use]
    pub fn unavailable(cause: impl Into<BoxedError>) -> Self {
        Self::Unavailable(cause.into())
    }

    /// A configuration the plugin cannot use.
    #[must_use]
    pub fn config(cause: impl Into<BoxedError>) -> Self {
        Self::Config(cause.into())
    }

    /// A permanent backend failure.
    #[must_use]
    pub fn backend(cause: impl Into<BoxedError>) -> Self {
        Self::Backend(cause.into())
    }
}

/// What a [`PortError::NotFound`] refers to.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Subject {
    /// A provisioner instance.
    #[error("instance {0}")]
    Instance(InstanceId),
    /// A worker known to the scheduler.
    #[error("worker {0}")]
    Worker(WorkerId),
    /// A certificate Gang issued.
    #[error("certificate {0}")]
    Certificate(CertSerial),
    /// An identity provider.
    #[error("identity provider {0}")]
    Provider(ProviderName),
    /// A principal the server has seen or revoked.
    #[error("principal {0}")]
    Principal(Identity),
}

/// Why a backend refused a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    /// The proof presented did not verify.
    #[error("the proof failed verification")]
    InvalidProof,
    /// The proof verified, but the identity provider's policy does not admit the caller.
    #[error("the proof does not satisfy the provider's policy")]
    PolicyDenied,
    /// The proof verified, but the server has revoked its principal.
    #[error("the principal is revoked")]
    PrincipalRevoked,
    /// The request is malformed and cannot be served.
    #[error("the request is malformed")]
    MalformedRequest,
}

/// A plugin failed to build from its configuration. The cause is the [`PortError`] the plugin
/// reported.
#[derive(Debug, thiserror::Error)]
#[error("cannot build the {domain} plugin")]
pub struct BuildError {
    /// The key of the plugin domain whose plugin failed to build, such as `cache`.
    pub domain: &'static str,
    /// Why the plugin could not be built.
    #[source]
    pub source: PortError,
}

impl BuildError {
    /// A build failure for the plugin in `domain`, caused by `source`.
    #[must_use]
    pub const fn new(domain: &'static str, source: PortError) -> Self {
        Self { domain, source }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gang_types::error_chain;

    use super::{BuildError, PortError};

    #[test]
    fn build_error_display_names_the_domain() {
        let error = BuildError::new("cache", PortError::config("bad"));

        assert_eq!(error.to_string(), "cannot build the cache plugin");
    }

    #[test]
    fn build_error_source_is_the_port_error() {
        let error = BuildError::new("cache", PortError::config("bad"));

        let source = error.source().expect("build error has a source");
        let port = source
            .downcast_ref::<PortError>()
            .expect("source is a PortError");
        assert!(matches!(port, PortError::Config(cause) if cause.to_string() == "bad"));
    }

    #[test]
    fn build_error_chain_includes_the_configuration_cause() {
        let error = BuildError::new("cache", PortError::config("bad"));

        assert_eq!(
            error_chain(&error).to_string(),
            "cannot build the cache plugin: invalid plugin configuration: bad"
        );
    }
}
