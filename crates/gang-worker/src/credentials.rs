use std::time::Duration;

use gang_clock::Timestamp;
use gang_spi::BoxedError;
use gang_types::{CertificatePem, PrivateKeyPem, Proof};

/// Certificate material issued to a worker by the certificate authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    /// The worker's certificate, which the worker presents on its mutual TLS connections.
    pub certificate: CertificatePem,
    /// The private key matching `certificate`.
    pub private_key: PrivateKeyPem,
    /// The certificate authority that signed `certificate`.
    pub ca_certificate: CertificatePem,
    /// The instant after which `certificate` is no longer valid.
    pub not_after: Timestamp,
}

/// Enrollment with the certificate authority failed.
#[derive(Debug, thiserror::Error)]
#[error("enrollment failed")]
pub struct EnrollError {
    /// The failure reported by the enrollment path.
    #[source]
    source: BoxedError,
    /// Set when the authority refused the enrollment because this client is rate limited.
    rate_limit: Option<RateLimit>,
}

impl EnrollError {
    /// Creates an enrollment failure that is not a rate-limit refusal.
    pub fn new(source: impl Into<BoxedError>) -> Self {
        Self {
            source: source.into(),
            rate_limit: None,
        }
    }

    /// Creates an enrollment failure caused by a rate-limit refusal, which may carry the delay the
    /// authority asked the client to wait before retrying.
    pub fn rate_limited(source: impl Into<BoxedError>, retry_after: Option<Duration>) -> Self {
        Self {
            source: source.into(),
            rate_limit: Some(RateLimit { retry_after }),
        }
    }

    /// The rate-limit details of this failure, or `None` when the refusal was not a rate limit.
    #[must_use]
    pub fn rate_limit(&self) -> Option<RateLimit> {
        self.rate_limit
    }
}

/// A rate-limit refusal of the certificate authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// How long the authority asked the client to wait before retrying, when it said so.
    pub retry_after: Option<Duration>,
}

/// Exchanges a workload proof for certificate credentials.
pub trait Enroller: Send + Sync {
    /// Enrolls this worker using `proof`.
    ///
    /// # Errors
    ///
    /// Returns [`EnrollError`] when the authority rejects the proof or cannot be reached.
    fn enroll(&self, proof: Proof)
    -> impl Future<Output = Result<Credentials, EnrollError>> + Send;
}
