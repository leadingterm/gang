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
    /// Set when the authority refused the enrollment for now: it is rate limiting this client
    /// or is temporarily unavailable.
    retry: Option<Retry>,
}

impl EnrollError {
    /// Creates an enrollment failure that is not worth retrying.
    pub fn new(source: impl Into<BoxedError>) -> Self {
        Self {
            source: source.into(),
            retry: None,
        }
    }

    /// Creates an enrollment failure the authority refused for now, which may carry the delay
    /// the authority asked the client to wait before retrying.
    pub fn retry_later(source: impl Into<BoxedError>, after: Option<Duration>) -> Self {
        Self {
            source: source.into(),
            retry: Some(Retry { after }),
        }
    }

    /// How the authority asked to be retried, or `None` when the failure is not worth retrying.
    #[must_use]
    pub fn retry(&self) -> Option<Retry> {
        self.retry
    }
}

/// A refusal of the certificate authority that a later enrollment may overcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    /// How long the authority asked the client to wait before retrying, when it said so.
    pub after: Option<Duration>,
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
