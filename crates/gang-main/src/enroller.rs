//! The workload enroller of `gang worker`: exchanges the worker's workload proof for a client
//! certificate from the server. The configured server CA pin is resolved on the first enrollment;
//! without a pin, the public web PKI is trusted.

use gang_client::ClientError;
use gang_mtls::{KeyPair, MtlsError};
use gang_types::{Proof, ProviderName};
use gang_wire::CreateCertificateRequest;
use gang_worker::{Credentials, EnrollError, Enroller};

use crate::worker_server::{ServerCallError, ServerCalls, WorkerServer};

/// Why a workload enrollment failed: the server exchange, or the client key.
#[derive(Debug, thiserror::Error)]
enum EnrollmentFailure {
    /// The server CA pin could not be read, or the server could not be reached, rejected the
    /// proof, or answered badly.
    #[error("cannot exchange the workload proof for a certificate")]
    Server(#[source] ServerCallError),
    /// The client key or its certificate signing request could not be produced.
    #[error("cannot generate the client key")]
    Key(#[source] MtlsError),
    /// The server published no certificate authority, so the worker has no trust anchor.
    #[error("the server does not publish its certificate authority")]
    NoCaCertificate,
    /// The server created the certificate but did not return its PEM.
    #[error("the server returned no certificate")]
    NoCertificate,
}

/// Enrolls a worker with the server through the workload provider of its worker authentication.
pub(crate) struct WorkloadEnroller<S = WorkerServer> {
    api: S,
    provider: ProviderName,
}

impl WorkloadEnroller {
    /// An enroller through `server`, which connects on the first enrollment. Performs no I/O.
    pub(crate) fn new(server: WorkerServer, provider: ProviderName) -> Self {
        Self {
            api: server,
            provider,
        }
    }
}

impl<S: ServerCalls> WorkloadEnroller<S> {
    /// Generates a fresh key and fetches the server's CA certificate. Fails before requesting
    /// a certificate when the server publishes no CA, then requests a certificate for the key,
    /// presenting `proof`.
    async fn exchange(&self, proof: Proof) -> Result<Credentials, EnrollmentFailure> {
        let key = KeyPair::generate().map_err(EnrollmentFailure::Key)?;
        let pem_csr = key.certificate_request().map_err(EnrollmentFailure::Key)?;
        let server = self.api.server().await.map_err(EnrollmentFailure::Server)?;
        let Some(ca_certificate) = server.ca_certificate else {
            return Err(EnrollmentFailure::NoCaCertificate);
        };
        let certificate = self
            .api
            .create_certificate(&CreateCertificateRequest {
                provider: self.provider.clone(),
                proof,
                pem_csr,
            })
            .await
            .map_err(EnrollmentFailure::Server)?;
        let Some(pem_certificate) = certificate.pem_certificate else {
            return Err(EnrollmentFailure::NoCertificate);
        };
        Ok(Credentials {
            certificate: pem_certificate,
            private_key: key.private_key().clone(),
            ca_certificate,
            not_after: certificate.expire_time,
        })
    }
}

/// Classifies an enrollment failure for the worker: a rate-limited exchange keeps its retry
/// delay, every other failure is a plain enrollment error.
fn enroll_error(failure: EnrollmentFailure) -> EnrollError {
    let rate_limit = match &failure {
        EnrollmentFailure::Server(ServerCallError::Client(ClientError::RateLimited {
            retry_after,
        })) => Some(*retry_after),
        _ => None,
    };
    match rate_limit {
        Some(retry_after) => EnrollError::rate_limited(failure, retry_after),
        None => EnrollError::new(failure),
    }
}

impl<S: ServerCalls> Enroller for WorkloadEnroller<S> {
    async fn enroll(&self, proof: Proof) -> Result<Credentials, EnrollError> {
        self.exchange(proof).await.map_err(enroll_error)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gang_clock::Timestamp;
    use gang_types::CertificatePem;
    use gang_wire::{Certificate, Server};
    use gang_worker::RateLimit;

    use super::*;

    #[test]
    fn rate_limited_failure_carries_the_retry_after_delay() {
        let failure =
            EnrollmentFailure::Server(ServerCallError::Client(ClientError::RateLimited {
                retry_after: Some(Duration::from_secs(5)),
            }));

        let error = enroll_error(failure);

        assert_eq!(
            error.rate_limit(),
            Some(RateLimit {
                retry_after: Some(Duration::from_secs(5)),
            })
        );
    }

    #[test]
    fn rate_limited_failure_without_a_retry_after_delay_is_still_rate_limited() {
        let failure =
            EnrollmentFailure::Server(ServerCallError::Client(ClientError::RateLimited {
                retry_after: None,
            }));

        let error = enroll_error(failure);

        assert_eq!(error.rate_limit(), Some(RateLimit { retry_after: None }));
    }

    #[test]
    fn server_error_is_not_a_rate_limit() {
        let failure = EnrollmentFailure::Server(ServerCallError::Client(ClientError::Server {
            status: 500,
            message: "internal error".to_owned(),
        }));

        let error = enroll_error(failure);

        assert_eq!(error.rate_limit(), None);
    }

    #[test]
    fn missing_certificate_authority_is_not_a_rate_limit() {
        let error = enroll_error(EnrollmentFailure::NoCaCertificate);

        assert_eq!(error.rate_limit(), None);
    }

    /// A PEM certificate that `gang_types` accepts; the enrollment never parses its contents.
    const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    /// A second PEM certificate, distinct from `CERTIFICATE`, issued to the worker.
    const ISSUED_CERTIFICATE: &str =
        "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----\n";

    /// The `expireTime` the fake server reports for the certificate it issues.
    const EXPIRE_TIME: &str = "2026-01-15T09:19:30.000Z";

    struct FakeServer {
        ca_certificate: Option<CertificatePem>,
        pem_certificate: Option<CertificatePem>,
        expire_time: Timestamp,
        server_error: Option<(u16, &'static str)>,
        created: AtomicUsize,
    }

    impl ServerCalls for FakeServer {
        async fn server(&self) -> Result<Server<serde_json::Value>, ServerCallError> {
            if let Some((status, message)) = self.server_error {
                return Err(ClientError::Server {
                    status,
                    message: message.to_owned(),
                }
                .into());
            }
            let mut server = serde_json::json!({
                "name": "server",
                "remoteExecutor": "grpcs://example.com",
                "remoteCache": "grpcs://example.com",
                "defaultIsa": "x86-64",
                "authentication": {"none": {}},
            });
            if let Some(ca_certificate) = &self.ca_certificate {
                server["caCertificate"] = ca_certificate.as_str().into();
            }
            serde_json::from_value(server).map_err(|error| ClientError::Body(error).into())
        }

        async fn create_certificate(
            &self,
            _request: &CreateCertificateRequest,
        ) -> Result<Certificate, ServerCallError> {
            self.created.fetch_add(1, Ordering::SeqCst);
            let mut certificate = serde_json::json!({
                "name": "certificates/1",
                "principal": "principals/workload",
                "createTime": "2026-01-14T09:19:30.000Z",
                "expireTime": self.expire_time.to_rfc3339(),
            });
            if let Some(pem_certificate) = &self.pem_certificate {
                certificate["pemCertificate"] = pem_certificate.as_str().into();
            }
            serde_json::from_value(certificate).map_err(|error| ClientError::Body(error).into())
        }
    }

    #[tokio::test]
    async fn a_server_without_a_ca_is_not_asked_for_a_certificate() -> Result<(), Box<dyn Error>> {
        let enroller = WorkloadEnroller {
            api: FakeServer {
                ca_certificate: None,
                pem_certificate: Some(CertificatePem::try_new(CERTIFICATE)?),
                expire_time: Timestamp::from_rfc3339(EXPIRE_TIME)?,
                created: AtomicUsize::new(0),
                server_error: None,
            },
            provider: ProviderName::try_new("workloads")?,
        };

        let result = enroller.exchange(Proof::new("proof")).await;

        assert!(matches!(result, Err(EnrollmentFailure::NoCaCertificate)));
        assert_eq!(enroller.api.created.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn a_server_reply_without_a_certificate_is_a_failure() -> Result<(), Box<dyn Error>> {
        let enroller = WorkloadEnroller {
            api: FakeServer {
                ca_certificate: Some(CertificatePem::try_new(CERTIFICATE)?),
                pem_certificate: None,
                expire_time: Timestamp::from_rfc3339(EXPIRE_TIME)?,
                created: AtomicUsize::new(0),
                server_error: None,
            },
            provider: ProviderName::try_new("workloads")?,
        };

        let result = enroller.exchange(Proof::new("proof")).await;

        assert!(matches!(result, Err(EnrollmentFailure::NoCertificate)));
        assert_eq!(enroller.api.created.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn credentials_carry_the_servers_ca_certificate_and_the_issued_certificate()
    -> Result<(), Box<dyn Error>> {
        let enroller = WorkloadEnroller {
            api: FakeServer {
                ca_certificate: Some(CertificatePem::try_new(CERTIFICATE)?),
                pem_certificate: Some(CertificatePem::try_new(ISSUED_CERTIFICATE)?),
                expire_time: Timestamp::from_rfc3339(EXPIRE_TIME)?,
                created: AtomicUsize::new(0),
                server_error: None,
            },
            provider: ProviderName::try_new("workloads")?,
        };

        let credentials = enroller
            .exchange(Proof::new("proof"))
            .await
            .map_err(|failure| format!("{failure:?}"))?;

        assert_eq!(enroller.api.created.load(Ordering::SeqCst), 1);
        assert_eq!(credentials.ca_certificate.as_str(), CERTIFICATE);
        assert_eq!(credentials.certificate.as_str(), ISSUED_CERTIFICATE);
        assert_eq!(credentials.not_after, Timestamp::from_rfc3339(EXPIRE_TIME)?);
        assert!(!credentials.private_key.expose().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn enroll_through_the_trait_maps_a_client_failure() -> Result<(), Box<dyn Error>> {
        let enroller = WorkloadEnroller {
            api: FakeServer {
                ca_certificate: None,
                pem_certificate: None,
                expire_time: Timestamp::from_rfc3339(EXPIRE_TIME)?,
                created: AtomicUsize::new(0),
                server_error: Some((500, "internal error")),
            },
            provider: ProviderName::try_new("workloads")?,
        };

        let Err(error) = Enroller::enroll(&enroller, Proof::new("p")).await else {
            return Err("enrollment succeeded against a failing server".into());
        };

        assert_eq!(error.rate_limit(), None);
        Ok(())
    }
}
