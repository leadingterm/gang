//! The host the server lends to authentication plugins: the ledger, the authority's signer, the
//! identity providers, and the clock, behind [`gang_spi::AuthenticationHost`].

use std::sync::Arc;

use gang_clock::{Clock, Timestamp};
use gang_mtls::{ClientCertificateRequest, MtlsError};
use gang_spi::{
    AuthenticationHost, AuthorityRequest, CertStatus, IssuedCertRecord, IssuedCertificate,
    PortError, ProofVerifierPort, Rejection, StateRepositoryPort, Subject,
};
use gang_types::{Identity, Principal, Proof, ProviderName, Url};
use tracing::warn;

use crate::deployment::Stack;
use crate::endpoint::PublicEndpoint;
use crate::identity::Providers;
use crate::principals::{PrincipalSightings, RevokedPrincipals};
use crate::security::Security;

/// The authentication host: lends the server's collaborators to plugins for one call.
pub(crate) struct Host<K: Stack> {
    /// The ledger of certificates and principals.
    pub(crate) repository: K::Repository,
    /// The signer that holds the authority's key.
    pub(crate) signer: Arc<K::Signer>,
    /// How the listeners are secured, and the certificate authority when they are.
    pub(crate) security: Security<K>,
    /// The configured identity providers.
    pub(crate) providers: Providers<K::Identity>,
    /// The clock that times certificates, sessions and the state stream.
    pub(crate) clock: Arc<K::Clock>,
    /// The public endpoint clients reach the server on.
    pub(crate) endpoint: PublicEndpoint,
    /// The principals the server refuses.
    pub(crate) revoked_principals: RevokedPrincipals,
    /// Decides when a principal's sighting is written to the ledger.
    pub(crate) sightings: PrincipalSightings,
}

impl<K: Stack> Host<K> {
    /// Records that `identity` was seen, when its last written sighting is due. A ledger failure
    /// is logged and swallowed: a sighting never fails the request that caused it.
    pub(crate) async fn observe(&self, identity: &Identity) {
        if !self.sightings.due(identity, self.clock.monotonic()) {
            return;
        }
        if let Err(error) = self
            .repository
            .observe_principal(identity, self.clock.wall())
            .await
        {
            warn!(error = %error, "failed to record principal sighting");
        }
    }
}

impl<K: Stack> AuthenticationHost for Host<K> {
    type Repository = K::Repository;
    type Signer = K::Signer;

    fn repository(&self) -> &Self::Repository {
        &self.repository
    }

    fn signer(&self) -> &Self::Signer {
        &self.signer
    }

    fn now(&self) -> Timestamp {
        self.clock.wall()
    }

    fn endpoint(&self) -> &Url {
        self.endpoint.url()
    }

    async fn verify(&self, provider: &ProviderName, proof: &Proof) -> Result<Principal, PortError> {
        let entry = self
            .providers
            .get(provider)
            .ok_or_else(|| PortError::NotFound(Subject::Provider(provider.clone())))?;
        let principal = entry.verifier.verify(proof).await?;
        let identity = Identity::new(provider.clone(), principal.clone());
        if self.revoked_principals.contains(&identity) {
            return Err(PortError::refused(Rejection::PrincipalRevoked));
        }
        self.observe(&identity).await;
        Ok(principal)
    }

    async fn issue_certificate(
        &self,
        request: &AuthorityRequest<'_>,
    ) -> Result<IssuedCertificate, PortError> {
        let authority = self.security.authority().ok_or(PortError::Unsupported {
            operation: "issue_certificate",
        })?;
        let issued = authority
            .issue_client(&ClientCertificateRequest {
                csr: request.csr,
                provider: &request.identity.provider,
                principal: &request.identity.principal,
                lifetime: request.lifetime,
            })
            .await
            .map_err(certificate_failure)?;
        Ok(IssuedCertificate {
            certificate: issued.certificate,
            ca_certificate: authority.certificate().clone(),
            record: IssuedCertRecord {
                serial: issued.serial,
                identity: request.identity.clone(),
                issued_at: self.now(),
                expires_at: issued.not_after,
                status: CertStatus::Active,
            },
        })
    }
}

/// The port failure a certificate authority failure is reported as: a malformed signing request is
/// the caller's fault, a signer failure keeps the signer's own classification, and anything else is
/// a backend failure.
fn certificate_failure(error: MtlsError) -> PortError {
    match error {
        error @ MtlsError::Csr(_) => PortError::rejected(Rejection::MalformedRequest, error),
        MtlsError::Signer(port_error) => port_error,
        other => PortError::backend(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use gang_mtls::{CsrError, MtlsError};
    use gang_spi::{PortError, Rejection};

    use super::certificate_failure;

    #[test]
    fn malformed_signing_request_is_a_rejection() {
        let error = certificate_failure(MtlsError::Csr(CsrError::UnsupportedVersion));
        assert!(matches!(
            error,
            PortError::Rejected {
                reason: Rejection::MalformedRequest,
                ..
            }
        ));
    }

    #[test]
    fn signer_failure_keeps_its_classification() {
        let error =
            certificate_failure(MtlsError::Signer(PortError::unavailable("signer is down")));
        assert!(matches!(error, PortError::Unavailable(_)));
    }

    #[test]
    fn other_authority_failures_are_backend_failures() {
        let error = certificate_failure(MtlsError::NoServerNames);
        assert!(matches!(error, PortError::Backend(_)));
    }
}
