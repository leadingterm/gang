//! Revocation and principal restore. Each change lands in the ledger first, then in the live
//! state that cuts established connections off and refuses new credentials.

use std::sync::Arc;

use gang_spi::{
    AuthenticationHost, AuthenticationPort, CertificateTarget, CredentialLifecyclePort,
    IssuedCertRecord, PortError, StateRepositoryPort, Subject,
};
use gang_tls::RevocationSet;
use gang_types::{CertSerial, Identity};
use gang_wire::{Certificate, Principal, RevokePrincipalResponse};

use crate::core::Core;
use crate::deployment::Stack;
use crate::resources::{wire_certificate, wire_principal};

/// The revocation operations of the server.
///
/// A serial revocation covers one certificate. A principal revocation blocks new credentials
/// and revokes its worker certificates. Restoring does not restore revoked certificates.
pub(crate) struct Revocations<K: Stack> {
    core: Arc<Core<K>>,
    revoked_certificates: RevocationSet,
}

impl<K: Stack> Revocations<K> {
    /// Builds the revocation service over the core and the live set of revoked certificates.
    #[must_use]
    pub(crate) fn new(core: Arc<Core<K>>, revoked_certificates: RevocationSet) -> Self {
        Self {
            core,
            revoked_certificates,
        }
    }

    /// Revokes the certificate with `serial`. Revoking it again keeps the original revocation
    /// time.
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] with [`Subject::Certificate`] when no surface issues the
    /// certificate; other [`PortError`] variants when the ledger or a plugin fails.
    pub(crate) async fn revoke_certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Certificate, PortError> {
        let Core { host, workers, .. } = &*self.core;
        // A clients plugin presenting mTLS is refused at startup (ServerError::ClientScheme).
        if !workers.capabilities().certificates {
            return Err(PortError::NotFound(Subject::Certificate(serial.clone())));
        }
        let revoked = workers
            .revoke_certificates(host, &CertificateTarget::Serial(serial))
            .await?;
        self.cut_off(&revoked);
        revoked
            .into_iter()
            .next()
            .map(|record| wire_certificate(record, None))
            .ok_or_else(|| PortError::NotFound(Subject::Certificate(serial.clone())))
    }

    /// Revokes `identity`, registering it first if the registry has not seen it, and every
    /// certificate it holds. Revoking it again keeps the original revocation time and revokes
    /// only the certificates issued since.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the ledger or a plugin fails.
    pub(crate) async fn revoke_principal(
        &self,
        identity: &Identity,
    ) -> Result<RevokePrincipalResponse, PortError> {
        let Core { host, workers, .. } = &*self.core;
        let principal = host
            .repository
            .revoke_principal(identity, host.now())
            .await?;
        host.revoked_principals.revoke(identity.clone());

        let revoked = revoke_held(&**workers, host, identity).await?;
        self.cut_off(&revoked);
        Ok(RevokePrincipalResponse {
            principal: wire_principal(principal),
            certificates: revoked
                .into_iter()
                .map(|record| wire_certificate(record, None))
                .collect(),
        })
    }

    /// Lifts the block on a revoked principal. Its certificates stay revoked.
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] with [`Subject::Principal`] when the registry has no such
    /// principal; other [`PortError`] variants when the ledger fails.
    pub(crate) async fn restore_principal(
        &self,
        identity: &Identity,
    ) -> Result<Principal, PortError> {
        let host = &self.core.host;
        let principal = host.repository.restore_principal(identity).await?;
        host.revoked_principals.restore(identity);
        Ok(wire_principal(principal))
    }

    /// Cuts the revoked serials off the live set.
    fn cut_off(&self, revoked: &[IssuedCertRecord]) {
        for certificate in revoked {
            self.revoked_certificates.revoke(certificate.serial.clone());
        }
    }
}

/// Revokes every active certificate `identity` holds through `plugin`. A surface that does not
/// issue certificates, or holds none for `identity`, contributes nothing.
async fn revoke_held<P, H>(
    plugin: &P,
    host: &H,
    identity: &Identity,
) -> Result<Vec<IssuedCertRecord>, PortError>
where
    P: AuthenticationPort + CredentialLifecyclePort,
    H: AuthenticationHost,
{
    if !plugin.capabilities().certificates {
        return Ok(Vec::new());
    }
    match plugin
        .revoke_certificates(host, &CertificateTarget::Identity(identity))
        .await
    {
        Ok(revoked) => Ok(revoked),
        Err(PortError::NotFound(_)) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}
