//! Client certificate verification that also rejects revoked serials.

use std::sync::Arc;

use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, Error, SignatureScheme};

use crate::revocation::RevocationSet;
use crate::tls_material::leaf_serial;

/// Wraps a webpki client verifier and fails certificates whose serial is revoked.
#[derive(Debug)]
pub(crate) struct RevokingClientVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    revocations: RevocationSet,
}

impl RevokingClientVerifier {
    /// Pairs the chain-validating `inner` verifier with the live revocation set.
    pub(crate) fn new(inner: Arc<dyn ClientCertVerifier>, revocations: RevocationSet) -> Self {
        Self { inner, revocations }
    }
}

impl ClientCertVerifier for RevokingClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let serial = leaf_serial(end_entity)
            .map_err(|_| Error::InvalidCertificate(CertificateError::BadEncoding))?;
        if self.revocations.is_revoked(&serial) {
            return Err(Error::InvalidCertificate(CertificateError::Revoked));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
