//! The certificate authority: establishes the CA and issues client and server certificates.

use std::time::Duration;

use der::Decode;
use gang_clock::{Clock, Timestamp};
use gang_spi::{CertificateSignerPort, SignatureAlgorithm};
use gang_types::{
    CertSerial, CertificatePem, CertificateRequestPem, NonZeroDuration, Principal, ProviderName,
    TrustDomain,
};
use p256::ecdsa::VerifyingKey;
use spki::SubjectPublicKeyInfoOwned;
use x509_cert::ext::Extension;
use x509_cert::name::Name;
use x509_cert::time::Validity;

use crate::certificate::{self, Window};
use crate::csr;
use crate::errors::MtlsError;
use crate::profile;
use crate::serial::Serial;
use crate::signing;
use crate::spiffe;
use crate::subject_name::SubjectName;
use crate::time;

/// A certificate authority whose signing key lives behind a [`CertificateSignerPort`].
///
/// The authority never holds private key material: every signature is produced by the signer
/// and checked against the signer's public key before it is used.
pub struct CertificateAuthority<S, C> {
    signer: S,
    clock: C,
    backdate: Duration,
    trust_domain: TrustDomain,
    name: Name,
    key_id: Vec<u8>,
    verifying_key: VerifyingKey,
    certificate: CertificatePem,
}

impl<S: CertificateSignerPort, C: Clock> CertificateAuthority<S, C> {
    /// Establishes the authority for `trust_domain` by self-signing its certificate.
    ///
    /// The signer's key must be an ECDSA P-256 key. The CA certificate never expires.
    ///
    /// Each issued certificate's `notBefore` starts `backdate` before its issuance, absorbing
    /// clock skew between the issuer and verifiers.
    ///
    /// # Errors
    ///
    /// Returns [`MtlsError::UnsupportedAlgorithm`] unless the signer uses ECDSA P-256 with
    /// SHA-256, and an error when the public key or certificate cannot be encoded or signed.
    pub async fn establish(
        signer: S,
        clock: C,
        trust_domain: TrustDomain,
        backdate: Duration,
    ) -> Result<Self, MtlsError> {
        if signer.algorithm() != SignatureAlgorithm::EcdsaP256Sha256 {
            return Err(MtlsError::UnsupportedAlgorithm);
        }
        let public_key = SubjectPublicKeyInfoOwned::from_der(&signer.public_key().await?)?;
        let verifying_key = signing::verifying_key(&public_key)?;
        let key_id = profile::key_identifier(&public_key);
        let name = certificate::common_name(&format!("gang-ca {}", trust_domain.as_str()))?;
        let tbs_certificate = certificate::tbs_certificate(
            &Serial::generate()?.to_asn1()?,
            &name,
            &name,
            &Validity::new(time::epoch()?, time::never_expires()?),
            &public_key,
            &profile::ca_extensions(&key_id)?,
        )?;
        let certificate =
            certificate::sign_certificate(&signer, &verifying_key, &tbs_certificate).await?;
        Ok(Self {
            signer,
            clock,
            backdate,
            trust_domain,
            name,
            key_id,
            verifying_key,
            certificate,
        })
    }

    /// The self-signed CA certificate that clients and servers trust.
    #[must_use]
    pub fn certificate(&self) -> &CertificatePem {
        &self.certificate
    }

    /// Issues a client certificate for the key in the request's CSR.
    ///
    /// The CSR must prove possession of its key. Only the public key is taken from the CSR;
    /// every other requested attribute or extension is ignored.
    ///
    /// # Errors
    ///
    /// Returns an error when the CSR is invalid or the certificate cannot be signed.
    pub async fn issue_client(
        &self,
        request: &ClientCertificateRequest<'_>,
    ) -> Result<IssuedCertificate, MtlsError> {
        let window = certificate::window(self.clock.wall(), request.lifetime, self.backdate);
        let csr = csr::verify_csr(request.csr)?;
        let spiffe_id = spiffe::spiffe_id(&self.trust_domain, request.provider, request.principal);
        let extensions = profile::client_extensions(
            &profile::key_identifier(&csr.public_key),
            &self.key_id,
            &spiffe_id,
        )?;
        self.issue(
            window,
            &certificate::common_name(request.principal.as_str())?,
            &csr.public_key,
            &extensions,
        )
        .await
    }

    /// Issues a server certificate for the key in the request's CSR.
    ///
    /// The subject alternative names come from `request.names`, never from the CSR.
    ///
    /// # Errors
    ///
    /// Returns [`MtlsError::NoServerNames`] when `request.names` is empty, and an error when the
    /// CSR is invalid or the certificate cannot be signed.
    pub async fn issue_server(
        &self,
        request: &ServerCertificateRequest<'_>,
    ) -> Result<IssuedCertificate, MtlsError> {
        let window = certificate::window(self.clock.wall(), request.lifetime, self.backdate);
        let first = request.names.first().ok_or(MtlsError::NoServerNames)?;
        let csr = csr::verify_csr(request.csr)?;
        let extensions = profile::server_extensions(
            &profile::key_identifier(&csr.public_key),
            &self.key_id,
            request.names,
        )?;
        self.issue(
            window,
            &certificate::common_name(&first.to_string())?,
            &csr.public_key,
            &extensions,
        )
        .await
    }

    async fn issue(
        &self,
        window: Window,
        subject: &Name,
        subject_public_key: &SubjectPublicKeyInfoOwned,
        extensions: &[Extension],
    ) -> Result<IssuedCertificate, MtlsError> {
        let serial = Serial::generate()?;
        let tbs_certificate = certificate::tbs_certificate(
            &serial.to_asn1()?,
            &self.name,
            subject,
            &certificate::validity(window)?,
            subject_public_key,
            extensions,
        )?;
        let certificate =
            certificate::sign_certificate(&self.signer, &self.verifying_key, &tbs_certificate)
                .await?;
        Ok(IssuedCertificate {
            certificate,
            serial: serial.cert_serial()?,
            not_before: window.not_before,
            not_after: window.not_after,
        })
    }
}

/// A request for a client certificate.
#[derive(Debug, Clone, Copy)]
pub struct ClientCertificateRequest<'a> {
    /// PKCS#10 request carrying the client's public key and proof of possession.
    pub csr: &'a CertificateRequestPem,
    /// The identity provider that vouched for `principal`.
    pub provider: &'a ProviderName,
    /// The authenticated principal the certificate is issued to.
    pub principal: &'a Principal,
    /// How long the certificate remains valid after issuance.
    pub lifetime: NonZeroDuration,
}

/// A request for a server certificate.
#[derive(Debug, Clone, Copy)]
pub struct ServerCertificateRequest<'a> {
    /// PKCS#10 request carrying the server's public key and proof of possession.
    pub csr: &'a CertificateRequestPem,
    /// Names the certificate is valid for. The first name becomes the subject common name.
    pub names: &'a [SubjectName],
    /// How long the certificate remains valid after issuance.
    pub lifetime: NonZeroDuration,
}

/// A certificate issued by the authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedCertificate {
    /// The PEM-encoded certificate.
    pub certificate: CertificatePem,
    /// The serial number the authority assigned.
    pub serial: CertSerial,
    /// Start of the validity window, in whole seconds.
    pub not_before: Timestamp,
    /// End of the validity window, in whole seconds.
    pub not_after: Timestamp,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use der::Encode;
    use gang_clock::TestClock;
    use gang_testkit::{TestSigner, signing_key};
    use p256::ecdsa::DerSignature;
    use x509_cert::builder::{Builder, RequestBuilder};
    use x509_cert::name::Name;

    use super::*;

    /// Returns the subject of a PEM certificate as an RFC 4514 string.
    fn subject_of(certificate: &CertificatePem) -> String {
        let (label, der) = der::pem::decode_vec(certificate.as_str().as_bytes())
            .expect("certificate is a PEM block");
        assert_eq!(label, "CERTIFICATE");
        let certificate = x509_cert::Certificate::from_der(&der).expect("certificate decodes");
        certificate.tbs_certificate().subject().to_string()
    }

    #[tokio::test]
    async fn client_certificate_subject_is_the_principal_not_the_csr_subject() {
        let authority = CertificateAuthority::establish(
            TestSigner::authority().expect("the test seeds are valid scalars"),
            TestClock::new(Timestamp::from_unix_millis(1_000_000)),
            TrustDomain::try_new("example.test").expect("valid trust domain"),
            Duration::from_mins(1),
        )
        .await
        .expect("authority establishes");

        let key = signing_key(1).expect("test seed is a valid P-256 scalar");
        let subject =
            Name::from_str("CN=admin").expect("test subject is a valid distinguished name");
        let request = RequestBuilder::new(subject)
            .expect("request builder accepts the test subject")
            .build::<_, DerSignature>(&key)
            .expect("request signs with the test key");
        let der = request.to_der().expect("request encodes as DER");
        let pem = der::pem::encode_string("CERTIFICATE REQUEST", der::pem::LineEnding::LF, &der)
            .expect("request encodes as PEM");
        let csr = CertificateRequestPem::try_new(pem).expect("request is a PEM block");
        let principal = Principal::try_new("dev@example.test").expect("valid principal");
        let provider = ProviderName::try_new("alpha").expect("valid provider");

        let issued = authority
            .issue_client(&ClientCertificateRequest {
                csr: &csr,
                provider: &provider,
                principal: &principal,
                lifetime: NonZeroDuration::from_hours(1),
            })
            .await
            .expect("client certificate issues");

        assert_eq!(subject_of(&issued.certificate), "CN=dev@example.test");
    }
}
