//! A throwaway certificate authority for TLS tests: it self-signs a CA and issues leaves with
//! chosen names, usages, serials and validity windows.
//!
//! Every leaf is signed with a freshly generated key, so tests that need a certificate chain
//! build it here instead of shipping fixed PEM files.

use gang_types::{CertSerial, CertSerialError, CertificatePem, PemError, PrivateKeyPem};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose,
    SerialNumber, date_time_ymd,
};
use rustls_pki_types::CertificateDer;

pub use rcgen::ExtendedKeyUsagePurpose;

/// A validity window of a test certificate: `(not_before, not_after)`, each a UTC calendar date
/// given as `(year, month, day)`.
pub type Window = ((i32, u8, u8), (i32, u8, u8));

/// Why a test authority or leaf could not be built. Every message is lowercase and names the
/// step that failed; the cause is the error's source, so the message never repeats it.
#[derive(Debug, thiserror::Error)]
pub enum TlsFixtureError {
    /// A key pair could not be generated.
    #[error("cannot generate a test key")]
    KeyGeneration(#[source] rcgen::Error),
    /// A certificate could not be signed by its authority.
    #[error("cannot sign a test certificate")]
    Signing(#[source] rcgen::Error),
    /// The subject names are not valid for a certificate.
    #[error("test certificate names are invalid")]
    Names(#[source] rcgen::Error),
    /// A key or certificate is not a PEM block of the expected kind.
    #[error("test key or certificate is not valid pem")]
    Pem(#[from] PemError),
    /// The serial number cannot be written as certificate serial text.
    #[error("test serial number is invalid")]
    Serial(#[from] CertSerialError),
}

/// A certificate authority that issues the test certificates.
pub struct TestAuthority {
    params: CertificateParams,
    key: KeyPair,
    /// The authority's certificate, PEM encoded.
    pub pem: CertificatePem,
    /// The authority's certificate, DER encoded.
    pub der: CertificateDer<'static>,
}

/// A leaf certificate issued by a [`TestAuthority`], with its key.
pub struct TestLeaf {
    /// The certificate's serial number, as lowercase hex.
    pub serial: CertSerial,
    /// The leaf certificate, PEM encoded.
    pub pem: CertificatePem,
    /// The leaf's private key, PEM encoded.
    pub key_pem: PrivateKeyPem,
    /// The leaf certificate, DER encoded.
    pub der: CertificateDer<'static>,
    /// The leaf's private key, DER encoded as PKCS#8.
    pub key_der: Vec<u8>,
}

impl TestAuthority {
    /// Creates a self-signed certificate authority named `common_name`.
    ///
    /// # Errors
    ///
    /// Returns [`TlsFixtureError::KeyGeneration`] when the authority's key cannot be generated,
    /// [`TlsFixtureError::Signing`] when it cannot self-sign, and [`TlsFixtureError::Pem`] when
    /// its certificate text is not PEM.
    pub fn new(common_name: &str) -> Result<Self, TlsFixtureError> {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        let key = KeyPair::generate().map_err(TlsFixtureError::KeyGeneration)?;
        let certificate = params.self_signed(&key).map_err(TlsFixtureError::Signing)?;
        Ok(Self {
            pem: CertificatePem::try_new(certificate.pem())?,
            der: certificate.der().clone(),
            params,
            key,
        })
    }

    /// Issues a leaf named by `names`, with `usages`, `serial` and `common_name`, valid for
    /// rcgen's default window.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`TestAuthority::issue_within`].
    pub fn issue(
        &self,
        names: Vec<String>,
        usages: Vec<ExtendedKeyUsagePurpose>,
        serial: [u8; 16],
        common_name: &str,
    ) -> Result<TestLeaf, TlsFixtureError> {
        self.issue_within(names, usages, serial, common_name, None)
    }

    /// Issues a leaf as [`TestAuthority::issue`] does, valid within `window` when one is given.
    ///
    /// # Errors
    ///
    /// Returns [`TlsFixtureError::Names`] when `names` are invalid,
    /// [`TlsFixtureError::KeyGeneration`] when the leaf's key cannot be generated,
    /// [`TlsFixtureError::Signing`] when signing fails, and [`TlsFixtureError::Serial`] or
    /// [`TlsFixtureError::Pem`] when the leaf's fields cannot be built.
    ///
    /// # Panics
    ///
    /// Panics if a `window` date is not a real calendar date, as rcgen's `date_time_ymd` does.
    pub fn issue_within(
        &self,
        names: Vec<String>,
        usages: Vec<ExtendedKeyUsagePurpose>,
        serial: [u8; 16],
        common_name: &str,
        window: Option<Window>,
    ) -> Result<TestLeaf, TlsFixtureError> {
        let mut params = CertificateParams::new(names).map_err(TlsFixtureError::Names)?;
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.serial_number = Some(SerialNumber::from_slice(&serial));
        params.extended_key_usages = usages;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        if let Some((from, until)) = window {
            params.not_before = date_time_ymd(from.0, from.1, from.2);
            params.not_after = date_time_ymd(until.0, until.1, until.2);
        }
        let key = KeyPair::generate().map_err(TlsFixtureError::KeyGeneration)?;
        let issuer = Issuer::new(self.params.clone(), &self.key);
        let certificate = params
            .signed_by(&key, &issuer)
            .map_err(TlsFixtureError::Signing)?;
        Ok(TestLeaf {
            serial: CertSerial::from_be_bytes(&serial)?,
            pem: CertificatePem::try_new(certificate.pem())?,
            key_pem: PrivateKeyPem::try_new(key.serialize_pem())?,
            der: certificate.der().clone(),
            key_der: key.serialize_der(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestResult;

    #[test]
    fn leaf_carries_requested_serial_and_parseable_pems() -> TestResult {
        let authority = TestAuthority::new("gang test ca")?;
        let serial = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0, 1, 2, 3, 4, 5, 6, 7,
        ];
        let leaf = authority.issue(
            vec!["localhost".to_owned()],
            vec![ExtendedKeyUsagePurpose::ServerAuth],
            serial,
            "localhost",
        )?;

        assert_eq!(leaf.serial.as_str(), hex::encode(serial));
        CertificatePem::try_new(authority.pem.as_str())?;
        CertificatePem::try_new(leaf.pem.as_str())?;
        PrivateKeyPem::try_new(leaf.key_pem.expose())?;
        Ok(())
    }

    #[test]
    fn leaf_serial_matches_certificate_der_after_leading_zero() -> TestResult {
        use x509_cert::Certificate;
        use x509_cert::der::Decode;

        let authority = TestAuthority::new("gang test ca")?;
        let serial = [0, 0x3f, 0xa9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let leaf = authority.issue(
            vec!["localhost".to_owned()],
            vec![ExtendedKeyUsagePurpose::ServerAuth],
            serial,
            "localhost",
        )?;
        let certificate = Certificate::from_der(leaf.der.as_ref())?;
        let parsed =
            CertSerial::from_be_bytes(certificate.tbs_certificate().serial_number().as_bytes())?;
        assert_eq!(leaf.serial, parsed);
        Ok(())
    }
}
