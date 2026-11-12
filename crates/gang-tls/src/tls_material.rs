//! PEM decoding and certificate helpers shared by the acceptor, connector and verifier.

use gang_types::{CertSerial, CertificatePem, PrivateKeyPem};
use rustls::RootCertStore;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use x509_cert::der::Decode;

use crate::errors::TransportError;

/// Builds a root store that trusts every certificate in the CA document.
pub(crate) fn root_store(ca: &CertificatePem) -> Result<RootCertStore, TransportError> {
    let mut roots = RootCertStore::empty();
    for certificate in certificate_chain(ca)? {
        roots.add(certificate).map_err(TransportError::Tls)?;
    }
    Ok(roots)
}

/// Decodes every certificate block in the document, leaf first.
pub(crate) fn certificate_chain(
    certificate: &CertificatePem,
) -> Result<Vec<CertificateDer<'static>>, TransportError> {
    let chain = CertificateDer::pem_slice_iter(certificate.as_str().as_bytes())
        .collect::<Result<Vec<CertificateDer<'static>>, _>>()
        .map_err(TransportError::Pem)?;
    if chain.is_empty() {
        return Err(TransportError::NoCertificate);
    }
    Ok(chain)
}

/// Decodes the single PKCS#8 private key block in the document.
pub(crate) fn private_key_der(
    key: &PrivateKeyPem,
) -> Result<PrivateKeyDer<'static>, TransportError> {
    PrivateKeyDer::from_pem_slice(key.expose().as_bytes()).map_err(TransportError::Pem)
}

/// Returns the serial number of a DER-encoded leaf certificate as a `CertSerial`.
///
/// The serial is the CA-issued big-endian integer, rendered as lowercase hexadecimal.
pub(crate) fn leaf_serial(leaf: &CertificateDer<'_>) -> Result<CertSerial, TransportError> {
    let certificate =
        x509_cert::Certificate::from_der(leaf.as_ref()).map_err(TransportError::Certificate)?;
    CertSerial::from_be_bytes(certificate.tbs_certificate().serial_number().as_bytes())
        .map_err(TransportError::Serial)
}

/// The base64 body of a PEM key; a `Debug` dump must never print it.
#[cfg(test)]
pub(crate) fn key_body(key: &PrivateKeyPem) -> String {
    let body = key.expose().lines().nth(1).unwrap_or_default().to_owned();
    assert!(body.len() > 32, "the key body must be a real base64 line");
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::{ExtendedKeyUsagePurpose, TestAuthority, TestLeaf};

    const SERIAL: [u8; 16] = [
        0x40, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];

    /// A leaf for `localhost` issued by a fresh test authority, carrying `serial`.
    fn issued(serial: [u8; 16]) -> TestLeaf {
        TestAuthority::new("gang test ca")
            .expect("test ca builds")
            .issue(
                vec!["localhost".to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                serial,
                "localhost",
            )
            .expect("test leaf issues")
    }

    #[test]
    fn leaf_serial_is_lowercase_hex_of_the_serial_bytes() {
        let leaf = issued(SERIAL);
        let serial = leaf_serial(&leaf.der).expect("leaf parses");
        assert_eq!(serial.as_str(), "400102030405060708090a0b0c0d0e0f");
    }

    #[test]
    fn leaf_serial_rejects_garbage_der() {
        let garbage = CertificateDer::from(vec![1u8, 2, 3]);
        assert!(leaf_serial(&garbage).is_err());
    }

    #[test]
    fn certificate_and_key_pem_round_trip_to_the_original_der() {
        let leaf = issued(SERIAL);

        let chain = certificate_chain(&leaf.pem).expect("chain decodes");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].as_ref(), leaf.der.as_ref());

        let key_der = private_key_der(&leaf.key_pem).expect("key decodes");
        assert_eq!(key_der.secret_der(), leaf.key_der.as_slice());
    }

    #[test]
    fn garbage_pem_body_is_rejected() {
        let certificate_pem = CertificatePem::try_new(
            "-----BEGIN CERTIFICATE-----\n!!!not base64!!!\n-----END CERTIFICATE-----",
        )
        .expect("shape is a pem block");
        assert!(matches!(
            certificate_chain(&certificate_pem),
            Err(TransportError::Pem(_))
        ));

        let key_pem = PrivateKeyPem::try_new(
            "-----BEGIN PRIVATE KEY-----\n!!!not base64!!!\n-----END PRIVATE KEY-----",
        )
        .expect("shape is a pem block");
        assert!(matches!(
            private_key_der(&key_pem),
            Err(TransportError::Pem(_))
        ));
    }
}
