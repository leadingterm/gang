//! Typed errors for certificate authority operations and CSR verification.

use gang_clock::Timestamp;
use gang_spi::PortError;
use gang_types::{CertSerialError, PemError};
use thiserror::Error;

/// Errors from establishing a certificate authority and from issuing certificates or verifying CSRs.
#[derive(Debug, Error)]
pub enum MtlsError {
    /// The certificate signer failed to produce a signature or public key.
    #[error("certificate signer failed")]
    Signer(#[from] PortError),
    /// The signer's algorithm is not ECDSA P-256 with SHA-256.
    #[error("signer algorithm is not ecdsa with sha-256 on p-256")]
    UnsupportedAlgorithm,
    /// The signer's public key is not a P-256 ECDSA key.
    #[error("signer public key is not a p-256 ecdsa key")]
    UnsupportedKey,
    /// The certificate signing request was rejected.
    #[error("certificate signing request rejected")]
    Csr(#[from] CsrError),
    /// A server certificate was requested without any names.
    #[error("server certificate needs at least one name")]
    NoServerNames,
    /// A server name is neither an IP address literal nor a valid DNS name.
    #[error("invalid server name `{0}`")]
    InvalidServerName(String),
    /// The signature produced by the signer does not verify with the CA public key.
    #[error("signature does not verify with the signer public key")]
    SignatureInvalid,
    /// A timestamp cannot be represented as an X.509 time.
    #[error("timestamp {0:?} is outside the x509 time range")]
    TimeRange(Timestamp),
    /// A structure could not be DER-encoded.
    #[error("certificate encoding failed")]
    Encoding(#[from] der::Error),
    /// A PEM text could not be built from its encoded body.
    #[error("invalid pem text")]
    Pem(#[from] PemError),
    /// A certificate serial is not valid hexadecimal certificate text.
    #[error("invalid certificate serial")]
    InvalidCertSerial(#[from] CertSerialError),
    /// The operating system provided no randomness.
    #[error("the operating system provided no randomness")]
    Entropy,
    /// The private key is not PKCS#8 text holding a P-256 ECDSA key.
    #[error("private key is not a p-256 ecdsa pkcs#8 key")]
    InvalidPrivateKey,
    /// A key pair's private key could not be encoded as PKCS#8.
    #[error("private key encoding failed")]
    KeyEncoding(#[from] p256::pkcs8::Error),
    /// A certificate signing request could not be assembled.
    #[error("certificate request could not be built")]
    RequestBuild(#[from] x509_cert::builder::Error),
}

/// Errors from verifying a PKCS#10 certificate signing request.
#[derive(Debug, Error)]
pub enum CsrError {
    /// The text is not a PEM block labelled `CERTIFICATE REQUEST`.
    #[error("certificate request is not valid pem")]
    Pem(#[source] der::Error),
    /// The request is not valid DER for a PKCS#10 `CertificationRequest`.
    #[error("certificate request is malformed")]
    Malformed(#[from] der::Error),
    /// The request version is not version 1.
    #[error("certificate request version is not 1")]
    UnsupportedVersion,
    /// The request signature algorithm is not ECDSA with SHA-256.
    #[error("certificate request signature algorithm is not ecdsa with sha-256")]
    UnsupportedAlgorithm,
    /// The requested public key is not a P-256 ECDSA key.
    #[error("certificate request public key is not a p-256 ecdsa key")]
    UnsupportedKey,
    /// The request signature does not verify with the request's own public key.
    #[error("certificate request signature does not verify with its own key")]
    BadSignature,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gang_testkit::assert_sources_not_repeated;

    use super::*;

    /// Returns a deterministic DER error for the source-bearing variants.
    fn der_error() -> der::Error {
        der::pem::Error::Label.into()
    }

    #[test]
    fn no_variant_repeats_its_source() {
        let mtls_errors = [
            MtlsError::Signer(PortError::Unsupported { operation: "sign" }),
            MtlsError::UnsupportedAlgorithm,
            MtlsError::UnsupportedKey,
            MtlsError::Csr(CsrError::BadSignature),
            MtlsError::NoServerNames,
            MtlsError::InvalidServerName("invalid".to_owned()),
            MtlsError::SignatureInvalid,
            MtlsError::TimeRange(Timestamp::UNIX_EPOCH),
            MtlsError::Encoding(der_error()),
            MtlsError::Pem(PemError::Malformed {
                label: "CERTIFICATE",
            }),
            MtlsError::InvalidCertSerial(CertSerialError::Empty),
            MtlsError::Entropy,
            MtlsError::InvalidPrivateKey,
            MtlsError::KeyEncoding(der_error().into()),
            MtlsError::RequestBuild(der_error().into()),
        ];
        let csr_errors = [
            CsrError::Pem(der_error()),
            CsrError::Malformed(der_error()),
            CsrError::UnsupportedVersion,
            CsrError::UnsupportedAlgorithm,
            CsrError::UnsupportedKey,
            CsrError::BadSignature,
        ];

        assert_sources_not_repeated(&mtls_errors);
        assert_sources_not_repeated(&csr_errors);
        assert!(csr_errors[0].source().is_some());
    }
}
