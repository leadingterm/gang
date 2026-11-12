//! Typed errors for the TLS transport.

use std::error::Error;
use std::io;

/// An error raised while building or running the TLS transport.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The TLS handshake did not complete within the configured timeout.
    #[error("tls handshake timed out")]
    HandshakeTimeout,
    /// The TLS handshake failed: the connection broke, or the peer's certificate was rejected or revoked.
    #[error("tls handshake failed")]
    Handshake(#[source] std::io::Error),
    /// The TLS engine rejected the configuration or the peer's messages.
    #[error("tls engine error")]
    Tls(#[source] rustls::Error),
    /// The client certificate verifier could not be built from the client CA.
    #[error("client certificate verifier could not be built")]
    ClientVerifier(#[source] rustls::server::VerifierBuilderError),
    /// A PEM document could not be decoded.
    #[error("pem document could not be decoded")]
    Pem(#[source] rustls_pki_types::pem::Error),
    /// A PEM certificate document contains no certificate block.
    #[error("pem document contains no certificate")]
    NoCertificate,
    /// The operating system's certificate store holds no certificate usable as a trust anchor.
    #[error("no system root certificates are available")]
    NoSystemRoots,
    /// The peer completed the handshake without presenting a certificate.
    #[error("peer presented no certificate")]
    NoPeerCertificate,
    /// A certificate is not valid DER-encoded X.509.
    #[error("certificate is malformed")]
    Certificate(#[source] x509_cert::der::Error),
    /// A certificate serial number cannot be represented as a `CertSerial`.
    #[error("certificate serial is invalid")]
    Serial(#[source] gang_types::CertSerialError),
}

/// Reports whether a certificate in the error's source chain was refused by certificate
/// validation, including when it is wrapped in an `io::Error`.
pub fn is_certificate_refused(error: &(dyn Error + 'static)) -> bool {
    chain_contains(error, |tls| {
        matches!(tls, rustls::Error::InvalidCertificate(_))
    })
}

/// Reports whether any TLS engine error appears in the error's source chain, including when it
/// is wrapped in an `io::Error`.
pub fn is_tls_failure(error: &(dyn Error + 'static)) -> bool {
    chain_contains(error, |_| true)
}

/// Walks the source chain looking for a `rustls::Error` accepted by `found`.
///
/// `io::Error::source` skips the error wrapped by a custom `io::Error`, so the wrapped error is
/// checked through `get_ref` before continuing along `source`.
fn chain_contains(error: &(dyn Error + 'static), found: fn(&rustls::Error) -> bool) -> bool {
    if error.downcast_ref::<rustls::Error>().is_some_and(found) {
        return true;
    }
    let wrapped = error
        .downcast_ref::<io::Error>()
        .and_then(io::Error::get_ref);
    if wrapped.is_some_and(|inner| chain_contains(inner, found)) {
        return true;
    }
    error
        .source()
        .is_some_and(|source| chain_contains(source, found))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unknown_issuer() -> rustls::Error {
        rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)
    }

    #[test]
    fn no_variant_repeats_its_source() {
        let errors = [
            TransportError::HandshakeTimeout,
            TransportError::Handshake(io::Error::other("connection broke")),
            TransportError::Tls(unknown_issuer()),
            TransportError::ClientVerifier(rustls::server::VerifierBuilderError::NoRootAnchors),
            TransportError::Pem(rustls_pki_types::pem::Error::NoItemsFound),
            TransportError::NoCertificate,
            TransportError::NoSystemRoots,
            TransportError::NoPeerCertificate,
            TransportError::Certificate(x509_cert::der::ErrorKind::Failed.into()),
            TransportError::Serial(gang_types::CertSerialError::Empty),
        ];

        gang_testkit::assert_sources_not_repeated(&errors);
    }

    #[test]
    fn bare_refused_certificate_is_refused_and_tls_failure() {
        let error = unknown_issuer();
        assert!(is_certificate_refused(&error));
        assert!(is_tls_failure(&error));
    }

    #[test]
    fn refused_certificate_wrapped_in_io_error_is_detected() {
        let error = io::Error::new(io::ErrorKind::InvalidData, unknown_issuer());
        assert!(is_certificate_refused(&error));
        assert!(is_tls_failure(&error));
    }

    #[test]
    fn refused_certificate_wrapped_in_handshake_is_detected_through_source() {
        let error =
            TransportError::Handshake(io::Error::new(io::ErrorKind::InvalidData, unknown_issuer()));
        assert!(is_certificate_refused(&error));
        assert!(is_tls_failure(&error));
    }

    #[test]
    fn handshake_not_complete_is_tls_failure_but_not_refusal() {
        let error = rustls::Error::HandshakeNotComplete;
        assert!(!is_certificate_refused(&error));
        assert!(is_tls_failure(&error));
    }

    #[test]
    fn plain_io_error_is_neither() {
        let error = io::Error::other("boom");
        assert!(!is_certificate_refused(&error));
        assert!(!is_tls_failure(&error));
    }

    #[test]
    fn missing_certificate_is_neither() {
        let error = TransportError::NoCertificate;
        assert!(!is_certificate_refused(&error));
        assert!(!is_tls_failure(&error));
    }
}
