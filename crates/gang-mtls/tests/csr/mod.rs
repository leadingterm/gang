//! Tests for `gang_mtls::verify_csr`.

use crate::support;

use gang_mtls::{CsrError, verify_csr};
use gang_types::CertificateRequestPem;
use support::csr::{csr_pem, csr_with_extensions, modified, oid, tampered, with_version};

#[test]
fn well_formed_request_verifies() {
    let (request, _) = csr_pem(5, "dev@example.test");
    assert!(verify_csr(&request).is_ok());
}

#[test]
fn request_with_extra_extensions_still_verifies() {
    let (request, _) = csr_with_extensions(5, "dev@example.test", "dev.example.test");
    assert!(verify_csr(&request).is_ok());
}

#[test]
fn tampered_request_fails_signature_check() {
    let (request, _) = csr_pem(5, "dev@example.test");
    let bad = tampered(&request);
    assert!(matches!(verify_csr(&bad), Err(CsrError::BadSignature)));
}

#[test]
fn malformed_request_body_is_rejected() {
    let bad = CertificateRequestPem::try_new(
        "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----\n",
    )
    .expect("fixture wraps a request label around a body");
    assert!(matches!(verify_csr(&bad), Err(CsrError::Malformed(_))));
}

#[test]
fn unsupported_version_is_rejected() {
    let (request, _) = csr_pem(5, "dev@example.test");
    let bad = with_version(&request, 2);
    assert!(matches!(
        verify_csr(&bad),
        Err(CsrError::UnsupportedVersion)
    ));
}

#[test]
fn unsupported_signature_algorithm_is_rejected() {
    let (request, _) = csr_pem(5, "dev@example.test");
    let bad = modified(&request, |req| req.algorithm.oid = oid("1.3.101.112"));
    assert!(matches!(
        verify_csr(&bad),
        Err(CsrError::UnsupportedAlgorithm)
    ));
}
