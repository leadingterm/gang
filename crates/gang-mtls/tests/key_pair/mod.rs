//! Tests for `gang_mtls::KeyPair`: generation, import, deterministic requests, and issuance.

use crate::support;

use der::Decode;
use gang_mtls::{CertificateAuthority, ClientCertificateRequest, KeyPair, MtlsError, verify_csr};
use gang_testkit::TestSigner;
use gang_types::{CertificateRequestPem, NonZeroDuration, Principal, PrivateKeyPem, ProviderName};
use p256::SecretKey;
use p256::elliptic_curve::sec1::ToSec1Point;
use p256::pkcs8::DecodePrivateKey;
use support::certificates::parse_certificate;
use support::csr::csr_der;
use support::fixtures;
use x509_cert::request::CertReq;

/// A PKCS#8 RSA-2048 private key, used only as a non-P-256 import fixture.
const RSA_PRIVATE_KEY: &str = include_str!("../fixtures/rsa_2048_private_key.pem");

/// A PKCS#8 P-384 private key, used only as a non-P-256 import fixture.
const P384_PRIVATE_KEY: &str = include_str!("../fixtures/p384_private_key.pem");

/// The SEC1 uncompressed public point of the key pair's P-256 key.
fn public_point(key: &KeyPair) -> Vec<u8> {
    let secret = SecretKey::from_pkcs8_pem(key.private_key().expose())
        .expect("key pair private key is a pkcs8 p-256 key");
    secret.public_key().to_sec1_point(false).as_bytes().to_vec()
}

/// The SEC1 uncompressed public point carried by the request's `SubjectPublicKeyInfo`.
fn request_point(request: &CertificateRequestPem) -> Vec<u8> {
    let req = CertReq::from_der(&csr_der(request)).expect("request decodes");
    req.info.public_key.subject_public_key.raw_bytes().to_vec()
}

#[test]
fn generated_request_verifies_and_carries_key_pair_public_key() {
    let key = KeyPair::generate().expect("operating system randomness is available");
    let request = key
        .certificate_request()
        .expect("request builds from a generated key");
    verify_csr(&request).expect("generated request verifies");
    assert_eq!(request_point(&request), public_point(&key));
}

#[test]
fn generated_key_pairs_differ() {
    let first = KeyPair::generate().expect("operating system randomness is available");
    let second = KeyPair::generate().expect("operating system randomness is available");
    assert_ne!(first.private_key(), second.private_key());
    assert_ne!(public_point(&first), public_point(&second));
}

#[test]
fn imported_key_yields_identical_request_and_public_key() {
    let key = KeyPair::generate().expect("operating system randomness is available");
    let imported = KeyPair::from_private_key(key.private_key().clone())
        .expect("generated private key imports");
    assert_eq!(imported.private_key(), key.private_key());
    assert_eq!(public_point(&imported), public_point(&key));
    assert_eq!(
        imported
            .certificate_request()
            .expect("request builds from imported key")
            .as_str(),
        key.certificate_request()
            .expect("request builds from generated key")
            .as_str()
    );
}

#[test]
fn garbage_private_key_is_rejected() {
    let garbage =
        PrivateKeyPem::try_new("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")
            .expect("fixture is a PEM block");
    assert!(matches!(
        KeyPair::from_private_key(garbage),
        Err(MtlsError::InvalidPrivateKey)
    ));
}

#[test]
fn rsa_private_key_is_rejected() {
    let rsa = PrivateKeyPem::try_new(RSA_PRIVATE_KEY).expect("fixture is a PEM block");
    assert!(matches!(
        KeyPair::from_private_key(rsa),
        Err(MtlsError::InvalidPrivateKey)
    ));
}

#[test]
fn other_curve_private_key_is_rejected() {
    let p384 = PrivateKeyPem::try_new(P384_PRIVATE_KEY).expect("fixture is a PEM block");
    assert!(matches!(
        KeyPair::from_private_key(p384),
        Err(MtlsError::InvalidPrivateKey)
    ));
}

#[test]
fn debug_never_prints_key_material() {
    let key = KeyPair::generate().expect("operating system randomness is available");
    let debug = format!("{key:?}");
    let pem = key.private_key().expose();
    let body: Vec<&str> = pem
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();

    assert!(
        !body.is_empty(),
        "the PEM must carry a base64 body to check"
    );
    assert!(!debug.contains(pem), "Debug must not print the PEM text");
    for line in body {
        assert!(
            !debug.contains(line),
            "Debug must not print a PEM body line"
        );
    }
}

#[tokio::test]
async fn authority_issues_certificate_for_generated_request() {
    let signer = TestSigner::authority().expect("the test seeds are valid scalars");
    let ca = CertificateAuthority::establish(
        signer,
        fixtures::clock(),
        fixtures::trust_domain(),
        fixtures::backdate(),
    )
    .await
    .expect("authority establishes with the test signer");
    let key = KeyPair::generate().expect("operating system randomness is available");
    let request = key
        .certificate_request()
        .expect("request builds from a generated key");
    let provider: ProviderName = fixtures::provider();
    let principal: Principal = fixtures::principal();
    let issued = ca
        .issue_client(&ClientCertificateRequest {
            csr: &request,
            provider: &provider,
            principal: &principal,
            lifetime: NonZeroDuration::from_hours(1),
        })
        .await
        .expect("client certificate issues for a generated request");
    let certificate = parse_certificate(&issued.certificate);
    assert_eq!(
        certificate
            .tbs_certificate()
            .subject_public_key_info()
            .subject_public_key
            .raw_bytes(),
        public_point(&key).as_slice()
    );
}
