//! Tests for `CertificateAuthority` establishment and client certificate issuance.

use crate::support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use const_oid::ObjectIdentifier;
use gang_clock::{TestClock, Timestamp};
use gang_mtls::{
    CertificateAuthority, ClientCertificateRequest, CsrError, IssuedCertificate, MtlsError,
    spiffe_id,
};
use gang_spi::{CertificateSignerPort, PortError, SignatureAlgorithm};
use gang_testkit::{FailingSigner, GarbageSigner, TestSigner, WrongKeySigner};
use gang_types::{CertificateRequestPem, NonZeroDuration, Principal, ProviderName};
use p256::ecdsa::VerifyingKey;
use x509_cert::Certificate;
use x509_cert::der::Decode;
use x509_cert::ext::pkix::name::GeneralName;
use x509_cert::ext::pkix::{
    AuthorityKeyIdentifier, BasicConstraints, ExtendedKeyUsage, KeyUsage, KeyUsages,
    SubjectAltName, SubjectKeyIdentifier,
};

use support::certificates::{parse_certificate, verify_signed_by};
use support::csr::{csr_pem, csr_with_extensions, tampered};
use support::fixtures;

const BASIC_CONSTRAINTS: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.19");
const KEY_USAGE: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.15");
const EXTENDED_KEY_USAGE: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.37");
const SUBJECT_ALT_NAME: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.17");
const SUBJECT_KEY_IDENTIFIER: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.14");
const AUTHORITY_KEY_IDENTIFIER: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.35");
const CLIENT_AUTH: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.3.6.1.5.5.7.3.2");

/// Wraps [`TestSigner`] and counts every signature it produces.
struct CountingSigner {
    inner: TestSigner,
    calls: Arc<AtomicUsize>,
}

impl CertificateSignerPort for CountingSigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        self.inner.algorithm()
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        self.inner.public_key().await
    }

    async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, PortError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.sign(message).await
    }
}

/// Decodes the extension `id` from `certificate`, if present and well formed.
fn extension<'a, T: Decode<'a>>(certificate: &'a Certificate, id: ObjectIdentifier) -> Option<T> {
    let extensions = certificate.tbs_certificate().extensions()?;
    let found = extensions
        .iter()
        .find(|extension| extension.extn_id == id)?;
    T::from_der(found.extn_value.as_bytes()).ok()
}

/// Whole seconds since the Unix epoch.
fn secs(at: Timestamp) -> u64 {
    at.as_unix_millis() / 1000
}

async fn authority() -> (CertificateAuthority<TestSigner, TestClock>, VerifyingKey) {
    authority_with_backdate(fixtures::backdate()).await
}

async fn authority_with_backdate(
    backdate: Duration,
) -> (CertificateAuthority<TestSigner, TestClock>, VerifyingKey) {
    let signer = TestSigner::authority().expect("the test seeds are valid scalars");
    let key = signer.verifying_key();
    let ca = CertificateAuthority::establish(
        signer,
        fixtures::clock(),
        fixtures::trust_domain(),
        backdate,
    )
    .await
    .expect("authority establishes with the test signer");
    (ca, key)
}

async fn issue<S: CertificateSignerPort>(
    ca: &CertificateAuthority<S, TestClock>,
    csr: &CertificateRequestPem,
    lifetime: NonZeroDuration,
) -> Result<IssuedCertificate, MtlsError> {
    let provider: ProviderName = fixtures::provider();
    let principal: Principal = fixtures::principal();
    ca.issue_client(&ClientCertificateRequest {
        csr,
        provider: &provider,
        principal: &principal,
        lifetime,
    })
    .await
}

#[tokio::test]
async fn authority_certificate_is_self_signed_ca() {
    let (ca, key) = authority().await;
    let cert = parse_certificate(ca.certificate());
    let tbs = cert.tbs_certificate();

    assert_eq!(tbs.subject().to_string(), "CN=gang-ca example.test");
    assert_eq!(tbs.issuer(), tbs.subject());
    assert_eq!(tbs.validity().not_before.to_unix_duration(), Duration::ZERO);
    assert_eq!(
        tbs.validity().not_after.to_unix_duration().as_secs(),
        secs(Timestamp::END_OF_TIME)
    );

    let constraints = extension::<BasicConstraints>(&cert, BASIC_CONSTRAINTS);
    assert!(constraints.is_some_and(|constraints| constraints.ca));

    let usage = extension::<KeyUsage>(&cert, KEY_USAGE).expect("authority has key usage");
    assert_eq!(usage.0, KeyUsage(KeyUsages::KeyCertSign.into()).0);

    assert!(extension::<SubjectKeyIdentifier>(&cert, SUBJECT_KEY_IDENTIFIER).is_some());
    verify_signed_by(&cert, &key);
}

#[tokio::test]
async fn client_certificate_has_expected_identity_and_validity() {
    let (ca, _) = authority().await;
    let (csr, _) = csr_pem(5, "dev@example.test");
    let issued = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("client certificate issues");
    let cert = parse_certificate(&issued.certificate);
    let tbs = cert.tbs_certificate();

    assert_eq!(tbs.subject().to_string(), "CN=dev@example.test");
    assert_eq!(
        secs(issued.not_before),
        secs(fixtures::now() - Duration::from_mins(1))
    );
    assert_eq!(
        secs(issued.not_after),
        secs(fixtures::now() + Duration::from_hours(1))
    );
    assert_eq!(
        tbs.validity().not_before.to_unix_duration().as_secs(),
        secs(issued.not_before)
    );
    assert_eq!(
        tbs.validity().not_after.to_unix_duration().as_secs(),
        secs(issued.not_after)
    );
}

#[tokio::test]
async fn zero_backdate_sets_not_before_to_issuance() {
    let (ca, _) = authority_with_backdate(Duration::ZERO).await;
    let (csr, _) = csr_pem(5, "dev@example.test");
    let issued = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("client certificate issues");
    let cert = parse_certificate(&issued.certificate);

    assert_eq!(secs(issued.not_before), secs(fixtures::now()));
    assert_eq!(
        cert.tbs_certificate()
            .validity()
            .not_before
            .to_unix_duration()
            .as_secs(),
        secs(fixtures::now())
    );
}

#[tokio::test]
async fn custom_backdate_moves_not_before_back() {
    let (ca, _) = authority_with_backdate(Duration::from_mins(5)).await;
    let (csr, _) = csr_pem(5, "dev@example.test");
    let issued = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("client certificate issues");
    let cert = parse_certificate(&issued.certificate);
    let expected = secs(fixtures::now() - Duration::from_mins(5));

    assert_eq!(secs(issued.not_before), expected);
    assert_eq!(
        cert.tbs_certificate()
            .validity()
            .not_before
            .to_unix_duration()
            .as_secs(),
        expected
    );
}

#[tokio::test]
async fn client_certificate_carries_spiffe_san_and_usages() {
    let (ca, key) = authority().await;
    let (csr, _) = csr_pem(5, "dev@example.test");
    let issued = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("client certificate issues");
    let cert = parse_certificate(&issued.certificate);
    let ca_cert = parse_certificate(ca.certificate());

    let expected = spiffe_id(
        &fixtures::trust_domain(),
        &fixtures::provider(),
        &fixtures::principal(),
    );
    assert_eq!(expected, "spiffe://example.test/alpha/dev%40example.test");

    let san = extension::<SubjectAltName>(&cert, SUBJECT_ALT_NAME).expect("client has SAN");
    assert_eq!(san.0.len(), 1);
    assert!(matches!(
        &san.0[0],
        GeneralName::UniformResourceIdentifier(uri) if uri.as_str() == expected
    ));

    let eku = extension::<ExtendedKeyUsage>(&cert, EXTENDED_KEY_USAGE).expect("client has EKU");
    assert_eq!(eku.0, vec![CLIENT_AUTH]);

    let usage = extension::<KeyUsage>(&cert, KEY_USAGE).expect("client has key usage");
    assert!(usage.0.contains(KeyUsages::DigitalSignature));

    let constraints = extension::<BasicConstraints>(&cert, BASIC_CONSTRAINTS);
    assert!(!constraints.is_some_and(|constraints| constraints.ca));

    let ca_ski = extension::<SubjectKeyIdentifier>(&ca_cert, SUBJECT_KEY_IDENTIFIER)
        .expect("authority has a subject key identifier");
    let aki = extension::<AuthorityKeyIdentifier>(&cert, AUTHORITY_KEY_IDENTIFIER)
        .expect("client has authority key identifier");
    assert_eq!(
        aki.key_identifier.map(|id| id.as_bytes().to_vec()),
        Some(ca_ski.0.as_bytes().to_vec())
    );

    assert_eq!(
        cert.tbs_certificate().issuer(),
        ca_cert.tbs_certificate().subject()
    );
    verify_signed_by(&cert, &key);
}

#[tokio::test]
async fn client_certificate_public_key_is_csr_key() {
    let (ca, _) = authority().await;
    let (csr, csr_key) = csr_pem(5, "dev@example.test");
    let issued = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("client certificate issues");
    let cert = parse_certificate(&issued.certificate);

    assert_eq!(
        cert.tbs_certificate()
            .subject_public_key_info()
            .subject_public_key
            .raw_bytes(),
        csr_key.to_sec1_point(false).as_bytes()
    );
}

#[tokio::test]
async fn client_certificate_serial_matches_issued_serial() {
    let (ca, _) = authority().await;
    let (csr, _) = csr_pem(5, "dev@example.test");
    let issued = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("client certificate issues");
    let cert = parse_certificate(&issued.certificate);
    let bytes = cert.tbs_certificate().serial_number().as_bytes();

    assert_eq!(bytes.len(), 16);
    assert!(bytes[0] & 0x80 == 0 && bytes[0] & 0x40 != 0);
    assert_eq!(hex::encode(bytes), issued.serial.as_str());
    assert_eq!(issued.serial.as_str().len(), 32);
}

#[tokio::test]
async fn successive_issuances_get_distinct_serials() {
    let (ca, _) = authority().await;
    let (csr, _) = csr_pem(5, "dev@example.test");
    let first = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("first certificate issues");
    let second = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("second certificate issues");

    assert_ne!(first.serial.as_str(), second.serial.as_str());
}

#[tokio::test]
async fn csr_extensions_do_not_reach_client_certificate() {
    let (ca, _) = authority().await;
    let (csr, _) = csr_with_extensions(5, "dev@example.test", "dev.example.test");
    let issued = issue(&ca, &csr, NonZeroDuration::from_hours(1))
        .await
        .expect("client certificate issues");
    let cert = parse_certificate(&issued.certificate);

    let constraints = extension::<BasicConstraints>(&cert, BASIC_CONSTRAINTS);
    assert!(!constraints.is_some_and(|constraints| constraints.ca));

    let san = extension::<SubjectAltName>(&cert, SUBJECT_ALT_NAME).expect("client has SAN");
    assert_eq!(san.0.len(), 1);
    assert!(matches!(
        &san.0[0],
        GeneralName::UniformResourceIdentifier(_)
    ));
}

#[tokio::test]
async fn tampered_csr_is_rejected_before_signing() {
    let calls = Arc::new(AtomicUsize::new(0));
    let signer = CountingSigner {
        inner: TestSigner::authority().expect("the test seeds are valid scalars"),
        calls: Arc::clone(&calls),
    };
    let ca = CertificateAuthority::establish(
        signer,
        fixtures::clock(),
        fixtures::trust_domain(),
        fixtures::backdate(),
    )
    .await
    .expect("authority establishes with the counting signer");
    let before = calls.load(Ordering::SeqCst);

    let (csr, _) = csr_pem(5, "dev@example.test");
    let bad = tampered(&csr);
    let result = issue(&ca, &bad, NonZeroDuration::from_hours(1)).await;

    assert!(matches!(
        result,
        Err(MtlsError::Csr(CsrError::BadSignature))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn failing_signer_surfaces_signer_error() {
    let result = CertificateAuthority::establish(
        FailingSigner,
        fixtures::clock(),
        fixtures::trust_domain(),
        fixtures::backdate(),
    )
    .await;
    assert!(matches!(result, Err(MtlsError::Signer(_))));
}

#[tokio::test]
async fn wrong_key_signer_fails_self_check() {
    let result = CertificateAuthority::establish(
        WrongKeySigner::new().expect("the test seeds are valid scalars"),
        fixtures::clock(),
        fixtures::trust_domain(),
        fixtures::backdate(),
    )
    .await;
    assert!(matches!(result, Err(MtlsError::SignatureInvalid)));
}

#[tokio::test]
async fn garbage_signer_fails_self_check() {
    let result = CertificateAuthority::establish(
        GarbageSigner::new().expect("the test seeds are valid scalars"),
        fixtures::clock(),
        fixtures::trust_domain(),
        fixtures::backdate(),
    )
    .await;
    assert!(matches!(result, Err(MtlsError::SignatureInvalid)));
}
