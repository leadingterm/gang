use crate::support;

use std::net::IpAddr;

use der::Decode;
use der::asn1::ObjectIdentifier;
use gang_clock::{Clock, TestClock, Timestamp};
use gang_mtls::{CertificateAuthority, MtlsError, ServerCertificateRequest, SubjectName};
use gang_testkit::TestSigner;
use gang_types::NonZeroDuration;
use x509_cert::Certificate;
use x509_cert::ext::Extension;
use x509_cert::ext::pkix::name::GeneralName;
use x509_cert::ext::pkix::{BasicConstraints, ExtendedKeyUsage, SubjectAltName};

const SUBJECT_ALT_NAME: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.17");
const BASIC_CONSTRAINTS: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.19");
const EXTENDED_KEY_USAGE: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.29.37");
const SERVER_AUTH: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.3.6.1.5.5.7.3.1");

const LIFETIME: NonZeroDuration = NonZeroDuration::from_hours(1);

async fn establish(clock: &TestClock) -> CertificateAuthority<TestSigner, &TestClock> {
    CertificateAuthority::establish(
        TestSigner::authority().expect("the test seeds are valid scalars"),
        clock,
        support::fixtures::trust_domain(),
        support::fixtures::backdate(),
    )
    .await
    .expect("certificate authority establishes")
}

fn extension(certificate: &Certificate, oid: ObjectIdentifier) -> &Extension {
    certificate
        .tbs_certificate()
        .extensions()
        .into_iter()
        .flatten()
        .find(|extension| extension.extn_id == oid)
        .expect("certificate carries the extension")
}

fn ip_from_octets(octets: &[u8]) -> Option<IpAddr> {
    match octets.len() {
        4 => <[u8; 4]>::try_from(octets).ok().map(IpAddr::from),
        16 => <[u8; 16]>::try_from(octets).ok().map(IpAddr::from),
        _ => None,
    }
}

fn san_names(certificate: &Certificate) -> Vec<String> {
    let san = SubjectAltName::from_der(
        extension(certificate, SUBJECT_ALT_NAME)
            .extn_value
            .as_bytes(),
    )
    .expect("subject alternative name decodes");
    san.0
        .iter()
        .map(|name| match name {
            GeneralName::DnsName(dns) => dns.as_str().to_owned(),
            GeneralName::IpAddress(octets) => ip_from_octets(octets.as_bytes())
                .map_or_else(|| format!("{name:?}"), |ip| ip.to_string()),
            other => format!("{other:?}"),
        })
        .collect()
}

fn unix_seconds(timestamp: Timestamp) -> u64 {
    timestamp.as_unix_millis() / 1000
}

#[tokio::test]
async fn server_certificate_carries_exactly_the_requested_names() {
    let clock = support::fixtures::clock();
    let authority = establish(&clock).await;
    let key = TestSigner::authority()
        .expect("the test seeds are valid scalars")
        .verifying_key();
    let (csr, _) = support::csr::csr_pem(3, "csr-subject.example.test");
    let names = [
        "API.Example.Test"
            .parse::<SubjectName>()
            .expect("dns name is valid"),
        "10.0.0.7"
            .parse::<SubjectName>()
            .expect("ip literal is valid"),
    ];

    let issued = authority
        .issue_server(&ServerCertificateRequest {
            csr: &csr,
            names: &names,
            lifetime: LIFETIME,
        })
        .await
        .expect("server certificate issues");

    let certificate = support::certificates::parse_certificate(&issued.certificate);
    support::certificates::verify_signed_by(&certificate, &key);
    assert_eq!(san_names(&certificate), ["api.example.test", "10.0.0.7"]);
    assert_eq!(
        certificate.tbs_certificate().subject().to_string(),
        "CN=api.example.test"
    );
}

#[tokio::test]
async fn server_certificate_is_limited_to_server_auth() {
    let clock = support::fixtures::clock();
    let authority = establish(&clock).await;
    let (csr, _) = support::csr::csr_pem(4, "csr-subject.example.test");
    let names = ["api.example.test"
        .parse::<SubjectName>()
        .expect("valid name")];

    let issued = authority
        .issue_server(&ServerCertificateRequest {
            csr: &csr,
            names: &names,
            lifetime: LIFETIME,
        })
        .await
        .expect("server certificate issues");
    let certificate = support::certificates::parse_certificate(&issued.certificate);

    let usages = ExtendedKeyUsage::from_der(
        extension(&certificate, EXTENDED_KEY_USAGE)
            .extn_value
            .as_bytes(),
    )
    .expect("extended key usage decodes");
    assert_eq!(usages.0, vec![SERVER_AUTH]);

    let constraints = BasicConstraints::from_der(
        extension(&certificate, BASIC_CONSTRAINTS)
            .extn_value
            .as_bytes(),
    )
    .expect("basic constraints decode");
    assert!(!constraints.ca);
}

#[tokio::test]
async fn server_certificate_validity_matches_client_certificates() {
    let clock = support::fixtures::clock();
    let now = clock.wall();
    let authority = establish(&clock).await;
    let (csr, _) = support::csr::csr_pem(5, "csr-subject.example.test");
    let names = ["api.example.test"
        .parse::<SubjectName>()
        .expect("valid name")];

    let issued = authority
        .issue_server(&ServerCertificateRequest {
            csr: &csr,
            names: &names,
            lifetime: LIFETIME,
        })
        .await
        .expect("server certificate issues");

    assert_eq!(issued.not_before, now - support::fixtures::backdate());
    assert_eq!(issued.not_after, now + LIFETIME.get());

    let certificate = support::certificates::parse_certificate(&issued.certificate);
    let validity = certificate.tbs_certificate().validity();
    assert_eq!(
        validity.not_before.to_unix_duration().as_secs(),
        unix_seconds(now - support::fixtures::backdate())
    );
    assert_eq!(
        validity.not_after.to_unix_duration().as_secs(),
        unix_seconds(now + LIFETIME.get())
    );
}

#[tokio::test]
async fn server_certificate_without_names_is_rejected() {
    let clock = support::fixtures::clock();
    let authority = establish(&clock).await;
    let (csr, _) = support::csr::csr_pem(6, "csr-subject.example.test");

    let result = authority
        .issue_server(&ServerCertificateRequest {
            csr: &csr,
            names: &[],
            lifetime: LIFETIME,
        })
        .await;

    assert!(matches!(result, Err(MtlsError::NoServerNames)));
}
