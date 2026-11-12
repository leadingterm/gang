use crate::support;

use std::time::Duration;

use der::Document;
use gang_clock::{Clock, TestClock};
use gang_mtls::{CertificateAuthority, ClientCertificateRequest};
use gang_testkit::TestSigner;
use gang_types::NonZeroDuration;
use rustls_pki_types::{CertificateDer, TrustAnchor, UnixTime};
use webpki::{
    ALL_VERIFICATION_ALGS, EndEntityCert, Error as WebpkiError, KeyUsage, anchor_from_trusted_cert,
};

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

fn der_of(pem: &str) -> Vec<u8> {
    let (_, document) = Document::from_pem(pem).expect("pem decodes");
    document.as_bytes().to_vec()
}

fn unix_time(clock: &TestClock) -> UnixTime {
    UnixTime::since_unix_epoch(Duration::from_millis(clock.wall().as_unix_millis()))
}

fn verify(
    certificate: &CertificateDer<'_>,
    anchors: &[TrustAnchor<'_>],
    time: UnixTime,
    usage: KeyUsage,
) -> Result<(), WebpkiError> {
    let end_entity = EndEntityCert::try_from(certificate)?;
    end_entity
        .verify_for_usage(ALL_VERIFICATION_ALGS, anchors, &[], time, usage, None, None)
        .map(|_| ())
}

async fn issue_client(
    authority: &CertificateAuthority<TestSigner, &TestClock>,
    key_seed: u8,
    subject: &str,
) -> (CertificateDer<'static>, gang_types::CertSerial) {
    let (csr, _) = support::csr::csr_pem(key_seed, subject);
    let issued = authority
        .issue_client(&ClientCertificateRequest {
            csr: &csr,
            provider: &support::fixtures::provider(),
            principal: &support::fixtures::principal(),
            lifetime: LIFETIME,
        })
        .await
        .expect("client certificate issues");
    let certificate = CertificateDer::from(der_of(issued.certificate.as_str()));
    (certificate, issued.serial)
}

#[tokio::test]
async fn issued_client_certificate_verifies_for_client_auth() {
    let clock = support::fixtures::clock();
    let authority = establish(&clock).await;
    let ca_der = CertificateDer::from(der_of(authority.certificate().as_str()));
    let anchor = anchor_from_trusted_cert(&ca_der).expect("ca certificate is a trust anchor");
    let anchors = [anchor];
    let (client, _) = issue_client(&authority, 11, "client-a.example.test").await;

    let result = verify(
        &client,
        &anchors,
        unix_time(&clock),
        KeyUsage::client_auth(),
    );

    assert!(result.is_ok(), "client certificate must verify: {result:?}");
}

#[tokio::test]
async fn client_certificate_is_rejected_for_server_auth() {
    let clock = support::fixtures::clock();
    let authority = establish(&clock).await;
    let ca_der = CertificateDer::from(der_of(authority.certificate().as_str()));
    let anchor = anchor_from_trusted_cert(&ca_der).expect("ca certificate is a trust anchor");
    let anchors = [anchor];
    let (client, _) = issue_client(&authority, 12, "client-b.example.test").await;

    let result = verify(
        &client,
        &anchors,
        unix_time(&clock),
        KeyUsage::server_auth(),
    );

    assert!(
        matches!(result, Err(WebpkiError::RequiredEkuNotFoundContext(_))),
        "a client certificate must not verify for server auth: {result:?}"
    );
}
