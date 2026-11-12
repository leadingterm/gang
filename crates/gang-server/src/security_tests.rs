//! Establishing the transport security of a server: plaintext has no authority, pin or
//! certificate, and TLS has all three, named after the endpoints.

use std::sync::Arc;

use gang_mtls::SubjectName;
use gang_testkit::TestSigner;
use gang_types::Url;

use crate::ServerError;
use crate::endpoint::{EndpointRole, WorkerEndpoint};
use crate::security::Security;
use crate::settings::TransportSettings;
use crate::test_support::{TlsDeployment, start_clock, tls_settings};

fn worker(text: &str) -> WorkerEndpoint {
    let url = Url::parse(text).expect("test url parses");
    WorkerEndpoint::try_new(url, true).expect("the scheme is the one tls requires")
}

async fn establish(
    transport: TransportSettings,
    worker: &WorkerEndpoint,
) -> Result<Security<TlsDeployment>, ServerError> {
    let settings = tls_settings();
    Security::establish(
        transport,
        &settings.endpoint,
        worker,
        Arc::new(TestSigner::authority().expect("the test signer builds")),
        start_clock(),
    )
    .await
}

#[tokio::test]
async fn plaintext_has_no_authority_pin_or_certificate() {
    let security = establish(
        TransportSettings::Plaintext,
        &worker("grpcs://worker.example.test:8443"),
    )
    .await
    .unwrap_or_else(|error| panic!("plaintext establishes: {error}"));

    assert!(!security.is_tls());
    assert!(security.authority().is_none());
    assert!(security.ca_pin().is_none());
    assert!(security.certificate().is_none());
}

#[tokio::test]
async fn tls_has_an_authority_a_pin_and_its_certificate() {
    let transport = tls_settings().transport;
    let security = establish(transport, &worker("grpcs://worker.example.test:8443"))
        .await
        .unwrap_or_else(|error| panic!("tls establishes: {error}"));

    assert!(security.is_tls());
    assert!(security.authority().is_some());
    let certificate = security
        .certificate()
        .expect("the authority has a certificate");
    let pin = gang_tls::ca_pin_of_certificate(&certificate).expect("the certificate is pinnable");
    assert_eq!(security.ca_pin(), Some(pin));
}

#[tokio::test]
async fn tls_names_the_listeners_after_the_endpoints() {
    let transport = tls_settings().transport;
    let security = establish(transport, &worker("grpcs://worker.example.test:8443"))
        .await
        .unwrap_or_else(|error| panic!("tls establishes: {error}"));

    let Security::Tls(tls) = security else {
        panic!("tls transport establishes tls security");
    };
    assert_eq!(
        tls.names.public,
        SubjectName::Dns("gang.example.test".to_owned())
    );
    assert_eq!(
        tls.names.worker,
        SubjectName::Dns("worker.example.test".to_owned())
    );
}

#[tokio::test]
async fn tls_without_a_worker_host_name_is_refused() {
    let transport = tls_settings().transport;
    let result = establish(transport, &worker("grpcs:worker")).await;

    assert!(matches!(
        result,
        Err(ServerError::EndpointHost {
            role: EndpointRole::Worker,
            ..
        })
    ));
}
