//! Behaviour of the authentication host: provider verification, the revoked-principal refusal,
//! sightings, and certificate issuance under each kind of transport security.

use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Clock, TestClock};
use gang_spi::{AuthenticationHost, AuthorityRequest, PortError, Rejection, Subject};
use gang_testkit::{FakeRepository, TestSigner};
use gang_types::{Identity, NonZeroDuration, Principal, Proof, ProviderName, Url};

use crate::endpoint::WorkerEndpoint;
use crate::host::Host;
use crate::principals::{PrincipalSightings, RevokedPrincipals};
use crate::security::Security;
use crate::test_support::{
    TlsDeployment, endpoint, providers, signing_request, start_clock, tls_settings,
};

const SIGHTING_INTERVAL: Duration = Duration::from_mins(1);

fn provider() -> ProviderName {
    ProviderName::try_new("example-idp").expect("test provider name is valid")
}

fn alice() -> Identity {
    Identity::new(
        provider(),
        Principal::try_new("alice").expect("test principal is valid"),
    )
}

fn host(
    clock: &Arc<TestClock>,
    repository: &FakeRepository,
    revoked_principals: RevokedPrincipals,
    security: Security<TlsDeployment>,
) -> Host<TlsDeployment> {
    let (providers, _verifier) = providers();
    Host {
        repository: repository.clone(),
        signer: Arc::new(TestSigner::authority().expect("the test signer builds")),
        security,
        providers,
        clock: clock.clone(),
        endpoint: endpoint("https://gang.example.test"),
        revoked_principals,
        sightings: PrincipalSightings::new(
            SIGHTING_INTERVAL,
            std::num::NonZeroUsize::new(10).expect("ten is non-zero"),
        ),
    }
}

fn plaintext_host(
    clock: &Arc<TestClock>,
    repository: &FakeRepository,
    revoked_principals: RevokedPrincipals,
) -> Host<TlsDeployment> {
    host(clock, repository, revoked_principals, Security::Plaintext)
}

async fn tls_security(clock: &Arc<TestClock>) -> Security<TlsDeployment> {
    let settings = tls_settings();
    let worker = WorkerEndpoint::try_new(
        Url::parse("grpcs://worker.example.test:8443").expect("test url parses"),
        true,
    )
    .expect("grpcs is accepted under tls");
    Security::establish(
        settings.transport,
        &settings.endpoint,
        &worker,
        Arc::new(TestSigner::authority().expect("the test signer builds")),
        clock.clone(),
    )
    .await
    .unwrap_or_else(|error| panic!("the test security establishes: {error}"))
}

#[tokio::test]
async fn an_unknown_provider_is_not_found() {
    let clock = start_clock();
    let host = plaintext_host(&clock, &FakeRepository::default(), RevokedPrincipals::new());
    let other = ProviderName::try_new("other").expect("test provider name is valid");

    let result = host.verify(&other, &Proof::new("alice-proof")).await;

    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Provider(name))) if name == other
    ));
}

#[tokio::test]
async fn a_revoked_principal_is_refused_and_not_recorded() {
    let clock = start_clock();
    let repository = FakeRepository::default();
    let host = plaintext_host(
        &clock,
        &repository,
        RevokedPrincipals::from_identities([alice()]),
    );

    let result = host.verify(&provider(), &Proof::new("alice-proof")).await;

    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::PrincipalRevoked,
            ..
        })
    ));
    assert!(repository.principals().is_empty());
}

#[tokio::test]
async fn a_verified_principal_is_returned_and_recorded() {
    let clock = start_clock();
    let repository = FakeRepository::default();
    let host = plaintext_host(&clock, &repository, RevokedPrincipals::new());

    let principal = host
        .verify(&provider(), &Proof::new("alice-proof"))
        .await
        .expect("the proof is accepted");

    assert_eq!(principal, alice().principal);
    let recorded = repository.principals();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].identity, alice());
}

#[tokio::test]
async fn a_sighting_is_written_again_only_after_the_interval() {
    let clock = start_clock();
    let repository = FakeRepository::default();
    let host = plaintext_host(&clock, &repository, RevokedPrincipals::new());
    let first = clock.wall();

    host.observe(&alice()).await;
    clock.advance(SIGHTING_INTERVAL / 2);
    host.observe(&alice()).await;
    assert_eq!(repository.principals()[0].last_seen, first);

    clock.advance(SIGHTING_INTERVAL / 2 + Duration::from_secs(1));
    host.observe(&alice()).await;
    let recorded = repository.principals();
    assert_eq!(recorded[0].first_seen, first);
    assert_eq!(recorded[0].last_seen, clock.wall());
}

fn authority_request<'a>(
    csr: &'a gang_types::CertificateRequestPem,
    identity: &'a Identity,
) -> AuthorityRequest<'a> {
    AuthorityRequest {
        csr,
        identity,
        lifetime: NonZeroDuration::from_mins(1),
    }
}

#[tokio::test]
async fn issuing_a_certificate_under_plaintext_is_unsupported() {
    let clock = start_clock();
    let host = plaintext_host(&clock, &FakeRepository::default(), RevokedPrincipals::new());
    let csr = signing_request();
    let identity = alice();

    let result = host
        .issue_certificate(&authority_request(&csr, &identity))
        .await;

    assert!(matches!(
        result,
        Err(PortError::Unsupported {
            operation: "issue_certificate"
        })
    ));
}

#[tokio::test]
async fn issuing_a_certificate_under_tls_uses_the_authority() {
    let clock = start_clock();
    let security = tls_security(&clock).await;
    let host = host(
        &clock,
        &FakeRepository::default(),
        RevokedPrincipals::new(),
        security,
    );
    let csr = signing_request();
    let identity = alice();

    let issued = host
        .issue_certificate(&authority_request(&csr, &identity))
        .await
        .expect("the authority issues the certificate");

    assert!(issued.record.expires_at > clock.wall());
}

#[test]
fn the_host_endpoint_is_the_public_endpoint() {
    let clock = start_clock();
    let host = plaintext_host(&clock, &FakeRepository::default(), RevokedPrincipals::new());
    assert_eq!(host.endpoint().as_str(), "https://gang.example.test/");
}
