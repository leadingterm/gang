//! The `mtls` plugin against the shared fakes: issuance into the ledger, certificate
//! authentication by serial, and revocation by serial or identity.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use gang_authn::{Mtls, MtlsConfig};
use gang_clock::Timestamp;
use gang_spi::{
    Authenticated, AuthenticationHost, AuthenticationPort, Capabilities, CertStatus,
    CertificateIssue, CertificateTarget, ClientCertificates, CredentialLifecyclePort,
    IssuedCertRecord, IssuedCertificate, Peer, Plugin, PluginContext, PortError, Rejection, Scheme,
    StateRepositoryPort, Subject, TokenIssue,
};
use gang_types::{CertSerial, CertificateRequestPem, Identity, Principal, Proof, ProviderName};
use http::HeaderMap;

use crate::support::{TestHost, evidence, identity};

const START_MS: u64 = 1_800_000_000_000;
const PROVIDER: &str = "corp";
const OTHER_PROVIDER: &str = "other-idp";
const PRINCIPAL: &str = "alice@example.com";
const PROOF: &str = "alice-proof";
const CSR: &str = "-----BEGIN CERTIFICATE REQUEST-----\nMIIB\n-----END CERTIFICATE REQUEST-----\n";

fn start() -> Timestamp {
    Timestamp::from_unix_millis(START_MS)
}

/// A host that knows both providers, so a refusal is the plugin's and not the host's.
fn host() -> TestHost {
    let host = TestHost::new(start());
    host.accept_proof(PROVIDER, PROOF, PRINCIPAL);
    host.accept_proof(OTHER_PROVIDER, PROOF, PRINCIPAL);
    host
}

fn mtls_with(config_json: &str) -> Mtls {
    let config: MtlsConfig =
        serde_json::from_str(config_json).expect("the test configuration is valid");
    Mtls::build(
        config,
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )
    .expect("the test configuration builds")
}

fn mtls() -> Mtls {
    mtls_with(r#"{"provider":"corp"}"#)
}

fn csr() -> CertificateRequestPem {
    CertificateRequestPem::try_new(CSR).expect("the test request is a PEM block")
}

async fn issue(
    plugin: &Mtls,
    host: &TestHost,
    provider: &str,
    proof: &str,
) -> Result<IssuedCertificate, PortError> {
    let provider = ProviderName::try_new(provider).expect("test provider is a valid name");
    let proof = Proof::new(proof);
    let csr = csr();
    plugin
        .issue_certificate(
            host,
            &CertificateIssue {
                provider: &provider,
                proof: &proof,
                csr: &csr,
            },
        )
        .await
}

/// The serial the test authority gives its first certificate.
fn first_serial() -> CertSerial {
    CertSerial::from_be_bytes(&1_u64.to_be_bytes()).expect("the serial is not empty")
}

fn serial(byte: u8) -> CertSerial {
    CertSerial::from_be_bytes(&[byte]).expect("the serial is not empty")
}

fn presenting(serial: &CertSerial) -> Peer {
    Peer {
        certificate: Some(serial.clone()),
        ..Peer::default()
    }
}

async fn authenticated(
    plugin: &Mtls,
    host: &TestHost,
    peer: &Peer,
) -> Result<Option<Authenticated>, PortError> {
    let headers = HeaderMap::new();
    plugin.authenticate(host, &evidence(peer, &headers)).await
}

async fn authenticate(
    plugin: &Mtls,
    host: &TestHost,
    peer: &Peer,
) -> Result<Option<Arc<Identity>>, PortError> {
    authenticated(plugin, host, peer)
        .await
        .map(|found| found.map(|authenticated| authenticated.identity))
}

async fn issue_for_alice(plugin: &Mtls, host: &TestHost) -> IssuedCertificate {
    issue(plugin, host, PROVIDER, PROOF)
        .await
        .expect("a valid proof is issued a certificate")
}

#[test]
fn mtls_scheme_is_mtls() {
    assert_eq!(mtls().scheme(), Scheme::Mtls);
}

#[test]
fn capabilities_offer_certificates_but_not_tokens() {
    assert_eq!(
        mtls().capabilities(),
        Capabilities {
            certificates: true,
            tokens: false,
            sessions: false,
        }
    );
}

#[test]
fn client_certificates_are_required() {
    assert_eq!(mtls().client_certificates(), ClientCertificates::Required);
}

#[tokio::test]
async fn issuance_returns_the_certificate_the_authority_signed() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    assert!(
        issued
            .certificate
            .as_str()
            .starts_with("-----BEGIN CERTIFICATE-----")
    );
    assert_eq!(issued.record.serial, first_serial());
    assert_eq!(issued.record.expires_at, start() + Duration::from_hours(24));
}

#[tokio::test]
async fn authority_signs_for_the_identity_with_the_configured_ttl() {
    let plugin = mtls_with(r#"{"provider":"corp","certificate_ttl":"2h"}"#);
    let host = host();
    issue_for_alice(&plugin, &host).await;
    assert_eq!(
        host.authority_requests(),
        vec![(identity(PROVIDER, PRINCIPAL), Duration::from_hours(2))]
    );
}

#[tokio::test]
async fn issued_certificate_is_recorded_active_until_not_after() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    let record = host
        .repository()
        .certificate(&issued.record.serial)
        .await
        .expect("the ledger reads")
        .expect("an issued certificate is in the ledger");
    assert_eq!(record.status, CertStatus::Active);
    assert_eq!(record, issued.record);
}

#[tokio::test]
async fn failed_ledger_write_fails_issuance() {
    let plugin = mtls();
    let host = host();
    host.repository().fail_record_issued(true);
    let result = issue(&plugin, &host, PROVIDER, PROOF).await;
    assert!(matches!(result, Err(PortError::Backend(_))));
}

#[tokio::test]
async fn failed_ledger_write_leaves_no_usable_certificate() {
    let plugin = mtls();
    let host = host();
    host.repository().fail_record_issued(true);
    let _ = issue(&plugin, &host, PROVIDER, PROOF).await;
    let serial = first_serial();
    let record = host
        .repository()
        .certificate(&serial)
        .await
        .expect("the ledger reads");
    assert!(record.is_none());
    let peer = presenting(&serial);
    assert!(matches!(
        authenticate(&plugin, &host, &peer).await,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn provider_other_than_configured_is_not_found() {
    let plugin = mtls();
    let host = host();
    let result = issue(&plugin, &host, OTHER_PROVIDER, PROOF).await;
    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Provider(name)))
            if name == ProviderName::try_new(OTHER_PROVIDER).expect("valid name")
    ));
}

#[tokio::test]
async fn rejected_proof_is_propagated() {
    let plugin = mtls();
    let host = host();
    host.reject_proofs(true);
    let result = issue(&plugin, &host, PROVIDER, PROOF).await;
    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn peer_without_certificate_is_not_this_plugins_credential() {
    let plugin = mtls();
    let host = host();
    let found = authenticate(&plugin, &host, &Peer::default())
        .await
        .expect("a missing certificate is not an error");
    assert!(found.is_none());
}

#[tokio::test]
async fn issued_serial_identifies_provider_and_principal() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    let identified = authenticate(&plugin, &host, &presenting(&issued.record.serial))
        .await
        .expect("an issued certificate authenticates")
        .expect("a presented certificate identifies its caller");
    assert_eq!(*identified, identity(PROVIDER, PRINCIPAL));
}

#[tokio::test]
async fn certificate_authentication_is_valid_until_the_certificate_expires() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    let expected = Some(Authenticated {
        identity: Arc::new(identity(PROVIDER, PRINCIPAL)),
        valid_until: Some(issued.record.expires_at),
    });

    let looked_up = authenticated(&plugin, &host, &presenting(&issued.record.serial))
        .await
        .expect("an issued certificate authenticates");
    let memoized = authenticated(&plugin, &host, &presenting(&issued.record.serial))
        .await
        .expect("a memoized certificate authenticates");

    assert_eq!(looked_up, expected);
    assert_eq!(
        memoized, expected,
        "the memoized identity keeps the certificate's expiry"
    );
}

#[tokio::test]
async fn certificate_presented_after_it_expires_is_rejected() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    let peer = presenting(&issued.record.serial);
    assert!(
        authenticate(&plugin, &host, &peer)
            .await
            .expect("certificate authenticates")
            .is_some()
    );

    host.advance(Duration::from_hours(24));
    assert_eq!(host.now(), issued.record.expires_at);
    assert!(matches!(
        authenticate(&plugin, &host, &peer).await,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
    host.advance(Duration::from_secs(1));
    assert!(matches!(
        authenticate(&plugin, &host, &peer).await,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn expired_record_is_rejected_without_a_memo_entry() {
    let plugin = mtls();
    let host = host();
    let expired = serial(0x42);
    host.repository()
        .record_issued(&IssuedCertRecord {
            serial: expired.clone(),
            identity: identity(PROVIDER, PRINCIPAL),
            issued_at: start(),
            expires_at: host.now(),
            status: CertStatus::Active,
        })
        .await
        .expect("the ledger accepts the record");

    assert!(matches!(
        authenticate(&plugin, &host, &presenting(&expired)).await,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn unknown_serial_is_rejected_as_invalid_proof() {
    let plugin = mtls();
    let host = host();
    let result = authenticate(&plugin, &host, &presenting(&serial(0xee))).await;
    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        })
    ));
}

#[tokio::test]
async fn record_of_another_provider_is_policy_denied() {
    let plugin = mtls();
    let host = host();
    let foreign = serial(0x42);
    host.repository()
        .record_issued(&IssuedCertRecord {
            serial: foreign.clone(),
            identity: Identity::new(
                ProviderName::try_new(OTHER_PROVIDER).expect("valid name"),
                Principal::try_new(PRINCIPAL).expect("valid principal"),
            ),
            issued_at: start(),
            expires_at: start() + Duration::from_hours(1),
            status: CertStatus::Active,
        })
        .await
        .expect("the ledger accepts the record");
    let result = authenticate(&plugin, &host, &presenting(&foreign)).await;
    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::PolicyDenied,
            ..
        })
    ));
}

#[tokio::test]
async fn revoked_record_is_policy_denied() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&issued.record.serial))
        .await
        .expect("an issued certificate revokes");
    let result = authenticate(&plugin, &host, &presenting(&issued.record.serial)).await;
    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::PolicyDenied,
            ..
        })
    ));
}

#[tokio::test]
async fn serial_memoized_before_revocation_is_then_rejected() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    let peer = presenting(&issued.record.serial);
    authenticate(&plugin, &host, &peer)
        .await
        .expect("the certificate authenticates before revocation");
    plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&issued.record.serial))
        .await
        .expect("an issued certificate revokes");
    let result = authenticate(&plugin, &host, &peer).await;
    assert!(matches!(
        result,
        Err(PortError::Rejected {
            reason: Rejection::PolicyDenied,
            ..
        })
    ));
}

#[tokio::test]
async fn revoking_by_serial_reports_the_host_clock() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    host.advance(Duration::from_mins(5));
    let revoked = plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&issued.record.serial))
        .await
        .expect("an issued certificate revokes");
    assert_eq!(
        revoked,
        vec![IssuedCertRecord {
            status: CertStatus::Revoked {
                at: start() + Duration::from_mins(5),
            },
            ..issued.record
        }]
    );
}

#[tokio::test]
async fn revoking_again_keeps_the_original_revocation_time() {
    let plugin = mtls();
    let host = host();
    let issued = issue_for_alice(&plugin, &host).await;
    plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&issued.record.serial))
        .await
        .expect("an issued certificate revokes");
    host.advance(Duration::from_hours(1));
    let revoked = plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&issued.record.serial))
        .await
        .expect("a revoked certificate revokes again");
    assert_eq!(
        revoked,
        vec![IssuedCertRecord {
            status: CertStatus::Revoked { at: start() },
            ..issued.record
        }]
    );
}

/// Issues three certificates for alice a minute apart; returns them in issue order.
async fn issue_three(plugin: &Mtls, host: &TestHost) -> [IssuedCertificate; 3] {
    let first = issue_for_alice(plugin, host).await;
    host.advance(Duration::from_mins(1));
    let second = issue_for_alice(plugin, host).await;
    host.advance(Duration::from_mins(1));
    let third = issue_for_alice(plugin, host).await;
    [first, second, third]
}

/// Revoking by identity covers only the certificates still active. The second was already revoked
/// by serial, so it is absent here; the first and third are revoked now, at the current host time.
#[tokio::test]
async fn revoking_by_identity_lists_every_certificate_in_ledger_order() {
    let plugin = mtls();
    let host = host();
    let [first, second, third] = issue_three(&plugin, &host).await;
    host.advance(Duration::from_mins(1));
    plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&second.record.serial))
        .await
        .expect("an issued certificate revokes");
    host.advance(Duration::from_mins(1));
    let revoked = plugin
        .revoke_certificates(
            &host,
            &CertificateTarget::Identity(&identity(PROVIDER, PRINCIPAL)),
        )
        .await
        .expect("the identity has active certificates");
    assert_eq!(
        revoked,
        vec![
            IssuedCertRecord {
                status: CertStatus::Revoked {
                    at: start() + Duration::from_mins(4),
                },
                ..first.record
            },
            IssuedCertRecord {
                status: CertStatus::Revoked {
                    at: start() + Duration::from_mins(4),
                },
                ..third.record
            },
        ]
    );
}

#[tokio::test]
async fn identity_revocation_keeps_earlier_revocation_time() {
    let plugin = mtls();
    let host = host();
    let [_, second, _] = issue_three(&plugin, &host).await;
    host.advance(Duration::from_mins(1));
    plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&second.record.serial))
        .await
        .expect("an issued certificate revokes");
    host.advance(Duration::from_mins(1));
    plugin
        .revoke_certificates(
            &host,
            &CertificateTarget::Identity(&identity(PROVIDER, PRINCIPAL)),
        )
        .await
        .expect("the identity has active certificates");
    host.advance(Duration::from_mins(1));
    let revoked = plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&second.record.serial))
        .await
        .expect("a revoked certificate revokes again");
    assert_eq!(
        revoked,
        vec![IssuedCertRecord {
            status: CertStatus::Revoked {
                at: start() + Duration::from_mins(3),
            },
            ..second.record
        }]
    );
}

#[tokio::test]
async fn revoking_unknown_serial_is_not_found_certificate() {
    let plugin = mtls();
    let host = host();
    let unknown = serial(0xee);
    let result = plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&unknown))
        .await;
    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Certificate(found))) if found == unknown
    ));
}

#[tokio::test]
async fn revoking_unknown_identity_is_not_found_principal() {
    let plugin = mtls();
    let host = host();
    let nobody = identity(PROVIDER, "nobody@example.com");
    let result = plugin
        .revoke_certificates(&host, &CertificateTarget::Identity(&nobody))
        .await;
    assert!(matches!(
        result,
        Err(PortError::NotFound(Subject::Principal(found))) if found == nobody
    ));
}

#[tokio::test]
async fn revoked_serials_lists_exactly_the_revoked() {
    let plugin = mtls();
    let host = host();
    let [first, second, third] = issue_three(&plugin, &host).await;
    for revoked in [&first, &third] {
        plugin
            .revoke_certificates(&host, &CertificateTarget::Serial(&revoked.record.serial))
            .await
            .expect("an issued certificate revokes");
    }
    let mut actual = plugin
        .revoked_serials(&host)
        .await
        .expect("the ledger lists revocations");
    actual.sort();
    let mut expected = vec![first.record.serial, third.record.serial];
    expected.sort();
    assert_eq!(actual, expected);
    assert!(!actual.contains(&second.record.serial));
}

#[tokio::test]
async fn issue_token_is_unsupported() {
    let plugin = mtls();
    let host = host();
    let provider = ProviderName::try_new(PROVIDER).expect("valid name");
    let proof = Proof::new(PROOF);
    let result = plugin
        .issue_token(
            &host,
            &TokenIssue {
                provider: &provider,
                proof: &proof,
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(PortError::Unsupported {
            operation: "issue_token",
        })
    ));
}

#[tokio::test]
async fn issue_session_is_unsupported() {
    let plugin = mtls();
    let host = host();
    let provider = ProviderName::try_new(PROVIDER).expect("valid name");
    let proof = Proof::new(PROOF);
    let result = plugin
        .issue_session(
            &host,
            &TokenIssue {
                provider: &provider,
                proof: &proof,
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(PortError::Unsupported {
            operation: "issue_session",
        })
    ));
}
