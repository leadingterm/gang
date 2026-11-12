//! The `none` plugin: every caller is the anonymous identity, and no credential is issued.

use std::path::Path;
use std::sync::Arc;

use gang_authn::{Anonymous, NoneConfig};
use gang_clock::Timestamp;
use gang_spi::{
    AuthenticationPort, Capabilities, CertificateIssue, CertificateTarget, ClientCertificates,
    CredentialLifecyclePort, Peer, Plugin, PluginContext, PortError, Scheme, TokenIssue,
};
use gang_types::{CertSerial, CertificateRequestPem, Proof, ProviderName};

use crate::support::{TestHost, bearer, evidence, identity};

const NOW_MS: u64 = 1_800_000_000_000;

fn plugin() -> Anonymous {
    let config: NoneConfig = serde_json::from_str("{}").expect("an empty section is valid");
    Anonymous::build(
        config,
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )
    .expect("the none plugin builds from an empty section")
}

fn host() -> TestHost {
    TestHost::new(Timestamp::from_unix_millis(NOW_MS))
}

fn anonymous() -> Arc<gang_types::Identity> {
    Arc::new(identity("none", "anonymous"))
}

#[test]
fn scheme_is_none() {
    assert_eq!(plugin().scheme(), Scheme::None);
}

#[test]
fn capabilities_offer_neither_certificates_nor_tokens() {
    assert_eq!(plugin().capabilities(), Capabilities::default());
}

#[test]
fn client_certificates_are_never_requested() {
    assert_eq!(plugin().client_certificates(), ClientCertificates::Never);
}

#[tokio::test]
async fn request_without_headers_is_the_anonymous_identity() {
    let plugin = plugin();
    let host = host();
    let peer = Peer::default();
    let headers = http::HeaderMap::new();

    let caller = plugin
        .authenticate(&host, &evidence(&peer, &headers))
        .await
        .expect("the none plugin does not fail")
        .expect("the none plugin identifies every caller")
        .identity;

    assert_eq!(*caller, identity("none", "anonymous"));
}

#[tokio::test]
async fn request_with_a_bearer_token_is_the_anonymous_identity() {
    let plugin = plugin();
    let host = host();
    let peer = Peer::default();
    let headers = bearer("not-a-gang-token");

    let caller = plugin
        .authenticate(&host, &evidence(&peer, &headers))
        .await
        .expect("the none plugin does not fail")
        .expect("the none plugin identifies every caller")
        .identity;

    assert_eq!(*caller, identity("none", "anonymous"));
}

#[tokio::test]
async fn request_with_a_peer_certificate_is_the_anonymous_identity() {
    let plugin = plugin();
    let host = host();
    let peer = Peer {
        certificate: Some(CertSerial::try_new("01").expect("test serial is valid")),
        ..Peer::default()
    };
    let headers = http::HeaderMap::new();

    let caller = plugin
        .authenticate(&host, &evidence(&peer, &headers))
        .await
        .expect("the none plugin does not fail")
        .expect("the none plugin identifies every caller")
        .identity;

    assert_eq!(*caller, identity("none", "anonymous"));
}

#[tokio::test]
async fn every_request_shares_one_anonymous_identity() {
    let plugin = plugin();
    let host = host();
    let peer = Peer::default();
    let headers = http::HeaderMap::new();

    let first = plugin
        .authenticate(&host, &evidence(&peer, &headers))
        .await
        .expect("the none plugin does not fail")
        .expect("the none plugin identifies every caller")
        .identity;
    let second = plugin
        .authenticate(&host, &evidence(&peer, &headers))
        .await
        .expect("the none plugin does not fail")
        .expect("the none plugin identifies every caller")
        .identity;

    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(first, anonymous());
}

#[tokio::test]
async fn anonymous_caller_is_never_time_limited() {
    let plugin = plugin();
    let host = host();
    let peer = Peer::default();
    let headers = http::HeaderMap::new();

    let caller = plugin
        .authenticate(&host, &evidence(&peer, &headers))
        .await
        .expect("the none plugin does not fail")
        .expect("the none plugin identifies every caller");

    assert_eq!(caller.valid_until, None);
}

#[tokio::test]
async fn issue_certificate_is_unsupported() {
    let plugin = plugin();
    let host = host();
    let provider = ProviderName::try_new("example-idp").expect("test provider is valid");
    let proof = Proof::new("proof");
    let csr = CertificateRequestPem::try_new(
        "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----",
    )
    .expect("test request is PEM");

    let error = plugin
        .issue_certificate(
            &host,
            &CertificateIssue {
                provider: &provider,
                proof: &proof,
                csr: &csr,
            },
        )
        .await
        .expect_err("the none plugin issues no certificate");

    assert!(matches!(
        error,
        PortError::Unsupported {
            operation: "issue_certificate"
        }
    ));
}

#[tokio::test]
async fn issue_token_is_unsupported() {
    let plugin = plugin();
    let host = host();
    let provider = ProviderName::try_new("example-idp").expect("test provider is valid");
    let proof = Proof::new("proof");

    let error = plugin
        .issue_token(
            &host,
            &TokenIssue {
                provider: &provider,
                proof: &proof,
            },
        )
        .await
        .expect_err("the none plugin mints no token");

    assert!(matches!(
        error,
        PortError::Unsupported {
            operation: "issue_token"
        }
    ));
}

#[tokio::test]
async fn issue_session_is_unsupported() {
    let plugin = plugin();
    let host = host();
    let provider = ProviderName::try_new("example-idp").expect("test provider is valid");
    let proof = Proof::new("proof");

    let error = plugin
        .issue_session(
            &host,
            &TokenIssue {
                provider: &provider,
                proof: &proof,
            },
        )
        .await
        .expect_err("the none plugin mints no session");

    assert!(matches!(
        error,
        PortError::Unsupported {
            operation: "issue_session"
        }
    ));
}

#[tokio::test]
async fn revoke_certificates_is_unsupported() {
    let plugin = plugin();
    let host = host();
    let serial = CertSerial::try_new("01").expect("test serial is valid");

    let error = plugin
        .revoke_certificates(&host, &CertificateTarget::Serial(&serial))
        .await
        .expect_err("the none plugin revokes no certificate");

    assert!(matches!(
        error,
        PortError::Unsupported {
            operation: "revoke_certificates"
        }
    ));
}

#[tokio::test]
async fn revoked_serials_is_unsupported() {
    let plugin = plugin();
    let host = host();

    let error = plugin
        .revoked_serials(&host)
        .await
        .expect_err("the none plugin keeps no revocation list");

    assert!(matches!(
        error,
        PortError::Unsupported {
            operation: "revoked_serials"
        }
    ));
}
