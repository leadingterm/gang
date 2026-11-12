//! Capability conformance: for each plugin, every credential operation its [`Capabilities`] do not
//! offer answers `Unsupported` naming that operation, and every operation they offer does not.

use std::path::Path;

use gang_authn::{Anonymous, Mtls, MtlsConfig, NoneConfig, Token, TokenConfig};
use gang_clock::Timestamp;
use gang_spi::{
    AuthenticationPort, CertificateIssue, CertificateTarget, CredentialLifecyclePort, Plugin,
    PluginContext, PortError, TokenIssue,
};
use gang_types::{CertificateRequestPem, Proof, ProviderName};

use crate::support::{TestHost, identity};

const START_MS: u64 = 1_800_000_000_000;
const PRINCIPAL: &str = "alice@example.com";
const PROOF: &str = "alice-proof";
const CSR: &str = "-----BEGIN CERTIFICATE REQUEST-----\nMIIB\n-----END CERTIFICATE REQUEST-----\n";

fn host(provider: &str) -> TestHost {
    let host = TestHost::new(Timestamp::from_unix_millis(START_MS));
    host.accept_proof(provider, PROOF, PRINCIPAL);
    host
}

fn token() -> Token {
    let config: TokenConfig = serde_json::from_str(r#"{"providers": ["example-idp"]}"#)
        .expect("the configuration parses");
    Token::build(
        config,
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )
    .expect("the configuration builds")
}

fn mtls() -> Mtls {
    let config: MtlsConfig =
        serde_json::from_str(r#"{"provider":"corp"}"#).expect("the configuration parses");
    Mtls::build(
        config,
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )
    .expect("the configuration builds")
}

fn none() -> Anonymous {
    let config: NoneConfig = serde_json::from_str("{}").expect("an empty section is valid");
    Anonymous::build(
        config,
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )
    .expect("the none plugin builds from an empty section")
}

/// Checks one outcome against whether the capability that backs its operation is offered.
fn expect_offered(offered: bool, operation: &'static str, outcome: Result<(), PortError>) {
    match (offered, outcome) {
        (
            true,
            Err(PortError::Unsupported {
                operation: reported,
            }),
        ) => {
            panic!("{reported} is unsupported although its capability is offered")
        }
        (true, _) => {}
        (
            false,
            Err(PortError::Unsupported {
                operation: reported,
            }),
        ) => {
            assert_eq!(reported, operation);
        }
        (false, outcome) => {
            panic!("{operation} has no capability offered but returned {outcome:?}")
        }
    }
}

async fn check_plugin<P>(plugin: &P, provider: &str)
where
    P: AuthenticationPort + CredentialLifecyclePort,
{
    let host = host(provider);
    let capabilities = plugin.capabilities();
    let provider_name = ProviderName::try_new(provider).expect("test provider is a valid name");
    let proof = Proof::new(PROOF);
    let csr = CertificateRequestPem::try_new(CSR).expect("the test request is a PEM block");
    let alice = identity(provider, PRINCIPAL);

    let certificate = plugin
        .issue_certificate(
            &host,
            &CertificateIssue {
                provider: &provider_name,
                proof: &proof,
                csr: &csr,
            },
        )
        .await
        .map(|_| ());
    expect_offered(capabilities.certificates, "issue_certificate", certificate);

    let token = plugin
        .issue_token(
            &host,
            &TokenIssue {
                provider: &provider_name,
                proof: &proof,
            },
        )
        .await
        .map(|_| ());
    expect_offered(capabilities.tokens, "issue_token", token);

    let session = plugin
        .issue_session(
            &host,
            &TokenIssue {
                provider: &provider_name,
                proof: &proof,
            },
        )
        .await
        .map(|_| ());
    expect_offered(capabilities.sessions, "issue_session", session);

    let revoked = plugin
        .revoke_certificates(&host, &CertificateTarget::Identity(&alice))
        .await
        .map(|_| ());
    expect_offered(capabilities.certificates, "revoke_certificates", revoked);

    let serials = plugin.revoked_serials(&host).await.map(|_| ());
    expect_offered(capabilities.certificates, "revoked_serials", serials);
}

#[tokio::test]
async fn token_operations_match_capabilities() {
    check_plugin(&token(), "example-idp").await;
}

#[tokio::test]
async fn mtls_operations_match_capabilities() {
    check_plugin(&mtls(), "corp").await;
}

#[tokio::test]
async fn none_operations_match_capabilities() {
    check_plugin(&none(), "example-idp").await;
}
