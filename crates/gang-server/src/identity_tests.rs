//! Provider construction: the plugin section is kept only for interactive providers.

use crate::Provider;
use gang_spi::LoginSupport;
use serde_json::json;

use crate::test_support::FakeVerifier;

#[test]
fn interactive_provider_exposes_its_section() {
    let section = json!({ "issuer": "https://idp.example.test", "client_id": "gang-cli" });
    let provider = Provider::new(
        FakeVerifier::new(LoginSupport::Interactive),
        section.clone(),
    );

    assert_eq!(provider.login(), Some(&section));
}

#[test]
fn unsupported_provider_hides_its_section() {
    let provider = Provider::new(
        FakeVerifier::new(LoginSupport::Unsupported),
        json!({ "issuer": "https://idp.example.test" }),
    );

    assert_eq!(provider.login(), None);
}
