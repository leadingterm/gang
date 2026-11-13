//! The server resource a client stores at `gang login --server`.

use gang_config::EndpointTrust;
use gang_spi::{LoginPort, Scheme};
use gang_types::{CertificatePem, Isa, ProviderName};
use gang_wire::name::SERVER;
use gang_wire::{Authentication, DeviceLogin, NoAuthentication, Server, TokenAuthentication};

use crate::endpoint::PublicEndpoint;
use crate::errors::ServerError;
use crate::identity::Providers;

/// Builds the server resource from the endpoint, the certificate authority, the clients
/// authentication scheme, and the identity providers. The CA is advertised only when clients see
/// Gang's own certificate.
pub(crate) fn wire_server<V: LoginPort>(
    endpoint: &PublicEndpoint,
    default_isa: Isa,
    endpoint_trust: EndpointTrust,
    ca: Option<CertificatePem>,
    scheme: Scheme,
    providers: &Providers<V>,
) -> Result<Server<serde_json::Value>, ServerError> {
    let authentication = match scheme {
        Scheme::Token => Authentication::Token(TokenAuthentication {
            login: providers
                .iter()
                .filter_map(|(name, provider)| {
                    provider
                        .login()
                        .map(|section| (name.clone(), section.clone()))
                })
                .collect(),
            device_login: None,
        }),
        Scheme::None => Authentication::None(NoAuthentication {}),
        Scheme::Mtls => return Err(ServerError::ClientScheme),
    };
    Ok(Server {
        name: SERVER.to_owned(),
        remote_executor: endpoint.remote().clone(),
        remote_cache: endpoint.remote().clone(),
        ca_certificate: match endpoint_trust {
            EndpointTrust::Ca => ca,
            EndpointTrust::System => None,
        },
        default_isa: default_isa.into(),
        authentication,
    })
}

/// Advertises device login on token authentication when its routes are mounted.
pub(crate) fn advertise_device_login(
    server: &mut Server<serde_json::Value>,
    provider: &ProviderName,
) {
    if let Authentication::Token(token) = &mut server.authentication {
        token.device_login = Some(DeviceLogin {
            provider: provider.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use gang_config::EndpointTrust;
    use gang_spi::{LoginSupport, Scheme};
    use gang_types::{CertificatePem, Isa, ProviderName, Url};
    use gang_wire::{Authentication, NoAuthentication, TokenAuthentication};

    use super::wire_server;
    use crate::endpoint::PublicEndpoint;
    use crate::errors::ServerError;
    use crate::identity::{Provider, Providers};
    use crate::test_support::FakeVerifier;

    fn name(value: &str) -> ProviderName {
        ProviderName::try_new(value).expect("provider name is valid")
    }

    fn providers() -> Providers<FakeVerifier> {
        let interactive = FakeVerifier::new(LoginSupport::Interactive);
        let headless = FakeVerifier::new(LoginSupport::Unsupported);
        let mut providers = BTreeMap::new();
        providers.insert(
            name("interactive"),
            Provider::new(
                interactive,
                serde_json::json!({ "issuer": "https://idp.example.test" }),
            ),
        );
        providers.insert(
            name("headless"),
            Provider::new(
                headless,
                serde_json::json!({ "issuer": "https://other.example.test" }),
            ),
        );
        providers
    }

    fn endpoint(value: &str) -> PublicEndpoint {
        let url = Url::parse(value).expect("endpoint is a valid url");
        PublicEndpoint::try_new(url).expect("endpoint scheme is accepted")
    }

    fn ca() -> CertificatePem {
        CertificatePem::try_new("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----")
            .expect("certificate block is valid")
    }

    #[test]
    fn token_scheme_lists_only_interactive_providers() {
        let info = wire_server(
            &endpoint("https://gang.example.test:443/"),
            Isa::X86_64,
            EndpointTrust::Ca,
            None,
            Scheme::Token,
            &providers(),
        )
        .expect("server info builds");

        let Authentication::Token(TokenAuthentication { login, .. }) = info.authentication else {
            panic!("token scheme must advertise token authentication");
        };
        assert_eq!(login.len(), 1);
        assert_eq!(
            login.get(&name("interactive")),
            Some(&serde_json::json!({ "issuer": "https://idp.example.test" }))
        );
    }

    #[test]
    fn none_scheme_advertises_no_authentication() {
        let info = wire_server(
            &endpoint("https://gang.example.test/"),
            Isa::Aarch64,
            EndpointTrust::Ca,
            None,
            Scheme::None,
            &providers(),
        )
        .expect("server info builds");

        assert_eq!(
            info.authentication,
            Authentication::None(NoAuthentication {})
        );
    }

    #[test]
    fn mtls_scheme_is_refused_for_clients() {
        let result = wire_server(
            &endpoint("https://gang.example.test/"),
            Isa::X86_64,
            EndpointTrust::Ca,
            None,
            Scheme::Mtls,
            &providers(),
        );

        assert!(matches!(result, Err(ServerError::ClientScheme)));
    }

    #[test]
    fn remote_executor_and_cache_are_the_endpoint_remote_url() {
        let secure = wire_server(
            &endpoint("https://gang.example.test:8443/base"),
            Isa::X86_64,
            EndpointTrust::Ca,
            None,
            Scheme::None,
            &providers(),
        )
        .expect("server info builds");
        assert_eq!(
            secure.remote_executor.as_str(),
            "grpcs://gang.example.test:8443/base"
        );
        assert_eq!(secure.remote_cache, secure.remote_executor);
    }

    #[test]
    fn certificate_authority_is_carried_when_present() {
        let with_ca = wire_server(
            &endpoint("https://gang.example.test/"),
            Isa::X86_64,
            EndpointTrust::Ca,
            Some(ca()),
            Scheme::None,
            &providers(),
        )
        .expect("server info builds");
        assert_eq!(with_ca.ca_certificate, Some(ca()));

        let without_ca = wire_server(
            &endpoint("https://gang.example.test/"),
            Isa::X86_64,
            EndpointTrust::Ca,
            None,
            Scheme::None,
            &providers(),
        )
        .expect("server info builds");
        assert_eq!(without_ca.ca_certificate, None);
    }

    #[test]
    fn a_publicly_trusted_endpoint_advertises_no_ca() {
        let info = wire_server(
            &endpoint("https://gang.example.test/"),
            Isa::X86_64,
            EndpointTrust::System,
            Some(ca()),
            Scheme::None,
            &providers(),
        )
        .expect("server info builds");
        assert_eq!(info.ca_certificate, None);
    }
}
