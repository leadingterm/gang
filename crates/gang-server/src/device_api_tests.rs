//! Device authorization and refresh-token exchanges through the API.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use gang_config::EndpointTrust;
use gang_spi::{ApiService, LoginGrant, Peer, RequestGate, Scheme};
use gang_tls::RevocationSet;
use gang_types::{Principal, Proof, RefreshToken};
use gang_wire::{AuthenticateResponse, ErrorReason};
use http::header::{DATE, RETRY_AFTER};
use serde_json::{Value, json};

use crate::api::{Api, ApiParts};
use crate::authorizer::Authorizer;
use crate::client_address::ClientAddress;
use crate::device_logins::Approval;
use crate::gate::Caller;
use crate::info::{advertise_device_login, wire_server};
use crate::rate_limit::RateLimiter;
use crate::renewal_seal::RenewalSeal;
use crate::revocation::Revocations;
use crate::test_support::{ApiReply, api_request, identity, read, request_parts, tls_settings};
use crate::test_support_deployment::TlsDeployment;
use crate::test_support_session_harness::{Harness, HarnessOptions};

/// An API and browser routes sharing the same device authorizations.
struct Fixture {
    harness: Harness<TlsDeployment>,
    api: Api<TlsDeployment>,
}

impl Fixture {
    /// Builds a token API with the requested browser and device mounting.
    fn new(login: bool, device: bool) -> Self {
        let harness = Harness::token(HarnessOptions {
            login,
            device,
            ..HarnessOptions::default()
        });
        let settings = tls_settings();
        let mut info = wire_server(
            &harness.core.host.endpoint,
            settings.default_isa,
            EndpointTrust::System,
            None,
            Scheme::Token,
            &harness.core.host.providers,
        )
        .expect("server info builds");
        let mounted = harness.routes.device();
        if let Some(device) = &mounted {
            advertise_device_login(&mut info, &device.provider);
        }
        let api = Api::new(ApiParts {
            readiness: crate::health::Readiness::for_test(),
            core: Arc::clone(&harness.core),
            revocations: Revocations::new(
                Arc::clone(&harness.core),
                RevocationSet::from_serials(Vec::new()),
            ),
            info,
            authorizer: Authorizer::new(settings.roles),
            limits: settings.limits,
            limiter: Arc::new(RateLimiter::new(
                &settings.rate_limit,
                Arc::clone(&harness.clock),
            )),
            dashboard: None,
            builds: crate::builds::BuildsCell::new(&gang_config::BuildsConfig::default()),
            login_provider: harness.routes.login_provider().cloned(),
            device: mounted,
        });
        Self { harness, api }
    }

    /// Sends an anonymous JSON request to the API.
    async fn send(&self, method: &str, path: &str, body: Option<Value>) -> ApiReply {
        read(self.api.call(api_request(method, path, None, body)).await).await
    }

    /// Starts an authorization and returns its JSON representation.
    async fn create(&self) -> Value {
        self.send("POST", "/v1/deviceAuthorizations", None)
            .await
            .value()
    }

    /// Approves an existing authorization using the configured Alice proof.
    async fn approve(&self, authorization: &Value) {
        let code = authorization["name"]
            .as_str()
            .expect("name")
            .strip_prefix("deviceAuthorizations/")
            .expect("collection");
        let device = self.harness.routes.device().expect("device is mounted");
        let repository = &self.harness.core.host.repository;
        let seal = RenewalSeal::new(
            &device.client.secret,
            self.harness.core.host.endpoint.origin(),
        );
        let code = device
            .logins
            .begin_approval(repository, code)
            .await
            .expect("storage")
            .expect("pending code");
        assert!(
            device
                .logins
                .approve(
                    repository,
                    &seal,
                    &code,
                    Approval {
                        code: code.code.clone(),
                        grant: LoginGrant {
                            proof: Proof::new("alice-proof"),
                            refresh_token: Some(RefreshToken::new("refresh")),
                            principal: Principal::try_new("alice").expect("valid principal"),
                        },
                    }
                )
                .await
                .expect("storage")
        );
    }

    /// Redeems the authorization with a chosen secret.
    async fn redeem(&self, authorization: &Value, secret: &str) -> ApiReply {
        let name = authorization["name"].as_str().expect("name");
        self.send(
            "POST",
            &format!("/v1/{name}:redeem"),
            Some(json!({"secret": secret})),
        )
        .await
    }
}

/// Extracts the secret returned only at creation.
fn secret(authorization: &Value) -> &str {
    authorization["secret"].as_str().expect("secret")
}

#[tokio::test]
async fn server_info_announces_device_login_only_when_mounted() {
    for (login, device, expected) in [
        (true, true, true),
        (true, false, false),
        (false, true, false),
    ] {
        let fixture = Fixture::new(login, device);
        let info = fixture.send("GET", "/v1/server", None).await.value();
        let advertised = info["authentication"]["token"].get("deviceLogin");
        assert_eq!(advertised.is_some(), expected);
        if expected {
            assert_eq!(advertised.expect("device login")["provider"], "example-idp");
        }
    }
}

#[tokio::test]
async fn create_returns_a_code_secret_and_the_verification_uri() {
    let fixture = Fixture::new(true, true);
    let mut request = api_request("POST", "/v1/deviceAuthorizations", None, None);
    request
        .extensions_mut()
        .insert(ClientAddress("192.0.2.4".parse().expect("client address")));
    let authorization = read(fixture.api.call(request).await).await.value();
    let code = authorization["name"]
        .as_str()
        .expect("name")
        .strip_prefix("deviceAuthorizations/")
        .expect("collection");
    assert_eq!(code.len(), 9);
    assert_eq!(code.as_bytes()[4], b'-');
    assert!(
        code.chars()
            .filter(|ch| *ch != '-')
            .all(|ch| "BCDFGHJKLMNPQRSTVWXZ".contains(ch))
    );
    assert_eq!(secret(&authorization).len(), 43);
    assert_eq!(authorization["state"], "PENDING");
    assert_eq!(authorization["pollInterval"], "5s");
    assert_eq!(authorization["clientAddress"], "192.0.2.4");
    assert_eq!(
        authorization["verificationUri"],
        "https://gang.example.test/ui/device"
    );
}

#[tokio::test]
async fn get_never_returns_the_secret() {
    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    let path = format!("/v1/{}", authorization["name"].as_str().expect("name"));
    let read = fixture.send("GET", &path, None).await.value();
    assert!(read.get("secret").is_none());
    assert_eq!(read["name"], authorization["name"]);
}

#[tokio::test]
async fn a_percent_encoded_code_with_a_space_is_found() {
    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    let name = authorization["name"].as_str().expect("name");
    let path = format!("/v1/{}", name.replace('-', "%20"));
    let reply = fixture.send("GET", &path, None).await;
    assert_eq!(reply.value()["name"], name);

    fixture.approve(&authorization).await;
    let reply = fixture
        .send(
            "POST",
            &format!("{path}:redeem"),
            Some(json!({"secret": secret(&authorization)})),
        )
        .await;
    assert_eq!(reply.status, http::StatusCode::OK);
}

#[tokio::test]
async fn an_undecodable_code_is_not_found() {
    let fixture = Fixture::new(true, true);
    for (method, path, body) in [
        ("GET", "/v1/deviceAuthorizations/%FF", None),
        (
            "POST",
            "/v1/deviceAuthorizations/%FF:redeem",
            Some(json!({"secret": "wrong"})),
        ),
    ] {
        let reply = fixture.send(method, path, body).await;
        let status = reply.refused(ErrorReason::DeviceAuthorizationNotFound);
        let info = status.error_info().expect("error info");
        assert_eq!(
            info.metadata.get("deviceAuthorization").map(String::as_str),
            Some("%FF")
        );
    }
}

#[tokio::test]
async fn redeem_pending_is_authorization_pending_then_rate_limited() {
    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    fixture
        .redeem(&authorization, secret(&authorization))
        .await
        .refused(ErrorReason::AuthorizationPending);
    let reply = fixture.redeem(&authorization, secret(&authorization)).await;
    reply.refused(ErrorReason::RateLimited);
    assert_eq!(reply.headers[RETRY_AFTER], "5");
}

#[tokio::test]
async fn redeem_not_found_answers_are_byte_identical() {
    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    let wrong = fixture.redeem(&authorization, "wrong").await;
    fixture.approve(&authorization).await;
    let success = fixture.redeem(&authorization, secret(&authorization)).await;
    assert_eq!(success.status, http::StatusCode::OK);
    let consumed = fixture.redeem(&authorization, secret(&authorization)).await;
    let expired_authorization = fixture.create().await;
    let code = authorization["name"]
        .as_str()
        .expect("name")
        .strip_prefix("deviceAuthorizations/")
        .expect("collection");
    let expired_code = expired_authorization["name"]
        .as_str()
        .expect("name")
        .strip_prefix("deviceAuthorizations/")
        .expect("collection");
    let unknown_code = ["BCDF-GHJK", "LMNP-QRST", "VWXZ-BCDF"]
        .into_iter()
        .find(|candidate| *candidate != code && *candidate != expired_code)
        .expect("a code distinct from both created codes");
    let unknown = fixture
        .send(
            "POST",
            &format!("/v1/deviceAuthorizations/{unknown_code}:redeem"),
            Some(json!({"secret":"wrong"})),
        )
        .await;
    fixture.harness.clock.advance(Duration::from_secs(601));
    let expired = fixture
        .redeem(&expired_authorization, secret(&expired_authorization))
        .await;
    for (reply, expected_code) in [
        (&wrong, code),
        (&consumed, code),
        (&unknown, unknown_code),
        (&expired, expired_code),
    ] {
        let status = reply.refused(ErrorReason::DeviceAuthorizationNotFound);
        let info = status.error_info().expect("error info");
        assert_eq!(
            info.metadata.get("deviceAuthorization").map(String::as_str),
            Some(expected_code)
        );
        assert_eq!(wrong.status, reply.status);
        let mut left = wrong.headers.clone();
        let mut right = reply.headers.clone();
        left.remove(DATE);
        right.remove(DATE);
        assert_eq!(left, right);
        assert_eq!(wrong.text(), reply.text().replace(expected_code, code));
    }
}

#[tokio::test]
async fn redeem_after_approval_issues_a_token_with_the_refresh_token() {
    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    fixture.approve(&authorization).await;
    let reply: AuthenticateResponse = fixture
        .redeem(&authorization, secret(&authorization))
        .await
        .ok();
    assert!(!reply.token.expose().is_empty());
    assert_eq!(
        reply.refresh_token.expect("refresh token").expose(),
        "refresh"
    );
}

#[tokio::test]
async fn redeem_of_a_revoked_principal_is_principal_revoked_and_consumes_the_entry() {
    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    fixture.approve(&authorization).await;
    fixture
        .harness
        .core
        .host
        .revoked_principals
        .revoke(identity("alice"));
    fixture
        .redeem(&authorization, secret(&authorization))
        .await
        .refused(ErrorReason::PrincipalRevoked);
    fixture
        .redeem(&authorization, secret(&authorization))
        .await
        .refused(ErrorReason::DeviceAuthorizationNotFound);
}

#[tokio::test]
async fn authenticate_with_a_refresh_token_renews_with_the_web_client() {
    let fixture = Fixture::new(true, true);
    fixture
        .harness
        .verifier
        .renew_to("old", "alice-proof", Some("rotated"));
    let reply: AuthenticateResponse = fixture
        .send(
            "POST",
            "/v1/session:authenticate",
            Some(json!({"provider":"example-idp", "refreshToken":"old"})),
        )
        .await
        .ok();
    assert!(!reply.token.expose().is_empty());
    assert_eq!(
        reply.refresh_token.expect("rotated token").expose(),
        "rotated"
    );
    assert_eq!(
        fixture.harness.verifier.renewals(),
        vec![("old".to_owned(), Some("web-client".to_owned()))]
    );
}

#[tokio::test]
async fn authenticate_reuses_rotated_grant_after_signer_failure() {
    let fixture = Fixture::new(true, true);
    fixture
        .harness
        .verifier
        .renew_to("old", "alice-proof", Some("rotated"));
    let outage = fixture.harness.core.host.signer.outage();
    outage.store(true, Ordering::SeqCst);
    let body = json!({"provider":"example-idp", "refreshToken":"old"});

    fixture
        .send("POST", "/v1/session:authenticate", Some(body.clone()))
        .await
        .refused(ErrorReason::DependencyUnavailable);

    outage.store(false, Ordering::SeqCst);
    fixture.harness.verifier.renew_unavailable("old");
    let reply: AuthenticateResponse = fixture
        .send("POST", "/v1/session:authenticate", Some(body))
        .await
        .ok();
    assert_eq!(
        reply.refresh_token.expect("rotated token").expose(),
        "rotated"
    );
    assert_eq!(
        fixture.harness.verifier.renewals(),
        vec![("old".to_owned(), Some("web-client".to_owned()))]
    );

    let mut parts = request_parts("/v1/server", Some(&reply.token));
    fixture
        .harness
        .gate
        .admit(
            gang_spi::RequestScope::Controller,
            &Peer::default(),
            &mut parts,
        )
        .await
        .expect("the gate admits the open API route");
    let authenticated = Caller::of(&parts.extensions)
        .as_authenticated()
        .expect("the bearer token authenticates the caller");
    assert_eq!(authenticated.identity.provider.as_str(), "example-idp");
    assert_eq!(authenticated.identity.principal.as_str(), "alice");
}

#[tokio::test]
async fn authenticate_with_a_refresh_token_for_another_provider_is_proof_rejected() {
    let fixture = Fixture::new(true, true);
    fixture
        .send(
            "POST",
            "/v1/session:authenticate",
            Some(json!({"provider":"example-workers", "refreshToken":"old"})),
        )
        .await
        .refused(ErrorReason::ProofRejected);
    assert!(fixture.harness.verifier.renewals().is_empty());
}

#[tokio::test]
async fn authenticate_with_both_or_neither_credential_is_malformed() {
    let fixture = Fixture::new(true, true);
    for body in [
        json!({"provider":"example-idp"}),
        json!({"provider":"example-idp", "proof":"alice-proof", "refreshToken":"old"}),
    ] {
        fixture
            .send("POST", "/v1/session:authenticate", Some(body))
            .await
            .refused(ErrorReason::MalformedRequest);
    }
}

#[tokio::test]
async fn device_routes_are_not_found_when_device_login_is_off() {
    let fixture = Fixture::new(true, false);
    for (method, path, body) in [
        ("POST", "/v1/deviceAuthorizations", None),
        ("GET", "/v1/deviceAuthorizations/BCDF-GHJK", None),
        (
            "POST",
            "/v1/deviceAuthorizations/BCDF-GHJK:redeem",
            Some(json!({"secret":"wrong"})),
        ),
    ] {
        fixture
            .send(method, path, body)
            .await
            .refused(ErrorReason::RouteNotFound);
    }
}

#[tokio::test]
async fn unavailable_device_records_return_service_unavailable() {
    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    fixture.harness.core.host.repository.fail_records(true);
    let name = authorization["name"].as_str().expect("name");
    assert_eq!(
        fixture
            .send("POST", "/v1/deviceAuthorizations", None)
            .await
            .status,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        fixture
            .send("GET", &format!("/v1/{name}"), None)
            .await
            .status,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        fixture
            .redeem(&authorization, secret(&authorization))
            .await
            .status,
        http::StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn a_second_core_continues_a_device_login() -> gang_testkit::TestResult {
    use crate::core::Core;
    use crate::device_logins::DeviceLogins;
    use crate::host::Host;
    use crate::principals::{PrincipalSightings, RevokedPrincipals};
    use crate::security::Security;
    use crate::settings::DeviceSettings;
    use crate::test_support_deployment::{endpoint, mtls, providers, token};
    use gang_types::{DeviceCodeTtl, DevicePollInterval, DeviceSecret};
    use std::num::NonZeroUsize;

    let fixture = Fixture::new(true, true);
    let authorization = fixture.create().await;
    let original = &fixture.harness.core;
    let core: Arc<Core<TlsDeployment>> = Core::new(
        Host {
            proxy_protocol: original.host.proxy_protocol.clone(),
            repository: original.host.repository.clone(),
            signer: Arc::clone(&original.host.signer),
            security: Security::Plaintext,
            providers: providers().0,
            clock: Arc::clone(&fixture.harness.clock),
            endpoint: endpoint(original.host.endpoint.url().as_str()),
            revoked_principals: RevokedPrincipals::new(),
            sightings: PrincipalSightings::new(
                Duration::from_mins(1),
                NonZeroUsize::new(10).expect("nonzero"),
            ),
        },
        token(),
        mtls(),
    );
    let device = fixture.harness.routes.device().expect("mounted");
    let other = DeviceLogins::new(
        &DeviceSettings {
            ttl: DeviceCodeTtl::from_units(600),
            poll_interval: DevicePollInterval::from_units(5),
        },
        Arc::clone(&core.host.clock),
    );
    let device_code = authorization["name"]
        .as_str()
        .expect("name")
        .strip_prefix("deviceAuthorizations/")
        .expect("collection");
    let secret = DeviceSecret::new(secret(&authorization));
    let seal = RenewalSeal::new(&device.client.secret, core.host.endpoint.origin());
    assert!(matches!(
        other
            .redeem(&core.host.repository, &seal, device_code, &secret)
            .await?,
        Err(crate::device_logins::Redeem::Pending)
    ));
    let user_code = other
        .begin_approval(&core.host.repository, device_code)
        .await?
        .expect("pending");
    assert!(
        other
            .approve(
                &core.host.repository,
                &seal,
                &user_code,
                Approval {
                    code: user_code.code.clone(),
                    grant: LoginGrant {
                        proof: Proof::new("alice-proof"),
                        refresh_token: Some(RefreshToken::new("refresh")),
                        principal: Principal::try_new("alice").expect("valid principal"),
                    },
                }
            )
            .await?
    );
    fixture.harness.clock.advance(Duration::from_secs(5));
    assert_eq!(
        other
            .redeem(&core.host.repository, &seal, device_code, &secret)
            .await?
            .expect("approved")
            .grant
            .principal
            .as_str(),
        "alice"
    );
    Ok(())
}
