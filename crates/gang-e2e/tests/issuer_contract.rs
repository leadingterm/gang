//! Contract between the harness issuer and the real OIDC provider.

use std::error::Error;
use std::sync::Arc;

use gang_clock::{SystemClock, TestClock, Timestamp};
use gang_e2e::{AUDIENCE, ORGANIZATION, ORGANIZATION_CLAIM, Settings, TestIssuer, TokenSpec};
use gang_oidc::{OidcConfig, Provider};
use gang_spi::{LoginPort, LoginRequest, PortError, ProofVerifierPort, Rejection};
use gang_tls::{HttpClient, HttpProtocol, HttpRequest, HttpSettings, HttpTransport};
use gang_types::{NonZeroBytes, NonZeroDuration, Proof, Url};
use http::{Method, StatusCode, header};
use serde_json::json;

/// Deadline for the authorization request.
const HTTP_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);
/// Maximum buffered issuer response.
const MAX_BODY: NonZeroBytes = NonZeroBytes::kib(64);

#[test]
fn issuer_contract_with_real_provider() -> Result<(), Box<dyn Error>> {
    const NOW_MILLIS: u64 = 1_800_000_000_000;
    let clock = Arc::new(TestClock::new(Timestamp::from_unix_millis(NOW_MILLIS)));
    // The issuer owns a runtime, so it must be dropped outside the test runtime.
    let settings = Settings::default();
    let issuer = TestIssuer::start_with_clock(&settings, clock.clone())?;
    let config: OidcConfig = serde_json::from_value(json!({
        "issuer_url": issuer.url(),
        "audience": AUDIENCE,
        "principal_claim": "sub",
        "required_claims": { ORGANIZATION_CLAIM: ORGANIZATION },
        "login": {
            "scopes": ["openid"],
            "authorization_params": { "login_hint": "login-subject" }
        }
    }))?;
    // At this earlier instant the expired token is halfway through its original lifetime.
    let earlier = Arc::new(TestClock::new(Timestamp::from_unix_millis(
        NOW_MILLIS - 3 * u64::try_from(settings.token_lifetime.as_millis())? / 2,
    )));
    let earlier_provider = Provider::new(config.clone(), earlier)?;
    let provider = Provider::new(config, clock.clone())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let admitted = issuer.mint(&TokenSpec::admitted("subject-1"))?;
        assert_eq!(
            provider.verify(&Proof::new(admitted)).await?.as_str(),
            "subject-1"
        );

        let expired = Proof::new(issuer.mint_expired(&TokenSpec::admitted("subject-1"))?);
        assert_eq!(
            earlier_provider.verify(&expired).await?.as_str(),
            "subject-1"
        );
        assert!(matches!(
            provider.verify(&expired).await,
            Err(PortError::Rejected {
                reason: Rejection::InvalidProof,
                ..
            })
        ));
        let foreign_audience = issuer.mint(&TokenSpec::foreign_audience("subject-1"))?;
        assert!(matches!(
            provider.verify(&Proof::new(foreign_audience)).await,
            Err(PortError::Rejected {
                reason: Rejection::InvalidProof,
                ..
            })
        ));
        let foreign_org = issuer.mint(&TokenSpec::foreign_organization("subject-1"))?;
        assert!(matches!(
            provider.verify(&Proof::new(foreign_org)).await,
            Err(PortError::Rejected {
                reason: Rejection::PolicyDenied,
                ..
            })
        ));
        let redirect = Url::parse("http://127.0.0.1:43111/callback")?;
        let challenge = provider
            .begin_login(&LoginRequest {
                redirect_uri: &redirect,
                client: None,
            })
            .await?;
        let client = HttpClient::try_new(&HttpSettings {
            transport: HttpTransport::Plain,
            protocol: HttpProtocol::Negotiated,
            timeout: HTTP_TIMEOUT,
            max_body: MAX_BODY,
            clock: Arc::new(SystemClock::new()),
        })?;
        let response = client
            .send(HttpRequest {
                method: Method::GET,
                url: &challenge.authorization_url,
                bearer: None,
                headers: &[],
                body: None,
            })
            .await?;
        assert_eq!(response.status, StatusCode::FOUND);
        let callback = Url::parse(
            response
                .headers
                .get(header::LOCATION)
                .ok_or("missing location")?
                .to_str()?,
        )?;
        let grant = provider
            .complete_login(challenge.session, &callback)
            .await?;
        assert_eq!(
            provider.verify(&grant.proof).await?.as_str(),
            "login-subject"
        );
        assert!(grant.refresh_token.is_some());
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}
