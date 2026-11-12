//! Workload identity: the instance obtains a short-lived web identity token from STS and presents
//! it as its proof, so no secret is stored on the worker.
//!
//! STS must be reached on a regional endpoint (the SDK config's region). Outbound identity
//! federation must be enabled once per account (`iam:EnableOutboundWebIdentityFederation`), and the
//! instance role needs `sts:GetWebIdentityToken`. The token is fetched fresh on every call and is
//! never cached.

use aws_config::SdkConfig;
use aws_sdk_sts::Client;
use gang_types::{Audience, Proof};

use crate::config::{AwsConfig, WorkloadTokenAlgorithm};
use crate::errors::{AwsError, sdk_error};

const OPERATION: &str = "GetWebIdentityToken";

/// Issues workload web identity tokens through STS.
pub(crate) struct WebIdentity {
    sts: Client,
    audience: Audience,
    algorithm: WorkloadTokenAlgorithm,
    duration_seconds: i32,
}

impl WebIdentity {
    /// Builds an issuer for the audience, algorithm and lifetime in `config`, using `sdk`'s region
    /// and credentials.
    pub(crate) fn new(sdk: &SdkConfig, config: &AwsConfig) -> Self {
        // The token lifetime is bounded by an hour, so this never saturates.
        let duration_seconds =
            i32::try_from(config.workload_token.ttl.as_units()).unwrap_or(i32::MAX);
        Self {
            sts: Client::new(sdk),
            audience: config.workload_token.audience.clone(),
            algorithm: config.workload_token_algorithm,
            duration_seconds,
        }
    }

    /// Requests a fresh web identity token for the configured audience.
    ///
    /// # Errors
    ///
    /// [`AwsError::Transient`] or [`AwsError::Refused`] when the call fails, and
    /// [`AwsError::MalformedResponse`] when the response carries no token or an empty one.
    pub(crate) async fn token(&self) -> Result<Proof, AwsError> {
        let output = self
            .sts
            .get_web_identity_token()
            .audience(self.audience.as_str())
            .signing_algorithm(self.algorithm.sts_name())
            .duration_seconds(self.duration_seconds)
            .send()
            .await
            .map_err(|e| sdk_error(OPERATION, e))?;
        let token = output
            .web_identity_token()
            .filter(|token| !token.is_empty())
            .ok_or_else(|| AwsError::malformed(OPERATION, "web identity token"))?;
        Ok(Proof::new(token))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::error::Error;

    use serde_json::json;

    use super::*;
    use crate::fake_aws::{FakeAws, FakeFailure};

    const FIRST_TOKEN: &str = "header.payload.first";
    const SECOND_TOKEN: &str = "header.payload.second";
    const AUDIENCE: &str = "example-audience";

    fn config(overrides: serde_json::Value) -> Result<AwsConfig, serde_json::Error> {
        let mut value = json!({"region": "us-east-1", "scratch_mount_point": "/mnt/scratch"});
        if let (Some(base), serde_json::Value::Object(extra)) = (value.as_object_mut(), overrides) {
            for (key, field) in extra {
                base.insert(key, field);
            }
        }
        serde_json::from_value(value)
    }

    async fn issuer(fake: &FakeAws, config: &AwsConfig) -> WebIdentity {
        WebIdentity::new(&fake.sdk_config().await, config)
    }

    fn last_request(fake: &FakeAws) -> Result<BTreeMap<String, String>, Box<dyn Error>> {
        let state = fake.state();
        let request = state
            .requests
            .iter()
            .rev()
            .find(|request| request.operation == OPERATION);
        request
            .map(|request| request.parameters.clone())
            .ok_or_else(|| "no GetWebIdentityToken request was recorded".into())
    }

    #[tokio::test]
    async fn a_fresh_token_is_requested_on_every_call() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        let webidentity = issuer(&fake, &config(json!({}))?).await;
        fake.state().web_identity_token = FIRST_TOKEN.to_owned();
        assert_eq!(webidentity.token().await?.expose(), FIRST_TOKEN);
        fake.state().web_identity_token = SECOND_TOKEN.to_owned();
        assert_eq!(webidentity.token().await?.expose(), SECOND_TOKEN);
        let requests = fake
            .state()
            .requests
            .iter()
            .filter(|request| request.operation == OPERATION)
            .count();
        assert_eq!(requests, 2);
        Ok(())
    }

    #[tokio::test]
    async fn requests_the_configured_audience_with_defaults() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        let webidentity = issuer(&fake, &config(json!({}))?).await;
        webidentity.token().await?;
        let parameters = last_request(&fake)?;
        assert_eq!(
            parameters.get("Audience.member.1").map(String::as_str),
            Some("gang")
        );
        assert_eq!(
            parameters.get("SigningAlgorithm").map(String::as_str),
            Some("RS256")
        );
        assert_eq!(
            parameters.get("DurationSeconds").map(String::as_str),
            Some("900")
        );
        Ok(())
    }

    #[tokio::test]
    async fn requests_the_configured_audience_algorithm_and_lifetime() -> Result<(), Box<dyn Error>>
    {
        let fake = FakeAws::start().await?;
        let config = config(json!({
            "workload_token": {"audience": AUDIENCE, "ttl": "30m"},
            "workload_token_algorithm": "es384",
        }))?;
        let webidentity = issuer(&fake, &config).await;
        webidentity.token().await?;
        let parameters = last_request(&fake)?;
        assert_eq!(
            parameters.get("Audience.member.1").map(String::as_str),
            Some(AUDIENCE)
        );
        assert_eq!(
            parameters.get("SigningAlgorithm").map(String::as_str),
            Some("ES384")
        );
        assert_eq!(
            parameters.get("DurationSeconds").map(String::as_str),
            Some("1800")
        );
        Ok(())
    }

    #[tokio::test]
    async fn refused_when_federation_is_disabled() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        fake.state().failures.insert(
            OPERATION,
            FakeFailure {
                status: 403,
                code: "OutboundWebIdentityFederationDisabledException",
                message: "outbound identity federation is disabled",
            },
        );
        let webidentity = issuer(&fake, &config(json!({}))?).await;
        let Err(error) = webidentity.token().await else {
            return Err("expected the token request to fail".into());
        };
        assert!(matches!(
            error,
            AwsError::Refused {
                operation: OPERATION,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn throttling_is_transient() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        fake.state().failures.insert(
            OPERATION,
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "rate exceeded",
            },
        );
        let webidentity = issuer(&fake, &config(json!({}))?).await;
        let Err(error) = webidentity.token().await else {
            return Err("expected the token request to fail".into());
        };
        assert!(matches!(
            error,
            AwsError::Transient {
                operation: OPERATION,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn empty_token_is_malformed() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        fake.state().web_identity_token = String::new();
        let webidentity = issuer(&fake, &config(json!({}))?).await;
        assert!(matches!(
            webidentity.token().await,
            Err(AwsError::MalformedResponse {
                operation: OPERATION,
                ..
            })
        ));
        Ok(())
    }
}
