//! The caller's `session` singleton and the bearer-credential exchange that creates one.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_types::{AccessToken, Proof, ProviderName, RefreshToken};
use serde::{Deserialize, Serialize};

use crate::Permission;

/// Name of the session cookie on HTTPS.
const SECURE_SESSION: &str = "__Host-gang_session";
/// Name of the sign-in cookie on HTTPS.
const SECURE_LOGIN: &str = "__Host-gang_login";
/// Name of the renewal cookie on HTTPS.
const SECURE_RENEWAL: &str = "__Host-gang_renewal";
/// Name of the session cookie on HTTP.
const PLAIN_SESSION: &str = "gang_session";
/// Name of the sign-in cookie on HTTP.
const PLAIN_LOGIN: &str = "gang_login";
/// Name of the renewal cookie on HTTP.
const PLAIN_RENEWAL: &str = "gang_renewal";

/// The names of the browser cookies of a session on one endpoint. On `https` they carry the
/// `__Host-` prefix, which binds them to the host and the path `/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CookieNames {
    /// The session cookie.
    pub session: &'static str,
    /// The sign-in cookie, alive between the login redirect and the callback.
    pub login: &'static str,
    /// The renewal cookie: the sealed refresh token of a browser sign-in (ADR-39).
    pub renewal: &'static str,
}

impl CookieNames {
    /// The names on an endpoint served over `https` when `secure`, over plain `http` otherwise.
    #[must_use]
    pub const fn for_https(secure: bool) -> Self {
        if secure {
            Self {
                session: SECURE_SESSION,
                login: SECURE_LOGIN,
                renewal: SECURE_RENEWAL,
            }
        } else {
            Self {
                session: PLAIN_SESSION,
                login: PLAIN_LOGIN,
                renewal: PLAIN_RENEWAL,
            }
        }
    }
}

/// What `POST /v1/session:authenticate` exchanges: exactly one of `proof` and `refreshToken`.
#[derive(gang_macros::Known, Debug, Clone)]
pub enum Credential {
    /// A proof of identity issued by the provider.
    Proof(Proof),
    /// A refresh token of the device-login provider's web client (ADR-37); the server renews it.
    RefreshToken(RefreshToken),
}

/// Body of `POST /v1/session:authenticate`.
#[derive(gang_macros::Known, Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "RawAuthenticateRequest", into = "RawAuthenticateRequest")]
pub struct AuthenticateRequest {
    /// Identity provider that vouches for the caller.
    pub provider: ProviderName,
    /// The credential exchanged for a bearer token.
    pub credential: Credential,
}

/// The wire representation before enforcing the credential oneof.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawAuthenticateRequest {
    /// Identity provider that vouches for the caller.
    provider: ProviderName,
    /// Provider-issued proof, when using proof authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proof: Option<Proof>,
    /// Provider refresh token, when renewing a device login.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<RefreshToken>,
}

impl TryFrom<RawAuthenticateRequest> for AuthenticateRequest {
    type Error = &'static str;

    fn try_from(raw: RawAuthenticateRequest) -> Result<Self, Self::Error> {
        let credential = match (raw.proof, raw.refresh_token) {
            (Some(proof), None) => Credential::Proof(proof),
            (None, Some(token)) => Credential::RefreshToken(token),
            _ => return Err("exactly one of proof and refreshToken"),
        };
        Ok(Self {
            provider: raw.provider,
            credential,
        })
    }
}

impl From<AuthenticateRequest> for RawAuthenticateRequest {
    fn from(request: AuthenticateRequest) -> Self {
        let (proof, refresh_token) = match request.credential {
            Credential::Proof(proof) => (Some(proof), None),
            Credential::RefreshToken(token) => (None, Some(token)),
        };
        Self {
            provider: request.provider,
            proof,
            refresh_token,
        }
    }
}

impl crate::Known for RefreshToken {
    fn is_known(&self) -> bool {
        true
    }
}

/// Body of a successful `POST /v1/session:authenticate`.
#[derive(gang_macros::Known, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticateResponse {
    /// Gang-signed ES256 JWT to present as a bearer token.
    pub token: AccessToken,
    /// Lifetime of the token from issue. It is relative, so client clock skew does not matter.
    #[serde(with = "crate::duration")]
    pub lifetime: Duration,
    /// The provider's rotated refresh token, after a `refreshToken` exchange that rotated it;
    /// the caller stores it before the token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<RefreshToken>,
}

/// The caller's session, the body of `GET /v1/session`.
///
/// A session always exists: an anonymous caller's session simply has no `principal`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Session {
    /// Resource name, always `session`.
    pub name: String,
    /// Reference `principals/{id}` of the signed-in identity; absent when the caller is not
    /// signed in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// The permissions the caller holds.
    pub permissions: Vec<Permission>,
    /// When the session expires; absent when it does not.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::time::option"
    )]
    #[ts(type = "Timestamp")]
    pub expire_time: Option<Timestamp>,
    /// Present exactly when the server offers browser sign-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login: Option<SessionLogin>,
}

/// The identity provider that a browser sign-in uses.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct SessionLogin {
    /// The identity provider's name.
    #[ts(type = "string")]
    pub provider: ProviderName,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn cookie_names_follow_endpoint_security() {
        let secure = CookieNames::for_https(true);
        assert_eq!(secure.session, "__Host-gang_session");
        assert_eq!(secure.login, "__Host-gang_login");
        assert_eq!(secure.renewal, "__Host-gang_renewal");
        let plain = CookieNames::for_https(false);
        assert_eq!(plain.session, "gang_session");
        assert_eq!(plain.login, "gang_login");
        assert_eq!(plain.renewal, "gang_renewal");
    }

    #[test]
    fn authenticate_request_takes_exactly_one_credential() {
        for (field, value) in [
            ("proof", "proof-secret"),
            ("refreshToken", "refresh-secret"),
        ] {
            let json = json!({"provider": "okta", (field): value});
            let request: AuthenticateRequest =
                serde_json::from_value(json.clone()).expect("request parses");
            match (&request.credential, field) {
                (Credential::Proof(secret), "proof") => assert_eq!(secret.expose(), value),
                (Credential::RefreshToken(secret), "refreshToken") => {
                    assert_eq!(secret.expose(), value);
                }
                _ => panic!("wrong credential variant"),
            }
            assert_eq!(serde_json::to_value(request).expect("serializes"), json);
        }
        for json in [
            json!({"provider": "okta"}),
            json!({"provider": "okta", "proof": "p", "refreshToken": "r"}),
            json!({"provider": "okta", "proof": "p", "unknown": true}),
        ] {
            assert!(serde_json::from_value::<AuthenticateRequest>(json).is_err());
        }
    }

    #[test]
    fn authenticate_response_omits_an_absent_refresh_token() {
        let value = json!({"token": "token", "lifetime": "60s"});
        let response: AuthenticateResponse =
            serde_json::from_value(value.clone()).expect("response parses");
        assert!(response.refresh_token.is_none());
        assert_eq!(serde_json::to_value(response).expect("serializes"), value);
    }

    #[test]
    fn authenticate_request_debug_redacts_proof() {
        let request: AuthenticateRequest =
            serde_json::from_value(json!({"provider": "okta", "proof": "tok-123"}))
                .expect("request parses");
        assert!(!format!("{request:?}").contains("tok-123"));
    }

    #[test]
    fn authenticate_response_uses_a_google_duration() {
        let value = json!({"token": "eyJhbGciOiJFUzI1NiJ9.e30.c2ln", "lifetime": "600s"});
        let response: AuthenticateResponse =
            serde_json::from_value(value.clone()).expect("response parses");
        assert_eq!(response.lifetime, Duration::from_mins(10));
        assert_eq!(
            serde_json::to_value(&response).expect("response serializes"),
            value
        );
        assert!(!format!("{response:?}").contains("eyJhbGciOiJFUzI1NiJ9"));
    }

    #[test]
    fn authenticate_response_ignores_unknown_fields_and_rejects_config_durations() {
        let tolerated = json!({"token": "a.b.c", "lifetime": "1.500s", "future": 1});
        let response: AuthenticateResponse =
            serde_json::from_value(tolerated).expect("response parses");
        assert_eq!(response.lifetime, Duration::from_millis(1500));
        assert!(
            serde_json::from_value::<AuthenticateResponse>(
                json!({"token": "a.b.c", "lifetime": "10m"})
            )
            .is_err()
        );
    }

    #[test]
    fn signed_in_session_round_trips_exactly() {
        let value = json!({
            "name": "session",
            "principal": "principals/google%3Aops%40example.org",
            "permissions": ["state.read", "events.read"],
            "expireTime": "2026-01-14T09:30:00.000Z",
            "login": {"provider": "google"},
        });
        let session: Session = serde_json::from_value(value.clone()).expect("session parses");
        assert_eq!(
            session.permissions,
            [Permission::StateRead, Permission::EventsRead]
        );
        assert_eq!(
            serde_json::to_value(&session).expect("session serializes"),
            value
        );
    }

    #[test]
    fn anonymous_session_omits_absent_members_and_keeps_empty_permissions() {
        let value = json!({"name": "session", "permissions": []});
        let session: Session = serde_json::from_value(value.clone()).expect("session parses");
        assert_eq!(session.principal, None);
        assert_eq!(session.expire_time, None);
        assert_eq!(session.login, None);
        assert_eq!(
            serde_json::to_value(&session).expect("session serializes"),
            value
        );
    }

    #[test]
    fn session_ignores_unknown_fields() {
        let value = json!({"name": "session", "permissions": [], "future": {"x": 1}});
        assert!(serde_json::from_value::<Session>(value).is_ok());
    }

    #[test]
    fn unknown_permission_is_kept() {
        let value = json!({"name": "session", "permissions": ["state.read", "future.permission"]});
        let session: Session = serde_json::from_value(value.clone()).expect("session parses");
        assert_eq!(
            serde_json::to_value(&session).expect("session serializes"),
            value
        );
    }
}
