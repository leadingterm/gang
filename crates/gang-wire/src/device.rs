//! Device authorization resources for headless login.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_types::{DeviceSecret, ProviderName, Url};
use serde::{Deserialize, Serialize};

/// The characters of a user code: consonants without Y, so no code spells a word and none is
/// mistaken for a digit or another letter.
pub const USER_CODE_ALPHABET: &str = "BCDFGHJKLMNPQRSTVWXZ";
/// Characters in a user code, displayed as two halves joined by `-`.
pub const USER_CODE_LENGTH: usize = 8;

impl crate::Known for DeviceSecret {
    fn is_known(&self) -> bool {
        true
    }
}

/// Body of `POST /ui/device`: the code the user typed.
#[derive(gang_macros::Known, Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApproveDeviceAuthorizationRequest {
    /// The user code, `XXXX-XXXX`; case, spaces and the hyphen are ignored.
    pub user_code: String,
}

/// Answer of `POST /ui/device`: where the browser signs in to approve. The page navigates there
/// itself because `form-action 'self'` also covers a form submission's redirects.
#[derive(gang_macros::Known, Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ApproveDeviceAuthorizationResponse {
    /// The identity provider's authorization URL for this approval.
    #[ts(type = "string")]
    pub authorization_url: Url,
}

/// `deviceAuthorizations/{userCode}`: a pending headless login (ADR-37).
#[derive(gang_macros::Known, Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct DeviceAuthorization {
    /// Resource name containing the user code.
    pub name: String,
    /// Identity provider used for approval.
    #[ts(type = "string")]
    pub provider: ProviderName,
    /// Dashboard page where the user enters the code.
    #[ts(type = "string")]
    pub verification_uri: Url,
    /// Server-observed address of the creator, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_address: Option<String>,
    /// Current approval state.
    pub state: DeviceAuthorizationState,
    /// Time the authorization was created.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub create_time: Timestamp,
    /// Time the authorization expires.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub expire_time: Timestamp,
    /// Minimum interval between redemption polls.
    #[serde(with = "crate::duration")]
    #[ts(type = "string")]
    pub poll_interval: Duration,
    /// Returned only by the create; required to redeem.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(skip)]
    pub secret: Option<DeviceSecret>,
}

/// Pending until a browser approves, then approved until redeemed.
#[derive(
    gang_macros::Known, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeviceAuthorizationState {
    /// Awaiting browser approval.
    Pending,
    /// Approved and ready to redeem.
    Approved,
}

/// Body of `:redeem`.
#[derive(gang_macros::Known, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RedeemDeviceAuthorizationRequest {
    /// Secret returned by the create operation.
    pub secret: DeviceSecret,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn device_authorization_serializes_camel_case_and_omits_the_secret_when_absent() {
        let value = json!({
            "name": "deviceAuthorizations/BCDF-GHJK",
            "provider": "okta",
            "verificationUri": "https://example.com/ui/device",
            "clientAddress": "192.0.2.1",
            "state": "PENDING",
            "createTime": "2026-01-15T10:00:00.000Z",
            "expireTime": "2026-01-15T10:10:00.000Z",
            "pollInterval": "5s"
        });
        let mut authorization: DeviceAuthorization =
            serde_json::from_value(value.clone()).expect("authorization parses");
        assert!(authorization.secret.is_none());
        assert_eq!(
            serde_json::to_value(&authorization).expect("serializes"),
            value
        );
        authorization.secret = Some(DeviceSecret::new("secret"));
        authorization.state = DeviceAuthorizationState::Approved;
        let serialized = serde_json::to_value(authorization).expect("serializes");
        assert_eq!(serialized["secret"], "secret");
        assert_eq!(serialized["state"], "APPROVED");
    }
}
