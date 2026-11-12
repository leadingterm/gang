//! The `principals/{id}` resource and its list and revoke answers.

use gang_clock::Timestamp;
use gang_types::ProviderName;
use serde::{Deserialize, Serialize};

use crate::Certificate;
use crate::page::PageToken;

/// A principal known to the server, named `principals/{id}`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Principal {
    /// Resource name, `principals/{id}` with the id percent-encoded.
    pub name: String,
    /// The identity provider that vouches for the principal.
    #[ts(type = "string")]
    pub provider: ProviderName,
    /// The principal as its identity provider names it.
    #[ts(type = "string")]
    pub principal: gang_types::Principal,
    /// When the server first saw the principal.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub create_time: Timestamp,
    /// When the server last saw the principal.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub last_seen_time: Timestamp,
    /// When the principal's access was revoked; absent while it is active.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::time::option"
    )]
    #[ts(type = "Timestamp")]
    pub revoke_time: Option<Timestamp>,
}

/// One page of principals, the body of `GET /v1/principals`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct ListPrincipalsResponse {
    /// The principals on this page.
    pub principals: Vec<Principal>,
    /// Token that resumes the list; absent on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<PageToken>,
}

/// Body of a successful `POST /v1/principals/{principal}:revoke`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokePrincipalResponse {
    /// The principal, now revoked.
    pub principal: Principal,
    /// The certificates the revocation revoked.
    pub certificates: Vec<Certificate>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn principal_value() -> serde_json::Value {
        json!({
            "name": "principals/google%3Aops%40example.org",
            "provider": "google",
            "principal": "ops@example.org",
            "createTime": "2026-01-02T08:00:00.000Z",
            "lastSeenTime": "2026-01-14T09:20:01.250Z",
            "revokeTime": "2026-01-14T09:30:00.000Z",
        })
    }

    #[test]
    fn revoked_principal_round_trips_exactly() {
        let value = principal_value();
        let principal: Principal = serde_json::from_value(value.clone()).expect("principal parses");
        assert!(principal.revoke_time.is_some());
        assert_eq!(
            serde_json::to_value(&principal).expect("principal serializes"),
            value
        );
    }

    #[test]
    fn active_principal_omits_the_revoke_time() {
        let mut value = principal_value();
        value
            .as_object_mut()
            .expect("principal is an object")
            .remove("revokeTime");
        let principal: Principal = serde_json::from_value(value.clone()).expect("principal parses");
        assert_eq!(principal.revoke_time, None);
        assert_eq!(
            serde_json::to_value(&principal).expect("principal serializes"),
            value
        );
    }

    #[test]
    fn principal_ignores_unknown_fields() {
        let mut value = principal_value();
        value["future"] = json!(true);
        assert!(serde_json::from_value::<Principal>(value).is_ok());
    }

    #[test]
    fn list_response_carries_the_next_page_token_only_when_present() {
        let last = json!({"principals": [principal_value()]});
        let parsed: ListPrincipalsResponse =
            serde_json::from_value(last.clone()).expect("last page parses");
        assert_eq!(parsed.next_page_token, None);
        assert_eq!(
            serde_json::to_value(&parsed).expect("last page serializes"),
            last
        );

        let token = serde_json::to_value(PageToken::new("", "cursor")).expect("token serializes");
        let more = json!({"principals": [], "nextPageToken": token});
        let parsed: ListPrincipalsResponse =
            serde_json::from_value(more.clone()).expect("page parses");
        assert!(parsed.next_page_token.is_some());
        assert_eq!(
            serde_json::to_value(&parsed).expect("page serializes"),
            more
        );
    }

    #[test]
    fn revoke_response_round_trips_exactly() {
        let value = json!({
            "principal": principal_value(),
            "certificates": [{
                "name": "certificates/3fa9",
                "principal": "principals/google%3Aops%40example.org",
                "createTime": "2026-01-14T09:00:00.000Z",
                "expireTime": "2026-01-21T09:00:00.000Z",
                "revokeTime": "2026-01-14T09:30:00.000Z",
            }],
        });
        let response: RevokePrincipalResponse =
            serde_json::from_value(value.clone()).expect("response parses");
        assert_eq!(response.certificates.len(), 1);
        assert_eq!(
            serde_json::to_value(&response).expect("response serializes"),
            value
        );
    }
}
