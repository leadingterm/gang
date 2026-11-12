//! The wire format of Bazel's credential helper protocol: the request Bazel writes to the helper's
//! standard input, and the reply the helper writes to its standard output.

use gang_types::Url;
use serde::Deserialize;

use crate::client::BearerToken;

/// The request Bazel sends to a credential helper, read from its standard input.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CredentialRequest {
    /// The URI of the remote cache or executor the credential is for.
    pub uri: Url,
}

/// The helper's reply for `token`: the authorization header and the time Bazel should stop using
/// it. Bazel expects exactly this shape on standard output.
#[must_use]
pub fn credential_reply(token: &BearerToken) -> String {
    serde_json::json!({
        "headers": {"authorization": [format!("Bearer {}", token.value.expose())]},
        "expires": token.expires_at.to_rfc3339_seconds(),
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use gang_clock::Timestamp;
    use gang_types::{AccessToken, Url};

    use super::{CredentialRequest, credential_reply};
    use crate::client::BearerToken;

    #[test]
    fn parses_bazel_request() {
        let request: CredentialRequest =
            serde_json::from_str(r#"{"uri":"https://cache.example.com/"}"#)
                .expect("request parses");

        assert_eq!(
            request,
            CredentialRequest {
                uri: Url::parse("https://cache.example.com/").expect("url is valid"),
            }
        );
    }

    #[test]
    fn rejects_non_json_request() {
        let result = serde_json::from_str::<CredentialRequest>("uri=https://cache.example.com/");

        assert!(result.is_err());
    }

    #[test]
    fn reply_has_bazel_shape() {
        let token = BearerToken {
            value: AccessToken::new("gang-token"),
            expires_at: Timestamp::from_unix_millis(1_793_966_400_000),
        };

        let reply = credential_reply(&token);

        let parsed: serde_json::Value = serde_json::from_str(&reply).expect("reply is JSON");

        assert_eq!(
            parsed,
            serde_json::json!({
                "headers": {"authorization": ["Bearer gang-token"]},
                "expires": "2026-11-06T12:00:00Z",
            })
        );
    }

    #[test]
    fn reply_is_valid_json() {
        let token = BearerToken {
            value: AccessToken::new("a\"b"),
            expires_at: Timestamp::UNIX_EPOCH,
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&credential_reply(&token)).expect("reply is JSON");

        assert_eq!(parsed["headers"]["authorization"][0], "Bearer a\"b");
    }

    #[test]
    fn reply_expiry_is_whole_seconds() {
        let token = BearerToken {
            value: AccessToken::new("gang-token"),
            expires_at: Timestamp::from_unix_millis(1_700_000_000_000),
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&credential_reply(&token)).expect("reply is JSON");

        assert_eq!(parsed["expires"], "2023-11-14T22:13:20Z");
    }
}
