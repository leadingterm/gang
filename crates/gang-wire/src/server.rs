//! The `server` singleton: what a client learns before it authenticates.

use std::collections::BTreeMap;

use gang_types::{CertificatePem, ProviderName, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

wire_enum! {
    /// Instruction set architecture of workers, as written on the wire.
    pub enum Isa default X86_64 from gang_types::Isa {
        /// 64-bit Arm.
        Aarch64 = "aarch64",
        /// 64-bit x86.
        X86_64 = "x86-64",
    }
}

/// Server information a client stores at `gang login --server`; the body of `GET /v1/server`.
///
/// `Login` is the identity-provider plugin section the server serializes for each provider
/// that supports interactive login; the client deserializes it into its own configuration type.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    bound(
        serialize = "Login: Serialize",
        deserialize = "Login: Deserialize<'de>"
    )
)]
pub struct Server<Login> {
    /// Resource name, always `server`.
    pub name: String,
    /// `grpcs://` URL for Bazel `--remote_executor`.
    pub remote_executor: Url,
    /// `grpcs://` URL for Bazel `--remote_cache`.
    pub remote_cache: Url,
    /// CA certificate the client trusts for those endpoints; omitted when the server has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_certificate: Option<CertificatePem>,
    /// Instruction set architecture workers use when a build does not request one.
    ///
    /// The client writes it into the Bazel default execution properties as `ISA=<default_isa>`.
    pub default_isa: Isa,
    /// How the client surface authenticates its callers.
    pub authentication: Authentication<Login>,
}

/// Authentication the client surface requires: exactly one member key is present.
///
/// A member this version does not know deserializes to [`Authentication::Unrecognized`] and
/// serializes back unchanged. A known member whose body does not parse also lands there.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    bound(
        serialize = "Login: Serialize",
        deserialize = "Login: Deserialize<'de>"
    )
)]
pub enum Authentication<Login> {
    /// Gang-signed bearer tokens, obtained by presenting a proof of identity from one of the
    /// providers in `login` to `POST /v1/session:authenticate`.
    Token(TokenAuthentication<Login>),
    /// No authentication: every caller is treated as anonymous.
    None(NoAuthentication),
    /// A scheme this version does not know, kept so it round-trips.
    #[serde(untagged)]
    Unrecognized(Map<String, Value>),
}

/// Bearer-token authentication.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    bound(
        serialize = "Login: Serialize",
        deserialize = "Login: Deserialize<'de>"
    )
)]
pub struct TokenAuthentication<Login> {
    /// Providers that support interactive login, keyed by provider name. The keys are data, and
    /// the values are owned by the provider's plugin.
    pub login: BTreeMap<ProviderName, Login>,
}

/// The empty member of [`Authentication::None`]; serializes to `{}`.
#[derive(gang_macros::Known, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoAuthentication {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn server_value(authentication: &Value) -> Value {
        json!({
            "name": "server",
            "remoteExecutor": "grpcs://gang.example.com:443",
            "remoteCache": "grpcs://gang.example.com:443",
            "caCertificate": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----",
            "defaultIsa": "x86-64",
            "authentication": authentication,
        })
    }

    #[test]
    fn token_server_parses_and_round_trips_exactly() {
        let value = server_value(&json!({
            "token": {"login": {"google": {"client_id": "abc", "scopes": ["openid"]}}}
        }));
        let server: Server<Value> = serde_json::from_value(value.clone()).expect("server parses");
        let Authentication::Token(token) = &server.authentication else {
            panic!(
                "expected token authentication, got {:?}",
                server.authentication
            );
        };
        assert_eq!(token.login.len(), 1);
        assert_eq!(
            serde_json::to_value(&server).expect("server serializes"),
            value
        );
    }

    #[test]
    fn none_server_serializes_an_empty_member() {
        let value = server_value(&json!({"none": {}}));
        let server: Server<Value> = serde_json::from_value(value.clone()).expect("server parses");
        assert_eq!(
            server.authentication,
            Authentication::None(NoAuthentication {})
        );
        assert_eq!(
            serde_json::to_value(&server).expect("server serializes"),
            value
        );
    }

    #[test]
    fn absent_ca_certificate_is_omitted() {
        let mut value = server_value(&json!({"none": {}}));
        value
            .as_object_mut()
            .expect("server is an object")
            .remove("caCertificate");
        let server: Server<Value> = serde_json::from_value(value.clone()).expect("server parses");
        assert_eq!(server.ca_certificate, None);
        assert_eq!(
            serde_json::to_value(&server).expect("server serializes"),
            value
        );
    }

    #[test]
    fn unknown_authentication_member_is_kept() {
        let value = server_value(&json!({"mtls": {"realm": "x"}}));
        let server: Server<Value> = serde_json::from_value(value.clone()).expect("server parses");
        assert_eq!(
            server.authentication,
            Authentication::Unrecognized(
                json!({"mtls": {"realm": "x"}})
                    .as_object()
                    .expect("member is an object")
                    .clone()
            )
        );
        assert_eq!(
            serde_json::to_value(&server).expect("server serializes"),
            value
        );
    }

    #[test]
    fn unknown_default_isa_is_kept() {
        let mut value = server_value(&json!({"none": {}}));
        value["defaultIsa"] = json!("riscv64");
        let server: Server<Value> = serde_json::from_value(value.clone()).expect("server parses");
        assert_eq!(
            serde_json::to_value(&server).expect("server serializes"),
            value
        );
    }

    #[test]
    fn unknown_response_fields_are_ignored() {
        let mut value = server_value(&json!({"none": {}}));
        value["futureField"] = json!(1);
        let server: Server<Value> = serde_json::from_value(value).expect("server parses");
        assert_eq!(
            server.authentication,
            Authentication::None(NoAuthentication {})
        );
    }

    #[test]
    fn missing_required_field_is_rejected() {
        let mut value = server_value(&json!({"none": {}}));
        value
            .as_object_mut()
            .expect("server is an object")
            .remove("defaultIsa");
        assert!(serde_json::from_value::<Server<Value>>(value).is_err());
    }
}
