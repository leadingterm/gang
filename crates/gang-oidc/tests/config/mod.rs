//! Configuration parsing, validation, and login capability, without any network access.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::TestClock;
use gang_oidc::{OidcConfig, Provider};
use gang_spi::{LoginPort, LoginSupport, Plugin, PluginContext, PortError};
use gang_types::NonZeroDuration;
use serde_json::{Value, json};

fn base_config() -> Value {
    json!({
        "issuer_url": "https://idp.example.test/",
        "audience": "gang-client",
        "principal_claim": "email",
    })
}

fn with_login(mut value: Value, login: Value) -> Value {
    value["login"] = login;
    value
}

fn parse(value: Value) -> OidcConfig {
    serde_json::from_value(value).expect("config parses")
}

fn assert_config_without_login_eq(actual: &OidcConfig, expected: &OidcConfig) {
    assert_eq!(actual.issuer_url, expected.issuer_url);
    assert_eq!(actual.audience, expected.audience);
    assert_eq!(actual.additional_audiences, expected.additional_audiences);
    assert_eq!(actual.principal_claim, expected.principal_claim);
    assert_eq!(actual.required_claims, expected.required_claims);
    assert!(actual.login.is_none());
    assert!(expected.login.is_none());
    assert_eq!(actual.cache, expected.cache);
    assert_eq!(actual.http, expected.http);
    assert_eq!(actual.tokens, expected.tokens);
}

fn build(value: Value) -> Result<Provider, PortError> {
    Provider::new(parse(value), Arc::new(TestClock::default()))
}

fn rejected_as_config(value: Value, expected: &str) {
    let error = serde_json::from_value::<OidcConfig>(value).expect_err("config is invalid");
    assert!(
        error.to_string().contains(expected),
        "unexpected error: {error}"
    );
}

fn plugin_build(value: Value) -> Result<Provider, PortError> {
    <Provider as Plugin>::build(
        parse(value),
        &PluginContext {
            config_path: Path::new("gang.toml"),
        },
    )
}

#[test]
fn login_parameters_round_trip_through_serde() {
    let input = json!({
        "issuer_url": "https://idp.example.test/",
        "audience": "gang-client",
        "principal_claim": "email",
        "cache": {
            "key_ttl": "2h",
            "key_refresh_interval": "45s",
            "discovery_ttl": "1h",
            "discovery_retry_interval": "30s",
        },
        "http": {
            "timeout": "7s",
            "max_response_size": "2MiB",
        },
        "tokens": {
            "clock_skew": "5s",
        },
        "required_claims": {
            "email_verified": true,
            "tenant": "alpha",
            "level": 3,
            "groups": ["a", "b"],
            "unset": null,
        },
        "login": {
            "client_secret": "native-app-secret",
            "scopes": ["openid", "email"],
            "authorization_params": {"prompt": "select_account"},
        },
    });

    let config = parse(input.clone());

    assert_eq!(
        serde_json::to_value(&config).expect("config serializes"),
        input
    );
    assert_eq!(config.required_claims.get("level"), Some(&json!(3)));
    assert_eq!(
        config.required_claims.get("email_verified"),
        Some(&json!(true))
    );
    assert_eq!(config.required_claims.get("tenant"), Some(&json!("alpha")));
    let login = config.login.as_ref().expect("login is configured");
    assert!(
        login
            .client_secret
            .as_ref()
            .expect("secret is configured")
            .matches("native-app-secret")
    );
    assert_eq!(login.scopes, vec!["openid", "email"]);
    assert_eq!(
        login.authorization_params,
        BTreeMap::from([("prompt".to_owned(), "select_account".to_owned())])
    );
}

#[test]
fn absent_login_table_is_not_serialized() {
    let config = parse(base_config());

    let value = serde_json::to_value(&config).expect("config serializes");

    assert!(config.login.is_none());
    assert!(value.get("login").is_none());
    assert_eq!(value["required_claims"], json!({}));
}

#[test]
fn login_table_without_client_secret_round_trips() {
    let input = with_login(base_config(), json!({"scopes": ["openid"]}));

    let config = parse(input.clone());

    assert_eq!(
        serde_json::to_value(&config).expect("config serializes")["login"],
        json!({"scopes": ["openid"], "authorization_params": {}})
    );
    let login = config.login.as_ref().expect("login is configured");
    assert!(login.client_secret.is_none());
    assert_eq!(login.scopes, vec!["openid"]);
    assert!(login.authorization_params.is_empty());
}

#[test]
fn unknown_top_level_field_is_rejected() {
    let mut input = base_config();
    input["extra"] = json!(true);

    assert!(serde_json::from_value::<OidcConfig>(input).is_err());
}

#[test]
fn unknown_login_field_is_rejected() {
    let input = with_login(
        base_config(),
        json!({"scopes": ["openid"], "unexpected": "value"}),
    );

    assert!(serde_json::from_value::<OidcConfig>(input).is_err());
}

#[test]
fn https_issuer_is_accepted() {
    assert!(build(base_config()).is_ok());
}

#[test]
fn plain_http_is_accepted_only_for_loopback_hosts() {
    for issuer in [
        "http://127.0.0.1:1/",
        "http://127.0.0.2:1/",
        "http://localhost:1/",
        "http://[::1]:1/",
    ] {
        let mut value = base_config();
        value["issuer_url"] = json!(issuer);
        assert!(build(value).is_ok(), "{issuer} should be accepted");
    }
}

#[test]
fn plain_http_is_rejected_for_non_loopback_hosts() {
    for issuer in [
        "http://idp.example.test/",
        "http://10.0.0.1/",
        "http://localhost.example.test/",
    ] {
        let mut value = base_config();
        value["issuer_url"] = json!(issuer);
        rejected_as_config(
            value,
            "issuer url must use https, or http for a loopback address",
        );
    }
}

#[test]
fn issuer_with_query_or_fragment_is_rejected() {
    for issuer in [
        "https://idp.example.test/?tenant=alpha",
        "https://idp.example.test/#frag",
    ] {
        let mut value = base_config();
        value["issuer_url"] = json!(issuer);
        rejected_as_config(value, "issuer url must not carry a query or fragment");
    }
}

#[test]
fn empty_principal_claim_is_rejected() {
    let mut value = base_config();
    value["principal_claim"] = json!("");

    rejected_as_config(value, "principal claim must not be empty");
}

#[test]
fn reserved_authorization_params_are_rejected() {
    for reserved in [
        "response_type",
        "client_id",
        "redirect_uri",
        "scope",
        "state",
        "nonce",
        "code_challenge",
        "code_challenge_method",
    ] {
        let value = with_login(
            base_config(),
            json!({"scopes": ["openid"], "authorization_params": {reserved: "value"}}),
        );
        rejected_as_config(
            value,
            &format!("authorization parameter `{reserved}` is set by the login flow"),
        );
    }
}

#[test]
fn login_is_unsupported_without_a_login_table() {
    let provider = build(base_config()).expect("provider builds");

    assert_eq!(provider.login(), LoginSupport::Unsupported);
}

#[test]
fn login_is_interactive_with_a_login_table() {
    let provider =
        build(with_login(base_config(), json!({"scopes": ["openid"]}))).expect("provider builds");

    assert_eq!(provider.login(), LoginSupport::Interactive);
}

#[test]
fn omitted_sections_take_documented_defaults() {
    let config = parse(base_config());

    assert_eq!(config.cache.key_ttl, NonZeroDuration::from_hours(1));
    assert_eq!(
        config.cache.key_refresh_interval,
        NonZeroDuration::from_secs(30)
    );
    assert_eq!(config.cache.discovery_ttl, NonZeroDuration::from_hours(1));
    assert_eq!(
        config.cache.discovery_retry_interval,
        NonZeroDuration::from_secs(30)
    );
    assert_eq!(config.http.timeout, NonZeroDuration::from_secs(10));
    assert_eq!(config.http.max_response_size.as_u64(), 1024 * 1024);
    assert_eq!(config.tokens.clock_skew, Duration::from_secs(30));
}

#[test]
fn default_fields_round_trip_through_serde() {
    let config = parse(base_config());

    let value = serde_json::to_value(&config).expect("config serializes");

    assert_eq!(value["cache"]["key_ttl"], json!("1h"));
    assert_eq!(value["http"]["max_response_size"], json!("1MiB"));
    assert_config_without_login_eq(&parse(value), &config);
}

#[test]
fn zero_values_in_zero_able_fields_fail_to_parse() {
    for (section, field, zero) in [
        ("cache", "key_ttl", "0s"),
        ("cache", "key_refresh_interval", "0s"),
        ("cache", "discovery_ttl", "0s"),
        ("cache", "discovery_retry_interval", "0s"),
        ("http", "timeout", "0s"),
        ("http", "max_response_size", "0B"),
    ] {
        let mut value = base_config();
        value[section] = json!({ field: zero });
        assert!(
            serde_json::from_value::<OidcConfig>(value).is_err(),
            "{section}.{field} should be rejected"
        );
    }
}

#[test]
fn additional_audiences_default_to_empty() {
    assert!(parse(base_config()).additional_audiences.is_empty());
}

#[test]
fn empty_additional_audiences_are_not_serialized() {
    let value = serde_json::to_value(parse(base_config())).expect("config serializes");
    assert!(value.get("additional_audiences").is_none());
}

#[test]
fn additional_audiences_round_trip_through_serde() {
    let mut value = base_config();
    value["additional_audiences"] = json!(["gang-web"]);
    let config = parse(value);
    let serialized = serde_json::to_value(&config).expect("config serializes");
    assert_eq!(serialized["additional_audiences"], json!(["gang-web"]));
    assert_config_without_login_eq(&parse(serialized), &config);
}

#[test]
fn empty_additional_audience_is_a_config_error() {
    let mut value = base_config();
    value["additional_audiences"] = json!([""]);
    rejected_as_config(value, "empty audience");
}

#[test]
fn plugin_build_accepts_a_valid_config() {
    assert!(plugin_build(base_config()).is_ok());
}
