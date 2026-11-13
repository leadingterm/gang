//! Provider-agnostic `OpenID` Connect for Gang: verifies identity tokens on the server and
//! runs interactive logins on the client.
//!
//! Configuration is an [`OidcConfig`]; the server builds a [`Provider`] from it.
//!
//! ```
//! use gang_oidc::OidcConfig;
//!
//! let config: OidcConfig = serde_json::from_str(
//!     r#"{"issuer_url":"https://idp.example.test","audience":"gang","principal_claim":"email"}"#,
//! )?;
//! assert_eq!(config.principal_claim, "email");
//! assert!(config.login.is_none());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod claims;
mod config;
mod discovery;
mod errors;
mod http;
mod keys;
mod login;
mod provider;
mod refresh_cache;
mod verify;

pub use config::{
    ConfigError, IssuerUrl, IssuerUrlError, LoginConfig, OfflineLogin, OidcCache, OidcConfig,
    OidcHttp, OidcTokens,
};
pub use provider::Provider;
