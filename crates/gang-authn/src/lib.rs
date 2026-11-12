//! Authentication plugins for Gang, implementing the contract of ADR-05.
//!
//! A surface selects one plugin, and that plugin decides how callers identify themselves and
//! which credentials it issues. Three plugins ship here:
//!
//! - [`Token`] verifies Gang-signed tokens, presented as bearer credentials or in the session
//!   cookie, and mints them for callers that present a proof from an identity provider. It is
//!   configured by [`TokenConfig`].
//! - [`Mtls`] identifies callers by the client certificate the transport verified, and issues and
//!   revokes those certificates through Gang's authority. It is configured by [`MtlsConfig`].
//! - [`Anonymous`] admits every caller as [`anonymous`] and issues nothing. It is configured by
//!   [`NoneConfig`].
//!
//! Each plugin implements [`gang_spi::Plugin`], [`gang_spi::AuthenticationPort`] and
//! [`gang_spi::CredentialLifecyclePort`]. The operations a plugin does not offer return
//! [`gang_spi::PortError::Unsupported`].
//!
//! ```
//! use gang_authn::{NoneConfig, anonymous};
//!
//! let _config: NoneConfig = serde_json::from_str("{}")?;
//! assert_eq!(anonymous().to_string(), "none:anonymous");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod errors;
mod jose;
mod key;
mod memo;
mod mtls_plugin;
mod none_plugin;
mod token;
mod token_plugin;

pub use errors::TokenError;
pub use key::TokenKey;
pub(crate) use memo::Memo;
pub use mtls_plugin::{Mtls, MtlsConfig};
pub use none_plugin::{Anonymous, NoneConfig, anonymous};
pub use token::{AUDIENCE, TokenUse, Verified, issuer_of, mint, verify};
pub use token_plugin::{NoTokenProviders, Token, TokenConfig};
