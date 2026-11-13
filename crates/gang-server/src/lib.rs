//! Server-side pieces of `gang server`.
//!
//! The crate composes a Gang server from its ports. One public listener carries the client
//! services and the Gang API, which the controller serves as its fallback; one private listener
//! carries workers. Each listener is guarded by a plugin that authenticates its callers. The
//! crate also provides the autoscaler control loop that reconciles fleet capacity against the
//! scheduler's snapshot.
//!
//! A server endpoint is an http or https URL:
//!
//! ```
//! use gang_server::PublicEndpoint;
//! use gang_types::Url;
//!
//! let grpc = Url::parse("grpc://example.com").expect("url parses");
//! assert!(PublicEndpoint::try_new(grpc).is_err());
//! let https = Url::parse("https://example.com").expect("url parses");
//! assert!(PublicEndpoint::try_new(https).is_ok());
//! ```

mod actions;
mod api;
mod api_error;
mod attempts;
mod authorizer;
mod client_address;
mod composite;
mod control;
mod cookies;
mod core;
mod dashboard;
mod deployment;
mod device_logins;
mod endpoint;
mod errors;
mod events;
mod gate;
mod host;
mod hub;
mod identity;
mod info;
mod listeners;
mod login;
mod principals;
mod publish;
mod query;
mod rate_limit;
mod renewal_flights;
mod renewal_seal;
mod resources;
mod responses;
mod revocation;
mod security;
mod server;
mod settings;
mod stream;
mod ttl_map;

#[cfg(test)]
mod actions_tests;
#[cfg(test)]
mod api_certificates_tests;
#[cfg(test)]
mod api_contract_tests;
#[cfg(test)]
mod api_docs_tests;
#[cfg(test)]
mod api_events_tests;
#[cfg(test)]
mod api_principals_tests;
#[cfg(test)]
mod api_revocation_tests;
#[cfg(test)]
mod api_roles_tests;
#[cfg(test)]
mod api_routing_tests;
#[cfg(test)]
mod api_session_tests;
#[cfg(test)]
mod api_state_tests;
#[cfg(test)]
mod api_test_support;
#[cfg(test)]
mod composite_tests;
#[cfg(test)]
mod control_tests;
#[cfg(test)]
mod dashboard_tests;
#[cfg(test)]
mod device_api_tests;
#[cfg(test)]
mod gate_tests;
#[cfg(test)]
mod host_tests;
#[cfg(test)]
mod hub_tests;
#[cfg(test)]
mod identity_tests;
#[cfg(test)]
mod listeners_tests;
#[cfg(test)]
mod rate_limit_tests;
#[cfg(test)]
mod security_tests;
#[cfg(test)]
mod serving_tests;
#[cfg(test)]
mod session_login_tests;
#[cfg(test)]
mod session_tests;
#[cfg(test)]
mod shutdown_tests;
#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod test_support_api;
#[cfg(test)]
mod test_support_clock;
#[cfg(test)]
mod test_support_crypto;
#[cfg(test)]
mod test_support_deployment;
#[cfg(test)]
mod test_support_fleet;
#[cfg(test)]
mod test_support_memory;
#[cfg(test)]
mod test_support_session;
#[cfg(test)]
mod test_support_session_harness;
#[cfg(test)]
mod test_support_verifier;

pub use deployment::{Deployment, Listeners, Parts, Stack};
pub use endpoint::{EndpointRole, PublicEndpoint};
pub use errors::ServerError;
pub use identity::{Provider, Providers};
pub use server::Server;
pub use settings::{
    DeviceSettings, HandshakeSettings, LoginSettings, RequestLimits, ServerSettings, TlsSettings,
    TransportSettings,
};
