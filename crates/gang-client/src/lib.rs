//! Client side of Gang identity: `gang login` (interactive and workload), the bearer tokens that
//! Bazel's credential helper presents to the server, and the Bazel settings of a server.
//! It also backs `gang doctor`, which checks a workspace's Bazel settings against a server.
//!
//! The crate has no command line. A binary builds [`ClientOptions`], a [`ClientHome`], a
//! [`LoginProviders`] implementation, and a [`Console`], and calls [`Client`].
//!
//! ```
//! use gang_client::ClientOptions;
//!
//! let options: ClientOptions = serde_json::from_str("{}").expect("empty object is valid");
//! assert_eq!(options, ClientOptions::default());
//! ```

mod api;
mod bazel_options;
mod bazelrc;
mod client;
mod console;
mod credential_helper;
mod doctor;
mod doctor_checks;
mod doctor_report;
mod errors;
mod home;
mod lock;
mod loopback;
mod options;
mod proof_source;
mod providers;
mod server_probe;
mod session;
mod stored;
#[cfg(test)]
mod test_support;

pub use api::ServerApi;
pub use bazel_options::{BazelCommand, BazelProbe, EffectiveOptions, OptionSource, Setting};
pub use bazelrc::CA_FILE_NAME;
pub use client::{
    Bazelrc, BazelrcRequest, BearerToken, Client, InteractiveLogin, LoggedIn, WorkloadLogin,
};
pub use console::Console;
pub use credential_helper::CredentialRequest;
pub use doctor::{Doctor, Event};
pub use doctor_report::{Check, Finding, Report, Status};
pub use errors::{BazelError, ClientError};
pub use home::ClientHome;
pub use options::{ClientOptions, DEFAULT_BROWSER_COMMAND};
pub use proof_source::ProofSource;
pub use providers::LoginProviders;
pub use server_probe::{
    ClientProbe, CredentialFault, ServerFault, ServerProbe, ServerReading, StoredLogin,
    stored_login,
};
