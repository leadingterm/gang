//! Support for the code `port` and `compose!` generate; not a public API.

/// Names the generated code resolves: the macros' own dependencies and the types that appear
/// in port signatures. It also re-exports `gang_types` for the settings delegate macros and the
/// fleet option guard.
#[doc(hidden)]
pub mod __private {
    pub use gang_clock::Timestamp;
    pub use gang_types;
    pub use gang_types::{
        AuthenticationSettings, CertSerial, Fleet, Identity, InstanceId, LoginSession, Principal,
        Proof, ProviderName, RefreshToken, SchedulerSettings, Url, WorkerId, WorkerLifetime,
        string_enum,
    };
    pub use serde;
}
