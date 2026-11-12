//! Wire schema for Gang's HTTP API: the resources and bodies each route of [`routes::ROUTES`]
//! speaks, the stream frames of the live state, the error envelope, and the conventions they
//! share (RFC 3339 timestamps, Google durations, resource names, page tokens).
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let certificate: gang_wire::Certificate = serde_json::from_str(
//!     r#"{
//!         "name": "certificates/3fa9",
//!         "principal": "principals/aws-workers%3Ai-0e10",
//!         "createTime": "2026-01-14T09:00:00.000Z",
//!         "expireTime": "2026-01-21T09:00:00.000Z",
//!         "pemCertificate": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----"
//!     }"#,
//! )?;
//! let round_trip: gang_wire::Certificate =
//!     serde_json::from_str(&serde_json::to_string(&certificate)?)?;
//! assert_eq!(certificate, round_trip);
//! # Ok(())
//! # }
//! ```

extern crate self as gang_wire;

#[macro_use]
mod enumeration;
mod certificate;
pub mod duration;
mod error;
mod errors;
mod event;
mod frame;
mod known;
pub mod name;
pub mod page;
mod permission;
mod principal;
mod role;
pub mod routes;
mod server;
mod session;
mod state;
pub mod time;

pub use certificate::{Certificate, CreateCertificateRequest};
pub use error::{
    BadRequest, Code, Detail, ERROR_DOMAIN, ErrorInfo, ErrorReason, ErrorResponse, FieldViolation,
    INTERNAL_ERROR_BODY, RetryInfo, Status, UnknownErrorReason,
};
pub use errors::{FrameError, PageError, ResourceNameError, WireDurationError};
pub use event::{Cause, Event, EventChange, ListEventsResponse};
pub use frame::{
    ActionsDelta, CLOSED_EVENT, Closed, ClosedReason, DELTA_EVENT, Delta, Frame, FrameRef, Keyed,
    SNAPSHOT_EVENT, TICK_EVENT, Tick,
};
pub use known::Known;
pub use permission::Permission;
pub use principal::{ListPrincipalsResponse, Principal, RevokePrincipalResponse};
pub use role::{ListRolesResponse, Role};
pub use routes::{CALLBACK_PATH, ERROR_PATH, LOGIN_PATH, LOGOUT_PATH, RouteId, UiRoute};
pub use server::{Authentication, Isa, NoAuthentication, Server, TokenAuthentication};
pub use session::{AuthenticateRequest, AuthenticateResponse, CookieNames, Session, SessionLogin};
pub use state::{
    Actions, Demand, Fleet, Instance, InstanceCounts, InstanceSize, Lifecycle, Named, Pass,
    PassState, Phase, Policy, Queue, QueuedAction, RunningAction, SkipReason, State, Truncated,
    Worker, WorkerCounts,
};
