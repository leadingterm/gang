//! Native execution engine adapter implementing [`gang_spi::SchedulerPort`].
//!
//! Houses the controller actor shell, public REAPI v2 and internal worker gRPC services, listener
//! and handoff transport plumbing, and the worker action execution runtime.

mod acquisition;
mod gate;
mod handoff;
mod observer;
mod refusals;
mod serve;
mod standing;
mod worker_session;

pub use worker_session::{
    ConnectError, ConnectedChannel, SessionConfig, SessionConnector, SessionError, SessionEvent,
    SessionOutbound, SessionSendError, TelemetrySource, backoff_delay, cache_stats_to_proto,
    generate_worker_id, run_session,
};
