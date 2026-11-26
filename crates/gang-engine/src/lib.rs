//! Native execution engine adapter implementing [`gang_spi::SchedulerPort`].
//!
//! Houses the controller actor shell, public REAPI v2 and internal worker gRPC services, listener
//! and handoff transport plumbing, and the worker action execution runtime.
