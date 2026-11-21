//! The autoscaler's reconcile pass: from the scheduler's queue and the fleets' supply to the
//! steps that resize, drain, and terminate workers.
//!
//! The pass performs no I/O. The caller creates a [`FleetPass`] with an owned policy for each
//! fleet, refreshes its supply, indexes the scheduler snapshot with [`HostedSnapshot`], runs
//! [`Reconciler::reconcile`], and applies the resulting [`Plan`].
//!
//! ```
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! use gang_clock::{TestClock, Timestamp};
//! use gang_config::{FleetPolicy, HistoryWindow, InstanceBounds, ScalingConfig};
//! use gang_reconciler::{Action, FleetPass, HostedSnapshot, NeedCause, PassTime, Plan, Reconciler, SourceProfiles};
//! use gang_spi::{QueuedDemand, SchedulerSnapshot};
//! use gang_types::{ActionResources, FleetName, FleetSet, Isa, NonZeroBytes, nonzero_u32};
//!
//! let policy = FleetPolicy {
//!     name: FleetName::try_new("x86-64-fleet")?,
//!     isa: Isa::X86_64,
//!     action_environment: "host:test".parse()?,
//!     instances: InstanceBounds::up_to(nonzero_u32(4)),
//!     vcpus_per_instance: nonzero_u32(4),
//!     memory_per_instance: NonZeroBytes::gib(8),
//!     disk_per_instance: NonZeroBytes::gib(100),
//!     target_actions_per_instance: nonzero_u32(4),
//!     idle_timeout: Duration::from_secs(15),
//!     max_drain_wait: Duration::from_mins(1),
//!     boot_grace: Duration::from_secs(30),
//!     actions: Default::default(),
//! };
//! let mut fleets = [FleetPass::new(policy.clone())];
//!
//! let mut snapshot = SchedulerSnapshot {
//!     queued: vec![QueuedDemand {
//!         candidates: FleetSet::prefix(1)?,
//!         source: None,
//!         resources: ActionResources::default(),
//!         queued_since: Timestamp::from_unix_millis(0),
//!         priority: 0,
//!         expected: None,
//!     }],
//!     ..SchedulerSnapshot::default()
//! };
//! let mut hosted = HostedSnapshot::default();
//! hosted.replace(&mut snapshot);
//! let clock = TestClock::new(Timestamp::from_unix_millis(1_000));
//!
//! let scaling = Arc::new(ScalingConfig::default());
//! let profiles = SourceProfiles::new(&scaling, HistoryWindow::from_units(1));
//! let mut reconciler = Reconciler::new(std::slice::from_ref(&policy), scaling, profiles);
//! let mut plan = Plan::new();
//! reconciler.reconcile(&mut fleets, &hosted, PassTime::read(&clock), &mut plan);
//! assert!(matches!(
//!     plan.steps.as_slice(),
//!     [step] if matches!(step.action, Action::ScaleOut { from: 0, to: 1, need: NeedCause::Demand, .. })
//! ));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod activity;
mod decision;
mod demand;
mod fleet;
mod hosted_snapshot;
mod observations;
mod pass_time;
mod plan;
mod profile;
mod rates;
mod reconciler;
mod vectors;
mod wait;

pub use activity::Activity;
pub use decision::{CordonDecision, Metric, ScaleDecision, ScalingDimension};
pub use demand::Demand;
pub use fleet::{FleetOutlook, FleetPass, WorkerCounts};
pub use hosted_snapshot::HostedSnapshot;
pub use pass_time::PassTime;
pub use plan::{Action, DrainReason, NeedCause, Plan, PlanStep, TerminationCause};
pub use profile::SourceProfiles;
pub use reconciler::Reconciler;
