//! The autoscaler's reconcile pass: from the scheduler's queue and the fleets' supply to the
//! steps that resize, drain, and terminate workers.
//!
//! The pass performs no I/O. The caller creates a [`FleetPass`] with an owned policy for each
//! fleet, refreshes its supply, indexes the scheduler snapshot with [`HostedSnapshot`], runs
//! [`Reconciler::reconcile`], and applies the resulting [`Plan`].
//!
//! ```
//! use std::time::Duration;
//!
//! use gang_clock::{TestClock, Timestamp};
//! use gang_config::{FleetPolicy, InstanceBounds};
//! use gang_reconciler::{Action, FleetPass, HostedSnapshot, PassTime, Plan, PlanStep, Reconciler};
//! use gang_spi::{QueuedDemand, SchedulerSnapshot};
//! use gang_types::{FleetName, FleetSet, Isa, NonZeroBytes, Resources, nonzero_u32};
//!
//! let policy = FleetPolicy {
//!     name: FleetName::try_new("x86-64-fleet")?,
//!     isa: Isa::X86_64,
//!     instances: InstanceBounds::up_to(nonzero_u32(4)),
//!     vcpus_per_instance: nonzero_u32(4),
//!     memory_per_instance: NonZeroBytes::gib(8),
//!     disk_per_instance: NonZeroBytes::gib(100),
//!     target_actions_per_instance: nonzero_u32(4),
//!     idle_timeout: Duration::from_secs(15),
//!     max_drain_wait: Duration::from_mins(1),
//!     boot_grace: Duration::from_secs(30),
//! };
//! let mut fleets = [FleetPass::new(policy.clone())];
//!
//! let mut snapshot = SchedulerSnapshot {
//!     queued: vec![QueuedDemand {
//!         candidates: FleetSet::prefix(1)?,
//!         resources: Resources::default(),
//!         queued_since: Timestamp::from_unix_millis(0),
//!         priority: 0,
//!     }],
//!     ..SchedulerSnapshot::default()
//! };
//! let mut hosted = HostedSnapshot::default();
//! hosted.replace(&mut snapshot);
//! let clock = TestClock::new(Timestamp::from_unix_millis(1_000));
//!
//! let mut reconciler = Reconciler::new(std::slice::from_ref(&policy));
//! let mut plan = Plan::new();
//! reconciler.reconcile(&mut fleets, &hosted, PassTime::read(&clock), &mut plan);
//! assert_eq!(
//!     plan.steps,
//!     [PlanStep {
//!         fleet: 0,
//!         action: Action::ScaleOut { from: 0, to: 1 },
//!     }]
//! );
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod activity;
mod demand;
mod fleet;
mod hosted_snapshot;
mod observations;
mod pass_time;
mod plan;
mod reconciler;

pub use activity::Activity;
pub use demand::Demand;
pub use fleet::{FleetOutlook, FleetPass, WorkerCounts};
pub use hosted_snapshot::HostedSnapshot;
pub use pass_time::PassTime;
pub use plan::{Action, Plan, PlanStep, TerminationCause};
pub use reconciler::Reconciler;
