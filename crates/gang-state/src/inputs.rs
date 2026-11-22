//! The values one scheduling pass hands to the state builder.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_reconciler::{FleetPass, HostedSnapshot};
use gang_spi::{ActionsView, MeasuredUsage, Traffic};
use gang_types::FleetCatalog;
use gang_wire::SkipReason;

/// Static settings of the state builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuilderConfig {
    /// The number of scaling events kept in the state.
    pub events_limit: usize,
    /// The age after which the actions sample is reported as stale.
    pub stale_after: Duration,
    /// The control poll interval reported to clients.
    pub interval: Duration,
    /// How far back `State::recent` and the workers' phases reach.
    pub history_window: Duration,
    /// The span of one sample of `State::recent`.
    pub history_step: Duration,
    /// The name of the provisioner reported in the state.
    pub provisioner: String,
}

/// The outcome of the scheduling pass that produced the inputs.
#[derive(Debug, Clone)]
pub enum PassStatus<'a> {
    /// The pass completed.
    Ok {
        /// The number of fleet changes applied by the pass.
        applied: u32,
        /// The number of fleet changes that failed.
        failed: u32,
        /// The first diagnostic from this pass, if any.
        error: Option<&'a str>,
    },
    /// The pass was skipped for the given reason.
    Skipped {
        /// Why the pass was skipped.
        reason: SkipReason,
        /// The first diagnostic from this pass, if any.
        error: Option<&'a str>,
    },
    /// An independent observation, without a completed or skipped control pass.
    Observed {
        /// Fresh scheduler snapshot, or `None` when unavailable.
        hosted: Option<&'a HostedSnapshot>,
        /// Whether a complete supply has been read successfully for each fleet, in fleet order.
        /// A later failed read retains the last successful supply.
        supplies: &'a [bool],
        /// Whether each fleet's supply read succeeded on this independent observation tick,
        /// in fleet order. Unlike `supplies`, this is not retained across failed reads.
        fresh_supplies: &'a [bool],
    },
}

/// A sample of the scheduler's actions, taken at a known time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionsSample {
    /// The time the sample was taken.
    pub at: Timestamp,
    /// The running and queued actions at that time.
    pub view: ActionsView,
}

/// Everything one scheduling pass contributes to the state.
#[derive(Debug, Clone)]
pub struct Inputs<'a> {
    /// The wall-clock time of the pass.
    pub wall: Timestamp,
    /// The outcome of the pass.
    pub pass: PassStatus<'a>,
    /// The fleets in declaration order, each with its policy, supply and outlook.
    /// Always present, also for a skipped pass, because the actions part names the fleets.
    pub fleets: &'a [FleetPass],
    /// Every fleet's properties, in the same declaration order as `fleets`.
    pub catalog: &'a FleetCatalog,
    /// The latest actions sample, when the scheduler can be asked for one.
    pub actions: Option<&'a ActionsSample>,
    /// The newest scaling events, oldest first.
    pub events: &'a [gang_wire::Event],
    /// The complete live operational record snapshot for control or skipped passes.
    /// Independent observations do not carry a snapshot and leave this empty.
    pub infrastructure: &'a [gang_spi::FleetInfrastructureEvent],
    /// The builds the recorder holds.
    pub builds: &'a gang_insight::Builds,
    /// Cumulative farm-wide cache traffic.
    pub traffic: Traffic,
    /// The provider's latest successful external-store measurement.
    pub measured: Option<&'a MeasuredUsage>,
    /// The epoch of the principal set, bumped whenever principals change.
    pub principals_epoch: u64,
}
