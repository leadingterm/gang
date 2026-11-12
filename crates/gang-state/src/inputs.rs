//! The values one scheduling pass hands to the state builder.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_reconciler::{FleetPass, HostedSnapshot};
use gang_spi::ActionsView;
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
        /// The indexed scheduler snapshot the pass was reconciled with.
        hosted: &'a HostedSnapshot,
    },
    /// The pass was skipped for the given reason.
    Skipped(SkipReason),
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
    /// The epoch of the principal set, bumped whenever principals change.
    pub principals_epoch: u64,
}
