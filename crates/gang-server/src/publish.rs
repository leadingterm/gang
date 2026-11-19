//! Publishes the live dashboard state after every control pass.
//!
//! The publisher turns one pass's outcome, snapshot and plan into the next state frame. It holds
//! the state builder, the latest actions sample, the farm-wide cache traffic and the cache
//! provider's latest measurement; each pass brings the fleets it ran over. Each published frame
//! reaches the hub's viewers as a delta.

use gang_clock::Timestamp;
use gang_reconciler::FleetPass;
use gang_spi::{ObservationSink, RecordedScalingEvent};
use gang_state::{Builder, BuilderConfig, CacheBaseline, Inputs, PassStatus};
use gang_types::FleetCatalog;

use crate::actions::ActionsCache;
use crate::builds::BuildsCell;
use crate::cache_usage::CacheUsage;
use crate::events::wire_event;
use crate::hub::StateHub;

/// Builds the dashboard state from control passes and publishes it to a [`StateHub`].
pub(crate) struct StatePublisher {
    hub: StateHub,
    builder: Builder,
    cache: ActionsCache,
    builds: BuildsCell,
    sink: ObservationSink,
    usage: CacheUsage,
}

impl StatePublisher {
    /// A publisher that builds the state with `config`, reads actions from `cache`, cache traffic
    /// from `sink` and the provider's measurement from `usage`. The `baseline` taken at the start
    /// of the term never enters a sample.
    #[must_use]
    pub(crate) fn new(
        hub: StateHub,
        config: BuilderConfig,
        cache: ActionsCache,
        builds: BuildsCell,
        sink: ObservationSink,
        usage: CacheUsage,
        baseline: CacheBaseline,
    ) -> Self {
        Self {
            hub,
            builder: Builder::new(config, baseline),
            cache,
            builds,
            sink,
            usage,
        }
    }

    /// Adds a recorded scaling event to the hub's history. It reaches viewers with the next
    /// publish.
    pub(crate) fn record_event(&self, recorded: RecordedScalingEvent) {
        self.hub.record_event(wire_event(recorded));
    }

    /// Publishes one pass over `fleets` and `catalog`, in the same declaration order. A skipped
    /// pass ignores the snapshot, supplies and outlook of the fleets it is given.
    pub(crate) fn publish(
        &mut self,
        wall: Timestamp,
        pass: PassStatus<'_>,
        fleets: &[FleetPass],
        catalog: &FleetCatalog,
    ) {
        let builder = &mut self.builder;
        let cache = &self.cache;
        let builds = &self.builds;
        let traffic = self.sink.traffic();
        let measured = self.usage.get();
        self.hub.publish(|cur, prev, seq, events| {
            let () = cache.read(|actions| {
                builds.read(|builds| {
                    builder.fill(
                        cur,
                        prev,
                        seq,
                        &Inputs {
                            wall,
                            pass,
                            fleets,
                            catalog,
                            actions,
                            builds,
                            events,
                            traffic,
                            measured: measured.as_ref(),
                            principals_epoch: prev.principals_epoch,
                        },
                    );
                });
            });
        });
    }
}
