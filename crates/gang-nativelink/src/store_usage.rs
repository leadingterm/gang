//! Reads the controller stores' usage, budget and evictions from the metrics `NativeLink`
//! publishes through `tracing`.
//!
//! A store with an eviction map publishes it as the `evicting_map` group: a span of target
//! `nativelink_metric` whose `__name` is the group, around one event per metric carrying its
//! `__name` and counter `__value`. A collector scoped to one synchronous publish sums the three
//! metrics of every eviction map of the store.

use std::fmt;
use std::sync::Arc;

use gang_spi::{EngineStore, StoreKind};
use nativelink_metric::{MetricFieldData, MetricKind, MetricsComponent};
use nativelink_store::store_manager::StoreManager;
use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Dispatch, Event, Metadata, Subscriber};

use crate::properties::{ACTION_STORE, CONTENT_STORE};

/// The `tracing` target of every metric span and event `NativeLink` publishes.
const METRIC_TARGET: &str = "nativelink_metric";
/// The group a store publishes its eviction map under.
const EVICTING_MAP: &str = "evicting_map";
/// The field naming a published group or metric.
const NAME_FIELD: &str = "__name";
/// The field carrying a published metric's value.
const VALUE_FIELD: &str = "__value";

/// The controller stores, in reporting order.
const STORES: [(StoreKind, &str); 2] = [
    (StoreKind::Content, CONTENT_STORE),
    (StoreKind::Action, ACTION_STORE),
];

/// Reads the controller stores through one collector reused by every pass.
pub(crate) struct StoreUsage {
    /// The stores the controller serves.
    manager: Arc<StoreManager>,
    /// What the collector gathered during the current store's publish.
    tally: Arc<Mutex<Tally>>,
    /// The collector, made the default only around a store's publish.
    collector: Dispatch,
    /// One pass at a time: the tally belongs to a single publish.
    pass: Mutex<()>,
}

impl StoreUsage {
    /// Builds the reader of `manager`'s controller stores.
    pub(crate) fn new(manager: Arc<StoreManager>) -> Self {
        let tally = Arc::new(Mutex::new(Tally::default()));
        Self {
            manager,
            collector: Dispatch::new(Collector(Arc::clone(&tally))),
            tally,
            pass: Mutex::new(()),
        }
    }

    /// Overwrites `into` with one row per controller store that publishes an eviction map, in
    /// [`STORES`] order. `into`'s buffer and the collector's are reused.
    pub(crate) fn fill(&self, into: &mut Vec<EngineStore>) {
        into.clear();
        let _pass = self.pass.lock();
        for (kind, name) in STORES {
            let Some(store) = self.manager.get_store(name) else {
                continue;
            };
            self.tally.lock().reset();
            let published = tracing::dispatcher::with_default(&self.collector, || {
                MetricsComponent::publish(&store, MetricKind::Component, MetricFieldData::default())
            });
            if let Err(error) = published {
                tracing::warn!(store = name, %error, "the store's metrics could not be read");
                continue;
            }
            into.extend(self.tally.lock().reading.row(kind));
        }
    }
}

/// What the collector gathered during one store's publish.
#[derive(Default)]
struct Tally {
    /// Whether each span opened during the publish is an eviction-map group, at its id minus one.
    spans: Vec<bool>,
    /// Eviction-map groups entered and not yet exited.
    depth: usize,
    /// The metrics published inside eviction-map groups.
    reading: Reading,
}

impl Tally {
    /// Forgets the previous publish, keeping the span buffer's capacity.
    fn reset(&mut self) {
        self.spans.clear();
        self.depth = 0;
        self.reading = Reading::default();
    }

    /// Whether `span` is an eviction-map group of this publish.
    fn is_map(&self, span: &Id) -> bool {
        usize::try_from(span.into_u64().saturating_sub(1))
            .ok()
            .and_then(|index| self.spans.get(index))
            .copied()
            .unwrap_or(false)
    }
}

/// A store's eviction-map metrics, summed over its maps; `None` until published.
#[derive(Default)]
struct Reading {
    /// `sum_store_size`: the bytes the map holds.
    used: Option<u64>,
    /// `max_bytes`: the size at which the map evicts.
    budget: Option<u64>,
    /// `evicted_bytes`: the bytes evicted since the store was built.
    evicted: Option<u64>,
}

impl Reading {
    fn add(&mut self, metric: Metric, value: u64) {
        let total = match metric {
            Metric::Used => &mut self.used,
            Metric::Budget => &mut self.budget,
            Metric::Evicted => &mut self.evicted,
        };
        *total = Some(total.unwrap_or(0).saturating_add(value));
    }

    /// The store's row, once every metric was published.
    fn row(&self, store: StoreKind) -> Option<EngineStore> {
        Some(EngineStore {
            store,
            used: self.used?,
            budget: self.budget?,
            evicted: self.evicted?,
        })
    }
}

/// An eviction-map metric the engine reports.
#[derive(Clone, Copy)]
enum Metric {
    Used,
    Budget,
    Evicted,
}

impl Metric {
    fn named(name: &str) -> Option<Self> {
        match name {
            "sum_store_size" => Some(Self::Used),
            "max_bytes" => Some(Self::Budget),
            "evicted_bytes" => Some(Self::Evicted),
            _ => None,
        }
    }
}

/// Whether a group span names the eviction map.
struct GroupName(bool);

impl Visit for GroupName {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == NAME_FIELD {
            self.0 = value == EVICTING_MAP;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

/// A published metric this module reads, and its counter value.
#[derive(Default)]
struct Published {
    metric: Option<Metric>,
    value: Option<u64>,
}

impl Visit for Published {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == VALUE_FIELD {
            self.value = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == NAME_FIELD {
            self.metric = Metric::named(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

/// Collects `NativeLink`'s metric spans and events into a shared tally; ignores all else.
struct Collector(Arc<Mutex<Tally>>);

impl Subscriber for Collector {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.enabled(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == METRIC_TARGET
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::INFO)
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        let mut group = GroupName(false);
        span.record(&mut group);
        let mut tally = self.0.lock();
        tally.spans.push(group.0);
        Id::from_u64(u64::try_from(tally.spans.len()).unwrap_or(u64::MAX))
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut published = Published::default();
        event.record(&mut published);
        let (Some(metric), Some(value)) = (published.metric, published.value) else {
            return;
        };
        let mut tally = self.0.lock();
        if tally.depth > 0 {
            tally.reading.add(metric, value);
        }
    }

    fn enter(&self, span: &Id) {
        let mut tally = self.0.lock();
        if tally.is_map(span) {
            tally.depth += 1;
        }
    }

    fn exit(&self, span: &Id) {
        let mut tally = self.0.lock();
        if tally.is_map(span) {
            tally.depth = tally.depth.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use bytes::Bytes;
    use gang_spi::{ControllerSpec, ObservationSink, RenderedStoreSpec};
    use gang_testkit::TestResult;
    use gang_types::FleetCatalog;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::store_trait::StoreLike;
    use serde_json::{Value, json};

    use super::*;
    use crate::settings;
    use crate::stores::build_stores;

    /// The filesystem store's default block size: it counts every file in whole blocks.
    const BLOCK: u64 = 4096;
    /// Three blocks and a half: three one-block blobs fit, a fourth evicts one.
    const FILESYSTEM_BUDGET: u64 = 3 * BLOCK + BLOCK / 2;
    /// The memory store's blob size.
    const MEMORY_BLOB: u64 = 300;
    /// Three blobs and a third: three blobs fit, a fourth evicts one.
    const MEMORY_BUDGET: u64 = 1000;
    /// Blobs written to each store: two more than fit.
    const BLOBS: u8 = 5;

    /// Builds the controller's stores from the rendered `content` and `action` specs, as the
    /// controller does.
    async fn controller_stores(
        content: Value,
        action: Value,
    ) -> Result<Arc<StoreManager>, Box<dyn Error>> {
        let fleets = FleetCatalog::try_new(Vec::new())?;
        let (observations, _receiver) =
            ObservationSink::channel(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let (content, action) = (RenderedStoreSpec(content), RenderedStoreSpec(action));
        let spec = ControllerSpec {
            actions: Arc::new(gang_config::ActionsConfig::default()),
            content_store: &content,
            action_store: &action,
            fleets: &fleets,
            max_workers: 1,
            observations: &observations,
            client_action_timeout: Duration::from_mins(1),
        };
        Ok(build_stores(settings::controller_stores(&spec)?).await?)
    }

    /// Writes `BLOBS` distinct blobs of `size` bytes to the store `name`, oldest first.
    async fn fill_store(stores: &StoreManager, name: &str, size: u64) -> TestResult {
        let store = stores.get_store(name).ok_or("the store is registered")?;
        for index in 0..BLOBS {
            let blob = Bytes::from(vec![index; usize::try_from(size)?]);
            store
                .update_oneshot(DigestInfo::new([index; 32], size), blob)
                .await?;
        }
        Ok(())
    }

    /// U6. Simulated flaws: reading `lifetime_inserted_bytes` as used; swapping `max_bytes` and
    /// `sum_store_size`; a renamed group or metric; a tally carried over to the next pass.
    #[tokio::test]
    async fn bounded_stores_report_usage_budget_and_evictions() -> TestResult {
        let scratch = tempfile::tempdir()?;
        let path = |name: &str| {
            scratch
                .path()
                .join(name)
                .to_str()
                .map(str::to_owned)
                .ok_or("the scratch path is UTF-8")
        };
        let content = json!({"filesystem": {
            "content_path": path("content")?,
            "temp_path": path("tmp")?,
            "eviction_policy": {"max_bytes": FILESYSTEM_BUDGET},
        }});
        let action = json!({"memory": {"eviction_policy": {"max_bytes": MEMORY_BUDGET}}});
        let stores = controller_stores(content, action).await?;
        fill_store(&stores, CONTENT_STORE, BLOCK).await?;
        fill_store(&stores, ACTION_STORE, MEMORY_BLOB).await?;
        // Publishing before the collector exists registers the metric call sites without it.
        MetricsComponent::publish(&*stores, MetricKind::Component, MetricFieldData::default())?;

        let usage = StoreUsage::new(Arc::clone(&stores));
        let mut rows = Vec::new();
        usage.fill(&mut rows);
        let expected = [
            EngineStore {
                store: StoreKind::Content,
                used: 3 * BLOCK,
                budget: FILESYSTEM_BUDGET,
                evicted: 2 * BLOCK,
            },
            EngineStore {
                store: StoreKind::Action,
                used: 3 * MEMORY_BLOB,
                budget: MEMORY_BUDGET,
                evicted: 2 * MEMORY_BLOB,
            },
        ];
        assert_eq!(rows, expected);

        let capacity = rows.capacity();
        usage.fill(&mut rows);
        assert_eq!(rows, expected);
        assert_eq!(rows.capacity(), capacity);
        Ok(())
    }

    /// Simulated flaw: reporting a zero row for a store that publishes no eviction map.
    #[tokio::test]
    async fn a_store_without_an_eviction_map_has_no_row() -> TestResult {
        let stores = controller_stores(json!({"noop": {}}), json!({"memory": {}})).await?;
        let mut rows = Vec::new();
        StoreUsage::new(stores).fill(&mut rows);
        assert_eq!(
            rows,
            [EngineStore {
                store: StoreKind::Action,
                used: 0,
                budget: 0,
                evicted: 0,
            }]
        );
        Ok(())
    }
}
