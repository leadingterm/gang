use std::collections::HashMap;

use gang_clock::Timestamp;
use gang_config::BuildsConfig;
use gang_spi::Observation;
use gang_types::{BuildId, BuildKey};
use gang_wire::{Build, BuildState};

use crate::{BuildFilter, record::Record};

/// Bounded in-memory build summaries.
#[derive(Debug)]
pub struct Builds {
    config: BuildsConfig,
    records: HashMap<BuildKey, Record>,
}

impl Builds {
    /// An empty store shaped by `config`.
    #[must_use]
    pub fn new(config: &BuildsConfig) -> Self {
        Self {
            config: config.clone(),
            records: HashMap::with_capacity(config.max_live.get()),
        }
    }

    fn record(&mut self, at: Timestamp, key: &BuildKey) -> &mut Record {
        if !self.records.contains_key(key)
            && self.records.len() == self.config.max_live.get()
            && let Some(oldest) = self
                .records
                .iter()
                .min_by_key(|(_, record)| (record.build.last_active_time, record.id))
                .map(|(key, _)| key.clone())
        {
            self.records.remove(&oldest);
        }
        self.records
            .entry(key.clone())
            .or_insert_with(|| Record::new(at, key, &self.config))
    }

    /// Folds an observation made at `at` into its build.
    pub fn observe(&mut self, at: Timestamp, observation: &Observation) {
        self.record(at, &observation.origin().build)
            .observe(at, observation);
    }

    /// Counts observations lost for a build at `at`.
    pub fn lose(&mut self, at: Timestamp, key: &BuildKey, count: u64) {
        let record = self.record(at, key);
        record.touch(at);
        record.build.unrecorded += count;
    }

    /// Returns the full view of a build as of `now`.
    #[must_use]
    pub fn get(&self, id: BuildId, now: Timestamp) -> Option<Build> {
        let record = self.records.values().find(|record| record.id == id)?;
        let mut build = record.view();
        build.state = self.state(build.last_active_time, now);
        Some(build)
    }

    /// Returns matching basic views, latest activity first and ties by name.
    #[must_use]
    pub fn list(&self, filter: &BuildFilter, now: Timestamp) -> Vec<Build> {
        let mut builds = Vec::with_capacity(self.records.len());
        for record in self
            .records
            .values()
            .filter(|record| filter.matches(&record.build))
        {
            let mut build = Build::default();
            self.copy_basic(&record.build, now, &mut build);
            builds.push(build);
        }
        builds.sort_by(|a, b| {
            b.last_active_time
                .cmp(&a.last_active_time)
                .then_with(|| a.name.cmp(&b.name))
        });
        builds
    }

    /// Overwrites `into` with recent basic views ordered by name, reusing row buffers.
    pub fn fill_recent(&self, since: Timestamp, now: Timestamp, into: &mut Vec<Build>) {
        let mut len = 0;
        for record in self
            .records
            .values()
            .filter(|record| record.build.last_active_time >= since)
        {
            if len == into.len() {
                into.push(Build::default());
            }
            self.copy_basic(&record.build, now, &mut into[len]);
            len += 1;
        }
        into.truncate(len);
        into.sort_by(|a, b| a.name.cmp(&b.name));
    }

    fn state(&self, last: Timestamp, now: Timestamp) -> BuildState {
        if now.saturating_duration_since(last) < self.config.idle_after.get() {
            BuildState::Active
        } else {
            BuildState::Idle
        }
    }

    fn copy_basic(&self, from: &Build, now: Timestamp, to: &mut Build) {
        to.name.clone_from(&from.name);
        to.principal.clone_from(&from.principal);
        to.invocation_id.clone_from(&from.invocation_id);
        to.correlated_invocations_id
            .clone_from(&from.correlated_invocations_id);
        to.tool.clone_from(&from.tool);
        to.state = self.state(from.last_active_time, now);
        to.start_time = from.start_time;
        to.last_active_time = from.last_active_time;
        to.usage.clone_from(&from.usage);
        to.unrecorded = from.unrecorded;
        to.mnemonics.clear();
        to.other_mnemonics = None;
        to.missed_targets.clear();
        to.slowest.clear();
        to.timeline = None;
    }
}
