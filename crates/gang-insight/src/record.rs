use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::BuildsConfig;
use gang_spi::{CallOutcome, Observation, Phase, Rpc};
use gang_types::{BuildId, BuildKey};
use gang_wire::{Build, MissedTarget, MnemonicUsage, SlowAction, Usage, name};

use crate::timeline::History;

#[derive(Debug)]
pub(crate) struct Record {
    pub(crate) id: BuildId,
    pub(crate) build: Build,
    history: History,
    target_order: Vec<u64>,
    next_order: u64,
    mnemonic_cap: usize,
    target_cap: usize,
    slow_cap: usize,
}

impl Record {
    pub(crate) fn new(at: Timestamp, key: &BuildKey, config: &BuildsConfig) -> Self {
        let mnemonic_cap = config.mnemonics.get();
        let target_cap = config.targets.get();
        let slow_cap = config.slowest_actions.get();
        let id = key.id();
        Self {
            id,
            build: Build {
                name: name::of::<name::Builds>(&id),
                principal: name::of::<name::Principals>(&key.principal),
                invocation_id: key.invocation.to_string(),
                start_time: at,
                last_active_time: at,
                mnemonics: Vec::with_capacity(mnemonic_cap),
                missed_targets: Vec::with_capacity(target_cap),
                slowest: Vec::with_capacity(slow_cap),
                ..Build::default()
            },
            history: History::new(at, config.timeline_buckets.get()),
            target_order: Vec::with_capacity(target_cap),
            next_order: 0,
            mnemonic_cap,
            target_cap,
            slow_cap,
        }
    }

    pub(crate) fn touch(&mut self, at: Timestamp) {
        self.build.last_active_time = self.build.last_active_time.max(at);
        self.history.advance(at);
    }

    pub(crate) fn view(&self) -> Build {
        let mut build = self.build.clone();
        build.timeline = Some(self.history.timeline.clone());
        build.missed_targets.sort_by(|a, b| {
            b.misses
                .cmp(&a.misses)
                .then_with(|| a.target.cmp(&b.target))
        });
        build
    }

    pub(crate) fn observe(&mut self, at: Timestamp, observation: &Observation) {
        self.touch(at);
        let origin = observation.origin();
        if self.build.correlated_invocations_id.is_empty()
            && let Some(value) = &origin.correlated
        {
            self.build.correlated_invocations_id = value.to_string();
        }
        if self.build.tool.is_empty()
            && let Some(value) = &origin.tool
        {
            self.build.tool = value.to_string();
        }
        fold_usage(&mut self.build.usage, observation);
        let row = origin.mnemonic.as_ref().and_then(|mnemonic| {
            let value = mnemonic.as_str();
            if let Some(index) = self
                .build
                .mnemonics
                .iter()
                .position(|row| row.mnemonic == value)
            {
                Some(index)
            } else if self.build.mnemonics.len() < self.mnemonic_cap {
                self.build.mnemonics.push(MnemonicUsage {
                    mnemonic: value.to_owned(),
                    usage: Usage::default(),
                });
                Some(self.build.mnemonics.len() - 1)
            } else {
                None
            }
        });
        if let Some(index) = row {
            fold_usage(&mut self.build.mnemonics[index].usage, observation);
        } else {
            fold_usage(
                self.build
                    .other_mnemonics
                    .get_or_insert_with(Usage::default),
                observation,
            );
        }
        match observation {
            Observation::Call(call) if call.rpc == Rpc::GetActionResult => match call.outcome {
                CallOutcome::Ok => self.history.point(at, true),
                CallOutcome::NotFound => {
                    self.history.point(at, false);
                    if let Some(target) = &origin.target {
                        self.miss(target.as_str());
                    }
                }
                CallOutcome::Error => {}
            },
            Observation::Executed(executed) => {
                let times = &executed.times;
                if let Some(phase) = times.queued {
                    self.history.interval(phase.start, phase.end, at, true);
                }
                if let Some(phase) = times.executing {
                    self.history.interval(phase.start, phase.end, at, false);
                }
                let Some(whole) = times.whole else {
                    return;
                };
                let span = whole.duration();
                let pos = self.build.slowest.partition_point(|row| row.span >= span);
                if pos < self.slow_cap {
                    let action = SlowAction {
                        target: origin
                            .target
                            .as_ref()
                            .map_or_else(String::new, ToString::to_string),
                        mnemonic: origin
                            .mnemonic
                            .as_ref()
                            .map_or_else(String::new, ToString::to_string),
                        digest: executed.digest.clone(),
                        worker: executed
                            .worker
                            .as_ref()
                            .map_or_else(String::new, name::of::<name::Workers>),
                        queue_time: whole.start,
                        span,
                        queued: phase_duration(times.queued),
                        input_fetch: phase_duration(times.input_fetch),
                        executing: phase_duration(times.executing),
                        output_upload: phase_duration(times.output_upload),
                        exit_code: executed.exit_code,
                    };
                    if self.build.slowest.len() == self.slow_cap {
                        self.build.slowest.pop();
                    }
                    self.build.slowest.insert(pos, action);
                }
            }
            Observation::Call(_) => {}
        }
    }

    fn miss(&mut self, target: &str) {
        if let Some(row) = self
            .build
            .missed_targets
            .iter_mut()
            .find(|row| row.target == target)
        {
            row.misses += 1;
            return;
        }
        let order = self.next_order;
        self.next_order = self.next_order.saturating_add(1);
        if self.build.missed_targets.len() < self.target_cap {
            self.build.missed_targets.push(MissedTarget {
                target: target.to_owned(),
                misses: 1,
                overestimate: 0,
            });
            self.target_order.push(order);
        } else if let Some(index) = (0..self.build.missed_targets.len())
            .min_by_key(|&i| (self.build.missed_targets[i].misses, self.target_order[i]))
        {
            let previous = self.build.missed_targets[index].misses;
            self.build.missed_targets[index] = MissedTarget {
                target: target.to_owned(),
                misses: previous + 1,
                overestimate: previous,
            };
            self.target_order[index] = order;
        }
    }
}

fn phase_duration(phase: Option<Phase>) -> Duration {
    phase.map_or(Duration::ZERO, |phase| phase.duration())
}

fn fold_usage(usage: &mut Usage, observation: &Observation) {
    match observation {
        Observation::Call(call) => match (call.rpc, call.outcome) {
            (Rpc::GetActionResult, CallOutcome::Ok) => usage.cache.hits += 1,
            (Rpc::GetActionResult, CallOutcome::NotFound) => usage.cache.misses += 1,
            (Rpc::GetActionResult, CallOutcome::Error) => usage.cache.errors += 1,
            (Rpc::UpdateActionResult, CallOutcome::Ok) => usage.cache.uploaded_results += 1,
            (Rpc::Execute, CallOutcome::Ok) => usage.executions.execute_calls += 1,
            (Rpc::Execute, _) => usage.executions.rejected += 1,
            (Rpc::UpdateActionResult, _) => {}
        },
        Observation::Executed(executed) => {
            if executed.exit_code == 0 && !executed.error {
                usage.executions.completed += 1;
            } else {
                usage.executions.failed += 1;
            }
            let t = &executed.times;
            usage.action_time.queued += phase_duration(t.queued);
            usage.action_time.input_fetch += phase_duration(t.input_fetch);
            usage.action_time.executing += phase_duration(t.executing);
            usage.action_time.output_upload += phase_duration(t.output_upload);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use gang_spi::{Call, Executed, ExecutionTimes, Origin};

    #[test]
    fn flood_preserves_record_vector_capacities() {
        let config = BuildsConfig::default();
        let at = Timestamp::from_unix_millis(100_000);
        let mut origin = Origin {
            build: BuildKey {
                principal: Arc::new("example-idp:alice".parse().unwrap()),
                invocation: "inv".parse().unwrap(),
            },
            correlated: None,
            tool: None,
            target: None,
            mnemonic: None,
        };
        let mut record = Record::new(Timestamp::UNIX_EPOCH, &origin.build, &config);
        for i in 0..10_000 {
            origin.target = Some(format!("//pkg:t{i}").parse().unwrap());
            origin.mnemonic = Some(format!("m{i}").parse().unwrap());
            let call = Observation::Call(Call {
                origin: origin.clone(),
                rpc: Rpc::GetActionResult,
                outcome: CallOutcome::NotFound,
            });
            record.observe(at, &call);
            let times = ExecutionTimes {
                queued: Some(Phase {
                    start: Timestamp::UNIX_EPOCH,
                    end: at,
                }),
                input_fetch: None,
                executing: None,
                output_upload: None,
                whole: Some(Phase {
                    start: Timestamp::UNIX_EPOCH,
                    end: at,
                }),
            };
            record.observe(
                at,
                &Observation::Executed(Executed {
                    origin: origin.clone(),
                    digest: i.to_string(),
                    worker: None,
                    exit_code: 0,
                    error: false,
                    times,
                }),
            );
        }
        assert!(record.build.mnemonics.capacity() <= config.mnemonics.get());
        assert!(record.build.missed_targets.capacity() <= config.targets.get());
        assert!(record.build.slowest.capacity() <= config.slowest_actions.get());
        assert!(record.target_order.capacity() <= config.targets.get());
        assert!(record.history.timeline.buckets.capacity() <= config.timeline_buckets.get());
    }
}
