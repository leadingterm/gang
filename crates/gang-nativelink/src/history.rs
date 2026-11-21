//! Stable, bounded snapshots of the scheduler's learned measurements.
use std::sync::Arc;

use gang_clock::Timestamp;
use gang_spi::{LearnedHistory, PortError, RecordId, RecordKey, RecordKind};
use nativelink_scheduler::resource_sketch::{
    ResourceSketch, SAMPLER_GENERATION, SketchKey, Snapshot,
};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use crate::durations::ActionDurations;

const VERSION: u8 = 1;

pub(crate) fn history_id(tag: u8, parts: &[&str]) -> RecordId {
    let mut hash = Sha256::new();
    hash.update(b"gang-learned-history-key-v1\0");
    hash.update([tag]);
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    RecordId::new(hash.finalize().into())
}

fn resource_key(key: &SketchKey) -> RecordKey {
    let id = match key {
        SketchKey::Target {
            target,
            mnemonic: Some(mnemonic),
        } => history_id(0, &[target, mnemonic]),
        SketchKey::Target {
            target,
            mnemonic: None,
        } => history_id(1, &[target]),
        SketchKey::Mnemonic { mnemonic } => history_id(2, &[mnemonic]),
        SketchKey::Command { digest } => history_id(3, &[digest]),
    };
    RecordKey {
        kind: RecordKind::ResourceHistory,
        id,
    }
}

pub(crate) struct Histories {
    pub(crate) resource: Arc<ResourceSketch>,
    pub(crate) durations: Arc<Mutex<ActionDurations>>,
}

impl Histories {
    pub(crate) fn new(
        resource: Arc<ResourceSketch>,
        durations: Arc<Mutex<ActionDurations>>,
    ) -> Self {
        Self {
            resource,
            durations,
        }
    }

    pub(crate) fn snapshot(
        &self,
        now: Timestamp,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        into.clear();
        let window = self.resource.window().as_secs();
        let entries = self.resource.snapshot(now.as_unix_millis() / 1000).entries;
        for (key, entry) in entries {
            let expires = entry
                .last_seen_s
                .saturating_add(window)
                .saturating_mul(1000);
            if entry.seeded || expires <= now.as_unix_millis() {
                continue;
            }
            let record_key = resource_key(&key);
            let written_at_s = entry.last_seen_s;
            let snapshot = Snapshot {
                sampler_generation: SAMPLER_GENERATION,
                written_at_s,
                entries: vec![(key, entry)],
            };
            let mut body = snapshot.encode();
            let mut prefix = [0; 9];
            prefix[0] = VERSION;
            prefix[1..].copy_from_slice(&window.to_be_bytes());
            drop(body.splice(0..0, prefix));
            into.push(LearnedHistory {
                key: record_key,
                body,
                expires_at: Timestamp::from_unix_millis(expires),
            });
        }
        self.durations.lock().snapshot(now, into)
    }

    pub(crate) fn restore(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        let window = self.resource.window().as_secs();
        let mut entries = Vec::new();
        for record in records {
            if record.key.kind != RecordKind::ResourceHistory
                || record.expires_at <= now
                || record.body.len() < 9
                || record.body[0] != VERSION
                || record.body[1..9] != window.to_be_bytes()
            {
                continue;
            }
            let Ok(mut snapshot) = Snapshot::decode(&record.body[9..], 1) else {
                continue;
            };
            if snapshot.sampler_generation != SAMPLER_GENERATION || snapshot.entries.len() != 1 {
                continue;
            }
            let (key, entry) = snapshot.entries.remove(0);
            if entry.seeded
                || resource_key(&key) != record.key
                || snapshot.written_at_s != entry.last_seen_s
                || entry
                    .last_seen_s
                    .saturating_add(window)
                    .saturating_mul(1000)
                    != record.expires_at.as_unix_millis()
            {
                continue;
            }
            entries.push((key, entry));
        }
        self.resource.clear();
        entries.sort_by_key(|(_, entry)| entry.last_seen_s);
        let snapshot = Snapshot {
            sampler_generation: SAMPLER_GENERATION,
            written_at_s: 0,
            entries,
        };
        self.resource
            .load(snapshot, now.as_unix_millis() / 1000)
            .map_err(PortError::backend)?;
        self.resource.age_out(now.as_unix_millis() / 1000);
        self.durations.lock().restore(now, records);
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use nativelink_scheduler::resource_sketch::{Budget, Observation, Policy};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn histories(window_s: u64, capacity: usize) -> Histories {
        let resource = ResourceSketch::with_shards(
            Policy {
                headroom_percent: 0,
                cpu_reservation_percentile: 95,
                classes: vec![],
                reserve_cpu: true,
                reserve_memory: true,
                reserve_disk: true,
            },
            Budget {
                target: capacity,
                mnemonic: capacity,
                command: capacity,
                probation: capacity,
            },
            Duration::from_secs(window_s),
            1,
        );
        Histories::new(
            Arc::new(resource),
            Arc::new(Mutex::new(ActionDurations::new(
                NonZeroUsize::new(capacity).unwrap(),
                gang_config::HistoryWindow::from_units(1),
            ))),
        )
    }

    fn time(seconds: u64) -> Timestamp {
        Timestamp::from_unix_millis(seconds * 1000)
    }

    #[test]
    fn restart_preserves_samples_expiry_and_stable_payload() {
        let source = histories(86_400, 4);
        let key = SketchKey::for_attempt(Some("target"), None, "");
        source.resource.observe(
            key.clone(),
            &Observation {
                now_s: 10,
                peak_memory_kb: 100,
                ..Observation::default()
            },
        );
        source.durations.lock().record("", "compile", 123, time(10));
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records).unwrap();
        let mut next = Vec::new();
        source.snapshot(time(12), &mut next).unwrap();
        assert_eq!(records, next);
        let restored = histories(86_400, 4);
        restored.restore(time(12), &records).unwrap();
        assert_eq!(
            restored
                .resource
                .lookup(&key, 12)
                .unwrap()
                .current
                .max_memory_kb,
            100
        );
        assert_eq!(
            restored.durations.lock().expected("", "compile", time(12)),
            Some(Duration::from_millis(123))
        );
        restored.restore(time(86_410), &records).unwrap();
        assert!(restored.resource.lookup(&key, 86_410).is_none());
        assert_eq!(
            restored
                .durations
                .lock()
                .expected("", "compile", time(86_410)),
            None
        );
    }

    #[test]
    fn incompatible_and_malformed_resource_entries_are_discarded() {
        let source = histories(86_400, 4);
        let key = SketchKey::for_attempt(Some("target"), None, "");
        source.resource.observe(
            key.clone(),
            &Observation {
                now_s: 10,
                peak_memory_kb: 100,
                ..Observation::default()
            },
        );
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records).unwrap();
        let different_window = histories(172_800, 4);
        different_window.restore(time(11), &records).unwrap();
        assert!(different_window.resource.lookup(&key, 11).is_none());
        for offset in [0, 9, 14] {
            let mut broken = records.clone();
            broken[0].body[offset] ^= 0xff;
            let restored = histories(86_400, 4);
            restored.restore(time(11), &broken).unwrap();
            assert!(restored.resource.lookup(&key, 11).is_none());
        }
    }
}
