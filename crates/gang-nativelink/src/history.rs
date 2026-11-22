//! Stable, bounded snapshots of the scheduler's learned measurements.
//!
//! Current resource envelopes and IDs use their own versioned namespace. Authentic
//! legacy rollback records retain their original bytes and expiry independently;
//! they only seed predictive aggregates when no current logical key was restored.
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::Arc;

use gang_clock::Timestamp;
use gang_spi::{LearnedHistory, PortError, RecordId, RecordKey, RecordKind};
use nativelink_scheduler::resource_sketch::{
    ResourceSketch, SAMPLER_GENERATION, SketchKey, Snapshot,
};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use crate::durations::ActionDurations;

const VERSION: u8 = 2;
const LEGACY_VERSION: u8 = 1;
const LEGACY_SNAPSHOT_FORMAT: u8 = 3;
const CURRENT_SNAPSHOT_FORMAT: u8 = 4;
// Both generations can contain aggregate histories without immutable action identity.
const LEGACY_RESOURCE_SAMPLER_GENERATIONS: [u32; 2] = [2, 3];

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
    resource_key_with(key, |tag, parts| {
        let mut hash = Sha256::new();
        hash.update(b"gang-resource-history-key-v2\0");
        hash.update([tag]);
        for part in parts {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        RecordId::new(hash.finalize().into())
    })
}

fn legacy_resource_key(key: &SketchKey) -> Option<RecordKey> {
    if matches!(key, SketchKey::Action { .. }) {
        return None;
    }
    Some(resource_key_with(key, history_id))
}

fn resource_key_with(key: &SketchKey, id: fn(u8, &[&str]) -> RecordId) -> RecordKey {
    let id = match key {
        SketchKey::Target {
            target,
            mnemonic: Some(mnemonic),
        } => id(0, &[target, mnemonic]),
        SketchKey::Target {
            target,
            mnemonic: None,
        } => id(1, &[target]),
        SketchKey::Mnemonic { mnemonic } => id(2, &[mnemonic]),
        SketchKey::Command { digest } => id(3, &[digest]),
        SketchKey::Action { identity } => id(4, &[identity]),
    };
    RecordKey {
        kind: RecordKind::ResourceHistory,
        id,
    }
}

pub(crate) struct Histories {
    pub(crate) resource: Arc<ResourceSketch>,
    pub(crate) durations: Arc<Mutex<ActionDurations>>,
    // Original rollback records are immutable: never re-encode or refresh their expiry.
    legacy_resources: Mutex<Vec<LearnedHistory>>,
}

impl Histories {
    pub(crate) fn new(
        resource: Arc<ResourceSketch>,
        durations: Arc<Mutex<ActionDurations>>,
    ) -> Self {
        Self {
            resource,
            durations,
            legacy_resources: Mutex::new(Vec::new()),
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
                body: body.into(),
                expires_at: Timestamp::from_unix_millis(expires),
            });
        }
        self.durations.lock().snapshot(now, into)?;
        let mut originals = self.legacy_resources.lock();
        originals.retain(|record| record.expires_at > now);
        into.extend(originals.iter().cloned());
        Ok(())
    }

    pub(crate) fn restore(
        &self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), PortError> {
        let window = self.resource.window().as_secs();
        let mut entries = HashMap::new();
        let mut originals = Vec::new();
        let mut original_keys = HashSet::new();
        for record in records {
            if record.key.kind != RecordKind::ResourceHistory
                || record.expires_at <= now
                || record.body.len() < 9
                || !matches!(record.body[0], VERSION | LEGACY_VERSION)
                || record.body[1..9] != window.to_be_bytes()
            {
                continue;
            }
            let Ok(mut snapshot) = Snapshot::decode(&record.body[9..], 1) else {
                continue;
            };
            if snapshot.entries.len() != 1 {
                continue;
            }
            let (key, entry) = snapshot.entries.remove(0);
            let current = record.body[0] == VERSION;
            let valid_namespace = if current {
                snapshot.sampler_generation == SAMPLER_GENERATION
                    && record.body[13] == CURRENT_SNAPSHOT_FORMAT
                    && resource_key(&key) == record.key
            } else {
                LEGACY_RESOURCE_SAMPLER_GENERATIONS.contains(&snapshot.sampler_generation)
                    && legacy_resource_key(&key).as_ref() == Some(&record.key)
            };
            if !valid_namespace {
                continue;
            }
            if entry.seeded
                || entry.last_seen_s > now.as_unix_millis() / 1000
                || snapshot.written_at_s != entry.last_seen_s
                || entry
                    .last_seen_s
                    .saturating_add(window)
                    .saturating_mul(1000)
                    != record.expires_at.as_unix_millis()
            {
                continue;
            }
            if !current
                && snapshot.sampler_generation == 2
                && record.body[13] == LEGACY_SNAPSHOT_FORMAT
                && original_keys.insert(key.clone())
            {
                originals.push(record.clone());
            }
            if !current && matches!(&key, SketchKey::Mnemonic { .. }) {
                continue;
            }
            match entries.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert((current, entry));
                }
                Entry::Occupied(mut slot) => {
                    let (was_current, previous) = slot.get();
                    if (current && !*was_current)
                        || (current == *was_current && entry.last_seen_s > previous.last_seen_s)
                    {
                        slot.insert((current, entry));
                    }
                }
            }
        }
        *self.legacy_resources.lock() = originals;
        let mut entries: Vec<_> = entries
            .into_iter()
            .map(|(key, (_, entry))| (key, entry))
            .collect();
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
    use nativelink_config::schedulers::CpuReservationPercentile;
    use nativelink_scheduler::resource_sketch::{Budget, HistoryScope, Observation, Policy};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn histories(window_s: u64, capacity: usize) -> Histories {
        let resource = ResourceSketch::with_shards(
            Policy {
                headroom_percent: 20,
                cpu_reservation_percentile: CpuReservationPercentile::P95,
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
                completed: true,
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

    fn set_generation(record: &mut LearnedHistory, generation: u32) {
        // The outer record prefix is nine bytes; the snapshot generation
        // follows its four-byte magic and one-byte version.
        Arc::make_mut(&mut record.body)[14..18].copy_from_slice(&generation.to_le_bytes());
        assert_eq!(
            Snapshot::decode(&record.body[9..], 1)
                .unwrap()
                .sampler_generation,
            generation
        );
    }

    fn rewrite_snapshot(record: &mut LearnedHistory, snapshot: &Snapshot, version: u8) {
        let mut body = record.body[..9].to_vec();
        body[0] = version;
        body.extend(snapshot.encode());
        if version == LEGACY_VERSION {
            // Target, Command and Mnemonic have identical entry layouts in formats 3 and 4.
            body[13] = LEGACY_SNAPSHOT_FORMAT;
            record.key = legacy_resource_key(&snapshot.entries[0].0).unwrap();
        }
        record.body = body.into();
    }

    fn legacy_records(window_s: u64) -> Vec<LearnedHistory> {
        let source = histories(window_s, 8);
        let observation = Observation {
            now_s: 10,
            peak_memory_kb: 100_000,
            completed: true,
            ..Observation::default()
        };
        for key in [
            SketchKey::for_attempt(Some("target"), Some("compile"), ""),
            SketchKey::for_attempt(Some("plain-target"), None, ""),
            SketchKey::for_attempt(None, None, "digest"),
        ] {
            source.resource.observe(key.clone(), &observation);
            source.resource.observe(key, &observation);
        }
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records).unwrap();
        for record in &mut records {
            let mut snapshot = Snapshot::decode(&record.body[9..], 1).unwrap();
            snapshot.sampler_generation = 2;
            rewrite_snapshot(record, &snapshot, LEGACY_VERSION);
        }
        records
    }

    #[test]
    fn rollback_originals_survive_repeated_flushes_restarts_and_expire_unchanged() {
        let originals = legacy_records(100);
        assert_eq!(originals.len(), 4);
        let target = SketchKey::for_attempt(Some("target"), Some("compile"), "");
        let mnemonic = SketchKey::for_attempt(None, Some("compile"), "");
        let action = SketchKey::Action {
            identity: Arc::from("action"),
        };
        let restored = histories(100, 8);
        restored.restore(time(11), &originals).unwrap();
        assert!(restored.resource.lookup(&mnemonic, 11).is_none());
        assert!(restored.resource.lookup(&action, 11).is_none());
        restored.resource.observe(target.clone(), &Observation {
            now_s: 20,
            peak_memory_kb: 100,
            completed: true,
            ..Observation::default()
        });
        restored.resource.observe(action.clone(), &Observation {
            now_s: 20,
            peak_memory_kb: 80_000,
            killed: true,
            killed_peak_memory_kb: Some(80_000),
            ..Observation::default()
        });
        let mut records = Vec::new();
        for now in [21, 41, 71] {
            restored.snapshot(time(now), &mut records).unwrap();
            for original in &originals {
                let retained: Vec<_> = records.iter()
                    .filter(|record| record.key == original.key).collect();
                assert_eq!(retained.len(), 1);
                assert_eq!(retained[0], original);
                assert!(Arc::ptr_eq(&retained[0].body, &original.body));
            }
            // A generation-2/format-3 reader can only see the original namespace.
            let rollback: Vec<_> = records.iter().filter(|record| {
                record.body[0] == LEGACY_VERSION
                    && record.body[13] == LEGACY_SNAPSHOT_FORMAT
                    && Snapshot::decode(&record.body[9..], 1).unwrap().sampler_generation == 2
            }).collect();
            assert_eq!(rollback.len(), originals.len());
            for current in records.iter().filter(|record| record.body[0] == VERSION) {
                let snapshot = Snapshot::decode(&current.body[9..], 1).unwrap();
                assert_eq!(snapshot.sampler_generation, SAMPLER_GENERATION);
                assert_eq!(current.key, resource_key(&snapshot.entries[0].0));
                assert!(originals.iter().all(|original| original.key != current.key));
            }
        }
        let restarted = histories(100, 8);
        for _ in 0..2 {
            restarted.restore(time(71), &records).unwrap();
        }
        let mut after_restart = Vec::new();
        restarted.snapshot(time(71), &mut after_restart).unwrap();
        assert_eq!(after_restart.len(), records.len());
        for record in &records {
            let same = after_restart.iter().find(|next| next.key == record.key).unwrap();
            assert_eq!(same.expires_at, record.expires_at);
            if record.body[0] == LEGACY_VERSION {
                assert_eq!(same, record);
                assert!(Arc::ptr_eq(&same.body, &record.body));
            }
        }
        let action_entry = restarted.resource.lookup(&action, 71).unwrap();
        assert_eq!(action_entry.killed_peak_memory_kb, 80_000);
        assert_eq!(restarted.resource.lookup(&mnemonic, 71).unwrap().max_memory_kb(), 100);
        restarted.snapshot(time(110), &mut after_restart).unwrap();
        assert!(after_restart.iter().all(|record| record.body[0] == VERSION));
        assert!(after_restart.iter().any(|record| record.key == resource_key(&target)));
        assert!(after_restart.iter().any(|record| record.key == resource_key(&action)));
        assert!(restarted.legacy_resources.lock().is_empty());

        // A replacement restore must discard the previous backup set too.
        restarted.restore(time(71), &originals).unwrap();
        restarted.restore(time(71), &[]).unwrap();
        restarted.snapshot(time(71), &mut after_restart).unwrap();
        assert!(after_restart.is_empty());
    }

    #[test]
    fn current_lower_aggregate_wins_over_legacy_in_either_restore_order() {
        let target = SketchKey::for_attempt(Some("target"), Some("compile"), "");
        let source = histories(100, 8);
        source.resource.observe(target.clone(), &Observation {
            now_s: 20,
            peak_memory_kb: 100,
            completed: true,
            ..Observation::default()
        });
        let mut current = Vec::new();
        source.snapshot(time(21), &mut current).unwrap();
        let originals = legacy_records(100);
        for reverse in [false, true] {
            let mut mixed = originals.clone();
            mixed.extend(current.iter().cloned());
            mixed.push(originals[0].clone());
            if reverse {
                mixed.reverse();
            }
            let restored = histories(100, 8);
            restored.restore(time(21), &mixed).unwrap();
            let expected = source.resource.lookup(&target, 21).unwrap();
            assert_eq!(restored.resource.lookup(&target, 21), Some(expected.clone()));
            let mut persisted = Vec::new();
            restored.snapshot(time(21), &mut persisted).unwrap();
            assert_eq!(persisted.iter().filter(|record| record.body[0] == LEGACY_VERSION).count(), originals.len());
            let restarted = histories(100, 8);
            restarted.restore(time(21), &persisted).unwrap();
            assert_eq!(restarted.resource.lookup(&target, 21), Some(expected));
        }
    }

    #[test]
    fn rollback_cache_rejects_corruption_and_namespace_sampler_spoofing() {
        let target = SketchKey::for_attempt(Some("target"), Some("compile"), "");
        let originals = legacy_records(100);
        let original = originals.iter()
            .find(|record| Some(&record.key) == legacy_resource_key(&target).as_ref()).unwrap();
        let mut invalid = Vec::new();
        for offset in [0, 1, 9, 13, 14] {
            let mut record = original.clone();
            Arc::make_mut(&mut record.body)[offset] ^= 0xff;
            invalid.push(record);
        }
        let mut wrong_key = original.clone();
        wrong_key.key = resource_key(&target);
        invalid.push(wrong_key);
        let mut wrong_expiry = original.clone();
        wrong_expiry.expires_at = time(111);
        invalid.push(wrong_expiry);
        for generation in [1, SAMPLER_GENERATION, 5] {
            let mut record = original.clone();
            set_generation(&mut record, generation);
            invalid.push(record);
        }
        let mut wrong_envelope = original.clone();
        Arc::make_mut(&mut wrong_envelope.body)[0] = VERSION;
        invalid.push(wrong_envelope);
        let mut wrong_sampler = original.clone();
        wrong_sampler.key = resource_key(&target);
        Arc::make_mut(&mut wrong_sampler.body)[0] = VERSION;
        Arc::make_mut(&mut wrong_sampler.body)[13] = CURRENT_SNAPSHOT_FORMAT;
        invalid.push(wrong_sampler);
        let mut wrong_format = original.clone();
        wrong_format.key = resource_key(&target);
        Arc::make_mut(&mut wrong_format.body)[0] = VERSION;
        set_generation(&mut wrong_format, SAMPLER_GENERATION);
        invalid.push(wrong_format);
        for mutation in 0..4 {
            let mut record = original.clone();
            let mut snapshot = Snapshot::decode(&record.body[9..], 1).unwrap();
            match mutation {
                0 => snapshot.written_at_s += 1,
                1 => snapshot.entries[0].1.seeded = true,
                2 => snapshot.entries.push(snapshot.entries[0].clone()),
                _ => snapshot.entries[0].0 = SketchKey::Action {
                    identity: Arc::from("spoofed-action"),
                },
            }
            if mutation == 3 {
                // An Action encoded under a legacy key must not enter either history.
                let mut body = record.body[..9].to_vec();
                body.extend(snapshot.encode());
                body[13] = LEGACY_SNAPSHOT_FORMAT;
                record.body = body.into();
            } else {
                rewrite_snapshot(&mut record, &snapshot, LEGACY_VERSION);
            }
            invalid.push(record);
        }
        let restored = histories(100, 8);
        for record in invalid {
            restored.restore(time(11), &[record]).unwrap();
            let mut output = Vec::new();
            restored.snapshot(time(11), &mut output).unwrap();
            assert!(output.is_empty());
        }
        restored.restore(time(110), &originals).unwrap();
        let mut output = Vec::new();
        restored.snapshot(time(110), &mut output).unwrap();
        assert!(output.is_empty());
        let other_window = histories(200, 8);
        other_window.restore(time(11), &originals).unwrap();
        other_window.snapshot(time(11), &mut output).unwrap();
        assert!(output.is_empty());

        // A readable migration payload is not necessarily readable by the rollback binary.
        let mut modern_format = original.clone();
        Arc::make_mut(&mut modern_format.body)[13] = CURRENT_SNAPSHOT_FORMAT;
        restored.restore(time(11), &[modern_format]).unwrap();
        assert!(restored.resource.lookup(&target, 11).is_some());
        restored.snapshot(time(11), &mut output).unwrap();
        assert!(output.iter().all(|record| record.body[0] == VERSION));
    }

    #[test]
    fn migrates_only_unpolluted_legacy_resource_keys() {
        assert_eq!(SAMPLER_GENERATION, 4);
        let source = histories(86_400, 8);
        let target = SketchKey::for_attempt(Some("target"), Some("compile"), "");
        let mnemonic = SketchKey::Mnemonic {
            mnemonic: Arc::from("compile"),
        };
        let command = SketchKey::for_attempt(None, None, "digest");
        let low = Observation {
            now_s: 10,
            peak_memory_kb: 10_000,
            completed: true,
            ..Observation::default()
        };
        for _ in 0..20 {
            assert!(source.resource.observe(target.clone(), &low));
        }
        assert!(source.resource.observe(
            target.clone(),
            &Observation { peak_memory_kb: 200_000, ..low },
        ));
        assert!(!source.resource.observe(command.clone(), &low));
        assert!(source.resource.observe(command.clone(), &low));
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records).unwrap();
        // Encode real legacy-format aggregate entries, including historical kill data.
        // The current sampler no longer accepts killed aggregate observations.
        for record in &mut records {
            let mut legacy = Snapshot::decode(&record.body[9..], 1).unwrap();
            legacy.entries[0].1.killed_peak_memory_kb = 40_000;
            legacy.entries[0].1.killed_memory_s = 10;
            legacy.sampler_generation = 2;
            rewrite_snapshot(record, &legacy, LEGACY_VERSION);
        }
        let target_entry = Snapshot::decode(
            &records.iter().find(|record| Some(&record.key) == legacy_resource_key(&target).as_ref()).unwrap().body[9..],
            1,
        ).unwrap().entries.remove(0).1;
        let command_entry = Snapshot::decode(
            &records.iter().find(|record| Some(&record.key) == legacy_resource_key(&command).as_ref()).unwrap().body[9..],
            1,
        ).unwrap().entries.remove(0).1;
        assert_eq!(target_entry.current.max_memory_kb, 200_000);
        assert_eq!(target_entry.killed_peak_memory_kb, 40_000);
        assert_eq!(command_entry.killed_peak_memory_kb, 40_000);
        let mut polluted = records.iter().find(|record| Some(&record.key) == legacy_resource_key(&target).as_ref()).unwrap().clone();
        let mut legacy = Snapshot::decode(&polluted.body[9..], 1).unwrap();
        legacy.entries[0].0 = mnemonic.clone();
        rewrite_snapshot(&mut polluted, &legacy, LEGACY_VERSION);
        records.retain(|record| Some(&record.key) != legacy_resource_key(&mnemonic).as_ref());
        records.push(polluted);
        for generation in [2, 3] {
            for record in &mut records {
                set_generation(record, generation);
            }
            let restored = histories(86_400, 8);
            restored.restore(time(11), &records).unwrap();
            assert_eq!(restored.resource.lookup(&target, 11), Some(target_entry.clone()));
            assert_eq!(restored.resource.lookup(&command, 11), Some(command_entry.clone()));
            assert!(restored.resource.lookup(&mnemonic, 11).is_none());
            assert_eq!(restored.resource.shape(&target_entry, HistoryScope::Aggregate).memory_kb, 19_200);
        }
        let restored = histories(86_400, 8);
        restored.restore(time(11), &records).unwrap();
        let migrated_target = restored.resource.lookup(&target, 11).unwrap();
        assert_eq!(migrated_target, target_entry);
        assert_eq!(restored.resource.shape(&migrated_target, HistoryScope::Aggregate).memory_kb, 19_200);
        assert_eq!(
            restored.resource.lookup(&command, 11).unwrap(),
            command_entry
        );
        assert!(restored.resource.lookup(&mnemonic, 11).is_none());

        // Generation 3 migrates predictively, but is not a rollback backup for generation 2.
        let mut migrated = Vec::new();
        restored.snapshot(time(11), &mut migrated).unwrap();
        assert!(migrated.iter().all(|record| record.body[0] == VERSION));
        assert!(migrated.iter().all(|record| {
            Snapshot::decode(&record.body[9..], 1)
                .unwrap()
                .sampler_generation
                == SAMPLER_GENERATION
        }));

        let target_record = records
            .iter()
            .find(|record| Some(&record.key) == legacy_resource_key(&target).as_ref())
            .unwrap();
        let mut wrong_key = target_record.clone();
        wrong_key.key = legacy_resource_key(&command).unwrap();
        let mut wrong_expiry = target_record.clone();
        wrong_expiry.expires_at = time(50_000);
        let mut wrong_body = target_record.clone();
        let mut snapshot = Snapshot::decode(&wrong_body.body[9..], 1).unwrap();
        snapshot.written_at_s += 1;
        rewrite_snapshot(&mut wrong_body, &snapshot, LEGACY_VERSION);
        for invalid in [wrong_key, wrong_expiry, wrong_body] {
            restored.restore(time(11), &[invalid]).unwrap();
            assert!(restored.resource.lookup(&target, 11).is_none());
        }

        for generation in [1, 5] {
            let mut incompatible = records.clone();
            for record in &mut incompatible {
                set_generation(record, generation);
            }
            restored.restore(time(11), &incompatible).unwrap();
            assert!(restored.resource.lookup(&target, 11).is_none());
            assert!(restored.resource.lookup(&command, 11).is_none());
            assert!(restored.resource.lookup(&mnemonic, 11).is_none());
        }
    }

    #[test]
    fn action_history_round_trips_without_colliding_with_aggregate_keys() {
        let source = histories(86_400, 8);
        let action = SketchKey::Action { identity: Arc::from("same-value") };
        let command = SketchKey::Command { digest: Arc::from("same-value") };
        let target = SketchKey::for_attempt(Some("target"), Some("compile"), "");
        let mnemonic = SketchKey::for_attempt(None, Some("compile"), "");
        assert_ne!(resource_key(&action), resource_key(&command));
        assert!(source.resource.observe(action.clone(), &Observation {
            now_s: 10,
            peak_memory_kb: 80_000,
            killed: true,
            killed_peak_memory_kb: Some(80_000),
            ..Observation::default()
        }));
        assert!(source.resource.lookup(&target, 10).is_none());
        assert!(source.resource.lookup(&mnemonic, 10).is_none());
        assert!(source.resource.observe(target.clone(), &Observation {
            now_s: 10,
            peak_memory_kb: 10_000,
            completed: true,
            ..Observation::default()
        }));
        assert!(source.resource.lookup(&mnemonic, 10).is_some());
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records).unwrap();
        let restored = histories(86_400, 8);
        restored.restore(time(11), &records).unwrap();
        let entry = restored.resource.lookup(&action, 11).unwrap();
        assert_eq!(entry.killed_peak_memory_kb, 80_000);
        assert_eq!(restored.resource.shape(&entry, HistoryScope::Action).memory_kb, 96_000);
        assert_eq!(restored.resource.lookup(&target, 11).unwrap().max_memory_kb(), 10_000);
        assert_eq!(restored.resource.lookup(&mnemonic, 11).unwrap().max_memory_kb(), 10_000);
        for generation in [2, 3] {
            let mut legacy_action = records.iter()
                .find(|record| record.key == resource_key(&action)).unwrap().clone();
            set_generation(&mut legacy_action, generation);
            restored.restore(time(11), &[legacy_action]).unwrap();
            assert!(restored.resource.lookup(&action, 11).is_none());
        }
    }

    #[test]
    fn current_mnemonic_round_trips_and_other_generations_are_rejected() {
        assert_eq!(SAMPLER_GENERATION, 4);
        let source = histories(86_400, 4);
        let target = SketchKey::for_attempt(Some("target"), Some("compile"), "");
        let mnemonic = SketchKey::Mnemonic {
            mnemonic: Arc::from("compile"),
        };
        assert!(source.resource.observe(
            target,
            &Observation {
                now_s: 10,
                peak_memory_kb: 123_456,
                completed: true,
                ..Observation::default()
            },
        ));
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records).unwrap();
        let restored = histories(86_400, 4);
        restored.restore(time(11), &records).unwrap();
        let expected = source.resource.lookup(&mnemonic, 11).unwrap();
        assert_eq!(expected.current.max_memory_kb, 123_456);
        assert_eq!(restored.resource.lookup(&mnemonic, 11).unwrap(), expected);
        let mnemonic_record = records
            .iter()
            .find(|record| record.key == resource_key(&mnemonic))
            .unwrap();
        for generation in [1, 5] {
            let mut incompatible = mnemonic_record.clone();
            set_generation(&mut incompatible, generation);
            restored.restore(time(11), &[incompatible]).unwrap();
            assert!(restored.resource.lookup(&mnemonic, 11).is_none());
        }
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
                completed: true,
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
            Arc::make_mut(&mut broken[0].body)[offset] ^= 0xff;
            let restored = histories(86_400, 4);
            restored.restore(time(11), &broken).unwrap();
            assert!(restored.resource.lookup(&key, 11).is_none());
        }
    }
}
