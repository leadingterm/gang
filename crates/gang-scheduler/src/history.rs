//! Stable, bounded v1 learned-history codec and key hashing.
//!
//! Produces v1 persistence records (`[VERSION=1][window_secs u64 BE][Snapshot::encode()]`) and
//! `SHA-256` keys (`gang-learned-history-key-v1\0`) byte-for-byte identical to the `NativeLink`
//! adapter so restarts and handovers preserve learned state seamlessly.

use std::num::NonZeroUsize;

use gang_clock::Timestamp;
use gang_spi::{LearnedHistory, RecordId, RecordKey, RecordKind};
use gang_types::Digest;

use crate::durations::ActionDurations;
use crate::sketch::{ResourceSketch, SAMPLER_GENERATION, SketchError, SketchKey, Snapshot};

const VERSION: u8 = 1;
const HISTORY_KEY_DOMAIN: &[u8] = b"gang-learned-history-key-v1\0";

// Hyper 1.12.0's HTTP/1 default read-buffer maximum bounds accepted request headers.
const MAX_REQUEST_HEADER_BYTES: usize = 8_192 + 4_096 * 100;
const DURATION_JSON_OVERHEAD: usize = 1_024;

/// Computes the canonical `SHA-256` record identifier for a tag and length-prefixed parts under
/// the `gang-learned-history-key-v1\0` domain.
#[must_use]
pub fn history_id(tag: u8, parts: &[&str]) -> RecordId {
    let body_len = parts.iter().map(|part| 8 + part.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(HISTORY_KEY_DOMAIN.len() + 1 + body_len);
    buf.extend_from_slice(HISTORY_KEY_DOMAIN);
    buf.push(tag);
    for part in parts {
        buf.extend_from_slice(&(part.len() as u64).to_be_bytes());
        buf.extend_from_slice(part.as_bytes());
    }
    RecordId::new(*Digest::from_bytes(&buf).hash())
}

/// Computes the canonical [`RecordKey`] for a [`SketchKey`].
#[must_use]
pub fn resource_key(key: &SketchKey) -> RecordKey {
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

/// Combined learned resource sketch and action duration state persisted by the controller.
#[derive(Debug)]
pub struct Histories {
    resource: ResourceSketch,
    durations: ActionDurations,
}

impl Histories {
    /// Creates a learned-history container from its resource sketch and duration estimator.
    #[must_use]
    pub const fn new(resource: ResourceSketch, durations: ActionDurations) -> Self {
        Self {
            resource,
            durations,
        }
    }

    /// Returns a shared reference to the [`ResourceSketch`].
    #[must_use]
    pub const fn resource(&self) -> &ResourceSketch {
        &self.resource
    }

    /// Returns a mutable reference to the [`ResourceSketch`].
    pub const fn resource_mut(&mut self) -> &mut ResourceSketch {
        &mut self.resource
    }

    /// Returns a shared reference to [`ActionDurations`].
    #[must_use]
    pub const fn durations(&self) -> &ActionDurations {
        &self.durations
    }

    /// Returns a mutable reference to [`ActionDurations`].
    pub const fn durations_mut(&mut self) -> &mut ActionDurations {
        &mut self.durations
    }

    /// Maximum encoded body size in bytes for one learned-history record.
    #[must_use]
    pub fn byte_limit(&self) -> NonZeroUsize {
        let resource = usize::try_from(self.resource.snapshot_byte_limit())
            .unwrap_or(usize::MAX)
            .saturating_add(9);
        let duration = MAX_REQUEST_HEADER_BYTES * 6 + DURATION_JSON_OVERHEAD;
        NonZeroUsize::new(resource.max(duration)).unwrap_or(NonZeroUsize::MIN)
    }

    /// Snapshots all live, non-seeded resource and duration records into `into`, clearing it
    /// first.
    pub fn snapshot(&self, now: Timestamp, into: &mut Vec<LearnedHistory>) {
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
            let payload = snapshot.encode();
            let mut body = Vec::with_capacity(9 + payload.len());
            body.push(VERSION);
            body.extend_from_slice(&window.to_be_bytes());
            body.extend_from_slice(&payload);
            into.push(LearnedHistory {
                key: record_key,
                body,
                expires_at: Timestamp::from_unix_millis(expires),
            });
        }
        self.durations.snapshot(now, into);
    }

    /// Restores compatible live resource and duration records, clearing prior entries.
    ///
    /// Incompatible, expired, seeded, or malformed records are silently discarded.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError`] if the resource sketch refuses to load the validated batch.
    pub fn restore(
        &mut self,
        now: Timestamp,
        records: &[LearnedHistory],
    ) -> Result<(), SketchError> {
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
        self.resource.load(snapshot, now.as_unix_millis() / 1000)?;
        self.resource.age_out(now.as_unix_millis() / 1000);
        self.durations.restore(now, records);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_config::HistoryWindow;
    use gang_types::Percent;

    use super::*;
    use crate::sketch::{Budget, Observation, Policy};

    fn histories(window_s: u64, capacity: usize) -> Histories {
        let resource = ResourceSketch::with_shards(
            Policy {
                headroom_percent: 0,
                cpu_reservation_percentile: Percent::from_percent(95),
                classes: Vec::new(),
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
            resource,
            ActionDurations::new(
                NonZeroUsize::new(capacity).expect("nonzero"),
                HistoryWindow::from_units(1),
            ),
        )
    }

    fn time(seconds: u64) -> Timestamp {
        Timestamp::from_unix_millis(seconds * 1000)
    }

    #[test]
    fn restart_preserves_samples_expiry_and_stable_payload() {
        let mut source = histories(86_400, 4);
        let key = SketchKey::for_attempt(Some("target"), None, "");
        source.resource_mut().observe(
            key.clone(),
            &Observation {
                now_s: 10,
                peak_memory_kb: 100,
                ..Observation::default()
            },
        );
        source.durations_mut().record("", "compile", 123, time(10));
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records);
        let mut next = Vec::new();
        source.snapshot(time(12), &mut next);
        assert_eq!(records, next);

        let mut restored = histories(86_400, 4);
        restored.restore(time(12), &records).expect("restore");
        assert_eq!(
            restored
                .resource_mut()
                .lookup(&key, 12)
                .expect("restored resource")
                .current
                .max_memory_kb,
            100
        );
        assert_eq!(
            restored.durations_mut().expected("", "compile", time(12)),
            Some(Duration::from_millis(123))
        );
        restored
            .restore(time(86_410), &records)
            .expect("restore expired");
        assert!(restored.resource_mut().lookup(&key, 86_410).is_none());
        assert_eq!(
            restored
                .durations_mut()
                .expected("", "compile", time(86_410)),
            None
        );
    }

    #[test]
    fn incompatible_and_malformed_resource_entries_are_discarded() {
        let mut source = histories(86_400, 4);
        let key = SketchKey::for_attempt(Some("target"), None, "");
        source.resource_mut().observe(
            key.clone(),
            &Observation {
                now_s: 10,
                peak_memory_kb: 100,
                ..Observation::default()
            },
        );
        let mut records = Vec::new();
        source.snapshot(time(11), &mut records);

        let mut different_window = histories(172_800, 4);
        different_window
            .restore(time(11), &records)
            .expect("restore ignores mismatched window");
        assert!(different_window.resource_mut().lookup(&key, 11).is_none());

        for offset in [0, 9, 14] {
            let mut broken = records.clone();
            broken[0].body[offset] ^= 0xff;
            let mut restored = histories(86_400, 4);
            restored
                .restore(time(11), &broken)
                .expect("corrupt record ignored");
            assert!(restored.resource_mut().lookup(&key, 11).is_none());
        }
    }

    #[test]
    fn history_id_separates_tags_and_part_boundaries() {
        assert_ne!(history_id(0, &["ab", "c"]), history_id(0, &["a", "bc"]));
        assert_ne!(history_id(0, &["x"]), history_id(1, &["x"]));
    }
}
