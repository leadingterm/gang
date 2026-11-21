//! Source-global activity periods and their fenced learned-history checkpoints.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Tick, Timestamp};
use gang_config::{HistoryWindow, ScalingConfig};
use gang_spi::{LearnedHistory, PortError, RecordId, RecordKey, RecordKind};
use gang_types::SourceId;
use lru::LruCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::PassTime;

const FORMAT_VERSION: u32 = 1;
const RECORD_BYTES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(512 - 1);
const KEY_DOMAIN: &[u8] = b"gang-source-profile-v1\0";

/// Shared source activity history for one leadership term.
///
/// Clones share live periods, restored checkpoints and completed aggregates. Keep a clone
/// through the final learned-history flush after dropping the reconciler.
#[derive(Clone)]
pub struct SourceProfiles {
    inner: Arc<Mutex<Profiles>>,
}

impl fmt::Debug for SourceProfiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceProfiles").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ActivityForecast {
    pub age: Duration,
    pub learned_mean: Option<Duration>,
    pub new_period: bool,
}

struct Profiles {
    source_idle: Duration,
    half_window_ms: u64,
    entries: LruCache<SourceId, Entry>,
    unattributed: Entry,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Half {
    sum_ms: u64,
    count: u32,
}

impl Half {
    fn record(&mut self, duration_ms: u64) {
        // Skip the entire sample on overflow, as ActionDurations does.
        if let (Some(sum_ms), Some(count)) = (
            self.sum_ms.checked_add(duration_ms),
            self.count.checked_add(1),
        ) {
            self.sum_ms = sum_ms;
            self.count = count;
        }
    }

    fn valid(self) -> bool {
        self.count != 0 || self.sum_ms == 0
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    age_ms: u64,
    last_active_wall: u64,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredEntry {
    period: u64,
    current: Half,
    previous: Half,
    last_seen_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint: Option<Checkpoint>,
}

impl StoredEntry {
    fn rotate(&mut self, period: u64) {
        match period.saturating_sub(self.period) {
            0 => return,
            1 => self.previous = self.current,
            _ => self.previous = Half::default(),
        }
        self.current = Half::default();
        self.period = period;
    }

    fn mean(self) -> Option<Duration> {
        let count = u64::from(self.current.count) + u64::from(self.previous.count);
        if count == 0 {
            return None;
        }
        let sum = u128::from(self.current.sum_ms) + u128::from(self.previous.sum_ms);
        Some(Duration::from_millis(
            u64::try_from(sum / u128::from(count)).unwrap_or(u64::MAX),
        ))
    }

    fn record(&mut self, age_ms: u64, last_active_wall: u64, now_ms: u64, half_ms: u64) {
        self.rotate(now_ms / half_ms);
        // A rollback can put an activity timestamp in the future. Clamp only that case;
        // an actually old checkpoint must never be moved into a retained bucket.
        let sample_period = last_active_wall.min(now_ms) / half_ms;
        if sample_period == self.period {
            self.current.record(age_ms);
        } else if self.period.checked_sub(1) == Some(sample_period) {
            self.previous.record(age_ms);
        }
    }

    fn meaningful(self) -> bool {
        self.current.count != 0 || self.previous.count != 0 || self.checkpoint.is_some()
    }

    fn valid(self, half_ms: u64) -> bool {
        self.current.valid()
            && self.previous.valid()
            && self.period <= u64::MAX / half_ms
            && self.period >= self.last_seen_ms / half_ms
            && (self.period != 0 || self.previous.count == 0)
            && self
                .checkpoint
                .is_none_or(|checkpoint| checkpoint.last_active_wall == self.last_seen_ms)
            && self.meaningful()
    }
}

#[derive(Clone, Copy)]
struct LivePeriod {
    start: Tick,
    last_active: Tick,
    carried: Duration,
    last_active_wall: u64,
}

impl LivePeriod {
    fn age(self) -> Duration {
        self.carried
            .saturating_add(self.last_active.saturating_duration_since(self.start))
    }

    fn checkpoint(self) -> Checkpoint {
        Checkpoint {
            age_ms: millis(self.age()),
            last_active_wall: self.last_active_wall,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Entry {
    stored: StoredEntry,
    live: Option<LivePeriod>,
}

impl Entry {
    fn settle_pending(&mut self, time: PassTime, idle: Duration, half_ms: u64) {
        let now_ms = time.wall.as_unix_millis();
        self.stored.rotate(now_ms / half_ms);
        if let Some(checkpoint) = self.stored.checkpoint
            && Duration::from_millis(now_ms.saturating_sub(checkpoint.last_active_wall)) >= idle
        {
            self.stored.checkpoint = None;
            self.stored.record(
                checkpoint.age_ms,
                checkpoint.last_active_wall,
                now_ms,
                half_ms,
            );
        }
    }

    fn close(&mut self, time: PassTime, half_ms: u64, authenticated: bool) {
        let checkpoint = self
            .live
            .take()
            .map(LivePeriod::checkpoint)
            .or_else(|| self.stored.checkpoint.take());
        if authenticated && let Some(checkpoint) = checkpoint {
            self.stored.record(
                checkpoint.age_ms,
                checkpoint.last_active_wall,
                time.wall.as_unix_millis(),
                half_ms,
            );
        }
    }

    fn forecast(self, authenticated: bool, new_period: bool) -> ActivityForecast {
        ActivityForecast {
            age: self.live.map_or_else(
                || {
                    self.stored.checkpoint.map_or(Duration::ZERO, |checkpoint| {
                        Duration::from_millis(checkpoint.age_ms)
                    })
                },
                LivePeriod::age,
            ),
            learned_mean: if authenticated {
                self.stored.mean()
            } else {
                None
            },
            new_period,
        }
    }

    fn observe(
        &mut self,
        active_at: Tick,
        time: PassTime,
        idle: Duration,
        half_ms: u64,
        authenticated: bool,
    ) -> ActivityForecast {
        self.settle_pending(time, idle, half_ms);
        let active_at = active_at.min(time.now);
        if self
            .live
            .is_some_and(|live| active_at.saturating_duration_since(live.last_active) >= idle)
        {
            self.close(time, half_ms, authenticated);
        }
        let new_period = self.live.is_none();
        let now_ms = time.wall.as_unix_millis();
        let live = self.live.get_or_insert_with(|| LivePeriod {
            start: active_at,
            last_active: active_at,
            carried: self
                .stored
                .checkpoint
                .take()
                .map_or(Duration::ZERO, |checkpoint| {
                    Duration::from_millis(checkpoint.age_ms)
                }),
            last_active_wall: now_ms,
        });
        live.last_active = live.last_active.max(active_at);
        live.last_active_wall = now_ms;
        self.stored.last_seen_ms = now_ms;
        self.forecast(authenticated, new_period)
    }

    fn snapshot(self) -> StoredEntry {
        let mut stored = self.stored;
        if let Some(live) = self.live {
            stored.checkpoint = Some(live.checkpoint());
        }
        stored
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredProfile {
    version: u32,
    window_ms: u64,
    source: SourceId,
    entry: StoredEntry,
}

fn record_key(source: SourceId) -> RecordKey {
    let mut digest = Sha256::new();
    digest.update(KEY_DOMAIN);
    digest.update(source.as_ref());
    RecordKey {
        kind: RecordKind::SourceProfile,
        id: RecordId::new(digest.finalize().into()),
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl SourceProfiles {
    /// Allocates bounded authenticated-source state and reserved unattributed live state.
    #[must_use]
    pub fn new(scaling: &ScalingConfig, window: HistoryWindow) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Profiles {
                source_idle: scaling.source_idle.get(),
                // HistoryWindow is a positive whole-day value bounded in nanoseconds.
                half_window_ms: window.get().as_secs() * 1_000 / 2,
                entries: LruCache::new(scaling.max_sources.into()),
                unattributed: Entry::default(),
            })),
        }
    }

    pub(crate) fn observe(
        &self,
        source: Option<SourceId>,
        active_at: Tick,
        time: PassTime,
    ) -> ActivityForecast {
        let mut profiles = self.inner.lock();
        let idle = profiles.source_idle;
        let half_ms = profiles.half_window_ms;
        let entry = match source {
            Some(source) => profiles.entries.get_or_insert_mut(source, Entry::default),
            None => &mut profiles.unattributed,
        };
        entry.observe(active_at, time, idle, half_ms, source.is_some())
    }

    /// Closes a period after the rate model has confirmed continuous observed quiet.
    /// Missing/failed passes cannot prove quiet; only the caller owns that evidence.
    pub(crate) fn close(&self, source: Option<SourceId>, time: PassTime) {
        let mut profiles = self.inner.lock();
        let half_ms = profiles.half_window_ms;
        let entry = match source {
            Some(source) => profiles.entries.peek_mut(&source),
            None => Some(&mut profiles.unattributed),
        };
        if let Some(entry) = entry {
            entry.close(time, half_ms, source.is_some());
        }
    }

    pub(crate) fn finish_pass(&self, time: PassTime) {
        let mut profiles = self.inner.lock();
        let idle = profiles.source_idle;
        let half_ms = profiles.half_window_ms;
        for (_, entry) in &mut profiles.entries {
            entry.settle_pending(time, idle, half_ms);
        }
        // Live periods deliberately remain open until the rates module confirms quiet.
    }

    pub(crate) fn forecast(&self, source: Option<SourceId>, time: PassTime) -> ActivityForecast {
        let mut profiles = self.inner.lock();
        let idle = profiles.source_idle;
        let half_ms = profiles.half_window_ms;
        let entry = match source {
            Some(source) => profiles.entries.peek_mut(&source),
            None => Some(&mut profiles.unattributed),
        };
        let Some(entry) = entry else {
            return ActivityForecast::default();
        };
        entry.settle_pending(time, idle, half_ms);
        entry.forecast(source.is_some(), false)
    }

    /// Appends unexpired profiles without clearing scheduler-owned learned histories.
    /// Unjoined restored checkpoints settle once, including during a final flush.
    ///
    /// # Errors
    /// Returns a backend error if a profile cannot be encoded.
    pub fn snapshot(
        &self,
        time: PassTime,
        into: &mut Vec<LearnedHistory>,
    ) -> Result<(), PortError> {
        let mut profiles = self.inner.lock();
        let half_ms = profiles.half_window_ms;
        let window_ms = half_ms * 2;
        let idle = profiles.source_idle;
        for (source, entry) in profiles.entries.iter_mut().rev() {
            entry.settle_pending(time, idle, half_ms);
            let stored = entry.snapshot();
            let expires = stored.last_seen_ms.saturating_add(window_ms);
            if expires <= time.wall.as_unix_millis() || !stored.meaningful() {
                continue;
            }
            let body = serde_json::to_vec(&StoredProfile {
                version: FORMAT_VERSION,
                window_ms,
                source: *source,
                entry: stored,
            })
            .map_err(PortError::backend)?;
            into.push(LearnedHistory {
                key: record_key(*source),
                body,
                expires_at: Timestamp::from_unix_millis(expires),
            });
        }
        Ok(())
    }

    /// Replaces term-local state with valid, compatible, unexpired source profiles.
    /// Capacity reductions retain the newest activity timestamps, breaking ties by source id.
    pub fn restore(&self, time: PassTime, records: &[LearnedHistory]) {
        let mut profiles = self.inner.lock();
        profiles.entries.clear();
        profiles.unattributed = Entry::default();
        let half_ms = profiles.half_window_ms;
        let window_ms = half_ms * 2;
        let capacity = profiles.entries.cap().get();
        // At most max_sources decoded entries are retained, even for oversized input slices.
        let mut newest = BTreeMap::new();
        for record in records {
            if record.key.kind != RecordKind::SourceProfile
                || record.expires_at <= time.wall
                || record.body.len() > RECORD_BYTES.get()
            {
                continue;
            }
            let Ok(stored) = serde_json::from_slice::<StoredProfile>(&record.body) else {
                continue;
            };
            if stored.version != FORMAT_VERSION
                || stored.window_ms != window_ms
                || record_key(stored.source) != record.key
                || stored.entry.last_seen_ms.saturating_add(window_ms)
                    != record.expires_at.as_unix_millis()
                || !stored.entry.valid(half_ms)
            {
                continue;
            }
            let mut entry = Entry {
                stored: stored.entry,
                live: None,
            };
            entry.settle_pending(time, profiles.source_idle, half_ms);
            if !entry.stored.meaningful() {
                continue;
            }
            let rank = (stored.entry.last_seen_ms, stored.source);
            if newest.len() == capacity {
                if newest
                    .first_key_value()
                    .is_some_and(|(oldest, _)| rank <= *oldest)
                {
                    continue;
                }
                newest.pop_first();
            }
            newest.insert(rank, entry);
        }
        for ((_, source), entry) in newest {
            profiles.entries.put(source, entry);
        }
    }

    /// Maximum encoded body size of one source-profile record, independent of source count.
    #[must_use]
    pub const fn record_byte_limit() -> NonZeroUsize {
        RECORD_BYTES
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    const HALF_DAY: u64 = 43_200_000;
    const DAY: u64 = HALF_DAY * 2;

    fn source(value: u8) -> SourceId {
        format!("{value:016x}").parse().unwrap()
    }

    fn time(tick_ms: u64, wall_ms: u64) -> PassTime {
        PassTime {
            now: Tick::from_elapsed(Duration::from_millis(tick_ms)),
            wall: Timestamp::from_unix_millis(wall_ms),
        }
    }

    fn at(seconds: u64) -> PassTime {
        time(seconds * 1_000, seconds * 1_000)
    }

    fn profiles(capacity: usize) -> SourceProfiles {
        let scaling = ScalingConfig {
            max_sources: gang_config::SourceLimit::try_new(capacity).unwrap(),
            ..ScalingConfig::default()
        };
        SourceProfiles::new(&scaling, HistoryWindow::from_units(1))
    }

    fn observe(profiles: &SourceProfiles, source: SourceId, seconds: u64) -> ActivityForecast {
        profiles.observe(Some(source), at(seconds).now, at(seconds))
    }

    fn snapshot(profiles: &SourceProfiles, time: PassTime) -> Vec<LearnedHistory> {
        let mut records = Vec::new();
        profiles.snapshot(time, &mut records).unwrap();
        records
    }

    fn decode(record: &LearnedHistory) -> StoredProfile {
        serde_json::from_slice(&record.body).unwrap()
    }

    fn completed(profiles: &SourceProfiles, source: SourceId, start: u64, end: u64) {
        observe(profiles, source, start);
        observe(profiles, source, end);
        profiles.close(Some(source), at(end + 60));
    }

    #[test]
    fn source_global_overlapping_fleets_fold_one_period() {
        let profiles = profiles(2);
        let source = source(1);
        assert!(observe(&profiles, source, 0).new_period);
        assert!(!observe(&profiles, source, 10).new_period);
        assert!(!observe(&profiles, source, 30).new_period);
        profiles.close(Some(source), at(90));
        profiles.close(Some(source), at(91));
        let records = snapshot(&profiles, at(91));
        let stored = decode(&records[0]);
        assert_eq!(stored.entry.current.count, 1);
        assert_eq!(stored.entry.current.sum_ms, 30_000);
        assert_eq!(
            profiles.forecast(Some(source), at(91)).learned_mean,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn lull_freezes_age_and_expiry_but_resumed_short_lull_counts() {
        let profiles = profiles(1);
        let source = source(1);
        observe(&profiles, source, 0);
        observe(&profiles, source, 30);
        let before = snapshot(&profiles, at(30));
        profiles.finish_pass(at(80));
        assert_eq!(
            profiles.forecast(Some(source), at(80)).age,
            Duration::from_secs(30)
        );
        let during = snapshot(&profiles, at(80));
        assert_eq!(before, during);
        assert_eq!(observe(&profiles, source, 89).age, Duration::from_secs(89));
        profiles.close(Some(source), at(149));
        assert_eq!(
            profiles.forecast(Some(source), at(149)).learned_mean,
            Some(Duration::from_secs(89))
        );
    }

    #[test]
    fn unobserved_gap_splits_at_inclusive_idle_boundary() {
        let profiles = profiles(1);
        let source = source(1);
        observe(&profiles, source, 0);
        observe(&profiles, source, 30);
        let next = observe(&profiles, source, 90);
        assert!(next.new_period);
        assert_eq!(next.age, Duration::ZERO);
        assert_eq!(next.learned_mean, Some(Duration::from_secs(30)));
    }

    #[test]
    fn equal_zero_and_future_ticks_are_safe() {
        let profiles = profiles(1);
        let source = source(1);
        profiles.observe(Some(source), at(100).now, at(0));
        assert!(!observe(&profiles, source, 0).new_period);
        assert_eq!(observe(&profiles, source, 1).age, Duration::from_secs(1));
        profiles.close(Some(source), at(61));
        assert_eq!(
            profiles.forecast(Some(source), at(61)).learned_mean,
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn finish_and_forecast_do_not_treat_skips_as_observed_quiet() {
        let profiles = profiles(1);
        let source = source(1);
        observe(&profiles, source, 0);
        observe(&profiles, source, 10);
        profiles.finish_pass(at(300));
        let forecast = profiles.forecast(Some(source), at(300));
        assert_eq!(forecast.age, Duration::from_secs(10));
        assert_eq!(forecast.learned_mean, None);
        assert!(
            decode(&snapshot(&profiles, at(300))[0])
                .entry
                .checkpoint
                .is_some()
        );
        profiles.close(Some(source), at(360));
        let forecast = profiles.forecast(Some(source), at(360));
        assert_eq!(forecast.age, Duration::ZERO);
        assert_eq!(forecast.learned_mean, Some(Duration::from_secs(10)));
    }

    #[test]
    fn current_previous_mean_rotates_and_old_samples_expire() {
        let profiles = profiles(1);
        let source = source(1);
        completed(&profiles, source, 0, 20);
        let next = HALF_DAY / 1_000;
        completed(&profiles, source, next, next + 40);
        assert_eq!(
            profiles.forecast(Some(source), at(next + 100)).learned_mean,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            profiles.forecast(Some(source), time(DAY, DAY)).learned_mean,
            Some(Duration::from_secs(40))
        );
        assert_eq!(
            profiles
                .forecast(Some(source), time(DAY + HALF_DAY, DAY + HALF_DAY))
                .learned_mean,
            None
        );
    }

    #[test]
    fn first_build_checkpoint_joins_without_handoff_gap() {
        let original = profiles(1);
        let source = source(1);
        observe(&original, source, 0);
        observe(&original, source, 30);
        let records = snapshot(&original, at(40));
        let stored = decode(&records[0]);
        assert_eq!(stored.entry.current.count, 0);
        assert_eq!(stored.entry.checkpoint.unwrap().age_ms, 30_000);
        let restored = profiles(1);
        restored.restore(time(0, 50_000), &records);
        let joined = restored.observe(Some(source), Tick::ORIGIN, time(0, 50_000));
        assert!(joined.new_period);
        assert_eq!(joined.age, Duration::from_secs(30));
        assert_eq!(joined.learned_mean, None);
        let continued =
            restored.observe(Some(source), time(10_000, 60_000).now, time(10_000, 60_000));
        assert_eq!(continued.age, Duration::from_secs(40));
        let joined_records = snapshot(&restored, time(10_000, 60_000));
        assert_eq!(decode(&joined_records[0]).entry.current.count, 0);
    }

    #[test]
    fn repeated_handoffs_do_not_duplicate_completed_periods() {
        let source = source(1);
        let mut current = profiles(1);
        observe(&current, source, 0);
        observe(&current, source, 20);
        for _ in 0..3 {
            let records = snapshot(&current, at(20));
            let next = profiles(1);
            next.restore(at(20), &records);
            observe(&next, source, 20);
            current = next;
        }
        observe(&current, source, 40);
        current.close(Some(source), at(100));
        observe(&current, source, 200);
        observe(&current, source, 240);
        let records = snapshot(&current, at(240));
        let next = profiles(1);
        next.restore(at(240), &records);
        observe(&next, source, 240);
        observe(&next, source, 280);
        next.close(Some(source), at(340));
        assert_eq!(
            next.forecast(Some(source), at(340)).learned_mean,
            Some(Duration::from_mins(1))
        );
        assert_eq!(decode(&snapshot(&next, at(340))[0]).entry.current.count, 2);
    }

    #[test]
    fn unjoined_checkpoint_settles_once_at_flush_idle_boundary() {
        let original = profiles(1);
        let source = source(1);
        observe(&original, source, 0);
        observe(&original, source, 30);
        let records = snapshot(&original, at(30));
        let restored = profiles(1);
        restored.restore(at(40), &records);
        assert!(
            decode(&snapshot(&restored, at(89))[0])
                .entry
                .checkpoint
                .is_some()
        );
        let settled = snapshot(&restored, at(90));
        let stored = decode(&settled[0]);
        assert!(stored.entry.checkpoint.is_none());
        assert_eq!(stored.entry.current.count, 1);
        assert_eq!(stored.entry.current.sum_ms, 30_000);
        assert_eq!(settled[0].expires_at, records[0].expires_at);
        assert_eq!(snapshot(&restored, at(100)), settled);
        let again = profiles(1);
        again.restore(at(100), &settled);
        assert_eq!(snapshot(&again, at(110)), settled);
    }

    #[test]
    fn pending_checkpoint_uses_original_bucket_and_never_resurrects() {
        let original = profiles(1);
        let source = source(1);
        observe(&original, source, 0);
        observe(&original, source, 30);
        let records = snapshot(&original, at(30));
        let restored = profiles(1);
        restored.restore(time(HALF_DAY, HALF_DAY), &records);
        assert_eq!(
            decode(&snapshot(&restored, time(HALF_DAY, HALF_DAY))[0])
                .entry
                .previous
                .count,
            1
        );
        let expired = profiles(1);
        expired.restore(time(DAY, DAY), &records);
        assert!(snapshot(&expired, time(DAY, DAY)).is_empty());
        let fresh = expired.observe(Some(source), time(DAY, DAY).now, time(DAY, DAY));
        assert_eq!(fresh.learned_mean, None);
    }

    #[test]
    fn expired_completed_samples_do_not_return_when_activity_refreshes_ttl() {
        let profiles = profiles(1);
        let source = source(1);
        completed(&profiles, source, 0, 30);
        let fresh = profiles.observe(Some(source), time(DAY, DAY).now, time(DAY, DAY));
        assert_eq!(fresh.learned_mean, None);
        assert_eq!(
            decode(&snapshot(&profiles, time(DAY, DAY))[0])
                .entry
                .current
                .count,
            0
        );
    }

    #[test]
    fn none_is_reserved_live_only_and_never_learns_or_serializes() {
        let profiles = profiles(1);
        profiles.observe(None, at(0).now, at(0));
        let forecast = profiles.observe(None, at(30).now, at(30));
        assert_eq!(forecast.age, Duration::from_secs(30));
        observe(&profiles, source(1), 30);
        observe(&profiles, source(2), 31);
        assert_eq!(profiles.forecast(None, at(31)).age, Duration::from_secs(30));
        profiles.close(None, at(90));
        assert_eq!(profiles.forecast(None, at(90)).learned_mean, None);
        assert_eq!(snapshot(&profiles, at(90)).len(), 1);
    }

    #[test]
    fn queries_do_not_refresh_lru_recency() {
        let profiles = profiles(2);
        let one = source(1);
        observe(&profiles, one, 0);
        observe(&profiles, source(2), 1);
        profiles.forecast(Some(one), at(2));
        profiles.finish_pass(at(2));
        snapshot(&profiles, at(2));
        observe(&profiles, source(3), 3);
        let records = snapshot(&profiles, at(3));
        assert_eq!(records.len(), 2);
        assert!(!records.iter().any(|record| decode(record).source == one));
    }

    #[test]
    fn reduced_capacity_restore_keeps_newest_activity_deterministically() {
        let original = profiles(4);
        for (source, seconds) in [
            (source(1), 1),
            (source(2), 2),
            (source(3), 2),
            (source(4), 3),
        ] {
            observe(&original, source, seconds);
        }
        let mut records = snapshot(&original, at(3));
        records.reverse();
        let restored = profiles(2);
        restored.restore(at(4), &records);
        let kept = snapshot(&restored, at(4));
        assert_eq!(kept.len(), 2);
        assert_eq!(decode(&kept[0]).source, source(3));
        assert_eq!(decode(&kept[1]).source, source(4));
    }

    #[test]
    fn restore_and_flush_preserve_original_expiration() {
        let original = profiles(1);
        let source = source(1);
        completed(&original, source, 0, 30);
        let records = snapshot(&original, at(100));
        let restored = profiles(1);
        restored.restore(at(200), &records);
        assert_eq!(snapshot(&restored, at(300)), records);
        assert!(snapshot(&restored, time(DAY + 30_000, DAY + 30_000)).is_empty());
    }

    #[test]
    fn malformed_incompatible_and_impossible_records_are_ignored() {
        let original = profiles(1);
        let source = source(1);
        completed(&original, source, 0, 30);
        let valid = snapshot(&original, at(100)).remove(0);
        for mutation in 0..10 {
            let mut record = valid.clone();
            let mut stored = decode(&record);
            match mutation {
                0 => record.body = b"not json".to_vec(),
                1 => stored.version += 1,
                2 => stored.window_ms += 1,
                3 => record.key = record_key(super::tests::source(2)),
                4 => record.expires_at = Timestamp::from_unix_millis(DAY),
                5 => stored.entry.current.count = 0,
                6 => stored.entry.current = Half::default(),
                7 => {
                    stored.entry.previous = Half {
                        sum_ms: 1,
                        count: 0,
                    };
                }
                8 => {
                    stored.entry.checkpoint = Some(Checkpoint {
                        age_ms: 0,
                        last_active_wall: 0,
                    });
                }
                _ => stored.entry.period = u64::MAX,
            }
            if mutation != 0 {
                record.body = serde_json::to_vec(&stored).unwrap();
            }
            let restored = profiles(1);
            restored.restore(at(100), &[record]);
            assert!(
                snapshot(&restored, at(100)).is_empty(),
                "mutation {mutation}"
            );
        }
        let expired = profiles(1);
        expired.restore(time(DAY + 30_000, DAY + 30_000), &[valid]);
        assert!(snapshot(&expired, time(DAY + 30_000, DAY + 30_000)).is_empty());
    }

    #[test]
    fn wall_jumps_do_not_inflate_live_or_joined_monotonic_age() {
        let original = profiles(1);
        let source = source(1);
        original.observe(Some(source), time(0, 100_000).now, time(0, 100_000));
        original.observe(
            Some(source),
            time(10_000, 1_000_000).now,
            time(10_000, 1_000_000),
        );
        assert_eq!(
            original.forecast(Some(source), time(20_000, 2_000_000)).age,
            Duration::from_secs(10)
        );
        let records = snapshot(&original, time(10_000, 1_000_000));
        let rollback = profiles(1);
        rollback.restore(time(0, 1_000), &records);
        let joined = rollback.observe(Some(source), Tick::ORIGIN, time(0, 1_000));
        assert_eq!(joined.age, Duration::from_secs(10));
        let forward = profiles(1);
        forward.restore(time(0, 1_060_000), &records);
        let split = forward.observe(Some(source), Tick::ORIGIN, time(0, 1_060_000));
        assert_eq!(split.age, Duration::ZERO);
        assert_eq!(split.learned_mean, Some(Duration::from_secs(10)));
    }

    #[test]
    fn expired_samples_stay_expired_after_wall_rollback() {
        let profiles = profiles(1);
        let source = source(1);
        completed(&profiles, source, 0, 30);
        profiles.forecast(Some(source), time(DAY, DAY));
        let fresh = profiles.observe(Some(source), time(DAY, 1_000).now, time(DAY, 1_000));
        assert_eq!(fresh.learned_mean, None);
    }

    #[test]
    fn source_record_key_is_domain_separated_and_schema_contains_no_principal() {
        let profiles = profiles(1);
        let source = source(1);
        observe(&profiles, source, 0);
        let records = snapshot(&profiles, at(0));
        let stored = decode(&records[0]);
        assert_eq!(stored.source, source);
        let mut hash = Sha256::new();
        hash.update(b"gang-source-profile-v1\0");
        hash.update(source.as_ref());
        assert_eq!(
            records[0].key.id.as_bytes(),
            &<[u8; 32]>::from(hash.finalize())
        );
        assert_ne!(
            *records[0].key.id.as_bytes(),
            <[u8; 32]>::from(Sha256::digest(source.as_ref()))
        );
        let json: serde_json::Value = serde_json::from_slice(&records[0].body).unwrap();
        let mut keys: Vec<_> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, ["entry", "source", "version", "window_ms"]);
        assert_eq!(json["source"], source.to_string());
        let debug = format!("{profiles:?}");
        assert!(!debug.contains(&source.to_string()));
    }

    #[test]
    fn worst_scalar_body_fits_per_record_ceiling_and_snapshot_appends() {
        let half_ms = HistoryWindow::MAX.get().as_secs() * 1_000 / 2;
        let mut stored = StoredProfile {
            version: FORMAT_VERSION,
            window_ms: half_ms * 2,
            source: "ffffffffffffffff".parse().unwrap(),
            entry: StoredEntry {
                period: u64::MAX / half_ms,
                current: Half {
                    sum_ms: u64::MAX,
                    count: u32::MAX,
                },
                previous: Half {
                    sum_ms: u64::MAX,
                    count: u32::MAX,
                },
                last_seen_ms: u64::MAX,
                checkpoint: Some(Checkpoint {
                    age_ms: u64::MAX,
                    last_active_wall: u64::MAX,
                }),
            },
        };
        assert!(stored.entry.valid(half_ms));
        assert!(
            serde_json::to_vec(&stored).unwrap().len() <= SourceProfiles::record_byte_limit().get()
        );
        // Independently maximizing even mutually constrained scalars is an upper bound
        // for every valid body, including smaller windows with larger period numbers.
        stored.window_ms = u64::MAX;
        stored.entry.period = u64::MAX;
        assert!(
            serde_json::to_vec(&stored).unwrap().len() <= SourceProfiles::record_byte_limit().get()
        );
        let profiles = profiles(1);
        observe(&profiles, source(1), 0);
        let sentinel = LearnedHistory {
            key: RecordKey {
                kind: RecordKind::DurationHistory,
                id: RecordId::new([0; 32]),
            },
            body: vec![1, 2, 3],
            expires_at: Timestamp::from_unix_millis(DAY),
        };
        let mut records = vec![sentinel.clone()];
        profiles.snapshot(at(0), &mut records).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], sentinel);
    }

    #[test]
    fn overflow_skips_whole_sample_and_mean_uses_wide_arithmetic() {
        let mut half = Half {
            sum_ms: u64::MAX,
            count: 1,
        };
        half.record(1);
        assert_eq!((half.sum_ms, half.count), (u64::MAX, 1));
        let mut full_count = Half {
            sum_ms: 0,
            count: u32::MAX,
        };
        full_count.record(1);
        assert_eq!((full_count.sum_ms, full_count.count), (0, u32::MAX));
        let stored = StoredEntry {
            current: half,
            previous: half,
            ..StoredEntry::default()
        };
        assert_eq!(stored.mean(), Some(Duration::from_millis(u64::MAX)));
    }

    #[test]
    fn cloned_handle_shares_state_and_warm_passes_allocate_nothing() {
        let profiles = profiles(2);
        let source = source(1);
        let clone = profiles.clone();
        observe(&profiles, source, 0);
        profiles.observe(None, Tick::ORIGIN, at(0));
        let allocations = allocation_counter::measure(|| {
            for seconds in 1..100 {
                let time = at(seconds);
                clone.observe(Some(source), time.now, time);
                clone.observe(None, time.now, time);
                clone.forecast(Some(source), time);
                clone.forecast(None, time);
                clone.finish_pass(time);
            }
            clone.close(Some(source), at(200));
            clone.close(None, at(200));
        });
        assert_eq!(allocations.count_total, 0);
        assert_eq!(
            profiles.forecast(Some(source), at(200)).learned_mean,
            Some(Duration::from_secs(99))
        );
    }

    #[test]
    fn pending_checkpoints_settle_on_finish_and_forecast_without_ttl_refresh() {
        let original = profiles(1);
        let source = source(1);
        observe(&original, source, 0);
        observe(&original, source, 30);
        let records = snapshot(&original, at(30));
        for finish in [false, true] {
            let restored = profiles(1);
            restored.restore(at(40), &records);
            if finish {
                restored.finish_pass(at(90));
            } else {
                let forecast = restored.forecast(Some(source), at(90));
                assert_eq!(forecast.age, Duration::ZERO);
                assert_eq!(forecast.learned_mean, Some(Duration::from_secs(30)));
            }
            let settled = snapshot(&restored, at(91));
            assert!(decode(&settled[0]).entry.checkpoint.is_none());
            assert_eq!(decode(&settled[0]).entry.current.count, 1);
            assert_eq!(settled[0].expires_at, records[0].expires_at);
        }
    }

    #[test]
    fn subpass_activity_uses_receipt_tick_not_later_pass_tick() {
        let profiles = profiles(1);
        let source = source(1);
        observe(&profiles, source, 0);
        let forecast = profiles.observe(Some(source), at(10).now, at(20));
        assert_eq!(forecast.age, Duration::from_secs(10));
        let records = snapshot(&profiles, at(30));
        let checkpoint = decode(&records[0]).entry.checkpoint.unwrap();
        assert_eq!(checkpoint.age_ms, 10_000);
        assert_eq!(checkpoint.last_active_wall, 20_000);
        profiles.close(Some(source), at(90));
        assert_eq!(
            profiles.forecast(Some(source), at(90)).learned_mean,
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn zero_duration_period_is_a_valid_completed_sample() {
        let original = profiles(1);
        let source = source(1);
        observe(&original, source, 0);
        original.close(Some(source), at(60));
        let records = snapshot(&original, at(60));
        assert_eq!(decode(&records[0]).entry.current.count, 1);
        let restored = profiles(1);
        restored.restore(at(100), &records);
        assert_eq!(
            restored.forecast(Some(source), at(100)).learned_mean,
            Some(Duration::ZERO)
        );
    }
}
