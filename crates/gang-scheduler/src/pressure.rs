//! Pure worker-side memory `PSI`, disk free-space, graveyard credit, soft-ceiling, victim
//! selection, and post-kill quiescence policy (`§5.8` & `§6.5`).
//!
//! All inputs are parsed numbers and monotonic [`Tick`]s; filesystem and cgroup sampling remain in
//! `gang-engine`.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Tick;
use gang_types::{HeadroomPercent, Percent};

/// Maximum graveyard pending credit expressed as a percentage of `disk_per_instance`.
pub const GRAVEYARD_CREDIT_CAP_PERCENT: u64 = 10;

/// Raw `statvfs` free-space emergency floor expressed as a percentage of the scratch volume.
///
/// Below this floor, disk pressure triggers without any graveyard credit.
pub const DISK_EMERGENCY_FLOOR_PERCENT: u64 = 2;

/// Parses the cumulative `PSI` stall time in microseconds from `memory.pressure` text, preferring
/// `some` and falling back to `full`.
#[must_use]
pub fn parse_psi_total_us(text: &str) -> Option<u64> {
    let total = |kind: &str| {
        text.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            if fields.next()? != kind {
                return None;
            }
            fields.find_map(|field| field.strip_prefix("total=")?.parse().ok())
        })
    };
    total("some").or_else(|| total("full"))
}

/// Parses a cgroup memory limit file (`memory.max` or `memory.high`).
///
/// Returns `Some(None)` for `"max"`, `Some(Some(bytes))` for a numeric byte limit, or `None` for
/// malformed content.
#[must_use]
pub fn parse_cgroup_limit(text: &str) -> Option<Option<u64>> {
    let trimmed = text.trim();
    if trimmed == "max" {
        Some(None)
    } else {
        Some(Some(trimmed.parse::<u64>().ok()?))
    }
}

/// Computes available bytes in the action memory pool: `min(memory.max, memory.high) - current`,
/// saturating at zero. Returns `None` when both limits are `"max"` (unbounded).
#[must_use]
pub fn pool_available_bytes(
    current: u64,
    memory_max: Option<u64>,
    memory_high: Option<u64>,
) -> Option<u64> {
    let limit = match (memory_max, memory_high) {
        (Some(max), Some(high)) => max.min(high),
        (Some(limit), None) | (None, Some(limit)) => limit,
        (None, None) => return None,
    };
    Some(limit.saturating_sub(current))
}

/// Returns whether an action memory pool has less than `reserve_bytes` available.
///
/// Missing or malformed samples and an entirely unbounded pool fail closed (`false`).
#[must_use]
pub fn pool_shortage(
    current: Option<u64>,
    memory_max: Option<Option<u64>>,
    memory_high: Option<Option<u64>>,
    reserve_bytes: u64,
) -> bool {
    let (Some(current), Some(max), Some(high)) = (current, memory_max, memory_high) else {
        return false;
    };
    pool_available_bytes(current, max, high).is_some_and(|avail| avail < reserve_bytes)
}

/// Continuous `PSI` shortage window for worker memory pressure kills.
///
/// Counts an interval as pressured when `delta_stall_us * 100 >= stall_percent * elapsed_us` and
/// pool available bytes are strictly below `reserve_bytes`. Resets baseline and window on
/// counter decrease, malformed/missing sample, non-monotonic time, pool availability at or above
/// reserve, or unsettled post-kill quiescence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryPressureMonitor {
    stall_percent: Percent,
    reserve_bytes: u64,
    after: Duration,
    baseline: Option<(u64, Tick)>,
    since: Option<Tick>,
}

impl MemoryPressureMonitor {
    /// Creates a new memory `PSI` pressure monitor.
    #[must_use]
    pub const fn new(stall_percent: Percent, reserve_bytes: u64, after: Duration) -> Self {
        Self {
            stall_percent,
            reserve_bytes,
            after,
            baseline: None,
            since: None,
        }
    }

    /// Resets the baseline and active stall window.
    pub const fn reset(&mut self) {
        self.baseline = None;
        self.since = None;
    }

    /// Evaluates one memory pressure sample at monotonic `now`.
    ///
    /// Pass `settled = false` while a prior pressure kill is still settling (quiescence rule).
    /// Returns `true` when sustained shortage and `PSI` stalls justify selecting and killing a
    /// victim, resetting the window upon firing.
    pub fn observe(
        &mut self,
        psi_total_us: Option<u64>,
        available_bytes: Option<u64>,
        settled: bool,
        now: Tick,
    ) -> bool {
        if !settled || available_bytes.is_none_or(|avail| avail >= self.reserve_bytes) {
            self.reset();
            return false;
        }
        let Some(total) = psi_total_us else {
            self.reset();
            return false;
        };
        let Some((previous, prev_time)) = self.baseline.replace((total, now)) else {
            return false;
        };
        if now <= prev_time {
            self.reset();
            return false;
        }
        let elapsed = now.saturating_duration_since(prev_time);
        let Some(stalled) = total.checked_sub(previous) else {
            self.reset();
            return false;
        };
        if u128::from(stalled) * 100 < u128::from(self.stall_percent.get()) * elapsed.as_micros() {
            self.since = None;
            return false;
        }
        let since = *self.since.get_or_insert(prev_time);
        if now.saturating_duration_since(since) >= self.after {
            self.reset();
            return true;
        }
        false
    }
}

/// Computes the reclaimable bytes of a graveyard entry after subtracting bytes shared with
/// `DiskCas` via reflinks or hardlinks (unlinking a shared inode frees zero bytes).
#[must_use]
pub const fn reclaimable_bytes(walk_bytes: u64, cas_shared_bytes: u64) -> u64 {
    walk_bytes.saturating_sub(cas_shared_bytes)
}

/// Caps `graveyard_pending_bytes` at `10%` of `disk_per_instance_bytes`.
#[must_use]
pub fn graveyard_credit(graveyard_pending_bytes: u64, disk_per_instance_bytes: u64) -> u64 {
    let cap = u64::try_from(
        u128::from(disk_per_instance_bytes) * u128::from(GRAVEYARD_CREDIT_CAP_PERCENT) / 100,
    )
    .unwrap_or(u64::MAX);
    graveyard_pending_bytes.min(cap)
}

/// One filesystem and graveyard sample for the disk pressure monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSample {
    /// Raw available bytes reported by `statvfs`.
    pub free_bytes: u64,
    /// Total volume bytes reported by `statvfs`.
    pub total_bytes: u64,
    /// Sum of [`reclaimable_bytes`] for entries currently queued or deleting in `.graveyard`.
    pub graveyard_pending_bytes: u64,
}

/// Computes effective free bytes after adding bounded graveyard credit.
#[must_use]
pub fn effective_free_bytes(sample: DiskSample, disk_per_instance_bytes: u64) -> u64 {
    sample.free_bytes.saturating_add(graveyard_credit(
        sample.graveyard_pending_bytes,
        disk_per_instance_bytes,
    ))
}

/// Returns whether the scratch volume is under disk pressure, honoring both the `2%` raw
/// emergency floor (which graveyard credit can never mask) and the `10%` graveyard credit cap.
#[must_use]
pub fn disk_under_pressure(
    sample: DiskSample,
    disk_per_instance_bytes: u64,
    free_percent: Percent,
) -> bool {
    if sample.total_bytes == 0 {
        return false;
    }
    if u128::from(sample.free_bytes) * 100
        < u128::from(sample.total_bytes) * u128::from(DISK_EMERGENCY_FLOOR_PERCENT)
    {
        return true;
    }
    let effective = effective_free_bytes(sample, disk_per_instance_bytes);
    u128::from(effective) * 100 <= u128::from(sample.total_bytes) * u128::from(free_percent.get())
}

/// Whole-percent free space rounded down, returning `None` when `total_bytes == 0`.
#[must_use]
pub fn disk_free_percent(free_bytes: u64, total_bytes: u64) -> Option<u64> {
    (total_bytes > 0).then(|| {
        u64::try_from(u128::from(free_bytes) * 100 / u128::from(total_bytes))
            .unwrap_or(u64::MAX)
            .min(100)
    })
}

/// Continuous free-space window for worker disk pressure kills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskPressureMonitor {
    free_percent: Percent,
    disk_per_instance_bytes: u64,
    after: Duration,
    over_since: Option<Tick>,
    last_tick: Option<Tick>,
}

impl DiskPressureMonitor {
    /// Creates a new disk pressure monitor.
    #[must_use]
    pub const fn new(free_percent: Percent, disk_per_instance_bytes: u64, after: Duration) -> Self {
        Self {
            free_percent,
            disk_per_instance_bytes,
            after,
            over_since: None,
            last_tick: None,
        }
    }

    /// Resets the active sustained pressure window.
    pub const fn reset(&mut self) {
        self.over_since = None;
    }

    /// Evaluates one disk sample at monotonic `now`.
    ///
    /// Pass `settled = false` while a prior pressure kill or its graveyard entry is still
    /// draining. Resets on missing/empty sample, non-monotonic time, healthy effective free
    /// space, or unsettled quiescence. Returns `true` when sustained pressure justifies a kill.
    pub fn observe(&mut self, sample: Option<DiskSample>, settled: bool, now: Tick) -> bool {
        let non_monotonic = self.last_tick.is_some_and(|prev| now < prev);
        self.last_tick = Some(now);
        if !settled || non_monotonic {
            self.reset();
            return false;
        }
        let Some(sample) = sample.filter(|s| s.total_bytes > 0) else {
            self.reset();
            return false;
        };
        if !disk_under_pressure(sample, self.disk_per_instance_bytes, self.free_percent) {
            self.reset();
            return false;
        }
        let since = *self.over_since.get_or_insert(now);
        if now.saturating_duration_since(since) >= self.after {
            self.reset();
            return true;
        }
        false
    }
}

/// Computes a soft-enforcement ceiling from a nonzero reservation and headroom percentage.
#[must_use]
pub fn soft_ceiling(reserved: u64, headroom: HeadroomPercent) -> Option<u64> {
    (reserved > 0).then(|| headroom.scale_u64(reserved))
}

/// Outcome of one soft-ceiling usage sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoftVerdict {
    /// Usage is within limits or has not reached the breach threshold.
    Ok,
    /// Consecutive breaches or single-sample spike exceeded the threshold; kill the attempt.
    Kill,
}

/// Per-attempt soft-ceiling consecutive sample and single-sample spike counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SoftCeilingMonitor {
    limit: u64,
    soft_samples: NonZeroU32,
    spike_percent: Option<u32>,
    consecutive_breaches: u32,
}

impl SoftCeilingMonitor {
    /// Creates a soft-ceiling monitor with `limit`, `soft_samples` consecutive breach threshold,
    /// and an optional immediate-kill `spike_percent` of `limit` (e.g. `200` for `2x` ceiling).
    #[must_use]
    pub const fn new(limit: u64, soft_samples: NonZeroU32, spike_percent: Option<u32>) -> Self {
        Self {
            limit,
            soft_samples,
            spike_percent,
            consecutive_breaches: 0,
        }
    }

    /// Returns the soft ceiling limit.
    #[must_use]
    pub const fn limit(&self) -> u64 {
        self.limit
    }

    /// Returns the current consecutive over-ceiling sample count.
    #[must_use]
    pub const fn consecutive_breaches(&self) -> u32 {
        self.consecutive_breaches
    }

    /// Evaluates one usage sample against the ceiling and spike threshold.
    pub fn observe(&mut self, observed: u64) -> SoftVerdict {
        if observed > self.limit {
            self.consecutive_breaches = self.consecutive_breaches.saturating_add(1);
        } else {
            self.consecutive_breaches = 0;
        }
        let gross = self.spike_percent.is_some_and(|spike| {
            u128::from(observed) * 100 > u128::from(self.limit) * u128::from(spike)
        });
        if self.consecutive_breaches >= self.soft_samples.get() || gross {
            SoftVerdict::Kill
        } else {
            SoftVerdict::Ok
        }
    }
}

/// One running attempt evaluated as a pressure-kill victim candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PressureCandidate<T> {
    /// Caller's handle identifying the running attempt.
    pub item: T,
    /// Last sampled resource usage (`0` if unsampled or already finished).
    pub usage: u64,
    /// Resource reservation (`0` if none).
    pub reserved: u64,
    /// Whether the attempt has finished or already has a pending kill.
    pub finished_or_killing: bool,
}

/// Selects the victim with the greatest positive `usage - reserved`, or the greatest `usage`
/// when no candidate exceeds its reservation.
///
/// Excludes unsampled (`usage == 0`), finished, and already-killing attempts. Ties prefer the
/// later candidate in iteration order (typically the newest attempt).
#[must_use]
pub fn pick_pressure_victim<T>(
    candidates: impl IntoIterator<Item = PressureCandidate<T>>,
) -> Option<T> {
    candidates
        .into_iter()
        .filter(|c| c.usage > 0 && !c.finished_or_killing)
        .max_by_key(|c| {
            let excess = c.usage.saturating_sub(c.reserved);
            (excess > 0, if excess > 0 { excess } else { c.usage })
        })
        .map(|c| c.item)
}

/// Tracks the post-kill quiescence gate shared by memory and disk pressure monitors (`§5.8`,
/// `§6.5`).
///
/// After any memory or disk pressure kill, neither monitor accumulates a window until the killed
/// attempt has finished cleanup and left the running-actions map, and (when `await_graveyard` is
/// set) its graveyard entry has been fully processed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QuiescenceGate<K> {
    generation: u64,
    pending_victim: Option<K>,
    pending_graveyard: Option<K>,
}

impl<K: Eq + Clone> QuiescenceGate<K> {
    /// Creates an initially settled quiescence gate.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            generation: 0,
            pending_victim: None,
            pending_graveyard: None,
        }
    }

    /// Monotonic counter incremented each time a pressure kill starts.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns `true` when no prior kill is still waiting for attempt or graveyard cleanup.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        self.pending_victim.is_none() && self.pending_graveyard.is_none()
    }

    /// Records a newly initiated pressure kill for `victim`.
    pub fn begin_kill(&mut self, victim: K, await_graveyard: bool) {
        self.generation = self.generation.wrapping_add(1);
        self.pending_graveyard = await_graveyard.then(|| victim.clone());
        self.pending_victim = Some(victim);
    }

    /// Clears the pending victim flag when `victim` has left the running-actions set.
    pub fn on_victim_removed(&mut self, victim: &K) {
        if self.pending_victim.as_ref() == Some(victim) {
            self.pending_victim = None;
        }
    }

    /// Clears the pending graveyard flag when `victim`'s graveyard entry finishes (or fails).
    pub fn on_graveyard_processed(&mut self, victim: &K) {
        if self.pending_graveyard.as_ref() == Some(victim) {
            self.pending_graveyard = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(ms: u64) -> Tick {
        Tick::ORIGIN + Duration::from_millis(ms)
    }

    #[test]
    fn psi_parse_prefers_some_and_falls_back_to_full() {
        assert_eq!(
            parse_psi_total_us("full avg10=0 total=53\nsome avg10=0 total=91\n"),
            Some(91)
        );
        assert_eq!(parse_psi_total_us("full total=53\n"), Some(53));
        assert_eq!(parse_psi_total_us("malformed\n"), None);
    }

    #[test]
    fn pool_shortage_requires_finite_limit_and_strict_reserve() {
        let ok = parse_cgroup_limit;
        assert!(!pool_shortage(Some(90), ok("100"), ok("max"), 10));
        assert!(pool_shortage(Some(91), ok("100"), ok("max"), 10));
        assert!(pool_shortage(Some(91), ok("200"), ok("100"), 10));
        assert!(pool_shortage(Some(91), ok("max"), ok("100"), 10));
        assert!(!pool_shortage(Some(91), ok("max"), ok("max"), 10));
        assert!(!pool_shortage(None, ok("100"), ok("max"), 10));
        assert!(!pool_shortage(Some(91), ok("bad"), ok("100"), 10));
        assert!(!pool_shortage(
            Some(0),
            ok("18446744073709551615"),
            ok("max"),
            u64::MAX
        ));
    }

    #[test]
    fn memory_psi_window_resets_on_flat_counter_errors_quiescence_and_clock_regression() {
        let mut w =
            MemoryPressureMonitor::new(Percent::from_percent(7), 100, Duration::from_millis(230));
        // First reading sets baseline.
        assert!(!w.observe(Some(120), Some(50), true, t(0)));
        // 8000us in 100ms = 8% >= 7%, starts window at t(0).
        assert!(!w.observe(Some(8_120), Some(50), true, t(100)));
        // Flat stall (0%) clears active window.
        assert!(!w.observe(Some(8_120), Some(50), true, t(200)));
        // Starts window again at t(200).
        assert!(!w.observe(Some(16_120), Some(50), true, t(300)));
        // Missing sample resets baseline & window.
        assert!(!w.observe(None, Some(50), true, t(400)));
        // Re-baseline at t(500), accumulate through t(800) (300ms >= 230ms).
        assert!(!w.observe(Some(24_120), Some(50), true, t(500)));
        assert!(!w.observe(Some(32_120), Some(50), true, t(600)));
        assert!(!w.observe(Some(40_120), Some(50), true, t(700)));
        assert!(w.observe(Some(48_120), Some(50), true, t(800)));

        // After firing, requires a fresh baseline.
        assert!(!w.observe(Some(48_120), Some(50), true, t(900)));
        // Unsettled quiescence resets.
        assert!(!w.observe(Some(56_120), Some(50), false, t(1000)));
        // Counter decrease resets.
        assert!(!w.observe(Some(64_120), Some(50), true, t(1100)));
        assert!(!w.observe(Some(3), Some(50), true, t(1200)));
        // Non-monotonic time resets.
        assert!(!w.observe(Some(9_000), Some(50), true, t(1150)));
        // Available >= reserve resets.
        assert!(!w.observe(Some(17_000), Some(100), true, t(1300)));
    }

    #[test]
    fn graveyard_credit_and_emergency_floor_follow_spec() {
        // 100 GiB volume, 80 GiB disk_per_instance -> credit cap is 8 GiB, emergency floor is 2 GiB,
        // threshold at 5% is 5 GiB.
        let total = 100_000;
        let disk_per_instance = 80_000;
        let free_pct = Percent::from_percent(5);

        // Shared CAS bytes are subtracted from walk bytes.
        assert_eq!(reclaimable_bytes(10_000, 6_000), 4_000);
        assert_eq!(reclaimable_bytes(4_000, 6_000), 0);

        // 3% raw free + 4% credit = 7% > 5% -> healthy.
        assert!(!disk_under_pressure(
            DiskSample {
                free_bytes: 3_000,
                total_bytes: total,
                graveyard_pending_bytes: 4_000,
            },
            disk_per_instance,
            free_pct,
        ));

        // Credit is capped at 10% of disk_per_instance (8_000). With 10% threshold (10_000),
        // 2_000 raw + min(20_000, 8_000) = 10_000 <= 10_000 -> pressured.
        assert!(disk_under_pressure(
            DiskSample {
                free_bytes: 2_000,
                total_bytes: total,
                graveyard_pending_bytes: 20_000,
            },
            disk_per_instance,
            Percent::from_percent(10),
        ));

        // Below 2% raw free (1_999 < 2_000), credit can NEVER mask pressure.
        assert!(disk_under_pressure(
            DiskSample {
                free_bytes: 1_999,
                total_bytes: total,
                graveyard_pending_bytes: 50_000,
            },
            disk_per_instance,
            free_pct,
        ));
    }

    #[test]
    fn disk_pressure_window_honors_quiescence_and_duration() {
        let mut monitor =
            DiskPressureMonitor::new(Percent::from_percent(5), 80_000, Duration::from_secs(5));
        let pressured = Some(DiskSample {
            free_bytes: 3_000,
            total_bytes: 100_000,
            graveyard_pending_bytes: 0,
        });
        assert!(!monitor.observe(pressured, true, t(1_000)));
        assert!(!monitor.observe(pressured, true, t(5_999)));
        // Unsettled quiescence cancels window.
        assert!(!monitor.observe(pressured, false, t(6_000)));
        assert!(!monitor.observe(pressured, true, t(6_500)));
        assert!(monitor.observe(pressured, true, t(11_500)));
    }

    #[test]
    fn soft_ceiling_counts_consecutive_breaches_and_immediate_spikes() {
        let limit = soft_ceiling(1_000, HeadroomPercent::from_percent(20)).expect("positive");
        assert_eq!(limit, 1_200);
        let mut mem = SoftCeilingMonitor::new(limit, NonZeroU32::MIN.saturating_add(1), Some(200));
        assert_eq!(mem.observe(1_201), SoftVerdict::Ok);
        assert_eq!(mem.observe(1_100), SoftVerdict::Ok);
        assert_eq!(mem.consecutive_breaches(), 0);
        assert_eq!(mem.observe(1_201), SoftVerdict::Ok);
        assert_eq!(mem.observe(1_201), SoftVerdict::Kill);

        let mut spike =
            SoftCeilingMonitor::new(limit, NonZeroU32::MIN.saturating_add(4), Some(200));
        assert_eq!(spike.observe(2_400), SoftVerdict::Ok);
        assert_eq!(spike.observe(2_401), SoftVerdict::Kill);
    }

    #[test]
    fn victim_selection_picks_greatest_excess_else_greatest_usage() {
        let candidates = [
            PressureCandidate {
                item: "unsampled",
                usage: 0,
                reserved: 0,
                finished_or_killing: false,
            },
            PressureCandidate {
                item: "killing",
                usage: 900,
                reserved: 100,
                finished_or_killing: true,
            },
            PressureCandidate {
                item: "large-within-reservation",
                usage: 500,
                reserved: 600,
                finished_or_killing: false,
            },
            PressureCandidate {
                item: "over-by-50",
                usage: 150,
                reserved: 100,
                finished_or_killing: false,
            },
        ];
        assert_eq!(pick_pressure_victim(candidates), Some("over-by-50"));

        let within_only = [
            PressureCandidate {
                item: "small",
                usage: 100,
                reserved: 200,
                finished_or_killing: false,
            },
            PressureCandidate {
                item: "large",
                usage: 400,
                reserved: 500,
                finished_or_killing: false,
            },
        ];
        assert_eq!(pick_pressure_victim(within_only), Some("large"));
        assert_eq!(
            pick_pressure_victim(Vec::<PressureCandidate<&str>>::new()),
            None
        );
    }

    #[test]
    fn quiescence_gate_waits_for_both_victim_and_graveyard() {
        let mut gate = QuiescenceGate::new();
        assert!(gate.is_settled());
        gate.begin_kill("attempt-1", true);
        assert!(!gate.is_settled());
        assert_eq!(gate.generation(), 1);
        gate.on_victim_removed(&"attempt-1");
        assert!(!gate.is_settled());
        gate.on_graveyard_processed(&"attempt-1");
        assert!(gate.is_settled());
    }
}
