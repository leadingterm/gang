use std::time::Duration;

use gang_clock::{Timestamp, millis_of};
use gang_wire::{Timeline, TimelineBucket};

const MILLIS_PER_SECOND: u64 = 1_000;
const INITIAL_STEP: Duration = Duration::from_secs(1);
const STEP_MULTIPLIER: u32 = 2;

#[derive(Debug)]
pub(crate) struct History {
    pub(crate) timeline: Timeline,
    cap: usize,
}

fn merged(a: &TimelineBucket, b: &TimelineBucket) -> TimelineBucket {
    TimelineBucket {
        hits: a.hits + b.hits,
        misses: a.misses + b.misses,
        executing: a.executing + b.executing,
        queued: a.queued + b.queued,
    }
}

impl History {
    pub(crate) fn new(at: Timestamp, cap: usize) -> Self {
        let start = at.as_unix_millis() / MILLIS_PER_SECOND * MILLIS_PER_SECOND;
        Self {
            timeline: Timeline {
                start_time: Timestamp::from_unix_millis(start),
                step: INITIAL_STEP,
                buckets: Vec::with_capacity(cap),
            },
            cap,
        }
    }

    fn index(&self, offset: u64) -> Option<usize> {
        // A step wider than the entire timestamp range contains every possible offset.
        if self.timeline.step > Duration::from_millis(u64::MAX) {
            Some(0)
        } else {
            usize::try_from(offset / millis_of(self.timeline.step)).ok()
        }
    }

    fn ensure(&mut self, at: Timestamp) -> Option<usize> {
        let offset = at
            .as_unix_millis()
            .saturating_sub(self.timeline.start_time.as_unix_millis());
        while self.index(offset).is_none_or(|index| index >= self.cap) {
            let len = self.timeline.buckets.len();
            for i in 0..len / 2 {
                self.timeline.buckets[i] = merged(
                    &self.timeline.buckets[2 * i],
                    &self.timeline.buckets[2 * i + 1],
                );
            }
            if !len.is_multiple_of(2) {
                let last = std::mem::take(&mut self.timeline.buckets[len - 1]);
                self.timeline.buckets[len / 2] = last;
            }
            self.timeline.buckets.truncate(len.div_ceil(2));
            // At most one doubling beyond the u64 millisecond range is ever needed.
            self.timeline.step *= STEP_MULTIPLIER;
        }
        let index = self.index(offset)?;
        if self.timeline.buckets.len() <= index {
            self.timeline
                .buckets
                .resize_with(index + 1, TimelineBucket::default);
        }
        Some(index)
    }

    pub(crate) fn advance(&mut self, at: Timestamp) {
        let _ = self.ensure(at);
    }

    pub(crate) fn point(&mut self, at: Timestamp, hit: bool) {
        if let Some(index) = self.ensure(at) {
            let bucket = &mut self.timeline.buckets[index];
            if hit {
                bucket.hits += 1;
            } else {
                bucket.misses += 1;
            }
        }
    }

    pub(crate) fn interval(
        &mut self,
        start: Timestamp,
        end: Timestamp,
        at: Timestamp,
        queued: bool,
    ) {
        let start = start
            .as_unix_millis()
            .max(self.timeline.start_time.as_unix_millis());
        let end = end.as_unix_millis().min(at.as_unix_millis());
        if start >= end {
            return;
        }
        // The end is exclusive; the last covered millisecond determines the last bucket.
        if self.ensure(Timestamp::from_unix_millis(end - 1)).is_none() {
            return;
        }
        let base = self.timeline.start_time.as_unix_millis();
        let mut cursor = start;
        while cursor < end {
            let offset = cursor - base;
            let Some(index) = self.index(offset) else {
                return;
            };
            let remaining = if self.timeline.step > Duration::from_millis(u64::MAX) {
                end - cursor
            } else {
                let step = millis_of(self.timeline.step);
                step - offset % step
            };
            let next = cursor.saturating_add(remaining).min(end);
            let elapsed = Duration::from_millis(next - cursor);
            let bucket = &mut self.timeline.buckets[index];
            if queued {
                bucket.queued += elapsed;
            } else {
                bucket.executing += elapsed;
            }
            cursor = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earlier_point_clamps_to_first_bucket() {
        let mut history = History::new(Timestamp::from_unix_millis(1_200), 4);
        history.point(Timestamp::from_unix_millis(999), true);
        assert_eq!(history.timeline.buckets[0].hits, 1);
    }

    #[test]
    fn split_intervals_and_pairwise_merges_keep_bucket_placement() {
        let at = Timestamp::from_unix_millis;
        let mut history = History::new(at(0), 4);
        history.point(at(0), true);
        history.point(at(1_000), false);
        history.point(at(2_000), true);
        history.point(at(3_000), false);
        history.interval(at(500), at(1_500), at(3_000), false);
        assert_eq!(
            history.timeline.buckets[0].executing,
            Duration::from_millis(500)
        );
        assert_eq!(
            history.timeline.buckets[1].executing,
            Duration::from_millis(500)
        );
        history.point(at(4_000), true);
        assert_eq!(history.timeline.step, Duration::from_secs(2));
        let buckets = &history.timeline.buckets;
        assert_eq!(
            (buckets[0].hits, buckets[0].misses, buckets[0].executing),
            (1, 1, Duration::from_secs(1))
        );
        assert_eq!(
            (buckets[1].hits, buckets[1].misses, buckets[1].executing),
            (1, 1, Duration::ZERO)
        );
        assert_eq!((buckets[2].hits, buckets[2].misses), (1, 0));
    }

    #[test]
    fn single_bucket_covers_the_entire_timestamp_range() {
        let mut history = History::new(Timestamp::UNIX_EPOCH, 1);
        history.point(Timestamp::UNIX_EPOCH, true);
        let end = Timestamp::from_unix_millis(u64::MAX);
        history.point(end, false);
        history.interval(Timestamp::UNIX_EPOCH, end, end, true);
        assert_eq!(history.timeline.buckets.len(), 1);
        let bucket = &history.timeline.buckets[0];
        assert_eq!((bucket.hits, bucket.misses), (1, 1));
        assert_eq!(bucket.queued, Duration::from_millis(u64::MAX));
        assert!(history.timeline.step > Duration::from_millis(u64::MAX));
    }
}
