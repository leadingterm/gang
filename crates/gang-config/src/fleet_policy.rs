//! One fleet's sizing and timing.

use std::fmt::Display;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_types::{FleetName, FleetProperties, Isa, NonZeroBytes, Resources};

use crate::ScheduledMinimum;

/// Validated lower and upper bounds on the number of fleet instances.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceBounds {
    min: u32,
    scheduled: Vec<ScheduledMinimum>,
    max: NonZeroU32,
}

/// The minimum instance count exceeds the maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("minimum instances exceeds maximum instances")]
pub struct InstanceBoundsError;

/// A scheduled minimum is not above the floor and within the ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a scheduled minimum of {instances} instances is not above the minimum and within the maximum"
)]
pub struct ScheduledMinimumError {
    /// The invalid scheduled instance count.
    pub instances: NonZeroU32,
}

impl InstanceBounds {
    /// Bounds from zero up to max: always valid.
    #[must_use]
    pub const fn up_to(max: NonZeroU32) -> Self {
        Self {
            min: 0,
            scheduled: Vec::new(),
            max,
        }
    }

    /// Creates bounds whose minimum does not exceed their maximum.
    ///
    /// # Errors
    ///
    /// Returns [`InstanceBoundsError`] if `min` exceeds `max`.
    pub fn try_new(min: u32, max: NonZeroU32) -> Result<Self, InstanceBoundsError> {
        if min > max.get() {
            return Err(InstanceBoundsError);
        }
        Ok(Self {
            min,
            scheduled: Vec::new(),
            max,
        })
    }

    /// Adds the scheduled minimums; each must lie above `min` and at most `max`.
    ///
    /// # Errors
    /// Returns [`ScheduledMinimumError`] for the first count outside those bounds.
    pub fn with_schedule(
        mut self,
        scheduled: Vec<ScheduledMinimum>,
    ) -> Result<Self, ScheduledMinimumError> {
        for window in &scheduled {
            let instances = window.instances();
            if instances.get() <= self.min || instances > self.max {
                return Err(ScheduledMinimumError { instances });
            }
        }
        self.scheduled = scheduled;
        Ok(self)
    }

    /// The fewest instances kept at `wall`: `min`, raised to the largest scheduled minimum in force.
    #[must_use]
    pub fn min_at(&self, wall: Timestamp) -> u32 {
        self.scheduled
            .iter()
            .filter(|window| window.in_force(wall))
            .fold(self.min, |min, window| min.max(window.instances().get()))
    }

    /// The most workers ever running.
    #[must_use]
    pub const fn max(&self) -> NonZeroU32 {
        self.max
    }
}

/// How one worker fleet is sized and retired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetPolicy {
    /// The provisioner's name for the fleet (for example an Auto Scaling group, a pod name
    /// prefix or a job comment).
    pub name: FleetName,
    /// The instruction set its workers run.
    pub isa: Isa,
    /// The validated minimum and maximum instance counts.
    pub instances: InstanceBounds,
    /// CPU cores of one worker.
    pub vcpus_per_instance: NonZeroU32,
    /// Memory of one worker. Non-zero.
    pub memory_per_instance: NonZeroBytes,
    /// A lower bound of the disk the actions of one worker may hold in total, valid for every
    /// instance type the fleet may launch: scratch capacity less the fast content store's budget
    /// and a reserve for pressure protection and fast-writer overshoot.
    /// A worker whose scratch volume leaves its actions less than this (its free space at mount
    /// plus its own fast content store, less the fast content store's budget) stops before it
    /// enrolls.
    /// Non-zero.
    pub disk_per_instance: NonZeroBytes,
    /// Concurrent actions one worker is sized for.
    pub target_actions_per_instance: NonZeroU32,
    /// How long a worker may sit idle before it is drained.
    pub idle_timeout: Duration,
    /// How long a draining worker may keep running actions before it is terminated.
    pub max_drain_wait: Duration,
    /// How long an instance may run without its worker connecting before it counts as an orphan.
    pub boot_grace: Duration,
}

impl FleetPolicy {
    /// The size of one worker instance as the provisioner sees it.
    #[must_use]
    pub fn instance(&self) -> Resources {
        Resources {
            vcpus: self.vcpus_per_instance.get(),
            memory: self.memory_per_instance.get(),
            disk: self.disk_per_instance.get(),
        }
    }

    /// The platform properties of this fleet's workers in `sandbox`, with the provisioner `options` as exact entries.
    #[must_use]
    pub fn properties(
        &self,
        sandbox: &impl Display,
        options: impl IntoIterator<Item = (&'static str, String)>,
    ) -> FleetProperties {
        FleetProperties::new(&self.name, self.isa, sandbox, self.instance(), options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{ClockTime, IanaTimeZone, ScheduledMinimum, Weekday, Weekdays};

    fn timestamp(value: &str) -> gang_clock::Timestamp {
        gang_clock::Timestamp::from_rfc3339(value).expect("valid timestamp")
    }

    fn window(
        days: Vec<Weekday>,
        start: &str,
        end: &str,
        zone: &str,
        instances: u32,
    ) -> ScheduledMinimum {
        ScheduledMinimum::try_new(
            NonZeroU32::new(instances).expect("nonzero instances"),
            Weekdays::try_from(days).expect("valid days"),
            start.parse::<ClockTime>().expect("valid start"),
            end.parse::<ClockTime>().expect("valid end"),
            zone.parse::<IanaTimeZone>().expect("valid zone"),
        )
        .expect("valid window")
    }

    fn office(instances: u32) -> ScheduledMinimum {
        window(
            vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            "08:00",
            "19:00",
            "Europe/Paris",
            instances,
        )
    }

    fn bounds(min: u32, windows: Vec<ScheduledMinimum>) -> InstanceBounds {
        InstanceBounds::try_new(min, NonZeroU32::new(4).expect("nonzero"))
            .expect("valid bounds")
            .with_schedule(windows)
            .expect("valid schedule")
    }

    fn assert_floors(bounds: &InstanceBounds, cases: &[(&str, u32)]) {
        for (instant, expected) in cases {
            assert_eq!(bounds.min_at(timestamp(instant)), *expected, "{instant}");
        }
    }

    #[test]
    fn window_start_counts_minutes() {
        let days = vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
        ];
        let w = window(days, "07:55", "19:00", "Europe/Paris", 1);
        assert_floors(
            &bounds(0, vec![w]),
            &[("2026-11-16T06:54:59.999Z", 0), ("2026-11-16T06:55:00Z", 1)],
        );
    }

    #[test]
    fn non_whole_hour_timezone_uses_local_minutes() {
        let w = window(Weekday::ALL.to_vec(), "09:30", "10:00", "Asia/Kathmandu", 1);
        assert_floors(
            &bounds(0, vec![w]),
            &[
                ("2026-11-16T03:44:59.999Z", 0),
                ("2026-11-16T03:45:00Z", 1),
                ("2026-11-16T04:14:59.999Z", 1),
                ("2026-11-16T04:15:00Z", 0),
            ],
        );
    }

    #[test]
    fn local_weekday_not_utc_weekday_decides() {
        let paris = window(vec![Weekday::Mon], "00:00", "01:00", "Europe/Paris", 1);
        assert_floors(&bounds(0, vec![paris]), &[("2026-11-15T23:30:00Z", 1)]);
        let los_angeles = window(
            vec![Weekday::Fri],
            "16:00",
            "24:00",
            "America/Los_Angeles",
            1,
        );
        assert_floors(
            &bounds(0, vec![los_angeles]),
            &[("2026-11-21T00:30:00Z", 1), ("2026-11-22T00:30:00Z", 0)],
        );
    }

    #[test]
    fn office_hours_use_half_open_local_weekdays() {
        assert_floors(
            &bounds(0, vec![office(1)]),
            &[
                ("2026-11-16T06:59:59.999Z", 0),
                ("2026-11-16T07:00:00Z", 1),
                ("2026-11-16T17:59:59.999Z", 1),
                ("2026-11-16T18:00:00Z", 0),
                ("2026-11-21T10:00:00Z", 0),
            ],
        );
    }

    #[test]
    fn office_hours_follow_daylight_saving() {
        assert_floors(
            &bounds(0, vec![office(1)]),
            &[
                ("2026-03-27T06:59:59.999Z", 0),
                ("2026-03-27T07:00:00Z", 1),
                ("2026-03-30T05:59:59.999Z", 0),
                ("2026-03-30T06:00:00Z", 1),
            ],
        );
    }

    #[test]
    fn spring_gap_uses_the_current_wall_clock() {
        let w = window(vec![Weekday::Sun], "02:30", "04:00", "Europe/Paris", 1);
        assert_floors(
            &bounds(0, vec![w]),
            &[("2026-03-29T00:59:59.999Z", 0), ("2026-03-29T01:00:00Z", 1)],
        );
    }

    #[test]
    fn fall_fold_applies_both_occurrences() {
        let w = window(vec![Weekday::Sun], "00:00", "02:30", "Europe/Paris", 1);
        assert_floors(
            &bounds(0, vec![w]),
            &[
                ("2026-10-25T00:15:00Z", 1),
                ("2026-10-25T00:30:00Z", 0),
                ("2026-10-25T00:59:59.999Z", 0),
                ("2026-10-25T01:00:00Z", 1),
                ("2026-10-25T01:29:59.999Z", 1),
                ("2026-10-25T01:30:00Z", 0),
            ],
        );
    }

    #[test]
    fn midnight_ends_the_previous_day_window() {
        let w = window(vec![Weekday::Sat], "22:00", "24:00", "UTC", 1);
        assert_floors(
            &bounds(0, vec![w]),
            &[("2026-11-21T23:59:59.999Z", 1), ("2026-11-22T00:00:00Z", 0)],
        );
    }

    #[test]
    fn overlapping_windows_use_the_largest_floor() {
        let all = window(Weekday::ALL.to_vec(), "00:00", "24:00", "Europe/Paris", 1);
        assert_floors(
            &bounds(0, vec![all, office(3)]),
            &[("2026-11-16T09:00:00Z", 3), ("2026-11-16T19:00:00Z", 1)],
        );
    }

    #[test]
    fn configured_floor_applies_outside_windows() {
        assert_floors(
            &bounds(1, vec![office(2)]),
            &[("2026-11-16T06:00:00Z", 1), ("2026-11-16T09:00:00Z", 2)],
        );
    }

    #[test]
    fn scheduled_counts_must_raise_floor_and_not_exceed_ceiling() {
        let base =
            InstanceBounds::try_new(1, NonZeroU32::new(4).expect("nonzero")).expect("valid bounds");
        for instances in [1, 5] {
            assert_eq!(
                base.clone().with_schedule(vec![office(instances)]),
                Err(ScheduledMinimumError {
                    instances: NonZeroU32::new(instances).expect("nonzero")
                })
            );
        }
        assert!(base.with_schedule(vec![office(4)]).is_ok());
    }

    #[test]
    fn instance_bounds_accept_equal_minimum_and_maximum() {
        let bounds = InstanceBounds::try_new(1, NonZeroU32::MIN).expect("valid bounds");
        assert_eq!(bounds.min_at(timestamp("2026-11-16T00:00:00Z")), 1);
        assert_eq!(bounds.max(), NonZeroU32::MIN);
    }

    #[test]
    fn instance_bounds_reject_a_minimum_above_the_maximum() {
        assert_eq!(
            InstanceBounds::try_new(2, NonZeroU32::MIN),
            Err(InstanceBoundsError)
        );
    }
}
