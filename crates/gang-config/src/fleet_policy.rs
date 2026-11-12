//! One fleet's sizing and timing.

use std::fmt::Display;
use std::num::NonZeroU32;
use std::time::Duration;

use gang_types::{FleetName, FleetProperties, Isa, NonZeroBytes, Resources};

/// Validated lower and upper bounds on the number of fleet instances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceBounds {
    min: u32,
    max: NonZeroU32,
}

/// The minimum instance count exceeds the maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("minimum instances exceeds maximum instances")]
pub struct InstanceBoundsError;

impl InstanceBounds {
    /// Bounds from zero up to max: always valid.
    #[must_use]
    pub const fn up_to(max: NonZeroU32) -> Self {
        Self { min: 0, max }
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
        Ok(Self { min, max })
    }

    /// The fewest workers kept running.
    #[must_use]
    pub const fn min(self) -> u32 {
        self.min
    }

    /// The most workers ever running.
    #[must_use]
    pub const fn max(self) -> NonZeroU32 {
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
    /// A lower bound of the disk the actions of one worker may hold in total, valid for every instance type the fleet may launch: the scratch volume's capacity less the fast content store's budget less a reserve for overshoot. The reserve is at least `disk_per_instance × [scheduler.nativelink.disk] headroom_percent / 100 + write_rate × (10 s + one directory walk)`: `NativeLink` kills an action only above its reservation plus headroom, so a worker packed to this value may legitimately hold that headroom before any fast writer.
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

    #[test]
    fn instance_bounds_accept_equal_minimum_and_maximum() {
        let bounds = InstanceBounds::try_new(1, NonZeroU32::MIN).expect("valid bounds");
        assert_eq!(bounds.min(), 1);
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
