//! Fleet-independent reservations and their componentwise nominal-slot projection.

use std::ops::{Add, AddAssign};
use std::time::Duration;

use gang_config::{FleetPolicy, PaybackPercent};
use gang_spi::ResourceRates;
use gang_types::ActionResources;

/// The limiting resource of a scaling decision; ties prefer this declaration order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScalingDimension {
    /// Action concurrency.
    #[default]
    Actions,
    /// Reserved CPU.
    Cpu,
    /// Reserved memory.
    Memory,
    /// Reserved disk.
    Disk,
}

impl ScalingDimension {
    /// The stable audit spelling (not an HTTP enum).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Actions => "actions",
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::Disk => "disk",
        }
    }
}

/// Nonnegative finite raw reservations, occupancy or resource-time work.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct ResourceVector {
    pub actions: f64,
    pub cpu_millicores: f64,
    pub memory: f64,
    pub disk: f64,
}

/// Each coordinate is nominal slots (or slot-seconds), never a per-action maximum.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct SlotVector {
    pub actions: f64,
    pub cpu: f64,
    pub memory: f64,
    pub disk: f64,
}

// Inputs are finite and nonnegative. Saturation keeps arbitrarily large external
// durations/reservations finite through multiplication and fleet-level accumulation.
pub(crate) fn product(a: f64, b: f64) -> f64 {
    if a == 0.0 || b == 0.0 {
        0.0
    } else {
        (a * b).min(f64::MAX)
    }
}

// Reservations are projected into continuous rate units; rounding integers above
// f64's exact range is intentional here, never in stored identifiers or counters.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn u64_to_f64(value: u64) -> f64 {
    value as f64
}

pub(crate) fn u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

macro_rules! vector_ops {
    ($name:ident, $($field:ident),+) => {
        impl $name {
            pub fn scaled(self, factor: f64) -> Self {
                Self { $($field: product(self.$field, factor)),+ }
            }

            pub fn sub_floor(self, rhs: Self) -> Self {
                Self { $($field: (self.$field - rhs.$field).max(0.0)),+ }
            }

            pub fn component_max(self, rhs: Self) -> Self {
                Self { $($field: self.$field.max(rhs.$field)),+ }
            }
        }

        impl Add for $name {
            type Output = Self;
            fn add(self, rhs: Self) -> Self {
                Self { $($field: (self.$field + rhs.$field).min(f64::MAX)),+ }
            }
        }

        impl AddAssign for $name {
            fn add_assign(&mut self, rhs: Self) { *self = *self + rhs; }
        }
    };
}

vector_ops!(ResourceVector, actions, cpu_millicores, memory, disk);
vector_ops!(SlotVector, actions, cpu, memory, disk);

impl ResourceVector {
    pub fn from_action(resources: ActionResources) -> Self {
        Self {
            actions: 1.0,
            cpu_millicores: u64_to_f64(resources.cpu_millicores),
            memory: u64_to_f64(resources.memory.get()),
            disk: u64_to_f64(resources.disk.get()),
        }
    }

    pub fn from_rates(rates: ResourceRates) -> Self {
        Self {
            actions: rates.actions.get(),
            cpu_millicores: rates.cpu_millicores.get(),
            memory: rates.memory.get(),
            disk: rates.disk.get(),
        }
    }

    pub fn normalized(self, policy: &FleetPolicy) -> SlotVector {
        let slots = f64::from(policy.target_actions_per_instance.get());
        SlotVector {
            actions: self.actions,
            cpu: product(
                self.cpu_millicores,
                slots / (1000.0 * f64::from(policy.vcpus_per_instance.get())),
            ),
            memory: product(
                self.memory,
                slots / u64_to_f64(policy.memory_per_instance.get().get()),
            ),
            disk: product(
                self.disk,
                slots / u64_to_f64(policy.disk_per_instance.get().get()),
            ),
        }
    }
}

impl SlotVector {
    pub fn component_min(self, rhs: Self) -> Self {
        Self {
            actions: self.actions.min(rhs.actions),
            cpu: self.cpu.min(rhs.cpu),
            memory: self.memory.min(rhs.memory),
            disk: self.disk.min(rhs.disk),
        }
    }

    pub fn slot_gate(self, slots: u32) -> Self {
        let bound = product(self.actions, f64::from(slots));
        Self {
            actions: self.actions,
            cpu: self.cpu.min(bound),
            memory: self.memory.min(bound),
            disk: self.disk.min(bound),
        }
    }

    pub fn max_component(self) -> f64 {
        self.get(self.max_dimension())
    }

    pub fn max_dimension(self) -> ScalingDimension {
        let mut dimension = ScalingDimension::Actions;
        for next in [
            ScalingDimension::Cpu,
            ScalingDimension::Memory,
            ScalingDimension::Disk,
        ] {
            if self.get(next) > self.get(dimension) {
                dimension = next;
            }
        }
        dimension
    }

    pub fn get(self, dimension: ScalingDimension) -> f64 {
        match dimension {
            ScalingDimension::Actions => self.actions,
            ScalingDimension::Cpu => self.cpu,
            ScalingDimension::Memory => self.memory,
            ScalingDimension::Disk => self.disk,
        }
    }

    pub fn within(self, cap: Self, fraction: f64) -> bool {
        let cap = cap.scaled(fraction);
        self.actions <= cap.actions
            && self.cpu <= cap.cpu
            && self.memory <= cap.memory
            && self.disk <= cap.disk
    }

    pub fn utilization(self, cap: Self) -> Option<f64> {
        if cap.actions == 0.0 {
            return None;
        }
        let ratio = |load: f64, capacity: f64| {
            if capacity > 0.0 {
                (load / capacity).min(1.0)
            } else if load > 0.0 {
                1.0
            } else {
                0.0
            }
        };
        Some(
            ratio(self.actions, cap.actions)
                .max(ratio(self.cpu, cap.cpu))
                .max(ratio(self.memory, cap.memory))
                .max(ratio(self.disk, cap.disk)),
        )
    }
}

/// One exact payback horizon shared by classification, purchases and cordons.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Horizon {
    pub readiness: Duration,
    pub payback: Duration,
}

impl Horizon {
    pub fn new(
        readiness: Duration,
        idle_timeout: Duration,
        payback_percent: PaybackPercent,
    ) -> Self {
        let nanos = readiness
            .as_nanos()
            .saturating_add(idle_timeout.as_nanos())
            .saturating_mul(u128::from(payback_percent.get()))
            / 100;
        let nanos = nanos.min(Duration::MAX.as_nanos());
        Self {
            readiness,
            payback: Duration::new(
                u128_to_u64(nanos / 1_000_000_000),
                (nanos % 1_000_000_000) as u32,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::fleet_policy;

    fn exact(actual: f64, expected: f64) {
        assert_eq!(actual.to_bits(), expected.to_bits());
    }

    #[test]
    fn complementary_resources_sum_before_maximum() {
        let policy = fleet_policy("workers").unwrap();
        let cpu = ResourceVector {
            actions: 1.0,
            cpu_millicores: 1000.0 * f64::from(policy.vcpus_per_instance.get()),
            ..ResourceVector::default()
        };
        let memory = ResourceVector {
            actions: 1.0,
            memory: u64_to_f64(policy.memory_per_instance.get().get()),
            ..ResourceVector::default()
        };
        let slots = f64::from(policy.target_actions_per_instance.get());
        exact(
            (cpu + memory).normalized(&policy).max_component(),
            slots.max(2.0),
        );
        exact(
            cpu.normalized(&policy).max_component() + memory.normalized(&policy).max_component(),
            2.0 * slots,
        );
    }

    #[test]
    fn action_cap_and_slot_gate_preserve_separate_dimensions() {
        let cap = SlotVector {
            actions: 2.0,
            cpu: 16.0,
            memory: 16.0,
            disk: 16.0,
        };
        assert!(
            !SlotVector {
                actions: 3.0,
                ..SlotVector::default()
            }
            .within(cap, 1.0)
        );
        assert!(
            SlotVector {
                cpu: 10.0,
                ..SlotVector::default()
            }
            .within(cap, 1.0)
        );
        assert_eq!(
            SlotVector {
                actions: 0.0,
                ..cap
            }
            .slot_gate(16),
            SlotVector::default()
        );
        exact(
            SlotVector {
                actions: 0.5,
                ..cap
            }
            .slot_gate(16)
            .memory,
            8.0,
        );
    }

    #[test]
    fn dimensions_tie_stably_and_utilization_has_no_zero_division() {
        let equal = SlotVector {
            actions: 1.0,
            cpu: 1.0,
            memory: 1.0,
            disk: 1.0,
        };
        assert_eq!(equal.max_dimension(), ScalingDimension::Actions);
        assert_eq!(
            SlotVector {
                actions: 0.0,
                ..equal
            }
            .max_dimension(),
            ScalingDimension::Cpu
        );
        assert_eq!(equal.utilization(SlotVector::default()), None);
        assert_eq!(
            equal.utilization(equal.scaled(2.0)).map(f64::to_bits),
            Some(0.5_f64.to_bits())
        );
        assert_eq!(
            equal
                .utilization(SlotVector {
                    actions: 2.0,
                    ..SlotVector::default()
                })
                .map(f64::to_bits),
            Some(1.0_f64.to_bits())
        );
    }

    #[test]
    fn horizon_retains_half_millisecond_and_zero() {
        assert_eq!(
            Horizon::new(
                Duration::from_millis(1),
                Duration::ZERO,
                PaybackPercent::default()
            )
            .payback,
            Duration::from_micros(500)
        );
        assert_eq!(
            Horizon::new(Duration::ZERO, Duration::ZERO, PaybackPercent::default()).payback,
            Duration::ZERO
        );
    }

    #[test]
    fn horizon_saturates_after_percentage_not_before() {
        assert_eq!(
            Horizon::new(Duration::MAX, Duration::MAX, PaybackPercent::default()).payback,
            Duration::MAX
        );
        assert_eq!(
            Horizon::new(
                Duration::MAX,
                Duration::MAX,
                PaybackPercent::try_new(1000).unwrap()
            )
            .payback,
            Duration::MAX
        );
    }

    #[test]
    fn numeric_conversions_saturate_integers_and_keep_rate_coordinates_finite() {
        assert_eq!(u128_to_u64(42), 42);
        assert_eq!(u128_to_u64(u128::from(u64::MAX)), u64::MAX);
        assert_eq!(u128_to_u64(u128::MAX), u64::MAX);
        exact(u64_to_f64(16), 16.0);
        assert!(u64_to_f64(u64::MAX).is_finite());
        assert!(u64_to_f64(u64::MAX) > u64_to_f64(16));
    }
}
