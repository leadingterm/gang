//! Fleet-independent reservations and per-instance resource projections.

use std::num::NonZeroU32;
use std::ops::{Add, AddAssign};
use std::time::Duration;

use gang_config::{FleetPolicy, PaybackPercent};
use gang_spi::ResourceRates;
use gang_types::{ActionResources, property::MILLICORES_PER_CORE};

/// The limiting resource of a scaling decision; ties prefer this declaration order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScalingDimension {
    /// Reserved CPU.
    #[default]
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

/// Each coordinate is a fraction of one physical worker (or worker-seconds).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct CapacityVector {
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
vector_ops!(CapacityVector, cpu, memory, disk);

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

    pub fn normalized(
        self,
        policy: &FleetPolicy,
        cpu_oversubscription: NonZeroU32,
    ) -> CapacityVector {
        let cpu_capacity = u64::from(policy.vcpus_per_instance.get())
            .saturating_mul(MILLICORES_PER_CORE)
            .saturating_mul(u64::from(cpu_oversubscription.get()));
        CapacityVector {
            cpu: self.cpu_millicores / u64_to_f64(cpu_capacity),
            memory: self.memory / u64_to_f64(policy.memory_per_instance.get().get()),
            disk: self.disk / u64_to_f64(policy.disk_per_instance.get().get()),
        }
    }
}

impl CapacityVector {
    pub const ONE: Self = Self {
        cpu: 1.0,
        memory: 1.0,
        disk: 1.0,
    };

    pub fn component_min(self, rhs: Self) -> Self {
        Self {
            cpu: self.cpu.min(rhs.cpu),
            memory: self.memory.min(rhs.memory),
            disk: self.disk.min(rhs.disk),
        }
    }

    pub fn max_component(self) -> f64 {
        self.get(self.max_dimension())
    }

    pub fn max_dimension(self) -> ScalingDimension {
        let mut dimension = ScalingDimension::Cpu;
        for next in [ScalingDimension::Memory, ScalingDimension::Disk] {
            if self.get(next) > self.get(dimension) {
                dimension = next;
            }
        }
        dimension
    }

    pub fn get(self, dimension: ScalingDimension) -> f64 {
        match dimension {
            ScalingDimension::Cpu => self.cpu,
            ScalingDimension::Memory => self.memory,
            ScalingDimension::Disk => self.disk,
        }
    }

    pub fn within(self, cap: Self, fraction: f64) -> bool {
        let cap = cap.scaled(fraction);
        self.cpu <= cap.cpu && self.memory <= cap.memory && self.disk <= cap.disk
    }

    pub fn utilization(self, cap: Self) -> Option<f64> {
        if cap == Self::default() {
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
            ratio(self.cpu, cap.cpu)
                .max(ratio(self.memory, cap.memory))
                .max(ratio(self.disk, cap.disk)),
        )
    }
}

/// One exact horizon shared by classification and scale-in safety.
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
            cpu_millicores: u64_to_f64(MILLICORES_PER_CORE)
                * f64::from(policy.vcpus_per_instance.get()),
            ..ResourceVector::default()
        };
        let memory = ResourceVector {
            actions: 1.0,
            memory: u64_to_f64(policy.memory_per_instance.get().get()),
            ..ResourceVector::default()
        };
        exact(
            (cpu + memory)
                .normalized(&policy, NonZeroU32::MIN)
                .max_component(),
            1.0,
        );
        exact(
            cpu.normalized(&policy, NonZeroU32::MIN).max_component()
                + memory.normalized(&policy, NonZeroU32::MIN).max_component(),
            2.0,
        );
    }

    #[test]
    fn capacity_and_utilization_use_resource_fractions() {
        let cap = CapacityVector::ONE.scaled(2.0);
        let load = CapacityVector {
            cpu: 1.0,
            memory: 1.5,
            disk: 0.5,
        };
        assert!(load.within(cap, 1.0));
        assert!(!load.within(cap, 0.5));
        exact(load.utilization(cap).unwrap(), 0.75);
        assert_eq!(load.utilization(CapacityVector::default()), None);
        assert_eq!(CapacityVector::ONE.max_dimension(), ScalingDimension::Cpu);
        assert_eq!(load.max_dimension(), ScalingDimension::Memory);
        exact(
            load.utilization(CapacityVector {
                cpu: 2.0,
                memory: 0.0,
                disk: 2.0,
            })
            .unwrap(),
            1.0,
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
