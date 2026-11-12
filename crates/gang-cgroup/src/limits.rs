//! Pure computation of the kernel limits an action's scope is given, from the plugin config and
//! what the action requested. No I/O.

use std::fmt;
use std::time::Duration;

use gang_spi::ActionLimits;

use crate::config::{CgroupConfig, OomGroup};

/// The `cpu.max` quota and period, both in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CpuMax {
    /// CPU time the scope may use per period.
    pub(crate) quota_micros: u64,
    /// The CFS period.
    pub(crate) period_micros: u64,
}

impl CpuMax {
    /// The limit for `cpu_count` CPUs over one `period`.
    fn new(cpu_count: u32, period: Duration) -> Self {
        let period_micros = u64::try_from(period.as_micros()).unwrap_or(u64::MAX);
        // Cannot overflow: `cpu_count <= u32::MAX` and the period is at most 1s (1_000_000us),
        // so the product is below 4.3e15, far under `u64::MAX`.
        Self {
            quota_micros: u64::from(cpu_count) * period_micros,
            period_micros,
        }
    }
}

impl fmt::Display for CpuMax {
    /// Formats as the content of a `cpu.max` file: `"<quota> <period>"`.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.quota_micros, self.period_micros)
    }
}

/// The kernel limits written to one action's scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KernelLimits {
    /// The `memory.max` ceiling in bytes; `None` leaves memory unconstrained.
    pub(crate) memory: Option<u64>,
    /// The `memory.swap.max` ceiling in bytes, written with `memory`.
    pub(crate) swap: u64,
    /// The `cpu.max` limit; `None` leaves CPU unconstrained.
    pub(crate) cpu: Option<CpuMax>,
    /// The `pids.max` ceiling.
    pub(crate) pids: u32,
    /// The `memory.oom.group` behaviour of the scope.
    pub(crate) oom_group: OomGroup,
}

impl KernelLimits {
    /// The limits for an action that requested `requested`, under `config`.
    pub(crate) fn new(config: &CgroupConfig, requested: &ActionLimits) -> Self {
        Self {
            memory: requested
                .memory
                .map(|memory| config.memory_headroom_percent.scale_u64(memory.as_u64())),
            swap: config.swap_max.get(),
            cpu: requested
                .cpu_count
                .map(|cpu_count| CpuMax::new(cpu_count.get(), config.cpu_period.get())),
            pids: config.max_pids.get(),
            oom_group: config.oom_group,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use gang_spi::ActionLimits;
    use gang_types::{Bytes, HeadroomPercent, NonZeroBytes};

    use super::KernelLimits;
    use crate::config::{CgroupConfig, CpuPeriod};

    /// Builds the `ActionLimits` an action requests, with `None` for each unrequested resource.
    fn requested(memory: Option<u64>, cpu_count: Option<u32>) -> ActionLimits {
        ActionLimits {
            cpu_count: cpu_count.map(|count| NonZeroU32::new(count).expect("non-zero cpu count")),
            memory: memory.and_then(|bytes| NonZeroBytes::new(Bytes::new(bytes))),
        }
    }

    #[test]
    fn default_headroom_doubles_the_reservation() {
        let gib = Bytes::gib(1).get();
        let limits = KernelLimits::new(&CgroupConfig::default(), &requested(Some(gib), None));
        assert_eq!(limits.memory, Some(2 * gib));
    }

    #[test]
    fn zero_headroom_equals_the_reservation() {
        let gib = Bytes::gib(1).get();
        let config = CgroupConfig {
            memory_headroom_percent: HeadroomPercent::from_percent(0),
            ..CgroupConfig::default()
        };
        let limits = KernelLimits::new(&config, &requested(Some(gib), None));
        assert_eq!(limits.memory, Some(gib));
    }

    #[test]
    fn fifty_percent_headroom_adds_half() {
        let gib = Bytes::gib(1).get();
        let config = CgroupConfig {
            memory_headroom_percent: HeadroomPercent::from_percent(50),
            ..CgroupConfig::default()
        };
        let limits = KernelLimits::new(&config, &requested(Some(gib), None));
        assert_eq!(limits.memory, Some(gib + gib / 2));
    }

    #[test]
    fn memory_limit_rounds_down() {
        let config = CgroupConfig {
            memory_headroom_percent: HeadroomPercent::from_percent(50),
            ..CgroupConfig::default()
        };
        // 3 * 150 / 100 = 4.5, floored to 4.
        let limits = KernelLimits::new(&config, &requested(Some(3), None));
        assert_eq!(limits.memory, Some(4));
    }

    #[test]
    fn memory_limit_saturates_at_u64_max() {
        let limits = KernelLimits::new(&CgroupConfig::default(), &requested(Some(u64::MAX), None));
        assert_eq!(limits.memory, Some(u64::MAX));
    }

    #[test]
    fn no_memory_request_constrains_nothing() {
        let limits = KernelLimits::new(&CgroupConfig::default(), &requested(None, Some(1)));
        assert_eq!(limits.memory, None);
    }

    #[test]
    fn no_request_constrains_neither_memory_nor_cpu() {
        let limits = KernelLimits::new(&CgroupConfig::default(), &requested(None, None));
        assert_eq!(limits.memory, None);
        assert_eq!(limits.cpu, None);
    }

    #[test]
    fn the_cpu_quota_is_the_cpu_count_times_the_period() {
        let limits = KernelLimits::new(&CgroupConfig::default(), &requested(None, Some(3)));
        assert_eq!(
            limits.cpu.map(|cpu| (cpu.quota_micros, cpu.period_micros)),
            Some((300_000, 100_000))
        );

        let config = CgroupConfig {
            cpu_period: CpuPeriod::try_new(Duration::from_secs(1)).expect("1s is in range"),
            ..CgroupConfig::default()
        };
        let limits = KernelLimits::new(&config, &requested(None, Some(u32::MAX)));
        assert_eq!(
            limits.cpu.map(|cpu| cpu.quota_micros),
            Some(4_294_967_295_000_000)
        );
    }
}
