//! Pure computation of the kernel limits an action's scope is given, from the plugin config and
//! what the action requested. No I/O.

use std::fmt;
use std::time::Duration;

use gang_spi::ActionLimits;
use gang_types::CpuWeight;

use crate::config::CgroupConfig;

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
    /// The `cpu.max` limit; `None` leaves CPU unconstrained.
    pub(crate) cpu: Option<CpuMax>,
    /// The `cpu.weight` share; `None` leaves the parent's default.
    pub(crate) cpu_weight: Option<CpuWeight>,
    /// The `pids.max` ceiling.
    pub(crate) pids: u32,
}

impl KernelLimits {
    /// The limits for an action that requested `requested`, under `config`.
    pub(crate) fn new(config: &CgroupConfig, requested: &ActionLimits) -> Self {
        Self {
            cpu: requested
                .cpu_count
                .map(|cpu_count| CpuMax::new(cpu_count.get(), config.cpu_period.get())),
            cpu_weight: requested.cpu_weight,
            pids: config.max_pids.get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use gang_spi::ActionLimits;
    use gang_types::{Bytes, CpuWeight, NonZeroBytes};

    use super::KernelLimits;
    use crate::config::{CgroupConfig, CpuPeriod};

    /// Builds the `ActionLimits` an action requests, with `None` for each unrequested resource.
    fn requested(memory: Option<u64>, cpu_count: Option<u32>) -> ActionLimits {
        ActionLimits {
            cpu_count: cpu_count.map(|count| NonZeroU32::new(count).expect("non-zero cpu count")),
            memory: memory.and_then(|bytes| NonZeroBytes::new(Bytes::new(bytes))),
            cpu_weight: None,
        }
    }

    #[test]
    fn no_request_leaves_cpu_unconstrained() {
        let limits = KernelLimits::new(&CgroupConfig::default(), &requested(None, None));
        assert_eq!(limits.cpu, None);
        assert_eq!(limits.cpu_weight, None);
    }

    #[test]
    fn requested_cpu_weight_is_forwarded() {
        let limits = KernelLimits::new(
            &CgroupConfig::default(),
            &ActionLimits {
                cpu_weight: Some(CpuWeight::GATE),
                ..requested(None, None)
            },
        );
        assert_eq!(limits.cpu_weight, Some(CpuWeight::GATE));
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
