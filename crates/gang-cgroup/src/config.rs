//! The `[limits.cgroup]` section of `gang.toml`.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_types::{AbsolutePathBuf, Bytes, HeadroomPercent, NonZeroDuration};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_MEMORY_HEADROOM_PERCENT: HeadroomPercent = HeadroomPercent::from_percent(100);
const DEFAULT_MAX_PIDS: NonZeroU32 = match NonZeroU32::new(4096) {
    Some(pids) => pids,
    None => NonZeroU32::MIN,
};
const DEFAULT_REMOVAL_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REMOVAL_POLL_INTERVAL: NonZeroDuration = NonZeroDuration::from_millis(10);
const DEFAULT_SWAP_MAX: Bytes = Bytes::new(0);
const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// The kernel accepts a CFS period of at least 1ms.
const MIN_CPU_PERIOD: Duration = Duration::from_millis(1);
/// The kernel accepts a CFS period of at most 1s.
const MAX_CPU_PERIOD: Duration = Duration::from_secs(1);

/// The CFS period written to `cpu.max`: a whole number of milliseconds within the 1ms..=1s range
/// that the kernel accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NonZeroDuration", into = "NonZeroDuration")]
pub struct CpuPeriod(NonZeroDuration);

impl CpuPeriod {
    /// The period used when the configuration does not set one: 100ms.
    pub const DEFAULT: Self = Self(NonZeroDuration::from_millis(100));

    /// Wraps `period` if it is a whole number of milliseconds within 1ms..=1s.
    ///
    /// # Errors
    ///
    /// Returns [`CpuPeriodError`] when `period` is not a whole number of milliseconds within
    /// 1ms..=1s.
    pub fn try_new(period: Duration) -> Result<Self, CpuPeriodError> {
        match NonZeroDuration::try_new(period) {
            Ok(nonzero) if (MIN_CPU_PERIOD..=MAX_CPU_PERIOD).contains(&period) => Ok(Self(nonzero)),
            _ => Err(CpuPeriodError { period }),
        }
    }

    /// Returns the period.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0.get()
    }
}

impl TryFrom<NonZeroDuration> for CpuPeriod {
    type Error = CpuPeriodError;

    fn try_from(period: NonZeroDuration) -> Result<Self, Self::Error> {
        Self::try_new(period.get())
    }
}

impl From<CpuPeriod> for NonZeroDuration {
    fn from(period: CpuPeriod) -> Self {
        period.0
    }
}

/// A CFS period that is not a whole number of milliseconds within 1ms..=1s.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("cpu_period {period:?} is not a whole number of milliseconds in 1ms..=1s")]
pub struct CpuPeriodError {
    period: Duration,
}

gang_types::string_enum! {
    /// How `prepare` establishes that the worker's cgroup subtree is delegated to it.
    pub enum Delegation("delegation") {
        /// Requires systemd's delegate marker (`user.delegate` or `trusted.delegate` = 1) on the
        /// cgroup directory. systemd 251 or later sets it on units with `Delegate=yes`.
        SystemdMarker = "systemd_marker",
        /// Trusts the operator: no check, for supervisors that delegate the subtree without the
        /// marker.
        Trusted = "trusted",
    }
}

impl Default for Delegation {
    /// Requires the systemd delegate marker by default.
    fn default() -> Self {
        Self::SystemdMarker
    }
}

gang_types::string_enum! {
    /// Whether the kernel OOM killer takes down an action's scope as a whole or per process.
    pub enum OomGroup("oom group") {
        /// The kernel OOM killer takes down every process of the action's scope together
        /// (`memory.oom.group` = 1). Defaults to this.
        Group = "group",
        /// The kernel OOM killer may kill single processes of the action's scope
        /// (`memory.oom.group` = 0).
        Process = "process",
    }
}

impl Default for OomGroup {
    /// Kills the whole scope by default.
    fn default() -> Self {
        Self::Group
    }
}

impl OomGroup {
    /// The content written to `memory.oom.group`.
    #[must_use]
    pub(crate) const fn value(self) -> &'static str {
        match self {
            Self::Group => "1",
            Self::Process => "0",
        }
    }
}

/// Configuration of the cgroup v2 limits. Every field is optional and defaults as documented on
/// the field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CgroupConfig {
    /// Percentage of the memory reservation added on top of it to form `memory.max`. Defaults to
    /// 100, so `memory.max` is twice the reservation.
    pub memory_headroom_percent: HeadroomPercent,
    /// Value written to `memory.swap.max` together with `memory.max`. Defaults to 0: the memory
    /// ceiling is hard, so an action over it is killed rather than swapped. Skipped when the
    /// kernel provides no swap accounting.
    pub swap_max: Bytes,
    /// Maximum number of processes and threads of every action (`pids.max`). Defaults to 4096.
    pub max_pids: NonZeroU32,
    /// The CFS period written to `cpu.max`. Defaults to 100ms.
    pub cpu_period: CpuPeriod,
    /// How long `leave` waits for killed processes to exit before giving up on removing the
    /// scope. Defaults to 10s.
    #[serde(with = "gang_types::duration")]
    pub removal_timeout: Duration,
    /// Pause between removal attempts. Defaults to 10ms.
    pub removal_poll_interval: NonZeroDuration,
    /// How `prepare` checks that the worker's cgroup subtree is delegated to it. Defaults to
    /// `systemd_marker`, which requires systemd's delegate marker on the cgroup directory.
    pub delegation: Delegation,
    /// The mount point of the cgroup v2 filesystem. Defaults to `/sys/fs/cgroup`. A relative path
    /// is rejected when the configuration is parsed.
    pub cgroup_root: AbsolutePathBuf,
    /// Whether the kernel OOM killer takes down every process of an action's scope together
    /// (`group`) or may kill single processes (`process`). Defaults to `group`.
    pub oom_group: OomGroup,
}

impl Default for CgroupConfig {
    fn default() -> Self {
        Self {
            memory_headroom_percent: DEFAULT_MEMORY_HEADROOM_PERCENT,
            swap_max: DEFAULT_SWAP_MAX,
            max_pids: DEFAULT_MAX_PIDS,
            cpu_period: CpuPeriod::DEFAULT,
            removal_timeout: DEFAULT_REMOVAL_TIMEOUT,
            removal_poll_interval: DEFAULT_REMOVAL_POLL_INTERVAL,
            delegation: Delegation::SystemdMarker,
            cgroup_root: AbsolutePathBuf::rooted(DEFAULT_CGROUP_ROOT),
            oom_group: OomGroup::Group,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::time::Duration;

    use gang_types::{AbsolutePathBuf, Bytes, HeadroomPercent, NonZeroDuration};

    use super::{CgroupConfig, CpuPeriod, Delegation, OomGroup};

    const COMPLETE: &str = r#"
memory_headroom_percent = 50
swap_max = "1GiB"
max_pids = 128
cpu_period = "50ms"
removal_timeout = "3s"
removal_poll_interval = "20ms"
delegation = "trusted"
oom_group = "process"
cgroup_root = "/var/lib/gang/cgroup"
"#;

    fn custom() -> CgroupConfig {
        CgroupConfig {
            memory_headroom_percent: HeadroomPercent::from_percent(50),
            swap_max: Bytes::new(1 << 30),
            max_pids: NonZeroU32::new(128).expect("128 is non-zero"),
            cpu_period: CpuPeriod::try_new(Duration::from_millis(50)).expect("50ms is in range"),
            removal_timeout: Duration::from_secs(3),
            removal_poll_interval: NonZeroDuration::from_millis(20),
            delegation: Delegation::Trusted,
            oom_group: OomGroup::Process,
            cgroup_root: AbsolutePathBuf::try_new("/var/lib/gang/cgroup")
                .expect("the test cgroup root is absolute"),
        }
    }

    #[test]
    fn empty_section_gives_defaults() {
        let config: CgroupConfig = toml::from_str("").expect("empty section parses");
        assert_eq!(config, CgroupConfig::default());
        assert_eq!(config.memory_headroom_percent.get(), 100);
        assert_eq!(config.swap_max, Bytes::new(0));
        assert_eq!(config.max_pids.get(), 4096);
        assert_eq!(config.cpu_period.get(), Duration::from_millis(100));
        assert_eq!(config.removal_timeout, Duration::from_secs(10));
        assert_eq!(
            config.removal_poll_interval,
            NonZeroDuration::from_millis(10)
        );
        assert_eq!(config.delegation, Delegation::SystemdMarker);
        assert_eq!(config.cgroup_root.as_path(), Path::new("/sys/fs/cgroup"));
        assert_eq!(config.oom_group, OomGroup::Group);
    }

    #[test]
    fn every_field_parses_from_toml() {
        let config: CgroupConfig = toml::from_str(COMPLETE).expect("complete section parses");
        assert_eq!(config, custom());
    }

    #[test]
    fn round_trips_custom_values_through_toml() {
        let rendered = toml::to_string(&custom()).expect("config renders");
        let parsed: CgroupConfig = toml::from_str(&rendered).expect("rendered config parses");
        assert_eq!(parsed, custom());
    }

    #[test]
    fn round_trips_defaults_through_toml() {
        let rendered = toml::to_string(&CgroupConfig::default()).expect("config renders");
        let parsed: CgroupConfig = toml::from_str(&rendered).expect("rendered config parses");
        assert_eq!(parsed, CgroupConfig::default());
    }

    #[test]
    fn delegation_parses_both_values() {
        let trusted: CgroupConfig =
            toml::from_str(r#"delegation = "trusted""#).expect("trusted parses");
        assert_eq!(trusted.delegation, Delegation::Trusted);
        let marker: CgroupConfig =
            toml::from_str(r#"delegation = "systemd_marker""#).expect("systemd_marker parses");
        assert_eq!(marker.delegation, Delegation::SystemdMarker);
    }

    #[test]
    fn rejects_unknown_delegation_value() {
        let error = toml::from_str::<CgroupConfig>(r#"delegation = "bogus""#)
            .expect_err("unknown delegation is rejected");
        assert!(error.message().contains("bogus"));
    }

    #[test]
    fn rejects_unknown_key() {
        let error =
            toml::from_str::<CgroupConfig>("unknown = 1\n").expect_err("unknown key is rejected");
        assert!(error.message().contains("unknown"));
    }

    #[test]
    fn rejects_zero_max_pids() {
        let error = toml::from_str::<CgroupConfig>("max_pids = 0\n")
            .expect_err("zero max_pids is rejected");
        assert!(error.to_string().contains("max_pids"));
    }

    #[test]
    fn rejects_a_malformed_swap_max() {
        let error = toml::from_str::<CgroupConfig>(r#"swap_max = "lots""#)
            .expect_err("malformed swap_max is rejected");
        assert!(error.to_string().contains("swap_max"));
    }

    #[test]
    fn rejects_a_cpu_period_over_one_second() {
        let error = toml::from_str::<CgroupConfig>("cpu_period = \"2s\"")
            .expect_err("cpu_period above 1s is rejected");
        assert!(error.to_string().contains("cpu_period"));
    }

    #[test]
    fn cpu_period_range_is_enforced_by_the_type() {
        assert!(CpuPeriod::try_new(Duration::from_micros(999)).is_err());
        assert!(CpuPeriod::try_new(Duration::from_millis(1001)).is_err());
        let message = CpuPeriod::try_new(Duration::from_micros(999))
            .expect_err("below 1ms")
            .to_string();
        assert!(message.contains("1ms..=1s"));
    }

    #[test]
    fn rejects_a_cpu_period_that_is_not_whole_milliseconds() {
        let message = CpuPeriod::try_new(Duration::from_micros(1500))
            .expect_err("not whole milliseconds")
            .to_string();
        assert!(
            message.contains("whole number of milliseconds"),
            "{message}"
        );
    }

    #[test]
    fn accepts_the_period_bounds() {
        for (text, period) in [
            ("1ms", Duration::from_millis(1)),
            ("1s", Duration::from_secs(1)),
        ] {
            let config: CgroupConfig =
                toml::from_str(&format!("cpu_period = \"{text}\"")).expect("period bound parses");
            assert_eq!(config.cpu_period.get(), period, "{text} is accepted");
        }
    }

    #[test]
    fn rejects_a_zero_removal_poll_interval() {
        let error = toml::from_str::<CgroupConfig>("removal_poll_interval = \"0s\"")
            .expect_err("zero removal_poll_interval is rejected");
        assert!(error.to_string().contains("removal_poll_interval"));
    }

    #[test]
    fn rejects_a_relative_cgroup_root() {
        let error = toml::from_str::<CgroupConfig>(r#"cgroup_root = "sys/fs/cgroup""#)
            .expect_err("relative cgroup_root is rejected");
        assert!(error.message().contains("not absolute"));
    }

    #[test]
    fn oom_group_parses_both_values() {
        let group: CgroupConfig = toml::from_str(r#"oom_group = "group""#).expect("group parses");
        assert_eq!(group.oom_group, OomGroup::Group);
        let process: CgroupConfig =
            toml::from_str(r#"oom_group = "process""#).expect("process parses");
        assert_eq!(process.oom_group, OomGroup::Process);
    }

    #[test]
    fn rejects_unknown_oom_group_value() {
        let error = toml::from_str::<CgroupConfig>(r#"oom_group = "bogus""#)
            .expect_err("unknown oom_group is rejected");
        assert!(error.message().contains("bogus"));
    }
}
