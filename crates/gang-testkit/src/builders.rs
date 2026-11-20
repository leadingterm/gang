//! Sample fleet policies and fleet properties for tests.
//!
//! [`fleet_policy`] is a valid policy a test can adjust with struct-update syntax. [`fleet_of`]
//! derives the [`FleetProperties`] that `gang_config::Fleets` derives for a member with the
//! `noop` sandbox and no options, so tests that build properties by hand agree with the
//! configuration.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_config::{FleetPolicy, InstanceBounds, ScheduledMinimum, Weekday, Weekdays};
use gang_types::{FleetName, FleetProperties, Isa, NonZeroBytes, nonzero_u32};

use crate::TestResult;
use crate::plugins::SandboxKey;

/// A scheduled minimum of `instances` on `days` from `start` to `end` (`HH:MM`) in `time_zone`,
/// as a `[[fleets.scheduled_minimum]]` table writes it.
///
/// # Errors
///
/// When a field is not valid, as parsing `gang.toml` would reject it.
pub fn scheduled_minimum(
    instances: u32,
    days: &[Weekday],
    start: &str,
    end: &str,
    time_zone: &str,
) -> TestResult<ScheduledMinimum> {
    Ok(ScheduledMinimum::try_new(
        NonZeroU32::new(instances).ok_or("zero instances")?,
        Weekdays::try_from(days.to_vec())?,
        start.parse()?,
        end.parse()?,
        time_zone.parse()?,
    )?)
}

/// A valid sample fleet policy named `name`: up to four x86-64 instances of four vCPUs, 8 GiB of
/// memory and 100 GiB of disk, each running four concurrent actions.
///
/// # Errors
///
/// When `name` or the sample action environment is invalid.
pub fn fleet_policy(name: &str) -> TestResult<FleetPolicy> {
    Ok(FleetPolicy {
        name: FleetName::try_new(name)?,
        isa: Isa::X86_64,
        action_environment: "host:test".parse()?,
        instances: InstanceBounds::up_to(nonzero_u32(4)),
        vcpus_per_instance: nonzero_u32(4),
        memory_per_instance: NonZeroBytes::gib(8),
        disk_per_instance: NonZeroBytes::gib(100),
        target_actions_per_instance: nonzero_u32(4),
        idle_timeout: Duration::from_secs(15),
        max_drain_wait: Duration::from_mins(1),
        boot_grace: Duration::from_secs(30),
    })
}

/// The platform properties `gang_config::Fleets` derives for `policy` with the `noop` sandbox and
/// no provisioner options.
#[must_use]
pub fn fleet_of(policy: &FleetPolicy) -> FleetProperties {
    policy.properties(&SandboxKey::Noop, [])
}

/// The platform properties of the sample fleet named `name`, as [`fleet_of`] derives them for
/// [`fleet_policy`].
///
/// # Errors
///
/// When `name` or the sample action environment is invalid.
pub fn fleet(name: &str) -> TestResult<FleetProperties> {
    Ok(fleet_of(&fleet_policy(name)?))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use gang_config::{FleetMember, Fleets, GangConfig};

    use super::*;
    use crate::TestResult;
    use crate::plugins::{Opaque, STANDARD_GANG_TOML, TestPlugins};

    #[test]
    fn scheduled_minimum_builds_window_and_rejects_zero() -> TestResult {
        let days = [Weekday::Mon, Weekday::Tue];
        let expected = ScheduledMinimum::try_new(
            NonZeroU32::new(2).ok_or("zero instances")?,
            Weekdays::try_from(days.to_vec())?,
            "08:00".parse()?,
            "19:00".parse()?,
            "Europe/Paris".parse()?,
        )?;
        assert_eq!(
            scheduled_minimum(2, &days, "08:00", "19:00", "Europe/Paris")?,
            expected
        );
        assert!(scheduled_minimum(0, &days, "08:00", "19:00", "Europe/Paris").is_err());
        Ok(())
    }

    #[test]
    fn fleet_policy_is_the_sample_policy() -> TestResult {
        let policy = fleet_policy("workers")?;
        assert_eq!(policy.name.to_string(), "workers");
        assert_eq!(
            policy,
            FleetPolicy {
                name: FleetName::try_new("workers")?,
                isa: Isa::X86_64,
                action_environment: "host:test".parse()?,
                instances: InstanceBounds::up_to(nonzero_u32(4)),
                vcpus_per_instance: NonZeroU32::new(4).ok_or("four is not zero")?,
                memory_per_instance: NonZeroBytes::gib(8),
                disk_per_instance: NonZeroBytes::gib(100),
                target_actions_per_instance: NonZeroU32::new(4).ok_or("four is not zero")?,
                idle_timeout: Duration::from_secs(15),
                max_drain_wait: Duration::from_mins(1),
                boot_grace: Duration::from_secs(30),
            }
        );
        Ok(())
    }

    #[test]
    fn fleet_policy_rejects_invalid_names() {
        assert!(fleet_policy("").is_err());
        assert!(fleet_policy("Not Valid!").is_err());
    }

    #[test]
    fn fleet_of_matches_what_config_derives() -> TestResult {
        let fleets = Fleets::<TestPlugins>::try_new(vec![
            FleetMember {
                policy: fleet_policy("a")?,
                sandbox: SandboxKey::Noop,
                options: Opaque::default(),
            },
            FleetMember {
                policy: fleet_policy("b")?,
                sandbox: SandboxKey::Noop,
                options: Opaque::default(),
            },
        ])?;
        assert_eq!(fleets.len(), 2);
        let first = fleets.get(0).ok_or("no fleet at index 0")?;
        assert_eq!(first.properties, &fleet("a")?);
        Ok(())
    }

    #[test]
    fn fleet_is_the_properties_of_fleet_policy() -> TestResult {
        assert_eq!(fleet("workers")?, fleet_of(&fleet_policy("workers")?));
        assert!(fleet("").is_err());
        Ok(())
    }

    #[test]
    fn standard_gang_toml_fleet_matches_the_sample() -> TestResult {
        let config: GangConfig<TestPlugins> = STANDARD_GANG_TOML.parse()?;
        assert_eq!(config.fleets.len(), 1);
        let policy = config
            .fleets
            .policies()
            .first()
            .ok_or("no fleet declared")?;
        assert_eq!(policy, &fleet_policy("example-fleet")?);
        Ok(())
    }
}
