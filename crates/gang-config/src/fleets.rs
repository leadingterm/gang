//! The worker fleets of `gang.toml`, in the order they are declared, each checked against the
//! other fleets and the configured sandboxes.

use std::num::NonZeroU32;
use std::time::Duration;

use gang_types::{
    Fleet, FleetCatalog, FleetCountError, FleetName, FleetProperties, Isa, MAX_FLEETS, NonZeroBytes,
};
use serde::{Deserialize, Serialize};

use crate::errors::ConfigError;
use crate::fleet_policy::{FleetPolicy, InstanceBounds};
use crate::plugin_set::{FleetOptionsTable, PluginSet, SandboxSelection};

/// One `[[fleets]]` entry as written: the policy fields, the `sandbox` key and the
/// `[fleets.options]` table. Serde rejects unknown and missing keys while the file is parsed.
/// It is public only so a parsed but unresolved configuration can be edited and written back.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawFleet<K> {
    /// Fleet name.
    pub name: FleetName,
    /// Instruction set architecture.
    pub isa: Isa,
    /// Minimum number of instances.
    pub min_instances: u32,
    /// Maximum number of instances.
    pub max_instances: NonZeroU32,
    /// Virtual CPUs per instance.
    pub vcpus_per_instance: NonZeroU32,
    /// Memory per instance.
    pub memory_per_instance: NonZeroBytes,
    /// Disk space per instance.
    pub disk_per_instance: NonZeroBytes,
    /// Target concurrent actions per instance.
    pub target_actions_per_instance: NonZeroU32,
    /// Idle time before an instance can be removed.
    #[serde(with = "gang_types::duration")]
    pub idle_timeout: Duration,
    /// Maximum time to wait for an instance to drain.
    #[serde(with = "gang_types::duration")]
    pub max_drain_wait: Duration,
    /// Grace period for an instance to boot.
    #[serde(with = "gang_types::duration")]
    pub boot_grace: Duration,
    /// Sandbox used by the fleet.
    pub sandbox: K,
    /// Provisioner-specific options.
    #[serde(default)]
    pub options: FleetOptionsTable,
}

impl<K> RawFleet<K> {
    /// Splits the entry into its validated policy, sandbox key and options table.
    fn into_parts(self) -> Result<(FleetPolicy, K, FleetOptionsTable), ConfigError> {
        let RawFleet {
            name,
            isa,
            min_instances,
            max_instances,
            vcpus_per_instance,
            memory_per_instance,
            disk_per_instance,
            target_actions_per_instance,
            idle_timeout,
            max_drain_wait,
            boot_grace,
            sandbox,
            options,
        } = self;
        let instances = InstanceBounds::try_new(min_instances, max_instances).map_err(|_| {
            ConfigError::FleetBounds {
                fleet: name.clone(),
                min_instances,
                max_instances: max_instances.get(),
            }
        })?;
        let policy = FleetPolicy {
            name,
            isa,
            instances,
            vcpus_per_instance,
            memory_per_instance,
            disk_per_instance,
            target_actions_per_instance,
            idle_timeout,
            max_drain_wait,
            boot_grace,
        };
        Ok((policy, sandbox, options))
    }
}

/// The worker fleets in preference order: one column per field, indexed alike.
#[derive(Debug)]
pub struct Fleets<P: PluginSet> {
    policies: Vec<FleetPolicy>,
    sandboxes: Vec<P::SandboxKey>,
    options: Vec<P::FleetOptions>,
    catalog: FleetCatalog,
}

/// A fleet ready to be stored in [`Fleets`]: its policy, sandbox and typed options.
#[derive(Debug)]
pub struct FleetMember<P: PluginSet> {
    /// The fleet's sizing and timing.
    pub policy: FleetPolicy,
    /// The sandbox every action of the fleet runs in.
    pub sandbox: P::SandboxKey,
    /// The typed `[fleets.options]` table.
    pub options: P::FleetOptions,
}

impl<P: PluginSet> Fleets<P> {
    /// Checks each `[[fleets]]` entry in declaration order and collects the fleets.
    /// Parsing rejects an entry of the wrong shape as [`ConfigError::Gang`] before this runs.
    ///
    /// # Errors
    ///
    /// [`ConfigError::TooManyFleets`] when there are more than the supported number of fleets,
    /// [`ConfigError::DuplicateFleet`], [`ConfigError::FleetBounds`],
    /// [`ConfigError::FleetSandboxNotConfigured`], and [`ConfigError::FleetOptions`] for an
    /// entry that is not valid.
    pub(crate) fn resolve(
        raw: Vec<RawFleet<P::SandboxKey>>,
        provisioner: &P::Provisioner,
        sandbox: &P::Sandbox,
    ) -> Result<Self, ConfigError> {
        if raw.len() > MAX_FLEETS {
            return Err(ConfigError::TooManyFleets { count: raw.len() });
        }
        let mut members = Vec::with_capacity(raw.len());
        for raw in raw {
            let (policy, key, options_table) = raw.into_parts()?;

            if members
                .iter()
                .any(|existing: &FleetMember<P>| existing.policy.name == policy.name)
            {
                return Err(ConfigError::DuplicateFleet(policy.name));
            }
            if !sandbox.is_configured(key) {
                return Err(ConfigError::FleetSandboxNotConfigured {
                    fleet: policy.name.clone(),
                    key: key.to_string(),
                });
            }
            let typed = P::fleet_options(provisioner, options_table).map_err(|source| {
                ConfigError::FleetOptions {
                    fleet: policy.name.clone(),
                    source: Box::new(source),
                }
            })?;
            members.push(FleetMember {
                policy,
                sandbox: key,
                options: typed,
            });
        }
        let named: Vec<(&FleetName, &P::FleetOptions)> = members
            .iter()
            .map(|member| (&member.policy.name, &member.options))
            .collect();
        P::validate_fleets(&named).map_err(ConfigError::FleetSet)?;
        Self::try_new(members)
            .map_err(|FleetCountError { count }| ConfigError::TooManyFleets { count })
    }

    /// Builds the fleets from their members in preference order, with one column per field. The
    /// [`FleetCatalog`] is derived from each member's policy, sandbox and options, so the
    /// properties cannot disagree with them.
    ///
    /// [`Self::resolve`] validates each `[[fleets]]` entry before calling this.
    ///
    /// # Errors
    ///
    /// [`FleetCountError`] when there are more fleets than the catalog supports.
    pub fn try_new(members: Vec<FleetMember<P>>) -> Result<Self, FleetCountError> {
        let mut policies = Vec::with_capacity(members.len());
        let mut sandboxes = Vec::with_capacity(members.len());
        let mut options = Vec::with_capacity(members.len());
        let mut properties = Vec::with_capacity(members.len());
        for member in members {
            let pairs = P::option_properties(&member.options);
            properties.push(member.policy.properties(&member.sandbox, pairs));
            policies.push(member.policy);
            sandboxes.push(member.sandbox);
            options.push(member.options);
        }
        let catalog = FleetCatalog::try_new(properties)?;
        Ok(Self {
            policies,
            sandboxes,
            options,
            catalog,
        })
    }

    /// The number of fleets.
    #[must_use]
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    /// Whether no fleet is declared.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// The fleets' policies in preference order.
    #[must_use]
    pub fn policies(&self) -> &[FleetPolicy] {
        &self.policies
    }

    /// The properties of every fleet, in preference order, matched by the scheduler.
    #[must_use]
    pub fn catalog(&self) -> &FleetCatalog {
        &self.catalog
    }

    /// The fleet at `index` in preference order.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<FleetConfig<'_, P>> {
        Some(FleetConfig {
            index,
            policy: self.policies.get(index)?,
            sandbox: *self.sandboxes.get(index)?,
            options: self.options.get(index)?,
            properties: self.catalog.fleets().get(index)?,
        })
    }

    /// Every fleet in preference order.
    pub fn iter(&self) -> impl Iterator<Item = FleetConfig<'_, P>> + '_ {
        (0..self.len()).filter_map(|index| self.get(index))
    }

    /// The preference-order index of the fleet named `name`.
    #[must_use]
    pub fn position(&self, name: &FleetName) -> Option<usize> {
        self.policies.iter().position(|policy| &policy.name == name)
    }

    /// The fleet named `name`.
    #[must_use]
    pub fn by_name(&self, name: &FleetName) -> Option<FleetConfig<'_, P>> {
        self.position(name).and_then(|index| self.get(index))
    }
}

/// One fleet with its sandbox, typed options and platform properties.
#[derive(Debug)]
pub struct FleetConfig<'a, P: PluginSet> {
    /// The fleet's position in preference order.
    pub index: usize,
    /// The fleet's sizing and timing.
    pub policy: &'a FleetPolicy,
    /// The sandbox every action of the fleet runs in.
    pub sandbox: P::SandboxKey,
    /// The typed `[fleets.options]` table.
    pub options: &'a P::FleetOptions,
    /// The platform properties the fleet's workers register and the scheduler matches.
    pub properties: &'a FleetProperties,
}

impl<'a, P: PluginSet> FleetConfig<'a, P> {
    /// The fleet as the provisioner sees it.
    #[must_use]
    pub fn fleet(&self) -> Fleet<'a, P::FleetOptions> {
        Fleet {
            name: &self.policy.name,
            isa: self.policy.isa,
            instance: self.policy.instance(),
            options: self.options,
        }
    }
}

impl<P: PluginSet> Clone for FleetConfig<'_, P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: PluginSet> Copy for FleetConfig<'_, P> {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gang_types::Resources;

    use super::*;

    const FLEET: &str = r#"
name = "gang-arm64"
isa = "aarch64"
min_instances = 0
max_instances = 4
vcpus_per_instance = 8
memory_per_instance = "29GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 2
idle_timeout = "5m"
max_drain_wait = "1h"
boot_grace = "10m"
sandbox = "bwrap"
"#;

    #[test]
    fn parses_isa_and_instance_size() -> Result<(), Box<dyn Error>> {
        let raw: RawFleet<String> = toml::from_str(FLEET)?;
        let (policy, sandbox, options) = raw.into_parts()?;

        assert_eq!(policy.name, "gang-arm64".parse::<FleetName>()?);
        assert_eq!(policy.isa, "aarch64".parse::<Isa>()?);
        assert_eq!(
            policy.instance(),
            Resources {
                vcpus: 8,
                memory: NonZeroBytes::gib(29).get(),
                disk: NonZeroBytes::gib(100).get(),
            }
        );
        assert_eq!(sandbox, "bwrap");
        assert!(options.is_empty());
        Ok(())
    }

    #[test]
    fn rejects_an_unknown_key() -> Result<(), Box<dyn Error>> {
        let error = toml::from_str::<RawFleet<String>>(&format!("{FLEET}bogus = 1\n"))
            .err()
            .ok_or("accepted")?;

        assert!(error.to_string().contains("bogus"), "{error}");
        Ok(())
    }

    #[test]
    fn rejects_a_missing_sandbox() -> Result<(), Box<dyn Error>> {
        let error = toml::from_str::<RawFleet<String>>(&FLEET.replace("sandbox = \"bwrap\"\n", ""))
            .err()
            .ok_or("accepted")?;

        assert!(error.to_string().contains("sandbox"), "{error}");
        Ok(())
    }

    #[test]
    fn rejects_a_missing_disk_per_instance() -> Result<(), Box<dyn Error>> {
        let error = toml::from_str::<RawFleet<String>>(
            &FLEET.replace("disk_per_instance = \"100GiB\"\n", ""),
        )
        .err()
        .ok_or("accepted")?;

        assert!(error.to_string().contains("disk_per_instance"), "{error}");
        Ok(())
    }
}
