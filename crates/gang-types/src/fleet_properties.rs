//! The properties one fleet registers with the scheduler, and the rule its requests match by.

use std::fmt::Display;

use crate::{
    ActionEnvironment, Capability, FleetActionPolicy, FleetName, Isa, NetworkRequest, NonZeroBytes,
    Resources, property,
};

/// How a request value matches a fleet's value for one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchKind {
    /// The request value must equal the fleet's value.
    Exact,
    /// The request value must be a number no greater than the fleet's.
    Minimum,
    /// A requested capability requires an immutable fleet grant.
    Capability,
    /// The key imposes nothing on matching.
    Ignored,
}

/// How requests match one entry. A key that imposes nothing is never an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rule {
    /// The request value must equal the fleet's value.
    Exact,
    /// The request value must be no greater than the fleet's limit.
    Minimum(u64),
    /// A capability granted by this fleet.
    Capability,
}

/// One property of a fleet: its key, its value, and how requests match it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    key: &'static str,
    value: String,
    rule: Rule,
    capacity: Option<u64>,
}

/// One fleet's properties: its name, ISA, sandbox, instance size and provisioner options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetProperties {
    entries: Vec<Entry>,
    option_count: usize,
    actions: FleetActionPolicy,
}

impl FleetProperties {
    const FLEET_POSITION: usize = 0;
    const SANDBOX_POSITION: usize = 2;
    const ENVIRONMENT_POSITION: usize = 3;
    const CPU_POSITION: usize = 4;
    const MEMORY_POSITION: usize = 6;
    const FIXED_ENTRY_COUNT: usize = 8;

    /// Creates a fleet's properties.
    ///
    /// The entries are the fleet, ISA, sandbox, action environment, `cpu_count`,
    /// `cpu_millicores`, `memory_kb` and `disk_kb`; resource entries are minimums,
    /// with memory and disk in KiB. Provisioner options follow as exact entries.
    pub fn new(
        name: &FleetName,
        isa: Isa,
        sandbox: &impl Display,
        environment: &ActionEnvironment,
        instance: Resources,
        actions: &FleetActionPolicy,
        options: impl IntoIterator<Item = (&'static str, String)>,
    ) -> Self {
        let effective = actions.resources.effective(instance);
        let fixed: [Entry; Self::FIXED_ENTRY_COUNT] = [
            Entry {
                key: property::FLEET,
                value: name.to_string(),
                rule: Rule::Exact,
                capacity: None,
            },
            Entry {
                key: property::ISA,
                value: isa.to_string(),
                rule: Rule::Exact,
                capacity: None,
            },
            Entry {
                key: property::SANDBOX,
                value: sandbox.to_string(),
                rule: Rule::Exact,
                capacity: None,
            },
            Entry {
                key: property::ACTION_ENVIRONMENT,
                value: environment.to_string(),
                rule: Rule::Exact,
                capacity: None,
            },
            minimum(
                property::CPU_COUNT,
                u64::from(instance.vcpus),
                u64::from(effective.vcpus),
            ),
            minimum(
                property::CPU_MILLICORES,
                u64::from(instance.vcpus) * property::MILLICORES_PER_CORE,
                u64::from(effective.vcpus) * property::MILLICORES_PER_CORE,
            ),
            minimum(
                property::MEMORY_KB,
                instance.memory.get() / 1024,
                effective.memory.get() / 1024,
            ),
            minimum(
                property::DISK_KB,
                instance.disk.get() / 1024,
                effective.disk.get() / 1024,
            ),
        ];
        let mut entries = Vec::from(fixed);
        entries.extend(options.into_iter().map(|(key, value)| Entry {
            key,
            value,
            rule: Rule::Exact,
            capacity: None,
        }));
        let option_count = entries.len() - Self::FIXED_ENTRY_COUNT;
        for capability in &actions.grants {
            match capability {
                Capability::Network => entries.push(Entry {
                    key: capability.property(),
                    value: NetworkRequest::Requested.as_str().to_owned(),
                    rule: Rule::Capability,
                    capacity: None,
                }),
            }
        }
        Self {
            entries,
            option_count,
            actions: actions.clone(),
        }
    }

    /// The sandbox the fleet's workers run actions in.
    #[must_use]
    pub fn sandbox(&self) -> &str {
        self.entries[Self::SANDBOX_POSITION].value.as_str()
    }

    /// Replaces the launch identity with the actual local sandbox identity.
    pub fn set_action_environment(&mut self, environment: &ActionEnvironment) {
        self.entries[Self::ENVIRONMENT_POSITION].value = environment.to_string();
    }

    /// Replaces estimated pools with actual worker capacity, preserving configured action ceilings.
    pub fn set_capacity(&mut self, actual: Resources) {
        let effective = self.actions.resources.effective(actual);
        for (offset, key, capacity, limit) in [
            (
                0,
                property::CPU_COUNT,
                u64::from(actual.vcpus),
                u64::from(effective.vcpus),
            ),
            (
                1,
                property::CPU_MILLICORES,
                u64::from(actual.vcpus) * property::MILLICORES_PER_CORE,
                u64::from(effective.vcpus) * property::MILLICORES_PER_CORE,
            ),
            (
                2,
                property::MEMORY_KB,
                actual.memory.get() / 1024,
                effective.memory.get() / 1024,
            ),
            (
                3,
                property::DISK_KB,
                actual.disk.get() / 1024,
                effective.disk.get() / 1024,
            ),
        ] {
            self.entries[Self::CPU_POSITION + offset] = minimum(key, capacity, limit);
        }
    }

    /// Replaces the estimated memory with actual worker capacity, preserving the action ceiling.
    pub fn set_memory_capacity(&mut self, actual: NonZeroBytes) {
        let capacity = actual.as_u64() / 1024;
        let limit = self
            .actions
            .resources
            .memory
            .map_or(capacity, |ceiling| capacity.min(ceiling.as_u64() / 1024));
        self.entries[Self::MEMORY_POSITION] = minimum(property::MEMORY_KB, capacity, limit);
    }

    /// The fleet's name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.entries[Self::FLEET_POSITION].value.as_str()
    }

    /// The limit of the minimum entry named `key`, if the fleet has one: memory and disk in
    /// KiB, CPU in cores or millicores.
    #[must_use]
    pub fn limit(&self, key: &str) -> Option<u64> {
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .and_then(|entry| match entry.rule {
                Rule::Minimum(limit) => Some(limit),
                Rule::Exact | Rule::Capability => None,
            })
    }

    /// Physical worker capacity for a resource key, independent of action ceilings.
    #[must_use]
    pub fn capacity(&self, key: &str) -> Option<u64> {
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .and_then(|entry| entry.capacity)
    }

    /// The fleet's configured action grants and ceilings.
    #[must_use]
    pub fn actions(&self) -> &FleetActionPolicy {
        &self.actions
    }

    /// Explicitly configured per-action resource ceilings in scheduler units.
    pub fn action_ceilings(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        [
            self.actions.resources.cpu_count.map(|v| {
                (
                    property::CPU_MILLICORES,
                    u64::from(v.get()) * property::MILLICORES_PER_CORE,
                )
            }),
            self.actions
                .resources
                .memory
                .map(|v| (property::MEMORY_KB, v.as_u64() / 1024)),
            self.actions
                .resources
                .disk
                .map(|v| (property::DISK_KB, v.as_u64() / 1024)),
        ]
        .into_iter()
        .flatten()
    }

    /// The provisioner options, in registration order: every entry after the fleet, ISA,
    /// sandbox and instance size.
    pub fn options(&self) -> impl Iterator<Item = (&'static str, &str)> + '_ {
        self.entries
            .iter()
            .skip(Self::FIXED_ENTRY_COUNT)
            .take(self.option_count)
            .map(|entry| (entry.key, entry.value.as_str()))
    }

    /// Iterates over every property in registration order, as the worker registers it.
    pub fn worker_properties(&self) -> impl Iterator<Item = (&'static str, &str)> + '_ {
        self.entries
            .iter()
            .map(|entry| (entry.key, entry.value.as_str()))
    }

    /// Returns the value of the property named `key`, if the fleet has one.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.value.as_str())
    }

    /// Iterates over every entry's key and how it matches, in registration order.
    pub(crate) fn declarations(&self) -> impl Iterator<Item = (&'static str, MatchKind)> + '_ {
        self.entries.iter().map(|entry| {
            if entry.key == property::ACTION_ENVIRONMENT {
                return (entry.key, MatchKind::Ignored);
            }
            let kind = match entry.rule {
                Rule::Exact => MatchKind::Exact,
                Rule::Minimum(_) => MatchKind::Minimum,
                Rule::Capability => MatchKind::Capability,
            };
            (entry.key, kind)
        })
    }

    /// Iterates over every entry's key, value and how it matches, in registration order.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&'static str, &str, Rule)> + '_ {
        self.entries
            .iter()
            .map(|entry| (entry.key, entry.value.as_str(), entry.rule))
    }
}

/// Creates an entry that matches requests up to `limit`.
fn minimum(key: &'static str, capacity: u64, limit: u64) -> Entry {
    Entry {
        key,
        value: capacity.to_string(),
        rule: Rule::Minimum(limit),
        capacity: Some(capacity),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bytes, FleetCatalog};

    fn gang_x86() -> FleetProperties {
        let name = FleetName::try_new("gang-x86").unwrap();
        FleetProperties::new(
            &name,
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().unwrap(),
            Resources {
                vcpus: 15,
                memory: Bytes::new(29 << 30),
                disk: Bytes::new(100 << 30),
            },
            &FleetActionPolicy::default(),
            [("capacity", "spot".to_owned())],
        )
    }

    #[test]
    fn sandbox_returns_the_sandbox_given_to_new() {
        assert_eq!(gang_x86().sandbox(), "bwrap");
    }

    #[test]
    fn name_is_the_fleet_name() {
        assert_eq!(gang_x86().name(), "gang-x86");
    }

    #[test]
    fn limit_reads_minimum_entries_only() {
        let fleet = gang_x86();
        assert_eq!(fleet.limit(property::MEMORY_KB), Some(29 * 1024 * 1024));
        assert_eq!(fleet.limit(property::SANDBOX), None);
        assert_eq!(fleet.limit("absent"), None);
    }

    #[test]
    fn options_return_only_provisioner_options_in_order() {
        let name = FleetName::try_new("gang-x86").unwrap();
        let instance = Resources {
            vcpus: 15,
            memory: Bytes::new(29 << 30),
            disk: Bytes::new(100 << 30),
        };
        let properties = FleetProperties::new(
            &name,
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().unwrap(),
            instance,
            &FleetActionPolicy::default(),
            [("capacity", "spot".to_owned()), ("zone", "west".to_owned())],
        );
        assert_eq!(
            properties.options().collect::<Vec<_>>(),
            [("capacity", "spot"), ("zone", "west")]
        );

        let without_options = FleetProperties::new(
            &name,
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().unwrap(),
            instance,
            &FleetActionPolicy::default(),
            [],
        );
        assert_eq!(without_options.options().count(), 0);
    }

    #[test]
    fn worker_properties_lists_every_entry_in_order() {
        let properties = gang_x86();
        let entries: Vec<(&str, &str)> = properties.worker_properties().collect();
        assert_eq!(
            entries,
            [
                ("fleet", "gang-x86"),
                ("ISA", "x86-64"),
                ("sandbox", "bwrap"),
                ("gang_action_environment", "host:test"),
                ("cpu_count", "15"),
                ("cpu_millicores", "15000"),
                ("memory_kb", "30408704"),
                ("disk_kb", "104857600"),
                ("capacity", "spot"),
            ]
        );
    }

    #[test]
    fn get_returns_value_of_present_keys_only() {
        let properties = gang_x86();
        assert_eq!(properties.get("ISA"), Some("x86-64"));
        assert_eq!(properties.get("capacity"), Some("spot"));
        assert_eq!(properties.get("machine_class"), None);
    }

    #[test]
    fn declarations_list_every_entry_with_its_match_kind() {
        let properties = gang_x86();
        assert_eq!(
            properties.declarations().collect::<Vec<_>>(),
            [
                ("fleet", MatchKind::Exact),
                ("ISA", MatchKind::Exact),
                ("sandbox", MatchKind::Exact),
                ("gang_action_environment", MatchKind::Ignored),
                ("cpu_count", MatchKind::Minimum),
                ("cpu_millicores", MatchKind::Minimum),
                ("memory_kb", MatchKind::Minimum),
                ("disk_kb", MatchKind::Minimum),
                ("capacity", MatchKind::Exact),
            ]
        );
    }

    #[test]
    fn entries_list_every_key_value_and_match_kind() {
        let properties = gang_x86();
        assert_eq!(
            properties.entries().collect::<Vec<_>>(),
            [
                ("fleet", "gang-x86", Rule::Exact),
                ("ISA", "x86-64", Rule::Exact),
                ("sandbox", "bwrap", Rule::Exact),
                ("gang_action_environment", "host:test", Rule::Exact),
                ("cpu_count", "15", Rule::Minimum(15)),
                ("cpu_millicores", "15000", Rule::Minimum(15000)),
                ("memory_kb", "30408704", Rule::Minimum(30_408_704)),
                ("disk_kb", "104857600", Rule::Minimum(104_857_600)),
                ("capacity", "spot", Rule::Exact),
            ]
        );
    }

    #[test]
    fn worker_identity_replaces_launch_identity() {
        let mut properties = gang_x86();
        properties.set_action_environment(&"host:actual".parse().unwrap());
        assert_eq!(
            properties.get(property::ACTION_ENVIRONMENT),
            Some("host:actual")
        );
        assert_eq!(
            properties
                .worker_properties()
                .filter(|(key, _)| *key == property::ACTION_ENVIRONMENT)
                .count(),
            1
        );
    }

    #[test]
    fn actual_memory_capacity_preserves_action_ceiling_and_other_properties() {
        let mut fleet = gang_x86();
        fleet.actions.resources.memory = Some(NonZeroBytes::gib(4));
        fleet.set_memory_capacity(NonZeroBytes::gib(12));
        assert_eq!(fleet.capacity(property::MEMORY_KB), Some(12 * 1024 * 1024));
        assert_eq!(fleet.limit(property::MEMORY_KB), Some(4 * 1024 * 1024));
        assert_eq!(fleet.get(property::MEMORY_KB), Some("12582912"));
        assert_eq!(fleet.options().collect::<Vec<_>>(), [("capacity", "spot")]);
        assert_eq!(fleet.get(property::CPU_COUNT), Some("15"));
        let catalog = FleetCatalog::try_new(vec![fleet.clone()]).unwrap();
        assert!(catalog.admit([(property::MEMORY_KB, "4194304")]).is_ok());
        assert!(catalog.admit([(property::MEMORY_KB, "4194305")]).is_err());

        fleet.set_memory_capacity(NonZeroBytes::gib(2));
        assert_eq!(fleet.capacity(property::MEMORY_KB), Some(2 * 1024 * 1024));
        assert_eq!(fleet.limit(property::MEMORY_KB), Some(2 * 1024 * 1024));

        fleet.actions.resources.memory = None;
        fleet.set_memory_capacity(NonZeroBytes::gib(8));
        assert_eq!(fleet.capacity(property::MEMORY_KB), Some(8 * 1024 * 1024));
        assert_eq!(fleet.limit(property::MEMORY_KB), Some(8 * 1024 * 1024));
    }

    #[test]
    fn actual_capacity_preserves_independent_action_ceilings() {
        let mut fleet = gang_x86();
        fleet.actions.resources.cpu_count = std::num::NonZeroU32::new(4);
        fleet.actions.resources.memory = Some(NonZeroBytes::gib(4));
        fleet.actions.resources.disk = Some(NonZeroBytes::gib(6));
        let mut actual = Resources {
            vcpus: 20,
            memory: Bytes::gib(12),
            disk: Bytes::gib(120),
        };
        fleet.set_capacity(actual);
        for (key, capacity, limit) in [
            (property::CPU_COUNT, 20, 4),
            (property::CPU_MILLICORES, 20_000, 4_000),
            (property::MEMORY_KB, 12 * 1024 * 1024, 4 * 1024 * 1024),
            (property::DISK_KB, 120 * 1024 * 1024, 6 * 1024 * 1024),
        ] {
            assert_eq!(fleet.capacity(key), Some(capacity));
            assert_eq!(fleet.limit(key), Some(limit));
            assert_eq!(fleet.get(key), Some(capacity.to_string().as_str()));
        }
        assert_eq!(fleet.options().collect::<Vec<_>>(), [("capacity", "spot")]);
        let catalog = FleetCatalog::try_new(vec![fleet.clone()]).unwrap();
        assert!(catalog.admit([(property::MEMORY_KB, "4194304")]).is_ok());
        assert!(catalog.admit([(property::MEMORY_KB, "4194305")]).is_err());

        actual.vcpus = 2;
        actual.memory = Bytes::gib(2);
        actual.disk = Bytes::gib(3);
        fleet.set_capacity(actual);
        assert_eq!(fleet.limit(property::CPU_COUNT), Some(2));
        assert_eq!(fleet.limit(property::MEMORY_KB), Some(2 * 1024 * 1024));
        assert_eq!(fleet.limit(property::DISK_KB), Some(3 * 1024 * 1024));

        fleet.actions.resources = crate::ResourceCeilings::default();
        fleet.set_capacity(actual);
        assert_eq!(fleet.limit(property::CPU_COUNT), Some(2));
        assert_eq!(fleet.limit(property::MEMORY_KB), Some(2 * 1024 * 1024));
        assert_eq!(fleet.limit(property::DISK_KB), Some(3 * 1024 * 1024));
    }

    #[test]
    fn empty_minimum_request_accepts_no_fleet() {
        let catalog = FleetCatalog::try_new(vec![gang_x86()]).unwrap();
        assert!(catalog.admit([("cpu_count", "")]).is_err());
    }

    #[test]
    fn overflowing_minimum_request_accepts_no_fleet() {
        let catalog = FleetCatalog::try_new(vec![gang_x86()]).unwrap();
        assert!(
            catalog
                .admit([("cpu_count", "18446744073709551616")])
                .is_err()
        );
    }

    #[test]
    fn action_ceilings_do_not_reduce_worker_capacity_or_options() {
        let instance = Resources {
            vcpus: 8,
            memory: Bytes::new(16 << 30),
            disk: Bytes::new(100 << 30),
        };
        let actions = FleetActionPolicy {
            grants: [Capability::Network].into_iter().collect(),
            resources: crate::ResourceCeilings {
                cpu_count: std::num::NonZeroU32::new(4),
                memory: Some(crate::NonZeroBytes::gib(4)),
                disk: None,
            },
        };
        let name = FleetName::try_new("capped").unwrap();
        let fleet = FleetProperties::new(
            &name,
            Isa::X86_64,
            &"bwrap",
            &"host:test".parse().unwrap(),
            instance,
            &actions,
            [("capacity", "spot".into())],
        );
        assert_eq!(fleet.capacity(property::CPU_MILLICORES), Some(8000));
        assert_eq!(fleet.limit(property::CPU_MILLICORES), Some(4000));
        assert_eq!(fleet.capacity(property::MEMORY_KB), Some(16 * 1024 * 1024));
        assert_eq!(fleet.limit(property::MEMORY_KB), Some(4 * 1024 * 1024));
        assert_eq!(fleet.get(property::CPU_MILLICORES), Some("8000"));
        assert_eq!(fleet.get(property::NETWORK), Some("requested"));
        assert_eq!(fleet.options().collect::<Vec<_>>(), [("capacity", "spot")]);
        assert_eq!(
            fleet.action_ceilings().collect::<Vec<_>>(),
            [
                (property::CPU_MILLICORES, 4000),
                (property::MEMORY_KB, 4 * 1024 * 1024)
            ]
        );
        // Two legal actions can consume the physical total despite the per-action cap.
        assert_eq!(
            2 * fleet.limit(property::CPU_MILLICORES).unwrap(),
            fleet.capacity(property::CPU_MILLICORES).unwrap()
        );
    }
}
