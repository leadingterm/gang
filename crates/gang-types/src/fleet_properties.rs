//! The properties one fleet registers with the scheduler, and the rule its requests match by.

use std::fmt::Display;

use crate::{FleetName, Isa, Resources, property};

/// How a request value matches a fleet's value for one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchKind {
    /// The request value must equal the fleet's value.
    Exact,
    /// The request value must be a number no greater than the fleet's.
    Minimum,
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
}

/// One property of a fleet: its key, its value, and how requests match it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    key: &'static str,
    value: String,
    rule: Rule,
}

/// One fleet's properties: its name, ISA, sandbox, instance size and provisioner options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetProperties {
    entries: Vec<Entry>,
}

impl FleetProperties {
    const SANDBOX_POSITION: usize = 2;
    const FIXED_ENTRY_COUNT: usize = 6;

    /// Creates a fleet's properties.
    ///
    /// The entries are the fleet, ISA, sandbox, `cpu_count`, `memory_kb` and `disk_kb` (all
    /// minimums, with memory and disk in KiB), followed by the provisioner options as exact
    /// entries.
    pub fn new(
        name: &FleetName,
        isa: Isa,
        sandbox: &impl Display,
        instance: Resources,
        options: impl IntoIterator<Item = (&'static str, String)>,
    ) -> Self {
        let fixed: [Entry; Self::FIXED_ENTRY_COUNT] = [
            Entry {
                key: property::FLEET,
                value: name.to_string(),
                rule: Rule::Exact,
            },
            Entry {
                key: property::ISA,
                value: isa.to_string(),
                rule: Rule::Exact,
            },
            Entry {
                key: property::SANDBOX,
                value: sandbox.to_string(),
                rule: Rule::Exact,
            },
            minimum(property::CPU_COUNT, u64::from(instance.vcpus)),
            minimum(property::MEMORY_KB, instance.memory.get() / 1024),
            minimum(property::DISK_KB, instance.disk.get() / 1024),
        ];
        let mut entries = Vec::from(fixed);
        entries.extend(options.into_iter().map(|(key, value)| Entry {
            key,
            value,
            rule: Rule::Exact,
        }));
        Self { entries }
    }

    /// The sandbox the fleet's workers run actions in.
    #[must_use]
    pub fn sandbox(&self) -> &str {
        self.entries[Self::SANDBOX_POSITION].value.as_str()
    }

    /// The provisioner options, in registration order: every entry after the fleet, ISA,
    /// sandbox and instance size.
    pub fn options(&self) -> impl Iterator<Item = (&'static str, &str)> + '_ {
        self.entries
            .iter()
            .skip(Self::FIXED_ENTRY_COUNT)
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
            let kind = match entry.rule {
                Rule::Exact => MatchKind::Exact,
                Rule::Minimum(_) => MatchKind::Minimum,
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
fn minimum(key: &'static str, limit: u64) -> Entry {
    Entry {
        key,
        value: limit.to_string(),
        rule: Rule::Minimum(limit),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bytes, FleetCatalog, FleetSet};

    fn gang_x86() -> FleetProperties {
        let name = FleetName::try_new("gang-x86").unwrap();
        FleetProperties::new(
            &name,
            Isa::X86_64,
            &"bwrap",
            Resources {
                vcpus: 15,
                memory: Bytes::new(29 << 30),
                disk: Bytes::new(100 << 30),
            },
            [("capacity", "spot".to_owned())],
        )
    }

    #[test]
    fn sandbox_returns_the_sandbox_given_to_new() {
        assert_eq!(gang_x86().sandbox(), "bwrap");
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
            instance,
            [("capacity", "spot".to_owned()), ("zone", "west".to_owned())],
        );
        assert_eq!(
            properties.options().collect::<Vec<_>>(),
            [("capacity", "spot"), ("zone", "west")]
        );

        let without_options = FleetProperties::new(&name, Isa::X86_64, &"bwrap", instance, []);
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
                ("cpu_count", "15"),
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
                ("cpu_count", MatchKind::Minimum),
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
                ("cpu_count", "15", Rule::Minimum(15)),
                ("memory_kb", "30408704", Rule::Minimum(30408704)),
                ("disk_kb", "104857600", Rule::Minimum(104857600)),
                ("capacity", "spot", Rule::Exact),
            ]
        );
    }

    #[test]
    fn empty_minimum_request_accepts_no_fleet() {
        let catalog = FleetCatalog::try_new(vec![gang_x86()]).unwrap();
        let accepting = catalog.accepting(|key| (key == "cpu_count").then_some(""));
        assert_eq!(accepting, FleetSet::EMPTY);
    }

    #[test]
    fn overflowing_minimum_request_accepts_no_fleet() {
        let catalog = FleetCatalog::try_new(vec![gang_x86()]).unwrap();
        let accepting =
            catalog.accepting(|key| (key == "cpu_count").then_some("18446744073709551616"));
        assert_eq!(accepting, FleetSet::EMPTY);
    }
}
