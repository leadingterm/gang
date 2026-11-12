//! The fleets a controller matches requests against, in preference order.

use crate::fleet_properties::Rule;
use crate::{FleetCountError, FleetProperties, FleetSet, MatchKind, property};

/// Every fleet's properties, in preference order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetCatalog {
    fleets: Vec<FleetProperties>,
    declared: Vec<(&'static str, MatchKind)>,
    columns: Vec<Column>,
    all: FleetSet,
}

/// One declared key with its values compiled into fleet masks.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Column {
    key: &'static str,
    rule: ColumnRule,
}

/// How requests for one key are matched, compiled into fleet masks.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ColumnRule {
    /// Each distinct fleet value with the fleets that have it, sorted by value.
    Exact(Box<[(Box<str>, FleetSet)]>),
    /// Each distinct fleet limit, ascending, with the fleets whose limit is at least it.
    Minimum(Box<[(u64, FleetSet)]>),
}

/// Adds a fleet index to the mask for a value.
fn add_to_mask<T: PartialEq>(masks: &mut Vec<(T, FleetSet)>, value: T, index: usize) {
    if let Some((_, mask)) = masks.iter_mut().find(|(known, _)| *known == value) {
        mask.insert(index);
    } else {
        let mut mask = FleetSet::EMPTY;
        mask.insert(index);
        masks.push((value, mask));
    }
}

/// Compiles the first entry for a key in each fleet into exact-value masks.
fn exact_rule(key: &str, fleets: &[FleetProperties]) -> ColumnRule {
    let mut values = Vec::new();
    for (index, fleet) in fleets.iter().enumerate() {
        if let Some((_, value, _)) = fleet.entries().find(|(entry_key, _, _)| *entry_key == key) {
            add_to_mask(&mut values, value, index);
        }
    }
    values.sort_unstable_by_key(|(value, _)| *value);
    ColumnRule::Exact(
        values
            .into_iter()
            .map(|(value, mask)| (value.into(), mask))
            .collect(),
    )
}

/// Compiles the first entry for a key in each fleet into cumulative minimum masks.
fn minimum_rule(key: &str, fleets: &[FleetProperties]) -> ColumnRule {
    let mut limits = Vec::new();
    for (index, fleet) in fleets.iter().enumerate() {
        if let Some((_, value, rule)) = fleet.entries().find(|(entry_key, _, _)| *entry_key == key)
        {
            let limit = match rule {
                Rule::Exact => value.parse::<u64>().ok(),
                Rule::Minimum(limit) => Some(limit),
            };
            if let Some(limit) = limit {
                add_to_mask(&mut limits, limit, index);
            }
        }
    }
    limits.sort_unstable_by_key(|(limit, _)| *limit);
    let mut suffix = FleetSet::EMPTY;
    for (_, mask) in limits.iter_mut().rev() {
        suffix |= *mask;
        *mask = suffix;
    }
    ColumnRule::Minimum(limits.into_boxed_slice())
}

impl FleetCatalog {
    /// Creates a catalog from every fleet's properties, in preference order.
    ///
    /// # Errors
    ///
    /// Returns [`FleetCountError`] when there are more than [`MAX_FLEETS`](crate::MAX_FLEETS)
    /// fleets.
    pub fn try_new(fleets: Vec<FleetProperties>) -> Result<Self, FleetCountError> {
        let all = FleetSet::prefix(fleets.len())?;
        let mut declared: Vec<(&'static str, MatchKind)> = Vec::new();
        for fleet in &fleets {
            for (key, kind) in fleet.declarations() {
                if !declared.iter().any(|(known, _)| *known == key) {
                    declared.push((key, kind));
                }
            }
        }
        for key in property::IGNORED {
            if !declared.iter().any(|(known, _)| *known == key) {
                declared.push((key, MatchKind::Ignored));
            }
        }
        let columns = declared
            .iter()
            .filter_map(|&(key, kind)| {
                let rule = match kind {
                    MatchKind::Exact => exact_rule(key, &fleets),
                    MatchKind::Minimum => minimum_rule(key, &fleets),
                    MatchKind::Ignored => return None,
                };
                Some(Column { key, rule })
            })
            .collect();
        Ok(Self {
            fleets,
            declared,
            columns,
            all,
        })
    }

    /// Returns every fleet's properties, in preference order.
    #[must_use]
    pub fn fleets(&self) -> &[FleetProperties] {
        &self.fleets
    }

    /// Returns the number of fleets.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fleets.len()
    }

    /// Returns whether the catalog has no fleets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fleets.is_empty()
    }

    /// Returns every key the scheduler declares with how it matches, each once, in first-seen
    /// order: the fleets' own keys, then the keys that impose nothing.
    #[must_use]
    pub fn declared(&self) -> &[(&'static str, MatchKind)] {
        &self.declared
    }

    /// Returns the fleets that accept a request, given the request's value for each key.
    ///
    /// `request(key)` returns the request's value for `key`, or `None` when the request does not
    /// set it. A fleet accepts the request when, for every key the request sets and that some
    /// fleet matches exactly or by minimum, the fleet accepts that value. Other keys impose
    /// nothing.
    pub fn accepting<'r>(&self, request: impl Fn(&str) -> Option<&'r str>) -> FleetSet {
        let mut accepting = self.all;
        for column in &self.columns {
            if accepting.is_empty() {
                break;
            }
            let Some(value) = request(column.key) else {
                continue;
            };
            let mask = match &column.rule {
                ColumnRule::Exact(values) => values
                    .binary_search_by(|(known, _)| (**known).cmp(value))
                    .ok()
                    .and_then(|index| values.get(index))
                    .map_or(FleetSet::EMPTY, |(_, mask)| *mask),
                ColumnRule::Minimum(limits) => value
                    .parse::<u64>()
                    .ok()
                    .and_then(|requested| {
                        limits.get(limits.partition_point(|(limit, _)| *limit < requested))
                    })
                    .map_or(FleetSet::EMPTY, |(_, mask)| *mask),
            };
            accepting &= mask;
        }
        accepting
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bytes, FleetName, Isa, MAX_FLEETS, Resources};

    /// Builds a fleet with the `bwrap` sandbox, `gib` GiB of memory and `disk_gib` GiB of
    /// disk.
    fn fleet(
        name: &str,
        isa: Isa,
        vcpus: u32,
        gib: u64,
        disk_gib: u64,
        options: &[(&'static str, &str)],
    ) -> FleetProperties {
        FleetProperties::new(
            &FleetName::try_new(name).unwrap(),
            isa,
            &"bwrap",
            Resources {
                vcpus,
                memory: Bytes::new(gib << 30),
                disk: Bytes::new(disk_gib << 30),
            },
            options
                .iter()
                .map(|(key, value)| (*key, (*value).to_owned())),
        )
    }

    /// Returns a request closure over `pairs`.
    fn request<'m>(
        pairs: &'m [(&'static str, &'static str)],
    ) -> impl Fn(&str) -> Option<&'m str> + 'm {
        move |key| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| *value)
        }
    }

    fn set(indices: &[usize]) -> FleetSet {
        FleetSet::try_from_indices(indices.iter().copied()).expect("indices in range")
    }

    fn catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![
            fleet("spot-x86", Isa::X86_64, 8, 16, 200, &[("capacity", "spot")]),
            fleet(
                "on-demand-x86",
                Isa::X86_64,
                8,
                16,
                200,
                &[("capacity", "on-demand")],
            ),
            fleet("arm", Isa::Aarch64, 4, 8, 100, &[("capacity", "on-demand")]),
        ])
        .unwrap()
    }

    #[test]
    fn empty_request_accepts_every_fleet() {
        assert_eq!(catalog().accepting(request(&[])), set(&[0, 1, 2]));
    }

    #[test]
    fn fleet_name_pins_one_fleet() {
        assert_eq!(
            catalog().accepting(request(&[("fleet", "on-demand-x86")])),
            set(&[1])
        );
    }

    #[test]
    fn an_unknown_fleet_name_matches_nothing() {
        assert_eq!(catalog().accepting(request(&[("fleet", "nope")])), set(&[]));
    }

    #[test]
    fn isa_selects_fleets_of_that_isa() {
        let catalog = catalog();
        assert_eq!(
            catalog.accepting(request(&[("ISA", "x86-64")])),
            set(&[0, 1])
        );
        assert_eq!(catalog.accepting(request(&[("ISA", "aarch64")])), set(&[2]));
    }

    #[test]
    fn contradictory_isa_and_capacity_accept_nothing() {
        let accepting = catalog().accepting(request(&[("ISA", "aarch64"), ("capacity", "spot")]));
        assert_eq!(accepting, set(&[]));
    }

    #[test]
    fn exact_option_selects_matching_fleets() {
        assert_eq!(
            catalog().accepting(request(&[("capacity", "on-demand")])),
            set(&[1, 2])
        );
    }

    #[test]
    fn sandbox_no_fleet_declares_accepts_nothing() {
        assert_eq!(
            catalog().accepting(request(&[("sandbox", "noop")])),
            set(&[])
        );
    }

    #[test]
    fn cpu_count_is_a_minimum() {
        let catalog = catalog();
        assert_eq!(
            catalog.accepting(request(&[("cpu_count", "8")])),
            set(&[0, 1])
        );
        assert_eq!(catalog.accepting(request(&[("cpu_count", "9")])), set(&[]));
    }

    #[test]
    fn memory_kb_is_a_minimum_in_kib() {
        let catalog = catalog();
        // 16 GiB is 16777216 KiB; the arm fleet has 8 GiB.
        assert_eq!(
            catalog.accepting(request(&[("memory_kb", "16777216")])),
            set(&[0, 1])
        );
        assert_eq!(
            catalog.accepting(request(&[("memory_kb", "16777217")])),
            set(&[])
        );
        assert_eq!(
            catalog.accepting(request(&[("memory_kb", "8388608")])),
            set(&[0, 1, 2])
        );
    }

    #[test]
    fn unparseable_minimum_accepts_nothing() {
        assert_eq!(
            catalog().accepting(request(&[("cpu_count", "eight")])),
            set(&[])
        );
    }

    #[test]
    fn undeclared_keys_impose_nothing() {
        assert_eq!(
            catalog().accepting(request(&[("colour", "red")])),
            set(&[0, 1, 2])
        );
    }

    #[test]
    fn disk_is_a_minimum() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("small-x86", Isa::X86_64, 8, 16, 100, &[]),
            fleet("large-x86", Isa::X86_64, 8, 16, 200, &[]),
        ])
        .unwrap();
        // 200 GiB is 209715200 KiB; the small fleet has 100 GiB.
        assert_eq!(
            catalog.accepting(request(&[("disk_kb", "209715200")])),
            set(&[1])
        );
        assert_eq!(
            catalog.accepting(request(&[("disk_kb", "209715201")])),
            set(&[])
        );
        assert_eq!(
            catalog.accepting(request(&[("disk_kb", "1")])),
            set(&[0, 1])
        );
    }

    #[test]
    fn key_declared_by_one_fleet_rejects_fleets_lacking_it() {
        let catalog = FleetCatalog::try_new(vec![
            fleet(
                "gpu-x86",
                Isa::X86_64,
                8,
                16,
                200,
                &[("machine_class", "gpu")],
            ),
            fleet("cpu-x86", Isa::X86_64, 8, 16, 200, &[]),
            fleet("cpu-arm", Isa::Aarch64, 4, 8, 100, &[]),
        ])
        .unwrap();
        assert_eq!(
            catalog.accepting(request(&[("machine_class", "gpu")])),
            set(&[0])
        );
        assert_eq!(
            catalog.accepting(request(&[("machine_class", "cpu")])),
            set(&[])
        );
    }

    #[test]
    fn declared_lists_each_key_once_with_its_match_kind() {
        assert_eq!(
            catalog().declared(),
            &[
                ("fleet", MatchKind::Exact),
                ("ISA", MatchKind::Exact),
                ("sandbox", MatchKind::Exact),
                ("cpu_count", MatchKind::Minimum),
                ("memory_kb", MatchKind::Minimum),
                ("disk_kb", MatchKind::Minimum),
                ("capacity", MatchKind::Exact),
                ("network", MatchKind::Ignored),
            ]
        );
    }

    #[test]
    fn declared_appends_new_option_keys_after_earlier_ones() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("spot-x86", Isa::X86_64, 8, 16, 200, &[("capacity", "spot")]),
            fleet(
                "zoned-x86",
                Isa::X86_64,
                8,
                16,
                200,
                &[("capacity", "spot"), ("zone", "a")],
            ),
        ])
        .unwrap();
        let keys: Vec<&str> = catalog.declared().iter().map(|(key, _)| *key).collect();
        assert_eq!(
            keys,
            [
                "fleet",
                "ISA",
                "sandbox",
                "cpu_count",
                "memory_kb",
                "disk_kb",
                "capacity",
                "zone",
                "network",
            ]
        );
    }

    #[test]
    fn accepts_up_to_max_fleets() {
        let fleets = |count: usize| -> Vec<FleetProperties> {
            (0..count)
                .map(|index| fleet(&format!("fleet-{index}"), Isa::X86_64, 1, 1, 1, &[]))
                .collect()
        };
        assert_eq!(
            FleetCatalog::try_new(fleets(MAX_FLEETS)).map(|catalog| catalog.len()),
            Ok(MAX_FLEETS)
        );
        assert_eq!(
            FleetCatalog::try_new(fleets(MAX_FLEETS + 1)).err(),
            Some(FleetCountError {
                count: MAX_FLEETS + 1
            })
        );
    }

    #[test]
    fn non_numeric_minimum_request_accepts_no_fleet() {
        assert_eq!(
            catalog().accepting(request(&[("memory_kb", "16 GiB")])),
            set(&[])
        );
        assert_eq!(catalog().accepting(request(&[("cpu_count", "")])), set(&[]));
    }

    #[test]
    fn fleet_lacking_an_exact_key_never_accepts_it() {
        let catalog = FleetCatalog::try_new(vec![
            fleet(
                "gpu-x86",
                Isa::X86_64,
                8,
                16,
                200,
                &[("machine_class", "gpu")],
            ),
            fleet("cpu-x86", Isa::X86_64, 8, 16, 200, &[]),
        ])
        .unwrap();
        assert_eq!(
            catalog.accepting(request(&[("machine_class", "gpu")])),
            set(&[0])
        );
        assert_eq!(
            catalog.accepting(request(&[("machine_class", "")])),
            set(&[])
        );
    }

    #[test]
    fn request_is_called_once_per_declared_non_ignored_key() {
        let catalog = catalog();
        let declared = catalog.declared();
        let calls: Vec<std::cell::Cell<u8>> =
            declared.iter().map(|_| std::cell::Cell::new(0)).collect();
        let pairs: &[(&'static str, &'static str)] = &[("capacity", "spot"), ("network", "eth0")];
        let lookup = request(pairs);
        let accepting = catalog.accepting(|key| {
            if let Some(count) = declared
                .iter()
                .position(|(known, _)| *known == key)
                .and_then(|index| calls.get(index))
            {
                count.set(count.get() + 1);
            }
            lookup(key)
        });
        assert_eq!(accepting, set(&[0]));
        let expected: Vec<u8> = declared
            .iter()
            .map(|(_, kind)| u8::from(*kind != MatchKind::Ignored))
            .collect();
        assert_eq!(
            calls.iter().map(std::cell::Cell::get).collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn ignored_key_imposes_nothing() {
        assert_eq!(
            catalog().accepting(request(&[("network", "eth0")])),
            set(&[0, 1, 2])
        );
    }

    #[test]
    fn repeated_keys_use_the_first_fleet_entry() {
        let catalog = FleetCatalog::try_new(vec![fleet(
            "duplicate",
            Isa::X86_64,
            4,
            8,
            100,
            &[
                ("capacity", "spot"),
                ("capacity", "on-demand"),
                ("cpu_count", "100"),
            ],
        )])
        .unwrap();
        assert_eq!(
            catalog.accepting(request(&[("capacity", "spot")])),
            set(&[0])
        );
        assert_eq!(
            catalog.accepting(request(&[("capacity", "on-demand")])),
            set(&[])
        );
        assert_eq!(catalog.accepting(request(&[("cpu_count", "5")])), set(&[]));
    }

    #[test]
    fn minimum_between_limits_accepts_only_larger_fleets() {
        assert_eq!(
            catalog().accepting(request(&[("cpu_count", "5")])),
            set(&[0, 1])
        );
    }
}
