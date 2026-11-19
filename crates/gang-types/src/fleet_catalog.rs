//! The fleets a controller matches requests against, in preference order.

use crate::fleet_properties::Rule;
use crate::{
    Capability, FleetCountError, FleetProperties, FleetSet, MatchKind, NetworkRequest, Refusal,
    property,
};

/// Every fleet's properties, in preference order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetCatalog {
    fleets: Vec<FleetProperties>,
    declared: Vec<(&'static str, MatchKind)>,
    columns: Vec<Column>,
    all: FleetSet,
}

/// The fleet a request is fitted to and the minimum keys lowered to that fleet's limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fit {
    /// Index of the fitted fleet in preference order.
    pub fleet: usize,
    /// Minimum properties lowered to the fleet's limits, in declaration order.
    pub lowered: Vec<(&'static str, u64)>,
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
    /// Fleets granted the capability.
    Capability(FleetSet),
}

impl ColumnRule {
    /// Returns the fleets accepting one requested value.
    fn mask(&self, value: &str) -> FleetSet {
        match self {
            Self::Exact(values) => values
                .binary_search_by(|(known, _)| (**known).cmp(value))
                .ok()
                .and_then(|index| values.get(index))
                .map_or(FleetSet::EMPTY, |(_, mask)| *mask),
            Self::Minimum(limits) => value
                .parse::<u64>()
                .ok()
                .and_then(|requested| {
                    limits.get(limits.partition_point(|(limit, _)| *limit < requested))
                })
                .map_or(FleetSet::EMPTY, |(_, mask)| *mask),
            Self::Capability(mask) => *mask,
        }
    }
}

/// Returns a fleet's limit when it lowers the requested minimum.
fn lowered_limit(limits: &[(u64, FleetSet)], fleet: usize, requested: Option<u64>) -> Option<u64> {
    let limit = limits
        .iter()
        .rev()
        .find(|(_, mask)| mask.contains(fleet))?
        .0;
    requested.is_none_or(|value| value > limit).then_some(limit)
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
        if let Some((_, _, Rule::Minimum(limit))) =
            fleet.entries().find(|(entry_key, _, _)| *entry_key == key)
        {
            add_to_mask(&mut limits, limit, index);
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
        for capability in Capability::ALL {
            let key = capability.property();
            if !declared.iter().any(|(known, _)| *known == key) {
                declared.push((key, MatchKind::Capability));
            }
        }
        for key in property::HARMLESS_PROPERTIES {
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
                    MatchKind::Capability => {
                        let mut mask = FleetSet::EMPTY;
                        for (index, fleet) in fleets.iter().enumerate() {
                            if fleet.get(key).is_some() {
                                mask.insert(index);
                            }
                        }
                        ColumnRule::Capability(mask)
                    }
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
    /// order: the fleets' own keys, then capabilities and harmless properties.
    #[must_use]
    pub fn declared(&self) -> &[(&'static str, MatchKind)] {
        &self.declared
    }

    /// Returns fleets matching the declared properties supplied by `request`.
    ///
    /// Call [`Self::admit`] on the complete request to reject undeclared properties.
    pub fn accepting<'r>(&self, request: impl Fn(&str) -> Option<&'r str>) -> FleetSet {
        let mut accepting = self.all;
        for column in &self.columns {
            let Some(value) = request(column.key) else {
                continue;
            };
            if let ColumnRule::Capability(mask) = &column.rule {
                if value.is_empty() {
                    continue;
                }
                match value.parse::<NetworkRequest>() {
                    Ok(NetworkRequest::NotRequested) => continue,
                    Ok(NetworkRequest::Requested) => accepting &= *mask,
                    Err(_) => return FleetSet::EMPTY,
                }
            } else {
                accepting &= column.rule.mask(value);
            }
            if accepting.is_empty() {
                break;
            }
        }
        accepting
    }

    /// Returns matching fleets without constraining the action environment.
    ///
    /// Used when matching against live worker identities rather than launch identities.
    pub fn accepting_base<'r>(&self, request: impl Fn(&str) -> Option<&'r str>) -> FleetSet {
        self.accepting(|key| {
            if key == property::ACTION_ENVIRONMENT {
                None
            } else {
                request(key)
            }
        })
    }

    /// Validates every request property and returns the eligible fleets.
    ///
    /// # Errors
    /// Returns a deterministic refusal for an unknown, malformed or unsatisfied property.
    pub fn admit<'a>(
        &self,
        request: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<FleetSet, Refusal> {
        let mut pairs: Vec<_> = request.into_iter().collect();
        pairs.sort_unstable_by(|a, b| a.0.cmp(b.0).then(a.1.cmp(b.1)));
        let mut accepting = self.all;
        let mut capability = None;
        let mut mismatch = None;
        for (key, value) in pairs {
            let Some(column) = self.columns.iter().find(|column| column.key == key) else {
                if property::HARMLESS_PROPERTIES.contains(&key) {
                    continue;
                }
                return Err(Refusal::Unknown {
                    key: key.to_owned(),
                });
            };
            if let ColumnRule::Capability(mask) = &column.rule {
                if value.is_empty() {
                    continue;
                }
                match value.parse::<NetworkRequest>() {
                    Ok(NetworkRequest::NotRequested) => continue,
                    Ok(NetworkRequest::Requested) => capability = Some((column.key, value, *mask)),
                    Err(error) => {
                        return Err(Refusal::InvalidCapability {
                            key: column.key,
                            requested: value.to_owned(),
                            error,
                        });
                    }
                }
                continue;
            }
            if let ColumnRule::Minimum(_) = &column.rule
                && value.parse::<u64>().is_err()
            {
                return Err(Refusal::InvalidMinimum {
                    key: column.key,
                    requested: value.to_owned(),
                });
            }
            let before = accepting;
            accepting &= column.rule.mask(value);
            if accepting.is_empty() && mismatch.is_none() {
                mismatch = Some(self.incompatible(column.key, value, before));
            }
        }
        if let Some(refusal) = mismatch {
            return Err(refusal);
        }
        if let Some((key, value, mask)) = capability {
            let before = accepting;
            accepting &= mask;
            if accepting.is_empty() {
                return Err(self.incompatible(key, value, before));
            }
        }
        if accepting.is_empty() {
            Err(Refusal::Combination)
        } else {
            Ok(accepting)
        }
    }

    fn incompatible(&self, key: &'static str, requested: &str, eligible: FleetSet) -> Refusal {
        Refusal::Incompatible {
            key,
            requested: requested.to_owned(),
            fleets: eligible
                .iter()
                .map(|index| self.fleets[index].name().to_owned())
                .collect(),
        }
    }

    /// Fits minimum requests to an admitted fleet, preserving all exact matches.
    ///
    /// An accepting fleet wins in preference order. Otherwise the fleet lowering the fewest
    /// minimum keys wins, then the one with the smallest sum of relative shortfalls, then
    /// preference order. Each term is rounded down to units of 2^-64, distinguishing different
    /// limits on one key; equal rounded sums go to preference order. A non-numeric lowered value
    /// contributes one.
    pub fn fit<'r>(
        &self,
        admitted: FleetSet,
        request: impl Fn(&str) -> Option<&'r str>,
    ) -> Option<Fit> {
        let mut considered = admitted & self.all;
        for column in &self.columns {
            if matches!(&column.rule, ColumnRule::Exact(_))
                && let Some(value) = request(column.key)
            {
                considered &= column.rule.mask(value);
            }
        }
        let mut scores = [(0_u32, 0_u128); crate::MAX_FLEETS];
        for column in &self.columns {
            let ColumnRule::Minimum(limits) = &column.rule else {
                continue;
            };
            let Some(value) = request(column.key) else {
                continue;
            };
            let requested = value.parse::<u64>().ok();
            for fleet in considered {
                if let Some(limit) = lowered_limit(limits, fleet, requested) {
                    let term = requested.map_or(1_u128 << 64, |value| {
                        (u128::from(value - limit) << 64) / u128::from(value)
                    });
                    let score = &mut scores[fleet];
                    score.0 = score.0.saturating_add(1);
                    score.1 = score.1.saturating_add(term);
                }
            }
        }
        // `min_by_key` keeps the first equal minimum, preserving fleet preference.
        let fleet = considered.iter().min_by_key(|&fleet| scores[fleet])?;
        let lowered = self
            .columns
            .iter()
            .filter_map(|column| {
                let ColumnRule::Minimum(limits) = &column.rule else {
                    return None;
                };
                let requested = request(column.key)?.parse::<u64>().ok();
                lowered_limit(limits, fleet, requested).map(|limit| (column.key, limit))
            })
            .collect();
        Some(Fit { fleet, lowered })
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
            &"host:test".parse().unwrap(),
            Resources {
                vcpus,
                memory: Bytes::new(gib << 30),
                disk: Bytes::new(disk_gib << 30),
            },
            &crate::FleetActionPolicy::default(),
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
    fn admitted(catalog: &FleetCatalog, pairs: &[(&str, &str)]) -> FleetSet {
        catalog
            .admit(pairs.iter().copied())
            .unwrap_or(FleetSet::EMPTY)
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
    fn environment_matches_exactly_except_in_base_admission() {
        let catalog = catalog();
        let wrong = request(&[
            (property::ACTION_ENVIRONMENT, "host:other"),
            (property::ISA, "x86-64"),
        ]);
        assert_eq!(catalog.accepting(&wrong), set(&[]));
        assert_eq!(catalog.accepting_base(&wrong), set(&[0, 1]));
        assert_eq!(
            catalog.accepting(request(&[(property::ACTION_ENVIRONMENT, "host:test")])),
            set(&[0, 1, 2])
        );
        assert!(catalog.fit(set(&[0, 1, 2]), &wrong).is_none());
    }

    #[test]
    fn empty_request_accepts_every_fleet() {
        assert_eq!(catalog().admit([]), Ok(set(&[0, 1, 2])));
    }

    #[test]
    fn persistent_worker_marker_does_not_restrict_fleets() {
        let catalog = catalog();
        for value in ["1", "", "any value"] {
            assert_eq!(
                catalog.admit([(property::SUPPORTS_WORKERS, value)]),
                Ok(set(&[0, 1, 2]))
            );
            assert_eq!(
                catalog.admit([(property::SUPPORTS_WORKERS, value), ("ISA", "aarch64")]),
                Ok(set(&[2]))
            );
        }
    }

    #[test]
    fn action_policy_rejects_ungranted_network() {
        assert_eq!(
            catalog().admit([("network", "requested")]).unwrap_err(),
            Refusal::Incompatible {
                key: "network",
                requested: "requested".into(),
                fleets: vec!["spot-x86".into(), "on-demand-x86".into(), "arm".into()]
            }
        );
    }

    #[test]
    fn action_policy_rejects_unknown_property() {
        assert_eq!(
            catalog()
                .admit([("ISA", "x86-64"), ("unrecognized-capability", "requested"),])
                .unwrap_err(),
            Refusal::Unknown {
                key: "unrecognized-capability".into()
            }
        );
    }

    #[test]
    fn fleet_name_pins_one_fleet() {
        assert_eq!(
            admitted(&catalog(), &[("fleet", "on-demand-x86")]),
            set(&[1])
        );
    }

    #[test]
    fn an_unknown_fleet_name_matches_nothing() {
        assert_eq!(admitted(&catalog(), &[("fleet", "nope")]), set(&[]));
    }

    #[test]
    fn isa_selects_fleets_of_that_isa() {
        let catalog = catalog();
        assert_eq!(admitted(&catalog, &[("ISA", "x86-64")]), set(&[0, 1]));
        assert_eq!(admitted(&catalog, &[("ISA", "aarch64")]), set(&[2]));
    }

    #[test]
    fn contradictory_isa_and_capacity_accept_nothing() {
        let accepting = admitted(&catalog(), &[("ISA", "aarch64"), ("capacity", "spot")]);
        assert_eq!(accepting, set(&[]));
    }

    #[test]
    fn exact_option_selects_matching_fleets() {
        assert_eq!(
            admitted(&catalog(), &[("capacity", "on-demand")]),
            set(&[1, 2])
        );
    }

    #[test]
    fn sandbox_no_fleet_declares_accepts_nothing() {
        assert_eq!(admitted(&catalog(), &[("sandbox", "noop")]), set(&[]));
    }

    #[test]
    fn cpu_count_is_a_minimum() {
        let catalog = catalog();
        assert_eq!(admitted(&catalog, &[("cpu_count", "8")]), set(&[0, 1]));
        assert_eq!(admitted(&catalog, &[("cpu_count", "9")]), set(&[]));
    }

    #[test]
    fn memory_kb_is_a_minimum_in_kib() {
        let catalog = catalog();
        // 16 GiB is 16777216 KiB; the arm fleet has 8 GiB.
        assert_eq!(
            admitted(&catalog, &[("memory_kb", "16777216")]),
            set(&[0, 1])
        );
        assert_eq!(admitted(&catalog, &[("memory_kb", "16777217")]), set(&[]));
        assert_eq!(
            admitted(&catalog, &[("memory_kb", "8388608")]),
            set(&[0, 1, 2])
        );
    }

    #[test]
    fn unparseable_minimum_accepts_nothing() {
        assert_eq!(admitted(&catalog(), &[("cpu_count", "eight")]), set(&[]));
    }

    #[test]
    fn undeclared_keys_are_rejected() {
        assert!(matches!(
            catalog().admit([("colour", "red")]),
            Err(Refusal::Unknown { .. })
        ));
    }

    #[test]
    fn action_environment_constrains_admission() {
        let catalog = catalog();
        assert_eq!(
            catalog.admit([
                (property::ISA, "aarch64"),
                (property::ACTION_ENVIRONMENT, "host:test"),
                (property::SUPPORTS_WORKERS, "any value"),
            ]),
            Ok(set(&[2]))
        );
        for value in ["host:other", ""] {
            assert!(catalog.admit([(property::ACTION_ENVIRONMENT, value)]).is_err());
        }
    }

    #[test]
    fn disk_is_a_minimum() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("small-x86", Isa::X86_64, 8, 16, 100, &[]),
            fleet("large-x86", Isa::X86_64, 8, 16, 200, &[]),
        ])
        .unwrap();
        // 200 GiB is 209715200 KiB; the small fleet has 100 GiB.
        assert_eq!(admitted(&catalog, &[("disk_kb", "209715200")]), set(&[1]));
        assert_eq!(admitted(&catalog, &[("disk_kb", "209715201")]), set(&[]));
        assert_eq!(admitted(&catalog, &[("disk_kb", "1")]), set(&[0, 1]));
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
        assert_eq!(admitted(&catalog, &[("machine_class", "gpu")]), set(&[0]));
        assert_eq!(admitted(&catalog, &[("machine_class", "cpu")]), set(&[]));
    }

    #[test]
    fn declared_lists_each_key_once_with_its_match_kind() {
        assert_eq!(
            catalog().declared(),
            &[
                ("fleet", MatchKind::Exact),
                ("ISA", MatchKind::Exact),
                ("sandbox", MatchKind::Exact),
                ("gang_action_environment", MatchKind::Exact),
                ("cpu_count", MatchKind::Minimum),
                ("cpu_millicores", MatchKind::Minimum),
                ("memory_kb", MatchKind::Minimum),
                ("disk_kb", MatchKind::Minimum),
                ("capacity", MatchKind::Exact),
                ("network", MatchKind::Capability),
                ("supports-workers", MatchKind::Ignored),
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
                "gang_action_environment",
                "cpu_count",
                "cpu_millicores",
                "memory_kb",
                "disk_kb",
                "capacity",
                "zone",
                "network",
                "supports-workers",
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
        assert_eq!(admitted(&catalog(), &[("memory_kb", "16 GiB")]), set(&[]));
        assert_eq!(admitted(&catalog(), &[("cpu_count", "")]), set(&[]));
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
        assert_eq!(admitted(&catalog, &[("machine_class", "gpu")]), set(&[0]));
        assert_eq!(admitted(&catalog, &[("machine_class", "")]), set(&[]));
    }

    #[test]
    fn baseline_network_values_do_not_restrict_fleets() {
        for value in ["", "not-requested"] {
            assert_eq!(catalog().admit([("network", value)]), Ok(set(&[0, 1, 2])));
        }
        assert!(matches!(
            catalog().admit([("network", "eth0")]),
            Err(Refusal::InvalidCapability { .. })
        ));
    }

    #[test]
    fn grant_and_isa_intersect_and_fit_preserves_admission() {
        let policy = crate::FleetActionPolicy {
            grants: [Capability::Network].into_iter().collect(),
            resources: crate::ResourceCeilings {
                cpu_count: std::num::NonZeroU32::new(2),
                ..Default::default()
            },
        };
        let network_fleet = FleetProperties::new(
            &FleetName::try_new("network-arm").unwrap(),
            Isa::Aarch64,
            &"bwrap",
            &"host:test".parse().unwrap(),
            Resources {
                vcpus: 8,
                memory: Bytes::new(16 << 30),
                disk: Bytes::new(100 << 30),
            },
            &policy,
            [],
        );
        let catalog = FleetCatalog::try_new(vec![
            fleet("offline-x86", Isa::X86_64, 8, 16, 100, &[]),
            network_fleet,
        ])
        .unwrap();
        let admitted = catalog.admit([("network", "requested")]).unwrap();
        assert_eq!(admitted, set(&[1]));
        let with_environment = [
            (property::NETWORK, "requested"),
            (property::ACTION_ENVIRONMENT, "host:test"),
        ];
        assert_eq!(catalog.admit(with_environment), Ok(set(&[1])));
        assert_eq!(catalog.accepting(request(&with_environment)), set(&[1]));
        assert_eq!(
            catalog.accepting(request(&[
                (property::NETWORK, "requested"),
                (property::ACTION_ENVIRONMENT, "host:other"),
            ])),
            set(&[])
        );
        assert_eq!(
            catalog.accepting_base(request(&[
                (property::NETWORK, "requested"),
                (property::ACTION_ENVIRONMENT, "host:other"),
            ])),
            set(&[1])
        );
        assert_eq!(
            catalog.admit([("ISA", "aarch64"), ("network", "requested")]),
            Ok(set(&[1]))
        );
        let refusal = catalog
            .admit([("network", "requested"), ("ISA", "x86-64")])
            .unwrap_err();
        assert_eq!(
            refusal,
            Refusal::Incompatible {
                key: "network",
                requested: "requested".into(),
                fleets: vec!["offline-x86".into()],
            }
        );
        assert!(
            catalog
                .admit([("network", "requested"), ("cpu_count", "3")])
                .is_err()
        );
        assert_eq!(
            catalog.fit(admitted, request(&[("cpu_count", "8")])),
            Some(Fit {
                fleet: 1,
                lowered: vec![("cpu_count", 2)],
            })
        );
        for request in [
            vec![
                ("ISA", "aarch64"),
                ("cpu_count", "1"),
                ("memory_kb", "1024"),
                ("disk_kb", "1024"),
            ],
            vec![
                ("ISA", "x86-64"),
                ("cpu_millicores", "1000"),
                ("network", "not-requested"),
            ],
        ] {
            assert!(catalog.admit(request).is_ok());
        }
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
        assert_eq!(admitted(&catalog, &[("capacity", "spot")]), set(&[0]));
        assert_eq!(admitted(&catalog, &[("capacity", "on-demand")]), set(&[]));
        assert_eq!(admitted(&catalog, &[("cpu_count", "5")]), set(&[]));
    }

    #[test]
    fn refusal_explains_minimum_values() {
        let catalog = catalog();
        assert_eq!(catalog.admit([("cpu_count", "8")]), Ok(set(&[0, 1])));
        assert_eq!(
            catalog.admit([("cpu_count", "9")]).unwrap_err(),
            Refusal::Incompatible {
                key: "cpu_count",
                requested: "9".into(),
                fleets: vec!["spot-x86".into(), "on-demand-x86".into(), "arm".into()],
            }
        );
    }

    #[test]
    fn refusal_names_a_minimum_that_is_not_a_number() {
        let refusal = catalog().admit([("memory_kb", "4GiB")]).unwrap_err();
        assert_eq!(
            refusal,
            Refusal::InvalidMinimum {
                key: "memory_kb",
                requested: "4GiB".into(),
            }
        );
        assert_eq!(
            refusal.to_string(),
            "memory_kb 4GiB is not a non-negative integer"
        );
    }

    #[test]
    fn refusal_is_stable_across_input_order() {
        let first = [("capacity", "reserved"), ("cpu_count", "20")];
        let second = [("cpu_count", "20"), ("capacity", "reserved")];
        assert_eq!(catalog().admit(first), catalog().admit(second));
    }

    #[test]
    fn refusal_explains_exact_values_and_combinations() {
        let catalog = catalog();
        let refusal = catalog.admit([("capacity", "reserved")]).unwrap_err();
        assert!(matches!(
            refusal,
            Refusal::Incompatible {
                key: "capacity",
                ..
            }
        ));
        let combination = catalog
            .admit([("ISA", "aarch64"), ("capacity", "spot")])
            .unwrap_err();
        assert_eq!(
            combination,
            Refusal::Incompatible {
                key: "capacity",
                requested: "spot".into(),
                fleets: vec!["arm".into()],
            }
        );
        assert!(combination.to_string().contains("arm"));
    }

    #[test]
    fn refusal_explains_empty_exact_column() {
        let mut catalog = catalog();
        catalog.columns.push(Column {
            key: "empty",
            rule: ColumnRule::Exact(Box::new([])),
        });
        assert!(matches!(
            catalog.admit([("empty", "value")]),
            Err(Refusal::Incompatible { key: "empty", .. })
        ));
    }

    #[test]
    fn fit_selects_first_accepting_admitted_fleet() {
        assert_eq!(
            catalog().fit(set(&[1, 2]), request(&[("cpu_count", "4")])),
            Some(Fit {
                fleet: 1,
                lowered: vec![]
            })
        );
        assert_eq!(
            catalog().fit(set(&[0, 1]), request(&[("capacity", "reserved")])),
            None
        );
    }

    #[test]
    fn fit_lowers_to_admitted_limit_and_breaks_ties_by_preference() {
        let catalog = catalog();
        assert_eq!(
            catalog.fit(set(&[0, 1, 2]), request(&[("cpu_count", "20")])),
            Some(Fit {
                fleet: 0,
                lowered: vec![("cpu_count", 8)]
            })
        );
        assert_eq!(
            catalog.fit(set(&[2]), request(&[("cpu_count", "20")])),
            Some(Fit {
                fleet: 2,
                lowered: vec![("cpu_count", 4)]
            })
        );
        assert_eq!(
            catalog.fit(set(&[2]), request(&[("cpu_count", "many")])),
            Some(Fit {
                fleet: 2,
                lowered: vec![("cpu_count", 4)]
            })
        );
    }

    #[test]
    fn fit_prefers_fewest_lowered_keys() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("small", Isa::X86_64, 4, 8, 100, &[]),
            fleet("large", Isa::X86_64, 8, 16, 100, &[]),
        ])
        .unwrap();
        assert_eq!(
            catalog.fit(
                set(&[0, 1]),
                request(&[("cpu_count", "20"), ("memory_kb", "10000000")])
            ),
            Some(Fit {
                fleet: 1,
                lowered: vec![("cpu_count", 8)]
            })
        );
    }

    #[test]
    fn fit_clamps_an_oversized_memory_reservation_to_the_largest_fleet() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("small", Isa::X86_64, 4, 8, 100, &[]),
            fleet("large", Isa::X86_64, 16, 32, 100, &[]),
        ])
        .unwrap();
        assert_eq!(
            catalog.fit(set(&[0, 1]), request(&[("memory_kb", "35232154")])),
            Some(Fit {
                fleet: 1,
                lowered: vec![("memory_kb", 33_554_432)],
            })
        );
    }

    #[test]
    fn fit_clamps_oversized_cpu_millicores_to_the_largest_fleet() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("small", Isa::X86_64, 4, 8, 100, &[]),
            fleet("large", Isa::X86_64, 16, 32, 100, &[]),
        ])
        .unwrap();
        assert_eq!(
            catalog.fit(set(&[0, 1]), request(&[("cpu_millicores", "19200")])),
            Some(Fit {
                fleet: 1,
                lowered: vec![("cpu_millicores", 16000)],
            })
        );
    }

    #[test]
    fn fit_prefers_the_larger_fleet_for_an_outlier_request() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("small", Isa::X86_64, 4, 8, 100, &[]),
            fleet("large", Isa::X86_64, 8, 8, 100, &[]),
        ])
        .unwrap();
        assert_eq!(
            catalog.fit(set(&[0, 1]), request(&[("cpu_millicores", "8710000")])),
            Some(Fit {
                fleet: 1,
                lowered: vec![("cpu_millicores", 8000)],
            }),
            "the larger fleet has the smaller relative shortfall"
        );
    }

    #[test]
    fn fit_prefers_the_smaller_shortfall_sum_across_keys() {
        let catalog = FleetCatalog::try_new(vec![
            fleet("cpu", Isa::X86_64, 8, 8, 100, &[]),
            fleet("memory", Isa::X86_64, 4, 16, 100, &[]),
        ])
        .unwrap();
        // Both per-mille sums are 999 + 999. The memory fleet gains
        // 8_388_608 / 100_000_000_000, exceeding the CPU fleet's advantage
        // of 4 / 8_710_000, so its shortfall sum is smaller at Q64.64 precision.
        assert_eq!(
            catalog.fit(
                set(&[0, 1]),
                request(&[("cpu_count", "8710000"), ("memory_kb", "100000000000")]),
            ),
            Some(Fit {
                fleet: 1,
                lowered: vec![("cpu_count", 4), ("memory_kb", 16_777_216)],
            }),
            "the sum of relative shortfalls favors the memory fleet"
        );
    }

    #[test]
    fn fit_treats_shortfall_sums_within_2_pow_minus_64_as_a_tie() {
        let mut catalog = FleetCatalog::try_new(vec![
            fleet("first", Isa::X86_64, 4, 8, 100, &[]),
            fleet("second", Isa::X86_64, 4, 8, 100, &[]),
        ])
        .unwrap();
        catalog.columns.extend([
            Column {
                key: "first_minimum",
                rule: ColumnRule::Minimum(vec![(1, set(&[0, 1])), (2, set(&[0]))].into()),
            },
            Column {
                key: "second_minimum",
                rule: ColumnRule::Minimum(vec![(1, set(&[0, 1])), (2, set(&[1]))].into()),
            },
        ]);
        assert_eq!(
            catalog.fit(
                set(&[0, 1]),
                request(&[
                    ("first_minimum", "18446744073709551615"),
                    ("second_minimum", "18446744073709551614"),
                ]),
            ),
            Some(Fit {
                fleet: 0,
                lowered: vec![("first_minimum", 2), ("second_minimum", 1)],
            }),
            "equal Q64.64 sums select the first fleet even when its exact sum is larger"
        );
    }

    #[test]
    fn fit_lists_all_lowered_keys_in_declaration_order() {
        assert_eq!(
            catalog().fit(
                set(&[2]),
                request(&[("memory_kb", "99999999"), ("cpu_count", "20")]),
            ),
            Some(Fit {
                fleet: 2,
                lowered: vec![("cpu_count", 4), ("memory_kb", 8_388_608)],
            })
        );
    }

    #[test]
    fn minimum_between_limits_accepts_only_larger_fleets() {
        assert_eq!(admitted(&catalog(), &[("cpu_count", "5")]), set(&[0, 1]));
    }
}
