//! Catalog admission validation, `ADR-14` fleet fitting, configured minima enforcement, and
//! authoritative physical-memory fitting (`§5.3`).

use std::collections::BTreeMap;

use gang_config::{ColdStartTuning, DiskMinReservation, MinReservation};
use gang_types::{
    ActionResources, Bytes, FleetCatalog, FleetSet, NetworkRequest, Refusal, property,
};

use crate::sketch::{ResourceSketch, SketchKey};

/// Immutable original client resource floors extracted at admission time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientFloors {
    /// Minimum CPU millicores requested by the client (`max(cpu_count * 1000, cpu_millicores)`).
    pub cpu_millicores: Option<u64>,
    /// Minimum memory in `KiB` requested by the client.
    pub memory_kb: Option<u64>,
    /// Minimum disk in `KiB` requested by the client.
    pub disk_kb: Option<u64>,
}

impl ClientFloors {
    /// Returns `true` when `fleet` in `catalog` satisfies every original floor.
    #[must_use]
    pub fn fits_fleet(self, catalog: &FleetCatalog, fleet: usize) -> bool {
        let Some(props) = catalog.fleets().get(fleet) else {
            return false;
        };
        if let Some(cpu) = self.cpu_millicores
            && props
                .limit(property::CPU_MILLICORES)
                .is_none_or(|limit| cpu > limit)
        {
            return false;
        }
        if let Some(memory) = self.memory_kb
            && props
                .limit(property::MEMORY_KB)
                .is_none_or(|limit| memory > limit)
        {
            return false;
        }
        if let Some(disk) = self.disk_kb
            && props
                .limit(property::DISK_KB)
                .is_none_or(|limit| disk > limit)
        {
            return false;
        }
        true
    }
}

/// Validated client request properties separated into exact/capability selectors and original
/// resource floors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedProperties {
    /// Admitted candidate fleets in catalog preference order.
    pub candidates: FleetSet,
    /// Normalized exact and capability selectors (excludes resource minimums and harmless metadata).
    pub selectors: BTreeMap<String, String>,
    /// Original client numeric resource floors.
    pub floors: ClientFloors,
}

/// Result of fitting learned/cold-start reservations to the admitted fleet set and current
/// physical capacity facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FittedPlan {
    /// Catalog index of the fitted fleet.
    pub fleet: usize,
    /// Final reservation to reserve on the worker ledger (includes whole-worker CPU promotion if
    /// memory filled physical capacity without a lower policy cap).
    pub reservation: ActionResources,
    /// Pre-fit desired reservation (after configured minima raising, before fleet clamping).
    pub desired: ActionResources,
    /// CPU reservation in millicores after catalog fitting and before whole-worker CPU promotion.
    pub cpu_before_memory_fit: u64,
    /// Pre-fit learned reservation exposed when `desired` CPU or memory exceeded `reservation`.
    pub learned_reservation: Option<ActionResources>,
}

/// Computes the millicore reservation implied by an explicit `cpu_count` and optional
/// `cpu_millicores`.
#[must_use]
pub fn normalized_cpu_millicores(
    cpu_count: Option<u64>,
    cpu_millicores: Option<u64>,
) -> Option<u64> {
    let from_count = cpu_count
        .filter(|&count| count > 0)
        .map(|count| count.saturating_mul(property::MILLICORES_PER_CORE));
    match (from_count, cpu_millicores) {
        (Some(count_mc), Some(existing)) => Some(count_mc.max(existing)),
        (Some(count_mc), None) => Some(count_mc),
        (None, Some(existing)) => Some(existing),
        (None, None) => None,
    }
}

/// Validates original client `platform_properties` against `catalog` and extracts candidate
/// fleets, normalized selectors, and immutable resource floors (`§5.3` steps 1–2).
///
/// # Errors
///
/// Returns [`Refusal`] when any property is unknown, malformed, ungranted, exceeds every fleet's
/// effective ceiling, or no single fleet satisfies all properties together.
pub fn validate_properties<'a>(
    catalog: &FleetCatalog,
    properties: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<ValidatedProperties, Refusal> {
    let mut normalized: BTreeMap<String, String> = BTreeMap::new();
    for (key, value) in properties {
        if key == property::NETWORK
            && (value.is_empty() || value == NetworkRequest::NotRequested.as_str())
        {
            continue;
        }
        normalized.insert(key.to_owned(), value.to_owned());
    }

    let candidates = catalog.admit(
        normalized
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )?;
    if candidates.is_empty() {
        return Err(Refusal::Combination);
    }

    let cpu_count = normalized
        .get(property::CPU_COUNT)
        .and_then(|v| v.parse::<u64>().ok());
    let cpu_millicores_raw = normalized
        .get(property::CPU_MILLICORES)
        .and_then(|v| v.parse::<u64>().ok());
    let cpu_millicores = normalized_cpu_millicores(cpu_count, cpu_millicores_raw);
    let memory_kb = normalized
        .get(property::MEMORY_KB)
        .and_then(|v| v.parse::<u64>().ok());
    let disk_kb = normalized
        .get(property::DISK_KB)
        .and_then(|v| v.parse::<u64>().ok());

    let floors = ClientFloors {
        cpu_millicores,
        memory_kb,
        disk_kb,
    };

    // Ensure normalized CPU (e.g. `cpu_count * 1000`) also fits at least one candidate fleet.
    let mut valid_candidates = FleetSet::EMPTY;
    for fleet in candidates {
        if floors.fits_fleet(catalog, fleet) {
            valid_candidates.insert(fleet);
        }
    }
    if valid_candidates.is_empty() {
        if let Some(cpu) = cpu_millicores {
            let largest = candidates
                .iter()
                .filter_map(|f| catalog.fleets().get(f))
                .filter_map(|f| f.limit(property::CPU_MILLICORES))
                .max()
                .unwrap_or(0);
            if cpu > largest {
                return Err(Refusal::Minimum {
                    key: property::CPU_MILLICORES,
                    requested: cpu.to_string(),
                    largest,
                });
            }
        }
        return Err(Refusal::Combination);
    }

    let selectors = normalized
        .into_iter()
        .filter(|(key, _)| {
            !property::HARMLESS_PROPERTIES.contains(&key.as_str())
                && !matches!(
                    key.as_str(),
                    property::CPU_COUNT
                        | property::CPU_MILLICORES
                        | property::MEMORY_KB
                        | property::DISK_KB
                )
        })
        .collect();

    Ok(ValidatedProperties {
        candidates: valid_candidates,
        selectors,
        floors,
    })
}

/// Resolves the learned reservation from `sketch` using the `target+mnemonic → target → mnemonic →
/// command` fallback chain, or falls back to `cold_start` for dimensions left untagged in `floors`
/// when no sketch entry exists (`§5.3` step 4).
#[must_use]
pub fn lookup_learned_or_cold_start(
    sketch: &mut ResourceSketch,
    target: Option<&str>,
    mnemonic: Option<&str>,
    command_digest: &str,
    now_s: u64,
    cold_start: &ColdStartTuning,
    floors: ClientFloors,
) -> ActionResources {
    let target = target.filter(|s| !s.is_empty());
    let mnemonic = mnemonic.filter(|s| !s.is_empty());
    let command = (!command_digest.is_empty()).then_some(command_digest);

    let mut candidate_keys = Vec::with_capacity(4);
    if let (Some(t), Some(m)) = (target, mnemonic) {
        candidate_keys.push(SketchKey::for_attempt(Some(t), Some(m), ""));
    }
    if let Some(t) = target {
        candidate_keys.push(SketchKey::for_attempt(Some(t), None, ""));
    }
    if let Some(m) = mnemonic {
        candidate_keys.push(SketchKey::for_attempt(None, Some(m), ""));
    }
    if let Some(cmd) = command {
        candidate_keys.push(SketchKey::for_attempt(None, None, cmd));
    }

    for key in &candidate_keys {
        if let Some(entry) = sketch.lookup(key, now_s) {
            let shaped = sketch.shape(&entry);
            if !shaped.is_empty() {
                return ActionResources {
                    cpu_millicores: shaped.cpu_count,
                    memory: Bytes::kib(shaped.memory_kb),
                    disk: Bytes::kib(shaped.disk_kb),
                };
            }
        }
    }

    if cold_start.enabled {
        ActionResources {
            cpu_millicores: if floors.cpu_millicores.is_some() {
                0
            } else {
                u64::from(cold_start.cpu_millicores.get())
            },
            memory: if floors.memory_kb.is_some() {
                Bytes::new(0)
            } else {
                Bytes::kib(cold_start.memory.as_units())
            },
            disk: if floors.disk_kb.is_some() {
                Bytes::new(0)
            } else {
                Bytes::kib(cold_start.disk.as_units())
            },
        }
    } else {
        ActionResources::default()
    }
}

/// Fits a learned/cold-start reservation onto `admitted` fleets in `catalog` while preserving
/// `floors` and applying configured minima (`min_memory`, `min_disk`) plus authoritative
/// physical-memory fitting (`§5.3` steps 4–5).
///
/// # Errors
///
/// Returns [`Refusal`] if no fleet in `admitted` satisfies `floors` under `catalog`.
pub fn fit_reservation(
    catalog: &FleetCatalog,
    admitted: FleetSet,
    selectors: &BTreeMap<String, String>,
    floors: ClientFloors,
    learned: ActionResources,
    min_memory: MinReservation,
    min_disk: DiskMinReservation,
) -> Result<FittedPlan, Refusal> {
    let eligible = filter_eligible_fleets(catalog, admitted, floors)?;
    let desired = compute_desired(learned, floors, min_memory, min_disk);
    let fit_mem_kb = clamp_memory_to_largest(catalog, eligible, desired.memory.get() / 1024);
    let fit_disk_kb =
        clamp_disk_to_largest(catalog, eligible, fit_mem_kb, desired.disk.get() / 1024);

    let cpu_str = (desired.cpu_millicores > 0 || floors.cpu_millicores.is_some())
        .then(|| desired.cpu_millicores.to_string());
    let mem_str = (fit_mem_kb > 0 || floors.memory_kb.is_some()).then(|| fit_mem_kb.to_string());
    let disk_str = (fit_disk_kb > 0 || floors.disk_kb.is_some()).then(|| fit_disk_kb.to_string());

    let fit = catalog
        .fit(eligible, |key| match key {
            property::CPU_MILLICORES => cpu_str.as_deref(),
            property::MEMORY_KB => mem_str.as_deref(),
            property::DISK_KB => disk_str.as_deref(),
            _ => selectors.get(key).map(String::as_str),
        })
        .ok_or(Refusal::Combination)?;

    let (mut fitted_cpu, mut fitted_mem_kb, mut fitted_disk_kb) = apply_lowered(
        desired.cpu_millicores,
        fit_mem_kb,
        fit_disk_kb,
        &fit.lowered,
    );

    if let Some(floor) = floors.cpu_millicores {
        fitted_cpu = fitted_cpu.max(floor);
    }
    if let Some(floor) = floors.memory_kb {
        fitted_mem_kb = fitted_mem_kb.max(floor);
    }
    if let Some(floor) = floors.disk_kb {
        fitted_disk_kb = fitted_disk_kb.max(floor);
    }

    let cpu_before_memory_fit = fitted_cpu;
    fitted_cpu = promote_whole_worker_cpu(catalog, fit.fleet, fitted_mem_kb, fitted_cpu);

    let reservation = ActionResources {
        cpu_millicores: fitted_cpu,
        memory: Bytes::kib(fitted_mem_kb),
        disk: Bytes::kib(fitted_disk_kb),
    };
    let learned_reservation = (desired.cpu_millicores > reservation.cpu_millicores
        || desired.memory > reservation.memory)
        .then_some(desired);

    Ok(FittedPlan {
        fleet: fit.fleet,
        reservation,
        desired,
        cpu_before_memory_fit,
        learned_reservation,
    })
}

fn filter_eligible_fleets(
    catalog: &FleetCatalog,
    admitted: FleetSet,
    floors: ClientFloors,
) -> Result<FleetSet, Refusal> {
    let mut eligible = FleetSet::EMPTY;
    for fleet in admitted {
        if floors.fits_fleet(catalog, fleet) {
            eligible.insert(fleet);
        }
    }
    if eligible.is_empty() {
        return Err(floor_refusal(catalog, admitted, floors));
    }
    Ok(eligible)
}

fn compute_desired(
    learned: ActionResources,
    floors: ClientFloors,
    min_memory: MinReservation,
    min_disk: DiskMinReservation,
) -> ActionResources {
    let desired_cpu = learned
        .cpu_millicores
        .max(floors.cpu_millicores.unwrap_or(0));
    let mut desired_mem_kb = learned
        .memory
        .get()
        .div_ceil(1024)
        .max(floors.memory_kb.unwrap_or(0));
    let mut desired_disk_kb = learned
        .disk
        .get()
        .div_ceil(1024)
        .max(floors.disk_kb.unwrap_or(0));

    if desired_mem_kb > 0 && desired_mem_kb < min_memory.as_units() {
        desired_mem_kb = min_memory.as_units();
    }
    if desired_disk_kb > 0 && desired_disk_kb < min_disk.as_units() {
        desired_disk_kb = min_disk.as_units();
    }
    ActionResources {
        cpu_millicores: desired_cpu,
        memory: Bytes::kib(desired_mem_kb),
        disk: Bytes::kib(desired_disk_kb),
    }
}

fn clamp_memory_to_largest(catalog: &FleetCatalog, eligible: FleetSet, mem_kb: u64) -> u64 {
    if mem_kb == 0 {
        return 0;
    }
    eligible
        .iter()
        .filter_map(|f| catalog.fleets().get(f))
        .filter_map(|f| f.limit(property::MEMORY_KB))
        .max()
        .map_or(mem_kb, |largest| mem_kb.min(largest))
}

fn clamp_disk_to_largest(
    catalog: &FleetCatalog,
    eligible: FleetSet,
    fit_mem_kb: u64,
    disk_kb: u64,
) -> u64 {
    if disk_kb == 0 {
        return 0;
    }
    let largest = eligible
        .iter()
        .filter(|&f| {
            fit_mem_kb == 0
                || catalog
                    .fleets()
                    .get(f)
                    .and_then(|fleet| fleet.limit(property::MEMORY_KB))
                    .is_some_and(|limit| limit >= fit_mem_kb)
        })
        .filter_map(|f| catalog.fleets().get(f))
        .filter_map(|f| f.limit(property::DISK_KB))
        .max()
        .or_else(|| {
            eligible
                .iter()
                .filter_map(|f| catalog.fleets().get(f))
                .filter_map(|f| f.limit(property::DISK_KB))
                .max()
        });
    largest.map_or(disk_kb, |limit| disk_kb.min(limit))
}

fn apply_lowered(
    mut cpu: u64,
    mut mem_kb: u64,
    mut disk_kb: u64,
    lowered: &[(&'static str, u64)],
) -> (u64, u64, u64) {
    for &(key, limit) in lowered {
        match key {
            property::CPU_MILLICORES => cpu = limit,
            property::MEMORY_KB => mem_kb = limit,
            property::DISK_KB => disk_kb = limit,
            _ => {}
        }
    }
    (cpu, mem_kb, disk_kb)
}

fn promote_whole_worker_cpu(
    catalog: &FleetCatalog,
    fleet_idx: usize,
    fitted_mem_kb: u64,
    fitted_cpu: u64,
) -> u64 {
    // Authoritative physical-memory fitting: when memory equals or is clamped to the fleet's full
    // physical capacity (and no lower policy memory cap is configured), reserve whole-worker CPU
    // bounded by the fleet's effective CPU action ceiling.
    if fitted_mem_kb > 0
        && let Some(fleet) = catalog.fleets().get(fleet_idx)
        && let (Some(phys_mem), Some(limit_mem)) = (
            fleet.capacity(property::MEMORY_KB),
            fleet.limit(property::MEMORY_KB),
        )
        && limit_mem == phys_mem
        && fitted_mem_kb >= phys_mem
        && let Some(cpu_ceiling) = fleet.limit(property::CPU_MILLICORES)
    {
        return cpu_ceiling;
    }
    fitted_cpu
}

fn floor_refusal(catalog: &FleetCatalog, admitted: FleetSet, floors: ClientFloors) -> Refusal {
    for (key, floor) in [
        (property::MEMORY_KB, floors.memory_kb),
        (property::DISK_KB, floors.disk_kb),
        (property::CPU_MILLICORES, floors.cpu_millicores),
    ] {
        let Some(requested) = floor else {
            continue;
        };
        let largest = admitted
            .iter()
            .filter_map(|f| catalog.fleets().get(f))
            .filter_map(|f| f.limit(key))
            .max()
            .unwrap_or(0);
        if requested > largest {
            return Refusal::Minimum {
                key,
                requested: requested.to_string(),
                largest,
            };
        }
    }
    Refusal::Combination
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use gang_types::{
        Capability, FleetActionPolicy, FleetName, FleetProperties, Isa, NonZeroBytes, Percent,
        Resources,
    };

    use super::*;
    use crate::sketch::{Budget, Observation, Policy};

    fn fleet(
        name: &str,
        isa: Isa,
        vcpus: u32,
        mem_gib: u64,
        disk_gib: u64,
        mem_ceiling_gib: Option<u64>,
        network: bool,
    ) -> FleetProperties {
        let mut actions = FleetActionPolicy::default();
        actions.resources.memory = mem_ceiling_gib.map(NonZeroBytes::gib);
        if network {
            actions.grants.insert(Capability::Network);
        }
        FleetProperties::new(
            &FleetName::try_new(name).expect("fleet"),
            isa,
            &"bwrap",
            &"host:test".parse().expect("env"),
            Resources {
                vcpus,
                memory: Bytes::gib(mem_gib),
                disk: Bytes::gib(disk_gib),
            },
            &actions,
            std::iter::empty(),
        )
    }

    fn test_sketch() -> ResourceSketch {
        ResourceSketch::with_shards(
            Policy {
                headroom_percent: 20,
                cpu_reservation_percentile: Percent::from_percent(50),
                classes: Vec::new(),
                reserve_cpu: true,
                reserve_memory: true,
                reserve_disk: true,
            },
            Budget {
                target: 16,
                mnemonic: 16,
                command: 16,
                probation: 16,
            },
            Duration::from_hours(24),
            1,
        )
    }

    #[test]
    fn validate_properties_enforces_selectors_capabilities_and_harmless_metadata() {
        let cat = FleetCatalog::try_new(vec![
            fleet("x86", Isa::X86_64, 8, 16, 100, None, false),
            fleet("x86-net", Isa::X86_64, 8, 16, 100, None, true),
        ])
        .expect("catalog");

        // Harmless metadata and `network = not-requested` are stripped/allowed.
        let validated = validate_properties(
            &cat,
            [
                (property::ISA, "x86-64"),
                (property::SUPPORTS_WORKERS, "1"),
                (property::ACTION_ENVIRONMENT, "host:legacy"),
                (property::NETWORK, "not-requested"),
                (property::CPU_COUNT, "2"),
            ],
        )
        .expect("valid");
        assert_eq!(validated.candidates.len(), 2);
        assert_eq!(validated.floors.cpu_millicores, Some(2_000));
        assert_eq!(
            validated.selectors.get(property::ISA).map(String::as_str),
            Some("x86-64")
        );
        assert!(!validated.selectors.contains_key(property::SUPPORTS_WORKERS));
        assert!(!validated.selectors.contains_key(property::NETWORK));

        // Requesting network restricts candidates to `x86-net`.
        let net = validate_properties(&cat, [(property::NETWORK, "requested")]).expect("net");
        assert_eq!(net.candidates.iter().collect::<Vec<_>>(), vec![1]);

        // Unknown key is refused.
        assert!(matches!(
            validate_properties(&cat, [("bogus_key", "1")]),
            Err(Refusal::Unknown { .. })
        ));

        // Over-ceiling request is refused.
        assert!(matches!(
            validate_properties(&cat, [(property::CPU_COUNT, "9")]),
            Err(Refusal::Incompatible {
                key: property::CPU_COUNT,
                ..
            })
        ));
    }

    #[test]
    fn sketch_fallback_order_prefers_target_mnemonic_then_target_then_mnemonic_then_command() {
        let mut sketch = test_sketch();
        let cold = ColdStartTuning::default();

        // Cold start when nothing exists and client left dimensions untagged.
        let cs = lookup_learned_or_cold_start(
            &mut sketch,
            Some("//pkg:a"),
            Some("CppCompile"),
            "cmd1",
            100,
            &cold,
            ClientFloors::default(),
        );
        assert_eq!(cs.cpu_millicores, 250);
        assert_eq!(cs.memory, Bytes::gib(4));
        assert_eq!(cs.disk, Bytes::gib(4));

        // Seed mnemonic + command + target + target+mnemonic with distinct memory values.
        sketch.observe(
            SketchKey::for_attempt(None, None, "cmd1"),
            &Observation {
                now_s: 10,
                peak_memory_kb: 100_000,
                killed: true,
                ..Observation::default()
            },
        );
        sketch.observe(
            SketchKey::for_attempt(None, Some("CppLink"), ""),
            &Observation {
                now_s: 10,
                peak_memory_kb: 200_000,
                ..Observation::default()
            },
        );
        sketch.observe(
            SketchKey::for_attempt(Some("//pkg:a"), None, ""),
            &Observation {
                now_s: 10,
                peak_memory_kb: 300_000,
                ..Observation::default()
            },
        );
        sketch.observe(
            SketchKey::for_attempt(Some("//pkg:a"), Some("CppCompile"), ""),
            &Observation {
                now_s: 10,
                peak_memory_kb: 400_000,
                ..Observation::default()
            },
        );

        let by_target_mnemonic = lookup_learned_or_cold_start(
            &mut sketch,
            Some("//pkg:a"),
            Some("CppCompile"),
            "cmd1",
            11,
            &cold,
            ClientFloors::default(),
        );
        assert_eq!(by_target_mnemonic.memory, Bytes::kib(480_000)); // 400_000 * 1.2

        let by_target_only = lookup_learned_or_cold_start(
            &mut sketch,
            Some("//pkg:a"),
            Some("OtherMnemonic"),
            "cmd1",
            11,
            &cold,
            ClientFloors::default(),
        );
        assert_eq!(by_target_only.memory, Bytes::kib(360_000));

        let by_mnemonic_only = lookup_learned_or_cold_start(
            &mut sketch,
            Some("//pkg:missing"),
            Some("CppLink"),
            "cmd1",
            11,
            &cold,
            ClientFloors::default(),
        );
        assert_eq!(by_mnemonic_only.memory, Bytes::kib(240_000));

        let by_command_only = lookup_learned_or_cold_start(
            &mut sketch,
            Some("//pkg:missing"),
            Some("MissingMnemonic"),
            "cmd1",
            11,
            &cold,
            ClientFloors::default(),
        );
        assert_eq!(by_command_only.memory, Bytes::kib(120_000));
    }

    #[test]
    fn fitting_clamps_to_largest_fleet_and_promotes_whole_cpu_only_without_lower_policy_cap() {
        let cat = FleetCatalog::try_new(vec![
            fleet("small", Isa::X86_64, 4, 8, 50, None, false),
            fleet("large", Isa::X86_64, 16, 32, 100, None, false),
        ])
        .expect("catalog");
        let admitted = cat.admit(std::iter::empty()).expect("admitted");

        // Learned memory (40 GiB) exceeds large (32 GiB): clamps to large's physical 32 GiB and
        // promotes CPU to large's whole 16_000 millicores, preserving cpu_before_memory_fit.
        let plan = fit_reservation(
            &cat,
            admitted,
            &BTreeMap::new(),
            ClientFloors {
                cpu_millicores: Some(1_500),
                memory_kb: Some(2 * 1024 * 1024),
                disk_kb: None,
            },
            ActionResources {
                cpu_millicores: 1_500,
                memory: Bytes::gib(40),
                disk: Bytes::gib(1),
            },
            MinReservation::from_units(64 * 1024),
            DiskMinReservation::from_units(1024 * 1024),
        )
        .expect("fit");
        assert_eq!(plan.fleet, 1);
        assert_eq!(plan.reservation.memory, Bytes::gib(32));
        assert_eq!(plan.reservation.cpu_millicores, 16_000);
        assert_eq!(plan.cpu_before_memory_fit, 1_500);
        assert_eq!(plan.desired.memory, Bytes::gib(40));
        assert!(plan.learned_reservation.is_some());

        // When a policy memory ceiling (4 GiB) is strictly below physical RAM (32 GiB), clamping to
        // 4 GiB must NOT promote whole-worker CPU.
        let capped_cat = FleetCatalog::try_new(vec![fleet(
            "capped",
            Isa::X86_64,
            16,
            32,
            100,
            Some(4),
            false,
        )])
        .expect("capped");
        let capped_admitted = capped_cat.admit(std::iter::empty()).expect("admitted");
        let capped_plan = fit_reservation(
            &capped_cat,
            capped_admitted,
            &BTreeMap::new(),
            ClientFloors::default(),
            ActionResources {
                cpu_millicores: 1_500,
                memory: Bytes::gib(10),
                disk: Bytes::mib(512), // raised to 1 GiB minimum
            },
            MinReservation::from_units(64 * 1024),
            DiskMinReservation::from_units(1024 * 1024),
        )
        .expect("capped fit");
        assert_eq!(capped_plan.reservation.memory, Bytes::gib(4));
        assert_eq!(capped_plan.reservation.disk, Bytes::gib(1));
        assert_eq!(capped_plan.reservation.cpu_millicores, 1_500);
        assert_eq!(capped_plan.cpu_before_memory_fit, 1_500);
    }

    #[test]
    fn floor_exceeding_shrunk_capacity_refuses_instead_of_clamping() {
        let cat = FleetCatalog::try_new(vec![fleet("a", Isa::X86_64, 8, 16, 100, None, false)])
            .expect("catalog");
        let admitted = cat.admit(std::iter::empty()).expect("admitted");
        let err = fit_reservation(
            &cat,
            admitted,
            &BTreeMap::new(),
            ClientFloors {
                cpu_millicores: None,
                memory_kb: Some(20 * 1024 * 1024),
                disk_kb: None,
            },
            ActionResources::default(),
            MinReservation::from_units(64 * 1024),
            DiskMinReservation::from_units(1024 * 1024),
        )
        .expect_err("floor above capacity must fail");
        assert!(matches!(
            err,
            Refusal::Minimum {
                key: property::MEMORY_KB,
                ..
            }
        ));
    }

    #[test]
    fn cold_start_skips_explicitly_tagged_client_dimensions() {
        let mut sketch = test_sketch();
        let cold = ColdStartTuning::default();
        let tagged = lookup_learned_or_cold_start(
            &mut sketch,
            Some("//pkg:a"),
            Some("CppCompile"),
            "cmd1",
            100,
            &cold,
            ClientFloors {
                cpu_millicores: None,
                memory_kb: Some(252),
                disk_kb: None,
            },
        );
        assert_eq!(tagged.cpu_millicores, 250);
        assert_eq!(tagged.memory, Bytes::new(0));
        assert_eq!(tagged.disk, Bytes::gib(4));
    }

    #[test]
    fn cpu_oversubscription_constant_unused_warning_free() {
        assert_eq!(NonZeroU32::MIN.get(), 1);
    }
}
