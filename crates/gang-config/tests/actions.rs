//! Parsing action policy and rejecting invalid action settings.

use gang_config::{ActionsConfig, DiskEnforcement, MemoryEnforcement};

const CUSTOM: &str = r#"
reservation_headroom_percent = 25
cpu_oversubscription = 3
history_window = "9d"
history_keys = 321
history_memory_budget = "123MiB"
history_flush_interval = "17s"
retain_completed_for = "41s"
max_job_retries = 4
max_worker_loss_retries = 6
timeout = "13m"
kill_grace = "1300ms"
[memory]
enforcement = "soft"
min_reservation = "12MiB"
reserve = "32MiB"
pressure_stall_percent = 7
pressure_poll_interval = "50ms"
pressure_kill_after = "250ms"
sample_interval = "20ms"
escalation_percent = 250
max_steps = 3
headroom_percent = 31
soft_samples = 4
soft_spike_percent = 275
[disk]
enforcement = "guard"
min_reservation = "23MiB"
pressure_free_percent = 9
pressure_poll_interval = "3s"
pressure_kill_after = "1500ms"
sample_interval = "4s"
escalation_percent = 230
max_steps = 5
headroom_percent = 33
soft_samples = 3
[cold_start]
enabled = false
cpu_millicores = 3000
memory = "48MiB"
disk = "52MiB"
"#;

#[test]
fn memory_pressure_defaults_parse() {
    let config: ActionsConfig = toml::from_str("").expect("default action policy");
    assert_eq!(config.cpu_oversubscription.get(), 2);
    assert_eq!(config.cold_start.cpu_millicores.get(), 250);
    assert_eq!(config.memory.pressure_stall_percent.get(), 10);
    assert_eq!(config.memory.pressure_poll_interval.as_millis(), 250);
    assert_eq!(config.memory.pressure_kill_after.as_millis(), 5000);
}

#[test]
fn all_action_fields_parse_with_nondefault_values() {
    let config: ActionsConfig = toml::from_str(CUSTOM).expect("valid action policy");
    assert_eq!(config.reservation_headroom_percent.get(), 25);
    assert_eq!(config.cpu_oversubscription.get(), 3);
    assert_eq!(config.history_window.as_units(), 9);
    assert_eq!(config.history_keys.get(), 321);
    assert_eq!(config.history_memory_budget.as_u64(), 123 * 1024 * 1024);
    assert_eq!(config.history_flush_interval.as_millis(), 17_000);
    assert_eq!(config.retain_completed_for.as_units(), 41);
    assert_eq!(config.max_job_retries.get(), 4);
    assert_eq!(config.max_worker_loss_retries.get(), 6);
    assert_eq!(config.timeout.as_units(), 780);
    assert_eq!(config.kill_grace.as_millis(), 1300);
    let memory = config.memory;
    assert_eq!(memory.enforcement, MemoryEnforcement::Soft);
    assert_eq!(memory.min_reservation.as_units(), 12 * 1024);
    assert_eq!(memory.reserve.as_u64(), 32 * 1024 * 1024);
    assert_eq!(memory.pressure_stall_percent.get(), 7);
    assert_eq!(memory.pressure_poll_interval.as_millis(), 50);
    assert_eq!(memory.pressure_kill_after.as_millis(), 250);
    assert_eq!(memory.sample_interval.as_millis(), 20);
    assert_eq!(memory.escalation_percent.get(), 250);
    assert_eq!(memory.max_steps.get(), 3);
    assert_eq!(memory.headroom_percent.get(), 31);
    assert_eq!(memory.soft_samples.get(), 4);
    assert_eq!(memory.soft_spike_percent.get(), 275);
    let disk = config.disk;
    assert_eq!(disk.enforcement, DiskEnforcement::Guard);
    assert_eq!(disk.min_reservation.as_units(), 23 * 1024);
    assert_eq!(disk.pressure_free_percent.get(), 9);
    assert_eq!(disk.pressure_poll_interval.as_millis(), 3000);
    assert_eq!(disk.pressure_kill_after.as_millis(), 1500);
    assert_eq!(disk.sample_interval.as_millis(), 4000);
    assert_eq!(disk.escalation_percent.get(), 230);
    assert_eq!(disk.max_steps.get(), 5);
    assert_eq!(disk.headroom_percent.get(), 33);
    assert_eq!(disk.soft_samples.get(), 3);
    assert!(!config.cold_start.enabled);
    assert_eq!(config.cold_start.cpu_millicores.get(), 3000);
    assert_eq!(config.cold_start.memory.as_units(), 48 * 1024);
    assert_eq!(config.cold_start.disk.as_units(), 52 * 1024);
}

#[test]
fn all_enforcement_modes_parse_and_memory_guard_is_rejected() {
    for mode in ["none", "pressure", "soft"] {
        let text = format!("[memory]\nenforcement = \"{mode}\"");
        assert!(toml::from_str::<ActionsConfig>(&text).is_ok(), "{mode}");
    }
    for mode in ["none", "guard", "pressure", "soft"] {
        let text = format!("[disk]\nenforcement = \"{mode}\"");
        assert!(toml::from_str::<ActionsConfig>(&text).is_ok(), "{mode}");
    }
    assert!(toml::from_str::<ActionsConfig>("[memory]\nenforcement = \"guard\"").is_err());
}

#[test]
fn invalid_values_and_removed_fields_are_rejected() {
    for text in [
        "cpu_oversubscription = 0",
        "history_window = \"0d\"",
        "history_window = \"25h\"",
        "history_keys = 0",
        "history_memory_budget = \"0B\"",
        "history_flush_interval = \"0ms\"",
        "retain_completed_for = \"1500ms\"",
        "max_job_retries = 0",
        "max_worker_loss_retries = 0",
        "timeout = \"0s\"",
        "kill_grace = \"0ms\"",
        "unknown = 2",
        "[memory]\nunknown = 1",
        "[memory]\nmin_reservation = \"3MiB\"",
        "[memory]\nmin_reservation = \"4097B\"",
        "[memory]\nreserve = \"0B\"",
        "[memory]\npressure_stall_percent = 0",
        "[memory]\npressure_stall_percent = 101",
        "[memory]\npressure_poll_interval = \"0ms\"",
        "[memory]\npressure_kill_after = \"0ms\"",
        "[memory]\nsample_interval = \"0ms\"",
        "[memory]\nescalation_percent = 100",
        "[memory]\nmax_steps = 0",
        "[memory]\nsoft_samples = 0",
        "[memory]\nsoft_spike_percent = 100",
        "[memory]\nescalation_max_steps = 3",
        "[disk]\nmin_reservation = \"512KiB\"",
        "[disk]\npressure_free_percent = 0",
        "[disk]\npressure_free_percent = 101",
        "[disk]\npressure_poll_interval = \"0ms\"",
        "[disk]\npressure_kill_after = \"0ms\"",
        "[disk]\nsample_interval = \"0ms\"",
        "[disk]\nescalation_percent = 100",
        "[disk]\nmax_steps = 0",
        "[disk]\nsoft_samples = 0",
        "[disk]\nunknown = 1",
        "[cold_start]\ncpu_millicores = 0",
        "[cold_start]\ncpu_count = 1",
        "[cold_start]\nmemory = \"0B\"",
        "[cold_start]\ndisk = \"1B\"",
        "[cold_start]\nunknown = 1",
    ] {
        assert!(
            toml::from_str::<ActionsConfig>(text).is_err(),
            "accepted {text}"
        );
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
struct Backend(toml::Table);
impl gang_types::AuthenticationSettings for Backend {}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum SandboxKey {
    Noop,
}
impl std::fmt::Display for SandboxKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("noop")
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Sandboxes {
    noop: Option<toml::Table>,
}
impl gang_config::SandboxSelection for Sandboxes {
    type Key = SandboxKey;
    fn is_configured(&self, _: SandboxKey) -> bool {
        self.noop.is_some()
    }
    fn supports_capability(&self, _: SandboxKey, _: gang_types::Capability) -> bool {
        false
    }
    fn try_for_each_program<E>(
        &mut self,
        _: SandboxKey,
        _: impl FnMut(&mut std::path::PathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        Ok(())
    }
    fn try_for_each_root<E>(
        &mut self,
        _: SandboxKey,
        _: impl FnMut(&mut gang_types::AbsolutePathBuf) -> Result<(), E>,
    ) -> Result<(), E> {
        Ok(())
    }
}
#[derive(Debug)]
struct Plugins;
impl gang_config::PluginSet for Plugins {
    type Provisioner = Backend;
    type Cache = Backend;
    type Signer = Backend;
    type Database = Backend;
    type SandboxKey = SandboxKey;
    type Sandbox = Sandboxes;
    type IdentityProvider = Backend;
    type ClientAuthentication = Backend;
    type WorkerAuthentication = Backend;
    type Limits = Backend;
    type FleetOptions = ();
    fn fleet_options(
        _: &Backend,
        _: gang_config::FleetOptionsTable,
    ) -> Result<(), gang_config::FleetOptionsError> {
        Ok(())
    }
    fn option_properties((): &()) -> Vec<(&'static str, String)> {
        Vec::new()
    }
}

const CONFIG: &str = r#"
poll_interval = "3s"
client_action_timeout = "80s"
default_isa = "x86-64"
listen = "127.0.0.1:50051"
worker_listen = "127.0.0.1:9090"
[[fleets]]
name = "small"
isa = "x86-64"
action_environment = "host:test"
sandbox = "noop"
min_instances = 0
max_instances = 2
vcpus_per_instance = 2
memory_per_instance = "6GiB"
disk_per_instance = "8GiB"
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "1m"
[provisioner.local]
[cache.local]
[signer.local]
[database.local]
[sandbox.noop]
environment = "host:test"
[server]
transport = "plaintext"
[authentication.clients.none]
[authentication.workers.none]
"#;

#[test]
fn fleet_resource_validation_names_the_fleet_setting_and_values() {
    for (section, key, value, capacity) in [
        (
            "[actions.memory]\nreserve = \"6GiB\"",
            "actions.memory.reserve",
            6_u64 << 30,
            6_u64 << 30,
        ),
        (
            "[actions.memory]\nreserve = \"7GiB\"",
            "actions.memory.reserve",
            7_u64 << 30,
            6_u64 << 30,
        ),
        (
            "[actions.memory]\nmin_reservation = \"7GiB\"",
            "actions.memory.min_reservation",
            7_u64 << 30,
            5_u64 << 30,
        ),
        (
            "[actions.disk]\nmin_reservation = \"9GiB\"",
            "actions.disk.min_reservation",
            9_u64 << 30,
            8_u64 << 30,
        ),
    ] {
        let text = format!("{CONFIG}\n{section}\n");
        let error = text
            .parse::<gang_config::GangConfig<Plugins>>()
            .expect_err("invalid fleet resource");
        assert!(
            matches!(error, gang_config::ConfigError::ActionResourceDoesNotFit {
            key: actual_key, value: actual_value, capacity: actual_capacity, ref fleet, ..
        } if actual_key == key && actual_value == value && actual_capacity == capacity && fleet.as_str() == "small")
        );
    }
}

#[test]
fn cold_start_must_fit_one_fleet_unless_disabled() {
    let text = format!(
        "{CONFIG}\n[actions.cold_start]\ncpu_millicores = 3000\nmemory = \"5GiB\"\ndisk = \"7GiB\"\n"
    );
    assert!(matches!(
        text.parse::<gang_config::GangConfig<Plugins>>(),
        Err(gang_config::ConfigError::ColdStartFitsNoFleet {
            cpu_millicores: 3000,
            ..
        })
    ));
    let disabled = format!("{text}enabled = false\n");
    assert!(disabled.parse::<gang_config::GangConfig<Plugins>>().is_ok());
}

#[test]
fn every_fleet_is_checked_and_minimum_equality_is_allowed() {
    let second = r#"
[[fleets]]
name = "tiny"
isa = "x86-64"
action_environment = "host:test"
sandbox = "noop"
min_instances = 0
max_instances = 2
vcpus_per_instance = 2
memory_per_instance = "2112MiB"
disk_per_instance = "3GiB"
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "1m"
"#;
    let config = CONFIG.replace(
        "[provisioner.local]",
        &format!("{second}\n[provisioner.local]"),
    );
    let at_capacity = format!(
        "{config}\n[actions.memory]\nmin_reservation = \"2GiB\"\nreserve = \"64MiB\"\n[actions.disk]\nmin_reservation = \"3GiB\"\n"
    );
    assert!(
        at_capacity
            .parse::<gang_config::GangConfig<Plugins>>()
            .is_ok()
    );
    let too_large = at_capacity.replace("min_reservation = \"2GiB\"", "min_reservation = \"3GiB\"");
    assert!(
        matches!(too_large.parse::<gang_config::GangConfig<Plugins>>(), Err(gang_config::ConfigError::ActionResourceDoesNotFit { ref fleet, .. }) if fleet.as_str() == "tiny")
    );
}

#[test]
fn usable_memory_excludes_reserve_for_cold_start_and_minimum() {
    let config = CONFIG.replace(
        "memory_per_instance = \"6GiB\"",
        "memory_per_instance = \"4GiB\"",
    );
    assert!(matches!(
        config.parse::<gang_config::GangConfig<Plugins>>(),
        Err(gang_config::ConfigError::ColdStartFitsNoFleet { .. })
    ));

    let minimum = format!("{config}\n[actions.memory]\nmin_reservation = \"4GiB\"\n");
    assert!(matches!(
        minimum.parse::<gang_config::GangConfig<Plugins>>(),
        Err(gang_config::ConfigError::ActionResourceDoesNotFit {
            key: "actions.memory.min_reservation",
            value,
            capacity,
            relation: "at most",
            ..
        }) if value == 4_u64 << 30 && capacity == 3_u64 << 30
    ));
}

#[test]
fn reserve_is_subtracted_once_from_nominal_memory() {
    let config = CONFIG.replace(
        "memory_per_instance = \"6GiB\"",
        "memory_per_instance = \"3GiB\"",
    );
    let config = format!(
        "{config}\n[actions.memory]\nreserve = \"2GiB\"\nmin_reservation = \"1GiB\"\n[actions.cold_start]\nenabled = false\n"
    );
    let resolved = config
        .parse::<gang_config::GangConfig<Plugins>>()
        .expect("usable memory fits");
    assert_eq!(
        resolved.fleets.policies()[0].memory_per_instance,
        gang_types::NonZeroBytes::gib(1)
    );

    let large = CONFIG.replace(
        "memory_per_instance = \"6GiB\"",
        "memory_per_instance = \"64GiB\"",
    );
    let resolved = large
        .parse::<gang_config::GangConfig<Plugins>>()
        .expect("large fleet fits");
    assert_eq!(
        resolved.fleets.policies()[0].memory_per_instance,
        gang_types::NonZeroBytes::gib(63)
    );
}

#[test]
fn action_memory_ceiling_is_checked_against_usable_memory() {
    let config = CONFIG.replace(
        "sandbox = \"noop\"",
        "sandbox = \"noop\"\nactions = { resources = { memory = \"6GiB\" } }",
    );
    assert!(matches!(
        config.parse::<gang_config::GangConfig<Plugins>>(),
        Err(gang_config::ConfigError::FleetActionPolicy { .. })
    ));
}
