//! Checks catalog admission against `NativeLink` matching and per-action ceilings.

use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use gang_spi::{ControllerSpec, RenderedStoreSpec, StoreAccess, WorkerSpec, WorkerTls};
use gang_testkit::TestResult;
use gang_types::{
    Bytes, FleetCatalog, FleetName, FleetProperties, InstanceId, Isa, Resources, WorkerLifetime,
    property,
};
use nativelink_config::cas_server::WorkerProperty;
use nativelink_config::schedulers::SchedulerSpec;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_util::platform_properties::PlatformProperties;
use serde_json::json;

use crate::config::NativeLinkConfig;
use crate::settings::{scheduler, worker};

/// The names of the test fleets, in preference order.
const NAMES: [&str; 4] = ["gang-small", "gang-large", "gang-arm", "gang-gpu"];

/// Builds one test fleet with the given ISA, sandbox, memory and disk sizes in GiB, and
/// provisioner options.
fn fleet(
    name: &str,
    isa: Isa,
    sandbox: &str,
    vcpus: u32,
    gib: u64,
    disk_gib: u64,
    options: &[(&'static str, &str)],
) -> Result<FleetProperties, Box<dyn Error>> {
    let mut policy = gang_types::FleetActionPolicy::default();
    if name == NAMES[1] {
        policy.grants.insert(gang_types::Capability::Network);
        policy.resources.memory = Some(gang_types::NonZeroBytes::gib(8));
    }
    Ok(FleetProperties::new(
        &FleetName::try_new(name)?,
        isa,
        &sandbox,
        &"host:test".parse().expect("valid environment"),
        Resources {
            vcpus,
            memory: Bytes::new(gib << 30),
            disk: Bytes::new(disk_gib << 30),
        },
        &policy,
        options
            .iter()
            .map(|(key, value)| (*key, (*value).to_owned())),
    ))
}

/// The test fleets: two sizes of one ISA and sandbox, one fleet without the `capacity` option,
/// and one fleet with a different ISA and sandbox.
fn fleets() -> Result<Vec<FleetProperties>, Box<dyn Error>> {
    Ok(vec![
        fleet(
            NAMES[0],
            Isa::X86_64,
            "bwrap",
            2,
            4,
            50,
            &[("capacity", "spot")],
        )?,
        fleet(
            NAMES[1],
            Isa::X86_64,
            "bwrap",
            8,
            16,
            100,
            &[("capacity", "on-demand")],
        )?,
        fleet(NAMES[2], Isa::Aarch64, "docker", 8, 8, 100, &[])?,
        fleet(
            NAMES[3],
            Isa::X86_64,
            "docker",
            4,
            8,
            50,
            &[("capacity", "gpu")],
        )?,
    ])
}

/// The request values to combine: every key with each value it can take. A request sets at
/// most one value per key, and may leave a key out.
fn request_groups() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        (property::FLEET, NAMES.to_vec()),
        (property::ISA, Isa::ALL.map(Isa::as_str).to_vec()),
        (property::SANDBOX, vec!["bwrap", "docker"]),
        ("capacity", vec!["spot", "on-demand", "gpu"]),
        // Each fleet's exact size appears, plus one value just above the largest fleet.
        (property::CPU_COUNT, vec!["1", "2", "4", "8", "9", "64"]),
        (property::CPU_MILLICORES, vec!["1", "1500", "4000", "8001"]),
        (
            property::MEMORY_KB,
            vec![
                "1048576", "4194304", "8388608", "16777216", "16777217", "33554432",
            ],
        ),
        // Fits every fleet, exactly the small fleets' disk, exactly the largest fleets' disk,
        // and one KiB above every fleet.
        (
            property::DISK_KB,
            vec!["1", "52428800", "104857600", "104857601"],
        ),
        (property::NETWORK, vec!["", "not-requested", "requested"]),
    ]
}

/// Every request made of at most one value per key, over `groups`.
fn requests(
    groups: &[(&'static str, Vec<&'static str>)],
) -> Vec<Vec<(&'static str, &'static str)>> {
    let mut requests = vec![Vec::new()];
    for (key, values) in groups {
        let mut next = Vec::new();
        for request in &requests {
            next.push(request.clone());
            for value in values {
                let mut extended = request.clone();
                extended.push((*key, *value));
                next.push(extended);
            }
        }
        requests = next;
    }
    requests
}

fn observations() -> &'static gang_spi::ObservationSink {
    static SINK: std::sync::LazyLock<gang_spi::ObservationSink> = std::sync::LazyLock::new(|| {
        gang_spi::ObservationSink::channel(std::num::NonZeroUsize::MIN, std::num::NonZeroUsize::MIN)
            .0
    });
    &SINK
}

/// The controller spec over the test fleets, with in-memory stores.
fn controller_spec<'a>(
    content: &'a RenderedStoreSpec,
    action: &'a RenderedStoreSpec,
    fleets: &'a FleetCatalog,
) -> ControllerSpec<'a> {
    ControllerSpec {
        actions: std::sync::Arc::new(gang_config::ActionsConfig::default()),
        scaling: std::sync::Arc::new(gang_config::ScalingConfig::default()),
        clock: std::sync::Arc::new(gang_clock::SystemClock::new()),
        control_acquire_timeout: gang_types::NonZeroDuration::from_secs(1),
        content_store: content,
        action_store: action,
        fleets,
        max_workers: 8,
        observations: observations(),
        client_action_timeout: Duration::from_mins(10),
        attempt_budget: gang_config::WorkerConfig::default()
            .attempt_budget(gang_config::ActionsConfig::default().timeout),
    }
}

/// The `NativeLink` configuration the controller runs with.
fn config() -> Result<NativeLinkConfig, Box<dyn Error>> {
    Ok(NativeLinkConfig {
        worker_endpoint: Some("grpc://127.0.0.1:9100".parse()?),
        ..NativeLinkConfig::default()
    })
}

/// The matcher `NativeLink` uses, built from the scheduler the controller declares.
fn native_manager(
    config: &NativeLinkConfig,
    spec: &ControllerSpec<'_>,
) -> Result<PlatformPropertyManager, Box<dyn Error>> {
    let SchedulerSpec::HistoricalResource(historical) = scheduler(config, spec)?.spec else {
        return Err("controller scheduler is not historical_resource".into());
    };
    let SchedulerSpec::Simple(simple) = *historical.scheduler else {
        return Err("nested controller scheduler is not simple".into());
    };
    let declared = simple
        .supported_platform_properties
        .ok_or("controller scheduler declares no platform properties")?;
    Ok(PlatformPropertyManager::new(declared))
}

/// Converts `pairs` into the platform properties `NativeLink` matches requests and workers with.
/// Requests are normalized before conversion; worker properties are left unchanged.
fn native_properties<K: AsRef<str>, V: AsRef<str>>(
    manager: &PlatformPropertyManager,
    pairs: impl IntoIterator<Item = (K, V)>,
    normalize_request: bool,
) -> Result<PlatformProperties, Box<dyn Error>> {
    let mut map: HashMap<String, String> = pairs
        .into_iter()
        .map(|(key, value)| (key.as_ref().to_owned(), value.as_ref().to_owned()))
        .collect();
    if normalize_request
        && let Some(millicores) = crate::placeable::normalized_cpu_reservation(&map)
    {
        map.insert(property::CPU_MILLICORES.to_owned(), millicores.to_string());
    }
    if normalize_request
        && map
            .get(property::NETWORK)
            .is_some_and(|value| value.is_empty() || value == "not-requested")
    {
        map.remove(property::NETWORK);
    }
    Ok(manager
        .make_platform_properties(map)
        .map_err(|error| error.to_string())?)
}

/// The single value a worker registers for `key`, as the string `NativeLink` matches on.
fn worker_value(key: &str, property: &WorkerProperty) -> Result<(String, String), Box<dyn Error>> {
    match property {
        WorkerProperty::Values(values) => match values.as_slice() {
            [value] => Ok((key.to_owned(), value.clone())),
            _ => Err(format!("worker property {key} does not register exactly one value").into()),
        },
        WorkerProperty::QueryCmd(_) => {
            Err(format!("worker property {key} is not a static value").into())
        }
    }
}

#[test]
fn catalog_accepts_exactly_the_fleets_native_matches_for_normalized_requests() -> TestResult {
    let fleets = fleets()?;
    let catalog = FleetCatalog::try_new(fleets.clone())?;
    let content = RenderedStoreSpec(json!({ "memory": {} }));
    let action = RenderedStoreSpec(json!({ "memory": {} }));
    let spec = controller_spec(&content, &action, &catalog);
    let mut config = config()?;
    let endpoint = config.take_worker_endpoint()?;
    let manager = native_manager(&config, &spec)?;

    let instance = InstanceId::try_new("local-1")?;
    let mut workers = Vec::with_capacity(fleets.len());
    for fleet in &fleets {
        let worker_spec = WorkerSpec {
            actions: std::sync::Arc::clone(&spec.actions),
            memory_pool: None,
            instance: instance.clone(),
            properties: fleet.clone(),
            lifetime: WorkerLifetime::LongLived,
            work_directory: PathBuf::from("/var/gang/work"),
            fast_cas_directory: PathBuf::from("/var/gang/cas"),
            fast_cas_budget: Bytes::new(1024),
            content_store: content.clone(),
            action_store: action.clone(),
            store_access: StoreAccess::Direct,
            entrypoint: PathBuf::from("/usr/bin/gang"),
        };
        let rendered = worker(&config, &endpoint, &worker_spec, WorkerTls::Plain)?.worker;
        let pairs = rendered
            .platform_properties
            .iter()
            .map(|(key, property)| worker_value(key, property))
            .collect::<Result<Vec<_>, _>>()?;
        workers.push((
            native_properties(&manager, pairs, false)?,
            rendered.action_ceilings,
        ));
    }

    let mut any_accepted = false;
    for request in requests(&request_groups()) {
        let accepting = catalog.admit(request.iter().copied()).unwrap_or_default();
        let native = native_properties(&manager, request.iter().copied(), true)?;
        let request_map: HashMap<String, String> = request
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        let normalized_cpu =
            crate::placeable::normalized_cpu_reservation(&request_map).or_else(|| {
                request_map
                    .get(property::CPU_MILLICORES)?
                    .parse::<u64>()
                    .ok()
            });
        for (index, (worker, ceilings)) in workers.iter().enumerate() {
            let within_ceiling = ceilings.iter().all(|(key, limit)| {
                let value = if key == property::CPU_MILLICORES {
                    normalized_cpu
                } else {
                    request_map
                        .get(key)
                        .and_then(|value| value.parse::<u64>().ok())
                };
                value.is_none_or(|value| value <= *limit)
            });
            let expected = native.is_satisfied_by(worker, false) && within_ceiling;
            assert_eq!(
                accepting.contains(index),
                expected,
                "request {request:?} against fleet {index}"
            );
            any_accepted |= expected;
        }
    }
    assert!(any_accepted, "no request was accepted by any fleet");
    Ok(())
}

#[test]
fn admission_rejects_unknown_invalid_and_ungranted_properties() -> TestResult {
    let catalog = FleetCatalog::try_new(fleets()?)?;
    assert!(catalog.admit([("unrecognized", "value")]).is_err());
    assert!(catalog.admit([(property::NETWORK, "invalid")]).is_err());
    let requested = catalog
        .admit([(property::ISA, "x86-64"), (property::NETWORK, "requested")])
        .map_err(|error| error.to_string())?;
    assert_eq!(requested.iter().collect::<Vec<_>>(), vec![1]);
    assert!(
        catalog
            .admit([(property::ISA, "aarch64"), (property::NETWORK, "requested")])
            .is_err()
    );
    Ok(())
}
