//! Typed `NativeLink` configuration built from the controller and worker specs. Complex values
//! are deserialized from JSON so `NativeLink`'s own serde defaults apply.

use std::path::Path;

use gang_spi::{
    CPU_COUNT_ENV, ControllerSpec, MEMORY_KB_ENV, NETWORK_ENV, StoreAccess, WorkerSpec, WorkerTls,
};
use gang_types::{FleetCatalog, MatchKind, Url, property};
use nativelink_config::cas_server::{
    AcStoreConfig, ByteStreamConfig, CapabilitiesConfig, CapabilitiesRemoteExecutionConfig,
    CasStoreConfig, ExecutionConfig, LocalWorkerConfig, SchedulerConfig, StoreConfig,
    WithInstanceName, WorkerApiConfig,
};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::config::{Http2Tuning, NativeLinkConfig, WholeSeconds};
use crate::enforcement::{ColdStartTuning, DiskEnforcement, MemoryEnforcement, MemoryTuning};
use crate::errors::NativeLinkConfigError;
use crate::properties::{ACTION_STORE, CONTENT_STORE, WORKER_ACTION_STORE, WORKER_CONTENT_STORE};

/// The stores the controller serves: the content and action stores of the [`ControllerSpec`].
pub(crate) fn controller_stores(
    spec: &ControllerSpec<'_>,
) -> Result<Vec<StoreConfig>, NativeLinkConfigError> {
    Ok(vec![
        named_store(CONTENT_STORE, &spec.content_store.0)?,
        named_store(ACTION_STORE, &spec.action_store.0)?,
    ])
}

/// The scheduler the controller runs: a simple scheduler with the configured timeouts and the
/// platform properties that workers register.
pub(crate) fn scheduler(
    config: &NativeLinkConfig,
    spec: &ControllerSpec<'_>,
) -> Result<SchedulerConfig, NativeLinkConfigError> {
    if config.cold_start.enabled {
        let cold = &config.cold_start;
        let memory_kb = cold.memory.as_kib();
        let disk_kb = cold.disk.as_kib();
        let (cpu_text, memory_text, disk_text) = (
            cold.cpu_count.to_string(),
            memory_kb.to_string(),
            disk_kb.to_string(),
        );
        let accepting = spec.fleets.accepting(|key| match key {
            property::CPU_COUNT => Some(cpu_text.as_str()),
            property::MEMORY_KB => Some(memory_text.as_str()),
            property::DISK_KB => Some(disk_text.as_str()),
            _ => None,
        });
        if accepting.is_empty() {
            return Err(NativeLinkConfigError::ColdStartFitsNoFleet {
                cpu_count: cold.cpu_count.get(),
                memory_kb,
                disk_kb,
            });
        }
    }
    let tuning = &config.scheduler;
    let client_action_timeout = WholeSeconds::try_new(spec.client_action_timeout)
        .map_err(|_| NativeLinkConfigError::Duration {
            field: "client_action_timeout",
            requirement: "must be a whole number of seconds, at least 1s and at most about 584 years",
        })?
        .as_units();
    let mut simple = json!({
        "supported_platform_properties": supported_platform_properties(spec.fleets),
        "client_action_timeout_s": client_action_timeout,
        "worker_timeout_s": tuning.worker_timeout.as_units(),
        "retain_completed_for_s": tuning.retain_completed_for.as_units(),
        "max_job_retries": tuning.max_job_retries,
        "max_worker_loss_retries": tuning.max_worker_loss_retries,
    });
    if let Some(escalation) = memory_escalation(&config.memory) {
        simple["memory_escalation"] = escalation;
    }
    let mut historical = json!({
        "learn_from_workers": true,
        "headroom_percent": tuning.headroom_percent,
        "window_days": tuning.window_days,
        "scheduler": { "simple": simple },
    });
    if let Some(cold) = cold_start(&config.cold_start) {
        historical["cold_start"] = cold;
    }
    deserialize(json!({
        "name": config.scheduler_name.as_str(),
        "historical_resource": historical,
    }))
}

/// The scheduler's memory escalation, which requeues a killed action with a larger reservation.
/// Only `soft` memory enforcement kills actions, so it is absent otherwise.
fn memory_escalation(memory: &MemoryTuning) -> Option<Value> {
    (memory.enforcement == MemoryEnforcement::Soft).then(|| {
        json!({
            "percent": memory.escalation_percent.get(),
            "max_steps": memory.escalation_max_steps,
        })
    })
}

/// The scheduler's cold start: the reservation of an action no hint matches and the client left
/// untagged. Absent when cold start is disabled, so such actions reserve nothing.
fn cold_start(tuning: &ColdStartTuning) -> Option<Value> {
    tuning.enabled.then(|| {
        json!({
            "cpu_count": tuning.cpu_count.get(),
            "memory_kb": tuning.memory.as_kib(),
            "disk_kb": tuning.disk.as_kib(),
        })
    })
}

/// The platform properties the scheduler declares: every key the catalog declares, each with the
/// match kind `NativeLink` applies to it.
fn supported_platform_properties(fleets: &FleetCatalog) -> Map<String, Value> {
    fleets
        .declared()
        .iter()
        .map(|&(key, kind)| (key.to_owned(), json!(property_type(kind))))
        .collect()
}

/// The `NativeLink` property type that matches a key of `kind`.
const fn property_type(kind: MatchKind) -> &'static str {
    match kind {
        MatchKind::Exact => "exact",
        MatchKind::Minimum => "minimum",
        MatchKind::Ignored => "ignore",
    }
}

/// The services of the client listener: `CAS`, `AC`, `ByteStream`, capabilities and execution.
pub(crate) struct ClientServices {
    /// The `CAS` services.
    pub cas: Vec<WithInstanceName<CasStoreConfig>>,
    /// The `AC` services.
    pub ac: Vec<WithInstanceName<AcStoreConfig>>,
    /// The `ByteStream` services.
    pub bytestream: Vec<WithInstanceName<ByteStreamConfig>>,
    /// The capabilities services.
    pub capabilities: Vec<WithInstanceName<CapabilitiesConfig>>,
    /// The execution services.
    pub execution: Vec<WithInstanceName<ExecutionConfig>>,
}

/// The services of the worker listener: the worker API, `CAS`, `AC` and `ByteStream`.
pub(crate) struct WorkerServices {
    /// The worker API the workers connect to.
    pub worker_api: WorkerApiConfig,
    /// The `CAS` services.
    pub cas: Vec<WithInstanceName<CasStoreConfig>>,
    /// The `AC` services.
    pub ac: Vec<WithInstanceName<AcStoreConfig>>,
    /// The `ByteStream` services.
    pub bytestream: Vec<WithInstanceName<ByteStreamConfig>>,
}

/// The services of the client listener: `CAS`, `AC`, `ByteStream`, capabilities and execution.
pub(crate) fn client_services(config: &NativeLinkConfig) -> ClientServices {
    let name = config.scheduler_name.as_str();
    ClientServices {
        cas: vec![cas()],
        ac: vec![ac()],
        bytestream: vec![bytestream(&config.http2)],
        capabilities: vec![WithInstanceName {
            instance_name: String::new(),
            config: CapabilitiesConfig {
                remote_execution: Some(CapabilitiesRemoteExecutionConfig {
                    scheduler: name.to_owned(),
                }),
                remote_cache_compression: false,
            },
        }],
        execution: vec![WithInstanceName {
            instance_name: String::new(),
            config: ExecutionConfig {
                experimental_buck2_invocation_isolation: false,
                cas_store: CONTENT_STORE.to_owned(),
                scheduler: name.to_owned(),
            },
        }],
    }
}

/// The services of the worker listener: the worker API, `CAS`, `AC` and `ByteStream`.
pub(crate) fn worker_services(config: &NativeLinkConfig) -> WorkerServices {
    WorkerServices {
        worker_api: WorkerApiConfig {
            scheduler: config.scheduler_name.as_str().to_owned(),
            disable_kill_revoked_operations: false,
            kill_revoked_operations_interval_s: 0,
        },
        cas: vec![cas()],
        ac: vec![ac()],
        bytestream: vec![bytestream(&config.http2)],
    }
}

/// The content store served by either listener.
fn cas() -> WithInstanceName<CasStoreConfig> {
    WithInstanceName {
        instance_name: String::new(),
        config: CasStoreConfig {
            cas_store: CONTENT_STORE.to_owned(),
            experimental_chunking: None,
        },
    }
}

/// The action store served by either listener.
fn ac() -> WithInstanceName<AcStoreConfig> {
    WithInstanceName {
        instance_name: String::new(),
        config: AcStoreConfig {
            ac_store: ACTION_STORE.to_owned(),
            read_only: false,
        },
    }
}

/// The stores and the local worker of one worker process.
pub(crate) struct WorkerSettings {
    /// The worker's fast-slow content store and its action cache.
    pub stores: Vec<StoreConfig>,
    /// The local worker that executes actions.
    pub worker: LocalWorkerConfig,
}

/// Builds the worker's stores and the local worker that uses them.
pub(crate) fn worker(
    config: &NativeLinkConfig,
    endpoint: &Url,
    spec: &WorkerSpec,
    tls: WorkerTls<'_>,
) -> Result<WorkerSettings, NativeLinkConfigError> {
    let endpoint = worker_endpoint(endpoint, tls)?;
    let (content, action) = match spec.store_access {
        StoreAccess::Direct => (spec.content_store.0.clone(), spec.action_store.0.clone()),
        StoreAccess::ViaController => (
            controller_grpc_store(&endpoint, "cas", tls)?,
            controller_grpc_store(&endpoint, "ac", tls)?,
        ),
    };
    let fast = path_str(&spec.fast_cas_directory)?;
    let fast_slow = json!({
        "fast_slow": {
            "fast": {
                "filesystem": {
                    "content_path": format!("{fast}/content"),
                    "temp_path": format!("{fast}/tmp"),
                    "eviction_policy": { "max_bytes": spec.fast_cas_budget.get() },
                },
            },
            "slow": content,
        },
    });
    let stores = vec![
        named_store(WORKER_CONTENT_STORE, &fast_slow)?,
        named_store(WORKER_ACTION_STORE, &action)?,
    ];
    let worker = deserialize(local_worker(config, spec, &endpoint)?)?;
    Ok(WorkerSettings { stores, worker })
}

/// The worker endpoint, with its scheme checked against TLS and without a trailing `/`.
fn worker_endpoint(endpoint: &Url, tls: WorkerTls<'_>) -> Result<String, NativeLinkConfigError> {
    let expected = tls.scheme();
    if endpoint.scheme() != expected {
        return Err(NativeLinkConfigError::WorkerEndpointTls { expected });
    }
    Ok(endpoint.as_str().trim_end_matches('/').to_owned())
}

/// A gRPC store that reaches the controller's worker listener for `store_type` (`cas` or `ac`).
fn controller_grpc_store(
    endpoint: &str,
    store_type: &str,
    tls: WorkerTls<'_>,
) -> Result<Value, NativeLinkConfigError> {
    let mut grpc_endpoint = json!({ "address": endpoint });
    if let Some(tls) = tls_config(tls)? {
        grpc_endpoint["tls_config"] = tls;
    }
    Ok(json!({
        "grpc": {
            "instance_name": "",
            "endpoints": [grpc_endpoint],
            "store_type": store_type,
        },
    }))
}

/// The local worker's JSON configuration.
fn local_worker(
    config: &NativeLinkConfig,
    spec: &WorkerSpec,
    endpoint: &str,
) -> Result<Value, NativeLinkConfigError> {
    let tuning = &config.worker;
    let keepalive = tuning.keepalive_interval.as_units();
    let api_endpoint = json!({
        "uri": endpoint,
        "timeout": tuning.connect_timeout.get().as_secs_f32(),
        "tcp_keepalive_s": keepalive,
        "http2_keepalive_interval_s": keepalive,
    });
    let platform_properties: Map<String, Value> = spec
        .properties
        .worker_properties()
        .map(|(key, value)| (key.to_owned(), json!({ "values": [value] })))
        .collect();
    let mut worker = json!({
        "name": spec.instance.worker_name(),
        "worker_api_endpoint": api_endpoint,
        "max_inflight_tasks": spec.slots.count().get(),
        "single_use": spec.slots.is_single_use(),
        "cas_fast_slow_store": WORKER_CONTENT_STORE,
        "upload_action_result": { "ac_store": WORKER_ACTION_STORE },
        "work_directory": path_str(&spec.work_directory)?,
        "entrypoint": path_str(&spec.entrypoint)?,
        "additional_environment": {
            (NETWORK_ENV): { "property": property::NETWORK },
            (CPU_COUNT_ENV): { "property": property::CPU_COUNT },
            (MEMORY_KB_ENV): { "property": property::MEMORY_KB },
        },
        "platform_properties": platform_properties,
        "max_action_timeout_s": tuning.action_timeout.as_units(),
    });
    if let Some(enforcement) = resource_enforcement(config) {
        worker["resource_enforcement"] = enforcement;
    }
    Ok(worker)
}

/// The worker's resource enforcement, or `None` when neither axis is enforced. An axis that is not
/// enforced is written as `none` rather than omitted, because `NativeLink` enables memory by
/// default: omitting it would enable it.
fn resource_enforcement(config: &NativeLinkConfig) -> Option<Value> {
    let memory = config.memory.enforcement == MemoryEnforcement::Soft;
    let disk = config.disk.enforcement;
    if !memory && disk == DiskEnforcement::None {
        return None;
    }
    let mut enforcement = Map::new();
    if memory {
        enforcement.insert("memory".to_owned(), json!("soft"));
        enforcement.insert(
            "memory_headroom_percent".to_owned(),
            json!(config.memory.headroom_percent),
        );
    } else {
        enforcement.insert("memory".to_owned(), json!("none"));
    }
    let disk_name = match disk {
        DiskEnforcement::None => "none",
        DiskEnforcement::Guard => "guard",
        DiskEnforcement::Soft => "soft",
    };
    enforcement.insert("disk".to_owned(), json!(disk_name));
    if disk == DiskEnforcement::Soft {
        enforcement.insert(
            "disk_headroom_percent".to_owned(),
            json!(config.disk.headroom_percent),
        );
    }
    Some(Value::Object(enforcement))
}

/// The `ByteStream` service entry, with how long a write survives a client disconnect.
fn bytestream(http2: &Http2Tuning) -> WithInstanceName<ByteStreamConfig> {
    WithInstanceName {
        instance_name: String::new(),
        config: ByteStreamConfig {
            cas_store: CONTENT_STORE.to_owned(),
            max_bytes_per_stream: 0,
            // A timeout larger than usize::MAX seconds is effectively unbounded.
            persist_stream_on_disconnect_timeout_s: usize::try_from(
                http2.stream_idle_timeout.as_units(),
            )
            .unwrap_or(usize::MAX),
        },
    }
}

/// The TLS settings of a dialing client, in `NativeLink`'s `tls_config` shape.
fn tls_config(tls: WorkerTls<'_>) -> Result<Option<Value>, NativeLinkConfigError> {
    Ok(match tls {
        WorkerTls::Plain => None,
        WorkerTls::ServerOnly {
            server_ca: Some(ca),
        } => Some(json!({ "ca_file": path_str(ca)? })),
        WorkerTls::ServerOnly { server_ca: None } => Some(json!({ "use_native_roots": true })),
        WorkerTls::Mutual(tls) => Some(json!({
            "ca_file": path_str(tls.server_ca)?,
            "cert_file": path_str(tls.certificate)?,
            "key_file": path_str(tls.private_key)?,
        })),
    })
}

/// A store named `name` built from a rendered store spec, which must be one JSON object with
/// exactly one store variant key.
fn named_store(name: &str, spec: &Value) -> Result<StoreConfig, NativeLinkConfigError> {
    let mut object = spec
        .as_object()
        .cloned()
        .ok_or(NativeLinkConfigError::StoreSpec(
            "a store spec must be a JSON object",
        ))?;
    object.insert("name".to_owned(), Value::String(name.to_owned()));
    serde_json::from_value(Value::Object(object)).map_err(|_| {
        NativeLinkConfigError::StoreSpec("a store spec must have exactly one store variant")
    })
}

/// Deserializes a settings value, mapping failures to [`NativeLinkConfigError::Settings`].
fn deserialize<T: DeserializeOwned>(value: Value) -> Result<T, NativeLinkConfigError> {
    serde_json::from_value(value).map_err(NativeLinkConfigError::Settings)
}

/// A path as the UTF-8 string `NativeLink`'s configuration requires.
fn path_str(path: &Path) -> Result<&str, NativeLinkConfigError> {
    path.to_str()
        .ok_or_else(|| NativeLinkConfigError::NotUtf8(path.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use gang_spi::{ClientTls, ControllerSpec, RenderedStoreSpec, Slots, StoreAccess, WorkerSpec};
    use gang_types::{
        Bytes, FleetCatalog, FleetName, FleetProperties, HeadroomPercent, InstanceId, Isa,
        Resources,
    };
    use nativelink_config::cas_server::{
        DiskEnforcement as NativeDiskEnforcement, MemoryEnforcement as NativeMemoryEnforcement,
        ResourceEnforcementConfig,
    };
    use nativelink_config::schedulers::{HistoricalResourceSpec, SchedulerSpec, SimpleSpec};
    use nativelink_config::stores::{ClientTlsConfig, StoreSpec};
    use serde_json::json;

    use super::*;
    use crate::config::NativeLinkConfig;
    use crate::enforcement::{DiskTuning, EscalationPercent};
    use crate::whole_kib::WholeKib;

    fn config() -> NativeLinkConfig {
        serde_json::from_value(json!({
            "scheduler_name": "MAIN_SCHEDULER",
        }))
        .expect("valid config")
    }

    /// The worker endpoint the plugin resolves: plain gRPC on loopback.
    fn endpoint() -> Url {
        "grpc://127.0.0.1:9100".parse().expect("url")
    }

    fn memory_store() -> RenderedStoreSpec {
        RenderedStoreSpec(json!({ "memory": {} }))
    }

    fn fleet(name: &str, isa: Isa, capacity: &str) -> FleetProperties {
        FleetProperties::new(
            &FleetName::try_new(name).expect("valid fleet name"),
            isa,
            &"docker",
            Resources {
                vcpus: 4,
                memory: Bytes::new(8 * 1024 * 1024 * 1024),
                disk: Bytes::new(100 << 30),
            },
            [("capacity", capacity.to_owned())],
        )
    }

    fn catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![fleet("fleet-a", Isa::X86_64, "2")]).expect("valid catalog")
    }

    fn controller_spec<'a>(
        content: &'a RenderedStoreSpec,
        action: &'a RenderedStoreSpec,
        fleets: &'a FleetCatalog,
    ) -> ControllerSpec<'a> {
        ControllerSpec {
            content_store: content,
            action_store: action,
            fleets,
            client_action_timeout: Duration::from_mins(10),
        }
    }

    fn worker_spec(
        instance: &InstanceId,
        properties: &FleetProperties,
        content: &RenderedStoreSpec,
        action: &RenderedStoreSpec,
        store_access: StoreAccess,
    ) -> WorkerSpec {
        WorkerSpec {
            instance: instance.clone(),
            properties: properties.clone(),
            slots: Slots::LongLived(NonZeroU32::new(4).expect("non-zero slots")),
            work_directory: Path::new("/var/gang/work").to_path_buf(),
            fast_cas_directory: Path::new("/var/gang/cas").to_path_buf(),
            fast_cas_budget: Bytes::new(1024),
            content_store: content.clone(),
            action_store: action.clone(),
            store_access,
            entrypoint: Path::new("/usr/bin/gang").to_path_buf(),
        }
    }

    #[test]
    fn non_object_store_spec_is_rejected() {
        let content = RenderedStoreSpec(json!("memory"));
        let action = memory_store();
        let fleets = catalog();
        assert!(matches!(
            controller_stores(&controller_spec(&content, &action, &fleets)),
            Err(NativeLinkConfigError::StoreSpec(_))
        ));
    }

    #[test]
    fn scheduler_takes_name_and_tuning() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        let scheduler =
            scheduler(&config(), &controller_spec(&content, &action, &fleets)).expect("scheduler");
        assert_eq!(scheduler.name, "MAIN_SCHEDULER");
        let SchedulerSpec::HistoricalResource(historical) = &scheduler.spec else {
            panic!("expected historical_resource, got {:?}", scheduler.spec);
        };
        assert!(historical.learn_from_workers);
        assert_eq!(historical.headroom_percent, 20);
        assert_eq!(historical.window_days, 7);
        let SchedulerSpec::Simple(simple) = historical.scheduler.as_ref() else {
            panic!(
                "expected nested simple scheduler, got {:?}",
                historical.scheduler
            );
        };
        assert_eq!(simple.client_action_timeout_s, 600);
        assert!(simple.supported_platform_properties.is_some());
    }

    /// A config whose `cold_start` section is `cold`, with every other setting defaulted.
    fn config_with_cold_start(cold: &Value) -> NativeLinkConfig {
        serde_json::from_value(json!({
            "worker_endpoint": "grpc://127.0.0.1:9100",
            "scheduler_name": "MAIN_SCHEDULER",
            "cold_start": cold,
        }))
        .expect("valid config")
    }

    #[test]
    fn default_cold_start_fits_the_catalog() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        assert!(scheduler(&config(), &controller_spec(&content, &action, &fleets)).is_ok());
    }

    #[test]
    fn cold_start_no_fleet_fits_is_rejected() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        let spec = controller_spec(&content, &action, &fleets);
        let memory = config_with_cold_start(&json!({ "memory": "64GiB" }));
        assert!(matches!(
            scheduler(&memory, &spec),
            Err(NativeLinkConfigError::ColdStartFitsNoFleet {
                cpu_count: 1,
                memory_kb: 67_108_864,
                disk_kb: 4_194_304,
            })
        ));
        let disk = config_with_cold_start(&json!({ "disk": "1TiB" }));
        assert!(matches!(
            scheduler(&disk, &spec),
            Err(NativeLinkConfigError::ColdStartFitsNoFleet {
                cpu_count: 1,
                memory_kb: 4_194_304,
                disk_kb: 1_073_741_824,
            })
        ));
    }

    #[test]
    fn oversized_cold_start_builds_when_disabled() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        let disabled = config_with_cold_start(&json!({
            "enabled": false,
            "memory": "64GiB",
            "disk": "1TiB",
        }));
        assert!(scheduler(&disabled, &controller_spec(&content, &action, &fleets)).is_ok());
    }

    #[test]
    fn declaration_covers_fleet_keys_and_resource_minimums() {
        let declared = supported_platform_properties(&catalog());
        for key in [
            property::FLEET,
            property::ISA,
            property::SANDBOX,
            "capacity",
        ] {
            assert_eq!(declared.get(key), Some(&json!("exact")), "{key}");
        }
        for key in [property::CPU_COUNT, property::MEMORY_KB, property::DISK_KB] {
            assert_eq!(declared.get(key), Some(&json!("minimum")), "{key}");
        }
        assert_eq!(declared.get(property::NETWORK), Some(&json!("ignore")));
        assert_eq!(declared.len(), 8);
    }

    #[test]
    fn client_action_timeout_must_be_whole_seconds() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        for timeout in [Duration::ZERO, Duration::from_millis(1500)] {
            let spec = ControllerSpec {
                client_action_timeout: timeout,
                ..controller_spec(&content, &action, &fleets)
            };
            assert!(matches!(
                scheduler(&config(), &spec),
                Err(NativeLinkConfigError::Duration {
                    field: "client_action_timeout",
                    ..
                })
            ));
        }
    }

    #[test]
    fn client_services_carry_the_stream_timeout() {
        let services = client_services(&config());
        assert_eq!(services.cas.len(), 1);
        assert_eq!(services.cas[0].instance_name, "");
        assert_eq!(services.cas[0].config.cas_store, CONTENT_STORE);
        assert!(services.cas[0].config.experimental_chunking.is_none());
        assert_eq!(services.ac.len(), 1);
        assert_eq!(services.ac[0].config.ac_store, ACTION_STORE);
        assert!(!services.ac[0].config.read_only);
        assert_eq!(services.bytestream.len(), 1);
        assert_eq!(services.bytestream[0].config.cas_store, CONTENT_STORE);
        assert_eq!(services.bytestream[0].config.max_bytes_per_stream, 0);
        assert_eq!(
            services.bytestream[0]
                .config
                .persist_stream_on_disconnect_timeout_s,
            10
        );
        assert_eq!(services.capabilities.len(), 1);
        let remote_execution = services.capabilities[0].config.remote_execution.as_ref();
        assert_eq!(
            remote_execution.map(|r| r.scheduler.as_str()),
            Some("MAIN_SCHEDULER")
        );
        assert!(!services.capabilities[0].config.remote_cache_compression);
        assert_eq!(services.execution.len(), 1);
        assert_eq!(services.execution[0].config.scheduler, "MAIN_SCHEDULER");
        assert_eq!(services.execution[0].config.cas_store, CONTENT_STORE);
        assert!(
            !services.execution[0]
                .config
                .experimental_buck2_invocation_isolation
        );
    }

    #[test]
    fn worker_services_use_the_same_stores_and_stream_timeout() {
        let services = worker_services(&config());
        assert_eq!(services.worker_api.scheduler, "MAIN_SCHEDULER");
        assert!(!services.worker_api.disable_kill_revoked_operations);
        assert_eq!(services.worker_api.kill_revoked_operations_interval_s, 0);
        assert_eq!(services.cas.len(), 1);
        assert_eq!(services.cas[0].instance_name, "");
        assert_eq!(services.cas[0].config.cas_store, CONTENT_STORE);
        assert!(services.cas[0].config.experimental_chunking.is_none());
        assert_eq!(services.ac.len(), 1);
        assert_eq!(services.ac[0].instance_name, "");
        assert_eq!(services.ac[0].config.ac_store, ACTION_STORE);
        assert!(!services.ac[0].config.read_only);
        assert_eq!(services.bytestream.len(), 1);
        assert_eq!(services.bytestream[0].instance_name, "");
        assert_eq!(services.bytestream[0].config.cas_store, CONTENT_STORE);
        assert_eq!(services.bytestream[0].config.max_bytes_per_stream, 0);
        assert_eq!(
            services.bytestream[0]
                .config
                .persist_stream_on_disconnect_timeout_s,
            10
        );
    }

    #[test]
    fn via_controller_worker_reaches_the_stores_over_grpc() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let settings = worker(
            &config(),
            &endpoint(),
            &worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::ViaController,
            ),
            WorkerTls::Plain,
        )
        .expect("worker");
        let StoreSpec::FastSlow(fast_slow) = &settings.stores[0].spec else {
            panic!("worker content store must be fast-slow");
        };
        let StoreSpec::Grpc(grpc) = &fast_slow.slow else {
            panic!("slow store must be grpc");
        };
        assert_eq!(grpc.endpoints[0].address, "grpc://127.0.0.1:9100");
    }

    #[test]
    fn tls_worker_requires_a_grpcs_endpoint() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let tls = ClientTls {
            certificate: Path::new("/tls/cert.pem"),
            private_key: Path::new("/tls/key.pem"),
            server_ca: Path::new("/tls/ca.pem"),
        };
        let result = worker(
            &config(),
            &endpoint(),
            &worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::Direct,
            ),
            WorkerTls::Mutual(tls),
        );
        assert!(matches!(
            result,
            Err(NativeLinkConfigError::WorkerEndpointTls { expected: "grpcs" })
        ));
    }

    /// Asserts that `tls_config` names the `/tls` certificate, key, and CA files the test passed in.
    fn assert_tls_files(tls_config: Option<&ClientTlsConfig>) {
        let tls_config = tls_config.expect("tls_config");
        assert_eq!(tls_config.cert_file.as_deref(), Some("/tls/cert.pem"));
        assert_eq!(tls_config.key_file.as_deref(), Some("/tls/key.pem"));
        assert_eq!(tls_config.ca_file.as_deref(), Some("/tls/ca.pem"));
    }

    #[test]
    fn tls_files_render_into_the_controller_stores() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let grpcs: Url = "grpcs://127.0.0.1:9100".parse().expect("url");
        let tls = ClientTls {
            certificate: Path::new("/tls/cert.pem"),
            private_key: Path::new("/tls/key.pem"),
            server_ca: Path::new("/tls/ca.pem"),
        };
        let settings = worker(
            &config(),
            &grpcs,
            &worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::ViaController,
            ),
            WorkerTls::Mutual(tls),
        )
        .expect("worker");
        assert!(settings.worker.worker_api_endpoint.tls_config.is_none());
        let StoreSpec::FastSlow(fast_slow) = &settings.stores[0].spec else {
            panic!("worker content store must be fast-slow");
        };
        let StoreSpec::Grpc(content_grpc) = &fast_slow.slow else {
            panic!("slow store must be grpc");
        };
        assert_tls_files(content_grpc.endpoints[0].tls_config.as_ref());
        let StoreSpec::Grpc(action_grpc) = &settings.stores[1].spec else {
            panic!("worker action store must be grpc");
        };
        assert_tls_files(action_grpc.endpoints[0].tls_config.as_ref());
    }

    /// Server-authenticated TLS reaches both controller-backed stores.
    fn assert_server_only_settings(
        tls: WorkerTls<'_>,
        ca: Option<&str>,
        native_roots: Option<bool>,
    ) {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let spec = worker_spec(
            &instance,
            &properties,
            &content,
            &action,
            StoreAccess::ViaController,
        );
        let grpcs: Url = "grpcs://127.0.0.1:9100".parse().expect("url");
        let settings =
            worker(&config(), &grpcs, &spec, tls).expect("worker accepts server-only TLS");
        let StoreSpec::FastSlow(fast_slow) = &settings.stores[0].spec else {
            panic!("worker content store must be fast-slow");
        };
        let StoreSpec::Grpc(content_grpc) = &fast_slow.slow else {
            panic!("slow store must be grpc");
        };
        let StoreSpec::Grpc(action_grpc) = &settings.stores[1].spec else {
            panic!("worker action store must be grpc");
        };
        assert!(settings.worker.worker_api_endpoint.tls_config.is_none());
        for config in [
            content_grpc.endpoints[0].tls_config.as_ref(),
            action_grpc.endpoints[0].tls_config.as_ref(),
        ] {
            let config = config.expect("TLS configured");
            assert_eq!(config.ca_file.as_deref(), ca);
            assert_eq!(config.use_native_roots, native_roots);
            assert!(config.cert_file.is_none());
            assert!(config.key_file.is_none());
        }
    }

    #[test]
    fn server_only_with_ca_renders_no_client_identity() {
        assert_server_only_settings(
            WorkerTls::ServerOnly {
                server_ca: Some(Path::new("/tls/ca.pem")),
            },
            Some("/tls/ca.pem"),
            None,
        );
    }

    #[test]
    fn server_only_without_ca_uses_native_roots() {
        assert_server_only_settings(WorkerTls::ServerOnly { server_ca: None }, None, Some(true));
    }

    #[test]
    fn server_only_worker_rejects_a_plain_endpoint() {
        assert!(matches!(
            worker_endpoint(&endpoint(), WorkerTls::ServerOnly { server_ca: None }),
            Err(NativeLinkConfigError::WorkerEndpointTls { expected: "grpcs" })
        ));
    }

    #[test]
    fn plain_worker_rejects_a_grpcs_endpoint() {
        let grpcs: Url = "grpcs://127.0.0.1:9100".parse().expect("url");
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let result = worker(
            &config(),
            &grpcs,
            &worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::Direct,
            ),
            WorkerTls::Plain,
        );
        assert!(matches!(
            result,
            Err(NativeLinkConfigError::WorkerEndpointTls { expected: "grpc" })
        ));
    }

    #[test]
    fn single_use_worker_takes_one_action_and_says_so() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let mut spec = worker_spec(
            &instance,
            &properties,
            &content,
            &action,
            StoreAccess::Direct,
        );
        spec.slots = Slots::SingleUse;
        let settings = worker(&config(), &endpoint(), &spec, WorkerTls::Plain).expect("worker");
        assert_eq!(settings.worker.max_inflight_tasks, 1);
        assert!(settings.worker.single_use);
    }

    #[test]
    fn long_lived_worker_is_not_single_use() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let settings = worker(
            &config(),
            &endpoint(),
            &worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::Direct,
            ),
            WorkerTls::Plain,
        )
        .expect("worker");
        assert!(!settings.worker.single_use);
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_paths_are_rejected() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let bad = Path::new(OsStr::from_bytes(&[0xff]));
        assert!(matches!(
            path_str(bad),
            Err(NativeLinkConfigError::NotUtf8(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn a_non_utf8_tls_path_is_a_config_error() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let grpcs: Url = "grpcs://127.0.0.1:9100".parse().expect("url");
        let bad = Path::new(OsStr::from_bytes(b"/tls/\xff/cert.pem"));
        let tls = ClientTls {
            certificate: bad,
            private_key: Path::new("/tls/key.pem"),
            server_ca: Path::new("/tls/ca.pem"),
        };
        let result = worker(
            &config(),
            &grpcs,
            &worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::ViaController,
            ),
            WorkerTls::Mutual(tls),
        );
        assert!(matches!(result, Err(NativeLinkConfigError::NotUtf8(_))));
    }

    /// The historical resource wrapper the scheduler renders.
    fn historical_of(scheduler: &SchedulerConfig) -> &HistoricalResourceSpec {
        let SchedulerSpec::HistoricalResource(historical) = &scheduler.spec else {
            panic!("expected historical_resource, got {:?}", scheduler.spec);
        };
        historical
    }

    /// The simple scheduler nested in the scheduler's historical resource wrapper.
    fn simple_of(scheduler: &SchedulerConfig) -> &SimpleSpec {
        let historical = historical_of(scheduler);
        let SchedulerSpec::Simple(simple) = historical.scheduler.as_ref() else {
            panic!(
                "expected nested simple scheduler, got {:?}",
                historical.scheduler
            );
        };
        simple
    }

    /// The config with its memory enforcement replaced.
    fn config_with_memory(memory: MemoryTuning) -> NativeLinkConfig {
        let mut config = config();
        config.memory = memory;
        config
    }

    /// The config with its disk enforcement replaced.
    fn config_with_disk(disk: DiskTuning) -> NativeLinkConfig {
        let mut config = config();
        config.disk = disk;
        config
    }

    /// The scheduler the controller renders for `config`.
    fn rendered_scheduler(config: &NativeLinkConfig) -> SchedulerConfig {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        scheduler(config, &controller_spec(&content, &action, &fleets)).expect("scheduler")
    }

    /// The worker's `resource_enforcement` as `NativeLink` renders it.
    fn resource_enforcement_of(config: &NativeLinkConfig) -> Option<ResourceEnforcementConfig> {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let settings = worker(
            config,
            &endpoint(),
            &worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::Direct,
            ),
            WorkerTls::Plain,
        )
        .expect("worker");
        settings.worker.resource_enforcement
    }

    #[test]
    fn scheduler_renders_memory_escalation_by_default() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        let scheduler =
            scheduler(&config(), &controller_spec(&content, &action, &fleets)).expect("scheduler");
        let escalation = simple_of(&scheduler)
            .memory_escalation
            .as_ref()
            .expect("memory_escalation");
        assert_eq!(escalation.percent, 200);
        assert_eq!(escalation.max_steps, 8);
    }

    #[test]
    fn scheduler_omits_memory_escalation_when_memory_is_not_enforced() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        let config = config_with_memory(MemoryTuning {
            enforcement: MemoryEnforcement::None,
            ..MemoryTuning::default()
        });
        let scheduler =
            scheduler(&config, &controller_spec(&content, &action, &fleets)).expect("scheduler");
        assert!(simple_of(&scheduler).memory_escalation.is_none());
    }

    #[test]
    fn worker_renders_memory_soft_enforcement_by_default() {
        let enforcement = resource_enforcement_of(&config()).expect("resource_enforcement");
        assert_eq!(enforcement.memory, NativeMemoryEnforcement::Soft);
        assert_eq!(enforcement.memory_headroom_percent, 20);
    }

    #[test]
    fn custom_memory_tuning_reaches_worker_and_scheduler() {
        let config = config_with_memory(MemoryTuning {
            enforcement: MemoryEnforcement::Soft,
            headroom_percent: HeadroomPercent::from_percent(35),
            escalation_percent: EscalationPercent::try_new(300).expect("valid percent"),
            escalation_max_steps: 2,
        });
        let enforcement = resource_enforcement_of(&config).expect("resource_enforcement");
        assert_eq!(enforcement.memory, NativeMemoryEnforcement::Soft);
        assert_eq!(enforcement.memory_headroom_percent, 35);

        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        let scheduler =
            scheduler(&config, &controller_spec(&content, &action, &fleets)).expect("scheduler");
        let escalation = simple_of(&scheduler)
            .memory_escalation
            .as_ref()
            .expect("memory_escalation");
        assert_eq!(escalation.percent, 300);
        assert_eq!(escalation.max_steps, 2);
    }

    #[test]
    fn no_memory_enforcement_keeps_disk_and_writes_memory_none() {
        let config = config_with_memory(MemoryTuning {
            enforcement: MemoryEnforcement::None,
            ..MemoryTuning::default()
        });
        let enforcement = resource_enforcement_of(&config).expect("resource_enforcement");
        assert_eq!(enforcement.memory, NativeMemoryEnforcement::None);
        assert_eq!(enforcement.disk, NativeDiskEnforcement::Soft);
    }

    #[test]
    fn no_enforcement_on_either_axis_omits_resource_enforcement() {
        let mut config = config_with_memory(MemoryTuning {
            enforcement: MemoryEnforcement::None,
            ..MemoryTuning::default()
        });
        config.disk = DiskTuning {
            enforcement: DiskEnforcement::None,
            ..DiskTuning::default()
        };
        assert!(resource_enforcement_of(&config).is_none());
    }

    #[test]
    fn worker_renders_disk_soft_enforcement_by_default() {
        let enforcement = resource_enforcement_of(&config()).expect("resource_enforcement");
        assert_eq!(enforcement.disk, NativeDiskEnforcement::Soft);
        assert_eq!(enforcement.disk_headroom_percent, 20);
    }

    #[test]
    fn disk_modes_render_their_names_beside_memory() {
        for (mode, expected) in [
            (DiskEnforcement::None, NativeDiskEnforcement::None),
            (DiskEnforcement::Guard, NativeDiskEnforcement::Guard),
            (DiskEnforcement::Soft, NativeDiskEnforcement::Soft),
        ] {
            let config = config_with_disk(DiskTuning {
                enforcement: mode,
                ..DiskTuning::default()
            });
            let enforcement = resource_enforcement_of(&config).expect("resource_enforcement");
            assert_eq!(enforcement.memory, NativeMemoryEnforcement::Soft);
            assert_eq!(enforcement.disk, expected);
        }
    }

    #[test]
    fn custom_disk_headroom_reaches_the_worker() {
        let config = config_with_disk(DiskTuning {
            enforcement: DiskEnforcement::Soft,
            headroom_percent: HeadroomPercent::from_percent(35),
        });
        let enforcement = resource_enforcement_of(&config).expect("resource_enforcement");
        assert_eq!(enforcement.disk, NativeDiskEnforcement::Soft);
        assert_eq!(enforcement.disk_headroom_percent, 35);
    }

    #[test]
    fn scheduler_renders_cold_start_by_default() {
        let scheduler = rendered_scheduler(&config());
        let cold = historical_of(&scheduler)
            .cold_start
            .as_ref()
            .expect("cold_start");
        assert_eq!(cold.cpu_count, 1);
        assert_eq!(cold.memory_kb, 4_194_304);
        assert_eq!(cold.disk_kb, 4_194_304);
    }

    #[test]
    fn custom_cold_start_reaches_the_scheduler() {
        let mut config = config();
        config.cold_start = ColdStartTuning {
            enabled: true,
            cpu_count: NonZeroU32::new(4).expect("non-zero cpu count"),
            memory: WholeKib::from_kib(2 * 1024 * 1024),
            disk: WholeKib::from_kib(8 * 1024 * 1024),
        };
        let scheduler = rendered_scheduler(&config);
        let cold = historical_of(&scheduler)
            .cold_start
            .as_ref()
            .expect("cold_start");
        assert_eq!(cold.cpu_count, 4);
        assert_eq!(cold.memory_kb, 2_097_152);
        assert_eq!(cold.disk_kb, 8_388_608);
    }

    #[test]
    fn disabled_cold_start_is_not_rendered() {
        let mut config = config();
        config.cold_start = ColdStartTuning {
            enabled: false,
            ..ColdStartTuning::default()
        };
        let scheduler = rendered_scheduler(&config);
        assert!(historical_of(&scheduler).cold_start.is_none());
    }

    /// A catalog of one fleet with one CPU, `memory` bytes of memory and `disk` bytes of disk.
    fn one_cpu_catalog(memory: u64, disk: u64) -> FleetCatalog {
        let fleet = FleetProperties::new(
            &FleetName::try_new("fleet-a").expect("valid fleet name"),
            Isa::X86_64,
            &"docker",
            Resources {
                vcpus: 1,
                memory: Bytes::new(memory),
                disk: Bytes::new(disk),
            },
            [("capacity", "2".to_owned())],
        );
        FleetCatalog::try_new(vec![fleet]).expect("valid catalog")
    }

    /// A config whose cold start is one CPU with `memory_kib` of memory and `disk_kib` of disk.
    fn config_with_cold_start_kib(memory_kib: u64, disk_kib: u64) -> NativeLinkConfig {
        let mut config = config();
        config.cold_start = ColdStartTuning {
            enabled: true,
            cpu_count: NonZeroU32::new(1).expect("non-zero cpu count"),
            memory: WholeKib::from_kib(memory_kib),
            disk: WholeKib::from_kib(disk_kib),
        };
        config
    }

    /// Four GiB in KiB: the default cold start's memory and disk.
    const FOUR_GIB_KIB: u64 = 4_194_304;

    #[test]
    fn cold_start_exactly_matching_the_only_fleet_fits() {
        let content = memory_store();
        let action = memory_store();
        let fleets = one_cpu_catalog(FOUR_GIB_KIB * 1024, FOUR_GIB_KIB * 1024);
        let config = config_with_cold_start_kib(FOUR_GIB_KIB, FOUR_GIB_KIB);
        assert!(scheduler(&config, &controller_spec(&content, &action, &fleets)).is_ok());
    }

    #[test]
    fn cold_start_one_kib_of_memory_over_the_only_fleet_is_rejected() {
        let content = memory_store();
        let action = memory_store();
        let fleets = one_cpu_catalog(FOUR_GIB_KIB * 1024, FOUR_GIB_KIB * 1024);
        let config = config_with_cold_start_kib(FOUR_GIB_KIB + 1, FOUR_GIB_KIB);
        assert!(matches!(
            scheduler(&config, &controller_spec(&content, &action, &fleets)),
            Err(NativeLinkConfigError::ColdStartFitsNoFleet { .. })
        ));
    }

    #[test]
    fn disk_headroom_renders_when_memory_is_not_enforced() {
        let mut config = config_with_memory(MemoryTuning {
            enforcement: MemoryEnforcement::None,
            ..MemoryTuning::default()
        });
        config.disk = DiskTuning {
            enforcement: DiskEnforcement::Soft,
            headroom_percent: HeadroomPercent::from_percent(35),
        };
        let enforcement = resource_enforcement_of(&config).expect("resource_enforcement");
        assert_eq!(enforcement.memory, NativeMemoryEnforcement::None);
        assert_eq!(enforcement.disk, NativeDiskEnforcement::Soft);
        assert_eq!(enforcement.disk_headroom_percent, 35);
    }

    #[test]
    fn memory_headroom_renders_beside_guarded_disk() {
        let mut config = config_with_memory(MemoryTuning {
            enforcement: MemoryEnforcement::Soft,
            headroom_percent: HeadroomPercent::from_percent(40),
            ..MemoryTuning::default()
        });
        config.disk = DiskTuning {
            enforcement: DiskEnforcement::Guard,
            ..DiskTuning::default()
        };
        let enforcement = resource_enforcement_of(&config).expect("resource_enforcement");
        assert_eq!(enforcement.memory, NativeMemoryEnforcement::Soft);
        assert_eq!(enforcement.memory_headroom_percent, 40);
        assert_eq!(enforcement.disk, NativeDiskEnforcement::Guard);
    }

    #[test]
    fn escalation_renders_a_percent_with_zero_steps() {
        let config = config_with_memory(MemoryTuning {
            enforcement: MemoryEnforcement::Soft,
            escalation_percent: EscalationPercent::try_new(150).expect("valid percent"),
            escalation_max_steps: 0,
            ..MemoryTuning::default()
        });
        let scheduler = rendered_scheduler(&config);
        let escalation = simple_of(&scheduler)
            .memory_escalation
            .as_ref()
            .expect("memory_escalation");
        assert_eq!(escalation.percent, 150);
        assert_eq!(escalation.max_steps, 0);
    }
}
