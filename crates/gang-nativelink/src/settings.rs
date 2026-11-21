//! Typed `NativeLink` configuration built from the controller and worker specs. Complex values
//! are deserialized from JSON so `NativeLink`'s own serde defaults apply.

use std::path::Path;

use gang_config::{ActionsConfig, ColdStartTuning, DiskEnforcement, Http2Config, WholeSeconds};
use gang_spi::{
    CPU_COUNT_ENV, ControllerSpec, MEMORY_KB_ENV, NETWORK_ENV, SIDE_CHANNEL_FILE_ENV, StoreAccess,
    WorkerSpec, WorkerTls,
};
use gang_types::{FleetCatalog, FleetProperties, MatchKind, Url, WorkerLifetime, property};
use nativelink_config::cas_server::{
    AcStoreConfig, ByteStreamConfig, CapabilitiesConfig, CapabilitiesRemoteExecutionConfig,
    CasStoreConfig, ExecutionConfig, LocalWorkerConfig, SchedulerConfig, StoreConfig,
    WithInstanceName, WorkerApiConfig,
};
use nativelink_scheduler::awaited_action_db::CLIENT_KEEPALIVE_DURATION;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::config::NativeLinkConfig;
use crate::errors::NativeLinkConfigError;
use crate::properties::{ACTION_STORE, CONTENT_STORE, WORKER_ACTION_STORE, WORKER_CONTENT_STORE};

const SCHEDULER_NAME: &str = "MAIN_SCHEDULER";

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
    let client_action_timeout = WholeSeconds::try_new(spec.client_action_timeout)
        .map_err(|_| NativeLinkConfigError::Duration {
            field: "client_action_timeout",
            requirement: "must be a whole number of seconds, at least 1s and at most about 584 years",
        })?
        .as_units();
    if client_action_timeout <= CLIENT_KEEPALIVE_DURATION.as_secs() {
        return Err(NativeLinkConfigError::Duration {
            field: "client_action_timeout",
            requirement: "must be strictly greater than the native client keepalive duration",
        });
    }
    let mut simple = json!({
        "supported_platform_properties": supported_platform_properties(spec.fleets),
        "client_action_timeout_s": client_action_timeout,
        "worker_timeout_s": config.worker_timeout.as_units(),
        "retain_completed_for_s": spec.actions.retain_completed_for.as_units(),
        "max_job_retries": spec.actions.max_job_retries,
        "max_worker_loss_retries": spec.actions.max_worker_loss_retries,
    });
    simple["resource_escalation"] = resource_escalation(&spec.actions, spec.fleets);
    let mut historical = json!({
        "learn_from_workers": true,
        "cpu_property_name": property::CPU_MILLICORES,
        "headroom_percent": spec.actions.reservation_headroom_percent,
        "window_days": spec.actions.history_window.as_units(),
        "memory_budget_bytes": spec.actions.history_memory_budget.get().get(),
        "scheduler": { "simple": simple },
    });
    if let Some(cold) = cold_start(&spec.actions.cold_start) {
        historical["cold_start"] = cold;
    }
    deserialize(json!({
        "name": SCHEDULER_NAME,
        "historical_resource": historical,
    }))
}

/// Requeue resource kills against the declared capacities of each fleet.
/// CPU count stays a catalog admission constraint; only its millicore ledger is a native minimum.
fn resource_escalation(actions: &ActionsConfig, fleets: &FleetCatalog) -> Value {
    let capacities: Vec<Value> = fleets
        .fleets()
        .iter()
        .map(|fleet| {
            let minimum: Map<String, Value> = fleets
                .declared()
                .iter()
                .filter_map(|&(key, kind)| {
                    (kind == MatchKind::Minimum && key != property::CPU_COUNT)
                        .then(|| {
                            fleet.capacity(key).map(|limit| {
                                (
                                    key.to_owned(),
                                    json!(scheduler_capacity(key, limit, actions)),
                                )
                            })
                        })
                        .flatten()
                })
                .collect();
            // Preserve exact environment and capability identity when selecting a ceiling.
            let exact: Map<String, Value> = fleets
                .declared()
                .iter()
                .filter_map(|&(key, kind)| {
                    (kind == MatchKind::Exact || kind == MatchKind::Capability)
                        .then(|| fleet.get(key).map(|value| (key.to_owned(), json!(value))))
                        .flatten()
                })
                .collect();
            let action_ceilings = action_ceilings(fleet);
            json!({ "minimum": minimum, "exact": exact, "action_ceilings": action_ceilings })
        })
        .collect();
    let mut escalation = json!({
        "memory": {
            "property": property::MEMORY_KB,
            "percent": actions.memory.escalation_percent.get(),
            "max_steps": actions.memory.max_steps,
        },
        "headroom_percent": actions.reservation_headroom_percent,
        "cpu_property": property::CPU_MILLICORES,
        "capacities": capacities,
    });
    if actions.disk.enforcement == DiskEnforcement::Pressure {
        escalation["disk"] = json!({
            "property": property::DISK_KB,
            "percent": actions.disk.escalation_percent.get(),
            "max_steps": actions.disk.max_steps,
        });
    }
    escalation
}

/// CPU is an aggregate scheduling budget; other capacities remain physical.
fn scheduler_capacity(key: &str, capacity: u64, actions: &ActionsConfig) -> u64 {
    if key == property::CPU_MILLICORES {
        capacity.saturating_mul(u64::from(actions.cpu_oversubscription.get()))
    } else {
        capacity
    }
}

/// Keep the effective physical CPU limit per action even when the aggregate budget is larger.
fn action_ceilings(properties: &FleetProperties) -> Map<String, Value> {
    let mut ceilings: Map<String, Value> = properties
        .action_ceilings()
        .map(|(key, limit)| (key.to_owned(), json!(limit)))
        .collect();
    if let Some(limit) = properties.limit(property::CPU_MILLICORES) {
        ceilings.insert(property::CPU_MILLICORES.to_owned(), json!(limit));
    }
    ceilings
}

/// The scheduler's cold start: the reservation of an action no hint matches and the client left
/// untagged. Absent when cold start is disabled, so such actions reserve nothing.
fn cold_start(tuning: &ColdStartTuning) -> Option<Value> {
    tuning.enabled.then(|| {
        json!({
            "cpu_count": u64::from(tuning.cpu_millicores.get()),
            "memory_kb": tuning.memory.as_units(),
            "disk_kb": tuning.disk.as_units(),
        })
    })
}

/// The platform properties the scheduler declares. CPU count is admitted by the catalog but
/// ignored here because its reservation is represented by millicores.
fn supported_platform_properties(fleets: &FleetCatalog) -> Map<String, Value> {
    fleets
        .declared()
        .iter()
        .map(|&(key, kind)| {
            let kind = if key == property::CPU_COUNT {
                MatchKind::Ignored
            } else {
                kind
            };
            (key.to_owned(), json!(property_type(kind)))
        })
        .collect()
}

/// The `NativeLink` property type that matches a key of `kind`.
const fn property_type(kind: MatchKind) -> &'static str {
    match kind {
        MatchKind::Exact | MatchKind::Capability => "exact",
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
    let name = SCHEDULER_NAME;
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
            scheduler: SCHEDULER_NAME.to_owned(),
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

/// The most connections one worker opens to the controller's worker listener: one for its worker
/// API and, under [`StoreAccess::ViaController`], one for each of its content and action stores.
pub(crate) const WORKER_CONNECTIONS: u64 = 3;

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
        .map(|(key, value)| {
            if key == property::CPU_MILLICORES
                && let Some(capacity) = spec.properties.capacity(key)
            {
                let budget = scheduler_capacity(key, capacity, &spec.actions).to_string();
                return (key.to_owned(), json!({ "values": [budget] }));
            }
            (key.to_owned(), json!({ "values": [value] }))
        })
        .collect();
    let action_ceilings = action_ceilings(&spec.properties);
    let worker = json!({
        "name": spec.instance.worker_name(),
        "worker_api_endpoint": api_endpoint,
        "max_inflight_tasks": u64::from(matches!(spec.lifetime, WorkerLifetime::SingleUse)),
        "single_use": matches!(spec.lifetime, WorkerLifetime::SingleUse),
        "cas_fast_slow_store": WORKER_CONTENT_STORE,
        "upload_action_result": { "ac_store": WORKER_ACTION_STORE },
        "work_directory": path_str(&spec.work_directory)?,
        "entrypoint": path_str(&spec.entrypoint)?,
        "enforced_environment": {
            (NETWORK_ENV): { "property": property::NETWORK },
            (CPU_COUNT_ENV): { "property": property::CPU_COUNT },
            (MEMORY_KB_ENV): { "property": property::MEMORY_KB },
            (SIDE_CHANNEL_FILE_ENV): "side_channel_file",
        },
        "platform_properties": platform_properties,
        "action_ceilings": action_ceilings,
        "max_action_timeout_s": spec.actions.timeout.as_units(),
        "resource_enforcement": resource_enforcement(spec)?,
    });
    Ok(worker)
}

/// The worker's resource policy, including settings for inactive enforcement modes.
fn resource_enforcement(spec: &WorkerSpec) -> Result<Value, NativeLinkConfigError> {
    let memory = &spec.actions.memory;
    let disk = &spec.actions.disk;
    let pool = spec
        .memory_pool
        .as_ref()
        .map(|pool| path_str(&pool.cgroup))
        .transpose()?;
    Ok(json!({
        "memory": memory.enforcement,
        "memory_property_name": property::MEMORY_KB,
        "memory_headroom_percent": memory.headroom_percent,
        "memory_soft_samples": memory.soft_samples,
        "memory_soft_spike_percent": memory.soft_spike_percent.get(),
        "memory_sample_interval_ms": memory.sample_interval.as_millis(),
        "memory_pressure_stall_percent": memory.pressure_stall_percent,
        "memory_pressure_poll_interval_ms": memory.pressure_poll_interval.as_millis(),
        "memory_pressure_kill_after_ms": memory.pressure_kill_after.as_millis(),
        "memory_pressure_reserve_bytes": memory.reserve.get().get(),
        "memory_pool_path": pool,
        "disk": disk.enforcement,
        "disk_property_name": property::DISK_KB,
        "disk_headroom_percent": disk.headroom_percent,
        "disk_soft_samples": disk.soft_samples,
        "disk_sample_interval_ms": disk.sample_interval.as_millis(),
        "disk_pressure_free_percent": disk.pressure_free_percent,
        "disk_pressure_poll_interval_ms": disk.pressure_poll_interval.as_millis(),
        "disk_pressure_kill_after_ms": disk.pressure_kill_after.as_millis(),
    }))
}

/// The `ByteStream` service entry, with how long a write survives a client disconnect.
fn bytestream(http2: &Http2Config) -> WithInstanceName<ByteStreamConfig> {
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

    use gang_spi::{ClientTls, ControllerSpec, RenderedStoreSpec, StoreAccess, WorkerSpec};
    use gang_types::{Bytes, FleetCatalog, FleetName, FleetProperties, InstanceId, Isa, Resources};
    use nativelink_config::cas_server::ResourceEnforcementConfig;
    use nativelink_config::schedulers::{HistoricalResourceSpec, SchedulerSpec, SimpleSpec};
    use nativelink_config::stores::{ClientTlsConfig, StoreSpec};
    use serde_json::json;

    use super::*;
    use crate::config::NativeLinkConfig;

    fn config() -> NativeLinkConfig {
        NativeLinkConfig::default()
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
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 4,
                memory: Bytes::new(8 * 1024 * 1024 * 1024),
                disk: Bytes::new(100 << 30),
            },
            &gang_types::FleetActionPolicy::default(),
            [("capacity", capacity.to_owned())],
        )
    }

    fn catalog() -> FleetCatalog {
        FleetCatalog::try_new(vec![fleet("fleet-a", Isa::X86_64, "2")]).expect("valid catalog")
    }

    fn observations() -> &'static gang_spi::ObservationSink {
        static SINK: std::sync::LazyLock<gang_spi::ObservationSink> =
            std::sync::LazyLock::new(|| {
                gang_spi::ObservationSink::channel(
                    std::num::NonZeroUsize::MIN,
                    std::num::NonZeroUsize::MIN,
                )
                .0
            });
        &SINK
    }

    fn controller_spec<'a>(
        content: &'a RenderedStoreSpec,
        action: &'a RenderedStoreSpec,
        fleets: &'a FleetCatalog,
    ) -> ControllerSpec<'a> {
        ControllerSpec {
            actions: std::sync::Arc::new(ActionsConfig::default()),
            scaling: std::sync::Arc::new(gang_config::ScalingConfig::default()),
            clock: std::sync::Arc::new(gang_clock::SystemClock::new()),
            control_acquire_timeout: gang_types::NonZeroDuration::from_secs(1),
            content_store: content,
            action_store: action,
            fleets,
            max_workers: 8,
            observations: observations(),
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
            actions: std::sync::Arc::new(ActionsConfig::default()),
            memory_pool: None,
            instance: instance.clone(),
            properties: properties.clone(),
            lifetime: WorkerLifetime::LongLived,
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
        assert_eq!(historical.cpu_property_name, property::CPU_MILLICORES);
        assert_eq!(
            historical
                .cold_start
                .as_ref()
                .expect("cold start")
                .cpu_count,
            250
        );
        let SchedulerSpec::Simple(simple) = historical.scheduler.as_ref() else {
            panic!(
                "expected nested simple scheduler, got {:?}",
                historical.scheduler
            );
        };
        assert_eq!(simple.client_action_timeout_s, 600);
        assert!(simple.supported_platform_properties.is_some());
    }

    #[test]
    fn fifteen_vcpu_fleet_advertises_millicores_as_minimum() {
        let fleet = FleetProperties::new(
            &FleetName::try_new("fleet-a").expect("valid fleet name"),
            Isa::X86_64,
            &"docker",
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 15,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
            },
            &gang_types::FleetActionPolicy::default(),
            [],
        );
        assert_eq!(
            fleet
                .worker_properties()
                .find(|(key, _)| *key == property::CPU_MILLICORES),
            Some((property::CPU_MILLICORES, "15000"))
        );
        let fleets = FleetCatalog::try_new(vec![fleet]).expect("valid catalog");
        assert_eq!(
            supported_platform_properties(&fleets).get(property::CPU_MILLICORES),
            Some(&json!("minimum"))
        );
        assert_eq!(
            supported_platform_properties(&fleets).get(property::CPU_COUNT),
            Some(&json!("ignore"))
        );
        let escalation = resource_escalation(&ActionsConfig::default(), &fleets);
        let minimum = escalation["capacities"][0]["minimum"]
            .as_object()
            .expect("capacity minimum map");
        assert!(!minimum.contains_key(property::CPU_COUNT));
        assert_eq!(minimum.get(property::CPU_MILLICORES), Some(&json!(30000)));
        assert_eq!(
            escalation["capacities"][0]["action_ceilings"][property::CPU_MILLICORES],
            json!(15000)
        );
    }

    #[test]
    fn client_action_timeout_must_be_whole_seconds() {
        let content = memory_store();
        let action = memory_store();
        let fleets = catalog();
        for timeout in [
            Duration::ZERO,
            Duration::from_millis(1500),
            Duration::from_secs(1),
            CLIENT_KEEPALIVE_DURATION,
        ] {
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

        let timeout = CLIENT_KEEPALIVE_DURATION + Duration::from_secs(1);
        let spec = ControllerSpec {
            client_action_timeout: timeout,
            ..controller_spec(&content, &action, &fleets)
        };
        let scheduler = scheduler(&config(), &spec).expect("scheduler");
        let SchedulerSpec::HistoricalResource(historical) = &scheduler.spec else {
            panic!("expected historical_resource, got {:?}", scheduler.spec);
        };
        let SchedulerSpec::Simple(simple) = historical.scheduler.as_ref() else {
            panic!(
                "expected nested simple scheduler, got {:?}",
                historical.scheduler
            );
        };
        assert_eq!(simple.client_action_timeout_s, timeout.as_secs());
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
    fn worker_renders_side_channel_environment_source() {
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
        let rendered = local_worker(&config(), &spec, "grpc://127.0.0.1:9100").expect("worker");
        assert_eq!(
            rendered["enforced_environment"][SIDE_CHANNEL_FILE_ENV],
            "side_channel_file"
        );
    }

    #[test]
    fn cpu_budget_and_physical_action_ceiling_render_to_native_configs() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let fleets = FleetCatalog::try_new(vec![properties.clone()]).expect("catalog");

        for factor in [2, 3] {
            let mut actions = ActionsConfig::default();
            assert_eq!(actions.cpu_oversubscription.get(), 2);
            actions.cpu_oversubscription = NonZeroU32::new(factor).expect("nonzero factor");
            let mut worker_spec = worker_spec(
                &instance,
                &properties,
                &content,
                &action,
                StoreAccess::Direct,
            );
            worker_spec.actions = std::sync::Arc::new(actions.clone());
            let native = worker(&config(), &endpoint(), &worker_spec, WorkerTls::Plain)
                .expect("native worker")
                .worker;
            let rendered = serde_json::to_value(&native).expect("serialize native worker");
            assert_eq!(
                rendered["platform_properties"][property::CPU_MILLICORES]["values"],
                json!([(4000 * factor).to_string()])
            );
            assert_eq!(
                rendered["platform_properties"][property::CPU_COUNT]["values"],
                json!(["4"])
            );
            assert_eq!(
                rendered["platform_properties"][property::MEMORY_KB]["values"],
                json!(["8388608"])
            );
            assert_eq!(
                rendered["platform_properties"][property::DISK_KB]["values"],
                json!(["104857600"])
            );
            assert_eq!(
                rendered["action_ceilings"][property::CPU_MILLICORES],
                json!(4000)
            );
            assert_eq!(native.max_inflight_tasks, 0);

            let mut controller = controller_spec(&content, &action, &fleets);
            controller.actions = std::sync::Arc::new(actions);
            let scheduler = scheduler(&config(), &controller).expect("native scheduler");
            let escalation = simple_of(&scheduler)
                .resource_escalation
                .as_ref()
                .expect("escalation");
            let capacities = serde_json::to_value(&escalation.capacities).expect("capacities");
            assert_eq!(
                capacities[0]["minimum"][property::CPU_MILLICORES],
                json!(4000 * factor)
            );
            assert!(
                !capacities[0]["minimum"]
                    .as_object()
                    .expect("capacity minimum map")
                    .contains_key(property::CPU_COUNT)
            );
            assert_eq!(
                capacities[0]["minimum"][property::MEMORY_KB],
                json!(8_388_608)
            );
            assert_eq!(
                capacities[0]["minimum"][property::DISK_KB],
                json!(104_857_600)
            );
            assert_eq!(
                capacities[0]["action_ceilings"][property::CPU_MILLICORES],
                json!(4000)
            );
        }
    }

    #[test]
    fn cpu_budget_saturates_without_affecting_other_capacities() {
        let actions = ActionsConfig::default();
        assert_eq!(
            scheduler_capacity(property::CPU_MILLICORES, u64::MAX, &actions),
            u64::MAX
        );
        assert_eq!(
            scheduler_capacity(property::MEMORY_KB, u64::MAX / 2, &actions),
            u64::MAX / 2
        );
    }

    #[test]
    fn action_ceiling_does_not_reduce_physical_worker_capacity() {
        let mut policy = gang_types::FleetActionPolicy::default();
        policy.grants.insert(gang_types::Capability::Network);
        policy.resources.memory = Some(gang_types::NonZeroBytes::gib(4));
        policy.resources.cpu_count = NonZeroU32::new(2);
        let properties = FleetProperties::new(
            &FleetName::try_new("limited").expect("fleet"),
            Isa::X86_64,
            &"docker",
            &"host:test".parse().expect("valid environment"),
            Resources {
                vcpus: 4,
                memory: Bytes::gib(8),
                disk: Bytes::gib(100),
            },
            &policy,
            [],
        );
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let spec = worker_spec(
            &instance,
            &properties,
            &content,
            &action,
            StoreAccess::Direct,
        );
        let rendered = local_worker(&config(), &spec, "grpc://127.0.0.1:9100").expect("worker");
        let native: LocalWorkerConfig =
            serde_json::from_value(rendered.clone()).expect("native worker");
        let native = serde_json::to_value(native).expect("serialize native worker");
        assert_eq!(
            native["action_ceilings"][property::CPU_MILLICORES],
            json!(2000)
        );
        assert_eq!(
            native["platform_properties"][property::CPU_MILLICORES]["values"],
            json!(["8000"])
        );
        assert_eq!(
            rendered["platform_properties"][property::MEMORY_KB]["values"],
            json!(["8388608"])
        );
        assert_eq!(
            rendered["platform_properties"][property::NETWORK]["values"],
            json!(["requested"])
        );
        assert_eq!(
            rendered["action_ceilings"][property::MEMORY_KB],
            json!(4_194_304)
        );
        let fleets = FleetCatalog::try_new(vec![properties]).expect("catalog");
        let capacity = resource_escalation(&ActionsConfig::default(), &fleets);
        assert_eq!(
            capacity["capacities"][0]["minimum"][property::CPU_MILLICORES],
            json!(8000)
        );
        assert_eq!(
            capacity["capacities"][0]["action_ceilings"][property::CPU_MILLICORES],
            json!(2000)
        );
        assert_eq!(
            capacity["capacities"][0]["minimum"][property::MEMORY_KB],
            json!(8_388_608)
        );
        assert_eq!(
            capacity["capacities"][0]["action_ceilings"][property::MEMORY_KB],
            json!(4_194_304)
        );
        assert_eq!(
            capacity["capacities"][0]["exact"][property::NETWORK],
            json!("requested")
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

    /// How many connections a worker with `settings` opens to the controller: its worker API, and
    /// every connection of each store that reaches the controller over gRPC.
    fn controller_dials(settings: &WorkerSettings) -> usize {
        let grpc_connections: usize = settings
            .stores
            .iter()
            .filter_map(|store| match &store.spec {
                StoreSpec::Grpc(grpc) => Some(grpc),
                StoreSpec::FastSlow(fast_slow) => match &fast_slow.slow {
                    StoreSpec::Grpc(grpc) => Some(grpc),
                    _ => None,
                },
                _ => None,
            })
            .map(|grpc| grpc.endpoints.len() * grpc.connections_per_endpoint.max(1))
            .sum();
        1 + grpc_connections
    }

    #[test]
    fn a_worker_dials_the_controller_at_most_worker_connections_times() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let content = memory_store();
        let action = memory_store();
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let most = usize::try_from(WORKER_CONNECTIONS).expect("three fits a usize");
        for (access, dials) in [(StoreAccess::ViaController, most), (StoreAccess::Direct, 1)] {
            let spec = worker_spec(&instance, &properties, &content, &action, access);
            let settings = worker(&config(), &endpoint(), &spec, WorkerTls::Plain).expect("worker");
            assert_eq!(controller_dials(&settings), dials, "{access:?}");
        }
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
        spec.lifetime = WorkerLifetime::SingleUse;
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
        let spec = worker_spec(
            &instance,
            &properties,
            &content,
            &action,
            StoreAccess::Direct,
        );
        let settings = worker(&config(), &endpoint(), &spec, WorkerTls::Plain).expect("worker");
        assert_eq!(settings.worker.max_inflight_tasks, 0);
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

    fn custom_actions() -> ActionsConfig {
        serde_json::from_value(json!({
            "reservation_headroom_percent": 37,
            "history_window": "9d",
            "history_memory_budget": "320MiB",
            "retain_completed_for": "83s",
            "max_job_retries": 5,
            "max_worker_loss_retries": 13,
            "timeout": "23m",
            "kill_grace": "1700ms",
            "memory": {
                "reserve": "2GiB",
                "enforcement": "soft",
                "headroom_percent": 31,
                "soft_samples": 4,
                "soft_spike_percent": 240,
                "sample_interval": "310ms",
                "pressure_stall_percent": 7,
                "pressure_poll_interval": "430ms",
                "pressure_kill_after": "1700ms",
                "escalation_percent": 230,
                "max_steps": 6
            },
            "disk": {
                "enforcement": "soft",
                "headroom_percent": 41,
                "soft_samples": 3,
                "sample_interval": "13s",
                "pressure_free_percent": 12,
                "pressure_poll_interval": "2300ms",
                "pressure_kill_after": "7500ms",
                "escalation_percent": 270,
                "max_steps": 5
            },
            "cold_start": { "enabled": true, "cpu_millicores": 2000, "memory": "2GiB", "disk": "3GiB" }
        }))
        .expect("valid actions")
    }

    #[test]
    fn default_memory_pressure_renders_to_native_worker_config() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let content = memory_store();
        let action = memory_store();
        let spec = worker_spec(
            &instance,
            &properties,
            &content,
            &action,
            StoreAccess::Direct,
        );
        let rendered =
            local_worker(&config(), &spec, "grpc://127.0.0.1:9100").expect("worker json");
        let enforcement = &rendered["resource_enforcement"];
        assert_eq!(enforcement["memory_pressure_stall_percent"], 10);
        assert_eq!(enforcement["memory_pressure_poll_interval_ms"], 250);
        assert_eq!(enforcement["memory_pressure_kill_after_ms"], 5000);
        assert_eq!(
            enforcement["memory_pressure_reserve_bytes"],
            1_073_741_824_u64
        );
        let native: ResourceEnforcementConfig =
            serde_json::from_value(enforcement.clone()).expect("fork accepts enforcement");
        assert_eq!(native.memory_pressure_reserve_bytes, 1_073_741_824);
    }

    #[test]
    fn custom_actions_render_to_native_worker_config() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let content = memory_store();
        let action = memory_store();
        let mut spec = worker_spec(
            &instance,
            &properties,
            &content,
            &action,
            StoreAccess::Direct,
        );
        spec.actions = std::sync::Arc::new(custom_actions());
        spec.memory_pool = Some(gang_spi::ActionMemoryPool {
            cgroup: Path::new("/sys/fs/cgroup/gang/actions").to_path_buf(),
            capacity: gang_types::NonZeroBytes::gib(6),
        });
        let rendered =
            local_worker(&config(), &spec, "grpc://127.0.0.1:9100").expect("worker json");
        let enforcement = &rendered["resource_enforcement"];
        assert_eq!(
            *enforcement,
            json!({
                "memory": "soft",
                "memory_property_name": "memory_kb",
                "memory_headroom_percent": 31,
                "memory_soft_samples": 4,
                "memory_soft_spike_percent": 240,
                "memory_sample_interval_ms": 310,
                "memory_pressure_stall_percent": 7,
                "memory_pressure_poll_interval_ms": 430,
                "memory_pressure_kill_after_ms": 1700,
                "memory_pressure_reserve_bytes": 2_147_483_648_u64,
                "memory_pool_path": "/sys/fs/cgroup/gang/actions",
                "disk": "soft",
                "disk_property_name": "disk_kb",
                "disk_headroom_percent": 41,
                "disk_soft_samples": 3,
                "disk_sample_interval_ms": 13_000,
                "disk_pressure_free_percent": 12,
                "disk_pressure_poll_interval_ms": 2300,
                "disk_pressure_kill_after_ms": 7500
            })
        );
        for removed in [
            "disk_pressure_kill_after_s",
            "memory_pressure_kill_percent",
            "memory_pressure_kill_after_s",
        ] {
            assert!(enforcement.get(removed).is_none());
        }
        let native: ResourceEnforcementConfig =
            serde_json::from_value(enforcement.clone()).expect("fork accepts enforcement");
        assert_eq!(native.memory_pressure_reserve_bytes, 2_147_483_648);
        let native: LocalWorkerConfig =
            serde_json::from_value(rendered).expect("fork accepts worker");
        assert_eq!(native.max_action_timeout_s, 1380);
        assert!(native.resource_enforcement.is_some());
    }

    #[test]
    fn custom_actions_render_to_native_scheduler_config() {
        let content = memory_store();
        let action = memory_store();
        let fleets = FleetCatalog::try_new(vec![
            fleet("fleet-a", Isa::X86_64, "2"),
            FleetProperties::new(
                &FleetName::try_new("fleet-b").expect("fleet name"),
                Isa::Aarch64,
                &"bwrap",
                &"host:test".parse().expect("valid environment"),
                Resources {
                    vcpus: 6,
                    memory: Bytes::gib(16),
                    disk: Bytes::gib(80),
                },
                &gang_types::FleetActionPolicy::default(),
                [
                    ("capacity", "3".to_owned()),
                    ("machine_class", "arm".to_owned()),
                ],
            ),
        ])
        .expect("catalog");
        let mut spec = controller_spec(&content, &action, &fleets);
        let mut actions = custom_actions();
        actions.disk.enforcement = DiskEnforcement::Pressure;
        spec.actions = std::sync::Arc::new(actions);
        let mut config = config();
        config.worker_timeout = WholeSeconds::from_units(17);
        let scheduler = scheduler(&config, &spec).expect("fork accepts scheduler");
        let historical = historical_of(&scheduler);
        assert_eq!(historical.headroom_percent, 37);
        assert_eq!(historical.window_days, 9);
        assert_eq!(historical.memory_budget_bytes, 320 * 1024 * 1024);
        let cold = historical.cold_start.as_ref().expect("cold start");
        assert_eq!(
            (cold.cpu_count, cold.memory_kb, cold.disk_kb),
            (2000, 2_097_152, 3_145_728)
        );
        let simple = simple_of(&scheduler);
        assert_eq!(simple.worker_timeout_s, 17);
        assert_eq!(simple.retain_completed_for_s, 83);
        assert_eq!(simple.max_job_retries, 5);
        assert_eq!(simple.max_worker_loss_retries, 13);
        let escalation = simple
            .resource_escalation
            .as_ref()
            .expect("resource escalation");
        assert_eq!(
            (
                escalation.memory.property.as_str(),
                escalation.memory.percent,
                escalation.memory.max_steps
            ),
            ("memory_kb", 230, 6)
        );
        let disk = escalation.disk.as_ref().expect("disk escalation");
        assert_eq!(
            (disk.property.as_str(), disk.percent, disk.max_steps),
            ("disk_kb", 270, 5)
        );
        assert_eq!(escalation.headroom_percent, 37);
        assert_eq!(escalation.cpu_property, "cpu_millicores");
        assert_eq!(
            serde_json::to_value(&escalation.capacities).expect("capacities"),
            json!([
                {
                    "minimum": { "cpu_millicores": 8000, "memory_kb": 8_388_608, "disk_kb": 104_857_600 },
                    "exact": { "fleet": "fleet-a", "ISA": "x86-64", "sandbox": "docker", "capacity": "2" },
                    "action_ceilings": { "cpu_millicores": 4000 }
                },
                {
                    "minimum": { "cpu_millicores": 12000, "memory_kb": 16_777_216, "disk_kb": 83_886_080 },
                    "exact": { "fleet": "fleet-b", "ISA": "aarch64", "sandbox": "bwrap", "capacity": "3", "machine_class": "arm" },
                    "action_ceilings": { "cpu_millicores": 6000 }
                }
            ])
        );
        let json = serde_json::to_value(simple).expect("simple json");
        assert!(json.get("memory_escalation").is_none());
        let escalation_json = &json["resource_escalation"];
        assert_eq!(escalation_json["capacity_ignored_properties"], json!([]));
        for removed in ["ladder_kb", "max_kb", "max_steps", "disk_property"] {
            assert!(escalation_json.get(removed).is_none());
        }
    }

    #[test]
    fn worker_modes_and_missing_pool_reach_native_config() {
        let instance = InstanceId::try_new("local-1").expect("instance");
        let properties = fleet("fleet-a", Isa::X86_64, "2");
        let content = memory_store();
        let action = memory_store();
        let mut spec = worker_spec(
            &instance,
            &properties,
            &content,
            &action,
            StoreAccess::Direct,
        );
        let mut actions = custom_actions();
        for memory in [
            gang_config::MemoryEnforcement::None,
            gang_config::MemoryEnforcement::Pressure,
            gang_config::MemoryEnforcement::Soft,
        ] {
            for disk in [
                DiskEnforcement::None,
                DiskEnforcement::Guard,
                DiskEnforcement::Pressure,
                DiskEnforcement::Soft,
            ] {
                actions.memory.enforcement = memory;
                actions.disk.enforcement = disk;
                spec.actions = std::sync::Arc::new(actions.clone());
                let rendered = resource_enforcement(&spec).expect("resource enforcement");
                assert!(rendered["memory_pool_path"].is_null());
                let native: ResourceEnforcementConfig =
                    serde_json::from_value(rendered.clone()).expect("fork accepts modes");
                assert_eq!(
                    serde_json::to_value(native.memory).expect("memory mode"),
                    rendered["memory"]
                );
                assert_eq!(
                    serde_json::to_value(native.disk).expect("disk mode"),
                    rendered["disk"]
                );
            }
        }
    }

    #[test]
    fn modes_and_disabled_cold_start_are_rendered() {
        let mut actions = custom_actions();
        for (mode, expected) in [
            (DiskEnforcement::None, false),
            (DiskEnforcement::Guard, false),
            (DiskEnforcement::Pressure, true),
            (DiskEnforcement::Soft, false),
        ] {
            actions.disk.enforcement = mode;
            let json = resource_escalation(&actions, &catalog());
            assert_eq!(json.get("disk").is_some(), expected);
            let native: nativelink_config::schedulers::ResourceEscalationSpec =
                serde_json::from_value(json).expect("fork accepts escalation");
            assert_eq!(native.disk.is_some(), expected);
        }
        actions.cold_start.enabled = false;
        assert!(cold_start(&actions.cold_start).is_none());
    }
}
