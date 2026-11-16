//! Plugin configuration: dialable endpoints, scheduler name, and tuning.

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use gang_types::{
    BoundedBytes, BoundedDuration, HeadroomPercent, NANOS_PER_SECOND, NonZeroBytes,
    NonZeroDuration, SchedulerName, SchedulerSettings, Url,
};
use serde::{Deserialize, Serialize};

use crate::enforcement::{ColdStartTuning, DiskTuning, MemoryTuning};
use crate::errors::NativeLinkConfigError;
use crate::open_file_limit::OpenFileLimit;

/// A non-zero whole number of seconds, capped at the representable maximum of about 584 years.
pub(crate) type WholeSeconds =
    BoundedDuration<NANOS_PER_SECOND, 1, { u64::MAX / NANOS_PER_SECOND }>;

/// How many bytes a client may send on one HTTP/2 connection, and on any one of its streams,
/// before the controller acknowledges them.
pub type ReceiveWindow = BoundedBytes<1, MIN_RECEIVE_WINDOW, MAX_RECEIVE_WINDOW>;

/// A worker silent for longer than this is dropped from the scheduler.
const DEFAULT_WORKER_TIMEOUT: WholeSeconds = WholeSeconds::from_units(10);
/// How long a completed action stays visible to clients after it finishes.
const DEFAULT_RETAIN_COMPLETED_FOR: WholeSeconds = WholeSeconds::from_units(60);
/// How many times an action is retried after its job fails.
const DEFAULT_MAX_JOB_RETRIES: u32 = 3;
/// How many times an action is retried after the worker running it is lost.
const DEFAULT_MAX_WORKER_LOSS_RETRIES: u32 = 10;
/// Extra reservation over measured usage, in percent.
const DEFAULT_HEADROOM_PERCENT: HeadroomPercent = HeadroomPercent::from_percent(20);
/// How many days a resource measurement counts: seven days.
const DEFAULT_WINDOW_DAYS: NonZeroU32 = NonZeroU32::MIN.saturating_add(6);
/// Longest time one action may run on a worker.
const DEFAULT_ACTION_TIMEOUT: WholeSeconds = WholeSeconds::from_units(20 * 60);
/// Longest time a worker may spend uploading the outputs of one action.
const DEFAULT_UPLOAD_TIMEOUT: WholeSeconds = WholeSeconds::from_units(10 * 60);
/// Longest time a worker may spend downloading the inputs of one action.
const DEFAULT_DOWNLOAD_TIMEOUT: WholeSeconds = WholeSeconds::from_units(10 * 60);
/// Time between a polite and a forced kill of an action's process.
const DEFAULT_KILL_GRACE: NonZeroDuration = NonZeroDuration::from_secs(5);
/// Longest time a worker waits to connect to the controller.
const DEFAULT_CONNECT_TIMEOUT: WholeSeconds = WholeSeconds::from_units(5);
/// Time a worker waits for its last action directory to be cleaned up.
const DEFAULT_CLEANUP_WAIT: WholeSeconds = WholeSeconds::from_units(30);
/// Interval of HTTP/2 keepalive pings sent by a worker.
const DEFAULT_WORKER_KEEPALIVE_INTERVAL: WholeSeconds = WholeSeconds::from_units(30);
/// Largest gRPC message a connection accepts: 4 MiB.
const DEFAULT_MAX_MESSAGE_SIZE: NonZeroBytes = NonZeroBytes::mib(4);
/// How many connections the public listener serves at once: 128 × the default 16 MiB
/// `receive_window` is 2 GiB, within the 4 GiB reference controller.
const DEFAULT_MAX_CONNECTIONS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(127);
/// How many requests one HTTP/2 connection runs at once: 100, as many as Bazel sends on one
/// connection by default (`--remote_max_concurrency_per_connection`).
const DEFAULT_MAX_CONCURRENT_STREAMS: NonZeroU32 = NonZeroU32::MIN.saturating_add(99);
/// Shortest time between two log lines of a listener's refused connections.
const DEFAULT_REFUSAL_LOG_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(60);
/// How long a connection on the public listener that has not earned its place may stay without a
/// request in flight.
const DEFAULT_UNPROVEN_IDLE_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(30);
/// The smallest receive window a server can honour: every HTTP/2 connection starts with a
/// 65535-byte window, which can only grow (RFC 9113, section 6.9.2).
const MIN_RECEIVE_WINDOW: u64 = 65_535;
/// The largest flow-control window HTTP/2 allows (RFC 9113, section 6.9.1).
const MAX_RECEIVE_WINDOW: u64 = (1 << 31) - 1;
/// 16 MiB: HTTP/2 flow control lets one connection 140 ms away upload up to about 114 MiB/s.
const DEFAULT_RECEIVE_WINDOW: ReceiveWindow = ReceiveWindow::from_units(16 * 1024 * 1024);
/// How long a `ByteStream` write survives a client disconnect.
const DEFAULT_STREAM_IDLE_TIMEOUT: WholeSeconds = WholeSeconds::from_units(10);
/// Interval of HTTP/2 keepalive pings sent to clients.
const DEFAULT_HTTP2_KEEPALIVE_INTERVAL: NonZeroDuration = NonZeroDuration::from_secs(30);
/// How long `run_controller` lets open connections finish after both connection streams have ended.
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
/// How long an HTTP/1.1 connection may take to send a request head before it is closed.
const DEFAULT_HEADER_READ_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(30);

/// The key of the worker endpoint in `[scheduler.nativelink]`.
const WORKER_ENDPOINT: &str = "worker_endpoint";

/// The controller's worker endpoint and how the controller is tuned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawNativeLinkConfig")]
pub struct NativeLinkConfig {
    /// The URL workers dial to reach the controller; its scheme is `grpc` or `grpcs`. Omitted
    /// means derived from the bound worker listener before the plugin is built.
    #[serde(default)]
    pub worker_endpoint: Option<Url>,
    /// The scheduler the controller runs.
    pub scheduler_name: SchedulerName,
    /// The process open file limit; `NativeLink` uses it to bound concurrent file access.
    /// Default: 24576.
    #[serde(default)]
    pub max_open_files: OpenFileLimit,
    /// Scheduler behaviour: worker liveness, retention, retries, and resource reservation.
    #[serde(default)]
    pub scheduler: SchedulerTuning,
    /// Worker behaviour: action, transfer, and connection timeouts.
    #[serde(default)]
    pub worker: WorkerTuning,
    /// Memory enforcement of reservations.
    #[serde(default)]
    pub memory: MemoryTuning,
    /// Disk enforcement of reservations.
    #[serde(default)]
    pub disk: DiskTuning,
    /// The reservation of an action whose kind nothing has measured yet, and whether it applies.
    /// `cpu_count` is in cores; learned and cold-start scheduler CPU reservations use millicores.
    #[serde(default)]
    pub cold_start: ColdStartTuning,
    /// HTTP/2 settings of every connection the controller is handed: connection, stream, and
    /// message limits, the receive window, timeouts, keepalives, and the shutdown grace.
    #[serde(default)]
    pub http2: Http2Tuning,
}

/// Deserialized configuration before checking relationships between fields.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNativeLinkConfig {
    #[serde(default)]
    worker_endpoint: Option<Url>,
    scheduler_name: SchedulerName,
    #[serde(default)]
    max_open_files: OpenFileLimit,
    #[serde(default)]
    scheduler: SchedulerTuning,
    #[serde(default)]
    worker: WorkerTuning,
    #[serde(default)]
    memory: MemoryTuning,
    #[serde(default)]
    disk: DiskTuning,
    #[serde(default)]
    cold_start: ColdStartTuning,
    #[serde(default)]
    http2: Http2Tuning,
}

impl TryFrom<RawNativeLinkConfig> for NativeLinkConfig {
    type Error = NativeLinkConfigError;

    fn try_from(raw: RawNativeLinkConfig) -> Result<Self, Self::Error> {
        // Workers send keepalives every half connect_timeout; the scheduler evicts silent workers.
        if raw.worker.connect_timeout.get() / 2 >= raw.scheduler.worker_timeout.get() {
            return Err(NativeLinkConfigError::Duration {
                field: "worker.connect_timeout",
                requirement: "must be shorter than twice scheduler.worker_timeout, or the scheduler evicts workers between keepalives",
            });
        }
        Ok(Self {
            worker_endpoint: raw.worker_endpoint,
            scheduler_name: raw.scheduler_name,
            max_open_files: raw.max_open_files,
            scheduler: raw.scheduler,
            worker: raw.worker,
            memory: raw.memory,
            disk: raw.disk,
            cold_start: raw.cold_start,
            http2: raw.http2,
        })
    }
}

/// Scheduler behaviour. Every field has a default, so an empty section is valid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SchedulerTuning {
    /// A worker silent for longer than this is dropped. Default: `10s`.
    pub worker_timeout: WholeSeconds,
    /// How long a completed action stays visible to clients. Default: `60s`.
    pub retain_completed_for: WholeSeconds,
    /// How many times an action is retried after its job fails. Default: 3.
    pub max_job_retries: u32,
    /// How many times an action is retried after the worker running it is lost. Default: 10.
    pub max_worker_loss_retries: u32,
    /// Extra reservation over measured usage, in percent. Default: 20.
    pub headroom_percent: HeadroomPercent,
    /// How many days a resource measurement counts. Default: 7.
    pub window_days: NonZeroU32,
}

impl Default for SchedulerTuning {
    fn default() -> Self {
        Self {
            worker_timeout: DEFAULT_WORKER_TIMEOUT,
            retain_completed_for: DEFAULT_RETAIN_COMPLETED_FOR,
            max_job_retries: DEFAULT_MAX_JOB_RETRIES,
            max_worker_loss_retries: DEFAULT_MAX_WORKER_LOSS_RETRIES,
            headroom_percent: DEFAULT_HEADROOM_PERCENT,
            window_days: DEFAULT_WINDOW_DAYS,
        }
    }
}

/// Worker behaviour. Every field has a default, so an empty section is valid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerTuning {
    /// Longest time one action may run. Default: `20m`.
    pub action_timeout: WholeSeconds,
    /// Longest time the outputs of one action may take to upload. Default: `10m`.
    pub upload_timeout: WholeSeconds,
    /// Longest time the inputs of one action may take to download. Default: `10m`.
    pub download_timeout: WholeSeconds,
    /// Time between a polite and a forced kill of an action's process. Default: `5s`.
    pub kill_grace: NonZeroDuration,
    /// Longest time a worker waits to connect to the controller, and for each request it sends to
    /// the controller. The worker's keepalive interval is half of it, so it must stay below twice
    /// `scheduler.worker_timeout`. Default: `5s`.
    pub connect_timeout: WholeSeconds,
    /// Time a worker waits for its last action directory to be cleaned up. Default: `30s`.
    pub cleanup_wait: WholeSeconds,
    /// Interval of HTTP/2 keepalive pings from the worker. Default: `30s`.
    pub keepalive_interval: WholeSeconds,
}

impl Default for WorkerTuning {
    fn default() -> Self {
        Self {
            action_timeout: DEFAULT_ACTION_TIMEOUT,
            upload_timeout: DEFAULT_UPLOAD_TIMEOUT,
            download_timeout: DEFAULT_DOWNLOAD_TIMEOUT,
            kill_grace: DEFAULT_KILL_GRACE,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            cleanup_wait: DEFAULT_CLEANUP_WAIT,
            keepalive_interval: DEFAULT_WORKER_KEEPALIVE_INTERVAL,
        }
    }
}

/// HTTP/2 settings of every connection the controller is handed. Every field has a default, so an
/// empty section is valid.
///
/// They bound the request data each listener holds before a handler reads it. An HTTP/2
/// connection holds at most `receive_window` bytes that no handler has read, and an HTTP/1.1
/// connection at most hyper's 408 KiB read buffer, so a listener holds at most its connection
/// limit × `receive_window` unread (2 GiB on the public listener at the defaults). What a handler
/// reads is its own: a gRPC request decodes one message of at most `max_message_size` (plus one
/// 16 KiB HTTP/2 frame) at a time, on at most `max_concurrent_streams` requests per connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Http2Tuning {
    /// Largest gRPC message a connection accepts. Default: `4MiB`.
    pub max_message_size: NonZeroBytes,
    /// How many connections the public listener serves at once. A connection earns its place once
    /// the gate admits an authenticated request on it; at the limit, a new connection replaces the
    /// oldest one that has not, an idle one first, and is closed only when every connection has.
    /// The worker listener serves two generations of every connection of every worker the fleets
    /// can run (the sum of their `max_instances`). Default: `128`.
    pub max_connections: NonZeroUsize,
    /// Shortest time between two log lines of a listener's refused connections; each line counts
    /// the refusals since the previous one and in all. Default: `60s`.
    pub refusal_log_interval: NonZeroDuration,
    /// How long a connection on the public listener that has not earned its place may stay without
    /// a request in flight, before its first request included, before it is closed. Default: `30s`.
    pub unproven_idle_timeout: NonZeroDuration,
    /// How many requests one HTTP/2 connection runs at once; its client queues the rest.
    /// Default: `100`.
    pub max_concurrent_streams: NonZeroU32,
    /// How many bytes a client may send on one connection, and on any one of its streams, before
    /// the controller reads them; between 65535 and 2147483647 bytes. A connection uploads at most
    /// one window per round trip. Default: `16MiB`.
    pub receive_window: ReceiveWindow,
    /// How long a `ByteStream` write survives a client disconnect. Default: `10s`.
    pub stream_idle_timeout: WholeSeconds,
    /// Interval of HTTP/2 keepalive pings sent to clients. Default: `30s`.
    pub keepalive_interval: NonZeroDuration,
    /// How long `run_controller` lets open connections finish after both connection streams have
    /// ended, before it closes them. Default: `30s`.
    #[serde(with = "gang_types::duration")]
    pub shutdown_grace: Duration,
    /// How long an HTTP/1.1 connection may take to send a request head before it is closed. Does
    /// not apply to HTTP/2. Default: `30s`.
    pub header_read_timeout: NonZeroDuration,
}

impl Default for Http2Tuning {
    fn default() -> Self {
        Self {
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            refusal_log_interval: DEFAULT_REFUSAL_LOG_INTERVAL,
            unproven_idle_timeout: DEFAULT_UNPROVEN_IDLE_TIMEOUT,
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            receive_window: DEFAULT_RECEIVE_WINDOW,
            stream_idle_timeout: DEFAULT_STREAM_IDLE_TIMEOUT,
            keepalive_interval: DEFAULT_HTTP2_KEEPALIVE_INTERVAL,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            header_read_timeout: DEFAULT_HEADER_READ_TIMEOUT,
        }
    }
}

impl NativeLinkConfig {
    /// The worker endpoint once it is resolved.
    ///
    /// # Errors
    ///
    /// [`NativeLinkConfigError::WorkerEndpointUnresolved`] when `worker_endpoint` was omitted and
    /// not derived.
    pub(crate) fn resolved_worker_endpoint(&self) -> Result<&Url, NativeLinkConfigError> {
        self.worker_endpoint
            .as_ref()
            .ok_or(NativeLinkConfigError::WorkerEndpointUnresolved)
    }

    /// Checks the endpoint, including one supplied after deserialization by the host.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint is not `grpc`/`grpcs` or has no host.
    pub(crate) fn validate(&self) -> Result<(), NativeLinkConfigError> {
        if let Some(endpoint) = &self.worker_endpoint {
            check_endpoint(WORKER_ENDPOINT, endpoint)?;
        }

        Ok(())
    }
}

impl SchedulerSettings for NativeLinkConfig {
    fn worker_endpoint(&self) -> Option<&Url> {
        self.worker_endpoint.as_ref()
    }

    fn set_worker_endpoint(&mut self, endpoint: Option<Url>) {
        self.worker_endpoint = endpoint;
    }

    fn worker_endpoint_key(&self) -> Option<&'static str> {
        Some(WORKER_ENDPOINT)
    }
}

fn check_endpoint(endpoint: &'static str, url: &Url) -> Result<(), NativeLinkConfigError> {
    if !matches!(url.scheme(), "grpc" | "grpcs") {
        return Err(NativeLinkConfigError::EndpointScheme {
            endpoint,
            scheme: url.scheme().to_owned(),
        });
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(NativeLinkConfigError::EndpointHost(endpoint));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::enforcement::{DiskEnforcement, EscalationPercent, MemoryEnforcement, WholeKib};

    fn base() -> Value {
        json!({
            "worker_endpoint": "grpc://localhost:9002",
            "scheduler_name": "MAIN",
        })
    }

    fn parse(value: Value) -> Result<NativeLinkConfig, serde_json::Error> {
        serde_json::from_value(value)
    }

    #[test]
    fn empty_sections_take_the_defaults_and_round_trip() {
        let config = parse(base()).expect("valid config");
        assert_eq!(config.max_open_files.get(), 24_576);
        assert_eq!(config.http2.max_connections.get(), 128);
        assert_eq!(config.http2.max_concurrent_streams.get(), 100);
        assert_eq!(config.http2.receive_window.as_units(), 16 * 1024 * 1024);
        assert_eq!(
            config.http2.refusal_log_interval,
            NonZeroDuration::from_secs(60)
        );
        assert_eq!(
            config.http2.unproven_idle_timeout,
            NonZeroDuration::from_secs(30)
        );
        assert_eq!(config.scheduler, SchedulerTuning::default());
        assert_eq!(config.worker, WorkerTuning::default());
        assert_eq!(config.http2, Http2Tuning::default());
        assert_eq!(config.memory, MemoryTuning::default());
        assert_eq!(config.disk, DiskTuning::default());
        assert_eq!(config.cold_start, ColdStartTuning::default());

        let serialized = serde_json::to_value(&config).expect("serializes");
        assert_eq!(parse(serialized).expect("round trips"), config);
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        let mut top = base();
        top["bogus"] = json!(1);
        assert!(parse(top).is_err());

        let mut section = base();
        section["scheduler"] = json!({ "nope": 1 });
        assert!(parse(section).is_err());
    }

    #[test]
    fn a_scheduler_headroom_beyond_u16_is_rejected() {
        let mut value = base();
        value["scheduler"] = json!({ "headroom_percent": 65536 });
        assert!(parse(value).is_err());

        let mut value = base();
        value["scheduler"] = json!({ "headroom_percent": 65535 });
        let config = parse(value).expect("valid config");
        assert_eq!(
            config.scheduler.headroom_percent,
            HeadroomPercent::from_percent(65535)
        );
    }

    #[test]
    fn listen_addresses_and_handshake_timeout_are_not_configurable() {
        let mut listen = base();
        listen["client_listen"] = json!("127.0.0.1:9000");
        assert!(parse(listen).is_err());

        let mut handshake = base();
        handshake["http2"] = json!({ "handshake_timeout": "10s" });
        assert!(parse(handshake).is_err());
    }

    #[test]
    fn durations_and_bytes_parse_from_strings() {
        let mut value = base();
        value["scheduler"] = json!({ "worker_timeout": "30s" });
        value["http2"] = json!({
            "max_message_size": "8MiB",
            "receive_window": "8MiB",
            "max_connections": 2,
            "max_concurrent_streams": 3,
            "refusal_log_interval": "5s",
            "unproven_idle_timeout": "7s"
        });
        let config = parse(value).expect("valid config");
        assert_eq!(
            config.scheduler.worker_timeout,
            WholeSeconds::from_units(30)
        );
        assert_eq!(config.http2.max_message_size, NonZeroBytes::mib(8));
        assert_eq!(config.http2.receive_window.as_units(), 8 * 1024 * 1024);
        assert_eq!(config.http2.max_connections.get(), 2);
        assert_eq!(
            config.http2.refusal_log_interval,
            NonZeroDuration::from_secs(5)
        );
        assert_eq!(
            config.http2.unproven_idle_timeout,
            NonZeroDuration::from_secs(7)
        );
        assert_eq!(config.http2.max_concurrent_streams.get(), 3);
    }

    #[test]
    fn whole_second_tunables_enforce_bounds_and_precision() {
        let max = u64::MAX / NANOS_PER_SECOND;
        for text in ["1s".to_owned(), format!("{max}s")] {
            let mut value = base();
            value["worker"] = json!({ "action_timeout": text });
            assert!(parse(value).is_ok());
        }
        for text in [
            "0s".to_owned(),
            "1500ms".to_owned(),
            format!("{}s", max + 1),
        ] {
            let mut value = base();
            value["worker"] = json!({ "action_timeout": text });
            assert!(parse(value).is_err());
        }
    }

    #[test]
    fn invalid_limits_are_rejected_during_parsing() {
        let mut value = base();
        value["scheduler"] = json!({ "window_days": 0 });
        assert!(parse(value).is_err());

        for limit in [0, 4, 7, (1 << 30) + 1] {
            let mut value = base();
            value["max_open_files"] = json!(limit);
            assert!(parse(value).is_err(), "limit {limit}");
        }
        for limit in [8, 1 << 30] {
            let mut value = base();
            value["max_open_files"] = json!(limit);
            assert!(parse(value).is_ok(), "limit {limit}");
        }

        let mut value = base();
        value["http2"] = json!({ "max_message_size": "0B" });
        assert!(parse(value).is_err());

        for limit in ["max_connections", "max_concurrent_streams"] {
            let mut value = base();
            value["http2"] = json!({ limit: 0 });
            assert!(parse(value).is_err(), "{limit}");
        }

        for window in ["65534B", "2GiB"] {
            let mut value = base();
            value["http2"] = json!({ "receive_window": window });
            assert!(parse(value).is_err(), "window {window}");
        }
        for window in ["65535B", "2147483647B"] {
            let mut value = base();
            value["http2"] = json!({ "receive_window": window });
            assert!(parse(value).is_ok(), "window {window}");
        }
    }

    #[test]
    fn endpoint_schemes_are_checked() {
        let mut value = base();
        value["worker_endpoint"] = json!("http://localhost:9002");
        let config = parse(value).expect("valid config");
        assert!(matches!(
            config.validate(),
            Err(NativeLinkConfigError::EndpointScheme {
                endpoint: "worker_endpoint",
                ..
            })
        ));

        let mut value = base();
        value["worker_endpoint"] = json!("grpcs://localhost:9002");
        let config = parse(value).expect("valid config");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn endpoints_without_a_host_are_rejected() {
        let mut value = base();
        value["worker_endpoint"] = json!("grpc:opaque");
        let config = parse(value).expect("valid config");
        assert!(matches!(
            config.validate(),
            Err(NativeLinkConfigError::EndpointHost("worker_endpoint"))
        ));
    }

    #[test]
    fn omitted_worker_endpoint_is_none_and_validates() {
        let mut value = base();
        value
            .as_object_mut()
            .expect("object")
            .remove("worker_endpoint");
        let config = parse(value).expect("valid config");
        assert_eq!(config.worker_endpoint, None);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn with_worker_endpoint_fills_only_an_omitted_endpoint() {
        let mut value = base();
        value
            .as_object_mut()
            .expect("object")
            .remove("worker_endpoint");
        let derived: Url = "grpc://127.0.0.1:9100".parse().expect("url");
        let explicit: Url = "grpc://127.0.0.1:9200".parse().expect("url");
        let filled = parse(value)
            .expect("valid config")
            .with_worker_endpoint(derived.clone());
        assert_eq!(filled.worker_endpoint.as_ref(), Some(&derived));
        let kept = filled.with_worker_endpoint(explicit);
        assert_eq!(kept.worker_endpoint, Some(derived));
    }

    #[test]
    fn unresolved_worker_endpoint_is_a_typed_error() {
        let mut value = base();
        value
            .as_object_mut()
            .expect("object")
            .remove("worker_endpoint");
        let config = parse(value).expect("valid config");
        assert!(matches!(
            config.resolved_worker_endpoint(),
            Err(NativeLinkConfigError::WorkerEndpointUnresolved)
        ));
    }

    #[test]
    fn worker_endpoint_key_names_the_worker_endpoint_field() {
        let config = parse(base()).expect("valid config");
        assert_eq!(
            SchedulerSettings::worker_endpoint_key(&config),
            Some("worker_endpoint")
        );
    }

    #[test]
    fn scheduler_names_are_validated() {
        assert!(SchedulerName::try_new("MAIN_scheduler-1").is_ok());
        assert!(SchedulerName::try_new("").is_err());
        assert!(SchedulerName::try_new("a/b").is_err());
        assert!(serde_json::from_value::<SchedulerName>(json!("a b")).is_err());
        assert_eq!(
            serde_json::from_value::<SchedulerName>(json!("MAIN"))
                .expect("valid name")
                .as_str(),
            "MAIN"
        );
    }

    fn validated(edit: impl FnOnce(&mut Value)) -> Result<(), NativeLinkConfigError> {
        let mut value = base();
        edit(&mut value);
        parse(value).expect("valid JSON").validate()
    }

    #[test]
    fn default_durations_pass_validation() {
        assert!(validated(|_| {}).is_ok());
    }

    #[test]
    fn shutdown_grace_parses_from_a_string() {
        let config = parse(base()).expect("valid config");
        assert_eq!(config.http2.shutdown_grace, Duration::from_secs(30));

        let mut value = base();
        value["http2"] = json!({ "shutdown_grace": "5s" });
        let config = parse(value).expect("valid config");
        assert_eq!(config.http2.shutdown_grace, Duration::from_secs(5));
    }

    #[test]
    fn whole_second_settings_reject_zero_and_fractional_durations() {
        let fields = [
            ("scheduler", "worker_timeout"),
            ("scheduler", "retain_completed_for"),
            ("worker", "action_timeout"),
            ("worker", "upload_timeout"),
            ("worker", "download_timeout"),
            ("worker", "cleanup_wait"),
            ("worker", "connect_timeout"),
            ("worker", "keepalive_interval"),
            ("http2", "stream_idle_timeout"),
        ];
        for (section, key) in fields {
            for bad in ["0s", "500ms", "1500ms"] {
                let mut value = base();
                value[section] = json!({ (key): bad });
                assert!(
                    parse(value).is_err(),
                    "{section}.{key} = {bad} must be rejected"
                );
            }
        }
    }

    #[test]
    fn whole_second_settings_accept_whole_seconds() {
        let mut value = base();
        value["scheduler"] = json!({ "worker_timeout": "10s" });
        let config = parse(value).expect("valid config");
        assert_eq!(
            config.scheduler.worker_timeout,
            WholeSeconds::from_units(10)
        );
    }

    #[test]
    fn kill_grace_parses_as_nonzero_duration() {
        let with_kill_grace = |text: &str| {
            let mut value = base();
            value["worker"] = json!({ "kill_grace": text });
            parse(value)
        };
        assert_eq!(
            with_kill_grace("1500ms")
                .expect("valid config")
                .worker
                .kill_grace,
            NonZeroDuration::from_millis(1500)
        );
        assert!(with_kill_grace("0ms").is_err());
    }

    #[test]
    fn http2_non_zero_intervals_reject_zero() {
        for key in [
            "keepalive_interval",
            "header_read_timeout",
            "refusal_log_interval",
            "unproven_idle_timeout",
        ] {
            let mut value = base();
            value["http2"] = json!({ (key): "0s" });
            assert!(parse(value).is_err(), "http2.{key} = 0s must be rejected");
        }
    }

    #[test]
    fn http2_header_read_timeout_parses_from_a_string() {
        let mut value = base();
        value["http2"] = json!({ "header_read_timeout": "5s" });
        let config = parse(value).expect("valid config");
        assert_eq!(
            config.http2.header_read_timeout.get(),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn connect_timeout_must_stay_below_twice_the_worker_timeout() {
        let with_connect_timeout = |connect: &str| {
            let mut value = base();
            value["scheduler"] = json!({ "worker_timeout": "10s" });
            value["worker"] = json!({ "connect_timeout": connect });
            parse(value)
        };
        assert!(with_connect_timeout("19s").is_ok());
        assert!(with_connect_timeout("20s").is_err());
    }

    #[test]
    fn memory_enforcement_parses_from_snake_case_strings() {
        let mut value = base();
        value["memory"] = json!({ "enforcement": "none" });
        let config = parse(value).expect("valid config");
        assert_eq!(config.memory.enforcement, MemoryEnforcement::None);

        let mut value = base();
        value["memory"] = json!({ "enforcement": "soft" });
        let config = parse(value).expect("valid config");
        assert_eq!(config.memory.enforcement, MemoryEnforcement::Soft);
    }

    #[test]
    fn memory_tuning_parses_all_fields() {
        let mut value = base();
        value["memory"] = json!({
            "enforcement": "soft",
            "min_reservation": "64MiB",
            "headroom_percent": 30,
            "escalation_percent": 150,
            "escalation_max_steps": 4,
        });
        let config = parse(value).expect("valid config");
        assert_eq!(config.memory.enforcement, MemoryEnforcement::Soft);
        assert_eq!(config.memory.min_reservation.as_units(), 65_536);
        assert_eq!(config.memory.headroom_percent.get(), 30);
        assert_eq!(config.memory.escalation_percent.get(), 150);
        assert_eq!(config.memory.escalation_max_steps, 4);
    }

    #[test]
    fn unknown_memory_fields_are_rejected() {
        let mut value = base();
        value["memory"] = json!({ "disk": "soft" });
        assert!(parse(value).is_err());
    }

    #[test]
    fn escalation_percent_must_grow_the_reservation() {
        let with_percent = |percent: u32| {
            let mut value = base();
            value["memory"] = json!({ "escalation_percent": percent });
            parse(value)
        };
        assert!(with_percent(100).is_err());
        assert_eq!(
            with_percent(101)
                .expect("valid config")
                .memory
                .escalation_percent,
            EscalationPercent::try_new(101).expect("greater than 100")
        );
    }

    #[test]
    fn disk_enforcement_parses_from_snake_case_strings() {
        for (text, expected) in [
            ("none", DiskEnforcement::None),
            ("guard", DiskEnforcement::Guard),
            ("soft", DiskEnforcement::Soft),
        ] {
            let mut value = base();
            value["disk"] = json!({ "enforcement": text });
            let config = parse(value).expect("valid config");
            assert_eq!(config.disk.enforcement, expected);
        }
    }

    #[test]
    fn unknown_disk_fields_are_rejected() {
        let mut value = base();
        value["disk"] = json!({ "memory": "soft" });
        assert!(parse(value).is_err());
    }

    #[test]
    fn cold_start_parses_byte_strings() {
        let mut value = base();
        value["cold_start"] = json!({ "cpu_count": 2, "memory": "2GiB", "disk": "1GiB" });
        let config = parse(value).expect("valid config");
        assert!(config.cold_start.enabled);
        assert_eq!(config.cold_start.cpu_count.get(), 2);
        assert_eq!(
            config.cold_start.memory,
            WholeKib::from_units(2 * 1024 * 1024)
        );
        assert_eq!(config.cold_start.disk, WholeKib::from_units(1024 * 1024));
    }

    #[test]
    fn unknown_cold_start_fields_are_rejected() {
        let mut value = base();
        value["cold_start"] = json!({ "gpu": 1 });
        assert!(parse(value).is_err());
    }

    #[test]
    fn cold_start_sizes_parse_only_as_integral_kibibytes() {
        let cold = |value: Value| {
            let mut config = base();
            config["cold_start"] = value;
            parse(config)
        };
        assert!(cold(json!({ "cpu_count": 0 })).is_err());
        for field in ["memory", "disk"] {
            for size in ["0B", "512B", "1500B"] {
                assert!(
                    cold(json!({ field: size })).is_err(),
                    "{field} = {size} must be rejected"
                );
            }
            assert!(cold(json!({ field: "1KiB" })).is_ok(), "{field} = 1KiB");
        }
    }
}
