//! The embedded worker: `NativeLink`'s local worker, assembled here rather than by
//! `new_local_worker` so that Gang holds the worker's actions manager and can stop the worker
//! with every action it runs ended and cleaned up.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use gang_spi::{WorkerSpec, WorkerTls};
use gang_types::Url;
use nativelink_config::cas_server::LocalWorkerConfig;
use nativelink_error::{Code, Error, ResultExt};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_util::common::fs;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::tls_utils;
use nativelink_worker::local_worker::LocalWorker;
#[cfg(target_os = "linux")]
use nativelink_worker::running_actions_manager::UseNamespaces;
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, PersistentWorkersSettings, ResourceEnforcement, RunningActionsManager,
    RunningActionsManagerArgs, RunningActionsManagerImpl,
};
use nativelink_worker::worker_api_client_wrapper::WorkerApiClientWrapper;
use tokio::sync::broadcast;

use crate::config::{NativeLinkConfig, WorkerTuning};
use crate::errors::NativeLinkEmbedError;
use crate::globals;
use crate::properties::{WORKER_ACTION_STORE, WORKER_CONTENT_STORE};
use crate::settings;
use crate::stores::{build_stores, lookup};

/// How long a failed action cleanup waits before it retries, at most: `NativeLink`'s own default
/// (`DEFAULT_MAX_CLEANUP_BACKOFF`), which it does not export.
const CLEANUP_BACKOFF: Duration = Duration::from_millis(500);

/// How long the worker waits for an HTTP/2 keepalive acknowledgement before it drops the
/// connection: `NativeLink`'s own default (`DEFAULT_HTTP2_KEEPALIVE_TIMEOUT`), which it does not
/// export.
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// The local worker and the actions manager it runs its actions through.
type Worker = LocalWorker<WorkerApiClientWrapper, RunningActionsManagerImpl>;

/// Runs one embedded `NativeLink` local worker until `stop` completes, the worker fails, or a
/// single-use worker has finished its action.
///
/// Once `stop` completes the worker declines every new action, and every running action is
/// killed (its whole process group) and its directory removed before this returns, so the next
/// worker may take over `spec.work_directory` at once.
pub(crate) async fn run(
    config: &NativeLinkConfig,
    endpoint: &Url,
    spec: &WorkerSpec,
    tls: WorkerTls<'_>,
    stop: impl Future<Output = ()>,
) -> Result<(), NativeLinkEmbedError> {
    globals::init(config.max_open_files);
    globals::reap_orphans();
    let mut stop = pin!(stop);
    let (worker, manager) = tokio::select! {
        biased;
        () = &mut stop => return Ok(()),
        built = build(config, endpoint, spec, tls) => built?,
    };
    // The worker declines new actions once it receives the guard; it drops its clone when done.
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<ShutdownGuard>(1);
    let mut running = pin!(worker.run(shutdown_rx));
    tokio::select! {
        biased;
        result = &mut running => return result.map_err(NativeLinkEmbedError::native("worker")),
        () = &mut stop => {}
    }
    // The send fails only once the worker has ended, when it has no action left to decline.
    let _ = shutdown_tx.send(ShutdownGuard::default());
    // The actions end and clean up only while the worker that runs them is polled. A worker that
    // ends first drops its actions, whose cleanup `kill_all` still waits for.
    let mut killed = pin!(manager.kill_all());
    tokio::select! {
        biased;
        () = &mut killed => Ok(()),
        result = running => {
            killed.await;
            result.map_err(NativeLinkEmbedError::native("worker"))
        }
    }
}

/// Builds the worker's stores, a fresh `work_directory`, the actions manager, and the local
/// worker that dials the controller.
async fn build(
    config: &NativeLinkConfig,
    endpoint: &Url,
    spec: &WorkerSpec,
    tls: WorkerTls<'_>,
) -> Result<(Worker, Arc<RunningActionsManagerImpl>), NativeLinkEmbedError> {
    let settings::WorkerSettings { stores, worker } =
        settings::worker(config, endpoint, spec, tls)?;
    let store_manager = build_stores(stores).await?;
    let content = lookup(&store_manager, WORKER_CONTENT_STORE)?;
    let action_cache = lookup(&store_manager, WORKER_ACTION_STORE)?;
    let fast_slow = content
        .downcast_ref::<FastSlowStore>(None)
        .and_then(FastSlowStore::get_arc)
        .err_tip(|| "the worker's content store must be a fast/slow store")
        .map_err(NativeLinkEmbedError::native("worker init"))?;
    fresh_directory(&worker.work_directory)
        .await
        .map_err(NativeLinkEmbedError::native("worker directory"))?;
    let manager = Arc::new(
        RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory: worker.work_directory.clone(),
            execution_configuration: execution(&worker, &config.worker),
            cas_store: fast_slow,
            ac_store: Some(action_cache),
            // Settings never name a historical results store, so it is the content store.
            historical_store: content,
            upload_action_result_config: &worker.upload_action_result,
            max_action_timeout: config.worker.action_timeout.get(),
            max_upload_timeout: config.worker.upload_timeout.get(),
            max_download_timeout: config.worker.download_timeout.get(),
            max_cleanup_wait: config.worker.cleanup_wait.get(),
            max_cleanup_backoff: CLEANUP_BACKOFF,
            timeout_handled_externally: false,
            directory_cache: None,
            active_input_leases: false,
            #[cfg(target_os = "linux")]
            readonly_input_mounts: Vec::new(),
            #[cfg(target_os = "linux")]
            use_namespaces: UseNamespaces::No,
        })
        .map_err(NativeLinkEmbedError::native("worker actions"))?,
    );
    let worker = Arc::new(worker);
    let connection = Connection {
        timeout: config.worker.connect_timeout.get(),
        keepalive: config.worker.keepalive_interval.get(),
    };
    let local = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::clone(&worker),
        Arc::clone(&manager),
        Box::new(move || connect(Arc::clone(&worker), connection)),
        Box::new(|delay| Box::pin(tokio::time::sleep(delay))),
    );
    Ok((local, manager))
}

/// How the worker executes actions: Gang's entrypoint, environment and enforcement, and every
/// other setting as `NativeLink` leaves it unset.
fn execution(worker: &LocalWorkerConfig, tuning: &WorkerTuning) -> ExecutionConfiguration {
    ExecutionConfiguration {
        resource_enforcement: worker
            .resource_enforcement
            .as_ref()
            .and_then(ResourceEnforcement::from_config),
        kill_grace: tuning.kill_grace.get(),
        set_tmpdir: worker.set_tmpdir,
        entrypoint: Some(worker.entrypoint.clone()),
        additional_environment: worker.additional_environment.clone(),
        persistent_workers: PersistentWorkersSettings::default(),
        ..ExecutionConfiguration::default()
    }
}

/// The timeouts of the worker's connection to the controller.
#[derive(Clone, Copy)]
struct Connection {
    /// How long one connection attempt and one request may take.
    timeout: Duration,
    /// The TCP and HTTP/2 keepalive interval.
    keepalive: Duration,
}

/// Dials the controller's worker endpoint with the TLS files the worker's configuration names.
fn connect(
    worker: Arc<LocalWorkerConfig>,
    connection: Connection,
) -> BoxFuture<'static, Result<WorkerApiClientWrapper, Error>> {
    Box::pin(async move {
        let endpoint = &worker.worker_api_endpoint;
        let tls = tls_utils::load_client_config(&endpoint.tls_config)
            .err_tip(|| "loading the worker's TLS files")?;
        let transport = tls_utils::endpoint_from(&endpoint.uri, tls)?
            .connect_timeout(connection.timeout)
            .timeout(connection.timeout)
            .tcp_keepalive(Some(connection.keepalive))
            .http2_keep_alive_interval(connection.keepalive)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect()
            .await
            .map_err(|error| {
                Error::from_std_err(Code::Unavailable, &error)
                    .append(format!("cannot connect to {}", endpoint.uri))
            })?;
        Ok(WorkerApiClient::new(transport).into())
    })
}

/// Empties `directory`, which a previous worker may have left populated, and creates it.
async fn fresh_directory(directory: &str) -> Result<(), Error> {
    if let Ok(path) = fs::canonicalize(directory).await {
        fs::remove_dir_all(&path)
            .await
            .err_tip(|| format!("removing {}", path.display()))?;
    }
    fs::create_dir_all(directory)
        .await
        .err_tip(|| format!("creating {directory}"))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::path::PathBuf;

    use gang_spi::{RenderedStoreSpec, Slots, StoreAccess, WorkerSpec, WorkerTls};
    use gang_types::{
        Bytes, FleetName, FleetProperties, InstanceId, Isa, NonZeroDuration, Resources,
    };
    use serde_json::json;

    use super::*;

    #[test]
    fn execution_uses_configured_kill_grace() {
        let mut config: NativeLinkConfig =
            serde_json::from_value(json!({ "scheduler_name": "MAIN_SCHEDULER" }))
                .expect("valid config");
        config.worker.kill_grace = NonZeroDuration::from_millis(1500);
        let spec = WorkerSpec {
            instance: InstanceId::try_new("local-1").expect("instance"),
            properties: FleetProperties::new(
                &FleetName::try_new("fleet-a").expect("fleet name"),
                Isa::X86_64,
                &"docker",
                Resources {
                    vcpus: 4,
                    memory: Bytes::new(8 * 1024 * 1024 * 1024),
                    disk: Bytes::new(100 << 30),
                },
                [("capacity", "2".to_owned())],
            ),
            slots: Slots::LongLived(NonZeroU32::new(4).expect("non-zero slots")),
            work_directory: PathBuf::from("/var/gang/work"),
            fast_cas_directory: PathBuf::from("/var/gang/cas"),
            fast_cas_budget: Bytes::new(1024),
            content_store: RenderedStoreSpec(json!({ "memory": {} })),
            action_store: RenderedStoreSpec(json!({ "memory": {} })),
            store_access: StoreAccess::ViaController,
            entrypoint: PathBuf::from("/usr/bin/gang"),
        };
        let endpoint = "grpc://127.0.0.1:9100".parse().expect("url");
        let worker_config = settings::worker(&config, &endpoint, &spec, WorkerTls::Plain)
            .expect("worker settings")
            .worker;
        assert_eq!(
            execution(&worker_config, &config.worker).kill_grace,
            config.worker.kill_grace.get()
        );
    }
}
