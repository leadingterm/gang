//! The embedded worker: `NativeLink`'s local worker, assembled here rather than by
//! `new_local_worker` so that Gang holds the worker's actions manager and can stop the worker
//! with every action it runs ended and cleaned up.

use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use gang_clock::SystemClock;
use gang_spi::{WorkerSpec, WorkerTls};
use gang_tls::{Alpn, ClientIdentity, ConnectorSettings, Dialer, ServerTrust, TlsConnector};
use gang_types::{CertificatePem, NonZeroDuration, PrivateKeyPem, Url};
use nativelink_config::cas_server::LocalWorkerConfig;
use nativelink_error::{Code, Error, ResultExt};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_worker::local_worker::LocalWorker;
#[cfg(target_os = "linux")]
use nativelink_worker::running_actions_manager::UseNamespaces;
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, PersistentWorkersSettings, ResourceEnforcement, RunningActionsManager,
    RunningActionsManagerArgs, RunningActionsManagerImpl,
};
use nativelink_worker::worker_api_client_wrapper::WorkerApiClientWrapper;
use tokio::sync::broadcast;
use tonic::transport::Endpoint;

use crate::config::NativeLinkConfig;
use crate::errors::{NativeLinkConfigError, NativeLinkEmbedError};
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
///
/// A worker whose certificate the controller refuses as revoked stops like on `stop` and fails
/// with [`NativeLinkEmbedError::CertificateRevoked`]; it never reconnects.
/// An observed revocation takes precedence over a concurrent stop or clean worker completion.
pub(crate) async fn run(
    config: &NativeLinkConfig,
    endpoint: &Url,
    spec: &WorkerSpec,
    tls: WorkerTls<'_>,
    stop: impl Future<Output = ()>,
) -> Result<(), NativeLinkEmbedError> {
    globals::init(config.max_open_files);
    globals::reap_orphans();
    let dialer = dialer(tls, config.worker.connect_timeout.into())?
        .with_tcp_keepalive(config.worker.keepalive_interval.get());
    let revoked = dialer.revoked().clone();
    let mut stop = pin!(stop);
    let (worker, manager) = tokio::select! {
        biased;
        () = &mut stop => return Ok(()),
        built = build(config, endpoint, spec, tls, dialer) => built?,
    };
    // The worker declines new actions once it receives the guard; it drops its clone when done.
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<ShutdownGuard>(1);
    let mut running = pin!(worker.run(shutdown_rx));
    let revocation = match wait_for_exit(&mut running, &mut stop, revoked.raised()).await {
        WorkerExit::Finished(result) => {
            return result.map_err(NativeLinkEmbedError::native("worker"));
        }
        WorkerExit::Stopped => None,
        WorkerExit::Revoked(revoked) => Some(revoked),
    };
    // The send fails only once the worker has ended, when it has no action left to decline.
    let _ = shutdown_tx.send(ShutdownGuard::default());
    // The actions end and clean up only while the worker that runs them is polled. A worker that
    // ends first drops its actions, whose cleanup `kill_all` still waits for.
    let mut killed = pin!(manager.kill_all());
    tokio::select! {
        biased;
        () = &mut killed => {}
        result = running => {
            killed.await;
            result.map_err(NativeLinkEmbedError::native("worker"))?;
        }
    }
    revocation.map_or(Ok(()), |revoked| {
        Err(NativeLinkEmbedError::CertificateRevoked(revoked))
    })
}

/// Why the running worker ended or was asked to shut down.
enum WorkerExit<R> {
    Finished(Result<(), Error>),
    Stopped,
    Revoked(R),
}

/// Preserves terminal revocation, then worker completion, before an ordinary stop.
async fn wait_for_exit<R>(
    running: impl Future<Output = Result<(), Error>>,
    stop: impl Future<Output = ()>,
    revoked: impl Future<Output = R>,
) -> WorkerExit<R> {
    tokio::select! {
        biased;
        revoked = revoked => WorkerExit::Revoked(revoked),
        result = running => WorkerExit::Finished(result),
        () = stop => WorkerExit::Stopped,
    }
}

/// Builds the worker's stores, a fresh `work_directory`, the actions manager, and the local
/// worker that dials the controller.
async fn build(
    config: &NativeLinkConfig,
    endpoint: &Url,
    spec: &WorkerSpec,
    tls: WorkerTls<'_>,
    dialer: Dialer,
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
            execution_configuration: execution(&worker, spec.actions.kill_grace),
            cas_store: fast_slow,
            ac_store: Some(action_cache),
            // Settings never name a historical results store, so it is the content store.
            historical_store: content,
            upload_action_result_config: &worker.upload_action_result,
            max_action_timeout: spec.actions.timeout.get(),
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
    let endpoint = controller_endpoint(endpoint, tls, connection)?;
    let local = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::clone(&worker),
        Arc::clone(&manager),
        Box::new(move || connect(endpoint.clone(), dialer.clone())),
        Box::new(|delay| Box::pin(tokio::time::sleep(delay))),
    );
    Ok((local, manager))
}

/// How the worker executes actions: Gang's entrypoint, environment, enforcement and one-shot
/// execution, with other settings left at `NativeLink` defaults.
fn execution(
    worker: &LocalWorkerConfig,
    kill_grace: gang_types::NonZeroDuration,
) -> ExecutionConfiguration {
    ExecutionConfiguration {
        resource_enforcement: worker
            .resource_enforcement
            .as_ref()
            .and_then(ResourceEnforcement::from_config),
        kill_grace: kill_grace.get(),
        set_tmpdir: worker.set_tmpdir,
        entrypoint: Some(worker.entrypoint.clone()),
        additional_environment: worker.additional_environment.clone(),
        // The pooled process protocol bypasses Gang's per-action entrypoint; actions run one-shot.
        persistent_workers: PersistentWorkersSettings {
            enabled: false,
            ..PersistentWorkersSettings::default()
        },
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

/// The dialer of the worker's channel to the controller: TCP, then TLS as `tls` says.
fn dialer(
    tls: WorkerTls<'_>,
    handshake_timeout: NonZeroDuration,
) -> Result<Dialer, NativeLinkEmbedError> {
    let (ca, certificate, private_key) = match tls {
        WorkerTls::Plain => return Ok(Dialer::new(None)),
        WorkerTls::ServerOnly { server_ca } => {
            (server_ca.map(read_certificate).transpose()?, None, None)
        }
        WorkerTls::Mutual(files) => (
            Some(read_certificate(files.server_ca)?),
            Some(read_certificate(files.certificate)?),
            Some(read_private_key(files.private_key)?),
        ),
    };
    let trust = ca.as_ref().map_or(ServerTrust::System, ServerTrust::Ca);
    let identity =
        certificate
            .as_ref()
            .zip(private_key.as_ref())
            .map(|(certificate, private_key)| ClientIdentity {
                certificate,
                private_key,
            });
    let connector = TlsConnector::try_new(&ConnectorSettings {
        trust,
        identity,
        handshake_timeout,
        clock: Arc::new(SystemClock::default()),
        alpn: Alpn::H2,
    })
    .map_err(NativeLinkEmbedError::WorkerTls)?;
    Ok(Dialer::new(Some(connector)))
}

/// Reads a worker certificate from its PEM file.
fn read_certificate(path: &Path) -> Result<CertificatePem, NativeLinkEmbedError> {
    let text =
        std::fs::read_to_string(path).map_err(|source| NativeLinkEmbedError::WorkerTlsFile {
            path: path.to_owned(),
            source,
        })?;
    CertificatePem::try_new(text).map_err(|source| NativeLinkEmbedError::WorkerTlsPem {
        path: path.to_owned(),
        source,
    })
}

/// Reads a worker private key from its PEM file.
fn read_private_key(path: &Path) -> Result<PrivateKeyPem, NativeLinkEmbedError> {
    let text =
        std::fs::read_to_string(path).map_err(|source| NativeLinkEmbedError::WorkerTlsFile {
            path: path.to_owned(),
            source,
        })?;
    PrivateKeyPem::try_new(text).map_err(|source| NativeLinkEmbedError::WorkerTlsPem {
        path: path.to_owned(),
        source,
    })
}

/// Builds the controller endpoint. The dialer chooses the transport, so tonic always sees `http`;
/// under TLS the origin is `https://{host}:{port}` so requests carry the `https` scheme.
fn controller_endpoint(
    endpoint: &Url,
    tls: WorkerTls<'_>,
    connection: Connection,
) -> Result<Endpoint, NativeLinkEmbedError> {
    let tls = !matches!(tls, WorkerTls::Plain);
    let host = endpoint
        .host_str()
        .ok_or(NativeLinkConfigError::EndpointHost("worker_endpoint"))?;
    let port = endpoint.port().unwrap_or(if tls { 443 } else { 80 });
    let authority = format!("{host}:{port}");
    let transport = Endpoint::from_shared(format!("http://{authority}")).map_err(|error| {
        NativeLinkEmbedError::native("worker endpoint")(Error::from_std_err(
            Code::InvalidArgument,
            &error,
        ))
    })?;
    let transport = if tls {
        let origin = format!("https://{authority}").parse().map_err(|error| {
            NativeLinkEmbedError::native("worker endpoint")(Error::from_std_err(
                Code::InvalidArgument,
                &error,
            ))
        })?;
        transport.origin(origin)
    } else {
        transport
    };
    Ok(transport
        .connect_timeout(connection.timeout)
        .timeout(connection.timeout)
        .http2_keep_alive_interval(connection.keepalive)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true))
}

/// Dials the controller's worker endpoint.
fn connect(
    endpoint: Endpoint,
    dialer: Dialer,
) -> BoxFuture<'static, Result<WorkerApiClientWrapper, Error>> {
    Box::pin(async move {
        let uri = endpoint.uri().clone();
        let transport = endpoint
            .connect_with_connector(dialer)
            .await
            .map_err(|error| {
                Error::from_std_err(Code::Unavailable, &error)
                    .append(format!("cannot connect to {uri}"))
            })?;
        Ok(WorkerApiClient::new(transport).into())
    })
}

/// Empties the work directory while preserving the side-channel root prepared by the worker.
async fn fresh_directory(directory: &str) -> Result<(), Error> {
    let root = Path::new(directory);
    let side_channels = root.join(".side_channels");
    clear_directory(&side_channels, false)
        .await
        .map_err(|error| Error::from_std_err(Code::Internal, &error))
        .err_tip(|| format!("cleaning {}", side_channels.display()))?;
    clear_directory(root, true)
        .await
        .map_err(|error| Error::from_std_err(Code::Internal, &error))
        .err_tip(|| format!("cleaning {}", root.display()))
}

/// Removes directory contents without following symbolic links.
async fn clear_directory(directory: &Path, preserve_side_channels: bool) -> std::io::Result<()> {
    let metadata = tokio::fs::symlink_metadata(directory).await?;
    if !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected a directory",
        ));
    }
    let mut entries = tokio::fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        if preserve_side_channels && entry.file_name() == ".side_channels" {
            continue;
        }
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(entry.path()).await?;
        } else {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};
    use std::num::NonZeroU32;
    use std::path::PathBuf;

    use gang_spi::{RenderedStoreSpec, Slots, StoreAccess, WorkerSpec, WorkerTls};
    use gang_types::{
        Bytes, FleetName, FleetProperties, InstanceId, Isa, NonZeroDuration, Resources,
    };
    use serde_json::json;

    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn fresh_directory_preserves_private_side_channel_root() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().expect("temporary directory");
        let work = root.path().join("work");
        std::fs::create_dir_all(work.join(".side_channels")).expect("old side-channel root");
        std::fs::set_permissions(
            work.join(".side_channels"),
            std::fs::Permissions::from_mode(0o700),
        )
        .expect("private side-channel root");
        std::fs::write(work.join(".side_channels/stale"), b"stale").expect("stale report");
        std::fs::write(work.join("stale"), b"stale").expect("stale action data");
        fresh_directory(work.to_str().expect("UTF-8 temporary path"))
            .await
            .expect("prepare worker directory");
        assert!(!work.join("stale").exists());
        let side_channels = work.join(".side_channels");
        assert_eq!(
            std::fs::metadata(&side_channels)
                .expect("side-channel root")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::read_dir(side_channels).expect("directory").count(),
            0
        );
    }

    #[tokio::test]
    async fn a_ready_revocation_wins_a_simultaneously_ready_stop() {
        assert!(matches!(
            wait_for_exit(pending::<Result<(), Error>>(), ready(()), ready("revoked")).await,
            WorkerExit::Revoked("revoked")
        ));
    }

    #[tokio::test]
    async fn a_ready_revocation_wins_a_simultaneously_ready_worker_completion() {
        assert!(matches!(
            wait_for_exit(ready(Ok(())), ready(()), ready("revoked")).await,
            WorkerExit::Revoked("revoked")
        ));
    }

    #[tokio::test]
    async fn a_ready_worker_error_wins_a_simultaneously_ready_stop() {
        let error = nativelink_error::make_err!(Code::Internal, "worker failed");
        assert!(matches!(
            wait_for_exit(ready(Err(error)), ready(()), pending::<()>()).await,
            WorkerExit::Finished(Err(_))
        ));
    }

    #[tokio::test]
    async fn a_ready_stop_wins_when_other_futures_are_pending() {
        assert!(matches!(
            wait_for_exit(pending::<Result<(), Error>>(), ready(()), pending::<()>()).await,
            WorkerExit::Stopped
        ));
    }

    #[tokio::test]
    async fn a_ready_worker_completion_wins_when_other_futures_are_pending() {
        assert!(matches!(
            wait_for_exit(ready(Ok(())), pending::<()>(), pending::<()>()).await,
            WorkerExit::Finished(Ok(()))
        ));
    }

    #[test]
    fn worker_tls_files_report_read_and_pem_errors() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("worker.pem");
        assert!(matches!(
            read_certificate(&path),
            Err(NativeLinkEmbedError::WorkerTlsFile { path: failed, .. }) if failed == path
        ));
        std::fs::write(&path, "not a pem").expect("write malformed pem");
        assert!(matches!(
            read_certificate(&path),
            Err(NativeLinkEmbedError::WorkerTlsPem { path: failed, .. }) if failed == path
        ));
        assert!(matches!(
            read_private_key(&path),
            Err(NativeLinkEmbedError::WorkerTlsPem { path: failed, .. }) if failed == path
        ));
    }

    #[test]
    fn controller_endpoint_uses_http_transport_for_tls_and_ipv6() {
        let connection = Connection {
            timeout: Duration::from_secs(2),
            keepalive: Duration::from_secs(10),
        };
        let url = "grpcs://[::1]:9100".parse().expect("url");
        let endpoint =
            controller_endpoint(&url, WorkerTls::ServerOnly { server_ca: None }, connection)
                .expect("endpoint");
        assert_eq!(endpoint.uri().scheme_str(), Some("http"));
        assert_eq!(
            endpoint.uri().authority().map(http::uri::Authority::as_str),
            Some("[::1]:9100")
        );
    }

    #[test]
    fn execution_uses_configured_kill_grace() {
        let config = NativeLinkConfig::default();
        let actions = gang_config::ActionsConfig {
            kill_grace: NonZeroDuration::from_millis(1500),
            ..gang_config::ActionsConfig::default()
        };
        let spec = WorkerSpec {
            actions: Arc::new(actions),
            memory_pool: None,
            instance: InstanceId::try_new("local-1").expect("instance"),
            properties: FleetProperties::new(
                &FleetName::try_new("fleet-a").expect("fleet name"),
                Isa::X86_64,
                &"docker",
                &"host:test".parse().expect("valid environment"),
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
            execution(&worker_config, spec.actions.kill_grace).kill_grace,
            spec.actions.kill_grace.get()
        );
    }
}
