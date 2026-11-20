use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use gang_clock::SystemClock;
use gang_os::fs::ensure_private_dir;
use gang_spi::{
    ControllerHostIdentity, ControllerHostPort, FleetSupply, Held, HostMilestone, InstanceIdentity,
    MountedScratchVolume, NodeLifecyclePort, Plugin, PluginContext, PortError, PreemptionNotice,
    ProvisionerPort, ScratchVolumePort, ServerEndpoints, Sidecar, WorkloadCredentialPort,
};
use gang_types::{
    Audience, Fleet, FleetName, InstanceId, Isa, Proof, Resources, TokenTtl, WorkerLifetime,
};

use crate::config::{LocalConfig, LocalFleetOptions};
use crate::errors::LocalError;
use crate::fleet::{LaunchSpec, LocalFleet};
use crate::host_identity;
use crate::imds::{ImdsState, MockImds};
use crate::imds_client::ImdsClient;
use crate::jwt::JwtMinter;
use crate::key_file;
use crate::layout::StateLayout;
use crate::processes::{WorkerEndpoints, WorkerProcesses};
use crate::scratch;
use crate::worker_identity::WorkerIdentity;

/// The local provisioner: one plugin that serves the worker fleet, node lifecycle, scratch
/// volumes and workload credentials from files under `state_dir`.
///
/// Building the plugin has no filesystem effect, so it runs on every node that runs `gang`.
/// Each role opens the resources it uses on first use: the state layout on the first
/// filesystem-touching port call, and the mock IMDS state when the provisioner attaches.
///
/// It changes a fleet only through its [`ProvisionerPort`] methods, each under the fence's
/// permission. Controller incarnation may also persist private host identity metadata; it never
/// changes the fleet. There is no unfenced fleet mutation such as recording a Spot interruption:
///
/// ```compile_fail
/// fn interrupt(local: &gang_local::Local, instance: &gang_types::InstanceId) {
///     let _ = local.request_preemption(&instance, gang_clock::Timestamp::from_unix_millis(0));
/// }
/// ```
#[derive(Clone)]
pub struct Local {
    /// Shared filesystem-backed provisioner state.
    state: Arc<LocalState>,
    /// Metadata client used by worker-facing ports.
    imds_client: Arc<ImdsClient>,
}

/// Filesystem-backed state shared with blocking port operations.
struct LocalState {
    /// Paths for local state.
    layout: StateLayout,
    /// Records whether the state directories have been prepared.
    prepared: OnceLock<()>,
    /// Persistent identity location for this configuration's logical controller host.
    controller_host_path: PathBuf,
    /// Identity shared by clones after its first successful disk read or creation.
    controller_host_identity: OnceLock<InstanceId>,
    /// Worker fleet and its process operations.
    fleet: LocalFleet<WorkerProcesses, SystemClock>,
    /// Lazily initialized mock metadata service state.
    imds: OnceLock<Arc<ImdsState<SystemClock>>>,
    /// Listen address for the mock metadata service.
    imds_listen: SocketAddr,
    /// Audience for minted workload tokens.
    workload_token_audience: Audience,
    /// Lifetime of minted workload tokens.
    workload_token_ttl: TokenTtl,
}

/// A fleet's identity, owned so it can cross onto the blocking pool.
/// The local provisioner has no fleet options.
struct OwnedFleet {
    /// Fleet name.
    name: FleetName,
    /// Instance instruction set architecture.
    isa: Isa,
    /// Resources provided by each instance.
    instance: Resources,
}

impl OwnedFleet {
    /// Copies the fleet identity into owned storage.
    fn of(fleet: Fleet<'_, LocalFleetOptions>) -> Self {
        Self {
            name: fleet.name.clone(),
            isa: fleet.isa,
            instance: fleet.instance,
        }
    }

    /// Borrows this identity as a fleet with no local options.
    fn view(&self) -> Fleet<'_, LocalFleetOptions> {
        Fleet {
            name: &self.name,
            isa: self.isa,
            instance: self.instance,
            options: &LocalFleetOptions {},
        }
    }
}

impl Plugin for Local {
    type Config = LocalConfig;

    fn build(config: LocalConfig, context: &PluginContext<'_>) -> Result<Self, PortError> {
        let program = std::env::current_exe().map_err(LocalError::CurrentExecutable)?;
        let launch = LaunchSpec {
            program,
            config_path: context.config_path.to_path_buf(),
            launcher: config
                .worker_launcher
                .argv()
                .iter()
                .map(OsString::from)
                .collect(),
        };
        let layout = StateLayout::new(config.state_dir.into_path_buf());
        let config_path = std::path::absolute(context.config_path).map_err(|source| {
            LocalError::Io(gang_os::PathIo::new(
                "resolve controller configuration path",
                context.config_path,
                source,
            ))
        })?;
        let controller_host_path = layout.controller_host_path(&config_path);
        let fleet = LocalFleet::new(
            layout.clone(),
            launch,
            WorkerProcesses::new(),
            SystemClock::new(),
            config.relaunch_backoff,
            config.terminate_grace,
        );
        Ok(Self {
            state: Arc::new(LocalState {
                layout,
                prepared: OnceLock::new(),
                controller_host_path,
                controller_host_identity: OnceLock::new(),
                fleet,
                imds: OnceLock::new(),
                imds_listen: config.mock_imds_listen.get(),
                workload_token_audience: config.workload_token.audience,
                workload_token_ttl: config.workload_token.ttl,
            }),
            imds_client: Arc::new(ImdsClient::new(
                config.mock_imds_listen.get(),
                config.imds_request_timeout,
                Arc::new(SystemClock::new()),
            )?),
        })
    }
}

impl Local {
    /// Runs `work` on the blocking pool with the shared state: every local port call touches the
    /// filesystem, takes locks, or forks and signals worker processes.
    fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(&LocalState) -> Result<T, LocalError> + Send + 'static,
    ) -> impl Future<Output = Result<T, PortError>> + Send + 'static {
        let state = Arc::clone(&self.state);
        async move {
            tokio::task::spawn_blocking(move || work(&state))
                .await
                .map_err(PortError::backend)?
                .map_err(PortError::from)
        }
    }

    /// Checks and records endpoints before acquiring the supplied listener.
    async fn attach_with(
        &self,
        endpoints: &ServerEndpoints,
        listen: impl Future<Output = std::io::Result<tokio::net::TcpListener>> + Send,
    ) -> Result<Option<Sidecar>, PortError> {
        let endpoints = WorkerEndpoints {
            public: endpoints.public.clone(),
            worker: endpoints.worker.clone(),
        };
        self.state.fleet.set_server_endpoints(endpoints)?;
        let listener = listen.await.map_err(LocalError::MockImdsListen)?;
        let imds = self
            .blocking(move |state| {
                state.layout()?;
                state.imds()
            })
            .await?;
        let imds = MockImds::new(listener, imds.router());
        let addr = imds.local_addr()?;
        Ok(Some(Sidecar::new(addr, async move {
            match imds.serve().await {
                Err(error) => PortError::from(error),
                Ok(()) => {
                    PortError::backend(std::io::Error::other("mock metadata service stopped"))
                }
            }
        })))
    }
}

impl LocalState {
    /// The state layout, with its directories created on first use. Every filesystem-touching
    /// method calls this first. A racing first caller creates the same directories, so either
    /// one recording first is fine.
    fn layout(&self) -> Result<&StateLayout, LocalError> {
        if self.prepared.get().is_none() {
            self.layout.prepare()?;
            let _ = self.prepared.set(());
        }
        Ok(&self.layout)
    }

    /// The mock IMDS state, built on first use. Loading the workload signing key creates it
    /// when absent; a racing first caller loads the same key, so either result is kept.
    fn imds(&self) -> Result<Arc<ImdsState<SystemClock>>, LocalError> {
        if let Some(imds) = self.imds.get() {
            return Ok(Arc::clone(imds));
        }
        let layout = self.layout()?;
        ensure_private_dir(&layout.imds_dir())?;
        let jwt_key = key_file::load_or_create(&layout.jwt_key_path())?;
        let minter = JwtMinter::new(
            &jwt_key,
            format!("http://{}", self.imds_listen),
            self.workload_token_audience.clone(),
            self.workload_token_ttl,
        )?;
        let imds = Arc::new(ImdsState::new(
            self.fleet.records().clone(),
            minter,
            SystemClock::new(),
        ));
        Ok(Arc::clone(self.imds.get_or_init(|| imds)))
    }
}

impl ProvisionerPort for Local {
    type FleetOptions = LocalFleetOptions;

    fn supply(
        &self,
        fleet: Fleet<'_, LocalFleetOptions>,
        into: &mut FleetSupply,
    ) -> impl Future<Output = Result<(), PortError>> + Send {
        let fleet = OwnedFleet::of(fleet);
        // Move the buffer into the blocking task and back to reuse its allocation.
        let mut buffer = std::mem::take(into);
        let work = self.blocking(move |state| {
            state.layout()?;
            state.fleet.supply(fleet.view(), &mut buffer)?;
            Ok(buffer)
        });
        async move {
            *into = work.await?;
            Ok(())
        }
    }

    fn converge(
        &self,
        held: &Held,
        fleet: Fleet<'_, LocalFleetOptions>,
    ) -> impl Future<Output = Result<(), PortError>> + Send {
        let fleet = OwnedFleet::of(fleet);
        let deadline = held.deadline();
        self.blocking(move |state| {
            deadline.check()?;
            state.layout()?;
            state.fleet.converge(&deadline, fleet.view())
        })
    }

    fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, LocalFleetOptions>,
        desired: u32,
    ) -> impl Future<Output = Result<(), PortError>> + Send {
        let fleet = OwnedFleet::of(fleet);
        let deadline = held.deadline();
        self.blocking(move |state| {
            deadline.check()?;
            state.layout()?;
            state
                .fleet
                .set_desired_capacity(&deadline, fleet.view(), desired)
        })
    }

    fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, LocalFleetOptions>,
        instance: &InstanceId,
    ) -> impl Future<Output = Result<(), PortError>> + Send {
        let fleet = OwnedFleet::of(fleet);
        let deadline = held.deadline();
        let instance = instance.clone();
        self.blocking(move |state| {
            deadline.check()?;
            state.layout()?;
            state.fleet.terminate(&deadline, fleet.view(), &instance)
        })
    }

    fn acknowledge_exit(
        &self,
        held: &Held,
        fleet: Fleet<'_, LocalFleetOptions>,
        instance: &InstanceId,
    ) -> impl Future<Output = Result<(), PortError>> + Send {
        let fleet = OwnedFleet::of(fleet);
        let deadline = held.deadline();
        let instance = instance.clone();
        self.blocking(move |state| {
            deadline.check()?;
            state.layout()?;
            state
                .fleet
                .acknowledge_exit(&deadline, fleet.view(), &instance)
        })
    }

    fn worker_lifetime(&self) -> WorkerLifetime {
        WorkerLifetime::LongLived
    }

    /// Records worker endpoints before binding the mock metadata service. The returned sidecar
    /// serves metadata while the server runs.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for different endpoints, and an unavailable error when the
    /// metadata listener cannot be bound. Retrying with the same endpoints is permitted.
    async fn attach(&self, endpoints: &ServerEndpoints) -> Result<Option<Sidecar>, PortError> {
        self.attach_with(
            endpoints,
            tokio::net::TcpListener::bind(self.state.imds_listen),
        )
        .await
    }
}

impl ControllerHostPort for Local {
    async fn incarnation(&self) -> Result<ControllerHostIdentity, PortError> {
        self.blocking(|state| {
            if let Some(instance) = state.controller_host_identity.get() {
                return Ok(ControllerHostIdentity::Persistent(instance.clone()));
            }
            state.layout()?;
            let instance = host_identity::load_or_create(&state.controller_host_path)?;
            let _ = state.controller_host_identity.set(instance.clone());
            Ok(ControllerHostIdentity::Persistent(instance))
        })
        .await
    }

    /// Nothing retires a local host; `gang server` hands off on SIGTERM instead.
    async fn retiring(&self) -> Result<(), PortError> {
        std::future::pending().await
    }

    /// Nothing waits for local host milestones.
    async fn reached(&self, _milestone: HostMilestone) -> Result<(), PortError> {
        Ok(())
    }
}

impl NodeLifecyclePort for Local {
    /// Fetches the worker's identity document, then records the cgroup the worker runs in, which
    /// the fleet reaps once the worker is dead. `gang worker` identifies itself before
    /// `ActionLimitsPort::prepare` moves it into its own leaf, so the cgroup recorded is the whole
    /// scope its launcher created.
    async fn identity(&self) -> Result<InstanceIdentity, PortError> {
        let identity = WorkerIdentity::from_environment()?;
        let document = self
            .imds_client
            .identity(&identity)
            .await
            .map_err(PortError::from)?;
        let instance = identity.instance.clone();
        self.blocking(move |state| {
            if let Some(cgroup) = gang_os::cgroup::of_process(std::process::id())? {
                state
                    .fleet
                    .records()
                    .write_worker_cgroup(&instance, &cgroup)?;
            }
            Ok(())
        })
        .await?;
        Ok(document)
    }

    async fn preemption_notice(&self) -> Result<Option<PreemptionNotice>, PortError> {
        let identity = WorkerIdentity::from_environment()?;
        self.imds_client
            .preemption(&identity)
            .await
            .map_err(PortError::from)
    }

    /// The local provisioner never parks a node, so this is always `false`.
    async fn parked(&self) -> Result<bool, PortError> {
        Ok(false)
    }
}

impl WorkloadCredentialPort for Local {
    async fn workload_credential(&self) -> Result<Proof, PortError> {
        let identity = WorkerIdentity::from_environment()?;
        self.imds_client
            .workload_token(&identity)
            .await
            .map_err(PortError::from)
    }
}

impl ScratchVolumePort for Local {
    fn mount(
        &self,
        instance: &InstanceId,
    ) -> impl Future<Output = Result<MountedScratchVolume, PortError>> + Send {
        let instance = instance.clone();
        self.blocking(move |state| scratch::mount(state.layout()?, &instance))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_types::Url;
    use std::path::Path;

    use crate::test_support::plugin_context;

    // Tests using port 9 never bind it; attach tests hold their own listener instead.
    fn config(state_dir: &Path, listen: SocketAddr) -> LocalConfig {
        serde_json::from_value(serde_json::json!({
            "state_dir": state_dir,
            "mock_imds_listen": listen,
            "workload_token": {
                "audience": "gang",
            },
        }))
        .expect("valid local config")
    }

    #[tokio::test]
    async fn controller_identity_survives_clones_and_rebuilding() -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let listen: SocketAddr = "127.0.0.1:9".parse()?;
        let state = dir.path().join("state");
        let config_path = dir.path().join("controller.toml");
        let context = PluginContext {
            config_path: &config_path,
        };
        let local = Local::build(config(&state, listen), &context)?;
        let clone = local.clone();
        assert!(!state.exists());
        let first = local.incarnation().await?;
        assert!(matches!(first, ControllerHostIdentity::Persistent(_)));
        assert_eq!(clone.incarnation().await?, first);
        drop(local);
        drop(clone);
        let restarted = Local::build(config(&state, listen), &context)?;
        assert_eq!(restarted.incarnation().await?, first);
        Ok(())
    }

    #[tokio::test]
    async fn distinct_configs_sharing_state_have_distinct_controller_identities()
    -> gang_testkit::TestResult {
        let dir = tempfile::tempdir()?;
        let listen: SocketAddr = "127.0.0.1:9".parse()?;
        let state = dir.path().join("state");
        let first_path = dir.path().join("controller-a.toml");
        let second_path = dir.path().join("controller-b.toml");
        let first = Local::build(
            config(&state, listen),
            &PluginContext {
                config_path: &first_path,
            },
        )?;
        let second = Local::build(
            config(&state, listen),
            &PluginContext {
                config_path: &second_path,
            },
        )?;
        let (first, second) = tokio::join!(first.incarnation(), second.incarnation());
        assert_ne!(first?, second?);
        Ok(())
    }

    #[tokio::test]
    async fn independent_state_roots_do_not_derive_identity_from_the_config_path()
    -> gang_testkit::TestResult {
        let first_root = tempfile::tempdir()?;
        let second_root = tempfile::tempdir()?;
        ensure_private_dir(first_root.path())?;
        ensure_private_dir(second_root.path())?;
        let listen: SocketAddr = "127.0.0.1:9".parse()?;
        let context = PluginContext {
            config_path: Path::new("/installation/controller.toml"),
        };
        let first = Local::build(config(first_root.path(), listen), &context)?;
        let second = Local::build(config(second_root.path(), listen), &context)?;
        assert_ne!(first.incarnation().await?, second.incarnation().await?);
        Ok(())
    }

    #[tokio::test]
    async fn controller_identity_rejects_insecure_root_before_creating_metadata()
    -> gang_testkit::TestResult {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        let state = dir.path().join("state");
        std::fs::create_dir(&state)?;
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755))?;
        let local = Local::build(config(&state, "127.0.0.1:9".parse()?), &plugin_context())?;
        assert!(matches!(
            local.incarnation().await,
            Err(PortError::Config(_))
        ));
        assert!(!state.join("controller-hosts").exists());
        assert_eq!(
            std::fs::metadata(&state)?.permissions().mode() & 0o777,
            0o755
        );
        Ok(())
    }

    #[tokio::test]
    async fn controller_identity_uses_installation_symlink_not_its_target()
    -> gang_testkit::TestResult {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir()?;
        let state = dir.path().join("state");
        let config_path = dir.path().join("controller.toml");
        let target = dir.path().join("revision-a.toml");
        std::fs::write(&target, "")?;
        symlink(&target, &config_path)?;
        let context = PluginContext {
            config_path: &config_path,
        };
        let listen = "127.0.0.1:9".parse()?;
        let local = Local::build(config(&state, listen), &context)?;
        let first = local.incarnation().await?;
        let target_host = Local::build(
            config(&state, listen),
            &PluginContext {
                config_path: &target,
            },
        )?;
        assert_ne!(target_host.incarnation().await?, first);
        std::fs::remove_file(&config_path)?;
        let replacement = dir.path().join("revision-b.toml");
        std::fs::write(&replacement, "")?;
        symlink(&replacement, &config_path)?;
        let rebuilt = Local::build(config(&state, listen), &context)?;
        assert_eq!(rebuilt.incarnation().await?, first);
        Ok(())
    }

    #[test]
    fn a_loopback_metadata_listen_address_is_accepted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let listen: SocketAddr = "127.0.0.1:9".parse().expect("valid socket address");
        let context = plugin_context();

        assert!(Local::build(config(&dir.path().join("state"), listen), &context).is_ok());
    }

    #[test]
    fn building_the_plugin_creates_no_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = dir.path().join("state");
        let listen: SocketAddr = "127.0.0.1:9".parse().expect("valid socket address");
        let context = plugin_context();

        assert!(Local::build(config(&state, listen), &context).is_ok());

        assert!(!state.exists());
    }

    #[test]
    fn the_first_filesystem_use_creates_the_layout() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let state = dir.path().join("state");
        let listen: SocketAddr = "127.0.0.1:9".parse()?;
        let context = plugin_context();
        let local =
            Local::build(config(&state, listen), &context).map_err(|error| format!("{error:?}"))?;

        local.state.layout()?;

        assert!(state.join("instances").is_dir());
        assert!(state.join("fleets").is_dir());
        assert!(state.join("logs").is_dir());
        Ok(())
    }

    #[tokio::test]
    async fn the_local_node_is_never_parked() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let listen: SocketAddr = "127.0.0.1:9".parse()?;
        let context = plugin_context();
        let local = Local::build(config(&dir.path().join("state"), listen), &context)
            .map_err(|error| format!("{error:?}"))?;

        assert!(!local.parked().await.map_err(|error| format!("{error:?}"))?);
        Ok(())
    }

    fn built_local(dir: &Path, listen: SocketAddr) -> Result<Local, Box<dyn std::error::Error>> {
        let context = plugin_context();
        let local = Local::build(config(&dir.join("state"), listen), &context)
            .map_err(|error| format!("{error:?}"))?;
        Ok(local)
    }

    fn server_endpoints(worker: &str) -> Result<ServerEndpoints, Box<dyn std::error::Error>> {
        Ok(ServerEndpoints {
            public: Url::parse("https://gang.example:8443")?,
            worker: Url::parse(worker)?,
        })
    }

    #[tokio::test]
    async fn attach_retries_after_bind_failure_and_accepts_the_same_endpoints()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let local = built_local(dir.path(), listener.local_addr()?)?;
        let endpoints = server_endpoints("http://127.0.0.1:41001")?;

        assert!(matches!(
            local.attach(&endpoints).await,
            Err(PortError::Unavailable(_))
        ));
        let listen = || async { tokio::net::TcpListener::from_std(listener.try_clone()?) };
        let first = local.attach_with(&endpoints, listen()).await?;
        assert!(first.is_some());
        drop(first);
        let second = local.attach_with(&endpoints, listen()).await?;
        assert!(second.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn attach_records_endpoints_even_when_metadata_listener_is_busy()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let local = built_local(dir.path(), listener.local_addr()?)?;
        let first = server_endpoints("http://127.0.0.1:41001")?;
        let different = server_endpoints("http://127.0.0.1:41002")?;

        assert!(matches!(
            local.attach(&first).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(matches!(
            local.attach(&different).await,
            Err(PortError::Config(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn attaching_to_different_endpoints_is_a_config_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let listen = listener.local_addr()?;
        let local = built_local(dir.path(), listen)?;
        let first = local
            .attach_with(&server_endpoints("http://127.0.0.1:41001")?, async {
                tokio::net::TcpListener::from_std(listener.try_clone()?)
            })
            .await?;
        assert!(first.is_some());

        let second = local
            .attach(&server_endpoints("http://127.0.0.1:41002")?)
            .await;

        assert!(matches!(second, Err(PortError::Config(_))));
        Ok(())
    }
}
