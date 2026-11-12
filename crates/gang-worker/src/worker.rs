//! The `gang worker` run loop: describes the worker once, then runs it generation by generation
//! until shutdown, preemption, or the scheduler's worker returns.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gang_clock::{Clock, Timestamp};
use gang_config::{PluginSet, SandboxFile, SandboxSelection, WorkerConfig};
use gang_spi::{
    ActionLimitsPort, CacheStorePort, ClientTls, NodeLifecyclePort, PortError, ProvisionerPort,
    SchedulerPort, ScratchVolumePort, Slots, WorkerSpec, WorkerTls, WorkloadCredentialPort,
};
use gang_types::WorkerLifetime;

use crate::credentials::Enroller;
use crate::deployment::{WorkerDeployment, WorkerParts, WorkerSettings};
use crate::errors::WorkerError;
use crate::layout::{IdentityFiles, Layout};
use crate::programs::resolve_program;
use crate::transport::{ServerCaSource, Transport};

/// Runs the worker until `shutdown` completes, a preemption notice's grace period ends, or
/// the scheduler's worker returns. Shutdown and preemption end the run at any point after the
/// node serves, including while the server CA is fetched or the first enrollment is retried.
///
/// The worker is restarted in a new generation when its certificate reaches the refresh
/// point. The next credentials are obtained while the current worker keeps running, into a
/// directory of their own; the current worker is stopped only once they are in place. The
/// previous generation's files are removed after the switch. The running worker's files are
/// never touched.
///
/// A parked node (see [`NodeLifecyclePort::parked`]) waits and does nothing: nothing is read,
/// mounted, fetched, enrolled or registered until the provisioner stops parking it. `shutdown` still
/// ends the wait.
///
/// # Errors
///
/// [`WorkerError::NoFleet`] when no fleet has the name of the instance's fleet;
/// [`WorkerError::IsaMismatch`] when that fleet is configured for another instruction set;
/// [`WorkerError::FleetDiskUnavailable`] when the scratch volume leaves the actions less disk
/// than the fleet declares;
/// [`WorkerError::Config`] when the sandbox file cannot be rendered;
/// [`WorkerError::SandboxProgram`] when a sandbox program does not resolve to an executable
/// file;
/// [`WorkerError::Port`] or [`WorkerError::Io`] when the instance cannot be prepared;
/// [`WorkerError::Enroll`] when enrollment cannot be retried within its deadline;
/// [`WorkerError::ServerCa`] when the server-only transport's CA certificate cannot be obtained;
/// [`WorkerError::ExpiredCertificate`] when issued credentials have already expired;
/// [`WorkerError::Scheduler`] when the scheduler's worker fails.
pub async fn run<D: WorkerDeployment, P: PluginSet>(
    parts: WorkerParts<D>,
    settings: WorkerSettings<P>,
    build_limits: impl FnOnce(P::Limits) -> Result<D::Limits, PortError>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), WorkerError> {
    let WorkerParts {
        provisioner,
        cache,
        scheduler,
        clock,
        transport,
    } = parts;
    let interval = settings.worker.lifecycle_poll_interval.get();
    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        () = requested(shutdown.as_mut()) => return Ok(()),
        () = wait_until_serving(&provisioner, &clock, interval) => {}
    }
    let (plan, _limits) = describe::<D, P>(&provisioner, &cache, settings, build_limits).await?;
    let mut generation = tokio::select! {
        biased;
        () = requested(shutdown.as_mut()) => return Ok(()),
        () = preemption(&provisioner, &clock, &plan) => return Ok(()),
        generation = async {
            match &transport {
                Transport::Plain => Ok::<_, WorkerError>(Generation::Plain),
                Transport::ServerOnly(source) => Ok(Generation::ServerOnly(
                    source
                        .server_ca()
                        .await?
                        .map(|ca| plan.layout.write_server_ca(&ca))
                        .transpose()?,
                )),
                Transport::Mutual(enroller) => Ok(Generation::Mutual {
                    enroller,
                    current: first_tls::<D>(&plan, &provisioner, enroller, &clock).await?,
                }),
            }
        } => generation?,
    };
    loop {
        let tls = match &generation {
            Generation::Plain => WorkerTls::Plain,
            Generation::ServerOnly(server_ca) => WorkerTls::ServerOnly {
                server_ca: server_ca.as_deref(),
            },
            Generation::Mutual { current, .. } => WorkerTls::Mutual(ClientTls {
                certificate: &current.files.certificate,
                private_key: &current.files.private_key,
                server_ca: &current.files.server_ca,
            }),
        };
        let mut end = None;
        let stop = async {
            end = Some(tokio::select! {
                biased;
                () = requested(shutdown.as_mut()) => End::Shutdown,
                () = preemption(&provisioner, &clock, &plan) => End::Preempted,
                next = async {
                    match &generation {
                        Generation::Mutual { enroller, current } =>
                            renew::<D>(&plan, &provisioner, enroller, &clock, current).await,
                        Generation::Plain | Generation::ServerOnly(_) => std::future::pending().await,
                    }
                } => End::Renewed(next),
            });
        };
        scheduler
            .run_worker(&plan.spec, tls, stop)
            .await
            .map_err(WorkerError::Scheduler)?;
        match end {
            None | Some(End::Shutdown | End::Preempted) => return Ok(()),
            Some(End::Renewed(next)) => {
                let next = next?;
                tracing::info!("worker certificate renewed; starting a new generation");
                if let Generation::Mutual { current, .. } = &mut generation {
                    let previous = std::mem::replace(current, next);
                    if let Err(error) = plan.layout.remove_credentials(&previous.files) {
                        tracing::warn!(error = %gang_types::error_chain(&error), "removing the previous generation's credentials failed");
                    }
                }
            }
        }
    }
}

/// Completes once `shutdown` does, logging that the worker stops.
async fn requested(shutdown: Pin<&mut impl Future<Output = ()>>) {
    shutdown.await;
    tracing::info!("worker shutdown requested");
}

/// Connection security and credentials for the active worker generation.
enum Generation<'a, E> {
    /// No TLS.
    Plain,
    /// Server authentication, optionally with a custom CA file.
    ServerOnly(Option<std::path::PathBuf>),
    /// Enrolled mutual TLS credentials and their enroller.
    Mutual { enroller: &'a E, current: Tls },
}

/// The event that requested a running generation to stop.
enum End {
    /// Shutdown was requested.
    Shutdown,
    /// The preemption grace period ended.
    Preempted,
    /// Renewal completed, successfully or otherwise.
    Renewed(Result<Tls, WorkerError>),
}

/// The values of a worker that do not change between generations.
struct Plan {
    layout: Layout,
    worker: WorkerConfig,
    spec: WorkerSpec,
}

/// Describes the worker: its identity and sizing, its prepared scratch volume, its rendered
/// sandbox file, its limits and its stores.
///
/// Each sandbox program is resolved on the worker's `PATH` before the sandbox file is rendered,
/// so the file names absolute canonical programs; a program that does not resolve fails the
/// description.
///
/// The fast CAS is sized from the free space at mount plus what the worker's own fast CAS
/// directory already holds, because that content is reclaimable. A restart therefore keeps the
/// budget of a cold start instead of shrinking it by the cache it inherited.
/// A scratch volume that leaves the actions less than the fleet's `disk_per_instance` stops
/// the worker before it enrolls.
///
/// The limits are returned so the caller keeps them alive for the whole run.
async fn describe<D: WorkerDeployment, P: PluginSet>(
    provisioner: &D::Provisioner,
    cache: &D::Cache,
    settings: WorkerSettings<P>,
    build_limits: impl FnOnce(P::Limits) -> Result<D::Limits, PortError>,
) -> Result<(Plan, Option<D::Limits>), WorkerError> {
    let WorkerSettings {
        fleets,
        worker,
        sandbox,
        execution,
        limits,
        executable,
        search_path,
    } = settings;
    let identity = provisioner.identity().await?;
    let fleet = fleets
        .by_name(&identity.fleet)
        .ok_or_else(|| WorkerError::NoFleet(identity.fleet.clone()))?;
    if fleet.policy.isa != identity.isa {
        return Err(WorkerError::IsaMismatch {
            fleet: identity.fleet,
            configured: fleet.policy.isa,
            actual: identity.isa,
        });
    }
    let slots = match provisioner.worker_lifetime() {
        WorkerLifetime::SingleUse => Slots::SingleUse,
        WorkerLifetime::LongLived => Slots::LongLived(fleet.policy.target_actions_per_instance),
    };
    let volume = provisioner.mount(&identity.instance).await?;
    let layout = Layout::new(&volume.path);
    let mut sandbox = sandbox;
    sandbox.try_for_each_program(fleet.sandbox, |program| {
        *program = resolve_program(program.as_path(), &search_path)?;
        Ok::<(), WorkerError>(())
    })?;
    let file = SandboxFile::<P> {
        work_directory: gang_types::AbsolutePathBuf::try_new(layout.work_directory())
            .map_err(|error| WorkerError::Config(error.to_string()))?,
        selected: fleet.sandbox,
        sandbox,
        execution,
        limits,
    };
    let text = file
        .to_toml()
        .map_err(|error| WorkerError::Config(error.to_string()))?;
    layout.prepare(&text, &executable)?;
    let cached = gang_os::fs::usage(&layout.fast_cas_directory())?;
    let scratch = volume.capacity.saturating_add(cached);
    let fast_cas_budget = worker.fast_cas_percent.of_bytes(scratch);
    let measured = scratch.saturating_sub(fast_cas_budget);
    if measured < fleet.policy.disk_per_instance.get() {
        return Err(WorkerError::FleetDiskUnavailable {
            fleet: identity.fleet,
            declared: fleet.policy.disk_per_instance.get(),
            measured,
        });
    }
    let limits = file.limits.map(build_limits).transpose()?;
    if let Some(limits) = &limits {
        limits.prepare().await?;
    }
    let spec = WorkerSpec {
        instance: identity.instance,
        properties: fleet.properties.clone(),
        slots,
        work_directory: layout.work_directory(),
        fast_cas_directory: layout.fast_cas_directory(),
        fast_cas_budget,
        content_store: cache.content_store()?,
        action_store: cache.action_store()?,
        store_access: cache.worker_access(),
        entrypoint: layout.entrypoint(),
    };
    let plan = Plan {
        layout,
        worker,
        spec,
    };
    Ok((plan, limits))
}

/// The TLS material of one generation: the files the worker reads, when to refresh them, and
/// when the certificate expires.
struct Tls {
    /// The generation number: the first is `0`, and each renewal takes the next.
    generation: u64,
    files: IdentityFiles,
    refresh: Duration,
    not_after: Timestamp,
}

/// Obtains the credentials of `generation`: a workload proof, enrollment, and the identity files
/// written into that generation's own directory.
async fn acquire<D: WorkerDeployment>(
    plan: &Plan,
    provisioner: &D::Provisioner,
    enroller: &D::Enroller,
    clock: &D::Clock,
    generation: u64,
) -> Result<Tls, WorkerError> {
    let credentials = enroller
        .enroll(provisioner.workload_credential().await?)
        .await?;
    let now = clock.wall();
    if credentials.not_after <= now {
        return Err(WorkerError::ExpiredCertificate {
            not_after: credentials.not_after,
        });
    }
    let refresh = plan
        .worker
        .certificate_refresh_percent
        .of_duration(credentials.not_after.saturating_duration_since(now));
    let files = plan.layout.write_credentials(generation, &credentials)?;
    Ok(Tls {
        generation,
        files,
        refresh,
        not_after: credentials.not_after,
    })
}

/// How long to wait before retrying after `error`: the `retry_after` the server sent with a
/// rate-limit refusal, or `interval` when the failure carries none.
fn retry_wait(error: &WorkerError, interval: Duration) -> Duration {
    match error {
        WorkerError::Enroll(error) => error.rate_limit().and_then(|limit| limit.retry_after),
        _ => None,
    }
    .unwrap_or(interval)
}

/// Obtains the first generation's TLS material, before any worker runs.
///
/// A rate-limited refusal is retried only if the retry can start by the enrollment deadline.
/// Every other failure is returned at once.
async fn first_tls<D: WorkerDeployment>(
    plan: &Plan,
    provisioner: &D::Provisioner,
    enroller: &D::Enroller,
    clock: &D::Clock,
) -> Result<Tls, WorkerError> {
    let started = clock.monotonic();
    let tls = loop {
        match acquire::<D>(plan, provisioner, enroller, clock, 0).await {
            Ok(tls) => break tls,
            Err(error) => {
                let rate_limited = matches!(
                    &error,
                    WorkerError::Enroll(enroll) if enroll.rate_limit().is_some()
                );
                let wait = retry_wait(&error, plan.worker.enrollment_retry_interval.get());
                let remaining = plan
                    .worker
                    .enrollment_deadline
                    .get()
                    .saturating_sub(clock.monotonic().saturating_duration_since(started));
                if !rate_limited || wait > remaining || remaining.is_zero() {
                    return Err(error);
                }
                tracing::warn!(error = %gang_types::error_chain(&error), retry_after = ?wait, "worker enrollment rate limited; retrying");
                clock.sleep(wait).await;
            }
        }
    };
    tracing::info!(refresh_after = ?tls.refresh, "worker generation starting");
    Ok(tls)
}

/// Obtains the next generation's TLS material while the current worker keeps running: waits for
/// the refresh point, then retries failed attempts only while the wait fits before expiration.
async fn renew<D: WorkerDeployment>(
    plan: &Plan,
    provisioner: &D::Provisioner,
    enroller: &D::Enroller,
    clock: &D::Clock,
    current: &Tls,
) -> Result<Tls, WorkerError> {
    clock.sleep(current.refresh).await;
    loop {
        match acquire::<D>(plan, provisioner, enroller, clock, current.generation + 1).await {
            Ok(tls) => return Ok(tls),
            Err(error) => {
                let wait = retry_wait(&error, plan.worker.enrollment_retry_interval.get());
                let remaining = current.not_after.saturating_duration_since(clock.wall());
                if wait > remaining || remaining.is_zero() {
                    return Err(error);
                }
                tracing::warn!(error = %gang_types::error_chain(&error), retry_after = ?wait, "worker certificate renewal failed; retrying");
                clock.sleep(wait).await;
            }
        }
    }
}

/// Runs `check` until it returns a value, sleeping `interval` on the clock between attempts.
async fn poll_until<T, F: Future<Output = Option<T>>>(
    clock: &impl Clock,
    interval: Duration,
    mut check: impl FnMut() -> F,
) -> T {
    loop {
        if let Some(value) = check().await {
            return value;
        }
        clock.sleep(interval).await;
    }
}

/// Completes when the provisioner's preemption notice leaves the worker its grace period.
///
/// A failed check is logged and retried like a check that found no notice.
async fn preemption<P: NodeLifecyclePort>(provisioner: &P, clock: &impl Clock, plan: &Plan) {
    let notice = poll_until(
        clock,
        plan.worker.lifecycle_poll_interval.get(),
        move || async move {
            match provisioner.preemption_notice().await {
                Ok(Some(notice)) => {
                    tracing::info!(
                        terminate_at = notice.terminate_at.as_unix_millis(),
                        "preemption notice received"
                    );
                    Some(notice)
                }
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(error = %gang_types::error_chain(&error), "preemption check failed");
                    None
                }
            }
        },
    )
    .await;
    let wait = notice
        .terminate_at
        .saturating_duration_since(clock.wall())
        .saturating_sub(plan.worker.preemption_margin);
    clock.sleep(wait).await;
}

/// Completes once the provisioner reports the node as not parked.
///
/// A parked node is logged once and re-checked every `interval`. A failed check is logged and
/// retried the same way: it never lets a parked node serve.
async fn wait_until_serving<P: NodeLifecyclePort>(
    provisioner: &P,
    clock: &impl Clock,
    interval: Duration,
) {
    let logged = &AtomicBool::new(false);
    poll_until(clock, interval, move || async move {
        match provisioner.parked().await {
            Ok(false) => Some(()),
            Ok(true) => {
                if !logged.swap(true, Ordering::Relaxed) {
                    tracing::info!("node is parked; waiting");
                }
                None
            }
            Err(error) => {
                tracing::warn!(error = %gang_types::error_chain(&error), "parked check failed");
                None
            }
        }
    })
    .await;
}
