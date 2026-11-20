//! Fail-closed setup and lifetime management for a public-egress sandbox.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::process::Stdio;

use gang_spi::{ActionExecutionOutcome, ActionExecutionSpec, PortError, SandboxedAction};
use gang_types::{PublicEgressPolicy, Url};
use rustix::fs::{MemfdFlags, SealFlags, fcntl_add_seals, memfd_create};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

use crate::bwrap::{Bwrap, NetworkSetup, StartedBwrap};
use crate::egress_policy::{helper_command, prepare_policy};
use crate::errors::BwrapError;
use crate::relay::Relay;

/// slirp4netns writes ASCII '1' to --ready-fd when the tap is configured.
const SLIRP_READY: u8 = b'1';

struct Setup {
    bwrap: Option<StartedBwrap>,
    nft: Option<Child>,
    ip: Option<Child>,
    slirp: Option<Child>,
    exit: Option<std::io::PipeWriter>,
    gate: Option<gang_os::AsyncExecutionGate>,
    ready: Option<gang_os::ExecutionReady>,
}

fn kill_before_close(kill: impl FnOnce(), close: impl FnOnce()) {
    kill();
    close();
}

impl Setup {
    fn new() -> Self {
        Self {
            bwrap: None,
            nft: None,
            ip: None,
            slirp: None,
            exit: None,
            gate: None,
            ready: None,
        }
    }

    // Kill every owned process before closing either control channel. In particular,
    // the pidfd pins the gated payload independently of the bwrap launcher.
    fn initiate_abort(&mut self) {
        let ready = &self.ready;
        let (bwrap, nft, ip, slirp) = (
            &mut self.bwrap,
            &mut self.nft,
            &mut self.ip,
            &mut self.slirp,
        );
        let (gate, exit) = (&mut self.gate, &mut self.exit);
        kill_before_close(
            || {
                if let Some(ready) = ready
                    && let Err(error) = rustix::process::pidfd_send_signal(
                        &ready.process,
                        rustix::process::Signal::KILL,
                    )
                    && error != rustix::io::Errno::SRCH
                {
                    tracing::warn!(%error, "failed to kill gated egress process");
                }
                for child in [
                    bwrap.as_mut().map(|b| &mut b.child),
                    nft.as_mut(),
                    ip.as_mut(),
                    slirp.as_mut(),
                ]
                .into_iter()
                .flatten()
                {
                    if let Err(error) = child.start_kill()
                        && error.kind() != io::ErrorKind::InvalidInput
                    {
                        tracing::warn!(%error, "failed to kill egress setup child");
                    }
                }
            },
            || {
                gate.take();
                exit.take();
            },
        );
        self.ready.take();
    }

    async fn abort(&mut self) {
        self.initiate_abort();
        for child in [
            self.nft.as_mut(),
            self.ip.as_mut(),
            self.slirp.as_mut(),
            self.bwrap.as_mut().map(|b| &mut b.child),
        ]
        .into_iter()
        .flatten()
        {
            if let Err(error) = child.wait().await {
                tracing::warn!(%error, "failed to reap egress setup child");
            }
        }
    }
}

impl Drop for Setup {
    fn drop(&mut self) {
        self.initiate_abort();
    }
}

fn sealed_file(name: &str, write: impl FnOnce(&mut File) -> io::Result<()>) -> io::Result<File> {
    let fd = memfd_create(name, MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING)?;
    let mut file = File::from(fd);
    write(&mut file)?;
    file.seek(SeekFrom::Start(0))?;
    fcntl_add_seals(
        &file,
        SealFlags::WRITE | SealFlags::GROW | SealFlags::SHRINK | SealFlags::SEAL,
    )?;
    Ok(file)
}

fn check_identity(bwrap: &Bwrap) -> io::Result<()> {
    use rustix::process::{getegid, geteuid, getgid, getuid};
    if getuid() != geteuid()
        || getgid() != getegid()
        || std::fs::metadata(&bwrap.program)?.mode() & 0o6000 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "public egress requires non-setid bubblewrap and equal real and effective IDs",
        ));
    }
    Ok(())
}

fn check_namespace(ready: &gang_os::ExecutionReady) -> io::Result<std::os::fd::OwnedFd> {
    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    let mut fds = [PollFd::new(&ready.process, PollFlags::IN)];
    if poll(
        &mut fds,
        Some(&Timespec {
            tv_sec: 0,
            tv_nsec: 0,
        }),
    )? != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "gated process has exited",
        ));
    }
    let host = File::open("/proc/self/ns/net")?.metadata()?;
    let sandbox = rustix::fs::fstat(&ready.network_namespace)?;
    if host.dev() == sandbox.st_dev && host.ino() == sandbox.st_ino {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "sandbox has the host network namespace",
        ));
    }
    gang_os::namespace_owner(ready.network_namespace.as_fd())
}

// gang-exec runs one action on a current-thread runtime and does not spawn another process
// concurrently. Keep descriptor inheritance, spawn and restoration synchronous, with no await.
// Namespace descriptors are created after bwrap starts and cannot be inherited by its payload.
fn spawn_helper(mut command: Command, fds: &[BorrowedFd<'_>]) -> io::Result<Child> {
    for (index, fd) in fds.iter().enumerate() {
        if let Err(error) = rustix::io::fcntl_setfd(fd.as_fd(), rustix::io::FdFlags::empty()) {
            for previous in &fds[..index] {
                if let Err(restore) =
                    rustix::io::fcntl_setfd(previous.as_fd(), rustix::io::FdFlags::CLOEXEC)
                {
                    tracing::error!(%restore, "failed to restore helper descriptor close-on-exec");
                }
            }
            return Err(error.into());
        }
    }
    command.kill_on_drop(true);
    let result = match gang_os::block_terminations() {
        Ok(blocked) => {
            let result = command.spawn();
            drop(blocked);
            result
        }
        Err(error) => Err(error),
    };
    let mut restore_error = None;
    for fd in fds {
        if let Err(error) = rustix::io::fcntl_setfd(fd.as_fd(), rustix::io::FdFlags::CLOEXEC) {
            restore_error.get_or_insert(io::Error::from(error));
        }
    }
    match (result, restore_error) {
        (Ok(mut child), Some(error)) => {
            if let Err(kill_error) = child.start_kill() {
                tracing::warn!(%kill_error, "failed to kill helper after descriptor restoration failure");
            }
            Err(error)
        }
        (Err(error), _) => Err(error),
        (Ok(child), None) => Ok(child),
    }
}

async fn ready_byte(reader: std::io::PipeReader) -> io::Result<()> {
    let reader = gang_os::PipeReader::new(OwnedFd::from(reader))?;
    let mut byte = [0];
    match reader.read(&mut byte).await? {
        1 if byte[0] == SLIRP_READY => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid slirp readiness byte or EOF",
        )),
    }
}

async fn wait_slirp_ready(reader: std::io::PipeReader, child: &mut Child) -> io::Result<()> {
    if let Err(error) = ready_byte(reader).await {
        // EOF can precede process exit by a small interval. Do not wait indefinitely:
        // a broken helper can close or write the readiness pipe and keep running.
        let status =
            tokio::time::timeout(std::time::Duration::from_millis(100), child.wait()).await;
        return match status {
            Ok(Ok(status)) => {
                tracing::error!(%status, %error, "slirp4netns exited before readiness");
                Err(io::Error::new(
                    error.kind(),
                    format!("{error}: slirp4netns exited with {status} before readiness"),
                ))
            }
            Ok(Err(wait_error)) => {
                tracing::error!(%wait_error, %error, "failed to obtain slirp4netns exit status after readiness failure");
                Err(io::Error::new(
                    error.kind(),
                    format!("{error}: failed to obtain slirp4netns exit status: {wait_error}"),
                ))
            }
            Err(_) => {
                tracing::error!(%error, "slirp4netns readiness failed while helper was still running");
                Err(io::Error::new(
                    error.kind(),
                    format!("{error}: slirp4netns has not exited; exit status unavailable"),
                ))
            }
        };
    }
    if let Some(status) = child.try_wait()? {
        tracing::error!(%status, "slirp4netns exited during setup");
        return Err(io::Error::other(format!(
            "slirp4netns exited during setup: {status}"
        )));
    }
    Ok(())
}

fn prepare_sealed_inputs(
    spec: &ActionExecutionSpec<'_>,
    policy: &PublicEgressPolicy,
) -> Result<(File, File), BwrapError> {
    let resolver = sealed_file("gang-resolver", |file| {
        for address in &policy.dns_servers {
            writeln!(file, "nameserver {address}")?;
        }
        Ok(())
    })
    .map_err(BwrapError::NetworkSetup)?;
    let environment = sealed_file("gang-environment", |file| {
        gang_os::write_execution_environment(file, spec.environment)
    })
    .map_err(BwrapError::NetworkSetup)?;
    Ok((resolver, environment))
}

async fn apply_nft(
    child: &mut Option<Child>,
    command: Command,
    fds: &[BorrowedFd<'_>],
    rules: &str,
) -> Result<(), BwrapError> {
    *child = Some(spawn_helper(command, fds).map_err(BwrapError::NetworkSetup)?);
    let nft = child
        .as_mut()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("nft child missing")))?;
    let mut input = nft
        .stdin
        .take()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("nft stdin missing")))?;
    input
        .write_all(rules.as_bytes())
        .await
        .map_err(BwrapError::NetworkSetup)?;
    drop(input);
    let status = nft.wait().await.map_err(BwrapError::NetworkSetup)?;
    *child = None;
    if !status.success() {
        return Err(BwrapError::NetworkSetup(io::Error::other(format!(
            "nft exited with {status}"
        ))));
    }
    Ok(())
}

async fn apply_ip(
    child: &mut Option<Child>,
    command: Command,
    fds: &[BorrowedFd<'_>],
) -> Result<(), BwrapError> {
    *child = Some(spawn_helper(command, fds).map_err(BwrapError::NetworkSetup)?);
    let ip = child
        .as_mut()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("ip child missing")))?;
    let status = ip.wait().await.map_err(BwrapError::NetworkSetup)?;
    *child = None;
    if !status.success() {
        return Err(BwrapError::NetworkSetup(io::Error::other(format!(
            "ip exited with {status}"
        ))));
    }
    Ok(())
}

fn release_gate(state: &mut Setup, environment: &File) -> Result<(), BwrapError> {
    // release consumes the gate even on failure. Keep a duplicate open until
    // abort has signalled the pinned process and all children in that case.
    let gate = state
        .gate
        .as_ref()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("execution gate missing")))?;
    let hold = gate.duplicate().map_err(BwrapError::NetworkSetup)?;
    let gate = state
        .gate
        .take()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("execution gate missing")))?;
    if let Err(error) = gate.release(environment.as_fd()) {
        state.initiate_abort();
        drop(hold);
        return Err(BwrapError::NetworkSetup(error));
    }
    drop(hold);
    state.ready.take();
    Ok(())
}

async fn start_slirp(
    state: &mut Setup,
    spec: &ActionExecutionSpec<'_>,
    program: &std::path::Path,
    owner: &OwnedFd,
    user_path: &str,
    net_path: &str,
) -> Result<(), BwrapError> {
    let ready = state
        .ready
        .as_ref()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("execution readiness missing")))?;
    let (ready_read, ready_write) = std::io::pipe().map_err(BwrapError::NetworkSetup)?;
    let (exit_read, exit_write) = std::io::pipe().map_err(BwrapError::NetworkSetup)?;
    state.exit = Some(exit_write);
    let mut slirp = helper_command(program, spec.launcher);
    slirp
        .args([
            "--configure",
            "--enable-ipv6",
            "--disable-host-loopback",
            "--disable-dns",
            "--netns-type=path",
        ])
        .arg(format!("--userns-path={user_path}"))
        .arg(format!("--ready-fd={}", ready_write.as_raw_fd()))
        .arg(format!("--exit-fd={}", exit_read.as_raw_fd()))
        .arg(net_path)
        .arg("tap0")
        .stdin(Stdio::null());
    state.slirp = Some(
        spawn_helper(
            Command::from(slirp),
            &[
                owner.as_fd(),
                ready.network_namespace.as_fd(),
                ready_write.as_fd(),
                exit_read.as_fd(),
            ],
        )
        .map_err(BwrapError::NetworkSetup)?,
    );
    drop((ready_write, exit_read));
    let child = state
        .slirp
        .as_mut()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("slirp child missing")))?;
    wait_slirp_ready(ready_read, child)
        .await
        .map_err(BwrapError::NetworkSetup)?;
    // A ready process can exit while helpers are being configured.
    check_namespace(ready).map_err(BwrapError::NetworkSetup)?;
    Ok(())
}

async fn configure(
    state: &mut Setup,
    bwrap: &Bwrap,
    spec: &ActionExecutionSpec<'_>,
    policy: &PublicEgressPolicy,
    endpoints: &[Url],
) -> Result<(), BwrapError> {
    let helpers = bwrap
        .egress
        .as_ref()
        .ok_or(BwrapError::NetworkNotConfigured)?;
    check_identity(bwrap).map_err(BwrapError::NetworkSetup)?;
    let rules = prepare_policy(helpers, spec, policy, endpoints)
        .await
        .map_err(BwrapError::NetworkSetup)?;
    let (resolver, environment) = prepare_sealed_inputs(spec, policy)?;
    let (gate, gate_child) = gang_os::execution_gate_pair().map_err(BwrapError::NetworkSetup)?;
    state.gate = Some(gate.into_async().map_err(BwrapError::NetworkSetup)?);
    state.bwrap = Some({
        let network = NetworkSetup {
            gate: gate_child,
            resolver,
        };
        bwrap.spawn(spec, Some((network.gate.as_fd(), network.resolver.as_fd())))?
    });
    state
        .bwrap
        .as_mut()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("bwrap child missing")))?
        .read_info()
        .await?;
    state.ready = Some(
        state
            .gate
            .as_ref()
            .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("execution gate missing")))?
            .ready()
            .await
            .map_err(BwrapError::NetworkSetup)?,
    );
    let ready = state
        .ready
        .as_ref()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("execution readiness missing")))?;
    let owner = check_namespace(ready).map_err(BwrapError::NetworkSetup)?;
    let user_path = format!("/proc/self/fd/{}", owner.as_raw_fd());
    let net_path = format!("/proc/self/fd/{}", ready.network_namespace.as_raw_fd());

    let mut nft = helper_command(&helpers.nsenter, spec.launcher);
    nft.arg(format!("--user={user_path}"))
        .arg(format!("--net={net_path}"))
        .arg("--preserve-credentials")
        .arg("--")
        .arg(&helpers.nft)
        .args(["-f", "-"])
        .stdin(Stdio::piped());
    apply_nft(
        &mut state.nft,
        Command::from(nft),
        &[owner.as_fd(), ready.network_namespace.as_fd()],
        &rules,
    )
    .await?;

    start_slirp(
        state,
        spec,
        &helpers.slirp4netns,
        &owner,
        &user_path,
        &net_path,
    )
    .await?;
    // Configure IPv6 before release rather than racing router advertisements and DAD.
    for args in [
        &["-6", "addr", "add", "fd00::100/64", "dev", "tap0", "nodad"][..],
        &[
            "-6", "route", "add", "default", "via", "fd00::2", "dev", "tap0",
        ][..],
    ] {
        let ready = state.ready.as_ref().ok_or_else(|| {
            BwrapError::NetworkSetup(io::Error::other("execution readiness missing"))
        })?;
        let mut ip = helper_command(&helpers.nsenter, spec.launcher);
        ip.arg(format!("--user={user_path}"))
            .arg(format!("--net={net_path}"))
            .arg("--preserve-credentials")
            .arg("--")
            .arg(&helpers.ip)
            .args(args)
            .stdin(Stdio::null());
        apply_ip(
            &mut state.ip,
            Command::from(ip),
            &[owner.as_fd(), ready.network_namespace.as_fd()],
        )
        .await?;
    }
    release_gate(state, &environment)
}

pub(crate) async fn start(
    bwrap: &Bwrap,
    spec: &ActionExecutionSpec<'_>,
    policy: &PublicEgressPolicy,
    endpoints: &[Url],
) -> Result<SandboxedAction, PortError> {
    let mut state = Setup::new();
    match tokio::time::timeout(
        policy.setup_timeout.get(),
        configure(&mut state, bwrap, spec, policy, endpoints),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            state.abort().await;
            return Err(error.into());
        }
        Err(_) => {
            state.abort().await;
            return Err(BwrapError::NetworkTimeout(policy.setup_timeout).into());
        }
    }
    let started = state
        .bwrap
        .take()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("bwrap child missing")))?;
    let relay = match (started.launcher, started.sandbox) {
        (Some(launcher), Some(sandbox)) => Some(Relay::new(launcher, sandbox)),
        _ => None,
    };
    let mut child = started.child;
    let mut slirp = state
        .slirp
        .take()
        .ok_or_else(|| BwrapError::NetworkSetup(io::Error::other("slirp child missing")))?;
    let exit = state.exit.take();
    let wait = async move {
        let status = tokio::select! {
            result = child.wait() => {
                drop(exit);
                if let Err(error) = slirp.start_kill()
                    && error.kind() != io::ErrorKind::InvalidInput
                {
                    tracing::warn!(%error, "failed to stop slirp");
                }
                if let Err(error) = slirp.wait().await { tracing::warn!(%error, "failed to reap slirp"); }
                result.map_err(BwrapError::Wait)?
            }
            result = slirp.wait() => {
                tracing::error!(?result, "public egress helper exited while action was running; killing sandbox");
                drop(exit);
                if let Err(error) = child.start_kill()
                    && error.kind() != io::ErrorKind::InvalidInput
                {
                    tracing::warn!(%error, "failed to kill sandbox after slirp exit");
                }
                child.wait().await.map_err(BwrapError::Wait)?
            }
        };
        ActionExecutionOutcome::try_from(status)
    };
    Ok(match relay {
        Some(relay) => SandboxedAction::relayed(wait, move |termination| {
            relay.relay(termination).map_err(PortError::from)
        }),
        None => SandboxedAction::new(wait),
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::{apply_ip, kill_before_close, ready_byte, sealed_file, wait_slirp_ready};
    use rustix::fs::{SealFlags, fcntl_get_seals};

    #[test]
    fn sealed_files_are_immutable_and_rewound() -> std::io::Result<()> {
        let mut file = sealed_file("test", |file| file.write_all(b"resolver"))?;
        let required = SealFlags::WRITE | SealFlags::GROW | SealFlags::SHRINK | SealFlags::SEAL;
        assert!(fcntl_get_seals(&file)?.contains(required));
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        assert_eq!(contents, "resolver");
        assert!(file.write_all(b"x").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn ip_helper_failure_fails_setup_and_reaps_child() {
        let mut child = None;
        let command = tokio::process::Command::new("/bin/false");
        assert!(apply_ip(&mut child, command, &[]).await.is_err());
        assert!(child.is_none());
        let command = tokio::process::Command::new("/nonexistent/gang-ip-helper");
        assert!(apply_ip(&mut child, command, &[]).await.is_err());
        assert!(child.is_none());
    }

    #[tokio::test]
    async fn slirp_readiness_requires_the_exact_byte() -> std::io::Result<()> {
        for byte in [0, 1, 2, b'R', b'0'] {
            let (reader, mut writer) = std::io::pipe()?;
            writer.write_all(&[byte])?;
            assert_eq!(
                ready_byte(reader).await.unwrap_err().kind(),
                std::io::ErrorKind::InvalidData
            );
        }
        let (reader, writer) = std::io::pipe()?;
        drop(writer);
        assert_eq!(
            ready_byte(reader).await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        let (reader, mut writer) = std::io::pipe()?;
        writer.write_all(b"1")?;
        ready_byte(reader).await
    }

    #[tokio::test]
    async fn slirp_readiness_failure_reports_exit_status() -> std::io::Result<()> {
        let mut child = tokio::process::Command::new("/bin/false").spawn()?;
        let status = child.wait().await?;
        let (reader, writer) = std::io::pipe()?;
        drop(writer);
        let error = wait_slirp_ready(reader, &mut child).await.unwrap_err();
        assert!(error.to_string().contains(&status.to_string()));
        assert!(
            error
                .to_string()
                .contains("invalid slirp readiness byte or EOF")
        );
        Ok(())
    }

    #[tokio::test]
    async fn slirp_invalid_readiness_reports_signal_status() -> std::io::Result<()> {
        let mut child = tokio::process::Command::new("/bin/sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()?;
        child.start_kill()?;
        let status = child.wait().await?;
        let (reader, mut writer) = std::io::pipe()?;
        writer.write_all(&[2])?;
        let error = wait_slirp_ready(reader, &mut child).await.unwrap_err();
        assert!(error.to_string().contains(&status.to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn slirp_readiness_failure_does_not_wait_indefinitely_for_running_helper()
    -> std::io::Result<()> {
        let mut child = tokio::process::Command::new("/bin/sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()?;
        let (reader, mut writer) = std::io::pipe()?;
        writer.write_all(&[2])?;
        let error = wait_slirp_ready(reader, &mut child).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid slirp readiness byte or EOF")
        );
        assert!(child.try_wait()?.is_none());
        child.start_kill()?;
        child.wait().await?;
        Ok(())
    }

    #[tokio::test]
    async fn slirp_exit_after_readiness_is_reported() -> std::io::Result<()> {
        let mut child = tokio::process::Command::new("/bin/false").spawn()?;
        let status = child.wait().await?;
        let (reader, mut writer) = std::io::pipe()?;
        writer.write_all(b"1")?;
        let error = wait_slirp_ready(reader, &mut child).await.unwrap_err();
        assert!(error.to_string().contains(&status.to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn closed_gate_is_not_ready() -> std::io::Result<()> {
        let (parent, child) = gang_os::execution_gate_pair()?;
        let parent = parent.into_async()?;
        drop(child);
        assert!(parent.ready().await.is_err());
        Ok(())
    }

    #[test]
    fn abort_initiates_kills_before_closing_channels() {
        use std::cell::Cell;

        #[derive(Debug, PartialEq)]
        enum Step {
            Kill,
            Close,
        }

        let steps = Cell::new(Vec::new());
        kill_before_close(
            || {
                let mut current = steps.take();
                current.push(Step::Kill);
                steps.set(current);
            },
            || {
                let mut current = steps.take();
                current.push(Step::Close);
                steps.set(current);
            },
        );
        assert_eq!(steps.into_inner(), [Step::Kill, Step::Close]);
    }
}
