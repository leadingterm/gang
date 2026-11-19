//! Private fault actors for the production public-egress lifecycle fixture.

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::{self, File};
    use std::io::{self, Read, Write};
    use std::os::unix::net::UnixStream;
    use std::path::{Path, PathBuf};
    use std::process::ExitCode;

    use rustix::event::{PollFd, PollFlags, poll};
    use serde::{Deserialize, Serialize};

    /// A plan is trusted fixture input, stored next to the private executable copy.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) enum FaultPlan {
        NftFail {
            control: PathBuf,
        },
        NftOom {
            control: PathBuf,
            bytes: u64,
        },
        SlirpEarlyExit {
            control: PathBuf,
        },
        SlirpBadReady {
            control: PathBuf,
        },
        SlirpControlled {
            control: PathBuf,
            bytes: u64,
            nsenter: PathBuf,
            ip: PathBuf,
            timeout: std::time::Duration,
        },
        Payload {
            marker: PathBuf,
            control: Option<PathBuf>,
            uid: u32,
            gid: u32,
        },
    }

    fn inherited_argument(name: &str) -> io::Result<File> {
        let prefix = format!("--{name}=");
        let fd = std::env::args()
            .skip(1)
            .find_map(|arg| arg.strip_prefix(&prefix).map(str::to_owned))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}")))?
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        Ok(File::from(gang_os::duplicate_inherited_fd(fd)?))
    }

    fn connect(path: &Path) -> io::Result<UnixStream> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "relative fault control path",
            ));
        }
        UnixStream::connect(path)
    }

    fn allocate(bytes: u64) -> io::Result<()> {
        let mut retained = Vec::new();
        let mut remaining = bytes;
        while remaining > 0 {
            let size = usize::try_from(remaining.min(1024 * 1024))
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let mut chunk = vec![0u8; size];
            for offset in (0..size).step_by(4096) {
                chunk[offset] = 1;
            }
            retained.push(chunk);
            remaining -= size as u64;
            std::hint::black_box(&retained);
        }
        Err(io::Error::other("allocation completed without kernel OOM"))
    }

    fn nft_fail(control: &Path) -> io::Result<()> {
        io::copy(&mut io::stdin().lock(), &mut io::sink())?;
        connect(control)?.write_all(b"B")?;
        Err(io::Error::other("injected nft failure"))
    }

    fn nft_oom(control: &Path, bytes: u64) -> io::Result<()> {
        io::copy(&mut io::stdin().lock(), &mut io::sink())?;
        let mut stream = connect(control)?;
        stream.write_all(b"B")?;
        let mut command = [0];
        stream.read_exact(&mut command)?;
        if command != *b"O" {
            return Err(io::Error::other("expected allocation command"));
        }
        allocate(bytes)
    }

    fn slirp_early_exit(control: &Path) -> io::Result<()> {
        let _ready = inherited_argument("ready-fd")?;
        let _exit = inherited_argument("exit-fd")?;
        connect(control)?.write_all(b"B")?;
        Ok(())
    }

    fn slirp_bad_ready(control: &Path) -> io::Result<()> {
        let mut ready = inherited_argument("ready-fd")?;
        let _exit = inherited_argument("exit-fd")?;
        let mut stream = connect(control)?;
        ready.write_all(b"0")?;
        stream.write_all(b"B")
    }

    fn create_tap(nsenter: &Path, ip: &Path, timeout: std::time::Duration) -> io::Result<()> {
        if !nsenter.is_absolute() || !ip.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tap tools must be absolute",
            ));
        }
        let args: Vec<_> = std::env::args().skip(1).collect();
        let userns = args
            .iter()
            .find_map(|arg| arg.strip_prefix("--userns-path="))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing slirp user namespace")
            })?;
        let [.., netns, tap] = args.as_slice() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing slirp network namespace or tap",
            ));
        };
        if !Path::new(userns).is_absolute() || !Path::new(netns).is_absolute() || tap != "tap0" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid slirp namespace paths or tap",
            ));
        }
        for ip_args in [
            &["tuntap", "add", "dev", "tap0", "mode", "tap"][..],
            &["link", "set", "tap0", "up"][..],
        ] {
            let mut command = Command::new(nsenter);
            command
                .env_clear()
                .arg(format!("--user={userns}"))
                .arg(format!("--net={netns}"))
                .arg("--preserve-credentials")
                .arg("--")
                .arg(ip)
                .args(ip_args)
                .stdin(Stdio::null());
            let output = crate::process::run(command, None, timeout)
                .map_err(|error| io::Error::other(format!("creating slirp tap: {error}")))?;
            if !output.succeeded() {
                return Err(io::Error::other(format!(
                    "slirp tap command exited with {:?}: {}",
                    output.code, output.stderr
                )));
            }
        }
        Ok(())
    }

    fn slirp_controlled(
        control: &Path,
        bytes: u64,
        nsenter: &Path,
        ip: &Path,
        timeout: std::time::Duration,
    ) -> io::Result<()> {
        let mut ready = inherited_argument("ready-fd")?;
        let mut exit = inherited_argument("exit-fd")?;
        let mut stream = connect(control)?;
        stream.write_all(b"B")?;
        loop {
            let mut fds = [
                PollFd::new(&stream, PollFlags::IN),
                PollFd::new(&exit, PollFlags::IN),
            ];
            poll(&mut fds, None)?;
            let control_events = fds[0].revents();
            let exit_events = fds[1].revents();
            if !exit_events.is_empty() {
                let mut byte = [0];
                if exit.read(&mut byte)? == 0 {
                    return Ok(());
                }
            }
            if !control_events.is_empty() {
                let mut command = [0];
                if stream.read(&mut command)? == 0 {
                    return Err(io::Error::other("control closed"));
                }
                match command[0] {
                    b'R' => {
                        create_tap(nsenter, ip, timeout)?;
                        ready.write_all(b"1")?;
                    }
                    b'E' => return Err(io::Error::other("injected slirp exit")),
                    b'O' => return allocate(bytes),
                    _ => return Err(io::Error::other("unknown slirp command")),
                }
            }
        }
    }

    fn payload(marker: &Path, control: Option<PathBuf>, uid: u32, gid: u32) -> io::Result<()> {
        if std::env::var_os("GANG_SIDE_CHANNEL_FILE").is_some() {
            return Err(io::Error::other(
                "trusted side-channel path leaked to payload",
            ));
        }
        if rustix::process::getuid().as_raw() != uid || rustix::process::getgid().as_raw() != gid {
            return Err(io::Error::other(
                "payload identity differs from fixture identity",
            ));
        }
        if !marker.is_absolute() {
            return Err(io::Error::other("relative payload marker"));
        }
        fs::write(marker, b"started")?;
        let mut retained = vec![0u8; 1024 * 1024];
        for offset in (0..retained.len()).step_by(4096) {
            retained[offset] = 1;
        }
        std::hint::black_box(&retained);
        if let Some(path) = control {
            let mut stream = connect(&path)?;
            stream.write_all(b"R")?;
            let mut command = [0];
            stream.read_exact(&mut command)?;
            if command != *b"E" {
                return Err(io::Error::other("unknown payload command"));
            }
        }
        std::hint::black_box(retained);
        Ok(())
    }

    fn actor(plan: FaultPlan) -> io::Result<()> {
        match plan {
            FaultPlan::NftFail { control } => nft_fail(&control),
            FaultPlan::NftOom { control, bytes } => nft_oom(&control, bytes),
            FaultPlan::SlirpEarlyExit { control } => slirp_early_exit(&control),
            FaultPlan::SlirpBadReady { control } => slirp_bad_ready(&control),
            FaultPlan::SlirpControlled {
                control,
                bytes,
                nsenter,
                ip,
                timeout,
            } => slirp_controlled(&control, bytes, &nsenter, &ip, timeout),
            FaultPlan::Payload {
                marker,
                control,
                uid,
                gid,
            } => payload(&marker, control, uid, gid),
        }
    }

    pub(super) fn fault_helper_main() -> Option<ExitCode> {
        let executable = std::env::current_exe().ok()?;
        let name = executable.file_name()?.to_str()?;
        if name != "gang-egress-fault-helper" && name != "gang-egress-fault-payload" {
            return None;
        }
        let result = (|| -> io::Result<()> {
            let plan: FaultPlan =
                serde_json::from_slice(&fs::read(executable.with_extension("json"))?)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            match (&plan, name) {
                (FaultPlan::Payload { .. }, "gang-egress-fault-payload") => {}
                (FaultPlan::Payload { .. }, _) | (_, "gang-egress-fault-payload") => {
                    return Err(io::Error::other("fault plan does not match executable"));
                }
                _ => {}
            }
            actor(plan)
        })();
        Some(match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("egress fault actor: {error}");
                ExitCode::FAILURE
            }
        })
    }
    use std::ffi::OsString;
    use std::net::IpAddr;
    use std::os::unix::fs::symlink;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use gang_main::plugins::{LimitsConfig, Plugins, SandboxConfig, SandboxKey};
    use gang_spi::{ActionLimitsPort, Plugin, PluginContext};
    use gang_types::{AbsolutePathBuf, Bytes, Capability, FleetActionPolicy, Url};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{UnixListener, UnixStream as AsyncUnixStream};

    use crate::check::{Check, Report};
    use crate::egress_wire::EgressTargets;
    use crate::errors::{E2eError, Result};
    use crate::settings::DeploymentInputs;

    const MIB: u64 = 1024 * 1024;
    const LIFECYCLE_RESERVE: u64 = 192 * MIB;
    const LIFECYCLE_POOL: u64 = 64 * MIB;
    const LIFECYCLE_SCOPE: u64 = LIFECYCLE_RESERVE + LIFECYCLE_POOL;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum CaseKind {
        MissingNft,
        NftFail,
        SlirpEarlyExit,
        SlirpBadReady,
        CancelDuringSetup,
        NftSetupOom,
        ActionFirst,
        SlirpFirst,
        PoolOom,
    }

    struct DriverContext<'a> {
        inputs: &'a DeploymentInputs,
        targets: &'a EgressTargets,
        root: &'a Path,
        config: &'a gang_cgroup::CgroupConfig,
        controller: &'a gang_cgroup::Cgroup,
        pool: &'a Path,
        allocation: u64,
    }

    fn json_write(path: &Path, value: &impl Serialize) -> Result<()> {
        let data = serde_json::to_vec(value).map_err(E2eError::parse("lifecycle JSON"))?;
        fs::write(path, data).map_err(E2eError::io("writing lifecycle JSON"))
    }

    fn json_read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
        let data = fs::read(path).map_err(E2eError::io("reading lifecycle JSON"))?;
        serde_json::from_slice(&data).map_err(E2eError::parse("lifecycle JSON"))
    }

    #[derive(Deserialize)]
    struct IpInterface {
        addr_info: Vec<IpAddress>,
    }

    #[derive(Deserialize)]
    struct IpAddress {
        local: IpAddr,
        prefixlen: u8,
    }

    fn parse_interface_addresses(json: &str) -> Result<Vec<IpAddr>> {
        let interfaces: Vec<IpInterface> =
            serde_json::from_str(json).map_err(E2eError::parse("ip address JSON"))?;
        let mut addresses = Vec::new();
        for address in interfaces
            .into_iter()
            .flat_map(|interface| interface.addr_info)
        {
            let max_prefix = if address.local.is_ipv4() { 32 } else { 128 };
            if address.prefixlen > max_prefix {
                return Err(E2eError::system(format!(
                    "invalid prefix length for host address {}",
                    address.local
                )));
            }
            if !addresses.contains(&address.local) {
                addresses.push(address.local);
            }
        }
        Ok(addresses)
    }

    fn host_policy(inputs: &DeploymentInputs, targets: &mut EgressTargets) -> Result<()> {
        let tools = inputs
            .action_tools
            .egress
            .as_ref()
            .ok_or_else(|| E2eError::system("lifecycle egress tools missing"))?;
        if !tools.ip.is_absolute() {
            return Err(E2eError::system("lifecycle ip program is not absolute"));
        }
        let mut command = Command::new(&tools.ip);
        command.env_clear().args(["-j", "address", "show"]);
        let output = crate::process::run(command, None, inputs.settings.command_timeout)?;
        if !output.succeeded() {
            return Err(E2eError::system(format!(
                "querying host interface addresses failed: {}",
                output.stderr
            )));
        }
        let mut networks = targets.policy.protected_networks.to_vec();
        for address in parse_interface_addresses(&output.stdout)? {
            let prefix = if address.is_ipv4() { 32 } else { 128 };
            let network = format!("{address}/{prefix}")
                .parse()
                .map_err(E2eError::parse("host address network"))?;
            if !networks.contains(&network) {
                networks.push(network);
            }
        }
        targets.policy.protected_networks = networks.into_boxed_slice();
        if targets.policy.validate().is_err() {
            for candidate in ["1.1.1.1", "8.8.8.8", "9.9.9.9", "2606:4700:4700::1111"] {
                let address = candidate
                    .parse()
                    .map_err(E2eError::parse("lifecycle DNS address"))?;
                targets.policy.dns_servers = vec![address].into_boxed_slice();
                if targets.policy.validate().is_ok() {
                    return Ok(());
                }
            }
            return Err(E2eError::system(
                "no lifecycle DNS candidate is public and outside protected networks",
            ));
        }
        Ok(())
    }

    fn require_systemd_version(program: &str, timeout: Duration) -> Result<()> {
        fn major(version: &str) -> Option<u32> {
            let digits: String = version.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        }
        fn query(command: Command, description: &'static str, timeout: Duration) -> Result<String> {
            let result = crate::process::run(command, None, timeout)
                .map_err(E2eError::system_from(description))?;
            if !result.succeeded() {
                return Err(E2eError::system(format!(
                    "{description} failed: {}",
                    result.stderr
                )));
            }
            Ok(result.stdout)
        }
        let mut client = Command::new(program);
        client.arg("--version");
        let output = query(client, "querying systemd-run client version", timeout)?;
        let client_version = output
            .lines()
            .next()
            .and_then(|line| {
                let mut words = line.split_whitespace();
                if words.next() == Some("systemd") {
                    words.next()
                } else {
                    None
                }
            })
            .and_then(major)
            .ok_or_else(|| E2eError::system("malformed systemd-run client version"))?;
        let mut manager = Command::new(Path::new(program).with_file_name("systemctl"));
        manager.args(["--user", "show", "--property=Version", "--value"]);
        let output = query(manager, "querying systemd user-manager version", timeout)?;
        let manager_version = major(output.trim())
            .ok_or_else(|| E2eError::system("malformed systemd user-manager version"))?;
        if client_version < 255 || manager_version < 255 {
            return Err(E2eError::system(format!(
                "egress lifecycle requires systemd-run and user manager version 255 or newer for safe PIDFD scope registration (client {client_version}, manager {manager_version})"
            )));
        }
        Ok(())
    }

    pub(super) fn run(inputs: &DeploymentInputs, targets: &EgressTargets) -> Result<Report> {
        let launcher = &inputs.settings.worker_launcher;
        let program = launcher.first().ok_or_else(|| {
            E2eError::system("egress lifecycle requires a systemd-run worker launcher")
        })?;
        if Path::new(program).file_name().and_then(|s| s.to_str()) != Some("systemd-run")
            || !launcher.iter().any(|s| s == "--user")
            || !launcher.iter().any(|s| s == "--scope")
            || !launcher.iter().any(|s| s == "--property=Delegate=yes")
        {
            return Err(E2eError::system(
                "egress lifecycle requires systemd-run --user --scope with Delegate=yes",
            ));
        }
        let separator = launcher
            .iter()
            .position(|s| s == "--")
            .ok_or_else(|| E2eError::system("systemd-run launcher has no -- separator"))?;
        if separator + 1 != launcher.len()
            || launcher
                .iter()
                .any(|s| s.starts_with("--property=MemoryMax="))
        {
            return Err(E2eError::system(
                "egress lifecycle requires an unbound systemd-run launcher ending in --",
            ));
        }
        require_systemd_version(program, inputs.settings.command_timeout)?;
        let dir =
            tempfile::tempdir().map_err(E2eError::io("creating lifecycle launcher directory"))?;
        let input_path = dir.path().join("inputs.json");
        let target_path = dir.path().join("targets.json");
        json_write(&input_path, inputs)?;
        json_write(&target_path, targets)?;
        let executable =
            std::env::current_exe().map_err(E2eError::io("locating lifecycle driver"))?;
        let mut command = Command::new(program);
        command
            .args(&launcher[1..separator])
            .arg(format!("--property=MemoryMax={LIFECYCLE_SCOPE}"))
            .arg("--")
            .arg(executable)
            .arg("--gang-e2e-egress-lifecycle")
            .arg(input_path)
            .arg(target_path);
        let timeout = inputs
            .settings
            .command_timeout
            .saturating_mul(12)
            .saturating_add(inputs.settings.startup_timeout);
        let output = crate::process::run(command, None, timeout)?;
        if !output.succeeded() {
            return Err(E2eError::Command {
                program: program.clone(),
                outcome: format!("exited with {:?}", output.code),
                output: output.stderr,
            });
        }
        serde_json::from_str(&output.stdout).map_err(E2eError::parse("egress lifecycle report"))
    }

    pub(super) fn private_main(args: &[OsString]) -> Option<ExitCode> {
        if args.first()?.to_str()? != "--gang-e2e-egress-lifecycle" {
            return None;
        }
        let result = (|| {
            if args.len() != 3 {
                return Err(E2eError::system("invalid egress lifecycle arguments"));
            }
            let inputs: DeploymentInputs = json_read(Path::new(&args[1]))?;
            let mut targets: EgressTargets = json_read(Path::new(&args[2]))?;
            if rustix::process::getuid().as_raw() != targets.uid
                || rustix::process::getgid().as_raw() != targets.gid
            {
                return Err(E2eError::system(
                    "lifecycle driver identity differs from fixture identity",
                ));
            }
            host_policy(&inputs, &mut targets)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(E2eError::io("building lifecycle runtime"))?;
            runtime.block_on(driver(&inputs, &targets))
        })();
        Some(match result {
            Ok(report) => match serde_json::to_writer(io::stdout().lock(), &report) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("egress lifecycle report: {error}");
                    ExitCode::FAILURE
                }
            },
            Err(error) => {
                eprintln!("egress lifecycle: {}", gang_types::error_chain(&error));
                ExitCode::FAILURE
            }
        })
    }

    async fn driver(inputs: &DeploymentInputs, targets: &EgressTargets) -> Result<Report> {
        let dir = tempfile::Builder::new()
            .prefix("gang-egress-life-")
            .tempdir_in("/tmp")
            .map_err(E2eError::io("creating lifecycle fixture"))?;
        let config = gang_cgroup::CgroupConfig {
            delegation: gang_cgroup::Delegation::Trusted,
            cgroup_root: AbsolutePathBuf::try_new(inputs.settings.cgroup_root.clone())
                .map_err(E2eError::system_from("invalid cgroup root"))?,
            ..Default::default()
        };
        let config_path = dir.path().join("sandbox.toml");
        let controller = gang_cgroup::Cgroup::build(
            config.clone(),
            &PluginContext {
                config_path: &config_path,
            },
        )
        .map_err(E2eError::system_from("building lifecycle cgroup"))?;
        let pool = controller
            .prepare(Bytes::new(LIFECYCLE_RESERVE))
            .await
            .map_err(E2eError::system_from(
                "preparing delegated lifecycle cgroup",
            ))?
            .ok_or_else(|| E2eError::system("cgroup preparation returned no action pool"))?;
        let capacity = pool.capacity.as_u64();
        if capacity > LIFECYCLE_POOL {
            return Err(E2eError::system(
                "lifecycle action pool exceeds 64 MiB safety bound",
            ));
        }
        let allocation = capacity
            .checked_mul(2)
            .ok_or_else(|| E2eError::system("lifecycle pool capacity overflow"))?;
        let context = DriverContext {
            inputs,
            targets,
            root: dir.path(),
            config: &config,
            controller: &controller,
            pool: &pool.cgroup,
            allocation,
        };
        let mut report = Report::default();
        for (index, (kind, name)) in [
            (CaseKind::MissingNft, "missing nft"),
            (CaseKind::NftFail, "failing nft"),
            (CaseKind::SlirpEarlyExit, "early slirp EOF"),
            (CaseKind::SlirpBadReady, "invalid slirp readiness"),
            (CaseKind::CancelDuringSetup, "cancel during slirp setup"),
            (CaseKind::NftSetupOom, "nft setup OOM"),
            (CaseKind::ActionFirst, "action exits first"),
            (CaseKind::SlirpFirst, "slirp exits first"),
            (CaseKind::PoolOom, "post-release pool OOM"),
        ]
        .into_iter()
        .enumerate()
        {
            let result = case(&context, index, kind).await;
            let check = match result {
                Ok(()) => Check::passed(format!("egress lifecycle: {name}")),
                Err(error) => Check::failed(
                    format!("egress lifecycle: {name}"),
                    gang_types::error_chain(&error).to_string(),
                ),
            };
            report.extend([check]);
        }
        Ok(report)
    }

    fn copied_actor(
        source: &Path,
        directory: &Path,
        name: &str,
        plan: &FaultPlan,
    ) -> Result<PathBuf> {
        fs::create_dir_all(directory).map_err(E2eError::io("creating fault actor directory"))?;
        let binary = directory.join(name);
        fs::copy(source, &binary).map_err(E2eError::io("copying fault actor"))?;
        json_write(&binary.with_extension("json"), plan)?;
        Ok(binary)
    }

    async fn receive(
        listener: &UnixListener,
        expected: u8,
        child: &mut tokio::process::Child,
    ) -> Result<AsyncUnixStream> {
        tokio::select! {
            biased;
            result = async {
                let (mut stream, _) = listener.accept().await.map_err(E2eError::io("accepting fault actor"))?;
                let byte = stream.read_u8().await.map_err(E2eError::io("reading fault actor event"))?;
                if byte != expected { return Err(E2eError::system(format!("unexpected fault actor event {byte}"))); }
                Ok(stream)
            } => result,
            status = child.wait() => {
                let status = status.map_err(E2eError::io("waiting for lifecycle gang-exec"))?;
                Err(E2eError::system(format!("gang-exec exited with {status} before fault actor event {expected}")))
            }
        }
    }

    async fn command(stream: &mut AsyncUnixStream, value: u8) -> Result<()> {
        stream
            .write_all(&[value])
            .await
            .map_err(E2eError::io("sending fault actor command"))
    }

    async fn closed(stream: &mut AsyncUnixStream) -> Result<()> {
        let mut byte = [0];
        if stream
            .read(&mut byte)
            .await
            .map_err(E2eError::io("reading fault actor closure"))?
            != 0
        {
            return Err(E2eError::system(
                "fault actor sent unexpected data instead of closing",
            ));
        }
        Ok(())
    }
    struct CaseFixture {
        dir: PathBuf,
        work_root: PathBuf,
        work: PathBuf,
        side: PathBuf,
        exec: PathBuf,
        marker: PathBuf,
        payload: PathBuf,
        nft: PathBuf,
        slirp: PathBuf,
        helper_listener: Option<UnixListener>,
        payload_listener: Option<UnixListener>,
    }

    fn helper_programs(
        tools: &crate::settings::EgressTools,
        dir: &Path,
        helper_socket: &Path,
        kind: CaseKind,
        allocation: u64,
        timeout: Duration,
    ) -> Result<(PathBuf, PathBuf)> {
        let helper_plan = match kind {
            CaseKind::MissingNft => None,
            CaseKind::NftFail => Some(FaultPlan::NftFail {
                control: helper_socket.to_path_buf(),
            }),
            CaseKind::SlirpEarlyExit => Some(FaultPlan::SlirpEarlyExit {
                control: helper_socket.to_path_buf(),
            }),
            CaseKind::SlirpBadReady => Some(FaultPlan::SlirpBadReady {
                control: helper_socket.to_path_buf(),
            }),
            CaseKind::CancelDuringSetup
            | CaseKind::ActionFirst
            | CaseKind::SlirpFirst
            | CaseKind::PoolOom => Some(FaultPlan::SlirpControlled {
                control: helper_socket.to_path_buf(),
                bytes: allocation,
                nsenter: tools.nsenter.clone(),
                ip: tools.ip.clone(),
                timeout,
            }),
            CaseKind::NftSetupOom => Some(FaultPlan::NftOom {
                control: helper_socket.to_path_buf(),
                bytes: allocation,
            }),
        };
        let helper = helper_plan
            .as_ref()
            .map(|plan| {
                copied_actor(
                    &tools.peer,
                    &dir.join("helper"),
                    "gang-egress-fault-helper",
                    plan,
                )
            })
            .transpose()?;
        let nft = match kind {
            CaseKind::MissingNft => dir.join("missing-nft"),
            CaseKind::NftFail | CaseKind::NftSetupOom => helper
                .clone()
                .ok_or_else(|| E2eError::system("nft fault actor missing"))?,
            _ => tools.nft.clone(),
        };
        let slirp = match kind {
            CaseKind::SlirpEarlyExit
            | CaseKind::SlirpBadReady
            | CaseKind::CancelDuringSetup
            | CaseKind::ActionFirst
            | CaseKind::SlirpFirst
            | CaseKind::PoolOom => {
                helper.ok_or_else(|| E2eError::system("slirp fault actor missing"))?
            }
            _ => tools.slirp4netns.clone(),
        };
        Ok((nft, slirp))
    }

    fn prepare_case(
        context: &DriverContext<'_>,
        index: usize,
        kind: CaseKind,
    ) -> Result<CaseFixture> {
        let DriverContext {
            inputs,
            targets,
            root,
            allocation,
            ..
        } = *context;
        let dir = root.join(format!("case-{index}"));
        let work_root = dir.join("work");
        let work = work_root.join("a/work");
        let side = work_root.join(".side_channels/a");
        fs::create_dir_all(&work).map_err(E2eError::io("creating lifecycle action directory"))?;
        fs::create_dir_all(&side).map_err(E2eError::io("creating lifecycle side channel"))?;
        let exec = dir.join("gang-exec");
        symlink(&inputs.gang, &exec).map_err(E2eError::io("linking gang-exec"))?;
        symlink(&inputs.gang, dir.join("gang-join")).map_err(E2eError::io("linking gang-join"))?;
        let tools = inputs
            .action_tools
            .egress
            .as_ref()
            .ok_or_else(|| E2eError::system("lifecycle egress tools missing"))?;
        for path in [
            &inputs.gang,
            &tools.peer,
            &tools.nft,
            &tools.slirp4netns,
            &tools.nsenter,
            &tools.ip,
        ] {
            if !path.is_absolute() {
                return Err(E2eError::system(format!(
                    "lifecycle program is not absolute: {}",
                    path.display()
                )));
            }
        }
        let marker = work.join("marker");
        let payload_socket = work.join("payload.sock");
        let helper_socket = dir.join("helper.sock");
        let helper_listener = if kind == CaseKind::MissingNft {
            None
        } else {
            Some(
                UnixListener::bind(&helper_socket)
                    .map_err(E2eError::io("binding helper control"))?,
            )
        };
        let payload_listener = if matches!(kind, CaseKind::SlirpFirst | CaseKind::PoolOom) {
            Some(
                UnixListener::bind(&payload_socket)
                    .map_err(E2eError::io("binding payload control"))?,
            )
        } else {
            None
        };
        let payload = copied_actor(
            &tools.peer,
            &work,
            "gang-egress-fault-payload",
            &FaultPlan::Payload {
                marker: marker.clone(),
                control: payload_listener.as_ref().map(|_| payload_socket),
                uid: targets.uid,
                gid: targets.gid,
            },
        )?;
        let (nft, slirp) = helper_programs(
            tools,
            &dir,
            &helper_socket,
            kind,
            allocation,
            inputs.settings.command_timeout,
        )?;
        Ok(CaseFixture {
            dir,
            work_root,
            work,
            side,
            exec,
            marker,
            payload,
            nft,
            slirp,
            helper_listener,
            payload_listener,
        })
    }

    fn write_sandbox(
        context: &DriverContext<'_>,
        dir: &Path,
        work_root: PathBuf,
        nft: PathBuf,
        slirp: PathBuf,
    ) -> Result<()> {
        let DriverContext {
            inputs,
            targets,
            config: cgroup_config,
            ..
        } = *context;
        let tools = inputs
            .action_tools
            .egress
            .as_ref()
            .ok_or_else(|| E2eError::system("lifecycle egress tools missing"))?;
        let root = inputs
            .action_tools
            .action_rootfs
            .as_ref()
            .ok_or_else(|| E2eError::system("lifecycle action rootfs missing"))?;
        let root = fs::canonicalize(root)
            .map_err(E2eError::io("canonicalizing lifecycle action rootfs"))?;
        let bwrap = gang_bwrap::BwrapConfig {
            root: AbsolutePathBuf::try_new(root)
                .map_err(E2eError::system_from("invalid lifecycle rootfs"))?,
            egress: Some(gang_bwrap::EgressPrograms {
                slirp4netns: slirp,
                nft,
                nsenter: tools.nsenter.clone(),
                ip: tools.ip.clone(),
            }),
            program: inputs.action_tools.bwrap_program()?.to_path_buf(),
        };
        let mut actions = FleetActionPolicy::default();
        actions.grants.insert(Capability::Network);
        let file = gang_config::SandboxFile::<Plugins> {
            work_directory: AbsolutePathBuf::try_new(work_root)
                .map_err(E2eError::system_from("invalid action work root"))?,
            selected: SandboxKey::Bwrap,
            sandbox: SandboxConfig {
                noop: None,
                bwrap: Some(bwrap),
            },
            execution: gang_config::ExecutionConfig {
                egress: Some(targets.policy.clone()),
                ..Default::default()
            },
            actions,
            controller_endpoints: vec![
                Url::parse("http://127.0.0.1:1")
                    .map_err(E2eError::parse("lifecycle controller endpoint"))?,
            ]
            .into_boxed_slice(),
            limits: Some(LimitsConfig::Cgroup(cgroup_config.clone())),
        };
        let text = file
            .to_toml()
            .map_err(E2eError::system_from("rendering lifecycle sandbox"))?;
        fs::write(dir.join("sandbox.toml"), text)
            .map_err(E2eError::io("writing lifecycle sandbox"))?;
        Ok(())
    }

    async fn case(context: &DriverContext<'_>, index: usize, kind: CaseKind) -> Result<()> {
        let fixture = prepare_case(context, index, kind)?;
        let CaseFixture {
            dir,
            work_root,
            work,
            side,
            exec,
            marker,
            payload,
            nft,
            slirp,
            helper_listener,
            payload_listener,
        } = fixture;
        write_sandbox(context, &dir, work_root, nft, slirp)?;
        let DriverContext { inputs, pool, .. } = *context;
        let log_path = dir.join("gang-exec.stderr");
        let log =
            File::create(&log_path).map_err(E2eError::io("creating lifecycle gang-exec log"))?;
        let mut child = tokio::process::Command::new(&exec)
            .arg(payload)
            .current_dir(&work)
            .env_clear()
            .env("GANG_NETWORK", "requested")
            .env("GANG_SIDE_CHANNEL_FILE", side.join("report"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .map_err(E2eError::io("starting lifecycle gang-exec"))?;
        let creator = child
            .id()
            .ok_or_else(|| E2eError::system("gang-exec PID missing"))?;
        let timeout = inputs.settings.command_timeout;
        let result = tokio::time::timeout(
            timeout,
            case_running(
                &mut child,
                helper_listener.as_ref(),
                payload_listener.as_ref(),
                &marker,
                &side,
                pool,
                kind,
            ),
        )
        .await;
        finish_case(context, &mut child, creator, kind, &log_path, result).await
    }

    async fn finish_case(
        context: &DriverContext<'_>,
        child: &mut tokio::process::Child,
        creator: u32,
        kind: CaseKind,
        log_path: &Path,
        result: std::result::Result<Result<()>, tokio::time::error::Elapsed>,
    ) -> Result<()> {
        let DriverContext {
            inputs, controller, ..
        } = *context;
        let timeout = inputs.settings.command_timeout;
        let outcome: Result<()> = async {
            match result {
                Ok(Ok(())) if kind == CaseKind::MissingNft => {
                    let mut text = String::new();
                    File::open(log_path)
                        .map_err(E2eError::io("opening missing-nft diagnostics"))?
                        .take(crate::process::OUTPUT_LIMIT.as_u64().min(MIB))
                        .read_to_string(&mut text)
                        .map_err(E2eError::io("reading missing-nft diagnostics"))?;
                    if !text.contains("missing-nft") || !text.contains("No such file or directory")
                    {
                        return Err(E2eError::system(
                            "missing nft did not report its ENOENT in network setup diagnostics",
                        ));
                    }
                    Ok(())
                }
                Ok(Ok(())) => Ok(()),
                failure => {
                    if child.id().is_some() {
                        child
                            .start_kill()
                            .map_err(E2eError::io("killing failed lifecycle gang-exec"))?;
                    }
                    tokio::time::timeout(timeout, child.wait())
                        .await
                        .map_err(|_| E2eError::timeout("reaping lifecycle gang-exec", timeout))?
                        .map_err(E2eError::io("reaping lifecycle gang-exec"))?;
                    tokio::time::timeout(timeout, controller.reap_abandoned(creator))
                        .await
                        .map_err(|_| {
                            E2eError::timeout("reaping abandoned lifecycle scope", timeout)
                        })?
                        .map_err(E2eError::system_from("reaping abandoned lifecycle scope"))?;
                    match failure {
                        Ok(Err(error)) => Err(error),
                        Err(_) => Err(E2eError::timeout("egress lifecycle case", timeout)),
                        Ok(Ok(())) => Ok(()),
                    }
                }
            }
        }
        .await;
        match outcome {
            Ok(()) => Ok(()),
            Err(error) => {
                let excerpt = (|| -> io::Result<Vec<u8>> {
                    let mut bytes = Vec::new();
                    let limit = crate::process::OUTPUT_LIMIT
                        .as_u64()
                        .min(u64::try_from(inputs.settings.log_tail_bytes).unwrap_or(u64::MAX));
                    File::open(log_path)?.take(limit).read_to_end(&mut bytes)?;
                    Ok(bytes)
                })();
                let context = match excerpt {
                    Ok(bytes) => format!(
                        "gang-exec stderr (bounded): {}",
                        String::from_utf8_lossy(&bytes)
                    ),
                    Err(read_error) => format!("cannot read gang-exec stderr: {read_error}"),
                };
                Err(E2eError::system_from(context)(error))
            }
        }
    }

    async fn case_running(
        child: &mut tokio::process::Child,
        helper_listener: Option<&UnixListener>,
        payload_listener: Option<&UnixListener>,
        marker: &Path,
        side: &Path,
        pool: &Path,
        kind: CaseKind,
    ) -> Result<()> {
        let mut helper = match helper_listener {
            Some(listener) => Some(receive(listener, b'B', child).await?),
            None => None,
        };
        let mut payload = drive_case(child, &mut helper, payload_listener, marker, kind).await?;
        let status = child
            .wait()
            .await
            .map_err(E2eError::io("waiting for lifecycle gang-exec"))?;
        if let Some(stream) = helper.as_mut() {
            closed(stream).await?;
        }
        if let Some(stream) = payload.as_mut() {
            closed(stream).await?;
        }
        verify_report(status, marker, side, pool, kind)
    }

    async fn drive_case(
        child: &mut tokio::process::Child,
        helper: &mut Option<AsyncUnixStream>,
        payload_listener: Option<&UnixListener>,
        marker: &Path,
        kind: CaseKind,
    ) -> Result<Option<AsyncUnixStream>> {
        if matches!(kind, CaseKind::CancelDuringSetup | CaseKind::NftSetupOom)
            && marker
                .try_exists()
                .map_err(E2eError::io("checking gated payload marker"))?
        {
            return Err(E2eError::system("payload ran before the setup gate opened"));
        }
        if kind == CaseKind::CancelDuringSetup {
            let raw = child
                .id()
                .ok_or_else(|| E2eError::system("gang-exec PID missing"))?;
            let raw = i32::try_from(raw).map_err(E2eError::system_from("invalid gang-exec PID"))?;
            let pid = rustix::process::Pid::from_raw(raw)
                .ok_or_else(|| E2eError::system("invalid gang-exec PID"))?;
            rustix::process::kill_process(pid, rustix::process::Signal::TERM)
                .map_err(E2eError::system_from("signalling gang-exec"))?;
        } else if kind == CaseKind::NftSetupOom {
            command(
                helper
                    .as_mut()
                    .ok_or_else(|| E2eError::system("nft control missing"))?,
                b'O',
            )
            .await?;
        } else if matches!(
            kind,
            CaseKind::ActionFirst | CaseKind::SlirpFirst | CaseKind::PoolOom
        ) {
            command(
                helper
                    .as_mut()
                    .ok_or_else(|| E2eError::system("slirp control missing"))?,
                b'R',
            )
            .await?;
        }
        let payload = match payload_listener {
            Some(listener) => Some(receive(listener, b'R', child).await?),
            None => None,
        };
        if matches!(kind, CaseKind::SlirpFirst | CaseKind::PoolOom) {
            command(
                helper
                    .as_mut()
                    .ok_or_else(|| E2eError::system("slirp control missing"))?,
                if kind == CaseKind::SlirpFirst {
                    b'E'
                } else {
                    b'O'
                },
            )
            .await?;
        }
        Ok(payload)
    }

    fn verify_report(
        status: std::process::ExitStatus,
        marker: &Path,
        side: &Path,
        pool: &Path,
        kind: CaseKind,
    ) -> Result<()> {
        let marker_exists = marker
            .try_exists()
            .map_err(E2eError::io("checking payload marker"))?;
        if marker_exists
            != matches!(
                kind,
                CaseKind::ActionFirst | CaseKind::SlirpFirst | CaseKind::PoolOom
            )
        {
            return Err(E2eError::system(
                "payload marker disagrees with gate outcome",
            ));
        }
        match kind {
            CaseKind::CancelDuringSetup if status.code() != Some(143) => {
                return Err(E2eError::system(format!(
                    "cancellation exited with {status}, expected 143"
                )));
            }
            CaseKind::ActionFirst if !status.success() => {
                return Err(E2eError::system(format!(
                    "action-first exited with {status}"
                )));
            }
            CaseKind::SlirpFirst if status.code() != Some(137) => {
                return Err(E2eError::system(format!(
                    "helper-first exited with {status}, expected killed outcome 137"
                )));
            }
            CaseKind::MissingNft
            | CaseKind::NftFail
            | CaseKind::SlirpEarlyExit
            | CaseKind::SlirpBadReady
            | CaseKind::NftSetupOom
            | CaseKind::SlirpFirst
            | CaseKind::PoolOom
                if status.success() || status.code().is_none() =>
            {
                return Err(E2eError::system(format!(
                    "expected ordinary nonzero gang-exec exit, got {status}"
                )));
            }
            _ => {}
        }
        let cgroup = PathBuf::from(
            fs::read_to_string(side.join("report.cgroup"))
                .map_err(E2eError::io("reading actual action cgroup"))?,
        );
        if !cgroup.is_absolute() || cgroup.parent() != Some(pool) {
            return Err(E2eError::system(
                "reported action cgroup is outside the prepared pool",
            ));
        }
        if cgroup.exists() {
            let members = fs::read_to_string(cgroup.join("cgroup.procs"))
                .map_err(E2eError::io("reading remaining action processes"))?;
            if !members.trim().is_empty() {
                return Err(E2eError::system("action cgroup still has processes"));
            }
        }
        let usage: serde_json::Value = json_read(&side.join("report"))?;
        if usage
            .get("peak_memory_kb")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
            == 0
        {
            return Err(E2eError::system(
                "actual side channel has no measured peak memory",
            ));
        }
        let memory_failure =
            usage.get("failure").and_then(serde_json::Value::as_str) == Some("memory");
        if memory_failure != matches!(kind, CaseKind::NftSetupOom | CaseKind::PoolOom) {
            return Err(E2eError::system(format!(
                "unexpected action memory outcome: {usage}"
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::parse_interface_addresses;
        use std::net::IpAddr;

        #[test]
        fn parses_all_host_interface_addresses() {
            let json = r#"[
                {"ifname":"lo","addr_info":[{"local":"127.0.0.1","prefixlen":8},{"local":"::1","prefixlen":128}]},
                {"ifname":"eth0","addr_info":[{"local":"192.168.2.240","prefixlen":24},{"local":"2001:db8::1","prefixlen":64},{"local":"127.0.0.1","prefixlen":8}]},
                {"ifname":"empty","addr_info":[]}
            ]"#;
            let expected: Vec<IpAddr> = ["127.0.0.1", "::1", "192.168.2.240", "2001:db8::1"]
                .into_iter()
                .map(|address| address.parse().unwrap())
                .collect();
            assert_eq!(parse_interface_addresses(json).unwrap(), expected);
            assert!(
                parse_interface_addresses(r#"[{"addr_info":[{"local":"::1","prefixlen":129}]}]"#)
                    .is_err()
            );
            assert!(
                parse_interface_addresses(r#"[{"addr_info":[{"local":"bad","prefixlen":24}]}]"#)
                    .is_err()
            );
        }

        #[test]
        fn control_wait_reports_early_child_exit() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let listener =
                    tokio::net::UnixListener::bind(dir.path().join("control.sock")).unwrap();
                let mut child = tokio::process::Command::new("/bin/sh")
                    .args(["-c", "exit 23"])
                    .spawn()
                    .unwrap();
                let error = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    super::receive(&listener, b'B', &mut child),
                )
                .await
                .unwrap()
                .unwrap_err();
                assert!(
                    error.to_string().contains("before fault actor event"),
                    "{error}"
                );
                assert!(error.to_string().contains("23"), "{error}");
            });
        }
    }
}

/// Launches lifecycle checks from the host harness in a freshly delegated systemd scope.
///
/// # Errors
/// Returns an error when the required launcher or lifecycle driver cannot run.
pub fn run(
    inputs: &crate::DeploymentInputs,
    targets: &crate::EgressTargets,
) -> crate::Result<crate::Report> {
    #[cfg(target_os = "linux")]
    {
        linux::run(inputs, targets)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (inputs, targets);
        Err(crate::E2eError::system("egress lifecycle requires Linux"))
    }
}

/// Handles the private lifecycle driver command.
#[must_use]
pub fn private_main(args: &[std::ffi::OsString]) -> Option<std::process::ExitCode> {
    #[cfg(target_os = "linux")]
    {
        linux::private_main(args)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = args;
        None
    }
}

/// Dispatches only the fixture's private copied executable names.
#[must_use]
pub fn fault_helper_main() -> Option<std::process::ExitCode> {
    #[cfg(target_os = "linux")]
    {
        linux::fault_helper_main()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
