//! Rootless, disconnected network fixture for the public-egress scenario.

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::fs;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, UdpSocket};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitCode, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use crate::check::{Check, Report};
    use crate::egress_wire::{EgressTargets, PeerCommand, PeerMessage};
    use crate::errors::{E2eError, Result};
    use crate::settings::{DeploymentInputs, EgressTools};

    // The action rootfs sandbox maps the worker identity to these IDs.
    const ACTION_UID: u32 = 1000;
    const ACTION_GID: u32 = 1000;

    fn command(program: &Path) -> Command {
        let mut command = Command::new(program);
        command.env_clear();
        for key in [
            "HOME",
            "XDG_RUNTIME_DIR",
            "DBUS_SESSION_BUS_ADDRESS",
            "PATH",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        command
    }

    fn tools(inputs: &DeploymentInputs) -> Result<&EgressTools> {
        let tools = inputs
            .action_tools
            .egress
            .as_ref()
            .ok_or_else(|| E2eError::system("egress fixture tools are missing"))?;
        for path in [
            &tools.slirp4netns,
            &tools.nft,
            &tools.nsenter,
            &tools.ip,
            &tools.unshare,
            &tools.probe,
            &tools.peer,
        ] {
            if !path.is_absolute() || !path.is_file() {
                return Err(E2eError::system(format!(
                    "egress fixture tool is missing or not absolute: {}",
                    path.display()
                )));
            }
        }
        Ok(tools)
    }

    fn deadline(inputs: &DeploymentInputs) -> Duration {
        inputs
            .settings
            .build_timeout
            .saturating_mul(12)
            .saturating_add(inputs.settings.startup_timeout.saturating_mul(8))
            .saturating_add(inputs.settings.command_timeout.saturating_mul(16))
    }

    fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(E2eError::parse("fixture JSON"))?;
        fs::write(path, bytes).map_err(E2eError::io("writing fixture JSON"))
    }
    fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
        let bytes = fs::read(path).map_err(E2eError::io("reading fixture JSON"))?;
        serde_json::from_slice(&bytes).map_err(E2eError::parse("fixture JSON"))
    }
    fn executable() -> Result<PathBuf> {
        std::env::current_exe().map_err(E2eError::io("locating fixture executable"))
    }
    fn run(command: Command, timeout: Duration) -> Result<String> {
        let program = command.get_program().to_string_lossy().into_owned();
        let result = crate::process::run(command, None, timeout)?;
        if !result.succeeded() {
            return Err(E2eError::Command {
                program,
                outcome: format!("exited with {:?}", result.code),
                output: "diagnostics withheld to protect fixture inputs".into(),
            });
        }
        Ok(result.stdout)
    }

    fn host_identity() -> Result<(u32, u32)> {
        let uid = rustix::process::getuid().as_raw();
        let gid = rustix::process::getgid().as_raw();
        if uid == 0 || gid == 0 {
            return Err(E2eError::system(
                "egress fixture requires a non-root user and group",
            ));
        }
        Ok((uid, gid))
    }

    pub(crate) fn lifecycle_targets(inputs: &DeploymentInputs) -> Result<EgressTargets> {
        host_identity()?;
        let timeout = inputs.settings.handshake_timeout;
        if timeout.is_zero() {
            return Err(E2eError::system(
                "fixture handshake timeout must be nonzero",
            ));
        }
        tools(inputs)?;
        plan(timeout)
    }

    fn prefetch(inputs: &mut DeploymentInputs, root: &Path) -> Result<()> {
        if !inputs.sample.join("MODULE.bazel.lock").is_file() {
            return Err(E2eError::system(
                "offline egress fixture requires the sample's checked-in MODULE.bazel.lock",
            ));
        }
        let targets = lifecycle_targets(inputs)?;
        let tools = inputs
            .action_tools
            .egress
            .as_mut()
            .ok_or_else(|| E2eError::system("egress fixture tools are missing"))?;
        tools.targets = Some(targets);
        let template = crate::template::ConfigTemplate::parse(&inputs.base_config)?;
        let salt = crate::sample::salt()?;
        let parameters = crate::stack::sample_parameters(
            inputs,
            &crate::system::Variant::baseline(crate::system::Sandbox::Bwrap),
            &salt,
            &root.join("prefetch-signal"),
            template.default_isa()?,
        );
        let workspace = root.join("prefetch-workspace");
        crate::sample::stage(&inputs.sample, &workspace, &parameters)?;
        let cache = root.join("repository-cache");
        fs::create_dir(&cache).map_err(E2eError::io("creating fixture repository cache"))?;
        // Only this host-namespace prefetch may access the internet; fixture builds replay offline.
        crate::bazel::Bazel::new(
            inputs.bazel.clone(),
            inputs.settings.clone(),
            inputs.action_tools.clone(),
            workspace,
            root.join("prefetch-out"),
        )
        .prefetch(&cache)?;
        inputs
            .action_tools
            .egress
            .as_mut()
            .ok_or_else(|| E2eError::system("egress fixture tools are missing"))?
            .repository_cache = Some(cache);
        Ok(())
    }

    /// Runs the Linux fixture in an isolated process tree.
    ///
    /// # Errors
    ///
    /// Returns an error if fixture setup, execution, or report parsing fails.
    pub fn launch(inputs: &DeploymentInputs) -> Result<Report> {
        let tools = tools(inputs)?;
        let (uid, gid) = host_identity()?;
        let dir =
            tempfile::tempdir().map_err(E2eError::io("creating private fixture directory"))?;
        let mut prepared = inputs.clone();
        prefetch(&mut prepared, dir.path()).map_err(E2eError::system_from(
            "prefetching Bazel repositories for the offline egress fixture",
        ))?;
        let input_path = dir.path().join("inputs.json");
        write_json(&input_path, &prepared)?;
        let mut cmd = command(&tools.unshare);
        cmd.args([
            "--user",
            "--map-root-user",
            "--mount",
            "--net",
            "--pid",
            "--fork",
            "--kill-child=KILL",
            "--mount-proc",
            "--",
        ])
        .arg(executable()?)
        .arg("--gang-e2e-egress-root")
        .arg(input_path)
        .arg(uid.to_string())
        .arg(gid.to_string());
        let output = run(cmd, deadline(inputs))?;
        serde_json::from_str(&output).map_err(E2eError::parse("fixture report"))
    }

    /// Dispatches a private fixture mode, if one is present in the arguments.
    #[must_use]
    pub fn private_main(args: &[OsString]) -> Option<ExitCode> {
        let mode = args.first()?.to_str()?;
        if mode != "--gang-e2e-egress-root" && mode != "--gang-e2e-egress-worker" {
            return None;
        }
        let result = if mode == "--gang-e2e-egress-root" {
            if args.len() == 4 {
                let ids = args[2]
                    .to_str()
                    .and_then(|s| s.parse::<u32>().ok())
                    .zip(args[3].to_str().and_then(|s| s.parse::<u32>().ok()));
                match ids {
                    Some((uid, gid)) if uid != 0 && gid != 0 => root(Path::new(&args[1]), uid, gid),
                    _ => Err(E2eError::system("invalid egress identity")),
                }
            } else {
                Err(E2eError::system("invalid egress root arguments"))
            }
        } else if args.len() == 3 {
            (|| {
                let inputs = read_json(Path::new(&args[1]))?;
                let targets = read_json(Path::new(&args[2]))?;
                Ok(crate::suite::egress_worker(&inputs, &targets))
            })()
        } else {
            Err(E2eError::system("invalid egress worker arguments"))
        };
        Some(match result {
            Ok(report) => match serde_json::to_writer(std::io::stdout().lock(), &report) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("egress fixture: {error}");
                    ExitCode::FAILURE
                }
            },
            Err(error) => {
                eprintln!("egress fixture: {}", gang_types::error_chain(&error));
                ExitCode::FAILURE
            }
        })
    }

    struct OwnedChild(Child);
    impl Drop for OwnedChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    struct Peer {
        child: OwnedChild,
        stream: UnixStream,
        reader: BufReader<UnixStream>,
    }
    impl Peer {
        fn recv(&mut self) -> Result<PeerMessage> {
            let mut line = String::new();
            if self
                .reader
                .read_line(&mut line)
                .map_err(E2eError::io("reading peer control"))?
                == 0
                || line.len() > 65536
            {
                return Err(E2eError::system("invalid peer control response"));
            }
            serde_json::from_str(&line).map_err(E2eError::parse("peer control response"))
        }
        fn send(&mut self, message: &PeerCommand) -> Result<PeerMessage> {
            serde_json::to_writer(&mut self.stream, message)
                .map_err(E2eError::parse("peer command"))?;
            self.stream
                .write_all(b"\n")
                .map_err(E2eError::io("writing peer control"))?;
            self.recv()
        }
    }
    fn spawn_peer(tools: &EgressTools, targets: &EgressTargets) -> Result<Peer> {
        let (parent, child) = UnixStream::pair().map_err(E2eError::io("creating peer control"))?;
        parent
            .set_read_timeout(Some(targets.timeout))
            .map_err(E2eError::io("setting peer timeout"))?;
        parent
            .set_write_timeout(Some(targets.timeout))
            .map_err(E2eError::io("setting peer timeout"))?;
        let reader = BufReader::new(
            parent
                .try_clone()
                .map_err(E2eError::io("cloning peer control"))?,
        );
        let json = serde_json::to_string(targets).map_err(E2eError::parse("peer targets"))?;
        let mut cmd = command(&tools.unshare);
        cmd.args(["--net", "--"])
            .arg(&tools.peer)
            .arg("--control-fd")
            .arg(child.as_raw_fd().to_string())
            .arg("--targets")
            .arg(json)
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        rustix::io::fcntl_setfd(&child, rustix::io::FdFlags::empty())
            .map_err(|e| E2eError::io("inheriting peer control")(e.into()))?;
        let spawned = cmd.spawn();
        let restored = rustix::io::fcntl_setfd(&child, rustix::io::FdFlags::CLOEXEC);
        let process = spawned.map_err(E2eError::io("starting peer"))?;
        let owned = OwnedChild(process);
        restored.map_err(|e| E2eError::io("restoring peer control flags")(e.into()))?;
        drop(child);
        let mut peer = Peer {
            child: owned,
            stream: parent,
            reader,
        };
        if !matches!(peer.recv()?, PeerMessage::Booted) {
            return Err(E2eError::system("peer did not boot"));
        }
        Ok(peer)
    }
    fn ip(tools: &EgressTools, pid: Option<u32>, args: &[&str], timeout: Duration) -> Result<()> {
        let mut cmd = if let Some(pid) = pid {
            let mut cmd = command(&tools.nsenter);
            cmd.args(["--target", &pid.to_string(), "--net", "--"])
                .arg(&tools.ip);
            cmd
        } else {
            command(&tools.ip)
        };
        cmd.args(args);
        run(cmd, timeout).map(|_| ())
    }
    fn address(ip: &str) -> Result<SocketAddr> {
        format!("{ip}:0")
            .parse()
            .map_err(E2eError::parse("fixture address"))
    }
    fn plan(timeout: Duration) -> Result<EgressTargets> {
        let dns: IpAddr = "93.184.216.34"
            .parse()
            .map_err(E2eError::parse("DNS address"))?;
        let policy = gang_types::PublicEgressPolicy {
            protected_networks: [
                "10.20.0.0/16",
                "8.8.4.4/32",
                "2600:abcd:1234:5600::/56",
                "10.255.255.0/30",
                "fd42:feed::/64",
            ]
            .into_iter()
            .map(str::parse)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(E2eError::parse("protected networks"))?
            .into_boxed_slice(),
            dns_servers: vec![dns].into_boxed_slice(),
            setup_timeout: gang_types::NonZeroDuration::try_new(timeout)
                .map_err(E2eError::system_from("invalid fixture timeout"))?,
        };
        let mut blocked = BTreeMap::new();
        for (name, ip) in [
            ("imds4", "169.254.169.254"),
            ("imds6", "[fd00:ec2::254]"),
            ("vpc4", "10.20.0.10"),
            ("controller", "8.8.4.4"),
            ("vpc6", "[2600:abcd:1234:5600::10]"),
            ("testnet", "203.0.113.10"),
        ] {
            blocked.insert(name.to_string(), address(ip)?);
        }
        Ok(EgressTargets {
            public_ipv4: address("93.184.216.34")?,
            public_ipv6: address("[2001:4860:1234::34]")?,
            blocked,
            dns,
            nonce: "gang-egress-fixture".into(),
            uid: ACTION_UID,
            gid: ACTION_GID,
            timeout,
            policy,
        })
    }
    fn topology(tools: &EgressTools, pid: u32, timeout: Duration) -> Result<()> {
        for args in [
            vec!["link", "set", "lo", "up"],
            vec!["link", "add", "gw", "type", "veth", "peer", "name", "gp"],
            vec!["addr", "add", "10.255.255.1/30", "dev", "gw"],
            vec!["-6", "addr", "add", "fd42:feed::1/64", "dev", "gw", "nodad"],
            vec!["link", "set", "gw", "up"],
        ] {
            ip(tools, None, &args, timeout)?;
        }
        ip(
            tools,
            None,
            &["link", "set", "gp", "netns", &pid.to_string()],
            timeout,
        )?;
        for args in [
            vec!["link", "set", "lo", "up"],
            vec!["addr", "add", "10.255.255.2/30", "dev", "gp"],
            vec!["-6", "addr", "add", "fd42:feed::2/64", "dev", "gp", "nodad"],
            vec!["link", "set", "gp", "up"],
        ] {
            ip(tools, Some(pid), &args, timeout)?;
        }
        for addr in [
            "93.184.216.34/32",
            "169.254.169.254/32",
            "10.20.0.10/32",
            "8.8.4.4/32",
            "203.0.113.10/32",
        ] {
            ip(
                tools,
                Some(pid),
                &["addr", "add", addr, "dev", "gp"],
                timeout,
            )?;
            ip(
                tools,
                None,
                &["route", "add", addr, "via", "10.255.255.2", "dev", "gw"],
                timeout,
            )?;
        }
        for addr in [
            "2001:4860:1234::34/128",
            "fd00:ec2::254/128",
            "2600:abcd:1234:5600::10/128",
        ] {
            ip(
                tools,
                Some(pid),
                &["-6", "addr", "add", addr, "dev", "gp", "nodad"],
                timeout,
            )?;
            ip(
                tools,
                None,
                &[
                    "-6",
                    "route",
                    "add",
                    addr,
                    "via",
                    "fd42:feed::2",
                    "dev",
                    "gw",
                ],
                timeout,
            )?;
        }
        Ok(())
    }

    type Counters = BTreeMap<String, Arc<AtomicU64>>;
    fn host_listener(
        label: &str,
        ip: &str,
        targets: &EgressTargets,
        counts: &mut Counters,
    ) -> Result<SocketAddr> {
        let listener =
            TcpListener::bind(address(ip)?).map_err(E2eError::io("binding host TCP listener"))?;
        let addr = listener
            .local_addr()
            .map_err(E2eError::io("reading host listener address"))?;
        let udp = UdpSocket::bind(addr).map_err(E2eError::io("binding host UDP listener"))?;
        let tcp_count = Arc::new(AtomicU64::new(0));
        let udp_count = Arc::new(AtomicU64::new(0));
        counts.insert(format!("{label}.tcp"), Arc::clone(&tcp_count));
        counts.insert(format!("{label}.udp"), Arc::clone(&udp_count));
        let nonce = targets.nonce.clone();
        let timeout = targets.timeout;
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                tcp_count.fetch_add(1, Ordering::SeqCst);
                let _ = stream.set_read_timeout(Some(timeout));
                let _ = stream.set_write_timeout(Some(timeout));
                let mut request = [0; 4096];
                if stream.read(&mut request).is_ok() {
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        nonce.len(),
                        nonce
                    );
                }
            }
        });
        std::thread::spawn(move || {
            let mut buffer = [0; 4096];
            while let Ok((n, from)) = udp.recv_from(&mut buffer) {
                udp_count.fetch_add(1, Ordering::SeqCst);
                let _ = udp.send_to(&buffer[..n], from);
            }
        });
        Ok(addr)
    }
    fn baseline(addr: SocketAddr, targets: &EgressTargets) -> Result<()> {
        let mut stream = TcpStream::connect_timeout(&addr, targets.timeout)
            .map_err(E2eError::io("connecting fixture baseline TCP"))?;
        stream
            .set_read_timeout(Some(targets.timeout))
            .map_err(E2eError::io("setting TCP timeout"))?;
        stream
            .set_write_timeout(Some(targets.timeout))
            .map_err(E2eError::io("setting TCP timeout"))?;
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: egress.fixture.test\r\nConnection: close\r\n\r\n")
            .map_err(E2eError::io("sending baseline HTTP"))?;
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .map_err(E2eError::io("reading baseline HTTP"))?;
        if !response.starts_with("HTTP/1.1 200 ") || !response.ends_with(&targets.nonce) {
            return Err(E2eError::system("invalid baseline HTTP response"));
        }
        let bind = if addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let udp = UdpSocket::bind(bind).map_err(E2eError::io("binding baseline UDP"))?;
        udp.set_read_timeout(Some(targets.timeout))
            .map_err(E2eError::io("setting UDP timeout"))?;
        udp.set_write_timeout(Some(targets.timeout))
            .map_err(E2eError::io("setting UDP timeout"))?;
        udp.connect(addr)
            .map_err(E2eError::io("connecting baseline UDP"))?;
        udp.send(targets.nonce.as_bytes())
            .map_err(E2eError::io("sending baseline UDP"))?;
        let mut data = [0; 1024];
        let n = udp
            .recv(&mut data)
            .map_err(E2eError::io("receiving baseline UDP"))?;
        if &data[..n] != targets.nonce.as_bytes() {
            return Err(E2eError::system("invalid baseline UDP response"));
        }
        Ok(())
    }
    fn dns_baseline(targets: &EgressTargets) -> Result<()> {
        for (name, kind, expected) in [
            ("egress.fixture.test", 1u16, targets.public_ipv4.ip()),
            ("egress.fixture.test", 28, targets.public_ipv6.ip()),
            (
                "forbidden.fixture.test",
                1,
                "10.20.0.10"
                    .parse()
                    .map_err(E2eError::parse("DNS expected address"))?,
            ),
        ] {
            let mut query = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
            for label in name.split('.') {
                query.push(
                    u8::try_from(label.len())
                        .map_err(|_| E2eError::system("DNS label too long"))?,
                );
                query.extend_from_slice(label.as_bytes());
            }
            query.push(0);
            query.extend_from_slice(&kind.to_be_bytes());
            query.extend_from_slice(&[0, 1]);
            let expected_bytes = match expected {
                IpAddr::V4(ip) => ip.octets().to_vec(),
                IpAddr::V6(ip) => ip.octets().to_vec(),
            };
            let udp = UdpSocket::bind("0.0.0.0:0").map_err(E2eError::io("binding baseline DNS"))?;
            udp.set_read_timeout(Some(targets.timeout))
                .map_err(E2eError::io("setting DNS timeout"))?;
            udp.connect(SocketAddr::new(targets.dns, 53))
                .map_err(E2eError::io("connecting DNS"))?;
            udp.send(&query).map_err(E2eError::io("sending DNS"))?;
            let mut reply = [0; 4096];
            let n = udp
                .recv(&mut reply)
                .map_err(E2eError::io("receiving DNS"))?;
            if n < 12
                || reply[0..2] != query[0..2]
                || reply[6..8] != [0, 1]
                || !reply[..n].ends_with(&expected_bytes)
            {
                return Err(E2eError::system("invalid baseline DNS response"));
            }
            let mut tcp =
                TcpStream::connect_timeout(&SocketAddr::new(targets.dns, 53), targets.timeout)
                    .map_err(E2eError::io("connecting TCP DNS"))?;
            tcp.set_read_timeout(Some(targets.timeout))
                .map_err(E2eError::io("setting DNS timeout"))?;
            tcp.set_write_timeout(Some(targets.timeout))
                .map_err(E2eError::io("setting DNS timeout"))?;
            let query_len =
                u16::try_from(query.len()).map_err(|_| E2eError::system("DNS query too long"))?;
            tcp.write_all(&query_len.to_be_bytes())
                .and_then(|()| tcp.write_all(&query))
                .map_err(E2eError::io("sending TCP DNS"))?;
            let mut length = [0; 2];
            tcp.read_exact(&mut length)
                .map_err(E2eError::io("reading TCP DNS length"))?;
            let mut reply = vec![0; u16::from_be_bytes(length) as usize];
            tcp.read_exact(&mut reply)
                .map_err(E2eError::io("reading TCP DNS"))?;
            if reply.len() < 12
                || reply[0..2] != query[0..2]
                || reply[6..8] != [0, 1]
                || !reply.ends_with(&expected_bytes)
            {
                return Err(E2eError::system("invalid baseline TCP DNS response"));
            }
        }
        Ok(())
    }
    fn record_counts(report: &mut Report, totals: &BTreeMap<String, u64>) -> Result<()> {
        for label in [
            "public4",
            "public6",
            "imds4",
            "imds6",
            "vpc4",
            "controller",
            "vpc6",
            "testnet",
            "host4",
            "host_loopback",
        ] {
            for protocol in ["tcp", "udp"] {
                let key = format!("{label}.{protocol}");
                let count = *totals
                    .get(&key)
                    .ok_or_else(|| E2eError::system(format!("missing fixture counter {key}")))?;
                let public = label.starts_with("public");
                report.checks.push(Check::expect(
                    format!(
                        "egress fixture {key} {}",
                        if public { "observed" } else { "blocked" }
                    ),
                    if public { count > 0 } else { count == 0 },
                    || format!("observed {count} connections"),
                ));
            }
        }
        Ok(())
    }
    fn root(input_path: &Path, uid: u32, gid: u32) -> Result<Report> {
        let mut inputs: DeploymentInputs = read_json(input_path)?;
        let tools = tools(&inputs)?.clone();
        let timeout = inputs.settings.handshake_timeout;
        if timeout.is_zero() {
            return Err(E2eError::system(
                "fixture handshake timeout must be nonzero",
            ));
        }
        let initial = plan(timeout)?;
        let mut peer = spawn_peer(&tools, &initial)?;
        topology(&tools, peer.child.0.id(), timeout)?;
        let PeerMessage::Ready(targets) = peer.send(&PeerCommand::Start)? else {
            return Err(E2eError::system("peer was not ready"));
        };
        let mut targets = *targets;
        let mut counts = Counters::new();
        for (label, ip) in [("host4", "10.255.255.1"), ("host_loopback", "127.0.0.1")] {
            let addr = host_listener(label, ip, &targets, &mut counts)?;
            targets.blocked.insert(label.into(), addr);
        }
        for addr in std::iter::once(targets.public_ipv4)
            .chain(std::iter::once(targets.public_ipv6))
            .chain(targets.blocked.values().copied())
        {
            baseline(addr, &targets)?;
        }
        dns_baseline(&targets)?;
        match peer.send(&PeerCommand::Reset)? {
            PeerMessage::Counts(values) if values.values().all(|&n| n == 0) => {}
            _ => return Err(E2eError::system("peer counter reset failed")),
        }
        for count in counts.values() {
            count.store(0, Ordering::SeqCst);
        }
        let egress = inputs
            .action_tools
            .egress
            .as_mut()
            .ok_or_else(|| E2eError::system("egress tools disappeared"))?;
        egress.policy = Some(targets.policy.clone());
        egress.targets = Some(targets.clone());
        let dir = input_path
            .parent()
            .ok_or_else(|| E2eError::system("fixture input has no parent"))?;
        let worker_inputs = dir.join("worker-inputs.json");
        let worker_targets = dir.join("targets.json");
        write_json(&worker_inputs, &inputs)?;
        write_json(&worker_targets, &targets)?;
        let mut cmd = command(&tools.unshare);
        cmd.arg("--user")
            .arg("--map-user")
            .arg(uid.to_string())
            .arg("--map-group")
            .arg(gid.to_string())
            .arg("--")
            .arg(executable()?)
            .arg("--gang-e2e-egress-worker")
            .arg(worker_inputs)
            .arg(worker_targets);
        let output = run(cmd, deadline(&inputs))?;
        let mut report: Report =
            serde_json::from_str(&output).map_err(E2eError::parse("worker report"))?;
        let PeerMessage::Counts(mut totals) = peer.send(&PeerCommand::Counts)? else {
            return Err(E2eError::system("peer counts unavailable"));
        };
        for (key, value) in counts {
            totals.insert(key, value.load(Ordering::SeqCst));
        }
        record_counts(&mut report, &totals)?;
        if !matches!(peer.send(&PeerCommand::Stop)?, PeerMessage::Stopped) {
            return Err(E2eError::system("peer did not stop"));
        }
        Ok(report)
    }
}

/// Runs the Linux fixture in an isolated process tree.
#[cfg(target_os = "linux")]
pub use linux::{launch, private_main};

#[cfg(target_os = "linux")]
pub(crate) use linux::lifecycle_targets;

#[cfg(not(target_os = "linux"))]
pub(crate) fn lifecycle_targets(
    _: &crate::settings::DeploymentInputs,
) -> crate::errors::Result<crate::EgressTargets> {
    Err(crate::errors::E2eError::system(
        "egress fixture requires Linux",
    ))
}

/// The real network fixture is Linux-only.
///
/// # Errors
///
/// Always returns an error because the fixture requires Linux.
#[cfg(not(target_os = "linux"))]
pub fn launch(
    _: &crate::settings::DeploymentInputs,
) -> crate::errors::Result<crate::check::Report> {
    Err(crate::errors::E2eError::system(
        "egress fixture requires Linux",
    ))
}

/// Recognizes no private fixture modes on non-Linux platforms.
#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn private_main(_: &[std::ffi::OsString]) -> Option<std::process::ExitCode> {
    None
}
