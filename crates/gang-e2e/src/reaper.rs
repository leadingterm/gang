//! The `local` process outlives the harness it runs, so no process the harness starts survives it,
//! however the harness ends. On Linux it becomes the child subreaper of its descendants and runs
//! the harness as its only child, re-executing itself. Orphans at any depth are reparented to the
//! reaper rather than init, including workers in their own process groups and scopes and daemonized
//! Bazel servers. While the harness runs, the reaper reaps each exited descendant at once, whatever
//! its process group, so no exited Bazel server or worker lingers as a zombie. Once the harness
//! returns, panics, aborts or is killed, even with SIGKILL, or the reaper receives SIGTERM, SIGINT,
//! SIGHUP or SIGQUIT, it kills and reaps every process left below it.
//! macOS has no subreaper, so there the harness runs in the `local` process itself: a stack's drop
//! still stops what it started when the harness returns or panics, but a harness killed by a signal
//! leaves its servers, workers and Bazel servers running.

use std::process::ExitCode;

/// Runs `harness` below a reaper and returns its exit code: on Linux in a child process (this
/// program re-executed with the same arguments), and elsewhere in this process.
pub fn reaped(harness: impl FnOnce() -> ExitCode) -> ExitCode {
    #[cfg(target_os = "linux")]
    if !linux::is_reaped_harness() {
        return linux::reap_self();
    }
    harness()
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::HashSet;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, ExitCode};

    use rustix::io::Errno;
    use rustix::process::{Pid, Signal, WaitOptions, WaitStatus};
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
    use tokio::signal::unix::{SignalKind, signal};

    use crate::errors::{E2eError, Result};

    /// Holds the pid of the reaper that started this harness. Only its own child runs as the
    /// harness; an inherited or stale value starts a new reaper.
    pub(super) const REAPED_ENV: &str = "GANG_E2E_REAPED";

    /// `__WALL` (linux/wait.h): waits for every child, including `clone(2)` children whose exit
    /// signal is not `SIGCHLD`, which a plain wait ignores.
    const ALL_CHILDREN: WaitOptions = WaitOptions::from_bits_retain(0x4000_0000);

    /// Returns whether the current process is the marked reaper's direct child.
    pub(super) fn is_reaped_harness() -> bool {
        std::env::var(REAPED_ENV)
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .zip(rustix::process::getppid())
            .is_some_and(|(marked, parent)| {
                u32::try_from(parent.as_raw_nonzero().get()).ok() == Some(marked)
            })
    }

    /// Re-executes this program beneath a reaper, reporting setup failures to the user.
    pub(super) fn reap_self() -> ExitCode {
        match harness_command().and_then(reap) {
            Ok(code) => ExitCode::from(code),
            Err(error) => {
                eprintln!("error: {}", gang_types::error_chain(&error));
                ExitCode::from(2)
            }
        }
    }

    /// This program, re-executed with the same arguments and marked as the harness.
    fn harness_command() -> Result<Command> {
        let executable =
            std::env::current_exe().map_err(E2eError::io("finding the harness executable"))?;
        let mut arguments = std::env::args_os();
        let mut command = Command::new(executable);
        if let Some(arg0) = arguments.next() {
            command.arg0(arg0);
        }
        command
            .args(arguments)
            .env(REAPED_ENV, std::process::id().to_string());
        Ok(command)
    }

    /// How supervision ended.
    enum Ended {
        /// The harness exited with this status.
        Harness(WaitStatus),
        /// The reaper received this signal.
        Stopped(Signal),
    }

    impl Ended {
        /// Returns the shell-style exit code of the run.
        fn code(&self) -> u8 {
            let code = match self {
                Self::Harness(status) => status
                    .exit_status()
                    .or_else(|| status.terminating_signal().map(|signal| 128 + signal)),
                Self::Stopped(signal) => Some(128 + signal.as_raw()),
            };
            code.and_then(|code| u8::try_from(code).ok()).unwrap_or(1)
        }
    }

    /// Supervises the harness, kills and reaps its descendants, and returns the exit code of the run.
    pub(super) fn reap(mut harness: Command) -> Result<u8> {
        rustix::process::set_child_subreaper(Some(rustix::process::getpid()))
            .map_err(|errno| E2eError::io("becoming the child subreaper")(errno.into()))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .map_err(E2eError::io("building the reaper runtime"))?;
        let ended = runtime.block_on(supervise(&mut harness));
        let killed = kill_descendants();
        let ended = ended?;
        killed?;
        Ok(ended.code())
    }

    /// Waits for the harness to exit or for a stop signal, reaping exited children along the way.
    async fn supervise(harness: &mut Command) -> Result<Ended> {
        // Register before spawning so a stop signal cannot find a started harness without a handler.
        let mut exits =
            signal(SignalKind::child()).map_err(E2eError::io("listening for signals"))?;
        let mut terminate =
            signal(SignalKind::terminate()).map_err(E2eError::io("listening for signals"))?;
        let mut interrupt =
            signal(SignalKind::interrupt()).map_err(E2eError::io("listening for signals"))?;
        let mut hangup =
            signal(SignalKind::hangup()).map_err(E2eError::io("listening for signals"))?;
        let mut quit = signal(SignalKind::quit()).map_err(E2eError::io("listening for signals"))?;
        let child = harness
            .spawn()
            .map_err(E2eError::io("starting the harness"))?;
        let pid = i32::try_from(child.id())
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| E2eError::system("the harness has no process id"))?;
        drop(child);
        loop {
            if let Some(status) = reap_exited(pid)? {
                return Ok(Ended::Harness(status));
            }
            tokio::select! {
                _ = exits.recv() => {},
                _ = terminate.recv() => return Ok(Ended::Stopped(Signal::TERM)),
                _ = interrupt.recv() => return Ok(Ended::Stopped(Signal::INT)),
                _ = hangup.recv() => return Ok(Ended::Stopped(Signal::HUP)),
                _ = quit.recv() => return Ok(Ended::Stopped(Signal::QUIT)),
            }
        }
    }

    /// Reaps every exited child without waiting, whatever its process group. `waitpid(None)` would
    /// only wait for children in the caller's process group.
    fn reap_exited(harness: Pid) -> Result<Option<WaitStatus>> {
        let mut ended = None;
        loop {
            match rustix::process::wait(WaitOptions::NOHANG | ALL_CHILDREN) {
                Ok(Some((pid, status))) => {
                    if pid == harness {
                        ended = Some(status);
                    }
                }
                Ok(None) | Err(Errno::CHILD) => return Ok(ended),
                Err(Errno::INTR) => {}
                Err(errno) => return Err(E2eError::io("reaping children")(errno.into())),
            }
        }
    }

    /// Kills and reaps every descendant it can signal, even if another cannot be signalled.
    /// Killing a child reparents its children here, so each round reaches another generation.
    fn kill_descendants() -> Result<()> {
        let mut unkillable = HashSet::new();
        let mut first_error = None;
        loop {
            let children = children();
            if children.iter().all(|pid| unkillable.contains(pid)) {
                return first_error.map_or(Ok(()), Err);
            }
            for &pid in &children {
                if unkillable.contains(&pid) {
                    continue;
                }
                match rustix::process::kill_process(pid, Signal::KILL) {
                    Ok(()) | Err(Errno::SRCH) => {}
                    Err(errno) => {
                        unkillable.insert(pid);
                        first_error.get_or_insert_with(|| {
                            E2eError::io(format!("killing process {pid}"))(errno.into())
                        });
                    }
                }
            }
            for pid in children {
                if unkillable.contains(&pid) {
                    continue;
                }
                loop {
                    match rustix::process::waitpid(Some(pid), ALL_CHILDREN) {
                        Ok(_) | Err(Errno::CHILD) => break,
                        Err(Errno::INTR) => {}
                        Err(errno) => {
                            return Err(E2eError::io(format!("reaping process {pid}"))(
                                errno.into(),
                            ));
                        }
                    }
                }
            }
        }
    }

    /// Reads this process's direct children from the process table.
    fn children() -> Vec<Pid> {
        let mut system = System::new();
        system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().without_tasks(),
        );
        let parent = sysinfo::Pid::from_u32(std::process::id());
        system
            .processes()
            .iter()
            .filter_map(|(pid, process)| {
                (process.parent() == Some(parent))
                    .then(|| i32::try_from(pid.as_u32()).ok().and_then(Pid::from_raw))
                    .flatten()
            })
            .collect()
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, ExitCode, Stdio};
    use std::time::Duration;

    use gang_testkit::ChildTest;
    use rustix::io::Errno;
    use rustix::process::{Pid, Signal, kill_process};

    /// The ignored child test invoked in each process in the test tree.
    const CHILD_HELPER: ChildTest = ChildTest::new("reaper::tests::reaper_helper");
    /// Selects the role of a child test process.
    const MARKER: &str = "GANG_E2E_REAPER_HELPER";

    /// Creates a command for one role in the test tree.
    fn helper(mode: &str) -> Command {
        let mut command = CHILD_HELPER
            .command()
            .expect("the test binary path is known");
        command.env(MARKER, mode);
        command
    }

    /// Starts a child in its own process group and reports its pid before exiting.
    fn orphan_child() {
        // Leave the child unwaited so the reaper inherits it when this process exits.
        let pid = helper("exit")
            .process_group(0)
            .stdout(Stdio::null())
            .spawn()
            .expect("child starts")
            .id();
        println!("\norphan {pid}");
        std::io::stdout().flush().expect("stdout flushes");
    }

    /// Waits for the reaper to reap an orphan while the harness remains alive.
    fn harness_awaits_reaping() {
        let mut orphaner = helper("orphaner")
            .stdout(Stdio::piped())
            .spawn()
            .expect("orphaner starts");
        let stdout = orphaner.stdout.take().expect("orphaner stdout is piped");
        let orphan = BufReader::new(stdout)
            .lines()
            .map(|line| line.expect("orphaner output is readable"))
            .find_map(|line| {
                line.strip_prefix("orphan ")
                    .map(|pid| pid.parse::<i32>().expect("orphan pid is valid"))
            })
            .and_then(Pid::from_raw)
            .expect("orphan has a pid");
        orphaner.wait().expect("orphaner exits");
        while rustix::process::test_kill_process(orphan) != Err(Errno::SRCH) {
            std::thread::yield_now();
        }
        std::process::exit(7);
    }

    /// Runs one role only when explicitly invoked as a child process.
    #[ignore = "child process helper of the reaper tests"]
    #[test]
    fn reaper_helper() {
        let Ok(mode) = std::env::var(MARKER) else {
            return;
        };
        if let Some(inner) = mode.strip_prefix("reaper:") {
            std::process::exit(i32::from(
                super::linux::reap(helper(inner)).expect("the reaper runs"),
            ));
        }
        match mode.as_str() {
            "orphaner" => orphan_child(),
            "harness-awaits-reaping" => harness_awaits_reaping(),
            "exit" => {}
            "reaped" => {
                let code = super::reaped(|| {
                    println!(
                        "\nharness {} under {}",
                        std::process::id(),
                        rustix::process::getppid().map_or(0, |p| p.as_raw_nonzero().get())
                    );
                    std::io::stdout().flush().expect("stdout flushes");
                    ExitCode::from(7)
                });
                std::process::exit(if code == ExitCode::from(7) { 7 } else { 1 });
            }
            "server" | "server-exits" => {
                // The harness closes its pipe once it stops reading: a worker writing to it could
                // exit on its own and hide a leak.
                let mut worker = helper("park")
                    .process_group(0)
                    .stdout(Stdio::null())
                    .spawn()
                    .expect("worker starts");
                println!("\nworker {}", worker.id());
                println!("server {}", std::process::id());
                std::io::stdout().flush().expect("stdout flushes");
                if mode == "server-exits" {
                    // Exits like a killed server: its worker is orphaned to the reaper.
                    std::process::exit(0);
                }
                worker.wait().expect("the worker runs until it is killed");
            }
            "park" => loop {
                std::thread::park();
            },
            "harness-panics" | "harness-parks" | "harness-orphans" => {
                let server_mode = if mode == "harness-orphans" {
                    "server-exits"
                } else {
                    "server"
                };
                let mut server = helper(server_mode)
                    .stdout(Stdio::piped())
                    .spawn()
                    .expect("server starts");
                let stdout = server.stdout.take().expect("server stdout is piped");
                let mut found = 0;
                let mut worker = None;
                for line in BufReader::new(stdout).lines() {
                    let line = line.expect("server output is readable");
                    if line.starts_with("worker ") || line.starts_with("server ") {
                        if let Some(pid) = line.strip_prefix("worker ") {
                            worker = Some(pid.parse::<u32>().expect("worker pid is valid"));
                        }
                        println!("\n{line}");
                        found += 1;
                        if found == 2 {
                            break;
                        }
                    }
                }
                assert_eq!(found, 2, "server reported both process ids");
                println!("harness {}", std::process::id());
                std::io::stdout().flush().expect("stdout flushes");
                assert!(mode != "harness-panics", "the scenario panics");
                if mode == "harness-orphans" {
                    server.wait().expect("server exits");
                    let pid = worker.expect("worker reported its pid");
                    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
                        .expect("worker remains alive");
                    let fields = stat.rsplit_once(')').expect("worker stat has a command").1;
                    let parent = fields
                        .split_whitespace()
                        .nth(1)
                        .expect("worker stat has a parent");
                    println!("worker-parent {parent}");
                    std::io::stdout().flush().expect("stdout flushes");
                    loop {
                        std::thread::park();
                    }
                }
                server.wait().expect("the server runs until it is killed");
            }
            other => panic!("unknown reaper helper mode: {other}"),
        }
    }

    /// Process ids reported by a running harness and its descendants.
    struct Tree {
        /// The scenario harness.
        harness: Pid,
        /// The server started by the harness.
        server: Pid,
        /// The worker in its own process group.
        worker: Pid,
    }

    /// Starts a reaper and waits for the complete tree to report its process ids.
    fn start(inner: &str) -> (Child, Tree) {
        let mut child = helper(&format!("reaper:{inner}"))
            .stdout(Stdio::piped())
            .spawn()
            .expect("reaper starts");
        let stdout = child.stdout.take().expect("reaper stdout is piped");
        let mut reader = BufReader::new(stdout);
        let (mut harness, mut server, mut worker) = (None, None, None);
        for line in (&mut reader).lines() {
            let line = line.expect("reaper output is readable");
            let Some((name, raw)) = line.split_once(' ') else {
                continue;
            };
            let target = match name {
                "harness" => &mut harness,
                "server" => &mut server,
                "worker" => &mut worker,
                _ => continue,
            };
            *target = raw.parse::<i32>().ok().and_then(Pid::from_raw);
            if harness.is_some() && server.is_some() && worker.is_some() {
                break;
            }
        }
        child.stdout = Some(reader.into_inner());
        (
            child,
            Tree {
                harness: harness.expect("harness reported its pid"),
                server: server.expect("server reported its pid"),
                worker: worker.expect("worker reported its pid"),
            },
        )
    }

    /// Asserts that no process from the tree survived, killing any leak discovered.
    fn assert_gone(tree: &Tree) {
        let results = [
            ("harness", tree.harness),
            ("server", tree.server),
            ("worker", tree.worker),
        ]
        .map(|(role, pid)| (role, kill_process(pid, Signal::KILL)));
        for (role, result) in results {
            assert_eq!(result, Err(Errno::SRCH), "the {role} outlived the run");
        }
    }

    /// Waits for the reaper to exit within the test deadline.
    fn wait(child: &mut Child) -> std::process::ExitStatus {
        crate::process::wait_within(child, Duration::from_secs(30), "the reaper to exit")
            .expect("the reaper exits")
    }

    /// An exited orphan leading its own process group, such as Bazel's server, must be reaped while
    /// the harness runs: a zombie server makes `bazel shutdown` wait out its grace period. Exit code
    /// 7 proves the reaper reports the harness's status rather than the orphan's.
    #[test]
    fn an_orphan_in_its_own_process_group_is_reaped_while_the_harness_runs() {
        let mut child = helper("reaper:harness-awaits-reaping")
            .stdout(Stdio::null())
            .spawn()
            .expect("reaper starts");
        assert_eq!(wait(&mut child).code(), Some(7));
    }

    #[test]
    fn a_harness_that_panics_leaves_no_process_behind() {
        let (mut child, tree) = start("harness-panics");
        assert_eq!(wait(&mut child).code(), Some(101));
        assert_gone(&tree);
    }

    #[test]
    fn a_killed_harness_leaves_no_process_behind() {
        for signal in [Signal::KILL, Signal::TERM] {
            let (mut child, tree) = start("harness-parks");
            kill_process(tree.harness, signal).expect("the harness is killed");
            assert_eq!(wait(&mut child).code(), Some(128 + signal.as_raw()));
            assert_gone(&tree);
        }
    }

    #[test]
    fn a_stopped_reaper_leaves_no_process_behind() {
        for signal in [Signal::TERM, Signal::INT, Signal::HUP, Signal::QUIT] {
            let (mut child, tree) = start("harness-parks");
            let pid = i32::try_from(child.id())
                .ok()
                .and_then(Pid::from_raw)
                .expect("reaper has a pid");
            kill_process(pid, signal).expect("the reaper is signalled");
            assert_eq!(wait(&mut child).code(), Some(128 + signal.as_raw()));
            assert_gone(&tree);
        }
    }

    /// Asserts a marked or unmarked helper executes its harness in a reaper child.
    fn assert_reaped(mut command: Command) {
        let mut child = command
            .stdout(Stdio::piped())
            .spawn()
            .expect("helper starts");
        let reaper = child.id();
        let stdout = child.stdout.take().expect("helper stdout is piped");
        let line = BufReader::new(stdout)
            .lines()
            .map(|line| line.expect("helper output is readable"))
            .find(|line| line.starts_with("harness "))
            .expect("harness reports its parent");
        let fields: Vec<_> = line.split_whitespace().collect();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[2], "under");
        let harness = fields[1].parse::<u32>().expect("harness pid is valid");
        let parent = fields[3].parse::<u32>().expect("parent pid is valid");
        assert_eq!(wait(&mut child).code(), Some(7));
        assert_eq!(parent, reaper);
        assert_ne!(harness, reaper);
    }

    #[test]
    fn reaped_runs_the_harness_in_a_child_of_the_reaper() {
        let mut command = helper("reaped");
        command.env_remove(super::linux::REAPED_ENV);
        assert_reaped(command);
    }

    #[test]
    fn a_stale_marker_does_not_bypass_the_reaper() {
        let mut command = helper("reaped");
        command.env(super::linux::REAPED_ENV, "1");
        assert_reaped(command);
    }

    #[test]
    fn a_worker_outlives_its_server_while_the_harness_runs() {
        let mut child = helper("reaper:harness-orphans")
            .stdout(Stdio::piped())
            .spawn()
            .expect("reaper starts");
        let reaper = child.id();
        let stdout = child.stdout.take().expect("reaper stdout is piped");
        let (mut harness, mut server, mut worker, mut parent) = (None, None, None, None);
        for line in BufReader::new(stdout).lines() {
            let line = line.expect("reaper output is readable");
            let Some((name, raw)) = line.split_once(' ') else {
                continue;
            };
            match name {
                "harness" => harness = raw.parse::<i32>().ok().and_then(Pid::from_raw),
                "server" => server = raw.parse::<i32>().ok().and_then(Pid::from_raw),
                "worker" => worker = raw.parse::<i32>().ok().and_then(Pid::from_raw),
                "worker-parent" => parent = raw.parse::<u32>().ok(),
                _ => {}
            }
            if parent.is_some() {
                break;
            }
        }
        assert_eq!(parent, Some(reaper), "worker was reparented to the reaper");
        let tree = Tree {
            harness: harness.expect("harness reported its pid"),
            server: server.expect("server reported its pid"),
            worker: worker.expect("worker reported its pid"),
        };
        kill_process(tree.harness, Signal::KILL).expect("the harness is killed");
        assert_eq!(wait(&mut child).code(), Some(128 + Signal::KILL.as_raw()));
        assert_gone(&tree);
    }
}
