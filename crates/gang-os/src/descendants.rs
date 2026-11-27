//! Tracks and drains surviving action descendants under kernel-enforced ownership.
//!
//! On Linux, [`DescendantTracker::start`] enables `PR_SET_CHILD_SUBREAPER` so every orphaned
//! descendant—including processes that detached via `setsid()` or `setpgid()`—is atomically
//! reparented to the caller. [`DescendantTracker::drain`] kills the action's original process
//! group and iteratively kills and reaps only unreaped direct/adopted children (excluding any
//! configured guardian), preventing PID-reuse races because only this process can reap its own
//! children.
//!
//! On macOS and other non-Linux Unix systems without cgroups, unprivileged XNU provides neither
//! a child subreaper, `EVFILT_PROC` `NOTE_TRACK` (removed in macOS 10.5), nor an atomic
//! identity-bound signal syscall for reparented non-children. Rather than using heuristic PID or
//! working-directory scans that risk signalling unrelated or recycled processes, [`DescendantTracker`]
//! drains only the action's original process group (whose ID remains pinned while the executor
//! lives) and does not claim cleanup of descendants that explicitly detach via `setsid()` or
//! `setpgid()`.

#[cfg(target_os = "linux")]
use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use rustix::io::Errno;
use rustix::process::{Pid, Signal, kill_process_group, test_kill_process_group};
#[cfg(target_os = "linux")]
use rustix::process::{WaitOptions, kill_process, waitpid};

/// Owns kernel-backed descendant cleanup until the action's surviving processes have drained.
///
/// On Linux the caller must own reaping of its children and exclude any long-lived guardian.
/// On non-Linux platforms, cleanup drains the action's original process group only.
#[derive(Debug)]
#[must_use]
pub struct DescendantTracker {
    #[cfg(target_os = "linux")]
    excluded_child: Option<u32>,
}

impl DescendantTracker {
    /// Prepares descendant cleanup before the action starts.
    ///
    /// On Linux this enables `PR_SET_CHILD_SUBREAPER` and propagates any kernel failure instead
    /// of silently running without escaped-descendant ownership. On non-Linux platforms no kernel
    /// subreaper exists, so this succeeds without heuristic trackers and [`Self::drain`] cleans
    /// only the action's original process group.
    ///
    /// # Errors
    ///
    /// Returns an I/O error on Linux if `PR_SET_CHILD_SUBREAPER` fails.
    pub fn start(_action_dir: &Path, excluded_child: Option<u32>) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::start_with(excluded_child, || {
                rustix::process::set_child_subreaper(Some(rustix::process::getpid()))
                    .map_err(io::Error::from)
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = excluded_child;
            Ok(Self {})
        }
    }

    #[cfg(target_os = "linux")]
    fn start_with(
        excluded_child: Option<u32>,
        enable_subreaper: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<Self> {
        enable_subreaper()?;
        Ok(Self { excluded_child })
    }

    /// Terminates and drains surviving processes owned by the action.
    ///
    /// On Linux this kills `action_group` and iteratively kills and reaps unreaped direct/adopted
    /// children (excluding `excluded_child`) under subreaper ownership. On non-Linux platforms
    /// this kills and drains `action_group` only.
    ///
    /// # Errors
    ///
    /// Returns an I/O error on a failed kernel operation or when `timeout` expires.
    pub fn drain(&mut self, action_group: Pid, timeout: Duration) -> io::Result<()> {
        let deadline = drain_deadline(timeout)?;
        loop {
            kill_group(action_group)?;
            #[cfg(target_os = "linux")]
            let children_empty = {
                let children = select_direct_children(
                    proc_entries()?,
                    std::process::id(),
                    self.excluded_child,
                );
                // Only our unreaped children are stable PID references. A living parent may reap
                // and recycle a deeper descendant before we can signal it, so never snapshot those.
                for &child in &children {
                    tolerate_missing(kill_process(child, Signal::KILL))?;
                }
                for &child in &children {
                    reap_child(child)?;
                }
                children.is_empty()
            };
            #[cfg(not(target_os = "linux"))]
            let children_empty = true;

            match test_kill_process_group(action_group) {
                Err(Errno::SRCH) if children_empty => return Ok(()),
                Err(Errno::SRCH) | Ok(()) if Instant::now() >= deadline => {
                    return Err(drain_timeout());
                }
                Err(Errno::SRCH) | Ok(()) => std::thread::sleep(Duration::from_millis(10)),
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn proc_entries() -> io::Result<Vec<(u32, u32)>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let Ok(entry) = entry else {
            continue;
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let parent = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(1))
            .and_then(|value| value.parse().ok());
        if let Some(parent) = parent {
            entries.push((pid, parent));
        }
    }
    Ok(entries)
}

#[cfg(target_os = "linux")]
fn select_direct_children(
    entries: impl IntoIterator<Item = (u32, u32)>,
    self_pid: u32,
    excluded_child: Option<u32>,
) -> Vec<Pid> {
    entries
        .into_iter()
        .filter(|&(pid, parent)| {
            parent == self_pid && pid != self_pid && Some(pid) != excluded_child
        })
        .filter_map(|(pid, _)| i32::try_from(pid).ok().and_then(Pid::from_raw))
        .collect()
}

fn drain_deadline(timeout: Duration) -> io::Result<Instant> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "drain timeout overflow"))
}

fn drain_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "action group did not drain")
}

fn kill_group(group: Pid) -> io::Result<()> {
    tolerate_missing(kill_process_group(group, Signal::KILL))
}

#[cfg(target_os = "linux")]
fn reap_child(pid: Pid) -> io::Result<()> {
    match waitpid(Some(pid), WaitOptions::NOHANG) {
        Ok(_) | Err(Errno::CHILD) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn tolerate_missing(result: rustix::io::Result<()>) -> io::Result<()> {
    match result {
        Ok(()) | Err(Errno::SRCH) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    #[cfg(target_os = "linux")]
    use std::io;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::os::unix::process::CommandExt;
    #[cfg(target_os = "linux")]
    use std::path::Path;
    use std::path::PathBuf;
    #[cfg(target_os = "linux")]
    use std::process::Command;
    use std::process::Stdio;
    use std::time::Duration;

    use gang_testkit::ChildTest;
    use rustix::io::Errno;
    use rustix::process::{Pid, Signal, kill_process};
    #[cfg(target_os = "linux")]
    use rustix::process::{WaitOptions, waitpid};

    use super::DescendantTracker;
    #[cfg(target_os = "linux")]
    use super::select_direct_children;

    const DEADLINE: Duration = Duration::from_secs(15);
    const UNRELATED_HOST: ChildTest = ChildTest::new("descendants::tests::unrelated_host");
    const UNRELATED_MEMBER: ChildTest = ChildTest::new("descendants::tests::unrelated_member");

    #[cfg(target_os = "linux")]
    const LINUX_HOST: ChildTest = ChildTest::new("descendants::tests::linux_subreaper_host");
    #[cfg(target_os = "linux")]
    const LINUX_MEMBER: ChildTest = ChildTest::new("descendants::tests::linux_subreaper_member");

    #[cfg(target_os = "linux")]
    #[test]
    fn select_direct_children_excludes_grandchildren_guardian_and_self() {
        let entries = [
            (100, 1),   // self
            (101, 100), // guardian
            (102, 100), // direct child
            (103, 102), // grandchild under living parent
            (104, 1),   // unrelated process
            (105, 100), // adopted descendant
        ];
        let mut selected: Vec<i32> = select_direct_children(entries, 100, Some(101))
            .into_iter()
            .map(|pid| pid.as_raw_nonzero().get())
            .collect();
        selected.sort_unstable();
        assert_eq!(selected, vec![102, 105]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn subreaper_failure_is_propagated_on_start() {
        let error = DescendantTracker::start_with(None, || Err(io::Error::from(Errno::PERM)))
            .expect_err("subreaper denial must fail startup");
        assert_eq!(error.raw_os_error(), Some(Errno::PERM.raw_os_error()));
    }

    #[test]
    fn drain_leaves_unrelated_process_in_action_dir_alive() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("unrelated.sock");
        let listener = UnixListener::bind(&socket)?;
        let mut unrelated = UNRELATED_MEMBER
            .command()?
            .current_dir(directory.path())
            .env("TRACKER_ROLE", "unrelated")
            .env("TRACKER_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;

        let (stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(DEADLINE))?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let (role, raw) = line.trim().split_once(' ').ok_or("invalid readiness")?;
        assert_eq!(role, "unrelated");
        let unrelated_pid = Pid::from_raw(raw.parse()?).ok_or("invalid unrelated pid")?;
        let _keep_open = reader.into_inner();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let mut host = tokio::process::Command::from(UNRELATED_HOST.command()?);
        host.env("TRACKER_ACTION_DIR", directory.path());
        let output = runtime.block_on(async {
            tokio::time::timeout(DEADLINE, async {
                crate::run_bounded(&mut host, None, gang_types::NonZeroBytes::kib(64))
                    .await
                    .map_err(Box::<dyn Error>::from)
            })
            .await
        })??;

        let alive = rustix::process::test_kill_process(unrelated_pid);
        let _ = kill_process(unrelated_pid, Signal::KILL);
        let _ = unrelated.wait();

        assert!(
            output.status.success(),
            "unrelated host failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(UNRELATED_HOST.completed_in(std::str::from_utf8(&output.stdout)?));
        assert_eq!(
            alive,
            Ok(()),
            "unrelated process inside action_dir was signalled by drain"
        );
        Ok(())
    }

    #[test]
    #[ignore = "host helper for unrelated-process preservation regression"]
    fn unrelated_host() -> Result<(), Box<dyn Error>> {
        let action_dir = PathBuf::from(
            std::env::var_os("TRACKER_ACTION_DIR").ok_or("missing TRACKER_ACTION_DIR")?,
        );
        let socket = action_dir.join("host.sock");
        let listener = UnixListener::bind(&socket)?;
        let mut tracker = DescendantTracker::start(&action_dir, None)?;
        let mut leader = UNRELATED_MEMBER
            .command()?
            .current_dir(&action_dir)
            .env("TRACKER_ROLE", "spawner")
            .env("TRACKER_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;
        let group = Pid::from_raw(i32::try_from(leader.id())?).ok_or("invalid leader pid")?;
        let (stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(DEADLINE))?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let (role, raw) = line.trim().split_once(' ').ok_or("invalid readiness")?;
        assert_eq!(role, "background");
        let bg_pid = Pid::from_raw(raw.parse()?).ok_or("invalid background pid")?;
        let _keep_open = reader.into_inner();
        assert!(leader.wait()?.success(), "spawner helper failed");
        tracker.drain(group, Duration::from_secs(5))?;
        assert_eq!(rustix::process::test_kill_process(bg_pid), Err(Errno::SRCH));
        UNRELATED_HOST.complete();
        Ok(())
    }

    #[test]
    #[ignore = "member helper for unrelated-process preservation regression"]
    fn unrelated_member() -> Result<(), Box<dyn Error>> {
        let role = std::env::var("TRACKER_ROLE")?;
        let socket = PathBuf::from(std::env::var_os("TRACKER_SOCKET").ok_or("missing socket")?);
        if role == "spawner" {
            let _bg = UNRELATED_MEMBER
                .command()?
                .env("TRACKER_ROLE", "background")
                .env("TRACKER_SOCKET", &socket)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;
            return Ok(());
        }
        let mut stream = UnixStream::connect(socket)?;
        stream.set_read_timeout(Some(DEADLINE))?;
        writeln!(stream, "{role} {}", std::process::id())?;
        let _ = stream.read(&mut [0])?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    struct ReadyMember {
        role: String,
        pid: Pid,
        stream: UnixStream,
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn subreaper_drain_adopts_and_reaps_nested_descendants_without_signalling_reaped_children()
    -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let mut command = tokio::process::Command::from(LINUX_HOST.command()?);
        let output = runtime.block_on(async {
            tokio::time::timeout(DEADLINE, async {
                crate::run_bounded(&mut command, None, gang_types::NonZeroBytes::kib(64))
                    .await
                    .map_err(Box::<dyn Error>::from)
            })
            .await
        })??;
        assert!(
            output.status.success(),
            "tracker host failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(LINUX_HOST.completed_in(std::str::from_utf8(&output.stdout)?));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn linux_member_command(role: &str, socket: &Path) -> io::Result<Command> {
        let mut command = LINUX_MEMBER.command()?;
        command
            .env("TRACKER_ROLE", role)
            .env("TRACKER_SOCKET", socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        Ok(command)
    }

    #[cfg(target_os = "linux")]
    fn ready_members(listener: &UnixListener) -> Result<Vec<ReadyMember>, Box<dyn Error>> {
        let mut members = Vec::new();
        for _ in 0..3 {
            let (stream, _) = listener.accept()?;
            stream.set_read_timeout(Some(DEADLINE))?;
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let (role, raw) = line
                .trim()
                .split_once(' ')
                .ok_or("invalid readiness message")?;
            let pid = Pid::from_raw(raw.parse()?).ok_or("invalid descendant pid")?;
            if role != "action" {
                assert_eq!(
                    rustix::process::getsid(Some(pid))?,
                    pid,
                    "{role} did not escape"
                );
            }
            members.push(ReadyMember {
                role: role.into(),
                pid,
                stream: reader.into_inner(),
            });
        }
        Ok(members)
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "isolated process host for Linux subreaper descendant regressions"]
    fn linux_subreaper_host() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("ready.sock");
        let listener = UnixListener::bind(&socket)?;
        let mut guardian = linux_member_command("guardian", &socket)?
            .stdin(Stdio::piped())
            .spawn()?;
        let mut tracker = DescendantTracker::start(directory.path(), Some(guardian.id()))?;
        let mut action = linux_member_command("action", &socket)?
            .current_dir(directory.path())
            .process_group(0)
            .spawn()?;
        let group = Pid::from_raw(i32::try_from(action.id())?).ok_or("invalid action pid")?;
        let mut members = ready_members(&listener)?;
        let root = members
            .iter_mut()
            .find(|member| member.role == "action")
            .ok_or("missing action readiness")?;
        root.stream.write_all(&[1])?;
        assert!(action.wait()?.success(), "action helper failed");
        tracker.drain(group, Duration::from_secs(5))?;
        for member in &members {
            assert_eq!(
                rustix::process::test_kill_process(member.pid),
                Err(Errno::SRCH),
                "{} descendant {:?} survived",
                member.role,
                member.pid
            );
            assert!(
                matches!(
                    waitpid(Some(member.pid), WaitOptions::NOHANG),
                    Err(Errno::CHILD)
                ),
                "descendant {:?} was not reaped",
                member.pid
            );
        }
        let guardian_pid =
            Pid::from_raw(i32::try_from(guardian.id())?).ok_or("invalid guardian pid")?;
        assert_eq!(
            rustix::process::test_kill_process(guardian_pid),
            Ok(()),
            "guardian was signalled"
        );
        drop(guardian.stdin.take());
        assert!(guardian.wait()?.success());
        LINUX_HOST.complete();
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "real fork/session helper for Linux subreaper descendant regressions"]
    fn linux_subreaper_member() -> Result<(), Box<dyn Error>> {
        let role = std::env::var("TRACKER_ROLE")?;
        let socket = PathBuf::from(std::env::var_os("TRACKER_SOCKET").ok_or("missing socket")?);
        match role.as_str() {
            "short" => return Ok(()),
            "guardian" => {
                io::copy(&mut io::stdin(), &mut io::sink())?;
                return Ok(());
            }
            "action" => {
                let _parent = linux_member_command("parent", &socket)?.spawn()?;
            }
            "parent" => {
                rustix::process::setsid()?;
                for _ in 0..8 {
                    assert!(linux_member_command("short", &socket)?.status()?.success());
                }
                let _grandchild = linux_member_command("grandchild", &socket)?.spawn()?;
            }
            "grandchild" => {
                rustix::process::setsid()?;
            }
            _ => return Err("invalid tracker helper role".into()),
        }
        let mut stream = UnixStream::connect(socket)?;
        stream.set_read_timeout(Some(DEADLINE))?;
        writeln!(stream, "{role} {}", std::process::id())?;
        if role == "action" {
            stream.read_exact(&mut [0])?;
        } else {
            // EOF on failure releases escaped helpers even though their stdio is closed.
            let _ = stream.read(&mut [0])?;
        }
        Ok(())
    }
}
