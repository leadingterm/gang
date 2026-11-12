//! What the execution scenarios observe of a real deployment: its journal, its processes and cgroups, and
//! the files its workers and Local Mode instances keep.

use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use gang_clock::{Clock as _, SystemClock};
use gang_config::Document;
use gang_main::plugins::Plugins;
use gang_types::announcement::Announcement;
use x509_cert::Certificate;
use x509_cert::der::DecodePem;

use crate::errors::{E2eError, Result};
use crate::execution::{Execution, Generations, JOURNAL_END, Proc, Role, ServerExit};
use crate::gang_cli::GangCli;
use crate::journal::Journal;
use crate::process::{self, Supervised};
use crate::settings::Settings;
use crate::stack::Stack;

/// Everything the Local Mode workers of the deployment whose state directory is `state_dir`
/// logged (`<state_dir>/logs/*.log`), concatenated in name order; empty when none logged.
///
/// # Errors
///
/// When the worker log directory or a log exists and cannot be read.
pub(crate) fn worker_logs(state_dir: &Path) -> Result<String> {
    let entries = match fs::read_dir(state_dir.join("logs")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(String::new()),
        Err(error) => return Err(error).map_err(E2eError::io("listing worker logs")),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(E2eError::io("reading worker log entry"))?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "log")
            && entry
                .file_type()
                .map_err(E2eError::io("reading worker log type"))?
                .is_file()
        {
            paths.push(entry.path());
        }
    }
    paths.sort();
    let mut logs = String::new();
    for path in paths {
        logs.push_str(&fs::read_to_string(&path).map_err(E2eError::io(format!(
            "reading worker log {}",
            path.display()
        )))?);
    }
    Ok(logs)
}

/// The last `limit` bytes, decoded as lossy UTF-8.
pub(crate) fn tail(bytes: &[u8], limit: usize) -> String {
    let start = bytes.len().saturating_sub(limit);
    String::from_utf8_lossy(bytes.get(start..).unwrap_or_default()).into_owned()
}

/// The [`Execution`] observer of a real [`Stack`].
pub(crate) struct Observer {
    journal: Journal,
    go: PathBuf,
    config_file: PathBuf,
    state_dir: PathBuf,
    noop_scratch: PathBuf,
    cgroup_root: PathBuf,
    binary: PathBuf,
    home: PathBuf,
    server_log: PathBuf,
    exit_timeout: Duration,
    log_tail_bytes: usize,
}

impl Observer {
    /// The observer of `stack`: creates the journal beside its hold signal. `settings` names the cgroup hierarchy.
    pub(crate) fn new(stack: &Stack<'_>, settings: &Settings) -> Result<Self> {
        let signal = stack.signal_path();
        let placement = stack.placement();
        Ok(Self {
            journal: Journal::create(&signal.with_file_name("journal"), settings.hold_timeout)?,
            go: signal.with_file_name("go"),
            config_file: placement.config_file(),
            state_dir: placement.state_dir(),
            noop_scratch: placement.noop_scratch(),
            cgroup_root: settings.cgroup_root.clone(),
            binary: stack.binary().to_path_buf(),
            home: placement.home(),
            server_log: placement.server_log(),
            exit_timeout: settings.history_timeout,
            log_tail_bytes: settings.log_tail_bytes,
        })
    }

    fn process_cgroup(&self, pid: u32) -> Result<Option<PathBuf>> {
        if !cfg!(target_os = "linux") {
            return Ok(None);
        }
        let path = format!("/proc/{pid}/cgroup");
        let content = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).map_err(E2eError::io(format!("reading {path}"))),
        };
        Ok(content.lines().find_map(|line| {
            line.strip_prefix("0::")
                .map(|value| self.cgroup_root.join(value.trim_start_matches('/')))
        }))
    }
}

impl Execution for Observer {
    fn next_line(&self) -> Result<Option<String>> {
        let line = self.journal.read_line()?;
        Ok((line != JOURNAL_END).then_some(line))
    }

    fn end_lines(&self) -> Result<()> {
        self.journal.write_line(JOURNAL_END)
    }

    fn set_go(&self, go: bool) -> Result<()> {
        if go {
            fs::write(&self.go, b"").map_err(E2eError::io(format!(
                "setting the go flag {}",
                self.go.display()
            )))
        } else {
            match fs::remove_file(&self.go) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error).map_err(E2eError::io(format!(
                    "clearing the go flag {}",
                    self.go.display()
                ))),
            }
        }
    }

    fn processes(&self) -> Result<Vec<Proc>> {
        let entries = process::snapshot();
        let workers: HashSet<u32> = process::workers_of(&entries, &self.config_file)
            .into_iter()
            .map(|entry| entry.pid)
            .collect();
        let state_text = self.state_dir.to_string_lossy();
        let journal_text = self.journal.path().to_string_lossy();
        let mut processes = Vec::new();
        for entry in entries {
            let Some(program) = entry.args.first() else {
                continue;
            };
            let role = if workers.contains(&entry.pid) {
                Some((Role::Worker, None))
            } else if Path::new(program)
                .file_name()
                .is_some_and(|name| name == "gang-exec")
                && program.starts_with(state_text.as_ref())
            {
                let marker = format!("{state_text}/instances/");
                let instance = program
                    .strip_prefix(&marker)
                    .and_then(|remaining| remaining.split('/').next())
                    .filter(|part| !part.is_empty())
                    .map(str::to_owned);
                Some((Role::GangExec, instance))
            } else if Path::new(program)
                .file_name()
                .is_some_and(|name| name == "gang-guard")
                && Path::new(program).starts_with(&self.state_dir)
            {
                entry
                    .args
                    .get(1)
                    .and_then(|arg| arg.parse().ok())
                    .map(|creator| (Role::GangGuard { creator }, None))
            } else if Path::new(program)
                .file_name()
                .is_some_and(|name| name == "bwrap")
                && entry
                    .args
                    .iter()
                    .any(|arg| arg.starts_with(state_text.as_ref()))
            {
                Some((Role::Bwrap, None))
            } else if entry.args.iter().any(|arg| arg == journal_text.as_ref()) {
                entry
                    .args
                    .iter()
                    .find(|arg| arg.starts_with("e2e-"))
                    .map(|arg| (Role::Marked(arg.clone()), None))
            } else {
                None
            };
            if let Some((role, instance)) = role {
                let Some(cgroup) = self.process_cgroup(entry.pid)? else {
                    if cfg!(target_os = "linux") {
                        continue;
                    }
                    processes.push(Proc {
                        pid: entry.pid,
                        role,
                        cgroup: None,
                        instance,
                    });
                    continue;
                };
                processes.push(Proc {
                    pid: entry.pid,
                    role,
                    cgroup: Some(cgroup),
                    instance,
                });
            }
        }
        Ok(processes)
    }

    fn cgroup_members(&self, cgroup: &Path) -> Result<Vec<u32>> {
        let path = cgroup.join("cgroup.procs");
        let content = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error).map_err(E2eError::io("reading cgroup.procs")),
        };
        content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| line.parse().map_err(E2eError::parse("cgroup.procs")))
            .collect()
    }

    fn cgroup_children(&self, cgroup: &Path) -> Result<Vec<PathBuf>> {
        let entries = match fs::read_dir(cgroup) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error).map_err(E2eError::io("listing cgroup children")),
        };
        let mut children = Vec::new();
        for entry in entries {
            let entry = entry.map_err(E2eError::io("reading a cgroup child"))?;
            if entry
                .file_type()
                .map_err(E2eError::io("reading a cgroup child type"))?
                .is_dir()
            {
                children.push(entry.path());
            }
        }
        children.sort();
        Ok(children)
    }

    fn cgroup_exists(&self, cgroup: &Path) -> Result<bool> {
        match fs::metadata(cgroup) {
            Ok(metadata) => Ok(metadata.is_dir()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).map_err(E2eError::io("checking cgroup")),
        }
    }

    fn kill(&self, pid: u32) -> Result<()> {
        let target = sysinfo::Pid::from_u32(pid);
        let mut table = sysinfo::System::new();
        table.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[target]), true);
        // A process that is already gone is not an error; one that refuses the signal is.
        match table.process(target) {
            Some(process) if !process.kill() => Err(E2eError::system(format!(
                "process {pid} could not be sent SIGKILL"
            ))),
            _ => Ok(()),
        }
    }

    fn await_gone(&self, pid: u32, timeout: Duration) -> Result<bool> {
        #[cfg(target_os = "linux")]
        {
            use rustix::event::{PollFd, PollFlags, Timespec, poll};
            use rustix::process::{Pid, PidfdFlags, pidfd_open};

            let raw = i32::try_from(pid)
                .map_err(|_| E2eError::system(format!("invalid process id {pid}")))?;
            let target = Pid::from_raw(raw)
                .ok_or_else(|| E2eError::system(format!("invalid process id {pid}")))?;
            let fd = match pidfd_open(target, PidfdFlags::empty()) {
                Ok(fd) => fd,
                Err(rustix::io::Errno::SRCH) => return Ok(true),
                Err(error) => {
                    return Err(E2eError::system(format!(
                        "opening pidfd for {pid}: {error}"
                    )));
                }
            };
            let timeout = Timespec {
                tv_sec: i64::try_from(timeout.as_secs())
                    .map_err(|_| E2eError::system("process wait timeout is too large"))?,
                tv_nsec: timeout.subsec_nanos().into(),
            };
            let mut fds = [PollFd::new(&fd, PollFlags::IN)];
            poll(&mut fds, Some(&timeout))
                .map(|ready| ready != 0)
                .map_err(|error| E2eError::system(format!("waiting for process {pid}: {error}")))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (pid, timeout);
            Err(E2eError::system(
                "waiting for process exit using pidfd is Linux-only",
            ))
        }
    }

    fn freeze(&self, cgroup: &Path, frozen: bool, timeout: Duration) -> Result<bool> {
        #[cfg(target_os = "linux")]
        {
            use std::mem::MaybeUninit;
            use std::time::Instant;

            use rustix::event::{PollFd, PollFlags, Timespec, poll};
            use rustix::fs::inotify::{CreateFlags, Reader, WatchFlags, add_watch, init};
            use rustix::io::Errno;

            let events = cgroup.join("cgroup.events");
            let wanted = if frozen { "frozen 1" } else { "frozen 0" };
            let fd = init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK).map_err(|error| {
                E2eError::system(format!("opening inotify for {}: {error}", cgroup.display()))
            })?;
            match add_watch(&fd, &events, WatchFlags::MODIFY) {
                Ok(_) => {}
                Err(Errno::NOENT) => return Ok(false),
                Err(error) => {
                    return Err(E2eError::system(format!(
                        "watching cgroup {}: {error}",
                        cgroup.display()
                    )));
                }
            }
            match fs::write(cgroup.join("cgroup.freeze"), if frozen { "1" } else { "0" }) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
                Err(error) => {
                    return Err(E2eError::io(format!(
                        "writing freezer for cgroup {}",
                        cgroup.display()
                    ))(error));
                }
            }
            let deadline = Instant::now() + timeout;
            let mut buffer = [MaybeUninit::uninit(); 4096];
            let mut reader = Reader::new(&fd, &mut buffer);
            loop {
                let content = match fs::read_to_string(&events) {
                    Ok(content) => content,
                    Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
                    Err(error) => {
                        return Err(E2eError::io(format!(
                            "reading events for cgroup {}",
                            cgroup.display()
                        ))(error));
                    }
                };
                if content.lines().any(|line| line == wanted) {
                    return Ok(true);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                let timeout = Timespec {
                    tv_sec: i64::try_from(remaining.as_secs())
                        .map_err(|_| E2eError::system("cgroup freeze timeout is too large"))?,
                    tv_nsec: remaining.subsec_nanos().into(),
                };
                let mut fds = [PollFd::new(&fd, PollFlags::IN)];
                let ready = poll(&mut fds, Some(&timeout)).map_err(|error| {
                    E2eError::system(format!("waiting for cgroup {}: {error}", cgroup.display()))
                })?;
                if ready == 0 {
                    return Ok(false);
                }
                loop {
                    match reader.next() {
                        Ok(_) => {}
                        Err(Errno::WOULDBLOCK) => break,
                        Err(error) => {
                            return Err(E2eError::system(format!(
                                "reading notifications for cgroup {}: {error}",
                                cgroup.display()
                            )));
                        }
                    }
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (cgroup, frozen, timeout);
            Err(E2eError::system("freezing a cgroup is Linux-only"))
        }
    }

    fn worker_only_environment(&self, worker: u32) -> Result<Vec<String>> {
        if !cfg!(target_os = "linux") {
            return Err(E2eError::system(
                "process environments are read from /proc, which exists only on Linux",
            ));
        }
        let content = fs::read(format!("/proc/{worker}/environ"))
            .map_err(E2eError::io(format!("reading worker {worker} environment")))?;
        let own: HashSet<String> = std::env::vars_os()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .collect();
        let mut names: Vec<String> = content
            .split(|byte| *byte == 0)
            .filter(|item| !item.is_empty())
            .filter_map(|item| item.split(|byte| *byte == b'=').next())
            .map(String::from_utf8_lossy)
            .map(std::borrow::Cow::into_owned)
            .filter(|name| !own.contains(name))
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    fn readonly_bind_paths(&self) -> Result<Vec<String>> {
        let content =
            fs::read_to_string(&self.config_file).map_err(E2eError::io("reading gang.toml"))?;
        let document =
            Document::<Plugins>::parse(&content).map_err(E2eError::parse("gang.toml"))?;
        let bwrap = document.sandbox.bwrap.ok_or_else(|| {
            E2eError::system("[sandbox.bwrap] readonly_bind_paths is not a list of paths")
        })?;
        Ok(bwrap
            .readonly_bind_paths
            .iter()
            .map(|path| path.as_path().to_string_lossy().into_owned())
            .collect())
    }

    fn temp_dirs(&self) -> Result<Vec<PathBuf>> {
        let entries = match fs::read_dir(&self.noop_scratch) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error).map_err(E2eError::io("listing noop scratch")),
        };
        let mut dirs = Vec::new();
        for entry in entries {
            let entry = entry.map_err(E2eError::io("reading noop scratch entry"))?;
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with("gang-action-")
            {
                dirs.push(entry.path());
            }
        }
        dirs.sort();
        Ok(dirs)
    }

    fn path_exists(&self, path: &Path) -> Result<bool> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).map_err(E2eError::io(format!("checking {}", path.display()))),
        }
    }

    fn request_preemption(&self, instance: &str) -> Result<()> {
        let dir = self.state_dir.join("instances").join(instance);
        if !dir.join("instance.json").exists() {
            return Err(E2eError::system(format!(
                "Local Mode instance {instance} has no record"
            )));
        }
        let bytes = serde_json::to_vec(&SystemClock::new().wall())
            .map_err(E2eError::system_from("encoding the preemption time"))?;
        let temporary = dir.join("preemption.json.e2e");
        fs::write(&temporary, bytes).map_err(E2eError::io("writing the preemption notice"))?;
        fs::rename(temporary, dir.join("preemption.json"))
            .map_err(E2eError::io("renaming the preemption notice"))
    }

    fn identity_generations(&self, instance: &str) -> Result<Vec<u64>> {
        let path = self
            .state_dir
            .join("instances")
            .join(instance)
            .join("scratch/identity");
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error).map_err(E2eError::io("listing identity generations")),
        };
        let mut generations = Vec::new();
        for entry in entries {
            let entry = entry.map_err(E2eError::io("reading an identity generation"))?;
            if let Some(generation) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse().ok())
            {
                generations.push(generation);
            }
        }
        generations.sort_unstable();
        Ok(generations)
    }

    fn certificate_not_after(&self, instance: &str, generation: u64) -> Result<SystemTime> {
        let path = self
            .state_dir
            .join("instances")
            .join(instance)
            .join("scratch/identity")
            .join(generation.to_string())
            .join("certificate.pem");
        let pem = fs::read(&path).map_err(E2eError::io(format!("reading {}", path.display())))?;
        let certificate =
            Certificate::from_pem(&pem).map_err(E2eError::parse("the worker certificate"))?;
        let validity = certificate.tbs_certificate().validity();
        Ok(SystemTime::UNIX_EPOCH + validity.not_after.to_unix_duration())
    }

    fn generations_at(&self, instance: &str, at: SystemTime) -> Result<Generations> {
        if let Ok(remaining) = at.duration_since(SystemTime::now()) {
            std::thread::sleep(remaining);
        }
        let held = self.identity_generations(instance)?;
        Ok(Generations {
            held,
            read_at: SystemTime::now(),
        })
    }

    fn start_refused_server(&self) -> Result<ServerExit> {
        let cli = GangCli::new(
            self.binary.clone(),
            self.config_file.clone(),
            self.home.clone(),
        );
        let mut server = Supervised::spawn(&mut cli.server(), &self.server_log)?;
        let announced = server
            .await_announcements(self.exit_timeout, |line| {
                line.trim().parse::<Announcement>().is_ok()
            })?
            .is_some();
        // A server that announced may still refuse its configuration once it runs: wait, once and
        // bounded, for its output to close. One that keeps running is killed and has no exit code.
        let exited = !announced
            || match server.await_announcements(self.exit_timeout, |_| false) {
                Ok(_) => true,
                Err(E2eError::Timeout { .. }) => false,
                Err(error) => return Err(error),
            };
        let code = if exited {
            server.terminate(self.exit_timeout)?.code()
        } else {
            None
        };
        let log =
            fs::read_to_string(&self.server_log).map_err(E2eError::io("reading the server log"))?;
        Ok(ServerExit {
            announced,
            code,
            log,
        })
    }

    fn server_log_tail(&self) -> Result<String> {
        let bytes = match fs::read(&self.server_log) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(String::new()),
            Err(error) => return Err(error).map_err(E2eError::io("reading the server log")),
        };
        Ok(tail(&bytes, self.log_tail_bytes))
    }

    fn worker_logs(&self) -> Result<String> {
        worker_logs(&self.state_dir)
    }
}
