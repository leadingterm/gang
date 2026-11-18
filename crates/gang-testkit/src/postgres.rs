//! A real `PostgreSQL` test server unpacked from the pinned archive supplied by the build graph
//! (`MODULE.bazel`), never from the host installation. Pausing requires Linux `/proc` and stops
//! the postmaster and its child processes.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::net::TcpListener;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak, mpsc};
use std::time::Duration;

use flate2::read::GzDecoder;
use parking_lot::Mutex;
use rustix::process::{Pid, Signal, kill_process, kill_process_group};
use tempfile::TempDir;

/// Environment variable containing the pinned `PostgreSQL` archive path.
pub const POSTGRES_ARCHIVE_ENV: &str = "GANG_TEST_POSTGRES";
/// The superuser created in each test cluster.
pub const SUPERUSER: &str = "postgres";

const SOCKET_PORT: u16 = 5432;
const START_TIMEOUT: Duration = Duration::from_mins(1);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const LOG_LINES: usize = 2000;

#[cfg(all(test, target_os = "linux"))]
type TimeoutHook = Mutex<Option<Box<dyn FnOnce(Pid)>>>;

#[cfg(all(test, target_os = "linux"))]
thread_local! {
    static START_TIMEOUT_HOOK: TimeoutHook = Mutex::new(None);
    static DROP_TIMEOUT_HOOK: TimeoutHook = Mutex::new(None);
}

#[cfg(all(test, target_os = "linux"))]
fn startup_timeout_for_test(
    pid: Pid,
    rx: &mpsc::Receiver<()>,
) -> Option<Result<(), mpsc::RecvTimeoutError>> {
    START_TIMEOUT_HOOK
        .with(|hook| hook.lock().take())
        .map(|hook| {
            let result = rx.recv_timeout(Duration::ZERO);
            hook(pid);
            result
        })
}
const TLS_ATTEMPTS: usize = 5;

/// An error starting or using a `PostgreSQL` test server.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// The archive environment variable is not set.
    #[error("PostgreSQL archive environment variable is unset")]
    Env,
    /// A filesystem or process operation failed.
    #[error("failed to {action}")]
    Io {
        /// The operation being performed.
        action: &'static str,
        /// The underlying error.
        #[source]
        source: io::Error,
    },
    /// Initialization exited unsuccessfully.
    #[error("initdb failed: {output}")]
    Initdb {
        /// Combined standard error and standard output.
        output: String,
    },
    /// The postmaster exited before it was ready.
    #[error("PostgreSQL exited before ready: {log}")]
    Exited {
        /// Recent server log lines.
        log: String,
    },
    /// The postmaster did not become ready before the deadline.
    #[error("PostgreSQL was not ready before the deadline: {log}")]
    NotReady {
        /// Recent server log lines.
        log: String,
    },
    /// A database connection or command failed.
    #[error("PostgreSQL connection or command failed")]
    Connect(#[source] tokio_postgres::Error),
    /// Signalling the server failed.
    #[error("signalling PostgreSQL failed")]
    Signal(#[source] rustix::io::Errno),
    /// The requested operation is unavailable on this platform.
    #[error("{0}")]
    Unsupported(&'static str),
}

fn io_error(action: &'static str) -> impl FnOnce(io::Error) -> PostgresError {
    move |source| PostgresError::Io { action, source }
}

/// An isolated `PostgreSQL` server and its temporary installation and data.
pub struct PostgresServer {
    child: Option<Child>,
    pid: Pid,
    port: u16,
    socket: TempDir,
    log: Arc<Mutex<Vec<String>>>,
    _data: TempDir,
    _install: TempDir,
    next_database: AtomicU64,
}

struct PreparedServer {
    install: TempDir,
    data: TempDir,
    bin: PathBuf,
    tls_files: Option<(PathBuf, PathBuf)>,
}

impl PostgresServer {
    /// Unpacks the archive and starts a server listening only on a Unix socket.
    ///
    /// # Errors
    /// Returns an error if the archive, initialization, or server startup fails.
    pub fn start(archive: &Path) -> Result<Self, PostgresError> {
        Self::start_with(archive, &[])
    }

    /// Starts a Unix-socket server with additional `PostgreSQL` configuration settings.
    ///
    /// Settings are passed as `-c key=value` arguments after the server defaults.
    ///
    /// # Errors
    /// Returns an error if the archive, initialization, or server startup fails.
    pub fn start_with(archive: &Path, settings: &[(&str, &str)]) -> Result<Self, PostgresError> {
        Self::start_inner(archive, None, settings)
    }

    /// Starts a Unix-socket server using the archive named by [`POSTGRES_ARCHIVE_ENV`].
    ///
    /// # Errors
    /// Returns an error if the environment variable is absent or startup fails.
    pub fn from_env() -> Result<Self, PostgresError> {
        let archive = std::env::var_os(POSTGRES_ARCHIVE_ENV).ok_or(PostgresError::Env)?;
        Self::start(Path::new(&archive))
    }

    /// Starts a server with TLS on loopback in addition to its Unix socket.
    ///
    /// # Errors
    /// Returns an error if the files, initialization, or server startup fail.
    pub fn start_tls(
        archive: &Path,
        certificate: &Path,
        key: &Path,
    ) -> Result<Self, PostgresError> {
        Self::start_inner(archive, Some((certificate, key)), &[])
    }

    fn prepare(
        archive: &Path,
        tls: Option<(&Path, &Path)>,
    ) -> Result<PreparedServer, PostgresError> {
        let install =
            tempfile::tempdir().map_err(io_error("create PostgreSQL installation directory"))?;
        let file = File::open(archive).map_err(io_error("open PostgreSQL archive"))?;
        tar::Archive::new(GzDecoder::new(file))
            .unpack(install.path())
            .map_err(io_error("unpack PostgreSQL archive"))?;
        let mut entries =
            fs::read_dir(install.path()).map_err(io_error("read PostgreSQL installation"))?;
        let root = entries
            .next()
            .transpose()
            .map_err(io_error("read PostgreSQL installation"))?
            .ok_or_else(|| {
                io_error("read PostgreSQL installation")(io::Error::other(
                    "archive has no root directory",
                ))
            })?;
        if !root
            .file_type()
            .map_err(io_error("inspect PostgreSQL installation"))?
            .is_dir()
            || entries
                .next()
                .transpose()
                .map_err(io_error("read PostgreSQL installation"))?
                .is_some()
        {
            return Err(io_error("read PostgreSQL installation")(io::Error::other(
                "archive must contain one root directory",
            )));
        }
        let bin = root.path().join("bin");
        let data = tempfile::tempdir().map_err(io_error("create PostgreSQL data directory"))?;
        let pgdata = data.path().join("pgdata");
        let output = Command::new(bin.join("initdb"))
            .arg("-D")
            .arg(&pgdata)
            .args([
                "-U",
                SUPERUSER,
                "--auth=trust",
                "--encoding=UTF8",
                "--locale=C",
                "--no-sync",
            ])
            .output()
            .map_err(io_error("run initdb"))?;
        if !output.status.success() {
            return Err(PostgresError::Initdb {
                output: format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stderr),
                    String::from_utf8_lossy(&output.stdout)
                ),
            });
        }
        let tls_files = if let Some((certificate, key)) = tls {
            let cert_copy = data.path().join("server.crt");
            let key_copy = data.path().join("server.key");
            fs::copy(certificate, &cert_copy).map_err(io_error("copy PostgreSQL certificate"))?;
            let mut source = File::open(key).map_err(io_error("open PostgreSQL key"))?;
            let mut dest = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&key_copy)
                .map_err(io_error("create PostgreSQL key"))?;
            io::copy(&mut source, &mut dest).map_err(io_error("copy PostgreSQL key"))?;
            Some((cert_copy, key_copy))
        } else {
            None
        };
        Ok(PreparedServer {
            install,
            data,
            bin,
            tls_files,
        })
    }

    fn start_inner(
        archive: &Path,
        tls: Option<(&Path, &Path)>,
        settings: &[(&str, &str)],
    ) -> Result<Self, PostgresError> {
        let PreparedServer {
            install,
            data,
            bin,
            tls_files,
        } = Self::prepare(archive, tls)?;
        let pgdata = data.path().join("pgdata");
        // Unix socket paths are short; Bazel's test temporary directory may exceed the OS limit.
        let socket = tempfile::Builder::new()
            .prefix("pg")
            .tempdir_in("/tmp")
            .map_err(io_error("create PostgreSQL socket directory"))?;
        let mut last_error = None;
        for _ in 0..if tls_files.is_some() { TLS_ATTEMPTS } else { 1 } {
            let port = Self::select_port(tls_files.is_some())?;
            let mut command = Self::server_command(
                &bin,
                &pgdata,
                socket.path(),
                port,
                tls_files.as_ref(),
                settings,
            );
            let mut child = command.spawn().map_err(io_error("start PostgreSQL"))?;
            let Some(pid) = i32::try_from(child.id()).ok().and_then(Pid::from_raw) else {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io_error("read PostgreSQL process id")(io::Error::other(
                    "invalid process id",
                )));
            };
            let stderr = child.stderr.take().ok_or_else(|| {
                io_error("capture PostgreSQL log")(io::Error::other("stderr was not piped"))
            })?;
            let log = Arc::new(Mutex::new(Vec::<String>::new()));
            let reader_log = Arc::clone(&log);
            let (tx, rx) = mpsc::channel();
            #[cfg(all(test, target_os = "linux"))]
            let test_result = startup_timeout_for_test(pid, &rx);
            std::thread::spawn(move || {
                let mut ready = false;
                for line in BufReader::new(stderr).lines() {
                    let Ok(line) = line else { break };
                    let is_ready = line.contains("database system is ready to accept connections");
                    let mut lines = reader_log.lock();
                    if lines.len() == LOG_LINES {
                        lines.remove(0);
                    }
                    lines.push(line);
                    drop(lines);
                    if is_ready && !ready {
                        let _ = tx.send(());
                        ready = true;
                    }
                }
            });
            #[cfg(all(test, target_os = "linux"))]
            let result = test_result.unwrap_or_else(|| rx.recv_timeout(START_TIMEOUT));
            #[cfg(not(all(test, target_os = "linux")))]
            let result = rx.recv_timeout(START_TIMEOUT);
            if result.is_ok() {
                return Ok(Self {
                    child: Some(child),
                    pid,
                    port,
                    socket,
                    log,
                    _data: data,
                    _install: install,
                    next_database: AtomicU64::new(0),
                });
            }
            let exited = if matches!(result, Err(mpsc::RecvTimeoutError::Disconnected)) {
                child.wait().map_err(io_error("wait for PostgreSQL"))?;
                true
            } else {
                child
                    .try_wait()
                    .map_err(io_error("check PostgreSQL process"))?
                    .is_some()
            };
            let text = log.lock().join("\n");
            if !exited {
                kill_server(pid);
                let _ = child.wait();
            }
            let retry = tls_files.is_some() && exited && text.contains("could not bind");
            let error = if exited {
                PostgresError::Exited { log: text }
            } else {
                PostgresError::NotReady { log: text }
            };
            if !retry {
                return Err(error);
            }
            last_error = Some(error);
        }
        Err(last_error.unwrap_or_else(|| {
            io_error("start PostgreSQL")(io::Error::other("no startup attempts"))
        }))
    }

    fn select_port(tls: bool) -> Result<u16, PostgresError> {
        if tls {
            let listener =
                TcpListener::bind(("127.0.0.1", 0)).map_err(io_error("select PostgreSQL port"))?;
            Ok(listener
                .local_addr()
                .map_err(io_error("read PostgreSQL port"))?
                .port())
        } else {
            Ok(SOCKET_PORT)
        }
    }

    fn server_command(
        bin: &Path,
        pgdata: &Path,
        socket: &Path,
        port: u16,
        tls_files: Option<&(PathBuf, PathBuf)>,
        settings: &[(&str, &str)],
    ) -> Command {
        let mut command = Command::new(bin.join("postgres"));
        command
            .arg("-D")
            .arg(pgdata)
            .arg("-p")
            .arg(port.to_string())
            .arg("-k")
            .arg(socket)
            .arg("-c")
            .arg(if tls_files.is_some() {
                "listen_addresses=127.0.0.1"
            } else {
                "listen_addresses="
            })
            .args([
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
                "-c",
                "full_page_writes=off",
                "-c",
                "max_connections=300",
                "-c",
                "dynamic_shared_memory_type=mmap",
                "-c",
                "logging_collector=off",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .process_group(0);
        if let Some((cert, key)) = tls_files {
            command
                .args(["-c", "ssl=on"])
                .arg("-c")
                .arg(format!("ssl_cert_file={}", cert.display()))
                .arg("-c")
                .arg(format!("ssl_key_file={}", key.display()));
        }
        for (key, value) in settings {
            command.arg("-c").arg(format!("{key}={value}"));
        }
        command
    }

    /// Returns the retained server log lines in chronological order.
    #[must_use]
    pub fn log(&self) -> Vec<String> {
        self.log.lock().clone()
    }

    /// Returns the directory containing the server's Unix socket.
    #[must_use]
    pub fn socket_dir(&self) -> &Path {
        self.socket.path()
    }

    /// Returns the server's socket and TCP port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the postmaster process ID.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid.as_raw_nonzero().get().unsigned_abs()
    }

    /// Creates a fresh database and returns its name.
    ///
    /// # Errors
    /// Returns an error if connecting or creating the database fails.
    pub async fn create_database(&self) -> Result<String, PostgresError> {
        let (client, connection) = tokio_postgres::Config::new()
            .host_path(self.socket_dir())
            .port(self.port)
            .user(SUPERUSER)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(PostgresError::Connect)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let name = format!(
            "gang_test_{}",
            self.next_database.fetch_add(1, Ordering::Relaxed)
        );
        client
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .map_err(PostgresError::Connect)?;
        Ok(name)
    }

    /// Stops the postmaster and its children until resumed, using Linux `/proc`.
    ///
    /// # Errors
    /// Returns an error if process discovery or signalling fails, or on non-Linux platforms.
    pub fn pause(&self) -> Result<(), PostgresError> {
        #[cfg(target_os = "linux")]
        {
            kill_process(self.pid, Signal::STOP).map_err(PostgresError::Signal)?;
            if let Err(error) = signal_children(self.pid, Signal::STOP) {
                let _ = self.resume();
                return Err(error);
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(PostgresError::Unsupported("pausing needs Linux /proc"))
        }
    }

    /// Resumes the postmaster and its children using Linux `/proc`.
    ///
    /// # Errors
    /// Returns an error if process discovery or signalling fails, or on non-Linux platforms.
    pub fn resume(&self) -> Result<(), PostgresError> {
        #[cfg(target_os = "linux")]
        {
            let children = signal_children(self.pid, Signal::CONT);
            let postmaster = kill_process(self.pid, Signal::CONT).map_err(PostgresError::Signal);
            children.and(postmaster)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(PostgresError::Unsupported("pausing needs Linux /proc"))
        }
    }
}

#[cfg(target_os = "linux")]
fn signal_children(parent: Pid, signal: Signal) -> Result<(), PostgresError> {
    let entries = fs::read_dir("/proc").map_err(io_error("read process directory"))?;
    for entry in entries {
        let entry = entry.map_err(io_error("read process directory entry"))?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
            .and_then(Pid::from_raw)
        else {
            continue;
        };
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error("read process status")(error)),
        };
        let parent_id = stat
            .rsplit_once(')')
            .and_then(|(_, fields)| fields.split_whitespace().nth(1))
            .and_then(|value| value.parse::<i32>().ok());
        if parent_id == Some(parent.as_raw_nonzero().get()) {
            match kill_process(pid, signal) {
                Ok(()) | Err(rustix::io::Errno::SRCH) => {}
                Err(error) => return Err(PostgresError::Signal(error)),
            }
        }
    }
    Ok(())
}

fn kill_server(pid: Pid) {
    #[cfg(target_os = "linux")]
    let _ = signal_children(pid, Signal::KILL);
    let _ = kill_process_group(pid, Signal::KILL);
}

impl Drop for PostgresServer {
    fn drop(&mut self) {
        let _ = self.resume();
        #[cfg(all(test, target_os = "linux"))]
        let stop_timeout = if let Some(hook) = DROP_TIMEOUT_HOOK.with(|hook| hook.lock().take()) {
            hook(self.pid);
            Duration::ZERO
        } else {
            STOP_TIMEOUT
        };
        #[cfg(not(all(test, target_os = "linux")))]
        let stop_timeout = STOP_TIMEOUT;
        let _ = kill_process(self.pid, Signal::INT);
        if let Some(mut child) = self.child.take() {
            let (tx, rx) = mpsc::channel();
            let waiter = std::thread::spawn(move || {
                let _ = tx.send(child.wait());
            });
            if rx.recv_timeout(stop_timeout).is_err() {
                kill_server(self.pid);
            }
            let _ = waiter.join();
        }
    }
}

/// Returns one server per test binary while any holder keeps it; it stops after the last drop.
///
/// # Errors
/// Returns an error if the archive environment variable is absent or startup fails.
pub fn shared_postgres() -> Result<Arc<PostgresServer>, PostgresError> {
    static SHARED: Mutex<Weak<PostgresServer>> = Mutex::new(Weak::new());
    let mut shared = SHARED.lock();
    if let Some(server) = shared.upgrade() {
        return Ok(server);
    }
    let server = Arc::new(PostgresServer::from_env()?);
    *shared = Arc::downgrade(&server);
    Ok(server)
}

/// Tests for server process cleanup.
#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        DROP_TIMEOUT_HOOK, PostgresError, PostgresServer, START_TIMEOUT_HOOK, SUPERUSER,
        kill_server, signal_children,
    };
    use crate::TestResult;
    use rustix::process::{Pid, Signal, kill_process};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn stop_children(parent: Pid) -> TestResult<Vec<Pid>> {
        let deadline = Instant::now() + Duration::from_secs(15);
        let children_file = format!("/proc/{0}/task/{0}/children", parent.as_raw_nonzero());
        loop {
            if !std::fs::read_to_string(&children_file)?.trim().is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "PostgreSQL did not spawn children"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        kill_process(parent, Signal::STOP)?;
        let stat_file = format!("/proc/{}/stat", parent.as_raw_nonzero());
        loop {
            let stat = std::fs::read_to_string(&stat_file)?;
            if stat
                .rsplit_once(')')
                .is_some_and(|(_, fields)| fields.trim_start().starts_with('T'))
            {
                break;
            }
            assert!(Instant::now() < deadline, "postmaster did not stop");
            std::thread::sleep(Duration::from_millis(5));
        }
        signal_children(parent, Signal::STOP)?;
        Ok(std::fs::read_to_string(&children_file)?
            .split_whitespace()
            .filter_map(|value| value.parse().ok().and_then(Pid::from_raw))
            .collect())
    }

    fn assert_children_killed(children: &[Pid]) {
        let deadline = Instant::now() + Duration::from_secs(2);
        let survivors = loop {
            let alive: Vec<_> = children
                .iter()
                .copied()
                .filter(|pid| {
                    std::fs::read_to_string(format!("/proc/{}/stat", pid.as_raw_nonzero()))
                        .is_ok_and(|stat| {
                            stat.rsplit_once(')')
                                .is_some_and(|(_, fields)| !fields.trim_start().starts_with('Z'))
                        })
                })
                .collect();
            if alive.is_empty() || Instant::now() >= deadline {
                break alive;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        for pid in &survivors {
            let _ = kill_process(*pid, Signal::KILL);
        }
        assert!(
            survivors.is_empty(),
            "forced stop left children alive: {survivors:?}"
        );
    }

    #[test]
    fn startup_timeout_kills_stopped_children() -> TestResult {
        let (tx, rx) = mpsc::channel();
        START_TIMEOUT_HOOK.with(|hook| {
            *hook.lock() = Some(Box::new(move |pid| {
                let _ = tx.send(stop_children(pid));
            }));
        });
        let result = PostgresServer::from_env();
        assert!(matches!(result, Err(PostgresError::NotReady { .. })));
        assert_children_killed(&rx.recv()??);
        Ok(())
    }

    #[test]
    fn drop_timeout_kills_stopped_children() -> TestResult {
        let server = PostgresServer::from_env()?;
        let (tx, rx) = mpsc::channel();
        DROP_TIMEOUT_HOOK.with(|hook| {
            *hook.lock() = Some(Box::new(move |pid| {
                let _ = tx.send(stop_children(pid));
            }));
        });
        drop(server);
        assert_children_killed(&rx.recv()??);
        Ok(())
    }

    #[tokio::test]
    async fn kill_server_closes_paused_backend_connections() -> TestResult {
        let server = PostgresServer::from_env()?;
        let (client, connection) = tokio_postgres::Config::new()
            .host_path(server.socket_dir())
            .port(server.port())
            .user(SUPERUSER)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await?;
        let task = tokio::spawn(connection);
        server.pause()?;
        kill_server(server.pid);
        let _ = task.await?;
        drop(client);
        Ok(())
    }
}
