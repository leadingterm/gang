//! Worker action process runner, protected side-channel writer, kill escalation, and subreaper
//! (`§3.3`, `§3.4`, `§6.4`).
//!
//! Prepares `.side_channels/<attempt_id>/` (mode `0700`) with `cpu_weight` and `read_only_trees`,
//! spawns the worker entrypoint (`gang-exec`) in its own process group with [`Command::env_clear`],
//! builds the action environment in exact precedence order (`PWD`/`TMPDIR` defaults, client env,
//! enforced `GANG_*` platform properties + `GANG_SIDE_CHANNEL_FILE`), captures stdout/stderr
//! cleanly with bounded memory, enforces execution timeouts, attempt deadlines, and external kills
//! (`SIGTERM` -> `kill_grace` -> `cgroup.kill` / process-group `SIGKILL` / wrapper-PID fallback),
//! merges sampled usage with the final side-channel report, cleans up `.side_channels/<attempt_id>/`,
//! and returns [`RunOutcome`] ready to emit `WorkerMessage::Executed` before output upload.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::fs::{self, OpenOptions};
use std::hash::BuildHasher;
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Component, Path, PathBuf};
use std::pin::pin;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use gang_clock::{Clock, Timestamp};
use gang_os::ProcessGroup;
use gang_os::fs::{PRIVATE_FILE, ensure_private_dir, remove_dir_optional};
use gang_reapi::google::rpc as rpc_proto;
use gang_reapi::leadingterm::gang::worker::v1 as proto;
use gang_spi::{CPU_COUNT_ENV, MAX_RO_BINDS, MEMORY_KB_ENV, NETWORK_ENV, SIDE_CHANNEL_FILE_ENV};
use gang_types::NonZeroDuration;
use gang_types::{AttemptId, CpuWeight, property};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt as _};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

use crate::worker_sampling::{
    AttemptRegistration, AttemptUsage, KillTrigger, KillWaiter, SamplingRegistry,
    TrackedAttemptGuard, read_cgroup_sample, read_cgroup_side_channel,
};

/// Subdirectory under `<work>` holding worker-written, action-unwritable side-channel directories.
pub const SIDE_CHANNELS_DIR: &str = ".side_channels";

/// Side-channel file carrying decimal [`CpuWeight::get`].
pub const CPU_WEIGHT_FILE: &str = "cpu_weight";

/// Side-channel file carrying `NUL`-delimited `(source, target)` pairs for read-only binds.
pub const READ_ONLY_TREES_FILE: &str = "read_only_trees";

/// Side-channel report file written by `gang exec` after sandbox exit.
pub const REPORT_FILE: &str = "report";

/// Default per-stream stdout/stderr in-memory capture threshold (256 `KiB`).
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 256 * 1024;

/// Interval between orphan-zombie scans by the worker subreaper.
pub const ORPHAN_REAP_INTERVAL: Duration = Duration::from_secs(5);

/// Brief grace given to `gang exec` to finish `leave(scope)` and write `report` after `cgroup.kill`.
const CGROUP_KILL_SETTLE_CAP: Duration = Duration::from_millis(500);

/// Maximum wait to drain remaining stdout/stderr bytes after the direct child has been reaped.
const POST_EXIT_PIPE_DRAIN_CAP: Duration = Duration::from_millis(250);

/// Maximum size accepted when reading `<side_channel_dir>/report`.
const MAX_REPORT_BYTES: u64 = 16 * 1024;

/// Reserved environment variables mapped from admitted `Assign.platform` keys.
const PLATFORM_ENV_MAPPING: [(&str, &str); 3] = [
    (NETWORK_ENV, property::NETWORK),
    (CPU_COUNT_ENV, property::CPU_COUNT),
    (MEMORY_KB_ENV, property::MEMORY_KB),
];

/// One read-only input tree entry written into `.side_channels/<attempt_id>/read_only_trees`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadOnlyBind {
    /// Absolute host path to the immutable cached tree directory.
    pub source: PathBuf,
    /// Relative target path strictly inside `<work>/<attempt_id>` (for example `work/external/sdk`).
    pub target: PathBuf,
}

/// Worker-provided inputs placed in `.side_channels/<attempt_id>/` before spawning `gang exec`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SideChannelInputs {
    /// Optional `cgroup` v2 `cpu.weight` for the action's scope (`1000`, `100`, or `20`).
    pub cpu_weight: Option<CpuWeight>,
    /// Immutable input trees to bind read-only inside the action directory (at most [`MAX_RO_BINDS`]).
    pub read_only_trees: Vec<ReadOnlyBind>,
}

/// Usage and memory-failure facts parsed from `gang exec`'s side-channel `report` file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SideChannelReport {
    /// `true` when `gang exec` recorded `"failure":"memory"` (kernel `OOM` kill in scope).
    pub memory_failure: bool,
    /// Peak memory reported in whole `KiB`, when present.
    pub peak_memory_kb: Option<u64>,
}

/// Errors returned by [`SideChannelDir`], [`encode_read_only_trees`], and [`run_attempt`].
#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    /// More than [`MAX_RO_BINDS`] read-only trees were configured for one attempt.
    #[error("too many read-only trees: {count} exceeds maximum {max}")]
    TooManyReadOnlyTrees {
        /// Number of read-only trees supplied.
        count: usize,
        /// Maximum supported by `gang exec` and `gang-bwrap`.
        max: usize,
    },
    /// A read-only tree source or target path failed validation.
    #[error("invalid read-only tree ({}, {})", source_path.display(), target_path.display())]
    InvalidReadOnlyTree {
        /// Supplied host source path.
        source_path: PathBuf,
        /// Supplied relative target path.
        target_path: PathBuf,
    },
    /// The command working directory escaped `<work>/<attempt_id>/work` or was invalid.
    #[error("invalid working directory `{}`: {reason}", path.display())]
    InvalidWorkingDirectory {
        /// Supplied relative working directory.
        path: PathBuf,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// A filesystem preparation or side-channel I/O operation failed.
    #[error(transparent)]
    Io(#[from] gang_os::PathIo),
    /// Spawning the action entrypoint failed.
    #[error("failed to spawn action entrypoint `{}`", entrypoint.display())]
    Spawn {
        /// Entrypoint executable path.
        entrypoint: PathBuf,
        /// Underlying OS spawn error.
        #[source]
        source: io::Error,
    },
    /// Waiting on the spawned action process failed.
    #[error("failed waiting for action entrypoint `{}`", entrypoint.display())]
    Wait {
        /// Entrypoint executable path.
        entrypoint: PathBuf,
        /// Underlying OS wait error.
        #[source]
        source: io::Error,
    },
}

/// Encodes `entries` into the exact `NUL`-delimited `<source>\0<target>\0` format parsed by
/// `gang-exec/src/side_channel.rs`.
///
/// # Errors
///
/// Returns [`RunnerError::TooManyReadOnlyTrees`] if `entries.len() > MAX_RO_BINDS`, or
/// [`RunnerError::InvalidReadOnlyTree`] if any source is not absolute / contains `..` / contains
/// `NUL`, or any target is not a clean relative path outside `.side_channels` and `.graveyard`.
pub fn encode_read_only_trees(entries: &[ReadOnlyBind]) -> Result<Vec<u8>, RunnerError> {
    if entries.len() > MAX_RO_BINDS {
        return Err(RunnerError::TooManyReadOnlyTrees {
            count: entries.len(),
            max: MAX_RO_BINDS,
        });
    }
    let mut encoded = Vec::new();
    for entry in entries {
        if !valid_bind_source(&entry.source) || !valid_bind_target(&entry.target) {
            return Err(RunnerError::InvalidReadOnlyTree {
                source_path: entry.source.clone(),
                target_path: entry.target.clone(),
            });
        }
        let source_bytes = entry.source.as_os_str().as_bytes();
        let target_bytes = entry.target.as_os_str().as_bytes();
        if source_bytes.is_empty()
            || target_bytes.is_empty()
            || source_bytes.contains(&0)
            || target_bytes.contains(&0)
        {
            return Err(RunnerError::InvalidReadOnlyTree {
                source_path: entry.source.clone(),
                target_path: entry.target.clone(),
            });
        }
        encoded.extend_from_slice(source_bytes);
        encoded.push(0);
        encoded.extend_from_slice(target_bytes);
        encoded.push(0);
    }
    Ok(encoded)
}

fn valid_bind_source(source: &Path) -> bool {
    source.is_absolute()
        && !source
            .components()
            .any(|component| matches!(component, Component::ParentDir))
}

fn valid_bind_target(target: &Path) -> bool {
    let mut components = target.components();
    let Some(Component::Normal(first)) = components.next() else {
        return false;
    };
    if first == SIDE_CHANNELS_DIR || first == ".graveyard" {
        return false;
    }
    components.all(|component| matches!(component, Component::Normal(_)))
}

/// RAII handle for `<work>/.side_channels/<attempt_id>/`.
///
/// Created with mode `0700` before spawning `gang exec` and automatically removed on completion
/// or drop so stale side-channel directories never accumulate.
#[derive(Debug)]
pub struct SideChannelDir {
    dir: PathBuf,
    cleaned: bool,
}

impl SideChannelDir {
    /// Creates `<canonical_work>/.side_channels/<attempt_id>/` (mode `0700`) and writes any
    /// configured `cpu_weight` and `read_only_trees` files (mode `0600`).
    ///
    /// # Errors
    ///
    /// Returns [`RunnerError`] if `inputs` are invalid or if creating or writing the side-channel
    /// directory fails.
    pub fn create(
        work_directory: &Path,
        attempt: &AttemptId,
        inputs: &SideChannelInputs,
    ) -> Result<Self, RunnerError> {
        let encoded_trees = if inputs.read_only_trees.is_empty() {
            None
        } else {
            Some(encode_read_only_trees(&inputs.read_only_trees)?)
        };
        let canonical_work = fs::canonicalize(work_directory).map_err(gang_os::PathIo::at(
            "canonicalize work directory",
            work_directory,
        ))?;
        let side_channels_root = canonical_work.join(SIDE_CHANNELS_DIR);
        let _ = ensure_private_dir(&side_channels_root)?;
        let dir = side_channels_root.join(attempt.as_str());
        let private = ensure_private_dir(&dir)?;
        let guard = Self {
            dir,
            cleaned: false,
        };
        if let Some(weight) = inputs.cpu_weight {
            private.write_atomic(
                CPU_WEIGHT_FILE,
                weight.get().to_string().as_bytes(),
                PRIVATE_FILE,
            )?;
        }
        if let Some(bytes) = encoded_trees {
            private.write_atomic(READ_ONLY_TREES_FILE, &bytes, PRIVATE_FILE)?;
        }
        Ok(guard)
    }

    /// Returns the canonical path to `<work>/.side_channels/<attempt_id>`.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the canonical path passed in `GANG_SIDE_CHANNEL_FILE`
    /// (`<work>/.side_channels/<attempt_id>/report`).
    #[must_use]
    pub fn report_path(&self) -> PathBuf {
        self.dir.join(REPORT_FILE)
    }

    /// Reads the validated absolute `cgroup` path from `report.cgroup`, if present.
    #[must_use]
    pub fn read_cgroup_path(&self) -> Option<PathBuf> {
        read_cgroup_side_channel(&self.dir)
    }

    /// Reads and parses `report` written by `gang exec`, returning `Ok(None)` if absent.
    ///
    /// Malformed or oversized reports are logged and treated as `Ok(None)` so a corrupt report
    /// never discards an otherwise valid execution outcome.
    #[must_use]
    pub fn read_report(&self) -> Option<SideChannelReport> {
        let path = self.report_path();
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "cannot open side-channel report");
                return None;
            }
        };
        let mut bytes = Vec::new();
        if let Err(error) =
            Read::take(file, MAX_REPORT_BYTES.saturating_add(1)).read_to_end(&mut bytes)
        {
            tracing::warn!(path = %path.display(), %error, "cannot read side-channel report");
            return None;
        }
        if u64::try_from(bytes.len()).is_ok_and(|len| len > MAX_REPORT_BYTES) {
            tracing::warn!(path = %path.display(), "side-channel report exceeds size bound");
            return None;
        }
        let Ok(text) = std::str::from_utf8(&bytes) else {
            tracing::warn!(path = %path.display(), "side-channel report is not valid UTF-8");
            return None;
        };
        if let Some(report) = parse_side_channel_report(text) {
            Some(report)
        } else {
            tracing::warn!(path = %path.display(), "ignoring malformed side-channel report");
            None
        }
    }

    /// Explicitly removes `<work>/.side_channels/<attempt_id>/`.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerError::Io`] if directory removal fails for a reason other than absence.
    pub fn cleanup(mut self) -> Result<(), RunnerError> {
        self.cleaned = true;
        remove_dir_optional(&self.dir)?;
        Ok(())
    }
}

impl Drop for SideChannelDir {
    fn drop(&mut self) {
        if !self.cleaned {
            self.cleaned = true;
            if let Err(error) = remove_dir_optional(&self.dir) {
                tracing::warn!(
                    dir = %self.dir.display(),
                    error = %gang_types::error_chain(&error),
                    "failed to remove attempt side-channel directory on drop"
                );
            }
        }
    }
}

/// Parses the compact `JSON` object written by `gang-exec/src/side_channel.rs` (`failure` and/or
/// `peak_memory_kb`).
#[must_use]
pub fn parse_side_channel_report(text: &str) -> Option<SideChannelReport> {
    let trimmed = text.trim();
    let body = trimmed.strip_prefix('{')?.strip_suffix('}')?.trim();
    if body.is_empty() {
        return Some(SideChannelReport::default());
    }
    let mut report = SideChannelReport::default();
    let mut rest = body;
    loop {
        rest = rest.trim_start();
        let (key, after_key) = parse_json_string(rest)?;
        let after_colon = after_key.trim_start().strip_prefix(':')?.trim_start();
        let remaining = match key.as_str() {
            "failure" => {
                let (value, tail) = parse_json_string(after_colon)?;
                if value == "memory" {
                    report.memory_failure = true;
                }
                tail
            }
            "peak_memory_kb" => {
                let (value, tail) = parse_json_u64(after_colon)?;
                report.peak_memory_kb = Some(value);
                tail
            }
            _ => {
                // Skip unknown scalar JSON values defensively.
                skip_json_scalar(after_colon)?
            }
        };
        let remaining = remaining.trim_start();
        if remaining.is_empty() {
            return Some(report);
        }
        rest = remaining.strip_prefix(',')?;
    }
}

fn parse_json_string(input: &str) -> Option<(String, &str)> {
    let mut chars = input.strip_prefix('"')?.char_indices();
    let mut out = String::new();
    while let Some((index, ch)) = chars.next() {
        match ch {
            '"' => return Some((out, &input[index + 2..])),
            '\\' => {
                let (_, escaped) = chars.next()?;
                match escaped {
                    '"' | '\\' | '/' => out.push(escaped),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    _ => return None,
                }
            }
            ch if ch.is_control() => return None,
            ch => out.push(ch),
        }
    }
    None
}

fn parse_json_u64(input: &str) -> Option<(u64, &str)> {
    let len = input.bytes().take_while(u8::is_ascii_digit).count();
    if len == 0 {
        return None;
    }
    let value = input[..len].parse::<u64>().ok()?;
    Some((value, &input[len..]))
}

fn skip_json_scalar(input: &str) -> Option<&str> {
    if input.starts_with('"') {
        parse_json_string(input).map(|(_, tail)| tail)
    } else if let Some(tail) = input
        .strip_prefix("true")
        .or_else(|| input.strip_prefix("false"))
        .or_else(|| input.strip_prefix("null"))
    {
        Some(tail)
    } else {
        let len = input
            .bytes()
            .take_while(|byte| {
                byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E')
            })
            .count();
        (len > 0).then(|| &input[len..])
    }
}

/// Builds the complete clean action environment in exact `§6.4` precedence order:
///
/// 1. Defaults `PWD` = canonical working directory and `TMPDIR` = canonical `<attempt>/tmp`.
/// 2. Client `Command.environment_variables` (may override `PWD`/`TMPDIR`; preserves non-reserved
///    `GANG_*` variables).
/// 3. Reserved `GANG_*` variables from the admitted `platform` (`GANG_NETWORK`, `GANG_CPU_COUNT`,
///    `GANG_MEMORY_KB` — removing any that are absent or empty) and `GANG_SIDE_CHANNEL_FILE`, which
///    always win.
///
/// `cpu_weight` is intentionally excluded from the environment so action cache keys remain
/// unchanged.
#[must_use]
pub fn build_action_environment<S: BuildHasher>(
    working_directory: &Path,
    tmp_directory: &Path,
    client_environment: &[(OsString, OsString)],
    platform: &HashMap<String, String, S>,
    side_channel_report: &Path,
) -> Vec<(OsString, OsString)> {
    let mut env = BTreeMap::<OsString, OsString>::new();
    env.insert(
        OsString::from("PWD"),
        working_directory.as_os_str().to_os_string(),
    );
    env.insert(
        OsString::from("TMPDIR"),
        tmp_directory.as_os_str().to_os_string(),
    );
    for (key, value) in client_environment {
        env.insert(key.clone(), value.clone());
    }
    for (env_key, platform_key) in PLATFORM_ENV_MAPPING {
        let key = OsStr::new(env_key);
        match platform.get(platform_key).filter(|value| !value.is_empty()) {
            Some(value) => {
                env.insert(key.to_os_string(), OsString::from(value));
            }
            None => {
                env.remove(key);
            }
        }
    }
    env.insert(
        OsString::from(SIDE_CHANNEL_FILE_ENV),
        side_channel_report.as_os_str().to_os_string(),
    );
    env.into_iter().collect()
}

/// Complete stdout or stderr captured in memory or in an attempt-owned spool file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapturedOutput {
    /// Complete output bytes retained within the in-memory threshold.
    Bytes(Vec<u8>),
    /// Attempt-owned file containing the complete stream after spilling from memory.
    File(PathBuf),
}

impl Default for CapturedOutput {
    fn default() -> Self {
        Self::Bytes(Vec::new())
    }
}

/// Wall-clock start and finish timestamps for one execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTimestamps {
    /// Wall-clock time when the action entrypoint was spawned.
    pub started: Timestamp,
    /// Wall-clock time when all action processes had exited.
    pub finished: Timestamp,
}

/// Complete outcome of executing one attempt, ready to send `WorkerMessage::Executed` before
/// output upload starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    /// Process exit code (`128 + signal` when terminated by a signal).
    pub exit_code: i32,
    /// Captured standard output.
    pub stdout: CapturedOutput,
    /// Captured standard error.
    pub stderr: CapturedOutput,
    /// Measured resource usage merged from live sampling and the final side-channel report.
    pub usage: AttemptUsage,
    /// Kill classification when terminated by timeout, deadline, cancellation, pressure, limit,
    /// or `OOM`.
    pub kill: Option<proto::KillReason>,
    /// Wall-clock start timestamp.
    pub started: Timestamp,
    /// Wall-clock finish timestamp.
    pub finished: Timestamp,
}

impl RunOutcome {
    /// Returns the execution start and finish timestamps.
    #[must_use]
    pub const fn timestamps(&self) -> ExecutionTimestamps {
        ExecutionTimestamps {
            started: self.started,
            finished: self.finished,
        }
    }

    /// Builds the `worker.v1::Executed` protobuf frame for `attempt`.
    #[must_use]
    pub fn to_executed_proto(&self, attempt: &AttemptId) -> proto::Executed {
        let kill = self.kill.map(|reason| proto::Kill {
            attempt: attempt.as_str().to_owned(),
            reason: kill_reason_i32(reason),
        });
        let status = self.kill.map(kill_reason_status);
        proto::Executed {
            attempt: attempt.as_str().to_owned(),
            usage: Some(self.usage.to_proto()),
            kill,
            status,
            exit_code: self.exit_code,
            started: Some(timestamp_to_proto(self.started)),
            finished: Some(timestamp_to_proto(self.finished)),
        }
    }
}

/// Specification for executing a single staged attempt via [`run_attempt`].
#[derive(Debug, Clone)]
pub struct RunSpec {
    /// Validated attempt identifier (`<work>/<attempt>`).
    pub attempt: AttemptId,
    /// Absolute path to the worker entrypoint (`gang-exec`).
    pub entrypoint: PathBuf,
    /// Action command and arguments passed after `entrypoint`.
    pub argv: Vec<OsString>,
    /// Worker work root (`<work>`).
    pub work_directory: PathBuf,
    /// Relative command working directory inside `<work>/<attempt>/work` (empty for `work` itself).
    pub working_directory: PathBuf,
    /// Client `Command.environment_variables`.
    pub environment: Vec<(OsString, OsString)>,
    /// All declared output paths relative to the working directory.
    pub output_paths: Vec<PathBuf>,
    /// Authoritative admitted platform properties from `Assign.platform`.
    pub platform: HashMap<String, String>,
    /// Worker-provided side-channel inputs (`cpu_weight` and `read_only_trees`).
    pub side_channel: SideChannelInputs,
    /// Reserved memory in bytes for soft-ceiling and pressure-victim ranking.
    pub reserved_memory_bytes: u64,
    /// Reserved disk in bytes for soft-ceiling and pressure-victim ranking.
    pub reserved_disk_bytes: u64,
    /// Action execution timeout starting when the process spawns.
    pub timeout: Option<Duration>,
    /// Absolute wall-clock attempt deadline from `Assign.attempt_deadline`.
    pub attempt_deadline: Option<Timestamp>,
    /// Grace period between `SIGTERM` and forced `cgroup.kill` / `SIGKILL` escalation.
    pub kill_grace: NonZeroDuration,
    /// Maximum stdout/stderr bytes retained per stream in memory before spilling to a file.
    pub max_output_bytes: usize,
}

/// Executes one staged action attempt under `spec`, returning [`RunOutcome`] once the process and
/// its process group have exited and final usage has been collected.
///
/// Always cleans up `.side_channels/<attempt_id>/` and unregisters from `sampler` on completion or
/// early cancellation/drop.
///
/// # Errors
///
/// Returns [`RunnerError`] if working-directory validation, side-channel creation, process spawn,
/// or process wait fails.
pub async fn run_attempt<K: Clock>(
    spec: &RunSpec,
    clock: &K,
    sampler: Option<&SamplingRegistry>,
    kill_waiter: KillWaiter,
    kill_trigger: KillTrigger,
) -> Result<RunOutcome, RunnerError> {
    let dirs = prepare_attempt_dirs(&spec.work_directory, &spec.attempt, &spec.working_directory)?;
    let side_channel = SideChannelDir::create(&dirs.work_root, &spec.attempt, &spec.side_channel)?;
    publish_ephemeral_dir(
        &dirs.attempt_dir,
        &spec.working_directory,
        &spec.environment,
        &spec.output_paths,
        &spec.side_channel.read_only_trees,
        side_channel.dir(),
    );
    let env = build_action_environment(
        &dirs.working_dir,
        &dirs.tmp_dir,
        &spec.environment,
        &spec.platform,
        &side_channel.report_path(),
    );

    let tracked = sampler.map(|registry| {
        registry.register(AttemptRegistration {
            attempt: spec.attempt.clone(),
            attempt_dir: dirs.attempt_dir.clone(),
            side_channel_dir: side_channel.dir().to_path_buf(),
            reserved_memory_bytes: spec.reserved_memory_bytes,
            reserved_disk_bytes: spec.reserved_disk_bytes,
            kill: kill_trigger.clone(),
        })
    });

    let mut command = Command::new(&spec.entrypoint);
    command
        .args(&spec.argv)
        .current_dir(&dirs.working_dir)
        .env_clear()
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0);

    let started = clock.wall();
    let mut child = command.spawn().map_err(|source| RunnerError::Spawn {
        entrypoint: spec.entrypoint.clone(),
        source,
    })?;
    let group = ProcessGroup::of(&child);
    if let (Some(registry), Some(pid)) = (sampler, child.id()) {
        registry.set_pgid(&spec.attempt, pid);
    }
    let stdout_task = child.stdout.take().map(|pipe| {
        PipeTask(tokio::spawn(drain_bounded(
            pipe,
            spec.max_output_bytes,
            dirs.attempt_dir.join("stdout"),
        )))
    });
    let stderr_task = child.stderr.take().map(|pipe| {
        PipeTask(tokio::spawn(drain_bounded(
            pipe,
            spec.max_output_bytes,
            dirs.attempt_dir.join("stderr"),
        )))
    });

    let (status, kill_reason) = supervise_child(
        &mut child,
        group.as_ref(),
        &side_channel,
        spec,
        started,
        clock,
        &kill_waiter,
    )
    .await?;

    let finished = clock.wall();
    if let Some(group) = group {
        // Leader is already reaped by `child.wait()`; disarm before PID/PGID can be reused.
        group.disarm();
    }

    let (stdout, stderr) = tokio::join!(
        collect_pipe(stdout_task, clock),
        collect_pipe(stderr_task, clock)
    );
    let (usage, kill) =
        collect_final_usage(&side_channel, sampler, &spec.attempt, tracked, kill_reason);

    if let Err(error) = side_channel.cleanup() {
        tracing::warn!(
            attempt = %spec.attempt,
            error = %gang_types::error_chain(&error),
            "failed to clean up attempt side-channel directory"
        );
    }

    Ok(RunOutcome {
        exit_code: exit_code_of(status),
        stdout: stdout?,
        stderr: stderr?,
        usage,
        kill,
        started,
        finished,
    })
}

fn collect_final_usage(
    side_channel: &SideChannelDir,
    sampler: Option<&SamplingRegistry>,
    attempt: &AttemptId,
    tracked: Option<TrackedAttemptGuard>,
    mut kill_reason: Option<proto::KillReason>,
) -> (AttemptUsage, Option<proto::KillReason>) {
    let final_cgroup_path = side_channel
        .read_cgroup_path()
        .or_else(|| sampler.and_then(|registry| registry.cgroup_path(attempt)));
    let final_cgroup_sample = final_cgroup_path.as_deref().map(read_cgroup_sample);
    let report = side_channel.read_report();

    let mut usage = tracked.map_or_else(AttemptUsage::default, TrackedAttemptGuard::finish);
    if let Some(sample) = final_cgroup_sample
        && sample.observed()
    {
        if let Some(current) = sample.current_memory_bytes {
            usage.record_memory(current);
        }
        if let Some(peak) = sample.peak_memory_bytes {
            usage.record_memory(peak);
        }
        if let Some(cpu_time) = sample.cpu_time {
            usage.record_cpu_time(cpu_time);
        }
        usage.record_oom_kills(sample.oom_kills);
    }
    if let Some(report) = report {
        if let Some(peak_kb) = report.peak_memory_kb {
            usage.record_memory(peak_kb.saturating_mul(1024));
        }
        if report.memory_failure {
            usage.record_oom_kills(1);
            if kill_reason.is_none() {
                kill_reason = Some(proto::KillReason::Memory);
            }
        }
    }
    if usage.oom_kills > 0 && kill_reason.is_none() {
        kill_reason = Some(proto::KillReason::Memory);
    }
    (usage, kill_reason)
}

#[allow(deprecated)]
pub(crate) fn declared_output_paths(
    command: &gang_reapi::build::bazel::remote::execution::v2::Command,
) -> Vec<PathBuf> {
    command
        .output_paths
        .iter()
        .chain(&command.output_files)
        .chain(&command.output_directories)
        .map(PathBuf::from)
        .collect()
}

fn publish_ephemeral_dir(
    action: &Path,
    cwd: &Path,
    environment: &[(OsString, OsString)],
    outputs: &[PathBuf],
    binds: &[ReadOnlyBind],
    channel: &Path,
) {
    let Some((_, value)) = environment
        .iter()
        .rev()
        .find(|(name, _)| name == "TEST_TMPDIR")
    else {
        return;
    };
    let scratch = Path::new(value);
    if scratch.as_os_str().is_empty()
        || scratch.as_os_str().as_bytes().contains(&0)
        || scratch
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return;
    }
    let mut target = PathBuf::from("work");
    for component in cwd.components().chain(scratch.components()) {
        if let Component::Normal(part) = component {
            if part == SIDE_CHANNELS_DIR || part == ".graveyard" {
                return;
            }
            target.push(part);
        }
    }
    let mut base = PathBuf::from("work");
    for component in cwd.components() {
        if let Component::Normal(part) = component {
            base.push(part);
        }
    }
    if target == base || target.as_os_str().as_bytes().len() >= 64 * 1024 {
        return;
    }
    for output in outputs {
        if output
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return;
        }
        let mut path = base.clone();
        for part in output.components() {
            if let Component::Normal(part) = part {
                path.push(part);
            }
        }
        if path.starts_with(&target) || target.starts_with(&path) {
            return;
        }
    }
    if binds
        .iter()
        .any(|bind| bind.target.starts_with(&target) || target.starts_with(&bind.target))
    {
        return;
    }
    let Ok(directory) = open_scratch_directory(action, &target) else {
        return;
    };
    let Ok(entries) = rustix::fs::Dir::read_from(&directory) else {
        return;
    };
    for entry in entries {
        let Ok(entry) = entry else { return };
        if !matches!(entry.file_name().to_bytes(), b"." | b"..") {
            return;
        }
    }
    let Ok(private) = ensure_private_dir(channel) else {
        return;
    };
    let mut bytes = target.as_os_str().as_bytes().to_vec();
    bytes.push(0);
    let _ = private.write_atomic("ephemeral_dirs", &bytes, PRIVATE_FILE);
}

fn open_scratch_directory(action: &Path, target: &Path) -> io::Result<OwnedFd> {
    let flags = rustix::fs::OFlags::RDONLY
        | rustix::fs::OFlags::DIRECTORY
        | rustix::fs::OFlags::NOFOLLOW
        | rustix::fs::OFlags::CLOEXEC;
    let mut directory = rustix::fs::open(action, flags, rustix::fs::Mode::empty())?;
    for component in target.components() {
        let Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid scratch directory",
            ));
        };
        match rustix::fs::mkdirat(&directory, name, gang_os::fs::PRIVATE_DIR) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        directory = rustix::fs::openat(&directory, name, flags, rustix::fs::Mode::empty())?;
    }
    Ok(directory)
}

/// Canonicalized directories for one staged attempt (`<work>/<attempt>/{tmp,work/<cwd>}`).
struct AttemptDirs {
    work_root: PathBuf,
    attempt_dir: PathBuf,
    tmp_dir: PathBuf,
    working_dir: PathBuf,
}

fn prepare_attempt_dirs(
    work_directory: &Path,
    attempt: &AttemptId,
    relative_cwd: &Path,
) -> Result<AttemptDirs, RunnerError> {
    if relative_cwd.is_absolute()
        || relative_cwd.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(RunnerError::InvalidWorkingDirectory {
            path: relative_cwd.to_path_buf(),
            reason: "working directory must be relative without parent traversal",
        });
    }
    let work_root = fs::canonicalize(work_directory).map_err(gang_os::PathIo::at(
        "canonicalize work directory",
        work_directory,
    ))?;
    let attempt_dir = work_root.join(attempt.as_str());
    let work_dir = attempt_dir.join("work");
    let tmp_dir = attempt_dir.join("tmp");
    fs::create_dir_all(&work_dir).map_err(gang_os::PathIo::at(
        "create action work directory",
        &work_dir,
    ))?;
    fs::create_dir_all(&tmp_dir)
        .map_err(gang_os::PathIo::at("create action tmp directory", &tmp_dir))?;
    let target_cwd = if relative_cwd.as_os_str().is_empty() {
        work_dir.clone()
    } else {
        work_dir.join(relative_cwd)
    };
    fs::create_dir_all(&target_cwd).map_err(gang_os::PathIo::at(
        "create action working directory",
        &target_cwd,
    ))?;
    let canonical_work_dir = fs::canonicalize(&work_dir).map_err(gang_os::PathIo::at(
        "canonicalize action work directory",
        &work_dir,
    ))?;
    let canonical_tmp = fs::canonicalize(&tmp_dir).map_err(gang_os::PathIo::at(
        "canonicalize action tmp directory",
        &tmp_dir,
    ))?;
    let canonical_cwd = fs::canonicalize(&target_cwd).map_err(gang_os::PathIo::at(
        "canonicalize action working directory",
        &target_cwd,
    ))?;
    if !canonical_cwd.starts_with(&canonical_work_dir) {
        return Err(RunnerError::InvalidWorkingDirectory {
            path: relative_cwd.to_path_buf(),
            reason: "resolved working directory escapes action work directory",
        });
    }
    Ok(AttemptDirs {
        work_root,
        attempt_dir,
        tmp_dir: canonical_tmp,
        working_dir: canonical_cwd,
    })
}

async fn supervise_child<K: Clock>(
    child: &mut Child,
    group: Option<&ProcessGroup>,
    side_channel: &SideChannelDir,
    spec: &RunSpec,
    started: Timestamp,
    clock: &K,
    kill_waiter: &KillWaiter,
) -> Result<(ExitStatus, Option<proto::KillReason>), RunnerError> {
    let deadline_remaining = spec
        .attempt_deadline
        .map(|deadline| deadline.saturating_duration_since(started));
    let mut deadline_sleep = pin!(sleep_optional(clock, deadline_remaining));
    let mut timeout_sleep = pin!(sleep_optional(clock, spec.timeout));

    let kill_reason = tokio::select! {
        biased;
        waited = child.wait() => {
            let status = waited.map_err(|source| RunnerError::Wait {
                entrypoint: spec.entrypoint.clone(),
                source,
            })?;
            if let Some(cgroup) = side_channel.read_cgroup_path() {
                let _ = try_cgroup_kill(&cgroup);
            }
            return Ok((status, kill_waiter.reason()));
        }
        reason = kill_waiter.wait() => reason,
        () = &mut deadline_sleep => proto::KillReason::DeadlineExceeded,
        () = &mut timeout_sleep => proto::KillReason::Timeout,
    };

    let status = escalate_kill(
        child,
        group,
        side_channel,
        spec.kill_grace.get(),
        clock,
        &spec.entrypoint,
    )
    .await?;
    Ok((status, Some(kill_reason)))
}

async fn escalate_kill<K: Clock>(
    child: &mut Child,
    group: Option<&ProcessGroup>,
    side_channel: &SideChannelDir,
    kill_grace: Duration,
    clock: &K,
    entrypoint: &Path,
) -> Result<ExitStatus, RunnerError> {
    // Step 1: graceful SIGTERM to the action's process group, wait up to kill_grace.
    if let Some(group) = group
        && let Err(error) = group.terminate()
    {
        tracing::warn!(%error, "failed to send SIGTERM to action process group");
    }
    if let Ok(waited) = clock.timeout(kill_grace, child.wait()).await {
        // Sweep any SIGTERM-ignoring descendants that outlived the exiting leader.
        if let Some(cgroup) = side_channel.read_cgroup_path() {
            let _ = try_cgroup_kill(&cgroup);
        }
        if let Some(group) = group {
            let _ = group.kill();
        }
        return waited.map_err(|source| RunnerError::Wait {
            entrypoint: entrypoint.to_path_buf(),
            source,
        });
    }

    // Step 2: prefer cgroup.kill when valid report.cgroup metadata exists so `gang exec` can still
    // leave the scope and emit its final side-channel report.
    if let Some(cgroup) = side_channel.read_cgroup_path()
        && try_cgroup_kill(&cgroup)
    {
        let settle = kill_grace.min(CGROUP_KILL_SETTLE_CAP);
        if let Ok(waited) = clock.timeout(settle, child.wait()).await {
            return waited.map_err(|source| RunnerError::Wait {
                entrypoint: entrypoint.to_path_buf(),
                source,
            });
        }
    }

    // Step 3: process-group SIGKILL, plus direct wrapper-PID SIGKILL fallback in case `gang exec`
    // moved into the worker's process group (`join_parent_group`).
    if let Some(group) = group
        && let Err(error) = group.kill()
    {
        tracing::warn!(%error, "failed to send SIGKILL to action process group");
    }
    if let Err(error) = child.start_kill()
        && error.kind() != io::ErrorKind::InvalidInput
    {
        tracing::warn!(%error, "failed to send fallback SIGKILL to action wrapper pid");
    }
    child.wait().await.map_err(|source| RunnerError::Wait {
        entrypoint: entrypoint.to_path_buf(),
        source,
    })
}

/// Writes `"1"` to `<cgroup>/cgroup.kill` when the interface file exists and is writable.
fn try_cgroup_kill(cgroup: &Path) -> bool {
    let kill_file = cgroup.join("cgroup.kill");
    match OpenOptions::new().write(true).open(&kill_file) {
        Ok(mut file) => file.write_all(b"1").is_ok(),
        Err(_) => false,
    }
}

async fn sleep_optional<K: Clock>(clock: &K, duration: Option<Duration>) {
    match duration {
        Some(duration) => clock.sleep(duration).await,
        None => std::future::pending().await,
    }
}

struct PipeTask(JoinHandle<Result<CapturedOutput, RunnerError>>);

impl Drop for PipeTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn drain_bounded<R: AsyncRead + Unpin>(
    mut reader: R,
    limit: usize,
    path: PathBuf,
) -> Result<CapturedOutput, RunnerError> {
    let mut bytes = Vec::new();
    let mut spool: Option<tokio::fs::File> = None;
    let mut chunk = vec![0_u8; 16 * 1024];
    loop {
        let count = reader
            .read(&mut chunk)
            .await
            .map_err(gang_os::PathIo::at("read action output pipe", &path))?;
        if count == 0 {
            break;
        }
        if spool.is_none() && count > limit.saturating_sub(bytes.len()) {
            // Never follow or overwrite an action-created entry at the spool location.
            let fd = rustix::fs::open(
                &path,
                rustix::fs::OFlags::WRONLY
                    | rustix::fs::OFlags::CREATE
                    | rustix::fs::OFlags::EXCL
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::from_raw_mode(0o600),
            )
            .map_err(|source| {
                gang_os::PathIo::new("create action output spool", &path, source.into())
            })?;
            let mut file = tokio::fs::File::from_std(fs::File::from(fd));
            file.write_all(&bytes)
                .await
                .map_err(gang_os::PathIo::at("write action output spool", &path))?;
            bytes = Vec::new();
            spool = Some(file);
        }
        if let Some(file) = &mut spool {
            file.write_all(&chunk[..count])
                .await
                .map_err(gang_os::PathIo::at("write action output spool", &path))?;
        } else {
            bytes.extend_from_slice(&chunk[..count]);
        }
    }
    if let Some(mut file) = spool {
        file.flush()
            .await
            .map_err(gang_os::PathIo::at("flush action output spool", &path))?;
        Ok(CapturedOutput::File(path))
    } else {
        Ok(CapturedOutput::Bytes(bytes))
    }
}

async fn collect_pipe<K: Clock>(
    task: Option<PipeTask>,
    clock: &K,
) -> Result<CapturedOutput, RunnerError> {
    let Some(mut handle) = task else {
        return Ok(CapturedOutput::default());
    };
    let source = match clock.timeout(POST_EXIT_PIPE_DRAIN_CAP, &mut handle.0).await {
        Ok(Ok(output)) => return output,
        Ok(Err(error)) => io::Error::other(error),
        Err(_) => {
            handle.0.abort();
            let _ = (&mut handle.0).await;
            io::Error::new(
                io::ErrorKind::TimedOut,
                "action output pipe did not reach EOF",
            )
        }
    };
    Err(gang_os::PathIo::new("collect action output", Path::new("stdout/stderr"), source).into())
}

fn exit_code_of(status: ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        code
    } else if let Some(signal) = status.signal() {
        128_i32.saturating_add(signal)
    } else {
        1
    }
}

fn timestamp_to_proto(timestamp: Timestamp) -> prost_types::Timestamp {
    let millis = timestamp.as_unix_millis();
    let seconds = i64::try_from(millis / 1_000).unwrap_or(i64::MAX);
    let nanos = i32::try_from((millis % 1_000) * 1_000_000).unwrap_or(0);
    prost_types::Timestamp { seconds, nanos }
}

const fn kill_reason_i32(reason: proto::KillReason) -> i32 {
    reason as i32
}

fn kill_reason_status(reason: proto::KillReason) -> rpc_proto::Status {
    let (code, message) = match reason {
        proto::KillReason::Cancelled => (rpc_proto::Code::Cancelled, "attempt cancelled"),
        proto::KillReason::DeadlineExceeded => (
            rpc_proto::Code::DeadlineExceeded,
            "attempt deadline exceeded",
        ),
        proto::KillReason::Timeout => (
            rpc_proto::Code::DeadlineExceeded,
            "action execution timed out",
        ),
        proto::KillReason::Memory => (
            rpc_proto::Code::ResourceExhausted,
            "action killed due to memory limit or pressure",
        ),
        proto::KillReason::DiskPressure => (
            rpc_proto::Code::ResourceExhausted,
            "action killed due to worker disk pressure",
        ),
        proto::KillReason::DiskLimit => (
            rpc_proto::Code::ResourceExhausted,
            "action exceeded soft disk ceiling",
        ),
        proto::KillReason::Unspecified => (rpc_proto::Code::Aborted, "attempt terminated"),
    };
    rpc_proto::Status {
        code: code as i32,
        message: message.to_owned(),
        details: Vec::new(),
    }
}

/// Marks the worker process as a `Linux` child subreaper so orphaned descendants of actions
/// reparent here instead of PID 1.
///
/// On non-`Linux` platforms this is a no-op returning `Ok(())`.
///
/// # Errors
///
/// Returns an OS I/O error if `prctl(PR_SET_CHILD_SUBREAPER)` fails on `Linux`.
pub fn become_subreaper() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        rustix::process::set_child_subreaper(Some(rustix::process::getpid())).map_err(Into::into)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(())
    }
}

/// Reaps orphaned zombie children whose `PPID` is this process and that were already observed as
/// zombies in `seen_last` (avoiding interference with freshly exited direct `tokio::process::Child`
/// handles).
///
/// Returns `(reaped_count, currently_observed_zombies)`. On non-`Linux` platforms this is a no-op.
#[must_use]
pub fn reap_orphaned_zombies(seen_last: &BTreeSet<u32>) -> (usize, BTreeSet<u32>) {
    #[cfg(target_os = "linux")]
    {
        reap_orphaned_zombies_linux(seen_last)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = seen_last;
        (0, BTreeSet::new())
    }
}

#[cfg(target_os = "linux")]
fn reap_orphaned_zombies_linux(seen_last: &BTreeSet<u32>) -> (usize, BTreeSet<u32>) {
    use rustix::process::{Pid, WaitOptions, waitpid};

    let self_pid = std::process::id();
    let Ok(entries) = fs::read_dir("/proc") else {
        return (0, BTreeSet::new());
    };
    let mut reaped = 0_usize;
    let mut observed = BTreeSet::new();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some((state, parent_pid)) = parse_proc_state_and_ppid(&stat) else {
            continue;
        };
        if state != 'Z' || parent_pid != self_pid {
            continue;
        }
        if seen_last.contains(&pid)
            && let Some(raw_pid) = i32::try_from(pid).ok().and_then(Pid::from_raw)
            && matches!(waitpid(Some(raw_pid), WaitOptions::NOHANG), Ok(Some(_)))
        {
            reaped = reaped.saturating_add(1);
            continue;
        }
        observed.insert(pid);
    }
    (reaped, observed)
}

/// Parses `(state_char, ppid)` from a `Linux` `/proc/<pid>/stat` line.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_proc_state_and_ppid(stat: &str) -> Option<(char, u32)> {
    let (_, after_comm) = stat.rsplit_once(')')?;
    let mut fields = after_comm.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let ppid = fields.next()?.parse::<u32>().ok()?;
    Some((state, ppid))
}

/// Starts the single process-wide child subreaper and background orphan-zombie reaper once.
pub fn start_orphan_reaper() {
    static REAPER: std::sync::Once = std::sync::Once::new();
    REAPER.call_once(|| {
        if let Err(error) = become_subreaper() {
            tracing::warn!(
                %error,
                "cannot become child subreaper; orphaned action descendants will not be reaped here"
            );
        }
        drop(tokio::spawn(async {
            let mut seen_last = BTreeSet::new();
            loop {
                tokio::time::sleep(ORPHAN_REAP_INTERVAL).await;
                let (reaped, seen) =
                    tokio::task::spawn_blocking(move || reap_orphaned_zombies(&seen_last))
                        .await
                        .unwrap_or_default();
                seen_last = seen;
                if reaped > 0 {
                    tracing::info!(reaped, "reaped orphaned zombie descendants");
                }
            }
        }));
    });
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::{OsStr, OsString};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Clock, SystemClock, TestClock};
    use gang_config::DiskEnforcement;
    use gang_config::DiskTuning;
    use gang_config::MemoryEnforcement;
    use gang_config::MemoryTuning;
    use gang_reapi::leadingterm::gang::worker::v1::KillReason;
    use gang_spi::{
        CPU_COUNT_ENV, MAX_RO_BINDS, MEMORY_KB_ENV, NETWORK_ENV, SIDE_CHANNEL_FILE_ENV,
    };
    use gang_testkit::ChildTest;
    use gang_types::{AttemptId, CpuWeight, property};
    use gang_types::{NonZeroBytes, NonZeroDuration, Percent};
    use parking_lot::Mutex;

    use super::*;
    use crate::worker_sampling::{
        AttemptCgroupSample, MemoryPoolSample, SampleProbe, SamplingConfig, WorkerSampler,
        kill_channel,
    };

    const ENV_DUMP_CHILD: ChildTest = ChildTest::new("worker_runner::tests::env_dump_child");
    const GROUP_SLEEPER_CHILD: ChildTest =
        ChildTest::new("worker_runner::tests::group_sleeper_child");
    const GRANDCHILD_SLEEPER: ChildTest =
        ChildTest::new("worker_runner::tests::grandchild_sleeper");
    const REPORT_WRITER_CHILD: ChildTest =
        ChildTest::new("worker_runner::tests::report_writer_child");
    const SCRATCH_READER_CHILD: ChildTest =
        ChildTest::new("worker_runner::tests::scratch_reader_child");

    #[ignore = "child test helper reading staged scratch input"]
    #[test]
    fn scratch_reader_child() -> Result<(), Box<dyn std::error::Error>> {
        let scratch = PathBuf::from(std::env::var_os("TEST_TMPDIR").ok_or("missing TEST_TMPDIR")?);
        assert_eq!(fs::read(scratch.join("seed"))?, b"seeded input");
        let report =
            PathBuf::from(std::env::var_os(SIDE_CHANNEL_FILE_ENV).ok_or("missing report")?);
        assert!(!report.with_file_name("ephemeral_dirs").exists());
        SCRATCH_READER_CHILD.complete();
        Ok(())
    }

    #[test]
    fn staged_scratch_input_survives_action_execution() -> Result<(), Box<dyn std::error::Error>> {
        run_gated(async {
            let work = tempfile::tempdir()?;
            let root = work.path().canonicalize()?;
            fs::create_dir_all(root.join("seeded/work/scratch"))?;
            fs::write(root.join("seeded/work/scratch/seed"), b"seeded input")?;
            let mut spec = child_spec(&root, "seeded", SCRATCH_READER_CHILD)?;
            spec.environment
                .push(("TEST_TMPDIR".into(), "scratch".into()));
            let (trigger, waiter) = kill_channel();
            let outcome = run_attempt(&spec, &SystemClock::new(), None, waiter, trigger).await?;
            assert_eq!(outcome.exit_code, 0);
            let CapturedOutput::Bytes(stdout) = outcome.stdout else {
                return Err("expected captured output".into());
            };
            assert!(SCRATCH_READER_CHILD.completed_in(&String::from_utf8(stdout)?));
            Ok(())
        })
    }

    fn child_spec(
        work_dir: &Path,
        attempt: &str,
        child: ChildTest,
    ) -> Result<RunSpec, Box<dyn std::error::Error>> {
        let exe = std::env::current_exe()?;
        let argv = child.arguments().into_iter().map(OsString::from).collect();
        Ok(RunSpec {
            attempt: AttemptId::try_new(attempt)?,
            entrypoint: exe,
            argv,
            work_directory: work_dir.to_path_buf(),
            working_directory: PathBuf::new(),
            environment: Vec::new(),
            platform: HashMap::new(),
            output_paths: Vec::new(),
            side_channel: SideChannelInputs::default(),
            reserved_memory_bytes: 0,
            reserved_disk_bytes: 0,
            timeout: Some(Duration::from_secs(15)),
            attempt_deadline: None,
            kill_grace: NonZeroDuration::from_millis(150),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        })
    }

    struct BrokenPipeReader;

    impl AsyncRead for BrokenPipeReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected pipe read failure",
            )))
        }
    }

    #[tokio::test]
    async fn capture_propagates_pipe_read_failure() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::TempDir::new()?;
        let result = drain_bounded(BrokenPipeReader, 32, tmp.path().join("stdout")).await;
        assert!(
            matches!(result, Err(RunnerError::Io(ref error)) if error.source.kind() == io::ErrorKind::BrokenPipe)
        );
        let task = PipeTask(tokio::spawn(async move { result }));
        assert!(
            collect_pipe(Some(task), &SystemClock::new()).await.is_err(),
            "pipe collection swallowed a capture error"
        );
        Ok(())
    }

    #[tokio::test]
    async fn capture_spills_complete_stream_and_keeps_small_output_in_memory()
    -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::TempDir::new()?;
        let path = tmp.path().join("stdout");
        let small = drain_bounded(&b"small"[..], 32, path.clone()).await?;
        assert_eq!(small, CapturedOutput::Bytes(b"small".to_vec()));
        assert!(!path.exists());
        let payload = vec![b'x'; 3 * 1024 * 1024 + 7];
        let captured = drain_bounded(payload.as_slice(), 32, path.clone()).await?;
        assert_eq!(captured, CapturedOutput::File(path.clone()));
        assert_eq!(fs::metadata(&path)?.len(), payload.len() as u64);
        assert_eq!(fs::read(&path)?, payload);
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);
        Ok(())
    }

    #[ignore = "child test helper for clean environment verification"]
    #[test]
    fn env_dump_child() -> Result<(), Box<dyn std::error::Error>> {
        let cwd = std::env::current_dir()?;
        println!("CWD={}", cwd.display());
        let mut vars: Vec<(OsString, OsString)> = std::env::vars_os().collect();
        vars.sort();
        for (key, value) in vars {
            println!("ENV:{}={}", key.to_string_lossy(), value.to_string_lossy());
        }
        ENV_DUMP_CHILD.complete();
        Ok(())
    }

    #[ignore = "child test helper spawning a descendant in the same process group"]
    #[test]
    fn group_sleeper_child() -> Result<(), Box<dyn std::error::Error>> {
        let pid_file = std::env::var_os("PID_RECORD_FILE").ok_or("missing PID_RECORD_FILE")?;
        let ignore_term = std::env::var_os("IGNORE_SIGTERM").is_some();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async move {
            let _term = if ignore_term {
                Some(tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?)
            } else {
                None
            };
            let mut grandchild = GRANDCHILD_SLEEPER
                .command()?
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;
            let child_pid = grandchild.id();
            fs::write(&pid_file, format!("{}\n{child_pid}\n", std::process::id()))?;
            if ignore_term {
                tokio::time::sleep(Duration::from_mins(1)).await;
            } else {
                let _ = grandchild.wait();
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })?;
        Ok(())
    }

    #[ignore = "grandchild sleeper helper"]
    #[test]
    fn grandchild_sleeper() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("IGNORE_SIGTERM").is_some() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let _term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                tokio::time::sleep(Duration::from_mins(1)).await;
                Ok::<(), io::Error>(())
            })?;
        } else {
            std::thread::sleep(Duration::from_mins(1));
        }
        Ok(())
    }

    fn run_gated<F, T>(future: F) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Future<Output = Result<T, Box<dyn std::error::Error>>>,
    {
        let _spawn = gang_testkit::fork_gate::spawner();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(future)
    }

    struct SyntheticPressureProbe {
        pool: Mutex<MemoryPoolSample>,
    }

    struct SharedSyntheticProbe(Arc<SyntheticPressureProbe>);

    impl SampleProbe for SharedSyntheticProbe {
        fn read_attempt_cgroup_path(&self, side_channel_dir: &Path) -> Option<PathBuf> {
            Some(side_channel_dir.join("synthetic-cg"))
        }

        fn read_attempt_cgroup(&self, _cgroup: &Path) -> AttemptCgroupSample {
            AttemptCgroupSample {
                current_memory_bytes: Some(256 * 1024 * 1024),
                peak_memory_bytes: Some(300 * 1024 * 1024),
                cpu_time: Some(Duration::from_millis(45)),
                oom_kills: 0,
            }
        }

        fn read_attempt_disk(&self, _attempt_dir: &Path) -> Option<u64> {
            Some(12_345)
        }

        fn read_memory_pool(&self, _pool_cgroup: &Path) -> MemoryPoolSample {
            *self.0.pool.lock()
        }

        fn read_disk_space(&self, _work_dir: &Path) -> Option<gang_scheduler::DiskSample> {
            None
        }
    }

    #[ignore = "child test helper writing side-channel report"]
    #[test]
    fn report_writer_child() -> Result<(), Box<dyn std::error::Error>> {
        let report_path =
            std::env::var_os(SIDE_CHANNEL_FILE_ENV).ok_or("missing side-channel env")?;
        fs::write(
            &report_path,
            br#"{"failure":"memory","peak_memory_kb":4096}"#,
        )?;
        REPORT_WRITER_CHILD.complete();
        Ok(())
    }

    fn process_alive(pid: u32) -> bool {
        let Some(raw) = i32::try_from(pid)
            .ok()
            .and_then(rustix::process::Pid::from_raw)
        else {
            return false;
        };
        if rustix::process::test_kill_process(raw).is_err() {
            return false;
        }
        #[cfg(target_os = "linux")]
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat"))
            && let Some((_, rest)) = stat.rsplit_once(')')
            && matches!(rest.trim_start().as_bytes().first(), Some(b'Z' | b'X'))
        {
            return false;
        }
        true
    }

    async fn wait_for_pids(pid_file: &Path) -> Result<(u32, u32), Box<dyn std::error::Error>> {
        for _ in 0..200 {
            if let Ok(text) = fs::read_to_string(pid_file) {
                let mut lines = text.lines();
                if let (Some(a), Some(b)) = (lines.next(), lines.next())
                    && let (Ok(leader), Ok(grandchild)) = (a.parse::<u32>(), b.parse::<u32>())
                {
                    return Ok((leader, grandchild));
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err("timed out waiting for child pid file".into())
    }

    #[test]
    fn ephemeral_scratch_rejects_overlaps_and_unsafe_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let action = root.path().join("action");
        let channel = root.path().join("channel");
        fs::create_dir_all(action.join("work"))?;
        fs::create_dir(&channel)?;
        fs::create_dir_all(action.join("work/seeded"))?;
        fs::write(action.join("work/seeded/seed"), b"input")?;
        publish_ephemeral_dir(
            &action,
            Path::new(""),
            &[("TEST_TMPDIR".into(), "seeded".into())],
            &[],
            &[],
            &channel,
        );
        assert!(!channel.join("ephemeral_dirs").exists());
        for (cwd, tmp, expected) in [
            ("", "tmp/a", "work/tmp/a\0"),
            ("sub", "tmp/a", "work/sub/tmp/a\0"),
        ] {
            publish_ephemeral_dir(
                &action,
                Path::new(cwd),
                &[("TEST_TMPDIR".into(), tmp.into())],
                &[],
                &[],
                &channel,
            );
            assert_eq!(
                fs::read(channel.join("ephemeral_dirs"))?,
                expected.as_bytes()
            );
            fs::remove_file(channel.join("ephemeral_dirs"))?;
        }
        for value in ["/absolute", "../escape", ".", "link/nested"] {
            if value.starts_with("link") {
                std::os::unix::fs::symlink(root.path(), action.join("work/link"))?;
            }
            publish_ephemeral_dir(
                &action,
                Path::new(""),
                &[("TEST_TMPDIR".into(), value.into())],
                &[],
                &[],
                &channel,
            );
            assert!(!channel.join("ephemeral_dirs").exists(), "accepted {value}");
        }
        for output in ["tmp", "tmp/a", "tmp/a/out"] {
            publish_ephemeral_dir(
                &action,
                Path::new(""),
                &[("TEST_TMPDIR".into(), "tmp/a".into())],
                &[output.into()],
                &[],
                &channel,
            );
            assert!(
                !channel.join("ephemeral_dirs").exists(),
                "accepted output {output}"
            );
        }
        let bind = ReadOnlyBind {
            source: root.path().into(),
            target: "work/tmp".into(),
        };
        publish_ephemeral_dir(
            &action,
            Path::new(""),
            &[("TEST_TMPDIR".into(), "tmp/a".into())],
            &[],
            &[bind],
            &channel,
        );
        assert!(!channel.join("ephemeral_dirs").exists());
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn every_declared_output_field_blocks_ephemeral_scratch()
    -> Result<(), Box<dyn std::error::Error>> {
        use gang_reapi::build::bazel::remote::execution::v2::Command as ReapiCommand;
        let root = tempfile::tempdir()?;
        let action = root.path().join("action");
        let channel = root.path().join("channel");
        fs::create_dir_all(action.join("work"))?;
        fs::create_dir(&channel)?;
        for command in [
            ReapiCommand {
                output_paths: vec!["tmp/out".into()],
                ..Default::default()
            },
            ReapiCommand {
                output_files: vec!["tmp/out".into()],
                ..Default::default()
            },
            ReapiCommand {
                output_directories: vec!["tmp/out".into()],
                ..Default::default()
            },
        ] {
            publish_ephemeral_dir(
                &action,
                Path::new(""),
                &[("TEST_TMPDIR".into(), "tmp".into())],
                &declared_output_paths(&command),
                &[],
                &channel,
            );
            assert!(!channel.join("ephemeral_dirs").exists());
        }
        Ok(())
    }

    #[test]
    fn side_channel_format_matches_gang_exec_byte_for_byte()
    -> Result<(), Box<dyn std::error::Error>> {
        let work = tempfile::tempdir()?;
        let canonical = work.path().canonicalize()?;
        let attempt = AttemptId::try_new("attempt-side-channel")?;
        let inputs = SideChannelInputs {
            cpu_weight: Some(CpuWeight::GATE),
            read_only_trees: vec![
                ReadOnlyBind {
                    source: PathBuf::from("/fast-cas/trees/a"),
                    target: PathBuf::from("work/external/toolchain"),
                },
                ReadOnlyBind {
                    source: PathBuf::from("/fast-cas/trees/b"),
                    target: PathBuf::from("work/external/sdk"),
                },
            ],
        };
        let sc = SideChannelDir::create(&canonical, &attempt, &inputs)?;
        let dir = sc.dir().to_path_buf();
        assert_eq!(dir, canonical.join(".side_channels/attempt-side-channel"));
        assert_eq!(sc.report_path(), dir.join("report"));

        let mode = fs::metadata(&dir)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        assert_eq!(fs::read_to_string(dir.join("cpu_weight"))?, "1000");

        let expected_trees =
            b"/fast-cas/trees/a\0work/external/toolchain\0/fast-cas/trees/b\0work/external/sdk\0";
        assert_eq!(fs::read(dir.join("read_only_trees"))?, expected_trees);

        // Drop cleans up `.side_channels/<attempt_id>/` automatically.
        drop(sc);
        assert!(!dir.exists());

        // Validation matches gang-exec rules: > MAX_RO_BINDS and traversal targets rejected.
        let too_many: Vec<ReadOnlyBind> = (0..=MAX_RO_BINDS)
            .map(|i| ReadOnlyBind {
                source: PathBuf::from(format!("/cas/{i}")),
                target: PathBuf::from(format!("work/t-{i}")),
            })
            .collect();
        assert!(matches!(
            encode_read_only_trees(&too_many),
            Err(RunnerError::TooManyReadOnlyTrees {
                count,
                max: MAX_RO_BINDS
            }) if count == MAX_RO_BINDS + 1
        ));
        for (src, dst) in [
            ("relative/src", "work/ok"),
            ("/cas/../escape", "work/ok"),
            ("/cas/ok", "/abs/target"),
            ("/cas/ok", "../escape"),
            ("/cas/ok", ".side_channels/x"),
            ("/cas/ok", ".graveyard/x"),
        ] {
            assert!(matches!(
                encode_read_only_trees(&[ReadOnlyBind {
                    source: PathBuf::from(src),
                    target: PathBuf::from(dst),
                }]),
                Err(RunnerError::InvalidReadOnlyTree { .. })
            ));
        }
        Ok(())
    }

    fn parse_env_dump(stdout: &str) -> BTreeMap<String, String> {
        let mut env_map = BTreeMap::new();
        for line in stdout.lines() {
            if let Some(pair) = line.strip_prefix("ENV:")
                && let Some((k, v)) = pair.split_once('=')
            {
                env_map.insert(k.to_owned(), v.to_owned());
            }
        }
        env_map
    }

    #[test]
    fn clean_environment_isolation_defaults_overrides_and_reserved_enforcement()
    -> Result<(), Box<dyn std::error::Error>> {
        run_gated(async {
            let work = tempfile::tempdir()?;
            let canonical_work = work.path().canonicalize()?;

            // Case 1: default PWD & TMPDIR, ambient vars dropped, non-reserved GANG_* kept,
            // spoofed reserved GANG_* overwritten or removed when absent from platform.
            let mut spec = child_spec(&canonical_work, "env-default", ENV_DUMP_CHILD)?;
            spec.working_directory = PathBuf::from("sub/dir");
            spec.environment = vec![
                (
                    OsString::from("GANG_E2E_EXACT_ENV"),
                    OsString::from("marker"),
                ),
                (OsString::from("GANG_MEMORY_KB"), OsString::from("spoofed")),
                (OsString::from("GANG_CPU_COUNT"), OsString::from("spoofed")),
                (OsString::from("GANG_NETWORK"), OsString::from("spoofed")),
                (
                    OsString::from(SIDE_CHANNEL_FILE_ENV),
                    OsString::from("spoofed"),
                ),
            ];
            spec.platform
                .insert(property::MEMORY_KB.to_owned(), "2048".to_owned());
            spec.side_channel.cpu_weight = Some(CpuWeight::GATE);

            let (tx, rx) = kill_channel();
            let clock = SystemClock::new();
            let outcome = run_attempt(&spec, &clock, None, rx, tx).await?;
            assert_eq!(outcome.exit_code, 0);
            assert_eq!(outcome.kill, None);

            let CapturedOutput::Bytes(stdout) = outcome.stdout else {
                return Err("expected small stdout in memory".into());
            };
            let stdout = String::from_utf8(stdout)?;
            assert!(ENV_DUMP_CHILD.completed_in(&stdout), "{stdout}");

            let env_map = parse_env_dump(&stdout);
            let expected_cwd = canonical_work.join("env-default/work/sub/dir");
            let expected_tmp = canonical_work.join("env-default/tmp");
            let expected_sc = canonical_work.join(".side_channels/env-default/report");
            assert_eq!(
                env_map.get("PWD").map(String::as_str),
                expected_cwd.to_str()
            );
            assert_eq!(
                env_map.get("TMPDIR").map(String::as_str),
                expected_tmp.to_str()
            );
            assert_eq!(
                env_map.get("GANG_E2E_EXACT_ENV").map(String::as_str),
                Some("marker")
            );
            assert_eq!(env_map.get(MEMORY_KB_ENV).map(String::as_str), Some("2048"));
            assert_eq!(
                env_map.get(SIDE_CHANNEL_FILE_ENV).map(String::as_str),
                expected_sc.to_str()
            );
            // Absent reserved platform keys and ambient host keys MUST NOT exist.
            for forbidden in [
                CPU_COUNT_ENV,
                NETWORK_ENV,
                "HOME",
                "PATH",
                "LD_LIBRARY_PATH",
                "USER",
            ] {
                assert!(
                    !env_map.contains_key(forbidden),
                    "unexpected env key {forbidden}: {env_map:?}"
                );
            }
            // `<attempt>` must contain ONLY `["tmp", "work"]` (verdict.rs contract).
            let mut attempt_entries: Vec<String> =
                fs::read_dir(canonical_work.join("env-default"))?
                    .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
                    .collect::<Result<_, _>>()?;
            attempt_entries.sort();
            assert_eq!(attempt_entries, vec!["tmp", "work"]);
            // Side-channel directory must be cleaned up on completion.
            assert!(!canonical_work.join(".side_channels/env-default").exists());

            // Case 2: client may override PWD and TMPDIR; cpu_count & network set when admitted.
            let mut override_spec = child_spec(&canonical_work, "env-override", ENV_DUMP_CHILD)?;
            override_spec.environment = vec![
                (OsString::from("PWD"), OsString::from("/custom/pwd")),
                (OsString::from("TMPDIR"), OsString::from("/custom/tmp")),
            ];
            override_spec
                .platform
                .insert(property::CPU_COUNT.to_owned(), "4".to_owned());
            override_spec
                .platform
                .insert(property::NETWORK.to_owned(), "requested".to_owned());
            let (tx, rx) = kill_channel();
            let outcome = run_attempt(&override_spec, &clock, None, rx, tx).await?;
            assert_eq!(outcome.exit_code, 0);
            let CapturedOutput::Bytes(stdout) = outcome.stdout else {
                return Err("expected small stdout in memory".into());
            };
            let stdout = String::from_utf8(stdout)?;
            assert!(stdout.contains("ENV:PWD=/custom/pwd\n"), "{stdout}");
            assert!(stdout.contains("ENV:TMPDIR=/custom/tmp\n"), "{stdout}");
            assert!(stdout.contains("ENV:GANG_CPU_COUNT=4\n"), "{stdout}");
            assert!(stdout.contains("ENV:GANG_NETWORK=requested\n"), "{stdout}");
            Ok(())
        })
    }

    #[test]
    fn timeout_kills_entire_process_group_including_sigterm_ignoring_descendants()
    -> Result<(), Box<dyn std::error::Error>> {
        run_gated(async {
            let work = tempfile::tempdir()?;
            let canonical_work = work.path().canonicalize()?;
            let pid_file = canonical_work.join("pids.txt");

            let mut spec = child_spec(&canonical_work, "timeout-group", GROUP_SLEEPER_CHILD)?;
            spec.environment = vec![
                (
                    OsString::from("PID_RECORD_FILE"),
                    pid_file.clone().into_os_string(),
                ),
                (OsString::from("IGNORE_SIGTERM"), OsString::from("1")),
            ];
            spec.timeout = Some(Duration::from_millis(350));
            spec.kill_grace = NonZeroDuration::from_millis(120);

            let (tx, rx) = kill_channel();
            let clock = SystemClock::new();
            let handle =
                tokio::spawn(async move { run_attempt(&spec, &clock, None, rx, tx).await });

            let (leader_pid, grandchild_pid) = wait_for_pids(&pid_file).await?;
            let outcome = handle.await??;
            assert_eq!(outcome.kill, Some(KillReason::Timeout));
            assert_ne!(outcome.exit_code, 0);

            // Give the kernel a brief moment to finish tearing down the killed grandchild.
            for _ in 0..40 {
                if !process_alive(leader_pid) && !process_alive(grandchild_pid) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(!process_alive(leader_pid), "leader {leader_pid} survived");
            assert!(
                !process_alive(grandchild_pid),
                "grandchild {grandchild_pid} survived timeout kill"
            );
            assert!(!canonical_work.join(".side_channels/timeout-group").exists());
            Ok(())
        })
    }

    #[test]
    fn external_cancel_kills_process_group_and_classifies_cancelled()
    -> Result<(), Box<dyn std::error::Error>> {
        run_gated(async {
            let work = tempfile::tempdir()?;
            let canonical_work = work.path().canonicalize()?;
            let pid_file = canonical_work.join("cancel-pids.txt");

            let mut spec = child_spec(&canonical_work, "cancel-group", GROUP_SLEEPER_CHILD)?;
            spec.environment = vec![(
                OsString::from("PID_RECORD_FILE"),
                pid_file.clone().into_os_string(),
            )];
            spec.timeout = Some(Duration::from_secs(30));
            spec.kill_grace = NonZeroDuration::from_millis(100);

            let (tx, rx) = kill_channel();
            let external_tx = tx.clone();
            let clock = SystemClock::new();
            let handle =
                tokio::spawn(async move { run_attempt(&spec, &clock, None, rx, tx).await });

            let (leader_pid, grandchild_pid) = wait_for_pids(&pid_file).await?;
            assert!(external_tx.kill(KillReason::Cancelled));

            let outcome = handle.await??;
            assert_eq!(outcome.kill, Some(KillReason::Cancelled));
            let executed = outcome.to_executed_proto(&AttemptId::try_new("cancel-group")?);
            assert_eq!(
                executed.kill.as_ref().map(|k| k.reason),
                Some(KillReason::Cancelled as i32)
            );
            assert!(executed.started.is_some());
            assert!(executed.finished.is_some());

            for _ in 0..40 {
                if !process_alive(leader_pid) && !process_alive(grandchild_pid) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(!process_alive(leader_pid));
            assert!(!process_alive(grandchild_pid));
            Ok(())
        })
    }

    #[test]
    fn side_channel_memory_report_classifies_oom_kill_and_peak_memory()
    -> Result<(), Box<dyn std::error::Error>> {
        run_gated(async {
            let work = tempfile::tempdir()?;
            let canonical_work = work.path().canonicalize()?;
            let spec = child_spec(&canonical_work, "oom-report", REPORT_WRITER_CHILD)?;
            let (tx, rx) = kill_channel();
            let outcome = run_attempt(&spec, &SystemClock::new(), None, rx, tx).await?;
            assert_eq!(outcome.kill, Some(KillReason::Memory));
            assert_eq!(outcome.usage.oom_kills, 1);
            assert_eq!(outcome.usage.peak_memory_bytes, Some(4096 * 1024));
            Ok(())
        })
    }

    #[test]
    fn pressure_sampler_kills_running_process_and_reports_memory_kill_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        run_gated(async {
            let work = tempfile::tempdir()?;
            let canonical_work = work.path().canonicalize()?;
            let pid_file = canonical_work.join("pressure-pids.txt");

            let probe = Arc::new(SyntheticPressureProbe {
                pool: Mutex::new(MemoryPoolSample {
                    psi_total_us: Some(0),
                    available_bytes: Some(1024),
                }),
            });
            let config = SamplingConfig {
                memory: MemoryTuning {
                    enforcement: MemoryEnforcement::Pressure,
                    reserve: NonZeroBytes::mib(100),
                    pressure_stall_percent: Percent::from_percent(10),
                    pressure_poll_interval: NonZeroDuration::from_millis(50),
                    pressure_kill_after: NonZeroDuration::from_millis(100),
                    sample_interval: NonZeroDuration::from_millis(50),
                    ..MemoryTuning::default()
                },
                disk: DiskTuning {
                    enforcement: DiskEnforcement::None,
                    ..DiskTuning::default()
                },
                memory_pool: Some(PathBuf::from("/sys/fs/cgroup/actions")),
                work_directory: canonical_work.clone(),
                disk_per_instance_bytes: 1_000_000,
            };
            let mut sampler = WorkerSampler::new(config, SharedSyntheticProbe(Arc::clone(&probe)));
            let registry = sampler.registry();

            let mut spec = child_spec(&canonical_work, "psi-victim", GROUP_SLEEPER_CHILD)?;
            spec.environment = vec![(
                OsString::from("PID_RECORD_FILE"),
                pid_file.clone().into_os_string(),
            )];
            spec.reserved_memory_bytes = 64 * 1024 * 1024;
            spec.timeout = Some(Duration::from_secs(30));
            spec.kill_grace = NonZeroDuration::from_millis(100);

            let (tx, rx) = kill_channel();
            let runner_registry = registry.clone();
            let handle = tokio::spawn(async move {
                run_attempt(&spec, &SystemClock::new(), Some(&runner_registry), rx, tx).await
            });

            let (leader_pid, grandchild_pid) = wait_for_pids(&pid_file).await?;
            let test_clock = TestClock::default();
            // Drive 3 PSI polls (baseline + 2 sustained intervals = 100ms >= 100ms).
            *probe.pool.lock() = MemoryPoolSample {
                psi_total_us: Some(10_000),
                available_bytes: Some(1_024),
            };
            assert_eq!(sampler.poll_memory_pressure(test_clock.monotonic()), None);
            test_clock.advance(Duration::from_millis(50));
            *probe.pool.lock() = MemoryPoolSample {
                psi_total_us: Some(25_000),
                available_bytes: Some(1_024),
            };
            assert_eq!(sampler.poll_memory_pressure(test_clock.monotonic()), None);
            test_clock.advance(Duration::from_millis(50));
            *probe.pool.lock() = MemoryPoolSample {
                psi_total_us: Some(40_000),
                available_bytes: Some(1_024),
            };
            let killed = sampler.poll_memory_pressure(test_clock.monotonic());
            assert_eq!(killed, Some(AttemptId::try_new("psi-victim")?));

            let outcome = handle.await??;
            assert_eq!(outcome.kill, Some(KillReason::Memory));
            assert!(outcome.usage.sampled);
            assert_eq!(outcome.usage.peak_memory_bytes, Some(300 * 1024 * 1024));
            assert_eq!(outcome.usage.cpu_time, Some(Duration::from_millis(45)));
            assert!(registry.is_quiescence_settled());

            for _ in 0..40 {
                if !process_alive(leader_pid) && !process_alive(grandchild_pid) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(!process_alive(leader_pid));
            assert!(!process_alive(grandchild_pid));
            Ok(())
        })
    }

    #[test]
    fn parses_side_channel_report_variants_and_proc_stat() {
        assert_eq!(
            parse_side_channel_report(r#"{"failure":"memory","peak_memory_kb":3}"#),
            Some(SideChannelReport {
                memory_failure: true,
                peak_memory_kb: Some(3),
            })
        );
        assert_eq!(
            parse_side_channel_report(r#"{"failure":"memory"}"#),
            Some(SideChannelReport {
                memory_failure: true,
                peak_memory_kb: None,
            })
        );
        assert_eq!(
            parse_side_channel_report(r#"{"peak_memory_kb":2}"#),
            Some(SideChannelReport {
                memory_failure: false,
                peak_memory_kb: Some(2),
            })
        );
        assert_eq!(parse_side_channel_report("not json"), None);
        assert_eq!(
            parse_proc_state_and_ppid("123 (foo (bar)) Z 42 123 123 0"),
            Some(('Z', 42))
        );
        // Validate non-UTF-8 raw OsStr bytes work in read-only tree encoding.
        let weird_source = PathBuf::from(OsStr::from_bytes(b"/cas/\xff/tree"));
        let weird_target = PathBuf::from(OsStr::from_bytes(b"work/sub\xfe"));
        let bytes = encode_read_only_trees(&[ReadOnlyBind {
            source: weird_source,
            target: weird_target,
        }])
        .expect("valid unix bytes");
        assert_eq!(bytes, b"/cas/\xff/tree\0work/sub\xfe\0");
    }
}
