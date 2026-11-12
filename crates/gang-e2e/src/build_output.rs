//! What a Bazel build reported: its exit code, its diagnostics, and its process summary.

use std::collections::BTreeMap;

/// Lower-case diagnostics that mark a failed TLS handshake with the remote executor: alert
/// descriptions (`bad certificate`, `certificate required`, ...), handshake failures, the
/// `SSLHandshakeException` they surface as, and `ssl exception`, grpc-java's rendering of a TLS
/// failure (the server's rejection of a client certificate included). Matched after `_` is
/// replaced with a space.
const TLS_FAILURE_MARKERS: [&str; 7] = [
    "bad certificate",
    "certificate revoked",
    "certificate required",
    "unknown ca",
    "handshake failure",
    "sslhandshakeexception",
    "ssl exception",
];

/// The exit status of an action killed by SIGKILL: 128 + 9, as `NativeLink` reports a signal.
const SIGKILL_EXIT_STATUS: i32 = 137;

/// The processes a build ran, by the runner that ran them, as Bazel's summary line counts them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProcessSummary {
    runners: BTreeMap<String, u32>,
}

impl ProcessSummary {
    /// Reads the process summary Bazel printed in `stderr`.
    ///
    /// Bazel prints `INFO: 5 processes: 2 internal, 1 remote cache hit, 2 remote.` (or
    /// `INFO: 1 process: 1 remote.`, or `INFO: 0 processes.`). The last such line wins; a
    /// text with no such line has an empty summary.
    #[must_use]
    pub fn parse(stderr: &str) -> Self {
        stderr
            .lines()
            .filter_map(|line| line.strip_prefix("INFO: "))
            .filter_map(parse_summary_line)
            .next_back()
            .map_or_else(Self::default, |runners| Self { runners })
    }

    /// How many processes `runner` ran; zero when it ran none.
    #[must_use]
    pub fn count(&self, runner: &str) -> u32 {
        self.runners.get(runner).copied().unwrap_or_default()
    }

    /// How many processes ran remotely.
    #[must_use]
    pub fn remote(&self) -> u32 {
        self.count("remote")
    }

    /// How many processes were served from the remote cache.
    #[must_use]
    pub fn remote_cache_hits(&self) -> u32 {
        self.count("remote cache hit")
    }
}

/// The runner counts of one summary line, without its `INFO: ` prefix, or `None` when the line
/// is not a complete summary.
fn parse_summary_line(rest: &str) -> Option<BTreeMap<String, u32>> {
    let (count, tail) = rest.split_once(' ')?;
    count.parse::<u32>().ok()?;
    if tail == "processes." || tail == "process." {
        return Some(BTreeMap::new());
    }
    let list = tail
        .strip_prefix("processes:")
        .or_else(|| tail.strip_prefix("process:"))?;
    let mut runners = BTreeMap::new();
    for item in list.trim().trim_end_matches('.').split(',') {
        let (count, runner) = item.trim().split_once(' ')?;
        runners.insert(runner.to_owned(), count.parse().ok()?);
    }
    Some(runners)
}

/// The result of one Bazel build: how it ended and what it said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOutcome {
    /// The exit code of the build; `None` when it was terminated by a signal.
    pub exit_code: Option<i32>,
    /// Everything Bazel wrote to standard error (and then standard output).
    pub stderr: String,
    /// The process summary read from `stderr`.
    pub processes: ProcessSummary,
}

impl BuildOutcome {
    /// Records a finished build and reads its process summary from `stderr`.
    #[must_use]
    pub fn new(exit_code: Option<i32>, stderr: String) -> Self {
        let processes = ProcessSummary::parse(&stderr);
        Self {
            exit_code,
            stderr,
            processes,
        }
    }

    /// Whether the build exited with status zero.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.exit_code == Some(0)
    }

    /// Describes the build's exit code and captured output.
    #[must_use]
    pub fn exit_summary(&self) -> String {
        format!(
            "the build exited with {:?}: {}",
            self.exit_code, self.stderr
        )
    }

    /// Whether an action failed with exit status `code`, as Bazel reports it: `(Exit <code>)`.
    #[must_use]
    pub fn action_exit(&self, code: i32) -> bool {
        self.stderr.contains(&format!("(Exit {code})"))
    }

    /// Whether an action was killed by SIGKILL, which Bazel reports as `(Killed)` or, for the
    /// same exit status, as `(Exit 137)`.
    #[must_use]
    pub fn action_killed(&self) -> bool {
        self.action_exit(SIGKILL_EXIT_STATUS) || self.stderr.contains("(Killed)")
    }

    /// The last `lines` lines of `stderr`, in their original order, joined with `\n`.
    #[must_use]
    pub fn stderr_tail(&self, lines: usize) -> String {
        let mut tail: Vec<&str> = self.stderr.lines().rev().take(lines).collect();
        tail.reverse();
        tail.join("\n")
    }

    /// Whether the build's diagnostics name a failed TLS handshake with the remote executor
    /// (case-insensitively, `_` read as a space), not a failure of the build itself.
    #[must_use]
    pub fn tls_failure(&self) -> bool {
        let stderr = self.stderr.to_lowercase().replace('_', " ");
        TLS_FAILURE_MARKERS
            .iter()
            .any(|marker| stderr.contains(marker))
    }
}
