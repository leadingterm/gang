//! A recording action-limits double: it logs every call, fails one chosen step, and reports what it was asked.

use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;

use gang_spi::{ActionLimits, ActionLimitsPort, ActionScope, ActionUsage, PortError};
use gang_types::Bytes;
use parking_lot::Mutex;

use crate::CallLog;

/// The join link a [`RecordingLimits`] launches actions through by default.
const DEFAULT_JOIN_LINK: &str = "/test/bin/gang-join";

/// The scope directory a [`RecordingLimits`] names by default, passed to the join link.
const DEFAULT_SCOPE_DIR: &str = "/test/actions/scope";

/// One step of the action-limits lifecycle, which a [`RecordingLimits`] can be told to fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitsStep {
    /// [`ActionLimitsPort::prepare`].
    Prepare,
    /// [`ActionLimitsPort::enter`].
    Enter,
    /// [`ActionLimitsPort::leave`].
    Leave,
    /// [`ActionLimitsPort::reap_abandoned`].
    Reap,
}

/// What a [`RecordingLimits`] has been asked, shared by its clones.
#[derive(Debug, Default)]
struct Record {
    entered: Vec<ActionLimits>,
    observed: Vec<usize>,
    reaped: Vec<u32>,
}

/// The scope a [`RecordingLimits`] creates: it holds the launcher the double hands out.
#[derive(Debug, Default)]
pub struct RecordedScope {
    /// The command prefix that starts a process inside the scope.
    launcher: Box<[OsString]>,
}

impl ActionScope for RecordedScope {
    fn launcher(&self) -> &[OsString] {
        &self.launcher
    }
}

/// An action-limits double that logs each call to a [`CallLog`], reports a fixed usage, and can
/// fail one step. Clones share what they were asked. Each scope it creates launches through the
/// join link and the scope directory.
#[derive(Clone)]
pub struct RecordingLimits {
    log: CallLog,
    failing: Option<LimitsStep>,
    observe: Option<Arc<dyn Fn() -> usize + Send + Sync>>,
    record: Arc<Mutex<Record>>,
    launcher: Box<[OsString]>,
}

impl RecordingLimits {
    /// Creates a double that logs into `log` and succeeds at every step. Its `leave` reports a peak
    /// memory of 4096 bytes and a CPU time of 7 milliseconds.
    #[must_use]
    pub fn new(log: &CallLog) -> Self {
        Self {
            log: log.clone(),
            failing: None,
            observe: None,
            record: Arc::default(),
            launcher: [
                OsString::from(DEFAULT_JOIN_LINK),
                OsString::from(DEFAULT_SCOPE_DIR),
            ]
            .into(),
        }
    }

    /// Makes `step` return an error after it is logged. Every other step still succeeds.
    #[must_use]
    pub fn failing(mut self, step: LimitsStep) -> Self {
        self.failing = Some(step);
        self
    }

    /// Makes each `prepare` also record `observe()`, the value the test wants to see at that point.
    #[must_use]
    pub fn observing_prepare(
        mut self,
        observe: impl Fn() -> usize + Send + Sync + 'static,
    ) -> Self {
        self.observe = Some(Arc::new(observe));
        self
    }

    /// The limits of every `enter` call, in the order they were made.
    #[must_use]
    pub fn entered(&self) -> Vec<ActionLimits> {
        self.record.lock().entered.clone()
    }

    /// The value `observe` returned at each `prepare`, in the order the calls were made.
    #[must_use]
    pub fn observed_at_prepare(&self) -> Vec<usize> {
        self.record.lock().observed.clone()
    }

    /// The creator pid of each `reap_abandoned` call, including failed calls.
    #[must_use]
    pub fn reaped(&self) -> Vec<u32> {
        self.record.lock().reaped.clone()
    }

    /// The error `step` returns: an error when it is the failing step, success otherwise.
    fn outcome(&self, step: LimitsStep) -> Result<(), PortError> {
        if self.failing == Some(step) {
            return Err(PortError::backend("limits step failed"));
        }
        Ok(())
    }
}

impl ActionLimitsPort for RecordingLimits {
    type Scope = RecordedScope;

    async fn prepare(&self) -> Result<(), PortError> {
        self.log.record("limits.prepare");
        if let Some(observe) = &self.observe {
            let value = observe();
            self.record.lock().observed.push(value);
        }
        self.outcome(LimitsStep::Prepare)
    }

    async fn enter(&self, limits: &ActionLimits) -> Result<RecordedScope, PortError> {
        self.log.record("limits.enter");
        self.record.lock().entered.push(*limits);
        self.outcome(LimitsStep::Enter)?;
        Ok(RecordedScope {
            launcher: self.launcher.clone(),
        })
    }

    async fn leave(&self, _scope: RecordedScope) -> Result<ActionUsage, PortError> {
        self.log.record("limits.leave");
        self.outcome(LimitsStep::Leave)?;
        Ok(ActionUsage {
            peak_memory: Bytes::new(4096),
            cpu_time: Duration::from_millis(7),
        })
    }

    async fn reap_abandoned(&self, creator: u32) -> Result<(), PortError> {
        self.log.record("limits.reap_abandoned");
        self.record.lock().reaped.push(creator);
        self.outcome(LimitsStep::Reap)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use gang_spi::{ActionLimits, ActionLimitsPort, ActionScope, ActionUsage};
    use gang_types::{Bytes, NonZeroBytes};

    use super::{DEFAULT_JOIN_LINK, DEFAULT_SCOPE_DIR, LimitsStep, RecordedScope, RecordingLimits};
    use crate::{CallLog, TestResult, must_ready as ready};

    const CALLS: [&str; 3] = ["limits.prepare", "limits.enter", "limits.leave"];

    fn sample_limits(cpus: u32, memory: u64) -> ActionLimits {
        ActionLimits {
            cpu_count: NonZeroU32::new(cpus),
            memory: NonZeroBytes::new(Bytes::new(memory)),
        }
    }

    /// Runs the three steps in lifecycle order and reports which of them succeeded.
    async fn run_lifecycle(limits: &RecordingLimits) -> Vec<bool> {
        let mut outcomes = vec![limits.prepare().await.is_ok()];
        outcomes.push(limits.enter(&sample_limits(2, 1024)).await.is_ok());
        outcomes.push(limits.leave(RecordedScope::default()).await.is_ok());
        outcomes
    }

    #[test]
    fn lifecycle_calls_are_logged_in_order() {
        let log = CallLog::default();
        let limits = RecordingLimits::new(&log);

        let outcomes = ready(run_lifecycle(&limits));

        assert_eq!((outcomes, log.calls()), (vec![true; 3], CALLS.to_vec()));
    }

    #[test]
    fn failing_step_fails_only_that_call_after_logging_it() {
        let cases = [
            (LimitsStep::Prepare, [false, true, true]),
            (LimitsStep::Enter, [true, false, true]),
            (LimitsStep::Leave, [true, true, false]),
        ];
        for (step, expected) in cases {
            let log = CallLog::default();
            let limits = RecordingLimits::new(&log).failing(step);

            let outcomes = ready(run_lifecycle(&limits));

            assert_eq!(
                (outcomes, log.calls()),
                (expected.to_vec(), CALLS.to_vec()),
                "failing {step:?}"
            );
        }
    }

    #[test]
    fn entered_reports_the_limits_of_every_enter_in_order() -> TestResult {
        let log = CallLog::default();
        let limits = RecordingLimits::new(&log);
        let first = sample_limits(1, 1024);
        let second = sample_limits(4, 4096);

        ready(limits.enter(&first))?;
        ready(limits.enter(&second))?;

        assert_eq!(limits.entered(), [first, second]);
        Ok(())
    }

    #[test]
    fn reap_calls_are_logged_and_counted_across_clones_even_on_failure() {
        let log = CallLog::default();
        let limits = RecordingLimits::new(&log).failing(LimitsStep::Reap);
        assert!(ready(limits.clone().reap_abandoned(10)).is_err());
        assert!(ready(limits.reap_abandoned(20)).is_err());
        assert_eq!(limits.reaped(), [10, 20]);
        assert_eq!(
            log.calls(),
            ["limits.reap_abandoned", "limits.reap_abandoned"]
        );
    }

    #[test]
    fn enter_returns_a_scope_launching_through_the_default_join_command() -> TestResult {
        let log = CallLog::default();
        let limits = RecordingLimits::new(&log);

        let scope = ready(limits.enter(&sample_limits(1, 1024)))?;

        assert_eq!(
            scope.launcher(),
            [
                OsString::from(DEFAULT_JOIN_LINK),
                OsString::from(DEFAULT_SCOPE_DIR)
            ]
        );
        Ok(())
    }

    #[test]
    fn observed_at_prepare_records_the_probe_value_of_each_prepare() -> TestResult {
        let probe = Arc::new(AtomicUsize::new(1));
        let reading = Arc::clone(&probe);
        let log = CallLog::default();
        let limits =
            RecordingLimits::new(&log).observing_prepare(move || reading.load(Ordering::Relaxed));

        ready(limits.prepare())?;
        probe.store(2, Ordering::Relaxed);
        ready(limits.prepare())?;

        assert_eq!(limits.observed_at_prepare(), [1, 2]);
        Ok(())
    }

    #[test]
    fn leave_reports_the_fixed_usage() -> TestResult {
        let log = CallLog::default();
        let usage = ready(RecordingLimits::new(&log).leave(RecordedScope::default()))?;

        assert_eq!(
            usage,
            ActionUsage {
                peak_memory: Bytes::new(4096),
                cpu_time: Duration::from_millis(7),
            }
        );
        Ok(())
    }
}
