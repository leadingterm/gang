//! What a worker is doing, read the same way by the autoscaler and the dashboard.

use gang_spi::WorkerState;

/// What a connected worker is doing right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    /// Not draining, and running actions.
    Busy,
    /// Not draining, and running no action.
    Idle,
    /// Told to drain: the scheduler assigns it no new actions.
    Draining,
}

impl From<&WorkerState> for Activity {
    fn from(worker: &WorkerState) -> Self {
        if worker.draining {
            Self::Draining
        } else if worker.running_actions == 0 {
            Self::Idle
        } else {
            Self::Busy
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use gang_clock::Timestamp;
    use gang_spi::WorkerState;
    use gang_testkit::{TestResult, worker_id};
    use gang_types::FleetName;

    use super::Activity;

    fn worker(running_actions: u32, draining: bool) -> TestResult<WorkerState> {
        Ok(WorkerState {
            id: worker_id("i-0abc123")?,
            fleet: FleetName::try_new("aarch64-fleet")?,
            running_actions,
            max_inflight_actions: NonZeroU32::new(4),
            paused: false,
            draining,
            last_seen: Timestamp::default(),
        })
    }

    #[test]
    fn activity_follows_drain_then_running_actions() -> TestResult {
        assert_eq!(Activity::from(&worker(0, false)?), Activity::Idle);
        assert_eq!(Activity::from(&worker(2, false)?), Activity::Busy);
        assert_eq!(Activity::from(&worker(2, true)?), Activity::Draining);
        Ok(())
    }
}
