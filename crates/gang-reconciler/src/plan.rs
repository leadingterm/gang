//! The steps one reconcile pass decided on, in the order the caller applies them.

use gang_clock::Timestamp;
use gang_spi::{ScalingAuditEvent, ScalingEventKind};
use gang_types::{FleetName, InstanceId, ScalingCause, WorkerId};

/// What one reconcile pass decided. The caller applies the steps in order.
///
/// Steps are idempotent for the caller: a [`Action::StartDrain`] or [`Action::CancelDrain`] may
/// be applied again until the scheduler reflects it. The caller records an audit event only
/// after its step was applied successfully.
///
/// Each step names its fleet by its index in the [`FleetPass`](crate::FleetPass) slice of the pass,
/// which is the order of the configured fleets. The caller resolves the index to the fleet's
/// configuration with the same index.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Plan {
    /// The wall time of the pass.
    pub at: Timestamp,
    /// The steps, in the order the caller applies them.
    pub steps: Vec<PlanStep>,
}

impl Plan {
    /// Creates an empty plan.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// One step of a plan: an [`Action`] on the fleet at `fleet` in the pass's
/// [`FleetPass`](crate::FleetPass) slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanStep {
    /// The index of the fleet in the [`FleetPass`](crate::FleetPass) slice of the pass.
    pub fleet: usize,
    /// The change the step makes.
    pub action: Action,
}

impl PlanStep {
    /// The audit event that records this step at `at` for `fleet`, the name of the step's fleet.
    /// Only the caller records it, and only after the step was applied successfully.
    #[must_use]
    pub fn audit_event(&self, at: Timestamp, fleet: &FleetName) -> ScalingAuditEvent {
        ScalingAuditEvent {
            at,
            fleet: fleet.clone(),
            kind: self.action.kind(),
            cause: Some(self.action.cause()),
        }
    }
}

/// Why an instance is terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationCause {
    /// An in-service instance had no connected worker for its fleet's boot grace.
    Orphan,
    /// A draining worker finished its work and was retired.
    DrainFinished,
    /// A draining worker reached the longest allowed wait and was retired anyway.
    MaxDrainWait,
}

impl From<TerminationCause> for ScalingCause {
    fn from(cause: TerminationCause) -> Self {
        match cause {
            TerminationCause::Orphan => Self::Orphan,
            TerminationCause::DrainFinished => Self::DrainFinished,
            TerminationCause::MaxDrainWait => Self::MaxDrainWait,
        }
    }
}

/// One change to a fleet or to a worker. Repeating a step before the provisioner or the
/// scheduler reflects it must be harmless; see [`Plan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Raise a fleet's desired capacity. The caller asks the provisioner through
    /// `ProvisionerPort::set_desired_capacity`.
    ScaleOut {
        /// The capacity the pass counted before scaling out.
        from: u32,
        /// The capacity requested.
        to: u32,
    },
    /// Stop a worker taking new actions. The caller calls
    /// `SchedulerPort::set_draining(worker, true)`.
    StartDrain(WorkerId),
    /// Resume a draining worker so it takes new actions again. The caller calls
    /// `SchedulerPort::set_draining(worker, false)`. Used instead of scaling out.
    CancelDrain(WorkerId),
    /// Terminate an instance. The caller calls `ProvisionerPort::terminate`; the provisioner
    /// lowers the desired capacity itself.
    Terminate {
        /// The instance to terminate.
        instance: InstanceId,
        /// Why the instance is terminated.
        cause: TerminationCause,
    },
}

impl Action {
    /// The audit event kind this action records.
    #[must_use]
    pub fn kind(&self) -> ScalingEventKind {
        match self {
            Self::ScaleOut { from, to } => ScalingEventKind::ScaledOut {
                from: *from,
                to: *to,
            },
            Self::StartDrain(worker) => ScalingEventKind::DrainStarted {
                worker: worker.clone(),
            },
            Self::CancelDrain(worker) => ScalingEventKind::DrainCancelled {
                worker: worker.clone(),
            },
            Self::Terminate { instance, .. } => ScalingEventKind::Terminated {
                instance: instance.clone(),
            },
        }
    }

    /// The autoscaler decision this action carries out. Fixed per variant, except for
    /// [`Action::Terminate`], which carries its own cause.
    #[must_use]
    pub fn cause(&self) -> ScalingCause {
        match self {
            Self::ScaleOut { .. } | Self::CancelDrain(_) => ScalingCause::Demand,
            Self::StartDrain(_) => ScalingCause::IdleTimeout,
            Self::Terminate { cause, .. } => (*cause).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use gang_testkit::{TestResult, worker_id};

    use super::*;

    fn instance() -> TestResult<InstanceId> {
        Ok(InstanceId::try_new("i-0abc123")?)
    }

    fn fleet(name: &str) -> TestResult<FleetName> {
        Ok(FleetName::try_new(name)?)
    }

    #[test]
    fn scale_out_audits_as_scaled_out() -> TestResult {
        let step = PlanStep {
            fleet: 0,
            action: Action::ScaleOut { from: 2, to: 5 },
        };
        let at = Timestamp::from_unix_millis(42);

        assert_eq!(
            step.audit_event(at, &fleet("x86-64-fleet")?),
            ScalingAuditEvent {
                at,
                fleet: fleet("x86-64-fleet")?,
                kind: ScalingEventKind::ScaledOut { from: 2, to: 5 },
                cause: Some(ScalingCause::Demand),
            }
        );
        Ok(())
    }

    #[test]
    fn start_drain_audits_as_drain_started() -> TestResult {
        let step = PlanStep {
            fleet: 1,
            action: Action::StartDrain(worker_id("i-0abc123")?),
        };
        let at = Timestamp::from_unix_millis(7);

        assert_eq!(
            step.audit_event(at, &fleet("aarch64-fleet")?),
            ScalingAuditEvent {
                at,
                fleet: fleet("aarch64-fleet")?,
                kind: ScalingEventKind::DrainStarted {
                    worker: worker_id("i-0abc123")?,
                },
                cause: Some(ScalingCause::IdleTimeout),
            }
        );
        Ok(())
    }

    #[test]
    fn cancel_drain_audits_as_drain_cancelled() -> TestResult {
        let step = PlanStep {
            fleet: 1,
            action: Action::CancelDrain(worker_id("i-0abc123")?),
        };
        let at = Timestamp::from_unix_millis(3);

        assert_eq!(
            step.audit_event(at, &fleet("aarch64-fleet")?),
            ScalingAuditEvent {
                at,
                fleet: fleet("aarch64-fleet")?,
                kind: ScalingEventKind::DrainCancelled {
                    worker: worker_id("i-0abc123")?,
                },
                cause: Some(ScalingCause::Demand),
            }
        );
        Ok(())
    }

    #[test]
    fn terminate_audits_as_terminated() -> TestResult {
        let step = PlanStep {
            fleet: 0,
            action: Action::Terminate {
                instance: instance()?,
                cause: TerminationCause::DrainFinished,
            },
        };
        let at = Timestamp::from_unix_millis(9);

        assert_eq!(
            step.audit_event(at, &fleet("x86-64-fleet")?),
            ScalingAuditEvent {
                at,
                fleet: fleet("x86-64-fleet")?,
                kind: ScalingEventKind::Terminated {
                    instance: instance()?,
                },
                cause: Some(ScalingCause::DrainFinished),
            }
        );
        Ok(())
    }
}
