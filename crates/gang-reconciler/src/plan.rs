//! The steps one reconcile pass decided on, in the order the caller applies them.

use gang_clock::Timestamp;
use gang_spi::{ScalingAuditEvent, ScalingEventKind};
use gang_types::{FleetName, InstanceId, ScalingCause, WorkerId};

use crate::{CordonDecision, ScaleDecision};

/// An idempotent plan. The caller records audit events only after successful application.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Plan {
    /// The wall time of the pass.
    pub at: Timestamp,
    /// Steps in application order; fleet indices follow the configured fleet order.
    pub steps: Vec<PlanStep>,
}

impl Plan {
    /// Creates an empty plan.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// One action on the fleet at the corresponding index in the pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanStep {
    /// Configured fleet index.
    pub fleet: usize,
    /// The change to apply.
    pub action: Action,
}

impl PlanStep {
    /// Records a successfully applied decision. Physical cordon repairs are silent.
    #[must_use]
    pub fn audit_event(&self, at: Timestamp, fleet: &FleetName) -> Option<ScalingAuditEvent> {
        let mut kind = self.action.kind()?;
        match (&self.action, &mut kind) {
            (Action::ScaleOut { summary, .. }, ScalingEventKind::ScaledOut { detail, .. }) => {
                *detail = Some(format!(
                    "dimension={} readiness={:?} horizon={:?} slots={} reliable={} capacity={} pre_ready={} persistent_load={} transient_backlog={} transient_extra={} persistent_queue={} extra_instances={} reachability={} persistent_streams={} streams={} zero_overhead={}",
                    summary.dimension.as_str(),
                    summary.readiness,
                    summary.horizon,
                    summary.slots,
                    summary.reliable,
                    summary.capacity.get(),
                    summary.pre_ready.get(),
                    summary.persistent_load.get(),
                    summary.transient_backlog.get(),
                    summary.transient_extra.get(),
                    summary.persistent_queue.get(),
                    summary.extra_instances,
                    summary.reachability,
                    summary.persistent_streams,
                    summary.streams,
                    summary.zero_overhead,
                ));
            }
            (
                Action::StartDrain {
                    summary: Some(summary),
                    ..
                },
                ScalingEventKind::DrainStarted { detail, .. },
            ) => {
                *detail = Some(format!(
                    "dimension={} readiness={:?} horizon={:?} serving_instances={} remaining_capacity={} total_load={} queued_work={} waterline_percent={} dwell={:?} empty_after={:?} zero_overhead={}",
                    summary.dimension.as_str(),
                    summary.readiness,
                    summary.horizon,
                    summary.serving_instances,
                    summary.remaining_capacity.get(),
                    summary.total_load.get(),
                    summary.queued_work.get(),
                    summary.waterline_percent,
                    summary.dwell,
                    summary.empty_after,
                    summary.horizon.is_zero(),
                ));
            }
            _ => {}
        }
        Some(ScalingAuditEvent {
            at,
            fleet: fleet.clone(),
            kind,
            cause: self.action.cause(),
        })
    }
}

/// What sets the need for added capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeedCause {
    /// Queued or running work.
    Demand,
    /// The configured minimum exceeds the work's need.
    Minimum,
}

impl From<NeedCause> for ScalingCause {
    fn from(cause: NeedCause) -> Self {
        match cause {
            NeedCause::Demand => Self::Demand,
            NeedCause::Minimum => Self::Minimum,
        }
    }
}

/// Why a new physical-instance drain starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainReason {
    /// An idle surplus instance reached its timeout.
    IdleTimeout,
    /// N−1 capacity held the load waterline for a full dwell.
    Utilization,
}

impl From<DrainReason> for ScalingCause {
    fn from(reason: DrainReason) -> Self {
        match reason {
            DrainReason::IdleTimeout => Self::IdleTimeout,
            DrainReason::Utilization => Self::Utilization,
        }
    }
}

/// Why an instance is terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationCause {
    /// An in-service instance had no worker for its boot grace.
    Orphan,
    /// Every connection drained and all hosted work finished.
    DrainFinished,
    /// The longest allowed drain wait elapsed.
    MaxDrainWait,
    /// An idle worker served an obsolete action environment.
    StaleEnvironment,
}

impl From<TerminationCause> for ScalingCause {
    fn from(cause: TerminationCause) -> Self {
        match cause {
            TerminationCause::Orphan => Self::Orphan,
            TerminationCause::DrainFinished | TerminationCause::StaleEnvironment => {
                Self::DrainFinished
            }
            TerminationCause::MaxDrainWait => Self::MaxDrainWait,
        }
    }
}

/// A change applied through the provisioner or physical-instance scheduler port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Raises desired capacity, recording the inputs without allocating audit text.
    ScaleOut {
        /// Desired capacity after this pass's terminations.
        from: u32,
        /// Desired capacity to request.
        to: u32,
        /// What sets the capacity need.
        need: NeedCause,
        /// Captured purchase inputs.
        summary: ScaleDecision,
    },
    /// Withdraws requests whose launches have not started.
    Withdraw {
        /// Previous desired capacity.
        from: u32,
        /// New desired capacity.
        to: u32,
    },
    /// Cordons every connection of the representative worker's physical instance.
    StartDrain {
        /// A currently hosted representative connection.
        worker: WorkerId,
        /// Why the new drain starts.
        reason: DrainReason,
        /// N−1 inputs for utilization drains; absent for idle timeout.
        summary: Option<CordonDecision>,
    },
    /// Silently reapplies an existing owned physical cordon to a mismatched connection.
    ReinforceDrain(WorkerId),
    /// Resumes an owned physical drain before buying capacity.
    CancelDrain(WorkerId, NeedCause),
    /// Terminates an instance; the provisioner lowers desired capacity itself.
    Terminate {
        /// Physical instance to terminate.
        instance: InstanceId,
        /// Why termination is required.
        cause: TerminationCause,
    },
}

impl Action {
    /// The event kind without formatted detail; cordon repairs have no event.
    #[must_use]
    pub fn kind(&self) -> Option<ScalingEventKind> {
        Some(match self {
            Self::ScaleOut { from, to, .. } => ScalingEventKind::ScaledOut {
                from: *from,
                to: *to,
                detail: None,
            },
            Self::Withdraw { from, to } => ScalingEventKind::Withdrawn {
                from: *from,
                to: *to,
            },
            Self::StartDrain { worker, .. } => ScalingEventKind::DrainStarted {
                worker: worker.clone(),
                detail: None,
            },
            Self::ReinforceDrain(_) => return None,
            Self::CancelDrain(worker, _) => ScalingEventKind::DrainCancelled {
                worker: worker.clone(),
            },
            Self::Terminate { instance, .. } => ScalingEventKind::Terminated {
                instance: instance.clone(),
            },
        })
    }

    /// The decision cause; an idempotent repair is not a new decision.
    #[must_use]
    pub fn cause(&self) -> Option<ScalingCause> {
        Some(match self {
            Self::ScaleOut { need, .. } | Self::CancelDrain(_, need) => (*need).into(),
            Self::Withdraw { .. } => ScalingCause::Demand,
            Self::StartDrain { reason, .. } => (*reason).into(),
            Self::ReinforceDrain(_) => return None,
            Self::Terminate { cause, .. } => (*cause).into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use gang_testkit::{TestResult, worker_id};

    use super::*;
    use crate::ScalingDimension;
    use crate::decision::metric;

    fn event(action: Action) -> TestResult<ScalingAuditEvent> {
        PlanStep { fleet: 0, action }
            .audit_event(
                Timestamp::from_unix_millis(42),
                &FleetName::try_new("workers")?,
            )
            .ok_or_else(|| "expected an audited decision".into())
    }

    #[test]
    fn scale_out_audits_as_scaled_out() -> TestResult {
        let summary = ScaleDecision {
            dimension: ScalingDimension::Memory,
            persistent_queue: metric(17.0),
            zero_overhead: true,
            ..ScaleDecision::default()
        };
        let audit = event(Action::ScaleOut {
            from: 2,
            to: 5,
            need: NeedCause::Demand,
            summary,
        })?;
        assert_eq!(audit.cause, Some(ScalingCause::Demand));
        let ScalingEventKind::ScaledOut {
            from,
            to,
            detail: Some(detail),
        } = audit.kind
        else {
            return Err("missing scale inputs".into());
        };
        assert_eq!((from, to), (2, 5));
        assert!(detail.contains("dimension=memory"));
        assert!(detail.contains("persistent_queue=17"));
        assert!(detail.contains("zero_overhead=true"));
        Ok(())
    }

    #[test]
    fn withdraw_audits_as_withdrawn_for_demand() -> TestResult {
        let audit = event(Action::Withdraw { from: 4, to: 2 })?;
        assert_eq!(audit.kind, ScalingEventKind::Withdrawn { from: 4, to: 2 });
        assert_eq!(audit.cause, Some(ScalingCause::Demand));
        Ok(())
    }

    #[test]
    fn start_drain_audits_as_drain_started() -> TestResult {
        let worker = worker_id("i-0abc123")?;
        let audit = event(Action::StartDrain {
            worker: worker.clone(),
            reason: DrainReason::IdleTimeout,
            summary: None,
        })?;
        assert_eq!(
            audit.kind,
            ScalingEventKind::DrainStarted {
                worker,
                detail: None
            }
        );
        assert_eq!(audit.cause, Some(ScalingCause::IdleTimeout));
        Ok(())
    }

    #[test]
    fn utilization_drain_records_waterline_and_dwell() -> TestResult {
        let audit = event(Action::StartDrain {
            worker: worker_id("i-0abc123")?,
            reason: DrainReason::Utilization,
            summary: Some(CordonDecision {
                waterline_percent: 70,
                dwell: std::time::Duration::from_mins(5),
                ..CordonDecision::default()
            }),
        })?;
        assert_eq!(audit.cause, Some(ScalingCause::Utilization));
        let ScalingEventKind::DrainStarted {
            detail: Some(detail),
            ..
        } = audit.kind
        else {
            return Err("missing cordon inputs".into());
        };
        assert!(detail.contains("waterline_percent=70"));
        assert!(detail.contains("dwell=300s"));
        Ok(())
    }

    #[test]
    fn reinforcement_never_audits() -> TestResult {
        let action = Action::ReinforceDrain(worker_id("i-0abc123")?);
        assert_eq!(action.kind(), None);
        assert_eq!(action.cause(), None);
        assert_eq!(
            PlanStep { fleet: 0, action }
                .audit_event(Timestamp::default(), &FleetName::try_new("workers")?),
            None
        );
        Ok(())
    }

    #[test]
    fn cancel_drain_audits_as_drain_cancelled() -> TestResult {
        let worker = worker_id("i-0abc123")?;
        let audit = event(Action::CancelDrain(worker.clone(), NeedCause::Demand))?;
        assert_eq!(audit.kind, ScalingEventKind::DrainCancelled { worker });
        assert_eq!(audit.cause, Some(ScalingCause::Demand));
        Ok(())
    }

    #[test]
    fn capacity_added_for_minimum_audits_as_minimum() -> TestResult {
        for action in [
            Action::ScaleOut {
                from: 0,
                to: 1,
                need: NeedCause::Minimum,
                summary: ScaleDecision::default(),
            },
            Action::CancelDrain(worker_id("i-0abc123")?, NeedCause::Minimum),
        ] {
            assert_eq!(event(action)?.cause, Some(ScalingCause::Minimum));
        }
        Ok(())
    }

    #[test]
    fn terminate_audits_as_terminated() -> TestResult {
        let instance = InstanceId::try_new("i-0abc123")?;
        let audit = event(Action::Terminate {
            instance: instance.clone(),
            cause: TerminationCause::DrainFinished,
        })?;
        assert_eq!(audit.kind, ScalingEventKind::Terminated { instance });
        assert_eq!(audit.cause, Some(ScalingCause::DrainFinished));
        Ok(())
    }
}
