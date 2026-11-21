//! The steps one reconcile pass decided on, in the order the caller applies them.

use std::fmt;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_spi::{ScalingAuditEvent, ScalingEventKind};
use gang_types::{FleetName, InstanceId, ScalingCause, WorkerId};

use crate::{CordonDecision, Metric, ScaleDecision};

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

/// At most 14 ASCII bytes: fixed four decimals below 1e9, trimmed, or scientific notation.
/// Rounding the fixed branch up to 1e9 leaves just ten digits after trimming.
struct MetricText(Metric);

impl fmt::Display for MetricText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self.0.get();
        if value < 1e-6 {
            return formatter.write_str("0");
        }
        if value >= 1e9 {
            return write!(formatter, "{value:.4e}");
        }
        // The intermediate fixed representation is at most 15 bytes before zero trimming.
        let mut buffer = [0u8; 15];
        let mut remaining = &mut buffer[..];
        std::io::Write::write_fmt(&mut remaining, format_args!("{value:.4}"))
            .map_err(|_| fmt::Error)?;
        let length = 15 - remaining.len();
        let text = std::str::from_utf8(&buffer[..length]).map_err(|_| fmt::Error)?;
        formatter.write_str(text.trim_end_matches('0').trim_end_matches('.'))
    }
}

/// At most 25 ASCII bytes: a u64 seconds count, optional three millisecond digits, and `s`.
/// Sub-millisecond precision is omitted from this diagnostic, never from decision arithmetic.
struct DurationText(Duration);

impl fmt::Display for DurationText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.0.as_secs();
        let millis = self.0.subsec_millis();
        if millis == 0 {
            write!(formatter, "{seconds}s")
        } else {
            write!(formatter, "{seconds}.{millis:03}s")
        }
    }
}

impl PlanStep {
    /// Records a successfully applied decision. Physical cordon repairs are silent.
    ///
    /// Scale-out detail is at most 200 bytes, including dimensions, metrics and instance counts.
    /// Drain detail is at most 315 bytes, bounded by `148 + 7 + 4*25 + 3*14 + 10 + 3 + 5`;
    /// its last numeric values are one u32 and one u8. Both fit storage for every summary.
    #[must_use]
    pub fn audit_event(&self, at: Timestamp, fleet: &FleetName) -> Option<ScalingAuditEvent> {
        let mut kind = self.action.kind()?;
        match (&self.action, &mut kind) {
            (Action::ScaleOut { summary, .. }, ScalingEventKind::ScaledOut { detail, .. }) => {
                *detail = Some(format!(
                    "dimension={} demand={} capacity={} reliable={} booting={} needed={} extra_instances={}",
                    summary.dimension.as_str(),
                    MetricText(summary.demand),
                    MetricText(summary.capacity),
                    summary.reliable,
                    summary.booting,
                    summary.needed,
                    summary.extra_instances,
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
                    "dimension={} readiness={} horizon={} serving_instances={} remaining_capacity={} total_load={} queued_work={} waterline_percent={} dwell={} empty_after={} zero_overhead={}",
                    summary.dimension.as_str(),
                    DurationText(summary.readiness),
                    DurationText(summary.horizon),
                    summary.serving_instances,
                    MetricText(summary.remaining_capacity),
                    MetricText(summary.total_load),
                    MetricText(summary.queued_work),
                    summary.waterline_percent,
                    DurationText(summary.dwell),
                    DurationText(summary.empty_after),
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
        /// Captured resource scaling inputs.
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
    /// Silently applies or repairs physical drain intent for a hosted connection.
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
    fn decision_scalar_format_is_compact_and_preserves_ordinary_values() {
        for (value, expected) in [
            (f64::from_bits(1), "0"),
            (1e-7, "0"),
            (0.000_06, "0.0001"),
            (1.0 / 3.0, "0.3333"),
            (17.0, "17"),
            (12.5, "12.5"),
            (999_999_999.999_9, "999999999.9999"),
            (999_999_999.999_99, "1000000000"),
            (1e9, "1.0000e9"),
            (f64::MAX, "1.7977e308"),
        ] {
            let text = MetricText(metric(value)).to_string();
            assert_eq!(text, expected);
            assert!(text.len() <= 14);
        }
        assert_eq!(DurationText(Duration::ZERO).to_string(), "0s");
        assert_eq!(DurationText(Duration::from_mins(5)).to_string(), "300s");
        assert_eq!(
            DurationText(Duration::from_millis(1_234)).to_string(),
            "1.234s"
        );
        assert_eq!(
            DurationText(Duration::MAX).to_string(),
            "18446744073709551615.999s",
        );
        assert!(DurationText(Duration::MAX).to_string().len() <= 25);
    }

    #[test]
    fn extreme_scale_summaries_always_validate_within_the_proven_bound() -> TestResult {
        for value in [
            f64::from_bits(1),
            1.0 / 3.0,
            f64::MAX,
            999_999_999.999_9,
            999_999_999.999_99,
        ] {
            let value = metric(value);
            let audit = event(Action::ScaleOut {
                from: 0,
                to: u32::MAX,
                need: NeedCause::Demand,
                summary: ScaleDecision {
                    dimension: ScalingDimension::Cpu,
                    demand: value,
                    capacity: value,
                    reliable: u32::MAX,
                    booting: u32::MAX,
                    needed: u32::MAX,
                    extra_instances: u32::MAX,
                },
            })?;
            assert!(audit.kind.validate().is_ok());
            let ScalingEventKind::ScaledOut {
                detail: Some(detail),
                ..
            } = audit.kind
            else {
                return Err("missing scale summary".into());
            };
            assert!(detail.len() <= 200);
            assert!(detail.len() <= ScalingEventKind::MAX_DECISION_DETAIL_BYTES);
        }
        Ok(())
    }

    #[test]
    fn extreme_cordon_summaries_always_validate_within_the_proven_bound() -> TestResult {
        for value in [
            f64::from_bits(1),
            1.0 / 3.0,
            f64::MAX,
            999_999_999.999_9,
            999_999_999.999_99,
        ] {
            let value = metric(value);
            let audit = event(Action::StartDrain {
                worker: worker_id("i-0abc123")?,
                reason: DrainReason::Utilization,
                summary: Some(CordonDecision {
                    readiness: Duration::MAX,
                    horizon: Duration::MAX,
                    dimension: ScalingDimension::Cpu,
                    serving_instances: u32::MAX,
                    remaining_capacity: value,
                    total_load: value,
                    queued_work: value,
                    waterline_percent: u8::MAX,
                    dwell: Duration::MAX,
                    empty_after: Duration::MAX,
                }),
            })?;
            assert!(audit.kind.validate().is_ok());
            let ScalingEventKind::DrainStarted {
                detail: Some(detail),
                ..
            } = audit.kind
            else {
                return Err("missing cordon summary".into());
            };
            assert!(detail.len() <= 315);
            assert!(detail.len() <= ScalingEventKind::MAX_DECISION_DETAIL_BYTES);
        }
        Ok(())
    }

    #[test]
    fn scale_out_audits_as_scaled_out() -> TestResult {
        let summary = ScaleDecision {
            dimension: ScalingDimension::Memory,
            demand: metric(17.0),
            needed: 5,
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
        assert!(detail.contains("demand=17"));
        assert!(detail.contains("needed=5"));
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
