//! Shared storage representation of scaling audit event kinds.

use gang_types::{InstanceId, WorkerId};

use crate::ScalingEventKind;

gang_types::string_enum! {
    /// The stored name of each scaling event kind.
    enum KindTag("scaling event kind") {
        ScaledOut = "scaled_out",
        LaunchFailed = "launch_failed",
        Withdrawn = "withdrawn",
        DrainStarted = "drain_started",
        DrainCancelled = "drain_cancelled",
        Terminated = "terminated",
        ExitedEarly = "exited_early",
        WorkerRegistered = "worker_registered",
        WorkerDeregistered = "worker_deregistered",
    }
}

/// Borrowed fields that encode a scaling event kind in storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredScalingKind<'name, 'value> {
    /// Stored kind name.
    pub name: &'name str,
    /// Capacity before the change, when applicable.
    pub from_capacity: Option<u32>,
    /// Capacity after the change, when applicable.
    pub to_capacity: Option<u32>,
    /// Worker or instance identifier, when applicable.
    pub subject: Option<&'value str>,
    /// Launch or exit failure detail, when applicable.
    pub detail: Option<&'value str>,
}

/// A stored scaling kind cannot be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoredScalingKindError {
    /// The stored name is not recognized.
    #[error("unknown scaling event kind {0:?}")]
    UnknownKind(String),
    /// A field required by the kind is absent.
    #[error("missing {field} for scaling event kind {kind}")]
    Missing {
        /// Stored kind name.
        kind: &'static str,
        /// Missing field name.
        field: &'static str,
    },
    /// A stored field failed validation.
    #[error("invalid {field}: {detail}")]
    Invalid {
        /// Invalid field name.
        field: &'static str,
        /// Validation failure.
        detail: String,
    },
}

impl StoredScalingKindError {
    /// The storage field responsible for the error.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::UnknownKind(_) => "kind",
            Self::Missing { field, .. } | Self::Invalid { field, .. } => field,
        }
    }
}

impl ScalingEventKind {
    /// Flattens this kind into its borrowed storage representation.
    #[must_use]
    pub fn stored(&self) -> StoredScalingKind<'static, '_> {
        let (tag, from_capacity, to_capacity, subject, detail) = match self {
            Self::ScaledOut { from, to } => {
                (KindTag::ScaledOut, Some(*from), Some(*to), None, None)
            }
            Self::LaunchFailed { from, to, reason } => (
                KindTag::LaunchFailed,
                Some(*from),
                Some(*to),
                None,
                Some(reason.as_str()),
            ),
            Self::Withdrawn { from, to } => {
                (KindTag::Withdrawn, Some(*from), Some(*to), None, None)
            }
            Self::DrainStarted { worker } => (
                KindTag::DrainStarted,
                None,
                None,
                Some(worker.as_str()),
                None,
            ),
            Self::DrainCancelled { worker } => (
                KindTag::DrainCancelled,
                None,
                None,
                Some(worker.as_str()),
                None,
            ),
            Self::Terminated { instance } => (
                KindTag::Terminated,
                None,
                None,
                Some(instance.as_str()),
                None,
            ),
            Self::ExitedEarly { instance, failure } => (
                KindTag::ExitedEarly,
                None,
                None,
                Some(instance.as_str()),
                Some(failure.as_str()),
            ),
            Self::WorkerRegistered { worker } => (
                KindTag::WorkerRegistered,
                None,
                None,
                Some(worker.as_str()),
                None,
            ),
            Self::WorkerDeregistered { worker } => (
                KindTag::WorkerDeregistered,
                None,
                None,
                Some(worker.as_str()),
                None,
            ),
        };
        StoredScalingKind {
            name: tag.as_str(),
            from_capacity,
            to_capacity,
            subject,
            detail,
        }
    }
}

impl TryFrom<StoredScalingKind<'_, '_>> for ScalingEventKind {
    type Error = StoredScalingKindError;

    fn try_from(stored: StoredScalingKind<'_, '_>) -> Result<Self, Self::Error> {
        let tag = stored
            .name
            .parse::<KindTag>()
            .map_err(|_| StoredScalingKindError::UnknownKind(stored.name.to_owned()))?;
        let missing = |field| StoredScalingKindError::Missing {
            kind: tag.as_str(),
            field,
        };
        let from = || stored.from_capacity.ok_or_else(|| missing("from_capacity"));
        let to = || stored.to_capacity.ok_or_else(|| missing("to_capacity"));
        let detail = || {
            stored
                .detail
                .map(str::to_owned)
                .ok_or_else(|| missing("detail"))
        };
        let subject = || stored.subject.ok_or_else(|| missing("subject"));
        let worker = || {
            WorkerId::try_new(subject()?).map_err(|error| StoredScalingKindError::Invalid {
                field: "subject",
                detail: error.to_string(),
            })
        };
        let instance = || {
            InstanceId::try_new(subject()?).map_err(|error| StoredScalingKindError::Invalid {
                field: "subject",
                detail: error.to_string(),
            })
        };
        Ok(match tag {
            KindTag::ScaledOut => Self::ScaledOut {
                from: from()?,
                to: to()?,
            },
            KindTag::LaunchFailed => Self::LaunchFailed {
                from: from()?,
                to: to()?,
                reason: detail()?,
            },
            KindTag::Withdrawn => Self::Withdrawn {
                from: from()?,
                to: to()?,
            },
            KindTag::DrainStarted => Self::DrainStarted { worker: worker()? },
            KindTag::DrainCancelled => Self::DrainCancelled { worker: worker()? },
            KindTag::Terminated => Self::Terminated {
                instance: instance()?,
            },
            KindTag::ExitedEarly => Self::ExitedEarly {
                instance: instance()?,
                failure: detail()?,
            },
            KindTag::WorkerRegistered => Self::WorkerRegistered { worker: worker()? },
            KindTag::WorkerDeregistered => Self::WorkerDeregistered { worker: worker()? },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds() -> Vec<ScalingEventKind> {
        let worker = WorkerId::try_new("worker-1").expect("valid worker");
        let instance = InstanceId::try_new("instance-1").expect("valid instance");
        vec![
            ScalingEventKind::ScaledOut { from: 1, to: 2 },
            ScalingEventKind::LaunchFailed {
                from: 1,
                to: 2,
                reason: "failure".into(),
            },
            ScalingEventKind::Withdrawn { from: 2, to: 1 },
            ScalingEventKind::DrainStarted {
                worker: worker.clone(),
            },
            ScalingEventKind::DrainCancelled {
                worker: worker.clone(),
            },
            ScalingEventKind::Terminated {
                instance: instance.clone(),
            },
            ScalingEventKind::ExitedEarly {
                instance,
                failure: "failure".into(),
            },
            ScalingEventKind::WorkerRegistered {
                worker: worker.clone(),
            },
            ScalingEventKind::WorkerDeregistered { worker },
        ]
    }

    #[test]
    fn every_kind_round_trips() {
        for kind in kinds() {
            assert_eq!(ScalingEventKind::try_from(kind.stored()), Ok(kind.clone()));
        }
    }

    #[test]
    fn missing_fields_and_invalid_subjects() {
        for kind in kinds() {
            let original = kind.stored();
            for field in ["from_capacity", "to_capacity", "subject", "detail"] {
                let mut stored = original;
                let present = match field {
                    "from_capacity" => stored.from_capacity.take().is_some(),
                    "to_capacity" => stored.to_capacity.take().is_some(),
                    "subject" => stored.subject.take().is_some(),
                    "detail" => stored.detail.take().is_some(),
                    _ => unreachable!(),
                };
                if present {
                    let error = ScalingEventKind::try_from(stored).expect_err("field is required");
                    assert!(
                        matches!(&error, StoredScalingKindError::Missing { kind, field: actual } if *kind == original.name && *actual == field)
                    );
                    assert_eq!(error.field(), field);
                }
            }
            if original.subject.is_some() {
                let error = ScalingEventKind::try_from(StoredScalingKind {
                    subject: Some(""),
                    ..original
                })
                .expect_err("invalid subject");
                assert!(matches!(
                    error,
                    StoredScalingKindError::Invalid {
                        field: "subject",
                        ..
                    }
                ));
                assert_eq!(error.field(), "subject");
            }
        }
    }

    #[test]
    fn unknown_kind() {
        let stored = StoredScalingKind {
            name: "unknown",
            from_capacity: None,
            to_capacity: None,
            subject: None,
            detail: None,
        };
        let error = ScalingEventKind::try_from(stored).expect_err("unknown kind");
        assert_eq!(error, StoredScalingKindError::UnknownKind("unknown".into()));
        assert_eq!(error.field(), "kind");
    }
}
