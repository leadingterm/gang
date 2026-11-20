//! Provider observations retained as short-lived infrastructure status.

use gang_clock::Timestamp;
use gang_types::{FleetName, InstanceId};
use serde::{Deserialize, Serialize};

/// One provider observation and its source identity and time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureEvent {
    /// Provider event identifier, stable across redelivery.
    pub id: String,
    /// Time of the observation at its source.
    pub at: Timestamp,
    /// The observed infrastructure change.
    pub kind: InfrastructureEventKind,
}

/// An observation together with the fleet it concerns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetInfrastructureEvent {
    /// The fleet to which the observation belongs.
    pub fleet: FleetName,
    /// The provider observation.
    pub event: InfrastructureEvent,
}

/// A provider-observed infrastructure change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum InfrastructureEventKind {
    /// The provider recommends rebalancing an instance.
    RebalanceRecommendation {
        /// The instance at risk.
        instance: InstanceId,
    },
    /// The provider warns of an interruption.
    SpotInterruption {
        /// The instance at risk.
        instance: InstanceId,
        /// The action the provider intends to take.
        action: InfrastructureInterruptionAction,
    },
    /// An instance launched without a documented rebalance cause.
    InstanceLaunched {
        /// The new instance.
        instance: InstanceId,
    },
    /// A rebalance replacement was launched.
    ReplacementLaunched {
        /// The new instance.
        instance: InstanceId,
        /// The old instance, only when known from the source.
        replacing: Option<InstanceId>,
    },
    /// An instance was terminated.
    InstanceTerminated {
        /// The terminated instance.
        instance: InstanceId,
        /// The provider's termination reason.
        reason: InfrastructureTerminationReason,
    },
    /// The provider could not supply capacity.
    CapacityUnavailable {
        /// The provider's reason.
        reason: String,
    },
}

/// The action announced in an interruption warning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InfrastructureInterruptionAction {
    /// The instance will terminate.
    Terminate,
    /// The instance will stop.
    Stop,
    /// The instance will hibernate.
    Hibernate,
}

/// The provider's reason for terminating an instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InfrastructureTerminationReason {
    /// A rebalance replaced the instance.
    Rebalance,
    /// Spot capacity was reclaimed.
    SpotReclaimed,
    /// Another provider-supplied reason.
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fleet_observation_round_trips_with_numeric_source_time() {
        let value = json!({"fleet": "workers", "event": {"id": "notice-1", "at": 1234, "kind": {"type": "spotInterruption", "instance": "i-1", "action": "hibernate"}}});
        let event: FleetInfrastructureEvent =
            serde_json::from_value(value.clone()).expect("valid observation");
        assert_eq!(serde_json::to_value(event).expect("serializes"), value);
    }

    #[test]
    fn ordinary_launch_round_trips_distinct_from_replacement() {
        let value = json!({"type": "instanceLaunched", "instance": "i-1"});
        let kind: InfrastructureEventKind =
            serde_json::from_value(value.clone()).expect("launch parses");
        assert!(matches!(
            kind,
            InfrastructureEventKind::InstanceLaunched { .. }
        ));
        assert_eq!(serde_json::to_value(kind).expect("serializes"), value);
    }

    #[test]
    fn termination_reason_preserves_provider_text() {
        let kind = InfrastructureEventKind::InstanceTerminated {
            instance: InstanceId::try_new("i-1").expect("valid instance"),
            reason: InfrastructureTerminationReason::Other("provider reason".into()),
        };
        let value = json!({"type": "instanceTerminated", "instance": "i-1", "reason": {"other": "provider reason"}});
        assert_eq!(serde_json::to_value(&kind).expect("serializes"), value);
        assert_eq!(
            serde_json::from_value::<InfrastructureEventKind>(value).expect("parses"),
            kind
        );
    }
}
