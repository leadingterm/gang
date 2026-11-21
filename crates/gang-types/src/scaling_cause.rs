//! Reasons the autoscaler records for a scaling decision.

crate::string_enum! {
    /// Why the autoscaler started or retired a worker.
    pub enum ScalingCause("scaling cause") {
        /// Queued work needed another worker.
        Demand = "demand",
        /// The fleet's minimum in force, not its work, needed the capacity.
        Minimum = "minimum",
        /// A worker stayed idle past its timeout.
        IdleTimeout = "idle_timeout",
        /// A worker was cordoned because measured fleet load stayed below the waterline for the dwell.
        Utilization = "utilization",
        /// An in-service instance had no connected worker for its fleet's boot grace.
        Orphan = "orphan",
        /// A draining worker finished its work and was retired.
        DrainFinished = "drain_finished",
        /// A draining worker reached the longest allowed wait and was retired anyway.
        MaxDrainWait = "max_drain_wait",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_and_from_str_round_trip() {
        for cause in ScalingCause::ALL {
            assert_eq!(cause.as_str().parse::<ScalingCause>(), Ok(cause));
            assert_eq!(cause.to_string(), cause.as_str());
        }
    }

    #[test]
    fn utilization_has_a_stable_storage_name_and_is_enumerated() {
        assert_eq!("utilization".parse(), Ok(ScalingCause::Utilization));
        assert_eq!(ScalingCause::Utilization.as_str(), "utilization");
        assert!(ScalingCause::ALL.contains(&ScalingCause::Utilization));
    }

    #[test]
    fn unknown_cause_is_rejected() {
        assert!("idle".parse::<ScalingCause>().is_err());
        assert!("IdleTimeout".parse::<ScalingCause>().is_err());
        assert!("IDLE_TIMEOUT".parse::<ScalingCause>().is_err());
    }
}
