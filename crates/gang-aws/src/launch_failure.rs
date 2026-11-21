//! Classifies whether a failed Auto Scaling launch proves that a fleet cannot grow.
//!
//! A failure of one instance type and availability zone is not evidence that other
//! candidates are exhausted. When the candidate count is unknown or greater than
//! one, unknown failures therefore remain warnings unless AWS reports an explicit
//! fleetwide error. Callers retain the original AWS status message as the reason.

use aws_sdk_autoscaling::types::{Activity, ScalingActivityStatusCode};

/// Returns whether this activity provides positive evidence that growth is blocked.
pub(crate) fn blocks_growth(activity: &Activity, candidate_count: Option<usize>) -> bool {
    if activity.status_code() != Some(&ScalingActivityStatusCode::Failed)
        || !activity
            .description()
            .is_some_and(|description| description.starts_with("Launching a new EC2 instance"))
    {
        return false;
    }

    let Some(message) = activity
        .status_message()
        .filter(|message| !message.is_empty())
    else {
        return false;
    };

    candidate_count == Some(1)
        || message
            .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.')
            .map(|token| token.trim_end_matches('.'))
            .any(|token| {
                matches!(
                    token,
                    "MaxSpotInstanceCountExceeded"
                        | "SpotMaxInstanceCountLimitExceeded"
                        | "VcpuLimitExceeded"
                        | "InstanceLimitExceeded"
                        | "UnauthorizedOperation"
                        | "AccessDenied"
                        | "AccessDeniedException"
                        | "InvalidIamInstanceProfile"
                        | "InvalidLaunchTemplateId.NotFound"
                        | "InvalidLaunchTemplateId.VersionNotFound"
                        | "InvalidAMIID.NotFound"
                        | "InvalidAMIID.Unavailable"
                )
            })
}

#[cfg(test)]
mod tests {
    use aws_sdk_autoscaling::types::{Activity, ScalingActivityStatusCode};
    use aws_smithy_types::DateTime;

    use super::blocks_growth;

    const LAUNCH: &str = "Launching a new EC2 instance";
    const UNSUPPORTED: &str = "The instance type 'r8id.4xlarge' is not supported in your requested Availability Zone 'us-east-1b'. Please retry your request by not specifying an Availability Zone or choosing us-east-1a, us-east-1c. Launching EC2 instance failed. Status Reason: InvalidFleetConfiguration";

    fn activity(
        status: Option<ScalingActivityStatusCode>,
        description: Option<&str>,
        message: Option<&str>,
    ) -> Activity {
        Activity::builder()
            .activity_id("activity-1")
            .auto_scaling_group_name("workers")
            .cause("At 2024-01-01T00:00:00Z a change in desired capacity was requested")
            // 2024-01-01T00:00:00Z, the activity start time.
            .start_time(DateTime::from_secs(1_704_067_200))
            .set_status_code(status)
            .set_description(description.map(str::to_owned))
            .set_status_message(message.map(str::to_owned))
            .build()
    }

    fn failed(message: &str) -> Activity {
        activity(
            Some(ScalingActivityStatusCode::Failed),
            Some(LAUNCH),
            Some(message),
        )
    }

    #[test]
    fn candidate_failure_does_not_prove_multiple_candidates_exhausted() {
        for message in [
            UNSUPPORTED,
            "InsufficientInstanceCapacity: no capacity available",
            "An unknown launch failure occurred",
        ] {
            let activity = failed(message);
            assert!(!blocks_growth(&activity, Some(2)), "{message}");
            assert!(!blocks_growth(&activity, Some(0)), "{message}");
            assert!(!blocks_growth(&activity, None), "{message}");
            assert!(blocks_growth(&activity, Some(1)), "{message}");
            assert_eq!(activity.status_message(), Some(message));
        }
    }

    #[test]
    fn explicit_fleetwide_codes_block_even_with_multiple_or_unknown_candidates() {
        for code in [
            "MaxSpotInstanceCountExceeded",
            "SpotMaxInstanceCountLimitExceeded",
            "VcpuLimitExceeded",
            "InstanceLimitExceeded",
            "UnauthorizedOperation",
            "AccessDenied",
            "AccessDeniedException",
            "InvalidIamInstanceProfile",
            "InvalidLaunchTemplateId.NotFound",
            "InvalidLaunchTemplateId.VersionNotFound",
            "InvalidAMIID.NotFound",
            "InvalidAMIID.Unavailable",
        ] {
            let message = format!("Launch failed: {code}. Please retry.");
            let activity = failed(&message);
            assert!(blocks_growth(&activity, Some(5)), "{code}");
            assert!(blocks_growth(&activity, None), "{code}");
            assert_eq!(activity.status_message(), Some(message.as_str()));
        }
    }

    #[test]
    fn requires_failed_launch_with_nonempty_message() {
        for status in [
            Some(ScalingActivityStatusCode::Successful),
            Some(ScalingActivityStatusCode::InProgress),
            None,
        ] {
            assert!(!blocks_growth(
                &activity(status, Some(LAUNCH), Some("AccessDenied")),
                Some(1)
            ));
        }
        for description in [None, Some("Terminating EC2 instance: i-123"), Some("")] {
            assert!(!blocks_growth(
                &activity(
                    Some(ScalingActivityStatusCode::Failed),
                    description,
                    Some("AccessDenied")
                ),
                Some(1)
            ));
        }
        for message in [None, Some("")] {
            assert!(!blocks_growth(
                &activity(
                    Some(ScalingActivityStatusCode::Failed),
                    Some(LAUNCH),
                    message
                ),
                Some(1)
            ));
        }
    }

    #[test]
    fn does_not_match_a_code_inside_another_code() {
        for message in [
            "NotAccessDenied",
            "AccessDeniedExtra",
            "AccessDenied_Exception",
            "AccessDeniedException.Other",
            "InvalidAMIID.NotFoundExtra",
            "Other.InvalidAMIID.NotFound",
            "InstanceLimitExceeded2",
        ] {
            assert!(!blocks_growth(&failed(message), Some(3)), "{message}");
        }
        assert!(blocks_growth(
            &failed("(AccessDeniedException): denied"),
            Some(3)
        ));
        assert!(blocks_growth(&failed("InvalidAMIID.NotFound."), Some(3)));
    }
}
