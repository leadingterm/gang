//! Classifies whether a failed Auto Scaling launch proves that a fleet cannot grow.
//!
//! A failure of one instance type and availability zone is not evidence that other
//! candidates are exhausted. Multiple failures can establish exhaustion only when
//! they cover every configured candidate since the latest launch progress.

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

/// Returns the newest failure message only when current launch options are exhausted.
///
/// Activities are newest first and already restricted to the caller's evidence window.
pub(crate) fn blocked_reason<'a>(
    activities: &'a [Activity],
    candidates: Option<&crate::group::LaunchCandidates>,
    candidate_count: Option<usize>,
) -> Option<&'a str> {
    let mut launches = launch_activities(activities);
    let newest = launches.next()?;
    if newest.status_code() != Some(&ScalingActivityStatusCode::Failed) {
        return None;
    }
    let reason = newest.status_message().filter(|message| !message.is_empty())?;
    if blocks_growth(newest, candidate_count) {
        return Some(reason);
    }
    let candidates = candidates?;
    let mut attempts = Vec::new();
    for activity in std::iter::once(newest).chain(launches) {
        if activity.status_code() != Some(&ScalingActivityStatusCode::Failed) {
            break;
        }
        if let Some(attempt) = classify_attempt(activity, candidates) {
            attempts.push(attempt);
        }
    }
    let mut pairs = candidates.pairs().peekable();
    pairs.peek()?;
    pairs
        .all(|(instance_type, zone)| {
            attempts.iter().any(|attempt| {
                attempt.zone == zone
                    && attempt.instance_type.is_none_or(|actual| actual == instance_type)
            })
        })
        .then_some(reason)
}

/// Iterates over launch activities in their original order.
pub(crate) fn launch_activities(activities: &[Activity]) -> impl Iterator<Item = &Activity> {
    activities.iter().filter(|activity| is_launch(activity))
}

fn is_launch(activity: &Activity) -> bool {
    activity
        .description()
        .is_some_and(|description| description.starts_with("Launching a new EC2 instance"))
}

struct Attempt<'a> {
    zone: &'a str,
    instance_type: Option<&'a str>,
}

#[derive(serde::Deserialize)]
struct Details<'a> {
    #[serde(rename = "Availability Zone", borrow)]
    zone: Option<&'a str>,
    #[serde(rename = "Instance Type", borrow)]
    instance_type: Option<&'a str>,
}

fn tokens(message: &str) -> impl Iterator<Item = &str> {
    message
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' && ch != '-')
        .map(|token| token.trim_end_matches('.'))
}

fn classify_attempt<'a>(
    activity: &'a Activity,
    candidates: &crate::group::LaunchCandidates,
) -> Option<Attempt<'a>> {
    let message = activity.status_message()?;
    let capacity = tokens(message)
        .any(|token| matches!(token, "InsufficientInstanceCapacity" | "UnfulfillableCapacity"));
    let invalid = tokens(message).any(|token| token == "InvalidFleetConfiguration");
    if !capacity && !invalid {
        return None;
    }

    let details = activity
        .details()
        .and_then(|details| serde_json::from_str::<Details<'_>>(details).ok());
    let unique_match = |is_match: &dyn Fn(&str) -> bool| {
        let mut matching = tokens(message).filter(|token| is_match(token));
        let first = matching.next()?;
        matching.all(|token| token == first).then_some(first)
    };
    let zone = match details.as_ref().and_then(|details| details.zone) {
        Some(zone) => zone,
        None => unique_match(&|token| candidates.pairs().any(|(_, zone)| zone == token))?,
    };
    if !candidates.pairs().any(|(_, candidate_zone)| candidate_zone == zone) {
        return None;
    }
    let instance_type = match details.as_ref().and_then(|details| details.instance_type) {
        Some(instance_type) => Some(instance_type),
        None => unique_match(&|token| candidates.pairs().any(|(kind, _)| kind == token)),
    };
    if let Some(instance_type) = instance_type {
        if !candidates.pairs().any(|(kind, _)| kind == instance_type) {
            return None;
        }
    } else if invalid || tokens(message).any(looks_like_instance_type) {
        return None;
    }
    Some(Attempt { zone, instance_type })
}

fn looks_like_instance_type(token: &str) -> bool {
    let Some((family, size)) = token.split_once('.') else {
        return false;
    };
    family.bytes().any(|byte| byte.is_ascii_digit())
        && (matches!(size, "nano" | "micro" | "small" | "medium" | "large" | "metal")
            || size.ends_with("xlarge")
            || size.starts_with("metal-"))
}

#[cfg(test)]
mod tests {
    use aws_sdk_autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput;
    use aws_sdk_autoscaling::types::{
        Activity, AutoScalingGroup, LaunchTemplate, LaunchTemplateOverrides, MixedInstancesPolicy,
        ScalingActivityStatusCode,
    };
    use aws_smithy_types::DateTime;

    use super::{blocked_reason, blocks_growth};
    use crate::group::{Group, LaunchCandidates};

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

    fn candidates() -> LaunchCandidates {
        let group = AutoScalingGroup::builder()
            .auto_scaling_group_name("workers")
            .desired_capacity(1)
            .availability_zones("us-east-1a")
            .availability_zones("us-east-1b")
            .mixed_instances_policy(
                MixedInstancesPolicy::builder()
                    .launch_template(
                        LaunchTemplate::builder()
                            .overrides(LaunchTemplateOverrides::builder().instance_type("c7g.large").build())
                            .overrides(LaunchTemplateOverrides::builder().instance_type("m7g.large").build())
                            .build(),
                    )
                    .build(),
            )
            .build();
        let mut output = DescribeAutoScalingGroupsOutput::builder().auto_scaling_groups(group).build();
        Group::read(&mut output, "workers")
            .expect("fixture has a valid group")
            .launch_candidates
            .expect("fixture has explicit candidates")
    }

    fn with_details(message: &str, details: &str) -> Activity {
        Activity::builder()
            .activity_id("activity-1")
            .auto_scaling_group_name("workers")
            .cause("fixture")
            .start_time(DateTime::from_secs(1_704_067_200))
            .status_code(ScalingActivityStatusCode::Failed)
            .description(LAUNCH)
            .status_message(message)
            .details(details)
            .build()
    }

    #[test]
    fn proves_full_candidate_coverage_since_progress() {
        let candidates = candidates();
        let reason = "InsufficientInstanceCapacity: capacity unavailable; exact reason.";
        let zone_a = || with_details(reason, r#"{"Availability Zone":"us-east-1a"}"#);
        let zone_b = || with_details("UnfulfillableCapacity", r#"{"Availability Zone":"us-east-1b"}"#);
        assert_eq!(blocked_reason(&[zone_a()], Some(&candidates), Some(4)), None);
        assert_eq!(blocked_reason(&[zone_a(), zone_b()], Some(&candidates), Some(4)), Some(reason));
        assert_eq!(blocked_reason(&[zone_a(), zone_b()], None, Some(4)), None);

        let pair = |kind: &str, zone: &str| {
            with_details(
                "InvalidFleetConfiguration: unsupported",
                &format!(r#"{{"Availability Zone":"{zone}","Instance Type":"{kind}"}}"#),
            )
        };
        assert_eq!(blocked_reason(&[pair("c7g.large", "us-east-1a")], Some(&candidates), Some(4)), None);
        let all = [
            pair("c7g.large", "us-east-1a"),
            pair("m7g.large", "us-east-1a"),
            pair("c7g.large", "us-east-1b"),
            pair("m7g.large", "us-east-1b"),
        ];
        assert_eq!(blocked_reason(&all, Some(&candidates), Some(4)), Some("InvalidFleetConfiguration: unsupported"));
        let progress = activity(Some(ScalingActivityStatusCode::InProgress), Some(LAUNCH), None);
        assert_eq!(blocked_reason(&[progress.clone(), zone_a(), zone_b()], Some(&candidates), Some(4)), None);
        assert_eq!(blocked_reason(&[zone_a(), progress, zone_a(), zone_b()], Some(&candidates), Some(4)), None);
    }

    #[test]
    fn unattributed_or_unknown_failures_do_not_cover_candidates() {
        let candidates = candidates();
        let other_zone = with_details("InsufficientInstanceCapacity", r#"{"Availability Zone":"us-east-1z"}"#);
        let other_type = with_details("InsufficientInstanceCapacity", r#"{"Availability Zone":"us-east-1a","Instance Type":"x9.large"}"#);
        let no_zone = failed("InsufficientInstanceCapacity");
        let zone_b = with_details("InsufficientInstanceCapacity", r#"{"Availability Zone":"us-east-1b"}"#);
        for failure in [
            other_zone,
            other_type,
            no_zone,
            failed("InsufficientInstanceCapacity us-east-1a x9.large"),
            failed("InsufficientInstanceCapacity us-east-1a m8i.metal-24xl"),
            failed("InsufficientInstanceCapacity us-east-1a m8i.metal-48xl"),
        ] {
            assert_eq!(blocked_reason(&[failure, zone_b.clone()], Some(&candidates), Some(4)), None);
        }
        assert_eq!(blocked_reason(&[failed("InsufficientInstanceCapacity us-east-1a-extra"), zone_b], Some(&candidates), Some(4)), None);
    }

    #[test]
    fn ignores_nonlaunch_activities_and_preserves_newest_reason() {
        let candidates = candidates();
        let terminate = activity(Some(ScalingActivityStatusCode::Failed), Some("Terminating EC2 instance"), Some("AccessDenied"));
        let a = failed("InsufficientInstanceCapacity us-east-1a");
        let b = failed("UnfulfillableCapacity us-east-1b");
        assert_eq!(blocked_reason(&[terminate, a, b], Some(&candidates), Some(4)), Some("InsufficientInstanceCapacity us-east-1a"));
        assert_eq!(blocked_reason(&[failed("AccessDenied")], None, None), Some("AccessDenied"));
        assert_eq!(
            blocked_reason(
                &[failed("InvalidFleetConfiguration c7g.large us-east-1a"), failed("VcpuLimitExceeded")],
                Some(&candidates),
                Some(4),
            ),
            None
        );
    }

    #[test]
    fn uses_exact_message_tokens_and_prefers_structured_details() {
        let candidates = candidates();
        let failures = [
            failed("InvalidFleetConfiguration c7g.large us-east-1a"),
            failed("InvalidFleetConfiguration m7g.large us-east-1a"),
            failed("InvalidFleetConfiguration c7g.large us-east-1b"),
            failed("InvalidFleetConfiguration m7g.large us-east-1b"),
        ];
        assert_eq!(blocked_reason(&failures, Some(&candidates), Some(4)), failures[0].status_message());
        let ambiguous = failed("InvalidFleetConfiguration c7g.large us-east-1a us-east-1b");
        assert_eq!(blocked_reason(&[ambiguous, failures[1].clone(), failures[2].clone(), failures[3].clone()], Some(&candidates), Some(4)), None);
        let preferred = with_details(
            "InvalidFleetConfiguration c7g.large us-east-1a us-east-1b",
            r#"{"Availability Zone":"us-east-1a","Instance Type":"c7g.large"}"#,
        );
        assert_eq!(blocked_reason(&[preferred, failures[1].clone(), failures[2].clone(), failures[3].clone()], Some(&candidates), Some(4)), Some("InvalidFleetConfiguration c7g.large us-east-1a us-east-1b"));
        let wrong_type = failed("InvalidFleetConfiguration c7g.large-extra us-east-1a");
        assert_eq!(blocked_reason(&[wrong_type, failures[1].clone(), failures[2].clone(), failures[3].clone()], Some(&candidates), Some(4)), None);
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
