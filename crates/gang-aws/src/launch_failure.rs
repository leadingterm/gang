//! Classifies whether a failed Auto Scaling launch proves that a fleet cannot grow.
//!
//! A failure of one instance type and availability zone is not evidence that other
//! candidates are exhausted. Multiple failures can establish exhaustion only when
//! they cover every configured candidate since the latest launch progress.

use std::borrow::Cow;

use aws_sdk_autoscaling::types::{Activity, ScalingActivityStatusCode};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Assessment<'a> {
    Blocked(&'a str),
    NotBlocked,
    Incomplete,
}

/// Assesses newest-first activities in the caller's evidence window.
///
/// A block requires complete history, since an older launch may still be in flight.
pub(crate) fn assess<'a>(
    activities: &'a [Activity],
    candidates: Option<&crate::group::LaunchCandidates<'_>>,
    history_complete: bool,
) -> Assessment<'a> {
    if has_launch_in_flight(activities) {
        return Assessment::NotBlocked;
    }
    let mut launches = launch_activities(activities);
    let Some(newest) = launches.next() else {
        return if history_complete {
            Assessment::NotBlocked
        } else {
            Assessment::Incomplete
        };
    };
    if newest.status_code() != Some(&ScalingActivityStatusCode::Failed) {
        return Assessment::NotBlocked;
    }
    let Some(reason) = newest
        .status_message()
        .filter(|message| !message.is_empty())
    else {
        return Assessment::NotBlocked;
    };
    if is_global_failure(reason) {
        return if history_complete {
            Assessment::Blocked(reason)
        } else {
            Assessment::Incomplete
        };
    }
    let Some(candidates) = candidates else {
        return Assessment::NotBlocked;
    };
    let mut attempts = Vec::new();
    let mut progress_boundary = false;
    for activity in std::iter::once(newest).chain(launches) {
        if activity.status_code() != Some(&ScalingActivityStatusCode::Failed) {
            progress_boundary = true;
            break;
        }
        if let Some(attempt) = classify_attempt(activity, candidates) {
            attempts.push(attempt);
        }
    }
    let mut pairs = candidates.pairs().peekable();
    if pairs.peek().is_none() {
        return Assessment::NotBlocked;
    }
    let Some(newest_attempt) = attempts.first() else {
        return if history_complete || progress_boundary {
            Assessment::NotBlocked
        } else {
            Assessment::Incomplete
        };
    };
    let covered = pairs.all(|(instance_type, zone)| {
        attempts.iter().any(|attempt| {
            attempt.zone == zone
                && attempt
                    .instance_type
                    .as_deref()
                    .is_none_or(|actual| actual == instance_type)
        })
    });
    if covered {
        if history_complete {
            // Every classified attempt matches at least one current candidate.
            Assessment::Blocked(newest_attempt.message)
        } else {
            Assessment::Incomplete
        }
    } else if history_complete || progress_boundary {
        Assessment::NotBlocked
    } else {
        Assessment::Incomplete
    }
}

fn is_global_failure(message: &str) -> bool {
    message
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

/// Iterates over launch activities in their original order.
fn launch_activities(activities: &[Activity]) -> impl Iterator<Item = &Activity> {
    activities.iter().filter(|activity| is_launch(activity))
}

/// An active launch, or an active activity of unknown kind, can still grow the fleet.
fn has_launch_in_flight(activities: &[Activity]) -> bool {
    activities.iter().any(|activity| {
        let terminal = matches!(
            activity
                .status_code()
                .map(ScalingActivityStatusCode::as_str),
            Some("Failed" | "Successful" | "Cancelled")
        );
        let termination = activity
            .description()
            .is_some_and(|description| description.starts_with("Terminating EC2 instance"));
        !terminal && !termination
    })
}

fn is_launch(activity: &Activity) -> bool {
    activity
        .description()
        .is_some_and(|description| description.starts_with("Launching a new EC2 instance"))
}

struct Attempt<'a> {
    zone: Cow<'a, str>,
    instance_type: Option<Cow<'a, str>>,
    message: &'a str,
}

#[derive(serde::Deserialize)]
struct Details<'a> {
    #[serde(rename = "Availability Zone", borrow)]
    zone: Option<Cow<'a, str>>,
    #[serde(rename = "Instance Type", borrow)]
    instance_type: Option<Cow<'a, str>>,
}

fn tokens(message: &str) -> impl Iterator<Item = &str> {
    message
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' && ch != '-')
        .map(|token| token.trim_end_matches('.'))
}

fn classify_attempt<'a>(
    activity: &'a Activity,
    candidates: &crate::group::LaunchCandidates<'_>,
) -> Option<Attempt<'a>> {
    let message = activity.status_message()?;
    // AWS documents these status messages without error codes:
    // https://docs.aws.amazon.com/autoscaling/ec2/userguide/ts-as-instancelaunchfailure.html#ts-as-capacity-2
    let specific_capacity = message
        .split_once("We currently do not have sufficient ")
        .and_then(|(_, rest)| rest.split_once(" capacity in the Availability Zone you requested"))
        .map(|(kind, rest)| {
            let zone = rest
                .strip_prefix(" (")
                .and_then(|rest| rest.split_once(')'))
                .map(|(zone, _)| zone);
            (kind, zone)
        });
    let unsupported = message
        .split_once("Your requested instance type (")
        .and_then(|(_, rest)| {
            rest.split_once(") is not supported in your requested Availability Zone (")
        })
        .and_then(|(kind, rest)| rest.split_once(')').map(|(zone, _)| (kind, zone)));
    let capacity = message
        .contains("There is no Spot capacity available that matches your request.")
        || specific_capacity.is_some()
        || tokens(message).any(|token| {
            matches!(
                token,
                "InsufficientInstanceCapacity" | "UnfulfillableCapacity"
            )
        });
    let invalid =
        unsupported.is_some() || tokens(message).any(|token| token == "InvalidFleetConfiguration");
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
    let (detail_zone, detail_type) = details
        .map(|details| (details.zone, details.instance_type))
        .unwrap_or_default();
    let reported_zone = specific_capacity
        .and_then(|(_, zone)| zone)
        .or(unsupported.map(|(_, zone)| zone));
    let zone = match detail_zone.filter(|zone| !zone.trim().is_empty()) {
        Some(zone) => zone,
        None => Cow::Borrowed(match reported_zone {
            Some(zone) => zone,
            None => unique_match(&|token| candidates.pairs().any(|(_, zone)| zone == token))?,
        }),
    };
    if !candidates
        .pairs()
        .any(|(_, candidate_zone)| candidate_zone == zone.as_ref())
    {
        return None;
    }
    if reported_zone.is_some_and(|reported| reported != zone.as_ref()) {
        return None;
    }
    let reported_type = specific_capacity
        .map(|(kind, _)| kind)
        .or(unsupported.map(|(kind, _)| kind));
    let instance_type = match detail_type.filter(|kind| !kind.trim().is_empty()) {
        Some(instance_type) => Some(instance_type),
        None => reported_type.map(Cow::Borrowed).or_else(|| {
            unique_match(&|token| candidates.pairs().any(|(kind, _)| kind == token))
                .map(Cow::Borrowed)
        }),
    };
    if let Some(instance_type) = &instance_type {
        if reported_type.is_some_and(|reported| reported != instance_type.as_ref())
            || !candidates
                .pairs()
                .any(|(kind, _)| kind == instance_type.as_ref())
        {
            return None;
        }
    } else if invalid || tokens(message).any(looks_like_instance_type) {
        return None;
    }
    Some(Attempt {
        zone,
        instance_type,
        message,
    })
}

fn looks_like_instance_type(token: &str) -> bool {
    let Some((family, size)) = token.split_once('.') else {
        return false;
    };
    family.bytes().any(|byte| byte.is_ascii_digit())
        && (matches!(
            size,
            "nano" | "micro" | "small" | "medium" | "large" | "metal"
        ) || size.ends_with("xlarge")
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

    use super::{Assessment, assess};
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

    fn candidate_snapshot(single: bool) -> DescribeAutoScalingGroupsOutput {
        let mut template = LaunchTemplate::builder().overrides(
            LaunchTemplateOverrides::builder()
                .instance_type("c7g.large")
                .build(),
        );
        if !single {
            template = template.overrides(
                LaunchTemplateOverrides::builder()
                    .instance_type("m7g.large")
                    .build(),
            );
        }
        let mut group = AutoScalingGroup::builder()
            .auto_scaling_group_name("workers")
            .desired_capacity(1)
            .max_size(1000)
            .availability_zones("us-east-1a");
        if !single {
            group = group.availability_zones("us-east-1b");
        }
        let group = group
            .mixed_instances_policy(
                MixedInstancesPolicy::builder()
                    .launch_template(template.build())
                    .build(),
            )
            .build();
        DescribeAutoScalingGroupsOutput::builder()
            .auto_scaling_groups(group)
            .build()
    }

    fn candidates(output: &DescribeAutoScalingGroupsOutput) -> LaunchCandidates<'_> {
        Group::read(output, "workers")
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
    fn documented_uncoded_spot_stockouts_require_every_zone() {
        let output = candidate_snapshot(false);
        let candidates = candidates(&output);
        let reason = "There is no Spot capacity available that matches your request. Launching EC2 instance failed.";
        let a = with_details(reason, r#"{"Availability Zone":"us-east-1a"}"#);
        let b = with_details(reason, r#"{"Availability Zone":"us-east-1b"}"#);
        assert_eq!(
            assess(std::slice::from_ref(&a), Some(&candidates), true),
            Assessment::NotBlocked
        );
        assert_eq!(
            assess(std::slice::from_ref(&a), Some(&candidates), false),
            Assessment::Incomplete
        );
        assert_eq!(
            assess(&[a.clone(), b.clone()], Some(&candidates), true),
            Assessment::Blocked(reason)
        );
        assert_eq!(
            assess(&[a, b], Some(&candidates), false),
            Assessment::Incomplete
        );
        let coded = [
            with_details(
                "UnfulfillableCapacity",
                r#"{"Availability Zone":"us-east-1a"}"#,
            ),
            with_details(
                "InsufficientInstanceCapacity",
                r#"{"Availability Zone":"us-east-1b"}"#,
            ),
        ];
        assert_eq!(
            assess(&coded, Some(&candidates), true),
            Assessment::Blocked("UnfulfillableCapacity")
        );
    }

    #[test]
    fn documented_type_specific_failures_only_cover_the_reported_pair() {
        let output = candidate_snapshot(false);
        let candidates = candidates(&output);
        let reason = "We currently do not have sufficient c7g.large capacity in the Availability Zone you requested. Launching EC2 instance failed.";
        let first = with_details(reason, r#"{"Availability Zone":"us-east-1a"}"#);
        let remaining = [
            failed("InvalidFleetConfiguration m7g.large us-east-1a"),
            failed("InvalidFleetConfiguration c7g.large us-east-1b"),
            failed("InvalidFleetConfiguration m7g.large us-east-1b"),
        ];
        assert_eq!(
            assess(std::slice::from_ref(&first), Some(&candidates), true),
            Assessment::NotBlocked
        );
        assert_eq!(
            assess(
                &[first.clone(), remaining[1].clone(), remaining[2].clone()],
                Some(&candidates),
                true
            ),
            Assessment::NotBlocked
        );
        assert_eq!(
            assess(
                &[
                    first,
                    remaining[0].clone(),
                    remaining[1].clone(),
                    remaining[2].clone()
                ],
                Some(&candidates),
                true
            ),
            Assessment::Blocked(reason)
        );
        for foreign in [
            with_details(
                "We currently do not have sufficient x9.large capacity in the Availability Zone you requested",
                r#"{"Availability Zone":"us-east-1a"}"#,
            ),
            with_details(reason, r#"{"Availability Zone":"us-east-1z"}"#),
            with_details(
                reason,
                r#"{"Availability Zone":"us-east-1a","Instance Type":"m7g.large"}"#,
            ),
            with_details(
                "An unknown launch failure occurred",
                r#"{"Availability Zone":"us-east-1a"}"#,
            ),
        ] {
            assert_eq!(
                assess(
                    &[
                        foreign,
                        remaining[0].clone(),
                        remaining[1].clone(),
                        remaining[2].clone()
                    ],
                    Some(&candidates),
                    true
                ),
                Assessment::NotBlocked
            );
        }
    }

    #[test]
    fn requested_zone_takes_precedence_over_capacity_suggestions() {
        let output = candidate_snapshot(false);
        let candidates = candidates(&output);
        let foreign = "We currently do not have sufficient c7g.large capacity in the Availability Zone you requested (us-east-1z). You can currently get c7g.large capacity by choosing us-east-1a.";
        let current = "We currently do not have sufficient c7g.large capacity in the Availability Zone you requested (us-east-1a). You can currently get c7g.large capacity by choosing us-east-1z.";
        let remaining = [
            failed("InvalidFleetConfiguration m7g.large us-east-1a"),
            failed("InvalidFleetConfiguration c7g.large us-east-1b"),
            failed("InvalidFleetConfiguration m7g.large us-east-1b"),
        ];
        for activity in [
            failed(foreign),
            with_details(foreign, r#"{"Availability Zone":"us-east-1a"}"#),
        ] {
            assert_eq!(
                assess(
                    &[
                        activity,
                        remaining[0].clone(),
                        remaining[1].clone(),
                        remaining[2].clone()
                    ],
                    Some(&candidates),
                    true
                ),
                Assessment::NotBlocked
            );
        }
        assert_eq!(
            assess(
                &[failed(current), remaining[1].clone(), remaining[2].clone()],
                Some(&candidates),
                true
            ),
            Assessment::NotBlocked
        );
        assert_eq!(
            assess(
                &[
                    failed(current),
                    remaining[0].clone(),
                    remaining[1].clone(),
                    remaining[2].clone()
                ],
                Some(&candidates),
                true
            ),
            Assessment::Blocked(current)
        );
    }

    #[test]
    fn documented_unsupported_type_and_zone_require_exact_current_pair() {
        let output = candidate_snapshot(false);
        let candidates = candidates(&output);
        let reason = "Your requested instance type (c7g.large) is not supported in your requested Availability Zone (us-east-1a).";
        let remaining = [
            failed("InvalidFleetConfiguration m7g.large us-east-1a"),
            failed("InvalidFleetConfiguration c7g.large us-east-1b"),
            failed("InvalidFleetConfiguration m7g.large us-east-1b"),
        ];
        assert_eq!(
            assess(
                &[
                    failed(reason),
                    remaining[0].clone(),
                    remaining[1].clone(),
                    remaining[2].clone()
                ],
                Some(&candidates),
                true
            ),
            Assessment::Blocked(reason)
        );
        for message in [
            "Your requested instance type (x9.large) is not supported in your requested Availability Zone (us-east-1a).",
            "Your requested instance type (c7g.large) is not supported in your requested Availability Zone (us-east-1z).",
        ] {
            assert_eq!(
                assess(
                    &[
                        failed(message),
                        remaining[0].clone(),
                        remaining[1].clone(),
                        remaining[2].clone()
                    ],
                    Some(&candidates),
                    true
                ),
                Assessment::NotBlocked
            );
        }
    }

    #[test]
    fn single_candidate_requires_current_attributed_evidence() {
        let output = candidate_snapshot(true);
        let candidates = candidates(&output);
        for failure in [
            with_details(
                "InsufficientInstanceCapacity",
                r#"{"Availability Zone":"us-east-1z"}"#,
            ),
            with_details(
                "InsufficientInstanceCapacity",
                r#"{"Availability Zone":"us-east-1a","Instance Type":"x9.large"}"#,
            ),
            failed("InsufficientInstanceCapacity"),
            failed("InsufficientInstanceCapacity us-east-1a x9.large"),
            failed("InsufficientInstanceCapacity us-east-1a m8i.metal-24xl"),
            failed("InsufficientInstanceCapacity us-east-1a m8i.metal-48xl"),
            failed("InsufficientInstanceCapacity us-east-1a-extra"),
            failed(UNSUPPORTED),
            failed("An unknown launch failure occurred"),
        ] {
            assert_eq!(
                assess(&[failure], Some(&candidates), true),
                Assessment::NotBlocked
            );
        }
        let message = "InsufficientInstanceCapacity c7g.large us-east-1a; exact reason.";
        assert_eq!(
            assess(&[failed(message)], Some(&candidates), true),
            Assessment::Blocked(message)
        );
        assert_eq!(
            assess(&[failed(message)], Some(&candidates), false),
            Assessment::Incomplete
        );
    }

    #[test]
    fn coverage_uses_newest_contributing_message_and_respects_history() {
        let output = candidate_snapshot(false);
        let candidates = candidates(&output);
        let reason = "InsufficientInstanceCapacity us-east-1a; exact reason.";
        let failures = [
            failed("InsufficientInstanceCapacity us-east-1z"),
            failed(reason),
            failed("UnfulfillableCapacity us-east-1b"),
        ];
        assert_eq!(
            assess(&failures, Some(&candidates), true),
            Assessment::Blocked(reason)
        );
        assert_eq!(
            assess(&failures, Some(&candidates), false),
            Assessment::Incomplete
        );
        assert_eq!(
            assess(&failures[..2], Some(&candidates), true),
            Assessment::NotBlocked
        );
        assert_eq!(
            assess(&failures[..2], Some(&candidates), false),
            Assessment::Incomplete
        );
        assert_eq!(assess(&failures, None, false), Assessment::NotBlocked);
        let stale_quota = [
            failed("InvalidFleetConfiguration c7g.large us-east-1a"),
            failed("VcpuLimitExceeded"),
        ];
        assert_eq!(
            assess(&stale_quota, Some(&candidates), true),
            Assessment::NotBlocked
        );
    }

    #[test]
    fn structured_details_support_empty_and_escaped_fields() {
        let output = candidate_snapshot(true);
        let candidates = candidates(&output);
        let fallback = "InvalidFleetConfiguration c7g.large us-east-1a";
        let empty = with_details(fallback, r#"{"Availability Zone":"","Instance Type":"  "}"#);
        assert_eq!(
            assess(&[empty], Some(&candidates), true),
            Assessment::Blocked(fallback)
        );
        let escaped = with_details(
            "InvalidFleetConfiguration: exact original message",
            r#"{"Availability Zone":"us-east-1\u0061","Instance Type":"c7g.\u006carge"}"#,
        );
        assert_eq!(
            assess(&[escaped], Some(&candidates), true),
            Assessment::Blocked("InvalidFleetConfiguration: exact original message")
        );
        let preferred = with_details(
            "InvalidFleetConfiguration c7g.large us-east-1a us-east-1b",
            r#"{"Availability Zone":"us-east-1a","Instance Type":"c7g.large"}"#,
        );
        assert_eq!(
            assess(&[preferred], Some(&candidates), true),
            Assessment::Blocked("InvalidFleetConfiguration c7g.large us-east-1a us-east-1b")
        );
    }

    #[test]
    fn exact_pair_coverage_and_ambiguity() {
        let output = candidate_snapshot(false);
        let candidates = candidates(&output);
        let failures = [
            failed("InvalidFleetConfiguration c7g.large us-east-1a"),
            failed("InvalidFleetConfiguration m7g.large us-east-1a"),
            failed("InvalidFleetConfiguration c7g.large us-east-1b"),
            failed("InvalidFleetConfiguration m7g.large us-east-1b"),
        ];
        assert_eq!(
            assess(&failures, Some(&candidates), true),
            Assessment::Blocked("InvalidFleetConfiguration c7g.large us-east-1a")
        );
        for message in [
            "InvalidFleetConfiguration c7g.large us-east-1a us-east-1b",
            "InvalidFleetConfiguration c7g.large-extra us-east-1a",
        ] {
            let partial = [
                failed(message),
                failures[1].clone(),
                failures[2].clone(),
                failures[3].clone(),
            ];
            assert_eq!(
                assess(&partial, Some(&candidates), true),
                Assessment::NotBlocked
            );
        }
    }

    #[test]
    fn progress_boundaries_and_in_flight_launches() {
        let output = candidate_snapshot(false);
        let candidates = candidates(&output);
        let a = || failed("InsufficientInstanceCapacity us-east-1a");
        let b = || failed("InsufficientInstanceCapacity us-east-1b");
        for status in ["Successful", "Cancelled"] {
            let boundary = activity(
                Some(ScalingActivityStatusCode::from(status)),
                Some(LAUNCH),
                None,
            );
            assert_eq!(
                assess(&[a(), boundary.clone(), b()], Some(&candidates), false),
                Assessment::NotBlocked
            );
            assert_eq!(
                assess(
                    &[failed("VcpuLimitExceeded"), boundary],
                    Some(&candidates),
                    true
                ),
                Assessment::Blocked("VcpuLimitExceeded")
            );
        }
        for active in [
            activity(
                Some(ScalingActivityStatusCode::InProgress),
                Some(LAUNCH),
                None,
            ),
            activity(
                Some(ScalingActivityStatusCode::from("WaitingForSpotInstanceId")),
                Some(LAUNCH),
                None,
            ),
            activity(None, None, None),
            activity(
                Some(ScalingActivityStatusCode::InProgress),
                Some("unknown"),
                None,
            ),
        ] {
            assert_eq!(
                assess(
                    &[failed("VcpuLimitExceeded"), active.clone()],
                    Some(&candidates),
                    true
                ),
                Assessment::NotBlocked
            );
            assert_eq!(
                assess(&[a(), b(), active], Some(&candidates), true),
                Assessment::NotBlocked
            );
        }
        let termination = activity(
            Some(ScalingActivityStatusCode::InProgress),
            Some("Terminating EC2 instance: i-123"),
            None,
        );
        assert_eq!(
            assess(
                &[failed("VcpuLimitExceeded"), termination],
                Some(&candidates),
                true
            ),
            Assessment::Blocked("VcpuLimitExceeded")
        );
    }

    #[test]
    fn global_codes_only_apply_to_newest_launch() {
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
            let activities = [failed(&message)];
            assert_eq!(
                assess(&activities, None, true),
                Assessment::Blocked(message.as_str())
            );
            assert_eq!(assess(&activities, None, false), Assessment::Incomplete);
        }
        for message in [
            "NotAccessDenied",
            "AccessDeniedExtra",
            "AccessDenied_Exception",
            "AccessDeniedException.Other",
            "InvalidAMIID.NotFoundExtra",
            "Other.InvalidAMIID.NotFound",
            "InstanceLimitExceeded2",
        ] {
            assert_eq!(
                assess(&[failed(message)], None, true),
                Assessment::NotBlocked
            );
        }
        for message in ["(AccessDeniedException): denied", "InvalidAMIID.NotFound."] {
            assert_eq!(
                assess(&[failed(message)], None, true),
                Assessment::Blocked(message)
            );
        }
    }

    #[test]
    fn requires_launch_and_nonempty_failed_message() {
        assert_eq!(assess(&[], None, true), Assessment::NotBlocked);
        assert_eq!(assess(&[], None, false), Assessment::Incomplete);
        for status in [
            Some(ScalingActivityStatusCode::Successful),
            Some(ScalingActivityStatusCode::InProgress),
            None,
        ] {
            assert_eq!(
                assess(
                    &[activity(status, Some(LAUNCH), Some("AccessDenied"))],
                    None,
                    true
                ),
                Assessment::NotBlocked
            );
        }
        for description in [None, Some("Terminating EC2 instance: i-123"), Some("")] {
            assert_eq!(
                assess(
                    &[activity(
                        Some(ScalingActivityStatusCode::Failed),
                        description,
                        Some("AccessDenied")
                    )],
                    None,
                    true
                ),
                Assessment::NotBlocked
            );
        }
        for message in [None, Some("")] {
            assert_eq!(
                assess(
                    &[activity(
                        Some(ScalingActivityStatusCode::Failed),
                        Some(LAUNCH),
                        message
                    )],
                    None,
                    false
                ),
                Assessment::NotBlocked
            );
        }
    }
}
