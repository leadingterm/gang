//! Adapter scenarios of the ADR-23 on-demand fallback, run through [`Fleets`] against the fake
//! AWS. Each test seeds the spot group and its on-demand twin, runs one call, and checks the
//! writes and reads the fake recorded.

use std::error::Error;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_spi::FleetSupply;
use gang_types::InstanceId;

use crate::config::{AwsFleetOptions, Capacity, RetrySpotAfter, RetrySpotAfterError, SpotFallback};
use crate::errors::AwsError;
use crate::fake_aws::{FakeFailure, FakeGroup, FakeInstance};
use crate::fleet_harness::{FLEET, Harness, fleet_with, rfc3339_ago};

type TestResult = Result<(), Box<dyn Error>>;

/// Name of the on-demand twin of [`FLEET`], `<Fleet::name>-on-demand`.
const TWIN: &str = "gang-fleet-on-demand";
/// The `retry_spot_after` the opted-in fleets run with.
const RETRY_SPOT_AFTER: Duration = Duration::from_mins(5);
/// Launch time seeded for the spot members that have one.
const SPOT_LAUNCH: &str = "2026-01-01T00:00:00.000Z";
/// Unix milliseconds of [`SPOT_LAUNCH`].
const SPOT_LAUNCH_MILLIS: u64 = 1_767_225_600_000;
/// Launch time seeded for the on-demand members that have one, one day after [`SPOT_LAUNCH`].
const ON_DEMAND_LAUNCH: &str = "2026-01-02T00:00:00.000Z";
/// Unix milliseconds of [`ON_DEMAND_LAUNCH`].
const ON_DEMAND_LAUNCH_MILLIS: u64 = 1_767_312_000_000;

/// Fleet options that opt a spot fleet into the fallback.
fn fallback_options(drain_on_demand_first: bool) -> Result<AwsFleetOptions, RetrySpotAfterError> {
    Ok(AwsFleetOptions {
        capacity: Capacity::Spot,
        machine_class: None,
        fallback: Some(SpotFallback {
            retry_spot_after: RetrySpotAfter::try_new(RETRY_SPOT_AFTER)?,
            drain_on_demand_first,
        }),
    })
}

/// Fleet options of a plain spot fleet, without the fallback.
fn plain_options() -> AwsFleetOptions {
    AwsFleetOptions {
        capacity: Capacity::Spot,
        machine_class: None,
        fallback: None,
    }
}

/// A group with scale-in protection on new instances, as an opted-in fleet requires.
fn protected(name: &str, desired: i32, instances: &[(&str, &str)]) -> FakeGroup {
    FakeGroup {
        new_instances_protected: true,
        ..FakeGroup::new(name, desired, instances)
    }
}

/// The on-demand twin with a warm pool that reuses instances on scale-in. Its members are
/// protected, and the fake scales them when desired capacity is set.
fn warm_twin(desired: i32, instances: &[(&str, &str)]) -> FakeGroup {
    let mut group = protected(TWIN, desired, instances);
    group.reuse_on_scale_in = Some(true);
    group.simulate = true;
    for member in &mut group.instances {
        member.protected = true;
    }
    group
}

/// A harness whose fake holds exactly these groups.
async fn harness_with(groups: Vec<FakeGroup>) -> Result<Harness, Box<dyn Error>> {
    let harness = Harness::start().await?;
    for group in groups {
        harness.add_group_with(group);
    }
    Ok(harness)
}

/// The `(group, DesiredCapacity)` of every `SetDesiredCapacity` the fake received, in order.
fn writes(harness: &Harness) -> Vec<(String, String)> {
    harness
        .request_log()
        .into_iter()
        .filter(|(operation, _)| operation == "SetDesiredCapacity")
        .map(|(_, parameters)| {
            (
                parameters
                    .get("AutoScalingGroupName")
                    .cloned()
                    .unwrap_or_default(),
                parameters
                    .get("DesiredCapacity")
                    .cloned()
                    .unwrap_or_default(),
            )
        })
        .collect()
}

/// The `(group, DesiredCapacity)` pairs as string literals, for comparison.
fn write(group: &str, desired: &str) -> (String, String) {
    (group.to_owned(), desired.to_owned())
}

/// A throttling failure, as the Auto Scaling API answers it.
fn throttled() -> FakeFailure {
    FakeFailure {
        status: 400,
        code: "Throttling",
        message: "Rate exceeded",
    }
}

/// Plain spot fleets never ask for scaling activities and never touch the on-demand twin.
#[tokio::test]
async fn plain_spot_fleet_never_reads_activities_or_twin() -> TestResult {
    let harness = harness_with(vec![FakeGroup::new(
        FLEET,
        6,
        &[
            ("i-spot-1", "InService"),
            ("i-spot-2", "InService"),
            ("i-spot-3", "InService"),
            ("i-spot-4", "InService"),
            ("i-spot-5", "InService"),
            ("i-spot-6", "InService"),
        ],
    )])
    .await?;
    let fleet = fleet_with(FLEET, plain_options())?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;
    harness.fleets.set_desired_capacity(fleet.view(), 8).await?;

    assert_eq!(harness.requests("DescribeScalingActivities"), 0);
    assert!(harness.request_log().iter().all(|(_, parameters)| {
        parameters
            .values()
            .all(|value| !value.contains("-on-demand"))
    }));
    assert_eq!(writes(&harness), vec![write(FLEET, "8")]);
    Ok(())
}

/// With Spot healthy, a raise goes to the spot group only, and the next supply reports the raised
/// capacity after one scaling activities call that finds nothing.
#[tokio::test]
async fn healthy_spot_raise_goes_to_spot_only() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;
    assert_eq!(writes(&harness), vec![write(FLEET, "10")]);
    assert_eq!(harness.desired(TWIN), Some(0));

    let activities_before = harness.requests("DescribeScalingActivities");
    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 10);
    assert_eq!(
        harness.requests("DescribeScalingActivities") - activities_before,
        1
    );
    Ok(())
}

/// The first detection of a failed Spot launch withdraws the unmet Spot request, then raises the
/// on-demand twin by the gap, in that order; the following supply reports the raised capacity.
#[tokio::test]
async fn first_failure_withdraws_spot_then_raises_on_demand() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            10,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(30));
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 6);

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;
    assert_eq!(writes(&harness), vec![write(FLEET, "6"), write(TWIN, "4")]);

    let activities_before = harness.requests("DescribeScalingActivities");
    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 10);
    assert_eq!(
        harness.requests("DescribeScalingActivities") - activities_before,
        0
    );
    Ok(())
}

/// A restart before any failure is recorded reports the booting Spot capacity and writes nothing.
#[tokio::test]
async fn restart_before_any_failure_reports_booting_capacity() -> TestResult {
    let mut harness = harness_with(vec![
        protected(
            FLEET,
            10,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.restart();
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;

    assert_eq!(into.desired, 10);
    assert!(writes(&harness).is_empty());
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(30));
    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;
    assert_eq!(writes(&harness), vec![write(FLEET, "6"), write(TWIN, "4")]);
    Ok(())
}

/// A restart after the withdraw, with the failure still in the window, sends the next raise
/// straight to the on-demand twin, with no Spot write.
#[tokio::test]
async fn restart_after_withdraw_raises_on_demand_without_a_spot_write() -> TestResult {
    let mut harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(30));
    harness.restart();
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 6);

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;
    assert_eq!(writes(&harness), vec![write(TWIN, "4")]);
    assert!(writes(&harness).iter().all(|(group, _)| group != FLEET));
    Ok(())
}

/// A restart after both writes reports the full capacity and repeats no write.
#[tokio::test]
async fn restart_after_both_writes_repeats_nothing() -> TestResult {
    let mut harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(
            TWIN,
            4,
            &[
                ("i-od-1", "InService"),
                ("i-od-2", "InService"),
                ("i-od-3", "InService"),
                ("i-od-4", "InService"),
            ],
        ),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(30));
    harness.restart();
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 10);

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;
    assert!(writes(&harness).is_empty());
    Ok(())
}

/// One second inside the retry window, growth still goes to on-demand.
#[tokio::test]
async fn growth_one_second_inside_retry_window_goes_on_demand() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(0));
    harness
        .clock
        .advance(RETRY_SPOT_AFTER.saturating_sub(Duration::from_secs(1)));
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;

    assert_eq!(writes(&harness), vec![write(TWIN, "4")]);
    Ok(())
}

/// One second past the retry window, growth goes back to Spot.
#[tokio::test]
async fn growth_one_second_past_retry_window_goes_to_spot() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(0));
    harness
        .clock
        .advance(RETRY_SPOT_AFTER + Duration::from_secs(1));
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;

    assert_eq!(writes(&harness), vec![write(FLEET, "10")]);
    assert_eq!(harness.desired(TWIN), Some(0));
    Ok(())
}

/// Once Spot recovers and its instances appear, growth stays on Spot and no old failure re-arms
/// the fallback.
#[tokio::test]
async fn recovered_spot_keeps_growing_on_spot() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(0));
    harness
        .clock
        .advance(RETRY_SPOT_AFTER + Duration::from_secs(1));
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;

    {
        let mut state = harness.fake.state();
        let group = state
            .groups
            .iter_mut()
            .find(|group| group.name == FLEET)
            .ok_or("the spot group is seeded")?;
        for n in 0..4 {
            group.instances.push(FakeInstance {
                id: format!("i-spot-new-{n}"),
                lifecycle_state: "InService".to_owned(),
                protected: false,
            });
        }
    }
    let mut into = FleetSupply::default();
    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 10);

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 12)
        .await?;
    assert_eq!(
        writes(&harness),
        vec![write(FLEET, "10"), write(FLEET, "12")]
    );
    assert_eq!(harness.desired(TWIN), Some(0));
    Ok(())
}

/// A Spot member that leaves, with its replacement failing, falls back on the next raise.
#[tokio::test]
async fn interrupted_spot_with_failed_replacement_falls_back() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(30));
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 5);

    harness.fleets.set_desired_capacity(fleet.view(), 6).await?;
    assert_eq!(writes(&harness), vec![write(FLEET, "5"), write(TWIN, "1")]);
    Ok(())
}

/// A failed launch older than the retry window no longer marks the gap as unmet: it counts as
/// booting capacity, and the raise goes to Spot.
#[tokio::test]
async fn failure_older_than_window_counts_gap_as_booting() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(600));
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;
    assert_eq!(into.desired, 6);

    harness.fleets.set_desired_capacity(fleet.view(), 7).await?;
    assert_eq!(writes(&harness), vec![write(FLEET, "7")]);
    Ok(())
}

/// Terminating an on-demand member decrements the on-demand twin and leaves the spot group alone.
#[tokio::test]
async fn terminate_on_demand_member_decrements_only_the_twin() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(
            TWIN,
            4,
            &[
                ("i-od-1", "InService"),
                ("i-od-2", "InService"),
                ("i-od-3", "InService"),
                ("i-od-4", "InService"),
            ],
        ),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    harness
        .fleets
        .terminate(fleet.view(), &InstanceId::try_new("i-od-1")?)
        .await?;

    assert_eq!(harness.desired(TWIN), Some(3));
    assert_eq!(harness.desired(FLEET), Some(6));
    Ok(())
}

/// Terminating a spot member decrements the spot group alone.
#[tokio::test]
async fn terminate_spot_member_decrements_only_spot() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(
            TWIN,
            4,
            &[
                ("i-od-1", "InService"),
                ("i-od-2", "InService"),
                ("i-od-3", "InService"),
                ("i-od-4", "InService"),
            ],
        ),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    harness
        .fleets
        .terminate(fleet.view(), &InstanceId::try_new("i-spot-1")?)
        .await?;

    assert_eq!(harness.desired(FLEET), Some(5));
    assert_eq!(harness.desired(TWIN), Some(4));
    Ok(())
}

#[tokio::test]
async fn terminate_warm_twin_member_releases_into_pool() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        warm_twin(
            3,
            &[
                ("i-od-1", "InService"),
                ("i-od-2", "InService"),
                ("i-od-3", "InService"),
            ],
        ),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    harness
        .fleets
        .terminate(fleet.view(), &InstanceId::try_new("i-od-1")?)
        .await?;

    let mutations: Vec<String> = harness
        .request_log()
        .into_iter()
        .map(|(operation, _)| operation)
        .filter(|operation| {
            matches!(
                operation.as_str(),
                "SetInstanceProtection"
                    | "SetDesiredCapacity"
                    | "TerminateInstanceInAutoScalingGroup"
            )
        })
        .collect();
    assert_eq!(
        mutations,
        vec![
            "SetInstanceProtection".to_owned(),
            "SetDesiredCapacity".to_owned()
        ]
    );
    assert!(harness.request_log().iter().any(|(operation, parameters)| {
        operation == "SetInstanceProtection"
            && parameters.get("AutoScalingGroupName").map(String::as_str) == Some(TWIN)
    }));
    assert_eq!(harness.desired(TWIN), Some(2));
    assert_eq!(harness.warm_ids(TWIN), vec!["i-od-1".to_owned()]);
    assert_eq!(
        harness.instance_ids(TWIN),
        vec!["i-od-2".to_owned(), "i-od-3".to_owned()]
    );
    Ok(())
}

/// Terminating a spot member beside a warm-pool twin terminates it once and touches no protection.
#[tokio::test]
async fn terminate_spot_member_beside_warm_twin_skips_protection() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        warm_twin(
            3,
            &[
                ("i-od-1", "InService"),
                ("i-od-2", "InService"),
                ("i-od-3", "InService"),
            ],
        ),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    harness
        .fleets
        .terminate(fleet.view(), &InstanceId::try_new("i-spot-1")?)
        .await?;

    assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 1);
    assert_eq!(harness.requests("SetInstanceProtection"), 0);
    assert!(harness.request_log().iter().any(|(operation, parameters)| {
        operation == "TerminateInstanceInAutoScalingGroup"
            && parameters.get("InstanceId").map(String::as_str) == Some("i-spot-1")
    }));
    assert_eq!(harness.desired(FLEET), Some(5));
    assert_eq!(harness.desired(TWIN), Some(3));
    Ok(())
}

/// An opted-in fleet without its on-demand twin is refused, and nothing is written.
#[tokio::test]
async fn opt_in_without_twin_group_is_missing_group() -> TestResult {
    let harness = harness_with(vec![protected(FLEET, 6, &[("i-spot-1", "InService")])]).await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    let error = harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await
        .err()
        .ok_or("the raise must be refused")?;

    assert!(matches!(error, AwsError::MissingGroup(_)));
    assert!(writes(&harness).is_empty());
    Ok(())
}

/// An opted-in fleet whose spot group lacks scale-in protection is refused, and nothing is
/// written.
#[tokio::test]
async fn opt_in_with_unprotected_spot_group_is_config_error() -> TestResult {
    let harness = harness_with(vec![
        FakeGroup::new(FLEET, 6, &[("i-spot-1", "InService")]),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    let error = harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await
        .err()
        .ok_or("the raise must be refused")?;

    assert!(matches!(error, AwsError::Config { .. }));
    assert!(writes(&harness).is_empty());
    Ok(())
}

/// An opted-in fleet whose on-demand twin lacks scale-in protection is refused, and nothing is
/// written.
#[tokio::test]
async fn opt_in_with_unprotected_twin_group_is_config_error() -> TestResult {
    let harness = harness_with(vec![
        protected(FLEET, 6, &[("i-spot-1", "InService")]),
        FakeGroup::new(TWIN, 0, &[]),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    let error = harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await
        .err()
        .ok_or("the raise must be refused")?;

    assert!(matches!(error, AwsError::Config { .. }));
    assert!(writes(&harness).is_empty());
    Ok(())
}

/// A throttled scaling activities call makes the supply a transient error.
#[tokio::test]
async fn throttled_activities_make_supply_transient() -> TestResult {
    let harness = harness_with(vec![
        protected(FLEET, 10, &[("i-spot-1", "InService")]),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.fail("DescribeScalingActivities", throttled());
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    let error = harness
        .fleets
        .supply(fleet.view(), &mut into)
        .await
        .err()
        .ok_or("the supply must fail")?;

    assert!(matches!(error, AwsError::Transient { .. }));
    Ok(())
}

/// A throttled scaling activities call makes a raise a transient error, with no writes.
#[tokio::test]
async fn throttled_activities_make_raise_transient_and_write_nothing() -> TestResult {
    let harness = harness_with(vec![
        protected(FLEET, 10, &[("i-spot-1", "InService")]),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.fail("DescribeScalingActivities", throttled());
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;

    let error = harness
        .fleets
        .set_desired_capacity(fleet.view(), 12)
        .await
        .err()
        .ok_or("the raise must fail")?;

    assert!(matches!(error, AwsError::Transient { .. }));
    assert!(writes(&harness).is_empty());
    Ok(())
}

/// With `drain_on_demand_first` off, no instance is released first.
#[tokio::test]
async fn drain_on_demand_first_off_releases_nothing_first() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            2,
            &[("i-spot-1", "InService"), ("i-spot-2", "InService")],
        ),
        protected(TWIN, 2, &[("i-od-1", "InService"), ("i-od-2", "InService")]),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(false)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;

    assert_eq!(into.instances.len(), 4);
    assert!(
        into.instances
            .iter()
            .all(|instance| !instance.release_first)
    );
    Ok(())
}

/// With `drain_on_demand_first` on, on-demand members are released first and spot members are
/// not; the instances are listed spot first, then on-demand.
#[tokio::test]
async fn on_demand_members_release_first_and_spot_listed_first() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            2,
            &[("i-spot-1", "InService"), ("i-spot-2", "InService")],
        ),
        protected(TWIN, 2, &[("i-od-1", "InService"), ("i-od-2", "InService")]),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;

    let order: Vec<(&str, bool)> = into
        .instances
        .iter()
        .map(|instance| (instance.id.as_str(), instance.release_first))
        .collect();
    assert_eq!(
        order,
        vec![
            ("i-spot-1", false),
            ("i-spot-2", false),
            ("i-od-1", true),
            ("i-od-2", true),
        ]
    );
    Ok(())
}

/// Launch times come from one EC2 read that covers both groups.
#[tokio::test]
async fn launch_times_cover_both_groups() -> TestResult {
    let harness = harness_with(vec![
        protected(FLEET, 1, &[("i-spot-1", "InService")]),
        protected(TWIN, 1, &[("i-od-1", "InService")]),
    ])
    .await?;
    harness.add_launch("i-spot-1", SPOT_LAUNCH);
    harness.add_launch("i-od-1", ON_DEMAND_LAUNCH);
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness.fleets.supply(fleet.view(), &mut into).await?;

    let launches: Vec<(&str, Timestamp)> = into
        .instances
        .iter()
        .map(|instance| (instance.id.as_str(), instance.launched_at))
        .collect();
    assert_eq!(
        launches,
        vec![
            ("i-spot-1", Timestamp::from_unix_millis(SPOT_LAUNCH_MILLIS)),
            (
                "i-od-1",
                Timestamp::from_unix_millis(ON_DEMAND_LAUNCH_MILLIS)
            ),
        ]
    );
    Ok(())
}

/// After a fallback raise returns, the next supply reports at least the requested capacity.
#[tokio::test]
async fn supply_after_fallback_raise_reports_at_least_the_target() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            10,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    harness.add_activity(FLEET, "Failed", &rfc3339_ago(30));
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;
    harness.fleets.supply(fleet.view(), &mut into).await?;

    assert!(into.desired >= 10);
    Ok(())
}

/// After a Spot raise returns, the next supply reports at least the requested capacity.
#[tokio::test]
async fn supply_after_spot_raise_reports_at_least_the_target() -> TestResult {
    let harness = harness_with(vec![
        protected(
            FLEET,
            6,
            &[
                ("i-spot-1", "InService"),
                ("i-spot-2", "InService"),
                ("i-spot-3", "InService"),
                ("i-spot-4", "InService"),
                ("i-spot-5", "InService"),
                ("i-spot-6", "InService"),
            ],
        ),
        protected(TWIN, 0, &[]),
    ])
    .await?;
    let fleet = fleet_with(FLEET, fallback_options(true)?)?;
    let mut into = FleetSupply::default();

    harness
        .fleets
        .set_desired_capacity(fleet.view(), 10)
        .await?;
    harness.fleets.supply(fleet.view(), &mut into).await?;

    assert!(into.desired >= 10);
    Ok(())
}
