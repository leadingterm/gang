//! Warm-pool behaviour (ADR-22) through `Fleets` against the fake AWS account.

use std::error::Error;

use gang_spi::FleetSupply;
use gang_types::{InstanceId, InstanceLifecycle};

use crate::config::CooldownPolicy;
use crate::errors::AwsError;
use crate::fake_aws::{FakeFailure, FakeGroup, FakeInstance};
use crate::fleet_harness::{FLEET, Harness, fleet};

type TestResult = Result<(), Box<dyn Error>>;

const SET_PROTECTION: &str = "SetInstanceProtection";
const SET_DESIRED: &str = "SetDesiredCapacity";
const TERMINATE: &str = "TerminateInstanceInAutoScalingGroup";
const DESCRIBE_ACTIVITIES: &str = "DescribeScalingActivities";
const HONOR_COOLDOWN: &str = "HonorCooldown";
const THROTTLED: FakeFailure = FakeFailure {
    status: 400,
    code: "Throttling",
    message: "rate exceeded",
};

/// Parses an instance id for a test call.
fn id(raw: &str) -> Result<InstanceId, Box<dyn Error>> {
    Ok(InstanceId::try_new(raw)?)
}

/// A group that releases into its warm pool: members as `(id, state, protected)`, and new
/// instances protected from scale-in, as the Terraform-owned groups are.
fn reuse_group(desired: i32, members: &[(&str, &str, bool)], simulate: bool) -> FakeGroup {
    let mut group = FakeGroup::new(FLEET, desired, &[]);
    group.instances = members
        .iter()
        .map(|&(instance_id, state, protected)| FakeInstance {
            id: instance_id.to_owned(),
            lifecycle_state: state.to_owned(),
            protected,
        })
        .collect();
    group.reuse_on_scale_in = Some(true);
    group.new_instances_protected = true;
    group.simulate = simulate;
    group
}

/// The supply the adapter reports for the test fleet.
async fn supply_of(harness: &Harness) -> Result<FleetSupply, Box<dyn Error>> {
    let mut into = FleetSupply::default();
    let test_fleet = fleet(FLEET)?;
    harness.fleets.supply(test_fleet.view(), &mut into).await?;
    Ok(into)
}

/// The protection and capacity writes the fake received, in order.
fn write_order(harness: &Harness) -> Vec<String> {
    harness
        .request_log()
        .into_iter()
        .map(|(operation, _)| operation)
        .filter(|operation| matches!(operation.as_str(), SET_PROTECTION | SET_DESIRED))
        .collect()
}

/// The number of write requests of any kind the fake received.
fn write_count(harness: &Harness) -> usize {
    harness.requests(SET_PROTECTION) + harness.requests(SET_DESIRED) + harness.requests(TERMINATE)
}

#[tokio::test]
async fn plain_group_terminates_with_one_atomic_decrement() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group(
        FLEET,
        3,
        &[
            ("i-1", "InService"),
            ("i-2", "InService"),
            ("i-3", "InService"),
        ],
    );
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(harness.requests(TERMINATE), 1);
    assert_eq!(harness.requests(SET_PROTECTION), 0);
    assert_eq!(harness.requests(SET_DESIRED), 0);
    assert_eq!(harness.requests(DESCRIBE_ACTIVITIES), 0);
    assert_eq!(harness.desired(FLEET), Some(2));
    assert_eq!(harness.instance_ids(FLEET), ["i-2", "i-3"]);
    Ok(())
}

#[tokio::test]
async fn plain_group_supply_reports_the_larger_of_desired_and_live() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group(
        FLEET,
        2,
        &[
            ("i-1", "InService"),
            ("i-2", "Pending"),
            ("i-3", "InService"),
        ],
    );

    let supply = supply_of(&harness).await?;

    assert_eq!(supply.desired, 3);
    assert_eq!(supply.instances.len(), 3);
    assert_eq!(harness.requests(SET_PROTECTION), 0);
    assert_eq!(harness.requests(DESCRIBE_ACTIVITIES), 0);
    Ok(())
}

#[tokio::test]
async fn supply_reports_no_warm_instances_for_a_group_without_a_warm_pool() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group(FLEET, 1, &[("i-1", "InService")]);

    let supply = supply_of(&harness).await?;

    assert_eq!(supply.warm, 0);
    Ok(())
}

#[tokio::test]
async fn supply_reports_the_instances_parked_in_the_warm_pool() -> TestResult {
    let harness = Harness::start().await?;
    let mut group = reuse_group(1, &[("i-1", "InService", true)], false);
    group.warm_pool = vec!["i-w1".to_owned(), "i-w2".to_owned()];
    harness.add_group_with(group);

    let supply = supply_of(&harness).await?;

    assert_eq!(supply.warm, 2);
    assert_eq!(supply.desired, 1);
    assert_eq!(supply.instances.len(), 1);
    Ok(())
}

#[tokio::test]
async fn supply_reports_max_of_desired_and_live_while_released_member_remains() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        3,
        &[
            ("i-1", "InService", true),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        false,
    ));
    let test_fleet = fleet(FLEET)?;
    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    let supply = supply_of(&harness).await?;

    assert_eq!(harness.desired(FLEET), Some(2));
    assert_eq!(harness.instance_ids(FLEET), ["i-1", "i-2", "i-3"]);
    assert_eq!(supply.desired, 3);
    Ok(())
}

#[tokio::test]
async fn warm_instances_are_never_counted_as_capacity() -> TestResult {
    let harness = Harness::start().await?;
    let mut group = reuse_group(
        1,
        &[
            ("i-1", "InService", true),
            ("i-warming", "Warmed:Stopped", false),
        ],
        false,
    );
    group.warm_pool = vec!["i-pool".to_owned()];
    harness.add_group_with(group);

    let supply = supply_of(&harness).await?;

    assert_eq!(supply.desired, 1);
    assert!(
        supply
            .instances
            .iter()
            .all(|instance| instance.id.as_str() != "i-pool")
    );
    let counted = supply
        .instances
        .iter()
        .filter(|instance| {
            matches!(
                instance.lifecycle,
                InstanceLifecycle::Pending | InstanceLifecycle::InService
            )
        })
        .count();
    assert_eq!(counted, 1);
    Ok(())
}

#[tokio::test]
async fn release_unprotects_then_lowers_and_the_fake_moves_the_instance_to_the_pool() -> TestResult
{
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        3,
        &[
            ("i-1", "InService", true),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        true,
    ));
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(write_order(&harness), [SET_PROTECTION, SET_DESIRED]);
    assert_eq!(harness.desired(FLEET), Some(2));
    assert_eq!(harness.instance_ids(FLEET), ["i-2", "i-3"]);
    assert_eq!(harness.warm_ids(FLEET), ["i-1"]);
    assert_eq!(harness.is_protected(FLEET, "i-2"), Some(true));
    assert_eq!(harness.is_protected(FLEET, "i-3"), Some(true));
    Ok(())
}

#[tokio::test]
async fn releasing_the_same_instance_twice_lowers_capacity_once() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        3,
        &[
            ("i-1", "InService", true),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        true,
    ));
    let test_fleet = fleet(FLEET)?;
    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;
    let after_first = write_order(&harness);

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(write_order(&harness), after_first);
    assert_eq!(harness.desired(FLEET), Some(2));
    Ok(())
}

#[tokio::test]
async fn crash_after_unprotect_before_lowering_completes_with_one_lowering() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        3,
        &[
            ("i-1", "InService", true),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        false,
    ));
    let test_fleet = fleet(FLEET)?;
    harness.fail(SET_DESIRED, THROTTLED);

    let first = harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await;

    assert!(first.is_err());
    assert_eq!(harness.is_protected(FLEET, "i-1"), Some(false));
    assert_eq!(harness.desired(FLEET), Some(3));
    harness.clear_failure(SET_DESIRED);
    let lowerings_before_retry = harness.requests(SET_DESIRED);

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(harness.requests(SET_PROTECTION), 1);
    assert_eq!(harness.requests(SET_DESIRED) - lowerings_before_retry, 1);
    assert_eq!(harness.desired(FLEET), Some(2));
    Ok(())
}

#[tokio::test]
async fn crash_after_lowering_before_removal_writes_nothing_on_retry() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        2,
        &[
            ("i-1", "InService", false),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        false,
    ));
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(harness.requests(SET_PROTECTION), 0);
    assert_eq!(harness.requests(SET_DESIRED), 0);
    assert_eq!(harness.desired(FLEET), Some(2));
    Ok(())
}

#[tokio::test]
async fn second_release_lost_to_a_failed_lowering_completes_on_retry() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        3,
        &[
            ("i-1", "InService", true),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        true,
    ));
    let test_fleet = fleet(FLEET)?;
    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;
    harness.fail(SET_DESIRED, THROTTLED);

    let lost = harness
        .fleets
        .terminate(test_fleet.view(), &id("i-2")?)
        .await;

    assert!(lost.is_err());
    assert_eq!(harness.desired(FLEET), Some(2));
    harness.clear_failure(SET_DESIRED);

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-2")?)
        .await?;

    assert_eq!(harness.desired(FLEET), Some(1));
    assert_eq!(harness.instance_ids(FLEET), ["i-3"]);
    let mut warm = harness.warm_ids(FLEET);
    warm.sort();
    assert_eq!(warm, ["i-1", "i-2"]);
    Ok(())
}

#[tokio::test]
async fn in_flight_scale_out_is_kept_when_releasing_one_member() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        5,
        &[
            ("i-1", "InService", true),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
            ("i-4", "Pending", true),
        ],
        false,
    ));
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(harness.desired(FLEET), Some(4));
    Ok(())
}

#[tokio::test]
async fn reuse_without_protection_is_a_config_error_with_no_writes() -> TestResult {
    let harness = Harness::start().await?;
    let mut group = reuse_group(
        2,
        &[("i-1", "InService", false), ("i-2", "InService", false)],
        false,
    );
    group.new_instances_protected = false;
    harness.add_group_with(group);
    let test_fleet = fleet(FLEET)?;
    let mut into = FleetSupply::default();

    let supplied = harness.fleets.supply(test_fleet.view(), &mut into).await;
    let terminated = harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await;

    assert!(matches!(supplied, Err(AwsError::Config { .. })));
    assert!(matches!(terminated, Err(AwsError::Config { .. })));
    assert_eq!(write_count(&harness), 0);
    Ok(())
}

#[tokio::test]
async fn reuse_flag_false_terminates_atomically() -> TestResult {
    let harness = Harness::start().await?;
    let mut group = reuse_group(
        2,
        &[("i-1", "InService", false), ("i-2", "InService", false)],
        false,
    );
    group.reuse_on_scale_in = Some(false);
    harness.add_group_with(group);
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(harness.requests(TERMINATE), 1);
    assert_eq!(harness.requests(SET_PROTECTION), 0);
    assert_eq!(harness.requests(SET_DESIRED), 0);
    assert_eq!(harness.desired(FLEET), Some(1));
    Ok(())
}

#[tokio::test]
async fn pending_member_terminates_atomically_even_in_a_reuse_group() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        3,
        &[
            ("i-1", "Pending", true),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        false,
    ));
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(harness.requests(TERMINATE), 1);
    assert_eq!(harness.requests(SET_PROTECTION), 0);
    assert_eq!(harness.requests(SET_DESIRED), 0);
    assert_eq!(harness.instance_ids(FLEET), ["i-2", "i-3"]);
    assert_eq!(harness.desired(FLEET), Some(2));
    Ok(())
}

#[tokio::test]
async fn warmed_member_is_already_leaving_so_terminate_writes_nothing() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        2,
        &[("i-1", "Warmed:Pending", false), ("i-2", "InService", true)],
        false,
    ));
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    assert_eq!(write_count(&harness), 0);
    assert_eq!(harness.desired(FLEET), Some(2));
    Ok(())
}

#[tokio::test]
async fn warm_start_is_counted_as_capacity_after_a_raise_draws_the_pool_first() -> TestResult {
    let harness = Harness::start().await?;
    let mut group = reuse_group(
        2,
        &[("i-1", "InService", true), ("i-2", "InService", true)],
        true,
    );
    group.warm_pool = vec!["i-warm".to_owned()];
    harness.add_group_with(group);
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .set_desired_capacity(test_fleet.view(), 3)
        .await?;

    assert_eq!(harness.desired(FLEET), Some(3));
    assert!(harness.warm_ids(FLEET).is_empty());
    assert_eq!(harness.instance_ids(FLEET), ["i-1", "i-2", "i-warm"]);
    let supply = supply_of(&harness).await?;
    assert_eq!(supply.desired, 3);
    assert!(supply.instances.iter().any(|instance| {
        instance.id.as_str() == "i-warm" && instance.lifecycle == InstanceLifecycle::Pending
    }));

    harness.fake.state().promote(FLEET, "i-warm");

    assert_eq!(harness.is_protected(FLEET, "i-warm"), Some(true));
    let promoted = supply_of(&harness).await?;
    assert_eq!(promoted.desired, 3);
    assert!(promoted.instances.iter().any(|instance| {
        instance.id.as_str() == "i-warm" && instance.lifecycle == InstanceLifecycle::InService
    }));
    Ok(())
}

#[tokio::test]
async fn honored_cooldown_applies_to_raises_and_release_lowerings() -> TestResult {
    let harness = Harness::start_with(CooldownPolicy::Honor).await?;
    harness.add_group_with(reuse_group(
        2,
        &[("i-1", "InService", true), ("i-2", "InService", true)],
        false,
    ));
    let test_fleet = fleet(FLEET)?;
    harness
        .fleets
        .set_desired_capacity(test_fleet.view(), 3)
        .await?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-1")?)
        .await?;

    let writes: Vec<_> = harness
        .request_log()
        .into_iter()
        .filter(|(operation, _)| operation.as_str() == SET_DESIRED)
        .collect();
    assert_eq!(writes.len(), 2);
    assert!(writes.iter().all(|(_, parameters)| {
        parameters.get(HONOR_COOLDOWN).map(String::as_str) == Some("true")
    }));
    Ok(())
}

#[tokio::test]
async fn release_is_refused_while_a_pre_existing_unprotected_member_would_enter_the_pool()
-> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        3,
        &[
            ("i-0", "InService", false),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        true,
    ));
    let test_fleet = fleet(FLEET)?;

    let result = harness
        .fleets
        .terminate(test_fleet.view(), &id("i-2")?)
        .await;

    assert!(matches!(
        result,
        Err(AwsError::Config {
            field: "scale_in_protection",
            ..
        })
    ));
    assert_eq!(harness.requests(SET_PROTECTION), 0);
    assert_eq!(harness.requests(SET_DESIRED), 0);
    assert_eq!(write_count(&harness), 0);
    Ok(())
}

#[tokio::test]
async fn release_proceeds_once_desired_is_already_lowered_below_live() -> TestResult {
    let harness = Harness::start().await?;
    harness.add_group_with(reuse_group(
        2,
        &[
            ("i-0", "InService", false),
            ("i-2", "InService", true),
            ("i-3", "InService", true),
        ],
        false,
    ));
    let test_fleet = fleet(FLEET)?;

    harness
        .fleets
        .terminate(test_fleet.view(), &id("i-2")?)
        .await?;

    assert_eq!(harness.requests(SET_PROTECTION), 1);
    assert_eq!(harness.requests(SET_DESIRED), 1);
    assert!(harness.desired(FLEET).is_some_and(|desired| desired < 2));
    Ok(())
}
