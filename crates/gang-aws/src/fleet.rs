//! Auto scaling groups as fleets: their size, their instances, and how they grow and shrink.
//!
//! Each fleet is one Auto Scaling group named `Fleet::name`. A fleet that opts into spot fallback
//! also owns its on-demand twin, the group named `Fleet::name` followed by `-on-demand`. The
//! groups' capacity and instance types are their own configuration, so the adapter reads no
//! fleet options beyond the fallback choice.

use std::collections::HashMap;
use std::sync::Arc;

use aws_config::SdkConfig;
use aws_sdk_autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput;
use aws_sdk_autoscaling::types::Filter as ScalingFilter;
use aws_sdk_ec2::types::Filter;
use aws_sdk_ec2::types::builders::FilterBuilder;
use gang_clock::{Clock, Timestamp};
use gang_spi::{FleetInstance, FleetSupply};
use gang_types::{Fleet, InstanceId, InstanceLifecycle};

use crate::config::{AwsFleetOptions, CooldownPolicy, ON_DEMAND_SUFFIX, SpotFallback};
use crate::errors::{AwsError, sdk_error};
use crate::fallback::{self, Counts, Tier};
use crate::group::{DESCRIBE_GROUPS, Group, Member};

const SET_DESIRED_CAPACITY: &str = "SetDesiredCapacity";
const SET_INSTANCE_PROTECTION: &str = "SetInstanceProtection";
const TERMINATE_INSTANCE: &str = "TerminateInstanceInAutoScalingGroup";
const DESCRIBE_INSTANCES: &str = "DescribeInstances";
const DESCRIBE_SCALING_ACTIVITIES: &str = "DescribeScalingActivities";

/// EC2 tag that auto scaling sets to the name of the group an instance belongs to.
const GROUP_TAG_FILTER: &str = "tag:aws:autoscaling:groupName";
/// Activity filter that selects activities by status.
const STATUS_FILTER: &str = "Status";
/// Activity filter that selects activities that started at or after a time.
const START_TIME_FILTER: &str = "StartTimeLowerBound";
/// Status of a scaling activity whose launch failed.
const FAILED_STATUS: &str = "Failed";
/// One record answers whether any failed activity exists.
const ACTIVITY_PROBE_LIMIT: i32 = 1;

/// The auto scaling groups and EC2 instances behind the fleets of one AWS account and region.
///
/// Each fleet is one group named `Fleet::name`, plus the on-demand twin for an opted-in fleet.
pub(crate) struct Fleets {
    autoscaling: aws_sdk_autoscaling::Client,
    ec2: aws_sdk_ec2::Client,
    clock: Arc<dyn Clock>,
    /// Applied to every desired-capacity write: raises, spot withdrawals and release lowerings.
    cooldown: CooldownPolicy,
}

impl Fleets {
    /// Creates the fleet adapter from the shared SDK config; `clock` stamps instances EC2 does
    /// not know about, and `cooldown` decides whether every desired-capacity write (raises, spot
    /// withdrawals and release lowerings alike) honors the group's cooldown.
    pub(crate) fn new(sdk: &SdkConfig, clock: Arc<dyn Clock>, cooldown: CooldownPolicy) -> Self {
        Self {
            autoscaling: aws_sdk_autoscaling::Client::new(sdk),
            ec2: aws_sdk_ec2::Client::new(sdk),
            clock,
            cooldown,
        }
    }

    /// Overwrites `into` with the fleet's desired capacity and its instances; an opted-in fleet
    /// merges its spot group and its twin, spot first.
    pub(crate) async fn supply(
        &self,
        fleet: Fleet<'_, AwsFleetOptions>,
        into: &mut FleetSupply,
    ) -> Result<(), AwsError> {
        let names = GroupNames::of(fleet);
        let output = self.describe_groups(names.iter()).await?;
        let groups = FleetGroups::read(&output, &names)?;
        let desired = match &groups {
            FleetGroups::Plain(group) => group.reported(),
            FleetGroups::Fallback { spot, twin, policy } => {
                let down =
                    Counts::from(spot).gap() > 0 && self.spot_down(spot, *policy).await?.is_some();
                fallback::reported(Counts::from(spot), Counts::from(twin), down)
            }
        };
        let member_count: usize = groups.groups().map(|group| group.members.len()).sum();
        let launched = if member_count == 0 {
            HashMap::new()
        } else {
            self.launch_times(names.iter()).await?
        };
        let now = self.clock.wall();
        into.desired = desired;
        into.warm = groups
            .groups()
            .fold(0_u32, |warm, group| warm.saturating_add(group.warm));
        into.instances.clear();
        into.exits.clear();
        into.instances.reserve(member_count);
        match &groups {
            FleetGroups::Plain(group) => into.instances.extend(
                group
                    .members
                    .iter()
                    .map(|member| fleet_instance(member, false, &launched, now)),
            ),
            FleetGroups::Fallback { spot, twin, policy } => {
                into.instances.extend(
                    spot.members
                        .iter()
                        .map(|member| fleet_instance(member, false, &launched, now)),
                );
                into.instances.extend(twin.members.iter().map(|member| {
                    fleet_instance(member, policy.drain_on_demand_first, &launched, now)
                }));
            }
        }
        Ok(())
    }

    /// Raises the fleet's capacity to `desired`; never lowers it. An opted-in fleet raises its
    /// spot group and its on-demand twin in the order the fallback rule gives.
    pub(crate) async fn set_desired_capacity(
        &self,
        fleet: Fleet<'_, AwsFleetOptions>,
        desired: u32,
    ) -> Result<(), AwsError> {
        let names = GroupNames::of(fleet);
        let output = self.describe_groups(names.iter()).await?;
        match FleetGroups::read(&output, &names)? {
            FleetGroups::Plain(group) => {
                if group.reported() < desired {
                    self.set_capacity(&group.name, desired).await?;
                }
                Ok(())
            }
            FleetGroups::Fallback { spot, twin, policy } => {
                let failed = self.spot_down(&spot, policy).await?;
                let down = failed.is_some();
                for write in
                    fallback::raise(Counts::from(&spot), Counts::from(&twin), down, desired)
                {
                    let name = match write.tier {
                        Tier::Spot => &spot.name,
                        Tier::OnDemand => &twin.name,
                    };
                    if down {
                        tracing::info!(
                            fleet = %fleet.name,
                            group = %name,
                            desired = write.desired,
                            status_message = failed.as_ref().and_then(|failed| failed.status_message.as_deref()),
                            "spot fallback: writing desired capacity"
                        );
                    }
                    self.set_capacity(name, write.desired).await?;
                }
                Ok(())
            }
        }
    }

    /// Removes one instance from the fleet. An instance that is in service in a warm-pool group
    /// is released into the pool; any other instance is terminated and its group's desired
    /// capacity lowered with it.
    pub(crate) async fn terminate(
        &self,
        fleet: Fleet<'_, AwsFleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), AwsError> {
        let names = GroupNames::of(fleet);
        let output = self.describe_groups(names.iter()).await?;
        let groups = FleetGroups::read(&output, &names)?;
        let Some((group, member)) = groups
            .groups()
            .find_map(|group| group.member(instance).map(|member| (group, member)))
        else {
            return Ok(());
        };
        if member.lifecycle == InstanceLifecycle::Terminating {
            return Ok(());
        }
        if member.lifecycle == InstanceLifecycle::InService && group.reuse_on_scale_in {
            return self.release(group, member, instance).await;
        }
        self.autoscaling
            .terminate_instance_in_auto_scaling_group()
            .instance_id(instance.as_str())
            .should_decrement_desired_capacity(true)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| sdk_error(TERMINATE_INSTANCE, e))
    }

    /// Releases an in-service instance of a warm-pool group. The release target is computed
    /// first, so a refusal leaves the group untouched; then the instance's scale-in protection
    /// is lifted and the group's desired capacity lowered to the target.
    async fn release(
        &self,
        group: &Group,
        member: &Member,
        instance: &InstanceId,
    ) -> Result<(), AwsError> {
        let target = group.release_target(instance)?;
        if member.protected {
            self.set_protection(&group.name, instance, false).await?;
        }
        if target < group.desired {
            self.set_capacity(&group.name, target).await?;
        }
        Ok(())
    }

    /// Writes one group's desired capacity, honoring the cooldown policy. Every desired-capacity
    /// write goes through here.
    async fn set_capacity(&self, group: &str, desired: u32) -> Result<(), AwsError> {
        let wanted = i32::try_from(desired).map_err(|_| AwsError::Config {
            field: "desired",
            reason: "exceeds the largest capacity an auto scaling group accepts".to_owned(),
        })?;
        self.autoscaling
            .set_desired_capacity()
            .auto_scaling_group_name(group)
            .desired_capacity(wanted)
            .honor_cooldown(self.cooldown.honors())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| sdk_error(SET_DESIRED_CAPACITY, e))
    }

    /// Sets one instance's scale-in protection within its group.
    async fn set_protection(
        &self,
        group: &str,
        instance: &InstanceId,
        protected: bool,
    ) -> Result<(), AwsError> {
        self.autoscaling
            .set_instance_protection()
            .auto_scaling_group_name(group)
            .instance_ids(instance.as_str())
            .protected_from_scale_in(protected)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| sdk_error(SET_INSTANCE_PROTECTION, e))
    }

    /// The failed scaling activity that marks the spot group down, if any: a failed activity
    /// inside its retry window.
    async fn spot_down(
        &self,
        spot: &Group,
        policy: SpotFallback,
    ) -> Result<Option<FailedActivity>, AwsError> {
        let since = self.clock.wall() - policy.retry_spot_after.get();
        self.scaling_down(&spot.name, since).await
    }

    /// The newest failed scaling activity of the named group that started at or after `since`.
    async fn scaling_down(
        &self,
        group: &str,
        since: Timestamp,
    ) -> Result<Option<FailedActivity>, AwsError> {
        let start = since.to_rfc3339();
        let output = self
            .autoscaling
            .describe_scaling_activities()
            .auto_scaling_group_name(group)
            .filters(
                ScalingFilter::builder()
                    .name(STATUS_FILTER)
                    .values(FAILED_STATUS)
                    .build(),
            )
            .filters(
                ScalingFilter::builder()
                    .name(START_TIME_FILTER)
                    .values(start)
                    .build(),
            )
            .max_records(ACTIVITY_PROBE_LIMIT)
            .send()
            .await
            .map_err(|e| sdk_error(DESCRIBE_SCALING_ACTIVITIES, e))?;
        Ok(output.activities().first().map(|activity| FailedActivity {
            status_message: activity.status_message().map(str::to_owned),
        }))
    }

    /// Reads the named groups in one call.
    async fn describe_groups<'n>(
        &self,
        names: impl Iterator<Item = &'n str>,
    ) -> Result<DescribeAutoScalingGroupsOutput, AwsError> {
        let mut request = self.autoscaling.describe_auto_scaling_groups();
        for name in names {
            request = request.auto_scaling_group_names(name);
        }
        request
            .send()
            .await
            .map_err(|e| sdk_error(DESCRIBE_GROUPS, e))
    }

    /// Maps each EC2 instance of the named groups to its launch time.
    async fn launch_times<'n>(
        &self,
        groups: impl Iterator<Item = &'n str>,
    ) -> Result<HashMap<String, Timestamp>, AwsError> {
        let filter = groups
            .fold(
                Filter::builder().name(GROUP_TAG_FILTER),
                FilterBuilder::values,
            )
            .build();
        let mut pages = self
            .ec2
            .describe_instances()
            .filters(filter)
            .into_paginator()
            .send();
        let mut launches = HashMap::new();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| sdk_error(DESCRIBE_INSTANCES, e))?;
            for instance in page
                .reservations()
                .iter()
                .flat_map(aws_sdk_ec2::types::Reservation::instances)
            {
                if let (Some(id), Some(launched)) = (instance.instance_id(), instance.launch_time())
                {
                    let millis = launched
                        .to_millis()
                        .map_err(|_| AwsError::malformed(DESCRIBE_INSTANCES, "launch time"))?;
                    launches.insert(id.to_owned(), timestamp_of(millis));
                }
            }
        }
        Ok(launches)
    }
}

/// The groups a fleet's operations read, built once per call: its own group, and its on-demand
/// twin with the spot fallback policy when the fleet opts in.
struct GroupNames<'a> {
    primary: &'a str,
    twin: Option<(String, SpotFallback)>,
}

impl<'a> GroupNames<'a> {
    /// The names of the groups behind `fleet`.
    fn of(fleet: Fleet<'a, AwsFleetOptions>) -> Self {
        Self {
            primary: fleet.name.as_str(),
            twin: fleet
                .options
                .fallback
                .map(|policy| (format!("{}{ON_DEMAND_SUFFIX}", fleet.name.as_str()), policy)),
        }
    }

    /// The names to read: the primary group, then the twin.
    fn iter(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.primary).chain(self.twin.as_ref().map(|(name, _)| name.as_str()))
    }
}

/// The groups of a fleet, read from one `DescribeAutoScalingGroups` response.
enum FleetGroups {
    /// A fleet that is its one group.
    Plain(Group),
    /// A spot fleet with its on-demand twin; both protect new instances from scale-in.
    Fallback {
        spot: Group,
        twin: Group,
        policy: SpotFallback,
    },
}

impl FleetGroups {
    /// Reads a fleet's groups from `output`.
    ///
    /// An opted-in fleet needs both groups to protect new instances from scale-in: the twin's warm
    /// pool depends on it as the spot group's does.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Group::read`], and [`AwsError::Config`] naming a group that does not
    /// protect new instances from scale-in.
    fn read(
        output: &DescribeAutoScalingGroupsOutput,
        names: &GroupNames<'_>,
    ) -> Result<Self, AwsError> {
        let spot = Group::read(output, names.primary)?;
        let Some((twin_name, policy)) = &names.twin else {
            return Ok(Self::Plain(spot));
        };
        let twin = Group::read(output, twin_name)?;
        for group in [&spot, &twin] {
            if !group.protects_new_instances {
                return Err(AwsError::Config {
                    field: "fallback",
                    reason: format!(
                        "group {} does not protect new instances from scale in, which spot fallback requires",
                        group.name
                    ),
                });
            }
        }
        Ok(Self::Fallback {
            spot,
            twin,
            policy: *policy,
        })
    }

    /// The spot group, or the fleet's one group.
    fn spot(&self) -> &Group {
        match self {
            Self::Plain(group) => group,
            Self::Fallback { spot, .. } => spot,
        }
    }

    /// The fleet's groups: spot first, then the on-demand twin.
    fn groups(&self) -> impl Iterator<Item = &Group> {
        let twin = match self {
            Self::Plain(_) => None,
            Self::Fallback { twin, .. } => Some(twin),
        };
        std::iter::once(self.spot()).chain(twin)
    }
}

/// Maps one group member to the provisioner's view of the instance; an instance EC2 does not
/// know about is stamped with `now`.
fn fleet_instance(
    member: &Member,
    release_first: bool,
    launched: &HashMap<String, Timestamp>,
    now: Timestamp,
) -> FleetInstance {
    FleetInstance {
        id: member.id.clone(),
        lifecycle: member.lifecycle,
        launched_at: launched.get(member.id.as_str()).copied().unwrap_or(now),
        release_first,
    }
}

/// Converts an EC2 launch time in Unix milliseconds; a time before the epoch is the epoch.
fn timestamp_of(millis: i64) -> Timestamp {
    Timestamp::from_unix_millis(u64::try_from(millis).unwrap_or_default())
}

/// A failed scaling activity that marks the spot group down.
struct FailedActivity {
    /// The activity's status message, when EC2 recorded one.
    status_message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Capacity, CooldownPolicy, RetrySpotAfter};
    use crate::fake_aws::{FakeFailure, FakeGroup};
    use crate::fleet_harness::{FLEET, Harness, NOW_MILLIS, fleet, fleet_with};
    use std::error::Error;
    use std::time::Duration;

    type TestResult = Result<(), Box<dyn Error>>;

    /// `2026-01-01T00:00:00Z` in Unix milliseconds.
    const LAUNCHED_MILLIS: u64 = 1_767_225_600_000;
    const LAUNCHED_RFC3339: &str = "2026-01-01T00:00:00.000Z";

    #[test]
    fn launch_times_before_the_epoch_clamp_to_the_epoch() {
        assert_eq!(timestamp_of(-1), Timestamp::UNIX_EPOCH);
        assert_eq!(
            timestamp_of(1_767_225_600_000),
            Timestamp::from_unix_millis(LAUNCHED_MILLIS)
        );
    }

    #[tokio::test]
    async fn supply_maps_instances_with_desired_capacity_and_launch_times() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(
            FLEET,
            3,
            &[
                ("i-pending", "Pending"),
                ("i-service", "InService"),
                ("i-ending", "Terminating:Wait"),
            ],
        );
        harness.add_launch("i-pending", LAUNCHED_RFC3339);
        harness.add_launch("i-service", "2026-01-01T00:00:30Z");
        harness.add_launch("i-ending", LAUNCHED_RFC3339);

        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;

        assert_eq!(
            into,
            FleetSupply {
                desired: 3,
                instances: vec![
                    FleetInstance {
                        id: InstanceId::try_new("i-pending")?,
                        lifecycle: InstanceLifecycle::Pending,
                        launched_at: Timestamp::from_unix_millis(LAUNCHED_MILLIS),
                        release_first: false,
                    },
                    FleetInstance {
                        id: InstanceId::try_new("i-service")?,
                        lifecycle: InstanceLifecycle::InService,
                        launched_at: Timestamp::from_unix_millis(LAUNCHED_MILLIS + 30_000),
                        release_first: false,
                    },
                    FleetInstance {
                        id: InstanceId::try_new("i-ending")?,
                        lifecycle: InstanceLifecycle::Terminating,
                        launched_at: Timestamp::from_unix_millis(LAUNCHED_MILLIS),
                        release_first: false,
                    },
                ],
                warm: 0,
                exits: Vec::new(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn supply_stamps_instances_unknown_to_ec2_with_the_clock() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-unknown", "InService")]);

        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;

        assert_eq!(
            into.instances,
            vec![FleetInstance {
                id: InstanceId::try_new("i-unknown")?,
                lifecycle: InstanceLifecycle::InService,
                launched_at: Timestamp::from_unix_millis(NOW_MILLIS),
                release_first: false,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn supply_skips_ec2_for_a_group_without_instances() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 2, &[]);

        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;

        assert_eq!(into.desired, 2);
        assert!(into.instances.is_empty());
        assert_eq!(harness.requests("DescribeInstances"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn supply_clamps_a_negative_desired_capacity_to_zero() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, -1, &[]);

        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;

        assert_eq!(into.desired, 0);
        Ok(())
    }

    #[tokio::test]
    async fn supply_overwrites_a_prefilled_supply() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-new", "InService")]);
        let mut into = FleetSupply {
            desired: 99,
            instances: vec![FleetInstance {
                id: InstanceId::try_new("i-stale")?,
                lifecycle: InstanceLifecycle::Terminating,
                launched_at: Timestamp::UNIX_EPOCH,
                release_first: false,
            }],
            warm: 0,
            exits: Vec::new(),
        };

        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;

        assert_eq!(
            into,
            FleetSupply {
                warm: 0,
                exits: Vec::new(),
                desired: 1,
                instances: vec![FleetInstance {
                    id: InstanceId::try_new("i-new")?,
                    lifecycle: InstanceLifecycle::InService,
                    launched_at: Timestamp::from_unix_millis(NOW_MILLIS),
                    release_first: false,
                }],
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn supply_of_a_missing_group_is_missing_group() -> TestResult {
        let harness = Harness::start().await?;
        let mut into = FleetSupply::default();

        let result = harness.fleets.supply(fleet(FLEET)?.view(), &mut into).await;

        assert!(matches!(result, Err(AwsError::MissingGroup(_))));
        Ok(())
    }

    #[tokio::test]
    async fn throttled_describe_is_transient() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[]);
        harness.fail(
            "DescribeAutoScalingGroups",
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "rate exceeded",
            },
        );

        let mut into = FleetSupply::default();
        let result = harness.fleets.supply(fleet(FLEET)?.view(), &mut into).await;

        assert!(matches!(result, Err(AwsError::Transient { .. })));
        Ok(())
    }

    fn honor_cooldown_sent(harness: &Harness) -> Option<String> {
        harness
            .fake
            .state()
            .requests
            .iter()
            .find(|request| request.operation == SET_DESIRED_CAPACITY)
            .and_then(|request| request.parameters.get("HonorCooldown").cloned())
    }

    #[tokio::test]
    async fn set_desired_capacity_sends_honor_cooldown_when_configured() -> TestResult {
        let harness = Harness::start_with(CooldownPolicy::Honor).await?;
        harness.add_group(FLEET, 2, &[]);

        harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 5)
            .await?;

        assert_eq!(honor_cooldown_sent(&harness).as_deref(), Some("true"));
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_ignores_cooldown_when_configured() -> TestResult {
        let harness = Harness::start_with(CooldownPolicy::Ignore).await?;
        harness.add_group(FLEET, 2, &[]);

        harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 5)
            .await?;

        assert_eq!(honor_cooldown_sent(&harness).as_deref(), Some("false"));
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_raises_the_group() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 2, &[]);

        harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 5)
            .await?;

        assert_eq!(harness.desired(FLEET), Some(5));
        assert_eq!(harness.requests("SetDesiredCapacity"), 1);
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_never_lowers_the_group() -> TestResult {
        let harness = Harness::start().await?;
        let members = [
            ("i-1", "InService"),
            ("i-2", "InService"),
            ("i-3", "InService"),
            ("i-4", "InService"),
            ("i-5", "InService"),
        ];
        harness.add_group(FLEET, 5, &members);

        harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 2)
            .await?;

        assert_eq!(harness.requests("SetDesiredCapacity"), 0);
        assert_eq!(harness.desired(FLEET), Some(5));
        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;
        assert_eq!(into.desired, 5);
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_never_lowers_a_fallback_fleet() -> TestResult {
        const TWIN: &str = "gang-fleet-on-demand";
        let harness = Harness::start().await?;
        let members = [
            ("i-1", "InService"),
            ("i-2", "InService"),
            ("i-3", "InService"),
            ("i-4", "InService"),
            ("i-5", "InService"),
        ];
        for group in [
            FakeGroup {
                new_instances_protected: true,
                ..FakeGroup::new(FLEET, 5, &members)
            },
            FakeGroup {
                new_instances_protected: true,
                ..FakeGroup::new(TWIN, 0, &[])
            },
        ] {
            harness.add_group_with(group);
        }
        let options = AwsFleetOptions {
            capacity: Capacity::Spot,
            machine_class: None,
            fallback: Some(SpotFallback {
                retry_spot_after: RetrySpotAfter::try_new(Duration::from_mins(5))?,
                drain_on_demand_first: true,
            }),
        };
        let fleet = fleet_with(FLEET, options)?;

        harness.fleets.set_desired_capacity(fleet.view(), 2).await?;

        assert_eq!(harness.requests("SetDesiredCapacity"), 0);
        assert_eq!(harness.desired(FLEET), Some(5));
        let mut into = FleetSupply::default();
        harness.fleets.supply(fleet.view(), &mut into).await?;
        assert_eq!(into.desired, 5);
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_above_the_scale_limit_is_a_config_error() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[]);

        let result = harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), u32::MAX)
            .await;

        assert!(matches!(
            result,
            Err(AwsError::Config {
                field: "desired",
                ..
            })
        ));
        assert_eq!(harness.requests("SetDesiredCapacity"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_of_a_missing_group_is_missing_group() -> TestResult {
        let harness = Harness::start().await?;

        let result = harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 1)
            .await;

        assert!(matches!(result, Err(AwsError::MissingGroup(_))));
        Ok(())
    }

    #[tokio::test]
    async fn denied_set_desired_capacity_is_refused() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[]);
        harness.fail(
            "SetDesiredCapacity",
            FakeFailure {
                status: 403,
                code: "AccessDenied",
                message: "not authorized",
            },
        );

        let result = harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 3)
            .await;

        assert!(matches!(result, Err(AwsError::Refused { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn terminate_removes_the_instance_and_lowers_desired() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 2, &[("i-a", "InService"), ("i-b", "InService")]);

        harness
            .fleets
            .terminate(fleet(FLEET)?.view(), &InstanceId::try_new("i-a")?)
            .await?;

        assert_eq!(harness.instance_ids(FLEET), vec!["i-b".to_owned()]);
        assert_eq!(harness.desired(FLEET), Some(1));
        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 1);
        let decrements = harness
            .fake
            .state()
            .requests
            .iter()
            .filter(|request| request.operation == "TerminateInstanceInAutoScalingGroup")
            .all(|request| {
                request
                    .parameters
                    .get("ShouldDecrementDesiredCapacity")
                    .map(String::as_str)
                    == Some("true")
            });
        assert!(decrements);
        Ok(())
    }

    #[tokio::test]
    async fn terminating_twice_is_ok_and_sends_one_request() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-a", "InService")]);
        let fleet = fleet(FLEET)?;
        let instance = InstanceId::try_new("i-a")?;

        harness.fleets.terminate(fleet.view(), &instance).await?;
        harness.fleets.terminate(fleet.view(), &instance).await?;

        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 1);
        Ok(())
    }

    #[tokio::test]
    async fn terminating_an_instance_that_is_already_ending_is_ok() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-t", "Terminating")]);

        harness
            .fleets
            .terminate(fleet(FLEET)?.view(), &InstanceId::try_new("i-t")?)
            .await?;

        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminating_an_absent_instance_is_ok() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[]);

        harness
            .fleets
            .terminate(fleet(FLEET)?.view(), &InstanceId::try_new("i-gone")?)
            .await?;

        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_of_an_instance_in_another_group_is_ok() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group("other-fleet", 1, &[("i-x", "InService")]);
        harness.add_group(FLEET, 1, &[]);

        harness
            .fleets
            .terminate(fleet(FLEET)?.view(), &InstanceId::try_new("i-x")?)
            .await?;

        assert!(
            harness
                .request_log()
                .iter()
                .all(|(operation, _)| operation != "TerminateInstanceInAutoScalingGroup")
        );
        assert_eq!(harness.instance_ids("other-fleet"), vec!["i-x".to_owned()]);
        assert_eq!(harness.desired("other-fleet"), Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn terminate_with_a_missing_group_is_missing_group() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group("other-fleet", 1, &[("i-x", "InService")]);

        let result = harness
            .fleets
            .terminate(fleet(FLEET)?.view(), &InstanceId::try_new("i-x")?)
            .await;

        assert!(matches!(result, Err(AwsError::MissingGroup(_))));
        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_cancels_a_pending_instance() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-p", "Pending")]);

        harness
            .fleets
            .terminate(fleet(FLEET)?.view(), &InstanceId::try_new("i-p")?)
            .await?;

        assert!(harness.instance_ids(FLEET).is_empty());
        assert_eq!(harness.desired(FLEET), Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn fleets_of_one_isa_address_their_own_groups() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group("fleet-a", 1, &[("i-a", "InService")]);
        harness.add_group("fleet-b", 2, &[("i-b", "InService")]);

        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet("fleet-b")?.view(), &mut into)
            .await?;
        assert_eq!(into.desired, 2);
        let ids: Vec<&str> = into.instances.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["i-b"]);

        harness
            .fleets
            .set_desired_capacity(fleet("fleet-a")?.view(), 3)
            .await?;
        assert_eq!(harness.desired("fleet-a"), Some(3));
        assert_eq!(harness.desired("fleet-b"), Some(2));

        harness
            .fleets
            .terminate(fleet("fleet-a")?.view(), &InstanceId::try_new("i-b")?)
            .await?;
        assert_eq!(harness.instance_ids("fleet-b"), vec!["i-b".to_owned()]);
        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    /// A group with no members that is inside a scaling cooldown.
    fn cooling_group(name: &str, desired: i32) -> FakeGroup {
        let mut group = FakeGroup::new(name, desired, &[]);
        group.in_cooldown = true;
        group
    }

    #[tokio::test]
    async fn supply_takes_launch_times_only_for_its_own_instances() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group("fleet-a", 1, &[("i-a", "InService")]);
        harness.add_group(
            "fleet-b",
            2,
            &[("i-b1", "InService"), ("i-b2", "InService")],
        );
        harness.add_launch("i-a", LAUNCHED_RFC3339);
        harness.add_launch("i-b1", "2026-03-01T00:00:00Z");
        harness.add_launch("i-b2", "2026-03-02T00:00:00Z");

        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet("fleet-a")?.view(), &mut into)
            .await?;

        assert_eq!(
            into.instances,
            vec![FleetInstance {
                id: InstanceId::try_new("i-a")?,
                lifecycle: InstanceLifecycle::InService,
                launched_at: Timestamp::from_unix_millis(LAUNCHED_MILLIS),
                release_first: false,
            }]
        );
        let filtered_groups: Vec<String> = harness
            .request_log()
            .into_iter()
            .filter(|(operation, _)| operation == DESCRIBE_INSTANCES)
            .flat_map(|(_, parameters)| {
                parameters
                    .into_iter()
                    .filter(|(key, _)| key.starts_with("Filter.") && key.contains(".Value."))
                    .map(|(_, value)| value)
            })
            .collect();
        assert_eq!(filtered_groups, vec!["fleet-a".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn supply_reads_launch_times_of_every_page() -> TestResult {
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
        harness.add_launch("i-1", LAUNCHED_RFC3339);
        harness.add_launch("i-2", "2026-01-01T00:00:30Z");
        harness.add_launch("i-3", "2026-01-01T00:01:00Z");
        harness.fake.state().page_size = Some(1);

        let mut into = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;

        let launched: Vec<(&str, Timestamp)> = into
            .instances
            .iter()
            .map(|instance| (instance.id.as_str(), instance.launched_at))
            .collect();
        assert_eq!(
            launched,
            vec![
                ("i-1", Timestamp::from_unix_millis(LAUNCHED_MILLIS)),
                ("i-2", Timestamp::from_unix_millis(LAUNCHED_MILLIS + 30_000)),
                ("i-3", Timestamp::from_unix_millis(LAUNCHED_MILLIS + 60_000)),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn raise_into_a_cooldown_is_transient_when_honoring_cooldown() -> TestResult {
        let harness = Harness::start_with(CooldownPolicy::Honor).await?;
        harness.add_group_with(cooling_group(FLEET, 2));

        let result = harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 5)
            .await;

        assert!(matches!(result, Err(AwsError::Transient { .. })));
        assert_eq!(harness.desired(FLEET), Some(2));
        Ok(())
    }

    #[tokio::test]
    async fn raise_into_a_cooldown_applies_when_ignoring_cooldown() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group_with(cooling_group(FLEET, 2));

        harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 5)
            .await?;

        assert_eq!(harness.desired(FLEET), Some(5));
        Ok(())
    }

    #[tokio::test]
    async fn raise_above_the_max_size_is_refused() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(FLEET, 2, &[]);
        group.max_size = 4;
        harness.add_group_with(group);

        let result = harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 5)
            .await;

        assert!(matches!(result, Err(AwsError::Refused { .. })));
        assert_eq!(harness.desired(FLEET), Some(2));
        Ok(())
    }

    #[tokio::test]
    async fn raise_to_exactly_the_max_size_is_applied() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(FLEET, 2, &[]);
        group.max_size = 4;
        harness.add_group_with(group);

        harness
            .fleets
            .set_desired_capacity(fleet(FLEET)?.view(), 4)
            .await?;

        assert_eq!(harness.desired(FLEET), Some(4));
        Ok(())
    }

    #[tokio::test]
    async fn release_scales_in_the_oldest_unprotected_instance() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(
            FLEET,
            2,
            &[
                ("i-a", "InService"),
                ("i-b", "InService"),
                ("i-k", "InService"),
                ("i-r", "InService"),
            ],
        );
        for instance in &mut group.instances {
            instance.protected = matches!(instance.id.as_str(), "i-k" | "i-r");
        }
        group.reuse_on_scale_in = Some(true);
        group.new_instances_protected = true;
        group.simulate = true;
        harness.add_group_with(group);
        harness.add_launch("i-b", "2026-01-01T00:00:00Z");
        harness.add_launch("i-r", "2026-01-01T00:01:00Z");
        harness.add_launch("i-a", "2026-01-01T00:02:00Z");
        harness.add_launch("i-k", "2026-01-01T00:03:00Z");

        harness
            .fleets
            .terminate(fleet(FLEET)?.view(), &InstanceId::try_new("i-r")?)
            .await?;

        assert_eq!(harness.instance_ids(FLEET), vec!["i-k".to_owned()]);
        assert_eq!(harness.desired(FLEET), Some(1));
        assert_eq!(
            harness.warm_ids(FLEET),
            vec!["i-b".to_owned(), "i-r".to_owned(), "i-a".to_owned()]
        );
        Ok(())
    }
}
