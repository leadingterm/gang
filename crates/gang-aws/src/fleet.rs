//! Auto scaling groups as fleets: their size, their instances, and how they grow and shrink.
//!
//! Each fleet is one Auto Scaling group named `Fleet::name`. A fleet that opts into spot fallback
//! also owns its on-demand twin, the group named `Fleet::name` followed by `-on-demand`. The
//! groups' capacity and instance types are their own configuration, so the adapter reads no
//! fleet options beyond the fallback choice.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_config::SdkConfig;
use aws_sdk_autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput;
use aws_sdk_autoscaling::types::Filter as ScalingFilter;
use aws_sdk_autoscaling::types::ScalingActivityStatusCode;
use aws_sdk_ec2::types::Filter;
use aws_sdk_ec2::types::builders::FilterBuilder;
use gang_aws_sdk::call_error;
use gang_clock::{Clock, Tick, Timestamp};
use gang_spi::{FleetInstance, FleetSupply, Held, LaunchFailure, TerminationMode};
use gang_types::{Fleet, InstanceId, InstanceLifecycle};
use tokio::sync::Mutex;

use crate::config::{
    AwsFleetOptions, CooldownPolicy, DEFAULT_SUPPLY_POLL_INTERVAL, ON_DEMAND_SUFFIX, SpotFallback,
};
use crate::errors::AwsError;
use crate::events::Events;
use crate::fallback::{self, Counts, Tier};
use crate::group::{DESCRIBE_GROUPS, Group, Member};

const SET_DESIRED_CAPACITY: &str = "SetDesiredCapacity";
const UPDATE_AUTO_SCALING_GROUP: &str = "UpdateAutoScalingGroup";
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

/// A complete batch, shared without copying its AWS group payload between supply calls.
struct Observation {
    output: Arc<DescribeAutoScalingGroupsOutput>,
    observed: Tick,
    revision: u64,
}

/// The auto scaling groups and EC2 instances behind the fleets of one AWS account and region.
///
/// Each fleet is one group named `Fleet::name`, plus the on-demand twin for an opted-in fleet.
pub(crate) struct Fleets {
    autoscaling: aws_sdk_autoscaling::Client,
    ec2: aws_sdk_ec2::Client,
    clock: Arc<dyn Clock>,
    /// Applied to every desired-capacity write: raises, spot withdrawals and release lowerings.
    cooldown: CooldownPolicy,
    events: Events,
    observation_interval: Duration,
    observation: Mutex<Option<Observation>>,
}

impl Fleets {
    /// Creates the fleet adapter from the shared SDK config; `clock` stamps instances EC2 does
    /// not know about, and `cooldown` decides whether every desired-capacity write (raises, spot
    /// withdrawals and release lowerings alike) honors the group's cooldown.
    pub(crate) fn new(
        sdk: &SdkConfig,
        clock: Arc<dyn Clock>,
        cooldown: CooldownPolicy,
        events: Events,
        autoscaling: aws_sdk_autoscaling::Client,
    ) -> Self {
        Self {
            autoscaling,
            ec2: aws_sdk_ec2::Client::new(sdk),
            clock,
            cooldown,
            events,
            observation_interval: DEFAULT_SUPPLY_POLL_INTERVAL.get(),
            observation: Mutex::new(None),
        }
    }

    pub(crate) fn with_observation_interval(mut self, interval: Duration) -> Self {
        self.observation_interval = interval;
        self
    }

    pub(crate) fn events(&self) -> &Events {
        &self.events
    }

    /// Purely reads the fleet's desired capacity and instances. An opted-in fleet merges its
    /// Spot group and twin, Spot first, and reports a down Spot group's unmet request as abandoned.
    pub(crate) async fn supply(
        &self,
        fleet: Fleet<'_, AwsFleetOptions>,
        into: &mut FleetSupply,
    ) -> Result<(), AwsError> {
        let names = GroupNames::of(fleet);
        let output = self.observe_groups(&names).await?;
        let groups = FleetGroups::read(&output, &names)?;
        let (desired, spot_down, abandoned) = match &groups {
            FleetGroups::Plain(group) => (group.reported(), false, 0),
            FleetGroups::Fallback { spot, twin, policy } => {
                let gap = Counts::from(spot).gap();
                let down = gap > 0 && self.spot_down(spot, *policy).await?.is_some();
                (
                    fallback::reported(Counts::from(spot), Counts::from(twin), down),
                    down,
                    if down { gap } else { 0 },
                )
            }
        };
        let mut launch_failure = None;
        for (index, group) in groups.groups().enumerate() {
            if (index == 0 && spot_down) || group.desired <= group.live() {
                continue;
            }
            if let Some(failure) = self.latest_launch_failure(&group.name).await? {
                launch_failure.get_or_insert(failure);
            } else {
                launch_failure = None;
                break;
            }
        }
        let member_count: usize = groups.groups().map(|group| group.members.len()).sum();
        let launched = if member_count == 0 {
            HashMap::new()
        } else {
            self.launch_times(names.iter()).await?
        };
        let now = self.clock.wall();
        into.desired = desired;
        into.requested = Some(
            groups
                .groups()
                .fold(0_u32, |sum, group| sum.saturating_add(group.desired))
                .saturating_sub(abandoned),
        );
        if let Err(error) = self
            .events
            .supply(fleet.name.as_str(), names.iter(), &mut into.events)
            .await
        {
            tracing::warn!(
                fleet = fleet.name.as_str(),
                %error,
                "AWS infrastructure activity history unavailable; continuing fleet supply"
            );
        }
        into.abandoned = abandoned;
        into.launch_failure = launch_failure;
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

    /// Sets the fleet request, withdrawing unmet capacity without removing live instances.
    /// An opted-in fleet lowers the on-demand twin before healthy Spot.
    pub(crate) async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, AwsFleetOptions>,
        desired: u32,
    ) -> Result<(), AwsError> {
        // Serialize fresh mutation reads and writes with observation fills.
        let mut observation = self.observation.lock().await;
        *observation = None;
        let names = GroupNames::of(fleet);
        let output = self.describe_groups(names.iter()).await?;
        match FleetGroups::read(&output, &names)? {
            FleetGroups::Plain(group) => {
                if group.reported() < desired {
                    self.set_capacity(held, &group, desired).await?;
                } else {
                    let target = desired.max(group.live());
                    if target < group.desired {
                        self.set_capacity(held, &group, target).await?;
                    }
                }
                Ok(())
            }
            FleetGroups::Fallback { spot, twin, policy } => {
                let failed = self.spot_down(&spot, policy).await?;
                let down = failed.is_some();
                for write in
                    fallback::writes(Counts::from(&spot), Counts::from(&twin), down, desired)
                {
                    let group = match write.tier {
                        Tier::Spot => &spot,
                        Tier::OnDemand => &twin,
                    };
                    if down {
                        tracing::info!(
                            fleet = %fleet.name,
                            group = %group.name,
                            desired = write.desired,
                            status_message = failed.as_ref().and_then(|failed| failed.status_message.as_deref()),
                            "spot fallback: writing desired capacity"
                        );
                    }
                    self.set_capacity(held, group, write.desired).await?;
                }
                Ok(())
            }
        }
    }

    /// Retires an instance. Scale-in may reuse the warm pool when its launch-template id and
    /// immutable version match the group's current template. Replacement and unknown or stale
    /// generations are terminated, decrementing desired capacity when it is nonzero.
    pub(crate) async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, AwsFleetOptions>,
        instance: &InstanceId,
        mode: TerminationMode,
    ) -> Result<(), AwsError> {
        let mut observation = self.observation.lock().await;
        *observation = None;
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
        if mode == TerminationMode::ScaleIn
            && member.lifecycle == InstanceLifecycle::InService
            && group.reuse_on_scale_in
            && member.current_generation
        {
            return self.release(held, group, member, instance).await;
        }
        held.check()?;
        self.autoscaling
            .terminate_instance_in_auto_scaling_group()
            .instance_id(instance.as_str())
            .should_decrement_desired_capacity(group.desired != 0)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| call_error(TERMINATE_INSTANCE, e).into())
    }

    /// Releases an in-service instance of a warm-pool group. The release target is computed
    /// first, so a refusal leaves the group untouched; then the instance's scale-in protection
    /// is lifted and the group's desired capacity lowered to the target.
    async fn release(
        &self,
        held: &Held,
        group: &Group,
        member: &Member,
        instance: &InstanceId,
    ) -> Result<(), AwsError> {
        let target = group.release_target(instance)?;
        if member.protected {
            self.set_protection(held, &group.name, instance, false)
                .await?;
        }
        if target < group.desired {
            self.set_capacity(held, group, target).await?;
        }
        Ok(())
    }

    /// Raises the physical limit when necessary before writing desired capacity. The logical
    /// fleet bound is enforced by the reconciler, not by this retained ASG limit.
    async fn set_capacity(&self, held: &Held, group: &Group, desired: u32) -> Result<(), AwsError> {
        let wanted = i32::try_from(desired).map_err(|_| AwsError::Config {
            field: "desired",
            reason: "exceeds the largest capacity an auto scaling group accepts".to_owned(),
        })?;
        if desired > group.max_size {
            let maximum = desired.max(group.desired).max(group.live());
            let maximum = i32::try_from(maximum).map_err(|_| AwsError::Config {
                field: "desired",
                reason: "exceeds the largest capacity an auto scaling group accepts".to_owned(),
            })?;
            held.check()?;
            self.autoscaling
                .update_auto_scaling_group()
                .auto_scaling_group_name(&group.name)
                .max_size(maximum)
                .send()
                .await
                .map_err(|e| call_error(UPDATE_AUTO_SCALING_GROUP, e))?;
        }
        held.check()?;
        self.autoscaling
            .set_desired_capacity()
            .auto_scaling_group_name(&group.name)
            .desired_capacity(wanted)
            .honor_cooldown(self.cooldown.honors())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| call_error(SET_DESIRED_CAPACITY, e).into())
    }

    /// Sets one instance's scale-in protection within its group.
    async fn set_protection(
        &self,
        held: &Held,
        group: &str,
        instance: &InstanceId,
        protected: bool,
    ) -> Result<(), AwsError> {
        held.check()?;
        self.autoscaling
            .set_instance_protection()
            .auto_scaling_group_name(group)
            .instance_ids(instance.as_str())
            .protected_from_scale_in(protected)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| call_error(SET_INSTANCE_PROTECTION, e).into())
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
            .map_err(|e| call_error(DESCRIBE_SCALING_ACTIVITIES, e))?;
        Ok(output.activities().first().map(|activity| FailedActivity {
            status_message: activity.status_message().map(str::to_owned),
        }))
    }

    /// Reads the newest activity without filtering its status.
    async fn latest_launch_failure(&self, group: &str) -> Result<Option<LaunchFailure>, AwsError> {
        let output = self
            .autoscaling
            .describe_scaling_activities()
            .auto_scaling_group_name(group)
            .max_records(ACTIVITY_PROBE_LIMIT)
            .send()
            .await
            .map_err(|e| call_error(DESCRIBE_SCALING_ACTIVITIES, e))?;
        let Some(activity) = output.activities().first() else {
            return Ok(None);
        };
        if activity
            .status_code()
            .map(ScalingActivityStatusCode::as_str)
            != Some(FAILED_STATUS)
        {
            return Ok(None);
        }
        Ok(Some(LaunchFailure {
            reason: activity
                .status_message()
                .unwrap_or(FAILED_STATUS)
                .to_owned(),
        }))
    }

    /// Coalesces supply reads into one complete snapshot of all managed groups.
    async fn observe_groups(
        &self,
        requested: &GroupNames<'_>,
    ) -> Result<Arc<DescribeAutoScalingGroupsOutput>, AwsError> {
        let mut cached = self.observation.lock().await;
        let managed = self.events.managed();
        let revision = managed.revision();
        let now = self.clock.monotonic();
        if let Some(observation) = cached.as_ref()
            && observation.revision == revision
            && now.saturating_duration_since(observation.observed) < self.observation_interval
            && requested.iter().all(|name| {
                observation
                    .output
                    .auto_scaling_groups()
                    .iter()
                    .any(|group| group.auto_scaling_group_name() == Some(name))
            })
        {
            return Ok(Arc::clone(&observation.output));
        }
        let mut names = managed.group_names();
        for name in requested.iter() {
            if !names.iter().any(|candidate| candidate == name) {
                names.push(name.to_owned());
            }
        }
        let output = Arc::new(
            self.describe_groups(names.iter().map(String::as_str))
                .await?,
        );
        self.events.observe_groups(&output).await;
        *cached = Some(Observation {
            output: Arc::clone(&output),
            observed: self.clock.monotonic(),
            revision,
        });
        Ok(output)
    }

    /// Reads every page of the named groups in API-sized batches.
    async fn describe_groups<'n>(
        &self,
        names: impl Iterator<Item = &'n str>,
    ) -> Result<DescribeAutoScalingGroupsOutput, AwsError> {
        let names: Vec<_> = names.collect();
        let mut groups = Vec::new();
        for chunk in names.chunks(50) {
            let mut request = self.autoscaling.describe_auto_scaling_groups();
            for name in chunk {
                request = request.auto_scaling_group_names(*name);
            }
            let mut pages = request.into_paginator().send();
            while let Some(page) = pages.next().await {
                let page = page.map_err(|e| call_error(DESCRIBE_GROUPS, e))?;
                groups.extend(page.auto_scaling_groups.unwrap_or_default());
            }
        }
        Ok(DescribeAutoScalingGroupsOutput::builder()
            .set_auto_scaling_groups(Some(groups))
            .build())
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
            let page = page.map_err(|e| call_error(DESCRIBE_INSTANCES, e))?;
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
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    use std::time::Duration;

    type TestResult = Result<(), Box<dyn Error>>;

    /// `2026-01-01T00:00:00Z` in Unix milliseconds.
    const LAUNCHED_MILLIS: u64 = 1_767_225_600_000;
    const LAUNCHED_RFC3339: &str = "2026-01-01T00:00:00.000Z";

    #[tokio::test]
    async fn supply_batches_concurrent_fleets_and_expires_at_interval() -> TestResult {
        let harness = Harness::start().await?;
        let first = fleet(FLEET)?;
        let second = fleet("second")?;
        harness.add_group(FLEET, 0, &[]);
        harness.add_group("second", 0, &[]);
        harness.fleets.events().managed().add(first.view());
        harness.fleets.events().managed().add(second.view());
        let mut control = FleetSupply::default();
        let mut observer = FleetSupply::default();
        let (a, b) = tokio::join!(
            harness.fleets.supply(first.view(), &mut control),
            harness.fleets.supply(second.view(), &mut observer),
        );
        a?;
        b?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 1);
        harness.clock.advance(Duration::from_millis(999));
        harness.fleets.supply(first.view(), &mut control).await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 1);
        harness.clock.advance(Duration::from_millis(1));
        harness.fleets.supply(second.view(), &mut observer).await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 2);
        Ok(())
    }

    #[tokio::test]
    async fn slow_batch_is_fresh_when_completed() -> TestResult {
        let harness = Harness::start().await?;
        let first = fleet(FLEET)?;
        let second = fleet("second")?;
        harness.add_group(FLEET, 0, &[]);
        harness.add_group("second", 0, &[]);
        harness.fleets.events().managed().add(first.view());
        harness.fleets.events().managed().add(second.view());

        let mut supply = FleetSupply::default();
        {
            let fetch = harness.fleets.supply(first.view(), &mut supply);
            tokio::pin!(fetch);
            poll_fn(|cx| match fetch.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("the fake AWS request must yield before completing"),
            })
            .await;
            harness.clock.advance(Duration::from_secs(1));
            fetch.await?;
        }

        harness.fleets.supply(second.view(), &mut supply).await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 1);
        Ok(())
    }

    #[tokio::test]
    async fn managed_revision_and_unregistered_names_refresh_snapshot() -> TestResult {
        let harness = Harness::start().await?;
        let first = fleet(FLEET)?;
        let second = fleet("second")?;
        let third = fleet("third")?;
        for name in [FLEET, "second", "third"] {
            harness.add_group(name, 0, &[]);
        }
        harness.fleets.events().managed().add(first.view());
        let mut supply = FleetSupply::default();
        harness.fleets.supply(first.view(), &mut supply).await?;
        harness.fleets.events().managed().add(second.view());
        harness.fleets.supply(first.view(), &mut supply).await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 2);
        harness.fleets.supply(second.view(), &mut supply).await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 2);
        harness.fleets.supply(third.view(), &mut supply).await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 3);
        Ok(())
    }

    #[tokio::test]
    async fn supply_batches_and_paginates_more_than_fifty_groups() -> TestResult {
        let harness = Harness::start().await?;
        harness.fake.state().group_page_size = Some(10);
        for index in 0..51 {
            let name = format!("fleet-{index:02}");
            harness.add_group(&name, 0, &[]);
            harness.fleets.events().managed().add(fleet(&name)?.view());
        }
        let mut supply = FleetSupply::default();
        harness
            .fleets
            .supply(fleet("fleet-50")?.view(), &mut supply)
            .await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 6);
        harness
            .fleets
            .supply(fleet("fleet-00")?.view(), &mut supply)
            .await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 6);
        let requests = harness.request_log();
        let group_requests: Vec<_> = requests
            .iter()
            .filter(|(operation, _)| operation == DESCRIBE_GROUPS)
            .collect();
        assert!(
            group_requests
                .iter()
                .any(|(_, params)| params.contains_key("NextToken"))
        );
        assert!(group_requests.iter().all(|(_, params)| {
            params
                .keys()
                .filter(|key| key.starts_with("AutoScalingGroupNames.member."))
                .count()
                <= 50
        }));
        Ok(())
    }

    #[tokio::test]
    async fn mutation_reads_fresh_and_invalidates_observation() -> TestResult {
        let harness = Harness::start().await?;
        let test_fleet = fleet(FLEET)?;
        harness.add_group(FLEET, 1, &[]);
        let mut supply = FleetSupply::default();
        harness
            .fleets
            .supply(test_fleet.view(), &mut supply)
            .await?;
        harness.fake.state().groups[0].desired = 3;
        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, test_fleet.view(), 2)
                .await
        })
        .await?;
        assert_eq!(harness.desired(FLEET), Some(2));
        harness
            .fleets
            .supply(test_fleet.view(), &mut supply)
            .await?;
        assert_eq!(supply.desired, 2);
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 3);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_reads_fresh_and_invalidates_observation() -> TestResult {
        let harness = Harness::start().await?;
        let test_fleet = fleet(FLEET)?;
        harness.add_group(FLEET, 1, &[]);
        let mut supply = FleetSupply::default();
        harness
            .fleets
            .supply(test_fleet.view(), &mut supply)
            .await?;
        harness.fake.state().groups[0] = FakeGroup::new(FLEET, 1, &[("i-new", "Pending")]);
        let instance = InstanceId::try_new("i-new")?;
        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(held, test_fleet.view(), &instance, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        assert_eq!(harness.requests(TERMINATE_INSTANCE), 1);
        harness
            .fleets
            .supply(test_fleet.view(), &mut supply)
            .await?;
        assert_eq!(supply.desired, 0);
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 3);
        Ok(())
    }

    #[tokio::test]
    async fn expired_observation_is_not_used_when_refresh_is_throttled() -> TestResult {
        let harness = Harness::start().await?;
        let test_fleet = fleet(FLEET)?;
        harness.add_group(FLEET, 1, &[]);
        let mut supply = FleetSupply::default();
        harness
            .fleets
            .supply(test_fleet.view(), &mut supply)
            .await?;
        harness.clock.advance(Duration::from_secs(1));
        harness.fail(
            DESCRIBE_GROUPS,
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "rate exceeded",
            },
        );
        let result = harness.fleets.supply(test_fleet.view(), &mut supply).await;
        assert!(matches!(
            result,
            Err(AwsError::Call(gang_aws_sdk::CallError::Transient { .. }))
        ));
        assert_eq!(harness.requests(SET_DESIRED_CAPACITY), 0);
        assert_eq!(harness.requests(TERMINATE_INSTANCE), 0);
        Ok(())
    }

    #[tokio::test]
    async fn failed_later_page_never_publishes_partial_snapshot() -> TestResult {
        let harness = Harness::start().await?;
        let first = fleet(FLEET)?;
        let second = fleet("second")?;
        harness.add_group(FLEET, 0, &[]);
        harness.add_group("second", 0, &[]);
        harness.fleets.events().managed().add(first.view());
        harness.fleets.events().managed().add(second.view());
        harness.fake.state().group_page_size = Some(1);
        let mut supply = FleetSupply::default();
        harness.fleets.supply(first.view(), &mut supply).await?;
        harness.clock.advance(Duration::from_secs(1));
        harness.fake.state().group_page_failure = Some((
            "1".to_owned(),
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "rate exceeded",
            },
        ));
        for _ in 0..2 {
            assert!(matches!(
                harness.fleets.supply(first.view(), &mut supply).await,
                Err(AwsError::Call(gang_aws_sdk::CallError::Transient { .. }))
            ));
        }
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 6);
        harness.fake.state().group_page_failure = None;
        harness.fleets.supply(second.view(), &mut supply).await?;
        assert_eq!(harness.requests(DESCRIBE_GROUPS), 8);
        assert_eq!(harness.requests(SET_DESIRED_CAPACITY), 0);
        assert_eq!(harness.requests(TERMINATE_INSTANCE), 0);
        Ok(())
    }

    #[test]
    fn launch_times_before_the_epoch_clamp_to_the_epoch() {
        assert_eq!(timestamp_of(-1), Timestamp::UNIX_EPOCH);
        assert_eq!(
            timestamp_of(1_767_225_600_000),
            Timestamp::from_unix_millis(LAUNCHED_MILLIS)
        );
    }

    #[tokio::test]
    async fn supply_uses_failed_status_when_activity_has_no_message() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[]);
        harness
            .fake
            .state()
            .activities
            .push(crate::fake_aws::FakeActivity {
                group: FLEET.to_owned(),
                status_code: "Failed".to_owned(),
                start_time: "2026-01-01T00:00:00Z".to_owned(),
                message: None,
            });
        let mut supply = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut supply)
            .await?;
        assert_eq!(
            supply
                .launch_failure
                .as_ref()
                .map(|failure| failure.reason.as_str()),
            Some("Failed")
        );
        Ok(())
    }

    #[tokio::test]
    async fn supply_reports_only_the_newest_failed_activity_for_unmet_requests() -> TestResult {
        for (newest, expected) in [
            ("Failed", Some("launch refused")),
            ("InProgress", None),
            ("Successful", None),
        ] {
            let harness = Harness::start().await?;
            harness.add_group(FLEET, 2, &[("i-live", "InService")]);
            harness.add_activity(FLEET, "Failed", "2026-01-01T00:00:00Z");
            harness
                .fake
                .state()
                .activities
                .push(crate::fake_aws::FakeActivity {
                    group: FLEET.to_owned(),
                    status_code: newest.to_owned(),
                    start_time: "2026-01-02T00:00:00Z".to_owned(),
                    message: Some("launch refused".to_owned()),
                });
            let mut supply = FleetSupply::default();
            harness
                .fleets
                .supply(fleet(FLEET)?.view(), &mut supply)
                .await?;
            assert_eq!(
                supply
                    .launch_failure
                    .as_ref()
                    .map(|failure| failure.reason.as_str()),
                expected
            );
            let requests = harness.request_log();
            let (_, parameters) = requests
                .iter()
                .find(|(operation, _)| operation == "DescribeScalingActivities")
                .ok_or("missing activity request")?;
            assert_eq!(parameters.get("MaxRecords").map(String::as_str), Some("1"));
            assert!(!parameters.keys().any(|key| key.starts_with("Filters.")));
        }
        Ok(())
    }

    #[tokio::test]
    async fn supply_skips_activity_probe_when_request_is_met() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-live", "InService")]);
        harness.add_activity(FLEET, "Failed", "2026-01-01T00:00:00Z");
        let mut supply = FleetSupply::default();
        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut supply)
            .await?;
        assert_eq!(supply.launch_failure, None);
        assert_eq!(harness.activity_probes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn an_expired_permission_writes_no_capacity() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(FLEET, 1, &[("i-1", "InService")]);
        group.max_size = 1;
        harness.add_group_with(group);
        let test_fleet = fleet(FLEET)?;
        let result = gang_testkit::fence::with_expired_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, test_fleet.view(), 5)
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::LeaseLapsed(_))
                | gang_spi::RunError::Overran(_))
        ));
        assert_eq!(harness.requests("SetDesiredCapacity"), 0);
        assert_eq!(harness.requests(UPDATE_AUTO_SCALING_GROUP), 0);
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_withdraws_only_unmet_requests() -> TestResult {
        for (current, requested, expected, writes) in [(4, 2, 2, 1), (4, 0, 2, 1), (2, 0, 2, 0)] {
            let harness = Harness::start().await?;
            harness.add_group(FLEET, current, &[("i-1", "InService"), ("i-2", "Pending")]);
            gang_testkit::fence::with_held(async |held| {
                harness
                    .fleets
                    .set_desired_capacity(
                        held,
                        fleet(FLEET).expect("test value is valid").view(),
                        requested,
                    )
                    .await
            })
            .await?;
            assert_eq!(harness.desired(FLEET), Some(expected));
            assert_eq!(harness.requests("SetDesiredCapacity"), writes);
        }
        Ok(())
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
                abandoned: 0,
                desired: 3,
                requested: Some(3),
                events: Vec::new(),
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
                launch_failure: None,
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
            abandoned: 0,
            desired: 99,
            requested: None,
            events: Vec::new(),
            instances: vec![FleetInstance {
                id: InstanceId::try_new("i-stale")?,
                lifecycle: InstanceLifecycle::Terminating,
                launched_at: Timestamp::UNIX_EPOCH,
                release_first: false,
            }],
            warm: 0,
            exits: Vec::new(),
            launch_failure: Some(LaunchFailure {
                reason: "stale".to_owned(),
            }),
        };

        harness
            .fleets
            .supply(fleet(FLEET)?.view(), &mut into)
            .await?;

        assert_eq!(
            into,
            FleetSupply {
                abandoned: 0,
                warm: 0,
                exits: Vec::new(),
                launch_failure: None,
                desired: 1,
                requested: Some(1),
                events: Vec::new(),
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

        assert!(matches!(
            result,
            Err(AwsError::Call(gang_aws_sdk::CallError::Transient { .. }))
        ));
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

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 5)
                .await
        })
        .await?;

        assert_eq!(honor_cooldown_sent(&harness).as_deref(), Some("true"));
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_ignores_cooldown_when_configured() -> TestResult {
        let harness = Harness::start_with(CooldownPolicy::Ignore).await?;
        harness.add_group(FLEET, 2, &[]);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 5)
                .await
        })
        .await?;

        assert_eq!(honor_cooldown_sent(&harness).as_deref(), Some("false"));
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_raises_the_group() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 2, &[]);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 5)
                .await
        })
        .await?;

        assert_eq!(harness.desired(FLEET), Some(5));
        assert_eq!(harness.requests("SetDesiredCapacity"), 1);
        Ok(())
    }

    #[tokio::test]
    async fn rollout_grows_physical_maximum_for_each_larger_request() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(
            FLEET,
            2,
            &[("i-old-1", "InService"), ("i-old-2", "InService")],
        );
        group.min_size = 1;
        group.max_size = 2;
        group.stale_instances = vec!["i-old-1".to_owned(), "i-old-2".to_owned()];
        group.simulate = true;
        harness.add_group_with(group);
        let fleet = fleet(FLEET)?;

        for target in [4, 6, 6] {
            gang_testkit::fence::with_held(async |held| {
                harness
                    .fleets
                    .set_desired_capacity(held, fleet.view(), target)
                    .await
            })
            .await?;
        }
        let state = harness.fake.state();
        let group = &state.groups[0];
        assert_eq!((group.min_size, group.max_size, group.desired), (1, 6, 6));
        assert_eq!(group.instances.len(), 6);
        let writes: Vec<_> = state
            .requests
            .iter()
            .filter(|request| {
                matches!(
                    request.operation.as_str(),
                    UPDATE_AUTO_SCALING_GROUP | SET_DESIRED_CAPACITY
                )
            })
            .map(|request| (request.operation.as_str(), &request.parameters))
            .collect();
        assert_eq!(writes.len(), 4);
        assert_eq!(writes[0].0, UPDATE_AUTO_SCALING_GROUP);
        assert_eq!(writes[0].1.get("MaxSize").map(String::as_str), Some("4"));
        assert_eq!(writes[1].0, SET_DESIRED_CAPACITY);
        assert_eq!(writes[2].0, UPDATE_AUTO_SCALING_GROUP);
        assert_eq!(writes[2].1.get("MaxSize").map(String::as_str), Some("6"));
        assert_eq!(writes[3].0, SET_DESIRED_CAPACITY);
        Ok(())
    }

    #[tokio::test]
    async fn failed_maximum_growth_never_sends_desired_and_retry_is_safe() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(FLEET, 2, &[("i-old", "InService")]);
        group.max_size = 2;
        harness.add_group_with(group);
        harness.fail(
            UPDATE_AUTO_SCALING_GROUP,
            FakeFailure {
                status: 400,
                code: "ValidationError",
                message: "maximum update refused",
            },
        );
        let fleet = fleet(FLEET)?;
        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet.view(), 4)
                .await
        })
        .await;
        assert!(result.is_err());
        assert_eq!(harness.requests(SET_DESIRED_CAPACITY), 0);
        assert_eq!(harness.desired(FLEET), Some(2));
        harness.clear_failure(UPDATE_AUTO_SCALING_GROUP);
        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet.view(), 4)
                .await
        })
        .await?;
        assert_eq!(harness.desired(FLEET), Some(4));
        Ok(())
    }

    #[tokio::test]
    async fn existing_maximum_needs_no_update() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(FLEET, 2, &[]);
        group.max_size = 4;
        harness.add_group_with(group);
        let fleet = fleet(FLEET)?;
        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet.view(), 4)
                .await
        })
        .await?;
        assert_eq!(harness.requests(UPDATE_AUTO_SCALING_GROUP), 0);
        assert_eq!(harness.desired(FLEET), Some(4));
        Ok(())
    }

    #[tokio::test]
    async fn cooldown_failure_retains_grown_maximum_and_retry_only_sets_desired() -> TestResult {
        let harness = Harness::start_with(CooldownPolicy::Honor).await?;
        let mut group = FakeGroup::new(FLEET, 2, &[]);
        group.max_size = 2;
        group.in_cooldown = true;
        harness.add_group_with(group);
        let fleet = fleet(FLEET)?;
        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet.view(), 4)
                .await
        })
        .await;
        assert!(result.is_err());
        assert_eq!(harness.desired(FLEET), Some(2));
        assert_eq!(harness.requests(UPDATE_AUTO_SCALING_GROUP), 1);
        harness.fake.state().groups[0].in_cooldown = false;
        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet.view(), 4)
                .await
        })
        .await?;
        assert_eq!(harness.requests(UPDATE_AUTO_SCALING_GROUP), 1);
        assert_eq!(harness.desired(FLEET), Some(4));
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_keeps_live_group_members() -> TestResult {
        let harness = Harness::start().await?;
        let members = [
            ("i-1", "InService"),
            ("i-2", "InService"),
            ("i-3", "InService"),
            ("i-4", "InService"),
            ("i-5", "InService"),
        ];
        harness.add_group(FLEET, 5, &members);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 2)
                .await
        })
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
    async fn set_desired_capacity_keeps_live_fallback_members() -> TestResult {
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

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet.view(), 2)
                .await
        })
        .await?;

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

        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    u32::MAX,
                )
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::Config {
                field: "desired",
                ..
            }))
        ));
        assert_eq!(harness.requests("SetDesiredCapacity"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn set_desired_capacity_of_a_missing_group_is_missing_group() -> TestResult {
        let harness = Harness::start().await?;

        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 1)
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::MissingGroup(_)))
        ));
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

        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 3)
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::Call(
                gang_aws_sdk::CallError::Refused { .. }
            )))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn an_expired_permission_terminates_nothing() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-1", "InService")]);
        let test_fleet = fleet(FLEET)?;
        let instance = InstanceId::try_new("i-1")?;
        let result = gang_testkit::fence::with_expired_held(async |held| {
            harness
                .fleets
                .terminate(held, test_fleet.view(), &instance, TerminationMode::ScaleIn)
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::LeaseLapsed(_))
                | gang_spi::RunError::Overran(_))
        ));
        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_removes_the_instance_and_lowers_desired() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 2, &[("i-a", "InService"), ("i-b", "InService")]);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    &InstanceId::try_new("i-a").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
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

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(held, fleet.view(), &instance, TerminationMode::ScaleIn)
                .await
        })
        .await?;
        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(held, fleet.view(), &instance, TerminationMode::ScaleIn)
                .await
        })
        .await?;

        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 1);
        Ok(())
    }

    #[tokio::test]
    async fn terminating_an_instance_that_is_already_ending_is_ok() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-t", "Terminating")]);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    &InstanceId::try_new("i-t").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
        .await?;

        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminating_an_absent_instance_is_ok() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[]);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    &InstanceId::try_new("i-gone").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
        .await?;

        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_of_an_instance_in_another_group_is_ok() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group("other-fleet", 1, &[("i-x", "InService")]);
        harness.add_group(FLEET, 1, &[]);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    &InstanceId::try_new("i-x").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
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

        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    &InstanceId::try_new("i-x").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::MissingGroup(_)))
        ));
        assert_eq!(harness.requests("TerminateInstanceInAutoScalingGroup"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_cancels_a_pending_instance() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(FLEET, 1, &[("i-p", "Pending")]);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    &InstanceId::try_new("i-p").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
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

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(
                    held,
                    fleet("fleet-a").expect("test value is valid").view(),
                    3,
                )
                .await
        })
        .await?;
        assert_eq!(harness.desired("fleet-a"), Some(3));
        assert_eq!(harness.desired("fleet-b"), Some(2));

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet("fleet-a").expect("test value is valid").view(),
                    &InstanceId::try_new("i-b").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
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

        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 5)
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::Call(
                gang_aws_sdk::CallError::Transient { .. }
            )))
        ));
        assert_eq!(harness.desired(FLEET), Some(2));
        Ok(())
    }

    #[tokio::test]
    async fn raise_into_a_cooldown_applies_when_ignoring_cooldown() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group_with(cooling_group(FLEET, 2));

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 5)
                .await
        })
        .await?;

        assert_eq!(harness.desired(FLEET), Some(5));
        Ok(())
    }

    #[tokio::test]
    async fn refused_maximum_growth_leaves_capacity_unchanged() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(FLEET, 2, &[]);
        group.max_size = 4;
        harness.add_group_with(group);
        harness.fail(
            UPDATE_AUTO_SCALING_GROUP,
            FakeFailure {
                status: 403,
                code: "AccessDenied",
                message: "maximum update denied",
            },
        );

        let result = gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 5)
                .await
        })
        .await;

        assert!(matches!(
            result,
            Err(gang_spi::RunError::Failed(AwsError::Call(
                gang_aws_sdk::CallError::Refused { .. }
            )))
        ));
        assert_eq!(harness.desired(FLEET), Some(2));
        assert_eq!(harness.requests(SET_DESIRED_CAPACITY), 0);
        assert_eq!(harness.requests(UPDATE_AUTO_SCALING_GROUP), 1);
        let state = harness.fake.state();
        assert_eq!(state.groups[0].max_size, 4);
        let update = state
            .requests
            .iter()
            .find(|request| request.operation == UPDATE_AUTO_SCALING_GROUP)
            .expect("the maximum update was requested");
        assert_eq!(
            update.parameters.get("MaxSize").map(String::as_str),
            Some("5")
        );
        Ok(())
    }

    #[tokio::test]
    async fn raise_to_exactly_the_max_size_is_applied() -> TestResult {
        let harness = Harness::start().await?;
        let mut group = FakeGroup::new(FLEET, 2, &[]);
        group.max_size = 4;
        harness.add_group_with(group);

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .set_desired_capacity(held, fleet(FLEET).expect("test value is valid").view(), 4)
                .await
        })
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

        gang_testkit::fence::with_held(async |held| {
            harness
                .fleets
                .terminate(
                    held,
                    fleet(FLEET).expect("test value is valid").view(),
                    &InstanceId::try_new("i-r").expect("test value is valid"),
                    TerminationMode::ScaleIn,
                )
                .await
        })
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
