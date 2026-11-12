//! One auto scaling group as read from AWS: its desired capacity, its instances, and the
//! warm-pool rules that decide how it shrinks.

use aws_sdk_autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput;
use aws_sdk_autoscaling::types::{
    Instance as AutoScalingInstance, InstanceReusePolicy, LifecycleState,
};
use gang_types::{FleetName, InstanceId, InstanceLifecycle};

use crate::errors::AwsError;
use crate::fallback::Counts;

/// The operation whose response describes the groups.
pub(crate) const DESCRIBE_GROUPS: &str = "DescribeAutoScalingGroups";

/// One instance of a group, with the lifecycle and scale-in protection it had when read.
#[derive(Debug)]
pub(crate) struct Member {
    /// The instance's EC2 id.
    pub id: InstanceId,
    /// The provisioner's view of the instance's lifecycle.
    pub lifecycle: InstanceLifecycle,
    /// Whether the group protects the instance from scale-in.
    pub protected: bool,
}

/// An auto scaling group as read from AWS, with the warm-pool rules that govern its release.
#[derive(Debug)]
pub(crate) struct Group {
    /// The group's name.
    pub name: String,
    /// The desired capacity; a negative capacity reads as zero.
    pub desired: u32,
    /// Every instance the group lists, in the order AWS lists them.
    pub members: Vec<Member>,
    /// Whether scale-in parks instances in the warm pool instead of terminating them.
    pub reuse_on_scale_in: bool,
    /// Whether instances the group launches are protected from scale-in.
    pub protects_new_instances: bool,
    /// The instances parked in the group's warm pool; `0` without one.
    pub warm: u32,
}

impl Group {
    /// Reads the group named `name` from a `DescribeAutoScalingGroups` response.
    ///
    /// A group that reuses warm instances on scale-in must also protect new instances from
    /// scale-in, or a scale-in could terminate an instance the warm pool expects to keep.
    ///
    /// # Errors
    ///
    /// Returns [`AwsError::MissingGroup`] when the response lacks the group,
    /// [`AwsError::MalformedResponse`] when the group or an instance lacks a field the adapter
    /// needs, and [`AwsError::Config`] when warm-pool reuse is configured without protection.
    pub(crate) fn read(
        output: &DescribeAutoScalingGroupsOutput,
        name: &str,
    ) -> Result<Self, AwsError> {
        let group = output
            .auto_scaling_groups()
            .iter()
            .find(|group| group.auto_scaling_group_name() == Some(name))
            .ok_or_else(|| missing(name))?;
        let reuse_on_scale_in = group
            .warm_pool_configuration()
            .and_then(|pool| pool.instance_reuse_policy())
            .and_then(InstanceReusePolicy::reuse_on_scale_in)
            .unwrap_or_default();
        let protects_new_instances = group
            .new_instances_protected_from_scale_in()
            .unwrap_or_default();
        if reuse_on_scale_in && !protects_new_instances {
            return Err(AwsError::Config {
                field: "warm_pool",
                reason: format!(
                    "group {name} reuses warm instances on scale in without protecting new instances from scale in"
                ),
            });
        }
        let desired = group
            .desired_capacity()
            .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "desired capacity"))?;
        let members = group
            .instances()
            .iter()
            .map(member)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            name: name.to_owned(),
            desired: capacity_of(desired),
            members,
            reuse_on_scale_in,
            protects_new_instances,
            warm: group.warm_pool_size().map_or(0, capacity_of),
        })
    }

    /// The number of instances that are pending or in service; only these count toward capacity.
    #[must_use]
    pub(crate) fn live(&self) -> u32 {
        let count = self.members.iter().filter(|member| is_live(member)).count();
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    /// The capacity the group reports: its desired capacity, or its live instances if those
    /// are more, so an instance AWS still holds is never invisible to the fleet.
    #[must_use]
    pub(crate) fn reported(&self) -> u32 {
        Counts::from(self).reported()
    }

    /// The member with the given instance id, if the group lists it.
    #[must_use]
    pub(crate) fn member(&self, id: &InstanceId) -> Option<&Member> {
        self.members.iter().find(|member| member.id == *id)
    }

    /// The desired capacity that releases `releasing` into the warm pool and keeps the rest.
    ///
    /// The target keeps every pending member, every protected in-service member other than
    /// `releasing`, and the capacity requested but not yet live (`desired` minus `live`).
    /// Unprotected in-service members are not kept, so scale-in may remove them. A target at or
    /// above `desired` means the group needs no write.
    ///
    /// # Errors
    ///
    /// Returns [`AwsError::Config`] when an in-service member other than `releasing` is
    /// unprotected and the group is not already lowering (`desired` is at least `live`): such a
    /// member predates scale-in protection, and a lowering would move it into the warm pool.
    pub(crate) fn release_target(&self, releasing: &InstanceId) -> Result<u32, AwsError> {
        let live = self.live();
        if self.desired >= live {
            let unprotected = self.members.iter().find(|member| {
                member.id != *releasing
                    && member.lifecycle == InstanceLifecycle::InService
                    && !member.protected
            });
            if let Some(member) = unprotected {
                return Err(AwsError::Config {
                    field: "scale_in_protection",
                    reason: format!(
                        "group {} has instance {} unprotected from scale in because it launched before protection was enabled; replace it before the group can release instances",
                        self.name,
                        member.id.as_str(),
                    ),
                });
            }
        }
        let keepers = self
            .members
            .iter()
            .filter(|member| member.id != *releasing && is_kept(member))
            .count();
        let keepers = u32::try_from(keepers).unwrap_or(u32::MAX);
        let in_flight = self.desired.saturating_sub(live);
        Ok(keepers.saturating_add(in_flight))
    }
}

impl From<&Group> for Counts {
    /// Counts the group's desired capacity and its live members.
    fn from(group: &Group) -> Self {
        Self {
            desired: group.desired,
            live: group.live(),
        }
    }
}

/// Converts one listed instance of a group to a member.
fn member(instance: &AutoScalingInstance) -> Result<Member, AwsError> {
    let raw_id = instance
        .instance_id()
        .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "instance id"))?;
    let id = InstanceId::try_new(raw_id)
        .map_err(|_| AwsError::malformed(DESCRIBE_GROUPS, "instance id"))?;
    let state = instance
        .lifecycle_state()
        .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "lifecycle state"))?;
    Ok(Member {
        id,
        lifecycle: lifecycle_of(state),
        protected: instance.protected_from_scale_in().unwrap_or_default(),
    })
}

/// Whether a member counts toward its group's live capacity.
fn is_live(member: &Member) -> bool {
    matches!(
        member.lifecycle,
        InstanceLifecycle::Pending | InstanceLifecycle::InService
    )
}

/// Whether a member stays in its group when another member is released.
fn is_kept(member: &Member) -> bool {
    match member.lifecycle {
        InstanceLifecycle::Pending => true,
        InstanceLifecycle::InService => member.protected,
        InstanceLifecycle::Terminating => false,
    }
}

/// The error for a group the response does not list: the name must be a fleet name to be
/// reported as missing, and a name that cannot be one is a configuration error.
fn missing(name: &str) -> AwsError {
    match FleetName::try_new(name) {
        Ok(fleet) => AwsError::MissingGroup(fleet),
        Err(error) => AwsError::Config {
            field: "fleet",
            reason: format!("{name} is not a valid fleet name: {error}"),
        },
    }
}

/// Converts an auto scaling capacity to a group size; a negative capacity is an empty group.
fn capacity_of(desired: i32) -> u32 {
    u32::try_from(desired).unwrap_or_default()
}

/// Maps an auto scaling lifecycle state to the provisioner's view of the instance.
pub(crate) fn lifecycle_of(state: &LifecycleState) -> InstanceLifecycle {
    match state {
        LifecycleState::Pending | LifecycleState::PendingWait | LifecycleState::PendingProceed => {
            InstanceLifecycle::Pending
        }
        LifecycleState::InService => InstanceLifecycle::InService,
        _ => InstanceLifecycle::Terminating,
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput;
    use aws_sdk_autoscaling::types::{
        AutoScalingGroup, Instance, InstanceReusePolicy, LifecycleState, WarmPoolConfiguration,
    };
    use gang_types::InstanceId;

    use super::{AwsError, Group, InstanceLifecycle, lifecycle_of};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const GROUP: &str = "gang-fleet";

    fn instance(id: &str, state: &str, protected: bool) -> Instance {
        Instance::builder()
            .instance_id(id)
            .lifecycle_state(LifecycleState::from(state))
            .protected_from_scale_in(protected)
            .build()
    }

    /// A group that reuses warm instances on scale-in, with new instances protected or not.
    fn reuse_group(desired: i32, instances: Vec<Instance>, protects: bool) -> AutoScalingGroup {
        let policy = InstanceReusePolicy::builder()
            .reuse_on_scale_in(true)
            .build();
        AutoScalingGroup::builder()
            .auto_scaling_group_name(GROUP)
            .desired_capacity(desired)
            .new_instances_protected_from_scale_in(protects)
            .warm_pool_configuration(
                WarmPoolConfiguration::builder()
                    .instance_reuse_policy(policy)
                    .build(),
            )
            .set_instances(Some(instances))
            .build()
    }

    fn read(group: AutoScalingGroup) -> Result<Group, AwsError> {
        let output = DescribeAutoScalingGroupsOutput::builder()
            .auto_scaling_groups(group)
            .build();
        Group::read(&output, GROUP)
    }

    #[test]
    fn lifecycle_maps_each_state_family() {
        let cases = [
            (LifecycleState::Pending, InstanceLifecycle::Pending),
            (LifecycleState::PendingWait, InstanceLifecycle::Pending),
            (LifecycleState::PendingProceed, InstanceLifecycle::Pending),
            (LifecycleState::InService, InstanceLifecycle::InService),
            (
                LifecycleState::from("InServiceX"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("in service"),
                InstanceLifecycle::Terminating,
            ),
            (LifecycleState::Terminating, InstanceLifecycle::Terminating),
            (
                LifecycleState::from("Terminating:Wait"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("Terminated"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("Detaching"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("Detached"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("EnteringStandby"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("Standby"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("Warmed:Stopped"),
                InstanceLifecycle::Terminating,
            ),
            (
                LifecycleState::from("Unknown"),
                InstanceLifecycle::Terminating,
            ),
            (LifecycleState::from(""), InstanceLifecycle::Terminating),
        ];
        for (state, expected) in cases {
            assert_eq!(lifecycle_of(&state), expected, "state {state:?}");
        }
    }

    #[test]
    fn reported_is_the_larger_of_desired_and_live() -> TestResult {
        let group = read(reuse_group(
            1,
            vec![
                instance("i-a", "InService", true),
                instance("i-b", "Pending", false),
                instance("i-c", "Terminating:Wait", false),
            ],
            true,
        ))?;
        assert_eq!(group.live(), 2);
        assert_eq!(group.reported(), 2);

        let group = read(reuse_group(
            5,
            vec![instance("i-a", "InService", true)],
            true,
        ))?;
        assert_eq!(group.reported(), 5);

        let group = read(reuse_group(
            -3,
            vec![instance("i-c", "Terminating:Wait", false)],
            true,
        ))?;
        assert_eq!(group.desired, 0);
        assert_eq!(group.reported(), 0);
        Ok(())
    }

    #[test]
    fn release_target_drops_the_releasing_member() -> TestResult {
        let group = read(reuse_group(
            2,
            vec![
                instance("i-a", "InService", true),
                instance("i-b", "InService", true),
            ],
            true,
        ))?;
        assert_eq!(group.release_target(&InstanceId::try_new("i-a")?)?, 1);
        Ok(())
    }

    #[test]
    fn release_target_keeps_in_flight_scale_out() -> TestResult {
        let group = read(reuse_group(
            4,
            vec![
                instance("i-a", "InService", true),
                instance("i-b", "InService", true),
            ],
            true,
        ))?;
        assert_eq!(group.release_target(&InstanceId::try_new("i-a")?)?, 3);
        Ok(())
    }

    #[test]
    fn release_target_is_unchanged_once_desired_is_lowered() -> TestResult {
        let group = read(reuse_group(
            1,
            vec![instance("i-b", "InService", true)],
            true,
        ))?;
        let target = group.release_target(&InstanceId::try_new("i-a")?)?;
        assert_eq!(target, group.desired);
        Ok(())
    }

    #[test]
    fn release_target_keeps_pending_members() -> TestResult {
        let group = read(reuse_group(
            2,
            vec![
                instance("i-a", "InService", true),
                instance("i-p", "Pending", false),
            ],
            true,
        ))?;
        assert_eq!(group.release_target(&InstanceId::try_new("i-a")?)?, 1);
        Ok(())
    }

    #[test]
    fn release_target_refuses_unprotected_member_without_a_lowering() -> TestResult {
        let group = read(reuse_group(
            2,
            vec![
                instance("i-a", "InService", true),
                instance("i-b", "InService", false),
            ],
            true,
        ))?;
        let Err(AwsError::Config { field, reason }) =
            group.release_target(&InstanceId::try_new("i-a")?)
        else {
            return Err("expected a config error".into());
        };
        assert_eq!(field, "scale_in_protection");
        assert!(reason.contains(GROUP), "reason {reason:?}");
        assert!(reason.contains("i-b"), "reason {reason:?}");
        Ok(())
    }

    #[test]
    fn release_target_allows_unprotected_member_while_a_release_is_in_flight() -> TestResult {
        let group = read(reuse_group(
            1,
            vec![
                instance("i-a", "InService", true),
                instance("i-b", "InService", false),
            ],
            true,
        ))?;
        assert_eq!(group.release_target(&InstanceId::try_new("i-a")?)?, 0);
        Ok(())
    }

    #[test]
    fn reuse_without_protected_new_instances_is_a_config_error() -> TestResult {
        let Err(AwsError::Config { reason, .. }) = read(reuse_group(
            1,
            vec![instance("i-a", "InService", false)],
            false,
        )) else {
            return Err("expected a config error".into());
        };
        assert!(reason.contains(GROUP), "reason {reason:?}");
        Ok(())
    }

    #[test]
    fn a_missing_group_is_a_missing_group_error() {
        let output = DescribeAutoScalingGroupsOutput::builder().build();
        assert!(matches!(
            Group::read(&output, GROUP),
            Err(AwsError::MissingGroup(_))
        ));
    }
}
