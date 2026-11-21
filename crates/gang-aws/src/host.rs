//! Controller host retirement and Auto Scaling lifecycle hook hand-off.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

use aws_sdk_autoscaling::types::{AutoScalingGroup, LifecycleState};
use gang_aws_sdk::call_error;
use gang_spi::{
    ControllerHostIdentity, ControllerHostPort, ControllerHostState, HostMilestone, PortError,
};

use crate::errors::AwsError;
use crate::group::DESCRIBE_GROUPS;
use crate::plugin::{Aws, Runtime};

const DESCRIBE_HOOKS: &str = "DescribeLifecycleHooks";
const REGISTER_TARGETS: &str = "RegisterTargets";
const DESCRIBE_TARGET_HEALTH: &str = "DescribeTargetHealth";
const COMPLETE_ACTION: &str = "CompleteLifecycleAction";
const HEARTBEAT: &str = "RecordLifecycleActionHeartbeat";
const LAUNCH_TRANSITION: &str = "autoscaling:EC2_INSTANCE_LAUNCHING";
const TERMINATE_TRANSITION: &str = "autoscaling:EC2_INSTANCE_TERMINATING";
const CONTINUE: &str = "CONTINUE";
#[cfg(test)]
pub(crate) const HOOK_HEARTBEAT_SECONDS: u32 = 3600;
const MAX_HOOK_SECONDS: u64 = 48 * 60 * 60;

async fn group(runtime: &Runtime, name: &str) -> Result<AutoScalingGroup, AwsError> {
    let response = runtime
        .autoscaling
        .describe_auto_scaling_groups()
        .auto_scaling_group_names(name)
        .send()
        .await
        .map_err(|error| AwsError::from(call_error(DESCRIBE_GROUPS, error)))?;
    response
        .auto_scaling_groups()
        .iter()
        .find(|group| group.auto_scaling_group_name() == Some(name))
        .cloned()
        .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "controller group"))
}

async fn lifecycle_optional(
    runtime: &Runtime,
    group: &AutoScalingGroup,
) -> Result<Option<LifecycleState>, AwsError> {
    let instance = runtime.metadata.instance_id().await?;
    let Some(member) = group
        .instances()
        .iter()
        .find(|candidate| candidate.instance_id() == Some(instance.as_str()))
    else {
        return Ok(None);
    };
    member
        .lifecycle_state()
        .cloned()
        .map(Some)
        .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "controller instance lifecycle state"))
}

async fn lifecycle(
    runtime: &Runtime,
    group: &AutoScalingGroup,
) -> Result<LifecycleState, AwsError> {
    lifecycle_optional(runtime, group)
        .await?
        .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "controller instance lifecycle state"))
}

fn termination_completed(state: &LifecycleState) -> bool {
    matches!(
        state.as_str(),
        "Terminating:Proceed" | "Terminating:Retained" | "Terminated"
    )
}

async fn hook(
    runtime: &Runtime,
    group: &str,
    transition: &str,
) -> Result<Option<aws_sdk_autoscaling::types::LifecycleHook>, AwsError> {
    let hooks = runtime
        .autoscaling
        .describe_lifecycle_hooks()
        .auto_scaling_group_name(group)
        .send()
        .await
        .map_err(|error| AwsError::from(call_error(DESCRIBE_HOOKS, error)))?;
    Ok(hooks
        .lifecycle_hooks()
        .iter()
        .find(|hook| hook.lifecycle_transition() == Some(transition))
        .cloned())
}

async fn group_name(runtime: &Runtime) -> Result<Option<String>, AwsError> {
    let name = runtime.metadata.group_name().await?;
    if name.is_none() && runtime.metadata.retiring().await?.is_some() {
        return Err(AwsError::malformed("IMDS GET", "auto scaling group tag"));
    }
    Ok(name)
}

impl Aws {
    async fn prepared(&self, runtime: &Runtime, name: &str) -> Result<(), PortError> {
        let instance = runtime.metadata.instance_id().await?;
        let mut requested = HashSet::new();
        loop {
            if self.state().await? != ControllerHostState::Active {
                return Err(AwsError::malformed(
                    DESCRIBE_GROUPS,
                    "eligible controller for target registration",
                )
                .into());
            }
            let group = group(runtime, name).await?;
            let mut ready = true;
            for arn in group.target_group_arns() {
                let response = runtime
                    .elbv2
                    .describe_target_health()
                    .target_group_arn(arn)
                    .send()
                    .await
                    .map_err(|error| AwsError::from(call_error(DESCRIBE_TARGET_HEALTH, error)))?;
                let own = response
                    .target_health_descriptions()
                    .iter()
                    .find(|description| {
                        description.target().and_then(|target| target.id())
                            == Some(instance.as_str())
                    });
                if let Some(description) = own {
                    let health = description
                        .target_health()
                        .and_then(|health| health.state())
                        .ok_or_else(|| {
                            AwsError::malformed(DESCRIBE_TARGET_HEALTH, "target health")
                        })?;
                    if !matches!(health.as_str(), "healthy" | "unhealthy") {
                        ready = false;
                    }
                } else {
                    ready = false;
                    if !requested.contains(arn) {
                        if self.state().await? != ControllerHostState::Active {
                            return Err(AwsError::malformed(
                                DESCRIBE_GROUPS,
                                "eligible controller for target registration",
                            )
                            .into());
                        }
                        let target =
                            aws_sdk_elasticloadbalancingv2::types::TargetDescription::builder()
                                .id(instance.as_str())
                                .build();
                        runtime
                            .elbv2
                            .register_targets()
                            .target_group_arn(arn)
                            .targets(target)
                            .send()
                            .await
                            .map_err(|error| AwsError::from(call_error(REGISTER_TARGETS, error)))?;
                        requested.insert(arn.clone());
                    }
                }
            }
            if ready {
                return Ok(());
            }
            self.clock
                .sleep(self.config.retirement_poll_interval.get())
                .await;
        }
    }

    async fn unrouted(&self, runtime: &Runtime, name: &str) -> Result<(), PortError> {
        let instance = runtime.metadata.instance_id().await?;
        loop {
            let group = group(runtime, name).await?;
            let mut unrouted = true;
            for arn in group.target_group_arns() {
                let response = runtime
                    .elbv2
                    .describe_target_health()
                    .target_group_arn(arn)
                    .send()
                    .await
                    .map_err(|error| AwsError::from(call_error(DESCRIBE_TARGET_HEALTH, error)))?;
                let mut own_other = false;
                let mut own_unhealthy = false;
                let mut healthy_other = false;
                for description in response.target_health_descriptions() {
                    let target = description
                        .target()
                        .and_then(|target| target.id())
                        .ok_or_else(|| AwsError::malformed(DESCRIBE_TARGET_HEALTH, "target id"))?;
                    let health = description
                        .target_health()
                        .and_then(|health| health.state())
                        .ok_or_else(|| {
                            AwsError::malformed(DESCRIBE_TARGET_HEALTH, "target health")
                        })?;
                    if target == instance.as_str() {
                        match health.as_str() {
                            "draining" | "unused" => {}
                            "unhealthy" => own_unhealthy = true,
                            _ => own_other = true,
                        }
                    } else if health.as_str() == "healthy" {
                        healthy_other = true;
                    }
                }
                // A missing target is already deregistered. An unhealthy target can receive
                // fail-open traffic unless another target in its group is healthy.
                if own_other || (own_unhealthy && !healthy_other) {
                    unrouted = false;
                }
            }
            if unrouted {
                return Ok(());
            }
            self.clock
                .sleep(self.config.retirement_poll_interval.get())
                .await;
        }
    }
}

impl ControllerHostPort for Aws {
    async fn incarnation(&self) -> Result<ControllerHostIdentity, PortError> {
        Ok(self.runtime().await?.metadata.incarnation().await?)
    }

    async fn state(&self) -> Result<ControllerHostState, PortError> {
        let runtime = self.runtime().await?;
        if self.retirement_seen.load(Ordering::Acquire) {
            return Ok(ControllerHostState::Retiring);
        }
        let retiring = runtime.metadata.retiring().await?;
        if retiring == Some(true) {
            self.retirement_seen.store(true, Ordering::Release);
        }
        let Some(name) = runtime.metadata.group_name().await? else {
            return if retiring.is_none() {
                Ok(ControllerHostState::Active)
            } else {
                Err(AwsError::malformed("IMDS GET", "auto scaling group tag").into())
            };
        };
        let retiring =
            retiring.ok_or_else(|| AwsError::malformed("IMDS GET", "target lifecycle state"))?;
        let lifecycle = lifecycle(runtime, &group(runtime, &name).await?).await?;
        let retiring = retiring
            || lifecycle.as_str().starts_with("Terminating")
            || lifecycle.as_str() == "Terminated";
        if retiring {
            self.retirement_seen.store(true, Ordering::Release);
            return Ok(ControllerHostState::Retiring);
        }
        match lifecycle.as_str() {
            "Pending" | "Pending:Wait" | "Pending:Proceed" | "InService" => {
                Ok(ControllerHostState::Active)
            }
            _ => Err(
                AwsError::malformed(DESCRIBE_GROUPS, "eligible controller lifecycle state").into(),
            ),
        }
    }

    async fn drain_limit(&self) -> Result<Option<Duration>, PortError> {
        let runtime = self.runtime().await?;
        let Some(name) = group_name(runtime).await? else {
            return Ok(None);
        };
        let hook = hook(runtime, &name, TERMINATE_TRANSITION)
            .await?
            .ok_or_else(|| AwsError::malformed(DESCRIBE_HOOKS, "termination lifecycle hook"))?;
        let seconds = hook
            .heartbeat_timeout()
            .and_then(|n| u64::try_from(n).ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| AwsError::malformed(DESCRIBE_HOOKS, "heartbeat timeout"))?;
        Ok(Some(Duration::from_secs(
            MAX_HOOK_SECONDS.min(seconds.saturating_mul(100)),
        )))
    }

    async fn heartbeat(&self) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        let Some(name) = group_name(runtime).await? else {
            return Ok(());
        };
        let Some(state) = lifecycle_optional(runtime, &group(runtime, &name).await?).await? else {
            return Ok(());
        };
        if termination_completed(&state) {
            return Ok(());
        }
        if state != LifecycleState::TerminatingWait {
            if !matches!(
                state.as_str(),
                "Pending" | "Pending:Wait" | "Pending:Proceed" | "InService" | "Terminating"
            ) {
                return Err(AwsError::malformed(
                    DESCRIBE_GROUPS,
                    "outstanding termination lifecycle action",
                )
                .into());
            }
            return Ok(());
        }
        let hook = hook(runtime, &name, TERMINATE_TRANSITION)
            .await?
            .ok_or_else(|| AwsError::malformed(DESCRIBE_HOOKS, "termination lifecycle hook"))?;
        let hook_name = hook
            .lifecycle_hook_name()
            .ok_or_else(|| AwsError::malformed(DESCRIBE_HOOKS, "lifecycle hook name"))?;
        let instance = runtime.metadata.instance_id().await?;
        runtime
            .autoscaling
            .record_lifecycle_action_heartbeat()
            .auto_scaling_group_name(&name)
            .lifecycle_hook_name(hook_name)
            .instance_id(instance.as_str())
            .send()
            .await
            .map_err(|error| AwsError::from(call_error(HEARTBEAT, error)))?;
        Ok(())
    }

    async fn retiring(&self) -> Result<(), PortError> {
        loop {
            match self.state().await {
                Ok(ControllerHostState::Retiring) => return Ok(()),
                Ok(ControllerHostState::Active) => {}
                Err(error @ PortError::Unavailable(_)) => {
                    tracing::warn!(%error, "controller retirement read failed; retrying");
                }
                Err(error) => return Err(error),
            }
            self.clock
                .sleep(self.config.retirement_poll_interval.get())
                .await;
        }
    }

    async fn reached(&self, milestone: HostMilestone) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        let Some(name) = group_name(runtime).await? else {
            return Ok(());
        };
        if milestone == HostMilestone::Prepared {
            return self.prepared(runtime, &name).await;
        }
        if milestone == HostMilestone::Unrouted {
            return self.unrouted(runtime, &name).await;
        }
        let transition = if milestone == HostMilestone::Serving {
            LAUNCH_TRANSITION
        } else {
            TERMINATE_TRANSITION
        };
        if milestone == HostMilestone::HandedOff {
            let state = lifecycle_optional(runtime, &group(runtime, &name).await?).await?;
            if state.as_ref().is_none_or(termination_completed) {
                return Ok(());
            }
            if self.state().await? == ControllerHostState::Active {
                return Ok(());
            }
        }
        let hook = hook(runtime, &name, transition).await?;
        let Some(hook) = hook else {
            return if milestone == HostMilestone::Serving {
                Ok(())
            } else {
                Err(AwsError::malformed(DESCRIBE_HOOKS, "termination lifecycle hook").into())
            };
        };
        let hook_name = hook
            .lifecycle_hook_name()
            .ok_or_else(|| AwsError::malformed(DESCRIBE_HOOKS, "lifecycle hook name"))?;
        let instance = runtime.metadata.instance_id().await?;
        loop {
            let Some(state) = lifecycle_optional(runtime, &group(runtime, &name).await?).await?
            else {
                return if milestone == HostMilestone::HandedOff {
                    Ok(())
                } else {
                    Err(
                        AwsError::malformed(DESCRIBE_GROUPS, "controller instance lifecycle state")
                            .into(),
                    )
                };
            };
            let expected = if milestone == HostMilestone::Serving {
                LifecycleState::PendingWait
            } else {
                LifecycleState::TerminatingWait
            };
            if state == expected {
                break;
            }
            if milestone == HostMilestone::Serving {
                return Ok(());
            }
            if termination_completed(&state) {
                return Ok(());
            }
            if !matches!(state.as_str(), "InService" | "Terminating") {
                return Err(AwsError::malformed(
                    DESCRIBE_GROUPS,
                    "outstanding termination lifecycle action",
                )
                .into());
            }
            self.clock
                .sleep(self.config.retirement_poll_interval.get())
                .await;
        }
        runtime
            .autoscaling
            .complete_lifecycle_action()
            .auto_scaling_group_name(&name)
            .lifecycle_hook_name(hook_name)
            .instance_id(instance.as_str())
            .lifecycle_action_result(CONTINUE)
            .send()
            .await
            .map_err(|error| AwsError::from(call_error(COMPLETE_ACTION, error)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::{Clock, TestClock, Timestamp};
    use gang_spi::CandidatePriority;
    use gang_testkit::TestResult;
    use gang_types::InstanceId;
    use serde_json::json;

    use super::*;
    use crate::config::AwsConfig;
    use crate::fake_aws::{FakeAws, FakeFailure, FakeGroup};
    use crate::plugin::Runtime;

    const GROUP: &str = "controllers";
    const INSTANCE: &str = "i-controller";

    async fn setup() -> Result<(FakeAws, Aws, Arc<TestClock>), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        let clock = Arc::new(TestClock::new(Timestamp::UNIX_EPOCH));
        let aws = aws_for(&fake, clock.clone()).await?;
        Ok((fake, aws, clock))
    }

    async fn aws_for(
        fake: &FakeAws,
        clock: Arc<TestClock>,
    ) -> Result<Aws, Box<dyn std::error::Error>> {
        let config: AwsConfig = serde_json::from_value(
            json!({"region":"us-east-1", "scratch_mount_point":"/mnt/scratch"}),
        )?;
        let runtime = Runtime::from_sdk(
            &fake.sdk_config().await,
            fake.imds_client()?,
            &config,
            clock.clone(),
            &Arc::new(crate::events::Managed::default()),
        );
        let mut aws = Aws::with_runtime(config, runtime);
        aws.clock = clock.clone();
        Ok(aws)
    }

    fn seed(fake: &FakeAws) {
        let mut state = fake.state();
        state
            .groups
            .push(FakeGroup::new(GROUP, 1, &[(INSTANCE, "Pending:Wait")]));
        state.group_tag = Some(GROUP.to_owned());
        state.target_lifecycle_state = Some("InService".to_owned());
        state.identity_document =
            Some(json!({"instanceId": INSTANCE, "architecture": "x86_64"}).to_string());
        state.lifecycle_hooks.insert(
            GROUP.to_owned(),
            vec![
                ("launch".to_owned(), LAUNCH_TRANSITION.to_owned()),
                ("terminate".to_owned(), TERMINATE_TRANSITION.to_owned()),
            ],
        );
    }

    #[tokio::test]
    async fn preparation_registers_own_instance_and_waits_for_each_group() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        seed(&fake);
        {
            let mut state = fake.state();
            state.groups[0].target_group_arns = vec!["first".into(), "second".into()];
            state.target_health.insert("first".into(), Vec::new());
            state.target_health.insert("second".into(), Vec::new());
            state.target_health.insert("unattached".into(), Vec::new());
        }
        let task =
            tokio::spawn(async move { Box::pin(aws.reached(HostMilestone::Prepared)).await });
        while fake
            .state()
            .requests
            .iter()
            .filter(|request| request.operation == REGISTER_TARGETS)
            .count()
            < 2
        {
            tokio::task::yield_now().await;
        }
        assert!(!task.is_finished());
        {
            let mut state = fake.state();
            let registrations: Vec<_> = state
                .requests
                .iter()
                .filter(|request| request.operation == REGISTER_TARGETS)
                .collect();
            assert_eq!(registrations.len(), 2);
            assert!(registrations.iter().all(|request| {
                request
                    .parameters
                    .get("Targets.member.1.Id")
                    .map(String::as_str)
                    == Some(INSTANCE)
            }));
            assert!(
                registrations
                    .iter()
                    .all(|request| !request.parameters.contains_key("Targets.member.1.Port"))
            );
            assert!(state.lifecycle_completions.is_empty());
            assert!(state.target_health["unattached"].is_empty());
            state.target_health.get_mut("first").expect("seeded group")[0].1 = "unhealthy".into();
            state.target_health.get_mut("second").expect("seeded group")[0].1 = "healthy".into();
        }
        while !task.is_finished() {
            clock.advance(Duration::from_secs(5));
            tokio::task::yield_now().await;
        }
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn preparation_preserves_existing_registration_and_rejects_retiring_hosts() -> TestResult
    {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        {
            let mut state = fake.state();
            state.groups[0].target_group_arns = vec!["tg".into()];
            state
                .target_health
                .insert("tg".into(), vec![(INSTANCE.into(), "unhealthy".into())]);
        }
        Box::pin(aws.reached(HostMilestone::Prepared)).await?;
        Box::pin(aws.reached(HostMilestone::Prepared)).await?;
        assert!(
            !fake
                .state()
                .requests
                .iter()
                .any(|request| request.operation == REGISTER_TARGETS)
        );
        assert_eq!(fake.state().target_health["tg"][0].1, "unhealthy");
        fake.state().groups[0].instances[0].lifecycle_state = "Terminating:Wait".into();
        assert!(
            Box::pin(aws.reached(HostMilestone::Prepared))
                .await
                .is_err()
        );
        assert!(
            !fake
                .state()
                .requests
                .iter()
                .any(|request| request.operation == REGISTER_TARGETS)
        );
        Ok(())
    }

    #[tokio::test]
    async fn incarnation_is_cached_restart_stable_and_needs_no_worker_fleet_tag() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        let launched = "2026-01-01T00:00:00Z";
        fake.state().identity_document =
            Some(json!({"instanceId": INSTANCE, "pendingTime": launched}).to_string());
        let expected = ControllerHostIdentity::Launched {
            at: Timestamp::from_rfc3339(launched)?,
            instance: InstanceId::try_new(INSTANCE)?,
        };
        assert_eq!(aws.incarnation().await?, expected);
        assert_eq!(aws.incarnation().await?, expected);
        let document_reads = || {
            fake.state()
                .requests
                .iter()
                .filter(|request| {
                    request.operation == "ImdsGet"
                        && request.parameters.get("path").map(String::as_str)
                            == Some("/latest/dynamic/instance-identity/document")
                })
                .count()
        };
        assert_eq!(document_reads(), 1);
        let restarted = aws_for(&fake, clock).await?;
        assert_eq!(restarted.incarnation().await?, expected);
        assert_eq!(document_reads(), 2);
        assert!(!fake.state().requests.iter().any(|request| {
            request.operation == "ImdsGet"
                && request
                    .parameters
                    .get("path")
                    .is_some_and(|path| path.starts_with("/latest/meta-data/tags/instance/"))
        }));
        fake.state().identity_document = None;
        assert_eq!(aws.incarnation().await?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn missing_or_malformed_controller_launch_time_is_permanent() -> TestResult {
        let (fake, aws, _) = setup().await?;
        for document in [
            json!({"instanceId": INSTANCE}),
            json!({"instanceId": INSTANCE, "pendingTime": "not-a-time"}),
            json!({"instanceId": INSTANCE, "pendingTime": "1969-12-31T23:59:59Z"}),
            json!({"instanceId": INSTANCE, "pendingTime": 42}),
        ] {
            fake.state().identity_document = Some(document.to_string());
            assert!(matches!(
                aws.incarnation().await,
                Err(PortError::Backend(_))
            ));
        }
        fake.state().identity_document = Some(
            json!({"instanceId": INSTANCE, "pendingTime": "2026-01-01T00:00:00Z"}).to_string(),
        );
        assert_eq!(
            aws.incarnation().await?,
            ControllerHostIdentity::Launched {
                at: Timestamp::from_rfc3339("2026-01-01T00:00:00Z")?,
                instance: InstanceId::try_new(INSTANCE)?,
            }
        );
        Ok(())
    }

    async fn host_priority(aws: &Aws) -> Result<CandidatePriority, Box<dyn std::error::Error>> {
        match aws.incarnation().await? {
            ControllerHostIdentity::Launched { at, instance } => {
                Ok(CandidatePriority::launched(at, &instance))
            }
            ControllerHostIdentity::Persistent(_) => {
                Err("AWS returned a persistent identity".into())
            }
        }
    }

    #[tokio::test]
    async fn immutable_launch_time_then_instance_id_totally_order_controller_hosts() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        fake.state().identity_document =
            Some(json!({"instanceId": "i-z", "pendingTime": "2026-01-01T00:00:00Z"}).to_string());
        let old = host_priority(&aws).await?;
        fake.state().identity_document =
            Some(json!({"instanceId": "i-a", "pendingTime": "2026-01-01T00:00:01Z"}).to_string());
        let newer = host_priority(&aws_for(&fake, clock.clone()).await?).await?;
        fake.state().identity_document =
            Some(json!({"instanceId": "i-b", "pendingTime": "2026-01-01T00:00:01Z"}).to_string());
        let tied = host_priority(&aws_for(&fake, clock.clone()).await?).await?;
        assert!(old < newer);
        assert!(newer < tied);
        fake.state().identity_document =
            Some(json!({"instanceId": "i-z", "pendingTime": "2026-01-01T00:00:00Z"}).to_string());
        assert_eq!(host_priority(&aws_for(&fake, clock).await?).await?, old);
        Ok(())
    }

    #[tokio::test]
    async fn milestones_complete_only_the_matching_hook() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        Box::pin(aws.reached(HostMilestone::Serving)).await?;
        assert_eq!(
            fake.state().lifecycle_completions,
            vec![(GROUP.to_owned(), "launch".to_owned(), INSTANCE.to_owned())]
        );
        {
            let mut state = fake.state();
            state.target_lifecycle_state = Some("Terminated".to_owned());
            state.groups[0].instances[0].lifecycle_state = "Terminating:Wait".to_owned();
        }
        Box::pin(aws.reached(HostMilestone::HandedOff)).await?;
        let state = fake.state();
        assert_eq!(
            state.lifecycle_completions,
            vec![
                (GROUP.to_owned(), "launch".to_owned(), INSTANCE.to_owned()),
                (
                    GROUP.to_owned(),
                    "terminate".to_owned(),
                    INSTANCE.to_owned()
                )
            ]
        );
        assert!(
            state
                .requests
                .iter()
                .filter(|request| request.operation == COMPLETE_ACTION)
                .all(|request| request
                    .parameters
                    .get("LifecycleActionResult")
                    .is_some_and(|value| value == CONTINUE))
        );
        Ok(())
    }

    #[tokio::test]
    async fn serving_restart_on_in_service_does_not_complete_launch_hook() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        seed(&fake);
        Box::pin(aws.reached(HostMilestone::Serving)).await?;
        fake.state().promote(GROUP, INSTANCE);

        let restarted = aws_for(&fake, clock).await?;
        Box::pin(restarted.reached(HostMilestone::Serving)).await?;

        let state = fake.state();
        assert_eq!(
            state
                .requests
                .iter()
                .filter(|request| request.operation == COMPLETE_ACTION)
                .count(),
            1
        );
        assert_eq!(
            state.lifecycle_completions,
            vec![(GROUP.to_owned(), "launch".to_owned(), INSTANCE.to_owned())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn serving_on_in_service_does_not_complete_launch_hook() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().promote(GROUP, INSTANCE);

        Box::pin(aws.reached(HostMilestone::Serving)).await?;

        let state = fake.state();
        assert!(
            !state
                .requests
                .iter()
                .any(|request| request.operation == COMPLETE_ACTION)
        );
        assert!(state.lifecycle_completions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn serving_skips_other_lifecycle_states() -> TestResult {
        for lifecycle in ["Pending", "Pending:Proceed", "Terminating:Wait"] {
            let (fake, aws, _) = setup().await?;
            seed(&fake);
            fake.state().groups[0].instances[0].lifecycle_state = lifecycle.to_owned();

            Box::pin(aws.reached(HostMilestone::Serving)).await?;

            let state = fake.state();
            assert!(
                !state
                    .requests
                    .iter()
                    .any(|request| request.operation == COMPLETE_ACTION),
                "completed lifecycle action in {lifecycle}"
            );
            assert!(state.lifecycle_completions.is_empty());
        }
        Ok(())
    }

    #[tokio::test]
    async fn serving_checks_the_controller_in_the_requested_group() -> TestResult {
        for (controller_state, peer_state, completes) in [
            ("Pending:Wait", "InService", true),
            ("InService", "Pending:Wait", false),
        ] {
            let (fake, aws, _) = setup().await?;
            seed(&fake);
            {
                let mut state = fake.state();
                state.groups = vec![
                    FakeGroup::new("other", 1, &[("i-other", "Pending:Wait")]),
                    FakeGroup::new(
                        GROUP,
                        2,
                        &[("i-peer", peer_state), (INSTANCE, controller_state)],
                    ),
                ];
            }

            Box::pin(aws.reached(HostMilestone::Serving)).await?;

            let state = fake.state();
            let describe = state
                .requests
                .iter()
                .find(|request| request.operation == DESCRIBE_GROUPS)
                .expect("serving describes the controller group");
            assert_eq!(
                describe
                    .parameters
                    .get("AutoScalingGroupNames.member.1")
                    .map(String::as_str),
                Some(GROUP)
            );
            assert_eq!(
                state
                    .requests
                    .iter()
                    .filter(|request| request.operation == COMPLETE_ACTION)
                    .count(),
                usize::from(completes)
            );
            let expected = if completes {
                vec![(GROUP.to_owned(), "launch".to_owned(), INSTANCE.to_owned())]
            } else {
                Vec::new()
            };
            assert_eq!(state.lifecycle_completions, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn serving_propagates_transient_group_read_failure() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().failures.insert(
            DESCRIBE_GROUPS,
            FakeFailure {
                status: 503,
                code: "ServiceUnavailable",
                message: "unavailable",
            },
        );

        assert!(matches!(
            Box::pin(aws.reached(HostMilestone::Serving)).await,
            Err(PortError::Unavailable(_))
        ));
        assert!(
            !fake
                .state()
                .requests
                .iter()
                .any(|request| request.operation == COMPLETE_ACTION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn serving_propagates_group_read_refusals() -> TestResult {
        for (status, code) in [(403, "AccessDenied"), (400, "ValidationError")] {
            let (fake, aws, _) = setup().await?;
            seed(&fake);
            fake.state().failures.insert(
                DESCRIBE_GROUPS,
                FakeFailure {
                    status,
                    code,
                    message: "denied",
                },
            );

            assert!(matches!(
                Box::pin(aws.reached(HostMilestone::Serving)).await,
                Err(PortError::Backend(_))
            ));
            assert!(
                !fake
                    .state()
                    .requests
                    .iter()
                    .any(|request| request.operation == COMPLETE_ACTION)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn serving_rejects_missing_group_or_member() -> TestResult {
        for missing_group in [true, false] {
            let (fake, aws, _) = setup().await?;
            seed(&fake);
            {
                let mut state = fake.state();
                if missing_group {
                    state.groups.clear();
                } else {
                    state.groups[0].instances.clear();
                }
            }

            assert!(matches!(
                Box::pin(aws.reached(HostMilestone::Serving)).await,
                Err(PortError::Backend(_))
            ));
            assert!(
                !fake
                    .state()
                    .requests
                    .iter()
                    .any(|request| request.operation == COMPLETE_ACTION)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_identity_blocks_completion_until_recovery() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().identity_document = None;

        assert!(matches!(
            Box::pin(aws.reached(HostMilestone::Serving)).await,
            Err(PortError::Backend(_))
        ));
        {
            let state = fake.state();
            assert!(
                state
                    .requests
                    .iter()
                    .any(|request| request.operation == DESCRIBE_HOOKS)
            );
            assert!(
                !state
                    .requests
                    .iter()
                    .any(|request| request.operation == COMPLETE_ACTION)
            );
            assert!(state.lifecycle_completions.is_empty());
        }

        fake.state().identity_document =
            Some(json!({"instanceId": INSTANCE, "architecture": "x86_64"}).to_string());
        Box::pin(aws.reached(HostMilestone::Serving)).await?;
        let state = fake.state();
        assert_eq!(
            state.lifecycle_completions,
            vec![(GROUP.to_owned(), "launch".to_owned(), INSTANCE.to_owned())]
        );
        let requests: Vec<_> = state
            .requests
            .iter()
            .filter(|request| request.operation == COMPLETE_ACTION)
            .collect();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .parameters
                .get("LifecycleActionResult")
                .is_some_and(|value| value == CONTINUE)
        );
        Ok(())
    }

    #[tokio::test]
    async fn handoff_does_not_complete_termination_for_a_serving_host() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().target_lifecycle_state = Some("InService".to_owned());
        Box::pin(aws.reached(HostMilestone::HandedOff)).await?;
        assert!(fake.state().lifecycle_completions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn outside_group_does_not_call_autoscaling() -> TestResult {
        let (fake, aws, _) = setup().await?;
        Box::pin(aws.reached(HostMilestone::Serving)).await?;
        assert!(
            !fake
                .state()
                .requests
                .iter()
                .any(|request| request.operation == DESCRIBE_HOOKS
                    || request.operation == COMPLETE_ACTION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_hook_does_not_complete_an_action() -> TestResult {
        let (fake, aws, _) = setup().await?;
        fake.state().group_tag = Some(GROUP.to_owned());
        fake.state().lifecycle_hooks.insert(
            GROUP.to_owned(),
            vec![("terminate".to_owned(), TERMINATE_TRANSITION.to_owned())],
        );
        Box::pin(aws.reached(HostMilestone::Serving)).await?;
        assert!(
            !fake
                .state()
                .requests
                .iter()
                .any(|request| request.operation == COMPLETE_ACTION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn refused_hook_call_is_a_backend_error() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().failures.insert(
            DESCRIBE_HOOKS,
            FakeFailure {
                status: 403,
                code: "AccessDenied",
                message: "denied",
            },
        );
        assert!(matches!(
            Box::pin(aws.reached(HostMilestone::Serving)).await,
            Err(PortError::Backend(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn refused_completion_is_a_backend_error() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().failures.insert(
            COMPLETE_ACTION,
            FakeFailure {
                status: 403,
                code: "AccessDenied",
                message: "denied",
            },
        );
        assert!(matches!(
            Box::pin(aws.reached(HostMilestone::Serving)).await,
            Err(PortError::Backend(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn validation_refusal_is_a_backend_error() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().failures.insert(
            COMPLETE_ACTION,
            FakeFailure {
                status: 400,
                code: "ValidationError",
                message: "denied",
            },
        );
        assert!(matches!(
            Box::pin(aws.reached(HostMilestone::Serving)).await,
            Err(PortError::Backend(_))
        ));
        Ok(())
    }

    #[test]
    fn hook_timeout_matches_terraform() -> TestResult {
        let json: serde_json::Value = serde_json::from_str(include_str!(
            "../../../infra/terraform/modules/controller/lifecycle-hook.json"
        ))?;
        assert_eq!(json["heartbeat_timeout_seconds"], HOOK_HEARTBEAT_SECONDS);
        Ok(())
    }

    #[tokio::test]
    async fn drain_limit_uses_actual_hook_and_global_cap() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        assert_eq!(aws.drain_limit().await?, Some(Duration::from_hours(48)));
        fake.state().hook_heartbeat_seconds = 17;
        assert_eq!(aws.drain_limit().await?, Some(Duration::from_secs(1700)));
        fake.state().hook_heartbeat_seconds = 0;
        assert!(aws.drain_limit().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn retirement_state_is_fail_closed_and_sticky() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        assert_eq!(aws.state().await?, ControllerHostState::Active);
        fake.state().target_lifecycle_state = None;
        assert!(aws.state().await.is_err());
        fake.state().target_lifecycle_state = Some("InService".into());
        fake.state().groups[0].instances[0].lifecycle_state = "Terminating:Wait".into();
        assert_eq!(aws.state().await?, ControllerHostState::Retiring);
        fake.state().groups[0].instances[0].lifecycle_state = "InService".into();
        assert_eq!(aws.state().await?, ControllerHostState::Retiring);
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_and_completion_only_touch_waiting_retirements() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        aws.heartbeat().await?;
        assert!(
            !fake
                .state()
                .requests
                .iter()
                .any(|request| request.operation == HEARTBEAT)
        );
        fake.state().groups[0].instances[0].lifecycle_state = "Terminating:Wait".into();
        aws.heartbeat().await?;
        Box::pin(aws.reached(HostMilestone::HandedOff)).await?;
        {
            let state = fake.state();
            let operations: Vec<_> = state
                .requests
                .iter()
                .map(|request| request.operation.as_str())
                .collect();
            let heartbeat = operations
                .iter()
                .position(|operation| *operation == HEARTBEAT)
                .expect("heartbeat sent");
            let completion = operations
                .iter()
                .position(|operation| *operation == COMPLETE_ACTION)
                .expect("completion sent");
            assert!(heartbeat < completion);
            assert_eq!(state.lifecycle_completions.len(), 1);
        }
        Box::pin(aws.reached(HostMilestone::HandedOff)).await?;
        aws.heartbeat().await?;
        Ok(())
    }

    #[tokio::test]
    async fn lost_completion_response_is_recovered_from_platform_progress() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        {
            let mut state = fake.state();
            state.groups[0].instances[0].lifecycle_state = "Terminating:Wait".into();
            state.lose_completion_response = true;
        }
        assert!(matches!(
            Box::pin(aws.reached(HostMilestone::HandedOff)).await,
            Err(PortError::Unavailable(_))
        ));
        assert_eq!(fake.state().lifecycle_completions.len(), 1);
        Box::pin(aws.reached(HostMilestone::HandedOff)).await?;
        aws.heartbeat().await?;
        assert_eq!(
            fake.state()
                .requests
                .iter()
                .filter(|request| request.operation == COMPLETE_ACTION)
                .count(),
            1
        );
        fake.state().groups[0].instances.clear();
        Box::pin(aws.reached(HostMilestone::HandedOff)).await?;
        aws.heartbeat().await?;
        // A fresh process must still fail closed for leadership if its member is absent.
        let restarted = aws_for(&fake, Arc::new(TestClock::new(Timestamp::UNIX_EPOCH))).await?;
        assert!(restarted.state().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn retirement_indication_waits_for_the_actual_hook() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        seed(&fake);
        fake.state().groups[0].instances[0].lifecycle_state = "InService".into();
        fake.state().target_lifecycle_state = Some("Terminated".into());
        let task =
            tokio::spawn(async move { Box::pin(aws.reached(HostMilestone::HandedOff)).await });
        while !fake
            .state()
            .requests
            .iter()
            .any(|request| request.operation == DESCRIBE_HOOKS)
        {
            tokio::task::yield_now().await;
        }
        assert!(fake.state().lifecycle_completions.is_empty());
        fake.state().groups[0].instances[0].lifecycle_state = "Terminating:Wait".into();
        while !task.is_finished() {
            clock.advance(Duration::from_secs(5));
            tokio::task::yield_now().await;
        }
        task.await??;
        assert_eq!(fake.state().lifecycle_completions.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_refusal_is_not_reported_as_success() -> TestResult {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        fake.state().groups[0].instances[0].lifecycle_state = "Terminating:Wait".into();
        fake.state().failures.insert(
            HEARTBEAT,
            FakeFailure {
                status: 403,
                code: "AccessDenied",
                message: "denied",
            },
        );
        assert!(matches!(aws.heartbeat().await, Err(PortError::Backend(_))));
        Ok(())
    }

    #[tokio::test]
    async fn zero_delay_deregistration_is_observed_without_a_timer() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        seed(&fake);
        {
            let mut state = fake.state();
            state.groups[0].target_group_arns = vec!["tg".into()];
            state
                .target_health
                .insert("tg".into(), vec![("i-successor".into(), "healthy".into())]);
        }
        let before = clock.wall();
        Box::pin(aws.reached(HostMilestone::Unrouted)).await?;
        assert_eq!(clock.wall(), before);
        assert!(
            fake.state()
                .requests
                .iter()
                .any(|request| request.operation == DESCRIBE_TARGET_HEALTH)
        );
        Ok(())
    }

    #[tokio::test]
    async fn target_health_is_checked_for_every_attached_group_and_refusals_propagate() -> TestResult
    {
        let (fake, aws, _) = setup().await?;
        seed(&fake);
        {
            let mut state = fake.state();
            state.groups[0].target_group_arns = vec!["first".into(), "second".into()];
            state
                .target_health
                .insert("first".into(), vec![(INSTANCE.into(), "draining".into())]);
            state
                .target_health
                .insert("second".into(), vec![(INSTANCE.into(), "unused".into())]);
        }
        Box::pin(aws.reached(HostMilestone::Unrouted)).await?;
        let requested: Vec<_> = fake
            .state()
            .requests
            .iter()
            .filter(|request| request.operation == DESCRIBE_TARGET_HEALTH)
            .filter_map(|request| request.parameters.get("TargetGroupArn").cloned())
            .collect();
        assert_eq!(requested, ["first", "second"]);
        fake.state().failures.insert(
            DESCRIBE_TARGET_HEALTH,
            FakeFailure {
                status: 403,
                code: "AccessDenied",
                message: "denied",
            },
        );
        assert!(matches!(
            Box::pin(aws.reached(HostMilestone::Unrouted)).await,
            Err(PortError::Backend(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unhealthy_target_waits_for_healthy_successor_even_after_short_drain() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        seed(&fake);
        {
            let mut state = fake.state();
            state.groups[0].target_group_arns = vec!["tg".into()];
            state
                .target_health
                .insert("tg".into(), vec![(INSTANCE.into(), "unhealthy".into())]);
        }
        let task =
            tokio::spawn(async move { Box::pin(aws.reached(HostMilestone::Unrouted)).await });
        while !fake
            .state()
            .requests
            .iter()
            .any(|request| request.operation == DESCRIBE_TARGET_HEALTH)
        {
            tokio::task::yield_now().await;
        }
        assert!(!task.is_finished());
        fake.state()
            .target_health
            .get_mut("tg")
            .expect("seeded target group")
            .push(("i-successor".into(), "healthy".into()));
        while !task.is_finished() {
            clock.advance(Duration::from_secs(5));
            tokio::task::yield_now().await;
        }
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn retirement_waits_through_nonterminal_states() -> TestResult {
        let (fake, aws, clock) = setup().await?;
        seed(&fake);
        let task = tokio::spawn(async move { aws.retiring().await });
        for expected in 1..=2 {
            while fake
                .state()
                .requests
                .iter()
                .filter(|request| {
                    request
                        .parameters
                        .get("path")
                        .is_some_and(|path| path.ends_with("target-lifecycle-state"))
                })
                .count()
                < expected
            {
                clock.advance(Duration::from_secs(5));
                tokio::task::yield_now().await;
            }
            assert!(!task.is_finished());
            fake.state().target_lifecycle_state = Some("InService".to_owned());
        }
        fake.state().target_lifecycle_state = Some("Terminated".to_owned());
        while !task.is_finished() {
            clock.advance(Duration::from_secs(5));
            tokio::task::yield_now().await;
        }
        task.await??;
        Ok(())
    }
}
