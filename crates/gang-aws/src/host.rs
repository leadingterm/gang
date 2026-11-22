//! Controller host retirement and Auto Scaling lifecycle hook hand-off.

use aws_sdk_autoscaling::types::LifecycleState;
use gang_aws_sdk::{CallError, call_error};
use gang_spi::{ControllerHostIdentity, ControllerHostPort, HostMilestone, PortError};

use crate::errors::AwsError;
use crate::group::DESCRIBE_GROUPS;
use crate::plugin::Aws;

const DESCRIBE_HOOKS: &str = "DescribeLifecycleHooks";
const COMPLETE_ACTION: &str = "CompleteLifecycleAction";
const LAUNCH_TRANSITION: &str = "autoscaling:EC2_INSTANCE_LAUNCHING";
const TERMINATE_TRANSITION: &str = "autoscaling:EC2_INSTANCE_TERMINATING";
const CONTINUE: &str = "CONTINUE";

impl ControllerHostPort for Aws {
    async fn incarnation(&self) -> Result<ControllerHostIdentity, PortError> {
        Ok(self.runtime().await?.metadata.incarnation().await?)
    }

    async fn retiring(&self) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        loop {
            match runtime.metadata.retiring().await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error @ AwsError::Call(CallError::Transient { .. })) => {
                    tracing::warn!(%error, "controller retirement metadata read failed; retrying");
                }
                Err(error) => return Err(error.into()),
            }
            self.clock
                .sleep(self.config.retirement_poll_interval.get())
                .await;
        }
    }

    async fn reached(&self, milestone: HostMilestone) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        if matches!(milestone, HostMilestone::HandedOff) && !runtime.metadata.retiring().await? {
            return Ok(());
        }
        let Some(group) = runtime.metadata.group_name().await? else {
            return Ok(());
        };
        let transition = match milestone {
            HostMilestone::Serving => LAUNCH_TRANSITION,
            HostMilestone::HandedOff => TERMINATE_TRANSITION,
        };
        let hooks = runtime
            .autoscaling
            .describe_lifecycle_hooks()
            .auto_scaling_group_name(&group)
            .send()
            .await
            .map_err(|error| AwsError::from(call_error(DESCRIBE_HOOKS, error)))?;
        let Some(hook) = hooks
            .lifecycle_hooks()
            .iter()
            .find(|hook| hook.lifecycle_transition() == Some(transition))
        else {
            return Ok(());
        };
        let name = hook
            .lifecycle_hook_name()
            .ok_or_else(|| AwsError::malformed(DESCRIBE_HOOKS, "lifecycle hook name"))?;
        let instance = runtime.metadata.instance_id().await?;
        if matches!(milestone, HostMilestone::Serving) {
            let response = runtime
                .autoscaling
                .describe_auto_scaling_groups()
                .auto_scaling_group_names(&group)
                .send()
                .await
                .map_err(|error| AwsError::from(call_error(DESCRIBE_GROUPS, error)))?;
            let controller_group = response
                .auto_scaling_groups()
                .iter()
                .find(|candidate| candidate.auto_scaling_group_name() == Some(group.as_str()))
                .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "controller group"))?;
            let controller = controller_group
                .instances()
                .iter()
                .find(|candidate| candidate.instance_id() == Some(instance.as_str()))
                .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "controller instance"))?;
            let state = controller
                .lifecycle_state()
                .ok_or_else(|| AwsError::malformed(DESCRIBE_GROUPS, "lifecycle state"))?;
            if *state != LifecycleState::PendingWait {
                return Ok(());
            }
        }
        runtime
            .autoscaling
            .complete_lifecycle_action()
            .auto_scaling_group_name(&group)
            .lifecycle_hook_name(name)
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

    use gang_clock::{Clock, Sleep, TestClock, Tick, Timestamp};
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

    // Runtime HTTP pacing advances independently; host polling retains its manually driven clock.
    struct AutoAdvanceClock {
        wall: Arc<TestClock>,
        monotonic: TestClock,
    }

    impl Clock for AutoAdvanceClock {
        fn wall(&self) -> Timestamp {
            self.wall.wall()
        }

        fn monotonic(&self) -> Tick {
            self.monotonic.monotonic()
        }

        fn sleep_until(&self, deadline: Tick) -> Sleep {
            self.monotonic
                .advance(deadline.saturating_duration_since(self.monotonic.monotonic()));
            self.monotonic.sleep_until(deadline)
        }
    }

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
            Arc::new(AutoAdvanceClock {
                wall: clock.clone(),
                monotonic: TestClock::default(),
            }),
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
        aws.reached(HostMilestone::Serving).await?;
        assert_eq!(
            fake.state().lifecycle_completions,
            vec![(GROUP.to_owned(), "launch".to_owned(), INSTANCE.to_owned())]
        );
        {
            let mut state = fake.state();
            state.target_lifecycle_state = Some("Terminated".to_owned());
            state.groups[0].instances[0].lifecycle_state = "Terminating:Wait".to_owned();
        }
        aws.reached(HostMilestone::HandedOff).await?;
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
        aws.reached(HostMilestone::Serving).await?;
        fake.state().promote(GROUP, INSTANCE);

        let restarted = aws_for(&fake, clock).await?;
        restarted.reached(HostMilestone::Serving).await?;

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

        aws.reached(HostMilestone::Serving).await?;

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

            aws.reached(HostMilestone::Serving).await?;

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

            aws.reached(HostMilestone::Serving).await?;

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
            aws.reached(HostMilestone::Serving).await,
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
                aws.reached(HostMilestone::Serving).await,
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
                aws.reached(HostMilestone::Serving).await,
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
            aws.reached(HostMilestone::Serving).await,
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
        aws.reached(HostMilestone::Serving).await?;
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
        aws.reached(HostMilestone::HandedOff).await?;
        assert!(fake.state().lifecycle_completions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn outside_group_does_not_call_autoscaling() -> TestResult {
        let (fake, aws, _) = setup().await?;
        aws.reached(HostMilestone::Serving).await?;
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
        aws.reached(HostMilestone::Serving).await?;
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
            aws.reached(HostMilestone::Serving).await,
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
            aws.reached(HostMilestone::Serving).await,
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
            aws.reached(HostMilestone::Serving).await,
            Err(PortError::Backend(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn retirement_waits_through_missing_and_nonterminal_states() -> TestResult {
        let (fake, aws, clock) = setup().await?;
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
