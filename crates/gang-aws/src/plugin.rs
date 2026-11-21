//! The provisioner plugin, `[provisioner.aws]`: the worker fleet, the node's own identity, the
//! scratch volume and the workload credential, served from AWS.
//!
//! The ports:
//!
//! - [`ProvisionerPort`] runs each fleet as one Auto Scaling group, named `Fleet::name`. It
//!   reads the group and its instances, raises the desired capacity, and terminates one instance
//!   within its group.
//! - [`NodeLifecyclePort`] answers who this node is from the instance identity document, reports
//!   a Spot interruption from the instance-action document and reports whether the node is parked
//!   in its warm pool from the target lifecycle state.
//! - [`ControllerHostPort`](gang_spi::ControllerHostPort) detects controller retirement and
//!   completes the matching Auto Scaling lifecycle hook at each host milestone. Instance
//!   metadata tags must be enabled to read the Auto Scaling group tag.
//! - [`ScratchVolumePort`] adopts the local scratch volume. The volume belongs to the node, not
//!   to an instance, so every call returns the same volume.
//! - [`WorkloadCredentialPort`] issues a web identity token through STS.
//!
//! Prerequisites: a root service prepares the `NVMe` RAID0 XFS reflink volume at
//! `scratch_mount_point` before the worker starts. This plugin only adopts and verifies that
//! volume; it never formats or mounts it.
//!
//! IAM actions used by the plugin:
//!
//! - `autoscaling:DescribeAutoScalingGroups`
//! - `autoscaling:DescribeScalingActivities`
//! - `autoscaling:DescribeLifecycleHooks`
//! - `autoscaling:CompleteLifecycleAction`
//! - `autoscaling:SetDesiredCapacity`
//! - `autoscaling:SetInstanceProtection`
//! - `autoscaling:TerminateInstanceInAutoScalingGroup`
//! - `ec2:DescribeInstances`
//! - `sts:GetWebIdentityToken`

use std::sync::Arc;

use aws_config::SdkConfig;
use gang_clock::{Clock, SystemClock};
use gang_spi::{
    FleetSupply, Held, InstanceIdentity, MountedScratchVolume, NodeLifecyclePort, Plugin,
    PluginContext, PortError, PreemptionNotice, ProvisionerPort, ScratchVolumePort,
    WorkloadCredentialPort,
};
use gang_types::{Fleet, InstanceId, Proof, WorkerLifetime};
use tokio::sync::OnceCell;

use crate::config::{AwsConfig, AwsFleetOptions};
use crate::credential::WebIdentity;
use crate::errors::AwsError;
use crate::events::{Events, Managed};
use crate::fleet::Fleets;
use crate::lifecycle::{Metadata, imds_client};
use crate::scratch::{Scratch, SystemMachine};
use gang_aws_sdk as sdk;

/// The provisioner plugin. Building it only records the configuration; AWS clients are opened on
/// the first port call.
pub struct Aws {
    pub(crate) config: AwsConfig,
    pub(crate) clock: Arc<dyn Clock>,
    scratch: Arc<Scratch<SystemMachine>>,
    managed: Arc<Managed>,
    runtime: OnceCell<Runtime>,
}

/// The AWS clients and helpers the ports use, opened together on first use.
pub(crate) struct Runtime {
    fleets: Fleets,
    pub(crate) autoscaling: aws_sdk_autoscaling::Client,
    pub(crate) metadata: Metadata,
    web_identity: WebIdentity,
    #[cfg(test)]
    managed: Arc<Managed>,
}

impl Runtime {
    /// Builds the runtime from a validated configuration, SDK configuration and IMDS client.
    pub(crate) fn from_sdk(
        sdk: &SdkConfig,
        imds: aws_config::imds::Client,
        config: &AwsConfig,
        clock: Arc<dyn Clock>,
        managed: &Arc<Managed>,
    ) -> Self {
        let autoscaling = crate::autoscaling::client(
            sdk,
            Arc::clone(&clock),
            config.autoscaling_request_interval.get(),
        );
        let events = Events::new(sdk, config, Arc::clone(&clock), Arc::clone(managed))
            .with_autoscaling(autoscaling.clone());
        Self {
            fleets: Fleets::new(sdk, clock, config.set_capacity_cooldown, events)
                .with_autoscaling(autoscaling.clone())
                .with_observation_interval(config.supply_poll_interval.get()),
            autoscaling,
            metadata: Metadata::new(imds, config.fleet_tag_key.as_str()),
            web_identity: WebIdentity::new(sdk, config),
            #[cfg(test)]
            managed: Arc::clone(managed),
        }
    }
}

impl Aws {
    /// Returns the runtime, opening the AWS clients on the first call.
    pub(crate) async fn runtime(&self) -> Result<&Runtime, AwsError> {
        self.runtime.get_or_try_init(|| self.open()).await
    }

    /// Loads the SDK configuration and the IMDS client from the plugin's settings.
    async fn open(&self) -> Result<Runtime, AwsError> {
        let sdk = sdk::load(&self.config.region, &self.config.client).await;
        let imds = imds_client(sdk::http_client(), self.config.imds_request_timeout, None)?;
        Ok(Runtime::from_sdk(
            &sdk,
            imds,
            &self.config,
            Arc::clone(&self.clock),
            &self.managed,
        ))
    }

    /// Builds the plugin around an already opened runtime. Test seam: the runtime's clients
    /// talk to a fake endpoint.
    #[cfg(test)]
    pub(crate) fn with_runtime(config: AwsConfig, runtime: Runtime) -> Self {
        let scratch = Scratch::new(
            config.scratch_mount_point.as_path().to_path_buf(),
            SystemMachine,
        );
        Self {
            clock: Arc::new(SystemClock::new()),
            managed: Arc::clone(&runtime.managed),
            scratch: Arc::new(scratch),
            runtime: OnceCell::new_with(Some(runtime)),
            config,
        }
    }
}

impl Plugin for Aws {
    type Config = AwsConfig;

    fn build(config: AwsConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        if !(1..=10).contains(&config.event_batch_limit) {
            return Err(AwsError::Config {
                field: "event_batch_limit",
                reason: "must be between 1 and 10".to_owned(),
            }
            .into());
        }
        let visibility = config.event_visibility_timeout.get();
        if visibility.subsec_nanos() != 0 || visibility.as_secs() > 43_200 {
            return Err(AwsError::Config {
                field: "event_visibility_timeout",
                reason: "must be whole seconds up to 12 hours".to_owned(),
            }
            .into());
        }
        if !(1..=1000).contains(&config.activity_limit) {
            return Err(AwsError::Config {
                field: "activity_limit",
                reason: "must be between 1 and 1000".to_owned(),
            }
            .into());
        }
        let scratch = Scratch::new(
            config.scratch_mount_point.as_path().to_path_buf(),
            SystemMachine,
        );
        Ok(Self {
            config,
            clock: Arc::new(SystemClock::new()),
            managed: Arc::new(Managed::default()),
            scratch: Arc::new(scratch),
            runtime: OnceCell::new(),
        })
    }
}

impl ProvisionerPort for Aws {
    type FleetOptions = AwsFleetOptions;

    fn reset_configured_fleets(&self) {
        self.managed.clear();
    }

    fn configure_fleet(&self, fleet: Fleet<'_, AwsFleetOptions>) -> Result<(), PortError> {
        self.managed.add(fleet);
        Ok(())
    }

    fn observation_error(
        &self,
        fleet: Fleet<'_, AwsFleetOptions>,
    ) -> Result<Option<String>, PortError> {
        Ok(self.runtime.get().and_then(|runtime| {
            runtime
                .fleets
                .events()
                .observation_error(fleet.name.as_str())
        }))
    }

    async fn converge(
        &self,
        held: &Held,
        _fleet: Fleet<'_, AwsFleetOptions>,
    ) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        if let Err(error) = runtime.fleets.events().receive(held).await {
            held.check()?;
            tracing::warn!(%error, "AWS event queue poll failed");
        }
        Ok(())
    }

    async fn acknowledge_event(
        &self,
        held: &Held,
        fleet: Fleet<'_, AwsFleetOptions>,
        event_id: &str,
    ) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        runtime
            .fleets
            .events()
            .acknowledge(held, fleet.name.as_str(), event_id)
            .await?;
        Ok(())
    }

    async fn supply(
        &self,
        fleet: Fleet<'_, AwsFleetOptions>,
        into: &mut FleetSupply,
    ) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        runtime.fleets.supply(fleet, into).await?;
        Ok(())
    }

    async fn set_desired_capacity(
        &self,
        held: &Held,
        fleet: Fleet<'_, AwsFleetOptions>,
        desired: u32,
    ) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        runtime
            .fleets
            .set_desired_capacity(held, fleet, desired)
            .await?;
        Ok(())
    }

    async fn terminate(
        &self,
        held: &Held,
        fleet: Fleet<'_, AwsFleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        runtime.fleets.terminate(held, fleet, instance).await?;
        Ok(())
    }

    fn worker_lifetime(&self) -> WorkerLifetime {
        WorkerLifetime::LongLived
    }
}

impl NodeLifecyclePort for Aws {
    async fn identity(&self) -> Result<InstanceIdentity, PortError> {
        let runtime = self.runtime().await?;
        Ok(runtime.metadata.identity().await?)
    }

    async fn preemption_notice(&self) -> Result<Option<PreemptionNotice>, PortError> {
        let runtime = self.runtime().await?;
        Ok(runtime.metadata.preemption_notice().await?)
    }

    async fn parked(&self) -> Result<bool, PortError> {
        let runtime = self.runtime().await?;
        Ok(runtime.metadata.parked().await?)
    }
}

impl WorkloadCredentialPort for Aws {
    async fn workload_credential(&self) -> Result<Proof, PortError> {
        let runtime = self.runtime().await?;
        Ok(runtime.web_identity.token().await?)
    }
}

impl ScratchVolumePort for Aws {
    async fn mount(&self, _instance: &InstanceId) -> Result<MountedScratchVolume, PortError> {
        let scratch = Arc::clone(&self.scratch);
        let adopted = tokio::task::spawn_blocking(move || scratch.adopt())
            .await
            .map_err(PortError::backend)?;
        adopted.map_err(|error| PortError::from(AwsError::Scratch(error)))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::{AwsFleetOptions, Capacity, RetrySpotAfter, SpotFallback};
    use crate::fake_aws::{FakeAws, FakeFailure, FakeGroup};
    use crate::fleet_harness::{Harness, NOW_MILLIS, fleet, fleet_with, rfc3339_ago};
    use crate::test_support::context;
    use gang_clock::{Sleep, TestClock, Tick, Timestamp};
    use gang_types::InstanceLifecycle;
    use serde_json::json;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const REGION: &str = "us-east-1";
    const SCRATCH: &str = "/mnt/scratch";
    const GROUP: &str = "gang-worker-x86-64";
    const OTHER_GROUP: &str = "gang-worker-arm64";
    const INSTANCE: &str = "i-0000000000000001";
    const OTHER_INSTANCE: &str = "i-0000000000000002";
    const LAUNCH_TIME: &str = "2026-01-01T00:00:00Z";
    const IN_SERVICE: &str = "InService";

    // Pace HTTP requests deterministically without advancing the fixed wall clock.
    struct AutoAdvanceClock {
        monotonic: TestClock,
    }

    impl Clock for AutoAdvanceClock {
        fn wall(&self) -> Timestamp {
            Timestamp::from_unix_millis(NOW_MILLIS)
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

    fn test_clock() -> Arc<dyn Clock> {
        Arc::new(AutoAdvanceClock {
            monotonic: TestClock::default(),
        })
    }

    fn config(scratch_mount_point: &str) -> Result<AwsConfig, serde_json::Error> {
        serde_json::from_value(json!({
            "region": REGION,
            "scratch_mount_point": scratch_mount_point,
        }))
    }

    async fn with_fake(fake: &FakeAws) -> Result<Aws, Box<dyn std::error::Error>> {
        let config = config(SCRATCH)?;
        let runtime = Runtime::from_sdk(
            &fake.sdk_config().await,
            fake.imds_client()?,
            &config,
            test_clock(),
            &Arc::new(Managed::default()),
        );
        Ok(Aws::with_runtime(config, runtime))
    }

    #[test]
    fn building_an_aws_plugin_makes_no_aws_call() -> TestResult {
        let aws = Aws::build(config(SCRATCH)?, &context())?;
        assert_eq!(aws.observation_error(fleet(GROUP)?.view())?, None);
        assert!(!aws.runtime.initialized());
        Ok(())
    }

    #[test]
    fn building_rejects_programmatic_invalid_event_batch_limit() -> TestResult {
        let mut invalid = config(SCRATCH)?;
        invalid.event_batch_limit = 11;
        assert!(matches!(
            Aws::build(invalid, &context()),
            Err(PortError::Config(_))
        ));
        Ok(())
    }

    #[test]
    fn building_rejects_programmatic_invalid_event_settings() -> TestResult {
        for duration in [
            gang_types::NonZeroDuration::from_millis(500),
            gang_types::NonZeroDuration::from_hours(13),
        ] {
            let mut invalid = config(SCRATCH)?;
            invalid.event_visibility_timeout = duration;
            assert!(matches!(
                Aws::build(invalid, &context()),
                Err(PortError::Config(_))
            ));
        }
        for limit in [0, 1001] {
            let mut invalid = config(SCRATCH)?;
            invalid.activity_limit = limit;
            assert!(matches!(
                Aws::build(invalid, &context()),
                Err(PortError::Config(_))
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn queue_failure_does_not_block_supply_and_expired_fence_does_not_poll() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(GROUP, 1, &[(INSTANCE, IN_SERVICE)]);
        harness.add_launch(INSTANCE, LAUNCH_TIME);
        harness.fail(
            "ReceiveMessage",
            FakeFailure {
                status: 503,
                code: "ServiceUnavailable",
                message: "queue unavailable",
            },
        );
        let mut settings = config(SCRATCH)?;
        settings.events_queue_url = Some(harness.fake.queue_url().parse()?);
        let runtime = Runtime::from_sdk(
            &harness.fake.sdk_config().await,
            harness.fake.imds_client()?,
            &settings,
            test_clock(),
            &Arc::new(Managed::default()),
        );
        let aws = Aws::with_runtime(settings, runtime);
        let test_fleet = fleet(GROUP)?;
        gang_testkit::fence::with_held(async |held| aws.converge(held, test_fleet.view()).await)
            .await?;
        assert_eq!(harness.requests("ReceiveMessage"), 1);
        let mut supply = FleetSupply::default();
        aws.supply(test_fleet.view(), &mut supply).await?;
        assert_eq!(supply.desired, 1);
        assert_eq!(supply.instances.len(), 1);
        assert_eq!(supply.instances[0].id, InstanceId::try_new(INSTANCE)?);
        let expired = gang_testkit::fence::with_expired_held(async |held| {
            aws.converge(held, test_fleet.view()).await
        })
        .await;
        assert!(expired.is_err());
        assert_eq!(harness.requests("ReceiveMessage"), 1);
        Ok(())
    }

    #[tokio::test]
    async fn supply_returns_the_in_service_instance() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(GROUP, 1, &[(INSTANCE, IN_SERVICE)]);
        harness.add_launch(INSTANCE, LAUNCH_TIME);
        let aws = with_fake(&harness.fake).await?;

        let mut supply = FleetSupply::default();
        aws.supply(fleet(GROUP)?.view(), &mut supply).await?;

        assert_eq!(supply.desired, 1);
        assert_eq!(supply.instances.len(), 1);
        let instance = supply.instances.first().ok_or("no instance supplied")?;
        assert_eq!(instance.id, InstanceId::try_new(INSTANCE)?);
        assert_eq!(instance.lifecycle, InstanceLifecycle::InService);
        Ok(())
    }

    #[tokio::test]
    async fn supply_exposes_best_effort_activity_error_without_failing() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(GROUP, 0, &[]);
        harness.fail(
            "DescribeScalingActivities",
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "rate exceeded",
            },
        );
        let aws = with_fake(&harness.fake).await?;
        let test_fleet = fleet(GROUP)?;
        let mut supply = FleetSupply::default();
        aws.supply(test_fleet.view(), &mut supply).await?;
        let error = aws
            .observation_error(test_fleet.view())?
            .ok_or("missing observation error")?;
        assert!(error.contains("DescribeScalingActivities"));
        Ok(())
    }

    #[tokio::test]
    async fn abandoned_requests_follow_the_provisioner_contract() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group_with(FakeGroup {
            new_instances_protected: true,
            ..FakeGroup::new(GROUP, 4, &[])
        });
        harness.add_group_with(FakeGroup {
            new_instances_protected: true,
            ..FakeGroup::new("gang-worker-x86-64-on-demand", 0, &[])
        });
        harness.add_activity(GROUP, "Failed", &rfc3339_ago(30));
        let aws = with_fake(&harness.fake).await?;
        let fleet = fleet_with(
            GROUP,
            AwsFleetOptions {
                capacity: Capacity::Spot,
                machine_class: None,
                fallback: Some(SpotFallback {
                    retry_spot_after: RetrySpotAfter::try_new(Duration::from_mins(5))?,
                    drain_on_demand_first: true,
                }),
            },
        )?;

        gang_testkit::provisioner_contract::abandoned_requests_contract(&aws, fleet.view()).await
    }

    #[tokio::test]
    async fn supply_of_a_missing_group_is_a_config_error() -> TestResult {
        let harness = Harness::start().await?;
        let aws = with_fake(&harness.fake).await?;

        let mut supply = FleetSupply::default();
        let result = aws.supply(fleet(GROUP)?.view(), &mut supply).await;

        assert!(matches!(result, Err(PortError::Config(_))));
        Ok(())
    }

    #[tokio::test]
    async fn throttled_group_query_is_unavailable() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(GROUP, 1, &[(INSTANCE, IN_SERVICE)]);
        harness.add_launch(INSTANCE, LAUNCH_TIME);
        harness.fail(
            "DescribeAutoScalingGroups",
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "rate exceeded",
            },
        );
        let aws = with_fake(&harness.fake).await?;

        let mut supply = FleetSupply::default();
        let result = aws.supply(fleet(GROUP)?.view(), &mut supply).await;

        assert!(matches!(result, Err(PortError::Unavailable(_))));
        Ok(())
    }

    #[tokio::test]
    async fn terminate_with_a_missing_group_is_a_config_error() -> TestResult {
        let harness = Harness::start().await?;
        harness.add_group(OTHER_GROUP, 1, &[(OTHER_INSTANCE, IN_SERVICE)]);
        harness.add_launch(OTHER_INSTANCE, LAUNCH_TIME);
        let aws = with_fake(&harness.fake).await?;

        let result = gang_testkit::fence::with_held(async |held| {
            aws.terminate(
                held,
                fleet(GROUP).expect("test value is valid").view(),
                &InstanceId::try_new(OTHER_INSTANCE).expect("test value is valid"),
            )
            .await
        })
        .await;

        assert!(matches!(
            &result,
            Err(gang_spi::RunError::Failed(PortError::Config(cause))) if cause.to_string().contains(GROUP)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn mount_on_a_plain_directory_is_a_backend_error() -> TestResult {
        let dir = tempfile::tempdir()?;
        let mount_point = dir.path().to_str().ok_or("temporary path is not utf-8")?;
        let aws = Aws::build(config(mount_point)?, &context())?;

        let result = aws.mount(&InstanceId::try_new(INSTANCE)?).await;

        assert!(matches!(result, Err(PortError::Backend(_))));
        Ok(())
    }
}
