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
//! - `autoscaling:SetDesiredCapacity`
//! - `autoscaling:SetInstanceProtection`
//! - `autoscaling:TerminateInstanceInAutoScalingGroup`
//! - `ec2:DescribeInstances`
//! - `sts:GetWebIdentityToken`

use std::sync::Arc;

use aws_config::SdkConfig;
use gang_clock::{Clock, SystemClock};
use gang_spi::{
    FleetSupply, InstanceIdentity, MountedScratchVolume, NodeLifecyclePort, Plugin, PluginContext,
    PortError, PreemptionNotice, ProvisionerPort, ScratchVolumePort, WorkloadCredentialPort,
};
use gang_types::{Fleet, InstanceId, Proof, WorkerLifetime};
use tokio::sync::OnceCell;

use crate::config::{AwsConfig, AwsFleetOptions};
use crate::credential::WebIdentity;
use crate::errors::AwsError;
use crate::fleet::Fleets;
use crate::lifecycle::{Metadata, imds_client};
use crate::scratch::{Scratch, SystemMachine};
use crate::sdk;

/// The provisioner plugin. Building it only records the configuration; AWS clients are opened on
/// the first port call.
pub struct Aws {
    config: AwsConfig,
    clock: Arc<dyn Clock>,
    scratch: Arc<Scratch<SystemMachine>>,
    runtime: OnceCell<Runtime>,
}

/// The AWS clients and helpers the ports use, opened together on first use.
pub(crate) struct Runtime {
    fleets: Fleets,
    metadata: Metadata,
    web_identity: WebIdentity,
}

impl Runtime {
    /// Builds the runtime from an SDK configuration and an IMDS client.
    pub(crate) fn from_sdk(
        sdk: &SdkConfig,
        imds: aws_config::imds::Client,
        config: &AwsConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            fleets: Fleets::new(sdk, clock, config.set_capacity_cooldown),
            metadata: Metadata::new(imds, config.fleet_tag_key.as_str()),
            web_identity: WebIdentity::new(sdk, config),
        }
    }
}

impl Aws {
    /// Returns the runtime, opening the AWS clients on the first call.
    async fn runtime(&self) -> Result<&Runtime, AwsError> {
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
            scratch: Arc::new(scratch),
            runtime: OnceCell::new_with(Some(runtime)),
            config,
        }
    }
}

impl Plugin for Aws {
    type Config = AwsConfig;

    fn build(config: AwsConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        let scratch = Scratch::new(
            config.scratch_mount_point.as_path().to_path_buf(),
            SystemMachine,
        );
        Ok(Self {
            config,
            clock: Arc::new(SystemClock::new()),
            scratch: Arc::new(scratch),
            runtime: OnceCell::new(),
        })
    }
}

impl ProvisionerPort for Aws {
    type FleetOptions = AwsFleetOptions;

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
        fleet: Fleet<'_, AwsFleetOptions>,
        desired: u32,
    ) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        runtime.fleets.set_desired_capacity(fleet, desired).await?;
        Ok(())
    }

    async fn terminate(
        &self,
        fleet: Fleet<'_, AwsFleetOptions>,
        instance: &InstanceId,
    ) -> Result<(), PortError> {
        let runtime = self.runtime().await?;
        runtime.fleets.terminate(fleet, instance).await?;
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
    use super::*;
    use crate::fake_aws::{FakeAws, FakeFailure};
    use crate::fleet_harness::{Harness, fleet};
    use crate::test_support::context;
    use gang_clock::TestClock;
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
            Arc::new(TestClock::default()),
        );
        Ok(Aws::with_runtime(config, runtime))
    }

    #[test]
    fn building_an_aws_plugin_makes_no_aws_call() -> TestResult {
        let aws = Aws::build(config(SCRATCH)?, &context())?;
        assert!(!aws.runtime.initialized());
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

        let result = aws
            .terminate(fleet(GROUP)?.view(), &InstanceId::try_new(OTHER_INSTANCE)?)
            .await;

        assert!(matches!(
            &result,
            Err(PortError::Config(cause)) if cause.to_string().contains(GROUP)
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
