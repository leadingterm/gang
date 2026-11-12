//! Instance metadata: who this node is, whether the provisioner is taking it back, and whether
//! the node is parked.
//!
//! Reads go to the instance metadata service through `IMDSv2` session tokens, which the SDK client
//! fetches and refreshes itself. The identity document is fetched once per process.

use aws_config::imds::Client;
use aws_config::imds::client::error::ImdsError;
use aws_config::provider_config::ProviderConfig;
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use gang_clock::{Rfc3339Error, Timestamp};
use gang_spi::{InstanceIdentity, PreemptionNotice};
use gang_types::{FleetName, InstanceId, Isa, NonZeroDuration};
use serde::Deserialize;
use tokio::sync::OnceCell;

use crate::errors::{AwsError, Retryability, classify};

/// Path of the signed instance identity document.
const IDENTITY_PATH: &str = "/latest/dynamic/instance-identity/document";
/// Path of the spot interruption notice; absent (404) until the provisioner announces one.
const PREEMPTION_PATH: &str = "/latest/meta-data/spot/instance-action";
/// Path of the target lifecycle state of this instance in its Auto Scaling group; absent (404)
/// when the instance belongs to no group.
const TARGET_LIFECYCLE_PATH: &str = "/latest/meta-data/autoscaling/target-lifecycle-state";
/// Prefix of the target lifecycle state of an instance that its group keeps in the warm pool.
const WARMED_PREFIX: &str = "Warmed:";

/// Prefix of the metadata path of an instance tag; the tag key follows it. The Auto Scaling
/// group propagates the fleet tag to its instances; requires instance metadata tags to be
/// enabled on the launch template.
const FLEET_TAG_PATH_PREFIX: &str = "/latest/meta-data/tags/instance/";
/// Field reported when the node's fleet tag is missing or invalid.
const FLEET_TAG_FIELD: &str = "fleet instance tag";
/// Operation name reported for every failed metadata read.
const IMDS_OPERATION: &str = "IMDS GET";
/// Configuration field reported when the metadata endpoint cannot be used.
const IMDS_ENDPOINT_FIELD: &str = "imds_endpoint";
/// Architecture string the metadata service reports for x86-64 instances.
const X86_64_ARCHITECTURE: &str = "x86_64";
/// Architecture string the metadata service reports for 64-bit Arm instances.
const ARM64_ARCHITECTURE: &str = "arm64";
/// HTTP status the metadata service returns when a path has no value.
const NOT_FOUND: u16 = 404;

/// Builds the SDK metadata client over `http_client`, with every timeout set to `timeout`.
///
/// Without `endpoint` the SDK default applies, which honours the standard metadata endpoint
/// environment override.
///
/// # Errors
///
/// [`AwsError::Config`] when `endpoint` is not a valid URI.
pub(crate) fn imds_client(
    http_client: SharedHttpClient,
    timeout: NonZeroDuration,
    endpoint: Option<&str>,
) -> Result<Client, AwsError> {
    let timeout = timeout.get();
    let builder = Client::builder()
        .configure(&ProviderConfig::default().with_http_client(http_client))
        .connect_timeout(timeout)
        .read_timeout(timeout)
        .operation_timeout(timeout);
    let builder = match endpoint {
        Some(endpoint) => builder
            .endpoint(endpoint)
            .map_err(|source| AwsError::Config {
                field: IMDS_ENDPOINT_FIELD,
                reason: source.to_string(),
            })?,
        None => builder,
    };
    Ok(builder.build())
}

/// Reads this node's identity, preemption notices and warm pool state from the instance metadata
/// service.
pub(crate) struct Metadata {
    imds: Client,
    fleet_tag_key: String,
    fleet_tag_path: String,
    identity: OnceCell<InstanceIdentity>,
}

impl Metadata {
    /// Metadata reads served by `imds`. The node's fleet is the value of its `fleet_tag_key`
    /// instance tag.
    pub(crate) fn new(imds: Client, fleet_tag_key: &str) -> Self {
        Self {
            imds,
            fleet_tag_key: fleet_tag_key.to_owned(),
            fleet_tag_path: format!("{FLEET_TAG_PATH_PREFIX}{fleet_tag_key}"),
            identity: OnceCell::new(),
        }
    }

    /// This node's instance id and instruction set. The first success is memoized.
    pub(crate) async fn identity(&self) -> Result<InstanceIdentity, AwsError> {
        self.identity
            .get_or_try_init(|| self.fetch_identity())
            .await
            .cloned()
    }

    /// The pending spot preemption of this node, or `None` when no action is announced.
    pub(crate) async fn preemption_notice(&self) -> Result<Option<PreemptionNotice>, AwsError> {
        let body = match self.imds.get(PREEMPTION_PATH).await {
            Ok(body) => body,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(imds_error(error)),
        };
        let action: SpotInstanceAction = serde_json::from_str(body.as_ref())
            .map_err(|_| AwsError::malformed(IMDS_OPERATION, "preemption notice"))?;
        Ok(Some(PreemptionNotice {
            terminate_at: preemption_time(&action.time)?,
        }))
    }

    /// Whether the Auto Scaling group has parked this node in its warm pool: its target lifecycle
    /// state starts with `Warmed:`. A missing value or any other state is not parked.
    pub(crate) async fn parked(&self) -> Result<bool, AwsError> {
        match self.imds.get(TARGET_LIFECYCLE_PATH).await {
            Ok(body) => Ok(body.as_ref().starts_with(WARMED_PREFIX)),
            Err(error) if is_not_found(&error) => Ok(false),
            Err(error) => Err(imds_error(error)),
        }
    }

    /// Fetches and parses the identity document; the caller memoizes the result.
    async fn fetch_identity(&self) -> Result<InstanceIdentity, AwsError> {
        let body = self.imds.get(IDENTITY_PATH).await.map_err(imds_error)?;
        let document: IdentityDocument = serde_json::from_str(body.as_ref())
            .map_err(|_| AwsError::malformed(IMDS_OPERATION, "instance identity document"))?;
        let instance = InstanceId::try_new(document.instance_id)
            .map_err(|_| AwsError::malformed(IMDS_OPERATION, "instance id"))?;
        let isa = isa_of(&document.architecture)
            .ok_or_else(|| AwsError::malformed(IMDS_OPERATION, "supported architecture"))?;
        let fleet = self.fetch_fleet().await?;
        Ok(InstanceIdentity {
            instance,
            fleet,
            isa,
        })
    }

    /// Fetches this node's fleet from its instance metadata tags.
    async fn fetch_fleet(&self) -> Result<FleetName, AwsError> {
        let body = match self.imds.get(self.fleet_tag_path.as_str()).await {
            Ok(body) => body,
            Err(error) if is_not_found(&error) => {
                return Err(AwsError::Config {
                    field: FLEET_TAG_FIELD,
                    reason: format!(
                        "the instance has no `{}` tag: enable instance metadata tags on the launch template and tag the group",
                        self.fleet_tag_key
                    ),
                });
            }
            Err(error) => return Err(imds_error(error)),
        };
        let value = body.as_ref();
        FleetName::try_new(value.to_owned()).map_err(|error| AwsError::Config {
            field: FLEET_TAG_FIELD,
            reason: format!(
                "tag `{key}` has value `{value}`, which is not a valid fleet name: {error}",
                key = self.fleet_tag_key
            ),
        })
    }
}

/// The identity document, as far as this crate reads it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityDocument {
    instance_id: String,
    architecture: String,
}

/// The spot interruption notice, as far as this crate reads it. Every action ends the node.
#[derive(Deserialize)]
struct SpotInstanceAction {
    time: String,
}

/// The instruction set of an architecture name from the identity document.
fn isa_of(architecture: &str) -> Option<Isa> {
    match architecture {
        X86_64_ARCHITECTURE => Some(Isa::X86_64),
        ARM64_ARCHITECTURE => Some(Isa::Aarch64),
        _ => None,
    }
}

/// The RFC 3339 action time as a timestamp; times before the epoch clamp to the epoch.
fn preemption_time(text: &str) -> Result<Timestamp, AwsError> {
    match Timestamp::from_rfc3339(text) {
        Ok(at) => Ok(at),
        Err(Rfc3339Error::BeforeEpoch) => Ok(Timestamp::UNIX_EPOCH),
        Err(Rfc3339Error::Malformed(_)) => {
            Err(AwsError::malformed(IMDS_OPERATION, "preemption time"))
        }
    }
}

/// Whether a metadata read failed because the path has no value.
fn is_not_found(error: &ImdsError) -> bool {
    matches!(error, ImdsError::ErrorResponse(response) if response.response().status().as_u16() == NOT_FOUND)
}

/// Classifies a failed metadata read: connection and throttling failures are transient, the rest
/// are refusals.
fn imds_error(error: ImdsError) -> AwsError {
    let retryability = match &error {
        ImdsError::FailedToLoadToken(token) if token.is_dispatch_failure() => {
            Retryability::Transient
        }
        ImdsError::IoError(_) => Retryability::Transient,
        ImdsError::ErrorResponse(response) => {
            classify(None, Some(response.response().status().as_u16()))
        }
        _ => Retryability::Permanent,
    };
    AwsError::failed(IMDS_OPERATION, retryability, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_aws::{FakeAws, FakeFailure};
    use crate::names::FleetTagKey;

    const INSTANCE_ID: &str = "i-0123456789abcdef0";
    const TERMINATE_AT_MILLIS: u64 = 1_767_225_600_000;

    fn identity_document(architecture: &str) -> String {
        format!(
            r#"{{"instanceId":"{INSTANCE_ID}","architecture":"{architecture}","region":"us-east-1"}}"#
        )
    }

    const FLEET: &str = "example-fleet";
    const CUSTOM_KEY: &str = "custom-fleet-key";

    /// Seeds the identity document and fleet tag of an instance of `architecture`.
    fn seed_identity(fake: &FakeAws, architecture: &str) {
        let mut state = fake.state();
        state.identity_document = Some(identity_document(architecture));
        state.fleet_tag = Some(FLEET.to_owned());
    }

    fn tag_requests(fake: &FakeAws, key: &str) -> usize {
        let path = format!("{FLEET_TAG_PATH_PREFIX}{key}");
        fake.state()
            .requests
            .iter()
            .filter(|request| {
                request.operation == "ImdsGet"
                    && request.parameters.get("path").map(String::as_str) == Some(path.as_str())
            })
            .count()
    }

    fn metadata(fake: &FakeAws) -> Result<Metadata, AwsError> {
        Ok(Metadata::new(fake.imds_client()?, FleetTagKey::DEFAULT))
    }

    fn document_requests(fake: &FakeAws) -> usize {
        fake.state()
            .requests
            .iter()
            .filter(|request| {
                request.operation == "ImdsGet"
                    && request.parameters.get("path").map(String::as_str) == Some(IDENTITY_PATH)
            })
            .count()
    }

    #[tokio::test]
    async fn identity_maps_x86_64() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        seed_identity(&fake, "x86_64");
        let identity = metadata(&fake)?.identity().await?;
        assert_eq!(identity.instance, InstanceId::try_new(INSTANCE_ID)?);
        assert_eq!(identity.fleet, FleetName::try_new(FLEET)?);
        assert_eq!(identity.isa, Isa::X86_64);
        Ok(())
    }

    #[tokio::test]
    async fn identity_maps_arm64() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        seed_identity(&fake, "arm64");
        let identity = metadata(&fake)?.identity().await?;
        assert_eq!(identity.isa, Isa::Aarch64);
        Ok(())
    }

    #[tokio::test]
    async fn unknown_architecture_is_malformed() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().identity_document = Some(identity_document("riscv64"));
        let result = metadata(&fake)?.identity().await;
        assert!(matches!(
            result,
            Err(AwsError::MalformedResponse {
                what: "supported architecture",
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn identity_is_fetched_once() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        seed_identity(&fake, "x86_64");
        let meta = metadata(&fake)?;
        let first = meta.identity().await?;
        let second = meta.identity().await?;
        assert_eq!(first, second);
        assert_eq!(document_requests(&fake), 1);
        Ok(())
    }

    #[tokio::test]
    async fn missing_identity_document_is_an_error() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        let result = metadata(&fake)?.identity().await;
        assert!(matches!(result, Err(AwsError::Refused { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn fleet_tag_is_memoized_with_the_identity() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        seed_identity(&fake, "x86_64");
        let meta = metadata(&fake)?;
        let first = meta.identity().await?;
        let second = meta.identity().await?;
        assert_eq!(first.fleet, second.fleet);
        assert_eq!(tag_requests(&fake, FleetTagKey::DEFAULT), 1);
        Ok(())
    }

    #[tokio::test]
    async fn missing_fleet_tag_is_a_config_error() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().identity_document = Some(identity_document("x86_64"));
        let result = metadata(&fake)?.identity().await;
        assert!(matches!(
            result,
            Err(AwsError::Config {
                field: "fleet instance tag",
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn invalid_fleet_tag_is_a_config_error() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        seed_identity(&fake, "x86_64");
        fake.state().fleet_tag = Some("Not A Fleet".to_owned());
        let result = metadata(&fake)?.identity().await;
        assert!(matches!(
            result,
            Err(AwsError::Config {
                field: "fleet instance tag",
                reason,
            }) if reason.contains(FleetTagKey::DEFAULT) && reason.contains("Not A Fleet"),
        ));
        Ok(())
    }

    #[tokio::test]
    async fn custom_fleet_tag_key_is_the_one_requested() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        seed_identity(&fake, "x86_64");
        let identity = Metadata::new(fake.imds_client()?, CUSTOM_KEY)
            .identity()
            .await?;
        assert_eq!(identity.fleet, FleetName::try_new(FLEET)?);
        assert_eq!(tag_requests(&fake, CUSTOM_KEY), 1);
        assert_eq!(tag_requests(&fake, FleetTagKey::DEFAULT), 0);
        Ok(())
    }

    #[tokio::test]
    async fn missing_custom_fleet_tag_names_the_key() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().identity_document = Some(identity_document("x86_64"));
        let result = Metadata::new(fake.imds_client()?, CUSTOM_KEY)
            .identity()
            .await;
        assert!(matches!(
            result,
            Err(AwsError::Config {
                field: "fleet instance tag",
                reason,
            }) if reason.contains(CUSTOM_KEY)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn no_spot_action_means_no_preemption() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        assert_eq!(metadata(&fake)?.preemption_notice().await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_action_reports_its_time() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().spot_instance_action =
            Some(r#"{"action":"terminate","time":"2026-01-01T00:00:00Z"}"#.to_owned());
        let notice = metadata(&fake)?.preemption_notice().await?;
        let Some(notice) = notice else {
            return Err("expected a preemption notice".into());
        };
        assert_eq!(notice.terminate_at.as_unix_millis(), TERMINATE_AT_MILLIS);
        Ok(())
    }

    #[tokio::test]
    async fn stop_action_also_counts() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().spot_instance_action =
            Some(r#"{"action":"stop","time":"2026-01-01T00:00:00Z"}"#.to_owned());
        let notice = metadata(&fake)?.preemption_notice().await?;
        assert!(notice.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn garbage_time_is_malformed() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().spot_instance_action =
            Some(r#"{"action":"terminate","time":"not a time"}"#.to_owned());
        let result = metadata(&fake)?.preemption_notice().await;
        assert!(matches!(
            result,
            Err(AwsError::MalformedResponse {
                what: "preemption time",
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn malformed_json_is_malformed() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().spot_instance_action = Some(r#"{"action":"#.to_owned());
        let result = metadata(&fake)?.preemption_notice().await;
        assert!(matches!(result, Err(AwsError::MalformedResponse { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn warmed_stopped_is_parked() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().target_lifecycle_state = Some("Warmed:Stopped".to_owned());
        assert!(metadata(&fake)?.parked().await?);
        Ok(())
    }

    #[tokio::test]
    async fn warmed_pending_is_parked() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().target_lifecycle_state = Some("Warmed:Pending".to_owned());
        assert!(metadata(&fake)?.parked().await?);
        Ok(())
    }

    #[tokio::test]
    async fn in_service_is_not_parked() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().target_lifecycle_state = Some("InService".to_owned());
        assert!(!metadata(&fake)?.parked().await?);
        Ok(())
    }

    #[tokio::test]
    async fn absent_target_state_is_not_parked() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        assert!(!metadata(&fake)?.parked().await?);
        Ok(())
    }

    #[tokio::test]
    async fn transient_target_state_failure_is_transient() -> Result<(), Box<dyn std::error::Error>>
    {
        let fake = FakeAws::start().await?;
        fake.state().failures.insert(
            "ImdsGet",
            FakeFailure {
                status: 503,
                code: "ServiceUnavailable",
                message: "unavailable",
            },
        );
        let result = metadata(&fake)?.parked().await;
        assert!(matches!(result, Err(AwsError::Transient { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn refused_target_state_failure_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeAws::start().await?;
        fake.state().failures.insert(
            "ImdsGet",
            FakeFailure {
                status: 403,
                code: "AccessDenied",
                message: "denied",
            },
        );
        let result = metadata(&fake)?.parked().await;
        assert!(matches!(result, Err(AwsError::Refused { .. })));
        Ok(())
    }
}
