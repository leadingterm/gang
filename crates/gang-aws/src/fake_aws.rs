//! In-process fake of the AWS endpoints the plugins call, served over loopback HTTP.
//!
//! One axum router answers Auto Scaling, EC2 and STS (awsQuery), KMS (awsJson1.1) and
//! the `IMDSv2` paths. Tests seed [`FakeState`], run the real SDK clients against
//! [`FakeAws::sdk_config`], then inspect what the fake recorded.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io;
use std::num::NonZeroU32;
use std::sync::Arc;

use aws_config::SdkConfig;
use aws_credential_types::Credentials;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use gang_clock::{TestClock, Timestamp};
use gang_types::NonZeroDuration;
use p256::ecdsa::signature::{Signer as _, hazmat::PrehashSigner as _};
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::EncodePublicKey as _;
use parking_lot::{Mutex, MutexGuard};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::errors::AwsError;
use crate::lifecycle::imds_client;
use gang_aws_sdk::{ClientSettings, Region, http_client, loader};

const LOOPBACK_BIND: &str = "127.0.0.1:0";
const REGION: &str = "us-east-1";
const ACCESS_KEY_ID: &str = "AKIDEXAMPLE";
const SECRET_ACCESS_KEY: &str = "secretexample";
/// Bound for every client call to the fake: it never elapses within a test, so no verdict depends on wall-clock time.
const UNREACHABLE_DEADLINE: NonZeroDuration = NonZeroDuration::from_hours(24);

const IMDS_TOKEN: &str = "fake-token";
const IMDS_TOKEN_PATH: &str = "/latest/api/token";
const IDENTITY_PATH: &str = "/latest/dynamic/instance-identity/document";
const SPOT_ACTION_PATH: &str = "/latest/meta-data/spot/instance-action";
const FLEET_TAG_PATH_PREFIX: &str = "/latest/meta-data/tags/instance/";
const SESSION_TOKEN_HEADER: &str = "x-aws-ec2-metadata-token";
const IMDS_TOKEN_TTL_HEADER: &str = "x-aws-ec2-metadata-token-ttl-seconds";
const IMDS_TOKEN_TTL_SECONDS: &str = "21600";
const IMDS_GET_OPERATION: &str = "ImdsGet";
const IMDS_TOKEN_OPERATION: &str = "ImdsToken";
const PATH_PARAMETER: &str = "path";

const KMS_TARGET_HEADER: &str = "x-amz-target";
const KMS_TARGET_PREFIX: &str = "TrentService.";
const KMS_CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const ERROR_TYPE_HEADER: &str = "x-amzn-errortype";

const AUTO_SCALING_NS: &str = "http://autoscaling.amazonaws.com/doc/2011-01-01/";
const EC2_NS: &str = "http://ec2.amazonaws.com/doc/2016-11-15/";
const STS_NS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const ELBV2_NS: &str = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/";
const REQUEST_ID: &str = "00000000-0000-0000-0000-000000000000";
const ACTIVITY_ID: &str = "00000000-0000-0000-0000-000000000001";
const ACTIVITY_STATUS: &str = "InProgress";
const CREATED_TIME: &str = "2026-11-06T00:00:00Z";
const EXPIRATION: &str = "2026-11-06T01:00:00Z";
const ZONE: &str = "us-east-1a";
const ACCOUNT_ID: &str = "000000000000";
const RESERVATION_ID: &str = "r-00000000000000000";
const INSTANCE_TYPE: &str = "c7g.large";
const DEFAULT_COOLDOWN: u32 = 300;
/// Largest size a group accepts unless it is seeded otherwise: the AWS account default.
const GROUP_MAX_SIZE: i32 = 1000;

const VALIDATION_ERROR: &str = "ValidationError";
const SCALING_ACTIVITY_IN_PROGRESS: &str = "ScalingActivityInProgress";
const NOT_FOUND_EXCEPTION: &str = "NotFoundException";
const INVALID_ACTION: &str = "InvalidAction";
const MISSING_PARAMETER: &str = "required parameter is missing";
const MALFORMED_NUMBER: &str = "parameter is not a number";
const MALFORMED_TIMESTAMP: &str = "parameter is not an RFC 3339 timestamp";
const UNKNOWN_GROUP: &str = "scaling group does not exist";
const UNKNOWN_INSTANCE: &str = "instance is not in any scaling group";
const UNKNOWN_OPERATION: &str = "operation is not faked";
const OUT_OF_BOUNDS: &str = "desired capacity is outside the group's minimum and maximum size";
const COOLDOWN_ACTIVE: &str = "a scaling activity is in progress";
/// Token `GetWebIdentityToken` serves until a test sets another; a well-formed JWT-shaped value.
const DEFAULT_WEB_IDENTITY_TOKEN: &str = "header.payload.signature";
const ACCESS_DENIED: &str = "AccessDenied";
const KMS_ACCESS_DENIED: &str = "AccessDeniedException";
const DENIED_MESSAGE: &str = "operation is not granted to this role";
const DENIED_STATUS: u16 = 403;
/// Operations the controller role is granted: the IAM list in `plugin.rs`, plus KMS and STS.
const GRANTED_ACTIONS: &[&str] = &[
    "DescribeAutoScalingGroups",
    "SetDesiredCapacity",
    "TerminateInstanceInAutoScalingGroup",
    "DescribeInstances",
    "GetWebIdentityToken",
    "GetPublicKey",
    "Sign",
    "SetInstanceProtection",
    "DescribeScalingActivities",
    "DescribeLifecycleHooks",
    "CompleteLifecycleAction",
    "RecordLifecycleActionHeartbeat",
    "DescribeTargetHealth",
    "RegisterTargets",
    "ReceiveMessage",
    "DeleteMessage",
];
const UNKNOWN_KEY: &str = "key does not exist";
const UNAUTHORIZED: &str = "missing or invalid session token";
const NO_METADATA: &str = "no metadata at path";
const TARGET_LIFECYCLE_PATH: &str = "/latest/meta-data/autoscaling/target-lifecycle-state";
const LIFECYCLE_IN_SERVICE: &str = "InService";
const LIFECYCLE_PENDING: &str = "Pending";
const LAUNCHED_ID_PREFIX: &str = "i-launched-";
const ACTIVITY_ID_PREFIX: &str = "00000000-0000-0000-0002-";
const FILTERS_PREFIX: &str = "Filters.member.";
const STATUS_FILTER: &str = "Status";
const START_TIME_FILTER: &str = "StartTimeLowerBound";
const HONOR_COOLDOWN: &str = "HonorCooldown";
const NEXT_TOKEN: &str = "NextToken";
/// The `DescribeInstances` filter naming the Auto Scaling groups whose members are wanted.
const GROUP_NAME_TAG: &str = "tag:aws:autoscaling:groupName";
const EC2_FILTER_PREFIX: &str = "Filter.";
/// Page size of `DescribeScalingActivities` when `MaxRecords` is absent: the AWS default.
const DEFAULT_MAX_RECORDS: usize = 100;

/// Request parameters: decoded form fields, or top-level string fields of a JSON body.
type Parameters = BTreeMap<String, String>;
/// State shared by the server task and the test that owns the fake.
type Shared = Arc<Mutex<FakeState>>;

/// A running fake AWS endpoint. Dropping it stops the server.
pub(crate) struct FakeAws {
    endpoint: String,
    region: Region,
    state: Shared,
    server: JoinHandle<()>,
}

impl Drop for FakeAws {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeAws {
    /// Binds a loopback listener and starts serving the fake endpoints.
    pub(crate) async fn start() -> Result<Self, io::Error> {
        let listener = TcpListener::bind(LOOPBACK_BIND).await?;
        let address = listener.local_addr()?;
        let region = Region::try_new(REGION).map_err(io::Error::other)?;
        let state: Shared = Arc::new(Mutex::new(FakeState::default()));
        let router = Router::new()
            .route("/", post(post_root))
            .route(IMDS_TOKEN_PATH, put(imds_token))
            .route(IDENTITY_PATH, get(identity_document))
            .route(SPOT_ACTION_PATH, get(spot_instance_action))
            .route(&format!("{FLEET_TAG_PATH_PREFIX}{{key}}"), get(fleet_tag))
            .route(TARGET_LIFECYCLE_PATH, get(target_lifecycle_state))
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(Self {
            endpoint: format!("http://{address}"),
            region,
            state,
            server,
        })
    }

    pub(crate) fn queue_url(&self) -> &str {
        &self.endpoint
    }

    /// Returns an SDK configuration whose clients talk to this fake with static credentials, one
    /// attempt per call and no timeout in reach.
    pub(crate) async fn sdk_config(&self) -> SdkConfig {
        self.sdk_config_with(&ClientSettings {
            request_timeout: UNREACHABLE_DEADLINE,
            connect_timeout: UNREACHABLE_DEADLINE,
            max_attempts: NonZeroU32::MIN,
            retry_initial_backoff: NonZeroDuration::from_millis(1),
            retry_max_backoff: NonZeroDuration::from_millis(1),
        })
        .await
    }

    /// Returns an SDK configuration whose clients talk to this fake with static credentials and
    /// the given timeouts, attempt budget and retry backoff.
    pub(crate) async fn sdk_config_with(&self, settings: &ClientSettings) -> SdkConfig {
        loader(&self.region, settings)
            .endpoint_url(self.endpoint.as_str())
            .credentials_provider(Credentials::new(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY,
                None,
                None,
                "fake",
            ))
            .load()
            .await
    }

    /// Returns an IMDS client that talks to this fake.
    pub(crate) fn imds_client(&self) -> Result<aws_config::imds::Client, AwsError> {
        imds_client(http_client(), UNREACHABLE_DEADLINE, Some(&self.endpoint))
    }

    /// Returns a client with a test-controlled request timeout.
    pub(crate) fn imds_client_with_timeout(
        &self,
        timeout: NonZeroDuration,
    ) -> Result<aws_config::imds::Client, AwsError> {
        imds_client(http_client(), timeout, Some(&self.endpoint))
    }

    /// Stops the endpoint so subsequent requests fail with a connection I/O error.
    pub(crate) async fn stop(&mut self) {
        self.server.abort();
        let _ = (&mut self.server).await;
    }

    /// Locks the fake's state for seeding or inspection.
    ///
    /// The server locks the same state while answering, so hold the guard only briefly and
    /// never across an SDK call.
    pub(crate) fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TokenFailure {
    Timeout,
}

/// Everything the fake answers from. Tests seed it and inspect `requests`.
#[derive(Debug, Clone)]
pub(crate) struct FakeState {
    /// Auto Scaling groups, in the order they are described.
    pub(crate) groups: Vec<FakeGroup>,
    /// Lifecycle hooks by group, as (name, transition).
    pub(crate) lifecycle_hooks: HashMap<String, Vec<(String, String)>>,
    /// Completed lifecycle actions, as (group, hook, instance).
    pub(crate) lifecycle_completions: Vec<(String, String, String)>,
    /// Apply the next completion but return a transient failure, modeling a lost response.
    pub(crate) lose_completion_response: bool,
    /// Hook heartbeat timeout returned by lifecycle discovery.
    pub(crate) hook_heartbeat_seconds: u32,
    /// Target health by target group ARN and instance id.
    pub(crate) target_health: HashMap<String, Vec<(String, String)>>,
    /// EC2 instance id to RFC 3339 launch time; EC2 knows only these instances.
    pub(crate) launch_times: HashMap<String, String>,
    /// EC2 state-reason code by instance, including recently terminated instances.
    pub(crate) state_reasons: HashMap<String, String>,
    /// The KMS key answered for `GetPublicKey` and `Sign`.
    pub(crate) kms: Option<FakeKey>,
    /// Token returned by `GetWebIdentityToken`.
    pub(crate) web_identity_token: String,
    /// Body of the instance identity document; `None` answers 404.
    pub(crate) identity_document: Option<String>,
    /// Body of the spot instance-action document; `None` answers 404.
    pub(crate) spot_instance_action: Option<String>,
    /// Value of every instance tag read through the metadata service; `None` answers 404.
    pub(crate) fleet_tag: Option<String>,
    /// Auto Scaling group instance tag; `None` answers 404.
    pub(crate) group_tag: Option<String>,
    /// Failures by operation name; each matching call is answered with the failure.
    pub(crate) failures: HashMap<&'static str, FakeFailure>,
    /// Transport failure injected into the IMDS token request.
    pub(crate) token_failure: Option<TokenFailure>,
    /// Signals that the token endpoint has received a request.
    pub(crate) token_started: Arc<tokio::sync::Notify>,
    /// Every request received, in order.
    pub(crate) requests: Vec<FakeRequest>,
    /// Clock to advance after the next protection write is answered.
    pub(crate) advance_after_protection: Option<Arc<TestClock>>,
    /// Scaling activities answered by `DescribeScalingActivities`, in the order they were recorded.
    pub(crate) activities: Vec<FakeActivity>,
    /// Value served at the target lifecycle state path; `None` answers 404.
    pub(crate) target_lifecycle_state: Option<String>,
    /// Instances the simulation has launched so far; names new instance ids.
    pub(crate) launched: u32,
    /// Instances per `DescribeInstances` page; `None` answers every matching instance in one page.
    pub(crate) page_size: Option<usize>,
    /// Messages available to the fake SQS queue.
    pub(crate) messages: Vec<FakeMessage>,
    /// Additional activity identity and description, indexed by activity position.
    pub(crate) activity_details: HashMap<usize, (String, String, String)>,
}

impl Default for FakeState {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            lifecycle_hooks: HashMap::new(),
            lifecycle_completions: Vec::new(),
            lose_completion_response: false,
            hook_heartbeat_seconds: crate::host::HOOK_HEARTBEAT_SECONDS,
            target_health: HashMap::new(),
            launch_times: HashMap::new(),
            state_reasons: HashMap::new(),
            kms: None,
            web_identity_token: DEFAULT_WEB_IDENTITY_TOKEN.to_owned(),
            identity_document: None,
            spot_instance_action: None,
            fleet_tag: None,
            group_tag: None,
            failures: HashMap::new(),
            token_failure: None,
            token_started: Arc::new(tokio::sync::Notify::new()),
            requests: Vec::new(),
            advance_after_protection: None,
            activities: Vec::new(),
            target_lifecycle_state: None,
            launched: 0,
            page_size: None,
            messages: Vec::new(),
            activity_details: HashMap::new(),
        }
    }
}

impl FakeState {
    /// Promotes a pending member of `group_name` into service: it becomes `InService`, protected exactly when the group protects new instances.
    ///
    /// # Panics
    /// When the group or the member does not exist; tests seed both before promoting.
    pub(crate) fn promote(&mut self, group_name: &str, id: &str) {
        let group = self
            .groups
            .iter_mut()
            .find(|candidate| candidate.name == group_name)
            .unwrap_or_else(|| panic!("promote: no group named {group_name}"));
        let protected = group.new_instances_protected;
        let instance = group
            .instances
            .iter_mut()
            .find(|instance| instance.id == id)
            .unwrap_or_else(|| panic!("promote: no member {id}"));
        instance.lifecycle_state = LIFECYCLE_IN_SERVICE.to_owned();
        instance.protected = protected;
    }
}

/// The ASG field that carries its launch template.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FakeTemplateKind {
    Direct,
    Mixed,
}

/// An Auto Scaling group with its current desired capacity and members.
#[derive(Debug, Clone)]
pub(crate) struct FakeGroup {
    pub(crate) name: String,
    pub(crate) desired: i32,
    /// Lowest desired capacity `SetDesiredCapacity` accepts.
    pub(crate) min_size: i32,
    /// Highest desired capacity `SetDesiredCapacity` accepts.
    pub(crate) max_size: i32,
    /// Whether a scaling activity is in progress; `SetDesiredCapacity` with `HonorCooldown` is refused meanwhile.
    pub(crate) in_cooldown: bool,
    pub(crate) instances: Vec<FakeInstance>,
    pub(crate) target_group_arns: Vec<String>,
    /// Members launched with an older launch-template version.
    pub(crate) stale_instances: Vec<String>,
    /// The ASG field that declares the current launch template.
    pub(crate) template_kind: FakeTemplateKind,
    /// Whether instances entering the group start protected from scale-in.
    pub(crate) new_instances_protected: bool,
    /// `None` when the group has no warm pool; `Some(reuse)` when it has one with that reuse flag.
    pub(crate) reuse_on_scale_in: Option<bool>,
    /// Ids of instances parked in the warm pool in `Warmed:Stopped`.
    pub(crate) warm_pool: Vec<String>,
    /// Whether `SetDesiredCapacity` also scales the members; `false` only stores `desired`.
    pub(crate) simulate: bool,
}

impl FakeGroup {
    /// A group with `(id, lifecycle state)` members, all unprotected, without a warm pool and without simulation.
    /// Its size bounds are `0` to `1000` and it starts out of cooldown.
    pub(crate) fn new(name: &str, desired: i32, instances: &[(&str, &str)]) -> Self {
        Self {
            name: name.to_owned(),
            desired,
            min_size: 0,
            max_size: GROUP_MAX_SIZE,
            in_cooldown: false,
            target_group_arns: Vec::new(),
            instances: instances
                .iter()
                .map(|(id, lifecycle_state)| FakeInstance {
                    id: (*id).to_owned(),
                    lifecycle_state: (*lifecycle_state).to_owned(),
                    protected: false,
                })
                .collect(),
            stale_instances: Vec::new(),
            template_kind: FakeTemplateKind::Direct,
            new_instances_protected: false,
            reuse_on_scale_in: None,
            warm_pool: Vec::new(),
            simulate: false,
        }
    }
}

/// A member of an Auto Scaling group.
#[derive(Debug, Clone, Default)]
pub(crate) struct FakeInstance {
    pub(crate) id: String,
    /// Lifecycle state such as `Pending`, `InService` or `Terminating`.
    pub(crate) lifecycle_state: String,
    /// Whether the instance is protected from scale-in. Only the simulated scale-in of
    /// `SetDesiredCapacity` honors it; `TerminateInstanceInAutoScalingGroup` does not.
    pub(crate) protected: bool,
}

/// A scaling activity answered by `DescribeScalingActivities`.
#[derive(Debug, Clone)]
pub(crate) struct FakeActivity {
    pub(crate) group: String,
    pub(crate) status_code: String,
    /// RFC 3339 UTC start time; `StartTimeLowerBound` compares it as an instant.
    pub(crate) start_time: String,
    pub(crate) message: Option<String>,
}

/// One SQS message; tests control visibility without wall-clock sleeps.
#[derive(Debug, Clone)]
pub(crate) struct FakeMessage {
    pub(crate) id: String,
    pub(crate) body: String,
    pub(crate) receipt: String,
    pub(crate) visible: bool,
}

/// A KMS key, addressable by its ARN or alias.
#[derive(Debug, Clone)]
pub(crate) struct FakeKey {
    pub(crate) arn: String,
    pub(crate) alias: String,
    pub(crate) key_spec: String,
    pub(crate) key_usage: String,
    /// Algorithms answered by `GetPublicKey`; a key built for ECDSA answers `["ECDSA_SHA_256"]`.
    pub(crate) signing_algorithms: Vec<String>,
    /// Private key used to answer KMS signing requests.
    pub(crate) signing_key: SigningKey,
}

/// An error answered in place of an operation's success response.
#[derive(Debug, Clone)]
pub(crate) struct FakeFailure {
    pub(crate) status: u16,
    pub(crate) code: &'static str,
    pub(crate) message: &'static str,
}

/// One request as the fake received it.
#[derive(Debug, Clone)]
pub(crate) struct FakeRequest {
    pub(crate) operation: String,
    pub(crate) parameters: BTreeMap<String, String>,
}

/// Which awsQuery service a request belongs to; decides namespace and error shape.
#[derive(Clone, Copy)]
enum QueryService {
    AutoScaling,
    Ec2,
    Sts,
    Elbv2,
}

impl QueryService {
    fn of(action: &str) -> Self {
        match action {
            "DescribeInstances" => Self::Ec2,
            "GetWebIdentityToken" => Self::Sts,
            "DescribeTargetHealth" | "RegisterTargets" => Self::Elbv2,
            _ => Self::AutoScaling,
        }
    }

    fn namespace(self) -> &'static str {
        match self {
            Self::AutoScaling => AUTO_SCALING_NS,
            Self::Ec2 => EC2_NS,
            Self::Sts => STS_NS,
            Self::Elbv2 => ELBV2_NS,
        }
    }
}

fn status_of(code: u16) -> StatusCode {
    StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

fn invalid(message: &'static str) -> FakeFailure {
    FakeFailure {
        status: 400,
        code: VALIDATION_ERROR,
        message,
    }
}

fn required<'a>(params: &'a Parameters, key: &str) -> Result<&'a str, FakeFailure> {
    params
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| invalid(MISSING_PARAMETER))
}

fn members<'a>(params: &'a Parameters, prefix: &str) -> Vec<&'a str> {
    params
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .map(|(_, value)| value.as_str())
        .collect()
}

/// Parses an RFC 3339 instant, refusing text that is not one.
fn timestamp(text: &str) -> Result<Timestamp, FakeFailure> {
    Timestamp::from_rfc3339(text).map_err(|_| invalid(MALFORMED_TIMESTAMP))
}

/// Runs one operation under the state lock: records the request, refuses it with `denied` when
/// the operation is IAM-gated and not in `GRANTED_ACTIONS`, then answers either the configured
/// failure for the operation or the result of `handle`. `None` leaves the operation ungated.
fn dispatch(
    shared: &Shared,
    operation: &str,
    parameters: &Parameters,
    denied: Option<FakeFailure>,
    handle: impl FnOnce(&mut FakeState, &Parameters) -> Result<String, FakeFailure>,
) -> Result<String, FakeFailure> {
    let mut state = shared.lock();
    state.requests.push(FakeRequest {
        operation: operation.to_owned(),
        parameters: parameters.clone(),
    });
    if let Some(refusal) = denied.filter(|_| !GRANTED_ACTIONS.contains(&operation)) {
        return Err(refusal);
    }
    if let Some(failure) = state.failures.get(operation).cloned() {
        return Err(failure);
    }
    let result = handle(&mut state, parameters);
    if operation == "SetInstanceProtection"
        && let Some(clock) = state.advance_after_protection.take()
    {
        clock.advance(std::time::Duration::from_secs(1));
    }
    result
}

/// The refusal for an operation the controller role is not granted.
fn access_denied(code: &'static str) -> FakeFailure {
    FakeFailure {
        status: DENIED_STATUS,
        code,
        message: DENIED_MESSAGE,
    }
}

/// `POST /`: KMS when the target header is present, awsQuery otherwise.
async fn post_root(State(state): State<Shared>, headers: HeaderMap, body: Bytes) -> Response {
    match headers.get(KMS_TARGET_HEADER) {
        Some(target)
            if target
                .to_str()
                .is_ok_and(|text| text.starts_with("AmazonSQS.")) =>
        {
            sqs(&state, target.to_str().unwrap_or_default(), &body)
        }
        Some(target) => kms(&state, target, &body),
        None => query(&state, &body),
    }
}

fn sqs(state: &Shared, target: &str, body: &[u8]) -> Response {
    let operation = target.strip_prefix("AmazonSQS.").unwrap_or_default();
    let value: Value = serde_json::from_slice(body).unwrap_or_default();
    let mut parameters = json_parameters(body);
    for name in [
        "MaxNumberOfMessages",
        "VisibilityTimeout",
        "WaitTimeSeconds",
    ] {
        if let Some(number) = value.get(name).and_then(Value::as_i64) {
            parameters.insert(name.to_owned(), number.to_string());
        }
    }
    let outcome = dispatch(
        state,
        operation,
        &parameters,
        Some(access_denied(ACCESS_DENIED)),
        |fake, params| match operation {
            "ReceiveMessage" => {
                let count = params
                    .get("MaxNumberOfMessages")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(1);
                let messages: Vec<Value> = fake.messages.iter_mut().filter(|message| message.visible).take(count).map(|message| {
                    message.visible = false;
                    json!({"MessageId": message.id, "ReceiptHandle": message.receipt, "Body": message.body})
                }).collect();
                Ok(json!({"Messages": messages}).to_string())
            }
            "DeleteMessage" => {
                let receipt = required(params, "ReceiptHandle")?;
                fake.messages.retain(|message| message.receipt != receipt);
                Ok("{}".to_owned())
            }
            _ => Err(invalid(UNKNOWN_OPERATION)),
        },
    );
    match outcome {
        Ok(body) => ([(header::CONTENT_TYPE, "application/x-amz-json-1.0")], body).into_response(),
        Err(error) => (
            status_of(error.status),
            [(header::CONTENT_TYPE, "application/x-amz-json-1.0")],
            json!({"__type": error.code, "message": error.message}).to_string(),
        )
            .into_response(),
    }
}
fn query(state: &Shared, body: &[u8]) -> Response {
    let parameters = parse_form(body);
    let action = parameters.get("Action").cloned().unwrap_or_default();
    let outcome = dispatch(
        state,
        &action,
        &parameters,
        Some(access_denied(ACCESS_DENIED)),
        |fake, params| {
            let response = query_operation(fake, &action, params)?;
            if action == "CompleteLifecycleAction" && fake.lose_completion_response {
                fake.lose_completion_response = false;
                return Err(FakeFailure {
                    status: 500,
                    code: "InternalFailure",
                    message: "completion response lost",
                });
            }
            Ok(response)
        },
    );
    let service = QueryService::of(&action);
    match outcome {
        Ok(xml) => xml_response(StatusCode::OK, xml),
        Err(failure) => xml_response(status_of(failure.status), query_error(service, &failure)),
    }
}

fn query_operation(
    state: &mut FakeState,
    action: &str,
    params: &Parameters,
) -> Result<String, FakeFailure> {
    match action {
        "DescribeLifecycleHooks" => {
            let group = required(params, "AutoScalingGroupName")?;
            let mut hooks = String::new();
            for (name, transition) in state.lifecycle_hooks.get(group).into_iter().flatten() {
                write!(hooks, "<member><LifecycleHookName>{name}</LifecycleHookName><AutoScalingGroupName>{group}</AutoScalingGroupName><LifecycleTransition>{transition}</LifecycleTransition><HeartbeatTimeout>{}</HeartbeatTimeout></member>", state.hook_heartbeat_seconds).expect("writing to a String cannot fail");
            }
            Ok(query_ok(
                QueryService::AutoScaling,
                action,
                Some(&format!("<LifecycleHooks>{hooks}</LifecycleHooks>")),
            ))
        }
        "DescribeTargetHealth" => {
            let arn = required(params, "TargetGroupArn")?;
            let targets = state.target_health.get(arn).ok_or_else(|| invalid("unknown target group"))?;
            let descriptions: String = targets.iter().map(|(id, health)| format!("<member><Target><Id>{id}</Id></Target><TargetHealth><State>{health}</State></TargetHealth></member>")).collect();
            Ok(query_ok(QueryService::Elbv2, action, Some(&format!("<TargetHealthDescriptions>{descriptions}</TargetHealthDescriptions>"))))
        }
        "RegisterTargets" => {
            let arn = required(params, "TargetGroupArn")?;
            let id = required(params, "Targets.member.1.Id")?;
            let targets = state.target_health.get_mut(arn).ok_or_else(|| invalid("unknown target group"))?;
            if !targets.iter().any(|(target, _)| target == id) {
                targets.push((id.to_owned(), "initial".to_owned()));
            }
            Ok(query_ok(QueryService::Elbv2, action, None))
        }
        "RecordLifecycleActionHeartbeat" => {
            let group_name = required(params, "AutoScalingGroupName")?;
            let hook_name = required(params, "LifecycleHookName")?;
            let id = required(params, "InstanceId")?;
            let valid_hook = state.lifecycle_hooks.get(group_name).is_some_and(|hooks| hooks.iter().any(|(name, transition)| name == hook_name && transition == "autoscaling:EC2_INSTANCE_TERMINATING"));
            let waiting = state.groups.iter().find(|group| group.name == group_name).is_some_and(|group| group.instances.iter().any(|instance| instance.id == id && instance.lifecycle_state == "Terminating:Wait"));
            if !valid_hook || !waiting { return Err(invalid("no active lifecycle action")); }
            Ok(query_ok(QueryService::AutoScaling, action, None))
        }
        "CompleteLifecycleAction" => {
            let no_action = || invalid("no active lifecycle action");
            let group_name = params.get("AutoScalingGroupName").ok_or_else(no_action)?;
            let hook_name = params.get("LifecycleHookName").ok_or_else(no_action)?;
            let instance_id = params.get("InstanceId").ok_or_else(no_action)?;
            let result = required(params, "LifecycleActionResult")?;
            if !matches!(result, "CONTINUE" | "ABANDON") {
                return Err(invalid("invalid lifecycle action result"));
            }
            let transition = state
                .lifecycle_hooks
                .get(group_name)
                .and_then(|hooks| hooks.iter().find(|(name, _)| name == hook_name))
                .map(|(_, transition)| transition.as_str())
                .ok_or_else(no_action)?;
            let (waiting, proceeding) = match transition {
                "autoscaling:EC2_INSTANCE_LAUNCHING" => ("Pending:Wait", "Pending:Proceed"),
                "autoscaling:EC2_INSTANCE_TERMINATING" => {
                    ("Terminating:Wait", "Terminating:Proceed")
                }
                _ => return Err(no_action()),
            };
            let member = state
                .groups
                .iter_mut()
                .find(|group| group.name == *group_name)
                .and_then(|group| {
                    group
                        .instances
                        .iter_mut()
                        .find(|member| member.id == *instance_id)
                })
                .filter(|member| member.lifecycle_state == waiting)
                .ok_or_else(no_action)?;
            member.lifecycle_state = if result == "ABANDON" {
                "Terminating:Proceed"
            } else {
                proceeding
            }
            .to_owned();
            state.lifecycle_completions.push((
                group_name.clone(),
                hook_name.clone(),
                instance_id.clone(),
            ));
            Ok(query_ok(QueryService::AutoScaling, action, None))
        }
        "DescribeAutoScalingGroups" => Ok(query_ok(
            QueryService::AutoScaling,
            action,
            Some(&describe_groups(state, params)),
        )),
        "SetDesiredCapacity" => {
            set_desired_capacity(state, params)?;
            Ok(query_ok(QueryService::AutoScaling, action, None))
        }
        "TerminateInstanceInAutoScalingGroup" => {
            let activity = terminate(state, params)?;
            Ok(query_ok(QueryService::AutoScaling, action, Some(&activity)))
        }
        "SetInstanceProtection" => {
            set_instance_protection(state, params)?;
            Ok(query_ok(QueryService::AutoScaling, action, None))
        }
        "DescribeScalingActivities" => Ok(query_ok(
            QueryService::AutoScaling,
            action,
            Some(&describe_activities(state, params)?),
        )),
        "DescribeInstances" => Ok(query_ok(
            QueryService::Ec2,
            action,
            Some(&describe_ec2(state, params)?),
        )),
        "GetWebIdentityToken" => Ok(query_ok(
            QueryService::Sts,
            action,
            Some(&web_identity_token(state)),
        )),
        _ => Err(FakeFailure {
            status: 400,
            code: INVALID_ACTION,
            message: UNKNOWN_OPERATION,
        }),
    }
}

fn describe_groups(state: &FakeState, params: &Parameters) -> String {
    let requested = members(params, "AutoScalingGroupNames.member.");
    let groups: String = state
        .groups
        .iter()
        .filter(|group| requested.is_empty() || requested.contains(&group.name.as_str()))
        .map(group_xml)
        .collect();
    format!("<AutoScalingGroups>{groups}</AutoScalingGroups>")
}

/// Describes one group: its size bounds, its members with their protection, the protection of new instances and, when the group has a warm pool, the reuse flag and pool size.
fn group_xml(group: &FakeGroup) -> String {
    let instances: String = group
        .instances
        .iter()
        .map(|instance| {
            format!(
                "<member><InstanceId>{id}</InstanceId><AvailabilityZone>{ZONE}</AvailabilityZone><HealthStatus>Healthy</HealthStatus><LifecycleState>{state}</LifecycleState><ProtectedFromScaleIn>{protected}</ProtectedFromScaleIn><LaunchTemplate><LaunchTemplateId>lt-test</LaunchTemplateId><Version>{version}</Version></LaunchTemplate></member>",
                id = instance.id,
                state = instance.lifecycle_state,
                protected = instance.protected,
                version = if group.stale_instances.contains(&instance.id) { "1" } else { "2" },
            )
        })
        .collect::<Vec<_>>()
        .concat();
    let warm_pool = match group.reuse_on_scale_in {
        Some(reuse) => format!(
            "<WarmPoolConfiguration><PoolState>Stopped</PoolState><InstanceReusePolicy><ReuseOnScaleIn>{reuse}</ReuseOnScaleIn></InstanceReusePolicy></WarmPoolConfiguration><WarmPoolSize>{size}</WarmPoolSize>",
            size = group.warm_pool.len(),
        ),
        None => String::new(),
    };
    let template = match group.template_kind {
        FakeTemplateKind::Mixed => {
            "<MixedInstancesPolicy><LaunchTemplate><LaunchTemplateSpecification><LaunchTemplateId>lt-test</LaunchTemplateId><Version>2</Version></LaunchTemplateSpecification></LaunchTemplate></MixedInstancesPolicy>"
        }
        FakeTemplateKind::Direct => {
            "<LaunchTemplate><LaunchTemplateId>lt-test</LaunchTemplateId><Version>2</Version></LaunchTemplate>"
        }
    };
    format!(
        "<member><AutoScalingGroupName>{name}</AutoScalingGroupName><AvailabilityZones><member>{ZONE}</member></AvailabilityZones><CreatedTime>{CREATED_TIME}</CreatedTime><DefaultCooldown>{DEFAULT_COOLDOWN}</DefaultCooldown><DesiredCapacity>{desired}</DesiredCapacity><MinSize>{min_size}</MinSize><MaxSize>{max_size}</MaxSize><HealthCheckType>EC2</HealthCheckType><Instances>{instances}</Instances><TargetGroupARNs>{target_groups}</TargetGroupARNs><NewInstancesProtectedFromScaleIn>{protected}</NewInstancesProtectedFromScaleIn>{warm_pool}{template}</member>",
        name = group.name,
        desired = group.desired,
        min_size = group.min_size,
        max_size = group.max_size,
        protected = group.new_instances_protected,
        target_groups = group.target_group_arns.iter().map(|arn| format!("<member>{arn}</member>")).collect::<String>(),
    )
}

/// Answers `SetDesiredCapacity`: the desired capacity must lie within the group's bounds, and `HonorCooldown` is refused while a scaling activity is in progress. An accepted write stores the capacity and, for a simulated group, scales its members.
fn set_desired_capacity(state: &mut FakeState, params: &Parameters) -> Result<(), FakeFailure> {
    let name = required(params, "AutoScalingGroupName")?;
    let desired: i32 = required(params, "DesiredCapacity")?
        .parse()
        .map_err(|_| invalid(MALFORMED_NUMBER))?;
    let honor_cooldown = params
        .get(HONOR_COOLDOWN)
        .is_some_and(|value| value == "true");
    let group = state
        .groups
        .iter_mut()
        .find(|group| group.name == name)
        .ok_or_else(|| invalid(UNKNOWN_GROUP))?;
    if !(group.min_size..=group.max_size).contains(&desired) {
        return Err(invalid(OUT_OF_BOUNDS));
    }
    if honor_cooldown && group.in_cooldown {
        return Err(FakeFailure {
            status: 400,
            code: SCALING_ACTIVITY_IN_PROGRESS,
            message: COOLDOWN_ACTIVE,
        });
    }
    group.desired = desired;
    if group.simulate {
        simulate(group, &mut state.launched, &state.launch_times)?;
    }
    Ok(())
}

/// The simulated scale-in victim: the unprotected `InService` member with the oldest launch time, where a member without a launch time counts as oldest. Ties go to the lowest id. Returns its index in the group. Only simulated scale-in uses this rule; explicit termination takes the named instance regardless of protection.
fn scale_in_victim(
    group: &FakeGroup,
    launch_times: &HashMap<String, String>,
) -> Result<Option<usize>, FakeFailure> {
    let mut victim: Option<(Option<Timestamp>, &str, usize)> = None;
    for (index, instance) in group.instances.iter().enumerate() {
        if instance.lifecycle_state != LIFECYCLE_IN_SERVICE || instance.protected {
            continue;
        }
        let launched = launch_times
            .get(&instance.id)
            .map(|text| timestamp(text))
            .transpose()?;
        let candidate = (launched, instance.id.as_str(), index);
        if victim
            .as_ref()
            .is_none_or(|best| (candidate.0, candidate.1) < (best.0, best.1))
        {
            victim = Some(candidate);
        }
    }
    Ok(victim.map(|(_, _, index)| index))
}

/// Moves a group towards its desired capacity once: scale-in removes the scale-in victim (parking it in the warm pool when reuse is on), scale-out takes the first warm-pool id or launches a new `Pending` member.
fn simulate(
    group: &mut FakeGroup,
    launched: &mut u32,
    launch_times: &HashMap<String, String>,
) -> Result<(), FakeFailure> {
    let target = usize::try_from(group.desired).unwrap_or(0);
    while group.instances.len() > target {
        let Some(index) = scale_in_victim(group, launch_times)? else {
            break;
        };
        let removed = group.instances.remove(index);
        if group.reuse_on_scale_in == Some(true) {
            group.warm_pool.push(removed.id);
        }
    }
    while group.instances.len() < target {
        let id = if group.warm_pool.is_empty() {
            *launched += 1;
            format!("{LAUNCHED_ID_PREFIX}{launched}")
        } else {
            group.warm_pool.remove(0)
        };
        group.instances.push(FakeInstance {
            id,
            lifecycle_state: LIFECYCLE_PENDING.to_owned(),
            protected: false,
        });
    }
    Ok(())
}

/// Answers `SetInstanceProtection`: every named member must exist before any of them changes.
fn set_instance_protection(state: &mut FakeState, params: &Parameters) -> Result<(), FakeFailure> {
    let name = required(params, "AutoScalingGroupName")?;
    let protected = required(params, "ProtectedFromScaleIn")? == "true";
    let ids = members(params, "InstanceIds.member.");
    let group = state
        .groups
        .iter_mut()
        .find(|group| group.name == name)
        .ok_or_else(|| invalid(UNKNOWN_GROUP))?;
    if ids
        .iter()
        .any(|id| !group.instances.iter().any(|instance| instance.id == *id))
    {
        return Err(invalid(UNKNOWN_INSTANCE));
    }
    for instance in group
        .instances
        .iter_mut()
        .filter(|instance| ids.contains(&instance.id.as_str()))
    {
        instance.protected = protected;
    }
    Ok(())
}

/// Answers `DescribeScalingActivities`: activities of the named group (all groups when absent) that pass every `Status` and `StartTimeLowerBound` filter, newest first, capped at `MaxRecords`. Start times compare as instants.
fn describe_activities(state: &FakeState, params: &Parameters) -> Result<String, FakeFailure> {
    let named_group = params.get("AutoScalingGroupName").map(String::as_str);
    let statuses = filter_values(params, STATUS_FILTER);
    let since: Option<Timestamp> = filter_values(params, START_TIME_FILTER)
        .into_iter()
        .map(timestamp)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max();
    let limit = match params.get("MaxRecords") {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| invalid(MALFORMED_NUMBER))?,
        None => DEFAULT_MAX_RECORDS,
    };
    let mut matching: Vec<(Timestamp, usize, &FakeActivity)> = Vec::new();
    let ids = members(params, "ActivityIds.member.");
    for (index, activity) in state.activities.iter().enumerate() {
        let activity_id = state.activity_details.get(&index).map_or_else(
            || format!("{ACTIVITY_ID_PREFIX}{index:012}"),
            |details| details.0.clone(),
        );
        if !ids.is_empty() && !ids.contains(&activity_id.as_str()) {
            continue;
        }
        if named_group.is_some_and(|name| activity.group != name) {
            continue;
        }
        if !(statuses.is_empty() || statuses.contains(&activity.status_code.as_str())) {
            continue;
        }
        let start = timestamp(&activity.start_time)?;
        if since.is_some_and(|bound| start < bound) {
            continue;
        }
        matching.push((start, index, activity));
    }
    matching.sort_by(|(left, _, _), (right, _, _)| right.cmp(left));
    let start = params.get(NEXT_TOKEN).map_or(Ok(0), |raw| {
        raw.parse::<usize>().map_err(|_| invalid(MALFORMED_NUMBER))
    })?;
    let end = start.saturating_add(limit).min(matching.len());
    let next = if end < matching.len() {
        format!("<NextToken>{end}</NextToken>")
    } else {
        String::new()
    };
    let items: String = matching
        .into_iter().skip(start).take(limit)
        .map(|(_, index, activity)| {
            let status_message = activity.message.as_ref()
                .map(|message| format!("<StatusMessage>{message}</StatusMessage>"))
                .unwrap_or_default();
            let message = activity.message.as_deref().unwrap_or_default();
            let fallback_id = format!("{ACTIVITY_ID_PREFIX}{index:012}");
            let (id, cause, description) = state.activity_details.get(&index)
                .map_or((fallback_id.as_str(), message, message), |details| (details.0.as_str(), details.1.as_str(), details.2.as_str()));
            format!(
                "<member><ActivityId>{id}</ActivityId><AutoScalingGroupName>{group}</AutoScalingGroupName><Cause>{cause}</Cause><Description>{description}</Description><StartTime>{start}</StartTime><StatusCode>{status}</StatusCode>{status_message}<Progress>0</Progress></member>",
                group = activity.group,
                start = activity.start_time,
                status = activity.status_code,
            )
        })
        .collect::<Vec<_>>()
        .concat();
    Ok(format!("<Activities>{items}</Activities>{next}"))
}

/// Values of every `Filters` entry named `name`, in request order.
fn filter_values<'a>(params: &'a Parameters, name: &str) -> Vec<&'a str> {
    params
        .iter()
        .filter(|(_, value)| value.as_str() == name)
        .filter_map(|(key, _)| key.strip_prefix(FILTERS_PREFIX)?.strip_suffix(".Name"))
        .flat_map(|index| members(params, &format!("{FILTERS_PREFIX}{index}.Values.member.")))
        .collect()
}

/// Answers `TerminateInstanceInAutoScalingGroup` for the named instance. Scale-in protection does not block it, because AWS documents that protection guards only against scale-in and not against manual termination through this call; the instance is removed, and desired capacity drops by one when asked.
fn terminate(state: &mut FakeState, params: &Parameters) -> Result<String, FakeFailure> {
    let id = required(params, "InstanceId")?;
    let decrement = required(params, "ShouldDecrementDesiredCapacity")? == "true";
    let group = state
        .groups
        .iter_mut()
        .find(|group| group.instances.iter().any(|instance| instance.id == id))
        .ok_or_else(|| invalid(UNKNOWN_INSTANCE))?;
    group.instances.retain(|instance| instance.id != id);
    if decrement {
        group.desired = group.desired.saturating_sub(1);
    }
    Ok(query_ok(
        QueryService::AutoScaling,
        "TerminateInstanceInAutoScalingGroup",
        Some(&format!(
            "<Activity><ActivityId>{ACTIVITY_ID}</ActivityId><AutoScalingGroupName>{group}</AutoScalingGroupName><Cause>terminated by fake</Cause><StartTime>{CREATED_TIME}</StartTime><StatusCode>{ACTIVITY_STATUS}</StatusCode><Progress>0</Progress><Description>Terminating EC2 instance: {id}</Description></Activity>",
            group = group.name,
        )),
    ))
}

/// The group names of the `DescribeInstances` filters on `tag:aws:autoscaling:groupName`, or `None` when the request carries no such filter.
fn group_filter_names(params: &Parameters) -> Option<Vec<&str>> {
    let indices: Vec<&str> = params
        .iter()
        .filter(|(_, value)| value.as_str() == GROUP_NAME_TAG)
        .filter_map(|(key, _)| key.strip_prefix(EC2_FILTER_PREFIX)?.strip_suffix(".Name"))
        .collect();
    if indices.is_empty() {
        return None;
    }
    Some(
        indices
            .into_iter()
            .flat_map(|index| members(params, &format!("{EC2_FILTER_PREFIX}{index}.Value.")))
            .collect(),
    )
}

/// Answers `DescribeInstances`: the launched instances, restricted to the members of the groups named by the group filter, at most `page_size` per call. `NextToken` resumes from an earlier page's `nextToken`.
fn describe_ec2(state: &FakeState, params: &Parameters) -> Result<String, FakeFailure> {
    let start = match params.get(NEXT_TOKEN) {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| invalid(MALFORMED_NUMBER))?,
        None => 0,
    };
    let scoped_ids: Option<Vec<&str>> = group_filter_names(params).map(|names| {
        state
            .groups
            .iter()
            .filter(|group| names.contains(&group.name.as_str()))
            .flat_map(|group| group.instances.iter().map(|instance| instance.id.as_str()))
            .collect()
    });
    let requested_ids = members(params, "InstanceId.");
    let mut launches: Vec<(&String, &String)> = state
        .launch_times
        .iter()
        .filter(|(id, _)| {
            scoped_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(&id.as_str()))
                && (requested_ids.is_empty() || requested_ids.contains(&id.as_str()))
        })
        .collect();
    launches.sort();
    let total = launches.len();
    let end = state
        .page_size
        .map_or(total, |size| start.saturating_add(size))
        .min(total);
    let page = launches.get(start.min(end)..end).unwrap_or_default();
    let items: String = page
        .iter()
        .map(|(id, launched)| {
            let group = state.groups.iter().find(|group| group.instances.iter().any(|instance| instance.id == **id));
            let tags = group.map_or_else(String::new, |group| format!("<tagSet><item><key>aws:autoscaling:groupName</key><value>{}</value></item></tagSet>", group.name));
            let reason = state.state_reasons.get(id.as_str())
                .map_or_else(String::new, |code| format!("<stateReason><code>{code}</code></stateReason>"));
            format!(
                "<item><instanceId>{id}</instanceId><instanceType>{INSTANCE_TYPE}</instanceType><instanceState><code>16</code><name>running</name></instanceState><launchTime>{launched}</launchTime><placement><availabilityZone>{ZONE}</availabilityZone></placement>{tags}{reason}</item>"
            )
        })
        .collect::<Vec<_>>()
        .concat();
    let reservation = if items.is_empty() {
        String::new()
    } else {
        format!(
            "<item><reservationId>{RESERVATION_ID}</reservationId><ownerId>{ACCOUNT_ID}</ownerId><groupSet/><instancesSet>{items}</instancesSet></item>"
        )
    };
    let next_token = if end < total {
        format!("<nextToken>{end}</nextToken>")
    } else {
        String::new()
    };
    Ok(format!(
        "<reservationSet>{reservation}</reservationSet>{next_token}"
    ))
}

fn web_identity_token(state: &FakeState) -> String {
    format!(
        "<WebIdentityToken>{token}</WebIdentityToken><Expiration>{EXPIRATION}</Expiration>",
        token = state.web_identity_token,
    )
}

/// Wraps a result in the response envelope of an awsQuery service.
fn query_ok(service: QueryService, action: &str, result: Option<&str>) -> String {
    let namespace = service.namespace();
    let body = match (service, result) {
        (QueryService::Ec2, result) => format!(
            "<requestId>{REQUEST_ID}</requestId>{}",
            result.unwrap_or_default()
        ),
        (_, Some(result)) => format!(
            "<{action}Result>{result}</{action}Result><ResponseMetadata><RequestId>{REQUEST_ID}</RequestId></ResponseMetadata>"
        ),
        (_, None) => {
            format!("<ResponseMetadata><RequestId>{REQUEST_ID}</RequestId></ResponseMetadata>")
        }
    };
    format!("<{action}Response xmlns=\"{namespace}\">{body}</{action}Response>")
}

fn query_error(service: QueryService, failure: &FakeFailure) -> String {
    let code = failure.code;
    let message = failure.message;
    if matches!(service, QueryService::Ec2) {
        format!(
            "<Response><Errors><Error><Code>{code}</Code><Message>{message}</Message></Error></Errors><RequestId>{REQUEST_ID}</RequestId></Response>"
        )
    } else {
        let namespace = service.namespace();
        format!(
            "<ErrorResponse xmlns=\"{namespace}\"><Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error><RequestId>{REQUEST_ID}</RequestId></ErrorResponse>"
        )
    }
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "text/xml")], body).into_response()
}

/// Dispatches a KMS (awsJson1.1) call on its `X-Amz-Target` value.
fn kms(state: &Shared, target: &HeaderValue, body: &[u8]) -> Response {
    let operation = target
        .to_str()
        .ok()
        .and_then(|target| target.strip_prefix(KMS_TARGET_PREFIX))
        .unwrap_or_default();
    let outcome = dispatch(
        state,
        operation,
        &json_parameters(body),
        Some(access_denied(KMS_ACCESS_DENIED)),
        |fake, params| kms_operation(fake, operation, params),
    );
    match outcome {
        Ok(json) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, KMS_CONTENT_TYPE)],
            json,
        )
            .into_response(),
        Err(failure) => (
            status_of(failure.status),
            [
                (header::CONTENT_TYPE, KMS_CONTENT_TYPE),
                (
                    header::HeaderName::from_static(ERROR_TYPE_HEADER),
                    failure.code,
                ),
            ],
            json!({"__type": failure.code, "message": failure.message}).to_string(),
        )
            .into_response(),
    }
}

fn kms_operation(
    state: &mut FakeState,
    operation: &str,
    params: &Parameters,
) -> Result<String, FakeFailure> {
    match operation {
        "GetPublicKey" => {
            let key = kms_key(state, params)?;
            let public_key = key
                .signing_key
                .verifying_key()
                .to_public_key_der()
                .map_err(|_| invalid("cannot encode public key"))?;
            Ok(json!({
                "KeyId": key.arn,
                "PublicKey": STANDARD.encode(public_key.as_ref()),
                "KeySpec": key.key_spec,
                "KeyUsage": key.key_usage,
                "SigningAlgorithms": key.signing_algorithms,
            })
            .to_string())
        }
        "Sign" => {
            let key = kms_key(state, params)?;
            let algorithm = required(params, "SigningAlgorithm")?;
            let message = STANDARD
                .decode(required(params, "Message")?)
                .map_err(|_| invalid("message is not valid base64"))?;
            let signature: Signature = match required(params, "MessageType")? {
                "RAW" => key.signing_key.sign(&message),
                "DIGEST" => key
                    .signing_key
                    .sign_prehash(&message)
                    .map_err(|_| invalid("invalid message digest"))?,
                _ => return Err(invalid("unsupported message type")),
            };
            Ok(json!({
                "KeyId": key.arn,
                "Signature": STANDARD.encode(signature.to_der().as_bytes()),
                "SigningAlgorithm": algorithm,
            })
            .to_string())
        }
        _ => Err(FakeFailure {
            status: 400,
            code: INVALID_ACTION,
            message: UNKNOWN_OPERATION,
        }),
    }
}

/// Finds the key named by `KeyId`, which must equal its ARN or its alias.
fn kms_key<'a>(state: &'a FakeState, params: &Parameters) -> Result<&'a FakeKey, FakeFailure> {
    let key_id = required(params, "KeyId")?;
    state
        .kms
        .as_ref()
        .filter(|key| key.arn == key_id || key.alias == key_id)
        .ok_or(FakeFailure {
            status: 400,
            code: NOT_FOUND_EXCEPTION,
            message: UNKNOWN_KEY,
        })
}

fn json_parameters(body: &[u8]) -> Parameters {
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(fields)) => fields
            .into_iter()
            .filter_map(|(key, value)| match value {
                Value::String(text) => Some((key, text)),
                _ => None,
            })
            .collect(),
        _ => Parameters::new(),
    }
}

/// `PUT /latest/api/token`: issues the session token with the TTL header `IMDSv2` requires.
async fn imds_token(State(state): State<Shared>) -> Response {
    let failure = {
        let guard = state.lock();
        guard.token_started.notify_one();
        guard.token_failure
    };
    match failure {
        Some(TokenFailure::Timeout) => return std::future::pending().await,
        None => {}
    }
    match dispatch(
        &state,
        IMDS_TOKEN_OPERATION,
        &Parameters::new(),
        None,
        |_, _| Ok(IMDS_TOKEN.to_owned()),
    ) {
        Ok(token) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/plain"),
                (
                    header::HeaderName::from_static(IMDS_TOKEN_TTL_HEADER),
                    IMDS_TOKEN_TTL_SECONDS,
                ),
            ],
            token,
        )
            .into_response(),
        Err(failure) => (status_of(failure.status), failure.message).into_response(),
    }
}

async fn identity_document(State(state): State<Shared>, headers: HeaderMap) -> Response {
    imds_get(&state, &headers, IDENTITY_PATH, |fake| {
        fake.identity_document.clone()
    })
}

async fn spot_instance_action(State(state): State<Shared>, headers: HeaderMap) -> Response {
    imds_get(&state, &headers, SPOT_ACTION_PATH, |fake| {
        fake.spot_instance_action.clone()
    })
}

async fn fleet_tag(
    State(state): State<Shared>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = format!("{FLEET_TAG_PATH_PREFIX}{key}");
    imds_get(&state, &headers, &path, |fake| {
        if key == "aws:autoscaling:groupName" {
            fake.group_tag.clone()
        } else {
            fake.fleet_tag.clone()
        }
    })
}

/// Serves the target lifecycle state through the metadata service.
async fn target_lifecycle_state(State(state): State<Shared>, headers: HeaderMap) -> Response {
    imds_get(&state, &headers, TARGET_LIFECYCLE_PATH, |fake| {
        fake.target_lifecycle_state.clone()
    })
}

/// Answers a metadata read: needs the session token, then serves the configured body.
fn imds_get(
    state: &Shared,
    headers: &HeaderMap,
    path: &str,
    body: impl FnOnce(&FakeState) -> Option<String>,
) -> Response {
    let parameters = Parameters::from([(PATH_PARAMETER.to_owned(), path.to_owned())]);
    let outcome = dispatch(state, IMDS_GET_OPERATION, &parameters, None, |fake, _| {
        let token = headers
            .get(SESSION_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok());
        if token != Some(IMDS_TOKEN) {
            return Err(FakeFailure {
                status: 401,
                code: UNAUTHORIZED,
                message: UNAUTHORIZED,
            });
        }
        body(fake).ok_or(FakeFailure {
            status: 404,
            code: NO_METADATA,
            message: NO_METADATA,
        })
    });
    text_outcome(outcome)
}

fn text_outcome(outcome: Result<String, FakeFailure>) -> Response {
    match outcome {
        Ok(body) => (StatusCode::OK, [(header::CONTENT_TYPE, "text/plain")], body).into_response(),
        Err(failure) => (status_of(failure.status), failure.message).into_response(),
    }
}

/// Parses an `application/x-www-form-urlencoded` body into its decoded fields.
fn parse_form(body: &[u8]) -> Parameters {
    url::form_urlencoded::parse(body).into_owned().collect()
}

#[test]
fn lifecycle_completion_consumes_only_the_matching_active_action() {
    for (transition, waiting, result, proceeding) in [
        (
            "autoscaling:EC2_INSTANCE_LAUNCHING",
            "Pending:Wait",
            "CONTINUE",
            "Pending:Proceed",
        ),
        (
            "autoscaling:EC2_INSTANCE_TERMINATING",
            "Terminating:Wait",
            "CONTINUE",
            "Terminating:Proceed",
        ),
        (
            "autoscaling:EC2_INSTANCE_LAUNCHING",
            "Pending:Wait",
            "ABANDON",
            "Terminating:Proceed",
        ),
        (
            "autoscaling:EC2_INSTANCE_TERMINATING",
            "Terminating:Wait",
            "ABANDON",
            "Terminating:Proceed",
        ),
    ] {
        let mut state = FakeState::default();
        state
            .groups
            .push(FakeGroup::new("group", 1, &[("i-1", waiting)]));
        state
            .lifecycle_hooks
            .insert("group".into(), vec![("hook".into(), transition.into())]);
        let params: Parameters = [
            ("AutoScalingGroupName", "group"),
            ("LifecycleHookName", "hook"),
            ("InstanceId", "i-1"),
            ("LifecycleActionResult", result),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();

        assert!(query_operation(&mut state, "CompleteLifecycleAction", &params).is_ok());
        assert_eq!(state.groups[0].instances[0].lifecycle_state, proceeding);
        assert_eq!(
            state.lifecycle_completions,
            vec![("group".into(), "hook".into(), "i-1".into())]
        );
        let error = query_operation(&mut state, "CompleteLifecycleAction", &params).unwrap_err();
        assert_eq!(
            (error.status, error.code, error.message),
            (400, "ValidationError", "no active lifecycle action")
        );
        assert_eq!(state.lifecycle_completions.len(), 1);
    }
}

#[test]
fn lifecycle_completion_rejects_nonmatching_actions_and_invalid_results() {
    let mut state = FakeState::default();
    state.groups.push(FakeGroup::new(
        "group",
        2,
        &[("i-1", "Pending:Wait"), ("i-2", "InService")],
    ));
    state.lifecycle_hooks.insert(
        "group".into(),
        vec![("hook".into(), "autoscaling:EC2_INSTANCE_LAUNCHING".into())],
    );
    let params: Parameters = [
        ("AutoScalingGroupName", "group"),
        ("LifecycleHookName", "hook"),
        ("InstanceId", "i-1"),
        ("LifecycleActionResult", "CONTINUE"),
    ]
    .into_iter()
    .map(|(key, value)| (key.into(), value.into()))
    .collect();

    for (key, value) in [
        ("AutoScalingGroupName", "other"),
        ("LifecycleHookName", "other"),
        ("InstanceId", "other"),
        ("InstanceId", "i-2"),
    ] {
        let mut bad = params.clone();
        bad.insert(key.into(), value.into());
        let error = query_operation(&mut state, "CompleteLifecycleAction", &bad).unwrap_err();
        assert_eq!(
            (error.status, error.code, error.message),
            (400, "ValidationError", "no active lifecycle action")
        );
        assert!(state.lifecycle_completions.is_empty());
    }
    for key in ["AutoScalingGroupName", "LifecycleHookName", "InstanceId"] {
        let mut bad = params.clone();
        bad.remove(key);
        let error = query_operation(&mut state, "CompleteLifecycleAction", &bad).unwrap_err();
        assert_eq!(
            (error.status, error.code, error.message),
            (400, "ValidationError", "no active lifecycle action")
        );
        assert!(state.lifecycle_completions.is_empty());
    }
    let mut bad_result = params.clone();
    bad_result.insert("LifecycleActionResult".into(), "INVALID".into());
    let error = query_operation(&mut state, "CompleteLifecycleAction", &bad_result).unwrap_err();
    assert_eq!((error.status, error.code), (400, "ValidationError"));
    assert!(state.lifecycle_completions.is_empty());
    assert!(query_operation(&mut state, "CompleteLifecycleAction", &params).is_ok());
    assert_eq!(state.lifecycle_completions.len(), 1);
}

// Real time, not paused: the 1 ms backoffs are real but tiny, and the verdict never depends on elapsed time.
#[tokio::test]
async fn retries_stop_at_max_attempts() -> Result<(), Box<dyn std::error::Error>> {
    use gang_aws_sdk::{CallError, call_error};
    use gang_types::NonZeroDuration;
    use std::num::NonZeroU32;

    let fake = FakeAws::start().await?;
    fake.state().failures.insert(
        "DescribeAutoScalingGroups",
        FakeFailure {
            status: 500,
            code: "InternalFailure",
            message: "injected",
        },
    );
    let settings = ClientSettings {
        request_timeout: NonZeroDuration::from_hours(24),
        connect_timeout: NonZeroDuration::from_hours(24),
        max_attempts: NonZeroU32::new(3).ok_or("zero attempts")?,
        retry_initial_backoff: NonZeroDuration::from_millis(1),
        retry_max_backoff: NonZeroDuration::from_millis(1),
    };
    let sdk = fake.sdk_config_with(&settings).await;
    let error = aws_sdk_autoscaling::Client::new(&sdk)
        .describe_auto_scaling_groups()
        .send()
        .await
        .err()
        .ok_or("describe unexpectedly succeeded")?;
    assert_eq!(fake.state().requests.len(), 3);
    assert!(matches!(
        call_error("DescribeAutoScalingGroups", error),
        CallError::Transient { .. }
    ));
    Ok(())
}
