//! `EventBridge` observations and bounded SQS delivery.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aws_config::SdkConfig;
use gang_aws_sdk::call_error;
use gang_clock::{Clock, Tick, Timestamp};
use gang_spi::{
    Held, InfrastructureEvent, InfrastructureEventKind, InfrastructureInterruptionAction,
    InfrastructureTerminationReason,
};
use gang_types::InstanceId;
use parking_lot::RwLock;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::{AwsConfig, AwsFleetOptions, ON_DEMAND_SUFFIX};
use crate::errors::AwsError;
use gang_types::Fleet;

const REBALANCE: &str = "EC2 instance rebalance recommendation";
const GROUP_TAG: &str = "aws:autoscaling:groupName";

/// Maps managed group names to their configured fleet.
#[derive(Default)]
pub(crate) struct Managed(RwLock<HashMap<String, String>>, AtomicU64);

impl Managed {
    pub(crate) fn clear(&self) {
        let mut names = self.0.write();
        names.clear();
        self.1.fetch_add(1, Ordering::Release);
    }
    pub(crate) fn add(&self, fleet: Fleet<'_, AwsFleetOptions>) {
        let mut names = self.0.write();
        let name = fleet.name.as_str();
        names.insert(name.to_owned(), name.to_owned());
        if fleet.options.fallback.is_some() {
            names.insert(format!("{name}{ON_DEMAND_SUFFIX}"), name.to_owned());
        }
        self.1.fetch_add(1, Ordering::Release);
    }
    pub(crate) fn revision(&self) -> u64 {
        self.1.load(Ordering::Acquire)
    }
    pub(crate) fn group_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.0.read().keys().cloned().collect();
        names.sort_unstable();
        names
    }
    fn fleet(&self, group: &str) -> Option<String> {
        self.0.read().get(group).cloned()
    }
}

struct Pending {
    fleet: String,
    event: InfrastructureEvent,
    receipt: String,
    received_at: Tick,
}

#[derive(Clone)]
enum InstanceGroup {
    Unresolved,
    Present(Option<String>),
}

enum GroupAdmission {
    Malformed,
    Unresolved,
    Group(Option<String>),
}

/// Delivery metadata only: the canonical repository owns the immutable observation payload.
struct ActivityDelivery {
    at: Timestamp,
    acknowledged: bool,
    seen: bool,
}

struct ActivityHistory {
    fleet: String,
    deliveries: HashMap<String, ActivityDelivery>,
}

struct ActivityAttempt {
    at: Tick,
    managed: bool,
}

struct ActivityError {
    fleet: String,
    message: String,
    sequence: u64,
    managed: bool,
}

#[derive(Default)]
struct ActivityPoll {
    attempts: HashMap<String, ActivityAttempt>,
    groups: HashMap<String, ObservedGroup>,
}

struct ObservedGroup {
    group: aws_sdk_autoscaling::types::AutoScalingGroup,
    dirty: bool,
}

impl ObservedGroup {
    fn unsettled(&self) -> bool {
        use aws_sdk_autoscaling::types::LifecycleState;

        let mut in_service = 0_usize;
        for instance in self.group.instances() {
            match instance.lifecycle_state() {
                Some(LifecycleState::InService) => in_service += 1,
                Some(
                    LifecycleState::Pending
                    | LifecycleState::PendingWait
                    | LifecycleState::PendingProceed,
                )
                | None => return true,
                _ => {}
            }
        }
        self.group.desired_capacity().is_none_or(|desired| {
            usize::try_from(desired).map_or(true, |desired| in_service < desired)
        })
    }
}

/// Queue state is shared by all fleets of a provisioner.
pub(crate) struct Events {
    sqs: aws_sdk_sqs::Client,
    ec2: aws_sdk_ec2::Client,
    autoscaling: aws_sdk_autoscaling::Client,
    queue: Option<String>,
    batch: i32,
    visibility: Duration,
    window: Duration,
    activity_poll_interval: Duration,
    limit: u32,
    clock: Arc<dyn Clock>,
    managed: Arc<Managed>,
    pending: Mutex<Vec<Pending>>,
    activity_poll: Mutex<ActivityPoll>,
    history: RwLock<HashMap<String, ActivityHistory>>,
    activity_errors: RwLock<HashMap<String, ActivityError>>,
    error_sequence: AtomicU64,
}

impl Events {
    pub(crate) fn new(
        sdk: &SdkConfig,
        config: &AwsConfig,
        clock: Arc<dyn Clock>,
        managed: Arc<Managed>,
    ) -> Self {
        Self {
            sqs: aws_sdk_sqs::Client::new(sdk),
            ec2: aws_sdk_ec2::Client::new(sdk),
            autoscaling: aws_sdk_autoscaling::Client::new(sdk),
            queue: config.events_queue_url.as_ref().map(ToString::to_string),
            batch: i32::from(config.event_batch_limit),
            visibility: config.event_visibility_timeout.get(),
            window: config.activity_history_window.get(),
            activity_poll_interval: config.activity_poll_interval.get(),
            limit: config.activity_limit,
            clock,
            managed,
            pending: Mutex::new(Vec::new()),
            activity_poll: Mutex::new(ActivityPoll::default()),
            history: RwLock::new(HashMap::new()),
            activity_errors: RwLock::new(HashMap::new()),
            error_sequence: AtomicU64::new(0),
        }
    }

    pub(crate) fn with_autoscaling(mut self, client: aws_sdk_autoscaling::Client) -> Self {
        self.autoscaling = client;
        self
    }

    pub(crate) fn managed(&self) -> &Arc<Managed> {
        &self.managed
    }

    pub(crate) fn observation_error(&self, fleet: &str) -> Option<String> {
        let managed = self.managed.0.read();
        self.activity_errors
            .read()
            .iter()
            .filter(|(group, error)| {
                error.fleet == fleet
                    && match managed.get(*group) {
                        Some(owner) => owner == fleet,
                        None => !error.managed,
                    }
            })
            .max_by_key(|(_, error)| error.sequence)
            .map(|(_, error)| error.message.clone())
    }

    pub(crate) async fn observe_groups(
        &self,
        output: &aws_sdk_autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput,
    ) {
        if self.queue.is_some() {
            return;
        }
        let mut poll = self.activity_poll.lock().await;
        let managed = self.managed.0.read();
        poll.groups.retain(|name, _| {
            managed.contains_key(name)
                && output
                    .auto_scaling_groups()
                    .iter()
                    .any(|group| group.auto_scaling_group_name() == Some(name.as_str()))
        });
        for group in output.auto_scaling_groups() {
            let Some(name) = group.auto_scaling_group_name() else {
                continue;
            };
            if !managed.contains_key(name) {
                continue;
            }
            match poll.groups.get_mut(name) {
                Some(previous) if previous.group != *group => {
                    previous.group = group.clone();
                    previous.dirty = true;
                }
                Some(_) => {}
                None => {
                    poll.groups.insert(
                        name.to_owned(),
                        ObservedGroup {
                            group: group.clone(),
                            dirty: true,
                        },
                    );
                }
            }
        }
    }

    pub(crate) async fn receive(&self, held: &Held) -> Result<(), AwsError> {
        held.check()?;
        let Some(queue) = &self.queue else {
            return Ok(());
        };
        let mut pending = self.pending.lock().await;
        let now = self.clock.monotonic();
        pending.retain(|item| now.saturating_duration_since(item.received_at) < self.visibility);
        let available = self
            .batch
            .saturating_sub(i32::try_from(pending.len()).unwrap_or(self.batch));
        if available == 0 {
            return Ok(());
        }
        held.check()?;
        let response = self
            .sqs
            .receive_message()
            .queue_url(queue)
            .max_number_of_messages(available)
            .wait_time_seconds(0)
            .visibility_timeout(i32::try_from(self.visibility.as_secs()).map_err(|_| {
                AwsError::Config {
                    field: "event_visibility_timeout",
                    reason: "must be whole seconds up to 12 hours".to_owned(),
                }
            })?)
            .send()
            .await
            .map_err(|error| call_error("ReceiveMessage", error))?;
        let mut groups = HashMap::<String, InstanceGroup>::new();
        let mut malformed = 0_u32;
        let mut foreign = 0_u32;
        for message in response.messages() {
            let (Some(body), Some(receipt)) = (message.body(), message.receipt_handle()) else {
                malformed += 1;
                continue;
            };
            let Some(raw) = parse_envelope(body) else {
                malformed += 1;
                continue;
            };
            let group = match self.event_group(&raw, &mut groups).await? {
                GroupAdmission::Malformed => {
                    malformed += 1;
                    continue;
                }
                GroupAdmission::Unresolved => {
                    tracing::warn!(
                        "AWS instance for event not yet visible; leaving message for retry"
                    );
                    continue;
                }
                GroupAdmission::Group(group) => group,
            };
            let Some(fleet) = group.as_deref().and_then(|group| self.managed.fleet(group)) else {
                foreign += 1;
                self.delete(held, queue, receipt).await?;
                continue;
            };
            let event = match self.normalize(&raw, group.as_deref()).await? {
                Parsed::Event(event) => event,
                Parsed::Ignore => {
                    self.delete(held, queue, receipt).await?;
                    continue;
                }
                Parsed::Malformed => {
                    malformed += 1;
                    continue;
                }
            };
            if let Some(existing) = pending.iter_mut().find(|item| item.event.id == event.id) {
                receipt.clone_into(&mut existing.receipt);
                existing.received_at = now;
            } else {
                pending.push(Pending {
                    fleet,
                    event,
                    receipt: receipt.to_owned(),
                    received_at: now,
                });
            }
        }
        if malformed != 0 {
            tracing::warn!(
                count = malformed,
                "malformed AWS event messages left for dead-letter queue"
            );
        }
        if foreign != 0 {
            tracing::warn!(count = foreign, "foreign AWS event messages dropped");
        }
        Ok(())
    }

    async fn event_group(
        &self,
        raw: &Envelope,
        groups: &mut HashMap<String, InstanceGroup>,
    ) -> Result<GroupAdmission, AwsError> {
        if raw.source == "aws.autoscaling" {
            return Ok(
                match raw
                    .detail
                    .get("AutoScalingGroupName")
                    .and_then(Value::as_str)
                {
                    Some(group) => GroupAdmission::Group(Some(group.to_owned())),
                    None => GroupAdmission::Malformed,
                },
            );
        }
        if raw.source != "aws.ec2" {
            return Ok(GroupAdmission::Group(None));
        }
        let Some(instance) = raw.detail.get("instance-id").and_then(Value::as_str) else {
            return Ok(GroupAdmission::Malformed);
        };
        if InstanceId::try_new(instance).is_err()
            || (raw.kind == "EC2 Spot Instance Interruption Warning"
                && !matches!(
                    raw.detail.get("instance-action").and_then(Value::as_str),
                    Some("terminate" | "stop" | "hibernate")
                ))
        {
            return Ok(GroupAdmission::Malformed);
        }
        let lookup = if let Some(lookup) = groups.get(instance) {
            lookup.clone()
        } else {
            let lookup = self.instance_group(instance).await?;
            groups.insert(instance.to_owned(), lookup.clone());
            lookup
        };
        Ok(match lookup {
            InstanceGroup::Unresolved => GroupAdmission::Unresolved,
            InstanceGroup::Present(group) => GroupAdmission::Group(group),
        })
    }

    async fn instance_group(&self, instance: &str) -> Result<InstanceGroup, AwsError> {
        let response = match self
            .ec2
            .describe_instances()
            .instance_ids(instance)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error)
                if error.as_service_error().is_some_and(|error| {
                    error.meta().code() == Some("InvalidInstanceID.NotFound")
                }) =>
            {
                return Ok(InstanceGroup::Unresolved);
            }
            Err(error) => return Err(call_error("DescribeInstances", error).into()),
        };
        let Some(instance) = response
            .reservations()
            .iter()
            .flat_map(aws_sdk_ec2::types::Reservation::instances)
            .find(|candidate| candidate.instance_id() == Some(instance))
        else {
            return Ok(InstanceGroup::Unresolved);
        };
        Ok(InstanceGroup::Present(
            instance
                .tags()
                .iter()
                .find(|tag| tag.key() == Some(GROUP_TAG))
                .and_then(|tag| tag.value())
                .map(str::to_owned),
        ))
    }

    async fn delete(&self, held: &Held, queue: &str, receipt: &str) -> Result<(), AwsError> {
        held.check()?;
        match self
            .sqs
            .delete_message()
            .queue_url(queue)
            .receipt_handle(receipt)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(aws_sdk_sqs::operation::delete_message::DeleteMessageError::is_receipt_handle_is_invalid) =>
            {
                Ok(())
            }
            Err(error) => Err(call_error("DeleteMessage", error).into()),
        }
    }

    pub(crate) async fn acknowledge(
        &self,
        held: &Held,
        fleet: &str,
        id: &str,
    ) -> Result<(), AwsError> {
        held.check()?;
        let Some(queue) = &self.queue else {
            let managed = self.managed.0.read();
            let since = self.clock.wall() - self.window;
            for (group, history) in self.history.write().iter_mut() {
                if history.fleet != fleet || managed.get(group) != Some(&history.fleet) {
                    continue;
                }
                if let Some(delivery) = history.deliveries.get_mut(id)
                    && delivery.at > since
                {
                    delivery.acknowledged = true;
                }
            }
            return Ok(());
        };
        let mut pending = self.pending.lock().await;
        if let Some(index) = pending
            .iter()
            .position(|item| item.fleet == fleet && item.event.id == id)
        {
            self.delete(held, queue, &pending[index].receipt).await?;
            pending.remove(index);
        }
        Ok(())
    }

    pub(crate) async fn supply<'a>(
        &self,
        fleet: &str,
        groups: impl Iterator<Item = &'a str> + Send,
        out: &mut Vec<InfrastructureEvent>,
    ) -> Result<(), AwsError> {
        out.clear();
        if self.queue.is_some() {
            let pending = self.pending.lock().await;
            out.extend(
                pending
                    .iter()
                    .filter(|item| item.fleet == fleet)
                    .map(|item| item.event.clone()),
            );
        } else {
            // Polls share seen markers, so reset, pagination, and sweep must not interleave.
            let mut poll = self.activity_poll.lock().await;
            self.prune_activity_transport(&mut poll);
            for group in groups {
                let now = self.clock.monotonic();
                if poll.attempts.get(group).is_some_and(|last| {
                    now.saturating_duration_since(last.at) < self.activity_poll_interval
                }) {
                    continue;
                }
                let pending_delivery = self.history.read().get(group).is_some_and(|history| {
                    history
                        .deliveries
                        .values()
                        .any(|delivery| !delivery.acknowledged)
                });
                if poll
                    .groups
                    .get(group)
                    .is_some_and(|observed| !observed.dirty && !observed.unsettled())
                    && !pending_delivery
                    && !self.activity_errors.read().contains_key(group)
                {
                    continue;
                }
                poll.attempts.insert(
                    group.to_owned(),
                    ActivityAttempt {
                        at: now,
                        managed: self.managed.0.read().contains_key(group),
                    },
                );
                let start = out.len();
                let result = self.activities(fleet, group, out).await;
                self.finish_activity_history(group, &out[start..], result.is_ok());
                {
                    let mut errors = self.activity_errors.write();
                    match &result {
                        Ok(()) => {
                            if let Some(observed) = poll.groups.get_mut(group) {
                                observed.dirty = false;
                            }
                            errors.remove(group);
                        }
                        Err(error) => {
                            errors.insert(
                                group.to_owned(),
                                ActivityError {
                                    fleet: fleet.to_owned(),
                                    message: error.to_string(),
                                    sequence: self.error_sequence.fetch_add(1, Ordering::Relaxed),
                                    managed: poll
                                        .attempts
                                        .get(group)
                                        .is_some_and(|attempt| attempt.managed),
                                },
                            );
                        }
                    }
                }
                result?;
            }
        }
        Ok(())
    }

    fn prune_activity_transport(&self, poll: &mut ActivityPoll) {
        let managed = self.managed.0.read();
        let since = self.clock.wall() - self.window;
        let now = self.clock.monotonic();
        poll.attempts.retain(|group, attempt| {
            let is_managed = managed.contains_key(group);
            let keep = (!attempt.managed || is_managed)
                && now.saturating_duration_since(attempt.at) < self.activity_poll_interval;
            attempt.managed |= is_managed;
            keep
        });
        poll.groups.retain(|group, _| managed.contains_key(group));
        self.history.write().retain(|group, history| {
            history.deliveries.retain(|_, delivery| delivery.at > since);
            managed.get(group) == Some(&history.fleet)
        });
        self.activity_errors
            .write()
            .retain(|group, error| match managed.get(group) {
                Some(fleet) if fleet == &error.fleet => {
                    error.managed = true;
                    true
                }
                Some(_) => false,
                None => !error.managed,
            });
    }

    async fn activities(
        &self,
        fleet: &str,
        group: &str,
        out: &mut Vec<InfrastructureEvent>,
    ) -> Result<(), AwsError> {
        let since = (self.clock.wall() - self.window).to_rfc3339();
        {
            let managed = self.managed.0.read();
            if managed.get(group).is_some_and(|name| name == fleet) {
                let mut histories = self.history.write();
                if let Some(history) = histories.get_mut(group) {
                    for delivery in history.deliveries.values_mut() {
                        delivery.seen = false;
                    }
                } else {
                    histories.insert(
                        group.to_owned(),
                        ActivityHistory {
                            fleet: fleet.to_owned(),
                            deliveries: HashMap::new(),
                        },
                    );
                }
            }
        }
        let mut token = None;
        let mut remaining = self.limit;
        while remaining > 0 {
            let count = remaining.min(100);
            let response = self
                .autoscaling
                .describe_scaling_activities()
                .auto_scaling_group_name(group)
                .filters(
                    aws_sdk_autoscaling::types::Filter::builder()
                        .name("StartTimeLowerBound")
                        .values(&since)
                        .build(),
                )
                .max_records(i32::try_from(count).map_err(|_| {
                    AwsError::malformed("DescribeScalingActivities", "activity limit")
                })?)
                .set_next_token(token)
                .send()
                .await
                .map_err(|error| call_error("DescribeScalingActivities", error))?;
            remaining = remaining
                .saturating_sub(u32::try_from(response.activities().len()).unwrap_or(remaining));
            for activity in response.activities() {
                if activity
                    .activity_id()
                    .is_some_and(|id| self.activity_acknowledged(group, id))
                {
                    continue;
                }
                if let Some(mut event) = activity_event(activity, false) {
                    if event.at + self.window <= self.clock.wall() {
                        continue;
                    }
                    self.enrich_termination(&mut event.kind).await;
                    out.push(event);
                }
            }
            token = response.next_token().map(str::to_owned);
            if token.is_none() || response.activities().is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn activity_acknowledged(&self, group: &str, id: &str) -> bool {
        let mut histories = self.history.write();
        let Some(delivery) = histories
            .get_mut(group)
            .and_then(|history| history.deliveries.get_mut(id))
        else {
            return false;
        };
        delivery.seen = true;
        delivery.acknowledged
    }

    /// Keep only the current bounded page-set after a complete poll. A failed poll must not
    /// discard acknowledgements for activities on pages it could not read.
    fn finish_activity_history(&self, group: &str, events: &[InfrastructureEvent], complete: bool) {
        let mut histories = self.history.write();
        let Some(history) = histories.get_mut(group) else {
            return;
        };
        if complete {
            history.deliveries.retain(|_, delivery| delivery.seen);
        }
        for event in events {
            if history.deliveries.contains_key(&event.id) {
                continue;
            }
            if u32::try_from(history.deliveries.len()).unwrap_or(self.limit) >= self.limit {
                break;
            }
            history.deliveries.insert(
                event.id.clone(),
                ActivityDelivery {
                    at: event.at,
                    acknowledged: false,
                    seen: true,
                },
            );
        }
    }

    async fn enrich_termination(&self, kind: &mut InfrastructureEventKind) {
        let InfrastructureEventKind::InstanceTerminated { instance, reason } = kind else {
            return;
        };
        if !matches!(reason, InfrastructureTerminationReason::Other(_)) {
            return;
        }
        let response = match self
            .ec2
            .describe_instances()
            .instance_ids(instance.as_str())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error)
                if error.as_service_error().is_some_and(|error| {
                    error.meta().code() == Some("InvalidInstanceID.NotFound")
                }) =>
            {
                return;
            }
            Err(error) => {
                tracing::warn!(
                    instance = instance.as_str(),
                    error = %call_error("DescribeInstances", error),
                    "AWS termination enrichment failed; retaining ASG cause"
                );
                return;
            }
        };
        let code = response
            .reservations()
            .iter()
            .flat_map(aws_sdk_ec2::types::Reservation::instances)
            .find(|candidate| candidate.instance_id() == Some(instance.as_str()))
            .and_then(aws_sdk_ec2::types::Instance::state_reason)
            .and_then(aws_sdk_ec2::types::StateReason::code);
        if matches!(
            code,
            Some("Server.SpotInstanceTermination" | "Server.SpotInstanceShutdown")
        ) {
            *reason = InfrastructureTerminationReason::SpotReclaimed;
        }
    }

    async fn normalize(&self, raw: &Envelope, group: Option<&str>) -> Result<Parsed, AwsError> {
        let instance = || {
            raw.detail
                .get("instance-id")
                .and_then(Value::as_str)
                .and_then(|id| InstanceId::try_new(id).ok())
        };
        let kind = match (raw.source.as_str(), raw.kind.as_str()) {
            ("aws.ec2", "EC2 Instance Rebalance Recommendation") => {
                let Some(instance) = instance() else {
                    return Ok(Parsed::Malformed);
                };
                InfrastructureEventKind::RebalanceRecommendation { instance }
            }
            ("aws.ec2", "EC2 Spot Instance Interruption Warning") => {
                let Some(instance) = instance() else {
                    return Ok(Parsed::Malformed);
                };
                let action = match raw.detail.get("instance-action").and_then(Value::as_str) {
                    Some("terminate") => InfrastructureInterruptionAction::Terminate,
                    Some("stop") => InfrastructureInterruptionAction::Stop,
                    Some("hibernate") => InfrastructureInterruptionAction::Hibernate,
                    _ => return Ok(Parsed::Malformed),
                };
                InfrastructureEventKind::SpotInterruption { instance, action }
            }
            ("aws.autoscaling", "EC2 Instance Launch Unsuccessful") => {
                let Some(reason) = raw
                    .detail
                    .get("StatusMessage")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                else {
                    return Ok(Parsed::Malformed);
                };
                InfrastructureEventKind::CapacityUnavailable {
                    reason: reason.to_owned(),
                }
            }
            (
                "aws.autoscaling",
                "EC2 Instance Launch Successful" | "EC2 Instance Terminate Successful",
            ) => {
                let Some(activity_id) = raw.detail.get("ActivityId").and_then(Value::as_str) else {
                    return Ok(Parsed::Malformed);
                };
                let Some(group) = group else {
                    return Ok(Parsed::Malformed);
                };
                let response = self
                    .autoscaling
                    .describe_scaling_activities()
                    .auto_scaling_group_name(group)
                    .activity_ids(activity_id)
                    .send()
                    .await
                    .map_err(|error| call_error("DescribeScalingActivities", error))?;
                let Some(activity) = response
                    .activities()
                    .iter()
                    .find(|activity| activity.activity_id() == Some(activity_id))
                else {
                    return Ok(Parsed::Malformed);
                };
                let Some(event) = activity_event(activity, true) else {
                    return Ok(Parsed::Malformed);
                };
                let Some(instance_id) = raw.detail.get("EC2InstanceId").and_then(Value::as_str)
                else {
                    return Ok(Parsed::Malformed);
                };
                let matching = match &event.kind {
                    InfrastructureEventKind::InstanceLaunched { instance }
                    | InfrastructureEventKind::ReplacementLaunched { instance, .. } => {
                        raw.kind == "EC2 Instance Launch Successful"
                            && instance.as_str() == instance_id
                    }
                    InfrastructureEventKind::InstanceTerminated { instance, .. } => {
                        raw.kind == "EC2 Instance Terminate Successful"
                            && instance.as_str() == instance_id
                    }
                    _ => false,
                };
                if !matching {
                    return Ok(Parsed::Malformed);
                }
                let mut kind = event.kind;
                self.enrich_termination(&mut kind).await;
                kind
            }
            _ => return Ok(Parsed::Ignore),
        };
        Ok(Parsed::Event(InfrastructureEvent {
            id: raw.id.clone(),
            at: raw.at,
            kind,
        }))
    }
}

struct Envelope {
    id: String,
    at: Timestamp,
    source: String,
    kind: String,
    detail: Value,
}
enum Parsed {
    Event(InfrastructureEvent),
    Ignore,
    Malformed,
}

fn parse_envelope(body: &str) -> Option<Envelope> {
    let Value::Object(mut value) = serde_json::from_str(body).ok()? else {
        return None;
    };
    let Value::String(id) = value.remove("id")? else {
        return None;
    };
    if id.is_empty() {
        return None;
    }
    let Value::String(time) = value.remove("time")? else {
        return None;
    };
    let at = Timestamp::from_rfc3339(&time).ok()?;
    let Value::String(source) = value.remove("source")? else {
        return None;
    };
    let Value::String(kind) = value.remove("detail-type")? else {
        return None;
    };
    let detail = value.remove("detail")?;
    if !detail.is_object() {
        return None;
    }
    Some(Envelope {
        id,
        at,
        source,
        kind,
        detail,
    })
}

fn activity_event(
    activity: &aws_sdk_autoscaling::types::Activity,
    successful_event: bool,
) -> Option<InfrastructureEvent> {
    let id = activity.activity_id()?.to_owned();
    let millis = activity.start_time()?.to_millis().ok()?;
    let at = Timestamp::from_unix_millis(u64::try_from(millis).ok()?);
    let status = activity.status_code()?.as_str();
    if status == "Failed" && !successful_event {
        if !activity
            .description()?
            .starts_with("Launching a new EC2 instance")
        {
            return None;
        }
        return Some(InfrastructureEvent {
            id,
            at,
            kind: InfrastructureEventKind::CapacityUnavailable {
                reason: activity.status_message()?.to_owned(),
            },
        });
    }
    if status != "Successful" && !successful_event {
        return None;
    }
    let cause = activity.cause()?;
    let description = activity.description()?;
    let (launch, raw) =
        if let Some(raw) = description.strip_prefix("Launching a new EC2 instance: ") {
            (true, raw)
        } else {
            (
                false,
                description.strip_prefix("Terminating EC2 instance: ")?,
            )
        };
    let instance = InstanceId::try_new(raw.trim()).ok()?;
    let kind = if launch {
        if cause.contains(REBALANCE) {
            InfrastructureEventKind::ReplacementLaunched {
                instance,
                replacing: None,
            }
        } else {
            InfrastructureEventKind::InstanceLaunched { instance }
        }
    } else {
        let reason = if cause.contains(REBALANCE) {
            InfrastructureTerminationReason::Rebalance
        } else {
            InfrastructureTerminationReason::Other(cause.to_owned())
        };
        InfrastructureEventKind::InstanceTerminated { instance, reason }
    };
    Some(InfrastructureEvent { id, at, kind })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_aws::{FakeActivity, FakeAws, FakeFailure, FakeGroup, FakeMessage};
    use crate::fleet::Fleets;
    use crate::fleet_harness::{FLEET, fleet};
    use gang_clock::TestClock;
    use gang_spi::FleetSupply;
    use serde_json::json;
    use std::error::Error;

    const OLD: &str = "i-00b2b4f30d74f12c9";
    const NEW: &str = "i-00517763c3f36b7d7";
    const LAUNCH_ID: &str = "890687cc-df17-5354-b28b-4ae0b862f36a";
    const TERMINATION_ID: &str = "628687cc-e39a-d42a-15be-01d0f565c81a";

    fn config(queue: Option<&str>) -> Result<AwsConfig, serde_json::Error> {
        serde_json::from_value(json!({
            "region": "us-east-1", "scratch_mount_point": "/mnt/scratch",
            "events_queue_url": queue
        }))
    }

    fn seed_activity(fake: &FakeAws, id: &str, time: &str, description: &str, cause: &str) {
        let mut state = fake.state();
        let index = state.activities.len();
        state.activities.push(FakeActivity {
            group: FLEET.to_owned(),
            status_code: "Successful".to_owned(),
            start_time: time.to_owned(),
            message: None,
        });
        state.activity_details.insert(
            index,
            (id.to_owned(), cause.to_owned(), description.to_owned()),
        );
    }

    async fn replay_setup() -> Result<(FakeAws, Arc<TestClock>, Fleets), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        fake.state()
            .groups
            .push(FakeGroup::new(FLEET, 1, &[(OLD, "InService")]));
        seed_activity(
            &fake,
            "1af687cc-db1b-a9c8-77cd-29ab9168eb4b",
            "2026-11-19T09:50:15.786Z",
            &format!("Launching a new EC2 instance: {OLD}"),
            "an instance was started in response to a difference between desired and actual capacity",
        );
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:50:15.786Z",
        )?));
        let config = config(None)?;
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let events = Events::new(&sdk, &config, clock.clone(), managed);
        let fleets = Fleets::new(&sdk, clock.clone(), config.set_capacity_cooldown, events);
        Ok((fake, clock, fleets))
    }

    #[tokio::test]
    async fn historical_activities_keep_provider_identity_and_requested_capacity()
    -> Result<(), Box<dyn Error>> {
        let (fake, clock, fleets) = replay_setup().await?;
        let mut supply = FleetSupply::default();
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert_eq!(
            (supply.requested, supply.desired, supply.instances.len()),
            (Some(1), 1, 1)
        );
        assert_eq!(supply.events.len(), 1);
        assert_eq!(
            supply.events[0].kind,
            InfrastructureEventKind::InstanceLaunched {
                instance: InstanceId::try_new(OLD)?
            }
        );

        clock.advance(Duration::from_millis(65_258));
        fake.state().groups[0] =
            FakeGroup::new(FLEET, 1, &[(OLD, "InService"), (NEW, "InService")]);
        seed_activity(
            &fake,
            LAUNCH_ID,
            "2026-11-19T09:51:21.044Z",
            &format!("Launching a new EC2 instance: {NEW}"),
            "At 2026-11-19T09:51:18Z an instance was launched in response to an EC2 instance rebalance recommendation.",
        );
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert_eq!(
            (supply.requested, supply.desired, supply.instances.len()),
            (Some(1), 2, 2)
        );
        let launch = supply
            .events
            .iter()
            .find(|event| event.id == LAUNCH_ID)
            .ok_or("missing launch")?;
        assert_eq!(
            launch.at,
            Timestamp::from_rfc3339("2026-11-19T09:51:21.044Z")?
        );
        assert_eq!(
            launch.kind,
            InfrastructureEventKind::ReplacementLaunched {
                instance: InstanceId::try_new(NEW)?,
                replacing: None
            }
        );

        clock.advance(Duration::from_millis(73_953));
        fake.state().groups[0] =
            FakeGroup::new(FLEET, 1, &[(OLD, "Terminating"), (NEW, "InService")]);
        seed_activity(
            &fake,
            TERMINATION_ID,
            "2026-11-19T09:52:34.997Z",
            &format!("Terminating EC2 instance: {OLD}"),
            "At 2026-11-19T09:52:34Z an instance was taken out of service in response to an EC2 instance rebalance recommendation.",
        );
        if let Some(activity) = fake.state().activities.last_mut() {
            activity.status_code = "InProgress".to_owned();
        }
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert!(
            supply
                .instances
                .iter()
                .any(|member| member.id.as_str() == OLD
                    && member.lifecycle == gang_types::InstanceLifecycle::Terminating)
        );
        assert!(!supply.events.iter().any(|event| event.id == TERMINATION_ID));

        clock.advance(Duration::from_mins(1));
        fake.state().groups[0] = FakeGroup::new(FLEET, 1, &[(NEW, "InService")]);
        if let Some(activity) = fake.state().activities.last_mut() {
            activity.status_code = "Successful".to_owned();
        }
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert_eq!(
            (supply.requested, supply.desired, supply.instances.len()),
            (Some(1), 1, 1)
        );
        let termination = supply
            .events
            .iter()
            .find(|event| event.id == TERMINATION_ID)
            .ok_or("missing termination")?;
        assert_eq!(
            termination.at,
            Timestamp::from_rfc3339("2026-11-19T09:52:34.997Z")?
        );
        assert_eq!(
            termination.kind,
            InfrastructureEventKind::InstanceTerminated {
                instance: InstanceId::try_new(OLD)?,
                reason: InfrastructureTerminationReason::Rebalance
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn acknowledged_activity_history_skips_enrichment_but_unacknowledged_replays()
    -> Result<(), Box<dyn Error>> {
        let (fake, clock, fleets) = replay_setup().await?;
        fake.state()
            .launch_times
            .insert(OLD.to_owned(), "2026-11-19T09:50:00Z".to_owned());
        fake.state()
            .state_reasons
            .insert(OLD.to_owned(), "Server.SpotInstanceTermination".to_owned());
        seed_activity(
            &fake,
            TERMINATION_ID,
            "2026-11-19T09:50:15.786Z",
            &format!("Terminating EC2 instance: {OLD}"),
            "an instance was taken out of service",
        );
        let enrichments = || {
            fake.state()
                .requests
                .iter()
                .filter(|request| {
                    request.operation == "DescribeInstances"
                        && request.parameters.get("InstanceId.1").map(String::as_str) == Some(OLD)
                })
                .count()
        };
        let mut supply = FleetSupply::default();
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        let first = supply.events.clone();
        assert_eq!(first.len(), 2);
        assert_eq!(enrichments(), 1);
        assert!(first.iter().any(|event| event.id == TERMINATION_ID
            && matches!(
                &event.kind,
                InfrastructureEventKind::InstanceTerminated {
                    reason: InfrastructureTerminationReason::SpotReclaimed,
                    ..
                }
            )));
        let expired = gang_testkit::fence::with_expired_held(async |held| {
            fleets
                .events()
                .acknowledge(held, FLEET, TERMINATION_ID)
                .await
        })
        .await;
        assert!(expired.is_err());
        gang_testkit::fence::with_held(async |held| {
            fleets
                .events()
                .acknowledge(held, "foreign", TERMINATION_ID)
                .await
        })
        .await?;
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert!(supply.events.is_empty());
        assert_eq!(enrichments(), 1);
        clock.advance(Duration::from_mins(1));
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert_eq!(supply.events, first);
        assert_eq!(enrichments(), 2);
        gang_testkit::fence::with_held(async |held| {
            for event in &supply.events {
                fleets.events().acknowledge(held, FLEET, &event.id).await?;
            }
            Ok::<(), AwsError>(())
        })
        .await?;
        clock.advance(Duration::from_mins(1));
        for _ in 0..3 {
            fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
            assert!(supply.events.is_empty());
            assert_eq!((supply.requested, supply.desired), (Some(1), 1));
        }
        assert_eq!(enrichments(), 2);

        // Only transport acknowledgements are cached; a fresh provider replays source facts.
        let sdk = fake.sdk_config().await;
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let restarted = Events::new(&sdk, &config(None)?, clock, managed);
        let mut replay = Vec::new();
        restarted
            .supply(FLEET, std::iter::once(FLEET), &mut replay)
            .await?;
        assert_eq!(replay, first);
        assert_eq!(enrichments(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn activity_history_throttling_preserves_healthy_supply_and_retries()
    -> Result<(), Box<dyn Error>> {
        let (fake, clock, fleets) = replay_setup().await?;
        fake.state().failures.insert(
            "DescribeScalingActivities",
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "activity history throttled",
            },
        );
        let mut supply = FleetSupply::default();
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert_eq!((supply.requested, supply.desired), (Some(1), 1));
        assert_eq!(supply.instances.len(), 1);
        assert!(supply.events.is_empty());
        let error = fleets
            .events()
            .observation_error(FLEET)
            .ok_or("missing activity error")?;
        assert!(error.contains("Throttling"));
        assert!(error.contains("activity history throttled"));
        assert!(fleets.events().observation_error("foreign").is_none());
        let attempts = || {
            fake.state()
                .requests
                .iter()
                .filter(|request| request.operation == "DescribeScalingActivities")
                .count()
        };
        let failed_attempts = attempts();
        assert!(failed_attempts > 0);
        fake.state().failures.remove("DescribeScalingActivities");
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert!(supply.events.is_empty());
        assert_eq!(attempts(), failed_attempts);
        assert_eq!(
            fleets.events().observation_error(FLEET).as_deref(),
            Some(error.as_str())
        );
        clock.advance(Duration::from_mins(1));
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert_eq!(supply.events.len(), 1);
        assert!(attempts() > failed_attempts);
        assert!(fleets.events().observation_error(FLEET).is_none());
        fake.state().failures.insert(
            "DescribeScalingActivities",
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "activity history throttled",
            },
        );
        clock.advance(Duration::from_mins(1));
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert!(fleets.events().observation_error(FLEET).is_some());
        fleets.events().managed().clear();
        assert!(fleets.events().observation_error(FLEET).is_none());
        let mut out = Vec::new();
        fleets
            .events()
            .supply(FLEET, std::iter::empty(), &mut out)
            .await?;
        assert!(fleets.events().activity_errors.read().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn settled_acked_groups_poll_only_after_a_change() -> Result<(), Box<dyn Error>> {
        use aws_sdk_autoscaling::operation::describe_auto_scaling_groups::DescribeAutoScalingGroupsOutput;
        use aws_sdk_autoscaling::types::AutoScalingGroup;

        let (fake, clock, fleets) = replay_setup().await?;
        let events = fleets.events();
        let snapshot = |min_size| {
            DescribeAutoScalingGroupsOutput::builder()
                .auto_scaling_groups(
                    AutoScalingGroup::builder()
                        .auto_scaling_group_name(FLEET)
                        .desired_capacity(0)
                        .min_size(min_size)
                        .build(),
                )
                .build()
        };
        let calls = || {
            fake.state()
                .requests
                .iter()
                .filter(|request| request.operation == "DescribeScalingActivities")
                .count()
        };
        let mut out = Vec::new();
        events.observe_groups(&snapshot(0)).await;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 1);
        assert_eq!(out.len(), 1);
        clock.advance(Duration::from_mins(1));
        events.observe_groups(&snapshot(0)).await;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 2, "unacknowledged deliveries must replay");
        gang_testkit::fence::with_held(async |held| {
            events.acknowledge(held, FLEET, &out[0].id).await
        })
        .await?;
        clock.advance(Duration::from_secs(1));
        events.observe_groups(&snapshot(1)).await;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 2, "changes still respect the interval");
        clock.advance(Duration::from_secs(59));
        events.observe_groups(&snapshot(1)).await;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 3);
        assert!(out.is_empty());
        clock.advance(Duration::from_mins(1));
        events.observe_groups(&snapshot(1)).await;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 3, "settled acknowledged groups stay idle");
        let unmet = DescribeAutoScalingGroupsOutput::builder()
            .auto_scaling_groups(
                AutoScalingGroup::builder()
                    .auto_scaling_group_name(FLEET)
                    .desired_capacity(1)
                    .build(),
            )
            .build();
        events.observe_groups(&unmet).await;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 4);
        clock.advance(Duration::from_mins(1));
        events.observe_groups(&unmet).await;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 5, "unmet capacity remains eligible");
        Ok(())
    }

    #[tokio::test]
    async fn activity_poll_interval_coalesces_and_replays_only_unacknowledged()
    -> Result<(), Box<dyn Error>> {
        let (fake, clock, fleets) = replay_setup().await?;
        seed_activity(
            &fake,
            LAUNCH_ID,
            "2026-11-19T09:50:15.786Z",
            &format!("Launching a new EC2 instance: {NEW}"),
            "ordinary launch",
        );
        let events = fleets.events();
        let calls = || {
            fake.state()
                .requests
                .iter()
                .filter(|request| request.operation == "DescribeScalingActivities")
                .count()
        };
        let mut left = Vec::new();
        let mut right = Vec::new();
        let (a, b) = tokio::join!(
            events.supply(FLEET, std::iter::once(FLEET), &mut left),
            events.supply(FLEET, std::iter::once(FLEET), &mut right),
        );
        a?;
        b?;
        assert_eq!(calls(), 1);
        assert_eq!(left.len() + right.len(), 2);
        let first = if left.is_empty() { &right } else { &left };
        gang_testkit::fence::with_held(async |held| {
            events.acknowledge(held, FLEET, LAUNCH_ID).await
        })
        .await?;
        clock.advance(Duration::from_secs(59));
        let mut replay = Vec::new();
        events
            .supply(FLEET, std::iter::once(FLEET), &mut replay)
            .await?;
        assert!(replay.is_empty());
        assert_eq!(calls(), 1);
        clock.advance(Duration::from_secs(1));
        events
            .supply(FLEET, std::iter::once(FLEET), &mut replay)
            .await?;
        assert_eq!(calls(), 2);
        assert_eq!(replay.len(), 1);
        assert_ne!(replay[0].id, LAUNCH_ID);
        assert!(first.contains(&replay[0]));
        Ok(())
    }

    #[tokio::test]
    async fn unconfigured_group_polling_is_also_rate_limited() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:50:15.786Z",
        )?));
        let events = Events::new(
            &sdk,
            &config(None)?,
            clock.clone(),
            Arc::new(Managed::default()),
        );
        let mut out = Vec::new();
        let calls = || {
            fake.state()
                .requests
                .iter()
                .filter(|request| request.operation == "DescribeScalingActivities")
                .count()
        };
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 1);
        clock.advance(Duration::from_mins(1));
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(calls(), 2);
        Ok(())
    }

    #[test]
    fn managed_revision_and_sorted_names_track_changes() -> Result<(), Box<dyn Error>> {
        let managed = Managed::default();
        let initial = managed.revision();
        managed.add(fleet("z-fleet")?.view());
        managed.add(fleet("a-fleet")?.view());
        assert_eq!(managed.revision(), initial + 2);
        assert_eq!(managed.group_names(), vec!["a-fleet", "z-fleet"]);
        managed.clear();
        assert_eq!(managed.revision(), initial + 3);
        assert!(managed.group_names().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn optional_termination_enrichment_failure_keeps_asg_cause_and_supply()
    -> Result<(), Box<dyn Error>> {
        let (fake, clock, fleets) = replay_setup().await?;
        fake.state().groups[0] = FakeGroup::new(FLEET, 0, &[]);
        let cause = "an instance was taken out of service in response to a user request";
        seed_activity(
            &fake,
            TERMINATION_ID,
            "2026-11-19T09:50:15.786Z",
            &format!("Terminating EC2 instance: {OLD}"),
            cause,
        );
        fake.state().failures.insert(
            "DescribeInstances",
            FakeFailure {
                status: 400,
                code: "RequestLimitExceeded",
                message: "termination enrichment throttled",
            },
        );
        let mut supply = FleetSupply::default();
        fleets.supply(fleet(FLEET)?.view(), &mut supply).await?;
        assert_eq!((supply.requested, supply.desired), (Some(0), 0));
        assert!(supply.instances.is_empty());
        assert!(supply.events.iter().any(|event| event.id == TERMINATION_ID
            && matches!(&event.kind,
                InfrastructureEventKind::InstanceTerminated {
                    reason: InfrastructureTerminationReason::Other(actual), ..
                } if actual == cause)));

        // Required instance discovery remains an error, even though enrichment is optional.
        fake.state().groups[0] = FakeGroup::new(FLEET, 1, &[(NEW, "InService")]);
        clock.advance(Duration::from_secs(1));
        assert!(
            fleets
                .supply(fleet(FLEET)?.view(), &mut supply)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn activity_polls_serialize_and_cancellation_preserves_acknowledgements()
    -> Result<(), Box<dyn Error>> {
        use std::future::{Future, poll_fn};
        use std::task::Poll;

        let (_fake, clock, fleets) = replay_setup().await?;
        let events = fleets.events();
        let mut initial = Vec::new();
        events
            .supply(FLEET, std::iter::once(FLEET), &mut initial)
            .await?;
        assert_eq!(initial.len(), 1);
        gang_testkit::fence::with_held(async |held| {
            events.acknowledge(held, FLEET, &initial[0].id).await
        })
        .await?;
        clock.advance(Duration::from_mins(1));

        let mut first_out = Vec::new();
        let mut second_out = Vec::new();
        let mut first = Box::pin(events.supply(FLEET, std::iter::once(FLEET), &mut first_out));
        let mut second = Box::pin(events.supply(FLEET, std::iter::once(FLEET), &mut second_out));
        poll_fn(|cx| {
            assert!(matches!(first.as_mut().poll(cx), Poll::Pending));
            assert!(!events.history.read()[FLEET].deliveries[&initial[0].id].seen);
            assert!(matches!(second.as_mut().poll(cx), Poll::Pending));
            assert!(events.activity_poll.try_lock().is_err());
            Poll::Ready(())
        })
        .await;
        drop(first);
        second.await?;
        assert!(second_out.is_empty());

        clock.advance(Duration::from_mins(1));
        let mut left = Vec::new();
        let mut right = Vec::new();
        let (left_result, right_result) = tokio::join!(
            events.supply(FLEET, std::iter::once(FLEET), &mut left),
            events.supply(FLEET, std::iter::once(FLEET), &mut right),
        );
        left_result?;
        right_result?;
        assert!(left.is_empty());
        assert!(right.is_empty());
        assert!(events.history.read()[FLEET].deliveries[&initial[0].id].acknowledged);
        Ok(())
    }

    #[tokio::test]
    async fn activity_acknowledgements_are_bounded_expiring_and_managed()
    -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        for (id, time) in [
            ("older", "2026-11-19T09:50:00Z"),
            ("retained", "2026-11-19T09:51:00Z"),
        ] {
            seed_activity(
                &fake,
                id,
                time,
                &format!("Launching a new EC2 instance: {NEW}"),
                "ordinary launch",
            );
        }
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:52:00Z",
        )?));
        let mut config = config(None)?;
        config.activity_limit = 2;
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let events = Events::new(&sdk, &config, clock.clone(), managed.clone());
        let mut out = Vec::new();
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(out.len(), 2);
        gang_testkit::fence::with_held(async |held| {
            for event in &out {
                events.acknowledge(held, FLEET, &event.id).await?;
            }
            Ok::<(), AwsError>(())
        })
        .await?;
        seed_activity(
            &fake,
            "newest",
            "2026-11-19T09:51:30Z",
            &format!("Launching a new EC2 instance: {NEW}"),
            "ordinary launch",
        );
        clock.advance(Duration::from_mins(1));
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "newest");
        gang_testkit::fence::with_held(async |held| {
            events.acknowledge(held, FLEET, "newest").await
        })
        .await?;
        assert_eq!(events.history.read()[FLEET].deliveries.len(), 2);
        assert!(
            !events.history.read()[FLEET]
                .deliveries
                .contains_key("older")
        );
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert!(out.is_empty());

        fake.state().failures.insert(
            "DescribeScalingActivities",
            FakeFailure {
                status: 400,
                code: "Throttling",
                message: "activity history throttled",
            },
        );
        clock.advance(Duration::from_mins(1));
        assert!(
            events
                .supply(FLEET, std::iter::once(FLEET), &mut out)
                .await
                .is_err()
        );
        fake.state().failures.remove("DescribeScalingActivities");
        clock.advance(Duration::from_mins(1));
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert!(out.is_empty(), "failed polls must retain acknowledgements");

        managed.clear();
        events.supply(FLEET, std::iter::empty(), &mut out).await?;
        assert!(out.is_empty());
        assert!(events.history.read().is_empty());
        assert!(events.activity_poll.lock().await.attempts.is_empty());
        managed.add(fleet(FLEET)?.view());
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(events.history.read()[FLEET].deliveries.len(), 2);
        clock.advance(config.activity_history_window.get());
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert!(out.is_empty());
        assert!(events.history.read()[FLEET].deliveries.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn queue_admits_managed_notices_and_leaves_malformed_for_dlq()
    -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        fake.state()
            .groups
            .push(FakeGroup::new(FLEET, 1, &[(OLD, "InService")]));
        fake.state()
            .groups
            .push(FakeGroup::new("foreign", 1, &[(NEW, "InService")]));
        fake.state()
            .launch_times
            .insert(OLD.to_owned(), "2026-11-19T09:50:15Z".to_owned());
        fake.state()
            .launch_times
            .insert(NEW.to_owned(), "2026-11-19T09:50:15Z".to_owned());
        let body = |id: &str, instance: &str| {
            json!({
            "id": id, "source": "aws.ec2", "time": "2026-11-19T09:51:18Z",
            "detail-type": "EC2 Instance Rebalance Recommendation", "detail": {"instance-id": instance}
        }).to_string()
        };
        for (id, payload) in [
            ("notice", body("notice", OLD)),
            ("foreign", body("foreign", NEW)),
            ("bad", "{".to_owned()),
        ] {
            fake.state().messages.push(FakeMessage {
                id: id.to_owned(),
                receipt: id.to_owned(),
                body: payload,
                visible: true,
            });
        }
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:52:00Z",
        )?));
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let config = config(Some(fake.queue_url()))?;
        let events = Events::new(&sdk, &config, clock, managed);
        gang_testkit::fence::with_held(async |held| {
            events.receive(held).await?;
            let mut out = Vec::new();
            events
                .supply(FLEET, std::iter::once(FLEET), &mut out)
                .await?;
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].id, "notice");
            events.acknowledge(held, FLEET, "notice").await?;
            Ok::<(), AwsError>(())
        })
        .await?;
        let state = fake.state();
        assert_eq!(state.messages.len(), 1);
        assert_eq!(state.messages[0].id, "bad");
        Ok(())
    }

    #[tokio::test]
    async fn eventbridge_launch_enriches_activity_and_deduplicates_delivery()
    -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        seed_activity(
            &fake,
            LAUNCH_ID,
            "2026-11-19T09:51:21.044Z",
            &format!("Launching a new EC2 instance: {NEW}"),
            "At 2026-11-19T09:51:18Z an instance was launched in response to an EC2 instance rebalance recommendation.",
        );
        let body = json!({
            "version": "0", "id": "eventbridge-1", "detail-type": "EC2 Instance Launch Successful",
            "source": "aws.autoscaling", "time": "2026-11-19T09:51:26Z",
            "detail": {"AutoScalingGroupName": FLEET, "ActivityId": LAUNCH_ID, "EC2InstanceId": NEW}
        })
        .to_string();
        for receipt in ["first", "second"] {
            fake.state().messages.push(FakeMessage {
                id: receipt.to_owned(),
                receipt: receipt.to_owned(),
                body: body.clone(),
                visible: true,
            });
        }
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:52:00Z",
        )?));
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let events = Events::new(&sdk, &config(Some(fake.queue_url()))?, clock, managed);
        gang_testkit::fence::with_held(async |held| {
            events.receive(held).await?;
            let mut out = Vec::new();
            events
                .supply(FLEET, std::iter::once(FLEET), &mut out)
                .await?;
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].id, "eventbridge-1");
            assert_eq!(
                out[0].kind,
                InfrastructureEventKind::ReplacementLaunched {
                    instance: InstanceId::try_new(NEW)
                        .map_err(|_| AwsError::malformed("test", "instance"))?,
                    replacing: None
                }
            );
            events.acknowledge(held, FLEET, "eventbridge-1").await?;
            Ok::<(), AwsError>(())
        })
        .await?;
        assert_eq!(fake.state().messages.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn activity_discovery_pages_and_respects_limit() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        for index in 0..101 {
            seed_activity(
                &fake,
                &format!("activity-{index}"),
                "2026-11-19T09:51:21Z",
                &format!("Launching a new EC2 instance: {NEW}"),
                "ordinary launch",
            );
        }
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:52:00Z",
        )?));
        let mut config = config(None)?;
        config.activity_limit = 101;
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let events = Events::new(&sdk, &config, clock.clone(), managed);
        let mut out = Vec::new();
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(out.len(), 101);
        assert!(out.iter().any(|event| event.id == "activity-100"));
        assert_eq!(
            fake.state()
                .requests
                .iter()
                .filter(|request| request.operation == "DescribeScalingActivities")
                .count(),
            2
        );
        gang_testkit::fence::with_held(async |held| {
            for event in &out {
                events.acknowledge(held, FLEET, &event.id).await?;
            }
            Ok::<(), AwsError>(())
        })
        .await?;
        seed_activity(
            &fake,
            "newest",
            "2026-11-19T09:51:30Z",
            &format!("Launching a new EC2 instance: {NEW}"),
            "ordinary launch",
        );
        clock.advance(config.activity_poll_interval.get());
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(
            out.len(),
            1,
            "later pages must retain acknowledged source IDs"
        );
        assert_eq!(out[0].id, "newest");
        assert_eq!(events.history.read()[FLEET].deliveries.len(), 101);
        Ok(())
    }

    #[tokio::test]
    async fn expired_pending_message_uses_fresh_receipt() -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        fake.state()
            .groups
            .push(FakeGroup::new(FLEET, 1, &[(OLD, "InService")]));
        fake.state()
            .launch_times
            .insert(OLD.to_owned(), "2026-11-19T09:50:15Z".to_owned());
        let body = json!({"id":"notice", "source":"aws.ec2", "time":"2026-11-19T09:51:18Z",
            "detail-type":"EC2 Spot Instance Interruption Warning",
            "detail":{"instance-id":OLD,"instance-action":"terminate"}})
        .to_string();
        fake.state().messages.push(FakeMessage {
            id: "message".to_owned(),
            body,
            receipt: "old".to_owned(),
            visible: true,
        });
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:52:00Z",
        )?));
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let events = Events::new(
            &sdk,
            &config(Some(fake.queue_url()))?,
            clock.clone(),
            managed,
        );
        gang_testkit::fence::with_held(async |held| {
            events.receive(held).await?;
            clock.advance(Duration::from_secs(31));
            {
                let mut state = fake.state();
                if let Some(message) = state.messages.first_mut() {
                    message.visible = true;
                    message.receipt = "new".to_owned();
                }
            }
            events.receive(held).await?;
            let mut out = Vec::new();
            events
                .supply(FLEET, std::iter::once(FLEET), &mut out)
                .await?;
            assert_eq!(out.len(), 1);
            assert_eq!(
                out[0].kind,
                InfrastructureEventKind::SpotInterruption {
                    instance: InstanceId::try_new(OLD)
                        .map_err(|_| AwsError::malformed("test", "instance"))?,
                    action: InfrastructureInterruptionAction::Terminate,
                }
            );
            events.acknowledge(held, FLEET, "notice").await?;
            Ok::<(), AwsError>(())
        })
        .await?;
        assert!(fake.state().messages.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn missing_instance_is_retried_until_managed_tag_is_visible() -> Result<(), Box<dyn Error>>
    {
        let fake = FakeAws::start().await?;
        let body = json!({"id":"notice", "source":"aws.ec2", "time":"2026-11-19T09:51:18Z",
            "detail-type":"EC2 Instance Rebalance Recommendation", "detail":{"instance-id":OLD}})
        .to_string();
        fake.state().messages.push(FakeMessage {
            id: "message".to_owned(),
            body,
            receipt: "old".to_owned(),
            visible: true,
        });
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:52:00Z",
        )?));
        let managed = Arc::new(Managed::default());
        managed.add(fleet(FLEET)?.view());
        let events = Events::new(&sdk, &config(Some(fake.queue_url()))?, clock, managed);
        gang_testkit::fence::with_held(async |held| {
            events.receive(held).await?;
            assert_eq!(fake.state().messages.len(), 1);
            let mut out = Vec::new();
            events
                .supply(FLEET, std::iter::once(FLEET), &mut out)
                .await?;
            assert!(out.is_empty());
            {
                let mut state = fake.state();
                state
                    .groups
                    .push(FakeGroup::new(FLEET, 1, &[(OLD, "InService")]));
                state
                    .launch_times
                    .insert(OLD.to_owned(), "2026-11-19T09:50:15Z".to_owned());
                if let Some(message) = state.messages.first_mut() {
                    message.visible = true;
                    message.receipt = "new".to_owned();
                }
            }
            events.receive(held).await?;
            events
                .supply(FLEET, std::iter::once(FLEET), &mut out)
                .await?;
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].id, "notice");
            events.acknowledge(held, FLEET, "notice").await?;
            Ok::<(), AwsError>(())
        })
        .await?;
        assert!(fake.state().messages.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn termination_reason_uses_ec2_and_failed_termination_is_not_capacity_failure()
    -> Result<(), Box<dyn Error>> {
        let fake = FakeAws::start().await?;
        for (id, code) in [
            (OLD, "Server.SpotInstanceTermination"),
            (NEW, "Client.UserInitiatedShutdown"),
        ] {
            let mut state = fake.state();
            state
                .launch_times
                .insert(id.to_owned(), "2026-11-19T09:50:00Z".to_owned());
            state.state_reasons.insert(id.to_owned(), code.to_owned());
        }
        seed_activity(
            &fake,
            "reclaimed",
            "2026-11-19T09:51:00Z",
            &format!("Terminating EC2 instance: {OLD}"),
            "an instance was taken out of service",
        );
        seed_activity(
            &fake,
            "user",
            "2026-11-19T09:51:01Z",
            &format!("Terminating EC2 instance: {NEW}"),
            "an instance was taken out of service in response to a user request",
        );
        seed_activity(
            &fake,
            "rebalance",
            "2026-11-19T09:51:01Z",
            &format!("Terminating EC2 instance: {OLD}"),
            "an instance was taken out of service in response to an EC2 instance rebalance recommendation",
        );
        seed_activity(
            &fake,
            "failed-termination",
            "2026-11-19T09:51:02Z",
            &format!("Terminating EC2 instance: {OLD}"),
            "termination failed",
        );
        if let Some(activity) = fake.state().activities.last_mut() {
            activity.status_code = "Failed".to_owned();
            activity.message = Some("termination failed".to_owned());
        }
        seed_activity(
            &fake,
            "failed-launch",
            "2026-11-19T09:51:03Z",
            "Launching a new EC2 instance",
            "launch failed",
        );
        if let Some(activity) = fake.state().activities.last_mut() {
            activity.status_code = "Failed".to_owned();
            activity.message = Some("insufficient capacity".to_owned());
        }
        let sdk = fake.sdk_config().await;
        let clock = Arc::new(TestClock::new(Timestamp::from_rfc3339(
            "2026-11-19T09:52:00Z",
        )?));
        let events = Events::new(&sdk, &config(None)?, clock, Arc::new(Managed::default()));
        let mut out = Vec::new();
        events
            .supply(FLEET, std::iter::once(FLEET), &mut out)
            .await?;
        assert_eq!(out.len(), 4);
        assert!(out.iter().any(|event| event.id == "reclaimed"
            && matches!(
                &event.kind,
                InfrastructureEventKind::InstanceTerminated {
                    reason: InfrastructureTerminationReason::SpotReclaimed,
                    ..
                }
            )));
        assert!(out.iter().any(|event| event.id == "user" && matches!(&event.kind,
            InfrastructureEventKind::InstanceTerminated { reason: InfrastructureTerminationReason::Other(cause), .. }
                if cause.contains("user request"))));
        assert!(out.iter().any(|event| event.id == "failed-launch" && matches!(&event.kind,
            InfrastructureEventKind::CapacityUnavailable { reason } if reason == "insufficient capacity")));
        assert!(!out.iter().any(|event| event.id == "failed-termination"));
        assert!(out.iter().any(|event| event.id == "rebalance"
            && matches!(
                &event.kind,
                InfrastructureEventKind::InstanceTerminated {
                    reason: InfrastructureTerminationReason::Rebalance,
                    ..
                }
            )));
        assert_eq!(
            fake.state()
                .requests
                .iter()
                .filter(|request| request.operation == "DescribeInstances")
                .count(),
            2,
            "proven rebalance must not query EC2, even when EC2 would report spot reclaim"
        );
        Ok(())
    }
}
