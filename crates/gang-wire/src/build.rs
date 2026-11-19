//! Build resources and their observed farm usage.

use std::time::Duration;

use gang_clock::Timestamp;
use serde::{Deserialize, Serialize};

use crate::Named;

wire_enum! {
    /// Whether a build is still sending observations.
    pub enum BuildState default Active {
        /// An observation arrived within the idle threshold.
        Active = "ACTIVE",
        /// No observation arrived within the idle threshold; the outcome is unknown.
        Idle = "IDLE",
    }
}

wire_enum! {
    /// An observed RPC forwarded by a follower.
    pub enum IngestRpc {
        /// Action cache lookup.
        GetActionResult = "GET_ACTION_RESULT",
        /// Action cache upload.
        UpdateActionResult = "UPDATE_ACTION_RESULT",
        /// Action execution request.
        Execute = "EXECUTE",
        /// Other `ActionCache`, `ContentAddressableStorage` or `ByteStream` call.
        Cas = "CAS",
    }
}

wire_enum! {
    /// The outcome of a forwarded RPC.
    pub enum IngestOutcome {
        /// The call succeeded.
        Ok = "OK",
        /// The requested result was not found.
        NotFound = "NOT_FOUND",
        /// The call failed otherwise.
        Error = "ERROR",
    }
}

/// Body of `POST /v1/builds:ingest`: observations a follower made, for the leader to fold.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(optional_fields)]
pub struct IngestBuildsRequest {
    /// Origins referenced by calls and losses.
    pub origins: Vec<IngestOrigin>,
    /// Observed calls.
    pub calls: Vec<IngestCall>,
    /// Counts of lost observations by origin.
    #[serde(default)]
    pub lost: Vec<IngestLoss>,
    /// Lost observations that could not be attributed to an origin.
    #[serde(default)]
    #[ts(type = "number")]
    pub unattributed: u64,
    /// Sender's cumulative cache traffic since its process started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic: Option<IngestTraffic>,
}

/// Cumulative cache traffic from one controller process.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IngestTraffic {
    /// Start of the sender's process.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub start_time: Timestamp,
    /// Action cache lookups.
    #[ts(type = "number")]
    pub lookups: u64,
    /// Successful action cache lookups.
    #[ts(type = "number")]
    pub hits: u64,
    /// Client-sent cache-service message bytes.
    #[ts(type = "number")]
    pub uploaded_bytes: u64,
    /// Client-received cache-service message bytes.
    #[ts(type = "number")]
    pub downloaded_bytes: u64,
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// A build and the Bazel metadata of the calls that reference it.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ts_rs::TS,
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(optional_fields)]
pub struct IngestOrigin {
    /// Principal identity in `provider:principal` form.
    pub principal: String,
    /// Bazel invocation identifier.
    pub invocation_id: String,
    /// Bazel correlated invocations identifier, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlated_invocations_id: Option<String>,
    /// Tool name and version, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    /// Target label, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Action mnemonic, when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mnemonic: Option<String>,
}

/// A call referring to an entry in the request's origins.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(optional_fields)]
pub struct IngestCall {
    /// Index into the request's origins.
    pub origin: u32,
    /// RPC that was called.
    pub rpc: IngestRpc,
    /// Outcome of the call.
    pub outcome: IngestOutcome,
    /// Client-sent message bytes, omitted when zero.
    #[serde(default, skip_serializing_if = "is_default")]
    #[ts(type = "number")]
    pub uploaded_bytes: u64,
    /// Client-received message bytes, omitted when zero.
    #[serde(default, skip_serializing_if = "is_default")]
    #[ts(type = "number")]
    pub downloaded_bytes: u64,
}

/// Lost observations attributed to an entry in the request's origins.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(optional_fields)]
pub struct IngestLoss {
    /// Index into the request's origins.
    pub origin: u32,
    /// Nonzero number of lost observations.
    #[ts(type = "number")]
    pub count: u64,
}

/// Successful response to a batch of forwarded observations.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(optional_fields)]
pub struct IngestBuildsResponse {}

/// One observed build, named `builds/{id}`.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Build {
    /// Resource name, `builds/{id}`.
    pub name: String,
    /// Authenticated principal, `principals/{identity}`.
    pub principal: String,
    /// Bazel invocation identifier.
    pub invocation_id: String,
    /// Bazel correlated invocations identifier, when supplied.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[ts(as = "Option<String>", optional)]
    pub correlated_invocations_id: String,
    /// Tool name and version, when supplied.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[ts(as = "Option<String>", optional)]
    pub tool: String,
    /// Whether the build has recently been active.
    pub state: BuildState,
    /// Time of the first observation.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub start_time: Timestamp,
    /// Time of the latest observation.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub last_active_time: Timestamp,
    /// Usage for the whole build.
    pub usage: Usage,
    /// Observations lost to a full channel.
    #[ts(type = "number")]
    pub unrecorded: u64,
    /// Usage by mnemonic, present in the full view.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<MnemonicUsage>>", optional)]
    pub mnemonics: Vec<MnemonicUsage>,
    /// Usage without a retained mnemonic, present in the full view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub other_mnemonics: Option<Usage>,
    /// Most-missed targets, most misses first, in the full view.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<MissedTarget>>", optional)]
    pub missed_targets: Vec<MissedTarget>,
    /// Slowest actions, longest span first, in the full view.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<SlowAction>>", optional)]
    pub slowest: Vec<SlowAction>,
    /// Observed activity over time, present in the full view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeline: Option<Timeline>,
}

impl Named for Build {
    fn name(&self) -> &str {
        &self.name
    }
}

/// What some actions of a build did on the farm.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Usage {
    /// Action cache calls.
    pub cache: CacheCounts,
    /// Execution calls and outcomes.
    pub executions: Executions,
    /// Sums of action phase times.
    pub action_time: ActionTime,
}

/// Observed action cache calls.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct CacheCounts {
    /// Successful lookups.
    #[ts(type = "number")]
    pub hits: u64,
    /// Lookups answered not found.
    #[ts(type = "number")]
    pub misses: u64,
    /// Lookups answered with another error.
    #[ts(type = "number")]
    pub errors: u64,
    /// Successful uploads of locally executed results.
    #[ts(type = "number")]
    pub uploaded_results: u64,
    /// Client-sent cache-service message bytes attributed to this build.
    #[serde(default)]
    #[ts(type = "number")]
    pub uploaded_bytes: u64,
    /// Client-received cache-service message bytes attributed to this build.
    #[serde(default)]
    #[ts(type = "number")]
    pub downloaded_bytes: u64,
}

/// Observed execution calls and own execution outcomes.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Executions {
    /// Execute calls answered successfully.
    #[ts(type = "number")]
    pub execute_calls: u64,
    /// Execute calls answered with an error.
    #[ts(type = "number")]
    pub rejected: u64,
    /// Own executions that succeeded.
    #[ts(type = "number")]
    pub completed: u64,
    /// Own executions that failed.
    #[ts(type = "number")]
    pub failed: u64,
}

/// Sums of action phase durations.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct ActionTime {
    /// Time spent queued.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub queued: Duration,
    /// Time spent fetching inputs.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub input_fetch: Duration,
    /// Time spent executing.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub executing: Duration,
    /// Time spent uploading outputs.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub output_upload: Duration,
}

/// Usage attributed to an action mnemonic.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct MnemonicUsage {
    /// Action mnemonic.
    pub mnemonic: String,
    /// Usage attributed to the mnemonic.
    pub usage: Usage,
}

/// An estimated number of cache misses for a target.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct MissedTarget {
    /// Target label.
    pub target: String,
    /// Estimated misses, an upper bound.
    #[ts(type = "number")]
    pub misses: u64,
    /// Error bound; true misses are at least `misses - overestimate`.
    #[ts(type = "number")]
    pub overestimate: u64,
}

/// One of the build's slowest observed actions.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct SlowAction {
    /// Target label, when supplied.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[ts(as = "Option<String>", optional)]
    pub target: String,
    /// Action mnemonic, when supplied.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[ts(as = "Option<String>", optional)]
    pub mnemonic: String,
    /// Action digest.
    pub digest: String,
    /// Worker resource name, when known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[ts(as = "Option<String>", optional)]
    pub worker: String,
    /// When the action was queued.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub queue_time: Timestamp,
    /// Time from queuing to worker completion.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub span: Duration,
    /// Time spent queued.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub queued: Duration,
    /// Time spent fetching inputs.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub input_fetch: Duration,
    /// Time spent executing.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub executing: Duration,
    /// Time spent uploading outputs.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub output_upload: Duration,
    /// Process exit code.
    pub exit_code: i32,
}

/// Activity buckets over a build's lifetime.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct Timeline {
    /// Beginning of the first bucket.
    #[serde(with = "crate::time")]
    #[ts(type = "Timestamp")]
    pub start_time: Timestamp,
    /// Width of each bucket.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub step: Duration,
    /// Consecutive buckets from `start_time`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[ts(as = "Option<Vec<TimelineBucket>>", optional)]
    pub buckets: Vec<TimelineBucket>,
}

/// One timeline step; executing divided by the step is mean executing actions.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct TimelineBucket {
    /// Cache hits in this step.
    #[ts(type = "number")]
    pub hits: u64,
    /// Cache misses in this step.
    #[ts(type = "number")]
    pub misses: u64,
    /// Executing action-time within this step.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub executing: Duration,
    /// Queued action-time within this step.
    #[serde(with = "crate::duration")]
    #[ts(type = "Duration")]
    pub queued: Duration,
}

/// A page of builds.
#[derive(
    gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ts_rs::TS,
)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct ListBuildsResponse {
    /// Builds in the page.
    pub builds: Vec<Build>,
    /// Token for the next page, absent on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<crate::page::PageToken>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn ingest_wire_round_trips_and_preserves_unknown_enum_values() {
        let value = json!({
            "origins": [{"principal": "provider:principal", "invocationId": "invocation"}],
            "calls": [{"origin": 0, "rpc": "FOO", "outcome": "BAR"}]
        });
        let request: IngestBuildsRequest = serde_json::from_value(value).expect("request parses");
        assert_eq!(request.calls[0].rpc, IngestRpc::Unrecognized("FOO".into()));
        assert_eq!(
            request.calls[0].outcome,
            IngestOutcome::Unrecognized("BAR".into())
        );
        assert!(request.lost.is_empty());
        assert_eq!(request.unattributed, 0);
        assert_eq!(request.calls[0].uploaded_bytes, 0);
        assert_eq!(request.calls[0].downloaded_bytes, 0);
        assert!(request.traffic.is_none());
        let encoded = serde_json::to_value(&request).expect("request serializes");
        assert_eq!(encoded["calls"][0]["rpc"], "FOO");
        assert_eq!(encoded["calls"][0]["outcome"], "BAR");
        assert!(encoded["calls"][0].get("uploadedBytes").is_none());
        assert!(encoded["calls"][0].get("downloadedBytes").is_none());
        assert!(encoded.get("traffic").is_none());
        assert!(encoded["origins"][0].get("tool").is_none());
        assert_eq!(
            serde_json::to_value(IngestBuildsResponse::default()).expect("response serializes"),
            json!({})
        );
        assert!(serde_json::from_value::<IngestBuildsResponse>(json!({"extra": true})).is_err());
    }

    #[test]
    fn ingest_traffic_and_call_bytes_round_trip() {
        let value = json!({
            "origins": [],
            "calls": [{"origin": 0, "rpc": "CAS", "outcome": "OK",
                "uploadedBytes": 123, "downloadedBytes": 456}],
            "lost": [], "unattributed": 0,
            "traffic": {"startTime": "2026-01-14T09:00:00.000Z", "lookups": 7,
                "hits": 6, "uploadedBytes": 1234, "downloadedBytes": 4567}
        });
        let request: IngestBuildsRequest = serde_json::from_value(value.clone()).expect("parses");
        assert_eq!(request.calls[0].rpc, IngestRpc::Cas);
        assert_eq!(request.calls[0].uploaded_bytes, 123);
        assert_eq!(
            request.traffic.as_ref().expect("traffic").downloaded_bytes,
            4567
        );
        assert_eq!(serde_json::to_value(request).expect("serializes"), value);
    }

    #[test]
    fn older_cache_counts_default_bytes() {
        let counts: CacheCounts = serde_json::from_value(json!({
            "hits": 1, "misses": 2, "errors": 3, "uploadedResults": 4
        }))
        .expect("old counts parse");
        assert_eq!((counts.uploaded_bytes, counts.downloaded_bytes), (0, 0));
    }

    #[test]
    fn ingest_rejects_unknown_fields_at_every_level() {
        for value in [
            json!({"origins": [], "calls": [], "extra": true}),
            json!({"origins": [{"principal": "p", "invocationId": "i", "extra": true}], "calls": []}),
            json!({"origins": [], "calls": [{"origin": 0, "rpc": "EXECUTE", "outcome": "OK", "extra": true}]}),
            json!({"origins": [], "calls": [], "lost": [{"origin": 0, "count": 1, "extra": true}]}),
        ] {
            assert!(serde_json::from_value::<IngestBuildsRequest>(value).is_err());
        }
    }

    #[test]
    fn full_build_round_trips_with_wire_names() {
        let usage = json!({"cache": {"hits": 1, "misses": 2, "errors": 3, "uploadedResults": 4, "uploadedBytes": 42, "downloadedBytes": 73},
            "executions": {"executeCalls": 5, "rejected": 6, "completed": 7, "failed": 8},
            "actionTime": {"queued": "1.500s", "inputFetch": "2s", "executing": "3s", "outputUpload": "4s"}});
        let value = json!({
            "name": "builds/0123456789abcdef", "principal": "principals/example", "invocationId": "invocation",
            "correlatedInvocationsId": "correlated", "tool": "bazel 8", "state": "ACTIVE",
            "startTime": "2026-01-14T09:00:00.000Z", "lastActiveTime": "2026-01-14T09:00:01.000Z",
            "usage": usage, "unrecorded": 9,
            "mnemonics": [{"mnemonic": "Compile", "usage": usage}], "otherMnemonics": usage,
            "missedTargets": [{"target": "//example:target", "misses": 10, "overestimate": 1}],
            "slowest": [{"target": "//example:target", "mnemonic": "Compile", "digest": "abc/1",
                "worker": "workers/one", "queueTime": "2026-01-14T09:00:00.000Z", "span": "5s",
                "queued": "1.500s", "inputFetch": "2s", "executing": "3s", "outputUpload": "4s", "exitCode": 1}],
            "timeline": {"startTime": "2026-01-14T09:00:00.000Z", "step": "1s",
                "buckets": [{"hits": 1, "misses": 2, "executing": "1.500s", "queued": "0s"}]}
        });
        let build: Build = serde_json::from_value(value.clone()).expect("build parses");
        assert_eq!(
            serde_json::to_value(&build).expect("build serializes"),
            value
        );
        let round_trip: Build =
            serde_json::from_str(&serde_json::to_string(&build).expect("build serializes"))
                .expect("build parses");
        assert_eq!(round_trip, build);
        assert_eq!(
            crate::duration::parse("1.5s").expect("short duration parses"),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn basic_build_omits_full_view_fields() {
        let value = serde_json::to_value(Build::default()).expect("build serializes");
        for key in [
            "mnemonics",
            "otherMnemonics",
            "missedTargets",
            "slowest",
            "timeline",
        ] {
            assert!(value.get(key).is_none(), "{key}");
        }
    }
}
