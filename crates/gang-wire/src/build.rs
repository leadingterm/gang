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
    fn older_cache_counts_default_bytes() {
        let counts: CacheCounts = serde_json::from_value(json!({
            "hits": 1, "misses": 2, "errors": 3, "uploadedResults": 4
        }))
        .expect("old counts parse");
        assert_eq!((counts.uploaded_bytes, counts.downloaded_bytes), (0, 0));
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
