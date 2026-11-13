//! Typed errors of the deploy decisions. Each message is the line the deploy workflow logs when
//! the decision refuses to go on.

use std::fmt;

use crate::github::RERUN_REMEDY;
use crate::key::PlanKey;
use crate::sha::{PlanSha256, Sha};
use crate::timestamp::{MaxAge, Timestamp};

/// The JSON documents the deploy decisions read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Document {
    /// `tofu show -json` of a saved plan.
    Plan,
    /// A plan manifest.
    Manifest,
    /// A store request printed by `gang deploy store-request`.
    StoreRequest,
    /// The pull requests of a commit, from the GitHub API.
    PullRequests,
    /// The comments of a pull request, from the GitHub API.
    Comments,
    /// The runs of a workflow, every page joined, from the GitHub API.
    WorkflowRuns,
    /// The evidence of a merge: its pull requests, their comments, the plan run and the workflow runs.
    Evidence,
    /// The versions of a plan object, from `aws s3api list-object-versions`.
    ObjectVersions,
    /// The head of a plan object, from `aws s3api head-object`.
    ObjectHead,
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Plan => "plan",
            Self::Manifest => "manifest",
            Self::StoreRequest => "store request",
            Self::PullRequests => "pull requests",
            Self::Comments => "comments",
            Self::WorkflowRuns => "workflow runs",
            Self::Evidence => "merge evidence",
            Self::ObjectVersions => "object versions",
            Self::ObjectHead => "object head",
        })
    }
}

/// The form a value must take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// A git object id: 40 lowercase hex characters.
    Sha,
    /// A SHA-256 digest: 64 lowercase hex characters.
    PlanSha256,
    /// A decimal number of digits only.
    Number,
    /// A positive decimal number of digits only.
    PositiveNumber,
    /// A GitHub repository as `owner/name`.
    Repository,
    /// A manifest time: `YYYY-MM-DDTHH:MM:SSZ`.
    Timestamp,
    /// A non-empty plan object key.
    PlanKey,
}

impl fmt::Display for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sha => "a 40-character lowercase hex sha",
            Self::PlanSha256 => "a 64-character lowercase hex sha256",
            Self::Number => "numeric",
            Self::PositiveNumber => "a positive number",
            Self::Repository => "an owner/name repository",
            Self::Timestamp => "a YYYY-MM-DDTHH:MM:SSZ timestamp",
            Self::PlanKey => "a non-empty plan key",
        })
    }
}

/// A refusal of a deploy decision, or input it cannot read.
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    /// A value does not have the form its field requires.
    #[error("`{text}` is not {expected}")]
    Invalid {
        /// The rejected text.
        text: String,
        /// The form the value must take.
        expected: Expected,
    },
    /// The plan prefix is empty.
    #[error("plan prefix must not be empty")]
    EmptyPrefix,
    /// The plan prefix is absolute.
    #[error("plan prefix must not start with /")]
    RootedPrefix,
    /// The plan store returned no version for the uploaded plan.
    #[error("plan upload returned no version id")]
    NoVersion,
    /// A time lies outside the years the manifest can write.
    #[error("{seconds} seconds since the unix epoch is out of range")]
    TimeRange {
        /// The time, in seconds since the Unix epoch.
        seconds: i64,
    },
    /// A JSON document does not parse into the shape the decision reads.
    #[error("cannot read {document} JSON")]
    Json {
        /// The document that could not be read.
        document: Document,
        /// The parse failure.
        #[source]
        source: serde_json::Error,
    },

    /// A comment does not carry exactly one manifest line.
    #[error("expected exactly one manifest line in comment, found {found}")]
    ManifestLines {
        /// The number of manifest lines found.
        found: usize,
    },
    /// The manifest line is not closed.
    #[error("manifest line does not end with -->")]
    ManifestLineEnd,
    /// The manifest payload has characters outside the base64 alphabet.
    #[error("manifest payload is not base64")]
    ManifestNotBase64,
    /// The manifest payload is not valid base64.
    #[error("manifest payload does not decode")]
    ManifestDecode(#[source] base64::DecodeError),
    /// The manifest is not a JSON object.
    #[error("manifest payload is not a JSON object")]
    ManifestNotObject,
    /// The manifest has another schema version.
    #[error("manifest schema_version must be 1")]
    ManifestSchema,
    /// The manifest lacks a key or has an extra one.
    #[error("manifest keys do not match the expected set")]
    ManifestKeys,
    /// A manifest value does not have the form its key requires.
    #[error("manifest {key} has an invalid value")]
    ManifestValue {
        /// The key of the value.
        key: &'static str,
        /// Why the value was rejected.
        #[source]
        source: serde_json::Error,
    },
    /// A manifest value differs from what the merge shows.
    #[error("manifest {key} mismatch (expected {expected}, found {found})")]
    ManifestMismatch {
        /// The key of the value.
        key: &'static str,
        /// The value the merge shows.
        expected: String,
        /// The value the manifest records.
        found: String,
    },

    /// The checked-out commit is not the commit the workflow runs for.
    #[error("HEAD is not GITHUB_SHA; refusing to store a plan for another revision")]
    StoreHead,
    /// The planned revision is not a merge of two parents.
    #[error("stored revision must merge exactly two parents; found {found}")]
    StoreParents {
        /// The number of parents.
        found: usize,
    },
    /// The second parent of the planned merge is not the pull request head.
    #[error("second parent {parent} is not PR_HEAD_SHA {pr_head}")]
    StoreSecondParent {
        /// The second parent of the merge.
        parent: Sha,
        /// The head of the pull request.
        pr_head: Sha,
    },

    /// The plan object does not have exactly one version.
    #[error("plan object {key} has {count} versions; exactly one is allowed")]
    ObjectVersions {
        /// The key of the plan object.
        key: PlanKey,
        /// The number of versions.
        count: usize,
    },
    /// The plan object has a delete marker.
    #[error("plan object {key} has a delete marker")]
    ObjectDeleteMarker {
        /// The key of the plan object.
        key: PlanKey,
    },
    /// The plan object's version is not the one the manifest records.
    #[error("plan object version differs from the manifest")]
    ObjectVersion,
    /// A metadata value of the plan object differs from the manifest.
    #[error("plan object metadata {name} mismatch (expected {expected}, found {found})")]
    ObjectMetadata {
        /// The metadata name.
        name: &'static str,
        /// The value the manifest implies.
        expected: String,
        /// The value the object carries.
        found: String,
    },
    /// The plan object has no readable modification time.
    #[error("plan object has no usable LastModified")]
    ObjectLastModified,
    /// The plan object was modified after now.
    #[error("plan object was modified in the future")]
    ObjectFuture,
    /// The plan object is older than the maximum age.
    #[error("plan object age {age} exceeds {max_age} seconds")]
    ObjectAge {
        /// The age of the object in seconds.
        age: i64,
        /// The maximum age.
        max_age: MaxAge,
    },
    /// A maximum age that is not a positive whole number of seconds up to `u32::MAX`.
    #[error("maximum age {0:?} is not a positive whole number of seconds up to 4294967295")]
    MaxAge(std::time::Duration),
    /// The plan expired.
    #[error("plan expired at {expires_at}")]
    Expired {
        /// When the plan expired.
        expires_at: Timestamp,
    },
    /// The downloaded plan is not the one the manifest records.
    #[error("plan sha256 mismatch (expected {expected}, found {found})")]
    Digest {
        /// The digest the manifest records.
        expected: PlanSha256,
        /// The digest of the downloaded plan.
        found: PlanSha256,
    },

    /// The workflow does not run for main.
    #[error("resolve runs only for refs/heads/main, got {git_ref}")]
    NotMain {
        /// The ref the workflow runs for.
        git_ref: String,
    },
    /// The checked-out commit is not the commit the workflow runs for.
    #[error("HEAD is not GITHUB_SHA")]
    ResolveHead,
    /// The main commit is not a squash merge.
    #[error("main commit must have exactly one parent (squash merge); found {found}")]
    MainParents {
        /// The number of parents.
        found: usize,
    },
    /// No single merged pull request of this repository has the commit as its merge commit.
    #[error("no single merged same-repository pull request has merge commit {sha}")]
    NoMergedPullRequest {
        /// The merge commit.
        sha: Sha,
    },
    /// The pull request does not have exactly one plan comment.
    #[error("expected one plan comment from github-actions[bot], found {found}")]
    PlanComments {
        /// The number of plan comments.
        found: usize,
    },
    /// The workflow ref names no workflow file.
    #[error("workflow ref has no .github/workflows/<name>.yml or .yaml path: {workflow_ref}")]
    WorkflowRef {
        /// The workflow ref.
        workflow_ref: String,
    },
    /// A field of the plan run differs from the manifest.
    #[error("plan run {field} mismatch (expected {expected}, found {found})")]
    RunMismatch {
        /// The field of the run.
        field: &'static str,
        /// The value the manifest implies.
        expected: String,
        /// The value of the run.
        found: String,
    },
    /// The plan run's attempt is not the one the manifest records.
    #[error(
        "plan run run_attempt mismatch (expected {expected}, found {found}); {remedy}",
        remedy = RERUN_REMEDY
    )]
    RunAttemptMismatch {
        /// The attempt the manifest records.
        expected: String,
        /// The attempt of the run.
        found: String,
    },
    /// No plan run exists for the pull request head.
    #[error("no plan run found for {head}")]
    NoPlanRun {
        /// The pull request head.
        head: Sha,
    },
    /// The plan run is not the newest successful run for the pull request head.
    #[error("a newer plan run exists for {head}; {remedy}", remedy = RERUN_REMEDY)]
    NewerPlanRun {
        /// The pull request head.
        head: Sha,
    },
}

/// Result type of the deploy decisions.
pub type Result<T, E = DeployError> = std::result::Result<T, E>;
