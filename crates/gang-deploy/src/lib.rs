//! The decisions of Gang's deploy workflow (`.github/workflows/deploy.yml`), kept apart from the
//! product crates: the plan policy over `tofu show -json`, the plan manifest and its comment
//! line, the plan object key and age, the verification of the plan run and the stored plan
//! object, and the plan comment.
//!
//! The library performs no I/O and reads no clock: the workflow's glue
//! (`infra/scripts/reviewed-plan.sh`) runs `tofu`, `aws`, `gh`, `git` and `date`, and pipes
//! their JSON to the hidden `gang deploy` commands, which call this crate.
//!
//! ```
//! use gang_deploy::{DeployError, Plan};
//!
//! let plan: Plan = r#"{"resource_changes": [
//!     {"address": "aws_eip.gateway", "type": "aws_eip", "change": {"actions": ["delete"]}}
//! ]}"#
//!     .parse()?;
//! assert!(plan.is_blocked());
//! assert_eq!(plan.protected_changes()[0].to_string(), "aws_eip.gateway\taws_eip\tdelete");
//! # Ok::<(), DeployError>(())
//! ```

mod comment;
mod errors;
mod github;
mod ids;
mod key;
mod manifest;
mod newtype;
mod object;
mod plan;
mod policy;
mod resolve;
mod sha;
mod store;
mod timestamp;

pub use comment::{CommentSource, PlanComment};
pub use errors::{DeployError, Document, Expected};
pub use github::{
    Comment, Comments, PLAN_COMMENT_AUTHOR, PLAN_COMMENT_MARKER, PullRequest, PullRequests,
    RERUN_REMEDY, WorkflowPath, WorkflowRun, WorkflowRuns,
};
pub use ids::{CommentId, PrNumber, Repository, RepositoryId, RunAttempt, RunId};
pub use key::{PlanKey, PlanPrefix, PlanVersion};
pub use manifest::{
    MANIFEST_LINE_PREFIX, MANIFEST_SCHEMA_VERSION, Manifest, ObjectMetadata, PlanFacts,
};
pub use object::{ObjectHead, ObjectVersions};
pub use plan::{Action, ActionKind, Actions, Change, OutputChange, Plan, ResourceChange};
pub use policy::{PROTECTED_TYPES, ProtectedChange};
pub use resolve::{ResolveEvidence, ResolveFacts, resolve};
pub use sha::{PlanSha256, Sha};
pub use store::{StoreInputs, StoreRequest};
pub use timestamp::{MaxAge, Timestamp, UnixTime, object_age};
