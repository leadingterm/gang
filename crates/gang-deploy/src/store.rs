//! Storing a reviewed plan: which merge may store a plan, the facts recorded about it, and the
//! manifest written once the plan store has returned the object version.

use std::fmt;
use std::str::FromStr;

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};

use crate::errors::{DeployError, Document, Result};
use crate::ids::{PrNumber, Repository, RepositoryId, RunAttempt, RunId};
use crate::key::{PlanKey, PlanPrefix, PlanVersion};
use crate::manifest::{Manifest, ObjectMetadata, PlanFacts};
use crate::plan::Plan;
use crate::sha::{PlanSha256, Sha};
use crate::timestamp::{MaxAge, Timestamp, UnixTime};

/// What the plan job knows about the merge it planned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreInputs {
    /// The repository: `GITHUB_REPOSITORY`.
    pub repository: Repository,
    /// The repository's numeric id: `GITHUB_REPOSITORY_ID`.
    pub repository_id: RepositoryId,
    /// The pull request: `PR_NUMBER`.
    pub pr_number: PrNumber,
    /// The head of the pull request: `PR_HEAD_SHA`.
    pub pr_head_sha: Sha,
    /// The merge commit the workflow runs for: `GITHUB_SHA`.
    pub sha: Sha,
    /// The commit checked out: `git rev-parse HEAD`.
    pub head: Sha,
    /// The parents of the checked-out commit, in order.
    pub parents: Vec<Sha>,
    /// The tree of the checked-out commit.
    pub tree_sha: Sha,
    /// The workflow run: `GITHUB_RUN_ID`.
    pub run_id: RunId,
    /// The attempt of the run: `GITHUB_RUN_ATTEMPT`.
    pub run_attempt: RunAttempt,
    /// The key prefix of the plan store.
    pub prefix: PlanPrefix,
    /// How long the stored plan stays usable.
    pub max_age: MaxAge,
    /// The time of storing.
    pub now: UnixTime,
}

/// What the plan store uploads: the object key, its metadata, and the facts the manifest records.
/// It prints as JSON with the members `facts`, `metadata` and `plan_key`; `metadata` is derived
/// from `facts` and ignored when the request is read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRequest {
    /// The object key to store the plan at.
    pub plan_key: PlanKey,
    /// What the plan is.
    pub facts: PlanFacts,
}

/// The JSON form of a store request.
#[derive(Serialize)]
struct StoreRequestJson<'a> {
    facts: &'a PlanFacts,
    metadata: String,
    plan_key: &'a PlanKey,
}

/// The JSON form of a store request as it is read back.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreRequestInput {
    facts: PlanFacts,
    #[serde(rename = "metadata")]
    _metadata: IgnoredAny,
    plan_key: PlanKey,
}

impl StoreRequest {
    /// The request to store the saved plan with digest `plan_sha256` that `plan` describes, for
    /// the merge `inputs` names.
    ///
    /// # Errors
    ///
    /// When the checked-out commit is not the merge the workflow runs for, is not a merge of two
    /// parents, or does not merge the pull request head; or when the expiry is out of range.
    pub fn try_new(inputs: StoreInputs, plan: &Plan, plan_sha256: PlanSha256) -> Result<Self> {
        if inputs.head != inputs.sha {
            return Err(DeployError::StoreHead);
        }
        let found = inputs.parents.len();
        let Ok([base_sha, second]) = <[Sha; 2]>::try_from(inputs.parents) else {
            return Err(DeployError::StoreParents { found });
        };
        if second != inputs.pr_head_sha {
            return Err(DeployError::StoreSecondParent {
                parent: second,
                pr_head: inputs.pr_head_sha,
            });
        }
        let expires = inputs
            .now
            .checked_add(inputs.max_age)
            .ok_or(DeployError::TimeRange {
                seconds: inputs.now.as_secs(),
            })?;
        let facts = PlanFacts {
            repository: inputs.repository,
            repository_id: inputs.repository_id,
            pr_number: inputs.pr_number,
            head_sha: inputs.pr_head_sha,
            base_sha,
            merge_sha: inputs.sha,
            tree_sha: inputs.tree_sha,
            workflow_run_id: inputs.run_id,
            workflow_run_attempt: inputs.run_attempt,
            plan_sha256,
            has_changes: plan.has_changes(),
            created_at: Timestamp::from_unix(inputs.now)?,
            expires_at: Timestamp::from_unix(expires)?,
        };
        Ok(Self {
            plan_key: facts.object_key(&inputs.prefix),
            facts,
        })
    }

    /// The S3 user metadata the plan object carries.
    #[must_use]
    pub fn metadata(&self) -> ObjectMetadata {
        self.facts.object_metadata()
    }

    /// The manifest of the plan once the plan store stored it as `plan_version`.
    #[must_use]
    pub fn into_manifest(self, plan_version: PlanVersion) -> Manifest {
        Manifest {
            facts: self.facts,
            plan_key: self.plan_key,
            plan_version,
        }
    }
}

impl fmt::Display for StoreRequest {
    /// The compact JSON form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json = StoreRequestJson {
            facts: &self.facts,
            metadata: self.metadata().to_string(),
            plan_key: &self.plan_key,
        };
        let text = serde_json::to_string(&json).map_err(|_| fmt::Error)?;
        f.write_str(&text)
    }
}

impl FromStr for StoreRequest {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        let input: StoreRequestInput =
            serde_json::from_str(text).map_err(|source| DeployError::Json {
                document: Document::StoreRequest,
                source,
            })?;
        Ok(Self {
            plan_key: input.plan_key,
            facts: input.facts,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{StoreInputs, StoreRequest};
    use crate::manifest::Manifest;
    use crate::plan::tests::{fixture, plan};
    use crate::sha::{PlanSha256, Sha};
    use crate::timestamp::{Timestamp, UnixTime};

    const BASE: &str = "2222222222222222222222222222222222222222";
    const HEAD: &str = "1111111111111111111111111111111111111111";
    const MERGE: &str = "3333333333333333333333333333333333333333";
    const TREE: &str = "4444444444444444444444444444444444444444";

    fn sha(text: &str) -> Sha {
        text.parse().unwrap()
    }

    /// The inputs of run 99 attempt 1 storing the merge of pull request 7 at 2026-01-02T00:00:00Z.
    fn inputs() -> StoreInputs {
        StoreInputs {
            repository: "example-org/gang".parse().unwrap(),
            repository_id: "1".parse().expect("positive repository id"),
            pr_number: "7".parse().expect("positive pull request number"),
            pr_head_sha: sha(HEAD),
            sha: sha(MERGE),
            head: sha(MERGE),
            parents: vec![sha(BASE), sha(HEAD)],
            tree_sha: sha(TREE),
            run_id: "99".parse().expect("positive run id"),
            run_attempt: "1".parse().expect("positive run attempt"),
            prefix: "plans".parse().unwrap(),
            max_age: "86400".parse().expect("positive max age"),
            now: UnixTime::from_seconds(1_767_312_000),
        }
    }

    fn request(inputs: StoreInputs) -> crate::errors::Result<StoreRequest> {
        StoreRequest::try_new(inputs, &plan(&[]), PlanSha256::digest(b"plan bytes\n"))
    }

    #[test]
    fn store_records_the_merge_of_a_two_parent_revision() {
        let request = request(inputs()).unwrap();
        let facts = &request.facts;
        assert_eq!(facts.base_sha, sha(BASE));
        assert_eq!(facts.head_sha, sha(HEAD));
        assert_eq!(facts.merge_sha, sha(MERGE));
        assert_eq!(facts.tree_sha, sha(TREE));
        assert_eq!(facts.plan_sha256, PlanSha256::digest(b"plan bytes\n"));
        assert_eq!(
            request.plan_key.as_str(),
            format!("plans/1/7/{HEAD}/{TREE}/99-1.tfplan")
        );
    }

    #[test]
    fn store_records_has_changes_false_for_an_empty_plan() {
        assert!(!request(inputs()).unwrap().facts.has_changes);
        let changing = StoreRequest::try_new(
            inputs(),
            &fixture("parameter-value"),
            PlanSha256::digest(b""),
        )
        .unwrap();
        assert!(changing.facts.has_changes);
    }

    #[test]
    fn store_sets_expires_at_to_created_at_plus_the_max_age() {
        let facts = request(inputs()).unwrap().facts;
        assert_eq!(
            facts.created_at,
            "2026-01-02T00:00:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(
            facts.expires_at,
            "2026-01-03T00:00:00Z".parse::<Timestamp>().unwrap()
        );
    }

    #[test]
    fn store_rejects_a_head_that_is_not_github_sha() {
        let error = request(StoreInputs {
            head: sha(HEAD),
            ..inputs()
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "HEAD is not GITHUB_SHA; refusing to store a plan for another revision"
        );
    }

    #[test]
    fn store_rejects_a_second_parent_that_is_not_pr_head_sha() {
        let error = request(StoreInputs {
            pr_head_sha: sha(BASE),
            ..inputs()
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("second parent {HEAD} is not PR_HEAD_SHA {BASE}")
        );
    }

    #[test]
    fn store_rejects_a_revision_with_one_parent() {
        let error = request(StoreInputs {
            parents: vec![sha(BASE)],
            ..inputs()
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "stored revision must merge exactly two parents; found 1"
        );
    }

    #[test]
    fn store_rejects_an_expiry_out_of_range() {
        let error = request(StoreInputs {
            now: UnixTime::from_seconds(253_402_300_000),
            max_age: crate::timestamp::MaxAge::from_seconds(std::num::NonZeroU32::MAX),
            ..inputs()
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "257697267295 seconds since the unix epoch is out of range"
        );
    }

    #[test]
    fn store_request_prints_the_object_metadata_and_reads_back() {
        let request = request(inputs()).unwrap();
        let printed = request.to_string();
        let json: Value = serde_json::from_str(&printed).unwrap();
        assert_eq!(json["metadata"], request.metadata().to_string());
        assert_eq!(json["plan_key"], request.plan_key.as_str());
        assert_eq!(printed.parse::<StoreRequest>().unwrap(), request);
    }

    #[test]
    fn store_records_the_stored_object_version_in_the_manifest() {
        let request = request(inputs()).unwrap();
        let manifest = request.clone().into_manifest("v-123".parse().unwrap());
        assert_eq!(manifest.plan_version.as_str(), "v-123");
        assert_eq!(manifest.plan_key, request.plan_key);
        assert_eq!(manifest.to_string().parse::<Manifest>().unwrap(), manifest);
    }
}
