//! Resolving the reviewed plan of a merge on main: the main commit must be the squash merge of a
//! pull request whose plan comment names the newest successful plan run of its head.

use std::fmt::Display;
use std::str::FromStr;

use serde::Deserialize;

use crate::errors::{DeployError, Document, Result};
use crate::github::{Comments, PullRequests, WorkflowPath, WorkflowRun, WorkflowRuns};
use crate::ids::{Repository, RepositoryId};
use crate::key::{PlanKey, PlanPrefix};
use crate::manifest::Manifest;
use crate::sha::Sha;

/// The only ref a reviewed plan is applied for.
const MAIN_REF: &str = "refs/heads/main";

/// What the apply job knows about the commit it runs for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveFacts {
    /// The ref the workflow runs for: `GITHUB_REF`.
    pub git_ref: String,
    /// The commit checked out: `git rev-parse HEAD`.
    pub head: Sha,
    /// The commit the workflow runs for: `GITHUB_SHA`.
    pub sha: Sha,
    /// The parents of the checked-out commit, in order.
    pub parents: Vec<Sha>,
    /// The tree of the checked-out commit.
    pub tree_sha: Sha,
    /// The repository: `GITHUB_REPOSITORY`.
    pub repository: Repository,
    /// The repository's numeric id: `GITHUB_REPOSITORY_ID`.
    pub repository_id: RepositoryId,
    /// The key prefix of the plan store.
    pub prefix: PlanPrefix,
    /// The ref of the running workflow: `GITHUB_WORKFLOW_REF`.
    pub workflow_ref: String,
}

/// What GitHub says about the merge: one JSON object with the members `pulls`, `comments`, `run`
/// and `runs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveEvidence {
    /// The pull requests of the main commit.
    pub pulls: PullRequests,
    /// The comments of the merged pull request.
    pub comments: Comments,
    /// The workflow run the plan comment's manifest names.
    pub run: WorkflowRun,
    /// The pull-request runs of the workflow for the pull request head.
    pub runs: WorkflowRuns,
}

impl FromStr for ResolveEvidence {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        serde_json::from_str(text).map_err(|source| DeployError::Json {
            document: Document::Evidence,
            source,
        })
    }
}

/// Checks that the manifest records `expected` under `key`.
fn expect<T: PartialEq + Display>(key: &'static str, expected: &T, found: &T) -> Result<()> {
    if expected == found {
        Ok(())
    } else {
        Err(DeployError::ManifestMismatch {
            key,
            expected: expected.to_string(),
            found: found.to_string(),
        })
    }
}

/// The manifest of the reviewed plan of the main commit `facts` describes, checked against what
/// GitHub says about it in `evidence`, in the order the checks are listed:
///
/// 1. the workflow runs for main, on the commit it was started for, a squash merge;
/// 2. exactly one merged pull request of this repository has that commit as its merge commit;
/// 3. the pull request has exactly one plan comment, carrying a manifest for this repository,
///    pull request, head, base and tree;
/// 4. the run the manifest names is the completed, successful pull-request run of this workflow;
/// 5. it is still the newest plan run of the head, and the manifest's plan key is the key of its
///    plan.
///
/// # Errors
///
/// The first check that fails.
pub fn resolve(facts: &ResolveFacts, evidence: &ResolveEvidence) -> Result<Manifest> {
    if facts.git_ref != MAIN_REF {
        return Err(DeployError::NotMain {
            git_ref: facts.git_ref.clone(),
        });
    }
    if facts.head != facts.sha {
        return Err(DeployError::ResolveHead);
    }
    let [base_sha] = facts.parents.as_slice() else {
        return Err(DeployError::MainParents {
            found: facts.parents.len(),
        });
    };

    let pull = evidence.pulls.merged(&facts.sha, &facts.repository)?;
    let manifest = evidence.comments.manifest()?;
    let recorded = &manifest.facts;
    expect("repository", &facts.repository, &recorded.repository)?;
    expect(
        "repository_id",
        &facts.repository_id,
        &recorded.repository_id,
    )?;
    expect("pr_number", &pull.number(), &recorded.pr_number)?;
    expect("head_sha", pull.head_sha(), &recorded.head_sha)?;
    expect("base_sha", base_sha, &recorded.base_sha)?;
    expect("tree_sha", &facts.tree_sha, &recorded.tree_sha)?;

    let workflow = WorkflowPath::from_ref(&facts.workflow_ref)?;
    evidence
        .run
        .check(&manifest, facts.repository_id, &workflow)?;

    let head = pull.head_sha();
    let latest = evidence
        .runs
        .latest(head, &facts.repository)
        .ok_or_else(|| DeployError::NoPlanRun { head: head.clone() })?;
    if latest.id() != recorded.workflow_run_id
        || latest.run_attempt() != recorded.workflow_run_attempt
        || !latest.succeeded()
    {
        return Err(DeployError::NewerPlanRun { head: head.clone() });
    }
    let key = PlanKey::new(
        &facts.prefix,
        facts.repository_id,
        pull.number(),
        head,
        &facts.tree_sha,
        latest.id(),
        latest.run_attempt(),
    );
    expect("plan_key", &key, &manifest.plan_key)?;
    expect("workflow_run_id", &latest.id(), &recorded.workflow_run_id)?;
    expect(
        "workflow_run_attempt",
        &latest.run_attempt(),
        &recorded.workflow_run_attempt,
    )?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{ResolveEvidence, ResolveFacts, resolve};
    use crate::github::PLAN_COMMENT_MARKER;
    use crate::github::tests::{REPOSITORY, comment, pull, run};
    use crate::key::PlanKey;
    use crate::manifest::Manifest;
    use crate::manifest::tests::{BASE, HEAD, TREE, manifest_json};
    use crate::sha::Sha;

    /// The squash-merge commit on main.
    const MAIN: &str = "6666666666666666666666666666666666666666";
    const OTHER: &str = "7777777777777777777777777777777777777777";

    fn sha(text: &str) -> Sha {
        text.parse().unwrap()
    }

    /// The main commit `MAIN`, a squash merge on `BASE` with tree `TREE`, in repository 42.
    fn facts() -> ResolveFacts {
        ResolveFacts {
            git_ref: "refs/heads/main".to_owned(),
            head: sha(MAIN),
            sha: sha(MAIN),
            parents: vec![sha(BASE)],
            tree_sha: sha(TREE),
            repository: REPOSITORY.parse().unwrap(),
            repository_id: "42".parse().expect("positive repository id"),
            prefix: "plans".parse().unwrap(),
            workflow_ref: format!("{REPOSITORY}/.github/workflows/infra.yml@refs/heads/main"),
        }
    }

    /// The key of the plan of run 99 attempt 2 for pull request `pr` under `prefix`.
    fn key(prefix: &str, pr: u64, run: u64) -> String {
        PlanKey::new(
            &prefix.parse().unwrap(),
            "42".parse().expect("positive repository id"),
            pr.to_string()
                .parse()
                .expect("positive pull request number"),
            &sha(HEAD),
            &sha(TREE),
            run.to_string().parse().expect("positive run id"),
            "2".parse().expect("positive run attempt"),
        )
        .to_string()
    }

    /// The manifest of pull request 7 at head `HEAD`, planned by run 99 attempt 2.
    fn manifest() -> Value {
        let mut json = manifest_json();
        json["merge_sha"] = json!(OTHER);
        json
    }

    /// The plan run 99 attempt 2 of `HEAD`, started at `started` with `conclusion`.
    fn plan_run(id: u64, started: &str, conclusion: &str) -> Value {
        let mut json = run(id, started, HEAD, "pull_request", conclusion, REPOSITORY);
        json["run_attempt"] = json!(2);
        json
    }

    /// The evidence of the merge of pull request 7 with plan comment `manifest`, plan run `run`
    /// and workflow runs `runs`.
    fn evidence(manifest: &Value, run: &Value, runs: &[Value]) -> ResolveEvidence {
        let manifest: Manifest = manifest.to_string().parse().unwrap();
        let body = format!("{PLAN_COMMENT_MARKER}\n\n{}\n", manifest.comment_line());
        json!({
            "pulls": [pull(7, true, MAIN, REPOSITORY, HEAD)],
            "comments": [comment(1, "github-actions[bot]", "Bot", &body)],
            "run": run,
            "runs": {"workflow_runs": runs},
        })
        .to_string()
        .parse()
        .unwrap()
    }

    fn happy() -> ResolveEvidence {
        let run = plan_run(99, "2026-01-02T00:00:00Z", "success");
        evidence(&manifest(), &run, std::slice::from_ref(&run))
    }

    fn rejection(facts: &ResolveFacts, evidence: &ResolveEvidence) -> String {
        resolve(facts, evidence).unwrap_err().to_string()
    }

    fn with(key: &str, value: Value) -> Value {
        let mut json = manifest();
        json[key] = value;
        json
    }

    fn rejection_of_manifest(manifest: &Value) -> String {
        let run = plan_run(99, "2026-01-02T00:00:00Z", "success");
        rejection(
            &facts(),
            &evidence(manifest, &run, std::slice::from_ref(&run)),
        )
    }

    #[test]
    fn resolve_accepts_the_reviewed_plan_of_the_merge_commit() {
        let resolved = resolve(&facts(), &happy()).unwrap();
        assert_eq!(resolved, manifest().to_string().parse().unwrap());
        assert_eq!(resolved.plan_key.as_str(), key("plans", 7, 99));
    }

    #[test]
    fn resolve_rejects_a_ref_other_than_main() {
        let facts = ResolveFacts {
            git_ref: "refs/heads/feature".to_owned(),
            ..facts()
        };
        assert_eq!(
            rejection(&facts, &happy()),
            "resolve runs only for refs/heads/main, got refs/heads/feature"
        );
    }

    #[test]
    fn resolve_rejects_a_head_that_is_not_github_sha() {
        let facts = ResolveFacts {
            head: sha(OTHER),
            ..facts()
        };
        assert_eq!(rejection(&facts, &happy()), "HEAD is not GITHUB_SHA");
    }

    #[test]
    fn resolve_rejects_a_head_with_two_parents() {
        let facts = ResolveFacts {
            parents: vec![sha(BASE), sha(OTHER)],
            ..facts()
        };
        assert_eq!(
            rejection(&facts, &happy()),
            "main commit must have exactly one parent (squash merge); found 2"
        );
    }

    #[test]
    fn resolve_rejects_a_commit_that_no_pull_request_merged() {
        let facts = ResolveFacts {
            head: sha(OTHER),
            sha: sha(OTHER),
            ..facts()
        };
        assert_eq!(
            rejection(&facts, &happy()),
            format!("no single merged same-repository pull request has merge commit {OTHER}")
        );
    }

    #[test]
    fn resolve_rejects_a_manifest_whose_tree_differs() {
        assert_eq!(
            rejection_of_manifest(&with("tree_sha", json!(OTHER))),
            format!("manifest tree_sha mismatch (expected {TREE}, found {OTHER})")
        );
    }

    #[test]
    fn resolve_rejects_a_manifest_whose_base_differs() {
        assert_eq!(
            rejection_of_manifest(&with("base_sha", json!(OTHER))),
            format!("manifest base_sha mismatch (expected {BASE}, found {OTHER})")
        );
    }

    #[test]
    fn resolve_rejects_a_manifest_whose_head_differs() {
        assert_eq!(
            rejection_of_manifest(&with("head_sha", json!(OTHER))),
            format!("manifest head_sha mismatch (expected {HEAD}, found {OTHER})")
        );
    }

    #[test]
    fn resolve_rejects_a_manifest_for_another_repository_or_pull_request() {
        assert_eq!(
            rejection_of_manifest(&with("repository", json!("example-org/other"))),
            "manifest repository mismatch (expected example-org/gang, found example-org/other)"
        );
        assert_eq!(
            rejection_of_manifest(&with("repository_id", json!(43))),
            "manifest repository_id mismatch (expected 42, found 43)"
        );
        assert_eq!(
            rejection_of_manifest(&with("pr_number", json!(8))),
            "manifest pr_number mismatch (expected 7, found 8)"
        );
    }

    #[test]
    fn resolve_rejects_a_plan_run_that_is_not_successful() {
        let failed = plan_run(99, "2026-01-02T00:00:00Z", "failure");
        assert_eq!(
            rejection(
                &facts(),
                &evidence(&manifest(), &failed, std::slice::from_ref(&failed))
            ),
            "plan run conclusion mismatch (expected success, found failure)"
        );
    }

    #[test]
    fn resolve_rejects_when_a_newer_plan_run_exists() {
        let run = plan_run(99, "2026-01-02T00:00:00Z", "success");
        let newer = plan_run(100, "2026-01-03T00:00:00Z", "failure");
        assert_eq!(
            rejection(
                &facts(),
                &evidence(&manifest(), &run, &[run.clone(), newer])
            ),
            format!(
                "a newer plan run exists for {HEAD}; re-run all jobs of the pull request workflow \
                 and merge again"
            )
        );
    }

    #[test]
    fn resolve_rejects_when_no_plan_run_is_listed() {
        let run = plan_run(99, "2026-01-02T00:00:00Z", "success");
        assert_eq!(
            rejection(&facts(), &evidence(&manifest(), &run, &[])),
            format!("no plan run found for {HEAD}")
        );
    }

    #[test]
    fn resolve_rejects_a_plan_key_naming_another_run() {
        let manifest = with("plan_key", json!(key("plans", 7, 98)));
        assert_eq!(
            rejection_of_manifest(&manifest),
            format!(
                "manifest plan_key mismatch (expected {}, found {})",
                key("plans", 7, 99),
                key("plans", 7, 98)
            )
        );
    }

    #[test]
    fn resolve_rejects_a_plan_key_under_another_prefix() {
        let manifest = with("plan_key", json!(key("other", 7, 99)));
        assert!(rejection_of_manifest(&manifest).starts_with("manifest plan_key mismatch"));
    }

    #[test]
    fn resolve_rejects_a_plan_key_for_another_pull_request() {
        let manifest = with("plan_key", json!(key("plans", 8, 99)));
        assert!(rejection_of_manifest(&manifest).starts_with("manifest plan_key mismatch"));
    }

    #[test]
    fn resolve_rejects_a_workflow_ref_outside_github_workflows() {
        let facts = ResolveFacts {
            workflow_ref: format!("{REPOSITORY}/infra.yml@refs/heads/main"),
            ..facts()
        };
        assert!(rejection(&facts, &happy()).starts_with("workflow ref has no"));
    }

    #[test]
    fn resolve_evidence_rejects_unknown_members() {
        let error = r#"{"pulls": [], "comments": [], "run": {}, "runs": {}, "extra": 1}"#
            .parse::<ResolveEvidence>()
            .unwrap_err();
        assert_eq!(error.to_string(), "cannot read merge evidence JSON");
    }
}
