//! The GitHub API documents the deploy decisions read: the pull requests of a commit, the
//! comments of a pull request, and workflow runs; and the workflow file a workflow ref names.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

use crate::errors::{DeployError, Document, Result};
use crate::ids::{CommentId, PrNumber, Repository, RepositoryId, RunAttempt, RunId};
use crate::manifest::Manifest;
use crate::sha::Sha;

/// The hidden first line that marks the plan comment of a pull request.
pub const PLAN_COMMENT_MARKER: &str = "<!-- gang-deploy-plan -->";
/// The login that posts the plan comment.
pub const PLAN_COMMENT_AUTHOR: &str = "github-actions[bot]";
/// What to do when the reviewed plan is stale.
pub const RERUN_REMEDY: &str = "re-run all jobs of the pull request workflow and merge again";
/// The event of a plan run.
const PULL_REQUEST_EVENT: &str = "pull_request";
/// The directory of workflow files.
const WORKFLOWS_DIRECTORY: &str = ".github/workflows/";

/// Parses the GitHub API document `text` of kind `document`.
fn parse<T: for<'de> Deserialize<'de>>(text: &str, document: Document) -> Result<T> {
    serde_json::from_str(text).map_err(|source| DeployError::Json { document, source })
}

/// A repository as a pull request head or workflow run names it.
#[derive(Debug, Clone, Deserialize)]
struct RepositoryName {
    /// `owner/name`.
    full_name: String,
}

/// A repository as a workflow run names it.
#[derive(Debug, Clone, Deserialize)]
struct RepositoryNumber {
    /// The numeric id.
    id: RepositoryId,
}

/// Reports whether `repository` is the repository `name`; a deleted repository is none.
fn is_repository(name: Option<&RepositoryName>, repository: &Repository) -> bool {
    name.is_some_and(|name| name.full_name == repository.as_str())
}

/// A pull request, as `GET /repos/{owner}/{repo}/commits/{sha}/pulls` lists it.
#[derive(Debug, Clone, Deserialize)]
pub struct PullRequest {
    /// The pull request number.
    number: PrNumber,
    /// When it was merged; `null` when it was not.
    merged_at: Option<String>,
    /// The commit it was merged as.
    merge_commit_sha: Option<String>,
    /// Its head.
    head: PullRequestHead,
}

/// The head of a pull request.
#[derive(Debug, Clone, Deserialize)]
struct PullRequestHead {
    /// The head commit.
    sha: Sha,
    /// The repository the head lives in; `null` once that repository is deleted.
    repo: Option<RepositoryName>,
}

impl PullRequest {
    /// The pull request number.
    #[must_use]
    pub fn number(&self) -> PrNumber {
        self.number
    }

    /// The head commit.
    #[must_use]
    pub fn head_sha(&self) -> &Sha {
        &self.head.sha
    }
}

/// The pull requests of a commit.
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct PullRequests(Vec<PullRequest>);

impl PullRequests {
    /// The one merged pull request from `repository` itself whose merge commit is `sha`.
    ///
    /// # Errors
    ///
    /// [`DeployError::NoMergedPullRequest`] when there is none, or more than one.
    pub fn merged(&self, sha: &Sha, repository: &Repository) -> Result<&PullRequest> {
        let mut matching = self.0.iter().filter(|pull| {
            pull.merged_at.is_some()
                && pull.merge_commit_sha.as_deref() == Some(sha.as_str())
                && is_repository(pull.head.repo.as_ref(), repository)
        });
        match (matching.next(), matching.next()) {
            (Some(pull), None) => Ok(pull),
            _ => Err(DeployError::NoMergedPullRequest { sha: sha.clone() }),
        }
    }
}

impl FromStr for PullRequests {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        parse(text, Document::PullRequests)
    }
}

/// The author of a comment.
#[derive(Debug, Clone, Deserialize)]
struct CommentAuthor {
    /// The login.
    login: String,
    /// `User`, `Bot` or `Organization`.
    #[serde(rename = "type")]
    kind: String,
}

/// An issue comment, as `GET /repos/{owner}/{repo}/issues/{number}/comments` lists it.
#[derive(Debug, Clone, Deserialize)]
pub struct Comment {
    /// The comment id.
    id: CommentId,
    /// The author; `null` for a deleted account.
    user: Option<CommentAuthor>,
    /// The text.
    body: Option<String>,
}

impl Comment {
    /// The text; empty when the comment has none.
    #[must_use]
    pub fn body(&self) -> &str {
        self.body.as_deref().unwrap_or_default()
    }

    /// Reports whether this is a plan comment: posted by the workflow's bot and marked.
    #[must_use]
    pub fn is_plan_comment(&self) -> bool {
        self.user
            .as_ref()
            .is_some_and(|user| user.kind == "Bot" && user.login == PLAN_COMMENT_AUTHOR)
            && self.body().contains(PLAN_COMMENT_MARKER)
    }
}

/// The comments of a pull request, every page joined.
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct Comments(Vec<Comment>);

impl Comments {
    /// The id of the first plan comment: the one a new plan replaces.
    #[must_use]
    pub fn plan_comment_id(&self) -> Option<CommentId> {
        self.0
            .iter()
            .find(|comment| comment.is_plan_comment())
            .map(|comment| comment.id)
    }

    /// The body of the one plan comment.
    ///
    /// # Errors
    ///
    /// [`DeployError::PlanComments`] when there is none, or more than one.
    pub fn plan_comment_body(&self) -> Result<&str> {
        let plan_comments: Vec<&Comment> = self
            .0
            .iter()
            .filter(|comment| comment.is_plan_comment())
            .collect();
        match plan_comments.as_slice() {
            [comment] => Ok(comment.body()),
            _ => Err(DeployError::PlanComments {
                found: plan_comments.len(),
            }),
        }
    }

    /// The manifest the one plan comment carries.
    ///
    /// # Errors
    ///
    /// When there is not exactly one plan comment, or it carries no valid manifest.
    pub fn manifest(&self) -> Result<Manifest> {
        Manifest::from_comment(self.plan_comment_body()?)
    }
}

impl FromStr for Comments {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        parse(text, Document::Comments)
    }
}

/// A workflow run, as `GET /repos/{owner}/{repo}/actions/runs/{id}` returns it.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowRun {
    /// The run id.
    id: RunId,
    /// The attempt.
    run_attempt: RunAttempt,
    /// When the attempt started.
    #[serde(default)]
    run_started_at: Option<String>,
    /// When the run was created.
    #[serde(default)]
    created_at: Option<String>,
    /// The commit the run is for.
    head_sha: String,
    /// The event that started the run.
    event: String,
    /// `queued`, `in_progress`, `completed`, ...
    #[serde(default)]
    status: Option<String>,
    /// `success`, `failure`, ...; `null` until completed.
    #[serde(default)]
    conclusion: Option<String>,
    /// The workflow file.
    path: String,
    /// The repository of the head; `null` once a fork is deleted.
    #[serde(default)]
    head_repository: Option<RepositoryName>,
    /// The repository the run belongs to.
    repository: RepositoryNumber,
}

/// Checks that `found` is `expected`, naming `field` of the plan run otherwise.
fn expect_run_field(field: &'static str, expected: &str, found: &str) -> Result<()> {
    if expected == found {
        Ok(())
    } else {
        Err(DeployError::RunMismatch {
            field,
            expected: expected.to_owned(),
            found: found.to_owned(),
        })
    }
}

impl WorkflowRun {
    /// The run id.
    #[must_use]
    pub fn id(&self) -> RunId {
        self.id
    }

    /// The attempt.
    #[must_use]
    pub fn run_attempt(&self) -> RunAttempt {
        self.run_attempt
    }

    /// Reports whether the run concluded successfully.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.conclusion.as_deref() == Some("success")
    }

    /// Checks that this is the completed, successful pull-request run of `workflow` in
    /// repository `repository_id` that made the plan the manifest records, field by field.
    ///
    /// # Errors
    ///
    /// [`DeployError::RunMismatch`] naming the first field that differs, or
    /// [`DeployError::RunAttemptMismatch`] when only another attempt of the run made the plan.
    pub fn check(
        &self,
        manifest: &Manifest,
        repository_id: RepositoryId,
        workflow: &WorkflowPath,
    ) -> Result<()> {
        let facts = &manifest.facts;
        expect_run_field(
            "id",
            &facts.workflow_run_id.to_string(),
            &self.id.to_string(),
        )?;
        expect_run_field("event", PULL_REQUEST_EVENT, &self.event)?;
        expect_run_field(
            "status",
            "completed",
            self.status.as_deref().unwrap_or_default(),
        )?;
        expect_run_field(
            "conclusion",
            "success",
            self.conclusion.as_deref().unwrap_or_default(),
        )?;
        expect_run_field("head_sha", facts.head_sha.as_str(), &self.head_sha)?;
        if self.run_attempt != facts.workflow_run_attempt {
            return Err(DeployError::RunAttemptMismatch {
                expected: facts.workflow_run_attempt.to_string(),
                found: self.run_attempt.to_string(),
            });
        }
        expect_run_field("path", workflow.as_str(), &self.path)?;
        expect_run_field(
            "repository.id",
            &repository_id.to_string(),
            &self.repository.id.to_string(),
        )
    }
}

/// The runs of a workflow, every page joined: `{"workflow_runs": [...]}`.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowRuns {
    /// The runs.
    workflow_runs: Vec<WorkflowRun>,
}

impl FromStr for WorkflowRuns {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        parse(text, Document::WorkflowRuns)
    }
}

impl WorkflowRuns {
    /// The newest pull-request run for `head` from `repository` itself: the latest start (or
    /// creation) time, then the highest id.
    #[must_use]
    pub fn latest(&self, head: &Sha, repository: &Repository) -> Option<&WorkflowRun> {
        self.workflow_runs
            .iter()
            .filter(|run| {
                run.event == PULL_REQUEST_EVENT
                    && run.head_sha == head.as_str()
                    && is_repository(run.head_repository.as_ref(), repository)
            })
            .max_by(|left, right| left.recency().cmp(&right.recency()))
    }
}

impl WorkflowRun {
    /// The order of runs from oldest to newest: start time (creation time before the run
    /// starts), then id. Times compare as text, which orders GitHub's ISO 8601 times.
    fn recency(&self) -> (Option<&str>, RunId) {
        let time = self
            .run_started_at
            .as_deref()
            .or(self.created_at.as_deref());
        (time, self.id)
    }
}

/// The workflow file of a workflow run: `.github/workflows/<name>.yml` or `.yaml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowPath(String);

impl WorkflowPath {
    /// The workflow file a workflow ref such as
    /// `owner/repo/.github/workflows/<name>.yml@refs/heads/main` names.
    ///
    /// # Errors
    ///
    /// [`DeployError::WorkflowRef`] when the ref names no file in `.github/workflows`.
    pub fn from_ref(workflow_ref: &str) -> Result<Self> {
        let path = workflow_ref
            .rsplit_once('@')
            .map_or(workflow_ref, |(path, _)| path);
        let path = path
            .split_once('/')
            .and_then(|(_, rest)| rest.split_once('/'))
            .map_or(path, |(_, rest)| rest);
        let valid = path
            .strip_prefix(WORKFLOWS_DIRECTORY)
            .and_then(|name| {
                name.strip_suffix(".yml")
                    .or_else(|| name.strip_suffix(".yaml"))
            })
            .is_some_and(|stem| {
                !stem.is_empty()
                    && stem.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                    })
            });
        if valid {
            Ok(Self(path.to_owned()))
        } else {
            Err(DeployError::WorkflowRef {
                workflow_ref: workflow_ref.to_owned(),
            })
        }
    }

    /// The path of the workflow file.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name of the workflow file, without its directory.
    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.0[WORKFLOWS_DIRECTORY.len()..]
    }
}

impl fmt::Display for WorkflowPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::{Value, json};

    use super::{
        CommentId, Comments, PLAN_COMMENT_MARKER, PullRequests, WorkflowPath, WorkflowRun,
        WorkflowRuns,
    };
    use crate::manifest::tests::{HEAD, manifest};
    use crate::sha::Sha;

    pub(crate) const REPOSITORY: &str = "example-org/gang";
    pub(crate) const FORK: &str = "example-fork/gang";
    pub(crate) const MERGE_COMMIT: &str = "6666666666666666666666666666666666666666";
    const OTHER: &str = "7777777777777777777777777777777777777777";

    /// A pull request as the commit pulls API lists it.
    pub(crate) fn pull(
        number: u64,
        merged: bool,
        merge_sha: &str,
        repo: &str,
        head: &str,
    ) -> Value {
        let merged_at = if merged {
            json!("2026-01-02T00:00:00Z")
        } else {
            Value::Null
        };
        json!({
            "number": number,
            "merged_at": merged_at,
            "merge_commit_sha": merge_sha,
            "head": {"sha": head, "repo": {"full_name": repo}},
            "state": "closed",
        })
    }

    /// A comment by `login` of account type `kind`.
    pub(crate) fn comment(id: u64, login: &str, kind: &str, body: &str) -> Value {
        json!({"id": id, "user": {"login": login, "type": kind}, "body": body})
    }

    /// A completed workflow run of `.github/workflows/infra.yml` in repository 42.
    pub(crate) fn run(
        id: u64,
        started: &str,
        head: &str,
        event: &str,
        conclusion: &str,
        repo: &str,
    ) -> Value {
        json!({
            "id": id,
            "run_attempt": 1,
            "run_started_at": started,
            "created_at": started,
            "head_sha": head,
            "event": event,
            "status": "completed",
            "conclusion": conclusion,
            "path": ".github/workflows/infra.yml",
            "head_repository": {"full_name": repo},
            "repository": {"id": 42},
        })
    }

    fn sha(text: &str) -> Sha {
        text.parse().unwrap()
    }

    fn merged(pulls: &[Value]) -> Option<(u64, String)> {
        let pulls: PullRequests = Value::from(pulls.to_vec()).to_string().parse().unwrap();
        pulls
            .merged(&sha(MERGE_COMMIT), &REPOSITORY.parse().unwrap())
            .ok()
            .map(|pull| (pull.number().get(), pull.head_sha().to_string()))
    }

    fn latest(runs: &[Value]) -> Option<(u64, u64, bool)> {
        let runs: WorkflowRuns = json!({"workflow_runs": runs}).to_string().parse().unwrap();
        runs.latest(&sha(HEAD), &REPOSITORY.parse().unwrap())
            .map(|run| (run.id().get(), run.run_attempt().get(), run.succeeded()))
    }

    fn comments(comments: &[Value]) -> Comments {
        Value::from(comments.to_vec()).to_string().parse().unwrap()
    }

    fn head_of(number: u64) -> String {
        number.to_string().repeat(40)[..40].to_owned()
    }

    #[test]
    fn merged_pr_picks_the_merged_same_repository_pull_request() {
        let merged_ok = pull(7, true, MERGE_COMMIT, REPOSITORY, &head_of(7));
        assert_eq!(merged(&[merged_ok]), Some((7, head_of(7))));
    }

    #[test]
    fn merged_pr_ignores_an_unmerged_pull_request() {
        let unmerged = pull(8, false, MERGE_COMMIT, REPOSITORY, &head_of(8));
        let merged_ok = pull(7, true, MERGE_COMMIT, REPOSITORY, &head_of(7));
        assert_eq!(merged(&[unmerged, merged_ok]), Some((7, head_of(7))));
    }

    #[test]
    fn merged_pr_ignores_a_fork_pull_request_with_the_same_merge_commit() {
        let fork = pull(1, true, MERGE_COMMIT, FORK, &head_of(1));
        let merged_ok = pull(7, true, MERGE_COMMIT, REPOSITORY, &head_of(7));
        assert_eq!(merged(&[fork, merged_ok]), Some((7, head_of(7))));
    }

    #[test]
    fn merged_pr_ignores_a_pull_request_whose_head_repository_is_deleted() {
        let mut deleted = pull(1, true, MERGE_COMMIT, REPOSITORY, &head_of(1));
        deleted["head"]["repo"] = Value::Null;
        assert_eq!(merged(&[deleted]), None);
    }

    #[test]
    fn merged_pr_rejects_a_pull_request_with_another_merge_commit() {
        assert_eq!(
            merged(&[pull(9, true, OTHER, REPOSITORY, &head_of(9))]),
            None
        );
    }

    #[test]
    fn merged_pr_rejects_two_matching_pull_requests() {
        let merged_ok = pull(7, true, MERGE_COMMIT, REPOSITORY, &head_of(7));
        let second = pull(5, true, MERGE_COMMIT, REPOSITORY, &head_of(5));
        assert_eq!(merged(&[merged_ok, second]), None);
    }

    #[test]
    fn merged_pr_rejects_when_only_an_unmerged_pull_request_matches() {
        assert_eq!(
            merged(&[pull(8, false, MERGE_COMMIT, REPOSITORY, &head_of(8))]),
            None
        );
    }

    #[test]
    fn merged_pr_names_the_merge_commit_it_found_no_pull_request_for() {
        let pulls: PullRequests = "[]".parse().unwrap();
        let error = pulls
            .merged(&sha(MERGE_COMMIT), &REPOSITORY.parse().unwrap())
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "no single merged same-repository pull request has merge commit {MERGE_COMMIT}"
            )
        );
    }

    #[test]
    fn latest_plan_run_picks_the_newest_run_by_start_time_not_the_highest_id() {
        let older = run(
            200,
            "2026-01-01T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        let newest = run(
            100,
            "2026-01-02T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        assert_eq!(latest(&[older, newest]), Some((100, 1, true)));
    }

    #[test]
    fn latest_plan_run_ignores_runs_for_another_head_event_or_repository() {
        let older = run(
            200,
            "2026-01-01T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        let newest = run(
            100,
            "2026-01-02T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        let other_head = run(
            300,
            "2026-01-03T00:00:00Z",
            OTHER,
            "pull_request",
            "success",
            REPOSITORY,
        );
        let push = run(
            301,
            "2026-01-03T00:00:00Z",
            HEAD,
            "push",
            "success",
            REPOSITORY,
        );
        let fork = run(
            302,
            "2026-01-03T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            FORK,
        );
        assert_eq!(
            latest(&[older, newest, other_head, push, fork]),
            Some((100, 1, true))
        );
    }

    #[test]
    fn latest_plan_run_picks_the_highest_id_when_start_times_are_equal() {
        let high = run(
            401,
            "2026-01-04T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        let low = run(
            400,
            "2026-01-04T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        assert_eq!(latest(&[high, low]), Some((401, 1, true)));
    }

    #[test]
    fn latest_plan_run_falls_back_to_the_creation_time() {
        let mut queued = run(
            500,
            "2026-01-05T00:00:00Z",
            HEAD,
            "pull_request",
            "",
            REPOSITORY,
        );
        queued["run_started_at"] = Value::Null;
        let older = run(
            600,
            "2026-01-01T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        assert_eq!(latest(&[queued, older]), Some((500, 1, false)));
    }

    #[test]
    fn latest_plan_run_reports_the_conclusion_of_the_newest_run() {
        let older = run(
            200,
            "2026-01-01T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        let failed = run(
            500,
            "2026-01-05T00:00:00Z",
            HEAD,
            "pull_request",
            "failure",
            REPOSITORY,
        );
        assert_eq!(latest(&[older, failed]), Some((500, 1, false)));
    }

    #[test]
    fn latest_plan_run_fails_when_no_run_matches() {
        let push = run(
            301,
            "2026-01-03T00:00:00Z",
            HEAD,
            "push",
            "success",
            REPOSITORY,
        );
        assert_eq!(latest(&[push]), None);
    }

    fn plan_body() -> String {
        format!("{PLAN_COMMENT_MARKER}\n## Infrastructure plan")
    }

    #[test]
    fn plan_comment_body_picks_the_single_marked_comment_of_github_actions_bot() {
        let comments = comments(&[
            comment(1, "github-actions[bot]", "Bot", "unrelated comment"),
            comment(2, "github-actions[bot]", "Bot", &plan_body()),
            comment(3, "reviewer", "User", "looks good"),
        ]);
        assert_eq!(comments.plan_comment_body().unwrap(), plan_body());
        assert_eq!(comments.plan_comment_id().map(CommentId::get), Some(2));
    }

    #[test]
    fn plan_comment_body_rejects_a_marked_comment_from_a_user() {
        let comments = comments(&[comment(1, "reviewer", "User", &plan_body())]);
        assert_eq!(
            comments.plan_comment_body().unwrap_err().to_string(),
            "expected one plan comment from github-actions[bot], found 0"
        );
        assert_eq!(comments.plan_comment_id(), None);
    }

    #[test]
    fn plan_comment_body_rejects_a_marked_comment_from_another_bot() {
        let comments = comments(&[comment(1, "other[bot]", "Bot", &plan_body())]);
        assert!(comments.plan_comment_body().is_err());
        assert_eq!(comments.plan_comment_id(), None);
    }

    #[test]
    fn plan_comment_body_rejects_when_no_comment_is_marked() {
        let mut deleted = comment(3, "x", "User", "");
        deleted["user"] = Value::Null;
        deleted["body"] = Value::Null;
        let comments = comments(&[
            comment(1, "github-actions[bot]", "Bot", "unrelated comment"),
            comment(2, "reviewer", "User", "looks good"),
            deleted,
        ]);
        assert!(comments.plan_comment_body().is_err());
        assert_eq!(comments.plan_comment_id(), None);
    }

    #[test]
    fn plan_comment_body_rejects_two_marked_bot_comments() {
        let comments = comments(&[
            comment(1, "github-actions[bot]", "Bot", &plan_body()),
            comment(2, "github-actions[bot]", "Bot", &plan_body()),
        ]);
        assert_eq!(
            comments.plan_comment_body().unwrap_err().to_string(),
            "expected one plan comment from github-actions[bot], found 2"
        );
    }

    #[test]
    fn plan_comment_id_is_the_first_plan_comment() {
        let comments = comments(&[
            comment(5, "github-actions[bot]", "Bot", &plan_body()),
            comment(9, "github-actions[bot]", "Bot", &plan_body()),
        ]);
        assert_eq!(comments.plan_comment_id().map(CommentId::get), Some(5));
    }

    #[test]
    fn workflow_path_extracts_the_workflow_file_from_the_workflow_ref() {
        let path =
            WorkflowPath::from_ref("example-org/gang/.github/workflows/infra.yml@refs/heads/main")
                .unwrap();
        assert_eq!(path.as_str(), ".github/workflows/infra.yml");
        assert_eq!(path.file_name(), "infra.yml");
    }

    #[test]
    fn workflow_path_accepts_a_yaml_workflow_file() {
        let path =
            WorkflowPath::from_ref("example-org/gang/.github/workflows/infra.yaml@refs/heads/main")
                .unwrap();
        assert_eq!(path.as_str(), ".github/workflows/infra.yaml");
        assert_eq!(path.file_name(), "infra.yaml");
    }

    #[test]
    fn workflow_path_rejects_a_ref_outside_github_workflows() {
        for workflow_ref in [
            "example-org/gang/infra.yml@refs/heads/main",
            "example-org/gang/.github/actions/infra.yml@refs/heads/main",
            "example-org/gang/.github/workflows/.yml@refs/heads/main",
            "example-org/gang/.github/workflows/in fra.yml@refs/heads/main",
            "example-org/gang/.github/workflows/sub/infra.yml@refs/heads/main",
        ] {
            let error = WorkflowPath::from_ref(workflow_ref).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!(
                    "workflow ref has no .github/workflows/<name>.yml or .yaml path: {workflow_ref}"
                )
            );
        }
    }

    /// A mismatch case of the plan run: the field, the change that breaks it, and the message.
    type RunCase = (&'static str, fn(&mut Value), &'static str);

    /// Checks `run` against the manifest's run, as `check_plan_run` did.
    fn run_rejection(change: impl FnOnce(&mut Value)) -> Option<String> {
        let mut json = run(
            99,
            "2026-01-02T00:00:00Z",
            HEAD,
            "pull_request",
            "success",
            REPOSITORY,
        );
        json["run_attempt"] = json!(2);
        change(&mut json);
        let run: WorkflowRun = serde_json::from_value(json).unwrap();
        let workflow =
            WorkflowPath::from_ref("example-org/gang/.github/workflows/infra.yml@refs/heads/main")
                .unwrap();
        run.check(
            &manifest(),
            "42".parse().expect("positive repository id"),
            &workflow,
        )
        .err()
        .as_ref()
        .map(ToString::to_string)
    }

    #[test]
    fn check_plan_run_accepts_the_successful_run_of_the_manifest() {
        assert_eq!(run_rejection(|_| {}), None);
    }

    #[test]
    fn check_plan_run_names_each_mismatching_field() {
        let cases: [RunCase; 7] = [
            (
                "id",
                |run| run["id"] = json!(98),
                "plan run id mismatch (expected 99, found 98)",
            ),
            (
                "event",
                |run| run["event"] = json!("push"),
                "plan run event mismatch (expected pull_request, found push)",
            ),
            (
                "status",
                |run| run["status"] = json!("in_progress"),
                "plan run status mismatch (expected completed, found in_progress)",
            ),
            (
                "conclusion",
                |run| run["conclusion"] = Value::Null,
                "plan run conclusion mismatch (expected success, found )",
            ),
            (
                "path",
                |run| run["path"] = json!(".github/workflows/other.yml"),
                "plan run path mismatch (expected .github/workflows/infra.yml, found \
                 .github/workflows/other.yml)",
            ),
            (
                "repository.id",
                |run| run["repository"]["id"] = json!(43),
                "plan run repository.id mismatch (expected 42, found 43)",
            ),
            (
                "conclusion",
                |run| run["conclusion"] = json!("failure"),
                "plan run conclusion mismatch (expected success, found failure)",
            ),
        ];
        for (field, change, message) in cases {
            assert_eq!(run_rejection(change).as_deref(), Some(message), "{field}");
        }
    }

    #[test]
    fn check_plan_run_names_a_run_head_sha_mismatch() {
        assert_eq!(
            run_rejection(|run| run["head_sha"] = json!(OTHER)),
            Some(format!(
                "plan run head_sha mismatch (expected {HEAD}, found {OTHER})"
            ))
        );
    }

    #[test]
    fn check_plan_run_ends_a_run_attempt_mismatch_with_the_rerun_remedy() {
        assert_eq!(
            run_rejection(|run| run["run_attempt"] = json!(1)),
            Some(
                "plan run run_attempt mismatch (expected 2, found 1); re-run all jobs of the pull \
                 request workflow and merge again"
                    .to_owned()
            )
        );
    }
}
