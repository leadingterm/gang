//! The hidden `gang deploy` commands: the decisions of the deploy workflow. The workflow's glue
//! (`infra/scripts/reviewed-plan.sh`) runs the tools and pipes their JSON to these commands; the
//! decisions themselves live in `gang_deploy`.

use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::process::ExitCode;

use gang_deploy::{
    CommentSource, Comments, Manifest, ObjectHead, ObjectVersions, Plan, PlanComment, PlanSha256,
    PullRequests, ResolveEvidence, ResolveFacts, StoreInputs, StoreRequest, WorkflowPath, resolve,
};
use gang_os::PathIo;

use crate::cli::{
    CheckObjectArgs, CheckPlanArgs, CommentArgs, DeployCommand, ManifestArgs, ManifestFileArgs,
    MergedPrArgs, ResolveArgs, StoreRequestArgs, WorkflowFileArgs,
};
use crate::errors::{MainError, Result};
use crate::output::Output;

/// Exit code of `gang deploy policy` for a plan that must not merge.
const BLOCKED_EXIT_CODE: u8 = 3;

/// Runs one `gang deploy` command.
pub(crate) fn run(command: DeployCommand, input: impl Read, out: &Output) -> Result<ExitCode> {
    match command {
        DeployCommand::Policy => policy(input, out),
        DeployCommand::Configuration => configuration(input, std::io::stdout().lock()),
        DeployCommand::RestartNeeded => restart_needed(input, out),
        DeployCommand::StoreRequest(args) => store_request(args, input, out),
        DeployCommand::Manifest(args) => manifest(args, input, out),
        DeployCommand::Comment(args) => comment(&args, input, out),
        DeployCommand::CommentId => comment_id(input, out),
        DeployCommand::MergedPr(args) => merged_pr(&args, input, out),
        DeployCommand::PlanComment => plan_comment(input, out),
        DeployCommand::WorkflowFile(args) => workflow_file(&args, out),
        DeployCommand::Resolve(args) => resolve_plan(args, input, out),
        DeployCommand::CheckVersions(args) => check_versions(&args, input),
        DeployCommand::CheckObject(args) => check_object(&args, input),
        DeployCommand::CheckPlan(args) => check_plan(&args),
    }
}

/// Reads all of standard input.
fn stdin_text(mut input: impl Read) -> Result<String> {
    let mut text = String::new();
    input.read_to_string(&mut text).map_err(MainError::Stdin)?;
    Ok(text)
}

/// Reads and parses the document on standard input.
fn stdin_document<T: std::str::FromStr<Err = gang_deploy::DeployError>>(
    input: impl Read,
) -> Result<T> {
    Ok(stdin_text(input)?.parse()?)
}

/// Reads the manifest file `path`.
fn read_manifest(path: &Path) -> Result<Manifest> {
    let text = fs::read_to_string(path).map_err(PathIo::at("read manifest", path))?;
    Ok(text.parse()?)
}

/// Prints `has_changes=` and `blocked=` in `GITHUB_OUTPUT` form. A blocked plan lists the
/// protected changes on stderr and exits with [`BLOCKED_EXIT_CODE`].
fn policy(input: impl Read, out: &Output) -> Result<ExitCode> {
    let plan: Plan = stdin_document(input)?;
    let protected = plan.protected_changes();
    let blocked = !protected.is_empty();
    out.data(format!("has_changes={}", plan.has_changes()))?;
    out.data(format!("blocked={blocked}"))?;
    if !blocked {
        return Ok(ExitCode::SUCCESS);
    }
    out.notice("blocked: plan deletes or replaces protected resources:");
    for change in &protected {
        out.notice(change.to_string());
    }
    Ok(ExitCode::from(BLOCKED_EXIT_CODE))
}

/// Prints `true` when applying the plan needs the controller to reload its configuration.
fn restart_needed(input: impl Read, out: &Output) -> Result<ExitCode> {
    let plan: Plan = stdin_document(input)?;
    out.data(plan.restart_needed().to_string())?;
    Ok(ExitCode::SUCCESS)
}

/// Writes the planned configuration without changing its bytes.
fn configuration(input: impl Read, mut output: impl Write) -> Result<ExitCode> {
    let plan: Plan = stdin_document(input)?;
    output
        .write_all(plan.configuration()?.as_bytes())
        .map_err(MainError::Stdout)?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the store request of the saved plan file, whose `tofu show -json` is on stdin.
fn store_request(args: StoreRequestArgs, input: impl Read, out: &Output) -> Result<ExitCode> {
    let plan: Plan = stdin_document(input)?;
    let bytes = fs::read(&args.plan_file).map_err(PathIo::at("read plan", &args.plan_file))?;
    let inputs = StoreInputs {
        repository: args.repository,
        repository_id: args.repository_id,
        pr_number: args.pr_number,
        pr_head_sha: args.pr_head_sha,
        sha: args.sha,
        head: args.head,
        parents: args.parents,
        tree_sha: args.tree_sha,
        run_id: args.run_id,
        run_attempt: args.run_attempt,
        prefix: args.prefix,
        max_age: args.max_age,
        now: args.now,
    };
    let request = StoreRequest::try_new(inputs, &plan, PlanSha256::digest(&bytes))?;
    out.data(request.to_string())?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the manifest of the stored plan whose store request is on stdin.
fn manifest(args: ManifestArgs, input: impl Read, out: &Output) -> Result<ExitCode> {
    let request: StoreRequest = stdin_document(input)?;
    out.data(request.into_manifest(args.plan_version).to_string())?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the `{"body": ...}` payload of the plan comment for the plan on stdin; empty stdin
/// means the plan did not complete.
fn comment(args: &CommentArgs, input: impl Read, out: &Output) -> Result<ExitCode> {
    let text = stdin_text(input)?;
    let plan: Option<Plan> = if text.is_empty() {
        None
    } else {
        Some(text.parse()?)
    };
    let manifest;
    let source = match args {
        CommentArgs::Tree(tree) => CommentSource::Tree(tree),
        CommentArgs::Manifest(path) => {
            manifest = read_manifest(path)?;
            CommentSource::Manifest(&manifest)
        }
    };
    let body = PlanComment::new(source, plan.as_ref()).to_string();
    out.data(serde_json::json!({ "body": body }).to_string())?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the id of the plan comment among the comments on stdin; nothing when there is none.
fn comment_id(input: impl Read, out: &Output) -> Result<ExitCode> {
    let comments: Comments = stdin_document(input)?;
    if let Some(id) = comments.plan_comment_id() {
        out.data(id.to_string())?;
    }
    Ok(ExitCode::SUCCESS)
}

/// Prints `<number> <head sha>` of the pull request merged as `--sha`, from the pulls on stdin.
fn merged_pr(args: &MergedPrArgs, input: impl Read, out: &Output) -> Result<ExitCode> {
    let pulls: PullRequests = stdin_document(input)?;
    let pull = pulls.merged(&args.sha, &args.repository)?;
    out.data(format!("{} {}", pull.number(), pull.head_sha()))?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the manifest the plan comment among the comments on stdin carries.
fn plan_comment(input: impl Read, out: &Output) -> Result<ExitCode> {
    let comments: Comments = stdin_document(input)?;
    out.data(comments.manifest()?.to_string())?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the workflow file name from the running workflow's ref.
fn workflow_file(args: &WorkflowFileArgs, out: &Output) -> Result<ExitCode> {
    let workflow = WorkflowPath::from_ref(&args.workflow_ref)?;
    out.data(workflow.file_name())?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the manifest of the reviewed plan of the main commit, from the evidence on stdin.
fn resolve_plan(args: ResolveArgs, input: impl Read, out: &Output) -> Result<ExitCode> {
    let evidence: ResolveEvidence = stdin_document(input)?;
    let facts = ResolveFacts {
        git_ref: args.git_ref,
        head: args.head,
        sha: args.sha,
        parents: args.parents,
        tree_sha: args.tree_sha,
        repository: args.repository,
        repository_id: args.repository_id,
        prefix: args.prefix,
        workflow_ref: args.workflow_ref,
    };
    out.data(resolve(&facts, &evidence)?.to_string())?;
    Ok(ExitCode::SUCCESS)
}

/// Checks the object versions on stdin against the manifest.
fn check_versions(args: &ManifestFileArgs, input: impl Read) -> Result<ExitCode> {
    let versions: ObjectVersions = stdin_document(input)?;
    read_manifest(&args.manifest)?.check_versions(&versions)?;
    Ok(ExitCode::SUCCESS)
}

/// Checks the object head on stdin against the manifest, the time and the maximum age.
fn check_object(args: &CheckObjectArgs, input: impl Read) -> Result<ExitCode> {
    let head: ObjectHead = stdin_document(input)?;
    read_manifest(&args.manifest)?.check_object(&head, args.now, args.max_age)?;
    Ok(ExitCode::SUCCESS)
}

/// Checks the digest of the downloaded plan file against the manifest.
fn check_plan(args: &CheckPlanArgs) -> Result<ExitCode> {
    let manifest = read_manifest(&args.manifest)?;
    let bytes = fs::read(&args.plan_file).map_err(PathIo::at("read plan", &args.plan_file))?;
    manifest.check_digest(&PlanSha256::digest(&bytes))?;
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::time::Duration;

    use gang_deploy::{MaxAge, UnixTime};
    use serde_json::json;

    use super::*;
    use crate::cli::Command;

    #[test]
    fn configuration_writes_exact_planned_bytes() {
        for value in ["no newline", "one\n", "two\n\n"] {
            let plan = json!({"resource_changes": [{
                "address": "module.gang.aws_ssm_parameter.config_generation",
                "type": "aws_ssm_parameter",
                "change": {"actions": ["no-op"], "after": {"value": value}}
            }]})
            .to_string();
            let mut output = Vec::new();
            assert_eq!(
                configuration(plan.as_bytes(), &mut output).expect("write configuration"),
                ExitCode::SUCCESS
            );
            assert_eq!(output, value.as_bytes());
        }
    }

    #[test]
    fn configuration_does_not_write_when_plan_is_invalid() {
        let mut output = Vec::new();
        assert!(configuration(b"{}".as_slice(), &mut output).is_err());
        assert!(output.is_empty());
    }

    fn gang_deploy(args: &[&str], stdin: &str) -> (ExitCode, String, String) {
        let command_line: Vec<OsString> = ["gang", "deploy"]
            .into_iter()
            .chain(args.iter().copied())
            .map(OsString::from)
            .collect();
        let parsed = crate::dispatch::parse(&command_line).expect("valid deploy arguments");
        let Command::Deploy(command) = parsed.command else {
            panic!("expected a deploy command");
        };
        let subject = command.subject();
        let (out, stdout, stderr) = Output::captured();
        let code = run(command, stdin.as_bytes(), &out)
            .unwrap_or_else(|error| out.failure(subject, &error));
        (code, stdout.text(), stderr.text())
    }

    const HEAD: &str = "1111111111111111111111111111111111111111";
    const BASE: &str = "2222222222222222222222222222222222222222";
    const MERGE: &str = "3333333333333333333333333333333333333333";
    const TREE: &str = "4444444444444444444444444444444444444444";
    const MAIN: &str = "6666666666666666666666666666666666666666";
    const REPOSITORY: &str = "example-org/gang";
    const WORKFLOW: &str = "example-org/gang/.github/workflows/infra.yml@refs/heads/main";
    const EMPTY_PLAN: &str = "{}";
    const BLOCKED_PLAN: &str = r#"{"resource_changes":[{"address":"aws_kms_key.state","type":"aws_kms_key","change":{"actions":["delete"]}}]}"#;
    const RESTART_PLAN: &str = r#"{"resource_changes":[{"address":"module.gang.aws_ssm_parameter.config","type":"aws_ssm_parameter","change":{"actions":["update"],"before":{"value":"a"},"after":{"value":"b"},"after_unknown":{}}}]}"#;

    struct Case<'a> {
        args: Vec<&'a str>,
        stdin: &'a str,
        code: ExitCode,
        stdout: &'a str,
        stderr: Option<&'a str>,
    }

    struct Fixtures {
        _dir: tempfile::TempDir,
        plan_file: String,
        wrong_plan_file: String,
        manifest_file: String,
        request_text: String,
        expected_request: String,
        expected_manifest: String,
        tree_comment: String,
        manifest_comment: String,
        comments: String,
        pulls: String,
        expected_pull: String,
        evidence: String,
        versions: String,
        object: String,
    }

    impl Fixtures {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("create deploy test directory");
            let plan_path = dir.path().join("plan");
            let wrong_plan_path = dir.path().join("wrong-plan");
            let manifest_path = dir.path().join("manifest.json");
            fs::write(&plan_path, b"plan bytes\n").expect("write plan");
            fs::write(&wrong_plan_path, b"other bytes\n").expect("write different plan");
            let plan_file = plan_path.to_str().expect("UTF-8 plan path").to_owned();
            let wrong_plan_file = wrong_plan_path
                .to_str()
                .expect("UTF-8 wrong plan path")
                .to_owned();
            let manifest_file = manifest_path
                .to_str()
                .expect("UTF-8 manifest path")
                .to_owned();

            let plan: Plan = EMPTY_PLAN.parse().expect("parse empty plan");
            let request = StoreRequest::try_new(
                StoreInputs {
                    repository: REPOSITORY.parse().expect("repository"),
                    repository_id: "42".parse().expect("repository id"),
                    pr_number: "7".parse().expect("pull request number"),
                    pr_head_sha: HEAD.parse().expect("head sha"),
                    sha: MERGE.parse().expect("merge sha"),
                    head: MERGE.parse().expect("checked out sha"),
                    parents: vec![
                        BASE.parse().expect("base sha"),
                        HEAD.parse().expect("head sha"),
                    ],
                    tree_sha: TREE.parse().expect("tree sha"),
                    run_id: "99".parse().expect("run id"),
                    run_attempt: "2".parse().expect("run attempt"),
                    prefix: "plans".parse().expect("prefix"),
                    max_age: MaxAge::try_from(Duration::from_hours(24)).expect("positive max age"),
                    now: UnixTime::from_seconds(1_767_312_000),
                },
                &plan,
                PlanSha256::digest(b"plan bytes\n"),
            )
            .expect("construct store request");
            let request_text = request.to_string();
            let manifest = request.into_manifest("ver-1".parse().expect("plan version"));
            let manifest_text = manifest.to_string();
            fs::write(&manifest_path, &manifest_text).expect("write manifest");
            let comments_value = Self::comments_value(&manifest);
            let pulls_value = Self::pulls_value();
            Self {
                _dir: dir,
                plan_file,
                wrong_plan_file,
                manifest_file,
                expected_request: format!("{request_text}\n"),
                request_text,
                expected_manifest: format!("{manifest_text}\n"),
                tree_comment: Self::tree_comment(),
                manifest_comment: Self::manifest_comment(&manifest),
                comments: comments_value.to_string(),
                pulls: pulls_value.to_string(),
                expected_pull: format!("7 {HEAD}\n"),
                evidence: Self::evidence(&pulls_value, &comments_value),
                versions: Self::versions(&manifest),
                object: Self::object(&manifest),
            }
        }

        /// Builds the expected comment for a failed plan.
        fn tree_comment() -> String {
            let body = format!(
                "<!-- gang-deploy-plan -->\n## Infrastructure plan\n\nTree: `{TREE}`\n\n\
                 **Plan did not complete.** See the workflow run logs.\n"
            );
            format!("{}\n", json!({"body": body}))
        }

        /// Builds the expected comment for a completed plan.
        fn manifest_comment(manifest: &Manifest) -> String {
            let body = format!(
                "<!-- gang-deploy-plan -->\n## Infrastructure plan\n\nTree: `{TREE}`\n\n\
                 No changes.\n\n| Action | Count |\n| --- | ---: |\n| create | 0 |\n\
                 | update | 0 |\n| delete | 0 |\n| replace | 0 |\n\nChanged resources:\n\n\n{}\n",
                manifest.comment_line()
            );
            format!("{}\n", json!({"body": body}))
        }

        /// Builds the comments used by comment and evidence commands.
        fn comments_value(manifest: &Manifest) -> serde_json::Value {
            json!([{
                "id": 12,
                "user": {"login": "github-actions[bot]", "type": "Bot"},
                "body": format!("<!-- gang-deploy-plan -->\n{}\n", manifest.comment_line())
            }])
        }

        /// Builds the pull requests used by pull and evidence commands.
        fn pulls_value() -> serde_json::Value {
            json!([{
                "number": 7,
                "merged_at": "2026-01-02T00:00:00Z",
                "merge_commit_sha": MAIN,
                "head": {"sha": HEAD, "repo": {"full_name": REPOSITORY}}
            }])
        }

        /// Builds the workflow evidence document.
        fn evidence(pulls: &serde_json::Value, comments: &serde_json::Value) -> String {
            let workflow_run = json!({
                "id": 99, "run_attempt": 2, "run_started_at": "2026-01-02T00:00:00Z",
                "head_sha": HEAD, "event": "pull_request", "status": "completed",
                "conclusion": "success", "path": ".github/workflows/infra.yml",
                "head_repository": {"full_name": REPOSITORY}, "repository": {"id": 42}
            });
            json!({
                "pulls": pulls, "comments": comments,
                "run": workflow_run, "runs": {"workflow_runs": [workflow_run]}
            })
            .to_string()
        }

        /// Builds the plan object version listing.
        fn versions(manifest: &Manifest) -> String {
            json!({
                "Versions": [{"Key": manifest.plan_key.as_str(), "VersionId": "ver-1"}]
            })
            .to_string()
        }

        /// Builds the plan object head.
        fn object(manifest: &Manifest) -> String {
            let metadata: serde_json::Map<String, serde_json::Value> = manifest
                .facts
                .object_metadata()
                .pairs()
                .iter()
                .map(|(name, value)| ((*name).to_owned(), json!(value)))
                .collect();
            json!({
                "LastModified": "2026-01-02T00:00:00Z", "Metadata": metadata
            })
            .to_string()
        }

        fn plan_cases(&self) -> Vec<Case<'_>> {
            vec![
                Case {
                    args: vec!["policy"],
                    stdin: EMPTY_PLAN,
                    code: ExitCode::SUCCESS,
                    stdout: "has_changes=false\nblocked=false\n",
                    stderr: None,
                },
                Case {
                    args: vec!["policy"],
                    stdin: BLOCKED_PLAN,
                    code: ExitCode::from(3),
                    stdout: "has_changes=true\nblocked=true\n",
                    stderr: Some("aws_kms_key.state"),
                },
                Case {
                    args: vec!["restart-needed"],
                    stdin: RESTART_PLAN,
                    code: ExitCode::SUCCESS,
                    stdout: "true\n",
                    stderr: None,
                },
                Case {
                    args: vec![
                        "store-request",
                        "--repository",
                        REPOSITORY,
                        "--repository-id",
                        "42",
                        "--pr-number",
                        "7",
                        "--pr-head-sha",
                        HEAD,
                        "--sha",
                        MERGE,
                        "--head",
                        MERGE,
                        "--parent",
                        BASE,
                        "--parent",
                        HEAD,
                        "--tree-sha",
                        TREE,
                        "--run-id",
                        "99",
                        "--run-attempt",
                        "2",
                        "--prefix",
                        "plans",
                        "--max-age",
                        "24h",
                        "--now",
                        "1767312000",
                        "--plan-file",
                        &self.plan_file,
                    ],
                    stdin: EMPTY_PLAN,
                    code: ExitCode::SUCCESS,
                    stdout: &self.expected_request,
                    stderr: None,
                },
                Case {
                    args: vec!["manifest", "--plan-version", "ver-1"],
                    stdin: &self.request_text,
                    code: ExitCode::SUCCESS,
                    stdout: &self.expected_manifest,
                    stderr: None,
                },
                Case {
                    args: vec!["policy"],
                    stdin: "not JSON",
                    code: ExitCode::FAILURE,
                    stdout: "",
                    stderr: Some("cannot read plan JSON"),
                },
            ]
        }

        fn comment_cases(&self) -> Vec<Case<'_>> {
            vec![
                Case {
                    args: vec!["comment", "--tree", TREE],
                    stdin: "",
                    code: ExitCode::SUCCESS,
                    stdout: &self.tree_comment,
                    stderr: None,
                },
                Case {
                    args: vec!["comment", "--manifest", &self.manifest_file],
                    stdin: EMPTY_PLAN,
                    code: ExitCode::SUCCESS,
                    stdout: &self.manifest_comment,
                    stderr: None,
                },
                Case {
                    args: vec!["comment-id"],
                    stdin: &self.comments,
                    code: ExitCode::SUCCESS,
                    stdout: "12\n",
                    stderr: None,
                },
                Case {
                    args: vec!["comment-id"],
                    stdin: "[]",
                    code: ExitCode::SUCCESS,
                    stdout: "",
                    stderr: None,
                },
                Case {
                    args: vec!["merged-pr", "--sha", MAIN, "--repository", REPOSITORY],
                    stdin: &self.pulls,
                    code: ExitCode::SUCCESS,
                    stdout: &self.expected_pull,
                    stderr: None,
                },
                Case {
                    args: vec!["plan-comment"],
                    stdin: &self.comments,
                    code: ExitCode::SUCCESS,
                    stdout: &self.expected_manifest,
                    stderr: None,
                },
                Case {
                    args: vec![
                        "workflow-file",
                        "--ref",
                        "example-org/gang/.github/workflows/ci.yml@refs/heads/main",
                    ],
                    stdin: "",
                    code: ExitCode::SUCCESS,
                    stdout: "ci.yml\n",
                    stderr: None,
                },
                Case {
                    args: vec![
                        "workflow-file",
                        "--ref",
                        "example-org/gang/ci.yml@refs/heads/main",
                    ],
                    stdin: "",
                    code: ExitCode::FAILURE,
                    stdout: "",
                    stderr: Some("workflow ref has no .github/workflows/"),
                },
            ]
        }

        fn check_cases(&self) -> Vec<Case<'_>> {
            vec![
                Case {
                    args: vec![
                        "resolve",
                        "--ref",
                        "refs/heads/main",
                        "--head",
                        MAIN,
                        "--sha",
                        MAIN,
                        "--parent",
                        BASE,
                        "--tree-sha",
                        TREE,
                        "--repository",
                        REPOSITORY,
                        "--repository-id",
                        "42",
                        "--prefix",
                        "plans",
                        "--workflow-ref",
                        WORKFLOW,
                    ],
                    stdin: &self.evidence,
                    code: ExitCode::SUCCESS,
                    stdout: &self.expected_manifest,
                    stderr: None,
                },
                Case {
                    args: vec!["check-versions", "--manifest", &self.manifest_file],
                    stdin: &self.versions,
                    code: ExitCode::SUCCESS,
                    stdout: "",
                    stderr: None,
                },
                Case {
                    args: vec![
                        "check-object",
                        "--manifest",
                        &self.manifest_file,
                        "--now",
                        "1767312001",
                        "--max-age",
                        "24h",
                    ],
                    stdin: &self.object,
                    code: ExitCode::SUCCESS,
                    stdout: "",
                    stderr: None,
                },
                Case {
                    args: vec![
                        "check-plan",
                        "--manifest",
                        &self.manifest_file,
                        "--plan-file",
                        &self.plan_file,
                    ],
                    stdin: "",
                    code: ExitCode::SUCCESS,
                    stdout: "",
                    stderr: None,
                },
                Case {
                    args: vec![
                        "check-plan",
                        "--manifest",
                        &self.manifest_file,
                        "--plan-file",
                        &self.wrong_plan_file,
                    ],
                    stdin: "",
                    code: ExitCode::FAILURE,
                    stdout: "",
                    stderr: Some("plan sha256 mismatch"),
                },
            ]
        }
    }

    #[test]
    fn every_deploy_command_maps_its_input_to_its_output_and_exit_code() {
        let fixtures = Fixtures::new();
        run_cases(fixtures.plan_cases());
        run_cases(fixtures.comment_cases());
        run_cases(fixtures.check_cases());
    }

    fn run_cases(cases: Vec<Case<'_>>) {
        for case in cases {
            let (code, stdout, stderr) = gang_deploy(&case.args, case.stdin);
            assert_eq!(code, case.code, "{:?}: {stderr}", case.args);
            assert_eq!(stdout, case.stdout, "{:?}", case.args);
            if let Some(expected) = case.stderr {
                assert!(stderr.contains(expected), "{:?}: {stderr}", case.args);
            } else {
                assert_eq!(stderr, "", "{:?}", case.args);
            }
        }
    }

    #[test]
    fn comment_requires_a_source() {
        let argv: Vec<OsString> = ["gang", "deploy", "comment"]
            .into_iter()
            .map(OsString::from)
            .collect();
        let error = crate::dispatch::parse(&argv).expect_err("comment requires a source");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }
}
