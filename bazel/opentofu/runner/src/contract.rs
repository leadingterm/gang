//! One `tofu` contract: stage the sources into a work directory, then run the four steps.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;

use crate::errors::RunError;

/// One `tofu` contract: a root under `sources` and the test file to run in it.
pub struct Contract {
    /// The `tofu` binary. Must be absolute, because the steps run in the staged root.
    pub tofu: PathBuf,
    /// Directory of pre-downloaded providers, passed to `tofu init` as `-plugin-dir`.
    pub provider_mirror: PathBuf,
    /// Directory holding `infra/` and optionally `configs/` and `Cargo.toml`.
    pub sources: PathBuf,
    /// Root directory, relative to `sources`.
    pub root: PathBuf,
    /// Test file, relative to the root.
    pub test_file: PathBuf,
    /// Optional absolute runfile containing test expectations.
    pub test_expectations: Option<PathBuf>,
}

/// A step of a contract run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// `tofu fmt -check`, which must succeed silently.
    Fmt,
    /// `tofu init`, which runs without a backend and against the provider mirror.
    Init,
    /// `tofu validate`, which must succeed silently.
    Validate,
    /// `tofu test` for the test file.
    Test,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fmt => "fmt",
            Self::Init => "init",
            Self::Validate => "validate",
            Self::Test => "test",
        })
    }
}

impl Contract {
    /// Stages `sources` into `work` and runs the four steps in order.
    ///
    /// `work` must exist and be writable. The staged copy of the root lives at
    /// `work/<root>`, so the steps never write into `sources`.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::Stage`] when the sources cannot be copied, [`RunError::MissingRoot`]
    /// or [`RunError::MissingTestFile`] when the staged tree lacks them, and the step errors
    /// when a step cannot start, exits unsuccessfully or writes to stderr where it must not.
    pub fn run(&self, work: &Path) -> Result<(), RunError> {
        stage(&self.sources, work)?;

        let root = work.join(&self.root);
        if !root.is_dir() {
            return Err(RunError::MissingRoot {
                root: self.root.clone(),
            });
        }
        if !root.join(&self.test_file).is_file() {
            return Err(RunError::MissingTestFile {
                path: self.test_file.clone(),
            });
        }

        let mut fmt = self.command(&root);
        fmt.args(["fmt", "-check", "-recursive", "-no-color"]);
        checked(Step::Fmt, fmt)?;

        let mut init = self.command(&root);
        init.args(["init", "-backend=false", "-input=false", "-no-color"]);
        let mut plugin_dir = OsString::from("-plugin-dir=");
        plugin_dir.push(&self.provider_mirror);
        init.arg(plugin_dir);
        if root.join(".terraform.lock.hcl").is_file() {
            init.arg("-lockfile=readonly");
        }
        unchecked(Step::Init, init)?;

        let mut validate = self.command(&root);
        validate.args(["validate", "-no-color"]);
        checked(Step::Validate, validate)?;

        let mut test = self.command(&root);
        let mut filter = OsString::from("-filter=");
        filter.push(&self.test_file);
        test.args(["test", "-no-color"]);
        if let Some(path) = &self.test_expectations {
            let contents = fs::read(path).map_err(|source| RunError::ReadExpectations {
                path: path.clone(),
                source,
            })?;
            let expectations: Expectations = serde_json::from_slice(&contents)
                .map_err(|source| RunError::ParseExpectations { source })?;
            test.args(["-verbose", "-json"]).arg(filter);
            let output = test.output().map_err(|source| RunError::Spawn {
                step: Step::Test,
                source,
            })?;
            write_stderr(&output.stderr);
            let diagnostics = report_events(
                &output.stdout,
                &mut io::stdout().lock(),
                &self.test_file,
                &expectations,
            );
            if !output.status.success() {
                return Err(RunError::Failed {
                    step: Step::Test,
                    status: output.status,
                });
            }
            if !output.stderr.is_empty() {
                return Err(RunError::Stderr { step: Step::Test });
            }
            diagnostics?;
            validate_plans(&output.stdout, &self.test_file, &expectations)?;
            validate_state(&output.stdout, &self.test_file, &expectations)?;
            if let Some(retention) = &expectations.retention {
                println!(
                    "Verified expected retention: forced-replacement plans and final managed SSM addresses (not cleanup-forget enumeration):"
                );
                for address in retention.resources.keys() {
                    println!("  {address}");
                }
            }
            Ok(())
        } else {
            test.arg(filter);
            unchecked(Step::Test, test)
        }
    }

    /// Returns a `tofu` command in `root` with the automation environment set.
    fn command(&self, root: &Path) -> Command {
        let mut command = Command::new(&self.tofu);
        command
            .current_dir(root)
            .env("TF_IN_AUTOMATION", "1")
            .env("TF_INPUT", "0")
            .env("CHECKPOINT_DISABLE", "1");
        command
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    plans: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    state_checks: Vec<StateCheck>,
    retention: Option<Retention>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateCheck {
    relation: Relation,
    left: StateRef,
    right: StateRef,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Relation {
    Equal,
    NotEqual,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StateRef {
    Output {
        run: String,
        name: String,
    },
    Resource {
        run: String,
        address: String,
        attribute: String,
    },
}

impl StateRef {
    fn run(&self) -> &str {
        match self {
            Self::Output { run, .. } | Self::Resource { run, .. } => run,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retention {
    run: String,
    resources: BTreeMap<String, String>,
}

fn evidence(reason: impl Into<String>) -> RunError {
    RunError::Evidence {
        reason: reason.into(),
    }
}

/// Validate JSON lines without ever including resource values in diagnostics.
fn validate_plans(
    output: &[u8],
    test_file: &Path,
    expectations: &Expectations,
) -> Result<(), RunError> {
    if expectations.plans.is_empty() {
        return Err(RunError::EmptyExpectations {
            run: "<all runs>".into(),
        });
    }
    for (run, resources) in &expectations.plans {
        if resources.is_empty() {
            return Err(RunError::EmptyExpectations { run: run.clone() });
        }
    }
    let mut seen_runs = BTreeSet::new();
    for (index, line) in output.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let invalid = |reason| RunError::InvalidPlanOutput {
            line: index + 1,
            reason,
        };
        let event: serde_json::Value =
            serde_json::from_slice(line).map_err(|_| invalid("malformed JSON"))?;
        let kind = event
            .get("type")
            .and_then(|value| value.as_str())
            .ok_or_else(|| invalid("missing event type"))?;
        if kind != "test_plan" {
            continue;
        }
        let file = event
            .get("@testfile")
            .and_then(|value| value.as_str())
            .ok_or_else(|| invalid("missing test file"))?;
        let run = event
            .get("@testrun")
            .and_then(|value| value.as_str())
            .ok_or_else(|| invalid("missing test run"))?;
        let resources = event
            .get("test_plan")
            .and_then(|value| value.get("resource_changes"))
            .and_then(|value| value.as_array())
            .ok_or_else(|| invalid("missing resource changes"))?;
        if Path::new(file) != test_file {
            validate_resources(resources, run, None, index + 1)?;
            continue;
        }
        let expected = expectations.plans.get_key_value(run);
        if let Some((expected_run, _)) = expected
            && !seen_runs.insert(expected_run.as_str())
        {
            return Err(RunError::PlanCount {
                run: run.into(),
                address: "<run>".into(),
                count: 2,
            });
        }
        validate_resources(
            resources,
            run,
            expected.map(|(_, resources)| resources),
            index + 1,
        )?;
    }
    for run in expectations.plans.keys() {
        if !seen_runs.contains(run.as_str()) {
            return Err(RunError::PlanCount {
                run: run.clone(),
                address: "<run>".into(),
                count: 0,
            });
        }
    }
    Ok(())
}

fn validate_state(output: &[u8], file: &Path, expectations: &Expectations) -> Result<(), RunError> {
    let needed = needed_states(expectations)?;
    let states = collect_states(output, file, &needed)?;
    for check in &expectations.state_checks {
        let left = state_value(&states, &check.left)?;
        let right = state_value(&states, &check.right)?;
        if (left == right) != matches!(check.relation, Relation::Equal) {
            return Err(evidence("state comparison failed"));
        }
    }
    if let Some(retention) = &expectations.retention {
        let state = states
            .get(&retention.run)
            .ok_or_else(|| evidence("missing retention state"))?;
        validate_retained_addresses(state, retention)?;
    }
    Ok(())
}

fn needed_states(expectations: &Expectations) -> Result<BTreeSet<&str>, RunError> {
    let mut needed = BTreeSet::new();
    for check in &expectations.state_checks {
        needed.insert(check.left.run());
        needed.insert(check.right.run());
    }
    if let Some(retention) = &expectations.retention {
        if retention.resources.is_empty() {
            return Err(evidence("retention resources must not be empty"));
        }
        for (address, run) in &retention.resources {
            let actions = expectations
                .plans
                .get(run)
                .and_then(|plan| plan.get(address));
            if !matches!(actions, Some(actions) if actions.as_slice() == ["forget", "create"]) {
                return Err(evidence(format!(
                    "missing forced-replacement proof for {address} in run {run}"
                )));
            }
        }
        needed.insert(retention.run.as_str());
    }
    Ok(needed)
}

fn collect_states(
    output: &[u8],
    file: &Path,
    needed: &BTreeSet<&str>,
) -> Result<BTreeMap<String, serde_json::Value>, RunError> {
    let mut states = BTreeMap::new();
    for line in output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut event: serde_json::Value =
            serde_json::from_slice(line).map_err(|_| evidence("malformed test JSON"))?;
        if event.get("type").and_then(|v| v.as_str()) != Some("test_state")
            || event
                .get("@testfile")
                .and_then(|v| v.as_str())
                .map(Path::new)
                != Some(file)
        {
            continue;
        }
        let run = event
            .get("@testrun")
            .and_then(|v| v.as_str())
            .ok_or_else(|| evidence("state missing run"))?
            .to_owned();
        if needed.contains(run.as_str()) {
            let state = event
                .as_object_mut()
                .and_then(|event| event.remove("test_state"))
                .ok_or_else(|| evidence("state missing test_state"))?;
            match states.entry(run) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(state);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    return Err(evidence(format!("duplicate state for run {}", entry.key())));
                }
            }
        }
    }
    for run in needed {
        if !states.contains_key(*run) {
            return Err(evidence(format!("missing state for run {run}")));
        }
    }
    Ok(states)
}

fn validate_retained_addresses(
    state: &serde_json::Value,
    retention: &Retention,
) -> Result<(), RunError> {
    let mut addresses = BTreeSet::new();
    visit_resources(root_module(state)?, &mut |resource| {
        if resource.get("mode").and_then(|v| v.as_str()) == Some("managed")
            && resource.get("type").and_then(|v| v.as_str()) == Some("aws_ssm_parameter")
        {
            let address = resource
                .get("address")
                .and_then(|v| v.as_str())
                .ok_or_else(|| evidence("SSM resource missing address"))?;
            if !addresses.insert(address) {
                return Err(evidence("duplicate SSM resource address"));
            }
        }
        Ok(())
    })?;
    if addresses
        .iter()
        .copied()
        .ne(retention.resources.keys().map(String::as_str))
    {
        return Err(evidence(
            "final managed SSM address set differs from retention expectations",
        ));
    }
    Ok(())
}

fn root_module(state: &serde_json::Value) -> Result<&serde_json::Value, RunError> {
    if state.get("format_version").and_then(|v| v.as_str()) != Some("1.0") {
        return Err(evidence("invalid state format version"));
    }
    state
        .get("values")
        .and_then(|v| v.get("root_module"))
        .filter(|v| v.is_object())
        .ok_or_else(|| evidence("missing state root module"))
}

fn visit_resources<'a>(
    module: &'a serde_json::Value,
    visit: &mut impl FnMut(&'a serde_json::Value) -> Result<(), RunError>,
) -> Result<(), RunError> {
    if let Some(resources) = module.get("resources") {
        for resource in resources
            .as_array()
            .ok_or_else(|| evidence("invalid state resources"))?
        {
            if !resource.is_object()
                || ["address", "mode", "type"]
                    .iter()
                    .any(|key| resource.get(key).and_then(|v| v.as_str()).is_none())
            {
                return Err(evidence("invalid state resource metadata"));
            }
            visit(resource)?;
        }
    }
    if let Some(children) = module.get("child_modules") {
        for child in children
            .as_array()
            .ok_or_else(|| evidence("invalid child modules"))?
        {
            if !child.is_object() {
                return Err(evidence("invalid child module"));
            }
            visit_resources(child, visit)?;
        }
    }
    Ok(())
}

fn state_value<'a>(
    states: &'a BTreeMap<String, serde_json::Value>,
    reference: &StateRef,
) -> Result<&'a str, RunError> {
    let state = states
        .get(reference.run())
        .ok_or_else(|| evidence("missing referenced state"))?;
    let root = root_module(state)?;
    let value = match reference {
        StateRef::Output { name, .. } => state
            .get("values")
            .and_then(|v| v.get("outputs"))
            .and_then(|v| v.get(name))
            .and_then(|v| v.get("value")),
        StateRef::Resource {
            address, attribute, ..
        } => {
            let mut found = None;
            visit_resources(root, &mut |resource| {
                if resource.get("address").and_then(|v| v.as_str()) == Some(address) {
                    if found.is_some() {
                        return Err(evidence("duplicate referenced resource"));
                    }
                    if resource.get("mode").and_then(|v| v.as_str()) != Some("managed")
                        || resource.get("type").and_then(|v| v.as_str()).is_none()
                    {
                        return Err(evidence("invalid referenced resource type or mode"));
                    }
                    found = Some(resource);
                }
                Ok(())
            })?;
            found
                .and_then(|resource| resource.get("values"))
                .and_then(|values| values.get(attribute))
        }
    };
    value
        .and_then(|v| v.as_str())
        .ok_or_else(|| evidence("missing or non-string state value"))
}

fn validate_resources(
    resources: &[serde_json::Value],
    run: &str,
    expected: Option<&BTreeMap<String, Vec<String>>>,
    line: usize,
) -> Result<(), RunError> {
    let invalid = |reason| RunError::InvalidPlanOutput { line, reason };
    let mut seen_resources = BTreeSet::new();
    for resource in resources {
        let address = resource
            .get("address")
            .and_then(|value| value.as_str())
            .ok_or_else(|| invalid("missing resource address"))?;
        let resource_type = resource
            .get("type")
            .and_then(|value| value.as_str())
            .ok_or_else(|| invalid("missing resource type"))?;
        let values = resource
            .get("change")
            .and_then(|value| value.get("actions"))
            .and_then(|value| value.as_array())
            .ok_or_else(|| invalid("missing resource actions"))?;
        if values.iter().any(|value| !value.is_string()) {
            return Err(invalid("invalid resource action"));
        }
        if resource_type == "aws_ssm_parameter"
            && values.iter().any(|value| value.as_str() == Some("delete"))
        {
            return Err(RunError::ParameterDelete {
                run: run.into(),
                address: address.into(),
                actions: values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_owned))
                    .collect(),
            });
        }
        if let Some(wanted) = expected.and_then(|resources| resources.get(address)) {
            if !seen_resources.insert(address) {
                return Err(RunError::PlanCount {
                    run: run.into(),
                    address: address.into(),
                    count: 2,
                });
            }
            if values.len() != wanted.len()
                || values
                    .iter()
                    .zip(wanted)
                    .any(|(value, action)| value.as_str() != Some(action.as_str()))
            {
                return Err(RunError::PlanActions {
                    run: run.into(),
                    address: address.into(),
                    expected: wanted.clone(),
                    actual: values
                        .iter()
                        .filter_map(|value| value.as_str().map(str::to_owned))
                        .collect(),
                });
            }
        }
    }
    if let Some(expected) = expected {
        for address in expected.keys() {
            if !seen_resources.contains(address.as_str()) {
                return Err(RunError::PlanCount {
                    run: run.into(),
                    address: address.clone(),
                    count: 0,
                });
            }
        }
    }
    Ok(())
}

/// Recognizes the exact `OpenTofu` successful-forget diagnostic, without its test scope.
pub(crate) fn is_successful_forget_notice(event: &serde_json::Value) -> bool {
    const DETAIL: &str = concat!(
        "As requested, OpenTofu has not deleted some remote objects that are no longer managed by this configuration. Those objects continue to exist in their remote system and so may continue to incur charges. Refer to the original plan for more information.\n",
        "To suppress this error for the future 'destroy' runs, you can add the CLI flag \"-suppress-forget-errors\"."
    );
    let Some(diagnostic) = event.get("diagnostic") else {
        return false;
    };
    event.get("type").and_then(|v| v.as_str()) == Some("diagnostic")
        && event.get("@level").and_then(|v| v.as_str()) == Some("error")
        && diagnostic.get("severity").and_then(|v| v.as_str()) == Some("error")
        && diagnostic.get("summary").and_then(|v| v.as_str())
            == Some("Destroy was successful but left behind forgotten instances")
        && diagnostic.get("detail").and_then(|v| v.as_str()) == Some(DETAIL)
}

/// Forward lifecycle and diagnostic events, but never plans or state.
fn report_events(
    output: &[u8],
    writer: &mut impl Write,
    file: &Path,
    expectations: &Expectations,
) -> Result<(), RunError> {
    let mut failure = None;
    let mut retention_count = 0;
    let mut files = 0;
    let mut summaries = 0;
    let mut passed = None;
    let mut runs = BTreeSet::new();
    for line in output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: serde_json::Value = if let Ok(event) = serde_json::from_slice(line) {
            event
        } else {
            failure = Some(evidence("malformed test JSON"));
            continue;
        };
        let kind = event.get("type").and_then(|v| v.as_str());
        if matches!(
            kind,
            Some("diagnostic" | "test_file" | "test_run" | "test_summary")
        ) {
            let _ = writer.write_all(line);
            let _ = writer.write_all(b"\n");
        }
        let expected_retention = is_successful_forget_notice(&event)
            && expectations.retention.as_ref().is_some_and(|retention| {
                event
                    .get("@testfile")
                    .and_then(|v| v.as_str())
                    .map(Path::new)
                    == Some(file)
                    && event.get("@testrun").and_then(|v| v.as_str())
                        == Some(retention.run.as_str())
            });
        if expected_retention {
            retention_count += 1;
        } else if kind == Some("test_cleanup") {
            let _ = writeln!(writer, "OpenTofu test cleanup failed");
            failure = Some(RunError::Diagnostic {
                severity: "cleanup".into(),
            });
        } else if let Some(level) = event
            .get("@level")
            .and_then(|v| v.as_str())
            .filter(|level| matches!(*level, "error" | "warn" | "warning"))
            .or_else(|| {
                event
                    .pointer("/diagnostic/severity")
                    .and_then(|v| v.as_str())
                    .filter(|level| matches!(*level, "error" | "warn" | "warning"))
            })
        {
            failure = Some(RunError::Diagnostic {
                severity: level.into(),
            });
        }
        if let Err(error) = check_lifecycle(&event, file, &mut files, &mut runs, &mut summaries) {
            failure = Some(error);
        }
        if kind == Some("test_summary") {
            passed = event
                .pointer("/test_summary/passed")
                .and_then(serde_json::Value::as_u64);
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    if files != 1 || summaries != 1 || runs.is_empty() || passed != u64::try_from(runs.len()).ok() {
        return Err(evidence("missing or duplicate test lifecycle events"));
    }
    if retention_count != usize::from(expectations.retention.is_some()) {
        return Err(evidence(
            "missing or duplicate expected retention diagnostic",
        ));
    }
    Ok(())
}

fn check_lifecycle(
    event: &serde_json::Value,
    file: &Path,
    files: &mut usize,
    runs: &mut BTreeSet<String>,
    summaries: &mut usize,
) -> Result<(), RunError> {
    let kind = event
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| evidence("missing event type"))?;
    if !matches!(kind, "test_file" | "test_run" | "test_summary") {
        return Ok(());
    }
    let body = event
        .get(kind)
        .ok_or_else(|| evidence("missing lifecycle body"))?;
    if body.get("status").and_then(|v| v.as_str()) != Some("pass") {
        return Err(evidence("test lifecycle status is not pass"));
    }
    if kind == "test_summary" {
        *summaries += 1;
        if ["failed", "errored", "skipped"]
            .iter()
            .any(|key| body.get(key).and_then(serde_json::Value::as_u64) != Some(0))
            || body
                .get("passed")
                .and_then(serde_json::Value::as_u64)
                .is_none_or(|count| count == 0)
        {
            return Err(evidence("invalid test summary counts"));
        }
    } else {
        if event
            .get("@testfile")
            .and_then(|v| v.as_str())
            .map(Path::new)
            != Some(file)
            || body.get("path").and_then(|v| v.as_str()).map(Path::new) != Some(file)
        {
            return Err(evidence("unexpected lifecycle test file"));
        }
        if kind == "test_file" {
            *files += 1;
        } else {
            let run = event
                .get("@testrun")
                .and_then(|v| v.as_str())
                .ok_or_else(|| evidence("missing lifecycle run"))?;
            if body.get("run").and_then(|v| v.as_str()) != Some(run) || !runs.insert(run.to_owned())
            {
                return Err(evidence("invalid or duplicate lifecycle run"));
            }
        }
    }
    Ok(())
}

/// Copies `infra/`, optional `configs/`, and optional `Cargo.toml` into `work`.
///
/// Symlinks are followed, so the copy holds regular files only.
pub(crate) fn stage(sources: &Path, work: &Path) -> Result<(), RunError> {
    // Each entry is (relative path, required, directory).
    const ENTRIES: &[(&str, bool, bool)] = &[
        ("infra", true, true),
        ("configs", false, true),
        ("Cargo.toml", false, false),
    ];

    for &(name, required, directory) in ENTRIES {
        let from = sources.join(name);
        let to = work.join(name);
        if directory {
            if required || from.is_dir() {
                copy_dir(&from, &to)?;
            }
        } else if required || from.is_file() {
            fs::copy(&from, &to).map_err(|source| RunError::Stage { source, path: from })?;
        }
    }
    Ok(())
}

/// Recursively copies the directory `from` to the new directory `to`, following symlinks.
fn copy_dir(from: &Path, to: &Path) -> Result<(), RunError> {
    let stage_error = |source: io::Error, path: &Path| RunError::Stage {
        source,
        path: path.to_path_buf(),
    };

    fs::create_dir(to).map_err(|source| stage_error(source, to))?;
    let entries = fs::read_dir(from).map_err(|source| stage_error(source, from))?;
    for entry in entries {
        let entry = entry.map_err(|source| stage_error(source, from))?;
        let source_path = entry.path();
        let target = to.join(entry.file_name());
        // `metadata` follows symlinks: runfiles entries are symlinks into the build outputs.
        let metadata =
            fs::metadata(&source_path).map_err(|source| stage_error(source, &source_path))?;
        if metadata.is_dir() {
            copy_dir(&source_path, &target)?;
        } else {
            fs::copy(&source_path, &target).map_err(|source| stage_error(source, &source_path))?;
        }
    }
    Ok(())
}

/// Runs a step whose exit status is checked; it must also write nothing to stderr.
///
/// The captured stderr goes to this process's stderr when the step fails.
fn checked(step: Step, mut command: Command) -> Result<(), RunError> {
    command.stdout(Stdio::inherit()).stderr(Stdio::piped());
    let output = command
        .output()
        .map_err(|source| RunError::Spawn { step, source })?;
    if !output.status.success() {
        write_stderr(&output.stderr);
        return Err(RunError::Failed {
            step,
            status: output.status,
        });
    }
    if !output.stderr.is_empty() {
        write_stderr(&output.stderr);
        return Err(RunError::Stderr { step });
    }
    Ok(())
}

/// Runs a step whose output passes through and whose exit status is the only verdict.
fn unchecked(step: Step, mut command: Command) -> Result<(), RunError> {
    let status = command
        .status()
        .map_err(|source| RunError::Spawn { step, source })?;
    if status.success() {
        Ok(())
    } else {
        Err(RunError::Failed { step, status })
    }
}

/// Writes captured child stderr to this process's stderr, best effort: the step error that
/// follows is the failure the caller reports.
fn write_stderr(bytes: &[u8]) {
    let _ = io::stderr().write_all(bytes);
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use super::*;

    /// Builds a contract for the fixture `root`, using the `OPENTOFU_TOFU` and
    /// `OPENTOFU_PROVIDER_MIRROR` environment variables, which may be relative to the current directory.
    fn contract(root: &str) -> Contract {
        let cwd = env::current_dir().expect("current directory");
        let tofu =
            env::var_os("OPENTOFU_TOFU").expect("OPENTOFU_TOFU is set by the test environment");
        let mirror = env::var_os("OPENTOFU_PROVIDER_MIRROR")
            .expect("OPENTOFU_PROVIDER_MIRROR is set by the test environment");
        Contract {
            tofu: cwd.join(tofu),
            provider_mirror: cwd.join(mirror),
            sources: cwd.join(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures"),
            root: PathBuf::from(root),
            test_file: PathBuf::from("tests/greeting.tftest.hcl"),
            test_expectations: None,
        }
    }

    fn run(root: &str) -> Result<(), RunError> {
        let work = tempfile::tempdir().expect("work directory");
        contract(root).run(work.path())
    }

    #[test]
    fn stages_workspace_manifest() {
        let sources = tempfile::tempdir().expect("sources directory");
        let work = tempfile::tempdir().expect("work directory");
        fs::create_dir(sources.path().join("infra")).expect("infra directory");
        fs::write(
            sources.path().join("Cargo.toml"),
            "[workspace.package]\nversion = \"1.2.3\"\n",
        )
        .expect("workspace manifest");

        stage(sources.path(), work.path()).expect("stage sources");

        assert_eq!(
            fs::read_to_string(work.path().join("Cargo.toml")).expect("staged manifest"),
            "[workspace.package]\nversion = \"1.2.3\"\n"
        );
        assert!(work.path().join("infra").is_dir());
        assert!(!work.path().join("configs").exists());
    }

    #[test]
    fn passing_root_runs_every_step() {
        let result = run("infra/passing");
        assert!(result.is_ok(), "got {result:?}");
    }

    #[test]
    fn misformatted_root_fails_fmt() {
        let result = run("infra/misformatted");
        assert!(
            matches!(
                result,
                Err(RunError::Failed {
                    step: Step::Fmt,
                    ..
                })
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn failing_test_fails_test() {
        let result = run("infra/failing");
        assert!(
            matches!(
                result,
                Err(RunError::Failed {
                    step: Step::Test,
                    ..
                })
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn missing_root_is_reported() {
        let result = run("infra/does-not-exist");
        assert!(
            matches!(result, Err(RunError::MissingRoot { .. })),
            "got {result:?}"
        );
    }
    fn plan_event() -> serde_json::Value {
        serde_json::json!({
            "type": "test_plan",
            "@testfile": "tests/config.tftest.hcl",
            "@testrun": "transition",
            "test_plan": { "resource_changes": [
                {"address": "aws_ssm_parameter.legacy", "type": "aws_ssm_parameter", "change": {"actions": ["no-op"]}},
                {"address": "aws_ssm_parameter.generation", "type": "aws_ssm_parameter", "change": {"actions": ["forget", "create"]}},
                {"address": "terraform_data.other", "type": "terraform_data", "change": {"actions": ["delete"]}}
            ]}
        })
    }

    fn expected() -> Expectations {
        serde_json::from_value(serde_json::json!({
            "plans": {"transition": {
                "aws_ssm_parameter.legacy": ["no-op"],
                "aws_ssm_parameter.generation": ["forget", "create"]
            }}
        }))
        .expect("valid test expectations")
    }

    fn validate_events(events: &[serde_json::Value]) -> Result<(), RunError> {
        let output = events
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        validate_plans(
            output.as_bytes(),
            Path::new("tests/config.tftest.hcl"),
            &expected(),
        )
    }

    #[test]
    fn accepts_forget_create_and_non_parameter_delete() {
        let mut unrelated = plan_event();
        unrelated["@testrun"] = "apply".into();
        assert!(validate_events(&[plan_event(), unrelated]).is_ok());
    }

    #[test]
    fn rejects_parameter_delete_even_in_unrequested_run() {
        let mut event = plan_event();
        event["@testrun"] = "apply".into();
        event["test_plan"]["resource_changes"][1]["change"]["actions"] =
            serde_json::json!(["delete", "create"]);
        assert!(matches!(
            validate_events(&[plan_event(), event]),
            Err(RunError::ParameterDelete { .. })
        ));
    }

    #[test]
    fn rejects_legacy_update() {
        let mut event = plan_event();
        event["test_plan"]["resource_changes"][0]["change"]["actions"] =
            serde_json::json!(["update"]);
        assert!(matches!(
            validate_events(&[event]),
            Err(RunError::PlanActions { .. })
        ));
    }

    #[test]
    fn rejects_missing_and_duplicate_runs() {
        assert!(matches!(
            validate_events(&[]),
            Err(RunError::PlanCount { count: 0, .. })
        ));
        assert!(matches!(
            validate_events(&[plan_event(), plan_event()]),
            Err(RunError::PlanCount { count: 2, .. })
        ));
    }

    #[test]
    fn rejects_missing_and_duplicate_resources() {
        let mut missing = plan_event();
        missing["test_plan"]["resource_changes"]
            .as_array_mut()
            .expect("resources")
            .remove(0);
        assert!(matches!(
            validate_events(&[missing]),
            Err(RunError::PlanCount { count: 0, .. })
        ));
        let mut duplicate = plan_event();
        let resources = duplicate["test_plan"]["resource_changes"]
            .as_array_mut()
            .expect("resources");
        resources.push(resources[0].clone());
        assert!(matches!(
            validate_events(&[duplicate]),
            Err(RunError::PlanCount { count: 2, .. })
        ));
    }

    #[test]
    fn rejects_wrong_file_and_run() {
        let mut wrong_file = plan_event();
        wrong_file["@testfile"] = "tests/other.tftest.hcl".into();
        assert!(matches!(
            validate_events(&[wrong_file]),
            Err(RunError::PlanCount { count: 0, .. })
        ));
        let mut wrong_run = plan_event();
        wrong_run["@testrun"] = "other".into();
        assert!(matches!(
            validate_events(&[wrong_run]),
            Err(RunError::PlanCount { count: 0, .. })
        ));
    }

    #[test]
    fn rejects_malformed_json_and_missing_plan_fields() {
        assert!(matches!(
            validate_plans(b"{", Path::new("tests/config.tftest.hcl"), &expected()),
            Err(RunError::InvalidPlanOutput { .. })
        ));
        let mut event = plan_event();
        event["test_plan"]
            .as_object_mut()
            .expect("plan")
            .remove("resource_changes");
        assert!(matches!(
            validate_events(&[event]),
            Err(RunError::InvalidPlanOutput { .. })
        ));
        assert!(
            serde_json::from_str::<Expectations>(
                r#"{"plans":{"transition": {"resource": "create"}}}"#
            )
            .is_err()
        );
    }
    #[test]
    fn rejects_empty_expectations() {
        let empty: Expectations = serde_json::from_str(r#"{"plans":{}}"#).expect("expectations");
        assert!(matches!(
            validate_plans(b"", Path::new("tests/config.tftest.hcl"), &empty),
            Err(RunError::EmptyExpectations { .. })
        ));
        let empty_run: Expectations =
            serde_json::from_str(r#"{"plans":{"transition":{}}}"#).expect("expectations");
        assert!(matches!(
            validate_plans(b"", Path::new("tests/config.tftest.hcl"), &empty_run),
            Err(RunError::EmptyExpectations { .. })
        ));
    }

    #[test]
    fn rejects_non_string_action_and_missing_resource_type() {
        let mut bad_action = plan_event();
        bad_action["test_plan"]["resource_changes"][0]["change"]["actions"] =
            serde_json::json!([42]);
        assert!(matches!(
            validate_events(&[bad_action]),
            Err(RunError::InvalidPlanOutput { .. })
        ));
        let mut missing_type = plan_event();
        missing_type["test_plan"]["resource_changes"][0]
            .as_object_mut()
            .expect("resource")
            .remove("type");
        assert!(matches!(
            validate_events(&[missing_type]),
            Err(RunError::InvalidPlanOutput { .. })
        ));
    }

    fn bytes(events: &[serde_json::Value]) -> Vec<u8> {
        events
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes()
    }

    fn lifecycle() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"type":"test_file","@testfile":"tests/config.tftest.hcl","test_file":{"path":"tests/config.tftest.hcl","status":"pass"}}),
            serde_json::json!({"type":"test_run","@testfile":"tests/config.tftest.hcl","@testrun":"apply","test_run":{"path":"tests/config.tftest.hcl","run":"apply","status":"pass"}}),
            serde_json::json!({"type":"test_summary","test_summary":{"status":"pass","passed":1,"failed":0,"errored":0,"skipped":0}}),
        ]
    }

    fn report(events: &[serde_json::Value], expectations: &Expectations) -> Result<(), RunError> {
        report_events(
            &bytes(events),
            &mut Vec::new(),
            Path::new("tests/config.tftest.hcl"),
            expectations,
        )
    }

    fn state(run: &str, value: &str, generation: &str) -> serde_json::Value {
        serde_json::json!({"type":"test_state","@testfile":"tests/config.tftest.hcl","@testrun":run,
        "test_state":{"format_version":"1.0","values":{"outputs":{"gang_toml":{"value":value},"uuid":{"value":generation}},
        "root_module":{"resources":[
            {"address":"aws_ssm_parameter.legacy","mode":"managed","type":"aws_ssm_parameter","values":{"value":value}},
            {"address":"aws_ssm_parameter.generation","mode":"managed","type":"aws_ssm_parameter","values":{"value":value}}
        ]}}}})
    }

    fn state_expectations() -> Expectations {
        serde_json::from_value(serde_json::json!({
            "plans":{"transition":{"aws_ssm_parameter.legacy":["forget","create"],"aws_ssm_parameter.generation":["forget","create"]}},
            "state_checks":[
                {"relation":"equal","left":{"kind":"output","run":"a","name":"gang_toml"},"right":{"kind":"resource","run":"a","address":"aws_ssm_parameter.legacy","attribute":"value"}},
                {"relation":"not_equal","left":{"kind":"resource","run":"a","address":"aws_ssm_parameter.legacy","attribute":"value"},"right":{"kind":"resource","run":"b","address":"aws_ssm_parameter.legacy","attribute":"value"}},
                {"relation":"not_equal","left":{"kind":"output","run":"a","name":"uuid"},"right":{"kind":"output","run":"b","name":"uuid"}}
            ],
            "retention":{"run":"b","resources":{"aws_ssm_parameter.legacy":"transition","aws_ssm_parameter.generation":"transition"}}
        })).expect("expectations")
    }

    fn check_states(
        events: &[serde_json::Value],
        expectations: &Expectations,
    ) -> Result<(), RunError> {
        validate_state(
            &bytes(events),
            Path::new("tests/config.tftest.hcl"),
            expectations,
        )
    }

    fn diagnostic() -> serde_json::Value {
        serde_json::json!({"type":"diagnostic","@level":"error","@testfile":"tests/config.tftest.hcl","@testrun":"b",
            "diagnostic":{"severity":"error","summary":"Destroy was successful but left behind forgotten instances",
            "detail": concat!("As requested, OpenTofu has not deleted some remote objects that are no longer managed by this configuration. Those objects continue to exist in their remote system and so may continue to incur charges. Refer to the original plan for more information.\n", "To suppress this error for the future 'destroy' runs, you can add the CLI flag \"-suppress-forget-errors\".")}})
    }

    #[test]
    fn successful_forget_notice_requires_exact_fields() {
        let original = diagnostic();
        assert!(is_successful_forget_notice(&original));
        for pointer in [
            "/type",
            "/@level",
            "/diagnostic/severity",
            "/diagnostic/summary",
            "/diagnostic/detail",
        ] {
            let mut changed = original.clone();
            *changed.pointer_mut(pointer).expect("field") = "different".into();
            assert!(!is_successful_forget_notice(&changed), "{pointer}");
        }
        let mut missing = original;
        missing["diagnostic"]
            .as_object_mut()
            .expect("diagnostic")
            .remove("detail");
        assert!(!is_successful_forget_notice(&missing));
        let mut events = lifecycle();
        events.push(missing);
        assert!(report(&events, &state_expectations()).is_err());
        events[3]["diagnostic"]["detail"] = "different".into();
        assert!(report(&events, &state_expectations()).is_err());
    }

    #[test]
    fn state_comparisons_and_retention_proof() {
        let expected = state_expectations();
        let a = state("a", "legacy bytes", "uuid-a");
        let b = state("b", "new bytes", "uuid-b");
        assert!(check_states(&[a.clone(), b.clone()], &expected).is_ok());
        assert!(
            check_states(
                &[a.clone(), state("b", "legacy bytes", "uuid-b")],
                &expected
            )
            .is_err()
        );
        assert!(check_states(&[a.clone(), state("b", "new bytes", "uuid-a")], &expected).is_err());
        assert!(check_states(std::slice::from_ref(&a), &expected).is_err());
        assert!(check_states(&[a.clone(), a.clone(), b.clone()], &expected).is_err());
        let mut wrong_file = b.clone();
        wrong_file["@testfile"] = "other".into();
        assert!(check_states(&[a.clone(), wrong_file], &expected).is_err());
    }

    #[test]
    fn rejects_invalid_state_resources_and_proofs() {
        let expected = state_expectations();
        let a = state("a", "legacy bytes", "uuid-a");
        let b = state("b", "new bytes", "uuid-b");
        for field in ["address", "mode", "type", "values"] {
            let mut bad = a.clone();
            bad["test_state"]["values"]["root_module"]["resources"][0]
                .as_object_mut()
                .expect("resource")
                .remove(field);
            assert!(
                check_states(&[bad, b.clone()], &expected).is_err(),
                "{field}"
            );
        }
        let mut duplicate = a.clone();
        let resources = duplicate["test_state"]["values"]["root_module"]["resources"]
            .as_array_mut()
            .expect("resources");
        resources.push(resources[0].clone());
        assert!(check_states(&[duplicate, b.clone()], &expected).is_err());
        for value in [serde_json::Value::Null, serde_json::json!(5)] {
            let mut bad = a.clone();
            bad["test_state"]["values"]["outputs"]["gang_toml"]["value"] = value;
            assert!(check_states(&[bad, b.clone()], &expected).is_err());
        }
        let mut extra = b.clone();
        extra["test_state"]["values"]["root_module"]["resources"].as_array_mut().expect("resources")
            .push(serde_json::json!({"address":"aws_ssm_parameter.extra","mode":"managed","type":"aws_ssm_parameter","values":{}}));
        assert!(check_states(&[a.clone(), extra], &expected).is_err());
        let mut wrong_proof = state_expectations();
        wrong_proof
            .plans
            .get_mut("transition")
            .expect("plan")
            .remove("aws_ssm_parameter.legacy");
        assert!(check_states(&[a.clone(), b.clone()], &wrong_proof).is_err());
        wrong_proof.plans.clear();
        assert!(check_states(&[a.clone(), b.clone()], &wrong_proof).is_err());
        let mut wrong_action = state_expectations();
        wrong_action
            .plans
            .get_mut("transition")
            .expect("plan")
            .insert("aws_ssm_parameter.legacy".into(), vec!["create".into()]);
        assert!(check_states(&[a, b], &wrong_action).is_err());
    }

    #[test]
    fn rejects_missing_attributes_and_finds_nested_resources() {
        let expected = state_expectations();
        let a = state("a", "one", "uuid-a");
        let b = state("b", "two", "uuid-b");
        let mut missing_attribute = a.clone();
        missing_attribute["test_state"]["values"]["root_module"]["resources"][0]["values"]
            .as_object_mut()
            .expect("values")
            .remove("value");
        assert!(check_states(&[missing_attribute, b.clone()], &expected).is_err());
        let mut missing_output = a.clone();
        missing_output["test_state"]["values"]["outputs"]
            .as_object_mut()
            .expect("outputs")
            .remove("gang_toml");
        assert!(check_states(&[missing_output, b.clone()], &expected).is_err());
        let mut malformed_state = b.clone();
        malformed_state["test_state"]["format_version"] = "2.0".into();
        assert!(check_states(&[a.clone(), malformed_state], &expected).is_err());
        let mut nested = a;
        let root = &mut nested["test_state"]["values"]["root_module"];
        let resources = root["resources"].as_array_mut().expect("resources");
        let resource = resources.remove(0);
        root["child_modules"] = serde_json::json!([{"resources":[resource]}]);
        assert!(check_states(&[nested, b], &expected).is_ok());
    }

    #[test]
    fn retention_proof_requires_matching_actual_plan() {
        let expected = state_expectations();
        let mut plan = plan_event();
        plan["test_plan"]["resource_changes"][0]["change"]["actions"] =
            serde_json::json!(["forget", "create"]);
        assert!(
            validate_plans(
                &bytes(&[plan.clone()]),
                Path::new("tests/config.tftest.hcl"),
                &expected
            )
            .is_ok()
        );
        plan["test_plan"]["resource_changes"][0]["change"]["actions"] =
            serde_json::json!(["no-op"]);
        assert!(
            validate_plans(
                &bytes(&[plan]),
                Path::new("tests/config.tftest.hcl"),
                &expected
            )
            .is_err()
        );
        let mut empty = state_expectations();
        empty
            .retention
            .as_mut()
            .expect("retention")
            .resources
            .clear();
        assert!(check_states(&[], &empty).is_err());
    }

    #[test]
    fn strict_lifecycle_and_retention_diagnostic() {
        let expected = state_expectations();
        let mut events = lifecycle();
        events.push(diagnostic());
        assert!(report(&events, &expected).is_ok());
        assert!(report(&lifecycle(), &expected).is_err());
        let mut duplicate = events.clone();
        duplicate.push(diagnostic());
        assert!(report(&duplicate, &expected).is_err());
        for key in ["@testfile", "@testrun"] {
            let mut wrong = events.clone();
            wrong[3][key] = "wrong".into();
            assert!(report(&wrong, &expected).is_err());
        }
        for extra in [
            serde_json::json!({"type":"test_cleanup","test_cleanup":null}),
            serde_json::json!({"type":"diagnostic","@level":"error","diagnostic":{"severity":"error","summary":"other"}}),
            serde_json::json!({"type":"other","@level":"warning"}),
        ] {
            let mut bad = events.clone();
            bad.push(extra);
            assert!(report(&bad, &expected).is_err());
        }
        let mut duplicate_file = events.clone();
        duplicate_file.push(events[0].clone());
        assert!(report(&duplicate_file, &expected).is_err());
        let mut duplicate_run = events.clone();
        duplicate_run.push(events[1].clone());
        assert!(report(&duplicate_run, &expected).is_err());
        let mut bad_summary = events.clone();
        bad_summary[2]["test_summary"]["failed"] = 1.into();
        assert!(report(&bad_summary, &expected).is_err());
        assert!(report(&events, &self::expected()).is_err());
        let mut forwarded = Vec::new();
        let mut private = lifecycle();
        private.push(serde_json::json!({"type":"test_state","secret":"hidden"}));
        assert!(
            report_events(
                &bytes(&private),
                &mut forwarded,
                Path::new("tests/config.tftest.hcl"),
                &self::expected()
            )
            .is_ok()
        );
        assert!(
            !String::from_utf8(forwarded)
                .expect("UTF-8")
                .contains("hidden")
        );
    }
}
