//! Configuration publication and refetch, followed by real published-process restarts.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use crate::check::Check;
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::issuer::TestIssuer;
use crate::process;
use crate::settings::DeploymentInputs;
use crate::system::Variant;
use crate::upgrade_fixture::{ConfigurationAccess, ConfigurationPlan, ReleaseFixture};

#[derive(Debug, PartialEq, Eq)]
struct Selector<'a> {
    name: &'a str,
    version: Option<u64>,
}

impl<'a> Selector<'a> {
    fn parse(reference: &'a str) -> Result<Self> {
        let (name, version) = match reference.split_once(':') {
            Some((name, text)) => {
                let version = text.parse::<u64>().map_err(|_| {
                    E2eError::system(format!("invalid parameter version in {reference:?}"))
                })?;
                if version == 0
                    || text.starts_with('0')
                    || text.bytes().any(|byte| !byte.is_ascii_digit())
                {
                    return Err(E2eError::system(
                        "parameter version must be a positive decimal",
                    ));
                }
                (name, Some(version))
            }
            None => (reference, None),
        };
        if name.is_empty()
            || !name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'.')
            })
        {
            return Err(E2eError::system("invalid parameter name"));
        }
        Ok(Self { name, version })
    }
}

/// Only a configured terminal namespace wildcard is interpreted; never arbitrary IAM globbing.
fn resource_matches(pattern: &str, arn: &str) -> bool {
    match pattern.strip_suffix("/*") {
        Some(namespace) if !namespace.contains('*') => arn
            .strip_prefix(namespace)
            .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1),
        _ => !pattern.contains('*') && pattern == arn,
    }
}

struct Parameters {
    legacy_name: String,
    legacy_arn: String,
    values: BTreeMap<String, BTreeMap<u64, Arc<str>>>,
}

impl Parameters {
    fn seeded(plan: &ConfigurationPlan, bytes: &Arc<str>) -> Result<Self> {
        let mut store = Self {
            legacy_name: plan.legacy_name.clone(),
            legacy_arn: plan.legacy_arn.clone(),
            values: BTreeMap::new(),
        };
        store.arn(&plan.legacy_name)?;
        for access in std::iter::once(&plan.controller).chain(&plan.workers) {
            let selector = Selector::parse(&access.reference)?;
            store
                .values
                .entry(selector.name.into())
                .or_default()
                .insert(selector.version.unwrap_or(1), Arc::clone(bytes));
        }
        Ok(store)
    }

    fn arn(&self, name: &str) -> Result<String> {
        let suffix = format!("parameter/{}", self.legacy_name.trim_start_matches('/'));
        let prefix = self
            .legacy_arn
            .strip_suffix(&suffix)
            .filter(|prefix| prefix.starts_with("arn:") && prefix.ends_with(':'))
            .ok_or_else(|| E2eError::system("legacy parameter ARN does not match its name"))?;
        if name != self.legacy_name && !generation_of(&self.legacy_name, name) {
            return Err(E2eError::system(
                "parameter is outside the configured generation namespace",
            ));
        }
        Ok(format!(
            "{prefix}parameter/{}",
            name.trim_start_matches('/')
        ))
    }

    fn fetch(&self, reference: &str, role: &ConfigurationAccess) -> Result<Arc<str>> {
        let selector = Selector::parse(reference)?;
        let arn = self.arn(selector.name)?;
        if !role
            .readable_resources
            .iter()
            .any(|resource| resource_matches(resource, &arn))
            || !role
                .excluded_from_deny
                .iter()
                .any(|resource| resource_matches(resource, &arn))
        {
            return Err(E2eError::system(format!(
                "parameter access denied for {reference:?}"
            )));
        }
        let versions = self
            .values
            .get(selector.name)
            .ok_or_else(|| E2eError::system("parameter does not exist"))?;
        let value = match selector.version {
            Some(version) => versions.get(&version),
            None => versions.last_key_value().map(|(_, value)| value),
        };
        value
            .cloned()
            .ok_or_else(|| E2eError::system("parameter version does not exist"))
    }

    fn publish(&mut self, plan: &ConfigurationPlan, bytes: &Arc<str>) -> Result<()> {
        if plan.legacy_name != self.legacy_name || plan.legacy_arn != self.legacy_arn {
            return Err(E2eError::system(
                "publication changed the legacy parameter identity",
            ));
        }
        if !plan.generations_retained {
            self.values.retain(|name, _| *name == self.legacy_name);
        }
        if !plan.legacy_unchanged {
            let versions = self.values.entry(self.legacy_name.clone()).or_default();
            let version = versions.last_key_value().map_or(Ok(1), |(version, _)| {
                version
                    .checked_add(1)
                    .ok_or_else(|| E2eError::system("parameter version overflow"))
            })?;
            versions.insert(version, Arc::clone(bytes));
        }
        for access in std::iter::once(&plan.controller).chain(&plan.workers) {
            let selector = Selector::parse(&access.reference)?;
            self.arn(selector.name)?;
            if selector.name == self.legacy_name {
                continue;
            }
            if selector.version != Some(1) {
                return Err(E2eError::system(
                    "new generation must select immutable version one",
                ));
            }
            let versions = self.values.entry(selector.name.into()).or_default();
            match versions.get(&1) {
                Some(existing) if existing.as_ref() != bytes.as_ref() => {
                    return Err(E2eError::system(
                        "publication attempted to overwrite a generation",
                    ));
                }
                Some(_) => {}
                None => {
                    versions.insert(1, Arc::clone(bytes));
                }
            }
        }
        Ok(())
    }
}

fn generation_of(legacy: &str, name: &str) -> bool {
    name.strip_prefix(legacy)
        .is_some_and(|tail| tail.starts_with("/generations/") && tail.len() > "/generations/".len())
}

fn require_modern(plan: &ConfigurationPlan) -> Result<()> {
    if !plan.legacy_unchanged || !plan.generations_retained || plan.workers.is_empty() {
        return Err(E2eError::system(
            "candidate publication does not preserve legacy and generations",
        ));
    }
    for access in std::iter::once(&plan.controller).chain(&plan.workers) {
        let selector = Selector::parse(&access.reference)?;
        if selector.version != Some(1)
            || !generation_of(&plan.legacy_name, selector.name)
            || access.reference != plan.controller.reference
        {
            return Err(E2eError::system(
                "candidate instances do not select one immutable active generation",
            ));
        }
    }
    Ok(())
}

fn require_bytes(actual: &str, expected: &str) -> Result<()> {
    if actual != expected {
        return Err(E2eError::system(
            "selected configuration bytes differ from expected publication",
        ));
    }
    Ok(())
}

fn verify_selections(
    store: &Parameters,
    old: &ConfigurationPlan,
    candidate: &ConfigurationPlan,
    old_bytes: &str,
    candidate_bytes: &str,
) -> Result<()> {
    for role in std::iter::once(&candidate.controller).chain(&candidate.workers) {
        require_bytes(&store.fetch(&role.reference, role)?, candidate_bytes)?;
    }
    require_bytes(
        &store.fetch(&old.controller.reference, &candidate.controller)?,
        old_bytes,
    )?;
    if old.workers.is_empty() {
        return Err(E2eError::system(
            "previous fixture has no worker configuration selectors",
        ));
    }
    for old_worker in &old.workers {
        for role in &candidate.workers {
            require_bytes(&store.fetch(&old_worker.reference, role)?, old_bytes)?;
        }
    }
    Ok(())
}

fn require_executable(pid: u32, binary: &Path) -> Result<()> {
    let executable = fs::canonicalize(format!("/proc/{pid}/exe"))
        .map_err(E2eError::io("resolving restarted executable"))?;
    let expected =
        fs::canonicalize(binary).map_err(E2eError::io("resolving published executable"))?;
    if executable != expected {
        return Err(E2eError::system(
            "process is not executing the pinned previous release",
        ));
    }
    Ok(())
}

fn require_controller_command(pid: u32, config: &Path) -> Result<()> {
    let args = process::arguments(pid)
        .ok_or_else(|| E2eError::system("controller process disappeared"))?;
    if args.get(1).is_none_or(|arg| arg != "server")
        || !args
            .windows(2)
            .any(|pair| pair[0] == "--config" && pair[1] == config.as_os_str())
    {
        return Err(E2eError::system(
            "restarted controller is not using the fetched configuration path",
        ));
    }
    Ok(())
}

fn worker_pid(
    cluster: &Cluster<'_>,
    path: &Path,
    binary: &Path,
    previous: Option<u32>,
) -> Result<u32> {
    let mut observed = None;
    cluster
        .stack()
        .wait_live_state(cluster.stack().settings().startup_timeout, |state| {
            let entries = process::snapshot();
            let workers = process::workers_of(&entries, path);
            let [worker] = workers.as_slice() else {
                return false;
            };
            if Some(worker.pid) == previous || require_executable(worker.pid, binary).is_err() {
                return false;
            }
            // Local's recorded identity, never its secret, ties the process to enrollment.
            let Ok(environment) = fs::read(format!("/proc/{}/environ", worker.pid)) else {
                return false;
            };
            let instance = environment
                .split(|byte| *byte == 0)
                .find_map(|entry| entry.strip_prefix(b"GANG_LOCAL_INSTANCE="));
            if !state.workers.iter().any(|row| {
                row.phase == gang_wire::Phase::Idle
                    && !row.paused
                    && instance.is_some()
                    && instance == row.instance.strip_prefix("instances/").map(str::as_bytes)
            }) {
                return false;
            }
            observed = Some(worker.pid);
            true
        })?;
    observed.ok_or_else(|| E2eError::system("no enrolled provider-owned worker was observed"))
}

fn published_previous(invocation: &DeploymentInputs) -> Result<&crate::settings::UpgradeInputs> {
    let inputs = invocation
        .upgrade
        .as_ref()
        .ok_or_else(|| E2eError::system("configuration restart requires upgrade artifacts"))?;
    // The release executable is a checksum-pinned input, not a host tool.
    let mut command = Command::new(&inputs.previous.binary);
    command.arg("--version");
    let version = process::run(command, None, invocation.settings.command_timeout)?;
    if !version.succeeded() || version.stdout.trim().is_empty() {
        return Err(E2eError::system(
            "cannot identify the published previous release",
        ));
    }
    Ok(inputs)
}

pub(crate) fn config_refetch_restart(
    invocation: &DeploymentInputs,
    variant: &Variant,
    issuer: &TestIssuer,
) -> Result<Vec<Check>> {
    let inputs = published_previous(invocation)?;
    let old = ReleaseFixture::read(&inputs.previous.fixture)?;
    let candidate = ReleaseFixture::read(&inputs.candidate_fixture)?;
    let calibration = ReleaseFixture::read(&inputs.calibration.fixture)?;
    require_modern(&candidate.configuration)?;
    let cluster =
        Cluster::start_release(invocation, variant, issuer, &inputs.previous.binary, &old)?;
    let result = preserved_restarts(&cluster, &old, &candidate, &inputs.previous.binary);
    let mut checks = cluster.diagnose(result)?;
    drop(cluster);
    checks.push(mutable_control(
        invocation,
        variant,
        issuer,
        &calibration,
        &candidate,
    )?);
    Ok(checks)
}

fn preserved_restarts(
    cluster: &Cluster<'_>,
    old: &ReleaseFixture,
    candidate: &ReleaseFixture,
    binary: &Path,
) -> Result<Vec<Check>> {
    let path = cluster.release_config_path(0)?;
    let old_bytes: Arc<str> = fs::read_to_string(&path)
        .map_err(E2eError::io(
            "reading already adapted previous configuration",
        ))?
        .into();
    let candidate_bytes: Arc<str> = cluster.render_release_config_for(0, candidate)?.into();
    let initial_worker = worker_pid(cluster, &path, binary, None)?;
    let initial_controller = cluster.controller_pid(0)?;
    require_executable(initial_controller, binary)?;
    require_controller_command(initial_controller, &path)?;
    let mut store = Parameters::seeded(&old.configuration, &old_bytes)?;
    store.publish(&candidate.configuration, &candidate_bytes)?;
    verify_selections(
        &store,
        &old.configuration,
        &candidate.configuration,
        &old_bytes,
        &candidate_bytes,
    )?;
    let worker_access = candidate
        .configuration
        .workers
        .first()
        .ok_or_else(|| E2eError::system("candidate has no worker role"))?;
    let old_worker = old
        .configuration
        .workers
        .first()
        .ok_or_else(|| E2eError::system("previous has no worker selector"))?;
    let fetched = store.fetch(&old_worker.reference, worker_access)?;
    cluster.restart_release_worker(0, &fetched)?;
    require_installed(&path, &fetched)?;
    let replacement_worker = worker_pid(cluster, &path, binary, Some(initial_worker))?;
    let fetched = store.fetch(
        &old.configuration.controller.reference,
        &candidate.configuration.controller,
    )?;
    cluster.restart_release_controller(0, binary, &fetched)?;
    let replacement_controller = cluster.controller_pid(0)?;
    if replacement_controller == initial_controller {
        return Err(E2eError::system(
            "controller restart retained its original PID",
        ));
    }
    require_executable(replacement_controller, binary)?;
    require_controller_command(replacement_controller, &path)?;
    require_installed(&path, &fetched)?;
    Ok(vec![Check::passed(format!(
        "configuration refetch preserves exact old bytes: controller selector={:?}, worker selectors={:?}, candidate selector={:?}, config={}, old bytes={}, candidate bytes={}, all {} candidate role selections fetch exact candidate bytes, worker PID={initial_worker}->{replacement_worker}, controller PID={initial_controller}->{replacement_controller}, pinned old executable, provider identity enrolled and idle, controller active health",
        old.configuration.controller.reference,
        old.configuration
            .workers
            .iter()
            .map(|access| &access.reference)
            .collect::<Vec<_>>(),
        candidate.configuration.controller.reference,
        path.display(),
        old_bytes.len(),
        candidate_bytes.len(),
        candidate.configuration.workers.len() + 1,
    ))])
}

fn require_installed(path: &Path, fetched: &str) -> Result<()> {
    let installed = fs::read_to_string(path)
        .map_err(E2eError::io("reading atomically installed configuration"))?;
    require_bytes(&installed, fetched)
}

fn schema_rejection(log: &str) -> bool {
    log.contains("invalid configuration")
        && ["unknown field", "missing field", "unknown variant"]
            .iter()
            .any(|marker| log.contains(marker))
}

fn mutable_control(
    invocation: &DeploymentInputs,
    variant: &Variant,
    issuer: &TestIssuer,
    old: &ReleaseFixture,
    candidate: &ReleaseFixture,
) -> Result<Check> {
    if old.configuration.legacy_unchanged || old.configuration.generations_retained {
        return Err(E2eError::system(
            "negative control requires a genuine mutable calibration publication",
        ));
    }
    let inputs = invocation
        .upgrade
        .as_ref()
        .ok_or_else(|| E2eError::system("missing upgrade inputs"))?;
    // Calibration stays independently pinned as the positive previous release advances.
    let mut command = Command::new(&inputs.calibration.binary);
    command.arg("--version");
    let version = process::run(command, None, invocation.settings.command_timeout)?;
    if !version.succeeded()
        || !version
            .stdout
            .split_whitespace()
            .any(|part| part == "0.3.2")
    {
        return Err(E2eError::system(
            "mutable publication control requires published calibration 0.3.2",
        ));
    }
    let cluster =
        Cluster::start_release(invocation, variant, issuer, &inputs.calibration.binary, old)?;
    let path = cluster.release_config_path(0)?;
    let old_bytes: Arc<str> = fs::read_to_string(&path)
        .map_err(E2eError::io("reading negative-control calibration bytes"))?
        .into();
    let candidate_bytes: Arc<str> = cluster.render_release_config_for(0, candidate)?.into();
    let original_pid = cluster.controller_pid(0)?;
    require_executable(original_pid, &inputs.calibration.binary)?;
    worker_pid(&cluster, &path, &inputs.calibration.binary, None)?;
    let mut store = Parameters::seeded(&old.configuration, &old_bytes)?;
    store.publish(&old.configuration, &candidate_bytes)?;
    let fetched = store.fetch(
        &old.configuration.controller.reference,
        &old.configuration.controller,
    )?;
    require_bytes(&fetched, &candidate_bytes)?;
    let prior_log = cluster.log(0)?.len();
    if cluster
        .restart_release_controller(0, &inputs.calibration.binary, &fetched)
        .is_ok()
    {
        return Err(E2eError::system(
            "mutable publication unexpectedly restarted the old controller",
        ));
    }
    let status = cluster.wait_exit(0)?;
    let log = cluster.log(0)?;
    let startup = log.get(prior_log..).unwrap_or_default();
    if status.success() || !schema_rejection(startup) {
        return Err(E2eError::system(
            "mutable control did not produce a nonzero old-process schema rejection",
        ));
    }
    require_installed(&path, &fetched)?;
    Ok(Check::passed(format!(
        "mutable publication control: selector={:?}, exact candidate bytes={}, previous PID={original_pid}, old controller exits {status} with configuration schema rejection (not a health timeout)",
        old.configuration.controller.reference,
        fetched.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(generation: Option<&str>) -> ConfigurationPlan {
        let name = "/example/config";
        let arn = "arn:aws:ssm:eu-west-1:000000000000:parameter/example/config";
        let reference = generation.map_or_else(
            || name.into(),
            |generation| format!("{name}/generations/{generation}:1"),
        );
        let access = ConfigurationAccess {
            reference,
            readable_resources: vec![arn.into(), format!("{arn}/generations/*")],
            excluded_from_deny: vec![arn.into(), format!("{arn}/generations/*")],
        };
        ConfigurationPlan {
            legacy_name: name.into(),
            legacy_arn: arn.into(),
            controller: access.clone(),
            workers: vec![access],
            legacy_unchanged: generation.is_some(),
            generations_retained: generation.is_some(),
        }
    }

    #[test]
    fn mutable_publication_exposes_candidate_to_old_bare_selectors() {
        let mutable = plan(None);
        let mut store = Parameters::seeded(&mutable, &Arc::from("old-schema")).expect("seed");
        store
            .publish(&mutable, &Arc::from("candidate-schema"))
            .expect("publish");
        assert_eq!(
            &*store
                .fetch(&mutable.controller.reference, &mutable.controller)
                .expect("fetch"),
            "candidate-schema"
        );
        assert_eq!(
            &*store
                .fetch("/example/config:1", &mutable.controller)
                .expect("historical"),
            "old-schema"
        );
    }

    #[test]
    fn frozen_legacy_and_retained_generation_keep_old_bytes() {
        for previous in [plan(None), plan(Some("old"))] {
            let candidate = plan(Some("new"));
            let mut store = Parameters::seeded(&previous, &Arc::from("old")).expect("seed");
            store
                .publish(&candidate, &Arc::from("candidate"))
                .expect("publish");
            verify_selections(&store, &previous, &candidate, "old", "candidate")
                .expect("selection invariants");
            assert!(store.publish(&candidate, &Arc::from("overwrite")).is_err());
        }
    }

    #[test]
    fn unchanged_configuration_bytes_remain_valid_across_release_publication() {
        for previous in [plan(None), plan(Some("old"))] {
            let candidate = plan(Some("new"));
            let bytes = Arc::from("unchanged-compatible-schema");
            let mut store = Parameters::seeded(&previous, &bytes).expect("seed");
            store.publish(&candidate, &bytes).expect("publish");
            verify_selections(&store, &previous, &candidate, &bytes, &bytes)
                .expect("unchanged bytes preserve every old and candidate selection");
        }
    }

    #[test]
    fn discarded_generations_are_not_readable_and_explicit_versions_never_fall_back() {
        let previous = plan(Some("old"));
        let mut candidate = plan(Some("new"));
        candidate.generations_retained = false;
        let mut store = Parameters::seeded(&previous, &Arc::from("old")).expect("seed");
        store
            .publish(&candidate, &Arc::from("candidate"))
            .expect("publish");
        assert!(
            store
                .fetch(&previous.controller.reference, &candidate.controller)
                .is_err()
        );
        assert!(
            store
                .fetch("/example/config/generations/new:2", &candidate.controller)
                .is_err()
        );
        assert_eq!(
            &*store
                .fetch("/example/config/generations/new", &candidate.controller)
                .expect("latest"),
            "candidate"
        );
    }

    #[test]
    fn allow_and_deny_exception_both_constrain_namespace_access() {
        let candidate = plan(Some("new"));
        let store = Parameters::seeded(&candidate, &Arc::from("old")).expect("seed");
        let mut role = candidate.controller.clone();
        role.excluded_from_deny.truncate(1);
        assert!(store.fetch(&role.reference, &role).is_err());
        role = candidate.controller.clone();
        role.readable_resources.truncate(1);
        assert!(store.fetch(&role.reference, &role).is_err());
        assert!(
            store
                .fetch(
                    "/example/config-other/generations/new:1",
                    &candidate.controller
                )
                .is_err()
        );
        let arn = &candidate.legacy_arn;
        assert!(!resource_matches(
            &format!("{arn}/generations/*"),
            &format!("{arn}/generations-evil/id")
        ));
        assert!(!resource_matches(&format!("{arn}*"), arn));
    }

    #[test]
    fn every_candidate_worker_role_must_read_both_selected_and_preserved_bytes() {
        let previous = plan(None);
        let mut candidate = plan(Some("new"));
        let mut denied = candidate.controller.clone();
        denied.excluded_from_deny.truncate(1);
        candidate.workers.push(denied);
        let mut store = Parameters::seeded(&previous, &Arc::from("old")).expect("seed");
        store
            .publish(&candidate, &Arc::from("candidate"))
            .expect("publish");
        assert!(verify_selections(&store, &previous, &candidate, "old", "candidate").is_err());
        candidate.workers.pop();
        verify_selections(&store, &previous, &candidate, "old", "candidate")
            .expect("all remaining roles can read their selections");
    }

    #[test]
    fn modern_instance_selectors_must_match_one_immutable_generation() {
        let mut candidate = plan(Some("new"));
        require_modern(&candidate).expect("immutable selection");
        candidate.workers[0].reference = "/example/config".into();
        assert!(require_modern(&candidate).is_err());
        candidate.workers[0].reference = "/example/config/generations/other:1".into();
        assert!(require_modern(&candidate).is_err());
        candidate.workers[0].reference = "/example/config/generations/new:2".into();
        assert!(require_modern(&candidate).is_err());
    }

    #[test]
    fn selector_grammar_rejects_ambiguous_versions() {
        for invalid in [
            "",
            ":1",
            "/config:",
            "/config:0",
            "/config:01",
            "/config:+1",
            "/config:1:2",
            "/config:latest",
            "/bad name:1",
        ] {
            assert!(Selector::parse(invalid).is_err(), "{invalid}");
        }
        assert_eq!(
            Selector::parse("/config:2").expect("version").version,
            Some(2)
        );
        assert_eq!(Selector::parse("/config").expect("bare").version, None);
    }

    #[test]
    fn only_schema_rejections_calibrate_mutable_publication() {
        assert!(schema_rejection(
            "invalid configuration: unknown field `candidate_setting`"
        ));
        for unrelated in [
            "timeout waiting for health",
            "connection refused",
            "invalid configuration: cannot read signer",
            "unknown field without configuration failure",
        ] {
            assert!(!schema_rejection(unrelated));
        }
    }
}
