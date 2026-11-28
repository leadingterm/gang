//! Extracts deployment and configuration-publication evidence from mocked releases.
//!
//! Cleanup retention composes engine replacement actions with the applied SSM
//! resource set. The expected forgotten-instance diagnostic has no inventory.
//! Cleanup failures retain resource inventory and diagnostics, never plan or state payloads.

use std::error::Error;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::process::Command;

use base64::Engine as _;
use serde::Serialize;
use serde::ser::SerializeMap;
use serde_json::Value;

const TEST: &str = "tests/upgrade.tftest.hcl";
const MIGRATION_TEST: &str = "tests/ecs_migration.tftest.hcl";
const MIGRATION_RUNS: [&str; 10] = [
    "baseline",
    "bootstrap_plan",
    "bootstrap_apply",
    "bootstrap_converge",
    "worker_plan",
    "worker_apply",
    "worker_converge",
    "handoff_plan",
    "handoff_apply",
    "handoff_converge",
];
const RUNS: [&str; 5] = ["seed", "transition", "promote", "upgrade", "retention"];
type Result<T, E = Box<dyn Error>> = std::result::Result<T, E>;

#[derive(Serialize)]
struct Health {
    path: String,
    interval_seconds: u64,
    timeout_seconds: u64,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
}
#[derive(Serialize)]
struct Frontend {
    port: u16,
    target_port: u16,
    proxy_protocol_v2: bool,
    preserve_client_ip: Option<bool>,
    health: Health,
}
struct Controller {
    instances: u32,
    health_check_type: String,
    min_healthy_percentage: u32,
    max_healthy_percentage: u32,
    warmup_seconds: u64,
    deregistration_delay_seconds: u64,
    mode: Option<&'static str>,
    image: Option<String>,
    stop_timeout_seconds: Option<u64>,
    network_mode: Option<String>,
    circuit_breaker_enable: Option<bool>,
    circuit_breaker_rollback: Option<bool>,
    target_type: Option<String>,
}

impl Serialize for Controller {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("instances", &self.instances)?;
        map.serialize_entry("min_healthy_percentage", &self.min_healthy_percentage)?;
        map.serialize_entry("max_healthy_percentage", &self.max_healthy_percentage)?;
        map.serialize_entry(
            "deregistration_delay_seconds",
            &self.deregistration_delay_seconds,
        )?;
        if let Some(mode) = self.mode {
            map.serialize_entry("mode", mode)?;
            map.serialize_entry("image", &self.image)?;
            map.serialize_entry("stop_timeout_seconds", &self.stop_timeout_seconds)?;
            map.serialize_entry("network_mode", &self.network_mode)?;
            map.serialize_entry("circuit_breaker_enable", &self.circuit_breaker_enable)?;
            map.serialize_entry("circuit_breaker_rollback", &self.circuit_breaker_rollback)?;
            map.serialize_entry("target_type", &self.target_type)?;
        } else {
            map.serialize_entry("health_check_type", &self.health_check_type)?;
            map.serialize_entry("warmup_seconds", &self.warmup_seconds)?;
        }
        map.end()
    }
}
#[derive(Serialize)]
struct Fixture {
    gang_toml: String,
    local_config: String,
    public: Frontend,
    workers: Frontend,
    controller: Controller,
    configuration: Configuration,
    seed: SeedConfiguration,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum HandoffProtocol {
    DrainBeforeRelease,
    ReleaseBeforeDrain,
}

impl HandoffProtocol {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "drain_before_release" => Ok(Self::DrainBeforeRelease),
            "release_before_drain" => Ok(Self::ReleaseBeforeDrain),
            _ => Err("unknown handoff protocol".into()),
        }
    }
}

#[derive(Serialize)]
struct FixtureDocument<'a> {
    handoff_protocol: HandoffProtocol,
    #[serde(flatten)]
    fixture: &'a Fixture,
}

#[derive(Serialize)]
struct SeedConfiguration {
    gang_toml: String,
    configuration: Configuration,
}

#[derive(Serialize)]
struct Configuration {
    controller: Access,
    workers: Vec<Access>,
    generations_retained: bool,
}

#[derive(Serialize)]
struct Access {
    reference: String,
    readable_resources: Vec<String>,
    excluded_from_deny: Vec<String>,
}

struct Evidence {
    seed: Value,
    transition: Value,
    promote: Value,
    upgrade: Value,
    retention: Value,
}

/// Stages the source release and extracts its mocked deployment plan.
///
/// # Errors
/// Returns an error if staging, `OpenTofu`, or strict plan extraction fails.
pub fn upgrade_fixture(
    source: &Path,
    test: &Path,
    tofu: &Path,
    mirror: &Path,
    output: &Path,
    handoff_protocol: &str,
) -> Result<()> {
    let handoff_protocol = HandoffProtocol::parse(handoff_protocol)?;
    let source = fs::canonicalize(source)?;
    let test = fs::canonicalize(test)?;
    let tofu = fs::canonicalize(tofu)?;
    let mirror = fs::canonicalize(mirror)?;
    let work = tempfile::tempdir()?;
    crate::contract::stage(&source, work.path())?;
    let root = work.path().join("infra/terraform/modules/deployment");
    let ecs = stage_upgrade_test(&root, &test)?;
    let local = fs::read_to_string(work.path().join("configs/gang.local.toml"))?;
    let _: toml::Value = toml::from_str(&local)?;
    let command = || {
        let mut cmd = Command::new(&tofu);
        cmd.current_dir(&root)
            .env_clear()
            .env("HOME", work.path())
            .env("TF_IN_AUTOMATION", "1")
            .env("TF_INPUT", "0")
            .env("CHECKPOINT_DISABLE", "1")
            .env("AWS_EC2_METADATA_DISABLED", "true");
        cmd
    };
    let mut init = command();
    let mut plugin = OsString::from("-plugin-dir=");
    plugin.push(mirror);
    init.args(["init", "-backend=false", "-input=false", "-no-color"])
        .arg(plugin);
    if root.join(".terraform.lock.hcl").is_file() {
        init.arg("-lockfile=readonly");
    }
    let result = init.output()?;
    if !result.status.success() {
        return Err(format!(
            "tofu init failed: {}",
            String::from_utf8_lossy(&result.stderr)
        )
        .into());
    }
    let result = command()
        .args([
            "test",
            "-json",
            "-verbose",
            "-filter=tests/upgrade.tftest.hcl",
        ])
        .output()?;
    if !result.status.success() {
        return Err(format!(
            "tofu test failed: {}\n{}",
            String::from_utf8_lossy(&result.stderr),
            String::from_utf8_lossy(&result.stdout)
        )
        .into());
    }
    let fixture = extract(&result.stdout, local)?;
    if ecs {
        let result = command()
            .args([
                "test",
                "-json",
                "-verbose",
                "-filter=tests/ecs_migration.tftest.hcl",
            ])
            .output()?;
        if !result.status.success() {
            return Err(format!(
                "tofu migration test failed: {}\n{}",
                String::from_utf8_lossy(&result.stderr),
                String::from_utf8_lossy(&result.stdout)
            )
            .into());
        }
        validate_migration_evidence(&result.stdout, &fixture.gang_toml)?;
    }
    let document = FixtureDocument {
        handoff_protocol,
        fixture: &fixture,
    };
    fs::write(output, serde_json::to_vec(&document)?)?;
    Ok(())
}

fn stage_upgrade_test(root: &Path, test: &Path) -> Result<bool> {
    fs::create_dir_all(root.join("tests"))?;
    let mut test_content = fs::read_to_string(test)?;
    let variables = fs::read_to_string(root.join("variables.tf"))?;
    let ecs = variables.contains("variable \"controller_image_digest\"");
    if ecs {
        test_content = test_content.replace(
            "# CONTROLLER_INSTANCE_TYPE",
            "controller_instance_type = \"t4g.large\"",
        );
        test_content = test_content.replace(
            "# CONTROLLER_ECS_INPUTS",
            "controller_image_digest = \"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n  controller_stop_timeout_seconds = 2460",
        );
        test_content = test_content.replace(
            "# CONTROLLER_ECS_OVERRIDES",
            r#"override_data {
  target = module.controller.data.aws_ec2_instance_type.this
  values = { supported_architectures = ["arm64"], burstable_performance_supported = true, memory_size = 8192, default_vcpus = 2, boot_modes = ["uefi"] }
}
override_resource {
  target = module.controller.aws_ecr_repository.controller[0]
  values = { repository_url = "000000000000.dkr.ecr.eu-north-1.amazonaws.com/gang-controller", arn = "arn:aws:ecr:eu-north-1:000000000000:repository/gang-controller" }
}
override_resource {
  target = module.controller.aws_autoscaling_group.capacity[0]
  values = { arn = "arn:aws:autoscaling:eu-north-1:000000000000:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/gang-capacity" }
}
override_resource {
  target = module.controller.aws_iam_role.execution[0]
  values = { arn = "arn:aws:iam::000000000000:role/gang-ecs-execution" }
}
override_resource {
  target = module.controller.aws_ecs_cluster.controller[0]
  values = { arn = "arn:aws:ecs:eu-north-1:000000000000:cluster/gang-controller" }
}
override_resource {
  target = module.controller.aws_ecs_task_definition.controller[0]
  values = { arn = "arn:aws:ecs:eu-north-1:000000000000:task-definition/gang-controller:1" }
}
override_resource {
  target = module.load_balancer[0].aws_lb_target_group.this
  values = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/public/0000000000000001" }
}
override_resource {
  target = module.load_balancer[0].aws_lb_target_group.internal["serve"]
  values = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/serve/0000000000000002" }
}
override_resource {
  target = module.load_balancer[0].aws_lb_target_group.internal["workers"]
  values = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/workers/0000000000000003" }
}"#,
        );
    } else {
        test_content = test_content.replace(
            "# CONTROLLER_INSTANCE_TYPE",
            "controller_instance_type = \"c7i.large\"",
        );
        test_content = test_content.replace(
            "# CONTROLLER_ECS_OVERRIDES",
            r#"override_resource {
  target = module.load_balancer[0].aws_lb_target_group.this
  values = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/public/0000000000000001" }
}
override_resource {
  target = module.load_balancer[0].aws_lb_target_group.internal["serve"]
  values = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/serve/0000000000000002" }
}
override_resource {
  target = module.load_balancer[0].aws_lb_target_group.internal["workers"]
  values = { arn = "arn:aws:elasticloadbalancing:eu-north-1:000000000000:targetgroup/workers/0000000000000003" }
}"#,
        );
    }
    if ecs {
        stage_migration_test(root, &test_content)?;
    }
    test_content = test_content
        .replace("# LAUNCH_TEMPLATE_DEFAULTS", "defaults = { id = \"lt-0123456789abcdef0\" }")
        .replace("# IMAGE_OVERRIDES", r#"override_data {
  target = data.aws_ami.image
  values = {
    name = "gang-x86_64-aaaaaaaaaaaaaaaa"
    boot_mode = "uefi"
    tags = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }
    deprecation_time = ""
  }
}"#);
    fs::write(root.join(TEST), &test_content)?;
    Ok(ecs)
}

// This isolated baseline is new-source infrastructure without ECS resources,
// not a simulation of historical ASG state.
fn stage_migration_test(root: &Path, content: &str) -> Result<()> {
    let (prefix, _) = content
        .split_once("run \"seed\" {")
        .ok_or("missing seed run")?;
    let image_overrides = r#"override_data {
  target = data.aws_ami.image["072320588884/x86_64/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
  values = { id = "ami-00000000000000002", name = "new-image", boot_mode = "uefi", tags = { "ec2:SharedTag/ImageSha256" = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }, deprecation_time = "" }
}
override_data {
  target = data.aws_ami.image["072320588884/x86_64/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
  values = { id = "ami-00000000000000001", name = "old-image", boot_mode = "uefi", tags = { "ec2:SharedTag/ImageSha256" = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "ec2:SharedTag/ActionEnvironment" = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }, deprecation_time = "" }
}"#;
    let prefix = prefix
        .replace("# IMAGE_OVERRIDES", image_overrides)
        .replace(
            "# LAUNCH_TEMPLATE_DEFAULTS",
            "defaults = { id = \"lt-0123456789abcdef0\", latest_version = 2, default_version = 2 }",
        );
    let runs = r#"
run "baseline" {
  command = apply
  variables {
    gang_settings = { poll_interval = "100ms" }
    images = { x86_64 = { owner = "072320588884", sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" } }
  }
  plan_options {
    target = [module.network, module.storage, module.load_balancer, module.worker_fleet, module.controller.module.instance_role, aws_ssm_parameter.config_generation]
  }
  override_resource {
    target = module.worker_fleet.aws_launch_template.this
    values = { id = "lt-0123456789abcdef0", latest_version = 1, default_version = 1 }
  }
}
run "bootstrap_plan" {
  command = plan
  plan_options { target = [module.controller.aws_ecr_repository.controller, aws_ssm_parameter.config_generation] }
}
run "bootstrap_apply" {
  command = apply
  plan_options { target = [module.controller.aws_ecr_repository.controller, aws_ssm_parameter.config_generation] }
}
run "bootstrap_converge" {
  command = plan
  plan_options { target = [module.controller.aws_ecr_repository.controller, aws_ssm_parameter.config_generation] }
}
run "worker_plan" {
  command = plan
  plan_options { target = [module.worker_fleet.aws_launch_template.this, module.worker_fleet.aws_autoscaling_group.this, aws_ssm_parameter.config_generation] }
}
run "worker_apply" {
  command = apply
  plan_options { target = [module.worker_fleet.aws_launch_template.this, module.worker_fleet.aws_autoscaling_group.this, aws_ssm_parameter.config_generation] }
  override_resource {
    target = module.worker_fleet.aws_launch_template.this
    values = { id = "lt-0123456789abcdef0", latest_version = 2, default_version = 2 }
  }
}
run "worker_converge" {
  command = plan
  plan_options { target = [module.worker_fleet.aws_launch_template.this, module.worker_fleet.aws_autoscaling_group.this, aws_ssm_parameter.config_generation] }
}
run "handoff_plan" {
  command = plan
  plan_options { target = [module.controller.aws_ecs_service.controller, aws_ssm_parameter.config_generation] }
}
run "handoff_apply" {
  command = apply
  plan_options { target = [module.controller.aws_ecs_service.controller, aws_ssm_parameter.config_generation] }
}
run "handoff_converge" {
  command = plan
  plan_options { target = [module.controller.aws_ecs_service.controller, aws_ssm_parameter.config_generation] }
}
"#;
    fs::write(root.join(MIGRATION_TEST), format!("{prefix}{runs}"))?;
    Ok(())
}

fn validate_migration_evidence(stdout: &[u8], gang: &str) -> Result<()> {
    let mut runs = [false; 10];
    let mut payloads: [Option<Value>; 10] = std::array::from_fn(|_| None);
    let mut abstract_seen = false;
    let mut file_seen = false;
    let mut summary_seen = false;
    let mut forgotten = false;
    for line in stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut event: Value = serde_json::from_slice(line)?;
        let kind = string(&event, "type")?.to_owned();
        if forgotten && matches!(kind.as_str(), "test_run" | "test_plan" | "test_state") {
            return Err("migration run evidence follows cleanup notice".into());
        }
        if kind == "diagnostic" {
            let complete = runs.iter().all(|run| *run) && payloads.iter().all(Option::is_some);
            migration_diagnostic(&event, &mut forgotten, complete)?;
            continue;
        }
        if matches!(event["@level"].as_str(), Some("error" | "warn" | "warning")) {
            return Err("unexpected migration warning or error".into());
        }
        match kind.as_str() {
            "test_abstract" => {
                if abstract_seen
                    || event[&kind] != serde_json::json!({(MIGRATION_TEST): MIGRATION_RUNS})
                {
                    return Err("unexpected migration abstract".into());
                }
                abstract_seen = true;
            }
            "test_run" | "test_plan" | "test_state" => {
                let index = MIGRATION_RUNS
                    .iter()
                    .position(|name| event["@testrun"] == *name)
                    .ok_or("unexpected migration run")?;
                if event["@testfile"] != MIGRATION_TEST {
                    return Err("unexpected migration test file".into());
                }
                if kind == "test_run" {
                    if runs[index]
                        || event[&kind]["path"] != MIGRATION_TEST
                        || event[&kind]["run"] != MIGRATION_RUNS[index]
                        || event[&kind]["status"] != "pass"
                    {
                        return Err("invalid migration run".into());
                    }
                    runs[index] = true;
                } else {
                    let expected = if matches!(index, 0 | 2 | 5 | 8) {
                        "test_state"
                    } else {
                        "test_plan"
                    };
                    if kind != expected || payloads[index].is_some() || !event[&kind].is_object() {
                        return Err("invalid migration payload".into());
                    }
                    payloads[index] = Some(event[&kind].take());
                }
            }
            "test_file" => {
                if file_seen
                    || event["@testfile"] != MIGRATION_TEST
                    || event[&kind]["path"] != MIGRATION_TEST
                    || event[&kind]["status"] != "pass"
                {
                    return Err("invalid migration file".into());
                }
                file_seen = true;
            }
            "test_summary" => {
                let summary = &event[&kind];
                if summary_seen
                    || summary["status"] != "pass"
                    || summary["passed"] != MIGRATION_RUNS.len()
                    || summary["failed"] != 0
                    || summary["errored"] != 0
                    || summary["skipped"] != 0
                {
                    return Err("invalid migration summary".into());
                }
                summary_seen = true;
            }
            other if other.starts_with("test_") => return Err("unexpected migration event".into()),
            _ => {}
        }
    }
    if !abstract_seen
        || !file_seen
        || !summary_seen
        || !forgotten
        || !runs.into_iter().all(|run| run)
        || !payloads.iter().all(Option::is_some)
    {
        return Err("incomplete migration evidence".into());
    }
    validate_migration_payloads(&payloads, gang)
}

fn migration_diagnostic(event: &Value, forgotten: &mut bool, complete: bool) -> Result<()> {
    if event["@testfile"] != MIGRATION_TEST {
        return Err("unbound migration diagnostic".into());
    }
    let severity = event["diagnostic"]["severity"].as_str();
    let summary = event["diagnostic"]["summary"].as_str();
    if severity == Some("warning")
        && MIGRATION_RUNS.contains(&event["@testrun"].as_str().unwrap_or(""))
        && matches!(
            summary,
            Some("Resource targeting is in effect" | "Applied changes may be incomplete")
        )
    {
        return Ok(());
    }
    if !*forgotten && complete && crate::contract::is_successful_forget_notice(event) {
        *forgotten = true;
        return Ok(());
    }
    Err(format!(
        "unexpected migration diagnostic {}: {}",
        severity.unwrap_or("unknown"),
        summary.unwrap_or("unknown")
    )
    .into())
}

fn validate_migration_payloads(payloads: &[Option<Value>; 10], gang: &str) -> Result<()> {
    let get = |index: usize| payloads[index].as_ref().ok_or("missing migration payload");
    for index in [1, 3, 4, 6, 7, 9] {
        validate_target_plan_phase(get(index)?, gang, matches!(index, 3 | 6 | 9), index == 4)?;
    }
    if !has_actions(
        get(1)?,
        "aws_ssm_parameter.config_generation",
        &["forget", "create"],
    )? || !has_actions(
        get(1)?,
        "terraform_data.config_generation",
        &["delete", "create"],
    )? || !has_actions(
        get(1)?,
        "module.controller.aws_ecr_repository.controller[0]",
        &["create"],
    )? || !has_actions(
        get(7)?,
        "module.controller.aws_ecs_service.controller[0]",
        &["create"],
    )? {
        return Err("migration plans lack expected actions".into());
    }
    for index in [0, 2, 5, 8] {
        let resources = state_resources(get(index)?)?;
        let config = &resource(&resources, "aws_ssm_parameter.config_generation")?["values"];
        if (index == 0 && string(config, "value")? == gang)
            || (index != 0 && string(config, "value")? != gang)
        {
            return Err("migration applied incorrect configuration".into());
        }
        for address in [
            "module.controller.aws_ecs_service.controller[0]",
            "module.controller.aws_ecs_task_definition.controller[0]",
            "module.controller.aws_autoscaling_group.capacity[0]",
        ] {
            let exists = resources.iter().any(|r| r["address"] == address);
            if exists != (index == 8) {
                return Err(format!("migration applied {address} in wrong phase").into());
            }
        }
        let repository = resources
            .iter()
            .any(|r| r["address"] == "module.controller.aws_ecr_repository.controller[0]");
        if repository != (index != 0) {
            return Err("migration repository exists in wrong phase".into());
        }
    }
    validate_worker_transition(get(0)?, get(4)?, get(5)?)?;
    Ok(())
}

fn selected_evidence(stdout: &[u8]) -> Result<Evidence> {
    let names = &RUNS;
    let mut runs = vec![false; names.len()];
    let mut payloads = vec![None; names.len()];
    let mut summary = false;
    let mut file = false;
    let mut abstract_seen = false;
    let mut forgotten_notice = false;
    for (line_index, line) in stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        let event: Value = serde_json::from_slice(line)?;
        if forgotten_notice
            && matches!(
                event["type"].as_str(),
                Some("test_run" | "test_state" | "test_plan")
            )
        {
            return Err("run evidence follows the cleanup notice".into());
        }
        if record_diagnostic(&event, &mut forgotten_notice)
            .map_err(|error| cleanup_failure_context(stdout, error.to_string(), line_index))?
        {
            if !runs.iter().all(|run| *run) || !payloads.iter().all(Option::is_some) {
                return Err("cleanup notice precedes completed run evidence".into());
            }
            continue;
        }
        let kind = string(&event, "type")?;
        match kind {
            "test_run" => record_run(&event, names, &mut runs)?,
            "test_state" | "test_plan" => record_payload(event, names, &mut payloads)?,
            "test_summary" => {
                let s = &event[kind];
                if summary
                    || s["status"] != "pass"
                    || s["passed"] != names.len()
                    || s["failed"] != 0
                    || s["errored"] != 0
                    || s["skipped"] != 0
                {
                    return Err("unexpected test summary".into());
                }
                summary = true;
            }
            "test_file" => {
                if file
                    || event[kind]["path"] != TEST
                    || event[kind]["status"] != "pass"
                    || event["@testfile"] != TEST
                {
                    return Err("unexpected test file".into());
                }
                file = true;
            }
            "test_abstract" => {
                let expected = serde_json::json!({(TEST): names});
                if abstract_seen || event[kind] != expected {
                    return Err("unexpected test abstract".into());
                }
                abstract_seen = true;
            }
            other if other.starts_with("test_") => return Err("unexpected test event".into()),
            _ => {}
        }
    }
    if !runs.into_iter().all(|run| run) || !summary || !file || !abstract_seen {
        return Err("missing test envelope".into());
    }
    let mut take = |name: &str| {
        names
            .iter()
            .position(|candidate| *candidate == name)
            .and_then(|index| payloads[index].take())
    };
    let evidence = Evidence {
        seed: take("seed").ok_or("missing seed state")?,
        transition: take("transition").ok_or("missing transition plan")?,
        promote: take("promote").ok_or("missing promote state")?,
        upgrade: take("upgrade").ok_or("missing upgrade plan")?,
        retention: take("retention").ok_or("missing retention plan")?,
    };
    validate_retention(&evidence, forgotten_notice)?;
    Ok(evidence)
}

fn cleanup_failure_context(stdout: &[u8], mut message: String, failed_line: usize) -> String {
    for (index, line) in stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        let Ok(event) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if event["type"] == "test_cleanup" {
            if let Some(resources) = event["test_cleanup"]["failed_resources"].as_array() {
                for resource in resources {
                    if let Some(instance) = resource["instance"].as_str() {
                        message.push_str("\ncleanup failed resource: ");
                        message.push_str(instance);
                        if let Some(key) = resource["deposed_key"].as_str() {
                            message.push_str(" (deposed key: ");
                            message.push_str(key);
                            message.push(')');
                        }
                    }
                }
            }
        } else if index > failed_line
            && event["type"] == "diagnostic"
            && event["@testfile"] == TEST
            && event["@testrun"] == "promote"
            && event["diagnostic"]["severity"] == "error"
        {
            if let Some(summary) = event["diagnostic"]["summary"].as_str() {
                message.push_str("\ncleanup diagnostic: ");
                message.push_str(summary);
            }
            if let Some(detail) = event["diagnostic"]["detail"].as_str() {
                message.push('\n');
                message.push_str(detail);
            }
            if let (Some(filename), Some(line)) = (
                event["diagnostic"]["range"]["filename"].as_str(),
                event["diagnostic"]["range"]["start"]["line"].as_u64(),
            ) {
                let _ = write!(message, "\ncleanup diagnostic location: {filename}:{line}");
            }
        }
    }
    message
}

fn record_diagnostic(event: &Value, forgotten_notice: &mut bool) -> Result<bool> {
    if event["type"] == "diagnostic" {
        if *forgotten_notice
            || event["@testfile"] != TEST
            || event["@testrun"] != "promote"
            || !crate::contract::is_successful_forget_notice(event)
        {
            return Err("unexpected OpenTofu diagnostic".into());
        }
        *forgotten_notice = true;
        return Ok(true);
    }
    if matches!(event["@level"].as_str(), Some("error" | "warn" | "warning")) {
        return Err(format!(
            "OpenTofu emitted an error or warning ({}): {}",
            event["type"].as_str().unwrap_or("unknown"),
            event["@message"].as_str().unwrap_or("no message")
        )
        .into());
    }
    Ok(false)
}

fn validate_retention(evidence: &Evidence, forgotten_notice: bool) -> Result<()> {
    let resources = state_resources(&evidence.promote)?;
    resource(&resources, "aws_ssm_parameter.config_generation")?;
    if !forgotten_notice
        || !has_actions(
            &evidence.retention,
            "aws_ssm_parameter.config_generation",
            &["forget", "create"],
        )?
        || !has_actions(
            &evidence.transition,
            "aws_ssm_parameter.config_generation",
            &["forget", "create"],
        )?
    {
        return Err("cleanup retention lacks engine action evidence".into());
    }
    Ok(())
}

fn record_run(event: &Value, names: &[&str], runs: &mut [bool]) -> Result<()> {
    let status = &event["test_run"];
    let index = names
        .iter()
        .position(|name| status["run"] == *name)
        .ok_or("unexpected test run")?;
    if runs[index]
        || status["path"] != TEST
        || status["status"] != "pass"
        || event["@testfile"] != TEST
        || event["@testrun"] != names[index]
    {
        return Err("invalid or duplicate test run".into());
    }
    runs[index] = true;
    Ok(())
}

fn record_payload(mut event: Value, names: &[&str], payloads: &mut [Option<Value>]) -> Result<()> {
    let index = names
        .iter()
        .position(|name| event["@testrun"] == *name)
        .ok_or("unexpected verbose run")?;
    let expected = if matches!(names[index], "seed" | "promote") {
        "test_state"
    } else {
        "test_plan"
    };
    if event["type"] != expected
        || event["@testfile"] != TEST
        || payloads[index].is_some()
        || !event[expected].is_object()
    {
        return Err("invalid or duplicate verbose evidence".into());
    }
    payloads[index] = Some(event[expected].take());
    Ok(())
}

fn extract(stdout: &[u8], local_config: String) -> Result<Fixture> {
    let evidence = selected_evidence(stdout)?;
    let plan = &evidence.upgrade;
    let gang = plan
        .pointer("/output_changes/gang_toml/after")
        .or_else(|| plan.pointer("/planned_values/outputs/gang_toml/value"))
        .and_then(Value::as_str)
        .ok_or("missing concrete gang_toml")?;
    if gang != state_output(&evidence.promote, "gang_toml")? {
        return Err("final plan differs from promoted configuration".into());
    }
    let config: toml::Value = toml::from_str(gang)?;
    let endpoint = |path: &[&str]| -> Option<&str> {
        let mut value = &config;
        for key in path {
            value = value.get(*key)?;
        }
        value.as_str()
    };
    let public_port = endpoint_port(endpoint(&["endpoint"]).ok_or("missing endpoint")?)?;
    let worker_port =
        endpoint_port(endpoint(&["worker_endpoint"]).ok_or("missing worker endpoint")?)?;
    let mut resources = Vec::new();
    collect_resources(
        plan.pointer("/planned_values/root_module")
            .ok_or("missing planned root module")?,
        &mut resources,
    )?;
    let ecs = resources
        .iter()
        .any(|r| r["address"] == "module.controller.aws_ecs_service.controller[0]");
    let (public, public_delay) = frontend(plan, &resources, public_port, "TLS", ecs)?;
    let (workers, workers_delay) = frontend(plan, &resources, worker_port, "TCP", ecs)?;
    if ecs {
        frontend(plan, &resources, public_port, "TCP", true)?;
    }
    let deregistration_delay_seconds = public_delay.max(workers_delay);
    let configuration = configuration(&evidence, &config)?;
    let seed_gang = state_output(&evidence.seed, "gang_toml")?;
    let seed_config = toml::from_str(seed_gang)?;
    let seed = SeedConfiguration {
        gang_toml: seed_gang.into(),
        configuration: state_configuration(
            &evidence.seed,
            &seed_config,
            configuration.generations_retained,
        )?,
    };
    Ok(Fixture {
        gang_toml: gang.into(),
        local_config,
        public,
        workers,
        controller: controller(plan, &resources, deregistration_delay_seconds, &config)?,
        configuration,
        seed,
    })
}

#[cfg(test)]
fn validate_target_plan(plan: &Value, gang: &str, convergence: bool) -> Result<()> {
    validate_target_plan_phase(plan, gang, convergence, false)
}

fn validate_target_plan_phase(
    plan: &Value,
    gang: &str,
    convergence: bool,
    worker_update: bool,
) -> Result<()> {
    let changes = plan["resource_changes"]
        .as_array()
        .ok_or("missing targeted resource changes")?;
    let mut seen = std::collections::HashSet::new();
    let mut config_seen = false;
    for resource in changes {
        let address = string(resource, "address")?;
        if !seen.insert(address) {
            return Err("duplicate targeted resource change".into());
        }
        let actions = resource["change"]["actions"]
            .as_array()
            .ok_or("missing targeted actions")?;
        let is = |expected: &[&str]| {
            actions.len() == expected.len()
                && actions
                    .iter()
                    .zip(expected)
                    .all(|(a, b)| a.as_str() == Some(*b))
        };
        let protected = address.starts_with("module.worker_fleet.")
            || address.contains(".aws_lb_listener.")
            || address.contains(".aws_lb_target_group.");
        let worker_version_update = worker_update
            && is(&["update"])
            && (address.starts_with("module.worker_fleet.aws_launch_template.this[")
                && worker_template_image_only(&resource["change"])?
                || address.starts_with("module.worker_fleet.aws_autoscaling_group.this[")
                    && worker_asg_version_only(&resource["change"])?);
        let unchanged = is(&["no-op"]) || (resource["mode"] == "data" && is(&["read"]));
        if protected && !(worker_version_update || unchanged) {
            return Err(format!("targeted plan mutates protected resource {address}").into());
        }
        if convergence && !is(&["no-op"]) && !(resource["mode"] == "data" && is(&["read"])) {
            return Err(format!("targeted plan did not converge for {address}").into());
        }
        if !(worker_version_update
            || is(&["no-op"])
            || is(&["create"])
            || (resource["mode"] == "data" && is(&["read"]))
            || (address == "terraform_data.config_generation" && is(&["delete", "create"]))
            || (address == "aws_ssm_parameter.config_generation" && is(&["forget", "create"])))
        {
            return Err(format!("unsafe targeted action for {address}").into());
        }
        if address == "aws_ssm_parameter.config_generation" {
            config_seen = true;
            let change = &resource["change"];
            if change["after"]["value"].as_str() != Some(gang)
                || change["after_unknown"]
                    .as_object()
                    .is_none_or(|unknown| unknown.get("value").is_some_and(|value| value != false))
            {
                return Err(
                    "targeted configuration value is unknown or differs from generated config"
                        .into(),
                );
            }
        }
    }
    if !config_seen {
        return Err("targeted plan omits configuration generation".into());
    }
    Ok(())
}

fn worker_template_image_only(change: &Value) -> Result<bool> {
    let mut before = change["before"].clone();
    let mut after = change["after"].clone();
    let old = string(&before, "image_id")?.to_owned();
    let new = string(&after, "image_id")?;
    if old.is_empty() || new.is_empty() || old == new {
        return Ok(false);
    }
    for field in ["image_id", "user_data", "latest_version", "default_version"] {
        before
            .as_object_mut()
            .ok_or("invalid old worker template")?
            .remove(field);
        after
            .as_object_mut()
            .ok_or("invalid new worker template")?
            .remove(field);
    }
    Ok(before == after)
}

fn worker_asg_version_only(change: &Value) -> Result<bool> {
    const VERSION: &str =
        "/mixed_instances_policy/0/launch_template/0/launch_template_specification/0/version";
    let mut before = change["before"].clone();
    let mut after = change["after"].clone();
    let old = before
        .pointer(VERSION)
        .and_then(Value::as_str)
        .ok_or("missing old worker ASG version")?
        .to_owned();
    let new = after.pointer(VERSION).and_then(Value::as_str);
    if new == Some(old.as_str()) {
        return Ok(false);
    }
    *before
        .pointer_mut(VERSION)
        .ok_or("missing old worker ASG version")? = Value::Null;
    *after
        .pointer_mut(VERSION)
        .ok_or("missing new worker ASG version")? = Value::Null;
    Ok(before == after)
}

fn validate_worker_transition(baseline: &Value, plan: &Value, applied: &Value) -> Result<()> {
    let old = state_resources(baseline)?;
    let new = state_resources(applied)?;
    let templates: Vec<_> = old
        .iter()
        .filter(|r| {
            r["address"]
                .as_str()
                .is_some_and(|a| a.starts_with("module.worker_fleet.aws_launch_template.this["))
        })
        .collect();
    let groups: Vec<_> = old
        .iter()
        .filter(|r| {
            r["address"]
                .as_str()
                .is_some_and(|a| a.starts_with("module.worker_fleet.aws_autoscaling_group.this["))
        })
        .collect();
    if templates.is_empty() || groups.is_empty() {
        return Err("missing baseline worker templates or groups".into());
    }
    for template in templates {
        let address = string(template, "address")?;
        let before = &template["values"];
        let after = &resource(&new, address)?["values"];
        let old_image = string(before, "image_id")?;
        let new_image = string(after, "image_id")?;
        let old_version = before["latest_version"]
            .as_i64()
            .ok_or("missing old worker template version")?;
        let new_version = after["latest_version"]
            .as_i64()
            .ok_or("missing new worker template version")?;
        if string(before, "id")? != string(after, "id")?
            || old_image.is_empty()
            || new_image.is_empty()
            || old_image == new_image
            || new_version <= old_version
            || !has_actions(plan, address, &["update"])?
        {
            return Err(
                format!("worker template did not update its image in place: {address}").into(),
            );
        }
    }
    for group in groups {
        let address = string(group, "address")?;
        let after = &resource(&new, address)?["values"];
        let before = &group["values"];
        let old_specification = before
            .pointer("/mixed_instances_policy/0/launch_template/0/launch_template_specification/0")
            .ok_or("missing old worker ASG launch template")?;
        let specification = after
            .pointer("/mixed_instances_policy/0/launch_template/0/launch_template_specification/0")
            .ok_or("missing worker ASG launch template")?;
        let id = string(specification, "launch_template_id")?;
        let version = string(specification, "version")?;
        let template =
            new.iter()
                .find(|r| {
                    r["address"].as_str().is_some_and(|a| {
                        a.starts_with("module.worker_fleet.aws_launch_template.this[")
                    }) && r["values"]["id"] == id
                })
                .ok_or("worker ASG does not reference an updated template")?;
        let latest = template["values"]["latest_version"]
            .as_i64()
            .ok_or("missing worker template version")?;
        if string(old_specification, "launch_template_id")? != id
            || string(old_specification, "version")? == version
            || version != latest.to_string()
            || !has_actions(plan, address, &["update"])?
        {
            return Err(format!("worker ASG did not adopt the latest template: {address}").into());
        }
    }
    Ok(())
}

fn state_resources(state: &Value) -> Result<Vec<&Value>> {
    let mut resources = Vec::new();
    collect_resources(
        state
            .pointer("/values/root_module")
            .ok_or("missing state root")?,
        &mut resources,
    )?;
    Ok(resources)
}

fn resource<'a>(resources: &[&'a Value], address: &str) -> Result<&'a Value> {
    let mut matches = resources.iter().filter(|r| r["address"] == address);
    let value = matches
        .next()
        .ok_or_else(|| format!("missing resource {address}"))?;
    if matches.next().is_some() {
        return Err(format!("duplicate resource {address}").into());
    }
    Ok(value)
}

fn state_output<'a>(state: &'a Value, name: &str) -> Result<&'a str> {
    string(&state["values"]["outputs"][name], "value")
}

fn has_actions(plan: &Value, address: &str, expected: &[&str]) -> Result<bool> {
    let changes = plan["resource_changes"]
        .as_array()
        .ok_or("missing resource changes")?;
    let mut matches = changes.iter().filter(|r| r["address"] == address);
    let change = matches.next().ok_or("missing transition change")?;
    if matches.next().is_some() {
        return Err("duplicate transition change".into());
    }
    let actions = change["change"]["actions"]
        .as_array()
        .ok_or("missing actions")?;
    if actions.is_empty()
        || actions.iter().any(|action| {
            !matches!(
                action.as_str(),
                Some("no-op" | "read" | "create" | "update" | "delete" | "forget")
            )
        })
    {
        return Err("unknown transition actions".into());
    }
    Ok(actions.len() == expected.len()
        && actions
            .iter()
            .zip(expected)
            .all(|(actual, expected)| actual.as_str() == Some(*expected)))
}

fn configuration(evidence: &Evidence, config: &toml::Value) -> Result<Configuration> {
    let seed = state_resources(&evidence.seed)?;
    let promoted = state_resources(&evidence.promote)?;
    if state_output(&evidence.seed, "gang_toml")? == state_output(&evidence.promote, "gang_toml")? {
        return Err("configuration transition did not change rendered bytes".into());
    }
    let retained = generation_retention(evidence, &seed, &promoted)?;
    state_configuration(&evidence.promote, config, retained)
}

fn state_configuration(
    state: &Value,
    config: &toml::Value,
    generations_retained: bool,
) -> Result<Configuration> {
    let resources = state_resources(state)?;
    let active = state_output(state, "config_parameter_reference")?;
    let generation = &resource(&resources, "aws_ssm_parameter.config_generation")?["values"];
    let arn = string(generation, "arn")?;
    let (arn_prefix, _) = arn
        .split_once(":parameter/")
        .ok_or("invalid parameter ARN")?;
    let name = string(generation, "name")?;
    let (name_prefix, _) = name
        .rsplit_once("/generations/")
        .ok_or("invalid generation name")?;
    let namespace = format!("{arn_prefix}:parameter{name_prefix}/generations/*");
    validate_reference(active, &resources, state_output(state, "gang_toml")?)?;
    let controller = if resources
        .iter()
        .any(|r| r["address"] == "module.controller.aws_ecs_task_definition.controller[0]")
    {
        ecs_access(&resources, active, &namespace, arn_prefix)?
    } else {
        role_access(
            &resources,
            "module.controller",
            "controller",
            active,
            &namespace,
        )?
    };
    let workers = worker_access(&resources, config, active, &namespace)?;
    Ok(Configuration {
        controller,
        workers,
        generations_retained,
    })
}

fn worker_access(
    resources: &[&Value],
    config: &toml::Value,
    active: &str,
    namespace: &str,
) -> Result<Vec<Access>> {
    let fleets = config
        .get("fleets")
        .and_then(toml::Value::as_array)
        .ok_or("missing rendered worker fleets")?;
    if fleets.is_empty() {
        return Err("missing worker launch templates".into());
    }
    let mut workers = Vec::with_capacity(fleets.len());
    for fleet in fleets {
        let name = fleet
            .get("name")
            .and_then(toml::Value::as_str)
            .ok_or("missing fleet name")?;
        let address = format!(
            "module.worker_fleet.aws_launch_template.this[{}]",
            serde_json::to_string(name)?
        );
        let template = resource(resources, &address)?;
        let mut access =
            instance_access(template, resources, "module.worker_fleet", "worker", active)?;
        validate_namespace(&mut access, namespace)?;
        workers.push(access);
    }
    let actual = resources
        .iter()
        .filter(|r| {
            r["type"] == "aws_launch_template"
                && r["address"]
                    .as_str()
                    .is_some_and(|a| a.starts_with("module.worker_fleet."))
        })
        .count();
    if actual != workers.len() {
        return Err("unaccounted worker launch template".into());
    }
    Ok(workers)
}

fn generation_retention(evidence: &Evidence, seed: &[&Value], promoted: &[&Value]) -> Result<bool> {
    let old = &resource(seed, "aws_ssm_parameter.config_generation")?["values"];
    let new = &resource(promoted, "aws_ssm_parameter.config_generation")?["values"];
    let old_name = string(old, "name")?;
    let new_name = string(new, "name")?;
    let (prefix, _) = old_name
        .rsplit_once("/generations/")
        .ok_or("invalid generation name")?;
    for (resources, parameter) in [(seed, old), (promoted, new)] {
        let id = string(
            &resource(resources, "terraform_data.config_generation")?["values"],
            "id",
        )?;
        if id.len() != 36
            || !id.bytes().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    b == b'-'
                } else {
                    b.is_ascii_hexdigit()
                }
            })
            || string(parameter, "name")? != format!("{prefix}/generations/{id}")
        {
            return Err("missing concrete generation UUID".into());
        }
    }
    if string(old, "value")? != state_output(&evidence.seed, "gang_toml")?
        || string(new, "value")? != state_output(&evidence.promote, "gang_toml")?
        || string(old, "value")? == string(new, "value")?
        || old_name == new_name
    {
        return Err("generation does not publish changed configuration".into());
    }
    let retained = has_actions(
        &evidence.transition,
        "aws_ssm_parameter.config_generation",
        &["forget", "create"],
    )?;
    Ok(retained && old["overwrite"] == false && new["overwrite"] == false)
}

fn validate_reference(reference: &str, resources: &[&Value], gang: &str) -> Result<()> {
    let (name, version) = reference
        .rsplit_once(':')
        .ok_or("missing configuration version")?;
    if version.parse::<u64>()? != 1
        || string(
            &resource(resources, "aws_ssm_parameter.config_generation")?["values"],
            "name",
        )? != name
    {
        return Err("selector does not resolve the active generation".into());
    }
    let mut parameters = resources
        .iter()
        .filter(|r| r["type"] == "aws_ssm_parameter" && r["values"]["name"] == name);
    let parameter = parameters
        .next()
        .ok_or("unfetchable configuration selector")?;
    if parameters.next().is_some()
        || parameter["values"]["version"].as_u64() != Some(1)
        || string(&parameter["values"], "value")? != gang
    {
        return Err("unfetchable or stale configuration version".into());
    }
    Ok(())
}

fn role_access(
    resources: &[&Value],
    module: &str,
    role: &str,
    active: &str,
    namespace: &str,
) -> Result<Access> {
    let prefix = format!("{module}.");
    let mut instances = resources.iter().filter(|r| {
        (r["type"] == "aws_launch_template" || r["type"] == "aws_instance")
            && r["address"]
                .as_str()
                .is_some_and(|a| a.starts_with(&prefix))
    });
    let instance = instances
        .next()
        .ok_or("missing controller launch configuration")?;
    if instances.next().is_some() {
        return Err("missing or ambiguous controller launch configuration".into());
    }
    let mut access = instance_access(instance, resources, module, role, active)?;
    validate_namespace(&mut access, namespace)?;
    Ok(access)
}

fn ecs_container(resources: &[&Value]) -> Result<Value> {
    let task = &resource(
        resources,
        "module.controller.aws_ecs_task_definition.controller[0]",
    )?["values"];
    let containers: Value = serde_json::from_str(string(task, "container_definitions")?)?;
    let mut matching = containers
        .as_array()
        .ok_or("missing containers")?
        .iter()
        .filter(|container| container["name"] == "controller");
    let container = matching.next().ok_or("missing controller container")?;
    if matching.next().is_some() {
        return Err("ambiguous controller container".into());
    }
    Ok(container.clone())
}

fn ecs_access(
    resources: &[&Value],
    active: &str,
    namespace: &str,
    arn_prefix: &str,
) -> Result<Access> {
    let task = &resource(
        resources,
        "module.controller.aws_ecs_task_definition.controller[0]",
    )?["values"];
    let execution = &resource(resources, "module.controller.aws_iam_role.execution[0]")?["values"];
    let attached = &resource(
        resources,
        "module.controller.aws_iam_role_policy.execution_secrets[0]",
    )?["values"];
    let policy = &resource(
        resources,
        "module.controller.data.aws_iam_policy_document.execution_secrets",
    )?["values"];
    if string(task, "execution_role_arn")? != string(execution, "arn")?
        || string(attached, "role")? != string(execution, "id")?
        || string(attached, "policy")? != string(policy, "json")?
    {
        return Err("controller task execution policy binding mismatch".into());
    }
    let container = ecs_container(resources)?;
    let mut selectors = container["secrets"]
        .as_array()
        .ok_or("missing ECS secrets")?
        .iter()
        .filter(|secret| secret["name"] == "GANG_CONFIG");
    let selector = selectors
        .next()
        .ok_or("missing ECS configuration selector")?;
    let name = active.split_once(':').map_or(active, |(name, _)| name);
    if selectors.next().is_some()
        || string(selector, "valueFrom")? != format!("{arn_prefix}:parameter{name}")
    {
        return Err("ECS configuration selector does not use active generation".into());
    }
    let statements = policy["statement"]
        .as_array()
        .ok_or("missing execution IAM statements")?;
    let mut readable_resources = policy_resources(
        statements,
        "ReadParameters",
        "Allow",
        "resources",
        &["ssm:GetParameters"],
    )?;
    if !readable_resources
        .iter()
        .any(|resource| resource == namespace)
    {
        return Err("execution role cannot read generation namespace".into());
    }
    readable_resources.retain(|resource| resource == namespace);
    Ok(Access {
        reference: active.into(),
        readable_resources,
        excluded_from_deny: Vec::new(),
    })
}

fn instance_access(
    instance: &Value,
    resources: &[&Value],
    module: &str,
    role: &str,
    active: &str,
) -> Result<Access> {
    let data = string(&instance["values"], "user_data")?;
    let decoded = if instance["type"] == "aws_launch_template" {
        decode_base64(data)?
    } else {
        data.into()
    };
    let mut words = decoded
        .lines()
        .next()
        .ok_or("empty user data")?
        .split_whitespace();
    if words.next() != Some(role) {
        return Err("unexpected gang-config role".into());
    }
    let reference = words
        .next()
        .ok_or("missing gang-config first-line selector")?;
    if reference != active {
        return Err("launch configuration does not use active output selector".into());
    }
    let profile = &resource(
        resources,
        &format!("{module}.module.instance_role.aws_iam_instance_profile.this"),
    )?["values"];
    validate_profile_binding(instance, profile)?;
    let policy = resource(
        resources,
        &format!("{module}.module.instance_role.data.aws_iam_policy_document.base"),
    )?;
    let statements = policy["values"]["statement"]
        .as_array()
        .ok_or("missing configured IAM statements")?;
    let readable_resources = policy_resources(
        statements,
        "ReadParameters",
        "Allow",
        "resources",
        &["ssm:GetParameter"],
    )?;
    let excluded_from_deny = policy_resources(
        statements,
        "DenyOtherParameters",
        "Deny",
        "not_resources",
        &[
            "ssm:GetParameter",
            "ssm:GetParameters",
            "ssm:GetParametersByPath",
            "ssm:GetParameterHistory",
        ],
    )?;
    Ok(Access {
        reference: reference.into(),
        readable_resources,
        excluded_from_deny,
    })
}

fn validate_profile_binding(instance: &Value, profile: &Value) -> Result<()> {
    let (attached, field) = if instance["type"] == "aws_launch_template" {
        let block = single(&instance["values"]["iam_instance_profile"])?;
        match (block.get("name"), block.get("arn")) {
            (Some(Value::String(name)), None | Some(Value::Null)) if !name.is_empty() => {
                (name.as_str(), "name")
            }
            (None | Some(Value::Null), Some(Value::String(arn))) if !arn.is_empty() => {
                (arn.as_str(), "arn")
            }
            _ => {
                return Err(
                    "missing, malformed, or ambiguous launch template instance profile".into(),
                );
            }
        }
    } else {
        (string(&instance["values"], "iam_instance_profile")?, "name")
    };
    if attached != string(profile, field)? {
        return Err("launch configuration uses a different instance role".into());
    }
    Ok(())
}

fn policy_resources(
    statements: &[Value],
    sid: &str,
    effect: &str,
    field: &str,
    actions: &[&str],
) -> Result<Vec<String>> {
    let mut matches = statements.iter().filter(|s| s["sid"] == sid);
    let statement = matches
        .next()
        .ok_or("missing configured parameter policy")?;
    let actual = statement["actions"]
        .as_array()
        .ok_or("missing IAM actions")?;
    if matches.next().is_some()
        || statement["effect"] != effect
        || actual.len() != actions.len()
        || !actions
            .iter()
            .all(|action| actual.iter().any(|a| a.as_str() == Some(*action)))
    {
        return Err("unexpected configured parameter policy".into());
    }
    statement[field]
        .as_array()
        .ok_or("missing IAM resources")?
        .iter()
        .map(|r| {
            r.as_str()
                .filter(|r| !r.is_empty())
                .map(String::from)
                .ok_or_else(|| "nonconcrete IAM resource".into())
        })
        .collect()
}

fn validate_namespace(access: &mut Access, namespace: &str) -> Result<()> {
    if !access
        .readable_resources
        .iter()
        .any(|resource| resource == namespace)
        || !access
            .excluded_from_deny
            .iter()
            .any(|resource| resource == namespace)
    {
        return Err("generation namespace lacks explicit allow and deny exemption".into());
    }
    access
        .readable_resources
        .retain(|resource| resource == namespace);
    access
        .excluded_from_deny
        .retain(|resource| resource == namespace);
    Ok(())
}

fn decode_base64(encoded: &str) -> Result<String> {
    Ok(String::from_utf8(
        base64::engine::general_purpose::STANDARD.decode(encoded)?,
    )?)
}
fn controller(
    plan: &Value,
    resources: &[&Value],
    deregistration_delay_seconds: u64,
    config: &toml::Value,
) -> Result<Controller> {
    if resources
        .iter()
        .any(|r| r["address"] == "module.controller.aws_ecs_service.controller[0]")
    {
        return ecs_controller(resources, deregistration_delay_seconds, config);
    }
    let asgs: Vec<_> = resources
        .iter()
        .filter(|r| {
            r["type"] == "aws_autoscaling_group"
                && r["address"].as_str().is_some_and(|a| {
                    a.starts_with("module.controller.") && a.contains(".aws_autoscaling_group.")
                })
        })
        .collect();
    if asgs.len() != 1 {
        return Err("missing or ambiguous controller autoscaling group".into());
    }
    let v = &asgs[0]["values"];
    if number(v, "min_size")? != 1
        || number(v, "desired_capacity")? != 1
        || number(v, "max_size")? != 2
    {
        return Err("unexpected controller capacity".into());
    }
    let health_type = string(v, "health_check_type")?;
    if health_type != "EC2" && health_type != "ELB" {
        return Err("unknown controller health check type".into());
    }
    let prefs = single(&v["instance_refresh"])?;
    let prefs = single(&prefs["preferences"])?;
    let controller = Controller {
        instances: 1,
        health_check_type: health_type.into(),
        min_healthy_percentage: number(prefs, "min_healthy_percentage")?.try_into()?,
        max_healthy_percentage: number(prefs, "max_healthy_percentage")?.try_into()?,
        warmup_seconds: warmup_seconds(plan, asgs[0], prefs)?,
        deregistration_delay_seconds,
        mode: None,
        image: None,
        stop_timeout_seconds: None,
        network_mode: None,
        circuit_breaker_enable: None,
        circuit_breaker_rollback: None,
        target_type: None,
    };
    if controller.min_healthy_percentage != 100 || controller.max_healthy_percentage != 200 {
        return Err("unexpected controller refresh percentages".into());
    }
    Ok(controller)
}

fn ecs_controller(
    resources: &[&Value],
    deregistration_delay_seconds: u64,
    config: &toml::Value,
) -> Result<Controller> {
    let service =
        &resource(resources, "module.controller.aws_ecs_service.controller[0]")?["values"];
    let task = &resource(
        resources,
        "module.controller.aws_ecs_task_definition.controller[0]",
    )?["values"];
    let capacity = &resource(
        resources,
        "module.controller.aws_launch_template.capacity[0]",
    )?["values"];
    let breaker = single(&service["deployment_circuit_breaker"])?;
    let instances = number(service, "desired_count")?;
    let min = number(service, "deployment_minimum_healthy_percent")?;
    let max = number(service, "deployment_maximum_percent")?;
    let network = string(task, "network_mode")?;
    if instances != 1
        || min != 100
        || max != 200
        || network != "host"
        || breaker["enable"] != true
        || breaker["rollback"] != true
    {
        return Err("invalid ECS deployment policy".into());
    }
    let container = ecs_container(resources)?;
    let image = string(&container, "image")?;
    let (repository, digest) = image
        .rsplit_once("@sha256:")
        .ok_or("image is not digest pinned")?;
    if repository.is_empty()
        || repository.chars().any(|c| c == '@' || c.is_whitespace())
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("malformed ECS image digest".into());
    }
    let user_data = decode_base64(string(capacity, "user_data")?)?;
    let mut budgets = user_data
        .lines()
        .filter_map(|line| line.trim().strip_prefix("ECS_CONTAINER_STOP_TIMEOUT="));
    let raw = budgets.next().ok_or("missing ECS host stop budget")?;
    if budgets.next().is_some() {
        return Err("ambiguous ECS host stop budget".into());
    }
    let host_timeout_seconds: u64 = raw
        .strip_suffix('s')
        .ok_or("invalid ECS host stop budget")?
        .parse()?;
    if container.get("stopTimeout").is_some() || host_timeout_seconds == 0 {
        return Err("ECS task and host stop budgets are invalid".into());
    }
    if config
        .get("server")
        .and_then(|server| server.get("stop_timeout"))
        .and_then(toml::Value::as_str)
        != Some(format!("{host_timeout_seconds}s").as_str())
    {
        return Err("ECS host stop budget differs from controller shutdown timeout".into());
    }
    Ok(Controller {
        instances: instances.try_into()?,
        health_check_type: String::new(),
        min_healthy_percentage: min.try_into()?,
        max_healthy_percentage: max.try_into()?,
        warmup_seconds: 0,
        deregistration_delay_seconds,
        mode: Some("ecs"),
        image: Some(image.into()),
        stop_timeout_seconds: Some(host_timeout_seconds),
        network_mode: Some(network.into()),
        circuit_breaker_enable: Some(true),
        circuit_breaker_rollback: Some(true),
        target_type: Some("instance".into()),
    })
}

fn warmup_seconds(plan: &Value, resource: &Value, preferences: &Value) -> Result<u64> {
    let group = &resource["values"];
    if let Some(seconds) = preference_warmup(plan, resource, preferences)? {
        return Ok(seconds);
    }
    if let Some(seconds) = nonnegative_seconds(group, "default_instance_warmup")? {
        return Ok(seconds);
    }
    if group.get("health_check_grace_period") == Some(&Value::Null) {
        known_omitted_field(plan, resource, "health_check_grace_period")?;
        // AWS provider 6.72.0 autoscaling/group.go declares Default(300).
        return Ok(300);
    }
    number(group, "health_check_grace_period")
}

fn preference_warmup(plan: &Value, resource: &Value, preferences: &Value) -> Result<Option<u64>> {
    known_preference_warmup(plan, resource)?;
    let field = preferences.get("instance_warmup");
    if matches!(field, None | Some(Value::Null)) || field.and_then(Value::as_str) == Some("") {
        known_unset_preference_warmup(plan, resource)?;
        return Ok(None);
    }
    // AWS 6.72.0 uses nullable.TypeNullableInt (a string), then ValueInt32.
    // Its preference validator accepts zero but not a negative sentinel.
    let seconds = if let Some(value) = field.and_then(Value::as_str) {
        u64::try_from(value.parse::<i32>()?)?
    } else {
        let seconds = number(preferences, "instance_warmup")?;
        i32::try_from(seconds)?;
        seconds
    };
    Ok(Some(seconds))
}

fn known_preference_warmup(plan: &Value, resource: &Value) -> Result<()> {
    let address = string(resource, "address")?;
    let changes = plan["resource_changes"]
        .as_array()
        .ok_or("missing resource changes")?;
    let mut matches = changes.iter().filter(|change| change["address"] == address);
    let change = matches.next().ok_or("missing warmup resource change")?;
    if matches.next().is_some() {
        return Err("duplicate warmup resource change".into());
    }
    let mut block = &change["change"]["after_unknown"];
    for key in ["instance_refresh", "preferences"] {
        let object = block.as_object().ok_or("unknown warmup block")?;
        match object.get(key) {
            None | Some(Value::Bool(false)) => return Ok(()),
            Some(value) => block = single(value)?,
        }
    }
    let object = block.as_object().ok_or("unknown warmup preferences")?;
    if object
        .get("instance_warmup")
        .is_some_and(|value| value.as_bool() != Some(false))
    {
        return Err("unknown instance_warmup".into());
    }
    Ok(())
}

fn known_unset_preference_warmup(plan: &Value, resource: &Value) -> Result<()> {
    let address = string(resource, "address")?;
    let local = address
        .strip_prefix("module.controller.")
        .ok_or("unrecognized warmup resource path")?;
    let local = local
        .split('[')
        .next()
        .ok_or("missing warmup resource name")?;
    let configured =
        plan["configuration"]["root_module"]["module_calls"]["controller"]["module"]["resources"]
            .as_array()
            .ok_or("missing warmup source configuration")?;
    let mut matches = configured
        .iter()
        .filter(|resource| resource["address"] == local);
    let configured = matches.next().ok_or("missing warmup source declaration")?;
    if matches.next().is_some() {
        return Err("duplicate warmup source declaration".into());
    }
    let refresh = single(&configured["expressions"]["instance_refresh"])?;
    let preferences = single(&refresh["preferences"])?
        .as_object()
        .ok_or("missing warmup source preferences")?;
    let Some(expression) = preferences.get("instance_warmup") else {
        return Ok(());
    };
    if expression.get("references").is_none()
        && (matches!(expression.get("constant_value"), Some(Value::Null))
            || expression.get("constant_value").and_then(Value::as_str) == Some(""))
    {
        return Ok(());
    }
    Err("unset instance_warmup is not proven by source configuration".into())
}

fn nonnegative_seconds(value: &Value, key: &str) -> Result<Option<u64>> {
    let field = &value[key];
    if field.is_null() || field.as_i64().is_some_and(|n| n < 0) {
        return Ok(None);
    }
    number(value, key).map(Some)
}

fn provider_seconds(value: &Value, key: &str) -> Result<u64> {
    if let Some(seconds) = value[key].as_str() {
        return Ok(seconds.parse()?);
    }
    number(value, key)
}

fn optional_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::String(value) if value == "true" => Some(true),
        Value::String(value) if value == "false" => Some(false),
        _ => None,
    }
}

fn endpoint_port(endpoint: &str) -> Result<u16> {
    let url = url::Url::parse(endpoint)?;
    if url.host_str().is_none() {
        return Err("endpoint has no host".into());
    }
    url.port_or_known_default()
        .ok_or_else(|| "endpoint has no port".into())
}
fn collect_resources<'a>(module: &'a Value, out: &mut Vec<&'a Value>) -> Result<()> {
    if let Some(resources) = module.get("resources") {
        out.extend(resources.as_array().ok_or("invalid resources")?);
    }
    if let Some(children) = module.get("child_modules") {
        for child in children.as_array().ok_or("invalid child modules")? {
            collect_resources(child, out)?;
        }
    }
    Ok(())
}
fn number(v: &Value, key: &str) -> Result<u64> {
    v[key]
        .as_u64()
        .ok_or_else(|| format!("missing concrete integer {key}").into())
}
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing concrete string {key}").into())
}
fn single(v: &Value) -> Result<&Value> {
    let values = v.as_array().ok_or("missing block")?;
    if values.len() != 1 {
        return Err("ambiguous block".into());
    }
    Ok(&values[0])
}

fn proxy_protocol_v2(plan: &Value, target: &Value) -> Result<bool> {
    match target["values"].get("proxy_protocol_v2") {
        Some(Value::Bool(value)) => return Ok(*value),
        Some(Value::Null) => {}
        _ => return Err("missing concrete proxy_protocol_v2".into()),
    }
    known_omitted_field(plan, target, "proxy_protocol_v2")?;
    // AWS provider 6.72.0 target_group.go declares Optional + Default(false), not
    // Computed. OpenTofu's mock composer leaves omitted non-computed fields null.
    Ok(false)
}

fn known_omitted_field(plan: &Value, target: &Value, field: &str) -> Result<()> {
    let address = string(target, "address")?;
    let (module, local) = if let Some(local) = address.strip_prefix("module.load_balancer[0].") {
        ("load_balancer", local)
    } else if let Some(local) = address.strip_prefix("module.controller.") {
        ("controller", local)
    } else {
        return Err("unrecognized resource configuration path".into());
    };
    let local = local.split('[').next().ok_or("missing resource name")?;
    let configured =
        plan["configuration"]["root_module"]["module_calls"][module]["module"]["resources"]
            .as_array()
            .ok_or("missing resource source configuration")?;
    let mut matches = configured
        .iter()
        .filter(|resource| resource["address"] == local);
    let resource = matches
        .next()
        .ok_or("missing resource source declaration")?;
    if matches.next().is_some()
        || resource["expressions"]
            .as_object()
            .ok_or("missing resource expressions")?
            .contains_key(field)
    {
        return Err(format!("configured {field} is not concrete").into());
    }
    let changes = plan["resource_changes"]
        .as_array()
        .ok_or("missing resource changes")?;
    let mut matches = changes
        .iter()
        .filter(|resource| resource["address"] == address);
    let change = matches.next().ok_or("missing resource change")?;
    let unknown = change["change"]["after_unknown"]
        .as_object()
        .ok_or("missing resource unknown-value evidence")?;
    if matches.next().is_some()
        || unknown
            .get(field)
            .is_some_and(|value| value.as_bool() != Some(false))
    {
        return Err(format!("unknown {field}").into());
    }
    Ok(())
}

fn frontend(
    plan: &Value,
    resources: &[&Value],
    port: u16,
    protocol: &str,
    ecs: bool,
) -> Result<(Frontend, u64)> {
    let listeners: Vec<_> = resources
        .iter()
        .filter(|r| {
            r["type"] == "aws_lb_listener"
                && r["values"]["port"].as_u64() == Some(u64::from(port))
                && r["values"]["protocol"] == protocol
        })
        .collect();
    if listeners.len() != 1 {
        return Err(format!("missing or ambiguous {protocol} listener on {port}").into());
    }
    let listener = listeners[0];
    let address = string(listener, "address")?;
    let action = single(&listener["values"]["default_action"])?;
    if string(action, "type")? != "forward" {
        return Err("listener does not forward to a target group".into());
    }
    let target_arn = string(action, "target_group_arn")?;
    let targets: Vec<_> = resources
        .iter()
        .filter(|r| {
            r["type"] == "aws_lb_target_group" && r["values"]["arn"].as_str() == Some(target_arn)
        })
        .collect();
    if targets.len() != 1 {
        return Err(format!("missing or ambiguous target group for {address}").into());
    }
    let target = &targets[0]["values"];
    if ecs {
        let expected_target = match address {
            "module.load_balancer[0].aws_lb_listener.this" => {
                "module.load_balancer[0].aws_lb_target_group.this"
            }
            "module.load_balancer[0].aws_lb_listener.internal[\"serve\"]" => {
                "module.load_balancer[0].aws_lb_target_group.internal[\"serve\"]"
            }
            "module.load_balancer[0].aws_lb_listener.internal[\"workers\"]" => {
                "module.load_balancer[0].aws_lb_target_group.internal[\"workers\"]"
            }
            _ => return Err("unexpected ECS listener address".into()),
        };
        if targets[0]["address"] != expected_target || string(target, "target_type")? != "instance"
        {
            return Err("ECS listener must use its original instance target group".into());
        }
    }
    let health = single(&target["health_check"])?;
    if string(health, "protocol")? != "HTTP" {
        return Err("unsupported target group health protocol".into());
    }
    let frontend = Frontend {
        port,
        target_port: number(target, "port")?.try_into()?,
        proxy_protocol_v2: proxy_protocol_v2(plan, targets[0])?,
        preserve_client_ip: optional_bool(&target["preserve_client_ip"]),
        health: Health {
            path: string(health, "path")?.into(),
            interval_seconds: number(health, "interval")?,
            timeout_seconds: number(health, "timeout")?,
            healthy_threshold: number(health, "healthy_threshold")?.try_into()?,
            unhealthy_threshold: number(health, "unhealthy_threshold")?.try_into()?,
        },
    };
    Ok((frontend, provider_seconds(target, "deregistration_delay")?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const FORGOTTEN_SUMMARY: &str = "Destroy was successful but left behind forgotten instances";
    const FORGOTTEN_DETAIL: &str = concat!(
        "As requested, OpenTofu has not deleted some remote objects that are no longer managed by this configuration. Those objects continue to exist in their remote system and so may continue to incur charges. Refer to the original plan for more information.\n",
        "To suppress this error for the future 'destroy' runs, you can add the CLI flag \"-suppress-forget-errors\".",
    );

    const GANG: &str = "endpoint = \"https://gang.example.com\"\nworker_endpoint = \"grpcs://gang.example.com:9090\"";

    fn plan(gang: &str) -> Value {
        let listener = |name: &str, port: u16, protocol: &str| {
            json!({
                "address": format!("module.load_balancer[0].aws_lb_listener.{name}"),
                "type": "aws_lb_listener",
                "values": {"port": port, "protocol": protocol, "default_action": [{"type": "forward", "target_group_arn": name}]}
            })
        };
        let target = |name: &str, port: u16| {
            json!({
                "address": format!("module.load_balancer[0].aws_lb_target_group.{name}"),
                "type": "aws_lb_target_group",
                "values": {
                    "arn": name, "port": port,
                    "proxy_protocol_v2": false, "preserve_client_ip": "true",
                    "deregistration_delay": "300",
                    "health_check": [{
                        "protocol": "HTTP", "path": "/leader",
                        "interval": 10, "timeout": 6,
                        "healthy_threshold": 2, "unhealthy_threshold": 3
                    }]
                }
            })
        };
        json!({
            "configuration":{"root_module":{"module_calls":{"controller":{"module":{"resources":[{
                "address":"aws_autoscaling_group.controller",
                "expressions":{"health_check_type":{"constant_value":"EC2"},"instance_refresh":[{
                    "preferences":[{"min_healthy_percentage":{"constant_value":100},"max_healthy_percentage":{"constant_value":200}}]
                }]}
            }]}}}}},
            "resource_changes":[{
                "address":"module.controller.aws_autoscaling_group.controller[0]",
                "change":{"actions":["no-op"],"after_unknown":{}}
            }],
            "output_changes": {"gang_toml": {"after": format!("poll_interval = \"50ms\"\n{gang}\n[[fleets]]\nname = \"gang-worker-x86-64\"\nmax_instances = 2\n")}},
            "planned_values": {"root_module": {"child_modules": [{"resources": [
                listener("this", 443, "TLS"), target("this", 8443),
                listener("unrelated[0]", 9443, "TLS"), target("unrelated[0]", 9444),
                listener("internal[\"worker\"]", 9090, "TCP"), target("internal[\"worker\"]", 9091),
                {"address": "module.controller.aws_autoscaling_group.controller[0]", "type": "aws_autoscaling_group",
                 "values": {"min_size": 1, "desired_capacity": 1, "max_size": 2, "health_check_type": "EC2",
                            "health_check_grace_period": 300,
                            "instance_refresh": [{"preferences": [{"min_healthy_percentage": 100, "max_healthy_percentage": 200}]}]}}
            ]}]}}
        })
    }

    fn resources(plan: &mut Value) -> &mut Vec<Value> {
        plan["planned_values"]["root_module"]["child_modules"][0]["resources"]
            .as_array_mut()
            .expect("resources")
    }

    fn stream(gang: &str) -> Vec<u8> {
        events(&plan(gang))
    }

    fn events(plan: &Value) -> Vec<u8> {
        encode_events(&synthetic_events(plan, false))
    }

    fn encode_events(events: &[Value]) -> Vec<u8> {
        events
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes()
    }

    fn synthetic_events(plan: &Value, unrelated: bool) -> Vec<Value> {
        let gang = plan["output_changes"]["gang_toml"]["after"]
            .as_str()
            .expect("gang");
        let seed = state(
            &gang.replace("max_instances = 2", "max_instances = 1"),
            unrelated,
            false,
        );
        let promote = state(gang, unrelated, true);
        let changes = vec![json!({"address":"aws_ssm_parameter.config_generation",
            "change":{"actions":["forget","create"]}})];
        let retention = json!({"resource_changes":[{
            "address":"aws_ssm_parameter.config_generation",
            "change":{"actions":["forget", "create"]}
        }]});
        let payloads = [
            seed,
            json!({"resource_changes": changes}),
            promote,
            plan.clone(),
            retention,
        ];
        let names = RUNS;
        let mut events = vec![json!({"type":"test_abstract","test_abstract":{(TEST):names}})];
        for (index, (name, payload)) in names.iter().zip(payloads).enumerate() {
            events.push(json!({"type":"test_run","@testfile":TEST,"@testrun":name,
                "test_run":{"path":TEST,"run":name,"status":"pass"}}));
            let kind = if index == 0 || index == 2 {
                "test_state"
            } else {
                "test_plan"
            };
            events.push(json!({"type":kind,"@testfile":TEST,"@testrun":name,(kind):payload}));
        }
        events.push(
            json!({"type":"test_file","@testfile":TEST,"test_file":{"path":TEST,"status":"pass"}}),
        );
        events.push(json!({"type":"test_summary","test_summary":{"status":"pass","passed":5,"failed":0,"errored":0,"skipped":0}}));
        events.push(forgotten_event());
        events
    }

    fn forgotten_event() -> Value {
        json!({
            "type":"diagnostic","@level":"error","@testfile":TEST,"@testrun":"promote",
            "diagnostic":{"severity":"error","summary":FORGOTTEN_SUMMARY,"detail":FORGOTTEN_DETAIL}
        })
    }

    #[test]
    fn migration_cleanup_notice_requires_completed_runs_and_exact_success() {
        let mut notice = forgotten_event();
        notice["@testfile"] = json!(MIGRATION_TEST);
        notice["@testrun"] = json!("baseline");
        let mut forgotten = false;
        assert!(migration_diagnostic(&notice, &mut forgotten, false).is_err());
        assert!(!forgotten);
        assert!(migration_diagnostic(&notice, &mut forgotten, true).is_ok());
        assert!(forgotten);
        assert!(migration_diagnostic(&notice, &mut forgotten, true).is_err());
        let mut other = notice.clone();
        other["@testfile"] = json!("tests/unrelated.tftest.hcl");
        assert!(migration_diagnostic(&other, &mut false, true).is_err());
        let mut failed = notice;
        failed["diagnostic"]["detail"] = json!("cleanup failed");
        assert!(migration_diagnostic(&failed, &mut false, true).is_err());
    }

    fn event_payload<'a>(events: &'a mut [Value], run: &str, kind: &str) -> &'a mut Value {
        &mut events
            .iter_mut()
            .find(|event| event["type"] == kind && event["@testrun"] == run)
            .expect("run payload")[kind]
    }

    fn state(gang: &str, unrelated: bool, promoted: bool) -> Value {
        let prefix = "/gang/gang.toml";
        let arn = "arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml";
        let id = if promoted {
            "22222222-2222-4222-8222-222222222222"
        } else {
            "11111111-1111-4111-8111-111111111111"
        };
        let active = format!("{prefix}/generations/{id}:1");
        let mut resources = vec![
            json!({"address":"aws_ssm_parameter.config_generation","type":"aws_ssm_parameter","mode":"managed",
                "values":{"name":format!("{prefix}/generations/{id}"),"arn":format!("{arn}/generations/{id}"),"version":1,"value":gang,"overwrite":false}}),
            json!({"address":"terraform_data.config_generation","type":"terraform_data","values":{"id":id}}),
        ];
        if unrelated {
            resources.push(json!({"address":"aws_ssm_parameter.config","type":"aws_ssm_parameter","mode":"managed",
                "values":{"name":prefix,"arn":arn,"version":1,"value":"unrelated"}}));
        }
        for (module, role, template) in [
            ("module.controller", "controller", "controller[0]"),
            (
                "module.worker_fleet",
                "worker",
                "this[\"gang-worker-x86-64\"]",
            ),
        ] {
            let profile = format!("arn:aws:iam::000000000000:instance-profile/{role}");
            resources.push(json!({"address":format!("{module}.aws_launch_template.{template}"),
                "type":"aws_launch_template","values":{
                    "user_data":base64::engine::general_purpose::STANDARD.encode(format!("{role} {active}\n# config digest")),
                    "iam_instance_profile":[{"arn":profile}]
                }}));
            resources.push(json!({"address":format!("{module}.module.instance_role.aws_iam_instance_profile.this"),
                "type":"aws_iam_instance_profile","values":{"arn":profile}}));
            let mut readable = vec![format!("{arn}/generations/*")];
            if unrelated {
                readable.push(arn.into());
            }
            resources.push(json!({"address":format!("{module}.module.instance_role.data.aws_iam_policy_document.base"),
                "type":"aws_iam_policy_document","mode":"data","values":{"json":"{}","statement":[
                    {"sid":"ReadParameters","effect":"Allow","actions":["ssm:GetParameter"],"resources":readable},
                    {"sid":"DenyOtherParameters","effect":"Deny","actions":["ssm:GetParameter","ssm:GetParameters","ssm:GetParametersByPath","ssm:GetParameterHistory"],"not_resources":readable}
                ]}}));
        }
        let outputs =
            json!({"gang_toml":{"value":gang},"config_parameter_reference":{"value":active}});
        json!({"values":{"outputs":outputs,"root_module":{"resources":resources}}})
    }

    fn evidence(unrelated: bool) -> Evidence {
        selected_evidence(&encode_events(&synthetic_events(&plan(GANG), unrelated)))
            .expect("evidence")
    }

    fn configuration_from(evidence: &Evidence) -> Result<Configuration> {
        let config = toml::from_str(state_output(&evidence.promote, "gang_toml")?)?;
        configuration(evidence, &config)
    }

    fn applied_resources(evidence: &mut Evidence) -> &mut Vec<Value> {
        evidence.promote["values"]["root_module"]["resources"]
            .as_array_mut()
            .expect("applied resources")
    }

    fn applied_resource<'a>(evidence: &'a mut Evidence, address: &str) -> &'a mut Value {
        applied_resources(evidence)
            .iter_mut()
            .find(|r| r["address"] == address)
            .expect("applied resource")
    }

    fn ecs_events() -> Vec<Value> {
        let mut candidate = plan(GANG);
        candidate["output_changes"]["gang_toml"]["after"] = json!(format!(
            "{}\n[server]\nstop_timeout = \"2460s\"\n",
            candidate["output_changes"]["gang_toml"]["after"]
                .as_str()
                .expect("gang")
        ));
        let entries = resources(&mut candidate);
        entries.retain(|r| r["type"] != "aws_autoscaling_group");
        for index in [1, 5] {
            entries[index]["values"]["target_type"] = json!("instance");
        }
        entries[4]["address"] =
            json!("module.load_balancer[0].aws_lb_listener.internal[\"workers\"]");
        entries[5]["address"] =
            json!("module.load_balancer[0].aws_lb_target_group.internal[\"workers\"]");
        let mut serve_listener = entries[4].clone();
        serve_listener["address"] =
            json!("module.load_balancer[0].aws_lb_listener.internal[\"serve\"]");
        serve_listener["values"]["port"] = json!(443);
        serve_listener["values"]["default_action"][0]["target_group_arn"] = json!("serve");
        let mut serve_target = entries[5].clone();
        serve_target["address"] =
            json!("module.load_balancer[0].aws_lb_target_group.internal[\"serve\"]");
        serve_target["values"]["arn"] = json!("serve");
        serve_target["values"]["port"] = json!(443);
        entries.push(serve_listener);
        entries.push(serve_target);
        entries.push(json!({"address":"module.controller.aws_autoscaling_group.capacity[0]","type":"aws_autoscaling_group",
            "values":{"min_size":1,"desired_capacity":1,"max_size":2}}));
        entries.push(json!({"address":"module.controller.aws_ecs_service.controller[0]","type":"aws_ecs_service",
            "values":{"desired_count":1,"deployment_minimum_healthy_percent":100,"deployment_maximum_percent":200,
                "deployment_circuit_breaker":[{"enable":true,"rollback":true}]}}));
        entries.push(json!({"address":"module.controller.aws_ecs_task_definition.controller[0]","type":"aws_ecs_task_definition",
            "values":{"network_mode":"host","container_definitions":json!([{"name":"controller",
                "image":format!("000000000000.dkr.ecr.eu-north-1.amazonaws.com/gang-controller@sha256:{}", "a".repeat(64))}]).to_string()}}));
        entries.push(json!({"address":"module.controller.aws_launch_template.capacity[0]","type":"aws_launch_template",
            "values":{"user_data":base64::engine::general_purpose::STANDARD.encode("ECS_CONTAINER_STOP_TIMEOUT=2460s")}}));
        let mut events = synthetic_events(&candidate, false);
        for run in ["seed", "promote"] {
            let state = event_payload(&mut events, run, "test_state");
            let active = state["values"]["outputs"]["config_parameter_reference"]["value"]
                .as_str()
                .expect("selector")
                .to_owned();
            let entries = state["values"]["root_module"]["resources"]
                .as_array_mut()
                .expect("resources");
            entries
                .retain(|r| r["address"] != "module.controller.aws_launch_template.controller[0]");
            let container = json!([{"name":"controller", "secrets":[{"name":"GANG_CONFIG",
                "valueFrom":format!("arn:aws:ssm:eu-north-1:000000000000:parameter{}", active.split_once(':').expect("version").0)}]}]);
            entries.push(json!({"address":"module.controller.aws_ecs_task_definition.controller[0]","type":"aws_ecs_task_definition",
                "values":{"execution_role_arn":"execution-arn","container_definitions":container.to_string()}}));
            entries.push(json!({"address":"module.controller.aws_iam_role.execution[0]","type":"aws_iam_role",
                "values":{"arn":"execution-arn","id":"execution"}}));
            entries.push(json!({"address":"module.controller.aws_iam_role_policy.execution_secrets[0]","type":"aws_iam_role_policy",
                "values":{"role":"execution","policy":"{}"}}));
            entries.push(json!({"address":"module.controller.data.aws_iam_policy_document.execution_secrets","type":"aws_iam_policy_document",
                "values":{"json":"{}","statement":[{"sid":"ReadParameters","effect":"Allow","actions":["ssm:GetParameters"],
                    "resources":["arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml/generations/*"]}]}}));
        }
        events
    }

    #[test]
    fn targeted_plans_reject_mutations_and_require_convergence() {
        let base = json!({"resource_changes":[{
            "address":"aws_ssm_parameter.config_generation","mode":"managed",
            "change":{"actions":["no-op"],"after":{"value":"config"},"after_unknown":{}}
        }]});
        assert!(validate_target_plan(&base, "config", true).is_ok());
        for (address, actions) in [
            (
                "module.worker_fleet.aws_autoscaling_group.this",
                json!(["create"]),
            ),
            (
                "module.load_balancer[0].aws_lb_listener.this",
                json!(["update"]),
            ),
            (
                "module.controller.aws_ecs_task_definition.controller[0]",
                json!(["delete", "create"]),
            ),
        ] {
            let mut plan = base.clone();
            plan["resource_changes"]
                .as_array_mut()
                .expect("changes")
                .push(json!({"address":address,"mode":"managed","change":{"actions":actions}}));
            assert!(validate_target_plan(&plan, "config", false).is_err());
        }
        let mut plan = base.clone();
        plan["resource_changes"][0]["change"]["actions"] = json!(["forget", "create"]);
        assert!(validate_target_plan(&plan, "config", false).is_ok());
        assert!(validate_target_plan(&plan, "config", true).is_err());
        plan["resource_changes"][0]["change"]["after_unknown"]["value"] = json!(true);
        assert!(validate_target_plan(&plan, "config", false).is_err());
    }

    #[test]
    fn handoff_allows_worker_version_update_without_capacity_changes() {
        let plan = json!({"resource_changes":[
            {"address":"aws_ssm_parameter.config_generation","mode":"managed",
             "change":{"actions":["no-op"],"after":{"value":"config"},"after_unknown":{}}},
            {"address":"module.worker_fleet.aws_launch_template.this[\"fleet\"]","mode":"managed",
             "change":{"actions":["update"],"before":{"image_id":"ami-old"},"after":{"image_id":"ami-new"}}},
            {"address":"module.worker_fleet.aws_autoscaling_group.this[\"fleet\"]","mode":"managed",
             "change":{"actions":["update"],"before":{"min_size":0,"max_size":2,"desired_capacity":1,"mixed_instances_policy":[{"launch_template":[{"launch_template_specification":[{"launch_template_id":"lt-1","version":"1"}]}]}]},"after":{"min_size":0,"max_size":2,"desired_capacity":1,"mixed_instances_policy":[{"launch_template":[{"launch_template_specification":[{"launch_template_id":"lt-1","version":"2"}]}]}]}}}
        ]});
        assert!(validate_target_plan_phase(&plan, "config", false, true).is_ok());
        assert!(validate_target_plan(&plan, "config", false).is_err());
        for (field, value) in [
            ("min_size", json!(1)),
            ("max_size", json!(1)),
            ("desired_capacity", json!(0)),
        ] {
            let mut unsafe_plan = plan.clone();
            unsafe_plan["resource_changes"][2]["change"]["after"][field] = value;
            assert!(validate_target_plan_phase(&unsafe_plan, "config", false, true).is_err());
        }
        let mut unsafe_template = plan.clone();
        unsafe_template["resource_changes"][1]["change"]["after"]["vpc_security_group_ids"] =
            json!(["sg-unexpected"]);
        assert!(validate_target_plan_phase(&unsafe_template, "config", false, true).is_err());
        let mut unsafe_plan = plan;
        unsafe_plan["resource_changes"][2]["change"]["actions"] = json!(["delete", "create"]);
        assert!(validate_target_plan_phase(&unsafe_plan, "config", false, true).is_err());
    }

    #[test]
    fn fixture_document_requires_explicit_known_handoff_protocol() {
        let fixture = extract(&stream(GANG), "local".into()).expect("historical fixture");
        for name in ["drain_before_release", "release_before_drain"] {
            let document = FixtureDocument {
                handoff_protocol: HandoffProtocol::parse(name).expect("protocol"),
                fixture: &fixture,
            };
            let json = serde_json::to_value(document).expect("document");
            assert_eq!(json["handoff_protocol"], name);
            assert!(json["controller"].is_object());
        }
        for invalid in ["", "asg", "DRAIN_BEFORE_RELEASE"] {
            assert!(HandoffProtocol::parse(invalid).is_err());
        }
    }

    #[test]
    fn migration_requires_applied_states_and_post_apply_convergence() {
        let config = |value| {
            json!({"address":"aws_ssm_parameter.config_generation",
            "values":{"value":value}})
        };
        let ecr = json!({"address":"module.controller.aws_ecr_repository.controller[0]"});
        let service = json!({"address":"module.controller.aws_ecs_service.controller[0]"});
        let task = json!({"address":"module.controller.aws_ecs_task_definition.controller[0]"});
        let asg = json!({"address":"module.controller.aws_autoscaling_group.capacity[0]"});
        let state =
            |resources: Vec<Value>| json!({"values":{"root_module":{"resources":resources}}});
        let config_change = |actions| {
            json!({"address":"aws_ssm_parameter.config_generation",
            "mode":"managed","change":{"actions":actions,"after":{"value":"new"},"after_unknown":{}}})
        };
        let no_op = json!({"resource_changes":[config_change(json!(["no-op"]))]});
        let bootstrap = json!({"resource_changes":[
            config_change(json!(["forget","create"])),
            {"address":"terraform_data.config_generation","change":{"actions":["delete","create"]}},
            {"address":"module.controller.aws_ecr_repository.controller[0]","change":{"actions":["create"]}}
        ]});
        let handoff = json!({"resource_changes":[config_change(json!(["no-op"])),
            {"address":"module.controller.aws_ecs_service.controller[0]","change":{"actions":["create"]}}
        ]});
        let template = |image: &str, version: i64| json!({"address":"module.worker_fleet.aws_launch_template.this[\"fleet\"]", "values":{"id":"lt-1", "image_id":image, "latest_version":version}});
        let group = |version: &str| json!({"address":"module.worker_fleet.aws_autoscaling_group.this[\"fleet\"]", "values":{"mixed_instances_policy":[{"launch_template":[{"launch_template_specification":[{"launch_template_id":"lt-1", "version":version}]}]}]}});
        let old_group = group("1");
        let new_group = group("2");
        let worker_plan = json!({"resource_changes":[config_change(json!(["no-op"])),
            {"address":"module.worker_fleet.aws_launch_template.this[\"fleet\"]","mode":"managed","change":{"actions":["update"],"before":{"image_id":"ami-old"},"after":{"image_id":"ami-new"}}},
            {"address":"module.worker_fleet.aws_autoscaling_group.this[\"fleet\"]","mode":"managed","change":{"actions":["update"],"before":old_group["values"],"after":new_group["values"]}}
        ]});
        let mut payloads = [
            Some(state(vec![
                config("old"),
                template("ami-old", 1),
                old_group,
            ])),
            Some(bootstrap),
            Some(state(vec![config("new"), ecr.clone()])),
            Some(no_op.clone()),
            Some(worker_plan),
            Some(state(vec![
                config("new"),
                ecr.clone(),
                template("ami-new", 2),
                new_group,
            ])),
            Some(no_op.clone()),
            Some(handoff),
            Some(state(vec![config("new"), ecr, service, task, asg])),
            Some(no_op),
        ];
        assert!(validate_migration_payloads(&payloads, "new").is_ok());
        let mut outdated = payloads.clone();
        outdated[5].as_mut().expect("state")["values"]["root_module"]["resources"][3]["values"]["mixed_instances_policy"]
            [0]["launch_template"][0]["launch_template_specification"][0]["version"] = json!("1");
        assert!(validate_migration_payloads(&outdated, "new").is_err());
        let mut unchanged = payloads.clone();
        unchanged[5].as_mut().expect("state")["values"]["root_module"]["resources"][2]["values"]
            ["image_id"] = json!("ami-old");
        assert!(validate_migration_payloads(&unchanged, "new").is_err());
        payloads[3] = payloads[1].clone();
        assert!(validate_migration_payloads(&payloads, "new").is_err());
    }

    #[test]
    fn extracts_ecs_service_and_execution_role_evidence() {
        let fixture = extract(&encode_events(&ecs_events()), "local".into()).expect("ECS fixture");
        let controller = serde_json::to_value(&fixture.controller).expect("controller JSON");
        assert_eq!(controller["mode"], "ecs");
        assert_eq!(
            controller["image"],
            format!(
                "000000000000.dkr.ecr.eu-north-1.amazonaws.com/gang-controller@sha256:{}",
                "a".repeat(64)
            )
        );
        assert_eq!(controller["network_mode"], "host");
        assert_eq!(controller["target_type"], "instance");
        assert_eq!(controller["stop_timeout_seconds"], 2460);
        assert_eq!(controller["circuit_breaker_enable"], true);
        assert_eq!(controller["circuit_breaker_rollback"], true);
        assert!(controller.get("warmup_seconds").is_none());
        assert!(controller.get("health_check_type").is_none());
        assert!(
            fixture
                .configuration
                .controller
                .excluded_from_deny
                .is_empty()
        );
        assert_eq!(fixture.configuration.controller.readable_resources.len(), 1);
        assert_eq!(
            fixture.configuration.controller.reference,
            fixture.configuration.workers[0].reference
        );
        let old = extract(&stream(GANG), "local".into()).expect("historical fixture");
        let old = serde_json::to_value(old.controller).expect("historical JSON");
        assert!(old.get("mode").is_none());
        assert_eq!(old["health_check_type"], "EC2");
        assert_eq!(old["warmup_seconds"], 300);
    }

    #[test]
    fn rejects_unresolved_or_ambiguous_listener_targets() {
        for duplicate in [false, true] {
            let mut events = ecs_events();
            let plan = event_payload(&mut events, "upgrade", "test_plan");
            let entries = resources(plan);
            if duplicate {
                entries[3]["values"]["arn"] = entries[1]["values"]["arn"].clone();
            } else {
                entries[0]["values"]["default_action"][0]["target_group_arn"] = json!("missing");
            }
            assert!(extract(&encode_events(&events), "local".into()).is_err());
        }
    }

    #[test]
    fn rejects_abandoned_target_group_addresses() {
        for (original, abandoned) in [
            ("this", "service[0]"),
            ("internal[\"serve\"]", "service_internal[\"serve\"]"),
            ("internal[\"workers\"]", "service_internal[\"workers\"]"),
        ] {
            let mut events = ecs_events();
            let plan = event_payload(&mut events, "upgrade", "test_plan");
            let original = format!("module.load_balancer[0].aws_lb_target_group.{original}");
            let abandoned = format!("module.load_balancer[0].aws_lb_target_group.{abandoned}");
            resources(plan)
                .iter_mut()
                .find(|resource| resource["address"] == original)
                .expect("target group")["address"] = json!(abandoned);
            assert!(extract(&encode_events(&events), "local".into()).is_err());
        }
    }

    #[test]
    fn rejects_versioned_ecs_selector() {
        let mut events = ecs_events();
        let state = event_payload(&mut events, "promote", "test_state");
        let task = state["values"]["root_module"]["resources"]
            .as_array_mut()
            .expect("resources")
            .iter_mut()
            .find(|r| r["address"] == "module.controller.aws_ecs_task_definition.controller[0]")
            .expect("task");
        let mut containers: Value = serde_json::from_str(
            task["values"]["container_definitions"]
                .as_str()
                .expect("definitions"),
        )
        .expect("container JSON");
        let selector = containers[0]["secrets"][0]["valueFrom"]
            .as_str()
            .expect("selector");
        containers[0]["secrets"][0]["valueFrom"] = json!(format!("{selector}:1"));
        task["values"]["container_definitions"] = json!(containers.to_string());
        assert!(extract(&encode_events(&events), "local".into()).is_err());
    }

    #[test]
    fn rejects_any_task_stop_timeout() {
        for value in [
            Value::Null,
            json!(0),
            json!(120),
            json!(2460),
            json!("2460"),
        ] {
            let mut events = ecs_events();
            let plan = event_payload(&mut events, "upgrade", "test_plan");
            let task = resources(plan)
                .iter_mut()
                .find(|r| r["address"] == "module.controller.aws_ecs_task_definition.controller[0]")
                .expect("task");
            let mut containers: Value = serde_json::from_str(
                task["values"]["container_definitions"]
                    .as_str()
                    .expect("definitions"),
            )
            .expect("container JSON");
            containers[0]["stopTimeout"] = value;
            task["values"]["container_definitions"] = json!(containers.to_string());
            assert!(extract(&encode_events(&events), "local".into()).is_err());
        }
    }

    #[test]
    fn rejects_invalid_or_ambiguous_host_stop_timeout() {
        for user_data in [
            "",
            "ECS_CONTAINER_STOP_TIMEOUT=0s",
            "ECS_CONTAINER_STOP_TIMEOUT=2459s",
            "ECS_CONTAINER_STOP_TIMEOUT=2460",
            "ECS_CONTAINER_STOP_TIMEOUT=2460s\nECS_CONTAINER_STOP_TIMEOUT=2460s",
        ] {
            let mut events = ecs_events();
            let plan = event_payload(&mut events, "upgrade", "test_plan");
            resources(plan)
                .iter_mut()
                .find(|r| r["address"] == "module.controller.aws_launch_template.capacity[0]")
                .expect("capacity")["values"]["user_data"] =
                json!(base64::engine::general_purpose::STANDARD.encode(user_data));
            assert!(extract(&encode_events(&events), "local".into()).is_err());
        }
    }

    #[test]
    fn rejects_invalid_ecs_service_evidence() {
        for (address, field, invalid) in [
            (
                "module.controller.aws_ecs_service.controller[0]",
                "deployment_circuit_breaker",
                json!([{"enable":true}]),
            ),
            (
                "module.controller.aws_ecs_service.controller[0]",
                "deployment_circuit_breaker",
                json!([{"enable":true,"rollback":false}]),
            ),
            (
                "module.controller.aws_ecs_task_definition.controller[0]",
                "network_mode",
                json!("awsvpc"),
            ),
            (
                "module.controller.aws_ecs_task_definition.controller[0]",
                "container_definitions",
                json!("[{\"name\":\"controller\"}]"),
            ),
            (
                "module.controller.aws_ecs_task_definition.controller[0]",
                "container_definitions",
                json!("[{\"name\":\"controller\",\"image\":\"example:latest\"}]"),
            ),
            (
                "module.controller.aws_ecs_task_definition.controller[0]",
                "container_definitions",
                json!("[{\"name\":\"controller\",\"image\":\"example@sha256:abc\"}]"),
            ),
            (
                "module.controller.aws_launch_template.capacity[0]",
                "user_data",
                json!(
                    base64::engine::general_purpose::STANDARD
                        .encode("ECS_CONTAINER_STOP_TIMEOUT=1s")
                ),
            ),
            (
                "module.load_balancer[0].aws_lb_target_group.this",
                "target_type",
                json!("ip"),
            ),
            (
                "module.load_balancer[0].aws_lb_target_group.internal[\"workers\"]",
                "target_type",
                json!("ip"),
            ),
            (
                "module.load_balancer[0].aws_lb_target_group.internal[\"serve\"]",
                "target_type",
                json!("ip"),
            ),
        ] {
            let mut events = ecs_events();
            let plan = event_payload(&mut events, "upgrade", "test_plan");
            resources(plan)
                .iter_mut()
                .find(|r| r["address"] == address)
                .expect("resource")["values"][field] = invalid;
            assert!(
                extract(&encode_events(&events), "local".into()).is_err(),
                "{address} {field}"
            );
        }
        for (address, field, invalid) in [
            (
                "module.controller.data.aws_iam_policy_document.execution_secrets",
                "statement",
                json!([{"sid":"ReadParameters","effect":"Allow","actions":["ssm:GetParameter"],"resources":["*"]}]),
            ),
            (
                "module.controller.aws_ecs_task_definition.controller[0]",
                "container_definitions",
                json!(
                    "[{\"name\":\"controller\",\"secrets\":[{\"name\":\"GANG_CONFIG\",\"valueFrom\":\"wrong\"}]}]"
                ),
            ),
            (
                "module.controller.aws_iam_role_policy.execution_secrets[0]",
                "role",
                json!("wrong"),
            ),
        ] {
            let mut events = ecs_events();
            let state = event_payload(&mut events, "promote", "test_state");
            state["values"]["root_module"]["resources"]
                .as_array_mut()
                .expect("resources")
                .iter_mut()
                .find(|r| r["address"] == address)
                .expect("resource")["values"][field] = invalid;
            assert!(
                extract(&encode_events(&events), "local".into()).is_err(),
                "{address} {field}"
            );
        }
    }

    #[test]
    fn derives_generation_properties_with_or_without_unrelated_parameter() {
        for unrelated in [false, true] {
            let fixture = extract(
                &encode_events(&synthetic_events(&plan(GANG), unrelated)),
                "local".into(),
            )
            .expect("fixture");
            assert!(fixture.configuration.generations_retained);
            for access in std::iter::once(&fixture.configuration.controller)
                .chain(&fixture.configuration.workers)
                .chain(std::iter::once(&fixture.seed.configuration.controller))
                .chain(&fixture.seed.configuration.workers)
            {
                assert_eq!(access.readable_resources.len(), 1);
                assert_eq!(access.excluded_from_deny.len(), 1);
                assert!(access.readable_resources[0].ends_with("/generations/*"));
                assert_eq!(access.readable_resources, access.excluded_from_deny);
            }
            assert_eq!(fixture.configuration.workers.len(), 1);
            assert_eq!(
                fixture.configuration.controller.reference,
                fixture.configuration.workers[0].reference
            );
        }
    }

    #[test]
    fn emits_applied_seed_and_promoted_max_only_transition() {
        let events = synthetic_events(&plan(GANG), false);
        let encoded = encode_events(&events);
        let evidence = selected_evidence(&encoded).expect("evidence");
        let fixture = extract(&encoded, "local".into()).expect("fixture");
        assert_eq!(
            fixture.seed.gang_toml,
            state_output(&evidence.seed, "gang_toml").expect("seed")
        );
        assert_eq!(
            fixture.gang_toml,
            state_output(&evidence.promote, "gang_toml").expect("promoted")
        );
        let mut seed: toml::Value = toml::from_str(&fixture.seed.gang_toml).expect("seed TOML");
        let promoted: toml::Value = toml::from_str(&fixture.gang_toml).expect("promoted TOML");
        assert_eq!(seed["poll_interval"].as_str(), Some("50ms"));
        assert_eq!(seed["fleets"][0]["max_instances"].as_integer(), Some(1));
        assert_eq!(promoted["fleets"][0]["max_instances"].as_integer(), Some(2));
        seed["fleets"][0]["max_instances"] = toml::Value::Integer(2);
        assert_eq!(seed, promoted);
        assert_eq!(fixture.controller.instances, 1);
        assert!(fixture.seed.configuration.generations_retained);
        assert_eq!(fixture.seed.configuration.workers.len(), 1);
        let old = &fixture.seed.configuration.controller.reference;
        let new = &fixture.configuration.controller.reference;
        assert_eq!(
            old,
            state_output(&evidence.seed, "config_parameter_reference").expect("seed selector")
        );
        assert_eq!(
            new,
            state_output(&evidence.promote, "config_parameter_reference")
                .expect("promoted selector")
        );
        assert_ne!(old, new);
        assert!(old.ends_with(":1") && new.ends_with(":1"));
        assert_eq!(old, &fixture.seed.configuration.workers[0].reference);
        let namespace = &fixture.configuration.controller.readable_resources[0];
        assert!(
            old.rsplit_once(':').expect("version").0.starts_with(
                namespace
                    .split_once(":parameter")
                    .expect("parameter ARN")
                    .1
                    .trim_end_matches('*')
            )
        );
        let serialized = serde_json::to_value(&fixture).expect("JSON fixture");
        assert_eq!(
            serialized["seed"]
                .as_object()
                .expect("mandatory seed")
                .len(),
            2
        );
        assert!(serialized["seed"]["configuration"].is_object());
    }

    #[test]
    fn rejects_unproven_seed_access_and_generation() {
        for (address, field, invalid) in [
            ("aws_ssm_parameter.config_generation", "version", json!(2)),
            (
                "module.controller.aws_launch_template.controller[0]",
                "user_data",
                json!(
                    base64::engine::general_purpose::STANDARD
                        .encode("controller /wrong/generation:1")
                ),
            ),
            (
                "module.worker_fleet.module.instance_role.data.aws_iam_policy_document.base",
                "statement",
                json!([]),
            ),
        ] {
            let mut events = synthetic_events(&plan(GANG), false);
            let seed = event_payload(&mut events, "seed", "test_state");
            let resource = seed["values"]["root_module"]["resources"]
                .as_array_mut()
                .expect("seed resources")
                .iter_mut()
                .find(|resource| resource["address"] == address)
                .expect("seed resource");
            resource["values"][field] = invalid;
            assert!(
                extract(&encode_events(&events), "local".into()).is_err(),
                "{address}"
            );
        }
        let mut events = synthetic_events(&plan(GANG), false);
        let seed = event_payload(&mut events, "seed", "test_state");
        seed["values"]["outputs"]["config_parameter_reference"]["value"] =
            json!("/gang/gang.toml/generations/22222222-2222-4222-8222-222222222222:1");
        assert!(extract(&encode_events(&events), "local".into()).is_err());
    }

    #[test]
    fn binds_launch_template_profile_names_to_the_actual_role() {
        for (module, template) in [
            ("module.controller", "controller[0]"),
            ("module.worker_fleet", "this[\"gang-worker-x86-64\"]"),
        ] {
            let mut evidence = evidence(false);
            let address = format!("{module}.aws_launch_template.{template}");
            applied_resource(&mut evidence, &address)["values"]["iam_instance_profile"] =
                json!([{"name":"actual-profile","arn":null}]);
            let profile_address =
                format!("{module}.module.instance_role.aws_iam_instance_profile.this");
            applied_resource(&mut evidence, &profile_address)["values"]["name"] =
                json!("actual-profile");
            assert!(configuration_from(&evidence).is_ok());
            for block in [
                json!({"name":"wrong-profile"}),
                json!({"name":"actual-profile","arn":"arn:aws:iam::000000000000:instance-profile/other"}),
                json!({"name":"","arn":null}),
                json!({"name":true}),
                json!({}),
                json!({"name":"actual-profile","arn":0}),
            ] {
                applied_resource(&mut evidence, &address)["values"]["iam_instance_profile"] =
                    json!([block]);
                assert!(configuration_from(&evidence).is_err());
            }
        }
    }

    #[test]
    fn parses_singleton_controller_first_line_without_secret_selectors() {
        let mut evidence = evidence(true);
        let active = state_output(&evidence.promote, "config_parameter_reference")
            .expect("active")
            .to_owned();
        let instance = applied_resource(
            &mut evidence,
            "module.controller.aws_launch_template.controller[0]",
        );
        instance["type"] = json!("aws_instance");
        instance["address"] = json!("module.controller.aws_instance.this[0]");
        instance["values"]["user_data"] = json!(format!(
            "controller {active} state=/dev/sdg\nsecret=/different/parameter"
        ));
        instance["values"]["iam_instance_profile"] = json!("controller-profile");
        let profile = applied_resource(
            &mut evidence,
            "module.controller.module.instance_role.aws_iam_instance_profile.this",
        );
        profile["values"]["name"] = json!("controller-profile");
        assert_eq!(
            configuration_from(&evidence)
                .expect("singleton")
                .controller
                .reference,
            active
        );
    }

    #[test]
    fn validates_every_rendered_worker_template() {
        let mut evidence = evidence(true);
        let mut config: toml::Value =
            toml::from_str(state_output(&evidence.promote, "gang_toml").expect("gang"))
                .expect("config");
        let fleets = config
            .get_mut("fleets")
            .expect("fleets")
            .as_array_mut()
            .expect("array");
        let mut second = fleets[0].clone();
        second["name"] = toml::Value::String("other".into());
        fleets.push(second);
        let address = "module.worker_fleet.aws_launch_template.this[\"gang-worker-x86-64\"]";
        let mut template = applied_resource(&mut evidence, address).clone();
        template["address"] = json!("module.worker_fleet.aws_launch_template.this[\"other\"]");
        applied_resources(&mut evidence).push(template);
        assert_eq!(
            configuration(&evidence, &config)
                .expect("two workers")
                .workers
                .len(),
            2
        );
        let second = applied_resource(
            &mut evidence,
            "module.worker_fleet.aws_launch_template.this[\"other\"]",
        );
        second["values"]["user_data"] =
            json!(base64::engine::general_purpose::STANDARD.encode("worker /gang/gang.toml:1"));
        assert!(configuration(&evidence, &config).is_err());
    }

    #[test]
    fn rejects_bare_pins_and_miswired_first_line_selectors() {
        for (address, role) in [
            (
                "module.controller.aws_launch_template.controller[0]",
                "controller",
            ),
            (
                "module.worker_fleet.aws_launch_template.this[\"gang-worker-x86-64\"]",
                "worker",
            ),
        ] {
            let mut evidence = evidence(true);
            applied_resource(&mut evidence, address)["values"]["user_data"] = json!(
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{role} /gang/gang.toml:1"))
            );
            assert!(configuration_from(&evidence).is_err());
        }
        let mut evidence = evidence(true);
        evidence.promote["values"]["outputs"]["config_parameter_reference"]["value"] =
            json!("/gang/gang.toml:1");
        assert!(configuration_from(&evidence).is_err());
    }

    #[test]
    fn rejects_missing_extra_worker_templates_and_wrong_profile() {
        let address = "module.worker_fleet.aws_launch_template.this[\"gang-worker-x86-64\"]";
        let mut missing = evidence(true);
        applied_resources(&mut missing).retain(|r| r["address"] != address);
        assert!(configuration_from(&missing).is_err());
        let mut extra = evidence(true);
        let mut template = applied_resource(&mut extra, address).clone();
        template["address"] = json!("module.worker_fleet.aws_launch_template.this[\"other\"]");
        applied_resources(&mut extra).push(template);
        assert!(configuration_from(&extra).is_err());
        let mut wrong = evidence(true);
        applied_resource(&mut wrong, address)["values"]["iam_instance_profile"][0]["arn"] =
            json!("other-role");
        assert!(configuration_from(&wrong).is_err());
    }

    #[test]
    fn rejects_unfetchable_generation_version_and_missing_uuid() {
        for (address, field, value) in [
            ("aws_ssm_parameter.config_generation", "version", json!(2)),
            ("terraform_data.config_generation", "id", Value::Null),
        ] {
            let mut evidence = evidence(true);
            let resource = evidence.promote["values"]["root_module"]["resources"]
                .as_array_mut()
                .expect("resources")
                .iter_mut()
                .find(|r| r["address"] == address)
                .expect("resource");
            resource["values"][field] = value;
            assert!(configuration_from(&evidence).is_err());
        }
    }

    #[test]
    fn rejects_missing_namespace_permissions_in_each_configured_role() {
        for module in ["module.controller", "module.worker_fleet"] {
            for (index, field) in [(0, "resources"), (1, "not_resources")] {
                let mut evidence = evidence(true);
                let address =
                    format!("{module}.module.instance_role.data.aws_iam_policy_document.base");
                let policy = applied_resource(&mut evidence, &address);
                policy["values"]["statement"][index][field] =
                    json!(["arn:aws:ssm:eu-north-1:000000000000:parameter/gang/gang.toml"]);
                policy["values"]["json"] =
                    json!("{\"Statement\":[{\"Effect\":\"Allow\",\"Resource\":\"*\"}]}");
                assert!(configuration_from(&evidence).is_err());
            }
        }
    }

    #[test]
    fn rejects_current_generation_permissions_without_retained_namespace() {
        for module in ["module.controller", "module.worker_fleet"] {
            let mut evidence = evidence(true);
            let state = state_resources(&evidence.promote).expect("promoted resources");
            let generation = string(
                &resource(&state, "aws_ssm_parameter.config_generation").expect("generation")["values"],
                "arn",
            ).expect("current generation ARN").to_owned();
            let allowed = json!([generation]);
            let address =
                format!("{module}.module.instance_role.data.aws_iam_policy_document.base");
            let policy = applied_resource(&mut evidence, &address);
            policy["values"]["statement"][0]["resources"] = allowed.clone();
            policy["values"]["statement"][1]["not_resources"] = allowed;
            assert!(configuration_from(&evidence).is_err());
        }
    }

    #[test]
    fn observes_generation_delete_and_overwrite() {
        let mut deleted = evidence(true);
        deleted.transition["resource_changes"][0]["change"]["actions"] =
            json!(["delete", "create"]);
        assert!(
            !configuration_from(&deleted)
                .expect("deleted")
                .generations_retained
        );
        let mut overwrite = evidence(true);
        applied_resource(&mut overwrite, "aws_ssm_parameter.config_generation")["values"]["overwrite"] =
            json!(true);
        assert!(
            !configuration_from(&overwrite)
                .expect("overwrite")
                .generations_retained
        );
    }

    #[test]
    fn requires_typed_cleanup_notice_bound_to_promote_only_for_generations() {
        for (key, value) in [
            ("@testrun", json!("upgrade")),
            ("@testfile", json!("other.tftest.hcl")),
            ("@level", json!("warn")),
            ("type", json!("log")),
            (
                "diagnostic",
                json!({"severity":"warning","summary":FORGOTTEN_SUMMARY,"detail":FORGOTTEN_DETAIL}),
            ),
            (
                "diagnostic",
                json!({"severity":"error","summary":FORGOTTEN_SUMMARY,"detail":"generic retention notice"}),
            ),
        ] {
            let mut events = synthetic_events(&plan(GANG), true);
            events.last_mut().expect("notice")[key] = value;
            assert!(selected_evidence(&encode_events(&events)).is_err());
        }
        let mut unbound = synthetic_events(&plan(GANG), true);
        unbound
            .last_mut()
            .expect("notice")
            .as_object_mut()
            .expect("object")
            .remove("@testrun");
        assert!(selected_evidence(&encode_events(&unbound)).is_err());
        let mut generic = synthetic_events(&plan(GANG), true);
        *generic.last_mut().expect("notice") = json!({
            "type":"log","@level":"error","@message":FORGOTTEN_SUMMARY,
            "@testfile":TEST,"@testrun":"promote"
        });
        assert!(selected_evidence(&encode_events(&generic)).is_err());
    }

    #[test]
    fn requires_both_engine_retention_actions_and_generation_resource() {
        for (run, index) in [("retention", 0), ("transition", 0)] {
            let mut events = synthetic_events(&plan(GANG), true);
            event_payload(&mut events, run, "test_plan")["resource_changes"][index]["change"]["actions"] =
                json!(["delete", "create"]);
            assert!(selected_evidence(&encode_events(&events)).is_err());
            let mut missing = synthetic_events(&plan(GANG), true);
            event_payload(&mut missing, run, "test_plan")["resource_changes"] = json!([]);
            assert!(selected_evidence(&encode_events(&missing)).is_err());
        }
        let mut missing = synthetic_events(&plan(GANG), true);
        event_payload(&mut missing, "promote", "test_state")["values"]["root_module"]["resources"]
            .as_array_mut()
            .expect("applied resources")
            .retain(|resource| resource["address"] != "aws_ssm_parameter.config_generation");
        assert!(selected_evidence(&encode_events(&missing)).is_err());
    }

    #[test]
    fn ignores_ssm_data_lookups_in_applied_resources() {
        for unrelated in [false, true] {
            let mut events = synthetic_events(&plan(GANG), unrelated);
            event_payload(&mut events, "promote", "test_state")["values"]["root_module"]["resources"]
                .as_array_mut().expect("applied resources")
                .push(json!({"address":"data.aws_ssm_parameter.lookup","type":"aws_ssm_parameter","mode":"data"}));
            assert!(selected_evidence(&encode_events(&events)).is_ok());
        }
    }

    #[test]
    fn accepts_cleanup_notice_only_after_all_five_runs_and_payloads() {
        let mut original = synthetic_events(&plan(GANG), true);
        let notice = original.pop().expect("notice");
        let final_payload = original
            .iter()
            .position(|event| event["type"] == "test_plan" && event["@testrun"] == "retention")
            .expect("retention payload");
        for index in 0..=final_payload {
            let mut premature = original.clone();
            premature.insert(index, notice.clone());
            assert!(selected_evidence(&encode_events(&premature)).is_err());
        }
        let mut before_summary = original;
        before_summary.insert(final_payload + 1, notice);
        assert!(selected_evidence(&encode_events(&before_summary)).is_ok());
        before_summary.push(json!({
            "type":"test_run","@testfile":TEST,"@testrun":"retention",
            "test_run":{"path":TEST,"run":"retention","status":"pass"}
        }));
        assert!(selected_evidence(&encode_events(&before_summary)).is_err());
    }

    #[test]
    fn rejects_other_cleanup_failures_and_all_warning_events() {
        for event in [
            json!({"type":"test_cleanup","@level":"info","@testfile":TEST,"@testrun":"promote",
                "test_cleanup":{"failed_resources":[{"instance":"aws_ssm_parameter.config"}]}}),
            json!({"type":"diagnostic","@level":"error","@testfile":TEST,"@testrun":"promote",
                "diagnostic":{"severity":"error","summary":"cleanup failed","detail":"provider deletion failed"}}),
            json!({"type":"log","@level":"warn","@message":"warning"}),
            json!({"type":"log","@level":"warning","@message":"warning"}),
            json!({"type":"diagnostic","@level":"info","diagnostic":{"severity":"warning","summary":"warning"}}),
        ] {
            for unrelated in [false, true] {
                let mut events = synthetic_events(&plan(GANG), unrelated);
                events.push(event.clone());
                assert!(selected_evidence(&encode_events(&events)).is_err());
            }
        }
    }

    #[test]
    fn reports_warning_context_without_dumping_plan_values() {
        let event = json!({
            "type":"log", "@level":"warn", "@message":"provider shutdown failed",
            "test_plan":{"secret":"must not appear"}
        });
        let error = record_diagnostic(&event, &mut false).expect_err("warning is rejected");
        let message = error.to_string();
        assert!(message.contains("(log): provider shutdown failed"));
        assert!(!message.contains("must not appear"));
    }

    #[test]
    fn reports_cleanup_inventory_and_later_diagnostic_without_payloads() {
        let mut events = synthetic_events(&plan(GANG), true);
        events.push(json!({
            "type":"test_cleanup", "@level":"warn", "@message":"cleanup failed",
            "@testfile":TEST, "@testrun":"promote",
            "test_cleanup":{"failed_resources":[
                {"instance":"aws_ebs_volume.state", "deposed_key":"deadbeef"}
            ]},
            "test_plan":{"secret":"plan-secret-sentinel"},
            "test_state":{"secret":"state-secret-sentinel"}
        }));
        events.push(json!({
            "type":"diagnostic", "@level":"error", "@testfile":TEST, "@testrun":"promote",
            "diagnostic":{"severity":"error", "summary":"destroy blocked",
                "detail":"prevent_destroy retained state volume",
                "range":{"filename":"main.tf", "start":{"line":42}},
                "snippet":{"code":"snippet-secret-sentinel"}}
        }));
        let error = selected_evidence(&encode_events(&events))
            .err()
            .expect("cleanup warning is rejected");
        let message = error.to_string();
        let prefix = "OpenTofu emitted an error or warning (test_cleanup): cleanup failed";
        assert!(message.starts_with(prefix));
        for expected in [
            "aws_ebs_volume.state",
            "deadbeef",
            "destroy blocked",
            "prevent_destroy retained state volume",
            "cleanup diagnostic location: main.tf:42",
        ] {
            assert!(message.contains(expected));
        }
        assert!(!message.contains("plan-secret-sentinel"));
        assert!(!message.contains("state-secret-sentinel"));
        assert!(!message.contains("snippet-secret-sentinel"));
    }

    #[test]
    fn rejects_missing_duplicate_malformed_or_wrong_run_evidence() {
        let original = synthetic_events(&plan(GANG), true);
        for index in 0..original.len() {
            let mut missing = original.clone();
            missing.remove(index);
            assert!(selected_evidence(&encode_events(&missing)).is_err());
            let mut duplicate = original.clone();
            duplicate.push(original[index].clone());
            assert!(selected_evidence(&encode_events(&duplicate)).is_err());
        }
        for (index, key, value) in [
            (1, "@testrun", json!("wrong")),
            (
                1,
                "test_run",
                json!({"path":TEST,"run":"seed","status":"fail"}),
            ),
            (2, "type", json!("test_plan")),
            (2, "test_state", Value::Null),
            (
                12,
                "test_summary",
                json!({"status":"pass","passed":3,"failed":0,"errored":0,"skipped":0}),
            ),
        ] {
            let mut malformed = original.clone();
            malformed[index][key] = value;
            assert!(selected_evidence(&encode_events(&malformed)).is_err());
        }
        assert!(selected_evidence(b"not json").is_err());
    }

    #[test]
    fn extracts_worker_endpoint_and_asymmetric_health() {
        let fixture = extract(&stream(GANG), "local".into()).expect("fixture");
        assert_eq!(fixture.public.target_port, 8443);
        assert_eq!(fixture.workers.target_port, 9091);
        assert_eq!(fixture.workers.health.unhealthy_threshold, 3);
        assert_eq!(fixture.workers.health.healthy_threshold, 2);
    }

    #[test]
    fn rejects_missing_plan_or_nonconcrete_fields() {
        let gang = "endpoint = \"https://gang.example.com\"\nworker_endpoint = \"grpcs://gang.example.com:9090\"";
        let data = stream(gang);
        let mut events: Vec<Value> = data
            .split(|b| *b == b'\n')
            .map(|l| serde_json::from_slice(l).expect("event"))
            .collect();
        let event = events
            .iter_mut()
            .find(|event| event["type"] == "test_plan" && event["@testrun"] == "upgrade")
            .expect("upgrade");
        event["test_plan"]["planned_values"]["root_module"]["child_modules"][0]["resources"][1]["values"]
            ["port"] = Value::Null;
        let data = events
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(extract(data.as_bytes(), "local".into()).is_err());
        assert!(extract(b"{\"type\":\"test_summary\"}", "local".into()).is_err());
    }

    #[test]
    fn retains_each_rendered_health_path_for_published_and_candidate_shapes() {
        for (public_path, workers_path) in [
            ("/serve", "/leader"),
            ("/ready", "/ready"),
            ("/module-selected-path", "/worker-selected-path"),
        ] {
            let mut plan = plan(GANG);
            resources(&mut plan)[1]["values"]["health_check"][0]["path"] = json!(public_path);
            resources(&mut plan)[5]["values"]["health_check"][0]["path"] = json!(workers_path);
            let fixture = extract(&events(&plan), "local".into()).expect("fixture");
            assert_eq!(fixture.public.health.path, public_path);
            assert_eq!(fixture.workers.health.path, workers_path);
        }
    }

    #[test]
    fn extracts_per_target_transport_and_preservation() {
        for (preserve, expected) in [
            (json!(true), true),
            (json!("true"), true),
            (json!(false), false),
            (json!("false"), false),
        ] {
            let mut plan = plan(GANG);
            resources(&mut plan)[1]["values"]["proxy_protocol_v2"] = json!(true);
            resources(&mut plan)[1]["values"]["preserve_client_ip"] = preserve;
            let fixture = extract(&events(&plan), "local".into()).expect("fixture");
            assert!(fixture.public.proxy_protocol_v2);
            assert_eq!(fixture.public.preserve_client_ip, Some(expected));
            assert!(!fixture.workers.proxy_protocol_v2);
            assert_eq!(fixture.workers.preserve_client_ip, Some(true));
        }
    }

    fn default_proxy_plan(gang: &str) -> Value {
        let mut plan = plan(gang);
        let mut changes = Vec::new();
        let mut configured = Vec::new();
        for (index, name) in [(1, "this"), (3, "unrelated"), (5, "internal")] {
            let target = &mut resources(&mut plan)[index];
            target["values"]["proxy_protocol_v2"] = Value::Null;
            changes.push(json!({
                "address":target["address"],
                "change":{"actions":["no-op"],"after_unknown":{}}
            }));
            configured.push(json!({
                "address":format!("aws_lb_target_group.{name}"),
                "expressions":{"port":{"constant_value":target["values"]["port"]}}
            }));
        }
        plan["configuration"]["root_module"]["module_calls"]["load_balancer"] =
            json!({"module":{"resources":configured}});
        changes.append(plan["resource_changes"].as_array_mut().expect("changes"));
        plan["resource_changes"] = Value::Array(changes);
        plan
    }

    #[test]
    fn resolves_only_proven_omitted_proxy_protocol_provider_defaults() {
        let mut plan = default_proxy_plan(GANG);
        let fixture = extract(&events(&plan), "local".into()).expect("omitted defaults");
        assert!(!fixture.public.proxy_protocol_v2);
        assert!(!fixture.workers.proxy_protocol_v2);
        resources(&mut plan)[1]["values"]["proxy_protocol_v2"] = json!(true);
        plan["configuration"]["root_module"]["module_calls"]["load_balancer"]["module"]["resources"]
            [0]["expressions"]["proxy_protocol_v2"] = json!({"constant_value":true});
        let fixture = extract(&events(&plan), "local".into()).expect("explicit true");
        assert!(fixture.public.proxy_protocol_v2);
    }

    #[test]
    fn rejects_null_proxy_protocol_when_configured_unknown_or_unproven() {
        for expression in [
            json!({"constant_value":null}),
            json!({"constant_value":true}),
            json!({"references":["var.proxy_protocol_v2"]}),
        ] {
            let mut plan = default_proxy_plan(GANG);
            plan["configuration"]["root_module"]["module_calls"]["load_balancer"]["module"]["resources"]
                [0]["expressions"]["proxy_protocol_v2"] = expression;
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
        for unknown in [json!(true), Value::Null, json!("false")] {
            let mut plan = default_proxy_plan(GANG);
            plan["resource_changes"][0]["change"]["after_unknown"]["proxy_protocol_v2"] = unknown;
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
        for field in ["configuration", "resource_changes"] {
            let mut plan = default_proxy_plan(GANG);
            plan.as_object_mut().expect("plan").remove(field);
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
        let mut plan = default_proxy_plan(GANG);
        resources(&mut plan)[1]["values"]
            .as_object_mut()
            .expect("target")
            .remove("proxy_protocol_v2");
        assert!(extract(&events(&plan), "local".into()).is_err());
    }

    #[test]
    fn leaves_unknown_preservation_unknown() {
        for unknown in [
            Value::Null,
            json!("TRUE"),
            json!(" false"),
            json!(0),
            json!({}),
        ] {
            let mut plan = plan(GANG);
            resources(&mut plan)[1]["values"]["preserve_client_ip"] = unknown;
            let fixture = extract(&events(&plan), "local".into()).expect("fixture");
            assert_eq!(fixture.public.preserve_client_ip, None);
        }
        let mut plan = plan(GANG);
        resources(&mut plan)[1]["values"]
            .as_object_mut()
            .expect("target")
            .remove("preserve_client_ip");
        let fixture = extract(&events(&plan), "local".into()).expect("fixture");
        assert_eq!(fixture.public.preserve_client_ip, None);
    }

    #[test]
    fn rejects_missing_or_malformed_target_policy() {
        for (field, invalid) in [
            ("proxy_protocol_v2", Value::Null),
            ("proxy_protocol_v2", json!("true")),
            ("deregistration_delay", Value::Null),
            ("deregistration_delay", json!(-1)),
            ("deregistration_delay", json!("invalid")),
            ("deregistration_delay", json!(1.5)),
        ] {
            let mut invalid_plan = plan(GANG);
            resources(&mut invalid_plan)[1]["values"][field] = invalid;
            assert!(extract(&events(&invalid_plan), "local".into()).is_err());
            let mut plan = plan(GANG);
            resources(&mut plan)[1]["values"]
                .as_object_mut()
                .expect("target")
                .remove(field);
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
    }

    #[test]
    fn rejects_non_http_or_missing_health_policy() {
        for (field, invalid) in [
            ("protocol", Value::Null),
            ("protocol", json!("")),
            ("protocol", json!("TCP")),
            ("protocol", json!("HTTPS")),
            ("path", Value::Null),
            ("path", json!("")),
        ] {
            let mut plan = plan(GANG);
            resources(&mut plan)[1]["values"]["health_check"][0][field] = invalid;
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
        for field in ["protocol", "path"] {
            let mut plan = plan(GANG);
            resources(&mut plan)[1]["values"]["health_check"][0]
                .as_object_mut()
                .expect("health")
                .remove(field);
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
    }

    #[test]
    fn uses_only_associated_target_groups_for_maximum_drain() {
        let mut plan = plan(GANG);
        for (index, delay) in [(1, json!("0")), (3, json!("900")), (5, json!(120))] {
            resources(&mut plan)[index]["values"]["deregistration_delay"] = delay;
        }
        let fixture = extract(&events(&plan), "local".into()).expect("fixture");
        assert_eq!(fixture.controller.deregistration_delay_seconds, 120);
    }

    #[test]
    fn preserves_zero_drain_and_serializes_effective_policy() {
        let mut plan = plan(GANG);
        for index in [1, 5] {
            resources(&mut plan)[index]["values"]["deregistration_delay"] = json!("0");
        }
        let fixture = extract(&events(&plan), "local".into()).expect("fixture");
        assert_eq!(fixture.controller.deregistration_delay_seconds, 0);
        let value = serde_json::to_value(fixture).expect("serialized fixture");
        assert_eq!(value["public"]["proxy_protocol_v2"], json!(false));
        assert_eq!(value["public"]["preserve_client_ip"], json!(true));
        assert_eq!(value["controller"]["warmup_seconds"], json!(300));
        assert_eq!(
            value["controller"]["deregistration_delay_seconds"],
            json!(0)
        );
    }

    fn default_grace_plan() -> Value {
        let mut plan = plan(GANG);
        let group = &mut resources(&mut plan)[6];
        group["values"]["health_check_grace_period"] = Value::Null;
        plan
    }

    #[test]
    fn resolves_only_proven_omitted_health_grace_provider_default() {
        let mut plan = default_grace_plan();
        let fixture = extract(&events(&plan), "local".into()).expect("provider grace default");
        assert_eq!(fixture.controller.warmup_seconds, 300);
        resources(&mut plan)[6]["values"]["health_check_grace_period"] = json!(0);
        plan["configuration"]["root_module"]["module_calls"]["controller"]["module"]["resources"]
            [0]["expressions"]["health_check_grace_period"] = json!({"constant_value":0});
        let fixture = extract(&events(&plan), "local".into()).expect("explicit zero");
        assert_eq!(fixture.controller.warmup_seconds, 0);
    }

    #[test]
    fn rejects_null_health_grace_when_configured_unknown_or_unproven() {
        for expression in [
            json!({"constant_value":null}),
            json!({"references":["var.health_check_grace_period"]}),
        ] {
            let mut plan = default_grace_plan();
            plan["configuration"]["root_module"]["module_calls"]["controller"]["module"]["resources"]
                [0]["expressions"]["health_check_grace_period"] = expression;
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
        let mut plan = default_grace_plan();
        plan["resource_changes"][0]["change"]["after_unknown"]["health_check_grace_period"] =
            json!(true);
        assert!(extract(&events(&plan), "local".into()).is_err());
        for field in ["configuration", "resource_changes"] {
            let mut plan = default_grace_plan();
            plan.as_object_mut().expect("plan").remove(field);
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
    }

    #[test]
    fn warmup_precedence_preserves_zero_and_group_negative_fallback() {
        for (preference, default, expected) in [
            (json!(30), json!(40), 30),
            (json!(0), json!(40), 0),
            (Value::Null, json!(40), 40),
            (json!("21"), json!(40), 21),
            (Value::Null, json!(0), 0),
            (Value::Null, Value::Null, 300),
            (Value::Null, json!(-1), 300),
        ] {
            let mut plan = plan(GANG);
            let group = &mut resources(&mut plan)[6]["values"];
            group["default_instance_warmup"] = default;
            group["instance_refresh"][0]["preferences"][0]["instance_warmup"] = preference;
            let fixture = extract(&events(&plan), "local".into()).expect("fixture");
            assert_eq!(fixture.controller.warmup_seconds, expected);
        }
        let mut plan = plan(GANG);
        resources(&mut plan)[6]["values"]["health_check_grace_period"] = json!(0);
        assert_eq!(
            extract(&events(&plan), "local".into())
                .expect("zero grace")
                .controller
                .warmup_seconds,
            0
        );
    }

    #[test]
    fn parses_concrete_nullable_preference_integer_strings_without_scaling() {
        for (preference, expected) in [
            (json!("21"), 21),
            (json!("0"), 0),
            (json!("00021"), 21),
            (json!("+21"), 21),
            (json!("-0"), 0),
            (json!("2147483647"), 2_147_483_647),
            (json!(21), 21),
            (json!(0), 0),
        ] {
            let mut plan = plan(GANG);
            resources(&mut plan)[6]["values"]["instance_refresh"][0]["preferences"][0]["instance_warmup"] =
                preference.clone();
            plan["configuration"]["root_module"]["module_calls"]["controller"]["module"]["resources"]
                [0]["expressions"]["instance_refresh"][0]["preferences"][0]["instance_warmup"] =
                json!({"constant_value":preference});
            assert_eq!(
                extract(&events(&plan), "local".into())
                    .expect("preference")
                    .controller
                    .warmup_seconds,
                expected
            );
        }
        for invalid in [
            json!("-1"),
            json!(-1),
            json!("invalid"),
            json!("21ms"),
            json!("1.5"),
            json!(" 21"),
            json!("2147483648"),
            json!(2_147_483_648_u64),
            json!("18446744073709551616"),
        ] {
            let mut plan = plan(GANG);
            let group = &mut resources(&mut plan)[6]["values"];
            group["default_instance_warmup"] = json!(40);
            group["instance_refresh"][0]["preferences"][0]["instance_warmup"] = invalid;
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
    }

    #[test]
    fn falls_back_only_from_proven_known_unset_preferences() {
        for unset in [Value::Null, json!("")] {
            let mut plan = plan(GANG);
            let group = &mut resources(&mut plan)[6]["values"];
            group["instance_refresh"][0]["preferences"][0]["instance_warmup"] = unset;
            group["default_instance_warmup"] = json!(40);
            assert_eq!(
                extract(&events(&plan), "local".into())
                    .expect("known unset")
                    .controller
                    .warmup_seconds,
                40
            );
        }
        let mut omitted = plan(GANG);
        resources(&mut omitted)[6]["values"]["default_instance_warmup"] = json!(40);
        assert_eq!(
            extract(&events(&omitted), "local".into())
                .expect("omitted")
                .controller
                .warmup_seconds,
            40
        );
        for constant in [Value::Null, json!("")] {
            let mut explicit_unset = plan(GANG);
            resources(&mut explicit_unset)[6]["values"]["instance_refresh"][0]["preferences"][0]
                ["instance_warmup"] = constant.clone();
            explicit_unset["configuration"]["root_module"]["module_calls"]["controller"]["module"]
                ["resources"][0]["expressions"]["instance_refresh"][0]["preferences"][0]["instance_warmup"] =
                json!({"constant_value":constant});
            assert_eq!(
                extract(&events(&explicit_unset), "local".into())
                    .expect("constant unset")
                    .controller
                    .warmup_seconds,
                300
            );
        }
    }

    #[test]
    fn rejects_unknown_higher_priority_preference_without_default_fallback() {
        for unknown in [
            json!({"instance_refresh":true}),
            json!({"instance_refresh":[{"preferences":true}]}),
            json!({"instance_refresh":[{"preferences":[{"instance_warmup":true}]}]}),
        ] {
            let mut plan = plan(GANG);
            let group = &mut resources(&mut plan)[6]["values"];
            group["instance_refresh"][0]["preferences"][0]["instance_warmup"] = Value::Null;
            group["default_instance_warmup"] = json!(40);
            plan["resource_changes"][0]["change"]["after_unknown"] = unknown;
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
        let mut unresolved = plan(GANG);
        resources(&mut unresolved)[6]["values"]["default_instance_warmup"] = json!(40);
        unresolved["configuration"]["root_module"]["module_calls"]["controller"]["module"]["resources"]
            [0]["expressions"]["instance_refresh"][0]["preferences"][0]["instance_warmup"] =
            json!({"references":["var.instance_warmup"]});
        assert!(extract(&events(&unresolved), "local".into()).is_err());
        let mut missing = plan(GANG);
        missing
            .as_object_mut()
            .expect("plan")
            .remove("resource_changes");
        assert!(extract(&events(&missing), "local".into()).is_err());
    }

    #[test]
    fn rejects_missing_or_malformed_effective_warmup() {
        for invalid in [json!(-1), json!("300"), json!(1.5), json!({})] {
            let mut plan = plan(GANG);
            resources(&mut plan)[6]["values"]["health_check_grace_period"] = invalid;
            assert!(extract(&events(&plan), "local".into()).is_err());
        }
        for field in ["instance_warmup", "default_instance_warmup"] {
            for invalid in [json!(true), json!(1.5), json!({})] {
                let mut plan = plan(GANG);
                let group = &mut resources(&mut plan)[6]["values"];
                if field == "instance_warmup" {
                    group["instance_refresh"][0]["preferences"][0][field] = invalid;
                } else {
                    group[field] = invalid;
                }
                assert!(extract(&events(&plan), "local".into()).is_err());
            }
        }
        let mut string_default = plan(GANG);
        resources(&mut string_default)[6]["values"]["default_instance_warmup"] = json!("30");
        assert!(extract(&events(&string_default), "local".into()).is_err());
        let mut plan = plan(GANG);
        resources(&mut plan)[6]["values"]
            .as_object_mut()
            .expect("group")
            .remove("health_check_grace_period");
        assert!(extract(&events(&plan), "local".into()).is_err());
    }
}
