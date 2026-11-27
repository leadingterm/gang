//! Behaviour of `gang exec`'s entry point: the sandbox file comes from beside the entrypoint, the
//! environment cannot choose it or the action directory, and hard links break only when the
//! sandbox file asks for it.

use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::num::NonZeroU32;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use gang_config::{ExecutionConfig, InputLinks, SandboxFile};
use gang_exec::{ExecError, Invocation, run_with};
use gang_spi::{
    ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort, ActionLimits,
    ActionLimitsPort, ActionMemoryPool, ActionScope, ActionUsage, CPU_COUNT_ENV, MEMORY_KB_ENV,
    NETWORK_ENV, PortError, SIDE_CHANNEL_FILE_ENV, SandboxedAction,
};
use gang_testkit::{
    CallLog, Opaque, RecordedScope, RecordingLimits, SandboxConfig, SandboxKey, ScriptedSandbox,
    TestLimits, TestPlugins,
};
use gang_types::{Bytes, NonZeroBytes, NonZeroDuration};
use gang_types::{Capability, CpuWeight, FleetActionPolicy, PublicEgressPolicy};
use parking_lot::Mutex;
use tempfile::TempDir;

/// Built sandboxes and their markers, used to identify which sandboxes actually started.
type Events = Arc<Mutex<Vec<(String, ScriptedSandbox)>>>;

/// The markers of the sandboxes that started, in build order.
fn started_markers(events: &Events) -> Vec<String> {
    events
        .lock()
        .iter()
        .filter(|(_, sandbox)| !sandbox.started().is_empty())
        .map(|(marker, _)| marker.clone())
        .collect()
}

/// What the builders of one run were called with: the sandbox and limits builds, and the limits
/// the limits plugin was entered with.
struct Calls {
    sandbox_builds: AtomicUsize,
    limits_builds: AtomicUsize,
    limits: RecordingLimits,
}

impl Default for Calls {
    fn default() -> Self {
        Self {
            sandbox_builds: AtomicUsize::new(0),
            limits_builds: AtomicUsize::new(0),
            limits: RecordingLimits::new(&CallLog::default()),
        }
    }
}

/// The text of a sandbox file whose `noop` sandbox carries `marker` and whose `[limits]` section
/// is `limits`.
fn sandbox_file(
    work_directory: &Path,
    marker: &str,
    input_links: InputLinks,
    limits: Option<TestLimits>,
) -> Result<String, Box<dyn Error>> {
    let file = SandboxFile::<TestPlugins> {
        work_directory: gang_types::AbsolutePathBuf::try_from(work_directory)?,
        selected: SandboxKey::Noop,
        sandbox: SandboxConfig {
            noop: Some(Opaque(
                [
                    (String::from("marker"), marker.to_owned().into()),
                    (String::from("environment"), "host:test".into()),
                ]
                .into_iter()
                .collect(),
            )),
            ..SandboxConfig::default()
        },
        execution: ExecutionConfig {
            input_links,
            input_trees: gang_config::InputTrees::default(),
            egress: None,
        },
        actions: FleetActionPolicy::default(),
        controller_endpoints: Box::default(),
        limits,
    };
    Ok(file.to_toml()?)
}

/// A temporary tree: `bin/` holds the entrypoint and its sandbox files, `root/` is the work root
/// with the action `a1`, whose `inputs/shared` is hard-linked to `cache/shared`.
struct Fixture {
    _temp: TempDir,
    base: PathBuf,
    root: PathBuf,
    entrypoint: PathBuf,
    working_directory: PathBuf,
    outside: PathBuf,
    decoy: PathBuf,
    shared: PathBuf,
    partner: PathBuf,
}

impl Fixture {
    fn new(input_links: InputLinks) -> Result<Self, Box<dyn Error>> {
        let temp = tempfile::tempdir()?;
        let base = temp.path().to_path_buf();
        let bin = base.join("bin");
        let root = base.join("root");
        let working_directory = root.join("a1/work/sub");
        let shared = root.join("a1/inputs/shared");
        let partner = base.join("cache/shared");
        let outside = base.join("outside");
        let decoy = base.join("decoy");
        for directory in [
            &bin,
            &working_directory,
            &root.join("a1/inputs"),
            &base.join("cache"),
            &outside,
            &decoy,
        ] {
            fs::create_dir_all(directory)?;
        }
        fs::write(&partner, "cached")?;
        fs::hard_link(&partner, &shared)?;
        let entrypoint = bin.join("gang-exec");
        fs::write(&entrypoint, "")?;
        fs::write(
            bin.join("sandbox.toml"),
            sandbox_file(&root, "trusted", input_links, None)?,
        )?;
        fs::write(
            bin.join("attacker.toml"),
            sandbox_file(&root, "attacker", InputLinks::Keep, None)?,
        )?;
        Ok(Self {
            _temp: temp,
            base,
            root,
            entrypoint,
            working_directory,
            outside,
            decoy,
            shared,
            partner,
        })
    }
}

fn argv() -> Vec<OsString> {
    vec![OsString::from("true")]
}

/// Runs the action in the composed test sandbox, with the limits double.
fn run_action(
    argv: &[OsString],
    entrypoint: &Path,
    working_directory: &Path,
    environment: &[(OsString, OsString)],
    started: &Events,
) -> Result<ExitCode, ExecError> {
    run_action_counting(
        argv,
        entrypoint,
        working_directory,
        environment,
        started,
        &Calls::default(),
    )
}

/// Runs the action as [`run_action`] does, counting the builds and entries in `calls`.
fn run_action_counting(
    argv: &[OsString],
    entrypoint: &Path,
    working_directory: &Path,
    environment: &[(OsString, OsString)],
    started: &Events,
    calls: &Calls,
) -> Result<ExitCode, ExecError> {
    let invocation = Invocation {
        entrypoint,
        argv,
        environment,
        working_directory,
    };
    run_with::<TestPlugins, ScriptedSandbox, RecordingLimits>(
        &invocation,
        |_key, sandbox, _context| {
            calls.sandbox_builds.fetch_add(1, Ordering::SeqCst);
            let section = sandbox
                .noop
                .ok_or_else(|| PortError::backend("missing sandbox"))?;
            let marker = section
                .0
                .get("marker")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_owned();
            let environment = section
                .0
                .get("environment")
                .and_then(|value| value.as_str())
                .ok_or_else(|| PortError::backend("missing sandbox environment"))?
                .parse()
                .map_err(PortError::backend)?;
            let sandbox = ScriptedSandbox::try_exiting(
                &CallLog::default(),
                ActionExecutionOutcome::Exited(0),
            )
            .map_err(PortError::backend)?
            .with_environment(environment);
            started.lock().push((marker, sandbox.clone()));
            Ok(sandbox)
        },
        |_limits, _context| {
            calls.limits_builds.fetch_add(1, Ordering::SeqCst);
            Ok(calls.limits.clone())
        },
        || Ok(()),
    )
}

/// The error of a run that must fail.
fn error_of(result: Result<ExitCode, ExecError>) -> Result<ExecError, Box<dyn Error>> {
    match result {
        Ok(code) => Err(format!("expected an error, got {code:?}").into()),
        Err(error) => Ok(error),
    }
}

#[test]
fn network_is_authorized_only_by_the_selected_fleet_file() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let path = fixture.entrypoint.with_file_name("sandbox.toml");
    let mut file: SandboxFile<TestPlugins> =
        sandbox_file(&fixture.root, "trusted", InputLinks::Keep, None)?.parse()?;
    let policy = PublicEgressPolicy {
        protected_networks: vec!["10.0.0.0/8".parse()?].into_boxed_slice(),
        dns_servers: vec!["8.8.8.8".parse()?].into_boxed_slice(),
        setup_timeout: NonZeroDuration::from_secs(10),
    };
    let endpoints = vec!["https://controller.example:443".parse()?].into_boxed_slice();
    file.execution.egress = Some(policy.clone());
    file.controller_endpoints = endpoints.clone();
    fs::write(&path, file.to_toml()?)?;
    let request = [(OsString::from(NETWORK_ENV), OsString::from("requested"))];
    let started = Events::default();
    let calls = Calls::default();
    assert!(matches!(
        run_action_counting(
            &argv(),
            &fixture.entrypoint,
            &fixture.working_directory,
            &request,
            &started,
            &calls
        ),
        Err(ExecError::NetworkNotGranted)
    ));
    assert_eq!(calls.sandbox_builds.load(Ordering::SeqCst), 0);

    file.selected = SandboxKey::Bwrap;
    file.sandbox.bwrap = Some(Opaque(
        [(String::from("egress"), true.into())]
            .into_iter()
            .collect(),
    ));
    file.actions.grants.insert(Capability::Network);
    fs::write(&path, file.to_toml()?)?;
    let args = argv();
    let invocation = Invocation {
        entrypoint: &fixture.entrypoint,
        argv: &args,
        environment: &request,
        working_directory: &fixture.working_directory,
    };
    let sandbox =
        ScriptedSandbox::try_exiting(&CallLog::default(), ActionExecutionOutcome::Exited(0))?;
    run_with::<TestPlugins, ScriptedSandbox, RecordingLimits>(
        &invocation,
        |_, _, _| Ok(sandbox.clone()),
        |_, _| Ok(RecordingLimits::new(&CallLog::default())),
        || Ok(()),
    )?;
    assert_eq!(sandbox.started()[0].network, Some((policy, endpoints)));

    for value in ["", "not-requested"] {
        let env = [(OsString::from(NETWORK_ENV), OsString::from(value))];
        let invocation = Invocation {
            environment: &env,
            ..invocation
        };
        let sandbox =
            ScriptedSandbox::try_exiting(&CallLog::default(), ActionExecutionOutcome::Exited(0))?;
        run_with::<TestPlugins, ScriptedSandbox, RecordingLimits>(
            &invocation,
            |_, _, _| Ok(sandbox.clone()),
            |_, _| Ok(RecordingLimits::new(&CallLog::default())),
            || Ok(()),
        )?;
        assert_eq!(sandbox.started()[0].network, None);
    }
    Ok(())
}

/// The link count of the file at `path`.
fn link_count(path: &Path) -> Result<u64, io::Error> {
    Ok(fs::metadata(path)?.nlink())
}

#[test]
fn ignores_a_sandbox_file_named_by_the_environment() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let started = Events::default();
    let environment = [(
        OsString::from("GANG_SANDBOX_FILE"),
        fixture
            .entrypoint
            .with_file_name("attacker.toml")
            .into_os_string(),
    )];

    run_action(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &environment,
        &started,
    )?;

    assert_eq!(started_markers(&started), ["trusted"]);
    Ok(())
}

#[test]
fn breaks_every_link_under_the_action_directory_whatever_the_environment_says()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Break)?;
    let started = Events::default();
    let environment = [(
        OsString::from("GANG_ACTION_DIRECTORY"),
        fixture.decoy.clone().into_os_string(),
    )];

    run_action(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &environment,
        &started,
    )?;

    assert_eq!(link_count(&fixture.shared)?, 1);
    assert_eq!(link_count(&fixture.partner)?, 1);
    assert_eq!(started_markers(&started), ["trusted"]);
    Ok(())
}

#[test]
fn keep_leaves_links_alone() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let started = Events::default();

    run_action(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &[],
        &started,
    )?;

    assert_eq!(link_count(&fixture.shared)?, 2);
    assert_eq!(link_count(&fixture.partner)?, 2);
    assert_eq!(started_markers(&started), ["trusted"]);
    Ok(())
}

#[test]
fn a_working_directory_outside_the_work_directory_is_rejected_whatever_input_links_says()
-> Result<(), Box<dyn Error>> {
    for input_links in [InputLinks::Keep, InputLinks::Break] {
        let fixture = Fixture::new(input_links)?;
        let started = Events::default();

        let error = error_of(run_action(
            &argv(),
            &fixture.entrypoint,
            &fixture.outside,
            &[],
            &started,
        ))?;

        assert!(matches!(error, ExecError::OutsideWorkDirectory { .. }));
        assert!(started_markers(&started).is_empty());
    }
    Ok(())
}

#[test]
fn the_sandbox_gets_the_action_directory_and_the_working_directory_inside_it()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let started = Events::default();

    run_action(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &[],
        &started,
    )?;

    let events = started.lock();
    let sandboxes: Vec<_> = events
        .iter()
        .filter(|(_, sandbox)| !sandbox.started().is_empty())
        .collect();
    assert_eq!(sandboxes.len(), 1);
    let actions = sandboxes[0].1.started();
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].action_directory,
        fixture.root.canonicalize()?.join("a1")
    );
    assert_eq!(
        actions[0].working_directory,
        fixture.root.canonicalize()?.join("a1/work/sub")
    );
    Ok(())
}

#[test]
fn a_relative_entrypoint_is_rejected() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let started = Events::default();

    let error = error_of(run_action(
        &argv(),
        Path::new("bin/gang-exec"),
        &fixture.working_directory,
        &[],
        &started,
    ))?;

    assert!(matches!(error, ExecError::EntrypointNotAbsolute { .. }));
    assert!(started_markers(&started).is_empty());
    Ok(())
}

#[test]
fn an_empty_command_is_rejected() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let started = Events::default();

    let error = error_of(run_action(
        &[],
        &fixture.entrypoint,
        &fixture.working_directory,
        &[],
        &started,
    ))?;

    assert!(matches!(error, ExecError::NoCommand));
    assert!(started_markers(&started).is_empty());
    Ok(())
}

#[test]
fn a_missing_sandbox_file_is_a_read_error() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let started = Events::default();
    let entrypoint = fixture.base.join("missing/gang-exec");

    let error = error_of(run_action(
        &argv(),
        &entrypoint,
        &fixture.working_directory,
        &[],
        &started,
    ))?;

    assert!(matches!(error, ExecError::ReadSandboxFile { .. }));
    assert!(started_markers(&started).is_empty());
    Ok(())
}

#[test]
fn limits_section_builds_the_limits_plugin_and_enter_receives_the_requested_limits()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let text = sandbox_file(
        &fixture.root,
        "trusted",
        InputLinks::Keep,
        Some(TestLimits { max_pids: 1 }),
    )?;
    fs::write(fixture.entrypoint.with_file_name("sandbox.toml"), text)?;
    let started = Events::default();
    let calls = Calls::default();
    let environment = [
        (OsString::from(CPU_COUNT_ENV), OsString::from("2")),
        (OsString::from(MEMORY_KB_ENV), OsString::from("1024")),
    ];

    run_action_counting(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &environment,
        &started,
        &calls,
    )?;

    assert_eq!(calls.limits_builds.load(Ordering::SeqCst), 1);
    assert_eq!(
        calls.limits.entered(),
        [ActionLimits {
            cpu_weight: None,
            cpu_count: NonZeroU32::new(2),
            memory: Some(NonZeroBytes::mib(1)),
        }]
    );
    assert_eq!(started_markers(&started), ["trusted"]);
    Ok(())
}
#[test]
fn reports_oom_without_exposing_path_or_changing_status() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let text = sandbox_file(
        &fixture.root,
        "trusted",
        InputLinks::Keep,
        Some(TestLimits { max_pids: 1 }),
    )?;
    fs::write(fixture.entrypoint.with_file_name("sandbox.toml"), text)?;
    let alias = fixture.base.join("work-alias");
    std::os::unix::fs::symlink(&fixture.root, &alias)?;
    fs::write(
        fixture.entrypoint.with_file_name("sandbox.toml"),
        sandbox_file(
            &alias,
            "trusted",
            InputLinks::Keep,
            Some(TestLimits { max_pids: 1 }),
        )?,
    )?;
    let path = alias.join(".side_channels/a1/report");
    fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
    let environment = [
        (
            OsString::from(SIDE_CHANNEL_FILE_ENV),
            path.clone().into_os_string(),
        ),
        (OsString::from("VISIBLE"), OsString::from("yes")),
    ];
    let calls = Calls {
        limits: RecordingLimits::new(&CallLog::default()).with_oom_kills(1),
        ..Calls::default()
    };
    let started = Events::default();
    let status = run_action_counting(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &environment,
        &started,
        &calls,
    )?;
    assert_eq!(status, ExitCode::SUCCESS);
    assert_eq!(
        fs::read(&path)?,
        br#"{"failure":"memory","peak_memory_kb":4}"#
    );
    let sandbox = started.lock()[0].1.clone();
    assert_eq!(
        sandbox.started()[0].environment,
        vec![(OsString::from("VISIBLE"), OsString::from("yes"))]
    );
    // A second report cannot replace the first and must not alter the action's status.
    let status = run_action_counting(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &environment,
        &started,
        &calls,
    )?;
    assert_eq!(status, ExitCode::SUCCESS);
    Ok(())
}
#[test]
fn oom_report_preserves_nonzero_action_status() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let text = sandbox_file(
        &fixture.root,
        "trusted",
        InputLinks::Keep,
        Some(TestLimits { max_pids: 1 }),
    )?;
    fs::write(fixture.entrypoint.with_file_name("sandbox.toml"), text)?;
    let path = fixture.root.join(".side_channels/a1/report");
    fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
    let environment = [(
        OsString::from(SIDE_CHANNEL_FILE_ENV),
        path.clone().into_os_string(),
    )];
    let arguments = argv();
    let invocation = Invocation {
        entrypoint: &fixture.entrypoint,
        argv: &arguments,
        environment: &environment,
        working_directory: &fixture.working_directory,
    };
    let status = run_with::<TestPlugins, ScriptedSandbox, RecordingLimits>(
        &invocation,
        |_, _, _| {
            ScriptedSandbox::try_exiting(&CallLog::default(), ActionExecutionOutcome::Signaled(9))
                .map_err(PortError::backend)
        },
        |_, _| Ok(RecordingLimits::new(&CallLog::default()).with_oom_kills(1)),
        || Ok(()),
    )?;
    assert_eq!(status, ExitCode::from(137));
    assert_eq!(
        fs::read(path)?,
        br#"{"failure":"memory","peak_memory_kb":4}"#
    );
    Ok(())
}

#[test]
fn setup_failure_reports_scope_usage_and_keeps_original_error() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    fs::write(
        fixture.entrypoint.with_file_name("sandbox.toml"),
        sandbox_file(
            &fixture.root,
            "trusted",
            InputLinks::Keep,
            Some(TestLimits { max_pids: 1 }),
        )?,
    )?;
    let path = fixture.root.join(".side_channels/a1/report");
    fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
    let env = [(
        OsString::from(SIDE_CHANNEL_FILE_ENV),
        path.clone().into_os_string(),
    )];
    let args = argv();
    let invocation = Invocation {
        entrypoint: &fixture.entrypoint,
        argv: &args,
        environment: &env,
        working_directory: &fixture.working_directory,
    };
    let log = CallLog::default();
    for (kills, expected) in [
        (1, br#"{"failure":"memory","peak_memory_kb":4}"#.as_slice()),
        (0, br#"{"peak_memory_kb":4}"#.as_slice()),
    ] {
        let sandbox = ScriptedSandbox::try_failing_to_start(&log)?;
        let error = error_of(run_with::<TestPlugins, ScriptedSandbox, RecordingLimits>(
            &invocation,
            |_, _, _| Ok(sandbox),
            |_, _| Ok(RecordingLimits::new(&log).with_oom_kills(kills)),
            || Ok(()),
        ))?;
        assert!(matches!(error, ExecError::Sandbox(_)));
        assert_eq!(fs::read(&path)?, expected);
        fs::remove_file(&path)?;
    }
    let target = fixture.base.join("victim");
    std::os::unix::fs::symlink(&target, &path)?;
    let sandbox = ScriptedSandbox::try_failing_to_start(&log)?;
    let error = error_of(run_with::<TestPlugins, ScriptedSandbox, RecordingLimits>(
        &invocation,
        |_, _, _| Ok(sandbox),
        |_, _| Ok(RecordingLimits::new(&log).with_oom_kills(1)),
        || Ok(()),
    ))?;
    assert!(matches!(error, ExecError::Sandbox(_)));
    assert!(!target.exists());
    Ok(())
}

#[test]
fn no_limits_section_never_builds_the_limits_plugin() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let started = Events::default();
    let calls = Calls::default();

    run_action_counting(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &[],
        &started,
        &calls,
    )?;

    assert_eq!(calls.limits_builds.load(Ordering::SeqCst), 0);
    assert_eq!(started_markers(&started), ["trusted"]);
    Ok(())
}

#[test]
fn a_malformed_sandbox_file_is_an_error_and_the_sandbox_never_starts() -> Result<(), Box<dyn Error>>
{
    let fixture = Fixture::new(InputLinks::Keep)?;
    fs::write(
        fixture.entrypoint.with_file_name("sandbox.toml"),
        "work_directory = [\n",
    )?;
    let started = Events::default();
    let calls = Calls::default();

    let error = error_of(run_action_counting(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &[],
        &started,
        &calls,
    ))?;

    assert!(matches!(error, ExecError::SandboxFile { .. }));
    assert_eq!(calls.sandbox_builds.load(Ordering::SeqCst), 0);
    assert!(started_markers(&started).is_empty());
    Ok(())
}

#[derive(Debug)]
struct ScopeWithCgroup(RecordedScope);

impl ActionScope for ScopeWithCgroup {
    fn launcher(&self) -> &[OsString] {
        self.0.launcher()
    }

    fn memory_cgroup(&self) -> Option<PathBuf> {
        Some(PathBuf::from("/sys/fs/cgroup/actions/123-456-7"))
    }
}

struct LimitsWithCgroup(RecordingLimits);

impl ActionLimitsPort for LimitsWithCgroup {
    type Scope = ScopeWithCgroup;

    async fn prepare(&self, reserve: Bytes) -> Result<Option<ActionMemoryPool>, PortError> {
        self.0.prepare(reserve).await
    }

    async fn enter(&self, limits: &ActionLimits) -> Result<Self::Scope, PortError> {
        self.0.enter(limits).await.map(ScopeWithCgroup)
    }

    async fn leave(&self, scope: Self::Scope) -> Result<ActionUsage, PortError> {
        self.0.leave(scope.0).await
    }

    async fn reap_abandoned(&self, creator: u32) -> Result<(), PortError> {
        self.0.reap_abandoned(creator).await
    }
}

struct MetadataCheckingSandbox(PathBuf, gang_types::ActionEnvironment);

impl ActionIsolationPort for MetadataCheckingSandbox {
    fn environment(&self) -> &gang_types::ActionEnvironment {
        &self.1
    }

    async fn start(&self, _: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        let metadata = fs::read(&self.0).map_err(PortError::backend)?;
        if metadata != b"/sys/fs/cgroup/actions/123-456-7" {
            return Err(PortError::backend(io::Error::other(
                "incorrect cgroup metadata",
            )));
        }
        Ok(
            SandboxedAction::new(async { Ok(ActionExecutionOutcome::Exited(0)) })
                .with_accounting_pid(Some(12345)),
        )
    }
}

#[test]
fn publishes_scope_metadata_before_start_with_aliased_work_root() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let alias = fixture.base.join("work-alias");
    std::os::unix::fs::symlink(&fixture.root, &alias)?;
    fs::write(
        fixture.entrypoint.with_file_name("sandbox.toml"),
        sandbox_file(
            &alias,
            "trusted",
            InputLinks::Keep,
            Some(TestLimits { max_pids: 9 }),
        )?,
    )?;
    let path = alias.join(".side_channels/a1/report");
    fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
    let env = [(
        OsString::from(SIDE_CHANNEL_FILE_ENV),
        path.clone().into_os_string(),
    )];
    let arguments = argv();
    let invocation = Invocation {
        entrypoint: &fixture.entrypoint,
        argv: &arguments,
        environment: &env,
        working_directory: &fixture.working_directory,
    };
    let metadata = path.with_file_name("report.cgroup");
    let status = run_with::<TestPlugins, MetadataCheckingSandbox, LimitsWithCgroup>(
        &invocation,
        |_, _, _| {
            Ok(MetadataCheckingSandbox(
                metadata.clone(),
                "host:test".parse().map_err(PortError::config)?,
            ))
        },
        |_, _| Ok(LimitsWithCgroup(RecordingLimits::new(&CallLog::default()))),
        || Ok(()),
    )?;
    assert_eq!(status, ExitCode::SUCCESS);
    assert_eq!(fs::read(&path)?, br#"{"peak_memory_kb":4}"#);
    assert_eq!(fs::read(path.with_file_name("report.pid"))?, b"12345");
    Ok(())
}

#[test]
fn refuses_planted_report_symlink_under_aliased_root() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    let alias = fixture.base.join("work-alias");
    std::os::unix::fs::symlink(&fixture.root, &alias)?;
    fs::write(
        fixture.entrypoint.with_file_name("sandbox.toml"),
        sandbox_file(
            &alias,
            "trusted",
            InputLinks::Keep,
            Some(TestLimits { max_pids: 9 }),
        )?,
    )?;
    let path = alias.join(".side_channels/a1/report");
    fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
    let target = fixture.base.join("victim");
    std::os::unix::fs::symlink(&target, &path)?;
    let env = [(OsString::from(SIDE_CHANNEL_FILE_ENV), path.into_os_string())];
    let calls = Calls {
        limits: RecordingLimits::new(&CallLog::default()).with_oom_kills(2),
        ..Calls::default()
    };
    assert_eq!(
        run_action_counting(
            &argv(),
            &fixture.entrypoint,
            &fixture.working_directory,
            &env,
            &Events::default(),
            &calls
        )?,
        ExitCode::SUCCESS
    );
    assert!(!target.exists());
    Ok(())
}

struct MountSandbox {
    inner: ScriptedSandbox,
    source: PathBuf,
    target: PathBuf,
    scratch: PathBuf,
}

impl ActionIsolationPort for MountSandbox {
    fn environment(&self) -> &gang_types::ActionEnvironment {
        self.inner.environment()
    }

    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        assert_eq!(spec.read_only_trees.len(), 1);
        assert_eq!(spec.read_only_trees[0].source, self.source.as_path());
        assert_eq!(spec.read_only_trees[0].target, self.target.as_path());
        assert_eq!(spec.ephemeral_dirs, &[self.scratch.as_path()]);
        self.inner.start(spec).await
    }
}

#[test]
fn protected_inputs_reach_the_sandbox_and_limits() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    fs::write(
        fixture.entrypoint.with_file_name("sandbox.toml"),
        sandbox_file(
            &fixture.root,
            "trusted",
            InputLinks::Keep,
            Some(TestLimits { max_pids: 1 }),
        )?,
    )?;
    let root = fixture.root.canonicalize()?;
    let action = root.join("a1");
    let channel = root.join(".side_channels/a1");
    fs::create_dir_all(&channel)?;
    fs::create_dir_all(action.join("work/tmp"))?;
    fs::write(channel.join("read_only_trees"), b"/cas/tree\0inputs\0")?;
    fs::write(channel.join("ephemeral_dirs"), b"work/tmp\0")?;
    fs::write(channel.join("cpu_weight"), b"1000\n")?;
    let environment = [
        (
            SIDE_CHANNEL_FILE_ENV.into(),
            channel.join("report").into_os_string(),
        ),
        (OsString::from("GANG_CPU_WEIGHT"), OsString::from("20")),
    ];
    let inner =
        ScriptedSandbox::try_exiting(&CallLog::default(), ActionExecutionOutcome::Exited(0))?;
    let limits = RecordingLimits::new(&CallLog::default());
    let outcome = run_with::<TestPlugins, MountSandbox, RecordingLimits>(
        &Invocation {
            entrypoint: &fixture.entrypoint,
            argv: &argv(),
            environment: &environment,
            working_directory: &fixture.working_directory,
        },
        |_, _, _| {
            Ok(MountSandbox {
                inner: inner.clone(),
                source: PathBuf::from("/cas/tree"),
                target: action.join("inputs"),
                scratch: action.join("work/tmp"),
            })
        },
        |_, _| Ok(limits.clone()),
        || Ok(()),
    )?;
    assert_eq!(outcome, ExitCode::SUCCESS);
    assert_eq!(inner.started().len(), 1);
    assert_eq!(limits.entered().len(), 1);
    assert_eq!(limits.entered()[0].cpu_weight, Some(CpuWeight::GATE));
    Ok(())
}

#[test]
fn ignores_cpu_weight_from_client_environment_without_side_channel() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(InputLinks::Keep)?;
    fs::write(
        fixture.entrypoint.with_file_name("sandbox.toml"),
        sandbox_file(
            &fixture.root,
            "trusted",
            InputLinks::Keep,
            Some(TestLimits { max_pids: 4 }),
        )?,
    )?;
    let calls = Calls::default();
    let started = Events::default();
    let environment = [(OsString::from("GANG_CPU_WEIGHT"), OsString::from("1000"))];

    let status = run_action_counting(
        &argv(),
        &fixture.entrypoint,
        &fixture.working_directory,
        &environment,
        &started,
        &calls,
    )?;
    assert_eq!(status, ExitCode::SUCCESS);
    assert_eq!(
        calls.limits.entered(),
        [ActionLimits {
            cpu_count: None,
            memory: None,
            cpu_weight: None,
        }]
    );
    Ok(())
}

/// Real host-group-sharing sandbox matching `Noop` for verifying descendant cleanup on normal exit.
struct HostGroupSandbox(gang_types::ActionEnvironment);

impl ActionIsolationPort for HostGroupSandbox {
    fn environment(&self) -> &gang_types::ActionEnvironment {
        &self.0
    }

    async fn start(&self, spec: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        let mut command = tokio::process::Command::new(spec.command.program());
        command
            .args(spec.command.args())
            .current_dir(spec.working_directory)
            .env_clear()
            .envs(spec.environment.iter().map(|(k, v)| (k, v)))
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(PortError::backend)?;
        let accounting_pid = child.id();
        Ok(SandboxedAction::new(async move {
            let status = child.wait().await.map_err(PortError::backend)?;
            ActionExecutionOutcome::try_from(status)
        })
        .with_accounting_pid(accounting_pid))
    }
}

const DESCENDANT_HOST: gang_testkit::ChildTest =
    gang_testkit::ChildTest::new("descendant_cleanup_host");

#[ignore = "child process helper of successful_noop_action_drains_surviving_descendant_with_closed_stdio"]
#[test]
fn descendant_cleanup_host() -> Result<(), Box<dyn Error>> {
    let entrypoint =
        PathBuf::from(std::env::var_os("TEST_ENTRYPOINT").ok_or("TEST_ENTRYPOINT is required")?);
    let pid_file = PathBuf::from(
        std::env::var_os("TEST_DESCENDANT_PID").ok_or("TEST_DESCENDANT_PID is required")?,
    );
    let script = format!(
        "sleep 60 </dev/null >/dev/null 2>&1 & echo $! > '{}'; exit 0",
        pid_file.display()
    );
    let argv = [
        OsString::from("/bin/sh"),
        OsString::from("-c"),
        OsString::from(script),
    ];
    let code = gang_exec::run::<TestPlugins, HostGroupSandbox, RecordingLimits>(
        &entrypoint,
        &argv,
        |_, _, _| {
            Ok(HostGroupSandbox(
                "host:test".parse().map_err(PortError::config)?,
            ))
        },
        |_, _| Ok(RecordingLimits::new(&CallLog::default())),
    )?;
    assert_eq!(code, ExitCode::SUCCESS);
    Ok(())
}

#[test]
fn successful_noop_action_drains_surviving_descendant_with_closed_stdio()
-> Result<(), Box<dyn Error>> {
    use std::os::unix::process::CommandExt as _;

    let fixture = Fixture::new(InputLinks::Keep)?;
    let pid_file = fixture.working_directory.join("descendant.pid");
    let status = DESCENDANT_HOST
        .command()?
        .current_dir(&fixture.working_directory)
        .env("TEST_ENTRYPOINT", &fixture.entrypoint)
        .env("TEST_DESCENDANT_PID", &pid_file)
        .process_group(0)
        .status()?;
    assert!(status.success(), "host helper exited with {status}");

    let raw_pid: i32 = fs::read_to_string(&pid_file)?.trim().parse()?;
    let pid = rustix::process::Pid::from_raw(raw_pid).ok_or("invalid descendant pid")?;
    let probe = rustix::process::test_kill_process(pid);
    if probe.is_ok() {
        let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
        return Err(format!(
            "surviving background descendant {raw_pid} outlived successful noop action"
        )
        .into());
    }
    assert_eq!(probe, Err(rustix::io::Errno::SRCH));
    Ok(())
}

#[cfg(target_os = "linux")]
const SETSID_ACTION: gang_testkit::ChildTest = gang_testkit::ChildTest::new("setsid_action_helper");

#[cfg(target_os = "linux")]
#[ignore = "child helper for the session cleanup regression"]
#[test]
fn setsid_action_helper() -> Result<(), Box<dyn Error>> {
    let path = PathBuf::from(std::env::var_os("TEST_DESCENDANT_PID").ok_or("missing pid file")?);
    if std::env::var_os("TEST_SETSID_CHILD").is_some() {
        rustix::process::setsid().expect("setsid must succeed");
        let pid = rustix::process::getpid();
        let sid = rustix::process::getsid(None)?;
        assert_eq!(sid, pid);
        let temporary = path.with_extension("tmp");
        fs::write(
            &temporary,
            format!("{} {}", pid.as_raw_nonzero(), sid.as_raw_nonzero()),
        )?;
        fs::rename(temporary, path)?;
        std::thread::sleep(std::time::Duration::from_secs(30));
        return Ok(());
    }
    let _child = SETSID_ACTION
        .command()?
        .env("TEST_SETSID_CHILD", "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            return Err("setsid child did not become ready".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[ignore = "host helper for the session cleanup regression"]
#[test]
fn setsid_cleanup_host() -> Result<(), Box<dyn Error>> {
    let entrypoint =
        PathBuf::from(std::env::var_os("TEST_ENTRYPOINT").ok_or("missing entrypoint")?);
    let mut argv = vec![std::env::current_exe()?.into_os_string()];
    let action = if std::env::var_os("TEST_NESTED_SETSID").is_some() {
        NESTED_ACTION
    } else {
        SETSID_ACTION
    };
    argv.extend(action.arguments().into_iter().map(OsString::from));
    let code = gang_exec::run::<TestPlugins, HostGroupSandbox, RecordingLimits>(
        &entrypoint,
        &argv,
        |_, _, _| {
            Ok(HostGroupSandbox(
                "host:test".parse().map_err(PortError::config)?,
            ))
        },
        |_, _| Ok(RecordingLimits::new(&CallLog::default())),
    )?;
    assert_eq!(code, ExitCode::SUCCESS);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn successful_noop_action_drains_setsid_descendant_with_closed_stdio() -> Result<(), Box<dyn Error>>
{
    use std::os::unix::process::CommandExt as _;
    let fixture = Fixture::new(InputLinks::Keep)?;
    let path = fixture.working_directory.join("setsid.pid");
    let host = gang_testkit::ChildTest::new("setsid_cleanup_host");
    let status = host
        .command()?
        .current_dir(&fixture.working_directory)
        .env("TEST_ENTRYPOINT", &fixture.entrypoint)
        .env("TEST_DESCENDANT_PID", &path)
        .process_group(0)
        .status()?;
    assert!(status.success(), "host helper exited with {status}");
    let content = fs::read_to_string(path)?;
    let mut parts = content.split_whitespace();
    let pid: i32 = parts.next().ok_or("missing pid")?.parse()?;
    let sid: i32 = parts.next().ok_or("missing sid")?.parse()?;
    assert_eq!(sid, pid);
    let pid = rustix::process::Pid::from_raw(pid).ok_or("invalid pid")?;
    let result = rustix::process::test_kill_process(pid);
    if result.is_ok() {
        let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
    }
    assert_eq!(result, Err(rustix::io::Errno::SRCH));
    Ok(())
}

#[cfg(target_os = "linux")]
const NESTED_ACTION: gang_testkit::ChildTest =
    gang_testkit::ChildTest::new("nested_setsid_action_helper");

#[cfg(target_os = "linux")]
fn nested_action_command(role: &str) -> io::Result<std::process::Command> {
    let mut command = NESTED_ACTION.command()?;
    command
        .env("TEST_NESTED_ROLE", role)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    Ok(command)
}

#[cfg(target_os = "linux")]
#[ignore = "real nested process helper for the session cleanup regression"]
#[test]
fn nested_setsid_action_helper() -> Result<(), Box<dyn Error>> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let role = std::env::var("TEST_NESTED_ROLE").unwrap_or_else(|_| "action".into());
    match role.as_str() {
        "short" => return Ok(()),
        "action" => {
            let _parent = nested_action_command("parent")?.spawn()?;
        }
        "parent" => {
            rustix::process::setsid()?;
            assert!(nested_action_command("short")?.status()?.success());
            let _grandchild = nested_action_command("grandchild")?.spawn()?;
        }
        "grandchild" => {
            rustix::process::setsid()?;
        }
        _ => return Err("invalid nested action role".into()),
    }
    let socket = std::env::var_os("TEST_NESTED_SOCKET").ok_or("missing nested action socket")?;
    let mut stream = UnixStream::connect(PathBuf::from(socket))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(15)))?;
    writeln!(stream, "{role} {}", std::process::id())?;
    if role == "action" {
        stream.read_exact(&mut [0])?;
    } else {
        // The controller holds the socket until cleanup; EOF also releases helpers on failure.
        let _ = stream.read(&mut [0])?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn successful_noop_action_drains_nested_setsid_descendants_with_reaping_parent()
-> Result<(), Box<dyn Error>> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let fixture = Fixture::new(InputLinks::Keep)?;
    let socket = fixture.base.join("nested.sock");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let listener = tokio::net::UnixListener::bind(&socket)?;
            let host = gang_testkit::ChildTest::new("setsid_cleanup_host");
            let mut command = tokio::process::Command::from(host.command()?);
            command
                .current_dir(&fixture.working_directory)
                .env("TEST_ENTRYPOINT", &fixture.entrypoint)
                .env("TEST_NESTED_SETSID", "1")
                .env("TEST_NESTED_SOCKET", &socket);
            let run = async {
                gang_os::run_bounded(&mut command, None, NonZeroBytes::kib(64))
                    .await
                    .map_err(Box::<dyn Error>::from)
            };
            let ready = async {
                let mut descendants = Vec::new();
                let mut held = Vec::new();
                let mut release = None;
                for _ in 0..3 {
                    let (stream, _) = listener.accept().await?;
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    reader.read_line(&mut line).await?;
                    let (role, raw) = line.trim().split_once(' ').ok_or("invalid readiness")?;
                    let pid = rustix::process::Pid::from_raw(raw.parse()?).ok_or("invalid pid")?;
                    if role == "action" {
                        release = Some(reader.into_inner());
                    } else {
                        assert_eq!(
                            rustix::process::getsid(Some(pid))?,
                            pid,
                            "{role} did not escape"
                        );
                        descendants.push(pid);
                        held.push(reader.into_inner());
                    }
                }
                assert_eq!(
                    descendants.len(),
                    2,
                    "both escaped generations must become ready"
                );
                release
                    .ok_or("missing action readiness")?
                    .write_all(&[1])
                    .await?;
                Ok::<_, Box<dyn Error>>((descendants, held))
            };
            let (output, (descendants, _held)) = tokio::try_join!(run, ready)?;
            assert!(
                output.status.success(),
                "nested cleanup host failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            for pid in descendants {
                assert_eq!(
                    rustix::process::test_kill_process(pid),
                    Err(rustix::io::Errno::SRCH),
                    "escaped descendant {pid:?} survived successful action"
                );
            }
            Ok::<(), Box<dyn Error>>(())
        })
        .await?
    })
}
