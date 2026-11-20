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
    PortError, SIDE_CHANNEL_FILE_ENV, SandboxedAction,
};
use gang_testkit::{
    CallLog, Opaque, RecordedScope, RecordingLimits, SandboxConfig, SandboxKey, ScriptedSandbox,
    TestLimits, TestPlugins,
};
use gang_types::{Bytes, NonZeroBytes};
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
                [(String::from("marker"), marker.to_owned().into())]
                    .into_iter()
                    .collect(),
            )),
            ..SandboxConfig::default()
        },
        execution: ExecutionConfig { input_links },
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
            let marker = sandbox
                .noop
                .and_then(|section| {
                    section
                        .0
                        .get("marker")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned)
                })
                .unwrap_or_default();
            let sandbox =
                ScriptedSandbox::exiting(&CallLog::default(), ActionExecutionOutcome::Exited(0));
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
            Ok(ScriptedSandbox::exiting(
                &CallLog::default(),
                ActionExecutionOutcome::Signaled(9),
            ))
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

struct MetadataCheckingSandbox(PathBuf);

impl ActionIsolationPort for MetadataCheckingSandbox {
    async fn start(&self, _: &ActionExecutionSpec<'_>) -> Result<SandboxedAction, PortError> {
        let metadata = fs::read(&self.0).map_err(PortError::backend)?;
        if metadata != b"/sys/fs/cgroup/actions/123-456-7" {
            return Err(PortError::backend(io::Error::other(
                "incorrect cgroup metadata",
            )));
        }
        Ok(SandboxedAction::new(async {
            Ok(ActionExecutionOutcome::Exited(0))
        }))
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
        |_, _, _| Ok(MetadataCheckingSandbox(metadata.clone())),
        |_, _| Ok(LimitsWithCgroup(RecordingLimits::new(&CallLog::default()))),
        || Ok(()),
    )?;
    assert_eq!(status, ExitCode::SUCCESS);
    assert_eq!(fs::read(&path)?, br#"{"peak_memory_kb":4}"#);
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
