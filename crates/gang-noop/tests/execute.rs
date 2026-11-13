//! Behaviour of the no-op sandbox's command execution.
//!
//! An action is this test binary re-run as its own `action` test. What the action does is chosen
//! by the `ACTION_MODE` variable that each test passes in the action's environment, so no shell
//! or other external program is involved. The environment of an action is exactly the one the
//! test gives it, which lets the modes observe that.

use std::error::Error;
use std::ffi::OsString;
use std::io::Read;
use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};

use gang_noop::{Noop, NoopConfig};
use gang_spi::{
    ActionCommand, ActionExecutionOutcome, ActionExecutionSpec, ActionIsolationPort,
    NetworkRequest, Plugin, PluginContext, PortError,
};
use gang_testkit::ChildTest;
use gang_types::AbsolutePathBuf;

/// SIGKILL's number on the platforms Gang runs on.
const SIGKILL: i32 = 9;

const ACTION: ChildTest = ChildTest::new("action");

/// The action, ignored by the ordinary run. It does nothing unless `ACTION_MODE` is set.
///
/// Modes:
/// - `exit:N` exits with status `N`.
/// - `signal:N` raises signal `N` on itself.
/// - `report` writes `out` and, when `TMPDIR` is a directory, `tmp_was_directory`.
/// - `block` connects to the socket at `READY` and parks forever.
/// - `echo` connects to the socket at `READY`, then echoes every byte it reads back until end of
///   input.
/// - `host` runs the `echo` action through the sandbox's `ActionIsolationPort::start` (READY
///   passed through) and waits for it, standing in for the `gang exec` process that hosts one
///   action.
#[ignore = "child process helper of the execute tests"]
#[test]
fn action() -> Result<(), Box<dyn Error>> {
    let Ok(mode) = std::env::var("ACTION_MODE") else {
        return Ok(());
    };
    if let Some(code) = mode.strip_prefix("exit:") {
        std::process::exit(code.parse::<i32>()?);
    }
    if let Some(number) = mode.strip_prefix("signal:") {
        let signal = rustix::process::Signal::from_named_raw(number.parse::<i32>()?)
            .ok_or("unknown signal")?;
        rustix::process::kill_process(rustix::process::getpid(), signal)?;
        return Err("the signal did not end the action".into());
    }
    match mode.as_str() {
        "report" => report(),
        "block" => block(),
        "echo" => echo(),
        "host" => host(),
        other => Err(format!("unknown ACTION_MODE {other}").into()),
    }
}

/// Records the action's view of its environment and working directory.
fn report() -> Result<(), Box<dyn Error>> {
    let marker = std::env::var("MARKER").unwrap_or_else(|_| "unset".to_owned());
    let home = std::env::var("HOME").unwrap_or_else(|_| "unset".to_owned());
    let cwd = std::env::current_dir()?.canonicalize()?;
    let tmpdir = std::env::var("TMPDIR").unwrap_or_default();
    if Path::new(&tmpdir).is_dir() {
        std::fs::write("tmp_was_directory", "")?;
    }
    std::fs::write(
        "out",
        format!("{marker}\n{home}\n{}\n{tmpdir}\n", cwd.display()),
    )?;
    Ok(())
}

/// Reports readiness by connecting to `READY`, then never returns.
fn block() -> Result<(), Box<dyn Error>> {
    let ready = std::env::var_os("READY").ok_or("READY is required")?;
    let _stream = UnixStream::connect(ready)?;
    loop {
        std::thread::park();
    }
}

/// Reports readiness by connecting to `READY`, then echoes every byte it reads back until end of
/// input.
fn echo() -> Result<(), Box<dyn Error>> {
    let ready = std::env::var_os("READY").ok_or("READY is required")?;
    let stream = UnixStream::connect(ready)?;
    let mut byte = [0_u8; 1];
    loop {
        if (&stream).read(&mut byte)? == 0 {
            return Ok(());
        }
        (&stream).write_all(&byte)?;
    }
}

/// Runs the `echo` action through the sandbox's isolation port and waits for it, standing in for
/// the `gang exec` process that hosts one action.
fn host() -> Result<(), Box<dyn Error>> {
    let ready = PathBuf::from(std::env::var_os("READY").ok_or("READY is required")?);
    let noop = noop()?;
    let argv = action_argv()?;
    let environment = action_environment("echo", Some(ready.as_path()));
    let working_directory = std::env::current_dir()?;
    let spec = ActionExecutionSpec {
        command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
        launcher: &[],
        environment: &environment,
        action_directory: &working_directory,
        working_directory: &working_directory,
        network: NetworkRequest::NotRequested,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async { noop.start(&spec).await?.wait().await.map(drop) })?;
    Ok(())
}

fn noop() -> Result<Noop, PortError> {
    Noop::build(
        NoopConfig { scratch_dir: None },
        &PluginContext {
            config_path: Path::new("sandbox.toml"),
        },
    )
}

/// A no-op sandbox whose actions get their temporary directory under `scratch`.
fn scratch_noop(scratch: &Path) -> Result<Noop, Box<dyn Error>> {
    Ok(Noop::build(
        NoopConfig {
            scratch_dir: Some(AbsolutePathBuf::try_new(scratch)?),
        },
        &PluginContext {
            config_path: Path::new("sandbox.toml"),
        },
    )?)
}

/// The command that re-runs the `action` test of this binary.
fn action_argv() -> Result<Vec<OsString>, std::io::Error> {
    let mut argv = vec![std::env::current_exe()?.into_os_string()];
    argv.extend(ACTION.arguments().into_iter().map(OsString::from));
    Ok(argv)
}

/// The environment that selects `mode` in the action, plus `READY` when `ready` is given.
fn action_environment(mode: &str, ready: Option<&Path>) -> Vec<(OsString, OsString)> {
    let mut environment = vec![(OsString::from("ACTION_MODE"), OsString::from(mode))];
    if let Some(ready) = ready {
        environment.push((OsString::from("READY"), ready.as_os_str().to_owned()));
    }
    environment
}

async fn spec_run(
    noop: &Noop,
    argv: &[OsString],
    environment: &[(OsString, OsString)],
    working_directory: &Path,
) -> Result<ActionExecutionOutcome, Box<dyn Error>> {
    let spec = ActionExecutionSpec {
        command: ActionCommand::from_argv(argv).ok_or("empty argv")?,
        launcher: &[],
        environment,
        action_directory: working_directory,
        working_directory,
        network: NetworkRequest::NotRequested,
    };
    let mut running = noop.start(&spec).await?;
    Ok(running.wait().await?)
}

/// Binds the listener an action reports readiness to, at `ready` in `dir`.
fn listen(dir: &Path) -> Result<(PathBuf, UnixListener), Box<dyn Error>> {
    let ready = dir.join("ready");
    let listener = UnixListener::bind(&ready)?;
    Ok((ready, listener))
}

/// Completes with the connection of the first client of `listener`. The accept blocks on a
/// blocking thread, so no timer or polling is involved.
async fn until_accepted(listener: UnixListener) -> Result<UnixStream, Box<dyn Error>> {
    Ok(tokio::task::spawn_blocking(move || listener.accept())
        .await??
        .0)
}

/// Reads the four lines the `report` mode writes to `out` in `dir`.
fn report_lines(dir: &Path) -> Result<[String; 4], Box<dyn Error>> {
    let text = std::fs::read_to_string(dir.join("out"))?;
    let lines: Vec<String> = text.lines().map(str::to_owned).collect();
    <[String; 4]>::try_from(lines)
        .map_err(|lines| format!("expected four report lines, got {lines:?}").into())
}

#[tokio::test]
async fn reports_the_exit_code() -> Result<(), Box<dyn Error>> {
    let noop = noop()?;
    let dir = tempfile::tempdir()?;
    let outcome = spec_run(
        &noop,
        &action_argv()?,
        &action_environment("exit:7", None),
        dir.path(),
    )
    .await?;

    assert_eq!(outcome, ActionExecutionOutcome::Exited(7));
    Ok(())
}

#[tokio::test]
async fn reports_the_terminating_signal() -> Result<(), Box<dyn Error>> {
    let noop = noop()?;
    let dir = tempfile::tempdir()?;
    let outcome = spec_run(
        &noop,
        &action_argv()?,
        &action_environment("signal:9", None),
        dir.path(),
    )
    .await?;

    assert_eq!(outcome, ActionExecutionOutcome::Signaled(SIGKILL));
    Ok(())
}

#[tokio::test]
async fn runs_in_the_working_directory_with_exactly_the_given_environment()
-> Result<(), Box<dyn Error>> {
    let noop = noop()?;
    let dir = tempfile::tempdir()?;
    let mut environment = action_environment("report", None);
    environment.push((OsString::from("MARKER"), OsString::from("bar")));

    let outcome = spec_run(&noop, &action_argv()?, &environment, dir.path()).await?;

    assert_eq!(outcome, ActionExecutionOutcome::Exited(0));
    let [marker, home, cwd, tmpdir] = report_lines(dir.path())?;
    let canonical = dir.path().canonicalize()?;
    assert_eq!(marker, "bar");
    assert_eq!(home, "unset");
    assert_eq!(cwd, canonical.display().to_string());
    assert!(!tmpdir.is_empty(), "TMPDIR is set for the action");
    assert!(
        !Path::new(&tmpdir).starts_with(&canonical),
        "TMPDIR {tmpdir} lies under the working directory"
    );
    assert!(dir.path().join("tmp_was_directory").exists());
    Ok(())
}

#[tokio::test]
async fn creates_the_temporary_directory_under_the_scratch_dir() -> Result<(), Box<dyn Error>> {
    let scratch = tempfile::tempdir()?;
    let noop = scratch_noop(scratch.path())?;
    let dir = tempfile::tempdir()?;

    let outcome = spec_run(
        &noop,
        &action_argv()?,
        &action_environment("report", None),
        dir.path(),
    )
    .await?;

    assert_eq!(outcome, ActionExecutionOutcome::Exited(0));
    let [_marker, _home, _cwd, tmpdir] = report_lines(dir.path())?;
    assert!(
        Path::new(&tmpdir).starts_with(scratch.path()),
        "TMPDIR {tmpdir} lies under the scratch directory"
    );
    Ok(())
}

#[tokio::test]
async fn gives_each_action_its_own_temporary_directory() -> Result<(), Box<dyn Error>> {
    let noop = noop()?;
    let first = tempfile::tempdir()?;
    let second = tempfile::tempdir()?;
    let third = tempfile::tempdir()?;
    let argv = action_argv()?;
    let report = action_environment("report", None);
    let mut given = action_environment("report", None);
    given.push((OsString::from("TMPDIR"), OsString::from("/given")));

    for (environment, dir) in [(&report, &first), (&report, &second), (&given, &third)] {
        let outcome = spec_run(&noop, &argv, environment, dir.path()).await?;
        assert_eq!(outcome, ActionExecutionOutcome::Exited(0));
    }

    let [_, _, _, first_tmp] = report_lines(first.path())?;
    let [_, _, _, second_tmp] = report_lines(second.path())?;
    let [_, _, _, third_tmp] = report_lines(third.path())?;

    assert_ne!(first_tmp, second_tmp);
    assert_ne!(third_tmp, "/given");
    for dir in [&first, &second, &third] {
        assert!(dir.path().join("tmp_was_directory").exists());
    }
    for tmp in [&first_tmp, &second_tmp, &third_tmp] {
        assert!(!Path::new(tmp).exists(), "{tmp} outlives the action");
    }
    Ok(())
}

#[tokio::test]
async fn a_missing_program_is_a_backend_failure() -> Result<(), Box<dyn Error>> {
    let noop = noop()?;
    let dir = tempfile::tempdir()?;
    let argv = [OsString::from("/nonexistent/program")];

    let error = spec_run(&noop, &argv, &[], dir.path())
        .await
        .err()
        .ok_or("a missing program cannot run")?;

    assert!(
        matches!(
            error.downcast_ref::<PortError>(),
            Some(PortError::Backend(_))
        ),
        "got {error:?}"
    );
    Ok(())
}

/// Dropping the running handle kills the action. The test waits for the action's readiness
/// before dropping the handle, so the action was running when `start` returned.
#[tokio::test]
async fn dropping_the_running_action_kills_it() -> Result<(), Box<dyn Error>> {
    let noop = noop()?;
    let dir = tempfile::tempdir()?;
    let (ready, listener) = listen(dir.path())?;
    let argv = action_argv()?;
    let environment = action_environment("block", Some(ready.as_path()));
    let spec = ActionExecutionSpec {
        command: ActionCommand::from_argv(&argv).ok_or("empty argv")?,
        launcher: &[],
        environment: &environment,
        action_directory: dir.path(),
        working_directory: dir.path(),
        network: NetworkRequest::NotRequested,
    };
    let running = noop.start(&spec).await?;

    let mut connection = until_accepted(listener).await?;
    drop(running);
    // The action holds the other end of the connection, so reading it reaches EOF only once the
    // action has died.
    let mut byte = [0_u8; 1];
    assert_eq!(
        connection.read(&mut byte)?,
        0,
        "the action outlived its dropped handle"
    );
    Ok(())
}

/// `NativeLink` stops an action by signalling the process group of the `gang exec` process that
/// hosts it, and on macOS it only ever sends SIGKILL to that group. The action must therefore
/// not outlive its host's process group.
#[tokio::test]
async fn killing_the_hosts_process_group_takes_the_action_down() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let (ready, listener) = listen(dir.path())?;
    // The host leads its own process group, so the group id is its pid.
    let mut host = ACTION
        .command()?
        .env_clear()
        .env("ACTION_MODE", "host")
        .env("READY", &ready)
        .current_dir(dir.path())
        .process_group(0)
        .spawn()?;
    // The echo action reports readiness.
    let mut connection = until_accepted(listener).await?;

    let group = gang_os::ProcessGroup::from_pid(host.id()).ok_or("the host has no process id")?;
    group.kill()?;
    host.wait()?;

    // The write may fail because the action is already gone. A surviving action would echo the
    // byte back. Dropping `connection` at the end of the test ends a surviving action, so a
    // failing run leaks no process.
    let _ = connection.write_all(b"x");
    let mut byte = [0_u8; 1];
    assert!(
        matches!(connection.read(&mut byte), Ok(0) | Err(_)),
        "the action outlived the process group of its host"
    );
    Ok(())
}
