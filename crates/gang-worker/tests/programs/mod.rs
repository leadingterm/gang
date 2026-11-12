//! Resolution of the sandbox program against the worker's `PATH`, observed through the sandbox
//! file that [`gang_worker::run`] renders.

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::future;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use gang_config::SandboxFile;
use gang_spi::InstanceIdentity;
use gang_testkit::{BoxedSection, ChildTest, SandboxKey, TestPlugins};
use gang_types::{FleetName, InstanceId, Isa, WorkerLifetime};
use gang_worker::{Layout, WorkerError, WorkerSettings};

use crate::support::{Behaviour, Harness};

/// The program name the tests resolve.
const PROGRAM: &str = "fake-sandbox";

/// Creates `path` as a file with `mode`, creating its parent directories.
fn write_file(path: &Path, mode: u32) {
    fs::create_dir_all(path.parent().expect("a file has a parent directory"))
        .expect("directory can be created");
    fs::write(path, b"").expect("file can be written");
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("mode can be set");
}

/// The `PATH` value of the given directories, in order.
fn search_path(directories: impl IntoIterator<Item = impl AsRef<OsStr>>) -> OsString {
    env::join_paths(directories).expect("directories can be joined into a PATH")
}

/// The canonical form of `path`, which must exist.
fn canonical(path: &Path) -> PathBuf {
    fs::canonicalize(path).expect("path can be canonicalized")
}

/// The settings of a run that resolves `program` against `search_path`.
fn configured(
    harness: &Harness,
    program: &Path,
    search_path: OsString,
) -> WorkerSettings<TestPlugins> {
    harness.provisioner.set_identity(InstanceIdentity {
        instance: InstanceId::try_new("i-0abc123").expect("instance id is valid"),
        fleet: FleetName::try_new("example-boxed").expect("test fleet name is valid"),
        isa: Isa::X86_64,
    });
    let mut settings = harness.settings();
    settings
        .sandbox
        .boxed
        .as_mut()
        .expect("the boxed sandbox section is configured")
        .program = program.to_path_buf();
    settings.search_path = search_path;
    settings
}

/// Runs the worker to completion with `program` resolved against `search_path`, and returns the
/// program the rendered sandbox file names.
async fn rendered(harness: &Harness, program: &Path, search_path: OsString) -> PathBuf {
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(false),
        configured(harness, program, search_path),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");
    let sandbox: SandboxFile<TestPlugins> =
        fs::read_to_string(Layout::new(&harness.volume).sandbox_file())
            .expect("sandbox file is readable")
            .parse()
            .expect("sandbox file parses");
    sandbox
        .sandbox
        .boxed
        .expect("the boxed sandbox section is rendered")
        .program
}

#[tokio::test]
async fn bare_name_resolves_to_the_first_directory_that_holds_it() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let root = tempfile::tempdir().expect("a temporary directory can be created");
    let first = root.path().join("first");
    let second = root.path().join("second");
    write_file(&first.join(PROGRAM), 0o755);
    write_file(&second.join(PROGRAM), 0o755);

    let program = rendered(&harness, Path::new(PROGRAM), search_path([&first, &second])).await;
    assert_eq!(program, canonical(&first.join(PROGRAM)));
}

#[tokio::test]
async fn non_executable_files_and_directories_are_skipped() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let root = tempfile::tempdir().expect("a temporary directory can be created");
    let directory = root.path().join("directory");
    let plain = root.path().join("plain");
    let good = root.path().join("good");
    fs::create_dir_all(directory.join(PROGRAM)).expect("directory can be created");
    write_file(&plain.join(PROGRAM), 0o644);
    write_file(&good.join(PROGRAM), 0o755);

    let program = rendered(
        &harness,
        Path::new(PROGRAM),
        search_path([&directory, &plain, &good]),
    )
    .await;
    assert_eq!(program, canonical(&good.join(PROGRAM)));
}

#[tokio::test]
async fn symbolic_link_on_the_path_resolves_to_its_target() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let root = tempfile::tempdir().expect("a temporary directory can be created");
    let real = root.path().join("real").join(PROGRAM);
    let links = root.path().join("links");
    write_file(&real, 0o755);
    fs::create_dir(&links).expect("directory can be created");
    symlink(&real, links.join(PROGRAM)).expect("symbolic link can be created");

    let program = rendered(&harness, Path::new(PROGRAM), search_path([&links])).await;
    assert_eq!(program, canonical(&real));
}

#[tokio::test]
async fn empty_search_path_finds_no_program() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let settings = configured(&harness, Path::new(PROGRAM), OsString::new());
    let error = gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("an empty PATH finds no program");
    assert!(matches!(error, WorkerError::SandboxProgram { .. }));
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn program_on_no_path_directory_fails_before_any_worker_starts() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let settings = configured(
        &harness,
        Path::new("no-such-sandbox"),
        search_path([&harness.bin]),
    );
    let error = gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("a program on no PATH directory fails the run");
    match error {
        WorkerError::SandboxProgram { program } => {
            assert_eq!(program, PathBuf::from("no-such-sandbox"));
        }
        other => panic!("expected a sandbox program error, got {other}"),
    }
    assert!(harness.scheduler.specs().is_empty());
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

#[tokio::test]
async fn absolute_symbolic_link_is_canonicalized_without_the_path() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let root = tempfile::tempdir().expect("a temporary directory can be created");
    let real = root.path().join("real").join(PROGRAM);
    let link = root.path().join("link");
    write_file(&real, 0o755);
    symlink(&real, &link).expect("symbolic link can be created");

    let program = rendered(&harness, &link, OsString::new()).await;
    assert_eq!(program, canonical(&real));
}

#[tokio::test]
async fn unselected_sandbox_programs_are_not_resolved() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let mut settings = harness.settings();
    settings.sandbox.boxed = Some(BoxedSection {
        program: PathBuf::from("missing-sandbox"),
    });
    gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect("the noop sandbox runs without its unselected program on PATH");

    let sandbox: SandboxFile<TestPlugins> =
        fs::read_to_string(Layout::new(&harness.volume).sandbox_file())
            .expect("sandbox file is readable")
            .parse()
            .expect("sandbox file parses");
    assert_eq!(sandbox.selected, SandboxKey::Noop);
    let boxed = sandbox
        .sandbox
        .boxed
        .expect("the boxed section is rendered");
    assert_eq!(boxed.program, PathBuf::from("missing-sandbox"));
}

#[tokio::test]
async fn a_program_name_with_a_directory_part_is_not_looked_up_on_the_path() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let settings = configured(
        &harness,
        Path::new("./fake-sandbox"),
        search_path([&harness.bin]),
    );
    let error = gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("a program with a directory part fails the run");
    assert!(matches!(error, WorkerError::SandboxProgram { .. }));
    assert!(harness.scheduler.specs().is_empty());
}

/// The environment variable that turns [`empty_path_entry_child`] into the checked run.
const CHILD_ENV: &str = "EMPTY_PATH_CHILD";

const EMPTY_PATH_ENTRY_CHILD: ChildTest = ChildTest::new("programs::empty_path_entry_child");

#[test]
fn an_empty_path_entry_is_never_the_current_directory() {
    let current = tempfile::tempdir().expect("a temporary directory can be created");
    let empty = tempfile::tempdir().expect("a temporary directory can be created");
    write_file(&current.path().join(PROGRAM), 0o755);
    // The current directory is process-wide, so the check runs in a re-executed copy of this
    // test binary: no host program runs.
    let output = EMPTY_PATH_ENTRY_CHILD
        .command()
        .expect("the test binary path is known")
        .current_dir(current.path())
        .env(CHILD_ENV, empty.path())
        .output()
        .expect("the test binary can be re-executed");
    EMPTY_PATH_ENTRY_CHILD.assert_completed(&output);
}

/// Runs the worker with an empty `PATH` entry ahead of a directory without the program, in a
/// process whose current directory holds the program. Does nothing unless [`CHILD_ENV`] is set.
#[ignore = "child process helper of the programs tests"]
#[test]
fn empty_path_entry_child() {
    let Some(empty) = env::var_os(CHILD_ENV) else {
        return;
    };
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let mut search = OsString::from(":");
    search.push(&empty);
    let settings = configured(&harness, Path::new(PROGRAM), search);
    let error = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime can be built")
        .block_on(gang_worker::run(
            harness.parts(false),
            settings,
            harness.build_limits(),
            future::pending(),
        ))
        .expect_err("an empty PATH entry is not the current directory");
    assert!(matches!(error, WorkerError::SandboxProgram { .. }));
    EMPTY_PATH_ENTRY_CHILD.complete();
}
