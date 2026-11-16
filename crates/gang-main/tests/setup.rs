//! Black-box consent and preview checks for `gang setup`.

mod black_box;
mod support;

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use black_box::{gang_command_for, output_within, serve_server_info};
use gang_testkit::run_answering;
use serde_json::json;
use support::required_path;

/// Test timeout for the process and its prompt reader.
const LIMIT: Duration = Duration::from_mins(1);
/// An anonymous server does not need login or credentials.
const SERVER: &str = r#"{"name":"server","remoteExecutor":"grpc://127.0.0.1:8980","remoteCache":"grpc://127.0.0.1:8980","defaultIsa":"x86-64","authentication":{"none":{}}}"#;
/// Existing workspace contents must be preserved exactly.
const WORKSPACE_RC: &str = "# workspace settings\nbuild --keep_going\n";
/// Existing home contents exercise both endpoint and credential conflicts.
const HOME_RC: &str = "build --remote_cache=grpcs://cache.example.com:443 \"--remote_header=Authorization=Bearer x\"\nbuild --disk_cache=/tmp/d\n";
/// The expected minimal home edit moves only the conflicting entry.
const HOME_FIXED: &str = "build:not-gang --remote_cache=grpcs://cache.example.com:443 \"--remote_header=Authorization=Bearer x\"\nbuild --disk_cache=/tmp/d\n";
/// Two separate entries in the same file must be edited together.
const HOME_TWO: &str = "build --keep_going --remote_cache=grpcs://cache.example.com:443\nbuild --remote_header=X=y\nbuild --disk_cache=/tmp/d\n";
/// Both conflicting entries moved without changing the unrelated entry.
const HOME_TWO_FIXED: &str = "build --keep_going\nbuild:not-gang --remote_cache=grpcs://cache.example.com:443\nbuild:not-gang --remote_header=X=y\nbuild --disk_cache=/tmp/d\n";
/// A home rc without remote options.
const HOME_CLEAN: &str = "build --disk_cache=/tmp/d\n";

/// Result type shared by integration tests.
type TestResult = Result<(), Box<dyn Error>>;

/// Isolated workspace, home, and external fake-Bazel fixture.
struct Fixture {
    /// Owns the temporary directory until the test exits.
    dir: tempfile::TempDir,
    /// Workspace containing the root marker.
    workspace: PathBuf,
    /// Home containing the conflicting rc file.
    home: PathBuf,
    /// Whether the first probe should already use healthy options.
    clean_only: bool,
}

impl Fixture {
    /// Creates the files and a build-event fixture with the real temporary rc path.
    fn new() -> Result<Self, Box<dyn Error>> {
        Self::with_home(HOME_RC, "Authorization=Bearer x", false)
    }

    /// Creates a home rc and event fixtures for conflicting and healthy probes.
    fn with_home(home_rc: &str, header: &str, clean_only: bool) -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let workspace = dir.path().join("workspace");
        let home = dir.path().join("home");
        fs::create_dir(&workspace)?;
        fs::create_dir(&home)?;
        fs::write(workspace.join("MODULE.bazel"), "")?;
        fs::write(workspace.join(".bazelrc"), WORKSPACE_RC)?;
        fs::write(home.join(".bazelrc"), home_rc)?;
        fs::set_permissions(home.join(".bazelrc"), fs::Permissions::from_mode(0o600))?;
        let options = [
            json!({"optionName":"remote_cache", "optionValue":"grpcs://cache.example.com:443"}),
            json!({"optionName":"remote_header", "optionValue":header}),
        ];
        let original = [
            json!({"optionName":"rc_source", "optionValue":home.join(".bazelrc")}),
            json!({"optionName":"default_override", "optionValue":"0:build=--remote_cache=grpcs://cache.example.com:443"}),
            json!({"optionName":"default_override", "optionValue":format!("0:build=--remote_header={header}")}),
        ];
        let event = |label: &str, options: &[serde_json::Value]| {
            json!({
                "id": {"structuredCommandLine": {"commandLineLabel": label}},
                "structuredCommandLine": {"commandLineLabel": label, "sections": [
                    {"sectionLabel": "command options", "optionList": {"option": options}}
                ]}
            })
        };
        let started = json!({"id":{"buildStarted":{}},"started":{"command":"build"}});
        fs::write(
            dir.path().join("events"),
            format!(
                "{}\n{}\n{}\n",
                started,
                event("canonical", &options),
                event("original", &original)
            ),
        )?;
        let clean_options = [
            json!({"optionName":"remote_executor", "optionValue":"grpc://127.0.0.1:8980"}),
            json!({"optionName":"remote_cache", "optionValue":"grpc://127.0.0.1:8980"}),
        ];
        let clean_original = [
            json!({"optionName":"rc_source", "optionValue":workspace.join(".gang/bazelrc")}),
            json!({"optionName":"default_override", "optionValue":"0:build=--remote_executor=grpc://127.0.0.1:8980"}),
            json!({"optionName":"default_override", "optionValue":"0:build=--remote_cache=grpc://127.0.0.1:8980"}),
        ];
        fs::write(
            dir.path().join("clean"),
            format!(
                "{}\n{}\n{}\n",
                started,
                event("canonical", &clean_options),
                event("original", &clean_original)
            ),
        )?;
        Ok(Self {
            dir,
            workspace,
            home,
            clean_only,
        })
    }

    /// Starts a command against the loopback server and fake Bazel.
    fn command(&self, server: &str, flag: Option<&str>) -> Result<Command, Box<dyn Error>> {
        let mut command = gang_command_for(required_path("GANG_BIN")?.as_os_str(), &self.home);
        command.current_dir(&self.workspace).env("NO_COLOR", "1");
        command.args(["setup", "--server", server, "--bazel"]);
        command.arg(required_path("FAKE_BAZEL")?);
        if let Some(flag) = flag {
            command.arg(flag);
        }
        command.arg("--");
        command.arg(format!(
            "--fake_events_from={}",
            self.dir
                .path()
                .join(if self.clean_only { "clean" } else { "events" })
                .display()
        ));
        command.arg(format!(
            "--fake_events_then={}",
            self.dir.path().join("clean").display()
        ));
        command.arg(format!(
            "--fake_record_events_path={}",
            self.dir.path().join("seen").display()
        ));
        command.arg(format!(
            "--fake_invocations_path={}",
            self.dir.path().join("invocations").display()
        ));
        Ok(command)
    }

    /// Captures every file's exact bytes, a stronger equality check than hashes.
    fn trees(&self) -> Result<BTreeMap<PathBuf, Vec<u8>>, Box<dyn Error>> {
        /// Recursively captures file contents relative to the fixture root.
        fn collect(
            root: &Path,
            path: &Path,
            files: &mut BTreeMap<PathBuf, Vec<u8>>,
        ) -> Result<(), Box<dyn Error>> {
            for entry in fs::read_dir(path)? {
                let path = entry?.path();
                if path.is_dir() {
                    collect(root, &path, files)?;
                } else {
                    files.insert(path.strip_prefix(root)?.to_owned(), fs::read(&path)?);
                }
            }
            Ok(())
        }
        let mut files = BTreeMap::new();
        collect(self.dir.path(), &self.workspace, &mut files)?;
        collect(self.dir.path(), &self.home, &mut files)?;
        Ok(files)
    }
}

/// Runs a non-interactive setup and captures its output.
fn noninteractive(
    fixture: &Fixture,
    flag: Option<&str>,
) -> Result<std::process::Output, Box<dyn Error>> {
    let server = serve_server_info(SERVER)?;
    output_within(&mut fixture.command(&server.url(), flag)?, LIMIT)
}

/// Combines the streams for output assertions.
fn text(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The preview-only modes do not change a single workspace or home file.
#[test]
fn dry_run_and_non_terminal_are_read_only() -> TestResult {
    for flag in [Some("--dry-run"), None] {
        let fixture = Fixture::new()?;
        let before = fixture.trees()?;
        let output = noninteractive(&fixture, flag)?;
        assert_eq!(fixture.trees()?, before);
        let printed = text(&output);
        assert!(printed.contains(".gang/bazelrc"), "{printed}");
        if flag.is_none() {
            assert_eq!(output.status.code(), Some(1));
            assert!(printed.contains("--yes"), "{printed}");
        } else {
            assert!(output.status.success(), "dry-run must exit successfully");
        }
    }
    Ok(())
}

/// Explicit consent writes only the Gang-owned files and the workspace import.
#[test]
fn yes_writes_the_preview_and_preserves_home() -> TestResult {
    let fixture = Fixture::new()?;
    let output = noninteractive(&fixture, Some("--yes"))?;
    let printed = text(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "unresolved conflict must exit 1"
    );
    assert!(
        printed.lines().any(|line| {
            let mut fields = line.split_whitespace();
            fields.next() == Some("fail")
                && fields.next() == Some("remote_cache")
                && line.contains("cache.example.com")
        }),
        "{printed}"
    );
    let rc_path = fixture.workspace.join(".gang/bazelrc");
    assert!(
        rc_path.exists(),
        "setup did not write {}: {printed}",
        rc_path.display()
    );
    let rc = fs::read_to_string(rc_path)?;
    let header = format!(
        "{} (new file)",
        fixture.workspace.join(".gang/bazelrc").display()
    );
    let after_header = printed
        .split_once(&header)
        .ok_or("missing bazelrc preview")?
        .1;
    let shown: Vec<&str> = after_header
        .lines()
        .skip(1)
        .map(str::trim_start)
        .take_while(|line| line.starts_with("+ "))
        .map(|line| &line[2..])
        .collect();
    assert_eq!(shown, rc.lines().collect::<Vec<_>>());
    assert_eq!(
        fs::read_to_string(fixture.workspace.join(".bazelrc"))?,
        format!(
            "{WORKSPACE_RC}# Gang's Bazel flags, written by `gang setup`.\ntry-import %workspace%/.gang/bazelrc\n"
        )
    );
    assert_eq!(fs::read_to_string(fixture.home.join(".bazelrc"))?, HOME_RC);
    Ok(())
}

/// Healthy effective options allow unattended setup to finish successfully.
#[test]
fn yes_succeeds_with_clean_home() -> TestResult {
    let fixture = Fixture::with_home(HOME_CLEAN, "", true)?;
    let output = noninteractive(&fixture, Some("--yes"))?;
    assert!(
        output.status.success(),
        "clean --yes setup must exit successfully"
    );
    assert_eq!(
        fs::read_to_string(fixture.home.join(".bazelrc"))?,
        HOME_CLEAN
    );
    Ok(())
}

/// Drives prompts as they appear on a pseudo-terminal, with a bounded child lifetime.
fn interactive(
    fixture: &Fixture,
    accept: bool,
) -> Result<(std::process::ExitStatus, String), Box<dyn Error>> {
    let server = serve_server_info(SERVER)?;
    let output = run_answering(
        fixture.command(&server.url(), None)?,
        move |line| {
            if line.ends_with(": ") && (line.contains("[Y]es ") || line.contains("[y]es ")) {
                Some(if accept { "y\n" } else { "n\n" })
            } else {
                None
            }
        },
        LIMIT,
    )?;
    Ok((output.status, output.transcript))
}

/// A clean interactive setup reuses the first effective-options probe.
#[test]
fn terminal_clean_setup_invokes_bazel_once() -> TestResult {
    let fixture = Fixture::with_home(HOME_CLEAN, "", true)?;
    let (status, output) = interactive(&fixture, true)?;
    assert_eq!(status.code(), Some(0), "{output}");
    let invocations = fs::read_to_string(fixture.dir.path().join("invocations"))?;
    assert_eq!(
        invocations.lines().count(),
        1,
        "clean setup must invoke Bazel once"
    );
    Ok(())
}

/// Interactive confirmation applies the minimal edit and keeps an exact backup.
#[test]
fn terminal_accepts_and_backs_up_conflicts() -> TestResult {
    let fixture = Fixture::new()?;
    let (status, output) = interactive(&fixture, true)?;
    assert!(
        status.success(),
        "interactive accept must exit successfully"
    );
    assert!(fixture.workspace.join(".gang/bazelrc").exists(), "{output}");
    assert_eq!(
        fs::read_to_string(fixture.home.join(".bazelrc"))?,
        HOME_FIXED,
        "{output}"
    );
    let backups: Vec<_> = fs::read_dir(&fixture.home)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".bazelrc.gang-backup-")
        })
        .collect();
    assert_eq!(backups.len(), 1, "{output}");
    assert_eq!(fs::read(backups[0].path())?, HOME_RC.as_bytes());
    let mode = fs::metadata(backups[0].path())?.permissions().mode() & 0o777;
    assert!(
        mode & !0o600 == 0,
        "backup must not grant group or world permissions"
    );
    Ok(())
}

/// Conflicts in one home rc are applied together and backed up only once.
#[test]
fn terminal_accepts_two_conflicts_in_one_file() -> TestResult {
    let fixture = Fixture::with_home(HOME_TWO, "X=y", false)?;
    let (status, output) = interactive(&fixture, true)?;
    assert!(
        status.success(),
        "interactive accept of two conflicts must exit successfully"
    );
    assert_eq!(
        fs::read_to_string(fixture.home.join(".bazelrc"))?,
        HOME_TWO_FIXED,
        "both conflicts must be edited: {output}"
    );
    let backups: Vec<_> = fs::read_dir(&fixture.home)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".bazelrc.gang-backup-")
        })
        .collect();
    assert_eq!(
        backups.len(),
        1,
        "two conflicts must create exactly one backup"
    );
    assert_eq!(fs::read(backups[0].path())?, HOME_TWO.as_bytes());
    Ok(())
}

/// Declining the first application leaves both trees byte-identical.
#[test]
fn terminal_decline_is_read_only() -> TestResult {
    let fixture = Fixture::new()?;
    let before = fixture.trees()?;
    let (status, _output) = interactive(&fixture, false)?;
    assert!(
        !status.success(),
        "interactive decline must exit unsuccessfully"
    );
    assert_eq!(fixture.trees()?, before);
    Ok(())
}
