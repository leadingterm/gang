//! Exercises setup against a TLS and token-authenticated deployment with a conflicting home rc.
//! After the suggested edit, a build discovers Gang through the workspace rc and runs remotely.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use gang_testkit::run_answering;

use crate::build_output::BuildOutcome;
use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::execution::Run;
use crate::gang_cli::{GangCli, client_home};
use crate::identity::Issuance;
use crate::issuer::TokenSpec;
use crate::names;
use crate::process;
use crate::secured::{self, Secured};
use crate::stack::Stack;
use crate::system::{Probe, System};
use crate::verdict;

/// Another service's cache and credential header, followed by an unrelated flag to preserve.
const HOME_RC: &str = "build --remote_cache=grpc://127.0.0.1:9 \"--remote_header=Authorization=Bearer e2e-leak\"\nbuild --jobs=8\n";
/// The same rc with the conflicting line moved into the opt-in configuration.
const HOME_RC_FIXED: &str = "build:not-gang --remote_cache=grpc://127.0.0.1:9 \"--remote_header=Authorization=Bearer e2e-leak\"\nbuild --jobs=8\n";

/// Runs setup, checks its advice and files, then builds after applying the suggested edit.
///
/// # Errors
///
/// Returns an error when the deployment or its commands cannot be driven.
pub fn setup(secured: &Secured<'_>) -> Result<Vec<Check>> {
    let stack = secured.stack();
    let home = stack.placement().root().join("setup-home");
    fs::create_dir_all(&home).map_err(E2eError::io(format!("creating {}", home.display())))?;
    let home_rc = home.join(".bazelrc");
    fs::write(&home_rc, HOME_RC).map_err(E2eError::io(format!("writing {}", home_rc.display())))?;
    let workspace = stack.placement().root().join("setup-workspace");
    stack.stage_sample(&workspace)?;
    let workspace = fs::canonicalize(&workspace)
        .map_err(E2eError::io(format!("resolving {}", workspace.display())))?;

    let endpoint = stack.endpoint()?;
    let pin = stack.ca_pin()?;
    let gang = fs::canonicalize(stack.binary()).map_err(E2eError::io(format!(
        "resolving {}",
        stack.binary().display()
    )))?;
    let cli = GangCli::new(gang.clone(), stack.placement().config_file(), home.clone());
    let credentials = match secured::log_in(
        &cli,
        secured.issuer(),
        &TokenSpec::admitted("setup"),
        &endpoint,
        &pin,
        &stack.placement().workspace(),
        stack.settings().command_timeout,
    )? {
        Issuance::Issued(credentials) => credentials,
        Issuance::Refused(output) => {
            return Err(E2eError::system(format!(
                "setup workload login refused: {output}"
            )));
        }
    };
    let authority = credentials
        .authority
        .ok_or_else(|| E2eError::system("setup login has no CA"))?;
    let command = |yes: bool| {
        let mut command = Command::new(&gang);
        command
            .arg("setup")
            .arg("--server")
            .arg(endpoint.as_str())
            .arg("--ca-pin")
            .arg(pin.to_string())
            .arg("--bazel")
            .arg(stack.bazel_program())
            .current_dir(&workspace)
            .env("NO_COLOR", "1")
            .env("PATH", stack.bazel().action_path());
        if yes {
            command.arg("--yes");
        }
        client_home(&mut command, &home);
        command
    };
    let output = process::run(command(true), None, stack.settings().command_timeout)?;
    let mut checks = initial_checks(&output, &workspace, &home_rc);

    checks.extend(interactive_checks(
        command(false),
        stack.settings().command_timeout,
        &home,
    )?);
    let after = process::run(command(true), None, stack.settings().command_timeout)?;
    checks.push(Check::expect(
        names::setup::YES_SUCCEEDS_AFTER_EDIT,
        after.code == Some(0),
        || {
            format!(
                "expected exit 0, got {:?}\n{}",
                after.code,
                after.combined()
            )
        },
    ));
    let after_text = after.combined();
    checks.push(Check::expect(
        names::setup::REUSES_SIGN_IN,
        after.code == Some(0)
            && after_text.lines().any(|line| {
                line.trim_start().starts_with("ok ")
                    && line.contains("sign-in")
                    && line.contains("signed in")
            }),
        || format!("expected a successful reused sign-in record\n{after_text}"),
    ));
    checks.extend(tls_checks(&workspace, &authority, &gang));
    checks.extend(build_checks(stack, &workspace, &home)?);
    Ok(checks)
}

/// Drives the interactive edit and checks its result and backup.
fn interactive_checks(command: Command, timeout: Duration, home: &Path) -> Result<Vec<Check>> {
    let interactive = run_interactive(command, timeout)?;
    let interactive_tail =
        BuildOutcome::new(interactive.code, interactive.stdout.clone()).stderr_tail(40);
    let interactive_failure =
        || format!("interactive gang setup output (last 40 lines):\n{interactive_tail}");
    let mut checks = vec![
        Check::expect(
            names::setup::INTERACTIVE_SUCCEEDS,
            interactive.code == Some(0),
            || {
                format!(
                    "expected exit 0, got {:?}\n{interactive_tail}",
                    interactive.code
                )
            },
        ),
        Check::expect(
            names::setup::INTERACTIVE_EDITS_HOME_RC,
            fs::read(home.join(".bazelrc")).is_ok_and(|rc| rc == HOME_RC_FIXED.as_bytes()),
            interactive_failure,
        ),
    ];
    let backups = fs::read_dir(home)
        .map_err(E2eError::io(format!("reading {}", home.display())))?
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(E2eError::io(format!("reading {}", home.display())))?;
    let backups: Vec<_> = backups
        .into_iter()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".bazelrc.gang-backup-")
        })
        .collect();
    checks.push(Check::expect(
        names::setup::INTERACTIVE_BACKS_UP_HOME_RC,
        backups.len() == 1 && fs::read(backups[0].path()).is_ok_and(|rc| rc == HOME_RC.as_bytes()),
        || {
            format!(
                "expected one exact home rc backup, found {}\n{interactive_tail}",
                backups.len()
            )
        },
    ));
    Ok(checks)
}

/// Checks the generated CA and authenticated Bazel settings.
fn tls_checks(workspace: &Path, authority: &Path, gang: &Path) -> Vec<Check> {
    let ca = workspace.join(".gang/ca.pem");
    let rc_path = workspace.join(".gang/bazelrc");
    let rc = fs::read_to_string(&rc_path).unwrap_or_default();
    vec![
        Check::expect(
            names::setup::WRITES_SERVER_CA,
            fs::read(authority).is_ok_and(|expected| {
                !expected.is_empty() && fs::read(&ca).is_ok_and(|actual| actual == expected)
            }),
            || {
                format!(
                    "{} differs from server CA {}",
                    ca.display(),
                    authority.display()
                )
            },
        ),
        Check::expect(
            names::setup::CONFIGURES_TLS_CERTIFICATE,
            rc.lines().any(|line| {
                line.starts_with("build --tls_certificate=")
                    && line.contains(&ca.display().to_string())
            }),
            || format!("{} does not reference {}", rc_path.display(), ca.display()),
        ),
        Check::expect(
            names::setup::CONFIGURES_CREDENTIAL_HELPER,
            gang.is_absolute()
                && rc.lines().any(|line| {
                    line.starts_with("build --credential_helper=")
                        && line.contains(&gang.display().to_string())
                }),
            || {
                format!(
                    "{} does not name helper {}",
                    rc_path.display(),
                    gang.display()
                )
            },
        ),
    ]
}

/// Builds with the edited rc files and checks that the action runs remotely.
fn build_checks(stack: &Stack<'_>, workspace: &Path, home: &Path) -> Result<Vec<Check>> {
    // A fresh run makes the action distinct from every earlier one, so it must execute.
    let run = Run::fresh()?;
    let outcome =
        stack
            .bazel()
            .build_with_user_rc(workspace, home, &[Probe::Unconstrained], Some(&run))?;
    Ok(vec![
        verdict::remote_execution(&outcome, 1, stack.stderr_tail_lines()),
        Check::expect(
            names::setup::BUILDS_REMOTELY_AFTER_THE_EDIT,
            outcome.succeeded(),
            || {
                format!(
                    "the build failed: {}",
                    outcome.stderr_tail(stack.stderr_tail_lines())
                )
            },
        ),
    ])
}

/// Checks the first noninteractive setup run and the files it wrote.
fn initial_checks(output: &process::Ran, workspace: &Path, home_rc: &Path) -> Vec<Check> {
    let text = output.combined();
    let tail = BuildOutcome::new(output.code, text.clone()).stderr_tail(40);
    let failure = || format!("gang setup output (last 40 lines):\n{tail}");
    let workspace_rc = fs::read_to_string(workspace.join(".bazelrc"));
    vec![
        Check::expect(
            names::setup::YES_EXITS_WITH_CONFLICT,
            output.code == Some(1),
            || format!("expected exit 1, got {:?}\n{tail}", output.code),
        ),
        Check::expect(
            names::setup::YES_REPORTS_FAILURE,
            text.lines()
                .any(|line| line.trim_start().starts_with("fail ")),
            failure,
        ),
        Check::expect(
            names::setup::REPORTS_THE_CONFLICTING_LINE,
            text.contains(&format!("{}:1", home_rc.display())),
            failure,
        ),
        Check::expect(
            names::setup::REPORTS_THE_MINIMAL_EDIT,
            text.contains("build:not-gang --remote_cache=grpc://127.0.0.1:9"),
            failure,
        ),
        Check::expect(
            names::setup::REPORTS_THE_HEADER_LEAK,
            text.contains("remote_header") && text.contains("credential header"),
            failure,
        ),
        Check::expect(
            names::setup::WRITES_THE_WORKSPACE_FILES,
            workspace.join(".gang/bazelrc").is_file()
                && workspace_rc.is_ok_and(|rc| {
                    rc.lines()
                        .any(|line| line == "try-import %workspace%/.gang/bazelrc")
                }),
            failure,
        ),
        Check::expect(
            names::setup::LEAVES_THE_HOME_RC_UNCHANGED,
            fs::read(home_rc).is_ok_and(|rc| rc == HOME_RC.as_bytes()),
            failure,
        ),
    ]
}

/// Drives every confirmation on a pseudo-terminal, with a bounded child lifetime.
fn run_interactive(mut command: Command, timeout: Duration) -> Result<process::Ran> {
    process::scrub_environment(&mut command);
    let output = run_answering(
        command,
        |line| {
            if line.ends_with(": ") && (line.contains("[Y]es ") || line.contains("[y]es ")) {
                Some("y\n")
            } else {
                None
            }
        },
        timeout,
    )
    .map_err(E2eError::io("driving the setup terminal"))?;
    let status = output.status;
    let stdout = output.transcript;
    Ok(process::Ran {
        code: status.code(),
        stdout,
        stderr: String::new(),
    })
}
