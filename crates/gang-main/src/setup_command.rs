//! Interactive, previewable setup of a Bazel workspace.

use std::io::{self, BufRead, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use gang_client::{
    BazelCommand, BazelProbe, BazelrcWriter, Change, ClientHome, ClientOptions, EffectiveOptions,
    GANG_CA, GANG_DIR, GANG_RC, ServerApi, Writable, backup_path, display_url, find_conflicts,
    gang_files, import_change, rc_edits, render_bazelrc, stored_login, workspace_root, writable,
};
use gang_clock::{Clock, SystemClock};
use gang_tls::ServerTrust;
use gang_types::{CertificatePem, Url};
use gang_wire::{Authentication, Server};
use serde_json::Value;

use crate::bazel_command::credential_helper;
use crate::cli::SetupArgs;
use crate::doctor_command;
use crate::errors::{MainError, Result};
use crate::output::{Line, Output, Record};
use crate::prompt::{Choice, Prompt, SharedInput};
use crate::runtime;
use crate::setup_mode::{self, Mode, Terminals};
use crate::setup_preview::preview;
use crate::setup_signin::{self, SignIn, Target};
use crate::workspace;

/// Workspace filename for the user's Bazel configuration.
const BAZELRC: &str = ".bazelrc";
/// Number of wizard steps.
const STEPS: usize = 5;
/// Message printed when the workspace rc gains the portable import.
const IMPORT_NOTE: &str = "added the import line to .bazelrc; it is safe to commit (try-import of a missing file does nothing)";
/// Choices for the initial workspace plan.
const PLAN_CHOICES: &[Choice] = &[
    Choice {
        key: 'y',
        label: "[Y]es",
        default: true,
    },
    Choice {
        key: 'n',
        label: "[n]o",
        default: false,
    },
    Choice {
        key: 's',
        label: "[s]tep by step",
        default: false,
    },
    Choice {
        key: 'f',
        label: "[f]ull files",
        default: false,
    },
];
/// Choices for each workspace file.
const FILE_CHOICES: &[Choice] = &[
    Choice {
        key: 'y',
        label: "[Y]es",
        default: true,
    },
    Choice {
        key: 'n',
        label: "[n]o",
        default: false,
    },
    Choice {
        key: 'f',
        label: "[f]ull file",
        default: false,
    },
];
/// Choices for edits to user-owned files.
const CONFLICT_CHOICES: &[Choice] = &[
    Choice {
        key: 'y',
        label: "[y]es",
        default: false,
    },
    Choice {
        key: 'n',
        label: "[N]o",
        default: true,
    },
    Choice {
        key: 'f',
        label: "[f]ull file",
        default: false,
    },
];

/// Runs setup using the process environment and terminal streams.
pub(crate) fn run(args: SetupArgs, out: &Output) -> Result<ExitCode> {
    let terminals = Terminals {
        stdin: io::stdin().is_terminal(),
        stderr: io::stderr().is_terminal(),
    };
    let mode = setup_mode::mode(args.yes, args.dry_run, terminals);
    let cwd = workspace::working_directory(|key| std::env::var_os(key))
        .ok_or(MainError::NoWorkingDirectory)?;
    let mut prompt = Prompt::new(SharedInput::default());
    run_with(args, out, &mut prompt, &cwd, mode)
}

/// The selected server and resources shared by the remaining wizard steps.
struct ServerStep {
    /// Server and authentication choices.
    target: Target,
    /// Public server information.
    info: Server<Value>,
    /// Resolved client options.
    options: ClientOptions,
    /// Clock shared with server and Bazel probes.
    clock: Arc<SystemClock>,
}

/// Runs the wizard with an injected answer stream and working directory.
fn run_with<R: BufRead>(
    mut args: SetupArgs,
    out: &Output,
    prompt: &mut Prompt<R>,
    cwd: &Path,
    mode: Mode,
) -> Result<ExitCode> {
    let server = server_step(&mut args, out, prompt, mode)?;
    sign_in_step(out, &server, mode)?;
    let root = match workspace_step(out, prompt, cwd, mode, &server)? {
        Ok(root) => root,
        Err(exit) => return Ok(exit),
    };
    let bazel = workspace_bazel(args, &server, &root);
    let effective = conflicts_step(out, prompt, mode, &server, &root, &bazel)?;
    finish_step(out, &server, &root, &bazel, effective)
}

/// Builds the Bazel probe for the selected workspace.
fn workspace_bazel(args: SetupArgs, server: &ServerStep, root: &Path) -> BazelCommand {
    BazelCommand::new(
        args.bazel,
        args.bazel_args,
        &server.options,
        server.clock.clone(),
    )
    .in_directory(root.to_path_buf())
}

/// Opens a wizard step with a numbered heading, on a terminal only.
fn heading(out: &Output, number: usize, name: &str) {
    out.decoration(
        None,
        &[
            Line::new(),
            Line::from(format!("step {number} of {STEPS} · {name}")),
        ],
    );
}

/// Resolves the server and reports its public information.
fn server_step<R: BufRead>(
    args: &mut SetupArgs,
    out: &Output,
    prompt: &mut Prompt<R>,
    mode: Mode,
) -> Result<ServerStep> {
    heading(out, 1, "server");
    let home = ClientHome::resolve(|key| std::env::var_os(key))?;
    let stored = stored_login(&home)?;
    let server = if let Some(server) = args.server.take() {
        server
    } else if mode.prompts() {
        let default = stored.as_ref().map_or("", |login| login.server.as_str());
        let value = prompt.ask(out, "server", default)?;
        Url::parse(&value).map_err(|source| MainError::SetupServerUrl { value, source })?
    } else {
        stored
            .as_ref()
            .map(|login| login.server.clone())
            .ok_or(MainError::NoServer)?
    };
    let ca_pin = gang_client::pin_for(&server, args.ca_pin.take(), stored.as_ref());
    let target = Target {
        server,
        ca_pin,
        provider: args.provider.take(),
    };
    let options = home.options()?;
    let clock = Arc::new(SystemClock::new());
    let api = ServerApi::try_new(
        &target.server,
        ServerTrust::from(target.ca_pin.as_ref()),
        &options,
        clock.clone(),
    )?;
    let info = runtime::block_on(api.server(None))?;
    out.result(&Record::ok(server_description(&target.server, &info)).subject("server"))?;
    Ok(ServerStep {
        target,
        info,
        options,
        clock,
    })
}

/// Signs in when permitted by the selected mode.
fn sign_in_step(out: &Output, server: &ServerStep, mode: Mode) -> Result<()> {
    heading(out, 2, "sign-in");
    if mode.signs_in() {
        match setup_signin::sign_in(out, &server.target, &server.info, mode.prompts())? {
            SignIn::Needed => out.result(
                &Record::warn("not signed in")
                    .subject("sign-in")
                    .fix(format!("gang login --server {}", server.target.server)),
            )?,
            SignIn::NotNeeded => out.result(&Record::ok("no sign-in needed").subject("sign-in"))?,
            SignIn::Already | SignIn::SignedIn => {}
        }
    } else {
        out.result(
            &Record::new(
                gang_client::Status::Skip.into(),
                "checked when the changes are applied",
            )
            .subject("sign-in"),
        )?;
    }
    Ok(())
}

/// Plans and applies workspace changes, stopping when changes remain unapplied.
fn workspace_step<R: BufRead>(
    out: &Output,
    prompt: &mut Prompt<R>,
    cwd: &Path,
    mode: Mode,
    server: &ServerStep,
) -> Result<std::result::Result<PathBuf, ExitCode>> {
    heading(out, 3, "workspace");
    let root = workspace_root(cwd)?;
    out.result(&Record::ok(root.display().to_string()).subject("workspace"))?;
    let helper = credential_helper(None)?;
    let cert = root.join(GANG_DIR).join(GANG_CA);
    let cert = cert
        .to_str()
        .ok_or_else(|| MainError::CertificatePath(cert.clone()))?;
    let host = server
        .target
        .server
        .host_str()
        .unwrap_or(server.target.server.as_str());
    let rc = render_bazelrc(&server.info, host, &helper, cert, BazelrcWriter::Setup);
    let mut changes = gang_files(
        &root,
        &rc,
        server
            .info
            .ca_certificate
            .as_ref()
            .map(CertificatePem::as_str),
    )?;
    if let Some(change) = import_change(&root)? {
        changes.push(change);
    }
    apply_plan(out, prompt, &root, &changes, mode)?;
    if !mode.writes() {
        out.notice("conflicts and checks run once the changes are applied");
        return if mode == Mode::PreviewOnly {
            Err(MainError::PreviewOnly)
        } else {
            Ok(Err(ExitCode::SUCCESS))
        };
    }
    ensure_workspace_applied(
        &root,
        &rc,
        server
            .info
            .ca_certificate
            .as_ref()
            .map(CertificatePem::as_str),
    )?;
    Ok(Ok(root))
}

/// Detects declined or partially applied changes by replanning the workspace.
fn ensure_workspace_applied(root: &Path, rc: &str, ca: Option<&str>) -> Result<()> {
    if !gang_files(root, rc, ca)?.is_empty() || import_change(root)?.is_some() {
        return Err(MainError::ChangesNotApplied);
    }
    Ok(())
}

/// Offers to resolve conflicts in user-owned rc files.
fn conflicts_step<R: BufRead>(
    out: &Output,
    prompt: &mut Prompt<R>,
    mode: Mode,
    server: &ServerStep,
    root: &Path,
    bazel: &BazelCommand,
) -> Result<Option<EffectiveOptions>> {
    heading(out, 4, "conflicts");
    if !mode.prompts() {
        out.notice("conflicts are reported by the checks below");
        return Ok(None);
    }
    let effective =
        match runtime::block_on(async { Ok::<_, MainError>(bazel.effective_options().await) })? {
            Ok(effective) => effective,
            Err(error) => {
                out.result(
                    &Record::warn("cannot read Bazel's effective flags")
                        .subject("conflicts")
                        .cause(error.to_string()),
                )?;
                return Ok(None);
            }
        };
    let mut edited = false;
    for file in rc_edits(find_conflicts(
        &server.info,
        &effective,
        &root.join(GANG_DIR).join(GANG_RC),
    )) {
        let before = std::fs::read(&file.path).map_err(|source| MainError::ReadConflict {
            path: file.path.clone(),
            source,
        })?;
        let change = Change {
            path: file.path.clone(),
            after: file.edit(&before)?,
            before: Some(before),
            backup: Some(backup_path(&file.path, server.clock.wall())),
        };
        for conflict in &file.conflicts {
            out.result(
                &Record::fail(format!("{}:{}", conflict.path.display(), conflict.line))
                    .subject("conflict")
                    .fix(conflict.fix()),
            )?;
        }
        show(out, &change, false)?;
        match writable(&change.path) {
            Writable::Yes => {
                if decide_file(out, prompt, &change, CONFLICT_CHOICES)? {
                    change.apply()?;
                    edited = true;
                }
            }
            Writable::Symlink { target } => {
                out.notice(format!("not offered: symlink to {}", target.display()));
            }
            Writable::NotOwned => {
                out.notice("not offered: file is not owned by the current user");
            }
            Writable::ReadOnly => {
                out.notice("not offered: file is read-only or not a regular file");
            }
        }
    }
    Ok((!edited).then_some(effective))
}

/// A successful Bazel reading retained while no rc edit invalidates it.
struct ReadOptions(EffectiveOptions);

impl BazelProbe for ReadOptions {
    async fn effective_options(
        &self,
    ) -> std::result::Result<EffectiveOptions, gang_client::BazelError> {
        Ok(self.0.clone())
    }
}

/// Runs final checks and reports the next command.
fn finish_step(
    out: &Output,
    server: &ServerStep,
    root: &Path,
    bazel: &BazelCommand,
    effective: Option<EffectiveOptions>,
) -> Result<ExitCode> {
    heading(out, 5, "checks");
    let owned_rc = root.join(GANG_DIR).join(GANG_RC);
    let target = &server.target;
    let report = match effective {
        Some(effective) => doctor_command::diagnose(
            out,
            &target.server,
            target.ca_pin.as_ref(),
            &server.options,
            &server.clock,
            &ReadOptions(effective),
            owned_rc,
        )?,
        None => doctor_command::diagnose(
            out,
            &target.server,
            target.ca_pin.as_ref(),
            &server.options,
            &server.clock,
            bazel,
            owned_rc,
        )?,
    };
    let exit = doctor_command::exit_code(report.as_ref());
    out.next(if exit == ExitCode::SUCCESS {
        "bazel build //..."
    } else {
        "gang setup"
    });
    Ok(exit)
}

/// Describes the server version and available authentication providers.
fn server_description(server: &Url, info: &Server<Value>) -> String {
    let version = info
        .version
        .as_ref()
        .map(|version| format!(" · {version}"))
        .unwrap_or_default();
    let auth = match &info.authentication {
        Authentication::None(_) => "no sign-in".to_owned(),
        Authentication::Token(token) => {
            let mut providers: Vec<_> = token.login.keys().map(ToString::to_string).collect();
            if let Some(device) = &token.device_login {
                let provider = device.provider.to_string();
                if !providers.contains(&provider) {
                    providers.push(provider);
                }
            }
            if providers.is_empty() {
                "sign-in: workload only".to_owned()
            } else {
                format!("sign-in: {}", providers.join(", "))
            }
        }
        Authentication::Unrecognized(_) => "sign-in: unknown".to_owned(),
    };
    format!("{}{version} · {auth}", display_url(server))
}

/// Prints exactly the change whose bytes will be applied.
fn show(out: &Output, change: &Change, full: bool) -> Result<()> {
    for line in preview(change, full) {
        out.data(line)?;
    }
    Ok(())
}

/// Repeats a per-file choice after displaying the full file when requested.
fn decide_file<R: BufRead>(
    out: &Output,
    prompt: &mut Prompt<R>,
    change: &Change,
    choices: &[Choice],
) -> Result<bool> {
    loop {
        match prompt.choose(out, &format!("apply {}?", change.path.display()), choices)? {
            'y' => return Ok(true),
            'n' => return Ok(false),
            'f' => show(out, change, true)?,
            _ => {}
        }
    }
}

/// Displays the complete plan before applying any part of it.
fn apply_plan<R: BufRead>(
    out: &Output,
    prompt: &mut Prompt<R>,
    root: &Path,
    changes: &[Change],
    mode: Mode,
) -> Result<()> {
    for change in changes {
        show(out, change, false)?;
    }
    if changes.is_empty() || !mode.writes() {
        return Ok(());
    }
    let step = if mode.prompts() {
        loop {
            match prompt.choose(out, "apply?", PLAN_CHOICES)? {
                'n' => return Ok(()),
                'y' => break false,
                's' => break true,
                'f' => {
                    for change in changes {
                        show(out, change, true)?;
                    }
                }
                _ => {}
            }
        }
    } else {
        false
    };
    for change in changes {
        if !step || decide_file(out, prompt, change, FILE_CHOICES)? {
            change.apply()?;
            if change.path == root.join(BAZELRC) {
                out.notice(IMPORT_NOTE);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    /// A server fixture that does not contact the network.
    fn test_server() -> ServerStep {
        let info = serde_json::from_value(serde_json::json!({
            "name": "server",
            "remoteExecutor": "grpcs://example.test:443",
            "remoteCache": "grpcs://example.test:443",
            "defaultIsa": "x86-64",
            "authentication": {"none": {}}
        }))
        .expect("server fixture");
        ServerStep {
            target: Target {
                server: Url::parse("https://example.test").expect("server URL"),
                ca_pin: None,
                provider: None,
            },
            info,
            options: ClientOptions::default(),
            clock: Arc::new(SystemClock::new()),
        }
    }

    /// Non-interactive setup defers conflict reporting to the checks.
    #[test]
    fn noninteractive_conflicts_defer_to_checks() {
        use clap::Parser;
        let dir = tempfile::tempdir().expect("temporary directory");
        let missing = dir.path().join("missing-bazel");
        let parsed = crate::cli::Cli::try_parse_from([
            "gang",
            "setup",
            "--bazel",
            missing.to_str().expect("UTF-8 path"),
        ])
        .expect("arguments");
        let crate::cli::Command::Setup(args) = parsed.command else {
            panic!("setup command");
        };
        let server = test_server();
        let bazel = workspace_bazel(args, &server, dir.path());
        for mode in [Mode::Yes, Mode::DryRun, Mode::PreviewOnly] {
            let (out, _, stderr) = Output::captured();
            let mut prompt = Prompt::new(Cursor::new(b""));
            conflicts_step(&out, &mut prompt, mode, &server, dir.path(), &bazel)
                .expect("conflicts");
            assert!(
                stderr
                    .text()
                    .contains("conflicts are reported by the checks below"),
                "non-interactive conflicts must defer to checks"
            );
        }
    }

    /// A failed interactive probe is reported rather than silently ignored.
    #[test]
    fn interactive_conflict_probe_failure_warns() {
        use clap::Parser;
        let dir = tempfile::tempdir().expect("temporary directory");
        let missing = dir.path().join("missing-bazel");
        let parsed = crate::cli::Cli::try_parse_from([
            "gang",
            "setup",
            "--bazel",
            missing.to_str().expect("UTF-8 path"),
        ])
        .expect("arguments");
        let crate::cli::Command::Setup(args) = parsed.command else {
            panic!("setup command");
        };
        let server = test_server();
        let bazel = workspace_bazel(args, &server, dir.path());
        let (out, stdout, _) = Output::captured();
        let mut prompt = Prompt::new(Cursor::new(b""));
        conflicts_step(
            &out,
            &mut prompt,
            Mode::Interactive,
            &server,
            dir.path(),
            &bazel,
        )
        .expect("conflicts");
        assert!(
            stdout
                .text()
                .contains("cannot read Bazel's effective flags"),
            "interactive conflict probe failure must warn"
        );
        assert!(
            stdout.text().contains("conflicts"),
            "warning must identify conflicts"
        );
    }

    /// The setup grammar accepts Bazel passthrough and rejects conflicting modes.
    #[test]
    fn setup_arguments_preserve_bazel_options() {
        use crate::cli::{Cli, Command};
        use clap::Parser;

        let parsed = Cli::try_parse_from([
            "gang",
            "setup",
            "--yes",
            "--bazel",
            "custom-bazel",
            "--",
            "--config=ci",
        ])
        .expect("setup arguments");
        let Command::Setup(args) = parsed.command else {
            panic!("setup command");
        };
        assert!(args.yes);
        assert_eq!(args.bazel, "custom-bazel");
        assert_eq!(args.bazel_args, ["--config=ci"]);
        assert!(Cli::try_parse_from(["gang", "setup", "--yes", "--dry-run"]).is_err());
    }

    /// A pair of planned edits, one to an existing file and one to a new file.
    fn plan(root: &Path) -> Vec<Change> {
        let first = root.join("first");
        std::fs::write(&first, b"old\n").expect("fixture");
        vec![
            Change {
                path: first,
                before: Some(b"old\n".to_vec()),
                after: b"new\n".to_vec(),
                backup: None,
            },
            Change {
                path: root.join("second"),
                before: None,
                after: b"second\n".to_vec(),
                backup: None,
            },
        ]
    }

    /// Declining the plan does not change existing or create new files.
    #[test]
    fn decline_leaves_tree_unchanged() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let changes = plan(dir.path());
        let (out, _, _) = Output::captured();
        let mut prompt = Prompt::new(Cursor::new(b"n\n"));
        apply_plan(&out, &mut prompt, dir.path(), &changes, Mode::Interactive).expect("plan");
        assert_eq!(std::fs::read(&changes[0].path).expect("original"), b"old\n");
        assert!(!changes[1].path.exists());
    }

    /// Declining all or part of a workspace plan reports a typed failure.
    #[test]
    fn unapplied_workspace_plan_is_an_error() {
        for answers in [b"n\n".as_slice(), b"s\ny\nn\nn\n".as_slice()] {
            let dir = tempfile::tempdir().expect("temporary directory");
            std::fs::write(dir.path().join("MODULE.bazel"), b"").expect("workspace marker");
            let (out, _, _) = Output::captured();
            let mut prompt = Prompt::new(Cursor::new(answers));
            let result = workspace_step(
                &out,
                &mut prompt,
                dir.path(),
                Mode::Interactive,
                &test_server(),
            );
            assert!(
                matches!(result, Err(MainError::ChangesNotApplied)),
                "declined workspace changes must return ChangesNotApplied"
            );
        }
    }

    /// A token server without interactive providers identifies workload-only sign-in.
    #[test]
    fn server_description_names_workload_only_server() {
        let info: Server<Value> = serde_json::from_value(serde_json::json!({
            "name": "server",
            "remoteExecutor": "grpcs://example.test:443",
            "remoteCache": "grpcs://example.test:443",
            "defaultIsa": "x86-64",
            "authentication": {"token": {"login": {}}}
        }))
        .expect("server fixture");
        let url = Url::parse("https://example.test").expect("server URL");
        let description = server_description(&url, &info);
        assert_eq!(description, "https://example.test · sign-in: workload only");
    }

    /// A device-only provider is available for sign-in.
    #[test]
    fn server_description_includes_device_only_provider() {
        let mut server = test_server();
        server.info.authentication = serde_json::from_value(serde_json::json!({
            "token": {"login": {}, "deviceLogin": {"provider": "okta"}}
        }))
        .expect("authentication fixture");
        let description = server_description(&server.target.server, &server.info);
        assert_eq!(description.rsplit(" · ").next(), Some("sign-in: okta"));
    }

    /// Dry runs show the same changes without reading input or changing files.
    #[test]
    fn dry_run_only_previews() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let changes = plan(dir.path());
        let (out, stdout, _) = Output::captured();
        let mut prompt = Prompt::new(Cursor::new(b""));
        apply_plan(&out, &mut prompt, dir.path(), &changes, Mode::DryRun).expect("plan");
        assert!(stdout.text().contains("+ new"));
        assert!(stdout.text().contains("+ second"));
        assert_eq!(std::fs::read(&changes[0].path).expect("original"), b"old\n");
        assert!(!changes[1].path.exists());
    }

    /// Adding the import reports its portability for both new and existing rc files.
    #[test]
    fn import_note_covers_new_and_existing_bazelrc() {
        for existing in [false, true] {
            let dir = tempfile::tempdir().expect("temporary directory");
            let path = dir.path().join(BAZELRC);
            let before = existing.then(|| b"old\n".to_vec());
            if let Some(bytes) = &before {
                std::fs::write(&path, bytes).expect("fixture");
            }
            let change = import_change(dir.path())
                .expect("plan import")
                .expect("import is needed");
            let (out, _, stderr) = Output::captured();
            let mut prompt = Prompt::new(Cursor::new(b""));
            apply_plan(&out, &mut prompt, dir.path(), &[change], Mode::Yes).expect("plan");
            assert!(stderr.text().contains(IMPORT_NOTE));
        }
    }

    /// Step-by-step consent applies only the approved file.
    #[test]
    fn step_by_step_applies_selected_file() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let changes = plan(dir.path());
        let (out, _, _) = Output::captured();
        let mut prompt = Prompt::new(Cursor::new(b"s\ny\nn\n"));
        apply_plan(&out, &mut prompt, dir.path(), &changes, Mode::Interactive).expect("plan");
        assert_eq!(
            std::fs::read(&changes[0].path).expect("updated"),
            changes[0].after
        );
        assert!(!changes[1].path.exists());
    }

    /// The planned added bytes shown in a new-file preview are the bytes written.
    #[test]
    fn preview_matches_written_bytes() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let change = Change {
            path: dir.path().join("new"),
            before: None,
            after: b"first\nsecond\n".to_vec(),
            backup: None,
        };
        let (out, stdout, _) = Output::captured();
        let mut prompt = Prompt::new(Cursor::new(b""));
        apply_plan(
            &out,
            &mut prompt,
            dir.path(),
            std::slice::from_ref(&change),
            Mode::Yes,
        )
        .expect("plan");
        let written = std::fs::read_to_string(&change.path).expect("written file");
        let shown = stdout.text();
        let added: String = shown
            .lines()
            .filter_map(|line| line.strip_prefix("+ "))
            .map(|line| format!("{line}\n"))
            .collect();
        assert_eq!(added, written);
        assert_eq!(written.as_bytes(), change.after);
    }
}
