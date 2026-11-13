//! Behaviour of the Bazel argument vectors: what each remote mode passes, and in what order.

use std::path::{Path, PathBuf};

use gang_e2e::flags::{RUN_DEFINE, Remote, build_args, helper_path, startup_args};
use gang_e2e::{ActionTools, Probe, Run, Settings};

const OUTPUT_ROOT: &str = "/state/bazel";
const TARGET: &str = "//:isolated";

fn settings() -> Settings {
    Settings {
        remote_retries: 7,
        bazel_flags: vec!["--extra_build_option".to_owned()],
        bazel_startup_flags: vec!["--extra_startup_option".to_owned()],
        ..Settings::default()
    }
}

/// The pinned tools passed to sample actions.
fn action_tools() -> ActionTools {
    ActionTools {
        shell: PathBuf::from("/nix/store/example-bash/bin/bash"),
        path: "/nix/store/example-coreutils/bin".to_owned(),
        action_bin: PathBuf::from("/nix/store/ddd-action-tools/bin"),
        bwrap: None,
        glibc_tool: None,
        glibc_loader: Vec::new(),
    }
}

fn gang() -> Remote {
    Remote::Gang {
        host: "gang.example".to_owned(),
        bazelrc: PathBuf::from("/workspace/gang.bazelrc"),
        helper: PathBuf::from("/usr/local/bin/gang"),
        home: PathBuf::from("/state/client-home"),
    }
}

fn plain() -> Remote {
    Remote::Plain {
        bazelrc: PathBuf::from("/workspace/gang.bazelrc"),
        home: PathBuf::from("/state/client-home"),
    }
}

fn position(args: &[String], wanted: &str) -> usize {
    let found = args.iter().position(|argument| argument == wanted);
    assert!(found.is_some(), "`{wanted}` is missing from {args:?}");
    found.unwrap_or_default()
}

fn probes() -> &'static [Probe] {
    &[Probe::Isolated]
}

#[test]
fn gang_build_passes_the_harness_flags() {
    let args = build_args(&settings(), &action_tools(), &gang(), probes(), None);

    assert!(args.contains(&"--shell_executable=/nix/store/example-bash/bin/bash".to_owned()));
    assert!(args.contains(&"--incompatible_strict_action_env".to_owned()));
    assert!(args.contains(&"--remote_retries=7".to_owned()));
    assert_eq!(args.first().map(String::as_str), Some("build"));
}

#[test]
fn builds_do_not_pass_action_environment_flags() {
    for remote in [gang(), plain()] {
        let args = build_args(&settings(), &action_tools(), &remote, probes(), None);
        assert!(
            args.iter()
                .all(|argument| !argument.starts_with("--action_env"))
        );
    }
}

#[test]
fn every_build_asks_for_verbose_failures() {
    for remote in [gang(), plain()] {
        let args = build_args(&settings(), &action_tools(), &remote, probes(), None);

        let verbose = position(&args, "--verbose_failures");
        assert!(
            verbose < position(&args, TARGET),
            "`--verbose_failures` comes before the targets for {remote:?}"
        );
    }
}

#[test]
fn every_build_keeps_going_after_a_failed_action() {
    for remote in [gang(), plain()] {
        let args = build_args(&settings(), &action_tools(), &remote, probes(), None);

        assert!(
            args.contains(&"--keep_going".to_owned()),
            "`--keep_going` is passed for {remote:?}"
        );
    }
}

#[test]
fn extra_build_flags_sit_between_harness_flags_and_targets() {
    let args = build_args(&settings(), &action_tools(), &gang(), probes(), None);

    let extra = position(&args, "--extra_build_option");
    let harness = position(&args, "--incompatible_strict_action_env");
    let first_target = position(&args, TARGET);

    assert!(harness < extra, "extra flags come after the harness flags");
    assert!(extra < first_target, "extra flags come before the targets");
    assert_eq!(
        args[args.len() - probes().len()..],
        [TARGET],
        "the targets are the last arguments"
    );
}

#[test]
fn a_probe_build_flag_precedes_the_labels() {
    let settings = settings();
    let flag = "--experimental_sibling_repository_layout";
    let args = build_args(
        &settings,
        &action_tools(),
        &gang(),
        &[Probe::Victim, Probe::InputRoot],
        None,
    );

    assert_eq!(
        args.iter()
            .filter(|argument| argument.as_str() == flag)
            .count(),
        1
    );
    let flag_position = position(&args, flag);
    for extra in &settings.bazel_flags {
        assert!(position(&args, extra) < flag_position);
    }
    assert!(flag_position < position(&args, "//:victim"));
    assert!(flag_position < position(&args, "//:input_root"));

    let victim_args = build_args(&settings, &action_tools(), &gang(), &[Probe::Victim], None);
    assert!(!victim_args.iter().any(|argument| argument == flag));
}

#[test]
fn gang_build_names_no_executor_and_asks_the_credential_helper() {
    let args = build_args(&settings(), &action_tools(), &gang(), probes(), None);

    assert!(
        args.iter()
            .all(|argument| !argument.starts_with("--remote_executor"))
    );
    assert!(args.contains(&"--credential_helper=gang.example=/usr/local/bin/gang".to_owned()));
    assert!(args.contains(&"--remote_retries=7".to_owned()));
}

#[test]
fn plain_build_names_no_executor_and_no_credential_helper() {
    let args = build_args(&settings(), &action_tools(), &plain(), probes(), None);

    assert!(args.iter().all(|argument| {
        !argument.starts_with("--remote_executor")
            && !argument.starts_with("--remote_default_exec_properties")
            && !argument.starts_with("--credential_helper")
    }));
}

#[test]
fn helper_path_puts_the_helper_directory_first() {
    let path = helper_path(
        Path::new("/usr/local/bin/gang"),
        "/nix/store/coreutils/bin:/nix/store/other/bin",
    );

    assert_eq!(
        path.to_str(),
        Some("/usr/local/bin:/nix/store/coreutils/bin:/nix/store/other/bin")
    );
}

#[test]
fn helper_path_without_pinned_tools_is_the_helper_directory() {
    let path = helper_path(Path::new("/usr/local/bin/gang"), "");

    assert_eq!(path.to_str(), Some("/usr/local/bin"));
}

#[test]
fn startup_always_sets_the_output_root_and_skips_home_and_system_rc() {
    for remote in [gang(), plain()] {
        let args = startup_args(&settings(), Path::new(OUTPUT_ROOT), &remote);

        assert!(args.contains(&format!("--output_user_root={OUTPUT_ROOT}")));
        assert!(args.contains(&"--nohome_rc".to_owned()));
        assert!(args.contains(&"--nosystem_rc".to_owned()));
    }
}

#[test]
fn extra_startup_flags_come_last() {
    let args = startup_args(&settings(), Path::new(OUTPUT_ROOT), &gang());

    assert_eq!(
        args.last().map(String::as_str),
        Some("--extra_startup_option")
    );
}

#[test]
fn every_remote_ignores_host_and_workspace_rc_files() {
    for remote in [gang(), plain()] {
        let args = startup_args(&settings(), Path::new(OUTPUT_ROOT), &remote);
        let rc = format!("--bazelrc={}", remote.bazelrc().display());
        let first_setting = position(&args, "--extra_startup_option");

        for wanted in ["--nohome_rc", "--nosystem_rc", "--noworkspace_rc", &rc] {
            assert!(
                position(&args, wanted) < first_setting,
                "`{wanted}` comes before the settings' startup flags for {remote:?}"
            );
        }
    }
}

#[test]
fn only_a_held_build_defines_its_run() -> gang_e2e::Result<()> {
    let run = Run::fresh()?;
    let held = build_args(&settings(), &action_tools(), &gang(), probes(), Some(&run));
    assert!(held.contains(&format!("--define={RUN_DEFINE}={run}")));
    let plain = build_args(&settings(), &action_tools(), &gang(), probes(), None);
    assert!(
        !plain
            .iter()
            .any(|arg| arg.starts_with(&format!("--define={RUN_DEFINE}=")))
    );
    Ok(())
}
