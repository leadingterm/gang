//! Tests of staging the sample workspace and of the salt that keys each run's actions.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use gang_e2e::sample::{SampleParameters, salt, stage};
use gang_e2e::{E2eError, Probe, SIGNAL_COMMAND, Sandbox};
use gang_types::Isa;

/// Lists every entry under `root` as its relative path, with its content or link target and
/// its permission bits, so two trees can be compared exactly.
#[cfg(unix)]
fn snapshot(root: &Path) -> io::Result<BTreeMap<PathBuf, (String, u32)>> {
    use std::os::unix::fs::PermissionsExt;

    fn walk(
        root: &Path,
        current: &Path,
        out: &mut BTreeMap<PathBuf, (String, u32)>,
    ) -> io::Result<()> {
        for entry in fs::read_dir(current)? {
            let path = entry?.path();
            let relative = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_path_buf();
            let metadata = fs::metadata(&path)?;
            let mode = metadata.permissions().mode() & 0o777;
            if metadata.is_dir() {
                out.insert(relative, ("dir".to_owned(), mode));
                walk(root, &path, out)?;
            } else {
                out.insert(relative, (fs::read_to_string(&path)?, mode));
            }
        }
        Ok(())
    }

    let mut out = BTreeMap::new();
    walk(root, root, &mut out)?;
    Ok(out)
}

fn parameters() -> SampleParameters {
    SampleParameters {
        salt: "00ff10ab20cd30ef".to_owned(),
        action_path: "/usr/bin:/bin".to_owned(),
        signal: PathBuf::from("/tmp/e2e-signal"),
        attempts: PathBuf::from("/tmp/e2e-attempts"),
        isa: Isa::X86_64,
        memory_kb: 65_536,
        cpu_count: 1,
        large_fleet: "local-large".to_owned(),
        large_memory_kb: 16_777_216,
        requested_sandbox: Sandbox::Noop,
        glibc_tool: None,
    }
}

/// Generates a build file with one target per supplied probe.
fn build_file(probes: impl IntoIterator<Item = Probe>) -> Result<String, std::fmt::Error> {
    probes
        .into_iter()
        .try_fold(String::new(), |mut build, probe| {
            writeln!(build, "filegroup(name = \"{}\")", probe.as_str())?;
            Ok(build)
        })
}

#[cfg(unix)]
#[test]
fn staged_tree_equals_the_source_with_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir_all(source.join("nested/deeper")).expect("source tree is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    fs::write(source.join("MODULE.bazel"), "module(name = \"sample\")\n").expect("file is written");
    fs::write(source.join("nested/deeper/BUILD"), "# build\n").expect("file is written");
    let script = source.join("nested/run.sh");
    fs::write(&script, "#!/bin/sh\necho hi\n").expect("file is written");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("mode is set");

    let destination = work.path().join("staged");
    stage(&source, &destination, &parameters()).expect("sample stages");

    let mut staged = snapshot(&destination).expect("staged tree is readable");
    staged.remove(Path::new("config.bzl"));
    assert_eq!(staged, snapshot(&source).expect("source tree is readable"));
}

#[cfg(unix)]
#[test]
fn staged_glibc_tool_keeps_content_and_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    let tool = work.path().join("tool");
    fs::write(&tool, b"tool").expect("tool is written");
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).expect("mode is set");
    let destination = work.path().join("staged");
    let parameters = SampleParameters {
        glibc_tool: Some(tool),
        ..parameters()
    };

    stage(&source, &destination, &parameters).expect("sample stages");

    let staged = destination.join("glibc-tool");
    assert_eq!(fs::read(&staged).expect("tool is readable"), b"tool");
    assert_eq!(
        fs::metadata(&staged)
            .expect("tool exists")
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
}

#[test]
fn config_bzl_names_the_requested_sandbox_by_its_key() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    let destination = work.path().join("staged");
    let parameters = SampleParameters {
        requested_sandbox: Sandbox::Bwrap,
        ..parameters()
    };

    stage(&source, &destination, &parameters).expect("sample stages");

    let config = fs::read_to_string(destination.join("config.bzl")).expect("config is written");
    assert!(
        config.contains("REQUESTED_SANDBOX = \"bwrap\"\n"),
        "{config}"
    );
}

/// The staged configuration exposes the action environment values.
#[test]
fn config_bzl_defines_the_action_environment() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    let destination = work.path().join("staged");

    stage(&source, &destination, &parameters()).expect("sample stages");

    let config = fs::read_to_string(destination.join("config.bzl")).expect("config is written");
    assert!(
        config.contains("ACTION_PATH = \"/usr/bin:/bin\"\n"),
        "{config}"
    );
    assert!(
        config.contains("SIGNAL = \"/tmp/e2e-signal\"\n"),
        "{config}"
    );
    assert!(
        config.contains("ATTEMPTS = \"/tmp/e2e-attempts\"\n"),
        "{config}"
    );
}

/// Quotes and backslashes in action environment values remain inside Starlark strings.
#[test]
fn config_bzl_escapes_action_environment_values() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    let destination = work.path().join("staged");
    let parameters = SampleParameters {
        action_path: "path\"with\\slashes\nand\r\t\u{01}".to_owned(),
        signal: PathBuf::from(r#"signal"with\slashes"#),
        attempts: PathBuf::from(r#"attempts"with\slashes"#),
        ..parameters()
    };

    stage(&source, &destination, &parameters).expect("sample stages");

    let config = fs::read_to_string(destination.join("config.bzl")).expect("config is written");
    assert!(
        config
            .lines()
            .any(|line| line == r#"ACTION_PATH = "path\"with\\slashes\nand\r\t\x01""#),
        "{config}"
    );
    assert!(
        config
            .lines()
            .any(|line| line == r#"SIGNAL = "signal\"with\\slashes""#),
        "{config}"
    );
    assert!(
        config
            .lines()
            .any(|line| line == r#"ATTEMPTS = "attempts\"with\\slashes""#),
        "{config}"
    );
}

#[test]
fn config_bzl_defines_the_hold_signal_command() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    let destination = work.path().join("staged");

    stage(&source, &destination, &parameters()).expect("sample stages");

    let config = fs::read_to_string(destination.join("config.bzl")).expect("config is written");
    let expected = format!(
        "SIGNAL_COMMAND = \"{}\"\n",
        SIGNAL_COMMAND.replace('\\', "\\\\").replace('"', "\\\"")
    );
    assert!(config.contains(&expected), "{config}");
}

#[test]
fn config_bzl_overwrites_a_config_bzl_from_the_source() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    fs::write(source.join("config.bzl"), "SALT = \"stale\"\n").expect("file is written");
    let destination = work.path().join("staged");

    stage(&source, &destination, &parameters()).expect("sample stages");

    let config = fs::read_to_string(destination.join("config.bzl")).expect("config is written");
    assert!(config.contains("SALT = \"00ff10ab20cd30ef\"\n"));
    assert!(!config.contains("stale"));
}

#[test]
fn stage_accepts_all_probe_targets_with_varying_spaces() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    let mut build = String::new();
    for (index, probe) in Probe::ALL.into_iter().enumerate() {
        let spaces = if index % 2 == 0 { "" } else { "  " };
        writeln!(
            build,
            "filegroup(name{spaces}={spaces}\"{}\")",
            probe.as_str()
        )
        .expect("build file formats");
    }
    fs::write(source.join("BUILD.bazel"), build).expect("build file is written");

    stage(&source, &work.path().join("staged"), &parameters()).expect("sample stages");
}

#[test]
fn stage_rejects_a_missing_probe_target() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    let missing = Probe::ALL[0];
    let mut build = build_file(Probe::ALL.into_iter().filter(|probe| *probe != missing))
        .expect("build file formats");
    writeln!(build, "filegroup(name = \"{}-extra\")", missing.as_str())
        .expect("build file formats");
    fs::write(source.join("BUILD.bazel"), build).expect("build file is written");

    match stage(&source, &work.path().join("staged"), &parameters()) {
        Err(E2eError::System { context, .. }) => assert_eq!(
            context,
            format!(
                "the sample workspace has no target for the probe `{}`",
                missing.as_str()
            )
        ),
        other => panic!("expected a system error naming the missing probe, got {other:?}"),
    }
}

#[test]
fn existing_destination_is_an_error() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    let destination = work.path().join("staged");
    fs::create_dir(&destination).expect("destination already exists");

    let result = stage(&source, &destination, &parameters());
    assert!(matches!(result, Err(E2eError::Io { .. })));
}

#[cfg(unix)]
#[test]
fn symbolic_links_are_refused_and_named() {
    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    std::os::unix::fs::symlink("/etc/hosts", source.join("link")).expect("link is created");
    let destination = work.path().join("staged");

    match stage(&source, &destination, &parameters()) {
        Err(E2eError::System { context, .. }) => assert!(context.contains("link"), "{context}"),
        other => panic!("expected a system error naming the link, got {other:?}"),
    }
}

#[test]
fn salt_is_sixteen_lowercase_hex_characters() {
    let value = salt().expect("randomness is available");
    assert_eq!(value.len(), 16);
    assert!(
        value
            .chars()
            .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase()),
        "{value} is lowercase hex"
    );
}
