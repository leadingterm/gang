//! Tests of staging the sample workspace and of the salt that keys each run's actions.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use gang_e2e::egress_wire::EgressTargets;
use gang_e2e::sample::{SampleParameters, salt, stage};
use gang_e2e::{E2eError, Probe, SIGNAL_COMMAND, Sandbox};
use gang_types::Isa;

/// Lists every entry under `root` as its relative path, with its content or link target and
/// its permission bits, so two trees can be compared exactly.
#[cfg(unix)]
fn snapshot(root: &Path, excluded: Option<&Path>) -> io::Result<BTreeMap<PathBuf, (Vec<u8>, u32)>> {
    use std::os::unix::fs::PermissionsExt;

    fn walk(
        root: &Path,
        current: &Path,
        excluded: Option<&Path>,
        out: &mut BTreeMap<PathBuf, (Vec<u8>, u32)>,
    ) -> io::Result<()> {
        for entry in fs::read_dir(current)? {
            let path = entry?.path();
            let relative = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_path_buf();
            if excluded == Some(relative.as_path()) {
                continue;
            }
            let metadata = fs::metadata(&path)?;
            let mode = metadata.permissions().mode() & 0o777;
            if metadata.is_dir() {
                out.insert(relative, (b"dir".to_vec(), mode));
                walk(root, &path, excluded, out)?;
            } else {
                out.insert(relative, (fs::read(&path)?, mode));
            }
        }
        Ok(())
    }

    let mut out = BTreeMap::new();
    walk(root, root, excluded, &mut out)?;
    Ok(out)
}

#[cfg(unix)]
fn same_file_contents(first: &Path, second: &Path) -> io::Result<bool> {
    use std::io::Read;

    let mut first = fs::File::open(first)?;
    let mut second = fs::File::open(second)?;
    let mut remaining = first.metadata()?.len();
    if remaining != second.metadata()?.len() {
        return Ok(false);
    }
    let mut a = [0_u8; 8192];
    let mut b = [0_u8; 8192];
    while remaining != 0 {
        let size = usize::try_from(remaining.min(a.len() as u64)).map_err(io::Error::other)?;
        first.read_exact(&mut a[..size])?;
        second.read_exact(&mut b[..size])?;
        if a[..size] != b[..size] {
            return Ok(false);
        }
        remaining -= size as u64;
    }
    Ok(true)
}

fn parameters() -> SampleParameters {
    SampleParameters {
        salt: "00ff10ab20cd30ef".to_owned(),
        action_path: "/usr/bin:/bin".to_owned(),
        action_shell: PathBuf::from("/bin/bash"),
        action_ipc: false,
        host_action_path: "/usr/bin:/bin".to_owned(),
        host_action_shell: PathBuf::from("/bin/bash"),
        signal: PathBuf::from("/tmp/e2e-signal"),
        attempts: PathBuf::from("/tmp/e2e-attempts"),
        isa: Isa::X86_64,
        memory_kb: 65_536,
        cpu_count: 1,
        large_fleet: "local-large".to_owned(),
        large_memory_kb: 16_777_216,
        requested_sandbox: Sandbox::Noop,
        glibc_tool: None,
        egress_probe: None,
        egress_targets: None,
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

    let mut staged = snapshot(&destination, Some(Path::new("action-environment-probe")))
        .expect("staged tree is readable");
    staged.remove(Path::new("config.bzl"));
    assert!(staged.remove(Path::new("action_environment.bzl")).is_some());
    let executable = std::env::current_exe().expect("current executable");
    let probe = destination.join("action-environment-probe");
    assert!(same_file_contents(&probe, &executable).expect("comparing executable contents"));
    assert_eq!(
        fs::metadata(probe)
            .expect("probe metadata")
            .permissions()
            .mode()
            & 0o777,
        fs::metadata(executable)
            .expect("executable metadata")
            .permissions()
            .mode()
            & 0o777,
    );
    assert_eq!(
        staged,
        snapshot(&source, None).expect("source tree is readable")
    );
}

#[cfg(unix)]
#[test]
fn restaging_replaces_readonly_stale_action_probe() {
    use std::os::unix::fs::PermissionsExt;

    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source directory is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    let first = work.path().join("first");
    stage(&source, &first, &parameters()).expect("first sample stages");
    let stale = first.join("action-environment-probe");
    fs::remove_file(&stale).expect("remove first probe");
    fs::write(&stale, b"stale").expect("write stale probe");
    fs::set_permissions(&stale, fs::Permissions::from_mode(0o555))
        .expect("make stale probe readonly");

    let second = work.path().join("second");
    stage(&first, &second, &parameters()).expect("sample restages over readonly probe");
    let executable = std::env::current_exe().expect("current executable");
    let probe = second.join("action-environment-probe");
    assert!(same_file_contents(&probe, &executable).expect("comparing executable contents"));
    assert_eq!(
        fs::metadata(probe)
            .expect("probe metadata")
            .permissions()
            .mode()
            & 0o777,
        fs::metadata(executable)
            .expect("executable metadata")
            .permissions()
            .mode()
            & 0o777,
    );
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

#[cfg(unix)]
#[test]
fn stages_egress_inputs_with_executable_permissions_and_round_trips_targets() {
    use std::os::unix::fs::PermissionsExt;

    let work = tempfile::tempdir().expect("work directory is created");
    let source = work.path().join("source");
    fs::create_dir(&source).expect("source is created");
    fs::write(
        source.join("BUILD.bazel"),
        build_file(Probe::ALL).expect("build file formats"),
    )
    .expect("build file is written");
    let tool = work.path().join("probe");
    fs::write(&tool, b"probe executable").expect("probe is written");
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).expect("mode is set");
    let targets: EgressTargets = serde_json::from_value(serde_json::json!({
        "public_ipv4": "93.184.216.34:8000",
        "public_ipv6": "[2001:4860:1234::34]:8001",
        "blocked": {"vpc4": "10.20.0.10:8002"},
        "dns": "93.184.216.34",
        "nonce": "fixture-nonce",
        "uid": 1234,
        "gid": 5678,
        "nft": "/nix/store/test-nft/bin/nft",
        "timeout": "5s",
        "policy": {
            "protected_networks": ["10.20.0.0/16"],
            "dns_servers": ["93.184.216.34"]
        }
    }))
    .expect("targets deserialize");
    let destination = work.path().join("staged");
    let parameters = SampleParameters {
        egress_probe: Some(tool),
        egress_targets: Some(targets.clone()),
        ..parameters()
    };

    stage(&source, &destination, &parameters).expect("sample stages");

    let staged = destination.join("egress-probe");
    assert_eq!(
        fs::read(&staged).expect("probe is readable"),
        b"probe executable"
    );
    assert_eq!(
        fs::metadata(&staged)
            .expect("probe exists")
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    let staged_targets: EgressTargets = serde_json::from_slice(
        &fs::read(destination.join("egress-targets.json")).expect("targets exist"),
    )
    .expect("targets deserialize");
    assert_eq!(staged_targets, targets);
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
        config.contains("ACTION_SHELL = \"/bin/bash\"\n"),
        "{config}"
    );
    assert!(config.contains("ACTION_IPC = False\n"), "{config}");
    assert!(
        config.contains("HOST_ACTION_PATH = \"/usr/bin:/bin\"\n"),
        "{config}"
    );
    assert!(
        config.contains("HOST_ACTION_SHELL = \"/bin/bash\"\n"),
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
