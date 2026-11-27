//! Reads worker-provided inputs and reports kernel usage through the protected side channel.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use gang_spi::{ActionUsage, MAX_EPHEMERAL_DIRS, MAX_RO_BINDS, SIDE_CHANNEL_FILE_ENV};
use gang_types::CpuWeight;
use rustix::fs;
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;

use crate::errors::SideChannelError;

/// File inside `.side_channels/<attempt_id>/` carrying `CpuWeight` as decimal text.
const CPU_WEIGHT_FILE: &str = "cpu_weight";
/// File inside `.side_channels/<attempt_id>/` carrying NUL-terminated `(source, target)` pairs.
const READ_ONLY_TREES_FILE: &str = "read_only_trees";
/// File containing NUL-terminated scratch paths relative to the action directory.
const EPHEMERAL_DIRS_FILE: &str = "ephemeral_dirs";
/// Maximum bytes accepted from the bounded scratch path list.
const MAX_EPHEMERAL_DIRS_BYTES: u64 = 64 * 1024;
/// Maximum bytes accepted from `cpu_weight`.
const MAX_CPU_WEIGHT_BYTES: u64 = 64;
/// Maximum bytes accepted from `read_only_trees` (bounded for `MAX_RO_BINDS` paths).
const MAX_READ_ONLY_TREES_BYTES: u64 = 512 * 1024;

/// A validated read-only input tree resolved into the canonical action directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadOnlyTreeOwned {
    /// Absolute host path to the immutable directory tree.
    pub source: PathBuf,
    /// Validated target path strictly inside the action directory.
    pub target: PathBuf,
}

/// Optional worker-provided inputs read from `.side_channels/<attempt_id>/`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SideChannelInputs {
    /// Read-only input trees to bind inside the action directory.
    pub read_only_trees: Vec<ReadOnlyTreeOwned>,
    /// Scratch directories resolved inside the canonical action directory.
    pub ephemeral_dirs: Vec<PathBuf>,
    /// Optional `cgroup` v2 `cpu.weight` for the action's scope.
    pub cpu_weight: Option<CpuWeight>,
}

/// Reads optional worker inputs from the protected directory when `GANG_SIDE_CHANNEL_FILE`
/// is configured. Read-only trees and scratch paths resolve inside the action directory.
pub(crate) fn read_inputs(
    environment: &[(OsString, OsString)],
    action: &Path,
) -> Result<SideChannelInputs, SideChannelError> {
    let Some(path) = channel_path(environment) else {
        return Ok(SideChannelInputs::default());
    };
    let dir = open_channel_dir(action, path).map_err(|_| SideChannelError::InvalidPath)?;
    let cpu_weight = read_cpu_weight(&dir)?;
    let read_only_trees = read_and_validate_trees(&dir, action)?;
    let ephemeral_dirs = read_ephemeral_dirs(&dir, action)?;
    Ok(SideChannelInputs {
        read_only_trees,
        ephemeral_dirs,
        cpu_weight,
    })
}

fn read_ephemeral_dirs(dir: &OwnedFd, action: &Path) -> Result<Vec<PathBuf>, SideChannelError> {
    let Some(bytes) = read_optional_file(dir, EPHEMERAL_DIRS_FILE, MAX_EPHEMERAL_DIRS_BYTES)?
    else {
        return Ok(Vec::new());
    };
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let Some(trimmed) = bytes.strip_suffix(b"\0") else {
        return Err(SideChannelError::MalformedEphemeralDirs);
    };
    let count = trimmed.split(|&byte| byte == 0).count();
    if count > MAX_EPHEMERAL_DIRS {
        return Err(SideChannelError::TooManyEphemeralDirs {
            count,
            max: MAX_EPHEMERAL_DIRS,
        });
    }
    let action_fd = fs::open(
        action,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| SideChannelError::TraverseTarget {
        target: action.to_path_buf(),
        source: source.into(),
    })?;
    let mut resolved = Vec::with_capacity(count);
    for field in trimmed.split(|&byte| byte == 0) {
        let target = Path::new(OsStr::from_bytes(field));
        if !valid_relative_target(target) {
            return Err(SideChannelError::InvalidEphemeralDir {
                path: target.to_path_buf(),
            });
        }
        walk_target_directory(&action_fd, target)?;
        resolved.push(action.join(target));
    }
    Ok(resolved)
}

fn read_cpu_weight(dir: &OwnedFd) -> Result<Option<CpuWeight>, SideChannelError> {
    let Some(bytes) = read_optional_file(dir, CPU_WEIGHT_FILE, MAX_CPU_WEIGHT_BYTES)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| SideChannelError::InvalidCpuWeight {
        reason: error.to_string(),
    })?;
    let weight =
        text.trim()
            .parse::<CpuWeight>()
            .map_err(|error| SideChannelError::InvalidCpuWeight {
                reason: error.to_string(),
            })?;
    Ok(Some(weight))
}

fn read_and_validate_trees(
    dir: &OwnedFd,
    action: &Path,
) -> Result<Vec<ReadOnlyTreeOwned>, SideChannelError> {
    let Some(bytes) = read_optional_file(dir, READ_ONLY_TREES_FILE, MAX_READ_ONLY_TREES_BYTES)?
    else {
        return Ok(Vec::new());
    };
    let pairs = parse_read_only_trees(&bytes)?;
    if pairs.len() > MAX_RO_BINDS {
        return Err(SideChannelError::TooManyReadOnlyTrees {
            count: pairs.len(),
            max: MAX_RO_BINDS,
        });
    }
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    let action_fd = fs::open(
        action,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| SideChannelError::TraverseTarget {
        target: action.to_path_buf(),
        source: source.into(),
    })?;
    let mut resolved = Vec::with_capacity(pairs.len());
    for (source, rel_target) in pairs {
        if !valid_source(&source) || !valid_relative_target(&rel_target) {
            return Err(SideChannelError::InvalidReadOnlyTree {
                host_path: source,
                target: rel_target,
            });
        }
        walk_target_directory(&action_fd, &rel_target)?;
        resolved.push(ReadOnlyTreeOwned {
            source,
            target: action.join(&rel_target),
        });
    }
    Ok(resolved)
}

fn valid_source(source: &Path) -> bool {
    source.is_absolute()
        && !source
            .components()
            .any(|component| matches!(component, Component::ParentDir))
}

fn valid_relative_target(target: &Path) -> bool {
    let mut components = target.components();
    let Some(Component::Normal(first)) = components.next() else {
        return false;
    };
    if first == ".side_channels" || first == ".graveyard" {
        return false;
    }
    components.all(|component| matches!(component, Component::Normal(_)))
}

fn walk_target_directory(action_fd: &OwnedFd, target: &Path) -> Result<(), SideChannelError> {
    let mut current: Option<OwnedFd> = None;
    for component in target.components() {
        let Component::Normal(segment) = component else {
            return Err(SideChannelError::TraverseTarget {
                target: target.to_path_buf(),
                source: io::Error::new(io::ErrorKind::InvalidInput, "invalid directory component"),
            });
        };
        let parent = current.as_ref().map_or(action_fd.as_fd(), AsFd::as_fd);
        let next = fs::openat(
            parent,
            segment,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|source| SideChannelError::TraverseTarget {
            target: target.to_path_buf(),
            source: source.into(),
        })?;
        current = Some(next);
    }
    if current.is_none() {
        return Err(SideChannelError::TraverseTarget {
            target: target.to_path_buf(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "empty target"),
        });
    }
    Ok(())
}

fn parse_read_only_trees(bytes: &[u8]) -> Result<Vec<(PathBuf, PathBuf)>, SideChannelError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let Some(trimmed) = bytes.strip_suffix(b"\0") else {
        return Err(SideChannelError::MalformedReadOnlyTrees);
    };
    let fields: Vec<&[u8]> = trimmed.split(|&byte| byte == 0).collect();
    if !fields.len().is_multiple_of(2) {
        return Err(SideChannelError::MalformedReadOnlyTrees);
    }
    let mut entries = Vec::with_capacity(fields.len() / 2);
    for pair in fields.chunks_exact(2) {
        let [source_bytes, target_bytes] = pair else {
            return Err(SideChannelError::MalformedReadOnlyTrees);
        };
        if source_bytes.is_empty() || target_bytes.is_empty() {
            return Err(SideChannelError::MalformedReadOnlyTrees);
        }
        entries.push((
            PathBuf::from(OsStr::from_bytes(source_bytes)),
            PathBuf::from(OsStr::from_bytes(target_bytes)),
        ));
    }
    Ok(entries)
}

fn read_optional_file(
    dir: &OwnedFd,
    name: &'static str,
    max_bytes: u64,
) -> Result<Option<Vec<u8>>, SideChannelError> {
    let fd = match fs::openat(
        dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(None),
        Err(errno) => {
            return Err(SideChannelError::Read {
                file: name,
                source: errno.into(),
            });
        }
    };
    let mut bytes = Vec::new();
    File::from(fd)
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| SideChannelError::Read { file: name, source })?;
    if u64::try_from(bytes.len()).is_ok_and(|len| len > max_bytes) {
        return Err(SideChannelError::Read {
            file: name,
            source: io::Error::new(
                io::ErrorKind::InvalidData,
                "side-channel file exceeds size limit",
            ),
        });
    }
    Ok(Some(bytes))
}

/// Publishes the trusted sandbox PID once startup completes.
pub(crate) fn publish_pid(environment: &[(OsString, OsString)], action: &Path, pid: Option<u32>) {
    let (Some(pid), Some(path)) = (pid, channel_path(environment)) else {
        return;
    };
    let result = (|| -> io::Result<()> {
        let dir = open_channel_dir(action, path)?;
        let file = fs::openat(
            &dir,
            "report.pid.tmp",
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?;
        File::from(file).write_all(pid.to_string().as_bytes())?;
        fs::renameat(&dir, "report.pid.tmp", &dir, "report.pid")?;
        Ok(())
    })();
    if let Err(error) = result {
        tracing::warn!(%error, "cannot write action accounting pid side channel");
    }
}

/// Publishes the scope before the sandbox starts, when a side channel is available.
pub(crate) fn publish_cgroup(
    environment: &[(OsString, OsString)],
    action: &Path,
    cgroup: Option<PathBuf>,
) {
    let Some(cgroup) = cgroup else { return };
    let Some(path) = channel_path(environment) else {
        return;
    };
    let result = if cgroup.is_absolute() {
        write_file(
            action,
            path,
            "report.cgroup",
            cgroup.as_os_str().as_encoded_bytes(),
        )
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cgroup path is not absolute",
        ))
    };
    if let Err(error) = result {
        tracing::warn!(%error, "cannot write action cgroup side channel");
    }
}

/// Reports measured kernel usage without changing the action's exit status.
pub(crate) fn report(
    environment: &[(OsString, OsString)],
    action: &Path,
    usage: Option<ActionUsage>,
) {
    let Some(usage) = usage else { return };
    let Some(path) = channel_path(environment) else {
        return;
    };
    if usage.oom_kills == 0 && usage.peak_memory.is_none() {
        return;
    }
    let peak = usage.peak_memory.map(|bytes| bytes.get().div_ceil(1024));
    let contents = match (usage.oom_kills > 0, peak) {
        (true, Some(peak)) => format!("{{\"failure\":\"memory\",\"peak_memory_kb\":{peak}}}"),
        (true, None) => String::from("{\"failure\":\"memory\"}"),
        (false, Some(peak)) => format!("{{\"peak_memory_kb\":{peak}}}"),
        (false, None) => return,
    };
    if let Err(error) = write_file(action, path, "report", contents.as_bytes()) {
        tracing::warn!(%error, "cannot write action usage side channel");
    }
}

fn channel_path(environment: &[(OsString, OsString)]) -> Option<&Path> {
    environment
        .iter()
        .find(|(name, _)| name == SIDE_CHANNEL_FILE_ENV)
        .map(|(_, path)| Path::new(path))
}

fn open_channel_dir(action: &Path, path: &Path) -> io::Result<OwnedFd> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid action side channel path",
        )
    };
    let (root, id) = action
        .parent()
        .zip(action.file_name())
        .ok_or_else(invalid)?;
    let parent = path
        .parent()
        .filter(|_| path.is_absolute() && path.file_name().is_some_and(|name| name == "report"))
        .ok_or_else(invalid)?;
    let expected = root.join(".side_channels").join(id);
    if std::fs::canonicalize(parent)? != expected {
        return Err(invalid());
    }
    fs::open(
        &expected,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(Into::into)
}

fn write_file(action: &Path, path: &Path, name: &str, contents: &[u8]) -> io::Result<()> {
    let dir = open_channel_dir(action, path)?;
    let file = fs::openat(
        &dir,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?;
    File::from(file).write_all(contents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_types::Bytes;
    use std::time::Duration;

    fn usage(peak: Option<u64>, oom_kills: u64) -> ActionUsage {
        ActionUsage {
            peak_memory: peak.map(Bytes::new),
            cpu_time: Duration::ZERO,
            oom_kills,
        }
    }

    #[test]
    fn reports_kernel_peaks_and_oom() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let action = root.path().canonicalize()?.join("a17");
        let dir = root.path().join(".side_channels/a17");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("report");
        let env = [(SIDE_CHANNEL_FILE_ENV.into(), path.clone().into_os_string())];
        report(&env, &action, Some(usage(None, 0)));
        assert!(!path.exists());
        for (peak, kills, expected) in [
            (Some(1025), 0, "{\"peak_memory_kb\":2}"),
            (
                Some(2050),
                3,
                "{\"failure\":\"memory\",\"peak_memory_kb\":3}",
            ),
            (None, 2, "{\"failure\":\"memory\"}"),
        ] {
            report(&env, &action, Some(usage(peak, kills)));
            assert_eq!(std::fs::read_to_string(&path)?, expected);
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    #[test]
    fn validates_parent_and_refuses_existing_files() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let real = root.path().join("real");
        let dir = real.join(".side_channels/a17");
        std::fs::create_dir_all(&dir)?;
        let action = real.canonicalize()?.join("a17");
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias)?;
        let path = alias.join(".side_channels/a17/report");
        write_file(&action, &path, "report", b"first")?;
        assert!(write_file(&action, &path, "report", b"second").is_err());
        assert_eq!(std::fs::read(&path)?, b"first");
        std::fs::remove_file(&path)?;
        let target = root.path().join("target");
        std::os::unix::fs::symlink(&target, &path)?;
        assert!(write_file(&action, &path, "report", b"bad").is_err());
        assert!(!target.exists());
        assert!(write_file(&action, Path::new("relative/report"), "report", b"bad").is_err());
        assert!(
            write_file(
                &action,
                &alias.join(".side_channels/a17/other"),
                "report",
                b"bad"
            )
            .is_err()
        );
        let other = real.join(".side_channels/other");
        std::fs::create_dir_all(&other)?;
        assert!(write_file(&action, &other.join("report"), "report", b"bad").is_err());
        Ok(())
    }

    #[test]
    fn cgroup_metadata_is_exclusive_and_absolute() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let action = root.path().canonicalize()?.join("a17");
        let dir = root.path().join(".side_channels/a17");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("report");
        let env = [(SIDE_CHANNEL_FILE_ENV.into(), path.into_os_string())];
        let metadata = dir.join("report.cgroup");
        publish_cgroup(&env, &action, Some("relative".into()));
        assert!(!metadata.exists());
        publish_cgroup(&env, &action, Some("/sys/fs/cgroup/actions/9-37-5".into()));
        publish_cgroup(&env, &action, Some("/sys/fs/cgroup/actions/9-37-6".into()));
        assert_eq!(std::fs::read(metadata)?, b"/sys/fs/cgroup/actions/9-37-5");
        Ok(())
    }

    fn encode_trees(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for (source, target) in entries {
            encoded.extend_from_slice(source.as_bytes());
            encoded.push(0);
            encoded.extend_from_slice(target.as_bytes());
            encoded.push(0);
        }
        encoded
    }

    #[test]
    fn reads_read_only_trees_and_cpu_weight_from_side_channel()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let canonical = root.path().canonicalize()?;
        let action = canonical.join("a17");
        std::fs::create_dir_all(action.join("work/external/toolchain"))?;
        std::fs::create_dir_all(action.join("work/external/sdk"))?;
        let dir = canonical.join(".side_channels/a17");
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("cpu_weight"), b"1000\n")?;
        std::fs::write(
            dir.join("read_only_trees"),
            encode_trees(&[
                ("/fast-cas/trees/a", "work/external/toolchain"),
                ("/fast-cas/trees/b", "work/external/sdk"),
            ]),
        )?;
        let env = [(
            SIDE_CHANNEL_FILE_ENV.into(),
            dir.join("report").into_os_string(),
        )];

        let inputs = read_inputs(&env, &action)?;

        assert_eq!(inputs.cpu_weight, Some(CpuWeight::GATE));
        assert_eq!(
            inputs.read_only_trees,
            vec![
                ReadOnlyTreeOwned {
                    source: PathBuf::from("/fast-cas/trees/a"),
                    target: action.join("work/external/toolchain"),
                },
                ReadOnlyTreeOwned {
                    source: PathBuf::from("/fast-cas/trees/b"),
                    target: action.join("work/external/sdk"),
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn rejects_symlink_in_intermediate_or_final_read_only_target_component()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let canonical = root.path().canonicalize()?;
        let action = canonical.join("a17");
        let outside = canonical.join("outside");
        std::fs::create_dir_all(outside.join("nested"))?;
        std::fs::create_dir_all(action.join("work"))?;
        std::os::unix::fs::symlink(&outside, action.join("work/link_dir"))?;
        let dir = canonical.join(".side_channels/a17");
        std::fs::create_dir_all(&dir)?;
        let env = [(
            SIDE_CHANNEL_FILE_ENV.into(),
            dir.join("report").into_os_string(),
        )];

        // Symlink as an intermediate component.
        std::fs::write(
            dir.join("read_only_trees"),
            encode_trees(&[("/fast-cas/tree", "work/link_dir/nested")]),
        )?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::TraverseTarget { .. })
        ));

        // Symlink as the final component.
        std::fs::write(
            dir.join("read_only_trees"),
            encode_trees(&[("/fast-cas/tree", "work/link_dir")]),
        )?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::TraverseTarget { .. })
        ));

        // Regular file instead of directory.
        std::fs::write(action.join("work/file"), b"not a dir")?;
        std::fs::write(
            dir.join("read_only_trees"),
            encode_trees(&[("/fast-cas/tree", "work/file")]),
        )?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::TraverseTarget { .. })
        ));

        // Missing directory component.
        std::fs::write(
            dir.join("read_only_trees"),
            encode_trees(&[("/fast-cas/tree", "work/missing")]),
        )?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::TraverseTarget { .. })
        ));
        Ok(())
    }

    #[test]
    fn rejects_invalid_relative_targets_and_over_limit_read_only_trees()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let canonical = root.path().canonicalize()?;
        let action = canonical.join("a17");
        std::fs::create_dir_all(action.join("work/sub"))?;
        let dir = canonical.join(".side_channels/a17");
        std::fs::create_dir_all(&dir)?;
        let env = [(
            SIDE_CHANNEL_FILE_ENV.into(),
            dir.join("report").into_os_string(),
        )];

        for (source, target) in [
            ("/fast-cas/tree", "/abs/target"),
            ("/fast-cas/tree", "../escape"),
            ("/fast-cas/tree", "work/../sub"),
            ("/fast-cas/tree", "./work/sub"),
            ("/fast-cas/tree", ".graveyard/x"),
            ("/fast-cas/tree", ".side_channels/x"),
            ("relative/source", "work/sub"),
            ("/fast-cas/../escape", "work/sub"),
        ] {
            std::fs::write(
                dir.join("read_only_trees"),
                encode_trees(&[(source, target)]),
            )?;
            assert!(
                matches!(
                    read_inputs(&env, &action),
                    Err(SideChannelError::InvalidReadOnlyTree { .. })
                ),
                "expected InvalidReadOnlyTree for ({source}, {target})"
            );
        }

        // More than MAX_RO_BINDS entries.
        let mut many = Vec::new();
        for index in 0..=MAX_RO_BINDS {
            let rel = format!("work/t-{index}");
            std::fs::create_dir_all(action.join(&rel))?;
            many.push((format!("/fast-cas/{index}"), rel));
        }
        let refs: Vec<(&str, &str)> = many.iter().map(|(s, t)| (s.as_str(), t.as_str())).collect();
        std::fs::write(dir.join("read_only_trees"), encode_trees(&refs))?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::TooManyReadOnlyTrees {
                count,
                max: MAX_RO_BINDS
            }) if count == MAX_RO_BINDS + 1
        ));

        // Exactly MAX_RO_BINDS entries succeeds.
        std::fs::write(
            dir.join("read_only_trees"),
            encode_trees(&refs[..MAX_RO_BINDS]),
        )?;
        assert_eq!(
            read_inputs(&env, &action)?.read_only_trees.len(),
            MAX_RO_BINDS
        );
        Ok(())
    }

    #[test]
    fn rejects_malformed_side_channel_files_and_symlinks() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let canonical = root.path().canonicalize()?;
        let action = canonical.join("a17");
        std::fs::create_dir_all(action.join("work"))?;
        let dir = canonical.join(".side_channels/a17");
        std::fs::create_dir_all(&dir)?;
        let env = [(
            SIDE_CHANNEL_FILE_ENV.into(),
            dir.join("report").into_os_string(),
        )];

        for bad_weight in ["0", "10001", "abc", ""] {
            std::fs::write(dir.join("cpu_weight"), bad_weight)?;
            assert!(matches!(
                read_inputs(&env, &action),
                Err(SideChannelError::InvalidCpuWeight { .. })
            ));
        }
        std::fs::remove_file(dir.join("cpu_weight"))?;

        for bad_trees in [
            b"/fast-cas/a\0work".as_slice(),
            b"/fast-cas/a\0".as_slice(),
            b"/fast-cas/a\0\0".as_slice(),
        ] {
            std::fs::write(dir.join("read_only_trees"), bad_trees)?;
            assert!(matches!(
                read_inputs(&env, &action),
                Err(SideChannelError::MalformedReadOnlyTrees)
            ));
        }
        std::fs::remove_file(dir.join("read_only_trees"))?;

        let victim = canonical.join("victim");
        std::fs::write(&victim, b"1000")?;
        std::os::unix::fs::symlink(&victim, dir.join("cpu_weight"))?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::Read {
                file: CPU_WEIGHT_FILE,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn absent_worker_inputs_default_to_empty() {
        assert_eq!(
            read_inputs(&[], Path::new("/action")).expect("no channel"),
            SideChannelInputs::default()
        );
    }

    fn scratch_channel() -> io::Result<(tempfile::TempDir, PathBuf, PathBuf)> {
        let root = tempfile::tempdir()?;
        let canonical = root.path().canonicalize()?;
        let action = canonical.join("a17");
        let channel = canonical.join(".side_channels/a17");
        std::fs::create_dir_all(&channel)?;
        std::fs::create_dir_all(action.join("work/tmp"))?;
        Ok((root, action, channel))
    }

    #[test]
    fn reads_ephemeral_dirs_up_to_the_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (_root, action, channel) = scratch_channel()?;
        let env = [(
            SIDE_CHANNEL_FILE_ENV.into(),
            channel.join("report").into_os_string(),
        )];
        assert!(read_inputs(&env, &action)?.ephemeral_dirs.is_empty());
        let file = channel.join(EPHEMERAL_DIRS_FILE);
        std::fs::write(&file, b"work/tmp\0")?;
        assert_eq!(
            read_inputs(&env, &action)?.ephemeral_dirs,
            [action.join("work/tmp")]
        );
        let mut encoded = Vec::new();
        for index in 0..MAX_EPHEMERAL_DIRS {
            let dir = format!("tmp-{index}");
            std::fs::create_dir(action.join(&dir))?;
            encoded.extend_from_slice(dir.as_bytes());
            encoded.push(0);
        }
        std::fs::write(&file, &encoded)?;
        assert_eq!(
            read_inputs(&env, &action)?.ephemeral_dirs.len(),
            MAX_EPHEMERAL_DIRS
        );
        encoded.extend_from_slice(b"extra\0");
        std::fs::write(&file, &encoded)?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::TooManyEphemeralDirs { count, max })
                if count == MAX_EPHEMERAL_DIRS + 1 && max == MAX_EPHEMERAL_DIRS
        ));
        std::fs::write(&file, b"")?;
        assert!(read_inputs(&env, &action)?.ephemeral_dirs.is_empty());
        Ok(())
    }

    #[test]
    fn rejects_malformed_or_escaping_scratch_paths() -> Result<(), Box<dyn std::error::Error>> {
        let (_root, action, channel) = scratch_channel()?;
        let env = [(
            SIDE_CHANNEL_FILE_ENV.into(),
            channel.join("report").into_os_string(),
        )];
        let file = channel.join(EPHEMERAL_DIRS_FILE);
        for invalid in [
            b"\0".as_slice(),
            b"/absolute\0",
            b"../escape\0",
            b"work/../tmp\0",
            b"./work/tmp\0",
            b".side_channels/x\0",
            b".graveyard/x\0",
            b"work/tmp\0\0",
        ] {
            std::fs::write(&file, invalid)?;
            assert!(
                matches!(
                    read_inputs(&env, &action),
                    Err(SideChannelError::InvalidEphemeralDir { .. })
                ),
                "accepted {invalid:?}"
            );
        }
        std::fs::write(&file, b"work/tmp")?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::MalformedEphemeralDirs)
        ));
        let oversized = vec![b'x'; usize::try_from(MAX_EPHEMERAL_DIRS_BYTES)? + 1];
        std::fs::write(&file, oversized)?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::Read {
                file: EPHEMERAL_DIRS_FILE,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn scratch_targets_must_be_existing_directories_without_symlinks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, action, channel) = scratch_channel()?;
        let env = [(
            SIDE_CHANNEL_FILE_ENV.into(),
            channel.join("report").into_os_string(),
        )];
        let file = channel.join(EPHEMERAL_DIRS_FILE);
        let outside = root.path().join("outside");
        std::fs::create_dir_all(outside.join("nested"))?;
        std::os::unix::fs::symlink(&outside, action.join("link"))?;
        std::fs::write(action.join("regular"), b"not a directory")?;
        for path in [
            b"link\0".as_slice(),
            b"link/nested\0",
            b"regular\0",
            b"missing\0",
        ] {
            std::fs::write(&file, path)?;
            assert!(
                matches!(
                    read_inputs(&env, &action),
                    Err(SideChannelError::TraverseTarget { .. })
                ),
                "accepted {path:?}"
            );
        }
        std::fs::remove_file(&file)?;
        std::os::unix::fs::symlink(action.join("work/tmp"), &file)?;
        assert!(matches!(
            read_inputs(&env, &action),
            Err(SideChannelError::Read {
                file: EPHEMERAL_DIRS_FILE,
                ..
            })
        ));
        Ok(())
    }
}
