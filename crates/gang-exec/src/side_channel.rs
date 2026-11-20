//! Reports kernel usage through the worker's protected side channel.

use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use gang_spi::{ActionUsage, SIDE_CHANNEL_FILE_ENV};
use rustix::fs::{Mode, OFlags, open, openat};

/// Publishes the scope before the sandbox starts, when a side channel is available.
pub(crate) fn publish_cgroup(
    environment: &[(OsString, OsString)],
    action: &Path,
    cgroup: Option<std::path::PathBuf>,
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

fn write_file(action: &Path, path: &Path, name: &str, contents: &[u8]) -> io::Result<()> {
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
    let dir = open(
        &expected,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let file = openat(
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
}
