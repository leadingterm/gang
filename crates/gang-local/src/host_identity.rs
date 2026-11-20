//! Private, atomically published identity for one logical controller installation.

use std::io::ErrorKind;
use std::path::Path;

use gang_os::fs::{PRIVATE_FILE, ensure_private_dir};
use gang_types::InstanceId;

use crate::errors::LocalError;

/// Random identity size; the file contains its lowercase hexadecimal encoding.
const IDENTITY_BYTES: usize = 16;

/// The caller must verify the configured state root before opening this private child directory.
/// Concurrent creators read the winning publication rather than replace it.
pub(crate) fn load_or_create(path: &Path) -> Result<InstanceId, LocalError> {
    let parent = path.parent().ok_or_else(|| {
        gang_os::PathIo::new(
            "read controller host identity",
            path,
            ErrorKind::InvalidInput.into(),
        )
    })?;
    let name = path.file_name().ok_or_else(|| {
        gang_os::PathIo::new(
            "read controller host identity",
            path,
            ErrorKind::InvalidInput.into(),
        )
    })?;
    let dir = ensure_private_dir(parent)?;
    if let Some(bytes) = dir.read_optional(name)? {
        return decode(path, &bytes);
    }

    let mut bytes = [0_u8; IDENTITY_BYTES];
    getrandom::fill(&mut bytes).map_err(|source| LocalError::Random(source.to_string()))?;
    let encoded = hex::encode(bytes);
    if dir.write_new_atomic(name, encoded.as_bytes(), PRIVATE_FILE)? {
        return decode(path, encoded.as_bytes());
    }
    let existing = dir.read_optional(name)?.ok_or_else(|| {
        gang_os::PathIo::new(
            "read controller host identity",
            path,
            ErrorKind::NotFound.into(),
        )
    })?;
    decode(path, &existing)
}

fn decode(path: &Path, bytes: &[u8]) -> Result<InstanceId, LocalError> {
    let invalid = || LocalError::HostIdentity {
        path: path.to_path_buf(),
    };
    if bytes.len() != IDENTITY_BYTES * 2
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(invalid());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
    InstanceId::try_new(text).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Barrier;

    use super::*;
    use crate::layout::StateLayout;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn concurrent_creators_and_restarts_keep_one_private_identity() -> TestResult {
        let root = tempfile::tempdir()?;
        let layout = StateLayout::new(root.path().join("state"));
        layout.prepare()?;
        let path = layout.controller_host_path(Path::new("/installation/gang.toml"));
        let barrier = Barrier::new(2);
        let (first, second) = std::thread::scope(|scope| {
            let create = || {
                barrier.wait();
                load_or_create(&path)
            };
            let first = scope.spawn(create);
            let second = scope.spawn(create);
            (first.join(), second.join())
        });
        let first = first.map_err(|_| "identity creator panicked")??;
        let second = second.map_err(|_| "identity creator panicked")??;
        assert_eq!(first, second);
        assert_eq!(load_or_create(&path)?, first);
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);
        assert_eq!(
            fs::metadata(layout.controller_hosts_dir())?
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(fs::read_dir(layout.controller_hosts_dir())?.count(), 1);
        Ok(())
    }

    #[test]
    fn corrupt_identity_is_rejected_without_replacement() -> TestResult {
        let root = tempfile::tempdir()?;
        let layout = StateLayout::new(root.path().join("state"));
        layout.prepare()?;
        let path = layout.controller_host_path(Path::new("/installation/gang.toml"));
        ensure_private_dir(&layout.controller_hosts_dir())?;
        for invalid in [
            "",
            "not-an-id",
            "0123456789abcdef0123456789abcdeF",
            "0123456789abcdef0123456789abcdef\n",
        ] {
            fs::write(&path, invalid)?;
            assert!(matches!(
                load_or_create(&path),
                Err(LocalError::HostIdentity { .. })
            ));
            assert_eq!(fs::read(&path)?, invalid.as_bytes());
        }
        Ok(())
    }
}
