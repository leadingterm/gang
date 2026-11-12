use std::io::ErrorKind;
use std::path::Path;

use gang_os::fs::{PRIVATE_FILE, ensure_private_dir, read_optional, write_new_atomic};
use p256::ecdsa::SigningKey;
use p256::elliptic_curve::Generate;
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey};

use crate::errors::LocalError;

/// Loads the PKCS#8 P-256 key at `path`, generating and persisting a new one when the file is absent.
///
/// The new key is published atomically and never replaces an existing file, so two racing first
/// starts agree on one key: the loser re-reads the winner's file. The file is created owner-only.
pub(crate) fn load_or_create(path: &Path) -> Result<SigningKey, LocalError> {
    match read_optional(path)? {
        Some(der) => decode(path, &der),
        None => create(path),
    }
}

fn decode(path: &Path, der: &[u8]) -> Result<SigningKey, LocalError> {
    SigningKey::from_pkcs8_der(der).map_err(|_| LocalError::Key {
        path: path.to_path_buf(),
    })
}

fn create(path: &Path) -> Result<SigningKey, LocalError> {
    let key = SigningKey::generate();
    let der = key.to_pkcs8_der().map_err(|_| LocalError::Key {
        path: path.to_path_buf(),
    })?;

    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        ensure_private_dir(parent)?;
    }
    if write_new_atomic(path, der.as_bytes(), PRIVATE_FILE)? {
        return Ok(key);
    }

    let existing = read_optional(path)?.ok_or_else(|| {
        LocalError::Io(gang_os::PathIo::new(
            "read key file",
            path,
            ErrorKind::NotFound.into(),
        ))
    })?;
    decode(path, &existing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn first_call_creates_an_owner_only_key_file() -> TestResult {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("nested").join("jwt.key");

        load_or_create(&path)?;

        assert!(path.is_file());
        let mode = fs::metadata(&path)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        Ok(())
    }

    #[test]
    fn second_call_returns_the_same_key() -> TestResult {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("keys").join("jwt.key");

        let first = load_or_create(&path)?;
        let second = load_or_create(&path)?;

        assert_eq!(first.to_bytes(), second.to_bytes());
        Ok(())
    }

    #[test]
    fn garbage_key_file_is_a_key_error() -> TestResult {
        let dir = tempfile::tempdir()?;
        let keys = dir.path().join("keys");
        fs::create_dir(&keys)?;
        fs::set_permissions(&keys, fs::Permissions::from_mode(0o700))?;
        let path = keys.join("jwt.key");
        fs::write(&path, b"not a key")?;

        assert!(matches!(load_or_create(&path), Err(LocalError::Key { .. })));
        Ok(())
    }

    #[test]
    fn concurrent_creators_agree_on_one_key() -> TestResult {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("keys").join("jwt.key");

        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| load_or_create(&path));
            let second = scope.spawn(|| load_or_create(&path));
            (first.join(), second.join())
        });

        let first = first.map_err(|_| "a creator thread panicked")??;
        let second = second.map_err(|_| "a creator thread panicked")??;
        assert_eq!(first.to_bytes(), second.to_bytes());
        Ok(())
    }
}
