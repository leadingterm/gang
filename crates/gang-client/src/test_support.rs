//! Fixtures shared by the crate's unit tests: file modes, a client home in a temporary
//! directory, and a Gang CA pin.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use gang_types::CaPin;
use tempfile::TempDir;

use crate::home::ClientHome;

/// The permission bits of `path`, without the file type.
pub(crate) fn mode(path: &Path) -> u32 {
    std::fs::metadata(path)
        .expect("file exists")
        .permissions()
        .mode()
        & 0o777
}

/// A client home below `tmp`, in a directory that does not exist yet.
pub(crate) fn home_in(tmp: &TempDir) -> ClientHome {
    ClientHome::try_new(tmp.path().join("nested").join("client")).expect("absolute temp path")
}

/// A CA pin of 64 zero digits.
pub(crate) fn ca_pin() -> CaPin {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("valid ca pin")
}
