//! Scratch: the fast local volume a worker builds actions on.

use std::path::PathBuf;

use gang_types::{Bytes, InstanceId};

use crate::{PortError, port};

/// Local scratch storage.
#[port]
pub trait ScratchVolumePort: Send + Sync {
    /// Prepares the scratch volume for the worker on `instance` and returns it. A warm volume
    /// that already holds data reports only the space still free. Repeating the call for the
    /// same instance returns the same volume.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the volume cannot be prepared.
    async fn mount(&self, instance: &InstanceId) -> Result<MountedScratchVolume, PortError>;
}

/// A scratch volume ready for use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountedScratchVolume {
    /// The directory actions may use.
    pub path: PathBuf,
    /// Bytes available to the worker's unprivileged user on the volume at mount time (the
    /// filesystem's free space, not its total size). The worker sizes the fast content store
    /// and the action work directories from it.
    pub capacity: Bytes,
}
