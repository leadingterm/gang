//! Scratch volumes: the per-instance directory a worker builds actions in. Its capacity is
//! the free space of the filesystem that holds it.

use gang_os::fs::{ensure_private_dir, space};
use gang_spi::MountedScratchVolume;
use gang_types::InstanceId;

use crate::errors::LocalError;
use crate::layout::StateLayout;

/// Creates the scratch directory of `instance` if needed and reports it with the free space
/// of its filesystem; mounting again returns the same directory.
pub(crate) fn mount(
    layout: &StateLayout,
    instance: &InstanceId,
) -> Result<MountedScratchVolume, LocalError> {
    let path = layout.scratch_dir(instance);
    ensure_private_dir(&path)?;
    let capacity = space(&path)?.available;
    Ok(MountedScratchVolume { path, capacity })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mounting_twice_returns_the_same_path() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let layout = StateLayout::new(dir.path().join("state"));
        let instance = InstanceId::try_new("local-1")?;

        let first = mount(&layout, &instance)?;
        let second = mount(&layout, &instance)?;

        assert_eq!(first.path, second.path);
        assert_eq!(first.path, layout.scratch_dir(&instance));
        assert!(first.path.is_dir());
        assert!(first.capacity.get() > 0);
        Ok(())
    }
}
