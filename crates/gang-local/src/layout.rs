use std::path::PathBuf;

use gang_os::fs::{ensure_private_dir, require_private_dir};
use gang_types::{FleetName, InstanceId};

use crate::errors::LocalError;

/// Name of the directory holding fleet bookkeeping under the state root.
const FLEETS_DIR: &str = "fleets";

/// Name of the directory holding per-instance worker logs under the state root.
const LOGS_DIR: &str = "logs";

/// Every on-disk location the local provisioner uses, all derived from one root directory.
#[derive(Debug, Clone)]
pub(crate) struct StateLayout {
    root: PathBuf,
}

impl StateLayout {
    /// Roots every state path under `root`, the configured `state_dir`.
    #[must_use]
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Parent of every per-instance directory.
    #[must_use]
    pub(crate) fn instances_dir(&self) -> PathBuf {
        self.root.join("instances")
    }

    /// Directory holding fleet bookkeeping: desired capacity, early-exit times, and the counter.
    #[must_use]
    pub(crate) fn fleets_dir(&self) -> PathBuf {
        self.root.join(FLEETS_DIR)
    }

    /// Directory holding per-instance worker logs.
    #[must_use]
    pub(crate) fn logs_dir(&self) -> PathBuf {
        self.root.join(LOGS_DIR)
    }

    /// Directory holding everything about one instance, scratch included.
    #[must_use]
    pub(crate) fn instance_dir(&self, instance: &InstanceId) -> PathBuf {
        self.instances_dir().join(instance.as_str())
    }

    /// The instance record; its presence is what makes a directory an instance.
    #[must_use]
    pub(crate) fn record_path(&self, instance: &InstanceId) -> PathBuf {
        self.instance_dir(instance).join("instance.json")
    }

    /// The simulated Spot interruption notice for one instance, when one was requested.
    #[must_use]
    pub(crate) fn preemption_path(&self, instance: &InstanceId) -> PathBuf {
        self.instance_dir(instance).join("preemption.json")
    }

    /// Scratch volume backing directory for one instance; removed with the instance.
    #[must_use]
    pub(crate) fn scratch_dir(&self, instance: &InstanceId) -> PathBuf {
        self.instance_dir(instance).join("scratch")
    }

    /// Combined stdout and stderr of the instance's worker process. Kept under `logs/`, outside the
    /// instance directory, so removing the instance keeps its log.
    #[must_use]
    pub(crate) fn log_path(&self, instance: &InstanceId) -> PathBuf {
        self.logs_dir().join(format!("{}.log", instance.as_str()))
    }

    /// Exclusive lock file whose holding marks the instance alive.
    #[must_use]
    pub(crate) fn lock_path(&self, instance: &InstanceId) -> PathBuf {
        self.instance_dir(instance).join("alive.lock")
    }

    /// The cgroup the instance's worker recorded once it was identified: the scope its launcher
    /// created.
    #[must_use]
    pub(crate) fn worker_cgroup_path(&self, instance: &InstanceId) -> PathBuf {
        self.instance_dir(instance).join("cgroup")
    }

    /// Persisted desired capacity for one fleet, stored as decimal text.
    #[must_use]
    pub(crate) fn desired_path(&self, fleet: &FleetName) -> PathBuf {
        self.fleets_dir()
            .join(format!("{}.desired", fleet.as_str()))
    }

    /// Persisted time of the latest early exit of one fleet, as decimal Unix milliseconds.
    #[must_use]
    pub(crate) fn early_exit_path(&self, fleet: &FleetName) -> PathBuf {
        self.fleets_dir()
            .join(format!("{}.early_exit", fleet.as_str()))
    }

    /// Persisted counter from which new instance numbers are taken.
    #[must_use]
    pub(crate) fn counter_path(&self) -> PathBuf {
        self.fleets_dir().join("instances.next")
    }

    /// Directory of the workload signing key. Created on first use of the mock IMDS.
    #[must_use]
    pub(crate) fn imds_dir(&self) -> PathBuf {
        self.root.join("imds")
    }

    /// PKCS#8 private key that signs workload JWTs and is published through JWKS.
    #[must_use]
    pub(crate) fn jwt_key_path(&self) -> PathBuf {
        self.imds_dir().join("jwt.key")
    }

    /// Creates the directories that must exist before any other operation runs: the root and the
    /// instance, fleet, and log directories beneath it.
    ///
    /// The root is the user's `state_dir`: it is created `0700` when missing and refused when it
    /// exists but is not private, because tightening a directory the user named would change
    /// permissions they set. The instance, fleet, and log directories Gang owns beneath it are
    /// created or tightened to `0700` by [`ensure_private_dir`]: a state directory owned by
    /// another user would let that user plant records.
    pub(crate) fn prepare(&self) -> Result<(), LocalError> {
        require_private_dir(&self.root)?;
        for directory in [self.instances_dir(), self.fleets_dir(), self.logs_dir()] {
            ensure_private_dir(&directory)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn paths_nest_under_the_root_and_instance_paths_contain_the_id() -> TestResult {
        let root = PathBuf::from("/state");
        let layout = StateLayout::new(root.clone());
        let instance = InstanceId::try_new("local-7")?;

        assert_eq!(layout.instances_dir(), PathBuf::from("/state/instances"));
        assert_eq!(layout.fleets_dir(), PathBuf::from("/state/fleets"));
        assert_eq!(layout.logs_dir(), PathBuf::from("/state/logs"));
        assert_eq!(
            layout.instance_dir(&instance),
            PathBuf::from("/state/instances/local-7")
        );
        assert_eq!(
            layout.record_path(&instance),
            PathBuf::from("/state/instances/local-7/instance.json")
        );
        assert_eq!(
            layout.preemption_path(&instance),
            PathBuf::from("/state/instances/local-7/preemption.json")
        );
        assert_eq!(
            layout.scratch_dir(&instance),
            PathBuf::from("/state/instances/local-7/scratch")
        );
        assert_eq!(
            layout.log_path(&instance),
            PathBuf::from("/state/logs/local-7.log")
        );
        assert_eq!(
            layout.lock_path(&instance),
            PathBuf::from("/state/instances/local-7/alive.lock")
        );
        assert_eq!(
            layout.worker_cgroup_path(&instance),
            PathBuf::from("/state/instances/local-7/cgroup")
        );

        let fleet = FleetName::try_new("gang-workers")?;
        assert_eq!(
            layout.desired_path(&fleet),
            PathBuf::from("/state/fleets/gang-workers.desired")
        );
        assert_eq!(
            layout.early_exit_path(&fleet),
            PathBuf::from("/state/fleets/gang-workers.early_exit")
        );

        for path in [
            layout.instances_dir(),
            layout.record_path(&instance),
            layout.desired_path(&fleet),
            layout.early_exit_path(&fleet),
            layout.counter_path(),
            layout.jwt_key_path(),
        ] {
            assert!(path.starts_with(&root), "{path:?} escapes the root");
        }
        Ok(())
    }

    #[test]
    fn prepare_creates_the_shared_directories() -> TestResult {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("state");
        let layout = StateLayout::new(root.clone());

        layout.prepare()?;

        assert!(layout.instances_dir().is_dir());
        assert!(layout.fleets_dir().is_dir());
        assert!(layout.logs_dir().is_dir());
        layout.prepare()?;
        Ok(())
    }

    #[test]
    fn prepare_creates_owner_only_directories() -> TestResult {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("state");
        let layout = StateLayout::new(root.clone());

        layout.prepare()?;

        for path in [
            root.clone(),
            layout.instances_dir(),
            layout.logs_dir(),
            layout.fleets_dir(),
        ] {
            let mode = fs::metadata(&path)?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{path:?}");
        }
        Ok(())
    }

    #[test]
    fn prepare_refuses_a_root_other_users_can_open_and_leaves_it_unchanged() -> TestResult {
        let dir = tempfile::tempdir()?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755))?;
        let layout = StateLayout::new(dir.path().to_path_buf());

        let error = layout
            .prepare()
            .err()
            .ok_or("prepare accepted a 0755 root")?;

        assert!(matches!(
            error,
            LocalError::PrivateDir(gang_os::fs::PrivateDirError::NotPrivate { mode: 0o755, .. })
        ));
        let mode = fs::metadata(dir.path())?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
        Ok(())
    }

    #[test]
    fn prepare_accepts_a_private_root_owned_by_the_current_user() -> TestResult {
        let dir = tempfile::tempdir()?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        let layout = StateLayout::new(dir.path().to_path_buf());

        layout.prepare()?;
        Ok(())
    }
}
