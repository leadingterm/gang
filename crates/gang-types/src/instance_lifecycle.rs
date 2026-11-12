//! Provisioner-reported instance lifecycle stages.

/// The stages of an instance's life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceLifecycle {
    /// Requested, not running yet (a queued job, an unschedulable or image-pulling pod, an
    /// instance launching). Never an orphan, however long it waits; counts as capacity.
    Pending,
    /// Running and eligible to take work; `boot_grace` is measured from its launch time.
    InService,
    /// Ending, finished (an exited single-use pod, a completed job) or being removed. Not
    /// capacity; left alone.
    Terminating,
}
