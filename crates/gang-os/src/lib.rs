//! Unix-only kernel-ring OS layer: cgroup v2 trees, private files and directories, atomic writes,
//! filesystem space, advisory file locks, bounded child runs, process groups, parent exits, logged
//! spawning and shutdown signals.
//!
//! ```
//! use gang_os::fs::{PRIVATE_FILE, ensure_private_dir, read_optional, write_atomic};
//!
//! let dir = tempfile::tempdir()?;
//! let state = dir.path().join("state");
//! ensure_private_dir(&state)?;
//! let file = state.join("token");
//! assert_eq!(read_optional(&file)?, None);
//! write_atomic(&file, b"secret", PRIVATE_FILE)?;
//! assert_eq!(read_optional(&file)?.as_deref(), Some(&b"secret"[..]));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod cgroup;
mod errors;
pub mod fs;
mod lock;
mod process;
mod signal;
mod sys;

pub use errors::{PathIo, RunError, SpawnError};
pub use lock::FileLock;
pub use process::{
    ParentExit, ProcessGroup, RunOutput, join_parent_group, parent_process_group, run_bounded,
    spawn_logged, terminate_process,
};
pub use signal::{TerminationsBlocked, block_terminations, clear_mask, shutdown_signal};
