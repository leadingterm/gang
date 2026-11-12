//! The `gang exec` action entrypoint: runs one action inside the sandbox its worker configured.
//!
//! `gang exec` is invoked through argv0 multicalling; `gang-main` passes the process invocation
//! to [`run`]. The steps, in order:
//!
//! 1. Read the sandbox file beside the `gang-exec` entrypoint link. The worker writes it there,
//!    and an action cannot write to that directory.
//! 2. Read the network, CPU and memory requests from the environment.
//! 3. If the file breaks input links, derive the action directory from the work directory the
//!    file records and the current directory.
//! 4. If the file has limits, build the limits plugin and start a scope guardian.
//! 5. If the file breaks input links, replace every hard-linked regular file under the action
//!    directory with a private copy.
//! 6. Run the action inside its limits scope and sandbox; its outcome becomes the exit status.
//!
//! ```
//! use gang_spi::ActionExecutionOutcome;
//!
//! // A process killed by signal 9 exits with 128 + 9.
//! assert_eq!(gang_exec::exit_status(ActionExecutionOutcome::Signaled(9)), 137);
//! ```

mod action_directory;
mod errors;
mod execute;
mod guard;
mod input_links;
mod private_copy;
mod properties;
mod run;
mod terminations;

pub use action_directory::action_directory;
pub use errors::{ExecError, LinkBreakError, PropertyError};
pub use execute::{Action, execute, exit_status};
pub use guard::guard;
pub use input_links::break_hard_links;
pub use properties::ActionProperties;
pub use run::{Invocation, read_sandbox_file, run, run_with};
