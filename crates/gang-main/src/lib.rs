//! Composition root of Gang: the single `gang` binary.
//!
//! Laptops, CI, the controller, and workers all run the same artifact, so
//! client and server versions cannot drift, and Local Mode starts further
//! copies of itself. The commands are:
//!
//! - `gang login`, `gang revoke`, `gang restore`, `gang get`, and `gang bazelrc`: client identity,
//!   revocation, and Bazel integration (run on laptops and CI).
//! - `gang server --config <FILE>`: the control loop, certificate endpoint,
//!   and listeners of a controller.
//! - `gang worker --config <FILE>`: the worker agent on worker nodes.
//! - `gang exec <COMMAND>...`: hidden; the `NativeLink` worker entrypoint,
//!   reached by argv0 multicall through the `gang-exec` link that
//!   `gang worker` creates to this binary. It runs one action in the
//!   configured sandbox.
//! - `gang local up`, `gang local down`: Local Mode fleet control on a
//!   workstation.
//! - `gang options [<PROVISIONER>]`: the `[fleets.options]` keys each provisioner accepts.
//! - `gang doctor [--server <url>] [-- <bazel args>]`: diagnose this machine's Bazel and login setup
//!   against a Gang server.
//! - `gang deploy <check>`: hidden; the decisions of the deploy workflow
//!   (`.github/workflows/deploy.yml`), each reading JSON on stdin; see `gang_deploy`.
//!
//! Multicall rule: `gang-exec` and `gang-join` pass their arguments through unparsed.
//! `gang-reset` clears the signal mask before executing its command; `gang-guard` watches
//! an action entrypoint and reaps its abandoned scopes. Other names use the CLI above.
//!
//! The command-line grammar is public as [`cli::Cli`] so tools that drive the binary can check the argv they build.
//!
//! The entry point is [`run`], which borrows the complete argument vector
//! (argv0 first) and returns the process exit code:
//!
//! ```
//! let _run: fn(&[std::ffi::OsString]) -> std::process::ExitCode = gang_main::run;
//! ```

mod bazel_command;
mod ca_pin_command;
#[cfg(test)]
mod checked_in_configs;
pub mod cli;
mod client_command;
mod config_file;
mod deploy_command;
mod dispatch;
mod doctor_command;
mod enroller;
mod errors;
mod exec_command;
mod guard_command;
mod help;
mod join_command;
mod local_down;
mod local_pin;
mod local_process;
mod local_record;
mod local_up;
mod logging;
mod login_providers;
mod look;
mod mark;
mod options_command;
mod output;
mod palette;
pub mod plugins;
mod progress;
mod record;
mod reset_command;
mod runtime;
mod server_command;
mod signer_pin;
mod terminal_console;
mod worker_command;
mod worker_server;
mod workspace;
mod wrap;

pub use dispatch::run;
