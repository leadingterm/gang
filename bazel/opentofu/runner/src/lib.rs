//! Runs one `tofu` contract: `tofu fmt`, `init`, `validate` and `test` for one root.
//!
//! The runner stages a writable copy of the `infra/` tree (and `configs/` when present) in a work
//! directory, because the runfiles tree is read-only and `tofu init` writes into the root. It then
//! runs the four steps in order inside the staged root. [`Contract::run`] is the entry point;
//! `main.rs` builds the contract from the environment that the Bazel rule sets.

mod contract;
mod errors;

pub use contract::{Contract, Step};
pub use errors::RunError;
