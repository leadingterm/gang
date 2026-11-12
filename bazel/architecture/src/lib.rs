//! Checks workspace structure against the architecture plan.

mod architecture;

#[cfg(test)]
mod tests;

pub use architecture::{Error, Plan, Violation, Workspace, check};
