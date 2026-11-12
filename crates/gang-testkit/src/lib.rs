//! Test-support kit shared by the workspace's test crates.
//!
//! This crate is test support: depend on it only from `[dev-dependencies]`. Production code never depends on it.
//!
//! The kit serves the workspace's tests and the end-to-end harness.
//!
//! The kit provides:
//!
//! - [`fork_gate`]: the process-wide gate between lifetime-lock users and child spawns.
//! - [`ChildTest`]: a test of the running binary that another test re-executes in a child process and requires to complete.
//! - [`TestResult`]: the result type test functions return.
//! - [`assert_sources_not_repeated`]: the error-message check for `every_variant()` tests.
//! - [`park_then_signal`]: a wrapper that signals once its future first parks, so tests can order wake-ups.
//! - [`ready_now`]: polls a future once, for tests asserting that an operation completes without waiting.
//! - [`must_ready`]: polls once and asserts that a future completes without waiting.
//! - [`FakeCache`]: a cache whose stores are in memory.
//! - [`TestSigner`], [`FailingSigner`], [`WrongKeySigner`], [`GarbageSigner`]: deterministic certificate signers.
//! - [`TestAuthority`] and [`TestLeaf`]: a throwaway certificate authority that issues TLS test certificates.
//! - [`FakeRepository`]: an in-memory state repository with failure injection.
//! - [`FakeProvisioner`]: a configurable fleet and worker-side provisioner.
//! - [`FakeScheduler`] and its [`Controller`], [`Behaviour`] and [`CallCount`] helpers.
//! - [`TestPlugins`], [`OpaquePlugins`] and [`STANDARD_GANG_TOML`]: plugin sets and a standard configuration.
//! - [`fleet_policy`], [`fleet_of`] and [`fleet`]: sample fleet builders.
//! - [`worker_on`] and [`worker_id`]: deterministic worker identifiers for an instance.
//! - [`generate_es256_key`]: generate the signing key and JWK that test issuers publish.
//! - [`signer_contract`]: checks the shared certificate signer boundary.
//!
//! ```
//! # use gang_testkit::{TestResult, fleet_policy};
//! # fn main() -> TestResult {
//! let policy = fleet_policy("workers")?;
//! assert_eq!(policy.instances.max().get(), 4);
//! # Ok(())
//! # }
//! ```

mod builders;
mod cache;
mod call_log;
mod child_test;
mod errors;
pub mod fork_gate;
mod jwt;
mod limits;
mod park;
mod plugins;
mod provisioner;
mod repository;
/// The contract suite that every `StateRepositoryPort` implementation must pass.
pub mod repository_contract;
mod sandbox;
mod scheduler;
mod signer;
/// The contract suite that every `CertificateSignerPort` implementation must pass.
pub mod signer_contract;
mod test_result;
mod tls_authority;
mod worker;

pub use builders::{fleet, fleet_of, fleet_policy};
pub use cache::FakeCache;
pub use call_log::CallLog;
pub use child_test::ChildTest;
pub use errors::assert_sources_not_repeated;
pub use jwt::generate_es256_key;
pub use limits::{LimitsStep, RecordedScope, RecordingLimits};
pub use park::{must_ready, park_then_signal, ready_now};
pub use plugins::{
    BoxedSection, Opaque, OpaquePlugins, STANDARD_GANG_TOML, SandboxConfig, SandboxKey, TestLimits,
    TestPluginSet, TestPlugins,
};
pub use provisioner::FakeProvisioner;
pub use repository::FakeRepository;
pub use sandbox::{ScriptedSandbox, StartedAction};
pub use scheduler::{
    Behaviour, CallCount, Controller, FakeScheduler, Received, RecordedSpec, RecordedTls, Side,
};
pub use signer::{
    AUTHORITY_SEED, FailingSigner, GarbageSigner, TestSigner, WrongKeySigner, signing_key,
};
pub use test_result::TestResult;
pub use tls_authority::{
    ExtendedKeyUsagePurpose, TestAuthority, TestLeaf, TlsFixtureError, Window,
};
pub use worker::{worker_id, worker_on};
