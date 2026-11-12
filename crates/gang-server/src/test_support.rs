//! Server-specific fixtures shared by the `gang-server` behavioural tests.
//!
//! The generic fakes (repository, scheduler, cache, signer, plugins) come from `gang-testkit`;
//! this module holds only the fixtures that exist for the server alone.
//!
//! Every fixture keeps its state in an `Arc<Mutex<..>>`, so a test keeps a handle after it moves a
//! clone into the code under test.

pub use crate::test_support_api::{
    ApiReply, admits, api_request, decode_sse, identity, mint_token, prepare_plain,
    raw_api_request, read, request_parts, serve,
};
pub use crate::test_support_clock::{at as clock_at, start as start_clock};
pub use crate::test_support_crypto::{
    authority, client_certificate, signing_request, tls_client, tls_client_over,
};
pub use crate::test_support_deployment::{
    PlainDeployment, TlsDeployment, endpoint, plain_parts, plain_settings, providers, role_for,
    tls_parts, tls_settings,
};
pub use crate::test_support_fleet::{fleets, fleets_of, queued_demand};
pub use crate::test_support_memory::{MemoryDialer, MemoryListener, memory_listener};
pub use crate::test_support_session::{SESSION_TTL, cookie_parts, session_token};
pub use crate::test_support_session_harness::{
    Harness, HarnessOptions, request, request_with_header,
};
pub use crate::test_support_verifier::{BeginFailure, FakeVerifier};

/// An instant in milliseconds since the Unix epoch.
pub fn at(millis: u64) -> gang_clock::Timestamp {
    gang_clock::Timestamp::from_unix_millis(millis)
}
