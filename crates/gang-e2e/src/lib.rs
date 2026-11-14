//! End-to-end harness for Gang: drives the real `gang` binary and real Bazel against a Local Mode
//! deployment and judges what it observes.
//!
//! The harness is a test-only consumer of the binary. The `local` binary runs it:
//! `bazel run //crates/gang-e2e:local -- --gate 1`. A scenario sees the deployment
//! through the [`System`] and [`Identity`] traits, so the same scenario code runs against the real
//! [`Stack`] and against a simulation in this crate's tests. Verdicts are pure functions in
//! [`verdict`].
//!
//! ```
//! use gang_e2e::{Check, Report, names};
//!
//! let mut report = Report::default();
//! report.extend([Check::passed(names::lifecycle::STARTS_AT_ZERO)]);
//! assert!(report.is_success());
//! ```

mod access;
mod access_secured;
mod access_verdict;
mod administrator;
mod admission;
mod bazel;
mod browser;
mod browser_secured;
mod browser_verdict;
mod build_output;
mod check;
mod checked_in;
mod chromium;
mod cli;
mod cli_terminal;
mod control;
mod dashboard;
mod device_verdict;
mod disk;
mod drain;
mod drain_cancel;
mod errors;
mod event_log;
mod execution;
mod fake_bazel;
mod fhs_tools;
mod fifo;
mod fleets;
mod gang_cli;
mod glibc_toolchain;
mod h2_frames;
mod hold;
mod host;
mod http;
mod identity;
mod input_root;
mod integrity;
mod issuer;
mod journal;
mod lifecycle;
mod limits;
mod memory;
mod mirror;
mod network;
mod observer;
mod oom;
mod orphan;
mod paths;
mod placement;
mod plaintext;
mod ports;
mod preemption;
mod report;
mod restart;
mod revocation;
mod sandbox_contract;
mod secured;
mod server;
mod server_scenarios;
mod server_stack;
mod session;
mod settings;
mod signer_key;
mod skipped_pass;
mod sse;
mod stack;
mod stale_scope;
mod state_api;
mod suite;
mod system;
mod template;
mod timeout;
mod tls_client;

pub mod flags;
pub mod names;
pub mod process;
pub mod sample;
pub mod server_verdict;
pub mod verdict;

pub use access::{
    Access, Administration, Aging, DeviceLogin, IdentityKnobs, Login, MEMBER_SUBJECT,
    MisdirectedLogin, Misdirection, PROVER_SUBJECT, REVOKED_TEXT, SIGN_IN_SUBJECT, Sessions,
    SignIn, StandingWorker, StoredSession, TokenAnswer, Validity, WorkerIdentity, identity,
};
pub use access_secured::proof_command;
pub use access_verdict::{
    CookieValue, ExpectedCookie, MaxAge, SetCookie, Verdict, authorization_request, cookie_set,
    failure_reported, has_line, login_cookie_name, query_value, redirected_to, set_cookies,
    status_is,
};
pub use administrator::{ADMINISTRATOR, Administrator};
pub use admission::{cold_start_misfit, memory_escalation, oversized_fleet, unplaceable_disk};
pub use bazel::Bazel;
pub use browser::{BROWSER_SUBJECT, Browser, SESSION_MAX_AGE, SESSION_TOKEN_TTL, sessions};
pub use browser_verdict::{
    RENEWAL_COOKIE_MAX_AGE, authorized, consent_free, cross_site_renewal_refused,
    offline_requested, rechecked, refused_by_provider, renewal_cleared, renewal_cookie,
    renewal_kept, renewed, sealed_refused, shared_grant, silent_renewals,
};
pub use build_output::{BuildOutcome, ProcessSummary};
pub use check::{Check, Outcome, Report};
pub use checked_in::{Shipped, ShippedConfig, Start, checked_in_configs};
pub use chromium::{Chromium, Navigation};
pub use cli::{
    Call, Colour, Local, Refused, RowStatus, StatusRow, Terminal, cli, plain, status_rows,
};
pub use control::{Control, ControlKnobs};
pub use dashboard::dashboard;
pub use device_verdict::{
    DevicePrompt, browser_approved, cross_site_approval_refused, device_approved, device_only,
    device_prompt, device_resource, device_session, logged_in_with, refused_with,
    renewed_by_web_client, rotation_stored, stored_nothing,
};
pub use disk::disk;
pub use drain_cancel::drain_cancel;
pub use errors::{E2eError, Result};
pub use event_log::EventFollower;
pub use execution::{
    ACTIONS_DIR, ActionTimeout, COLD_START_MISFIT_MESSAGE, ColdStart, ESCALATION_MAX_STEPS,
    Execution, ExecutionKnobs, FleetDisk, Generations, JOURNAL_END, MemoryEscalation,
    NO_FLEET_ACCEPTS, Proc, REFUSAL_RELAUNCH_BACKOFF, Role, Run, ServerExit, WORKER_LEAF,
    WorkerCertificates, held_build, renewal_deadline, scope_of,
};
pub use fake_bazel::{FAKE_BAZEL_ENV, command_line_event, fake_bazel, fake_bazel_requested};
pub use fhs_tools::fhs_tools;
pub use fleets::fleets;
pub use gang_cli::GangCli;
pub use gang_config::InputLinks;
pub use glibc_toolchain::glibc_toolchain;
pub use h2_frames::H2Channel;
pub use hold::{SIGNAL_COMMAND, Signal, Signalled};
pub use host::Host;
pub use identity::{
    Admission, Channel, ChannelEvent, Credentials, Identity, Issuance, Presenting, Reply,
};
pub use input_root::input_root;
pub use integrity::input_integrity;
pub use issuer::{
    AUDIENCE, AuthorizationRecord, CONSENT_PROMPT, DASHBOARD_CLIENT_ID, DASHBOARD_CLIENT_SECRET,
    OFFLINE_ACCESS, ORGANIZATION, ORGANIZATION_CLAIM, RefreshOutcome, RefreshRecord, SubjectPolicy,
    TestIssuer, TokenSpec,
};
pub use lifecycle::lifecycle;
pub use limits::limits;
pub use memory::memory;
pub use mirror::{Mirror, follow};
pub use network::network;
pub use oom::oom;
pub use orphan::orphan;
pub use paths::anchored;
pub use placement::Placement;
pub use plaintext::plaintext;
pub use preemption::{preemption, worker_renewal};
pub use process::Ran;
pub use report::{
    DiskNeighbourReport, InputRootReport, IsolationReport, Namespaces, PoisonReport, ProbeReport,
    VictimReport,
};
pub use restart::restart;
pub use revocation::revocation;
pub use sandbox_contract::sandbox_contract;
pub use secured::{Secured, bearer_of};
pub use server::{
    Answer, Body, CERTIFICATE_BACKDATE, CERTIFICATE_TTL, CertificateLifetime, Exit, Handshake,
    Leaf, MAX_REQUEST_SIZE, Presented, RATE_LIMIT_BURST, RATE_LIMIT_REFILL, REISSUE_AT_PERCENT,
    RawStream, Request, RequestLimits, ServerApi, ServerKnobs, TRUSTED_PROXIES, VIEWER_RETRY_AFTER,
    ViewerLimit,
};
pub use server_scenarios::{api, certificate_rotation, event_paging, request_limits, viewer_cap};
pub use server_stack::ServerStream;
pub use session::{SessionCookie, TokenUsage, cookie_name, session_cookie, signed_token};
pub use settings::{ActionTools, GLIBC_LOADER_PATHS, Settings};
pub use skipped_pass::skipped_pass;
pub use sse::EventStream;
pub use stack::Stack;
pub use stale_scope::stale_scope;
pub use state_api::{StateApi, StateStream};
pub use suite::{Gate, Invocation, Scenario, UnknownGate, run, unknown_scenarios};
pub use system::{
    ActionLimits, FleetLayout, HOLD_CPU_COUNT, HOLD_MEMORY_KB, LARGE_FLEET,
    LIMIT_CPU_PERIOD_MICROS, LIMIT_HEADROOM_PERCENT, LIMIT_MAX_PIDS, Limits, NOOP_FLEET,
    PermitNetwork, Probe, SMALL_FLEET, Sandbox, ScopeFacts, SoftMemory, System, Variant,
};
pub use template::{ConfigTemplate, Transport};
pub use timeout::{action_timeout, exit_signals};
