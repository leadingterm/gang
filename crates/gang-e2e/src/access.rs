//! The identity scenario: who may administer the deployment, how the `gang` client logs in and
//! renews its tokens, which credentials the server refuses, how the dashboard signs a browser in,
//! how a revoked worker certificate is cut off, and that a revocation survives a restart.
//!
//! The scenario sees the deployment only through [`Access`]; [`crate::access_secured`] drives a
//! real [`crate::Secured`] deployment with it.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use gang_types::{CertSerial, Url};
use gang_wire::{
    CALLBACK_PATH, DEVICE_PATH, ERROR_PATH, ErrorReason, LOGIN_PATH, LOGOUT_PATH, Permission,
};
use http::Method;

use crate::Ran;
use crate::access_verdict::{
    CookieValue, ExpectedCookie, MaxAge, Verdict, authorization_request, cookie_set,
    failure_reported, has_line, login_cookie_name, query_value, redirected_to, set_cookies,
    status_is,
};
use crate::browser_verdict::{authorized, offline_requested};
use crate::check::Check;
use crate::chromium::Navigation;
use crate::device_verdict::{
    DevicePrompt, cross_site_approval_refused, device_approved, device_only, device_resource,
    device_session, logged_in_with, refused_with, renewed_by_web_client, rotation_stored,
    stored_nothing,
};
use crate::errors::{E2eError, Result};
use crate::identity::{Admission, Channel, ChannelEvent, Credentials};
use crate::issuer::{AuthorizationRecord, DASHBOARD_CLIENT_ID, RefreshRecord, SubjectPolicy};
use crate::names::identity;
use crate::server::{Answer, Presented, Request};
use crate::session::{SessionCookie, cookie_name};

/// The subject the identity provider signs in interactively: the `login_hint` the deployment's
/// login configuration sends, for the `gang login` browser flow and the dashboard sign-in alike.
pub const SIGN_IN_SUBJECT: &str = "operator";

/// The subject of the principal that holds no role: it is refused every administrative action,
/// and its revocation is what `gang doctor` and the restart observe.
pub const MEMBER_SUBJECT: &str = "member";

/// The subject of the workload that logs in with a proof command.
pub const PROVER_SUBJECT: &str = "prover";

/// Whether the deployment offers interactive sign-in: the test provider's `login` section, the
/// dashboard client among its audiences, and `[dashboard.login]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignIn {
    /// The base configuration: workload logins only.
    Absent,
    /// `gang login` and the dashboard sign browsers in through the test provider, as
    /// [`SIGN_IN_SUBJECT`].
    Offered,
}

/// Whether the first fleet keeps a worker running with no build: `min_instances = 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandingWorker {
    /// The base configuration: workers run only while builds need them.
    Absent,
    /// One worker stays connected, so its certificate can be revoked while its stream is open.
    One,
}

/// How long the deployment's browser sessions and tokens live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sessions {
    /// The configured defaults.
    Default,
    /// Short lifetimes for the sessions scenario: `token_ttl = "1m"`, `clock_skew = "0s"` and
    /// `[dashboard] session_max_age = "3m"`; browsers sign in as [`crate::BROWSER_SUBJECT`], which
    /// holds a role granting `state.read`.
    Short,
}

/// The configuration knobs of the identity family: the `identity` field of [`crate::Variant`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityKnobs {
    /// Whether interactive sign-in is configured.
    pub sign_in: SignIn,
    /// Whether a worker runs without a build.
    pub standing_worker: StandingWorker,
    /// The lifetimes of sessions and tokens.
    pub sessions: Sessions,
}

impl IdentityKnobs {
    /// Every knob absent: the base configuration.
    pub const NONE: Self = Self {
        sign_in: SignIn::Absent,
        standing_worker: StandingWorker::Absent,
        sessions: Sessions::Default,
    };

    /// The dashboard offers interactive sign-in, mounting the `/ui/login` route.
    pub const SIGN_IN: Self = Self {
        sign_in: SignIn::Offered,
        standing_worker: StandingWorker::Absent,
        sessions: Sessions::Default,
    };

    /// The configuration of the identity scenario.
    pub const IDENTITY: Self = Self {
        sign_in: SignIn::Offered,
        standing_worker: StandingWorker::One,
        sessions: Sessions::Default,
    };

    /// The configuration of the sessions scenario: sign-in offered, short lifetimes.
    pub const SESSIONS: Self = Self {
        sign_in: SignIn::Offered,
        standing_worker: StandingWorker::Absent,
        sessions: Sessions::Short,
    };

    /// Every identity preset used by a scenario; configuration tests render each.
    pub const PRESETS: &[Self] = &[Self::SIGN_IN, Self::IDENTITY, Self::SESSIONS];
}

/// A `gang login` that points the client somewhere it must not log in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Misdirection {
    /// The real endpoint, with a `--ca-pin` that is not the server's.
    WrongPin,
    /// The real endpoint's host and port under `http://`, a scheme the client accepts.
    PlainScheme,
    /// The real endpoint's host and port under `ftp://`, a scheme the client refuses.
    UnsupportedScheme,
}

/// The end of a misdirected login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MisdirectedLogin {
    /// How `gang login` ended.
    pub outcome: Ran,
    /// Every regular file under the client home afterwards, relative to it.
    pub stored: Vec<PathBuf>,
}

/// An administrative `gang` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Administration {
    /// `gang revoke --principal <provider:principal>`.
    RevokePrincipal(String),
    /// `gang restore --principal <provider:principal>`.
    RestorePrincipal(String),
    /// `gang revoke --serial <hex>`, with a canonical certificate serial.
    RevokeSerial(CertSerial),
}

/// The validity window of a bearer token minted with the deployment's own signing key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validity {
    /// Issued now, valid for the token lifetime.
    Current,
    /// Expired an hour ago, far beyond any clock skew the server tolerates.
    Expired,
}

/// A client home whose `gang login` has run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Login {
    /// The client home.
    pub home: PathBuf,
    /// How `gang login` ended.
    pub outcome: Ran,
}

/// The session files a client home holds (absent ones are `None`/`false`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredSession {
    /// `kind` of `session.json`.
    pub kind: Option<String>,
    /// Whether `id-token` exists.
    pub id_token: bool,
    /// The trimmed `refresh-token`.
    pub refresh_token: Option<String>,
    /// When `refresh-token` was last written.
    pub refresh_written: Option<SystemTime>,
    /// When `token` was last written.
    pub token_written: Option<SystemTime>,
}

/// How a headless `gang login` ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceLogin {
    /// The client home.
    pub home: PathBuf,
    /// What it printed for the device login; `None` when it ended without a prompt.
    pub prompt: Option<DevicePrompt>,
    /// How it ended; `stderr` holds every line it printed.
    pub outcome: Ran,
}

/// What `gang get` answered for a client home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenAnswer {
    /// How `gang get` ended.
    pub outcome: Ran,
    /// The bearer token of its answer; `None` when it printed none.
    pub bearer: Option<String>,
}

/// What time is made to have passed for a client home's stored login.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aging {
    /// The stored bearer token has expired; the stored identity-provider proof is still fresh.
    Token,
    /// The stored bearer token and the stored identity-provider proof have both expired.
    TokenAndProof,
}

/// The certificate a running worker presents on the worker listener, and where its instance lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerIdentity {
    /// The canonical certificate serial.
    pub serial: CertSerial,
    /// The directory of the certificate, its private key and the CA that signed it.
    pub directory: PathBuf,
    /// The instance directory of the worker, which holds its liveness lock.
    pub instance: PathBuf,
}

/// What the identity scenario sees of a deployment whose clients log in with bearer tokens, whose
/// dashboard signs browsers in, and whose workers authenticate with certificates.
///
/// Every method blocks until its observation is complete. An error is a harness failure; what the
/// deployment answers, refusals included, is an `Ok` observation.
pub trait Access: Sync {
    /// The connection type of the worker listener.
    type Channel: Channel;

    /// The public endpoint of the running server.
    ///
    /// # Errors
    ///
    /// When no server has announced its endpoint.
    fn endpoint(&self) -> Result<Url>;

    /// The process id of the running server.
    fn server_pid(&self) -> Option<u32>;

    /// The administrator, logged in.
    ///
    /// # Errors
    ///
    /// When the administrator cannot log in.
    fn administrator(&self) -> Result<Credentials>;

    /// Logs a workload principal `subject` of the test provider in, in a new client home, with a
    /// token file.
    ///
    /// # Errors
    ///
    /// When the login cannot be run or the server refuses it.
    fn workload(&self, subject: &str) -> Result<Credentials>;

    /// Runs a misdirected `gang login` in a new client home.
    ///
    /// # Errors
    ///
    /// When the command cannot be run.
    fn misdirected_login(&self, misdirection: Misdirection) -> Result<MisdirectedLogin>;

    /// Runs `action` with the stored login of `who`.
    ///
    /// # Errors
    ///
    /// When the command cannot be run.
    fn administer(&self, who: &Credentials, action: &Administration) -> Result<Ran>;

    /// Sends `request` to the public endpoint; redirects are not followed.
    ///
    /// # Errors
    ///
    /// When the request cannot be sent.
    fn request(&self, request: &Request) -> Result<Answer>;

    /// Sends a `GET` of the absolute `url`, presenting nothing; redirects are not followed. The
    /// identity provider's authorization endpoint and the client's loopback callback are reached
    /// this way.
    ///
    /// # Errors
    ///
    /// When the request cannot be sent.
    fn visit(&self, url: &Url) -> Result<Answer>;

    /// Approves the device login `code` in a real headless browser: opens the approval page, types
    /// the code into its first input, presses Enter, clicks the button whose text contains "approve",
    /// and reports where the page went.
    ///
    /// # Errors
    ///
    /// When the browser cannot be started or driven.
    fn approve_in_browser(&self, code: &str) -> Result<Navigation>;

    /// Mints a bearer token for `principal` (`provider:principal`) with the deployment's own
    /// signing key, valid as `validity` says.
    ///
    /// # Errors
    ///
    /// When the key cannot be read or used.
    fn mint_bearer(&self, principal: &str, validity: Validity) -> Result<String>;

    /// Sends a GET with a freshly minted bearer token.
    ///
    /// # Errors
    ///
    /// When the token cannot be minted or the request cannot be sent.
    fn get_with_bearer(&self, path: &str, principal: &str, validity: Validity) -> Result<Answer> {
        let token = self.mint_bearer(principal, validity)?;
        self.request(&Request::get(path).presenting(Presented::Bearer(token)))
    }

    /// The dashboard session cookie of the principal of `who`, minted with the deployment's key.
    ///
    /// # Errors
    ///
    /// When the cookie cannot be minted.
    fn session_cookie(&self, who: &Credentials) -> Result<SessionCookie>;

    /// Logs the workload [`PROVER_SUBJECT`] in, in a new client home, with
    /// `gang login --workload --token-command`, whose command counts its runs.
    ///
    /// # Errors
    ///
    /// When the command cannot be run.
    fn command_login(&self) -> Result<Login>;

    /// How many times the proof command of the login in `home` has run.
    ///
    /// # Errors
    ///
    /// When the count cannot be read.
    fn proof_runs(&self, home: &Path) -> Result<usize>;

    /// Logs [`SIGN_IN_SUBJECT`] in, in a new client home, with the interactive `gang login`,
    /// completing its browser step by visiting the authorization URL it prints and the redirect
    /// the identity provider answers.
    ///
    /// # Errors
    ///
    /// When the command cannot be run or no authorization URL appears within the deadline.
    fn interactive_login(&self) -> Result<Login>;

    /// Runs a headless `gang login` without `--provider` in a new client home.
    /// Calls `approve` once a prompt appears, then waits for the command to end.
    ///
    /// # Errors
    ///
    /// When the command, approval, or observation cannot be completed.
    fn device_login(
        &self,
        approve: &mut dyn FnMut(&DevicePrompt) -> Result<()>,
    ) -> Result<DeviceLogin>;

    /// The session files in `home`.
    ///
    /// # Errors
    ///
    /// When the files cannot be read or parsed.
    fn session_files(&self, home: &Path) -> Result<StoredSession>;

    /// The provider's authorization requests for `subject`, oldest first.
    fn authorizations(&self, subject: &str) -> Vec<AuthorizationRecord>;

    /// The provider's refresh grants for `subject`, oldest first.
    fn refreshes(&self, subject: &str) -> Vec<RefreshRecord>;

    /// Sets how the provider treats `subject` from now on.
    fn set_policy(&self, subject: &str, policy: SubjectPolicy);

    /// The server's log so far, as text.
    ///
    /// # Errors
    ///
    /// When the server log cannot be read.
    fn server_log(&self) -> Result<String>;

    /// Runs `gang get` in `home` for the deployment's endpoint.
    ///
    /// # Errors
    ///
    /// When the command cannot be run.
    fn get(&self, home: &Path) -> Result<TokenAnswer>;

    /// Makes time pass for the stored login in `home`, as `aging` says.
    ///
    /// # Errors
    ///
    /// When the stored files cannot be read or written.
    fn age(&self, home: &Path, aging: Aging) -> Result<()>;

    /// How many refresh grants the identity provider served for the refresh token stored in
    /// `home`.
    ///
    /// # Errors
    ///
    /// When the stored refresh token cannot be read.
    fn refresh_grants(&self, home: &Path) -> Result<usize>;

    /// Makes the identity provider reject the refresh token stored in `home` from now on.
    ///
    /// # Errors
    ///
    /// When the stored refresh token cannot be read.
    fn reject_refresh(&self, home: &Path) -> Result<()>;

    /// Runs `gang doctor` with the stored login of `who`, from the deployment's workspace, with
    /// Bazel reading the rc file `gang bazelrc` renders.
    ///
    /// # Errors
    ///
    /// When the command cannot be run.
    fn doctor(&self, who: &Credentials) -> Result<Ran>;

    /// Waits until the standing worker is registered with the server, and returns the
    /// certificate it presents.
    ///
    /// # Errors
    ///
    /// When no worker registers within the deadline, or its certificate cannot be read.
    fn standing_worker(&self) -> Result<WorkerIdentity>;

    /// Opens a connection to the worker listener, presenting the certificate of `worker` when
    /// given and none otherwise.
    ///
    /// # Errors
    ///
    /// When the connection cannot be attempted. A TLS refusal is `Ok` with
    /// [`Admission::Refused`].
    fn connect_worker(&self, worker: Option<&WorkerIdentity>) -> Result<Admission<Self::Channel>>;

    /// Waits until the process of `worker` has exited, and returns its log then; `None` when it
    /// still runs at the deadline.
    ///
    /// # Errors
    ///
    /// When the wait cannot be made or the log cannot be read.
    fn worker_exit_log(&self, worker: &WorkerIdentity) -> Result<Option<String>>;

    /// Kills the server and starts it again on the same configuration and listeners.
    ///
    /// # Errors
    ///
    /// When the server cannot be stopped or does not start listening again.
    fn restart_server(&self) -> Result<()>;
}

/// Trust pin used to explain a refusal to connect to an untrusted server.
const TRUST_FIX: &str = "add --ca-pin sha256:<hex>; gang server prints it at startup";
/// Text reported when the server cannot be reached.
const REACH_TEXT: &str = "cannot reach the server";
/// Fix for an unsupported server URL scheme.
const SCHEME_FIX: &str = "use an http or https URL for --server";
/// Fix for an action refused for missing permission.
const FORBIDDEN_FIX: &str =
    "ask an operator for a role that grants the permission the action needs";
/// Fix for a refused stored login.
const LOGIN_AGAIN_FIX: &str = "run gang login again";
/// Reason reported when the provider refuses a stored refresh token.
const RENEWAL_REJECTED_TEXT: &str = "the identity provider no longer accepts the stored login";
/// Serial that has no certificate; permission checks precede certificate lookup.
const UNKNOWN_SERIAL: &str = "7f00ff";
/// Protected principal route requiring principals.read and accepting session cookies.
const PROTECTED_PATH: &str = "/v1/principals";
/// Session route.
const SESSION_PATH: &str = "/v1/session";
/// Safe post-login destination.
const NEXT_PATH: &str = "/ui/workers";
/// Unsafe post-login destination.
const UNSAFE_NEXT: &str = "https://evil.example/ui";
/// UI fallback destination.
const UI_HOME: &str = "/ui";
const DEVICE_AUTHORIZATIONS: &str = "/v1/deviceAuthorizations";
const DEVICE_PAGE: &str = "/ui/device";
const DEVICE_APPROVED: &str = "/ui/device/approved";
const LOOPBACK_CLIENT: &str = "127.0.0.1";
const FOREIGN_ORIGIN: &str = "https://evil.example";
const WRONG_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
/// Login cookie lifetime in seconds.
const LOGIN_COOKIE_MAX_AGE: u64 = 600;
/// The TLS alert a worker logs when the server refuses its revoked certificate.
pub const REVOKED_TEXT: &str = "CertificateRevoked";

/// Converts a verdict into the named scenario check.
fn judged(name: &'static str, verdict: Verdict) -> Check {
    match verdict {
        Ok(()) => Check::passed(name),
        Err(reason) => Check::failed(name, reason),
    }
}

/// Formats the endpoint's host and port.
fn host_port(endpoint: &Url) -> String {
    format!(
        "{}:{}",
        endpoint.host_str().unwrap_or_default(),
        endpoint.port_or_known_default().unwrap_or_default()
    )
}

/// Checks refusals for logins directed at untrusted servers.
fn misdirections<A: Access>(access: &A, endpoint: &Url) -> Result<Vec<Check>> {
    let host_port = host_port(endpoint);
    let wrong = access.misdirected_login(Misdirection::WrongPin)?;
    let mut checks = vec![
        judged(
            identity::WRONG_PIN_REFUSED,
            failure_reported(
                &wrong.outcome,
                "gang login",
                &format!("{REACH_TEXT} https://{host_port}"),
                TRUST_FIX,
            ),
        ),
        Check::expect(
            identity::WRONG_PIN_STORES_NOTHING,
            wrong.stored.is_empty(),
            || format!("gang login stored {:?}", wrong.stored),
        ),
    ];
    let plain = access.misdirected_login(Misdirection::PlainScheme)?;
    let expected_line = format!("  fail  gang login: {REACH_TEXT} http://{host_port}");
    checks.push(Check::expect(
        identity::PLAIN_SCHEME_ACCEPTED,
        plain.outcome.code == Some(1)
            && has_line(&plain.outcome.stderr, &expected_line)
            && !plain
                .outcome
                .stderr
                .contains("must use the http or https scheme"),
        || {
            format!(
                "code {:?}; stderr: {}",
                plain.outcome.code, plain.outcome.stderr
            )
        },
    ));
    let ftp = access.misdirected_login(Misdirection::UnsupportedScheme)?;
    checks.push(judged(
        identity::UNSUPPORTED_SCHEME_REFUSED,
        failure_reported(
            &ftp.outcome,
            "gang login",
            &format!("the server URL ftp://{host_port}/ must use the http or https scheme"),
            SCHEME_FIX,
        ),
    ));
    Ok(checks)
}

/// Checks that principal and certificate revocation require the operator permission.
fn permissions<A: Access>(
    access: &A,
    member: &Credentials,
    admin: &Credentials,
) -> Result<Vec<Check>> {
    let permission = Permission::PrincipalsRevoke.to_string();
    let mut checks = Vec::new();
    for (name, action, subject) in [
        (
            identity::MEMBER_REVOKE_REFUSED,
            Administration::RevokePrincipal(admin.principal.clone()),
            "gang revoke",
        ),
        (
            identity::MEMBER_RESTORE_REFUSED,
            Administration::RestorePrincipal(admin.principal.clone()),
            "gang restore",
        ),
        (
            identity::MEMBER_SERIAL_REFUSED,
            Administration::RevokeSerial(
                CertSerial::try_new(UNKNOWN_SERIAL)
                    .map_err(E2eError::parse("the unknown serial"))?,
            ),
            "gang revoke",
        ),
    ] {
        let outcome = access.administer(member, &action)?;
        checks.push(judged(
            name,
            failure_reported(
                &outcome,
                subject,
                &format!("the server answered 403: missing permission {permission}"),
                FORBIDDEN_FIX,
            ),
        ));
    }
    let principal = admin.identity()?;
    let answer = access.request(
        &Request::new(
            Method::POST,
            gang_wire::routes::REVOKE_PRINCIPAL.path(&principal),
        )
        .presenting(Presented::Bearer(member.token.clone())),
    )?;
    checks.push(judged(
        identity::MEMBER_API_REFUSED,
        crate::server_verdict::refusal(
            &answer,
            ErrorReason::MissingPermission,
            &[("permission", &permission)],
        ),
    ));
    Ok(checks)
}

/// Checks bearer-token admission and refusal across protected routes.
fn bearers<A: Access>(access: &A, admin: &Credentials) -> Result<Vec<Check>> {
    let get = || Request::get(PROTECTED_PATH);
    let answer = access.get_with_bearer(PROTECTED_PATH, &admin.principal, Validity::Current)?;
    let mut checks = vec![judged(
        identity::MINTED_BEARER_ACCEPTED,
        status_is(&answer, 200),
    )];
    let answer = access.get_with_bearer(PROTECTED_PATH, &admin.principal, Validity::Expired)?;
    checks.push(judged(
        identity::EXPIRED_BEARER_REFUSED,
        crate::server_verdict::refusal(&answer, ErrorReason::CredentialsInvalid, &[]),
    ));
    let cookie = access.session_cookie(admin)?;
    let answer = access.request(&get().presenting(Presented::Cookie(cookie.clone())))?;
    checks.push(judged(
        identity::SESSION_COOKIE_ACCEPTED,
        status_is(&answer, 200),
    ));
    let answer = access.request(&get().presenting(Presented::Bearer(cookie.value.clone())))?;
    checks.push(judged(
        identity::SESSION_AS_BEARER_REFUSED,
        crate::server_verdict::refusal(&answer, ErrorReason::CredentialsInvalid, &[]),
    ));
    let answer = access.request(&get().presenting(Presented::Cookie(SessionCookie {
        name: cookie.name,
        value: admin.token.clone(),
    })))?;
    checks.push(judged(
        identity::BEARER_AS_COOKIE_REFUSED,
        crate::server_verdict::refusal(&answer, ErrorReason::CredentialsMissing, &[]),
    ));
    Ok(checks)
}

/// Runs the identity scenario through `access`.
///
/// # Errors
///
/// When a program cannot be run or an observation cannot be made.
pub fn identity<A: Access>(access: &A) -> Result<Vec<Check>> {
    let endpoint = access.endpoint()?;
    let member = access.workload(MEMBER_SUBJECT)?;
    let administrator = access.administrator()?;
    let mut checks = Vec::new();
    checks.extend(misdirections(access, &endpoint)?);
    checks.extend(permissions(access, &member, &administrator)?);
    checks.extend(bearers(access, &administrator)?);
    checks.extend(sign_in(access, &endpoint)?);
    checks.extend(proof_command(access)?);
    checks.extend(device_checks(access, &endpoint, &administrator)?);
    checks.extend(renewal(access, &administrator)?);
    checks.extend(doctor(access, &endpoint, &member, &administrator)?);
    checks.extend(worker_certificate(access, &administrator)?);
    checks.extend(restart(access, &member, &administrator)?);
    Ok(checks)
}

/// An approval's server and provider responses.
struct Approval {
    started: Answer,
    authorization_url: Url,
    provider_location: String,
    done: Answer,
}

/// Approves a device code through the provider and its callback.
fn approve<A: Access>(
    access: &A,
    endpoint: &Url,
    code: &str,
) -> Result<std::result::Result<Approval, String>> {
    let body = serde_json::to_vec(&gang_wire::ApproveDeviceAuthorizationRequest {
        user_code: code.to_owned(),
    })
    .map_err(E2eError::system_from("serializing device approval"))?;
    let started = access.request(
        &Request::new(Method::POST, DEVICE_PAGE)
            .header("origin", endpoint.origin().ascii_serialization())
            .header("sec-fetch-site", "same-origin")
            .body("application/json", body),
    )?;
    let provider_url = match serde_json::from_slice::<gang_wire::ApproveDeviceAuthorizationResponse>(
        &started.body,
    ) {
        Ok(answer) if started.status == 200 => answer.authorization_url,
        _ => {
            return Ok(Err(format!(
                "the approval answered {} without a valid provider URL",
                started.status
            )));
        }
    };
    let provider = access.visit(&provider_url)?;
    let Some(provider_location) = provider.header("location") else {
        return Ok(Err(format!(
            "the provider answered {} without a callback redirect",
            provider.status
        )));
    };
    let Ok(redirect) = Url::parse(provider_location) else {
        return Ok(Err(format!(
            "the provider redirect is invalid: {provider_location}"
        )));
    };
    let name = login_cookie_name(endpoint);
    let value = set_cookies(&started)
        .into_iter()
        .find(|cookie| cookie.name == name)
        .map_or_else(String::new, |cookie| cookie.value);
    let done = access.request(
        &Request::get(format!(
            "{}?{}",
            redirect.path(),
            redirect.query().unwrap_or_default()
        ))
        .presenting(Presented::Cookie(SessionCookie {
            name: name.to_owned(),
            value,
        })),
    )?;
    Ok(Ok(Approval {
        started,
        authorization_url: provider_url,
        provider_location: provider_location.to_owned(),
        done,
    }))
}

/// Redeems a device authorization with its secret.
fn redeem<A: Access>(access: &A, code: &str, secret: &str) -> Result<Answer> {
    access.request(
        &Request::new(
            Method::POST,
            format!("{DEVICE_AUTHORIZATIONS}/{code}:redeem"),
        )
        .body(
            "application/json",
            serde_json::json!({ "secret": secret })
                .to_string()
                .into_bytes(),
        ),
    )
}

/// Reads a device authorization.
fn get_device<A: Access>(access: &A, code: &str) -> Result<Answer> {
    access.request(&Request::get(format!("{DEVICE_AUTHORIZATIONS}/{code}")))
}

/// Checks the headless device login and its subsequent renewal.
fn device_checks<A: Access>(access: &A, endpoint: &Url, admin: &Credentials) -> Result<Vec<Check>> {
    access.set_policy(SIGN_IN_SUBJECT, SubjectPolicy::Rotating);
    let mark = access.authorizations(SIGN_IN_SUBJECT).len();
    let mut resource = None;
    let mut approval = None;
    let login = access.device_login(&mut |prompt| {
        resource = Some(get_device(access, &prompt.user_code)?);
        approval = Some(approve(access, endpoint, &prompt.user_code)?);
        Ok(())
    })?;
    let expected = endpoint
        .join(DEVICE_PAGE)
        .map_err(E2eError::parse("the device page URL"))?
        .to_string();
    let prompted = match &login.prompt {
        None => Err(format!("no prompt; stderr: {}", login.outcome.stderr)),
        Some(prompt) if prompt.verification_uri != expected => Err(format!(
            "verification URI {}; expected {expected}",
            prompt.verification_uri
        )),
        Some(_) => device_only(&login.outcome.stderr),
    };
    let mut checks = vec![judged(identity::DEVICE_LOGIN_PROMPTED, prompted)];
    if let (Some(prompt), Some(Ok(approved)), Some(resource)) =
        (&login.prompt, &approval, &resource)
    {
        checks.push(judged(
            identity::DEVICE_SHOWS_CLIENT_ADDRESS,
            device_resource(resource, &prompt.user_code, LOOPBACK_CLIENT),
        ));
        checks.push(judged(
            identity::DEVICE_APPROVAL_REDIRECTS,
            device_approved(
                &approved.started,
                &approved.provider_location,
                &approved.done,
                cookie_name(endpoint),
            ),
        ));
        let records = access.authorizations(SIGN_IN_SUBJECT);
        let offline = offline_requested(approved.authorization_url.as_str())
            .and_then(|()| authorized(&records[mark..], true));
        checks.push(judged(identity::DEVICE_APPROVAL_OFFLINE, offline));
        let files = access.session_files(&login.home)?;
        checks.push(judged(
            identity::DEVICE_LOGIN_COMPLETES,
            logged_in_with(&login.outcome, crate::template::PROVIDER)
                .and_then(|()| device_session(&files)),
        ));
        device_renewal_checks(access, admin, &login.home, &mut checks)?;
    } else {
        let reason = match &approval {
            Some(Err(reason)) => reason.as_str(),
            _ => "no prompt or approval response",
        };
        for &name in &identity::DEVICE[1..7] {
            checks.push(Check::failed(
                name,
                format!("the device login was not approved: {reason}"),
            ));
        }
    }
    access.set_policy(SIGN_IN_SUBJECT, SubjectPolicy::Standard);
    device_missing_refresh_check(access, endpoint, &mut checks)?;
    device_attacker_checks(access, endpoint, admin, &mut checks)?;
    device_browser_check(access, endpoint, &mut checks)?;
    Ok(checks)
}

/// Checks renewal and revocation of a completed device session.
fn device_renewal_checks<A: Access>(
    access: &A,
    admin: &Credentials,
    home: &Path,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let first = access.get(home)?;
    access.age(home, Aging::Token)?;
    let before = access.refreshes(SIGN_IN_SUBJECT).len();
    let renewed = access.get(home)?;
    let grants = access.refreshes(SIGN_IN_SUBJECT);
    let files = access.session_files(home)?;
    let verdict = if first.outcome.code == Some(0)
        && first.bearer.is_some()
        && renewed.outcome.code == Some(0)
        && renewed.bearer.is_some()
    {
        renewed_by_web_client(&grants[before..]).and_then(|issued| rotation_stored(&files, &issued))
    } else {
        Err(format!(
            "first code {:?}, stderr {}; renewed code {:?}, stderr {}",
            first.outcome.code, first.outcome.stderr, renewed.outcome.code, renewed.outcome.stderr
        ))
    };
    checks.push(judged(identity::DEVICE_LOGIN_RENEWS, verdict));
    access.set_policy(SIGN_IN_SUBJECT, SubjectPolicy::Standard);
    let operator = format!("{}:{SIGN_IN_SUBJECT}", crate::template::PROVIDER);
    let revoke = access.administer(admin, &Administration::RevokePrincipal(operator.clone()))?;
    access.age(home, Aging::Token)?;
    let refused = access.get(home)?;
    let restore = access.administer(admin, &Administration::RestorePrincipal(operator))?;
    access.age(home, Aging::Token)?;
    let restored = access.get(home)?;
    let verdict = if revoke.code != Some(0)
        || restore.code != Some(0)
        || restored.outcome.code != Some(0)
        || restored.bearer.is_none()
    {
        Err(format!(
            "revoke {:?}: {}; restore {:?}: {}; get {:?}: {}",
            revoke.code,
            revoke.stderr,
            restore.code,
            restore.stderr,
            restored.outcome.code,
            restored.outcome.stderr
        ))
    } else {
        failure_reported(
            &refused.outcome,
            "gang get",
            "the server answered 403: principal revoked",
            FORBIDDEN_FIX,
        )
    };
    checks.push(judged(identity::DEVICE_LOGIN_REVOKED, verdict));
    Ok(())
}

/// Checks that a provider without refresh tokens cannot approve a device.
fn device_missing_refresh_check<A: Access>(
    access: &A,
    endpoint: &Url,
    checks: &mut Vec<Check>,
) -> Result<()> {
    access.set_policy(SIGN_IN_SUBJECT, SubjectPolicy::WithoutRefreshTokens);
    let mut seen = None;
    let failed = access.device_login(&mut |prompt| {
        let approval = approve(access, endpoint, &prompt.user_code)?;
        let resource = get_device(access, &prompt.user_code)?;
        seen = Some((approval, resource));
        Ok(())
    })?;
    access.set_policy(SIGN_IN_SUBJECT, SubjectPolicy::Standard);
    let files = access.session_files(&failed.home)?;
    let log = access.server_log()?;
    let verdict = match seen {
        Some((Ok(approval), resource)) => {
            let state = serde_json::from_slice::<serde_json::Value>(&resource.body)
                .ok()
                .and_then(|value| {
                    value
                        .get("state")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                });
            redirected_to(&approval.done, &format!("{DEVICE_PATH}?reason=failed"))
                .and_then(|()| {
                    if state.as_deref() == Some("PENDING") {
                        Ok(())
                    } else {
                        Err(format!("device state {state:?}"))
                    }
                })
                .and_then(|()| {
                    if failed.outcome.code == Some(1) {
                        Ok(())
                    } else {
                        Err(format!("login code {:?}", failed.outcome.code))
                    }
                })
                .and_then(|()| stored_nothing(&files))
                .and_then(|()| {
                    if log.contains("login.offline") {
                        Ok(())
                    } else {
                        Err("the server log has no warning naming login.offline".to_owned())
                    }
                })
        }
        Some((Err(reason), _)) => Err(reason),
        None => Err("no device approval was observed".to_owned()),
    };
    checks.push(judged(
        identity::DEVICE_WITHOUT_REFRESH_TOKEN_FAILS,
        verdict,
    ));
    Ok(())
}

/// Creates a device authorization and extracts its code and secret.
fn create_device<A: Access>(access: &A) -> Result<std::result::Result<(String, String), String>> {
    let created = access.request(
        &Request::new(Method::POST, DEVICE_AUTHORIZATIONS).body("application/json", b"{}".to_vec()),
    )?;
    let parsed = serde_json::from_slice::<serde_json::Value>(&created.body).ok();
    let fields = parsed.as_ref().and_then(|value| {
        Some((
            value
                .get("name")?
                .as_str()?
                .strip_prefix("deviceAuthorizations/")?
                .to_owned(),
            value.get("secret")?.as_str()?.to_owned(),
        ))
    });
    Ok(fields.filter(|_| created.status == 200).ok_or_else(|| {
        format!(
            "the device authorization could not be created: status {} {}",
            created.status,
            String::from_utf8_lossy(&created.body)
        )
    }))
}

/// Checks approval under the dashboard's browser-enforced policy.
fn device_browser_check<A: Access>(
    access: &A,
    endpoint: &Url,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let (code, _) = match create_device(access)? {
        Ok(device) => device,
        Err(reason) => {
            checks.push(Check::failed(identity::DEVICE_APPROVED_IN_BROWSER, reason));
            return Ok(());
        }
    };
    let navigation = access.approve_in_browser(&code)?;
    let resource = get_device(access, &code)?;
    checks.push(judged(
        identity::DEVICE_APPROVED_IN_BROWSER,
        crate::device_verdict::browser_approved(&navigation, endpoint, &resource),
    ));
    Ok(())
}

/// Checks cross-site, secret, revocation, and replay attacks.
fn device_attacker_checks<A: Access>(
    access: &A,
    endpoint: &Url,
    admin: &Credentials,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let (code, secret) = match create_device(access)? {
        Ok(device) => device,
        Err(reason) => {
            for &name in &identity::DEVICE[8..12] {
                checks.push(Check::failed(name, reason.clone()));
            }
            return Ok(());
        }
    };
    let body = serde_json::to_vec(&gang_wire::ApproveDeviceAuthorizationRequest {
        user_code: code.clone(),
    })
    .map_err(E2eError::system_from("serializing device approval"))?;
    let cross = access.request(
        &Request::new(Method::POST, DEVICE_PAGE)
            .header("sec-fetch-site", "cross-site")
            .header("origin", FOREIGN_ORIGIN)
            .body("application/json", body),
    )?;
    checks.push(judged(
        identity::CROSS_SITE_DEVICE_APPROVAL_REFUSED,
        cross_site_approval_refused(&cross),
    ));
    let wrong = redeem(access, &code, WRONG_SECRET)?;
    checks.push(judged(
        identity::WRONG_SECRET_REDEEM_REFUSED,
        refused_with(&wrong, 404, "DEVICE_AUTHORIZATION_NOT_FOUND"),
    ));
    let approved = approve(access, endpoint, &code)?;
    let operator = format!("{}:{SIGN_IN_SUBJECT}", crate::template::PROVIDER);
    let revoke = access.administer(admin, &Administration::RevokePrincipal(operator.clone()))?;
    let first = redeem(access, &code, &secret)?;
    let restore = access.administer(admin, &Administration::RestorePrincipal(operator))?;
    let second = redeem(access, &code, &secret)?;
    let verdict = approved
        .and_then(|approval| redirected_to(&approval.done, DEVICE_APPROVED))
        .and_then(|()| {
            if revoke.code == Some(0) && restore.code == Some(0) {
                Ok(())
            } else {
                Err(format!(
                    "revoke {:?}: {}; restore {:?}: {}",
                    revoke.code, revoke.stderr, restore.code, restore.stderr
                ))
            }
        })
        .and_then(|()| refused_with(&first, 403, "PRINCIPAL_REVOKED"));
    checks.push(judged(identity::REVOKED_BEFORE_REDEEM_REFUSED, verdict));
    checks.push(judged(
        identity::SECOND_REDEEM_REFUSED,
        refused_with(&second, 404, "DEVICE_AUTHORIZATION_NOT_FOUND"),
    ));
    Ok(())
}

/// One started authorization transaction and its callback information.
struct Leg {
    /// Response that began sign-in.
    started: Answer,
    /// State bound to the login cookie.
    state: String,
    /// Callback path and query.
    callback_target: String,
    /// Provider authorization location.
    location: String,
}

/// Starts provider authorization and records its callback information.
fn begin<A: Access>(
    access: &A,
    endpoint: &Url,
    next: &str,
) -> Result<std::result::Result<Leg, String>> {
    let mut url = endpoint
        .join(LOGIN_PATH)
        .map_err(E2eError::parse("the sign-in url"))?;
    url.query_pairs_mut().append_pair("next", next);
    let target = format!("{}?{}", url.path(), url.query().unwrap_or_default());
    let started = access.request(&Request::get(target))?;
    let Some(location) = started.header("location") else {
        return Ok(Err("the sign-in response has no location".to_owned()));
    };
    let location = location.to_owned();
    let Some(state) = query_value(&location, "state") else {
        return Ok(Err(format!(
            "the authorization URL has no state: {location}"
        )));
    };
    let Ok(provider_url) = Url::parse(&location) else {
        return Ok(Err(format!("the authorization URL is invalid: {location}")));
    };
    let approved = access.visit(&provider_url)?;
    let Some(redirect_location) = approved.header("location") else {
        return Ok(Err(
            "the provider response has no redirect location".to_owned()
        ));
    };
    let Ok(redirect) = Url::parse(redirect_location) else {
        return Ok(Err(format!(
            "the provider redirect is invalid: {redirect_location}"
        )));
    };
    Ok(Ok(Leg {
        started,
        state,
        callback_target: format!(
            "{}?{}",
            redirect.path(),
            redirect.query().unwrap_or_default()
        ),
        location,
    }))
}

/// Completes the provider callback with the login state cookie.
fn callback<A: Access>(access: &A, leg: &Leg, login_cookie: &str) -> Result<Answer> {
    access.request(
        &Request::get(&leg.callback_target).presenting(Presented::Cookie(SessionCookie {
            name: login_cookie.to_owned(),
            value: leg.state.clone(),
        })),
    )
}

/// Checks the dashboard sign-in, session, and logout flows.
fn sign_in<A: Access>(access: &A, endpoint: &Url) -> Result<Vec<Check>> {
    let callback_url = endpoint
        .join(CALLBACK_PATH)
        .map_err(E2eError::parse("the callback url"))?
        .to_string();
    let login_cookie = login_cookie_name(endpoint);
    let first = match begin(access, endpoint, NEXT_PATH)? {
        Ok(leg) => leg,
        Err(reason) => {
            let names = [
                identity::SIGN_IN_REDIRECTS,
                identity::LOGIN_COOKIE_SET,
                identity::STATE_MISMATCH_REFUSED,
                identity::SIGN_IN_COMPLETES,
                identity::SIGNED_IN_SESSION_ACCEPTED,
                identity::UNSAFE_NEXT_DROPPED,
                identity::CROSS_SITE_LOGOUT_REFUSED,
                identity::LOGOUT_CLEARS_SESSION,
            ];
            return Ok(names
                .into_iter()
                .map(|name| {
                    Check::failed(
                        name,
                        if name == identity::SIGN_IN_REDIRECTS {
                            reason.clone()
                        } else {
                            format!("the sign-in did not reach the provider's redirect: {reason}")
                        },
                    )
                })
                .collect());
        }
    };
    let redirect_verdict = if first.started.status == 302 {
        authorization_request(&first.location, DASHBOARD_CLIENT_ID, &callback_url)
    } else {
        Err(format!(
            "status {}; location {}",
            first.started.status, first.location
        ))
    };
    let mut checks = vec![judged(identity::SIGN_IN_REDIRECTS, redirect_verdict)];
    checks.push(judged(
        identity::LOGIN_COOKIE_SET,
        cookie_set(
            &first.started,
            &ExpectedCookie {
                name: login_cookie,
                value: CookieValue::Exactly(&first.state),
                same_site: "Lax",
                max_age: MaxAge::Exactly(LOGIN_COOKIE_MAX_AGE),
            },
        ),
    ));
    let mismatched = access.request(&Request::get(&first.callback_target).presenting(
        Presented::Cookie(SessionCookie {
            name: login_cookie.to_owned(),
            value: format!("{}-other", first.state),
        }),
    ))?;
    checks.push(judged(
        identity::STATE_MISMATCH_REFUSED,
        redirected_to(&mismatched, &format!("{ERROR_PATH}?reason=failed")),
    ));
    sign_in_complete(access, endpoint, &first, &mut checks)?;
    Ok(checks)
}

/// Completes the sign-in and checks the resulting session.
fn sign_in_complete<A: Access>(
    access: &A,
    endpoint: &Url,
    first: &Leg,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let login_cookie = login_cookie_name(endpoint);
    let session_name = cookie_name(endpoint);
    let done = callback(access, first, login_cookie)?;
    let session_cookie = set_cookies(&done)
        .into_iter()
        .find(|cookie| cookie.name == session_name)
        .map_or_else(String::new, |cookie| cookie.value);
    let completion = redirected_to(&done, NEXT_PATH)
        .and_then(|()| {
            cookie_set(
                &done,
                &ExpectedCookie {
                    name: session_name,
                    value: CookieValue::NonEmpty,
                    same_site: "Strict",
                    max_age: MaxAge::Positive,
                },
            )
        })
        .and_then(|()| {
            cookie_set(
                &done,
                &ExpectedCookie {
                    name: login_cookie,
                    value: CookieValue::Exactly(""),
                    same_site: "Lax",
                    max_age: MaxAge::Exactly(0),
                },
            )
        });
    checks.push(judged(identity::SIGN_IN_COMPLETES, completion));
    let session = SessionCookie {
        name: session_name.to_owned(),
        value: session_cookie,
    };
    let answer = access
        .request(&Request::get(SESSION_PATH).presenting(Presented::Cookie(session.clone())))?;
    checks.push(judged(
        identity::SIGNED_IN_SESSION_ACCEPTED,
        status_is(&answer, 200),
    ));
    match begin(access, endpoint, UNSAFE_NEXT)? {
        Err(reason) => checks.push(Check::failed(identity::UNSAFE_NEXT_DROPPED, reason)),
        Ok(leg) => {
            let done = callback(access, &leg, login_cookie)?;
            checks.push(judged(
                identity::UNSAFE_NEXT_DROPPED,
                redirected_to(&done, UI_HOME),
            ));
        }
    }
    logout(access, endpoint, session, checks)?;
    Ok(())
}

/// Checks cross-site and same-origin logout.
fn logout<A: Access>(
    access: &A,
    endpoint: &Url,
    session: SessionCookie,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let origin = endpoint.origin().ascii_serialization();
    let session_name = cookie_name(endpoint);
    let cross_site = access.request(
        &Request::new(Method::POST, LOGOUT_PATH)
            .presenting(Presented::Cookie(session.clone()))
            .header("sec-fetch-site", "cross-site"),
    )?;
    let cross_site_cookies = set_cookies(&cross_site);
    checks.push(Check::expect(
        identity::CROSS_SITE_LOGOUT_REFUSED,
        cross_site.status == 403 && cross_site_cookies.is_empty(),
        || {
            format!(
                "status {}; set cookies {cross_site_cookies:?}",
                cross_site.status
            )
        },
    ));
    let logout = access.request(
        &Request::new(Method::POST, LOGOUT_PATH)
            .presenting(Presented::Cookie(session))
            .header("origin", origin),
    )?;
    let logout_verdict = status_is(&logout, 204).and_then(|()| {
        cookie_set(
            &logout,
            &ExpectedCookie {
                name: session_name,
                value: CookieValue::Exactly(""),
                same_site: "Strict",
                max_age: MaxAge::Exactly(0),
            },
        )
    });
    checks.push(judged(identity::LOGOUT_CLEARS_SESSION, logout_verdict));
    Ok(())
}

/// Checks proof-command login and token reuse.
fn proof_command<A: Access>(access: &A) -> Result<Vec<Check>> {
    let login = access.command_login()?;
    let runs0 = access.proof_runs(&login.home)?;
    let mut checks = vec![Check::expect(
        identity::COMMAND_LOGIN,
        login.outcome.code == Some(0) && runs0 == 1,
        || {
            format!(
                "code {:?}; runs {runs0}; stderr {}",
                login.outcome.code, login.outcome.stderr
            )
        },
    )];
    let first = access.get(&login.home)?;
    let runs1 = access.proof_runs(&login.home)?;
    checks.push(Check::expect(
        identity::FRESH_TOKEN_REUSED,
        first.outcome.code == Some(0) && first.bearer.is_some() && runs1 == runs0,
        || {
            format!(
                "code {:?}; runs {runs0} then {runs1}; stderr {}",
                first.outcome.code, first.outcome.stderr
            )
        },
    ));
    access.age(&login.home, Aging::Token)?;
    let second = access.get(&login.home)?;
    let runs2 = access.proof_runs(&login.home)?;
    checks.push(Check::expect(
        identity::COMMAND_RERUN,
        second.outcome.code == Some(0) && second.bearer.is_some() && runs2 == runs1 + 1,
        || {
            format!(
                "code {:?}; runs {runs1} then {runs2}; stderr {}",
                second.outcome.code, second.outcome.stderr
            )
        },
    ));
    Ok(checks)
}

/// Checks interactive token renewal and revoked-principal refusal.
fn renewal<A: Access>(access: &A, admin: &Credentials) -> Result<Vec<Check>> {
    let mut checks = Vec::new();
    let first = access.interactive_login()?;
    let initial = access.get(&first.home)?;
    if first.outcome.code != Some(0) || initial.outcome.code != Some(0) || initial.bearer.is_none()
    {
        checks.push(Check::failed(
            identity::INTERACTIVE_LOGIN,
            format!(
                "login code {:?}; get code {:?}; {}",
                first.outcome.code, initial.outcome.code, initial.outcome.stderr
            ),
        ));
        checks.push(Check::failed(
            identity::EXPIRED_LOGIN_RENEWED,
            "the interactive login failed",
        ));
        checks.push(Check::failed(
            identity::RENEWAL_REJECTED,
            "the interactive login failed",
        ));
    } else {
        checks.push(Check::passed(identity::INTERACTIVE_LOGIN));
        access.age(&first.home, Aging::TokenAndProof)?;
        let renewed = access.get(&first.home)?;
        let grants = access.refresh_grants(&first.home)?;
        let served = match &renewed.bearer {
            Some(bearer) => {
                access
                    .request(
                        &Request::get(SESSION_PATH).presenting(Presented::Bearer(bearer.clone())),
                    )?
                    .status
            }
            None => 0,
        };
        checks.push(Check::expect(
            identity::EXPIRED_LOGIN_RENEWED,
            renewed.outcome.code == Some(0)
                && renewed.bearer.is_some()
                && grants == 1
                && served == 200,
            || {
                format!(
                    "code {:?}; grants {grants}; status {served}; stderr {}",
                    renewed.outcome.code, renewed.outcome.stderr
                )
            },
        ));
        access.reject_refresh(&first.home)?;
        access.age(&first.home, Aging::TokenAndProof)?;
        let rejected = access.get(&first.home)?;
        checks.push(judged(
            identity::RENEWAL_REJECTED,
            failure_reported(
                &rejected.outcome,
                "gang get",
                RENEWAL_REJECTED_TEXT,
                LOGIN_AGAIN_FIX,
            ),
        ));
    }
    revoked_login(access, admin, &mut checks)?;
    Ok(checks)
}

/// Checks that renewal refuses a revoked principal.
fn revoked_login<A: Access>(
    access: &A,
    admin: &Credentials,
    checks: &mut Vec<Check>,
) -> Result<()> {
    let second = access.interactive_login()?;
    let operator = format!(
        "{crate_provider}:{SIGN_IN_SUBJECT}",
        crate_provider = crate::template::PROVIDER
    );
    let revoked = access.administer(admin, &Administration::RevokePrincipal(operator))?;
    checks.push(Check::expect(
        identity::OPERATOR_REVOKED,
        revoked.code == Some(0),
        || format!("code {:?}; stderr {}", revoked.code, revoked.stderr),
    ));
    if second.outcome.code != Some(0) {
        checks.push(Check::failed(
            identity::REVOKED_LOGIN_REFUSED,
            format!(
                "the second interactive login failed: {}",
                second.outcome.stderr
            ),
        ));
        return Ok(());
    }
    access.age(&second.home, Aging::Token)?;
    let before = access.refresh_grants(&second.home)?;
    let refused = access.get(&second.home)?;
    let after = access.refresh_grants(&second.home)?;
    let refusal = failure_reported(
        &refused.outcome,
        "gang get",
        "the server answered 403: principal revoked",
        FORBIDDEN_FIX,
    );
    let verdict = refusal.and_then(|()| {
        if after == before + 1 {
            Ok(())
        } else {
            Err(format!("refresh grants went from {before} to {after}"))
        }
    });
    checks.push(judged(identity::REVOKED_LOGIN_REFUSED, verdict));
    Ok(())
}

/// Checks the credentials row reported by `gang doctor`.
fn doctor<A: Access>(
    access: &A,
    endpoint: &Url,
    member: &Credentials,
    admin: &Credentials,
) -> Result<Vec<Check>> {
    let before = access.doctor(member)?;
    let output = format!("{}\n{}", before.stdout, before.stderr);
    let plain = crate::cli::plain(&output);
    let rows = crate::cli::status_rows(&plain);
    let okay = rows.iter().any(|row| {
        row.subject == "credentials"
            && row.status == crate::cli::RowStatus::Ok
            && row.detail.ends_with(" get returns a token")
    });
    let mut checks = vec![Check::expect(identity::DOCTOR_CREDENTIALS_OK, okay, || {
        format!("doctor output: {output}")
    })];
    let revoked = access.administer(
        admin,
        &Administration::RevokePrincipal(member.principal.clone()),
    )?;
    checks.push(Check::expect(
        identity::MEMBER_REVOKED,
        revoked.code == Some(0),
        || format!("code {:?}; stderr {}", revoked.code, revoked.stderr),
    ));
    let after = access.doctor(member)?;
    let output = format!("{}\n{}", after.stdout, after.stderr);
    let plain = crate::cli::plain(&output);
    let rows = crate::cli::status_rows(&plain);
    let server = endpoint.as_str().trim_end_matches('/');
    let fix = format!(
        concat!(
            "log in again with `gang login --server {}`; if it still refuses, ",
            "ask the server's administrator whether your principal was revoked"
        ),
        server
    );
    checks.push(Check::expect(
        identity::DOCTOR_CREDENTIALS_REFUSED,
        after.code == Some(1)
            && rows.iter().any(|row| {
                row.subject == "credentials"
                    && row.status == crate::cli::RowStatus::Fail
                    && row.detail.starts_with("the server refused the token of ")
                    && row.fix == Some(fix.as_str())
            }),
        || format!("code {:?}; output: {output}", after.code),
    ));
    Ok(checks)
}

/// Converts a channel admission into a live, ping-acknowledged channel.
fn admitted<C: Channel>(admission: Admission<C>) -> Result<Option<C>> {
    match admission {
        Admission::Refused => Ok(None),
        Admission::Accepted(mut channel) => {
            if channel.ping()? == ChannelEvent::PingAcknowledged {
                Ok(Some(channel))
            } else {
                Ok(None)
            }
        }
    }
}

/// Checks worker certificate admission, revocation, and stream termination.
fn worker_certificate<A: Access>(access: &A, admin: &Credentials) -> Result<Vec<Check>> {
    let worker = access.standing_worker()?;
    let no_certificate = admitted(access.connect_worker(None)?)?.is_none();
    let mut checks = vec![Check::expect(
        identity::WORKER_LISTENER_NEEDS_CERTIFICATE,
        no_certificate,
        || "a connection without a certificate was admitted".to_owned(),
    )];
    let mut held = admitted(access.connect_worker(Some(&worker))?)?;
    checks.push(Check::expect(
        identity::WORKER_CERTIFICATE_ADMITTED,
        held.is_some(),
        || "the worker certificate was refused".to_owned(),
    ));
    let revoked = access.administer(admin, &Administration::RevokeSerial(worker.serial.clone()))?;
    checks.push(Check::expect(
        identity::SERIAL_REVOKED,
        revoked.code == Some(0),
        || format!("code {:?}; stderr {}", revoked.code, revoked.stderr),
    ));
    checks.push(match held.as_mut() {
        Some(channel) => Check::expect(
            identity::OPEN_WORKER_CONNECTION_CUT,
            channel.ping()? == ChannelEvent::Closed,
            || "the open worker connection did not close".to_owned(),
        ),
        None => Check::failed(
            identity::OPEN_WORKER_CONNECTION_CUT,
            "no connection was admitted before the revocation",
        ),
    });
    let refused = admitted(access.connect_worker(Some(&worker))?)?.is_none();
    checks.push(Check::expect(
        identity::REVOKED_CERTIFICATE_REFUSED,
        refused,
        || "a connection with the revoked certificate was admitted".to_owned(),
    ));
    checks.push(match access.worker_exit_log(&worker)? {
        Some(log) => Check::expect(
            identity::WORKER_STREAM_ENDS_REVOKED,
            log.contains(REVOKED_TEXT),
            || {
                let last_lines = log
                    .lines()
                    .rev()
                    .take(20)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    "the worker exited but its log does not contain \
                         `CertificateRevoked`: {last_lines}"
                )
            },
        ),
        None => Check::failed(
            identity::WORKER_STREAM_ENDS_REVOKED,
            "the worker with the revoked certificate still runs at the deadline",
        ),
    });
    Ok(checks)
}

/// Checks that principal revocation survives a server restart.
fn restart<A: Access>(access: &A, member: &Credentials, admin: &Credentials) -> Result<Vec<Check>> {
    let before = access.server_pid();
    access.restart_server()?;
    let after = access.server_pid();
    let mut checks = vec![Check::expect(
        identity::SERVER_RESTARTED,
        before.is_some() && after.is_some() && before != after,
        || format!("the server process went from {before:?} to {after:?}"),
    )];
    let revoked = access.get_with_bearer(PROTECTED_PATH, &member.principal, Validity::Current)?;
    checks.push(judged(
        identity::REVOCATION_SURVIVES_RESTART,
        crate::server_verdict::refusal(&revoked, ErrorReason::PrincipalRevoked, &[]),
    ));
    let served = access.get_with_bearer(PROTECTED_PATH, &admin.principal, Validity::Current)?;
    checks.push(judged(
        identity::RESTARTED_SERVER_SERVES,
        status_is(&served, 200),
    ));
    Ok(checks)
}
