//! Principal and worker revocation, followed by a leader handoff and restoration.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use gang_wire::{ClosedReason, Frame};
use x509_cert::Certificate;
use x509_cert::der::DecodePem;

use crate::administrator::Administrator;
use crate::check::Check;
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::gang_cli::{GangCli, client_home};
use crate::identity::Issuance;
use crate::issuer::{TestIssuer, TokenSpec};
use crate::names::cross_revocation::{
    BEARER_REFUSED, INITIAL_LOGIN, LOGIN_REFUSED, RESTORED, SECOND_STREAM_CLOSED, STREAM_CLOSED,
    STREAM_OPEN, WORKER_CUT,
};
use crate::process;
use crate::secured::log_in;
use crate::sse::{EventStream, STREAM_OK};
use crate::state_api::StateApi;

pub(crate) const CROSS_REVOCATION_SUBJECT: &str = "cross-revocation";

/// The default controller revocation refresh interval.
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// Time allowed for scheduling, transport and observation after a refresh.
const REFRESH_MARGIN: Duration = Duration::from_secs(5);
/// Default per-client token-bucket refill interval.
const DEFAULT_CREDENTIAL_PACE: Duration = Duration::from_secs(2);
/// The CLI reports this reason when the server has revoked the principal.
const PRINCIPAL_REVOKED: &str = "principal revoked";
/// The CLI reports this reason for a rate-limited request.
const RATE_LIMITED: &str = "rate limiting this client";
const WORKER_CERTIFICATE: &str = "certificate.pem";

fn cli(cluster: &Cluster<'_>, name: &str) -> Result<GangCli> {
    let stack = cluster.stack();
    let home = stack.placement().home().join(name);
    fs::create_dir_all(&home).map_err(E2eError::io(format!("creating {}", home.display())))?;
    Ok(GangCli::new(
        stack.binary().to_path_buf(),
        stack.placement().config_file(),
        home,
    ))
}

fn certificates(root: &Path, found: &mut Vec<PathBuf>) -> Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(E2eError::io(format!("reading {}", root.display()))(error)),
    };
    for entry in entries {
        let entry = entry.map_err(E2eError::io(format!("reading {}", root.display())))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(E2eError::io(format!("reading {}", path.display())))?;
        if kind.is_dir() {
            certificates(&path, found)?;
        } else if kind.is_file()
            && entry.file_name() == WORKER_CERTIFICATE
            && path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .is_some_and(|name| name == "identity")
        {
            found.push(path);
        }
    }
    Ok(())
}

fn worker_serial(cluster: &Cluster<'_>) -> Result<gang_types::CertSerial> {
    let mut paths = Vec::new();
    certificates(&cluster.stack().placement().state_dir(), &mut paths)?;
    if paths.len() != 1 {
        return Err(E2eError::system(format!(
            "expected one worker certificate, found {paths:?}"
        )));
    }
    let path = &paths[0];
    let pem = fs::read(path).map_err(E2eError::io(format!("reading {}", path.display())))?;
    let certificate =
        Certificate::from_pem(&pem).map_err(E2eError::parse("the worker certificate"))?;
    gang_types::CertSerial::from_be_bytes(certificate.tbs_certificate().serial_number().as_bytes())
        .map_err(E2eError::parse("the worker certificate"))
}

/// Reads the effective per-client refill interval from the rendered configuration.
fn credential_pace(cluster: &Cluster<'_>) -> Result<Duration> {
    let path = cluster.stack().placement().config_file();
    let text =
        fs::read_to_string(&path).map_err(E2eError::io(format!("reading {}", path.display())))?;
    let document: toml::Value = toml::from_str(&text).map_err(crate::template::template_error)?;
    let settings: gang_config::RateLimitConfig = match document
        .get("server")
        .and_then(|server| server.get("rate_limit"))
    {
        Some(value) => value
            .clone()
            .try_into()
            .map_err(crate::template::template_error)?,
        None => return Ok(DEFAULT_CREDENTIAL_PACE),
    };
    Ok(settings.client.refill_interval.get())
}

/// Distinguishes a revoked principal from rate limiting and other CLI failures.
fn issued(issuance: &Issuance) -> Result<bool> {
    match issuance {
        Issuance::Issued(_) => Ok(true),
        Issuance::Refused(reason) if reason.contains(PRINCIPAL_REVOKED) => Ok(false),
        Issuance::Refused(reason) if reason.contains(RATE_LIMITED) => Err(E2eError::system(
            format!("credential request was rate limited: {reason}"),
        )),
        Issuance::Refused(reason) => Err(E2eError::system(format!(
            "credential request failed unexpectedly: {reason}"
        ))),
    }
}

/// Uses bounded stream reads to wait between credential attempts without a busy loop.
fn wait_for_next_attempt(stream: &mut impl EventStream, until: Instant) -> Result<()> {
    loop {
        let remaining = until.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        match stream.next_event_within(remaining) {
            Ok(Some(_)) => {}
            Ok(None) => return Err(E2eError::system("credential pacing stream ended")),
            Err(E2eError::Timeout { .. }) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
}

fn issue_until(
    cluster: &Cluster<'_>,
    issuer: &TestIssuer,
    cli: &GangCli,
    endpoint: &gang_types::Url,
    deadline: Instant,
    want_issued: bool,
    pace: Duration,
) -> Result<bool> {
    let stack = cluster.stack();
    let administrator = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let mut pacing =
        StateApi::with_bearer(endpoint, &administrator, stack.settings().handshake_timeout)?
            .open_stream()?;
    if pacing.status() != STREAM_OK {
        return Err(E2eError::system(format!(
            "credential pacing stream answered {}",
            pacing.status()
        )));
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        let next_attempt = Instant::now() + pace;
        let result = log_in(
            cli,
            issuer,
            &TokenSpec::admitted(CROSS_REVOCATION_SUBJECT),
            endpoint,
            &cluster.stack().ca_pin()?,
            &cluster.stack().placement().workspace(),
            remaining.min(cluster.stack().settings().command_timeout),
        );
        match result {
            Ok(issuance) if issued(&issuance)? == want_issued => {
                return Ok(Instant::now() <= deadline);
            }
            Ok(_) => wait_for_next_attempt(&mut pacing, next_attempt.min(deadline))?,
            Err(E2eError::Timeout { .. }) if Instant::now() >= deadline => return Ok(false),
            Err(error) => return Err(error),
        }
    }
}

fn stream_closed(stream: &mut impl EventStream, deadline: Instant) -> Result<bool> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        match stream.next_event_within(remaining) {
            Ok(Some(Frame::Closed(closed))) => return Ok(closed.reason == ClosedReason::Revoked),
            Ok(Some(_)) => {}
            Ok(None) | Err(E2eError::Timeout { .. }) => return Ok(false),
            Err(error) => return Err(error),
        }
    }
}

/// Revokes the certificate of the registered standing worker through the administrator on A.
fn revoke_worker(
    cluster: &Cluster<'_>,
    administrator: &GangCli,
    bound: Duration,
) -> Result<(String, Instant)> {
    let stack = cluster.stack();
    let state = stack.wait_live_state(stack.settings().startup_timeout, |state| {
        !state.workers.is_empty()
    })?;
    let worker = state
        .workers
        .first()
        .ok_or_else(|| E2eError::system("no registered worker"))?
        .name
        .clone();
    let serial = worker_serial(cluster)?;
    let mut command = Command::new(stack.binary());
    client_home(&mut command, administrator.home());
    command.arg("revoke").arg("--serial").arg(serial.as_str());
    let deadline = Instant::now() + bound;
    let outcome = process::run(command, None, stack.settings().command_timeout)?;
    if !outcome.succeeded() {
        return Err(E2eError::system(format!(
            "worker revocation failed: {}",
            outcome.combined()
        )));
    }
    Ok((worker, deadline))
}

/// Observes the principal and worker concurrently against their original deadlines.
fn observe_revocation(
    cluster: &Cluster<'_>,
    issuer: &TestIssuer,
    principal_cli: &GangCli,
    endpoint: &gang_types::Url,
    streams: (
        &mut (impl EventStream + Send),
        &mut (impl EventStream + Send),
    ),
    worker: &str,
    deadlines: (Instant, Instant, Duration),
) -> Result<((bool, bool, bool), bool)> {
    let (principal_deadline, worker_deadline, pace) = deadlines;
    let (stream, second_stream) = streams;
    std::thread::scope(|scope| -> Result<_> {
        let login = scope.spawn(|| {
            issue_until(
                cluster,
                issuer,
                principal_cli,
                endpoint,
                principal_deadline,
                false,
                pace,
            )
        });
        let closure = scope.spawn(|| stream_closed(stream, principal_deadline));
        let second = scope.spawn(|| stream_closed(second_stream, principal_deadline));
        let worker_wait = scope.spawn(|| {
            let remaining = worker_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            match cluster.stack().wait_live_state(remaining, |state| {
                !state.workers.iter().any(|row| row.name == worker)
            }) {
                Ok(_) => Ok(Instant::now() <= worker_deadline),
                Err(E2eError::Timeout { .. }) => Ok(false),
                Err(error) => Err(error),
            }
        });
        let observed = (
            login
                .join()
                .map_err(|_| E2eError::system("login observation panicked"))??,
            closure
                .join()
                .map_err(|_| E2eError::system("stream observation panicked"))??,
            worker_wait
                .join()
                .map_err(|_| E2eError::system("worker observation panicked"))??,
        );
        let second_closed = second
            .join()
            .map_err(|_| E2eError::system("second stream observation panicked"))??;
        Ok((observed, second_closed))
    })
}

/// Judges the observed revocation and restoration against the shared bound.
fn revocation_checks(
    observed: (bool, bool, bool),
    restored: bool,
    worker: &str,
    bound: Duration,
    second_closed: bool,
    bearer_status: u16,
) -> Vec<Check> {
    let (refused, closed, cut) = observed;
    vec![
        Check::passed(INITIAL_LOGIN),
        Check::passed(STREAM_OPEN),
        Check::expect(LOGIN_REFUSED, refused, || {
            format!("leader did not refuse a fresh login within {bound:?}")
        }),
        Check::expect(STREAM_CLOSED, closed, || {
            format!("leader's stream did not close as revoked within {bound:?}")
        }),
        Check::expect(WORKER_CUT, cut, || {
            format!("worker {worker} remained registered past {bound:?}")
        }),
        Check::expect(RESTORED, restored, || {
            format!("new leader did not issue a credential within {bound:?} of restoration")
        }),
        Check::expect(BEARER_REFUSED, matches!(bearer_status, 401 | 403), || {
            format!("existing bearer answered {bearer_status} after revocation")
        }),
        Check::expect(SECOND_STREAM_CLOSED, second_closed, || {
            format!("second leader stream did not close as revoked within {bound:?}")
        }),
    ]
}

fn leader_inputs(
    cluster: &Cluster<'_>,
    endpoint: &gang_types::Url,
    credentials: &crate::identity::Credentials,
    cookie: &crate::session::SessionCookie,
) -> Result<(crate::state_api::StateStream, StateApi)> {
    let timeout = cluster.stack().settings().handshake_timeout;
    let mut second =
        StateApi::with_cookie(endpoint, credentials.authority.as_deref(), cookie, timeout)?
            .open_stream()?;
    if second.status() != STREAM_OK || second.next_event_within(timeout)?.is_none() {
        return Err(E2eError::system(
            "second leader stream did not open before revocation",
        ));
    }
    let bearer = StateApi::with_bearer(endpoint, credentials, timeout)?;
    if bearer.status(gang_wire::routes::GET_STATE.template)? != 200 {
        return Err(E2eError::system(
            "existing bearer was refused before revocation",
        ));
    }
    Ok((second, bearer))
}

fn restore_after_handoff(
    cluster: &Cluster<'_>,
    issuer: &TestIssuer,
    leader: usize,
    principal_cli: &GangCli,
    credentials: &crate::identity::Credentials,
    pin: &gang_types::CaPin,
    (bound, pace): (Duration, Duration),
) -> Result<(u16, bool, bool)> {
    let stack = cluster.stack();
    cluster.retire(leader)?;
    let next = cluster.wait_for_leader_except(Some(leader))?;
    cluster.wait_for_single_leader(next)?;
    let next_endpoint = cluster.direct_endpoint(next);
    let bearer_status = StateApi::with_bearer(
        &next_endpoint,
        credentials,
        stack.settings().handshake_timeout,
    )?
    .status(gang_wire::routes::GET_STATE.template)?;
    let persisted_refusal = issue_until(
        cluster,
        issuer,
        principal_cli,
        &next_endpoint,
        Instant::now() + bound,
        false,
        pace,
    )?;
    let admin_next = Administrator::new(
        cli(cluster, "cross-admin-next")?,
        issuer,
        next_endpoint.clone(),
        stack.placement().workspace(),
        stack.settings().command_timeout,
    );
    admin_next.enroll(pin)?;
    let restored_deadline = Instant::now() + bound;
    admin_next.restore(pin, &credentials.principal)?;
    let restored = issue_until(
        cluster,
        issuer,
        principal_cli,
        &next_endpoint,
        restored_deadline,
        true,
        pace,
    )?;
    Ok((bearer_status, persisted_refusal, restored))
}

/// Revokes on the leader, then verifies and restores on its successor.
///
/// # Errors
/// Returns an error when the cluster, clients or observations cannot be operated.
pub fn cross_revocation(cluster: &Cluster<'_>, issuer: &TestIssuer) -> Result<Vec<Check>> {
    let stack = cluster.stack();
    let pace = credential_pace(cluster)?;
    let bound = REFRESH_INTERVAL
        .saturating_add(REFRESH_MARGIN)
        .saturating_add(pace);
    let leader = cluster.wait_for_leader()?;
    let endpoint = cluster.direct_endpoint(leader);
    let administrator_cli = cli(cluster, "cross-admin-a")?;
    let admin_a = Administrator::new(
        administrator_cli.clone(),
        issuer,
        endpoint.clone(),
        stack.placement().workspace(),
        stack.settings().command_timeout,
    );
    let pin = stack.ca_pin()?;
    admin_a.enroll(&pin)?;
    let principal_cli = cli(cluster, "cross-principal")?;
    let initial = log_in(
        &principal_cli,
        issuer,
        &TokenSpec::admitted(CROSS_REVOCATION_SUBJECT),
        &endpoint,
        &pin,
        &stack.placement().workspace(),
        stack.settings().command_timeout,
    )?;
    if matches!(&initial, Issuance::Refused(_)) {
        issued(&initial)?;
    }
    let credentials = match initial {
        Issuance::Issued(credentials) => credentials,
        Issuance::Refused(reason) => return Ok(vec![Check::failed(INITIAL_LOGIN, reason)]),
    };
    let cookie = stack.session_cookie(&credentials.principal)?;
    let api = StateApi::with_cookie(
        &endpoint,
        credentials.authority.as_deref(),
        &cookie,
        stack.settings().handshake_timeout,
    )?;
    let mut stream = api.open_stream()?;
    if stream.status() != STREAM_OK {
        return Ok(vec![
            Check::passed(INITIAL_LOGIN),
            Check::failed(STREAM_OPEN, format!("stream answered {}", stream.status())),
        ]);
    }
    if stream
        .next_event_within(stack.settings().handshake_timeout)?
        .is_none()
    {
        return Ok(vec![
            Check::passed(INITIAL_LOGIN),
            Check::failed(STREAM_OPEN, "stream ended before revocation"),
        ]);
    }
    let (mut second_stream, bearer) = leader_inputs(cluster, &endpoint, &credentials, &cookie)?;
    let (worker, worker_deadline) = revoke_worker(cluster, &administrator_cli, bound)?;
    let principal_deadline = Instant::now() + bound;
    admin_a.revoke(&pin, &credentials.principal)?;
    let (observed, second_closed) = observe_revocation(
        cluster,
        issuer,
        &principal_cli,
        &endpoint,
        (&mut stream, &mut second_stream),
        &worker,
        (principal_deadline, worker_deadline, pace),
    )?;
    let original_bearer_status = bearer.status(gang_wire::routes::GET_STATE.template)?;
    let (bearer_status, persisted_refusal, restored) = restore_after_handoff(
        cluster,
        issuer,
        leader,
        &principal_cli,
        &credentials,
        &pin,
        (bound, pace),
    )?;
    let mut checks = revocation_checks(
        observed,
        restored,
        &worker,
        bound,
        second_closed,
        bearer_status,
    );
    checks.push(Check::expect(
        "cross-revocation: original leader refuses revoked bearer",
        matches!(original_bearer_status, 401 | 403),
        || format!("original leader answered {original_bearer_status} after revocation"),
    ));
    checks.push(Check::expect(
        "cross-revocation: new leader refuses revoked login",
        persisted_refusal,
        || "new leader issued a revoked principal's credential".to_owned(),
    ));
    Ok(checks)
}
