//! The server scenarios: the api contracts of one deployment, its request limits, its viewer
//! cap, its listener certificate rotation, and the paging of its scaling events. Each drives a
//! [`ServerApi`] and judges what it answered with the verdicts of
//! [`server_verdict`](crate::server_verdict).

use std::time::{Duration, Instant};

use gang_wire::page::PAGE_SIZE_MAX;
use gang_wire::routes::{
    AUTHENTICATE_SESSION, GET_SERVER, GET_STATE, LIST_EVENTS, LIST_PRINCIPALS, REVOKE_PRINCIPAL,
};
use gang_wire::{CLOSED_EVENT, ErrorReason, LOGIN_PATH, LOGOUT_PATH, SNAPSHOT_EVENT};
use http::Method;

use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::identity::{Channel as _, Credentials};
use crate::names;
use crate::server::{
    Answer, Exit, Handshake, MAX_REQUEST_SIZE, Presented, RATE_LIMIT_BURST, RawStream, Request,
    ServerApi,
};
use crate::server_verdict::{
    Ending, Page, SseBlock, api_rate_limited, asset_brotli, asset_immutable, asset_revalidates,
    burst_admitted, challenged, envelopes, established_kept, exited_zero, first_viewer_admitted,
    hashed_asset, last_page_omits_token, leaf_lifetime, leaf_pinned, leaf_reissued, logout_refused,
    page, page_size_clamped, refused, reissue_deadline, retry_line_matches, security_headers,
    shutdown_closed, still_admitted, take_blocks, tampered, ui_rate_limited, viewer_capped,
    walk_complete,
};
use crate::session::SessionCookie;

/// The address the body-limit check presents through `x-forwarded-for`, so that it starts with a
/// full bucket of its own (a documentation address, RFC 5737).
const BODY_CALLER: &str = "192.0.2.1";

/// The address the open-API checks present through `x-forwarded-for`, so that they start with a
/// full bucket of their own (a documentation address, RFC 5737).
const API_CALLER: &str = "192.0.2.2";

/// The address the sign-in checks present through `x-forwarded-for`, so that they start with a
/// full bucket of their own (a documentation address, RFC 5737).
const UI_CALLER: &str = "192.0.2.3";

/// An origin other than the deployment's, from which the cross-origin logout is sent.
const CROSS_ORIGIN: &str = "https://cross-origin.invalid";

/// The bearer presentation of `credentials`.
#[must_use]
fn bearer(credentials: &Credentials) -> Presented {
    Presented::Bearer(credentials.token.clone())
}

/// The requests of one scenario sent to a deployment, keeping the refusals of its `/v1/` routes
/// for the envelope check.
struct Exchanges<'a, A> {
    /// The deployment the requests go to.
    server: &'a A,
    /// The line and answer of each refused `/v1/` request, in the order sent.
    refusals: Vec<(String, Answer)>,
}

impl<A: ServerApi> Exchanges<'_, A> {
    /// Sends `request` and keeps its answer when it is a refusal of a `/v1/` route.
    ///
    /// # Errors
    ///
    /// When the request cannot be sent or its answer cannot be read.
    fn send(&mut self, request: &Request) -> Result<Answer> {
        let answer = self.server.send(request)?;
        if request.target.starts_with("/v1/") && answer.status >= 400 {
            self.refusals.push((request.line(), answer.clone()));
        }
        Ok(answer)
    }
}

/// A completed page walk or the reason a page could not be read.
enum PageRead<T> {
    /// All requested pages were read.
    Ok(T),
    /// A page was refused or malformed.
    Refused(String),
}

/// Walks `path` one page of `field` at a time using `request`, following each page's `next`
/// token for at most `bound` pages. The first page that cannot be read ends the walk with
/// its line and reason.
///
/// # Errors
///
/// When a request cannot be sent or its answer cannot be read.
fn walk<A: ServerApi>(
    exchanges: &mut Exchanges<'_, A>,
    path: &str,
    field: &str,
    request: &mut Request,
    bound: usize,
) -> Result<PageRead<Vec<Page>>> {
    let mut pages = Vec::new();
    let mut token: Option<String> = None;
    while pages.len() < bound {
        request.target = match &token {
            None => format!("{path}?pageSize=1"),
            Some(token) => format!("{path}?pageSize=1&pageToken={token}"),
        };
        let answer = exchanges.send(request)?;
        let current = match page(&answer, field) {
            Ok(current) => current,
            Err(reason) => return Ok(PageRead::Refused(format!("{}: {reason}", request.line()))),
        };
        token.clone_from(&current.next);
        pages.push(current);
        if token.is_none() {
            break;
        }
    }
    Ok(PageRead::Ok(pages))
}

/// Judges a page walk: both checks pass when the full list and the walk were both read; otherwise
/// both fail with the reason of the page that could not be read.
#[must_use]
fn judge_walk(
    complete: &'static str,
    omits: &'static str,
    full: &std::result::Result<Page, String>,
    walked: &PageRead<Vec<Page>>,
) -> Vec<Check> {
    match (full, walked) {
        (Ok(full), PageRead::Ok(pages)) => vec![
            walk_complete(complete, full, pages),
            last_page_omits_token(omits, pages),
        ],
        (Err(reason), _) | (_, PageRead::Refused(reason)) => vec![
            Check::failed(complete, reason.clone()),
            Check::failed(omits, reason.clone()),
        ],
    }
}

/// Reads blocks of `stream` into `blocks` until one satisfies `stop`, the stream ends or fails
/// to continue, or `timeout` passes, whichever comes first. `buffer` keeps the bytes that do not
/// yet form a whole block between calls.
///
/// # Errors
///
/// When the stream fails with anything but a timeout of one chunk.
fn read_blocks<S: RawStream>(
    stream: &mut S,
    buffer: &mut Vec<u8>,
    timeout: Duration,
    stop: impl Fn(&SseBlock) -> bool,
) -> Result<(Vec<SseBlock>, Ending)> {
    let deadline = Instant::now() + timeout;
    let mut blocks = Vec::new();
    loop {
        let fresh = take_blocks(buffer);
        let stopped = fresh.iter().any(&stop);
        blocks.extend(fresh);
        if stopped {
            return Ok((blocks, Ending::Stopped));
        }
        if stream.status() != 200 {
            return Ok((blocks, Ending::Ended));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok((blocks, Ending::TimedOut));
        }
        match stream.next_chunk_within(remaining) {
            Ok(Some(chunk)) => buffer.extend_from_slice(&chunk),
            Ok(None) => {
                blocks.extend(take_blocks(buffer));
                return Ok((blocks, Ending::Ended));
            }
            Err(E2eError::Timeout { .. }) => return Ok((blocks, Ending::TimedOut)),
            Err(error) => return Err(error),
        }
    }
}

/// Sends `target` from `caller` `RATE_LIMIT_BURST` times, then once more. Returns the answers of
/// the burst with their lines, and the answer to the request after it.
///
/// # Errors
///
/// When a request cannot be sent or its answer cannot be read.
fn burst<A: ServerApi>(
    server: &A,
    target: &str,
    caller: &'static str,
) -> Result<(Vec<(String, Answer)>, Answer)> {
    let mut admitted = Vec::new();
    for _ in 0..RATE_LIMIT_BURST {
        let request = Request::get(target).header("x-forwarded-for", caller);
        let answer = server.send(&request)?;
        admitted.push((request.line(), answer));
    }
    let request = Request::get(target).header("x-forwarded-for", caller);
    let after = server.send(&request)?;
    Ok((admitted, after))
}

/// Judges the api contracts of a deployment: the challenge of an unauthenticated request, the
/// routes and their refusals, the page walks and their tokens, the dashboard's security headers
/// and assets, revocation and logout through the cookie, the envelope of every refusal, and the
/// shutdown with an open state stream. The last step stops the server.
///
/// # Errors
///
/// When the deployment cannot be driven or observed; a deployment that misbehaves is a failed
/// [`Check`].
pub fn api<A: ServerApi>(server: &A, timeout: Duration) -> Result<Vec<Check>> {
    let admin = server.administrator_credentials()?;
    let mut authorized =
        Request::new(Method::DELETE, GET_STATE.template).presenting(bearer(&admin));
    let cookie = server.cookie_of(&admin)?;
    let mut exchanges = Exchanges {
        server,
        refusals: Vec::new(),
    };
    let mut checks = Vec::new();

    let request = Request::get(LIST_PRINCIPALS.template);
    let answer = exchanges.send(&request)?;
    checks.push(challenged(&request.line(), &answer));

    let answer = exchanges.send(&authorized)?;
    checks.push(refused(
        names::api::UNKNOWN_ROUTE_NOT_FOUND,
        &authorized.line(),
        &answer,
        ErrorReason::RouteNotFound,
        &[("method", "DELETE"), ("path", GET_STATE.template)],
    ));
    let request = Request::new(Method::DELETE, GET_STATE.template);
    let answer = exchanges.send(&request)?;
    checks.push(refused(
        names::api::UNKNOWN_ROUTE_NEEDS_CREDENTIALS,
        &request.line(),
        &answer,
        ErrorReason::CredentialsMissing,
        &[],
    ));
    checks.extend(paging_checks(&mut exchanges, &mut authorized)?);
    checks.extend(ui_checks(&mut exchanges)?);

    let principal = admin.identity()?;
    let request = Request::new(Method::POST, REVOKE_PRINCIPAL.path(&principal))
        .presenting(Presented::Cookie(cookie.clone()));
    let answer = exchanges.send(&request)?;
    checks.push(refused(
        names::api::COOKIE_POST_REFUSED,
        &request.line(),
        &answer,
        ErrorReason::CredentialsMissing,
        &[],
    ));
    LIST_PRINCIPALS.template.clone_into(&mut authorized.target);
    let answer = exchanges.send(&authorized)?;
    checks.push(still_admitted(&answer));

    let request = Request::new(Method::POST, LOGOUT_PATH)
        .presenting(Presented::Cookie(cookie.clone()))
        .header("origin", CROSS_ORIGIN);
    let answer = exchanges.send(&request)?;
    checks.push(logout_refused(&answer));

    checks.push(envelopes(&exchanges.refusals));

    checks.extend(shutdown_checks(server, &cookie, timeout)?);
    Ok(checks)
}

/// Judges the paging of the principals: a page size over the maximum is clamped, a walk one page
/// at a time lists the full list, and tampered or foreign-filter page tokens are refused.
///
/// # Errors
///
/// When a request cannot be sent or its answer cannot be read.
fn paging_checks<A: ServerApi>(
    exchanges: &mut Exchanges<'_, A>,
    request: &mut Request,
) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    request.method = Method::GET;
    request.target = format!(
        "{}?pageSize={}",
        LIST_PRINCIPALS.template,
        PAGE_SIZE_MAX + 1
    );
    let answer = exchanges.send(request)?;
    checks.push(page_size_clamped(&answer, "principals"));
    let full = page(&answer, "principals");
    let bound = full.as_ref().map_or(2, |full| full.items.len() + 1);
    let walked = walk(
        exchanges,
        LIST_PRINCIPALS.template,
        "principals",
        request,
        bound,
    )?;
    checks.extend(judge_walk(
        names::api::PAGE_WALK_COMPLETE,
        names::api::LAST_PAGE_OMITS_TOKEN,
        &full,
        &walked,
    ));

    let first_token = match &walked {
        PageRead::Ok(pages) => pages.first().and_then(|first| first.next.clone()),
        PageRead::Refused(_) => None,
    };
    match first_token {
        None => {
            let reason = match &walked {
                PageRead::Refused(reason) => reason.clone(),
                PageRead::Ok(_) => {
                    "the first page of /v1/principals carried no nextPageToken".to_owned()
                }
            };
            checks.push(Check::failed(
                names::api::TAMPERED_TOKEN_REFUSED,
                reason.clone(),
            ));
            checks.push(Check::failed(
                names::api::FOREIGN_FILTER_TOKEN_REFUSED,
                reason,
            ));
        }
        Some(next) => {
            request.target = format!(
                "{}?pageSize=1&pageToken={}",
                LIST_PRINCIPALS.template,
                tampered(&next),
            );
            let answer = exchanges.send(request)?;
            checks.push(refused(
                names::api::TAMPERED_TOKEN_REFUSED,
                &request.line(),
                &answer,
                ErrorReason::InvalidPageToken,
                &[],
            ));
            request.target = format!(
                "{}?pageSize=1&filter=revokeTime%3A*&pageToken={next}",
                LIST_PRINCIPALS.template,
            );
            let answer = exchanges.send(request)?;
            checks.push(refused(
                names::api::FOREIGN_FILTER_TOKEN_REFUSED,
                &request.line(),
                &answer,
                ErrorReason::InvalidPageToken,
                &[],
            ));
        }
    }
    Ok(checks)
}

/// Judges the dashboard page and one of its hashed assets: security headers, brotli encoding,
/// immutable caching and revalidation by `ETag`.
///
/// # Errors
///
/// When a request cannot be sent or its answer cannot be read.
fn ui_checks<A: ServerApi>(exchanges: &mut Exchanges<'_, A>) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    let request = Request::get("/ui/");
    let index = exchanges.send(&request)?;
    checks.push(security_headers(&index));
    match hashed_asset(&index.text()) {
        None => {
            let reason = "GET /ui/ references no /ui/ .js or .css asset";
            checks.push(Check::failed(names::api::UI_ASSET_BROTLI, reason));
            checks.push(Check::failed(names::api::UI_ASSET_IMMUTABLE, reason));
            checks.push(Check::failed(names::api::UI_ASSET_REVALIDATES, reason));
        }
        Some(path) => {
            let request = Request::get(path.clone()).header("accept-encoding", "br");
            let asset = exchanges.send(&request)?;
            checks.push(asset_brotli(&asset));
            checks.push(asset_immutable(&asset));
            match asset.header("etag").map(str::to_owned) {
                Some(etag) => {
                    let request = Request::get(path)
                        .header("accept-encoding", "br")
                        .header("if-none-match", etag);
                    let again = exchanges.send(&request)?;
                    checks.push(asset_revalidates(&again));
                }
                None => checks.push(Check::failed(
                    names::api::UI_ASSET_REVALIDATES,
                    "the asset answer carried no ETag",
                )),
            }
        }
    }
    Ok(checks)
}

/// Opens the state stream with `cookie`, stops the server with `SIGTERM`, and judges the
/// `closed` event the stream ends with and the server's exit status.
///
/// # Errors
///
/// When the stream cannot be opened or read, or the server cannot be signalled.
fn shutdown_checks<A: ServerApi>(
    server: &A,
    cookie: &SessionCookie,
    timeout: Duration,
) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    let mut stream = server.watch(cookie)?;
    let mut buffer = Vec::new();
    read_blocks(&mut stream, &mut buffer, timeout, |block| {
        block.event.as_deref() == Some(SNAPSHOT_EVENT)
    })?;
    let exit: std::result::Result<Exit, Check> = match server.terminate(timeout) {
        Ok(exit) => Ok(exit),
        Err(error @ E2eError::Timeout { .. }) => Err(Check::failed(
            names::api::SHUTDOWN_EXITS_ZERO,
            gang_types::error_chain(&error).to_string(),
        )),
        Err(error) => return Err(error),
    };
    let (blocks, ending) = read_blocks(&mut stream, &mut buffer, timeout, |block| {
        block.event.as_deref() == Some(CLOSED_EVENT)
    })?;
    checks.push(shutdown_closed(&blocks, ending));
    checks.push(match exit {
        Ok(exit) => exited_zero(exit),
        Err(check) => check,
    });
    Ok(checks)
}

/// Judges the request limits of a deployment: an oversized body is refused, the burst of open
/// requests of each caller is admitted, and the request after it is refused with a rate limit.
///
/// # Errors
///
/// When the deployment cannot be driven or observed; a deployment that misbehaves is a failed
/// [`Check`].
pub fn request_limits<A: ServerApi>(server: &A) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    let request = Request::new(Method::POST, AUTHENTICATE_SESSION.template)
        .header("x-forwarded-for", BODY_CALLER)
        .body("application/json", vec![b'a'; MAX_REQUEST_SIZE + 1]);
    let answer = server.send(&request)?;
    checks.push(refused(
        names::request_limits::REQUEST_TOO_LARGE,
        &request.line(),
        &answer,
        ErrorReason::RequestTooLarge,
        &[],
    ));

    let (mut admitted, api_limited) = burst(server, GET_SERVER.template, API_CALLER)?;
    let (ui_admitted, ui_limited) = burst(server, LOGIN_PATH, UI_CALLER)?;
    admitted.extend(ui_admitted);

    checks.push(burst_admitted(&admitted));
    checks.push(api_rate_limited(&api_limited));
    checks.push(ui_rate_limited(&ui_limited));
    Ok(checks)
}

/// Judges the viewer cap of a deployment: the first viewer's state stream opens with a snapshot,
/// and a second viewer over the cap is closed with the configured retry delay.
///
/// # Errors
///
/// When the deployment cannot be driven or observed; a deployment that misbehaves is a failed
/// [`Check`].
pub fn viewer_cap<A: ServerApi>(server: &A, timeout: Duration) -> Result<Vec<Check>> {
    let admin = server.administrator_credentials()?;
    let cookie = server.cookie_of(&admin)?;
    let mut checks = Vec::new();

    let mut first = server.watch(&cookie)?;
    let mut first_buffer = Vec::new();
    let (blocks, ending) = read_blocks(&mut first, &mut first_buffer, timeout, |block| {
        block.event.as_deref() == Some(SNAPSHOT_EVENT)
    })?;
    checks.push(first_viewer_admitted(first.status(), &blocks, ending));

    let mut second = server.watch(&cookie)?;
    let mut second_buffer = Vec::new();
    let (blocks, ending) = read_blocks(&mut second, &mut second_buffer, timeout, |block| {
        block.event.as_deref() == Some(CLOSED_EVENT)
    })?;
    checks.push(viewer_capped(second.status(), &blocks, ending));
    checks.push(retry_line_matches(&blocks));

    drop(second);
    drop(first);
    Ok(checks)
}

/// Judges the listener certificate rotation of a deployment: the leaf verifies against the pin,
/// lives for its configured lifetime, is reissued at its reissue point, and a connection
/// established before the reissue still answers.
///
/// # Errors
///
/// When the deployment cannot be driven or observed; a deployment that misbehaves is a failed
/// [`Check`].
pub fn certificate_rotation<A: ServerApi>(server: &A) -> Result<Vec<Check>> {
    let mut checks = Vec::new();

    let first = server.leaf()?;
    checks.push(leaf_pinned(
        names::certificate_rotation::FIRST_LEAF_PINNED,
        &first,
    ));
    let leaf = match first {
        Handshake::Verified(leaf) => leaf,
        Handshake::Refused(_) => {
            let reason = "the first leaf was refused";
            checks.push(Check::failed(
                names::certificate_rotation::LEAF_LIFETIME,
                reason,
            ));
            checks.push(Check::failed(
                names::certificate_rotation::SECOND_LEAF_PINNED,
                reason,
            ));
            checks.push(Check::failed(
                names::certificate_rotation::LEAF_REISSUED,
                reason,
            ));
            checks.push(Check::failed(
                names::certificate_rotation::ESTABLISHED_KEPT,
                reason,
            ));
            return Ok(checks);
        }
    };
    checks.push(leaf_lifetime(&leaf));

    let admin = server.administrator_credentials()?;
    let mut held = server.hold(&admin)?;
    server.wait_until(reissue_deadline(&leaf));
    let second = server.leaf()?;
    checks.push(leaf_pinned(
        names::certificate_rotation::SECOND_LEAF_PINNED,
        &second,
    ));
    checks.push(leaf_reissued(&leaf, &second));
    checks.push(established_kept(held.ping()?));
    Ok(checks)
}

/// Judges the paging of the scaling events of a deployment, after its lifecycle scenario has
/// settled: walking the events one page at a time lists each once, and the last page carries no
/// token.
///
/// # Errors
///
/// When the deployment cannot be driven or observed; a deployment that misbehaves is a failed
/// [`Check`].
pub fn event_paging<A: ServerApi>(server: &A) -> Result<Vec<Check>> {
    let token = bearer(&server.administrator_credentials()?);
    let mut exchanges = Exchanges {
        server,
        refusals: Vec::new(),
    };

    let target = format!("{}?pageSize={}", LIST_EVENTS.template, PAGE_SIZE_MAX);
    let mut request = Request::get(target).presenting(token);
    let answer = exchanges.send(&request)?;
    let full = page(&answer, "events");
    let bound = full.as_ref().map_or(2, |full| full.items.len() + 1);
    let walked = walk(
        &mut exchanges,
        LIST_EVENTS.template,
        "events",
        &mut request,
        bound,
    )?;
    Ok(judge_walk(
        names::event_paging::WALK_COMPLETE,
        names::event_paging::LAST_PAGE_OMITS_TOKEN,
        &full,
        &walked,
    ))
}
